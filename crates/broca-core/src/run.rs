//! The agentic loop (`run_loop`). Ties the seams together with the load-bearing
//! invariants:
//!  - a single durable writer per session, guarded by the LEASE — fail if held;
//!  - `stop_when` evaluated AFTER every step, before tool dispatch;
//!  - tool dispatch batch-bracketed (`tool_batch_started{call_ids}` … per-call …
//!    `tool_batch_finished`), results in assistant SOURCE ORDER;
//!  - every assistant tool-call gets a matching tool-result before the next send
//!    — denied/oversized/unavailable calls get a synthesized `is_error` result,
//!    never a sparse set (a tool_use with no result is a provider-invalid prompt);
//!  - the loop consumes only provider-ASSEMBLED control units; display deltas are
//!    forwarded to the caller untouched (off the per-token path).
//!
//! Every mutation is recorded in a write-ahead log at fsync barriers so a crashed
//! run resumes deterministically; the barrier points are commented inline.

pub use crate::clock::epoch_millis;
pub use crate::input_transformations::{
    input_transformations_reported, input_transformations_unmeasured_attempts,
    report_input_transformations,
};
pub use crate::output_events::OutputEvent;
pub use crate::output_reservation::OUTPUT_RESERVATION_OVERFLOW_CODE;
pub use crate::provider::CANCELLED_STEP_ATTEMPT_DRAIN;
pub use crate::provider_errors::{render_identity_error, stamp_credential};
pub use crate::repetition::{
    repetition_checkpoint, restore_repetition_guidance, restore_repetition_guidance_seeded,
    RepetitionCheckpointTaken, RepetitionGuidance,
};
pub use crate::stop_conditions::{limiting_stop_satisfied, stop_satisfied};
pub use crate::structured_output::{counted_submit, joined_text};
pub use crate::tool_results::{
    post_tool_text, pre_tool_refusal, pre_tool_subject, route_drained_result,
    transformed_tool_result,
};

use std::collections::HashSet;

use broca_protocol::BindIdentity;
use tokio_util::sync::CancellationToken;
use {
    broca_protocol::caller::PauseReason, broca_protocol::caller::RunFinishReason,
    broca_protocol::content::FinishReason, broca_protocol::content::Message,
    broca_protocol::content::Role, broca_protocol::request::BudgetConfig,
    broca_protocol::request::GenerationConfig, broca_protocol::request::ModelHandle,
    broca_protocol::request::StopCondition, broca_protocol::request::SubmissionRequest,
    broca_protocol::request::Tool, broca_protocol::request::ToolChoice, broca_protocol::ErrorClass,
    broca_protocol::ModelAttempt, broca_protocol::ProviderError, broca_protocol::RunId,
    broca_protocol::Usage,
};

mod control;
pub use control::RunControl;

/// The caller's request to start a run.
#[derive(Clone, Debug, PartialEq)]
pub struct RunRequest {
    pub route_decision_id: Option<String>,
    pub account_route: Option<cortexkit_role_llm_runner::send::AccountRoute>,
    pub input_written_at_ms: Option<u64>,
    pub directives: Option<Vec<cortexkit_role_llm_runner::directives::DirectiveName>>,
    pub session_class: Option<String>,
    /// Retention accepted for this send, recorded in a new episode's `RunStarted` but never
    /// included in a provider request. Resuming an existing episode cannot change its policy.
    pub retention: Option<u64>,
    pub on_restart: Option<broca_protocol::request::OnRestart>,
    /// The run's warming, which admission takes from the session's cache policy; restored from
    /// the run's start when it resumes. `None` warms nothing.
    pub keep_warm: Option<broca_protocol::request::KeepWarm>,
    /// The producer's bind project id, recorded on RunStarted for later inspection.
    /// Excluded from the session key so Some/None binds still open the same lineage.
    pub project_id: Option<String>,
    /// The principal the daemon stamped on the bind that submitted this run (for a drained
    /// queued submission, the bind that queued it), recorded on RunStarted. Like `project_id`
    /// it is neither part of the session key nor of the send identity.
    pub principal: Option<broca_protocol::BindPrincipal>,
    pub run_id: RunId,
    pub session: BindIdentity,
    /// The model this run is bound to (recorded in `run_config`; a model change
    /// between a run and its resume is detected as a mismatch).
    pub model: ModelHandle,
    /// New user message(s) this run appends to the session history.
    pub input: Vec<Message>,
    /// Tools the caller requests (intersected with `policy_allowed` into the
    /// immutable manifest).
    pub tools: Vec<Tool>,
    /// Harness policy: the tool names permitted for this run.
    pub policy_allowed: HashSet<String>,
    pub tool_choice: ToolChoice,
    pub generation: GenerationConfig,
    /// The session's cache policy for this run (llm-runner `cache`): its plan's `cache`, or a
    /// planless session's creating send's. The executor resolves it once for a fresh run and
    /// freezes the result into the provider render config; renderers never read it.
    pub cache: Option<cortexkit_role_llm_runner::cache::Cache>,
    /// Tool-call-count budget, frozen into the durable run config at admission.
    pub budget: Option<BudgetConfig>,
    /// Caller-supplied context-window override, resolved against the catalog before execution.
    pub context_limit: Option<u64>,
    /// Opaque caller-declared workload class, frozen into the WAL at admission.
    pub work_class: Option<String>,
    /// The caller's requested reasoning variant, recorded on RunStarted at admission.
    pub variant: Option<String>,
    /// The caller's requested service tier, already checked against what the model offers.
    /// A fresh or appended episode freezes it onto `RunStarted`; a resume keeps the frozen one.
    pub service_tier: Option<broca_protocol::request::ServiceTier>,
    /// Termination predicates (OR'd). Empty → `[StepCount{DEFAULT_MAX_STEPS}]`.
    pub stop_when: Vec<StopCondition>,
    /// Context management for this episode. A fresh or appended episode freezes this value onto
    /// its `RunStarted`; a resume ignores it and keeps the frozen one. The module resolves the
    /// caller's value, the session's inherited value and the process default into this field
    /// before the loop starts, so the loop only records what admission decided.
    pub context_management: Option<broca_protocol::request::ContextManagement>,
    /// If this run is started by consuming a durable queued submission, its id — written
    /// into the `RunStarted` record so the submission is provably marked CONSUMED (the
    /// pending-queue derivation excludes a submission once a `RunStarted` references it, so
    /// a crash after the consume never re-drains it). `None` for a directly-started run
    /// (a fresh conversation or a non-queued turn). Not byte-affecting: it's a durability
    /// classification field, never part of the rendered request.
    pub submission_id: Option<broca_protocol::SubmissionId>,
    /// The caller's `send_id` for the send carrying this run's message (for a drained queued
    /// submission, the key it was queued with). A fresh or appended episode records it on its
    /// `RunStarted`; a resume writes no `RunStarted` and ignores it. Never byte-affecting.
    pub send_id: Option<String>,
    /// The canonical identity of the caller's request when its run id can repeat, recorded on
    /// the `RunStarted` beside `send_id` so a keyed retry is compared against the WAL rather
    /// than the run index. Never byte-affecting.
    pub request_identity_digest: Option<String>,
    /// The human-readable title the caller's send carried (for a drained queued submission, the
    /// title it was queued with). A fresh or appended episode records it on its `RunStarted`; a
    /// resume writes no `RunStarted` and ignores it. Never byte-affecting: no renderer, routing
    /// decision, cache key or request identity reads it.
    pub title: Option<String>,
    /// The scope this episode's `RunStarted` freezes: the session's already-frozen scope, or
    /// the stamped scope admission decided to freeze or adopt. `None` for an unscoped episode.
    /// Admission sets it after its owner check; a resume writes no `RunStarted` and ignores it.
    pub scope: Option<broca_protocol::SessionScope>,
    /// The run's send arrived over a route the daemon stamped with a head scope: the run is a
    /// head's turn, which routing admits ahead of background work. Known only while broca runs;
    /// a run resumed after a restart reads false.
    pub head_turn: bool,
    /// Take the cache policy and caller cache key the session's previous run froze in its render
    /// config instead of resolving `cache`: a planless session keeps its creating send's policy
    /// for life, so its later runs carry the same cache markers and key and keep hitting the
    /// provider's cache. Admission sets it; a request never carries it.
    pub inherit_frozen_cache: bool,
    /// The schema-shaped output this run asks for. The module resolves it into the frozen
    /// render config when it resolves the provider; core reads the frozen value, never this
    /// request field, once the run has started.
    pub structured_output: Option<broca_protocol::structured_output::StructuredOutput>,
}

impl RunRequest {
    /// Rebuild a `RunRequest` from a queued submission's request + the coordinator-assigned
    /// identity (a fresh `run_id`, the session, and the `submission_id` consume-link). Every
    /// field that shapes the rendered request carries over unchanged, so a drained run renders
    /// the same bytes as the identical request sent when idle.
    pub fn from_submission(
        req: SubmissionRequest,
        run_id: RunId,
        session: BindIdentity,
        submission_id: broca_protocol::SubmissionId,
    ) -> Self {
        RunRequest {
            route_decision_id: req.route_decision_id,
            account_route: req.account_route,
            input_written_at_ms: None,
            directives: req.directives,
            session_class: None,
            retention: None,
            on_restart: req.on_restart,
            keep_warm: None,
            project_id: req.project_id,
            principal: req.principal,
            run_id,
            session,
            model: req.model,
            input: req.input,
            tools: req.tools.unwrap_or_default(),
            policy_allowed: req.policy_allowed,
            tool_choice: req.tool_choice,
            generation: req.generation,
            cache: req.cache,
            budget: req.budget,
            context_limit: req.context_limit,
            work_class: req.work_class,
            variant: req.variant,
            service_tier: req.service_tier,
            stop_when: req.stop_when,
            context_management: req.context_management,
            submission_id: Some(submission_id),
            // `send_id` and `request_identity_digest` identify the caller's send for retry
            // deduplication, and `title` labels it; none is part of the queued payload, so the
            // coordinator sets them after building the request.
            send_id: None,
            request_identity_digest: None,
            title: None,
            // Decided by admission from the session's history, never carried by the request.
            scope: None,
            head_turn: false,
            inherit_frozen_cache: false,
            structured_output: req.structured_output,
        }
    }
}

/// What a finished run reports back to the caller.
#[derive(Debug, Clone, PartialEq)]
pub struct RunSummary {
    pub reason: RunFinishReason,
    pub paused: Option<PauseReason>,
    pub steps: u64,
    pub usage: Usage,
    /// Provider retries spent across this episode's model steps.
    pub retries_used: u32,
    /// The provider's terminal reason for the last completed model step, when observed.
    pub final_step_finish_reason: Option<FinishReason>,
}

/// A requested reasoning variant that the model or route cannot render.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariantRefusal {
    pub model: String,
    pub variant: String,
    pub defined: Vec<String>,
    pub reason: String,
}

impl std::fmt::Display for VariantRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for VariantRefusal {}

/// An operator alias cannot be used for the declared tool on the selected provider wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolAliasInvalid {
    pub tool: String,
    pub reason: String,
}

impl std::fmt::Display for ToolAliasInvalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tool_alias_invalid: tool {:?}: {}",
            self.tool, self.reason
        )
    }
}

#[derive(Debug, Clone)]
pub enum RunError {
    /// The session is already held by a live writer (only one writer per session).
    LeaseHeld(String),
    Lease(String),
    /// A WAL write/barrier failed — FATAL: the run is aborted rather than continue
    /// without durability guarantees. The lease is released on return.
    Wal(String),
    /// An expiring session's byte budget refused an append. The accepted records stay
    /// writable so the run can commit a bounded permanent-error terminal.
    RetainedSessionTooLarge {
        max_bytes: u64,
    },
    SessionTooLarge {
        max_bytes: u64,
    },
    /// The run task PANICKED (unwound) before producing a terminal — synthesized by a
    /// drop-guard on the run-driving task so a panic still reports a terminal to the
    /// coordinator (otherwise the coordinator would wait forever for a result that never
    /// comes). The run did NOT finish cleanly; its session is recoverable on resume.
    Panicked,
    /// The run could not be RESOLVED before it durably started — building the provider /
    /// reading credentials / resolving the model failed BEFORE any `RunStarted` was written.
    /// The executor reports this via the admission ack so the coordinator replies a typed
    /// error and writes NOTHING durable (no ghost run). Distinct from a mid-run failure
    /// (which is a normal short terminal run).
    Config(String),
    /// The cached vault listing has not arrived; admission must not guess a credential.
    CredentialsUnavailable,
    /// More than one active vault row serves the requested key.
    CredentialAmbiguous {
        credential: String,
        candidate_ids: Vec<String>,
    },
    /// V2 Setup requires the model's real frozen context window.
    CompactionContextLimitMissing,
    /// Person media failed admission before a record could name it.
    MediaUnsupported(String),
    MediaTooLarge(cortexkit_role_llm_runner::errors::MediaTooLargeDetail),
    /// Preserve the upload contract's refusal code and retry class before admission.
    Upload(crate::media_uploads::UploadError),
    /// Unsupported reasoning variant, refused before durable admission.
    VariantUnsupported(VariantRefusal),
    ToolAliasInvalid(ToolAliasInvalid),
    /// Admission failed because a required dependency is temporarily unavailable. Nothing
    /// durable was written, so callers may retry the same send through their standard
    /// transient-error policy.
    Transient(String),
    /// A client reused a deterministic send-id-derived run id for a DIFFERENT logical send.
    /// Retries must resend the same input and frozen per-run config; a new logical send needs a
    /// new send id.
    SendIdReuse {
        differing: &'static str,
    },
    /// A session's system prompt can only be introduced by the first episode. Later turns may
    /// re-send the same bytes for convenience, but they must not silently change or add system
    /// instructions because that would fork the conversation contract and change every later
    /// rendered prefix.
    SystemPromptChanged,
    /// A session's context management is fixed by its first episode. Switching it would rewrite
    /// the cached prefix once and hand the new mode a history it did not shape, so a later send
    /// may omit the field or repeat the same value, and anything else is refused.
    ContextManagementChanged {
        frozen: broca_protocol::request::ContextManagement,
        requested: broca_protocol::request::ContextManagement,
    },
    /// The context management a send resolved to cannot be served by this process: a mode that
    /// does not exist yet, or a transform, which broca does not serve. Refused
    /// before anything durable is written, never downgraded to `"none"`.
    ContextManagementUnavailable {
        requested: broca_protocol::request::ContextManagement,
        reason: String,
    },
    /// The routing module refused to choose a provider account for this send. Refused before
    /// anything durable is written; the refusal says whether a later retry may be admitted.
    AccountSelectionRefused(AccountSelectionRefusal),
    /// broca got no usable account decision from the routing module for this send (routing
    /// unreachable, no decision within the admission budget, or a pick of a credential broca
    /// did not offer). Refused before anything durable is written, always transient: broca
    /// never picks an account on routing's behalf, so the caller retries the send.
    RoutingUnavailable(RoutingUnavailable),
    /// Numbering the transcript's tags would pass `u64::MAX` (llm-runner
    /// `tags::try_number_parts`): the run is refused rather than show two parts one number.
    TagNumbering(broca_protocol::tags::NumberingOverflow),
}

/// Why broca could not get a usable account decision from routing for a send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingUnavailableReason {
    /// The route failed to open, the connection was lost, or the reply could not be decoded.
    Unreachable,
    /// Routing did not decide within the admission budget.
    NoDecisionWithinBudget,
    /// Routing chose an account broca had not offered it: a defect in routing.
    InvalidPick,
}

impl RoutingUnavailableReason {
    /// The `detail.reason` spelling on the wire.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unreachable => "unreachable",
            Self::NoDecisionWithinBudget => "no_decision_within_budget",
            Self::InvalidPick => "invalid_pick",
        }
    }
}

/// broca's statement that routing could not answer for a send, as opposed to routing's own
/// refusal ([`AccountSelectionRefusal`]). Always transient.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingUnavailable {
    pub reason: RoutingUnavailableReason,
    /// The admission budget that ran out, only for
    /// [`RoutingUnavailableReason::NoDecisionWithinBudget`].
    pub budget_ms: Option<u64>,
}

impl RoutingUnavailable {
    pub fn unreachable() -> Self {
        Self {
            reason: RoutingUnavailableReason::Unreachable,
            budget_ms: None,
        }
    }

    pub fn no_decision_within_budget(budget_ms: u64) -> Self {
        Self {
            reason: RoutingUnavailableReason::NoDecisionWithinBudget,
            budget_ms: Some(budget_ms),
        }
    }

    pub fn invalid_pick() -> Self {
        Self {
            reason: RoutingUnavailableReason::InvalidPick,
            budget_ms: None,
        }
    }
}

/// Routing's refusal to choose an account, with its original detail and broca's retry hints.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountSelectionRefusal {
    /// Routing's refusal code, for example `account_hold`.
    pub routing_code: String,
    /// The preferred wait in seconds, from routing's deadline or its `retry_after_s`.
    pub retry_after_s: Option<u64>,
    /// Routing's reset time, when supplied, in Unix epoch milliseconds.
    pub resets_at_ms: Option<u64>,
    /// Routing's original structured detail, including fields unknown to this broca build.
    pub routing_detail: Option<Box<serde_json::Value>>,
    /// Routing's decision id, when it sent one.
    pub decision_id: Option<String>,
    /// The retry/auth classification of routing's refusal.
    pub class: ErrorClass,
}

impl RunError {
    /// Classify a failure that happened before the admission barrier. Permanent request/config
    /// failures cannot improve by retrying unchanged bytes; infrastructure and dependency
    /// failures may recover without changing the logical send.
    pub fn admission_error_class(&self) -> ErrorClass {
        match self {
            Self::Upload(error) => error.error_class(),
            Self::Config(_)
            | Self::CredentialAmbiguous { .. }
            | Self::CompactionContextLimitMissing
            | Self::MediaUnsupported(_)
            | Self::MediaTooLarge(_)
            | Self::VariantUnsupported(_)
            | Self::ToolAliasInvalid(_)
            | Self::SendIdReuse { .. }
            | Self::SystemPromptChanged
            | Self::ContextManagementChanged { .. }
            | Self::ContextManagementUnavailable { .. }
            | Self::RetainedSessionTooLarge { .. }
            | Self::SessionTooLarge { .. }
            | Self::TagNumbering(_) => ErrorClass::Permanent,
            Self::LeaseHeld(_)
            | Self::CredentialsUnavailable
            | Self::Lease(_)
            | Self::Wal(_)
            | Self::Panicked
            | Self::Transient(_)
            | Self::RoutingUnavailable(_) => ErrorClass::Transient,
            Self::AccountSelectionRefused(refusal) => refusal.class,
        }
    }
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::LeaseHeld(s) => write!(f, "session '{s}' held by a live writer"),
            RunError::CredentialsUnavailable => write!(f, "the vault credential listing has not loaded yet"),
            RunError::CredentialAmbiguous { credential, candidate_ids } => write!(f, "credential {credential} is ambiguous: {}", candidate_ids.join(", ")),
            RunError::Lease(s) => write!(f, "lease error: {s}"),
            RunError::RetainedSessionTooLarge { max_bytes } => write!(f, "{}", broca_wal::size_limit::error(*max_bytes).message),
            RunError::SessionTooLarge { max_bytes } => write!(f, "{}", broca_wal::size_limit::scoped_error(*max_bytes, broca_wal::size_limit::SizeScope::Session).message),
            RunError::Wal(s) => write!(f, "wal failure: {s}"),
            RunError::Panicked => write!(f, "run task panicked before producing a terminal"),
            RunError::TagNumbering(overflow) => write!(f, "{overflow}"),
            RunError::VariantUnsupported(s) => write!(f, "run resolution failed: {s}"),
            RunError::ToolAliasInvalid(s) => write!(f, "{s}"),
            RunError::Config(s) => write!(f, "run resolution failed: {s}"),
            RunError::CompactionContextLimitMissing => write!(f, "context_limit is required for mode-3 compaction"),
            RunError::MediaUnsupported(s) => write!(f, "{s}"),
            RunError::MediaTooLarge(_) => write!(f, "committed image exceeds a reported image limit"),
            RunError::Upload(error) => write!(f, "{error}"),
            RunError::Transient(s) => write!(f, "run admission temporarily unavailable: {s}"),
            RunError::SendIdReuse { differing } => write!(
                f,
                "run_id/send_id was already used by a send with different {differing}; retries must be byte-identical and a new send needs a new send_id"
            ),
            RunError::SystemPromptChanged => write!(
                f,
                "a session's system prompt is fixed at its first episode; re-send it byte-identical or start a new session"
            ),
            RunError::ContextManagementChanged { frozen, requested } => write!(
                f,
                "context_management_changed: this session's context management is fixed at {frozen}; \
                 the send asked for {requested}. Omit the field or send {frozen}, or start a new session"
            ),
            RunError::ContextManagementUnavailable { requested, reason } => write!(
                f,
                "context_management_unavailable: {requested} cannot be admitted: {reason}"
            ),
            RunError::AccountSelectionRefused(refusal) => write!(
                f,
                "account_selection_refused: routing refused to choose a provider account ({})",
                refusal.routing_code
            ),
            RunError::RoutingUnavailable(unavailable) => write!(
                f,
                "routing_unavailable: the routing module gave no usable account decision ({})",
                unavailable.reason.as_str()
            ),
        }
    }
}

impl std::error::Error for RunError {}

pub(crate) fn first_system_index(messages: &[Message]) -> Option<usize> {
    messages
        .iter()
        .position(|message| message.role == Role::System)
}

/// Whether every system message in `input` repeats, in order, the lineage's system message at the
/// same position among its system messages. Repeating only the first few is accepted; a changed
/// or additional system message is not.
pub(crate) fn repeats_prefix_system_messages(prefix: &[Message], input: &[Message]) -> bool {
    let mut prefix_systems = prefix.iter().filter(|message| message.role == Role::System);
    input
        .iter()
        .filter(|message| message.role == Role::System)
        .all(|message| {
            prefix_systems
                .next()
                .is_some_and(|prefix_system| prefix_system.content == message.content)
        })
}

pub(crate) fn first_system_message(messages: &[Message]) -> Option<&Message> {
    first_system_index(messages).and_then(|index| messages.get(index))
}

// Pairs the terminal state with any explicitly recorded interrupt reason.

/// Facts known at a physical send site, independent of rendering and retry policy.
#[derive(Clone, Debug)]
pub struct PhysicalSend {
    pub attempt: u32,
    pub send_index: u32,
    pub endpoint_host: String,
    pub model_sent: String,
    pub request_body_bytes: u64,
    pub compressed: bool,
    pub account_ref: Option<String>,
    /// Refresh replies carry account identity but do not include a resolved credential ID.
    pub auth_refreshed: bool,
    pub speed_requested: Option<String>,
    /// Present only for warms; derived from the selected frozen request's marker policy.
    pub warm: Option<crate::cache_warm::WarmSendPolicy>,
}

/// A receipt is permission to send only after the reservation has crossed a durability barrier.
/// Implementations must not hold writer locks across an await. Warms can use the same seam.
#[async_trait::async_trait]
pub trait AttemptHook: Send + Sync {
    async fn before_send(
        &self,
        send: PhysicalSend,
        cancel: CancellationToken,
    ) -> Result<broca_wal::record::AttemptId, ProviderError>;

    async fn finished(
        &self,
        id: broca_wal::record::AttemptId,
        attempt: ModelAttempt,
        outcome: broca_wal::record::BillingAttemptOutcome,
        response: Option<broca_wal::record::AttemptEnvelope>,
    ) -> Result<(), ProviderError>;

    /// Warm usage is independent of model-step telemetry. A hook that cannot persist it must
    /// refuse settlement rather than silently discard the provider's billing evidence.
    async fn warm_finished(
        &self,
        _id: broca_wal::record::AttemptId,
        _envelope: broca_wal::record::AttemptEnvelope,
        _sent: Option<crate::WarmSent>,
        _warm_body_sha256: String,
    ) -> Result<(), ProviderError> {
        Err(attempt_hook_error(
            "warm settlement is unsupported by this attempt hook",
        ))
    }
}

fn attempt_hook_error(detail: impl std::fmt::Display) -> ProviderError {
    ProviderError {
        class: ErrorClass::Permanent,
        message: format!("provider attempt durability failed: {detail}"),
        provider_code: Some("attempt_durability_failed".into()),
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        status: None,
        retry_after_secs: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    }
}
