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

use broca_diagnostics::{spawn_blocking, JobKind};

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use broca_protocol::structured_output::FrozenStructuredOutput;
use broca_protocol::BindIdentity;
use broca_protocol::{
    caller::{ControlUnit, PauseReason, RunFinishReason, SharedProviderError},
    content::{
        AssistantMessage, ContentBlock, FinishReason, Message, Role, ToolCall, ToolOutput,
        ToolResult,
    },
    request::{
        BudgetConfig, BudgetKind, CacheRequest, CacheTail, CallOptions, ExecutionMode,
        GenerationConfig, ModelHandle, ResponseFormat, StopCondition, SubmissionRequest, Tool,
        ToolChoice,
    },
    ApprovalResponse, BatchId, DisplayEvent, ErrorClass, ModelAttempt, NormalizedEvent,
    ProviderControl, ProviderError, RunId, StepId, ToolCallId, Usage,
};
use broca_wal::{DispatchOutcome, NoDispatchReason, RunConfig, WalRecord};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::{
    capability::{ApprovalPolicy, ToolManifest},
    context::{MaterializedStore, SessionStore},
    lease::{lease_key_for, LeaseError, LeaseStore},
    provider::Provider,
    replay::{replay, PendingTurn, ReplayResolution, ReplayState},
    tool_plane::{BatchCtx, DispatchResult, ToolPlane, ToolPlaneError},
    tool_repetition::{result_bytes, RepetitionFold, RepetitionObserver},
    wal_writer::{session_addr, WalHandle, WalStore, WalWriteError},
};

pub mod repetition_guidance;

/// Default safety cap when the caller supplies no `stop_when`.
///
/// This is a RUNAWAY BACKSTOP, not a work limit: it exists so that a caller who never thought
/// about termination cannot produce an unbounded loop, and it should sit above the longest run
/// real work produces. Measured against 18,629 episodes of agent history, 12% ran past 20 steps
/// and 0.53% past 100, so the previous 20 was cutting ordinary work rather than catching
/// pathology. 100 clears p99.5 of that distribution.
///
/// A LOW CAP IS ALSO THE WRONG INSTRUMENT FOR THE FAILURE IT LOOKS LIKE IT ADDRESSES. A model
/// looping on one failing tool call and a model doing long legitimate work are indistinguishable
/// to a counter, so lowering this to catch the first truncates the second. Repetition detection
/// is what separates them; this only bounds the blast radius when nothing else fires.
///
/// Not frozen into `RunConfig`: an empty caller `stop_when` stays empty in the durable record and
/// this default is re-applied on load, so a resumed run adopts the current value rather than the
/// one in force when it was admitted. That is deliberate (the record comments the same rule) and
/// safe in the widening direction only.
const DEFAULT_MAX_STEPS: u64 = 100;

/// Return the wall-clock metadata used by durable billing observability fields. A clock failure
/// leaves the field absent rather than introducing a non-durable or fabricated timestamp.
pub fn epoch_millis() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as u64)
}

/// One event on the runner→caller OutputStream. Display = lossy passthrough;
/// Control = the reliable assembled units.
#[derive(Debug, Clone, PartialEq)]
pub enum OutputEvent {
    /// Queue-ordered transcript position for the following step's advisory display output.
    DisplayContext {
        ordinal: Option<u64>,
    },
    Display(DisplayEvent),
    Control(ControlUnit),
}

/// The caller's request to start a run.
#[derive(Clone, Debug, PartialEq)]
pub struct RunRequest {
    pub input_written_at_ms: Option<u64>,
    pub directives: Option<Vec<cortexkit_role_llm_runner::directives::DirectiveName>>,
    pub session_class: Option<String>,
    /// Retention accepted for this send, recorded in a new episode's `RunStarted` but never
    /// included in a provider request. Resuming an existing episode cannot change its policy.
    pub retention: Option<u64>,
    pub on_restart: Option<broca_protocol::request::OnRestart>,
    /// This send's warming policy, restored from RunStarted when resuming the same episode.
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
    /// Admission-only prompt-cache preferences. The executor resolves this once for a fresh
    /// run and freezes the result into the provider render config; renderers never read this
    /// request field directly.
    pub cache: Option<CacheRequest>,
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
    /// Take the cache policy and caller cache key the session's previous episode froze in its
    /// render config instead of resolving `cache`. Set for a steer or interrupt into the session
    /// that omitted `cache`, so its requests carry the same cache markers and cache key as the
    /// session's earlier requests and keep hitting the provider's cache. Every other runner
    /// parameter such a send omits is already filled into this request.
    pub inherit_frozen_cache: bool,
    /// The schema-shaped output this run asks for. The module resolves it into the frozen
    /// render config when it resolves the provider; core reads the frozen value, never this
    /// request field, once the run has started.
    pub structured_output: Option<broca_protocol::structured_output::StructuredOutput>,
}

impl RunRequest {
    /// Strip the coordinator-assigned identity (`run_id`, `session`, `submission_id`) to
    /// the request PAYLOAD stored in a durable `Queued` record. By construction this is
    /// "everything byte-affecting RunRequest carries, minus identity" — so a queued-then-
    /// drained submission reconstructs the SAME request (see [`from_submission`]). If a new
    /// byte-affecting field is ever added to `RunRequest`, this and `from_submission` won't
    /// compile until it's handled — the completeness is enforced, not remembered.
    ///
    /// [`from_submission`]: RunRequest::from_submission
    pub fn to_submission(&self) -> SubmissionRequest {
        SubmissionRequest {
            directives: self.directives.clone(),
            on_restart: self.on_restart,
            keep_warm: self.keep_warm,
            scope: Default::default(),
            project_id: self.project_id.clone(),
            principal: self.principal.clone(),
            model: self.model.clone(),
            input: self.input.clone(),
            tools: Some(self.tools.clone()),
            policy_allowed: self.policy_allowed.clone(),
            tool_choice: self.tool_choice.clone(),
            generation: self.generation.clone(),
            cache: self.cache.clone(),
            budget: self.budget.clone(),
            auth: None,
            context_limit: self.context_limit,
            work_class: self.work_class.clone(),
            variant: self.variant.clone(),
            service_tier: self.service_tier,
            stop_when: self.stop_when.clone(),
            context_management: self.context_management.clone(),
            inherit_frozen: if self.inherit_frozen_cache {
                [broca_protocol::request::InheritedParam::Cache].into()
            } else {
                Default::default()
            },
            structured_output: self.structured_output.clone(),
            // The run keeps a send's client_message_id only inside `request_identity_digest`.
            client_message_id: None,
        }
    }

    /// Rebuild a `RunRequest` from a queued submission's request + the coordinator-assigned
    /// identity (a fresh `run_id`, the session, and the `submission_id` consume-link). The
    /// inverse of [`to_submission`]; the round trip is identity on the byte-affecting fields,
    /// so a drained run renders the same bytes as the identical request sent when idle.
    ///
    /// [`to_submission`]: RunRequest::to_submission
    pub fn from_submission(
        req: SubmissionRequest,
        run_id: RunId,
        session: BindIdentity,
        submission_id: broca_protocol::SubmissionId,
    ) -> Self {
        RunRequest {
            input_written_at_ms: None,
            directives: req.directives,
            session_class: None,
            retention: None,
            on_restart: req.on_restart,
            keep_warm: req.keep_warm,
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
            inherit_frozen_cache: req
                .inherit_frozen
                .contains(&broca_protocol::request::InheritedParam::Cache),
            structured_output: req.structured_output,
        }
    }
}

/// Admission-time WAL view: a materialized checkpoint plus only the records after its watermark.
#[derive(Debug, Clone, PartialEq)]
pub struct PriorWalSnapshot {
    /// Store watermark that precedes `tail`; zero for a complete-WAL fallback snapshot.
    pub base_watermark: u64,
    /// Durable writer head captured before this snapshot was loaded.
    pub head_seq: u64,
    pub tail: Arc<Vec<broca_wal::FramedRecord>>,
    /// Checkpoint at `base_watermark` for a bounded snapshot. Budget resume must start from
    /// this prefix state rather than state already folded through the WAL head.
    pub checkpoint_at_base: Option<crate::AdmissionCheckpointV1>,
    pub checkpoint_at_head: crate::AdmissionCheckpointV1,
}

impl PriorWalSnapshot {
    /// Move the durable pending queue into the actor's queue state before this snapshot is shared
    /// with run reconstruction, which never consumes pending submissions.
    pub fn take_pending_fifo(&mut self) -> Vec<crate::PendingSubmission> {
        let pending = std::mem::replace(
            &mut self.checkpoint_at_head.pending_fifo,
            Arc::new(Vec::new()),
        );
        Arc::try_unwrap(pending).expect("a freshly loaded admission queue is uniquely owned")
    }
}

/// Initialize advisory placement from the same validated prefix used for admission.
/// The actor calls this before exposing an inherited writer to concurrent run/steer work.
pub fn seed_display_placement<W: WalStore + ?Sized>(
    writer: &WalHandle,
    snapshot: &PriorWalSnapshot,
    wal_store: &W,
    store: Option<&dyn SessionStore>,
    session: &BindIdentity,
) {
    if !writer.needs_display_placement_seed() {
        return;
    }
    let seed = store
        .and_then(|s| s.projection_seed(session).ok().flatten())
        .filter(|s| s.watermark_seq == snapshot.base_watermark)
        .and_then(|s| crate::projection::ControlProjectionState::from_projection_seed(&s))
        .map(|s| s.placement().clone());
    let full;
    let (mut placement, records) = if snapshot.base_watermark == 0 {
        (
            crate::placement::Placement::default(),
            snapshot.tail.as_ref(),
        )
    } else if let Some(seed) = seed {
        (seed, snapshot.tail.as_ref())
    } else {
        let Ok(records) = wal_store.read_records(session) else {
            return;
        };
        full = records;
        (crate::placement::Placement::default(), &full)
    };
    for frame in records.iter().filter(|f| f.seq <= snapshot.head_seq) {
        if placement.observe(&frame.record).is_err() {
            return;
        }
    }
    writer.seed_display_placement(snapshot.head_seq, placement);
}

/// Load an admission snapshot bounded by `safe_head`. Valid materialized metadata reads only its
/// WAL suffix; every unavailable or inconsistent metadata case falls back to the complete WAL.
pub fn load_prior_wal_snapshot<W, S>(
    wal_store: &W,
    session: &BindIdentity,
    safe_head: u64,
    store: Option<&S>,
) -> Result<PriorWalSnapshot, String>
where
    W: WalStore + ?Sized,
    S: MaterializedStore + ?Sized,
{
    if let Some(store) = store {
        if let Ok(Some(snapshot)) = store.read_admission_snapshot(session) {
            if let Some(mut checkpoint) = snapshot.checkpoint {
                // A checkpoint stored before the ownership summary existed does not know who
                // drove the session's earlier episodes, which the owner check needs, and one
                // folded by a build that does not record the frozen plan manifest may have lost
                // it; only the complete WAL knows either.
                if checkpoint.at_seq == snapshot.watermark_seq
                    && snapshot.watermark_seq <= safe_head
                    && checkpoint.ownership_complete()
                    && checkpoint.head.is_some()
                    && checkpoint.compaction_complete()
                    && checkpoint.input_budget.is_some()
                {
                    // Only a budgeted episode can seed replay from the checkpoint at the store
                    // watermark. Decide from this pre-tail state: if the tail opens or corruptly
                    // claims a budgeted episode, omitting the base makes validation fail closed.
                    let pre_tail_budgeted = checkpoint
                        .last_episode
                        .as_ref()
                        .is_some_and(|episode| episode.config.budget.is_some());
                    let mut checkpoint_at_base = pre_tail_budgeted.then(|| checkpoint.clone());
                    if let Some(base) = checkpoint_at_base.as_mut() {
                        // The head queue must remain uniquely owned so `take_pending_fifo` can move
                        // it into the actor with `Arc::try_unwrap` instead of cloning its payloads.
                        base.pending_fifo = Arc::new(Vec::new());
                    }
                    if let Ok(range) = wal_store.read_records_range(
                        session,
                        snapshot.watermark_seq,
                        Some(safe_head),
                        snapshot.anchor.as_ref(),
                    ) {
                        let lineage_matches = match (
                            snapshot.lineage_id.as_deref(),
                            range.lineage_id.map(|lineage_id| lineage_id.to_string()),
                        ) {
                            (Some(stored), Some(observed)) => stored == observed,
                            _ => false,
                        };
                        // Preserve the existing replay policy for every source state; the
                        // safe-head and checkpoint checks below still decide seed validity.
                        let records = match range.into_records() {
                            broca_wal::WalRangeRecords::Complete(records)
                            | broca_wal::WalRangeRecords::Partial(records)
                            | broca_wal::WalRangeRecords::Unknown(records) => records,
                            broca_wal::WalRangeRecords::Absent => Vec::new(),
                        };
                        let reaches_safe_head = snapshot.watermark_seq == safe_head
                            || records.last().map(|record| record.seq) == Some(safe_head);
                        if lineage_matches
                            && reaches_safe_head
                            && checkpoint.fold_frames(&records).is_ok()
                            && checkpoint.compaction_boundary_complete()
                        {
                            let budgeted = checkpoint
                                .last_episode
                                .as_ref()
                                .is_some_and(|episode| episode.config.budget.is_some())
                                || records.iter().any(|record| {
                                    matches!(record.record, WalRecord::BudgetFired { .. })
                                });
                            if !budgeted
                                || checkpoint_at_base.as_ref().is_some_and(|base| {
                                    budget_checkpoint_seed_is_valid(base, &checkpoint, &records)
                                })
                            {
                                return Ok(PriorWalSnapshot {
                                    base_watermark: snapshot.watermark_seq,
                                    head_seq: safe_head,
                                    tail: Arc::new(records),
                                    checkpoint_at_base,
                                    checkpoint_at_head: checkpoint,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    let records = wal_store
        .read_records(session)
        .map_err(|error| error.to_string())?;
    let observed_head = records.last().map_or(0, |record| record.seq);
    if observed_head != safe_head {
        return Err(format!(
            "complete WAL read reached sequence {observed_head}, expected durable head {safe_head}"
        ));
    }
    let checkpoint_at_head =
        crate::AdmissionCheckpointV1::from_frames(&records).map_err(|error| error.to_string())?;
    Ok(PriorWalSnapshot {
        base_watermark: 0,
        head_seq: safe_head,
        tail: Arc::new(records),
        checkpoint_at_base: None,
        checkpoint_at_head,
    })
}

/// Check that a bounded admission tail starts from complete budget state and contains the caller
/// text recorded with each durable budget firing. The post-tail checkpoint has already folded these
/// records, so an invalid firing clears its replay state before this check can pass.
fn budget_checkpoint_seed_is_valid(
    checkpoint_at_base: &crate::AdmissionCheckpointV1,
    checkpoint_at_head: &crate::AdmissionCheckpointV1,
    tail: &[broca_wal::FramedRecord],
) -> bool {
    let (Some(base), Some(head)) = (
        checkpoint_at_base.last_episode.as_ref(),
        checkpoint_at_head.last_episode.as_ref(),
    ) else {
        return false;
    };
    let Some(budget) = base.config.budget.as_ref() else {
        return false;
    };
    let Some(state) = base.budget_replay_state.as_ref() else {
        return false;
    };
    base.run_id == head.run_id
        && base.config.budget == head.config.budget
        && state.is_valid_for(budget)
        && head
            .budget_replay_state
            .as_ref()
            .is_some_and(|state| state.is_valid_for(budget))
        && tail.iter().all(|frame| {
            !matches!(
                &frame.record,
                WalRecord::BudgetFired {
                    caller_text: None,
                    ..
                }
            )
        })
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

#[cfg(all(test, feature = "test-clock"))]
pub(crate) struct ApprovalWaitProbe {
    entered: mpsc::UnboundedSender<(bool, bool)>,
    release: tokio::sync::Mutex<mpsc::UnboundedReceiver<()>>,
}

/// Cooperative run controls. Optional controls default to "off"; ordinary control-payload
/// emission defaults to "on" so existing callers retain the complete output stream.
#[derive(Clone)]
pub struct RunControl {
    /// Installed by the late-result runtime to rejoin durable waiting slots without re-dispatch.
    /// Absent by default: a reader or a caller retry cannot drive a provider-held run.
    pub provider_held_lifecycle: Option<Arc<dyn crate::tool_plane::ProviderHeldLifecycle>>,
    /// Cheap actor-owned status; never a durability or cancellation input.
    pub step_progress: Option<crate::provider::LiveStepProgress>,
    #[cfg(all(test, feature = "test-clock"))]
    pub(crate) approval_wait_probe: Option<Arc<ApprovalWaitProbe>>,
    /// Clock shared with the coordinator's expiry decisions, used for `RunStarted`,
    /// `RunFinished` and `RunPaused` timestamps. Unset uses the system clock.
    pub clock: Option<crate::Clock>,
    /// Immediate cancellation: abort the model stream and stop dispatch, closing the batch
    /// under the existing at-most-once rules before finishing `Cancelled`. `None` disables
    /// this signal. Boundary cancellation uses the shared `steer` inbox instead, so it can
    /// forbid the next StepStarted without aborting this step's model or tool work.
    pub cancel: Option<CancellationToken>,
    /// Per-model-call timeout (the provider watchdog). The model-stream await is
    /// otherwise unbounded and would pin the lease until process death; on timeout the
    /// loop cooperatively cancels at the same safe boundary as `cancel` and releases the
    /// lease. `None` ⇒ unbounded (today's behavior).
    pub provider_call_timeout: Option<std::time::Duration>,
    /// Whether to emit ordinary payload-bearing control units (`AssistantMessage`, `ToolCall`,
    /// and `ToolResult`) on `out`. Direct loop consumers use these units, so this defaults to
    /// `true`. Module subscribers reconstruct the same units from the durable WAL projection and
    /// may disable this to avoid cloning payloads into an output channel that discards them.
    /// Lifecycle, pause, and error controls are never suppressed by this setting.
    pub emit_control_payloads: bool,
    /// Emit queue-ordered step positions for an actor's display drain. This is opt-in so
    /// direct loop consumers keep their existing bounded lifecycle-output flow.
    pub emit_display_context: bool,
    /// Pause the run for re-authentication IMMEDIATELY after admission, before any provider
    /// call. Set when the caller already knows — authoritatively, before the loop — that the
    /// credential needs re-auth (e.g. the credential vault returned `needs_reauth`: the
    /// refresh token is dead, so a provider call is doomed and would only waste a request).
    /// The run still durably ADMITS (writes `RunStarted`, so it's a real resumable run with a
    /// run id), then writes `RunPaused{AuthRequired}` carrying this error and returns —
    /// reusing the same durable-pause/resume-after-auth path a mid-run auth failure takes, so
    /// the run continues under the SAME run id once the user re-auths. `None` ⇒ never (the
    /// normal path; a credential problem that only the provider discovers still pauses
    /// mid-run via the Auth-class step failure). Off-by-default ⇒ byte-identical when unset.
    pub pause_for_auth_at_start: Option<ProviderError>,
    /// The auth-selection resolved for this execution. A fresh run freezes it into `RunStarted`;
    /// an auth-required resume compares it with the durable selection and records a separate
    /// off-render auth refreeze when it changed. Persisting the credential id keeps ordinary
    /// resumes on the same method, while persisting the vault account labels billing with the
    /// account that handled each segment. `None` means no vault token (for example, the mock
    /// path) and cannot produce an auth refreeze because there is no resolved selection to write.
    pub auth_selection: Option<broca_wal::AuthSelection>,
    /// The model's context-window limit, resolved at admission from a caller override or the
    /// catalog. A fresh run freezes it onto `RunConfig.context_limit`; a resume ignores this
    /// and keeps the value already frozen in the WAL. `None` means neither source had one.
    pub admitted_context_limit: Option<u64>,
    /// Admission identity bit for the optional tool surface. It is frozen in `RunConfig` but
    /// never supplied to a provider renderer.
    pub tools_were_omitted: bool,
    /// The module's shutdown stop signal for this run, handed to the provider transport with
    /// each step. Deliberately separate from `cancel`: tripping it never makes the loop write
    /// `Cancelled` (see [`crate::ShutdownSeal`]). `None` means nothing will ever seal this run
    /// from outside, and steps are driven exactly as without it.
    pub shutdown_seal: Option<crate::ShutdownSeal>,
    /// The module's request to stop, tripped when the process begins to shut down. From then on
    /// the run starts no new work: no next model step, and no tool call that has not been sent.
    /// Work already in flight is left to finish: a model response streaming in is read to its
    /// end and a tool call already sent runs to its result. The run then ends `Interrupted` at
    /// the next point where it would have started something, never `Cancelled`, which means a
    /// caller asked; a run whose step in flight turns out to be its last ends as it would have
    /// without the request. Deliberately separate from `cancel` and from `shutdown_seal`: the seal cuts
    /// work in flight, this only refuses to start more. `None` means nothing asks this run to
    /// stop.
    pub stop_requested: Option<CancellationToken>,
    /// Where the session's lineage actor hands this run the messages a caller steers into it
    /// (`session.send` with `delivery: steer`). The loop appends them to the prompt at its next
    /// step boundary, and closes the inbox before it writes a terminal. `None` means nothing can
    /// steer this run, and the loop behaves exactly as without it.
    pub steer: Option<std::sync::Arc<crate::SteerInbox>>,
    /// The frozen plan manifest of a session admitted with a fetch plan. A fresh episode
    /// records it on its `RunStarted`: the first episode freezes it there, and every later one
    /// repeats it so the episodes left after a WAL fold still carry it. A `RunStarted` carrying
    /// it is written as a gated frame that builds without `plan-manifest/v1` refuse to read.
    /// `None` for a session admitted without a plan, whose `RunStarted` any build reads.
    pub manifest: Option<Arc<broca_wal::PlanManifest>>,
    /// Told the WAL position of a fresh run's `RunStarted` once it is durable, and of each
    /// provider attempt record as it is appended, so the caller can report them to the routing
    /// module that chooses provider accounts. `None` reports nothing; either way the loop writes
    /// the same records.
    pub account_reporter: Option<Arc<dyn crate::AccountReporter>>,
    /// Optional installed write-time hook adapter. A frozen transform plan cannot advance
    /// without it, including on resume.
    pub hook_plane: Option<Arc<dyn crate::HookPlane>>,
    /// Input transforms computed by the lineage actor. A queue drain carries its recorded
    /// final input here; core only renders and records it, never calls pre_user.
    pub input_hooks: Option<Box<broca_wal::Hooks>>,
    /// An input hook refused the session's first send. Admission is durable before this
    /// terminal error is written; no model step or tool dispatch may follow it.
    pub pre_user_error: Option<ProviderError>,
    /// Optional driver that refreshes the provider's prompt cache during tool waits. The
    /// loop uses it only when this episode's keep_warm policy is auto.
    pub keep_warm: Option<Arc<dyn crate::keep_warm_wait::KeepWarmDriver>>,
    /// Files and reads human approval questions under the run's frozen session scope.
    /// The daemon's executor installs the transport; without it, a held call is refused.
    pub elicitation_plane: Option<Arc<dyn crate::tool_plane::ElicitationPlane>>,
    /// Adapter on the module-level role route stack, alongside the write-time hook plane.
    pub compaction_plane: Option<Arc<dyn crate::compaction::CompactionPlane>>,
    /// The budget for the compaction Setup call. Default uses COMPACTION_CALL_BUDGET_MS.
    pub compaction_config: crate::compaction::CompactionConfig,
    /// Views fetched from the session's compaction provider, offered at each step boundary.
    /// Absent, the session renders Setup's message and the views it already applied.
    pub compaction_views: Option<Arc<dyn crate::compaction::CompactionViews>>,
    #[cfg(feature = "test-clock")]
    pub test_compaction: Option<crate::compaction::test_control::Config>,
}

fn warm_wait<'a>(
    control: &'a RunControl,
    config: Option<&'a RunConfig>,
    req: &RunRequest,
    run_id: &'a RunId,
    unsupported_logged: &'a std::sync::atomic::AtomicBool,
    warm_missed: &'a std::sync::atomic::AtomicBool,
) -> Option<crate::keep_warm_wait::WarmWait<'a>> {
    if req.keep_warm != Some(broca_protocol::request::KeepWarm::Auto) {
        return None;
    }
    Some(crate::keep_warm_wait::WarmWait {
        driver: control.keep_warm.as_ref()?,
        config: config?,
        run_id,
        cancel: control.cancel.as_ref(),
        unsupported_logged,
        warm_missed,
    })
}

fn remember_warm_config(
    config: RunConfig,
    slot: &mut Option<RunConfig>,
    enabled: bool,
) -> Box<RunConfig> {
    if enabled {
        *slot = Some(config.clone());
    }
    Box::new(config)
}

impl RunControl {
    fn now_ms(&self) -> Option<u64> {
        self.clock
            .as_ref()
            .map_or_else(epoch_millis, |clock| Some(clock.now_ms()))
    }

    /// Stop accepting steered messages: the run writes no further step. Called before every
    /// terminal or pause record, so no `Steered` record can follow the episode's end.
    fn close_steers(&self) {
        if let Some(inbox) = &self.steer {
            inbox.close();
        }
    }
}

impl Default for RunControl {
    fn default() -> Self {
        Self {
            provider_held_lifecycle: None,
            step_progress: None,
            #[cfg(all(test, feature = "test-clock"))]
            approval_wait_probe: None,
            clock: None,
            cancel: None,
            compaction_plane: None,
            compaction_views: None,
            compaction_config: Default::default(),
            #[cfg(feature = "test-clock")]
            test_compaction: None,
            provider_call_timeout: None,
            emit_control_payloads: true,
            emit_display_context: false,
            pause_for_auth_at_start: None,
            auth_selection: None,
            admitted_context_limit: None,
            tools_were_omitted: false,
            shutdown_seal: None,
            stop_requested: None,
            steer: None,
            manifest: None,
            account_reporter: None,
            keep_warm: None,
            elicitation_plane: None,
            hook_plane: None,
            input_hooks: None,
            pre_user_error: None,
        }
    }
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
    /// The session's WAL holds a frame that requires WAL features this build does not know
    /// (`missing`): a newer broca build wrote this session. The bytes are intact, so this is
    /// not corruption, and nothing about the session changes between retries. Only placing a
    /// build that knows every missing feature lets it resume, which is why it is classed
    /// permanent rather than retried like a WAL I/O failure.
    NewerWriter {
        missing: Vec<String>,
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
    /// Unsupported reasoning variant, refused before durable admission.
    VariantUnsupported(VariantRefusal),
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
    /// Numbering the transcript's tags would pass `u64::MAX` (llm-runner
    /// `tags::try_number_parts`): the run is refused rather than show two parts one number.
    TagNumbering(broca_protocol::tags::NumberingOverflow),
}

/// Routing's refusal to choose an account, copied verbatim, with broca's classification.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountSelectionRefusal {
    /// Routing's refusal code, for example `account_hold`.
    pub routing_code: String,
    /// Routing's `retry_after_s`, when it sent one.
    pub retry_after_s: Option<u64>,
    /// Routing's decision id, when it sent one.
    pub decision_id: Option<String>,
    /// [`ErrorClass::Transient`] or [`ErrorClass::Permanent`].
    pub class: ErrorClass,
}

impl RunError {
    /// Classify a failure that happened before the admission barrier. Permanent request/config
    /// failures cannot improve by retrying unchanged bytes; infrastructure and dependency
    /// failures may recover without changing the logical send.
    pub fn admission_error_class(&self) -> ErrorClass {
        match self {
            Self::Config(_)
            | Self::VariantUnsupported(_)
            | Self::SendIdReuse { .. }
            | Self::SystemPromptChanged
            | Self::ContextManagementChanged { .. }
            | Self::ContextManagementUnavailable { .. }
            | Self::RetainedSessionTooLarge { .. }
            | Self::SessionTooLarge { .. }
            | Self::NewerWriter { .. }
            | Self::TagNumbering(_) => ErrorClass::Permanent,
            Self::LeaseHeld(_)
            | Self::Lease(_)
            | Self::Wal(_)
            | Self::Panicked
            | Self::Transient(_) => ErrorClass::Transient,
            Self::AccountSelectionRefused(refusal) => refusal.class,
        }
    }
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::LeaseHeld(s) => write!(f, "session '{s}' held by a live writer"),
            RunError::Lease(s) => write!(f, "lease error: {s}"),
            RunError::RetainedSessionTooLarge { max_bytes } => write!(f, "{}", crate::wal_writer::retained_size::error(*max_bytes).message),
            RunError::SessionTooLarge { max_bytes } => write!(f, "{}", crate::wal_writer::retained_size::scoped_error(*max_bytes, broca_wal::size_limit::SizeScope::Session).message),
            RunError::Wal(s) => write!(f, "wal failure: {s}"),
            RunError::NewerWriter { missing } => write!(
                f,
                "newer_writer: a newer broca build wrote this session's WAL, which requires WAL \
                 features this build does not know: {}. Retrying cannot help; resume the session \
                 on a build that knows them",
                missing.join(", ")
            ),
            RunError::Panicked => write!(f, "run task panicked before producing a terminal"),
            RunError::TagNumbering(overflow) => write!(f, "{overflow}"),
            RunError::VariantUnsupported(s) => write!(f, "run resolution failed: {s}"),
            RunError::Config(s) => write!(f, "run resolution failed: {s}"),
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
        }
    }
}

impl From<WalWriteError> for RunError {
    fn from(e: WalWriteError) -> Self {
        match e {
            WalWriteError::Failed(detail) => RunError::Wal(detail),
            WalWriteError::SessionTooLarge { max_bytes } => RunError::SessionTooLarge { max_bytes },
            WalWriteError::RetainedSessionTooLarge { max_bytes } => {
                RunError::RetainedSessionTooLarge { max_bytes }
            }
        }
    }
}

impl From<broca_wal::WalError> for RunError {
    /// Keep a newer-writer refusal typed so admission can class it permanent. Every other WAL
    /// failure, corruption included, stays [`RunError::Wal`] with its text, exactly as before.
    fn from(error: broca_wal::WalError) -> Self {
        match error {
            broca_wal::WalError::NewerWriter { missing, .. } => RunError::NewerWriter { missing },
            other => RunError::Wal(other.to_string()),
        }
    }
}

impl std::error::Error for RunError {}

/// Run the agentic loop to completion. Emits the OutputStream to `out`; returns a
/// summary, or `RunError` if the lease could not be acquired (before any output).
#[allow(clippy::too_many_arguments)]
pub async fn run_loop<P, T, L, A, W>(
    req: RunRequest,
    provider: &P,
    tool_plane: &T,
    lease_store: &L,
    approval: &A,
    wal_store: &W,
    out: &mpsc::Sender<OutputEvent>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    L: LeaseStore,
    A: ApprovalPolicy,
    W: WalStore,
{
    run_loop_with_store(
        req,
        provider,
        tool_plane,
        lease_store,
        approval,
        wal_store,
        None,
        out,
    )
    .await
}

/// `run_loop` with an optional materialized store for faster resume. When `store`
/// is `Some`, resume reconstructs from `store-prefix ⊕ WAL-tail` (reading only the
/// tail from disk) and the loop projects each replay-safe boundary into the store. With
/// `None`, resume reconstructs from a full-WAL replay (the correctness ground
/// truth / fallback). Both produce byte-identical resumes.
#[allow(clippy::too_many_arguments)]
pub async fn run_loop_with_store<P, T, L, A, W>(
    req: RunRequest,
    provider: &P,
    tool_plane: &T,
    lease_store: &L,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    out: &mpsc::Sender<OutputEvent>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    L: LeaseStore,
    A: ApprovalPolicy,
    W: WalStore,
{
    run_loop_inner(
        req,
        provider,
        tool_plane,
        lease_store,
        approval,
        wal_store,
        store,
        false,
        false,
        RunControl::default(),
        out,
    )
    .await
}

/// `run_loop_with_store` plus cooperative [`RunControl`] (cancel token + provider-call
/// watchdog). Optional controls default to off, so this is a superset of `run_loop_with_store`
/// — pass a `RunControl` with a `cancel` token to stop the run cooperatively, and/or a
/// `provider_call_timeout` to bound the model-stream await. Ordinary control payload emission
/// defaults on to preserve the complete stream for direct callers.
#[allow(clippy::too_many_arguments)]
pub async fn run_loop_with_control<P, T, L, A, W>(
    req: RunRequest,
    provider: &P,
    tool_plane: &T,
    lease_store: &L,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    control: RunControl,
    out: &mpsc::Sender<OutputEvent>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    L: LeaseStore,
    A: ApprovalPolicy,
    W: WalStore,
{
    run_loop_inner(
        req,
        provider,
        tool_plane,
        lease_store,
        approval,
        wal_store,
        store,
        false,
        false,
        control,
        out,
    )
    .await
}

/// Append a NEW turn (episode) to an EXISTING, idle lineage: the lineage's prior
/// conversation becomes this run's prefix, a fresh `RunStarted` opens a new episode
/// with `req.input` as the new user turn, and the loop runs fresh. This is the
/// multi-turn continuation entry — distinct from resuming the last (unfinished)
/// episode. It errors (interrupts) if the lineage is NOT idle (its last episode is
/// still resumable/paused/ambiguous): a new turn can only start over a finished one.
///
/// A new episode resolves its own request config from `req` (a follow-up turn may
/// carry different tools/model), unlike resume which is bound to the frozen config.
#[allow(clippy::too_many_arguments)]
pub async fn run_loop_append_episode<P, T, L, A, W>(
    req: RunRequest,
    provider: &P,
    tool_plane: &T,
    lease_store: &L,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    out: &mpsc::Sender<OutputEvent>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    L: LeaseStore,
    A: ApprovalPolicy,
    W: WalStore,
{
    run_loop_inner(
        req,
        provider,
        tool_plane,
        lease_store,
        approval,
        wal_store,
        store,
        true,
        false,
        RunControl::default(),
        out,
    )
    .await
}

/// Resume a run that PAUSED awaiting (re-)authentication, after the credential has been
/// refreshed. Continues the SAME run_id from the last durable boundary (no new episode,
/// no new `RunStarted`) — the prompt is the paused run's reconstructed prefix and the
/// loop renders byte-identical to what it would have before the pause. Only valid when
/// the last episode is actually paused (`ReplayResolution::AuthRequired`); over any other
/// state it falls through to the normal resume/idle behavior (the auth-resolved signal is
/// only consulted on a paused episode).
#[allow(clippy::too_many_arguments)]
pub async fn run_loop_resume_after_auth<P, T, L, A, W>(
    req: RunRequest,
    provider: &P,
    tool_plane: &T,
    lease_store: &L,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    out: &mpsc::Sender<OutputEvent>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    L: LeaseStore,
    A: ApprovalPolicy,
    W: WalStore,
{
    run_loop_resume_after_auth_with_control(
        req,
        provider,
        tool_plane,
        lease_store,
        approval,
        wal_store,
        store,
        RunControl::default(),
        out,
    )
    .await
}

/// Resume an auth-required run while supplying the newly resolved execution controls.
/// `control.auth_selection` is compared with the durable selection and refrozen before the
/// resumed provider call when it changed.
#[allow(clippy::too_many_arguments)]
pub async fn run_loop_resume_after_auth_with_control<P, T, L, A, W>(
    req: RunRequest,
    provider: &P,
    tool_plane: &T,
    lease_store: &L,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    control: RunControl,
    out: &mpsc::Sender<OutputEvent>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    L: LeaseStore,
    A: ApprovalPolicy,
    W: WalStore,
{
    run_loop_inner(
        req,
        provider,
        tool_plane,
        lease_store,
        approval,
        wal_store,
        store,
        false,
        true,
        control,
        out,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_loop_inner<P, T, L, A, W>(
    req: RunRequest,
    provider: &P,
    tool_plane: &T,
    lease_store: &L,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    append_episode: bool,
    auth_resolved: bool,
    control: RunControl,
    out: &mpsc::Sender<OutputEvent>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    L: LeaseStore,
    A: ApprovalPolicy,
    W: WalStore,
{
    // Acquire the single durable writer lease (fail if held). Held for the whole
    // run; released on return.
    let lease = lease_store
        .acquire(&lease_key_for(&req.session))
        .map_err(|e| match e {
            // Report the bare session name (not the namespaced scope key) — that's the
            // identity the caller knows.
            LeaseError::Held { .. } => RunError::LeaseHeld(req.session.session.clone()),
            other => RunError::Lease(other.to_string()),
        })?;

    // Move the lease to the writer thread so dropping this future cannot release it
    // while queued WAL appends or the final sync are still running.
    run_under_lease(
        req,
        lease.epoch(),
        Some(lease),
        provider,
        tool_plane,
        approval,
        wal_store,
        store,
        append_episode,
        auth_resolved,
        control,
        out,
    )
    .await
}

/// Run the loop body under an ALREADY-HELD lease, identified by its `lease_epoch` (the
/// forensic tenure stamp carried on WAL records). The caller owns lease acquisition AND
/// release — this neither acquires nor drops a lease. This is the seam the session COORDINATOR
/// uses: the coordinator holds ONE lease across a session's whole lifetime (many runs/episodes
/// between idle), so it cannot let each run re-acquire (it would self-deadlock on its own held
/// lease). The loop BODY is byte-for-byte the same as a lease-acquiring run (`run_loop_inner` is
/// just this under a freshly-acquired lease), so the rendered request bytes stay deterministic
/// (prompt-cache stability) and resume reconstruction is unchanged.
///
/// The WAL writer is still spawned + `shutdown().await`-ed per run (so it never outlives
/// the run), but the lease whose epoch the writer stamps is the caller's, not a per-run one.
#[allow(clippy::too_many_arguments)]
pub async fn run_loop_under_lease<P, T, A, W>(
    req: RunRequest,
    lease_epoch: u64,
    provider: &P,
    tool_plane: &T,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    control: RunControl,
    out: &mpsc::Sender<OutputEvent>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    A: ApprovalPolicy,
    W: WalStore,
{
    run_under_lease(
        req,
        lease_epoch,
        None,
        provider,
        tool_plane,
        approval,
        wal_store,
        store,
        false,
        false,
        control,
        out,
    )
    .await
}

/// Which start mode a run uses. The lineage coordinator chooses; the loop obeys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartMode {
    /// A fresh run, or resume the last (unfinished) episode (decided from the WAL: empty
    /// ⇒ fresh, non-empty ⇒ resume the last episode).
    FreshOrResume,
    /// Append a NEW episode to an idle lineage (the prior conversation is the prefix).
    AppendEpisode,
    /// Continue a run that paused awaiting re-auth (after the credential refreshed).
    ResumeAfterAuth,
}

impl StartMode {
    fn flags(self) -> (bool, bool) {
        match self {
            StartMode::FreshOrResume => (false, false),
            StartMode::AppendEpisode => (true, false),
            StartMode::ResumeAfterAuth => (false, true),
        }
    }
}

/// Run the loop body on an INJECTED, already-spawned WAL writer — the writer's LIFECYCLE
/// (spawn / shutdown) is the CALLER's. This is the seam the session COORDINATOR uses: it
/// owns ONE writer for the whole session lifetime (across many episodes) because the
/// durability domain is the SESSION/lineage (one WAL file, one seq space, one lease), not
/// the run. A run is just an episode within that domain. The body NEVER shuts the writer
/// down — a run ends with the `finish_run` `sync()` barrier (RunFinished durable before
/// the terminal is observable), leaving the writer live for the next episode + the
/// coordinator's queue writes.
///
/// The same body backs the per-run wrappers (`run_loop` etc., which spawn+shut a per-run
/// writer around this call), so the rendered request bytes stay deterministic
/// (prompt-cache stability) and resume reconstruction is byte-identical regardless of who
/// owns the writer lifecycle.
#[allow(clippy::too_many_arguments)]
pub async fn run_loop_on_writer<P, T, A, W>(
    req: RunRequest,
    writer: &WalHandle,
    mode: StartMode,
    provider: &P,
    tool_plane: &T,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    control: RunControl,
    out: &mpsc::Sender<OutputEvent>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    A: ApprovalPolicy,
    W: WalStore,
{
    run_loop_on_writer_admitted(
        req, writer, mode, provider, tool_plane, approval, wal_store, store, control, out, None,
    )
    .await
}

/// Like [`run_loop_on_writer`] but with an ADMISSION-ACK signal. `on_admitted` fires exactly
/// once when the run is durably ADMITTED — after the `RunStarted`+config+input sync barrier
/// (for a fresh/appended episode) or once the resume state reconstructs (for a continued
/// run), in both cases BEFORE the first provider call. The coordinator awaits this so it
/// replies `active{run_id}` only after the run is durable (no ghost run_id for a run that
/// never wrote `RunStarted`). A PRE-loop resolution failure (the executor failing to build
/// the provider) never reaches this function, so it fires `on_admitted(Err)` itself and
/// never writes `RunStarted`. The signal is observability/coordination only — the loop body
/// is byte-identical (it fires at the EXISTING barrier, adds no new one), so prompt-cache
/// stability and resume reconstruction are unchanged.
#[allow(clippy::too_many_arguments)]
pub async fn run_loop_on_writer_admitted<P, T, A, W>(
    req: RunRequest,
    writer: &WalHandle,
    mode: StartMode,
    provider: &P,
    tool_plane: &T,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    control: RunControl,
    out: &mpsc::Sender<OutputEvent>,
    on_admitted: Option<tokio::sync::oneshot::Sender<Result<RunId, RunError>>>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    A: ApprovalPolicy,
    W: WalStore,
{
    run_loop_on_writer_admitted_with_prior_snapshot(
        req,
        writer,
        mode,
        provider,
        tool_plane,
        approval,
        wal_store,
        store,
        control,
        out,
        on_admitted,
        None,
    )
    .await
}

/// Like [`run_loop_on_writer_admitted`], but reuses a caller-provided decoded WAL snapshot
/// for admission-time resume reconstruction when it still matches the writer head. If the
/// snapshot is stale, the loop falls back to a fresh `read_records` instead of silently
/// reconstructing from old frames.
#[allow(clippy::too_many_arguments)]
pub async fn run_loop_on_writer_admitted_with_prior_snapshot<P, T, A, W>(
    mut req: RunRequest,
    writer: &WalHandle,
    mode: StartMode,
    provider: &P,
    tool_plane: &T,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    control: RunControl,
    out: &mpsc::Sender<OutputEvent>,
    on_admitted: Option<tokio::sync::oneshot::Sender<Result<RunId, RunError>>>,
    prior_snapshot: Option<&PriorWalSnapshot>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    A: ApprovalPolicy,
    W: WalStore,
{
    let (append_episode, auth_resolved) = mode.flags();
    run_on_writer(
        &mut req,
        writer,
        append_episode,
        auth_resolved,
        provider,
        tool_plane,
        approval,
        wal_store,
        store,
        &control,
        out,
        on_admitted,
        prior_snapshot,
    )
    .await
}

/// The loop body: reconstruct resume state from durable admission metadata plus its WAL suffix,
/// then run. The writer is injected rather than opened here; fallbacks use a separate committed
/// read handle because the writer owns the append handle. Used by both per-run wrappers and the
/// coordinator's session-scoped writer.
#[allow(clippy::too_many_arguments)]
async fn run_on_writer<P, T, A, W>(
    req: &mut RunRequest,
    writer: &WalHandle,
    append_episode: bool,
    auth_resolved: bool,
    provider: &P,
    tool_plane: &T,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    control: &RunControl,
    out: &mpsc::Sender<OutputEvent>,
    on_admitted: Option<tokio::sync::oneshot::Sender<Result<RunId, RunError>>>,
    prior_snapshot: Option<&PriorWalSnapshot>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    A: ApprovalPolicy,
    W: WalStore,
{
    #[cfg(feature = "test-clock")]
    let test_control = {
        let mut control = control.clone();
        control.test_compaction = crate::compaction::test_control::Config::load(
            control.clock.as_ref(),
            &req.session.session,
        )?;
        if let Some(config) = &control.test_compaction {
            if let Some(clock) = &control.clock {
                clock.refresh().map_err(|e| RunError::Wal(e.to_string()))?;
            }
            control.compaction_config.call_budget_ms = config.call_timeout_ms;
            control.admitted_context_limit = Some(config.context_window);
            req.generation.max_output_tokens = Some(config.output_limit);
        }
        control
    };
    #[cfg(feature = "test-clock")]
    let control = &test_control;
    // A guard also covers unwind and task abort, where an async cleanup call cannot run.
    struct RunTransportGuard<'a, P: Provider>(&'a P);
    impl<P: Provider> Drop for RunTransportGuard<'_, P> {
        fn drop(&mut self) {
            self.0.close_run_transport();
        }
    }
    let _transport_guard = RunTransportGuard(provider);
    // If the run can't even reconstruct its resume state (a WAL read / replay failure
    // BEFORE any RunStarted barrier), the run never durably admits — fire the
    // admission-ack with the error so the coordinator replies an error, not a ghost
    // active. (A failure AFTER admission is a normal short terminal run, not this path.)
    let reconstruct = reconstruct_prior(
        req,
        writer,
        append_episode,
        wal_store,
        store.clone(),
        prior_snapshot,
    )
    .await;
    let (mut prior, is_resume, mut append_episode, last_input, approval_frames) = match reconstruct
    {
        Ok(v) => v,
        Err(e) => {
            if let Some(ack) = on_admitted {
                let _ = ack.send(Err(e.clone()));
            }
            return Err(e);
        }
    };
    // An applied view can leave the resumed prompt without the episode coordinates of earlier
    // episodes, which ChatGPT effort updates and per-episode repetition guidance render from.
    // The store projects them, so take them from there instead of replaying the whole WAL.
    if is_resume && !append_episode && prior.render_episodes.is_none() {
        if let Some(store) = store.as_ref() {
            match store_episode_render_history(Arc::clone(store), req.session.clone()).await {
                Ok(Some(episodes)) => prior.render_episodes = Some(episodes),
                Ok(None) => {}
                Err(error) => tracing::warn!(
                    target: "store",
                    "episode projection read failed; the admission replay decides: {error}"
                ),
            }
        }
    }
    // Compaction notices are numbered per session, not per lineage: the provider and the bus
    // both key them on the session. A compaction session's lineage that has recorded no notice
    // yet numbers its first one past every notice of this session's earlier lineages that
    // retention deleted, as their tombstones recorded. Only a session whose plan names a
    // compaction provider reads the tombstones, so every other admission does no extra I/O.
    let frozen_plan = if is_resume {
        prior.manifest.as_ref()
    } else {
        control.manifest.as_ref()
    };
    if frozen_plan.is_some_and(|manifest| crate::compaction::has_compaction(manifest))
        && prior
            .compaction
            .state()
            .is_none_or(|state| state.notice_seq == 0)
    {
        match wal_store.retired_notice_floor(&req.session) {
            Ok(floor) => prior.compaction_notice_floor = floor,
            Err(error) => {
                let error = RunError::Wal(error.to_string());
                if let Some(ack) = on_admitted {
                    let _ = ack.send(Err(error.clone()));
                }
                return Err(error);
            }
        }
    }

    // Promote a client's NEXT-TURN send to an append. A FreshOrResume send (append_episode
    // false) over an IDLE lineage is one of two things, and run_id IDENTITY is the only
    // signal that separates them: a genuine new turn (a client always mints a FRESH run_id →
    // append a new episode onto the prior conversation), versus a CRASH-RECOVERY re-drive of
    // a completed run (the SAME run_id re-driven over its own complete WAL → an idempotent
    // no-op that must NOT duplicate the episode). Without this, a next-turn send would hit the
    // LineageIdle no-op and SILENTLY DROP the new input. The append here routes through the
    // exact same path as an explicit AppendEpisode, so a subsequent direct send behaves like
    // a drained-queue turn (which already appends). Only promotes over LineageIdle, so it can
    // never turn a still-active/paused/ambiguous episode into a second concurrent episode.
    if !append_episode
        && is_resume
        && matches!(prior.resolution, ReplayResolution::LineageIdle { .. })
        && prior.run_id.as_ref() != Some(&req.run_id)
    {
        append_episode = true;
    }

    // The body ends with the `finish_run`/`pause_run` `sync()` barrier — RunFinished (or
    // RunPaused) is durable before the terminal is observable. The writer is NOT shut down
    // here; that's the caller's lifecycle decision (per-run wrapper shuts it down, the
    // coordinator keeps it live for the next episode).
    run_body_with_projector(
        req,
        prior,
        last_input,
        is_resume,
        append_episode,
        auth_resolved,
        control,
        provider,
        tool_plane,
        approval,
        writer,
        wal_store,
        store,
        out,
        on_admitted,
        approval_frames,
    )
    .await
}

type ReconstructedPrior = (
    ReplayState,
    bool,
    bool,
    Option<Vec<Message>>,
    Option<Arc<Vec<broca_wal::FramedRecord>>>,
);

async fn reconstruct_prior<W>(
    req: &RunRequest,
    writer: &WalHandle,
    append_episode: bool,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    prior_snapshot: Option<&PriorWalSnapshot>,
) -> Result<ReconstructedPrior, RunError>
where
    W: WalStore,
{
    require_agent_run_scope_writer(req.scope.as_ref())?;
    let snapshot = match prior_snapshot {
        Some(snapshot) if prior_snapshot_matches_writer_head(writer, snapshot) => snapshot.clone(),
        Some(_) => {
            // A reserved append can make a snapshot stale before the writer task has flushed it.
            // The barrier makes the retry's head authoritative before the loader tries the store
            // checkpoint and its WAL suffix once more.
            let safe_head = writer
                .sync()
                .await
                .map_err(|error| RunError::Wal(error.to_string()))?;
            let retried =
                load_prior_wal_snapshot(wal_store, &req.session, safe_head, store.as_deref())
                    .map_err(RunError::Wal)?;
            if prior_snapshot_matches_writer_head(writer, &retried) {
                retried
            } else {
                complete_snapshot(wal_store, &req.session)?
            }
        }
        None if store.is_some() => {
            let safe_head = writer
                .sync()
                .await
                .map_err(|error| RunError::Wal(error.to_string()))?;
            load_prior_wal_snapshot(wal_store, &req.session, safe_head, store.as_deref())
                .map_err(RunError::Wal)?
        }
        None => complete_snapshot(wal_store, &req.session)?,
    };

    require_agent_run_scope_writer(
        snapshot
            .checkpoint_at_head
            .ownership
            .as_ref()
            .and_then(|ownership| ownership.scope.as_ref()),
    )?;

    writer.seed_lineage_retention(snapshot.checkpoint_at_head.lineage_retention);
    // A lineage with no RunStarted yet takes its first tag from the highest `next_tag` that the
    // retention tombstones at this session's WAL address recorded (`retired_tag_floor`), so a
    // new lineage never reuses a tag of an earlier lineage that retention deleted.
    let lineage_tags = match snapshot.checkpoint_at_head.tags {
        Some(tags @ crate::tags::LineageTags::Unstarted { .. }) => Some(
            tags.with_floor(
                wal_store
                    .retired_tag_floor(&req.session)
                    .map_err(|error| RunError::Wal(error.to_string()))?,
            ),
        ),
        other => other,
    };
    writer.seed_lineage_tags(lineage_tags);
    seed_display_placement(writer, &snapshot, wal_store, store.as_deref(), &req.session);
    let episode = snapshot.checkpoint_at_head.last_episode.as_ref();
    let is_resume = episode.is_some();
    let last_input = episode.map(|episode| episode.input.clone());
    // Appending a new episode only makes sense over an existing lineage. A WAL containing only
    // durable queue records still has no prior conversation and therefore starts fresh.
    let append_episode = append_episode && is_resume;
    let run_config = episode.map(|episode| episode.config.clone());
    let run_id = episode.map(|episode| episode.run_id.clone());
    let budgeted = run_config
        .as_ref()
        .is_some_and(|config| config.budget.is_some())
        || snapshot
            .tail
            .iter()
            .any(|frame| matches!(&frame.record, WalRecord::BudgetFired { .. }));
    let budget_state = snapshot
        .checkpoint_at_base
        .as_ref()
        .and_then(|checkpoint| checkpoint.last_episode.as_ref())
        .and_then(|episode| episode.budget_replay_state.clone());
    let budget_seed_valid = !budgeted
        || snapshot
            .checkpoint_at_base
            .as_ref()
            .is_some_and(|checkpoint| {
                budget_checkpoint_seed_is_valid(
                    checkpoint,
                    &snapshot.checkpoint_at_head,
                    &snapshot.tail,
                )
            });

    let mut prior = match store {
        Some(store) if is_resume && (!budgeted || budget_seed_valid) => {
            match read_store_prefix_blocking(store, req.session.clone()).await {
                Ok(prefix)
                    if (snapshot.base_watermark..=snapshot.head_seq)
                        .contains(&prefix.watermark_seq)
                        && match (prefix.lineage_id.as_deref(), writer.lineage_id()) {
                            (Some(stored), Some(observed)) => stored == observed.to_string(),
                            _ => false,
                        }
                        && (!budgeted || prefix.watermark_seq == snapshot.base_watermark) =>
                {
                    let tail: Vec<WalRecord> = snapshot
                        .tail
                        .iter()
                        .filter(|frame| frame.seq > prefix.watermark_seq)
                        .map(|frame| frame.record.clone())
                        .collect();
                    match crate::context::resume_state_from_store(
                        &prefix,
                        &tail,
                        run_config.clone(),
                        run_id.clone(),
                        budget_state,
                    ) {
                        Ok(mut prior) => {
                            snapshot
                                .checkpoint_at_head
                                .restore_unchanged_effort(&mut prior);
                            if store_prior_needs_episode_spans(&prior) {
                                // Effort updates and tool-loop repetition warnings must stay in
                                // the episodes that first showed them. Recover those boundaries
                                // from the WAL when the checkpoint cannot replace them.
                                replay_complete_snapshot_or_wal(&snapshot, wal_store, &req.session)?
                            } else {
                                prior
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                target: "store",
                                "seeded admission replay failed; falling back to full WAL replay: {error}"
                            );
                            replay_complete_snapshot_or_wal(&snapshot, wal_store, &req.session)?
                        }
                    }
                }
                Ok(_) => replay_complete_snapshot_or_wal(&snapshot, wal_store, &req.session)?,
                Err(error) => {
                    tracing::warn!(
                        target: "store",
                        "store prefix read failed; falling back to full WAL replay: {error}"
                    );
                    replay_complete_snapshot_or_wal(&snapshot, wal_store, &req.session)?
                }
            }
        }
        _ => replay_complete_snapshot_or_wal(&snapshot, wal_store, &req.session)?,
    };
    prior.manifest = snapshot
        .checkpoint_at_head
        .ownership
        .as_ref()
        .and_then(|ownership| ownership.first_episode.as_ref())
        .and_then(|first| first.manifest.clone());
    prior.marker_order_version = episode.and_then(|episode| episode.marker_order_version);
    prior.keep_warm = episode.and_then(|episode| episode.keep_warm);
    prior.cancel_at = episode.and_then(|episode| episode.cancel_at);
    prior.input_budget = snapshot
        .checkpoint_at_head
        .input_budget
        .clone()
        .ok_or_else(|| {
            RunError::Wal("admission checkpoint has no session input accounting".into())
        })?;
    writer.seed_input_token_limit(prior.input_budget.limit);
    if let Some(compaction) = &snapshot.checkpoint_at_head.compaction {
        prior.compaction = compaction.clone();
        if let Some(state) = compaction.state() {
            prior.compaction_history = state.history.clone();
        }
    }
    // The store materializes whole committed turns, so an unfinished step's attempt
    // is in snapshot.tail. A prefix with no deferred calls has no older approvals to
    // recover. A missing summary or one reporting deferred calls needs complete history:
    // even a closed call can still need a withdrawal or a late execution report.
    let approval_frames = (snapshot.base_watermark == 0
        || snapshot.checkpoint_at_head.has_deferred_calls == Some(false))
    .then_some(snapshot.tail);
    Ok((
        prior,
        is_resume,
        append_episode,
        last_input,
        approval_frames,
    ))
}

// Public core callers can bypass module admission. Check both their requested scope and the
// durable session scope before the run can append a start or resume any model/tool work.
fn require_agent_run_scope_writer(
    scope: Option<&broca_protocol::SessionScope>,
) -> Result<(), RunError> {
    if !crate::scope_admission::AGENT_RUN_SCOPES_WRITER_ENABLED {
        if let Some(scope) =
            scope.filter(|scope| scope.run_id.is_some() || scope.agent_id.is_some())
        {
            return Err(RunError::Config(format!(
                "agent_run_scopes_unsupported: this build cannot start or resume work under scope '{}' (ref '{}', epoch {})",
                scope.owner.as_owner_string(), scope.scope_ref, scope.scope_epoch
            )));
        }
    }
    Ok(())
}

fn prior_snapshot_matches_writer_head(writer: &WalHandle, snapshot: &PriorWalSnapshot) -> bool {
    writer.next_record_seq() == snapshot.head_seq.saturating_add(1)
}

fn complete_snapshot<W: WalStore>(
    wal_store: &W,
    session: &BindIdentity,
) -> Result<PriorWalSnapshot, RunError> {
    let records = wal_store.read_records(session).map_err(RunError::from)?;
    let head_seq = records.last().map_or(0, |record| record.seq);
    let checkpoint_at_head = crate::AdmissionCheckpointV1::from_frames(&records)
        .map_err(|error| RunError::Wal(error.to_string()))?;
    Ok(PriorWalSnapshot {
        base_watermark: 0,
        head_seq,
        tail: Arc::new(records),
        checkpoint_at_base: None,
        checkpoint_at_head,
    })
}

fn replay_complete_snapshot_or_wal<W: WalStore>(
    snapshot: &PriorWalSnapshot,
    wal_store: &W,
    session: &BindIdentity,
) -> Result<ReplayState, RunError> {
    let records: Vec<WalRecord> = if snapshot.base_watermark == 0 {
        snapshot
            .tail
            .iter()
            .map(|frame| frame.record.clone())
            .collect()
    } else {
        wal_store
            .read_records(session)
            .map_err(RunError::from)?
            .into_iter()
            .map(|frame| frame.record)
            .collect()
    };
    replay(&records).map_err(|error| RunError::Wal(error.to_string()))
}

/// The resumed run's per-episode render inputs, from the store's episode projection.
///
/// A resumed prompt rebuilt from a materialized store prefix carries no episode boundaries, and
/// an applied view can hide most of the messages that would show them. The projection holds each
/// episode's start ordinal, its original admitted first user, its first model-step ordinal and its
/// frozen render config: the same coordinates a full WAL replay derives, which is what the test
/// beside this reads them against.
///
/// `Ok(None)` means the store cannot supply them, and the caller's WAL replay decides instead:
/// the session has no projection (a store written before the column existed), its projection is
/// older than the store's watermark (a write that advanced the transcript without recomputing
/// it), or a value in it was never computed. The stored render config is the episode's whole
/// frozen run config, from which replay renders the config blob it puts on the episode.
async fn store_episode_render_history(
    store: Arc<dyn SessionStore>,
    session: BindIdentity,
) -> Result<Option<Vec<broca_protocol::request::EpisodeRenderInput>>, String> {
    let stored = store
        .read_episode_projection_async(&session)
        .await
        .map_err(|error| format!("store episode projection read failed: {error}"))?;
    let Some(stored) = stored else {
        return Ok(None);
    };
    if stored.is_empty()
        || !stored
            .iter()
            .all(crate::context::EpisodeRenderProjection::is_complete)
        || stored
            .iter()
            .skip(1)
            .any(|episode| episode.start() == Some(0))
        || stored
            .windows(2)
            .any(|pair| pair[0].start() >= pair[1].start())
    {
        return Ok(None);
    }
    let mut episodes = Vec::with_capacity(stored.len());
    for episode in &stored {
        if episode.first_step().is_none() {
            return Ok(None);
        }
        let (Some(start), Some(first_user)) = (episode.start(), episode.first_user()) else {
            return Ok(None);
        };
        let usize_of = |ordinal: u64| usize::try_from(ordinal).map_err(|e| e.to_string());
        episodes.push(broca_protocol::request::EpisodeRenderInput {
            prompt_start: usize_of(start)?,
            first_user: first_user.map(usize_of).transpose()?,
            frozen_config: episode
                .render_config()
                .and_then(|config| {
                    serde_json::from_value::<crate::RunConfig>(config.clone())
                        .ok()
                        .map(|config| config.frozen_render_config)
                })
                .unwrap_or_else(|| episode.render_config().cloned().unwrap_or_default()),
        });
    }
    Ok(Some(episodes))
}

async fn read_store_prefix_blocking(
    store: Arc<dyn SessionStore>,
    session: BindIdentity,
) -> Result<crate::StorePrefix, String> {
    spawn_blocking(JobKind::Projection, move || {
        store.read_prefix_with_watermark(&session)
    })
    .await
    .map_err(|e| format!("store prefix task failed: {e}"))?
}

async fn read_projection_seed_blocking(
    store: Arc<dyn SessionStore>,
    session: BindIdentity,
) -> Result<Option<crate::ProjectionSeed>, String> {
    spawn_blocking(JobKind::Projection, move || store.projection_seed(&session))
        .await
        .map_err(|e| format!("store projection-seed task failed: {e}"))?
}

async fn project_store_blocking(
    store: Arc<dyn SessionStore>,
    session: BindIdentity,
    range: broca_wal::WalRangeRead,
) -> Result<(), String> {
    spawn_blocking(JobKind::Projection, move || {
        store.project_range(&session, &range)
    })
    .await
    .map_err(|e| format!("store projection task failed: {e}"))??;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_under_lease<P, T, A, W>(
    mut req: RunRequest,
    lease_epoch: u64,
    owned_lease: Option<Box<dyn crate::LeaseHandle>>,
    provider: &P,
    tool_plane: &T,
    approval: &A,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    append_episode: bool,
    auth_resolved: bool,
    control: RunControl,
    out: &mpsc::Sender<OutputEvent>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    A: ApprovalPolicy,
    W: WalStore,
{
    // PER-RUN writer: spawn it, run the body on it, then `shutdown().await` it BEFORE the
    // caller drops the lease (so a superseded writer can never append after a new run
    // acquires the session). The coordinator uses `run_loop_on_writer` instead with a
    // session-scoped writer it shuts down once at session close.
    let wal = wal_store.open(&req.session).map_err(RunError::from)?;
    let writer = match owned_lease {
        Some(lease) => WalHandle::spawn_with_lease(wal, lease),
        None => WalHandle::spawn(wal, lease_epoch),
    }
    .map_err(|error| RunError::Wal(error.to_string()))?;

    let mode = if append_episode {
        StartMode::AppendEpisode
    } else if auth_resolved {
        StartMode::ResumeAfterAuth
    } else {
        StartMode::FreshOrResume
    };
    // Heap-own the run body so lease ownership and shutdown bookkeeping do not
    // enlarge the public per-run future's inline frame.
    let result = Box::pin(run_on_writer(
        &mut req,
        &writer,
        mode == StartMode::AppendEpisode,
        mode == StartMode::ResumeAfterAuth,
        provider,
        tool_plane,
        approval,
        wal_store,
        store,
        &control,
        out,
        None, // the per-run wrapper has no coordinator awaiting admission
        None,
    ))
    .await;
    writer.shutdown().await.map_err(RunError::from)?;
    result
}

/// Recover the effective `RunConfig` for the last episode from an already-read WAL snapshot.
/// This delegates to the admission reducer so full-WAL recovery and checkpoint-tail folding have
/// one authority for refreeze validation and typed config reconstruction.
pub fn recover_prior_run_config(
    framed: &[broca_wal::FramedRecord],
) -> Result<Option<RunConfig>, RunError> {
    crate::AdmissionCheckpointV1::from_frames(framed)
        .map(|checkpoint| checkpoint.last_episode.map(|episode| episode.config))
        .map_err(|error| RunError::Wal(error.to_string()))
}

/// Read a session's frozen `RunConfig` (model, byte-affecting request config, and
/// the opaque `frozen_render_config` provider blob) from its WAL, including any pre-call
/// auth-resume refreeze, or `None` if the session has no run started yet.
///
/// This is the seam a "resume any session" tool uses BEFORE calling `run_loop`:
/// read the prior config here, deserialize `frozen_render_config` (the provider's
/// own opaque blob) to rebuild the exact provider authorized to produce this run, then call
/// `run_loop` with it. `run_loop` then verifies — via blob equality — that the
/// rebuilt provider renders byte-identical to the original; a mismatch stops the
/// run for review. `broca-core` never interprets the blob; only the caller does.
pub fn read_prior_run_config<W: WalStore>(
    wal_store: &W,
    session: &BindIdentity,
) -> Result<Option<RunConfig>, RunError> {
    // READ-ONLY: go through the non-truncating reader (read_records), not wal_store.open
    // (which truncates a torn tail — a writer-recovery action). Reading the prior config
    // must never mutate the file, even if a writer were concurrently active.
    let framed = wal_store.read_records(session).map_err(RunError::from)?;
    recover_prior_run_config(&framed)
}

type BudgetState = crate::BudgetReplayStateV1;

#[derive(Clone)]
struct BudgetFiring {
    kind: BudgetKind,
    threshold: u64,
    ordinal: u64,
    text: String,
}

fn due_budget_firings(
    config: Option<&BudgetConfig>,
    state: &BudgetState,
    ordinal: u64,
) -> Vec<BudgetFiring> {
    config.map_or_else(Vec::new, |config| {
        crate::budget_kernel::decide(state, config)
            .into_iter()
            .enumerate()
            .map(|(offset, due)| BudgetFiring {
                kind: due.kind,
                threshold: due.threshold,
                ordinal: ordinal + offset as u64,
                text: due.text,
            })
            .collect()
    })
}

impl BudgetFiring {
    fn record(&self, run_id: &RunId) -> WalRecord {
        WalRecord::BudgetFired {
            tags: None,
            run_id: run_id.clone(),
            kind: self.kind,
            threshold: self.threshold,
            ordinal: self.ordinal,
            caller_text: Some(self.text.clone()),
        }
    }
}

fn apply_committed_budget_firings(
    state: &mut BudgetState,
    config: Option<&BudgetConfig>,
    run_id: &RunId,
    firings: &[BudgetFiring],
) {
    for firing in firings {
        crate::budget_kernel::apply(
            crate::budget_kernel::State {
                facts: state,
                config,
                run_id: Some(run_id),
            },
            &firing.record(run_id),
        )
        .expect("committed live firing was due");
    }
}

/// The tags of the tool-role message holding `results`: each result is its own part, tagged
/// from `existing` (a terminal recovered from the WAL) or else with the tag the writer kept
/// when it appended the result. `None` only when no result has one (an untagged lineage).
fn live_result_tags(
    writer: &WalHandle,
    results: &[ToolResult],
    existing: &mut std::collections::HashMap<ToolCallId, std::collections::VecDeque<u64>>,
) -> Option<broca_protocol::tags::PartTags> {
    let parts: Vec<Option<u64>> = results
        .iter()
        .map(|result| {
            existing
                .get_mut(&result.tool_call_id)
                .and_then(std::collections::VecDeque::pop_front)
                .or_else(|| {
                    writer
                        .take_call_tags(&result.tool_call_id)
                        .and_then(|tags| tags.parts.first().copied())
                })
        })
        .collect();
    parts
        .iter()
        .any(Option::is_some)
        .then(|| broca_protocol::tags::PartTags::new(parts))
}

fn record_budget_firings(writer: &WalHandle, run_id: &RunId, firings: &[BudgetFiring]) {
    for firing in firings {
        writer.record(firing.record(run_id));
    }
}

async fn append_budget_firings(
    writer: &WalHandle,
    prompt: &mut Vec<Message>,
    run_id: &RunId,
    firings: &[BudgetFiring],
    out: &mpsc::Sender<OutputEvent>,
) {
    for firing in firings {
        debug_assert_eq!(prompt.len() as u64, firing.ordinal);
        let mut message = Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: firing.text.clone(),
            }],
        );
        crate::tags::attach(
            &mut message,
            writer.take_budget_tags(firing.ordinal).as_ref(),
        );
        prompt.push(message);
        let _ = send(
            out,
            OutputEvent::Control(ControlUnit::BudgetFired {
                run_id: run_id.clone(),
                kind: firing.kind,
                threshold: firing.threshold,
                ordinal: firing.ordinal,
            }),
        )
        .await;
    }
}

struct RequestConfig {
    /// The run's model — authoritative on resume (the frozen `run_config.model`), so the
    /// per-message cross-model reasoning gate compares against the SAME model identity
    /// that stamped each assistant turn's origin.
    model: ModelHandle,
    budget: Option<BudgetConfig>,
    stop_when: Vec<StopCondition>,
    tools: Arc<Vec<Tool>>,
    tool_choice: ToolChoice,
    generation: GenerationConfig,
    response_format: ResponseFormat,
    provider_options: serde_json::Value,
}

/// The workload class this episode is admitted under: the caller's on a fresh run or a new
/// episode, the FROZEN one on a resume.
///
/// Same rule as every other frozen field, for the same reason: a resumed episode continues a
/// run that was already classified, so taking the caller's value would let a retry relabel
/// history. `append_episode` is a NEW episode rather than a continuation, so it takes the
/// caller's value like a fresh run.
///
/// The value is opaque here and stays opaque: prefrontal owns the vocabulary, broca relays it.
fn admitted_work_class(
    requested: Option<&str>,
    is_resume: bool,
    append_episode: bool,
    frozen: Option<&str>,
) -> Option<String> {
    if is_resume && !append_episode {
        frozen.map(str::to_owned)
    } else {
        requested.map(str::to_owned)
    }
}

/// Resolve the request config frozen at admission:
///  - FRESH run → from `req`, with tools = `req.tools ∩ policy_allowed` while preserving
///    their order in the request.
///  - RESUME → the frozen `run_config` (req is IGNORED for these fields). A caller
///    passing different tools/generation/stop predicates on resume must not change the admitted
///    execution or diverge the rendered bytes, so run_config is authoritative. If the prior
///    WAL has no run_config (should be impossible — it's in the first WAL record and recovered
///    regardless of the store watermark), stop the run rather than silently fall back to
///    `req`, which could render divergent bytes.
fn request_config<P: Provider>(
    req: &mut RunRequest,
    is_resume: bool,
    prior: &ReplayState,
    provider: &P,
) -> Result<RequestConfig, RunError> {
    if is_resume {
        // Keep the request's tools intact: the same-send-id check later compares them with the
        // durable tools to distinguish an idempotent retry from a different logical send.
        let rc = prior.run_config.as_ref().ok_or_else(|| {
            RunError::Wal(
                "resume: run_config missing from the WAL (the seq-1 RunStarted must \
                 be recovered regardless of the store watermark)"
                    .into(),
            )
        })?;
        return Ok(RequestConfig {
            model: rc.model.clone(),
            budget: rc.budget.clone(),
            stop_when: rc.stop_when.clone(),
            tools: Arc::clone(&rc.tools),
            tool_choice: rc.tool_choice.clone(),
            generation: rc.generation.clone(),
            response_format: rc.response_format.clone(),
            provider_options: rc.provider_options.clone(),
        });
    }
    // Fresh run: req ∩ policy, req ORDER preserved.
    if req
        .stop_when
        .iter()
        .any(|condition| matches!(condition, StopCondition::StepCount { n: 0 }))
    {
        return Err(RunError::Config(
            "stop_when[].n must be at least 1 for type 'step_count'".into(),
        ));
    }
    if req.context_limit == Some(0) {
        return Err(RunError::Config(
            "context_limit=0 is invalid; omit context_limit to use the catalog value, or supply a value of at least 1"
                .into(),
        ));
    }
    if let Some(message) = provider.fresh_admission_config_error(&req.generation, &req.tool_choice)
    {
        return Err(RunError::Config(message));
    }
    let mut tools = std::mem::take(&mut req.tools);
    tools.retain(|tool| req.policy_allowed.contains(&tool.name));
    if matches!(&req.tool_choice, ToolChoice::Required) && tools.is_empty() {
        return Err(RunError::Config(
            "tool_choice 'required' needs at least one policy-allowed tool".into(),
        ));
    }
    if let ToolChoice::Tool { name } = &req.tool_choice {
        if !tools.iter().any(|t| t.name == *name) {
            return Err(RunError::Config(format!(
                "tool_choice names '{name}' which is not among the policy-allowed tools"
            )));
        }
    }
    // `cache.tail = volatile` tells the renderer to withhold the cache breakpoint on the final
    // message. On a run that can call tools that breakpoint is not waste, it is the entire
    // mechanism: each step re-sends the previous step's messages, so the anchor one step plants
    // on its last message is exactly what the next step reads back. Honouring the declaration
    // there would quietly make caching WORSE on the one leg where it already pays off, and
    // quietly is the problem -- so refuse, and name which of the two the caller should drop.
    //
    // Judged on the tools actually admitted (after the policy intersection) rather than the
    // ones requested: a run whose tools were all filtered out makes exactly one provider call,
    // and the declaration is sound for it.
    if tail_is_volatile(req.cache.as_ref()) && !tools.is_empty() {
        return Err(RunError::Config(
            "cache.tail 'volatile' conflicts with tools: a tool-bearing run re-sends each step's \
             messages, so the final-message breakpoint it withholds is what the next step reads \
             back; drop cache.tail or drop tools"
                .into(),
        ));
    }
    Ok(RequestConfig {
        model: std::mem::replace(&mut req.model, ModelHandle::new("", "")),
        budget: std::mem::take(&mut req.budget),
        stop_when: std::mem::take(&mut req.stop_when),
        tools: Arc::new(tools),
        tool_choice: std::mem::take(&mut req.tool_choice),
        generation: std::mem::take(&mut req.generation),
        response_format: ResponseFormat::default(),
        provider_options: serde_json::Value::Null,
    })
}

/// Whether the caller declared its final message varies between requests. The refusal above is
/// the only consumer in the loop; the placement it implies is resolved and frozen by the module
/// before the run starts, so nothing here reads it again on a resume.
fn tail_is_volatile(cache: Option<&CacheRequest>) -> bool {
    matches!(
        cache.and_then(|cache| cache.tail),
        Some(CacheTail::Volatile)
    )
}

/// Fire the admission-ack exactly once (no-op if already fired). `take()` ensures a second
/// call can't double-send. If a run path never fires this (an early return that didn't
/// durably admit), the `Sender` drops when the run future ends and the coordinator's await
/// observes a cancelled receiver — its liveness safety net.
fn admit(
    on_admitted: &mut Option<tokio::sync::oneshot::Sender<Result<RunId, RunError>>>,
    result: Result<RunId, RunError>,
) {
    if let Some(ack) = on_admitted.take() {
        let _ = ack.send(result);
    }
}

fn first_system_index(messages: &[Message]) -> Option<usize> {
    messages
        .iter()
        .position(|message| message.role == Role::System)
}

/// Whether every system message in `input` repeats, in order, the lineage's system message at the
/// same position among its system messages. Repeating only the first few is accepted; a changed
/// or additional system message is not.
fn repeats_prefix_system_messages(prefix: &[Message], input: &[Message]) -> bool {
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

fn first_system_message(messages: &[Message]) -> Option<&Message> {
    first_system_index(messages).and_then(|index| messages.get(index))
}

/// Validate that the provider's current renderer identity matches the state frozen at run
/// start. Null cannot prove identity: older WALs that recorded it were never resume-safe.
///
/// The recorded renderer version must be one the provider declares it still reproduces
/// byte-identically (its own version, or an earlier one it has proven), and the recorded frozen
/// config must then equal the blob the provider would have recorded at that version, exactly.
fn render_config_error<P: Provider>(provider: &P, recorded: &RunConfig) -> Option<ProviderError> {
    let current = provider.render_config_blob();
    let (message, provider_code) = if current.is_null() || recorded.frozen_render_config.is_null() {
        (
            "resume refused: the current provider or the recorded WAL has a null frozen render \
             config, so render identity cannot be verified; old WALs with a null frozen config \
             were never resume-safe and must be discarded",
            "frozen_render_config_unverifiable",
        )
    } else if !provider.reproduces_renderer_version(recorded.renderer_version) {
        (
            "resume refused: the provider renderer version differs from the version recorded at \
             run start, and the provider does not reproduce the recorded version's bytes",
            "renderer_version_drift",
        )
    } else if provider.render_config_blob_for_version(recorded.renderer_version)
        != recorded.frozen_render_config
    {
        (
            "resume refused: the provider frozen render config differs from the config recorded \
             at run start",
            "frozen_render_config_drift",
        )
    } else {
        return None;
    };

    Some(ProviderError {
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Permanent,
        message: message.into(),
        status: None,
        retry_after_secs: None,
        provider_code: Some(provider_code.into()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    })
}

const PROJECTOR_WRITE_WARNING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone, Copy, Default)]
struct ProjectionRequest {
    safe_h: u64,
    shutdown: bool,
    drain_required: bool,
    budgeted: bool,
}

struct ProjectionHandle {
    tx: watch::Sender<ProjectionRequest>,
}

impl ProjectionHandle {
    fn enqueue(&self, safe_h: u64) {
        self.tx.send_if_modified(|pending| {
            if pending.budgeted {
                // Budgeted projection still needs the complete WAL to reconstruct injected
                // messages, so projecting every turn would rewrite the full lineage. Leaving the
                // store stale until the terminal projection is crash-safe: reconstruct_prior uses
                // full-WAL ground truth for budgeted admission, and session.read falls back to a
                // full WAL when a BudgetFired tail no longer carries its RunConfig.
                return false;
            }
            if safe_h <= pending.safe_h {
                return false;
            }
            pending.safe_h = safe_h;
            true
        });
    }

    fn enqueue_terminal(&self, safe_h: u64) {
        self.tx.send_modify(|pending| {
            pending.safe_h = pending.safe_h.max(safe_h);
            pending.drain_required = true;
        });
    }

    fn set_budgeted(&self, budgeted: bool) {
        self.tx.send_modify(|pending| pending.budgeted = budgeted);
    }

    fn is_budgeted(&self) -> bool {
        self.tx.borrow().budgeted
    }

    fn close(&self) {
        self.tx.send_modify(|pending| pending.shutdown = true);
    }
}

fn projection_channel() -> (ProjectionHandle, watch::Receiver<ProjectionRequest>) {
    let (tx, rx) = watch::channel(ProjectionRequest::default());
    (ProjectionHandle { tx }, rx)
}

async fn projector_task<W: WalStore>(
    wal_store: &W,
    store: Arc<dyn SessionStore>,
    session: &BindIdentity,
    mut pending: watch::Receiver<ProjectionRequest>,
) {
    let mut last_processed = 0;
    loop {
        let request = *pending.borrow_and_update();
        if request.shutdown && (!request.drain_required || request.safe_h <= last_processed) {
            return;
        }
        if request.safe_h > last_processed {
            project_committed(
                wal_store,
                store.clone(),
                session,
                request.safe_h,
                request.budgeted,
            )
            .await;
            last_processed = request.safe_h;
            continue;
        }
        if pending.changed().await.is_err() {
            return;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_body_with_projector<P, T, A, W>(
    req: &mut RunRequest,
    prior: ReplayState,
    last_input: Option<Vec<Message>>,
    is_resume: bool,
    append_episode: bool,
    auth_resolved: bool,
    control: &RunControl,
    provider: &P,
    tool_plane: &T,
    approval: &A,
    writer: &WalHandle,
    wal_store: &W,
    store: Option<Arc<dyn SessionStore>>,
    out: &mpsc::Sender<OutputEvent>,
    on_admitted: Option<tokio::sync::oneshot::Sender<Result<RunId, RunError>>>,
    approval_frames: Option<Arc<Vec<broca_wal::FramedRecord>>>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    A: ApprovalPolicy,
    W: WalStore,
{
    if is_resume && (req.retention.is_some() || writer.is_retention_lineage() != Some(false)) {
        let records = wal_store.read_records(&req.session)?;
        let frozen = records.iter().rev().find_map(|frame| match &frame.record {
            WalRecord::RunStarted {
                retention: Some(seconds),
                ..
            }
            | WalRecord::Queued {
                retention: Some(seconds),
                ..
            }
            | WalRecord::Steered {
                retention: Some(seconds),
                ..
            } => Some(*seconds),
            _ => None,
        });
        // An appended episode is a new send and may shorten the lineage's policy.
        // Resuming the same episode must retain its already admitted policy exactly.
        let shortening = append_episode
            && req
                .retention
                .zip(frozen)
                .is_some_and(|(requested, current)| requested > 0 && requested < current);
        if req.retention.is_some() && req.retention != frozen && !shortening {
            return Err(RunError::Config(
                "retention disagrees with the lineage's frozen policy".into(),
            ));
        }
        req.retention = req.retention.or(frozen);
    }
    let policy_session = req.session.clone();
    let retention_lineage = || match writer.is_retention_lineage() {
        Some(retained) => Ok(retained),
        None => wal_store
            .is_retention_lineage(&policy_session)
            .map_err(RunError::from),
    };
    let Some(store) = store else {
        let result = run_body_with_approval_history(
            req,
            prior,
            last_input,
            is_resume,
            append_episode,
            auth_resolved,
            control,
            provider,
            tool_plane,
            approval,
            writer,
            retention_lineage,
            None,
            out,
            on_admitted,
            Some(wal_store),
            approval_frames.clone(),
        )
        .await;
        return seal_size_refusal(
            result,
            req,
            writer,
            wal_store,
            control,
            out,
            is_resume && !append_episode,
        )
        .await;
    };

    // Poll the projector as a sibling task in this writer-owning scope. Unlike a detached
    // spawn, this can borrow any WalStore implementation and cannot outlive the scope that
    // performs the ordered drain below.
    let (projector, pending) = projection_channel();
    let projection_session = req.session.clone();
    let projection = projector_task(wal_store, store, &projection_session, pending);
    tokio::pin!(projection);

    let mut result = {
        let body = run_body_with_approval_history(
            req,
            prior,
            last_input,
            is_resume,
            append_episode,
            auth_resolved,
            control,
            provider,
            tool_plane,
            approval,
            writer,
            retention_lineage,
            Some(&projector),
            out,
            on_admitted,
            Some(wal_store),
            approval_frames,
        );
        tokio::pin!(body);
        tokio::select! {
            result = &mut body => result,
            () = &mut projection => unreachable!("projector stopped before its owner"),
        }
    };

    result = seal_size_refusal(
        result,
        req,
        writer,
        wal_store,
        control,
        out,
        is_resume && !append_episode,
    )
    .await;

    // Intermediate projection is suppressed for budgeted runs, including paths such as
    // RunPaused and hard stops that return before run_body's ordinary terminal enqueue.
    // Schedule their one full projection after run_body reaches its durable return boundary,
    // then use the existing close-and-drain path so store completion still gates writer and
    // lease release.
    if result.is_ok() && projector.is_budgeted() {
        match writer.sync().await {
            Ok(safe_h) => projector.enqueue_terminal(safe_h),
            Err(error) => result = Err(error.into()),
        }
    }

    // Closing prevents queued work from starting. If a projection is already in flight,
    // wait for its blocking store write to acknowledge completion before the writer or lease
    // can be released.
    projector.close();
    projection.await;
    result
}

async fn seal_size_refusal<W: WalStore>(
    result: Result<RunSummary, RunError>,
    req: &RunRequest,
    writer: &WalHandle,
    wal_store: &W,
    control: &RunControl,
    out: &mpsc::Sender<OutputEvent>,
    owns_prior_episode: bool,
) -> Result<RunSummary, RunError> {
    let (max_bytes, scope) = match &result {
        Err(RunError::RetainedSessionTooLarge { max_bytes }) => (
            *max_bytes,
            broca_wal::size_limit::SizeScope::RetainedSession,
        ),
        Err(RunError::SessionTooLarge { max_bytes }) => {
            (*max_bytes, broca_wal::size_limit::SizeScope::Session)
        }
        _ => return result,
    };
    let _ = writer.sync_for_seal().await?;
    let frames = wal_store.read_records(&req.session)?;
    let records: Vec<_> = frames.into_iter().map(|frame| frame.record).collect();
    let prior = replay(&records).map_err(|error| RunError::Wal(error.to_string()))?;
    // A rejected new RunStarted must not finish an earlier run. A resume instead owns
    // the already-admitted run id, which need not equal the request id. If a crash happens
    // before the Error terminal below, startup commits an Interrupted orphan terminal.
    let owned = owns_prior_episode || prior.run_id.as_ref() == Some(&req.run_id);
    if owned && prior.run_finished_durable {
        control.close_steers();
        writer.clear_size_refusal();
        if let Some((reason, usage, retries_used)) =
            records.iter().rev().find_map(|record| match record {
                WalRecord::RunFinished {
                    reason,
                    usage,
                    retries_used,
                    ..
                } => Some((*reason, *usage, *retries_used)),
                _ => None,
            })
        {
            return Ok(RunSummary {
                reason,
                paused: None,
                steps: prior.completed_steps,
                usage,
                retries_used,
                final_step_finish_reason: prior.last_step_finish_reason,
            });
        }
    }
    if !prior.run_finished_durable && owned {
        control.close_steers();
        let run_id = prior
            .run_id
            .as_ref()
            .ok_or_else(|| RunError::Wal("unfinished resume has no run id".into()))?;
        writer.record_size_refusal_terminal(
            max_bytes,
            control.now_ms(),
            prior.usage,
            prior.retries_used,
        )?;
        writer.sync().await?;
        let error = crate::wal_writer::retained_size::scoped_error(max_bytes, scope);
        let _ = send(
            out,
            OutputEvent::Control(ControlUnit::Error {
                error: error.into(),
            }),
        )
        .await;
        let _ = send(
            out,
            OutputEvent::Control(ControlUnit::RunFinished {
                run_id: Some(run_id.clone()),
                reason: RunFinishReason::Error,
                usage: Some(prior.usage),
                final_step_finish_reason: prior.last_step_finish_reason,
                indeterminate_tool_calls: None,
                retries_used: prior.retries_used,
            }),
        )
        .await;
        return Ok(RunSummary {
            reason: RunFinishReason::Error,
            paused: None,
            steps: prior.completed_steps,
            usage: prior.usage,
            retries_used: prior.retries_used,
            final_step_finish_reason: prior.last_step_finish_reason,
        });
    } else {
        writer.clear_size_refusal();
    }
    result
}

/// Render the tags of the messages appended to `prompt` since `rendered_len`: a run that
/// renders tags shows numbers (by the rules each message was written under); any run shows
/// the model's text cleaned where an earlier tag-rendering run under the computed rules wrote
/// it. Each message is rendered once, from its stored text, so every later request repeats the
/// same bytes.
fn render_new_tags(
    prompt: &mut [Message],
    rendered_len: &mut usize,
    render_tags: bool,
    frozen_render_config: &serde_json::Value,
    mut originals: Option<&mut crate::compaction::OriginalMessages>,
) {
    if *rendered_len >= prompt.len() {
        return;
    }
    for (index, message) in prompt.iter_mut().enumerate().skip(*rendered_len) {
        let original = originals
            .as_ref()
            .filter(|_| message.part_tags().is_some())
            .map(|_| message.clone());
        if render_tags {
            broca_protocol::tags::render_tags_into(
                std::slice::from_mut(message),
                broca_protocol::tags::TagRendering::for_render_config(frozen_render_config),
            );
        } else {
            broca_protocol::tags::clean_computed_into(std::slice::from_mut(message));
        }
        if let (Some(originals), Some(original)) = (originals.as_deref_mut(), original) {
            originals.retain_change(index, original, message);
        }
    }
    *rendered_len = prompt.len();
}

fn stamp_raw_suffix(raw: &mut [Message], stamped_len: &mut usize) -> usize {
    let stamped = broca_protocol::content::stamp_ordinals_from(raw, *stamped_len);
    *stamped_len = raw.len();
    stamped
}

#[test]
fn raw_stamping_visits_only_new_messages() {
    let mut raw = vec![Message::new(Role::User, vec![]); 1000];
    let mut stamped_len = 0;
    assert_eq!(stamp_raw_suffix(&mut raw, &mut stamped_len), 1000);
    raw.extend([
        Message::new(Role::Assistant, vec![]),
        Message::new(Role::Tool, vec![]),
    ]);
    assert_eq!(stamp_raw_suffix(&mut raw, &mut stamped_len), 2);
    assert_eq!(raw[1001].ordinal, Some(1001));
    assert_eq!(stamp_raw_suffix(&mut raw, &mut stamped_len), 0);
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn run_body<P: Provider, T: ToolPlane, A: ApprovalPolicy>(
    req: &mut RunRequest,
    prior: ReplayState,
    last_input: Option<Vec<Message>>,
    is_resume: bool,
    append_episode: bool,
    auth_resolved: bool,
    control: &RunControl,
    provider: &P,
    tool_plane: &T,
    approval: &A,
    writer: &WalHandle,
    retention_lineage: impl Fn() -> Result<bool, RunError> + Send + Sync,
    projector: Option<&ProjectionHandle>,
    out: &mpsc::Sender<OutputEvent>,
    on_admitted: Option<tokio::sync::oneshot::Sender<Result<RunId, RunError>>>,
) -> Result<RunSummary, RunError> {
    Box::pin(run_body_with_approval_history(
        req,
        prior,
        last_input,
        is_resume,
        append_episode,
        auth_resolved,
        control,
        provider,
        tool_plane,
        approval,
        writer,
        retention_lineage,
        projector,
        out,
        on_admitted,
        None,
        None,
    ))
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_body_with_approval_history<P, T, A>(
    req: &mut RunRequest,
    mut prior: ReplayState,
    last_input: Option<Vec<Message>>,
    is_resume: bool,
    append_episode: bool,
    auth_resolved: bool,
    control: &RunControl,
    provider: &P,
    tool_plane: &T,
    approval: &A,
    writer: &WalHandle,
    retention_lineage: impl Fn() -> Result<bool, RunError> + Send + Sync,
    projector: Option<&ProjectionHandle>,
    out: &mpsc::Sender<OutputEvent>,
    on_admitted: Option<tokio::sync::oneshot::Sender<Result<RunId, RunError>>>,
    approval_store: Option<&dyn WalStore>,
    approval_frames: Option<Arc<Vec<broca_wal::FramedRecord>>>,
) -> Result<RunSummary, RunError>
where
    P: Provider,
    T: ToolPlane,
    A: ApprovalPolicy,
{
    if !is_resume && req.retention.is_some() && writer.is_retention_lineage() == Some(false) {
        return Err(RunError::Config(
            "retention disagrees with the lineage's frozen policy".into(),
        ));
    }
    let mut input_budget = prior.input_budget.clone();
    let frozen_input_limit = input_budget.limit;
    let requested_limit = req
        .budget
        .as_ref()
        .and_then(|budget| budget.max_input_tokens);
    // A queued send was validated when accepted. A later accepted send may have lowered
    // its limit while it waited; draining the earlier send must honor that tighter policy.
    let requested_limit = if req.submission_id.is_some() {
        requested_limit.map(|limit| input_budget.limit.map_or(limit, |frozen| frozen.min(limit)))
    } else {
        requested_limit
    };
    input_budget.limit = match input_budget.admit(requested_limit) {
        Ok(limit) => limit,
        Err(message) => {
            let error = RunError::Config(message);
            if let Some(ack) = on_admitted {
                let _ = ack.send(Err(error.clone()));
            }
            return Err(error);
        }
    };
    let input_cap_changed = input_budget.limit != frozen_input_limit;
    // Saved approvals cannot authorize work without the requester adapter. Refuse
    // before any admission, recovery or terminal record can change the episode.
    // Reuse the admission frames, whether a suffix or a complete fallback. When the
    // prefix summary is missing or reports deferred calls, read complete history once
    // and share it among the attempt-cut check and the approval consumers below.
    let frames_available = approval_store.is_some() || approval_frames.is_some();
    let approval_frames = match approval_frames {
        Some(frames) => frames,
        None => Arc::new(match approval_store {
            Some(store) => store.read_records(&req.session)?,
            None => Vec::new(),
        }),
    };
    let cut_pending_step = match &prior.resolution {
        ReplayResolution::Resume {
            pending: Some(pending),
        }
        | ReplayResolution::AuthRequired {
            pending: Some(pending),
            ..
        } if is_resume => step_attempt_was_args_cut(&approval_frames, pending.step_id),
        _ => false,
    };
    let mut approval_history = None;
    if control.elicitation_plane.is_none() && !cut_pending_step {
        let pending = match &prior.resolution {
            ReplayResolution::AuthRequired { pending, .. }
            | ReplayResolution::Resume { pending } => pending.as_ref(),
            ReplayResolution::ProviderHeld { pending, .. } => Some(pending),
            _ => None,
        };
        approval_gate::require_approval_adapter(
            control,
            pending,
            prior.open_tool_batch.as_ref().is_some_and(|batch| {
                batch.calls.iter().any(|(_, state)| {
                    matches!(
                        state,
                        crate::replay::OpenCallState::Deferred
                            | crate::replay::OpenCallState::Approved
                    )
                })
            }),
            false,
        )?;
        if frames_available {
            let frames = &approval_frames;
            if frames
                .iter()
                .any(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
            {
                let history = approval_gate::History::new(writer, frames.clone())?;
                history.require_installed_plane(control)?;
                approval_history = Some(history);
            }
        }
    }
    // The admission-ack, fired exactly once at the durable-admission point (the RunStarted
    // barrier for a fresh/appended episode, or the resume-state reconstruction for a
    // continued run) before the first provider call. Wrapped in an Option taken on fire.
    let mut on_admitted = on_admitted;
    // Terminal wait intents must be sealed before hook-plane or model configuration checks:
    // recovery is not permission to begin another external call, even on a changed build.
    if !prior.run_finished_durable {
        if let Some(mut state) = prior
            .compaction
            .state()
            .filter(|state| state.open_wait.is_some())
            .cloned()
        {
            if let Some(crate::compaction::runtime::Terminal(reason, error)) =
                crate::compaction::runtime::seal_recorded_wait(writer, &mut state).await?
            {
                let run_id = prior.run_id.clone().unwrap_or_else(|| req.run_id.clone());
                admit(&mut on_admitted, Ok(run_id.clone()));
                control.close_steers();
                let (summary, seq) = finish_run(
                    writer,
                    control,
                    out,
                    &run_id,
                    reason,
                    prior.completed_steps,
                    prior.usage,
                    prior.retries_used,
                    error.map(|error| *error),
                    prior.last_step_finish_reason,
                )
                .await?;
                if let Some(projector) = projector {
                    projector.enqueue_terminal(seq);
                }
                return Ok(summary);
            }
        }
    }
    // The durable manifest, not the resumer's request, decides whether hooks are needed.
    let frozen_hooks = if is_resume {
        prior.manifest.as_ref()
    } else {
        control.manifest.as_ref()
    };
    // A frozen plan the hook runtime cannot read is refused like a missing plane: the waiting
    // admission caller hears the error, and nothing is written or dispatched.
    let hook_plan = match frozen_hooks
        .filter(|manifest| crate::hooks::has_transforms(manifest))
        .map(|manifest| crate::hooks::HookPlan::from_manifest(manifest))
        .transpose()
    {
        Ok(plan) => plan,
        Err(detail) => {
            let error = RunError::Wal(detail);
            admit(&mut on_admitted, Err(error.clone()));
            return Err(error);
        }
    };
    if hook_plan.is_some() && control.hook_plane.is_none() {
        let error =
            RunError::Wal("plan.step_transform_items requires an installed hook plane".into());
        admit(&mut on_admitted, Err(error.clone()));
        return Err(error);
    }
    let hook_bind = req.session.clone();
    let hooks = hook_plan
        .as_ref()
        .zip(control.hook_plane.as_ref())
        .map(|(plan, plane)| crate::hooks::HookRuntime {
            plan,
            plane,
            bind: &hook_bind,
            writer,
            cancel: control.cancel.as_ref(),
            stop: control.stop_requested.as_ref(),
            seal: control.shutdown_seal.as_ref(),
        });
    let requires_approval = hook_plan
        .as_ref()
        .is_some_and(crate::hooks::HookPlan::approves)
        || prior.open_tool_batch.as_ref().is_some_and(|batch| {
            batch.calls.iter().any(|(_, state)| {
                matches!(
                    state,
                    crate::replay::OpenCallState::Deferred | crate::replay::OpenCallState::Approved
                )
            })
        });
    if approval_history.is_none() && (requires_approval || control.elicitation_plane.is_some()) {
        let frames = &approval_frames;
        if requires_approval
            || frames
                .iter()
                .any(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
        {
            approval_history = Some(approval_gate::History::new(writer, frames.clone())?);
        }
    }
    // Resuming or appending inherits the prior episode's cache marker order, even when absent.
    // Only the first episode of a new session selects the current order.
    let marker_order_version = if is_resume {
        prior.marker_order_version
    } else {
        Some(1)
    };
    // Steered messages the WAL holds for the episode being resumed that no step has placed
    // yet. The first step of the continued loop renders them, where replay places them. Empty
    // for a fresh or appended episode: a closed episode placed every steer at its terminal.
    let mut steer_backlog: Vec<Message> = if is_resume && !append_episode {
        std::mem::take(&mut prior.pending_steers)
    } else {
        Vec::new()
    };
    // The run_id the admission-ack reports. A FRESH/APPENDED episode uses the caller's
    // req.run_id (the new episode's id). A RESUME / continue uses the ORIGINAL run_id
    // recovered from the WAL (`prior.run_id`) — the continued run IS the original run, so
    // the coordinator must index + reply with that id, NOT a freshly-minted one. (Resume
    // reuses the original run_id; the loop already continues under it.)
    let resolved_run_id = if is_resume && !append_episode {
        prior.run_id.clone().unwrap_or_else(|| req.run_id.clone())
    } else {
        req.run_id.clone()
    };
    // The authoritative byte-affecting request config. RESUME (continue the last
    // episode) is bound to the frozen run_config (req is IGNORED for these fields: a
    // caller passing different tools/generation must not diverge the rendered bytes).
    // A FRESH run OR an APPENDED new episode resolves from req — a new episode is a new
    // turn that may carry its own tools/model — with tools filtered by policy while preserving
    // request order. Resume restores the admitted stop predicates alongside the
    // byte-affecting config; only `input` remains request-sourced.
    // The provider account this run's credential belongs to, as frozen in its auth selection:
    // the selection this episode supplies (a resume refreezes to it when it differs), else the
    // one a continued run froze earlier. An account-limit refusal names it, because the
    // provider's answer does not say which account ran out.
    let run_account_ref = control
        .auth_selection
        .as_ref()
        .or_else(|| {
            (is_resume && !append_episode)
                .then_some(prior.run_config.as_ref())
                .flatten()
                .and_then(|config| config.auth_selection.as_ref())
        })
        .and_then(|selection| selection.account_ref.clone());
    // The full credential id from the same frozen selection. A credential-scoped terminal names
    // it, because an api key carries no account and only the id says which key to stop using.
    let run_credential_id = control
        .auth_selection
        .as_ref()
        .or_else(|| {
            (is_resume && !append_episode)
                .then_some(prior.run_config.as_ref())
                .flatten()
                .and_then(|config| config.auth_selection.as_ref())
        })
        .map(|selection| selection.credential_id.clone());
    if is_resume && !append_episode {
        req.keep_warm = prior.keep_warm;
        req.on_restart = prior.on_restart;
    }
    let unsupported_logged = std::sync::atomic::AtomicBool::new(false);
    let warm_missed = std::sync::atomic::AtomicBool::new(false);
    let mut warm_config = control
        .keep_warm
        .as_ref()
        .and_then(|_| prior.run_config.clone());
    let cfg = match request_config(req, is_resume && !append_episode, &prior, provider) {
        Ok(config) => config,
        Err(error) => {
            admit(&mut on_admitted, Err(error.clone()));
            return Err(error);
        }
    };
    // Whether this run shows runner tags to the model, and under which rules: both frozen in
    // the run's own config on a resume, so a run started under the stored-ledger rules
    // (`runner-tags/v1`) finishes under them with its exact bytes. A new episode is admitted
    // under the computed rules (`runner-tags/v2`), rendering when the session's frozen plan
    // asks. Its checkpoint is the highest number the session has shown or the stored ledger
    // handed out, so the switch renumbers nothing a reader has seen.
    let (render_tags, computed_tags) = if is_resume && !append_episode {
        let config = prior.run_config.as_ref();
        (
            config.is_some_and(|config| config.render_tags),
            config.and_then(|config| config.computed_tags),
        )
    } else {
        let ledger_high = writer
            .lineage_tags()
            .unwrap_or(crate::tags::LineageTags::unstarted())
            .ledger_high();
        (
            control
                .manifest
                .as_deref()
                .or(prior.manifest.as_deref())
                .is_some_and(crate::tags::plan_renders_tags),
            Some(broca_wal::ComputedTagRules {
                checkpoint: ledger_high.max(broca_protocol::tags::high_after(&prior.prompt)),
            }),
        )
    };
    writer.retain_live_tags(render_tags && computed_tags.is_none());
    let context_limit = if is_resume && !append_episode {
        prior
            .run_config
            .as_ref()
            .and_then(|config| config.context_limit)
    } else {
        control.admitted_context_limit
    };
    let work_class = admitted_work_class(
        req.work_class.as_deref(),
        is_resume,
        append_episode,
        prior
            .run_config
            .as_ref()
            .and_then(|config| config.work_class.as_deref()),
    );
    // The requested reasoning variant follows the same frozen-on-resume rule as the workload
    // class: a resumed episode keeps what it was admitted with, whatever the retry sends.
    let variant = admitted_work_class(
        req.variant.as_deref(),
        is_resume,
        append_episode,
        prior
            .run_config
            .as_ref()
            .and_then(|config| config.variant.as_deref()),
    );
    // The service tier follows the same rule: a resumed episode keeps the tier it was admitted
    // with, whatever the resuming send asks for.
    let service_tier = if is_resume && !append_episode {
        prior
            .run_config
            .as_ref()
            .and_then(|config| config.service_tier)
    } else {
        req.service_tier
    };
    if let Some(projector) = projector {
        projector.set_budgeted(cfg.budget.is_some());
    }
    let mut budget_state = if is_resume && !append_episode {
        BudgetState {
            tool_call_count: prior.tool_call_count,
            fired_nudge_indices: {
                let mut indices = prior.budget_nudges_fired.clone();
                indices.sort_unstable();
                indices
            },
            finalize_fired: prior.budget_finalize_fired,
            finalize_pending: prior.budget_finalize_pending,
            ..BudgetState::default()
        }
    } else {
        BudgetState::default()
    };
    let prior_is_import = prior.run_config.as_ref().is_some_and(|config| {
        config.model.provider_module_id == "import"
            && config.model.model_id == "import"
            && config
                .provider_options
                .get("import_request_digest")
                .is_some_and(serde_json::Value::is_string)
    });
    let prior_last_step_finish_reason = prior.last_step_finish_reason;
    let mut compaction_state = prior.compaction.state().cloned();
    let compaction_notice_floor = prior.compaction_notice_floor;
    // What this run remembers about views between its step boundaries: the ones it will not
    // offer again, the ones a notice already reported, and a fetched view it holds to offer
    // again later.
    let mut boundary_views = crate::compaction::runtime::BoundaryViews::default();
    // The rendered size of the last request under compaction, from which a notice recorded
    // between boundaries estimates the next one.
    let mut notice_request_bytes = 0usize;
    let mut compaction_history = prior.compaction_history.clone();
    let compaction_item = frozen_hooks
        .map(|manifest| crate::compaction::item(manifest))
        .transpose()
        .map_err(RunError::Wal)?
        .flatten();
    let compaction_runtime =
        compaction_item
            .as_ref()
            .map(|(provider, _, _, _)| crate::compaction::runtime::Runtime {
                provider,
                bind: &hook_bind,
                plane: control.compaction_plane.as_ref(),
                writer,
                control,
            });
    let recovered_cancel = is_resume && !append_episode && prior.cancel_at.is_some();
    if recovered_cancel {
        if let Some(inbox) = &control.steer {
            inbox.restore_boundary_cancel();
        }
    }
    // Replay can infer that a no-tool final step or committed hard stop ended the episode even
    // when a crash lost its RunFinished. That inferred LineageIdle is correct for reads, but it is
    // not a durable seal: the append invariant requires a synced RunFinished before RunStarted.
    // The coordinator normally repairs this during actor-open recovery; keep the core append seam
    // safe for alternate callers by sealing the inferred terminal before writing the new episode.
    if append_episode && !prior.run_finished_durable {
        if let ReplayResolution::LineageIdle { last_reason } = &prior.resolution {
            // Commit RunFinished for the earlier run whose ending replay inferred, before
            // trying to start the caller's new run. ResumeStarted may exceed the retained
            // log's byte budget, but its reserved RunFinished must still be synced.
            writer.sync_for_seal().await?;
            writer.record(crate::resume::resume_started_record(
                &prior,
                last_record_seq(writer),
                Vec::new(),
            ));
            let prior_run_id = prior
                .run_id
                .as_ref()
                .ok_or_else(|| RunError::Wal("inferred terminal has no prior run id".into()))?;
            writer.record_terminal(WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: control.now_ms(),
                reason: *last_reason,
                usage: prior.usage,
                retries_used: prior.retries_used,
                error: None,
            })?;
            let last_fsynced_seq = writer.sync_for_seal().await?;
            let _ = send(
                out,
                OutputEvent::Control(ControlUnit::RunFinished {
                    run_id: Some(prior_run_id.clone()),
                    reason: *last_reason,
                    usage: (prior.completed_steps > 0).then_some(prior.usage),
                    final_step_finish_reason: prior_last_step_finish_reason,
                    indeterminate_tool_calls: None,
                    retries_used: prior.retries_used,
                }),
            )
            .await;
            if let Some(projector) = projector {
                projector.enqueue_terminal(last_fsynced_seq);
            }
        }
    }

    // Where each replayed episode's model steps sit in the prompt; read before the start-mode
    // arms below move the reconstructed prompt out of `prior`.
    let episode_step_spans = prior.episode_step_spans.clone();
    let mut render_episodes = prior.render_episodes.clone().unwrap_or_default();
    let tools = Arc::clone(&cfg.tools);
    let mut manifest = ToolManifest::from_shared_tools(Arc::clone(&tools));
    let frozen_plan = if is_resume {
        prior.manifest.as_ref()
    } else {
        control.manifest.as_ref()
    };
    if let Some(frozen_plan) = frozen_plan {
        manifest = manifest
            .with_plan_manifest(frozen_plan)
            .map_err(|error| RunError::Wal(error.to_string()))?;
    }
    let mut stop_when = cfg.stop_when.clone();
    if stop_when.is_empty() {
        stop_when.push(StopCondition::StepCount {
            n: DEFAULT_MAX_STEPS,
        });
    }

    // Three start modes: RESUME the last (unfinished) episode, APPEND a new episode to
    // an idle lineage, or start a FRESH run (empty WAL). `prompt` is seeded
    // accordingly; the prefix that carries across an append is the prior lineage's
    // committed conversation.
    let mut prompt: Vec<Message>;
    // Where this run's own messages begin in `prompt`: the ones tag numbering still has to
    // number. Earlier ones already carry their numbers: stored ones from their records, computed
    // ones from the replay that rebuilt them.
    let own_messages_start: usize;
    // Number of leading messages whose append-only positions are already stamped. Replay and
    // store reconstruction stamp their complete result; fresh/appended input begins after this
    // boundary and is stamped once before its first provider call.
    let mut stamped_len: usize;
    let mut total_usage: Usage;
    let mut total_retries_used: u32;
    let mut last_step_finish_reason: Option<FinishReason>;
    let mut completed_steps: u64;
    let mut pending_turn: Option<PendingTurn> = None;

    // APPEND a new episode: only valid over an IDLE lineage (the last episode reached a
    // durable terminal). If the last episode is still resumable/paused/ambiguous, a new
    // turn must NOT start over it — interrupt rather than write a second active episode.
    let lineage_prefix = if append_episode {
        if !matches!(prior.resolution, ReplayResolution::LineageIdle { .. }) {
            let _ = send(
                out,
                OutputEvent::Control(ControlUnit::RunFinished {
                    run_id: Some(resolved_run_id.clone()),
                    reason: RunFinishReason::Interrupted,
                    usage: (prior.completed_steps > 0).then_some(prior.usage),
                    final_step_finish_reason: prior_last_step_finish_reason,
                    indeterminate_tool_calls: None,
                    retries_used: prior.retries_used,
                }),
            )
            .await;
            // Nothing more is written for this run, so it takes no steers.
            control.close_steers();
            return Ok(RunSummary {
                reason: RunFinishReason::Interrupted,
                paused: None,
                steps: prior.completed_steps,
                usage: prior.usage,
                retries_used: prior.retries_used,
                final_step_finish_reason: prior_last_step_finish_reason,
            });
        }
        // The prior lineage's full conversation becomes this new episode's prefix. Only borrow
        // it here: the vector itself moves into the run's prompt in the fresh/append arm below.
        // A clone would keep a second decoded copy of the whole transcript alive for the entire
        // run (every send to an idle session takes this path), and std::mem::take would leave
        // an empty Vec behind for any later reader. The move compiles only because the resume
        // arms that also consume `prior.prompt` sit in the other branch of the same `if`, so
        // the compiler, not an argument, proves the two never both run.
        prior.prompt.as_slice()
    } else {
        &[]
    };
    let lineage_prefix_stamped_len = lineage_prefix.len();
    // The request is private to this run from here onward, so transfer its input buffer instead
    // of retaining a second deep copy for the provider turn. Resume identity checks below read
    // this moved vector before any append-only system-message normalization can apply.
    let mut episode_input = std::mem::take(&mut req.input);
    if !is_resume || append_episode {
        if let Some(final_input) = control
            .input_hooks
            .as_ref()
            .and_then(|hooks| hooks.final_value.as_ref())
        {
            episode_input = serde_json::from_value(final_input.clone())
                .map_err(|error| RunError::Wal(format!("invalid recorded input hooks: {error}")))?;
        }
    }
    if append_episode && first_system_index(&episode_input).is_some() {
        match first_system_message(lineage_prefix) {
            Some(_) if repeats_prefix_system_messages(lineage_prefix, &episode_input) => {
                // A client naturally re-sends the session's system prompt on every turn.
                // Strip the byte-identical repeat BEFORE it reaches the WAL so replay and
                // resume reconstruct the same single-instruction lineage and keep later
                // rendered prefixes cache-stable. Every system message renders into the
                // top-level system field, so ALL of them must be repeats: one extra would
                // change that field for every later request.
                episode_input.retain(|message| message.role != Role::System);
            }
            None if prior_is_import => {
                // Imports intentionally discard foreign system instructions. The first
                // continuing harness therefore establishes its own durable system prompt.
            }
            Some(_) | None => {
                let err = RunError::SystemPromptChanged;
                admit(&mut on_admitted, Err(err.clone()));
                return Err(err);
            }
        }
    }

    if is_resume && !append_episode {
        // A same-run_id re-drive over an IDLE lineage is only an idempotent retry if the
        // caller resent the SAME input and frozen budget. A different payload here means the
        // client reused a deterministic send id for a new logical send; fail BEFORE the hoisted
        // admission so the coordinator replies a typed error instead of admitting a silent
        // no-op that drops the new prompt. Empty input stays the existing internal resume
        // shape (resume tooling re-drives with no input at all).
        if matches!(&prior.resolution, ReplayResolution::LineageIdle { .. })
            && prior.run_id.as_ref() == Some(&req.run_id)
            && !episode_input.is_empty()
        {
            let Some(durable_input) = last_input.as_ref() else {
                let err = RunError::Wal(
                    "resume: last episode input missing from the WAL (the last RunStarted must carry it)"
                        .into(),
                );
                admit(&mut on_admitted, Err(err.clone()));
                return Err(err);
            };
            let durable_budget = prior
                .run_config
                .as_ref()
                .and_then(|config| config.budget.as_ref());
            let durable_tools = prior
                .run_config
                .as_ref()
                .map(|config| config.tools.as_slice())
                .unwrap_or_default();
            let durable_tools_were_omitted = prior
                .run_config
                .as_ref()
                .is_some_and(|config| config.tools_were_omitted);
            let durable_config = prior
                .run_config
                .as_ref()
                .expect("a durable run id has a frozen config");
            let differing = if durable_input != &episode_input {
                Some("input")
            } else if durable_config.model != req.model {
                Some("model")
            } else if durable_tools != req.tools.as_slice()
                || durable_tools_were_omitted != control.tools_were_omitted
            {
                Some("tools")
            } else if durable_config.tool_choice != req.tool_choice {
                Some("tool_choice")
            } else if durable_config.generation != req.generation {
                Some("generation")
            } else if durable_budget != req.budget.as_ref() {
                Some("budget")
            } else {
                None
            };
            if let Some(differing) = differing {
                let err = RunError::SendIdReuse { differing };
                admit(&mut on_admitted, Err(err.clone()));
                return Err(err);
            }
        }
        if input_cap_changed {
            writer.record(WalRecord::InputTokenCapFrozen {
                limit: input_budget.limit.expect("changed cap is finite"),
            });
            writer.sync().await?;
        }
        // A resume is durably ADMITTED the moment its prior state is reconstructed — its
        // RunStarted is already fsynced (a prior run wrote it). Fire the admission-ack HERE,
        // before the resolution match, so EVERY resume outcome (idempotent no-op / natural
        // finalize / still-paused / ambiguous-interrupt) reports the real, durable run_id and
        // surfaces its terminal via the stream — instead of dropping the ack sender on an
        // early return, which the coordinator can only read as the generic "run ended before
        // admission". Uses the resolved (original) run_id, the id the continued run owns.
        admit(&mut on_admitted, Ok(resolved_run_id.clone()));
        // Every arm below that acts on an episode with no durable terminal writes a resume
        // record first. Both shapes are built here, before the arms move parts of `prior`:
        // one listing what a continuing resume will redo, one for a resume that seals instead.
        // An episode whose terminal is already durable is not resumed and writes neither.
        let resumed_from_seq = last_record_seq(writer);
        let resume_continuing = crate::resume::resume_started_record(
            &prior,
            resumed_from_seq,
            crate::resume::planned_redo(&prior),
        );
        let resume_sealing =
            crate::resume::resume_started_record(&prior, resumed_from_seq, Vec::new());
        let close_out = crate::resume::close_out_open_batch(&prior, |id| {
            prior
                .open_tool_batch
                .as_ref()
                .and_then(|batch| approval_history.as_ref()?.withdrawal(&batch.batch_id, id))
                .map_or(
                    crate::resume::DeferredCloseKind::NotDispatched,
                    crate::resume::DeferredCloseKind::WithdrawElicitation,
                )
        });
        // A durable model response may survive a cut before the terminal barrier. Reconcile
        // its measured input before recovery can dispatch any of that response's tools.
        if !prior.run_finished_durable {
            if let Some(detail) = input_budget.exhausted() {
                writer.record(resume_sealing);
                writer.record(WalRecord::InputTokenBudgetExhausted { detail });
                let pending = match &prior.resolution {
                    ReplayResolution::Resume { pending }
                    | ReplayResolution::AuthRequired { pending, .. } => pending.as_ref(),
                    _ => None,
                };
                // A pending turn holding Prepared or approved calls is closed by `close_out`
                // below instead: it withdraws each unanswered approval card and tells the consent
                // provider an approved call never ran, neither of which the hard stop does.
                if let Some(pending) = pending
                    .filter(|pending| pending.deferred.is_empty() && pending.approved.is_empty())
                {
                    let calls: Vec<_> = pending.assistant.tool_calls().cloned().collect();
                    let existing: Vec<_> = pending
                        .existing
                        .iter()
                        .map(|(_, result)| result.clone())
                        .collect();
                    return finalize_hard_stop(
                        control,
                        &resolved_run_id,
                        pending.step_id,
                        &calls,
                        &existing,
                        pending.batch_id.is_some(),
                        DispatchOutcome::MaxSteps,
                        RunFinishReason::BudgetExhausted,
                        writer,
                        projector,
                        out,
                        prior.completed_steps,
                        prior.usage,
                        prior.retries_used,
                        prior_last_step_finish_reason,
                        &[],
                        control.emit_control_payloads,
                        None,
                        None,
                    )
                    .await;
                }
                for record in &close_out {
                    writer.record(record.clone());
                }
                if !close_out.is_empty() {
                    // The close-out is durable before the consent provider hears that an
                    // approved call will not run, as in every other sealing arm.
                    writer.sync().await?;
                    if let Some(history) = &approval_history {
                        approval_gate::report_close_out(history, &close_out, &req.session, control)
                            .await;
                    }
                }
                let (summary, seq) = finish_run(
                    writer,
                    control,
                    out,
                    &resolved_run_id,
                    RunFinishReason::BudgetExhausted,
                    prior.completed_steps,
                    prior.usage,
                    prior.retries_used,
                    None,
                    prior_last_step_finish_reason,
                )
                .await?;
                if let Some(projector) = projector {
                    projector.enqueue_terminal(seq);
                }
                return Ok(summary);
            }
        }
        // A durable model step that called `submit_result` may survive a cut before the barrier
        // that ends its run. Its calls must never be dispatched on recovery: end the run here
        // exactly as the live path would have.
        if !prior.run_finished_durable {
            let frozen = FrozenStructuredOutput::from_frozen_render_config(Some(
                &provider.render_config_blob(),
            ));
            if let (
                Some(frozen),
                ReplayResolution::Resume {
                    pending: Some(pending),
                },
            ) = (frozen.as_ref(), &prior.resolution)
            {
                let existing: Vec<_> = pending
                    .existing
                    .iter()
                    .map(|(_, result)| result.clone())
                    .collect();
                if let Some(submit) = counted_submit(frozen, &pending.assistant, &existing) {
                    let calls: Vec<_> = pending.assistant.tool_calls().cloned().collect();
                    writer.record(resume_sealing);
                    writer.record(submitted_result_record(
                        &resolved_run_id,
                        frozen,
                        &submit,
                        &calls,
                    ));
                    return finalize_hard_stop(
                        control,
                        &resolved_run_id,
                        pending.step_id,
                        &calls,
                        &existing,
                        pending.batch_id.is_some(),
                        DispatchOutcome::MaxSteps,
                        RunFinishReason::Completed,
                        writer,
                        projector,
                        out,
                        prior.completed_steps,
                        prior.usage,
                        prior.retries_used,
                        prior_last_step_finish_reason,
                        &[],
                        control.emit_control_payloads,
                        Some(&submit.tool_call_id),
                        None,
                    )
                    .await;
                }
            }
        }
        // A crash cannot undo an acknowledged cancel. Repair sent calls without dispatching
        // again; only a durable pending tool turn may finish before a boundary cancellation.
        if recovered_cancel
            && !prior.run_finished_durable
            && (prior.cancel_at == Some(broca_wal::record::RunCancelAt::Now)
                || !matches!(
                    &prior.resolution,
                    ReplayResolution::Resume { pending: Some(pending) }
                        if pending.deferred.is_empty() && pending.approved.is_empty()
                ))
        {
            writer.record(resume_sealing);
            for record in &close_out {
                writer.record(record.clone());
            }
            writer.sync().await?;
            if let Some(history) = &approval_history {
                approval_gate::report_close_out(history, &close_out, &req.session, control).await;
            }
            let reason = match &prior.resolution {
                ReplayResolution::LineageIdle { last_reason } => *last_reason,
                _ => RunFinishReason::Cancelled,
            };
            control.close_steers();
            let (summary, seq) = finish_run(
                writer,
                control,
                out,
                &resolved_run_id,
                reason,
                prior.completed_steps,
                prior.usage,
                prior.retries_used,
                None,
                prior_last_step_finish_reason,
            )
            .await?;
            if let Some(projector) = projector {
                projector.enqueue_terminal(seq);
            }
            return Ok(summary);
        }
        let prepared_open = prior.open_tool_batch.as_ref().is_some_and(|batch| {
            batch.calls.iter().any(|(_, state)| {
                matches!(
                    state,
                    crate::replay::OpenCallState::Deferred | crate::replay::OpenCallState::Approved
                )
            })
        });
        if prepared_open
            && (is_cancelled(control)
                || prior.on_restart != Some(broca_protocol::request::OnRestart::Resume)
                || prior
                    .run_config
                    .as_ref()
                    .and_then(|config| render_config_error(provider, config))
                    .is_some())
        {
            for record in &close_out {
                writer.record(record.clone());
            }
            writer.sync().await?;
            if let Some(history) = &approval_history {
                approval_gate::report_close_out(history, &close_out, &req.session, control).await;
            }
            control.close_steers();
            let (summary, seq) = finish_run(
                writer,
                control,
                out,
                &resolved_run_id,
                if is_cancelled(control) {
                    RunFinishReason::Cancelled
                } else {
                    RunFinishReason::Interrupted
                },
                prior.completed_steps,
                prior.usage,
                prior.retries_used,
                None,
                prior_last_step_finish_reason,
            )
            .await?;
            if let Some(projector) = projector {
                projector.enqueue_terminal(seq);
            }
            return Ok(summary);
        }
        match prior.resolution {
            // The last episode already reached a terminal, so the lineage is idle
            // (NOT sealed — a later turn appends a new episode via the run_id-identity
            // promotion in `run_on_writer`). If run_finished is durable → pure idempotent
            // no-op (a crash-recovery re-drive of the SAME run_id). If NOT (crash between the
            // final terminal turn and run_finished) → the episode ended naturally; FINALIZE
            // it (write run_finished) without re-running the model.
            ReplayResolution::LineageIdle {
                last_reason: reason,
            } => {
                if prior.run_finished_durable {
                    let usage = (prior.completed_steps > 0).then_some(prior.usage);
                    let _ = send(
                        out,
                        OutputEvent::Control(ControlUnit::RunFinished {
                            run_id: Some(resolved_run_id.clone()),
                            reason,
                            usage,
                            final_step_finish_reason: prior_last_step_finish_reason,
                            indeterminate_tool_calls: None,
                            retries_used: prior.retries_used,
                        }),
                    )
                    .await;
                    // Nothing more is written for this run, so it takes no steers.
                    control.close_steers();
                    return Ok(RunSummary {
                        reason,
                        paused: None,
                        steps: prior.completed_steps,
                        usage: prior.usage,
                        retries_used: prior.retries_used,
                        final_step_finish_reason: prior_last_step_finish_reason,
                    });
                }
                // Durable in the terminal's own barrier, just before it.
                writer.record(resume_sealing);
                control.close_steers();
                let (summary, last_fsynced_seq) = finish_run(
                    writer,
                    control,
                    out,
                    &resolved_run_id,
                    reason,
                    prior.completed_steps,
                    prior.usage,
                    prior.retries_used,
                    None,
                    prior_last_step_finish_reason,
                )
                .await?;
                if let Some(projector) = projector {
                    projector.enqueue_terminal(last_fsynced_seq);
                }
                return Ok(summary);
            }
            ReplayResolution::ProviderHeld {
                pending, open_held, ..
            } => {
                if !open_held.is_empty()
                    && pending.deferred.is_empty()
                    && pending.approved.is_empty()
                    && control.provider_held_lifecycle.is_none()
                {
                    let already_paused = approval_frames.iter().rev().any(|frame| {
                        matches!(
                            frame.record,
                            WalRecord::RunPaused {
                                reason: PauseReason::ToolHeld,
                                ..
                            }
                        )
                    });
                    if !already_paused {
                        return pause_run(
                            writer,
                            out,
                            control,
                            resolved_run_id.clone(),
                            PauseReason::ToolHeld,
                            None,
                            prior.completed_steps,
                            prior.usage,
                            prior.retries_used,
                        )
                        .await;
                    }
                    control.close_steers();
                    let _ = send(
                        out,
                        OutputEvent::Control(ControlUnit::Paused {
                            run_id: resolved_run_id.clone(),
                            reason: PauseReason::ToolHeld,
                            error: None,
                        }),
                    )
                    .await;
                    return Ok(RunSummary {
                        reason: RunFinishReason::Interrupted,
                        paused: Some(PauseReason::ToolHeld),
                        steps: prior.completed_steps,
                        usage: prior.usage,
                        retries_used: prior.retries_used,
                        final_step_finish_reason: None,
                    });
                }
                if let Some(error) = prior
                    .run_config
                    .as_ref()
                    .and_then(|config| render_config_error(provider, config))
                {
                    writer.record(resume_sealing);
                    let (summary, head) = finish_run(
                        writer,
                        control,
                        out,
                        &resolved_run_id,
                        RunFinishReason::Interrupted,
                        prior.completed_steps,
                        prior.usage,
                        prior.retries_used,
                        Some(error),
                        prior_last_step_finish_reason,
                    )
                    .await?;
                    if let Some(projector) = projector {
                        projector.enqueue_terminal(head);
                    }
                    return Ok(summary);
                }
                writer.record(resume_continuing);
                writer.sync().await?;
                prompt = prior.prompt;
                own_messages_start = prompt.len();
                stamped_len = prompt.len();
                total_usage = prior.usage;
                total_retries_used = prior.retries_used;
                last_step_finish_reason = prior_last_step_finish_reason;
                completed_steps = prior.completed_steps;
                pending_turn = Some(pending);
            }
            // The last episode is PAUSED awaiting (re-)auth — a NON-terminal durable
            // state. The actual re-auth-and-continue (resuming the SAME run_id from the
            // last durable boundary) is a later phase; here we must NOT write a
            // RunFinished (that would SEAL a resumable run, turning the pause into a
            // terminal). Surface the still-paused run and return without sealing it, so
            // a later re-auth resume still sees the pause.
            ReplayResolution::AuthRequired {
                reason, pending, ..
            } => {
                // The last episode is PAUSED awaiting re-auth. A CANCEL while paused WINS
                // (constraint: a user cancelling a paused-for-auth run wants it gone, not
                // stuck awaiting auth forever) — finalize it as RunFinished{Cancelled},
                // sealing the resumable pause into a terminal.
                if is_cancelled(control) {
                    control.close_steers();
                    let (summary, last_fsynced_seq) = finish_run(
                        writer,
                        control,
                        out,
                        &resolved_run_id,
                        RunFinishReason::Cancelled,
                        prior.completed_steps,
                        prior.usage,
                        prior.retries_used,
                        None,
                        prior_last_step_finish_reason,
                    )
                    .await?;
                    if let Some(projector) = projector {
                        projector.enqueue_terminal(last_fsynced_seq);
                    }
                    return Ok(summary);
                }
                // WITHOUT the caller's "auth resolved" signal, stay paused: surface it
                // and return (the durable RunPaused keeps the run resumable). WITH the
                // signal, CONTINUE the SAME run_id from the last durable boundary — seed
                // from the reconstructed state and fall through to the loop, exactly like
                // Resume, writing NO new RunStarted (the run is the same one, not a new
                // episode).
                if !auth_resolved && reason != PauseReason::Restart {
                    let _ = send(
                        out,
                        OutputEvent::Control(ControlUnit::Paused {
                            run_id: resolved_run_id.clone(),
                            reason,
                            error: None,
                        }),
                    )
                    .await;
                    // Nothing more is written for this run, so it takes no steers.
                    control.close_steers();
                    return Ok(RunSummary {
                        reason: RunFinishReason::Interrupted,
                        paused: Some(reason),
                        steps: prior.completed_steps,
                        usage: prior.usage,
                        retries_used: prior.retries_used,
                        final_step_finish_reason: None,
                    });
                }
                // Auth resolved → continue. A pause before the first provider call may have
                // frozen an incomplete credential-bound render config (Antigravity has no
                // project_id until credential.get succeeds). In that one state, replace and
                // sync the config before the first call. Any drift after a call began remains
                // unsafe and interrupts exactly like an ordinary resume, with the typed
                // drift error naming what diverged.
                let mut refreeze_recorded = false;
                if let Some(error) = prior
                    .run_config
                    .as_ref()
                    .and_then(|rc| render_config_error(provider, rc))
                {
                    if prior.provider_call_started
                        || error.provider_code.as_deref()
                            == Some("frozen_render_config_unverifiable")
                    {
                        let _ = send(
                            out,
                            OutputEvent::Control(ControlUnit::Error {
                                error: error.clone().into(),
                            }),
                        )
                        .await;
                        control.close_steers();
                        let (summary, last_fsynced_seq) = finish_run(
                            writer,
                            control,
                            out,
                            &resolved_run_id,
                            RunFinishReason::Interrupted,
                            prior.completed_steps,
                            prior.usage,
                            prior.retries_used,
                            Some(error),
                            prior_last_step_finish_reason,
                        )
                        .await?;
                        if let Some(projector) = projector {
                            projector.enqueue_terminal(last_fsynced_seq);
                        }
                        return Ok(summary);
                    }
                    if let Some(config) = &mut warm_config {
                        config.renderer_version = provider.renderer_version();
                        config.frozen_render_config = provider.render_config_blob();
                    }
                    writer.record(WalRecord::RunConfigRefrozen {
                        ts_ms: control.now_ms(),
                        renderer_version: provider.renderer_version(),
                        frozen_render_config: provider.render_config_blob(),
                    });
                    refreeze_recorded = true;
                }
                let frozen_auth_selection = prior
                    .run_config
                    .as_ref()
                    .and_then(|config| config.auth_selection.as_ref());
                if let Some(auth_selection) = control
                    .auth_selection
                    .as_ref()
                    .filter(|selection| Some(*selection) != frozen_auth_selection)
                {
                    if let Some(config) = &mut warm_config {
                        config.auth_selection = Some(auth_selection.clone());
                    }
                    writer.record(WalRecord::RunAuthRefrozen {
                        ts_ms: control.now_ms(),
                        auth_selection: auth_selection.clone(),
                    });
                    refreeze_recorded = true;
                }
                if reason == PauseReason::Restart {
                    writer.record(resume_continuing);
                    writer.sync().await?;
                } else if refreeze_recorded {
                    writer.sync().await?;
                }
                prompt = prior.prompt;
                own_messages_start = prompt.len();
                stamped_len = prompt.len();
                total_usage = prior.usage;
                total_retries_used = prior.retries_used;
                last_step_finish_reason = prior_last_step_finish_reason;
                completed_steps = prior.completed_steps;
                pending_turn = pending;
            }
            // A call had an ambiguous outcome: its side effect may have applied, so it is never
            // sent again and no model call is made. Close the open batch instead, pairing each
            // such call with an `outcome_unknown` result (and each planned call that never left
            // the process with an interrupted one), in one barrier with the resume record. Then
            // seal the episode Interrupted in a barrier of its own. The owner's next send renders
            // every call of that batch paired with its result. A crash between the two barriers
            // resumes here again, and the close-out then writes nothing for a call that already
            // has a result.
            ReplayResolution::Interrupted { .. } => {
                writer.record(resume_sealing);
                let results: Vec<ToolResult> = close_out
                    .iter()
                    .filter_map(|record| match record {
                        WalRecord::ToolResult { result, .. } => Some(result.as_ref().clone()),
                        _ => None,
                    })
                    .collect();
                for record in &close_out {
                    writer.record(record.clone());
                }
                writer.sync().await?;
                if let Some(history) = &approval_history {
                    approval_gate::report_close_out(history, &close_out, &req.session, control)
                        .await;
                }
                // Emitted after the barrier, like every live tool result, so a caller never
                // sees a result a later resume would report as lost.
                if control.emit_control_payloads {
                    for result in results {
                        let _ = send(
                            out,
                            OutputEvent::Control(ControlUnit::ToolResult { result }),
                        )
                        .await;
                    }
                }
                control.close_steers();
                let (summary, last_fsynced_seq) = finish_run(
                    writer,
                    control,
                    out,
                    &resolved_run_id,
                    RunFinishReason::Interrupted,
                    prior.completed_steps,
                    prior.usage,
                    prior.retries_used,
                    None,
                    prior_last_step_finish_reason,
                )
                .await?;
                if let Some(projector) = projector {
                    projector.enqueue_terminal(last_fsynced_seq);
                }
                return Ok(summary);
            }
            ReplayResolution::Resume { pending } => {
                // The resumed run will make more provider calls, so the provider
                // MUST render byte-identical to the original or the prompt cache
                // busts (and a structural change — e.g. the interleaved-thinking
                // echo or tool-id mapping — could make the conversation
                // provider-invalid). Confirm the provider's render config still
                // matches what was frozen at run start; if it drifted, stop for
                // review rather than silently render different bytes. (Mirrors the
                // ambiguous-outcome interrupt policy.)
                if let Some(error) = prior
                    .run_config
                    .as_ref()
                    .and_then(|rc| render_config_error(provider, rc))
                {
                    let _ = send(
                        out,
                        OutputEvent::Control(ControlUnit::Error {
                            error: error.clone().into(),
                        }),
                    )
                    .await;
                    // This resume seals instead of redoing anything; the record rides the
                    // terminal's barrier.
                    writer.record(resume_sealing);
                    control.close_steers();
                    let (summary, last_fsynced_seq) = finish_run(
                        writer,
                        control,
                        out,
                        &resolved_run_id,
                        RunFinishReason::Interrupted,
                        prior.completed_steps,
                        prior.usage,
                        prior.retries_used,
                        Some(error),
                        prior_last_step_finish_reason,
                    )
                    .await?;
                    if let Some(projector) = projector {
                        projector.enqueue_terminal(last_fsynced_seq);
                    }
                    return Ok(summary);
                }
                // Make the resume record durable before the first provider call or tool
                // dispatch it announces, so no effect of this resume precedes its record.
                writer.record(resume_continuing);
                writer.sync().await?;
                // Seed from the reconstructed state; do NOT re-write the run-start
                // record — it's already durable and authoritative.
                prompt = prior.prompt;
                own_messages_start = prompt.len();
                stamped_len = prompt.len();
                total_usage = prior.usage;
                total_retries_used = prior.retries_used;
                last_step_finish_reason = prior_last_step_finish_reason;
                completed_steps = prior.completed_steps;
                pending_turn = pending;
            }
        }
        let _ = send(
            out,
            OutputEvent::Control(ControlUnit::RunStarted {
                run_id: resolved_run_id.clone(),
                submission_id: req.submission_id.clone(),
            }),
        )
        .await;
    } else {
        // FRESH run: write the ONE atomic run_started record (config + input) and
        // fsync it BEFORE the first provider call — this is the config a later
        // resume treats as authoritative. Atomic so a torn start is never a partial
        // (config-or-input missing). Share the ordered, policy-filtered tools with the WAL record
        // through its durability barrier so resume preserves the same `tools` array without
        // retaining a duplicate manifest graph.
        // Share the input allocation with the WAL record through its durability barrier. Once
        // synced, reclaim the vector when no observer retained the record; otherwise the mutable
        // prompt takes its own copy only after the record is durable.
        let episode_input = Arc::new(episode_input);
        let (run_started_seq, input_tags) = writer.record_tagged(WalRecord::RunStarted {
            written_at_ms: req
                .input_written_at_ms
                .or_else(|| {
                    req.submission_id
                        .is_none()
                        .then(|| control.now_ms())
                        .flatten()
                })
                .filter(|at| *at != 0),
            tags: None,
            session_class: req.session_class.clone(),
            retention: req.retention,
            hooks: control.input_hooks.clone().map(|mut hooks| {
                // RunStarted.input holds the rendered final value, unlike Queued.request.
                hooks.final_value = None;
                hooks
            }),
            on_restart: req.on_restart,
            keep_warm: req.keep_warm,
            marker_order_version,
            // Must-understand: a manifest makes this a gated frame requiring `plan-manifest/v1`,
            // and the WAL raises the feature floor to that name before writing it.
            manifest: control.manifest.as_deref().cloned().map(Box::new),
            // Must-understand: the WAL raises the feature floor before this frame is written,
            // and a scoped record is always framed as a gated frame requiring `scope/v1`.
            scope: req.scope.clone(),
            send_id: req.send_id.clone(),
            request_identity_digest: req.request_identity_digest.clone(),
            // Informational: recorded for dashboards and never read back into a request.
            title: req.title.clone(),
            project_id: req.project_id.clone(),
            principal: req.principal.clone(),
            ts_ms: control.now_ms(),
            run_id: resolved_run_id.clone(),
            session: req.session.clone(),
            config: remember_warm_config(
                RunConfig {
                    turn_directives_v1: req.directives.clone(),
                    render_tags,
                    computed_tags,
                    model: cfg.model.clone(),
                    tools: Arc::clone(&tools),
                    tool_choice: cfg.tool_choice.clone(),
                    generation: cfg.generation.clone(),
                    response_format: cfg.response_format.clone(),
                    provider_options: cfg.provider_options.clone(),
                    cache_tiers: serde_json::Value::Null,
                    context_epoch: 0,
                    renderer_version: provider.renderer_version(),
                    frozen_render_config: provider.render_config_blob(),
                    // Freeze the resolved credential id so a resume retrieves the same credential,
                    // never choosing its method again from configuration or account routing.
                    // The frozen credential id is metadata, not a provider render input.
                    auth_selection: control.auth_selection.clone(),
                    tools_were_omitted: control.tools_were_omitted,
                    budget: cfg.budget.clone(),
                    context_limit,
                    work_class,
                    variant: variant.clone(),
                    service_tier,
                    stop_when: cfg.stop_when.clone(),
                    // Before any record is written, accepting the send resolves context management
                    // into req.context_management: the session's inherited mode, or on its
                    // first send the caller's choice, project/global broca.jsonc defaults,
                    // then "none", in that order.
                    context_management: req.context_management.clone(),
                },
                &mut warm_config,
                control.keep_warm.is_some(),
            ),
            input: Arc::clone(&episode_input),
            origin: broca_wal::RunOrigin::Fresh,
            // If this run consumes a durable queued submission, record its id so the
            // submission is provably marked CONSUMED (the pending-queue derivation excludes
            // a submission once a RunStarted references it — so a crash after this barrier
            // never re-drains it). `None` for a directly-started run.
            submission_id: req.submission_id.clone(),
        });
        if input_cap_changed {
            writer.record(WalRecord::InputTokenCapFrozen {
                limit: input_budget.limit.expect("changed cap is finite"),
            });
        }
        if let Err(error) = writer.sync().await {
            let error = RunError::from(error);
            admit(&mut on_admitted, Err(error.clone()));
            return Err(error);
        } // sync has committed the configuration before the first provider request.
        let run_started_seq = run_started_seq.into_result()?;
        if let Some(reporter) = &control.account_reporter {
            reporter.run_started(writer.lineage_id(), run_started_seq);
        }

        // The run is now durably ADMITTED — RunStarted+config+input are fsynced. Signal the
        // coordinator so it can reply `active{run_id}` + commit the durable run-index row
        // BEFORE the first provider call (a racing cancel/status then always resolves the
        // run_id). Fired at the EXISTING barrier — no new barrier, loop bytes unchanged.
        admit(&mut on_admitted, Ok(resolved_run_id.clone()));

        let _ = send(
            out,
            OutputEvent::Control(ControlUnit::RunStarted {
                run_id: resolved_run_id.clone(),
                submission_id: req.submission_id.clone(),
            }),
        )
        .await;

        // Fresh: prompt = the new input. Append: prompt = the prior lineage's
        // conversation (the prefix) followed by this episode's new input. Counters
        // reset either way — they are per-episode, and an appended episode is a new
        // episode (its own step numbering / usage).
        render_episodes.push(broca_protocol::request::EpisodeRenderInput {
            prompt_start: lineage_prefix_stamped_len,
            first_user: episode_input
                .iter()
                .position(|m| m.role == Role::User)
                .map(|i| lineage_prefix_stamped_len + i),
            frozen_config: provider.render_config_blob(),
        });
        // Fresh: the WAL holds no prior episode, so there is no prefix to carry over.
        prompt = if append_episode {
            prior.prompt
        } else {
            Vec::new()
        };
        let input_start = prompt.len();
        own_messages_start = input_start;
        prompt.extend(Arc::unwrap_or_clone(episode_input));
        // The writer tagged the input as it appended the RunStarted; the prompt's copies carry
        // the same tags, as replay attaches them.
        for (message, tags) in prompt[input_start..]
            .iter_mut()
            .zip(input_tags.iter().flatten())
        {
            crate::tags::attach(message, Some(tags));
        }
        stamped_len = lineage_prefix_stamped_len;
        total_usage = Usage::default();
        total_retries_used = 0;
        last_step_finish_reason = None;
        completed_steps = 0;
    }

    // For a RESUME / continue / idle-finalize path, RunStarted is ALREADY durable (it was
    // written by the original run), so the run is admitted by construction — signal Ok now
    // with the ORIGINAL run_id (a no-op if the fresh branch already fired). This guarantees
    // the coordinator's admission await always completes for every start mode, and a resume
    // indexes/replies the original run_id, not a freshly-minted one.
    admit(&mut on_admitted, Ok(resolved_run_id.clone()));

    if !is_resume || append_episode {
        if let Some(error) = control.pre_user_error.clone() {
            control.close_steers();
            let (summary, last_fsynced_seq) = finish_run(
                writer,
                control,
                out,
                &resolved_run_id,
                RunFinishReason::Error,
                completed_steps,
                total_usage,
                total_retries_used,
                Some(error),
                last_step_finish_reason,
            )
            .await?;
            if let Some(projector) = projector {
                projector.enqueue_terminal(last_fsynced_seq);
            }
            return Ok(summary);
        }
    }

    // Authoritative re-auth-at-start: the caller already knows (before any provider call)
    // that the credential needs re-auth. The run is now durably admitted, so pause it for
    // re-auth here — write the durable RunPaused{AuthRequired} and return, exactly like a
    // mid-run Auth-class failure, so the user re-auths and the run continues under the SAME
    // run id. Done AFTER admission (so it's a real resumable run with a run id) and BEFORE
    // the loop (so no doomed provider call is made). Honored UNCONDITIONALLY — it is an
    // explicit caller instruction, so the caller (the executor) is the sole decider: it
    // sets this only when its credential source is authoritative that re-auth is needed.
    // In particular, on a resume-after-auth where the credential is STILL dead, the caller
    // sets it again and we re-pause here rather than make a doomed provider call.
    if let Some(error) = control.pause_for_auth_at_start.clone() {
        return pause_run(
            writer,
            out,
            control,
            resolved_run_id.clone(),
            broca_protocol::PauseReason::AuthRequired,
            Some(error),
            completed_steps,
            total_usage,
            total_retries_used,
        )
        .await;
    }

    // A resumed prompt comes from raw WAL results, while an uninterrupted run retained the
    // model-only repetition guidance in memory. Rebuild the observer from only this episode's
    // completed steps, then reproduce that guidance before either path renders another request.
    let committed_episode_steps = completed_steps.saturating_sub(u64::from(pending_turn.is_some()));
    let episode_history_start = episode_history_start(&prompt, committed_episode_steps);
    let mut originals = compaction_runtime
        .as_ref()
        .map(|_| crate::compaction::OriginalMessages::default());
    let mut closing_tail = Vec::new();
    let mut raw_len = prompt.len();
    // The prompt can contain system messages anywhere, not just at its beginning.
    // Index their positions once as the append-only prompt grows instead of rescanning history.
    let mut system_positions = Vec::new();
    let mut system_indexed_len = 0;
    // Earlier episodes' tool results were sent with the guidance their own episode produced.
    // Every later request must render them the same way, or it edits a message the provider has
    // already seen (a cache miss everywhere, and a rejected request where signed thinking is bound
    // to its prefix). Each episode folds with its own fresh observer, as it did live.
    restore_earlier_episode_guidance(
        &mut prompt,
        episode_step_spans.as_deref(),
        episode_history_start,
        originals.as_mut(),
    );
    let mut repetition_observer = restore_guidance_with_originals(
        &mut prompt[episode_history_start..],
        episode_history_start,
        originals.as_mut(),
    );

    if let Some(history) = approval_history.as_mut() {
        Box::pin(approval_gate::admission(
            history,
            writer,
            &req.session,
            control,
            tool_plane,
        ))
        .await?;
    }
    // If resuming an incomplete tool turn, COMPLETE it before the main loop:
    // re-dispatch only the never-left calls, reuse the durable terminals, append
    // the now-complete tool turn to the prompt.
    if let Some(pending) = pending_turn.take() {
        let budget_firings =
            due_budget_firings(cfg.budget.as_ref(), &budget_state, prompt.len() as u64 + 2);
        let interrupt = Box::pin(complete_pending_turn(
            req,
            &resolved_run_id,
            pending,
            cut_pending_step,
            &manifest,
            tool_plane,
            approval,
            writer,
            &mut prompt,
            originals.as_mut(),
            // The resumed turn belongs to this (last) episode → stamp its model, matching
            // what replay stamps for the same turn (C7 byte-identity).
            cfg.model.clone(),
            computed_tags.is_none(),
            &budget_firings,
            control.emit_control_payloads,
            control.stop_requested.as_ref(),
            hooks.as_ref(),
            &mut repetition_observer,
            warm_wait(
                control,
                warm_config.as_ref(),
                req,
                &resolved_run_id,
                &unsupported_logged,
                &warm_missed,
            ),
            out,
            control,
            approval_history.as_mut(),
            &approval_frames,
        ))
        .await?;
        raw_len = prompt.len();
        if !interrupt.1.is_empty() || interrupt.2 {
            return pause_approval(
                writer,
                out,
                control,
                &req.session,
                &resolved_run_id,
                completed_steps,
                total_usage,
                total_retries_used,
                &interrupt.1,
                interrupt.2,
            )
            .await;
        }
        if let Some(projector) = projector {
            let safe_h = writer.sync().await?;
            projector.enqueue(safe_h);
        }
        apply_committed_budget_firings(
            &mut budget_state,
            cfg.budget.as_ref(),
            &resolved_run_id,
            &budget_firings,
        );
        append_budget_firings(writer, &mut prompt, &resolved_run_id, &budget_firings, out).await;
        if let Some(reason) = interrupt.0 {
            // The completed turn was itself ambiguous → interrupt.
            control.close_steers();
            let (summary, last_fsynced_seq) = finish_run(
                writer,
                control,
                out,
                &resolved_run_id,
                reason,
                completed_steps,
                total_usage,
                total_retries_used,
                None,
                last_step_finish_reason,
            )
            .await?;
            if let Some(projector) = projector {
                projector.enqueue_terminal(last_fsynced_seq);
            }
            return Ok(summary);
        }
    }

    // The typed terminal error, set only when the loop breaks with `Error`. Persisted on
    // the durable RunFinished so the control-stream projection can surface the cause.
    let mut terminal_error: Option<ProviderError> = None;
    // Reuse one immutable request snapshot for the whole turn loop. The renderer owns a
    // cloned Arc while its blocking job runs, so every append below first proves that no
    // in-flight render can force a copy of the durable prompt.
    // How much of the prompt has had its runner tags rendered (see `render_tags`).
    let mut tags_rendered_len = 0;
    // Number this run's messages as they join the prompt, by the run's frozen rules, exactly
    // as a replay of the WAL numbers them.
    let own_messages_start = own_messages_start.min(prompt.len());
    let mut tag_numbering = crate::tags::TagNumbering::for_run(
        &prompt[..own_messages_start],
        crate::tags::rules_for(computed_tags, render_tags, &provider.render_config_blob()),
        computed_tags,
    );
    let mut opts = Arc::new(CallOptions {
        ephemeral_session: retention_lineage()?,
        render_episodes: Arc::new(render_episodes.into()),
        prompt,
        tools: Arc::clone(&tools),
        tool_choice: Arc::new(cfg.tool_choice.clone()),
        response_format: Arc::new(cfg.response_format),
        generation: Arc::new(cfg.generation),
        provider_options: Arc::new(cfg.provider_options),
        step_context: None,
        target_model: Some(cfg.model.clone()),
        marker_order_version,
        declared_markers: Vec::new(),
    });

    // The structured output this run froze at admission, read back from the provider's frozen
    // render config so a resumed run decides exactly as the run that started.
    let mut structured_output =
        FrozenStructuredOutput::from_frozen_render_config(Some(&provider.render_config_blob()));
    // The joined text of the final message of a `final_text` run that ended because the model
    // answered without tools. `None` for every other end, which reports the result absent.
    // Only a `final_text` run copies the text, so other runs keep a single message graph.
    let wants_final_text = structured_output
        .as_ref()
        .is_some_and(|frozen| frozen.mode == broca_protocol::structured_output::Mode::FinalText);
    let mut natural_final_text: Option<String> = None;
    let mut interrupt_reason = None;
    let reason = loop {
        input_budget.limit = writer.input_token_limit().or(input_budget.limit);
        // A session budget is independent of episode counters and reminder inheritance.
        if let Some(detail) = input_budget.exhausted() {
            writer.record(WalRecord::InputTokenBudgetExhausted { detail });
            break RunFinishReason::BudgetExhausted;
        }
        // Cooperative cancel at the TURN boundary (a safe WAL boundary: the prior turn's
        // batch is complete and committed, so there is no pending tool_use to strand).
        // Stop cleanly as Cancelled. Checked here, before the next model step, so a cancel
        // that arrives during a step is honored at the next boundary.
        // A crash-cut can retain the completed tool turn while losing one or more coalesced
        // BudgetFired records from the same barrier. Re-derive any missing firing from the
        // durable call count before rendering the next request.
        let recovered_firings =
            due_budget_firings(cfg.budget.as_ref(), &budget_state, opts.prompt.len() as u64);
        if !recovered_firings.is_empty() {
            record_budget_firings(writer, &resolved_run_id, &recovered_firings);
            let safe_h = writer.sync().await?;
            if let Some(projector) = projector {
                projector.enqueue(safe_h);
            }
            apply_committed_budget_firings(
                &mut budget_state,
                cfg.budget.as_ref(),
                &resolved_run_id,
                &recovered_firings,
            );
            append_budget_firings(
                writer,
                &mut mutable_call_options(&mut opts).prompt,
                &resolved_run_id,
                &recovered_firings,
                out,
            )
            .await;
        }
        // Cancellation is observed only after any firing implied by the durable preceding
        // turn is itself durable and visible. This prevents a cancel race from creating a
        // live-only budget message or suppressing a message replay must reconstruct.
        if is_cancelled(control) || boundary_cancelled(control) || recovered_cancel {
            break RunFinishReason::Cancelled;
        }
        // A stop request ends the run here, before this step writes anything: no `StepStarted`,
        // no model call, and no steered message taken into a step. The previous turn is complete
        // and durable, so there is nothing left in flight. A resume-enabled episode pauses;
        // other episodes end Interrupted. Queued steers stay in the WAL for resume or close.
        if stop_is_requested(control.stop_requested.as_ref()) {
            let policy = if is_resume && !append_episode {
                prior.on_restart
            } else {
                req.on_restart
            };
            if policy.unwrap_or_default() == broca_protocol::request::OnRestart::Resume {
                return pause_run(
                    writer,
                    out,
                    control,
                    resolved_run_id.clone(),
                    PauseReason::Restart,
                    None,
                    completed_steps,
                    total_usage,
                    total_retries_used,
                )
                .await;
            }
            interrupt_reason = Some("runner_shutdown".to_owned());
            break RunFinishReason::Interrupted;
        }
        let finalize_turn = budget_state.finalize_pending;
        // Stage the model event for decisions; publish these facts only after the turn barrier.
        let mut step_budget_state = budget_state.clone();
        let step_id = StepId(completed_steps + 1);

        // Mark the step boundary durably BEFORE the live StepStarted unit is published
        // (run_step emits it to `out`). Recording first gives the unit a durable cursor,
        // so the control stream is fully projectable from the WAL (a re-subscriber
        // replays it). Replay places every steered message written before this record at
        // this point, so the same messages are appended to the live prompt here first.
        // The inbox's lock makes "which steers are before this StepStarted" the same question
        // for the WAL and for this prompt (see `crate::steer`).
        // Queued sends with an ordered `delivery` list are decided under the same lock: one whose
        // first possible mode is `steer` joins this step after the steered messages, and one that
        // expired ends. Their records precede `StepStarted`, exactly where replay places them.
        let mut steered = match &control.steer {
            Some(inbox) => match inbox.try_take_at_step_ordered(
                &resolved_run_id,
                control.now_ms().unwrap_or(0),
                |records| {
                    let assigned = records
                        .into_iter()
                        .map(|record| writer.record_tagged(record).1)
                        .collect();
                    writer.record(WalRecord::StepStarted { step_id });
                    assigned
                },
            ) {
                Some(steered) => steered,
                None => break RunFinishReason::Cancelled,
            },
            None => {
                writer.record(WalRecord::StepStarted { step_id });
                Vec::new()
            }
        };
        if control.emit_display_context {
            let _ = send(
                out,
                OutputEvent::DisplayContext {
                    ordinal: writer.display_ordinal(),
                },
            )
            .await;
        }
        steer_backlog.append(&mut steered);
        if !steer_backlog.is_empty() {
            mutable_call_options(&mut opts)
                .prompt
                .append(&mut steer_backlog);
        }

        // Ordinals are positional metadata over this append-only prompt. Replay/merge and prior
        // loop iterations already stamped the stable prefix, so touch only entries appended since
        // the previous provider call. Assigned values remain their absolute 0-based positions.
        stamp_raw_suffix(
            &mut mutable_call_options(&mut opts).prompt,
            &mut stamped_len,
        );
        raw_len = opts.prompt.len();
        system_positions.extend(
            opts.prompt[system_indexed_len..]
                .iter()
                .enumerate()
                .filter_map(|(i, message)| {
                    (message.role == Role::System).then_some(system_indexed_len + i)
                }),
        );
        system_indexed_len = opts.prompt.len();
        tag_numbering
            .number_new(&mut mutable_call_options(&mut opts).prompt)
            .map_err(RunError::TagNumbering)?;
        // A run frozen with `render_tags` shows each tagged user and tool-result text part
        // with its `§N§ ` prefix, and on Anthropic Messages each assistant text part's tag too.
        // Each message is rendered once, the first time a request includes it, and depends only
        // on its stored text and tags, so every later request repeats the same bytes. The
        // model prompt keeps its prefixes; compaction reads saved unprefixed originals when
        // those tags changed a message's content.
        // Under the computed rules a compaction provider's first Setup may anchor the numbers
        // at its checkpoint, so nothing of this run renders until Setup has answered (below).
        let setup_pending =
            computed_tags.is_some() && compaction_runtime.is_some() && compaction_state.is_none();
        if !setup_pending {
            render_new_tags(
                &mut mutable_call_options(&mut opts).prompt,
                &mut tags_rendered_len,
                render_tags,
                &provider.render_config_blob(),
                originals.as_mut(),
            );
        }

        // The finalize turn keeps the declared toolset BYTE-IDENTICAL and disables calls via
        // tool_choice instead of dropping the array: re-encoding the tools block on the last
        // turn of a long run would re-bill the whole prompt prefix. Families without a "none"
        // spelling degrade to the same bytes an empty toolset would produce.
        let opts_mut = mutable_call_options(&mut opts);
        opts_mut.tool_choice = Arc::new(if finalize_turn {
            ToolChoice::None
        } else {
            cfg.tool_choice.clone()
        });
        // Run/step identity for families whose body embeds a deterministic per-request id.
        // The run id is stable and the step index is replay-derived, so resumed bytes match.
        opts_mut.step_context = Some(broca_protocol::StepContext {
            run_id: resolved_run_id.0.clone(),
            step_index: step_id.0,
            session_address: Some(session_addr(&req.session)),
            credential_id: run_credential_id.clone(),
            account_ref: run_account_ref.clone(),
        });
        let mut model_opts = None;
        if let Some(runtime) = &compaction_runtime {
            // A legacy WAL mints its lineage on the first append. StepStarted is only
            // queued above, so wait for its barrier before reading the writer's identity.
            // A WAL backend that still cannot identify this lineage cannot host compaction.
            writer.sync().await?;
            let lineage_id = writer
                .lineage_id()
                .ok_or_else(|| {
                    RunError::Wal(
                        "compaction requires a WAL lineage id after the durable StepStarted barrier"
                            .into(),
                    )
                })?
                .to_string();
            if compaction_state.is_none() {
                let raw =
                    crate::compaction::StoredTranscript::new(&opts.prompt, originals.as_ref())
                        .setup_messages();
                let (_, preset, params, composition) =
                    compaction_item.as_ref().expect("runtime item");
                let mut setup = crate::compaction_provider::setup::SetupRequest::new(
                    &req.session.session,
                    &req.session.harness,
                    format!("{}:setup:{}", lineage_id, writer.next_record_seq()),
                    composition.clone(),
                    &cfg.model.model_id,
                    crate::compaction::now(),
                )
                .with_item(preset.clone(), params.clone())
                .with_model_details(
                    variant.clone(),
                    context_limit,
                    opts.generation.max_output_tokens,
                )
                .with_ride_wait_max_ms(crate::compaction::runtime::RIDE_WAIT_MAX_MS);
                setup.lineage_id = Some(lineage_id.clone());
                setup.newest = raw
                    .last()
                    .and_then(|message| message.ordinal)
                    .map(|ordinal| crate::compaction_provider::lineage::MessageRef {
                        ordinal,
                        mid: format!("m{ordinal}"),
                    });
                // Keep compaction sub-futures off the enclosing run frame. They still
                // borrow its single writer and are polled by this run, not spawned.
                match Box::pin(runtime.setup(setup, &raw)).await {
                    Ok(mut state) => {
                        state.history = compaction_history.clone();
                        // A moved session's Setup checkpoint anchors the numbers at its cut
                        // before anything of this run is rendered with a number.
                        if let (Some(_), Some(checkpoint)) = (computed_tags, &state.tag_checkpoint)
                        {
                            tag_numbering
                                .anchor_setup(
                                    &mut mutable_call_options(&mut opts).prompt,
                                    0,
                                    &[],
                                    "",
                                    checkpoint,
                                )
                                .map_err(RunError::TagNumbering)?;
                        }
                        compaction_state = Some(state);
                    }
                    Err(error) => {
                        terminal_error = Some(*error);
                        break RunFinishReason::Error;
                    }
                }
            }
            render_new_tags(
                &mut mutable_call_options(&mut opts).prompt,
                &mut tags_rendered_len,
                render_tags,
                &provider.render_config_blob(),
                originals.as_mut(),
            );
            if let Some(originals) = originals.as_mut() {
                originals.index_origins(&opts.prompt);
            }
            let raw = crate::compaction::StoredTranscript::new(&opts.prompt, originals.as_ref());
            let state = compaction_state.as_mut().expect("Setup is durable");
            let session_ref = crate::wal_writer::agent_session_ref(&req.session);
            // Ride when the prefix is being rewritten anyway, then fetch the newest announced
            // view with `view.get`; either is offered here, at the boundary, and recorded before
            // the request renders.
            let recovering_ceiling =
                state
                    .ceiling_check
                    .as_ref()
                    .is_some_and(|check| match check {
                        crate::compaction::CeilingCheck::Waiting {
                            step_id: checked, ..
                        }
                        | crate::compaction::CeilingCheck::Decided {
                            step_id: checked, ..
                        } => *checked == step_id,
                    });
            let view_events = if recovering_ceiling {
                Vec::new()
            } else {
                Box::pin(runtime.views_at_boundary(
                    &mut boundary_views,
                    state,
                    control.compaction_views.as_deref(),
                    &session_ref,
                    &lineage_id,
                    &cfg.model.model_id,
                    // The prompt the request renders from, so the decision sees the bytes
                    // the model is sent (tags and loop guidance included); and the stored
                    // copy, against which the view's own bytes are judged.
                    &opts.prompt,
                    raw,
                    provider.reasoning_is_prefix_bound(),
                    // The run's first step, where a view deferred over an open turn ends.
                    completed_steps == 0,
                    crate::compaction::runtime::StepAt {
                        run_id: &resolved_run_id.0,
                        step: step_id.0,
                        superseded: &[],
                    },
                ))
                .await?
            };
            let mut request = crate::compaction::BoundaryRequest::default();
            request
                .refresh(&opts, state, &system_positions, |view| {
                    provider.render_request(view)
                })
                .map_err(|error| RunError::Wal(error.to_string()))?;
            notice_request_bytes = request.bytes;
            let (estimate, _) =
                crate::compaction::request_estimate(notice_request_bytes, &compaction_history);
            // Tell the provider what the session wrote since its last notice. The notice is
            // durable in the WAL before the model call, and the bus publisher projects it from
            // there, so it is never published before it is durable.
            if let Some(notice) = raw.newest().and_then(|through| {
                crate::compaction::view::notice_through(
                    state,
                    through,
                    crate::compaction::view::NoticeFacts {
                        session_ref: session_ref.clone(),
                        lineage_id: &lineage_id,
                        est_tokens: estimate,
                        model: &cfg.model.model_id,
                        window: context_limit,
                        usage: compaction_history.usage(),
                        events: view_events,
                        seq_floor: compaction_notice_floor,
                    },
                )
            }) {
                runtime.record_notice(state, notice).await?;
            }
            state.history = compaction_history.clone();
            let ceiling_detail = Box::pin(runtime.ceiling_before_request(
                &mut boundary_views,
                state,
                &session_ref,
                &lineage_id,
                step_id,
                &resolved_run_id,
                out,
                &opts,
                &system_positions,
                &mut request,
                raw,
                provider,
                context_limit,
            ))
            .await?;
            // A ceiling ride or a view-ready notification may have changed the durable view.
            request
                .refresh(&opts, state, &system_positions, |view| {
                    provider.render_request(view)
                })
                .map_err(|error| RunError::Wal(error.to_string()))?;
            notice_request_bytes = request.bytes;
            if let Some(notice) = raw.newest().and_then(|through| {
                crate::compaction::view::notice_through(
                    state,
                    through,
                    crate::compaction::view::NoticeFacts {
                        session_ref,
                        lineage_id: &lineage_id,
                        est_tokens: crate::compaction::request_estimate(
                            notice_request_bytes,
                            &compaction_history,
                        )
                        .0,
                        model: &cfg.model.model_id,
                        window: context_limit,
                        usage: compaction_history.usage(),
                        events: Vec::new(),
                        seq_floor: compaction_notice_floor,
                    },
                )
            }) {
                runtime.record_notice(state, notice).await?;
            }
            if ceiling_detail.is_some() {
                break RunFinishReason::ContextCeiling;
            }
            if is_cancelled(control) || boundary_cancelled(control) {
                break RunFinishReason::Cancelled;
            }
            if stop_is_requested(control.stop_requested.as_ref()) {
                interrupt_reason = Some("runner_shutdown".to_owned());
                break RunFinishReason::Interrupted;
            }
            model_opts = Some(Arc::new(request.into_options()));
            #[cfg(feature = "test-clock")]
            {
                // The model prompt is Setup's message or the applied view, with the raw
                // transcript. CompactionMessage and CompactionViewApplied preserve the first;
                // RunStarted, ModelStepFinished and ToolResult preserve the second. Sync them
                // before model I/O so a crash rebuilds the identical prompt rather than ask
                // the provider again.
                writer.sync().await?;
                crate::compaction::test_control::barrier(
                    control.clock.as_ref(),
                    &req.session.session,
                    "FoldRecorded",
                )
                .await?;
            }
        }
        let sent_opts = model_opts.as_ref().unwrap_or(&opts);
        if boundary_cancelled(control) && compaction_runtime.is_some() {
            break RunFinishReason::Cancelled;
        }

        // Run the model step, draining the normalized stream, under the optional
        // provider-call watchdog. A timeout means the model stream didn't terminate in time:
        // nothing durable was recorded for this step, so stop cooperatively as Interrupted at
        // this safe boundary (a later episode re-runs the step). A caller cancellation uses the
        // separate Cancelled terminal chosen at the watch site. This releases the lease that an
        // unbounded await would pin until process death.
        //
        // Failed attempts need their own durability barrier: a retry may remain open longer
        // than account reporting waits, or the process may crash before the step ends.
        // Sync as soon as the loop sees the attempt end; a retry may already be in flight.
        // Complete attempts still share the step's existing barrier.
        let session = &req.session;
        let attempt_run_id = &resolved_run_id;
        let completed_attempt_time = std::sync::Mutex::new(None);
        let record_attempt = |attempt: broca_protocol::ModelAttempt| -> AttemptFuture<'_> {
            if matches!(
                attempt.outcome,
                broca_protocol::AttemptOutcome::Complete
                    | broca_protocol::AttemptOutcome::ToolArgsTrailingGarbage
            ) {
                *completed_attempt_time.lock().unwrap() = Some(attempt.ts_ms);
            }
            Box::pin(async move {
                if let Some(warm) = &control.keep_warm {
                    warm.note_attempt(&attempt);
                }
                let needs_sync = attempt.outcome != broca_protocol::AttemptOutcome::Complete;
                report_input_transformations(session, attempt_run_id, step_id, &attempt);
                match &control.account_reporter {
                    Some(reporter) => {
                        let seq = writer
                            .record(WalRecord::model_attempt_finished(step_id, attempt.clone()))
                            .into_result()?;
                        reporter.attempt_recorded(writer.lineage_id(), seq, &attempt);
                    }
                    None => {
                        writer.record(WalRecord::model_attempt_finished(step_id, attempt));
                    }
                }
                if needs_sync {
                    writer.sync().await?;
                }
                Ok(())
            })
        };
        let sent_bytes = compaction_runtime.as_ref().map(|_| notice_request_bytes);
        // The receipt-polling wrapper is generic over this future. Heap-own the future
        // before either await arm so the wrapper cannot duplicate its large inline frame.
        let mut schema_resent = false;
        let mut schema_prior_retries = 0;
        let step = loop {
            let model_step = Box::pin(run_step_watched(
                provider,
                step_id,
                sent_opts,
                control,
                out,
                &record_attempt,
            ));
            // Keep the same pinned model future while filing delayed receipts; no receipt may
            // cancel, restart, or change the request already being streamed by the provider.
            let model_result = model_step.await?;
            let mut step = match model_result {
                Ok(step) => step,
                Err(reason) => break Err(reason),
            };
            if let StepOutcome::Failed {
                error,
                retries_used,
            } = &step
            {
                if !schema_resent && provider.structured_output_grammar_rejection(error) {
                    // The resend is authorized only by durable state: a crash after this barrier
                    // must reconstruct a non-strict provider, even if no resend ever reached it.
                    writer.record(WalRecord::StructuredOutputDowngraded {
                        run_id: resolved_run_id.clone(),
                    });
                    writer.sync().await?;
                    provider.downgrade_structured_output();
                    if let Some(so) = &mut structured_output {
                        so.downgrade_grammar();
                    }
                    schema_resent = true;
                    schema_prior_retries = *retries_used;
                    continue;
                }
            }
            match &mut step {
                StepOutcome::Completed { retries_used, .. }
                | StepOutcome::Failed { retries_used, .. } => {
                    *retries_used = retries_used.saturating_add(schema_prior_retries);
                }
            }
            break Ok(step);
        };
        let step = match step {
            Ok(step) => step,
            Err(reason) => break reason,
        };
        let step_written_at_ms = completed_attempt_time
            .lock()
            .unwrap()
            .or_else(|| control.now_ms())
            .filter(|at| *at != 0);

        // Set below when this step's final answer is extended by a steered message.
        let extend_for_steer: bool;
        let mut rejected_tool_calls = Vec::new();
        let mut cut_resume = None;
        let assembled = match step {
            StepOutcome::Completed {
                mut message,
                usage,
                retries_used,
                mut finish_reason,
                service_tier_used,
                speed_used,
                tool_args_cuts,
            } => {
                for cut in tool_args_cuts {
                    tracing::warn!(session = %req.session.session, step = step_id.0,
                        tool_name = %cut.tool_name, json_bytes = cut.json_bytes,
                        whitespace_bytes = cut.whitespace_bytes, code = "tool_args_trailing_garbage",
                        "stopped provider attempt after runaway tool argument whitespace");
                    rejected_tool_calls.push(ToolResult {
                        tool_call_id: cut.tool_call_id.clone(),
                        output: ToolOutput::Error {
                            message: cut.error_text(),
                        },
                        is_error: true,
                    });
                }
                total_retries_used = total_retries_used.saturating_add(retries_used);
                // Each step is a separate billed call → ACCUMULATE, never overwrite,
                // or a multi-step run under-reports its own total spend.
                total_usage += usage;
                input_budget.observe(usage);
                input_budget.limit = writer.input_token_limit().or(input_budget.limit);
                if let Some(bytes) = sent_bytes {
                    compaction_history.previous_native_usage = Some(usage);
                    compaction_history.previous_provider_code = None;
                    compaction_history.previous_error = false;
                    compaction_history.previous_request_bytes = Some(bytes);
                    compaction_history.previous_model = Some(cfg.model.clone());
                }
                if let Some(seal) = &control.shutdown_seal {
                    seal.note_usage(step_id, usage);
                }
                let unfiltered_blocks = message.content.len();
                if finalize_turn {
                    message.content.retain(|block| match block {
                        ContentBlock::ToolCall(call) => rejected_tool_calls
                            .iter()
                            .any(|result| result.tool_call_id == call.tool_call_id),
                        _ => true,
                    });
                    if rejected_tool_calls.is_empty()
                        && finish_reason == Some(FinishReason::ToolCalls)
                    {
                        // The durable finalize message omits undeclared calls, so `Stop` is the
                        // terminal reason that describes the message subscribers and replay see.
                        finish_reason = Some(FinishReason::Stop);
                    }
                }
                let original_message = hooks.as_ref().map(|_| message.clone());
                let hook_output = if let Some(hooks) = &hooks {
                    let original_message = original_message
                        .as_ref()
                        .expect("installed hooks retain the subject");
                    let subject = crate::hooks::Subject::PostAssistant {
                        step_id: step_id.0.to_string(),
                        blocks: message
                            .content
                            .iter()
                            .filter_map(|part| match part {
                                ContentBlock::Text { text }
                                | ContentBlock::TextWithState { text, .. } => Some(text.clone()),
                                _ => None,
                            })
                            .collect(),
                    };
                    Some(
                        hooks
                            .chain(subject, crate::hooks::Trace::default(), |fallback| {
                                let Some(seal) = &control.shutdown_seal else {
                                    return true;
                                };
                                let mut final_message = original_message.clone();
                                crate::hooks::apply_message_ops(
                                    &mut final_message.content,
                                    &fallback.operations,
                                )
                                .expect("checked ops");
                                let metadata = fallback.trace.metadata(
                                    serde_json::to_value(original_message).unwrap(),
                                    serde_json::to_value(&final_message).unwrap(),
                                    false,
                                );
                                let (rejections, _) = tool_args_rejection_records(
                                    &resolved_run_id,
                                    step_id,
                                    &final_message.tool_calls().cloned().collect::<Vec<_>>(),
                                    rejected_tool_calls.clone(),
                                    false,
                                );
                                let mut records = vec![WalRecord::ModelStepFinished {
                                    written_at_ms: step_written_at_ms,
                                    tags: None,
                                    hooks: metadata,
                                    step_id,
                                    assistant_message: Arc::new(final_message),
                                    usage,
                                    retries_used,
                                    finish_reason,
                                    service_tier_used: service_tier_used.clone(),
                                    speed_used: speed_used.clone(),
                                }];
                                records.extend(rejections);
                                seal.deposit_hook_records(
                                    crate::shutdown_seal::HookStage::PostAssistant,
                                    records,
                                )
                            })
                            .await,
                    )
                } else {
                    None
                };
                if let Some(output) = &hook_output {
                    crate::hooks::apply_message_ops(&mut message.content, &output.operations)
                        .expect("checked ops");
                }
                let message_changed = message.content.len() != unfiltered_blocks
                    || original_message
                        .as_ref()
                        .is_some_and(|original| &message != original);
                let message_hooks = hook_output.as_ref().and_then(|output| {
                    output.trace.metadata(
                        serde_json::to_value(original_message.as_ref().unwrap()).unwrap(),
                        serde_json::to_value(&message).unwrap(),
                        false,
                    )
                });
                last_step_finish_reason = finish_reason;
                let has_tool_calls = message.tool_calls().next().is_some();
                // Surface the PROVIDER's terminal reason when it gave one, so the
                // caller can tell a natural stop from a `length` (token-ceiling) or
                // `content_filter` stop. Fall back to the message shape only when the
                // provider supplied none (keeps tool-call turns reading as ToolCalls).
                // Note: this is observability only — the loop's dispatch/stop control
                // flow keys on message shape, never on this reason.
                let step_finish_reason =
                    finish_reason.unwrap_or_else(|| message_finish_reason(&message));

                // A no-tool answer normally ends the turn. If a caller's steered message
                // is queued but no model step has seen it, add one step to process it.
                // LoopFinished merely recognizes a no-tool answer, so it allows that extra
                // step. A satisfied step-count/tool stop or the budget's final allowed turn
                // forbids extension; replay puts the unprocessed steer in the transcript
                // at close without marking it delivered. Decide under the inbox lock before
                // recording the answer, so a steer either extends the turn or is refused as
                // arriving after it ended; replay reads the same outcome (see `crate::steer`).
                extend_for_steer = !has_tool_calls
                    && match &control.steer {
                        Some(inbox)
                            if !finalize_turn
                                && !limiting_stop_satisfied(
                                    &stop_when,
                                    completed_steps + 1,
                                    &message,
                                ) =>
                        {
                            !inbox.close_if_empty()
                        }
                        Some(inbox) => {
                            inbox.close();
                            false
                        }
                        None => false,
                    };

                if !has_tool_calls
                    && !control.emit_control_payloads
                    && !extend_for_steer
                    && input_budget.exhausted().is_none()
                {
                    // A no-tool message ends this loop, and terminal handling reads only the
                    // counters and finish reason below. Replay and materialization rebuild the
                    // terminal prompt from this WAL record, so the live path can transfer its
                    // sole message graph instead of retaining an otherwise-unread clone.
                    // A step that requests no tools ended the run on its own, so the run is
                    // `completed` even when this step is also the `max_steps` limit: the limit
                    // cut nothing off. `max_steps` is reserved for a limiting step that asked
                    // for tools, which the hard stop below seals. A completed run keeps its final
                    // text, directives and structured output, which a `max_steps` run never has.
                    let no_tool_reason = RunFinishReason::Completed;
                    if no_tool_reason == RunFinishReason::Completed && wants_final_text {
                        natural_final_text = Some(joined_text(&message));
                    }
                    if originals.is_some() {
                        // The run's last message, announced before the run ends.
                        closing_tail.push(Message::assistant_from(
                            message.content.clone(),
                            cfg.model.clone(),
                        ));
                    }
                    let message = Arc::new(message);
                    record_model_hook(
                        control.shutdown_seal.as_ref(),
                        hooks.is_some(),
                        writer,
                        WalRecord::ModelStepFinished {
                            written_at_ms: step_written_at_ms,
                            tags: None,
                            hooks: message_hooks,
                            step_id,
                            assistant_message: Arc::clone(&message),
                            usage,
                            retries_used,
                            finish_reason,
                            service_tier_used,
                            speed_used,
                        },
                    )
                    .await;
                    if message_changed {
                        restream_message(&message, out).await;
                    }
                    let _ = send(
                        out,
                        OutputEvent::Control(ControlUnit::StepFinished {
                            step_id,
                            usage,
                            finish_reason: step_finish_reason,
                        }),
                    )
                    .await;
                    completed_steps += 1;
                    break no_tool_reason;
                }

                // Messages that remain live after recording stay shared by every pre-barrier
                // consumer: the WAL serializer, direct control subscribers, and borrowed call
                // extraction. The prompt reclaims ownership only after the turn is durable.
                let message = Arc::new(message);
                let model_record = WalRecord::ModelStepFinished {
                    written_at_ms: step_written_at_ms,
                    tags: None,
                    hooks: message_hooks,
                    step_id,
                    assistant_message: Arc::clone(&message),
                    usage,
                    retries_used,
                    // The provider's terminal reason for this step — classification /
                    // observability only (replay's prompt rebuild never reads it).
                    finish_reason,
                    service_tier_used,
                    speed_used,
                };
                crate::budget_kernel::apply(
                    crate::budget_kernel::State {
                        facts: &mut step_budget_state,
                        config: cfg.budget.as_ref(),
                        run_id: Some(&resolved_run_id),
                    },
                    &model_record,
                )
                .expect("model steps cannot invalidate budget facts");
                if rejected_tool_calls.is_empty() {
                    record_model_hook(
                        control.shutdown_seal.as_ref(),
                        hooks.is_some(),
                        writer,
                        model_record,
                    )
                    .await;
                } else {
                    let (rejections, resume) = tool_args_rejection_records(
                        &resolved_run_id,
                        step_id,
                        &message.tool_calls().cloned().collect::<Vec<_>>(),
                        rejected_tool_calls.clone(),
                        false,
                    );
                    let mut records = vec![model_record];
                    records.extend(rejections);
                    if let Some(seal) = control.shutdown_seal.as_ref().filter(|_| hooks.is_some()) {
                        if !seal.record_hook_records(
                            &crate::shutdown_seal::HookStage::PostAssistant,
                            writer,
                            records,
                        ) {
                            std::future::pending::<()>().await;
                        }
                    } else {
                        writer.record_group(records)?;
                    }
                    cut_resume = resume;
                }
                if message_changed {
                    restream_message(&message, out).await;
                }
                if control.emit_control_payloads {
                    let _ = send(
                        out,
                        OutputEvent::Control(ControlUnit::AssistantMessage {
                            message: Arc::clone(&message),
                        }),
                    )
                    .await;
                }
                let _ = send(
                    out,
                    OutputEvent::Control(ControlUnit::StepFinished {
                        step_id,
                        usage,
                        finish_reason: step_finish_reason,
                    }),
                )
                .await;
                message
            }
            StepOutcome::Failed {
                error,
                retries_used,
            } => {
                total_retries_used = total_retries_used.saturating_add(retries_used);
                let error = stamp_credential(
                    error,
                    run_account_ref.as_deref(),
                    run_credential_id.as_deref(),
                );
                // An AUTH-class failure (credential expired/revoked mid-run) is
                // RESUMABLE, not terminal: write a durable RunPaused (NOT RunFinished,
                // which replay treats as terminal and would seal a resumable run) at
                // this safe boundary and STOP. After re-auth, resume continues the SAME
                // run_id from here. Any other error class is surfaced terminally — model
                // -call retry is the provider's job, not the loop's.
                if error.class == ErrorClass::Auth {
                    return pause_run(
                        writer,
                        out,
                        control,
                        resolved_run_id.clone(),
                        broca_protocol::PauseReason::AuthRequired,
                        Some(error),
                        completed_steps,
                        total_usage,
                        total_retries_used,
                    )
                    .await;
                }
                // Surface the error live (direct consumers of `out`, e.g. the broca-run
                // CLI) AND carry it out of the loop so the durable RunFinished persists
                // it — module subscribers source control from the WAL projection, not
                // `out`, so without persisting it the cause would be lost on the stream.
                let _ = send(
                    out,
                    OutputEvent::Control(ControlUnit::Error {
                        error: error.clone().into(),
                    }),
                )
                .await;
                terminal_error = Some(error);
                break RunFinishReason::Error;
            }
        };
        completed_steps += 1;
        if let Some(detail) = input_budget.exhausted() {
            writer.record(WalRecord::InputTokenBudgetExhausted { detail });
            let pending: Vec<_> = assembled.tool_calls().cloned().collect();
            if !pending.is_empty() {
                return finalize_hard_stop(
                    control,
                    &resolved_run_id,
                    step_id,
                    &pending,
                    &rejected_tool_calls,
                    cut_resume.is_some(),
                    DispatchOutcome::MaxSteps,
                    RunFinishReason::BudgetExhausted,
                    writer,
                    projector,
                    out,
                    completed_steps,
                    total_usage,
                    total_retries_used,
                    last_step_finish_reason,
                    &[],
                    control.emit_control_payloads,
                    None,
                    ClosingNotice::new(
                        compaction_state.as_mut(),
                        crate::compaction::StoredTranscript::new(
                            &opts.prompt[..raw_len],
                            originals.as_ref(),
                        ),
                        &closing_tail,
                        writer,
                        &req.session,
                        &cfg.model,
                        context_limit,
                        &compaction_history,
                        notice_request_bytes,
                        compaction_notice_floor,
                    )
                    .map(|closing| (closing, assembled.content.as_slice())),
                )
                .await;
            }
            break RunFinishReason::BudgetExhausted;
        }
        // A `submit_tool` run ends at the step that calls `submit_result`: none of that step's
        // calls is dispatched. The counted submit gets the runner's own result, every sibling a
        // never-executed `skipped_after_submit` result, and the run's structured result is
        // written with them and the terminal, in one barrier.
        if let Some((frozen, submit)) = structured_output.as_ref().and_then(|frozen| {
            counted_submit(frozen, &assembled, &rejected_tool_calls).map(|submit| (frozen, submit))
        }) {
            let pending: Vec<ToolCall> = assembled.tool_calls().cloned().collect();
            writer.record(submitted_result_record(
                &resolved_run_id,
                frozen,
                &submit,
                &pending,
            ));
            control.close_steers();
            return finalize_hard_stop(
                control,
                &resolved_run_id,
                step_id,
                &pending,
                &rejected_tool_calls,
                cut_resume.is_some(),
                DispatchOutcome::MaxSteps,
                RunFinishReason::Completed,
                writer,
                projector,
                out,
                completed_steps,
                total_usage,
                total_retries_used,
                last_step_finish_reason,
                &[],
                control.emit_control_payloads,
                Some(&submit.tool_call_id),
                ClosingNotice::new(
                    compaction_state.as_mut(),
                    crate::compaction::StoredTranscript::new(
                        &opts.prompt[..raw_len],
                        originals.as_ref(),
                    ),
                    &closing_tail,
                    writer,
                    &req.session,
                    &cfg.model,
                    context_limit,
                    &compaction_history,
                    notice_request_bytes,
                    compaction_notice_floor,
                )
                .map(|closing| (closing, assembled.content.as_slice())),
            )
            .await;
        }
        if finalize_turn {
            if !rejected_tool_calls.is_empty() {
                let pending: Vec<_> = assembled.tool_calls().cloned().collect();
                return finalize_hard_stop(
                    control,
                    &resolved_run_id,
                    step_id,
                    &pending,
                    &rejected_tool_calls,
                    cut_resume.is_some(),
                    DispatchOutcome::Interrupted,
                    RunFinishReason::Completed,
                    writer,
                    projector,
                    out,
                    completed_steps,
                    total_usage,
                    total_retries_used,
                    last_step_finish_reason,
                    &[],
                    control.emit_control_payloads,
                    None,
                    ClosingNotice::new(
                        compaction_state.as_mut(),
                        crate::compaction::StoredTranscript::new(
                            &opts.prompt[..raw_len],
                            originals.as_ref(),
                        ),
                        &closing_tail,
                        writer,
                        &req.session,
                        &cfg.model,
                        context_limit,
                        &compaction_history,
                        notice_request_bytes,
                        compaction_notice_floor,
                    )
                    .map(|closing| (closing, assembled.content.as_slice())),
                )
                .await;
            }
            // The budget grants exactly this one tool-disabled turn. Its answer is already in the
            // WAL record; the local prompt is not read again after this terminal branch.
            if wants_final_text && assembled.tool_calls().next().is_none() {
                natural_final_text = Some(joined_text(&assembled));
            }
            break RunFinishReason::Completed;
        }
        // Firings caused by this step are decided once. Every branch below either commits
        // them in its existing turn barrier or keeps them out of the live prompt.
        let budget_firings = due_budget_firings(
            cfg.budget.as_ref(),
            &step_budget_state,
            opts.prompt.len() as u64 + 2,
        );

        // Evaluate stop_when AFTER the step, BEFORE tool dispatch.
        //
        // DELIBERATE hard-stop: if a stop condition fires on a step whose assistant
        // message carries PENDING tool calls, the run stops NOW and those calls are
        // NOT dispatched ("the budget is gone, stop", not "dispatch one more batch").
        //
        // But the assistant message is already recorded, so a dangling tool_use with
        // no matching tool_result would persist a provider-INVALID trailing turn
        // (the model rejects a tool_use not followed by its result on resume). So
        // when a stop fires with PENDING calls, synthesize a provider-valid terminal
        // for each — tagged MaxSteps or Interrupted, which are definite "never ran"
        // terminals (never the ambiguous Unknown) — and persist them as a complete
        // batch before finishing. The calls were NEVER dispatched (no intent record),
        // so on resume the terminal means "never ran, never re-dispatch" and the
        // conversation stays valid.
        // A queued steer already won over a natural LoopFinished at the inbox lock.
        // No limiting condition can have been satisfied when that extension was granted.
        if let Some(cond) =
            stop_satisfied(&stop_when, completed_steps, &assembled).filter(|_| !extend_for_steer)
        {
            let pending: Vec<ToolCall> = assembled.tool_calls().cloned().collect();
            // `max_steps` means the limit cut off a run that wanted to continue: only a
            // limiting step that requested tools ends that way. A step with no tool calls
            // ended the run on its own, so it is `completed` even at the limit, the same as
            // the no-tool branch above and as replay reads it.
            let reason = match cond {
                StopCondition::StepCount { .. } if !pending.is_empty() => RunFinishReason::MaxSteps,
                _ => RunFinishReason::Completed,
            };
            if !pending.is_empty() {
                let outcome = match cond {
                    StopCondition::StepCount { .. } => DispatchOutcome::MaxSteps,
                    _ => DispatchOutcome::Interrupted,
                };
                // Finalize the hard stop ATOMICALLY: the synthetic repair batch AND the
                // RunFinished terminal go in ONE fsync barrier, then this returns. If they
                // were two barriers (repair-batch synced, then RunFinished synced
                // separately), a crash in between would leave the tool turn durably
                // COMMITTED with no terminal — and replay would read that as a resumable
                // open turn and re-run the model PAST the stop limit. One barrier closes
                // that window: a crash either loses the whole repair (resume re-runs the
                // step, idempotent) or has the terminal too (resume finds the lineage idle).
                control.close_steers();
                return finalize_hard_stop(
                    control,
                    &resolved_run_id,
                    step_id,
                    &pending,
                    &rejected_tool_calls,
                    cut_resume.is_some(),
                    outcome,
                    reason,
                    writer,
                    projector,
                    out,
                    completed_steps,
                    total_usage,
                    total_retries_used,
                    last_step_finish_reason,
                    &budget_firings,
                    control.emit_control_payloads,
                    None,
                    ClosingNotice::new(
                        compaction_state.as_mut(),
                        crate::compaction::StoredTranscript::new(
                            &opts.prompt[..raw_len],
                            originals.as_ref(),
                        ),
                        &closing_tail,
                        writer,
                        &req.session,
                        &cfg.model,
                        context_limit,
                        &compaction_history,
                        notice_request_bytes,
                        compaction_notice_floor,
                    )
                    .map(|closing| (closing, assembled.content.as_slice())),
                )
                .await;
            }
            // A satisfied stop on a step without tool calls ends the run `completed`, exactly
            // as an unconditioned no-tool answer does. Fall through to that branch so both keep
            // the final text and announce the last message the same way.
        }

        // Dispatch tools, or finish if the step produced none.
        let calls: Vec<ToolCall> = assembled.tool_calls().cloned().collect();
        if calls.is_empty() {
            if extend_for_steer {
                // The answer joins the prompt exactly as replay commits a no-tool step (its
                // content, stamped with this episode's model), and the next iteration renders
                // the steered message after it.
                budget_state = step_budget_state;
                let mut answer = Message {
                    cache_prefix_blocks: None,
                    role: Role::Assistant,
                    content: assembled.content.clone(),
                    origin: Some(cfg.model.clone()),
                    ordinal: None,
                    tags: Default::default(),
                };
                crate::tags::attach(&mut answer, writer.take_step_tags(step_id).as_ref());
                mutable_call_options(&mut opts).prompt.push(answer);
                continue;
            }
            if wants_final_text {
                natural_final_text = Some(joined_text(&assembled));
            }
            if originals.is_some() {
                // The run's last message, announced before the run ends.
                closing_tail.push(Message::assistant_from(
                    assembled.content.clone(),
                    cfg.model.clone(),
                ));
            }
            break RunFinishReason::Completed;
        }
        let calls_len = calls.len();
        // As with the model wait, retain one heap-owned batch future across both paths;
        // the reply wrapper must not embed the full dispatch state in the run frame.
        let batch_future = Box::pin(dispatch_batch_controlled(
            &resolved_run_id,
            step_id,
            &req.session,
            req.scope.as_ref(),
            calls,
            &manifest,
            tool_plane,
            approval,
            writer,
            cut_resume,
            &budget_firings,
            control.emit_control_payloads,
            control.stop_requested.as_ref(),
            hooks.as_ref(),
            warm_wait(
                control,
                warm_config.as_ref(),
                req,
                &resolved_run_id,
                &unsupported_logged,
                &warm_missed,
            ),
            out,
            control,
            approval_history.as_mut(),
            req.on_restart == Some(broca_protocol::request::OnRestart::Resume),
        ));
        let mut batch = batch_future.await?;

        if matches!(
            batch.interruption,
            Some(BatchInterruption::RestartPause | BatchInterruption::ProviderHeldPause)
        ) {
            return pause_approval(
                writer,
                out,
                control,
                &req.session,
                &resolved_run_id,
                completed_steps,
                total_usage,
                total_retries_used,
                &batch.open,
                batch.interruption == Some(BatchInterruption::ProviderHeldPause),
            )
            .await;
        }
        budget_state = step_budget_state;
        apply_committed_budget_firings(
            &mut budget_state,
            cfg.budget.as_ref(),
            &resolved_run_id,
            &budget_firings,
        );

        // Every call now has a matching result. Dispatch crossed the durability barrier, so the
        // next prompt may reclaim the assistant graph (or clone it now if a subscriber retained it).
        let assembled = Arc::unwrap_or_clone(assembled);
        debug_assert_eq!(
            batch.results.len(),
            calls_len,
            "no sparse call<->result set"
        );
        // The WAL already contains the tool's unmodified output. Add guard guidance only to the
        // returned copies that enter the model prompt, keeping tool output and loop advice separate.
        let original_results = originals.as_ref().map(|_| {
            Message::new(
                Role::Tool,
                batch
                    .results
                    .iter()
                    .cloned()
                    .map(ContentBlock::ToolResult)
                    .collect(),
            )
        });
        attach_repetition_guidance(
            &mut repetition_observer,
            assembled.tool_calls(),
            &mut batch.results,
        );
        let mut answer = Message {
            cache_prefix_blocks: None,
            role: Role::Assistant,
            content: assembled.content,
            // Stamp the producing model so a later cross-model continuation can strip a
            // signature this model minted. Same `cfg.model` replay stamps from the frozen
            // config → byte-identical live-vs-resume.
            origin: Some(cfg.model.clone()),
            ordinal: None,
            tags: Default::default(),
        };
        crate::tags::attach(&mut answer, writer.take_step_tags(step_id).as_ref());
        mutable_call_options(&mut opts).prompt.push(answer);
        // Computed-tag runs number this appended message at the next step boundary. Only a
        // legacy stored-ledger run needs the result tags assigned by the WAL writer here.
        let stored_result_tags = if computed_tags.is_none() {
            live_result_tags(writer, &batch.results, &mut Default::default())
        } else {
            None
        };
        let mut results = Message {
            cache_prefix_blocks: None,
            role: Role::Tool,
            content: batch
                .results
                .into_iter()
                .map(ContentBlock::ToolResult)
                .collect(),
            origin: None,
            ordinal: None,
            tags: Default::default(),
        };
        if let Some(tags) = stored_result_tags {
            crate::tags::attach_parts(&mut results, Some(tags));
        }
        if let (Some(originals), Some(original)) = (originals.as_mut(), original_results) {
            originals.retain_change(opts.prompt.len(), original, &results);
        }
        mutable_call_options(&mut opts).prompt.push(results);
        raw_len = opts.prompt.len();
        append_budget_firings(
            writer,
            &mut mutable_call_options(&mut opts).prompt,
            &resolved_run_id,
            &budget_firings,
            out,
        )
        .await;

        // This fsync also makes the current turn durable, so if we crash before
        // starting the next step, we can lose at most the one step that was already
        // in flight.

        // Publish only the fsynced head to the coalescing projector. Store reads and writes
        // run concurrently with the next model step, while the safe-head bound prevents a
        // read-only WAL handle from projecting a later append that is not durable yet.
        if let Some(projector) = projector {
            let safe_h = writer.sync().await?;
            projector.enqueue(safe_h);
        }

        // A dispatch returned an AMBIGUOUS outcome (the side effect may have
        // applied). Do NOT feed it back into another model step — stop the run for
        // review. The tool-role message above (with the provider-valid synthetic
        // unknown result) is persisted so the conversation stays valid, but the
        // loop does not continue.
        if let Some(reason) = BatchOutcome::finish_reason(batch.interruption) {
            if batch.interruption == Some(BatchInterruption::StopRequested) {
                interrupt_reason = Some("runner_shutdown".to_owned());
            }
            break reason;
        }
    };

    broca_protocol::content::stamp_ordinals_from(
        &mut mutable_call_options(&mut opts).prompt,
        stamped_len,
    );
    // Announce what the run wrote after its last step boundary (its final answer) now, not at
    // the next send's first boundary: the contract owes a notice after each durable write. The
    // notice follows those records in the WAL and its own barrier makes both durable before the
    // publisher can project it.
    if let (Some(runtime), Some(closing)) = (
        &compaction_runtime,
        ClosingNotice::new(
            compaction_state.as_mut(),
            crate::compaction::StoredTranscript::new(&opts.prompt[..raw_len], originals.as_ref()),
            &closing_tail,
            writer,
            &req.session,
            &cfg.model,
            context_limit,
            &compaction_history,
            notice_request_bytes,
            compaction_notice_floor,
        ),
    ) {
        if let Some((state, notice)) = closing.notice(&[], &[]) {
            runtime.record_notice(state, notice).await?;
        }
    }
    // A completed run admitted with structured output records what it produced in the same
    // barrier as its terminal. A run that submitted returned above; reaching here, a
    // `submit_tool` run completed without calling `submit_result`.
    if let (Some(frozen), RunFinishReason::Completed) = (&structured_output, reason) {
        writer.record(WalRecord::StructuredOutputResult {
            run_id: resolved_run_id.clone(),
            result: broca_protocol::structured_output::result(
                frozen,
                true,
                None,
                natural_final_text.as_deref(),
                Vec::new(),
            ),
        });
    }
    control.close_steers();
    let (summary, last_fsynced_seq) = finish_run(
        writer,
        control,
        out,
        &resolved_run_id,
        RunTerminal {
            reason,
            interrupt_reason,
        },
        completed_steps,
        total_usage,
        total_retries_used,
        terminal_error,
        last_step_finish_reason,
    )
    .await?;
    if let Some(projector) = projector {
        projector.enqueue_terminal(last_fsynced_seq);
    }

    Ok(summary)
}

/// Project the session's durable WAL into the configured materialized store. Reads a
/// SEPARATE WAL handle bounded by the last fsynced sequence, then projects up to the latest
/// replay-safe boundary. Best-effort: a projection
/// failure is reported but remains non-fatal because full-WAL replay keeps resume
/// correct regardless.
/// SWEPT 2026-08-12: this was the only OBSERVATION among the workspace's process-global
/// atomics; the other five are mechanism (id minting, token allocation, a single-sweep latch,
/// and two throttle fields that reset to zero after firing) and are correctly file-local.
/// The discriminator, so a later sweep does not re-derive it: does the value describe SOMETHING
/// THAT HAPPENED, or does it drive a decision? A value that is reset after use is state; a
/// value that only ever accumulates is an observation, and an observation with no reader is a
/// signal nobody receives.
///
/// Failed projection attempts by THIS process since boot.
///
/// Compiled into production deliberately. A projection failure leaves the store's watermark
/// where it was, so every later read of that session merges its prefix with a WAL tail that
/// grows each turn -- and until this counter existed the only trace was one stderr line in a
/// log shared with every other module, where roughly one line in twelve is spliced by a
/// concurrent writer. A condition whose only record is a line in a shared log is one nobody
/// finds until a reader complains about latency, days later, pointing at the wrong subsystem.
///
/// It is honest on a self-reported surface because it counts what this process OBSERVED ITSELF
/// DOING. Zero means "this process has seen no failures since boot", never "the store is
/// current" -- a freshly started process reads zero with an arbitrarily stale store behind it.
/// Naming it for the attempt rather than for the store is what keeps it inside that bound.
static PROJECTION_WARNING_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Failed projection attempts by this process since boot. A count of attempts, never a
/// statement about whether the store is current; see [`PROJECTION_WARNING_COUNT`].
pub fn projection_failures() -> u64 {
    PROJECTION_WARNING_COUNT.load(std::sync::atomic::Ordering::Relaxed) as u64
}

/// Sessions whose latest projection attempt failed and have not projected successfully since.
pub fn projection_behind_sessions() -> u64 {
    crate::projection_health::behind_sessions()
}

pub(crate) async fn project_committed<W: WalStore>(
    wal_store: &W,
    store: Arc<dyn SessionStore>,
    session: &BindIdentity,
    safe_h: u64,
    budgeted: bool,
) {
    // Older rows and third-party persisters may not have a complete projection seed. In that
    // case reading after zero preserves the full-prefix correctness fallback.
    let seed = read_projection_seed_blocking(store.clone(), session.clone())
        .await
        .ok()
        .flatten();
    let prior_policy = seed
        .as_ref()
        .and_then(|seed| seed.admission_checkpoint.lineage_retention);
    let retained_seed = matches!(prior_policy, Some(crate::LineageRetention::Retained { .. }));
    let budgeted = budgeted
        || seed
            .as_ref()
            .and_then(|seed| seed.admission_checkpoint.last_episode.as_ref())
            .is_some_and(|episode| episode.config.budget.is_some());
    // Budgeted suffixes may use the watermark only after the persisted state proves every
    // earlier firing. Missing/invalid state preserves the complete-WAL fallback.
    let use_seed = seed.as_ref().is_some_and(|seed| {
        !retained_seed
            && prior_policy.is_some()
            && seed
                .deferred_calls
                .as_ref()
                .is_none_or(|calls| calls.stopped_runs.is_some())
            && (!budgeted || crate::replay::budget_replay_seed_is_valid(seed))
    });
    let after_seq = if use_seed {
        seed.as_ref().map_or(0, |seed| seed.watermark_seq)
    } else {
        0
    };
    let anchor = use_seed
        .then(|| seed.as_ref().and_then(|seed| seed.anchor.as_ref()))
        .flatten();
    // This WAL read runs inline on the async task while the store write below is offloaded to a
    // blocking thread. It is an incremental tail bounded by the projection watermark only when
    // `use_seed` holds. A retained lineage, a seed without a recorded policy, a seed whose
    // deferred-call summary lacks its stopped-run set, or a budgeted run without a valid seed
    // reads from sequence zero, so its cost grows with the WAL; the seeded-write retry below is
    // also a full read.
    //
    // Moving the read onto a blocking thread needs an owned read handle: WalStore is threaded
    // through the run_loop entry points as a borrow, and giving it Clone is a public API change.
    let range = match wal_store.read_records_range(session, after_seq, Some(safe_h), anchor) {
        Ok(range) => range,
        Err(error) => {
            // Same consequence as the write failure below: the run is fine, the watermark does
            // not advance, and this session's reads pay a growing WAL replay until a later
            // projection succeeds.
            tracing::warn!(
                target: "store",
                "HARD WARNING: store projection WAL read failed for session {session:?} \
                 (run is unaffected; this session's reads fall back to WAL replay and get slower \
                 each turn until a projection succeeds): {error}"
            );
            PROJECTION_WARNING_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            crate::projection_health::record_failure(session);
            return;
        }
    };
    let records = match range.records() {
        broca_wal::WalRangeRecords::Complete(records)
        | broca_wal::WalRangeRecords::Partial(records)
        | broca_wal::WalRangeRecords::Unknown(records) => records,
        broca_wal::WalRangeRecords::Absent => &[],
    };
    if matches!(
        crate::retention::projection_policy(
            records,
            prior_policy.map(|policy| (policy, after_seq))
        ),
        Some(crate::LineageRetention::Retained { .. })
    ) {
        let lineage = range.lineage_id.map(|id| id.to_string());
        let metadata = crate::projection_plan::retained_withdrawal_projection(
            None,
            records,
            lineage.as_deref(),
            None,
        );
        let result = match (metadata, lineage) {
            (Ok(metadata), Some(lineage)) => {
                let session = session.clone();
                spawn_blocking(JobKind::Projection, move || -> Result<(), String> {
                    if let broca_wal::WalRangeRecords::Complete(records) = range.records() {
                        store.project_retained_head(&session, &lineage, records)?;
                    }
                    if let Some(metadata) = metadata {
                        store.project_retention_metadata(&session, &metadata)?;
                    }
                    Ok(())
                })
                .await
                .map_err(|_| ())
                .and_then(|result| result.map_err(|_| ()))
            }
            (Ok(_), None) => Ok(()),
            (Err(_), _) => Err(()),
        };
        if result.is_err() {
            tracing::warn!(target: "store", "retention head or withdrawal metadata projection failed; the WAL remains authoritative");
            PROJECTION_WARNING_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            crate::projection_health::record_failure(session);
        } else {
            crate::projection_health::record_success(session);
        }
        return;
    }
    let write = project_store_blocking(store.clone(), session.clone(), range);
    tokio::pin!(write);
    let write_result = tokio::select! {
        result = &mut write => result,
        () = tokio::time::sleep(PROJECTOR_WRITE_WARNING_TIMEOUT) => {
            tracing::warn!(
                target: "store",
                "HARD WARNING: store projection write exceeded 30s for session {:?}; waiting for completion before lease release",
                session
            );
            write.await
        }
    };
    let write_result = if write_result.is_err() && use_seed {
        // Seeded projection is an optimization only. A replay or store error retries against the
        // complete WAL so a malformed seed cannot leave a partial incremental result in use.
        match wal_store.read_records_range(session, 0, Some(safe_h), None) {
            Ok(full_range) => {
                project_store_blocking(store.clone(), session.clone(), full_range).await
            }
            Err(error) => Err(error.to_string()),
        }
    } else {
        write_result
    };
    match write_result {
        Ok(()) => crate::projection_health::record_success(session),
        Err(error) => {
            // Non-fatal to the RUN, which is why the message says so -- the WAL is authoritative
            // and nothing is lost. It is not non-consequential to READERS, and that is the part
            // worth stating: the store's watermark stays where it was, so every subsequent
            // session.read on this session merges its prefix with a WAL tail that grows with
            // each turn. Measured on a session whose watermark never advanced, page reads cost
            // ~127ms against ~13ms for one projected normally. "Non-fatal" alone reads as an
            // event that has already passed; the durable consequence is a session that gets
            // slower to read for as long as the condition persists.
            tracing::warn!(
                target: "store",
                "HARD WARNING: store projection failed (run is unaffected, WAL is authoritative; \
                 this session's reads fall back to WAL replay and get slower each turn until a \
                 projection succeeds): {error}"
            );
            PROJECTION_WARNING_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            crate::projection_health::record_failure(session);
        }
    }
}

/// Name the credential behind a credential-scoped terminal, so every surface that reports the
/// terminal error (the durable `RunFinished`, the control lane's `Error` unit, `run.status`,
/// `session.read`) says WHICH key or account to stop using:
///
/// - an account-limit or billing refusal gets the run's provider account in `account_ref`
///   (`None` when the run recorded none, falling back to provider-wide treatment);
/// - every error [`ProviderError::is_credential_scoped`] accepts gets the run's frozen
///   credential id in `credential_id` (`None` when the run froze no selection);
/// - a rate limit gets the typed [`broca_protocol::error::RATE_LIMITED`] code in place of the
///   provider's own echo. This runs once, at the terminal, after the transport's in-step retries,
///   so it never changes a retry decision.
///
/// Every other error is returned unchanged.
fn stamp_credential(
    mut error: ProviderError,
    account_ref: Option<&str>,
    credential_id: Option<&str>,
) -> ProviderError {
    if error.is_account_limit_exhausted() || error.is_billing_exhausted() {
        error.account_ref = account_ref.map(str::to_owned);
    }
    if error.is_credential_scoped() {
        error.credential_id = credential_id.map(str::to_owned);
    }
    if error.is_rate_limited() {
        error.provider_code = Some(broca_protocol::error::RATE_LIMITED.into());
    }
    error
}

/// The sequence number of the last record in the log as this writer knows it: the record a
/// resume that has written nothing yet is resuming from.
fn last_record_seq(writer: &WalHandle) -> u64 {
    writer.next_record_seq().saturating_sub(1)
}

// Pairs the terminal state with any explicitly recorded interrupt reason.
struct RunTerminal {
    reason: RunFinishReason,
    interrupt_reason: Option<String>,
}

impl From<RunFinishReason> for RunTerminal {
    fn from(reason: RunFinishReason) -> Self {
        Self {
            reason,
            interrupt_reason: None,
        }
    }
}

fn sealed_interrupt_reason(
    control: &RunControl,
    reason: RunFinishReason,
    recorded: Option<String>,
) -> Option<String> {
    // These coordinator-issued signals establish a planned stop on the new terminal, even
    // when an in-flight tool's outcome is unknown. Never infer provenance from tool outcomes,
    // timestamps, or missing metadata on an older terminal.
    let planned_stop = stop_is_requested(control.stop_requested.as_ref())
        || control
            .shutdown_seal
            .as_ref()
            .is_some_and(crate::ShutdownSeal::is_tripped);
    if reason == RunFinishReason::Interrupted && planned_stop {
        Some("runner_shutdown".into())
    } else {
        recorded
    }
}

/// Finalize the run: record + fsync `run_finished` BEFORE emitting the terminal
/// RunFinished and returning (so a durable run_finished means the run is truly
/// done, and resume treats it as a no-op).
#[allow(clippy::too_many_arguments)]
async fn finish_run(
    writer: &WalHandle,
    control: &RunControl,
    out: &mpsc::Sender<OutputEvent>,
    // Threaded so the TERMINAL can name its run. A subscriber attaching mid-stream has no
    // other way to attribute it: a resumed run writes no new `RunStarted`, so a client
    // that identifies its run by waiting for one waits through its own completion.
    run_id: &RunId,
    terminal: impl Into<RunTerminal>,
    steps: u64,
    usage: Usage,
    retries_used: u32,
    error: Option<ProviderError>,
    final_step_finish_reason: Option<FinishReason>,
) -> Result<(RunSummary, u64), RunError> {
    let RunTerminal {
        reason,
        interrupt_reason,
    } = terminal.into();
    // `error` is the typed terminal error, persisted on the durable RunFinished so the
    // control-stream projection (a module subscriber's only source) can emit a
    // ControlUnit::Error with the cause. Provider failures and resume render-drift interrupts
    // pass `Some`; terminals without a typed cause pass `None`.
    writer.record(WalRecord::RunFinished {
        interrupt_reason: sealed_interrupt_reason(control, reason, interrupt_reason),
        ts_ms: control.now_ms(),
        reason,
        usage,
        error,
        retries_used,
    });
    let last_fsynced_seq = writer.sync().await?;
    let _ = send(
        out,
        OutputEvent::Control(ControlUnit::RunFinished {
            run_id: Some(run_id.clone()),
            reason,
            usage: (steps > 0).then_some(usage),
            final_step_finish_reason,
            indeterminate_tool_calls: None,
            retries_used,
        }),
    )
    .await;
    Ok((
        RunSummary {
            reason,
            paused: None,
            steps,
            usage,
            retries_used,
            final_step_finish_reason,
        },
        last_fsynced_seq,
    ))
}

/// Pause the run at a safe boundary: record + fsync `RunPaused` (a NON-terminal durable
/// record) BEFORE emitting the `Paused` control unit and returning. The run is RESUMABLE
/// — after the cause clears, resume continues the SAME run_id from here. The summary keeps
/// the surface-level `Interrupted` reason but also carries the pause cause so callers such
/// as the coordinator can index this as `Paused`, not a terminal interruption. The durable
/// `RunPaused` is what makes a later resume continue rather than treat it as finished.
#[allow(clippy::too_many_arguments)]
async fn pause_run_impl(
    writer: &WalHandle,
    out: &mpsc::Sender<OutputEvent>,
    control: &RunControl,
    run_id: RunId,
    reason: broca_protocol::PauseReason,
    error: Option<ProviderError>,
    steps: u64,
    usage: Usage,
    retries_used: u32,
    open: Option<(&[approval_gate::Held], &BatchCtx)>,
) -> Result<RunSummary, RunError> {
    // A paused or sealed run renders no further step, so it takes no more steers.
    control.close_steers();
    // Cancellation wins until the pause itself is durable. This closes the admission-to-pause
    // race where the actor has acknowledged the run, receives cancel, and the provider then
    // reports an auth failure: seal the episode instead of publishing a pause nobody can abandon.
    if is_cancelled(control) || boundary_cancelled(control) {
        if let Some((calls, ctx)) = open {
            approval_gate::close_open(calls, ctx, writer).await?;
            approval_gate::report_open(calls, ctx, control).await;
        }
        return finish_run(
            writer,
            control,
            out,
            &run_id,
            RunFinishReason::Cancelled,
            steps,
            usage,
            retries_used,
            None,
            None,
        )
        .await
        .map(|(summary, _)| summary);
    }

    writer.record(WalRecord::RunPaused {
        ts_ms: control.now_ms(),
        run_id: run_id.clone(),
        reason,
        error: error.clone(),
    });
    writer.sync().await?;
    let _ = send(
        out,
        OutputEvent::Control(ControlUnit::Paused {
            run_id,
            reason,
            error: error.map(SharedProviderError::from),
        }),
    )
    .await;
    Ok(RunSummary {
        reason: RunFinishReason::Interrupted,
        paused: Some(reason),
        steps,
        usage,
        retries_used,
        final_step_finish_reason: None,
    })
}

/// Find the first assistant message produced by this episode's committed model steps.
///
/// A lineage prompt can contain many prior episodes, but `completed_steps` is already scoped to the
/// current one. Counting that many assistant messages back from the prompt tail therefore gives the
/// exact observer boundary without carrying repetition state across user turns.
fn episode_history_start(prompt: &[Message], committed_steps: u64) -> usize {
    if committed_steps == 0 {
        return prompt.len();
    }

    let mut remaining = committed_steps;
    for (index, message) in prompt.iter().enumerate().rev() {
        if message.role == Role::Assistant {
            remaining -= 1;
            if remaining == 0 {
                return index;
            }
        }
    }

    debug_assert_eq!(
        remaining, 0,
        "completed step count exceeds assistant prompt history"
    );
    0
}

/// Provider-reported input transformations seen by this process since it started.
static INPUT_TRANSFORMATIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Provider attempts this process sent without asking for the report, since it started.
static INPUT_TRANSFORMATIONS_UNMEASURED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many entries providers have listed in `input_transformations` since this process started.
/// Every entry means the provider changed what the model saw, for Anthropic by dropping (or
/// letting through) a signed thinking block whose earlier context no longer matched: the
/// rendered conversation was not append-only. Zero is the healthy reading only for the attempts
/// that asked for the report: see [`input_transformations_unmeasured_attempts`].
pub fn input_transformations_reported() -> u64 {
    INPUT_TRANSFORMATIONS.load(std::sync::atomic::Ordering::Relaxed)
}

/// How many provider attempts this process sent without asking for the
/// `input_transformations` report (every Claude Code persona request, which carries no
/// thinking-binding beta). Their transformations are unknown, not zero, and are never counted in
/// [`input_transformations_reported`].
pub fn input_transformations_unmeasured_attempts() -> u64 {
    INPUT_TRANSFORMATIONS_UNMEASURED.load(std::sync::atomic::Ordering::Relaxed)
}

/// What one attempt's record says about provider input transformations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransformationReading {
    /// The request did not ask for the report, so nothing is known.
    NotMeasured,
    /// The response carried no report although the request may have asked (or the record
    /// predates the report).
    Absent,
    /// The response reported this many transformations.
    Reported(usize),
}

impl TransformationReading {
    pub(crate) fn of(attempt: &broca_protocol::ModelAttempt) -> Self {
        if attempt.input_transformations_unmeasured {
            return Self::NotMeasured;
        }
        match &attempt.input_transformations {
            Some(entries) => Self::Reported(entries.len()),
            None => Self::Absent,
        }
    }
}

/// Count and warn about every entry of an attempt's `input_transformations`, whatever its reason:
/// on an account that is not enforced, a mismatched thinking block is dropped silently, so this
/// log line and the counter are the only place the loss shows up.
fn report_input_transformations(
    session: &BindIdentity,
    run_id: &RunId,
    step_id: StepId,
    attempt: &broca_protocol::ModelAttempt,
) {
    let entries = match TransformationReading::of(attempt) {
        TransformationReading::NotMeasured => {
            // Not measured is not "none": counted apart, so the reported total never reads a
            // persona run's silence as a healthy zero.
            INPUT_TRANSFORMATIONS_UNMEASURED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        TransformationReading::Absent | TransformationReading::Reported(0) => return,
        TransformationReading::Reported(_) => {
            attempt.input_transformations.as_deref().unwrap_or_default()
        }
    };
    INPUT_TRANSFORMATIONS.fetch_add(entries.len() as u64, std::sync::atomic::Ordering::Relaxed);
    let reasons: Vec<String> = entries
        .iter()
        .map(|entry| {
            format!(
                "{}:{}@{}",
                entry.kind.as_deref().unwrap_or("?"),
                entry.reason.as_deref().unwrap_or("?"),
                entry.path.as_deref().unwrap_or("?"),
            )
        })
        .collect();
    tracing::warn!(
        target: "provider",
        session = %session.session,
        run_id = %run_id.0,
        step = step_id.0,
        attempt = attempt.attempt,
        count = entries.len(),
        transformations = ?reasons,
        "provider transformed the request input: an earlier part of the conversation changed"
    );
}

/// Re-apply repetition guidance to every episode that finished before `current_start`.
///
/// `spans` are the replayed episodes' model-step ranges. A span reaching past `current_start` is
/// the episode being resumed, which the caller folds itself, so it is skipped here rather than
/// decorated twice. `None` means the prompt came from a store prefix without episode boundaries;
/// admission only takes that path when [`store_prior_needs_episode_spans`] found no guidance to
/// reproduce before the current episode.
fn restore_earlier_episode_guidance(
    prompt: &mut [Message],
    spans: Option<&[std::ops::Range<usize>]>,
    current_start: usize,
    mut originals: Option<&mut crate::compaction::OriginalMessages>,
) {
    for span in spans.unwrap_or_default() {
        if span.end <= current_start && span.end <= prompt.len() {
            restore_guidance_with_originals(
                &mut prompt[span.clone()],
                span.start,
                originals.as_deref_mut(),
            );
        }
    }
}

/// Whether a prior state assembled from a materialized store prefix is missing information the
/// run loop needs: the episode boundaries of earlier episodes that showed repetition guidance.
///
/// Checked by folding everything before the current episode's model steps with ONE observer. That
/// fold is a safe over-approximation of the per-episode folds: every observer counter only grows
/// until a reset event, and each episode's steps are a contiguous run of the prompt, so a count
/// reached inside one episode is always reached (or exceeded) at the same message when counting
/// starts earlier. If the single fold never produces guidance, no per-episode fold can, and the
/// fast path renders the same bytes a full replay would.
fn store_prior_needs_episode_spans(prior: &ReplayState) -> bool {
    // Eligible ChatGPT backend sessions keep the request's reasoning effort at its first
    // episode's setting, inserting later changes in the conversation. The checkpoint can
    // supply that first setting only when it proves no historical effort ever changed.
    if prior.render_episodes.is_none()
        && prior.run_config.as_ref().is_some_and(|config| {
            config.frozen_render_config["chatgpt_effort_updates"].as_bool() == Some(true)
        })
    {
        return true;
    }
    let current_start = match &prior.resolution {
        // An idle lineage either stays idle or gains a new episode after everything in it.
        ReplayResolution::LineageIdle { .. } => prior.prompt.len(),
        // An open tool turn has a durable model step but is not in the committed
        // prompt until its batch finishes. Pauses retain that same pending turn.
        ReplayResolution::Resume { pending } | ReplayResolution::AuthRequired { pending, .. } => {
            episode_history_start(
                &prior.prompt,
                prior
                    .completed_steps
                    .saturating_sub(u64::from(pending.is_some())),
            )
        }
        ReplayResolution::ProviderHeld { .. } => {
            episode_history_start(&prior.prompt, prior.completed_steps.saturating_sub(1))
        }
        // An ambiguous episode is sealed without another request, so nothing is rendered.
        ReplayResolution::Interrupted { .. } => return false,
    };
    repetition_guidance_fires(&prior.prompt[..current_start.min(prior.prompt.len())])
}

/// Whether folding `prompt` with a single observer produces guidance for any tool result.
fn repetition_guidance_fires(prompt: &[Message]) -> bool {
    let mut fold = RepetitionFold::default();
    prompt.iter().any(|message| {
        fold.push(message)
            .iter()
            .any(|(_, verdict)| verdict.guidance().is_some())
    })
}

fn append_repetition_guidance(result: &mut ToolResult, guidance: &str) {
    const LABEL: &str = "Tool repetition guidance: ";
    let suffix = format!("\n\n{LABEL}{guidance}");

    match &mut result.output {
        ToolOutput::Parts { parts } => {
            parts.push(broca_protocol::ToolOutputPart::Text { text: suffix })
        }
        ToolOutput::Text { text } => text.push_str(&suffix),
        ToolOutput::Denied { reason } => reason.push_str(&suffix),
        ToolOutput::Error { message } => message.push_str(&suffix),
        ToolOutput::Json { value } => {
            // Structured outputs have no metadata slot. Preserve their JSON text before the note;
            // this copy is provider-facing only, while the durable result remains structured.
            let text = format!("{value}{suffix}");
            result.output = ToolOutput::Text { text };
        }
    }
}

fn attach_repetition_guidance_to_result(
    observer: &mut RepetitionObserver,
    call: &ToolCall,
    result: &mut ToolResult,
) {
    let bytes = result_bytes(&result.output);
    let verdict = observer.observe(call, bytes.as_ref(), result.is_error);
    if let Some(guidance) = verdict.guidance() {
        append_repetition_guidance(result, &guidance);
    }
}

fn attach_repetition_guidance<'a>(
    observer: &mut RepetitionObserver,
    calls: impl Iterator<Item = &'a ToolCall>,
    results: &mut [ToolResult],
) {
    for (call, result) in calls.zip(results) {
        attach_repetition_guidance_to_result(observer, call, result);
    }
}

/// Recreate guidance that an uninterrupted run retained in its in-memory prompt. Replay deliberately
/// reconstructs only the tool-owned output from the WAL, so this folds raw result copies in source
/// order and decorates those copies without changing durable history.
///
/// Pass one episode's model-step messages, excluding other episodes' messages and the
/// episode's initial input. Each call starts fresh repetition counters. The slice must
/// contain raw, undecorated tool outputs: applying this fold twice would duplicate warnings.
/// Offline renderers use this same fold so their reconstructed inputs receive exactly the
/// decoration the run loop sends to the provider.
pub fn restore_repetition_guidance(prompt: &mut [Message]) {
    restore_guidance_with_originals(prompt, 0, None);
}

fn restore_guidance_with_originals(
    prompt: &mut [Message],
    base: usize,
    mut originals: Option<&mut crate::compaction::OriginalMessages>,
) -> RepetitionObserver {
    let mut fold = RepetitionFold::default();
    for (index, message) in prompt.iter_mut().enumerate() {
        let verdicts = fold.push(message);
        let original = originals
            .as_ref()
            .filter(|_| !verdicts.is_empty())
            .map(|_| message.clone());
        for (block_index, verdict) in verdicts {
            if let Some(guidance) = verdict.guidance() {
                if let ContentBlock::ToolResult(result) = &mut message.content[block_index] {
                    append_repetition_guidance(result, &guidance);
                }
            }
        }
        if let (Some(originals), Some(original)) = (originals.as_deref_mut(), original) {
            originals.retain_change(base + index, original, message);
        }
    }
    fold.into_observer()
}

/// On resume, complete an incomplete trailing tool turn WITHOUT re-running the
/// (already durable, already billed) model step. Re-dispatches ONLY the calls that
/// never left the process, reuses the durable terminals, and appends the
/// now-complete turn to `prompt`. Returns `Some(reason)` if completing it was
/// itself ambiguous (and so the run must stop for review).
#[allow(clippy::too_many_arguments)]
async fn complete_pending_turn<T: ToolPlane, A: ApprovalPolicy>(
    req: &RunRequest,
    resolved_run_id: &RunId,
    pending: PendingTurn,
    args_cut: bool,
    manifest: &ToolManifest,
    tool_plane: &T,
    approval: &A,
    writer: &WalHandle,
    prompt: &mut Vec<Message>,
    originals: Option<&mut crate::compaction::OriginalMessages>,
    origin: ModelHandle,
    stored_tag_rules: bool,
    budget_firings: &[BudgetFiring],
    emit_control_payloads: bool,
    stop: Option<&CancellationToken>,
    hooks: Option<&crate::hooks::HookRuntime<'_>>,
    repetition_observer: &mut RepetitionObserver,
    warm: Option<crate::keep_warm_wait::WarmWait<'_>>,
    out: &mpsc::Sender<OutputEvent>,
    control: &RunControl,
    history: Option<&mut approval_gate::History>,
    frames: &[broca_wal::FramedRecord],
) -> Result<(Option<RunFinishReason>, Vec<approval_gate::Held>, bool), RunError> {
    let intents = held_resume_intents(&pending, frames, writer)?;
    // The full ToolCall set (name+input) from the durable assistant message, in
    // PLANNED order (so the reconstructed tool-role message is source-ordered).
    let mut by_id: std::collections::HashMap<ToolCallId, std::collections::VecDeque<ToolCall>> =
        std::collections::HashMap::new();
    for call in pending.assistant.tool_calls() {
        by_id
            .entry(call.tool_call_id.clone())
            .or_default()
            .push_back(call.clone());
    }
    let calls: Vec<ToolCall> = pending
        .planned
        .iter()
        .filter_map(|id| by_id.get_mut(id)?.pop_front())
        .collect();
    let mut existing: std::collections::HashMap<
        ToolCallId,
        std::collections::VecDeque<ToolResult>,
    > = std::collections::HashMap::new();
    for (id, result) in pending.existing {
        existing.entry(id).or_default().push_back(result);
    }
    let mut existing_tags: std::collections::HashMap<ToolCallId, std::collections::VecDeque<u64>> =
        std::collections::HashMap::new();
    for (id, tag) in pending.existing_tags {
        existing_tags.entry(id).or_default().push_back(tag);
    }
    let assistant_tags = pending.assistant_tags;

    let mut resume = ResumeCtx {
        batch_already_started: pending.batch_id.is_some(),
        existing,
        prepared: pending
            .deferred
            .into_iter()
            .chain(pending.approved)
            .collect(),
        provider_held: pending.provider_held,
        intents,
    };
    if args_cut {
        // A crash may preserve the assistant's calls but lose their error results. Calls
        // without a dispatch intent have no possible side effect, so refuse them here.
        // A dispatch intent without a result already stops the run for review; replay
        // never sends that potentially executed call through this recovery path.
        // A Prepared or approved call is not one of them: the live path gated it, which it
        // never does for a cut call, so it keeps its approval state and stays in the gate.
        let rejected = calls.iter()
            .filter(|call| !resume.existing.contains_key(&call.tool_call_id)
                && !resume.prepared.iter().any(|prepared| matches!(&*prepared.prepared,
                    WalRecord::ToolCallDeferred { call_id, .. } if call_id == &call.tool_call_id)))
            .map(|call| error_result(call,
                "tool_args_trailing_garbage: the cut attempt's rejection details were not committed before interruption (json_bytes=unavailable, whitespace_bytes=unavailable). Re-issue the call with valid, properly escaped arguments."
            )).collect();
        let (records, recovered) = tool_args_rejection_records(
            resolved_run_id,
            pending.step_id,
            &calls,
            rejected,
            resume.batch_already_started,
        );
        writer.record_group(records)?;
        if let Some(recovered) = recovered {
            resume.batch_already_started = recovered.batch_already_started;
            resume.existing.extend(recovered.existing);
        }
    }
    let mut batch = dispatch_batch_controlled(
        resolved_run_id,
        pending.step_id,
        &req.session,
        req.scope.as_ref(),
        calls,
        manifest,
        tool_plane,
        approval,
        writer,
        Some(resume),
        budget_firings,
        emit_control_payloads,
        stop,
        hooks,
        warm,
        out,
        control,
        history,
        req.on_restart == Some(broca_protocol::request::OnRestart::Resume),
    )
    .await?;
    if matches!(
        batch.interruption,
        Some(BatchInterruption::RestartPause | BatchInterruption::ProviderHeldPause)
    ) {
        return Ok((
            None,
            batch.open,
            batch.interruption == Some(BatchInterruption::ProviderHeldPause),
        ));
    }

    // As on the uninterrupted path, dispatch persistence is complete before the model-facing copy
    // receives loop guidance. Existing results from the partial batch are folded here as well.
    let original_results = originals.as_ref().map(|_| {
        Message::new(
            Role::Tool,
            batch
                .results
                .iter()
                .cloned()
                .map(ContentBlock::ToolResult)
                .collect(),
        )
    });
    attach_repetition_guidance(
        repetition_observer,
        pending.assistant.tool_calls(),
        &mut batch.results,
    );

    // Append the now-complete tool turn (assistant + results) to the prompt. The assistant
    // message and the results that were durable before the resume take their tags from the
    // replayed records (`pending.assistant_tags`, `pending.existing_tags`); each result this
    // resume dispatched takes the tag the writer assigned when it appended that result's
    // record (`WalHandle::take_call_tags`).
    let mut answer = Message {
        cache_prefix_blocks: None,
        role: Role::Assistant,
        content: pending.assistant.content,
        origin: Some(origin),
        ordinal: None,
        tags: Default::default(),
    };
    crate::tags::attach(&mut answer, assistant_tags.as_ref());
    prompt.push(answer);
    // Recovered stored-ledger results keep their recorded tags; computed-tag results are
    // numbered with the rest of the appended prompt at the next step boundary.
    let recovered_result_tags = if stored_tag_rules {
        live_result_tags(writer, &batch.results, &mut existing_tags)
    } else {
        None
    };
    let mut results = Message {
        cache_prefix_blocks: None,
        role: Role::Tool,
        content: batch
            .results
            .into_iter()
            .map(ContentBlock::ToolResult)
            .collect(),
        origin: None,
        ordinal: None,
        tags: Default::default(),
    };
    if let Some(tags) = recovered_result_tags {
        crate::tags::attach_parts(&mut results, Some(tags));
    }
    if let (Some(originals), Some(original)) = (originals, original_results) {
        originals.retain_change(prompt.len(), original, &results);
    }
    prompt.push(results);

    Ok((
        BatchOutcome::finish_reason(batch.interruption),
        Vec::new(),
        false,
    ))
}

/// Return the durable request only when no blocking renderer can still hold its snapshot.
/// This intentionally uses `Arc::get_mut`, never `Arc::make_mut`: copying a multi-megabyte
/// prompt while a render is in flight would move the same head-of-line work back to the actor.
fn mutable_call_options(opts: &mut Arc<CallOptions>) -> &mut CallOptions {
    debug_assert_eq!(
        Arc::strong_count(opts),
        1,
        "the durable prompt must not be appended while a render job owns its snapshot"
    );
    Arc::get_mut(opts).expect("the durable prompt is exclusively owned before append")
}

enum StepOutcome {
    Completed {
        tool_args_cuts: Vec<broca_protocol::provider::ToolArgsTrailingGarbage>,
        message: AssistantMessage,
        usage: Usage,
        retries_used: u32,
        /// The provider's own terminal finish reason (e.g. `length` when the model
        /// hit the token ceiling, `content_filter` when output was filtered). `None`
        /// when the provider didn't supply one. Carried through so the caller sees
        /// WHY the model stopped — these are signals a harness acts on (continue on
        /// `length`, surface `content_filter`), and recomputing from message shape
        /// alone would silently drop them.
        finish_reason: Option<FinishReason>,
        /// The service tier the provider's response reported for this step, verbatim; `None`
        /// when the response reported none. Never the requested tier.
        service_tier_used: Option<String>,
        /// Anthropic's reported `usage.speed`, verbatim; `None` when not reported.
        speed_used: Option<String>,
    },
    Failed {
        error: ProviderError,
        retries_used: u32,
    },
}

/// Whether the run's cancel token (if any) has been tripped.
fn is_cancelled(control: &RunControl) -> bool {
    control.cancel.as_ref().is_some_and(|t| t.is_cancelled())
}

fn boundary_cancelled(control: &RunControl) -> bool {
    control
        .steer
        .as_ref()
        .is_some_and(|inbox| inbox.boundary_cancel().is_cancelled())
}

/// Whether the module has asked the run to stop (see [`RunControl::stop_requested`]).
fn stop_is_requested(stop: Option<&CancellationToken>) -> bool {
    stop.is_some_and(CancellationToken::is_cancelled)
}

/// Drive one model step under cancellation and the optional provider-call watchdog. The result
/// identifies the signal that won before the step produced its terminal, so a caller cancellation
/// remains distinct from a watchdog timeout before the run terminal is recorded. Timeout-bearing
/// calls use an isolated step token so a watchdog can tear down provider work without cancelling the
/// run.
async fn run_step_watched<P: Provider>(
    provider: &P,
    step_id: StepId,
    opts: &Arc<CallOptions>,
    control: &RunControl,
    out: &mpsc::Sender<OutputEvent>,
    on_attempt: &AttemptSink<'_>,
) -> Result<Result<StepOutcome, RunFinishReason>, WalWriteError> {
    struct ProgressGuard(Option<crate::provider::LiveStepProgress>);
    impl Drop for ProgressGuard {
        fn drop(&mut self) {
            if let Some(progress) = &self.0 {
                *progress.lock().unwrap() = None;
            }
        }
    }
    let _progress_guard = ProgressGuard(control.step_progress.clone());
    if let Some(progress) = &control.step_progress {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        *progress.lock().unwrap() = Some(broca_protocol::provider::InFlightStep {
            step_id,
            started_at_ms: now,
            attempt: 0,
            attempt_started_at_ms: now,
            last_content_at_ms: None,
            max_content_gap_ms: None,
            max_content_gap_ended_at_ms: None,
        });
    }
    // The renderer gets a shared handle to the durable snapshot, never a copy of the prompt.
    let render_opts = Arc::clone(opts);

    let step_cancel = match (control.cancel.as_ref(), control.provider_call_timeout) {
        // A watchdog cancels the step token directly, so its token must not share the run's identity.
        (Some(run_cancel), Some(_)) => run_cancel.child_token(),
        // Without a watchdog, only parent cancellation can trip the step token; sharing avoids the
        // child token's allocation while preserving propagation into provider work.
        (Some(run_cancel), None) => run_cancel.clone(),
        (None, _) => CancellationToken::new(),
    };
    // The step's provider stream, parked outside the step future so it outlives a cancel.
    let mut events: Option<mpsc::Receiver<NormalizedEvent>> = None;
    let outcome = {
        let watchdog_deadline = control
            .provider_call_timeout
            .map(|timeout| tokio::time::Instant::now() + timeout);
        let step = run_step(
            provider,
            step_id,
            render_opts,
            &step_cancel,
            watchdog_deadline,
            out,
            &mut events,
            on_attempt,
            control.shutdown_seal.as_ref(),
            control.step_progress.clone(),
        );
        tokio::pin!(step);

        match watchdog_deadline {
            Some(deadline) => {
                tokio::select! {
                    biased;
                    _ = step_cancel.cancelled() => Ok(Err(RunFinishReason::Cancelled)),
                    outcome = &mut step => outcome.map(Ok),
                    _ = tokio::time::sleep_until(deadline) => {
                        step_cancel.cancel();
                        Ok(Err(RunFinishReason::Interrupted))
                    }
                }
            }
            None => {
                tokio::select! {
                    biased;
                    _ = step_cancel.cancelled() => Ok(Err(RunFinishReason::Cancelled)),
                    outcome = &mut step => outcome.map(Ok),
                }
            }
        }
    };
    let outcome = attribute_step_failure_to_cancel(outcome?, is_cancelled(control));
    // A cancel or watchdog stops the step cooperatively: the provider records its in-flight
    // attempt and then closes the stream. Keep reading until then (bounded), so that record is
    // appended BEFORE the caller writes the run's terminal. Only attempt records are taken;
    // anything else the provider still sends belongs to a step that no longer counts.
    if let (Err(_), Some(events), true) = (
        &outcome,
        events.as_mut(),
        provider.reports_attempt_evidence(),
    ) {
        let drained = tokio::time::timeout(CANCELLED_STEP_ATTEMPT_DRAIN, async {
            while let Some(event) = events.recv().await {
                if let NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) =
                    event
                {
                    on_attempt(attempt).await?;
                }
            }
            Ok::<(), WalWriteError>(())
        })
        .await;
        if let Ok(result) = drained {
            result?;
        }
    }
    // If cancellation or the watchdog drops this future while a blocking render is still
    // running, the durable prompt remains in `opts`; only the render job owns the extra Arc.
    // The job drops that final snapshot reference on its blocking worker, never on this actor.
    Ok(outcome)
}

/// A step that FAILED while the run's cancel was tripped is a cancellation, not an error.
///
/// The biased select above only sees the cancel if the token is already set when it checks. On
/// a multi-threaded runtime the provider's transport can observe the cancel on another thread,
/// record the in-flight attempt and close the stream in the gap between that check and the
/// step's poll. run_step then sees a stream that ended without Finished and returns Failed, so
/// the run ended as Error for a caller's cancel. Measured: once in CI on Linux, in
/// a_caller_cancel_mid_stream_is_recorded_before_the_cancelled_terminal.
///
/// Only a FAILED outcome is reattributed. A step that completed keeps its result: real work
/// finished, and the next check of the cancel token still ends the run as Cancelled.
fn attribute_step_failure_to_cancel(
    outcome: Result<StepOutcome, RunFinishReason>,
    run_cancelled: bool,
) -> Result<StepOutcome, RunFinishReason> {
    match outcome {
        Ok(StepOutcome::Failed { .. }) if run_cancelled => Err(RunFinishReason::Cancelled),
        other => other,
    }
}

/// Receives each provider attempt record of the current step, in order, as it arrives.
type AttemptFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), WalWriteError>> + Send + 'a>>;
type AttemptSink<'a> = dyn Fn(ModelAttempt) -> AttemptFuture<'a> + Send + Sync + 'a;

/// How long a cancelled step's stream is read for its last attempt record. The transport sends
/// that record and closes the stream without any network wait, so this bound is only reached
/// if the provider task is starved; the record is then lost rather than the cancel delayed.
const CANCELLED_STEP_ATTEMPT_DRAIN: std::time::Duration = std::time::Duration::from_secs(5);

/// Drive ONE model step: forward display deltas, capture the assembled message +
/// usage, end on the provider's terminal `Finished`/`Error`.
// The stream slot and attempt sink are borrowed from the watch site so a cancelled step's stream
// outlives this future; grouping them would only hide that ownership.
#[allow(clippy::too_many_arguments)]
async fn run_step<P: Provider>(
    provider: &P,
    step_id: StepId,
    opts: Arc<CallOptions>,
    cancel: &CancellationToken,
    deadline: Option<tokio::time::Instant>,
    out: &mpsc::Sender<OutputEvent>,
    events: &mut Option<mpsc::Receiver<NormalizedEvent>>,
    on_attempt: &AttemptSink<'_>,
    shutdown_seal: Option<&crate::ShutdownSeal>,
    progress: Option<crate::provider::LiveStepProgress>,
) -> Result<StepOutcome, WalWriteError> {
    let _ = send(
        out,
        OutputEvent::Control(ControlUnit::StepStarted { step_id }),
    )
    .await;
    let stream = provider
        .step_with_runtime(
            step_id,
            opts,
            crate::provider::StepRuntime {
                cancel: cancel.clone(),
                deadline,
                seal: shutdown_seal.cloned(),
                progress,
            },
        )
        .await;
    let rx = events.insert(stream);

    let mut message: Option<AssistantMessage> = None;
    let mut tool_args_cuts = Vec::new();
    let mut usage = Usage::default();
    let mut finish_reason: Option<FinishReason> = None;
    let mut service_tier_used: Option<String> = None;
    let mut speed_used: Option<String> = None;
    let mut retries_used = 0;
    let mut finished = false;
    while let Some(ev) = rx.recv().await {
        match ev {
            NormalizedEvent::Display(d) => {
                let _ = send(out, OutputEvent::Display(d)).await;
            }
            NormalizedEvent::Control(c) => match c {
                ProviderControl::AssistantMessage { message: m } => message = Some(m),
                ProviderControl::ToolArgsTrailingGarbage { cut } => tool_args_cuts.push(cut),
                ProviderControl::StepFinished {
                    usage: u,
                    finish_reason: fr,
                    service_tier_used: tier,
                    speed_used: speed,
                    ..
                } => {
                    usage = u;
                    finish_reason = Some(fr);
                    // Recorded exactly as the provider reported it; absent stays absent.
                    service_tier_used = tier;
                    speed_used = speed;
                }
                ProviderControl::Finished {
                    usage: u,
                    finish_reason: fr,
                    retries_used: r,
                } => {
                    retries_used = r;
                    if usage == Usage::default() {
                        usage = u;
                    }
                    // The provider's terminal reason (the StepFinished above may not
                    // arrive for every grammar; the Finished event always does).
                    finish_reason = Some(fr);
                    finished = true;
                    break;
                }
                ProviderControl::Error {
                    error,
                    retries_used,
                } => {
                    return Ok(StepOutcome::Failed {
                        error,
                        retries_used,
                    })
                }
                ProviderControl::StreamStart { .. } | ProviderControl::StepStarted { .. } => {}
                // Attempt evidence is recorded as it arrives; it never ends the step.
                ProviderControl::AttemptFinished { attempt } => on_attempt(attempt).await?,
            },
        }
    }

    Ok(match (message, finished) {
        (Some(message), true) => StepOutcome::Completed {
            tool_args_cuts,
            message,
            usage,
            retries_used,
            finish_reason,
            service_tier_used,
            speed_used,
        },
        // A message is valid only after the provider emits the protocol's final Finished control.
        // HTTP transports retry a missing final control; other providers fail safely here.
        _ => StepOutcome::Failed {
            retries_used: 0,
            error: ProviderError { provider_details: None, provider_subcode: None,
            refusal: None,
            class: broca_protocol::ErrorClass::ProviderUnavailable,
            message: "provider stream ended without an assembled assistant message and Finished control"
                .into(),
            status: None,
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None, },
        },
    })
}

/// Per-call plan after duplicate-id validation and the capability + approval gates. A
/// `NoDispatch` call's durable terminal is a `tool_no_dispatch` record; a `Synthetic` call
/// gets a `tool_result` without reaching the plane; and a `Dispatch` call gets an intent
/// followed by a `tool_result`.
enum CallPlan {
    /// Approved (input possibly narrowed) — dispatch through the plane.
    Dispatch(PlannedDispatch),
    /// Refused before dispatch (capability/denial/plane-down). Carries ONLY the
    /// durable `tool_no_dispatch` reason; the result is derived from it via the
    /// SHARED `no_dispatch_result` — the same fn replay uses — so the live and
    /// reconstructed results are byte-identical by construction.
    NoDispatch {
        tool_call_id: ToolCallId,
        reason: NoDispatchReason,
        hooks: Option<Box<broca_wal::Hooks>>,
    },
    /// Hook-made terminal is durable at barrier 1 and must never be written at barrier 2.
    HookTerminal {
        result: Arc<ToolResult>,
        reason: String,
        hooks: Option<Box<broca_wal::Hooks>>,
    },
    /// A durable error result, in the shape a provider accepts, written without sending the
    /// call to the tool plane. Two cases: the provider emitted the same call id more than once
    /// in one batch (only the first instance may dispatch), or the tool plane reported at batch
    /// start that the run's scope or its parent is gone. In the second case `reason` names why
    /// and is written as the `tool_result` record's typed reason.
    Synthetic {
        result: Arc<ToolResult>,
        reason: Option<String>,
    },
    /// RESUME ONLY: this call already has a durable terminal in the WAL — include
    /// its reconstructed result as-is, write NO new record, NEVER re-dispatch.
    Already(ToolResult),
    /// The module asked the run to stop before this call was sent, so it is never sent. It gets
    /// a `tool_result` with outcome `Interrupted` and the not-dispatched text, the same terminal
    /// a resume writes for a planned call that never left the process.
    Stopped(ToolCallId),
}

/// An approved call whose argument allocation is shared with its intent record until the
/// pre-dispatch durability barrier completes.
struct PlannedDispatch {
    tool_call_id: ToolCallId,
    tool_name: String,
    input: Arc<serde_json::Value>,
    thought_signature: Option<String>,
    hooks: Option<Box<broca_wal::Hooks>>,
    call_key: Option<String>,
    approved: Option<String>,
}

impl From<ToolCall> for PlannedDispatch {
    fn from(call: ToolCall) -> Self {
        Self {
            tool_call_id: call.tool_call_id,
            tool_name: call.tool_name,
            input: Arc::new(call.input),
            thought_signature: call.thought_signature,
            hooks: None,
            call_key: None,
            approved: None,
        }
    }
}

impl PlannedDispatch {
    fn into_tool_call(self) -> ToolCall {
        ToolCall {
            tool_call_id: self.tool_call_id,
            tool_name: self.tool_name,
            input: Arc::unwrap_or_clone(self.input),
            thought_signature: self.thought_signature,
        }
    }
}

/// Resume context for `dispatch_batch` (`None` for a fresh batch). On resume, the
/// `tool_batch_started` may already be durable (don't rewrite it — that would
/// reset the batch if we crash and replay again), and the already-terminated
/// calls' reconstructed results are included as-is.
// Default is an unopened batch with no existing terminal, approval, held slot or dispatch intent.
#[derive(Default)]
struct ResumeCtx {
    batch_already_started: bool,
    existing: std::collections::HashMap<ToolCallId, std::collections::VecDeque<ToolResult>>,
    prepared: Vec<crate::replay::PreparedCall>,
    provider_held: Vec<crate::replay::ProviderHeldCall>,
    intents: std::collections::HashMap<ToolCallId, PlannedDispatch>,
}

fn held_resume_intents(
    pending: &PendingTurn,
    frames: &[broca_wal::FramedRecord],
    writer: &WalHandle,
) -> Result<std::collections::HashMap<ToolCallId, PlannedDispatch>, RunError> {
    let mut intents = std::collections::HashMap::new();
    for frame in frames {
        if let WalRecord::ToolDispatchIntent {
            batch_id,
            tool_call_id,
            args,
            call_key,
            ..
        } = &frame.record
        {
            if Some(batch_id) != pending.batch_id.as_ref()
                || !(pending.held_successors.contains(tool_call_id)
                    || pending
                        .provider_held
                        .iter()
                        .any(|held| &held.tool_call_id == tool_call_id))
            {
                continue;
            }
            let Some(call) = pending
                .assistant
                .tool_calls()
                .find(|call| &call.tool_call_id == tool_call_id)
            else {
                continue;
            };
            let mut dispatch = PlannedDispatch::from(call.clone());
            dispatch.input = Arc::clone(args);
            dispatch.call_key = call_key.clone().or_else(|| {
                writer
                    .lineage_id()
                    .map(|lineage| crate::context::call_key(lineage, frame.seq))
            });
            if let Some(key) = call_key {
                dispatch.approved = frames.iter().find_map(|frame| match &frame.record {
                    WalRecord::ToolCallDeferred { args_jcs, .. }
                        if writer.lineage_id().is_some_and(|lineage| {
                            crate::context::call_key(lineage, frame.seq) == *key
                        }) =>
                    {
                        Some(args_jcs.clone())
                    }
                    _ => None,
                });
            }
            intents.insert(tool_call_id.clone(), dispatch);
        }
    }
    if intents.len() != pending.provider_held.len() + pending.held_successors.len() {
        return Err(RunError::Wal(
            "provider-held resume lacks its durable dispatch intents".into(),
        ));
    }
    Ok(intents)
}

fn step_attempt_was_args_cut(frames: &[broca_wal::FramedRecord], step_id: StepId) -> bool {
    frames.iter().rev()
        .take_while(|frame| !matches!(frame.record, WalRecord::StepStarted { .. } | WalRecord::RunStarted { .. }))
        .any(|frame| matches!(&frame.record, WalRecord::ModelAttemptFinished { step_id: attempted_step, outcome, .. }
            if *attempted_step == step_id && outcome.as_str() == "tool_args_trailing_garbage"))
}

fn tool_args_rejection_records(
    run_id: &RunId,
    step_id: StepId,
    calls: &[ToolCall],
    rejected: Vec<ToolResult>,
    batch_already_started: bool,
) -> (Vec<WalRecord>, Option<ResumeCtx>) {
    if rejected.is_empty() {
        return (Vec::new(), None);
    }
    let batch_id = BatchId(format!("{}-b{}", run_id.0, step_id.0));
    let mut records = Vec::new();
    if !batch_already_started {
        records.push(WalRecord::ToolBatchStarted {
            batch_id: batch_id.clone(),
            step_id,
            planned_calls: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
        });
    }
    let mut existing = std::collections::HashMap::new();
    for result in rejected {
        records.push(WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            hooks: None,
            batch_id: batch_id.clone(),
            result: Arc::new(result.clone()),
            outcome: DispatchOutcome::Completed,
            reason: Some("tool_args_trailing_garbage".into()),
            withdraw_elicitation: None,
            provider_held: None,
        });
        existing
            .entry(result.tool_call_id.clone())
            .or_insert_with(std::collections::VecDeque::new)
            .push_back(result);
    }
    (
        records,
        Some(ResumeCtx {
            batch_already_started: true,
            existing,
            prepared: Vec::new(),
            provider_held: Vec::new(),
            intents: std::collections::HashMap::new(),
        }),
    )
}

/// Source-ordered results and the typed cause of a batch stop. Caller cancellation
/// remains distinct from shutdown, scope loss, and ambiguous dispatched outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchInterruption {
    ProviderHeldPause,
    CallerCancel,
    StopRequested,
    Ambiguous,
    ScopeDrained,
    RestartPause,
}

struct BatchOutcome {
    results: Vec<ToolResult>,
    interruption: Option<BatchInterruption>,
    open: Vec<approval_gate::Held>,
}

impl BatchOutcome {
    fn finish_reason(interruption: Option<BatchInterruption>) -> Option<RunFinishReason> {
        match interruption {
            Some(BatchInterruption::CallerCancel) => Some(RunFinishReason::Cancelled),
            Some(BatchInterruption::StopRequested) => Some(RunFinishReason::Interrupted),
            Some(BatchInterruption::RestartPause | BatchInterruption::ProviderHeldPause) => None,
            Some(BatchInterruption::Ambiguous | BatchInterruption::ScopeDrained) => {
                Some(RunFinishReason::Interrupted)
            }
            None => None,
        }
    }
}

/// Outcome of resolving ONE planned call: the provider-valid result, whether it
/// was ambiguous (stops the run), and its WAL `tool_result` outcome tag — `None`
/// for a `NoDispatch` call (its terminal is the `tool_no_dispatch` record from the
/// plan barrier, so it gets NO `tool_result` record). A synthetic result uses a
/// `Completed` outcome so it is durable without ever creating a dispatch intent.
/// The same shared `Arc<ToolResult>` is retained while the WAL record is serialized and
/// fsynced, so crossing the durability barrier does not create a second payload copy.
struct CallResolution {
    result: Arc<ToolResult>,
    ambiguous: bool,
    /// The call was never sent because the module asked the run to stop or the caller
    /// cancelled it; the run ends after this batch.
    stopped: bool,
    outcome: Option<DispatchOutcome>,
    /// The typed reason written on the call's `tool_result` record. Set only when the route the
    /// call was to use, opened under the run's scope, closed and the tool plane reported why,
    /// such as `scope_ended` or `scope_agent_changed`. `None` otherwise.
    reason: Option<String>,
    hooks: Option<Box<broca_wal::Hooks>>,
    hook_stage: Option<crate::shutdown_seal::HookStage>,
    plane_subject: bool,
}

impl CallResolution {
    fn plain(result: ToolResult, outcome: Option<DispatchOutcome>) -> Self {
        Self {
            result: Arc::new(result),
            ambiguous: false,
            stopped: false,
            outcome,
            reason: None,
            hooks: None,
            hook_stage: None,
            plane_subject: false,
        }
    }
}
#[allow(clippy::too_many_arguments)]
async fn dispatch_batch<T: ToolPlane, A: ApprovalPolicy>(
    run_id: &RunId,
    step_id: StepId,
    session: &BindIdentity,
    scope: Option<&broca_protocol::SessionScope>,
    calls: Vec<ToolCall>,
    manifest: &ToolManifest,
    tool_plane: &T,
    approval: &A,
    writer: &WalHandle,
    resume: Option<ResumeCtx>,
    budget_firings: &[BudgetFiring],
    emit_control_payloads: bool,
    stop: Option<&CancellationToken>,
    hooks: Option<&crate::hooks::HookRuntime<'_>>,
    warm: Option<crate::keep_warm_wait::WarmWait<'_>>,
    out: &mpsc::Sender<OutputEvent>,
    ordered: Option<&RunControl>,
) -> Result<BatchOutcome, RunError> {
    let batch_id = BatchId(format!("{}-b{}", run_id.0, step_id.0));
    let ctx = BatchCtx {
        batch_id: batch_id.clone(),
        bind: session.clone(),
        run_id: run_id.clone(),
        // The episode's frozen scope: every provider route this batch opens carries it, so a
        // route is only ever opened under the scope this run was admitted under.
        scope: scope.cloned(),
    };
    // The caller's immediate cancellation stops every call not yet sent, like a stop request.
    // Calls already sent finish and keep their outcomes. Boundary cancellation is a separate
    // token that lets the batch finish, so it is not consulted here.
    let cancel = ordered.and_then(|control| control.cancel.as_ref());
    // Validate ids before making the batch durable. The first instance remains eligible for
    // dispatch; later instances are represented by synthetic error results in source order.
    let mut seen_call_ids = HashSet::new();
    let duplicate_calls: Vec<bool> = calls
        .iter()
        .map(|call| !seen_call_ids.insert(call.tool_call_id.clone()))
        .collect();
    // On resume, the batch may already be durable — re-writing tool_batch_started
    // would reset the batch if we crash and replay again, so skip it.
    if !resume
        .as_ref()
        .map(|r| r.batch_already_started)
        .unwrap_or(false)
    {
        let call_ids: Vec<ToolCallId> = calls.iter().map(|c| c.tool_call_id.clone()).collect();
        writer.record(WalRecord::ToolBatchStarted {
            batch_id: batch_id.clone(),
            step_id,
            planned_calls: call_ids,
        });
    }
    // What the tool plane's `batch_started` answers decides how the calls that would dispatch
    // are planned: `None` dispatches them; otherwise each gets a durable terminal record and is
    // never sent to the tool plane.
    enum BatchRefusal {
        /// The tool plane is unavailable or refused the route: each call gets a durable
        /// `tool_no_dispatch`.
        Unavailable(String),
        /// With nothing in flight, the tool plane found the run's scope or its parent gone:
        /// each call gets a `route_drained` result naming the reason, and the run stops
        /// Interrupted after this batch.
        ScopeDrained(String),
        /// The module asked the run to stop before any call of the batch was planned: no call
        /// is sent, each gets the not-dispatched result, and the run stops Interrupted after
        /// this batch. The tool plane is never told the batch started.
        Stopped,
    }
    // A stop requested before this point refuses the whole batch before anything is written
    // for its calls: no dispatch intent, and no route opened by the tool plane. An immediate
    // cancellation that arrived after the model step refuses it the same way.
    let cancelled_before_batch = cancel.is_some_and(CancellationToken::is_cancelled);
    let batch_refusal = if stop_is_requested(stop) || cancelled_before_batch {
        Some(BatchRefusal::Stopped)
    } else {
        match tool_plane.batch_started(&ctx, &calls, manifest).await {
            Ok(()) => None,
            // An ordinary tool-plane failure keeps the exact detail text it always had: sessions
            // already on disk hold it as their durable no-dispatch reason.
            Err(ToolPlaneError::Unavailable(_)) => {
                Some(BatchRefusal::Unavailable("tool plane unavailable".into()))
            }
            Err(error @ ToolPlaneError::ScopeRefused { .. }) => {
                Some(BatchRefusal::Unavailable(error.to_string()))
            }
            Err(ToolPlaneError::ScopeDrained { reason }) => {
                Some(BatchRefusal::ScopeDrained(reason))
            }
        }
    };
    let scope_stopped = matches!(batch_refusal, Some(BatchRefusal::ScopeDrained(_)));
    let batch_stopped = matches!(batch_refusal, Some(BatchRefusal::Stopped));
    let (mut existing, mut existing_intents, resumed_held) = resume
        .map(|r| (r.existing, r.intents, r.provider_held))
        .unwrap_or_default();
    let durable_intents: HashSet<_> = existing_intents.keys().cloned().collect();

    // Gate every call (sequential, cheap): capability then approval. A call that
    // already has a durable terminal (resume) is Already — never re-gated/dispatched.
    let mut plan: Vec<CallPlan> = Vec::with_capacity(calls.len());
    let mut gating_stop = None;
    for (mut call, is_duplicate) in calls.into_iter().zip(duplicate_calls) {
        if emit_control_payloads {
            // Direct loop consumers observe proposed calls here. Module subscribers reconstruct
            // ToolCall from the durable ModelStepFinished record, so skipping this clone is safe
            // when the module uses the WAL projector as its control source.
            let _ = send(
                out,
                OutputEvent::Control(ControlUnit::ToolCall { call: call.clone() }),
            )
            .await;
        }

        if let Some(prior) = existing
            .get_mut(&call.tool_call_id)
            .and_then(|results| results.pop_front())
        {
            plan.push(CallPlan::Already(prior));
            continue;
        }
        if let Some(dispatch) = existing_intents.remove(&call.tool_call_id) {
            plan.push(CallPlan::Dispatch(dispatch));
            continue;
        }
        if gating_stop.is_some() {
            plan.push(CallPlan::Stopped(call.tool_call_id.clone()));
            continue;
        }
        if is_duplicate {
            plan.push(CallPlan::Synthetic {
                result: Arc::new(error_result(&call, "duplicate tool_call_id from provider")),
                reason: None,
            });
            continue;
        }
        match &batch_refusal {
            None => {}
            Some(BatchRefusal::Unavailable(detail)) => {
                plan.push(CallPlan::NoDispatch {
                    tool_call_id: call.tool_call_id.clone(),
                    reason: NoDispatchReason::Unavailable {
                        detail: detail.clone(),
                    },
                    hooks: None,
                });
                continue;
            }
            Some(BatchRefusal::ScopeDrained(reason)) => {
                plan.push(CallPlan::Synthetic {
                    result: Arc::new(route_drained_result(&call.tool_call_id, reason)),
                    reason: Some(reason.clone()),
                });
                continue;
            }
            Some(BatchRefusal::Stopped) => {
                plan.push(CallPlan::Stopped(call.tool_call_id.clone()));
                continue;
            }
        }
        if let Err(v) = manifest.validate_call(&call) {
            plan.push(CallPlan::NoDispatch {
                tool_call_id: call.tool_call_id.clone(),
                reason: NoDispatchReason::ValidationFailed {
                    detail: v.to_string(),
                },
                hooks: None,
            });
            continue;
        }
        let original = call.input.clone();
        let mut trace = crate::hooks::Trace::default();
        if let Some(runtime) = hooks {
            let output = runtime
                .chain(
                    pre_tool_subject(
                        step_id,
                        &call,
                        cortexkit_role_step_transform_provider::subscription::Phase::Mutate,
                    ),
                    trace,
                    |_| true,
                )
                .await;
            trace = output.trace;
            if let Some(cause) = output.cancellation {
                gating_stop = Some(match cause {
                    crate::hooks::HookCancellation::CallerCancel => BatchInterruption::CallerCancel,
                    crate::hooks::HookCancellation::StopRequested
                    | crate::hooks::HookCancellation::ShutdownSeal => {
                        BatchInterruption::StopRequested
                    }
                });
                plan.push(CallPlan::Stopped(call.tool_call_id.clone()));
                continue;
            }
            if let crate::hooks::Subject::PreTool { input, .. } = output.subject {
                call.input = input;
            }
            if let Some((provider, text, unavailable)) = output.refusal {
                plan.push(pre_tool_terminal(
                    &call,
                    &original,
                    &trace,
                    &provider,
                    &text,
                    unavailable,
                ));
                continue;
            }
        }
        if let Err(v) = manifest.validate_call(&call) {
            plan.push(CallPlan::NoDispatch {
                tool_call_id: call.tool_call_id.clone(),
                reason: NoDispatchReason::ValidationFailed {
                    detail: v.to_string(),
                },
                hooks: trace.metadata(original, call.input.clone(), true),
            });
            continue;
        }
        // Policy must see the input produced by mutate, after capability validation.
        match approval.decide(&call).await {
            ApprovalResponse::Allow { .. } => {}
            ApprovalResponse::AllowNarrowed { input, .. } => call.input = input,
            ApprovalResponse::Deny { reason, .. } => {
                plan.push(CallPlan::NoDispatch {
                    tool_call_id: call.tool_call_id.clone(),
                    reason: NoDispatchReason::Denied { reason },
                    hooks: trace.metadata(original, call.input.clone(), true),
                });
                continue;
            }
        }
        if let Some(runtime) = hooks {
            let output = runtime
                .chain(
                    pre_tool_subject(
                        step_id,
                        &call,
                        cortexkit_role_step_transform_provider::subscription::Phase::Validate,
                    ),
                    trace,
                    |_| true,
                )
                .await;
            trace = output.trace;
            if let Some(cause) = output.cancellation {
                gating_stop = Some(match cause {
                    crate::hooks::HookCancellation::CallerCancel => BatchInterruption::CallerCancel,
                    crate::hooks::HookCancellation::StopRequested
                    | crate::hooks::HookCancellation::ShutdownSeal => {
                        BatchInterruption::StopRequested
                    }
                });
                plan.push(CallPlan::Stopped(call.tool_call_id.clone()));
                continue;
            }
            if let Some((provider, text, unavailable)) = output.refusal {
                plan.push(pre_tool_terminal(
                    &call,
                    &original,
                    &trace,
                    &provider,
                    &text,
                    unavailable,
                ));
                continue;
            }
        }
        let metadata = trace.metadata(original, call.input.clone(), false);
        let mut dispatch = PlannedDispatch::from(call);
        dispatch.hooks = metadata;
        plan.push(CallPlan::Dispatch(dispatch));
    }

    if gating_stop.is_some() {
        for entry in &mut plan {
            if let CallPlan::Dispatch(call) = entry {
                *entry = CallPlan::Stopped(call.tool_call_id.clone());
            }
        }
    }
    let has_dispatch = plan.iter().any(|p| matches!(p, CallPlan::Dispatch(_)));
    if has_dispatch {
        // BARRIER 1: before any tool call is sent, fsync the batch start and a
        // durable plan for each call. Dispatchable calls get an intent record,
        // refused calls get a terminal no_dispatch record, and calls already known to
        // be finished need no new record. That way, a crash before a dispatch
        // finishes leaves a durable "maybe ran" intent, while refused calls will not
        // be dispatched again.
        for p in &mut plan {
            match p {
                CallPlan::Dispatch(c) => {
                    if durable_intents.contains(&c.tool_call_id) {
                        continue;
                    }
                    let seq = writer
                        .record(WalRecord::ToolDispatchIntent {
                            dispatched_at_ms: None,
                            hooks: c.hooks.clone(),
                            batch_id: batch_id.clone(),
                            tool_call_id: c.tool_call_id.clone(),
                            // Store the tool name here only so replay and UI display can
                            // show it. When rebuilding the prompt, the assistant message's
                            // tool_calls carry the authoritative name.
                            tool_name: Some(c.tool_name.clone()),
                            args: Arc::clone(&c.input),
                            // The module the plane resolved for this call. An intent naming one
                            // requires `dispatch-module/v1`, and the WAL raises its feature floor
                            // to that name before the first such frame.
                            module_id: tool_plane.dispatch_target(&ctx, &c.tool_call_id),
                            // A session's frozen tools carry no schema digest yet (a fetched
                            // catalog's `schema_digest` and `semantics` are not part of the
                            // frozen `Tool`), so there is no pin to record.
                            schema_pin: None,
                            approval_digest: None,
                            call_key: None,
                        })
                        .into_result()?;
                    c.call_key = writer
                        .lineage_id()
                        .map(|lineage| crate::context::call_key(lineage, seq));
                }
                CallPlan::NoDispatch {
                    tool_call_id,
                    reason,
                    hooks,
                } => {
                    writer.record(WalRecord::ToolNoDispatch {
                        dispatched_at_ms: None,
                        tags: None,
                        hooks: hooks.clone(),
                        batch_id: batch_id.clone(),
                        tool_call_id: tool_call_id.clone(),
                        reason: reason.clone(),
                    });
                }
                CallPlan::HookTerminal {
                    result,
                    reason,
                    hooks,
                } => {
                    writer.record(WalRecord::ToolResult {
                        finished_at_ms: None,
                        tags: None,
                        hooks: hooks.clone(),
                        batch_id: batch_id.clone(),
                        result: Arc::clone(result),
                        outcome: DispatchOutcome::Completed,
                        reason: Some(reason.clone()),
                        withdraw_elicitation: None,
                        provider_held: None,
                    });
                }
                CallPlan::Synthetic { .. } | CallPlan::Already(_) | CallPlan::Stopped(_) => {}
            }
        }
        writer.sync().await?;
    } else {
        // In this branch, no call will be sent out: every call is either refused or
        // already has a durable terminal record. Because of that, we can fsync the
        // remaining terminal records together with batch_finished and turn_finished in
        // one barrier, then emit results afterward.
        for p in &plan {
            match p {
                CallPlan::NoDispatch {
                    tool_call_id,
                    reason,
                    hooks,
                } => {
                    writer.record(WalRecord::ToolNoDispatch {
                        dispatched_at_ms: None,
                        tags: None,
                        hooks: hooks.clone(),
                        batch_id: batch_id.clone(),
                        tool_call_id: tool_call_id.clone(),
                        reason: reason.clone(),
                    });
                }
                CallPlan::HookTerminal {
                    result,
                    reason,
                    hooks,
                } => {
                    writer.record(WalRecord::ToolResult {
                        finished_at_ms: None,
                        tags: None,
                        hooks: hooks.clone(),
                        batch_id: batch_id.clone(),
                        result: Arc::clone(result),
                        outcome: DispatchOutcome::Completed,
                        reason: Some(reason.clone()),
                        withdraw_elicitation: None,
                        provider_held: None,
                    });
                }
                CallPlan::Synthetic { result, reason } => {
                    writer.record(WalRecord::ToolResult {
                        finished_at_ms: None,
                        tags: None,
                        hooks: None,
                        batch_id: batch_id.clone(),
                        result: Arc::clone(result),
                        outcome: DispatchOutcome::Completed,
                        reason: reason.clone(),
                        withdraw_elicitation: None,
                        provider_held: None,
                    });
                }
                CallPlan::Stopped(tool_call_id) => {
                    writer.record(WalRecord::ToolResult {
                        finished_at_ms: None,
                        tags: None,
                        hooks: None,
                        batch_id: batch_id.clone(),
                        result: Arc::new(crate::resume::not_dispatched_result(tool_call_id)),
                        outcome: DispatchOutcome::Interrupted,
                        reason: None,
                        withdraw_elicitation: None,
                        provider_held: None,
                    });
                }
                CallPlan::Dispatch(_) | CallPlan::Already(_) => {}
            }
        }
        writer.record(WalRecord::ToolBatchFinished {
            batch_id: batch_id.clone(),
        });
        writer.record(WalRecord::TurnFinished { step_id });
        record_budget_firings(writer, run_id, budget_firings);
        writer.sync().await?;
        // A batch refused by a stop request was never announced to the tool plane, so there is
        // nothing for it to finish.
        if !batch_stopped {
            let _ = tool_plane.batch_finished(&ctx).await;
        }

        let mut resolutions: Vec<CallResolution> = Vec::with_capacity(plan.len());
        for p in plan {
            resolutions.push(resolve(p, &ctx, tool_plane, cancel, stop).await);
        }
        debug_assert!(
            !resolutions.iter().any(|r| r.ambiguous),
            "a plan with no dispatches must not produce ambiguous results"
        );
        // No call of this batch was sent, so no result is ambiguous. A scope drain seen at batch
        // start still stops the run: every later call in the run would meet the same closed
        // scope. A call the stop request kept back stops it too: the run was asked to start
        // nothing further.
        let interruption = if scope_stopped {
            Some(BatchInterruption::ScopeDrained)
        } else if let Some(cause) = gating_stop {
            Some(cause)
        } else if resolutions.iter().any(|r| r.stopped) {
            Some(unsent_stop_cause(cancel))
        } else {
            None
        };
        let results: Vec<ToolResult> = resolutions
            .into_iter()
            .map(|r| Arc::unwrap_or_clone(r.result))
            .collect();

        if emit_control_payloads {
            // Every result here already has a durable terminal representation (ToolNoDispatch,
            // ToolResult, or a terminal from the resumed batch). The WAL projector emits the
            // subscriber's ToolResult, so the module can avoid cloning the same payload into its
            // discarded live-control channel.
            for r in &results {
                let _ = send(
                    out,
                    OutputEvent::Control(ControlUnit::ToolResult { result: r.clone() }),
                )
                .await;
            }
        }
        return Ok(BatchOutcome {
            results,
            interruption,
            open: Vec::new(),
        });
    }

    register_dispatch_plans(tool_plane, &ctx, plan.iter()).await?;

    // Sequential if ANY dispatched call's tool forces it; else parallel.
    let any_sequential = plan.iter().any(|p| match p {
        CallPlan::Dispatch(c) => {
            matches!(
                manifest.get(&c.tool_name).map(|t| t.execution_mode),
                Some(ExecutionMode::Sequential)
            )
        }
        _ => false,
    });

    let recording_plane = ProviderHeldPlane {
        inner: tool_plane,
        writer,
        ctx: &ctx,
        sequential: any_sequential,
        held: ProviderHeldBatch::new(resumed_held),
        lifecycle: ordered.and_then(|control| control.provider_held_lifecycle.as_deref()),
        clock: ordered.and_then(|control| control.clock.as_ref()),
    };
    let tool_plane = &recording_plane;
    let (mut resolutions, provider_held_pause) = crate::keep_warm_wait::wait_tools(
        resolve_provider_held_batch(plan, step_id, &ctx, tool_plane, cancel, stop, hooks),
        warm,
    )
    .await?;

    // A call the stop request kept from being sent ends the run after this batch, like an
    // ambiguous one: the run was asked to start nothing further.
    let interruption = if resolutions.iter().any(|r| r.ambiguous) {
        Some(BatchInterruption::Ambiguous)
    } else if scope_stopped {
        Some(BatchInterruption::ScopeDrained)
    } else if resolutions.iter().any(|r| r.stopped) {
        Some(unsent_stop_cause(cancel))
    } else {
        None
    };

    // BARRIER 2: fsync every dispatched call's tool_result, plus batch_finished and
    // turn_finished, before emitting any result to the caller. That way the caller never
    // sees a result that a later resume would treat as lost. NoDispatch calls already have
    // a durable tool_no_dispatch record from barrier 1, so they do not need a tool_result.
    for r in &mut resolutions {
        if let Some(outcome) = r.outcome {
            let record = WalRecord::ToolResult {
                finished_at_ms: None,
                tags: None,
                hooks: r.hooks.take(),
                batch_id: batch_id.clone(),
                result: Arc::clone(&r.result),
                outcome,
                reason: r.reason.clone(),
                withdraw_elicitation: None,
                provider_held: None,
            };
            if let Some((seal, stage)) = hooks
                .and_then(|runtime| runtime.seal)
                .zip(r.hook_stage.as_ref())
            {
                record_hook_stage(seal, stage, writer, record).await;
            } else {
                writer.record(record);
            }
        }
    }
    if provider_held_pause {
        writer.sync().await?;
        return Ok(BatchOutcome {
            results: Vec::new(),
            interruption: Some(BatchInterruption::ProviderHeldPause),
            open: Vec::new(),
        });
    }
    writer.record(WalRecord::ToolBatchFinished {
        batch_id: batch_id.clone(),
    });
    let appends = deliver_ordered_with_results(
        writer,
        ordered,
        run_id,
        resolutions.iter().map(|r| r.result.as_ref()),
    );
    writer.record(WalRecord::TurnFinished { step_id });
    record_budget_firings(writer, run_id, budget_firings);
    writer.sync().await?;
    let _ = tool_plane.batch_finished(&ctx).await;

    // Reclaim the shared result only after the durability barrier. The writer normally drops its
    // shallow Arc at the barrier; if an observer retained the record, the owned copy is made here,
    // after the result is durable rather than while serialize and fsync are pending.
    let results: Vec<ToolResult> = resolutions
        .into_iter()
        .map(|r| Arc::unwrap_or_clone(r.result))
        .collect();

    // Emit results to the caller in source order — AFTER their fsync.
    if emit_control_payloads {
        // Direct loop consumers need the live result payloads. Module subscribers receive the
        // source-ordered results from the durable batch projection, so suppressing these clones
        // cannot remove them from the subscriber control stream.
        for r in &results {
            let _ = send(
                out,
                OutputEvent::Control(ControlUnit::ToolResult { result: r.clone() }),
            )
            .await;
        }
    }
    let mut results = results;
    apply_ordered_appends(&mut results, &appends);
    Ok(BatchOutcome {
        results,
        interruption,
        open: Vec::new(),
    })
}

/// Deliver, at the commit of a tool batch, each queued ordered send whose first possible mode is
/// `tool_result` (see `crate::delivery_order`). Its records go between the batch's
/// `ToolBatchFinished` and `TurnFinished`, in the same barrier, so a projection boundary never
/// separates a result from the text delivered with it. Returns the text to append to the live
/// copies of the results; the durable `ToolResult` records are never changed.
fn deliver_ordered_with_results<'a>(
    writer: &WalHandle,
    control: Option<&RunControl>,
    run_id: &RunId,
    results: impl IntoIterator<Item = &'a ToolResult>,
) -> Vec<(ToolCallId, String)> {
    let Some((inbox, control)) =
        control.and_then(|control| Some((control.steer.as_deref()?, control)))
    else {
        return Vec::new();
    };
    inbox.deliver_with_tool_results(run_id, control.now_ms().unwrap_or(0), results, |records| {
        for record in records {
            writer.record(record);
        }
    })
}

/// Append each delivered text to the live copy of the result it rides on, as replay renders it
/// from the `SendDelivered` record. The caller's events carry the tool's own output unchanged.
fn apply_ordered_appends(results: &mut [ToolResult], appends: &[(ToolCallId, String)]) {
    for (tool_call_id, text) in appends {
        if let Some(result) = results
            .iter_mut()
            .find(|result| &result.tool_call_id == tool_call_id)
        {
            crate::delivery_order::append_to_result(result, text);
        }
    }
}

/// UTF-8 bytes bound the character count, so this sits far below the smallest known
/// per-item provider limit (OpenAI Responses: 10,485,760 characters). A result this size
/// already overflows most context windows.
pub const TOOL_RESULT_MAX_BYTES: usize = 1_048_576;
/// Reserve space only for subscribed text subjects; structured JSON retains the full ceiling.
pub const POST_TOOL_HEADROOM: usize = 16_384;

/// Cap once before persistence so live execution and replay use the same stored bytes.
fn cap_tool_result(result: &mut ToolResult, run_id: &RunId, tool_name: &str, headroom: bool) {
    let limit = if headroom {
        TOOL_RESULT_MAX_BYTES - POST_TOOL_HEADROOM
    } else {
        TOOL_RESULT_MAX_BYTES
    };
    // ToolOutput has no image/content-block variant. JSON is measured as serialized model
    // text, and oversized structured results become text rather than invalid partial JSON.
    // Denied's provider-facing prefix is included; small outputs retain their original variant.
    let serialized;
    let text = match &result.output {
        // Cap text parts only; capture-time media validation owns size limits for stored objects.
        ToolOutput::Parts { .. } => {
            if let ToolOutput::Parts { parts } = &mut result.output {
                let mut remaining = limit;
                for part in parts.iter_mut() {
                    if let broca_protocol::ToolOutputPart::Text { text } = part {
                        let mut end = text.len().min(remaining);
                        while !text.is_char_boundary(end) {
                            end -= 1;
                        }
                        text.truncate(end);
                        remaining -= end;
                    }
                }
            }
            return;
        }
        ToolOutput::Text { text } => text.as_str(),
        ToolOutput::Error { message } => message.as_str(),
        ToolOutput::Denied { reason } => {
            serialized = format!("denied: {reason}");
            &serialized
        }
        ToolOutput::Json { value } => {
            serialized = value.to_string();
            &serialized
        }
    };
    let original_bytes = text.len();
    if headroom {
        if original_bytes <= limit {
            return;
        }
    } else if original_bytes <= TOOL_RESULT_MAX_BYTES {
        return;
    }
    // Tool names are unbounded; shorten only the diagnostic label so it cannot consume
    // the entire result budget. The warning below retains the full name, never result content.
    let shortened_name;
    let marker_name = if tool_name.len() > 256 {
        let mut end = 256;
        while !tool_name.is_char_boundary(end) {
            end -= 1;
        }
        shortened_name = format!("{}…", &tool_name[..end]);
        shortened_name.as_str()
    } else {
        tool_name
    };
    let marker = |kept| {
        if headroom {
            format!("\n[broca: tool result truncated: {marker_name} returned {original_bytes} bytes, over the {limit} byte limit; POST_TOOL_HEADROOM reserves {POST_TOOL_HEADROOM} bytes; the first {kept} bytes are shown. Narrow the request (a smaller range, a filter, or a more specific query).]")
        } else {
            // Keep the hookless marker byte-identical for existing sessions.
            format!("\n[broca: tool result truncated: {marker_name} returned {original_bytes} bytes, over the {TOOL_RESULT_MAX_BYTES} byte limit; the first {kept} bytes are shown. Narrow the request (a smaller range, a filter, or a more specific query).]")
        }
    };
    let mut kept_bytes = limit.saturating_sub(marker(0).len());
    // Account for the kept count's own decimal digits, then find the last complete character.
    while kept_bytes + marker(kept_bytes).len() > limit || !text.is_char_boundary(kept_bytes) {
        kept_bytes = kept_bytes.saturating_sub(1);
    }
    let capped = format!("{}{}", &text[..kept_bytes], marker(kept_bytes));
    result.output = ToolOutput::Text { text: capped };
    tracing::warn!(
        target: "tool_results",
        run_id = %run_id.0,
        tool_name,
        original_bytes,
        kept_bytes,
        "tool result truncated"
    );
}

/// A synthetic `interrupted` resolution for a call abandoned because an earlier
/// sequential call was ambiguous. It WAS planned dispatchable (its intent was
/// written), so it gets a `tool_result{Interrupted}` terminal.
fn interrupted_resolution(id: &ToolCallId) -> CallResolution {
    CallResolution::plain(interrupted_result(id), Some(DispatchOutcome::Interrupted))
}

/// The resolution of a call never sent because the module asked the run to stop first: the
/// not-dispatched result with outcome `Interrupted`, the terminal a resume writes for a planned
/// call that never left the process. Never `Unknown`, because the call definitely did not run.
fn stopped_resolution(id: &ToolCallId) -> CallResolution {
    let mut resolution = CallResolution::plain(
        crate::resume::not_dispatched_result(id),
        Some(DispatchOutcome::Interrupted),
    );
    resolution.stopped = true;
    resolution
}

/// Why a batch whose unsent calls were kept back stops the run: the caller's immediate
/// cancellation when it is set (the run ends `cancelled`), otherwise the module's stop request
/// (the run ends `interrupted`).
fn unsent_stop_cause(cancel: Option<&CancellationToken>) -> BatchInterruption {
    if cancel.is_some_and(CancellationToken::is_cancelled) {
        BatchInterruption::CallerCancel
    } else {
        BatchInterruption::StopRequested
    }
}

async fn register_dispatch_plans<'a>(
    plane: &impl ToolPlane,
    ctx: &BatchCtx,
    plans: impl Iterator<Item = &'a CallPlan>,
) -> Result<(), RunError> {
    let calls: Vec<_> = plans
        .filter_map(|plan| match plan {
            CallPlan::Dispatch(call) => {
                call.call_key
                    .as_ref()
                    .map(|key| crate::tool_plane::DispatchRegistration {
                        tool_call_id: call.tool_call_id.clone(),
                        call_key: key.clone(),
                        approved: call.approved.is_some(),
                    })
            }
            _ => None,
        })
        .collect();
    if !calls.is_empty() {
        plane
            .register_dispatch_group(ctx, &calls)
            .await
            .map_err(|error| RunError::Wal(format!("late-results registration: {error}")))?;
    }
    Ok(())
}

#[derive(Default)]
struct ProviderHeldBatch {
    open: std::sync::Mutex<std::collections::HashMap<ToolCallId, crate::replay::ProviderHeldCall>>,
    changed: tokio::sync::Notify,
    resumed: Vec<crate::replay::ProviderHeldCall>,
}

impl ProviderHeldBatch {
    fn new(resumed: Vec<crate::replay::ProviderHeldCall>) -> Self {
        Self {
            open: std::sync::Mutex::new(
                resumed
                    .iter()
                    .map(|held| (held.tool_call_id.clone(), held.clone()))
                    .collect(),
            ),
            resumed,
            changed: tokio::sync::Notify::new(),
        }
    }

    fn contains(&self, id: &ToolCallId) -> bool {
        self.open
            .lock()
            .expect("provider-held slots")
            .contains_key(id)
    }

    fn len(&self) -> usize {
        self.open.lock().expect("provider-held slots").len()
    }

    fn complete(&self, id: &ToolCallId) {
        self.open.lock().expect("provider-held slots").remove(id);
    }
}

/// Sync a delivered sequential held terminal before the next send, not at the batch barrier.
async fn sync_held_terminal(
    result: &mut CallResolution,
    ctx: &BatchCtx,
    writer: &WalHandle,
    hooks: Option<&crate::hooks::HookRuntime<'_>>,
) -> Result<(), RunError> {
    approval_gate::sync_result(result, ctx, writer, hooks).await?;
    result.outcome = None;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn resolve_provider_held_batch<T: ToolPlane>(
    plans: Vec<CallPlan>,
    step: StepId,
    ctx: &BatchCtx,
    plane: &ProviderHeldPlane<'_, T>,
    cancel: Option<&CancellationToken>,
    stop: Option<&CancellationToken>,
    hooks: Option<&crate::hooks::HookRuntime<'_>>,
) -> Result<(Vec<CallResolution>, bool), RunError> {
    use futures::stream::{FuturesUnordered, StreamExt};
    let count = plans.len();
    let mut remaining = plans.into_iter().enumerate();
    let mut jobs = FuturesUnordered::new();
    let mut results = std::collections::BTreeMap::new();
    let mut tripped = false;
    loop {
        while !plane.sequential || jobs.is_empty() {
            let Some((index, plan)) = remaining.next() else {
                break;
            };
            let stopped = tripped && matches!(plan, CallPlan::Dispatch(_));
            jobs.push(async move {
                let result = if stopped {
                    let CallPlan::Dispatch(call) = plan else {
                        unreachable!()
                    };
                    interrupted_resolution(&call.tool_call_id)
                } else {
                    resolve_hooked(plan, index, step, ctx, plane, cancel, stop, hooks).await
                };
                (index, result)
            });
        }
        if results.len() == count {
            return Ok((results.into_values().collect(), false));
        }
        tokio::select! {
            biased;
            Some((index, mut result)) = jobs.next() => {
                tripped |= result.ambiguous;
                if plane.held.contains(&result.result.tool_call_id) {
                    // Even on resume, no successor may leave until the held terminal is durable.
                    sync_held_terminal(&mut result, ctx, plane.writer, hooks).await?;
                    plane.held.complete(&result.result.tool_call_id);
                }
                results.insert(index, result);
            }
            _ = async {}, if !tripped && plane.held.len() > 0 && jobs.len() == plane.held.len() => {
                return Ok((results.into_values().collect(), true));
            }
            _ = plane.held.changed.notified() => {}
        }
    }
}

/// Keeps the batch's WAL writer on the held path, before the adapter can join a waiting slot.
struct ProviderHeldPlane<'a, T> {
    inner: &'a T,
    writer: &'a WalHandle,
    ctx: &'a BatchCtx,
    sequential: bool,
    held: ProviderHeldBatch,
    lifecycle: Option<&'a dyn crate::tool_plane::ProviderHeldLifecycle>,
    clock: Option<&'a crate::Clock>,
}

#[async_trait::async_trait]
impl<T: ToolPlane> crate::tool_plane::ProviderHeldRecorder for ProviderHeldPlane<'_, T> {
    async fn sync_held(&self, reply: &crate::tool_plane::ProviderHeldReply) -> Result<(), String> {
        let held_at_ms = self
            .clock
            .map_or_else(|| epoch_millis().unwrap_or(0), crate::Clock::now_ms);
        self.writer
            .record(WalRecord::ToolCallProviderHeld {
                batch_id: self.ctx.batch_id.clone(),
                tool_call_id: reply.tool_call_id.clone(),
                call_key: reply.call_key.clone(),
                module: reply.module.clone(),
                reason: reply.reason.clone(),
                detail_digest: reply.detail_digest.clone(),
                held_at_ms,
                sequential: self.sequential,
            })
            .into_result()
            .map_err(|error| error.to_string())?;
        self.writer
            .sync()
            .await
            .map_err(|error| error.to_string())?;
        self.held.open.lock().expect("provider-held slots").insert(
            reply.tool_call_id.clone(),
            crate::replay::ProviderHeldCall {
                tool_call_id: reply.tool_call_id.clone(),
                call_key: reply.call_key.clone(),
                module: reply.module.clone(),
                reason: reply.reason.clone(),
                detail_digest: reply.detail_digest.clone(),
                held_at_ms,
                sequential: self.sequential,
            },
        );
        self.held.changed.notify_one();
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: ToolPlane> ToolPlane for ProviderHeldPlane<'_, T> {
    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        self.inner.batch_started(ctx, calls, manifest).await
    }
    async fn batch_finished(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        self.inner.batch_finished(ctx).await
    }
    fn dispatch_target(&self, ctx: &BatchCtx, id: &ToolCallId) -> Option<String> {
        self.inner.dispatch_target(ctx, id)
    }
    async fn register_dispatch_group(
        &self,
        ctx: &BatchCtx,
        calls: &[crate::tool_plane::DispatchRegistration],
    ) -> Result<(), ToolPlaneError> {
        self.inner.register_dispatch_group(ctx, calls).await
    }
    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.inner.dispatch(ctx, call).await
    }
    async fn dispatch_keyed(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        key: crate::tool_plane::DispatchKey<'_>,
    ) -> DispatchResult {
        if let Some(held) = self
            .held
            .resumed
            .iter()
            .find(|held| held.tool_call_id == call.tool_call_id)
        {
            return match self.lifecycle {
                Some(lifecycle) => lifecycle.wait(ctx, call, &held.reply()).await,
                None => std::future::pending().await,
            };
        }
        self.inner
            .dispatch_keyed(
                ctx,
                call,
                crate::tool_plane::DispatchKey {
                    call_key: key.call_key,
                    held_recorder: Some(self),
                },
            )
            .await
    }
    async fn dispatch_approved(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        args: &str,
        key: &str,
        signals: &crate::tool_plane::DispatchSignals,
    ) -> DispatchResult {
        self.inner
            .dispatch_approved(ctx, call, args, key, signals)
            .await
    }
    async fn dispatch_approved_keyed(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        args: &str,
        key: crate::tool_plane::DispatchKey<'_>,
        signals: &crate::tool_plane::DispatchSignals,
    ) -> DispatchResult {
        if self
            .held
            .resumed
            .iter()
            .any(|held| held.tool_call_id == call.tool_call_id)
        {
            return self.dispatch_keyed(ctx, call, key).await;
        }
        self.inner
            .dispatch_approved_keyed(
                ctx,
                call,
                args,
                crate::tool_plane::DispatchKey {
                    call_key: key.call_key,
                    held_recorder: Some(self),
                },
                signals,
            )
            .await
    }
    async fn recheck_approval_scope(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        module: Option<&str>,
    ) -> Result<(), ToolPlaneError> {
        self.inner.recheck_approval_scope(ctx, call, module).await
    }
}

/// Execute one planned call, classifying the dispatch outcome:
///  - `Completed`  → the definite result, recorded as `Completed`.
///  - `NotSent`    → the request never left, so it's a safe graceful `is_error`;
///    recorded as `Completed` (a definite terminal — resume includes it and never
///    re-dispatches it).
///  - `SentButOutcomeUnknown` → ambiguous: a provider-valid synthetic "unknown"
///    result + `ambiguous = true`, which stops the run; recorded as `Unknown`.
///  - a `NoDispatch` plan resolves instantly to its synth result; its terminal is
///    the `tool_no_dispatch` record from barrier 1, so it gets no tool_result.
///  - a `HookTerminal` (a `pre_tool` deny or unavailable-refuse) is likewise already
///    durable from barrier 1, so it also resolves with no outcome and no second record.
async fn resolve<T: ToolPlane>(
    plan: CallPlan,
    ctx: &BatchCtx,
    tool_plane: &T,
    cancel: Option<&CancellationToken>,
    stop: Option<&CancellationToken>,
) -> CallResolution {
    match plan {
        CallPlan::Stopped(id) => stopped_resolution(&id),
        // A stop requested after this call's intent was written, but before the call was sent:
        // it is not sent. Calls of the batch already sent are left to finish.
        CallPlan::Dispatch(call) if stop_is_requested(stop) => {
            stopped_resolution(&call.tool_call_id)
        }
        // The caller cancelled immediately before this call was sent: it is not sent either.
        CallPlan::Dispatch(call) if cancel.is_some_and(CancellationToken::is_cancelled) => {
            stopped_resolution(&call.tool_call_id)
        }
        // RESUME: the call's terminal is already durable — include the
        // reconstructed result, write NO new record (outcome=None).
        CallPlan::Already(result) => CallResolution::plain(result, None),
        CallPlan::HookTerminal { result, .. } => {
            CallResolution::plain(Arc::unwrap_or_clone(result), None)
        }
        CallPlan::Synthetic { result, reason } => {
            let mut resolution = CallResolution::plain(
                Arc::unwrap_or_clone(result),
                Some(DispatchOutcome::Completed),
            );
            resolution.reason = reason;
            resolution
        }
        // The SHARED no_dispatch_result is the single source of truth for both the
        // live result and the one reconstructed on resume, so they can't drift.
        CallPlan::NoDispatch {
            tool_call_id,
            reason,
            ..
        } => CallResolution::plain(
            crate::replay::no_dispatch_result(&tool_call_id, &reason),
            None,
        ),
        CallPlan::Dispatch(call) => {
            // The writer normally releases its shallow Arc before acknowledging the intent
            // barrier. If a live WAL observer retained the record, clone only after durability.
            let approved = call.approved.clone();
            let is_approved = approved.is_some();
            let key = call.call_key.clone();
            let call = call.into_tool_call();
            let result = match approved {
                Some(bytes) => {
                    tool_plane
                        .dispatch_approved_keyed(
                            ctx,
                            &call,
                            &bytes,
                            crate::tool_plane::DispatchKey {
                                call_key: key.as_deref().expect("approved call has Prepared key"),
                                held_recorder: None,
                            },
                            &crate::tool_plane::DispatchSignals::new(cancel, stop),
                        )
                        .await
                }
                None => match key.as_deref() {
                    Some(call_key) => {
                        tool_plane
                            .dispatch_keyed(
                                ctx,
                                &call,
                                crate::tool_plane::DispatchKey {
                                    call_key,
                                    held_recorder: None,
                                },
                            )
                            .await
                    }
                    None => tool_plane.dispatch(ctx, &call).await,
                },
            };
            match result {
                DispatchResult::Completed(result) => {
                    let malformed_held = matches!(&result.output, ToolOutput::Error { message } if message.starts_with("malformed provider-held reply:"));
                    let mut resolution =
                        CallResolution::plain(result, Some(DispatchOutcome::Completed));
                    if malformed_held {
                        resolution.reason = Some("malformed_provider_held_reply".into());
                    }
                    resolution.plane_subject = true;
                    resolution
                }
                DispatchResult::NotSent { reason } => {
                    let mut result = CallResolution::plain(
                        error_result(&call, &format!("tool not dispatched: {reason}")),
                        Some(DispatchOutcome::Completed),
                    );
                    if is_approved {
                        result.reason = Some(reason);
                    }
                    result
                }
                DispatchResult::SentButOutcomeUnknown { reason } => {
                    let mut resolution = CallResolution::plain(
                        unknown_result(&call, &reason),
                        Some(DispatchOutcome::Unknown),
                    );
                    resolution.ambiguous = true;
                    resolution
                }
                // The request was never sent, so the refusal is definite. The model sees it, the
                // `tool_result` record's `completed` outcome keeps a resume from dispatching it
                // again, and the run goes on.
                DispatchResult::ScopeDrained {
                    reason,
                    sent: false,
                } => {
                    let mut resolution = CallResolution::plain(
                        route_drained_result(&call.tool_call_id, &reason),
                        Some(DispatchOutcome::Completed),
                    );
                    resolution.reason = Some(reason);
                    resolution
                }
                // Sent with no result: the side effect may have applied, so this is the same
                // ambiguous outcome as any lost reply and stops the run Interrupted.
                DispatchResult::ScopeDrained { reason, sent: true } => {
                    let mut resolution = CallResolution::plain(
                        unknown_result(
                            &call,
                            &format!("its route was drained ({reason}) after the call was sent"),
                        ),
                        Some(DispatchOutcome::Unknown),
                    );
                    resolution.ambiguous = true;
                    resolution.reason = Some(reason);
                    resolution
                }
            }
        }
    }
}

/// The `route_drained` result for a call a scope drain stopped before it was sent. The text
/// leads with the name so the model, and anyone reading the transcript, sees why.
fn route_drained_result(id: &ToolCallId, reason: &str) -> ToolResult {
    ToolResult {
        tool_call_id: id.clone(),
        output: ToolOutput::Error {
            message: format!(
                "route_drained: the tool was not run because its route was closed ({reason})"
            ),
        },
        is_error: true,
    }
}

fn error_result(call: &ToolCall, message: &str) -> ToolResult {
    ToolResult {
        tool_call_id: call.tool_call_id.clone(),
        output: ToolOutput::Error {
            message: message.to_owned(),
        },
        is_error: true,
    }
}

/// A provider-valid synthetic result for an ambiguous dispatch (sent but no result
/// came back). The conversation stays valid; the run stops for review rather than
/// feeding the unknown outcome back to the model.
fn unknown_result(call: &ToolCall, reason: &str) -> ToolResult {
    ToolResult {
        tool_call_id: call.tool_call_id.clone(),
        output: ToolOutput::Error {
            message: format!("tool outcome unknown (interrupted for review): {reason}"),
        },
        is_error: true,
    }
}

/// A synthetic `interrupted` result for a call abandoned because an earlier
/// sequential call was ambiguous — keeps the result set complete.
fn interrupted_result(id: &ToolCallId) -> ToolResult {
    ToolResult {
        tool_call_id: id.clone(),
        output: ToolOutput::Error {
            message: "tool not run: batch interrupted".into(),
        },
        is_error: true,
    }
}

/// The advance notice a compaction session records when its run ends, announcing the writes
/// after its last step boundary (the final answer, and the results of a hard stop) before the
/// terminal (compaction-provider CONTRACT.md §5: a notice after each durable write). `None`
/// for a session without compaction, or a WAL that has no lineage id yet.
struct ClosingNotice<'a> {
    state: &'a mut crate::compaction::CompactionState,
    raw: crate::compaction::StoredTranscript<'a>,
    extra: &'a [Message],
    lineage_id: String,
    session: &'a broca_protocol::BindIdentity,
    model: &'a broca_protocol::ModelHandle,
    window: Option<u64>,
    history: &'a crate::compaction::CompactionHistory,
    request_bytes: usize,
    floor: u64,
}

impl<'a> ClosingNotice<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        state: Option<&'a mut crate::compaction::CompactionState>,
        raw: crate::compaction::StoredTranscript<'a>,
        extra: &'a [Message],
        writer: &WalHandle,
        session: &'a broca_protocol::BindIdentity,
        model: &'a broca_protocol::ModelHandle,
        window: Option<u64>,
        history: &'a crate::compaction::CompactionHistory,
        request_bytes: usize,
        floor: u64,
    ) -> Option<Self> {
        Some(Self {
            state: state?,
            raw,
            extra,
            lineage_id: writer.lineage_id()?.to_string(),
            session,
            model,
            window,
            history,
            request_bytes,
            floor,
        })
    }

    /// Build the notice from untagged transcript messages not yet announced, plus the latest
    /// assistant message when it is not in the prompt and any tool results written after it.
    /// Return `None` if nothing new is available or the notice would be malformed.
    fn notice(
        self,
        assistant: &[ContentBlock],
        results: &[ToolResult],
    ) -> Option<(
        &'a mut crate::compaction::CompactionState,
        crate::compaction_provider::notice::AdvanceNotice,
    )> {
        let mut extra = self.extra.to_vec();
        if !assistant.is_empty() {
            extra.push(Message::assistant_from(
                assistant.to_vec(),
                self.model.clone(),
            ));
        }
        if !results.is_empty() {
            extra.push(Message::new(
                Role::Tool,
                results
                    .iter()
                    .cloned()
                    .map(ContentBlock::ToolResult)
                    .collect(),
            ));
        }
        for (index, message) in extra.iter_mut().enumerate() {
            message.ordinal = Some((self.raw.len() + index) as u64);
        }
        let announced = self
            .state
            .announced
            .as_ref()
            .and_then(|announced| announced.through_ordinal);
        let appended: usize = self
            .raw
            .messages_after(announced)
            .chain(extra.iter())
            .filter(|m| m.ordinal.is_some_and(|o| announced.is_none_or(|a| o > a)))
            .map(|m| serde_json::to_vec(m).map_or(0, |bytes| bytes.len()))
            .sum();
        let (estimate, _) = crate::compaction::request_estimate(
            self.request_bytes.saturating_add(appended),
            self.history,
        );
        let through = extra.last().or_else(|| self.raw.newest())?;
        let notice = crate::compaction::view::notice_through(
            self.state,
            through,
            crate::compaction::view::NoticeFacts {
                session_ref: crate::wal_writer::agent_session_ref(self.session),
                lineage_id: &self.lineage_id,
                est_tokens: estimate,
                model: &self.model.model_id,
                window: self.window,
                usage: self.history.usage(),
                events: Vec::new(),
                seq_floor: self.floor,
            },
        )?;
        notice.check().ok()?;
        Some((self.state, notice))
    }

    /// Record the notice for a hard stop's step in the hard stop's own barrier: queued here
    /// after its synthetic results, made durable with them and the terminal by the one sync
    /// the hard stop owes, and only then visible to the publisher.
    fn record_with_terminal(
        self,
        writer: &WalHandle,
        assistant: &[ContentBlock],
        results: &[ToolResult],
    ) {
        if let Some((state, notice)) = self.notice(assistant, results) {
            let record = WalRecord::CompactionNotice {
                seq: notice.seq,
                headers: notice.to_headers(),
            };
            writer.record(record.clone());
            state.fold(&record);
        }
    }
}

/// Finalize a hard stop (a stop/max-steps condition fired while the model's last step
/// still had PENDING, never-dispatched tool calls). Persists a provider-valid terminal
/// for each pending call (so the durable turn isn't a dangling tool_use the model would
/// reject on resume) AND the `RunFinished` terminal — all in ONE fsync barrier — then
/// emits to the caller and returns the run summary.
///
/// The single barrier is load-bearing for crash safety: the repair batch and the
/// terminal MUST be durable together. If they were two barriers, a crash in between would
/// leave the tool turn committed with no `RunFinished`, and replay would classify that as
/// a resumable open turn and re-run the model past the stop limit. With one barrier, a
/// crash either loses the whole repair (resume re-runs the idempotent model step) or has
/// the terminal too (resume finds the lineage idle) — never the broken in-between state.
///
/// The synthetic results are tagged MaxSteps/Interrupted — a definite "never ran" outcome
/// (never the ambiguous Unknown) — so resume includes them but never re-dispatches.
#[allow(clippy::too_many_arguments)]
async fn finalize_hard_stop(
    control: &RunControl,
    resolved_run_id: &RunId,
    step_id: StepId,
    pending: &[ToolCall],
    rejected: &[ToolResult],
    batch_already_started: bool,
    outcome: DispatchOutcome,
    reason: RunFinishReason,
    writer: &WalHandle,
    projector: Option<&ProjectionHandle>,
    out: &mpsc::Sender<OutputEvent>,
    steps: u64,
    usage: Usage,
    retries_used: u32,
    final_step_finish_reason: Option<FinishReason>,
    budget_firings: &[BudgetFiring],
    emit_control_payloads: bool,
    submitted: Option<&ToolCallId>,
    closing: Option<(ClosingNotice<'_>, &[ContentBlock])>,
) -> Result<RunSummary, RunError> {
    let batch_id = BatchId(format!("{}-b{}", resolved_run_id.0, step_id.0));
    let call_ids: Vec<ToolCallId> = pending.iter().map(|c| c.tool_call_id.clone()).collect();
    if !batch_already_started {
        writer.record(WalRecord::ToolBatchStarted {
            batch_id: batch_id.clone(),
            step_id,
            planned_calls: call_ids,
        });
    }
    let results: Vec<ToolResult> = pending
        .iter()
        .map(|c| {
            rejected
                .iter()
                .find(|result| result.tool_call_id == c.tool_call_id)
                .cloned()
                .unwrap_or_else(|| {
                    if submitted == Some(&c.tool_call_id) {
                        ToolResult {
                            tool_call_id: c.tool_call_id.clone(),
                            output: ToolOutput::Text {
                                text: broca_protocol::structured_output::SUBMIT_RESULT_RECORDED
                                    .into(),
                            },
                            is_error: false,
                        }
                    } else if submitted.is_some() {
                        error_result(
                            c,
                            broca_protocol::structured_output::SKIPPED_AFTER_SUBMIT_MESSAGE,
                        )
                    } else if reason == RunFinishReason::BudgetExhausted {
                        error_result(
                            c,
                            "budget_exhausted: tool not executed; input-token cap stopped dispatch",
                        )
                    } else {
                        hard_stop_result(c, outcome)
                    }
                })
        })
        .collect();
    // No ToolDispatchIntent — these calls were never dispatched.
    for r in &results {
        if batch_already_started
            && rejected
                .iter()
                .any(|result| result.tool_call_id == r.tool_call_id)
        {
            continue;
        }
        let is_rejected = rejected
            .iter()
            .any(|result| result.tool_call_id == r.tool_call_id);
        writer.record(WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            hooks: None,
            batch_id: batch_id.clone(),
            result: r.clone().into(),
            // The counted submit is the runner's own completed call; nothing else here ran.
            outcome: if is_rejected || submitted == Some(&r.tool_call_id) {
                DispatchOutcome::Completed
            } else {
                outcome
            },
            reason: if submitted == Some(&r.tool_call_id) {
                None
            } else if submitted.is_some() && !is_rejected {
                Some(broca_protocol::structured_output::SKIPPED_AFTER_SUBMIT.into())
            } else if reason == RunFinishReason::BudgetExhausted {
                Some("budget_exhausted".into())
            } else {
                rejected
                    .iter()
                    .any(|result| result.tool_call_id == r.tool_call_id)
                    .then(|| "tool_args_trailing_garbage".into())
            },
            withdraw_elicitation: None,
            provider_held: None,
        });
    }
    writer.record(WalRecord::ToolBatchFinished { batch_id });
    writer.record(WalRecord::TurnFinished { step_id });
    record_budget_firings(writer, resolved_run_id, budget_firings);
    if let Some((closing, assistant)) = closing {
        closing.record_with_terminal(writer, assistant, &results);
    }
    // The terminal goes in the SAME barrier as the repair batch (no intermediate sync).
    control.close_steers();
    writer.record(WalRecord::RunFinished {
        interrupt_reason: sealed_interrupt_reason(control, reason, None),
        ts_ms: control.now_ms(),
        reason,
        usage,
        error: None,
        retries_used,
    });
    // This sole barrier makes the synthetic tool results, ToolBatchFinished, TurnFinished,
    // and RunFinished durable together; TurnFinished must not introduce another crash cut.
    let safe_h = writer.sync().await?;
    // Emit AFTER fsync so the caller's surface matches the durable WAL.
    for firing in budget_firings {
        let _ = send(
            out,
            OutputEvent::Control(ControlUnit::BudgetFired {
                run_id: resolved_run_id.clone(),
                kind: firing.kind,
                threshold: firing.threshold,
                ordinal: firing.ordinal,
            }),
        )
        .await;
    }
    if emit_control_payloads {
        // Hard-stop results are projected from the repair records written above. Module
        // subscribers therefore retain the full ToolResult stream without paying for a second
        // set of payload clones on the actor's discarded live-control path.
        for r in &results {
            let _ = send(
                out,
                OutputEvent::Control(ControlUnit::ToolResult { result: r.clone() }),
            )
            .await;
        }
    }
    let _ = send(
        out,
        OutputEvent::Control(ControlUnit::RunFinished {
            run_id: Some(resolved_run_id.clone()),
            reason,
            usage: (steps > 0).then_some(usage),
            final_step_finish_reason,
            indeterminate_tool_calls: None,
            retries_used,
        }),
    )
    .await;
    if let Some(projector) = projector {
        projector.enqueue_terminal(safe_h);
    }
    Ok(RunSummary {
        reason,
        paused: None,
        steps,
        usage,
        retries_used,
        final_step_finish_reason,
    })
}

/// The text parts of `message` joined exactly as `run.result` joins them: in order, with
/// nothing inserted.
fn joined_text(message: &AssistantMessage) -> String {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(cortexkit_role_llm_runner::run::TEXT_SEPARATOR)
}

/// The `submit_result` call that ends a `submit_tool` run at this step: the first one the
/// model emitted whose arguments parsed. `None` for a `final_text` run or a step without one.
fn counted_submit(
    frozen: &FrozenStructuredOutput,
    message: &AssistantMessage,
    rejected: &[ToolResult],
) -> Option<ToolCall> {
    if frozen.mode != broca_protocol::structured_output::Mode::SubmitTool {
        return None;
    }
    message
        .tool_calls()
        .find(|call| {
            call.tool_name == broca_protocol::structured_output::SUBMIT_RESULT_TOOL
                && !rejected
                    .iter()
                    .any(|result| result.tool_call_id == call.tool_call_id)
        })
        .cloned()
}

/// The structured result of a run that ended by calling `submit_result`: the counted call's
/// input validated against the frozen schema, and every other call of the step, in the order
/// the model emitted them, listed as a skipped sibling.
fn submitted_result_record(
    run_id: &RunId,
    frozen: &FrozenStructuredOutput,
    submit: &ToolCall,
    step_calls: &[ToolCall],
) -> WalRecord {
    let skipped = step_calls
        .iter()
        .filter(|call| call.tool_call_id != submit.tool_call_id)
        .map(|call| {
            broca_protocol::structured_output::SkippedSibling::new(
                call.tool_call_id.0.clone(),
                call.tool_name.clone(),
            )
        })
        .collect();
    WalRecord::StructuredOutputResult {
        run_id: run_id.clone(),
        result: broca_protocol::structured_output::result(
            frozen,
            true,
            Some(&submit.input),
            None,
            skipped,
        ),
    }
}

/// The synthetic hard-stop terminal for one pending call.
fn hard_stop_result(call: &ToolCall, outcome: DispatchOutcome) -> ToolResult {
    let message = match outcome {
        DispatchOutcome::MaxSteps => "tool not run: max steps reached before dispatch".to_string(),
        _ => "tool not run: interrupted before dispatch".to_string(),
    };
    ToolResult {
        tool_call_id: call.tool_call_id.clone(),
        output: ToolOutput::Error { message },
        is_error: true,
    }
}

fn message_finish_reason(m: &AssistantMessage) -> FinishReason {
    if m.tool_calls().next().is_some() {
        FinishReason::ToolCalls
    } else {
        FinishReason::Stop
    }
}

/// The first satisfied stop condition, or None.
pub(crate) fn stop_satisfied<'a>(
    conds: &'a [StopCondition],
    completed_steps: u64,
    last: &AssistantMessage,
) -> Option<&'a StopCondition> {
    conds.iter().find(|c| match c {
        StopCondition::StepCount { n } => completed_steps >= *n,
        StopCondition::HasToolCall { name } => last
            .tool_calls()
            .any(|tc| name.as_ref().is_none_or(|n| &tc.tool_name == n)),
        StopCondition::LoopFinished => last.tool_calls().next().is_none(),
    })
}

/// Whether any satisfied stop other than LoopFinished forbids adding a model step
/// to process a queued steer. Check every condition, even after a matching LoopFinished.
pub(crate) fn limiting_stop_satisfied(
    conds: &[StopCondition],
    completed_steps: u64,
    last: &AssistantMessage,
) -> bool {
    conds.iter().any(|condition| {
        !matches!(condition, StopCondition::LoopFinished)
            && stop_satisfied(std::slice::from_ref(condition), completed_steps, last).is_some()
    })
}

async fn send(out: &mpsc::Sender<OutputEvent>, ev: OutputEvent) -> bool {
    out.send(ev).await.is_ok()
}

#[cfg(test)]
#[path = "run/boundary_cancel_tests.rs"]
mod boundary_cancel_tests;
#[cfg(test)]
#[path = "run/delivery_order_tests.rs"]
mod delivery_order_tests;
#[path = "run/hook_dispatch.rs"]
mod hook_dispatch;
use hook_dispatch::{
    pre_tool_subject, pre_tool_terminal, record_hook_stage, record_model_hook, resolve_hooked,
    restream_message,
};

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
        time::Duration,
    };

    use tokio::sync::Notify;

    use super::*;

    fn transformation_attempt(
        report: Option<Vec<broca_protocol::InputTransformation>>,
        unmeasured: bool,
    ) -> broca_protocol::ModelAttempt {
        broca_protocol::ModelAttempt {
            content_progress: Default::default(),
            attempt: 0,
            request_body_len: 1,
            request_body_sha256: "aa".into(),
            http_status: Some(200),
            content_type: None,
            response_body_len: 0,
            response_body_sha256: "bb".into(),
            response_body_cap: None,
            outcome: broca_protocol::provider::AttemptOutcome::Complete,
            ts_ms: 0,
            sent_at_ms: None,
            transport: None,
            websocket_request: None,
            websocket_close_code: None,
            input_transformations: report,
            input_transformations_unmeasured: unmeasured,
            credential_refresh: None,
            served_account_ref: None,
            account_observation: Default::default(),
        }
    }

    /// An attempt whose request did not ask for the report (a Claude Code persona request) is
    /// "not measured", never "no transformations", even if a report field were present.
    #[test]
    fn an_unmeasured_attempt_never_reads_as_no_transformations() {
        assert_eq!(
            TransformationReading::of(&transformation_attempt(None, true)),
            TransformationReading::NotMeasured
        );
        assert_eq!(
            TransformationReading::of(&transformation_attempt(Some(Vec::new()), true)),
            TransformationReading::NotMeasured
        );
        assert_eq!(
            TransformationReading::of(&transformation_attempt(Some(Vec::new()), false)),
            TransformationReading::Reported(0)
        );
        assert_eq!(
            TransformationReading::of(&transformation_attempt(None, false)),
            TransformationReading::Absent
        );
    }

    #[tokio::test]
    async fn run_future_frame_sizes() {
        let session = BindIdentity::new("/future-size", "test", "sizes");
        let mut req = RunRequest::from_submission(
            serde_json::from_value(serde_json::json!({
                "model": {"provider_module_id": "stub", "model_id": "m"},
                "input": [{"role": "user", "content": [{"type": "text", "text": "hello"}]}]
            }))
            .unwrap(),
            RunId("r".into()),
            session,
            broca_protocol::SubmissionId("s".into()),
        );
        let provider = crate::StubProvider::new(vec![]);
        let plane = crate::StubToolPlane::new();
        let control = RunControl::default();
        let writer = WalHandle::spawn(
            Box::new(RecordingWal {
                records: Arc::new(std::sync::Mutex::new(vec![])),
            }),
            1,
        )
        .unwrap();
        let wal_store = ReadableWalStore;
        let lease_store = crate::FileLeaseStore::new("/future-size/leases");
        let (out, _events) = mpsc::channel(1);
        let body = run_body_with_approval_history(
            &mut req,
            crate::replay(&[]).unwrap(),
            None,
            false,
            false,
            false,
            &control,
            &provider,
            &plane,
            &crate::AllowAll,
            &writer,
            || Ok(false),
            None,
            &out,
            None,
            None,
            None,
        );
        println!(
            "run_body_with_approval_history={} bytes",
            std::mem::size_of_val(&body)
        );
        drop(body);
        let leased = run_under_lease(
            req.clone(),
            1,
            None,
            &provider,
            &plane,
            &crate::AllowAll,
            &wal_store,
            None,
            false,
            false,
            RunControl::default(),
            &out,
        );
        println!("run_under_lease={} bytes", std::mem::size_of_val(&leased));
        let public = run_loop_with_control(
            req,
            &provider,
            &plane,
            &lease_store,
            &crate::AllowAll,
            &wal_store,
            None,
            RunControl::default(),
            &out,
        );
        println!(
            "run_loop_with_control={} bytes",
            std::mem::size_of_val(&public)
        );
        drop(public);
        drop(leased);
        writer
            .shutdown()
            .await
            .expect("measurement writer shutdown");
    }

    /// `stamp_credential` sets `credential_id` only on credential-scoped terminals (a billing
    /// refusal, an exhausted account window, a rate limit) and on no other error. It rewrites
    /// only rate limits to the `rate_limited` code, and sets `account_ref` only on the billing
    /// and account-limit refusals.
    #[test]
    fn stamp_credential_names_the_credential_only_on_credential_scoped_errors() {
        let error = |class, status: Option<u16>, code: Option<&str>| ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class,
            message: "m".into(),
            status,
            retry_after_secs: None,
            provider_code: code.map(str::to_owned),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        use broca_protocol::error::{ACCOUNT_LIMIT_EXHAUSTED, BILLING_EXHAUSTED, RATE_LIMITED};
        use ErrorClass::*;
        // (case, error, expected provider_code after stamping, expected account_ref stamped)
        let scoped = [
            (
                "billing",
                error(Permanent, Some(403), Some(BILLING_EXHAUSTED)),
                Some(BILLING_EXHAUSTED),
                true,
            ),
            (
                "billing on 400",
                error(Permanent, Some(400), Some(BILLING_EXHAUSTED)),
                Some(BILLING_EXHAUSTED),
                true,
            ),
            (
                "account limit",
                error(Transient, Some(429), Some(ACCOUNT_LIMIT_EXHAUSTED)),
                Some(ACCOUNT_LIMIT_EXHAUSTED),
                true,
            ),
            (
                "anthropic 429",
                error(Transient, Some(429), Some("rate_limit_error")),
                Some(RATE_LIMITED),
                false,
            ),
            (
                "openai 429",
                error(Transient, Some(429), Some("rate_limit_exceeded")),
                Some(RATE_LIMITED),
                false,
            ),
            (
                "google 429",
                error(Transient, Some(429), Some("RESOURCE_EXHAUSTED")),
                Some(RATE_LIMITED),
                false,
            ),
            (
                "bare 429",
                error(Transient, Some(429), None),
                Some(RATE_LIMITED),
                false,
            ),
            (
                "in-band rate limit",
                error(Transient, None, Some("rate_limit_error")),
                Some(RATE_LIMITED),
                false,
            ),
        ];
        for (case, raw, code, names_account) in scoped {
            let stamped =
                stamp_credential(raw.clone(), Some("acct-1"), Some("apikey:anthropic:work"));
            assert_eq!(
                stamped.credential_id.as_deref(),
                Some("apikey:anthropic:work"),
                "{case}"
            );
            assert_eq!(stamped.provider_code.as_deref(), code, "{case}");
            assert_eq!(
                stamped.account_ref.as_deref(),
                names_account.then_some("acct-1"),
                "{case}"
            );
            // A run that froze no credential selection names none.
            let bare = stamp_credential(raw, None, None);
            assert_eq!(bare.credential_id, None, "{case} without a selection");
        }
        let unscoped = [
            (
                "context overflow",
                error(ContextOverflow, Some(400), Some("context_length_exceeded")),
            ),
            (
                "bad request",
                error(Permanent, Some(400), Some("invalid_request_error")),
            ),
            (
                "policy refusal",
                error(Permanent, None, Some("cyber_policy")),
            ),
            (
                "content filter",
                error(Permanent, None, Some("content_filter")),
            ),
            (
                "tier refusal on 429",
                error(Permanent, Some(429), Some("service_tier_unavailable")),
            ),
            (
                "response deadline",
                error(Transient, None, Some("response_deadline")),
            ),
            (
                "server error",
                error(Transient, Some(500), Some("api_error")),
            ),
            (
                "overloaded",
                error(Transient, Some(529), Some("overloaded_error")),
            ),
            ("timeout", error(Transient, Some(408), None)),
            ("stream cut", error(ProviderUnavailable, None, None)),
            ("auth", error(Auth, Some(401), Some("authentication_error"))),
            (
                "crash orphan",
                error(Transient, None, Some("previous_process_ended_unsealed")),
            ),
        ];
        for (case, raw) in unscoped {
            let stamped =
                stamp_credential(raw.clone(), Some("acct-1"), Some("apikey:anthropic:work"));
            assert_eq!(stamped, raw, "{case} is returned unchanged");
        }
    }

    #[test]
    fn unchanged_effort_checkpoint_keeps_opted_in_store_prior_on_fast_path() {
        let frozen = serde_json::json!({
            "family_id": "openai_responses_chatgpt", "wire_model_id": "gpt-6.1-sol",
            "chatgpt_effort_updates": true, "reasoning": { "effort": "xhigh" }
        });
        let mut prior = crate::replay::replay(&[]).unwrap();
        prior.render_episodes = None;
        prior.run_config = Some(RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: broca_protocol::request::ModelHandle::new("chatgpt", "gpt-6.1-sol"),
            tools: vec![].into(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: frozen.clone(),
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![],
            context_management: None,
        });
        assert!(
            store_prior_needs_episode_spans(&prior),
            "unknown history cannot use the store seed"
        );
        let mut checkpoint = crate::AdmissionCheckpointV1::empty();
        checkpoint.effort_history.as_mut().unwrap().first_config = frozen;
        checkpoint.effort_history.as_mut().unwrap().has_first = true;
        checkpoint.restore_unchanged_effort(&mut prior);
        assert!(
            !store_prior_needs_episode_spans(&prior),
            "unchanged effort must not force full replay"
        );
        checkpoint.effort_history.as_mut().unwrap().changed = true;
        prior.render_episodes = None;
        checkpoint.restore_unchanged_effort(&mut prior);
        assert!(
            store_prior_needs_episode_spans(&prior),
            "any historical change still needs full replay"
        );
    }

    fn failed_step() -> Result<StepOutcome, RunFinishReason> {
        Ok(StepOutcome::Failed {
            error: ProviderError { provider_details: None, provider_subcode: None,
            refusal: None,
            class: broca_protocol::ErrorClass::ProviderUnavailable,
            message: "provider stream ended without an assembled assistant message and Finished control".into(),
            status: None,
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None, },
            retries_used: 0,
        })
    }

    /// The run-loop half of the cancel race: a step that failed while the run's cancel was
    /// tripped ends the run as Cancelled. The e2e test in broca-provider exercises the race
    /// itself but loses it only occasionally, so the rule is pinned here deterministically.
    /// Both control arms matter: a failure with no cancel stays a failure, and a completed
    /// step keeps its result even when cancel is set.
    #[test]
    fn a_step_that_failed_during_a_cancel_is_a_cancellation() {
        assert!(matches!(
            attribute_step_failure_to_cancel(failed_step(), true),
            Err(RunFinishReason::Cancelled)
        ));
        assert!(matches!(
            attribute_step_failure_to_cancel(failed_step(), false),
            Ok(StepOutcome::Failed { .. })
        ));
        let completed = Ok(StepOutcome::Completed {
            tool_args_cuts: Vec::new(),
            message: AssistantMessage {
                message_id: broca_protocol::ids::MessageId("m".into()),
                content: Vec::new(),
            },
            usage: Usage::default(),
            retries_used: 0,
            finish_reason: None,
            service_tier_used: None,
            speed_used: None,
        });
        assert!(matches!(
            attribute_step_failure_to_cancel(completed, true),
            Ok(StepOutcome::Completed { .. })
        ));
        assert!(matches!(
            attribute_step_failure_to_cancel(Err(RunFinishReason::Interrupted), true),
            Err(RunFinishReason::Interrupted)
        ));
    }

    #[test]
    fn resume_work_class_uses_frozen_value_over_caller_value() {
        assert_eq!(
            admitted_work_class(Some("athena"), true, false, Some("mason")),
            Some("mason".into())
        );
        assert_eq!(
            admitted_work_class(Some("athena"), false, true, Some("mason")),
            Some("athena".into())
        );
    }

    #[test]
    fn fresh_tool_filter_matches_clone_filter_and_preserves_request_order() {
        let make_tool = |name: &str, index: usize| Tool {
            name: name.into(),
            description: format!("description-{index}"),
            input_schema: serde_json::json!({"index": index}),
            module: Some(format!("module-{index}")),
            execution_mode: if index.is_multiple_of(2) {
                ExecutionMode::Parallel
            } else {
                ExecutionMode::Sequential
            },
            reply: None,
        };
        let mut req = RunRequest {
            input_written_at_ms: None,
            directives: None,
            session_class: None,
            retention: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("filter-order".into()),
            session: BindIdentity {
                project_root: "/tmp/filter-order".into(),
                harness: "test".into(),
                session: "filter-order".into(),
            },
            model: ModelHandle::new("stub", "stub-model"),
            input: Vec::new(),
            tools: vec![
                make_tool("denied-first", 0),
                make_tool("allowed-first", 1),
                make_tool("denied-middle", 2),
                make_tool("allowed-second", 3),
                make_tool("denied-last", 4),
            ],
            policy_allowed: ["allowed-first".into(), "allowed-second".into()]
                .into_iter()
                .collect(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            submission_id: None,
            context_management: None,
            inherit_frozen_cache: false,
            structured_output: None,
        };
        // This is the cloning filter that retain replaces; comparing complete Tool values pins
        // both policy membership and the provider-visible request order.
        let clone_filter_output = req
            .tools
            .iter()
            .filter(|tool| req.policy_allowed.contains(&tool.name))
            .cloned()
            .collect::<Vec<_>>();
        let prior = replay(&[]).expect("empty replay state");

        let provider = crate::provider::StubProvider::new(Vec::new());
        let cfg =
            request_config(&mut req, false, &prior, &provider).expect("fresh config resolves");

        assert_eq!(cfg.tools.as_ref(), &clone_filter_output);
        assert!(
            req.tools.is_empty(),
            "fresh config takes ownership of the request tool buffer"
        );
    }

    #[tokio::test]
    async fn retention_call_options_flag_and_projection_follow_first_wal_run() {
        for (name, retention) in [("retained", Some(3600)), ("forever", None)] {
            let root = broca_test_support::scratch_root()
                .join(format!("core-retention-call-{}-{name}", std::process::id()));
            let session = BindIdentity::new("/retention-test", "runner", name);
            let wal_store = crate::FileWalStore::new(root.join("wal"));
            let path = if retention.is_some() {
                wal_store.ephemeral_wal_path(&session)
            } else {
                wal_store.wal_path(&session)
            };
            crate::wal_writer::retention_tests::write_decoded_fixture(
                &path,
                &crate::retention::tests::frames(retention),
            );
            let submission = serde_json::from_value(serde_json::json!({
                "model": {"provider_module_id": "stub", "model_id": "m"},
                "input": [{"role": "user", "content": [{"type": "text", "text": "next"}]}],
                "stop_when": [{"type": "step_count", "n": 1}]
            }))
            .unwrap();
            let req = RunRequest::from_submission(
                submission,
                RunId("r2".into()),
                session,
                broca_protocol::SubmissionId("sub2".into()),
            );
            let provider = crate::StubProvider::new(vec![Vec::new()]);
            let store = Arc::new(CapturingProjectionStore::default());
            let (out, _events) = mpsc::channel(64);
            run_loop_append_episode(
                req,
                &provider,
                &crate::StubToolPlane::new(),
                &crate::FileLeaseStore::new(root.join("leases")),
                &crate::AllowAll,
                &wal_store,
                Some(store.clone()),
                &out,
            )
            .await
            .unwrap();
            let calls = provider.calls();
            assert_eq!(
                calls.len(),
                1,
                "the provider must observe the actual execution path"
            );
            assert_eq!(calls[0].ephemeral_session, retention.is_some());
            if retention.is_some() {
                assert_eq!(
                    store
                        .projection_calls
                        .load(std::sync::atomic::Ordering::SeqCst),
                    0,
                    "no content crosses the Persister seam, even on a terminal"
                );
            } else {
                assert!(
                    store
                        .projection_calls
                        .load(std::sync::atomic::Ordering::SeqCst)
                        > 0,
                    "forever projection positive control"
                );
            }
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn retention_projection_skips_per_turn_and_terminal_persister_calls() {
        let root = broca_test_support::scratch_root()
            .join(format!("core-retention-persister-{}", std::process::id()));
        let session = BindIdentity::new("/retention-test", "runner", "persister");
        let wal_store = crate::FileWalStore::new(root.join("wal"));
        crate::wal_writer::retention_tests::write_decoded_fixture(
            &wal_store.ephemeral_wal_path(&session),
            &crate::retention::tests::frames(Some(3600)),
        );
        let store = Arc::new(CapturingProjectionStore::default());
        for safe_h in [4, 5] {
            project_committed(&wal_store, store.clone(), &session, safe_h, false).await;
        }
        assert_eq!(
            store
                .projection_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(store.projected_seqs.lock().unwrap().is_empty());
        assert!(store.persisted_anchor.lock().unwrap().is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn retained_approval_debt_uses_only_the_content_free_persister_seam() {
        struct WalView {
            records: Vec<broca_wal::FramedRecord>,
            lineage: broca_wal::WalLineageId,
        }
        impl WalStore for WalView {
            fn open(
                &self,
                _: &BindIdentity,
            ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
                unreachable!("read-only fixture")
            }
            fn read_records(
                &self,
                _: &BindIdentity,
            ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
                Ok(self.records.clone())
            }
            fn read_records_range(
                &self,
                _: &BindIdentity,
                after: u64,
                through: Option<u64>,
                _: Option<&broca_wal::WalByteAnchor>,
            ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
                let mut range = broca_wal::WalRangeRead::without_anchors(
                    self.records
                        .iter()
                        .filter(|frame| {
                            frame.seq > after && through.is_none_or(|head| frame.seq <= head)
                        })
                        .cloned()
                        .collect(),
                );
                range.lineage_id = Some(self.lineage);
                Ok(range)
            }
        }
        let lineage = crate::projection_plan_tests::incoming_lineage();
        let records = crate::retention::tests::deferred_records(Some(3600), &lineage.to_string());
        let safe_h = records.last().unwrap().seq;
        let wal = WalView { records, lineage };
        let store = Arc::new(CapturingProjectionStore::default());
        project_committed(
            &wal,
            store.clone(),
            &BindIdentity::new("/retention-test", "runner", "approval"),
            safe_h,
            false,
        )
        .await;
        let metadata = store.retention_metadata.lock().unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].withdrawals_owed_runs, ["r1"]);
        assert!(!format!("{metadata:?}").contains(crate::retention::tests::MARKER));
        assert_eq!(
            store
                .projection_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(store.projected_seqs.lock().unwrap().is_empty());
        assert!(store.persisted_anchor.lock().unwrap().is_none());
    }

    #[test]
    fn transformed_render_reuses_payloads_and_append_keeps_the_snapshot() {
        let mut request = CallOptions::new(vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "large snapshot sentinel".into(),
            }],
        )]);
        request.tools = Arc::new(vec![Tool {
            name: "nested-tool".into(),
            description: "shared transform payload".into(),
            input_schema: serde_json::json!({"properties": {"nested": {"type": "object"}}}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }]);
        request.provider_options = Arc::new(serde_json::json!({"large": "x".repeat(4096)}));
        let opts = Arc::new(request);
        assert_eq!(Arc::strong_count(&opts.tools), 1);
        assert_eq!(Arc::strong_count(&opts.tool_choice), 1);
        assert_eq!(Arc::strong_count(&opts.response_format), 1);
        assert_eq!(Arc::strong_count(&opts.generation), 1);
        assert_eq!(Arc::strong_count(&opts.provider_options), 1);
        let transformed = opts.with_prompt(vec![Message::new(Role::User, Vec::new())]);
        // `with_prompt` must clone these Arc handles rather than their payloads, increasing each
        // strong count from one to two; copying a payload into a new Arc would leave it at one.
        assert_eq!(Arc::strong_count(&opts.tools), 2);
        assert_eq!(Arc::strong_count(&opts.tool_choice), 2);
        assert_eq!(Arc::strong_count(&opts.response_format), 2);
        assert_eq!(Arc::strong_count(&opts.generation), 2);
        assert_eq!(Arc::strong_count(&opts.provider_options), 2);
        assert!(Arc::ptr_eq(&opts.tools, &transformed.tools));
        assert!(Arc::ptr_eq(&opts.tool_choice, &transformed.tool_choice));
        assert!(Arc::ptr_eq(
            &opts.response_format,
            &transformed.response_format
        ));
        assert!(Arc::ptr_eq(&opts.generation, &transformed.generation));
        assert!(Arc::ptr_eq(
            &opts.provider_options,
            &transformed.provider_options
        ));
        drop(transformed);

        let allocation = Arc::as_ptr(&opts);
        let mut opts = opts;
        assert_eq!(
            Arc::strong_count(&opts),
            1,
            "append requires an exclusive snapshot"
        );
        mutable_call_options(&mut opts)
            .prompt
            .push(Message::new(Role::User, Vec::new()));
        assert_eq!(
            Arc::as_ptr(&opts),
            allocation,
            "append must not allocate a cloned prompt"
        );
    }

    struct CountingWal {
        last_seq: u64,
        sync_calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl broca_wal::Wal for CountingWal {
        fn append(
            &mut self,
            _fence: u64,
            _record: &WalRecord,
            sync: bool,
        ) -> Result<u64, broca_wal::WalError> {
            self.last_seq += 1;
            if sync {
                self.sync()?;
            }
            Ok(self.last_seq)
        }

        fn sync(&self) -> Result<(), broca_wal::WalError> {
            self.sync_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Ok(Vec::new())
        }

        fn tail_after_seq(
            &self,
            _after_seq: u64,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Ok(Vec::new())
        }

        fn last_seq(&self) -> u64 {
            self.last_seq
        }
    }

    struct RecordingWal {
        records: Arc<std::sync::Mutex<Vec<WalRecord>>>,
    }

    struct HeldProbePlane {
        durable_head: Arc<std::sync::atomic::AtomicU64>,
        groups: std::sync::Mutex<Vec<Vec<crate::tool_plane::DispatchRegistration>>>,
    }

    #[async_trait::async_trait]
    impl ToolPlane for HeldProbePlane {
        async fn batch_started(
            &self,
            _: &BatchCtx,
            _: &[ToolCall],
            _: &ToolManifest,
        ) -> Result<(), ToolPlaneError> {
            Ok(())
        }
        async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
            Ok(())
        }
        async fn dispatch(&self, _: &BatchCtx, _: &ToolCall) -> DispatchResult {
            panic!("durable call key was dropped")
        }
        async fn register_dispatch_group(
            &self,
            _: &BatchCtx,
            calls: &[crate::tool_plane::DispatchRegistration],
        ) -> Result<(), ToolPlaneError> {
            self.groups.lock().unwrap().push(calls.to_vec());
            Ok(())
        }
        async fn dispatch_keyed(
            &self,
            _: &BatchCtx,
            call: &ToolCall,
            key: crate::tool_plane::DispatchKey<'_>,
        ) -> DispatchResult {
            assert_eq!(self.groups.lock().unwrap().len(), 1);
            key.held_recorder
                .expect("batch supplies its writer")
                .sync_held(&crate::tool_plane::ProviderHeldReply {
                    tool_call_id: call.tool_call_id.clone(),
                    call_key: key.call_key.into(),
                    module: "provider".into(),
                    reason: "running".into(),
                    detail_digest: None,
                })
                .await
                .unwrap();
            assert_eq!(
                self.durable_head.load(std::sync::atomic::Ordering::Acquire),
                1,
                "held record must be synced before acting"
            );
            DispatchResult::Completed(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Text {
                    text: "late terminal".into(),
                },
                is_error: false,
            })
        }
    }

    #[tokio::test]
    async fn provider_held_dispatch_syncs_its_record_before_acting() {
        let records = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = WalHandle::spawn(
            Box::new(RecordingWal {
                records: records.clone(),
            }),
            1,
        )
        .unwrap();
        writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
        let ctx = BatchCtx {
            batch_id: BatchId("batch".into()),
            bind: BindIdentity::new("/project", "broca", "session"),
            run_id: RunId("run".into()),
            scope: None,
        };
        let inner = HeldProbePlane {
            durable_head: writer.durable_head(),
            groups: Default::default(),
        };
        let plane = ProviderHeldPlane {
            inner: &inner,
            writer: &writer,
            ctx: &ctx,
            sequential: true,
            held: ProviderHeldBatch::default(),
            lifecycle: None,
            clock: None,
        };
        let mut dispatch = PlannedDispatch::from(ToolCall {
            tool_call_id: ToolCallId("call".into()),
            tool_name: "tool".into(),
            input: serde_json::json!({}),
            thought_signature: None,
        });
        dispatch.call_key = Some("lineage:19".into());
        let plan = CallPlan::Dispatch(dispatch);
        register_dispatch_plans(&plane, &ctx, std::iter::once(&plan))
            .await
            .unwrap();
        let result = resolve(plan, &ctx, &plane, None, None).await;
        assert_eq!(
            result.result.output,
            ToolOutput::Text {
                text: "late terminal".into()
            }
        );
        let records = records.lock().unwrap().clone();
        assert_eq!(records.len(), 1);
        assert!(
            matches!(&records[0], WalRecord::ToolCallProviderHeld { batch_id, tool_call_id, call_key, module, reason, sequential: true, .. }
            if batch_id == &ctx.batch_id && tool_call_id.0 == "call" && call_key == "lineage:19" && module == "provider" && reason == "running")
        );
        writer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn malformed_provider_held_reply_is_recorded_as_refused_without_pausing() {
        let records = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = WalHandle::spawn(
            Box::new(RecordingWal {
                records: records.clone(),
            }),
            1,
        )
        .unwrap();
        writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
        let call = ToolCall {
            tool_call_id: ToolCallId("call".into()),
            tool_name: "tool".into(),
            input: serde_json::json!({}),
            thought_signature: None,
        };
        let manifest = ToolManifest::from_tools(vec![serde_json::from_value(serde_json::json!({"name":"tool","description":"","input_schema":{},"execution_mode":"parallel"})).unwrap()]);
        let plane = crate::StubToolPlane::new().with_tool("tool", |_| {
            crate::StubReply::Output(ToolOutput::Error {
                message: "malformed provider-held reply: prerequisites not met".into(),
            })
        });
        let (out, _events) = mpsc::channel(64);
        let outcome = dispatch_batch(
            &RunId("run".into()),
            StepId(1),
            &BindIdentity::new("/project", "broca", "session"),
            None,
            vec![call],
            &manifest,
            &plane,
            &crate::AllowAll,
            &writer,
            None,
            &[],
            false,
            None,
            None,
            None,
            &out,
            None,
        )
        .await
        .unwrap();
        assert!(outcome.interruption.is_none());
        assert!(outcome.open.is_empty());
        let records = records.lock().unwrap().clone();
        assert!(records.iter().any(|record| matches!(record, WalRecord::ToolResult { reason: Some(reason), .. } if reason == "malformed_provider_held_reply")));
        assert!(!records.iter().any(|record| matches!(
            record,
            WalRecord::RunPaused { .. } | WalRecord::ToolCallProviderHeld { .. }
        )));
        writer.shutdown().await.unwrap();
    }

    struct LineageRecordingWal(RecordingWal);
    impl broca_wal::Wal for LineageRecordingWal {
        fn append(
            &mut self,
            fence: u64,
            record: &WalRecord,
            sync: bool,
        ) -> Result<u64, broca_wal::WalError> {
            self.0.append(fence, record, sync)
        }
        fn sync(&self) -> Result<(), broca_wal::WalError> {
            self.0.sync()
        }
        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            self.0.replay()
        }
        fn tail_after_seq(
            &self,
            seq: u64,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            self.0.tail_after_seq(seq)
        }
        fn last_seq(&self) -> u64 {
            self.0.last_seq()
        }
        fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
            use sha2::Digest;
            let mut frame = [0x5a; 69];
            frame[..4].copy_from_slice(&16u32.to_le_bytes());
            frame[4] = 2;
            frame[5..21].fill(0);
            let digest = sha2::Sha256::digest([&frame[4..21], &frame[53..69]].concat());
            frame[21..53].copy_from_slice(&digest);
            let broca_wal::framing::DecodeStep::Lineage { lineage_id, .. } =
                broca_wal::framing::decode_next(&frame)
            else {
                panic!("test lineage frame")
            };
            Some(lineage_id)
        }
    }

    struct ParkedPlane {
        records: Arc<std::sync::Mutex<Vec<WalRecord>>>,
        head: Arc<std::sync::atomic::AtomicU64>,
        sent: std::sync::Mutex<Vec<ToolCallId>>,
        gate: Option<Arc<tokio::sync::Notify>>,
    }

    #[async_trait::async_trait]
    impl ToolPlane for ParkedPlane {
        async fn batch_started(
            &self,
            _: &BatchCtx,
            _: &[ToolCall],
            _: &ToolManifest,
        ) -> Result<(), ToolPlaneError> {
            Ok(())
        }
        async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
            Ok(())
        }
        async fn dispatch(&self, _: &BatchCtx, _: &ToolCall) -> DispatchResult {
            panic!("keyed dispatch required")
        }
        async fn dispatch_keyed(
            &self,
            _: &BatchCtx,
            call: &ToolCall,
            key: crate::tool_plane::DispatchKey<'_>,
        ) -> DispatchResult {
            self.sent.lock().unwrap().push(call.tool_call_id.clone());
            if call.tool_call_id.0 == "a" {
                if let Some(gate) = &self.gate {
                    gate.notified().await;
                }
                key.held_recorder
                    .unwrap()
                    .sync_held(&crate::tool_plane::ProviderHeldReply {
                        tool_call_id: call.tool_call_id.clone(),
                        call_key: key.call_key.into(),
                        module: "tools".into(),
                        reason: "running".into(),
                        detail_digest: None,
                    })
                    .await
                    .unwrap();
                return std::future::pending().await;
            }
            let records = self.records.lock().unwrap();
            let terminal = records.iter().position(|record| matches!(record, WalRecord::ToolResult { result, outcome: DispatchOutcome::Completed, .. } if result.tool_call_id.0 == "a")).expect("held terminal before successor send");
            assert!(
                self.head.load(Ordering::Acquire) > terminal as u64,
                "held terminal must be synced before successor send"
            );
            DispatchResult::Completed(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                is_error: false,
                output: ToolOutput::Text {
                    text: "successor".into(),
                },
            })
        }
    }

    struct DeliveredHeld;
    #[async_trait::async_trait]
    impl crate::tool_plane::ProviderHeldLifecycle for DeliveredHeld {
        async fn wait(
            &self,
            _: &BatchCtx,
            call: &ToolCall,
            _: &crate::tool_plane::ProviderHeldReply,
        ) -> DispatchResult {
            DispatchResult::Completed(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                is_error: false,
                output: ToolOutput::Text {
                    text: "answer".into(),
                },
            })
        }
    }

    #[tokio::test]
    async fn provider_held_sequential_pause_resume_syncs_before_successor() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut prefix = crate::replay::deferred_tests::batch(&["a", "b"]);
            if let WalRecord::ToolBatchStarted { batch_id, .. } = &mut prefix[2] {
                *batch_id = BatchId("r-b1".into());
            }
            let records = Arc::new(std::sync::Mutex::new(prefix));
            let writer = WalHandle::spawn(
                Box::new(LineageRecordingWal(RecordingWal {
                    records: records.clone(),
                })),
                1,
            )
            .unwrap();
            writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
            let plane = ParkedPlane {
                records: records.clone(),
                head: writer.durable_head(),
                sent: Default::default(),
                gate: None,
            };
            let calls: Vec<_> = match &records.lock().unwrap()[1] {
                WalRecord::ModelStepFinished {
                    assistant_message, ..
                } => assistant_message.tool_calls().cloned().collect(),
                _ => unreachable!(),
            };
            let manifest = ToolManifest::from_tools(vec![Tool {
                name: "edit".into(),
                description: "edit".into(),
                input_schema: serde_json::json!({"type":"object"}),
                module: Some("tools".into()),
                execution_mode: ExecutionMode::Sequential,
                reply: None,
            }]);
            let (out, _rx) = mpsc::channel(32);
            let control = RunControl::default();
            let batch = dispatch_batch(
                &RunId("r".into()),
                StepId(1),
                &BindIdentity::new("/project", "broca", "session"),
                None,
                calls.clone(),
                &manifest,
                &plane,
                &crate::AllowAll,
                &writer,
                Some(ResumeCtx {
                    batch_already_started: true,
                    ..Default::default()
                }),
                &[],
                false,
                None,
                None,
                None,
                &out,
                Some(&control),
            )
            .await
            .unwrap();
            assert_eq!(
                batch.interruption,
                Some(BatchInterruption::ProviderHeldPause)
            );
            assert_eq!(*plane.sent.lock().unwrap(), [ToolCallId("a".into())]);
            let summary = pause_approval(
                &writer,
                &out,
                &control,
                &BindIdentity::new("/project", "broca", "session"),
                &RunId("r".into()),
                1,
                Usage::default(),
                0,
                &[],
                true,
            )
            .await
            .unwrap();
            assert_eq!(summary.paused, Some(PauseReason::ToolHeld));
            let state = crate::replay(&records.lock().unwrap()).unwrap();
            let dispatch_times: Vec<_> = records
                .lock()
                .unwrap()
                .iter()
                .filter_map(|record| match record {
                    WalRecord::ToolDispatchIntent {
                        tool_call_id,
                        dispatched_at_ms,
                        ..
                    } => Some((tool_call_id.clone(), *dispatched_at_ms)),
                    _ => None,
                })
                .collect();
            assert_eq!(dispatch_times.len(), 2);
            assert!(
                dispatch_times.iter().all(|(_, time)| time.is_some()),
                "the writer stamps both barrier-1 observations"
            );
            let ReplayResolution::ProviderHeld { pending, .. } = state.resolution else {
                panic!("not provider-held")
            };
            assert_eq!(pending.held_successors, [ToolCallId("b".into())]);
            let frames: Vec<_> = records
                .lock()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(index, record)| broca_wal::FramedRecord {
                    seq: index as u64 + 1,
                    fence: 1,
                    record: record.clone(),
                })
                .collect();
            let intents = held_resume_intents(&pending, &frames, &writer).unwrap();
            let resumed = ResumeCtx {
                batch_already_started: true,
                intents,
                provider_held: pending.provider_held,
                ..Default::default()
            };
            let control = RunControl {
                provider_held_lifecycle: Some(Arc::new(DeliveredHeld)),
                ..Default::default()
            };
            let outcome = dispatch_batch(
                &RunId("r".into()),
                StepId(1),
                &BindIdentity::new("/project", "broca", "session"),
                None,
                calls,
                &manifest,
                &plane,
                &crate::AllowAll,
                &writer,
                Some(resumed),
                &[],
                false,
                None,
                None,
                None,
                &out,
                Some(&control),
            )
            .await
            .unwrap();
            assert_eq!(outcome.interruption, None);
            assert_eq!(outcome.results.len(), 2);
            assert_eq!(
                *plane.sent.lock().unwrap(),
                [ToolCallId("a".into()), ToolCallId("b".into())]
            );
            assert_eq!(
                records
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|record| matches!(record, WalRecord::ToolDispatchIntent { .. }))
                    .count(),
                2
            );
            assert_eq!(
                records
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|record| matches!(record, WalRecord::ToolResult { .. }))
                    .count(),
                2
            );
            assert_eq!(
                records
                    .lock()
                    .unwrap()
                    .iter()
                    .filter_map(|record| match record {
                        WalRecord::ToolDispatchIntent {
                            tool_call_id,
                            dispatched_at_ms,
                            ..
                        } => Some((tool_call_id.clone(), *dispatched_at_ms)),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                dispatch_times,
                "resuming does not re-observe either original dispatch intent",
            );
            assert!(
                records
                    .lock()
                    .unwrap()
                    .iter()
                    .filter_map(|record| match record {
                        WalRecord::ToolResult { finished_at_ms, .. } => Some(finished_at_ms),
                        _ => None,
                    })
                    .all(Option::is_some),
                "held delivery and successor completion use the writer's tool-time observations"
            );
            writer.shutdown().await.unwrap();
        })
        .await
        .expect("held batch must pause or deliver without sending through its hold");
    }

    struct WarmProbe {
        active: Arc<AtomicBool>,
        started: tokio::sync::Notify,
        calls: AtomicU64,
    }
    #[async_trait::async_trait]
    impl crate::keep_warm_wait::KeepWarmDriver for WarmProbe {
        fn timing(&self, _: &RunConfig) -> Result<crate::keep_warm_wait::WarmTiming, String> {
            Ok(crate::keep_warm_wait::WarmTiming {
                ttl_ms: 1000,
                lead_ms: 1,
                after_send_ms: Some(1),
            })
        }
        fn last_sent_at_ms(&self) -> Option<u64> {
            Some(0)
        }
        fn costs(&self, _: &RunConfig) -> Result<crate::keep_warm::WarmCosts, String> {
            Ok(crate::keep_warm::WarmCosts {
                warm: 1.0,
                bust: 100.0,
            })
        }
        async fn warm(&self, _: &RunConfig, _: &RunId) -> crate::keep_warm_wait::WarmTick {
            struct ActiveWarm(Arc<AtomicBool>);
            impl Drop for ActiveWarm {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::Release);
                }
            }
            self.active.store(true, Ordering::Release);
            let _active = ActiveWarm(self.active.clone());
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.started.notify_one();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn provider_held_pause_stops_keep_warm_for_a_day() {
        let records = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = WalHandle::spawn(
            Box::new(LineageRecordingWal(RecordingWal {
                records: records.clone(),
            })),
            1,
        )
        .unwrap();
        writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
        let gate = Arc::new(tokio::sync::Notify::new());
        let plane = ParkedPlane {
            records,
            head: writer.durable_head(),
            sent: Default::default(),
            gate: Some(gate.clone()),
        };
        let probe = Arc::new(WarmProbe {
            active: Arc::new(AtomicBool::new(false)),
            started: tokio::sync::Notify::new(),
            calls: AtomicU64::new(0),
        });
        let driver: Arc<dyn crate::keep_warm_wait::KeepWarmDriver> = probe.clone();
        let config = match crate::replay::deferred_tests::batch(&["a"]).remove(0) {
            WalRecord::RunStarted { config, .. } => config,
            _ => unreachable!(),
        };
        let run = RunId("r".into());
        let unsupported = AtomicBool::new(false);
        let missed = AtomicBool::new(false);
        let warm = crate::keep_warm_wait::WarmWait {
            driver: &driver,
            config: &config,
            run_id: &run,
            cancel: None,
            unsupported_logged: &unsupported,
            warm_missed: &missed,
        };
        let manifest = ToolManifest::from_tools(vec![Tool {
            name: "edit".into(),
            description: "edit".into(),
            input_schema: serde_json::json!({"type":"object"}),
            module: Some("tools".into()),
            execution_mode: ExecutionMode::Sequential,
            reply: None,
        }]);
        let (out, _rx) = mpsc::channel(16);
        let bind = BindIdentity::new("/project", "broca", "warm-held");
        let call = ToolCall {
            tool_call_id: ToolCallId("a".into()),
            tool_name: "edit".into(),
            input: serde_json::json!({}),
            thought_signature: None,
        };
        let batch = dispatch_batch(
            &run,
            StepId(1),
            &bind,
            None,
            vec![call],
            &manifest,
            &plane,
            &crate::AllowAll,
            &writer,
            None,
            &[],
            false,
            None,
            None,
            Some(warm),
            &out,
            None,
        );
        let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(batch, async {
                probe.started.notified().await;
                gate.notify_one();
            })
        })
        .await
        .expect("provider-held batch must pause rather than warm indefinitely");
        assert_eq!(
            result.unwrap().interruption,
            Some(BatchInterruption::ProviderHeldPause)
        );
        assert!(
            !probe.active.load(Ordering::Acquire),
            "pausing drops in-flight keep-warm IO"
        );
        let count = probe.calls.load(Ordering::Acquire);
        assert_eq!(count, 1, "live tool waits still warm");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;
        assert_eq!(
            probe.calls.load(Ordering::Acquire),
            count,
            "paused runs never warm"
        );
        writer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn direct_core_writer_freezes_agent_run_scope() {
        let scope: broca_protocol::SessionScope = serde_json::from_str(r#"{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"parent-run","scope_epoch":1,"sender":{"kind":"reserved","module_id":"basal"},"agent_id":"agent-1","run_id":"parent-run"}"#).unwrap();
        let submission = serde_json::from_value(serde_json::json!({
            "model": {"provider_module_id": "stub", "model_id": "m"},
            "input": [{"role": "user", "content": [{"type": "text", "text": "written"}]}],
            "stop_when": [{"type": "step_count", "n": 1}]
        }))
        .unwrap();
        let mut req = RunRequest::from_submission(
            submission,
            RunId("next".into()),
            BindIdentity::new("/agent-run-writer", "runner", "basal:codemode-parent-run:0"),
            broca_protocol::SubmissionId("next".into()),
        );
        req.scope = Some(scope.clone());
        let records = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = WalHandle::spawn(
            Box::new(RecordingWal {
                records: records.clone(),
            }),
            1,
        )
        .unwrap();
        let (out, _events) = mpsc::channel(64);
        let (ack, admitted) = tokio::sync::oneshot::channel();
        let result = run_loop_on_writer_admitted_with_prior_snapshot(
            req,
            &writer,
            StartMode::FreshOrResume,
            &crate::StubProvider::new(vec![Vec::new()]),
            &crate::StubToolPlane::new(),
            &crate::AllowAll,
            &ReadableWalStore,
            None,
            RunControl::default(),
            &out,
            Some(ack),
            None,
        )
        .await;
        assert!(result.is_ok(), "{result:?}");
        assert!(admitted.await.unwrap().is_ok());
        writer.sync().await.unwrap();
        assert!(records.lock().unwrap().iter().any(|record| matches!(
            record, WalRecord::RunStarted { scope: Some(frozen), .. } if frozen == &scope
        )));
        let _ = writer.shutdown().await;
    }

    impl broca_wal::Wal for RecordingWal {
        fn append(
            &mut self,
            _fence: u64,
            record: &WalRecord,
            _sync: bool,
        ) -> Result<u64, broca_wal::WalError> {
            let mut records = self.records.lock().unwrap();
            records.push(record.clone());
            Ok(records.len() as u64)
        }

        fn sync(&self) -> Result<(), broca_wal::WalError> {
            Ok(())
        }

        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Ok(Vec::new())
        }

        fn tail_after_seq(
            &self,
            _after_seq: u64,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Ok(Vec::new())
        }

        fn last_seq(&self) -> u64 {
            self.records.lock().unwrap().len() as u64
        }
    }

    struct ReadableWalStore;

    impl WalStore for ReadableWalStore {
        fn open(
            &self,
            _session: &BindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            unreachable!("projection only reads the WAL")
        }

        fn read_records(
            &self,
            _session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Ok(Vec::new())
        }
    }

    struct BoundedReadableWalStore;

    impl WalStore for BoundedReadableWalStore {
        fn open(
            &self,
            _session: &BindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            unreachable!("projection only reads the WAL")
        }

        fn read_records(
            &self,
            _session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Ok((1..=64)
                .map(|seq| broca_wal::FramedRecord {
                    seq,
                    fence: 1,
                    record: WalRecord::TurnFinished {
                        step_id: StepId(seq),
                    },
                })
                .collect())
        }
    }

    struct AnchoredReadableWalStore {
        observed_anchor: std::sync::Mutex<Option<broca_wal::WalByteAnchor>>,
    }

    impl WalStore for AnchoredReadableWalStore {
        fn open(
            &self,
            _session: &BindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            unreachable!("projection only reads the WAL")
        }

        fn read_records(
            &self,
            _session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            unreachable!("the test exercises the range implementation")
        }

        fn read_records_range(
            &self,
            _session: &BindIdentity,
            after_seq: u64,
            through_seq: Option<u64>,
            anchor: Option<&broca_wal::WalByteAnchor>,
        ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
            assert_eq!(after_seq, 1);
            assert_eq!(through_seq, Some(3));
            *self.observed_anchor.lock().unwrap() = anchor.copied();
            let records = (2..=3)
                .map(|seq| broca_wal::FramedRecord {
                    seq,
                    fence: 1,
                    record: WalRecord::TurnFinished {
                        step_id: StepId(seq),
                    },
                })
                .collect();
            let anchors = (2..=3)
                .map(|seq| broca_wal::WalByteAnchor {
                    file_id: 11,
                    seq,
                    byte_end: seq * 100,
                })
                .collect();
            Ok(broca_wal::WalRangeRead::with_anchors(records, anchors))
        }
    }

    #[derive(Default)]
    struct CapturingProjectionStore {
        seed: Option<crate::ProjectionSeed>,
        seed_watermark: u64,
        seed_anchor: Option<broca_wal::WalByteAnchor>,
        projected_watermark: Option<u64>,
        projected_seqs: std::sync::Mutex<Vec<u64>>,
        persisted_anchor: std::sync::Mutex<Option<broca_wal::WalByteAnchor>>,
        projection_calls: std::sync::atomic::AtomicUsize,
        retention_metadata:
            std::sync::Mutex<Vec<crate::projection_plan::RetainedWithdrawalProjection>>,
    }

    impl crate::MaterializedStore for CapturingProjectionStore {
        fn read_prefix_with_watermark(
            &self,
            _session: &BindIdentity,
        ) -> Result<crate::StorePrefix, String> {
            Ok(crate::StorePrefix::empty())
        }
    }

    impl crate::Persister for CapturingProjectionStore {
        fn project_retention_metadata(
            &self,
            _session: &BindIdentity,
            metadata: &crate::projection_plan::RetainedWithdrawalProjection,
        ) -> Result<(), String> {
            self.retention_metadata
                .lock()
                .unwrap()
                .push(metadata.clone());
            Ok(())
        }
        fn projection_seed(
            &self,
            _session: &BindIdentity,
        ) -> Result<Option<crate::ProjectionSeed>, String> {
            Ok(self.seed.clone().or_else(|| {
                Some(crate::ProjectionSeed {
                    watermark_seq: self.seed_watermark,
                    anchor: self.seed_anchor,
                    ..crate::ProjectionSeed::empty()
                })
            }))
        }

        fn project(
            &self,
            _session: &BindIdentity,
            records: &[broca_wal::FramedRecord],
        ) -> Result<u64, String> {
            self.projection_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            *self.projected_seqs.lock().unwrap() =
                records.iter().map(|record| record.seq).collect();
            Ok(self
                .projected_watermark
                .unwrap_or_else(|| records.last().map(|record| record.seq).unwrap_or(0)))
        }

        fn persist_wal_anchor(
            &self,
            _session: &BindIdentity,
            anchor: Option<&broca_wal::WalByteAnchor>,
        ) -> Result<(), String> {
            *self.persisted_anchor.lock().unwrap() = anchor.copied();
            Ok(())
        }
    }

    #[derive(Default)]
    struct BlockingProjectionState {
        started: bool,
        released: bool,
        calls: usize,
        projected_seqs: Vec<u64>,
    }

    #[derive(Default)]
    struct BlockingProjectionStore {
        state: std::sync::Mutex<BlockingProjectionState>,
        changed: std::sync::Condvar,
    }

    impl BlockingProjectionStore {
        fn wait_until_started(&self) {
            let state = self.state.lock().unwrap();
            let (state, timeout) = self
                .changed
                .wait_timeout_while(state, std::time::Duration::from_secs(2), |state| {
                    !state.started
                })
                .unwrap();
            assert!(!timeout.timed_out(), "projection write did not start");
            assert!(state.started);
        }

        fn release(&self) {
            let mut state = self.state.lock().unwrap();
            state.released = true;
            self.changed.notify_all();
        }
    }

    impl crate::MaterializedStore for BlockingProjectionStore {
        fn read_prefix_with_watermark(
            &self,
            _session: &BindIdentity,
        ) -> Result<crate::StorePrefix, String> {
            Ok(crate::StorePrefix::empty())
        }
    }

    impl crate::Persister for BlockingProjectionStore {
        fn project(
            &self,
            _session: &BindIdentity,
            records: &[broca_wal::FramedRecord],
        ) -> Result<u64, String> {
            let mut state = self.state.lock().unwrap();
            state.started = true;
            self.changed.notify_all();
            while !state.released {
                state = self.changed.wait(state).unwrap();
            }
            state.calls += 1;
            state.projected_seqs = records.iter().map(|record| record.seq).collect();
            Ok(records.last().map_or(0, |record| record.seq))
        }
    }

    /// Serializes the tests that read the process-wide projection warning counter. libtest runs
    /// tests on parallel threads in one process, so one test's failing projection moves the count
    /// another test is asserting on: an exact count then fails, or a reset lands between another
    /// test's before and after reads. Any test that makes a projection fail, or reads the
    /// counter, holds this for its whole body.
    static PROJECTION_COUNTER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct FailingProjectionStore;

    impl crate::MaterializedStore for FailingProjectionStore {
        fn read_prefix_with_watermark(
            &self,
            _session: &BindIdentity,
        ) -> Result<crate::StorePrefix, String> {
            Ok(crate::StorePrefix::empty())
        }
    }

    impl crate::Persister for FailingProjectionStore {
        fn project(
            &self,
            _session: &BindIdentity,
            _records: &[broca_wal::FramedRecord],
        ) -> Result<u64, String> {
            Err("injected projection failure".into())
        }
    }

    #[tokio::test]
    async fn tripped_sequential_resume_preserves_already_durable_terminal() {
        let batch_id = BatchId("resume-trip-b1".into());
        let prior_result = ToolResult {
            tool_call_id: ToolCallId("call_b".into()),
            output: ToolOutput::Json {
                value: serde_json::json!({
                    "status": "committed before crash",
                    "nested": {"attempt": 7}
                }),
            },
            is_error: false,
        };
        let prior_record = WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            hooks: None,
            batch_id: batch_id.clone(),
            result: prior_result.clone().into(),
            outcome: DispatchOutcome::Completed,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
        };
        let prior_bytes = serde_json::to_vec(&prior_record).expect("serialize prior terminal");
        let records = Arc::new(std::sync::Mutex::new(vec![prior_record]));
        let writer = WalHandle::spawn(
            Box::new(RecordingWal {
                records: records.clone(),
            }),
            1,
        )
        .expect("spawn writer");
        // This test calls `dispatch_batch` directly, skipping `reconstruct_prior`, the admission
        // step that seeds the writer's tag state from the WAL. Seed it as that step would: the
        // seeded history's only record is an untagged ToolResult, which is a lineage admitted
        // before tags existed, so its state is `Off` and the new records stay untagged.
        writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
        let calls = vec![
            ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call_a".into()),
                tool_name: "edit".into(),
                input: serde_json::json!({"path": "a"}),
            },
            ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call_b".into()),
                tool_name: "edit".into(),
                input: serde_json::json!({"path": "b"}),
            },
        ];
        let manifest = ToolManifest::from_tools(vec![Tool {
            name: "edit".into(),
            description: "edit a file".into(),
            input_schema: serde_json::json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Sequential,
            reply: None,
        }]);
        let plane = crate::StubToolPlane::new().with_tool("edit", |call| {
            if call.tool_call_id.0 == "call_a" {
                crate::StubReply::OutcomeUnknown("reset after send".into())
            } else {
                crate::StubReply::Output(ToolOutput::Text {
                    text: "SHOULD-NOT-RUN".into(),
                })
            }
        });
        let resume = ResumeCtx {
            batch_already_started: true,
            prepared: Vec::new(),
            provider_held: Vec::new(),
            intents: std::collections::HashMap::new(),
            existing: [(
                ToolCallId("call_b".into()),
                std::collections::VecDeque::from([prior_result.clone()]),
            )]
            .into_iter()
            .collect(),
        };
        let (out, _events) = mpsc::channel(8);

        let outcome = dispatch_batch(
            &RunId("resume-trip".into()),
            StepId(1),
            &BindIdentity::new("/tmp/project", "test", "resume-trip"),
            None,
            calls,
            &manifest,
            &plane,
            &crate::AllowAll,
            &writer,
            Some(resume),
            &[],
            false,
            None,
            None,
            None,
            &out,
            None,
        )
        .await
        .expect("resume batch resolves");

        assert_eq!(outcome.interruption, Some(BatchInterruption::Ambiguous));
        assert_eq!(outcome.results.len(), 2);
        assert_eq!(
            serde_json::to_vec(&outcome.results[1]).expect("serialize resumed result"),
            serde_json::to_vec(&prior_result).expect("serialize prior result"),
            "the resumed result remains byte-identical"
        );
        assert_eq!(plane.exec_count(&ToolCallId("call_b".into())), 0);

        {
            let records = records.lock().unwrap();
            let b_terminals: Vec<&WalRecord> = records
                .iter()
                .filter(|record| match record {
                    WalRecord::ToolNoDispatch { tool_call_id, .. } => tool_call_id.0 == "call_b",
                    WalRecord::ToolResult { result, .. } => result.tool_call_id.0 == "call_b",
                    _ => false,
                })
                .collect();
            assert_eq!(
                b_terminals.len(),
                1,
                "an existing durable terminal must not be rewritten"
            );
            assert_eq!(
                serde_json::to_vec(b_terminals[0]).expect("serialize retained terminal"),
                prior_bytes,
                "the durable terminal record remains byte-identical"
            );
        }
        let _ = writer.shutdown().await;
    }

    #[tokio::test]
    async fn cancellation_wins_before_pause_becomes_durable() {
        let records = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = WalHandle::spawn(
            Box::new(RecordingWal {
                records: records.clone(),
            }),
            1,
        )
        .expect("spawn writer");
        let cancel = CancellationToken::new();
        cancel.cancel();
        let control = RunControl {
            cancel: Some(cancel),
            ..Default::default()
        };
        let (out, mut events) = mpsc::channel(8);

        let summary = pause_run(
            &writer,
            &out,
            &control,
            RunId("paused-race".into()),
            broca_protocol::PauseReason::AuthRequired,
            None,
            2,
            Usage::default(),
            1,
        )
        .await
        .unwrap();

        assert_eq!(summary.reason, RunFinishReason::Cancelled);
        assert_eq!(summary.paused, None);
        assert!(matches!(
            events.recv().await,
            Some(OutputEvent::Control(ControlUnit::RunFinished { .. }))
        ));
        assert!(matches!(
            records.lock().unwrap().as_slice(),
            [WalRecord::RunFinished {
                reason: RunFinishReason::Cancelled,
                ..
            }]
        ));
        let _ = writer.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_projection_uses_exact_fsync_head_despite_later_append() {
        let sync_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let writer = Arc::new(
            WalHandle::spawn(
                Box::new(CountingWal {
                    last_seq: 0,
                    sync_calls: sync_calls.clone(),
                }),
                1,
            )
            .expect("spawn writer"),
        );
        let (projector, pending) = projection_channel();
        let (out, mut events) = mpsc::channel(1);
        out.send(OutputEvent::Control(ControlUnit::RunFinished {
            run_id: Some(RunId("run-probe".into())),
            reason: RunFinishReason::Completed,
            usage: None,
            final_step_finish_reason: None,
            indeterminate_tool_calls: None,
            retries_used: 0,
        }))
        .await
        .unwrap();

        let task_writer = writer.clone();
        let task = tokio::spawn(async move {
            let result = finish_run(
                task_writer.as_ref(),
                &RunControl::default(),
                &out,
                &RunId("run-probe".into()),
                RunFinishReason::Completed,
                0,
                Usage::default(),
                0,
                None,
                None,
            )
            .await;
            (result, projector)
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while sync_calls.load(std::sync::atomic::Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("terminal barrier fsyncs");

        let later_seq = writer
            .record(WalRecord::ApprovalRequested {
                tool_call_id: ToolCallId("racing-actor-record".into()),
            })
            .into_result()
            .unwrap();
        assert_eq!(later_seq, 2);
        events.recv().await.expect("remove prefilled event");
        events.recv().await.expect("receive terminal event");
        let (result, projector) = task.await.expect("finish task joins");
        let (_summary, last_fsynced_seq) = result.expect("finish succeeds");
        projector.enqueue_terminal(last_fsynced_seq);

        let request = *pending.borrow();
        assert_eq!(
            request.safe_h, 1,
            "projection must stop at the fsynced head"
        );
        assert_eq!(writer.next_record_seq(), 3);
        projector.close();
        drop(projector);
        drop(pending);
        let _ = Arc::try_unwrap(writer)
            .ok()
            .expect("finish task released writer")
            .shutdown()
            .await;
    }

    #[tokio::test]
    async fn budgeted_terminal_projection_uses_a_valid_tail_seed() {
        struct BudgetTailWalStore {
            observed_after: std::sync::Mutex<Vec<u64>>,
        }
        impl WalStore for BudgetTailWalStore {
            fn open(
                &self,
                _session: &BindIdentity,
            ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
                unreachable!("projection only reads the WAL")
            }

            fn read_records(
                &self,
                _session: &BindIdentity,
            ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
                unreachable!("valid budget seed must avoid full reads")
            }

            fn read_records_range(
                &self,
                _session: &BindIdentity,
                after_seq: u64,
                through_seq: Option<u64>,
                _anchor: Option<&broca_wal::WalByteAnchor>,
            ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
                assert_eq!(through_seq, Some(10));
                self.observed_after.lock().unwrap().push(after_seq);
                Ok(broca_wal::WalRangeRead::with_anchors(
                    (after_seq + 1..=10)
                        .map(|seq| broca_wal::FramedRecord {
                            seq,
                            fence: 1,
                            record: WalRecord::TurnFinished {
                                step_id: StepId(seq),
                            },
                        })
                        .collect(),
                    Vec::new(),
                ))
            }
        }

        let config = RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("stub", "m"),
            tools: Vec::new().into(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: Some(BudgetConfig {
                max_input_tokens: None,
                nudges: Vec::new(),
                finalize_at_tool_calls: 99,
                finalize_text: "finalize".into(),
            }),
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        };
        let seed = crate::ProjectionSeed {
            open_episode_span: Some(1),
            open_episode: None,
            earlier_episodes: Vec::new(),
            projection_episodes: None,
            deferred_calls: None,
            watermark_seq: 8,
            anchor: None,
            prefix_len: 2,
            completed_steps: 1,
            usage: Usage::default(),
            retries_used: 0,
            last_step_usage: None,
            last_step_finish_reason: None,
            trailing_origin: None,
            last_run_id: Some(RunId("budget".into())),
            admission_checkpoint: crate::AdmissionCheckpointV1 {
                head: None,
                has_deferred_calls: None,
                tags: Some(crate::tags::LineageTags::Off),
                pending_order: None,
                input_budget: Some(crate::input_budget::InputTokenBudget::default()),
                session_class: None,
                // A valid tail seed includes the first run's WAL-derived policy.
                lineage_retention: Some(crate::LineageRetention::Forever),
                compaction: None,
                effort_history: None,
                ownership: Some(crate::SessionOwnershipV1::default()),
                at_seq: 8,
                last_episode: Some(crate::LastEpisodeV1 {
                    cancel_at: None,
                    on_restart: None,
                    pause_reason: None,
                    keep_warm: None,
                    marker_order_version: None,
                    request_identity_digest: None,
                    run_id: RunId("budget".into()),
                    config,
                    input: Vec::new(),
                    provider_call_started: true,
                    config_refrozen: false,
                    disposition: crate::EpisodeDisposition::Running,
                    usage: None,
                    retries_used: 0,
                    budget_replay_state: Some(crate::BudgetReplayStateV1 {
                        version: crate::BUDGET_REPLAY_STATE_VERSION,
                        tool_call_count: 0,
                        fired_nudge_indices: Vec::new(),
                        finalize_fired: false,
                        finalize_pending: false,
                        budget_last_threshold: None,
                    }),
                }),
                pending_fifo: Arc::new(Vec::new()),
            },
            tag_seed: Default::default(),
        };
        let wal_store = BudgetTailWalStore {
            observed_after: std::sync::Mutex::new(Vec::new()),
        };
        let store = Arc::new(CapturingProjectionStore {
            seed: Some(seed),
            ..Default::default()
        });
        project_committed(
            &wal_store,
            store.clone() as Arc<dyn SessionStore>,
            &BindIdentity::new("/tmp/project", "runner", "budget-tail"),
            10,
            true,
        )
        .await;
        assert_eq!(*wal_store.observed_after.lock().unwrap(), vec![8]);
        assert_eq!(*store.projected_seqs.lock().unwrap(), vec![9, 10]);
    }

    #[tokio::test]
    async fn projection_read_is_bounded_by_the_fsynced_safe_head() {
        let session = BindIdentity::new("/tmp/project", "runner", "session");
        let store = Arc::new(CapturingProjectionStore::default());
        let (projector, pending) = projection_channel();
        projector.enqueue(2);
        let task_store = store.clone();
        let task = tokio::spawn(async move {
            projector_task(
                &BoundedReadableWalStore,
                task_store as Arc<dyn SessionStore>,
                &session,
                pending,
            )
            .await;
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store
                .projection_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("projection starts");
        projector.close();
        task.await.expect("projector task joins");

        assert_eq!(*store.projected_seqs.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn projector_coalesces_rapid_barriers_to_the_highest_safe_head() {
        const BARRIERS: u64 = 64;
        let session = BindIdentity::new("/tmp/project", "runner", "coalesced");
        let store = Arc::new(CapturingProjectionStore::default());
        let (projector, pending) = projection_channel();

        for safe_h in 1..=BARRIERS {
            projector.enqueue(safe_h);
        }
        let task_store = store.clone();
        let task = tokio::spawn(async move {
            projector_task(
                &BoundedReadableWalStore,
                task_store as Arc<dyn SessionStore>,
                &session,
                pending,
            )
            .await;
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store
                .projection_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("projection starts");
        projector.close();
        task.await.expect("projector task joins");

        assert_eq!(
            store.projected_seqs.lock().unwrap().last().copied(),
            Some(BARRIERS)
        );
        assert!(
            store
                .projection_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                < BARRIERS as usize,
            "coalescing must perform fewer iterations than published barriers"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn projector_shutdown_waits_for_current_write_and_skips_queued_work() {
        let session = BindIdentity::new("/tmp/project", "runner", "blocked-drain");
        let store = Arc::new(BlockingProjectionStore::default());
        let (projector, pending) = projection_channel();
        projector.enqueue(1);
        let task_store = store.clone();
        let mut task = tokio::spawn(async move {
            projector_task(
                &BoundedReadableWalStore,
                task_store as Arc<dyn SessionStore>,
                &session,
                pending,
            )
            .await;
        });

        let wait_store = store.clone();
        tokio::task::spawn_blocking(move || wait_store.wait_until_started())
            .await
            .expect("start waiter joins");
        projector.enqueue(2);
        projector.close();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut task)
                .await
                .is_err(),
            "shutdown must not abandon the in-flight blocking write"
        );

        store.release();
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("drain completes after write release")
            .expect("projector task joins");
        let state = store.state.lock().unwrap();
        assert_eq!(state.calls, 1, "queued-only projection must be skipped");
        assert_eq!(state.projected_seqs, vec![1]);
    }

    #[tokio::test]
    async fn projection_consumes_and_persists_exact_watermark_anchor() {
        let session = BindIdentity::new("/tmp/project", "runner", "session");
        let seed_anchor = broca_wal::WalByteAnchor {
            file_id: 11,
            seq: 1,
            byte_end: 100,
        };
        let wal_store = AnchoredReadableWalStore {
            observed_anchor: std::sync::Mutex::new(None),
        };
        let store = Arc::new(CapturingProjectionStore {
            seed_watermark: 1,
            seed_anchor: Some(seed_anchor),
            projected_watermark: Some(2),
            ..CapturingProjectionStore::default()
        });

        project_committed(
            &wal_store,
            store.clone() as Arc<dyn SessionStore>,
            &session,
            3,
            false,
        )
        .await;

        assert_eq!(
            *wal_store.observed_anchor.lock().unwrap(),
            Some(seed_anchor)
        );
        assert_eq!(*store.projected_seqs.lock().unwrap(), vec![2, 3]);
        assert_eq!(
            *store.persisted_anchor.lock().unwrap(),
            Some(broca_wal::WalByteAnchor {
                file_id: 11,
                seq: 2,
                byte_end: 200,
            }),
            "the projection watermark can precede the range's last record"
        );
    }

    /// Projection reads the WAL through the same decoder, so a WAL a newer build appended to
    /// projects nothing: the store's watermark stays put rather than advancing over a record
    /// this build cannot interpret, and the failure is counted as a projection warning.
    #[tokio::test]
    async fn projection_projects_nothing_from_a_newer_writer_wal() {
        let _counter = PROJECTION_COUNTER_LOCK.lock().await;
        let root = broca_test_support::scratch_root()
            .join(format!("broca-projection-newer-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let wal_store = crate::FileWalStore::new(root.join("wal"));
        let session = BindIdentity::new("/tmp/project", "runner", "newer-writer");
        let record = |n| WalRecord::StepStarted { step_id: StepId(n) };
        {
            let mut wal = wal_store.open(&session).unwrap();
            wal.append(1, &record(1), true).unwrap();
            wal.append(1, &record(2), true).unwrap();
        }
        let path = wal_store.wal_path(&session);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend(
            broca_wal::framing::encode_with_requires(3, 1, &["from-the-future/v1"], &record(3))
                .unwrap(),
        );
        std::fs::write(&path, bytes).unwrap();
        assert!(matches!(
            wal_store.read_records_range(&session, 0, Some(3), None),
            Err(broca_wal::WalError::NewerWriter { ver: 3, .. })
        ));

        let store = Arc::new(CapturingProjectionStore::default());
        let warnings_before = projection_failures();
        project_committed(
            &wal_store,
            store.clone() as Arc<dyn SessionStore>,
            &session,
            3,
            false,
        )
        .await;
        assert_eq!(
            store
                .projection_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(store.projected_seqs.lock().unwrap().is_empty());
        assert!(projection_failures() > warnings_before);
        assert!(crate::projection_health::is_behind(&session));
        project_committed(
            &ReadableWalStore,
            Arc::new(CapturingProjectionStore::default()) as Arc<dyn SessionStore>,
            &session,
            3,
            false,
        )
        .await;
        assert!(!crate::projection_health::is_behind(&session));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn projection_warning_mutations_are_not_test_gated() {
        // A compile-time assertion cannot prove that these call sites survive non-test builds:
        // Rust const evaluation has no way to inspect a statement's cfg attributes. This source
        // check guards the exact failure mode by inspecting the statements compiled in production.
        let source = include_str!("run.rs");
        let mutation = ["PROJECTION_WARNING_COUNT", ".fetch_add"].concat();
        let mutations: Vec<_> = source.match_indices(&mutation).collect();
        assert_eq!(
            mutations.len(),
            3,
            "expected content-read, content-write and retained-metadata warning counter mutations"
        );

        for (offset, _) in mutations {
            let since_previous_statement = source[..offset]
                .rsplit_once(';')
                .map_or(&source[..offset], |(_, suffix)| suffix);
            let compact: String = since_previous_statement
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect();
            assert!(
                !compact.contains("#[cfg(test)]"),
                "projection warning counter mutation is test-gated"
            );
        }
    }

    #[tokio::test]
    async fn projection_failure_then_success_clears_behind_but_keeps_lifetime_count() {
        let _counter = PROJECTION_COUNTER_LOCK.lock().await;
        let failures_before = projection_failures();
        let session = BindIdentity::new("/tmp/project", "runner", "projection-recovery");

        let (projector, pending) = projection_channel();
        projector.enqueue(1);
        let task_session = session.clone();
        let task_session_probe = session.clone();
        let task = tokio::spawn(async move {
            projector_task(
                &ReadableWalStore,
                Arc::new(FailingProjectionStore) as Arc<dyn SessionStore>,
                &task_session,
                pending,
            )
            .await;
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !crate::projection_health::is_behind(&task_session_probe) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("projection failure is reported");
        projector.close();
        task.await.expect("projector task joins");

        // This retry succeeds: health no longer reports the session behind, while the lifetime
        // projection_failures metric still records the original failed attempt.
        // Other tests project in parallel, so the process-wide counters only ever prove a lower
        // bound; this session's own membership is exact.
        let failures_after_failure = super::projection_failures();
        assert!(
            failures_after_failure > failures_before,
            "a failing projection must be visible to operators"
        );
        assert!(
            crate::projection_health::is_behind(&session),
            "the failed session is still behind"
        );

        project_committed(
            &ReadableWalStore,
            Arc::new(CapturingProjectionStore::default()) as Arc<dyn SessionStore>,
            &session,
            1,
            false,
        )
        .await;
        assert!(
            projection_failures() >= failures_after_failure,
            "recovery preserves the process-lifetime failure count"
        );
        assert!(
            !crate::projection_health::is_behind(&session),
            "successful projection clears the session's recovery debt"
        );
    }

    #[tokio::test]
    async fn two_projection_failures_with_one_recovery_leave_one_session_behind() {
        let _counter = PROJECTION_COUNTER_LOCK.lock().await;
        let failures_before = projection_failures();
        let first = BindIdentity::new("/tmp/project", "runner", "health-first");
        let second = BindIdentity::new("/tmp/project", "runner", "health-second");

        for session in [&first, &second] {
            project_committed(
                &ReadableWalStore,
                Arc::new(FailingProjectionStore) as Arc<dyn SessionStore>,
                session,
                1,
                false,
            )
            .await;
        }
        let failures_after = projection_failures();
        assert!(failures_after >= failures_before + 2);
        assert!(crate::projection_health::is_behind(&first));
        assert!(crate::projection_health::is_behind(&second));

        project_committed(
            &ReadableWalStore,
            Arc::new(CapturingProjectionStore::default()) as Arc<dyn SessionStore>,
            &first,
            1,
            false,
        )
        .await;
        assert!(projection_failures() >= failures_after);
        assert!(!crate::projection_health::is_behind(&first));
        assert!(
            crate::projection_health::is_behind(&second),
            "recovering one session leaves the other session's debt intact"
        );

        // Do not leave synthetic debt for later tests in this shared process.
        project_committed(
            &ReadableWalStore,
            Arc::new(CapturingProjectionStore::default()) as Arc<dyn SessionStore>,
            &second,
            1,
            false,
        )
        .await;
        assert!(!crate::projection_health::is_behind(&second));
    }

    struct CancellationProbeProvider {
        started: Arc<Notify>,
        cancelled: Arc<AtomicBool>,
    }

    impl CancellationProbeProvider {
        fn new() -> Self {
            Self {
                started: Arc::new(Notify::new()),
                cancelled: Arc::new(AtomicBool::new(false)),
            }
        }

        async fn wait_cancelled(&self) {
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while !self.cancelled.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("step token was not cancelled");
        }
    }

    #[async_trait::async_trait]
    impl Provider for CancellationProbeProvider {
        async fn step(
            &self,
            _step_id: StepId,
            _opts: &CallOptions,
        ) -> mpsc::Receiver<NormalizedEvent> {
            panic!("cancellation tests must pass the step token to the provider")
        }

        async fn step_with_cancel(
            &self,
            _step_id: StepId,
            _opts: &CallOptions,
            cancel: CancellationToken,
            _deadline: Option<tokio::time::Instant>,
        ) -> mpsc::Receiver<NormalizedEvent> {
            let (tx, rx) = mpsc::channel(1);
            let started = Arc::clone(&self.started);
            let cancelled = Arc::clone(&self.cancelled);
            tokio::spawn(async move {
                started.notify_one();
                cancel.cancelled().await;
                cancelled.store(true, Ordering::SeqCst);
                drop(tx);
            });
            rx
        }
    }

    /// If each step clones the run cancellation token, the watchdog cancellation propagates to the
    /// run itself, causing the final assertion to fail.
    #[tokio::test]
    async fn watchdog_step_cancel_is_isolated_from_run_cancel() {
        let provider = CancellationProbeProvider::new();
        let run_cancel = CancellationToken::new();
        let control = RunControl {
            cancel: Some(run_cancel.clone()),
            provider_call_timeout: Some(std::time::Duration::from_millis(10)),
            ..Default::default()
        };
        let (out, _output_rx) = mpsc::channel(1);

        let outcome = run_step_watched(
            &provider,
            StepId(1),
            &Arc::new(CallOptions::new(Vec::new())),
            &control,
            &out,
            &|_| Box::pin(async { Ok(()) }),
        )
        .await;

        assert!(matches!(outcome, Ok(Err(RunFinishReason::Interrupted))));
        provider.wait_cancelled().await;
        assert!(
            !run_cancel.is_cancelled(),
            "a watchdog must cancel only the timed-out step"
        );
    }

    #[tokio::test]
    async fn no_timeout_parent_cancel_propagates_to_step() {
        let provider = CancellationProbeProvider::new();
        let run_cancel = CancellationToken::new();
        let control = RunControl {
            cancel: Some(run_cancel.clone()),
            provider_call_timeout: None,
            ..Default::default()
        };
        let (out, _output_rx) = mpsc::channel(1);
        let opts = Arc::new(CallOptions::new(Vec::new()));
        let run = run_step_watched(&provider, StepId(1), &opts, &control, &out, &|_| {
            Box::pin(async { Ok(()) })
        });
        let cancel_parent = async {
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                provider.started.notified(),
            )
            .await
            .expect("provider step did not start");
            run_cancel.cancel();
        };

        let (outcome, ()) = tokio::join!(run, cancel_parent);

        assert!(matches!(outcome, Ok(Err(RunFinishReason::Cancelled))));
        provider.wait_cancelled().await;
    }

    struct MessageWithoutFinishedProvider;

    #[async_trait::async_trait]
    impl Provider for MessageWithoutFinishedProvider {
        async fn step(
            &self,
            _step_id: StepId,
            _opts: &CallOptions,
        ) -> mpsc::Receiver<NormalizedEvent> {
            let (tx, rx) = mpsc::channel(1);
            tx.send(NormalizedEvent::Control(
                ProviderControl::AssistantMessage {
                    message: AssistantMessage {
                        message_id: broca_protocol::MessageId("unterminated".into()),
                        content: vec![ContentBlock::Text {
                            text: "not authoritative".into(),
                        }],
                    },
                },
            ))
            .await
            .unwrap();
            rx
        }
    }

    #[tokio::test]
    async fn message_without_finished_control_is_not_authoritative() {
        let (out, _output_rx) = mpsc::channel(2);
        let outcome = run_step(
            &MessageWithoutFinishedProvider,
            StepId(1),
            Arc::new(CallOptions::new(Vec::new())),
            &CancellationToken::new(),
            None,
            &out,
            &mut None,
            &|_| Box::pin(async { Ok(()) }),
            None,
            None,
        )
        .await;

        match outcome.unwrap() {
            StepOutcome::Failed { error, .. } => {
                assert_eq!(error.class, ErrorClass::ProviderUnavailable);
                assert!(error.message.contains("Finished control"));
            }
            StepOutcome::Completed { .. } => {
                panic!("a message without Finished must not complete a model step")
            }
        }
    }
}

#[cfg(test)]
#[path = "run/terminal_regression_tests.rs"]
mod terminal_regression_tests;

#[cfg(test)]
#[path = "run/hook_tests.rs"]
mod hook_tests;

/// The run stores this approval wait on the heap to keep its future's stack size bounded.
/// A call's durable records decide whether it still awaits consent; the status marker
/// only supplies display metadata and can never authorize execution.
mod approval_gate {
    use super::*;
    use crate::tool_plane::{ApprovalPage, ApprovalRouteError};
    use broca_wal::{DeferredDecision, LateDecision, LateExecution, LateOutcome};
    use futures::{stream::FuturesUnordered, StreamExt};
    use std::{collections::HashMap, future::Future, pin::Pin, time::Duration};

    #[derive(Clone)]
    pub(super) struct Held {
        pub key: String,
        pub prepared: Arc<WalRecord>,
        pub id: Option<String>,
        pub approved: bool,
        filed: bool,
        retry_at: u64,
        backoff: u64,
    }
    impl Held {
        fn call(&self) -> ToolCall {
            let WalRecord::ToolCallDeferred {
                call_id,
                tool,
                args_jcs,
                ..
            } = &*self.prepared
            else {
                unreachable!()
            };
            ToolCall {
                tool_call_id: call_id.clone(),
                tool_name: tool.clone(),
                input: serde_json::from_str(args_jcs).expect("Prepared contains valid JSON"),
                thought_signature: None,
            }
        }
        fn expires(&self) -> u64 {
            let WalRecord::ToolCallDeferred { ask, .. } = &*self.prepared else {
                unreachable!()
            };
            ask.expires_at_ms
        }
        fn ctx(&self, ctx: &BatchCtx) -> BatchCtx {
            let WalRecord::ToolCallDeferred { scope, .. } = &*self.prepared else {
                unreachable!()
            };
            BatchCtx {
                scope: Some(scope.clone()),
                ..ctx.clone()
            }
        }
    }
    enum Slot {
        Ready(Option<CallPlan>),
        Held(Held),
        Running,
        Done(CallResolution),
    }
    enum Work {
        Dispatched(CallResolution),
        Validated(Option<CallResolution>),
        Filed(
            Result<crate::consent::RecoveredCard, ApprovalRouteError>,
            Option<String>,
        ),
        Awaited(Result<Option<crate::consent::ConsentAnswer>, ApprovalRouteError>),
    }
    enum Feedback {
        Reported,
        Released(Held, Option<crate::consent::ConsentAnswer>),
    }
    struct PolledPage {
        page: ApprovalPage,
        complete: bool,
    }
    #[derive(Default)]
    struct FeedProgress {
        initialized: bool,
        cursor: Option<crate::tool_plane::ApprovalCursor>,
        records: Vec<serde_json::Value>,
        seen: HashSet<(String, u8)>,
    }
    type FeedDecisions = Vec<(
        String,
        Result<crate::consent::ConsentAnswer, ApprovalRouteError>,
    )>;
    type AnswerRead<'a> =
        Pin<Box<dyn Future<Output = Result<PolledPage, ApprovalRouteError>> + Send + 'a>>;
    type Job<'a> = Pin<Box<dyn Future<Output = (usize, Work)> + Send + 'a>>;

    pub(super) struct HistoryFrames {
        base: Arc<Vec<broca_wal::FramedRecord>>,
        appended: Vec<broca_wal::FramedRecord>,
    }
    impl HistoryFrames {
        fn iter(&self) -> impl DoubleEndedIterator<Item = &broca_wal::FramedRecord> + Clone {
            self.base.iter().chain(self.appended.iter())
        }
        fn first(&self) -> Option<&broca_wal::FramedRecord> {
            self.iter().next()
        }
        fn last(&self) -> Option<&broca_wal::FramedRecord> {
            self.iter().next_back()
        }
        fn push(&mut self, frame: broca_wal::FramedRecord) {
            self.appended.push(frame);
        }
    }
    pub(super) struct History {
        pub frames: HistoryFrames,
        pub(super) projection: crate::replay::DeferredCallProjection,
        scans: HashMap<String, Arc<std::sync::Mutex<crate::consent::LookupProgress>>>,
        live_scan: Arc<std::sync::Mutex<FeedProgress>>,
    }
    impl History {
        pub fn new(
            writer: &WalHandle,
            frames: impl Into<Arc<Vec<broca_wal::FramedRecord>>>,
        ) -> Result<Self, RunError> {
            let lineage = writer
                .lineage_id()
                .ok_or_else(|| RunError::Wal("approval requires WAL lineage".into()))?;
            let frames = frames.into();
            let mut projection = crate::replay::DeferredCallProjection::new(lineage);
            projection.fold_frames(&frames);
            Ok(Self {
                // New records must not force a copy of the shared admission vector.
                frames: HistoryFrames {
                    base: frames,
                    appended: Vec::new(),
                },
                projection,
                scans: HashMap::new(),
                live_scan: Arc::new(std::sync::Mutex::new(FeedProgress::default())),
            })
        }
        fn scan(&mut self, key: &str) -> Arc<std::sync::Mutex<crate::consent::LookupProgress>> {
            self.scans.entry(key.into()).or_default().clone()
        }
        fn completed_feed_candidate(
            &mut self,
            key: &str,
            answer: crate::consent::ConsentAnswer,
            cursor: &crate::tool_plane::ApprovalCursor,
        ) -> Result<crate::consent::ConsentAnswer, ApprovalRouteError> {
            let prepared = self
                .projection
                .calls
                .get(key)
                .expect("feed candidate has a prepared call")
                .prepared
                .clone();
            // Retain the checked snapshot and its head cursor. The authorization
            // gate still reads any tail appended before it mints a capability.
            self.scan(key)
                .lock()
                .expect("consent authorization progress")
                .confirm_snapshot(key, &prepared, &answer, cursor)?;
            Ok(answer)
        }
        pub(super) fn require_installed_plane(&self, control: &RunControl) -> Result<(), RunError> {
            let held = self.projection.calls.values().any(|call| {
                matches!(
                    call.state,
                    crate::replay::OpenCallState::Deferred | crate::replay::OpenCallState::Approved
                )
            });
            let late = !self
                .projection
                .obligations()
                .pending_late_execution
                .is_empty();
            require_approval_adapter(control, None, held, late)
        }
        fn fold(&mut self, seq: u64, record: WalRecord) {
            let frame = broca_wal::FramedRecord {
                seq,
                fence: 0,
                record,
            };
            self.projection.fold_frames(std::slice::from_ref(&frame));
            self.frames.push(frame);
        }
        fn record(&mut self, writer: &WalHandle, record: WalRecord) {
            if let Ok(seq) = writer.record(record.clone()).into_result() {
                self.fold(seq, record);
            }
        }
        pub fn withdrawal(
            &self,
            batch: &BatchId,
            id: &ToolCallId,
        ) -> Option<broca_wal::WithdrawElicitation> {
            self.projection
                .calls
                .iter()
                .find_map(|(key, call)| match &*call.prepared {
                    WalRecord::ToolCallDeferred {
                        batch_id, call_id, ..
                    } if batch_id == batch && call_id == id => {
                        Some(broca_wal::WithdrawElicitation {
                            call_key: key.clone(),
                            elicitation_id: call.elicitation_id.clone(),
                        })
                    }
                    _ => None,
                })
        }
        fn resume(&self, call: crate::replay::PreparedCall) -> Result<Held, RunError> {
            // A projection tail's record index is not a WAL sequence. Locate the actual
            // Prepared frame instead of inventing a new anchor from that relative index.
            let (key, projected) = self
                .projection
                .calls
                .iter()
                .find(|(_, value)| value.prepared == call.prepared)
                .ok_or_else(|| RunError::Wal("Prepared anchor missing from full WAL".into()))?;
            let approved = call.approval.as_deref().is_some_and(|approval| {
                matches!(
                    approval,
                    WalRecord::ApprovalRecorded {
                        decision: DeferredDecision::Approved,
                        ..
                    }
                )
            });
            if call.approval.is_some() && !approved {
                return Err(RunError::Wal(
                    "resumed call lacks an approved decision".into(),
                ));
            }
            if let WalRecord::ToolCallDeferred {
                args_jcs, executor, ..
            } = &*call.prepared
            {
                serde_json::from_str::<serde_json::Value>(args_jcs).map_err(|error| {
                    RunError::Wal(format!("invalid Prepared arguments: {error}"))
                })?;
                if *executor != broca_wal::DeferredExecutor::Broca {
                    return Err(RunError::Wal("Prepared executor is not broca".into()));
                }
            }
            // An AwaitingApproval id is only identity metadata, not proof that
            // reconciliation completed. Only ApprovalRecorded proves authorization;
            // every still-pending resume starts through lookup before get or await.
            Ok(Held {
                key: key.clone(),
                prepared: call.prepared,
                id: call
                    .elicitation_id
                    .or_else(|| projected.elicitation_id.clone()),
                approved,
                filed: false,
                retry_at: 0,
                backoff: 50,
            })
        }
    }
    pub(super) fn require_approval_adapter(
        control: &RunControl,
        pending: Option<&PendingTurn>,
        held: bool,
        late: bool,
    ) -> Result<(), RunError> {
        if control.elicitation_plane.is_some() {
            return Ok(());
        }
        crate::resume::refuse_prepared_resume(pending)?;
        if held || late {
            return Err(crate::resume::prepared_resume_refusal());
        }
        Ok(())
    }
    fn now(control: &RunControl) -> u64 {
        control.now_ms().unwrap_or(0)
    }
    const PRE_TOOL_UNAVAILABLE: &str = "pre_tool_unavailable";

    fn closed(call: &ToolCall, reason: &str) -> ToolResult {
        error_result(call, &format!("{reason}: the tool was not executed"))
    }
    async fn marker(
        slots: &[Slot],
        ctx: &BatchCtx,
        writer: &WalHandle,
        history: &mut History,
    ) -> Result<(), RunError> {
        let pending = slots
            .iter()
            .filter_map(|slot| match slot {
                Slot::Held(call) if !call.approved => Some(broca_wal::AwaitingApprovalEntry {
                    call_key: call.key.clone(),
                    call_id: call.call().tool_call_id,
                    elicitation_id: call.id.clone(),
                    expires_at_ms: call.expires(),
                }),
                _ => None,
            })
            .collect();
        history.record(
            writer,
            WalRecord::AwaitingApproval {
                batch_id: ctx.batch_id.clone(),
                pending,
            },
        );
        writer.sync().await?;
        Ok(())
    }
    async fn decision(
        call: &Held,
        decision: DeferredDecision,
        digest: Option<String>,
        writer: &WalHandle,
        history: &mut History,
    ) -> Result<Option<CallResolution>, RunError> {
        decision_reason(call, decision, digest, None, None, writer, history).await
    }
    // Move the authorization through approved_decision and decision_reason so it
    // cannot be reused. A second approval must check the latest consent feed again.
    pub(super) async fn approved_decision(
        call: &Held,
        authorization: crate::consent::AuthorizedAnswer,
        writer: &WalHandle,
        history: &mut History,
    ) -> Result<Option<CallResolution>, RunError> {
        decision_reason(
            call,
            DeferredDecision::Approved,
            None,
            None,
            Some(authorization),
            writer,
            history,
        )
        .await
    }
    async fn refusal_decision(
        call: &Held,
        error: &ApprovalRouteError,
        writer: &WalHandle,
        history: &mut History,
    ) -> Result<CallResolution, RunError> {
        Ok(decision_reason(
            call,
            DeferredDecision::Unavailable,
            None,
            Some(&error.code),
            None,
            writer,
            history,
        )
        .await?
        .expect("refusal closes"))
    }
    async fn decision_reason(
        call: &Held,
        decision: DeferredDecision,
        digest: Option<String>,
        code: Option<&str>,
        authorization: Option<crate::consent::AuthorizedAnswer>,
        writer: &WalHandle,
        history: &mut History,
    ) -> Result<Option<CallResolution>, RunError> {
        if decision == DeferredDecision::Approved
            && !authorization.is_some_and(|proof| {
                proof.value().decision == crate::consent::ConsentDecision::Approved
                    && call.id.as_deref() == Some(proof.value().id.as_str())
                    && proof.matches(&call.key, &call.prepared)
            })
        {
            return Err(RunError::Wal(
                "approval_record_requires_authorization: candidate was not reconciled".into(),
            ));
        }
        let reason = match decision {
            DeferredDecision::Approved => None,
            DeferredDecision::Declined => Some("pre_tool_declined"),
            DeferredDecision::Expired => Some("pre_tool_expired"),
            DeferredDecision::Unavailable => Some(PRE_TOOL_UNAVAILABLE),
        };
        let result = reason.map(|reason| {
            let reason =
                code.map_or_else(|| reason.to_string(), |code| format!("{reason}: {code}"));
            Arc::new(closed(&call.call(), &reason))
        });
        history.record(
            writer,
            WalRecord::ApprovalRecorded {
                closed_at_ms: None,
                tags: None,
                call_key: call.key.clone(),
                call_id: call.call().tool_call_id,
                elicitation_id: call.id.clone(),
                decision,
                approval_digest: digest.filter(|_| decision == DeferredDecision::Approved),
                closing_result: result.clone(),
            },
        );
        writer.sync().await?;
        Ok(result.map(|result| CallResolution::plain(Arc::unwrap_or_clone(result), None)))
    }
    pub(super) async fn close_open(
        open: &[Held],
        ctx: &BatchCtx,
        writer: &WalHandle,
    ) -> Result<(), RunError> {
        for call in open {
            writer.record(WalRecord::ToolResult {
                finished_at_ms: None,
                tags: None,
                batch_id: ctx.batch_id.clone(),
                result: Arc::new(crate::resume::not_dispatched_result(
                    &call.call().tool_call_id,
                )),
                outcome: DispatchOutcome::Interrupted,
                reason: None,
                hooks: None,
                provider_held: None,
                withdraw_elicitation: (!call.approved).then(|| broca_wal::WithdrawElicitation {
                    call_key: call.key.clone(),
                    elicitation_id: call.id.clone(),
                }),
            });
        }
        writer.sync().await?;
        Ok(())
    }
    pub(super) async fn report_close_out(
        history: &History,
        records: &[WalRecord],
        bind: &BindIdentity,
        control: &RunControl,
    ) {
        let Some(plane) = &control.elicitation_plane else {
            return;
        };
        for record in records {
            let WalRecord::ToolResult {
                batch_id,
                result,
                outcome,
                ..
            } = record
            else {
                continue;
            };
            for call in history
                .projection
                .calls
                .values()
                .filter(|call| call.decision == Some(DeferredDecision::Approved))
            {
                let WalRecord::ToolCallDeferred {
                    batch_id: origin,
                    call_id,
                    scope,
                    ..
                } = &*call.prepared
                else {
                    continue;
                };
                if origin != batch_id || call_id != &result.tool_call_id {
                    continue;
                }
                if let (Some(id), Some(run_id)) = (&call.elicitation_id, &call.run_id) {
                    let ctx = BatchCtx {
                        batch_id: batch_id.clone(),
                        bind: bind.clone(),
                        run_id: run_id.clone(),
                        scope: Some(scope.clone()),
                    };
                    let value = if *outcome == DispatchOutcome::Unknown {
                        crate::tool_plane::ApprovalExecution::OutcomeUnknown
                    } else {
                        crate::tool_plane::ApprovalExecution::ApprovedNotExecuted
                    };
                    crate::consent::report(plane.as_ref(), &ctx, id, value, None).await;
                }
            }
        }
    }
    pub(super) async fn report_open(open: &[Held], ctx: &BatchCtx, control: &RunControl) {
        if let Some(plane) = &control.elicitation_plane {
            for call in open.iter().filter(|call| call.approved) {
                if let Some(id) = &call.id {
                    crate::consent::report(
                        plane.as_ref(),
                        &call.ctx(ctx),
                        id,
                        crate::tool_plane::ApprovalExecution::ApprovedNotExecuted,
                        None,
                    )
                    .await;
                }
            }
        }
    }
    pub(super) async fn sync_result(
        resolution: &mut CallResolution,
        ctx: &BatchCtx,
        writer: &WalHandle,
        hooks: Option<&crate::hooks::HookRuntime<'_>>,
    ) -> Result<(), RunError> {
        if let Some(outcome) = resolution.outcome.take() {
            let record = WalRecord::ToolResult {
                finished_at_ms: None,
                tags: None,
                batch_id: ctx.batch_id.clone(),
                result: Arc::clone(&resolution.result),
                outcome,
                reason: resolution.reason.clone(),
                hooks: resolution.hooks.take(),
                withdraw_elicitation: None,
                provider_held: None,
            };
            if let Some((seal, stage)) = hooks
                .and_then(|runtime| runtime.seal)
                .zip(resolution.hook_stage.as_ref())
            {
                record_hook_stage(seal, stage, writer, record).await;
            } else {
                writer.record(record);
            }
            // Commit each completed call immediately, even while other calls in the
            // assistant's batch are still running or waiting for approval.
            writer.sync().await?;
        }
        Ok(())
    }
    async fn validate<T: ToolPlane>(
        held: &Held,
        ctx: &BatchCtx,
        step: StepId,
        plane: &T,
        hooks: Option<&crate::hooks::HookRuntime<'_>>,
    ) -> Option<CallResolution> {
        let call = held.call();
        if matches!(
            &*held.prepared,
            WalRecord::ToolCallDeferred {
                schema_pin: Some(_),
                ..
            }
        ) {
            let reason = "approval_schema_pin_unsupported";
            let mut result = CallResolution::plain(
                route_drained_result(&call.tool_call_id, reason),
                Some(DispatchOutcome::Completed),
            );
            result.reason = Some(reason.into());
            return Some(result);
        }
        if let Some(runtime) = hooks {
            // A second validation must refuse even subscriptions that normally pass on
            // unavailability. The clone leaves the originating plan unchanged.
            let plan = runtime.plan.second_validation();
            let strict = crate::hooks::HookRuntime {
                plan: &plan,
                ..*runtime
            };
            let mut subject = pre_tool_subject(
                step,
                &call,
                cortexkit_role_step_transform_provider::subscription::Phase::Validate,
            );
            if let crate::hooks::Subject::PreTool { call_key, .. } = &mut subject {
                *call_key = Some(held.key.clone());
            }
            let output = strict
                .chain(subject, crate::hooks::Trace::default(), |_| true)
                .await;
            if output.cancellation.is_some() {
                return Some(stopped_resolution(&call.tool_call_id));
            }
            if let Some((provider, text, unavailable)) = output.refusal {
                let CallPlan::HookTerminal {
                    result,
                    reason,
                    hooks,
                } = pre_tool_terminal(
                    &call,
                    &call.input,
                    &output.trace,
                    &provider,
                    &text,
                    unavailable,
                )
                else {
                    unreachable!()
                };
                let mut resolution = CallResolution::plain(
                    Arc::unwrap_or_clone(result),
                    Some(DispatchOutcome::Completed),
                );
                resolution.reason = Some(reason);
                resolution.hooks = hooks;
                return Some(resolution);
            }
        }
        let WalRecord::ToolCallDeferred { module, .. } = &*held.prepared else {
            unreachable!()
        };
        match plane
            .recheck_approval_scope(&held.ctx(ctx), &call, module.as_deref())
            .await
        {
            Ok(()) => None,
            Err(error) => {
                let reason = match error {
                    ToolPlaneError::ScopeDrained { reason } => reason,
                    ToolPlaneError::ScopeRefused { code, .. } => code,
                    ToolPlaneError::Unavailable(reason) => reason,
                };
                let mut result = CallResolution::plain(
                    route_drained_result(&call.tool_call_id, &reason),
                    Some(DispatchOutcome::Completed),
                );
                result.reason = Some(reason);
                Some(result)
            }
        }
    }
    #[allow(clippy::too_many_arguments)]
    async fn gate<A: ApprovalPolicy>(
        mut call: ToolCall,
        step: StepId,
        manifest: &ToolManifest,
        approval: &A,
        hooks: Option<&crate::hooks::HookRuntime<'_>>,
        ctx: &BatchCtx,
        plane: &impl ToolPlane,
        writer: &WalHandle,
        history: &mut History,
        elicitation_installed: bool,
    ) -> Result<Slot, RunError> {
        let no_dispatch = |call: &ToolCall, reason, metadata| {
            Slot::Ready(Some(CallPlan::NoDispatch {
                tool_call_id: call.tool_call_id.clone(),
                reason,
                hooks: metadata,
            }))
        };
        if let Err(error) = manifest.validate_call(&call) {
            return Ok(no_dispatch(
                &call,
                NoDispatchReason::ValidationFailed {
                    detail: error.to_string(),
                },
                None,
            ));
        }
        let original = call.input.clone();
        let mut trace = crate::hooks::Trace::default();
        if let Some(runtime) = hooks {
            let output = runtime
                .chain(
                    pre_tool_subject(
                        step,
                        &call,
                        cortexkit_role_step_transform_provider::subscription::Phase::Mutate,
                    ),
                    trace,
                    |_| true,
                )
                .await;
            trace = output.trace;
            if output.cancellation.is_some() {
                return Ok(Slot::Ready(Some(CallPlan::Stopped(call.tool_call_id))));
            }
            if let crate::hooks::Subject::PreTool { input, .. } = output.subject {
                call.input = input;
            }
            if let Some((provider, text, unavailable)) = output.refusal {
                return Ok(Slot::Ready(Some(pre_tool_terminal(
                    &call,
                    &original,
                    &trace,
                    &provider,
                    &text,
                    unavailable,
                ))));
            }
        }
        if let Err(error) = manifest.validate_call(&call) {
            return Ok(no_dispatch(
                &call,
                NoDispatchReason::ValidationFailed {
                    detail: error.to_string(),
                },
                trace.metadata(original, call.input.clone(), true),
            ));
        }
        // Narrow the validated mutation before asking for consent or freezing bytes.
        match approval.decide(&call).await {
            ApprovalResponse::Deny { reason, .. } => {
                return Ok(no_dispatch(
                    &call,
                    NoDispatchReason::Denied { reason },
                    trace.metadata(original, call.input.clone(), true),
                ))
            }
            ApprovalResponse::AllowNarrowed { input, .. } => call.input = input,
            ApprovalResponse::Allow { .. } => {}
        }
        let mut ask = None;
        if let Some(runtime) = hooks {
            for phase in [
                cortexkit_role_step_transform_provider::subscription::Phase::Approve,
                cortexkit_role_step_transform_provider::subscription::Phase::Validate,
            ] {
                let output = runtime
                    .chain(pre_tool_subject(step, &call, phase), trace, |_| true)
                    .await;
                trace = output.trace;
                if output.cancellation.is_some() {
                    return Ok(Slot::Ready(Some(CallPlan::Stopped(call.tool_call_id))));
                }
                if let crate::hooks::Subject::PreTool { input, .. } = output.subject {
                    call.input = input;
                }
                if let Some((provider, text, unavailable)) = output.refusal {
                    return Ok(Slot::Ready(Some(pre_tool_terminal(
                        &call,
                        &original,
                        &trace,
                        &provider,
                        &text,
                        unavailable,
                    ))));
                }
                if output.ask.is_some() {
                    ask = output.ask;
                }
            }
        }
        let Some(ask) = ask else {
            let mut dispatch = PlannedDispatch::from(call);
            dispatch.hooks = trace.metadata(original, (*dispatch.input).clone(), false);
            return Ok(Slot::Ready(Some(CallPlan::Dispatch(dispatch))));
        };
        let Some(scope) = &ctx.scope else {
            return Ok(Slot::Ready(Some(CallPlan::Synthetic {
                result: Arc::new(closed(&call, "approve_requires_scope")),
                reason: Some("approve_requires_scope".into()),
            })));
        };
        if !elicitation_installed {
            // Without the requester and its withdrawal owner, no deferred-call
            // record may be emitted, even when an approval hook asks for consent.
            return Ok(Slot::Ready(Some(CallPlan::Synthetic {
                result: Arc::new(closed(&call, PRE_TOOL_UNAVAILABLE)),
                reason: Some(PRE_TOOL_UNAVAILABLE.into()),
            })));
        }
        let bytes = serde_jcs::to_vec(&call.input)
            .map_err(|error| RunError::Wal(format!("freeze approved arguments: {error}")))?;
        use sha2::Digest;
        let args_hash = format!("{:x}", sha2::Sha256::digest(&bytes));
        // Filing retains this exact Prepared record after sync, so observe its time before
        // sharing it; the filed record must still equal the durable record.
        let prepared = WalRecord::ToolCallDeferred {
            deferred_at_ms: epoch_millis(),
            batch_id: ctx.batch_id.clone(),
            call_id: call.tool_call_id.clone(),
            executor: broca_wal::DeferredExecutor::Broca,
            module: plane.dispatch_target(ctx, &call.tool_call_id),
            tool: call.tool_name,
            schema_pin: None,
            args_jcs: String::from_utf8(bytes).expect("JCS is UTF-8"),
            args_hash,
            scope: scope.clone(),
            ask,
        };
        let seq = writer.record(prepared.clone()).into_result()?;
        // No route operation can observe this key until the Prepared frame is durable.
        writer.sync().await?;
        let key = crate::context::call_key(
            writer
                .lineage_id()
                .ok_or_else(|| RunError::Wal("Prepared lineage missing".into()))?,
            seq,
        );
        history.fold(seq, prepared.clone());
        Ok(Slot::Held(Held {
            key,
            prepared: Arc::new(prepared),
            id: None,
            approved: false,
            filed: false,
            retry_at: 0,
            backoff: 50,
        }))
    }

    async fn prepare_ready(
        plan: &mut CallPlan,
        ctx: &BatchCtx,
        plane: &impl ToolPlane,
        writer: &WalHandle,
    ) -> Result<(), RunError> {
        match plan {
            CallPlan::Dispatch(call) => {
                if call.call_key.is_some() {
                    return Ok(());
                }
                let seq = writer
                    .record(WalRecord::ToolDispatchIntent {
                        dispatched_at_ms: None,
                        batch_id: ctx.batch_id.clone(),
                        tool_call_id: call.tool_call_id.clone(),
                        tool_name: Some(call.tool_name.clone()),
                        args: Arc::clone(&call.input),
                        module_id: plane.dispatch_target(ctx, &call.tool_call_id),
                        schema_pin: None,
                        approval_digest: None,
                        call_key: None,
                        hooks: call.hooks.clone(),
                    })
                    .into_result()?;
                call.call_key = writer
                    .lineage_id()
                    .map(|lineage| crate::context::call_key(lineage, seq));
            }
            CallPlan::NoDispatch {
                tool_call_id,
                reason,
                hooks,
            } => {
                writer.record(WalRecord::ToolNoDispatch {
                    dispatched_at_ms: None,
                    tags: None,
                    batch_id: ctx.batch_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                    reason: reason.clone(),
                    hooks: hooks.clone(),
                });
            }
            CallPlan::HookTerminal {
                result,
                reason,
                hooks,
            } => {
                writer.record(WalRecord::ToolResult {
                    finished_at_ms: None,
                    tags: None,
                    batch_id: ctx.batch_id.clone(),
                    result: Arc::clone(result),
                    reason: Some(reason.clone()),
                    hooks: hooks.clone(),
                    outcome: DispatchOutcome::Completed,
                    withdraw_elicitation: None,
                    provider_held: None,
                });
            }
            _ => return Ok(()),
        }
        writer.sync().await?;
        register_dispatch_plans(plane, ctx, std::iter::once(&*plan)).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn batch<T: ToolPlane, A: ApprovalPolicy>(
        run_id: &RunId,
        step: StepId,
        session: &BindIdentity,
        scope: Option<&broca_protocol::SessionScope>,
        calls: Vec<ToolCall>,
        manifest: &ToolManifest,
        plane: &T,
        approval: &A,
        writer: &WalHandle,
        resume: Option<ResumeCtx>,
        firings: &[BudgetFiring],
        emit: bool,
        control: &RunControl,
        hooks: Option<&crate::hooks::HookRuntime<'_>>,
        warm: Option<crate::keep_warm_wait::WarmWait<'_>>,
        out: &mpsc::Sender<OutputEvent>,
        history: &mut History,
        restart: bool,
    ) -> Result<BatchOutcome, RunError> {
        if !hooks.is_some_and(|runtime| runtime.plan.approves())
            && resume.as_ref().is_none_or(|r| r.prepared.is_empty())
        {
            return dispatch_batch(
                run_id,
                step,
                session,
                scope,
                calls,
                manifest,
                plane,
                approval,
                writer,
                resume,
                firings,
                emit,
                control.stop_requested.as_ref(),
                hooks,
                warm,
                out,
                Some(control),
            )
            .await;
        }
        let ctx = BatchCtx {
            batch_id: BatchId(format!("{}-b{}", run_id.0, step.0)),
            bind: session.clone(),
            run_id: run_id.clone(),
            scope: scope.cloned(),
        };
        let resumed = resume.as_ref().is_some_and(|r| r.batch_already_started);
        history.projection.last_run_id = Some(ctx.run_id.clone());
        if !resumed {
            writer.record(WalRecord::ToolBatchStarted {
                batch_id: ctx.batch_id.clone(),
                step_id: step,
                planned_calls: calls.iter().map(|c| c.tool_call_id.clone()).collect(),
            });
        }
        let mut resume = resume.unwrap_or(ResumeCtx {
            batch_already_started: false,
            existing: HashMap::new(),
            prepared: Vec::new(),
            provider_held: Vec::new(),
            intents: std::collections::HashMap::new(),
        });
        let mut held: HashMap<ToolCallId, Held> = HashMap::new();
        for prepared in resume.prepared {
            let call = history.resume(prepared)?;
            held.insert(call.call().tool_call_id, call);
        }
        // Restore the provider saved in ToolCallDeferred instead of resolving an
        // unqualified tool name again against a catalog that may have changed.
        let routed_manifest = if held.is_empty() {
            manifest.clone()
        } else {
            let mut tools = manifest.tools();
            for tool in &mut tools {
                if let Some(module) = held.values().find_map(|call| match &*call.prepared {
                    WalRecord::ToolCallDeferred {
                        tool: name,
                        module: Some(module),
                        ..
                    } if name == &tool.name => Some(module.clone()),
                    _ => None,
                }) {
                    tool.module = Some(module);
                }
            }
            let mut routed = ToolManifest::from_tools(tools);
            if let Some(frozen) =
                history
                    .frames
                    .iter()
                    .rev()
                    .find_map(|frame| match &frame.record {
                        WalRecord::RunStarted {
                            run_id: origin,
                            manifest,
                            ..
                        } if origin == run_id => manifest.as_deref(),
                        _ => None,
                    })
            {
                routed = routed
                    .with_plan_manifest(frozen)
                    .map_err(|error| RunError::Wal(error.to_string()))?;
            }
            routed
        };
        let announced =
            !is_cancelled(control) && !stop_is_requested(control.stop_requested.as_ref());
        let start = if announced {
            plane.batch_started(&ctx, &calls, &routed_manifest).await
        } else {
            Err(ToolPlaneError::Unavailable(
                "batch stopped before route open".into(),
            ))
        };
        let sequential = calls.iter().any(|call| {
            manifest
                .get(&call.tool_name)
                .is_some_and(|tool| tool.execution_mode == ExecutionMode::Sequential)
        });
        let mut slots = Vec::with_capacity(calls.len());
        // Duplicates are decided by source position before restored results or Prepared calls
        // consume any id, as the plain dispatcher does. A first instance answered from the WAL
        // still makes every later instance with its id a duplicate, so a resume refuses the
        // same calls the live batch refused.
        let mut seen = HashSet::new();
        let duplicate_calls: Vec<bool> = calls
            .iter()
            .map(|call| !seen.insert(call.tool_call_id.clone()))
            .collect();
        for (call, is_duplicate) in calls.into_iter().zip(duplicate_calls) {
            if emit {
                let _ = send(
                    out,
                    OutputEvent::Control(ControlUnit::ToolCall { call: call.clone() }),
                )
                .await;
            }
            let slot = if let Some(result) = resume
                .existing
                .get_mut(&call.tool_call_id)
                .and_then(|queue| queue.pop_front())
            {
                Slot::Ready(Some(CallPlan::Already(result)))
            } else if let Some(dispatch) = resume.intents.remove(&call.tool_call_id) {
                Slot::Ready(Some(CallPlan::Dispatch(dispatch)))
            } else if let Some(prepared) = held.remove(&call.tool_call_id) {
                Slot::Held(prepared)
            } else if is_duplicate {
                Slot::Ready(Some(CallPlan::Synthetic {
                    result: Arc::new(error_result(&call, "duplicate tool_call_id from provider")),
                    reason: None,
                }))
            } else if is_cancelled(control) || stop_is_requested(control.stop_requested.as_ref()) {
                Slot::Ready(Some(CallPlan::Stopped(call.tool_call_id)))
            } else if let Err(error) = &start {
                Slot::Ready(Some(CallPlan::NoDispatch {
                    tool_call_id: call.tool_call_id,
                    reason: NoDispatchReason::Unavailable {
                        detail: error.to_string(),
                    },
                    hooks: None,
                }))
            } else {
                gate(
                    call,
                    step,
                    manifest,
                    approval,
                    hooks,
                    &ctx,
                    plane,
                    writer,
                    history,
                    control.elicitation_plane.is_some(),
                )
                .await?
            };
            slots.push(slot);
        }
        writer.sync().await?;
        let spend =
            crate::keep_warm_wait::batch_spend_iter(warm.as_ref(), step, history.frames.iter());
        let result = crate::keep_warm_wait::wait_tools_with_spend(
            Box::pin(wait(
                &mut slots,
                &ctx,
                step,
                plane,
                writer,
                control,
                hooks,
                history,
                sequential,
                restart,
                resume.provider_held,
            )),
            warm,
            spend,
        )
        .await?;
        if matches!(
            result,
            Some(BatchInterruption::RestartPause | BatchInterruption::ProviderHeldPause)
        ) {
            let open = slots
                .into_iter()
                .filter_map(|slot| match slot {
                    Slot::Held(call) => Some(call),
                    _ => None,
                })
                .collect();
            if announced && result != Some(BatchInterruption::ProviderHeldPause) {
                let _ = plane.batch_finished(&ctx).await;
            }
            return Ok(BatchOutcome {
                results: Vec::new(),
                interruption: result,
                open,
            });
        }
        writer.record(WalRecord::ToolBatchFinished {
            batch_id: ctx.batch_id.clone(),
        });
        let appends = deliver_ordered_with_results(
            writer,
            Some(control),
            run_id,
            slots.iter().filter_map(|slot| match slot {
                Slot::Done(result) => Some(result.result.as_ref()),
                _ => None,
            }),
        );
        writer.record(WalRecord::TurnFinished { step_id: step });
        record_budget_firings(writer, run_id, firings);
        writer.sync().await?;
        if announced {
            let _ = plane.batch_finished(&ctx).await;
        }
        let results: Vec<_> = slots
            .into_iter()
            .map(|slot| match slot {
                Slot::Done(result) => Arc::unwrap_or_clone(result.result),
                _ => unreachable!("wait closes every slot"),
            })
            .collect();
        if emit {
            for result in &results {
                let _ = send(
                    out,
                    OutputEvent::Control(ControlUnit::ToolResult {
                        result: result.clone(),
                    }),
                )
                .await;
            }
        }
        let mut results = results;
        apply_ordered_appends(&mut results, &appends);
        Ok(BatchOutcome {
            results,
            interruption: result,
            open: Vec::new(),
        })
    }

    async fn cancelled(token: Option<&CancellationToken>) {
        match token {
            Some(token) => token.cancelled().await,
            None => std::future::pending().await,
        }
    }

    fn provider_held_can_pause(slots: &[Slot], open_held: usize, active: usize) -> bool {
        open_held > 0
            && active == open_held
            && !slots
                .iter()
                .any(|slot| matches!(slot, Slot::Held(call) if !call.approved))
    }

    #[test]
    fn provider_held_pause_waits_for_approval_and_inflight_siblings() {
        let mut slots = vec![
            Slot::Running,
            Slot::Held(Held {
                key: "key".into(),
                prepared: Arc::new(crate::replay::deferred_tests::prepared("b", "b")),
                id: Some("card".into()),
                approved: false,
                filed: true,
                retry_at: 0,
                backoff: 50,
            }),
        ];
        assert!(
            !provider_held_can_pause(&slots, 1, 1),
            "an unanswered card keeps consent intake live"
        );
        let Slot::Held(call) = &mut slots[1] else {
            unreachable!()
        };
        call.approved = true;
        assert!(
            provider_held_can_pause(&slots, 1, 1),
            "approval behind a sequential hold may wait"
        );
        assert!(
            !provider_held_can_pause(&slots, 1, 2),
            "an in-flight sibling keeps the batch live"
        );
        assert!(
            !provider_held_can_pause(&slots, 0, 0),
            "ordinary approvals do not pause tool_held"
        );
    }

    #[allow(clippy::too_many_arguments)]
    async fn wait<T: ToolPlane>(
        slots: &mut [Slot],
        ctx: &BatchCtx,
        step: StepId,
        plane: &T,
        writer: &WalHandle,
        control: &RunControl,
        hooks: Option<&crate::hooks::HookRuntime<'_>>,
        history: &mut History,
        sequential: bool,
        restart: bool,
        resumed_held: Vec<crate::replay::ProviderHeldCall>,
    ) -> Result<Option<BatchInterruption>, RunError> {
        let recording_plane = ProviderHeldPlane {
            inner: plane,
            writer,
            ctx,
            sequential,
            held: ProviderHeldBatch::new(resumed_held),
            lifecycle: control.provider_held_lifecycle.as_deref(),
            clock: control.clock.as_ref(),
        };
        let plane = &recording_plane;
        let mut jobs: FuturesUnordered<Job<'_>> = FuturesUnordered::new();
        let mut active = HashSet::new();
        let mut dispatched = HashSet::new();
        let mut cursor = None;
        let mut polling: Option<AnswerRead<'_>> = None;
        let mut answer_poll = AnswerPollSchedule::default();
        let clock = control.clock.clone().unwrap_or_default();
        let mut clock_changes = clock.subscribe();
        let mut interruption = None;
        let mut waits = HashMap::<usize, CancellationToken>::new();
        let mut card_ids = HashMap::<usize, Arc<std::sync::Mutex<Option<String>>>>::new();
        let mut approved_calls = HashMap::<usize, Held>::new();
        let mut adoptions = HashMap::<usize, crate::consent::ConsentAnswer>::new();
        let mut feedback: FuturesUnordered<Pin<Box<dyn Future<Output = Feedback> + Send + '_>>> =
            FuturesUnordered::new();
        if slots.iter().any(|slot| matches!(slot, Slot::Held(_))) {
            marker(slots, ctx, writer, history).await?;
        }
        loop {
            let cancelled = is_cancelled(control);
            let stopped = stop_is_requested(control.stop_requested.as_ref());
            if control
                .shutdown_seal
                .as_ref()
                .is_some_and(crate::ShutdownSeal::is_tripped)
            {
                // Once the actor takes the writer, it owns close-out and all later records.
                std::future::pending::<()>().await;
            }
            if cancelled {
                interruption = Some(BatchInterruption::CallerCancel);
            } else if stopped && interruption.is_none() {
                interruption = Some(BatchInterruption::StopRequested);
            }
            let stopping = interruption.is_some();
            if stopping {
                polling = None;
                for token in waits.values() {
                    token.cancel();
                }
            }
            let eligible = slots
                .iter()
                .position(|slot| !matches!(slot, Slot::Done(_)))
                .unwrap_or(slots.len());
            for index in 0..slots.len() {
                // consent.request can return a card id before the AwaitingApproval
                // WAL sync completes. Expiry or cancellation must not lose that id:
                // syncing the closing record also commits the earlier id marker,
                // so later detach or withdrawal can address the existing card.
                if let (Slot::Held(call), Some(saved)) = (&mut slots[index], card_ids.get(&index)) {
                    if let Some(id) = saved.lock().expect("recovery id lock").clone() {
                        call.id = Some(id);
                    }
                }
                if let Slot::Held(call) = &slots[index] {
                    if !stopping && !call.approved && now(control) >= call.expires() {
                        if let Some(token) = waits.remove(&index) {
                            token.cancel();
                        }
                        let held = call.clone();
                        let kind = if call.id.is_some() {
                            DeferredDecision::Expired
                        } else {
                            DeferredDecision::Unavailable
                        };
                        let result = decision(call, kind, None, writer, history)
                            .await?
                            .expect("closing decision");
                        slots[index] = Slot::Done(result);
                        marker(slots, ctx, writer, history).await?;
                        if let (Some(elicitation), Some(id)) =
                            (control.elicitation_plane.as_ref(), held.id.clone())
                        {
                            feedback.push(Box::pin(async move {
                                let answer =
                                    release(elicitation.as_ref(), &held.ctx(ctx), &held, &id).await;
                                Feedback::Released(held, answer)
                            }));
                        }
                        continue;
                    }
                }
                if active.contains(&index) {
                    continue;
                }
                if let Slot::Ready(plan) = &mut slots[index] {
                    if sequential && index != eligible && !stopping {
                        continue;
                    }
                    let mut plan = plan.take().expect("ready plan owned once");
                    if stopping {
                        if let CallPlan::Dispatch(call) = plan {
                            plan = CallPlan::Stopped(call.tool_call_id);
                        }
                    }
                    prepare_ready(&mut plan, ctx, plane, writer).await?;
                    active.insert(index);
                    dispatched.insert(index);
                    slots[index] = Slot::Running;
                    jobs.push(Box::pin(async move {
                        (
                            index,
                            Work::Dispatched(
                                resolve_hooked(
                                    plan,
                                    index,
                                    step,
                                    ctx,
                                    plane,
                                    control.cancel.as_ref(),
                                    control.stop_requested.as_ref(),
                                    hooks,
                                )
                                .await,
                            ),
                        )
                    }));
                    continue;
                }
                let Slot::Held(call) = &mut slots[index] else {
                    continue;
                };
                if stopping {
                    continue;
                }
                if call.approved {
                    if sequential && index != eligible {
                        continue;
                    }
                    let held = call.clone();
                    approved_calls.insert(index, held.clone());
                    active.insert(index);
                    jobs.push(Box::pin(async move {
                        let result = tokio::select! {
                            biased;
                            _ = cancelled_token(control.cancel.as_ref()) => Some(stopped_resolution(&held.call().tool_call_id)),
                            _ = cancelled_token(control.stop_requested.as_ref()) => Some(stopped_resolution(&held.call().tool_call_id)),
                            result = validate(&held, ctx, step, plane, hooks) => result,
                        };
                        (index, Work::Validated(result))
                    }));
                } else if !call.filed && now(control) >= call.retry_at {
                    let Some(elicitation) = control.elicitation_plane.as_ref() else {
                        let result =
                            decision(call, DeferredDecision::Unavailable, None, writer, history)
                                .await?
                                .expect("unavailable closes");
                        slots[index] = Slot::Done(result);
                        marker(slots, ctx, writer, history).await?;
                        continue;
                    };
                    let held = call.clone();
                    let timestamp = now(control);
                    let scan = history.scan(&held.key);
                    let adoption = adoptions.remove(&index);
                    active.insert(index);
                    let wait_cancel = CancellationToken::new();
                    waits.insert(index, wait_cancel.clone());
                    let saved_id = Arc::new(std::sync::Mutex::new(held.id.clone()));
                    card_ids.insert(index, saved_id.clone());
                    jobs.push(Box::pin(async move {
                        let filing_ctx = held.ctx(ctx);
                        let saved = saved_id.clone();
                        let result = tokio::select! {
                            biased;
                            _ = wait_cancel.cancelled() => Err(ApprovalRouteError { code: "stopped".into(), retryable: true }),
                            _ = cancelled_token(control.cancel.as_ref()) => Err(ApprovalRouteError { code: "cancelled".into(), retryable: true }),
                            _ = cancelled_token(control.stop_requested.as_ref()) => Err(ApprovalRouteError { code: "stopped".into(), retryable: true }),
                            result = tokio::time::timeout(Duration::from_millis(held.expires().saturating_sub(timestamp).min(10_000)), async {
                                let durable = |record| {
                                    let saved = saved.clone();
                                    async move {
                                        let id = match &record { WalRecord::AwaitingApproval { pending, .. } => pending[0].elicitation_id.clone(), _ => None };
                                        writer.record(record).into_result().map_err(|_| crate::consent::refusal("wal_write_failed", true))?;
                                        *saved.lock().expect("recovery id lock") = id;
                                        writer.sync().await.map_err(|_| crate::consent::refusal("wal_sync_failed", true))?;
                                        Ok(())
                                    }
                                };
                                if let Some(candidate) = adoption { scan.lock().expect("consent scan progress").seed_candidate(&held.key, &held.prepared, &candidate)?; }
                                if let Some(id) = &held.id {
                                    let pending = scan.lock().expect("consent scan progress").expected();
                                    if let Some(candidate) = pending {
                                        if candidate.id != *id { return crate::consent::reconcile_with_progress(elicitation.as_ref(), &filing_ctx, &held.key, &held.prepared, id, scan.as_ref(), durable).await; }
                                        let answer = crate::consent::verify_readback(elicitation.as_ref(), &filing_ctx, &held.key, &held.prepared, candidate, scan.as_ref()).await?;
                                        return Ok(crate::consent::RecoveredCard { id: id.clone(), answer: Some(answer) });
                                    }
                                    let card = crate::consent::reconcile_with_progress(elicitation.as_ref(), &filing_ctx, &held.key, &held.prepared, id, scan.as_ref(), durable).await?;
                                    if card.answer.is_some() { return Ok(card); }
                                    let question = elicitation.get(&filing_ctx, &card.id).await?;
                                    let answer = check_readback(elicitation.as_ref(), &filing_ctx, &held, question, scan.as_ref()).await?;
                                    Ok(crate::consent::RecoveredCard { id: card.id, answer })
                                } else {
                                    crate::consent::recover_with_progress(elicitation.as_ref(), &filing_ctx, &held.key, &held.prepared, timestamp, scan.as_ref(), durable).await
                                }
                            }) => result.unwrap_or_else(|_| Err(ApprovalRouteError { code: "timeout".into(), retryable: true })),
                        };
                        let id = saved_id.lock().expect("recovery id lock").clone();
                        (index, Work::Filed(result, id))
                    }));
                } else if call.filed && now(control) >= call.retry_at {
                    let Some(elicitation) = control.elicitation_plane.as_ref() else {
                        continue;
                    };
                    let held = call.clone();
                    let scan = history.scan(&held.key);
                    let wait_cancel = CancellationToken::new();
                    waits.insert(index, wait_cancel.clone());
                    active.insert(index);
                    jobs.push(Box::pin(async move {
                        let wait_ms = held.expires().saturating_sub(now(control)).min(120_000);
                        let await_ctx = held.ctx(ctx);
                        let result = tokio::select! {
                            biased;
                            _ = wait_cancel.cancelled() => Err(crate::consent::refusal("stopped", true)),
                            _ = cancelled_token(control.cancel.as_ref()) => Err(crate::consent::refusal("cancelled", true)),
                            _ = cancelled_token(control.stop_requested.as_ref()) => Err(crate::consent::refusal("stopped", true)),
                            result = tokio::time::timeout(Duration::from_millis(wait_ms), async {
                                let pending = scan.lock().expect("consent scan progress").expected();
                                if let Some(candidate) = pending {
                                    let budget = held.expires().saturating_sub(now(control)).min(10_000);
                                    return tokio::time::timeout(Duration::from_millis(budget), crate::consent::verify_readback(elicitation.as_ref(), &await_ctx, &held.key, &held.prepared, candidate, scan.as_ref())).await.map_err(|_| crate::consent::refusal("timeout", true))?.map(Some);
                                }
                                let question = elicitation.r#await(&await_ctx, held.id.as_deref().expect("durable card id"), wait_ms).await?;
                                let budget = held.expires().saturating_sub(now(control)).min(10_000);
                                tokio::time::timeout(Duration::from_millis(budget), check_readback(elicitation.as_ref(), &await_ctx, &held, question, scan.as_ref())).await.map_err(|_| crate::consent::refusal("timeout", true))?
                            }) => result.unwrap_or_else(|_| Err(crate::consent::refusal("timeout", true))),
                        };
                        (index, Work::Awaited(result))
                    }));
                }
            }
            if slots.iter().all(|slot| matches!(slot, Slot::Done(_))) {
                jobs.clear();
                drop(polling.take());
                while let Some(event) = feedback.next().await {
                    finish_feedback(event, ctx, writer, history, control).await?;
                }
                return Ok(interruption);
            }
            if provider_held_can_pause(slots, plane.held.len(), active.len())
                && (!stopping || (stopped && !cancelled))
            {
                // Sequential successors retain intents but must not be sent through a hold.
                for slot in slots.iter_mut() {
                    if let Slot::Ready(Some(plan)) = slot {
                        prepare_ready(plan, ctx, plane, writer).await?;
                    }
                }
                return Ok(Some(BatchInterruption::ProviderHeldPause));
            }
            if stopping && dispatched.is_empty() && active.is_empty() {
                if stopped
                    && !cancelled
                    && restart
                    && interruption == Some(BatchInterruption::StopRequested)
                {
                    marker(slots, ctx, writer, history).await?;
                    return Ok(Some(BatchInterruption::RestartPause));
                }
                let open: Vec<_> = slots
                    .iter()
                    .filter_map(|slot| match slot {
                        Slot::Held(call) => Some(call.clone()),
                        _ => None,
                    })
                    .collect();
                close_open(&open, ctx, writer).await?;
                report_open(&open, ctx, control).await;
                for slot in slots.iter_mut() {
                    if let Slot::Held(call) = slot {
                        *slot = Slot::Done(CallResolution::plain(
                            crate::resume::not_dispatched_result(&call.call().tool_call_id),
                            None,
                        ));
                    }
                }
                marker(slots, ctx, writer, history).await?;
                continue;
            }
            let needs_answers = !stopping
                && control.elicitation_plane.is_some()
                && !history
                    .projection
                    .obligations()
                    .expired_without_late_record
                    .is_empty();
            if polling.is_none() && answer_poll.ready(now(control)) && needs_answers {
                if let Some(elicitation) = control.elicitation_plane.as_ref() {
                    let since = cursor.clone();
                    let snapshot = history.projection.clone();
                    let progress = history.live_scan.clone();
                    polling = Some(Box::pin(async move {
                        read_feed(
                            elicitation.as_ref(),
                            ctx,
                            since,
                            &snapshot,
                            progress.as_ref(),
                        )
                        .await
                    }));
                }
            }
            let timestamp = now(control);
            let mut wake_at = if needs_answers && polling.is_none() {
                answer_poll.next
            } else {
                u64::MAX
            };
            for (index, slot) in slots.iter().enumerate() {
                if let Slot::Held(call) = slot {
                    if stopping {
                        continue;
                    }
                    if !call.approved {
                        wake_at = wake_at.min(call.expires());
                    }
                    if !call.approved && !active.contains(&index) {
                        wake_at = wake_at.min(call.retry_at);
                    }
                }
            }
            #[cfg(all(test, feature = "test-clock"))]
            if let Some(probe) = &control.approval_wait_probe {
                // Acknowledge the durable transition before tests advance the shared clock.
                let _ = probe.entered.send((polling.is_some(), !jobs.is_empty()));
                let _ = probe.release.lock().await.recv().await;
            }
            tokio::select! {
                biased;
                Some((index, work)) = jobs.next() => {
                    active.remove(&index);
                    waits.remove(&index);
                    match work {
                        Work::Dispatched(mut result) => {
                            dispatched.remove(&index);
                            if result.ambiguous { interruption = Some(BatchInterruption::Ambiguous); }
                            if result.stopped && interruption.is_none() { interruption = Some(BatchInterruption::StopRequested); }
                            let value = execution(&result);
                            sync_result(&mut result, ctx, writer, hooks).await?;
                            plane.held.complete(&result.result.tool_call_id);
                            if let (Some(held), Some(elicitation)) = (approved_calls.remove(&index), control.elicitation_plane.as_ref()) {
                                feedback.push(Box::pin(async move { if let Some(id) = &held.id { crate::consent::report(elicitation.as_ref(), &held.ctx(ctx), id, value, None).await; } Feedback::Reported }));
                            }
                            slots[index] = Slot::Done(result);
                        }
                        Work::Filed(result, saved_id) => {
                            if let Slot::Held(call) = &mut slots[index] { if saved_id.is_some() { call.id = saved_id; } }
                            if is_cancelled(control) || stop_is_requested(control.stop_requested.as_ref()) { continue }
                            let Slot::Held(call) = &mut slots[index] else { continue };
                            match result {
                                Ok(reply) => {
                                    call.id = Some(reply.id); call.filed = true; call.backoff = 50;
                                    marker(slots, ctx, writer, history).await?;
                                    if let Some(answer) = reply.answer { intake(slots, index, answer, ctx, writer, control, history).await?; }
                                }
                                Err(error) if error.retryable => { call.retry_at = now(control).saturating_add(call.backoff); call.backoff = (call.backoff * 2).min(1000); }
                                Err(error) => { let held = call.clone(); let result = refusal_decision(call, &error, writer, history).await?; slots[index] = Slot::Done(result); marker(slots, ctx, writer, history).await?;
                                    if let (Some(elicitation), Some(id)) = (control.elicitation_plane.as_ref(), held.id.clone()) { feedback.push(Box::pin(async move { let answer = release(elicitation.as_ref(), &held.ctx(ctx), &held, &id).await; Feedback::Released(held, answer) })); }
                                }
                            }
                        }
                        Work::Awaited(result) => {
                            if stopping || is_cancelled(control) || stop_is_requested(control.stop_requested.as_ref()) { continue; }
                            if adoptions.contains_key(&index) { continue; }
                            let Slot::Held(call) = &mut slots[index] else { continue };
                            if call.approved { continue; }
                            match result {
                                Ok(Some(answer)) => { intake(slots, index, answer, ctx, writer, control, history).await?; }
                                Ok(None) => { retry_await(call, now(control)); }
                                Err(error) if error.retryable => { retry_await(call, now(control)); }
                                Err(error) => { let held = call.clone(); let result = refusal_decision(call, &error, writer, history).await?; slots[index] = Slot::Done(result); marker(slots, ctx, writer, history).await?;
                                    // Invalid answered readbacks release the visible card just like transport refusals.
                                    if let (Some(elicitation), Some(id)) = (control.elicitation_plane.as_ref(), held.id.clone()) { feedback.push(Box::pin(async move { let answer = release(elicitation.as_ref(), &held.ctx(ctx), &held, &id).await; Feedback::Released(held, answer) })); }
                                }
                            }
                        }
                        Work::Validated(result) => {
                            // Stop/cancel wins before the intent. A validation in progress owns no
                            // dispatch and can be dropped without stranding an in-flight tool.
                            if is_cancelled(control) || stop_is_requested(control.stop_requested.as_ref()) { continue }
                            if let Some(mut result) = result {
                                sync_result(&mut result, ctx, writer, hooks).await?;
                                if let (Some(held), Some(elicitation)) = (approved_calls.remove(&index), control.elicitation_plane.as_ref()) { feedback.push(Box::pin(async move { if let Some(id) = &held.id { crate::consent::report(elicitation.as_ref(), &held.ctx(ctx), id, crate::tool_plane::ApprovalExecution::ApprovedNotExecuted, None).await; } Feedback::Reported })); }
                                slots[index] = Slot::Done(result);
                            } else {
                                let Slot::Held(held) = &slots[index] else { unreachable!() };
                                let held = held.clone(); let call = held.call();
                                let WalRecord::ToolCallDeferred { module, schema_pin, args_jcs, .. } = &*held.prepared else { unreachable!() };
                                history.record(writer, WalRecord::ToolDispatchIntent { dispatched_at_ms: None, batch_id: ctx.batch_id.clone(), tool_call_id: call.tool_call_id.clone(), tool_name: Some(call.tool_name.clone()),
                                    args: Arc::new(call.input.clone()), module_id: module.clone(), schema_pin: schema_pin.clone(), hooks: None,
                                    approval_digest: None, call_key: Some(held.key.clone()) });
                                writer.sync().await?;
                                let before_send_cancel = control.cancel.as_ref();
                                let stopped_after_intent = before_send_cancel.is_some_and(CancellationToken::is_cancelled) || stop_is_requested(control.stop_requested.as_ref());
                                let bytes = args_jcs.clone();
                                slots[index] = Slot::Running; active.insert(index); dispatched.insert(index);
                                let mut dispatch = PlannedDispatch::from(call);
                                dispatch.call_key = Some(held.key.clone());
                                dispatch.approved = Some(bytes);
                                let plan = if stopped_after_intent { CallPlan::Stopped(dispatch.tool_call_id.clone()) } else { CallPlan::Dispatch(dispatch) };
                                register_dispatch_plans(plane, &held.ctx(ctx), std::iter::once(&plan)).await?;
                                jobs.push(Box::pin(async move {
                                    let resolution = resolve_hooked(plan, index, step, &held.ctx(ctx), plane, before_send_cancel, control.stop_requested.as_ref(), hooks).await;
                                    (index, Work::Dispatched(resolution))
                                }));
                            }
                        }
                    }
                }
                page = async { match polling.as_mut() { Some(poll) => poll.await, None => std::future::pending().await } } => {
                    polling = None;
                    match &page {
                        Err(error) if error.code == "cursor_incarnation_changed" => { cursor = None; answer_poll = AnswerPollSchedule::default(); }
                        _ => {}
                    }
                    if page.as_ref().is_ok_and(|polled| !polled.page.records.is_empty()) {
                        answer_poll.reset_backoff();
                    }
                    // Even an incomplete or failed feed pass waits before another read.
                    answer_poll.completed(now(control));
                    match page {
                        Err(_) => {}
                        Ok(polled) => {
                            let page = polled.page;
                            let Ok(answers)=read_answers(&page.records, &history.projection) else {continue};
                            // Redundant defense: consent::authorize_candidate also reads every
                            // continuation before minting authorization, independently of live intake.
                            if !polled.complete { continue; }
                            for (key,answer) in answers {
                                let answer = match answer.and_then(|answer| history.completed_feed_candidate(&key, answer, &page.cursor)) {
                                    Ok(answer) => answer,
                                    Err(error) => {
                                        if let Some(index) = slots.iter().position(|slot| matches!(slot, Slot::Held(call) if call.key == key && !call.approved)) {
                                            if let Some(token) = waits.remove(&index) { token.cancel(); }
                                            let Slot::Held(held) = &slots[index] else { unreachable!() }; let held = held.clone();
                                            let result = refusal_decision(&held, &error, writer, history).await?;
                                            slots[index] = Slot::Done(result); marker(slots, ctx, writer, history).await?;
                                            if let (Some(elicitation), Some(id)) = (control.elicitation_plane.as_ref(), held.id.clone()) { feedback.push(Box::pin(async move { let answer = release(elicitation.as_ref(), &held.ctx(ctx), &held, &id).await; Feedback::Released(held, answer) })); }
                                        } else { late_refusal(history, writer, &key, &error).await?; }
                                        continue;
                                    }
                                };
                                if let Some(index) = slots.iter().position(|slot| matches!(slot, Slot::Held(call) if call.key == key && call.filed && !call.approved)) {
                                    if let Some(token) = waits.remove(&index) { token.cancel(); }
                                    let Slot::Held(call) = &mut slots[index] else { unreachable!() };
                                    if call.id.as_deref() != Some(answer.id.as_str()) {
                                        // Before adopting this older answer, withdraw the card
                                        // already recorded for the call. If it was answered first,
                                        // read that answer and require agreement before changing ids.
                                        // This is redundant defense: consent::authorize_candidate
                                        // independently proves the recorded duplicate closed.
                                        call.filed = false; call.retry_at = 0;
                                        adoptions.insert(index, answer);
                                    } else { intake(slots, index, answer, ctx, writer, control, history).await?; }
                                } else { late_answer(history, writer, ctx, control, &key, answer).await?; }
                            }
                            // Intake, including the marker updates, is durable before advancing.
                            writer.sync().await?;
                            cursor = Some(page.cursor);
                        }
                    }
                }
                Some(event) = feedback.next() => { finish_feedback(event, ctx, writer, history, control).await?; }
                _ = cancelled_token(control.cancel.as_ref()), if !stopping => {}
                _ = cancelled_token(control.stop_requested.as_ref()), if !stopping => {}
                _ = clock_changes.changed() => {}
                _ = plane.held.changed.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(wake_at.saturating_sub(timestamp))), if wake_at != u64::MAX => {}
            }
        }
    }
    fn retry_await(call: &mut Held, timestamp: u64) {
        // A provider can fail an await immediately. Repeated failures must not
        // turn the long-held card route into a high-rate RPC polling loop.
        let delay = call.backoff.clamp(50, 1000);
        call.retry_at = timestamp.saturating_add(delay);
        call.backoff = (delay * 2).min(1000);
    }

    #[derive(Default)]
    struct AnswerPollSchedule {
        next: u64,
        delay: u64,
    }

    impl AnswerPollSchedule {
        fn ready(&self, now: u64) -> bool {
            now >= self.next
        }

        fn completed(&mut self, now: u64) {
            // Only expired calls need feed polling; held cards have their own await.
            // Empty/error reads back off from 50 ms to 1 s. The total feed-pass
            // budget is separate, and cancellation and clock changes wake independently.
            let delay = self.delay.max(50);
            self.next = now.saturating_add(delay);
            self.delay = (delay * 2).min(1000);
        }

        fn reset_backoff(&mut self) {
            self.delay = 50;
        }
    }

    #[tokio::test]
    async fn late_feed_poll_count_is_bounded_and_mid_backoff_answer_meets_cap() {
        let (ticks, mut clock) = tokio::sync::mpsc::channel(1);
        let (observed, mut observations) = tokio::sync::mpsc::channel(1);
        let (answer, mut answer_gate) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            let mut schedule = AnswerPollSchedule::default();
            let mut polls = 0;
            let mut pickup = None;
            while let Some(now) = clock.recv().await {
                if schedule.ready(now) {
                    polls += 1;
                    if answer_gate.try_recv().is_ok() {
                        pickup = Some(now);
                    }
                    schedule.completed(now);
                }
                observed.send((polls, pickup)).await.unwrap();
            }
        });
        // Each clock advance is acknowledged before the next one; no scheduler or wall-time
        // assumption decides how many reads the late-answer feed performs.
        for now in 0..=5_000 {
            ticks.send(now).await.unwrap();
            observations.recv().await.unwrap();
        }
        answer.send(()).unwrap();
        let mut last = (0, None);
        for now in 5_001..=10_000 {
            ticks.send(now).await.unwrap();
            last = observations.recv().await.unwrap();
        }
        drop(ticks);
        waiter.await.unwrap();
        let (polls, pickup) = last;
        assert!(
            polls <= 15,
            "late-answer feed performed {polls} reads in 10 seconds"
        );
        let pickup = pickup.expect("answer gate was never read");
        assert!(
            (5_001..=6_001).contains(&pickup),
            "answer missed the 1 s pickup cap: {pickup}"
        );
    }
    async fn cancelled_token(token: Option<&CancellationToken>) {
        cancelled(token).await
    }
    async fn check_readback(
        plane: &dyn crate::tool_plane::ElicitationPlane,
        ctx: &BatchCtx,
        held: &Held,
        question: crate::tool_plane::ApprovalQuestion,
        scan: &std::sync::Mutex<crate::consent::LookupProgress>,
    ) -> Result<Option<crate::consent::ConsentAnswer>, ApprovalRouteError> {
        let answer = crate::consent::answer(&question.record, &held.key, &held.prepared)?;
        if answer
            .as_ref()
            .is_some_and(|answer| held.id.as_deref() != Some(answer.id.as_str()))
        {
            return Err(crate::consent::refusal("consent_card_mismatch", false));
        }
        match answer {
            Some(candidate) if candidate.decision == crate::consent::ConsentDecision::Approved => {
                crate::consent::verify_readback(
                    plane,
                    ctx,
                    &held.key,
                    &held.prepared,
                    candidate,
                    scan,
                )
                .await
                .map(Some)
            }
            answer => Ok(answer),
        }
    }
    fn execution(result: &CallResolution) -> crate::tool_plane::ApprovalExecution {
        use crate::tool_plane::ApprovalExecution;
        match result.outcome {
            Some(DispatchOutcome::Unknown) => ApprovalExecution::OutcomeUnknown,
            Some(DispatchOutcome::Completed) if result.plane_subject => ApprovalExecution::Executed,
            _ => ApprovalExecution::ApprovedNotExecuted,
        }
    }
    async fn release(
        plane: &dyn crate::tool_plane::ElicitationPlane,
        ctx: &BatchCtx,
        held: &Held,
        id: &str,
    ) -> Option<crate::consent::ConsentAnswer> {
        if let Ok(Ok(question)) =
            tokio::time::timeout(Duration::from_secs(1), plane.detach(ctx, id)).await
        {
            if let Ok(Some(answer)) =
                crate::consent::answer(&question.record, &held.key, &held.prepared)
            {
                if answer.id == id {
                    return Some(answer);
                }
            }
        }
        None
    }
    async fn finish_feedback(
        event: Feedback,
        ctx: &BatchCtx,
        writer: &WalHandle,
        history: &mut History,
        control: &RunControl,
    ) -> Result<(), RunError> {
        let Feedback::Released(held, Some(answer)) = event else {
            return Ok(());
        };
        let Some(projected) = history.projection.calls.get(&held.key) else {
            return Ok(());
        };
        let expired = projected.decision == Some(DeferredDecision::Expired);
        if projected.late_result.is_some() {
            return Ok(());
        }
        if answer.decision == crate::consent::ConsentDecision::Approved {
            if expired {
                // Fence non-execution before checking an approving detach readback.
                // A failed or interrupted authorization must not leave late work runnable.
                history.record(
                    writer,
                    WalRecord::LateResult {
                        call_key: held.key.clone(),
                        outcome: LateOutcome::NotSent,
                        result: Arc::new(closed(&held.call(), "detached_not_executed")),
                    },
                );
                writer.sync().await?;
            }
            let authorization =
                match authorize_answer(&held, answer.clone(), ctx, control, history).await {
                    Ok(authorization) => authorization,
                    Err(error) if error.retryable => return Ok(()),
                    Err(error) => return late_refusal(history, writer, &held.key, &error).await,
                };
            if expired && history.projection.calls[&held.key].late_approval.is_none() {
                let WalRecord::ToolCallDeferred { ask, .. } = &*held.prepared else {
                    unreachable!()
                };
                record_late_answer(
                    history,
                    writer,
                    &held.key,
                    authorization,
                    ask.late_execution,
                )
                .await?;
            }
            if let Some(plane) = &control.elicitation_plane {
                crate::consent::report(
                    plane.as_ref(),
                    &held.ctx(ctx),
                    &answer.id,
                    crate::tool_plane::ApprovalExecution::ApprovedNotExecuted,
                    None,
                )
                .await;
            }
        } else if expired {
            late_answer(history, writer, ctx, control, &held.key, answer).await?;
        }
        Ok(())
    }
    async fn report_late(control: &RunControl, ctx: &BatchCtx, call: &Held, outcome: LateOutcome) {
        if let (Some(plane), Some(id)) = (control.elicitation_plane.as_ref(), &call.id) {
            let value = match outcome {
                LateOutcome::Completed => crate::tool_plane::ApprovalExecution::Executed,
                LateOutcome::NotSent => crate::tool_plane::ApprovalExecution::ApprovedNotExecuted,
                LateOutcome::Unknown => crate::tool_plane::ApprovalExecution::OutcomeUnknown,
            };
            crate::consent::report(plane.as_ref(), ctx, id, value, None).await;
        }
    }
    async fn authorize_answer(
        held: &Held,
        candidate: crate::consent::ConsentAnswer,
        ctx: &BatchCtx,
        control: &RunControl,
        history: &mut History,
    ) -> Result<crate::consent::AuthorizedAnswer, ApprovalRouteError> {
        let plane = control
            .elicitation_plane
            .as_ref()
            .ok_or_else(|| crate::consent::refusal("elicitation_unavailable", false))?;
        let scan = history.scan(&held.key);
        let ctx = held.ctx(ctx);
        tokio::select! {
            biased;
            _ = cancelled_token(control.cancel.as_ref()) => Err(crate::consent::refusal("cancelled", true)),
            _ = cancelled_token(control.stop_requested.as_ref()) => Err(crate::consent::refusal("stopped", true)),
            result = tokio::time::timeout(Duration::from_secs(1), crate::consent::authorize_candidate(plane.as_ref(), &ctx, &held.key, &held.prepared, held.id.as_deref(), &candidate.record, scan.as_ref())) => result.unwrap_or_else(|_| Err(crate::consent::refusal("consent_authorization_timeout", true))),
        }
    }

    async fn intake(
        slots: &mut [Slot],
        index: usize,
        answer: crate::consent::ConsentAnswer,
        ctx: &BatchCtx,
        writer: &WalHandle,
        control: &RunControl,
        history: &mut History,
    ) -> Result<(), RunError> {
        let Slot::Held(held) = &slots[index] else {
            return Ok(());
        };
        if held.approved {
            return Ok(());
        }
        let held = held.clone();
        let authorization = match authorize_answer(&held, answer, ctx, control, history).await {
            Ok(authorization) => authorization,
            Err(error) if error.retryable => {
                if let Slot::Held(call) = &mut slots[index] {
                    retry_await(call, now(control));
                }
                return Ok(());
            }
            Err(error) => {
                let result = refusal_decision(&held, &error, writer, history).await?;
                slots[index] = Slot::Done(result);
                marker(slots, ctx, writer, history).await?;
                if let (Some(plane), Some(id)) =
                    (control.elicitation_plane.as_ref(), held.id.as_deref())
                {
                    release(plane.as_ref(), &held.ctx(ctx), &held, id).await;
                }
                return Ok(());
            }
        };
        let answer = authorization.value().clone();
        let Slot::Held(call) = &mut slots[index] else {
            unreachable!()
        };
        call.id = Some(answer.id.clone());
        if now(control) >= call.expires()
            || answer.decision == crate::consent::ConsentDecision::None
        {
            let key = call.key.clone();
            let result = decision(call, DeferredDecision::Expired, None, writer, history)
                .await?
                .expect("expiry closes");
            slots[index] = Slot::Done(result);
            late_answer(history, writer, ctx, control, &key, answer).await?;
        } else {
            let approved = answer.decision == crate::consent::ConsentDecision::Approved;
            let result = if approved {
                approved_decision(call, authorization, writer, history).await?
            } else {
                decision(call, DeferredDecision::Declined, None, writer, history).await?
            };
            if let Some(result) = result {
                slots[index] = Slot::Done(result);
            } else {
                call.approved = true;
            }
        }
        marker(slots, ctx, writer, history).await
    }
    fn dispatch_resolution(id: &ToolCallId, result: DispatchResult) -> CallResolution {
        match result {
            DispatchResult::Completed(result) => {
                CallResolution::plain(result, Some(DispatchOutcome::Completed))
            }
            DispatchResult::NotSent { reason } => {
                let mut result = CallResolution::plain(
                    route_drained_result(id, &reason),
                    Some(DispatchOutcome::Completed),
                );
                result.reason = Some(reason);
                result
            }
            DispatchResult::SentButOutcomeUnknown { reason } => {
                let mut result = CallResolution::plain(
                    crate::resume::outcome_unknown_result(id),
                    Some(DispatchOutcome::Unknown),
                );
                result.reason = Some(reason);
                result.ambiguous = true;
                result
            }
            DispatchResult::ScopeDrained { reason, sent } => {
                let mut result = CallResolution::plain(
                    route_drained_result(id, &reason),
                    Some(if sent {
                        DispatchOutcome::Unknown
                    } else {
                        DispatchOutcome::Completed
                    }),
                );
                result.reason = Some(reason);
                result.ambiguous = true;
                result
            }
        }
    }
    async fn late_answer(
        history: &mut History,
        writer: &WalHandle,
        ctx: &BatchCtx,
        control: &RunControl,
        key: &str,
        answer: crate::consent::ConsentAnswer,
    ) -> Result<(), RunError> {
        // Classify before deduplicating: expiry is an ordinary terminal but can still
        // acquire its one late record. All other terminals discard answers.
        let Some(call) = history.projection.calls.get(key).filter(|call| {
            call.decision == Some(DeferredDecision::Expired) && call.late_approval.is_none()
        }) else {
            return Ok(());
        };
        if call.late_result.is_some() || !history.projection.late_allowed(call) {
            return Ok(());
        }
        let WalRecord::ToolCallDeferred { ask, .. } = &*call.prepared else {
            unreachable!()
        };
        let late_execution = ask.late_execution;
        let held = Held {
            key: key.into(),
            prepared: call.prepared.clone(),
            id: call.elicitation_id.clone(),
            approved: false,
            filed: false,
            retry_at: 0,
            backoff: 50,
        };
        let authorized_late = match authorize_answer(&held, answer, ctx, control, history).await {
            Ok(authorization) => authorization,
            Err(error) if error.retryable => return Ok(()),
            Err(error) => return late_refusal(history, writer, key, &error).await,
        };
        record_late_answer(history, writer, key, authorized_late, late_execution).await
    }

    pub(super) async fn record_late_answer(
        history: &mut History,
        writer: &WalHandle,
        key: &str,
        authorization: crate::consent::AuthorizedAnswer,
        late_execution: LateExecution,
    ) -> Result<(), RunError> {
        let prepared = &history
            .projection
            .calls
            .get(key)
            .ok_or_else(|| RunError::Wal("late approval lacks a prepared call".into()))?
            .prepared;
        if !authorization.matches(key, prepared) {
            return Err(RunError::Wal(
                "late_approval_capability_mismatch: call key or binding differs".into(),
            ));
        }
        let answer = authorization.value();
        history.record(
            writer,
            WalRecord::LateApprovalRecorded {
                call_key: key.into(),
                elicitation_id: answer.id.clone(),
                decision: match answer.decision {
                    crate::consent::ConsentDecision::Approved => LateDecision::Approved,
                    crate::consent::ConsentDecision::Declined => LateDecision::Declined,
                    crate::consent::ConsentDecision::None => LateDecision::None,
                },
                approval_digest: None,
                late_execution: Some(late_execution),
            },
        );
        writer.sync().await?;
        Ok(())
    }

    async fn late_refusal(
        history: &mut History,
        writer: &WalHandle,
        key: &str,
        error: &ApprovalRouteError,
    ) -> Result<(), RunError> {
        let Some(call) = history.projection.calls.get(key).filter(|call| {
            call.late_approval.is_none() && call.decision == Some(DeferredDecision::Expired)
        }) else {
            return Ok(());
        };
        if call.late_result.is_some() || !history.projection.late_allowed(call) {
            return Ok(());
        }
        let WalRecord::ToolCallDeferred { call_id, .. } = &*call.prepared else {
            unreachable!()
        };
        let result = Arc::new(route_drained_result(call_id, &error.code));
        let id = call.elicitation_id.clone().unwrap_or_default();
        history.record(
            writer,
            WalRecord::LateApprovalRecorded {
                call_key: key.into(),
                elicitation_id: id,
                decision: LateDecision::None,
                approval_digest: None,
                late_execution: None,
            },
        );
        history.record(
            writer,
            WalRecord::LateResult {
                call_key: key.into(),
                outcome: LateOutcome::NotSent,
                result,
            },
        );
        writer.sync().await?;
        Ok(())
    }

    async fn close_stopped_late(
        history: &mut History,
        writer: &WalHandle,
        bind: &BindIdentity,
        control: &RunControl,
    ) -> Result<(), RunError> {
        let Some(stopped) = &history.projection.stopped_runs else {
            return Ok(());
        };
        let calls: Vec<_> = history
            .projection
            .calls
            .iter()
            .filter(|(_, call)| {
                crate::replay::DeferredCallProjection::late_open(call)
                    && call
                        .run_id
                        .as_ref()
                        .is_some_and(|run| stopped.contains(&run.0))
            })
            .map(|(key, call)| (key.clone(), call.clone()))
            .collect();
        for (key, projected) in calls {
            let WalRecord::ToolCallDeferred {
                call_id,
                batch_id,
                scope,
                ..
            } = &*projected.prepared
            else {
                unreachable!()
            };
            let sent = projected.late_dispatch_intent.is_some() || history.frames.iter().any(|frame| matches!(&frame.record, WalRecord::ToolDispatchIntent { call_key: Some(anchor), .. } if anchor == &key));
            let outcome = if sent {
                LateOutcome::Unknown
            } else {
                LateOutcome::NotSent
            };
            let result = if sent {
                crate::resume::outcome_unknown_result(call_id)
            } else {
                route_drained_result(call_id, "late_execution_stopped")
            };
            history.record(
                writer,
                WalRecord::LateResult {
                    call_key: key,
                    outcome,
                    result: Arc::new(result),
                },
            );
            writer.sync().await?;
            if matches!(
                projected.late_approval.as_deref(),
                Some(WalRecord::LateApprovalRecorded {
                    decision: LateDecision::Approved,
                    ..
                })
            ) {
                if let (Some(plane), Some(id), Some(run_id)) = (
                    &control.elicitation_plane,
                    &projected.elicitation_id,
                    &projected.run_id,
                ) {
                    let ctx = BatchCtx {
                        batch_id: batch_id.clone(),
                        bind: bind.clone(),
                        run_id: run_id.clone(),
                        scope: Some(scope.clone()),
                    };
                    let value = if sent {
                        crate::tool_plane::ApprovalExecution::OutcomeUnknown
                    } else {
                        crate::tool_plane::ApprovalExecution::ApprovedNotExecuted
                    };
                    crate::consent::report(plane.as_ref(), &ctx, id, value, None).await;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn admission<T: ToolPlane>(
        history: &mut History,
        writer: &WalHandle,
        session: &BindIdentity,
        control: &RunControl,
        plane: &T,
    ) -> Result<(), RunError> {
        if history.projection.stopped_runs.is_none() {
            let head = writer
                .durable_head()
                .load(std::sync::atomic::Ordering::SeqCst);
            let complete = history.frames.first().is_some_and(|frame| frame.seq == 1)
                && history.frames.last().is_some_and(|frame| frame.seq == head)
                && history
                    .frames
                    .iter()
                    .zip(history.frames.iter().skip(1))
                    .all(|(left, right)| left.seq.checked_add(1) == Some(right.seq));
            if !complete {
                return Err(RunError::Wal(
                    "consent_projection_incomplete: stop facts require a complete WAL fold".into(),
                ));
            }
            let mut rebuilt = crate::replay::DeferredCallProjection::new(
                writer.lineage_id().expect("approval lineage"),
            );
            rebuilt.fold_frames(&history.frames.base);
            rebuilt.fold_frames(&history.frames.appended);
            history.projection = rebuilt;
        }
        close_stopped_late(history, writer, session, control).await?;
        if control.elicitation_plane.is_none() {
            history.require_installed_plane(control)?;
        }
        let expired: Vec<_> = history
            .projection
            .calls
            .iter()
            .filter(|(_, call)| {
                history.projection.late_allowed(call)
                    && call.decision == Some(DeferredDecision::Expired)
                    && call.late_approval.is_none()
                    && call.late_result.is_none()
            })
            .map(|(key, call)| (key.clone(), call.clone()))
            .collect();
        for (key, projected) in expired {
            let Some(elicitation) = control.elicitation_plane.as_ref() else {
                break;
            };
            let held = Held {
                key: key.clone(),
                prepared: projected.prepared.clone(),
                id: projected.elicitation_id.clone(),
                approved: false,
                filed: false,
                retry_at: 0,
                backoff: 50,
            };
            let ctx = BatchCtx {
                batch_id: BatchId(format!("late:{key}")),
                bind: session.clone(),
                run_id: projected
                    .run_id
                    .clone()
                    .ok_or_else(|| RunError::Wal("late call lacks originating run".into()))?,
                scope: None,
            };
            let ctx = held.ctx(&ctx);
            let scan = history.scan(&key);
            // Completed heads can gain new settlements. An interrupted traversal,
            // however, must keep its checked prefix across admission budgets.
            scan.lock()
                .expect("late consent scan progress")
                .refresh_completed();
            let read =
                || async {
                    if let Some(id) = &held.id {
                        crate::consent::reconcile_with_progress(
                            elicitation.as_ref(),
                            &ctx,
                            &key,
                            &held.prepared,
                            id,
                            scan.as_ref(),
                            |record| async move {
                                writer.record(record).into_result().map_err(|_| {
                                    crate::consent::refusal("wal_write_failed", true)
                                })?;
                                writer.sync().await.map_err(|_| {
                                    crate::consent::refusal("wal_sync_failed", true)
                                })?;
                                Ok(())
                            },
                        )
                        .await
                        .map(|card| card.answer)
                    } else {
                        crate::consent::lookup_with_progress(
                            elicitation.as_ref(),
                            &ctx,
                            &key,
                            &held.prepared,
                            scan.as_ref(),
                        )
                        .await
                    }
                };
            let mut page = tokio::time::timeout(Duration::from_millis(1000), read()).await;
            // A fresh cursor can also be refused during a provider restart. Never interpret
            // that refusal as a stream head; leave the call eligible for the next admission.
            if matches!(&page, Ok(Err(error)) if error.code == "cursor_incarnation_changed") {
                page = tokio::time::timeout(Duration::from_millis(1000), read()).await;
            }
            match page {
                Ok(Ok(Some(answer))) => {
                    late_answer(history, writer, &ctx, control, &key, answer).await?;
                }
                Ok(Ok(None)) => {}
                Ok(Err(error)) if error.code == "consent_conflicting_decisions" => {
                    late_refusal(history, writer, &key, &error).await?;
                    continue;
                }
                _ => continue,
            }
            if history.projection.calls[&key].late_approval.is_some() {
                continue;
            }
            let Some(id) = held.id else { continue };
            if let Ok(Ok(question)) =
                tokio::time::timeout(Duration::from_millis(1000), elicitation.get(&ctx, &id)).await
            {
                if let Ok(Some(answer)) =
                    crate::consent::answer(&question.record, &key, &held.prepared)
                {
                    if answer.id == id {
                        late_answer(history, writer, &ctx, control, &key, answer).await?;
                    }
                }
            }
        }
        let pending: Vec<_> = history
            .projection
            .calls
            .iter()
            .filter(|(_, call)| {
                matches!(
                    call.late_approval.as_deref(),
                    Some(WalRecord::LateApprovalRecorded {
                        decision: LateDecision::Approved,
                        late_execution: Some(LateExecution::Execute),
                        ..
                    })
                ) && call.late_result.is_none()
                    && history.projection.late_allowed(call)
            })
            .map(|(key, call)| (key.clone(), call.clone()))
            .collect();
        for (key, projected) in pending {
            // Reserved executors remain readable, but only broca-owned calls can
            // be executed by this run loop. Leave other executors' work for their owners.
            if !matches!(
                &*projected.prepared,
                WalRecord::ToolCallDeferred {
                    executor: broca_wal::DeferredExecutor::Broca,
                    ..
                }
            ) {
                continue;
            }
            let call = Held {
                key: key.clone(),
                prepared: projected.prepared.clone(),
                id: projected.elicitation_id.clone(),
                approved: matches!(
                    projected.late_approval.as_deref(),
                    Some(WalRecord::LateApprovalRecorded {
                        decision: LateDecision::Approved,
                        ..
                    })
                ),
                filed: false,
                retry_at: 0,
                backoff: 50,
            };
            let dispatch_exists = projected.late_dispatch_intent.is_some()
                || history.frames.iter().any(|frame| {
                    matches!(&frame.record,
                WalRecord::ToolDispatchIntent { call_key: Some(anchor), .. } if anchor == &key)
                });
            if dispatch_exists {
                history.record(
                    writer,
                    WalRecord::LateResult {
                        call_key: key,
                        outcome: LateOutcome::Unknown,
                        result: Arc::new(crate::resume::outcome_unknown_result(
                            &call.call().tool_call_id,
                        )),
                    },
                );
                writer.sync().await?;
                let ctx = call.ctx(&BatchCtx {
                    batch_id: BatchId(format!("late:{}", call.key)),
                    bind: session.clone(),
                    run_id: projected.run_id.clone().expect("originating run"),
                    scope: None,
                });
                report_late(control, &ctx, &call, LateOutcome::Unknown).await;
                continue;
            }
            if is_cancelled(control) || stop_is_requested(control.stop_requested.as_ref()) {
                break;
            }
            let originating_step = history
                .frames
                .iter()
                .find_map(|frame| match (&*call.prepared, &frame.record) {
                    (
                        WalRecord::ToolCallDeferred { batch_id, .. },
                        WalRecord::ToolBatchStarted {
                            batch_id: started,
                            step_id,
                            ..
                        },
                    ) if batch_id == started => Some(*step_id),
                    _ => None,
                })
                .ok_or_else(|| RunError::Wal("late execution lacks originating batch".into()))?;
            let originating = history
                .frames
                .iter()
                .rev()
                .find_map(|frame| match &frame.record {
                    WalRecord::RunStarted {
                        run_id,
                        config,
                        manifest,
                        ..
                    } if Some(run_id) == projected.run_id.as_ref() => {
                        Some((run_id.clone(), config.clone(), manifest.clone()))
                    }
                    _ => None,
                })
                .ok_or_else(|| RunError::Wal("late execution lacks RunStarted".into()))?;
            let ctx = call.ctx(&BatchCtx {
                batch_id: BatchId(format!("late:{key}")),
                bind: session.clone(),
                run_id: originating.0,
                scope: None,
            });
            let WalRecord::ToolCallDeferred {
                module, args_jcs, ..
            } = &*call.prepared
            else {
                unreachable!()
            };
            let mut tools = (*originating.1.tools).clone();
            for tool in &mut tools {
                if tool.name == call.call().tool_name {
                    tool.module = module.clone();
                }
            }
            let mut manifest = ToolManifest::from_tools(tools);
            let plan = originating
                .2
                .as_deref()
                .map(crate::hooks::HookPlan::from_manifest)
                .transpose()
                .map_err(RunError::Wal)?;
            if let Some(frozen) = originating.2.as_deref() {
                manifest = manifest
                    .with_plan_manifest(frozen)
                    .map_err(|error| RunError::Wal(error.to_string()))?;
            }
            let runtime = plan
                .as_ref()
                .zip(control.hook_plane.as_ref())
                .map(|(plan, adapter)| crate::hooks::HookRuntime {
                    plan,
                    plane: adapter,
                    bind: session,
                    writer,
                    cancel: control.cancel.as_ref(),
                    stop: control.stop_requested.as_ref(),
                    seal: control.shutdown_seal.as_ref(),
                });
            let validation = if !call.approved || (plan.is_some() && runtime.is_none()) {
                Some(CallResolution::plain(
                    closed(&call.call(), "pre_tool_unavailable"),
                    Some(DispatchOutcome::Completed),
                ))
            } else {
                tokio::select! {
                    biased;
                    _ = cancelled_token(control.cancel.as_ref()) => break,
                    _ = cancelled_token(control.stop_requested.as_ref()) => break,
                    result = validate(&call, &ctx, originating_step, plane, runtime.as_ref()) => result,
                }
            };
            // A successful recheck can trip either token in its final poll.
            // Do not rely on the select arms that ran before that poll.
            if is_cancelled(control) || stop_is_requested(control.stop_requested.as_ref()) {
                break;
            }
            if let Some(mut result) = validation {
                // LateResult has no reason field, so retain the hook refusal code
                // in its provider-valid error result rather than losing it at the boundary.
                if let (Some("pre_tool_denied"), ToolOutput::Error { message }) = (
                    result.reason.as_deref(),
                    &mut Arc::make_mut(&mut result.result).output,
                ) {
                    *message = format!("pre_tool_denied: {message}");
                }
                history.record(
                    writer,
                    WalRecord::LateResult {
                        call_key: key,
                        outcome: LateOutcome::NotSent,
                        result: result.result,
                    },
                );
                writer.sync().await?;
                report_late(control, &ctx, &call, LateOutcome::NotSent).await;
                continue;
            }
            history.record(
                writer,
                WalRecord::LateDispatchIntent {
                    call_key: key.clone(),
                    // Keep the required approval_digest field for WAL schema compatibility.
                    // Generic consent issues no tool authorization token to put in it,
                    // so write an empty string, not a synthetic digest.
                    approval_digest: String::new(),
                    batch_id: ctx.batch_id.clone(),
                },
            );
            writer.sync().await?;
            let tool_call = call.call();
            let result =
                send_late(plane, &ctx, &tool_call, &manifest, args_jcs, &key, control).await;
            let outcome = match &result {
                DispatchResult::Completed(_) => LateOutcome::Completed,
                DispatchResult::NotSent { .. }
                | DispatchResult::ScopeDrained { sent: false, .. } => LateOutcome::NotSent,
                _ => LateOutcome::Unknown,
            };
            let resolution = dispatch_resolution(&tool_call.tool_call_id, result);
            history.record(
                writer,
                WalRecord::LateResult {
                    call_key: key,
                    outcome,
                    result: resolution.result,
                },
            );
            writer.sync().await?;
            report_late(control, &ctx, &call, outcome).await;
            let _ = plane.batch_finished(&ctx).await;
        }
        Ok(())
    }

    async fn send_late<T: ToolPlane>(
        plane: &T,
        ctx: &BatchCtx,
        call: &ToolCall,
        manifest: &ToolManifest,
        args_jcs: &str,
        key: &str,
        control: &RunControl,
    ) -> DispatchResult {
        // Stop may arrive while the intent sync or route open is pending. Neither
        // completed await is permission to send without checking both tokens again.
        if late_stopped(control) {
            return DispatchResult::NotSent {
                reason: "late_execution_stopped".into(),
            };
        }
        if let Err(error) = plane
            .batch_started(ctx, std::slice::from_ref(call), manifest)
            .await
        {
            return DispatchResult::NotSent {
                reason: error.to_string(),
            };
        }
        if late_stopped(control) {
            return DispatchResult::NotSent {
                reason: "late_execution_stopped".into(),
            };
        }
        if let Err(error) = plane
            .register_dispatch_group(
                ctx,
                &[crate::tool_plane::DispatchRegistration {
                    tool_call_id: call.tool_call_id.clone(),
                    call_key: key.into(),
                    approved: true,
                }],
            )
            .await
        {
            return DispatchResult::NotSent {
                reason: error.to_string(),
            };
        }
        plane
            .dispatch_approved(
                ctx,
                call,
                args_jcs,
                key,
                &crate::tool_plane::DispatchSignals::new(
                    control.cancel.as_ref(),
                    control.stop_requested.as_ref(),
                ),
            )
            .await
    }
    fn late_stopped(control: &RunControl) -> bool {
        is_cancelled(control) || stop_is_requested(control.stop_requested.as_ref())
    }

    // Validate every relevant answer before accepting the page, so an unreadable
    // record cannot advance the feed cursor past an answer that still needs recovery.
    fn read_answers(
        records: &[serde_json::Value],
        projection: &crate::replay::DeferredCallProjection,
    ) -> Result<FeedDecisions, ()> {
        let mut answers: std::collections::BTreeMap<
            String,
            Result<crate::consent::ConsentAnswer, ApprovalRouteError>,
        > = std::collections::BTreeMap::new();
        for record in records {
            let Some(key) = record
                .get("requester_ref")
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            let Some(call) = projection.calls.get(key) else {
                continue;
            };
            if call
                .decision
                .is_some_and(|decision| decision != DeferredDecision::Expired)
            {
                continue;
            }
            if call.late_result.is_some() {
                continue;
            }
            match crate::consent::answer(record, key, &call.prepared) {
                Ok(Some(answer)) => match answers.entry(key.into()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(Ok(answer));
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        if entry
                            .get()
                            .as_ref()
                            .is_ok_and(|old| old.decision != answer.decision)
                        {
                            let _ = entry.insert(Err(crate::consent::refusal(
                                "consent_conflicting_decisions",
                                false,
                            )));
                        }
                    }
                },
                Ok(None) => {}
                Err(_) => return Err(()),
            }
        }
        Ok(answers.into_iter().collect())
    }

    // Do not authorize an answer from a prefix: a later continuation can contain
    // a contradictory answer for the same key. The caller advances its cursor only
    // after this complete, binding-checked snapshot has been durably reduced.
    #[cfg(test)]
    pub(super) async fn scan_live_for_test(
        plane: &dyn crate::tool_plane::ElicitationPlane,
        ctx: &BatchCtx,
        history: &mut History,
    ) -> Result<bool, ApprovalRouteError> {
        read_feed(
            plane,
            ctx,
            None,
            &history.projection,
            history.live_scan.as_ref(),
        )
        .await
        .map(|page| page.complete)
    }
    // Read owner-feed pages for at most one second or 32 pages per invocation.
    // A prefix cannot authorize: only an empty page proves the snapshot complete.
    // Keep its checked decisions and next cursor across budget retries, rather
    // than discarding earlier answers or restarting a healthy long feed at zero.
    async fn read_feed(
        plane: &dyn crate::tool_plane::ElicitationPlane,
        ctx: &BatchCtx,
        cursor: Option<crate::tool_plane::ApprovalCursor>,
        projection: &crate::replay::DeferredCallProjection,
        progress: &std::sync::Mutex<FeedProgress>,
    ) -> Result<PolledPage, ApprovalRouteError> {
        {
            let mut state = progress.lock().expect("live consent scan");
            if !state.initialized {
                state.cursor = cursor;
                state.initialized = true;
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        for _ in 0..32 {
            let cursor = progress.lock().expect("live consent scan").cursor.clone();
            let page = match tokio::time::timeout_at(deadline, plane.answers(ctx, cursor.as_ref()))
                .await
            {
                Err(_) => {
                    return Err(crate::consent::refusal(
                        "consent_feed_budget_exhausted",
                        true,
                    ))
                }
                Ok(Err(error)) => {
                    if error.code == "cursor_incarnation_changed" {
                        *progress.lock().expect("live consent scan") = FeedProgress::default();
                    }
                    return Err(error);
                }
                Ok(Ok(page)) => page,
            };
            let readable = read_answers(&page.records, projection).is_ok();
            let head = page.records.is_empty();
            if !readable {
                let mut records = progress.lock().expect("live consent scan").records.clone();
                records.extend(page.records);
                return Ok(PolledPage {
                    page: ApprovalPage {
                        records,
                        cursor: page.cursor,
                    },
                    complete: false,
                });
            }
            let mut state = progress.lock().expect("live consent scan");
            for record in page.records {
                let Some(key) = record
                    .get("requester_ref")
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let Some(call) = projection.calls.get(key) else {
                    continue;
                };
                if call.late_result.is_some()
                    || call
                        .decision
                        .is_some_and(|kind| kind != DeferredDecision::Expired)
                {
                    continue;
                }
                if let Ok(Some(answer)) = crate::consent::answer(&record, key, &call.prepared) {
                    let decision = match answer.decision {
                        crate::consent::ConsentDecision::Approved => 0,
                        crate::consent::ConsentDecision::Declined => 1,
                        crate::consent::ConsentDecision::None => 2,
                    };
                    if state.seen.insert((key.into(), decision)) {
                        state.records.push(record);
                    }
                }
            }
            if head {
                let finished = std::mem::take(&mut *state);
                return Ok(PolledPage {
                    page: ApprovalPage {
                        records: finished.records,
                        cursor: page.cursor,
                    },
                    complete: true,
                });
            }
            if cursor.as_ref() == Some(&page.cursor) {
                return Err(crate::consent::refusal("consent_cursor_stalled", true));
            }
            state.cursor = Some(page.cursor);
        }
        tokio::task::yield_now().await;
        Err(crate::consent::refusal(
            "consent_feed_budget_exhausted",
            true,
        ))
    }
}

#[allow(clippy::too_many_arguments)]
async fn pause_run(
    writer: &WalHandle,
    out: &mpsc::Sender<OutputEvent>,
    control: &RunControl,
    run_id: RunId,
    reason: PauseReason,
    error: Option<ProviderError>,
    steps: u64,
    usage: Usage,
    retries: u32,
) -> Result<RunSummary, RunError> {
    pause_run_impl(
        writer, out, control, run_id, reason, error, steps, usage, retries, None,
    )
    .await
}
#[allow(clippy::too_many_arguments)]
async fn pause_approval(
    writer: &WalHandle,
    out: &mpsc::Sender<OutputEvent>,
    control: &RunControl,
    session: &BindIdentity,
    run: &RunId,
    steps: u64,
    usage: Usage,
    retries: u32,
    open: &[approval_gate::Held],
    provider_held: bool,
) -> Result<RunSummary, RunError> {
    let batch_id = match open.first().map(|call| &*call.prepared) {
        Some(WalRecord::ToolCallDeferred { batch_id, .. }) => batch_id.clone(),
        _ if provider_held => BatchId(format!("{}-b{}", run.0, steps)),
        _ => return Err(RunError::Wal("approval pause lacks open calls".into())),
    };
    let ctx = BatchCtx {
        batch_id,
        bind: session.clone(),
        run_id: run.clone(),
        scope: None,
    };
    pause_run_impl(
        writer,
        out,
        control,
        run.clone(),
        if provider_held {
            PauseReason::ToolHeld
        } else {
            PauseReason::Restart
        },
        None,
        steps,
        usage,
        retries,
        Some((open, &ctx)),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_batch_controlled<T: ToolPlane, A: ApprovalPolicy>(
    run_id: &RunId,
    step: StepId,
    session: &BindIdentity,
    scope: Option<&broca_protocol::SessionScope>,
    calls: Vec<ToolCall>,
    manifest: &ToolManifest,
    plane: &T,
    approval: &A,
    writer: &WalHandle,
    resume: Option<ResumeCtx>,
    firings: &[BudgetFiring],
    emit: bool,
    stop: Option<&CancellationToken>,
    hooks: Option<&crate::hooks::HookRuntime<'_>>,
    warm: Option<crate::keep_warm_wait::WarmWait<'_>>,
    out: &mpsc::Sender<OutputEvent>,
    control: &RunControl,
    history: Option<&mut approval_gate::History>,
    restart: bool,
) -> Result<BatchOutcome, RunError> {
    if let Some(history) = history {
        Box::pin(approval_gate::batch(
            run_id, step, session, scope, calls, manifest, plane, approval, writer, resume,
            firings, emit, control, hooks, warm, out, history, restart,
        ))
        .await
    } else {
        Box::pin(dispatch_batch(
            run_id,
            step,
            session,
            scope,
            calls,
            manifest,
            plane,
            approval,
            writer,
            resume,
            firings,
            emit,
            stop,
            hooks,
            warm,
            out,
            Some(control),
        ))
        .await
    }
}

#[cfg(test)]
#[path = "run/approval_tests.rs"]
mod approval_tests;
