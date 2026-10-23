//! The lineage actor: a single-task command loop owning one session's lease + WAL for
//! the session's lifetime.
//!
//! All ops on an open session are COMMANDS to this task, processed ONE AT A TIME — so
//! there is no intra-actor lock contention and no interleaving WITHIN the actor (the
//! concurrency is purely at the registry admission boundary + the per-subscriber fan-out
//! tasks). The actor runs at most ONE agentic run at a time (the
//! one-active-run-per-lineage invariant the lease enforces); a `Send` while a run is
//! active is durably queued, a `Send` while idle starts a run.
//!
//! One exception to one-at-a-time: while a restart-paused episode waits for its frozen
//! dependencies during admission, the actor keeps serving commands from inside that suspended
//! admission so status, cancel and new sends stay responsive. A nested command's exit request
//! or a writer poisoned under it ends that admission early (see `start_run_with_cancel`).
//!
//! Closing replies `Reopen` to buffered commands so callers retry through the registry,
//! then flushes and rechecks pending submissions, abandoning close if work remains.
//! It cancels and joins subscribers, shuts down the WAL writer, releases the lease,
//! and marks the registry Closed, in that order. Releasing the lease last prevents a
//! replacement actor from writing while the old writer or subscriber routes still live.
//! See `docs/lineage-actor-coordinator.md` §1d.

use broca_diagnostics::{spawn_blocking, JobKind};

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use broca_core::{
    send_identity_difference_with_conversation_key as send_identity_difference,
    send_identity_digest_with_conversation_key as send_identity_digest, submission_difference,
    submission_identity_digest, EpisodeDisposition, LeaseHandle, OutputEvent, PendingSubmission,
    PriorWalSnapshot, ReconciledRunState, RunControl, RunError, RunIndex, RunIndexRow,
    RunIndexState, RunRequest, RunSummary, SessionStore, WalHandle, WalStore,
};
use broca_protocol::{request::SubmissionRequest, ErrorClass, ProviderError, RunId};
use broca_wal::{QueuedAdmissionFailure, WalRecord};
use cortexkit_role_llm_runner::send::{delivered_as, Delivered};
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::executor::{RunExecutor, RunJob, RunKind};
use crate::registry::{ActorHandle, CloseToken, LineageKey, RegistryHandle};

/// How long an idle actor (no run, no pending work, no subscribers) holds the lease
/// before releasing it. Keeps the lease bounded AND exercises the reopen/close races in
/// v1 (a close path that never runs is a close path that rots).
pub const IDLE_TTL: Duration = Duration::from_secs(30);

/// How long the close teardown waits for the executor's `on_session_close` hook (a
/// best-effort wire send to free a tool-route channel) before proceeding with the close
/// anyway. Bounds external I/O so a stalled send can't hold the registry slot + lease open.
const ON_SESSION_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Deadline for the Interrupted seal write (WAL barriers, replay and index update), measured
/// from entry to shutdown sealing. Attempt handover and the aborted run's join spend this
/// time too, but the join itself is not timed out. Child teardown and the writer join retain
/// the slot and lease until complete; only the registry's total deadline bounds that wait.
const SHUTDOWN_SEAL_BOUND: Duration = Duration::from_secs(5);

/// How long the seal waits for the provider transport to hand over the record of the attempt
/// it had in flight. The transport does so without any network wait the moment it sees the
/// stop signal, so this is only reached when that task is starved or the provider ignores the
/// signal; the record is then lost, and the seal still has the rest of its bound for the
/// terminal.
const SEALED_ATTEMPT_HANDOVER_BOUND: Duration = Duration::from_secs(1);

/// How many attempt records the latest run of `step` already has: those after the last
/// `StepStarted` for it, because a step interrupted in one episode runs again under the same
/// id in the next, and its attempt ordinals restart at zero there.
fn attempts_recorded_for_latest_step(records: &[WalRecord], step: broca_protocol::StepId) -> u32 {
    let step_start = records
        .iter()
        .rposition(|r| matches!(r, WalRecord::StepStarted { step_id } if *step_id == step))
        .map_or(0, |at| at + 1);
    records[step_start..]
        .iter()
        .filter(
            |r| matches!(r, WalRecord::ModelAttemptFinished { step_id, .. } if *step_id == step),
        )
        .count() as u32
}

const STARTUP_RECOVERY_INITIAL_BACKOFF: Duration = Duration::from_millis(25);
const STARTUP_RECOVERY_MAX_BACKOFF: Duration = Duration::from_secs(1);
const STARTUP_RECOVERY_RETRY_BUDGET: Duration = Duration::from_secs(30);

/// How a send's run id was chosen. That decides whether a retry can repeat the run id, and what
/// the WAL records as the send's key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendKey {
    /// The server minted a unique run id. The send can never repeat, so it pays for no identity
    /// digest and no lookup.
    Minted,
    /// An internal caller chose a stable run id. A retry with the same id is deduplicated, but
    /// there is no caller `send_id` to record.
    RunId,
    /// The run id was derived from the caller's `send_id`. A retry with the same key is
    /// deduplicated, and the key is written on the WAL record that carries the message.
    SendId(String),
}

impl SendKey {
    /// Map the older boolean form: `true` means the caller chose a stable run id.
    pub fn from_keyed(idempotency_keyed: bool) -> Self {
        if idempotency_keyed {
            Self::RunId
        } else {
            Self::Minted
        }
    }

    /// Whether a retry can repeat this send's run id, and so must be deduplicated.
    pub fn is_keyed(&self) -> bool {
        !matches!(self, Self::Minted)
    }

    /// The caller's `send_id`, when the run id came from one.
    pub fn send_id(&self) -> Option<&str> {
        match self {
            Self::SendId(send_id) => Some(send_id),
            Self::Minted | Self::RunId => None,
        }
    }
}

/// The fetch plan a `session.send` carried, as the serve layer checked it.
#[derive(Debug, Clone)]
pub struct SendPlan {
    /// The plan's identity: SHA-256 of its RFC 8785 bytes.
    pub identity: String,
    /// The manifest admission built from the plan's fetches. Present only when the session had
    /// no episode when the serve layer checked it; the actor freezes it only if the session
    /// still has none when the send is admitted.
    pub manifest: Option<Arc<broca_wal::PlanManifest>>,
    /// The checked plan, present when the send left its model to the session. The serve layer
    /// cannot know that model, so the actor checks the plan's model-bearing items against the
    /// session's frozen model once it is resolved, refusing `plan_model_mismatch` there.
    pub model_check: Option<Arc<broca_fetch_plan::Plan>>,
}

/// A command to the actor. Each carries a reply channel; the actor NEVER silently drops a
/// command with a live reply (on close it replies `Reopen` so the caller re-routes).
pub enum LineageCmd {
    /// Pause dispatch while the cut holds its actor critical section. The reply
    /// carries a release channel whose drop also unblocks a cancelled operation.
    CutLock {
        reply: oneshot::Sender<(Option<String>, oneshot::Sender<()>)>,
    },
    RepairRetention {
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Boot recovery after the serving connection and its dependency routes are registered.
    ResumeRestart {
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Start a run now (idle) or durably queue it (active). The submission payload stays
    /// boxed because it is much larger than the other command variants.
    Send {
        session_class: Option<String>,
        retention: Option<u64>,
        stop_sequences_named: bool,
        conversation_key: Option<String>,
        /// The caller's informational title, recorded on the record the send writes.
        title: Option<String>,
        run_id: RunId,
        request: Box<SubmissionRequest>,
        kind: RunKind,
        key: SendKey,
        skip_materialization: bool,
        /// Queue behind an active run (the default), steer into it, or interrupt it.
        delivery: broca_protocol::Delivery,
        /// The fetch plan the send carried, if any. Boxed so this command stays small enough to
        /// hand back whole in the registry's `Err` when the actor's channel is closed.
        plan: Option<Box<SendPlan>>,
        /// The send's `delivery_order` group, when it uses one (an ordered `delivery` list, a
        /// `dedupe_key` or an expiry). Such a send is always queued; see
        /// `broca_core::delivery_order`.
        order: Option<Box<broca_protocol::DeliveryOrder>>,
        reply: oneshot::Sender<SendOutcome>,
    },
    /// Durably import a provider-free synthetic episode on the session writer.
    Import {
        request: Box<broca_core::ImportRequest>,
        reply: oneshot::Sender<ImportOutcome>,
    },
    /// Cancel a run by id. A boundary request preserves the in-flight step and tool batch.
    Cancel {
        run_id: RunId,
        at: broca_wal::record::RunCancelAt,
        reply: oneshot::Sender<CancelOutcome>,
    },
    /// Retract a still-pending queued submission (durable tombstone). Replies the outcome.
    Retract {
        submission_id: broca_protocol::SubmissionId,
        reply: oneshot::Sender<RetractOutcome>,
    },
    /// Report the actor's current status.
    Status {
        reply: oneshot::Sender<LineageStatus>,
    },
    /// Resolve a SPECIFIC run_id's lifecycle state (the three-tier resolution: live actor →
    /// durable index terminal → WAL reconciliation for a stale-Active/miss). `None` = the
    /// run_id is unknown to this session.
    RunState {
        run_id: RunId,
        reply: oneshot::Sender<Result<Option<ReconciledRunState>, String>>,
    },
    /// Attach a control-stream subscriber from `from`. The actor runs the small
    /// register-(R) + barrier-(H) step and spawns the off-actor replay task; `reply`
    /// acks once attached (so the caller knows the barrier was taken).
    Subscribe {
        from: crate::fanout::FromCursor,
        /// Resolve `from` against the attach barrier's durable head instead of replaying history.
        live: bool,
        sink: mpsc::Sender<crate::fanout::Delivery>,
        reply: oneshot::Sender<SubscribeOutcome>,
    },
    /// Warm the session's prompt cache by re-sending its most recent model request shaped to
    /// generate nothing. `lineage_id`, when given, must name the session's current lineage.
    Warm {
        lineage_id: Option<String>,
        reply: oneshot::Sender<WarmOutcome>,
    },
    /// Internal: a warm's task ended (sent on the terminal lane, like `RunFinished`).
    WarmFinished {
        warm: Box<(
            PendingWarm,
            Result<broca_core::WarmSent, crate::executor::WarmError>,
        )>,
    },
    /// The idle-TTL timer fired — consider closing.
    IdleTick,
    /// A durable compaction view/notice or effective model-window change may unblock the
    /// pending head. Refresh its admission snapshot before trying the drain again.
    CompactionChanged {
        reply: oneshot::Sender<Result<(), String>>,
    },
    ApplyCompaction {
        ready: broca_core::compaction_provider::notice::ViewReady,
        reply: oneshot::Sender<Result<bool, String>>,
    },
    /// Test-only: report the entry count and serialized size of the admission identity cache.
    #[cfg(test)]
    RetainedAdmissionIdentityState {
        reply: oneshot::Sender<(usize, usize)>,
    },
    /// Test-only: panic inside the actor task (exercises `ActorExitGuard`).
    #[cfg(test)]
    PanicInActor,
    /// Internal: the active run finished (the run task tells the actor).
    RunFinished {
        run_id: RunId,
        attempt_id: u64,
        result: Box<Result<RunSummary, broca_core::RunError>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ImportOutcome {
    SessionMoving { cut_id: String },
    SessionMoved { holder: String },
    Imported(broca_core::ImportOutcome),
    Busy,
    Reopen,
    Error(broca_core::ImportError),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SendOutcome {
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: String,
    },
    RetentionInvalid(crate::retention::InvalidRetention),
    SessionClassInvalid(String),
    InputTokenBudgetInvalid(String),
    ContextCeiling(broca_protocol::ContextCeilingDetail),
    /// The send's `structured_output` was refused before anything was written; `field` is the
    /// offending member path the role contract names.
    StructuredOutputInvalid {
        field: String,
    },
    VariantUnsupported(broca_core::run::VariantRefusal),
    /// The run started immediately (the lineage was idle).
    Active {
        run_id: RunId,
        /// On a retried steer only: its delivery receipt, naming the run whose model step first
        /// rendered the steered message, once the WAL shows one has (see `steer_delivery`).
        /// `None` on every other send.
        delivered: Option<Delivered>,
    },
    /// A retry with the same send identity and payload names a run whose finish reason
    /// is already recorded in the durable run index or WAL.
    Finished {
        run_id: RunId,
        reason: broca_protocol::RunFinishReason,
        /// On a retried steer only: its delivery receipt, as on `Active`. `None` on every other
        /// send.
        delivered: Option<Delivered>,
    },
    /// A run is active; this prompt was durably queued. Carries the assigned
    /// `submission_id` so the caller can later retract it.
    Pending {
        submission_id: broca_protocol::SubmissionId,
    },
    /// The actor is closing — re-route through the registry (re-open).
    Reopen,
    /// A deterministic send identity names a queued submission that was durably retracted.
    Retracted {
        submission_id: broca_protocol::SubmissionId,
    },
    /// A deterministic send identity was reused for a request whose frozen payload differs.
    SendIdReuse {
        differing: &'static str,
    },
    /// A joining steer named a setting the active run cannot change. Nothing was written.
    SteerSettingsConflict {
        field: &'static str,
    },
    /// Starting/queuing failed durably with a permanent error.
    Error(String),
    /// An admission failure whose typed cause determines the caller's remedy.
    ClassifiedError {
        reason: String,
        class: broca_protocol::ErrorClass,
    },
    /// Admission refused the send because the session's WAL holds a frame that requires WAL
    /// features this build does not know (`missing`). Nothing was written. `reason` is the
    /// text [`SendOutcome::Error`] would have carried for the same refusal.
    RetainedSessionTooLarge {
        max_bytes: u64,
    },
    SessionTooLarge {
        max_bytes: u64,
    },
    NewerWriter {
        missing: Vec<String>,
        reason: String,
    },
    /// Admission failed before any durable write because a dependency is temporarily unavailable.
    Transient(String),
    /// Admission was denied by a matching durable cap; no WAL record was written.
    BudgetDenied {
        cap_id: String,
        reason: String,
    },
    /// The routing module refused to choose a provider account; nothing was written.
    AccountSelectionRefused(broca_core::AccountSelectionRefusal),
    /// The owner check refused the send (a different identity, no stamp on a scoped session,
    /// an adoption the session's history does not allow, or a daemon without scopes). Nothing
    /// was written, and the same send meets the same refusal again.
    ScopeRefused(broca_core::ScopeRefusal),
    /// The send would freeze or adopt its stamped scope, but the session is busy: a run is
    /// active or submissions are queued, and a queued send cannot freeze a scope because its
    /// episode starts later. Nothing was written; the caller retries once the session is idle.
    ScopeFreezeBusy {
        reason: ScopeFreezeBusyReason,
        run_id: Option<RunId>,
    },
    /// A new message (not the paused run's own retry, and not an interrupt) arrived while the
    /// session's last run is paused: there is no running turn to join. Nothing was written.
    Paused {
        run_id: RunId,
        reason: broca_protocol::PauseReason,
        /// As on `Active`: on a retried ordered send that was delivered into the paused run, the
        /// receipt naming that run. `None` on every other send.
        delivered: Option<Delivered>,
    },
    RunPaused {
        run_id: RunId,
        reason: broca_protocol::PauseReason,
    },
    /// The send's fetch plan, or a member it carried beside a frozen plan, was refused. Nothing
    /// was written.
    PlanRefused(broca_fetch_plan::Refusal),
    /// A send using the `delivery_order` group was refused before anything was written: `code`
    /// names why (`sender_not_permitted`, `idle_start_not_granted`, or `invalid_params` for a
    /// list that can never be delivered or an expiry already past, with `field` naming it).
    DeliveryRefused {
        code: &'static str,
        field: &'static str,
        message: String,
    },
    /// A template send whose `part_tag` slot names a part the transcript does not show with a
    /// number, refused `template_part_gone` before anything was written. `slot` names the slot.
    TemplatePartGone {
        slot: String,
    },
    /// A retry of an ordered send that ended undelivered: replaced by a newer send under its
    /// dedupe key, or expired.
    SendEnded {
        submission_id: broca_protocol::SubmissionId,
        reason: broca_wal::record::SendEndReason,
    },
    /// An input hook refused a steer or queued send before any durable write or interrupt.
    PreUserUnavailable {
        provider: String,
    },
}

/// Why a send that would freeze or adopt a scope has to wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeFreezeBusyReason {
    RunActive,
    QueueNonempty,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubscribeOutcome {
    /// Attached: the barrier was taken and the off-actor replay task started.
    Attached,
    /// The actor is closing — re-route through the registry (re-open).
    Reopen,
    /// Attaching failed (a WAL barrier error while taking the durable head seq).
    Error(String),
}

/// What a warm's completion needs to record it, snapshotted before its task starts.
pub struct PendingWarmParts {
    /// The run whose request the warm re-sends.
    run_id: RunId,
    step_id: broca_protocol::StepId,
    request_body_sha256: String,
    /// The session's declared cache lifetime.
    ttl_ms: u64,
}

/// A warm in flight: what records it, and where its caller waits for the outcome.
pub struct PendingWarm {
    permit: crate::keep_warm::WarmPermit,
    reply: oneshot::Sender<WarmOutcome>,
    run_id: RunId,
    step_id: broca_protocol::StepId,
    request_body_sha256: String,
    ttl_ms: u64,
}

impl PendingWarm {
    fn new(
        parts: PendingWarmParts,
        reply: oneshot::Sender<WarmOutcome>,
        permit: crate::keep_warm::WarmPermit,
    ) -> Self {
        let PendingWarmParts {
            run_id,
            step_id,
            request_body_sha256,
            ttl_ms,
        } = parts;
        Self {
            permit,
            reply,
            run_id,
            step_id,
            request_body_sha256,
            ttl_ms,
        }
    }
}

/// How a `session.warm` ended.
#[derive(Debug, Clone, PartialEq)]
pub enum WarmOutcome {
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: String,
    },
    /// The provider answered the warm; its record is in the WAL.
    Warmed(broca_wire::WarmReply),
    /// Refused with a typed body. Every refusal but `warm_failed` sent nothing.
    Refused(broca_wire::WarmRefusal),
    /// The actor is closing — re-route through the registry (re-open).
    Reopen,
    /// Nothing was sent: a dependency failed and a retry may succeed.
    Transient(String),
    /// Nothing was sent: the session's WAL could not be read or written.
    Error(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CancelOutcome {
    Error(String),
    SessionMoving { cut_id: String },
    SessionMoved { holder: String },
    Ack,
    NotActive,
    Reopen,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RetractOutcome {
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: String,
    },
    /// The submission was pending and is now retracted (a durable tombstone was written).
    Retracted,
    /// The submission was already consumed into a run — too late to retract.
    AlreadyStarted,
    /// No such pending submission (unknown id, or already retracted).
    NotPending,
    /// The actor is closing — re-route through the registry (re-open).
    Reopen,
    SessionTooLarge {
        max_bytes: u64,
    },
    RetainedSessionTooLarge {
        max_bytes: u64,
    },
    /// Writing the retract tombstone failed durably.
    Error(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LineageStatus {
    Idle,
    Active { run_id: RunId, step: u64 },
    Closing,
}

/// Terminal index rows retain metadata even after WAL archival. Nonterminal rows
/// may lag a durable finish, so only the WAL can substantiate their state.
pub(crate) fn indexed_run_state(
    indexed: Option<RunIndexRow>,
    reconciled: Option<ReconciledRunState>,
) -> Result<Option<ReconciledRunState>, String> {
    let Some(row) = indexed.filter(|row| row.state.is_terminal()) else {
        return Ok(reconciled);
    };
    if row.state == RunIndexState::BudgetExhausted
        && reconciled
            .as_ref()
            .and_then(|state| state.budget_detail.as_ref())
            .is_none()
    {
        return Err("budget_exhausted detail is unavailable from the durable WAL".into());
    }
    if row.state == RunIndexState::ContextCeiling
        && reconciled
            .as_ref()
            .and_then(|state| state.context_ceiling.as_ref())
            .is_none()
    {
        return Err("context_ceiling detail is unavailable from the durable WAL".into());
    }
    // Interrupted and legacy rows do not store enough terminal metadata; the WAL
    // also carries steer receipts, error causes and structured-output results.
    let use_index_metadata = row.usage.is_some() && row.state != RunIndexState::Interrupted;
    let use_wal_error = !use_index_metadata || row.state == RunIndexState::Error;
    Ok(Some(ReconciledRunState {
        context_ceiling: reconciled
            .as_ref()
            .and_then(|state| state.context_ceiling.clone()),
        ceiling_deadline_ms: None,
        run_usage: reconciled
            .as_ref()
            .map(|state| state.run_usage.clone())
            .unwrap_or_default(),
        budget_detail: reconciled
            .as_ref()
            .and_then(|state| state.budget_detail.clone()),
        structured_output: reconciled
            .as_ref()
            .and_then(|state| state.structured_output.clone()),
        live_step: None,
        state: row.state,
        undelivered_steers: reconciled
            .as_ref()
            .and_then(|state| state.undelivered_steers.clone()),
        final_step_finish_reason: if use_index_metadata {
            row.final_step_finish_reason
        } else {
            reconciled
                .as_ref()
                .and_then(|state| state.final_step_finish_reason)
        },
        indeterminate_tool_calls: if use_index_metadata {
            None
        } else {
            reconciled
                .as_ref()
                .and_then(|state| state.indeterminate_tool_calls.clone())
        },
        usage: if use_index_metadata {
            row.usage
        } else {
            reconciled.as_ref().and_then(|state| state.usage)
        },
        retries_used: if use_index_metadata {
            row.retries_used
        } else {
            reconciled.as_ref().map_or(0, |state| state.retries_used)
        },
        title: row
            .title
            .or_else(|| reconciled.as_ref().and_then(|state| state.title.clone())),
        auth_selection: reconciled
            .as_ref()
            .and_then(|state| state.auth_selection.clone()),
        error: if use_wal_error {
            reconciled.and_then(|state| state.error)
        } else {
            None
        },
        pause_reason: None,
        ride_wait_deadline_ms: None,
    }))
}

/// Posts a `RunFinished` to the actor when the run task ends — INCLUDING on a panic
/// (unwind). The normal path calls `finish(result)`, which sends the real result and
/// disarms the guard; if the task panics before `finish`, `Drop` fires and sends a
/// synthetic `RunFinished{Err(Panicked)}`. Either way the actor learns the run ended and
/// never wedges `Active`. `Drop` is synchronous, so it uses the channel's non-blocking
/// `try_send` (the command channel has spare capacity for a single terminal). If that
/// try_send is ever dropped (a full channel at the instant of a panic — the panic IS the
/// end, so no later terminal is coming), the actor's idle-tick `is_finished()` backstop
/// (see `run`) still detects the finished run task and synthesizes the terminal. So this
/// guard is the fast path; the backstop is the guarantee.
/// Map a durably completed run summary to its run-index state. Once the coordinator marks a run
/// Active, core returns `Ok(summary)` only after syncing `RunFinished` or `RunPaused`; an
/// executor-level `Err` has no durable terminal and must never enter this derived index projection.
fn run_index_terminal_state(summary: &RunSummary) -> RunIndexState {
    if summary.paused.is_some() {
        return RunIndexState::Paused;
    }
    run_finish_index_state(summary.reason)
}

/// Recover the finish reason from a terminal run-index state; active and paused are unfinished.
pub(crate) fn indexed_finish_reason(
    state: &RunIndexState,
) -> Option<broca_protocol::RunFinishReason> {
    use broca_protocol::RunFinishReason;
    match state {
        RunIndexState::Active | RunIndexState::Paused => None,
        RunIndexState::Completed => Some(RunFinishReason::Completed),
        RunIndexState::Interrupted => Some(RunFinishReason::Interrupted),
        RunIndexState::MaxSteps => Some(RunFinishReason::MaxSteps),
        RunIndexState::BudgetExhausted => Some(RunFinishReason::BudgetExhausted),
        RunIndexState::ContextCeiling => Some(RunFinishReason::ContextCeiling),
        RunIndexState::Cancelled => Some(RunFinishReason::Cancelled),
        RunIndexState::Error => Some(RunFinishReason::Error),
        RunIndexState::TransformUnavailable => Some(RunFinishReason::TransformUnavailable),
    }
}

/// The run-index state a durable `RunFinished` reason maps to.
fn run_finish_index_state(reason: broca_protocol::RunFinishReason) -> RunIndexState {
    use broca_protocol::caller::RunFinishReason;
    match reason {
        RunFinishReason::Completed => RunIndexState::Completed,
        RunFinishReason::MaxSteps => RunIndexState::MaxSteps,
        RunFinishReason::BudgetExhausted => RunIndexState::BudgetExhausted,
        RunFinishReason::ContextCeiling => RunIndexState::ContextCeiling,
        RunFinishReason::Cancelled => RunIndexState::Cancelled,
        RunFinishReason::Interrupted => RunIndexState::Interrupted,
        RunFinishReason::Error => RunIndexState::Error,
        RunFinishReason::TransformUnavailable => RunIndexState::TransformUnavailable,
    }
}

struct RunFinishGuard {
    tx: Option<mpsc::UnboundedSender<LineageCmd>>,
    run_id: RunId,
    attempt_id: u64,
}

impl RunFinishGuard {
    fn new(tx: mpsc::UnboundedSender<LineageCmd>, run_id: RunId, attempt_id: u64) -> Self {
        Self {
            tx: Some(tx),
            run_id,
            attempt_id,
        }
    }

    /// The run finished normally: deliver the real result RELIABLY and DISARM the guard (so
    /// `Drop` posts nothing).
    fn finish(mut self, result: Result<RunSummary, broca_core::RunError>) {
        if let Some(tx) = self.tx.take() {
            if terminal_send_suppressed(&self.run_id) {
                return; // test seam: simulate a LOST terminal (full channel at the instant)
            }
            deliver_terminal(tx, self.run_id.clone(), self.attempt_id, Box::new(result));
        }
    }
}

impl Drop for RunFinishGuard {
    fn drop(&mut self) {
        // Only fires if `finish` did NOT run (a panic unwound the task) — `finish` took
        // the sender. Deliver a synthetic Panicked terminal so the actor leaves Active.
        if let Some(tx) = self.tx.take() {
            if terminal_send_suppressed(&self.run_id) {
                return; // test seam: simulate a LOST terminal (full channel at the instant)
            }
            deliver_terminal(
                tx,
                self.run_id.clone(),
                self.attempt_id,
                Box::new(Err(broca_core::RunError::Panicked)),
            );
        }
    }
}

/// Deliver a run terminal on a dedicated unbounded actor lane. External commands remain
/// bounded for backpressure, but a terminal must never wait behind that queue: the idle
/// backstop may observe the run task as finished and must see the real outcome first.
fn deliver_terminal(
    tx: mpsc::UnboundedSender<LineageCmd>,
    run_id: RunId,
    attempt_id: u64,
    result: Box<Result<RunSummary, broca_core::RunError>>,
) {
    let cmd = LineageCmd::RunFinished {
        run_id,
        attempt_id,
        result,
    };
    // A closed lane means the actor is already gone; there is nothing left to notify.
    let _ = tx.send(cmd);
}

/// Admission checks of a send's `structured_output` that depend on the session, after the
/// serve layer has checked the value itself. Returns the field of the `invalid_params` refusal.
///
/// A steer joining the active run runs under that run's frozen request, so it may only repeat
/// it exactly. Any other send starts a run: its admitted tool set (the plan's frozen tools, the
/// send's own, or the tools it inherits by omitting them) must not include `submit_result`, and
/// must be empty for `final_text`.
fn check_structured_output(
    structured_output: &broca_protocol::structured_output::StructuredOutput,
    request: &SubmissionRequest,
    plan: Option<&SendPlan>,
    latest: Option<&broca_wal::RunConfig>,
    joins_active_run: bool,
) -> Result<(), String> {
    use broca_protocol::structured_output::{FrozenStructuredOutput, FIELD};
    if joins_active_run {
        let frozen = latest.and_then(|config| {
            FrozenStructuredOutput::from_frozen_render_config(Some(&config.frozen_render_config))
        });
        return match frozen {
            Some(frozen) if frozen.matches(structured_output) => Ok(()),
            _ => Err(FIELD.to_owned()),
        };
    }
    let tools: Vec<String> = match (plan.and_then(|plan| plan.manifest.as_ref()), &request.tools) {
        (Some(manifest), _) => manifest
            .tools
            .iter()
            .map(|tool| tool.name.clone())
            .collect(),
        (None, Some(tools)) => tools.iter().map(|tool| tool.name.clone()).collect(),
        (None, None) => latest
            .map(|config| config.tools.iter().map(|tool| tool.name.clone()).collect())
            .unwrap_or_default(),
    };
    structured_output.check(true, &tools)
}

/// Fill each runner parameter `request` left to the session (`inherit_frozen`) from `frozen`, the
/// session's latest episode's config, so the new turn renders the same request prefix that
/// episode did. The cache is not filled here: its frozen form is a resolved policy, which the
/// executor takes from the render config (`RunRequest::inherit_frozen_cache`).
fn inherit_frozen_parameters(request: &mut SubmissionRequest, frozen: &broca_wal::RunConfig) {
    use broca_protocol::request::InheritedParam;
    for param in request.inherit_frozen.clone() {
        match param {
            InheritedParam::Model => {
                request.model = frozen.model.clone();
                request.variant.clone_from(&frozen.variant);
            }
            InheritedParam::ToolChoice => request.tool_choice = frozen.tool_choice.clone(),
            InheritedParam::Generation => request.generation = frozen.generation.clone(),
            InheritedParam::StopWhen => request.stop_when.clone_from(&frozen.stop_when),
            InheritedParam::Budget => {
                request.budget.clone_from(&frozen.budget);
                // Inherit tool-call reminders without restoring the preceding episode's old cap.
                // An omitted input cap comes from the session-wide WAL checkpoint instead.
                if let Some(budget) = &mut request.budget {
                    budget.max_input_tokens = None;
                }
            }
            InheritedParam::ContextLimit => request.context_limit = frozen.context_limit,
            InheritedParam::WorkClass => request.work_class.clone_from(&frozen.work_class),
            InheritedParam::ServiceTier => request.service_tier = frozen.service_tier,
            InheritedParam::Cache => {}
        }
    }
}

/// Compare only named settings: a joining steer inherits every setting it omits. The order
/// is stable so a send changing several settings always gets the same first field in its reply.
fn steer_settings_conflict(
    request: &SubmissionRequest,
    frozen: &broca_wal::RunConfig,
    stop_sequences_named: bool,
) -> Option<&'static str> {
    use broca_protocol::request::InheritedParam;
    if !request.inherit_frozen.contains(&InheritedParam::Model) && request.model != frozen.model {
        return Some("model");
    }
    if request.variant.is_some() && request.variant != frozen.variant {
        return Some("model.variant");
    }
    let generation = &request.generation;
    let frozen_generation = &frozen.generation;
    if generation.max_output_tokens.is_some()
        && generation.max_output_tokens != frozen_generation.max_output_tokens
    {
        return Some("generation.max_output_tokens");
    }
    if generation.temperature.is_some() && generation.temperature != frozen_generation.temperature {
        return Some("generation.temperature");
    }
    if generation.top_p.is_some() && generation.top_p != frozen_generation.top_p {
        return Some("generation.top_p");
    }
    if (stop_sequences_named || !generation.stop_sequences.is_empty())
        && generation.stop_sequences != frozen_generation.stop_sequences
    {
        return Some("generation.stop_sequences");
    }
    if !request.inherit_frozen.contains(&InheritedParam::StopWhen)
        && request.stop_when != frozen.stop_when
    {
        return Some("stop_when.max_steps");
    }
    if request.directives.as_ref().is_some_and(|names| {
        names.as_slice() != frozen.turn_directives_v1.as_deref().unwrap_or_default()
    }) {
        return Some("directives");
    }
    // Checked last, so a send that also conflicts on an earlier setting still reports that
    // setting rather than `auth`.
    if let Some(pin) = &request.auth {
        if !steer_auth_pin_matches(pin, frozen.auth_selection.as_ref()) {
            return Some("auth");
        }
    }
    None
}

/// Whether a joining steer's auth pin names the credential the active run is already using.
///
/// The run's credential is the one admission froze (or an auth refreeze replaced), read from the
/// episode config, never the provider's configured default. Its id has the form
/// `<method>:<provider>[:<account>]`. A pin naming only a method matches on the method alone,
/// even when the run was routed to an account. A pin naming an account matches only that frozen
/// account: a running run cannot be moved to another account, and a run with no frozen account
/// cannot be told which one it used. A run with no frozen selection at all (no vault, or a record
/// older than the field) cannot confirm any pin, so every pin is refused rather than ignored.
fn steer_auth_pin_matches(
    pin: &broca_protocol::request::AuthRequest,
    frozen: Option<&broca_wal::AuthSelection>,
) -> bool {
    let Some(selection) = frozen else {
        return false;
    };
    let mut segments = selection.credential_id.splitn(3, ':');
    let method = segments.next();
    let _provider = segments.next();
    let account = segments.next();
    if method != Some(pin.method.as_str()) {
        return false;
    }
    match &pin.account {
        None => true,
        Some(named) => account == Some(named.as_str()),
    }
}

/// Now, in milliseconds since the Unix epoch.
fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// The reply to a warm the executor did not complete.
fn warm_error_outcome(
    error: crate::executor::WarmError,
    request_body_sha256: String,
) -> WarmOutcome {
    use crate::executor::WarmError;
    use broca_wire::WarmRefusalDetail as Refusal;
    let detail = match error {
        WarmError::Unsupported(reason) => Refusal::WarmUnsupported { reason },
        WarmError::PrefixUnavailable(reason) => Refusal::WarmPrefixUnavailable {
            request_body_sha256: Some(request_body_sha256),
            reason,
        },
        WarmError::AccountChanged { frozen, served } => Refusal::AccountChanged {
            frozen_account: frozen,
            served_account: served,
        },
        // The warm itself found the provider account exhausted (`account_limit_exhausted`):
        // reported as `account_walled`, the refusal every later warm meets until the reset.
        WarmError::Provider(error) if error.is_account_limit_exhausted() => {
            match error.resets_at_ms {
                Some(resets_at_ms) => Refusal::AccountWalled { resets_at_ms },
                None => Refusal::WarmFailed {
                    error: (*error).into(),
                },
            }
        }
        WarmError::Provider(error) => Refusal::WarmFailed {
            error: (*error).into(),
        },
        WarmError::Unavailable(message) => return WarmOutcome::Transient(message),
    };
    WarmOutcome::Refused(broca_wire::WarmRefusal::new(detail))
}

/// Test-only seam: when set, the run-finish guard drops its terminal instead of sending it.
/// This exercises the idle-tick liveness backstop's genuine-loss path; ordinary bounded-command
/// backpressure cannot lose or park terminals because they use their own unbounded lane.
/// Compiled OUT of the production binary
/// entirely (`#[cfg(test)]`), so it has zero production surface.
#[cfg(test)]
fn terminal_send_suppressed(run_id: &RunId) -> bool {
    tests::SUPPRESSED_TERMINAL_RUN.lock().unwrap().as_deref() == Some(run_id.0.as_str())
}

#[cfg(not(test))]
#[inline(always)]
fn terminal_send_suppressed(_run_id: &RunId) -> bool {
    false
}

/// The start policy: projection, paused-run cancellation, and this send's delivery mode.
///
/// They were positional and adjacent, which is the arrangement where a transposition compiles
/// and changes behaviour in two directions at once: a one-shot send that materializes, and an
/// ordinary send that seals itself as cancelled before executing. Neither reads as wrong in a
/// diff. Clippy's argument-count warning is what surfaced it; the naming is the actual repair.
struct StartRunFlags {
    /// Skip the store projection for a send the caller declared single-turn.
    skip_materialization: bool,
    /// Pre-cancel the run token so the executor seals an already-admitted paused episode
    /// instead of driving it.
    cancel_before_execute: bool,
    input_delivery: broca_protocol::Delivery,
}

enum StartRunFailure {
    ContextCeiling(broca_protocol::ContextCeilingDetail),
    Expired,
    Shutdown,
    Infrastructure(String),
    Admission(RunError),
    /// The owner check refused the send against the session's history at admission.
    Scope(broca_core::ScopeRefusal),
}

/// The send key a run or queued submission is admitted under, as its WAL record will carry it.
#[derive(Debug, Clone, Default)]
struct SendIdentity {
    conversation_key: Option<String>,
    /// The caller's title, written on the record this send writes (`RunStarted`, `Queued` or
    /// `Steered`). Deliberately absent from `request_identity_digest`: a title labels the send
    /// and never changes what it asks for.
    title: Option<String>,
    /// The caller's `send_id`, when the run id was derived from one.
    send_id: Option<String>,
    /// The canonical request identity, for any send whose run id can repeat.
    request_identity_digest: Option<String>,
}

/// What the WAL holds for the send behind one run id: enough to answer a retry with the same
/// key without the run index, which is only a projection and can lose or never get the row.
#[derive(Debug, Clone, Default, PartialEq)]
struct RecordedSendKey {
    /// A `RunStarted` opened an episode under this run id, directly or by draining its queued
    /// send. `false` means only a `Queued` record exists, which the queue path answers.
    started: bool,
    /// The request identity digest recorded on the `RunStarted` or the `Queued` record. `None`
    /// when neither carries one (records written before the digest was recorded there).
    request_identity_digest: Option<String>,
}

/// Rebuild every recorded send key from the complete WAL. A queued submission is keyed by the
/// run id `queue_submission` derived its `sub-<run_id>` id from, which is the run id the drain
/// starts, so the `Queued` and `RunStarted` records of one send land on one entry.
fn recorded_send_keys(records: &[WalRecord]) -> HashMap<RunId, RecordedSendKey> {
    let mut keys: HashMap<RunId, RecordedSendKey> = HashMap::new();
    for record in records {
        let (run_id, digest, started) = match record {
            WalRecord::RunStarted {
                run_id,
                request_identity_digest,
                ..
            } => (run_id.clone(), request_identity_digest, true),
            WalRecord::Queued {
                submission_id,
                request_identity_digest,
                ..
            } => (
                run_id_of_queued(submission_id),
                request_identity_digest,
                false,
            ),
            _ => continue,
        };
        let entry = keys.entry(run_id).or_default();
        entry.started |= started;
        // The first record to carry a digest wins: the drain copies the queued digest onto its
        // `RunStarted`, so both hold the same value whenever both hold one.
        if entry.request_identity_digest.is_none() {
            entry.request_identity_digest = digest.clone();
        }
    }
    keys
}

/// What the WAL holds for one steer, keyed by its `send_id`: enough to answer a retry with the
/// same key without anything but the WAL.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RecordedSteer {
    /// The run the steer joined: the episode whose `RunStarted` precedes its `Steered` record.
    /// `None` only for a malformed WAL whose steer precedes every episode.
    pub(crate) run_id: Option<RunId>,
    /// The request identity digest the `Steered` record carries.
    request_identity_digest: Option<String>,
}

/// Rebuild every recorded steer from the complete WAL. A steer is keyed by its `send_id`, not
/// by a run id: it opens no episode of its own. The first record for a key wins.
pub(crate) fn recorded_steers(records: &[WalRecord]) -> HashMap<String, RecordedSteer> {
    let mut current_run = None;
    let mut steers = HashMap::new();
    for record in records {
        match record {
            WalRecord::RunStarted { run_id, .. } => current_run = Some(run_id.clone()),
            WalRecord::Steered {
                send_id: Some(send_id),
                request_identity_digest,
                ..
            } => {
                steers
                    .entry(send_id.clone())
                    .or_insert_with(|| RecordedSteer {
                        run_id: current_run.clone(),
                        request_identity_digest: request_identity_digest.clone(),
                    });
            }
            _ => {}
        }
    }
    steers
}

/// Where the WAL says the steer sent under `send_id` was delivered, or `None` while no model step
/// has rendered it yet.
///
/// A steer is rendered by the first `StepStarted` after its `Steered` record. The run loop takes
/// every steer queued so far and appends that `StepStarted` under one lock (`broca_core::steer`),
/// and replay places every pending steer at the next `StepStarted` too, so the record is the
/// point at which the message joins the prompt of a model call, live and after a crash alike. If
/// the run the steer joined ends first (cancel, error, limit, a crash sealed by the startup
/// sweep), replay places the steer at that run's close, and the first `StepStarted` after it lies
/// in a later episode whose prefix carries it. The run a `StepStarted` belongs to is the episode
/// opened by the latest `RunStarted` before it; a resumed run writes no `RunStarted` of its own,
/// so its steps stay with the run they resume.
///
/// - rendered by the run it joined: `step`, naming that run;
/// - rendered first by a later episode: `turn`, naming that later run.
///
/// The answer is a function of an append-only record sequence: once a `StepStarted` follows the
/// steer, later records cannot change which one came first, so a receipt once given never
/// changes. That holds only for records that cannot be lost, which is why callers pass the WAL
/// read up to its durable head.
pub(crate) fn steer_delivery(records: &[WalRecord], send_id: &str) -> Option<Delivered> {
    let mut current_run: Option<&RunId> = None;
    // `Some(joined)` once the steer's record has been seen; `joined` is the run it joined.
    let mut steered: Option<Option<&RunId>> = None;
    for record in records {
        match record {
            WalRecord::RunStarted { run_id, .. } => current_run = Some(run_id),
            WalRecord::Steered {
                send_id: Some(id), ..
            } if steered.is_none() && id == send_id => steered = Some(current_run),
            WalRecord::StepStarted { .. } => {
                let (Some(joined), Some(rendered_in)) = (steered, current_run) else {
                    continue;
                };
                let kind = if joined == Some(rendered_in) {
                    delivered_as::STEP
                } else {
                    delivered_as::TURN
                };
                return Some(Delivered::new(kind).with_ref(rendered_in.0.clone()));
            }
            _ => {}
        }
    }
    None
}

/// The run id a queued submission starts under: `queue_submission` names it `sub-<run_id>`.
fn run_id_of_queued(submission_id: &broca_protocol::SubmissionId) -> RunId {
    RunId(
        submission_id
            .0
            .strip_prefix("sub-")
            .unwrap_or(&submission_id.0)
            .to_owned(),
    )
}

/// Payload-independent caller identity retained for same-actor retries and pause finalization.
#[cfg_attr(test, derive(serde::Serialize))]
struct AdmittedRequestIdentity {
    /// The versioned digest stores one fixed-size hash per identity field, preserving the first
    /// field-specific reuse error without retaining that field's payload.
    request_identity_digest: Option<String>,
    /// Pause finalization needs to distinguish an inherited tool surface from an explicit one.
    /// Every other execution input is reconstructed from the durable episode.
    tools_were_omitted: bool,
}

impl std::fmt::Display for StartRunFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shutdown => write!(f, "module is stopping"),
            Self::ContextCeiling(detail) => write!(
                f,
                "context_ceiling: size {} in window {} ({})",
                detail.size, detail.window, detail.cause
            ),
            Self::Infrastructure(message) => f.write_str(message),
            Self::Expired => f.write_str("lineage expired before admission"),
            Self::Admission(error) => error.fmt(f),
            Self::Scope(refusal) => write!(f, "scope refused: {refusal:?}"),
        }
    }
}

/// Why a budget cap denied admission. The reason string is what callers and the queued-admission
/// tombstone see; the classification decides whether a durably queued send may be failed for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BudgetDenial {
    /// Recorded spend has reached the hard cap. Waiting does not lower spend.
    HardCapExceeded,
    /// Some spend is not yet resolved into the cap mirror.
    UnresolvedSpend,
    /// The cap mirror was applied against a different export source or epoch than this daemon's
    /// export index reports, or past its high-water. Only the cap provider re-applying the cap
    /// against this export store clears it, and nothing in broca triggers that, so it can last
    /// indefinitely (for example after the export store is recreated).
    ExportSourceMismatch,
    /// The cap mirror has not yet consumed every export fact this daemon wrote.
    TrailingWatermark,
    /// Caps apply, but this actor has no export index to compare the mirror against. This is
    /// fixed for the actor's lifetime, so it does not clear by waiting.
    ExportIndexUnavailable,
    /// The cap mirror is older than the cap allows.
    Stale,
}

impl BudgetDenial {
    fn reason(self) -> &'static str {
        match self {
            Self::HardCapExceeded => "hard_cap_exceeded",
            Self::UnresolvedSpend => "unresolved_spend",
            Self::ExportSourceMismatch => "export_source_mismatch",
            Self::TrailingWatermark => "trailing_watermark",
            Self::ExportIndexUnavailable => "export_index_unavailable",
            Self::Stale => "stale",
        }
    }

    /// Whether the denial may clear after accounting or cap-mirror state is updated in the normal
    /// course (spend resolves, or the cap provider's regular refresh brings the mirror up to
    /// date). A queued send denied for such a reason stays queued for a later drain. Any other
    /// denial fails the send, so the caller sees the refusal rather than a send that waits
    /// silently on something nothing will update.
    fn clears_by_waiting(self) -> bool {
        match self {
            Self::UnresolvedSpend | Self::TrailingWatermark | Self::Stale => true,
            Self::HardCapExceeded | Self::ExportIndexUnavailable | Self::ExportSourceMismatch => {
                false
            }
        }
    }

    fn from_reason(reason: &str) -> Option<Self> {
        [
            Self::HardCapExceeded,
            Self::UnresolvedSpend,
            Self::ExportSourceMismatch,
            Self::TrailingWatermark,
            Self::ExportIndexUnavailable,
            Self::Stale,
        ]
        .into_iter()
        .find(|denial| denial.reason() == reason)
    }

    fn into_error(self, cap_id: String) -> RunError {
        RunError::BudgetDenied {
            cap_id,
            reason: self.reason().into(),
        }
    }
}

fn queued_admission_is_permanent(error: &RunError) -> bool {
    if let RunError::BudgetDenied { reason, .. } = error {
        // An unrecognized reason keeps the permanent class it has always had.
        if BudgetDenial::from_reason(reason).is_some_and(BudgetDenial::clears_by_waiting) {
            return false;
        }
    }
    error.admission_error_class() == ErrorClass::Permanent
}

fn submission_from_episode(episode: &broca_core::LastEpisodeV1) -> SubmissionRequest {
    let mut request = submission_from_config(episode.config.clone(), episode.input.clone());
    request.on_restart = episode.on_restart;
    request.keep_warm = episode.keep_warm;
    request
}

fn submission_from_config(
    config: broca_core::RunConfig,
    input: Vec<broca_protocol::Message>,
) -> SubmissionRequest {
    let policy_allowed = config.tools.iter().map(|tool| tool.name.clone()).collect();
    SubmissionRequest {
        directives: config.turn_directives_v1,
        on_restart: None,
        keep_warm: None,
        scope: Default::default(),
        project_id: None,
        principal: None,
        model: config.model,
        input,
        tools: Some(Arc::unwrap_or_clone(config.tools)),
        policy_allowed,
        tool_choice: config.tool_choice,
        generation: config.generation,
        cache: None,
        budget: config.budget,
        // A resumed episode must reuse the frozen credential id rather than reconstructing a pin.
        auth: None,
        context_limit: config.context_limit,
        work_class: config.work_class,
        variant: config.variant,
        service_tier: None,
        stop_when: config.stop_when,
        context_management: config.context_management,
        inherit_frozen: Default::default(),
        structured_output: None,
        client_message_id: None,
    }
}

/// How a send with a legacy string `delivery` is admitted ([`LineageActor::string_sender`]).
enum StringSender {
    /// The session's owner, or a caller the plan does not name: admitted as before.
    Owner,
    /// A plan-named sender's `queue` or `steer`, admitted and delivered as the list it stands
    /// for.
    AsOrder(broca_protocol::DeliveryOrder),
    /// A plan-named sender's granted `interrupt`, continuing under this frozen principal.
    Interrupt(broca_protocol::BindPrincipal),
}

/// The `senders` of the session's frozen plan, or none for a session admitted without one.
fn frozen_plan_senders(
    ownership: &broca_core::SessionOwnershipV1,
) -> Vec<cortexkit_role_llm_runner::delivery_order::PlanSender> {
    ownership
        .first_episode
        .as_ref()
        .and_then(|first| first.manifest.as_ref())
        .and_then(|manifest| {
            serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text()).ok()
        })
        .and_then(|plan| {
            plan.as_object()
                .and_then(|plan| broca_fetch_plan::plan_senders(plan).ok())
        })
        .unwrap_or_default()
}

/// The actor's run state.
enum RunState {
    Idle,
    Active {
        /// The DURABLE run_id — the original id on a resume (the loop continues under it),
        /// or the caller's id on a fresh/appended episode. This is the id the client got
        /// back and the id in the run index, so run-keyed lookups (cancel/status) resolve
        /// against it.
        run_id: RunId,
        /// The run-driving TASK's id (the caller's `req.run_id`). On a resume this DIFFERS
        /// from `run_id` (the durable/original id). The `RunFinishGuard` and the idle-tick
        /// backstop fire terminals tagged with this task id, so the terminal-matching in
        /// `on_run_finished` keys on `task_run_id`, not the durable `run_id`.
        task_run_id: RunId,
        /// Distinguishes retries that reuse the same run id after a pre-admission failure.
        attempt_id: u64,
        /// Projection policy retained while the run is active so a cancellation finalizer uses
        /// the same policy as the attempt it seals.
        skip_materialization: bool,

        cancel: CancellationToken,
        /// Where a steer hands this run its message. The run closes it once it will render no
        /// further step, after which the session is treated as having no running turn.
        steer: Arc<broca_core::SteerInbox>,
        /// The shutdown seal's stop signal for this run's provider attempts. The seal trips
        /// this, never `cancel`, so a sealed run cannot end as Cancelled.
        shutdown_seal: broca_core::ShutdownSeal,
        /// Current in-progress StepId for advisory session-level status. Relaxed ordering is
        /// sufficient because this monotonic display value does not guard any run state or data.
        progress: Arc<AtomicU64>,
        step_progress: broca_core::provider::LiveStepProgress,
        /// The run-driving task's join handle — held so the idle-tick liveness backstop
        /// can detect a run task that ended WITHOUT delivering a terminal (the rare
        /// dropped-synthetic-terminal case) via `is_finished()`.
        handle: tokio::task::JoinHandle<()>,
        /// Set as soon as this run's `RunFinished` is synced, before the actor has processed the
        /// run's completion. See [`DurableFinish`].
        durable_finish: Arc<std::sync::OnceLock<DurableFinish>>,
    },
}

/// A run terminal that is already durable in the WAL while the actor still holds the run as
/// active.
///
/// Core syncs `RunFinished` and only then emits its `ControlUnit::RunFinished`, but the run task
/// reports completion to the actor later: after materialization, the executor's own post-run
/// work and the output drain. `run.result` reads the WAL, so it can already report the terminal
/// in that gap. The output forwarder records the terminal here when it sees that control unit,
/// so a re-sent send or a `run.status` answered from the actor's in-memory state agrees with the
/// WAL instead of answering `active` for a run that has ended.
#[derive(Debug, Clone)]
struct DurableFinish {
    /// The run the terminal names; core always names it, but `None` is accepted as this run.
    run_id: Option<RunId>,
    reason: broca_protocol::RunFinishReason,
}

/// Everything the actor owns for its session's lifetime.
pub struct LineageActor<W: WalStore> {
    // Retained after admission snapshots are recycled so internal retries meet the reader gate
    // without adding a whole-history read to every ordinary internal send.
    agent_run_scope: Option<Box<broca_protocol::SessionScope>>,
    #[cfg(all(test, feature = "test-clock"))]
    agent_run_writer_for_test: bool,
    retention_lineage: bool,
    retention_deadline: Option<u64>,
    admitting_retention: Option<u64>,
    admitting_session_class: Option<String>,
    warm_task: Option<tokio::task::JoinHandle<()>>,
    withdrawal_task: Option<crate::approval::WithdrawalTask>,
    withdrawal_index_lock: Arc<std::sync::Mutex<()>>,
    approvals: Arc<crate::approval_state::ApprovalState>,
    approval_seeded: bool,
    approval_debt: Option<bool>,
    actor_jobs: Arc<crate::approval::ActorJobs>,
    key: LineageKey,
    gen: u64,
    /// The session lease — held for the actor's whole lifetime, released (dropped) ONLY in
    /// the close teardown AFTER the writer is shut down. `Option` so the teardown can drop
    /// it in a controlled order. (The lease's epoch is already stamped on the pre-spawned
    /// session writer's records for forensic correlation, so the actor needs no separate epoch
    /// field.)
    lease: Option<Box<dyn LeaseHandle>>,
    /// The SESSION-scoped WAL writer: ONE writer for the whole lineage (the durability
    /// domain is the session/WAL file, not the run). Spawned once at open, drives every
    /// episode's records AND the coordinator's queue writes through ONE seq space, shut
    /// down ONCE in the close teardown (before the lease drops). Shared (`Arc`, `record()`
    /// is `&self`) so a run task and a concurrent queue-write both append safely.
    /// `Option` so the close teardown can TAKE it and consume it for a real
    /// `shutdown().await` (drain + final fsync + JOIN the writer thread) before the lease
    /// drops — by close time no run is active, so the actor holds the sole `Arc`.
    writer: Option<Arc<WalHandle>>,
    executor: Arc<dyn RunExecutor>,
    wal_store: Arc<W>,
    registry: RegistryHandle,
    rx: mpsc::Receiver<LineageCmd>,
    terminal_rx: mpsc::UnboundedReceiver<LineageCmd>,
    /// A clone of our own command sender, used to make our own `ActorHandle` on a close-abort.
    self_tx: mpsc::Sender<LineageCmd>,
    /// Run completions use a separate lane so bounded external-command backpressure cannot
    /// delay a real terminal behind the idle backstop.
    terminal_tx: mpsc::UnboundedSender<LineageCmd>,
    writer_poisoned: bool,
    /// Mirrors `state` for readers outside the actor: 1 while a run is in flight, 0 while idle.
    /// Written ONLY where `state` is written, so the two cannot disagree.
    run_in_flight: Arc<AtomicU64>,
    state: RunState,
    next_run_attempt: u64,
    /// Compact caller identities admitted during this actor lifetime. The digest preserves
    /// field-specific same-run reuse diagnostics, including omitted-vs-explicit tool surfaces,
    /// without retaining request payloads. The omission bit is also needed by pause finalization.
    admitted_request_identities: HashMap<RunId, AdmittedRequestIdentity>,
    /// How long the actor stays open (holding the lease) after the last activity before
    /// initiating close. Injected so tests can use a short TTL.
    idle_ttl: Duration,
    /// The control-stream fan-out: the projector + ring fed by the session writer's record
    /// observer, plus the live subscriber tasks. The actor runs the SMALL register+sync
    /// step of the attach barrier; the slow replay is off-actor. Torn down (subscribers
    /// cancelled+joined) in the close teardown BEFORE the writer shuts down + the lease
    /// drops.
    fanout: crate::fanout::Fanout<W>,
    /// The durable run index (coordinator-owned): the actor writes an `Active` row at
    /// admission and a terminal row in `on_run_finished`, so run-keyed ops (cancel/status)
    /// resolve a run_id without scanning. `None` when no index is configured (the fakes) —
    /// run-keyed lookups then fall back to the WAL. A DERIVED projection (the WAL is the
    /// authority): see `RunIndexState`'s stale-Active rule.
    run_index: Option<Arc<dyn RunIndex>>,
    /// Materialized reads and WAL projection are separate capabilities from the run index.
    materialized_store: Option<Arc<dyn SessionStore>>,
    /// Daemon-wide usage export is populated only after a durable terminal. It is optional so
    /// lightweight coordinator test openers and deployments can omit metering.
    export_index: Option<Arc<dyn broca_core::ExportIndex>>,
    charge_basis_resolver: Option<Arc<dyn broca_core::ChargeBasisResolver + Send + Sync>>,
    cap_store: Option<Arc<dyn broca_core::CapStore>>,
    /// The terminal run whose usage export has not yet advanced the export watermark. A queued
    /// run covered by the same cap must wait for that usage; retaining the id allows idle-tick
    /// retries after temporary export failures.
    pending_capped_export: Option<RunId>,
    /// Startup exhausted its bounded recovery retries without proving the durable queue state.
    /// Mutating admission must recover this state before it can use the in-memory queue view.
    startup_recovery_pending: bool,
    /// Idle admission view loaded at actor open or after the previous run's terminal barrier.
    prior_snapshot: Option<PriorWalSnapshot>,
    ceiling_drain_held: bool,
    /// Whether this actor has logged its compaction session passing the stored-message watch
    /// threshold; see `crate::compaction_watch`.
    compaction_watch: crate::compaction_watch::CompactionSessionWatch,
    pending_fifo: Vec<PendingSubmission>,
    /// Observed only during input hooks: dropping the RPC reply cancels this send, not its run.
    send_reply: Option<oneshot::Sender<SendOutcome>>,
    /// Best-effort execution hints for queued sends. They intentionally are not WAL fields: a
    /// recovery that loses a hint defaults to materialization, preserving read compatibility.
    skip_materialization_submissions: HashSet<broca_protocol::SubmissionId>,
    /// One-shot policy retained across same-process retries or pause finalization for a run.
    /// Reopen defaults to materialization because this execution hint is deliberately not durable.
    skip_materialization_runs: HashSet<RunId>,
    /// Set when a run sent with `one_shot` reaches a durable terminal. The caller declared it will
    /// not come back to this session, so the actor closes as soon as nothing else holds it open
    /// instead of keeping its writer, fan-out and run state for the whole idle TTL. A later send
    /// to the same session simply reopens it, as it does after any idle close.
    close_after_one_shot: bool,
    // An acknowledged cancel remains work until the WAL proves a terminal, even
    // if a racing pause or a transient storage failure delays its finalizer.
    pending_cancels: HashMap<RunId, bool>,
    /// Submission tombstones observed in the complete WAL. A deterministic send id is a logical
    /// identity, so its tombstone remains authoritative after the actor's pending cache drops it.
    retracted_submissions: HashSet<broca_protocol::SubmissionId>,
    /// The send key the WAL records for each run id, so a keyed retry is answered from the WAL
    /// (the authority) rather than from the run index. Loaded with `retracted_submissions` in
    /// one read of the WAL and kept current as this actor writes `RunStarted` and `Queued`.
    recorded_send_keys: HashMap<RunId, RecordedSendKey>,
    /// What the WAL records for each steer's `send_id`, loaded and kept current with
    /// `recorded_send_keys`.
    recorded_steers: HashMap<String, RecordedSteer>,
    /// Terminal WAL metadata is immutable. Cache it so repeated status polls do not scan
    /// the WAL again; reopened actors derive it anew from durable records.
    terminal_status_cache: HashMap<RunId, ReconciledRunState>,
    /// The title each started run's `RunStarted` records (`None` for none), read once per run
    /// for a status poll of the live run whose index row has no title, then kept: a run's
    /// `RunStarted` is written once and never changes.
    started_run_titles: HashMap<RunId, Option<String>>,
    /// Both `retracted_submissions` and `recorded_send_keys` reflect the complete WAL: loaded
    /// once, or known empty because the WAL was empty when the actor opened.
    wal_scan_complete: bool,
    /// Run ids the startup crash-orphan pass marked for this session (the run index still
    /// called them active when a previous process died). Taken from the registry when the
    /// actor starts and consumed by the first successful open-time recovery.
    crash_orphan_candidates: Option<HashSet<RunId>>,
    /// The session's first episode once known, shared with this actor's handle so the send
    /// reply can carry the admission baseline without reading anything.
    first_episode: Arc<std::sync::OnceLock<broca_core::FirstEpisodeV1>>,
    /// The manifest the send being handled froze for a session with no episode yet, held only
    /// while that send is handled so the episode it starts records it.
    admitting_manifest: Option<Arc<broca_wal::PlanManifest>>,
    /// The `delivery_order` group of the send being handled, held only while it is handled.
    admitting_order: Option<broca_protocol::DeliveryOrder>,
    /// The attested principal of the send being handled when it is a plan-named sender other
    /// than the session's owner; the send itself continues under the owner's principal.
    admitting_sent_by: Option<broca_protocol::BindPrincipal>,
    /// The ordered send being drained into a run during its own admission, with the route scope
    /// it arrived under. Such a send starts its run at once on an idle session, so its episode
    /// freezes that scope exactly as a direct send would; any other drained submission waited
    /// behind a run and carries only the scope the session already froze.
    immediate_ordered_drain: Option<(broca_protocol::SubmissionId, broca_protocol::SendScope)>,
    /// For sends with an ordered `delivery` list: `ended_submissions` holds each one the WAL
    /// records as ended undelivered (`SendEnded`: replaced or expired) with its reason, and
    /// `delivered_submissions` each one it records as delivered into a running turn
    /// (`SendDelivered`) with that run. Both are loaded and kept current with
    /// `retracted_submissions`, so a keyed retry of such a send is answered with its outcome.
    ended_submissions: HashMap<broca_protocol::SubmissionId, broca_wal::record::SendEndReason>,
    delivered_submissions: HashMap<broca_protocol::SubmissionId, RunId>,
    /// The order each ordered send's `DeliveryOrdered` record carries, by submission, loaded and
    /// kept current with `retracted_submissions`. A keyed retry must carry the same order.
    ordered_submissions: HashMap<broca_protocol::SubmissionId, broca_protocol::DeliveryOrder>,
    restart_admitting: bool,
    restart_waiters: Vec<oneshot::Sender<Result<(), String>>>,
    /// Set while startup restart recovery drives the admission, because that branch answers
    /// parked `restart_waiters` itself once it knows the episode's outcome.
    restart_recovery_owns_waiters: bool,
    /// A command dispatched inside a suspended restart admission asked this generation to
    /// exit. The admission ends early, and the enclosing command returns the exit request.
    nested_exit_requested: bool,
    /// Shared permit for manual and automatic cache refreshes. While held, keep the writer
    /// open so a completed response can still record its usage.
    warm_in_flight: crate::keep_warm::WarmSlot,
}

// A current-thread actor test can enable agent-run scope writing when constructing its actor.
// The captured flag cannot change an existing actor, and neither the flag nor field ships.
#[cfg(test)]
#[cfg(feature = "test-clock")]
thread_local! {
    static AGENT_RUN_WRITER_FOR_TEST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Why an episode is being sealed `Interrupted` outside the run loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SealCause {
    /// A graceful stop of a run this process is still running.
    Shutdown,
    /// The startup pass sealing a run a crashed process left running: a resume of the episode.
    CrashOrphan,
}

/// Whether an unfinished episode a crashed process left behind is sealed `Interrupted` by the
/// startup crash-orphan pass.
///
/// Only a RUNNING episode whose replay would re-drive the model (`Resume`) or stop for review
/// (`Interrupted`) qualifies. A paused episode is waiting for re-auth or an answer and its
/// caller resumes it, so it is never an orphan. `LineageIdle` means the last step already ended
/// the episode naturally and only its terminal record was lost; ordinary recovery writes that
/// true terminal without calling the model, which is more accurate than `Interrupted`.
fn crash_orphan_sealable(
    disposition: EpisodeDisposition,
    resolution: &broca_core::ReplayResolution,
) -> bool {
    matches!(
        (disposition, resolution),
        (
            EpisodeDisposition::Running,
            broca_core::ReplayResolution::Resume { .. }
                | broca_core::ReplayResolution::Interrupted { .. }
        )
    )
}

impl<W: WalStore + 'static> LineageActor<W> {
    /// Spawn the actor task; returns its command handle. The caller (the opener) has
    /// already acquired `lease` and spawned the session `writer` (stamping the lease epoch as
    /// forensic tenure metadata) WITH the `fanout`'s record observer, so the writer's appended
    /// records feed
    /// the control-stream projector.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        key: LineageKey,
        gen: u64,
        lease: Box<dyn LeaseHandle>,
        writer: Arc<WalHandle>,
        executor: Arc<dyn RunExecutor>,
        wal_store: Arc<W>,
        registry: RegistryHandle,
        idle_ttl: Duration,
        fanout: crate::fanout::Fanout<W>,
        run_index: Option<Arc<dyn RunIndex>>,
        materialized_store: Option<Arc<dyn SessionStore>>,
    ) -> ActorHandle {
        Self::spawn_with_export(
            key,
            gen,
            lease,
            writer,
            executor,
            wal_store,
            registry,
            idle_ttl,
            fanout,
            run_index,
            materialized_store,
            None,
            None,
        )
    }

    /// Spawn with the optional daemon-wide usage export capability.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_with_export(
        key: LineageKey,
        gen: u64,
        lease: Box<dyn LeaseHandle>,
        writer: Arc<WalHandle>,
        executor: Arc<dyn RunExecutor>,
        wal_store: Arc<W>,
        registry: RegistryHandle,
        idle_ttl: Duration,
        fanout: crate::fanout::Fanout<W>,
        run_index: Option<Arc<dyn RunIndex>>,
        materialized_store: Option<Arc<dyn SessionStore>>,
        export_index: Option<Arc<dyn broca_core::ExportIndex>>,
        charge_basis_resolver: Option<Arc<dyn broca_core::ChargeBasisResolver + Send + Sync>>,
    ) -> ActorHandle {
        Self::spawn_with_export_and_caps(
            key,
            gen,
            lease,
            writer,
            executor,
            wal_store,
            registry,
            idle_ttl,
            fanout,
            run_index,
            materialized_store,
            export_index,
            charge_basis_resolver,
            None,
        )
    }

    /// Spawn with export accounting and the daemon-scoped cap-table mirror.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_with_export_and_caps(
        key: LineageKey,
        gen: u64,
        lease: Box<dyn LeaseHandle>,
        writer: Arc<WalHandle>,
        executor: Arc<dyn RunExecutor>,
        wal_store: Arc<W>,
        registry: RegistryHandle,
        idle_ttl: Duration,
        fanout: crate::fanout::Fanout<W>,
        run_index: Option<Arc<dyn broca_core::RunIndex>>,
        materialized_store: Option<Arc<dyn broca_core::SessionStore>>,
        export_index: Option<Arc<dyn broca_core::ExportIndex>>,
        charge_basis_resolver: Option<Arc<dyn broca_core::ChargeBasisResolver + Send + Sync>>,
        cap_store: Option<Arc<dyn broca_core::CapStore>>,
    ) -> ActorHandle {
        let (tx, rx) = mpsc::channel(64);
        let (terminal_tx, terminal_rx) = mpsc::unbounded_channel();
        let durable_head = writer.durable_head();
        let run_in_flight = Arc::new(AtomicU64::new(0));
        let retractions_complete = durable_head.load(Ordering::Acquire) == 0;
        let first_episode = Arc::new(std::sync::OnceLock::new());
        let approvals = fanout.approval_state();
        let mut actor = LineageActor {
            agent_run_scope: None,
            #[cfg(all(test, feature = "test-clock"))]
            agent_run_writer_for_test: AGENT_RUN_WRITER_FOR_TEST.get(),
            retention_lineage: writer.is_retention_lineage() == Some(true),
            retention_deadline: None,
            admitting_retention: None,
            admitting_session_class: None,
            warm_task: None,
            withdrawal_task: None,
            withdrawal_index_lock: Arc::default(),
            approvals: approvals.clone(),
            approval_seeded: false,
            approval_debt: None,
            actor_jobs: Arc::default(),
            key: key.clone(),
            gen,
            lease: Some(lease),
            writer: Some(writer),
            executor,
            wal_store,
            registry: registry.clone(),
            rx,
            terminal_rx,
            self_tx: tx.clone(),
            terminal_tx,
            writer_poisoned: false,
            run_in_flight: Arc::clone(&run_in_flight),
            state: RunState::Idle,
            next_run_attempt: 1,
            admitted_request_identities: HashMap::new(),
            idle_ttl,
            fanout,
            run_index,
            materialized_store,
            export_index,
            charge_basis_resolver,
            cap_store,
            pending_capped_export: None,
            startup_recovery_pending: false,
            prior_snapshot: None,
            ceiling_drain_held: false,
            compaction_watch: Default::default(),
            pending_fifo: Vec::new(),
            send_reply: None,
            skip_materialization_submissions: HashSet::new(),
            skip_materialization_runs: HashSet::new(),
            close_after_one_shot: false,
            pending_cancels: HashMap::new(),
            retracted_submissions: HashSet::new(),
            recorded_send_keys: HashMap::new(),
            recorded_steers: HashMap::new(),
            terminal_status_cache: HashMap::new(),
            started_run_titles: HashMap::new(),
            wal_scan_complete: retractions_complete,
            crash_orphan_candidates: None,
            first_episode: Arc::clone(&first_episode),
            admitting_manifest: None,
            admitting_order: None,
            admitting_sent_by: None,
            immediate_ordered_drain: None,
            ended_submissions: HashMap::new(),
            delivered_submissions: HashMap::new(),
            ordered_submissions: HashMap::new(),
            restart_admitting: false,
            restart_waiters: Vec::new(),
            restart_recovery_owns_waiters: false,
            nested_exit_requested: false,
            warm_in_flight: Default::default(),
        };
        tokio::spawn(async move {
            let outcome = std::panic::AssertUnwindSafe(actor.run())
                .catch_unwind()
                .await;
            match outcome {
                Ok(()) => {
                    // Every normal exit path tears down children, writer and lease before
                    // returning, which makes this call a no-op for them. It still gives any
                    // other clean exit the same ordered teardown before the slot is finalized,
                    // instead of leaving it to field drop order (which drops the lease first).
                    actor.emergency_teardown().await;
                    // Generation-checked: a no-op once a close has already removed the slot.
                    actor.registry.finalize_exit(&actor.key, actor.gen);
                }
                Err(panic) => {
                    #[cfg(test)]
                    actor.registry.report_actor_panic();
                    // Keep the actor owned here across unwinding so emergency teardown can stop
                    // the detached run and writer before making the registry slot reopenable.
                    actor.emergency_teardown().await;
                    actor.registry.finalize_exit(&actor.key, actor.gen);
                    std::panic::resume_unwind(panic);
                }
            }
        });
        ActorHandle {
            gen,
            tx,
            durable_head,
            run_in_flight,
            first_episode,
            approvals,
        }
    }

    /// The ONLY way to become active. Paired with [`Self::set_idle`] so the health gauge cannot
    /// drift from the state it reports.
    ///
    /// This half is the dangerous one and it is why both exist. A future site that starts a run
    /// without raising the gauge leaves it reading 0, and A STUCK 0 IS INDISTINGUISHABLE FROM
    /// QUIET -- an operator checking before a restart sees exactly what an idle module looks like
    /// and cuts a run mid-generation. The clearing half fails the other way: a stuck 1 reads busy
    /// forever, which wastes an operator's patience and costs nobody their work.
    ///
    /// So the asymmetry is deliberate: both transitions go through a function, because the
    /// compiler cannot require it and the failure this prevents is silent.
    fn set_active(&mut self, state: RunState) {
        debug_assert!(
            matches!(state, RunState::Active { .. }),
            "set_active is for the active transition; use set_idle to clear"
        );
        self.run_in_flight.store(1, Ordering::Relaxed);
        self.state = state;
    }

    /// The ONLY way to return to idle. See [`Self::set_active`] for why both halves are funnelled.
    fn set_idle(&mut self) {
        self.run_in_flight.store(0, Ordering::Relaxed);
        self.state = RunState::Idle;
    }

    async fn run(&mut self) {
        // Reconcile durable work before the first command. A command that opened this actor may
        // already be waiting in `rx`; it must not overtake an unfinished episode or an older
        // queued submission recovered from the WAL.
        let seal = self.registry.seal_signal();
        self.crash_orphan_candidates = self.registry.take_crash_orphan_mark(&self.key);
        let recovery_started = tokio::time::Instant::now();
        let _ = self.sync_retention_index().await;
        let mut recovery_backoff = STARTUP_RECOVERY_INITIAL_BACKOFF;
        loop {
            if self.registry.is_stopping() {
                break;
            }
            let recovered = self.recover_durable_work().await;
            if std::mem::take(&mut self.nested_exit_requested) {
                self.emergency_teardown().await;
                return;
            }
            match recovered {
                Ok(()) => break,
                Err(error) => {
                    let now = self
                        .registry
                        .retention_service()
                        .map_or_else(epoch_ms, |service| service.clock.now_ms());
                    if self.expire_actor(now).await {
                        return;
                    }
                    let elapsed = recovery_started.elapsed();
                    if elapsed >= STARTUP_RECOVERY_RETRY_BUDGET {
                        tracing::warn!(
                            target: "admission",
                            "HARD WARNING: startup durable-work recovery still failed after {:?}; serving read-only commands while mutating admission remains recovery-gated: {error}",
                            STARTUP_RECOVERY_RETRY_BUDGET
                        );
                        self.startup_recovery_pending = true;
                        break;
                    }
                    tracing::warn!(
                        target: "admission",
                        "startup durable-work recovery deferred for {:?}: {error}",
                        recovery_backoff
                    );
                    let remaining = STARTUP_RECOVERY_RETRY_BUDGET.saturating_sub(elapsed);
                    tokio::time::sleep(recovery_backoff.min(remaining)).await;
                    recovery_backoff = recovery_backoff
                        .saturating_mul(2)
                        .min(STARTUP_RECOVERY_MAX_BACKOFF);
                }
            }
        }

        let _ = self.sync_retention_index().await;
        // Idle-TTL: a timer that fires `idle_ttl` after the last activity. We re-arm it on
        // every command; when it fires while idle with no pending work, we try to close.
        if let Some(plane) = self.executor.elicitation_plane() {
            self.withdrawal_task = Some(crate::approval::WithdrawalTask::spawn_with_pending(
                self.wal_store.clone(),
                self.key.clone(),
                self.writer.as_ref().unwrap().clone(),
                self.run_index.clone(),
                self.withdrawal_index_lock.clone(),
                plane,
                self.registry
                    .retention_service()
                    .map(|service| service.clock.clone())
                    .unwrap_or_default(),
                self.approval_debt != Some(false)
                    || self.approvals.debt_ready.load(Ordering::Acquire),
            ));
        }
        let mut idle_deadline = tokio::time::Instant::now() + self.idle_ttl;
        let clock = self
            .registry
            .retention_service()
            .map(|service| service.clock.clone())
            .unwrap_or_default();
        let mut clock_changes = clock.subscribe();
        loop {
            let expiry_wait = self
                .retention_deadline
                .map(|deadline| Duration::from_millis(deadline.saturating_sub(clock.now_ms())));
            let expiry = async {
                match expiry_wait {
                    Some(wait) => tokio::time::sleep(wait).await,
                    None => std::future::pending::<()>().await,
                }
            };
            let tick = tokio::time::sleep_until(idle_deadline);
            tokio::select! {
                biased;
                _ = seal.cancelled() => {
                    self.seal_for_shutdown().await;
                    return;
                }
                _ = clock_changes.changed() => {},
                _ = expiry => {
                    if self.expire_actor(clock.now_ms()).await { return; }
                }
                terminal = self.terminal_rx.recv() => {
                    if let Some(terminal) = terminal {
                        if self.handle_cmd(terminal).await {
                            self.emergency_teardown().await;
                            return;
                        }
                        if self.writer_poisoned {
                            self.teardown_poisoned().await;
                            return;
                        }
                        if self.close_finished_one_shot().await {
                            return;
                        }
                    }
                }
                cmd = self.rx.recv() => {
                    let Some(cmd) = cmd else { break };  // all handles dropped → exit
                    idle_deadline = tokio::time::Instant::now() + self.idle_ttl;
                    if self.handle_cmd(cmd).await {
                        self.emergency_teardown().await;
                        return;
                    }
                    if self.writer_poisoned {
                        self.teardown_poisoned().await;
                        return;
                    }
                    if self.close_finished_one_shot().await {
                        return;
                    }
                }
                _ = tick => {
                    self.fanout.reap_finished();
                    // Before considering close, handle external commands that are already
                    // queued, so close does not overtake work that raced in.
                    while let Ok(cmd) = self.rx.try_recv() {
                        if self.handle_cmd(cmd).await {
                            self.emergency_teardown().await;
                            return;
                        }
                    }
                    if self.idle_tick_backstop().await {
                        self.emergency_teardown().await;
                        return;
                    }
                    if self.writer_poisoned {
                        self.teardown_poisoned().await;
                        return;
                    }
                    if self.try_close().await { return; }  // closed + finalized
                    // close aborted (work raced in) → keep serving; re-arm next loop
                    idle_deadline = tokio::time::Instant::now() + self.idle_ttl;
                }
            }
        }
        // The actor retains a sender for its own close-abort path, so channel closure is not a
        // normal exit. If that invariant changes, still stop children and the writer before the
        // spawn wrapper makes this generation's registry slot reopenable.
        self.emergency_teardown().await;
    }

    /// Liveness backstop run on the idle tick. Returns true when the actor must take its
    /// emergency teardown.
    ///
    /// A run task publishes its terminal on the dedicated terminal lane before it can finish
    /// (its `RunFinishGuard` sends either the real result or a synthetic `Panicked`). So once
    /// the task is finished, any terminal it produced is already queued on that lane. The lane
    /// is checked before the tick in `select!`, but only at poll time: the run can publish and
    /// finish after that poll, or while the tick awaits an external command it drained. The
    /// lane is therefore drained here, after observing the finished task and before deciding
    /// the terminal was lost. Only a task that is still the active one after that drain had
    /// its terminal genuinely lost, and gets a synthetic `Panicked` so the lineage cannot stay
    /// wedged Active.
    async fn idle_tick_backstop(&mut self) -> bool {
        if !matches!(&self.state, RunState::Active { handle, .. } if handle.is_finished()) {
            return false;
        }
        while let Ok(terminal) = self.terminal_rx.try_recv() {
            if self.handle_cmd(terminal).await {
                return true;
            }
        }
        let RunState::Active {
            task_run_id,
            attempt_id,
            handle,
            ..
        } = &self.state
        else {
            return false;
        };
        if !handle.is_finished() {
            // A drained terminal let a queued submission start a new run.
            return false;
        }
        // Synthesize with the TASK id and attempt (what on_run_finished matches on).
        let task_run_id = task_run_id.clone();
        let attempt_id = *attempt_id;
        self.on_run_finished(task_run_id, attempt_id, Err(broca_core::RunError::Panicked))
            .await
    }

    /// Dispatch ONE command. Shared by the live `recv` arm and the pre-close drain.
    /// Returns true when an active run ended without a durable terminal and this actor generation
    /// must exit so the next generation recovers from the WAL before serving more work.
    async fn handle_cmd(&mut self, cmd: LineageCmd) -> bool {
        if let LineageCmd::CutLock { reply } = cmd {
            let busy = if matches!(self.state, RunState::Active { .. }) {
                Some("run_active".into())
            } else if self.has_pending() {
                Some("send_queued".into())
            } else if self.warm_in_flight.occupied() || self.warm_task.is_some() {
                Some("cache_warm".into())
            } else if self.approval_debt != Some(false) {
                Some("approval_open".into())
            } else if self.startup_recovery_pending {
                Some("unknown".into())
            } else {
                match self.paused_run().await {
                    Ok(Some(_)) => Some("run_active".into()),
                    Ok(None) => None,
                    Err(_) => Some("unknown".into()),
                }
            };
            let (release, wait) = oneshot::channel();
            if reply.send((busy, release)).is_ok() {
                let _ = wait.await;
            }
            return false;
        }
        if !matches!(
            &cmd,
            LineageCmd::RunFinished { .. } | LineageCmd::WarmFinished { .. }
        ) {
            let now = self
                .registry
                .retention_service()
                .map_or_else(epoch_ms, |service| service.clock.now_ms());
            if self
                .retention_deadline
                .is_some_and(|deadline| deadline <= now)
                && self.expire_actor(now).await
            {
                self.reply_writer_poisoned(cmd, "expired".into());
                return true;
            }
        }
        // A run task can poison the shared writer while this actor is waiting for another
        // command. Check before dispatch so a queued durability-bearing command receives a
        // clean fail-closed reply instead of being acknowledged through a poisoned writer.
        if let Some(error) = self.observe_writer_poison() {
            return self.reply_writer_poisoned(cmd, error);
        }

        let records_retention = matches!(&cmd, LineageCmd::RunFinished { .. });
        let should_exit = match cmd {
            LineageCmd::CutLock { .. } => {
                unreachable!("cut critical section handled before dispatch")
            }
            LineageCmd::RepairRetention { reply } => {
                let _ = reply.send(self.sync_retention_index().await);
                false
            }
            LineageCmd::Send {
                session_class,
                retention,
                stop_sequences_named,
                conversation_key,
                title,
                run_id,
                request,
                kind,
                key,
                skip_materialization,
                delivery,
                plan,
                order,
                reply,
            } => {
                self.admitting_order = order.map(|order| *order);
                // Keep the reply available to input hooks so a dropped RPC cannot write its
                // unacknowledged transform. Ordinary run admission keeps its existing barrier.
                let prior_reply = self.send_reply.replace(reply);
                let outcome = self
                    .on_send(
                        run_id,
                        *request,
                        kind,
                        key,
                        skip_materialization,
                        delivery,
                        plan.map(|plan| *plan),
                        conversation_key,
                        title,
                        retention,
                        session_class,
                        stop_sequences_named,
                    )
                    .await;
                let _ = self.sync_retention_index().await;
                self.admitting_retention = None;
                self.admitting_order = None;
                self.admitting_sent_by = None;
                // A manifest only ever applies to the episode its own send starts.
                self.admitting_manifest = None;
                let _ = self
                    .send_reply
                    .take()
                    .expect("send reply installed")
                    .send(outcome);
                self.send_reply = prior_reply;
                false
            }
            LineageCmd::Import { request, reply } => {
                let outcome = if self.registry.is_stopping() {
                    ImportOutcome::Reopen
                } else {
                    self.on_import(*request).await
                };
                let _ = reply.send(outcome);
                false
            }
            LineageCmd::Cancel { run_id, at, reply } => {
                let _ = reply.send(self.on_cancel_at(&run_id, at).await);
                false
            }
            LineageCmd::Retract {
                submission_id,
                reply,
            } => {
                let outcome = self.on_retract(submission_id).await;
                let _ = reply.send(outcome);
                false
            }
            LineageCmd::ResumeRestart { reply } => {
                if self.restart_admitting {
                    self.restart_waiters.push(reply);
                    return false;
                }
                let result = if matches!(self.state, RunState::Idle) {
                    self.recover_durable_work().await
                } else {
                    Ok(())
                };
                let _ = reply.send(result);
                false
            }
            LineageCmd::Status { reply } => {
                let _ = reply.send(self.status());
                false
            }
            LineageCmd::RunState { run_id, reply } => {
                let _ = reply.send(self.resolve_run_state(&run_id).await);
                false
            }
            LineageCmd::Subscribe {
                from,
                live,
                sink,
                reply,
            } => {
                let outcome = self.on_subscribe(from, live, sink).await;
                let _ = reply.send(outcome);
                false
            }
            LineageCmd::RunFinished {
                run_id,
                attempt_id,
                result,
            } => self.on_run_finished(run_id, attempt_id, *result).await,
            LineageCmd::Warm { lineage_id, reply } => {
                if self.registry.is_stopping() {
                    let _ = reply.send(WarmOutcome::Reopen);
                } else {
                    self.on_warm(lineage_id, reply).await;
                }
                false
            }
            LineageCmd::WarmFinished { warm } => {
                let (pending, result) = *warm;
                self.on_warm_finished(pending, result).await;
                false
            }
            // IdleTick only re-arms the actor's idle timer; it performs no session work.
            LineageCmd::IdleTick => false,
            LineageCmd::CompactionChanged { reply } => {
                self.prior_snapshot = None;
                let result = if matches!(self.state, RunState::Idle) {
                    async {
                        self.refresh_admission_view().await?;
                        self.drain_queue_one().await
                    }
                    .await
                } else {
                    Ok(())
                };
                let _ = reply.send(result);
                false
            }
            LineageCmd::ApplyCompaction { ready, reply } => {
                let result = if matches!(self.state, RunState::Idle) {
                    let store = self.wal_store.clone();
                    let key = self.key.clone();
                    match spawn_blocking(JobKind::WalRead, move || store.read_records(&key)).await {
                        Ok(Ok(records)) => {
                            self.executor
                                .apply_idle_compaction(
                                    &self.key,
                                    self.writer.as_ref().expect("writer live while open"),
                                    records.into_iter().map(|framed| framed.record).collect(),
                                    &ready,
                                )
                                .await
                        }
                        Ok(Err(error)) => Err(error.to_string()),
                        Err(error) => Err(error.to_string()),
                    }
                } else {
                    Ok(false)
                };
                let _ = reply.send(result);
                false
            }
            #[cfg(test)]
            LineageCmd::RetainedAdmissionIdentityState { reply } => {
                let retained_bytes = self
                    .admitted_request_identities
                    .iter()
                    .map(|(run_id, identity)| {
                        run_id.0.len()
                            + serde_json::to_vec(identity)
                                .expect("admission identities serialize")
                                .len()
                    })
                    .sum();
                let _ = reply.send((self.admitted_request_identities.len(), retained_bytes));
                false
            }
            #[cfg(test)]
            LineageCmd::PanicInActor => panic!("test: actor task panic"),
        };
        if records_retention {
            let _ = self.sync_retention_index().await;
            self.admitting_retention = None;
        }
        self.observe_writer_poison();
        should_exit || std::mem::take(&mut self.nested_exit_requested)
    }

    /// Mark the actor for emergency teardown as soon as its shared writer becomes poisoned.
    /// This is checked around every command dispatch because a run task owns another Arc.
    fn observe_writer_poison(&mut self) -> Option<String> {
        let error = self
            .writer
            .as_ref()
            .and_then(|writer| writer.poison_error());
        if error.is_some() {
            self.writer_poisoned = true;
        }
        error
    }

    /// Reply without making a durability claim when a queued command reaches a poisoned actor.
    /// Closing responses make callers re-route after the emergency teardown has released the slot.
    fn reply_writer_poisoned(&mut self, cmd: LineageCmd, error: String) -> bool {
        match cmd {
            LineageCmd::RepairRetention { reply } => {
                let _ = reply.send(Err("session writer is poisoned".into()));
            }
            LineageCmd::Send { reply, .. } => {
                let outcome = if error == "expired" {
                    SendOutcome::Reopen
                } else {
                    SendOutcome::Error(format!("session writer is poisoned: {error}"))
                };
                let _ = reply.send(outcome);
            }
            LineageCmd::Import { reply, .. } => {
                let _ = reply.send(ImportOutcome::Error(broca_core::ImportError::Wal(error)));
            }
            LineageCmd::Cancel { reply, .. } => {
                let _ = reply.send(CancelOutcome::Reopen);
            }
            LineageCmd::Retract { reply, .. } => {
                let _ = reply.send(if error == "expired" {
                    RetractOutcome::Reopen
                } else {
                    RetractOutcome::Error(error)
                });
            }
            LineageCmd::ResumeRestart { reply } => {
                let _ = reply.send(Err("session actor is closing".into()));
            }
            LineageCmd::Status { reply } => {
                let _ = reply.send(LineageStatus::Closing);
            }
            LineageCmd::RunState { reply, .. } => {
                let _ = reply.send(Err(format!("session writer is poisoned: {error}")));
            }
            LineageCmd::Subscribe { reply, .. } => {
                let _ = reply.send(if error == "expired" {
                    SubscribeOutcome::Reopen
                } else {
                    SubscribeOutcome::Error(error)
                });
            }
            LineageCmd::RunFinished { .. } => return true,
            LineageCmd::Warm { reply, .. } => {
                let _ = reply.send(if error == "expired" {
                    WarmOutcome::Reopen
                } else {
                    WarmOutcome::Error(format!("session writer is poisoned: {error}"))
                });
            }
            LineageCmd::WarmFinished { warm } => {
                let _ = warm.0.reply.send(WarmOutcome::Error(format!(
                    "session writer is poisoned: {error}"
                )));
            }
            LineageCmd::CutLock { reply } => {
                drop(reply);
            }
            LineageCmd::IdleTick
            | LineageCmd::CompactionChanged { .. }
            | LineageCmd::ApplyCompaction { .. } => {}
            #[cfg(test)]
            LineageCmd::RetainedAdmissionIdentityState { reply } => {
                let _ = reply.send((usize::MAX, usize::MAX));
            }
            #[cfg(test)]
            LineageCmd::PanicInActor => {}
        }
        false
    }

    /// Attach a subscriber in this order so it sees every record exactly once: register the
    /// live receiver first, then enable live forwarding, then sync the WAL to get the
    /// durable point, and finally start replaying from that point. Records written before
    /// live forwarding was enabled come from the WAL. Later records arrive through the live
    /// receiver, and replay skips any item already covered by the durable prefix. The actor
    /// only performs the small register-and-sync step here; the slower replay runs in the
    /// spawned task.
    async fn on_subscribe(
        &mut self,
        from: crate::fanout::FromCursor,
        live: bool,
        sink: mpsc::Sender<crate::fanout::Delivery>,
    ) -> SubscribeOutcome {
        // The reserved slot reports a failed gate-owner handoff from Drop if the barrier below
        // fails before spawn, allowing another attach in the same tenure to provide the seed.
        let (slot, rx, detach_rx) = self.fanout.prepare_attach();
        // (H) the real writer barrier — fsync everything queued so far, get the durable head.
        let writer = self.writer.as_ref().expect("writer live while open");
        let h = match writer.sync().await {
            Ok(h) => h,
            Err(e) => return SubscribeOutcome::Error(e.to_string()),
        };
        // Spawn the off-actor replay + live-forward task. Live remains separate until replay
        // finds the last control cursor covered by the fsynced attach head.
        let from = if live {
            crate::fanout::AttachFrom::Live
        } else {
            crate::fanout::AttachFrom::Cursor(from)
        };
        self.fanout.spawn_subscriber(
            slot,
            from,
            h,
            rx,
            detach_rx,
            sink,
            self.materialized_store.clone(),
        );
        SubscribeOutcome::Attached
    }

    /// The finish reason of `run_id` when this actor still holds it as the active run but its
    /// `RunFinished` is already durable. Answers from memory, so a re-send stays as cheap as the
    /// plain `active` answer it replaces.
    fn durable_finish_of_active(&self, run_id: &RunId) -> Option<broca_protocol::RunFinishReason> {
        let RunState::Active {
            run_id: active,
            durable_finish,
            ..
        } = &self.state
        else {
            return None;
        };
        if active != run_id {
            return None;
        }
        durable_finish
            .get()
            .filter(|finish| finish.run_id.as_ref().is_none_or(|named| named == active))
            .map(|finish| finish.reason)
    }

    fn status(&self) -> LineageStatus {
        match &self.state {
            RunState::Idle => LineageStatus::Idle,
            RunState::Active {
                run_id, progress, ..
            } => LineageStatus::Active {
                run_id: run_id.clone(),
                step: progress.load(Ordering::Relaxed),
            },
        }
    }

    async fn on_import(&mut self, request: broca_core::ImportRequest) -> ImportOutcome {
        if let Err(error) = self.wal_store.check_move(&self.key, true) {
            return match error.into_move_refusal() {
                Ok(broca_wal::migration::MoveRefusal::Moving { cut_id }) => {
                    ImportOutcome::SessionMoving { cut_id }
                }
                Ok(broca_wal::migration::MoveRefusal::Moved { holder }) => {
                    ImportOutcome::SessionMoved { holder }
                }
                Err(error) => ImportOutcome::Error(broca_core::ImportError::Wal(error.to_string())),
            };
        }
        // Import outcome matrix:
        // - Idle + fresh: run the import once.
        // - Idle + completed retry: the run path returns the same already-imported acknowledgement
        //   and catches up the materialized projection when configured.
        // - Idle + mismatched reuse: the run path returns ImportIdReuse.
        // - Active + fresh: the preflight misses and the request returns Busy.
        // - Active + completed retry: the preflight wins immediately without a materialized store;
        //   with one, it falls through to the run path so projection still catches up.
        // - Active + mismatched reuse: the preflight returns ImportIdReuse.
        let Some(writer) = self.writer.clone() else {
            return ImportOutcome::Error(broca_core::ImportError::Wal(
                "session writer is unavailable".into(),
            ));
        };
        let request = if matches!(self.state, RunState::Idle) {
            request
        } else {
            // The retry preflight reads and decodes the whole WAL. Run it on the blocking pool
            // like every other actor WAL read, so a long session does not hold a runtime worker
            // thread that provider streams, fan-out and other actors are scheduled on.
            let store = self.wal_store.clone();
            let preflight_writer = writer.clone();
            let preflight = spawn_blocking(JobKind::WalRead, move || {
                let result =
                    broca_core::check_import_retry(&request, store.as_ref(), &preflight_writer);
                (request, result)
            })
            .await;
            let (request, result) = match preflight {
                Ok(preflight) => preflight,
                Err(error) => {
                    return ImportOutcome::Error(broca_core::ImportError::Wal(error.to_string()))
                }
            };
            match result {
                Ok(Some(outcome)) if self.materialized_store.is_none() => {
                    return ImportOutcome::Imported(outcome)
                }
                Ok(Some(_)) => {}
                Err(error) => return ImportOutcome::Error(error),
                Ok(None) => return ImportOutcome::Busy,
            }
            request
        };
        match broca_core::run_import_with_store(
            request,
            &writer,
            self.wal_store.as_ref(),
            self.materialized_store.clone(),
        )
        .await
        {
            Ok(outcome) => {
                self.prior_snapshot = None;
                if let Err(error) = self.refresh_admission_view().await {
                    tracing::warn!(
                        target: "admission",
                        "admission view refresh after import failed: {error}"
                    );
                }
                ImportOutcome::Imported(outcome)
            }
            Err(error) => ImportOutcome::Error(error),
        }
    }

    fn finish_restart_waiters(&mut self, result: Result<(), String>) {
        for reply in std::mem::take(&mut self.restart_waiters) {
            let _ = reply.send(result.clone());
        }
    }

    async fn recover_durable_work(&mut self) -> Result<(), String> {
        // A session that is moving or has moved away is not ours to recover. Any other failure
        // to read the move fence (an I/O error) proves nothing about durable work, so it must
        // reach the caller's back-off and recovery gate instead of reading as "nothing to do".
        if let Err(error) = self.wal_store.check_move(&self.key, true) {
            return match error.into_move_refusal() {
                Ok(_) => Ok(()),
                Err(error) => Err(format!("session move check failed: {error}")),
            };
        }
        self.recover_durable_work_inner().await?;
        if self.registry.has_recovery_debt(&self.key) {
            // A readable replay alone cannot discharge a failed fsync. Prove the recovered
            // session durable before clearing only its debt; keep the historical failure event.
            self.writer
                .as_ref()
                .expect("writer live during recovery")
                .sync()
                .await
                .map_err(|error| error.to_string())?;
            self.registry.report_recovered(&self.key);
        }
        Ok(())
    }

    async fn recover_durable_work_inner(&mut self) -> Result<(), String> {
        if !self.pending_cancels.is_empty() && matches!(self.state, RunState::Idle) {
            return self.retry_pending_cancels().await;
        }
        self.refresh_admission_view().await?;
        if self.seal_crash_orphan().await? {
            // The episode now ends in a durable terminal; recover from that view, which
            // backfills its usage export and drains any queued turn exactly as after a seal.
            self.refresh_admission_view().await?;
        }
        let snapshot = self
            .prior_snapshot
            .as_ref()
            .expect("refresh installs an admission snapshot");
        let Some(episode) = snapshot.checkpoint_at_head.last_episode.as_ref() else {
            return self.drain_queue_one().await;
        };
        let episode_disposition = episode.disposition;
        let episode_run_id = episode.run_id.clone();
        let episode_config = episode.config.clone();
        let episode_keep_warm = episode.keep_warm;
        let episode_on_restart = episode.on_restart;
        let episode_pause_reason = episode.pause_reason;
        let episode_input = episode.input.clone();
        let episode_request_identity_digest = episode.request_identity_digest.clone();
        let tail: Vec<WalRecord> = snapshot
            .tail
            .iter()
            .map(|frame| frame.record.clone())
            .collect();
        let tail_resolution = broca_core::replay(&tail)
            .map_err(|error| error.to_string())?
            .resolution;

        // The terminal run index cannot identify a crash that happened before its own update.
        // Reopening the affected lineage closes that earlier WAL-to-index window directly from
        // the authoritative finished episode. Repeated emission is safe by immutable fact_id.
        if episode_disposition == EpisodeDisposition::Finished {
            if self.has_matching_caps() {
                self.pending_capped_export = Some(episode_run_id.clone());
                self.flush_capped_export_barrier().await?;
            } else if let Err(error) = self.emit_usage_export(&episode_run_id).await {
                tracing::warn!(
                    target: "export",
                    "usage export reopen backfill failed (non-fatal): {error}"
                );
            }
            self.approvals.finished(&episode_run_id);
            return self.drain_queue_one().await;
        }

        if episode.cancel_at.is_some() {
            let mut request = submission_from_config(episode_config, episode_input);
            request.keep_warm = episode_keep_warm;
            request.on_restart = episode_on_restart;
            return self
                .start_run(
                    episode_run_id,
                    request,
                    RunKind::FreshOrResume,
                    None,
                    SendIdentity {
                        title: None,
                        conversation_key: None,
                        send_id: None,
                        request_identity_digest: episode_request_identity_digest,
                    },
                    false,
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string());
        }

        if episode_disposition == EpisodeDisposition::Paused
            && episode_pause_reason == Some(broca_protocol::PauseReason::Restart)
            && self.registry.restart_resume_ready()
        {
            let mut request = submission_from_config(episode_config, episode_input);
            request.keep_warm = episode_keep_warm;
            request.on_restart = episode_on_restart;
            // This branch answers parked `ResumeRestart` waiters below, after it knows whether
            // a refused resume was sealed.
            self.restart_recovery_owns_waiters = true;
            let result = self
                .start_run(
                    episode_run_id.clone(),
                    request,
                    RunKind::FreshOrResume,
                    None,
                    SendIdentity {
                        title: None,
                        conversation_key: None,
                        send_id: None,
                        request_identity_digest: episode_request_identity_digest,
                    },
                    false,
                )
                .await;
            self.restart_recovery_owns_waiters = false;
            match result {
                Ok(_) => {
                    self.finish_restart_waiters(Ok(()));
                    tracing::info!(target: "lifecycle", "restart resume: resumed run '{}' in session '{}'", episode_run_id.0, self.key.session);
                    return Ok(());
                }
                Err(StartRunFailure::Shutdown) => {
                    // Sealing, not dependency refusal, interrupted this boot admission. Keep
                    // the active task owned so the normal shutdown path aborts and joins it.
                    self.finish_restart_waiters(Err(
                        "shutdown interrupted restart admission".into()
                    ));
                    return Ok(());
                }
                Err(error) if self.nested_exit_requested || self.writer_poisoned => {
                    // The admission ended because this generation is exiting, not because a
                    // dependency refused the resume. Leave the episode paused for the next
                    // generation's recovery instead of sealing it as refused.
                    let error = error.to_string();
                    self.finish_restart_waiters(Err(error.clone()));
                    return Err(error);
                }
                Err(error) => {
                    let refusal = ProviderError {
                        provider_details: None,
                        provider_subcode: None,
                        refusal: None,
                        class: ErrorClass::Permanent,
                        message: error.to_string(),
                        status: None,
                        retry_after_secs: None,
                        provider_code: Some("restart_resume_refused".into()),
                        resets_at_ms: None,
                        resets_at_source: None,
                        limit_window: None,
                        account_ref: None,
                        credential_id: None,
                        affordable_max_tokens: None,
                    };
                    let sealed = async {
                        self.write_interrupted_seal_with_error(
                            None,
                            SealCause::CrashOrphan,
                            Some(refusal),
                        )
                        .await?;
                        self.refresh_admission_view().await
                    }
                    .await;
                    if let Err(seal_error) = sealed {
                        self.finish_restart_waiters(Err(seal_error.clone()));
                        return Err(seal_error);
                    }
                    tracing::info!(target: "lifecycle", "restart resume: sealed run '{}' interrupted in session '{}': {error}", episode_run_id.0, self.key.session);
                    self.finish_restart_waiters(Ok(()));
                    return self.drain_queue_one().await;
                }
            }
        }

        // An interrupt was queued while this episode ran, and the process died before the
        // episode ended: the interrupt asked for it to stop, so it is sealed rather than resumed,
        // exactly as the startup sweep seals a run the index still called active. Its message
        // then drains first.
        if self
            .pending_fifo
            .first()
            .is_some_and(PendingSubmission::is_interrupt)
            && crash_orphan_sealable(episode_disposition, &tail_resolution)
        {
            if self
                .write_interrupted_seal(None, SealCause::CrashOrphan)
                .await?
            {
                self.refresh_admission_view().await?;
            }
            return self.drain_queue_one().await;
        }

        // Replay may infer LineageIdle from a natural final step or committed hard stop even when
        // the crash lost RunFinished. That inferred end is sufficient for reads, but it is not a
        // durable seal: RunStarted may follow only a synced RunFinished. Recover the original
        // episode first so core writes the missing terminal before any queued append can drain.
        if episode_disposition == EpisodeDisposition::Running
            && matches!(
                tail_resolution,
                broca_core::ReplayResolution::Resume { .. }
                    | broca_core::ReplayResolution::Interrupted { .. }
                    | broca_core::ReplayResolution::LineageIdle { .. }
            )
        {
            let prepared_open = matches!(&tail_resolution, broca_core::ReplayResolution::Resume { pending: Some(pending) }
                if !pending.deferred.is_empty() || !pending.approved.is_empty());
            if prepared_open
                && (self.executor.elicitation_plane().is_none()
                    || episode_on_restart != Some(broca_protocol::request::OnRestart::Resume))
            {
                self.write_interrupted_seal(None, SealCause::CrashOrphan)
                    .await?;
                self.refresh_admission_view().await?;
                return self.drain_queue_one().await;
            }
            // The cache preference is resolved during admission but is not stored in LastEpisodeV1.
            // For keyed on_send retries, the durable request-identity digest is checked before the
            // in-memory identity; a matching digest returns Active immediately, so an unknown cache
            // preference cannot reject an otherwise identical keyed retry.
            //
            // The digest comes from the episode's own `RunStarted`. Only an episode recorded
            // before that field existed asks the run index, which then held the only copy.
            let request_identity_digest = match episode_request_identity_digest {
                Some(digest) => Some(digest),
                None => self
                    .lookup_indexed_run(&episode_run_id)
                    .await?
                    .and_then(|indexed| indexed.request_identity_digest),
            };
            let mut request = submission_from_config(episode_config, episode_input);
            request.keep_warm = episode_keep_warm;
            request.on_restart = episode_on_restart;
            // A re-drive writes no new `RunStarted`, so there is no send_id to record here.
            return self
                .start_run(
                    episode_run_id,
                    request,
                    RunKind::FreshOrResume,
                    None,
                    SendIdentity {
                        title: None,
                        conversation_key: None,
                        send_id: None,
                        request_identity_digest,
                    },
                    false,
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string());
        }

        self.drain_queue_one().await
    }

    /// Start a run (idle), acknowledge an idempotent retry, or durably queue new work. With
    /// a run active, `delivery` decides what the new message does: wait for it as the next turn
    /// (`Queue`), join it at its next step (`Steer`), or replace it (`Interrupt`).
    #[allow(clippy::too_many_arguments)]
    async fn on_send(
        &mut self,
        run_id: RunId,
        mut request: SubmissionRequest,
        kind: RunKind,
        key: SendKey,
        skip_materialization: bool,
        delivery: broca_protocol::Delivery,
        plan: Option<SendPlan>,
        conversation_key: Option<String>,
        title: Option<String>,
        retention: Option<u64>,
        session_class: Option<String>,
        stop_sequences_named: bool,
    ) -> SendOutcome {
        if let Err(error) = self.wal_store.check_move(&self.key, true) {
            return match error.into_move_refusal() {
                Ok(broca_wal::migration::MoveRefusal::Moving { cut_id }) => {
                    SendOutcome::SessionMoving { cut_id }
                }
                Ok(broca_wal::migration::MoveRefusal::Moved { holder }) => {
                    SendOutcome::SessionMoved { holder }
                }
                Err(error) => SendOutcome::Error(error.to_string()),
            };
        }
        self.admitting_retention = None;
        self.admitting_session_class = session_class.clone();
        if retention.is_some() {
            let records = match self.read_wal_framed_records().await {
                Ok(records) => records,
                Err(error) => return SendOutcome::Error(error),
            };
            let current = crate::retention::policy(&records).map(|(seconds, _, _)| seconds);
            let exists = records
                .iter()
                .any(|frame| matches!(frame.record, WalRecord::RunStarted { .. }));
            self.admitting_retention =
                match crate::retention::validate_retention(retention, current, exists) {
                    Ok(update) => update,
                    Err(detail) => return SendOutcome::RetentionInvalid(detail),
                };
        }
        if self.registry.is_stopping() {
            return SendOutcome::Transient("module is stopping".into());
        }
        let send_id = key.send_id().map(str::to_owned);
        let skip_materialization =
            skip_materialization || self.skip_materialization_runs.contains(&run_id);
        if let Err(error) = self.recover_startup_admission_view().await {
            return SendOutcome::Transient(format!(
                "startup durable-work recovery is still pending: {error}"
            ));
        }
        if let Some(class) = session_class.as_deref() {
            let checkpoint = match self.load_checkpoint_at_head().await {
                Ok(checkpoint) => checkpoint,
                Err(error) => return SendOutcome::Error(error),
            };
            let frozen = checkpoint
                .last_episode
                .as_ref()
                .map(|_| checkpoint.session_class.as_deref());
            self.admitting_session_class = match crate::session_class::admit(Some(class), frozen) {
                Ok(crate::session_class::Admitted::Class(class)) => class,
                Ok(crate::session_class::Admitted::IgnoredOnUnlabelled(label)) => {
                    tracing::info!(
                        target: "broca.session_class",
                        session = %self.key.session,
                        label = %label,
                        "session_class ignored: the session was created unlabelled, so its placement stays where it was created"
                    );
                    None
                }
                Err(message) => return SendOutcome::SessionClassInvalid(message),
            };
        }
        if let Some(limit) = request
            .budget
            .as_ref()
            .and_then(|budget| budget.max_input_tokens)
        {
            let checkpoint = match self.load_checkpoint_at_head().await {
                Ok(checkpoint) => checkpoint,
                Err(error) => return SendOutcome::Error(error),
            };
            let Some(budget) = checkpoint.input_budget else {
                return SendOutcome::Error(
                    "admission checkpoint has no session input accounting".into(),
                );
            };
            if let Err(message) = budget.admit(Some(limit)) {
                return SendOutcome::InputTokenBudgetInvalid(message);
            }
        }
        if let Some(structured_output) = &request.structured_output {
            let checkpoint = match self.load_checkpoint_at_head().await {
                Ok(checkpoint) => checkpoint,
                Err(error) => return SendOutcome::Error(error),
            };
            let joins_active_run = delivery == broca_protocol::Delivery::Steer
                && matches!(self.state, RunState::Active { .. });
            if let Err(field) = check_structured_output(
                structured_output,
                &request,
                plan.as_ref(),
                checkpoint
                    .last_episode
                    .as_ref()
                    .map(|episode| &episode.config),
                joins_active_run,
            ) {
                return SendOutcome::StructuredOutputInvalid { field };
            }
        }

        // THE OWNER CHECK COMES FIRST, before a keyed retry is answered: a resend of an
        // auth-paused run under a stamp the session cannot adopt must meet the same refusal
        // every time, not resume the paused run. It reads the session's whole history as of
        // the durable head, which includes an active run's own `RunStarted`.
        //
        // When the send reopens the session's scope at a later epoch, `reopen_refusal` holds the
        // `scope_owner_mismatch` refusal for it. The send is refused with it further down if it
        // turns out to be a retry of a run started under the old epoch, which it cannot resume.
        let mut reopen_refusal = None;
        // A send using the `delivery_order` group from a module other than the session's owner is
        // admitted only if the session's frozen plan names that module as a sender, matched on
        // the daemon-attested principal. Such a sender never becomes the session's principal: the
        // send continues as an internal send under the session's frozen principal, so it takes
        // the frozen scope (its tool routes carry the owner's) and never freezes, adopts or
        // reopens one. The sender itself is recorded only on its `DeliveryOrdered` record.
        self.admitting_sent_by = None;
        // A plan-named sender may also send a legacy string `delivery` (llm-runner 0.5.2 §9.7):
        // `queue` and `steer` stand for the lists `[idle_start]` and `[steer, idle_start]` and
        // are admitted and delivered exactly as those lists, through the ordered path below;
        // `interrupt` is admitted only on the sender's `interrupt` grant. Either way the run it
        // starts or joins keeps the owner's principal and scope.
        if self.admitting_order.is_none() {
            match self.string_sender(&request, delivery).await {
                Ok(StringSender::Owner) => {}
                Ok(StringSender::AsOrder(order)) => self.admitting_order = Some(order),
                Ok(StringSender::Interrupt(owner)) => {
                    self.admitting_sent_by = request.principal.replace(owner);
                    request.scope = broca_protocol::SendScope::Internal;
                }
                Err(outcome) => return outcome,
            }
        }
        if let Some(order) = self.admitting_order.clone() {
            let owner = match self.ordered_sender(&request, &order).await {
                Ok(owner) => owner,
                Err(outcome) => return outcome,
            };
            if let Some(template) = &order.template {
                if let Err(outcome) = self.render_ordered_template(&mut request, template).await {
                    return outcome;
                }
            }
            if let Some(owner) = owner {
                self.admitting_sent_by = request.principal.replace(owner);
                request.scope = broca_protocol::SendScope::Internal;
            }
        }
        if !self.agent_run_writer_enabled() {
            if let Some(scope) = &self.agent_run_scope {
                return SendOutcome::ScopeRefused(
                    broca_core::ScopeRefusal::AgentRunScopesUnsupported {
                        scope: scope.clone(),
                    },
                );
            }
        }
        if self.agent_run_writer_enabled()
            && matches!(&request.scope, broca_protocol::SendScope::Stamped(bind) if bind.scope.run_id.is_some())
        {
            if let Some(field) = broca_core::scope_admission::model_only_scope_tool_field(
                &request.scope,
                request
                    .tools
                    .as_ref()
                    .is_some_and(|tools| !tools.is_empty()),
                plan.as_ref()
                    .and_then(|plan| plan.manifest.as_ref())
                    .is_some_and(|manifest| !manifest.tools.is_empty()),
            ) {
                return SendOutcome::Error(format!("agent-run sends cannot carry {field}"));
            }
        }
        if request.scope != broca_protocol::SendScope::Internal {
            let ownership = match self.load_ownership().await {
                Ok(ownership) => ownership,
                Err(error) => {
                    return SendOutcome::Transient(format!(
                        "cannot read the session's history for the owner check: {error}"
                    ))
                }
            };
            if let Some(first) = &ownership.first_episode {
                let _ = self.first_episode.set(first.clone());
            }
            match self.decide_session_scope(&ownership, request.principal.as_ref(), &request.scope)
            {
                Err(refusal) => return SendOutcome::ScopeRefused(refusal),
                Ok(broca_core::ScopeDecision::Reopen(_)) => {
                    let frozen = ownership
                        .scope
                        .as_ref()
                        .expect("only a scoped session can be reopened");
                    if let Some(outcome) = self
                        .reopen_cannot_start_now(&run_id, delivery, frozen, &request)
                        .await
                    {
                        return outcome;
                    }
                    reopen_refusal = Some(broca_core::scope_admission::reopen_refused(
                        frozen,
                        request.principal.as_ref(),
                        &request.scope,
                    ));
                }
                Ok(broca_core::ScopeDecision::Freeze(_)) => {
                    // A queued send's episode starts after the current run, and only a
                    // `RunStarted` can carry the scope, so it cannot freeze one. This busy
                    // refusal is reached only after the adoption rules passed, so a send that
                    // could never adopt gets its adoption refusal instead of being told to retry.
                    match &self.state {
                        RunState::Active { run_id: active, .. } if *active != run_id => {
                            return SendOutcome::ScopeFreezeBusy {
                                reason: ScopeFreezeBusyReason::RunActive,
                                run_id: Some(active.clone()),
                            }
                        }
                        RunState::Idle if self.has_pending() => {
                            return SendOutcome::ScopeFreezeBusy {
                                reason: ScopeFreezeBusyReason::QueueNonempty,
                                run_id: None,
                            }
                        }
                        _ => {}
                    }
                }
                Ok(_) => {}
            }
        }

        // A plan owns the tool surface for the entire lineage. Reject an explicit later
        // surface before any retry, queue, steer or interrupt path can ignore or freeze it.
        if request.tools.is_some() {
            let planned = match self
                .prior_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.checkpoint_at_head.ownership.as_ref())
            {
                Some(ownership) => ownership
                    .first_episode
                    .as_ref()
                    .is_some_and(|first| first.manifest.is_some()),
                None => match self.load_ownership().await {
                    Ok(ownership) => ownership
                        .first_episode
                        .as_ref()
                        .is_some_and(|first| first.manifest.is_some()),
                    Err(error) => return SendOutcome::Transient(error),
                },
            };
            if self
                .first_episode
                .get()
                .is_some_and(|first| first.manifest.is_some())
                || planned
            {
                return SendOutcome::DeliveryRefused {
                    code: "invalid_params",
                    field: "tools",
                    message: "tools are frozen by the session's fetch plan".into(),
                };
            }
        }

        // A send carrying a plan meets the session's frozen plan before its key is looked up, so
        // a retry under the same key with another plan is refused by name like any later send.
        // The identical plan changes nothing: the send goes on as an ordinary later send.
        let carries_plan = plan.is_some();
        let deferred_model_check = plan.as_ref().and_then(|plan| plan.model_check.clone());
        if let Some(plan) = plan {
            let ownership = match self.load_ownership().await {
                Ok(ownership) => ownership,
                Err(error) => {
                    return SendOutcome::Transient(format!(
                        "cannot read the session's history for its frozen plan: {error}"
                    ))
                }
            };
            if ownership.episodes == 0 {
                match plan.manifest {
                    Some(manifest) => self.admitting_manifest = Some(manifest),
                    // The serve layer saw episodes when it checked the plan, so it fetched nothing,
                    // yet the history read here has none: the two reads disagree, and the caller
                    // retries the send so the plan is checked against one history.
                    None => {
                        return SendOutcome::Transient(
                            "the session's history changed while its plan was checked; retry \
                             the send"
                                .into(),
                        )
                    }
                }
            } else {
                let frozen = ownership
                    .first_episode
                    .as_ref()
                    .and_then(|first| first.manifest.as_ref())
                    .map(|manifest| manifest.plan_identity.as_str());
                if let Err(refusal) = broca_fetch_plan::check_later_plan(frozen, &plan.identity) {
                    return SendOutcome::PlanRefused(refusal);
                }
            }
        }

        // The running loop can deliver an ordered send while this actor stays active. Drop
        // those stale queue entries before a retry can incorrectly answer pending again.
        if let RunState::Active { steer, .. } = &self.state {
            let consumed = steer.take_consumed();
            if !consumed.is_empty() {
                self.pending_fifo
                    .retain(|pending| !consumed.contains(&pending.submission_id));
                self.wal_scan_complete = false;
            }
        }
        // Nonce-minted sends can never repeat and deliberately pay no digest or lookup. A keyed
        // send first asks whether an episode already started under its run id. The WAL answers
        // that: `RunStarted` records the send's identity digest, and the actor keeps what the WAL
        // records for every run id. The run index is only a cache here. Its terminal state
        // saves a WAL reconcile, and its digest stands in only for an episode recorded before
        // `RunStarted` carried one. A lost or stale index row therefore cannot change the
        // answer. Only a send whose run id has no started episode follows the ordinary admission path.
        let request_identity_digest = key
            .is_keyed()
            .then(|| send_identity_digest(&request, delivery, conversation_key.as_deref()));
        // The delivery order is part of a keyed send's identity: a retry that asks for another
        // order (an ordered send retried as a legacy one, or the reverse) is refused by name.
        if key.is_keyed() {
            match self.send_order_differs(&run_id).await {
                Ok(true) => {
                    return SendOutcome::SendIdReuse {
                        differing: "delivery",
                    }
                }
                Ok(false) => {}
                Err(error) => return SendOutcome::Error(error),
            }
        }
        // A key already used by a steer is answered from that steer's record: a steer opens no
        // episode of its own, so the run-id lookup below cannot find it. This also refuses a
        // later send of another mode under the same key, naming `delivery`.
        if let (Some(send_id), Some(request_digest)) =
            (key.send_id(), request_identity_digest.as_deref())
        {
            match self.recorded_steer(send_id).await {
                Ok(Some(steer)) => {
                    return self
                        .answer_steer_retry(
                            send_id,
                            steer,
                            request_digest,
                            &request,
                            delivery,
                            conversation_key.as_deref(),
                        )
                        .await
                }
                Ok(None) => {}
                Err(error) => return SendOutcome::Error(error),
            }
        }
        let mut resume_indexed_run = false;
        if let Some(request_digest) = request_identity_digest.as_deref() {
            let recorded = match self.recorded_send_key(&run_id).await {
                Ok(recorded) => recorded,
                Err(error) => return SendOutcome::Error(error),
            };
            let indexed = match self.lookup_indexed_run(&run_id).await {
                Ok(indexed) => indexed,
                Err(error) => return SendOutcome::Error(error),
            };
            // A key with only a `Queued` record and no started episode is answered further down
            // by the queued-send checks: `queue_submission` returns Pending for the same payload
            // and refuses a different one, and `submission_was_retracted` reports a retraction.
            let started = recorded.as_ref().is_some_and(|recorded| recorded.started);
            if started || indexed.is_some() {
                let recorded_digest =
                    recorded.and_then(|recorded| recorded.request_identity_digest);
                let indexed_digest = indexed
                    .as_ref()
                    .and_then(|indexed| indexed.request_identity_digest.clone());
                let Some(stored_digest) = recorded_digest.or(indexed_digest) else {
                    // Neither the WAL nor a legacy index row can prove that the payload is the
                    // same request. Failing loud avoids both re-executing a possibly completed
                    // send and silently accepting a different payload under its key.
                    return SendOutcome::SendIdReuse {
                        differing:
                            "request identity because the legacy run-index row has no digest",
                    };
                };
                if stored_digest != request_digest {
                    let differing = self
                        .admitted_request_identities
                        .get(&run_id)
                        .and_then(|identity| identity.request_identity_digest.as_deref())
                        .and_then(|digest| {
                            send_identity_difference(
                                digest,
                                &request,
                                delivery,
                                conversation_key.as_deref(),
                            )
                        })
                        .or_else(|| {
                            send_identity_difference(
                                &stored_digest,
                                &request,
                                delivery,
                                conversation_key.as_deref(),
                            )
                        })
                        .unwrap_or("request identity");
                    return SendOutcome::SendIdReuse { differing };
                }
                if started {
                    let records = match self.read_wal_records().await {
                        Ok(records) => records,
                        Err(error) => return SendOutcome::Error(error),
                    };
                    if let Some(outcome) = crate::send_fate::recorded_send_outcome(
                        &records,
                        &run_id,
                        key.send_id().unwrap_or(""),
                    ) {
                        // An auth-paused re-send is still the explicit re-drive operation;
                        // polling never takes that branch or starts its executor.
                        if !matches!(
                            outcome,
                            SendOutcome::Paused {
                                reason: broca_protocol::PauseReason::AuthRequired,
                                ..
                            }
                        ) {
                            return outcome;
                        }
                    }
                }
                // An ordered send's own run is its `idle_start` delivery, so a retry of it carries
                // the `turn` receipt naming that run, as a retry of one delivered into a running
                // turn carries `step`. The order comes from the WAL, so this holds after a restart.
                let delivered = self
                    .ordered_submissions
                    .contains_key(&broca_protocol::SubmissionId(format!("sub-{}", run_id.0)))
                    .then(|| Delivered::new(delivered_as::TURN).with_ref(run_id.0.clone()));
                if let Some(reason) = indexed
                    .as_ref()
                    .and_then(|indexed| indexed_finish_reason(&indexed.state))
                {
                    return SendOutcome::Finished {
                        run_id,
                        reason,
                        delivered,
                    };
                }
                if !self.restart_admitting
                    && matches!(&self.state, RunState::Active { run_id: active, .. } if *active == run_id)
                {
                    // A live retry stays active and never enters the queue. Once the run's
                    // terminal is durable it has ended, whatever the actor has processed so far:
                    // `run.result` already reports it from the WAL, so this answer must agree.
                    if let Some(reason) = self.durable_finish_of_active(&run_id) {
                        return SendOutcome::Finished {
                            run_id,
                            reason,
                            delivered,
                        };
                    }
                    return SendOutcome::Active { run_id, delivered };
                }
                match self.reconcile_from_wal(&run_id).await {
                    Ok(Some(state)) if state.state.is_terminal() => {
                        // Active/paused index rows can be stale after a crash. Reconciliation is
                        // permitted only on this index-hit path and prevents an older completed
                        // run from reaching core's latest-episode auto-append arm.
                        return SendOutcome::Finished {
                            run_id,
                            reason: indexed_finish_reason(&state.state)
                                .expect("terminal index state has a finish reason"),
                            delivered,
                        };
                    }
                    Ok(Some(state))
                        if state.pause_reason == Some(broca_protocol::PauseReason::Restart) =>
                    {
                        return SendOutcome::Paused {
                            run_id,
                            reason: broca_protocol::PauseReason::Restart,
                            delivered,
                        };
                    }
                    Ok(Some(_)) => {
                        // The caller is retrying the unfinished durable run itself. Resume it
                        // ahead of later queued turns; appending those turns is illegal until
                        // this episode reaches a terminal.
                        resume_indexed_run = true;
                    }
                    Ok(None) => {
                        // Only an index row without any WAL record for its run id gets here.
                        return SendOutcome::Error(
                            "run index contains the send id but its WAL identity is missing".into(),
                        );
                    }
                    Err(error) => return SendOutcome::Error(error),
                }
                // Active/paused without a live task falls through to the existing same-run
                // re-drive. Core reconciles the WAL and either resumes or reports the no-op.
            }
        }
        if resume_indexed_run {
            // The re-driven episode keeps the scope its own `RunStarted` recorded, under the
            // epoch that ended, so a send presenting the later epoch cannot resume it.
            if let Some(refusal) = reopen_refusal {
                return SendOutcome::ScopeRefused(refusal);
            }
        }

        // Identity is compared on the CALLER's payload (an omitted tool surface stays
        // `None` here), so an identical omitted retry is idempotent while an explicit
        // surface differing from the original omission is a loud reuse error.
        if let Some(differing) = self
            .admitted_request_identities
            .get(&run_id)
            .and_then(|identity| identity.request_identity_digest.as_deref())
            .and_then(|digest| {
                send_identity_difference(digest, &request, delivery, conversation_key.as_deref())
            })
        {
            return SendOutcome::SendIdReuse { differing };
        }
        if let RunState::Active {
            run_id: active_id, ..
        } = &self.state
        {
            if run_id == *active_id {
                // As above: a run whose terminal is already durable is answered as finished.
                if let Some(reason) = self.durable_finish_of_active(&run_id) {
                    return SendOutcome::Finished {
                        run_id,
                        reason,
                        delivered: None,
                    };
                }
                return SendOutcome::Active {
                    run_id: active_id.clone(),
                    delivered: None,
                };
            }
        }

        // An existing keyed run keeps its recorded answer, and an unfinished one keeps the
        // configuration it was admitted with. Only a send that declares new tools needs a plan:
        // on a scoped session, tool providers require a per-call preset, and presets come only
        // from a plan, never from a legacy tool list. Check before queueing, steering or starting
        // work, since each of these can freeze the tool list into a new episode.
        if !resume_indexed_run
            && !carries_plan
            && request
                .tools
                .as_ref()
                .is_some_and(|tools| !tools.is_empty())
        {
            let ownership = match self.load_ownership().await {
                Ok(ownership) => ownership,
                Err(error) => {
                    return SendOutcome::Transient(format!(
                        "cannot read the session's history for its tool plan: {error}"
                    ))
                }
            };
            let scoped = ownership.scope.is_some()
                || matches!(&request.scope, broca_protocol::SendScope::Stamped(_));
            let planned = ownership
                .first_episode
                .as_ref()
                .is_some_and(|first| first.manifest.is_some());
            if scoped && !planned {
                return SendOutcome::PlanRefused(broca_fetch_plan::Refusal::ScopedToolsRequirePlan);
            }
        }

        let identity = SendIdentity {
            conversation_key,
            // A retry that resumes its own unfinished episode writes no `RunStarted`; that episode
            // keeps the title its first send recorded, whatever title the retry carries.
            title: if resume_indexed_run { None } else { title },
            send_id,
            request_identity_digest,
        };
        if identity.request_identity_digest.is_some() {
            let submission_id = broca_protocol::SubmissionId(format!("sub-{}", run_id.0));
            let already_pending = self
                .pending_fifo
                .iter()
                .any(|pending| pending.submission_id == submission_id);
            if already_pending {
                // A steer may have fallen back to the queue. Answer that durable queue entry
                // before entering any delivery handler (and before asking a hook again).
                return self
                    .queue_submission(run_id, request, identity, skip_materialization, delivery)
                    .await;
            }
            if !already_pending {
                match self.submission_was_retracted(&submission_id).await {
                    Ok(true) => return SendOutcome::Retracted { submission_id },
                    Ok(false) => {}
                    Err(error) => return SendOutcome::Error(error),
                }
            }
        }

        // A send that left its model to the session is checked against the plan's model-bearing
        // items here, with the model it inherits: the latest episode's frozen model, which
        // `record_frozen_model` and run start also take. It runs after the same-key receipts
        // above, so a retry of an admitted send keeps its answer even if a later episode
        // changed the session's model.
        if let Some(plan) = deferred_model_check {
            let checkpoint = match self.load_checkpoint_at_head().await {
                Ok(checkpoint) => checkpoint,
                Err(error) => return SendOutcome::Transient(error),
            };
            // With no episode there is no model to inherit; admission refuses the send for its
            // missing model further on.
            if let Some(episode) = &checkpoint.last_episode {
                if let Err(refusal) = broca_fetch_plan::admit::check_plan_model(
                    &plan,
                    episode.config.model.provider_module_id.as_str(),
                    &episode.config.model.model_id,
                    episode.config.variant.as_deref(),
                ) {
                    return SendOutcome::PlanRefused(refusal);
                }
            }
        }

        // Same-key receipts above remain idempotent even when later work filled the context.
        // Only compaction sessions need an additional admission read for a joining send.
        let has_compaction = self
            .first_episode
            .get()
            .and_then(|first| first.manifest.as_ref())
            .is_some_and(|manifest| broca_core::compaction::has_compaction(manifest));
        if has_compaction && self.compaction_watch.pending() {
            self.watch_compaction_session_size().await;
        }
        if !resume_indexed_run
            && has_compaction
            && (matches!(self.state, RunState::Idle) || delivery != broca_protocol::Delivery::Queue)
        {
            let checkpoint = match self.load_checkpoint_at_head().await {
                Ok(checkpoint) => checkpoint,
                Err(error) => return SendOutcome::Transient(error),
            };
            let mut effective = request.clone();
            if let Some(episode) = &checkpoint.last_episode {
                inherit_frozen_parameters(&mut effective, &episode.config);
            }
            if let Some(detail) = checkpoint
                .compaction
                .as_ref()
                .and_then(|c| c.state())
                .and_then(|state| {
                    state.send_ceiling(
                        &effective.input,
                        effective.context_limit.or_else(|| {
                            checkpoint
                                .last_episode
                                .as_ref()
                                .filter(|episode| episode.config.model == effective.model)
                                .and_then(|episode| episode.config.context_limit)
                        }),
                        &effective.model,
                    )
                })
            {
                return SendOutcome::ContextCeiling(detail);
            }
        }
        // A send using the `delivery_order` group is always queued with its order, and delivered
        // at the first request boundary where one of its modes is possible.
        if !resume_indexed_run {
            if let Some(order) = self.admitting_order.clone() {
                return self
                    .queue_ordered_submission(
                        run_id,
                        request,
                        identity,
                        skip_materialization,
                        order,
                    )
                    .await;
            }
        }
        // A retry of a send whose own episode is unfinished resumes that episode, whatever mode
        // it was sent with: its message is already durable and must not be steered or queued
        // a second time.
        if !resume_indexed_run {
            match delivery {
                broca_protocol::Delivery::Queue => {
                    // A paused session is not idle, although no run is in flight. Admitted as a
                    // run, a new message would only re-drive the paused episode (which renders
                    // nothing until its pause is resolved) and its input would be recorded
                    // nowhere; queued, it would wait behind a run nothing is going to end. So
                    // it is refused before anything is written, like a steer. The paused run's
                    // own retry has this run id and is not refused: it resumes the run.
                    if matches!(self.state, RunState::Idle) {
                        match self.paused_run().await {
                            Ok(Some((paused, reason)))
                                if paused != run_id
                                    && reason == broca_protocol::PauseReason::AuthRequired =>
                            {
                                return SendOutcome::RunPaused {
                                    run_id: paused,
                                    reason,
                                }
                            }
                            Ok(_) => {}
                            Err(error) => return SendOutcome::Transient(error),
                        }
                    }
                }
                broca_protocol::Delivery::Steer => {
                    return self
                        .steer_submission(
                            run_id,
                            request,
                            kind,
                            identity,
                            skip_materialization,
                            stop_sequences_named,
                        )
                        .await
                }
                broca_protocol::Delivery::Interrupt => {
                    return self
                        .interrupt_submission(run_id, request, kind, identity, skip_materialization)
                        .await
                }
            }
        }
        self.send_plain(
            run_id,
            request,
            kind,
            identity,
            skip_materialization,
            resume_indexed_run,
            broca_protocol::Delivery::Queue,
        )
        .await
    }

    /// A send with no run to steer into or interrupt: start it now when the lineage is idle,
    /// or durably queue it (as `delivery`) behind the active run or the queued work.
    #[allow(clippy::too_many_arguments)]
    async fn send_plain(
        &mut self,
        run_id: RunId,
        request: SubmissionRequest,
        kind: RunKind,
        identity: SendIdentity,
        skip_materialization: bool,
        resume_indexed_run: bool,
        delivery: broca_protocol::Delivery,
    ) -> SendOutcome {
        if matches!(self.state, RunState::Idle) && !resume_indexed_run {
            match self.paused_run().await {
                Ok(Some((_, broca_protocol::PauseReason::Restart))) => {
                    return self
                        .queue_submission(run_id, request, identity, skip_materialization, delivery)
                        .await
                }
                Ok(_) => {}
                Err(error) => return SendOutcome::Error(error),
            }
        }
        match &self.state {
            RunState::Idle if self.has_pending() && !resume_indexed_run => {
                self.queue_submission(run_id, request, identity, skip_materialization, delivery)
                    .await
            }
            RunState::Idle => match self
                .start_run_with_cancel(
                    run_id,
                    request,
                    kind,
                    None,
                    identity,
                    StartRunFlags {
                        skip_materialization,
                        cancel_before_execute: false,
                        input_delivery: delivery,
                    },
                )
                .await
            {
                Ok(run_id) => SendOutcome::Active {
                    run_id,
                    delivered: None,
                },
                Err(StartRunFailure::Expired) => SendOutcome::Reopen,
                Err(StartRunFailure::ContextCeiling(detail)) => SendOutcome::ContextCeiling(detail),
                Err(StartRunFailure::Admission(RunError::Transient(message))) => {
                    SendOutcome::Transient(message)
                }
                Err(StartRunFailure::Admission(RunError::SendIdReuse { differing })) => {
                    SendOutcome::SendIdReuse { differing }
                }
                Err(StartRunFailure::Admission(RunError::BudgetDenied { cap_id, reason })) => {
                    SendOutcome::BudgetDenied { cap_id, reason }
                }
                Err(StartRunFailure::Admission(RunError::VariantUnsupported(e))) => {
                    SendOutcome::VariantUnsupported(e)
                }
                Err(StartRunFailure::Admission(RunError::AccountSelectionRefused(refusal))) => {
                    SendOutcome::AccountSelectionRefused(refusal)
                }
                Err(StartRunFailure::Admission(RunError::SessionTooLarge { max_bytes })) => {
                    SendOutcome::SessionTooLarge { max_bytes }
                }
                Err(StartRunFailure::Admission(RunError::RetainedSessionTooLarge {
                    max_bytes,
                })) => SendOutcome::RetainedSessionTooLarge { max_bytes },
                Err(StartRunFailure::Admission(RunError::NewerWriter { missing })) => {
                    // The same text the generic arm below renders for this refusal.
                    let reason = RunError::NewerWriter {
                        missing: missing.clone(),
                    }
                    .to_string();
                    SendOutcome::NewerWriter { missing, reason }
                }
                Err(StartRunFailure::Scope(refusal)) => SendOutcome::ScopeRefused(refusal),
                Err(StartRunFailure::Admission(error)) => SendOutcome::ClassifiedError {
                    class: error.admission_error_class(),
                    reason: error.to_string(),
                },
                // Infrastructure strings include replay failures, so retryability is unknown.
                Err(error) => SendOutcome::Error(error.to_string()),
            },
            RunState::Active { .. } => {
                self.queue_submission(run_id, request, identity, skip_materialization, delivery)
                    .await
            }
        }
    }

    /// The run a paused session is waiting in, when the session's last episode is paused. The
    /// actor is idle during a pause, so this is how a steer or an interrupt tells a paused
    /// session from one with no run at all.
    async fn paused_run(&mut self) -> Result<Option<(RunId, broca_protocol::PauseReason)>, String> {
        if self.prior_snapshot.is_none() {
            self.refresh_admission_view().await?;
        }
        Ok(self
            .prior_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.checkpoint_at_head.last_episode.as_ref())
            .filter(|episode| episode.disposition == EpisodeDisposition::Paused)
            // Default older checkpoints to auth, the only pause reason they could record.
            .map(|episode| {
                (
                    episode.run_id.clone(),
                    episode
                        .pause_reason
                        .unwrap_or(broca_protocol::PauseReason::AuthRequired),
                )
            }))
    }

    /// Why a send that reopens the session's scope at a later epoch cannot be admitted now,
    /// or `None` when it opens its own episode at once: the session is idle, nothing is queued
    /// and no run is paused. Only that episode's `RunStarted` can record the new epoch, so
    /// every other path is refused and nothing is written:
    ///
    /// - A steer would join a run (active, or paused) whose tool routes belong to the epoch
    ///   that ended, and a retry of that run's own send would re-drive it under that epoch.
    ///   Both are refused `scope_owner_mismatch` naming the frozen scope, as any send under
    ///   another epoch was before reopening existed.
    /// - A queued send or an INTERRUPT is refused `scope_freeze_busy`, retryable once the
    ///   session is idle, even though an interrupt otherwise always goes through. Both would
    ///   wait in the queue, and a `Queued` record keeps no scope, so the drained episode would
    ///   run under the old epoch. A first-time freeze waits for the same reason.
    ///
    /// `frozen` is the session's frozen scope, which the refusal names.
    async fn reopen_cannot_start_now(
        &mut self,
        run_id: &RunId,
        delivery: broca_protocol::Delivery,
        frozen: &broca_protocol::SessionScope,
        request: &SubmissionRequest,
    ) -> Option<SendOutcome> {
        let joins_a_run = delivery == broca_protocol::Delivery::Steer;
        let refused = || {
            SendOutcome::ScopeRefused(broca_core::scope_admission::reopen_refused(
                frozen,
                request.principal.as_ref(),
                &request.scope,
            ))
        };
        let busy_with = |run: &RunId| SendOutcome::ScopeFreezeBusy {
            reason: ScopeFreezeBusyReason::RunActive,
            run_id: Some(run.clone()),
        };
        let unfinished = match &self.state {
            // A run whose terminal is already durable has ended; the actor learns it a moment
            // later. The session is about to be idle, so the send is told to retry.
            RunState::Active { run_id: active, .. }
                if active != run_id && self.durable_finish_of_active(active).is_some() =>
            {
                return Some(busy_with(active));
            }
            RunState::Active { run_id: active, .. } => Some(active.clone()),
            RunState::Idle => match self.paused_run().await {
                Ok(paused) => paused.map(|(paused, _)| paused),
                Err(error) => return Some(SendOutcome::Transient(error)),
            },
        };
        if let Some(unfinished) = unfinished {
            if joins_a_run || unfinished == *run_id {
                return Some(refused());
            }
            return Some(busy_with(&unfinished));
        }
        if self.has_pending() {
            return Some(SendOutcome::ScopeFreezeBusy {
                reason: ScopeFreezeBusyReason::QueueNonempty,
                run_id: None,
            });
        }
        None
    }

    /// `delivery: steer`: add the message to the running turn at its next step boundary.
    ///
    /// The `Steered` record is appended under the run's inbox lock (see `broca_core::steer`)
    /// and synced before the reply, which names the running run: the message is then durably in
    /// the transcript. With no run active this is a plain send. A restart-paused run accepts
    /// durable steers for its next step; other pauses refuse messages they cannot render.
    async fn steer_submission(
        &mut self,
        run_id: RunId,
        request: SubmissionRequest,
        kind: RunKind,
        identity: SendIdentity,
        skip_materialization: bool,
        stop_sequences_named: bool,
    ) -> SendOutcome {
        let RunState::Active {
            run_id: active,
            steer,
            ..
        } = &self.state
        else {
            match self.paused_run().await {
                Ok(Some((paused, broca_protocol::PauseReason::Restart))) => {
                    let input = request
                        .input
                        .iter()
                        .filter(|message| message.role != broca_protocol::content::Role::System)
                        .cloned()
                        .collect::<Vec<_>>();
                    let manifest = match self.frozen_input_manifest().await {
                        Ok(manifest) => manifest,
                        Err(outcome) => return outcome,
                    };
                    let prepared = match self
                        .prepare_input(input, broca_protocol::Delivery::Steer, manifest)
                        .await
                    {
                        Ok(prepared) => prepared,
                        Err(outcome) => return outcome,
                    };
                    if let Some(provider) = prepared.refused {
                        return SendOutcome::PreUserUnavailable { provider };
                    }
                    // A writer reopened for a paused session has not been seeded by a run yet;
                    // the steer's message needs its runner tags from the lineage's state.
                    if let Err(error) = self.seed_writer_tags().await {
                        return SendOutcome::Transient(error);
                    }
                    let writer = self.writer.as_ref().expect("writer live while paused");
                    if let Some(limit) = request
                        .budget
                        .as_ref()
                        .and_then(|budget| budget.max_input_tokens)
                    {
                        writer.record(WalRecord::InputTokenCapFrozen { limit });
                    }
                    writer.record(WalRecord::Steered {
                        tags: None,
                        retention: self.admitting_retention,
                        ts_ms: self.retention_timestamp(),
                        send_id: identity.send_id,
                        request_identity_digest: identity.request_identity_digest,
                        title: identity.title,
                        delivery: Some(broca_protocol::Delivery::Steer),
                        input: Arc::new(prepared.input),
                        hooks: prepared.hooks,
                    });
                    if let Err(error) = writer.sync().await {
                        return SendOutcome::Error(error.to_string());
                    }
                    self.wal_scan_complete = false;
                    self.prior_snapshot = None;
                    return SendOutcome::Paused {
                        run_id: paused,
                        reason: broca_protocol::PauseReason::Restart,
                        delivered: None,
                    };
                }
                Ok(Some((run_id, reason))) => return SendOutcome::RunPaused { run_id, reason },
                Ok(None) => {}
                Err(error) => return SendOutcome::Transient(error),
            }
            return self
                .send_plain(
                    run_id,
                    request,
                    kind,
                    identity,
                    skip_materialization,
                    false,
                    broca_protocol::Delivery::Steer,
                )
                .await;
        };
        let active = active.clone();
        let inbox = Arc::clone(steer);
        if !inbox.is_closed() {
            // Read the admitted episode, not the snapshot from before it started: a resumed
            // run may also have ignored the settings carried by its resuming send.
            let checkpoint = match self.load_checkpoint_at_head().await {
                Ok(checkpoint) => checkpoint,
                Err(error) => return SendOutcome::Transient(error),
            };
            // The running turn's admission seeded the writer; this only fills a writer that
            // reopened without one, from the same history.
            self.writer
                .as_ref()
                .expect("writer live while open")
                .seed_lineage_tags(checkpoint.tags);
            let Some(episode) = checkpoint
                .last_episode
                .filter(|episode| episode.run_id == active)
            else {
                return SendOutcome::Transient(
                    "the active run has no frozen episode config".into(),
                );
            };
            if let Some(field) =
                steer_settings_conflict(&request, &episode.config, stop_sequences_named)
            {
                // The inbox lock linearizes this refusal against the run's decision to end.
                // A closed inbox sends the message to a new turn instead, which can honour it.
                if !inbox.is_closed() {
                    return SendOutcome::SteerSettingsConflict { field };
                }
            }
        }
        // The running turn already has its system prompt; a steer adds conversation only.
        let input: Vec<broca_protocol::Message> = request
            .input
            .iter()
            .filter(|message| message.role != broca_protocol::content::Role::System)
            .cloned()
            .collect();
        let manifest = match self.frozen_input_manifest().await {
            Ok(manifest) => manifest,
            Err(outcome) => return outcome,
        };
        let prepared = match self
            .prepare_input(input, broca_protocol::Delivery::Steer, manifest)
            .await
        {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        if let Some(provider) = &prepared.refused {
            return SendOutcome::PreUserUnavailable {
                provider: provider.clone(),
            };
        }
        let input = prepared.input.clone();
        let writer = Arc::clone(self.writer.as_ref().expect("writer live while open"));
        let record = WalRecord::Steered {
            tags: None,
            retention: self.admitting_retention,
            ts_ms: self.retention_timestamp(),
            send_id: identity.send_id.clone(),
            request_identity_digest: identity.request_identity_digest.clone(),
            title: identity.title.clone(),
            delivery: Some(broca_protocol::Delivery::Steer),
            input: Arc::new(input.clone()),
            hooks: prepared.hooks.clone(),
        };
        if !inbox.deliver(input, || {
            if let Some(limit) = request
                .budget
                .as_ref()
                .and_then(|budget| budget.max_input_tokens)
            {
                writer.record(WalRecord::InputTokenCapFrozen { limit });
            }
            writer.record_tagged(record).1
        }) {
            // The run has decided it renders no further step and is writing its terminal: there
            // is no running turn left to join, so the message waits for the next turn instead.
            return self
                .queue_prepared_submission(
                    run_id,
                    request,
                    identity,
                    skip_materialization,
                    broca_protocol::Delivery::Steer,
                    Some(prepared),
                )
                .await;
        }
        if let Err(error) = writer.sync().await {
            return SendOutcome::Error(error.to_string());
        }
        if self.wal_scan_complete {
            if let Some(send_id) = identity.send_id {
                self.recorded_steers
                    .entry(send_id)
                    .or_insert_with(|| RecordedSteer {
                        run_id: Some(active.clone()),
                        request_identity_digest: identity.request_identity_digest,
                    });
            }
        }
        SendOutcome::Active {
            run_id: active,
            delivered: None,
        }
    }

    /// `delivery: interrupt`: stop the running turn and start this message as the next turn.
    ///
    /// THE ORDER IS THE GUARANTEE. The message is made durable first (a `Queued` record
    /// carrying `delivery: interrupt`, synced), and only then is the running turn cancelled, so
    /// a crash at any point leaves the message either not yet acknowledged or queued: never
    /// lost, and never run twice. The cancelled run ends `Cancelled` (or `Interrupted`, sealed
    /// by the startup sweep, if the process dies first), and the queue then drains the
    /// interrupt ahead of every plain entry. A paused run is ended the same way; with no run
    /// at all this is a plain send.
    async fn interrupt_submission(
        &mut self,
        run_id: RunId,
        request: SubmissionRequest,
        kind: RunKind,
        identity: SendIdentity,
        skip_materialization: bool,
    ) -> SendOutcome {
        const INTERRUPT: broca_protocol::Delivery = broca_protocol::Delivery::Interrupt;
        if let RunState::Active { cancel, .. } = &self.state {
            let cancel = cancel.clone();
            let outcome = self
                .queue_submission(run_id, request, identity, skip_materialization, INTERRUPT)
                .await;
            if matches!(outcome, SendOutcome::Pending { .. }) {
                cancel.cancel();
            }
            return outcome;
        }
        let paused = match self.paused_run().await {
            Ok(paused) => paused,
            Err(error) => return SendOutcome::Transient(error),
        };
        let Some((paused, _)) = paused else {
            return self
                .send_plain(
                    run_id,
                    request,
                    kind,
                    identity,
                    skip_materialization,
                    false,
                    INTERRUPT,
                )
                .await;
        };
        let outcome = self
            .queue_submission(run_id, request, identity, skip_materialization, INTERRUPT)
            .await;
        if matches!(outcome, SendOutcome::Pending { .. }) {
            self.end_paused_run_for_interrupt(&paused).await;
        }
        outcome
    }

    /// End a paused run because an interrupt is queued behind it: the existing cancel path
    /// seals it `Cancelled`, and its terminal then drains the interrupt. A failure to start the
    /// finalizer leaves both durable; the next drain attempt retries it.
    async fn end_paused_run_for_interrupt(&mut self, paused: &RunId) {
        let skip_materialization = self.skip_materialization_runs.contains(paused);
        if let Err(error) = self
            .start_paused_cancel_finalizer(paused, skip_materialization)
            .await
        {
            tracing::warn!(
                target: "lifecycle",
                "interrupt could not end the paused run yet: {error}"
            );
        }
    }

    /// Answer a send whose key the WAL records as a steer. The same steer again is answered by
    /// the run it joined: `active` while that run is live, its `finished` once it has ended,
    /// with its delivery receipt once a model step has rendered it. It never starts a turn. Any
    /// other payload or mode under the key is refused by field name.
    async fn answer_steer_retry(
        &mut self,
        send_id: &str,
        steer: RecordedSteer,
        request_digest: &str,
        request: &SubmissionRequest,
        delivery: broca_protocol::Delivery,
        conversation_key: Option<&str>,
    ) -> SendOutcome {
        let Some(stored) = steer.request_identity_digest else {
            return SendOutcome::SendIdReuse {
                differing: "request identity because the steer record has no digest",
            };
        };
        if stored != request_digest {
            return SendOutcome::SendIdReuse {
                differing: send_identity_difference(&stored, request, delivery, conversation_key)
                    .unwrap_or("request identity"),
            };
        }
        let Some(target) = steer.run_id else {
            return SendOutcome::Error("the WAL records a steer before any run".into());
        };
        // The receipt is read from the WAL up to its durable head, never from what this actor
        // holds: a restarted broca reads the same records and so gives the same answer, and a
        // record that a crash could still lose never makes a receipt it would later contradict.
        // A receipt absent here only means no durable step had rendered the steer when the WAL
        // was read; a later retry reads it again. The run loop appends `StepStarted` without
        // an fsync, so syncing first lets a step that has already started count, rather than
        // waiting for the run's next sync point, which a long model call can hold off.
        let writer = Arc::clone(self.writer.as_ref().expect("writer live while open"));
        if let Err(error) = writer.sync().await {
            return SendOutcome::Error(error.to_string());
        }
        let records = match self.read_wal_records().await {
            Ok(records) => records,
            Err(error) => return SendOutcome::Error(error),
        };
        crate::send_fate::recorded_send_outcome(&records, &target, send_id).unwrap_or_else(|| {
            SendOutcome::Error("the recorded steer disappeared from the WAL".into())
        })
    }

    fn check_budget_admission(&self) -> Result<(), RunError> {
        let Some(cap_store) = self.cap_store.as_ref() else {
            return Ok(());
        };
        // This mirror lookup is the entire common-case path: no SQLite, WAL, or
        // executor work occurs when no cap matches the daemon principal.
        let caps = cap_store.matching_caps(&self.key.harness, &self.key.project_root);
        if caps.is_empty() {
            return Ok(());
        }
        let export_position = self.export_index.as_deref().map(|index| {
            (
                index.source_instance_id(),
                index.source_epoch(),
                index.high_water(),
            )
        });
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or(i64::MAX);
        for cap in caps {
            let denial = if cap.spend_nanos >= cap.hard_cap_nanos {
                Some(BudgetDenial::HardCapExceeded)
            // Upgrade safety: persisted negative counts are impossible accounting states, not
            // proof that all spend resolved. Any nonzero value must therefore fail closed.
            } else if cap.unresolved_count != 0 {
                Some(BudgetDenial::UnresolvedSpend)
            } else if let Some((source_instance_id, source_epoch, high_water)) = export_position {
                if cap.applied_watermark.source_instance_id != source_instance_id
                    || cap.applied_watermark.source_epoch != source_epoch
                    || cap.applied_watermark.consumed_export_seq > high_water
                {
                    Some(BudgetDenial::ExportSourceMismatch)
                } else if cap.applied_watermark.consumed_export_seq < high_water {
                    Some(BudgetDenial::TrailingWatermark)
                } else {
                    None
                }
            } else {
                Some(BudgetDenial::ExportIndexUnavailable)
            };
            if let Some(denial) = denial {
                return Err(denial.into_error(cap.cap_id));
            }
            let age_ms = now_ms.saturating_sub(cap.as_of_ms).max(0) as u64;
            if age_ms > cap.max_staleness_ms {
                return Err(BudgetDenial::Stale.into_error(cap.cap_id));
            }
        }
        Ok(())
    }

    /// Resolve admission fields and start a run as a child task; track it Active. The run
    /// posts `RunFinished` back.
    ///
    /// ADMISSION BARRIER: the actor AWAITS the run's admission-ack before replying. The run
    /// is durably admitted (RunStarted fsynced) → set state Active (and the health gauge),
    /// write the durable run-index `Active` row, reply `Active{run_id}`. The gauge flips at
    /// admission durability, not after the index round-trip: `health.check` reads the atomic
    /// without the actor channel, so a post-index flip leaves a window where a live run
    /// reports zero. A pre-admission RESOLUTION failure (the executor failing to build the
    /// provider) → reply `Error`, no state change, no ghost run (RunStarted was never
    /// written). Because the actor blocks on admission (~1 fsync) and the index write before
    /// replying, a cancel/status racing the reply is serialized BEHIND it and always finds
    /// the durable index row.
    async fn start_run(
        &mut self,
        run_id: RunId,
        request: SubmissionRequest,
        kind: RunKind,
        submission_id: Option<broca_protocol::SubmissionId>,
        identity: SendIdentity,
        skip_materialization: bool,
    ) -> Result<RunId, StartRunFailure> {
        self.start_run_with_cancel(
            run_id,
            request,
            kind,
            submission_id,
            identity,
            StartRunFlags {
                skip_materialization,
                cancel_before_execute: false,
                input_delivery: broca_protocol::Delivery::Queue,
            },
        )
        .await
    }

    async fn start_run_with_cancel(
        &mut self,
        run_id: RunId,
        request: SubmissionRequest,
        kind: RunKind,
        submission_id: Option<broca_protocol::SubmissionId>,
        identity: SendIdentity,
        flags: StartRunFlags,
    ) -> Result<RunId, StartRunFailure> {
        let StartRunFlags {
            skip_materialization,
            cancel_before_execute,
            input_delivery,
        } = flags;
        let SendIdentity {
            conversation_key,
            title,
            send_id,
            request_identity_digest,
        } = identity;
        let tools_were_omitted = request.tools.is_none();
        let admitted_request_identity = AdmittedRequestIdentity {
            request_identity_digest: request_identity_digest.clone(),
            tools_were_omitted,
        };
        // This is admission for fresh/resumed execution, not for sealing an already-admitted
        // paused episode. A cancel finalizer must remain available even when a cap now denies work.
        if !cancel_before_execute {
            self.check_budget_admission()
                .map_err(StartRunFailure::Admission)?;
        }
        if self.prior_snapshot.is_none() {
            if let Err(error) = self.refresh_admission_view().await {
                return Err(StartRunFailure::Infrastructure(error));
            }
        }
        let prior_snapshot = self
            .prior_snapshot
            .clone()
            .expect("refresh installs an admission snapshot");
        // The scope this episode's `RunStarted` carries, decided against the same history the
        // episode is admitted on. For a caller's send that is its stamped scope when the send
        // freezes the session's first scope, adopts one, or reopens the frozen one at a later
        // epoch; otherwise it is the scope the session already froze. So every episode of a
        // scoped session carries a scope.
        let Some(ownership) = prior_snapshot.checkpoint_at_head.ownership.as_ref() else {
            return Err(StartRunFailure::Infrastructure(
                "the admission snapshot carries no ownership summary".into(),
            ));
        };
        let episode_scope = self
            .decide_session_scope(ownership, request.principal.as_ref(), &request.scope)
            .map_err(StartRunFailure::Scope)?
            .scope()
            .cloned();
        if let Some(first) = &ownership.first_episode {
            let _ = self.first_episode.set(first.clone());
        }
        // An episode admitted on a history with none is the session's first.
        let admits_the_session = ownership.episodes == 0;
        let mut resolved_request = request;
        if tools_were_omitted {
            // A fresh lineage has no prior config, so omission resolves to a tool-less run.
            let inherited_tools = prior_snapshot
                .checkpoint_at_head
                .last_episode
                .as_ref()
                .map(|episode| Arc::unwrap_or_clone(Arc::clone(&episode.config.tools)))
                .unwrap_or_default();
            resolved_request.policy_allowed = inherited_tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect();
            resolved_request.tools = Some(inherited_tools);
        }
        if !resolved_request.inherit_frozen.is_empty() {
            if let Some(frozen) = prior_snapshot
                .checkpoint_at_head
                .last_episode
                .as_ref()
                .map(|episode| &episode.config)
            {
                inherit_frozen_parameters(&mut resolved_request, frozen);
            } else {
                if resolved_request
                    .inherit_frozen
                    .contains(&broca_protocol::request::InheritedParam::Model)
                {
                    return Err(StartRunFailure::Admission(broca_core::RunError::Config(
                        "model is required: the session has no episode to inherit it from".into(),
                    )));
                }
                // A WAL header need not contain an episode. With a named model this is a first
                // send, so omitted parameters retain their defaults, including cache policy.
                resolved_request.inherit_frozen.clear();
            }
        }
        let checkpoint = &prior_snapshot.checkpoint_at_head;
        let resuming = checkpoint.last_episode.as_ref().is_some_and(|episode| {
            episode.run_id == run_id && episode.disposition != EpisodeDisposition::Finished
        });
        if !cancel_before_execute && !resuming {
            if let Some(detail) = checkpoint
                .compaction
                .as_ref()
                .and_then(|c| c.state())
                .and_then(|state| {
                    state.send_ceiling(
                        &resolved_request.input,
                        resolved_request.context_limit.or_else(|| {
                            checkpoint
                                .last_episode
                                .as_ref()
                                .filter(|episode| episode.config.model == resolved_request.model)
                                .and_then(|episode| episode.config.context_limit)
                        }),
                        &resolved_request.model,
                    )
                })
            {
                return Err(StartRunFailure::ContextCeiling(detail));
            }
        }
        if self.ceiling_drain_held {
            tracing::info!(session_ref = %broca_core::agent_session_ref(&self.key), "context ceiling queue hold cleared");
            self.ceiling_drain_held = false;
        }
        // A session admitted with a fetch plan runs with the tools its manifest froze, and its
        // first episode opens with the manifest's joined system text; nothing a later send
        // carries replaces either. The episode records the manifest on its `RunStarted`.
        let manifest = if admits_the_session {
            self.admitting_manifest.take()
        } else {
            ownership
                .first_episode
                .as_ref()
                .and_then(|first| first.manifest.clone())
        };
        // Every path that starts an episode passes here (a direct send, a drained queue, a
        // queue rebuilt after a crash), so an episode whose plan asks a provider to confine its
        // tools never starts without the scope that confinement is keyed on.
        if episode_scope.is_none()
            && manifest.as_ref().is_some_and(|manifest| {
                serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text())
                    .is_ok_and(|plan| broca_fetch_plan::names_confined_preset(&plan))
            })
        {
            return Err(StartRunFailure::Scope(
                broca_core::ScopeRefusal::ScopeRequired {
                    preset: broca_fetch_plan::CONFINED_TOOL_PRESET,
                },
            ));
        }
        if let Some(manifest) = &manifest {
            // The session's first episode, new or re-driven: only its input holds the system
            // text, exactly as its `RunStarted` recorded it.
            let first_episode_input = admits_the_session
                || ownership
                    .first_episode
                    .as_ref()
                    .is_some_and(|first| first.run_id == run_id);
            let is_system = |message: &&broca_protocol::content::Message| {
                message.role == broca_protocol::content::Role::System
            };
            let frozen_system = vec![broca_protocol::content::ContentBlock::Text {
                text: manifest.system_text.clone(),
            }];
            let systems: Vec<_> = resolved_request.input.iter().filter(is_system).collect();
            // A send carries neither tools nor system text: the manifest supplies both. An
            // episode re-driven from its own `RunStarted` carries the manifest's tools and, in
            // the first episode, the manifest's system text first; exactly those are accepted.
            let system_frozen = match systems.as_slice() {
                [] => true,
                [only] => {
                    first_episode_input
                        && std::ptr::eq(*only, &resolved_request.input[0])
                        && only.content == frozen_system
                }
                _ => false,
            };
            let tools_frozen = tools_were_omitted
                || resolved_request.tools.as_deref() == Some(manifest.tools.as_slice());
            if !system_frozen || !tools_frozen {
                return Err(StartRunFailure::Admission(broca_core::RunError::Config(
                    "this session's tools and system text are frozen from its fetch plan; a \
                     later send carries neither `tools` nor `system`"
                        .into(),
                )));
            }
            let needs_system =
                first_episode_input && systems.is_empty() && !manifest.system_text.is_empty();
            resolved_request.tools = Some(manifest.tools.as_ref().clone());
            resolved_request.policy_allowed = manifest
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect();
            if needs_system {
                resolved_request.input.insert(
                    0,
                    broca_protocol::content::Message::new(
                        broca_protocol::content::Role::System,
                        frozen_system,
                    ),
                );
            }
        }
        let first_tool_names: Vec<String> = resolved_request
            .tools
            .iter()
            .flatten()
            .map(|tool| tool.name.clone())
            .collect();
        // A consumed queue entry is already transformed, even if every provider passed or
        // the entry predates hooks. Never ask pre_user again when draining it.
        let mut input_hooks = None;
        let mut pre_user_error = None;
        if let Some(submission_id) = &submission_id {
            input_hooks = self
                .pending_fifo
                .iter()
                .find(|pending| &pending.submission_id == submission_id)
                .and_then(|pending| pending.hooks.clone())
                .map(|mut hooks| {
                    hooks.original =
                        Some(serde_json::to_value(&resolved_request.input).expect("message JSON"));
                    hooks
                });
        } else if !cancel_before_execute
            && (admits_the_session
                || matches!(kind, RunKind::AppendEpisode)
                || prior_snapshot
                    .checkpoint_at_head
                    .last_episode
                    .as_ref()
                    .is_some_and(|episode| episode.disposition == EpisodeDisposition::Finished))
        {
            let prepared = self
                .prepare_input(
                    resolved_request.input.clone(),
                    input_delivery,
                    manifest.clone(),
                )
                .await
                .map_err(|outcome| {
                    StartRunFailure::Infrastructure(format!(
                        "input preparation failed: {outcome:?}"
                    ))
                })?;
            if let Some(provider) = prepared.refused {
                pre_user_error = Some(serde_json::from_value(serde_json::json!({
                    "class": "permanent", "provider_code": cortexkit_role_llm_runner::errors::PRE_USER_UNAVAILABLE,
                    "message": format!("pre_user unavailable: {provider}"),
                })).expect("input refusal error JSON"));
            }
            resolved_request.input = prepared.input;
            input_hooks = prepared.hooks;
        }
        let auth = resolved_request.auth.take();
        let mut req = RunRequest::from_submission(
            resolved_request,
            run_id.clone(),
            self.key.clone(),
            submission_id
                .clone()
                .unwrap_or_else(|| broca_protocol::SubmissionId(String::new())),
        );
        req.retention = self.admitting_retention;
        req.session_class = if admits_the_session {
            self.admitting_session_class.clone()
        } else {
            prior_snapshot.checkpoint_at_head.session_class.clone()
        };
        req.submission_id = submission_id.clone();
        // Core writes both on the episode's `RunStarted` when it opens one, so the WAL alone
        // can answer a later retry with the same key.
        req.send_id = send_id;
        req.request_identity_digest = request_identity_digest.clone();
        req.title = title.clone();
        req.scope = episode_scope;

        if self.registry.is_stopping() {
            return Err(StartRunFailure::Infrastructure("module is stopping".into()));
        }
        let cancel = CancellationToken::new();
        if cancel_before_execute {
            cancel.cancel();
        }
        // The seal also carries the session's cache clock, so every request this run sends
        // restarts the clock a caller schedules warms from.
        let shutdown_seal =
            broca_core::ShutdownSeal::with_cache_clock(self.registry.cache_clock(&self.key));
        let clock = self.registry.cache_clock(&self.key);
        let resumed_auto = !matches!(kind, RunKind::AppendEpisode)
            && prior_snapshot
                .checkpoint_at_head
                .last_episode
                .as_ref()
                .is_some_and(|episode| {
                    episode.keep_warm == Some(broca_protocol::request::KeepWarm::Auto)
                });
        let mut recorded = None;
        if req.keep_warm == Some(broca_protocol::request::KeepWarm::Auto) || resumed_auto {
            let records = self
                .read_wal_records()
                .await
                .map_err(StartRunFailure::Infrastructure)?;
            let ttl = broca_core::cache_warm::latest_episode_config(&records)
                .and_then(|(_, config)| self.registry.cache_declaration(&config))
                .and_then(|declaration| declaration.ttl_ms);
            clock.seed_if_unknown(&records, ttl);
            recorded = broca_core::cache_warm::last_recorded_request(&records).ok();
        }
        let registry = self.registry.clone();
        self.retention_lineage = self.admitting_retention.is_some()
            || matches!(
                prior_snapshot.checkpoint_at_head.lineage_retention,
                Some(broca_core::LineageRetention::Retained { .. })
            );
        let warmer = Arc::new(crate::keep_warm::AutomaticWarmer {
            retention: self.retention_lineage,
            session: self.key.clone(),
            writer: self
                .writer
                .as_ref()
                .expect("writer live while open")
                .clone(),
            executor: Arc::clone(&self.executor),
            clock,
            seal: shutdown_seal.clone(),
            slot: self.warm_in_flight.clone(),
            declare: Arc::new(move |config| registry.cache_declaration(config)),
            recorded: std::sync::Mutex::new(recorded),
        });
        let steer = broca_core::SteerInbox::new();
        // The run may deliver every ordered send still queued, except the one this run consumes.
        steer.seed_ordered(
            self.pending_fifo
                .iter()
                .filter(|pending| submission_id.as_ref() != Some(&pending.submission_id))
                .filter_map(broca_core::delivery_order::OrderedSend::from_pending)
                .collect(),
        );
        let step_progress = Arc::new(std::sync::Mutex::new(None));
        let control = RunControl {
            step_progress: Some(Arc::clone(&step_progress)),
            clock: self
                .registry
                .retention_service()
                .map(|service| service.clock.clone()),
            keep_warm: Some(warmer),
            cancel: Some(cancel.clone()),
            tools_were_omitted,
            shutdown_seal: Some(shutdown_seal.clone()),
            // Once the module starts to stop, the run finishes the step it is in and starts
            // nothing new, so it usually leaves before the seal has to cut it.
            stop_requested: Some(self.registry.stopping_signal()),
            steer: Some(Arc::clone(&steer)),
            manifest: manifest.clone(),
            input_hooks,
            pre_user_error,
            hook_plane: self.executor.hook_plane(),
            elicitation_plane: self.executor.elicitation_plane(),
            compaction_plane: self.executor.compaction_plane(),
            compaction_views: self.executor.compaction_views(),
            ..Default::default()
        };
        let (admit_tx, admit_rx) = oneshot::channel::<Result<RunId, broca_core::RunError>>();
        let restart_resuming = prior_snapshot
            .checkpoint_at_head
            .last_episode
            .as_ref()
            .is_some_and(|episode| {
                episode.disposition == EpisodeDisposition::Paused
                    && episode.pause_reason == Some(broca_protocol::PauseReason::Restart)
            });
        let job = RunJob {
            conversation_key,
            req,
            auth,
            writer: self
                .writer
                .as_ref()
                .expect("writer live while open")
                .clone(),
            kind,
            skip_materialization,
            control,
            on_admitted: Some(admit_tx),
            prior_snapshot,
        };
        let executor = self.executor.clone();
        let terminal_tx = self.terminal_tx.clone();
        let rid = run_id.clone();
        let attempt_id = self.next_run_attempt;
        self.next_run_attempt = self.next_run_attempt.saturating_add(1);
        // Control normally comes only from the WAL projection, preventing double-publish.
        // StepStarted is consumed only to update advisory status and remains suppressed; all
        // other control events are dropped because core durably records every terminal first.
        let (out_tx, mut out_rx) = mpsc::channel::<OutputEvent>(256);
        let display = self.fanout.display_sender();
        let progress = Arc::new(AtomicU64::new(0));
        let output_progress = Arc::clone(&progress);
        let durable_finish = Arc::new(std::sync::OnceLock::new());
        let output_finish = Arc::clone(&durable_finish);
        let output_handle = tokio::spawn(async move {
            while let Some(ev) = out_rx.recv().await {
                match ev {
                    OutputEvent::Control(broca_protocol::caller::ControlUnit::StepStarted {
                        step_id,
                    }) => {
                        // Session status is advisory: a racing read may see the prior step, while
                        // the run lifecycle and durable control stream remain WAL-authoritative.
                        output_progress.store(step_id.0, Ordering::Relaxed);
                    }
                    // Once a run is admitted, core emits this only after the run's `RunFinished`
                    // is synced, so from here on the run has ended even though the actor has not
                    // yet processed its completion. (The one terminal core emits without writing
                    // it ends a run before admission, which never makes the actor active.) The
                    // first terminal wins; a run has only one.
                    OutputEvent::Control(broca_protocol::caller::ControlUnit::RunFinished {
                        run_id,
                        reason,
                        ..
                    }) => {
                        let _ = output_finish.set(DurableFinish { run_id, reason });
                    }
                    OutputEvent::Control(_) => {}
                    OutputEvent::Display(d) => {
                        let _ = display.send(Arc::new(d));
                    }
                }
            }
        });
        let handle = tokio::spawn(async move {
            // The guard reports a synthetic terminal if execution panics, preventing the actor
            // from remaining active forever and blocking durable queue drain.
            let guard = RunFinishGuard::new(terminal_tx, rid, attempt_id);
            let result = executor.execute(job, out_tx).await;
            let _ = output_handle.await;
            guard.finish(result);
        });

        let mut pending_active = Some(RunState::Active {
            steer,
            run_id: run_id.clone(),
            task_run_id: run_id.clone(),
            attempt_id,
            skip_materialization,
            cancel,
            shutdown_seal,
            progress,
            step_progress,
            handle,
            durable_finish,
        });
        if restart_resuming {
            // This episode is already durably admitted. Keep its actor responsive while its
            // frozen dependency routes are checked, without calling the paused WAL active.
            self.restart_admitting = true;
            self.set_active(pending_active.take().expect("pending resume task"));
        }
        let mut admit_rx = admit_rx;
        let seal = self.registry.seal_signal();
        let expiry_clock = self
            .registry
            .retention_service()
            .map(|service| service.clock.clone())
            .unwrap_or_default();
        let mut admission_expiry = self
            .retention_deadline
            .map(|deadline| Duration::from_millis(deadline.saturating_sub(expiry_clock.now_ms())));
        let mut expiry_changes = expiry_clock.subscribe();
        let admission = loop {
            let wait = admission_expiry.map(|wait| wait.min(Duration::from_millis(100)));
            let expiry = async {
                match wait {
                    Some(wait) => tokio::time::sleep(wait).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                _ = expiry => {
                    if self.retention_deadline.is_some_and(|deadline| deadline <= expiry_clock.now_ms()) {
                        let records = match self.read_wal_framed_records().await {
                            Ok(records) => records,
                            Err(error) => break Err(StartRunFailure::Infrastructure(error)),
                        };
                        self.retention_deadline = crate::retention::policy(&records).and_then(|(_, _, deadline)| deadline);
                        if self.retention_deadline.is_none_or(|deadline| deadline > expiry_clock.now_ms()) { admission_expiry = None; continue; }
                        // Waiting for a dependency writes no durable send and cannot extend expiry.
                        // Stop that task before closing the writer so it cannot append into
                        // an expired file after a fresh lineage has acquired the lease.
                        break Err(StartRunFailure::Expired);
                    }
                }
                _ = expiry_changes.changed() => {},
                _ = seal.cancelled(), if restart_resuming => {
                    break Err(StartRunFailure::Shutdown);
                }
                admission = &mut admit_rx => break match admission {
                    Ok(Ok(id)) => Ok(id),
                    Ok(Err(error)) => Err(StartRunFailure::Admission(error)),
                    Err(_) => Err(StartRunFailure::Infrastructure("run ended before admission".into())),
                },
                Some(command) = self.rx.recv(), if restart_resuming => {
                    // Commands are served while a restart-paused episode waits for its
                    // dependencies, so this one runs inside the suspended admission. Its exit
                    // request and any writer poison it observed must end this admission too:
                    // continuing would admit work into a generation that is about to tear down.
                    if Box::pin(self.handle_cmd(command)).await {
                        self.nested_exit_requested = true;
                        break Err(StartRunFailure::Infrastructure(
                            "session actor is exiting during restart admission".into(),
                        ));
                    }
                    if self.writer_poisoned {
                        break Err(StartRunFailure::Infrastructure(
                            "session writer was poisoned during restart admission".into(),
                        ));
                    }
                }
            }
        };
        self.restart_admitting = false;
        // `ResumeRestart` requests that arrived during this admission were parked because it
        // was already resuming the episode. Every path into restart admission must answer them
        // once it ends (a cancel or interrupt finalizer, a queued drain, a cancel recovery),
        // or the boot resume loop, which awaits each reply in turn, stalls behind this session.
        // Restart recovery answers them itself after deciding whether to seal a refused
        // resume, so it keeps them here.
        if restart_resuming && !self.restart_recovery_owns_waiters {
            self.finish_restart_waiters(match &admission {
                Ok(_) => Ok(()),
                Err(StartRunFailure::Shutdown) => {
                    Err("shutdown interrupted restart admission".into())
                }
                Err(error) => Err(error.to_string()),
            });
        }
        // A dropped acknowledgement means execution ended before the durable admission point.
        let admitted_run_id = match admission {
            Ok(id) => id,
            // Shutdown leaves the restart task in self.state so seal_for_shutdown
            // can stop and join it before durably recording the episode's end.
            Err(StartRunFailure::Shutdown) => return Err(StartRunFailure::Shutdown),
            Err(error) => {
                if let Some(RunState::Active { handle, .. }) = pending_active.take() {
                    handle.abort();
                    let _ = handle.await;
                } else {
                    self.stop_active_run().await;
                    self.set_idle();
                }
                return Err(error);
            }
        };

        if admits_the_session {
            let _ = self.first_episode.set(broca_core::FirstEpisodeV1 {
                run_id: admitted_run_id.clone(),
                tool_names: first_tool_names,
                manifest,
            });
            self.note_admitted_cache(&admitted_run_id).await;
        }
        if let Some(consumed) = submission_id.as_ref() {
            self.pending_fifo
                .retain(|pending| &pending.submission_id != consumed);
            self.skip_materialization_submissions.remove(consumed);
        }
        // Admission means a synced `RunStarted` exists under this run id: one core just wrote
        // with this identity, or, for a resume, the episode's original one.
        self.note_recorded_send_key(&admitted_run_id, true, request_identity_digest.as_deref());

        // Raise the in-flight gauge at the durable-admission moment, before the index write.
        // The index write is an await (spawn_blocking), and health.check reads run_in_flight
        // under the slots mutex without the actor channel — so flipping only after the write
        // leaves a window where an admitted, executing run still reports zero.
        self.admitted_request_identities
            .insert(admitted_run_id.clone(), admitted_request_identity);
        if skip_materialization {
            self.skip_materialization_runs
                .insert(admitted_run_id.clone());
        }
        if let Some(mut active) = pending_active {
            if let RunState::Active { run_id, .. } = &mut active {
                *run_id = admitted_run_id.clone();
            }
            self.set_active(active);
        }

        // Best-effort projection. A failed write must not undo Active: the run is already
        // admitted in the WAL and executing; cancel/status can still reconcile via the WAL.
        if let Some(index) = self.run_index.clone() {
            let row = RunIndexRow {
                withdrawals_owed: false,
                run_id: admitted_run_id.0.clone(),
                session: self.key.clone(),
                submission_id: submission_id.map(|id| id.0),
                request_identity_digest,
                state: RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
                // The title core recorded on this run's `RunStarted`. When admission resumed an
                // older episode instead (its durable run id differs from the one this send
                // asked for), no `RunStarted` was written for this send, so its title belongs to
                // no run and the row keeps the title it already holds.
                title: title.filter(|_| {
                    admitted_run_id == run_id
                        && self
                            .writer
                            .as_ref()
                            .and_then(|writer| writer.is_retention_lineage())
                            != Some(true)
                }),
            };
            match spawn_blocking(JobKind::StoreWrite, move || index.index_run_active(&row)).await {
                Ok(Err(error)) => {
                    self.registry.repair_retention(&self.key);
                    tracing::warn!(
                        target: "store",
                        "run-index active write failed (non-fatal): {error}"
                    )
                }
                Err(error) => {
                    self.registry.repair_retention(&self.key);
                    tracing::warn!(
                        target: "store",
                        "run-index active task failed (non-fatal): {error}"
                    )
                }
                Ok(Ok(())) => {}
            }
        }
        let _ = self.sync_retention_index().await;
        self.stamp_retained_run(&admitted_run_id).await;
        Ok(admitted_run_id)
    }

    /// Durably queue a submission while a run is active. The pending cache is changed only after
    /// the queue record reaches the writer barrier.
    ///
    /// The submission id is `sub-<run_id>` over the identity the idle-send path already chose,
    /// which is what lets the drain recover that identity by stripping the prefix rather than
    /// carrying a second mapping. Changing the shape here therefore breaks the drain and the
    /// recovery path, not just this function.
    async fn queue_submission(
        &mut self,
        run_id: RunId,
        request: SubmissionRequest,
        identity: SendIdentity,
        skip_materialization: bool,
        delivery: broca_protocol::Delivery,
    ) -> SendOutcome {
        self.queue_prepared_submission(
            run_id,
            request,
            identity,
            skip_materialization,
            delivery,
            None,
        )
        .await
    }

    async fn queue_prepared_submission(
        &mut self,
        run_id: RunId,
        mut request: SubmissionRequest,
        identity: SendIdentity,
        skip_materialization: bool,
        delivery: broca_protocol::Delivery,
        prepared: Option<pre_user::PreparedInput>,
    ) -> SendOutcome {
        if self.registry.is_stopping() {
            return SendOutcome::Transient("module is stopping".into());
        }
        let SendIdentity {
            conversation_key,
            title,
            send_id,
            request_identity_digest,
        } = identity;
        // Done before the retry comparison below as well, so a retry's request carries the same
        // frozen model as the pending record it is compared with.
        if let Err(error) = self.record_frozen_model(&mut request).await {
            return SendOutcome::Error(error);
        }
        // A plain queued send records no mode, so its bytes are exactly those it always had.
        let hook_delivery = delivery;
        let delivery = (delivery != broca_protocol::Delivery::Queue).then_some(delivery);
        let submission_id = broca_protocol::SubmissionId(format!("sub-{}", run_id.0));
        if let Some(existing) = self
            .pending_fifo
            .iter()
            .find(|pending| pending.submission_id == submission_id)
        {
            if existing.conversation_key != conversation_key {
                return SendOutcome::SendIdReuse {
                    differing: "conversation_key",
                };
            }
            if existing.delivery != delivery {
                return SendOutcome::SendIdReuse {
                    differing: "delivery",
                };
            }
            // A retry carries the same client_message_id as the queued send, or none when it
            // carried none (llm-runner §9.11). The queued request does not keep the id, so it is
            // read from the identity digest the queued record stored.
            let existing_client_message_id = existing
                .request_identity_digest
                .as_deref()
                .and_then(broca_core::digest_client_message_id);
            if existing_client_message_id != request.client_message_id {
                return SendOutcome::SendIdReuse {
                    differing: "client_message_id",
                };
            }
            // The title is deliberately not compared: it labels the send without being part of
            // its identity, so a retry carrying another title is answered `Pending` and the
            // queued record keeps the title it was written with.
            return match submission_difference(&existing.request, &request) {
                None => SendOutcome::Pending { submission_id },
                Some(differing) => SendOutcome::SendIdReuse { differing },
            };
        }
        let prepared = match prepared {
            Some(prepared) => prepared,
            None => {
                let manifest = match self.frozen_input_manifest().await {
                    Ok(manifest) => manifest,
                    Err(outcome) => return outcome,
                };
                match self
                    .prepare_input(request.input.clone(), hook_delivery, manifest)
                    .await
                {
                    Ok(prepared) => prepared,
                    Err(outcome) => return outcome,
                }
            }
        };
        if let Some(provider) = &prepared.refused {
            return SendOutcome::PreUserUnavailable {
                provider: provider.clone(),
            };
        }
        let hooks = prepared.queued_hooks();
        let principal = request.principal.clone();
        let pending = PendingSubmission {
            hooks: hooks.clone(),
            conversation_key: conversation_key.clone(),
            delivery,
            send_id: send_id.clone(),
            title: title.clone(),
            submission_id: submission_id.clone(),
            request: request.clone(),
            request_identity_digest: request_identity_digest.clone(),
            principal: principal.clone(),
            order: None,
            sent_by: None,
        };
        // The session writer is shared with the active run, so this sync is a durable queue
        // barrier in the same sequence space as run records. The `Pending` reply below follows
        // it: a caller told its send is queued can rely on the queued record (and its send key)
        // surviving a crash.
        let writer = self.writer.as_ref().expect("writer live while open");
        let queued = writer.record(WalRecord::Queued {
            retention: self.admitting_retention,
            ts_ms: self.retention_timestamp(),
            hooks,
            keep_warm: request.keep_warm,
            conversation_key,
            delivery,
            send_id,
            title,
            submission_id: submission_id.clone(),
            request: Box::new(request),
            request_identity_digest: request_identity_digest.clone(),
            principal,
        });
        if let Err(error) = queued.into_result() {
            return match error {
                broca_core::WalWriteError::SessionTooLarge { max_bytes } => {
                    SendOutcome::SessionTooLarge { max_bytes }
                }
                broca_core::WalWriteError::RetainedSessionTooLarge { max_bytes } => {
                    SendOutcome::RetainedSessionTooLarge { max_bytes }
                }
                broca_core::WalWriteError::Failed(error) => SendOutcome::Error(error),
            };
        }
        if let Err(error) = writer.sync().await {
            return SendOutcome::Error(error.to_string());
        }
        self.note_recorded_send_key(&run_id, false, request_identity_digest.as_deref());
        // The same drain order the queue is rebuilt in after a restart: interrupts first.
        broca_core::insert_in_drain_order(&mut self.pending_fifo, pending);
        if skip_materialization {
            self.skip_materialization_submissions
                .insert(submission_id.clone());
        }
        SendOutcome::Pending { submission_id }
    }

    /// Whether a keyed send reuses the run id of an earlier send that asked for a different
    /// delivery order: a legacy send (no order) and an ordered send, or two different orders.
    /// The same `send_id` is the same send, so the order is part of its identity.
    async fn send_order_differs(&mut self, run_id: &RunId) -> Result<bool, String> {
        self.ensure_wal_scan_loaded().await?;
        let submission_id = broca_protocol::SubmissionId(format!("sub-{}", run_id.0));
        let used = self.recorded_send_keys.contains_key(run_id)
            || self.ordered_submissions.contains_key(&submission_id);
        Ok(used && self.ordered_submissions.get(&submission_id) != self.admitting_order.as_ref())
    }

    /// How a send with a legacy string `delivery` is admitted: from the session's owner (or a
    /// caller the owner check decides on, as before), as its list from a plan-named sender, or
    /// as an interrupt from a named sender holding the `interrupt` grant, which then continues
    /// under the session's frozen principal. Admission is `delivery_order::admit_delivery`'s.
    async fn string_sender(
        &mut self,
        request: &SubmissionRequest,
        delivery: broca_protocol::Delivery,
    ) -> Result<StringSender, SendOutcome> {
        let Some(broca_protocol::BindPrincipal::Reserved { module_id }) = &request.principal else {
            return Ok(StringSender::Owner);
        };
        let ownership = self.load_ownership().await.map_err(|error| {
            SendOutcome::Transient(format!(
                "cannot read the session's history for the sender check: {error}"
            ))
        })?;
        if broca_core::scope_admission::decide_owner_access(
            &ownership,
            request.principal.as_ref(),
            &request.scope,
        )
        .is_ok()
        {
            return Ok(StringSender::Owner);
        }
        let senders = frozen_plan_senders(&ownership);
        // A caller the plan does not name meets the owner check further down, as it always has.
        if !senders.iter().any(|sender| &sender.module == module_id) {
            return Ok(StringSender::Owner);
        }
        let frozen = ownership
            .scope
            .as_ref()
            .map(|scope| scope.owner.clone())
            .or_else(|| ownership.first_principal.clone())
            .ok_or_else(|| {
                SendOutcome::Transient("the session has no frozen principal to send under".into())
            })?;
        let role_delivery = match delivery {
            broca_protocol::Delivery::Queue => cortexkit_role_llm_runner::send::Delivery::Queue,
            broca_protocol::Delivery::Steer => cortexkit_role_llm_runner::send::Delivery::Steer,
            broca_protocol::Delivery::Interrupt => {
                cortexkit_role_llm_runner::send::Delivery::Interrupt
            }
        };
        cortexkit_role_llm_runner::delivery_order::admit_delivery(
            module_id,
            Some(&frozen.as_owner_string()),
            &senders,
            role_delivery,
        )
        .map_err(|(code, field)| SendOutcome::DeliveryRefused {
            code,
            field: field.unwrap_or("delivery"),
            message: format!(
                "the session's plan does not let module {module_id} send delivery {}",
                delivery.as_str()
            ),
        })?;
        Ok(
            match cortexkit_role_llm_runner::delivery_order::legacy_equivalent(role_delivery) {
                Some(modes) => StringSender::AsOrder(broca_protocol::DeliveryOrder {
                    modes,
                    dedupe_key: None,
                    expires_at_ms: None,
                    template: None,
                }),
                None => StringSender::Interrupt(frozen),
            },
        )
    }

    /// Whether the send being handled comes from the session's owner (`Ok(None)`), from a
    /// plan-named sender other than the owner (`Ok(Some(frozen principal))`: the principal the
    /// send continues under), or must be refused. The sender is the principal the daemon stamped
    /// on the route, never a request field. A named sender may use `idle_start` only if the plan
    /// grants it. The session's frozen principal is its scope's owner when it is scoped, else
    /// the principal of its first episode.
    async fn ordered_sender(
        &mut self,
        request: &SubmissionRequest,
        order: &broca_protocol::DeliveryOrder,
    ) -> Result<Option<broca_protocol::BindPrincipal>, SendOutcome> {
        let ownership = self.load_ownership().await.map_err(|error| {
            SendOutcome::Transient(format!(
                "cannot read the session's history for the sender check: {error}"
            ))
        })?;
        if broca_core::scope_admission::decide_owner_access(
            &ownership,
            request.principal.as_ref(),
            &request.scope,
        )
        .is_ok()
        {
            return Ok(None);
        }
        let refused = |code: &'static str, message: String| SendOutcome::DeliveryRefused {
            code,
            field: "delivery",
            message,
        };
        let Some(broca_protocol::BindPrincipal::Reserved { module_id }) = &request.principal else {
            return Err(refused(
                cortexkit_role_llm_runner::errors::SCOPE_OWNER_MISMATCH,
                "only the session's owner or a module its plan names may send with an ordered delivery list".into(),
            ));
        };
        let senders = ownership
            .first_episode
            .as_ref()
            .and_then(|first| first.manifest.as_ref())
            .and_then(|manifest| {
                serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text()).ok()
            })
            .and_then(|plan| {
                plan.as_object()
                    .and_then(|plan| broca_fetch_plan::plan_senders(plan).ok())
            })
            .unwrap_or_default();
        let Some(sender) = senders.iter().find(|sender| &sender.module == module_id) else {
            return Err(refused(
                cortexkit_role_llm_runner::errors::SCOPE_OWNER_MISMATCH,
                format!("the session's plan does not name module {module_id} as a sender"),
            ));
        };
        if order.starts_runs() && !sender.idle_start {
            return Err(refused(
                cortexkit_role_llm_runner::errors::INVALID_PARAMS,
                format!("the session's plan does not let module {module_id} start runs"),
            ));
        }
        let frozen = ownership
            .scope
            .as_ref()
            .map(|scope| scope.owner.clone())
            .or_else(|| ownership.first_principal.clone());
        frozen.map(Some).ok_or_else(|| {
            SendOutcome::Transient("the session has no frozen principal to send under".into())
        })
    }

    /// Render once at admission from the attested sender's frozen template. The queued
    /// request contains the resulting text, so recovery replays bytes rather than slots.
    async fn render_ordered_template(
        &mut self,
        request: &mut SubmissionRequest,
        template: &cortexkit_role_llm_runner::delivery_order::TemplateUse,
    ) -> Result<(), SendOutcome> {
        let refused = |problem: String| SendOutcome::DeliveryRefused {
            code: cortexkit_role_llm_runner::errors::INVALID_PARAMS,
            field: "template",
            message: format!("cannot render the sender's template: {problem}"),
        };
        let ownership = self
            .load_ownership()
            .await
            .map_err(SendOutcome::Transient)?;
        let senders = ownership
            .first_episode
            .as_ref()
            .and_then(|first| first.manifest.as_ref())
            .and_then(|manifest| {
                serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text()).ok()
            })
            .and_then(|plan| {
                plan.as_object()
                    .and_then(|plan| broca_fetch_plan::plan_senders(plan).ok())
            })
            .unwrap_or_default();
        let Some(broca_protocol::BindPrincipal::Reserved { module_id }) = &request.principal else {
            return Err(refused("no template is declared for this caller".into()));
        };
        let templates = senders
            .iter()
            .find(|sender| &sender.module == module_id)
            .map(|sender| sender.templates.as_slice())
            .unwrap_or_default();
        // A `part_tag` slot names a part by its host identity and renders the number the
        // transcript shows for it now. Broca renders a template once, at admission, so the
        // lookup is against the transcript as it stands when the send is admitted.
        let mut transcript = Vec::new();
        if templates
            .iter()
            .flat_map(|template| template.slots.values())
            .any(|slot| {
                matches!(
                    slot,
                    cortexkit_role_llm_runner::delivery_order::Slot::PartTag
                )
            })
        {
            let records = self.read_wal_records().await.map_err(SendOutcome::Error)?;
            if !records.is_empty() {
                transcript = broca_core::replay(&records)
                    .map_err(|e| SendOutcome::Error(e.to_string()))?
                    .prompt;
            }
        }
        let rendered =
            cortexkit_role_llm_runner::delivery_order::render(templates, template, |part| {
                shown_number(&transcript, part)
            });
        // A part the transcript does not show ends the send by name, not as a malformed slot.
        if let Err(cortexkit_role_llm_runner::delivery_order::TemplateProblem::PartGone(slot)) =
            &rendered
        {
            return Err(SendOutcome::TemplatePartGone { slot: slot.clone() });
        }
        let text = rendered.map_err(|problem| refused(format!("{problem:?}")))?;
        request.input = vec![broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text { text }],
        )];
        Ok(())
    }

    /// Queue a send using the `delivery_order` group: `DeliveryOrdered` then `Queued`, in one
    /// barrier, synced before the reply. An undelivered send from the same sender under the same
    /// dedupe key ends `superseded` first, in the same barrier. While a turn runs, all three are
    /// written under its inbox lock, so a send the turn already delivered is never replaced and
    /// the turn sees the new send at its next boundary. On an idle session a list naming
    /// `idle_start` then drains into a run at once; any other list waits.
    async fn queue_ordered_submission(
        &mut self,
        run_id: RunId,
        mut request: SubmissionRequest,
        identity: SendIdentity,
        skip_materialization: bool,
        order: broca_protocol::DeliveryOrder,
    ) -> SendOutcome {
        if self.registry.is_stopping() {
            return SendOutcome::Transient("module is stopping".into());
        }
        let submission_id = broca_protocol::SubmissionId(format!("sub-{}", run_id.0));
        let request_scope = request.scope.clone();
        // A retry with the same `send_id` is the same send: answer what became of it.
        if let Some(existing) = self
            .pending_fifo
            .iter()
            .find(|pending| pending.submission_id == submission_id)
        {
            if existing.order.as_ref() != Some(&order) {
                return SendOutcome::SendIdReuse {
                    differing: "delivery",
                };
            }
            return SendOutcome::Pending { submission_id };
        }
        if identity.send_id.is_some() {
            if let Err(error) = self.ensure_wal_scan_loaded().await {
                return SendOutcome::Error(error);
            }
            let records = match self.read_wal_records().await {
                Ok(records) => records,
                Err(error) => return SendOutcome::Error(error),
            };
            if let Some(outcome) = crate::send_fate::recorded_send_outcome(
                &records,
                &run_id,
                identity.send_id.as_deref().unwrap_or(""),
            ) {
                return outcome;
            }
        }
        let expired = order.expired_at(self.ordered_now_ms());
        // Like a plain queued send, a send that may start a run is refused on a session paused
        // for re-authentication: it would wait behind a run nothing is going to end.
        if order.starts_runs() && matches!(self.state, RunState::Idle) {
            match self.paused_run().await {
                Ok(Some((paused, reason)))
                    if reason == broca_protocol::PauseReason::AuthRequired =>
                {
                    return SendOutcome::RunPaused {
                        run_id: paused,
                        reason,
                    }
                }
                Ok(_) => {}
                Err(error) => return SendOutcome::Transient(error),
            }
        }
        if let Err(error) = self.record_frozen_model(&mut request).await {
            return SendOutcome::Error(error);
        }
        let manifest = match self.frozen_input_manifest().await {
            Ok(manifest) => manifest,
            Err(outcome) => return outcome,
        };
        let prepared = match self
            .prepare_input(
                request.input.clone(),
                broca_protocol::Delivery::Queue,
                manifest,
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        if let Some(provider) = &prepared.refused {
            return SendOutcome::PreUserUnavailable {
                provider: provider.clone(),
            };
        }
        let SendIdentity {
            conversation_key,
            title,
            send_id,
            request_identity_digest,
        } = identity;
        let hooks = prepared.queued_hooks();
        let principal = request.principal.clone();
        let pending = PendingSubmission {
            hooks: hooks.clone(),
            conversation_key: conversation_key.clone(),
            delivery: None,
            send_id: send_id.clone(),
            title: title.clone(),
            submission_id: submission_id.clone(),
            request: request.clone(),
            request_identity_digest: request_identity_digest.clone(),
            principal: principal.clone(),
            order: Some(order.clone()),
            sent_by: self.admitting_sent_by.clone(),
        };
        // Whose dedupe key this is: the plan-named sender, or the owner sending as itself.
        let sender = self.admitting_sent_by.clone().or_else(|| principal.clone());
        let Some(send) = broca_core::delivery_order::OrderedSend::from_pending(&pending) else {
            unreachable!("an ordered pending entry has an order");
        };
        let queued = WalRecord::Queued {
            retention: self.admitting_retention,
            ts_ms: self.retention_timestamp(),
            hooks,
            keep_warm: request.keep_warm,
            conversation_key,
            delivery: None,
            send_id,
            title,
            submission_id: submission_id.clone(),
            request: Box::new(request),
            request_identity_digest: request_identity_digest.clone(),
            principal: principal.clone(),
        };
        let ordered = WalRecord::DeliveryOrdered {
            submission_id: submission_id.clone(),
            order: order.clone(),
            sent_by: self.admitting_sent_by.clone(),
        };
        let writer = Arc::clone(self.writer.as_ref().expect("writer live while open"));
        let write = |superseded: Option<&broca_protocol::SubmissionId>| {
            if let Some(old) = superseded {
                writer.record(broca_core::delivery_order::ended(
                    old,
                    broca_wal::record::SendEndReason::Superseded {
                        by: submission_id.clone(),
                    },
                ));
            }
            writer.record(ordered);
            writer.record(queued);
            if expired {
                writer.record(broca_core::delivery_order::ended(
                    &submission_id,
                    broca_wal::record::SendEndReason::Expired,
                ));
            }
        };
        let open_inbox = match &self.state {
            RunState::Active { steer, .. } if !expired && !steer.is_closed() => {
                Some(Arc::clone(steer))
            }
            _ => None,
        };
        let mut write = Some(write);
        let mut write_once = |old: Option<&broca_protocol::SubmissionId>| {
            if let Some(write) = write.take() {
                write(old);
            }
        };
        let superseded = match open_inbox.map(|inbox| inbox.accept_ordered(send, &mut write_once)) {
            Some(broca_core::OrderedAcceptance::Queued { superseded }) => superseded,
            Some(broca_core::OrderedAcceptance::Closed) | None => {
                // No turn can deliver anything now, so the actor's own queue decides what this
                // send replaces. First drop what the ending turn already delivered or ended, so
                // a delivered send is never replaced.
                if let RunState::Active { steer, .. } = &self.state {
                    let consumed = steer.take_consumed();
                    if !consumed.is_empty() {
                        self.pending_fifo
                            .retain(|pending| !consumed.contains(&pending.submission_id));
                        self.wal_scan_complete = false;
                    }
                }
                let queued_old = self
                    .pending_fifo
                    .iter()
                    .filter_map(broca_core::delivery_order::OrderedSend::from_pending)
                    .find(|queued| queued.replaced_by(sender.as_ref(), order.dedupe_key.as_deref()))
                    .map(|queued| queued.submission_id);
                write_once(queued_old.as_ref());
                queued_old
            }
        };
        if let Err(error) = writer.sync().await {
            return SendOutcome::Error(error.to_string());
        }
        if let Some(old) = superseded {
            self.pending_fifo
                .retain(|pending| pending.submission_id != old);
            self.skip_materialization_submissions.remove(&old);
            if self.wal_scan_complete {
                self.ended_submissions.insert(
                    old,
                    broca_wal::record::SendEndReason::Superseded {
                        by: submission_id.clone(),
                    },
                );
            }
        }
        self.note_recorded_send_key(&run_id, false, request_identity_digest.as_deref());
        if self.wal_scan_complete {
            self.ordered_submissions
                .insert(submission_id.clone(), order.clone());
        }
        if expired {
            self.ended_submissions.insert(
                submission_id.clone(),
                broca_wal::record::SendEndReason::Expired,
            );
            self.prior_snapshot = None;
            return SendOutcome::SendEnded {
                submission_id,
                reason: broca_wal::record::SendEndReason::Expired,
            };
        }
        broca_core::insert_in_drain_order(&mut self.pending_fifo, pending);
        if skip_materialization {
            self.skip_materialization_submissions
                .insert(submission_id.clone());
        }
        self.prior_snapshot = None;
        // On an idle session `idle_start` is the first possible mode of any list naming it
        // (nothing else is possible without a running turn), so the send starts its run now.
        if order.starts_runs() && matches!(self.state, RunState::Idle) {
            self.immediate_ordered_drain = Some((submission_id.clone(), request_scope));
            let drained = self.drain_queue_one().await;
            self.immediate_ordered_drain = None;
            if let Err(error) = drained {
                tracing::warn!(target: "admission", "ordered send drain deferred: {error}");
            }
            if let RunState::Active { run_id: active, .. } = &self.state {
                if *active == run_id {
                    return SendOutcome::Active {
                        run_id,
                        delivered: None,
                    };
                }
            }
        }
        SendOutcome::Pending { submission_id }
    }

    /// Retract a still-pending submission: decide from the durable records, and if it's
    /// retractable write a durable `Retracted` tombstone (record + sync, so the retract is
    /// durable before the reply — symmetric with the queue's durable-before-Pending).
    async fn on_retract(&mut self, submission_id: broca_protocol::SubmissionId) -> RetractOutcome {
        if let Err(error) = self.wal_store.check_move(&self.key, true) {
            return match error.into_move_refusal() {
                Ok(broca_wal::migration::MoveRefusal::Moving { cut_id }) => {
                    RetractOutcome::SessionMoving { cut_id }
                }
                Ok(broca_wal::migration::MoveRefusal::Moved { holder }) => {
                    RetractOutcome::SessionMoved { holder }
                }
                Err(error) => RetractOutcome::Error(error.to_string()),
            };
        }
        loop {
            let records = match self.read_wal_records().await {
                Ok(records) => records,
                Err(e) => return RetractOutcome::Error(e),
            };
            self.replace_wal_scan_sets(&records);
            return match broca_core::retract_decision(&records, &submission_id) {
                broca_core::RetractDecision::Retract => {
                    let writer = self.writer.as_ref().expect("writer live while open");
                    let record = || -> Result<(), broca_core::WalWriteError> {
                        let retracted = writer.record(WalRecord::Retracted {
                            submission_id: submission_id.clone(),
                            failure: None,
                        });
                        retracted.into_result().map(|_| ())
                    };
                    let recorded = match &self.state {
                        RunState::Active { steer, .. } => {
                            steer.retract_ordered(&submission_id, record)
                        }
                        _ => record().map(|()| true),
                    };
                    let recorded = match recorded {
                        Ok(recorded) => recorded,
                        Err(error) => {
                            return match error {
                                broca_core::WalWriteError::SessionTooLarge { max_bytes } => {
                                    RetractOutcome::SessionTooLarge { max_bytes }
                                }
                                broca_core::WalWriteError::RetainedSessionTooLarge {
                                    max_bytes,
                                } => RetractOutcome::RetainedSessionTooLarge { max_bytes },
                                broca_core::WalWriteError::Failed(error) => {
                                    RetractOutcome::Error(error)
                                }
                            }
                        }
                    };
                    if !recorded {
                        continue;
                    }
                    if let Err(e) = writer.sync().await {
                        return RetractOutcome::Error(e.to_string());
                    }
                    self.pending_fifo
                        .retain(|pending| pending.submission_id != submission_id);
                    self.skip_materialization_submissions.remove(&submission_id);
                    self.retracted_submissions.insert(submission_id);
                    self.prior_snapshot = None;
                    RetractOutcome::Retracted
                }
                broca_core::RetractDecision::AlreadyStarted => RetractOutcome::AlreadyStarted,
                broca_core::RetractDecision::NotPending => RetractOutcome::NotPending,
            };
        }
    }

    /// Decide every refusal for a warm from what the actor and the WAL already hold, and
    /// snapshot what sending it needs. Nothing is sent or written here.
    ///
    /// The base is the in-flight request when a run is driving one (its exact bytes, from the
    /// run's transport), else the most recent request the WAL records, which the executor renders
    /// again and sends only if the bytes match the digest that step recorded.
    async fn prepare_warm(
        &mut self,
        lineage_id: Option<String>,
    ) -> Result<(PendingWarmParts, crate::executor::WarmJob), Box<WarmOutcome>> {
        use broca_wire::WarmRefusalDetail as Refusal;
        let refuse = |detail| {
            Err(Box::new(WarmOutcome::Refused(
                broca_wire::WarmRefusal::new(detail),
            )))
        };
        let now_ms = epoch_ms();
        let Some(writer) = self.writer.clone() else {
            return Err(Box::new(WarmOutcome::Reopen));
        };

        if let Some(requested) = lineage_id {
            let current = match writer.lineage_id() {
                Some(current) => Some(current),
                None => match writer.sync().await {
                    Ok(_) => writer.lineage_id(),
                    Err(error) => return Err(Box::new(WarmOutcome::Error(error.to_string()))),
                },
            };
            if current.map(|current| current.to_string()).as_deref() != Some(requested.as_str()) {
                return refuse(Refusal::LineageChanged { requested });
            }
        }

        let (in_flight, active_run) = match &self.state {
            RunState::Active {
                run_id,
                shutdown_seal,
                ..
            } => (shutdown_seal.last_sent(), Some(run_id.clone())),
            RunState::Idle => {
                // A paused run holds no task, so the actor is idle while it waits.
                match self.paused_run().await {
                    Ok(Some((run_id, reason))) => {
                        return refuse(Refusal::RunPaused {
                            run_id: run_id.0,
                            reason,
                        })
                    }
                    Ok(None) => {}
                    Err(error) => return Err(Box::new(WarmOutcome::Error(error))),
                }
                (None, None)
            }
        };

        let records = match self.read_wal_records().await {
            Ok(records) => records,
            Err(error) => return Err(Box::new(WarmOutcome::Error(error))),
        };
        if in_flight.is_none() && !broca_core::cache_warm::has_model_step(&records) {
            return refuse(Refusal::NoStepYet);
        }
        if let Some(resets_at_ms) = broca_core::cache_warm::account_wall(&records, now_ms) {
            return refuse(Refusal::AccountWalled { resets_at_ms });
        }
        let Some((latest_run, latest_config)) =
            broca_core::cache_warm::latest_episode_config(&records)
        else {
            return refuse(Refusal::NoStepYet);
        };

        // What the session's frozen config allows, before any expiry or credential is looked
        // at: a session that can never be warmed says so whatever its clock reads.
        let declaration = self.registry.cache_declaration(&latest_config);
        let ttl_ms = match &declaration {
            None => {
                return refuse(Refusal::WarmUnsupported {
                    reason: "this runner cannot read the session's cache policy".into(),
                })
            }
            Some(crate::cache::CacheDeclaration {
                warm: Err(reason), ..
            }) => {
                return refuse(Refusal::WarmUnsupported {
                    reason: reason.clone(),
                })
            }
            Some(declaration) => declaration.ttl_ms,
        };

        let clock = self.registry.cache_clock(&self.key);
        // Empty after a restart until dated from the session's WAL.
        clock.seed_if_unknown(&records, ttl_ms);
        let reading = clock.read().map(|reading| broca_core::CacheClockReading {
            ttl_ms: reading.ttl_ms.or(ttl_ms),
            ..reading
        });
        // The expiry check prevents a warm from paying for a full cache write instead of
        // reading the cached prefix. Subscription writes are free, so an expired prefix may
        // be re-primed; metered requests keep the expiry check.
        let subscription = latest_config
            .auth_selection
            .as_ref()
            .is_some_and(|auth| auth.credential_id.split(':').next() == Some("chatgpt"));
        if !subscription {
            if let Err(reason) = broca_core::cache_warm::check_warm_expiry(reading, now_ms) {
                return refuse(Refusal::CacheExpired {
                    expires_at_ms: reading.and_then(|reading| reading.expires_at_ms()),
                    reason,
                });
            }
        }

        let (base, config, run_id, step_id) = match in_flight {
            Some(sent) => {
                let step_id = sent.step_id;
                (
                    crate::executor::WarmBase::InFlight(sent),
                    latest_config,
                    active_run.unwrap_or(latest_run),
                    step_id,
                )
            }
            None => match broca_core::cache_warm::last_recorded_request(&records) {
                Ok(recorded) => {
                    let (config, run_id, step_id) = (
                        recorded.config.clone(),
                        recorded.run_id.clone(),
                        recorded.step_id,
                    );
                    (
                        crate::executor::WarmBase::Recorded(Box::new(recorded)),
                        config,
                        run_id,
                        step_id,
                    )
                }
                Err(broca_core::RecordedRequestError::NoStep) => return refuse(Refusal::NoStepYet),
                Err(broca_core::RecordedRequestError::Unavailable(reason)) => {
                    return refuse(Refusal::WarmPrefixUnavailable {
                        request_body_sha256: None,
                        reason,
                    })
                }
            },
        };
        Ok((
            PendingWarmParts {
                run_id,
                step_id,
                request_body_sha256: base.request_body_sha256().to_owned(),
                ttl_ms: ttl_ms.unwrap_or_default(),
            },
            crate::executor::WarmJob {
                session: self.key.clone(),
                config,
                base,
            },
        ))
    }

    /// `session.warm`: refuse it here, or start it on its own task and answer `reply` when that
    /// task's `WarmFinished` comes back. A refusal sends and writes nothing.
    async fn on_warm(&mut self, lineage_id: Option<String>, reply: oneshot::Sender<WarmOutcome>) {
        if let Err(error) = self.wal_store.check_move(&self.key, true) {
            let _ = reply.send(match error.into_move_refusal() {
                Ok(broca_wal::migration::MoveRefusal::Moving { cut_id }) => {
                    WarmOutcome::SessionMoving { cut_id }
                }
                Ok(broca_wal::migration::MoveRefusal::Moved { holder }) => {
                    WarmOutcome::SessionMoved { holder }
                }
                Err(error) => WarmOutcome::Error(error.to_string()),
            });
            return;
        }
        // At most one warm per session at a time: a second would only duplicate the first's
        // effect and cost.
        let Some(permit) = self.warm_in_flight.acquire() else {
            let _ = reply.send(WarmOutcome::Refused(broca_wire::WarmRefusal::new(
                broca_wire::WarmRefusalDetail::WarmInFlight,
            )));
            return;
        };
        let (pending, job) = match self.prepare_warm(lineage_id).await {
            Ok(prepared) => prepared,
            Err(outcome) => {
                let _ = reply.send(*outcome);
                return;
            }
        };
        let pending = PendingWarm::new(pending, reply, permit);
        let executor = Arc::clone(&self.executor);
        let terminal_tx = self.terminal_tx.clone();

        self.warm_task = Some(tokio::spawn(async move {
            // A panicking executor still ends the warm, so the session can be warmed again.
            let result = std::panic::AssertUnwindSafe(executor.warm(job))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(crate::executor::WarmError::Unavailable(
                        "the warm task panicked before it had a result".into(),
                    ))
                });
            let _ = terminal_tx.send(LineageCmd::WarmFinished {
                warm: Box::new((pending, result)),
            });
        }));
    }

    /// A warm's task ended: record a warm the provider answered and reply to its caller. The
    /// record is informational, so it is written even when another run has started since; the
    /// warmed run's facts are exported now unless that run is the one still running, whose own
    /// end exports them.
    async fn on_warm_finished(
        &mut self,
        pending: PendingWarm,
        result: Result<broca_core::WarmSent, crate::executor::WarmError>,
    ) {
        // Warm completion releases the session-move fence even on failure or after the
        // caller stops waiting.
        self.warm_task.take();
        let PendingWarm {
            permit: _permit,
            reply,
            run_id,
            step_id,
            request_body_sha256,
            ttl_ms,
        } = pending;
        let sent = match result {
            Ok(sent) => sent,
            Err(error) => {
                if self.retention_lineage
                    || self
                        .writer
                        .as_ref()
                        .and_then(|writer| writer.is_retention_lineage())
                        == Some(true)
                {
                    tracing::warn!(target: "cache", variant = error.variant_name(), "retention warm failed");
                }
                let _ = reply.send(warm_error_outcome(error, request_body_sha256));
                return;
            }
        };
        let Some(writer) = self.writer.clone() else {
            let _ = reply.send(WarmOutcome::Error(
                "the session's writer closed before the warm was recorded".into(),
            ));
            return;
        };
        // The expiry counts from the warm's own send start.
        let expires_at_ms = sent.sent_at_ms.saturating_add(ttl_ms);
        self.warm_in_flight.note_headers(
            sent.sent_at_ms,
            sent.ratelimit_headers.iter().cloned().collect(),
        );
        writer.record(WalRecord::CacheWarmed {
            trigger: None,
            run_id: run_id.clone(),
            step_id,
            request_body_sha256: request_body_sha256.clone(),
            warm_body_sha256: sent.warm_body_sha256.clone(),
            usage: sent.usage,
            served_account_ref: sent.served_account_ref.clone(),
            ratelimit_headers: sent.ratelimit_headers.iter().cloned().collect(),
            sent_at_ms: sent.sent_at_ms,
            ttl_ms: Some(ttl_ms),
            expires_at_ms: Some(expires_at_ms),
        });
        self.registry
            .cache_clock(&self.key)
            .note_sent(sent.sent_at_ms, Some(ttl_ms));
        self.registry
            .cache_clock(&self.key)
            .note_usage(sent.sent_at_ms, sent.usage);
        let warmed_run_active = matches!(
            &self.state,
            RunState::Active { run_id: active, .. } if *active == run_id
        );
        if let Err(error) = writer.sync().await {
            // The provider answered (and billed) the warm; only its record is in doubt.
            tracing::warn!(target: "cache", "cache warm record not synced: {error}");
        } else if !warmed_run_active {
            self.spawn_live_usage_export(run_id);
        }
        let _ = reply.send(WarmOutcome::Warmed(broca_wire::WarmReply {
            request_body_sha256,
            warm_body_sha256: sent.warm_body_sha256,
            cache_read_tokens: sent.usage.cached_input_tokens,
            cache_write_tokens: sent.usage.cache_write_tokens,
            input_tokens: sent.usage.input_tokens,
            ttl_ms,
            expires_at_ms,
            expiry_basis: broca_wire::EXPIRY_BASIS_ESTIMATED.into(),
        }));
    }

    /// Remember what the episode that admitted the session declares about its prompt cache, so
    /// the reply to the admitting send carries it. Reads the session's WAL, which holds little
    /// more than that episode's start at this point.
    async fn note_admitted_cache(&self, run_id: &RunId) {
        if !self.registry.declares_cache() {
            return;
        }
        let Ok(records) = self.read_wal_records().await else {
            return;
        };
        let declaration = records.iter().find_map(|record| match record {
            WalRecord::RunStarted {
                run_id: started,
                config,
                ..
            } if started == run_id => self.registry.cache_declaration(config),
            _ => None,
        });
        if let Some(declaration) = declaration {
            self.registry
                .note_admitted_cache(&self.key, run_id.clone(), declaration);
        }
    }

    async fn on_cancel_at(
        &mut self,
        run_id: &RunId,
        at: broca_wal::record::RunCancelAt,
    ) -> CancelOutcome {
        if let Err(error) = self.wal_store.check_move(&self.key, true) {
            return match error.into_move_refusal() {
                Ok(broca_wal::migration::MoveRefusal::Moving { cut_id }) => {
                    CancelOutcome::SessionMoving { cut_id }
                }
                Ok(broca_wal::migration::MoveRefusal::Moved { holder }) => {
                    CancelOutcome::SessionMoved { holder }
                }
                Err(error) => CancelOutcome::Error(error.to_string()),
            };
        }
        if let RunState::Active {
            run_id: active,
            cancel,
            steer,
            durable_finish,
            ..
        } = &self.state
        {
            if active == run_id {
                if durable_finish.get().is_some() {
                    return CancelOutcome::NotActive;
                }
                if cancel.is_cancelled() {
                    return CancelOutcome::Ack;
                }
                if at == broca_wal::record::RunCancelAt::StepBoundary {
                    let writer = self
                        .writer
                        .as_ref()
                        .expect("an active actor owns its writer");
                    if let Err(error) = steer.request_boundary_cancel(writer, run_id).await {
                        return CancelOutcome::Error(error.to_string());
                    }
                    self.pending_cancels.insert(
                        run_id.clone(),
                        self.skip_materialization_runs.contains(run_id),
                    );
                    return CancelOutcome::Ack;
                }
                if steer.boundary_cancel().is_cancelled() {
                    let writer = self
                        .writer
                        .as_ref()
                        .expect("an active actor owns its writer");
                    if let Err(error) = writer
                        .record(WalRecord::RunCancelRequested {
                            run_id: run_id.clone(),
                            at,
                        })
                        .into_result()
                    {
                        return CancelOutcome::Error(error.to_string());
                    }
                    if let Err(error) = writer.sync().await {
                        return CancelOutcome::Error(error.to_string());
                    }
                }
                cancel.cancel();
                self.pending_cancels.insert(
                    run_id.clone(),
                    self.skip_materialization_runs.contains(run_id),
                );
                return CancelOutcome::Ack;
            }
        }

        let resolved = match self.resolve_run_state(run_id).await {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(target: "lifecycle", "cancel lifecycle resolution failed: {error}");
                return CancelOutcome::Error(error);
            }
        };
        if !resolved.is_some_and(|state| state.state == RunIndexState::Paused) {
            return CancelOutcome::NotActive;
        }

        let skip_materialization = self.skip_materialization_runs.contains(run_id);
        if at == broca_wal::record::RunCancelAt::StepBoundary {
            let writer = self.writer.as_ref().expect("an open actor owns its writer");
            if let Err(error) = writer
                .record(WalRecord::RunCancelRequested {
                    run_id: run_id.clone(),
                    at,
                })
                .into_result()
            {
                return CancelOutcome::Error(error.to_string());
            }
            if let Err(error) = writer.sync().await {
                return CancelOutcome::Error(error.to_string());
            }
        }
        self.pending_cancels
            .insert(run_id.clone(), skip_materialization);
        match self
            .start_paused_cancel_finalizer(run_id, skip_materialization)
            .await
        {
            Ok(()) => CancelOutcome::Ack,
            Err(error) => {
                tracing::warn!(
                    target: "lifecycle",
                    "paused-run cancel finalizer failed to start: {error}"
                );
                CancelOutcome::Error(error)
            }
        }
    }

    async fn retry_pending_cancels(&mut self) -> Result<(), String> {
        let pending: Vec<_> = self
            .pending_cancels
            .iter()
            .map(|(run, skip)| (run.clone(), *skip))
            .collect();
        for (run, skip) in pending {
            self.start_paused_cancel_finalizer(&run, skip).await?;
            if matches!(self.state, RunState::Active { .. }) {
                break;
            }
        }
        Ok(())
    }

    async fn start_paused_cancel_finalizer(
        &mut self,
        run_id: &RunId,
        skip_materialization: bool,
    ) -> Result<(), String> {
        // Rebuild the original episode from the durable admission view. This works both after a
        // same-process pause and after the actor has closed and reopened with no in-memory request.
        self.refresh_admission_view().await?;
        if self
            .prior_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.checkpoint_at_head.last_episode.as_ref())
            .is_some_and(|episode| {
                episode.run_id == *run_id && episode.disposition == EpisodeDisposition::Finished
            })
        {
            self.pending_cancels.remove(run_id);
            return Ok(());
        }
        let episode = self
            .prior_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.checkpoint_at_head.last_episode.as_ref())
            .filter(|episode| {
                episode.run_id == *run_id && episode.disposition == EpisodeDisposition::Paused
            })
            .ok_or_else(|| "run is no longer durably paused".to_string())?;
        let mut request = submission_from_episode(episode);
        let tools_were_omitted = episode.config.tools_were_omitted
            || self
                .admitted_request_identities
                .get(run_id)
                .is_some_and(|identity| identity.tools_were_omitted);
        if tools_were_omitted {
            request.tools = None;
        }
        // The episode's own `RunStarted` holds the digest; the run index is asked only for an
        // episode recorded before that field existed.
        let request_identity_digest = match episode.request_identity_digest.clone() {
            Some(digest) => Some(digest),
            None => self
                .lookup_indexed_run(run_id)
                .await?
                .and_then(|row| row.request_identity_digest),
        };

        // Core checks this pre-cancelled token before the auth-resolved branch. The ordinary
        // resume executor therefore reconstructs AuthRequired and seals the existing episode with
        // RunFinished::Cancelled without needing an auth-resolved signal or a second RunStarted.
        self.start_run_with_cancel(
            run_id.clone(),
            request,
            RunKind::FreshOrResume,
            None,
            SendIdentity {
                title: None,
                conversation_key: None,
                send_id: None,
                request_identity_digest,
            },
            StartRunFlags {
                skip_materialization,
                cancel_before_execute: true,
                input_delivery: broca_protocol::Delivery::Queue,
            },
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
    }

    /// Resolve a specific run_id's lifecycle state with the THREE-TIER resolution that keeps
    /// the durable run index (a derived projection) from ever reporting a stale/non-terminal
    /// state as authoritative. Tier 1: a LIVE actor running this run_id is authoritatively
    /// `Active`. Tier 2: else if the index says TERMINAL, trust it (terminals don't go
    /// stale-forward). Tier 3: else (the index says ACTIVE or PAUSED but no live actor here,
    /// OR the index misses) RECONCILE against
    /// the WAL (the authority) — a crash between a run's durable RunFinished and the index
    /// terminal write leaves the row Active for a finished run, so an unbacked Active must be
    /// re-derived from the WAL.
    /// `Ok(Some(state))` = resolved; `Ok(None)` = the run_id is genuinely unknown to this
    /// session (looked up successfully, no row + no WAL record); `Err` = a STORAGE failure
    /// (the index or WAL read errored) — surfaced as an error, NEVER collapsed into "unknown"
    /// (a consumer must be able to tell "this run doesn't exist" from "the store is down").
    async fn resolve_run_state(
        &mut self,
        run_id: &RunId,
    ) -> Result<Option<ReconciledRunState>, String> {
        // Tier 1: this actor is live; if it's running this run_id, it's authoritatively
        // Active (the index row, written at admission, agrees) -- unless the run's terminal is
        // already durable and only its completion is still on its way to this actor. That run
        // has ended, so it falls through: its index row still says Active, which sends it to the
        // WAL reconcile below, the same answer `run.result` reads.
        if let RunState::Active {
            run_id: active,
            step_progress,
            ..
        } = &self.state
        {
            if active == run_id
                && !self.restart_admitting
                && self.durable_finish_of_active(run_id).is_none()
            {
                let live_step = step_progress.lock().unwrap().clone();
                let title = self.live_run_title(run_id).await?;
                let (ceiling_deadline_ms, ride_wait_deadline_ms) = if self
                    .first_episode
                    .get()
                    .and_then(|first| first.manifest.as_ref())
                    .is_some_and(|manifest| broca_core::compaction::has_compaction(manifest))
                {
                    let checkpoint = self.load_checkpoint_at_head().await?;
                    let state = checkpoint.compaction.as_ref().and_then(|c| c.state());
                    (
                        state.and_then(|state| match state.ceiling_check {
                            Some(broca_core::compaction::CeilingCheck::Waiting {
                                deadline_ms,
                                ..
                            }) => Some(deadline_ms),
                            _ => None,
                        }),
                        state
                            .and_then(|state| state.ride_wait)
                            .map(|(_, deadline_ms)| deadline_ms),
                    )
                } else {
                    (None, None)
                };
                let head = self
                    .writer
                    .as_ref()
                    .expect("active writer")
                    .durable_head()
                    .load(Ordering::Acquire);
                self.approvals.wait_covered(head).await;
                let run_usage = match self.approvals.run_usage(run_id, head) {
                    Some(usage) => usage,
                    // A checkpoint-only startup may omit the run's earlier model steps.
                    // Reconcile the full WAL rather than report a partial cache as complete.
                    None => self
                        .reconcile_from_wal(run_id)
                        .await?
                        .map(|state| state.run_usage)
                        .unwrap_or_default(),
                };
                return Ok(Some(ReconciledRunState {
                    run_usage,
                    budget_detail: None,
                    context_ceiling: None,
                    ceiling_deadline_ms,
                    structured_output: None,
                    live_step,
                    state: RunIndexState::Active,
                    undelivered_steers: None,
                    final_step_finish_reason: None,
                    indeterminate_tool_calls: None,
                    usage: None,
                    retries_used: 0,
                    error: None,
                    pause_reason: None,
                    auth_selection: self.approvals.auth_selection(run_id, head),
                    title,
                    ride_wait_deadline_ms,
                }));
            }
        }

        // Tier 2/3: consult the index (propagate a lookup ERROR rather than mask it).
        let indexed = self.lookup_indexed_run(run_id).await?;

        let reconciled = if indexed.as_ref().is_some_and(|row| row.state.is_terminal()) {
            self.terminal_wal_state(run_id).await?
        } else {
            self.reconcile_from_wal(run_id).await?
        };
        indexed_run_state(indexed, reconciled)
    }

    async fn terminal_wal_state(
        &mut self,
        run_id: &RunId,
    ) -> Result<Option<ReconciledRunState>, String> {
        if let Some(state) = self.terminal_status_cache.get(run_id) {
            return Ok(Some(state.clone()));
        }
        let head = self
            .writer
            .as_ref()
            .expect("writer live while resolving status")
            .durable_head()
            .load(Ordering::Acquire);
        let state = if let Some(snapshot) = self
            .prior_snapshot
            .as_ref()
            .filter(|snapshot| snapshot.base_watermark == 0 && snapshot.head_seq == head)
        {
            let records: Vec<_> = snapshot
                .tail
                .iter()
                .map(|framed| framed.record.clone())
                .collect();
            broca_core::reconcile_run_state(&records, &run_id.0)
        } else {
            self.reconcile_from_wal(run_id).await?
        };
        if let Some(state) = &state {
            if state.state.is_terminal() {
                self.terminal_status_cache
                    .insert(run_id.clone(), state.clone());
            }
        }
        Ok(state)
    }

    /// The title of the run this actor is driving. It comes from the run-index row. When that
    /// row has no title (it was written before the column existed, written by a resume, which
    /// writes no `RunStarted` and so has no title of its own, or lost), it comes from the run's
    /// `RunStarted` in the WAL, read once and then cached for the actor's lifetime.
    async fn live_run_title(&mut self, run_id: &RunId) -> Result<Option<String>, String> {
        if let Some(title) = self.approvals.title(run_id) {
            return Ok(title);
        }
        if let Some(title) = self.started_run_titles.get(run_id) {
            return Ok(title.clone());
        }
        if let Some(title) = self
            .lookup_indexed_run(run_id)
            .await?
            .and_then(|row| row.title)
        {
            return Ok(Some(title));
        }
        let title = self
            .reconcile_from_wal(run_id)
            .await?
            .and_then(|state| state.title);
        self.started_run_titles
            .insert(run_id.clone(), title.clone());
        Ok(title)
    }

    /// Point-lookup one run and enforce this actor's session boundary. The index is global, so a
    /// foreign row is deliberately indistinguishable from a miss.
    async fn lookup_indexed_run(&self, run_id: &RunId) -> Result<Option<RunIndexRow>, String> {
        let indexed = match self.run_index.clone() {
            Some(index) => spawn_blocking(JobKind::StoreRead, {
                let run_id = run_id.0.clone();
                move || index.lookup_run(&run_id)
            })
            .await
            .map_err(|error| format!("run-index lookup task failed: {error}"))?
            .map_err(|error| format!("run-index lookup failed: {error}"))?,
            None => None,
        };
        Ok(indexed.filter(|row| row.session == self.key))
    }

    /// Reconcile a run_id's real state from the durable WAL (the authority). A WAL READ
    /// FAILURE is an `Err` (a real storage problem), never "unknown" — only a successful
    /// read with no matching record is `Ok(None)`.
    async fn reconcile_from_wal(
        &self,
        run_id: &RunId,
    ) -> Result<Option<ReconciledRunState>, String> {
        let records = self.read_wal_records().await?;
        Ok(broca_core::reconcile_run_state(&records, &run_id.0))
    }

    /// A run reported a terminal. Act only if it matches the currently active task attempt.
    /// Both fields matter because a transient pre-admission failure can retry the same run id;
    /// the older task's delayed terminal must not finish the newer attempt.
    async fn on_run_finished(
        &mut self,
        run_id: RunId,
        attempt_id: u64,
        result: Result<RunSummary, broca_core::RunError>,
    ) -> bool {
        // The terminal is tagged with the run-driving TASK's id (the caller's req.run_id);
        // match on `task_run_id`, not the durable `run_id` (which differs on a resume).
        let (durable_run_id, active_cancelled, skip_materialization) = match &self.state {
            RunState::Active {
                task_run_id,
                attempt_id: active_attempt,
                run_id: durable,
                cancel,
                steer,
                skip_materialization,
                ..
            } if *task_run_id == run_id && *active_attempt == attempt_id => {
                // Drop from the actor's queue every ordered send this run delivered or ended.
                // The queue is re-read from the WAL below, but that re-read can be deferred, and a
                // stale entry would be handed to the next run and delivered a second time.
                let consumed = steer.take_consumed();
                if !consumed.is_empty() {
                    self.pending_fifo
                        .retain(|pending| !consumed.contains(&pending.submission_id));
                    // The delivery and end records were written by the run, not this actor.
                    self.wal_scan_complete = false;
                }
                (
                    durable.clone(),
                    cancel.is_cancelled() || steer.boundary_cancel().is_cancelled(),
                    *skip_materialization,
                )
            }
            // Stale terminal (backstop already fired, or a different/older run) — ignore.
            _ => return false,
        };
        let run_id = durable_run_id; // index the DURABLE id's terminal below
                                     // The run task has ended, so its conversation, rendered requests and step state are
                                     // freed. Let the idle memory hook hand those pages back to the system.
        broca_memory_relief::note_freed();

        // INVARIANT: once admission has succeeded, core writes and syncs RunFinished/RunPaused
        // before returning Ok(summary). An executor-level Err (Panicked, Wal, Lease, etc.) therefore
        // NEVER proves a durable terminal. Leave the index nonterminal and the actor Active, then
        // make this generation exit through emergency teardown. The next generation must replay the
        // authoritative WAL before it can resume this episode or drain a later queued submission.
        let summary = match &result {
            Ok(summary) => summary,
            Err(error) => {
                tracing::warn!(
                    target: "lifecycle",
                    "active run ended without a durable terminal; reopening for WAL recovery: {error}"
                );
                return true;
            }
        };

        // Cancel may arrive after the provider has decided to pause but before the task publishes
        // its paused summary. If RunPaused won that narrow durability race, immediately redrive the
        // durable AuthRequired episode with a pre-cancelled token instead of collapsing to Idle.
        if active_cancelled && summary.paused.is_some() {
            self.pending_cancels
                .insert(run_id.clone(), skip_materialization);
            self.set_idle();
            if let Err(error) = self
                .start_paused_cancel_finalizer(&run_id, skip_materialization)
                .await
            {
                tracing::warn!(
                    target: "lifecycle",
                    "pause-completion cancel finalizer failed to start: {error}"
                );
            }
            return false;
        }

        let index_state = run_index_terminal_state(summary);
        if index_state.is_terminal() {
            self.pending_cancels.remove(&run_id);
        }

        // Update the durable run-index to the terminal state BEFORE going Idle (so a
        // status/cancel lookup after the run never sees a stale Active for a finished run
        // while a live actor still backs it). Non-fatal: the WAL is the authority, and the
        // stale-Active reconciliation covers a crash between this and the WAL terminal.
        let owed = match self
            .index_terminal_from_wal(
                &run_id,
                index_state.clone(),
                (summary.steps > 0).then_some(summary.usage),
                summary.final_step_finish_reason,
                summary.retries_used,
            )
            .await
        {
            Ok(owed) => owed,
            Err(error) => {
                self.registry.repair_retention(&self.key);
                tracing::warn!(target: "store", "run-index terminal write failed (non-fatal): {error}");
                self.approvals.prepared(
                    &run_id,
                    self.writer
                        .as_ref()
                        .unwrap()
                        .durable_head()
                        .load(Ordering::Acquire),
                ) != Some(false)
            }
        };

        let _ = self.sync_retention_index().await;
        let should_emit_usage = index_state.is_terminal();
        let capped_export = should_emit_usage && self.has_matching_caps();
        if capped_export {
            self.pending_capped_export = Some(run_id.clone());
        }
        self.set_idle();
        // Only a terminal ends a one-shot's business with this session; a pause stays resumable.
        self.close_after_one_shot = skip_materialization && index_state.is_terminal();
        if owed {
            if let Some(task) = &self.withdrawal_task {
                task.wake();
            }
        }
        if should_emit_usage {
            self.approvals.finished(&run_id);
        }

        let export_ready = if capped_export {
            match self.flush_capped_export_barrier().await {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(
                        target: "export",
                        "capped usage export failed; queued-run drain deferred: {error}"
                    );
                    false
                }
            }
        } else {
            true
        };
        let admission_ready = match self.refresh_admission_view().await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(target: "admission", "post-run admission refresh deferred: {error}");
                false
            }
        };
        if export_ready && admission_ready {
            if let Err(error) = self.drain_queue_one().await {
                tracing::warn!(target: "admission", "queued-run drain deferred: {error}");
            }
        }
        if should_emit_usage && !capped_export {
            // The terminal WAL is durable and the synchronous run-index update above has finished.
            // Startup catch-up retries indexed runs whose export task was lost. Reopening a
            // finished episode retries runs whose index write failed. If no admission cap depends
            // on the export watermark, export I/O can remain off the queue-drain path.
            self.spawn_live_usage_export(run_id);
        }
        false
    }

    /// Drain queue heads in FIFO order until one starts or the queue is empty. A request/config
    /// error cannot improve while its durable bytes stay unchanged, so resolve that head as an
    /// atomic failed tombstone and continue to the next submission. Retryable failures leave the
    /// head untouched for startup backoff or a later idle-tick attempt.
    async fn drain_queue_one(&mut self) -> Result<(), String> {
        if self.registry.is_stopping() {
            return Ok(());
        }
        self.flush_capped_export_barrier().await?;
        self.expire_ordered_sends().await?;
        loop {
            // An ordered send without `idle_start` never starts a run: it stays queued for the
            // boundaries of a run other work starts.
            let Some(first) = self
                .pending_fifo
                .iter()
                .find(|pending| pending.starts_runs())
                .cloned()
            else {
                return Ok(());
            };
            // An interrupt queued behind a paused run ends that run first (it ends `Cancelled`,
            // and its terminal drains the interrupt). This also completes an interrupt whose
            // process died between making its message durable and ending the paused run.
            if first.is_interrupt() {
                if let Some((paused, _)) = self.paused_run().await? {
                    self.end_paused_run_for_interrupt(&paused).await;
                    return Ok(());
                }
            }
            // `queue_submission` derives `sub-<run_id>`, so stripping that coordinator prefix
            // recovers the identity selected by the idle-send path.
            let run_id = run_id_of_queued(&first.submission_id);
            let submission_id = first.submission_id;
            let skip_materialization = self
                .skip_materialization_submissions
                .contains(&submission_id);
            // Legacy queue records predate the marker but still contain the complete caller
            // request, so their identity is verifiable without a WAL scan. Compute it once at
            // drain and persist it in the new run-index row. This is off the fresh-send path.
            let request_identity_digest = first
                .request_identity_digest
                .or_else(|| Some(submission_identity_digest(&first.request)));
            // The run records the principal that queued this submission. The request itself
            // never serializes a principal, so after the queue is rebuilt from the WAL or the
            // admission checkpoint only the pending entry still holds it.
            let mut request = first.request;
            request.principal = first.principal;
            // The owner check ran when the send was queued; the drained episode only carries
            // the scope the session froze (a queued send never freezes one). The exception is an
            // ordered send starting its run during its own admission: it is a direct start, and
            // its route's scope is frozen like any direct send's.
            request.scope = match self.immediate_ordered_drain.take() {
                Some((immediate, scope)) if immediate == submission_id => scope,
                _ => broca_protocol::SendScope::Internal,
            };
            match self
                .start_run(
                    run_id.clone(),
                    request,
                    RunKind::AppendEpisode,
                    Some(submission_id.clone()),
                    // The drained episode's `RunStarted` records the key and the title the send
                    // was queued with, taken from its `Queued` record when the queue was rebuilt
                    // after a restart.
                    SendIdentity {
                        conversation_key: first.conversation_key,
                        title: first.title,
                        send_id: first.send_id,
                        request_identity_digest,
                    },
                    skip_materialization,
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(StartRunFailure::ContextCeiling(detail)) => {
                    if !self.ceiling_drain_held {
                        tracing::info!(session_ref = %broca_core::agent_session_ref(&self.key), size = detail.size, window = detail.window, cause = %detail.cause, "queue drain held at context ceiling");
                        self.ceiling_drain_held = true;
                    }
                    return Ok(());
                }
                Err(StartRunFailure::Admission(error)) if queued_admission_is_permanent(&error) => {
                    // Tombstone only failures intrinsic to the durable request. Export lag is an
                    // external, recoverable dependency state, so a trailing watermark must leave
                    // the queue head intact for the next drain tick.
                    self.persist_failed_queued_admission(
                        submission_id,
                        run_id,
                        error.to_string(),
                        "queued_admission_failed",
                    )
                    .await?;
                }
                // A queued send whose run could only start unconfined can never start: end it, so
                // the queue behind it keeps draining and the sender sees the refusal.
                Err(StartRunFailure::Scope(
                    refusal @ broca_core::ScopeRefusal::ScopeRequired { .. },
                )) => {
                    self.persist_failed_queued_admission(
                        submission_id,
                        run_id,
                        format!("scope refused: {refusal:?}"),
                        "scope_required",
                    )
                    .await?;
                }
                Err(error) => return Err(error.to_string()),
            }
        }
    }

    /// End every queued ordered send whose expiry has passed: `SendEnded {expired}`, synced, and
    /// dropped from the queue, so it is never delivered.
    async fn expire_ordered_sends(&mut self) -> Result<(), String> {
        let now = self.ordered_now_ms();
        let expired: Vec<_> = self
            .pending_fifo
            .iter()
            .filter(|pending| {
                pending
                    .order
                    .as_ref()
                    .is_some_and(|order| order.expired_at(now))
            })
            .map(|pending| pending.submission_id.clone())
            .collect();
        if expired.is_empty() {
            return Ok(());
        }
        let writer = self.writer.as_ref().expect("writer live while open");
        for submission_id in &expired {
            writer.record(broca_core::delivery_order::ended(
                submission_id,
                broca_wal::record::SendEndReason::Expired,
            ));
        }
        writer.sync().await.map_err(|error| error.to_string())?;
        self.pending_fifo
            .retain(|pending| !expired.contains(&pending.submission_id));
        for submission_id in expired {
            self.skip_materialization_submissions.remove(&submission_id);
            if self.wal_scan_complete {
                self.ended_submissions
                    .insert(submission_id, broca_wal::record::SendEndReason::Expired);
            }
        }
        self.prior_snapshot = None;
        Ok(())
    }

    /// The wall clock ordered-send expiry is compared against: the retention clock when one is
    /// installed (tests drive it), else the system clock.
    fn ordered_now_ms(&self) -> u64 {
        self.registry
            .retention_service()
            .map_or_else(epoch_ms, |service| service.clock.now_ms())
    }

    async fn persist_failed_queued_admission(
        &mut self,
        submission_id: broca_protocol::SubmissionId,
        run_id: RunId,
        message: String,
        provider_code: &'static str,
    ) -> Result<(), String> {
        let terminal = ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Permanent,
            message,
            status: None,
            retry_after_secs: None,
            provider_code: Some(provider_code.into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        let writer = self.writer.as_ref().expect("writer live while open");
        writer.record(WalRecord::Retracted {
            submission_id: submission_id.clone(),
            failure: Some(QueuedAdmissionFailure {
                run_id,
                error: terminal,
            }),
        });
        writer.sync().await.map_err(|error| error.to_string())?;
        self.pending_fifo
            .retain(|pending| pending.submission_id != submission_id);
        self.skip_materialization_submissions.remove(&submission_id);
        self.retracted_submissions.insert(submission_id);
        // Force the next head to fold the just-written tombstone before admission. This keeps its
        // snapshot and queue view aligned even when several queued submissions drain in one pass.
        self.prior_snapshot = None;
        Ok(())
    }

    /// Close right after a one-shot run's terminal instead of waiting out the idle TTL. Every
    /// condition `try_close` already enforces still applies (no active run, subscriber, warm,
    /// owed withdrawal or pending cancel; queued work aborts the close). The checks here only
    /// skip claiming the registry slot when the close would abort anyway. Returns true when the
    /// actor closed and its loop must return.
    async fn close_finished_one_shot(&mut self) -> bool {
        if !std::mem::take(&mut self.close_after_one_shot) {
            return false;
        }
        if !matches!(self.state, RunState::Idle)
            || self.has_pending()
            || self.fanout.has_live_subscribers()
        {
            return false;
        }
        self.try_close().await
    }

    /// Attempt the synchronous close teardown. Returns true if the actor closed (caller
    /// returns), false if the close ABORTED (work raced in — keep serving).
    async fn try_close(&mut self) -> bool {
        if let Err(error) = self.recover_startup_admission_view().await {
            tracing::warn!(
                target: "admission",
                "startup durable-work recovery failed on idle tick; keeping actor open: {error}"
            );
            return false;
        }

        if matches!(self.state, RunState::Idle) && !self.pending_cancels.is_empty() {
            if let Err(error) = self.retry_pending_cancels().await {
                tracing::warn!(target: "lifecycle", "pending cancel finalizer deferred: {error}");
            }
            if !self.pending_cancels.is_empty() {
                return false;
            }
        }

        // Don't close while a run is active or a live subscriber is observing the session.
        // Check both before claiming the registry slot so subscribers never see a false close.
        // A warm in flight keeps the actor open too: its completion records through this
        // session's writer.
        if matches!(self.state, RunState::Active { .. })
            || self.fanout.has_live_subscribers()
            || self.warm_in_flight.occupied()
            || self.withdrawal_task.as_ref().is_some_and(|task| {
                task.pending(
                    self.writer
                        .as_ref()
                        .unwrap()
                        .durable_head()
                        .load(Ordering::Acquire),
                )
            })
        {
            return false;
        }
        // Claim the close: CAS Open{gen} → Closing. If the slot isn't Open{gen} (a re-open
        // already replaced us), just exit — nothing to tear down against the registry.
        let Some(close_token) = self.registry.begin_close(&self.key, self.gen) else {
            // Our slot is gone, so there is no registry close to commit, but this generation
            // still owns its subscribers, writer and lease. Tear them down in the same order
            // as a committed close before exiting: a lease released while the writer thread
            // still drains would let a replacement actor append beside it.
            self.emergency_teardown().await;
            return true;
        };

        // Drain any buffered commands, replying Reopen so none is silently dropped
        // (finding 6). A Send/Cancel that raced in here re-routes through the registry.
        // We also watch for durable pending work to ABORT the close.
        while let Ok(cmd) = self.rx.try_recv() {
            match cmd {
                LineageCmd::RepairRetention { reply } => {
                    let _ = reply.send(Err("session actor is closing".into()));
                }
                LineageCmd::Send { reply, .. } => {
                    let _ = reply.send(SendOutcome::Reopen);
                }
                LineageCmd::Import { reply, .. } => {
                    let _ = reply.send(ImportOutcome::Reopen);
                }
                LineageCmd::Cancel { reply, .. } => {
                    let _ = reply.send(CancelOutcome::Reopen);
                }
                LineageCmd::Retract { reply, .. } => {
                    let _ = reply.send(RetractOutcome::Reopen);
                }
                LineageCmd::ResumeRestart { reply } => {
                    let _ = reply.send(Err("session actor is closing".into()));
                }
                LineageCmd::Status { reply } => {
                    let _ = reply.send(LineageStatus::Closing);
                }
                LineageCmd::RunState { run_id, reply } => {
                    // Even while closing, a run-state lookup can be answered (it reads the
                    // index + WAL, not the live run state) — so a status racing close still
                    // resolves rather than forcing a reopen.
                    let _ = reply.send(self.resolve_run_state(&run_id).await);
                }
                LineageCmd::Subscribe { reply, .. } => {
                    // Subscribing during close re-routes (re-open) — never attach to a
                    // draining actor whose fan-out is about to be torn down.
                    let _ = reply.send(SubscribeOutcome::Reopen);
                }
                LineageCmd::RunFinished {
                    run_id,
                    attempt_id,
                    result,
                } => {
                    // A run finished right as we began closing — handle it and ABORT the
                    // close (it may have drained a queued submission into a new run).
                    self.on_run_finished(run_id, attempt_id, *result).await;
                    return self.abort_close(close_token);
                }
                LineageCmd::Warm { reply, .. } => {
                    let _ = reply.send(WarmOutcome::Reopen);
                }
                // Unreachable while closing (a warm in flight keeps the actor open), but answered
                // rather than dropped.
                LineageCmd::WarmFinished { warm } => {
                    let _ = warm.0.reply.send(WarmOutcome::Reopen);
                }
                LineageCmd::CutLock { reply } => {
                    drop(reply);
                }
                LineageCmd::IdleTick
                | LineageCmd::CompactionChanged { .. }
                | LineageCmd::ApplyCompaction { .. } => {}
                #[cfg(test)]
                LineageCmd::RetainedAdmissionIdentityState { reply } => {
                    let _ = reply.send((usize::MAX, usize::MAX));
                }
                #[cfg(test)]
                LineageCmd::PanicInActor => panic!("test: actor task panic"),
            }
        }

        // The pending cache changes only after queue/retract/consume durability barriers, so it
        // is already the flushed writer's queue view and needs no WAL scan here.
        if self.has_pending() {
            if let Err(error) = self.drain_queue_one().await {
                tracing::warn!(
                    target: "admission",
                    "queue drain failed while closing; aborting close: {error}"
                );
            }
            return self.abort_close(close_token);
        }
        // A run may have started from the drain → abort.
        if matches!(self.state, RunState::Active { .. }) {
            return self.abort_close(close_token);
        }

        // Commit the close — the synchronous teardown in its load-bearing ORDER:
        //   subscribers cancel+join → SESSION writer shutdown → drop lease → registry
        //   Closed. So once the registry shows the slot gone, there is provably no live
        //   subscriber AND no live writer on this actor — closing the lease-vs-subscriber
        //   and lease-vs-writer windows (the Oracle's findings 8/9/15 at the session
        //   level, strictly stronger than the per-run writer shutdown).
        self.teardown_subscribers().await;
        self.shutdown_writer().await; // drain + final fsync + join the SESSION writer
                                      // Let the executor release any per-session resources keyed off a SHARED handle (the
                                      // real executor sends a route GOODBYE so a shared subc consumer connection frees this
                                      // session's tool-route channel — else it leaks channels across sessions). Best-effort
                                      // and independent of the lease; done before the lease drops so the session is fully
                                      // wound down by the time the slot shows Closed. BOUNDED by a timeout: this awaits
                                      // external I/O (a wire send), and a stalled send must NOT hold the registry slot +
                                      // lease open indefinitely (which would block every re-open of the session). On timeout
                                      // we proceed with the close — the route channel lingers until the connection drops,
                                      // a bounded warm-resource cost, never a correctness issue.
        let _ = tokio::time::timeout(
            ON_SESSION_CLOSE_TIMEOUT,
            self.executor.on_session_close(&self.key),
        )
        .await;
        drop(self.lease.take()); // release the lease ONLY after the writer is fully down
        close_token.commit(); // registry slot → Closed; wake the re-open waiters
                              // The writer, fan-out and run state of this session are about to drop with the actor.
        broca_memory_relief::note_freed();
        true
    }

    /// Unexpected-exit teardown. The actor remains owned by the spawn wrapper while this runs, so
    /// both an actor panic and an undurable active-run result restore child/writer/lease ordering
    /// before the generation becomes reopenable.
    async fn emergency_teardown(&mut self) {
        // The lease is released only as the last step of an ordered close (idle close, expiry
        // or poisoned teardown). Once it is gone this generation has already stopped its
        // children, closed its writer and told the executor the session closed, and its
        // registry slot may already be reopenable. Running the executor close hook again would
        // retire routes a newer generation of the same session may have registered.
        if self.lease.is_none() {
            return;
        }
        self.stop_active_run().await;
        self.teardown_subscribers().await;
        self.shutdown_writer().await;
        let _ = tokio::time::timeout(
            ON_SESSION_CLOSE_TIMEOUT,
            self.executor.on_session_close(&self.key),
        )
        .await;
        drop(self.lease.take());
    }

    async fn teardown_poisoned(&mut self) {
        let close_token = self.registry.begin_close(&self.key, self.gen);
        // A poisoned writer cannot seal the active episode. Stop and join its task before taking
        // sole writer ownership, then close in the same child → writer → lease order as panic
        // teardown. Releasing the lease first would let a replacement append beside this child.
        self.stop_active_run().await;
        self.teardown_subscribers().await;
        self.shutdown_writer().await;
        let _ = tokio::time::timeout(
            ON_SESSION_CLOSE_TIMEOUT,
            self.executor.on_session_close(&self.key),
        )
        .await;
        drop(self.lease.take());
        if let Some(close_token) = close_token {
            close_token.commit();
        } else {
            self.registry.finalize_exit(&self.key, self.gen);
        }
    }

    /// Stop execution without the caller-cancellation token: that token writes Cancelled.
    /// Sync the writer before reading so an enqueued tool intent cannot disappear from the seal's
    /// history. RunFinished carries no tool-call set; normal projection derives it from that history.
    ///
    /// A provider attempt in flight is recorded first: the seal trips the run's shutdown stop
    /// signal (never its cancel token), the transport hands back that attempt's record, and the
    /// seal appends it just before the Interrupted terminal. Collecting it spends the write's
    /// five-second deadline, with a separate one-second handover limit. The aborted run's join
    /// is not timed out: sealing while it can still write would race the terminal.
    ///
    /// The five-second deadline bounds only the Interrupted write, not subscriber teardown or
    /// the writer join. These keep the slot and lease until complete, even after a failed write.
    /// Dropping the writer join early could release the lease while its dedicated thread still
    /// appends. The registry returns false at its total deadline; process exit then ends those
    /// tasks and threads and leaves the WAL authoritative for the next boot's recovery.
    async fn seal_for_shutdown(&mut self) {
        self.finish_restart_waiters(Err("shutdown interrupted restart admission".into()));
        let seal_deadline = tokio::time::Instant::now() + SHUTDOWN_SEAL_BOUND;
        let active = matches!(self.state, RunState::Active { .. });
        let mut sealed_attempt = None;
        let mut hook_seal = None;
        if let RunState::Active {
            handle,
            shutdown_seal,
            ..
        } = &mut self.state
        {
            sealed_attempt = shutdown_seal
                .seal_and_collect(SEALED_ATTEMPT_HANDOVER_BOUND)
                .await;
            hook_seal = Some(shutdown_seal.clone());
            handle.abort();
            let _ = handle.await;
        }
        let result = tokio::time::timeout_at(seal_deadline, async {
            if !active {
                let writer = self.writer.as_ref().expect("writer live during shutdown");
                writer.sync().await.map_err(|e| e.to_string())?;
                return Ok(());
            }
            // The seal took hook slots under the run's hand-over lock. Persist them before
            // close-out reads history, so it sees these terminals and cannot duplicate them.
            if let Some(seal) = hook_seal {
                seal.record_hook_fallbacks(
                    self.writer.as_ref().expect("writer live during shutdown"),
                )
                .await
                .map_err(|error| error.to_string())?;
            }
            self.write_interrupted_seal(sealed_attempt, SealCause::Shutdown)
                .await
                .map(|_| ())
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            let stage = if result.is_err() {
                crate::registry::SealFailureStage::SealDeadline
            } else if !active {
                crate::registry::SealFailureStage::FinalSync
            } else {
                crate::registry::SealFailureStage::Seal
            };
            self.registry.report_seal_failure(
                &self.key,
                self.gen,
                stage,
                format!("bound={SHUTDOWN_SEAL_BOUND:?}; result={result:?}"),
            );
            tracing::warn!(
                target: "lifecycle",
                session = %self.key.session,
                "shutdown seal incomplete; leaving WAL recovery authoritative: {result:?}"
            );
        }
        self.teardown_subscribers().await;
        self.shutdown_writer().await;
        self.set_idle();
        drop(self.lease.take());
    }

    /// Seal the startup crash-orphan pass's candidate, if this session has one: a run a
    /// crashed process left RUNNING, with no terminal, still listed active in the run index.
    /// Returns whether a terminal was written. Runs inside open-time recovery, before the actor
    /// takes any command, so it holds the session lease and cannot interleave with a send.
    ///
    /// The terminal is written by [`Self::write_interrupted_seal`], the same code the shutdown
    /// seal uses: an ungraceful exit ends the run exactly as a graceful one would have.
    async fn seal_crash_orphan(&mut self) -> Result<bool, String> {
        let Some(candidates) = self.crash_orphan_candidates.as_ref() else {
            return Ok(false);
        };
        let snapshot = self
            .prior_snapshot
            .as_ref()
            .expect("refresh installs an admission snapshot");
        // A marked run whose episode already ended in the WAL is not an orphan, but its index
        // row still reads active: the terminal was synced and the index write after it was
        // lost (a crash between the two, or a failed write). Bring the row up to the WAL.
        let finished_run = match snapshot.checkpoint_at_head.last_episode.as_ref() {
            Some(episode)
                if candidates.contains(&episode.run_id)
                    && matches!(episode.disposition, EpisodeDisposition::Finished) =>
            {
                Some(episode.run_id.clone())
            }
            _ => None,
        };
        let sealable = match snapshot.checkpoint_at_head.last_episode.as_ref() {
            // The index may still list an older episode's run; only the LAST episode can be
            // unfinished, so a mark for any other run is left alone.
            Some(episode)
                if candidates.contains(&episode.run_id) && episode.cancel_at.is_none() =>
            {
                let tail: Vec<WalRecord> = snapshot
                    .tail
                    .iter()
                    .map(|frame| frame.record.clone())
                    .collect();
                let resolution = broca_core::replay(&tail)
                    .map_err(|error| error.to_string())?
                    .resolution;
                crash_orphan_sealable(episode.disposition, &resolution)
            }
            _ => false,
        };
        let sealed = sealable
            && self
                .write_interrupted_seal(None, SealCause::CrashOrphan)
                .await?;
        if let Some(run_id) = finished_run {
            if self.repair_crash_orphan_index(&run_id).await {
                self.registry.report_crash_orphan_index_repaired();
            }
        }
        self.crash_orphan_candidates = None;
        if sealed {
            self.registry.report_crash_orphan_sealed();
        }
        Ok(sealed)
    }

    /// Check the full durable WAL for unanswered questions that closed calls still need to
    /// withdraw, then publish the run's terminal index state. The withdrawal task shares this
    /// lock so it cannot clear `withdrawals_owed` using a read older than this terminal write.
    async fn index_terminal_from_wal(
        &self,
        run_id: &RunId,
        state: RunIndexState,
        usage: Option<broca_protocol::Usage>,
        reason: Option<broca_protocol::FinishReason>,
        retries: u32,
    ) -> Result<bool, String> {
        let index = self.run_index.clone();
        let wal = self.wal_store.clone();
        let key = self.key.clone();
        let writer = self
            .writer
            .as_ref()
            .expect("writer live while indexing")
            .clone();
        let lock = self.withdrawal_index_lock.clone();
        let head = writer.durable_head().load(Ordering::Acquire);
        self.approvals.wait_covered(head).await;
        let prepared = self.approvals.prepared(run_id, head);
        let run_id = run_id.0.clone();
        let cache = self.approvals.clone();
        let job = self.actor_jobs.track(writer);
        spawn_blocking(JobKind::StoreWrite, move || {
            let _job = job;
            let _guard = lock.lock().map_err(|e| e.to_string())?;
            let mut owed = false;
            if state.is_terminal() && prepared != Some(false) {
                let projection =
                    crate::approval::read_projection(wal.as_ref(), &key, _job.writer())?;
                owed = crate::approval::owed_for_run(&projection, &run_id);
                if owed {
                    cache.debt_ready.store(true, Ordering::Release);
                }
                if let Some(index) = &index {
                    crate::approval::record_terminal_debt(index.as_ref(), &projection, &run_id)?;
                }
            }
            if let Some(index) = index {
                index.index_run_terminal(&run_id, state, usage, reason, retries)?;
            }
            Ok(owed)
        })
        .await
        .map_err(|e| e.to_string())?
    }

    /// Repair an index row that still reports Active after a crash, using the WAL's existing
    /// terminal without appending a second seal. Set its unanswered-question withdrawal flag
    /// before recording the terminal state, even when no store projection ran.
    async fn repair_crash_orphan_index(&self, run_id: &RunId) -> bool {
        let Some(index) = self.run_index.clone() else {
            return false;
        };
        let writer = self
            .writer
            .as_ref()
            .expect("writer live during recovery")
            .clone();
        let wal_store = self.wal_store.clone();
        let key = self.key.clone();
        let target = run_id.clone();
        let lock = self.withdrawal_index_lock.clone();
        let lineage = writer.lineage_id();
        let job = self.actor_jobs.track(writer);
        let repaired = spawn_blocking(JobKind::StoreWrite, move || -> Result<bool, String> {
            let _job = job;
            let _guard = lock.lock().map_err(|e| e.to_string())?;
            let head = _job.writer().durable_head().load(Ordering::Acquire);
            match index.lookup_run(&target.0)? {
                Some(row) if row.state == RunIndexState::Active => {}
                _ => return Ok(false),
            }
            let frames = wal_store
                .read_records_range(&key, 0, Some(head), None)
                .map_err(|e| e.to_string())?;
            let frames = crate::approval::complete_frames(frames, head)?;
            let projection = crate::approval::projection_from_frames(lineage, &frames)?;
            let records: Vec<WalRecord> = frames.into_iter().map(|frame| frame.record).collect();
            let prior = broca_core::replay(&records).map_err(|e| e.to_string())?;
            if prior.run_id.as_ref() != Some(&target) || !prior.run_finished_durable {
                return Ok(false);
            }
            // The terminal belongs to the most recent RunStarted before it; take the last one
            // recorded for this run.
            let mut current: Option<&RunId> = None;
            let mut terminal = None;
            for record in &records {
                match record {
                    WalRecord::RunStarted { run_id, .. } => current = Some(run_id),
                    WalRecord::RunFinished {
                        reason,
                        usage,
                        retries_used,
                        ..
                    } if current == Some(&target) => {
                        terminal = Some((*reason, *usage, *retries_used));
                    }
                    _ => {}
                }
            }
            let Some((reason, usage, retries_used)) = terminal else {
                return Ok(false);
            };
            // As the loop's own terminal write: no usage for a run that completed no step,
            // and the last step's provider finish reason.
            crate::approval::record_terminal_debt(index.as_ref(), &projection, &target.0)?;
            index.index_run_terminal(
                &target.0,
                run_finish_index_state(reason),
                (prior.completed_steps > 0).then_some(usage),
                prior.last_step_finish_reason,
                retries_used,
            )?;
            Ok(true)
        })
        .await
        .map_err(|e| e.to_string())
        .and_then(|result| result);
        match repaired {
            Ok(repaired) => {
                if repaired {
                    if let Some(task) = &self.withdrawal_task {
                        task.wake();
                    }
                }
                repaired
            }
            Err(error) => {
                tracing::warn!(
                    target: "lifecycle",
                    "crash-orphan pass could not bring run {}'s active index row up to its \
                     WAL terminal: {error}",
                    run_id.0,
                );
                false
            }
        }
    }

    /// End the latest RunStarted that has no synced RunFinished, then mark its
    /// run-index entry interrupted. Returns whether a terminal was written.
    ///
    /// Shutdown and startup crash recovery share this terminal-writing code. They use the
    /// same timestamp and accounting rules, but startup can first append ResumeStarted,
    /// whereas shutdown may append a handed-over provider attempt.
    ///
    /// `sealed_attempt` is the provider attempt the shutdown seal collected from a live run;
    /// the crash-orphan pass has none, because the process that ran it is gone.
    ///
    /// When a tool call is indeterminate (its intent is durable and its result is not, so it
    /// may have run), the seal first closes the open batch exactly as a run-loop resume does:
    /// an `outcome_unknown` result per such call, an interrupted result per planned call that
    /// never left the process, and batch commit markers.
    ///
    /// When no call is indeterminate but the last model step asked for tools and some were
    /// never dispatched (the run stopped between the step and a dispatch intent), the seal keeps
    /// that step in history: it completes the batch with a never-executed result for each call
    /// that has none, behind the same barrier as the terminal (see
    /// [`broca_core::close_out_pending_turn`]). Nothing already written is dropped.
    ///
    /// The indeterminate close-out's results sync before the terminal, and crash recovery
    /// normally writes ResumeStarted first. An expiring log may have space only for its
    /// reserved terminal: optional recovery/close-out appends can then be refused without
    /// blocking that terminal.
    /// The accepted prefix and any original dispatch intents remain intact; a full close-out
    /// sequence cannot be reserved because its tool-result payloads are not bounded here.
    async fn write_interrupted_seal(
        &self,
        sealed_attempt: Option<broca_core::SealedAttempt>,
        cause: SealCause,
    ) -> Result<bool, String> {
        // Use the terminal's existing diagnostic field without changing its Interrupted reason.
        // Recovery knows the old process left this run unsealed, not how that process ended.
        let error = (cause == SealCause::CrashOrphan).then(|| ProviderError { provider_details: None, provider_subcode: None,
        class: broca_protocol::ErrorClass::Transient,
        message: "the previous broca process ended without sealing this run: host crash, kill, or power loss".into(),
        provider_code: Some("previous_process_ended_unsealed".into()),
        status: None,
        retry_after_secs: None,
        refusal: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None, });
        self.write_interrupted_seal_with_error(sealed_attempt, cause, error)
            .await
    }

    async fn write_interrupted_seal_with_error(
        &self,
        sealed_attempt: Option<broca_core::SealedAttempt>,
        cause: SealCause,
        error: Option<ProviderError>,
    ) -> Result<bool, String> {
        let sealed_step = sealed_attempt.as_ref().map(|sealed| sealed.step_id);
        let writer = self.writer.as_ref().expect("writer live while sealing");
        writer.sync_for_seal().await.map_err(|e| e.to_string())?;
        let head = writer.durable_head().load(Ordering::Acquire);
        let wal_store = self.wal_store.clone();
        let key = self.key.clone();
        // Parsing/replay can be CPU-heavy on long sessions. Keep it off runtime threads so
        // both the shutdown seal's five-second limit and the global exit deadline remain
        // enforceable.
        let lineage = writer.lineage_id();
        let (prior, attempts_already_recorded, projection) =
            spawn_blocking(JobKind::WalRead, move || {
                let frames = wal_store
                    .read_records_range(&key, 0, Some(head), None)
                    .map_err(|e| e.to_string())?;
                let frames = crate::approval::complete_frames(frames, head)?;
                let projection = crate::approval::projection_from_frames(lineage, &frames)?;
                let records: Vec<_> = frames.into_iter().map(|frame| frame.record).collect();
                let recorded =
                    sealed_step.map(|step| attempts_recorded_for_latest_step(&records, step));
                broca_core::replay(&records)
                    .map(|prior| (prior, recorded, projection))
                    .map_err(|e| e.to_string())
            })
            .await
            .map_err(|e| e.to_string())??;
        if prior.run_id.is_none() || prior.run_finished_durable {
            return Ok(false);
        }
        if let Some(sealed) = sealed_attempt {
            // Ordinals within a step stay gapless and unique: the handed-over attempt
            // is written only when exactly its predecessors are already on record. An
            // earlier attempt's record can be missing when the aborted run task never
            // read it off the provider stream; writing this one anyway would leave a
            // hole, so it is dropped instead.
            if attempts_already_recorded == Some(sealed.attempt.attempt) {
                writer.record(WalRecord::model_attempt_finished(
                    sealed.step_id,
                    sealed.attempt,
                ));
            } else {
                tracing::warn!(
                    target: "lifecycle",
                    "shutdown seal dropped the in-flight attempt record: attempt {} of \
                     step {} but {:?} earlier attempts are on record",
                    sealed.attempt.attempt,
                    sealed.step_id.0,
                    attempts_already_recorded,
                );
            }
        }
        let close_out =
            broca_core::close_out_open_batch(&prior, |id| {
                if self.executor.elicitation_plane().is_some() {
                    if let Some(owed) = projection.calls.iter().find_map(|(key, call)| match &*call
                        .prepared
                    {
                        WalRecord::ToolCallDeferred {
                            batch_id, call_id, ..
                        } if call_id == id
                            && prior
                                .open_tool_batch
                                .as_ref()
                                .is_some_and(|batch| &batch.batch_id == batch_id) =>
                        {
                            Some(broca_wal::WithdrawElicitation {
                                call_key: key.clone(),
                                elicitation_id: call.elicitation_id.clone(),
                            })
                        }
                        _ => None,
                    }) {
                        return broca_core::DeferredCloseKind::WithdrawElicitation(owed);
                    }
                }
                broca_core::DeferredCloseKind::NotDispatched
            });
        let resuming = cause == SealCause::CrashOrphan;
        if resuming {
            writer.record(broca_core::resume_started_record(&prior, head, Vec::new()));
        }
        if !close_out.is_empty() {
            for record in close_out {
                writer.record(record);
            }
            // Sync the close-out records that were actually enqueued. If the byte cap
            // rejected later records, leave them unclaimed and still write RunFinished
            // through its reserved space. Recovery only deduplicates records present on disk.
            writer.sync_for_seal().await.map_err(|e| e.to_string())?;
        } else {
            // A durable tool turn whose calls were never dispatched is kept, not dropped: each
            // such call gets a never-executed result. No barrier here, so these records commit
            // with the terminal below; on their own they would read as a finished run.
            for record in broca_core::close_out_pending_turn(&prior) {
                writer.record(record);
            }
        }
        let terminal_seq = writer
            .record_terminal(WalRecord::RunFinished {
                interrupt_reason: Some(
                    match cause {
                        SealCause::CrashOrphan => "runner_crash",
                        _ => "runner_shutdown",
                    }
                    .into(),
                ),
                reason: broca_protocol::RunFinishReason::Interrupted,
                // Timestamp this terminal so exported spend belongs to its billing period.
                // A missing ts_ms becomes NULL occurred_at_ms and is omitted from period totals.
                ts_ms: self
                    .retention_timestamp()
                    .or_else(broca_core::run::epoch_millis),
                usage: prior.usage,
                error,
                retries_used: prior.retries_used,
            })
            .map_err(|error| error.to_string())?;
        writer.sync().await.map_err(|e| e.to_string())?;
        if let Some(run_id) = prior.run_id.clone() {
            self.executor.on_run_sealed(crate::executor::SealedRun {
                session: self.key.clone(),
                run_id,
                auth_selection: prior
                    .run_config
                    .as_ref()
                    .and_then(|config| config.auth_selection.clone()),
                lineage: writer.lineage_id(),
                terminal_seq,
            });
        }
        // TOUCH THE INDEX TOO. The WAL is the authority and tier-3a reconciles a stale
        // Active row on read, so nothing on the QUERY path was wrong when the seal
        // left the row alone -- but every reader that consumes run_index directly
        // (an operator's SELECT, a placement gate counting active runs) saw the sealed
        // run as live, forever. Measured 2026-09-18: four Active rows for runs that
        // were not, one of them the run this seal had terminated, and a gate reading
        // the index would never have found a quiet window on this host again. A
        // projection that is only ever right when read through its reconciler is a trap
        // for the next direct reader. After the durable write, so a crash between the
        // two leaves the WAL ahead of the index, never the reverse; the startup crash-orphan
        // pass repairs such a row from the WAL's terminal (see `repair_crash_orphan_index`).
        //
        // Once the sync above returns, the seal HAS happened: the terminal is durable and
        // every replay will see it. A failed index write after that point is logged, not
        // returned. Failing here would tell the crash-orphan pass the run was not sealed, so it
        // would neither count the seal nor clear its mark, and nothing would ever come back
        // to the row: the next open sees a finished episode and has nothing left to seal.
        if let (Some(_), Some(run_id)) = (self.run_index.clone(), prior.run_id.clone()) {
            // The row's usage column means "usage of a run that completed at least one
            // step"; the loop's own terminal writes None for zero-step runs (r99) and the
            // seal follows it, so a sealed pre-first-step run does not read as in=0 out=0.
            let usage = (prior.completed_steps > 0).then_some(prior.usage);
            let retries_used = prior.retries_used;
            let written = self
                .index_terminal_from_wal(
                    &run_id,
                    RunIndexState::Interrupted,
                    usage,
                    None,
                    retries_used,
                )
                .await;
            if let Err(error) = written {
                self.registry.repair_retention(&self.key);
                tracing::warn!(
                    target: "lifecycle",
                    "run {} sealed interrupted in the WAL, but its run-index row could not be \
                     updated and still reads active: {error}",
                    run_id.0,
                );
            }
        }
        if let Some(task) = &self.withdrawal_task {
            task.wake();
        }
        if let Some(service) = self.registry.retention_service() {
            let key = self.key.clone();
            let head = writer.durable_head().load(Ordering::Acquire);
            if !matches!(
                tokio::task::spawn_blocking(move || service.rederive_head(&key, Some(head))).await,
                Ok(Ok(()))
            ) {
                self.registry.repair_retention(&self.key);
            }
        }
        Ok(true)
    }

    /// Cancel, abort, and join an active run before the actor consumes the session writer.
    async fn stop_active_run(&mut self) {
        let state = std::mem::replace(&mut self.state, RunState::Idle);
        if let RunState::Active { cancel, handle, .. } = state {
            // Cooperative cancellation records intent for cancellation-aware work. Abort is still
            // required so a blocked child promptly drops RunJob and its session-writer Arc.
            if !self.registry.is_stopping() {
                cancel.cancel();
            }
            // abort() is idempotent for an already-completed task; awaiting it ensures the child
            // releases its writer Arc before shutdown_writer establishes sole ownership.
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Shut down the session-scoped writer (drain + final fsync + JOIN the writer thread).
    /// Called ONCE, in the close teardown, BEFORE the lease drops so another session owner
    /// cannot acquire the lease while this writer still writes. By close time `try_close`
    /// has confirmed no run is Active and the
    /// finished run task dropped its `Arc` clone before posting `RunFinished`, so the actor
    /// holds the SOLE `Arc` — `Arc::try_unwrap` yields the owned `WalHandle` to consume for
    /// a real `shutdown().await` (which joins the writer thread; a bare `sync()` would leave
    /// the thread alive). If a stray clone somehow remained (it shouldn't), fall back to a
    /// durable `sync()` barrier rather than block forever on the join.
    async fn shutdown_writer(&mut self) {
        if let Some(task) = self.withdrawal_task.take() {
            task.stop().await;
        }
        self.actor_jobs.join().await;
        let Some(arc) = self.writer.take() else {
            return;
        };
        let result = match Arc::try_unwrap(arc) {
            Ok(handle) => handle.shutdown().await, // drain + final fsync + join
            Err(arc) => {
                // SOLE-OWNER INVARIANT VIOLATED: a clone of the session writer outlived the
                // run at close time. This must NOT happen — by the teardown ordering the
                // run task drops its clone before posting RunFinished (processed before any
                // close, which bails while Active), and the queue-write uses `&self.writer`
                // (no clone). If the subscriber fan-out (or any future change) ever holds a
                // writer Arc clone, try_unwrap returns Err → the owned handle can't be taken
                // to JOIN the writer thread → it would stay alive past `drop(lease)` → the
                // single-writer close guarantee SILENTLY regresses (a live writer after the
                // lease drops).
                //
                // INVARIANT: subscriber tasks MUST read via the projection / WAL, NEVER hold
                // the session writer Arc. That is what keeps the actor the sole owner here.
                //
                // Fail LOUD in debug so a regression is caught in tests. In RELEASE, fail
                // CLOSED rather than silently skip: do a durable `sync()` barrier on the
                // surviving handle so every appended record is at least fsynced before the
                // lease drops (the worst residual is then an unjoined-but-idle writer thread,
                // not LOST durability). The thread itself drains + exits when its last Arc
                // drops.
                debug_assert!(
                    false,
                    "session writer had a surviving clone at close — sole-owner invariant \
                     violated (a subscriber/task is holding the writer Arc); the writer task \
                     won't be joined before the lease drops"
                );
                arc.sync().await.map(|_| ())
            }
        };
        if let Err(error) = result {
            self.registry.report_seal_failure(
                &self.key,
                self.gen,
                crate::registry::SealFailureStage::WriterShutdown,
                format!("writer shutdown failed: {error}"),
            );
            tracing::error!(target: "lifecycle", session = %self.key.session, "writer shutdown failed; WAL requires recovery: {error}");
        }
    }

    /// Re-arm: restore Open{gen} and keep serving.
    fn abort_close(&self, token: CloseToken) -> bool {
        token.abort(ActorHandle {
            approvals: self.approvals.clone(),
            gen: self.gen,
            tx: self.self_tx.clone(),
            durable_head: self
                .writer
                .as_ref()
                .expect("writer live while aborting close")
                .durable_head(),
            // The SAME gauge, not a fresh one: a re-armed close keeps serving the same actor, so
            // a new zeroed Arc here would silently report idle for a session that is running.
            run_in_flight: Arc::clone(&self.run_in_flight),
            first_episode: Arc::clone(&self.first_episode),
        });
        false
    }

    async fn recover_startup_admission_view(&mut self) -> Result<(), String> {
        if !self.startup_recovery_pending {
            return Ok(());
        }
        self.recover_durable_work().await?;
        self.startup_recovery_pending = false;
        Ok(())
    }

    /// Log once, on the lifecycle target, when this compaction session's stored transcript has
    /// passed `COMPACTION_SESSION_WATCH_MESSAGES`. Compaction carries on either way. The count
    /// is the store's materialized prefix, read from its metadata without decoding any message
    /// or WAL record, so it can trail the WAL by the turns not yet projected. Without a store,
    /// or when the read fails, nothing is logged and the next send checks again.
    async fn watch_compaction_session_size(&mut self) {
        let Some(store) = self.materialized_store.clone() else {
            return;
        };
        let key = self.key.clone();
        let counted = spawn_blocking(JobKind::StoreRead, move || {
            broca_core::MaterializedStore::materialized_message_count(store.as_ref(), &key)
        })
        .await;
        let Ok(Ok(Some(stored_messages))) = counted else {
            return;
        };
        if self.compaction_watch.observe(stored_messages) {
            tracing::info!(
                target: "lifecycle",
                session_ref = %broca_core::agent_session_ref(&self.key),
                stored_messages,
                threshold = crate::compaction_watch::COMPACTION_SESSION_WATCH_MESSAGES,
                "compaction session passed the stored-message watch threshold"
            );
        }
    }

    /// Who has driven the session, over its whole history up to the durable head, without
    /// touching the actor's admission view. The owner check reads it on every caller's send;
    /// with a materialized store it reads only the WAL tail past the store's watermark.
    async fn load_ownership(&self) -> Result<broca_core::SessionOwnershipV1, String> {
        self.load_checkpoint_at_head()
            .await?
            .ownership
            .ok_or_else(|| "the admission snapshot carries no ownership summary".to_owned())
    }

    fn agent_run_writer_enabled(&self) -> bool {
        #[cfg(all(test, feature = "test-clock"))]
        if self.agent_run_writer_for_test {
            return true;
        }
        broca_core::scope_admission::AGENT_RUN_SCOPES_WRITER_ENABLED
    }

    fn decide_session_scope(
        &self,
        ownership: &broca_core::SessionOwnershipV1,
        principal: Option<&broca_protocol::BindPrincipal>,
        send: &broca_protocol::SendScope,
    ) -> Result<broca_core::ScopeDecision, broca_core::ScopeRefusal> {
        #[cfg(all(test, feature = "test-clock"))]
        if self.agent_run_writer_for_test {
            return broca_core::scope_admission::decide_agent_run_scope_for_test(
                ownership, principal, send,
            );
        }
        broca_core::decide_scope(ownership, principal, send)
    }

    /// Seed the writer's runner-tag state from the session's history when nothing seeded it yet,
    /// so a record it appends outside a run (a steer into a paused run) is tagged like the rest.
    async fn seed_writer_tags(&self) -> Result<(), String> {
        let writer = self.writer.as_ref().expect("writer live while open");
        if writer.lineage_tags().is_none() {
            let checkpoint = self.load_checkpoint_at_head().await?;
            writer.seed_lineage_tags(checkpoint.tags);
        }
        Ok(())
    }

    /// The admission checkpoint of the session's history through its durable head, read without
    /// installing it as the actor's admission view.
    async fn load_checkpoint_at_head(&self) -> Result<broca_core::AdmissionCheckpointV1, String> {
        let safe_head = self
            .writer
            .as_ref()
            .expect("writer live while open")
            .sync()
            .await
            .map_err(|error| error.to_string())?;
        let wal_store = self.wal_store.clone();
        let materialized_store = self.materialized_store.clone();
        let key = self.key.clone();
        let checkpoint = spawn_blocking(JobKind::WalRead, move || {
            match broca_core::load_prior_wal_snapshot(
                wal_store.as_ref(),
                &key,
                safe_head,
                materialized_store.as_deref(),
            ) {
                Ok(snapshot) => Ok(snapshot.checkpoint_at_head),
                // The snapshot's complete-WAL fallback insists the file ends exactly at the
                // durable head, which an active run appending past it breaks. The owner check
                // only needs the history through that head, so fold exactly that prefix.
                Err(cause) => {
                    let range = wal_store
                        .read_records_range(&key, 0, Some(safe_head), None)
                        .map_err(|error| {
                            format!("{cause}; ownership prefix read failed: {error}")
                        })?;
                    let durable =
                        crate::durable_read::DurableRead::classify(&range, 0, Some(safe_head))
                            .records()
                            .map_err(|error| format!("{cause}; ownership prefix: {error}"))?;
                    broca_core::AdmissionCheckpointV1::from_frames(durable)
                        .map_err(|error| format!("{cause}; ownership prefix fold failed: {error}"))
                }
            }
        })
        .await
        .map_err(|error| format!("ownership snapshot task failed: {error}"))??;
        self.registry
            .note_session_ownership(&self.key, self.gen, &checkpoint);
        Ok(checkpoint)
    }

    /// Put the session's frozen model into a send that omitted `model` (its `inherit_frozen` set,
    /// the runner parameters taken from the session at admission, holds `Model`), before the
    /// send is recorded as a `Queued` record. The serve layer gives such a send an empty
    /// placeholder model. A build that predates `inherit_frozen` decodes a queued record without
    /// the set and starts the run on the recorded model, so with the placeholder it would fail;
    /// with the frozen model written in, it starts the run on the right model and takes the
    /// defaults for the other omitted parameters. This build still takes every omitted
    /// parameter from the session at admission, the model included.
    async fn record_frozen_model(&self, request: &mut SubmissionRequest) -> Result<(), String> {
        if !request
            .inherit_frozen
            .contains(&broca_protocol::request::InheritedParam::Model)
        {
            return Ok(());
        }
        let checkpoint = self.load_checkpoint_at_head().await?;
        let config = &checkpoint
            .last_episode
            .as_ref()
            .ok_or("a send left its model to the session, which has no episode to take it from")?
            .config;
        request.model = config.model.clone();
        request.variant.clone_from(&config.variant);
        Ok(())
    }

    async fn refresh_admission_view(&mut self) -> Result<(), String> {
        let safe_head = self
            .writer
            .as_ref()
            .expect("writer live while open")
            .sync()
            .await
            .map_err(|error| error.to_string())?;
        let wal_store = self.wal_store.clone();
        let materialized_store = self.materialized_store.clone();
        let key = self.key.clone();
        let mut snapshot = spawn_blocking(JobKind::WalRead, move || {
            broca_core::load_prior_wal_snapshot(
                wal_store.as_ref(),
                &key,
                safe_head,
                materialized_store.as_deref(),
            )
        })
        .await
        .map_err(|error| format!("admission snapshot task failed: {error}"))??;
        // Run reconstruction never reads the durable pending queue. Move its allocation into the
        // actor's queue state before sharing the slim checkpoint with executor/core clones.
        let mut pending_fifo = snapshot.take_pending_fifo();
        if !pending_fifo.is_empty() {
            self.ensure_wal_scan_loaded().await?;
            pending_fifo
                .retain(|pending| !self.retracted_submissions.contains(&pending.submission_id));
        }
        self.pending_fifo = pending_fifo;
        self.skip_materialization_submissions
            .retain(|submission_id| {
                self.pending_fifo
                    .iter()
                    .any(|pending| &pending.submission_id == submission_id)
            });
        self.retention_lineage = matches!(
            snapshot.checkpoint_at_head.lineage_retention,
            Some(broca_core::LineageRetention::Retained { .. })
        );
        if !self.approval_seeded {
            let lineage = self.writer.as_ref().unwrap().lineage_id();
            let prefix = if snapshot.base_watermark > 0 {
                if let Some(store) = self.materialized_store.clone() {
                    let key = self.key.clone();
                    spawn_blocking(JobKind::StoreRead, move || {
                        store.read_page_with_watermark(
                            &key,
                            Some(0),
                            0,
                            broca_core::context::ReadView::Raw,
                        )
                    })
                    .await
                    .ok()
                    .and_then(Result::ok)
                } else {
                    None
                }
            } else {
                None
            };
            let complete = snapshot.base_watermark == 0
                || prefix.as_ref().is_some_and(|prefix| {
                    prefix.watermark_seq >= snapshot.base_watermark
                        && prefix.watermark_seq <= snapshot.head_seq
                });
            let seed = prefix
                .as_ref()
                .and_then(|prefix| prefix.deferred_calls.as_ref());
            let current = snapshot
                .checkpoint_at_head
                .last_episode
                .as_ref()
                .map(|episode| episode.run_id.clone());
            self.approvals.seed(
                lineage,
                snapshot.head_seq,
                current.clone(),
                &snapshot.tail,
                seed,
                complete,
            );
            self.approval_debt = if complete {
                let identity = lineage.map(|id| id.to_string());
                let view = broca_core::replay::project_deferred_calls(
                    seed,
                    &snapshot.tail,
                    identity.as_deref(),
                    current.as_ref(),
                );
                let mut runs: HashSet<_> = view
                    .as_ref()
                    .into_iter()
                    .flat_map(|view| view.calls.values().filter_map(|call| call.run_id.clone()))
                    .collect();
                if let Some(current) = &current {
                    runs.insert(current.clone());
                }
                let debt = view
                    .as_ref()
                    .is_some_and(|view| !view.obligations().withdrawals.is_empty());
                let flagged = if let Some(index) = self.run_index.clone() {
                    spawn_blocking(JobKind::StoreRead, move || -> Result<bool, String> {
                        for run_id in runs {
                            if index
                                .lookup_run(&run_id.0)?
                                .is_some_and(|row| row.withdrawals_owed)
                            {
                                return Ok(true);
                            }
                        }
                        Ok(false)
                    })
                    .await
                    .ok()
                    .and_then(Result::ok)
                } else {
                    Some(false)
                };
                flagged.map(|flagged| debt || flagged)
            } else {
                None
            };
            self.approval_seeded = true;
        }
        self.agent_run_scope = snapshot
            .checkpoint_at_head
            .ownership
            .as_ref()
            .and_then(|ownership| ownership.scope.as_ref())
            .filter(|scope| scope.run_id.is_some() || scope.agent_id.is_some())
            .cloned()
            .map(Box::new);
        self.registry
            .note_session_ownership(&self.key, self.gen, &snapshot.checkpoint_at_head);
        // The writer also appends outside a run (a crash or shutdown seal, a steer into a paused
        // run), so its runner-tag state comes from the same history as this admission view.
        if let Some(writer) = &self.writer {
            let tags = match snapshot.checkpoint_at_head.tags {
                Some(tags @ broca_core::LineageTags::Unstarted { .. }) => Some(
                    tags.with_floor(
                        self.wal_store
                            .retired_tag_floor(&self.key)
                            .map_err(|error| error.to_string())?,
                    ),
                ),
                other => other,
            };
            writer.seed_lineage_tags(tags);
        }
        self.prior_snapshot = Some(snapshot);
        Ok(())
    }

    fn has_matching_caps(&self) -> bool {
        self.cap_store.as_ref().is_some_and(|store| {
            !store
                .matching_caps(&self.key.harness, &self.key.project_root)
                .is_empty()
        })
    }

    async fn flush_capped_export_barrier(&mut self) -> Result<(), String> {
        let Some(run_id) = self.pending_capped_export.clone() else {
            return Ok(());
        };
        self.emit_usage_export(&run_id).await?;
        self.pending_capped_export = None;
        Ok(())
    }

    async fn emit_usage_export(&self, run_id: &RunId) -> Result<(), String> {
        let (Some(export_index), Some(resolver)) = (
            self.export_index.clone(),
            self.charge_basis_resolver.clone(),
        ) else {
            return Ok(());
        };
        Self::emit_usage_export_owned(
            self.wal_store.clone(),
            self.key.clone(),
            export_index,
            resolver,
            run_id.clone(),
        )
        .await
    }

    fn spawn_live_usage_export(&self, run_id: RunId) {
        let (Some(export_index), Some(resolver)) = (
            self.export_index.clone(),
            self.charge_basis_resolver.clone(),
        ) else {
            return;
        };
        let wal_store = self.wal_store.clone();
        let key = self.key.clone();
        tokio::spawn(async move {
            if let Err(error) =
                Self::emit_usage_export_owned(wal_store, key, export_index, resolver, run_id).await
            {
                tracing::warn!(target: "export", "usage export emission failed (non-fatal): {error}");
            }
        });
    }

    async fn emit_usage_export_owned(
        wal_store: Arc<W>,
        key: LineageKey,
        export_index: Arc<dyn broca_core::ExportIndex>,
        resolver: Arc<dyn broca_core::ChargeBasisResolver + Send + Sync>,
        run_id: RunId,
    ) -> Result<(), String> {
        let records = spawn_blocking(JobKind::WalRead, move || wal_store.read_records(&key))
            .await
            .map_err(|error| format!("wal read task failed: {error}"))?
            .map_err(|error| format!("wal read failed: {error}"))?;
        spawn_blocking(JobKind::StoreWrite, move || {
            export_index.emit_run(&records, &run_id, resolver.as_ref())
        })
        .await
        .map_err(|error| format!("usage export task failed: {error}"))?
        .map_err(|error| error.to_string())
    }

    async fn read_wal_framed_records(&self) -> Result<Vec<broca_wal::FramedRecord>, String> {
        // Run-status reconciliation must not interpret a complete page-cache tail as a durable
        // terminal. A live actor already owns a writer proof, so bound the read at that head
        // rather than paying a second fsync or reading unbounded complete frames.
        let safe_head = self
            .writer
            .as_ref()
            .expect("writer live while reconciling WAL")
            .durable_head()
            .load(Ordering::Acquire);
        let wal_store = self.wal_store.clone();
        let key = self.key.clone();
        spawn_blocking(JobKind::WalRead, move || {
            let range = wal_store
                .read_records_range(&key, 0, Some(safe_head), None)
                .map_err(|error| format!("wal read failed: {error}"))?;
            crate::durable_read::DurableRead::classify(&range, 0, Some(safe_head))
                .records()
                .map(<[_]>::to_vec)
        })
        .await
        .map_err(|e| format!("wal read task failed: {e}"))?
    }

    fn replace_wal_scan_sets(&mut self, records: &[WalRecord]) {
        self.retracted_submissions = records
            .iter()
            .filter_map(|record| match record {
                WalRecord::Retracted { submission_id, .. } => Some(submission_id.clone()),
                _ => None,
            })
            .collect();
        self.recorded_send_keys = recorded_send_keys(records);
        self.recorded_steers = recorded_steers(records);
        self.ended_submissions = records
            .iter()
            .filter_map(|record| match record {
                WalRecord::SendEnded {
                    submission_id,
                    reason,
                } => Some((submission_id.clone(), reason.clone())),
                _ => None,
            })
            .collect();
        self.ordered_submissions = records
            .iter()
            .filter_map(|record| match record {
                WalRecord::DeliveryOrdered {
                    submission_id,
                    order,
                    ..
                } => Some((submission_id.clone(), order.clone())),
                _ => None,
            })
            .collect();
        self.delivered_submissions = records
            .iter()
            .filter_map(|record| match record {
                WalRecord::SendDelivered {
                    submission_id,
                    run_id,
                    ..
                } => Some((submission_id.clone(), run_id.clone())),
                _ => None,
            })
            .collect();
        self.wal_scan_complete = true;
    }

    /// What the WAL records for a steer sent under `send_id`, if one was.
    async fn recorded_steer(&mut self, send_id: &str) -> Result<Option<RecordedSteer>, String> {
        self.ensure_wal_scan_loaded().await?;
        Ok(self.recorded_steers.get(send_id).cloned())
    }

    /// Load the durable tombstone set and the recorded send keys, at most once per actor.
    ///
    /// This reads the whole WAL, which admission otherwise avoids: the checkpoint carries a
    /// pending list rather than a tombstone list, because a retraction REMOVES its entry there
    /// instead of recording that it existed. A retracted identity is therefore indistinguishable
    /// from one never seen, and only the records themselves still hold the distinction. The same
    /// holds for the send key of an episode older than the latest: only its `RunStarted` holds it.
    ///
    /// The cost is bounded by three things and none of them are per-send. A fresh lineage skips
    /// it entirely (nothing can be retracted before anything is written). This actor's own writes
    /// update both sets directly, so the common path never reaches here. And the first read sets
    /// `wal_scan_complete`, after which both answer from memory for the actor's remaining
    /// lifetime. Worst case is one read of one session's WAL per actor open.
    async fn ensure_wal_scan_loaded(&mut self) -> Result<(), String> {
        if self.wal_scan_complete {
            return Ok(());
        }
        let records = self.read_wal_records().await?;
        self.replace_wal_scan_sets(&records);
        Ok(())
    }

    /// The send key the WAL records for `run_id`, if any record carries that run id.
    async fn recorded_send_key(
        &mut self,
        run_id: &RunId,
    ) -> Result<Option<RecordedSendKey>, String> {
        self.ensure_wal_scan_loaded().await?;
        Ok(self.recorded_send_keys.get(run_id).cloned())
    }

    /// Keep the recorded send keys current after this actor made a record durable. Before the
    /// first WAL scan there is nothing to keep current: the scan will read the record itself.
    fn note_recorded_send_key(&mut self, run_id: &RunId, started: bool, digest: Option<&str>) {
        if !self.wal_scan_complete {
            return;
        }
        let entry = self.recorded_send_keys.entry(run_id.clone()).or_default();
        // A resume writes no record, so it must not lend the WAL a digest it never recorded.
        // Only a record this actor just wrote (the first `RunStarted`, or a `Queued`) carries it.
        let writes_new_record = !started || !entry.started;
        entry.started |= started;
        if writes_new_record && entry.request_identity_digest.is_none() {
            entry.request_identity_digest = digest.map(str::to_owned);
        }
    }

    async fn submission_was_retracted(
        &mut self,
        submission_id: &broca_protocol::SubmissionId,
    ) -> Result<bool, String> {
        if self.retracted_submissions.contains(submission_id) {
            return Ok(true);
        }
        self.ensure_wal_scan_loaded().await?;
        Ok(self.retracted_submissions.contains(submission_id))
    }

    async fn read_wal_records(&self) -> Result<Vec<WalRecord>, String> {
        Ok(self
            .read_wal_framed_records()
            .await?
            .into_iter()
            .map(|framed| framed.record)
            .collect())
    }

    /// Whether queued work will start a run once the session is idle. An ordered send whose list
    /// has no `idle_start` never starts one, so it does not count: it waits for a run that other
    /// work starts.
    fn has_pending(&self) -> bool {
        self.pending_fifo.iter().any(PendingSubmission::starts_runs)
    }

    /// Cancel + join the per-subscriber fan-out tasks (and the projector) BEFORE the writer
    /// shuts down + the lease drops — so no subscriber route or projector outlives the
    /// actor (the lease-vs-subscriber window the close-teardown order closes). The
    /// subscriber tasks hold only the ring receiver + a read-only WAL store handle, NEVER
    /// the writer Arc, so this teardown leaves the actor the sole writer owner for the
    /// `shutdown_writer` consume that follows.
    async fn teardown_subscribers(&mut self) {
        self.fanout.teardown().await;
    }
}

/// The number `transcript` shows for the part `part` names, by its message id (`m<ordinal>`)
/// and part key (`m<ordinal>#<index>`) as `session.read` spells them. `None` when no message
/// or part answers to the identity, or the part shows no number.
fn shown_number(
    transcript: &[broca_protocol::Message],
    part: &cortexkit_role_llm_runner::delivery_order::PartRef,
) -> Option<u64> {
    let ordinal: u64 = part.mid.strip_prefix('m')?.parse().ok()?;
    let (key_message, index) = part.part.split_once('#')?;
    if key_message != part.mid {
        return None;
    }
    let index: usize = index.parse().ok()?;
    let message = transcript
        .iter()
        .find(|message| message.ordinal == Some(ordinal))?;
    message.part_tags()?.parts.get(index).copied().flatten()
}

#[cfg(test)]
mod shown_number_tests {
    use super::shown_number;
    use cortexkit_role_llm_runner::delivery_order::PartRef;

    /// A `part_tag` slot renders the number the transcript shows for the part its `{mid, part}`
    /// names; a part that shows none, or that no message has, is gone.
    #[test]
    fn a_part_tag_names_the_number_its_part_shows() {
        let mut shown = broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text { text: "hi".into() }],
        );
        let mut quiet = shown.clone();
        let mut high = 41;
        broca_protocol::tags::number_message(
            &mut shown,
            broca_protocol::tags::ComputedRules {
                render: true,
                clean: true,
            },
            &mut high,
        )
        .unwrap();
        broca_protocol::tags::number_message(
            &mut quiet,
            broca_protocol::tags::ComputedRules {
                render: false,
                clean: false,
            },
            &mut high,
        )
        .unwrap();
        let mut transcript = vec![shown, quiet];
        broca_protocol::content::stamp_ordinals(&mut transcript);
        assert_eq!(
            shown_number(&transcript, &PartRef::new("m0", "m0#0")),
            Some(42)
        );
        assert_eq!(shown_number(&transcript, &PartRef::new("m1", "m1#0")), None);
        assert_eq!(shown_number(&transcript, &PartRef::new("m7", "m7#0")), None);
        assert_eq!(shown_number(&transcript, &PartRef::new("m0", "m1#0")), None);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use broca_core::{
        run_loop_on_writer_admitted_with_prior_snapshot, AllowAll, CapBaseline, CapInstallRequest,
        CapOperation, CapStore, CapWatermark, ChargeBasis, ExportIndex, FileLeaseStore,
        FileWalStore, LeaseStore, OutputEvent, Provider, RunError, RunIndex, RunSummary,
        SpendDeltaRequest, StartMode, StubToolPlane, WalHandle, WalStore,
    };
    use broca_protocol::caller::{ControlUnit, RunFinishReason};
    use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, ToolCall};
    use broca_protocol::provider::{NormalizedEvent, ProviderControl};
    use broca_protocol::request::{ExecutionMode, Tool};
    use broca_protocol::{BindIdentity, MessageId, RunId, StepId, ToolCallId, Usage};
    use broca_store::CapsStore;
    use tokio::sync::{mpsc, Notify, Semaphore};

    use super::*;
    use crate::executor::{RunExecutor, RunJob, RunKind};
    use crate::registry::{LineageOpener, LineageRegistry, RegistryHandle};

    /// Drives the `RunFinishGuard`'s test seam (see `terminal_send_suppressed`). When true,
    /// the guard drops its terminal — simulating a lost terminal so the backstop must fire.
    pub(super) static SUPPRESSED_TERMINAL_RUN: std::sync::Mutex<Option<String>> =
        std::sync::Mutex::new(None);

    /// A fake whose run ends NORMALLY (no panic) but, with the seam armed, whose guard
    /// drops the terminal — so the actor stays Active until the idle-tick backstop fires.
    struct EndsButTerminalLost;

    #[async_trait]
    impl RunExecutor for EndsButTerminalLost {
        async fn execute(
            &self,
            job: RunJob,
            _out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            // Record a RunStarted so the session writer/WAL path is exercised, then end
            // normally. The guard's terminal is suppressed by the seam, so this clean
            // return delivers NOTHING to the actor — only the backstop can recover it.
            let _ = job.writer.record(broca_wal::WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: None,
                run_id: job.req.run_id.clone(),
                session: job.req.session.clone(),
                config: Box::new(min_config()),
                input: job.req.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            });
            let _ = job.writer.sync().await;
            // Fire admission (RunStarted is durable) so the actor unblocks + replies Active.
            if let Some(ack) = job.on_admitted {
                let _ = ack.send(Ok(job.req.run_id.clone()));
            }
            Ok(RunSummary {
                reason: RunFinishReason::Completed,
                paused: None,
                steps: 1,
                usage: Usage::default(),
                retries_used: 0,
                final_step_finish_reason: None,
            })
        }
    }

    #[test]
    fn joining_steer_directives_are_frozen_and_omission_inherits() {
        let mut frozen = min_config();
        frozen.turn_directives_v1 = Some(vec![
            cortexkit_role_llm_runner::directives::DirectiveName::Wake,
        ]);
        let key = BindIdentity::new("/tmp/directives", "runner", "steer");
        let mut request = text_req(&key, "directives").to_submission();
        request.model = frozen.model.clone();
        request.stop_when = frozen.stop_when.clone();
        assert_eq!(steer_settings_conflict(&request, &frozen, false), None);
        request.directives = frozen.turn_directives_v1.clone();
        assert_eq!(steer_settings_conflict(&request, &frozen, false), None);
        request.directives = Some(vec![]);
        assert_eq!(
            steer_settings_conflict(&request, &frozen, false),
            Some("directives")
        );
        frozen.turn_directives_v1 = None;
        assert_eq!(steer_settings_conflict(&request, &frozen, false), None);
    }

    pub(super) fn min_config() -> broca_wal::RunConfig {
        broca_wal::RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: broca_protocol::request::ModelHandle::new("fake", "fake"),
            tools: vec![].into(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        }
    }

    /// An actor-test opener with a real lease and a selectable WAL backend.
    pub(super) struct SeamOpener<W: WalStore = FileWalStore> {
        pub(super) lease_store: Arc<FileLeaseStore>,
        pub(super) wal_store: Arc<W>,
        pub(super) idle_ttl: Duration,
        pub(super) executor: Arc<dyn RunExecutor>,
    }

    #[async_trait]
    // DELIBERATELY NOT THE PRODUCTION OPEN PATH, and the shortcut is the point.
    //
    // `DefaultOpener::open` goes through `WalHandle::open_and_spawn_observed`, which recovers and
    // FSYNC-PROVES the WAL off the async runtime before any slot is published, and moves the lease
    // through that blocking step so cancellation cannot release it mid-recovery. These doubles call
    // `spawn_observed` directly: no proof, and a failure panics rather than returning `OpenError`.
    //
    // That is correct division of labour rather than a gap. The open path's own properties are
    // asserted in opener.rs against failing and counting WAL stores --
    // `recovery_fsync_failure_fails_open_without_publishing_an_open_slot` and
    // `async_open_proves_once_off_runtime_and_reproves_each_tenure`. What the tests BELOW exercise
    // is actor behaviour, for which opening is setup.
    //
    // So do not "fix" the duplication by routing these through the real opener: it would couple
    // every actor test to fsync proof and re-assert, slowly, what opener.rs already pins.
    impl<W: WalStore + 'static> LineageOpener for SeamOpener<W> {
        async fn open(
            &self,
            key: &LineageKey,
            gen: u64,
            registry: RegistryHandle,
        ) -> Result<ActorHandle, crate::registry::OpenError> {
            let lease = self
                .lease_store
                .acquire(&broca_core::lease_key_for(key))
                .map_err(|e| crate::registry::OpenError::Open(e.to_string()))?;
            let (fanout, observer) =
                crate::fanout::Fanout::new(self.wal_store.clone(), key.clone());
            let wal = self
                .wal_store
                .open(key)
                .map_err(|e| crate::registry::OpenError::Open(e.to_string()))?;
            let writer = Arc::new(
                WalHandle::spawn_observed(wal, lease.epoch(), Some(observer))
                    .expect("spawn writer"),
            );
            Ok(LineageActor::spawn(
                key.clone(),
                gen,
                lease,
                writer,
                self.executor.clone(),
                self.wal_store.clone(),
                registry,
                self.idle_ttl,
                fanout,
                None,
                None,
            ))
        }
    }

    pub(super) fn text_req(key: &BindIdentity, run: &str) -> RunRequest {
        use broca_protocol::content::{ContentBlock, Message, Role};
        RunRequest {
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
            run_id: RunId(run.into()),
            session: key.clone(),
            model: broca_protocol::request::ModelHandle::new("fake", "fake"),
            input: vec![Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                tags: Default::default(),
                role: Role::User,
                content: vec![ContentBlock::Text { text: "hi".into() }],
            }],
            tools: vec![],
            policy_allowed: Default::default(),
            tool_choice: Default::default(),
            generation: Default::default(),
            cache: None,
            stop_when: vec![],
            submission_id: None,
            budget: None,
            context_limit: None,
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen_cache: false,
            structured_output: None,
        }
    }

    fn install_test_cap(
        store: &CapsStore,
        key: &BindIdentity,
        source_instance_id: &str,
        spend_nanos: i64,
        hard_cap_nanos: i64,
    ) {
        store
            .install_cap(&CapInstallRequest {
                cap_id: "cap-test".into(),
                cap_version: 1,
                op: CapOperation::Install,
                scope_key: key.harness.clone(),
                project_root: Some(key.project_root.to_string_lossy().into_owned()),
                constraint_dim: "spend_dollars".into(),
                period: "calendar_month".into(),
                period_tz: "Europe/Madrid".into(),
                hard_cap_nanos,
                max_staleness_ms: 60_000,
                replaces_cap_id: None,
                baseline: Some(CapBaseline {
                    spend_nanos,
                    unresolved_count: 0,
                    watermark: CapWatermark {
                        source_instance_id: source_instance_id.into(),
                        source_epoch: 1,
                        consumed_export_seq: 0,
                    },
                    period_bucket: Some("2026-07".into()),
                    as_of_ms: i64::MAX,
                }),
            })
            .expect("test cap installs");
    }

    fn apply_test_spend(store: &CapsStore, source_instance_id: &str, spend_nanos: i64) {
        store
            .apply_spend_delta(&SpendDeltaRequest {
                cap_id: "cap-test".into(),
                cap_version: 1,
                from_watermark: CapWatermark {
                    source_instance_id: source_instance_id.into(),
                    source_epoch: 1,
                    consumed_export_seq: 0,
                },
                to_watermark: CapWatermark {
                    source_instance_id: source_instance_id.into(),
                    source_epoch: 1,
                    consumed_export_seq: 1,
                },
                priced_delta_nanos: spend_nanos,
                unresolved_delta: 0,
                period_bucket: "2026-07".into(),
            })
            .expect("test spend delta applies");
    }

    async fn wait_for_starts(executor: &GateFirstRun, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.starts.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("expected run starts");
    }

    async fn wait_for_active_step(registry: &LineageRegistry, key: &BindIdentity, expected: u64) {
        broca_test_support::wait_until("active step becomes visible", || async {
            match registry.route_status(key).await.expect("status resolves") {
                LineageStatus::Active { step, .. } if step == expected => Ok(()),
                LineageStatus::Active { step, .. } if step < expected => {
                    Err(format!("step={step}, expected={expected}"))
                }
                other => panic!("expected active step {expected}, got {other:?}"),
            }
        })
        .await;
    }

    async fn wait_for_idle(registry: &LineageRegistry, key: &BindIdentity) {
        broca_test_support::wait_until("run reaches idle", || async {
            let status = registry.route_status(key).await.expect("status resolves");
            if status == LineageStatus::Idle {
                Ok(())
            } else {
                Err(format!("status={status:?}"))
            }
        })
        .await;
    }

    struct RetentionDone;

    #[async_trait]
    impl RunExecutor for RetentionDone {
        async fn execute(
            &self,
            job: RunJob,
            _out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            let mut config = min_config();
            config.tools_were_omitted = job.control.tools_were_omitted;
            job.writer.record(WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: None,
                run_id: job.req.run_id.clone(),
                session: job.req.session.clone(),
                config: Box::new(config),
                input: job.req.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: job.req.submission_id.clone(),
            });
            job.writer.sync().await?;
            if let Some(ack) = job.on_admitted {
                let _ = ack.send(Ok(job.req.run_id.clone()));
            }
            job.writer.record(WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            });
            job.writer.sync().await?;
            Ok(RunSummary {
                reason: RunFinishReason::Completed,
                paused: None,
                steps: 1,
                usage: Usage::default(),
                retries_used: 0,
                final_step_finish_reason: Some(FinishReason::Stop),
            })
        }
    }

    /// Completes a run immediately after admission (no panic) — used to open a session before
    /// driving the actor-task panic via `LineageCmd::PanicInActor`.
    struct QuickDone {
        close_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl RunExecutor for QuickDone {
        async fn execute(
            &self,
            job: RunJob,
            _out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            let _ = job.writer.record(broca_wal::WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: None,
                run_id: job.req.run_id.clone(),
                session: job.req.session.clone(),
                config: Box::new(min_config()),
                input: job.req.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: job.req.submission_id.clone(),
            });
            let _ = job.writer.sync().await;
            if let Some(ack) = job.on_admitted {
                let _ = ack.send(Ok(job.req.run_id.clone()));
            }
            Ok(RunSummary {
                reason: RunFinishReason::Completed,
                paused: None,
                steps: 1,
                usage: Usage::default(),
                retries_used: 0,
                final_step_finish_reason: None,
            })
        }

        async fn on_session_close(&self, _session: &BindIdentity) {
            self.close_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct StepProgressGate {
        release_first_step: Notify,
        first_step_sent: Notify,
        finish: Notify,
    }

    impl StepProgressGate {
        fn new() -> Self {
            Self {
                release_first_step: Notify::new(),
                first_step_sent: Notify::new(),
                finish: Notify::new(),
            }
        }
    }

    #[async_trait]
    impl RunExecutor for StepProgressGate {
        async fn execute(
            &self,
            job: RunJob,
            out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            job.writer.record(WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: None,
                run_id: job.req.run_id.clone(),
                session: job.req.session.clone(),
                config: Box::new(min_config()),
                input: job.req.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: job.req.submission_id.clone(),
            });
            job.writer.sync().await?;
            if let Some(ack) = job.on_admitted {
                let _ = ack.send(Ok(job.req.run_id.clone()));
            }

            self.release_first_step.notified().await;
            out.send(OutputEvent::Control(ControlUnit::StepStarted {
                step_id: StepId(1),
            }))
            .await
            .map_err(|_| RunError::Transient("status output drain closed".into()))?;
            self.first_step_sent.notify_one();
            self.finish.notified().await;

            job.writer.record(WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            });
            job.writer.sync().await?;
            Ok(RunSummary {
                reason: RunFinishReason::Completed,
                paused: None,
                steps: 1,
                usage: Usage::default(),
                retries_used: 0,
                final_step_finish_reason: Some(FinishReason::Stop),
            })
        }
    }

    struct BlockingTwoStepProvider {
        first_step_entered: Notify,
        release_first_step: Notify,
        second_step_entered: Notify,
        second_step_pending: Notify,
        allow_second_step: Notify,
        release_second_step: Notify,
    }

    impl BlockingTwoStepProvider {
        fn new() -> Self {
            Self {
                first_step_entered: Notify::new(),
                release_first_step: Notify::new(),
                second_step_entered: Notify::new(),
                second_step_pending: Notify::new(),
                allow_second_step: Notify::new(),
                release_second_step: Notify::new(),
            }
        }

        async fn scripted_step(events: Vec<NormalizedEvent>) -> mpsc::Receiver<NormalizedEvent> {
            let (tx, rx) = mpsc::channel(events.len().max(1));
            for event in events {
                tx.send(event).await.expect("script receiver stays open");
            }
            rx
        }
    }

    #[async_trait]
    impl Provider for BlockingTwoStepProvider {
        async fn step(
            &self,
            step_id: StepId,
            _opts: &broca_protocol::CallOptions,
        ) -> mpsc::Receiver<NormalizedEvent> {
            match step_id.0 {
                1 => {
                    self.first_step_entered.notify_one();
                    self.release_first_step.notified().await;
                    Self::scripted_step(vec![
                        NormalizedEvent::Control(ProviderControl::AssistantMessage {
                            message: AssistantMessage {
                                message_id: MessageId("step-1".into()),
                                content: vec![ContentBlock::ToolCall(ToolCall {
                                    tool_call_id: ToolCallId("call-1".into()),
                                    tool_name: "lookup".into(),
                                    input: serde_json::json!({}),
                                    thought_signature: None,
                                })],
                            },
                        }),
                        NormalizedEvent::Control(ProviderControl::Finished {
                            usage: Usage::default(),
                            finish_reason: FinishReason::ToolCalls,
                            retries_used: 0,
                        }),
                    ])
                    .await
                }
                2 => {
                    self.second_step_pending.notify_one();
                    self.allow_second_step.notified().await;
                    self.second_step_entered.notify_one();
                    self.release_second_step.notified().await;
                    Self::scripted_step(vec![
                        NormalizedEvent::Control(ProviderControl::AssistantMessage {
                            message: AssistantMessage {
                                message_id: MessageId("step-2".into()),
                                content: vec![ContentBlock::Text {
                                    text: "done".into(),
                                }],
                            },
                        }),
                        NormalizedEvent::Control(ProviderControl::Finished {
                            usage: Usage::default(),
                            finish_reason: FinishReason::Stop,
                            retries_used: 0,
                        }),
                    ])
                    .await
                }
                step => panic!("unexpected scripted provider step {step}"),
            }
        }
    }

    struct TwoStepCoreExecutor {
        wal_store: Arc<super::wal_test_support::MemoryWalStore>,
        provider: Arc<BlockingTwoStepProvider>,
        plane: StubToolPlane,
    }

    #[async_trait]
    impl RunExecutor for TwoStepCoreExecutor {
        async fn execute(
            &self,
            job: RunJob,
            out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            let RunJob {
                conversation_key: _,
                req,
                auth: _,
                writer,
                kind,
                skip_materialization: _,
                control,
                prior_snapshot,
                on_admitted,
            } = job;
            let mode = match kind {
                RunKind::FreshOrResume => StartMode::FreshOrResume,
                RunKind::AppendEpisode => StartMode::AppendEpisode,
                RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            };
            run_loop_on_writer_admitted_with_prior_snapshot(
                req,
                writer.as_ref(),
                mode,
                self.provider.as_ref(),
                &self.plane,
                &AllowAll,
                self.wal_store.as_ref(),
                None,
                control,
                &out,
                on_admitted,
                Some(&prior_snapshot),
            )
            .await
        }
    }

    struct ActivePanicExecutor {
        starts: AtomicUsize,
        active: Arc<AtomicUsize>,
        max_active: AtomicUsize,
        close_count: AtomicUsize,
    }

    struct ActiveExecution {
        active: Arc<AtomicUsize>,
    }

    impl Drop for ActiveExecution {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl RunExecutor for ActivePanicExecutor {
        async fn execute(
            &self,
            mut job: RunJob,
            _out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            let ordinal = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            let _execution = ActiveExecution {
                active: Arc::clone(&self.active),
            };

            if ordinal == 1 {
                job.writer.record(WalRecord::RunStarted {
                    tags: None,
                    session_class: None,
                    retention: None,
                    hooks: None,
                    title: None,
                    on_restart: None,
                    keep_warm: None,
                    marker_order_version: None,
                    manifest: None,
                    scope: None,
                    send_id: None,
                    request_identity_digest: None,
                    project_id: None,
                    principal: None,
                    ts_ms: Some(100),
                    run_id: job.req.run_id.clone(),
                    session: job.req.session.clone(),
                    config: Box::new(min_config()),
                    input: job.req.input.clone().into(),
                    origin: broca_wal::RunOrigin::Fresh,
                    submission_id: job.req.submission_id.clone(),
                });
                job.writer.sync().await?;
            }
            if let Some(ack) = job.on_admitted.take() {
                let _ = ack.send(Ok(job.req.run_id.clone()));
            }

            // The use after this blocking point keeps the complete job in the async state, so this
            // fake exercises the production child's ownership of the session-writer Arc.
            std::future::pending::<()>().await;
            drop(job);
            unreachable!("the active panic test executor is stopped only by task abort")
        }

        async fn on_session_close(&self, _session: &BindIdentity) {
            self.close_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct CountingCapStore {
        inner: Arc<CapsStore>,
        matching_calls: AtomicUsize,
    }

    impl CapStore for CountingCapStore {
        fn install_cap(
            &self,
            request: &CapInstallRequest,
        ) -> Result<broca_core::CapMutation, broca_core::CapStoreError> {
            self.inner.install_cap(request)
        }

        fn apply_spend_delta(
            &self,
            request: &SpendDeltaRequest,
        ) -> Result<broca_core::CapMutation, broca_core::CapStoreError> {
            self.inner.apply_spend_delta(request)
        }

        fn matching_caps(
            &self,
            scope_key: &str,
            project_root: &std::path::Path,
        ) -> Vec<broca_core::CapRow> {
            self.matching_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.matching_caps(scope_key, project_root)
        }
    }

    struct BlockingExportIndex {
        entered: tokio::sync::Notify,
        completed: tokio::sync::Notify,
        completed_count: AtomicUsize,
        high_water: AtomicU64,
        release: (Mutex<bool>, Condvar),
    }

    impl BlockingExportIndex {
        fn new() -> Self {
            Self {
                entered: tokio::sync::Notify::new(),
                completed: tokio::sync::Notify::new(),
                completed_count: AtomicUsize::new(0),
                high_water: AtomicU64::new(0),
                release: (Mutex::new(false), Condvar::new()),
            }
        }

        fn release(&self) {
            *self.release.0.lock().expect("export release lock") = true;
            self.release.1.notify_all();
        }

        async fn wait_for_completions(&self, count: usize) {
            loop {
                let notified = self.completed.notified();
                if self.completed_count.load(Ordering::SeqCst) >= count {
                    return;
                }
                notified.await;
            }
        }
    }

    impl ExportIndex for BlockingExportIndex {
        fn missing_terminal_runs(
            &self,
        ) -> Result<Vec<broca_core::MissingExportRun>, broca_core::ExportError> {
            Ok(Vec::new())
        }

        fn emit_run(
            &self,
            _records: &[broca_wal::FramedRecord],
            _run_id: &RunId,
            _resolver: &dyn broca_core::ChargeBasisResolver,
        ) -> Result<(), broca_core::ExportError> {
            self.entered.notify_one();
            let mut released = self.release.0.lock().expect("export release lock");
            while !*released {
                released = self.release.1.wait(released).expect("export release wait");
            }
            self.high_water.fetch_add(1, Ordering::Release);
            self.completed_count.fetch_add(1, Ordering::SeqCst);
            self.completed.notify_one();
            Ok(())
        }

        fn read_page(
            &self,
            _since: Option<broca_core::ExportCursor>,
            _limit: u64,
        ) -> Result<broca_core::ExportPage, broca_core::ExportError> {
            Err(broca_core::ExportError::Storage(
                "blocking export has no read surface".into(),
            ))
        }

        fn high_water(&self) -> u64 {
            self.high_water.load(Ordering::Acquire)
        }

        fn source_epoch(&self) -> u64 {
            1
        }

        fn source_instance_id(&self) -> &str {
            "blocking-export"
        }
    }

    struct GateFirstRun {
        starts: AtomicUsize,
        release_first: tokio::sync::Notify,
    }

    #[async_trait]
    impl RunExecutor for GateFirstRun {
        async fn execute(
            &self,
            mut job: RunJob,
            _out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            let ordinal = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
            job.writer.record(WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: Some(100),
                run_id: job.req.run_id.clone(),
                session: job.req.session.clone(),
                config: Box::new(min_config()),
                input: job.req.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: job.req.submission_id.clone(),
            });
            job.writer.sync().await?;
            if let Some(ack) = job.on_admitted.take() {
                let _ = ack.send(Ok(job.req.run_id.clone()));
            }
            if ordinal == 1 {
                self.release_first.notified().await;
            }
            job.writer.record(WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: Some(200),
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            });
            job.writer.sync().await?;
            Ok(RunSummary {
                reason: RunFinishReason::Completed,
                paused: None,
                steps: 1,
                usage: Usage::default(),
                retries_used: 0,
                final_step_finish_reason: None,
            })
        }
    }

    struct RecoveryGate {
        started: Mutex<Vec<String>>,
        start_signal: Semaphore,
        finish_signal: Semaphore,
        release_first: Notify,
    }

    impl RecoveryGate {
        fn new() -> Self {
            Self {
                started: Mutex::new(Vec::new()),
                start_signal: Semaphore::new(0),
                finish_signal: Semaphore::new(0),
                release_first: Notify::new(),
            }
        }

        fn started(&self) -> Vec<String> {
            self.started.lock().unwrap().clone()
        }

        async fn wait_for_start(&self) {
            self.start_signal
                .acquire()
                .await
                .expect("start semaphore remains open")
                .forget();
        }

        async fn wait_for_finish(&self) {
            self.finish_signal
                .acquire()
                .await
                .expect("finish semaphore remains open")
                .forget();
        }
    }

    #[async_trait]
    impl RunExecutor for RecoveryGate {
        async fn execute(
            &self,
            mut job: RunJob,
            _out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            let first = {
                let mut started = self.started.lock().unwrap();
                started.push(job.req.run_id.0.clone());
                started.len() == 1
            };
            job.writer.record(WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: Some(100),
                run_id: job.req.run_id.clone(),
                session: job.req.session.clone(),
                config: Box::new(min_config()),
                input: job.req.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: job.req.submission_id.clone(),
            });
            job.writer.sync().await?;
            if let Some(ack) = job.on_admitted.take() {
                let _ = ack.send(Ok(job.req.run_id.clone()));
            }
            self.start_signal.add_permits(1);
            if first {
                self.release_first.notified().await;
            }
            job.writer.record(WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: Some(200),
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            });
            job.writer.sync().await?;
            self.finish_signal.add_permits(1);
            Ok(RunSummary {
                reason: RunFinishReason::Completed,
                paused: None,
                steps: 1,
                usage: Usage::default(),
                retries_used: 0,
                final_step_finish_reason: None,
            })
        }
    }

    struct FailingExportIndex;

    impl ExportIndex for FailingExportIndex {
        fn missing_terminal_runs(
            &self,
        ) -> Result<Vec<broca_core::MissingExportRun>, broca_core::ExportError> {
            Ok(Vec::new())
        }

        fn emit_run(
            &self,
            _records: &[broca_wal::FramedRecord],
            _run_id: &RunId,
            _resolver: &dyn broca_core::ChargeBasisResolver,
        ) -> Result<(), broca_core::ExportError> {
            Err(broca_core::ExportError::Storage(
                "injected export failure".into(),
            ))
        }

        fn read_page(
            &self,
            _since: Option<broca_core::ExportCursor>,
            _limit: u64,
        ) -> Result<broca_core::ExportPage, broca_core::ExportError> {
            Err(broca_core::ExportError::Storage(
                "injected export failure".into(),
            ))
        }

        fn high_water(&self) -> u64 {
            0
        }

        fn source_epoch(&self) -> u64 {
            1
        }

        fn source_instance_id(&self) -> &str {
            "failing-export"
        }
    }

    struct FlakyReadWalStore {
        inner: FileWalStore,
        fail_reads: AtomicBool,
        fail_next_reads: AtomicUsize,
        read_attempts: AtomicUsize,
        read_attempt_signal: Semaphore,
    }

    impl FlakyReadWalStore {
        fn new(root: impl Into<std::path::PathBuf>) -> Self {
            Self {
                inner: FileWalStore::new(root),
                fail_reads: AtomicBool::new(false),
                fail_next_reads: AtomicUsize::new(0),
                read_attempts: AtomicUsize::new(0),
                read_attempt_signal: Semaphore::new(0),
            }
        }

        fn set_fail_reads(&self, fail: bool) {
            self.fail_reads.store(fail, Ordering::SeqCst);
        }

        fn fail_next_read(&self) {
            self.fail_next_reads.store(1, Ordering::SeqCst);
        }

        fn read_attempts(&self) -> usize {
            self.read_attempts.load(Ordering::SeqCst)
        }

        async fn wait_for_read_attempt(&self) {
            self.read_attempt_signal
                .acquire()
                .await
                .expect("read-attempt semaphore remains open")
                .forget();
        }

        fn discard_read_attempt_signals(&self) {
            while let Ok(permit) = self.read_attempt_signal.try_acquire() {
                permit.forget();
            }
        }

        fn before_read(&self) -> Result<(), broca_wal::WalError> {
            self.read_attempts.fetch_add(1, Ordering::SeqCst);
            self.read_attempt_signal.add_permits(1);
            let mut remaining = self.fail_next_reads.load(Ordering::SeqCst);
            let fail_once = loop {
                let Some(next) = remaining.checked_sub(1) else {
                    break false;
                };
                match self.fail_next_reads.compare_exchange_weak(
                    remaining,
                    next,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break true,
                    Err(actual) => remaining = actual,
                }
            };
            if self.fail_reads.load(Ordering::SeqCst) || fail_once {
                return Err(broca_wal::WalError::Io(std::io::Error::other(
                    "injected WAL read failure",
                )));
            }
            Ok(())
        }

        fn durable_records(&self, session: &BindIdentity) -> Vec<broca_wal::FramedRecord> {
            self.inner
                .read_records(session)
                .expect("read durable WAL through non-failing inner store")
        }
    }

    impl WalStore for FlakyReadWalStore {
        fn wal_present(&self, session: &BindIdentity) -> Option<bool> {
            // Preserve authoritative absence so status can consult an expiry tombstone
            // instead of trying to read a transcript that retention already deleted.
            self.inner.wal_present(session)
        }

        fn open(
            &self,
            session: &BindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            self.inner.open(session)
        }

        fn read_records(
            &self,
            session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            self.before_read()?;
            self.inner.read_records(session)
        }

        fn read_records_range(
            &self,
            session: &BindIdentity,
            after: u64,
            through: Option<u64>,
            anchor: Option<&broca_wal::WalByteAnchor>,
        ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
            self.before_read()?;
            self.inner
                .read_records_range(session, after, through, anchor)
        }
    }

    fn seed_finished_episode_with_queue(
        wal_store: &FlakyReadWalStore,
        key: &BindIdentity,
        queued_run_id: &str,
    ) {
        let mut wal = wal_store.open(key).expect("open WAL to seed restart state");
        wal.append(
            1,
            &WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: Some(100),
                run_id: RunId("a".into()),
                session: key.clone(),
                config: Box::new(min_config()),
                input: text_req(key, "a").input.into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            false,
        )
        .expect("seed completed episode start");
        wal.append(
            1,
            &WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: Some(200),
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
            false,
        )
        .expect("seed completed episode terminal");
        wal.append(
            1,
            &WalRecord::Queued {
                retention: None,
                ts_ms: None,
                hooks: None,
                title: None,
                keep_warm: None,
                conversation_key: None,
                send_id: None,
                delivery: None,
                submission_id: broca_protocol::SubmissionId(format!("sub-{queued_run_id}")),
                request: Box::new(text_req(key, queued_run_id).to_submission()),
                request_identity_digest: None,
                principal: None,
            },
            true,
        )
        .expect("seed durable queued work");
    }

    fn started_run_ids(records: &[broca_wal::FramedRecord]) -> Vec<String> {
        records
            .iter()
            .filter_map(|record| match &record.record {
                WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
                _ => None,
            })
            .collect()
    }

    async fn exhaust_startup_recovery_budget(
        registry: &LineageRegistry,
        key: &BindIdentity,
        wal_store: &FlakyReadWalStore,
    ) {
        registry.actor_for(key).await.expect("open session actor");
        wal_store.wait_for_read_attempt().await;
        tokio::time::advance(STARTUP_RECOVERY_RETRY_BUDGET).await;
    }

    struct QuickSeamOpener {
        lease_store: Arc<FileLeaseStore>,
        wal_store: Arc<FileWalStore>,
        idle_ttl: Duration,
        executor: Arc<QuickDone>,
    }

    #[async_trait]
    impl LineageOpener for QuickSeamOpener {
        async fn open(
            &self,
            key: &LineageKey,
            gen: u64,
            registry: RegistryHandle,
        ) -> Result<ActorHandle, crate::registry::OpenError> {
            let lease = self
                .lease_store
                .acquire(&broca_core::lease_key_for(key))
                .map_err(|e| crate::registry::OpenError::Open(e.to_string()))?;
            let (fanout, observer) =
                crate::fanout::Fanout::new(self.wal_store.clone(), key.clone());
            let wal = self
                .wal_store
                .open(key)
                .map_err(|e| crate::registry::OpenError::Open(e.to_string()))?;
            let writer = Arc::new(
                WalHandle::spawn_observed(wal, lease.epoch(), Some(observer))
                    .expect("spawn writer"),
            );
            Ok(LineageActor::spawn(
                key.clone(),
                gen,
                lease,
                writer,
                self.executor.clone(),
                self.wal_store.clone(),
                registry,
                self.idle_ttl,
                fanout,
                None,
                None,
            ))
        }
    }

    #[tokio::test]
    async fn active_status_is_zero_before_first_step_then_tracks_step_one() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-status-first-step-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let executor = Arc::new(StepProgressGate::new());
        let opener = Arc::new(SeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store: Arc::new(FileWalStore::new(dir.join("wal"))),
            idle_ttl: Duration::from_secs(60),
            executor: executor.clone(),
        });
        let registry = LineageRegistry::new(opener);

        registry
            .route_send(&key, text_req(&key, "r1"), RunKind::FreshOrResume)
            .await
            .expect("send is admitted");
        assert_eq!(
            registry.route_status(&key).await.expect("status resolves"),
            LineageStatus::Active {
                run_id: RunId("r1".into()),
                step: 0,
            },
            "an admitted run has no current step before StepStarted"
        );

        executor.release_first_step.notify_one();
        executor.first_step_sent.notified().await;
        wait_for_active_step(&registry, &key, 1).await;

        executor.finish.notify_one();
        wait_for_idle(&registry, &key).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn active_status_reports_second_step_while_provider_is_blocked() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-status-second-step-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(super::wal_test_support::MemoryWalStore::default());
        let provider = Arc::new(BlockingTwoStepProvider::new());
        let executor = Arc::new(TwoStepCoreExecutor {
            wal_store: wal_store.clone(),
            provider: provider.clone(),
            plane: StubToolPlane::new().with_canned_text("lookup", "found"),
        });
        let opener = Arc::new(SeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store,
            idle_ttl: Duration::from_secs(60),
            executor,
        });
        let registry = LineageRegistry::new(opener);
        let mut request = text_req(&key, "r1");
        request.tools.push(Tool {
            name: "lookup".into(),
            description: "lookup test data".into(),
            input_schema: serde_json::json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Sequential,
            reply: None,
        });
        request.policy_allowed.insert("lookup".into());

        registry
            .route_send(&key, request, RunKind::FreshOrResume)
            .await
            .expect("send is admitted");
        broca_test_support::wait_for(
            "provider enters step one",
            provider.first_step_entered.notified(),
            || format!("slots={:?}", registry.slot_counts()),
        )
        .await;
        wait_for_active_step(&registry, &key, 1).await;

        provider.release_first_step.notify_one();
        broca_test_support::wait_for(
            "second step is pending",
            provider.second_step_pending.notified(),
            || format!("slots={:?}", registry.slot_counts()),
        )
        .await;
        let entered = broca_test_support::wait_for(
            "provider enters step two",
            provider.second_step_entered.notified(),
            || format!("slots={:?}", registry.slot_counts()),
        );
        tokio::pin!(entered);
        assert!(futures::poll!(&mut entered).is_pending());
        // Force a scheduler-sized gap without CPU load or disk delays. Entry is an
        // event, so the wait must survive the former one-second latency assumption.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
        provider.allow_second_step.notify_one();
        entered.await;
        wait_for_active_step(&registry, &key, 2).await;

        provider.release_second_step.notify_one();
        wait_for_idle(&registry, &key).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    struct HeldRetentionCompletion {
        completed: Notify,
        release: Notify,
    }

    #[async_trait]
    impl RunExecutor for HeldRetentionCompletion {
        async fn execute(
            &self,
            job: RunJob,
            out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            let result = RetentionDone.execute(job, out).await;
            self.completed.notify_one();
            self.release.notified().await;
            result
        }
    }

    #[tokio::test]
    async fn admitted_identity_retention_is_independent_of_request_payload_size() {
        const EPISODES: usize = 4;
        const PAYLOAD_BYTES: usize = 128 * 1024;

        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-admission-retention-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let executor = Arc::new(HeldRetentionCompletion {
            completed: Notify::new(),
            release: Notify::new(),
        });
        let opener = Arc::new(SeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store: Arc::new(super::wal_test_support::MemoryWalStore::default()),
            idle_ttl: Duration::from_secs(60),
            executor: executor.clone(),
        });
        let registry = LineageRegistry::new(opener);

        for episode in 0..EPISODES {
            let run_id = RunId(format!("retained-{episode}"));
            let mut request = text_req(&key, &run_id.0).to_submission();
            request.input[0].content[0] = ContentBlock::Text {
                text: "x".repeat(PAYLOAD_BYTES),
            };
            let outcome = registry
                .route_submission(&key, run_id, request, RunKind::FreshOrResume, true)
                .await
                .expect("route large request");
            assert!(matches!(outcome, SendOutcome::Active { .. }));
            broca_test_support::wait_for(
                "executor records terminal",
                executor.completed.notified(),
                || format!("slots={:?}", registry.slot_counts()),
            )
            .await;
            let idle = wait_for_idle(&registry, &key);
            tokio::pin!(idle);
            assert!(futures::poll!(&mut idle).is_pending());
            // Completion delivery can lag the terminal. Retained-identity assertions
            // must not impose a one-second scheduler deadline on that delivery.
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(2)).await;
            tokio::time::resume();
            executor.release.notify_one();
            idle.await;
        }

        let handle = registry.actor_for(&key).await.expect("actor remains open");
        let (reply, rx) = oneshot::channel();
        assert!(
            handle
                .send(LineageCmd::RetainedAdmissionIdentityState { reply })
                .await
                .is_ok(),
            "inspect retained admission identities"
        );
        let (retained_runs, retained_bytes) =
            broca_test_support::wait_for("actor reports retained identity state", rx, || {
                format!("slots={:?}", registry.slot_counts())
            })
            .await
            .expect("actor reports retained identity state");
        assert_eq!(
            retained_runs, EPISODES,
            "every admission retains an identity"
        );
        assert!(
            retained_bytes < PAYLOAD_BYTES,
            "compact identity state retained {retained_bytes} bytes for {EPISODES} requests"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn compact_identity_preserves_field_specific_omitted_surface_reuse() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-compact-identity-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let opener = Arc::new(SeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store: Arc::new(FileWalStore::new(dir.join("wal"))),
            idle_ttl: Duration::from_secs(60),
            executor: Arc::new(RetentionDone),
        });
        let registry = LineageRegistry::new(opener);
        let run_id = RunId("omitted-surface".into());
        let mut original = text_req(&key, &run_id.0).to_submission();
        original.tools = None;

        let outcome = registry
            .route_submission(
                &key,
                run_id.clone(),
                original.clone(),
                RunKind::FreshOrResume,
                true,
            )
            .await
            .expect("admit omitted tool surface");
        assert!(matches!(outcome, SendOutcome::Active { .. }));
        wait_for_idle(&registry, &key).await;

        let mut changed_input = original.clone();
        changed_input.input[0].content[0] = ContentBlock::Text {
            text: "different input".into(),
        };
        assert_eq!(
            registry
                .route_submission(
                    &key,
                    run_id.clone(),
                    changed_input,
                    RunKind::FreshOrResume,
                    true,
                )
                .await
                .expect("compare changed input"),
            SendOutcome::SendIdReuse { differing: "input" }
        );

        let mut explicit_surface = original;
        explicit_surface.tools = Some(Vec::new());
        assert_eq!(
            registry
                .route_submission(&key, run_id, explicit_surface, RunKind::FreshOrResume, true,)
                .await
                .expect("compare explicit tool surface"),
            SendOutcome::SendIdReuse { differing: "tools" }
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// After an idle actor-task panic completes emergency teardown, a subsequent send must open a
    /// fresh working actor (not fail on a dead command handle) and leave slot counts sane.
    #[tokio::test]
    async fn actor_panic_finalizes_slot_and_next_send_reopens() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-actor-panic-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");

        let close_count = Arc::new(AtomicUsize::new(0));
        let opener = Arc::new(QuickSeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store: Arc::new(FileWalStore::new(dir.join("wal"))),
            idle_ttl: Duration::from_secs(60),
            executor: Arc::new(QuickDone {
                close_count: close_count.clone(),
            }),
        });
        let registry = LineageRegistry::new(opener);

        let handle = registry.actor_for(&key).await.expect("open session");
        assert_eq!(registry.slot_counts().open, 1);

        assert!(
            handle.send(LineageCmd::PanicInActor).await.is_ok(),
            "deliver panic command"
        );

        for _ in 0..80 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let counts = registry.slot_counts();
            if counts.open == 0 && counts.opening == 0 {
                break;
            }
        }
        assert_eq!(
            registry.slot_counts().open,
            0,
            "Open slot must be cleared after actor panic"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while close_count.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actor panic must trigger on_session_close cleanup");
        assert_eq!(close_count.load(Ordering::SeqCst), 1);

        let second = tokio::time::timeout(
            Duration::from_secs(5),
            registry.route_send(&key, text_req(&key, "r2"), RunKind::FreshOrResume),
        )
        .await
        .expect("second send must not hang on a dead handle")
        .expect("second send must open a fresh actor");

        assert!(
            matches!(second, SendOutcome::Active { .. }),
            "fresh actor must run the send: {:?}",
            second
        );
        assert_eq!(registry.slot_counts().opening, 0);
        assert!(registry.slot_counts().open <= 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An actor panic during an active run must stop that child before the registry slot becomes
    /// reopenable. Otherwise startup recovery can execute the same durable episode concurrently.
    #[tokio::test]
    async fn active_actor_panic_stops_old_run_before_reopen() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-active-actor-panic-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");

        let active = Arc::new(AtomicUsize::new(0));
        let executor = Arc::new(ActivePanicExecutor {
            starts: AtomicUsize::new(0),
            active: Arc::clone(&active),
            max_active: AtomicUsize::new(0),
            close_count: AtomicUsize::new(0),
        });
        let opener = Arc::new(SeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store: Arc::new(FileWalStore::new(dir.join("wal"))),
            idle_ttl: Duration::from_secs(60),
            executor: executor.clone(),
        });
        let registry = LineageRegistry::new(opener);
        let first_handle = registry.actor_for(&key).await.expect("open first actor");

        assert!(matches!(
            registry
                .route_send(&key, text_req(&key, "r1"), RunKind::FreshOrResume)
                .await
                .expect("start first run"),
            SendOutcome::Active { .. }
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.starts.load(Ordering::SeqCst) < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first execution starts");
        assert_eq!(active.load(Ordering::SeqCst), 1);

        assert!(
            first_handle.send(LineageCmd::PanicInActor).await.is_ok(),
            "deliver panic to active actor"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let counts = registry.slot_counts();
                if counts.open == 0 && counts.opening == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("panic teardown finalizes first actor");

        assert_eq!(
            active.load(Ordering::SeqCst),
            0,
            "the old execution must be gone before its slot becomes reopenable"
        );
        assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
        assert_eq!(executor.close_count.load(Ordering::SeqCst), 1);

        let second_handle = registry.actor_for(&key).await.expect("reopen actor");
        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.starts.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovery starts the interrupted episode once");
        assert_eq!(active.load(Ordering::SeqCst), 1);
        assert_eq!(
            executor.max_active.load(Ordering::SeqCst),
            1,
            "the old and recovered executions must never overlap"
        );

        assert!(
            second_handle.send(LineageCmd::PanicInActor).await.is_ok(),
            "stop recovered execution"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let counts = registry.slot_counts();
                if active.load(Ordering::SeqCst) == 0 && counts.open == 0 && counts.opening == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovered actor finishes panic teardown");
        assert_eq!(executor.close_count.load(Ordering::SeqCst), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A full external-command lane cannot park the real terminal, while a genuinely lost
    /// terminal is still recovered by the idle-tick backstop.
    #[tokio::test]
    async fn idle_tick_backstop_recovers_a_lost_terminal() {
        let (command_tx, _command_rx) = mpsc::channel(1);
        command_tx
            .try_send(LineageCmd::IdleTick)
            .expect("fill the bounded external-command lane");
        assert!(command_tx.try_send(LineageCmd::IdleTick).is_err());
        let (terminal_tx, mut terminal_rx) = mpsc::unbounded_channel();
        RunFinishGuard::new(terminal_tx, RunId("real-terminal".into()), 7).finish(Ok(RunSummary {
            reason: RunFinishReason::Completed,
            paused: None,
            steps: 1,
            usage: Usage::default(),
            retries_used: 0,
            final_step_finish_reason: None,
        }));
        assert!(matches!(
            terminal_rx.try_recv(),
            Ok(LineageCmd::RunFinished {
                run_id,
                result,
                ..
            }) if run_id.0 == "real-terminal" && matches!(*result, Ok(RunSummary { reason: RunFinishReason::Completed, .. }))
        ));

        let dir = broca_test_support::scratch_root()
            .join(format!("broca-backstop-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");

        // Arm the true-loss seam after proving bounded command backpressure cannot park a
        // real terminal. Only this test sets it.
        *SUPPRESSED_TERMINAL_RUN.lock().unwrap() = Some("lost-terminal".into());

        let opener = Arc::new(SeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store: Arc::new(FileWalStore::new(dir.join("wal"))),
            idle_ttl: Duration::from_millis(80),
            executor: Arc::new(EndsButTerminalLost),
        });
        let registry = LineageRegistry::new(opener);

        // Start a run. The run ends normally but its terminal is suppressed → the actor
        // would stay Active forever WITHOUT the backstop.
        let outcome = registry
            .route_send(
                &key,
                text_req(&key, "lost-terminal"),
                RunKind::FreshOrResume,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, SendOutcome::Active { .. }));

        // The ONLY recovery is the idle-tick backstop: after the run task finishes and an
        // idle tick fires, is_finished() is true while still Active → synthesize terminal →
        // leave Active → idle-close (slot gone, lease released).
        let mut closed = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(40)).await;
            if registry.slot_state(&key).is_none() {
                closed = true;
                break;
            }
        }
        *SUPPRESSED_TERMINAL_RUN.lock().unwrap() = None;
        assert!(
            closed,
            "the actor must recover a LOST terminal via the idle-tick backstop and close \
             (else it wedges Active forever holding the lease)"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A run that makes its terminal durable the way core does -- sync `RunFinished`, then emit
    /// `ControlUnit::RunFinished` -- and then holds before returning, so the actor cannot process
    /// its completion until the test releases it. That is the gap in which `run.result` already
    /// reports the terminal from the WAL while the actor still holds the run as active.
    struct HoldsAfterDurableFinish {
        reason: RunFinishReason,
        /// When set, the run waits after its start until `steered` is raised, then takes the
        /// steers delivered so far into one model step the way the run loop does (its
        /// `StepStarted` appended under the inbox lock) before it writes its terminal.
        render_steer_first: bool,
        /// Raised by the test once its steer has been admitted into the running run.
        steered: Notify,
        /// Raised once the terminal is durable and the output forwarder has handled it.
        held: Notify,
        release: Notify,
    }

    #[async_trait]
    impl RunExecutor for HoldsAfterDurableFinish {
        async fn execute(
            &self,
            job: RunJob,
            out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            job.writer.record(broca_wal::WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: job.req.send_id.clone(),
                request_identity_digest: job.req.request_identity_digest.clone(),
                project_id: None,
                principal: None,
                ts_ms: None,
                run_id: job.req.run_id.clone(),
                session: job.req.session.clone(),
                config: Box::new(min_config()),
                input: job.req.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            });
            job.writer.sync().await.expect("sync RunStarted");
            if let Some(ack) = job.on_admitted {
                let _ = ack.send(Ok(job.req.run_id.clone()));
            }
            if self.render_steer_first {
                self.steered.notified().await;
                let inbox = job
                    .control
                    .steer
                    .as_ref()
                    .expect("the actor gives a steer inbox");
                let taken = inbox.take_at_step(|| {
                    job.writer.record(broca_wal::WalRecord::StepStarted {
                        step_id: broca_protocol::StepId(1),
                    });
                });
                assert!(!taken.is_empty(), "the steer reaches the step");
            }
            job.writer.record(broca_wal::WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: self.reason,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            });
            job.writer.sync().await.expect("sync RunFinished");
            out.send(OutputEvent::Control(ControlUnit::RunFinished {
                run_id: Some(job.req.run_id.clone()),
                reason: self.reason,
                usage: Some(Usage::default()),
                final_step_finish_reason: None,
                indeterminate_tool_calls: None,
                retries_used: 0,
            }))
            .await
            .expect("output forwarder alive");
            // The forwarder handles one event fully before it receives the next, so once it has
            // taken this follow-up event off the channel it has finished with the terminal.
            out.send(OutputEvent::Control(ControlUnit::DisplayReset))
                .await
                .expect("output forwarder alive");
            while out.capacity() < out.max_capacity() {
                tokio::task::yield_now().await;
            }
            self.held.notify_one();
            self.release.notified().await;
            Ok(RunSummary {
                reason: self.reason,
                paused: None,
                steps: 1,
                usage: Usage::default(),
                retries_used: 0,
                final_step_finish_reason: None,
            })
        }
    }

    /// Start a send keyed by a caller `send_id` on a run that holds after its durable terminal,
    /// and return once the actor is still active for it but its `RunFinished` is synced.
    async fn start_held_durable_finish(
        name: &str,
    ) -> (
        std::path::PathBuf,
        BindIdentity,
        LineageRegistry,
        Arc<HoldsAfterDurableFinish>,
        RunId,
    ) {
        let dir =
            broca_test_support::scratch_root().join(format!("broca-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        // A reason other than `Completed`, so the answer is shown to carry the recorded reason
        // rather than a default.
        let executor = Arc::new(HoldsAfterDurableFinish {
            reason: RunFinishReason::MaxSteps,
            render_steer_first: false,
            steered: Notify::new(),
            held: Notify::new(),
            release: Notify::new(),
        });
        let opener = Arc::new(SeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store: Arc::new(FileWalStore::new(dir.join("wal"))),
            idle_ttl: Duration::from_secs(30),
            executor: executor.clone(),
        });
        let registry = LineageRegistry::new(opener);
        let run_id = RunId(format!("run-{name}"));
        let first = registry
            .route_submission_with_materialization(
                &key,
                run_id.clone(),
                text_req(&key, &run_id.0).to_submission(),
                RunKind::FreshOrResume,
                SendKey::SendId(format!("send-{name}")),
                false,
            )
            .await
            .unwrap();
        assert!(
            matches!(&first, SendOutcome::Active { run_id: active, .. } if *active == run_id),
            "the first send starts the run: {first:?}"
        );
        tokio::time::timeout(Duration::from_secs(10), executor.held.notified())
            .await
            .expect("the run reaches its durable terminal");
        (dir, key, registry, executor, run_id)
    }

    /// A caller that saw the run end through `run.result` and re-sends the same `send_id` must
    /// be told `finished` with the recorded reason, even though the actor has not yet processed
    /// the run's completion and still holds it as active.
    #[tokio::test]
    async fn resend_after_durable_terminal_answers_finished_before_completion_is_processed() {
        let (dir, key, registry, executor, run_id) =
            start_held_durable_finish("resend-durable-finish").await;
        let resend = || {
            registry.route_submission_with_materialization(
                &key,
                run_id.clone(),
                text_req(&key, &run_id.0).to_submission(),
                RunKind::FreshOrResume,
                SendKey::SendId("send-resend-durable-finish".into()),
                false,
            )
        };

        let held = resend().await.unwrap();
        assert!(
            matches!(
                &held,
                SendOutcome::Finished { run_id: finished, reason: RunFinishReason::MaxSteps, .. }
                    if *finished == run_id
            ),
            "a re-send after the terminal is durable must answer finished with the recorded \
             reason, not active: {held:?}"
        );

        // Once the actor processes the completion, the answer is the same.
        executor.release.notify_one();
        wait_for_idle(&registry, &key).await;
        let settled = resend().await.unwrap();
        assert!(
            matches!(
                &settled,
                SendOutcome::Finished { run_id: finished, reason: RunFinishReason::MaxSteps, .. }
                    if *finished == run_id
            ),
            "after completion is processed the re-send answers the same: {settled:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A steer retry agrees with every other re-send once the run it joined has a durable
    /// terminal: it answers `finished` with the recorded reason while the actor still holds the
    /// run as active, and carries the `step` receipt of the step that rendered it. After the
    /// actor processes the completion the answer is the same.
    #[tokio::test]
    async fn steer_retry_after_durable_terminal_answers_finished_with_its_receipt() {
        use cortexkit_role_llm_runner::send::{delivered_as, Delivered};
        let name = "steer-durable-finish";
        let dir =
            broca_test_support::scratch_root().join(format!("broca-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let executor = Arc::new(HoldsAfterDurableFinish {
            reason: RunFinishReason::MaxSteps,
            render_steer_first: true,
            steered: Notify::new(),
            held: Notify::new(),
            release: Notify::new(),
        });
        let opener = Arc::new(SeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store: Arc::new(FileWalStore::new(dir.join("wal"))),
            idle_ttl: Duration::from_secs(30),
            executor: executor.clone(),
        });
        let registry = LineageRegistry::new(opener);
        let run_id = RunId(format!("run-{name}"));
        let first = registry
            .route_submission_with_materialization(
                &key,
                run_id.clone(),
                text_req(&key, &run_id.0).to_submission(),
                RunKind::FreshOrResume,
                SendKey::SendId(format!("send-{name}")),
                false,
            )
            .await
            .unwrap();
        assert!(
            matches!(&first, SendOutcome::Active { run_id: active, .. } if *active == run_id),
            "the first send starts the run: {first:?}"
        );
        let steer_run = RunId(format!("run-steer-{name}"));
        let steer = || {
            registry.route_submission_with_delivery(
                &key,
                steer_run.clone(),
                text_req(&key, &steer_run.0).to_submission(),
                RunKind::FreshOrResume,
                SendKey::SendId(format!("steer-{name}")),
                false,
                broca_protocol::Delivery::Steer,
            )
        };
        let joined = steer().await.unwrap();
        assert!(
            matches!(
                &joined,
                SendOutcome::Active { run_id: active, delivered: None } if *active == run_id
            ),
            "the steer joins the running run: {joined:?}"
        );
        executor.steered.notify_one();
        tokio::time::timeout(Duration::from_secs(10), executor.held.notified())
            .await
            .expect("the run reaches its durable terminal");

        let receipt = Some(Delivered::new(delivered_as::STEP).with_ref(run_id.0.clone()));
        let held = steer().await.unwrap();
        assert!(
            matches!(
                &held,
                SendOutcome::Finished {
                    run_id: finished,
                    reason: RunFinishReason::MaxSteps,
                    delivered,
                } if *finished == run_id && *delivered == receipt
            ),
            "a steer retry after the terminal is durable answers finished with the recorded \
             reason and its step receipt: {held:?}"
        );

        executor.release.notify_one();
        wait_for_idle(&registry, &key).await;
        let settled = steer().await.unwrap();
        assert_eq!(
            format!("{settled:?}"),
            format!("{held:?}"),
            "after completion is processed the steer retry answers the same"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `run.status` for a run whose terminal is durable reports that terminal, not `active`,
    /// while the actor has not yet processed the run's completion.
    #[tokio::test]
    async fn run_status_after_durable_terminal_reports_it_before_completion_is_processed() {
        let (dir, key, registry, executor, run_id) =
            start_held_durable_finish("status-durable-finish").await;

        let held = registry
            .route_run_state(&key, run_id.clone())
            .await
            .unwrap()
            .expect("the run is known");
        assert_eq!(
            held.state,
            RunIndexState::MaxSteps,
            "run.status after the terminal is durable must report it, not active"
        );

        executor.release.notify_one();
        wait_for_idle(&registry, &key).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn slow_live_export_does_not_delay_queued_run_start() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-detached-export-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let executor = Arc::new(GateFirstRun {
            starts: AtomicUsize::new(0),
            release_first: tokio::sync::Notify::new(),
        });
        let export = Arc::new(BlockingExportIndex::new());
        let resolver = Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1));
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store,
            executor.clone(),
        )
        .with_export_index(export.clone())
        .with_charge_basis_resolver(resolver)
        .with_idle_ttl(Duration::from_secs(30));
        let registry = LineageRegistry::new(Arc::new(opener));

        assert!(matches!(
            registry
                .route_send(&key, text_req(&key, "r1"), RunKind::FreshOrResume)
                .await
                .expect("first send succeeds"),
            SendOutcome::Active { .. }
        ));
        assert!(matches!(
            registry
                .route_send(&key, text_req(&key, "r2"), RunKind::FreshOrResume)
                .await
                .expect("second send queues"),
            SendOutcome::Pending { .. }
        ));

        let export_entered = export.entered.notified();
        executor.release_first.notify_one();
        // Generous bound: this waits for liveness, not latency. Windows CI pays a real
        // fsync per actor open (spawn recovery proof) on a slow disk under 41-way test
        // parallelism, and a 1s bound flaked there.
        tokio::time::timeout(Duration::from_secs(15), export_entered)
            .await
            .expect("live export starts");
        let starts_before_export_release = executor.starts.load(Ordering::SeqCst);
        export.release();
        assert_eq!(
            starts_before_export_release, 2,
            "the queued run must start while the prior run's export is blocked"
        );
        tokio::time::timeout(Duration::from_secs(15), export.wait_for_completions(2))
            .await
            .expect("both detached exports complete after release");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn capped_export_barrier_prevents_queued_run_from_overshooting_hard_cap() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-capped-export-barrier-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let executor = Arc::new(GateFirstRun {
            starts: AtomicUsize::new(0),
            release_first: tokio::sync::Notify::new(),
        });
        let export = Arc::new(BlockingExportIndex::new());
        let caps = Arc::new(CapsStore::open_in_memory().unwrap());
        install_test_cap(&caps, &key, "blocking-export", 9, 10);
        let resolver = Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1));
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            executor.clone(),
        )
        .with_export_index(export.clone())
        .with_charge_basis_resolver(resolver)
        .with_cap_store(caps.clone())
        .with_idle_ttl(Duration::from_millis(20));
        let registry = LineageRegistry::new(Arc::new(opener));

        assert!(matches!(
            registry
                .route_send(&key, text_req(&key, "r1"), RunKind::FreshOrResume)
                .await
                .unwrap(),
            SendOutcome::Active { .. }
        ));
        assert!(matches!(
            registry
                .route_send(&key, text_req(&key, "r2"), RunKind::FreshOrResume)
                .await
                .unwrap(),
            SendOutcome::Pending { .. }
        ));

        let export_entered = export.entered.notified();
        executor.release_first.notify_one();
        tokio::time::timeout(Duration::from_secs(1), export_entered)
            .await
            .expect("capped terminal reaches synchronous export barrier");
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(
            executor.starts.load(Ordering::SeqCst),
            1,
            "the queued run cannot overtake the capped export barrier"
        );

        export.release();
        export.wait_for_completions(1).await;
        apply_test_spend(&caps, "blocking-export", 2);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let records = wal_store.read_records(&key).unwrap();
                if records.iter().any(|frame| {
                    matches!(
                        &frame.record,
                        WalRecord::Retracted {
                            failure: Some(failure),
                            ..
                        } if failure.error.message.contains("hard_cap_exceeded")
                    )
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("caught-up hard cap rejects the queued run");
        assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn capped_export_failure_keeps_queued_run_pending() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-capped-export-failure-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let executor = Arc::new(GateFirstRun {
            starts: AtomicUsize::new(0),
            release_first: tokio::sync::Notify::new(),
        });
        let caps = Arc::new(CapsStore::open_in_memory().unwrap());
        install_test_cap(&caps, &key, "failing-export", 0, 10);
        let resolver = Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1));
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            executor.clone(),
        )
        .with_export_index(Arc::new(FailingExportIndex))
        .with_charge_basis_resolver(resolver)
        .with_cap_store(caps)
        .with_idle_ttl(Duration::from_millis(20));
        let registry = LineageRegistry::new(Arc::new(opener));

        registry
            .route_send(&key, text_req(&key, "r1"), RunKind::FreshOrResume)
            .await
            .unwrap();
        assert!(matches!(
            registry
                .route_send(&key, text_req(&key, "r2"), RunKind::FreshOrResume)
                .await
                .unwrap(),
            SendOutcome::Pending { .. }
        ));
        executor.release_first.notify_one();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let records = wal_store.read_records(&key).unwrap();
        assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
        assert!(records.iter().any(|frame| matches!(
            &frame.record,
            WalRecord::Queued { request, .. } if request.model.model_id == "fake"
        )));
        assert!(!records
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::Retracted { .. })));
        assert!(!records.iter().any(|frame| matches!(
            &frame.record,
            WalRecord::RunStarted { run_id, .. } if run_id.0 == "r2"
        )));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn queued_trailing_watermark_retries_after_spend_delta() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-trailing-watermark-retry-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let executor = Arc::new(GateFirstRun {
            starts: AtomicUsize::new(0),
            release_first: tokio::sync::Notify::new(),
        });
        let export = Arc::new(BlockingExportIndex::new());
        let caps = Arc::new(CapsStore::open_in_memory().unwrap());
        install_test_cap(&caps, &key, "blocking-export", 0, 50);
        let counting_caps = Arc::new(CountingCapStore {
            inner: caps.clone(),
            matching_calls: AtomicUsize::new(0),
        });
        let resolver = Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1));
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            executor.clone(),
        )
        .with_export_index(export.clone())
        .with_charge_basis_resolver(resolver)
        .with_cap_store(counting_caps.clone())
        .with_idle_ttl(Duration::from_millis(20));
        let registry = LineageRegistry::new(Arc::new(opener));

        registry
            .route_send(&key, text_req(&key, "r1"), RunKind::FreshOrResume)
            .await
            .unwrap();
        assert!(matches!(
            registry
                .route_send(&key, text_req(&key, "r2"), RunKind::FreshOrResume)
                .await
                .unwrap(),
            SendOutcome::Pending { .. }
        ));

        let export_entered = export.entered.notified();
        executor.release_first.notify_one();
        tokio::time::timeout(Duration::from_secs(1), export_entered)
            .await
            .expect("capped export barrier entered");
        export.release();
        export.wait_for_completions(1).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while counting_caps.matching_calls.load(Ordering::SeqCst) < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued admission observes the trailing export watermark");

        let lagging_records = wal_store.read_records(&key).unwrap();
        assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
        assert!(!lagging_records
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::Retracted { .. })));

        apply_test_spend(&caps, "blocking-export", 0);
        wait_for_starts(&executor, 2).await;
        export.wait_for_completions(2).await;
        assert!(!wal_store
            .read_records(&key)
            .unwrap()
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::Retracted { .. })));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn finished_episode_reopen_backfills_export_without_run_index_row() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-finished-export-backfill-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let run_id = RunId("finished-before-index".into());
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let mut wal = wal_store.open(&key).expect("open WAL to seed terminal");
        wal.append(
            1,
            &WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                run_id: run_id.clone(),
                session: key.clone(),
                ts_ms: Some(100),
                config: Box::new(min_config()),
                input: text_req(&key, &run_id.0).input.into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            false,
        )
        .expect("seed run start");
        wal.append(
            1,
            &WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: Some(200),
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .expect("seed durable terminal");
        drop(wal);

        let export_store = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
        assert!(
            export_store.lookup_run(&run_id.0).unwrap().is_none(),
            "the residual crash window has no terminal run-index row"
        );
        let resolver = Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1));
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store,
            Arc::new(QuickDone {
                close_count: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .with_export_index(export_store.clone())
        .with_charge_basis_resolver(resolver)
        .with_idle_ttl(Duration::from_secs(30));
        let registry = LineageRegistry::new(Arc::new(opener));

        registry
            .actor_for(&key)
            .await
            .expect("explicitly reopen the finished lineage");
        registry
            .route_status(&key)
            .await
            .expect("opening the finished lineage succeeds");
        let page = export_store.read_page(None, 10).unwrap();
        assert_eq!(page.facts.len(), 1, "reopen emits exactly one export fact");
        assert_eq!(page.facts[0].segment.as_session().unwrap().run_id, run_id);
        assert_eq!(
            page.facts[0].segment.as_session().unwrap().occurred_at_ms,
            Some(200)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn finished_episode_reopen_ignores_export_backfill_failure() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-finished-export-failure-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let run_id = RunId("finished-with-export-down".into());
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let mut wal = wal_store.open(&key).expect("open WAL to seed terminal");
        wal.append(
            1,
            &WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                run_id: run_id.clone(),
                session: key.clone(),
                ts_ms: Some(100),
                config: Box::new(min_config()),
                input: text_req(&key, &run_id.0).input.into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            false,
        )
        .expect("seed run start");
        wal.append(
            1,
            &WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: Some(200),
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .expect("seed durable terminal");
        drop(wal);

        let resolver = Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1));
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store,
            Arc::new(QuickDone {
                close_count: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .with_export_index(Arc::new(FailingExportIndex))
        .with_charge_basis_resolver(resolver)
        .with_idle_ttl(Duration::from_secs(30));
        let registry = LineageRegistry::new(Arc::new(opener));

        registry.actor_for(&key).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), registry.route_status(&key))
            .await
            .expect("best-effort export failure must not hold startup recovery for retries")
            .expect("finished lineage remains accessible while export storage is unavailable");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(start_paused = true)]
    async fn startup_recovery_budget_heal_preserves_durable_fifo() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-actor-recovery-fifo-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FlakyReadWalStore::new(dir.join("wal")));
        seed_finished_episode_with_queue(wal_store.as_ref(), &key, "b");
        wal_store.set_fail_reads(true);

        let executor = Arc::new(RecoveryGate::new());
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            executor.clone(),
        )
        .with_idle_ttl(Duration::from_secs(3_600));
        let registry = LineageRegistry::new(Arc::new(opener));
        exhaust_startup_recovery_budget(&registry, &key, wal_store.as_ref()).await;

        wal_store.set_fail_reads(false);
        let outcome = registry
            .route_send(&key, text_req(&key, "c"), RunKind::FreshOrResume)
            .await
            .expect("send after WAL reads heal");
        assert!(
            matches!(outcome, SendOutcome::Pending { .. }),
            "new work must queue behind recovered durable work"
        );
        executor.wait_for_start().await;
        assert_eq!(executor.started(), vec!["b"]);
        assert_eq!(
            started_run_ids(&wal_store.durable_records(&key)),
            vec!["a", "b"],
            "the newer send must not start before the durable queue head"
        );

        executor.release_first.notify_one();
        executor.wait_for_start().await;
        executor.wait_for_finish().await;
        executor.wait_for_finish().await;
        assert_eq!(executor.started(), vec!["b", "c"]);
        assert_eq!(
            started_run_ids(&wal_store.durable_records(&key)),
            vec!["a", "b", "c"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(start_paused = true)]
    async fn startup_recovery_budget_exhaustion_fails_send_closed() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-recovery-fail-closed-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FlakyReadWalStore::new(dir.join("wal")));
        seed_finished_episode_with_queue(wal_store.as_ref(), &key, "b");
        wal_store.set_fail_reads(true);

        let executor = Arc::new(RecoveryGate::new());
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            executor.clone(),
        )
        .with_idle_ttl(Duration::from_secs(3_600));
        let registry = LineageRegistry::new(Arc::new(opener));
        exhaust_startup_recovery_budget(&registry, &key, wal_store.as_ref()).await;

        let outcome = registry
            .route_send(&key, text_req(&key, "c"), RunKind::FreshOrResume)
            .await
            .expect("failed-closed send returns an outcome");
        assert!(matches!(outcome, SendOutcome::Transient(_)));
        let records = wal_store.durable_records(&key);
        assert_eq!(started_run_ids(&records), vec!["a"]);
        let pending: Vec<_> = broca_core::pending_submissions(
            &records
                .iter()
                .map(|record| record.record.clone())
                .collect::<Vec<_>>(),
        )
        .into_iter()
        .map(|submission| submission.submission_id.0)
        .collect();
        assert_eq!(pending, vec!["sub-b"]);

        wal_store.set_fail_reads(false);
        let retry = registry
            .route_send(&key, text_req(&key, "c"), RunKind::FreshOrResume)
            .await
            .expect("retry after WAL reads heal");
        assert!(matches!(retry, SendOutcome::Pending { .. }));
        executor.wait_for_start().await;
        assert_eq!(executor.started(), vec!["b"]);
        assert_eq!(
            started_run_ids(&wal_store.durable_records(&key)),
            vec!["a", "b"]
        );
        executor.release_first.notify_one();
        executor.wait_for_start().await;
        executor.wait_for_finish().await;
        executor.wait_for_finish().await;
        assert_eq!(executor.started(), vec!["b", "c"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(start_paused = true)]
    async fn startup_recovery_budget_exhaustion_preserves_read_only_liveness() {
        use crate::fanout::FromCursor;

        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-recovery-read-only-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FlakyReadWalStore::new(dir.join("wal")));
        seed_finished_episode_with_queue(wal_store.as_ref(), &key, "b");
        wal_store.set_fail_reads(true);

        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            Arc::new(QuickDone {
                close_count: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .with_idle_ttl(Duration::from_secs(5));
        let registry = LineageRegistry::new_with_reader(
            Arc::new(opener),
            wal_store.clone() as Arc<dyn WalStore>,
            None,
        );
        exhaust_startup_recovery_budget(&registry, &key, wal_store.as_ref()).await;

        assert_eq!(
            registry.route_status(&key).await.expect("status responds"),
            LineageStatus::Idle
        );
        wal_store.discard_read_attempt_signals();
        tokio::time::advance(Duration::from_secs(5)).await;
        wal_store.wait_for_read_attempt().await;
        assert_eq!(
            registry
                .route_status(&key)
                .await
                .expect("status responds after failed idle-tick recovery"),
            LineageStatus::Idle
        );
        assert_eq!(registry.slot_state(&key), Some("open"));
        assert!(
            registry
                .route_run_state(&key, RunId("a".into()))
                .await
                .is_err(),
            "run.status must return its storage error instead of waiting for startup recovery"
        );
        assert!(
            registry.route_read(&key, None, None).await.is_err(),
            "session.read must return its storage error instead of waiting for startup recovery"
        );
        let (sink, _deliveries) = mpsc::channel(8);
        assert_eq!(
            registry
                .route_subscribe(&key, FromCursor::Start, sink)
                .await
                .expect("subscribe responds"),
            SubscribeOutcome::Attached
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn healthy_startup_does_not_repeat_recovery_before_send() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-healthy-recovery-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FlakyReadWalStore::new(dir.join("wal")));
        let executor = Arc::new(RecoveryGate::new());
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            executor.clone(),
        )
        .with_idle_ttl(Duration::from_secs(30));
        let registry = LineageRegistry::new(Arc::new(opener));

        let outcome = registry
            .route_send(&key, text_req(&key, "c"), RunKind::FreshOrResume)
            .await
            .expect("healthy send succeeds");
        assert!(matches!(outcome, SendOutcome::Active { .. }));
        executor.wait_for_start().await;
        assert_eq!(wal_store.read_attempts(), 1);
        executor.release_first.notify_one();
        executor.wait_for_finish().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn startup_recovery_retries_one_transient_wal_read_without_consuming_queue_head() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-one-read-failure-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FlakyReadWalStore::new(dir.join("wal")));

        let queued_req = text_req(&key, "queued");
        let mut wal = wal_store.open(&key).expect("open WAL to seed queue");
        wal.append(
            1,
            &WalRecord::Queued {
                retention: None,
                ts_ms: None,
                hooks: None,
                title: None,
                keep_warm: None,
                conversation_key: None,
                send_id: None,
                delivery: None,
                submission_id: broca_protocol::SubmissionId("sub-queued".into()),
                request: Box::new(queued_req.to_submission()),
                request_identity_digest: None,
                principal: None,
            },
            true,
        )
        .expect("seed durable queued work");
        drop(wal);
        wal_store.fail_next_read();

        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            Arc::new(QuickDone {
                close_count: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .with_idle_ttl(Duration::from_secs(30));
        let registry = LineageRegistry::new(Arc::new(opener));

        // Liveness wait, not a latency assertion; see the Windows CI note on the
        // slow-live-export test for why 1s bounds flake there.
        registry.actor_for(&key).await.unwrap();
        tokio::time::timeout(Duration::from_secs(15), registry.route_status(&key))
            .await
            .expect("one transient recovery read must retry, then serve the queued command")
            .expect("status after recovery");

        let records: Vec<_> = wal_store
            .read_records(&key)
            .expect("read recovered WAL")
            .into_iter()
            .map(|record| record.record)
            .collect();
        assert!(records.iter().any(|record| {
            matches!(
                record,
                WalRecord::RunStarted {
                    run_id,
                    submission_id: Some(submission_id),
                    ..
                } if run_id.0 == "queued" && submission_id.0 == "sub-queued"
            )
        }));
        assert!(!records.iter().any(|record| {
            matches!(
                record,
                WalRecord::Retracted {
                    failure: Some(_),
                    ..
                }
            )
        }));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn wal_read_failure_keeps_actor_open_until_queued_work_can_drain() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-actor-read-failure-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FlakyReadWalStore::new(dir.join("wal")));

        let queued_req = text_req(&key, "queued");
        let mut wal = wal_store.open(&key).expect("open WAL to seed queue");
        wal.append(
            1,
            &WalRecord::Queued {
                retention: None,
                ts_ms: None,
                hooks: None,
                title: None,
                keep_warm: None,
                conversation_key: None,
                send_id: None,
                delivery: None,
                submission_id: broca_protocol::SubmissionId("sub-queued".into()),
                request: Box::new(queued_req.to_submission()),
                request_identity_digest: None,
                principal: None,
            },
            true,
        )
        .expect("seed durable queued work");
        drop(wal);
        wal_store.set_fail_reads(true);

        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            Arc::new(QuickDone {
                close_count: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .with_idle_ttl(Duration::from_millis(40));
        let registry = LineageRegistry::new(Arc::new(opener));
        registry.actor_for(&key).await.expect("open session");

        tokio::time::sleep(Duration::from_millis(220)).await;
        assert_eq!(
            registry.slot_state(&key),
            Some("open"),
            "a WAL read failure must abort close because queued work may exist"
        );

        wal_store.set_fail_reads(false);
        // Bounded by the shared hang guard, not a fixed window: on a loaded runner the actor's
        // next attempt can come later than two seconds, and that is not the defect this test
        // looks for. Work that never drains still fails here, with the last records seen.
        let healed_at = std::time::Instant::now();
        broca_test_support::wait_until("queued work to drain after WAL reads recover", || {
            let records: Vec<_> = wal_store
                .read_records(&key)
                .expect("healed WAL read")
                .into_iter()
                .map(|framed| framed.record)
                .collect();
            let started = records.iter().any(|record| {
                matches!(
                    record,
                    WalRecord::RunStarted {
                        submission_id: Some(id),
                        ..
                    } if id.0 == "sub-queued"
                )
            });
            let result = if broca_core::pending_submissions(&records).is_empty() && started {
                Ok(())
            } else {
                Err(broca_test_support::record_kinds(&records))
            };
            std::future::ready(result)
        })
        .await;
        eprintln!(
            "queued work drained {:?} after reads recovered",
            healed_at.elapsed()
        );

        for _ in 0..40 {
            if registry.slot_state(&key).is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn one_shot_run_closes_its_actor_at_the_terminal_and_a_follow_up_reopens() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-actor-one-shot-close-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        // The idle TTL is far longer than the test, so only the one-shot rule can close the actor.
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            // Writes a durable RunFinished, so the reopened actor sees an idle lineage.
            Arc::new(RetentionDone),
        )
        .with_idle_ttl(Duration::from_secs(3_600));
        let registry = LineageRegistry::new(Arc::new(opener));

        let wait_closed = |registry: &LineageRegistry| {
            let registry = registry.clone();
            let key = key.clone();
            async move {
                for _ in 0..200 {
                    if registry.slot_state(&key).is_none() {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                false
            }
        };

        let outcome = registry
            .route_submission_with_materialization(
                &key,
                RunId("one-shot".into()),
                text_req(&key, "one-shot").to_submission(),
                RunKind::FreshOrResume,
                SendKey::RunId,
                true,
            )
            .await
            .expect("one-shot send routes");
        assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
        assert!(
            wait_closed(&registry).await,
            "a finished one-shot run must close its actor without waiting for the idle TTL"
        );

        // The same session reopens for a follow-up, and an ordinary (materialized) run keeps the
        // ordinary idle TTL rather than closing at its terminal.
        let outcome = registry
            .route_submission_with_materialization(
                &key,
                RunId("follow-up".into()),
                text_req(&key, "follow-up").to_submission(),
                RunKind::AppendEpisode,
                SendKey::RunId,
                false,
            )
            .await
            .expect("follow-up send reopens the session");
        assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
        for _ in 0..200 {
            if registry.route_status(&key).await.expect("status") == LineageStatus::Idle {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            registry.slot_state(&key),
            Some("open"),
            "a materialized run's actor stays open for the idle TTL"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn one_shot_run_keeps_its_actor_open_for_a_live_subscriber() {
        use crate::fanout::FromCursor;

        let dir = broca_test_support::scratch_root().join(format!(
            "broca-actor-one-shot-subscribed-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            Arc::new(RetentionDone),
        )
        .with_idle_ttl(Duration::from_secs(3_600));
        let registry = LineageRegistry::new(Arc::new(opener));
        let (sink, _deliveries) = mpsc::channel(64);
        registry
            .route_subscribe(&key, FromCursor::Start, sink)
            .await
            .expect("subscribe opens the session");

        let outcome = registry
            .route_submission_with_materialization(
                &key,
                RunId("one-shot".into()),
                text_req(&key, "one-shot").to_submission(),
                RunKind::FreshOrResume,
                SendKey::RunId,
                true,
            )
            .await
            .expect("one-shot send routes");
        assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
        for _ in 0..200 {
            if registry.route_status(&key).await.expect("status") == LineageStatus::Idle {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            registry.slot_state(&key),
            Some("open"),
            "a subscriber still observing the session keeps a one-shot's actor open"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
    include!("cancel_lifecycle_tests.rs");
    include!("admission_lifecycle_tests.rs");
}

#[cfg(test)]
mod import_idempotency_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use broca_core::{
        FileLeaseStore, FileWalStore, MaterializedStore, OutputEvent, RunError, RunIndex,
        RunIndexState, RunSummary, WalHandle, WalStore,
    };
    use broca_protocol::caller::RunFinishReason;
    use broca_protocol::content::{
        AssistantMessage, ContentBlock, FinishReason, Message, Role, ToolCall,
    };
    use broca_protocol::{BindIdentity, MessageId, RunId, StepId, ToolCallId, Usage};

    use super::tests::{min_config, text_req, SeamOpener};
    use super::{ImportOutcome, LineageStatus};
    use crate::executor::{RunExecutor, RunJob, RunKind};
    use crate::opener::DefaultOpener;
    use crate::registry::LineageRegistry;

    struct HoldsAfterAdmission;

    #[async_trait]
    impl RunExecutor for HoldsAfterAdmission {
        async fn execute(
            &self,
            job: RunJob,
            _out: tokio::sync::mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            job.writer.record(broca_wal::WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: None,
                run_id: job.req.run_id.clone(),
                session: job.req.session.clone(),
                config: Box::new(min_config()),
                input: job.req.input.into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            });
            job.writer
                .sync()
                .await
                .map_err(|error| RunError::Wal(error.to_string()))?;
            if let Some(ack) = job.on_admitted {
                let _ = ack.send(Ok(job.req.run_id));
            }
            std::future::pending().await
        }
    }

    struct CountingReadWalStore {
        inner: FileWalStore,
        reads: AtomicUsize,
    }

    impl CountingReadWalStore {
        fn new(root: impl Into<std::path::PathBuf>) -> Self {
            Self {
                inner: FileWalStore::new(root),
                reads: AtomicUsize::new(0),
            }
        }

        fn reset_reads(&self) {
            self.reads.store(0, Ordering::SeqCst);
        }

        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    impl WalStore for CountingReadWalStore {
        fn open(
            &self,
            session: &BindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            self.inner.open(session)
        }

        fn read_records(
            &self,
            session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.read_records(session)
        }

        fn read_records_range(
            &self,
            session: &BindIdentity,
            after_seq: u64,
            through_seq: Option<u64>,
            anchor: Option<&broca_wal::WalByteAnchor>,
        ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
            self.inner
                .read_records_range(session, after_seq, through_seq, anchor)
        }
    }

    #[tokio::test]
    async fn indexed_interrupted_status_reconciles_tool_call_ids_from_wal() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-interrupted-status-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("project")).expect("create project");
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let run_id = RunId("interrupted-run".into());
        let wal_store = Arc::new(CountingReadWalStore::new(root.join("wal")));
        let writer =
            WalHandle::spawn(wal_store.open(&key).expect("open WAL"), 1).expect("spawn WAL writer");
        writer.record(broca_wal::WalRecord::RunStarted {
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            ts_ms: None,
            run_id: run_id.clone(),
            session: key.clone(),
            config: Box::new(min_config()),
            input: Vec::new().into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        });
        writer.record(broca_wal::WalRecord::ModelStepFinished {
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call-1".into()),
                    tool_name: "edit".into(),
                    input: serde_json::json!({}),
                })],
            }
            .into(),
            usage: Usage::default(),
            finish_reason: Some(FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        });
        writer.record(broca_wal::WalRecord::ToolBatchStarted {
            batch_id: broca_protocol::BatchId("b1".into()),
            step_id: StepId(1),
            planned_calls: vec![ToolCallId("call-1".into())],
        });
        writer.record(broca_wal::WalRecord::ToolDispatchIntent {
            hooks: None,
            batch_id: broca_protocol::BatchId("b1".into()),
            tool_call_id: ToolCallId("call-1".into()),
            tool_name: Some("edit".into()),
            args: serde_json::json!({}).into(),
            module_id: None,
            schema_pin: None,
            approval_digest: None,
            call_key: None,
        });
        writer.record(broca_wal::WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Interrupted,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        });
        writer.sync().await.expect("sync interrupted lineage");
        let _ = writer.shutdown().await;

        let store =
            Arc::new(broca_store::SqliteStore::open_in_memory().expect("open materialized store"));
        store
            .index_run_active(&broca_core::RunIndexRow {
                withdrawals_owed: false,
                title: None,
                run_id: run_id.0.clone(),
                session: key.clone(),
                submission_id: None,
                request_identity_digest: None,
                state: RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
            })
            .expect("index active run");
        store
            .index_run_terminal(
                &run_id.0,
                RunIndexState::Interrupted,
                Some(Usage::default()),
                Some(FinishReason::ToolCalls),
                0,
            )
            .expect("index interrupted terminal");
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            wal_store.clone(),
            Arc::new(HoldsAfterAdmission),
        )
        .with_run_index(store);
        let registry = LineageRegistry::new(Arc::new(opener));
        registry
            .actor_for(&key)
            .await
            .expect("explicitly open actor");
        assert_eq!(
            registry.route_status(&key).await.expect("open actor"),
            LineageStatus::Idle
        );

        let status = registry
            .route_run_state(&key, run_id)
            .await
            .expect("status lookup succeeds")
            .expect("indexed run exists");
        assert_eq!(
            status.indeterminate_tool_calls,
            Some(vec![ToolCallId("call-1".into())])
        );

        let _ = std::fs::remove_dir_all(root);
    }

    fn import_request(key: &BindIdentity, source_ref: &str) -> broca_core::ImportRequest {
        broca_core::ImportRequest {
            project_id: None,
            principal: None,
            run_id: RunId("completed-import".into()),
            session: key.clone(),
            messages: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "imported".into(),
                }],
            )],
            origin: broca_core::ImportOrigin {
                source_ref: source_ref.into(),
            },
        }
    }

    #[tokio::test]
    async fn idle_fresh_import_reads_the_full_wal_once() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-import-idle-read-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("project")).expect("create project");
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let wal_store = Arc::new(CountingReadWalStore::new(root.join("wal")));
        let lease_store = Arc::new(FileLeaseStore::new(root.join("leases")));
        let store =
            Arc::new(broca_store::SqliteStore::open_in_memory().expect("open materialized store"));
        let opener = DefaultOpener::new(
            lease_store,
            wal_store.clone(),
            Arc::new(HoldsAfterAdmission),
        )
        .with_idle_ttl(Duration::from_secs(3600))
        .with_materialized_store(store);
        let registry = LineageRegistry::new(Arc::new(opener));

        registry
            .actor_for(&key)
            .await
            .expect("explicitly open idle actor");
        assert_eq!(
            registry.route_status(&key).await.expect("open idle actor"),
            LineageStatus::Idle
        );
        wal_store.reset_reads();

        assert_eq!(
            registry
                .route_import(&key, import_request(&key, "cc:idle-fresh"))
                .await
                .expect("route fresh import"),
            ImportOutcome::Imported(broca_core::ImportOutcome {
                run_id: RunId("completed-import".into()),
                synthesized_tool_results: 0,
                dropped_orphan_tool_results: 0,
                dropped_system_messages: 0,
                already_imported: false,
            })
        );
        assert_eq!(wal_store.reads(), 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn idle_completed_import_retry_preserves_ack_and_projects() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-import-idle-retry-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("project")).expect("create project");
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let wal_store = Arc::new(FileWalStore::new(root.join("wal")));
        let lease_store = Arc::new(FileLeaseStore::new(root.join("leases")));
        let request = import_request(&key, "cc:idle-retry");
        let writer = WalHandle::spawn(wal_store.open(&key).expect("open import WAL"), 1)
            .expect("spawn writer");
        broca_core::run_import(request.clone(), &writer, wal_store.as_ref())
            .await
            .expect("complete import without projection");
        let _ = writer.shutdown().await;

        let store =
            Arc::new(broca_store::SqliteStore::open_in_memory().expect("open materialized store"));
        let opener = DefaultOpener::new(lease_store, wal_store, Arc::new(HoldsAfterAdmission))
            .with_idle_ttl(Duration::from_secs(3600))
            .with_materialized_store(store.clone());
        let registry = LineageRegistry::new(Arc::new(opener));

        assert_eq!(
            registry
                .route_import(&key, request)
                .await
                .expect("route completed retry"),
            ImportOutcome::Imported(broca_core::ImportOutcome {
                run_id: RunId("completed-import".into()),
                synthesized_tool_results: 0,
                dropped_orphan_tool_results: 0,
                dropped_system_messages: 0,
                already_imported: true,
            })
        );
        let projected = store
            .read_page_with_watermark(&key, Some(0), 10, broca_core::context::ReadView::Raw)
            .expect("read projected retry");
        assert_eq!(projected.rows.len(), 1);
        assert!(matches!(
            &projected.rows[0].message.content[0],
            ContentBlock::Text { text } if text == "imported"
        ));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn completed_import_retry_wins_over_a_later_active_run() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-import-active-retry-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("project")).expect("create project");
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let wal_store = Arc::new(FileWalStore::new(root.join("wal")));
        let lease_store = Arc::new(FileLeaseStore::new(root.join("leases")));
        let import_request = broca_core::ImportRequest {
            project_id: None,
            principal: None,
            run_id: RunId("completed-import".into()),
            session: key.clone(),
            messages: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "imported".into(),
                }],
            )],
            origin: broca_core::ImportOrigin {
                source_ref: "cc:active-retry".into(),
            },
        };
        let writer = WalHandle::spawn(wal_store.open(&key).expect("open import WAL"), 1)
            .expect("spawn writer");
        broca_core::run_import(import_request.clone(), &writer, wal_store.as_ref())
            .await
            .expect("complete import");
        let _ = writer.shutdown().await;

        let registry = LineageRegistry::new(Arc::new(SeamOpener {
            lease_store,
            wal_store,
            idle_ttl: Duration::from_secs(3600),
            executor: Arc::new(HoldsAfterAdmission),
        }));
        registry
            .route_send(&key, text_req(&key, "later-run"), RunKind::FreshOrResume)
            .await
            .expect("route later run");

        let retry = registry
            .route_import(&key, import_request.clone())
            .await
            .expect("route import retry");
        assert!(matches!(
            retry,
            ImportOutcome::Imported(broca_core::ImportOutcome {
                already_imported: true,
                ..
            })
        ));

        let mut changed = import_request;
        changed.messages = vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "changed".into(),
            }],
        )];
        assert_eq!(
            registry
                .route_import(&key, changed)
                .await
                .expect("route changed retry"),
            ImportOutcome::Error(broca_core::ImportError::ImportIdReuse)
        );
        let _ = std::fs::remove_dir_all(root);
    }
}

#[cfg(test)]
mod run_gauge_transition_tests {
    /// The health gauge and `RunState` must move together, and the compiler cannot require it.
    ///
    /// SUBC's asymmetry is why this is worth a mechanical check rather than care: a stuck-HIGH
    /// gauge fails safe (an operator waits for a quiet window that never arrives, and nobody's
    /// run is cut), while a stuck-LOW gauge is silently lethal — 0 is exactly what an idle module
    /// looks like, so an operator checking before a restart cuts a run mid-generation.
    ///
    /// Asserting the invariant STRUCTURALLY rather than behaviourally is deliberate. Driving the
    /// real transitions needs a lease, a writer and a spawned run task, and a test that models
    /// the setters instead of calling them would pass while a new start path skipped the gauge —
    /// which is the exact defect this exists to catch. Counting the assignment sites cannot be
    /// satisfied by re-implementing the rule.
    #[test]
    fn run_state_is_only_assigned_inside_the_two_gauge_setters() {
        let source = include_str!("actor.rs");
        let assignments: Vec<&str> = source
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with("self.state = "))
            .collect();

        assert_eq!(
            assignments.len(),
            2,
            "expected exactly two assignments to self.state, both inside set_active/set_idle. \
             A new one means a transition that may not have moved the health gauge: if it starts \
             a run without raising it, runs_in_flight reads 0 during live work and an operator \
             restarts into it. Route the new site through set_active or set_idle. Found: {assignments:?}"
        );
    }
}

#[cfg(test)]
mod steer_auth_conflict_tests {
    //! How a joining steer's auth pin compares with the active run's frozen auth selection.
    use super::steer_settings_conflict;
    use broca_protocol::request::SubmissionRequest;

    /// A steer that names nothing but `auth`, with `auth` omitted when `pin` is `None`.
    fn steer(pin: Option<serde_json::Value>) -> SubmissionRequest {
        let mut body = serde_json::json!({
            "model": {"provider_module_id": "fake", "model_id": "fake"},
            "input": []
        });
        if let Some(pin) = pin {
            body["auth"] = pin;
        }
        serde_json::from_value(body).expect("a steer request")
    }

    /// The active run's frozen config, holding `credential_id` as its auth selection.
    fn frozen(credential_id: Option<&str>) -> broca_wal::RunConfig {
        let mut config = super::tests::min_config();
        config.auth_selection = credential_id.map(|id| broca_wal::AuthSelection {
            credential_id: id.into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        });
        config
    }

    fn conflict(
        pin: Option<serde_json::Value>,
        credential_id: Option<&str>,
    ) -> Option<&'static str> {
        steer_settings_conflict(&steer(pin), &frozen(credential_id), false)
    }

    #[test]
    fn an_omitted_auth_pin_inherits_whatever_the_run_froze() {
        assert_eq!(conflict(None, Some("oauth:anthropic:work")), None);
        assert_eq!(conflict(None, Some("apikey:anthropic")), None);
        assert_eq!(conflict(None, None), None);
    }

    #[test]
    fn a_pin_naming_the_frozen_method_is_admitted() {
        let apikey = serde_json::json!({"method": "apikey"});
        assert_eq!(
            conflict(Some(apikey.clone()), Some("apikey:anthropic")),
            None
        );
        // A method-only pin matches a run that routing put on one account of that method.
        let oauth = serde_json::json!({"method": "oauth"});
        assert_eq!(conflict(Some(oauth), Some("oauth:anthropic:work")), None);
        // Naming the same method and the same account is also a match.
        let pinned = serde_json::json!({"method": "oauth", "account": "work"});
        assert_eq!(conflict(Some(pinned), Some("oauth:anthropic:work")), None);
    }

    #[test]
    fn a_pin_naming_another_method_is_refused_as_auth() {
        let apikey = serde_json::json!({"method": "apikey"});
        assert_eq!(
            conflict(Some(apikey), Some("oauth:anthropic:work")),
            Some("auth")
        );
        let oauth = serde_json::json!({"method": "oauth", "account": "work"});
        assert_eq!(
            conflict(Some(oauth), Some("apikey:anthropic:work")),
            Some("auth")
        );
    }

    #[test]
    fn a_pin_naming_another_account_is_refused_as_auth() {
        let pin = serde_json::json!({"method": "oauth", "account": "home"});
        assert_eq!(
            conflict(Some(pin), Some("oauth:anthropic:work")),
            Some("auth")
        );
    }

    #[test]
    fn a_pin_naming_an_account_the_run_never_froze_is_refused_as_auth() {
        let pin = serde_json::json!({"method": "apikey", "account": "paid"});
        // The run froze the method but no account.
        assert_eq!(
            conflict(Some(pin.clone()), Some("apikey:anthropic")),
            Some("auth")
        );
        // The run froze no auth selection at all.
        assert_eq!(conflict(Some(pin), None), Some("auth"));
        let method_only = serde_json::json!({"method": "apikey"});
        assert_eq!(conflict(Some(method_only), None), Some("auth"));
    }

    #[test]
    fn auth_is_reported_after_every_generation_field() {
        let mut request = steer(Some(serde_json::json!({"method": "apikey"})));
        request.generation.stop_sequences = vec!["other".into()];
        assert_eq!(
            steer_settings_conflict(&request, &frozen(Some("oauth:anthropic:work")), false),
            Some("generation.stop_sequences")
        );
    }
}

#[cfg(test)]
#[path = "shutdown_tests.rs"]
mod shutdown_tests;

#[cfg(test)]
#[path = "steer_terminal_tests.rs"]
mod steer_terminal_tests;

#[cfg(test)]
#[path = "writer_shutdown_tests.rs"]
mod writer_shutdown_tests;

#[path = "pre_user.rs"]
mod pre_user;

#[cfg(test)]
#[path = "pre_user_tests.rs"]
mod pre_user_tests;

#[cfg(test)]
#[path = "wal_test_support.rs"]
mod wal_test_support;

#[cfg(test)]
#[path = "sealed_attempt_tests.rs"]
mod sealed_attempt_tests;

#[cfg(test)]
#[path = "graceful_stop_tests.rs"]
mod graceful_stop_tests;

#[cfg(test)]
#[path = "crash_orphan_tests.rs"]
mod crash_orphan_tests;

#[cfg(test)]
#[path = "cache_warm_tests.rs"]
mod cache_warm_tests;

impl<W: WalStore + 'static> LineageActor<W> {
    fn retention_timestamp(&self) -> Option<u64> {
        (self.admitting_retention.is_some()
            || self.retention_lineage
            || self
                .writer
                .as_ref()
                .and_then(|writer| writer.is_retention_lineage())
                == Some(true))
        .then(|| {
            self.registry
                .retention_service()
                .map_or_else(epoch_ms, |service| service.clock.now_ms())
        })
    }

    async fn sync_retention_index(&mut self) -> Result<(), String> {
        let Some(service) = self.registry.retention_service() else {
            return Ok(());
        };
        self.retention_deadline = None;
        let writer = self.writer.as_ref().expect("open writer");
        if writer.is_retention_lineage() == Some(false) && self.admitting_retention.is_none() {
            self.retention_lineage = false;
            self.retention_deadline = None;
            return Ok(());
        }
        // A failed sync or a missing identity leaves the row unprojected exactly as a failed
        // index write does, so it enrolls the same repair rather than waiting for a reopen.
        let identified = match writer.sync().await {
            Ok(_) => writer
                .lineage_id()
                .ok_or_else(|| "retention WAL has no identity".to_owned()),
            Err(error) => Err(error.to_string()),
        };
        let id = match identified {
            Ok(id) => id,
            Err(error) => {
                self.registry.repair_retention(&self.key);
                tracing::warn!(target: "retention", "retention sync could not read the durable head; repair enrolled");
                return Err(error);
            }
        };
        let head = writer.durable_head().load(Ordering::Acquire);
        self.retention_lineage = writer.is_retention_lineage() == Some(true);
        // Never schedule irreversible deletion from a stale summary. If a later send
        // shortened retention, deferring until repair may expire late, but never early.
        let key = self.key.clone();
        let retention_wal = self.wal_store.clone();
        let result = tokio::task::spawn_blocking(move || {
            service.sync_actor_head(retention_wal.as_ref(), &key, id, head)
        })
        .await
        .map_err(|e| e.to_string())
        .and_then(|result| result);
        if result.is_err() {
            self.registry.repair_retention(&self.key);
            tracing::warn!(target: "retention", "content-free index write failed; repair enrolled");
        }
        self.retention_deadline = result?;
        Ok(())
    }

    /// Give the run just admitted its retained lineage on its run-index row.
    ///
    /// `sync_retention_index` keeps the hot path bounded: it reuses a lineage summary that
    /// already matches the WAL, or projects run rows only when a 256 KiB tail reaches the
    /// lineage's first record. A later run in a long retained session therefore often gets
    /// no projection here, and a session read can refresh the summary without run rows,
    /// after which enrollment repair sees a current summary and never projects the run.
    /// Without a lineage the row is missed by withdrawal discovery and by expiry accounting
    /// that reads runs by lineage. This point update costs no WAL read. The store changes
    /// the row only when a live retained lineage with this id exists for this session, so
    /// a forever session is never stamped.
    async fn stamp_retained_run(&mut self, run_id: &RunId) {
        let Some(service) = self.registry.retention_service() else {
            return;
        };
        let Some(writer) = self.writer.as_ref() else {
            return;
        };
        if writer.is_retention_lineage() == Some(false) {
            return;
        }
        let Some(id) = writer.lineage_id() else {
            return;
        };
        let key = self.key.clone();
        let run = run_id.0.clone();
        let stamped = tokio::task::spawn_blocking(move || {
            service.index.stamp_run_lineage(&run, &key, &id.to_string())
        })
        .await
        .map_err(|e| e.to_string())
        .and_then(|result| result);
        if stamped.is_err() {
            self.registry.repair_retention(&self.key);
            tracing::warn!(target: "retention", "run lineage stamp failed; repair enrolled");
        }
    }

    async fn expire_actor(&mut self, now_ms: u64) -> bool {
        let records = match self.read_wal_framed_records().await {
            Ok(records) => records,
            Err(_) => {
                self.retention_deadline = Some(now_ms.saturating_add(1000));
                return false;
            }
        };
        self.retention_deadline =
            crate::retention::policy(&records).and_then(|(_, _, deadline)| deadline);
        let Some(deadline) = self
            .retention_deadline
            .filter(|deadline| *deadline <= now_ms)
        else {
            return false;
        };
        if matches!(self.state, RunState::Active { .. }) {
            self.retention_deadline = None;
            return false;
        }
        let Some(close) = self.registry.begin_close(&self.key, self.gen) else {
            // The slot is not ours to close right now. Leaving the elapsed deadline in place
            // would wake the actor loop immediately and reread the whole WAL in a busy loop;
            // retry after the same back-off as a failed read.
            self.retention_deadline = Some(now_ms.saturating_add(1000));
            return false;
        };
        self.pending_fifo.clear();
        if let Some(task) = self.warm_task.take() {
            task.abort();
            let _ = task.await;
        }
        self.fanout.expire(deadline).await;
        // Child tasks are stopped before the shared writer closes and releases its lease.
        self.shutdown_writer().await;
        // The lease still excludes writers. Export owed question identities before deleting
        // the expired content; the retention worker can then finish without this actor.
        if let Some(service) = self.registry.retention_service() {
            let key = self.key.clone();
            let deleted =
                tokio::task::spawn_blocking(move || service.delete_expired(&key, now_ms)).await;
            let failure = match deleted {
                Ok(Ok(_)) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(join) => Some(format!("deletion task failed: {join}")),
            };
            if let Some(error) = failure {
                self.registry.repair_retention(&self.key);
                tracing::warn!(target: "retention", %error, "expired content deletion deferred to sweep");
            }
        }
        while let Ok(cmd) = self.rx.try_recv() {
            self.reply_writer_poisoned(cmd, "expired".into());
        }
        let _ = tokio::time::timeout(
            ON_SESSION_CLOSE_TIMEOUT,
            self.executor.on_session_close(&self.key),
        )
        .await;
        drop(self.lease.take());
        close.commit();
        true
    }
}

#[cfg(test)]
#[path = "approval_tests.rs"]
mod approval_tests;

#[cfg(test)]
#[cfg(feature = "test-clock")]
#[path = "actor_agent_run_scope_tests.rs"]
mod agent_run_scope_tests;

#[cfg(test)]
#[path = "wal_proof_tests.rs"]
mod wal_proof_tests;

#[cfg(test)]
#[path = "closed_poll_tests.rs"]
mod closed_poll_tests;

#[cfg(test)]
#[path = "actor_lifecycle_audit_tests.rs"]
mod lifecycle_audit_tests;
