//! Commands and outcomes shared by the session actors.
//!
//! `RetiredForCut` carries the session lease a retired actor hands to a cut.

use std::sync::Arc;
use std::time::Duration;

use broca_core::ReconciledRunState;
use broca_protocol::{request::SubmissionRequest, RunId};
use cortexkit_role_llm_runner::send::Delivered;
use tokio::sync::{mpsc, oneshot};

use crate::actor::RetiredForCut;
use crate::executor::RunKind;

pub(crate) mod pre_user;

/// Deadline for the Interrupted seal write (WAL barriers, replay and index update), measured
/// from entry to shutdown sealing. Attempt handover and the aborted run's join spend this
/// time too, but the join itself is not timed out. Child teardown and the writer join retain
/// the session actor's registry slot and session lease until complete; only the registry's total
/// deadline bounds that wait.
pub(crate) const SHUTDOWN_SEAL_BOUND: Duration = Duration::from_secs(5);

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

/// An actor's answer to [`LineageCmd::CutLock`].
pub struct CutLockReply {
    /// Why the session cannot move now, or `None` when it is idle.
    pub busy: Option<String>,
    /// Dropping or signalling this ends the cut's critical section for the actor.
    pub release: oneshot::Sender<()>,
    /// Set when an idle actor retired for a mutating cut: it closed its writer and will not
    /// resume when released.
    pub retired: Option<RetiredForCut>,
}

/// A command to a session actor. Each carries a reply channel; the actor NEVER silently drops a
/// command with a live reply (on close it replies `Reopen` so the caller re-routes).
pub enum LineageCmd {
    /// Pause dispatch while the cut holds its actor critical section. The reply
    /// carries a release channel whose drop also unblocks a cancelled operation.
    /// A mutating cut may instead retire an idle actor; see
    /// [`crate::registry::CutLockMode`].
    CutLock {
        reply: oneshot::Sender<CutLockReply>,
        mode: crate::registry::CutLockMode,
    },
    RepairRetention {
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Boot recovery after the serving connection and its dependency routes are registered.
    ResumeRestart {
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// A late result for one of this session's provider-held calls was deposited in its
    /// waiting slot. A run paused on held calls is resumed into its original batch so it can
    /// take it; a live run's batch takes it where it waits. Never re-sends the held call.
    ResumeHeld {
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
    /// A command only a session kept in the session store serves: a file-source import, or
    /// installing the compaction provider's first view (see `store_import`). The legacy actor
    /// refuses it: a session on today's storage already exists.
    Store {
        command: crate::store_import::StoreCommand,
        reply: oneshot::Sender<crate::store_import::StoreOutcome>,
    },
    /// A read of a session kept in the session store, which its store actor answers from the
    /// session it holds (see `store_reads`). The legacy actor holds no such session: it answers
    /// `Reopen`, and the reader reads the session as stored.
    StoreRead {
        request: Box<crate::store_reads::StoreReadRequest>,
        reply: oneshot::Sender<crate::store_reads::StoreReadOutcome>,
    },
    /// Cancel a run by id. A boundary request preserves the in-flight step and tool batch.
    Cancel {
        run_id: RunId,
        at: broca_wal::record::RunCancelAt,
        reply: oneshot::Sender<CancelOutcome>,
    },
    CancelFrom {
        run_id: RunId,
        at: broca_wal::record::RunCancelAt,
        principal: Option<broca_protocol::BindPrincipal>,
        scope: broca_protocol::SendScope,
        reply: oneshot::Sender<CancelOutcome>,
    },
    SenderPolicy {
        principal: Option<broca_protocol::BindPrincipal>,
        scope: broca_protocol::SendScope,
        at: broca_wal::record::RunCancelAt,
        reply: oneshot::Sender<Result<Option<CancelAuthority>, crate::serve::ServeError>>,
    },
    /// Retract a still-pending queued submission (durable tombstone). Replies the outcome.
    Retract {
        submission_id: broca_protocol::SubmissionId,
        reply: oneshot::Sender<RetractOutcome>,
    },
    /// Accept a mid-session plan change, durable before its reply. The manifest was fetched
    /// by the serve layer with the same admission code as a first send. Applies at the next run
    /// selected by its policy (`immediately` or `on_prefix_rebuild`), never inside a running episode.
    SessionRefresh {
        generation: u64,
        policy: String,
        manifest: Box<broca_wal::PlanManifest>,
        reply: oneshot::Sender<SessionRefreshOutcome>,
    },
    /// Change when the pending plan takes effect, durable before its reply. Idempotent on `send_id`.
    SessionRefreshPolicy {
        generation: u64,
        policy: String,
        send_id: String,
        reply: oneshot::Sender<SessionRefreshPolicyOutcome>,
    },
    /// Drop the frozen prefix and apply any pending change at the next run, durable before
    /// its reply. Idempotent on `send_id`.
    SessionFlushPrefix {
        send_id: String,
        reply: oneshot::Sender<SessionFlushPrefixOutcome>,
    },
    /// Report the actor's current status.
    Status {
        reply: oneshot::Sender<LineageStatus>,
    },
    /// Estimate what the session holds in memory, by holder, for `memory.report`: a store
    /// actor answers with its sizes, walked now; an actor of today's storage answers that it is
    /// one, and is counted only.
    MemoryReport {
        reply: oneshot::Sender<crate::memory_report::MemoryAnswer>,
    },
    /// Resolve a SPECIFIC run_id's lifecycle state: live actor → durable index terminal →
    /// WAL reconciliation when the index still says Active or has no row. `None` means the
    /// run_id is unknown to this session.
    RunState {
        run_id: RunId,
        reply: oneshot::Sender<Result<Option<ReconciledRunState>, String>>,
    },
    /// Attach a control-stream subscriber from `from`. The actor runs the small
    /// registration and durable-head barrier step and spawns the off-actor replay task; `reply`
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
        /// Internal cache maintenance; never set by the public `session.warm` route.
        idle_auto: bool,
        reply: oneshot::Sender<WarmOutcome>,
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
    CompactionV2 {
        caller: Option<String>,
        method: String,
        params: Box<serde_json::value::RawValue>,
        reply: oneshot::Sender<
            Result<
                Box<serde_json::value::RawValue>,
                broca_core::compaction_v2::protocol::wire::Refusal,
            >,
        >,
    },
    /// Test-only: panic inside the actor task (exercises `ActorExitGuard`).
    #[cfg(test)]
    PanicInActor,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ImportOutcome {
    SessionMoving { cut_id: String },
    SessionMoved { holder: Option<String> },
    Imported(broca_core::ImportOutcome),
    Busy,
    Reopen,
    Error(broca_core::ImportError),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SendOutcome {
    /// A send asks the session store for `feature`, which it doesn't serve: refused
    /// `not_served_on_new_storage`, permanent, with nothing written.
    NotServedOnNewStorage {
        feature: &'static str,
    },
    CredentialsUnavailable,
    CredentialAmbiguous {
        credential: String,
        candidate_ids: Vec<String>,
    },
    RouteDecisionFixed,
    AccountRouteFixed,
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: Option<String>,
    },
    RetentionInvalid(crate::retention::InvalidRetention),
    SessionClassInvalid(String),
    InputTokenBudgetInvalid(String),
    ServiceTierInvalid {
        reason: &'static str,
        message: String,
    },
    ContextCeiling(broca_protocol::ContextCeilingDetail),
    /// The send's `structured_output` was refused before anything was written; `field` is the
    /// offending member path the role contract names.
    StructuredOutputInvalid {
        field: String,
    },
    VariantUnsupported(broca_core::run::VariantRefusal),
    ToolAliasInvalid(broca_core::run::ToolAliasInvalid),
    Upload(broca_core::media_uploads::UploadError),
    MediaTooLarge(cortexkit_role_llm_runner::errors::MediaTooLargeDetail),
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
    /// The send was refused because the module is stopping; it did nothing (see
    /// [`crate::serve::ServeError::ModuleStopping`]).
    ModuleStopping(crate::serve::StoppingAt),
    RetainedSessionTooLarge {
        max_bytes: u64,
    },
    SessionTooLarge {
        max_bytes: u64,
    },
    /// Admission failed before any durable write because a dependency is temporarily unavailable.
    Transient(String),
    /// The routing module refused to choose a provider account; nothing was written.
    AccountSelectionRefused(broca_core::AccountSelectionRefusal),
    /// The routing module gave no usable account decision; nothing was written. Transient.
    RoutingUnavailable(broca_core::RoutingUnavailable),
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

/// How a `session.warm` ended.
#[derive(Debug, Clone, PartialEq)]
pub enum WarmOutcome {
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: Option<String>,
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
    ScopeRefused(broca_core::ScopeRefusal),
    Error(String),
    SessionMoving { cut_id: String },
    SessionMoved { holder: Option<String> },
    Ack,
    NotActive,
    Reopen,
}

#[derive(Debug, Clone, Copy)]
pub enum CancelAuthority {
    Owner,
    Sender,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RetractOutcome {
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: Option<String>,
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

/// The actor accepted a `session.refresh` and made it durable before its reply.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionRefreshOutcome {
    Accepted {
        run_in_progress: bool,
        applies_from_next_run: bool,
    },
    Refused(crate::serve::SessionChangeError),
    Reopen,
    Error(String),
}

/// The actor changed when the pending plan takes effect.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionRefreshPolicyOutcome {
    Changed,
    Refused(crate::serve::SessionChangeError),
    Reopen,
    Error(String),
}

/// The actor flushed the frozen prefix.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionFlushPrefixOutcome {
    Flushed { pending_generation: Option<u64> },
    Reopen,
    Error(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LineageStatus {
    Idle,
    Active { run_id: RunId, step: u64 },
    Closing,
}

/// How a send with a legacy string `delivery` is admitted.
pub(crate) enum StringSender {
    /// The session's owner, or a caller the plan does not name: admitted as before.
    Owner,
    /// A plan-named sender's `queue` or `steer`, admitted and delivered as the list it stands
    /// for.
    AsOrder(broca_protocol::DeliveryOrder),
    /// A plan-named sender's granted `interrupt`, continuing under this frozen principal.
    Interrupt(broca_protocol::BindPrincipal),
}
