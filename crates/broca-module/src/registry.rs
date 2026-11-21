//! The session registry: process-global admission for one actor per session.
//!
//! Every `session.*`/`run.*` op for a session is routed through this registry so a
//! teardown can never race a concurrent op. The registry mutex is held only for the fast
//! state check and slot transition, never across the slow lease acquire and session load. It
//! admits exactly one `Opening` per session (concurrent opens share it), tags each `Open` slot
//! with a generation (so a stale handle a racing op grabbed can be detected), and serializes
//! Open↔Closing↔Closed. Actor teardown stops subscribers and closes the session before
//! releasing the lease, so a replacement actor cannot overlap the old writer or routes.
//!
//! See `docs/session-actors.md` §2.

use broca_diagnostics::JobKind;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_protocol::{BindIdentity, RunId};
use tokio::sync::{broadcast, mpsc};

fn spawn_blocking<F, T>(kind: JobKind, work: F) -> tokio::task::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    broca_diagnostics::spawn_blocking(kind, work)
}

const CLOSE_REOPEN_MAX_RETRIES: usize = 3;
const CLOSE_REOPEN_BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);

static SESSION_READS: AtomicU64 = AtomicU64::new(0);
static ACTOR_OPENS: AtomicU64 = AtomicU64::new(0);

tokio::task_local! {
    static SERVED_REQUEST_ACTOR_OPENED: Arc<AtomicBool>;
}

/// Run one served operation with a task-local marker for a successful actor open it starts.
///
/// The marker belongs to the opening request, not to a process-wide counter delta: other
/// sessions may open actors concurrently while this request is running.
pub async fn with_request_actor_opened<F: Future>(future: F) -> (F::Output, bool) {
    let opened = Arc::new(AtomicBool::new(false));
    let output = SERVED_REQUEST_ACTOR_OPENED
        .scope(opened.clone(), future)
        .await;
    (output, opened.load(Ordering::Relaxed))
}

/// `session.read` requests served by this process since boot.
///
/// This process-local lifetime count resets on restart; it is not a fleet total. Diff two
/// readings to derive a rate.
pub fn session_reads() -> u64 {
    SESSION_READS.load(Ordering::Relaxed)
}

/// Counts one `session.read` the handler serves.
pub(crate) fn count_session_read() {
    SESSION_READS.fetch_add(1, Ordering::Relaxed);
}

/// Registry actor opening attempts made by this process since boot.
///
/// Existing actors bypass the opening path and do not increment this process-local lifetime
/// count. It resets on restart and is not a fleet total; diff two readings to derive a rate.
pub fn actor_opens() -> u64 {
    ACTOR_OPENS.load(Ordering::Relaxed)
}

/// The tool list a session's latest episode was admitted with, as frozen on its `RunStarted`.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionReadTools {
    /// The run whose `RunStarted` recorded `tools`.
    pub run_id: RunId,
    pub tools: Arc<Vec<broca_protocol::request::Tool>>,
}

/// The session identity a lineage is keyed by — the same triple the lease + WAL + store
/// partition use, so one identity covers the whole session.
pub type LineageKey = BindIdentity;

/// Why opening a lineage failed (surfaced to every coalescing waiter, never swallowed).
#[derive(Debug, Clone)]
pub enum OpenError {
    HistoryRewriting,
    HistoryChanged,
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: Option<String>,
    },
    Expired {
        expired_at_ms: u64,
    },
    RetentionInvalid(crate::retention::InvalidRetention),
    SessionClassInvalid(String),
    /// The session's lease is held by another live writer (another process/host).
    LeaseHeld(String),
    /// The lease store / WAL / replay failed while opening.
    Open(String),
    /// broca is stopping, so it opens no session. Nothing was written for the request.
    ModuleStopping,
    /// A read failure whose typed cause is still available.
    Classified {
        reason: String,
        class: broca_protocol::ErrorClass,
    },
    /// A send creating a head asked for `feature`, which the session store doesn't serve on a
    /// session's creating send yet. A head starts nowhere else, so it is refused, permanently,
    /// before any session is opened or created.
    NotServedOnNewStorage {
        feature: &'static str,
    },
    /// The session doesn't exist: the session store doesn't hold it, and no creating send or
    /// import asked for it. Nothing was created.
    UnknownSession,
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HistoryRewriting => {
                f.write_str("history is being rewritten; retry after its writer finishes recovery")
            }
            Self::HistoryChanged => {
                f.write_str("history changed while the page was read; read the newest page")
            }
            Self::SessionMoving { cut_id } => write!(f, "session_moving {{cutId: {cut_id}}}"),
            Self::SessionMoved {
                holder: Some(holder),
            } => {
                write!(f, "session_moved {{holder: {holder}}}")
            }
            Self::SessionMoved { holder: None } => f.write_str("session_moved"),
            OpenError::Expired { expired_at_ms } => write!(f, "lineage expired at {expired_at_ms}"),
            OpenError::RetentionInvalid(_) => write!(f, "invalid retention"),
            OpenError::SessionClassInvalid(message) => f.write_str(message),
            OpenError::LeaseHeld(s) => write!(f, "session '{s}' held by a live writer"),
            OpenError::Open(s) | OpenError::Classified { reason: s, .. } => {
                write!(f, "open failed: {s}")
            }
            OpenError::ModuleStopping => f.write_str("open failed: module is stopping"),
            OpenError::NotServedOnNewStorage { feature } => {
                f.write_str(&crate::store_actor::unserved_message(feature))
            }
            OpenError::UnknownSession => f.write_str("session does not exist"),
        }
    }
}

impl std::error::Error for OpenError {}

/// A cloneable handle to an open actor: the command channel + the actor's GENERATION (its
/// identity, used to detect a stale handle after a re-open).
#[derive(Clone)]
pub struct ActorHandle {
    pub gen: u64,
    pub(crate) tx: mpsc::Sender<crate::actor::LineageCmd>,
    /// 1 while this session is driving a run, 0 while it is idle. Written by the actor at its
    /// own state transitions and read without touching the actor, so a health probe never
    /// queues behind the command loop.
    ///
    /// This exists because the durable run index cannot answer "is anything running right
    /// now". It CAN hold `active` rows, so a reader gets a confident zero rather than an
    /// obvious refusal -- and that zero is only as current as the last write. An operator
    /// deciding whether a restart will cut somebody's turn needs the present, not a
    /// well-formed reading of the recent past.
    pub(crate) run_in_flight: Arc<AtomicU64>,
    /// The session's first episode, once the actor knows it: set when the actor admits the
    /// session's first episode, or reads its history and finds it. Never changes after.
    pub(crate) first_episode: Arc<std::sync::OnceLock<broca_core::FirstEpisodeV1>>,
}

impl ActorHandle {
    /// Send a command to the actor (awaiting if its channel is full — backpressure on the
    /// command queue). `Err` means the channel is CLOSED (the actor began Closing or
    /// exited), so the caller must re-route through the registry (re-open). The command is
    /// returned in the error so the caller can retry it.
    pub(crate) async fn send(
        &self,
        cmd: crate::actor::LineageCmd,
    ) -> Result<(), Box<crate::actor::LineageCmd>> {
        self.tx.send(cmd).await.map_err(|e| Box::new(e.0))
    }
}

struct OpenGuard {
    inner: Arc<Inner>,
    key: LineageKey,
    ready: broadcast::Sender<Result<ActorHandle, OpenError>>,
    committed: bool,
}

impl OpenGuard {
    fn new(
        inner: Arc<Inner>,
        key: LineageKey,
        ready: broadcast::Sender<Result<ActorHandle, OpenError>>,
    ) -> Self {
        Self {
            inner,
            key,
            ready,
            committed: false,
        }
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for OpenGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut slots = self.inner.slots.lock().unwrap();
        if matches!(slots.get(&self.key), Some(Entry::Opening { .. })) {
            slots.remove(&self.key);
        }
        drop(slots);
        // Wake waiters parked on `ready` with the same fail-closed open error.
        let _ = self.ready.send(Err(OpenError::Open(
            "open cancelled or opener task dropped".into(),
        )));
    }
}

/// One registry slot for a session.
enum Entry {
    /// An open is in flight; waiters await `ready` (a `Result`, so an open FAILURE wakes
    /// them with the error instead of hanging — finding 10).
    Opening {
        gen: u64,
        ready: broadcast::Sender<Result<ActorHandle, OpenError>>,
    },
    /// An actor is live. `gen` is its identity (a finalizer/close only acts on its OWN
    /// generation, so it can't clobber a newer actor — finding 11).
    Open { gen: u64, handle: ActorHandle },
    /// The actor is tearing down; ops await `done` then re-open (the close holds the slot
    /// so a new op can't race a half-torn state — finding 6/8).
    Closing {
        gen: u64,
        done: broadcast::Sender<()>,
    },
    // (a Closed/Failed session is the ABSENT entry — removing the slot.)
}

/// Constructs + spawns the actor for a session (acquire lease → replay → spawn the actor
/// task → return its handle). The registry stays agnostic to the lease/WAL/executor by
/// depending on this seam — the race harness supplies a fake opener (fake lease + fake
/// executor) so the admission concurrency is deterministically testable.
#[async_trait]
pub trait LineageOpener: Send + Sync {
    async fn check_plan_selection(
        &self,
        plan: &serde_json::Value,
    ) -> Result<
        Option<cortexkit_role_llm_runner::head_selection::PlanModel>,
        cortexkit_role_llm_runner::head_selection::PlanModelProblem,
    > {
        cortexkit_role_llm_runner::head_selection::check_plan(
            plan,
            &broca_wire::broca_role_describe(env!("CARGO_PKG_VERSION")).role,
            |_| None,
        )
    }
    /// The context limit a fresh run of `model` would freeze when its send names none
    /// (`RunExecutor::compaction_context_limit`, which says why a send's own limit doesn't
    /// count). An opener that runs nothing answers `Err`.
    async fn compaction_context_limit(
        &self,
        _model: &cortexkit_role_llm_runner::head_selection::PlanModel,
    ) -> Result<Option<u64>, String> {
        Err("this runner can't resolve a model's context limit".into())
    }
    /// Retire routes synchronously when retention deletes a session without an actor.
    fn on_session_deleted(&self, _session: &LineageKey) {}
    fn retire_session_transport(&self, _session: &LineageKey) {}
    fn retire_idle_session_transport(&self, _session: &LineageKey) {}
    fn retire_all_transports(&self) {}

    async fn media_profile(
        &self,
        _request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<Option<Box<broca_provider::media_profile::MediaProfile>>, broca_core::RunError>
    {
        Ok(None)
    }
    /// Read-only access to the derived run index, without acquiring a session writer.
    /// The runtime serving sessions kept in the session store, when this opener has one.
    fn store_runtime(&self) -> Option<Arc<crate::store_open::StoreRuntime>> {
        None
    }
    /// Why a session can't be created in the session store right now although this opener keeps
    /// one: its deployment key hasn't loaded (the first seconds after a start), answered as the
    /// retryable `key_unavailable`. `None` when it can create one, or keeps no new storage.
    fn store_creation_pending(&self) -> Option<OpenError> {
        None
    }
    /// Where model calls are published as step events, when this opener publishes them.
    fn step_events(&self) -> Option<Arc<crate::step_events::StepEvents>> {
        None
    }
    /// The runtime holding session `key`, if the session store keeps it; `None` when no runtime
    /// holds it. A session held by the session store is refused `key_unavailable` while its
    /// deployment key hasn't loaded. A head that can't be checked is an error, never `None`.
    fn store_holding(
        &self,
        _key: &LineageKey,
    ) -> Result<Option<Arc<crate::store_open::StoreRuntime>>, OpenError> {
        Ok(None)
    }
    fn check_move(&self, _key: &LineageKey, _write: bool) -> Result<(), OpenError> {
        Ok(())
    }
    /// Whether this opener serves `key` from the session storage: the session's head exists and
    /// the opener has a store runtime. A head that can't be checked is an error, never `false`.
    fn serves_from_store(&self, _key: &LineageKey) -> Result<bool, OpenError> {
        Ok(false)
    }
    fn elicitation_plane(&self) -> Option<Arc<dyn broca_core::tool_plane::ElicitationPlane>> {
        None
    }
    /// Read-only admission capability. Hookless test openers inherit the fail-closed default;
    /// an opener with a hook executor must report it without acquiring a lease or opening a WAL.
    fn hook_plane_available(&self) -> bool {
        false
    }

    /// Whether this opener's executor has a compaction plane. Defaults to false, the same
    /// fail-closed reading as the hook plane, without acquiring a lease or opening a WAL.
    fn compaction_supported(&self) -> bool {
        false
    }
    fn backup_classes(&self) -> Vec<String> {
        vec!["head".into()]
    }

    /// Open the actor for `key`, registering it under `gen`. The opener gives the actor a
    /// [`RegistryHandle`] (so the actor's close/finalizer can transition its own slot) and
    /// returns the actor's command handle.
    async fn open_with_retention(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
        retention: Option<u64>,
    ) -> Result<ActorHandle, OpenError> {
        if retention.is_some() {
            return Err(OpenError::Open("retention opener is not configured".into()));
        }
        self.open(key, gen, registry).await
    }
    async fn open_with_class(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
        retention: Option<u64>,
        _session_class: Option<String>,
    ) -> Result<ActorHandle, OpenError> {
        self.open_with_retention(key, gen, registry, retention)
            .await
    }
    /// File-backed implementations classify status under the same session lease used
    /// for opening, and must refuse expired retained history without creating a writer.
    async fn open_for_status(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        self.open(key, gen, registry).await
    }

    async fn open(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError>;
}

/// The process-global registry.
#[derive(Clone)]
pub struct LineageRegistry {
    inner: Arc<Inner>,
}

/// Coordinates discovery needed to prove that an unrecorded session is new.
#[async_trait]
pub trait SessionInventoryReadiness: Send + Sync {
    fn start(&self);
    async fn wait(&self) -> Result<(), broca_store::session_address::SessionAddressError>;
}

mod idle_warm;
pub(crate) mod media_head;
pub mod store_move;
pub mod store_restore;

/// A failed durability boundary. Historical events survive successful session recovery.
#[derive(Clone, Debug)]
pub struct SealFailure {
    pub session: String,
    pub generation: u64,
    pub stage: SealFailureStage,
    pub timestamp: std::time::SystemTime,
    pub error: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SealFailureStage {
    Seal,
    SealDeadline,
    FinalSync,
    WriterShutdown,
}

#[derive(Default)]
struct SealState {
    failures: Vec<SealFailure>,
    recovery_debt: HashMap<LineageKey, SealFailure>,
}

/// Snapshot at the final stop's return point, not a later sampling of the actor slots.
#[derive(Clone, Debug)]
pub struct ShutdownReport {
    pub complete: bool,
    pub cause: &'static str,
    pub elapsed: std::time::Duration,
    pub budget: std::time::Duration,
    pub remaining_sessions: Vec<(String, &'static str)>,
    pub current_failures: Vec<SealFailure>,
    pub lifetime_failures: usize,
    pub recovery_debt: Vec<SealFailure>,
    pub latest_failure: Option<SealFailure>,
}

impl std::fmt::Display for ShutdownReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}; elapsed={:?}; budget={:?}; remaining_sessions={:?}; current_failures={:?}; lifetime_failures={}; recovery_debt_sessions={}; recovery_debt={:?}; latest_failure={:?}",
            self.cause,
            self.elapsed,
            self.budget,
            self.remaining_sessions,
            self.current_failures,
            self.lifetime_failures,
            self.recovery_debt.len(),
            self.recovery_debt,
            self.latest_failure,
        )
    }
}

pub struct MoveSessionGuard {
    /// Actors retired by a mutating cut: each closed its writer, handed over the
    /// still-held session lease (absent if the writer could not close cleanly), and left
    /// its registry slot `Closing` under a close token this guard now owns. Dropping the
    /// guard releases each lease and then commits each slot closed, all before the cut
    /// gates release, so the next cut lock or opener finds the slot already absent
    /// instead of racing the retired actor's exit.
    retired: Vec<RetiredActor>,
    gates: Vec<tokio::sync::OwnedMutexGuard<()>>,
    actors: Vec<tokio::sync::oneshot::Sender<()>>,
    pub busy: Vec<(LineageKey, Option<String>)>,
}

/// One actor generation a mutating cut retired; see [`MoveSessionGuard`].
struct RetiredActor {
    key: LineageKey,
    tenure: Option<crate::actor::CutTenure>,
    close: CloseToken,
}

impl Drop for MoveSessionGuard {
    fn drop(&mut self) {
        // Release the lease before the slot turns absent, and turn the slot absent
        // before the gates (dropped with the fields below) let anyone else in.
        for retired in self.retired.drain(..) {
            drop(retired.tenure);
            retired.close.commit();
        }
    }
}

impl MoveSessionGuard {
    /// Whether this guard holds the session lease handed over by `key`'s closed actor.
    pub fn holds_tenure(&self, key: &LineageKey) -> bool {
        self.retired
            .iter()
            .any(|retired| &retired.key == key && retired.tenure.is_some())
    }
}

/// How a move holds an open session actor while it works on that session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutLockMode {
    /// The move only reads. Open actors pause and later resume with their writer.
    Inspect,
    /// The move may publish or remove move markers for the session. An idle actor that
    /// owns its tenure through its writer closes that writer and hands the still-held
    /// lease to the guard; the guard commits its slot closed when released. Other actors
    /// pause as for `Inspect`; a busy actor refuses without changing its writer.
    Mutate,
    /// A restore from backup replaces the session (`fence_for_restore`): any idle actor closes,
    /// tearing down its subscribers and joining its writer, and hands its session lease to the
    /// guard, whether its writer or the actor itself held it. A busy actor refuses without
    /// changing its writer.
    Retire,
}

type ToolRouteHealth = dyn Fn(&[LineageKey]) -> serde_json::Value + Send + Sync;

/// What creates a session in the session store, which decides the class of root it is created in
/// (`broca_wal::placement`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreCreation {
    /// A send: its session is durable when its `session_class` is one of `backup_classes`.
    Send,
    /// A file import, which only ever brings a long-lived agent head onto broca: its session is
    /// durable whatever its label.
    Import,
    /// A send labelled [`HEAD_SESSION_CLASS`], placed as a send is. A head starts only in the
    /// session store; if it can't be created there, it is refused.
    Head,
}

/// The session label of an agent head. A session a send with this label creates starts in the
/// new storage whenever the module has one (see [`StoreCreation::Head`]).
pub const HEAD_SESSION_CLASS: &str = "head";

/// What the `route.bind` checks know about a session's ownership.
// Built once per bind check and consumed at once, so the size gap between variants costs
// nothing worth a heap allocation.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum BindOwnership {
    /// Read from the session's history, or authoritatively empty: no history at all, or the
    /// session expired.
    Known(broca_core::SessionOwnershipV1),
    /// This registry has no read-only history source, so it has nothing to check a bind
    /// against (the same answer a handler without history gives).
    NoHistorySource,
    /// Ownership could not be established: the validated read failed, or its coverage was
    /// unproven and decoding the whole log failed too. Callers must not treat this as a
    /// session without an owner.
    Unreadable(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotCounts {
    pub open: usize,
    pub opening: usize,
    pub closing: usize,
    /// Sessions driving a run at this instant.
    ///
    /// Distinct from `open`, and the distinction is the whole point: a slot stays Open while its
    /// session sits idle and closes on a TTL rather than when work finishes, so `open` answers
    /// "how many sessions exist" and this answers "is anything running". An operator deciding
    /// whether a restart will cut somebody's turn needs the second, and reading the first instead
    /// gives a plausible number about a different question.
    pub runs_in_flight: usize,
    /// System One batch calls (`classify.run`) running at this instant. They hold no session,
    /// so they are counted beside `runs_in_flight` rather than in it, and the stop's grace
    /// waits for them only as long as it waits for runs: a batch never extends the grace.
    pub batches_in_flight: usize,
}

/// A handle the actor uses to transition ITS OWN slot during close/finalize. Cloneable,
/// cheap; it never holds the registry lock across an await.
#[derive(Clone)]
pub struct RegistryHandle {
    inner: Arc<Inner>,
}

impl LineageRegistry {
    /// Route the send with its admission-only caller affinity and its informational title.
    #[allow(clippy::too_many_arguments)]
    pub async fn route_submission_with_affinity(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
        request: broca_protocol::request::SubmissionRequest,
        kind: crate::executor::RunKind,
        send_key: crate::actor::SendKey,
        skip_materialization: bool,
        delivery: broca_protocol::Delivery,
        plan: Option<crate::actor::SendPlan>,
        conversation_key: Option<String>,
        title: Option<String>,
        stop_sequences_named: bool,
    ) -> Result<crate::actor::SendOutcome, OpenError> {
        self.route_submission_with_retention(
            key,
            run_id,
            request,
            kind,
            send_key,
            skip_materialization,
            delivery,
            plan,
            conversation_key,
            title,
            None,
            stop_sequences_named,
        )
        .await
    }

    /// Route a send with its delivery mode and the fetch plan it carried, which the serve layer
    /// already parsed and, for a session with no episode, admitted into a manifest.
    #[allow(clippy::too_many_arguments)]
    pub async fn route_submission_with_plan(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
        request: broca_protocol::request::SubmissionRequest,
        kind: crate::executor::RunKind,
        send_key: crate::actor::SendKey,
        skip_materialization: bool,
        delivery: broca_protocol::Delivery,
        plan: Option<crate::actor::SendPlan>,
    ) -> Result<crate::actor::SendOutcome, OpenError> {
        self.route_submission_with_affinity(
            key,
            run_id,
            request,
            kind,
            send_key,
            skip_materialization,
            delivery,
            plan,
            None,
            None,
            false,
        )
        .await
    }

    /// Route a send with its delivery mode: queue it behind an active run (the default), steer
    /// it into the running turn, or interrupt that turn with it.
    #[allow(clippy::too_many_arguments)]
    pub async fn route_submission_with_delivery(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
        request: broca_protocol::request::SubmissionRequest,
        kind: crate::executor::RunKind,
        send_key: crate::actor::SendKey,
        skip_materialization: bool,
        delivery: broca_protocol::Delivery,
    ) -> Result<crate::actor::SendOutcome, OpenError> {
        self.route_submission_with_plan(
            key,
            run_id,
            request,
            kind,
            send_key,
            skip_materialization,
            delivery,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn route_submission_with_class(
        &self,
        key: &LineageKey,
        run_id: RunId,
        request: broca_protocol::request::SubmissionRequest,
        kind: crate::executor::RunKind,
        send_key: crate::actor::SendKey,
        skip_materialization: bool,
        delivery: broca_protocol::Delivery,
        plan: Option<crate::actor::SendPlan>,
        conversation_key: Option<String>,
        title: Option<String>,
        retention: Option<u64>,
        session_class: Option<String>,
        stop_sequences_named: bool,
    ) -> Result<crate::actor::SendOutcome, OpenError> {
        self.route_submission_with_order(
            key,
            run_id,
            request,
            kind,
            send_key,
            skip_materialization,
            delivery,
            plan,
            conversation_key,
            title,
            retention,
            session_class,
            stop_sequences_named,
            None,
        )
        .await
    }

    /// Prevent opening absent actors and pause existing actors, in contract ref
    /// order. The returned guard remains held through the durable cut completion.
    /// This is the inspection mode: no actor closes and the guard takes no tenure.
    pub async fn lock_move_sessions(
        &self,
        sessions: &[LineageKey],
        reject_open: bool,
    ) -> Result<MoveSessionGuard, OpenError> {
        self.lock_move_sessions_with_mode(sessions, reject_open, CutLockMode::Inspect)
            .await
    }

    /// Send a submission with its optional retention. Reject invalid bounds before creating
    /// a WAL; then check the lineage's current policy under its lease to exclude racing sends.
    #[allow(clippy::too_many_arguments)]
    pub async fn route_submission_with_retention(
        &self,
        key: &LineageKey,
        run_id: RunId,
        request: broca_protocol::request::SubmissionRequest,
        kind: crate::executor::RunKind,
        send_key: crate::actor::SendKey,
        skip_materialization: bool,
        delivery: broca_protocol::Delivery,
        plan: Option<crate::actor::SendPlan>,
        conversation_key: Option<String>,
        title: Option<String>,
        retention: Option<u64>,
        stop_sequences_named: bool,
    ) -> Result<crate::actor::SendOutcome, OpenError> {
        self.route_submission_with_class(
            key,
            run_id,
            request,
            kind,
            send_key,
            skip_materialization,
            delivery,
            plan,
            conversation_key,
            title,
            retention,
            None,
            stop_sequences_named,
        )
        .await
    }

    /// Route a send with its non-durable materialization policy. Command retries preserve the
    /// policy; recovery from WAL alone safely defaults to materialization.
    pub async fn route_submission_with_materialization(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
        request: broca_protocol::request::SubmissionRequest,
        kind: crate::executor::RunKind,
        send_key: crate::actor::SendKey,
        skip_materialization: bool,
    ) -> Result<crate::actor::SendOutcome, OpenError> {
        self.route_submission_with_delivery(
            key,
            run_id,
            request,
            kind,
            send_key,
            skip_materialization,
            broca_protocol::Delivery::Queue,
        )
        .await
    }

    /// Route a `run.cancel` to the lineage (same reopen-retry discipline as `send`).
    pub async fn route_cancel(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<crate::actor::CancelOutcome, OpenError> {
        self.route_cancel_at(key, run_id, broca_wal::record::RunCancelAt::Now)
            .await
    }

    /// Supply the durable identity inventory before accepting routed session binds.
    pub fn set_session_address_inventory(&self, inventory: Arc<broca_store::SqliteStore>) {
        self.set_session_address_resolver(inventory);
    }

    pub fn set_session_address_resolver(
        &self,
        inventory: Arc<dyn broca_store::SessionAddressResolver>,
    ) {
        assert!(
            self.inner.session_address_inventory.set(inventory).is_ok(),
            "session address inventory already configured"
        );
    }

    /// Share one repair coordinator across services using this registry.
    pub fn set_session_inventory_readiness(
        &self,
        readiness: Arc<dyn SessionInventoryReadiness>,
    ) -> Arc<dyn SessionInventoryReadiness> {
        self.inner
            .session_inventory_readiness
            .get_or_init(|| readiness)
            .clone()
    }

    /// Resolve a bind's session address through the configured inventory. A scoped bind passes
    /// its stamp as `reservation`, recorded only when the pair is new (see
    /// [`broca_store::SessionAddressResolver::resolve_session_address`]).
    pub async fn resolve_session_address(
        &self,
        bind: &broca_protocol::BindIdentity,
        workdir: Option<&std::path::Path>,
        reservation: Option<&broca_store::session_address::ScopeReservation>,
    ) -> Result<broca_protocol::BindIdentity, broca_store::session_address::SessionAddressError>
    {
        use broca_store::session_address::SessionAddressError;
        let inventory = self
            .inner
            .session_address_inventory
            .get()
            .cloned()
            .ok_or_else(|| {
                SessionAddressError::store_error("session address inventory is not configured")
            })?;
        let bind = bind.clone();
        let workdir = workdir.map(std::path::Path::to_path_buf);
        let reservation = reservation.cloned();
        let resolve = || {
            let inventory = inventory.clone();
            let bind = bind.clone();
            let workdir = workdir.clone();
            let reservation = reservation.clone();
            spawn_blocking(broca_diagnostics::JobKind::StoreWrite, move || {
                inventory.resolve_session_address(&bind, workdir.as_deref(), reservation.as_ref())
            })
        };
        let first = resolve().await.map_err(SessionAddressError::store_error)?;
        use broca_store::session_address::InventoryUnavailable;
        if !matches!(
            first,
            Err(SessionAddressError::Unavailable(
                InventoryUnavailable::IncompleteInventory | InventoryUnavailable::GapMarker
            ))
        ) {
            return first;
        }
        let Some(readiness) = self.inner.session_inventory_readiness.get() else {
            return first;
        };
        let ready = readiness.wait().await;
        let resolved = resolve().await.map_err(SessionAddressError::store_error)?;
        // Another writer can record the pair while discovery runs. Its positive
        // evidence remains usable even if unrelated discovery failed or timed out.
        if resolved.is_ok() || matches!(resolved, Err(SessionAddressError::Ambiguous)) {
            return resolved;
        }
        ready?;
        match resolved {
            Err(SessionAddressError::Unavailable(InventoryUnavailable::IncompleteInventory)) => {
                Err(SessionAddressError::Unavailable(
                    InventoryUnavailable::MigrationFailed(
                        "inventory changed before the new session could be reserved".into(),
                    ),
                ))
            }
            result => result,
        }
    }

    /// The scope (owner and ref) whose scoped `route.bind` reserved `key`'s address, if one did.
    /// `Ok(None)` also when this registry has no session address inventory: no bind can have
    /// reserved an address through it. An unreadable reservation is an error, never `None`, so
    /// a reserved session is not opened to everyone because its row could not be read.
    pub async fn scope_reservation(
        &self,
        key: &LineageKey,
    ) -> Result<Option<broca_core::scope_admission::ScopeReservation>, String> {
        let Some(inventory) = self.inner.session_address_inventory.get().cloned() else {
            return Ok(None);
        };
        let key = key.clone();
        spawn_blocking(JobKind::StoreRead, move || {
            inventory
                .scope_reservation(&key)
                .map_err(|error| format!("cannot read the session's scope reservation: {error}"))
        })
        .await
        .map_err(|error| error.to_string())?
    }

    /// `ownership` with [`broca_core::SessionOwnershipV1::reservation`] filled in from the
    /// inventory while the session has no episode. A session with episodes is returned as it
    /// is, without a read: its history decides who owns it.
    pub async fn with_scope_reservation(
        &self,
        key: &LineageKey,
        mut ownership: broca_core::SessionOwnershipV1,
    ) -> Result<broca_core::SessionOwnershipV1, String> {
        if ownership.episodes == 0 {
            ownership.reservation = self.scope_reservation(key).await?;
        }
        Ok(ownership)
    }

    pub(crate) async fn media_profile(
        &self,
        request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<Option<Box<broca_provider::media_profile::MediaProfile>>, broca_core::RunError>
    {
        self.inner.opener.media_profile(request).await
    }

    pub(crate) async fn check_plan_selection(
        &self,
        plan: &serde_json::Value,
    ) -> Result<
        Option<cortexkit_role_llm_runner::head_selection::PlanModel>,
        cortexkit_role_llm_runner::head_selection::PlanModelProblem,
    > {
        self.inner.opener.check_plan_selection(plan).await
    }

    /// The context limit a fresh run of `model` would freeze when its send names none, from
    /// the opener (`LineageOpener::compaction_context_limit`).
    pub(crate) async fn compaction_context_limit(
        &self,
        model: &cortexkit_role_llm_runner::head_selection::PlanModel,
    ) -> Result<Option<u64>, String> {
        self.inner.opener.compaction_context_limit(model).await
    }
    pub fn active_event_sessions(&self) -> Vec<LineageKey> {
        self.inner
            .slots
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(session, entry)| match entry {
                Entry::Open { handle, .. } if handle.run_in_flight.load(Ordering::Relaxed) != 0 => {
                    Some(session.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// The command channels of every open actor, for a report that asks each of them.
    pub(crate) fn open_actor_senders(&self) -> Vec<mpsc::Sender<crate::actor::LineageCmd>> {
        self.inner
            .slots
            .lock()
            .unwrap()
            .values()
            .filter_map(|entry| match entry {
                Entry::Open { handle, .. } => Some(handle.tx.clone()),
                _ => None,
            })
            .collect()
    }

    /// In-memory actor identities for route ownership diagnostics; never included in health JSON.
    pub fn open_sessions(&self) -> Vec<LineageKey> {
        self.inner
            .slots
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(key, entry)| match entry {
                Entry::Open { .. } => Some(key.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn set_tool_route_health(&self, health: Arc<ToolRouteHealth>) {
        let _ = self.inner.tool_route_health.set(health);
    }

    pub fn tool_routes_health(&self) -> serde_json::Value {
        let open = self.open_sessions();
        let mut health = self.inner.tool_route_health.get().map_or_else(
            || {
                serde_json::json!({
                    "open_session_actors": open.len(), "cached_routes": 0,
                    "cached_routes_per_provider": {}, "oldest_route_idle_secs": null,
                    "routes_for_closed_sessions": 0, "goodbye_failures_since_boot": 0,
                    "pending_goodbyes": 0,
                    "goodbye_retry_overflows_since_boot": 0,
                    "goodbye_giveups_since_boot": 0,
                })
            },
            |health| health(&open),
        );
        let holds = self.inner.actor_idle_holds.lock().unwrap();
        let mut reasons = std::collections::BTreeMap::<&str, usize>::new();
        for key in &open {
            if let Some((_, held)) = holds.get(key).filter(|(_, held)| !held.is_empty()) {
                for reason in held {
                    *reasons.entry(reason).or_default() += 1;
                }
            } else {
                *reasons.entry("idle_ttl").or_default() += 1;
            }
        }
        health["open_actor_reasons"] = serde_json::json!(reasons);
        health
    }
    pub async fn route_run_event(
        &self,
        session: &LineageKey,
        event_key: &str,
    ) -> Result<broca_wire::RunEventGetResponse, crate::serve::ServeError> {
        let store = self.inner.run_index.clone();
        if store.is_none() {
            return Ok(broca_wire::RunEventGetResponse::Absent);
        }
        let event_key = event_key.to_owned();
        let key = event_key.clone();
        let row = spawn_blocking(broca_diagnostics::JobKind::StoreRead, move || {
            store
                .ok_or_else(|| "run-event reader unavailable".to_string())?
                .read_run_event(&key)
        })
        .await
        .map_err(|e| crate::serve::ServeError::Open(e.to_string(), None))?
        .map_err(|e| crate::serve::ServeError::Open(e, None))?;
        match row {
            Some((owner, bytes)) if &owner == session => {
                Ok(broca_wire::RunEventGetResponse::Found {
                    event_key,
                    jcs: String::from_utf8(bytes)
                        .map_err(|e| crate::serve::ServeError::Open(e.to_string(), None))?,
                })
            }
            _ => Ok(broca_wire::RunEventGetResponse::Absent),
        }
    }
    fn cut_gate(&self, key: &LineageKey) -> Arc<tokio::sync::Mutex<()>> {
        let reference = broca_core::session_refs::legacy_session_ref(
            &key.harness,
            &key.project_root.to_string_lossy(),
            &key.session,
        );
        let mut gates = self.inner.cut_gates.lock().unwrap();
        gates.retain(|_, gate| gate.strong_count() != 0);
        let gate = gates
            .get(&reference)
            .and_then(std::sync::Weak::upgrade)
            .unwrap_or_else(|| Arc::new(tokio::sync::Mutex::new(())));
        gates.insert(reference, Arc::downgrade(&gate));
        gate
    }

    pub(crate) async fn lock_session_change(
        &self,
        key: &LineageKey,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let gate = {
            let mut gates = self.inner.change_gates.lock().unwrap();
            gates.retain(|_, gate| gate.strong_count() != 0);
            let gate = gates
                .get(key)
                .and_then(std::sync::Weak::upgrade)
                .unwrap_or_else(|| Arc::new(tokio::sync::Mutex::new(())));
            gates.insert(key.clone(), Arc::downgrade(&gate));
            gate
        };
        gate.lock_owned().await
    }

    /// Like [`Self::lock_move_sessions`], choosing how open actors are held. In
    /// [`CutLockMode::Mutate`] the guard may receive closed actors' session leases.
    pub async fn lock_move_sessions_with_mode(
        &self,
        sessions: &[LineageKey],
        reject_open: bool,
        mode: CutLockMode,
    ) -> Result<MoveSessionGuard, OpenError> {
        let mut sorted = sessions.to_vec();
        sorted.sort_by_key(|key| {
            broca_core::session_refs::legacy_session_ref(
                &key.harness,
                &key.project_root.to_string_lossy(),
                &key.session,
            )
        });
        sorted.dedup_by(|a, b| {
            broca_core::session_refs::legacy_session_ref(
                &a.harness,
                &a.project_root.to_string_lossy(),
                &a.session,
            ) == broca_core::session_refs::legacy_session_ref(
                &b.harness,
                &b.project_root.to_string_lossy(),
                &b.session,
            )
        });
        let mut guard = MoveSessionGuard {
            retired: Vec::new(),
            gates: Vec::new(),
            actors: Vec::new(),
            busy: Vec::new(),
        };
        for key in sorted {
            guard.gates.push(self.cut_gate(&key).lock_owned().await);
            if matches!(mode, CutLockMode::Mutate) {
                self.handle().stop_idle_warm(&key, "released_or_moving");
            }
            let handle = {
                let slots = self.inner.slots.lock().unwrap();
                if let Some((bound, _)) = slots.get_key_value(&key) {
                    if broca_core::session_refs::legacy_session_ref(
                        &bound.harness,
                        &bound.project_root.to_string_lossy(),
                        &bound.session,
                    ) != broca_core::session_refs::legacy_session_ref(
                        &key.harness,
                        &key.project_root.to_string_lossy(),
                        &key.session,
                    ) {
                        return Err(OpenError::Open(
                            "session actor belongs to a different carried bind triple".into(),
                        ));
                    }
                }
                match slots.get(&key) {
                    Some(Entry::Open { handle, .. }) => Some(handle.clone()),
                    Some(Entry::Opening { .. } | Entry::Closing { .. }) => {
                        return Err(OpenError::Open("session is opening or closing".into()))
                    }
                    None => None,
                }
            };
            if let Some(handle) = handle {
                if reject_open {
                    return Err(OpenError::Open("busy: session has an open actor".into()));
                }
                let (reply, wait) = tokio::sync::oneshot::channel();
                handle
                    .send(crate::actor::LineageCmd::CutLock { reply, mode })
                    .await
                    .map_err(|_| OpenError::Open("actor closed during cut lock".into()))?;
                let crate::actor::CutLockReply {
                    busy,
                    release,
                    retired,
                } = wait
                    .await
                    .map_err(|_| OpenError::Open("actor closed during cut lock".into()))?;
                guard.busy.push((key.clone(), busy));
                guard.actors.push(release);
                if let Some(crate::actor::RetiredForCut { tenure, close }) = retired {
                    // Only a mutating cut may retire an actor or take its session lease.
                    debug_assert_ne!(mode, CutLockMode::Inspect);
                    guard.retired.push(RetiredActor {
                        key: key.clone(),
                        tenure,
                        close,
                    });
                }
            }
            self.inner.opener.retire_idle_session_transport(&key);
        }
        Ok(guard)
    }
    pub fn check_move(&self, key: &LineageKey, write: bool) -> Result<(), OpenError> {
        // A restore holding a session the store doesn't serve yet refuses it until the restore
        // publishes it; the store answers for every session it serves. Both are known in
        // memory, so this check reads nothing.
        if !self.inner.opener.serves_from_store(key)? {
            if let Some(refusal) = self.inner.restore_fences.refusal(key) {
                return Err(refusal);
            }
        }
        self.inner.opener.check_move(key, write)
    }

    pub fn compaction_supported(&self) -> bool {
        self.inner.opener.compaction_supported()
    }

    /// What serves `session.fork_*` on heads kept in the session store, dated by retention's
    /// clock. Refuses on a module without the session store, which serves no fork.
    pub fn fork_manager(
        &self,
    ) -> Result<Arc<crate::fork_answer::ForkManager>, crate::fork_answer::ForkError> {
        if let Some(forks) = self.inner.forks.get() {
            return Ok(Arc::clone(forks));
        }
        let runtime = self.store_runtime().ok_or_else(|| {
            crate::fork_answer::ForkError::Unsupported("this module keeps no new storage".into())
        })?;
        // Retention's clock when the daemon installed it; else the clock it would install, read
        // from the same state root.
        let clock = match self.inner.retention.get() {
            Some(service) => service.clock.clone(),
            None => broca_core::Clock::for_state_root(runtime.state_root()).unwrap_or_default(),
        };
        Ok(Arc::clone(self.inner.forks.get_or_init(|| {
            Arc::new(
                crate::fork_answer::ForkManager::new(runtime, clock)
                    .with_step_events(self.inner.opener.step_events()),
            )
        })))
    }

    /// The runtime serving sessions kept in the session store, for reads that take no lease.
    pub fn store_runtime(&self) -> Option<Arc<crate::store_open::StoreRuntime>> {
        self.inner.opener.store_runtime()
    }

    /// Where a send's attachments are uploaded: beside the sessions, under the state root.
    pub(crate) fn upload_store(&self) -> Option<broca_core::media_uploads::Uploads> {
        self.store_runtime()
            .map(|store| broca_core::media_uploads::Uploads::at(store.state_root()))
    }
    pub fn backup_classes(&self) -> Vec<String> {
        self.inner.opener.backup_classes()
    }

    /// Why a session can't be created in the session store right now, when it can't: the
    /// retryable `key_unavailable` while its deployment key still loads.
    pub(crate) fn store_creation_pending(&self) -> Option<OpenError> {
        self.inner.opener.store_creation_pending()
    }

    pub(crate) fn store_can_create(&self) -> bool {
        self.inner
            .opener
            .store_runtime()
            .is_some_and(|runtime| runtime.can_create())
    }

    pub fn new(opener: Arc<dyn LineageOpener>) -> Self {
        Self::new_with_run_index(opener, None)
    }

    /// Build a registry that reads published run-event bodies (`run.event.get`) from
    /// `run_index`, the outbox the run-event publisher writes.
    pub fn new_with_run_index(
        opener: Arc<dyn LineageOpener>,
        run_index: Option<Arc<dyn broca_core::RunEventBodies>>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                session_address_inventory: std::sync::OnceLock::new(),
                session_inventory_readiness: std::sync::OnceLock::new(),
                tool_route_health: std::sync::OnceLock::new(),
                actor_idle_holds: Mutex::new(HashMap::new()),
                waiting_subscribers: Mutex::new(HashMap::new()),
                idle_warms: Mutex::new(HashMap::new()),
                idle_warm_stops: Mutex::new(HashSet::new()),
                batches_in_flight: Arc::new(AtomicUsize::new(0)),
                retention: std::sync::OnceLock::new(),
                forks: std::sync::OnceLock::new(),
                owners: Mutex::new(HashMap::new()),
                restore_fences: Default::default(),
                store_restorer: std::sync::OnceLock::new(),
                #[cfg(test)]
                close_started: tokio::sync::watch::channel(0).0,
                plan_admitter: std::sync::OnceLock::new(),
                slots: Mutex::new(HashMap::new()),
                cut_gates: Mutex::new(HashMap::new()),
                change_gates: Mutex::new(HashMap::new()),
                next_gen: AtomicU64::new(1),
                store_eligible: Mutex::new(HashMap::new()),
                opener,
                run_index,
                stopping: tokio_util::sync::CancellationToken::new(),
                seal: tokio_util::sync::CancellationToken::new(),
                seal_state: Mutex::new(SealState::default()),
                shutdown_report: Mutex::new(None),
                restart_resume_ready: AtomicBool::new(false),
                cache: crate::cache::CacheTable::default(),
            }),
        }
    }

    /// Install what reads a frozen config's prompt-cache declaration. Set once, before serving;
    /// only the first installation takes effect.
    pub fn set_cache_declarer(&self, declarer: Arc<dyn crate::cache::CacheDeclarer>) {
        self.inner.cache.set_declarer(declarer);
    }

    /// What `config` declares about its prompt cache, when a declarer is installed and can read
    /// it.
    pub fn cache_declaration(
        &self,
        config: &broca_wal::RunConfig,
    ) -> Option<crate::cache::CacheDeclaration> {
        self.inner.cache.declare(config)
    }

    /// The session's prompt-cache clock, from memory: `None` when this process has sent no
    /// request of the session.
    pub fn cache_clock_reading(&self, key: &LineageKey) -> Option<broca_core::CacheClockReading> {
        self.inner.cache.reading(key)
    }

    /// The cache declaration of the episode `run_id`, when it admitted the session in this
    /// process.
    pub fn admitted_cache(
        &self,
        key: &LineageKey,
        run_id: &RunId,
    ) -> Option<crate::cache::CacheDeclaration> {
        self.inner.cache.admitted(key, run_id)
    }

    /// Route a `session.warm` to the lineage (same reopen-retry discipline as `send`).
    pub async fn route_warm(
        &self,
        key: &LineageKey,
        lineage_id: Option<String>,
    ) -> Result<crate::actor::WarmOutcome, OpenError> {
        self.route_warm_mode(key, lineage_id, false).await
    }

    async fn route_warm_mode(
        &self,
        key: &LineageKey,
        lineage_id: Option<String>,
        idle_auto: bool,
    ) -> Result<crate::actor::WarmOutcome, OpenError> {
        use crate::actor::{LineageCmd, WarmOutcome};
        loop {
            if idle_auto && !self.inner.opener.serves_from_store(key)? {
                return Err(OpenError::Open(
                    "idle warming requires session storage".into(),
                ));
            }
            self.check_move(key, true)?;
            self.check_expired(key, None).await?;
            // A session that doesn't exist has sent no model request to warm.
            let handle = match self.actor_for(key).await {
                Err(OpenError::UnknownSession) => {
                    return Ok(WarmOutcome::Refused(broca_wire::WarmRefusal::new(
                        broca_wire::WarmRefusalDetail::NoStepYet,
                    )))
                }
                handle => handle?,
            };
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::Warm {
                lineage_id: lineage_id.clone(),
                idle_auto,
                reply,
            };
            if handle.send(cmd).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(WarmOutcome::Reopen) => continue,
                Ok(other) => return Ok(other),
                Err(_) => continue,
            }
        }
    }

    /// The compaction-runner wire adapter uses `bind` before acknowledging its
    /// route, then the same session-bound entry point for all three operations.
    pub async fn route_compaction_v2(
        &self,
        key: &LineageKey,
        caller: Option<String>,
        method: String,
        params: Box<serde_json::value::RawValue>,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::protocol::wire::Refusal>
    {
        use broca_core::compaction_v2::protocol::wire::Refusal;
        let internal = |message: String| Refusal::new("internal_error", message, None);
        if let Some(store) = self
            .store_holding(key)
            .map_err(|error| internal(error.to_string()))?
        {
            if method == "session.apply_view" {
                return crate::store_entries::apply_view(self, &store, key, caller, params).await;
            }
            return crate::store_entries::compaction_runner(
                self, &store, key, caller, method, params,
            )
            .await;
        }
        // A session the store doesn't hold exists nowhere, so no compaction provider holds it.
        Err(Refusal::compaction_not_permitted(
            broca_core::compaction_v2::protocol::wire::not_permitted_reasons::SESSION_NOT_HELD,
        ))
    }

    /// Install the admitter for sends carrying a fetch plan. Set once, before serving; only the
    /// first installation takes effect.
    pub fn set_plan_admitter(&self, admitter: Arc<dyn broca_fetch_plan::PlanAdmitter>) {
        let _ = self.inner.plan_admitter.set(admitter);
    }

    /// The installed admitter for sends carrying a fetch plan, or `None` if none was installed.
    pub fn plan_admitter(&self) -> Option<Arc<dyn broca_fetch_plan::PlanAdmitter>> {
        self.inner.plan_admitter.get().cloned()
    }

    /// Query the executor's hook adapter without opening a session actor.
    pub fn hook_plane_available(&self) -> bool {
        self.inner.opener.hook_plane_available()
    }

    /// Read the immutable first sender without opening an actor (which could resume work).
    /// Prefer the materialized admission index; an unprojected or legacy session needs one
    /// durable-WAL fallback. Eviction forgets only the optimization, never the recorded sender.
    pub async fn frozen_sender(
        &self,
        key: &LineageKey,
    ) -> Result<Option<Option<broca_protocol::BindPrincipal>>, OpenError> {
        match self.check_authorization_expired(key).await {
            Err(OpenError::Expired { .. }) => return Ok(None),
            Err(error) => return Err(error),
            Ok(()) => {}
        }
        if let Some(store) = self.store_holding(key)? {
            let ownership = store_session_ownership(self, &store, key).await?;
            return Ok((ownership.episodes > 0).then_some(ownership.first_principal));
        }
        Ok(None)
    }

    /// Cancel/retract ownership from a live reader, a checkpoint plus its durable suffix, or
    /// the complete WAL when no checkpoint is usable. Never open an actor to obtain it.
    pub(crate) async fn owner_ownership(
        &self,
        key: &LineageKey,
    ) -> Result<broca_core::SessionOwnershipV1, OpenError> {
        self.check_authorization_expired(key).await?;
        self.durable_session_ownership(key, false).await
    }

    /// Read the submission's original sender even after delivery or retraction, so retries
    /// retain the same authorization. Missing or ambiguous attribution grants no exception.
    pub(crate) async fn recorded_submission_sender(
        &self,
        key: &LineageKey,
        submission_id: &str,
    ) -> Result<Option<broca_protocol::BindPrincipal>, OpenError> {
        if let Some(store) = self.store_holding(key)? {
            use crate::store_reads::{StoreReadReply, StoreReadRequest};
            return match store
                .read(
                    self,
                    key,
                    StoreReadRequest::AuthorizationSubmissionSender(submission_id.into()),
                )
                .await
                .map_err(crate::store_reads::open_error)?
            {
                None => Ok(None),
                Some(StoreReadReply::AuthorizationSubmissionSender(sender)) => {
                    sender.map_err(crate::store_reads::open_error)
                }
                Some(_) => Err(crate::store_reads::open_error(
                    crate::store_reads::mismatched(),
                )),
            };
        }
        Ok(None)
    }

    /// Cold grant lookup never opens an actor, whose recovery could write history.
    pub(crate) async fn interrupt_cancel_granted(
        &self,
        key: &LineageKey,
        module_id: &str,
    ) -> Result<bool, OpenError> {
        self.check_authorization_expired(key).await?;
        let ownership = self.sender_ownership_for_authorization(key).await?;
        Ok(crate::sender_admission::interrupt_granted(
            &ownership, module_id,
        ))
    }

    /// Reuse an existing actor's admission source without opening or recovering a lineage.
    pub(crate) async fn warm_cancel_authority(
        &self,
        key: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        at: broca_wal::record::RunCancelAt,
    ) -> Result<Option<crate::actor::CancelAuthority>, crate::serve::ServeError> {
        self.check_authorization_expired(key).await?;
        match self.check_move(key, true) {
            // Moving/moved sessions need owner_ownership's full log validation. Cached
            // authority could hide corrupt history; move errors must wait for authorization.
            Err(OpenError::SessionMoving { .. } | OpenError::SessionMoved { .. }) => {
                return Ok(None)
            }
            Err(error) => return Err(error.into()),
            Ok(()) => {}
        }
        let handle = {
            let slots = self.inner.slots.lock().unwrap();
            match slots.get(key) {
                Some(Entry::Open { handle, .. }) => Some(handle.clone()),
                _ => None,
            }
        };
        if let Some(handle) = handle {
            let (reply, rx) = tokio::sync::oneshot::channel();
            if handle
                .send(crate::actor::LineageCmd::SenderPolicy {
                    principal: principal.cloned(),
                    scope: scope.clone(),
                    at,
                    reply,
                })
                .await
                .is_ok()
            {
                if let Ok(result) = rx.await {
                    return result;
                }
            }
        }
        Ok(None)
    }

    pub(crate) async fn resolve_cancel_authority(
        &self,
        key: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        at: broca_wal::record::RunCancelAt,
    ) -> Result<crate::actor::CancelAuthority, crate::serve::ServeError> {
        use crate::{actor::CancelAuthority, serve::ServeError};
        if let Some(authority) = self
            .warm_cancel_authority(key, principal, scope, at)
            .await?
        {
            return Ok(authority);
        }
        let ownership = self.owner_ownership(key).await?;
        let refusal =
            match broca_core::scope_admission::decide_owner_access(&ownership, principal, scope) {
                Ok(()) => return Ok(CancelAuthority::Owner),
                Err(refusal) => refusal,
            };
        if let (
            broca_wal::record::RunCancelAt::Now,
            Some(broca_protocol::BindPrincipal::Reserved { module_id }),
        ) = (at, principal)
        {
            if self.interrupt_cancel_granted(key, module_id).await? {
                return Ok(CancelAuthority::Sender);
            }
        }
        Err(ServeError::ScopeRefused(refusal))
    }

    /// Read existing durable policy without opening an actor or advancing a live writer.
    /// Older policy may be repaired in memory, never published for an unadmitted caller.
    async fn sender_ownership_for_authorization(
        &self,
        key: &LineageKey,
    ) -> Result<broca_core::SessionOwnershipV1, OpenError> {
        if let Some(store) = self.store_holding(key)? {
            return store_session_ownership(self, &store, key).await;
        }
        Ok(broca_core::SessionOwnershipV1::default())
    }

    /// Upload standing includes every current plan-named sender, not just session owners.
    /// Read the frozen first manifest without retaining its contents in owner caches.
    pub(crate) async fn upload_ownership(
        &self,
        key: &LineageKey,
    ) -> Result<broca_core::SessionOwnershipV1, OpenError> {
        self.check_authorization_expired(key).await?;
        self.durable_session_ownership(key, true).await
    }

    async fn durable_session_ownership(
        &self,
        key: &LineageKey,
        _for_bind: bool,
    ) -> Result<broca_core::SessionOwnershipV1, OpenError> {
        match self.check_move(key, true) {
            // A move freezes sends, not reads: who owns a moving session is still read from it.
            Err(OpenError::SessionMoving { .. } | OpenError::SessionMoved { .. }) | Ok(()) => {}
            Err(error) => return Err(error),
        }
        match self.store_holding(key)? {
            Some(store) => store_session_ownership(self, &store, key).await,
            // A session no send created has no run, so no owner yet.
            None => Ok(broca_core::SessionOwnershipV1::default()),
        }
    }

    /// The ownership summary of a session's durable history, read without opening its actor,
    /// for the prechecks that run before a session's actor sees a send or a bind. `None` when
    /// this registry has no read-only source or the history cannot be read. Each caller treats
    /// `None` as unknown: the session's actor makes the same decision authoritatively. The
    /// stamped-bind adoption check must not skip on unknown and uses [`Self::bind_ownership`].
    pub async fn session_ownership_for_bind(
        &self,
        key: &LineageKey,
    ) -> Option<broca_core::SessionOwnershipV1> {
        match self.bind_ownership(key).await {
            BindOwnership::Known(ownership) => Some(ownership),
            BindOwnership::NoHistorySource => None,
            BindOwnership::Unreadable(error) => {
                tracing::warn!(
                    target: "scope",
                    "route.bind ownership unknown, history unreadable: {error}"
                );
                None
            }
        }
    }

    /// The ownership a `route.bind` is checked against. The validated reader answers first,
    /// from the live session, a cached or stored checkpoint plus its WAL suffix, or the whole
    /// WAL. When that reader cannot prove it covered the whole history, the whole log is
    /// decoded instead, as stores without range proofs serve it. A validated-read error, or a
    /// failed whole-log fallback, is [`BindOwnership::Unreadable`]; a validated-read error is
    /// never retried against any other source.
    pub async fn bind_ownership(&self, key: &LineageKey) -> BindOwnership {
        let ownership = match self.bind_history_ownership(key).await {
            BindOwnership::Known(ownership) => ownership,
            other => return other,
        };
        // A session with no episode yet belongs to whoever reserved its address under a scope,
        // if anyone did; every check that reads this ownership applies that reservation.
        match self.with_scope_reservation(key, ownership).await {
            Ok(ownership) => BindOwnership::Known(ownership),
            Err(error) => BindOwnership::Unreadable(error),
        }
    }

    async fn bind_history_ownership(&self, key: &LineageKey) -> BindOwnership {
        if matches!(
            self.check_expired(key, None).await,
            Err(OpenError::Expired { .. })
        ) {
            return BindOwnership::Known(broca_core::SessionOwnershipV1::default());
        }
        // A session kept in the session store has no WAL to read. Its ownership comes from the
        // owner summary the stored session keeps (`Operations::owner`), which records the same
        // facts `durable_session_ownership` gathers from a WAL.
        match self.store_holding(key) {
            Ok(Some(store)) => store_bind_ownership(self, &store, key).await,
            // A session no send created has no run, so no owner yet.
            Ok(None) => BindOwnership::Known(broca_core::SessionOwnershipV1::default()),
            Err(error) => BindOwnership::Unreadable(error.to_string()),
        }
    }

    pub(crate) fn handle(&self) -> RegistryHandle {
        RegistryHandle {
            inner: self.inner.clone(),
        }
    }
}

/// The ownership of a session kept in the session store, from a read-only view of it. A session
/// whose creation never committed holds no run, so it has no owner yet.
async fn store_bind_ownership(
    registry: &LineageRegistry,
    store: &crate::store_open::StoreRuntime,
    key: &LineageKey,
) -> BindOwnership {
    match store_session_ownership(registry, store, key).await {
        Ok(ownership) => BindOwnership::Known(ownership),
        Err(error) => BindOwnership::Unreadable(error.to_string()),
    }
}

/// The ownership of a session kept in the session store, as every authorization path reads it:
/// the owner summary the stored session keeps (`Operations::owner`), with the first run's tools
/// and plan read back from the deployment's setup parts. It records the same facts
/// `durable_session_ownership` gathers from a WAL. A session whose creation never committed
/// holds no run, so it has no owner yet.
async fn store_session_ownership(
    registry: &LineageRegistry,
    store: &crate::store_open::StoreRuntime,
    key: &LineageKey,
) -> Result<broca_core::SessionOwnershipV1, OpenError> {
    use crate::store_reads::{StoreReadReply, StoreReadRequest};
    let ownership = match store
        .read(registry, key, StoreReadRequest::AuthorizationOwnership)
        .await
        .map_err(crate::store_reads::open_error)?
    {
        None => broca_core::SessionOwnershipV1::default(),
        Some(StoreReadReply::AuthorizationOwnership(ownership)) => {
            ownership.map_err(OpenError::Open)?
        }
        Some(_) => {
            return Err(crate::store_reads::open_error(
                crate::store_reads::mismatched(),
            ))
        }
    };
    registry.note_owner(key, &ownership);
    Ok(ownership)
}

/// How many sessions' owners the reader census keeps before it starts over.
const OWNERS_CACHED: usize = 4096;

/// A session's owners as its history names them: the principal of its first send, and the
/// owner of the scope it runs under.
#[derive(Clone)]
struct OwnerFacts {
    first: Option<broca_protocol::BindPrincipal>,
    scope_owner: Option<broca_protocol::BindPrincipal>,
}

impl LineageRegistry {
    /// Keeps who owns `key`, as an ownership read just found it, for the reader census. A
    /// session no run started has no owner yet and is forgotten.
    fn note_owner(&self, key: &LineageKey, ownership: &broca_core::SessionOwnershipV1) {
        let mut owners = self.inner.owners.lock().unwrap();
        if ownership.episodes == 0 {
            owners.remove(key);
            return;
        }
        if owners.len() >= OWNERS_CACHED && !owners.contains_key(key) {
            owners.clear();
        }
        owners.insert(
            key.clone(),
            OwnerFacts {
                first: ownership.first_principal.clone(),
                scope_owner: ownership.scope.as_ref().map(|scope| scope.owner.clone()),
            },
        );
    }

    /// Whether `principal` owns `key`, from the last ownership read of it: the principal of its
    /// first send, or its scope's owner. `None` when no read has been kept. It reads no storage.
    pub(crate) fn cached_session_owner(
        &self,
        key: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Option<bool> {
        let owners = self.inner.owners.lock().unwrap();
        let facts = owners.get(key)?;
        Some(principal.is_some_and(|principal| {
            facts.first.as_ref() == Some(principal) || facts.scope_owner.as_ref() == Some(principal)
        }))
    }

    /// Resolve `key` to a live actor handle, opening (or awaiting an in-flight open, or
    /// awaiting a close then re-opening) as needed. Exactly one actor per session.
    pub async fn actor_for(&self, key: &LineageKey) -> Result<ActorHandle, OpenError> {
        self.actor_for_retention(key, None).await
    }

    async fn actor_for_retention(
        &self,
        key: &LineageKey,
        retention: Option<u64>,
    ) -> Result<ActorHandle, OpenError> {
        self.actor_for_mode(key, retention, None, false).await
    }

    pub(crate) async fn actor_for_mode(
        &self,
        key: &LineageKey,
        retention: Option<u64>,
        session_class: Option<String>,
        status: bool,
    ) -> Result<ActorHandle, OpenError> {
        self.check_move(key, true)?;
        let mut close_retries = 0;
        loop {
            if self.inner.stopping.is_cancelled() {
                return Err(OpenError::ModuleStopping);
            }
            // Fast path under the lock: classify the slot + claim a transition. The slow
            // open/await happens AFTER the lock is dropped.
            enum Action {
                Open(broadcast::Sender<Result<ActorHandle, OpenError>>, u64),
                Wait(broadcast::Receiver<Result<ActorHandle, OpenError>>),
                WaitClose(broadcast::Receiver<()>),
                Have(ActorHandle),
            }
            let cut_gate = self.cut_gate(key).lock_owned().await;
            self.check_move(key, true)?;
            let action = {
                let mut slots = self.inner.slots.lock().unwrap();
                match slots.get(key) {
                    None => {
                        let (tx, rx) = broadcast::channel(1);
                        let _ = rx; // a fresh subscribe below gets the value
                        let gen = self.inner.next_gen.fetch_add(1, Ordering::SeqCst);
                        slots.insert(
                            key.clone(),
                            Entry::Opening {
                                gen,
                                ready: tx.clone(),
                            },
                        );
                        Action::Open(tx, gen)
                    }
                    Some(Entry::Opening { ready, .. }) => Action::Wait(ready.subscribe()),
                    Some(Entry::Open { handle, .. }) => Action::Have(handle.clone()),
                    Some(Entry::Closing { done, .. }) => Action::WaitClose(done.subscribe()),
                }
            };
            drop(cut_gate);
            match action {
                Action::Have(handle) => return Ok(handle),
                Action::Wait(mut rx) => {
                    return match rx.recv().await {
                        Ok(result) => result,
                        Err(error) => Err(OpenError::Open(format!(
                            "opener result channel closed before resolving: {error}"
                        ))),
                    };
                }
                Action::WaitClose(mut rx) => {
                    if close_retries >= CLOSE_REOPEN_MAX_RETRIES {
                        return Err(OpenError::Open(format!(
                            "session '{}' did not settle after {CLOSE_REOPEN_MAX_RETRIES} close/reopen retries",
                            key.session
                        )));
                    }
                    close_retries += 1;
                    let _ = rx.recv().await;
                    tokio::time::sleep(CLOSE_REOPEN_BACKOFF).await;
                }
                Action::Open(ready, gen) => {
                    // Slow open, OFF the lock and in THIS task — but the slot is already
                    // claimed (Opening), so a concurrent op coalesces onto `ready` rather
                    // than starting a second open. On ANY outcome we resolve `ready` AND
                    // fix the slot, so no waiter hangs. If this task is cancelled or panics
                    // mid-await, `OpenGuard` clears the Opening slot.
                    let open_guard = OpenGuard::new(self.inner.clone(), key.clone(), ready.clone());
                    let result = async {
                        ACTOR_OPENS.fetch_add(1, Ordering::Relaxed);
                        if status {
                            self.inner
                                .opener
                                .open_for_status(key, gen, self.handle())
                                .await
                        } else {
                            self.inner
                                .opener
                                .open_with_class(
                                    key,
                                    gen,
                                    self.handle(),
                                    retention,
                                    session_class.clone(),
                                )
                                .await
                        }
                    }
                    .await;
                    {
                        let mut slots = self.inner.slots.lock().unwrap();
                        match &result {
                            Ok(handle) => {
                                slots.insert(
                                    key.clone(),
                                    Entry::Open {
                                        gen,
                                        handle: handle.clone(),
                                    },
                                );
                            }
                            Err(_) => {
                                // Clear the failed Opening slot so a later, independent op
                                // may try again. This caller and all coalesced waiters receive
                                // the typed failure immediately.
                                slots.remove(key);
                            }
                        }
                    }
                    if let Ok(handle) = &result {
                        let _ = SERVED_REQUEST_ACTOR_OPENED.try_with(|opened| {
                            opened.store(true, Ordering::Relaxed);
                        });
                        self.attach_waiting(key, handle);
                    }
                    let _ = ready.send(result.clone());
                    open_guard.commit();
                    return result;
                }
            }
        }
    }

    /// [`Self::route_submission_with_class`] for a send that may use the `delivery_order` group:
    /// `order` is its ordered list, dedupe key and expiry, or `None` for an ordinary send.
    #[allow(clippy::too_many_arguments)]
    pub async fn route_submission_with_order(
        &self,
        key: &LineageKey,
        run_id: RunId,
        request: broca_protocol::request::SubmissionRequest,
        kind: crate::executor::RunKind,
        send_key: crate::actor::SendKey,
        skip_materialization: bool,
        delivery: broca_protocol::Delivery,
        plan: Option<crate::actor::SendPlan>,
        conversation_key: Option<String>,
        title: Option<String>,
        retention: Option<u64>,
        session_class: Option<String>,
        stop_sequences_named: bool,
        order: Option<broca_protocol::DeliveryOrder>,
    ) -> Result<crate::actor::SendOutcome, OpenError> {
        use crate::actor::{LineageCmd, SendOutcome};
        if let Some(class) = &session_class {
            broca_protocol::request::validate_session_class(class)
                .map_err(|message| OpenError::SessionClassInvalid(message.into()))?;
        }
        crate::retention::validate_retention(retention, None, false)
            .map_err(OpenError::RetentionInvalid)?;
        if let Some(manifest) = plan.as_ref().and_then(|plan| plan.manifest.as_ref()) {
            let policy: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text())
                .expect("an opaque plan is valid JSON");
            if let Some(policy) = policy.as_object().filter(|policy| {
                policy.contains_key(cortexkit_role_llm_runner::app_readers::PLAN_READERS)
            }) {
                let supported = self.store_holding(key)?.is_some() || self.store_can_create();
                // A session that can't be created yet (its key still loading after a start) is
                // refused as retryable, as the opener refuses it.
                if !supported {
                    if let Some(pending) = self.store_creation_pending() {
                        return Err(pending);
                    }
                }
                if let Err(error) = crate::reader_admission::check_plan(policy, supported) {
                    let message = match error {
                        crate::serve::ServeError::InvalidParams { message, .. } => message,
                        error => return Err(OpenError::Open(error.to_string())),
                    };
                    return Ok(SendOutcome::DeliveryRefused {
                        code: "invalid_params",
                        field: "plan.readers",
                        message,
                    });
                }
            }
        }
        // How the session starts if this send creates it.
        let creation = if session_class.as_deref() == Some(HEAD_SESSION_CLASS) {
            StoreCreation::Head
        } else {
            StoreCreation::Send
        };
        if !request.message_marks.is_empty() {
            // Refuse invalid marks before opening a writer, including its new-WAL header.
            // Ownership replay is large; carrying it inline multiplies the send future's stack
            // footprint at every await site, overflowing unoptimized lifecycle tests.
            let ownership = match Box::pin(self.bind_ownership(key)).await {
                BindOwnership::Known(ownership) => ownership,
                BindOwnership::NoHistorySource => {
                    return Ok(SendOutcome::Error(
                        "message marks require an ownership history reader".into(),
                    ));
                }
                BindOwnership::Unreadable(error) => return Ok(SendOutcome::Error(error)),
            };
            if let Err(field) = broca_core::scope_admission::admit_message_marks(
                &request.message_marks,
                &ownership,
                request.principal.as_ref(),
                &request.scope,
            ) {
                return Ok(SendOutcome::StructuredOutputInvalid {
                    field: field.into(),
                });
            }
        }
        loop {
            self.inner
                .store_eligible
                .lock()
                .unwrap()
                .insert(key.clone(), creation);
            let handle = self
                .actor_for_mode(key, retention, session_class.clone(), false)
                .await;
            // A mark the open didn't take, because the session was already open, must not
            // decide a later open.
            self.inner.store_eligible.lock().unwrap().remove(key);
            let handle = handle?;
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::Send {
                session_class: session_class.clone(),
                retention,
                stop_sequences_named,
                conversation_key: conversation_key.clone(),
                title: title.clone(),
                run_id: run_id.clone(),
                request: Box::new(request.clone()),
                kind,
                key: send_key.clone(),
                skip_materialization,
                delivery,
                plan: plan.clone().map(Box::new),
                order: order.clone().map(Box::new),
                reply,
            };
            if handle.send(cmd).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(SendOutcome::Reopen) => continue,
                Ok(other) => return Ok(other),
                Err(_) => continue,
            }
        }
    }

    /// Route a provider-free import through the lineage actor so it shares the lease and writer.
    pub async fn route_import(
        &self,
        key: &LineageKey,
        request: broca_core::ImportRequest,
    ) -> Result<crate::actor::ImportOutcome, OpenError> {
        use crate::actor::{ImportOutcome, LineageCmd};
        loop {
            self.inner
                .store_eligible
                .lock()
                .unwrap()
                .insert(key.clone(), StoreCreation::Send);
            let handle = self.actor_for(key).await;
            self.inner.store_eligible.lock().unwrap().remove(key);
            let handle = handle?;
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::Import {
                request: Box::new(request.clone()),
                reply,
            };
            if handle.send(cmd).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(ImportOutcome::Reopen) => continue,
                Ok(other) => return Ok(other),
                Err(_) => continue,
            }
        }
    }

    /// Route a command only the session store serves to the session's store actor. An import may
    /// create its session there; nothing else creates one. An imported session is a head, and a
    /// head starts in the session store whenever the opener has one, so an import needs only the
    /// store runtime. Any other command is refused by name unless the session store already keeps
    /// the session, as it keeps a head, whose compaction provider applies its views through here.
    pub async fn route_store_command(
        &self,
        key: &LineageKey,
        command: crate::store_import::StoreCommand,
    ) -> Result<crate::store_import::StoreOutcome, OpenError> {
        use crate::actor::LineageCmd;
        use crate::store_import::{ImportRefusal, StoreCommand, StoreOutcome};
        let creates = matches!(command, StoreCommand::Import(_));
        // An import that asks for retention creates its session in the ephemeral store, as a
        // send that does.
        let retention = match &command {
            StoreCommand::Import(import) => import.params.retention,
            _ => None,
        };
        if creates && self.store_runtime().is_none() {
            return Ok(StoreOutcome::Refused(ImportRefusal::new(
                "new_storage_unavailable",
                "this module doesn't create sessions in the new storage",
            )));
        }
        loop {
            if creates {
                self.inner
                    .store_eligible
                    .lock()
                    .unwrap()
                    .insert(key.clone(), StoreCreation::Import);
            }
            let handle = self.actor_for_mode(key, retention, None, false).await;
            // A mark the open didn't take, because the session was already open, must not
            // decide a later open.
            self.inner.store_eligible.lock().unwrap().remove(key);
            let handle = handle?;
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::Store {
                command: command.clone(),
                reply,
            };
            if handle.send(cmd).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(StoreOutcome::Reopen) | Err(_) => continue,
                Ok(other) => return Ok(other),
            }
        }
    }

    pub async fn route_cancel_at(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
        at: broca_wal::record::RunCancelAt,
    ) -> Result<crate::actor::CancelOutcome, OpenError> {
        self.route_cancel_inner(key, run_id, at, None).await
    }

    pub async fn route_cancel_from(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
        at: broca_wal::record::RunCancelAt,
        principal: Option<broca_protocol::BindPrincipal>,
        scope: broca_protocol::SendScope,
    ) -> Result<crate::actor::CancelOutcome, OpenError> {
        self.route_cancel_inner(key, run_id, at, Some((principal, scope)))
            .await
    }

    async fn route_cancel_inner(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
        at: broca_wal::record::RunCancelAt,
        caller: Option<(
            Option<broca_protocol::BindPrincipal>,
            broca_protocol::SendScope,
        )>,
    ) -> Result<crate::actor::CancelOutcome, OpenError> {
        use crate::actor::{CancelOutcome, LineageCmd};
        loop {
            self.check_move(key, true)?;
            self.check_expired(key, Some(&run_id.0)).await?;
            // A session that doesn't exist has no run to cancel.
            let handle = match self.actor_for(key).await {
                Err(OpenError::UnknownSession) => return Ok(CancelOutcome::NotActive),
                handle => handle?,
            };
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = match &caller {
                Some((principal, scope)) => LineageCmd::CancelFrom {
                    run_id: run_id.clone(),
                    at,
                    principal: principal.clone(),
                    scope: scope.clone(),
                    reply,
                },
                None => LineageCmd::Cancel {
                    run_id: run_id.clone(),
                    at,
                    reply,
                },
            };
            if handle.send(cmd).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(CancelOutcome::Reopen) => continue,
                Ok(other) => return Ok(other),
                Err(_) => continue,
            }
        }
    }

    /// Route a `session.subscribe` to the lineage: attach a control-stream subscriber from
    /// `from`, delivering to `sink`. Same reopen-retry discipline as `send`. The actor
    /// takes the attach barrier (register→sync→spawn) and acks `Attached`.
    pub async fn route_subscribe(
        &self,
        key: &LineageKey,
        from: crate::fanout::FromCursor,
        sink: mpsc::Sender<crate::fanout::Delivery>,
    ) -> Result<crate::actor::SubscribeOutcome, OpenError> {
        self.route_subscribe_inner(key, from, false, sink).await
    }

    /// Attach future-only at the durable head selected by the actor's attach barrier.
    pub(crate) async fn route_subscribe_live(
        &self,
        key: &LineageKey,
        sink: mpsc::Sender<crate::fanout::Delivery>,
    ) -> Result<crate::actor::SubscribeOutcome, OpenError> {
        self.route_subscribe_inner(key, crate::fanout::FromCursor::Start, true, sink)
            .await
    }

    async fn route_subscribe_inner(
        &self,
        key: &LineageKey,
        from: crate::fanout::FromCursor,
        live: bool,
        sink: mpsc::Sender<crate::fanout::Delivery>,
    ) -> Result<crate::actor::SubscribeOutcome, OpenError> {
        use crate::actor::{LineageCmd, SubscribeOutcome};
        loop {
            self.check_expired(key, None).await?;
            let handle = match self.actor_for(key).await {
                // Nothing has written the session yet: a subscriber from the start, or from now,
                // waits for its first send and then follows it from its start, which is all there
                // is to follow. A cursor can only name a session that exists.
                Err(OpenError::UnknownSession)
                    if matches!(from, crate::fanout::FromCursor::Start) =>
                {
                    self.wait_for_creation(key, sink.clone()).await;
                    return Ok(SubscribeOutcome::Attached);
                }
                handle => handle?,
            };
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::Subscribe {
                from,
                live,
                sink: sink.clone(),
                reply,
            };
            if handle.send(cmd).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(SubscribeOutcome::Reopen) => continue,
                Ok(other) => return Ok(other),
                Err(_) => continue,
            }
        }
    }

    /// Holds `sink` until the session `key` is created, after telling it it's attached: a
    /// display reset, as every attach starts with. A session that opened meanwhile hands it on
    /// at once.
    async fn wait_for_creation(
        &self,
        key: &LineageKey,
        sink: mpsc::Sender<crate::fanout::Delivery>,
    ) {
        let reset = Arc::new(broca_protocol::display::DisplayEvent::DisplayReset);
        let _ = sink.send(crate::fanout::Delivery::Display(reset)).await;
        {
            // A subscriber that left before its session was created leaves nothing behind.
            let mut waiting = self.inner.waiting_subscribers.lock().unwrap();
            waiting.retain(|_, sinks| {
                sinks.retain(|held| !held.is_closed());
                !sinks.is_empty()
            });
            waiting.entry(key.clone()).or_default().push(sink);
        }
        let open = match self.inner.slots.lock().unwrap().get(key) {
            Some(Entry::Open { handle, .. }) => Some(handle.clone()),
            _ => None,
        };
        if let Some(handle) = open {
            self.attach_waiting(key, &handle);
        }
    }

    /// Attaches every subscriber waiting for the session `key` to its now open actor, from the
    /// session's start. One whose actor closes before it attaches is dropped; its subscription
    /// ends, and it subscribes again.
    fn attach_waiting(&self, key: &LineageKey, handle: &ActorHandle) {
        let sinks = self
            .inner
            .waiting_subscribers
            .lock()
            .unwrap()
            .remove(key)
            .unwrap_or_default();
        for sink in sinks.into_iter().filter(|sink| !sink.is_closed()) {
            let handle = handle.clone();
            tokio::spawn(async move {
                let (reply, _) = tokio::sync::oneshot::channel();
                let _ = handle
                    .send(crate::actor::LineageCmd::Subscribe {
                        from: crate::fanout::FromCursor::Start,
                        live: false,
                        sink,
                        reply,
                    })
                    .await;
            });
        }
    }

    /// Route a `session.retract` to the lineage: retract a still-pending queued submission
    /// (same reopen-retry discipline). Serialized by the actor (the retract decision +
    /// tombstone write happen under the actor's single-task ordering, not a side path).
    pub async fn route_retract(
        &self,
        key: &LineageKey,
        submission_id: broca_protocol::SubmissionId,
    ) -> Result<crate::actor::RetractOutcome, OpenError> {
        use crate::actor::{LineageCmd, RetractOutcome};
        loop {
            self.check_move(key, true)?;
            self.check_expired(key, None).await?;
            // A session that doesn't exist has no queued send to retract.
            let handle = match self.actor_for(key).await {
                Err(OpenError::UnknownSession) => return Ok(RetractOutcome::NotPending),
                handle => handle?,
            };
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::Retract {
                submission_id: submission_id.clone(),
                reply,
            };
            if handle.send(cmd).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(RetractOutcome::Reopen) => continue,
                Ok(other) => return Ok(other),
                Err(_) => continue,
            }
        }
    }

    /// Route a `session.refresh` acceptance through the lineage actor so it shares the lease
    /// and writer with runs. The manifest was fetched by the serve layer.
    pub async fn route_session_refresh(
        &self,
        key: &LineageKey,
        generation: u64,
        policy: String,
        manifest: Box<broca_wal::PlanManifest>,
    ) -> Result<crate::actor::SessionRefreshOutcome, OpenError> {
        use crate::actor::{LineageCmd, SessionRefreshOutcome};
        loop {
            self.check_move(key, true)?;
            self.check_expired(key, None).await?;
            let handle = self.actor_for(key).await?;
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::SessionRefresh {
                generation,
                policy: policy.clone(),
                manifest: manifest.clone(),
                reply,
            };
            if handle.send(cmd).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(SessionRefreshOutcome::Reopen) => continue,
                Ok(other) => return Ok(other),
                Err(_) => continue,
            }
        }
    }

    /// Route a `session.refresh_policy` change through the lineage actor.
    pub async fn route_session_refresh_policy(
        &self,
        key: &LineageKey,
        generation: u64,
        policy: String,
        send_id: String,
    ) -> Result<crate::actor::SessionRefreshPolicyOutcome, OpenError> {
        use crate::actor::{LineageCmd, SessionRefreshPolicyOutcome};
        loop {
            self.check_move(key, true)?;
            self.check_expired(key, None).await?;
            let handle = self.actor_for(key).await?;
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::SessionRefreshPolicy {
                generation,
                policy: policy.clone(),
                send_id: send_id.clone(),
                reply,
            };
            if handle.send(cmd).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(SessionRefreshPolicyOutcome::Reopen) => continue,
                Ok(other) => return Ok(other),
                Err(_) => continue,
            }
        }
    }

    /// Route a `session.flush_prefix` through the lineage actor.
    pub async fn route_session_flush_prefix(
        &self,
        key: &LineageKey,
        send_id: String,
    ) -> Result<crate::actor::SessionFlushPrefixOutcome, OpenError> {
        use crate::actor::{LineageCmd, SessionFlushPrefixOutcome};
        loop {
            self.check_move(key, true)?;
            self.check_expired(key, None).await?;
            // A session that doesn't exist has no prefix to flush.
            let handle = match self.actor_for(key).await {
                Err(OpenError::UnknownSession) => {
                    return Ok(SessionFlushPrefixOutcome::Flushed {
                        pending_generation: None,
                    })
                }
                handle => handle?,
            };
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::SessionFlushPrefix {
                send_id: send_id.clone(),
                reply,
            };
            if handle.send(cmd).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(SessionFlushPrefixOutcome::Reopen) => continue,
                Ok(other) => return Ok(other),
                Err(_) => continue,
            }
        }
    }

    /// The session's first episode, as its open actor knows it, with no read of the WAL or the
    /// store. `None` when no actor holds the session or it has not learned the episode yet.
    pub fn first_episode(&self, key: &LineageKey) -> Option<broca_core::FirstEpisodeV1> {
        match self.inner.slots.lock().unwrap().get(key) {
            Some(Entry::Open { handle, .. }) => handle.first_episode.get().cloned(),
            _ => None,
        }
    }

    /// The session store's runtime when it keeps the session `key`; `None` when this module has
    /// no session store runtime for it. A session it keeps is refused `key_unavailable` while
    /// the deployment key hasn't loaded.
    pub(crate) fn store_holding(
        &self,
        key: &LineageKey,
    ) -> Result<Option<Arc<crate::store_open::StoreRuntime>>, OpenError> {
        self.inner.opener.store_holding(key)
    }

    /// Expiring logs blocked from deletion because they are unreadable and the indexed
    /// deadline cannot be proved to include every WAL record.
    pub fn retention_unreadable_sessions(&self) -> usize {
        self.inner
            .retention
            .get()
            .map_or(0, |service| service.unreadable_sessions())
    }

    /// A closed session's status comes from the store; its actor opens only when the session
    /// needs recovering first.
    pub async fn route_status(
        &self,
        key: &LineageKey,
    ) -> Result<crate::actor::LineageStatus, OpenError> {
        self.check_move(key, false)?;
        if matches!(
            self.check_move(key, true),
            Err(OpenError::SessionMoving { .. })
        ) {
            return Ok(crate::actor::LineageStatus::Idle);
        }
        use crate::actor::{LineageCmd, LineageStatus};
        loop {
            let live = {
                let slots = self.inner.slots.lock().unwrap();
                match slots.get(key) {
                    Some(Entry::Open { handle, .. }) => Some(handle.clone()),
                    _ => None,
                }
            };
            let Some(handle) = live else {
                // A closed session's actor answers once opened, when it needs recovering; any
                // other closed session, or one the store doesn't hold, has nothing running.
                if let Some(store) = self.store_holding(key)? {
                    if crate::store_status::status_needs_actor(&store, key).await? {
                        self.actor_for_mode(key, None, None, true).await?;
                        continue;
                    }
                }
                return Ok(LineageStatus::Idle);
            };
            let (reply, rx) = tokio::sync::oneshot::channel();
            if handle.send(LineageCmd::Status { reply }).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(LineageStatus::Closing) => continue,
                Ok(other) => return Ok(other),
                Err(_) => continue,
            }
        }
    }

    /// Stop admission and queued dispatch, asking active runs to stop at their next step boundary.
    /// This does not seal actors or start a deadline: the daemon's drain window belongs to
    /// in-flight work. Its channel-0 GOODBYE (or connection loss) starts the final seal budget.
    /// The daemon's drain command is irreversible; only a replacement process admits work again.
    pub fn begin_stop(&self) {
        self.inner.stopping.cancel();
        let idle_heads: Vec<_> = self
            .inner
            .idle_warms
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        for key in idle_heads {
            self.handle().stop_idle_warm(&key, "shutdown");
        }
    }

    /// A child token for boot work to stop when the module begins draining.
    /// Cancelling the child cannot stop the registry itself.
    pub fn stopping_signal(&self) -> tokio_util::sync::CancellationToken {
        self.inner.stopping.child_token()
    }

    /// Stop admission, allow short runs to finish, then seal and close all remaining actors.
    /// The shared 20-second budget reserves ten seconds below the supervisor's SIGKILL ceiling
    /// for scheduling and process exit. Actors seal concurrently; large WAL reads never extend it.
    /// The actors' five-second deadline covers the Interrupted write, not child/writer joins.
    /// A blocked join retains its slot and lease until complete; this method still returns at
    /// the total deadline, leaving process exit to end the writer without releasing its lease early.
    /// False means actors present during this stop could not finish: leave their WAL to crash
    /// recovery, not cancellation. Earlier failures do not taint this result; unresolved recovery
    /// debt and lifetime failure events remain visible in [`Self::shutdown_report`].
    ///
    /// The ceiling is subc's default module drain timeout, 30 seconds. The supervisor drains
    /// routes, sends GOODBYE on channel 0, then waits for child exit before SIGKILL. Changing
    /// that default requires reviewing this budget too; this budget is not derived from it.
    ///
    /// Do not read the module status's `drain_timeout_ms` to set these constants. It can change
    /// under a running child through config rescan, and `ck module restart <m> --drain-ms N`
    /// applies a timeout to one shutdown without changing status. `--now` means zero, so a
    /// boot-time status read cannot predict the timeout for a later restart.
    ///
    /// Sealing starts when serve returns after channel-0 GOODBYE. The fixed budget bounds our
    /// wait after that event; it does not attempt to track the supervisor's mutable policy.
    pub async fn stop_and_seal(&self) -> bool {
        self.stop_and_seal_with_budget(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(20),
        )
        .await
    }

    pub(crate) async fn stop_and_seal_with_budget(
        &self,
        grace: std::time::Duration,
        total: std::time::Duration,
    ) -> bool {
        // Include failures already reported by a closing slot that still belongs to this stop,
        // but never import a previous generation's failure when its session has reopened.
        let (stopping_generations, first_failure) = {
            let slots = self.inner.slots.lock().unwrap();
            let generations: HashSet<_> = slots
                .values()
                .map(|entry| match entry {
                    Entry::Opening { gen, .. }
                    | Entry::Open { gen, .. }
                    | Entry::Closing { gen, .. } => *gen,
                })
                .collect();
            let first_failure = self.inner.seal_state.lock().unwrap().failures.len();
            self.begin_stop();
            (generations, first_failure)
        };
        let started = tokio::time::Instant::now();
        let deadline = started + total;
        while started.elapsed() < grace.min(total) {
            let counts = self.slot_counts();
            if counts.runs_in_flight == 0 && counts.opening == 0 && counts.batches_in_flight == 0 {
                break;
            }
            tokio::time::sleep_until(
                (tokio::time::Instant::now() + std::time::Duration::from_millis(10)).min(deadline),
            )
            .await;
        }
        self.inner.seal.cancel();
        self.inner.opener.retire_all_transports();
        loop {
            let counts = self.slot_counts();
            if counts.open + counts.opening + counts.closing == 0 {
                return self.record_shutdown_report(
                    &stopping_generations,
                    first_failure,
                    false,
                    started,
                    total,
                );
            }
            if tokio::time::Instant::now() >= deadline {
                return self.record_shutdown_report(
                    &stopping_generations,
                    first_failure,
                    true,
                    started,
                    total,
                );
            }
            tokio::time::sleep_until(
                (tokio::time::Instant::now() + std::time::Duration::from_millis(10)).min(deadline),
            )
            .await;
        }
    }

    fn record_shutdown_report(
        &self,
        stopping_generations: &HashSet<u64>,
        first_failure: usize,
        deadline_expired: bool,
        started: tokio::time::Instant,
        total: std::time::Duration,
    ) -> bool {
        let mut remaining_sessions: Vec<_> = self
            .inner
            .slots
            .lock()
            .unwrap()
            .iter()
            .map(|(key, entry)| {
                let state = match entry {
                    Entry::Open { .. } => "open",
                    Entry::Opening { .. } => "opening",
                    Entry::Closing { .. } => "closing",
                };
                (key.session.clone(), state)
            })
            .collect();
        remaining_sessions.sort_unstable();
        let state = self.inner.seal_state.lock().unwrap();
        let current_failures = state
            .failures
            .iter()
            .enumerate()
            .filter(|(index, event)| {
                *index >= first_failure || stopping_generations.contains(&event.generation)
            })
            .map(|(_, event)| event.clone())
            .collect::<Vec<_>>();
        let complete =
            !deadline_expired && remaining_sessions.is_empty() && current_failures.is_empty();
        let cause = if deadline_expired {
            "registry total shutdown deadline expired"
        } else if !remaining_sessions.is_empty() {
            "shutdown slots remaining"
        } else if current_failures
            .iter()
            .any(|event| event.stage == SealFailureStage::SealDeadline)
        {
            "actor shutdown seal deadline expired"
        } else if current_failures.iter().any(|event| {
            matches!(
                event.stage,
                SealFailureStage::FinalSync | SealFailureStage::WriterShutdown
            )
        }) {
            "fresh final-sync or writer shutdown failure"
        } else if !current_failures.is_empty() {
            "actor bounded shutdown seal failed"
        } else {
            "current shutdown complete"
        };
        let mut recovery_debt: Vec<_> = state.recovery_debt.values().cloned().collect();
        recovery_debt.sort_by(|a, b| a.session.cmp(&b.session));
        *self.inner.shutdown_report.lock().unwrap() = Some(ShutdownReport {
            complete,
            cause,
            elapsed: started.elapsed(),
            budget: total,
            remaining_sessions,
            current_failures,
            lifetime_failures: state.failures.len(),
            recovery_debt,
            latest_failure: state.failures.last().cloned(),
        });
        complete
    }

    pub fn shutdown_report(&self) -> Option<ShutdownReport> {
        self.inner.shutdown_report.lock().unwrap().clone()
    }

    /// The gauge the System One batch service raises for each batch call it runs, so health
    /// and the stop's grace see batches beside session runs.
    pub fn batches_in_flight_gauge(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.inner.batches_in_flight)
    }

    /// Cheap inspection: count how many registry slots are opening, open, or closing.
    pub fn slot_counts(&self) -> SlotCounts {
        let slots = self.inner.slots.lock().unwrap();
        let mut counts = SlotCounts {
            open: 0,
            opening: 0,
            closing: 0,
            runs_in_flight: 0,
            batches_in_flight: self.inner.batches_in_flight.load(Ordering::Relaxed),
        };
        for entry in slots.values() {
            match entry {
                Entry::Opening { .. } => counts.opening += 1,
                Entry::Open { handle, .. } => {
                    counts.open += 1;
                    // An atomic the actor writes at its own state transitions, so this reports the
                    // present rather than a past someone computed. Only Open slots are counted:
                    // an Opening actor has admitted nothing yet, and a Closing one has already
                    // stopped taking work.
                    if handle.run_in_flight.load(Ordering::Relaxed) != 0 {
                        counts.runs_in_flight += 1;
                    }
                }
                Entry::Closing { .. } => counts.closing += 1,
            }
        }
        counts
    }

    /// Test/inspection: the current slot state name for a key (None = absent/closed).
    /// The actor of `key` when the registry holds it open, without opening one.
    pub(crate) fn open_handle(&self, key: &LineageKey) -> Option<ActorHandle> {
        match self.inner.slots.lock().unwrap().get(key) {
            Some(Entry::Open { handle, .. }) => Some(handle.clone()),
            _ => None,
        }
    }

    pub fn slot_state(&self, key: &LineageKey) -> Option<&'static str> {
        let slots = self.inner.slots.lock().unwrap();
        slots.get(key).map(|e| match e {
            Entry::Opening { .. } => "opening",
            Entry::Open { .. } => "open",
            Entry::Closing { .. } => "closing",
        })
    }

    /// What the open actor serving `key` last said keeps it from closing idle, under the
    /// reasons [`Self::tool_routes_health`] counts (`live_subscriber` for a subscriber still
    /// attached). Empty when no actor holds the session open or it named nothing.
    pub fn idle_holds(&self, key: &LineageKey) -> Vec<&'static str> {
        let slots = self.inner.slots.lock().unwrap();
        let Some(Entry::Open { gen, .. }) = slots.get(key) else {
            return Vec::new();
        };
        self.inner
            .actor_idle_holds
            .lock()
            .unwrap()
            .get(key)
            .filter(|(owner, _)| owner == gen)
            .map(|(_, holds)| holds.clone())
            .unwrap_or_default()
    }

    /// Let a session's run take a late result just deposited for one of its provider-held
    /// calls: open the session if it is closed (its open-time recovery runs first), then ask its
    /// actor to resume a run paused on held calls. A live run needs nothing more.
    pub async fn resume_held_run(&self, session: &LineageKey) -> Result<(), String> {
        // A restore holds this session: the delivery is retried on a later pull, which finds
        // the session as the restore left it.
        if self.inner.restore_fences.fenced(session) {
            return Err("session_moving: a restore holds this session".into());
        }
        for _ in 0..3 {
            let handle = self
                .actor_for(session)
                .await
                .map_err(|error| error.to_string())?;
            let (reply, rx) = tokio::sync::oneshot::channel();
            if handle
                .send(crate::actor::LineageCmd::ResumeHeld { reply })
                .await
                .is_err()
            {
                // The actor began closing: re-route through a fresh open.
                continue;
            }
            return rx
                .await
                .map_err(|_| "session actor dropped the held-run reply".to_owned())?;
        }
        Err("session actor kept closing while a late result waited".into())
    }

    /// Boot recovery for provider-held calls: open every session that has an intake
    /// registration, so its open-time recovery runs (a missing `RunPaused`, an owed batch commit,
    /// a crash-orphan seal) before any late result is pulled for it. At most `concurrency`
    /// sessions open at once. Returns how many opened and the sessions that could not be.
    pub async fn open_registered_sessions(
        &self,
        sessions: Vec<LineageKey>,
        concurrency: usize,
    ) -> (usize, Vec<(LineageKey, String)>) {
        use futures::stream::{self, StreamExt};
        let mut seen = HashSet::new();
        let sessions: Vec<LineageKey> = sessions
            .into_iter()
            .filter(|session| seen.insert(session.clone()))
            .collect();
        let outcomes: Vec<(LineageKey, Result<(), String>)> = stream::iter(sessions)
            .map(|session| async move {
                let opened = self
                    .actor_for(&session)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                (session, opened)
            })
            .buffer_unordered(concurrency.max(1))
            .collect()
            .await;
        let mut opened = 0;
        let mut failed = Vec::new();
        for (session, outcome) in outcomes {
            match outcome {
                Ok(()) => opened += 1,
                Err(error) => failed.push((session, error)),
            }
        }
        (opened, failed)
    }

    /// Allows restart admission once dependency routes are registered. The deployment-key
    /// loader owns store discovery.
    pub fn enable_restart_resumes(&self) {
        self.inner
            .restart_resume_ready
            .store(true, Ordering::Release);
    }
}

impl RegistryHandle {
    /// Takes the mark that the send or import opening `key` is one the store actor serves, which
    /// says how the session it creates is placed.
    pub(crate) fn take_store_eligible(&self, key: &LineageKey) -> Option<StoreCreation> {
        self.inner.store_eligible.lock().unwrap().remove(key)
    }

    /// Marks the next open of `key` as one a creating `creation` the store actor serves asked
    /// for, as a send or import does before it opens its session, so a test can open the
    /// session directly.
    #[cfg(test)]
    pub(crate) fn mark_store_eligible(&self, key: &LineageKey, creation: StoreCreation) {
        self.inner
            .store_eligible
            .lock()
            .unwrap()
            .insert(key.clone(), creation);
    }

    /// See [`LineageRegistry::scope_reservation`].
    pub(crate) async fn scope_reservation(
        &self,
        key: &LineageKey,
    ) -> Result<Option<broca_core::scope_admission::ScopeReservation>, String> {
        LineageRegistry {
            inner: self.inner.clone(),
        }
        .scope_reservation(key)
        .await
    }

    pub(crate) fn restart_resume_ready(&self) -> bool {
        self.inner.restart_resume_ready.load(Ordering::Acquire)
    }

    #[cfg(all(test, feature = "test-clock"))]
    pub(crate) fn enable_restart_resumes_for_test(&self) {
        self.inner
            .restart_resume_ready
            .store(true, Ordering::Release);
    }

    /// Whether a cache declarer is installed.
    pub(crate) fn declares_cache(&self) -> bool {
        self.inner.cache.has_declarer()
    }

    /// What `config` declares about its prompt cache; `None` without an installed declarer.
    pub(crate) fn cache_declaration(
        &self,
        config: &broca_wal::RunConfig,
    ) -> Option<crate::cache::CacheDeclaration> {
        self.inner.cache.declare(config)
    }

    /// Remember the cache declaration of the episode that admitted the session, for the reply
    /// to the admitting send.
    pub(crate) fn note_admitted_cache(
        &self,
        key: &LineageKey,
        run_id: RunId,
        declaration: crate::cache::CacheDeclaration,
    ) {
        self.inner.cache.note_admitted(key, run_id, declaration);
    }

    pub(crate) fn is_stopping(&self) -> bool {
        self.inner.stopping.is_cancelled()
    }

    pub(crate) fn seal_signal(&self) -> tokio_util::sync::CancellationToken {
        self.inner.seal.clone()
    }

    /// A token that trips when this registry begins to stop, for a run to read. A child token,
    /// so cancelling it can never stop the registry itself.
    pub(crate) fn stopping_signal(&self) -> tokio_util::sync::CancellationToken {
        self.inner.stopping.child_token()
    }

    /// Begin closing THIS actor's slot: CAS `Open{gen} → Closing` only if the slot is
    /// still `Open` at the SAME generation (a newer actor must never be clobbered). On
    /// success returns a `CloseToken` the actor signals when the teardown completes.
    /// Returns `None` if the slot isn't `Open{gen}` (a re-open already replaced this
    /// actor) — the actor should just exit without touching the slot.
    pub fn begin_close(&self, key: &LineageKey, gen: u64) -> Option<CloseToken> {
        let mut slots = self.inner.slots.lock().unwrap();
        match slots.get(key) {
            Some(Entry::Open { gen: g, .. }) if *g == gen => {
                let (done, _rx) = broadcast::channel(1);
                slots.insert(
                    key.clone(),
                    Entry::Closing {
                        gen,
                        done: done.clone(),
                    },
                );
                #[cfg(test)]
                self.inner
                    .close_started
                    .send_modify(|generation| *generation = generation.wrapping_add(1));
                Some(CloseToken {
                    inner: self.inner.clone(),
                    key: key.clone(),
                    done,
                    resolved: false,
                })
            }
            _ => None,
        }
    }

    /// Finalizer for an UNEXPECTED actor exit (panic / fatal error): CAS
    /// `Open{gen} → absent` for this actor's OWN generation only, so a racing op stops
    /// grabbing the dead handle and re-opens. A no-op if the slot is already past this
    /// generation (a newer actor) or already Closing/absent.
    pub fn finalize_exit(&self, key: &LineageKey, gen: u64) {
        let mut slots = self.inner.slots.lock().unwrap();
        if let Some(Entry::Open { gen: g, .. }) = slots.get(key) {
            if *g == gen {
                slots.remove(key);
            }
        }
        let mut holds = self.inner.actor_idle_holds.lock().unwrap();
        if holds.get(key).is_some_and(|(owner, _)| *owner == gen) {
            holds.remove(key);
        }
    }

    /// Records what keeps generation `gen`'s actor for `key` from closing idle. Returns false,
    /// recording nothing, unless that generation holds the slot open.
    pub(crate) fn note_idle_holds(
        &self,
        key: &LineageKey,
        gen: u64,
        reasons: Vec<&'static str>,
    ) -> bool {
        let slots = self.inner.slots.lock().unwrap();
        if matches!(slots.get(key), Some(Entry::Open { gen: owner, .. }) if *owner == gen) {
            self.inner
                .actor_idle_holds
                .lock()
                .unwrap()
                .insert(key.clone(), (gen, reasons));
            return true;
        }
        false
    }
}

/// The actor holds this for the duration of its close teardown. On `commit()` the slot
/// goes `Closing → absent` (Closed) and waiters are woken; on `abort()` it goes
/// `Closing → Open{gen}` (the close was cancelled because work raced in). Dropping it
/// without either is treated as a commit (the actor is gone regardless).
pub struct CloseToken {
    inner: Arc<Inner>,
    key: LineageKey,
    done: broadcast::Sender<()>,
    /// True once `commit`/`abort` ran. If the token is DROPPED still false (the actor task
    /// panicked mid-teardown), `Drop` treats it as a commit so the slot can't get stuck
    /// `Closing` forever (which would block every re-open of the session).
    resolved: bool,
}

impl CloseToken {
    /// The close finished: remove the slot (Closed) and wake the `actor_for` waiters.
    pub fn commit(mut self) {
        self.resolved = true;
        {
            let mut slots = self.inner.slots.lock().unwrap();
            slots.remove(&self.key);
        }
        let _ = self.done.send(());
    }
}

impl Drop for CloseToken {
    fn drop(&mut self) {
        // `commit`/`abort` set `resolved`. If we get here UNresolved, the actor task was
        // dropped mid-teardown (a panic between begin_close and commit). Treat it as a
        // commit — remove the `Closing` slot and wake the re-open waiters — so the session
        // doesn't get stuck `Closing` forever (which would deadlock every future open of it).
        if self.resolved {
            return;
        }
        {
            let mut slots = self.inner.slots.lock().unwrap();
            slots.remove(&self.key);
        }
        let _ = self.done.send(());
    }
}

impl LineageRegistry {
    /// Install before actors or requests can read retained sessions, so every reader and writer
    /// uses the same expiry clock.
    pub fn set_retention_service(&self, service: Arc<crate::retention::RetentionService>) {
        let inner = Arc::downgrade(&self.inner);
        service.set_evictor(Arc::new(move |key| {
            if let Some(inner) = inner.upgrade() {
                let registry = LineageRegistry { inner };
                // Deletion invokes this callback while it still holds the session lease.
                // Repeated cache eviction after that lease is released must not close routes
                // belonging to a session recreated at the same address.
                if registry.evict_retention_caches(key) {
                    registry.inner.opener.on_session_deleted(key);
                }
            }
        }));
        let _ = self.inner.retention.set(service);
    }

    /// Deletes the retained sessions whose policy has expired, each under its lease.
    pub async fn sweep_once(&self, now_ms: u64) -> Result<usize, OpenError> {
        let Some(service) = self.inner.retention.get().cloned() else {
            return Ok(0);
        };
        let now_ms = if service
            .clock
            .refresh()
            .map_err(|e| OpenError::Open(e.to_string()))?
        {
            service.clock.now_ms()
        } else {
            now_ms
        };
        let mut deleted = 0;
        if let Some(runtime) = self.store_runtime() {
            for key in service.sweep_store(&runtime, now_ms).await {
                self.evict_retention_caches(&key);
                deleted += 1;
            }
        }
        tracing::info!(target: "retention", deleted, "retention sweep complete");
        Ok(deleted)
    }

    /// Runs a pass every five minutes. A clock-change notification also runs a pass immediately,
    /// so tests can advance time rather than wait days.
    pub async fn run_retention_sweeper(&self) -> Result<(), OpenError> {
        let Some(service) = self.inner.retention.get().cloned() else {
            return Ok(());
        };
        tracing::info!(target: "retention", interval_ms = crate::retention::SWEEP_INTERVAL_MS, "retention sweep armed");
        let mut changes = service.clock.subscribe();
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(
            crate::retention::SWEEP_INTERVAL_MS,
        ));
        let clock = service.clock.clone();
        let poll = clock.poll_changes();
        tokio::pin!(poll);
        tokio::select! {
            _ = self.inner.stopping.cancelled() => Ok(()),
            result = &mut poll => result.map_err(|e| OpenError::Open(e.to_string())),
            _ = async {
                loop {
                    tokio::select! {
                        _ = interval.tick() => {},
                        _ = changes.changed() => {},
                    }
                    if self.sweep_once(service.clock.now_ms()).await.is_err() {
                        tracing::warn!(target: "retention", "retention sweep deferred; retrying next pass");
                    }
                }
            } => Ok(()),
        }
    }

    async fn check_authorization_expired(&self, key: &LineageKey) -> Result<(), OpenError> {
        match self.check_expired(key, None).await {
            // Ownership must be checked against the old copy before disclosing a move or cut.
            // The request's operation-specific move check runs after authorization succeeds.
            Err(OpenError::SessionMoving { .. } | OpenError::SessionMoved { .. }) => Ok(()),
            result => result,
        }
    }

    pub(crate) async fn check_expired(
        &self,
        key: &LineageKey,
        run: Option<&str>,
    ) -> Result<(), OpenError> {
        self.check_expired_with_view(key, run).await.map(|_| ())
    }

    /// Check the same expiry barrier as every operation, retaining its stored snapshot so a
    /// read can serve it without loading an over-budget view again.
    pub(crate) async fn check_expired_with_view(
        &self,
        key: &LineageKey,
        run: Option<&str>,
    ) -> Result<Option<crate::store_read::StoredLocalView>, OpenError> {
        let Some(runtime) = self.store_runtime() else {
            return Ok(None);
        };
        let now_ms = self
            .inner
            .retention
            .get()
            .map_or(0, |service| service.clock.now_ms());
        let (expired_at_ms, view) = runtime.expiry_with_view(key, run, now_ms).await?;
        if let Some(expired_at_ms) = expired_at_ms {
            return Err(OpenError::Expired { expired_at_ms });
        }
        Ok(view)
    }

    fn evict_retention_caches(&self, key: &LineageKey) -> bool {
        self.handle().stop_idle_warm(key, "expired");
        self.inner.opener.retire_session_transport(key);
        self.inner.cache.remove(key);
        true
    }
}

impl RegistryHandle {
    pub(crate) fn retention_service(&self) -> Option<Arc<crate::retention::RetentionService>> {
        self.inner.retention.get().cloned()
    }

    pub(crate) fn store_runtime(&self) -> Option<Arc<crate::store_open::StoreRuntime>> {
        self.inner.opener.store_runtime()
    }

    pub(crate) fn backup_classes(&self) -> Vec<String> {
        self.inner.opener.backup_classes()
    }
}

struct Inner {
    /// The sessions a move into the session store holds (see `store_move`), by address, and the
    /// reads in flight per address that take no session lease, which such a move waits out.
    restore_fences: store_move::RestoreFences,
    /// What runs `session.restore_from_backup`, installed by the daemon with its stores.
    store_restorer: std::sync::OnceLock<Arc<dyn store_restore::StoreRestorer>>,
    session_address_inventory: std::sync::OnceLock<Arc<dyn broca_store::SessionAddressResolver>>,
    session_inventory_readiness: std::sync::OnceLock<Arc<dyn SessionInventoryReadiness>>,
    tool_route_health: std::sync::OnceLock<Arc<ToolRouteHealth>>,
    actor_idle_holds: Mutex<HashMap<LineageKey, (u64, Vec<&'static str>)>>,
    /// Subscribers to sessions nothing has written yet, by session. They are attached from the
    /// session's start once its first send creates it.
    waiting_subscribers: Mutex<HashMap<LineageKey, Vec<mpsc::Sender<crate::fanout::Delivery>>>>,
    idle_warms: Mutex<HashMap<LineageKey, idle_warm::Scheduled>>,
    idle_warm_stops: Mutex<HashSet<LineageKey>>,
    /// Raised and lowered by the System One batch service (`classify.run` on the classifier
    /// route); read into `batches_in_flight`.
    batches_in_flight: Arc<AtomicUsize>,
    cut_gates: Mutex<HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    /// Serialize refresh fetches without blocking sends or the actor's running episode.
    change_gates: Mutex<HashMap<LineageKey, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    retention: std::sync::OnceLock<Arc<crate::retention::RetentionService>>,
    /// Serves the fork operations of the heads kept in the session store, made on the first one.
    forks: std::sync::OnceLock<Arc<crate::fork_answer::ForkManager>>,
    /// Who owns each session whose ownership was read lately, for the reader census, which
    /// must not read storage itself. Bounded: emptied when it reaches [`OWNERS_CACHED`].
    owners: Mutex<HashMap<LineageKey, OwnerFacts>>,
    #[cfg(test)]
    close_started: tokio::sync::watch::Sender<u64>,
    /// The admitter for sends carrying a fetch plan: it fetches from every provider the plan
    /// names and builds the manifest to freeze. Unset, a send carrying a plan is refused.
    plan_admitter: std::sync::OnceLock<Arc<dyn broca_fetch_plan::PlanAdmitter>>,
    slots: Mutex<HashMap<LineageKey, Entry>>,
    next_gen: AtomicU64,
    /// Sessions whose creating send or import the store actor serves, marked by it with how the
    /// session it creates is placed, and taken by the opener, which starts such a session in the
    /// new storage when it exists nowhere yet.
    store_eligible: Mutex<HashMap<LineageKey, StoreCreation>>,
    opener: Arc<dyn LineageOpener>,
    run_index: Option<Arc<dyn broca_core::RunEventBodies>>,
    stopping: tokio_util::sync::CancellationToken,
    seal: tokio_util::sync::CancellationToken,
    seal_state: Mutex<SealState>,
    shutdown_report: Mutex<Option<ShutdownReport>>,
    restart_resume_ready: AtomicBool,
    /// Every session's prompt-cache facts. Kept here rather than in the actor so they outlive
    /// the actor's idle close: a caller warms a session minutes after its last round.
    cache: crate::cache::CacheTable,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    struct NeverOpener;
    #[async_trait]
    impl LineageOpener for NeverOpener {
        async fn open(
            &self,
            _key: &LineageKey,
            _gen: u64,
            _registry: RegistryHandle,
        ) -> Result<ActorHandle, OpenError> {
            unreachable!("not used in this test")
        }
    }

    #[derive(Default)]
    struct TransportOpener {
        retired: Mutex<Vec<LineageKey>>,
        shutdowns: AtomicUsize,
        route_deletions: AtomicUsize,
    }

    #[async_trait]
    impl LineageOpener for TransportOpener {
        fn on_session_deleted(&self, _: &LineageKey) {
            self.route_deletions.fetch_add(1, Ordering::SeqCst);
        }
        fn retire_session_transport(&self, session: &LineageKey) {
            self.retired.lock().unwrap().push(session.clone());
        }

        fn retire_idle_session_transport(&self, session: &LineageKey) {
            self.retired.lock().unwrap().push(session.clone());
        }

        fn retire_all_transports(&self) {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
        }

        async fn open(
            &self,
            _key: &LineageKey,
            _gen: u64,
            _registry: RegistryHandle,
        ) -> Result<ActorHandle, OpenError> {
            unreachable!("transport invalidation must not open an actor")
        }
    }

    #[tokio::test]
    async fn retained_transport_move_and_deletion_invalidate_without_opening_actors() {
        let opener = Arc::new(TransportOpener::default());
        let registry = LineageRegistry::new(opener.clone());
        let key = key();
        let _guard = registry
            .lock_move_sessions(std::slice::from_ref(&key), false)
            .await
            .unwrap();
        registry.evict_retention_caches(&key);
        assert_eq!(*opener.retired.lock().unwrap(), vec![key.clone(), key]);
        assert_eq!(
            opener.route_deletions.load(Ordering::SeqCst),
            0,
            "post-lease cache eviction cannot tear down a recreated actor's routes"
        );
    }

    #[tokio::test]
    async fn retained_transport_shutdown_invalidates_even_without_open_actors() {
        let opener = Arc::new(TransportOpener::default());
        let registry = LineageRegistry::new(opener.clone());
        assert!(
            registry
                .stop_and_seal_with_budget(
                    std::time::Duration::ZERO,
                    std::time::Duration::from_secs(1)
                )
                .await
        );
        assert_eq!(opener.shutdowns.load(Ordering::SeqCst), 1);
    }

    /// An opener whose actors accept nothing: these tests only watch who opened them.
    struct GaugeOpener;

    #[tokio::test]
    async fn request_actor_open_flag_marks_only_the_request_that_opens_it() {
        let registry = LineageRegistry::new(Arc::new(GaugeOpener));
        let key = BindIdentity::new("/tmp/request-open", "agent", "session");

        let (opened, actor_opened) = with_request_actor_opened(registry.actor_for(&key)).await;
        assert!(opened.is_ok());
        assert!(actor_opened, "the request that opens the actor is marked");

        let (existing, actor_opened) = with_request_actor_opened(registry.actor_for(&key)).await;
        assert!(existing.is_ok());
        assert!(!actor_opened, "a request reusing the actor is not marked");
    }

    #[async_trait]
    impl LineageOpener for GaugeOpener {
        async fn open(
            &self,
            _key: &LineageKey,
            gen: u64,
            _registry: RegistryHandle,
        ) -> Result<ActorHandle, OpenError> {
            let (tx, _rx) = mpsc::channel(1);
            Ok(ActorHandle {
                gen,
                tx,
                run_in_flight: Arc::new(AtomicU64::new(0)),
                first_episode: Arc::new(std::sync::OnceLock::new()),
            })
        }
    }

    fn key() -> LineageKey {
        BindIdentity::new("/tmp/x", "runner", "s1")
    }

    #[test]
    fn slot_counts_report_each_slot_state() {
        let reg = LineageRegistry::new(Arc::new(NeverOpener));
        let open_key = BindIdentity::new("/tmp/x", "runner", "open");
        let opening_key = BindIdentity::new("/tmp/x", "runner", "opening");
        let closing_key = BindIdentity::new("/tmp/x", "runner", "closing");
        let open_gen = reg.inner.next_gen.fetch_add(1, Ordering::SeqCst);
        let (ready, _ready_rx) = broadcast::channel(1);
        let (done, _done_rx) = broadcast::channel(1);
        let (dummy_tx, _rx) = mpsc::channel(1);

        let mut slots = reg.inner.slots.lock().unwrap();
        slots.insert(opening_key, Entry::Opening { gen: 1, ready });
        slots.insert(
            open_key,
            Entry::Open {
                gen: open_gen,
                handle: ActorHandle {
                    gen: open_gen,
                    tx: dummy_tx,
                    run_in_flight: Arc::new(AtomicU64::new(0)),
                    first_episode: Arc::new(std::sync::OnceLock::new()),
                },
            },
        );
        slots.insert(closing_key, Entry::Closing { gen: 3, done });
        drop(slots);

        assert_eq!(
            reg.slot_counts(),
            SlotCounts {
                open: 1,
                opening: 1,
                closing: 1,
                runs_in_flight: 0,
                batches_in_flight: 0,
            }
        );
    }

    #[test]
    fn dropping_an_unresolved_close_token_clears_the_closing_slot() {
        // A panic between begin_close and commit drops the CloseToken without commit/abort.
        // Its Drop must clear the `Closing` slot, or the session stays stuck Closing forever
        // (every future open of it would block on a Closing slot that never resolves).
        let reg = LineageRegistry::new(Arc::new(NeverOpener));
        let k = key();
        let gen = reg.inner.next_gen.fetch_add(1, Ordering::SeqCst);
        // Seed an Open slot at `gen`, then begin closing it.
        let (dummy_tx, _rx) = mpsc::channel(1);
        reg.inner.slots.lock().unwrap().insert(
            k.clone(),
            Entry::Open {
                gen,
                handle: ActorHandle {
                    gen,
                    tx: dummy_tx,
                    run_in_flight: Arc::new(AtomicU64::new(0)),
                    first_episode: Arc::new(std::sync::OnceLock::new()),
                },
            },
        );
        let token = reg.handle().begin_close(&k, gen).expect("began close");
        assert_eq!(reg.slot_state(&k), Some("closing"));
        // Drop WITHOUT commit/abort (simulates a panic mid-teardown).
        drop(token);
        assert_eq!(
            reg.slot_state(&k),
            None,
            "dropping an unresolved CloseToken clears the Closing slot (treated as commit)"
        );
    }

    struct LeaseHeldOpener {
        calls: Arc<std::sync::atomic::AtomicU64>,
    }

    #[async_trait]
    impl LineageOpener for LeaseHeldOpener {
        async fn open(
            &self,
            key: &LineageKey,
            _gen: u64,
            _registry: RegistryHandle,
        ) -> Result<ActorHandle, OpenError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(OpenError::LeaseHeld(key.session.clone()))
        }
    }

    #[test]
    fn lease_held_open_error_is_returned_without_retrying() {
        let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let registry = LineageRegistry::new(Arc::new(LeaseHeldOpener {
            calls: calls.clone(),
        }));
        let session = key();
        let registry_for_op = registry.clone();
        let session_for_op = session.clone();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build test runtime");
            let result = runtime
                .block_on(registry_for_op.actor_for(&session_for_op))
                .map(|_| ());
            let _ = result_tx.send(result);
        });

        let error = result_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("a permanent open error must return instead of hot-spinning")
            .expect_err("the held lease must be surfaced");
        worker.join().expect("open-error worker joins");

        assert!(matches!(error, OpenError::LeaseHeld(ref held) if held == "s1"));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "open is not hot-retried");
        assert_eq!(registry.slot_state(&session), None);
    }

    struct GatedOpener {
        park: std::sync::Arc<std::sync::atomic::AtomicBool>,
        release: std::sync::Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl LineageOpener for GatedOpener {
        async fn open(
            &self,
            _key: &LineageKey,
            gen: u64,
            _registry: RegistryHandle,
        ) -> Result<ActorHandle, OpenError> {
            while self.park.load(std::sync::atomic::Ordering::SeqCst) {
                self.release.notified().await;
            }
            let (tx, _rx) = mpsc::channel(1);
            Ok(ActorHandle {
                gen,
                tx,
                run_in_flight: Arc::new(AtomicU64::new(0)),
                first_episode: Arc::new(std::sync::OnceLock::new()),
            })
        }
    }

    #[tokio::test]
    async fn closing_slot_retries_then_reopens() {
        let park = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = Arc::new(tokio::sync::Notify::new());
        let registry = LineageRegistry::new(Arc::new(GatedOpener { park, release }));
        let session = key();
        let (done, _done_rx) = broadcast::channel(1);
        registry.inner.slots.lock().unwrap().insert(
            session.clone(),
            Entry::Closing {
                gen: 1,
                done: done.clone(),
            },
        );

        let registry_for_open = registry.clone();
        let session_for_open = session.clone();
        let open =
            tokio::spawn(async move { registry_for_open.actor_for(&session_for_open).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        registry.inner.slots.lock().unwrap().remove(&session);
        let _ = done.send(());

        let handle = tokio::time::timeout(Duration::from_secs(1), open)
            .await
            .expect("close/reopen retry is bounded")
            .expect("actor_for task joins")
            .expect("transient Closing state recovers");
        assert!(handle.gen > 0);
        assert_eq!(registry.slot_state(&session), Some("open"));
    }

    /// If `actor_for` is aborted mid-open, `OpenGuard` must clear the `Opening` slot so a
    /// later `actor_for` completes (not hang forever). Revert `OpenGuard` to reproduce the hang.
    #[tokio::test]
    async fn opening_abort_clears_slot_and_next_actor_for_completes() {
        let park = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let reg = LineageRegistry::new(Arc::new(GatedOpener {
            park: park.clone(),
            release: release.clone(),
        }));
        let k = key();

        let reg2 = reg.clone();
        let k_open = k.clone();
        let open_task = tokio::spawn(async move { reg2.actor_for(&k_open).await });

        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            if reg.slot_state(&k) == Some("opening") {
                break;
            }
        }
        assert_eq!(reg.slot_state(&k), Some("opening"));

        open_task.abort();
        let _ = open_task.await;

        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            if reg.slot_state(&k).is_none() {
                break;
            }
        }
        assert_eq!(
            reg.slot_state(&k),
            None,
            "Opening slot must be cleared when opener task is aborted"
        );

        park.store(false, std::sync::atomic::Ordering::SeqCst);
        release.notify_waiters();

        let handle = tokio::time::timeout(Duration::from_secs(2), reg.actor_for(&k))
            .await
            .expect("second actor_for must not hang on a stranded Opening slot")
            .expect("second open succeeds");
        assert!(handle.gen > 0);
        assert_eq!(reg.slot_counts().opening, 0);
        assert_eq!(reg.slot_counts().open, 1);
    }

    /// The property an operator relies on: a session that EXISTS is not a session that is
    /// WORKING. If these two ever report the same number the gauge has stopped answering its own
    /// question, and the failure is silent because both readings look reasonable.
    #[tokio::test]
    async fn an_open_but_idle_session_counts_as_open_and_not_in_flight() {
        let registry = LineageRegistry::new(Arc::new(NeverOpener));
        let handle = ActorHandle {
            gen: 1,
            tx: mpsc::channel(1).0,
            run_in_flight: Arc::new(AtomicU64::new(0)),
            first_episode: Arc::new(std::sync::OnceLock::new()),
        };
        registry
            .inner
            .slots
            .lock()
            .unwrap()
            .insert(test_key("idle"), Entry::Open { gen: 1, handle });

        let counts = registry.slot_counts();
        assert_eq!(counts.open, 1, "the session exists");
        assert_eq!(
            counts.runs_in_flight, 0,
            "an idle session must not read as running -- that is the reading a restart decision \
             is made on"
        );
    }

    #[tokio::test]
    async fn a_session_driving_a_run_counts_in_flight() {
        let registry = LineageRegistry::new(Arc::new(NeverOpener));
        let gauge = Arc::new(AtomicU64::new(0));
        let handle = ActorHandle {
            gen: 1,
            tx: mpsc::channel(1).0,
            run_in_flight: Arc::clone(&gauge),
            first_episode: Arc::new(std::sync::OnceLock::new()),
        };
        registry
            .inner
            .slots
            .lock()
            .unwrap()
            .insert(test_key("busy"), Entry::Open { gen: 1, handle });

        assert_eq!(registry.slot_counts().runs_in_flight, 0);
        // The actor owns this write; simulating it here keeps the test on the registry's read.
        gauge.store(1, Ordering::Relaxed);
        assert_eq!(
            registry.slot_counts().runs_in_flight,
            1,
            "the gauge is shared, so the registry sees the actor's transition without asking it"
        );
    }

    fn test_key(name: &str) -> LineageKey {
        BindIdentity::new(std::path::PathBuf::from("/tmp").join(name), "runner", name)
    }
}
