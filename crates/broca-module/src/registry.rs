//! The lineage registry: process-global admission for one actor per session.
//!
//! Every `session.*`/`run.*` op for a session is routed through this registry so a
//! teardown can never race a concurrent op. The registry mutex is held ONLY for the fast
//! state check + slot transition (never across the slow lease-acquire/replay). It admits
//! exactly one `Opening` per session (double-reopen coalesce), tags each `Open` slot with
//! a GENERATION (so a stale handle a racing op grabbed can be detected), and serializes
//! Open↔Closing↔Closed. Actor teardown stops subscribers and the WAL writer before
//! releasing the lease, so a replacement actor cannot overlap the old writer or routes.
//!
//! See `docs/lineage-actor-coordinator.md` §1.

use broca_diagnostics::{spawn_blocking, JobKind};

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_protocol::{BindIdentity, RunId};
use tokio::sync::{broadcast, mpsc};

const CLOSE_REOPEN_MAX_RETRIES: usize = 3;
const CLOSE_REOPEN_BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);
const SESSION_CACHE_SESSIONS: usize = 4096;

/// FIFO cache for session facts that can be recomputed on a miss. Updating a fact does not
/// refresh its insertion age, and targeted removal also releases its eviction bookkeeping.
struct BoundedSessionCache<V> {
    entries: HashMap<LineageKey, V>,
    oldest: VecDeque<LineageKey>,
    capacity: usize,
}

impl<V> BoundedSessionCache<V> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            entries: HashMap::new(),
            oldest: VecDeque::new(),
            capacity,
        }
    }

    fn get(&self, key: &LineageKey) -> Option<&V> {
        self.entries.get(key)
    }

    fn get_mut(&mut self, key: &LineageKey) -> Option<&mut V> {
        self.entries.get_mut(key)
    }

    fn insert(&mut self, key: LineageKey, value: V) -> Option<V> {
        if let Some(entry) = self.entries.get_mut(&key) {
            return Some(std::mem::replace(entry, value));
        }
        if self.entries.len() >= self.capacity {
            let oldest = self
                .oldest
                .pop_front()
                .expect("full cache has an oldest key");
            self.entries.remove(&oldest);
        }
        self.oldest.push_back(key.clone());
        self.entries.insert(key, value)
    }

    fn remove(&mut self, key: &LineageKey) {
        if self.entries.remove(key).is_some() {
            self.oldest.retain(|queued| queued != key);
        }
    }
}

static SESSION_READS: AtomicU64 = AtomicU64::new(0);
static ACTOR_OPENS: AtomicU64 = AtomicU64::new(0);

/// `session.read` requests served by this process since boot.
///
/// This process-local lifetime count resets on restart; it is not a fleet total. Diff two
/// readings to derive a rate.
pub fn session_reads() -> u64 {
    SESSION_READS.load(Ordering::Relaxed)
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

/// The last `RunStarted` in `records`, which opens the latest episode among them. A resumed
/// run continues its episode without writing a new `RunStarted`, so the last one is the
/// current episode's.
fn latest_run_started_tools(records: &[broca_wal::FramedRecord]) -> Option<SessionReadTools> {
    records
        .iter()
        .rev()
        .find_map(|framed| match &framed.record {
            broca_wal::WalRecord::RunStarted { run_id, config, .. } => Some(SessionReadTools {
                run_id: run_id.clone(),
                tools: config.tools.clone(),
            }),
            _ => None,
        })
}

pub(crate) fn budget_firing_needs_full_replay(
    records: &[broca_wal::FramedRecord],
    materialized_prefix_len: u64,
) -> bool {
    if materialized_prefix_len > 0
        && records
            .iter()
            .any(|framed| matches!(framed.record, broca_wal::WalRecord::BudgetFired { .. }))
    {
        return true;
    }
    let mut has_current_run_config = false;
    for framed in records {
        match &framed.record {
            broca_wal::WalRecord::RunStarted { .. } => has_current_run_config = true,
            broca_wal::WalRecord::BudgetFired { .. } if !has_current_run_config => return true,
            _ => {}
        }
    }
    false
}

/// The session identity a lineage is keyed by — the same triple the lease + WAL + store
/// partition use, so one identity covers the whole session.
pub type LineageKey = BindIdentity;

/// Why opening a lineage failed (surfaced to every coalescing waiter, never swallowed).
#[derive(Debug, Clone)]
pub enum OpenError {
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: String,
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
    /// A read failure whose typed cause is still available.
    Classified {
        reason: String,
        class: broca_protocol::ErrorClass,
    },
    /// The session's WAL holds a complete frame that requires WAL features this build does not
    /// know (`missing`): a newer broca wrote it. Kept apart from [`OpenError::Open`] because the
    /// remedy differs: retrying cannot help, only placing a build that knows the features can.
    /// `reason` is the exact text [`OpenError::Open`] would have carried, so the rendered
    /// message is unchanged.
    NewerWriter {
        missing: Vec<String>,
        reason: String,
    },
}

impl OpenError {
    /// Classify a WAL read failure, keeping a newer-writer refusal typed. `reason` is the full
    /// text to report, already including the failure's own rendering.
    pub(crate) fn from_wal_read(reason: String, error: &broca_wal::WalError) -> Self {
        match error {
            broca_wal::WalError::SessionMoving { cut_id } => Self::SessionMoving {
                cut_id: cut_id.clone(),
            },
            broca_wal::WalError::SessionMoved { holder } => Self::SessionMoved {
                holder: holder.clone(),
            },
            broca_wal::WalError::NewerWriter { missing, .. } => OpenError::NewerWriter {
                missing: missing.clone(),
                reason,
            },
            // Only temporary OS errors establish that the same read may succeed on retry.
            broca_wal::WalError::Io(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                ) =>
            {
                OpenError::Classified {
                    reason,
                    class: broca_protocol::ErrorClass::Transient,
                }
            }
            // Corruption, replay/shape errors and opaque failures are not presumed retryable.
            _ => OpenError::Open(reason),
        }
    }
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionMoving { cut_id } => write!(f, "session_moving {{cutId: {cut_id}}}"),
            Self::SessionMoved { holder } => write!(f, "session_moved {{holder: {holder}}}"),
            OpenError::Expired { expired_at_ms } => write!(f, "lineage expired at {expired_at_ms}"),
            OpenError::RetentionInvalid(_) => write!(f, "invalid retention"),
            OpenError::SessionClassInvalid(message) => f.write_str(message),
            OpenError::LeaseHeld(s) => write!(f, "session '{s}' held by a live writer"),
            OpenError::Open(s)
            | OpenError::Classified { reason: s, .. }
            | OpenError::NewerWriter { reason: s, .. } => {
                write!(f, "open failed: {s}")
            }
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
    /// Shared with the registry's read-only path while this handle occupies an Open slot.
    pub(crate) durable_head: Arc<AtomicU64>,
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
    pub(crate) approvals: Arc<crate::approval_state::ApprovalState>,
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

/// If `actor_for`'s opener task is cancelled or panics before committing the `Opening` slot,
/// this guard removes the stranded `Opening` entry and wakes waiters with a typed error.
/// Commit = successful transition to `Open` (or explicit failure path that clears the slot).
/// Stack storage for one lineage id's canonical text, so the common (matching) read path can
/// compare against the stored id without allocating.
///
/// A `WalLineageId` renders as a 36-byte UUID and nothing else, so the buffer is exact rather
/// than a guess. `format` returns a borrowed `&str` deliberately: a caller cannot keep the text
/// past this frame, which is what stops it drifting back into a heap value.
struct LineageIdText {
    buf: [u8; Self::LEN],
    len: usize,
}

impl LineageIdText {
    const LEN: usize = 36;

    fn new() -> Self {
        Self {
            buf: [0u8; Self::LEN],
            len: 0,
        }
    }

    fn format(&mut self, lineage_id: broca_wal::WalLineageId) -> &str {
        use std::fmt::Write as _;
        self.len = 0;
        // Display is the single source of the canonical spelling; writing through it keeps this
        // in step with the WAL's own rendering instead of duplicating the layout here.
        let _ = write!(self, "{lineage_id}");
        std::str::from_utf8(&self.buf[..self.len]).unwrap_or("")
    }
}

impl std::fmt::Write for LineageIdText {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let end = self.len + s.len();
        // A lineage id cannot exceed LEN, so an overflow here means the rendering changed shape.
        // Refusing rather than truncating keeps a silently-shortened id from comparing unequal
        // and triggering a spurious full-WAL reread.
        if end > Self::LEN {
            return Err(std::fmt::Error);
        }
        self.buf[self.len..end].copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
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
    fn check_move(&self, _key: &LineageKey, _write: bool) -> Result<(), OpenError> {
        Ok(())
    }
    /// Best-effort repair through the session's writer fence. The range is already durable;
    /// implementations must not open or modify a WAL while materializing it.
    async fn backfill_attribution(
        &self,
        _key: &LineageKey,
        _range: broca_wal::WalRangeRead,
    ) -> Result<(), String> {
        Ok(())
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
    gates: Vec<tokio::sync::OwnedMutexGuard<()>>,
    actors: Vec<tokio::sync::oneshot::Sender<()>>,
    pub busy: Vec<(LineageKey, Option<String>)>,
}

struct Inner {
    cut_gates: Mutex<HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    /// Migration notices are deduplicated while the session remains in the bounded cache.
    attribution_notices: Mutex<BoundedSessionCache<()>>,
    retention: std::sync::OnceLock<Arc<crate::retention::RetentionService>>,
    #[cfg(test)]
    close_started: tokio::sync::watch::Sender<u64>,
    #[cfg(test)]
    expiry_read_probe: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(all(test, feature = "test-clock"))]
    status_open_probe: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    repairs: crate::retention::Repairs,
    /// The admitter for sends carrying a fetch plan: it fetches from every provider the plan
    /// names and builds the manifest to freeze. Unset, a send carrying a plan is refused.
    plan_admitter: std::sync::OnceLock<Arc<dyn broca_fetch_plan::PlanAdmitter>>,
    slots: Mutex<HashMap<LineageKey, Entry>>,
    next_gen: AtomicU64,
    opener: Arc<dyn LineageOpener>,
    reader: Option<ReadSources>,
    /// No-actor reads retain a file-identity-scoped proof so transcript pagination does not fsync
    /// again while every requested frame remains within the already-proven durable prefix.
    read_proofs: Mutex<BoundedSessionCache<ReadProof>>,
    /// Dispatch facts of committed tool turns already worked out, per session; see
    /// [`DispatchCache`].
    dispatch_cache: Mutex<HashMap<LineageKey, DispatchCache>>,
    /// Immutable first senders learned from the admission index or durable WAL. Empty sessions
    /// are never cached, since their next write may freeze a sender.
    frozen_senders: Mutex<BoundedSessionCache<Option<broca_protocol::BindPrincipal>>>,
    /// Each actor opening has a new generation. Its ownership is current only while both
    /// that opening and its last fsynced WAL sequence still match the published record.
    live_ownership: Mutex<HashMap<LineageKey, LiveOwnership>>,
    /// Cache only identities and the position after their represented WAL records. Prompts,
    /// queued requests and plan manifests stay out of this bounded, read-only cache.
    owner_snapshots: Mutex<OwnerSnapshots>,
    cap_store: Option<Arc<dyn broca_core::CapStore>>,
    stopping: tokio_util::sync::CancellationToken,
    seal: tokio_util::sync::CancellationToken,
    seal_state: Mutex<SealState>,
    shutdown_report: Mutex<Option<ShutdownReport>>,
    #[cfg(test)]
    actor_panics: AtomicU64,
    restart_resume_ready: AtomicBool,
    /// Sessions the startup crash-orphan pass marked, each with the run ids the run index
    /// still calls active there. The session's actor takes its mark when it opens, whoever
    /// opened it (the pass or a caller's send), and decides from its own WAL whether to seal.
    crash_orphan_marks: Mutex<HashMap<LineageKey, HashSet<RunId>>>,
    /// Runs actors have sealed `Interrupted` because of a crash-orphan mark.
    crash_orphans_sealed: AtomicU64,
    /// Marked runs whose WAL already held a terminal but whose index row still read active,
    /// brought up to that terminal instead of being sealed.
    crash_orphans_index_repaired: AtomicU64,
    /// Every session's prompt-cache facts. Kept here rather than in the actor so they outlive
    /// the actor's idle close: a caller warms a session minutes after its last round.
    cache: crate::cache::CacheTable,
}

/// What one startup crash-orphan pass found and did. Every candidate session lands in exactly
/// one of `marked`, `not_live`, `written_since_boot`, or `unreadable`; every marked session the
/// pass then opened lands in `lease_held` or `open_failed` if the open did not succeed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CrashOrphanSweep {
    /// Runs the run index listed as active.
    pub candidate_runs: usize,
    /// Sessions whose live WAL predates this process, marked for the actor to confirm.
    pub marked: usize,
    /// Sessions with no live WAL: archived (or never written). Not extracted to check.
    pub not_live: usize,
    /// Sessions whose WAL was written after this process started, so a live writer (this
    /// process or another) owns their state and nothing here is a crash's leftover.
    pub written_since_boot: usize,
    /// Sessions whose WAL could not be inspected.
    pub unreadable: usize,
    /// Marked sessions another live process holds the lease for. Skipped, never stolen.
    pub lease_held: usize,
    /// Marked sessions whose actor could not be opened for another reason.
    pub open_failed: usize,
    /// Runs sealed `Interrupted`, whichever open (this pass or a caller) took the mark.
    pub sealed: u64,
    /// Runs whose WAL already ended them but whose index row still read active (the index
    /// write after the terminal was lost). The row is written from the WAL's own terminal;
    /// nothing is sealed, so these are not counted in `sealed`.
    pub index_repaired: u64,
}

#[derive(Clone, Copy)]
struct ReadProof {
    file_identity: u64,
    proven_seq: u64,
}

struct LiveOwnership {
    generation: u64,
    head: u64,
    ownership: broca_core::SessionOwnershipV1,
}

const MAX_OWNER_SNAPSHOTS: usize = 4_096;

#[derive(Clone)]
struct OwnershipSnapshot {
    ownership: broca_core::SessionOwnershipV1,
    watermark_seq: u64,
    anchor: Option<broca_wal::WalByteAnchor>,
    lineage_id: Option<String>,
}

impl OwnershipSnapshot {
    fn from_store(snapshot: broca_core::AdmissionStoreSnapshot) -> Option<Self> {
        let checkpoint = snapshot.checkpoint?;
        if checkpoint.at_seq != snapshot.watermark_seq {
            return None;
        }
        Some(Self {
            ownership: checkpoint.ownership?.for_owner_access(),
            watermark_seq: snapshot.watermark_seq,
            anchor: snapshot.anchor,
            lineage_id: snapshot.lineage_id,
        })
    }
}

#[derive(Default)]
struct OwnerSnapshots {
    entries: HashMap<LineageKey, OwnershipSnapshot>,
    oldest: std::collections::VecDeque<LineageKey>,
}

impl OwnerSnapshots {
    fn get(&self, key: &LineageKey) -> Option<&OwnershipSnapshot> {
        self.entries.get(key)
    }

    fn insert(&mut self, key: LineageKey, mut snapshot: OwnershipSnapshot) {
        snapshot.ownership = snapshot.ownership.for_owner_access();
        if !self.entries.contains_key(&key) {
            if self.entries.len() == MAX_OWNER_SNAPSHOTS {
                if let Some(oldest) = self.oldest.pop_front() {
                    self.entries.remove(&oldest);
                }
            }
            self.oldest.push_back(key.clone());
        }
        self.entries.insert(key, snapshot);
    }

    fn remove(&mut self, key: &LineageKey) {
        self.entries.remove(key);
        self.oldest.retain(|entry| entry != key);
    }
}

/// The dispatch facts of one lineage's committed tool turns, by ordinal. A committed turn's
/// calls all have durable terminals, so its facts never change once known, and a page holding
/// it can be answered from here without reading its records again.
#[derive(Default)]
struct DispatchCache {
    lineage_id: String,
    by_ordinal: std::collections::BTreeMap<u64, Vec<broca_core::ToolCallDispatch>>,
    /// The transcript length the last whole-WAL pass covered. A message below it with no entry
    /// came from no recorded step (an imported one, say), so asking again would find nothing.
    covered: u64,
}

/// Sessions whose dispatch facts are kept at most; past it the cache starts over.
const DISPATCH_CACHE_SESSIONS: usize = 4096;

/// One `session.read` snapshot: the page, plus what the same snapshot says beyond it.
pub struct RouteReadPage {
    pub page: broca_core::SessionReadPage,
    /// Projection commits only complete tool turns. Any open batch therefore remains in the
    /// WAL records after the snapshot's watermark, which supply this batch's call states.
    pub open_batch: Option<broca_wire::OpenBatchView>,
    /// The tool list of the latest episode, when asked for.
    pub tools: Option<SessionReadTools>,
    /// The dispatch facts of every tool turn the snapshot's WAL tail commits, by ordinal. A
    /// turn already in the materialized prefix is not among them.
    pub dispatches: std::collections::BTreeMap<u64, Vec<broca_core::ToolCallDispatch>>,
    /// When the newest timestamped record of the snapshot's WAL tail was written, in
    /// milliseconds since the Unix epoch.
    pub newest_record_ms: Option<u64>,
    /// For retained lineages, the durable activity timestamp that sets expiry. Maintenance
    /// records do not advance it. Retained content is WAL-only, so its tail holds the policy.
    pub retention_activity_ms: Option<u64>,
    /// The session's newest caller title: the newest one in the snapshot's WAL tail, else the
    /// one the store's materialized prefix holds. `None` when no send recorded one.
    pub last_title: Option<String>,
}

enum RoutedPage {
    Raw(Box<RouteReadPage>),
    Model(broca_core::ModelPage),
}

#[derive(Clone)]
struct ReadSources {
    wal_store: Arc<dyn broca_core::WalStore>,
    store: Option<Arc<dyn broca_core::MaterializedStore>>,
}

#[derive(Clone, Copy)]
enum ReadPurpose {
    Session,
    Authorization,
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
}

/// A handle the actor uses to transition ITS OWN slot during close/finalize. Cloneable,
/// cheap; it never holds the registry lock across an await.
#[derive(Clone)]
pub struct RegistryHandle {
    inner: Arc<Inner>,
}

impl LineageRegistry {
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
    pub async fn route_run_event(
        &self,
        session: &LineageKey,
        event_key: &str,
    ) -> Result<broca_wire::RunEventGetResponse, crate::serve::ServeError> {
        let Some(store) = self.inner.reader.as_ref().and_then(|r| r.store.clone()) else {
            return Ok(broca_wire::RunEventGetResponse::Absent);
        };
        let event_key = event_key.to_owned();
        let key = event_key.clone();
        let row =
            broca_diagnostics::spawn_blocking(broca_diagnostics::JobKind::StoreRead, move || {
                store.read_run_event(&key)
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
        let reference = cortexkit_role_llm_runner::session_ref(
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

    /// Prevent opening absent actors and pause existing actors, in contract ref
    /// order. The returned guard remains held through the durable cut completion.
    pub async fn lock_move_sessions(
        &self,
        sessions: &[LineageKey],
        reject_open: bool,
    ) -> Result<MoveSessionGuard, OpenError> {
        let mut sorted = sessions.to_vec();
        sorted.sort_by_key(|key| {
            cortexkit_role_llm_runner::session_ref(
                &key.harness,
                &key.project_root.to_string_lossy(),
                &key.session,
            )
        });
        sorted.dedup_by(|a, b| {
            cortexkit_role_llm_runner::session_ref(
                &a.harness,
                &a.project_root.to_string_lossy(),
                &a.session,
            ) == cortexkit_role_llm_runner::session_ref(
                &b.harness,
                &b.project_root.to_string_lossy(),
                &b.session,
            )
        });
        let mut guard = MoveSessionGuard {
            gates: Vec::new(),
            actors: Vec::new(),
            busy: Vec::new(),
        };
        for key in sorted {
            guard.gates.push(self.cut_gate(&key).lock_owned().await);
            let handle = {
                let slots = self.inner.slots.lock().unwrap();
                if let Some((bound, _)) = slots.get_key_value(&key) {
                    if cortexkit_role_llm_runner::session_ref(
                        &bound.harness,
                        &bound.project_root.to_string_lossy(),
                        &bound.session,
                    ) != cortexkit_role_llm_runner::session_ref(
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
                    .send(crate::actor::LineageCmd::CutLock { reply })
                    .await
                    .map_err(|_| OpenError::Open("actor closed during cut lock".into()))?;
                let (busy, release) = wait
                    .await
                    .map_err(|_| OpenError::Open("actor closed during cut lock".into()))?;
                guard.busy.push((key, busy));
                guard.actors.push(release);
            }
        }
        Ok(guard)
    }
    pub fn check_move(&self, key: &LineageKey, write: bool) -> Result<(), OpenError> {
        self.inner.opener.check_move(key, write)?;
        if let Some(reader) = &self.inner.reader {
            reader
                .wal_store
                .check_move(key, write)
                .map_err(|e| OpenError::from_wal_read(e.to_string(), &e))?;
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn actor_panic_count(&self) -> u64 {
        self.inner.actor_panics.load(Ordering::SeqCst)
    }

    pub fn compaction_supported(&self) -> bool {
        self.inner.opener.compaction_supported()
    }
    pub fn backup_classes(&self) -> Vec<String> {
        self.inner.opener.backup_classes()
    }

    pub fn new(opener: Arc<dyn LineageOpener>) -> Self {
        Self::new_with_reader_and_caps(opener, None, None, None)
    }

    /// Build a registry that can serve read-only transcript pages without opening an actor.
    pub fn new_with_reader(
        opener: Arc<dyn LineageOpener>,
        wal_store: Arc<dyn broca_core::WalStore>,
        store: Option<Arc<dyn broca_core::MaterializedStore>>,
    ) -> Self {
        Self::new_with_reader_and_caps(opener, Some(wal_store), store, None)
    }

    /// Build a registry with the optional read-only sources and daemon-scoped cap table.
    pub fn new_with_reader_and_caps(
        opener: Arc<dyn LineageOpener>,
        wal_store: Option<Arc<dyn broca_core::WalStore>>,
        store: Option<Arc<dyn broca_core::MaterializedStore>>,
        cap_store: Option<Arc<dyn broca_core::CapStore>>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                attribution_notices: Mutex::new(BoundedSessionCache::new(SESSION_CACHE_SESSIONS)),
                retention: std::sync::OnceLock::new(),
                #[cfg(test)]
                close_started: tokio::sync::watch::channel(0).0,
                #[cfg(test)]
                expiry_read_probe: Mutex::new(None),
                #[cfg(all(test, feature = "test-clock"))]
                status_open_probe: Mutex::new(None),
                repairs: Default::default(),
                plan_admitter: std::sync::OnceLock::new(),
                slots: Mutex::new(HashMap::new()),
                cut_gates: Mutex::new(HashMap::new()),
                next_gen: AtomicU64::new(1),
                opener,
                reader: wal_store.map(|wal_store| ReadSources { wal_store, store }),
                read_proofs: Mutex::new(BoundedSessionCache::new(SESSION_CACHE_SESSIONS)),
                dispatch_cache: Mutex::new(HashMap::new()),
                frozen_senders: Mutex::new(BoundedSessionCache::new(SESSION_CACHE_SESSIONS)),
                live_ownership: Mutex::new(HashMap::new()),
                owner_snapshots: Mutex::new(OwnerSnapshots::default()),
                cap_store,
                stopping: tokio_util::sync::CancellationToken::new(),
                seal: tokio_util::sync::CancellationToken::new(),
                seal_state: Mutex::new(SealState::default()),
                shutdown_report: Mutex::new(None),
                #[cfg(test)]
                actor_panics: AtomicU64::new(0),
                restart_resume_ready: AtomicBool::new(false),
                crash_orphan_marks: Mutex::new(HashMap::new()),
                crash_orphans_sealed: AtomicU64::new(0),
                crash_orphans_index_repaired: AtomicU64::new(0),
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
        use crate::actor::{LineageCmd, WarmOutcome};
        loop {
            self.check_move(key, true)?;
            self.check_expired(key, None).await?;
            let handle = self.actor_for(key).await?;
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::Warm {
                lineage_id: lineage_id.clone(),
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
        if let Some(sender) = self.inner.frozen_senders.lock().unwrap().get(key).cloned() {
            return Ok(Some(sender));
        }
        let reader = self.inner.reader.clone().ok_or_else(|| {
            OpenError::Open("sender authorization reader is not configured".into())
        })?;
        if let Some(store) = reader.store {
            let read_key = key.clone();
            let snapshot = spawn_blocking(JobKind::StoreRead, move || {
                store.read_admission_snapshot(&read_key)
            })
            .await
            .map_err(|error| OpenError::Open(error.to_string()))?
            .map_err(|error| OpenError::Open(error.to_string()))?;
            if let Some(ownership) = snapshot
                .and_then(|snapshot| snapshot.checkpoint)
                .and_then(|checkpoint| checkpoint.ownership)
            {
                if ownership.episodes > 0 {
                    let sender = ownership.first_principal;
                    self.inner
                        .frozen_senders
                        .lock()
                        .unwrap()
                        .insert(key.clone(), sender.clone());
                    return Ok(Some(sender));
                }
            }
        }
        let (records, _) = self
            .read_durable_wal_for(key, ReadPurpose::Authorization)
            .await?;
        let sender = records.iter().find_map(|frame| match &frame.record {
            broca_wal::WalRecord::RunStarted { principal, .. }
            | broca_wal::WalRecord::Queued { principal, .. } => Some(principal.clone()),
            _ => None,
        });
        if let Some(sender) = &sender {
            self.inner
                .frozen_senders
                .lock()
                .unwrap()
                .insert(key.clone(), sender.clone());
        }
        Ok(sender)
    }

    /// Cancel/retract ownership from a live reader, a checkpoint plus its durable suffix, or
    /// the complete WAL when no checkpoint is usable. Never open an actor to obtain it.
    pub(crate) async fn owner_ownership(
        &self,
        key: &LineageKey,
    ) -> Result<broca_core::SessionOwnershipV1, OpenError> {
        self.check_authorization_expired(key).await?;
        let live = {
            let slots = self.inner.slots.lock().unwrap();
            match slots.get(key) {
                Some(Entry::Open { handle, .. }) => {
                    Some((handle.gen, handle.durable_head.load(Ordering::Acquire)))
                }
                _ => None,
            }
        };
        if let Some((generation, head)) = live {
            if let Some(cached) = self.inner.live_ownership.lock().unwrap().get(key) {
                if cached.generation == generation && cached.head == head {
                    return Ok(cached.ownership.clone());
                }
            }
        }
        let reader = self.inner.reader.as_ref().ok_or_else(|| {
            OpenError::Open("owner authorization reader is not configured".into())
        })?;
        let live_head = live.map(|(_, head)| head);
        let cached = self.inner.owner_snapshots.lock().unwrap().get(key).cloned();
        if let Some(snapshot) = cached {
            if let Some((current, head)) = self
                .owner_snapshot_at_head(reader, key, &snapshot, live_head)
                .await?
            {
                return Ok(self.remember_owner_snapshot(key, snapshot, &current, head, live));
            }
        }
        if let Some(store) = reader.store.clone() {
            let read_key = key.clone();
            let snapshot = spawn_blocking(JobKind::StoreRead, move || {
                store.read_admission_snapshot(&read_key)
            })
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
            .and_then(OwnershipSnapshot::from_store);
            if let Some(snapshot) = snapshot {
                if let Some((current, head)) = self
                    .owner_snapshot_at_head(reader, key, &snapshot, live_head)
                    .await?
                {
                    return Ok(self.remember_owner_snapshot(key, snapshot, &current, head, live));
                }
            }
        }
        // Admission checkpoints contain prompts and queued requests as well as ownership.
        // Finite-retention sessions must not copy that content into the persistent store,
        // so their owner lookup reads the WAL. A failed store read needs the same fallback,
        // not an assumption that no owner was recorded.
        let range = self
            .read_durable_range_for(reader, key, 0, None, live_head, ReadPurpose::Authorization)
            .await?;
        let records = crate::durable_read::DurableRead::classify(&range, 0, live_head)
            .records()
            .map_err(|reason| OpenError::Classified {
                reason: format!("owner authorization: {reason}"),
                class: broca_protocol::ErrorClass::Transient,
            })?;
        let mut ownership = broca_core::SessionOwnershipV1::default();
        ownership.fold_owner_records(records);
        let head = records.last().map_or(0, |record| record.seq);
        // An anchor at EOF cannot validate a following frame and makes the range reader
        // scan from zero. Retain a checkpoint before the last frame so repeated checks have
        // a real suffix to validate, even when no materialized snapshot exists.
        let prefix = &records[..records.len().saturating_sub(1)];
        let mut base = broca_core::SessionOwnershipV1::default();
        base.fold_owner_records(prefix);
        let watermark_seq = prefix.last().map_or(0, |record| record.seq);
        let snapshot = OwnershipSnapshot {
            watermark_seq,
            lineage_id: range.lineage_id.map(|id| id.to_string()),
            anchor: range.anchor_at(watermark_seq),
            ownership: base,
        };
        Ok(self.remember_owner_snapshot(key, snapshot, &ownership, head, live))
    }

    async fn owner_snapshot_at_head(
        &self,
        reader: &ReadSources,
        key: &LineageKey,
        snapshot: &OwnershipSnapshot,
        live_head: Option<u64>,
    ) -> Result<Option<(broca_core::SessionOwnershipV1, u64)>, OpenError> {
        if live_head.is_some_and(|head| snapshot.watermark_seq > head) {
            return Ok(None);
        }
        let range = match self
            .read_durable_range_for(
                reader,
                key,
                snapshot.watermark_seq,
                snapshot.anchor,
                live_head,
                ReadPurpose::Authorization,
            )
            .await
        {
            Ok(range) => range,
            Err(_) => return Ok(None),
        };
        if snapshot.lineage_id.as_deref() != range.lineage_id.map(|id| id.to_string()).as_deref()
            || snapshot.lineage_id.is_none()
        {
            return Ok(None);
        }
        let Ok(records) =
            crate::durable_read::DurableRead::classify(&range, snapshot.watermark_seq, live_head)
                .records()
        else {
            return Ok(None);
        };
        let head = records
            .last()
            .map_or(snapshot.watermark_seq, |record| record.seq);
        if live_head.is_some_and(|live| head != live) {
            return Ok(None);
        }
        let mut ownership = snapshot.ownership.clone();
        ownership.fold_owner_records(records);
        Ok(Some((ownership, head)))
    }

    fn remember_owner_snapshot(
        &self,
        key: &LineageKey,
        snapshot: OwnershipSnapshot,
        current: &broca_core::SessionOwnershipV1,
        head: u64,
        live: Option<(u64, u64)>,
    ) -> broca_core::SessionOwnershipV1 {
        let ownership = current.for_owner_access();
        if let Some((generation, live_head)) = live {
            if head == live_head {
                self.handle()
                    .note_owner_identity(key, generation, head, current);
            }
        }
        if ownership.episodes > 0 {
            self.inner
                .owner_snapshots
                .lock()
                .unwrap()
                .insert(key.clone(), snapshot);
        }
        ownership
    }

    /// The ownership summary of a session's durable history, read without opening its actor,
    /// for the adoption check at `route.bind`. `None` when this registry has no read-only source
    /// or the history cannot be read: the bind then goes ahead, and the same check runs
    /// authoritatively on the first send.
    pub async fn session_ownership_for_bind(
        &self,
        key: &LineageKey,
    ) -> Option<broca_core::SessionOwnershipV1> {
        if matches!(
            self.check_expired(key, None).await,
            Err(OpenError::Expired { .. })
        ) {
            return Some(broca_core::SessionOwnershipV1::default());
        }
        let sources = self.inner.reader.clone()?;
        let key = key.clone();
        let folded = spawn_blocking(JobKind::WalRead, move || {
            let records = match sources.wal_store.read_records(&key) {
                Ok(records) => records,
                Err(broca_wal::WalError::Io(error))
                    if error.kind() == std::io::ErrorKind::NotFound =>
                {
                    let meta = sources
                        .store
                        .as_ref()
                        .map(|store| store.read_admission_snapshot(&key))
                        .transpose()?
                        .flatten();
                    if !sources
                        .wal_store
                        .lineage_exists(&key, meta.as_ref())
                        .map_err(|e| e.to_string())?
                    {
                        let mut empty = broca_core::AdmissionCheckpointV1::from_frames(&[])
                            .map_err(|e| e.to_string())?;
                        empty.ownership = Some(broca_core::SessionOwnershipV1::default());
                        return Ok(empty);
                    }
                    return Err("session ownership history is missing".into());
                }
                Err(error) => return Err(error.to_string()),
            };
            if records.is_empty() {
                let meta = sources
                    .store
                    .as_ref()
                    .map(|store| store.read_admission_snapshot(&key))
                    .transpose()?
                    .flatten();
                if meta.as_ref().is_some_and(|meta| {
                    meta.watermark_seq > 0
                        || meta.checkpoint.as_ref().is_some_and(|checkpoint| {
                            checkpoint.last_episode.is_some()
                                || checkpoint
                                    .ownership
                                    .as_ref()
                                    .is_some_and(|ownership| ownership.episodes > 0)
                        })
                }) {
                    return Err("empty session WAL contradicts materialized history".into());
                }
            }
            broca_core::AdmissionCheckpointV1::from_frames(&records)
                .map_err(|error| error.to_string())
        })
        .await;
        match folded {
            Ok(Ok(checkpoint)) => checkpoint.ownership,
            Ok(Err(error)) => {
                tracing::warn!(
                    target: "scope",
                    "route.bind adoption check skipped, history unreadable: {error}"
                );
                None
            }
            Err(error) => {
                tracing::warn!(target: "scope", "route.bind adoption check task failed: {error}");
                None
            }
        }
    }

    pub(crate) fn handle(&self) -> RegistryHandle {
        RegistryHandle {
            inner: self.inner.clone(),
        }
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

    async fn actor_for_mode(
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
                return Err(OpenError::Open("module is stopping".into()));
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
                    ACTOR_OPENS.fetch_add(1, Ordering::Relaxed);
                    #[cfg(all(test, feature = "test-clock"))]
                    if status {
                        let probe = self.inner.status_open_probe.lock().unwrap().take();
                        if let Some(probe) = probe {
                            probe();
                        }
                    }
                    let result = if status {
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
                    };
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
                    let _ = ready.send(result.clone());
                    open_guard.commit();
                    return result;
                }
            }
        }
    }

    /// Route an already-concrete run request. Internal callers use this compatibility helper;
    /// a concrete `RunRequest` always represents an explicit tool surface.
    pub async fn route_send(
        &self,
        key: &LineageKey,
        req: broca_core::RunRequest,
        kind: crate::executor::RunKind,
    ) -> Result<crate::actor::SendOutcome, OpenError> {
        let run_id = req.run_id.clone();
        // Internal callers provide a stable explicit run id and use this helper's retry semantics.
        self.route_submission_with_class(
            key,
            run_id,
            req.to_submission(),
            kind,
            crate::actor::SendKey::RunId,
            false,
            broca_protocol::Delivery::Queue,
            None,
            None,
            req.title.clone(),
            req.retention,
            req.session_class.clone(),
            false,
        )
        .await
    }

    /// Route a `session.send` payload while preserving whether its tool surface was omitted.
    /// A `Reopen` reply (the actor was closing) or a closed channel retries through the
    /// registry, so a send racing a close never lands on a draining actor.
    pub async fn route_submission(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
        request: broca_protocol::request::SubmissionRequest,
        kind: crate::executor::RunKind,
        idempotency_keyed: bool,
    ) -> Result<crate::actor::SendOutcome, OpenError> {
        self.route_submission_with_materialization(
            key,
            run_id,
            request,
            kind,
            crate::actor::SendKey::from_keyed(idempotency_keyed),
            false,
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
        loop {
            let handle = self
                .actor_for_mode(key, retention, session_class.clone(), false)
                .await?;
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
            let handle = self.actor_for(key).await?;
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

    /// Apply a cap-table mutation without opening a session actor or touching a session WAL.
    pub async fn install_cap(
        &self,
        request: broca_core::CapInstallRequest,
    ) -> Result<broca_core::CapMutation, broca_core::CapStoreError> {
        let Some(store) = self.inner.cap_store.clone() else {
            return Err(broca_core::CapStoreError::Storage(
                "cap table is not configured".into(),
            ));
        };
        spawn_blocking(JobKind::StoreWrite, move || store.install_cap(&request))
            .await
            .map_err(|error| broca_core::CapStoreError::Storage(error.to_string()))?
    }

    /// Apply a spend delta without opening a session actor or touching a session WAL.
    pub async fn apply_spend_delta(
        &self,
        request: broca_core::SpendDeltaRequest,
    ) -> Result<broca_core::CapMutation, broca_core::CapStoreError> {
        let Some(store) = self.inner.cap_store.clone() else {
            return Err(broca_core::CapStoreError::Storage(
                "cap table is not configured".into(),
            ));
        };
        spawn_blocking(JobKind::StoreWrite, move || {
            store.apply_spend_delta(&request)
        })
        .await
        .map_err(|error| broca_core::CapStoreError::Storage(error.to_string()))?
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

    pub async fn route_cancel_at(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
        at: broca_wal::record::RunCancelAt,
    ) -> Result<crate::actor::CancelOutcome, OpenError> {
        use crate::actor::{CancelOutcome, LineageCmd};
        loop {
            self.check_move(key, true)?;
            self.check_expired(key, Some(&run_id.0)).await?;
            let handle = self.actor_for(key).await?;
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::Cancel {
                run_id: run_id.clone(),
                at,
                reply,
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
            let handle = self.actor_for(key).await?;
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
            let handle = self.actor_for(key).await?;
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

    /// Resolve a specific run's lifecycle state without reopening a closed session whose WAL
    /// already records its terminal or pause. Otherwise use the actor's live state, durable
    /// index and WAL reconciliation. `key` is the session the run belongs to.
    /// `None` = unknown run_id for this session.
    pub async fn route_run_state(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<Option<broca_core::ReconciledRunState>, OpenError> {
        self.route_run_state_with_wal(key, run_id)
            .await
            .map(|(state, _)| state)
    }

    /// Resolve run state and retain any authoritative WAL snapshot used to answer it.
    /// Result extraction can reuse that snapshot. Actor-backed answers carry no snapshot,
    /// so callers must still check the durable WAL for a just-written terminal.
    pub(crate) async fn route_run_state_with_wal(
        &self,
        key: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<
        (
            Option<broca_core::ReconciledRunState>,
            Option<Vec<broca_wal::FramedRecord>>,
        ),
        OpenError,
    > {
        self.check_expired(key, Some(&run_id.0)).await?;
        if matches!(
            self.check_move(key, true),
            Err(OpenError::SessionMoving { .. })
        ) {
            let records = self.read_durable_wal(key).await?.0;
            let state = broca_core::reconcile_run_state(
                records.iter().map(|frame| &frame.record),
                &run_id.0,
            );
            return Ok((state, Some(records)));
        }
        use crate::actor::LineageCmd;
        loop {
            let closed = !self.inner.slots.lock().unwrap().contains_key(key);
            if closed && self.inner.reader.is_some() {
                let records = self.read_durable_wal(key).await?.0;
                let state = broca_core::reconcile_run_state(
                    records.iter().map(|frame| &frame.record),
                    &run_id.0,
                );
                if matches!(
                    self.check_move(key, true),
                    Err(OpenError::SessionMoving { .. })
                ) {
                    return Ok((state, Some(records)));
                }
                if let Some(state) = state {
                    if state.state.is_terminal() || state.state == broca_core::RunIndexState::Paused
                    {
                        return Ok((Some(state), Some(records)));
                    }
                }
                // Missing or nonterminal WAL state is not proof of an unknown run: the index
                // may retain a terminal even when its WAL is unavailable. Preserve that fallback.
            }
            let handle = self.actor_for(key).await?;
            let (reply, rx) = tokio::sync::oneshot::channel();
            let cmd = LineageCmd::RunState {
                run_id: run_id.clone(),
                reply,
            };
            if handle.send(cmd).await.is_err() {
                continue;
            }
            match rx.await {
                // A STORAGE failure resolving the run is surfaced as an error (so the wire
                // reports it), never collapsed into "unknown run".
                Ok(Ok(state)) => return Ok((state, None)),
                Ok(Err(e)) => {
                    self.check_expired(key, Some(&run_id.0)).await?;
                    return Err(OpenError::Open(e));
                }
                Err(_) => continue,
            }
        }
    }

    /// Read a WAL suffix bounded by a writer proof or by a read-side flush proof.
    async fn read_durable_range(
        &self,
        reader: &ReadSources,
        key: &LineageKey,
        after_seq: u64,
        anchor: Option<broca_wal::WalByteAnchor>,
        live_head: Option<u64>,
    ) -> Result<broca_wal::WalRangeRead, OpenError> {
        self.read_durable_range_for(
            reader,
            key,
            after_seq,
            anchor,
            live_head,
            ReadPurpose::Session,
        )
        .await
    }

    async fn read_durable_range_for(
        &self,
        reader: &ReadSources,
        key: &LineageKey,
        after_seq: u64,
        anchor: Option<broca_wal::WalByteAnchor>,
        live_head: Option<u64>,
        purpose: ReadPurpose,
    ) -> Result<broca_wal::WalRangeRead, OpenError> {
        let range_read = |through_seq| {
            let wal_store = reader.wal_store.clone();
            let session = key.clone();
            let read_anchor = anchor;
            spawn_blocking(JobKind::WalRead, move || match purpose {
                ReadPurpose::Session => wal_store.read_records_range(
                    &session,
                    after_seq,
                    through_seq,
                    read_anchor.as_ref(),
                ),
                ReadPurpose::Authorization => wal_store.read_records_range_for_authorization(
                    &session,
                    after_seq,
                    through_seq,
                    read_anchor.as_ref(),
                ),
            })
        };

        if let Some(head) = live_head {
            let range = range_read(Some(head))
                .await
                .map_err(|error| OpenError::Open(format!("wal range-read task failed: {error}")))?
                .map_err(|error| {
                    OpenError::from_wal_read(format!("wal range-read failed: {error}"), &error)
                })?;
            // An actor-side head comes only from spawn recovery or a successful writer barrier.
            // Do not cache it here: this reader does not own the writer's file identity, so an
            // external replacement must force the next no-actor read through its own flush proof.
            return Ok(range);
        }

        let identity_before = spawn_blocking(JobKind::WalRead, {
            let wal_store = reader.wal_store.clone();
            let session = key.clone();
            move || match purpose {
                ReadPurpose::Session => wal_store.file_identity(&session),
                ReadPurpose::Authorization => wal_store.file_identity_for_authorization(&session),
            }
        })
        .await
        .ok()
        .and_then(Result::ok)
        .flatten();
        let cached_head =
            identity_before.and_then(|identity| self.cached_read_proof(key, identity));

        // Decode only to discover whether this page would cross the cached proof. These frames
        // remain unservable until the cache covers them or the flush below has succeeded.
        let candidate = range_read(None)
            .await
            .map_err(|error| OpenError::Open(format!("wal range-read task failed: {error}")))?
            .map_err(|error| {
                OpenError::from_wal_read(format!("wal range-read failed: {error}"), &error)
            })?;
        let candidate_head = (match candidate.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Partial(records)
            | broca_wal::WalRangeRecords::Unknown(records) => records,
            broca_wal::WalRangeRecords::Absent => &[],
        })
        .last()
        .map(|record| record.seq);
        let Some(candidate_head) = candidate_head else {
            return Ok(candidate);
        };
        if cached_head.is_some_and(|head| candidate_head <= head) {
            return Ok(candidate);
        }

        // Never use FileWal::open here: it has writer-recovery truncation semantics. This handle
        // has write permission solely because Windows FlushFileBuffers requires it; sync_all is
        // the only operation performed, so a reader cannot alter or truncate the WAL.
        let flush_identity = match spawn_blocking(JobKind::WalRead, {
            let wal_store = reader.wal_store.clone();
            let session = key.clone();
            move || match purpose {
                ReadPurpose::Session => wal_store.flush_for_read(&session),
                ReadPurpose::Authorization => wal_store.flush_for_authorization(&session),
            }
        })
        .await
        {
            Ok(Ok(identity)) => identity,
            Ok(Err(error)) => {
                tracing::warn!(
                    target: "session",
                    "session.read durability flush failed; serving only the materialized prefix: {error}"
                );
                return Ok(broca_wal::WalRangeRead::without_anchors(Vec::new()));
            }
            Err(error) => {
                tracing::warn!(
                    target: "session",
                    "session.read durability flush task failed; serving only the materialized prefix: {error}"
                );
                return Ok(broca_wal::WalRangeRead::without_anchors(Vec::new()));
            }
        };

        // The successful flush proves frames decoded before it. Re-read bounded at that observed
        // head so a later concurrent append cannot be served under this proof.
        let proven = range_read(Some(candidate_head))
            .await
            .map_err(|error| OpenError::Open(format!("wal range-read task failed: {error}")))?
            .map_err(|error| {
                OpenError::from_wal_read(format!("wal range-read failed: {error}"), &error)
            })?;
        // A bounded reread can lose frames even after a successful barrier. Preserve that
        // uncertainty instead of letting full-history consumers infer absence from a short read.
        if matches!(proven.records(), broca_wal::WalRangeRecords::Complete(_))
            && crate::durable_read::DurableRead::classify(&proven, after_seq, Some(candidate_head))
                .records()
                .is_err()
        {
            return Ok(broca_wal::WalRangeRead::without_anchors(Vec::new()));
        }
        let identity_after = spawn_blocking(JobKind::WalRead, {
            let wal_store = reader.wal_store.clone();
            let session = key.clone();
            move || match purpose {
                ReadPurpose::Session => wal_store.file_identity(&session),
                ReadPurpose::Authorization => wal_store.file_identity_for_authorization(&session),
            }
        })
        .await
        .ok()
        .and_then(Result::ok)
        .flatten();
        if flush_identity != identity_after {
            tracing::warn!(
                target: "session",
                "session.read WAL identity changed during the durability barrier; serving only the materialized prefix"
            );
            return Ok(broca_wal::WalRangeRead::without_anchors(Vec::new()));
        }
        if let Some(identity) = flush_identity {
            self.cache_read_proof(key, identity, candidate_head);
        }
        Ok(proven)
    }

    fn cached_read_proof(&self, key: &LineageKey, identity: u64) -> Option<u64> {
        self.inner
            .read_proofs
            .lock()
            .unwrap()
            .get(key)
            .filter(|proof| proof.file_identity == identity)
            .map(|proof| proof.proven_seq)
    }

    /// Keep only a bounded set of durability proofs. A miss repeats the flush-and-identity
    /// barrier in `read_durable_range`; eviction cannot publish an unproven WAL head.
    fn cache_read_proof(&self, key: &LineageKey, identity: u64, proven_seq: u64) {
        let mut proofs = self.inner.read_proofs.lock().unwrap();
        match proofs.get_mut(key) {
            Some(proof) if proof.file_identity == identity => {
                proof.proven_seq = proof.proven_seq.max(proven_seq);
            }
            _ => {
                proofs.insert(
                    key.clone(),
                    ReadProof {
                        file_identity: identity,
                        proven_seq,
                    },
                );
            }
        }
    }

    /// Read a paged durable transcript directly from the store/WAL snapshot. This deliberately
    /// bypasses `actor_for`, so an idle-session read does not acquire the lease or create a
    /// registry slot.
    pub async fn route_read(
        &self,
        key: &LineageKey,
        from_ordinal: Option<u64>,
        limit: Option<u64>,
    ) -> Result<broca_core::SessionReadPage, OpenError> {
        self.route_read_with_tools(key, from_ordinal, limit, false)
            .await
            .map(|(page, _)| page)
    }

    /// The dispatch facts of the tool turns at `ordinals`, from what `tail` (this read's own WAL
    /// tail) already worked out, else from the session's cache. A pass over the whole durable
    /// WAL is paid only for an ordinal that neither holds and that no earlier pass covered (an
    /// earlier pass covers every ordinal below the transcript length it saw). That pass fills
    /// the cache with every committed turn, so later reads of the same turns cost nothing
    /// beyond their page.
    pub async fn tool_turn_dispatches(
        &self,
        key: &LineageKey,
        lineage_id: &str,
        tail: &std::collections::BTreeMap<u64, Vec<broca_core::ToolCallDispatch>>,
        ordinals: &[u64],
        prompt_len: u64,
    ) -> Result<std::collections::BTreeMap<u64, Vec<broca_core::ToolCallDispatch>>, OpenError> {
        let lookup = |cache: &mut HashMap<LineageKey, DispatchCache>| {
            if cache.len() >= DISPATCH_CACHE_SESSIONS && !cache.contains_key(key) {
                cache.clear();
            }
            let entry = cache.entry(key.clone()).or_default();
            if entry.lineage_id != lineage_id {
                *entry = DispatchCache {
                    lineage_id: lineage_id.to_owned(),
                    ..DispatchCache::default()
                };
            }
            // The page's tail can include an open tool turn. Its facts change as results
            // arrive, so only the full replay's committed turns belong in the cache.
            let mut found = std::collections::BTreeMap::new();
            let mut missing = false;
            for ordinal in ordinals {
                match tail.get(ordinal).or_else(|| entry.by_ordinal.get(ordinal)) {
                    Some(facts) => {
                        found.insert(*ordinal, facts.clone());
                    }
                    None if *ordinal < entry.covered => {}
                    None => missing = true,
                }
            }
            (found, missing)
        };
        let (found, missing) = lookup(&mut self.inner.dispatch_cache.lock().unwrap());
        if !missing {
            return Ok(found);
        }
        let (records, wal_lineage) = self.read_durable_wal(key).await?;
        if wal_lineage.map(|lineage| lineage.to_string()).as_deref() != Some(lineage_id) {
            // The lineage moved under this read; answer what the page's own tail knows.
            return Ok(found);
        }
        let all = broca_core::tool_call_dispatches(&records)
            .map_err(|error| OpenError::Open(format!("replay failed: {error}")))?;
        let mut cache = self.inner.dispatch_cache.lock().unwrap();
        let entry = cache.entry(key.clone()).or_default();
        if entry.lineage_id == lineage_id {
            entry.by_ordinal.extend(all);
            entry.covered = entry.covered.max(prompt_len);
        }
        drop(cache);
        Ok(lookup(&mut self.inner.dispatch_cache.lock().unwrap()).0)
    }

    /// The session's first episode, as its open actor knows it, with no read of the WAL or the
    /// store. `None` when no actor holds the session or it has not learned the episode yet.
    pub fn first_episode(&self, key: &LineageKey) -> Option<broca_core::FirstEpisodeV1> {
        match self.inner.slots.lock().unwrap().get(key) {
            Some(Entry::Open { handle, .. }) => handle.first_episode.get().cloned(),
            _ => None,
        }
    }

    /// Whether the session has a WAL, live or archived: a stat, and an archive index lookup
    /// only when there is no live file. `true` when the store cannot tell.
    pub fn wal_written(&self, key: &LineageKey) -> bool {
        self.inner
            .reader
            .as_ref()
            .and_then(|reader| reader.wal_store.wal_present(key))
            .unwrap_or(true)
    }

    /// When the session's live WAL file was last written, in milliseconds since the Unix epoch;
    /// `None` when it has no live file (it was folded into an archive) or the store cannot say.
    pub fn live_wal_written_ms(&self, key: &LineageKey) -> Option<u64> {
        let reader = self.inner.reader.as_ref()?;
        let written = reader.wal_store.live_wal_last_write(key).ok()??;
        written
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
    }

    /// Copy the cache reference under the slots lock, then acquire its facts separately.
    /// Writer taps must not make status hold the registry-wide slots lock while waiting.
    pub(crate) fn cached_awaiting_approval(
        &self,
        key: &LineageKey,
        run_id: &RunId,
    ) -> Option<broca_wire::AwaitingApproval> {
        let (approvals, head) = {
            let slots = self.inner.slots.lock().unwrap();
            let Some(Entry::Open { handle, .. }) = slots.get(key) else {
                return None;
            };
            (
                handle.approvals.clone(),
                handle.durable_head.load(Ordering::Acquire),
            )
        };
        approvals.awaiting(run_id, head)
    }

    /// The session's whole durable WAL in order, with its lineage id: every frame a live writer
    /// has made durable, or every frame on disk when no writer holds the session.
    pub async fn read_durable_wal(
        &self,
        key: &LineageKey,
    ) -> Result<
        (
            Vec<broca_wal::FramedRecord>,
            Option<broca_wal::WalLineageId>,
        ),
        OpenError,
    > {
        self.read_durable_wal_for(key, ReadPurpose::Session).await
    }

    async fn read_durable_wal_for(
        &self,
        key: &LineageKey,
        purpose: ReadPurpose,
    ) -> Result<
        (
            Vec<broca_wal::FramedRecord>,
            Option<broca_wal::WalLineageId>,
        ),
        OpenError,
    > {
        match purpose {
            ReadPurpose::Session => self.check_expired(key, None).await?,
            ReadPurpose::Authorization => self.check_authorization_expired(key).await?,
        }
        let Some(reader) = &self.inner.reader else {
            return Err(OpenError::Open(
                "session.read reader is not configured".into(),
            ));
        };
        let live_head = {
            let slots = self.inner.slots.lock().unwrap();
            match slots.get(key) {
                Some(Entry::Open { handle, .. }) => {
                    Some(handle.durable_head.load(Ordering::Acquire))
                }
                Some(Entry::Opening { .. }) | Some(Entry::Closing { .. }) | None => None,
            }
        };
        let range = self
            .read_durable_range_for(reader, key, 0, None, live_head, purpose)
            .await?;
        let records = crate::durable_read::DurableRead::classify(&range, 0, live_head)
            .records()
            .map_err(|reason| OpenError::Classified {
                reason,
                class: broca_protocol::ErrorClass::Transient,
            })?
            .to_vec();
        Ok((records, range.lineage_id))
    }

    /// Returns the same page as `route_read` and, when `include_tools` is set, the tool list
    /// frozen on the durable `RunStarted` of the session's latest episode. The second value is
    /// `None` when `include_tools` is off or the session has no episode.
    ///
    /// The tools come from the same snapshot as the page: the WAL tail already read for it, or,
    /// when the latest episode began inside the materialized prefix, a WAL read bounded at that
    /// prefix's watermark. They are copied from the record as written, never rebuilt from the
    /// run index, which keeps only a digest of them.
    pub async fn route_read_with_tools(
        &self,
        key: &LineageKey,
        from_ordinal: Option<u64>,
        limit: Option<u64>,
        include_tools: bool,
    ) -> Result<(broca_core::SessionReadPage, Option<SessionReadTools>), OpenError> {
        self.route_read_page(key, from_ordinal, limit, include_tools)
            .await
            .map(|read| (read.page, read.tools))
    }

    /// Where the session's materialized transcript holds the part tagged `tag`, from the store's
    /// tag index alone: no message and no WAL record is read. `None` when no store with a tag
    /// index is configured, or its lookup failed (logged); the caller then reads the transcript.
    pub async fn route_tag_location(
        &self,
        key: &LineageKey,
        tag: u64,
    ) -> Result<Option<broca_core::TagLocation>, OpenError> {
        self.check_expired(key, None).await?;
        let Some(store) = self
            .inner
            .reader
            .as_ref()
            .and_then(|reader| reader.store.clone())
        else {
            return Ok(None);
        };
        let session = key.clone();
        match spawn_blocking(JobKind::StoreRead, move || {
            store.tag_location(&session, tag)
        })
        .await
        {
            Ok(Ok(location)) => Ok(location),
            Ok(Err(error)) => {
                tracing::warn!(
                    target: "session",
                    "session.resolve tag index lookup failed; reading the transcript instead: session={} wal={} {error}",
                    key.session,
                    broca_core::session_addr(key)
                );
                Ok(None)
            }
            Err(error) => {
                tracing::warn!(
                    target: "session",
                    "session.resolve tag index task failed; reading the transcript instead: session={} wal={} {error}",
                    key.session,
                    broca_core::session_addr(key)
                );
                Ok(None)
            }
        }
    }

    /// [`Self::route_read_with_tools`], with what the same snapshot says beyond the page: the
    /// dispatch facts of the tool turns its WAL tail commits, and the time of the tail's newest
    /// timestamped record.
    pub async fn route_read_page(
        &self,
        key: &LineageKey,
        from_ordinal: Option<u64>,
        limit: Option<u64>,
        include_tools: bool,
    ) -> Result<RouteReadPage, OpenError> {
        match self
            .route_read_page_view(key, from_ordinal, limit, include_tools, None)
            .await?
        {
            RoutedPage::Raw(page) => Ok(*page),
            RoutedPage::Model(_) => unreachable!("raw read requested"),
        }
    }

    /// Model reads serve only fsynced WAL records and discard a store prefix whose
    /// lineage differs from the WAL, just like transcript reads, without opening an actor.
    /// Unlike raw reads, model reads request the checkpoint's compaction state.
    pub async fn route_read_model_page(
        &self,
        key: &LineageKey,
        request: &broca_core::ModelPageRequest,
    ) -> Result<broca_core::ModelPage, OpenError> {
        match self
            .route_read_page_view(
                key,
                request.from_ordinal,
                request.limit,
                false,
                Some(request),
            )
            .await?
        {
            RoutedPage::Model(page) => Ok(page),
            RoutedPage::Raw(_) => unreachable!("model read requested"),
        }
    }

    async fn route_read_page_view(
        &self,
        key: &LineageKey,
        from_ordinal: Option<u64>,
        limit: Option<u64>,
        include_tools: bool,
        model: Option<&broca_core::ModelPageRequest>,
    ) -> Result<RoutedPage, OpenError> {
        self.check_expired(key, None).await?;
        let Some(reader) = &self.inner.reader else {
            return Err(OpenError::Open(
                "session.read reader is not configured".into(),
            ));
        };
        SESSION_READS.fetch_add(1, Ordering::Relaxed);
        let capped_limit = limit
            .unwrap_or(broca_core::SESSION_READ_DEFAULT_LIMIT)
            .clamp(1, broca_core::SESSION_READ_MAX_LIMIT);
        let view = if model.is_some() {
            broca_core::ReadView::Model
        } else {
            broca_core::ReadView::Raw
        };
        // Repeated-tool-call warnings require counters from before the page's range.
        // Supply the full stored prefix; core folds warnings before slicing the view.
        let store_from = if model.is_some() {
            Some(0)
        } else {
            from_ordinal
        };
        let store_limit = if model.is_some() {
            broca_core::SESSION_READ_MAX_LIMIT
        } else {
            capped_limit
        };
        let mut attribution_backfill = false;
        let mut snapshot = match reader.store.clone() {
            Some(store) => {
                let session = key.clone();
                match spawn_blocking(JobKind::StoreRead, move || {
                    store.read_page_with_attribution(&session, store_from, store_limit, view)
                })
                .await
                {
                    Ok(Ok(snapshot)) => snapshot,
                    Ok(Err(broca_core::StorePageReadError::AttributionStale)) => {
                        attribution_backfill = true;
                        let first_notice = self
                            .inner
                            .attribution_notices
                            .lock()
                            .unwrap()
                            .insert(key.clone(), ())
                            .is_none();
                        if first_notice {
                            tracing::info!(
                                target: "session",
                                "run attribution not yet stored; reading from WAL and backfilling: session={} wal={}",
                                key.session,
                                broca_core::session_addr(key)
                            );
                        }
                        broca_core::StorePageSnapshot::empty()
                    }
                    // NAMED, because these two lines are the reason the identity is here at all.
                    // 694 of them accumulated in the shared daemon log carrying no session and no
                    // timestamp, so the only way to bound the population was to abandon the log
                    // and query the store -- which is the right answer obtained the wrong way.
                    //
                    // The address rather than the session name alone: the WAL is named by the
                    // hash of the whole bind triple, so a reader holding a session name and a
                    // directory of 30,000 files cannot get from one to the other.
                    Ok(Err(error)) => {
                        tracing::warn!(
                            target: "session",
                            "session.read store page read failed; falling back to full WAL replay: session={} wal={} {error}",
                            key.session,
                            broca_core::session_addr(key)
                        );
                        broca_core::StorePageSnapshot::empty()
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "session",
                            "session.read store page task failed; falling back to full WAL replay: session={} wal={} {error}",
                            key.session,
                            broca_core::session_addr(key)
                        );
                        broca_core::StorePageSnapshot::empty()
                    }
                }
            }
            None => broca_core::StorePageSnapshot::empty(),
        };

        let live_head = {
            let slots = self.inner.slots.lock().unwrap();
            match slots.get(key) {
                Some(Entry::Open { handle, .. }) => {
                    Some(handle.durable_head.load(Ordering::Acquire))
                }
                // Opening and Closing have no published writer proof. The helper below takes a
                // dedicated flush-only handle before it serves any WAL frame beyond the store.
                Some(Entry::Opening { .. }) | Some(Entry::Closing { .. }) | None => None,
            }
        };
        let mut tail = self
            .read_durable_range(
                reader,
                key,
                snapshot.watermark_seq,
                snapshot.anchor,
                live_head,
            )
            .await?;

        // ABSENCE MEANS UNKNOWN, NOT MISMATCH. Only two PRESENT ids that DIFFER are a mismatch;
        // every other pairing falls through deliberately.
        //
        // Do not "tighten" this to treat a missing stored id as a mismatch. Every row written
        // before this column existed carries NULL -- 24,860 materialized sessions at the time it
        // shipped -- so that change would force a full-WAL reread for all of them on their next
        // read, turning a correctness guard into a fleet-wide performance cliff. A missing id is
        // the absence of evidence, and the incremental path it enables is the one that was
        // already correct for those sessions.
        // Formatted on the STACK rather than into a String, because the matching path -- which
        // is every ordinary read -- would allocate the text only to drop it unread. Measured at
        // 64 bytes and four allocator events per lineaged read.
        //
        // The mismatch arm still allocates, and should: it is rare, it logs, and it hands the
        // stale id to a repair task that outlives this frame.
        let mut observed_text = LineageIdText::new();
        let observed_lineage_id = tail
            .lineage_id
            .map(|lineage_id| observed_text.format(lineage_id));
        let lineage_mismatch = match (snapshot.lineage_id.as_deref(), observed_lineage_id) {
            (Some(stored), Some(observed)) if stored != observed => Some(stored.to_string()),
            _ => None,
        };
        let mut lineage_repair = None;
        if let Some(stale_lineage_id) = lineage_mismatch {
            tracing::warn!(
                target: "session",
                "session.read materialized lineage differs from the WAL; replaying from sequence zero"
            );
            snapshot = broca_core::StorePageSnapshot::empty();
            tail = self
                .read_durable_range(reader, key, 0, None, live_head)
                .await?;
            lineage_repair = reader
                .store
                .clone()
                .map(|store| (store, key.clone(), stale_lineage_id));
        }

        if budget_firing_needs_full_replay(
            match tail.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
            snapshot.prefix_len,
        ) {
            tracing::warn!(
                target: "session",
                "session.read WAL tail lacks the budget config for BudgetFired; falling back to full WAL replay"
            );
            snapshot = broca_core::StorePageSnapshot::empty();
            tail = self
                .read_durable_range(reader, key, 0, None, live_head)
                .await?;
        }

        // A CONSULTED ARCHIVE MISS IS NOT EVIDENCE OF LOSS BY ITSELF, and treating it as such
        // inverted this guard the day the tree was folded. Before any fold there is no
        // `wal-archive/` directory, so nothing is consulted and a brand-new session reads as an
        // empty page. After one, "no live WAL and no archived member" is the NORMAL state of a
        // session that has never been written -- so every new session refused its first read,
        // live in production from 2026-09-13 until this fix, and reported by the one client that
        // reads before it writes.
        //
        // I recorded this exact inversion for `wal_present` while designing the fold ("post-fold
        // that is the routine case, so the signal inverts") and did not carry it to the read
        // path. A guard whose population changes when an operator runs a maintenance pass needs
        // its condition stated in terms that survive the pass.
        //
        // The guard itself is real: it stops a materialized prefix from being served as a
        // complete transcript when the WAL behind it is gone, which is the state 61 production
        // sessions are in. What it lacked was EVIDENCE THE LINEAGE EVER EXISTED. The store
        // snapshot is already in hand, so a prefix of zero means nothing durable claims this
        // session has history and an empty page asserts nothing. A prefix above zero means the
        // store says there IS history and the log that proves it is unreachable, which is a
        // genuine failure and stays loud.
        //
        // The message deliberately shares NO phrase with the old one. Consumers pattern-match
        // this prose because there is no wire code that separates "never existed" from "existed
        // and unreadable" -- that split is mine to add -- and reusing the words here would hand
        // every such matcher a suppression rule for a real fault. Same collapse at the text
        // layer that the branch just removed at the code layer.
        if tail.archive_lookup_missed() && snapshot.prefix_len > 0 {
            return Err(OpenError::Open(format!(
                "session {} has {} materialized message(s) but its WAL is unreachable: not live, not archived",
                broca_core::session_addr(key),
                snapshot.prefix_len
            )));
        }
        let lineage_id = tail.lineage_id;
        if attribution_backfill {
            let opener = self.inner.opener.clone();
            let session = key.clone();
            let range = tail.clone();
            tokio::spawn(async move {
                if let Err(error) = opener.backfill_attribution(&session, range).await {
                    tracing::warn!(target: "store", "session run attribution backfill failed; reads remain WAL-authoritative: {error}");
                }
            });
        }
        // Each Prepared filing key combines the WAL lineage id and its frame sequence.
        // Supply the observed id before core reconstructs those keys, including when no
        // store projection has yet persisted a lineage id.
        if snapshot.lineage_id.is_none() {
            snapshot.lineage_id = lineage_id.map(|id| id.to_string());
        }
        if let Some(request) = model {
            let records = match tail.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            };
            let result = broca_core::read_session_model_page(&snapshot, records, request, || {
                Err("complete WAL required".into())
            });
            let mut page = match result {
                Err(broca_core::ModelPageError::CompleteRead(_)) => {
                    // Pin the fallback to the page's original WAL tail. A later apply must
                    // not leak into a page whose head describes an earlier snapshot.
                    let head = records
                        .last()
                        .map_or(snapshot.watermark_seq, |frame| frame.seq);
                    let full = self
                        .read_durable_range(reader, key, 0, None, Some(head))
                        .await?;
                    if full.lineage_id != lineage_id {
                        return Err(OpenError::Open("lineage changed during model read".into()));
                    }
                    let full_records = match full.into_records() {
                        broca_wal::WalRangeRecords::Complete(records)
                        | broca_wal::WalRangeRecords::Partial(records)
                        | broca_wal::WalRangeRecords::Unknown(records) => records,
                        broca_wal::WalRangeRecords::Absent => {
                            return Err(OpenError::Open(
                                "complete WAL unavailable for model read".into(),
                            ))
                        }
                    };
                    broca_core::read_session_model_page(&snapshot, records, request, || {
                        Ok(full_records)
                    })
                }
                other => other,
            }
            .map_err(|error| OpenError::Open(format!("model replay failed: {error}")))?;
            page.lineage_id = lineage_id.map(|id| id.to_string()).or(page.lineage_id);
            return Ok(RoutedPage::Model(page));
        }
        // Read the watermark before the snapshot moves into the page. When one of the
        // full-replay fallbacks above reset the snapshot to empty, this is 0: the tail then
        // covers the whole WAL and no separate prefix read is needed for tools.
        let prefix_watermark = snapshot.watermark_seq;
        let open_batch = match tail.records() {
            broca_wal::WalRangeRecords::Complete(records) => {
                let records: Vec<_> = records.iter().map(|frame| frame.record.clone()).collect();
                let replay = broca_core::replay(&records)
                    .map_err(|error| OpenError::Open(error.to_string()))?;
                Some(crate::approval::open_batch_view(
                    replay.open_tool_batch.as_ref(),
                ))
            }
            broca_wal::WalRangeRecords::Absent if snapshot.prefix_len == 0 => {
                Some(broca_wire::OpenBatchView::None)
            }
            _ => Some(broca_wire::OpenBatchView::NotReported {
                reason: "incomplete_tail".into(),
            }),
        };
        let newest_record_ms = match tail.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Partial(records)
            | broca_wal::WalRangeRecords::Unknown(records) => records
                .iter()
                .rev()
                .find_map(|framed| broca_core::record_time_ms(&framed.record)),
            broca_wal::WalRangeRecords::Absent => None,
        };
        let retention_activity_ms = match tail.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Partial(records)
            | broca_wal::WalRangeRecords::Unknown(records) => {
                crate::retention::policy(records).map(|(_, activity, _)| activity)
            }
            broca_wal::WalRangeRecords::Absent => None,
        };
        // A title in the tail is newer than any the store folded below its watermark. When the
        // store's lineage id differed from the WAL's, or the tail lacked the budget config its
        // `BudgetFired` needs, the snapshot was emptied and the tail re-read from sequence zero:
        // the tail is then the whole WAL and the store adds nothing.
        let last_title = match tail.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Partial(records)
            | broca_wal::WalRangeRecords::Unknown(records) => records
                .iter()
                .rev()
                .find_map(|framed| framed.record.title()),
            broca_wal::WalRangeRecords::Absent => None,
        }
        .map(str::to_owned)
        .or_else(|| snapshot.last_title.clone());
        let (mut page, dispatches) = broca_core::read_session_page_owned_with_dispatches(
            snapshot,
            match tail.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                // Legacy live-only absence still successfully serves the store prefix alone.
                // This silently truncates a transcript if projection lagged the missing WAL.
                // A September 2026 operator audit found 61 such sessions, all complete by
                // coincidence. Archive resolution preserves that legacy behavior only when no
                // archive was consulted; consulted misses are rejected above.
                broca_wal::WalRangeRecords::Absent => &[],
            },
            from_ordinal,
            Some(capped_limit),
        )
        .map_err(|e| OpenError::Open(format!("replay failed: {e}")))?;
        page.lineage_id = lineage_id;

        let tools = if include_tools {
            let tail_tools = latest_run_started_tools(match tail.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            });
            match tail_tools {
                Some(tools) => Some(tools),
                None if prefix_watermark > 0 => {
                    // The page already serves the store's rows up to this watermark without a
                    // flush, so reading the WAL up to the same point claims nothing the page
                    // does not. The watermark bounds this read the way a writer's durable head
                    // would.
                    let prefix = self
                        .read_durable_range(reader, key, 0, None, Some(prefix_watermark))
                        .await?;
                    let prefix_tools = latest_run_started_tools(match prefix.records() {
                        broca_wal::WalRangeRecords::Complete(records)
                        | broca_wal::WalRangeRecords::Partial(records)
                        | broca_wal::WalRangeRecords::Unknown(records) => records,
                        broca_wal::WalRangeRecords::Absent => &[],
                    });
                    // The store says this session has history, so an episode began somewhere.
                    // If the WAL cannot show its RunStarted, the tools are unknown -- answering
                    // with no `tools` would claim the session never had an episode.
                    Some(prefix_tools.ok_or_else(|| {
                        OpenError::Open(format!(
                            "session {} has materialized history but no readable RunStarted to report its tools from",
                            broca_core::session_addr(key)
                        ))
                    })?)
                }
                None => None,
            }
        } else {
            None
        };

        if let Some((store, session, stale_lineage_id)) = lineage_repair {
            match spawn_blocking(JobKind::Projection, move || {
                store.reproject_if_lineage_mismatch(&session, &stale_lineage_id, &tail)
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(
                    target: "session",
                    "session.read lineage reprojection failed; the returned page remains WAL-authoritative: {error}"
                ),
                Err(error) => tracing::warn!(
                    target: "session",
                    "session.read lineage reprojection task failed; the returned page remains WAL-authoritative: {error}"
                ),
            }
        }
        Ok(RoutedPage::Raw(Box::new(RouteReadPage {
            page,
            open_batch,
            tools,
            dispatches,
            newest_record_ms,
            retention_activity_ms,
            last_title,
        })))
    }

    /// Route a ready hint with the daemon's caller stamp, never a caller id from params.
    pub async fn route_compaction_ready(
        &self,
        key: &LineageKey,
        caller: &str,
        request: broca_wire::CompactionReady,
    ) -> Result<crate::actor::CompactionReadyOutcome, OpenError> {
        use crate::actor::{CompactionReadyOutcome, LineageCmd};
        loop {
            let handle = self.actor_for(key).await?;
            let (reply, rx) = tokio::sync::oneshot::channel();
            if handle
                .send(LineageCmd::CompactionReady {
                    caller: caller.to_owned(),
                    request: request.clone(),
                    reply,
                })
                .await
                .is_err()
            {
                continue;
            }
            match rx.await {
                Ok(CompactionReadyOutcome::Reopen) | Err(_) => continue,
                Ok(outcome) => return Ok(outcome),
            }
        }
    }

    /// Expiring logs blocked from deletion because they are unreadable and the indexed
    /// deadline cannot be proved to include every WAL record.
    pub fn retention_unreadable_sessions(&self) -> usize {
        self.inner
            .retention
            .get()
            .map_or(0, |service| service.unreadable_sessions())
    }

    /// Retained sessions with an outstanding repair obligation. Entries stay queued until
    /// repair succeeds; evicting them could forget a retention or run-index write still owed.
    pub fn retention_repairs_pending(&self) -> usize {
        self.inner.repairs.len()
    }

    #[cfg(all(test, feature = "test-clock"))]
    pub(crate) fn status_open_probe_for_test(&self, probe: Arc<dyn Fn() + Send + Sync>) {
        *self.inner.status_open_probe.lock().unwrap() = Some(probe);
    }

    /// Existing actors answer status in the same command loop that admits new runs.
    /// An unopened session is classified under its lease from the first RunStarted;
    /// ordinary status does not replay the entire WAL before creating its actor.
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
            let handle = self.actor_for_mode(key, None, None, true).await?;
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
            if counts.runs_in_flight == 0 && counts.opening == 0 {
                break;
            }
            tokio::time::sleep_until(
                (tokio::time::Instant::now() + std::time::Duration::from_millis(10)).min(deadline),
            )
            .await;
        }
        self.inner.seal.cancel();
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

    pub fn seal_failures(&self) -> Vec<SealFailure> {
        self.inner.seal_state.lock().unwrap().failures.clone()
    }

    pub fn recovery_debt(&self) -> Vec<SealFailure> {
        self.inner
            .seal_state
            .lock()
            .unwrap()
            .recovery_debt
            .values()
            .cloned()
            .collect()
    }

    /// Cheap inspection: count how many registry slots are opening, open, or closing.
    pub fn slot_counts(&self) -> SlotCounts {
        let slots = self.inner.slots.lock().unwrap();
        let mut counts = SlotCounts {
            open: 0,
            opening: 0,
            closing: 0,
            runs_in_flight: 0,
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
    pub fn slot_state(&self, key: &LineageKey) -> Option<&'static str> {
        let slots = self.inner.slots.lock().unwrap();
        slots.get(key).map(|e| match e {
            Entry::Opening { .. } => "opening",
            Entry::Open { .. } => "open",
            Entry::Closing { .. } => "closing",
        })
    }

    // Only the test-clock expiry tests close a slot by advancing time.
    #[cfg(all(test, feature = "test-clock"))]
    pub(crate) async fn wait_for_slot_closed(&self, key: &LineageKey) {
        let mut close_started = self.inner.close_started.subscribe();
        loop {
            let closed = {
                let slots = self.inner.slots.lock().unwrap();
                match slots.get(key) {
                    Some(Entry::Closing { done, .. }) => Some(done.subscribe()),
                    None => return,
                    Some(Entry::Opening { .. } | Entry::Open { .. }) => None,
                }
            };
            if let Some(mut done) = closed {
                done.recv()
                    .await
                    .expect("the registry close notification should resolve");
                continue;
            }
            close_started
                .changed()
                .await
                .expect("the registry close-start notification should remain available");
        }
    }

    /// Resume WAL-confirmed restart pauses after dependency routes are registered. Candidate
    /// index rows may lag the WAL; only a latest durable restart pause authorizes a resume.
    pub async fn resume_restart_pauses(
        &self,
        wal_store: Arc<dyn broca_core::WalStore>,
        candidates: Vec<(String, LineageKey)>,
    ) -> (usize, usize) {
        self.inner
            .restart_resume_ready
            .store(true, Ordering::Release);
        let mut seen = HashSet::new();
        let mut resumed = 0;
        let mut sealed = 0;
        for (_, key) in candidates {
            if !seen.insert(key.clone()) {
                continue;
            }
            let store = wal_store.clone();
            let session = key.clone();
            let prior = spawn_blocking(JobKind::WalRead, move || {
                store
                    .read_records(&session)
                    .map_err(|error| error.to_string())
                    .and_then(|records| {
                        broca_core::replay(
                            &records
                                .into_iter()
                                .map(|frame| frame.record)
                                .collect::<Vec<_>>(),
                        )
                        .map_err(|error| error.to_string())
                    })
            })
            .await;
            let prior = match prior {
                Ok(Ok(prior)) => prior,
                error => {
                    tracing::warn!(target: "lifecycle", "restart resume: refused session '{}': {error:?}", key.session);
                    continue;
                }
            };
            if !matches!(
                prior.resolution,
                broca_core::ReplayResolution::AuthRequired {
                    reason: broca_protocol::PauseReason::Restart,
                    ..
                }
            ) {
                continue;
            }
            let result = async {
                let handle = self
                    .actor_for(&key)
                    .await
                    .map_err(|error| error.to_string())?;
                let (reply, rx) = tokio::sync::oneshot::channel();
                handle
                    .send(crate::actor::LineageCmd::ResumeRestart { reply })
                    .await
                    .map_err(|_| "session actor closed".to_owned())?;
                rx.await
                    .map_err(|_| "session actor dropped recovery reply".to_owned())??;
                let store = wal_store.clone();
                let session = key.clone();
                spawn_blocking(JobKind::WalRead, move || {
                    store
                        .read_records(&session)
                        .map_err(|error| error.to_string())
                        .and_then(|records| {
                            broca_core::replay(
                                &records
                                    .into_iter()
                                    .map(|frame| frame.record)
                                    .collect::<Vec<_>>(),
                            )
                            .map_err(|error| error.to_string())
                        })
                })
                .await
                .map_err(|error| error.to_string())?
            }
            .await;
            match result {
                Ok(state)
                    if matches!(
                        state.resolution,
                        broca_core::ReplayResolution::LineageIdle {
                            last_reason: broca_protocol::RunFinishReason::Interrupted
                        }
                    ) =>
                {
                    sealed += 1
                }
                Ok(_) => resumed += 1,
                Err(error) => {
                    tracing::warn!(target: "lifecycle", "restart resume: refused session '{}': {error}", key.session)
                }
            }
        }
        tracing::info!(target: "lifecycle", "restart resume: {resumed} resumed, {sealed} sealed interrupted");
        (resumed, sealed)
    }

    /// First half of the startup crash-orphan pass: mark the sessions that may hold a run a
    /// crashed process left unfinished. Synchronous and cheap (one stat per candidate session),
    /// so a caller runs it BEFORE serving begins; after that, whichever open reaches a marked
    /// session first -- [`Self::seal_marked_crash_orphans`] or a caller's send -- seals the
    /// orphan inside the session's actor before the actor takes any command, so the two
    /// serialize and a send can never resume a run the pass meant to seal.
    ///
    /// `candidates` are the run index's active rows. A session is marked only when its live WAL
    /// was last written before `process_started`: a later write means a live writer owns the
    /// session. A session with no live WAL (folded into an archive) is skipped without being
    /// extracted, since a folded session had gone quiet before it was folded.
    pub fn mark_crash_orphans(
        &self,
        wal_store: &dyn broca_core::WalStore,
        candidates: Vec<(RunId, LineageKey)>,
        process_started: std::time::SystemTime,
    ) -> CrashOrphanSweep {
        let mut sweep = CrashOrphanSweep {
            candidate_runs: candidates.len(),
            ..CrashOrphanSweep::default()
        };
        let mut by_session: HashMap<LineageKey, HashSet<RunId>> = HashMap::new();
        for (run_id, key) in candidates {
            by_session.entry(key).or_default().insert(run_id);
        }
        let mut marks = self.inner.crash_orphan_marks.lock().unwrap();
        for (key, run_ids) in by_session {
            if wal_store.check_move(&key, true).is_err() {
                sweep.not_live += 1;
                continue;
            }
            match wal_store.live_wal_last_write(&key) {
                Ok(None) => sweep.not_live += 1,
                Ok(Some(written)) if written >= process_started => sweep.written_since_boot += 1,
                Ok(Some(_)) => {
                    marks.insert(key, run_ids);
                    sweep.marked += 1;
                }
                Err(error) => {
                    tracing::warn!(
                        target: "lifecycle",
                        "crash-orphan pass could not inspect the WAL of session '{}': {error}",
                        key.session
                    );
                    sweep.unreadable += 1;
                }
            }
        }
        sweep
    }

    /// Second half of the startup crash-orphan pass: open every session still marked, so its
    /// actor seals the orphan through the shutdown seal's own code. Safe to run while serving.
    ///
    /// Opening goes through the ordinary lease: a session another live process holds is
    /// counted and skipped, and its mark is dropped so a later open does not act on it.
    /// Waiting on a status reply is what makes the count final: an actor answers commands only
    /// after its open-time recovery, where the seal happens, has finished.
    pub async fn seal_marked_crash_orphans(&self, mut sweep: CrashOrphanSweep) -> CrashOrphanSweep {
        let marked: Vec<LineageKey> = self
            .inner
            .crash_orphan_marks
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        for key in marked {
            // A caller's send may already have opened this session and consumed the mark;
            // its actor sealed (or declined) in that open, and the counter below includes it.
            if !self
                .inner
                .crash_orphan_marks
                .lock()
                .unwrap()
                .contains_key(&key)
            {
                continue;
            }
            let opened = async {
                self.actor_for(&key).await?;
                self.route_status(&key).await
            }
            .await;
            match opened {
                Ok(_) => {}
                Err(error) => {
                    self.inner.crash_orphan_marks.lock().unwrap().remove(&key);
                    match error {
                        OpenError::SessionMoving { .. } | OpenError::SessionMoved { .. } => {
                            sweep.not_live += 1;
                        }
                        OpenError::Expired { .. }
                        | OpenError::RetentionInvalid(_)
                        | OpenError::SessionClassInvalid(_) => sweep.open_failed += 1,
                        OpenError::LeaseHeld(_) => sweep.lease_held += 1,
                        OpenError::Open(reason)
                        | OpenError::Classified { reason, .. }
                        | OpenError::NewerWriter { reason, .. } => {
                            tracing::warn!(
                                target: "lifecycle",
                                "crash-orphan pass could not open session '{}': {reason}",
                                key.session
                            );
                            sweep.open_failed += 1;
                        }
                    }
                }
            }
        }
        sweep.sealed = self.inner.crash_orphans_sealed.load(Ordering::Acquire);
        sweep.index_repaired = self
            .inner
            .crash_orphans_index_repaired
            .load(Ordering::Acquire);
        sweep
    }
}

impl RegistryHandle {
    pub(crate) fn note_session_ownership(
        &self,
        key: &LineageKey,
        generation: u64,
        checkpoint: &broca_core::AdmissionCheckpointV1,
    ) {
        if let Some(ownership) = checkpoint
            .ownership
            .as_ref()
            .filter(|owner| owner.episodes > 0)
        {
            self.note_owner_identity(key, generation, checkpoint.at_seq, ownership);
        }
    }

    fn note_owner_identity(
        &self,
        key: &LineageKey,
        generation: u64,
        head: u64,
        ownership: &broca_core::SessionOwnershipV1,
    ) {
        // Hold the slot lock through publication so a closing actor cannot repopulate its
        // entry after close removed it, or overwrite a newer actor's ownership.
        let slots = self.inner.slots.lock().unwrap();
        if matches!(slots.get(key), Some(Entry::Open { gen, .. }) if *gen == generation) {
            self.inner.live_ownership.lock().unwrap().insert(
                key.clone(),
                LiveOwnership {
                    generation,
                    head,
                    ownership: ownership.for_owner_access(),
                },
            );
        }
    }
    #[cfg(test)]
    pub(crate) fn report_actor_panic(&self) {
        self.inner.actor_panics.fetch_add(1, Ordering::SeqCst);
    }
    pub(crate) fn restart_resume_ready(&self) -> bool {
        self.inner.restart_resume_ready.load(Ordering::Acquire)
    }

    /// The session's prompt-cache clock, shared with every run and warm of the session.
    pub(crate) fn cache_clock(&self, key: &LineageKey) -> broca_core::CacheClock {
        self.inner.cache.clock(key)
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

    /// Take this session's crash-orphan mark, if the startup pass left one: the run ids the
    /// run index called active. Taken once, by the first actor to open the session.
    pub(crate) fn take_crash_orphan_mark(&self, key: &LineageKey) -> Option<HashSet<RunId>> {
        self.inner.crash_orphan_marks.lock().unwrap().remove(key)
    }

    pub(crate) fn report_crash_orphan_sealed(&self) {
        self.inner
            .crash_orphans_sealed
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn report_crash_orphan_index_repaired(&self) {
        self.inner
            .crash_orphans_index_repaired
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn report_seal_failure(
        &self,
        key: &LineageKey,
        generation: u64,
        stage: SealFailureStage,
        error: String,
    ) {
        let event = SealFailure {
            session: key.session.clone(),
            generation,
            stage,
            timestamp: std::time::SystemTime::now(),
            error,
        };
        let mut state = self.inner.seal_state.lock().unwrap();
        state.failures.push(event.clone());
        state.recovery_debt.insert(key.clone(), event);
    }

    pub(crate) fn has_recovery_debt(&self, key: &LineageKey) -> bool {
        self.inner
            .seal_state
            .lock()
            .unwrap()
            .recovery_debt
            .contains_key(key)
    }

    /// Only the actor's successful replay/recovery followed by a sync may discharge this debt.
    pub(crate) fn report_recovered(&self, key: &LineageKey) {
        self.inner
            .seal_state
            .lock()
            .unwrap()
            .recovery_debt
            .remove(key);
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
                self.inner.live_ownership.lock().unwrap().remove(key);
            }
        }
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
            self.inner.live_ownership.lock().unwrap().remove(&self.key);
        }
        let _ = self.done.send(());
    }

    /// The close ABORTED (pending work or a subscriber raced in): restore `Open{gen}` so
    /// the actor keeps serving, and wake the waiters (they re-route and find it Open).
    pub fn abort(mut self, handle: ActorHandle) {
        self.resolved = true;
        {
            let mut slots = self.inner.slots.lock().unwrap();
            slots.insert(
                self.key.clone(),
                Entry::Open {
                    gen: handle.gen,
                    handle,
                },
            );
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
            self.inner.live_ownership.lock().unwrap().remove(&self.key);
        }
        let _ = self.done.send(());
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use broca_core::{FileWalStore, WalHandle, WalStore};
    use broca_protocol::content::{AssistantMessage, ContentBlock, Message, Role};
    use broca_protocol::request::{ModelHandle, ToolChoice};
    use broca_protocol::{FinishReason, MessageId, RunId, StepId, Usage};
    use broca_wal::{FramedRecord, RunConfig, RunOrigin, WalError, WalRangeRead, WalRecord};

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

    struct GaugeOpener {
        durable_head: Mutex<Option<Arc<AtomicU64>>>,
    }

    #[tokio::test]
    async fn cached_approval_status_releases_slots_before_acquiring_facts() {
        let registry = LineageRegistry::new(Arc::new(GaugeOpener {
            durable_head: Mutex::new(Some(Arc::new(AtomicU64::new(0)))),
        }));
        let key = BindIdentity::new("/tmp/cache-status", "agent", "session");
        let handle = registry.actor_for(&key).await.unwrap();
        let inner = Arc::downgrade(&registry.inner);
        let reached = Arc::new(AtomicBool::new(false));
        let probe_reached = reached.clone();
        handle.approvals.on_awaiting_facts_lock(move || {
            let inner = inner.upgrade().unwrap();
            assert!(
                inner.slots.try_lock().is_ok(),
                "status held slots while acquiring approval facts"
            );
            probe_reached.store(true, Ordering::Release);
        });
        registry.cached_awaiting_approval(&key, &RunId("r".into()));
        assert!(
            reached.load(Ordering::Acquire),
            "status never reached the facts lock"
        );
    }

    #[test]
    fn cache_table_capacity_preserves_live_clock_for_registry_and_fresh_handle() {
        let registry = LineageRegistry::new(Arc::new(GaugeOpener {
            durable_head: Mutex::new(None),
        }));
        let handle = registry.handle();
        let key = BindIdentity::new("/cache", "runner", "live");
        let live = handle.cache_clock(&key);
        live.note_sent(100, Some(1000));
        for n in 0..65_536 {
            let inactive = BindIdentity::new("/cache", "runner", format!("inactive-{n}"));
            drop(handle.cache_clock(&inactive));
        }
        live.note_sent(200, Some(1000));
        assert_eq!(
            registry.cache_clock_reading(&key).unwrap().last_sent_at_ms,
            200
        );
        let fresh = handle.cache_clock(&key);
        fresh.note_sent(300, Some(1000));
        assert_eq!(live.read().unwrap().last_sent_at_ms, 300);
        assert_eq!(
            registry.cache_clock_reading(&key).unwrap().last_sent_at_ms,
            300
        );
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
                durable_head: self
                    .durable_head
                    .lock()
                    .unwrap()
                    .take()
                    .expect("test opener is called once"),
                first_episode: Arc::new(std::sync::OnceLock::new()),
                approvals: Arc::default(),
            })
        }
    }

    struct RecordingWalStore {
        inner: Arc<FileWalStore>,
        through_seq: Mutex<Vec<Option<u64>>>,
        flushes: AtomicU64,
    }

    struct CandidateWalStore;

    impl WalStore for CandidateWalStore {
        fn open(&self, _session: &LineageKey) -> Result<Box<dyn broca_wal::Wal + Send>, WalError> {
            unreachable!("degraded range test never opens a writer")
        }

        fn read_records(&self, _session: &LineageKey) -> Result<Vec<FramedRecord>, WalError> {
            unreachable!("degraded range test uses the range reader")
        }

        fn read_records_range(
            &self,
            session: &LineageKey,
            _after_seq: u64,
            _through_seq: Option<u64>,
            _anchor: Option<&broca_wal::WalByteAnchor>,
        ) -> Result<WalRangeRead, WalError> {
            Ok(WalRangeRead::without_anchors(vec![FramedRecord {
                seq: 1,
                fence: 1,
                record: started_record(session),
            }]))
        }

        fn flush_for_read(&self, _session: &LineageKey) -> Result<Option<u64>, WalError> {
            Err(WalError::Io(std::io::Error::other(
                "injected read-side flush failure",
            )))
        }
    }

    impl WalStore for RecordingWalStore {
        fn open(
            &self,
            session: &LineageKey,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            self.inner.open(session)
        }

        fn read_records(
            &self,
            session: &LineageKey,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            self.inner.read_records(session)
        }

        fn read_records_range(
            &self,
            session: &LineageKey,
            after_seq: u64,
            through_seq: Option<u64>,
            anchor: Option<&broca_wal::WalByteAnchor>,
        ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
            self.through_seq.lock().unwrap().push(through_seq);
            self.inner
                .read_records_range(session, after_seq, through_seq, anchor)
        }

        fn flush_for_read(&self, session: &LineageKey) -> Result<Option<u64>, broca_wal::WalError> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            self.inner.flush_for_read(session)
        }

        fn file_identity(&self, session: &LineageKey) -> Result<Option<u64>, broca_wal::WalError> {
            self.inner.file_identity(session)
        }
    }

    fn key() -> LineageKey {
        BindIdentity::new("/tmp/x", "runner", "s1")
    }

    #[test]
    fn frozen_sender_cache_evicts_oldest_at_capacity() {
        let registry = LineageRegistry::new(Arc::new(NeverOpener));
        let mut cache = registry.inner.frozen_senders.lock().unwrap();
        for i in 0..=SESSION_CACHE_SESSIONS {
            cache.insert(test_key(&format!("sender-{i}")), None);
        }
        assert_eq!(cache.entries.len(), SESSION_CACHE_SESSIONS);
        assert_eq!(cache.oldest.len(), SESSION_CACHE_SESSIONS);
        assert!(cache.get(&test_key("sender-0")).is_none());
        assert!(cache.get(&test_key("sender-1")).is_some());
        assert!(cache
            .get(&test_key(&format!("sender-{SESSION_CACHE_SESSIONS}")))
            .is_some());
    }

    #[test]
    fn session_cache_updates_and_removal_keep_fifo_bookkeeping_bounded() {
        let mut cache = BoundedSessionCache::new(2);
        let first = test_key("first");
        let second = test_key("second");
        let third = test_key("third");
        cache.insert(first.clone(), 1);
        cache.insert(second.clone(), 2);
        for value in 3..100 {
            cache.insert(first.clone(), value);
        }
        cache.insert(third.clone(), 100);
        assert!(cache.get(&first).is_none(), "updates do not refresh age");
        assert_eq!(cache.get(&second), Some(&2));
        for _ in 0..100 {
            cache.remove(&second);
            cache.insert(second.clone(), 2);
        }
        assert_eq!(cache.oldest.len(), 2);
        cache.remove(&third);
        cache.remove(&second);
        assert!(cache.entries.is_empty());
        assert!(cache.oldest.is_empty());
    }

    #[tokio::test]
    async fn frozen_sender_eviction_preserves_sender_not_owner_from_store_and_wal() {
        use crate::serve::{ServeError, ServeHandler};
        use broca_core::{scope_admission::ScopeRefusal, Persister};
        use broca_protocol::BindPrincipal;

        let basal = BindPrincipal::Reserved {
            module_id: broca_core::scope_admission::FLOW_ONLY_MODULE.into(),
        };
        let other = BindPrincipal::Reserved {
            module_id: "prefrontal-core".into(),
        };
        for indexed in [false, true] {
            for (label, sender) in [
                ("unrecorded", None),
                ("other", Some(other.clone())),
                ("basal", Some(basal.clone())),
            ] {
                let root = broca_test_support::scratch_root().join(format!(
                    "broca-frozen-sender-{indexed}-{label}-{}",
                    std::process::id()
                ));
                let session = BindIdentity::new(root.join("project"), "runner", "session");
                let file_store = Arc::new(FileWalStore::new(root.join("wal")));
                let mut record = started_record(&session);
                if let WalRecord::RunStarted { principal, .. } = &mut record {
                    *principal = sender.clone();
                }
                file_store
                    .open(&session)
                    .unwrap()
                    .append(1, &record, true)
                    .unwrap();
                let store = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
                if indexed {
                    let mut wal = file_store.open(&session).unwrap();
                    wal.append(1, &assistant_record(), true).unwrap();
                    wal.append(1, &WalRecord::TurnFinished { step_id: StepId(1) }, true)
                        .unwrap();
                    drop(wal);
                    Persister::project(
                        store.as_ref(),
                        &session,
                        &file_store.read_records(&session).unwrap(),
                    )
                    .unwrap();
                }
                let wal_store = Arc::new(RecordingWalStore {
                    inner: file_store,
                    through_seq: Mutex::new(Vec::new()),
                    flushes: AtomicU64::new(0),
                });
                let registry = LineageRegistry::new_with_reader(
                    Arc::new(NeverOpener),
                    wal_store.clone(),
                    indexed.then_some(store as Arc<dyn broca_core::MaterializedStore>),
                );
                assert_eq!(
                    registry.frozen_sender(&session).await.unwrap(),
                    Some(sender.clone())
                );
                let allowed = sender.as_ref() == Some(&basal);
                let before = registry.authorize_session(&session, Some(&basal)).await;
                assert_eq!(before.is_ok(), allowed);
                if !allowed {
                    assert!(matches!(
                        before,
                        Err(ServeError::ScopeRefused(ScopeRefusal::SenderNotOwner))
                    ));
                }
                {
                    let mut cache = registry.inner.frozen_senders.lock().unwrap();
                    // Remove the remembered sender explicitly to test reloading the recorded
                    // value on a miss. The separate capacity test checks automatic FIFO eviction.
                    cache.remove(&session);
                    for i in 0..SESSION_CACHE_SESSIONS {
                        cache.insert(test_key(&format!("filler-{i}")), None);
                    }
                    assert!(cache.get(&session).is_none());
                }
                let after = registry.authorize_session(&session, Some(&basal)).await;
                assert_eq!(after.is_ok(), allowed);
                if !allowed {
                    assert!(matches!(
                        after,
                        Err(ServeError::ScopeRefused(ScopeRefusal::SenderNotOwner))
                    ));
                }
                assert_eq!(
                    registry.frozen_sender(&session).await.unwrap(),
                    Some(sender)
                );
                assert_eq!(
                    wal_store
                        .through_seq
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|head| head.is_none())
                        .count(),
                    if indexed { 0 } else { 2 }
                );
                assert_eq!(registry.slot_counts().open, 0);
            }
        }
    }

    fn read_proof_fixture(
        archived: bool,
    ) -> (LineageRegistry, BindIdentity, Arc<RecordingWalStore>) {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-read-proof-eviction-{}-{archived}",
            std::process::id()
        ));
        let session = BindIdentity::new(root.join("project"), "runner", "session");
        let file_store = Arc::new(FileWalStore::new(root.join("wal")));
        file_store
            .open(&session)
            .unwrap()
            .append(1, &started_record(&session), true)
            .unwrap();
        if archived {
            let path = file_store.wal_path(&session);
            let archive_dir = root.join("wal-archive");
            std::fs::create_dir_all(&archive_dir).unwrap();
            let mut archive =
                broca_wal::archive::ArchiveWriter::create(archive_dir.join("fold-1.ark"))
                    .unwrap()
                    .without_feature_floor();
            archive
                .append(
                    &broca_core::session_addr(&session),
                    &std::fs::read(&path).unwrap(),
                )
                .unwrap();
            archive.seal().unwrap();
            std::fs::remove_file(path).unwrap();
        }
        let wal_store = Arc::new(RecordingWalStore {
            inner: file_store,
            through_seq: Mutex::new(Vec::new()),
            flushes: AtomicU64::new(0),
        });
        let registry =
            LineageRegistry::new_with_reader(Arc::new(NeverOpener), wal_store.clone(), None);
        (registry, session, wal_store)
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn reads_without_file_identity_repeat_the_barrier_without_caching_proofs() {
        let (registry, session, wal_store) = read_proof_fixture(false);
        assert_eq!(wal_store.file_identity(&session).unwrap(), None);
        let before = registry.route_read(&session, None, None).await.unwrap();
        let after = registry.route_read(&session, None, None).await.unwrap();
        assert_eq!(before.messages.len(), 1);
        assert_eq!(after.messages, before.messages);
        assert_eq!(after.head, before.head);
        assert_eq!(wal_store.flushes.load(Ordering::Relaxed), 2);
        assert!(registry
            .inner
            .read_proofs
            .lock()
            .unwrap()
            .entries
            .is_empty());
    }

    #[tokio::test]
    async fn read_proof_eviction_repeats_the_barrier_and_preserves_the_page() {
        // Archives have a content-based identity on every platform, including Windows,
        // where live WALs do not populate the proof cache.
        check_read_proof_eviction(true).await;
        #[cfg(unix)]
        check_read_proof_eviction(false).await;
    }

    async fn check_read_proof_eviction(archived: bool) {
        let (registry, session, wal_store) = read_proof_fixture(archived);
        let before = registry.route_read(&session, None, None).await.unwrap();
        assert_eq!(before.messages.len(), 1);
        let identity = wal_store.file_identity(&session).unwrap().unwrap();
        assert_eq!(registry.cached_read_proof(&session, identity), Some(1));
        registry.route_read(&session, None, None).await.unwrap();
        assert_eq!(wal_store.flushes.load(Ordering::Relaxed), 1);
        for i in 0..SESSION_CACHE_SESSIONS {
            registry.cache_read_proof(&test_key(&format!("proof-{i}")), 42, 1);
        }
        {
            let proofs = registry.inner.read_proofs.lock().unwrap();
            assert_eq!(proofs.entries.len(), SESSION_CACHE_SESSIONS);
            assert_eq!(proofs.oldest.len(), SESSION_CACHE_SESSIONS);
            assert!(proofs.get(&session).is_none());
            assert!(proofs.get(&test_key("proof-0")).is_some());
        }
        let after = registry.route_read(&session, None, None).await.unwrap();
        assert_eq!(after.messages, before.messages);
        assert_eq!(after.head, before.head);
        assert_eq!(wal_store.flushes.load(Ordering::Relaxed), 2);
        assert_eq!(registry.cached_read_proof(&session, identity), Some(1));
        assert_eq!(
            registry.cached_read_proof(&session, identity.wrapping_add(1)),
            None
        );
    }

    struct StaleAttributionPages;

    impl broca_core::MaterializedStore for StaleAttributionPages {
        fn read_prefix_with_watermark(
            &self,
            _key: &LineageKey,
        ) -> Result<broca_core::StorePrefix, String> {
            unreachable!("paged reads only")
        }

        fn read_page_with_watermark(
            &self,
            _key: &LineageKey,
            _from: Option<u64>,
            _limit: u64,
            _view: broca_core::ReadView,
        ) -> Result<broca_core::StorePageSnapshot, String> {
            unreachable!("typed attribution reads only")
        }

        fn read_page_with_attribution(
            &self,
            _key: &LineageKey,
            _from: Option<u64>,
            _limit: u64,
            _view: broca_core::ReadView,
        ) -> Result<broca_core::StorePageSnapshot, broca_core::StorePageReadError> {
            Err(broca_core::StorePageReadError::AttributionStale)
        }
    }

    #[tokio::test]
    async fn attribution_notice_cache_evicts_oldest_and_deduplicates_again() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-notice-eviction-{}", std::process::id()));
        let session = BindIdentity::new(root.join("project"), "runner", "session");
        let wal_store = Arc::new(FileWalStore::new(root.join("wal")));
        wal_store
            .open(&session)
            .unwrap()
            .append(1, &started_record(&session), true)
            .unwrap();
        let registry = LineageRegistry::new_with_reader(
            Arc::new(NeverOpener),
            wal_store,
            Some(Arc::new(StaleAttributionPages)),
        );
        let capture = broca_test_support::LogCapture::new();
        capture
            .scope_future(async {
                let before = registry.route_read(&session, None, None).await.unwrap();
                assert_eq!(before.messages.len(), 1);
                registry.route_read(&session, None, None).await.unwrap();
                {
                    let mut notices = registry.inner.attribution_notices.lock().unwrap();
                    for i in 0..SESSION_CACHE_SESSIONS {
                        notices.insert(test_key(&format!("notice-{i}")), ());
                    }
                    assert_eq!(notices.entries.len(), SESSION_CACHE_SESSIONS);
                    assert_eq!(notices.oldest.len(), SESSION_CACHE_SESSIONS);
                    assert!(notices.get(&session).is_none());
                    assert!(notices.get(&test_key("notice-0")).is_some());
                }
                let after = registry.route_read(&session, None, None).await.unwrap();
                assert_eq!(after.messages, before.messages);
                assert_eq!(after.head, before.head);
                registry.route_read(&session, None, None).await.unwrap();
            })
            .await;
        assert_eq!(
            capture
                .events()
                .iter()
                .filter(|event| event.target == "session"
                    && event
                        .message()
                        .starts_with("run attribution not yet stored;"))
                .count(),
            2
        );
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
                    durable_head: Arc::new(AtomicU64::new(0)),
                    run_in_flight: Arc::new(AtomicU64::new(0)),
                    first_episode: Arc::new(std::sync::OnceLock::new()),
                    approvals: Arc::default(),
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
                    durable_head: Arc::new(AtomicU64::new(0)),
                    run_in_flight: Arc::new(AtomicU64::new(0)),
                    first_episode: Arc::new(std::sync::OnceLock::new()),
                    approvals: Arc::default(),
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
            let result = runtime.block_on(registry_for_op.route_status(&session_for_op));
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
                durable_head: Arc::new(AtomicU64::new(0)),
                run_in_flight: Arc::new(AtomicU64::new(0)),
                first_episode: Arc::new(std::sync::OnceLock::new()),
                approvals: Arc::default(),
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

    #[tokio::test]
    async fn degraded_range_read_does_not_claim_wal_presence() {
        let registry = LineageRegistry::new(Arc::new(NeverOpener));
        let session = key();
        let reader = ReadSources {
            wal_store: Arc::new(CandidateWalStore),
            store: None,
        };

        let range = registry
            .read_durable_range(&reader, &session, 0, None, None)
            .await
            .expect("degraded read returns the materialized prefix");

        assert_eq!(
            (match range.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            })
            .len(),
            0
        );
        assert!(matches!(
            range.records(),
            broca_wal::WalRangeRecords::Unknown(_)
        ));
    }

    pub(super) fn started_record(session: &LineageKey) -> WalRecord {
        WalRecord::RunStarted {
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
            run_id: RunId("r1".into()),
            session: session.clone(),
            config: Box::new(RunConfig {
                render_tags: false,
                model: ModelHandle::new("stub", "stub-model"),
                tools: Vec::new().into(),
                tool_choice: ToolChoice::Auto,
                generation: Default::default(),
                response_format: Default::default(),
                provider_options: serde_json::Value::Null,
                cache_tiers: serde_json::Value::Null,
                context_epoch: 0,
                renderer_version: 1,
                frozen_render_config: serde_json::Value::Null,
                auth_selection: None,
                tools_were_omitted: true,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: Vec::new(),
                context_management: None,
            }),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "uncommitted user".into(),
                }],
            )]
            .into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        }
    }

    pub(super) fn assistant_record() -> WalRecord {
        WalRecord::ModelStepFinished {
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: (AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::Text {
                    text: "uncommitted assistant".into(),
                }],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: Some(FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_read_hides_flushed_records_until_the_writer_syncs() {
        static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-registry-durable-head-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&root);
        let session = BindIdentity::new(root.join("project"), "runner", "session");
        let file_store = Arc::new(FileWalStore::new(root.join("wal")));
        let wal = file_store.open(&session).expect("open test WAL");
        let writer = Arc::new(WalHandle::spawn(wal, 1).expect("spawn writer"));
        let recording_store = Arc::new(RecordingWalStore {
            inner: Arc::clone(&file_store),
            through_seq: Mutex::new(Vec::new()),
            flushes: AtomicU64::new(0),
        });
        let opener = Arc::new(GaugeOpener {
            durable_head: Mutex::new(Some(writer.durable_head())),
        });
        let reader: Arc<dyn WalStore> = recording_store.clone();
        let registry = LineageRegistry::new_with_reader(opener, reader, None);
        let handle = registry
            .actor_for(&session)
            .await
            .expect("install live slot");
        writer.record(started_record(&session));
        writer.record(assistant_record());
        broca_test_support::wait_for(
            "file WAL exposes both flushed records before sync",
            async {
                loop {
                    if file_store
                        .read_records(&session)
                        .expect("read flushed WAL")
                        .len()
                        == 2
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            },
            || {
                file_store
                    .read_records(&session)
                    .map(|frames| {
                        broca_test_support::record_kinds(frames.iter().map(|f| &f.record))
                    })
                    .unwrap_or_else(|e| format!("WAL read: {e}"))
            },
        )
        .await;
        let unbounded = file_store
            .read_records_range(&session, 0, None, None)
            .expect("unbounded revert-check read");
        assert_eq!(
            (match unbounded.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            })
            .len(),
            2,
            "revert check: removing the live through_seq bound exposes flushed records"
        );

        let before_sync = registry
            .route_read(&session, None, None)
            .await
            .expect("bounded live read before sync");
        assert!(before_sync.messages.is_empty());
        assert_eq!(before_sync.head, None);
        assert_eq!(
            recording_store.through_seq.lock().unwrap().last(),
            Some(&Some(0)),
            "the Open slot must bound its WAL tail by the writer's initial durable head"
        );

        assert_eq!(writer.sync().await.expect("sync test WAL"), 2);
        let after_sync = registry
            .route_read(&session, None, None)
            .await
            .expect("bounded live read after sync");
        assert_eq!(after_sync.messages.len(), 2);
        assert!(after_sync.head.is_some());
        assert_eq!(
            recording_store.through_seq.lock().unwrap().last(),
            Some(&Some(2)),
            "a successful sync must advance the registry-visible durable head"
        );

        let close = registry
            .handle()
            .begin_close(&session, handle.gen)
            .expect("begin test slot close");
        let closing_read = registry
            .route_read(&session, None, None)
            .await
            .expect("quiescent read while slot is closing");
        assert_eq!(closing_read.messages.len(), 2);
        assert_eq!(
            recording_store.through_seq.lock().unwrap().last(),
            Some(&Some(2)),
            "Closing takes a flush proof, then replays only the head observed before that proof"
        );
        close.commit();
        let idle_read = registry
            .route_read(&session, None, None)
            .await
            .expect("idle read after slot close");
        assert_eq!(idle_read.messages.len(), 2);
        // The idle-read shape depends on whether the platform exposes a stable WAL file
        // identity. With one (unix inode), the proof cached by the Closing read covers the
        // candidate head, so the unbounded candidate read is served directly. Without one
        // (Windows), the proven-head cache is disabled and every idle read takes a fresh
        // flush barrier, re-reading bounded at the candidate head observed before it.
        let expected_idle_bound = if file_store
            .file_identity(&session)
            .expect("probe WAL file identity")
            .is_some()
        {
            None
        } else {
            Some(2)
        };
        assert_eq!(
            recording_store.through_seq.lock().unwrap().last(),
            Some(&expected_idle_bound),
            "idle read: cache-covered candidate without identity support takes a fresh bounded proof"
        );

        let _ = Arc::try_unwrap(writer)
            .unwrap_or_else(|_| panic!("test owns the only writer handle"))
            .shutdown()
            .await;
        let _ = std::fs::remove_dir_all(root);
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
            durable_head: Arc::new(AtomicU64::new(0)),
            run_in_flight: Arc::new(AtomicU64::new(0)),
            first_episode: Arc::new(std::sync::OnceLock::new()),
            approvals: Arc::default(),
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
            durable_head: Arc::new(AtomicU64::new(0)),
            run_in_flight: Arc::clone(&gauge),
            first_episode: Arc::new(std::sync::OnceLock::new()),
            approvals: Arc::default(),
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

#[cfg(test)]
mod open_error_class_tests {
    use super::OpenError;

    #[test]
    fn temporary_wal_io_is_transient_but_unknown_io_stays_unclassified() {
        for kind in [
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::WouldBlock,
            std::io::ErrorKind::TimedOut,
        ] {
            let error = broca_wal::WalError::Io(std::io::Error::from(kind));
            assert!(matches!(
                OpenError::from_wal_read("read failed".into(), &error),
                OpenError::Classified {
                    class: broca_protocol::ErrorClass::Transient,
                    ..
                }
            ));
        }
        let error =
            broca_wal::WalError::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert!(matches!(
            OpenError::from_wal_read("read failed".into(), &error),
            OpenError::Open(_)
        ));
    }
}

impl LineageRegistry {
    /// Install before actors or requests can read retained WALs, so every reader and writer
    /// uses the same expiry clock and content-free index. The daemon then runs boot recovery.
    pub fn set_retention_service(&self, service: Arc<crate::retention::RetentionService>) {
        if let Some(plane) = self.inner.opener.elicitation_plane() {
            service.set_withdrawal_adapter(Arc::new(
                crate::retention_withdrawals::ScopedRetentionWithdrawalAdapter(plane),
            ));
        }
        let inner = Arc::downgrade(&self.inner);
        service.set_evictor(Arc::new(move |key| {
            if let Some(inner) = inner.upgrade() {
                LineageRegistry { inner }.evict_retention_caches(key);
            }
        }));
        let _ = self.inner.retention.set(service);
    }

    pub async fn boot_retention(&self) -> Result<usize, OpenError> {
        let Some(service) = self.inner.retention.get().cloned() else {
            return Ok(0);
        };
        let recovered = tokio::task::spawn_blocking({
            let service = service.clone();
            move || service.resume_deletions(service.clock.now_ms())
        })
        .await
        .map_err(|e| OpenError::Open(e.to_string()))?
        .map_err(OpenError::Open)?;
        for key in recovered {
            self.evict_retention_caches(&key);
        }
        let enrolled = tokio::task::spawn_blocking(move || service.recover_enrollment())
            .await
            .map_err(|e| OpenError::Open(e.to_string()))?
            .map_err(OpenError::Open)?;
        Ok(enrolled)
    }

    /// Repair failed index writes through the live actor, or under the lease when it is closed.
    /// A failed deletion keeps its indexed deadline so a later pass can retry that address.
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
        for (key, generation) in self.inner.repairs.snapshot() {
            let actor = match self.inner.slots.lock().unwrap().get(&key) {
                Some(Entry::Open { handle, .. }) => Some(handle.clone()),
                _ => None,
            };
            let repaired = if let Some(actor) = actor {
                let (reply, rx) = tokio::sync::oneshot::channel();
                actor
                    .send(crate::actor::LineageCmd::RepairRetention { reply })
                    .await
                    .is_ok()
                    && matches!(rx.await, Ok(Ok(())))
            } else {
                tokio::task::spawn_blocking({
                    let service = service.clone();
                    let key = key.clone();
                    move || {
                        let _lease = service.lease(&key)?;
                        service.rederive(&key)
                    }
                })
                .await
                .is_ok_and(|result| result.is_ok())
            };
            if repaired {
                self.inner.repairs.remove_if_unchanged(&key, generation);
            }
        }
        let recovered = tokio::task::spawn_blocking({
            let service = service.clone();
            move || {
                service.repair_discovery_flags()?;
                service.resume_deletions(now_ms)
            }
        })
        .await
        .map_err(|e| OpenError::Open(e.to_string()))?
        .map_err(OpenError::Open)?;
        let mut deleted = recovered.len();
        for key in recovered {
            self.evict_retention_caches(&key);
        }
        // Writers nominate changed addresses. Boot, queue overflow, or an index that
        // cannot vouch for recovery triggers a walk; lease failures retry only that path.
        tokio::task::spawn_blocking({
            let service = service.clone();
            move || service.enroll_changes()
        })
        .await
        .map_err(|e| OpenError::Open(e.to_string()))?
        .map_err(OpenError::Open)?;
        let candidates = service.index.candidates(now_ms).map_err(OpenError::Open)?;
        for row in candidates {
            let key = row.session;
            let result = tokio::task::spawn_blocking({
                let service = service.clone();
                let key = key.clone();
                move || {
                    let _lease = service.lease(&key)?;
                    service.delete_expired(&key, now_ms)
                }
            })
            .await;
            match result {
                Ok(Ok(Some(_))) => {
                    self.evict_retention_caches(&key);
                    deleted += 1;
                }
                Ok(Ok(None)) => {}
                _ => {
                    tracing::warn!(target: "retention", session = %broca_core::session_addr(&key), "deletion deferred; lease or durable operation unavailable")
                }
            }
        }
        tracing::info!(target: "retention", deleted, "retention sweep complete");
        Ok(deleted)
    }

    /// Retry changed WALs and poll indexed expiry deadlines every five minutes. A clock-change
    /// notification also runs a pass immediately, so tests can advance time rather than wait days.
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
            _ = service.run_withdrawal_worker() => Ok(()),
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

    #[cfg(all(test, feature = "test-clock"))]
    pub(crate) fn expiry_probe_for_test(&self, probe: Arc<dyn Fn() + Send + Sync>) {
        *self.inner.expiry_read_probe.lock().unwrap() = Some(probe);
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
        self.check_move(key, false)?;
        let Some(service) = self.inner.retention.get().cloned() else {
            return Ok(());
        };
        let head = match self.inner.slots.lock().unwrap().get(key) {
            Some(Entry::Open { handle, .. }) => Some(handle.durable_head.load(Ordering::Acquire)),
            _ => None,
        };
        let key = key.clone();
        let run = run.map(str::to_owned);
        let store = self
            .inner
            .reader
            .as_ref()
            .and_then(|reader| reader.store.clone());
        #[cfg(test)]
        let probe = self.inner.expiry_read_probe.lock().unwrap().take();
        let expired = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(probe) = probe {
                probe();
            }
            if run.is_none()
                && service
                    .wal
                    .resolve_live_path(&key)
                    .map_err(|e| e.to_string())?
                    .is_none()
            {
                let meta = store
                    .as_ref()
                    .map(|store| store.read_admission_snapshot(&key))
                    .transpose()?
                    .flatten();
                if service
                    .wal
                    .lineage_exists(&key, meta.as_ref())
                    .map_err(|e| e.to_string())?
                {
                    return Ok(None);
                }
            }
            service.expired_head(&key, run.as_deref(), head)
        })
        .await
        .map_err(|e| OpenError::Open(e.to_string()))?
        .map_err(OpenError::Open)?;
        if let Some(expired_at_ms) = expired {
            return Err(OpenError::Expired { expired_at_ms });
        }
        Ok(())
    }

    fn evict_retention_caches(&self, key: &LineageKey) {
        // A live WAL, archive member or untombstoned meta row belongs to a newer lineage.
        // Address-keyed caches must survive completion of the older lineage's deletion.
        if self.inner.reader.as_ref().is_some_and(|reader| {
            let meta = match reader
                .store
                .as_ref()
                .map(|store| store.read_admission_snapshot(key))
                .transpose()
            {
                Ok(meta) => meta.flatten(),
                Err(_) => return true,
            };
            reader
                .wal_store
                .lineage_exists(key, meta.as_ref())
                .unwrap_or(true)
        }) {
            return;
        }
        self.inner.read_proofs.lock().unwrap().remove(key);
        self.inner.dispatch_cache.lock().unwrap().remove(key);
        self.inner.frozen_senders.lock().unwrap().remove(key);
        self.inner.attribution_notices.lock().unwrap().remove(key);
        self.inner.live_ownership.lock().unwrap().remove(key);
        self.inner.owner_snapshots.lock().unwrap().remove(key);
        self.inner.cache.remove(key);
    }
}

impl RegistryHandle {
    pub(crate) fn retention_service(&self) -> Option<Arc<crate::retention::RetentionService>> {
        self.inner.retention.get().cloned()
    }
    pub(crate) fn repair_retention(&self, key: &LineageKey) {
        self.inner.repairs.insert(key);
    }
}

#[cfg(test)]
#[path = "registry_owner_tests.rs"]
mod owner_tests;
