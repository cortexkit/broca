//! `session.move_to_store`: moves one live, idle session from today's storage (its WAL and run
//! index) into the new session storage, keeping its bind triple, ids, tags, timestamps, call
//! keys, owner, scope, plan, account selection and billing. A moved session is a new log: its
//! incarnation, history version and cursors change, and an old cursor is refused
//! `cursor_expired`.
//!
//! The operation, in order:
//! 1. **Preconditions** refuse `not_movable {reason}` and write nothing.
//! 2. **Journal** `session-store/moves/<addr>`, synced. Startup acts only on journals, and from
//!    here every reader of today's storage treats the session as the new storage's
//!    (`broca_wal::owned_by_store`).
//! 3. **Fence** (`registry::store_move`), then **freeze** the WAL at its synced end with the
//!    move id as cut id, record the frozen prefix's length and SHA-256, and check the
//!    preconditions again on exactly that prefix.
//! 4. **Stage** a complete session under `session-store/staging/<move id>/`
//!    (`broca_session_convert::stage`) and **verify** it against the frozen prefix
//!    (`broca_session_convert::verify`).
//! 5. **Raise the feature floor** (the state root's list of log features a build must know to
//!    be placed on it) to `session-storage/v1`, so no build that can't serve the moved session
//!    from the new storage is installed afterwards.
//! 6. **Publish**: rename the staged log and objects into the live root, sync, then write the
//!    head with `expected = None`. The head write is the only point of no return.
//! 7. **Seal**: today's freeze becomes the `moved` marker (`migration::release`), the
//!    registry's caches and the session's materialized rows are dropped, today's late-results
//!    intake forgets the session, then the staging folder and the journal go, the journal last.
//!
//! Any failure before the head write aborts: the staging folder, any renamed live bytes, the
//! freeze and the journal go, each synced, and the session stays on today's storage unchanged.
//! After the head write the move only goes forward; [`recover_at_startup`] finishes it.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use broca_core::media_store::{MediaBytes, MediaStore};
use broca_core::RunIndexRow;
use broca_module::registry::store_move::{FenceRefusal, MoveRefusal, StoreMoveFence, StoreMover};
use broca_module::LineageRegistry;
use broca_protocol::BindIdentity;
use broca_session_convert::stage::{Stage, StageError};
use broca_session_convert::verify::{MediaRoot, Mode, Verify};
use broca_session_loop::parts::Parts as _;
use broca_session_store::backend::{LocalBackend, SessionAddr, SessionBackend as _};
use broca_wal::migration::{self, Freeze, MoveRefusal as MarkerRefusal};
use broca_wal::{FramedRecord, FrozenPrefix, MoveJournal, StoreOwnership, WalLineageId};
use serde_json::json;

/// Who holds a moved session's WAL, as the `moved` marker names it.
pub const HOLDER: &str = "session-store/v1";

/// Every operation a moved session's callers may use, which the new storage must serve before
/// any session moves: the session's reads and writes, its run and send facts, media and
/// uploads, refresh and warming, the compaction route, and every operation the reader census
/// records (`session_readers::OPERATIONS`).
pub const REQUIRED_OPERATIONS: &[&str] = &[
    crate::manifest::OP_READ,
    crate::manifest::OP_HEAD,
    crate::manifest::OP_SUBSCRIBE,
    cortexkit_role_llm_runner::ops::SESSION_HISTORY,
    "session.entries",
    "session.apply_view",
    "session.applied_view",
    crate::manifest::OP_SEND,
    crate::manifest::OP_CANCEL,
    crate::manifest::OP_RETRACT,
    crate::manifest::OP_SEND_STATUS,
    crate::manifest::OP_SESSION_SEND_STATUS,
    crate::manifest::OP_STATUS,
    crate::manifest::OP_RUN_RESULT,
    crate::manifest::OP_RUN_TOOL_CALLS,
    cortexkit_role_llm_runner::ops::SESSION_MEDIA_READ,
    "session.upload_begin",
    "session.upload_append",
    "session.upload_status",
    "session.upload_commit",
    crate::manifest::OP_REFRESH,
    crate::manifest::OP_REFRESH_POLICY,
    crate::manifest::OP_FLUSH_PREFIX,
    crate::manifest::OP_WARM,
];

/// Whether the new storage serves an operation, as `served_on_new_storage` decides for a
/// request with no parameters.
pub type Served = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// The run-index rows of a session.
pub type RunRows = Arc<dyn Fn(&BindIdentity) -> Result<Vec<RunIndexRow>, String> + Send + Sync>;

/// Drops a session's materialized rows (its projected messages, tags, episodes and pages).
pub type ForgetProjection = Arc<dyn Fn(&BindIdentity) -> Result<(), String> + Send + Sync>;

/// The operations of the `compaction-runner/v1` route. That route is dispatched by its role
/// before the serve layer's allowlist is consulted, and the registry sends a store session's
/// calls on it to the new storage (`LineageRegistry::route_compaction_v2`), so the new storage
/// serves them though the allowlist doesn't name them.
const COMPACTION_RUNNER_OPERATIONS: [&str; 3] = [
    "session.entries",
    "session.applied_view",
    "session.apply_view",
];

/// What the new storage serves, as precondition 1 reads it: the serve layer's own allowlist
/// (`served_on_new_storage`) for the session's route, and the compaction-runner route's
/// operations.
pub fn served_by_serve_layer() -> Served {
    Arc::new(|op: &str| {
        COMPACTION_RUNNER_OPERATIONS.contains(&op)
            || crate::serve::served_on_new_storage(op, &json!({}))
    })
}

/// What a move needs beyond the registry: where it writes, what it reads its preconditions
/// from, and what it seals.
pub struct StoreMove {
    pub(crate) registry: LineageRegistry,
    pub(crate) state_root: PathBuf,
    /// The new storage: its runtime creates the moved session (see `runtime`).
    pub(crate) slot: Arc<broca_module::store_open::StoreSlot>,
    pub(crate) wal_store: Arc<broca_core::FileWalStore>,
    pub(crate) leases: Arc<dyn broca_core::LeaseStore>,
    pub(crate) served: Served,
    pub(crate) run_rows: Option<RunRows>,
    pub(crate) forget_projection: Option<ForgetProjection>,
    pub(crate) late_results: Option<Arc<broca_module::late_results_store::LateResultsStore>>,
    pub(crate) held_slots: Option<Arc<broca_module::held_slots::HeldSlots>>,
    #[cfg(test)]
    pub(crate) faults: tests::Faults,
}

/// Why a move stopped.
pub(crate) enum Stop {
    Refused(MoveRefusal),
    /// A test's injected crash: nothing after it runs, as when the process dies.
    #[cfg(test)]
    Crashed,
}

impl From<MoveRefusal> for Stop {
    fn from(refusal: MoveRefusal) -> Self {
        Self::Refused(refusal)
    }
}

fn not_movable(reason: &str) -> MoveRefusal {
    MoveRefusal::new(
        "not_movable",
        format!("the session can't move into the new storage now: {reason}"),
    )
    .with_detail(json!({ "reason": reason }))
}

fn unavailable(what: &str, error: impl std::fmt::Display) -> MoveRefusal {
    MoveRefusal::new("unavailable", format!("{what}: {error}"))
}

fn io_failed(what: &'static str) -> impl Fn(std::io::Error) -> MoveRefusal {
    move |error| unavailable(what, error)
}

impl StoreMove {
    pub fn new(
        registry: LineageRegistry,
        state_root: &Path,
        slot: Arc<broca_module::store_open::StoreSlot>,
        wal_store: Arc<broca_core::FileWalStore>,
        leases: Arc<dyn broca_core::LeaseStore>,
        served: Served,
    ) -> Self {
        Self {
            registry,
            state_root: state_root.to_path_buf(),
            slot,
            wal_store,
            leases,
            served,
            run_rows: None,
            forget_projection: None,
            late_results: None,
            held_slots: None,
            #[cfg(test)]
            faults: Default::default(),
        }
    }

    pub fn with_run_rows(mut self, rows: RunRows) -> Self {
        self.run_rows = Some(rows);
        self
    }

    pub fn with_forget_projection(mut self, forget: ForgetProjection) -> Self {
        self.forget_projection = Some(forget);
        self
    }

    pub fn with_late_results(
        mut self,
        store: Arc<broca_module::late_results_store::LateResultsStore>,
        slots: Arc<broca_module::held_slots::HeldSlots>,
    ) -> Self {
        self.late_results = Some(store);
        self.held_slots = Some(slots);
        self
    }

    fn state_root(&self) -> &Path {
        &self.state_root
    }

    /// The runtime that creates the moved session, or the transient `key_unavailable` refusal
    /// store sessions get while the deployment key hasn't loaded. A move creates the session in
    /// the mode new sessions are created in (`session_storage.encryption`): a plain one as soon
    /// as the runtime is open, an encrypted one once the key has loaded.
    fn runtime(&self) -> Result<Arc<broca_module::store_open::StoreRuntime>, MoveRefusal> {
        self.slot.creating_runtime().ok_or_else(|| {
            MoveRefusal::new(
                "key_unavailable",
                broca_module::store_open::key_unavailable(self.slot.key_id()).to_string(),
            )
        })
    }

    /// A point where the move does a file operation or finishes a step. A test can crash the
    /// move here; a shipping build does nothing.
    #[allow(clippy::unnecessary_wraps)]
    fn step(&self, _name: &'static str) -> Result<(), Stop> {
        #[cfg(test)]
        if self.faults.crash_here(_name) {
            return Err(Stop::Crashed);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl StoreMover for StoreMove {
    async fn move_to_store(&self, key: &BindIdentity) -> Result<serde_json::Value, MoveRefusal> {
        match self.run(key).await {
            Ok(reply) => Ok(reply),
            Err(Stop::Refused(refusal)) => Err(refusal),
            #[cfg(test)]
            Err(Stop::Crashed) => Err(MoveRefusal::new("crashed", "a test crashed the move")),
        }
    }
}

/// What the moved session is created and read back with: the runtime's deployment key, which
/// creates sessions in the mode `session_storage.encryption` configures (plain, or encrypted
/// under that key). This is the one place a move chooses the new session's mode, for the
/// staged session's writer, for the verifier that reads it back, and for the setup parts the
/// runtime holds for it.
fn new_session_cipher(
    runtime: &broca_module::store_open::StoreRuntime,
) -> &broca_session_store::keys::DeploymentKey {
    runtime.deployment()
}

/// A new move id: `store-move:` and a random UUID (version 4).
fn new_move_id() -> Result<String, MoveRefusal> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| unavailable("a move id", error))?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(format!(
        "store-move:{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    ))
}

/// The new storage's root under `state_root`.
fn store_root(state_root: &Path) -> PathBuf {
    state_root.join(broca_wal::SESSION_STORAGE_DIR)
}

/// Writes `journal` to its file, synced: a temporary sibling, synced, renamed over the
/// journal, then the folder synced.
fn write_journal(state_root: &Path, journal: &MoveJournal) -> std::io::Result<()> {
    let path = broca_wal::session_storage_move_journal(state_root, &journal.address);
    let parent = path.parent().expect("a journal has a folder");
    migration::durable_dir(state_root, parent)?;
    let temp = path.with_extension("tmp");
    let bytes = serde_json::to_vec(journal).map_err(std::io::Error::other)?;
    {
        use std::io::Write as _;
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&temp, &path)?;
    migration::sync_dir(parent)
}

/// Removes the folder `path` and everything under it, then syncs its parent. An absent folder
/// is already removed.
fn remove_tree(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    match path.parent() {
        Some(parent) => match migration::sync_dir(parent) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            other => other,
        },
        None => Ok(()),
    }
}

/// The records of the WAL at `path` and its lineage, when the whole file reads as complete
/// records. A read that isn't complete can't show the session idle.
fn read_wal(path: &Path) -> Result<(Vec<FramedRecord>, Option<WalLineageId>), MoveRefusal> {
    let read =
        broca_wal::read_records_range(path, broca_wal::DEFAULT_MAX_READ_BYTES, 0, None, None)
            .map_err(|error| unavailable("the session's WAL couldn't be read", error))?;
    match read.records() {
        broca_wal::WalRangeRecords::Complete(records) => Ok((records.to_vec(), read.lineage_id)),
        _ => Err(not_movable(
            broca_session_convert::preconditions::SESSION_BUSY,
        )),
    }
}

/// Today's media store for a session, and the class of store the folder of its WAL chooses.
struct TodaysMedia {
    store: MediaStore,
    class: broca_core::media_store::MediaStoreClass,
}

/// Today's media store for the session whose live WAL is `wal`: the store of the class the WAL's
/// folder chooses, where the module publishes the session's media. `None` when the session's
/// `records` name no medium, or when that store doesn't exist, since opening it would create it:
/// a session naming media then has none to copy, and its move is refused for them.
fn todays_media(
    wal_store: &broca_core::FileWalStore,
    state_root: &Path,
    wal: &Path,
    records: &[FramedRecord],
) -> Result<Option<TodaysMedia>, MoveRefusal> {
    use broca_core::media_store::MediaStoreClass;
    if broca_session_convert::stage::media_digests(records).is_empty() {
        return Ok(None);
    }
    let class = if wal.parent() == Some(wal_store.durable_root().as_path()) {
        MediaStoreClass::Durable
    } else if wal.parent() == Some(wal_store.ephemeral_root().as_path()) {
        MediaStoreClass::Ephemeral
    } else {
        MediaStoreClass::NonDurable
    };
    let root = state_root.join(class.directory());
    if !(root.join("objects").is_dir() && root.join("refs").is_dir()) {
        return Ok(None);
    }
    MediaStore::open(state_root, class)
        .map(|store| Some(TodaysMedia { store, class }))
        .map_err(io_failed("today's media store couldn't be opened"))
}

fn media_bytes(media: &Option<TodaysMedia>) -> Option<&dyn MediaBytes> {
    media.as_ref().map(|media| &media.store as &dyn MediaBytes)
}

/// The move's paths, from its journal.
struct Paths {
    wal: PathBuf,
    staging: PathBuf,
    live_logs: PathBuf,
    live_objects: PathBuf,
    journal: PathBuf,
}

impl Paths {
    fn of(state_root: &Path, journal: &MoveJournal) -> Self {
        let store = store_root(state_root);
        Self {
            wal: state_root.join(&journal.wal),
            staging: store.join(&journal.staging),
            live_logs: store.join("logs").join(&journal.address),
            live_objects: store.join("objects").join(&journal.address),
            journal: broca_wal::session_storage_move_journal(state_root, &journal.address),
        }
    }
}

impl StoreMove {
    pub(crate) async fn run(&self, key: &BindIdentity) -> Result<serde_json::Value, Stop> {
        let state_root = self.state_root().to_path_buf();
        let address = broca_core::session_addr(key);
        match broca_wal::store_ownership(&state_root, &address)
            .map_err(io_failed("the session's owner couldn't be checked"))?
        {
            StoreOwnership::Today => {}
            StoreOwnership::Held => return Err(not_movable("in_new_storage").into()),
            StoreOwnership::Moving { move_id } => {
                return Err(MoveRefusal::new(
                    "session_moving",
                    format!("move {move_id} is moving this session"),
                )
                .into())
            }
        }
        let runtime = self.runtime()?;
        // Precondition 1: every operation the session's callers use is served there.
        if let Some(op) = REQUIRED_OPERATIONS.iter().find(|op| !(self.served)(op)) {
            return Err(not_movable(op).into());
        }
        let Some(wal) = self
            .wal_store
            .resolve_live_path(key)
            .map_err(|error| unavailable("the session's WAL couldn't be found", error))?
        else {
            return Err(not_movable("no_live_wal").into());
        };
        // The fast refusal: the preconditions on the WAL as it stands, read without a lease.
        // They are checked again on the frozen prefix under the fence.
        let (records, lineage) = read_wal(&wal)?;
        if !records
            .iter()
            .any(|frame| matches!(frame.record, broca_wal::WalRecord::RunStarted { .. }))
        {
            return Err(not_movable("no_run").into());
        }
        self.check_idle(key, &records, lineage)?;
        let media = todays_media(&self.wal_store, &state_root, &wal, &records)?;
        self.prepare(&records, lineage, media_bytes(&media)).await?;

        let move_id = new_move_id()?;
        let journal = MoveJournal {
            move_id: move_id.clone(),
            address: address.clone(),
            project_root: key.project_root.to_string_lossy().into_owned(),
            harness: key.harness.clone(),
            session: key.session.clone(),
            lineage_id: lineage.map(|id| id.to_string()),
            wal: wal
                .strip_prefix(&state_root)
                .map_err(|_| unavailable("the session's WAL", "outside the state root"))?
                .to_string_lossy()
                .into_owned(),
            staging: format!("staging/{move_id}"),
            frozen: None,
            appends_in_conversation: None,
        };
        write_journal(&state_root, &journal)
            .map_err(io_failed("the move's journal couldn't be written"))?;
        self.step("journal")?;
        let mut fence = None;
        match self.fenced(key, &runtime, journal, &mut fence).await {
            Ok(reply) => Ok(reply),
            Err(Abort::Before(journal, stop)) => match stop {
                Stop::Refused(refusal) => {
                    // The fence holds the session through the abort, and lets it go after.
                    self.abort(&journal)?;
                    drop(fence);
                    Err(refusal.into())
                }
                #[cfg(test)]
                Stop::Crashed => Err(Stop::Crashed),
            },
            Err(Abort::After(stop)) => Err(stop),
        }
    }

    /// The preconditions an idle session meets: no open work in its WAL, its late results and
    /// its held slots; and a settled run index.
    fn check_idle(
        &self,
        key: &BindIdentity,
        records: &[FramedRecord],
        lineage: Option<WalLineageId>,
    ) -> Result<(), MoveRefusal> {
        use broca_session_convert::preconditions::{wal_refusal, SESSION_BUSY};
        if let Some(reason) = wal_refusal(records, lineage) {
            return Err(not_movable(reason));
        }
        if let Some(store) = &self.late_results {
            let rows = store
                .registration_rows()
                .map_err(|error| unavailable("the late-results intake couldn't be read", error))?;
            if rows.iter().any(|row| {
                row.call.bind == *key
                    && row.state == broca_module::late_results_store::RegistrationState::Registered
            }) {
                return Err(not_movable(SESSION_BUSY));
            }
        }
        if self
            .held_slots
            .as_ref()
            .is_some_and(|slots| slots.holds_session(key))
        {
            return Err(not_movable(SESSION_BUSY));
        }
        // Every run-index row must be terminal, have its usage recorded and owe no withdrawal.
        // A row that doesn't is finished later by today's reconcilers, which read the WAL;
        // once the session has moved they'd be refused by today's storage on every attempt.
        let Some(rows) = &self.run_rows else {
            return Err(not_movable("reconcile_owed"));
        };
        let rows =
            rows(key).map_err(|error| unavailable("the run index couldn't be read", error))?;
        if rows
            .iter()
            .any(|row| !row.state.is_terminal() || row.usage.is_none() || row.withdrawals_owed)
        {
            return Err(not_movable("reconcile_owed"));
        }
        Ok(())
    }

    /// Converts the session, refusing what the move can't carry and what the new run loop
    /// refuses, and reads the bytes of its media from `media`, today's media store for it.
    /// Writes nothing.
    async fn prepare(
        &self,
        records: &[FramedRecord],
        lineage: Option<WalLineageId>,
        media: Option<&dyn MediaBytes>,
    ) -> Result<broca_session_convert::stage::Prepared, MoveRefusal> {
        let lineage = lineage.map(|id| id.to_string());
        let prepared =
            match broca_session_convert::stage::prepare(records, lineage.as_deref(), media) {
                Ok(prepared) => prepared,
                Err(StageError::NotMovable(reason)) => return Err(not_movable(reason)),
                Err(StageError::MediaMissing(missing)) => {
                    return Err(not_movable("media_missing").with_detail(json!({
                        "reason": "media_missing",
                        "digest": missing.digest,
                        "state": missing.state,
                        "missing": missing.missing,
                    })))
                }
                Err(StageError::Lossy(lost)) => {
                    return Err(not_movable("conversion_lossy").with_detail(json!({
                        "reason": "conversion_lossy",
                        "lost": lost,
                    })))
                }
                Err(StageError::Failed(error)) => return Err(unavailable("conversion", error)),
            };
        match broca_session_convert::preconditions::converted_refusal(&prepared).await {
            Ok(None) => Ok(prepared),
            Ok(Some(reason)) => Err(not_movable(reason)),
            Err(error) => Err(unavailable("conversion", error)),
        }
    }
}

/// Where a move stopped: before its head was written, so it aborts, or after, so only
/// startup or a retry of the seal finishes it.
enum Abort {
    Before(Box<MoveJournal>, Stop),
    After(Stop),
}

impl StoreMove {
    /// Steps 2 to 7, with the journal written. `fence` keeps the fence for the caller, which
    /// lets it go after an abort.
    async fn fenced(
        &self,
        key: &BindIdentity,
        runtime: &broca_module::store_open::StoreRuntime,
        mut journal: MoveJournal,
        fence: &mut Option<StoreMoveFence>,
    ) -> Result<serde_json::Value, Abort> {
        let state_root = self.state_root().to_path_buf();
        let paths = Paths::of(&state_root, &journal);
        macro_rules! before {
            ($result:expr) => {
                match $result {
                    Ok(value) => value,
                    Err(stop) => {
                        return Err(Abort::Before(Box::new(journal.clone()), Stop::from(stop)))
                    }
                }
            };
        }
        // Fence.
        *fence = Some(before!(self
            .registry
            .fence_store_move(key, self.leases.as_ref())
            .await
            .map_err(|refusal| match refusal {
                FenceRefusal::Busy(reason) => not_movable(
                    broca_session_convert::preconditions::SESSION_BUSY,
                )
                .with_detail(json!({
                    "reason": broca_session_convert::preconditions::SESSION_BUSY,
                    "busy": reason,
                })),
                FenceRefusal::Unavailable(error) => unavailable("the session's lease", error),
            })));
        before!(self.step("fence"));
        // Freeze the WAL at its synced end.
        let end_offset =
            before!(sync_len(&paths.wal).map_err(io_failed("the WAL couldn't be synced")));
        let wanted = Freeze {
            cut_id: journal.move_id.clone(),
            end_offset,
            lineage_id: journal.lineage_id.clone().unwrap_or_default(),
        };
        match before!(migration::freeze(&state_root, &paths.wal, &wanted)
            .map_err(io_failed("the WAL couldn't be frozen")))
        {
            Ok(_) => {}
            Err(MarkerRefusal::Moving { cut_id }) => {
                return Err(Abort::Before(
                    Box::new(journal.clone()),
                    MoveRefusal::new(
                        "session_moving",
                        format!("another move ({cut_id}) has frozen this session"),
                    )
                    .into(),
                ))
            }
            Err(other) => {
                return Err(Abort::Before(
                    Box::new(journal.clone()),
                    not_movable("moved")
                        .with_detail(json!({
                            "reason": "moved",
                            "marker": format!("{other:?}"),
                        }))
                        .into(),
                ))
            }
        }
        before!(self.step("freeze"));
        let bytes = before!(
            std::fs::read(&paths.wal).map_err(io_failed("the frozen WAL couldn't be read"))
        );
        if bytes.len() as u64 != end_offset {
            return Err(Abort::Before(
                Box::new(journal.clone()),
                not_movable(broca_session_convert::preconditions::SESSION_BUSY).into(),
            ));
        }
        journal.frozen = Some(FrozenPrefix {
            end_offset,
            sha256: migration::digest(&bytes),
        });
        before!(write_journal(&state_root, &journal)
            .map_err(io_failed("the move's journal couldn't be written")));
        before!(self.step("journal_frozen"));
        // The preconditions again, on exactly the frozen prefix.
        let (records, lineage) = before!(read_wal(&paths.wal));
        before!(self.check_idle(key, &records, lineage));
        let media = before!(todays_media(
            &self.wal_store,
            &state_root,
            &paths.wal,
            &records
        ));
        let prepared = before!(self.prepare(&records, lineage, media_bytes(&media)).await);
        journal.appends_in_conversation = Some(prepared.appends_in_conversation);
        before!(write_journal(&state_root, &journal)
            .map_err(io_failed("the move's journal couldn't be written")));
        before!(self.step("journal_layout"));
        // Stage. The moved session goes on serving the last update today's head serves for it.
        let lineage_text = lineage.map(|id| id.to_string());
        let store = store_root(&state_root);
        let todays_updated_at = broca_module::head_time::updated_at_of_wal(
            &records,
            broca_module::head_time::file_written_ms(&paths.wal),
        );
        let moved = broca_session_store::record::Moved {
            move_id: journal.move_id.clone(),
            source_activity_ms: todays_updated_at,
        };
        let stage = Stage {
            records: &records,
            lineage: lineage_text.as_deref(),
            session: key,
            deployment: new_session_cipher(runtime),
            store_root: &store,
            staging: &paths.staging,
            media: media_bytes(&media),
            moved: &moved,
        };
        let staged = before!(broca_session_convert::stage::write(&stage, prepared)
            .await
            .map_err(|error| unavailable("the session couldn't be staged", error)));
        before!(self.step("stage"));
        // Verify.
        #[cfg(test)]
        let records = self.faults.planted(records);
        let verified = before!(broca_session_convert::verify::verify(Verify {
            records: &records,
            lineage,
            session: key,
            deployment: new_session_cipher(runtime),
            store_root: &store,
            staging: &paths.staging,
            converted: &staged.converted,
            mode: Mode::Move,
            media: media_bytes(&media),
            media_root: media.as_ref().map(|media| MediaRoot {
                state_root: &state_root,
                class: media.class,
            }),
            todays_updated_at,
        })
        .await
        .map_err(|mismatch| {
            MoveRefusal::new(
                "move_verification_failed",
                format!("the staged session differs from today's: {mismatch}"),
            )
            .with_detail(json!({ "check": mismatch.check, "detail": mismatch.detail }))
        }));
        before!(self.step("verify"));
        // The runtime serving the session reads its setup parts from memory: hold the staged
        // ones there, already stored.
        before!(self.hold_parts(runtime, &staged.converted));
        // Feature floor.
        before!(broca_wal::raise_floor(
            &state_root.join(broca_wal::FEATURE_FLOOR_FILENAME),
            &[broca_wal::SESSION_STORAGE_FEATURE],
        )
        .map_err(io_failed("the feature floor couldn't be raised")));
        before!(self.step("floor"));
        // Publish: the data, then the head.
        before!(self.publish_data(&journal, &paths));
        let head = before!(self.write_head(&journal, &paths).await);
        // From here the move only goes forward.
        if let Err(stop) = self.step("head") {
            return Err(Abort::After(stop));
        }
        let _ = head;
        self.seal(key, &journal, &paths, fence)
            .await
            .map_err(Abort::After)?;
        let reply = json!({
            "moved": true,
            "move_id": journal.move_id,
            "runs": verified.runs,
            "messages": verified.messages,
            "media": verified.media,
            "checks": verified.checks,
        });
        tracing::info!(
            target: "store_move",
            session = %key.session,
            address = %journal.address,
            move_id = %journal.move_id,
            runs = verified.runs,
            messages = verified.messages,
            media = verified.media,
            checks = verified.checks.len(),
            "moved a session into the new storage"
        );
        Ok(reply)
    }

    /// Holds the staged session's setup parts in the runtime's parts, marked stored: they are
    /// in the setup store already.
    fn hold_parts(
        &self,
        runtime: &broca_module::store_open::StoreRuntime,
        converted: &broca_session_convert::Converted,
    ) -> Result<(), MoveRefusal> {
        let mut parts = runtime
            .parts_for(new_session_cipher(runtime).new_sessions())
            .clone();
        for part in converted
            .records
            .iter()
            .flat_map(broca_session_convert::measure::named_parts)
        {
            let bytes = converted
                .parts
                .get(&part.digest)
                .ok_or_else(|| unavailable("a setup part", "the conversion didn't keep it"))?;
            let value: serde_json::Value = serde_json::from_slice(bytes)
                .map_err(|error| unavailable("a setup part", error))?;
            let held = parts
                .put(part.kind, &value)
                .map_err(|error| unavailable("a setup part", error))?;
            if held != *part {
                return Err(unavailable(
                    "a setup part",
                    "its canonical form differs from the stored one",
                ));
            }
            parts.mark_stored(&[held]);
        }
        Ok(())
    }

    /// Renames the staged log and objects into the live root, then syncs `logs/`, `objects/`
    /// and their parent. A missing staged folder (a session with no objects) is skipped.
    fn publish_data(&self, journal: &MoveJournal, paths: &Paths) -> Result<(), Stop> {
        let store = store_root(self.state_root());
        for (folder, live, step) in [
            ("logs", &paths.live_logs, "rename_logs"),
            ("objects", &paths.live_objects, "rename_objects"),
        ] {
            let staged = paths.staging.join(folder).join(&journal.address);
            if !staged
                .try_exists()
                .map_err(io_failed("the staged session couldn't be checked"))?
            {
                continue;
            }
            let parent = live.parent().expect("a live folder has a parent");
            migration::durable_dir(self.state_root(), parent)
                .map_err(io_failed("the new storage's folders couldn't be made"))?;
            std::fs::rename(&staged, live)
                .map_err(io_failed("the staged session couldn't be published"))?;
            self.step(step)?;
        }
        for folder in [store.join("logs"), store.join("objects"), store.clone()] {
            match migration::sync_dir(&folder) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                other => {
                    other.map_err(io_failed("the new storage's folders couldn't be synced"))?
                }
            }
        }
        self.step("sync_data")?;
        Ok(())
    }

    /// Writes the staged head as the live head, through the backend's versioned head write,
    /// with no head expected, then syncs `heads/` and reads the head back.
    async fn write_head(&self, journal: &MoveJournal, paths: &Paths) -> Result<u64, Stop> {
        let addr = SessionAddr(journal.address.clone());
        let staged = LocalBackend::new(&paths.staging);
        let (bytes, version) = staged
            .read_head(&addr)
            .await
            .map_err(|error| unavailable("the staged head couldn't be read", format!("{error:?}")))?
            .ok_or_else(|| unavailable("the staged head", "absent"))?;
        let store = store_root(self.state_root());
        let live = LocalBackend::new(&store);
        live.write_head(&addr, &bytes, None, version)
            .await
            .map_err(|error| unavailable("the head couldn't be written", format!("{error:?}")))?;
        migration::sync_dir(&store.join("heads"))
            .map_err(io_failed("the new storage's heads couldn't be synced"))?;
        let read = live
            .read_head(&addr)
            .await
            .map_err(|error| unavailable("the head couldn't be read back", format!("{error:?}")))?;
        if read != Some((bytes, version)) {
            return Err(unavailable("the head", "it reads back other than it was written").into());
        }
        Ok(version)
    }

    /// Step 7: today's side is sealed, then the staging folder and the journal go.
    async fn seal(
        &self,
        key: &BindIdentity,
        journal: &MoveJournal,
        paths: &Paths,
        fence: &mut Option<StoreMoveFence>,
    ) -> Result<(), Stop> {
        seal_today(self.state_root(), journal, paths)?;
        self.step("release")?;
        self.registry.forget_moved_session(key);
        retire(
            key,
            self.forget_projection.as_ref(),
            self.late_results.as_deref(),
        )?;
        self.step("retire")?;
        drop(fence.take());
        remove_tree(&paths.staging).map_err(io_failed("the staging folder couldn't be removed"))?;
        self.step("remove_staging")?;
        migration::unlink(&paths.journal).map_err(io_failed("the journal couldn't be removed"))?;
        self.step("remove_journal")?;
        Ok(())
    }

    /// Undoes a move whose head wasn't written, each step synced: the staged and any renamed
    /// live bytes, the freeze, the journal.
    fn abort(&self, journal: &MoveJournal) -> Result<(), Stop> {
        roll_back(self.state_root(), journal)?;
        Ok(())
    }
}

/// The WAL's length once its contents are synced.
fn sync_len(path: &Path) -> std::io::Result<u64> {
    let file = std::fs::File::open(path)?;
    file.sync_all()?;
    Ok(file.metadata()?.len())
}

/// Turns the move's freeze into the `moved` marker naming the new storage.
fn seal_today(state_root: &Path, journal: &MoveJournal, paths: &Paths) -> Result<(), MoveRefusal> {
    match migration::release(state_root, &paths.wal, &journal.move_id, HOLDER)
        .map_err(io_failed("today's copy couldn't be sealed"))?
    {
        Ok(()) => Ok(()),
        Err(refusal) => Err(unavailable(
            "today's copy couldn't be sealed",
            format!("{refusal:?}"),
        )),
    }
}

/// Drops the moved session's materialized rows, and has today's late-results intake forget its
/// registrations, which were all settled when it moved: a late entry for one of its calls is
/// then acked as a no-op.
fn retire(
    key: &BindIdentity,
    forget_projection: Option<&ForgetProjection>,
    late_results: Option<&broca_module::late_results_store::LateResultsStore>,
) -> Result<(), MoveRefusal> {
    if let Some(forget) = forget_projection {
        forget(key)
            .map_err(|error| unavailable("the session's rows couldn't be dropped", error))?;
    }
    if let Some(store) = late_results {
        let calls: Vec<(String, String)> = store
            .registration_rows()
            .map_err(|error| unavailable("the late-results intake couldn't be read", error))?
            .into_iter()
            .filter(|row| row.call.bind == *key)
            .map(|row| (row.call.module, row.call.call_key))
            .collect();
        store
            .remove_inline_calls(&calls)
            .map_err(|error| unavailable("the late-results intake couldn't be updated", error))?;
    }
    Ok(())
}

/// Undoes a move whose head wasn't written: the live log and objects it renamed, its staging
/// folder, its freeze, then its journal, each synced. Only paths the journal names for its own
/// address and move id are touched; one that is absent was never written.
fn roll_back(state_root: &Path, journal: &MoveJournal) -> Result<(), MoveRefusal> {
    let paths = Paths::of(state_root, journal);
    for folder in [&paths.live_logs, &paths.live_objects, &paths.staging] {
        remove_tree(folder).map_err(io_failed("the move's copy couldn't be removed"))?;
    }
    migration::abort_freeze(&paths.wal, &journal.move_id)
        .map_err(io_failed("the WAL's freeze couldn't be lifted"))?;
    migration::unlink(&paths.journal).map_err(io_failed("the journal couldn't be removed"))
}

/// What startup did with the move journals it found, by address.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Recovered {
    /// Moves whose head wasn't written: undone, the session left on today's storage.
    pub rolled_back: Vec<String>,
    /// Moves whose head was written: finished.
    pub finished: Vec<String>,
    /// Journals left in place because a journal, head or marker couldn't be read. Nothing
    /// serves those sessions until an operator looks: today's storage refuses them while
    /// their journal exists.
    pub refused: Vec<String>,
}

/// The journals under `state_root`, by file name. A journal's temporary sibling (a write that
/// never completed) is removed: nothing reads it.
fn journals(state_root: &Path) -> std::io::Result<Vec<(String, PathBuf)>> {
    let folder = store_root(state_root).join(broca_wal::MOVE_JOURNAL_DIR);
    let entries = match std::fs::read_dir(&folder) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut journals = Vec::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".tmp") {
            migration::unlink(&entry.path())?;
            continue;
        }
        journals.push((name, entry.path()));
    }
    journals.sort();
    Ok(journals)
}

fn read_journal(name: &str, path: &Path) -> Result<MoveJournal, String> {
    let bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    let journal: MoveJournal = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if journal.address != name || !journal.move_id.starts_with("store-move:") {
        return Err("the journal names another address or move".into());
    }
    Ok(journal)
}

/// Whether the session at `address` has a live head. Only a head found absent is absent.
fn head_exists(state_root: &Path, address: &str) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(broca_wal::session_storage_head(state_root, address)) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Startup's first pass over move journals, run before any sweep or actor and touching only
/// files: a move with no live head is undone; a move whose head was written has today's copy
/// sealed and its staging folder removed, and keeps its journal for [`finish_at_startup`].
pub fn recover_at_startup(state_root: &Path) -> Recovered {
    let mut recovered = Recovered::default();
    let journals = match journals(state_root) {
        Ok(journals) => journals,
        Err(error) => {
            tracing::error!(target: "store_move", "move journals couldn't be listed: {error}");
            return recovered;
        }
    };
    for (name, path) in journals {
        let journal = match read_journal(&name, &path) {
            Ok(journal) => journal,
            Err(error) => {
                tracing::error!(target: "store_move", address = %name, "unreadable move journal; the session isn't served: {error}");
                recovered.refused.push(name);
                continue;
            }
        };
        let outcome = match head_exists(state_root, &journal.address) {
            Err(error) => Err(format!("its head couldn't be checked: {error}")),
            Ok(false) => roll_back(state_root, &journal)
                .map(|()| recovered.rolled_back.push(name.clone()))
                .map_err(|refusal| refusal.message),
            Ok(true) => {
                let paths = Paths::of(state_root, &journal);
                seal_today(state_root, &journal, &paths)
                    .and_then(|()| {
                        remove_tree(&paths.staging)
                            .map_err(io_failed("the staging folder couldn't be removed"))
                    })
                    .map_err(|refusal| refusal.message)
            }
        };
        if let Err(error) = outcome {
            tracing::error!(target: "store_move", address = %name, move_id = %journal.move_id, "a move couldn't be settled at startup; the session isn't served: {error}");
            recovered.refused.push(name);
        }
    }
    recovered
}

/// Startup's second pass, once the stores are open and before any actor: each move whose
/// head was written has its session's materialized rows and late-results registrations
/// dropped, then its journal removed.
pub fn finish_at_startup(
    state_root: &Path,
    forget_projection: Option<&ForgetProjection>,
    late_results: Option<&broca_module::late_results_store::LateResultsStore>,
) -> Recovered {
    let mut recovered = Recovered::default();
    let journals = match journals(state_root) {
        Ok(journals) => journals,
        Err(error) => {
            tracing::error!(target: "store_move", "move journals couldn't be listed: {error}");
            return recovered;
        }
    };
    for (name, path) in journals {
        let Ok(journal) = read_journal(&name, &path) else {
            recovered.refused.push(name);
            continue;
        };
        if !matches!(head_exists(state_root, &journal.address), Ok(true)) {
            recovered.refused.push(name);
            continue;
        }
        let key = BindIdentity::new(
            &journal.project_root,
            journal.harness.clone(),
            journal.session.clone(),
        );
        let paths = Paths::of(state_root, &journal);
        let outcome = retire(&key, forget_projection, late_results).and_then(|()| {
            migration::unlink(&paths.journal).map_err(io_failed("the journal couldn't be removed"))
        });
        match outcome {
            Ok(()) => {
                tracing::info!(target: "store_move", address = %name, move_id = %journal.move_id, "finished a move into the new storage at startup");
                recovered.finished.push(name);
            }
            Err(refusal) => {
                tracing::error!(target: "store_move", address = %name, "a move couldn't be finished at startup: {}", refusal.message);
                recovered.refused.push(name);
            }
        }
    }
    recovered
}

#[cfg(test)]
#[path = "store_move_tests.rs"]
mod tests;
