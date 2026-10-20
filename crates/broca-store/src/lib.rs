//! Materialized store — `SqliteStore` (the [`MaterializedStore`] impl) + the
//! persister that projects committed WAL turns into SQLite.
//!
//! Kept in its own crate so the bundled-SQLite native dep stays OUT of the pure
//! loop crate (the same separation as the subc adapter). The runner is the
//! EXCLUSIVE writer; the persister runs off the hot path.
//!
//! Projection is INCREMENTAL + idempotent. It snapshots the committed watermark,
//! computes the newly complete turns without holding the shared connection mutex,
//! then verifies the watermark and atomically writes messages plus metadata. A crash
//! before commit changes nothing; on restart it resumes from the last committed seq.
#![forbid(unsafe_code)]

use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;

use broca_core::projection_plan::ProjectionSnapshot;
#[cfg(test)]
use broca_core::replay;
use broca_core::{
    decode_admission_checkpoint, last_control_cursor_through, session_addr, AdmissionStoreSnapshot,
    Cursor, FileWalStore, MaterializedStore, Persister, ProjectionSeed, RunIndex, RunIndexRow,
    RunIndexState, StorePageRow, StorePageSnapshot, StorePrefix, WalStore,
};
use broca_protocol::{content::Message, BindIdentity};
use broca_protocol::{FinishReason, Usage};
use broca_wal::{
    format_manifest_line, FramedRecord, WalByteAnchor, WalRangeRead, DEFAULT_MAX_READ_BYTES,
};
use rusqlite::{config::DbConfig, Connection, OptionalExtension};

mod busy;
use busy::STORE_BUSY_TIMEOUT;

pub mod bus_events;
pub mod caps;
pub mod export_index;
mod fold_identities;
pub mod retention;
pub mod retention_withdrawals;
pub mod session_refs;
pub use fold_identities::fold_identities;

pub use caps::CapsStore;

/// Auditable outcome of one explicit legacy page-usage backfill.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageUsageBackfillCounts {
    pub scanned: u64,
    pub converted: u64,
    pub empty: u64,
    pub declined: u64,
}

/// Auditable outcome of one explicit legacy control-head repair.
/// Outcome of [`SqliteStore::reconcile_active_rows`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ActiveRowReconcileCounts {
    /// Rows the index called `active`.
    pub scanned: u64,
    /// Rows whose WAL carries a durable terminal for that run; moved to the WAL's state.
    pub reconciled: u64,
    /// Rows whose WAL has NO terminal for that run. Left alone: the index and the WAL agree
    /// that nothing ended the run, so this is an abandoned run and not a stale row, and
    /// writing a terminal here would put the projection AHEAD of its authority.
    pub unsealed: u64,
    /// Rows whose WAL could not be read (absent live and archived, or undecodable). Left alone
    /// and reported separately: no future run of this repair fixes them without a log.
    pub declined: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ControlHeadRepairCounts {
    pub scanned: u64,
    pub repaired: u64,
    pub declined: u64,
    /// Of `declined`, those whose WAL is ABSENT rather than merely uncooperative.
    ///
    /// `declined` folds three causes the repair treats identically and an operator does not:
    /// a missing WAL, an unreadable one, and one whose cursor cannot be derived. Only the
    /// first is permanent -- there is no log to read, so no future run of this repair will
    /// ever fix those rows, and an operator chasing a non-zero decline count deserves to know
    /// which part of it is finished business.
    ///
    /// Counted from `WalStore::wal_present`, so a store that cannot tell contributes nothing
    /// here rather than guessing. That means this is a LOWER BOUND on an unknowing store, and
    /// exact on a file-backed one.
    ///
    /// ABSENT from `conformance::ControlHeadRepairCounts`, which shares this name and is the
    /// contract external store implementors satisfy. That is deliberate rather than drift: an
    /// implementor with no filesystem cannot compute this, and an obliged implementor would
    /// return zero, which reads as "none were permanent" instead of "I cannot tell". The two
    /// structs are not to be reconciled by adding the field there.
    pub declined_no_wal: u64,
}

/// A complete candidate for replacing the diagnostic WAL manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalManifestRebuild {
    pub contents: String,
    pub addresses: std::collections::BTreeSet<String>,
    pub sessions_without_wal: u64,
    pub project_observations: std::collections::BTreeMap<String, ProjectObservations>,
}

/// Episode observations at rebuild time, not a continuously maintained identity index.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ProjectObservations {
    pub episodes_with_project_id: u64,
    pub project_ids: std::collections::BTreeSet<String>,
}

/// Why no complete manifest candidate could be derived from the store and WAL headers.
#[derive(Debug)]
pub enum WalManifestRebuildError {
    Store(StoreError),
    InvalidStoredIdentity {
        table: &'static str,
        value: String,
        reason: String,
    },
    Wal {
        path: std::path::PathBuf,
        source: broca_wal::WalError,
    },
    MissingLineage {
        path: std::path::PathBuf,
    },
    AddressCollision {
        address: String,
        first: String,
        second: String,
    },
}

impl std::fmt::Display for WalManifestRebuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(error) => write!(f, "read session store: {error}"),
            Self::InvalidStoredIdentity {
                table,
                value,
                reason,
            } => write!(f, "invalid {table} session identity {value:?}: {reason}"),
            Self::Wal { path, source } => {
                write!(f, "read WAL header {}: {source}", path.display())
            }
            Self::MissingLineage { path } => {
                write!(f, "WAL {} has no lineage header", path.display())
            }
            Self::AddressCollision {
                address,
                first,
                second,
            } => write!(
                f,
                "stored identities {first:?} and {second:?} collide at WAL address {address}"
            ),
        }
    }
}

impl std::error::Error for WalManifestRebuildError {}

#[cfg(any(test, feature = "test-conformance"))]
pub mod conformance;

#[derive(Debug)]
pub enum StoreError {
    #[cfg(windows)]
    PrivateAccess {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    Sqlite(rusqlite::Error),
    Int(std::num::TryFromIntError),
    Json(serde_json::Error),
    /// The WAL being projected is a corrupt lineage (replay refused to fabricate a
    /// prefix). Surfaced rather than silently materializing a wrong projection.
    Replay(broca_core::ReplayError),
    InconsistentSnapshot(&'static str),
    BusEvent(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(windows)]
            StoreError::PrivateAccess { path, source } => {
                write!(f, "private database {}: {source}", path.display())
            }
            StoreError::Int(e) => write!(f, "integer conversion: {e}"),
            StoreError::Sqlite(e) => write!(f, "sqlite: {e}"),
            StoreError::Json(e) => write!(f, "json: {e}"),
            StoreError::Replay(e) => write!(f, "replay: {e}"),
            StoreError::BusEvent(e) => write!(f, "run event: {e}"),
            StoreError::InconsistentSnapshot(message) => {
                write!(f, "inconsistent snapshot: {message}")
            }
        }
    }
}

impl std::error::Error for StoreError {}

impl From<broca_core::projection_plan::ProjectionPlanError> for StoreError {
    fn from(error: broca_core::projection_plan::ProjectionPlanError) -> Self {
        use broca_core::projection_plan::ProjectionPlanError;
        match error {
            ProjectionPlanError::Json(e) => Self::Json(e),
            ProjectionPlanError::Replay(e) => Self::Replay(e),
            ProjectionPlanError::Int(e) => Self::Int(e),
            ProjectionPlanError::InconsistentSnapshot(message) => {
                Self::InconsistentSnapshot(message)
            }
        }
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::Sqlite(e)
    }
}
impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::Json(e)
    }
}
impl From<broca_core::ReplayError> for StoreError {
    fn from(e: broca_core::ReplayError) -> Self {
        StoreError::Replay(e)
    }
}

/// SQLite-backed materialized store. The connection is behind a `Mutex` so the
/// store is `Sync` (rusqlite `Connection` is `Send` not `Sync`).
///
/// The single `Mutex<Connection>` serializes database access. Projection keeps WAL replay,
/// checkpoint folding, and JSON serialization outside that mutex so unrelated sessions wait
/// only for the short snapshot and commit phases.
pub struct SqliteStore {
    conn: Mutex<Connection>,
    bus_events: bus_events::State,
    pub(crate) export_high_water: AtomicU64,
    pub(crate) export_source_instance_id: String,
    pub(crate) export_source_epoch: u64,
}

const OPTIMISTIC_PROJECTION_ATTEMPTS: usize = 3;

enum ProjectionPlan {
    NoChange {
        expected_watermark: u64,
        expected_lineage_id: Option<String>,
    },
    Reset {
        expected_watermark: u64,
        expected_lineage_id: Option<String>,
    },
    RetentionMetadata {
        expected_watermark: u64,
        expected_lineage_id: Option<String>,
        metadata: broca_core::projection_plan::RetainedWithdrawalProjection,
    },
    Write(Box<PreparedProjection>),
}

impl ProjectionPlan {
    fn expected_state(&self) -> (u64, Option<&str>) {
        match self {
            Self::NoChange {
                expected_watermark,
                expected_lineage_id,
            }
            | Self::Reset {
                expected_watermark,
                expected_lineage_id,
            }
            | Self::RetentionMetadata {
                expected_watermark,
                expected_lineage_id,
                ..
            } => (*expected_watermark, expected_lineage_id.as_deref()),
            Self::Write(prepared) => (
                prepared.expected_watermark,
                prepared.expected_lineage_id.as_deref(),
            ),
        }
    }
}

struct PreparedProjection {
    expected_watermark: u64,
    expected_lineage_id: Option<String>,
    reset: bool,
    lineage_id: Option<String>,
    new_watermark: u64,
    next_ord: i64,
    truncate_prefix_to: Option<i64>,
    message_json: Vec<String>,
    message_run_json: Vec<String>,
    message_tags_json: Vec<Option<String>>,
    final_run_json: Vec<(i64, String)>,
    completed_steps: i64,
    usage_json: String,
    retries_used: i64,
    last_step_usage_json: Option<String>,
    last_step_finish_reason_json: Option<String>,
    last_run_id: Option<String>,
    last_title: Option<String>,
    last_episode_usage_json: String,
    checkpoint_json: String,
    anchor_file_id: Option<String>,
    anchor_seq: Option<i64>,
    anchor_byte_end: Option<i64>,
    control_head: Option<Cursor>,
    deferred_calls_json: Option<String>,
    withdrawals_owed_runs: Vec<String>,
}

enum ProjectionCommit {
    Applied(u64),
    Conflict,
}

/// Restrict a store file and its sqlite sidecars to the owner.
///
/// Best-effort by design: a store that opens but cannot be chmod'd should still serve, and
/// the failure is visible to an operator through `store-maintenance private-mode`, which
/// reports what it could not tighten. Silently degrading is the wrong trade only if nothing
/// else reports it, and something does.
fn restrict_to_owner(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        for candidate in [
            path.to_path_buf(),
            path.with_extension("db-wal"),
            path.with_extension("db-shm"),
        ] {
            if candidate.exists() {
                let _ =
                    std::fs::set_permissions(&candidate, std::fs::Permissions::from_mode(0o600));
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

fn parse_meta_identity(raw: &str) -> Result<BindIdentity, WalManifestRebuildError> {
    let mut fields = raw.split('\u{1f}');
    let (Some(project_root), Some(harness), Some(session), None) =
        (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return Err(WalManifestRebuildError::InvalidStoredIdentity {
            table: "meta",
            value: raw.to_owned(),
            reason: "expected exactly three U+001F-separated fields".to_owned(),
        });
    };
    let identity = BindIdentity::new(project_root, harness, session);
    identity.validate_keyable().map_err(|reason| {
        WalManifestRebuildError::InvalidStoredIdentity {
            table: "meta",
            value: raw.to_owned(),
            reason,
        }
    })?;
    Ok(identity)
}

#[cfg(test)]
thread_local! {
    static SQLITE_OPENS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static PROJECTION_AFTER_STATE_READ_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_projection_after_state_read_hook(hook: impl FnOnce() + 'static) {
    PROJECTION_AFTER_STATE_READ_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

fn sqlite_open_observed() {
    #[cfg(test)]
    SQLITE_OPENS.with(|count| count.set(count.get() + 1));
}

/// SQLite creates sidecars itself. On Windows a protected, inheritable parent DACL
/// gives those files owner+SYSTEM access at creation, including after checkpoint/reopen.
/// Existing broad trees are refused, not silently repaired on a writer's open path.
/// SQLite's default temp_store remains unchanged: temporary databases outside the state
/// root use the user's Windows TEMP directory and are outside this state-tree ACL policy.
/// SQLite's sidecars retain the token's default owner field (possibly Administrators when
/// elevated), but their DACL grants only the account SID and SYSTEM inherited from the parent.
#[cfg(windows)]
fn prepare_private_sqlite(path: &Path) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    broca_owner_only::validate_inheritable_directory(parent)?;
    broca_owner_only::validate_if_exists(path)?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        broca_owner_only::validate_if_exists(std::path::PathBuf::from(sidecar))?;
    }
    match broca_owner_only::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

impl SqliteStore {
    /// Open (creating + migrating) a store at `path`.
    ///
    /// On Unix the file is restricted to its owner after creation. This database holds the
    /// materialized transcript -- the same prompts, tool arguments and tool results as the
    /// WAL -- and sqlite creates it through the umask, so a default install lands it 0644.
    /// The WAL writer seals its own files at creation; sqlite gives no mode hook, so the
    /// chmod happens here, immediately after open, before any row is written.
    ///
    /// Sidecars are covered too: WAL-mode sqlite creates `-wal` and `-shm` beside the
    /// database, and the `-wal` holds committed frames not yet checkpointed into the main
    /// file. Sealing only the database would leave recent transcript content readable in a
    /// file nobody thinks to look at.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        #[cfg(windows)]
        prepare_private_sqlite(path).map_err(|source| StoreError::PrivateAccess {
            path: path.to_path_buf(),
            source,
        })?;
        sqlite_open_observed();
        let conn = Connection::open(path)?;
        let (source_instance_id, source_epoch, high_water) = Self::init(&conn)?;
        restrict_to_owner(path);
        Ok(Self {
            conn: Mutex::new(conn),
            bus_events: crate::bus_events::State::default(),
            export_high_water: AtomicU64::new(high_water),
            export_source_instance_id: source_instance_id,
            export_source_epoch: source_epoch,
        })
    }

    /// An in-memory store (tests).
    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        let (source_instance_id, source_epoch, high_water) = Self::init(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            bus_events: crate::bus_events::State::default(),
            export_high_water: AtomicU64::new(high_water),
            export_source_instance_id: source_instance_id,
            export_source_epoch: source_epoch,
        })
    }

    /// Discard one session's derived transcript and its last projected WAL sequence
    /// atomically so an installed move is rebuilt from its new WAL. Billing facts and the durable
    /// imported-run IDs that prevent billing another Mac's work again are
    /// machine-local authority and are deliberately not deleted.
    pub fn drop_session_projection(&self, session: &BindIdentity) -> Result<(), String> {
        let key = Self::key(session);
        let mut conn = self.conn.lock().map_err(|e| e.to_string())?;
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM message WHERE session=?1", [&key])
            .map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM message_tag WHERE session=?1", [&key])
            .map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM meta WHERE session=?1", [&key])
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }

    /// Populate page-usage metadata for legacy rows that still have SQL `NULL`.
    ///
    /// This is deliberately an explicit operation rather than part of [`Self::open`]. A row is
    /// changed only when its checkpoint decodes and its last episode belongs to `last_run_id`.
    /// Missing, malformed, and mismatched checkpoints remain SQL `NULL`, preserving page reads'
    /// checkpoint fallback.
    pub fn backfill_page_usage(&self) -> Result<PageUsageBackfillCounts, StoreError> {
        self.backfill_page_usage_inner(true)
    }

    /// Report the page-usage backfill counts without committing any writes.
    pub fn preview_page_usage_backfill(&self) -> Result<PageUsageBackfillCounts, StoreError> {
        self.backfill_page_usage_inner(false)
    }

    fn backfill_page_usage_inner(
        &self,
        commit: bool,
    ) -> Result<PageUsageBackfillCounts, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let legacy_rows = {
            let mut statement = tx.prepare(
                "SELECT session, last_run_id, admission_checkpoint_json
                   FROM meta
                  WHERE last_episode_usage_json IS NULL",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        let mut counts = PageUsageBackfillCounts {
            scanned: legacy_rows.len() as u64,
            ..PageUsageBackfillCounts::default()
        };
        for (session, last_run_id, checkpoint_json) in legacy_rows {
            let Some(checkpoint) = checkpoint_json
                .as_deref()
                .and_then(decode_admission_checkpoint)
            else {
                counts.declined += 1;
                continue;
            };
            let Some(last_episode) = checkpoint.last_episode else {
                counts.declined += 1;
                continue;
            };
            if last_run_id.as_deref() != Some(last_episode.run_id.0.as_str()) {
                counts.declined += 1;
                continue;
            }

            let usage_json = serde_json::to_string(&last_episode.usage)?;
            tx.execute(
                "UPDATE meta SET last_episode_usage_json=?2 WHERE session=?1",
                rusqlite::params![session, usage_json],
            )?;
            if last_episode.usage.is_some() {
                counts.converted += 1;
            } else {
                counts.empty += 1;
            }
        }
        if commit {
            tx.commit()?;
        } else {
            tx.rollback()?;
        }
        Ok(counts)
    }

    /// Populate control-head metadata for legacy projected rows by replaying their WALs.
    ///
    /// Move `active` index rows whose WAL already carries a durable terminal to that terminal.
    ///
    /// The WAL is the authority and the query path reconciles a stale row on read (tier 3a in
    /// the actor), so a stale `active` row was never wrong on `run.status`. It is wrong for every
    /// reader that consumes `run_index` DIRECTLY -- an operator's SELECT, a placement gate
    /// counting live runs. Measured 2026-09-18: four `active` rows for runs that were not, one
    /// of them terminated by the shutdown seal (which then wrote only the WAL), three from
    /// August that had survived every restart since. The seal now touches the index itself;
    /// this repairs what it left behind and whatever a crash between a WAL terminal and its
    /// index write leaves in future.
    ///
    /// Only rows whose WAL HAS a terminal for the run move. A row whose WAL has no terminal
    /// is an abandoned run, not a stale row: the index and the WAL agree, and the next append
    /// to that session seals it durably. Writing a terminal from here would put the projection
    /// ahead of its authority, which is the reverse of the defect being repaired.
    pub fn reconcile_active_rows(
        &self,
        wal_store: &dyn WalStore,
    ) -> Result<ActiveRowReconcileCounts, StoreError> {
        self.reconcile_active_rows_inner(wal_store, true)
    }

    /// Report what [`Self::reconcile_active_rows`] would do without committing.
    pub fn preview_active_row_reconcile(
        &self,
        wal_store: &dyn WalStore,
    ) -> Result<ActiveRowReconcileCounts, StoreError> {
        self.reconcile_active_rows_inner(wal_store, false)
    }

    /// Every run the index calls `active`, with its session: the candidate list for the
    /// startup crash-orphan pass.
    ///
    /// Candidates only. The WAL is the authority and this row can lag it in either direction
    /// (a crash between a WAL terminal and its index write leaves a finished run here), so a
    /// caller must confirm each run from its WAL before acting. Reading the index instead of
    /// replaying every live WAL is what keeps the pass proportional to the handful of runs
    /// that might be orphaned rather than to the tens of thousands of sessions on disk.
    ///
    /// A row whose session column does not decode is left out; the WAL cannot be located
    /// without it, so there is nothing a caller could confirm.
    pub fn active_runs(&self) -> Result<Vec<(String, BindIdentity)>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut statement =
            conn.prepare("SELECT run_id, session FROM run_index WHERE state = 'active'")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut runs = Vec::new();
        for row in rows {
            let (run_id, session) = row?;
            // `index_run_active` stores the session as a JSON-encoded `BindIdentity`.
            if let Ok(session) = serde_json::from_str::<BindIdentity>(&session) {
                runs.push((run_id, session));
            }
        }
        Ok(runs)
    }

    /// Candidate episodes for restart recovery, including a pause whose index update was lost.
    /// The caller must confirm the latest pause reason from the WAL before resuming anything.
    pub fn restart_resume_candidates(&self) -> Result<Vec<(String, BindIdentity)>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut statement = conn
            .prepare("SELECT run_id, session FROM run_index WHERE state IN ('active', 'paused')")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut runs = Vec::new();
        for row in rows {
            let (run_id, session) = row?;
            if let Ok(session) = serde_json::from_str::<BindIdentity>(&session) {
                runs.push((run_id, session));
            }
        }
        Ok(runs)
    }

    fn reconcile_active_rows_inner(
        &self,
        wal_store: &dyn WalStore,
        commit: bool,
    ) -> Result<ActiveRowReconcileCounts, StoreError> {
        use broca_protocol::caller::RunFinishReason;
        use broca_wal::WalRecord;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let active_rows = {
            let mut statement =
                tx.prepare("SELECT run_id, session FROM run_index WHERE state = 'active'")?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let mut counts = ActiveRowReconcileCounts {
            scanned: active_rows.len() as u64,
            ..ActiveRowReconcileCounts::default()
        };
        for (run_id, key) in active_rows {
            // `run_index.session` is the JSON triple (see `index_run_active`), not the flat
            // `meta.session` key the control-head repair parses. Same store, two encodings;
            // reaching for the sibling's parser here read every row as "declined".
            let Some(range) = serde_json::from_str::<BindIdentity>(&key)
                .ok()
                .and_then(|session| wal_store.read_records_range(&session, 0, None, None).ok())
            else {
                counts.declined += 1;
                continue;
            };
            let lineage = range.lineage_id.map(|id| id.to_string());
            let broca_wal::WalRangeRecords::Complete(records) = range.into_records() else {
                counts.declined += 1;
                continue;
            };
            let withdrawals = match broca_core::projection_plan::retained_withdrawal_projection(
                None,
                &records,
                lineage.as_deref(),
                None,
            ) {
                Ok(metadata) => metadata,
                Err(_) => {
                    counts.declined += 1;
                    continue;
                }
            };
            let withdrawals_owed = withdrawals
                .as_ref()
                .is_some_and(|metadata| metadata.withdrawals_owed_runs.contains(&run_id));
            // Walk episodes: a terminal belongs to the most recent RunStarted before it. The
            // stale row may be for an OLDER episode of a lineage that has run again since, so
            // the latest episode's terminal is not evidence about this run.
            let mut current: Option<String> = None;
            let mut terminal = None;
            for framed in &records {
                match &framed.record {
                    WalRecord::RunStarted {
                        run_id: started, ..
                    } => {
                        current = Some(started.0.clone());
                    }
                    WalRecord::RunFinished {
                        reason,
                        usage,
                        retries_used,
                        ..
                    } if current.as_deref() == Some(run_id.as_str()) => {
                        terminal = Some((*reason, *usage, *retries_used));
                    }
                    _ => {}
                }
            }
            let Some((reason, usage, retries_used)) = terminal else {
                counts.unsealed += 1;
                continue;
            };
            // The same mapping the actor applies to a live summary; a paused run writes
            // RunPaused, never RunFinished, so no arm here can produce `Paused`.
            let state = match reason {
                RunFinishReason::Completed => RunIndexState::Completed,
                RunFinishReason::MaxSteps => RunIndexState::MaxSteps,
                RunFinishReason::BudgetExhausted => RunIndexState::BudgetExhausted,
                RunFinishReason::ContextCeiling => RunIndexState::ContextCeiling,
                RunFinishReason::Cancelled => RunIndexState::Cancelled,
                RunFinishReason::Interrupted => RunIndexState::Interrupted,
                RunFinishReason::Error => RunIndexState::Error,
                RunFinishReason::TransformUnavailable => RunIndexState::TransformUnavailable,
            };
            // Same column, same encoding as `index_run_terminal`: the bare `Usage`, never an
            // `Option` wrapper, so a row written here reads back exactly like one the actor wrote.
            let usage_json = serde_json::to_string(&usage).map_err(StoreError::Json)?;
            tx.execute(
                "UPDATE run_index
                    SET state = ?2, usage_json = ?3, retries_used = ?4,
                        state_changed_ms = ?5, withdrawals_owed = MAX(withdrawals_owed, ?6)
                  WHERE run_id = ?1 AND state = 'active'",
                rusqlite::params![
                    run_id,
                    state.as_str(),
                    usage_json,
                    i64::from(retries_used),
                    Self::now_epoch_ms(),
                    withdrawals_owed,
                ],
            )?;
            counts.reconciled += 1;
        }
        if commit {
            tx.commit()?;
        } else {
            tx.rollback()?;
        }
        Ok(counts)
    }

    /// This is deliberately an explicit operation rather than part of [`Self::open`]. Missing or
    /// unreadable WALs and WALs with no derivable cursor remain unchanged, preserving the loud
    /// full-replay fallback instead of storing a guessed stream position.
    pub fn repair_control_heads(
        &self,
        wal_store: &dyn WalStore,
    ) -> Result<ControlHeadRepairCounts, StoreError> {
        self.repair_control_heads_inner(wal_store, true)
    }

    /// Report control-head repair counts without committing any writes.
    pub fn preview_control_head_repair(
        &self,
        wal_store: &dyn WalStore,
    ) -> Result<ControlHeadRepairCounts, StoreError> {
        self.repair_control_heads_inner(wal_store, false)
    }

    fn repair_control_heads_inner(
        &self,
        wal_store: &dyn WalStore,
        commit: bool,
    ) -> Result<ControlHeadRepairCounts, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let legacy_rows = {
            let mut statement = tx.prepare(
                "SELECT session, watermark_seq
                   FROM meta
                  WHERE watermark_seq > 0 AND control_head_wal_seq IS NULL",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        let mut counts = ControlHeadRepairCounts {
            scanned: legacy_rows.len() as u64,
            ..ControlHeadRepairCounts::default()
        };
        for (key, watermark) in legacy_rows {
            let derived = bind_identity_from_store_key(&key)
                .and_then(|session| wal_store.read_records(&session).ok())
                .and_then(|records| last_control_cursor_through(&records, Some(watermark)));
            let Some(cursor) = derived else {
                counts.declined += 1;
                // Split the permanent cause out of the count. `Some(false)` is the store
                // stating neither live nor archived log exists; an archived session is
                // routinely missing its live path, not its WAL. `None` means it cannot tell, and an
                // unknowing store must not be read as evidence of absence.
                if bind_identity_from_store_key(&key)
                    .and_then(|session| wal_store.wal_present(&session))
                    == Some(false)
                {
                    counts.declined_no_wal += 1;
                }
                continue;
            };

            tx.execute(
                "UPDATE meta
                    SET control_head_wal_seq=?2, control_head_sub_index=?3
                  WHERE session=?1 AND control_head_wal_seq IS NULL",
                rusqlite::params![key, cursor.wal_seq as i64, i64::from(cursor.sub_index),],
            )?;
            counts.repaired += 1;
        }
        if commit {
            tx.commit()?;
        } else {
            tx.rollback()?;
        }
        Ok(counts)
    }

    fn init(conn: &Connection) -> Result<(String, u64, u64), StoreError> {
        crate::busy::configure_writer_connection(conn)?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        // Retain WAL sidecars while broca is down so read-only readers can open the index.
        // SQLITE_FCNTL_PERSIST_WAL requires unsafe FFI, which this crate forbids; disabling
        // the close-time checkpoint uses rusqlite's safe API instead.
        if let Err(error) = conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true) {
            tracing::warn!(%error, "could not retain SQLite WAL sidecars on close");
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta (
                 session TEXT PRIMARY KEY,
                 watermark_seq INTEGER NOT NULL,
                 lineage_id TEXT,
                 control_head_wal_seq INTEGER,
                 control_head_sub_index INTEGER,
                 context_epoch INTEGER NOT NULL,
                 completed_steps INTEGER NOT NULL DEFAULT 0,
                 usage_json TEXT NOT NULL DEFAULT '{}',
                 last_step_usage_json TEXT,
                  last_step_finish_reason_json TEXT,
                  last_run_id TEXT,
                  last_episode_usage_json TEXT,
                  wal_anchor_file_id TEXT,
                  wal_anchor_seq INTEGER,
                  wal_anchor_byte_end INTEGER,
                  prefix_len INTEGER,
                  admission_checkpoint_json TEXT
             );
             CREATE TABLE IF NOT EXISTS message (
                 session TEXT NOT NULL,
                 ord     INTEGER NOT NULL,
                 json    TEXT NOT NULL,
                 PRIMARY KEY (session, ord)
             );
              CREATE TABLE IF NOT EXISTS run_index (
                  run_id                       TEXT PRIMARY KEY,
                   -- SAME NAME AND TYPE AS meta.session, DIFFERENT ENCODING: this one is the
                   -- serialised BindIdentity as JSON, meta.session is the triple joined by
                   -- U+001F. So a join on session matches nothing and returns zero rows
                   -- rather than an error, which reads as an absence of sessions instead of a
                   -- wrong key. To join, rebuild meta's shape from the JSON fields:
                   --   json_extract(session, path to project_root) || char(31) ||
                   --   json_extract(session, path to harness)      || char(31) ||
                   --   json_extract(session, path to session)
                   session                      TEXT NOT NULL,
                  submission_id                TEXT,
                  request_identity_digest      TEXT,
                  state                        TEXT NOT NULL,
                  usage_json                   TEXT,
                  final_step_finish_reason_json TEXT,
                  retries_used                 INTEGER NOT NULL DEFAULT 0
              );
              CREATE TABLE IF NOT EXISTS retention_lineage (
                  lineage_id TEXT PRIMARY KEY,
                  session TEXT NOT NULL,
                  retention_s INTEGER NOT NULL,
                  last_activity_ms INTEGER NOT NULL,
                  expires_at_ms INTEGER,
                  wal_bytes INTEGER NOT NULL,
                  expired_at_ms INTEGER
              );
              CREATE TABLE IF NOT EXISTS retention_withdrawal (
                  lineage_id TEXT NOT NULL,
                  session TEXT NOT NULL,
                  call_key TEXT NOT NULL,
                  scope_json TEXT NOT NULL,
                  elicitation_id TEXT,
                  provider_id TEXT NOT NULL,
                  provider_incarnation TEXT,
                  expires_at_ms TEXT NOT NULL,
                  state TEXT NOT NULL CHECK(state IN ('ready','id_unknown','scope_unknown')),
                  PRIMARY KEY(lineage_id, call_key)
              );
              CREATE TABLE IF NOT EXISTS retention_withdrawal_stop (
                  lineage_id TEXT NOT NULL,
                  call_key TEXT NOT NULL,
                  reason TEXT NOT NULL CHECK(reason = 'withdrawal_retry_exhausted'),
                  PRIMARY KEY(lineage_id, call_key)
              );
               CREATE TABLE IF NOT EXISTS retention_deletion (
                   lineage_id TEXT PRIMARY KEY,
                   session TEXT NOT NULL
               );
               CREATE INDEX IF NOT EXISTS retention_lineage_expires_idx
                  ON retention_lineage(expires_at_ms) WHERE expires_at_ms IS NOT NULL;
              CREATE UNIQUE INDEX IF NOT EXISTS retention_lineage_live_session_idx
                  ON retention_lineage(session) WHERE expired_at_ms IS NULL;",
        )?;
        let columns = {
            let mut stmt = conn.prepare("PRAGMA table_info(meta)")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
            rows.collect::<Result<std::collections::HashSet<_>, _>>()?
        };
        for (column, sql_type) in [
            ("lineage_id", "TEXT"),
            ("control_head_wal_seq", "INTEGER"),
            ("control_head_sub_index", "INTEGER"),
            ("retries_used", "INTEGER"),
            ("last_step_usage_json", "TEXT"),
            ("last_step_finish_reason_json", "TEXT"),
            ("last_run_id", "TEXT"),
            ("last_episode_usage_json", "TEXT"),
            ("wal_anchor_file_id", "TEXT"),
            ("wal_anchor_seq", "INTEGER"),
            ("wal_anchor_byte_end", "INTEGER"),
            ("prefix_len", "INTEGER"),
            ("admission_checkpoint_json", "TEXT"),
            // The session's newest caller title, projected from `RunStarted`, `Queued` and
            // `Steered`. NULL when no projected send carried one, and on rows not reprojected
            // since the column existed.
            ("last_title", "TEXT"),
            ("deferred_calls_json", "TEXT"),
            ("run_attribution_version", "INTEGER NOT NULL DEFAULT 0"),
        ] {
            if !columns.contains(column) {
                conn.execute(
                    &format!("ALTER TABLE meta ADD COLUMN {column} {sql_type}"),
                    [],
                )?;
            }
        }
        let message_columns = {
            let mut stmt = conn.prepare("PRAGMA table_info(message)")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
            rows.collect::<Result<std::collections::HashSet<_>, _>>()?
        };
        if !message_columns.contains("run_json") {
            // SQL NULL means uncomputed, including rows appended by a rolled-back writer.
            // JSON null is the explicit, computed marker for an unattributable message.
            conn.execute("ALTER TABLE message ADD COLUMN run_json TEXT", [])?;
        }
        if !message_columns.contains("tags_json") {
            // A message's runner tags (a JSON array, one per block). NULL for an untagged
            // message, and on every row written before tags existed: those sessions have none.
            conn.execute("ALTER TABLE message ADD COLUMN tags_json TEXT", [])?;
        }
        // The runner-tag index: which materialized message and block hold each tag, written
        // with the message rows, so `session.resolve` finds a part without reading the
        // transcript. Rows follow their message: a truncated or reset projection drops them.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS message_tag (
                 session TEXT NOT NULL,
                 tag     INTEGER NOT NULL,
                 ord     INTEGER NOT NULL,
                 part    INTEGER NOT NULL,
                 PRIMARY KEY (session, tag)
             )",
            [],
        )?;
        // SQLite stores columns added by ALTER TABLE after the existing checkpoint blob.
        // Covering the new scalar lets page reads fetch it without traversing that blob's
        // overflow pages; the remaining selected metadata already precedes the blob.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS meta_page_usage_idx
             ON meta(session, last_episode_usage_json)",
            [],
        )?;
        // The title column was added after the checkpoint blob too, so it gets its own covering
        // index for the same reason: `session.head` reads it on every call.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS meta_last_title_idx ON meta(session, last_title)",
            [],
        )?;
        let run_index_columns = {
            let mut stmt = conn.prepare("PRAGMA table_info(run_index)")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
            rows.collect::<Result<std::collections::HashSet<_>, _>>()?
        };
        for (column, sql_type) in [
            ("request_identity_digest", "TEXT"),
            ("usage_json", "TEXT"),
            ("final_step_finish_reason_json", "TEXT"),
            ("retries_used", "INTEGER NOT NULL DEFAULT 0"),
            // Nullable on purpose, and never defaulted. A row written before this column
            // existed has an unknown transition time; both 0 and "now" would spell that
            // unknown as a real-looking reading, which is the thing the column exists to
            // prevent. NULL reads as "no time recorded".
            ("state_changed_ms", "INTEGER"),
            // The caller's title for the run's send, written at admission. NULL for a send
            // without one and for rows written before the column existed.
            ("title", "TEXT"),
            ("lineage_id", "TEXT"),
            ("expired_at_ms", "INTEGER"),
            ("withdrawals_owed", "INTEGER NOT NULL DEFAULT 0"),
        ] {
            if !run_index_columns.contains(column) {
                conn.execute(
                    &format!("ALTER TABLE run_index ADD COLUMN {column} {sql_type}"),
                    [],
                )?;
            }
        }
        // All retained sessions (including expired ones) and per-lineage run lookups
        // need an index even when no withdrawal work is owed.
        // Adding an ordinary SQLite index changes neither columns nor row bytes;
        // older binaries can keep opening and writing this database on rollback.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS run_index_lineage_session_idx ON run_index(lineage_id, session)",
            [],
        )?;
        // Keep the partial discovery index: unlike the general index, it covers the
        // withdrawals_owed predicate without reading potentially large run rows.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS run_index_discovery_lineage_idx ON run_index(lineage_id, session) WHERE withdrawals_owed=1",
            [],
        )?;
        retention::init_work_schema(conn)?;
        session_refs::init_schema(conn)?;
        bus_events::init_schema(conn)?;
        export_index::init_schema(conn).map_err(StoreError::from)
    }

    /// Wall-clock millis for a run-index state transition. Only ever compared against other
    /// wall-clock readings to answer "how long has this row been in this state", never used
    /// for ordering or identity, so clock adjustment cannot corrupt anything durable.
    fn now_epoch_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    fn key(session: &BindIdentity) -> String {
        format!(
            "{}\u{1f}{}\u{1f}{}",
            session.project_root.display(),
            session.harness,
            session.session
        )
    }

    /// Derive a complete replacement manifest without consulting the existing sidecar.
    ///
    /// Both session-bearing tables are included because `meta.session` and `run_index.session`
    /// use different encodings and either table can contain the only surviving row for a WAL.
    /// Every database row and every present WAL is read before any caller writes the result.
    pub fn rebuild_wal_manifest(
        &self,
        wal_store: &FileWalStore,
    ) -> Result<WalManifestRebuild, WalManifestRebuildError> {
        let stored_rows = {
            let conn = self.conn.lock().unwrap();
            let meta = {
                let mut statement = conn
                    .prepare("SELECT session FROM meta")
                    .map_err(StoreError::from)
                    .map_err(WalManifestRebuildError::Store)?;
                let rows = statement
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(StoreError::from)
                    .map_err(WalManifestRebuildError::Store)?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(StoreError::from)
                    .map_err(WalManifestRebuildError::Store)?
            };
            let run_index = {
                let mut statement = conn
                    .prepare("SELECT DISTINCT session FROM run_index")
                    .map_err(StoreError::from)
                    .map_err(WalManifestRebuildError::Store)?;
                let rows = statement
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(StoreError::from)
                    .map_err(WalManifestRebuildError::Store)?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(StoreError::from)
                    .map_err(WalManifestRebuildError::Store)?
            };
            (meta, run_index)
        };

        let mut identities = std::collections::HashSet::new();
        for raw in stored_rows.0 {
            identities.insert(parse_meta_identity(&raw)?);
        }
        for raw in stored_rows.1 {
            let identity: BindIdentity = serde_json::from_str(&raw).map_err(|error| {
                WalManifestRebuildError::InvalidStoredIdentity {
                    table: "run_index",
                    value: raw.clone(),
                    reason: error.to_string(),
                }
            })?;
            identity.validate_keyable().map_err(|reason| {
                WalManifestRebuildError::InvalidStoredIdentity {
                    table: "run_index",
                    value: raw,
                    reason,
                }
            })?;
            identities.insert(identity);
        }
        let mut identities: Vec<_> = identities.into_iter().collect();
        identities.sort_by_key(Self::key);

        let mut mappings: std::collections::BTreeMap<String, (broca_wal::WalLineageId, String)> =
            std::collections::BTreeMap::new();
        let mut project_observations = std::collections::BTreeMap::new();
        let mut sessions_without_wal = 0u64;
        for identity in identities {
            let path = wal_store.wal_path(&identity);
            if !path
                .try_exists()
                .map_err(|source| WalManifestRebuildError::Wal {
                    path: path.clone(),
                    source: broca_wal::WalError::Io(source),
                })?
            {
                sessions_without_wal += 1;
                continue;
            }
            let records =
                broca_wal::read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None)
                    .map_err(|source| WalManifestRebuildError::Wal {
                        path: path.clone(),
                        source,
                    })?;
            let lineage_id = records
                .lineage_id
                .ok_or_else(|| WalManifestRebuildError::MissingLineage { path: path.clone() })?;
            let address = session_addr(&identity);
            let mut observed = ProjectObservations::default();
            let broca_wal::WalRangeRecords::Complete(frames) = records.records() else {
                return Err(WalManifestRebuildError::MissingLineage { path });
            };
            for frame in frames {
                if let broca_wal::WalRecord::RunStarted {
                    project_id: Some(id),
                    ..
                } = &frame.record
                {
                    observed.episodes_with_project_id += 1;
                    observed.project_ids.insert(id.clone());
                }
            }
            project_observations.insert(address.clone(), observed);
            let identity_key = Self::key(&identity);
            if let Some((_, first)) = mappings.get(&address) {
                return Err(WalManifestRebuildError::AddressCollision {
                    address,
                    first: first.clone(),
                    second: identity_key,
                });
            }
            mappings.insert(address, (lineage_id, identity_key));
        }

        let mut contents = String::new();
        for (address, (lineage_id, identity)) in &mappings {
            let line = format_manifest_line(address, *lineage_id, identity).map_err(|error| {
                WalManifestRebuildError::InvalidStoredIdentity {
                    table: "store",
                    value: identity.to_owned(),
                    reason: error.to_string(),
                }
            })?;
            contents.push_str(&line);
        }
        Ok(WalManifestRebuild {
            contents,
            addresses: mappings.into_keys().collect(),
            sessions_without_wal,
            project_observations,
        })
    }

    /// Project the newest replay-safe prefix from either a full WAL read or the suffix returned
    /// after [`Persister::projection_seed`], with an optimistic snapshot and atomic commit.
    pub fn project(
        &self,
        session: &BindIdentity,
        records: &[FramedRecord],
    ) -> Result<u64, StoreError> {
        self.project_records(session, records, None)
    }

    fn project_records(
        &self,
        session: &BindIdentity,
        records: &[FramedRecord],
        range: Option<&WalRangeRead>,
    ) -> Result<u64, StoreError> {
        let key = Self::key(session);
        for _ in 0..OPTIMISTIC_PROJECTION_ATTEMPTS {
            let snapshot = self.projection_snapshot(&key)?;
            let plan = Self::prepare_projection(snapshot, records, range)?;
            match self.try_commit_projection(&key, plan)? {
                ProjectionCommit::Applied(watermark) => return Ok(watermark),
                ProjectionCommit::Conflict => continue,
            }
        }

        // The served path has one writer-scoped, enqueue-coalesced projector per session, and
        // imports are serialized through that session's actor. A same-session watermark race is
        // therefore limited to direct store callers or an external writer; different sessions do
        // not conflict. Bound retries anyway so unusual contention cannot starve projection, then
        // preserve correctness by computing once under the lock held through commit.
        self.project_records_locked(&key, records, range)
    }

    fn projection_snapshot(&self, key: &str) -> Result<ProjectionSnapshot, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let snapshot = Self::projection_snapshot_from_tx(&tx, key)?;
        tx.commit()?;
        Ok(snapshot)
    }

    fn projection_snapshot_from_tx(
        tx: &rusqlite::Transaction<'_>,
        key: &str,
    ) -> Result<ProjectionSnapshot, StoreError> {
        let seed = projection_seed_from_tx(tx, key)?;
        let (watermark, next_ord) = current_projection_position(tx, key)?;
        let lineage_id = current_projection_lineage_id(tx, key)?;
        let previous_control_head = current_control_head(tx, key)?;
        Ok(ProjectionSnapshot {
            attribution_complete: attribution_complete(tx, key)?,
            seed,
            watermark,
            lineage_id,
            next_ord,
            previous_control_head,
        })
    }

    fn prepare_projection(
        snapshot: ProjectionSnapshot,
        records: &[FramedRecord],
        range: Option<&WalRangeRead>,
    ) -> Result<ProjectionPlan, StoreError> {
        use broca_core::projection_plan::ProjectionPlan as NeutralPlan;
        Ok(
            match broca_core::projection_plan::plan_projection(snapshot, records, range)? {
                NeutralPlan::NoChange {
                    expected_watermark,
                    expected_lineage_id,
                } => ProjectionPlan::NoChange {
                    expected_watermark,
                    expected_lineage_id,
                },
                NeutralPlan::Reset {
                    expected_watermark,
                    expected_lineage_id,
                } => ProjectionPlan::Reset {
                    expected_watermark,
                    expected_lineage_id,
                },
                NeutralPlan::RetentionMetadata {
                    expected_watermark,
                    expected_lineage_id,
                    metadata,
                } => ProjectionPlan::RetentionMetadata {
                    expected_watermark,
                    expected_lineage_id,
                    metadata,
                },
                NeutralPlan::Write(prepared) => {
                    ProjectionPlan::Write(Box::new(PreparedProjection {
                        expected_watermark: prepared.expected_watermark,
                        expected_lineage_id: prepared.expected_lineage_id,
                        reset: prepared.reset,
                        lineage_id: prepared.lineage_id,
                        new_watermark: prepared.new_watermark,
                        next_ord: prepared.next_ord,
                        truncate_prefix_to: prepared.truncate_prefix_to,
                        message_json: prepared.message_json,
                        message_run_json: prepared.message_run_json,
                        message_tags_json: prepared.message_tags_json,
                        final_run_json: prepared.final_run_json,
                        completed_steps: prepared.completed_steps,
                        usage_json: prepared.usage_json,
                        retries_used: prepared.retries_used,
                        last_step_usage_json: prepared.last_step_usage_json,
                        last_step_finish_reason_json: prepared.last_step_finish_reason_json,
                        last_run_id: prepared.last_run_id,
                        last_title: prepared.last_title,
                        last_episode_usage_json: prepared.last_episode_usage_json,
                        checkpoint_json: prepared.checkpoint_json,
                        anchor_file_id: prepared
                            .anchor
                            .as_ref()
                            .map(|anchor| anchor.file_id.to_string()),
                        anchor_seq: prepared
                            .anchor
                            .as_ref()
                            .map(|anchor| broca_core::projection_plan::u64_to_i64(anchor.seq))
                            .transpose()?,
                        anchor_byte_end: prepared
                            .anchor
                            .as_ref()
                            .map(|anchor| broca_core::projection_plan::u64_to_i64(anchor.byte_end))
                            .transpose()?,
                        control_head: prepared.control_head,
                        deferred_calls_json: prepared.deferred_calls_json,
                        withdrawals_owed_runs: prepared.withdrawals_owed_runs,
                    }))
                }
            },
        )
    }

    fn try_commit_projection(
        &self,
        key: &str,
        plan: ProjectionPlan,
    ) -> Result<ProjectionCommit, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        // A deferred read-to-write upgrade can return SQLITE_BUSY immediately,
        // bypassing the busy timeout even for a short migration transaction.
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let current_state = current_projection_state(&tx, key)?;
        #[cfg(test)]
        PROJECTION_AFTER_STATE_READ_HOOK.with(|slot| {
            if let Some(hook) = slot.borrow_mut().take() {
                hook();
            }
        });
        let conflict = {
            let expected_state = plan.expected_state();
            current_state.0 != expected_state.0 || current_state.1.as_deref() != expected_state.1
        };
        if conflict {
            tx.commit()?;
            return Ok(ProjectionCommit::Conflict);
        }
        Self::commit_projection_plan(tx, key, plan).map(ProjectionCommit::Applied)
    }

    fn project_records_locked(
        &self,
        key: &str,
        records: &[FramedRecord],
        range: Option<&WalRangeRead>,
    ) -> Result<u64, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let snapshot = Self::projection_snapshot_from_tx(&tx, key)?;
        let plan = Self::prepare_projection(snapshot, records, range)?;
        Self::commit_projection_plan(tx, key, plan)
    }

    fn commit_projection_plan(
        tx: rusqlite::Transaction<'_>,
        key: &str,
        plan: ProjectionPlan,
    ) -> Result<u64, StoreError> {
        let prepared = match plan {
            ProjectionPlan::NoChange {
                expected_watermark, ..
            } => {
                tx.commit()?;
                return Ok(expected_watermark);
            }
            ProjectionPlan::Reset { .. } => {
                tx.execute("DELETE FROM message WHERE session=?1", [key])?;
                tx.execute("DELETE FROM message_tag WHERE session=?1", [key])?;
                tx.execute("DELETE FROM meta WHERE session=?1", [key])?;
                tx.commit()?;
                return Ok(0);
            }
            ProjectionPlan::RetentionMetadata {
                expected_watermark,
                metadata,
                ..
            } => {
                Self::commit_retention_metadata(&tx, key, &metadata)?;
                tx.commit()?;
                return Ok(expected_watermark);
            }
            ProjectionPlan::Write(prepared) => *prepared,
        };

        if prepared.reset {
            tx.execute("DELETE FROM message WHERE session=?1", [key])?;
            tx.execute("DELETE FROM message_tag WHERE session=?1", [key])?;
            tx.execute("DELETE FROM meta WHERE session=?1", [key])?;
        } else if let Some(truncate_to) = prepared.truncate_prefix_to {
            tx.execute(
                "DELETE FROM message WHERE session=?1 AND ord>=?2",
                rusqlite::params![key, truncate_to],
            )?;
            tx.execute(
                "DELETE FROM message_tag WHERE session=?1 AND ord>=?2",
                rusqlite::params![key, truncate_to],
            )?;
        }
        let mut next_ord = prepared.next_ord;
        let mut tags_json = prepared.message_tags_json.into_iter();
        for (json, run_json) in prepared
            .message_json
            .into_iter()
            .zip(prepared.message_run_json)
        {
            let tags_json = tags_json.next().flatten();
            if let Some(tags_json) = &tags_json {
                let tags: broca_protocol::tags::PartTags = serde_json::from_str(tags_json)?;
                for (part, tag) in tags.tagged() {
                    tx.execute(
                        "INSERT OR REPLACE INTO message_tag (session, tag, ord, part) VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![
                            key,
                            i64::try_from(tag).map_err(StoreError::Int)?,
                            next_ord,
                            part as i64
                        ],
                    )?;
                }
            }
            tx.execute(
                "INSERT OR REPLACE INTO message (session, ord, json, run_json, tags_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![key, next_ord, json, run_json, tags_json],
            )?;
            next_ord += 1;
        }
        for (ordinal, run_json) in prepared.final_run_json {
            let run: broca_core::RunAttribution = serde_json::from_str(&run_json)?;
            // A terminal-only tail knows the final ordinal, but not the stored row's delivery
            // receipt. Update only the final flag so that metadata survives the projection seam.
            tx.execute(
                "UPDATE message SET run_json=json_set(run_json, '$.final', json('true')) WHERE session=?1 AND ord=?2 AND json_extract(run_json, '$.run_id')=?3",
                rusqlite::params![key, ordinal, run.run_id],
            )?;
        }

        tx.execute(
             "INSERT INTO meta (
                 session, watermark_seq, lineage_id, context_epoch, completed_steps, usage_json,
                 retries_used, last_step_usage_json, last_step_finish_reason_json, last_run_id,
                 last_episode_usage_json, prefix_len, admission_checkpoint_json, wal_anchor_file_id,
                 wal_anchor_seq, wal_anchor_byte_end, control_head_wal_seq, control_head_sub_index,
                 last_title
              ) VALUES (?1, ?2, ?3, 0, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
              ON CONFLICT(session) DO UPDATE SET
                 watermark_seq=?2, lineage_id=?3, completed_steps=?4, usage_json=?5,
                 retries_used=?6, last_step_usage_json=?7, last_step_finish_reason_json=?8,
                 last_run_id=?9, last_episode_usage_json=?10, prefix_len=?11,
                 admission_checkpoint_json=?12, wal_anchor_file_id=?13, wal_anchor_seq=?14,
                 wal_anchor_byte_end=?15, control_head_wal_seq=?16,
                 control_head_sub_index=?17,
                 last_title=COALESCE(?18, meta.last_title)",
            rusqlite::params![
                key,
                prepared.new_watermark as i64,
                prepared.lineage_id,
                prepared.completed_steps,
                prepared.usage_json,
                prepared.retries_used,
                prepared.last_step_usage_json,
                prepared.last_step_finish_reason_json,
                prepared.last_run_id,
                prepared.last_episode_usage_json,
                next_ord,
                prepared.checkpoint_json,
                prepared.anchor_file_id,
                prepared.anchor_seq,
                prepared.anchor_byte_end,
                prepared.control_head.map(|cursor| cursor.wal_seq as i64),
                prepared
                    .control_head
                    .map(|cursor| i64::from(cursor.sub_index)),
                // A batch with no title keeps the existing `last_title` (see `last_title` on the
                // prepared projection). A projection reset (the WAL's lineage changed) deleted the
                // session's meta row above before reprojecting from sequence one, so this insert
                // starts from no title and nothing stale survives.
                prepared.last_title,
            ],
        )?;
        tx.execute(
            "UPDATE meta SET deferred_calls_json=?2, run_attribution_version=?3 WHERE session=?1",
            rusqlite::params![key, prepared.deferred_calls_json, RUN_ATTRIBUTION_VERSION],
        )?;
        for run_id in prepared.withdrawals_owed_runs {
            tx.execute(
                "UPDATE run_index SET withdrawals_owed=1 WHERE run_id=?1",
                [run_id],
            )?;
        }
        tx.commit()?;
        Ok(prepared.new_watermark)
    }

    fn commit_retention_metadata(
        tx: &rusqlite::Transaction<'_>,
        key: &str,
        metadata: &broca_core::projection_plan::RetainedWithdrawalProjection,
    ) -> Result<(), StoreError> {
        let session = bind_identity_from_store_key(key).ok_or(StoreError::InconsistentSnapshot(
            "withdrawal metadata requires a session triple",
        ))?;
        let session_json = serde_json::to_string(&session)?;
        for run_id in &metadata.withdrawals_owed_runs {
            tx.execute(
                "UPDATE run_index SET withdrawals_owed=1 WHERE run_id=?1 AND session=?2
                 AND (lineage_id=?3 OR lineage_id IS NULL)",
                rusqlite::params![run_id, session_json, metadata.lineage_id],
            )?;
        }
        Ok(())
    }

    fn reproject_if_lineage_mismatch_inner(
        &self,
        session: &BindIdentity,
        stale_lineage_id: &str,
        complete_wal: &WalRangeRead,
    ) -> Result<(), StoreError> {
        let incoming_lineage_id = complete_wal.lineage_id.map(|id| id.to_string());
        if incoming_lineage_id
            .as_deref()
            .is_none_or(|id| id == stale_lineage_id)
        {
            return Err(StoreError::InconsistentSnapshot(
                "lineage repair requires a different identified WAL",
            ));
        }

        let key = Self::key(session);
        for _ in 0..OPTIMISTIC_PROJECTION_ATTEMPTS {
            let snapshot = self.projection_snapshot(&key)?;
            if snapshot.lineage_id.as_deref() != Some(stale_lineage_id) {
                return Ok(());
            }
            let plan = Self::prepare_projection(
                snapshot,
                match complete_wal.records() {
                    broca_wal::WalRangeRecords::Complete(records)
                    | broca_wal::WalRangeRecords::Partial(records)
                    | broca_wal::WalRangeRecords::Unknown(records) => records,
                    broca_wal::WalRangeRecords::Absent => &[],
                },
                Some(complete_wal),
            )?;
            match self.try_commit_projection(&key, plan)? {
                ProjectionCommit::Applied(_) => return Ok(()),
                ProjectionCommit::Conflict => continue,
            }
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let snapshot = Self::projection_snapshot_from_tx(&tx, &key)?;
        if snapshot.lineage_id.as_deref() != Some(stale_lineage_id) {
            tx.commit()?;
            return Ok(());
        }
        let plan = Self::prepare_projection(
            snapshot,
            match complete_wal.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
            Some(complete_wal),
        )?;
        Self::commit_projection_plan(tx, &key, plan)?;
        Ok(())
    }
}

impl MaterializedStore for SqliteStore {
    fn read_run_event(&self, event_key: &str) -> Result<Option<(BindIdentity, Vec<u8>)>, String> {
        self.get_bus_event(event_key).map_err(|e| e.to_string())
    }
    fn read_prefix_with_watermark(&self, session: &BindIdentity) -> Result<StorePrefix, String> {
        let key = Self::key(session);
        let conn = self.conn.lock().unwrap();
        // ONE read transaction: prefix + counters + watermark + epoch, consistent.
        let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
        let meta = tx
            .query_row(
                "SELECT watermark_seq, context_epoch, completed_steps, usage_json, \
                  retries_used, last_step_usage_json, last_step_finish_reason_json \
                  FROM meta WHERE session=?1",
                [&key],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)? as u64,
                        r.get::<_, i64>(1)? as u64,
                        r.get::<_, i64>(2)? as u64,
                        r.get::<_, String>(3)?,
                        r.get::<_, Option<i64>>(4)?,
                        r.get::<_, Option<String>>(5)?,
                        r.get::<_, Option<String>>(6)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| e.to_string())?;
        let encoded_prefix = read_message_json(&tx, &key).map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        drop(conn);

        // Match projection's short critical section: the transaction fixes and copies the
        // snapshot, while whole-lineage JSON decoding happens after releasing the shared
        // connection used by every session.
        let prefix = decode_messages(encoded_prefix).map_err(|e| e.to_string())?;
        let snapshot = match meta {
            Some((
                watermark_seq,
                context_epoch,
                completed_steps,
                usage_json,
                retries_used,
                last_step_usage_json,
                last_step_finish_reason_json,
            )) => StorePrefix {
                prefix,
                completed_steps,
                usage: serde_json::from_str(&usage_json).map_err(|e| e.to_string())?,
                retries_used: retries_used
                    .map_or(Ok(0), u32::try_from)
                    .map_err(|e| e.to_string())?,
                last_step_usage: last_step_usage_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| e.to_string())?,
                last_step_finish_reason: last_step_finish_reason_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| e.to_string())?,
                watermark_seq,
                context_epoch,
            },
            None if prefix.is_empty() => StorePrefix::empty(),
            None => {
                return Err(
                    "store snapshot is missing the meta row for materialized messages".into(),
                )
            }
        };
        Ok(snapshot)
    }

    fn read_admission_snapshot(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<AdmissionStoreSnapshot>, String> {
        let key = Self::key(session);
        let conn = self.conn.lock().unwrap();
        let tx = conn
            .unchecked_transaction()
            .map_err(|error| error.to_string())?;
        let snapshot = tx
            .query_row(
                "SELECT watermark_seq, lineage_id, wal_anchor_file_id, wal_anchor_seq, wal_anchor_byte_end, \
                 admission_checkpoint_json FROM meta WHERE session=?1",
                [&key],
                |row| {
                    let watermark_seq = row.get::<_, i64>(0)? as u64;
                    Ok(AdmissionStoreSnapshot {
                        watermark_seq,
                        lineage_id: row.get::<_, Option<String>>(1)?,
                        anchor: wal_anchor(
                            watermark_seq,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                        ),
                        checkpoint: row
                            .get::<_, Option<String>>(5)?
                            .as_deref()
                            .and_then(decode_admission_checkpoint),
                    })
                },
            )
            .optional()
            .map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())?;
        Ok(snapshot)
    }

    fn read_page_with_watermark(
        &self,
        session: &BindIdentity,
        from_ordinal: Option<u64>,
        limit: u64,
        view: broca_core::context::ReadView,
    ) -> Result<StorePageSnapshot, String> {
        self.read_page_with_attribution(session, from_ordinal, limit, view)
            .map_err(|error| error.to_string())
    }

    fn tag_location(
        &self,
        session: &BindIdentity,
        tag: u64,
    ) -> Result<Option<broca_core::TagLocation>, String> {
        let key = Self::key(session);
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
        let (_, prefix_len) = current_projection_position(&tx, &key).map_err(|e| e.to_string())?;
        let tag = i64::try_from(tag).map_err(|e| e.to_string())?;
        let found = tx
            .query_row(
                "SELECT ord, part FROM message_tag WHERE session=?1 AND tag=?2 AND ord<?3",
                rusqlite::params![key, tag, prefix_len],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(|e| e.to_string())?
            .map(|(ord, part)| {
                Ok::<_, String>((
                    u64::try_from(ord).map_err(|e| e.to_string())?,
                    usize::try_from(part).map_err(|e| e.to_string())?,
                ))
            })
            .transpose()?;
        Ok(Some(broca_core::TagLocation {
            found,
            indexed_through: u64::try_from(prefix_len).map_err(|e| e.to_string())?,
        }))
    }

    fn read_page_with_attribution(
        &self,
        session: &BindIdentity,
        from_ordinal: Option<u64>,
        limit: u64,
        view: broca_core::context::ReadView,
    ) -> Result<StorePageSnapshot, broca_core::StorePageReadError> {
        let key = Self::key(session);
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
        let (watermark_seq, prefix_len) =
            current_projection_position(&tx, &key).map_err(|e| e.to_string())?;
        if !attribution_complete(&tx, &key).map_err(|e| e.to_string())? {
            return Err(broca_core::StorePageReadError::AttributionStale);
        }
        let control_head = current_control_head(&tx, &key).map_err(|e| e.to_string())?;
        if watermark_seq > 0 && control_head.is_none() {
            return Err("store page metadata predates control-head tracking".into());
        }
        let prefix_len = u64::try_from(prefix_len).map_err(|e| e.to_string())?;
        let meta = tx
            .query_row(
                "SELECT lineage_id, last_run_id, wal_anchor_file_id, wal_anchor_seq, wal_anchor_byte_end, \
                  completed_steps, last_episode_usage_json, \
                  CASE WHEN last_episode_usage_json IS NULL THEN admission_checkpoint_json END \
                  FROM meta INDEXED BY meta_page_usage_idx WHERE session=?1",
                [&key],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        wal_anchor(
                            watermark_seq,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                        ),
                        row.get::<_, i64>(5)? as u64,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| e.to_string())?;
        let (lineage_id, last_run_id, anchor, completed_steps, last_episode_usage) = match meta {
            Some((_lineage_id, run_id, _anchor, _, _, _)) if prefix_len > 0 && run_id.is_none() => {
                return Err("store page metadata predates last-run tracking".into())
            }
            Some((
                lineage_id,
                run_id,
                anchor,
                completed_steps,
                usage_json,
                fallback_checkpoint_json,
            )) => {
                let usage = match usage_json {
                    Some(json) => serde_json::from_str::<Option<Usage>>(&json)
                        .map_err(|error| error.to_string())?,
                    None => fallback_checkpoint_json
                        .as_deref()
                        .and_then(decode_admission_checkpoint)
                        .and_then(|checkpoint| checkpoint.last_episode)
                        .filter(|episode| run_id.as_deref() == Some(episode.run_id.0.as_str()))
                        .and_then(|episode| episode.usage),
                };
                (
                    lineage_id,
                    run_id.map(broca_protocol::RunId),
                    anchor,
                    completed_steps,
                    usage,
                )
            }
            None if prefix_len == 0 => (None, None, None, 0, None),
            None => {
                return Err(
                    "store snapshot is missing the meta row for materialized messages".into(),
                )
            }
        };

        // Read through its own covering index: the column sits after the checkpoint blob.
        let last_title = tx
            .query_row(
                "SELECT last_title FROM meta INDEXED BY meta_last_title_idx WHERE session=?1",
                [&key],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(|e| e.to_string())?
            .flatten();

        let start = from_ordinal.unwrap_or_else(|| prefix_len.saturating_sub(limit));
        let end = start.saturating_add(limit).min(prefix_len);
        let start_i64 = i64::try_from(start).unwrap_or(i64::MAX);
        let end_i64 = i64::try_from(end).unwrap_or(i64::MAX);
        let mut stmt = tx
            .prepare(
                "SELECT ord, json, run_json, tags_json FROM message
                 WHERE session=?1 AND ord>=?2 AND ord<?3 ORDER BY ord ASC",
            )
            .map_err(|e| e.to_string())?;
        let encoded = stmt
            .query_map(rusqlite::params![key, start_i64, end_i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(|e| e.to_string())?;
        let mut rows = Vec::new();
        for encoded_row in encoded {
            let (ordinal, json, run_json, tags_json) = encoded_row.map_err(|e| e.to_string())?;
            let run_json = run_json.ok_or(broca_core::StorePageReadError::AttributionStale)?;
            let run = serde_json::from_str(&run_json).map_err(|e| e.to_string())?;
            let ordinal = u64::try_from(ordinal).map_err(|e| e.to_string())?;
            let mut message = decode_message(json, tags_json).map_err(|e| e.to_string())?;
            message.ordinal = Some(ordinal);
            rows.push(StorePageRow {
                ordinal,
                message,
                run,
            });
        }
        drop(stmt);

        // A projectable boundary ends in an assistant message or one consolidated tool
        // message, so the producing assistant origin is always among the final two rows.
        // Use the origin decoded from the final two rows only for the page that contains the
        // projection boundary. For earlier pages, read the actual final origin so replay starts
        // from the projection boundary rather than an intermediate page.
        let trailing_origin = if end == prefix_len {
            rows.iter()
                .rev()
                .take(2)
                .find_map(|row| row.message.origin.clone())
        } else {
            None
        };
        let trailing_origin = match trailing_origin {
            Some(origin) => Some(origin),
            None => trailing_origin_from_tx(&tx, &key).map_err(|e| e.to_string())?,
        };
        let deferred_calls_json: Option<String> = tx
            .query_row(
                "SELECT deferred_calls_json FROM meta WHERE session=?1",
                [&key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?
            .flatten();
        if watermark_seq > 0 && deferred_calls_json.is_none() {
            return Err("store page metadata predates approval tracking".into());
        }
        let deferred_calls = deferred_calls_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|e| e.to_string())?
            .flatten();
        // Only model pages need the potentially large replacement payload. Decode it in the
        // same transaction as the rows so the view cannot come from a newer projection.
        let compaction = if view == broca_core::context::ReadView::Model {
            let checkpoint_json: Option<String> = tx
                .query_row(
                    "SELECT admission_checkpoint_json FROM meta WHERE session=?1",
                    [&key],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| e.to_string())?
                .flatten();
            checkpoint_json
                .as_deref()
                .and_then(decode_admission_checkpoint)
                .filter(|checkpoint| checkpoint.at_seq == watermark_seq)
                .and_then(|checkpoint| checkpoint.compaction)
        } else {
            None
        };
        tx.commit().map_err(|e| e.to_string())?;
        Ok(StorePageSnapshot {
            deferred_calls,
            compaction,
            watermark_seq,
            lineage_id,
            control_head,
            anchor,
            prefix_len,
            completed_steps,
            rows,
            trailing_origin,
            last_run_id,
            last_episode_usage,
            last_title,
        })
    }

    fn reproject_if_lineage_mismatch(
        &self,
        session: &BindIdentity,
        stale_lineage_id: &str,
        complete_wal: &WalRangeRead,
    ) -> Result<(), String> {
        self.reproject_if_lineage_mismatch_inner(session, stale_lineage_id, complete_wal)
            .map_err(|error| error.to_string())
    }
}

impl Persister for SqliteStore {
    fn project_retention_metadata(
        &self,
        session: &BindIdentity,
        metadata: &broca_core::projection_plan::RetainedWithdrawalProjection,
    ) -> Result<(), String> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|error| error.to_string())?;
        Self::commit_retention_metadata(&tx, &Self::key(session), metadata)
            .map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())
    }
    fn projection_seed(&self, session: &BindIdentity) -> Result<Option<ProjectionSeed>, String> {
        let key = Self::key(session);
        let conn = self.conn.lock().unwrap();
        let tx = conn
            .unchecked_transaction()
            .map_err(|error| error.to_string())?;
        let seed = projection_seed_from_tx(&tx, &key).map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())?;
        Ok(seed)
    }

    fn project(&self, session: &BindIdentity, records: &[FramedRecord]) -> Result<u64, String> {
        SqliteStore::project(self, session, records).map_err(|e| e.to_string())
    }

    fn project_range(&self, session: &BindIdentity, range: &WalRangeRead) -> Result<u64, String> {
        self.project_records(
            session,
            match range.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
            Some(range),
        )
        .map_err(|error| error.to_string())
    }

    fn persist_wal_anchor(
        &self,
        session: &BindIdentity,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<(), String> {
        let Some(anchor) = anchor else {
            return Ok(());
        };
        let seq = i64::try_from(anchor.seq).map_err(|error| error.to_string())?;
        let byte_end = i64::try_from(anchor.byte_end).map_err(|error| error.to_string())?;
        let key = Self::key(session);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE meta SET wal_anchor_file_id=?2, wal_anchor_seq=?3, wal_anchor_byte_end=?4 \
             WHERE session=?1 AND watermark_seq=?3",
            rusqlite::params![key, anchor.file_id.to_string(), seq, byte_end],
        )
        .map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl RunIndex for SqliteStore {
    fn index_run_active(&self, row: &RunIndexRow) -> Result<(), String> {
        // The full session triple is stored as JSON so lookup_run reconstructs the exact
        // BindIdentity (the flat `key()` string is lossy — it can't be parsed back).
        let session_json = serde_json::to_string(&row.session).map_err(|e| e.to_string())?;
        let usage_json = row
            .usage
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| e.to_string())?;
        let finish_reason_json = row
            .final_step_finish_reason
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| e.to_string())?;
        let mut connection = self.conn.lock().unwrap();
        let conn = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        conn.execute("INSERT INTO agent_data_writer VALUES(1)", [])
            .map_err(|error| error.to_string())?;
        session_refs::insert(&conn, &session_addr(&row.session), &row.session)
            .map_err(|error| error.to_string())?;
        conn.execute(
            // The first title written for a run wins: its `RunStarted` is written once, so a
            // later admission of the same run id (a resume) never holds a different one.
            "INSERT INTO run_index \
                 (run_id, session, submission_id, request_identity_digest, state, usage_json, final_step_finish_reason_json, retries_used, state_changed_ms, title, withdrawals_owed) \
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
             ON CONFLICT(run_id) DO UPDATE SET \
                session=excluded.session, submission_id=excluded.submission_id, \
                 request_identity_digest=COALESCE(excluded.request_identity_digest, run_index.request_identity_digest), state=excluded.state, usage_json=excluded.usage_json, \
                 final_step_finish_reason_json=excluded.final_step_finish_reason_json, retries_used=excluded.retries_used, \
                 state_changed_ms=excluded.state_changed_ms, title=COALESCE(run_index.title, excluded.title), withdrawals_owed=MAX(run_index.withdrawals_owed, excluded.withdrawals_owed) \
             WHERE run_index.state IN ('active', 'paused')",
            rusqlite::params![
                row.run_id,
                session_json,
                row.submission_id,
                row.request_identity_digest,
                row.state.as_str(),
                usage_json,
                finish_reason_json,
                row.retries_used,
                Self::now_epoch_ms(),
                row.title,
                row.withdrawals_owed,
             ],
        )
        .map_err(|e| e.to_string())?;
        conn.execute("DELETE FROM agent_data_writer", [])
            .map_err(|error| error.to_string())?;
        conn.commit().map_err(|error| error.to_string())?;
        Ok(())
    }

    fn index_run_terminal(
        &self,
        run_id: &str,
        state: RunIndexState,
        usage: Option<Usage>,
        final_step_finish_reason: Option<FinishReason>,
        retries_used: u32,
    ) -> Result<(), String> {
        let usage_json = usage
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| e.to_string())?;
        let finish_reason_json = final_step_finish_reason
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| e.to_string())?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE run_index \
             SET state=?2, usage_json=?3, final_step_finish_reason_json=?4, retries_used=?5, \
                 state_changed_ms=?6 \
              WHERE run_id=?1",
            rusqlite::params![
                run_id,
                state.as_str(),
                usage_json,
                finish_reason_json,
                retries_used,
                Self::now_epoch_ms(),
            ],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn index_run_withdrawals_owed(&self, run_id: &str, owed: bool) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        let changed = conn
            .execute(
                "UPDATE run_index SET withdrawals_owed=?2 WHERE run_id=?1",
                rusqlite::params![run_id, owed],
            )
            .map_err(|e| e.to_string())?;
        if changed != 1 {
            return Err(format!(
                "cannot record withdrawal debt: run '{run_id}' has no index row"
            ));
        }
        Ok(())
    }

    fn runs_with_withdrawals_owed(&self) -> Result<Vec<broca_core::WithdrawalRun>, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT run_id, session FROM run_index WHERE withdrawals_owed=1 AND expired_at_ms IS NULL ORDER BY run_id ASC")
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| e.to_string())?;
        rows.map(|row| {
            let (run_id, session) = row.map_err(|e| e.to_string())?;
            Ok(broca_core::WithdrawalRun {
                run_id: broca_protocol::RunId(run_id),
                session: serde_json::from_str(&session).map_err(|e| e.to_string())?,
            })
        })
        .collect()
    }

    fn lookup_run(&self, run_id: &str) -> Result<Option<RunIndexRow>, String> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT session, submission_id, request_identity_digest, state, usage_json, \
                           final_step_finish_reason_json, retries_used, title, withdrawals_owed \
                  FROM run_index WHERE run_id=?1",
                [run_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<String>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, Option<String>>(4)?,
                        r.get::<_, Option<String>>(5)?,
                        r.get::<_, i64>(6)?,
                        r.get::<_, Option<String>>(7)?,
                        r.get::<_, bool>(8)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| e.to_string())?;
        match row {
            None => Ok(None),
            Some((
                session_json,
                submission_id,
                request_identity_digest,
                state_str,
                usage_json,
                finish_reason_json,
                retries_used,
                title,
                withdrawals_owed,
            )) => {
                let session: BindIdentity =
                    serde_json::from_str(&session_json).map_err(|e| e.to_string())?;
                let state = RunIndexState::parse(&state_str)
                    .ok_or_else(|| format!("unknown run-index state '{state_str}'"))?;
                let usage = usage_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| e.to_string())?;
                let final_step_finish_reason = finish_reason_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| e.to_string())?;
                Ok(Some(RunIndexRow {
                    run_id: run_id.to_string(),
                    session,
                    submission_id,
                    request_identity_digest,
                    state,
                    usage,
                    final_step_finish_reason,
                    retries_used: u32::try_from(retries_used).map_err(|e| e.to_string())?,
                    title,
                    withdrawals_owed,
                }))
            }
        }
    }
}

fn bind_identity_from_store_key(key: &str) -> Option<BindIdentity> {
    let mut fields = key.split('\u{1f}');
    let identity = BindIdentity::new(fields.next()?, fields.next()?, fields.next()?);
    fields.next().is_none().then_some(identity)
}

fn current_control_head(
    tx: &rusqlite::Transaction<'_>,
    key: &str,
) -> Result<Option<Cursor>, StoreError> {
    let head = tx
        .query_row(
            "SELECT control_head_wal_seq, control_head_sub_index FROM meta WHERE session=?1",
            [key],
            |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?)),
        )
        .optional()?;
    match head {
        Some((Some(wal_seq), Some(sub_index))) => Ok(Some(Cursor::new(
            u64::try_from(wal_seq).map_err(|_| {
                StoreError::InconsistentSnapshot("control head WAL sequence is negative")
            })?,
            u32::try_from(sub_index).map_err(|_| {
                StoreError::InconsistentSnapshot("control head sub-index is out of range")
            })?,
        ))),
        Some((None, None)) | None => Ok(None),
        Some(_) => Err(StoreError::InconsistentSnapshot(
            "control head cursor is partially populated",
        )),
    }
}

fn current_projection_state(
    tx: &rusqlite::Transaction<'_>,
    key: &str,
) -> Result<(u64, Option<String>), StoreError> {
    Ok(tx
        .query_row(
            "SELECT watermark_seq, lineage_id FROM meta WHERE session=?1",
            [key],
            |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, Option<String>>(1)?,
                ))
            },
        )
        .optional()?
        .unwrap_or((0, None)))
}

fn current_projection_lineage_id(
    tx: &rusqlite::Transaction<'_>,
    key: &str,
) -> Result<Option<String>, StoreError> {
    Ok(tx
        .query_row(
            "SELECT lineage_id FROM meta WHERE session=?1",
            [key],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten())
}

fn current_projection_position(
    tx: &rusqlite::Transaction<'_>,
    key: &str,
) -> Result<(u64, i64), StoreError> {
    let meta = tx
        .query_row(
            "SELECT watermark_seq, prefix_len FROM meta WHERE session=?1",
            [key],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
        )
        .optional()?;
    match meta {
        Some((watermark, Some(prefix_len))) => Ok((watermark as u64, prefix_len)),
        Some((watermark, None)) => {
            let prefix_len = tx.query_row(
                "SELECT COUNT(*) FROM message WHERE session=?1",
                [key],
                |row| row.get::<_, i64>(0),
            )?;
            tx.execute(
                "UPDATE meta SET prefix_len=?2 WHERE session=?1 AND prefix_len IS NULL",
                rusqlite::params![key, prefix_len],
            )?;
            Ok((watermark as u64, prefix_len))
        }
        None => {
            let prefix_len = tx.query_row(
                "SELECT COUNT(*) FROM message WHERE session=?1",
                [key],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((0, prefix_len))
        }
    }
}

/// The `meta.run_attribution_version` a current writer stamps on every projection. Rows with
/// any other version were written by an older format and are backfilled from the WAL.
pub(crate) const RUN_ATTRIBUTION_VERSION: i64 = 2;

fn attribution_complete(tx: &rusqlite::Transaction<'_>, key: &str) -> Result<bool, StoreError> {
    let version: Option<i64> = tx
        .query_row(
            "SELECT run_attribution_version FROM meta WHERE session=?1",
            [key],
            |row| row.get(0),
        )
        .optional()?;
    // Version 2 includes ordered-delivery receipts. Older rows are backfilled from the WAL
    // on their session's first read, just like rows missing run attribution.
    if version.is_some_and(|version| version != RUN_ATTRIBUTION_VERSION) {
        return Ok(false);
    }
    // Session-scoped only: no global index build or startup scan of historical bodies.
    let unknown: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM message WHERE session=?1 AND run_json IS NULL LIMIT 1)",
        [key],
        |row| row.get(0),
    )?;
    Ok(!unknown)
}

fn projection_seed_from_tx(
    tx: &rusqlite::Transaction<'_>,
    key: &str,
) -> Result<Option<ProjectionSeed>, StoreError> {
    if !attribution_complete(tx, key)? {
        return Ok(None);
    }
    let (watermark_seq, prefix_len) = current_projection_position(tx, key)?;
    let meta = tx
        .query_row(
             "SELECT completed_steps, usage_json, COALESCE(retries_used, 0), last_run_id, wal_anchor_file_id, \
              wal_anchor_seq, wal_anchor_byte_end, last_step_usage_json, \
              last_step_finish_reason_json, admission_checkpoint_json \
              FROM meta WHERE session=?1",
            [key],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    wal_anchor(
                        watermark_seq,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                    ),
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                ))
            },
        )
        .optional()?;

    let Some((
        completed_steps,
        usage_json,
        retries_used,
        last_run_id,
        anchor,
        last_step_usage_json,
        last_step_finish_reason_json,
        admission_checkpoint_json,
    )) = meta
    else {
        return Ok((prefix_len == 0).then(ProjectionSeed::empty));
    };
    let Some(admission_checkpoint) = admission_checkpoint_json
        .as_deref()
        .and_then(decode_admission_checkpoint)
        // Checkpoints that did not capture compaction state lack the earlier setup and
        // replacement records; replaying only records after the watermark cannot recover them.
        .filter(|checkpoint| {
            checkpoint.at_seq == watermark_seq && checkpoint.compaction_complete()
        })
    else {
        return Ok(None);
    };
    let checkpoint_run_id = admission_checkpoint
        .last_episode
        .as_ref()
        .map(|episode| episode.run_id.clone());
    let last_run_id = last_run_id.map(broca_protocol::RunId);
    if last_run_id != checkpoint_run_id {
        return Ok(None);
    }
    let trailing_origin = if prefix_len == 0 {
        None
    } else {
        let Some(origin) = trailing_origin_from_tx(tx, key)? else {
            return Ok(None);
        };
        Some(origin)
    };
    let deferred_calls_json: Option<String> = tx.query_row(
        "SELECT deferred_calls_json FROM meta WHERE session=?1",
        [key],
        |row| row.get(0),
    )?;
    let Some(deferred_calls_json) = deferred_calls_json else {
        return Ok(None);
    };
    Ok(Some(ProjectionSeed {
        deferred_calls: serde_json::from_str(&deferred_calls_json)?,
        watermark_seq,
        anchor,
        prefix_len: prefix_len as u64,
        completed_steps: completed_steps as u64,
        usage: serde_json::from_str(&usage_json)?,
        retries_used: u32::try_from(retries_used)
            .map_err(|_| StoreError::InconsistentSnapshot("retries_used is out of range"))?,
        last_step_usage: last_step_usage_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        last_step_finish_reason: last_step_finish_reason_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        trailing_origin,
        last_run_id,
        admission_checkpoint,
    }))
}

fn wal_anchor(
    watermark_seq: u64,
    file_id: Option<String>,
    seq: Option<i64>,
    byte_end: Option<i64>,
) -> Option<WalByteAnchor> {
    let anchor = WalByteAnchor {
        file_id: file_id?.parse().ok()?,
        seq: u64::try_from(seq?).ok()?,
        byte_end: u64::try_from(byte_end?).ok()?,
    };
    (anchor.seq == watermark_seq).then_some(anchor)
}

fn trailing_origin_from_tx(
    tx: &rusqlite::Transaction<'_>,
    key: &str,
) -> Result<Option<broca_protocol::request::ModelHandle>, StoreError> {
    let mut stmt =
        tx.prepare("SELECT json FROM message WHERE session=?1 ORDER BY ord DESC LIMIT 2")?;
    let encoded = stmt.query_map([key], |row| row.get::<_, String>(0))?;
    for json in encoded {
        let message: Message = serde_json::from_str(&json?)?;
        if message.origin.is_some() {
            return Ok(message.origin);
        }
    }
    Ok(None)
}

type EncodedMessage = (String, Option<String>);

fn read_message_json(
    tx: &rusqlite::Transaction,
    key: &str,
) -> Result<Vec<EncodedMessage>, StoreError> {
    let mut stmt =
        tx.prepare("SELECT json, tags_json FROM message WHERE session=?1 ORDER BY ord ASC")?;
    let rows = stmt.query_map([key], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// A stored message with its runner tags, which live in their own column, attached.
fn decode_message(json: String, tags_json: Option<String>) -> Result<Message, StoreError> {
    let mut message: Message = serde_json::from_str(&json)?;
    if let Some(tags_json) = tags_json {
        let tags: broca_protocol::tags::PartTags = serde_json::from_str(&tags_json)?;
        broca_core::tags::attach_parts(&mut message, Some(tags));
    }
    Ok(message)
}

fn decode_messages(encoded: Vec<EncodedMessage>) -> Result<Vec<Message>, StoreError> {
    encoded
        .into_iter()
        .map(|(json, tags_json)| decode_message(json, tags_json))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use broca_core::{MaterializedStore, Persister, RunIndex, WalStore};
    use broca_wal::{Wal, WalError, WalRangeRead};

    use super::{replay, ProjectionCommit, SqliteStore};

    #[test]
    fn model_pages_return_atomic_compaction_and_rows() {
        crate::conformance::model_pages_return_atomic_compaction_and_rows(
            SqliteStore::open_in_memory().unwrap(),
        );
    }

    #[test]
    fn absent_compaction_checkpoint_refolds_complete_wal() {
        crate::conformance::absent_compaction_checkpoint_refolds_complete_wal(
            SqliteStore::open_in_memory().unwrap(),
        );
    }

    #[test]
    fn retention_and_approval_migrations_commute_and_are_idempotent() {
        use crate::conformance::migration_order::{
            common_schema, deferred_schema, retention_schema,
        };
        use std::collections::BTreeMap;

        fn schema(conn: &rusqlite::Connection) -> BTreeMap<String, String> {
            let mut schema = BTreeMap::new();
            for table in ["meta", "message", "run_index", "retention_lineage"] {
                let mut stmt = conn
                    .prepare(&format!("PRAGMA table_info({table})"))
                    .unwrap();
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, i64>(5)?,
                        ))
                    })
                    .unwrap();
                for row in rows {
                    let (name, ty, not_null, default, pk) = row.unwrap();
                    // ALTER TABLE appends columns in release order; named columns, defaults and
                    // constraints must converge, not their historical physical ordinals.
                    schema.insert(
                        format!("{table}.{name}"),
                        format!("{ty}/{not_null}/{default:?}/{pk}"),
                    );
                }
            }
            let mut stmt = conn.prepare("SELECT name, sql FROM sqlite_schema WHERE type='index' AND sql IS NOT NULL ORDER BY name").unwrap();
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap();
            for row in rows {
                let (name, sql) = row.unwrap();
                schema.insert(
                    format!("index.{name}"),
                    sql.split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .replace("IF NOT EXISTS ", ""),
                );
            }
            schema
        }

        let mut layouts = Vec::new();
        for retention_first in [true, false] {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            conn.execute_batch(&common_schema("INTEGER", "INTEGER"))
                .unwrap();
            if retention_first {
                conn.execute_batch(&retention_schema("INTEGER")).unwrap();
            } else {
                conn.execute_batch(&deferred_schema("INTEGER NOT NULL DEFAULT 0"))
                    .unwrap();
                conn.execute(
                    "UPDATE run_index SET withdrawals_owed=1 WHERE run_id='existing'",
                    [],
                )
                .unwrap();
            }
            SqliteStore::init(&conn).unwrap();
            let first = schema(&conn);
            assert_eq!(first["run_index.lineage_id"], "TEXT/0/None/0");
            assert_eq!(first["run_index.expired_at_ms"], "INTEGER/0/None/0");
            assert_eq!(
                first["run_index.withdrawals_owed"],
                "INTEGER/1/Some(\"0\")/0"
            );
            assert_eq!(first["meta.deferred_calls_json"], "TEXT/0/None/0");
            assert_eq!(
                first
                    .keys()
                    .filter(|key| key.starts_with("retention_lineage."))
                    .count(),
                7
            );
            assert!(first["index.retention_lineage_expires_idx"]
                .contains("WHERE expires_at_ms IS NOT NULL"));
            assert!(first["index.retention_lineage_live_session_idx"].contains("UNIQUE INDEX"));
            assert!(first["index.retention_lineage_live_session_idx"]
                .contains("WHERE expired_at_ms IS NULL"));
            SqliteStore::init(&conn).unwrap();
            assert_eq!(schema(&conn), first);
            let (title, owed): (String, bool) = conn
                .query_row(
                    "SELECT title, withdrawals_owed FROM run_index WHERE run_id='existing'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(title, "keep this title");
            assert_eq!(owed, !retention_first);
            if retention_first {
                assert_eq!(conn.query_row("SELECT COUNT(*) FROM retention_lineage WHERE lineage_id='old' AND wal_bytes=1024", [], |row| row.get::<_, i64>(0)).unwrap(), 1);
            }
            layouts.push(first);
        }
        assert_eq!(layouts[0], layouts[1]);
    }

    #[test]
    fn deferred_projection_matches_replay_golden() {
        crate::conformance::deferred_projection_matches_replay_golden(
            SqliteStore::open_in_memory().unwrap(),
        );
    }

    #[test]
    fn withdrawal_projection_excludes_answered_calls() {
        crate::conformance::withdrawal_projection_excludes_answered_calls(
            SqliteStore::open_in_memory().unwrap(),
        );
    }

    #[test]
    fn withdrawal_projection_flag_survives_restart() {
        let dir = broca_test_support::scratch_root()
            .join(format!("deferred-restart-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db");
        let session = crate::conformance::session("withdrawal-restart");
        let run_id = session.project_root.to_string_lossy().into_owned();
        let records = crate::conformance::deferred::records(&session);
        {
            let store = SqliteStore::open(&path).unwrap();
            store
                .index_run_active(&broca_core::RunIndexRow {
                    run_id: run_id.clone(),
                    session: session.clone(),
                    submission_id: None,
                    request_identity_digest: None,
                    state: broca_core::RunIndexState::Active,
                    usage: None,
                    final_step_finish_reason: None,
                    retries_used: 0,
                    title: None,
                    withdrawals_owed: false,
                })
                .unwrap();
            store.project(&session, &records).unwrap();
            store
                .index_run_terminal(&run_id, broca_core::RunIndexState::Cancelled, None, None, 0)
                .unwrap();
        }
        let store = SqliteStore::open(&path).unwrap();
        assert!(store.lookup_run(&run_id).unwrap().unwrap().withdrawals_owed);
        assert_eq!(
            store.runs_with_withdrawals_owed().unwrap(),
            [broca_core::WithdrawalRun {
                run_id: broca_protocol::RunId(run_id),
                session: session.clone()
            }]
        );
        assert_eq!(
            store
                .read_page_with_watermark(&session, Some(0), 500, broca_core::ReadView::Raw)
                .unwrap()
                .deferred_calls
                .unwrap()
                .obligations()
                .withdrawals
                .len(),
            1
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn boot_withdrawal_discovery_excludes_retention_expired_runs() {
        let store = SqliteStore::open_in_memory().unwrap();
        for (run_id, state, owed) in [
            ("active", broca_core::RunIndexState::Active, true),
            ("terminal", broca_core::RunIndexState::Cancelled, true),
            ("expired", broca_core::RunIndexState::Cancelled, true),
            ("clear", broca_core::RunIndexState::Cancelled, false),
        ] {
            store
                .index_run_active(&broca_core::RunIndexRow {
                    run_id: run_id.into(),
                    session: crate::conformance::session(run_id),
                    submission_id: None,
                    request_identity_digest: None,
                    state,
                    usage: None,
                    final_step_finish_reason: None,
                    retries_used: 0,
                    title: None,
                    withdrawals_owed: owed,
                })
                .unwrap();
        }
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE run_index SET expired_at_ms=1 WHERE run_id='expired'",
                [],
            )
            .unwrap();
        let rows = store.runs_with_withdrawals_owed().unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.run_id.0.as_str())
                .collect::<Vec<_>>(),
            ["active", "terminal"]
        );
        assert!(
            store
                .lookup_run("expired")
                .unwrap()
                .unwrap()
                .withdrawals_owed
        );
    }

    struct CountingWalStore {
        inner: broca_core::FileWalStore,
        full_reads: AtomicUsize,
        range_reads: AtomicUsize,
        range_records: AtomicUsize,
    }

    impl CountingWalStore {
        fn new(root: impl Into<std::path::PathBuf>) -> Self {
            Self {
                inner: broca_core::FileWalStore::new(root),
                full_reads: AtomicUsize::new(0),
                range_reads: AtomicUsize::new(0),
                range_records: AtomicUsize::new(0),
            }
        }
    }

    impl WalStore for CountingWalStore {
        fn open(
            &self,
            session: &broca_protocol::BindIdentity,
        ) -> Result<Box<dyn Wal + Send>, WalError> {
            self.inner.open(session)
        }

        fn read_records(
            &self,
            session: &broca_protocol::BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            self.full_reads.fetch_add(1, Ordering::SeqCst);
            self.inner.read_records(session)
        }

        fn read_records_range(
            &self,
            session: &broca_protocol::BindIdentity,
            after_seq: u64,
            through_seq: Option<u64>,
            anchor: Option<&broca_wal::WalByteAnchor>,
        ) -> Result<WalRangeRead, WalError> {
            self.range_reads.fetch_add(1, Ordering::SeqCst);
            let range = self
                .inner
                .read_records_range(session, after_seq, through_seq, anchor)?;
            self.range_records.fetch_add(
                (match range.records() {
                    broca_wal::WalRangeRecords::Complete(records)
                    | broca_wal::WalRangeRecords::Partial(records)
                    | broca_wal::WalRangeRecords::Unknown(records) => records,
                    broca_wal::WalRangeRecords::Absent => &[],
                })
                .len(),
                Ordering::SeqCst,
            );
            Ok(range)
        }
    }

    /// A run-index row records WHEN it entered its state, so a stale row is distinguishable
    /// from a live one.
    ///
    /// Without this, an operator asking "is anything running right now" reads `active` and
    /// cannot tell a run started seconds ago from one abandoned weeks ago by a process that
    /// died mid-run. The two are the same row.
    /// Every file this store creates must be owner-only, enumerated rather than named.
    ///
    /// Asserting the mode of the file under discussion is what misses the sidecars: sqlite
    /// creates `-wal` and `-shm` beside the database, and the `-wal` holds committed frames
    /// not yet checkpointed, so it carries transcript content under a name nobody thinks to
    /// check. Enumerating the directory catches a creation site nobody was thinking about,
    /// which is the case that actually happens.
    #[cfg(unix)]
    #[test]
    fn every_file_the_store_creates_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = broca_test_support::scratch_root()
            .join(format!("broca-store-mode-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = SqliteStore::open(dir.join("run-index.db")).unwrap();
        // Force a write so the -wal sidecar exists rather than being absent by luck.
        store
            .index_run_active(&broca_core::RunIndexRow {
                withdrawals_owed: false,
                title: None,
                run_id: "run-mode".into(),
                session: crate::conformance::session("mode-probe"),
                submission_id: None,
                request_identity_digest: None,
                state: broca_core::RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
            })
            .expect("index active");

        // Establish what the ambient default IS before judging what the code set. Asserting
        // owner-only alone measures the machine: under a 0077 umask every file lands 0600
        // whether or not this code sets a mode, and this test stays green with the chmod
        // deleted (verified). A check that cannot discriminate must refuse rather than pass.
        let ambient = dir.join("ambient-control");
        std::fs::File::create(&ambient).unwrap();
        let ambient_mode = std::fs::metadata(&ambient).unwrap().permissions().mode() & 0o777;
        std::fs::remove_file(&ambient).unwrap();
        assert_ne!(
            ambient_mode & 0o077,
            0,
            "the ambient umask already yields {ambient_mode:o}, so this test cannot tell an \
             explicit mode from the default -- it would pass with the chmod removed"
        );

        let mut seen = 0;
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if !path.is_file() {
                continue;
            }
            seen += 1;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(
                mode & 0o077,
                0,
                "{path:?} is group- or world-accessible ({mode:o})"
            );
        }
        assert!(
            seen >= 2,
            "expected the database and at least one sidecar, saw {seen} files -- an empty \
             directory would satisfy the mode check by having nothing to check"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_index_sidecars_survive_close_for_read_only_reopen() {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-store-persistent-wal-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("run-index.db");
        let store = SqliteStore::open(&path).expect("open writer");
        store
            .index_run_active(&broca_core::RunIndexRow {
                withdrawals_owed: false,
                title: None,
                run_id: "retained-run".into(),
                session: crate::conformance::session("retained-run"),
                submission_id: None,
                request_identity_digest: None,
                state: broca_core::RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
            })
            .expect("write run row");
        drop(store);

        assert!(
            path.with_file_name("run-index.db-wal").exists(),
            "WAL must survive close"
        );
        assert!(
            path.with_file_name("run-index.db-shm").exists(),
            "SHM must survive close"
        );
        let uri = format!("file:{}?mode=ro", path.display());
        let reader = rusqlite::Connection::open_with_flags(
            uri,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .expect("read-only SQLite reader opens while writer is down");
        let state: String = reader
            .query_row(
                "SELECT state FROM run_index WHERE run_id = 'retained-run'",
                [],
                |row| row.get(0),
            )
            .expect("read committed run row");
        assert_eq!(state, "active");
        drop(reader);

        let reopened = SqliteStore::open(&path).expect("reopen writer from retained WAL");
        assert!(
            reopened.lookup_run("retained-run").unwrap().is_some(),
            "reopen must recover the row"
        );
        drop(reopened);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_run_index_row_records_when_it_changed_state() {
        let store = sqlite_store();
        let session = crate::conformance::session("state-changed-ms");
        let before = SqliteStore::now_epoch_ms();
        store
            .index_run_active(&broca_core::RunIndexRow {
                withdrawals_owed: false,
                title: None,
                run_id: "run-ts".into(),
                session,
                submission_id: None,
                request_identity_digest: None,
                state: broca_core::RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
            })
            .expect("index active");

        let read = |store: &SqliteStore| -> Option<i64> {
            let conn = store.conn.lock().unwrap();
            conn.query_row(
                "SELECT state_changed_ms FROM run_index WHERE run_id = 'run-ts'",
                [],
                |r| r.get::<_, Option<i64>>(0),
            )
            .unwrap()
        };

        let active_at = read(&store).expect("an active row must carry a transition time");
        assert!(
            active_at >= before,
            "the stamp must be the time of the transition, not an epoch default"
        );

        std::thread::sleep(std::time::Duration::from_millis(5));
        store
            .index_run_terminal(
                "run-ts",
                broca_core::RunIndexState::Completed,
                None,
                None,
                0,
            )
            .expect("index terminal");
        let terminal_at = read(&store).expect("a terminal row must carry a transition time");
        assert!(
            terminal_at > active_at,
            "reaching a terminal state must advance the stamp, or it records creation rather \
             than transition and a long-running row looks freshly changed"
        );
    }

    fn sqlite_store() -> SqliteStore {
        SqliteStore::open_in_memory().expect("in-memory sqlite store opens")
    }

    fn assert_seeded_resume_matches_full_replay(
        store: &SqliteStore,
        session: &broca_protocol::BindIdentity,
        snapshot: &broca_core::PriorWalSnapshot,
        records: &[broca_wal::FramedRecord],
    ) {
        let prefix = MaterializedStore::read_prefix_with_watermark(store, session)
            .expect("read materialized resume prefix");
        let tail = snapshot
            .tail
            .iter()
            .filter(|record| record.seq > prefix.watermark_seq)
            .map(|record| record.record.clone())
            .collect::<Vec<_>>();
        let episode = snapshot
            .checkpoint_at_head
            .last_episode
            .as_ref()
            .expect("head checkpoint retains the episode");
        let budget_state = snapshot
            .checkpoint_at_base
            .as_ref()
            .and_then(|checkpoint| checkpoint.last_episode.as_ref())
            .and_then(|episode| episode.budget_replay_state.clone());
        let seeded = broca_core::resume_state_from_store(
            &prefix,
            &tail,
            Some(episode.config.clone()),
            Some(episode.run_id.clone()),
            budget_state,
        )
        .expect("seeded resume succeeds");
        let complete = records
            .iter()
            .map(|record| record.record.clone())
            .collect::<Vec<_>>();
        let full = replay(&complete).expect("full replay oracle succeeds");

        assert_eq!(
            serde_json::to_vec(&seeded.prompt).unwrap(),
            serde_json::to_vec(&full.prompt).unwrap(),
            "seeded and full replay prompts must be byte-identical"
        );
        assert_eq!(seeded.run_config, full.run_config);
        assert_eq!(seeded.run_id, full.run_id);
        assert_eq!(seeded.completed_steps, full.completed_steps);
        assert_eq!(seeded.tool_call_count, full.tool_call_count);
        assert_eq!(seeded.budget_nudges_fired, full.budget_nudges_fired);
        assert_eq!(seeded.budget_finalize_fired, full.budget_finalize_fired);
        assert_eq!(seeded.budget_finalize_pending, full.budget_finalize_pending);
        assert_eq!(seeded.usage, full.usage);
        assert_eq!(seeded.retries_used, full.retries_used);
        assert_eq!(seeded.last_step_usage, full.last_step_usage);
        assert_eq!(seeded.last_step_finish_reason, full.last_step_finish_reason);
        assert_eq!(seeded.run_finished_durable, full.run_finished_durable);
        assert_eq!(seeded.resolution, full.resolution);
    }

    #[test]
    fn corrupt_message_row_still_reports_json_error() {
        let store = sqlite_store();
        let session = crate::conformance::session("corrupt-message-json");
        let key = SqliteStore::key(&session);
        {
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO meta (session, watermark_seq, context_epoch) VALUES (?1, 1, 0)",
                [&key],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO message (session, ord, json) VALUES (?1, 0, 'not-json')",
                [&key],
            )
            .unwrap();
        }

        let error = MaterializedStore::read_prefix_with_watermark(&store, &session).unwrap_err();
        assert!(
            error.starts_with("json:"),
            "corrupt rows must retain the StoreError::Json context: {error}"
        );
    }

    #[test]
    fn init_migrates_legacy_run_index_metadata_columns() {
        let conn = rusqlite::Connection::open_in_memory().expect("open legacy store");
        let session = crate::conformance::session("legacy-run-index-schema");
        let session_json = serde_json::to_string(&session).expect("serialize session");
        conn.execute_batch(
            "CREATE TABLE run_index (
                 run_id        TEXT PRIMARY KEY,
                 session       TEXT NOT NULL,
                 submission_id TEXT,
                 state         TEXT NOT NULL
             );",
        )
        .expect("create legacy run-index schema");
        conn.execute(
            "INSERT INTO run_index (run_id, session, submission_id, state)
             VALUES (?1, ?2, NULL, 'completed')",
            rusqlite::params!["legacy-run", session_json],
        )
        .expect("insert legacy terminal row");

        let (source_instance_id, source_epoch, high_water) =
            SqliteStore::init(&conn).expect("migrate legacy store");
        let store = SqliteStore {
            bus_events: crate::bus_events::State::default(),
            conn: std::sync::Mutex::new(conn),
            export_high_water: AtomicU64::new(high_water),
            export_source_instance_id: source_instance_id,
            export_source_epoch: source_epoch,
        };
        let row = store
            .lookup_run("legacy-run")
            .expect("lookup legacy row")
            .expect("legacy row exists");
        assert_eq!(row.usage, None);
        assert_eq!(row.final_step_finish_reason, None);
        assert_eq!(row.request_identity_digest, None);
    }

    #[test]
    fn projection_keeps_the_sessions_newest_title() {
        crate::conformance::projection_keeps_the_sessions_newest_title(
            sqlite_store(),
            sqlite_store(),
        );
    }

    /// A store opened on a database written before titles existed gains both nullable title
    /// columns, and its existing rows read back with no title rather than an empty one.
    #[test]
    fn init_adds_the_title_columns_to_an_existing_store() {
        let conn = rusqlite::Connection::open_in_memory().expect("open legacy store");
        let session = crate::conformance::session("legacy-title-schema");
        let session_json = serde_json::to_string(&session).expect("serialize session");
        conn.execute_batch(
            "CREATE TABLE run_index (
                 run_id        TEXT PRIMARY KEY,
                 session       TEXT NOT NULL,
                 submission_id TEXT,
                 state         TEXT NOT NULL
             );",
        )
        .expect("create legacy run-index schema");
        conn.execute(
            "INSERT INTO run_index (run_id, session, submission_id, state)
             VALUES (?1, ?2, NULL, 'completed')",
            rusqlite::params!["legacy-run", session_json],
        )
        .expect("insert legacy row");
        let (source_instance_id, source_epoch, high_water) =
            SqliteStore::init(&conn).expect("migrate legacy store");
        for (table, column) in [("run_index", "title"), ("meta", "last_title")] {
            let present: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name=?1"),
                    [column],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(present, 1, "{table}.{column} is added");
        }
        let store = SqliteStore {
            bus_events: crate::bus_events::State::default(),
            conn: std::sync::Mutex::new(conn),
            export_high_water: AtomicU64::new(high_water),
            export_source_instance_id: source_instance_id,
            export_source_epoch: source_epoch,
        };
        let row = store.lookup_run("legacy-run").unwrap().unwrap();
        assert_eq!(row.title, None);
    }

    #[test]
    fn incremental_projection_resumes_from_watermark() {
        crate::conformance::incremental_projection_resumes_from_watermark(sqlite_store());
    }

    #[test]
    fn incremental_projection_matches_full_prefix_corpus() {
        crate::conformance::incremental_projection_matches_full_prefix_corpus(sqlite_store());
    }

    #[test]
    fn budgeted_incremental_projection_matches_full_prefix() {
        crate::conformance::budgeted_incremental_projection_matches_full_prefix(sqlite_store());
    }

    #[test]
    fn projection_retention_policy_survives_suffixes_and_full_prefix_repair() {
        crate::conformance::projection_retention_policy_survives_suffixes_and_full_prefix_repair(
            sqlite_store(),
        );
    }

    #[test]
    fn retained_deferred_projection_keeps_only_withdrawal_flags_and_no_disk_marker() {
        let root = broca_test_support::scratch_root()
            .join(format!("retained-deferred-store-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("run-index.db");
        let store = SqliteStore::open(&path).unwrap();
        let reader = SqliteStore::open(&path).unwrap();
        let conn = reader.conn.lock().unwrap();
        // Keep a genuine read transaction open so the writes remain in live WAL
        // sidecars and the marker scan covers them, not just a closed database.
        conn.execute_batch("BEGIN; SELECT count(*) FROM meta;")
            .unwrap();
        crate::conformance::retained_deferred_projection_keeps_only_withdrawal_flags(store);
        let marker = crate::conformance::deferred::RETAINED_DEFERRED_MARKER.as_bytes();
        let control = b"forever-deferred-content-control";
        let mut found_control = false;
        for path in [
            path.clone(),
            root.join("run-index.db-wal"),
            root.join("run-index.db-shm"),
        ] {
            match std::fs::read(&path) {
                Ok(bytes) => {
                    assert!(
                        !bytes.windows(marker.len()).any(|window| window == marker),
                        "retained approval text leaked into {}",
                        path.display()
                    );
                    found_control |= bytes.windows(control.len()).any(|window| window == control);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("cannot inspect {}: {error}", path.display()),
            }
        }
        assert!(
            found_control,
            "the same scan must find the forever content control"
        );
        conn.execute_batch("COMMIT;").unwrap();
        drop(conn);
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn paged_reads_use_persisted_prefix_length() {
        crate::conformance::paged_reads_return_atomic_meta_and_rows(sqlite_store());
    }

    #[test]
    fn backfill_page_usage_preserves_read_semantics() {
        crate::conformance::backfill_page_usage_preserves_read_semantics(sqlite_store());
    }

    /// `declined_no_wal` separates the PERMANENT decline cause from the rest.
    ///
    /// Non-vacuous by construction: both sessions below decline, so `declined` alone cannot
    /// tell them apart -- the split is the only thing that does. One has no WAL on disk at
    /// all, which no future run of this repair can fix; the other has a real WAL whose cursor
    /// is simply not derivable.
    ///
    /// Uses a FILE-backed WAL store deliberately. The in-memory conformance store answers
    /// `None` to `wal_present` -- correctly, since it has no filesystem to consult -- so it
    /// cannot exercise this at all, and a test written against it would pass while measuring
    /// nothing.
    #[cfg(unix)]
    #[test]
    fn declined_no_wal_separates_the_permanent_cause() {
        use crate::conformance::ControlHeadConformance;
        use broca_core::{Persister, WalStore};

        let store = sqlite_store();
        // A counter rather than a clock reading, matching the sibling test below: the wall
        // clock does not tick at the resolution it advertises, so two tests entering in the
        // same tick would share a directory.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "ch-split-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let wal_store = broca_core::FileWalStore::new(dir.join("wal"));

        let present = crate::conformance::session("split-has-wal");
        let absent = crate::conformance::session("split-no-wal");

        // Store rows for both, then strip the control head so each looks like a legacy row.
        for session in [&present, &absent] {
            // A COMPLETED turn, matching the sibling conformance setup: projection only
            // materializes metadata for a turn that finished, so base records alone leave
            // no meta row for the repair to find.
            let mut records = crate::conformance::base_records(session);
            records.extend(crate::conformance::text_turn(2, 1, "projected"));
            Persister::project(&store, session, &records).expect("project");
            store
                .clear_control_head_for_conformance(session)
                .expect("simulate a legacy row");
        }

        // Only one of them gets a log on disk. It is empty of a derivable cursor, so it
        // declines too -- which is exactly what makes the split load-bearing.
        drop(wal_store.open(&present).expect("create a real WAL"));

        let counts = store
            .preview_control_head_repair(&wal_store)
            .expect("preview repair");
        assert_eq!(counts.declined, 2, "both rows must decline");
        assert_eq!(
            counts.declined_no_wal, 1,
            "exactly the session with no log on disk is the permanent one"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn archived_control_head_repair_is_not_declined_no_wal() {
        use crate::conformance::ControlHeadConformance;
        use broca_core::{Persister, WalStore};
        let store = sqlite_store();
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-archive-repair-{}", std::process::id()));
        let wal_store = broca_core::FileWalStore::new(dir.join("wal"));
        let session = crate::conformance::session("archived-repair");
        let mut records = crate::conformance::base_records(&session);
        records.extend(crate::conformance::text_turn(2, 1, "archived"));
        Persister::project(&store, &session, &records).unwrap();
        store.clear_control_head_for_conformance(&session).unwrap();
        let mut wal = wal_store.open(&session).unwrap();
        for record in &records {
            wal.append(record.fence, &record.record, true).unwrap();
        }
        drop(wal);
        let path = wal_store.wal_path(&session);
        let archives = dir.join("wal-archive");
        std::fs::create_dir_all(&archives).unwrap();
        let mut writer = broca_wal::archive::ArchiveWriter::create(archives.join("fold-1.ark"))
            .unwrap()
            .without_feature_floor();
        writer
            .append(
                &broca_core::session_addr(&session),
                &std::fs::read(&path).unwrap(),
            )
            .unwrap();
        writer.seal().unwrap();
        std::fs::remove_file(path).unwrap();
        let counts = store.preview_control_head_repair(&wal_store).unwrap();
        assert_eq!(counts.repaired, 1);
        assert_eq!(counts.declined_no_wal, 0);
        assert_eq!(wal_store.wal_present(&session), Some(true));
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Three rows the index calls `active`, each a different truth in the WAL:
    ///   sealed    -- its WAL carries RunFinished for that run: the row must move to that state
    ///   abandoned -- its WAL has RunStarted and nothing after: the row must NOT move, because
    ///                index and WAL agree nothing ended it, and a terminal written here would put
    ///                the projection ahead of its authority
    ///   older     -- its lineage ran AGAIN after it, and only the LATER episode has a terminal:
    ///                the row must not move, because that terminal is not this run's
    /// The third arm is the one a "does the WAL end in RunFinished" shortcut gets wrong.
    #[test]
    fn reconcile_active_rows_moves_only_rows_whose_wal_carries_their_terminal() {
        use broca_core::RunIndexState;
        use broca_protocol::caller::RunFinishReason;
        use broca_protocol::{BindIdentity, Usage};
        use broca_wal::{FramedRecord, WalRecord};
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-reconcile-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = SqliteStore::open(dir.join("run-index.db")).unwrap();
        let wal_store = broca_core::FileWalStore::new(dir.join("wal"));
        let model = || broca_protocol::ModelHandle::new("stub", "m");
        let finished = |seq, reason| FramedRecord {
            seq,
            fence: 1,
            record: WalRecord::RunFinished {
                reason,
                ts_ms: Some(1),
                usage: Usage {
                    input_tokens: Some(7),
                    ..Usage::default()
                },
                error: None,
                retries_used: 3,
            },
        };
        let index_active = |run_id: &str, session: &BindIdentity| {
            store
                .index_run_active(&broca_core::RunIndexRow {
                    withdrawals_owed: false,
                    title: None,
                    run_id: run_id.into(),
                    session: session.clone(),
                    submission_id: None,
                    request_identity_digest: None,
                    state: RunIndexState::Active,
                    usage: None,
                    final_step_finish_reason: None,
                    retries_used: 0,
                })
                .unwrap();
        };
        let write = |session: &BindIdentity, records: &[FramedRecord]| {
            let mut wal = wal_store.open(session).unwrap();
            for record in records {
                wal.append(record.fence, &record.record, true).unwrap();
            }
        };

        let sealed = crate::conformance::session("sealed");
        write(
            &sealed,
            &[
                crate::conformance::run_started(1, "run-sealed", &sealed, "hi", model()),
                finished(2, RunFinishReason::Interrupted),
            ],
        );
        index_active("run-sealed", &sealed);

        let abandoned = crate::conformance::session("abandoned");
        write(
            &abandoned,
            &[crate::conformance::run_started(
                1,
                "run-abandoned",
                &abandoned,
                "hi",
                model(),
            )],
        );
        index_active("run-abandoned", &abandoned);

        let older = crate::conformance::session("older");
        write(
            &older,
            &[
                crate::conformance::run_started(1, "run-older", &older, "hi", model()),
                crate::conformance::run_started(2, "run-newer", &older, "again", model()),
                finished(3, RunFinishReason::Completed),
            ],
        );
        index_active("run-older", &older);

        let preview = store.preview_active_row_reconcile(&wal_store).unwrap();
        assert_eq!(preview.scanned, 3);
        // Declined FIRST: a wrong session parse reads every row as declined and reconciled 0,
        // which an assertion on reconciled alone reports as "the walk found no terminal".
        assert_eq!(
            preview.declined, 0,
            "every row's WAL must have been readable"
        );
        assert_eq!(
            preview.reconciled, 1,
            "only the sealed row has ITS terminal in the WAL"
        );
        assert_eq!(
            preview.unsealed, 2,
            "abandoned and older-episode rows are left alone"
        );
        // Preview committed nothing.
        assert_eq!(
            store.lookup_run("run-sealed").unwrap().unwrap().state,
            RunIndexState::Active
        );

        let counts = store.reconcile_active_rows(&wal_store).unwrap();
        assert_eq!(
            counts, preview,
            "the write reports what the preview promised"
        );
        let row = store.lookup_run("run-sealed").unwrap().unwrap();
        assert_eq!(row.state, RunIndexState::Interrupted);
        assert_eq!(
            row.retries_used, 3,
            "the terminal's metadata travels to the row"
        );
        assert_eq!(row.usage.map(|u| u.input_tokens), Some(Some(7)));
        assert_eq!(
            store.lookup_run("run-abandoned").unwrap().unwrap().state,
            RunIndexState::Active,
            "no terminal in the WAL means no terminal in the index"
        );
        assert_eq!(
            store.lookup_run("run-older").unwrap().unwrap().state,
            RunIndexState::Active,
            "a later episode's terminal must not be attributed to an earlier run"
        );
        // The store holds run-index.db open inside `dir`; Windows refuses to remove a tree
        // with an open file in it (sharing violation) where unix unlinks it silently.
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn repair_control_heads_preserves_page_semantics() {
        crate::conformance::repair_control_heads_preserves_page_semantics(sqlite_store());
    }

    #[test]
    fn paged_reads_distinguish_null_usage_from_unpopulated_metadata() {
        crate::conformance::paged_reads_distinguish_null_usage_from_unpopulated_metadata(
            sqlite_store(),
        );
    }

    #[test]
    fn paged_reads_preserve_trailing_origin() {
        crate::conformance::paged_reads_preserve_trailing_origin(sqlite_store());
    }

    #[test]
    fn wal_anchor_roundtrip_and_stale_filtering() {
        crate::conformance::wal_anchor_roundtrip_and_stale_filtering(sqlite_store());
    }

    #[cfg(unix)]
    #[test]
    fn persisted_sqlite_anchor_drives_the_next_wal_range() {
        use broca_core::Persister;
        use broca_wal::{read_records_range, FileWal, Wal, DEFAULT_MAX_READ_BYTES};

        let store = sqlite_store();
        let session = crate::conformance::session("anchor-range-roundtrip");
        let mut logical = crate::conformance::base_records(&session);
        logical.extend(crate::conformance::text_turn(2, 1, "first"));
        // A counter, not a clock reading: the wall clock advertises nanoseconds and does
        // not tick at that resolution, so two tests entering here in the same tick would
        // share a path and the second one's remove_file would delete the first's file.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = broca_test_support::scratch_root().join(format!(
            "broca-store-anchor-{}-{}.wal",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for framed in &logical {
            wal.append(1, &framed.record, false).unwrap();
        }
        let first = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None).unwrap();
        let watermark = store
            .project(
                &session,
                match first.records() {
                    broca_wal::WalRangeRecords::Complete(records)
                    | broca_wal::WalRangeRecords::Partial(records)
                    | broca_wal::WalRangeRecords::Unknown(records) => records,
                    broca_wal::WalRangeRecords::Absent => &[],
                },
            )
            .unwrap();
        let anchor = first.anchor_at(watermark).expect("Unix range mints anchor");
        Persister::persist_wal_anchor(&store, &session, Some(&anchor)).unwrap();
        let seed = Persister::projection_seed(&store, &session)
            .unwrap()
            .expect("projection seed");
        assert_eq!(seed.anchor, Some(anchor));

        for framed in crate::conformance::text_turn(4, 2, "second") {
            wal.append(1, &framed.record, false).unwrap();
        }
        let anchored = read_records_range(
            &path,
            DEFAULT_MAX_READ_BYTES,
            seed.watermark_seq,
            None,
            seed.anchor.as_ref(),
        )
        .unwrap();
        let scanned = read_records_range(
            &path,
            DEFAULT_MAX_READ_BYTES,
            seed.watermark_seq,
            None,
            None,
        )
        .unwrap();
        assert_eq!(anchored, scanned);
        assert_eq!(
            (match anchored.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            })
            .iter()
            .map(|record| record.seq)
            .collect::<Vec<_>>(),
            vec![4, 5]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn additive_projection_columns_are_added_to_existing_meta_tables() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (
                 session TEXT PRIMARY KEY,
                 watermark_seq INTEGER NOT NULL,
                 context_epoch INTEGER NOT NULL,
                 completed_steps INTEGER NOT NULL DEFAULT 0,
                 usage_json TEXT NOT NULL DEFAULT '{}',
                 last_run_id TEXT
             );",
        )
        .unwrap();
        SqliteStore::init(&conn).unwrap();
        let columns = {
            let mut stmt = conn.prepare("PRAGMA table_info(meta)").unwrap();
            stmt.query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .collect::<Result<std::collections::HashSet<_>, _>>()
                .unwrap()
        };
        for column in [
            "lineage_id",
            "control_head_wal_seq",
            "control_head_sub_index",
            "last_step_usage_json",
            "last_step_finish_reason_json",
            "wal_anchor_file_id",
            "wal_anchor_seq",
            "wal_anchor_byte_end",
            "prefix_len",
            "admission_checkpoint_json",
            "last_episode_usage_json",
        ] {
            assert!(columns.contains(column));
        }
    }

    #[test]
    fn additive_lineage_column_preserves_legacy_meta_rows() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (
                 session TEXT PRIMARY KEY,
                 watermark_seq INTEGER NOT NULL,
                 context_epoch INTEGER NOT NULL,
                 completed_steps INTEGER NOT NULL DEFAULT 0,
                 usage_json TEXT NOT NULL DEFAULT '{}',
                 last_run_id TEXT
             );
             INSERT INTO meta (
                 session, watermark_seq, context_epoch, completed_steps, usage_json, last_run_id
             ) VALUES ('legacy', 7, 3, 2, '{}', 'run-legacy');",
        )
        .unwrap();

        SqliteStore::init(&conn).unwrap();
        let row = conn
            .query_row(
                "SELECT watermark_seq, context_epoch, completed_steps, last_run_id, lineage_id
                   FROM meta WHERE session='legacy'",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(row, (7, 3, 2, "run-legacy".to_string(), None));
    }

    #[test]
    fn null_prefix_length_is_recomputed_and_persisted() {
        use broca_core::MaterializedStore;
        use broca_protocol::content::{ContentBlock, Message, Role};

        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let (source_instance_id, source_epoch, high_water) = SqliteStore::init(&conn).unwrap();
        let session = crate::conformance::session("legacy-prefix-len");
        let key = SqliteStore::key(&session);
        conn.execute(
            "INSERT INTO meta (
                 session, watermark_seq, context_epoch, completed_steps, usage_json, last_run_id
             ) VALUES (?1, 0, 0, 0, '{}', 'r')",
            [&key],
        )
        .unwrap();
        let message = Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "legacy".into(),
            }],
        );
        conn.execute(
            "INSERT INTO message (session, ord, json) VALUES (?1, 0, ?2)",
            rusqlite::params![key, serde_json::to_string(&message).unwrap()],
        )
        .unwrap();
        let store = SqliteStore {
            bus_events: crate::bus_events::State::default(),
            conn: std::sync::Mutex::new(conn),
            export_high_water: AtomicU64::new(high_water),
            export_source_instance_id: source_instance_id,
            export_source_epoch: source_epoch,
        };

        assert_eq!(store.projection_snapshot(&key).unwrap().next_ord, 1);
        assert!(store
            .read_page_with_watermark(&session, Some(0), 1, broca_core::context::ReadView::Raw)
            .unwrap_err()
            .contains("run attribution requires WAL backfill"));
        let persisted = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT prefix_len FROM meta WHERE session=?1",
                [&key],
                |row| row.get::<_, Option<i64>>(0),
            )
            .unwrap();
        assert_eq!(persisted, Some(1));
    }

    #[test]
    fn re_projecting_same_wal_is_idempotent() {
        crate::conformance::re_projecting_same_wal_is_idempotent(sqlite_store());
    }

    #[test]
    fn stale_prepared_projection_is_rejected_before_commit() {
        use broca_core::MaterializedStore;

        let store = sqlite_store();
        let session = crate::conformance::session("projection-watermark-conflict");
        let key = SqliteStore::key(&session);
        let mut records = crate::conformance::base_records(&session);
        records.extend(crate::conformance::text_turn(2, 1, "first"));
        assert_eq!(store.project(&session, &records).unwrap(), 3);

        records.extend(crate::conformance::text_turn(4, 2, "second"));
        let stale_snapshot = store.projection_snapshot(&key).unwrap();
        let stale_plan = SqliteStore::prepare_projection(stale_snapshot, &records, None).unwrap();

        // Advance the same session beyond the stale plan after preparation but before its
        // commit attempt. Applying the stale plan would regress the watermark and prefix length.
        records.extend(crate::conformance::text_turn(6, 3, "third"));
        assert_eq!(store.project(&session, &records).unwrap(), 7);
        assert!(matches!(
            store.try_commit_projection(&key, stale_plan).unwrap(),
            ProjectionCommit::Conflict
        ));
        let prefix = MaterializedStore::read_prefix_with_watermark(&store, &session).unwrap();
        let expected_records = records
            .iter()
            .map(|framed| framed.record.clone())
            .collect::<Vec<_>>();
        let expected = replay(&expected_records).unwrap();
        let mut actual = prefix.prefix;
        broca_protocol::content::stamp_ordinals(&mut actual);
        assert_eq!(actual, expected.prompt, "no message is lost or duplicated");
        assert_eq!(prefix.watermark_seq, 7);
        assert_eq!(prefix.completed_steps, expected.completed_steps);
        let checkpoint = MaterializedStore::read_admission_snapshot(&store, &session)
            .unwrap()
            .unwrap()
            .checkpoint
            .unwrap();
        assert_eq!(checkpoint.at_seq, 7);
    }

    #[test]
    fn uncommitted_tail_is_not_materialized_but_merge_recovers_it() {
        crate::conformance::uncommitted_tail_is_not_materialized_but_merge_recovers_it(
            sqlite_store(),
        );
    }

    #[test]
    fn projection_boundary_keeps_terminal_record_in_tail() {
        crate::conformance::projection_boundary_keeps_terminal_record_in_tail(sqlite_store());
    }
    #[test]
    fn run_attribution_follows_each_episode() {
        crate::conformance::run_attribution_follows_each_episode(sqlite_store());
    }
    #[test]
    fn run_attribution_waits_for_terminal() {
        crate::conformance::run_attribution_waits_for_terminal(sqlite_store());
    }
    #[test]
    fn interrupted_tool_result_is_final() {
        crate::conformance::interrupted_tool_result_is_final(sqlite_store());
    }
    #[test]
    fn imported_and_pre_run_id_messages_omit_run() {
        crate::conformance::imported_and_pre_run_id_messages_omit_run(sqlite_store());
    }
    #[test]
    fn rollback_rows_require_attribution_backfill() {
        crate::conformance::rollback_rows_require_attribution_backfill(sqlite_store());
    }

    #[test]
    fn unbudgeted_warm_admission_omits_base_and_resumes_from_tail() {
        use broca_protocol::{caller::RunFinishReason, Usage};

        let root = broca_test_support::scratch_root().join(format!(
            "broca-admission-counting-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let wal_store = CountingWalStore::new(root.clone());
        let session = crate::conformance::session("counting-admission");
        let mut records = crate::conformance::base_records(&session);
        records.extend(crate::conformance::text_turn(2, 1, "projected"));
        records.push(broca_wal::FramedRecord {
            seq: 4,
            fence: 1,
            record: broca_wal::WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        });
        let mut wal = wal_store.open(&session).expect("open WAL");
        for frame in &records {
            wal.append(1, &frame.record, true).expect("append fixture");
        }
        let durable = wal.replay().expect("replay fixture");
        let safe_head = durable.last().expect("non-empty fixture").seq;
        drop(wal);

        let store = SqliteStore::open_in_memory().expect("open store");
        let projection_range = wal_store
            .inner
            .read_records_range(&session, 0, Some(safe_head), None)
            .expect("projection range");
        assert_eq!(
            Persister::project_range(&store, &session, &projection_range).expect("project fixture"),
            safe_head - 1
        );
        // Byte anchors carry a file identity that only the unix backend provides; without
        // one the range read still returns the correct tail, just without the byte skip.
        #[cfg(unix)]
        assert!(MaterializedStore::read_admission_snapshot(&store, &session)
            .expect("read metadata")
            .expect("metadata row")
            .anchor
            .is_some());

        let snapshot =
            broca_core::load_prior_wal_snapshot(&wal_store, &session, safe_head, Some(&store))
                .expect("warm admission snapshot");
        assert_eq!(wal_store.full_reads.load(Ordering::SeqCst), 0);
        assert_eq!(wal_store.range_reads.load(Ordering::SeqCst), 1);
        assert_eq!(wal_store.range_records.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.tail.len(), 1);
        assert_eq!(snapshot.head_seq, safe_head);
        assert!(
            snapshot.checkpoint_at_base.is_none(),
            "an unbudgeted resume must not clone a base checkpoint"
        );
        assert_seeded_resume_matches_full_replay(&store, &session, &snapshot, &records);
        assert_eq!(wal_store.full_reads.load(Ordering::SeqCst), 0);
        assert_eq!(
            snapshot
                .checkpoint_at_head
                .last_episode
                .as_ref()
                .expect("last episode")
                .disposition,
            broca_core::EpisodeDisposition::Finished
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn budgeted_warm_admission_keeps_valid_base_and_invalid_seed_falls_back() {
        use broca_protocol::request::{BudgetConfig, BudgetNudge};

        let root = broca_test_support::scratch_root().join(format!(
            "broca-budget-admission-counting-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let wal_store = CountingWalStore::new(root.clone());
        let session = crate::conformance::session("counting-budget-admission");
        let mut records = crate::conformance::base_records(&session);
        let broca_wal::WalRecord::RunStarted { config, .. } = &mut records[0].record else {
            unreachable!("base records begin with RunStarted");
        };
        config.budget = Some(BudgetConfig {
            max_input_tokens: None,
            nudges: vec![BudgetNudge {
                at_tool_calls: 99,
                text: "not due".into(),
            }],
            finalize_at_tool_calls: 100,
            finalize_text: "not due".into(),
        });
        records.extend(crate::conformance::text_turn(1, 1, "projected"));
        records.push(crate::conformance::text_step(3, 2, "tail"));
        let mut wal = wal_store.open(&session).expect("open WAL");
        for frame in &records {
            wal.append(1, &frame.record, true).expect("append fixture");
        }
        let safe_head = wal.replay().expect("replay fixture").last().unwrap().seq;
        drop(wal);

        let store = SqliteStore::open_in_memory().expect("open store");
        let projection_range = wal_store
            .inner
            .read_records_range(&session, 0, Some(3), None)
            .expect("projection range");
        assert_eq!(
            Persister::project_range(&store, &session, &projection_range)
                .expect("project budget fixture"),
            3
        );

        let mut snapshot =
            broca_core::load_prior_wal_snapshot(&wal_store, &session, safe_head, Some(&store))
                .expect("warm budget admission snapshot");
        assert_eq!(wal_store.full_reads.load(Ordering::SeqCst), 0);
        assert_eq!(wal_store.range_reads.load(Ordering::SeqCst), 1);
        assert_eq!(wal_store.range_records.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.base_watermark, 3);
        assert_eq!(snapshot.tail.len(), 1);
        assert!(snapshot.checkpoint_at_base.is_some());
        assert!(
            snapshot.take_pending_fifo().is_empty(),
            "the budget base must not retain the head queue Arc"
        );
        assert_seeded_resume_matches_full_replay(&store, &session, &snapshot, &records);
        assert_eq!(wal_store.full_reads.load(Ordering::SeqCst), 0);

        // Simulate a legacy checkpoint blob: omission is unknown state, never a benign zero
        // state. The loader must fall back to the complete WAL rather than risk re-firing a nudge.
        let mut legacy = MaterializedStore::read_admission_snapshot(&store, &session)
            .expect("read projected admission checkpoint")
            .expect("projected checkpoint exists")
            .checkpoint
            .expect("checkpoint payload exists");
        legacy
            .last_episode
            .as_mut()
            .expect("budgeted episode exists")
            .budget_replay_state = None;
        let key = SqliteStore::key(&session);
        let encoded = broca_core::encode_admission_checkpoint(&legacy).expect("encode legacy blob");
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE meta SET admission_checkpoint_json=?2 WHERE session=?1",
                rusqlite::params![key, encoded],
            )
            .expect("replace checkpoint with legacy shape");
        wal_store.full_reads.store(0, Ordering::SeqCst);
        wal_store.range_reads.store(0, Ordering::SeqCst);
        wal_store.range_records.store(0, Ordering::SeqCst);
        let legacy_snapshot =
            broca_core::load_prior_wal_snapshot(&wal_store, &session, safe_head, Some(&store))
                .expect("legacy checkpoint falls back to full replay");
        assert_eq!(legacy_snapshot.base_watermark, 0);
        assert!(legacy_snapshot.checkpoint_at_base.is_none());
        assert_eq!(wal_store.full_reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            legacy_snapshot.checkpoint_at_head,
            broca_core::AdmissionCheckpointV1::from_frames(
                &wal_store
                    .inner
                    .read_records(&session)
                    .expect("read full WAL oracle"),
            )
            .expect("fold full WAL oracle")
        );
        let _ = std::fs::remove_dir_all(root);
    }

    /// A session's frozen scope and recorded principals reach admission through the warm
    /// path: the scoped `RunStarted` lies in the materialized prefix, so only the stored
    /// checkpoint can carry it past the tail read. A checkpoint stored before the ownership
    /// summary existed is refused and admission folds the complete WAL instead.
    #[test]
    fn warm_admission_carries_the_frozen_scope_and_a_legacy_checkpoint_falls_back() {
        check_warm_scope(false);
    }

    #[test]
    fn warm_projection_preserves_agent_run_scope_identity() {
        check_warm_scope(true);
    }

    fn check_warm_scope(agent_run: bool) {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-scoped-admission-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let wal_store = CountingWalStore::new(root.clone());
        let session = crate::conformance::session(if agent_run {
            "agent-run-admission"
        } else {
            "scoped-admission"
        });
        let owner = broca_protocol::BindPrincipal::Reserved {
            module_id: "prefrontal-core".into(),
        };
        let scope = broca_protocol::SessionScope {
            owner: owner.clone(),
            scope_ref: "head-1".into(),
            scope_epoch: 3,
            flow_id: None,
            sender: agent_run.then(|| broca_protocol::BindPrincipal::Reserved {
                module_id: "basal".into(),
            }),
            agent_id: agent_run.then(|| "agent-1".into()),
            run_id: agent_run.then(|| "parent-run".into()),
        };
        let sender = scope.sender.clone().unwrap_or_else(|| owner.clone());
        let mut records = crate::conformance::base_records(&session);
        let broca_wal::WalRecord::RunStarted {
            scope: frozen,
            principal,
            ..
        } = &mut records[0].record
        else {
            unreachable!("base records begin with RunStarted");
        };
        *frozen = Some(scope.clone());
        *principal = Some(sender.clone());
        records.extend(crate::conformance::text_turn(1, 1, "projected"));
        records.push(crate::conformance::text_step(3, 2, "tail"));
        let mut wal = wal_store.open(&session).expect("open WAL");
        for frame in &records {
            wal.append(1, &frame.record, true).expect("append fixture");
        }
        let safe_head = wal.replay().expect("replay fixture").last().unwrap().seq;
        drop(wal);

        let store = SqliteStore::open_in_memory().expect("open store");
        let projection_range = wal_store
            .inner
            .read_records_range(&session, 0, Some(3), None)
            .expect("projection range");
        Persister::project_range(&store, &session, &projection_range)
            .expect("project the scoped prefix");

        let expected = broca_core::SessionOwnershipV1 {
            episodes: 1,
            scope: Some(scope),
            first_principal: Some(sender.clone()),
            any_unrecorded_principal: false,
            recorded_principals: vec![sender],
            first_episode: Some(broca_core::FirstEpisodeV1 {
                run_id: broca_protocol::RunId("r".into()),
                tool_names: Vec::new(),
                manifest: None,
            }),
            plan_manifest_folded: true,
        };
        let snapshot =
            broca_core::load_prior_wal_snapshot(&wal_store, &session, safe_head, Some(&store))
                .expect("warm admission snapshot");
        assert_eq!(wal_store.full_reads.load(Ordering::SeqCst), 0);
        assert_eq!(snapshot.base_watermark, 3, "the warm path was taken");
        assert_eq!(
            snapshot.checkpoint_at_head.ownership,
            Some(expected.clone())
        );

        let mut legacy = MaterializedStore::read_admission_snapshot(&store, &session)
            .expect("read projected admission checkpoint")
            .expect("projected checkpoint exists")
            .checkpoint
            .expect("checkpoint payload exists");
        legacy.ownership = None;
        let key = SqliteStore::key(&session);
        let encoded = broca_core::encode_admission_checkpoint(&legacy).expect("encode legacy blob");
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE meta SET admission_checkpoint_json=?2 WHERE session=?1",
                rusqlite::params![key, encoded],
            )
            .expect("replace checkpoint with the pre-ownership shape");
        let legacy_snapshot =
            broca_core::load_prior_wal_snapshot(&wal_store, &session, safe_head, Some(&store))
                .expect("a pre-ownership checkpoint falls back to the complete WAL");
        assert_eq!(legacy_snapshot.base_watermark, 0);
        assert_eq!(wal_store.full_reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            legacy_snapshot.checkpoint_at_head.ownership,
            Some(expected.clone())
        );

        // A summary from a build that does not record the first episode's plan manifest (stored
        // before this build, or re-stored by an older build after a rollback) may lack the
        // manifest, so it is not built on: admission rebuilds the summary from the complete WAL.
        let mut unaware = legacy.clone();
        let mut summary = expected.clone();
        summary.plan_manifest_folded = false;
        unaware.ownership = Some(summary);
        let encoded =
            broca_core::encode_admission_checkpoint(&unaware).expect("encode unaware blob");
        assert!(!encoded.contains("plan_manifest_folded"));
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE meta SET admission_checkpoint_json=?2 WHERE session=?1",
                rusqlite::params![key, encoded],
            )
            .expect("replace checkpoint with one that predates the manifest");
        let unaware_snapshot =
            broca_core::load_prior_wal_snapshot(&wal_store, &session, safe_head, Some(&store))
                .expect("a manifest-unaware checkpoint falls back to the complete WAL");
        assert_eq!(unaware_snapshot.base_watermark, 0);
        assert_eq!(wal_store.full_reads.load(Ordering::SeqCst), 2);
        assert_eq!(
            unaware_snapshot.checkpoint_at_head.ownership,
            Some(expected.clone())
        );

        // The next projection does not build on the pre-ownership checkpoint: it folds the
        // complete WAL and stores a checkpoint that carries the summary again. Finishing the
        // tail's turn gives it a boundary to project to.
        let mut wal = wal_store.open(&session).expect("reopen WAL");
        let turn_end = wal
            .append(
                1,
                &broca_wal::WalRecord::TurnFinished {
                    step_id: broca_protocol::StepId(2),
                },
                true,
            )
            .expect("finish the tail turn");
        drop(wal);
        let projection_range = wal_store
            .inner
            .read_records_range(&session, 0, Some(turn_end), None)
            .expect("projection range");
        Persister::project_range(&store, &session, &projection_range)
            .expect("project through the tail");
        let repaired = MaterializedStore::read_admission_snapshot(&store, &session)
            .expect("read repaired checkpoint")
            .expect("checkpoint row")
            .checkpoint
            .expect("checkpoint payload");
        assert_eq!(repaired.ownership, Some(expected));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn admission_checkpoint_conformance() {
        crate::conformance::admission_checkpoint_empty_roundtrip_and_incremental_equivalence(
            sqlite_store(),
        );
        crate::conformance::projection_range_co_commits_anchor(sqlite_store());
        crate::conformance::failed_projection_keeps_checkpoint_and_messages_atomic(sqlite_store());
    }

    #[test]
    fn injected_metadata_failure_rolls_back_messages_checkpoint_and_watermark() {
        let store = sqlite_store();
        let session = crate::conformance::session("atomic-admission-metadata");
        let mut records = crate::conformance::base_records(&session);
        records.extend(crate::conformance::text_turn(2, 1, "first"));
        store
            .project(&session, &records)
            .expect("initial projection");
        let before_prefix = MaterializedStore::read_prefix_with_watermark(&store, &session)
            .expect("read initial prefix");
        let before_admission = MaterializedStore::read_admission_snapshot(&store, &session)
            .expect("read initial metadata");

        store
            .conn
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER fail_admission_meta_update
                 BEFORE UPDATE OF watermark_seq ON meta
                 BEGIN SELECT RAISE(ABORT, 'injected metadata failure'); END;",
            )
            .expect("install failure trigger");
        records.extend(crate::conformance::text_turn(4, 2, "second"));
        assert!(store.project(&session, &records).is_err());

        let after_prefix = MaterializedStore::read_prefix_with_watermark(&store, &session)
            .expect("read rolled-back prefix");
        let after_admission = MaterializedStore::read_admission_snapshot(&store, &session)
            .expect("read rolled-back metadata");
        assert_eq!(after_prefix, before_prefix);
        assert_eq!(after_admission, before_admission);
    }

    #[test]
    fn null_unknown_and_malformed_checkpoints_are_unavailable_then_repaired() {
        use broca_core::{MaterializedStore, Persister};

        for (label, encoded) in [
            ("null", None),
            ("unknown", Some(r#"{"version":99,"checkpoint":{}}"#)),
            ("malformed", Some("not-json")),
        ] {
            let store = sqlite_store();
            let session = crate::conformance::session(label);
            let mut records = crate::conformance::base_records(&session);
            records.extend(crate::conformance::text_turn(2, 1, "first"));
            store
                .project(&session, &records)
                .expect("initial projection");
            let key = SqliteStore::key(&session);
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE meta SET admission_checkpoint_json=?2 WHERE session=?1",
                    rusqlite::params![key, encoded],
                )
                .expect("damage checkpoint");

            assert!(Persister::projection_seed(&store, &session)
                .expect("seed read")
                .is_none());
            assert!(MaterializedStore::read_admission_snapshot(&store, &session)
                .expect("admission read")
                .expect("meta row")
                .checkpoint
                .is_none());

            records.extend(crate::conformance::text_turn(4, 2, "repair"));
            store
                .project(&session, &records)
                .expect("full-prefix repair");
            let repaired = MaterializedStore::read_admission_snapshot(&store, &session)
                .expect("repaired admission read")
                .expect("meta row");
            assert_eq!(repaired.watermark_seq, 5);
            assert_eq!(repaired.checkpoint.expect("checkpoint").at_seq, 5);
        }
    }

    #[test]
    fn incomplete_projection_metadata_falls_back_to_full_prefix_replay() {
        let store = sqlite_store();
        let session = crate::conformance::session("projection-fallback");
        let mut records = crate::conformance::base_records(&session);
        records.extend(crate::conformance::text_turn(2, 1, "first"));
        store
            .project(&session, &records)
            .expect("initial projection");

        let key = SqliteStore::key(&session);
        store
            .conn
            .lock()
            .unwrap()
            .execute("UPDATE meta SET last_run_id=NULL WHERE session=?1", [&key])
            .expect("simulate metadata from before last-run tracking");
        assert_eq!(
            broca_core::Persister::projection_seed(&store, &session)
                .expect("seed read remains available"),
            None
        );

        records.extend(crate::conformance::text_turn(4, 2, "second"));
        assert_eq!(
            store
                .project(&session, &records)
                .expect("fallback projection"),
            5
        );
        let prefix = broca_core::MaterializedStore::read_prefix_with_watermark(&store, &session)
            .expect("snapshot reads");
        assert_eq!(prefix.prefix.len(), 3);
        assert_eq!(prefix.completed_steps, 2);
        assert!(
            broca_core::Persister::projection_seed(&store, &session)
                .expect("repaired seed reads")
                .is_some(),
            "the full-prefix fallback repairs projection metadata"
        );
    }

    #[test]
    fn watermark_read_propagates_database_errors() {
        let store = sqlite_store();
        store
            .conn
            .lock()
            .unwrap()
            .execute_batch("DROP TABLE meta")
            .expect("drop metadata table for fault injection");
        let session = broca_protocol::BindIdentity::new("/tmp/project", "runner", "session");

        let error = store
            .project(&session, &[])
            .expect_err("a real watermark query failure must not look like a new session");

        assert!(error.to_string().contains("no such table"));
    }

    #[test]
    fn run_index_conformance() {
        crate::conformance::assert_run_index_conformance(sqlite_store);
    }

    #[test]
    fn export_index_conformance() {
        crate::conformance::assert_export_conformance(sqlite_store);
    }
}

/// Exposes the SQLite caller's prepared fields without opening a database, so
/// cross-backend tests exercise adapter wiring as well as the shared planner.
#[cfg(any(test, feature = "test-conformance"))]
#[doc(hidden)]
pub fn projection_caller_output_for_conformance(
    snapshot: ProjectionSnapshot,
    records: &[FramedRecord],
    range: Option<&WalRangeRead>,
) -> Result<serde_json::Value, StoreError> {
    let ProjectionPlan::Write(prepared) =
        SqliteStore::prepare_projection(snapshot, records, range)?
    else {
        panic!("caller conformance fixture must produce a write");
    };
    Ok(serde_json::json!({
        "messages": prepared.message_json,
        "checkpoint": prepared.checkpoint_json,
        "watermark": prepared.new_watermark,
        "control_head": prepared.control_head.map(|cursor| serde_json::json!({"wal_seq": cursor.wal_seq, "sub_index": cursor.sub_index})),
    }))
}

#[cfg(test)]
mod projection_planner_tests {
    use super::*;

    #[test]
    fn sqlite_projection_overflow_returns_typed_error_without_panicking() {
        // This count cannot be reached with a practical WAL; construct the loaded
        // snapshot directly to exercise the conversion on the SQLite prepare path.
        let mut seed = ProjectionSeed::empty();
        seed.completed_steps = i64::MAX as u64 + 1;
        // The suffix fixture has no RunStarted: prove the existing lineage is
        // forever so the test reaches the signed-counter conversion, not the
        // independent unknown-policy refusal.
        seed.admission_checkpoint.lineage_retention = Some(broca_core::LineageRetention::Forever);
        let snapshot = ProjectionSnapshot {
            attribution_complete: true,
            seed: Some(seed),
            watermark: 0,
            lineage_id: None,
            next_ord: 0,
            previous_control_head: None,
        };
        let records = [FramedRecord {
            seq: 1,
            fence: 1,
            record: broca_wal::WalRecord::TurnFinished {
                step_id: broca_protocol::StepId(1),
            },
        }];
        let result =
            std::panic::catch_unwind(|| SqliteStore::prepare_projection(snapshot, &records, None));
        assert!(matches!(result, Ok(Err(StoreError::Int(_)))));
    }
}

#[cfg(test)]
#[path = "withdrawal_writer_tests.rs"]
mod withdrawal_writer_tests;

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{
        assert_inherited_private, assert_private, assert_protected_private, broaden, deny_acl_read,
        reset_acl, BroadParent,
    };
    use std::io::Write;
    fn sidecar(path: &Path, suffix: &str) -> std::path::PathBuf {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        name.into()
    }
    #[test]
    fn windows_sqlite_creation_and_recreated_sidecars_are_private() {
        let fixture = BroadParent::new("sqlite");
        let root = fixture.path.join("state");
        broca_owner_only::create_dir(&root).unwrap();
        let path = root.join("unusual-name.sqlite");
        for _ in 0..2 {
            let store = SqliteStore::open(&path).unwrap();
            store
                .conn
                .lock()
                .unwrap()
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS acl_control(x); INSERT INTO acl_control VALUES(1);",
                )
                .unwrap();
            assert_private(&root);
            assert_protected_private(&path);
            for suffix in ["-wal", "-shm"] {
                assert_inherited_private(&sidecar(&path, suffix));
            }
            store
                .conn
                .lock()
                .unwrap()
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
            drop(store);
            // The store retains sidecars for read-only reopen. Remove checkpointed files
            // explicitly while closed to exercise SQLite's independent recreation path.
            for suffix in ["-wal", "-shm"] {
                let candidate = sidecar(&path, suffix);
                if candidate.exists() {
                    std::fs::remove_file(candidate).unwrap();
                }
            }
        }
        let caps_path = root.join("caps.db");
        let _caps = CapsStore::open(&caps_path).unwrap();
        assert_protected_private(&caps_path);
        for suffix in ["-wal", "-shm"] {
            assert_inherited_private(&sidecar(&caps_path, suffix));
        }
    }
    #[test]
    fn windows_sqlite_refuses_acl_before_initialisation() {
        let fixture = BroadParent::new("sqlite-refusal");
        let root = fixture.path.join("state");
        broca_owner_only::create_dir(&root).unwrap();
        let path = root.join("run-index.db");
        drop(SqliteStore::open(&path).unwrap());
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let candidate = sidecar(&path, suffix);
            if !candidate.exists() {
                broca_owner_only::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&candidate)
                    .unwrap()
                    .write_all(b"sidecar sentinel")
                    .unwrap();
            }
            let original = std::fs::read(&candidate).unwrap();
            let files_before: Vec<_> = ["", "-wal", "-shm", "-journal"]
                .into_iter()
                .map(|suffix| sidecar(&path, suffix))
                .filter(|p| p.exists())
                .map(|p| {
                    let bytes = std::fs::read(&p).unwrap();
                    (p, bytes)
                })
                .collect();
            for unreadable in [false, true] {
                if unreadable {
                    deny_acl_read(&candidate);
                } else {
                    broaden(&candidate);
                }
                SQLITE_OPENS.with(|count| count.set(0));
                assert!(
                    matches!(
                        SqliteStore::open(&path),
                        Err(StoreError::PrivateAccess { .. })
                    ),
                    "insecure or unreadable ACL must refuse before SQLite"
                );
                assert!(
                    CapsStore::open(&path).is_err(),
                    "caps store accepted an insecure ACL"
                );
                SQLITE_OPENS.with(|count| {
                    assert_eq!(
                        count.get(),
                        0,
                        "SQLite initialisation reached after ACL refusal"
                    )
                });
                reset_acl(&candidate);
                assert_eq!(std::fs::read(&candidate).unwrap(), original);
                for (file, bytes) in &files_before {
                    assert_eq!(
                        &std::fs::read(file).unwrap(),
                        bytes,
                        "SQLite refusal changed {}",
                        file.display()
                    );
                }
                assert_eq!(
                    std::fs::read_dir(&root).unwrap().count(),
                    files_before.len(),
                    "SQLite refusal created another file"
                );
                broca_owner_only::repair(&candidate).unwrap();
            }
            if !suffix.is_empty() {
                std::fs::remove_file(candidate).unwrap();
            }
        }
        let new_database = fixture.path.join("broad-parent.db");
        assert!(SqliteStore::open(&new_database).is_err());
        assert!(
            !new_database.exists(),
            "broad existing parent must not leak new SQLite sidecars"
        );
    }
}
