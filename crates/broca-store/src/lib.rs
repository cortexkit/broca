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
use std::sync::{Arc, Mutex};

use broca_core::{session_addr, RunIndex, RunIndexRow, RunIndexState};
use broca_protocol::BindIdentity;
use broca_protocol::{FinishReason, Usage};
use rusqlite::{config::DbConfig, Connection, OptionalExtension};

mod served_history;

const STORE_SCHEMA_VERSION: i64 = 1;

mod busy;
use busy::STORE_BUSY_TIMEOUT;

pub mod bus_events;
pub mod diagnostics;
mod display_facts;
pub mod export_index;
mod keeper_connection;
pub mod legacy_inventory;
pub(crate) use export_index::ExportStore;
pub(crate) use keeper_connection::KeeperOpenError;
mod fold_identities;
pub mod grants;
pub mod late_results_store;
mod retained_head;
pub mod retention;
pub mod retention_withdrawals;
pub mod session_address;
pub mod session_inventory;
pub use session_inventory::SessionInventory;
pub mod session_log_evidence;
pub mod session_ref_audit;
pub mod session_refs;
pub mod system_one;
mod wal_recovery;
pub use fold_identities::{require_legacy_operator_root, LegacyOnlyOperator};

/// Reserve a logical session's storage address and verify its history through
/// the format-selected inventory.
pub trait SessionAddressResolver: Send + Sync {
    /// Resolve `bind` to the root its pair was first reserved under, reserving it under
    /// `workdir` (or the unscoped namespace) when the pair is new. A scoped bind passes its
    /// stamp as `reservation`; it is recorded only when this call reserves a new pair.
    fn resolve_session_address(
        &self,
        bind: &BindIdentity,
        workdir: Option<&Path>,
        reservation: Option<&session_address::ScopeReservation>,
    ) -> Result<BindIdentity, session_address::SessionAddressError>;

    /// The scope reservation a stamped bind recorded when it reserved `session`'s address.
    /// `None` for an address reserved without a stamp or before reservations were recorded.
    fn scope_reservation(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<session_address::ScopeReservation>, session_address::SessionAddressError>;

    /// Check history without opening a writer. Evidence failures must remain
    /// errors; an unreadable inventory cannot certify a new or available log.
    fn session_log_state(
        &self,
        session: &BindIdentity,
        wal: &dyn broca_core::WalStore,
    ) -> Result<session_log_evidence::SessionLogState, session_log_evidence::SessionLogError>;
}

impl SessionAddressResolver for SqliteStore {
    fn session_log_state(
        &self,
        session: &BindIdentity,
        wal: &dyn broca_core::WalStore,
    ) -> Result<session_log_evidence::SessionLogState, session_log_evidence::SessionLogError> {
        SqliteStore::checked_session_log_state(self, session, wal)
    }

    fn resolve_session_address(
        &self,
        bind: &BindIdentity,
        workdir: Option<&Path>,
        reservation: Option<&session_address::ScopeReservation>,
    ) -> Result<BindIdentity, session_address::SessionAddressError> {
        SqliteStore::resolve_session_address(self, bind, workdir, reservation)
    }

    fn scope_reservation(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<session_address::ScopeReservation>, session_address::SessionAddressError>
    {
        SqliteStore::scope_reservation(self, session)
    }
}

pub use system_one::SystemOneStore;

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
    AttributionStale,
    InconsistentSnapshot(&'static str),
    BusEvent(String),
    LegacyOnly(LegacyOnlyOperator),
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
            StoreError::AttributionStale => {
                std::fmt::Display::fmt(&broca_core::StorePageReadError::AttributionStale, f)
            }
            StoreError::BusEvent(e) => write!(f, "run event: {e}"),
            StoreError::LegacyOnly(e) => write!(f, "{e}"),
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
    conn: Arc<Mutex<Connection>>,
    wal_config: broca_wal::WalConfig,
    bus_events: bus_events::State,
    export: ExportStore,
}

struct SchemaUpgradeLog {
    from_version: i64,
    to_version: i64,
    started_at: std::time::Instant,
}

impl SchemaUpgradeLog {
    fn new(from_version: i64, to_version: i64) -> Self {
        tracing::info!(
            target: "store.schema",
            from_version,
            to_version,
            "store schema upgrade started"
        );
        Self {
            from_version,
            to_version,
            started_at: std::time::Instant::now(),
        }
    }
}

impl Drop for SchemaUpgradeLog {
    fn drop(&mut self) {
        tracing::info!(
            target: "store.schema",
            from_version = self.from_version,
            to_version = self.to_version,
            elapsed_ms = self.started_at.elapsed().as_millis(),
            "store schema upgrade ended"
        );
    }
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

#[cfg(test)]
thread_local! {
    static SQLITE_OPENS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static PROJECTION_AFTER_STATE_READ_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
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
fn validate_private_sqlite(path: &Path) -> std::io::Result<()> {
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
    Ok(())
}

#[cfg(windows)]
fn prepare_private_sqlite(path: &Path) -> std::io::Result<()> {
    validate_private_sqlite(path)?;
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

/// Create the SQLite database file at `path` owner-only before SQLite first opens it, for a
/// store outside this crate that keeps its own schema. SQLite creates files through the
/// process umask and gives no mode hook; a journal or WAL sidecar it creates later takes the
/// database file's own mode on Unix, and on Windows the state tree's protected, inheritable
/// DACL (checked here) gives every sidecar owner-only access. An existing file is opened as it
/// is; on Windows one with broader access is refused.
pub fn create_private_sqlite(path: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        prepare_private_sqlite(path)
    }
    #[cfg(not(windows))]
    {
        let mut options = broca_owner_only::OpenOptions::new();
        options.write(true).create(true).truncate(false);
        options.open(path).map(|_| ())
    }
}

#[cfg(test)]
type SchemaUpgradeHook = fn() -> Result<(), StoreError>;

fn initialize_journal_mode(
    deadline: std::time::Instant,
    mut initialize: impl FnMut() -> rusqlite::Result<()>,
) -> rusqlite::Result<()> {
    loop {
        match initialize() {
            Ok(()) => return Ok(()),
            Err(error) => {
                // Journal-mode changes must precede the schema transaction. SQLite
                // can skip its busy handler for a lock upgrade, so retry the whole
                // pragma under the same bounded wait rather than weakening schema locking.
                let retry = matches!(&error, rusqlite::Error::SqliteFailure(error, _)
                    if matches!(error.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked))
                    && std::time::Instant::now() < deadline;
                if !retry {
                    return Err(error);
                }
                crate::busy::sleep_between_busy_attempts(
                    std::time::Duration::from_millis(1)
                        .min(deadline.saturating_duration_since(std::time::Instant::now())),
                );
            }
        }
    }
}

#[cfg(test)]
std::thread_local! {
    static SCHEMA_UPGRADE_HOOK: std::cell::RefCell<Option<SchemaUpgradeHook>> = const { std::cell::RefCell::new(None) };
}

impl SqliteStore {
    /// Open an existing projection for inspection without creating a database or running migrations.
    /// Read-only SQLite connections still read committed WAL frames not yet checkpointed into the database.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        require_legacy_operator_root(path.parent().unwrap_or(Path::new(".")), "SqliteStore")
            .map_err(StoreError::LegacyOnly)?;
        Self::open_read_only_decided(path)
    }

    pub fn open_boot_legacy_read_only(
        root: broca_wal::state_format::LegacyStateRoot<'_>,
    ) -> Result<Self, StoreError> {
        Self::open_read_only_decided(&root.root().join("run-index.db"))
    }

    fn open_read_only_decided(path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let (source_instance_id, source_epoch, high_water) = conn.query_row(
            "SELECT source_instance_id, source_epoch, high_water FROM export_meta WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let conn = Arc::new(Mutex::new(conn));
        Ok(Self {
            conn: Arc::clone(&conn),
            wal_config: broca_wal::WalConfig::LEGACY,
            bus_events: bus_events::State::default(),
            export: ExportStore::from_shared(conn, (source_instance_id, source_epoch, high_water)),
        })
    }

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
        require_legacy_operator_root(path.parent().unwrap_or(Path::new(".")), "SqliteStore")
            .map_err(StoreError::LegacyOnly)?;
        Self::open_decided(path)
    }

    /// Boot has already classified this exact root; operator opens remain guarded.
    pub fn open_boot_legacy(
        root: broca_wal::state_format::LegacyStateRoot<'_>,
    ) -> Result<Self, StoreError> {
        Self::open_decided(&root.root().join("run-index.db"))
    }

    fn open_decided(path: &Path) -> Result<Self, StoreError> {
        #[cfg(windows)]
        prepare_private_sqlite(path).map_err(|source| StoreError::PrivateAccess {
            path: path.to_path_buf(),
            source,
        })?;
        sqlite_open_observed();
        let conn = Connection::open(path)?;
        let (source_instance_id, source_epoch, high_water) = Self::init(&conn)?;
        restrict_to_owner(path);
        let conn = Arc::new(Mutex::new(conn));
        Ok(Self {
            conn: Arc::clone(&conn),
            wal_config: broca_wal::WalConfig::LEGACY,
            bus_events: crate::bus_events::State::default(),
            export: ExportStore::from_shared(conn, (source_instance_id, source_epoch, high_water)),
        })
    }

    /// An in-memory store (tests).
    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        let (source_instance_id, source_epoch, high_water) = Self::init(&conn)?;
        let conn = Arc::new(Mutex::new(conn));
        Ok(Self {
            conn: Arc::clone(&conn),
            wal_config: broca_wal::WalConfig::LEGACY,
            bus_events: crate::bus_events::State::default(),
            export: ExportStore::from_shared(conn, (source_instance_id, source_epoch, high_water)),
        })
    }

    /// Supply the root format decided by the caller before sharing this connection.
    /// Unconfigured stores retain legacy SQL and never probe the root for recovery.
    pub fn with_wal_config(mut self, config: broca_wal::WalConfig) -> Self {
        self.wal_config = config;
        self
    }

    /// Discard one session's derived transcript and its last projected WAL sequence
    /// atomically so an installed move is rebuilt from its new WAL. Billing facts and the durable
    /// imported-run IDs that prevent billing another Mac's work again are
    /// machine-local authority and are deliberately not deleted.
    pub fn drop_session_projection(&self, session: &BindIdentity) -> Result<(), String> {
        let key = Self::key(session);
        let mut conn = self.conn.lock().map_err(|e| e.to_string())?;
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM display_fact WHERE session=?1", [&key])
            .map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM display_fact_seed WHERE session=?1", [&key])
            .map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM message WHERE session=?1", [&key])
            .map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM message_tag WHERE session=?1", [&key])
            .map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM episode_span WHERE session=?1", [&key])
            .map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM meta WHERE session=?1", [&key])
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
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

    fn init(conn: &Connection) -> Result<(String, u64, u64), StoreError> {
        crate::busy::configure_writer_connection(conn)?;
        let deadline = std::time::Instant::now() + crate::busy::STORE_BUSY_TIMEOUT;
        crate::busy::with_write_deadline(deadline, || {
            initialize_journal_mode(deadline, || conn.execute_batch("PRAGMA journal_mode=WAL;"))
        })?;
        // Retain WAL sidecars while broca is down so read-only readers can open the index.
        // SQLITE_FCNTL_PERSIST_WAL requires unsafe FFI, which this crate forbids; disabling
        // the close-time checkpoint uses rusqlite's safe API instead.
        if let Err(error) = conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true) {
            tracing::warn!(%error, "could not retain SQLite WAL sidecars on close");
        }
        // Start the schema transaction before inspecting columns. Schema creation
        // and upgrade steps share it so concurrent opens cannot see a partial upgrade.
        let schema_tx =
            rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
        let conn = &schema_tx;
        crate::served_history::initialize(conn)?;
        let from_version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        let has_existing_schema: bool = conn.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type='table' AND name IN ('meta', 'message', 'run_index')
             )",
            [],
            |row| row.get(0),
        )?;
        let _upgrade_log = (from_version < STORE_SCHEMA_VERSION && has_existing_schema)
            .then(|| SchemaUpgradeLog::new(from_version, STORE_SCHEMA_VERSION));
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
              CREATE TABLE IF NOT EXISTS episode_span (
                  session TEXT NOT NULL,
                  start_ord INTEGER NOT NULL,
                  end_ord INTEGER NOT NULL,
                  PRIMARY KEY (session, start_ord)
              );
              CREATE TABLE IF NOT EXISTS message (
                 session TEXT NOT NULL,
                 ord     INTEGER NOT NULL,
                 json    TEXT NOT NULL,
                  PRIMARY KEY (session, ord)
              );
              CREATE TABLE IF NOT EXISTS display_fact_seed (
                  session TEXT NOT NULL, lineage TEXT NOT NULL, state TEXT NOT NULL,
                  PRIMARY KEY(session,lineage)
              );
              CREATE TABLE IF NOT EXISTS display_fact (
                  session TEXT NOT NULL, lineage TEXT NOT NULL, ord INTEGER NOT NULL,
                  seq INTEGER NOT NULL, facts TEXT NOT NULL,
                  PRIMARY KEY(session,lineage,ord,seq)
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
            ("head_json", "TEXT"),
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
            ("compaction_setup_seen", "INTEGER"),
            ("deferred_calls_json", "TEXT"),
            ("run_attribution_version", "INTEGER NOT NULL DEFAULT 0"),
            ("episode_spans_complete", "INTEGER NOT NULL DEFAULT 0"),
            ("episode_spans_seq", "INTEGER"),
            ("open_episode_span", "INTEGER"),
            // Every projected episode's render inputs (start, original first user, first model
            // step, frozen render config), JSON in lineage order. NULL on a row not reprojected
            // since the column existed, and on a session with no episode yet.
            ("episode_metadata_json", "TEXT"),
            // The watermark `episode_metadata_json` was computed at. A row carrying another
            // value is a projection that advanced without recomputing it, so its metadata reads
            // as absent and the session replays from the WAL.
            ("episode_metadata_seq", "INTEGER"),
            // NULL is an older writer's unknown state; a JSON object records even an empty
            // schedule. No default or constraints: adding this column never rewrites rows.
            ("refusal_replay_json", "TEXT"),
        ] {
            if !columns.contains(column) {
                conn.execute(
                    &format!("ALTER TABLE meta ADD COLUMN {column} {sql_type}"),
                    [],
                )?;
            }
        }
        #[cfg(test)]
        SCHEMA_UPGRADE_HOOK.with(|hook| match *hook.borrow() {
            Some(hook) => hook(),
            None => Ok(()),
        })?;
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
        if !message_columns.contains("written_at_ms") {
            conn.execute("ALTER TABLE message ADD COLUMN written_at_ms INTEGER", [])?;
        }
        let add_completion = !message_columns.contains("completed_at_ms");
        let add_creation = !message_columns.contains("created_at_ms");
        if add_completion || add_creation {
            let started = std::time::Instant::now();
            tracing::info!(target: "store.schema", step = "message_display_fact_times",
                added_completed_at_ms = add_completion, added_created_at_ms = add_creation,
                "store schema step started");
            if add_completion {
                conn.execute("ALTER TABLE message ADD COLUMN completed_at_ms INTEGER", [])?;
            }
            if add_creation {
                conn.execute("ALTER TABLE message ADD COLUMN created_at_ms INTEGER", [])?;
            }
            tracing::info!(target: "store.schema", step = "message_display_fact_times",
                elapsed_ms = started.elapsed().as_millis(), "store schema step ended");
        }
        if !message_columns.contains("compaction_json") {
            conn.execute("ALTER TABLE message ADD COLUMN compaction_json TEXT", [])?;
        }
        if !message_columns.contains("compaction_compact_json") {
            conn.execute(
                "ALTER TABLE message ADD COLUMN compaction_compact_json TEXT",
                [],
            )?;
        }
        conn.execute("CREATE INDEX IF NOT EXISTS message_compaction_mid_idx ON message(session,json_extract(compaction_compact_json,'$.mid')) WHERE compaction_compact_json IS NOT NULL", [])?;
        // The one-time index build scans existing messages during upgrade. Thereafter
        // completeness probes visit only this session's unattributed rows, not its bodies.
        conn.execute("CREATE INDEX IF NOT EXISTS message_unattributed_session_idx ON message(session) WHERE run_json IS NULL", [])?;
        // System messages render at the head of every request wherever they sit in the
        // transcript, so a resume under an applied compaction view holds every one of them. This
        // partial index finds them without reading the other rows. A stored message is its serde
        // JSON, whose first member is always `role`; `substr` (unlike a JSON function) cannot fail
        // on a malformed row. The one-time build scans existing rows during upgrade.
        conn.execute(
            &format!(
                "CREATE INDEX IF NOT EXISTS message_system_idx ON message(session, ord) WHERE {SYSTEM_ROW_SQL}"
            ),
            [],
        )?;
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
        conn.execute(
            "CREATE INDEX IF NOT EXISTS meta_head_idx ON meta(session, head_json)",
            [],
        )?;
        retained_head::init(conn)?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS meta_compaction_setup_idx ON meta(session, compaction_setup_seen)",
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
            ("provider_module_id", "TEXT"),
            ("credential_id", "TEXT"),
            ("error_class", "TEXT"),
            ("error_code", "TEXT"),
            ("pause_reason", "TEXT"),
            ("interrupt_reason", "TEXT"),
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
        diagnostics::init_schema(conn)?;
        session_log_evidence::indexes::install(conn)?;
        let source = export_index::init_schema(conn).map_err(StoreError::from)?;
        if from_version < STORE_SCHEMA_VERSION {
            // `user_version` provides one global checkpoint for this idempotent schema path.
            // New databases get the marker without being reported as an upgrade.
            conn.pragma_update(None, "user_version", STORE_SCHEMA_VERSION)?;
        }
        schema_tx.commit()?;
        Ok(source)
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
}

impl broca_core::RunEventBodies for SqliteStore {
    fn read_run_event(&self, event_key: &str) -> Result<Option<(BindIdentity, Vec<u8>)>, String> {
        self.get_bus_event(event_key).map_err(|e| e.to_string())
    }
}

impl SqliteStore {}

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
        let finish_json = final_step_finish_reason
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| e.to_string())?;
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        diagnostics::write_lifecycle(
            &conn,
            run_id,
            state.as_str(),
            usage_json.as_deref(),
            finish_json.as_deref(),
            retries_used,
            None,
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    }

    fn index_run_attributed(
        &self,
        run_id: &str,
        state: RunIndexState,
        usage: Option<Usage>,
        final_step_finish_reason: Option<FinishReason>,
        retries_used: u32,
        attribution: &broca_core::RunIndexAttribution,
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
        diagnostics::write_lifecycle(
            &conn,
            run_id,
            state.as_str(),
            usage_json.as_deref(),
            finish_reason_json.as_deref(),
            retries_used,
            Some(attribution),
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
        let tx = if self.wal_config.is_overhaul() {
            Some(
                conn.unchecked_transaction()
                    .map_err(|error| error.to_string())?,
            )
        } else {
            None
        };
        let connection = tx.as_deref().unwrap_or(&conn);
        let row = connection
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
                if tx
                    .as_ref()
                    .map(|tx| wal_recovery::read_fence(tx, &Self::key(&session)))
                    .transpose()
                    .map_err(|error| error.to_string())?
                    .flatten()
                    .is_some_and(|fence| fence.pending)
                {
                    return Err(format!(
                        "WAL recovery is pending for run {run_id}; run summary is unavailable"
                    ));
                }
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

/// The `message` rows `message_system_idx` covers: system messages (see the index).
const SYSTEM_ROW_SQL: &str = "substr(json, 1, 16) = '{\"role\":\"system\"'";

#[cfg(test)]
mod tests {
    #[test]
    fn boot_selected_legacy_opens_do_not_read_the_marker_again() {
        use broca_wal::fs_ops::{self, FsOps};
        use std::{
            path::{Path, PathBuf},
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
        };
        struct CountMarker {
            marker: PathBuf,
            reads: Arc<AtomicUsize>,
        }
        impl FsOps for CountMarker {
            fn open(
                &self,
                path: &Path,
                options: &fs_ops::OpenOptions,
            ) -> std::io::Result<Box<dyn fs_ops::FileOps>> {
                if path == self.marker {
                    self.reads.fetch_add(1, Ordering::Relaxed);
                    return Err(std::io::Error::other("marker unavailable after boot"));
                }
                fs_ops::RealFs.open(path, options)
            }
            fn metadata(&self, p: &Path) -> std::io::Result<std::fs::Metadata> {
                fs_ops::RealFs.metadata(p)
            }
            fn create_dir_all(&self, p: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.create_dir_all(p)
            }
            fn rename(&self, a: &Path, b: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.rename(a, b)
            }
            fn remove_file(&self, p: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.remove_file(p)
            }
            fn sync_dir(&self, p: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.sync_dir(p)
            }
            fn create_dir(&self, p: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.create_dir(p)
            }
            fn hard_link(&self, a: &Path, b: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.hard_link(a, b)
            }
            fn canonicalize(&self, p: &Path) -> std::io::Result<PathBuf> {
                fs_ops::RealFs.canonicalize(p)
            }
            fn read_dir(&self, p: &Path) -> std::io::Result<fs_ops::ReadDir> {
                fs_ops::RealFs.read_dir(p)
            }
            fn validate_if_exists(&self, p: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.validate_if_exists(p)
            }
        }
        let root = broca_test_support::scratch_root()
            .join(format!("boot-open-marker-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("run-index.db");
        drop(super::SqliteStore::open(&path).unwrap());
        let decision = broca_wal::state_format::decide_state_root(&root).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        fs_ops::with_fs_ops(
            Arc::new(CountMarker {
                marker: root.join("state-format"),
                reads: reads.clone(),
            }),
            || {
                drop(super::SqliteStore::open_boot_legacy(decision.legacy().unwrap()).unwrap());
                drop(
                    super::SqliteStore::open_boot_legacy_read_only(decision.legacy().unwrap())
                        .unwrap(),
                );
                assert_eq!(reads.load(Ordering::Relaxed), 0);
                assert!(super::SqliteStore::open(&path).is_err());
                assert!(super::SqliteStore::open_read_only(&path).is_err());
                assert_eq!(reads.load(Ordering::Relaxed), 2);
            },
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_store_refuses_overhaul_destinations_without_touching_files() {
        let root = broca_test_support::scratch_root()
            .join(format!("legacy-store-overhaul-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("state-format"), "broca-state/2").unwrap();
        let path = root.join("run-index.db");
        assert!(matches!(
            super::SqliteStore::open(&path),
            Err(super::StoreError::LegacyOnly(_))
        ));
        assert!(!path.exists());
        let files = ["run-index.db", "run-index.db-wal", "run-index.db-shm"];
        for name in files {
            std::fs::write(root.join(name), format!("untouched {name}")).unwrap();
        }
        assert!(matches!(
            super::SqliteStore::open(&path),
            Err(super::StoreError::LegacyOnly(_))
        ));
        assert!(matches!(
            super::SqliteStore::open_read_only(&path),
            Err(super::StoreError::LegacyOnly(_))
        ));
        for name in files {
            assert_eq!(
                std::fs::read(root.join(name)).unwrap(),
                format!("untouched {name}").as_bytes()
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn schema_upgrade_failure_rolls_back_every_partial_change() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        super::SCHEMA_UPGRADE_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(|| {
                Err(super::StoreError::InconsistentSnapshot(
                    "injected schema failure",
                ))
            })
        });
        let result = SqliteStore::init(&conn);
        super::SCHEMA_UPGRADE_HOOK.with(|hook| *hook.borrow_mut() = None);
        assert!(result.is_err());
        let tables: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            tables, 0,
            "failed initialization must not publish an upgraded prefix of the schema"
        );
        SqliteStore::init(&conn).unwrap();
        SqliteStore::init(&conn).unwrap();
    }

    #[test]
    fn concurrent_store_opens_share_one_complete_schema_and_export_identity() {
        let path = broca_test_support::scratch_root().join("concurrent-store-init.sqlite");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    SqliteStore::open(path)
                        .unwrap()
                        .source_instance_id()
                        .to_owned()
                })
            })
            .collect();
        let identities: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert!(identities.iter().all(|identity| identity == &identities[0]));
    }

    #[test]
    fn journal_mode_initialization_retries_only_busy_within_its_budget() {
        let busy = || {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                None,
            )
        };
        let mut attempts = 0;
        super::initialize_journal_mode(
            std::time::Instant::now() + std::time::Duration::from_secs(1),
            || {
                attempts += 1;
                if attempts == 1 {
                    Err(busy())
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
        assert_eq!(attempts, 2);
        for expired in [true, false] {
            let mut attempts = 0;
            let deadline = std::time::Instant::now()
                + if expired {
                    std::time::Duration::ZERO
                } else {
                    std::time::Duration::from_secs(1)
                };
            assert!(super::initialize_journal_mode(deadline, || {
                attempts += 1;
                if expired {
                    Err(busy())
                } else {
                    Err(rusqlite::Error::InvalidQuery)
                }
            })
            .is_err());
            assert_eq!(attempts, 1);
        }
    }

    use std::sync::{Arc, Mutex};

    use broca_core::RunIndex;

    use super::{ExportStore, SqliteStore};

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
        let conn = Arc::new(Mutex::new(conn));
        let store = SqliteStore {
            wal_config: broca_wal::WalConfig::LEGACY,
            bus_events: crate::bus_events::State::default(),
            conn: Arc::clone(&conn),
            export: ExportStore::from_shared(conn, (source_instance_id, source_epoch, high_water)),
        };
        let row = store
            .lookup_run("legacy-run")
            .expect("lookup legacy row")
            .expect("legacy row exists");
        assert_eq!(row.usage, None);
        assert_eq!(row.final_step_finish_reason, None);
        assert_eq!(row.request_identity_digest, None);
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
        let conn = Arc::new(Mutex::new(conn));
        let store = SqliteStore {
            wal_config: broca_wal::WalConfig::LEGACY,
            bus_events: crate::bus_events::State::default(),
            conn: Arc::clone(&conn),
            export: ExportStore::from_shared(conn, (source_instance_id, source_epoch, high_water)),
        };
        let row = store.lookup_run("legacy-run").unwrap().unwrap();
        assert_eq!(row.title, None);
    }

    #[test]
    fn completion_column_upgrade_preserves_existing_message_rows_without_backfill() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE message (session TEXT NOT NULL, ord INTEGER NOT NULL, json TEXT NOT NULL, PRIMARY KEY(session,ord)); INSERT INTO message VALUES ('legacy',0,'unchanged');").unwrap();
        SqliteStore::init(&conn).unwrap();
        let (body, completion, creation): (String, Option<u64>, Option<u64>) = conn
            .query_row(
                "SELECT json,completed_at_ms,created_at_ms FROM message WHERE session='legacy' AND ord=0",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(body, "unchanged");
        assert_eq!(completion, None);
        assert_eq!(creation, None);
        for column in ["completed_at_ms", "created_at_ms"] {
            let default: Option<String> = conn
                .query_row(
                    "SELECT dflt_value FROM pragma_table_info('message') WHERE name=?1",
                    [column],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(default, None);
        }
        let indexes: u64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='message' AND sql LIKE '%created_at_ms%'", [], |row| row.get(0),
        ).unwrap();
        assert_eq!(indexes, 0);
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
    fn run_index_conformance() {
        crate::conformance::assert_run_index_conformance(sqlite_store);
    }

    #[test]
    fn export_index_conformance() {
        crate::conformance::assert_export_conformance(sqlite_store);
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
