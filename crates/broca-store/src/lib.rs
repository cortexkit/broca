//! broca's SQLite stores: `run-index.db` ([`SqliteStore`]: the session-ref index, scope
//! reservations, bus outboxes and usage export facts), `system-one.db`, `grants.db` and
//! `late-results.db`.
//!
//! None of them holds a session's records: those live in `broca-session-store`, and anything
//! here that is derived from them loses to them. Kept in its own crate so the bundled SQLite
//! dependency stays out of the run loop's crates.
#![forbid(unsafe_code)]

use std::path::Path;
use std::sync::{Arc, Mutex};

use broca_protocol::BindIdentity;
use rusqlite::{config::DbConfig, Connection};

const STORE_SCHEMA_VERSION: i64 = 1;
/// The size run-index.db's WAL file is cut back to after a checkpoint resets it.
const WAL_SIZE_LIMIT_BYTES: i64 = 64 * 1024 * 1024;

mod busy;
use busy::STORE_BUSY_TIMEOUT;

pub mod bus_events;
pub mod export_index;
pub mod findings_events;
mod keeper_connection;
pub mod legacy_inventory;
pub(crate) use export_index::ExportStore;
pub(crate) use keeper_connection::KeeperOpenError;
pub mod grants;
pub mod late_results_store;
pub mod session_address;
pub mod session_inventory;
mod state_marker;
pub use session_inventory::SessionInventory;
pub mod session_refs;
pub mod system_one;
pub use state_marker::{refuse_marked_state_root, MarkedStateRoot, STATE_FORMAT_MARKER};

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
}

impl SessionAddressResolver for SqliteStore {
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
    /// The log being projected is a corrupt lineage (replay refused to fabricate a
    /// prefix). Surfaced rather than silently materializing a wrong projection.
    Replay(broca_core::ReplayError),
    InconsistentSnapshot(&'static str),
    BusEvent(String),
    MarkedStateRoot(MarkedStateRoot),
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
            StoreError::MarkedStateRoot(e) => write!(f, "{e}"),
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
/// The single `Mutex<Connection>` serializes database access. Projection keeps log replay,
/// checkpoint folding, and JSON serialization outside that mutex so unrelated sessions wait
/// only for the short snapshot and commit phases.
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
    bus_events: bus_events::State,
    export: ExportStore,
}

/// Restrict a store file and its sqlite sidecars to the owner.
///
/// Best-effort by design: a store that opens but cannot be chmod'd should still serve. A file
/// left with wider permissions is logged at WARN, naming it, so the degradation is visible.
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
                if let Err(error) =
                    std::fs::set_permissions(&candidate, std::fs::Permissions::from_mode(0o600))
                {
                    tracing::warn!(path = %candidate.display(), %error, "could not restrict a store file to its owner");
                }
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
    /// Open an existing store for inspection without creating a database or running migrations.
    /// A read-only SQLite connection still reads committed frames of SQLite's own write-ahead log
    /// that are not yet checkpointed into the database.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        refuse_marked_state_root(path.parent().unwrap_or(Path::new(".")))
            .map_err(StoreError::MarkedStateRoot)?;
        Self::open_read_only_decided(path)
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
            bus_events: bus_events::State::default(),
            export: ExportStore::from_shared(conn, (source_instance_id, source_epoch, high_water)),
        })
    }

    /// Open (creating + migrating) a store at `path`.
    ///
    /// Refused in a state root marked with another format (see [`refuse_marked_state_root`]).
    ///
    /// On Unix the file is restricted to its owner after creation. This database holds
    /// session refs, usage facts and outbound events, and sqlite creates it through the umask,
    /// so a default install lands it 0644. sqlite gives no mode hook, so the chmod happens here,
    /// immediately after open, before any row is written.
    ///
    /// Sidecars are covered too: WAL-mode sqlite creates `-wal` and `-shm` beside the
    /// database, and the `-wal` holds committed frames not yet checkpointed into the main
    /// file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        refuse_marked_state_root(path.parent().unwrap_or(Path::new(".")))
            .map_err(StoreError::MarkedStateRoot)?;
        Self::open_decided(path)
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
            bus_events: crate::bus_events::State::default(),
            export: ExportStore::from_shared(conn, (source_instance_id, source_epoch, high_water)),
        })
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
        // SQLite reuses the WAL file from its start after a checkpoint but never shrinks it, so
        // one large transaction (a VACUUM writes the whole database through it) would leave a
        // WAL file of the database's size on disk for good. Truncate it to this after each
        // checkpoint that resets it.
        conn.pragma_update(None, "journal_size_limit", WAL_SIZE_LIMIT_BYTES)?;
        // Start the schema transaction before inspecting columns. Schema creation
        // and upgrade steps share it so concurrent opens cannot see a partial upgrade.
        let schema_tx =
            rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
        let conn = &schema_tx;
        let from_version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        session_refs::init_schema(conn)?;
        bus_events::init_schema(conn)?;
        findings_events::init_schema(conn)?;
        #[cfg(test)]
        SCHEMA_UPGRADE_HOOK.with(|hook| match *hook.borrow() {
            Some(hook) => hook(),
            None => Ok(()),
        })?;
        let source = export_index::init_schema(conn).map_err(StoreError::from)?;
        if from_version < STORE_SCHEMA_VERSION {
            // `user_version` provides one global checkpoint for this idempotent schema path.
            // New databases get the marker without being reported as an upgrade.
            conn.pragma_update(None, "user_version", STORE_SCHEMA_VERSION)?;
        }
        schema_tx.commit()?;
        Ok(source)
    }
}

impl broca_core::RunEventBodies for SqliteStore {
    fn read_run_event(&self, event_key: &str) -> Result<Option<(BindIdentity, Vec<u8>)>, String> {
        self.get_bus_event(event_key).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_marked_state_root_refuses_the_store_without_touching_files() {
        let root = broca_test_support::scratch_root()
            .join(format!("marked-state-root-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("state-format"), "broca-state/2").unwrap();
        let path = root.join("run-index.db");
        assert!(matches!(
            super::SqliteStore::open(&path),
            Err(super::StoreError::MarkedStateRoot(_))
        ));
        assert!(!path.exists());
        let files = ["run-index.db", "run-index.db-wal", "run-index.db-shm"];
        for name in files {
            std::fs::write(root.join(name), format!("untouched {name}")).unwrap();
        }
        assert!(matches!(
            super::SqliteStore::open(&path),
            Err(super::StoreError::MarkedStateRoot(_))
        ));
        assert!(matches!(
            super::SqliteStore::open_read_only(&path),
            Err(super::StoreError::MarkedStateRoot(_))
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

    use broca_core::SessionRefIndex;

    use super::SqliteStore;

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
        let session = crate::conformance::session("mode-probe");
        store
            .record_session(&broca_core::session_addr(&session), &session)
            .expect("record a session reference");

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
        let session = crate::conformance::session("retained");
        let address = broca_core::session_addr(&session);
        store
            .record_session(&address, &session)
            .expect("record a session reference");
        drop(store);

        assert!(
            path.with_file_name("run-index.db-wal").exists(),
            "the log must survive close"
        );
        assert!(
            path.with_file_name("run-index.db-shm").exists(),
            "SHM must survive close"
        );
        let committed = || {
            let uri = format!("file:{}?mode=ro", path.display());
            let reader = rusqlite::Connection::open_with_flags(
                uri,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
            )
            .expect("read-only SQLite reader opens while writer is down");
            reader
                .query_row(
                    "SELECT count(*) FROM agent_data_session_ref WHERE address = ?1",
                    [&address],
                    |row| row.get::<_, i64>(0),
                )
                .expect("read committed session reference")
        };
        assert_eq!(committed(), 1);

        let reopened = SqliteStore::open(&path).expect("reopen writer from retained WAL");
        drop(reopened);
        assert_eq!(committed(), 1, "reopen must keep the row");
        let _ = std::fs::remove_dir_all(dir);
    }

    fn sqlite_store() -> SqliteStore {
        SqliteStore::open_in_memory().expect("in-memory sqlite store opens")
    }

    /// The store's connection cuts its WAL file back after a checkpoint, so one VACUUM doesn't
    /// leave a WAL file the size of the database.
    #[test]
    fn the_store_caps_its_wal_file_size() {
        let store = sqlite_store();
        let limit: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("PRAGMA journal_size_limit", [], |row| row.get(0))
            .unwrap();
        assert_eq!(limit, super::WAL_SIZE_LIMIT_BYTES);
    }

    #[test]
    fn export_index_conformance() {
        crate::conformance::assert_export_conformance(sqlite_store);
    }
}

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
