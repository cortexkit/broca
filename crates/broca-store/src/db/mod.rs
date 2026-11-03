//! SQLite index for roots marked `broca-state/2`; other roots cannot open it.
//!
//! Callers supply a durable, published log prefix: bytes beyond that publication
//! boundary must never become visible through this index. Content stays in the log.
//!
//! Index-only commits use SQLite WAL mode with synchronous=NORMAL: losing their
//! newest suffix is safe because reconciliation replays the durable log. Writers
//! of authoritative state use FULL, which also makes earlier WAL commits durable.
//! Reconciliation must still handle an index ahead of a restored or torn log.

mod episode_reader;
mod index;
mod inventory;
pub use inventory::open_session_resolver;
mod operational;
mod outbox;
pub use operational::{
    open_diagnostic_reader, open_diagnostic_reader_decided, operational_database_name,
    DiagnosticEvidenceReader,
};
pub use outbox::OverhaulRunEventStore;
mod publication;
#[cfg(feature = "publication-faults")]
mod publication_faults;
mod readers;
mod rebuild;
mod reconcile;
mod recovery;
mod resume_reader;
mod retention;
mod schema;
mod seeds;

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{Connection, Transaction, TransactionBehavior};

pub use episode_reader::{DatabaseEpisodeReader, EpisodeSnapshot};
pub use index::{IndexOutcome, LogLocation, PublishedSession};
pub use readers::{RawHeadMedia, RawPageRequest, RawPageSnapshot};
pub use rebuild::{AddressFloor, CondemnedLineage, LedgerRows};
pub use reconcile::{ReconcileOutcome, ReconcileSession};

#[derive(Debug)]
pub enum DbError {
    Io(std::io::Error),
    Wal(broca_wal::WalError),
    Sqlite(rusqlite::Error),
    Projection(broca_core::projection_plan::ProjectionPlanError),
    Invalid(String),
    SeedFallback(broca_core::replay::SeedFallback),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Wal(e) => e.fmt(f),
            Self::Sqlite(e) => e.fmt(f),
            Self::Projection(e) => e.fmt(f),
            Self::Invalid(e) => f.write_str(e),
            Self::SeedFallback(e) => f.write_str(e.reason),
        }
    }
}

impl std::error::Error for DbError {}
impl From<std::io::Error> for DbError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e)
    }
}
impl From<broca_core::projection_plan::ProjectionPlanError> for DbError {
    fn from(e: broca_core::projection_plan::ProjectionPlanError) -> Self {
        Self::Projection(e)
    }
}

pub struct OverhaulDb {
    conn: Mutex<Connection>,
    root: std::path::PathBuf,
    raw_fallback_notices: Mutex<std::collections::HashSet<String>>,
    #[cfg(feature = "test-conformance")]
    snapshot_reads: std::sync::atomic::AtomicU64,
}

pub type SessionGateFactory =
    dyn Fn(&broca_protocol::BindIdentity) -> std::sync::Arc<std::sync::Mutex<()>> + Send + Sync;

impl OverhaulDb {
    /// Counts snapshot requests before opening SQLite so tests can verify that
    /// filesystem-only admission does not enter the database reader at all.
    #[cfg(feature = "test-conformance")]
    pub fn snapshot_reads_for_conformance(&self) -> u64 {
        self.snapshot_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Read a consistent index snapshot. SQLite's query-only guard prevents an
    /// accidental write from bypassing the transaction durability policy.
    pub fn read_snapshot<T>(
        &self,
        read: impl FnOnce(&Transaction<'_>) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        #[cfg(feature = "test-conformance")]
        self.snapshot_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // WAL-mode readers must not queue behind another session's index rebuild.
        let mut conn = Connection::open_with_flags(
            self.root.join("broca.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        conn.busy_timeout(crate::STORE_BUSY_TIMEOUT)?;
        conn.pragma_update(None, "query_only", true)?;
        let result = (|| {
            let tx = conn.transaction()?;
            read(&tx)
        })();
        conn.pragma_update(None, "query_only", false)?;
        result
    }

    /// Opening is deliberately refused on unmarked roots, including empty roots.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, DbError> {
        let root = root.as_ref();
        let marker = std::fs::read(root.join("state-format"))?;
        if marker != b"broca-state/2" && marker != b"broca-state/2\n" {
            return Err(DbError::Invalid(
                "broca.db requires a broca-state/2 root".into(),
            ));
        }
        let path = root.join("broca.db");
        crate::create_private_sqlite(&path)?;
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", true)?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        let db = Self {
            conn: Mutex::new(conn),
            root: root.to_path_buf(),
            raw_fallback_notices: Mutex::new(Default::default()),
            #[cfg(feature = "test-conformance")]
            snapshot_reads: Default::default(),
        };
        db.authoritative_transaction(|tx| {
            schema::initialize(tx)?;
            crate::served_history::initialize(tx)?;
            inventory::initialize(tx)?;
            retention::initialize(tx)?;
            operational::initialize(tx)?;
            recovery::initialize(tx)?;
            outbox::initialize(tx)?;
            Ok(())
        })?;
        Ok(db)
    }

    /// Operational readers never initialize a missing database or migrate a root.
    pub fn open_read_only(root: impl AsRef<Path>) -> Result<Self, DbError> {
        let root = root.as_ref();
        let marker = std::fs::read(root.join("state-format"))?;
        if marker != b"broca-state/2" && marker != b"broca-state/2\n" {
            return Err(DbError::Invalid(
                "broca.db requires a broca-state/2 root".into(),
            ));
        }
        let conn = Connection::open_with_flags(
            root.join("broca.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(Self {
            conn: Mutex::new(conn),
            root: root.to_path_buf(),
            raw_fallback_notices: Mutex::new(Default::default()),
            #[cfg(feature = "test-conformance")]
            snapshot_reads: Default::default(),
        })
    }

    /// All external writers use FULL, including mixed authoritative/index updates.
    /// Indexing and rebuild use NORMAL; other writers always use FULL.
    pub fn authoritative_transaction<T>(
        &self,
        write: impl FnOnce(&Transaction<'_>) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        self.transaction(true, write)
    }

    fn transaction<T>(
        &self,
        authoritative: bool,
        write: impl FnOnce(&Transaction<'_>) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| DbError::Invalid("database writer poisoned".into()))?;
        // SQLite refuses synchronous changes inside a transaction. Switch while
        // holding the sole writer lock, before BEGIN, on every transaction.
        conn.pragma_update(
            None,
            "synchronous",
            if authoritative { "FULL" } else { "NORMAL" },
        )?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = write(&tx)?;
        tx.commit()?;
        Ok(result)
    }
}

impl broca_core::ServedHistory for OverhaulDb {
    fn history_read_version(&self, address: &str) -> Result<Option<u64>, String> {
        crate::served_history::read_version(&self.conn.lock().unwrap(), address)
    }

    fn served_snapshot(
        &self,
        address: &str,
        has_log: bool,
        lineage: Option<&str>,
        expected_version: Option<Option<u64>>,
    ) -> Result<broca_core::served_history::ServedSnapshot, String> {
        crate::served_history::version(
            &mut self.conn.lock().unwrap(),
            address,
            has_log,
            lineage,
            expected_version,
        )
    }

    fn history_rewritten(
        &self,
        session: &broca_protocol::BindIdentity,
        lineage: Option<&str>,
    ) -> Result<(), String> {
        crate::served_history::rewritten(&mut self.conn.lock().unwrap(), session, lineage)
    }
    fn history_rewrite_finished(
        &self,
        session: &broca_protocol::BindIdentity,
    ) -> Result<(), String> {
        crate::served_history::rewrite_finished(&mut self.conn.lock().unwrap(), session)
    }
    fn rewriting_sessions(&self) -> Result<Vec<broca_protocol::BindIdentity>, String> {
        crate::served_history::rewriting_sessions(&self.conn.lock().unwrap())
    }
}
