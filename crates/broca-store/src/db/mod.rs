//! SQLite index for roots marked `broca-state/2`; other roots cannot open it.
//!
//! Callers supply a durable, published log prefix: bytes beyond that publication
//! boundary must never become visible through this index. Content stays in the log.
//!
//! Index-only commits use SQLite WAL mode with synchronous=NORMAL: losing their
//! newest suffix is safe because reconciliation replays the durable log. Writers
//! of authoritative state use FULL, which also makes earlier WAL commits durable.
//! Reconciliation must still handle an index ahead of a restored or torn log.

mod index;
mod publication;
mod rebuild;
mod schema;
mod seeds;

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{Connection, Transaction, TransactionBehavior};

pub use index::{IndexOutcome, LogLocation, PublishedSession};
pub use rebuild::{AddressFloor, CondemnedLineage, LedgerRows};

#[derive(Debug)]
pub enum DbError {
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
    Projection(broca_core::projection_plan::ProjectionPlanError),
    Invalid(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Sqlite(e) => e.fmt(f),
            Self::Projection(e) => e.fmt(f),
            Self::Invalid(e) => f.write_str(e),
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
}

impl OverhaulDb {
    /// Read a consistent index snapshot. SQLite's query-only guard prevents an
    /// accidental write from bypassing the transaction durability policy.
    pub fn read_snapshot<T>(
        &self,
        read: impl FnOnce(&Transaction<'_>) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| DbError::Invalid("database writer poisoned".into()))?;
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
        };
        db.authoritative_transaction(|tx| {
            schema::initialize(tx)?;
            Ok(())
        })?;
        Ok(db)
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
