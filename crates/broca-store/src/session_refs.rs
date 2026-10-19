//! Additive, durable agent-data discovery projection. No WAL payload is cached.
use crate::{SqliteStore, StoreError};
use broca_core::{agent_session_ref, session_addr, SessionRefIndex};
use broca_protocol::BindIdentity;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::BTreeMap;

#[derive(Default, Debug)]
pub struct SessionRefs {
    pub sessions: BTreeMap<String, (String, BindIdentity)>,
    pub complete: bool,
}

#[derive(Default, Debug)]
pub struct BackfillCounts {
    pub rows: u64,
    pub distinct_sessions: u64,
    pub inserted: u64,
    pub writes: MigrationWriteStats,
}

mod backfill;
mod batches;
mod census;
pub use batches::MigrationWriteStats;
pub use census::{census, migrate, CensusCounts};

pub fn init_schema(conn: &Connection) -> Result<(), StoreError> {
    init_schema_measured(conn, &mut MigrationWriteStats::default())
}

fn init_schema_measured(
    conn: &Connection,
    stats: &mut MigrationWriteStats,
) -> Result<(), StoreError> {
    // Managed writers record the candidate and writer token in one transaction.
    // Unmanaged run writes instead retain pending identities until migration.
    let schema = [
        "CREATE TABLE IF NOT EXISTS agent_data_session_ref (
            session_ref TEXT PRIMARY KEY CHECK(length(session_ref)=32 AND session_ref NOT GLOB '*[^0-9a-f]*'),
            address TEXT NOT NULL, project_root TEXT NOT NULL, harness TEXT NOT NULL, session TEXT NOT NULL
         ) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS agent_data_session_address ON agent_data_session_ref(address)",
        "CREATE TABLE IF NOT EXISTS agent_data_projection (id INTEGER PRIMARY KEY CHECK(id=1), backfilled INTEGER NOT NULL)",
        "CREATE TABLE IF NOT EXISTS agent_data_census (address TEXT PRIMARY KEY, covered INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS agent_data_census_pending ON agent_data_census(address) WHERE covered=0",
        "CREATE TABLE IF NOT EXISTS agent_data_census_inflight (address TEXT PRIMARY KEY) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS agent_data_census_done (id INTEGER PRIMARY KEY CHECK(id=1))",
        "CREATE TABLE IF NOT EXISTS agent_data_pending_session (session TEXT PRIMARY KEY) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS agent_data_writer (id INTEGER PRIMARY KEY CHECK(id=1))",
        "CREATE TABLE IF NOT EXISTS agent_data_backfill_cursor (id INTEGER PRIMARY KEY CHECK(id=1), last_run_id TEXT, valid INTEGER NOT NULL)",
        "CREATE TRIGGER IF NOT EXISTS agent_data_run_insert AFTER INSERT ON run_index
         WHEN NOT EXISTS(SELECT 1 FROM agent_data_writer) BEGIN
            INSERT OR IGNORE INTO agent_data_pending_session VALUES(NEW.session);
            DELETE FROM agent_data_census_done;
         END",
        "CREATE TRIGGER IF NOT EXISTS agent_data_run_update AFTER UPDATE OF session ON run_index
         WHEN NOT EXISTS(SELECT 1 FROM agent_data_writer) BEGIN
            INSERT OR IGNORE INTO agent_data_pending_session VALUES(NEW.session);
            DELETE FROM agent_data_census_done;
         END",
    ];
    let mut used = 0;
    while used < schema.len() {
        used += batches::write_batch(
            conn,
            &schema[used..],
            stats,
            true,
            |tx, sql| {
                tx.execute_batch(sql)?;
                Ok(())
            },
            |_, _| Ok(()),
        )?;
    }
    Ok(())
}

/// The same one-time migration used by daemon startup and the copy-only timing tool.
/// Triggers preserve lag information even if an older binary writes run_index.
pub fn backfill(conn: &Connection) -> Result<BackfillCounts, StoreError> {
    backfill::run(conn)
}

pub(crate) fn insert(
    conn: &Connection,
    address: &str,
    session: &BindIdentity,
) -> Result<usize, StoreError> {
    // Historical triples in re-keyed logs do not establish ownership at this address.
    if session_addr(session) != address {
        return Ok(0);
    }
    Ok(conn.execute(
        "INSERT OR IGNORE INTO agent_data_session_ref(session_ref,address,project_root,harness,session) VALUES(?1,?2,?3,?4,?5)",
        params![agent_session_ref(session),address,session.project_root.to_string_lossy(),session.harness,session.session],
    )?)
}

#[cfg(test)]
thread_local! {
    pub(super) static REF_WAIT_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

impl SessionRefIndex for SqliteStore {
    fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String> {
        let deadline = std::time::Instant::now() + crate::STORE_BUSY_TIMEOUT;
        let conn = loop {
            match self.conn.try_lock() {
                Ok(conn) => break conn,
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    return Err(format!("session-ref store poisoned: {error}"))
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    #[cfg(test)]
                    REF_WAIT_HOOK.with(|hook| {
                        if let Some(hook) = hook.borrow_mut().take() {
                            hook();
                        }
                    });
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        return Err("session-ref store busy timeout waiting for connection".into());
                    }
                    std::thread::sleep(remaining.min(std::time::Duration::from_millis(1)));
                }
            }
        };
        // Share one bounded budget between the connection mutex and SQLite's
        // write lock. Routine migration contention must not create a WAL gap.
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err("session-ref store busy timeout before write".into());
        }
        let timeout: u64 = conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .map_err(|error| error.to_string())?;
        conn.busy_timeout(remaining.min(std::time::Duration::from_millis(timeout)))
            .map_err(|error| error.to_string())?;
        let result = insert(&conn, address, session).map_err(|error| error.to_string());
        conn.busy_timeout(std::time::Duration::from_millis(timeout))
            .map_err(|error| error.to_string())?;
        result?;
        Ok(())
    }
}

impl SqliteStore {
    /// Missing refs are absent only in a complete projection snapshot. Pending
    /// legacy writes, an unfinished backfill, and database failures never prove absence.
    pub fn session_refs<'a>(
        &self,
        refs: impl Iterator<Item = &'a str>,
    ) -> Result<SessionRefs, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let root = conn
            .path()
            .map(std::path::Path::new)
            .and_then(std::path::Path::parent)
            .map(std::path::Path::to_path_buf);
        let gap = || -> Result<bool, StoreError> {
            root.as_deref()
                .map(broca_core::session_projection_incomplete)
                .transpose()
                .map(|exists| exists.unwrap_or(false))
                .map_err(|_| {
                    StoreError::InconsistentSnapshot(
                        "cannot inspect session-ref projection incomplete marker",
                    )
                })
        };
        let was_incomplete = gap()?;
        let tx = conn.transaction()?;
        let complete: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_data_projection WHERE id=1 AND backfilled=1)
             AND EXISTS(SELECT 1 FROM agent_data_census_done WHERE id=1)
             AND NOT EXISTS(SELECT 1 FROM agent_data_pending_session)",
            [],
            |row| row.get(0),
        )?;
        let mut sessions = BTreeMap::new();
        {
            let mut statement = tx.prepare_cached(
                "SELECT address,project_root,harness,session FROM agent_data_session_ref WHERE session_ref=?1",
            )?;
            for reference in refs {
                let entry = statement
                    .query_row([reference], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            BindIdentity::new(
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            ),
                        ))
                    })
                    .optional()?;
                if let Some(entry) = entry {
                    sessions.insert(reference.to_owned(), entry);
                }
            }
        }
        tx.commit()?;
        Ok(SessionRefs {
            sessions,
            complete: complete && !was_incomplete && !gap()?,
        })
    }
}

/// Remove only obsolete inventory artifacts, never follow symlinks or recurse.
/// Failure aborts boot rather than silently leaving the old write-heavy cache behind.
pub fn cleanup_legacy_inventory(root: &std::path::Path) -> std::io::Result<(u64, u64)> {
    let mut count = 0;
    let mut bytes = 0;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name != "move-inventory.json" && !name.starts_with("move-inventory.tmp-") {
            continue;
        }
        let metadata = std::fs::symlink_metadata(entry.path())?;
        if metadata.is_dir() {
            continue;
        }
        std::fs::remove_file(entry.path())?;
        count += 1;
        bytes += metadata.len();
    }
    tracing::info!(count, bytes, "removed obsolete move inventory files");
    Ok((count, bytes))
}

#[cfg(test)]
mod contention_tests;
#[cfg(test)]
mod tests;
