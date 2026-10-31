//! Additive, durable agent-data discovery projection. No WAL payload is cached.
use crate::{SqliteStore, StoreError};
#[cfg(test)]
use broca_core::{agent_session_ref, session_addr, SessionRefIndex};
use broca_protocol::BindIdentity;
#[cfg(test)]
use rusqlite::params;
use rusqlite::Connection;
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
mod write;
pub use batches::MigrationWriteStats;
pub use census::{census, migrate, CensusCounts};
pub(crate) use write::insert;
#[cfg(test)]
pub(super) use write::REF_WAIT_HOOK;

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
        "CREATE TABLE IF NOT EXISTS agent_data_session_ref_v2 (
            session_ref TEXT NOT NULL, address TEXT NOT NULL,
            PRIMARY KEY(session_ref,address)
         ) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS agent_data_session_address ON agent_data_session_ref(address)",
        "CREATE TABLE IF NOT EXISTS agent_data_projection (id INTEGER PRIMARY KEY CHECK(id=1), backfilled INTEGER NOT NULL)",
        "CREATE TABLE IF NOT EXISTS agent_data_census (address TEXT PRIMARY KEY, covered INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS agent_data_census_pending ON agent_data_census(address) WHERE covered=0",
        "CREATE TABLE IF NOT EXISTS agent_data_census_inflight (address TEXT PRIMARY KEY) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS agent_data_census_unreadable (address TEXT PRIMARY KEY, reason TEXT NOT NULL) WITHOUT ROWID",
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
    if !conn.is_autocommit() {
        // Store initialization already owns the schema transaction. Standalone
        // migration calls below retain their paced, short writer transactions.
        for sql in schema {
            conn.execute_batch(sql)?;
        }
        return rekey_v2(conn);
    }
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
    rekey_v2(conn)
}

/// Rebuild external aliases at startup without changing stored WAL addresses.
/// Multiple addresses deliberately survive under one alias so resolution can
/// refuse a pair of harness/session names shared by different roots.
fn rekey_v2(conn: &Connection) -> Result<(), StoreError> {
    let rows = conn
        .prepare("SELECT address,project_root,harness,session FROM agent_data_session_ref")?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                BindIdentity::new(
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ),
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    for (address, session) in rows {
        write::insert_v2(conn, &address, &session)?;
    }
    Ok(())
}

/// The same one-time migration used by daemon startup and the copy-only timing tool.
/// Triggers preserve lag information even if an older binary writes run_index.
pub fn backfill(conn: &Connection) -> Result<BackfillCounts, StoreError> {
    backfill::run(conn)
}

pub(crate) fn projection_gap(root: Option<&std::path::Path>) -> Result<bool, StoreError> {
    root.map(broca_core::session_projection_incomplete)
        .transpose()
        .map(|exists| exists.unwrap_or(false))
        .map_err(|_| {
            StoreError::InconsistentSnapshot(
                "cannot inspect session-ref projection incomplete marker",
            )
        })
}

pub(crate) fn projection_complete(conn: &Connection) -> Result<bool, StoreError> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_data_projection WHERE id=1 AND backfilled=1)
         AND EXISTS(SELECT 1 FROM agent_data_census_done WHERE id=1)
         AND NOT EXISTS(SELECT 1 FROM agent_data_pending_session)
         AND NOT EXISTS(SELECT 1 FROM agent_data_census WHERE covered=0)
         AND NOT EXISTS(SELECT 1 FROM agent_data_census_unreadable u
             JOIN agent_data_census c ON c.address=u.address WHERE c.covered=0)",
        [],
        |row| row.get(0),
    )?)
}

impl SqliteStore {
    /// Missing refs are absent only in a complete projection snapshot. Pending
    /// legacy writes, an unfinished backfill, and database failures never prove absence.
    pub fn session_refs<'a>(
        &self,
        refs: impl Iterator<Item = &'a str>,
    ) -> Result<SessionRefs, StoreError> {
        self.read_session_refs(|tx| {
            let mut sessions = BTreeMap::new();
            let mut statement = tx.prepare_cached(
                "SELECT DISTINCT address,project_root,harness,session FROM agent_data_session_ref
                 WHERE session_ref=?1 OR address IN
                   (SELECT address FROM agent_data_session_ref_v2 WHERE session_ref=?1)",
            )?;
            for reference in refs {
                let entries = statement
                    .query_map([reference], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            BindIdentity::new(
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            ),
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                if entries.len() > 1 {
                    return Err(StoreError::InconsistentSnapshot("session_ref_ambiguous"));
                }
                if let Some(entry) = entries.into_iter().next() {
                    sessions.insert(reference.to_owned(), entry);
                }
            }
            Ok(sessions)
        })
    }

    /// Scan only the compact bind inventory, never the much larger run history.
    /// The limit bounds candidate decoding; callers must refuse a full result page.
    pub fn session_refs_named(&self, name: &str, limit: u32) -> Result<SessionRefs, StoreError> {
        self.read_session_refs(|tx| {
            let mut statement = tx.prepare_cached(
                "SELECT session_ref,address,project_root,harness,session FROM agent_data_session_ref WHERE session=?1 LIMIT ?2",
            )?;
            let rows = statement.query_map(rusqlite::params![name, limit], |row| {
                Ok((row.get::<_, String>(0)?, (row.get::<_, String>(1)?, BindIdentity::new(
                    row.get::<_, String>(2)?, row.get::<_, String>(3)?, row.get::<_, String>(4)?,
                ))))
            })?;
            rows.collect::<Result<BTreeMap<_, _>, _>>().map_err(StoreError::from)
        })
    }

    fn read_session_refs(
        &self,
        select: impl FnOnce(&Connection) -> Result<BTreeMap<String, (String, BindIdentity)>, StoreError>,
    ) -> Result<SessionRefs, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let root = conn
            .path()
            .map(std::path::Path::new)
            .and_then(std::path::Path::parent)
            .map(std::path::Path::to_path_buf);
        let gap = || projection_gap(root.as_deref());
        let was_incomplete = gap()?;
        let tx = conn.transaction()?;
        let complete = projection_complete(&tx)?;
        let sessions = select(&tx)?;
        tx.commit()?;
        Ok(SessionRefs {
            sessions,
            complete: complete && !was_incomplete && !gap()?,
        })
    }
}

/// Remove only obsolete inventory artifacts, never follow symlinks or recurse.
/// Errors other than vanished entries are returned so cleanup cannot silently continue after a failure.
pub fn cleanup_legacy_inventory(root: &std::path::Path) -> std::io::Result<(u64, u64)> {
    cleanup_legacy_inventory_with(
        root,
        |path| std::fs::symlink_metadata(path),
        |path| std::fs::remove_file(path),
    )
}

fn cleanup_legacy_inventory_with(
    root: &std::path::Path,
    mut metadata: impl FnMut(&std::path::Path) -> std::io::Result<std::fs::Metadata>,
    mut remove: impl FnMut(&std::path::Path) -> std::io::Result<()>,
) -> std::io::Result<(u64, u64)> {
    let mut count = 0;
    let mut bytes = 0;
    for entry in std::fs::read_dir(root)? {
        let Some(entry) = listed_inventory_entry(root, entry)? else {
            continue;
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name != "move-inventory.json" && !name.starts_with("move-inventory.tmp-") {
            continue;
        }
        let Some(metadata) = listed_inventory_entry(root, metadata(&entry.path()))? else {
            continue;
        };
        if metadata.is_dir() {
            continue;
        }
        if listed_inventory_entry(root, remove(&entry.path()))?.is_none() {
            continue;
        }
        count += 1;
        bytes += metadata.len();
    }
    tracing::info!(count, bytes, "removed obsolete move inventory files");
    Ok((count, bytes))
}

fn listed_inventory_entry<T>(
    root: &std::path::Path,
    result: std::io::Result<T>,
) -> std::io::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A vanished inventory artifact needs no cleanup, just like an absent one,
            // provided the inventory root is still readable.
            std::fs::read_dir(root)?;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod contention_tests;
#[cfg(test)]
mod tests;
