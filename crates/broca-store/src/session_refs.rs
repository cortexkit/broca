//! The session-ref index: which stored session each agent-data ref names, so an agent move
//! finds its sessions without reading every stored head. No session content is cached.
use crate::{SessionInventory, SqliteStore, StoreError};
use broca_core::agent_session_ref;
#[cfg(test)]
use broca_core::session_addr;
use broca_core::SessionRefIndex;
use broca_protocol::BindIdentity;
use rusqlite::params;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use std::collections::BTreeMap;

pub use broca_core::SessionRefs;

mod batches;
mod census;
mod stored;
mod write;
pub(crate) use batches::MigrationWriteStats;
pub use census::{census, migrate, CensusCounts};
pub use stored::StoredSessions;
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
        "CREATE TABLE IF NOT EXISTS agent_data_session_ref_v2_rule (id INTEGER PRIMARY KEY CHECK(id=1), probe TEXT NOT NULL, sessions INTEGER NOT NULL)",
        "CREATE TABLE IF NOT EXISTS agent_data_census (address TEXT PRIMARY KEY, covered INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS agent_data_census_pending ON agent_data_census(address) WHERE covered=0",
        "CREATE TABLE IF NOT EXISTS agent_data_census_inflight (address TEXT PRIMARY KEY) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS agent_data_census_unreadable (address TEXT PRIMARY KEY, reason TEXT NOT NULL) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS agent_data_census_done (id INTEGER PRIMARY KEY CHECK(id=1))",
        crate::session_address::SCOPE_RESERVATION_SCHEMA,
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

/// Rebuild external aliases at startup without changing stored log addresses.
/// Multiple addresses deliberately survive under one alias so resolution can
/// refuse a pair of harness/session names shared by different roots.
///
/// Every insert keeps the aliases current, so a rebuild is needed only when the rule deriving an
/// alias changed since the last one, or when sessions were added by a build that keeps no rule
/// (an older one, after a rollback). The table records the alias of a fixed identity under the
/// rule of the build that rebuilt, and how many sessions there were: a start that finds both
/// unchanged skips the walk.
fn rekey_v2(conn: &Connection) -> Result<(), StoreError> {
    let probe = agent_session_ref(&BindIdentity::new(
        "/alias-rule-probe",
        "alias-rule-probe",
        "alias-rule-probe",
    ));
    let sessions: i64 =
        conn.query_row("SELECT count(*) FROM agent_data_session_ref", [], |row| {
            row.get(0)
        })?;
    let recorded: Option<(String, i64)> = conn
        .query_row(
            "SELECT probe, sessions FROM agent_data_session_ref_v2_rule WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if recorded == Some((probe.clone(), sessions)) {
        return Ok(());
    }
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
    conn.execute(
        "INSERT OR REPLACE INTO agent_data_session_ref_v2_rule(id,probe,sessions) VALUES(1,?1,?2)",
        params![probe, sessions],
    )?;
    Ok(())
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
        "SELECT EXISTS(SELECT 1 FROM agent_data_census_done WHERE id=1)
         AND NOT EXISTS(SELECT 1 FROM agent_data_census WHERE covered=0)
         AND NOT EXISTS(SELECT 1 FROM agent_data_census_unreadable u
             JOIN agent_data_census c ON c.address=u.address WHERE c.covered=0)",
        [],
        |row| row.get(0),
    )?)
}

impl SessionInventory {
    /// Missing refs are absent only in a complete projection snapshot. An unfinished census
    /// and database failures never prove absence.
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
        let (sessions, complete) = self.read_inventory(select)?;
        Ok(SessionRefs { sessions, complete })
    }

    fn read_inventory<T>(
        &self,
        select: impl FnOnce(&Connection) -> Result<T, StoreError>,
    ) -> Result<(T, bool), StoreError> {
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
        Ok((sessions, complete && !was_incomplete && !gap()?))
    }
}

impl SqliteStore {
    pub fn session_refs<'a>(
        &self,
        refs: impl Iterator<Item = &'a str>,
    ) -> Result<SessionRefs, StoreError> {
        SessionInventory::shared(&self.conn).session_refs(refs)
    }

    pub fn session_refs_named(&self, name: &str, limit: u32) -> Result<SessionRefs, StoreError> {
        SessionInventory::shared(&self.conn).session_refs_named(name, limit)
    }
}

impl SessionRefIndex for SqliteStore {
    fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String> {
        SessionInventory::shared(&self.conn).record_session(address, session)
    }
}

#[cfg(test)]
mod contention_tests;
#[cfg(test)]
mod tests;
