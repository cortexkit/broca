//! Snapshot identity survives log retention and rebuilds of the materialized message table.

use broca_core::served_history::{ServedHistory, ServedSnapshot};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

pub(crate) fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS served_history (
            session TEXT PRIMARY KEY,
            incarnation TEXT NOT NULL,
            lineage_id TEXT,
            version TEXT NOT NULL,
            rewriting_session TEXT
        );
        CREATE INDEX IF NOT EXISTS served_history_rewriting_idx ON served_history(session)
            WHERE rewriting_session IS NOT NULL;",
    )
}

#[derive(Clone)]
struct Row {
    incarnation: String,
    lineage: Option<String>,
    version: u64,
    rewriting_session: Option<String>,
}

fn row(conn: &Connection, address: &str) -> Result<Option<Row>, String> {
    let value: Option<(String, Option<String>, String, Option<String>)> = conn
        .query_row(
            "SELECT incarnation, lineage_id, version, rewriting_session FROM served_history WHERE session=?1",
            [address],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    value
        .map(|(incarnation, lineage, version, rewriting_session)| {
            Ok(Row {
                incarnation,
                lineage,
                version: version.parse::<u64>().map_err(|error| error.to_string())?,
                rewriting_session,
            })
        })
        .transpose()
}

fn checked_row(
    conn: &Connection,
    address: &str,
    expected: Option<Option<u64>>,
) -> Result<Option<Row>, String> {
    let row = row(conn, address)?;
    if row
        .as_ref()
        .is_some_and(|row| row.rewriting_session.is_some())
    {
        return Err(broca_core::served_history::HISTORY_REWRITING.into());
    }
    if expected.is_some_and(|expected| expected != row.as_ref().map(|row| row.version)) {
        return Err(broca_core::served_history::HISTORY_CHANGED.into());
    }
    Ok(row)
}

pub(crate) fn read_version(conn: &Connection, address: &str) -> Result<Option<u64>, String> {
    Ok(checked_row(conn, address, None)?.map(|row| row.version))
}

fn now_ms() -> Result<u64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_millis()
        .try_into()
        .map_err(|error| format!("history clock overflow: {error}"))
}

fn allocate(previous: Option<u64>, increment: u64, now: u64) -> Result<u64, String> {
    Ok(previous
        .unwrap_or(0)
        .checked_add(increment)
        .ok_or("served history version exhausted")?
        .max(now))
}

fn random_incarnation() -> Result<String, String> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|error| error.to_string())?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn snapshot(row: &Row) -> ServedSnapshot {
    ServedSnapshot {
        incarnation: row.incarnation.clone(),
        history_version: row.version,
    }
}

fn version_at(
    conn: &mut Connection,
    address: &str,
    has_log: bool,
    lineage: Option<&str>,
    now: u64,
    expected_version: Option<Option<u64>>,
) -> Result<ServedSnapshot, String> {
    let prior = checked_row(conn, address, expected_version)?;
    // No-log reads never write: 0 before any log, or the high-water + 1 after deletion.
    // A recreated log skips that empty version (+2), with wall time as the allocation floor.
    if !has_log {
        return Ok(ServedSnapshot {
            incarnation: "unwritten".into(),
            history_version: prior.map_or(Ok(0), |row| {
                row.version
                    .checked_add(1)
                    .ok_or_else(|| "served history version exhausted".to_owned())
            })?,
        });
    }
    if let Some(row) = &prior {
        if row.lineage.as_deref() == lineage {
            return Ok(snapshot(row));
        }
    }
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(|error| error.to_string())?;
    let tx = match conn.transaction_with_behavior(TransactionBehavior::Immediate) {
        Ok(tx) => tx,
        // A busy index must not fail a read. Serve the version this write would have recorded,
        // unrecorded: allocation takes the larger of the next version and the wall clock, so a
        // later recorded version is never below one served now. The cost is that a client may
        // see the version move and reload.
        Err(error) if is_busy(&error) => return Ok(snapshot(&next_row(prior, lineage, now)?)),
        Err(error) => return Err(error.to_string()),
    };
    let prior = checked_row(&tx, address, expected_version)?;
    let current = next_row(prior, lineage, now)?;
    tx.execute(
        "INSERT INTO served_history(session,incarnation,lineage_id,version) VALUES(?1,?2,?3,?4)
         ON CONFLICT(session) DO UPDATE SET incarnation=excluded.incarnation,lineage_id=excluded.lineage_id,version=excluded.version",
        params![address, current.incarnation, current.lineage, current.version.to_string()],
    ).map_err(|error| error.to_string())?;
    tx.commit().map_err(|error| error.to_string())?;
    Ok(snapshot(&current))
}

/// The row a served read records for `lineage`: the prior row when it already names this log,
/// otherwise a new incarnation under the next allocated version.
fn next_row(prior: Option<Row>, lineage: Option<&str>, now: u64) -> Result<Row, String> {
    Ok(match prior {
        Some(row) if row.lineage.as_deref() == lineage => row,
        // The first appended lineage frame identifies the same previously unframed legacy log.
        Some(mut row) if row.lineage.is_none() && lineage.is_some() => {
            row.lineage = lineage.map(str::to_owned);
            row
        }
        prior => Row {
            incarnation: match lineage {
                Some(id) => id.to_owned(),
                None => random_incarnation()?,
            },
            lineage: lineage.map(str::to_owned),
            version: allocate(
                prior.as_ref().map(|row| row.version),
                if prior.is_some() { 2 } else { 1 },
                now,
            )?,
            rewriting_session: None,
        },
    })
}

fn is_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

pub(crate) fn version(
    conn: &mut Connection,
    address: &str,
    has_log: bool,
    lineage: Option<&str>,
    expected_version: Option<Option<u64>>,
) -> Result<ServedSnapshot, String> {
    version_at(conn, address, has_log, lineage, now_ms()?, expected_version)
}

pub(crate) fn rewritten(
    conn: &mut Connection,
    session: &broca_protocol::BindIdentity,
    lineage: Option<&str>,
) -> Result<(), String> {
    let address = broca_core::session_addr(session);
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(|error| error.to_string())?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| error.to_string())?;
    let prior = row(&tx, &address)?;
    let lineage = lineage
        .map(str::to_owned)
        .or_else(|| prior.as_ref().and_then(|row| row.lineage.clone()));
    let lineage = lineage.as_deref();
    let same = prior
        .as_ref()
        .is_some_and(|row| row.lineage.as_deref() == lineage || row.lineage.is_none());
    let incarnation = match (&prior, lineage) {
        (Some(row), _) if same => row.incarnation.clone(),
        (_, Some(lineage)) => lineage.to_owned(),
        _ => random_incarnation()?,
    };
    let next = match &prior {
        Some(row) if same && row.rewriting_session.is_some() => row.version,
        _ => allocate(
            prior.as_ref().map(|row| row.version),
            if prior.is_some() && !same { 2 } else { 1 },
            now_ms()?,
        )?,
    };
    let identity = serde_json::to_string(session).map_err(|error| error.to_string())?;
    tx.execute("INSERT INTO served_history(session,incarnation,lineage_id,version,rewriting_session) VALUES(?1,?2,?3,?4,?5)
        ON CONFLICT(session) DO UPDATE SET incarnation=excluded.incarnation,lineage_id=excluded.lineage_id,version=excluded.version,rewriting_session=excluded.rewriting_session",
        params![address, incarnation, lineage, next.to_string(), identity]).map_err(|error| error.to_string())?;
    tx.commit().map_err(|error| error.to_string())
}

pub(crate) fn rewrite_finished(
    conn: &mut Connection,
    session: &broca_protocol::BindIdentity,
) -> Result<(), String> {
    if !row(conn, &broca_core::session_addr(session))?
        .is_some_and(|row| row.rewriting_session.is_some())
    {
        return Ok(());
    }
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(|error| error.to_string())?;
    conn.execute(
        "UPDATE served_history SET rewriting_session=NULL WHERE session=?1",
        [broca_core::session_addr(session)],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn rewriting_sessions(
    conn: &Connection,
) -> Result<Vec<broca_protocol::BindIdentity>, String> {
    let mut statement = conn.prepare("SELECT rewriting_session FROM served_history INDEXED BY served_history_rewriting_idx WHERE rewriting_session IS NOT NULL ORDER BY session").map_err(|error| error.to_string())?;
    let values = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?;
    values
        .map(|row| {
            serde_json::from_str(&row.map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())
        })
        .collect()
}

impl ServedHistory for crate::SqliteStore {
    fn history_read_version(&self, address: &str) -> Result<Option<u64>, String> {
        read_version(&self.conn.lock().unwrap(), address)
    }
    fn served_snapshot(
        &self,
        address: &str,
        has_log: bool,
        lineage: Option<&str>,
        expected_version: Option<Option<u64>>,
    ) -> Result<ServedSnapshot, String> {
        version(
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
        rewritten(&mut self.conn.lock().unwrap(), session, lineage)
    }
    fn history_rewrite_finished(
        &self,
        session: &broca_protocol::BindIdentity,
    ) -> Result<(), String> {
        rewrite_finished(&mut self.conn.lock().unwrap(), session)
    }
    fn rewriting_sessions(&self) -> Result<Vec<broca_protocol::BindIdentity>, String> {
        rewriting_sessions(&self.conn.lock().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_reads_never_write_and_recreated_logs_skip_the_empty_version() {
        let mut conn = Connection::open_in_memory().unwrap();
        initialize(&conn).unwrap();
        let first = version_at(&mut conn, "address", false, None, 100, None).unwrap();
        assert_eq!(
            (first.incarnation.as_str(), first.history_version),
            ("unwritten", 0)
        );
        assert!(row(&conn, "address").unwrap().is_none());
        let first = version_at(&mut conn, "address", true, Some("abc"), 100, None).unwrap();
        assert_eq!(
            (first.incarnation.as_str(), first.history_version),
            ("abc", 100)
        );
        assert_eq!(
            version_at(&mut conn, "address", true, Some("abc"), 200, None).unwrap(),
            first
        );
        assert_eq!(
            version_at(&mut conn, "address", false, None, 200, None)
                .unwrap()
                .history_version,
            101
        );
        assert_eq!(
            version_at(&mut conn, "address", true, Some("def"), 100, None)
                .unwrap()
                .history_version,
            102
        );
        assert_eq!(
            version_at(&mut conn, "address", true, Some("abc"), 500, None)
                .unwrap()
                .history_version,
            500
        );
    }

    #[test]
    fn rewrite_fences_block_pages_and_reject_a_pre_rewrite_snapshot_after_clear() {
        let mut conn = Connection::open_in_memory().unwrap();
        initialize(&conn).unwrap();
        let session = broca_protocol::BindIdentity::new("/project", "h", "s");
        let address = broca_core::session_addr(&session);
        let before = version_at(&mut conn, &address, true, Some("abc"), 100, None).unwrap();
        rewritten(&mut conn, &session, Some("abc")).unwrap();
        assert_eq!(
            read_version(&conn, &address).unwrap_err(),
            broca_core::served_history::HISTORY_REWRITING
        );
        assert_eq!(rewriting_sessions(&conn).unwrap(), vec![session.clone()]);
        rewrite_finished(&mut conn, &session).unwrap();
        assert!(rewriting_sessions(&conn).unwrap().is_empty());
        assert_eq!(
            version_at(
                &mut conn,
                &address,
                true,
                Some("abc"),
                200,
                Some(Some(before.history_version))
            )
            .unwrap_err(),
            broca_core::served_history::HISTORY_CHANGED
        );
        let after = version_at(&mut conn, &address, true, Some("abc"), 200, None).unwrap();
        assert!(after.history_version > before.history_version);
        assert_eq!(after.incarnation, before.incarnation);
    }

    #[test]
    fn an_unframed_log_keeps_its_incarnation_when_append_adds_a_lineage() {
        let mut conn = Connection::open_in_memory().unwrap();
        initialize(&conn).unwrap();
        let before = version_at(&mut conn, "a", true, None, 40, None).unwrap();
        assert_eq!(before.incarnation.len(), 32);
        let after = version_at(&mut conn, "a", true, Some("abc"), 50, None).unwrap();
        assert_eq!(before, after);
        assert_eq!(
            row(&conn, "a").unwrap().unwrap().lineage.as_deref(),
            Some("abc")
        );
    }
}
