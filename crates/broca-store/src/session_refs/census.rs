use super::{backfill, batches::write_batch, insert, MigrationWriteStats};
use crate::StoreError;
use broca_core::ReadOnlyWalReader;
use rusqlite::Connection;
use std::time::Instant;

#[derive(Debug, Default)]
pub struct CensusCounts {
    pub addresses: u64,
    pub resumed: u64,
    pub not_represented: u64,
    pub bytes_read: u64,
    pub elapsed_ms: u128,
    pub complete: bool,
    pub writes: MigrationWriteStats,
}

enum Action {
    Begin(String),
    Candidate(String, broca_protocol::BindIdentity),
    Covered(String),
}

fn pace_duration(elapsed: std::time::Duration, enabled: bool) {
    if enabled {
        std::thread::sleep(elapsed.saturating_mul(3));
    }
    std::thread::yield_now();
}

pub(super) fn run(
    conn: &mut Connection,
    wal: &ReadOnlyWalReader,
    limit: Option<usize>,
    paced: bool,
) -> Result<CensusCounts, StoreError> {
    let start = Instant::now();
    let mut counts = CensusCounts::default();
    let done: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_data_census_done)",
        [],
        |row| row.get(0),
    )?;
    if done {
        counts.complete = true;
        return Ok(counts);
    }
    let work = Instant::now();
    let addresses = wal.addresses().map_err(|_| {
        StoreError::InconsistentSnapshot("cannot enumerate WAL addresses for session-ref census")
    })?;
    let discovery_work = work.elapsed();
    let queue: Vec<_> = addresses.iter().collect();
    let mut queued = 0;
    while queued < queue.len() {
        queued += write_batch(
            conn,
            &queue[queued..],
            &mut counts.writes,
            paced,
            |tx, address| {
                tx.execute(
                    "INSERT OR IGNORE INTO agent_data_census(address) VALUES(?1)",
                    [address],
                )?;
                Ok(())
            },
            |_, _| Ok(()),
        )?;
    }
    pace_duration(discovery_work, paced);
    counts.addresses = addresses.len() as u64;
    counts.resumed = conn.query_row(
        "SELECT count(*) FROM agent_data_census WHERE covered=1",
        [],
        |row| row.get(0),
    )?;
    let mut processed = 0;
    loop {
        let work = Instant::now();
        let todo = {
            let mut statement = conn.prepare(
                "SELECT address FROM agent_data_census WHERE covered=0 ORDER BY address LIMIT 128",
            )?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        if todo.is_empty() {
            break;
        }
        let mut actions = Vec::new();
        for address in todo {
            if limit.is_some_and(|limit| processed >= limit) {
                break;
            }
            if addresses.contains(&address) {
                let represented: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM agent_data_session_ref WHERE address=?1)
                    AND NOT EXISTS(SELECT 1 FROM agent_data_census_inflight WHERE address=?1)",
                    [&address],
                    |row| row.get(0),
                )?;
                if represented {
                    counts.bytes_read += wal.probe(&address).map_err(|_| {
                        StoreError::InconsistentSnapshot(
                            "cannot probe represented WAL for session-ref census",
                        )
                    })?;
                } else {
                    counts.not_represented += 1;
                    let (records, bytes) = wal.records(&address).map_err(|_| {
                        StoreError::InconsistentSnapshot(
                            "cannot read uncovered WAL for session-ref census",
                        )
                    })?;
                    counts.bytes_read += bytes;
                    // Persist an in-flight row before candidate insertion. After a
                    // crash, a partial ref row must not make resume skip the rest
                    // of this WAL; Covered removes the row after all candidates.
                    actions.push(Action::Begin(address.clone()));
                    let mut candidates = std::collections::BTreeMap::new();
                    for frame in records {
                        if let broca_wal::WalRecord::RunStarted { session, .. } = frame.record {
                            if broca_core::session_addr(&session) == address {
                                candidates.insert(broca_core::agent_session_ref(&session), session);
                            }
                        }
                    }
                    actions.extend(
                        candidates
                            .into_values()
                            .map(|session| Action::Candidate(address.clone(), session)),
                    );
                }
            }
            actions.push(Action::Covered(address));
            processed += 1;
            std::thread::yield_now();
        }
        let read_work = work.elapsed();
        let mut written = 0;
        while written < actions.len() {
            written += write_batch(
                conn,
                &actions[written..],
                &mut counts.writes,
                paced,
                |tx, action| {
                    match action {
                        Action::Begin(address) => {
                            tx.execute(
                                "INSERT OR IGNORE INTO agent_data_census_inflight VALUES(?1)",
                                [address],
                            )?;
                        }
                        Action::Candidate(address, session) => {
                            insert(tx, address, session)?;
                        }
                        Action::Covered(address) => {
                            tx.execute(
                                "UPDATE agent_data_census SET covered=1 WHERE address=?1",
                                [address],
                            )?;
                            tx.execute(
                                "DELETE FROM agent_data_census_inflight WHERE address=?1",
                                [address],
                            )?;
                        }
                    }
                    Ok(())
                },
                |_, _| Ok(()),
            )?;
        }
        pace_duration(read_work, paced);
        if limit.is_some_and(|limit| processed >= limit) {
            counts.elapsed_ms = start.elapsed().as_millis();
            return Ok(counts);
        }
    }
    write_batch(
        conn,
        &[()],
        &mut counts.writes,
        paced,
        |tx, _| {
            tx.execute("INSERT INTO agent_data_census_done VALUES(1)", [])?;
            Ok(())
        },
        |_, _| Ok(()),
    )?;
    counts.complete = true;
    counts.elapsed_ms = start.elapsed().as_millis();
    tracing::info!(
        addresses = counts.addresses,
        resumed = counts.resumed,
        bytes_read = counts.bytes_read,
        not_represented = counts.not_represented,
        elapsed_ms = counts.elapsed_ms,
        max_write_lock_us = counts.writes.max_lock_us,
        write_transactions = counts.writes.transactions,
        "session-ref census complete"
    );
    Ok(counts)
}

/// A separate SQLite connection lets moves observe an incomplete snapshot while
/// the one-time backfill runs, instead of holding the serving connection's mutex.
pub fn migrate(db: &std::path::Path, wal: &ReadOnlyWalReader) -> Result<CensusCounts, StoreError> {
    let root = db.parent().unwrap_or_else(|| std::path::Path::new("."));
    let incomplete = broca_core::session_projection_incomplete(root).map_err(|_| {
        StoreError::InconsistentSnapshot("cannot inspect session-ref projection incomplete marker")
    })?;
    let repair = if incomplete {
        broca_core::ProjectionRepair::begin(root).map_err(|_| {
            StoreError::InconsistentSnapshot("cannot begin session-ref projection repair")
        })?
    } else {
        None
    };
    let mut conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    crate::busy::configure_writer_connection(&conn)?;
    conn.pragma_update(None, "wal_autocheckpoint", 0)?;
    let mut writes = MigrationWriteStats::default();
    if incomplete {
        write_batch(
            &conn,
            &[()],
            &mut writes,
            true,
            |tx, _| {
                tx.execute("DELETE FROM agent_data_census_done", [])?;
                Ok(())
            },
            |_, _| Ok(()),
        )?;
        let reset = {
            let mut statement = conn.prepare("SELECT address FROM agent_data_census WHERE covered=1 AND NOT EXISTS
                (SELECT 1 FROM agent_data_session_ref WHERE agent_data_session_ref.address=agent_data_census.address)")?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        let mut used = 0;
        while used < reset.len() {
            used += write_batch(
                &conn,
                &reset[used..],
                &mut writes,
                true,
                |tx, address| {
                    tx.execute(
                        "UPDATE agent_data_census SET covered=0 WHERE address=?1",
                        [address],
                    )?;
                    Ok(())
                },
                |_, _| Ok(()),
            )?;
        }
    }
    let backfilled = backfill(&conn)?;
    writes.merge(backfilled.writes);
    let mut counts = run(&mut conn, wal, None, true)?;
    counts.writes.merge(writes);
    if incomplete && counts.complete {
        counts.complete = match repair {
            Some(repair) => repair.finish().map_err(|_| {
                StoreError::InconsistentSnapshot(
                    "cannot durably remove session-ref projection incomplete marker",
                )
            })?,
            None => false,
        };
    }
    if broca_core::session_projection_incomplete(root).map_err(|_| {
        StoreError::InconsistentSnapshot("cannot inspect repaired session-ref projection marker")
    })? {
        counts.complete = false;
    }
    tracing::info!(
        write_transactions = counts.writes.transactions,
        max_write_lock_us = counts.writes.max_lock_us,
        max_batch_items = counts.writes.max_batch_items,
        complete = counts.complete,
        "session-ref migration finished"
    );
    Ok(counts)
}

pub fn census(conn: &mut Connection, wal: &ReadOnlyWalReader) -> Result<CensusCounts, StoreError> {
    run(conn, wal, None, true)
}
