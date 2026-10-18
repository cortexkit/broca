use super::{backfill, insert};
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
}

fn pace(started: Instant, enabled: bool) {
    if enabled {
        std::thread::sleep(started.elapsed().saturating_mul(3));
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
    // The queue is durable in small batches. Re-enumeration after a crash only
    // adds addresses; covered payloads are never replayed again.
    for batch in addresses.iter().collect::<Vec<_>>().chunks(128) {
        let tx = conn.transaction()?;
        for address in batch {
            tx.execute(
                "INSERT OR IGNORE INTO agent_data_census(address) VALUES(?1)",
                [address],
            )?;
        }
        tx.commit()?;
    }
    pace(work, paced);
    counts.addresses = addresses.len() as u64;
    counts.resumed = conn.query_row(
        "SELECT count(*) FROM agent_data_census WHERE covered=1",
        [],
        |row| row.get(0),
    )?;
    let mut processed = 0;
    loop {
        let work = Instant::now();
        let mut todo = Vec::new();
        {
            let mut statement = conn.prepare(
                "SELECT address FROM agent_data_census WHERE covered=0 ORDER BY address LIMIT 128",
            )?;
            for row in statement.query_map([], |row| row.get::<_, String>(0))? {
                todo.push(row?);
            }
        }
        if todo.is_empty() {
            break;
        }
        let mut completed = Vec::new();
        let mut candidates = Vec::new();
        for address in todo {
            if limit.is_some_and(|limit| processed >= limit) {
                break;
            }
            // A completed enumeration can retire an address left in a prior
            // attempt's queue. A disappearance after enumeration is instead an
            // error from the reader, so that race never certifies missing refs.
            if !addresses.contains(&address) {
                completed.push(address);
                processed += 1;
                continue;
            }
            let represented: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM agent_data_session_ref WHERE address=?1)",
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
                for frame in records {
                    if let broca_wal::WalRecord::RunStarted { session, .. } = frame.record {
                        candidates.push((address.clone(), session));
                    }
                }
            }
            completed.push(address);
            processed += 1;
            std::thread::yield_now();
        }
        let tx = conn.transaction()?;
        for (address, session) in candidates {
            insert(&tx, &address, &session)?;
        }
        for address in &completed {
            tx.execute(
                "UPDATE agent_data_census SET covered=1 WHERE address=?1",
                [address],
            )?;
        }
        tx.commit()?;
        // Include reads, decoding and the SQLite durability barrier in pacing.
        pace(work, paced);
        if limit.is_some_and(|limit| processed >= limit) {
            counts.elapsed_ms = start.elapsed().as_millis();
            return Ok(counts);
        }
    }
    let work = Instant::now();
    conn.execute("INSERT INTO agent_data_census_done VALUES(1)", [])?;
    pace(work, paced);
    counts.complete = true;
    counts.elapsed_ms = start.elapsed().as_millis();
    tracing::info!(
        addresses = counts.addresses,
        resumed = counts.resumed,
        bytes_read = counts.bytes_read,
        not_represented = counts.not_represented,
        elapsed_ms = counts.elapsed_ms,
        "session-ref census complete"
    );
    Ok(counts)
}

/// A separate SQLite connection lets moves observe an incomplete snapshot while
/// the one-time backfill runs, instead of holding the serving connection's mutex.
pub fn migrate(db: &std::path::Path, wal: &ReadOnlyWalReader) -> Result<CensusCounts, StoreError> {
    let mut conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    let work = Instant::now();
    backfill(&conn)?;
    pace(work, true);
    run(&mut conn, wal, None, true)
}

pub fn census(conn: &mut Connection, wal: &ReadOnlyWalReader) -> Result<CensusCounts, StoreError> {
    run(conn, wal, None, true)
}
