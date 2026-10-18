//! Copy-only projection timing; never prints identities or transcript content.
use broca_store::session_refs;
use rusqlite::{Connection, OpenFlags};
use std::{path::PathBuf, time::Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() < 2 || args[0] != "--db" {
        return Err("usage: session-ref-backfill --db <path.copy.db> [--reset] [--census <read-only-state-root>]".into());
    }
    let path = PathBuf::from(&args[1]);
    if !path
        .file_name()
        .is_some_and(|name| name.to_string_lossy().ends_with(".copy.db"))
    {
        return Err("refusing database path: filename must end in .copy.db".into());
    }
    if std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
        return Err("refusing symlink database path".into());
    }
    let mut reset = false;
    let mut census_root = None;
    let mut index = 2;
    while index < args.len() {
        match args[index].to_str() {
            Some("--reset") if !reset => {
                reset = true;
                index += 1;
            }
            Some("--census") if census_root.is_none() && index + 1 < args.len() => {
                census_root = Some(PathBuf::from(&args[index + 1]));
                index += 2;
            }
            _ => return Err("expected [--reset] [--census <read-only-state-root>]".into()),
        }
    }
    let mut conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")?;
    if reset {
        // Benchmark setup on the copy only, excluded from migration lock timing.
        conn.execute_batch("DROP TRIGGER IF EXISTS agent_data_run_insert; DROP TRIGGER IF EXISTS agent_data_run_update;
            DROP TABLE IF EXISTS agent_data_session_ref; DROP TABLE IF EXISTS agent_data_projection;
            DROP TABLE IF EXISTS agent_data_census; DROP TABLE IF EXISTS agent_data_census_done;
            DROP TABLE IF EXISTS agent_data_pending_session; DROP TABLE IF EXISTS agent_data_writer;
            DROP TABLE IF EXISTS agent_data_backfill_cursor; DROP TABLE IF EXISTS agent_data_census_inflight;")?;
    }
    let started = Instant::now();
    let counts = session_refs::backfill(&conn)?;
    let elapsed_ms = started.elapsed().as_millis();
    let table_bytes: u64 = conn.query_row(
        "SELECT coalesce(sum(pgsize),0) FROM dbstat WHERE name='agent_data_session_ref'",
        [],
        |row| row.get(0),
    )?;
    let index_bytes: u64 = conn.query_row(
        "SELECT coalesce(sum(pgsize),0) FROM dbstat WHERE name='agent_data_session_address'",
        [],
        |row| row.get(0),
    )?;
    println!("rows={} distinct_sessions={} refs_inserted={} elapsed_ms={} table_bytes={} index_bytes={} write_transactions={} max_write_lock_us={} max_batch_items={}",
        counts.rows,counts.distinct_sessions,counts.inserted,elapsed_ms,table_bytes,index_bytes,
        counts.writes.transactions,counts.writes.max_lock_us,counts.writes.max_batch_items);
    if let Some(root) = census_root {
        let wal = broca_core::ReadOnlyWalReader::new(root);
        let counts = session_refs::census(&mut conn, &wal)?;
        println!("addresses={} resumed={} not_represented={} bytes_read={} elapsed_ms={} complete={} write_transactions={} max_write_lock_us={} max_batch_items={}",
            counts.addresses,counts.resumed,counts.not_represented,counts.bytes_read,counts.elapsed_ms,u8::from(counts.complete),
            counts.writes.transactions,counts.writes.max_lock_us,counts.writes.max_batch_items);
    }
    Ok(())
}
