//! Copy-only projection timing; never prints identities or transcript content.
use broca_store::session_refs;
use rusqlite::{Connection, OpenFlags};
use std::{path::PathBuf, time::Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if !matches!(args.len(), 2 | 4)
        || args[0] != "--db"
        || (args.len() == 4 && args[2] != "--census")
    {
        return Err(
            "usage: session-ref-backfill --db <path.copy.db> [--census <read-only-state-root>]"
                .into(),
        );
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
    let mut conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
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
    println!(
        "rows={} distinct_sessions={} refs_inserted={} elapsed_ms={} table_bytes={} index_bytes={}",
        counts.rows,
        counts.distinct_sessions,
        counts.inserted,
        elapsed_ms,
        table_bytes,
        index_bytes
    );
    if args.len() == 4 {
        let wal = broca_core::ReadOnlyWalReader::new(PathBuf::from(&args[3]));
        let counts = session_refs::census(&mut conn, &wal)?;
        println!(
            "addresses={} resumed={} not_represented={} bytes_read={} elapsed_ms={} complete={}",
            counts.addresses,
            counts.resumed,
            counts.not_represented,
            counts.bytes_read,
            counts.elapsed_ms,
            u8::from(counts.complete)
        );
    }
    Ok(())
}
