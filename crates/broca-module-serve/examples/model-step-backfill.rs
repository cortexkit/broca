//! Runs the model step fact backfill once against a COPY of a broca state root and reports
//! what it did, for measuring the first start of a build that writes `kind: "model_step"`.
//!
//! Point it at an APFS clone or a `VACUUM INTO` copy, never the live state root: it writes
//! facts, checkpoints and walk state into that copy's `run-index.db`.
//!
//! ```text
//! cargo run --release -p broca-module-serve --example model-step-backfill -- <state-root> [fact.json]
//! ```
//!
//! With a second argument it also writes one stored model step fact to that path, its
//! `segment_json` bytes exactly as stored: the newest fact that has every optional field the
//! writer can fill (attempt digest, timing, wire family, cache policy).

use broca_core::FileWalStore;
use broca_module_serve::model_step_facts::{now_ms, Walk};
use broca_store::SqliteStore;
use std::time::{Duration, Instant};

fn main() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let root = std::path::PathBuf::from(args.next().ok_or("usage: <state-root> [fact.json]")?);
    let golden = args.next();
    let opened = Instant::now();
    let store = SqliteStore::open(root.join("run-index.db")).map_err(|e| e.to_string())?;
    let wal = FileWalStore::new(root.join("wal"));
    println!("opened store in {} ms", opened.elapsed().as_millis());

    let now = now_ms();
    let began = Instant::now();
    let mut walk = Walk::begin(&store, now)?;
    let mut slices = 0u64;
    let mut longest_slice = Duration::ZERO;
    loop {
        let slice = Instant::now();
        let done = walk.slice(&store, &wal, Duration::from_millis(100), now);
        longest_slice = longest_slice.max(slice.elapsed());
        slices += 1;
        if done {
            break;
        }
    }
    println!(
        "walk: sessions={} visited={} written={} older_than_30_days={} failed={} slices={} \
         longest_slice_ms={} elapsed_ms={}",
        walk.total,
        walk.visited,
        walk.written,
        walk.expired,
        walk.failed,
        slices,
        longest_slice.as_millis(),
        began.elapsed().as_millis()
    );

    let raw = rusqlite_count(&root)?;
    println!("export_facts by kind: {raw}");

    if let Some(path) = golden {
        let conn =
            rusqlite::Connection::open(root.join("run-index.db")).map_err(|e| e.to_string())?;
        let json: String = conn
            .query_row(
                "SELECT segment_json FROM export_facts
                  WHERE json_extract(segment_json, '$.kind')='model_step'
                    AND json_extract(segment_json, '$.request_body_sha256') IS NOT NULL
                    AND json_extract(segment_json, '$.first_content_at_ms') IS NOT NULL
                    AND json_extract(segment_json, '$.cache_policy.ttl') IS NOT NULL
                    AND json_extract(segment_json, '$.context_limit') IS NOT NULL
                  ORDER BY export_seq DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("no complete model step fact to capture: {e}"))?;
        std::fs::write(&path, format!("{json}\n")).map_err(|e| e.to_string())?;
        println!("wrote {path}");
    }
    Ok(())
}

fn rusqlite_count(root: &std::path::Path) -> Result<String, String> {
    let conn = rusqlite::Connection::open(root.join("run-index.db")).map_err(|e| e.to_string())?;
    let mut statement = conn
        .prepare(
            "SELECT COALESCE(json_extract(segment_json, '$.kind'), 'session'), COUNT(*)
               FROM export_facts GROUP BY 1 ORDER BY 1",
        )
        .map_err(|e| e.to_string())?;
    let rows = statement
        .query_map([], |row| {
            Ok(format!(
                "{}={}",
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?
            ))
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(rows.join(" "))
}
