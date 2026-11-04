//! `session-convert measure <path>...`: converts every session log under the given files or
//! folders in memory, writes nothing, and reports what the new format would store, how long
//! each log takes to load, and what its history would cost sealed.
//!
//! `session-convert append --dir <scratch> [--sample N] <path>...`: replays a sample of logs
//! through real barrier writes into files under `--dir`, then removes them, and reports barrier
//! latency.
//!
//! Point it only at a copy of a state root (an APFS clone, `cp -c`), never at live files: it opens
//! each log read-only, but a measurement run should never share a disk path with the daemon.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use broca_session_convert::measure::{append_log, measure};
use broca_session_convert::{convert, Tally};
use broca_session_store::codec::Digest;
use broca_session_store::frame::{FrameKey, FRAME_OVERHEAD};
use serde::Serialize;

/// Measurements encrypt with a fixed throwaway key: only the cost matters, not secrecy.
fn measurement_key() -> FrameKey {
    FrameKey::new(1, &[7; 32])
}

#[derive(Default, Serialize)]
struct KindTotal {
    count: u64,
    body_bytes: u64,
}

#[derive(Default, Serialize)]
struct ClassTotal {
    logs: u64,
    old_file_bytes: u64,
    new_bytes: u64,
}

#[derive(Default, Serialize)]
struct Report {
    logs: u64,
    failed_logs: u64,
    old_file_bytes: u64,
    old_records: u64,
    old_by_type: BTreeMap<String, Tally>,
    new_records: u64,
    new_body_bytes: u64,
    /// The logs framed and encrypted: bodies plus `FRAME_OVERHEAD` bytes per record.
    new_framed_bytes: u64,
    /// History sealed: zstd in 16 KiB blocks, encrypted, both streams.
    sealed_bytes: u64,
    /// The same with opaque reasoning, signatures and provider text state pruned from runs that
    /// ended.
    sealed_pruned_bytes: u64,
    pruned_opaque_bytes: u64,
    /// Loading a whole log: decrypt and decode every frame, from memory.
    load_us_per_log: Percentiles,
    load_us_total: u64,
    new_by_kind: BTreeMap<String, KindTotal>,
    new_by_stream: BTreeMap<String, KindTotal>,
    /// Setup parts across every log, each stored once.
    parts_distinct: u64,
    parts_distinct_bytes: u64,
    /// The same parts as today's run starts carry them, once per run.
    parts_bytes_per_run: u64,
    runs: u64,
    /// Old fields the conversion didn't place: misspelt or unhandled names, wrong types and
    /// missing required fields. A clean conversion leaves this empty.
    lost: BTreeMap<String, Tally>,
    dropped: BTreeMap<String, Tally>,
    by_class: BTreeMap<String, ClassTotal>,
    /// Per-log framed size, without setup parts, in bytes.
    new_bytes_per_log: Percentiles,
    read_ms: u128,
    convert_ms: u128,
    encode_ms: u128,
    errors: Vec<(String, String)>,
}

#[derive(Default, Serialize)]
struct Percentiles {
    p50: u64,
    p90: u64,
    p99: u64,
    max: u64,
}

fn percentiles(mut values: Vec<u64>) -> Percentiles {
    if values.is_empty() {
        return Percentiles::default();
    }
    values.sort_unstable();
    let at = |q: f64| values[((values.len() - 1) as f64 * q).round() as usize];
    Percentiles {
        p50: at(0.5),
        p90: at(0.9),
        p99: at(0.99),
        max: *values.last().unwrap(),
    }
}

fn collect(path: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let child = entry.path();
            if child.is_file() && child.extension().is_some_and(|e| e == "wal") {
                out.push(child);
            }
        }
    } else {
        out.push(path.to_path_buf());
    }
    Ok(())
}

fn usage() -> ! {
    eprintln!(
        "usage: session-convert measure [--out FILE] [--limit N] PATH...\n       \
         session-convert append --dir SCRATCH [--sample N] [--out FILE] PATH..."
    );
    std::process::exit(64);
}

struct Args {
    command: String,
    out_file: Option<PathBuf>,
    limit: usize,
    sample: usize,
    dir: Option<PathBuf>,
    files: Vec<PathBuf>,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_else(|| usage());
    let mut parsed = Args {
        command,
        out_file: None,
        limit: usize::MAX,
        sample: 40,
        dir: None,
        files: Vec::new(),
    };
    let mut paths = Vec::new();
    while let Some(arg) = args.next() {
        let mut number = || {
            args.next()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| usage())
        };
        match arg.as_str() {
            "--out" => {
                parsed.out_file = Some(PathBuf::from(args.next().unwrap_or_else(|| usage())))
            }
            "--limit" => parsed.limit = number(),
            "--sample" => parsed.sample = number(),
            "--dir" => parsed.dir = Some(PathBuf::from(args.next().unwrap_or_else(|| usage()))),
            _ => paths.push(PathBuf::from(arg)),
        }
    }
    for path in &paths {
        if let Err(e) = collect(path, &mut parsed.files) {
            eprintln!("{}: {e}", path.display());
            std::process::exit(1);
        }
    }
    parsed.files.sort();
    parsed.files.truncate(parsed.limit);
    if parsed.files.is_empty() {
        // A measurement over nothing must not look like a measurement of nothing to store.
        eprintln!("no .wal files found under the given paths");
        std::process::exit(2);
    }
    parsed
}

fn write_report<T: Serialize>(report: &T, out_file: Option<&Path>) {
    let json = serde_json::to_string_pretty(report).expect("report serializes");
    match out_file {
        Some(path) => std::fs::write(path, json).unwrap_or_else(|e| {
            eprintln!("{}: {e}", path.display());
            std::process::exit(1);
        }),
        None => println!("{json}"),
    }
}

fn main() {
    let args = parse_args();
    match args.command.as_str() {
        "measure" => run_measure(&args),
        "append" => run_append(&args),
        _ => usage(),
    }
}

fn run_measure(args: &Args) {
    let files = &args.files;
    let key = measurement_key();
    let mut report = Report::default();
    let mut parts: BTreeMap<Digest, u64> = BTreeMap::new();
    let mut per_log = Vec::with_capacity(files.len());
    let mut load_us = Vec::with_capacity(files.len());
    for (index, file) in files.iter().enumerate() {
        if index % 1000 == 0 {
            eprintln!("{index}/{}", files.len());
        }
        let size = std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
        let started = Instant::now();
        let old = match broca_wal::replay_file(file, u64::MAX) {
            Ok(old) => old,
            Err(e) => {
                report.failed_logs += 1;
                report
                    .errors
                    .push((file.display().to_string(), format!("read: {e}")));
                continue;
            }
        };
        report.read_ms += started.elapsed().as_millis();
        for framed in &old {
            let bytes = serde_json::to_vec(&framed.record).map_or(0, |v| v.len());
            let kind = serde_json::to_value(&framed.record)
                .ok()
                .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(str::to_owned))
                .unwrap_or_else(|| "unknown".into());
            let tally = report.old_by_type.entry(kind).or_default();
            tally.count += 1;
            tally.json_bytes += bytes as u64;
        }

        let started = Instant::now();
        let converted = match convert(&old) {
            Ok(converted) => converted,
            Err(e) => {
                report.failed_logs += 1;
                report
                    .errors
                    .push((file.display().to_string(), format!("convert: {e}")));
                continue;
            }
        };
        report.convert_ms += started.elapsed().as_millis();

        let started = Instant::now();
        for record in &converted.records {
            let body = record.encode_body().len() as u64;
            let kind = report.new_by_kind.entry(record.name().into()).or_default();
            kind.count += 1;
            kind.body_bytes += body;
            let stream = report
                .new_by_stream
                .entry(format!("{:?}", record.stream()))
                .or_default();
            stream.count += 1;
            stream.body_bytes += body;
            report.new_body_bytes += body;
        }
        report.encode_ms += started.elapsed().as_millis();

        let bind = file
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let measured = match measure(&converted, &key, bind.as_bytes()) {
            Ok(measured) => measured,
            Err(e) => {
                report.failed_logs += 1;
                report
                    .errors
                    .push((file.display().to_string(), format!("measure: {e}")));
                continue;
            }
        };
        report.new_framed_bytes += measured.frame_bytes;
        report.sealed_bytes += measured.sealed_bytes;
        report.sealed_pruned_bytes += measured.sealed_pruned_bytes;
        report.pruned_opaque_bytes += measured.pruned_opaque_bytes;
        report.load_us_total += measured.load_us;
        load_us.push(measured.load_us);

        report.logs += 1;
        report.old_file_bytes += size;
        report.old_records += old.len() as u64;
        report.new_records += converted.records.len() as u64;
        report.parts_bytes_per_run += converted.part_bytes_per_run;
        report.runs += converted.runs;
        for (digest, bytes) in &converted.parts {
            parts.insert(*digest, bytes.len() as u64);
        }
        for (name, tally) in converted.lost {
            let total = report.lost.entry(name).or_default();
            total.count += tally.count;
            total.json_bytes += tally.json_bytes;
        }
        for (name, tally) in converted.dropped {
            let total = report.dropped.entry(name).or_default();
            total.count += tally.count;
            total.json_bytes += tally.json_bytes;
        }
        let class = report
            .by_class
            .entry(converted.session_class.unwrap_or_else(|| "none".into()))
            .or_default();
        class.logs += 1;
        class.old_file_bytes += size;
        class.new_bytes += measured.frame_bytes;
        per_log.push(measured.frame_bytes);
    }
    debug_assert_eq!(
        report.new_framed_bytes,
        report.new_body_bytes + report.new_records * FRAME_OVERHEAD as u64
    );
    report.parts_distinct = parts.len() as u64;
    report.parts_distinct_bytes = parts.values().sum();
    report.new_bytes_per_log = percentiles(per_log);
    report.load_us_per_log = percentiles(load_us);
    write_report(&report, args.out_file.as_deref());
}

#[derive(Default, Serialize)]
struct AppendReport {
    logs: u64,
    records: u64,
    barriers: u64,
    bytes: u64,
    /// Write plus device sync per barrier, in microseconds.
    barrier_us: Percentiles,
    /// Per log: total append time against today's file size.
    per_log: Vec<(u64, u64, u64, f64)>,
}

fn run_append(args: &Args) {
    let dir = args.dir.clone().unwrap_or_else(|| usage());
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| {
        eprintln!("{}: {e}", dir.display());
        std::process::exit(1);
    });
    // An even spread over sizes, plus the five largest logs, so both typical and worst-case
    // sessions are timed.
    let mut by_size: Vec<(u64, &PathBuf)> = args
        .files
        .iter()
        .map(|f| (std::fs::metadata(f).map(|m| m.len()).unwrap_or(0), f))
        .collect();
    by_size.sort();
    let mut chosen: Vec<&PathBuf> = Vec::new();
    let step = (by_size.len() / args.sample.max(1)).max(1);
    chosen.extend(by_size.iter().step_by(step).map(|(_, f)| *f));
    chosen.extend(by_size.iter().rev().take(5).map(|(_, f)| *f));
    chosen.dedup();

    let key = measurement_key();
    let mut report = AppendReport::default();
    let mut latencies = Vec::new();
    for (index, file) in chosen.iter().enumerate() {
        let old = match broca_wal::replay_file(file, u64::MAX) {
            Ok(old) => old,
            Err(e) => {
                eprintln!("{}: {e}", file.display());
                continue;
            }
        };
        let Ok(converted) = convert(&old) else {
            continue;
        };
        let target = dir.join(format!("append-{index}.log"));
        let bind = format!("{index}");
        match append_log(&converted.records, &key, bind.as_bytes(), &target) {
            Ok(measured) => {
                let size = std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
                report.logs += 1;
                report.records += converted.records.len() as u64;
                report.barriers += measured.barriers;
                report.bytes += measured.bytes;
                report.per_log.push((
                    size,
                    converted.records.len() as u64,
                    measured.barriers,
                    measured.total_ms,
                ));
                latencies.extend(measured.barrier_us);
            }
            Err(e) => eprintln!("{}: {e}", file.display()),
        }
        let _ = std::fs::remove_file(&target);
    }
    report.barrier_us = percentiles(latencies);
    write_report(&report, args.out_file.as_deref());
}
