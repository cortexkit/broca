//! Paths may name session logs (`.wal`), WAL archive containers (`.ark`), or folders of either.
//! An archived log is written to a scratch file under `--dir` (or the system temporary folder)
//! and read back with the ordinary log reader.
//!
//! `session-convert measure <path>...`: converts every session log in memory, writes nothing,
//! and reports what the new format would store, and how long each log takes to load. It also
//! reports the size of each log *sealed*: its records compressed with zstd in 16 KiB blocks and
//! encrypted, which is how the new format stores history no longer being appended to.
//!
//! `session-convert check <path>...`: converts every session log, rebuilds today's records from
//! the new ones, and compares them with the originals. A log passes when every record comes back
//! exactly; differences are reported by record type and key path, never by value.
//!
//! `session-convert append --dir <scratch> [--sample N] <path>...`: replays a sample of logs
//! through real barrier writes into files under `--dir`, then removes them, and reports barrier
//! latency.
//!
//! `session-convert split <path>...`: converts every session log and checks that loading from a
//! checkpoint gives the state folding the whole log gives: at every split point of a short log,
//! and at its ends and 8 points between them for a longer one.
//!
//! Point it only at a copy of a state root (an APFS clone, `cp -c`), never at live files: it opens
//! each log read-only, but a measurement run should never share a disk path with the daemon.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use broca_session_convert::inverse::{comparable, first_difference, invert, without_nulls};
use broca_session_convert::measure::{append_log, measure};
use broca_session_convert::split::{check_splits, split_points};
use broca_session_convert::{convert, Tally};
use broca_session_store::codec::Digest;
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::frame::{FrameKey, FRAME_OVERHEAD};
use broca_session_store::record::{CancelPoint, Stream};
use broca_wal::FramedRecord;
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

/// Session logs (`.wal`) and archive containers (`.ark`) under `path`, or `path` itself.
fn collect(path: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let child = entry.path();
            if child.is_file() && child.extension().is_some_and(|e| e == "wal" || e == "ark") {
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
        "usage: session-convert measure [--dir SCRATCH] [--out FILE] [--limit N] PATH...\n       \
         session-convert check [--dir SCRATCH] [--out FILE] [--limit N] PATH...\n       \
         session-convert append --dir SCRATCH [--sample N] [--out FILE] PATH...\n       \
         session-convert split [--dir SCRATCH] [--out FILE] [--limit N] PATH..."
    );
    std::process::exit(64);
}

struct Args {
    command: String,
    out_file: Option<PathBuf>,
    limit: usize,
    sample: usize,
    /// Visit only the archived log at this session address.
    only: Option<String>,
    /// `ops`: compare at every prefix of each log, not only at its end.
    prefixes: bool,
    /// `ops --prefixes`: skip logs with more records than this, since each prefix is replayed
    /// from the start.
    max_records: Option<usize>,
    /// Visit only every Nth log.
    every: usize,
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
        only: None,
        prefixes: false,
        max_records: None,
        every: 1,
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
            "--prefixes" => parsed.prefixes = true,
            "--max-records" => parsed.max_records = Some(number()),
            "--every" => parsed.every = number().max(1),
            "--only" => parsed.only = Some(args.next().unwrap_or_else(|| usage())),
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
        eprintln!("no .wal or .ark files found under the given paths");
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
        "check" => run_check(&args),
        "prompt" => run_prompt(&args),
        "trace" => run_trace(&args),
        "ops" => run_ops(&args),
        "measure" => run_measure(&args),
        "append" => run_append(&args),
        "split" => run_split(&args),
        _ => usage(),
    }
}

#[derive(Default, Serialize)]
struct CheckReport {
    logs: u64,
    /// Logs whose every record rebuilds exactly from the new records.
    exact: u64,
    records: u64,
    /// The first difference of each mismatching log, by old record type and path. Paths name
    /// keys only, never values.
    mismatches: BTreeMap<String, u64>,
    /// Logs that failed to read, convert or rebuild, with the reason.
    failures: BTreeMap<String, u64>,
}

/// Converts each log, rebuilds today's records from the new ones, and compares them with the
/// original, record by record: the proof that a conversion keeps everything.
fn run_check(args: &Args) {
    let mut report = CheckReport::default();
    each_log(args, |item| {
        let Item::Log { old, .. } = item else {
            return;
        };
        report.logs += 1;
        let old = match old {
            Ok(old) => old,
            Err(e) => {
                *report.failures.entry(e).or_default() += 1;
                return;
            }
        };
        let converted = match convert(&old) {
            Ok(converted) => converted,
            Err(e) => {
                *report.failures.entry(format!("convert: {e}")).or_default() += 1;
                return;
            }
        };
        let rebuilt = match invert(&converted.records, &converted.parts) {
            Ok(rebuilt) => rebuilt,
            Err(e) => {
                *report.failures.entry(format!("rebuild: {e}")).or_default() += 1;
                return;
            }
        };
        let original = comparable(&old);
        report.records += original.len() as u64;
        if original.len() != rebuilt.len() {
            let count = format!("record count {} against {}", original.len(), rebuilt.len());
            *report.mismatches.entry(count).or_default() += 1;
            return;
        }
        let difference = original.iter().zip(&rebuilt).find_map(|(a, b)| {
            let b = without_nulls(b);
            first_difference(a, &b, "").map(|path| {
                let kind = a.get("type").and_then(|t| t.as_str()).unwrap_or("?");
                format!("{kind}{path}")
            })
        });
        match difference {
            None => report.exact += 1,
            Some(path) => *report.mismatches.entry(path).or_default() += 1,
        }
    });
    write_report(&report, args.out_file.as_deref());
}

#[derive(Default, Serialize)]
struct PromptReport {
    logs: u64,
    /// Logs whose rebuilt conversation equals today's replayed prompt, message for message.
    equal: u64,
    messages: u64,
    /// Logs today's replay refuses; they have no prompt to compare against.
    replay_refused: BTreeMap<String, u64>,
    /// Logs the new conversation fold refuses, by reason.
    fold_refused: BTreeMap<String, u64>,
    /// The first difference of each mismatching log, by field and roles. Never message content.
    mismatches: BTreeMap<String, u64>,
    /// A few mismatching logs per category, by name, to look at by hand.
    examples: BTreeMap<String, Vec<String>>,
}

/// Where two prompts first differ, named by position, field and role only.
fn prompt_difference(
    today: &[broca_protocol::content::Message],
    rebuilt: &[broca_protocol::content::Message],
) -> Option<String> {
    for (index, (a, b)) in today.iter().zip(rebuilt).enumerate() {
        let at = if index + 1 == today.len().min(rebuilt.len()) {
            "last common message"
        } else {
            "message"
        };
        let field = if a.role != b.role {
            "role"
        } else if a.content != b.content {
            if a.content.len() != b.content.len() {
                "block count"
            } else {
                "content"
            }
        } else if a.turn_system != b.turn_system {
            "turn_system"
        } else if a.cache_prefix_blocks != b.cache_prefix_blocks {
            "cache_prefix_blocks"
        } else if a.origin != b.origin {
            "origin"
        } else {
            continue;
        };
        return Some(format!(
            "{at}: {field} (today {:?}, rebuilt {:?})",
            a.role, b.role
        ));
    }
    (today.len() != rebuilt.len()).then(|| {
        let (longer, role) = if today.len() > rebuilt.len() {
            ("today longer", today[rebuilt.len()].role)
        } else {
            ("rebuilt longer", rebuilt[today.len()].role)
        };
        format!("length: {longer}, next is {role:?}")
    })
}

/// Rebuilds each log's conversation from its converted transcript records with the new fold,
/// and compares it with the prompt today's replay builds from the original log.
fn run_prompt(args: &Args) {
    let mut report = PromptReport::default();
    each_log(args, |item| {
        let Item::Log {
            name, old: Ok(old), ..
        } = item
        else {
            return;
        };
        report.logs += 1;
        let records: Vec<broca_wal::WalRecord> = old.iter().map(|f| f.record.clone()).collect();
        let today = match broca_core::replay::replay(&records) {
            Ok(state) => state.prompt,
            Err(e) => {
                let reason: String = e.to_string().chars().take(80).collect();
                *report.replay_refused.entry(reason).or_default() += 1;
                return;
            }
        };
        let Ok(converted) = convert(&old) else {
            *report.fold_refused.entry("convert".into()).or_default() += 1;
            return;
        };
        let mode3 = broca_core::is_mode3_log(&records);
        let mut conversation = Conversation::new(!mode3);
        let folded = converted
            .records
            .iter()
            .filter(|r| r.stream() == Stream::Transcript)
            .try_for_each(|r| conversation.apply(r));
        if let Err(e) = folded {
            let reason = format!("{e:?}").chars().take(60).collect::<String>();
            let key = reason.split('(').next().unwrap_or_default().to_owned();
            *report.fold_refused.entry(key.clone()).or_default() += 1;
            let examples = report.examples.entry(format!("fold: {key}")).or_default();
            if examples.len() < 5 {
                examples.push(name);
            }
            return;
        }
        report.messages += today.len() as u64;
        match prompt_difference(&today, &conversation.messages) {
            None => report.equal += 1,
            Some(category) => {
                *report.mismatches.entry(category.clone()).or_default() += 1;
                let examples = report.examples.entry(category).or_default();
                if examples.len() < 5 {
                    examples.push(name);
                }
            }
        }
    });
    write_report(&report, args.out_file.as_deref());
}

/// Prints, for one log, its old record types in order and the roles of today's prompt beside
/// the rebuilt conversation's. Types and roles only, never content, so a mismatch can be
/// located without reading the session.
fn run_trace(args: &Args) {
    each_log(args, |item| {
        let Item::Log {
            name, old: Ok(old), ..
        } = item
        else {
            return;
        };
        println!("== {name}");
        for framed in &old {
            let kind = serde_json::to_value(&framed.record)
                .ok()
                .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(str::to_owned))
                .unwrap_or_default();
            println!("  {:>5} {kind}", framed.seq);
        }
        let records: Vec<broca_wal::WalRecord> = old.iter().map(|f| f.record.clone()).collect();
        let today = broca_core::replay::replay(&records)
            .map(|s| s.prompt)
            .unwrap_or_default();
        let mut conversation = Conversation::new(!broca_core::is_mode3_log(&records));
        if let Ok(converted) = convert(&old) {
            for record in converted
                .records
                .iter()
                .filter(|r| r.stream() == Stream::Transcript)
            {
                if let Err(e) = conversation.apply(record) {
                    println!("  fold refused: {e:?}");
                    break;
                }
            }
        }
        let roles = |m: &[broca_protocol::content::Message]| -> Vec<String> {
            m.iter().map(|m| format!("{:?}", m.role)).collect()
        };
        println!("  today:   {}", roles(&today).join(" "));
        println!("  rebuilt: {}", roles(&conversation.messages).join(" "));
    });
}

#[derive(Default, Serialize)]
struct OpsReport {
    logs: u64,
    /// Logs left out by `--every` or `--max-records`.
    not_visited: u64,
    /// States compared: one per log, or one per prefix with `--prefixes`.
    compared: u64,
    /// Logs whose folded run state equals today's replay on every compared field, at every
    /// compared state.
    equal: u64,
    /// States today's replay refuses, so there is nothing to compare against.
    replay_refused: BTreeMap<String, u64>,
    /// Mismatching fields, counted once per log for each field that differs.
    mismatches: BTreeMap<String, u64>,
    examples: BTreeMap<String, Vec<String>>,
    /// The largest size each growing field of the operational state reached, over every
    /// compared state: the measured side of the bounds the fold declares.
    largest: BTreeMap<&'static str, u64>,
    /// For each compared field that is usually empty, how many compared states had a value on
    /// today's side. A field that is never set anywhere compares nothing.
    set_on_today: BTreeMap<&'static str, u64>,
}

/// Serializing values of different types through one closure.
mod erased {
    pub trait Serialize {
        fn to_json(&self) -> serde_json::Value;
    }
    impl<T: serde::Serialize> Serialize for T {
        fn to_json(&self) -> serde_json::Value {
            serde_json::to_value(self).expect("a replay value encodes")
        }
    }
}

/// Today's resolution in the operational fold's terms.
fn today_resolution(
    resolution: &broca_core::replay::ReplayResolution,
) -> broca_session_store::fold::operations::Resolution {
    use broca_core::replay::ReplayResolution as Today;
    use broca_session_store::fold::operations::Resolution;
    let sorted = |ids: &[broca_protocol::ids::ToolCallId]| {
        let mut ids = ids.to_vec();
        ids.sort_by(|a, b| a.0.cmp(&b.0));
        ids
    };
    match resolution {
        Today::LineageIdle { last_reason } => Resolution::Idle(*last_reason),
        Today::Interrupted { indeterminate } => Resolution::Interrupted(sorted(indeterminate)),
        Today::Resume { .. } => Resolution::Resume,
        Today::ProviderHeld { open_held, .. } => Resolution::ProviderHeld(sorted(open_held)),
        Today::AuthRequired { reason, .. } => Resolution::Paused(*reason),
    }
}

enum OpsOutcome {
    ReplayRefused(String),
    FoldRefused(String),
    /// Neither today's replay nor the fold has a run.
    NoRun,
    /// The fields that differ; empty when the two agree.
    /// The fields that differ (empty when the two agree), and the sizes of the fold's growing
    /// fields at this state.
    Compared(Vec<String>, Vec<(&'static str, u64)>, Vec<&'static str>),
}

/// Folds one log's converted ledger with the new operational fold, and compares the latest
/// run's state with what today's replay derives from the same records.
fn compare_ops(old: &[FramedRecord]) -> OpsOutcome {
    use broca_session_store::fold::operations::{CallState, Operations, Resolution};
    let records: Vec<broca_wal::WalRecord> = old.iter().map(|f| f.record.clone()).collect();
    let today = match broca_core::replay::replay(&records) {
        Ok(state) => state,
        Err(e) => return OpsOutcome::ReplayRefused(e.to_string().chars().take(80).collect()),
    };
    let Ok(converted) = convert(old) else {
        return OpsOutcome::FoldRefused("convert".into());
    };
    let mut operations = Operations::default();
    let folded = converted
        .records
        .iter()
        .filter(|r| r.stream() == Stream::Ledger)
        .try_for_each(|r| operations.apply(r));
    if let Err(e) = folded {
        return OpsOutcome::FoldRefused(format!("{e:?}"));
    }
    let refusal_schedule = operations.refusal_schedule.clone();
    let folded_queue: Vec<String> = operations
        .queue
        .iter()
        .map(|pending| pending.submission_id.clone())
        .collect();
    let today_queue: Vec<String> = broca_core::queue::pending_submissions(&records)
        .into_iter()
        .map(|pending| pending.submission_id.0)
        .collect();
    let Some(run) = operations.run else {
        // Nothing to compare only when today's replay has no run either.
        return if today.run_id.is_none() {
            OpsOutcome::NoRun
        } else {
            OpsOutcome::Compared(
                vec!["run (missing from the fold)".into()],
                Vec::new(),
                Vec::new(),
            )
        };
    };
    let mut differs = Vec::new();
    let mut folded_resolution = run.resolution();
    if let Resolution::Interrupted(ids) | Resolution::ProviderHeld(ids) = &mut folded_resolution {
        ids.sort_by(|a, b| a.0.cmp(&b.0));
    }
    let expected = today_resolution(&today.resolution);
    if expected != folded_resolution {
        let class = |r: &Resolution| match r {
            Resolution::Idle(reason) => format!("idle({reason:?})"),
            Resolution::Interrupted(ids) => format!("interrupted({})", ids.len()),
            Resolution::ProviderHeld(ids) => format!("held({})", ids.len()),
            Resolution::Paused(reason) => format!("paused({reason:?})"),
            Resolution::Resume => "resume".into(),
        };
        differs.push(format!(
            "resolution: today {}, folded {}",
            class(&expected),
            class(&folded_resolution)
        ));
    }
    let mut check = |field: &str, equal: bool| {
        if !equal {
            differs.push(field.to_owned());
        }
    };
    check(
        "run_id",
        today.run_id.as_ref().map(|id| id.0.as_str()) == Some(run.run_id.as_str()),
    );
    check(
        "completed_steps",
        today.completed_steps == run.completed_steps,
    );
    check(
        "tool_call_count",
        today.tool_call_count == run.tool_call_count,
    );
    check("usage", today.usage == run.usage);
    check("retries_used", today.retries_used == run.retries_used);
    check(
        "last_finish_reason",
        today.last_step_finish_reason == run.last_finish_reason,
    );
    check(
        "pending_steers",
        today.pending_steers.len() == run.pending_steers,
    );
    let mut nudges: Vec<u64> = today
        .budget_nudges_fired
        .iter()
        .map(|i| *i as u64)
        .collect();
    nudges.sort_unstable();
    let mut folded_nudges = run.budget.nudges_fired.clone();
    folded_nudges.sort_unstable();
    check("budget_nudges", nudges == folded_nudges);
    check(
        "finalize_fired",
        today.budget_finalize_fired == run.budget.finalize_fired,
    );
    check(
        "finalize_pending",
        today.budget_finalize_pending == run.budget.finalize_pending,
    );
    let today_batch = today.open_tool_batch.as_ref().map(|batch| {
        let states: Vec<String> = batch
            .calls
            .iter()
            .map(|(id, state)| format!("{}={state:?}", id.0))
            .collect();
        (batch.batch_id.0.clone(), batch.step_id.0, states)
    });
    let folded_batch = run.open_batch.as_ref().map(|batch| {
        let states: Vec<String> = (0..batch.calls.len())
            .map(|position| {
                let state = match batch.state(position) {
                    CallState::Terminated => "Terminated",
                    CallState::Dispatched => "Dispatched",
                    CallState::ProviderHeld => "ProviderHeld",
                    CallState::HeldSuccessor => "HeldSuccessor",
                    CallState::NotSent => "NotSent",
                };
                format!("{}={state}", batch.calls[position].id.0)
            })
            .collect();
        (batch.batch_id.clone(), batch.step_id, states)
    });
    check("open_batch", today_batch == folded_batch);
    // Today's values as JSON, absent and null alike as `None`, against the fold's.
    let as_json = |value: serde_json::Value| (!value.is_null()).then_some(value);
    let json_of = |value: &dyn erased::Serialize| as_json(value.to_json());
    check(
        "last_step_usage",
        today.last_step_usage == run.last_step_usage,
    );
    check(
        "provider_call_started",
        today.provider_call_started == run.provider_call_started,
    );
    check(
        "account_limit_pause",
        json_of(&today.account_limit_pause) == run.account_limit_pause,
    );
    check("keep_warm", json_of(&today.keep_warm) == run.keep_warm);
    check("on_restart", json_of(&today.on_restart) == run.on_restart);
    check(
        "refusal_schedule",
        json_of(&today.refusal_fallback) == refusal_schedule,
    );
    let today_cancel = today.cancel_at.map(|at| match at {
        broca_wal::record::RunCancelAt::Now => CancelPoint::Now,
        broca_wal::record::RunCancelAt::StepBoundary => CancelPoint::StepBoundary,
    });
    check("cancel", today_cancel == run.cancel);
    check("queue", today_queue == folded_queue);
    let mut set = Vec::new();
    for (field, value) in [
        ("account_limit_pause", json_of(&today.account_limit_pause)),
        ("keep_warm", json_of(&today.keep_warm)),
        ("on_restart", json_of(&today.on_restart)),
        ("refusal_schedule", json_of(&today.refusal_fallback)),
    ] {
        if value.is_some() {
            set.push(field);
        }
    }
    if today_cancel.is_some() {
        set.push("cancel");
    }
    if !today_queue.is_empty() {
        set.push("queue");
    }
    let sizes = vec![
        ("run.unknown", run.unknown.len() as u64),
        (
            "run.open_batch.calls",
            run.open_batch.as_ref().map_or(0, |b| b.calls.len() as u64),
        ),
        (
            "run.budget.nudges_fired",
            run.budget.nudges_fired.len() as u64,
        ),
        ("run.pending_steers", run.pending_steers as u64),
    ];
    OpsOutcome::Compared(differs, sizes, set)
}

/// Runs the operational comparison on each log, or with `--prefixes` on every prefix of it
/// that ends at a record boundary: the states a crash could leave, which are the states a
/// resume decides on. A field is counted once per log however many prefixes it differs at.
fn run_ops(args: &Args) {
    let mut report = OpsReport::default();
    each_log(args, |item| {
        let Item::Log {
            name, old: Ok(old), ..
        } = item
        else {
            return;
        };
        report.logs += 1;
        if report.logs % args.every as u64 != 0 {
            report.not_visited += 1;
            return;
        }
        if args.prefixes && args.max_records.is_some_and(|max| old.len() > max) {
            report.not_visited += 1;
            return;
        }
        let ends: Vec<usize> = if args.prefixes {
            (1..=old.len()).collect()
        } else {
            vec![old.len()]
        };
        let mut log_fields = std::collections::BTreeSet::new();
        let mut log_equal = true;
        for end in ends {
            report.compared += 1;
            match compare_ops(&old[..end]) {
                OpsOutcome::ReplayRefused(reason) => {
                    *report.replay_refused.entry(reason).or_default() += 1;
                }
                OpsOutcome::FoldRefused(reason) => {
                    log_equal = false;
                    log_fields.insert(format!("fold refused: {reason}"));
                }
                OpsOutcome::NoRun => {}
                OpsOutcome::Compared(differs, sizes, set) => {
                    for field in set {
                        *report.set_on_today.entry(field).or_default() += 1;
                    }
                    for (field, size) in sizes {
                        let largest = report.largest.entry(field).or_default();
                        *largest = (*largest).max(size);
                    }
                    log_equal &= differs.is_empty();
                    log_fields.extend(differs);
                }
            }
        }
        if log_equal {
            report.equal += 1;
        }
        for field in log_fields {
            *report.mismatches.entry(field.clone()).or_default() += 1;
            let examples = report.examples.entry(field).or_default();
            if examples.len() < 3 {
                examples.push(name.clone());
            }
        }
    });
    write_report(&report, args.out_file.as_deref());
}

/// Accumulates one report over many logs.
struct Measuring {
    key: FrameKey,
    report: Report,
    parts: BTreeMap<Digest, u64>,
    per_log: Vec<u64>,
    load_us: Vec<u64>,
}

impl Measuring {
    fn new() -> Self {
        Self {
            key: measurement_key(),
            report: Report::default(),
            parts: BTreeMap::new(),
            per_log: Vec::new(),
            load_us: Vec::new(),
        }
    }

    fn fail(&mut self, name: &str, error: String) {
        self.report.failed_logs += 1;
        self.report.errors.push((name.to_owned(), error));
    }

    /// Converts and measures one old log. `name` identifies it in errors, and its frames are
    /// encrypted with `name` as their authenticated data, the way a real log's frames are bound
    /// to that log. `size` is its size as stored today.
    fn log(&mut self, name: &str, size: u64, old: &[FramedRecord]) {
        let report = &mut self.report;
        for framed in old {
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
        let converted = match convert(old) {
            Ok(converted) => converted,
            Err(e) => return self.fail(name, format!("convert: {e}")),
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

        let measured = match measure(&converted, &self.key, name.as_bytes()) {
            Ok(measured) => measured,
            Err(e) => return self.fail(name, format!("measure: {e}")),
        };
        let report = &mut self.report;
        report.new_framed_bytes += measured.frame_bytes;
        report.sealed_bytes += measured.sealed_bytes;
        report.sealed_pruned_bytes += measured.sealed_pruned_bytes;
        report.pruned_opaque_bytes += measured.pruned_opaque_bytes;
        report.load_us_total += measured.load_us;
        self.load_us.push(measured.load_us);

        report.logs += 1;
        report.old_file_bytes += size;
        report.old_records += old.len() as u64;
        report.new_records += converted.records.len() as u64;
        report.parts_bytes_per_run += converted.part_bytes_per_run;
        report.runs += converted.runs;
        for (digest, bytes) in &converted.parts {
            self.parts.insert(*digest, bytes.len() as u64);
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
        self.per_log.push(measured.frame_bytes);
    }

    fn finish(mut self, out_file: Option<&Path>) {
        let report = &mut self.report;
        debug_assert_eq!(
            report.new_framed_bytes,
            report.new_body_bytes + report.new_records * FRAME_OVERHEAD as u64
        );
        report.parts_distinct = self.parts.len() as u64;
        report.parts_distinct_bytes = self.parts.values().sum();
        report.new_bytes_per_log = percentiles(self.per_log);
        report.load_us_per_log = percentiles(self.load_us);
        write_report(report, out_file);
    }
}

fn run_measure(args: &Args) {
    let mut measuring = Measuring::new();
    each_log(args, |item| match item {
        Item::Log {
            name,
            size,
            old: Ok(old),
            read_ms,
        } => {
            measuring.report.read_ms += read_ms;
            measuring.log(&name, size, &old);
        }
        Item::Log {
            name, old: Err(e), ..
        } => measuring.fail(&name, e),
        Item::Note(label, note) => measuring.report.errors.push((label, note)),
    });
    measuring.finish(args.out_file.as_deref());
}

/// One log to measure or check, or a note about how a container was read.
enum Item {
    Log {
        name: String,
        /// Its size as stored today.
        size: u64,
        old: Result<Vec<FramedRecord>, String>,
        read_ms: u128,
    },
    Note(String, String),
}

/// Visits every session log under the given paths: `.wal` files directly, and the members of
/// `.ark` WAL archive containers. A member is written to a scratch file (under `--dir`, or the
/// system temporary folder), read back with the ordinary log reader, and removed.
fn each_log(args: &Args, mut visit: impl FnMut(Item)) {
    let dir = args.dir.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!("session-convert-{}", std::process::id()))
    });
    let scratch = dir.join("member.wal");
    let mut members_seen = 0u64;
    let mut addresses = std::collections::BTreeSet::new();
    let read = |path: &Path| {
        let started = Instant::now();
        let old = broca_wal::replay_file(path, u64::MAX).map_err(|e| format!("read: {e}"));
        (old, started.elapsed().as_millis())
    };
    for (index, file) in args.files.iter().enumerate() {
        if index % 1000 == 0 {
            eprintln!("{index}/{}", args.files.len());
        }
        let label = file.display().to_string();
        if file.extension().is_some_and(|e| e == "wal") {
            let name = file
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let size = std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
            let (old, read_ms) = read(file);
            visit(Item::Log {
                name,
                size,
                old,
                read_ms,
            });
            continue;
        }
        let refused = |reason: String| Item::Log {
            name: label.clone(),
            size: 0,
            old: Err(reason),
            read_ms: 0,
        };
        if let Err(e) = std::fs::create_dir_all(&dir) {
            visit(refused(format!("scratch folder: {e}")));
            continue;
        }
        let mut reader = match broca_wal::archive::ArchiveReader::open(file, u64::MAX) {
            Ok(reader) => reader,
            Err(e) => {
                visit(refused(format!("open container: {e}")));
                continue;
            }
        };
        // A verified index names every member; without one, a header walk stops at the first
        // damaged member, so the note says the container was read that way.
        let members: Vec<String> = match reader.verified_members() {
            Ok(Some(members)) => members.into_iter().map(|m| m.session_addr).collect(),
            Ok(None) | Err(_) => match reader.rescan() {
                Ok(members) => {
                    visit(Item::Note(
                        label.clone(),
                        "no verified index; read by header walk".into(),
                    ));
                    members.into_iter().map(|m| m.session_addr).collect()
                }
                Err(e) => {
                    visit(refused(format!("list members: {e}")));
                    continue;
                }
            },
        };
        for addr in members {
            if args.only.as_ref().is_some_and(|only| &addr != only) {
                continue;
            }
            members_seen += 1;
            addresses.insert(addr.clone());
            let name = format!("{label}#{addr}");
            let item = match reader.read_member(&addr) {
                Ok(Some(member)) => match std::fs::write(&scratch, &member.payload) {
                    Ok(()) => {
                        let (old, read_ms) = read(&scratch);
                        Item::Log {
                            name,
                            size: member.payload.len() as u64,
                            old,
                            read_ms,
                        }
                    }
                    Err(e) => Item::Log {
                        name,
                        size: 0,
                        old: Err(format!("scratch write: {e}")),
                        read_ms: 0,
                    },
                },
                Ok(None) => Item::Log {
                    name,
                    size: 0,
                    old: Err("listed member not found".into()),
                    read_ms: 0,
                },
                Err(e) => Item::Log {
                    name,
                    size: 0,
                    old: Err(format!("read member: {e}")),
                    read_ms: 0,
                },
            };
            visit(item);
        }
    }
    let _ = std::fs::remove_file(&scratch);
    if members_seen > 0 {
        eprintln!(
            "{members_seen} archived logs, {} distinct session addresses",
            addresses.len()
        );
    }
}

#[derive(Default, Serialize)]
struct AppendReport {
    logs: u64,
    records: u64,
    barriers: u64,
    bytes: u64,
    /// Write plus device sync per barrier, in microseconds.
    barrier_us: Percentiles,
    /// Loading a written log, in microseconds: read the file, read and decrypt every frame, and
    /// fold both streams.
    load_us: Percentiles,
    /// Per log: the size of the session's log in today's format, records, barriers, total append time in milliseconds, the
    /// written log's size, and its load time in microseconds.
    per_log: Vec<(u64, u64, u64, f64, u64, u64)>,
    failures: Vec<String>,
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
    let mut loads = Vec::new();
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
                    measured.bytes,
                    measured.load_us,
                ));
                latencies.extend(measured.barrier_us);
                loads.push(measured.load_us);
            }
            Err(e) => report.failures.push(format!("{}: {e}", file.display())),
        }
        let _ = std::fs::remove_file(&target);
    }
    report.barrier_us = percentiles(latencies);
    report.load_us = percentiles(loads);
    write_report(&report, args.out_file.as_deref());
}

#[derive(Default, Serialize)]
struct SplitReport {
    logs: u64,
    points: u64,
    /// Logs with a split point where loading from the checkpoint gave a different state.
    differing_logs: u64,
    differing_points: u64,
    /// Logs a fold refuses, by the first part of the reason; they aren't split.
    refused: BTreeMap<String, u64>,
    examples: Vec<String>,
}

fn run_split(args: &Args) {
    let mut report = SplitReport::default();
    each_log(args, |item| {
        let Item::Log {
            name, old: Ok(old), ..
        } = item
        else {
            return;
        };
        let records: Vec<broca_wal::WalRecord> = old.iter().map(|f| f.record.clone()).collect();
        let appends_in_conversation = !broca_core::is_mode3_log(&records);
        let Ok(converted) = convert(&old) else {
            *report.refused.entry("convert".into()).or_default() += 1;
            return;
        };
        let points = split_points(converted.records.len(), 64, 8);
        match check_splits(&converted.records, appends_in_conversation, &points) {
            Ok(check) => {
                report.logs += 1;
                report.points += check.points;
                if !check.differing.is_empty() {
                    report.differing_logs += 1;
                    report.differing_points += check.differing.len() as u64;
                    if report.examples.len() < 10 {
                        report.examples.push(name);
                    }
                }
            }
            Err(e) => {
                let key = e.split('(').next().unwrap_or_default().to_owned();
                *report.refused.entry(key).or_default() += 1;
            }
        }
    });
    write_report(&report, args.out_file.as_deref());
}
