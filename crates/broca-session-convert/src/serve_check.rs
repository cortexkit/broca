//! `serve-check`: the run and send facts the new storage serves, against today's answers.
//!
//! For each log, today's builder answers `run.status`, `run.result` and `send.status` from the
//! WAL alone (`broca_module::store_status::todays`), and the new storage answers them from the
//! log's converted session, loaded read-only as when no actor holds it
//! (`broca_module::store_status::Reader`). The replies are compared as JSON with only the agreed
//! differences masked: message ids, and wall-clock times (kept as whether present). Every run the
//! log starts is compared, every queued send's run id, one id no run carries, and every send id a
//! run start, queued send or steer recorded.
//!
//! A run the log leaves unfinished reads differently by design: today's read opens the session,
//! which seals it, while the WAL alone says active; the new storage serves the end that open
//! records. Those are counted apart, by the state each side serves.
//!
//! One difference is today's and stays counted: `run.result`'s final text on a run of a log on
//! the current compaction contract (`broca_core::is_mode3_log`) whose plan has a compaction item
//! while its frozen `render_tags` is off. The compaction view showed that run tag numbers, so
//! `session.entries` and the live display clean its replies; today's `run.result` cleans by the
//! run's own tag rules only and serves the notation. The new storage cleans it as
//! `session.entries` does. `SERVE_CHECK_SHOW=1` prints each difference's shape and
//! the run's tag decisions by both rules, never a log's content.
//!
//! The largest log is also timed: one `run.status` of its latest run read as running, and of its
//! first run uncached and from the finished-run cache, each loading the session from disk under
//! `--dir` as the module does.

use std::collections::BTreeMap;
use std::time::Instant;

use broca_module::store_reads::{
    answer_in_memory, execute, ReadContext, StoreReadOutcome, StoreReadRequest, ViewCache,
};
use broca_module::store_status::{todays, Reader, SettledRuns};
use broca_protocol::RunId;
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{LocalBackend, MemoryBackend, SessionAddr, SessionBackend};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, WalRecord};
use futures::executor::block_on;
use serde::Serialize;
use serde_json::{json, Value};

use super::{convert_log, each_log, triple, write_report, Args, Item};

#[derive(Default, Serialize)]
struct Report {
    logs: u64,
    /// Logs whose converted session couldn't be built, by reason.
    failures: BTreeMap<String, u64>,
    /// Runs compared, and how many of each reply were equal.
    runs: u64,
    statuses_equal: u64,
    results_equal: u64,
    /// Run ids of queued sends that never started a run of their own.
    queued_ids: u64,
    queued_equal: u64,
    /// One id no run carries, per log.
    unknown_ids: u64,
    unknown_equal: u64,
    /// Recorded send ids, and how many `send.status` replies were equal.
    sends: u64,
    sends_equal: u64,
    /// Today's `run.status` answers compared, by state, so each state's share is visible.
    states: BTreeMap<String, u64>,
    /// Runs a log leaves unfinished: today's WAL-alone state, then the new storage's.
    unfinished: BTreeMap<String, u64>,
    /// The first difference of each reply that differs, by op and field.
    mismatches: BTreeMap<String, u64>,
    /// A few logs per difference, to read by hand.
    examples: BTreeMap<String, Vec<String>>,
    timing: Option<Timing>,
}

#[derive(Default, Serialize)]
struct Timing {
    log: String,
    records: usize,
    runs: usize,
    /// How many times each read was timed.
    samples: usize,
    /// Loading the session read-only (`Session::view`), p50 and p99 in microseconds.
    load_us: (u128, u128),
    /// `run.status` of the latest run, read as running, three ways.
    latest: Ways,
    /// `run.status` of the first run, walked from its records each time (no finished-run cache).
    first_walked: Ways,
    /// `run.status` of the first run once the finished-run cache holds its facts.
    first_kept: Ways,
}

/// One read's p50 and p99, in microseconds: served by the session's actor from what it holds
/// (`open`; with the time the actor was held), from a view kept for the session's position
/// (`cached`), and from a fresh load (`cold`).
#[derive(Default, Serialize)]
struct Ways {
    open_us: (u128, u128),
    open_held_us: (u128, u128),
    /// Whether the actor handed the read back to be read from storage.
    open_handed_back: bool,
    cached_us: (u128, u128),
    cold_us: (u128, u128),
}

/// A reply as JSON with what each storage keeps for itself taken out, or its refusal.
fn reply<T: Serialize>(reply: Result<T, broca_module::serve::ServeError>) -> Value {
    match reply {
        Ok(value) => masked(serde_json::to_value(value).expect("replies serialize")),
        Err(error) => json!({ "refused": format!("{error:?}") }),
    }
}

fn masked(mut value: Value) -> Value {
    fn walk(value: &mut Value) {
        match value {
            Value::Object(object) => {
                object.remove("mid");
                for (name, member) in object.iter_mut() {
                    if name.ends_with("_ms") || name.ends_with("_at") {
                        *member = Value::Bool(!member.is_null());
                    } else {
                        walk(member);
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(walk),
            _ => {}
        }
    }
    walk(&mut value);
    value
}

/// The state a `run.status` answer names, or how it was refused.
fn state_of(value: &Value) -> String {
    match (&value["state"], &value["refused"]) {
        (Value::String(state), _) => state.clone(),
        (_, Value::String(refused)) => {
            format!("refused {}", refused.chars().take(60).collect::<String>())
        }
        _ => "?".into(),
    }
}

/// The run ids a log starts, its queued sends' run ids that started none, and its send ids with
/// the run id each one's send owns.
fn ids(records: &[FramedRecord]) -> (Vec<String>, Vec<String>, Vec<(String, String)>) {
    let mut runs: Vec<String> = Vec::new();
    let mut queued = Vec::new();
    let mut sends = Vec::new();
    for frame in records {
        match &frame.record {
            WalRecord::RunStarted {
                run_id, send_id, ..
            } => {
                if !runs.contains(&run_id.0) {
                    runs.push(run_id.0.clone());
                }
                if let Some(send_id) = send_id {
                    sends.push((run_id.0.clone(), send_id.clone()));
                }
            }
            WalRecord::Queued {
                submission_id,
                send_id,
                ..
            } => {
                let run = submission_id
                    .0
                    .strip_prefix("sub-")
                    .unwrap_or(&submission_id.0)
                    .to_owned();
                if let Some(send_id) = send_id {
                    sends.push((run.clone(), send_id.clone()));
                }
                queued.push(run);
            }
            WalRecord::Steered {
                send_id: Some(send_id),
                ..
            } => sends.push((format!("steer-{send_id}"), send_id.clone())),
            _ => {}
        }
    }
    queued.retain(|run| !runs.contains(run));
    sends.dedup();
    (runs, queued, sends)
}

/// Writes the converted records into a new session in the backend `backend` makes, and loads it
/// read-only.
async fn stored<B: SessionBackend>(
    backend: impl Fn() -> B,
    name: &str,
    records: &[broca_session_store::record::Record],
    deployment: &DeploymentKey,
) -> Result<broca_session_store::session::SessionView<B>, String> {
    let addr = SessionAddr(name.to_owned());
    let mut session = Session::create(backend(), addr.clone(), triple(name), deployment, false)
        .await
        .map_err(|e| format!("create: {e:?}"))?;
    for record in records {
        session
            .append(std::slice::from_ref(record), false)
            .await
            .map_err(|e| format!("append {}: {e:?}", record.name()))?;
    }
    session.close().await.map_err(|e| format!("close: {e:?}"))?;
    Session::view(backend(), addr, &triple(name), deployment)
        .await
        .map_err(|e| format!("view: {e:?}"))
}

pub(super) fn run(args: &Args) {
    let mut report = Report::default();
    let deployment = DeploymentKey::new(1, &[0x5a; 32]);
    let mut largest: Option<Largest> = None;
    each_log(args, |item| {
        let Item::Log {
            name, old: Ok(old), ..
        } = item
        else {
            return;
        };
        report.logs += 1;
        let mut fail = |what: &str, e: String| {
            let key: String = format!("{what}: {e}").chars().take(100).collect();
            *report.failures.entry(key.clone()).or_default() += 1;
            let examples = report.examples.entry(key).or_default();
            if examples.len() < 5 {
                examples.push(name.clone());
            }
        };
        let converted = match convert_log(&old) {
            Ok(converted) => converted,
            Err(e) => return fail("convert", e.0),
        };
        let backend = MemoryBackend::default();
        let view = match block_on(stored(|| &backend, &name, &converted.records, &deployment)) {
            Ok(view) => view,
            Err(e) => return fail("session", e),
        };
        if largest
            .as_ref()
            .is_none_or(|largest: &Largest| old.len() > largest.old.len())
        {
            largest = Some(Largest {
                name: name.clone(),
                old: old.records.clone(),
                records: converted.records.clone(),
                parts: converted.parts.clone(),
            });
        }
        let parts = MemoryParts {
            parts: converted.parts.clone(),
        };
        let reader = Reader {
            parts: &parts,
            live: false,
            settled: None,
            session_name: &name,
            live_step: None,
        };
        let (runs, queued, sends) = ids(&old);
        let old_log: &[FramedRecord] = &old;
        let latest = runs.last().cloned();
        let mut compare = |op: &str, today: Value, store: Value| -> bool {
            if today == store {
                return true;
            }
            let path = broca_session_convert::inverse::first_difference(&today, &store, "")
                .unwrap_or_else(|| "?".into());
            // `SERVE_CHECK_SHOW=1` prints each difference's shape, never a log's content: the
            // members and states of both replies, and a final message's sizes.
            if std::env::var("SERVE_CHECK_SHOW").is_ok() {
                let members = |value: &Value| {
                    value
                        .as_object()
                        .map(|object| object.keys().cloned().collect::<Vec<_>>())
                        .unwrap_or_default()
                };
                let text = |value: &Value| {
                    value["final_message"]["text"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned()
                };
                let (old, new) = (text(&today), text(&store));
                // The run's frozen tag decisions by both of today's rules: the run's own
                // (`computed_rules`, which today's `run.result` cleans by) and the compaction
                // view's (`clean_replies`, which `session.entries` and live display clean by).
                if let Some(run) = today["run_id"].as_str() {
                    let mut first_manifest = None;
                    for frame in old_log {
                        if let WalRecord::RunStarted {
                            run_id,
                            config,
                            manifest,
                            ..
                        } = &frame.record
                        {
                            if first_manifest.is_none() {
                                first_manifest.clone_from(manifest);
                            }
                            if run_id.0 != run {
                                continue;
                            }
                            let manifest = manifest.as_ref().or(first_manifest.as_ref());
                            let compaction_item = manifest.is_some_and(|manifest| {
                                serde_json::from_str::<Value>(manifest.plan.as_json_text())
                                    .ok()
                                    .is_some_and(|plan| {
                                        plan.get("compaction_item")
                                            .is_some_and(|item| !item.is_null())
                                    })
                            });
                            let structured = broca_protocol::structured_output::FrozenStructuredOutput::from_frozen_render_config(Some(&config.frozen_render_config)).map(|frozen| format!("{:?}", frozen.mode));
                            eprintln!(
                                "{name} {run}: mode3 {} computed_tags {:?} render_tags {} \
                                 compaction_item {compaction_item} structured {structured:?} \
                                 computed_rules {:?} mode3 clean_replies {}",
                                broca_core::is_mode3_log(old_log.iter().map(|f| &f.record)),
                                config.computed_tags,
                                config.render_tags,
                                broca_core::tags::computed_rules(config),
                                broca_core::compaction_v2::metadata::clean_replies(
                                    config.render_tags || compaction_item,
                                    &config.frozen_render_config
                                ),
                            );
                            break;
                        }
                    }
                }
                let common = old
                    .chars()
                    .zip(new.chars())
                    .take_while(|(a, b)| a == b)
                    .count();
                eprintln!(
                    "{name} {op} {path}: today {} {:?} ordinal {} text {} chars; store {} {:?} \
                     ordinal {} text {} chars; {common} in common",
                    state_of(&today),
                    members(&today),
                    today["final_message"]["ordinal"],
                    old.chars().count(),
                    state_of(&store),
                    members(&store),
                    store["final_message"]["ordinal"],
                    new.chars().count(),
                );
            }
            let key: String = format!("{op} {path}").chars().take(100).collect();
            *report.mismatches.entry(key.clone()).or_default() += 1;
            let examples = report.examples.entry(key).or_default();
            if examples.len() < 5 {
                examples.push(name.clone());
            }
            false
        };
        for run in &runs {
            let id = RunId(run.clone());
            let today = reply(todays::run_status_reply(&old, &id));
            let store = reply(block_on(reader.run_status_reply(&view, &id)));
            if today["state"] == json!("active") && latest.as_ref() == Some(run) {
                *report
                    .unfinished
                    .entry(format!("{} -> {}", state_of(&today), state_of(&store)))
                    .or_default() += 1;
                continue;
            }
            report.runs += 1;
            *report.states.entry(state_of(&today)).or_default() += 1;
            report.statuses_equal += u64::from(compare("run.status", today, store));
            let today = reply(todays::run_result(&old, &id, &name));
            let store = reply(block_on(reader.run_result(&view, &id)));
            report.results_equal += u64::from(compare("run.result", today, store));
        }
        for run in &queued {
            let id = RunId(run.clone());
            let today = reply(todays::run_status_reply(&old, &id));
            let store = reply(block_on(reader.run_status_reply(&view, &id)));
            // Delivered into the log's unfinished latest run: it reads as that run reads.
            if today["state"] == json!("active") {
                *report
                    .unfinished
                    .entry(format!(
                        "queued: {} -> {}",
                        state_of(&today),
                        state_of(&store)
                    ))
                    .or_default() += 1;
                continue;
            }
            report.queued_ids += 1;
            report.queued_equal += u64::from(compare("queued run.status", today, store));
        }
        let unknown = RunId("serve-check-unknown".into());
        report.unknown_ids += 1;
        let today = reply(todays::run_status_reply(&old, &unknown));
        let store = reply(block_on(reader.run_status_reply(&view, &unknown)));
        report.unknown_equal += u64::from(compare("unknown run.status", today, store));
        for (own_run, send_id) in &sends {
            // A send whose run is the log's unfinished latest one reads as that run reads.
            if latest.as_ref() == Some(own_run)
                && reply(todays::run_status_reply(&old, &RunId(own_run.clone())))["state"]
                    == json!("active")
            {
                continue;
            }
            report.sends += 1;
            let today = reply(todays::send_status(&old, RunId(own_run.clone()), send_id));
            let store = reply(block_on(reader.send_status(
                &view,
                RunId(own_run.clone()),
                send_id,
                None,
            )));
            report.sends_equal += u64::from(compare("send.status", today, store));
        }
    });
    if let (Some(largest), Some(dir)) = (largest, args.dir.as_ref()) {
        report.timing = time_largest(largest, dir, &deployment);
    }
    write_report(&report, args.out_file.as_deref());
}

/// The largest log seen: its name, today's records and its converted records and parts.
struct Largest {
    name: String,
    old: Vec<FramedRecord>,
    records: Vec<broca_session_store::record::Record>,
    parts: BTreeMap<broca_session_store::codec::Digest, Vec<u8>>,
}

/// Times `run.status` on the largest log, stored on disk under `dir`.
fn time_largest(
    largest: Largest,
    dir: &std::path::Path,
    deployment: &DeploymentKey,
) -> Option<Timing> {
    let Largest {
        name,
        old,
        records,
        parts,
    } = largest;
    let root = dir.join("serve-check-store");
    let _ = std::fs::remove_dir_all(&root);
    block_on(stored(
        || LocalBackend::new(root.clone()),
        &name,
        &records,
        deployment,
    ))
    .ok()?;
    let parts = MemoryParts { parts };
    let (runs, _, _) = ids(&old);
    let (first, latest) = (runs.first()?, runs.last()?);
    let samples = 20;
    let backend = || LocalBackend::new(root.clone());
    let addr = SessionAddr(name.clone());
    let session_triple = triple(&name);
    let load = || Session::view(backend(), addr.clone(), &session_triple, deployment);
    let quantiles = |mut times: Vec<u128>| {
        times.sort_unstable();
        let at = |q: f64| times[((times.len() - 1) as f64 * q).round() as usize];
        (at(0.5), at(0.99))
    };
    // `kept`: whether the finished-run cache lives across the samples, as the module's does.
    let ways = |run: &str, kept: bool| -> Ways {
        let request = StoreReadRequest::RunStatusReply(RunId(run.to_owned()));
        let shared = SettledRuns::default();
        let mut ways = Ways::default();
        // Open: the session as its actor holds it.
        let mut writer = block_on(Session::load(
            backend(),
            addr.clone(),
            &session_triple,
            deployment,
        ))
        .expect("the stored session opens");
        let cache = ViewCache::default();
        let mut total = Vec::new();
        let mut held = Vec::new();
        for _ in 0..samples {
            let own = SettledRuns::default();
            let ctx = read_context(&parts, &name, if kept { &shared } else { &own });
            let started = Instant::now();
            let outcome = block_on(answer_in_memory(&mut writer, &ctx, &request));
            held.push(started.elapsed().as_micros());
            if let StoreReadOutcome::Stored { position, .. } = outcome {
                ways.open_handed_back = true;
                let view = block_on(cache.view(
                    &name,
                    position,
                    Session::view_at(
                        backend(),
                        addr.clone(),
                        &session_triple,
                        deployment,
                        &position,
                    ),
                ))
                .expect("the stored session loads");
                let _ = block_on(execute(&view, &ctx, &request));
            }
            total.push(started.elapsed().as_micros());
        }
        block_on(writer.close()).expect("the session closes");
        ways.open_us = quantiles(total);
        ways.open_held_us = quantiles(held);
        // Closed, cached: the position probed, the kept view served.
        let cache = ViewCache::default();
        let mut cached = Vec::new();
        for _ in 0..samples {
            let own = SettledRuns::default();
            let ctx = read_context(&parts, &name, if kept { &shared } else { &own });
            let started = Instant::now();
            let position = block_on(Session::stored_position(&backend(), &addr, deployment))
                .expect("the head reads")
                .expect("the session exists");
            let view = block_on(cache.view(
                &name,
                position,
                Session::view_at(
                    backend(),
                    addr.clone(),
                    &session_triple,
                    deployment,
                    &position,
                ),
            ))
            .expect("the stored session loads");
            let _ = block_on(execute(&view, &ctx, &request));
            cached.push(started.elapsed().as_micros());
        }
        ways.cached_us = quantiles(cached);
        // Closed, cold: a fresh load for every read.
        let mut cold = Vec::new();
        for _ in 0..samples {
            let own = SettledRuns::default();
            let ctx = read_context(&parts, &name, if kept { &shared } else { &own });
            let started = Instant::now();
            let view = block_on(load()).expect("the stored session loads");
            let _ = block_on(execute(&view, &ctx, &request));
            cold.push(started.elapsed().as_micros());
        }
        ways.cold_us = quantiles(cold);
        ways
    };
    let mut loads = Vec::new();
    for _ in 0..samples {
        let started = Instant::now();
        block_on(load()).expect("the stored session loads");
        loads.push(started.elapsed().as_micros());
    }
    let timing = Timing {
        log: name.clone(),
        records: old.len(),
        runs: runs.len(),
        samples,
        load_us: quantiles(loads),
        latest: ways(latest, true),
        first_walked: ways(first, false),
        first_kept: ways(first, true),
    };
    let _ = std::fs::remove_dir_all(&root);
    Some(timing)
}

/// What a timed read of the session `name` needs: the session held by its actor, as a polled
/// run's session is.
fn read_context<'a>(
    parts: &'a MemoryParts,
    name: &'a str,
    settled: &'a SettledRuns,
) -> ReadContext<'a> {
    ReadContext {
        parts,
        held: true,
        open: true,
        settled: Some((settled, name)),
        session_name: name,
        live_step: None,
    }
}
