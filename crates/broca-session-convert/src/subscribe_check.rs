//! `session-convert subscribe <path>...`: for every session log, today's control stream (the
//! payloads `session.subscribe` delivers, projected from the WAL) against the stream the new
//! storage's projection (`broca_module::store_control`) delivers from the converted records.
//! Payloads must be equal unit for unit, cursors aside; today's message ids are compared in the
//! public form the serve layer gives them.
//!
//! It also resumes the new storage's projection from a checkpoint, as a subscription catching up
//! from a log's start does: at sampled points between steps, the folds through that point seed a
//! fresh projection of the records after it, whose units (cursors included) must equal the
//! units the whole log's projection gives there.
//!
//! The report names counts and difference paths only, never values.

use std::collections::BTreeMap;
use std::time::Instant;

use broca_core::projection::ControlProjectionState;
use broca_module::store_control::{project_all, StoreControl, StoreUnit};
use broca_session_convert::inverse::first_difference;
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::record::Record;
use cortexkit_role_llm_runner::live::LiveEvent;
use serde::Serialize;
use serde_json::Value;

use super::{convert_log, each_log, write_report, Args, Item, OldLog};

#[derive(Default, Serialize)]
struct SubscribeReport {
    logs: u64,
    mode3_logs: u64,
    /// Logs whose two streams are equal, payload for payload.
    equal_logs: u64,
    /// Units today's stream delivers across every log, and of those, the ones the new stream
    /// delivers equal at the same place.
    today_units: u64,
    equal_units: u64,
    /// Units the new projection made, and of those, notices hidden because their record names
    /// no send.
    store_units: u64,
    hidden_notices: u64,
    /// Converted logs whose folds refuse a message id that differs from the one its step or
    /// batch announced. Must stay zero.
    id_mismatches: u64,
    /// Resumes from a checkpoint at a sampled point between steps, and those whose units after
    /// the point equal the whole projection's.
    resumes: u64,
    resumes_equal: u64,
    /// The first difference of each unequal log: what kind of unit, and the payload path.
    mismatches: BTreeMap<String, u64>,
    resume_mismatches: BTreeMap<String, u64>,
    failures: BTreeMap<String, u64>,
    /// Up to a few unequal log names per difference, so one can be looked at with `--only`.
    examples: BTreeMap<String, Vec<String>>,
}

/// A unit's payload as the wire carries it: the control unit and its lifecycle event.
type Payload = (Value, Value);

fn payload(unit: &broca_protocol::caller::ControlUnit, event: &LiveEvent) -> Payload {
    (
        serde_json::to_value(unit).expect("control units serialize"),
        serde_json::to_value(event).expect("live events serialize"),
    )
}

/// What kind of unit `payload` is, for a difference's name.
fn kind(payload: &Payload) -> String {
    let name = |value: &Value| {
        value
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                value
                    .as_object()
                    .and_then(|object| object.keys().next().cloned())
            })
            .unwrap_or_else(|| "?".into())
    };
    format!("{}/{}", name(&payload.0), name(&payload.1))
}

/// A difference path without array positions, so differences group by shape.
fn shape(path: &str) -> String {
    let mut out = String::new();
    let mut skipping = false;
    for c in path.chars() {
        match c {
            '[' => {
                skipping = true;
                out.push_str("[]");
            }
            ']' => skipping = false,
            _ if skipping => {}
            _ => out.push(c),
        }
    }
    out
}

/// Today's stream for `old`: every unit its projection makes, the notices its send keys allow,
/// with message ids in the public form a mode-3 log serves.
fn today(old: &OldLog, mode3: bool) -> Result<Vec<Payload>, String> {
    let mut state = ControlProjectionState::default();
    state.set_wal_lineage_id(old.lineage_id);
    let frames = broca_core::project_control_with_mids(old, &mut state)?;
    let mut keys = broca_core::notice_keys::NoticeSendKeys::default();
    keys.observe(old);
    let mut out = Vec::new();
    for mut frame in frames {
        if let LiveEvent::NoticeDelivered(notice) = &frame.event {
            let submission = frame
                .notice_submission_id
                .as_deref()
                .ok_or("a notice without its submission")?;
            if !keys.allows(submission, &notice.sender, &notice.send_id)? {
                continue;
            }
        }
        if mode3 {
            public_mids(&mut frame.unit, &mut frame.event, old.lineage.as_deref())?;
        }
        out.push(payload(&frame.unit, &frame.event));
    }
    Ok(out)
}

/// The serve layer's translation of a mode-3 log's native ids into public ones.
fn public_mids(
    unit: &mut broca_protocol::caller::ControlUnit,
    event: &mut LiveEvent,
    lineage: Option<&str>,
) -> Result<(), String> {
    let translate = |mid: &mut String| -> Result<(), String> {
        if let Some(position) = broca_core::position_of_mid(mid, false, None) {
            *mid = broca_core::message_mid(true, lineage, position)?;
        }
        Ok(())
    };
    if let broca_protocol::caller::ControlUnit::MessageBoundary { mid } = unit {
        translate(mid)?;
    }
    match event {
        LiveEvent::MessageStarted(event) | LiveEvent::MessageFinished(event) => {
            translate(&mut event.mid)?
        }
        LiveEvent::ToolStarted(event) => translate(&mut event.mid)?,
        LiveEvent::ToolFinished(event) => translate(&mut event.mid)?,
        LiveEvent::NoticeDelivered(event) => translate(&mut event.mid)?,
        _ => {}
    }
    Ok(())
}

fn visible(units: &[StoreUnit]) -> Vec<Payload> {
    units
        .iter()
        .filter(|unit| unit.visible)
        .map(|unit| payload(&unit.unit, &unit.event))
        .collect()
}

/// Points where a log may start: before a run's or a step's start, or a batch's start, that no
/// input message of the same placement precedes.
fn resume_points(records: &[(u64, Record)], samples: usize) -> Vec<usize> {
    let candidates: Vec<usize> = (1..records.len())
        .filter(|&at| {
            matches!(
                records[at].1,
                Record::RunStart(_) | Record::StepStart(_) | Record::BatchStart(_)
            ) && !matches!(records[at - 1].1, Record::Input(_))
        })
        .collect();
    if candidates.len() <= samples {
        return candidates;
    }
    (0..samples)
        .map(|n| candidates[n * (candidates.len() - 1) / (samples - 1).max(1)])
        .collect()
}

#[derive(Default, Serialize)]
struct CostReport {
    log: String,
    wal_bytes: u64,
    records: u64,
    /// Units one catch-up delivers: everything after the cursor through the session's end.
    units: u64,
    runs: u64,
    /// Each catch-up's time, from the read of the log holding the cursor through the resume
    /// check, in microseconds.
    catch_up_us: super::Percentiles,
    /// Each seeding of the live projection's time, from the read of the open log from its
    /// checkpoint through the session's end, in microseconds.
    seed_us: super::Percentiles,
    /// The estimated memory the seeded projection holds, in bytes.
    seed_held_bytes: u64,
    failures: Vec<String>,
}

/// `session-convert subscribe-cost --dir SCRATCH [--out FILE] PATH...`: what a subscriber's
/// catch-up costs on the largest log given, resuming from the first unit of the session's open
/// log. The log is converted and written to a session under `--dir` with the batches its run
/// barriers make, then caught up ten times over that session's files, as a subscriber's task
/// reads them, and the live projection seeded ten times, as the session's actor seeds it when it
/// opens. The session is removed afterwards.
pub(super) fn cost(args: &Args) {
    use broca_session_store::backend::LocalBackend;
    use broca_session_store::head::SessionTriple;

    const RUNS: usize = 10;
    let dir = args.dir.clone().unwrap_or_else(|| super::usage());
    let mut report = CostReport::default();
    let Some((wal_bytes, file)) = args
        .files
        .iter()
        .filter(|file| file.extension().is_some_and(|e| e == "wal"))
        .map(|file| (std::fs::metadata(file).map(|m| m.len()).unwrap_or(0), file))
        .max()
    else {
        super::usage()
    };
    report.log = file
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    report.wal_bytes = wal_bytes;
    let measured = (|| {
        let old = broca_wal::replay_file(file, u64::MAX)
            .map(|records| OldLog::new(records, file))
            .map_err(|e| format!("read: {e}"))?;
        let converted = convert_log(&old).map_err(|e| format!("convert: {}", e.0))?;
        let records = converted.records;
        report.records = records.len() as u64;
        let root = dir.join("subscribe-cost");
        let _ = std::fs::remove_dir_all(&root);
        let backend = LocalBackend::new(&root);
        let addr = broca_session_store::backend::SessionAddr("cost".into());
        let triple = SessionTriple {
            project_root: "/cost".into(),
            harness: "cost".into(),
            session: "cost".into(),
        };
        // Measurements encrypt with a fixed throwaway key: only the cost matters.
        let deployment = crate::deployment();
        let wal: Vec<broca_wal::WalRecord> = old.iter().map(|f| f.record.clone()).collect();
        let appends_in_conversation = !broca_core::is_mode3_log(&wal);
        let timed = futures::executor::block_on(async {
            let mut session = broca_session_store::session::Session::create(
                &backend,
                addr.clone(),
                triple.clone(),
                &deployment,
                appends_in_conversation,
            )
            .await
            .map_err(|e| format!("create: {e:?}"))?;
            let mut start = 0;
            for (index, record) in records.iter().enumerate() {
                let next = records.get(index + 1);
                if broca_session_convert::measure::barrier_after(record, next) || next.is_none() {
                    session
                        .append(&records[start..=index], true)
                        .await
                        .map_err(|e| format!("append: {e:?}"))?;
                    start = index + 1;
                }
            }
            let cursor = broca_module::store_fanout::open_log_first_cursor(&session)
                .await?
                .ok_or("the open log yields no unit")?;
            let mut times = Vec::new();
            for _ in 0..RUNS {
                let started = Instant::now();
                let units = broca_module::store_fanout::catch_up_from(&session, &cursor).await?;
                times.push(started.elapsed().as_micros() as u64);
                report.units = units as u64;
            }
            // The live projection's seeding, as the session's actor runs it when it opens.
            let mut seedings = Vec::new();
            for _ in 0..RUNS {
                let started = Instant::now();
                let (held, _) = broca_module::store_fanout::measure_seed(&session).await?;
                seedings.push(started.elapsed().as_micros() as u64);
                report.seed_held_bytes = held as u64;
            }
            session.close().await.map_err(|e| format!("close: {e:?}"))?;
            Ok::<_, String>((times, seedings))
        });
        let _ = std::fs::remove_dir_all(&root);
        timed
    })();
    match measured {
        Ok((times, seedings)) => {
            report.runs = times.len() as u64;
            report.catch_up_us = super::percentiles(times);
            report.seed_us = super::percentiles(seedings);
        }
        Err(e) => report.failures.push(e),
    }
    write_report(&report, args.out_file.as_deref());
}

pub(super) fn run(args: &Args) {
    let mut report = SubscribeReport::default();
    each_log(args, |item| {
        let Item::Log {
            name, old: Ok(old), ..
        } = item
        else {
            if let Item::Log { old: Err(e), .. } = item {
                *report.failures.entry(format!("read: {e}")).or_default() += 1;
            }
            return;
        };
        report.logs += 1;
        let records: Vec<broca_wal::WalRecord> = old.iter().map(|f| f.record.clone()).collect();
        let mode3 = broca_core::is_mode3_log(&records);
        report.mode3_logs += u64::from(mode3);
        let mut fail = |what: &str, e: String| {
            let key: String = format!("{what}: {e}").chars().take(120).collect();
            *report.failures.entry(key).or_default() += 1;
        };
        let today = match today(&old, mode3) {
            Ok(today) => today,
            Err(e) => return fail("today's projection", e),
        };
        let converted = match convert_log(&old) {
            Ok(converted) => converted,
            Err(e) => return fail("convert", e.0),
        };
        let stored: Vec<(u64, Record)> = converted
            .records
            .into_iter()
            .enumerate()
            .map(|(at, record)| (at as u64 + 1, record))
            .collect();
        // The folds a session loads with, which refuse a message id other than the one its
        // step or batch announced, snapshotted where resumes start.
        let points = resume_points(&stored, 8);
        let mut snapshots = Vec::new();
        let mut conversation = Conversation::new(false);
        let mut operations = Operations::default();
        for (at, (seq, record)) in stored.iter().enumerate() {
            if points.contains(&at) {
                let mut checkpoint = conversation.clone();
                checkpoint.clear_window();
                snapshots.push((at, checkpoint, operations.clone()));
            }
            if let Err(e) = broca_session_store::session::check_announced_mid(
                &conversation,
                &operations,
                record,
            )
            .and_then(|()| {
                broca_session_store::session::apply(
                    &mut conversation,
                    &mut operations,
                    *seq,
                    record,
                )
            }) {
                if e.starts_with("message_id_mismatch") {
                    report.id_mismatches += 1;
                }
                let e: String = e.chars().take(60).collect();
                return fail("fold", e);
            }
        }
        drop(conversation);
        let mut control = StoreControl::default();
        let units = match project_all(&mut control, &stored) {
            Ok(units) => units,
            Err(e) => return fail("store projection", e),
        };
        report.store_units += units.len() as u64;
        report.hidden_notices += units
            .iter()
            .filter(|unit| !unit.visible && matches!(unit.event, LiveEvent::NoticeDelivered(_)))
            .count() as u64;
        let store = visible(&units);
        report.today_units += today.len() as u64;
        let equal = today.iter().zip(&store).take_while(|(a, b)| a == b).count();
        report.equal_units += equal as u64;
        if equal == today.len() && today.len() == store.len() {
            report.equal_logs += 1;
        } else {
            let key = match (today.get(equal), store.get(equal)) {
                (Some(a), Some(b)) => {
                    let at = if a.0 != b.0 { "unit" } else { "event" };
                    let (x, y) = if a.0 != b.0 {
                        (&a.0, &b.0)
                    } else {
                        (&a.1, &b.1)
                    };
                    let path = first_difference(x, y, at).unwrap_or_default();
                    format!("{} vs {}: {}", kind(a), kind(b), shape(&path))
                }
                (Some(a), None) => format!("store stream ends before today's {}", kind(a)),
                (None, Some(b)) => format!("store stream has an extra {}", kind(b)),
                (None, None) => "equal prefix".into(),
            };
            *report.mismatches.entry(key.clone()).or_default() += 1;
            let examples = report.examples.entry(key).or_default();
            if examples.len() < 3 {
                examples.push(name.clone());
            }
        }
        for (at, checkpoint, operations) in snapshots {
            report.resumes += 1;
            let first = stored[at].0;
            let mut resumed = StoreControl::at_checkpoint(&checkpoint, &operations, first - 1);
            let after = match project_all(&mut resumed, &stored[at..]) {
                Ok(units) => units,
                Err(e) => {
                    let key: String = format!("projection: {e}").chars().take(80).collect();
                    *report.resume_mismatches.entry(key).or_default() += 1;
                    continue;
                }
            };
            let whole: Vec<&StoreUnit> = units.iter().filter(|unit| unit.seq >= first).collect();
            let same = after.len() == whole.len()
                && after.iter().zip(&whole).all(|(a, b)| {
                    (a.seq, a.sub_index, a.visible) == (b.seq, b.sub_index, b.visible)
                        && payload(&a.unit, &a.event) == payload(&b.unit, &b.event)
                });
            if same {
                report.resumes_equal += 1;
            } else {
                let differs = after
                    .iter()
                    .zip(&whole)
                    .position(|(a, b)| payload(&a.unit, &a.event) != payload(&b.unit, &b.event));
                let key = match differs {
                    Some(index) => {
                        let (a, b) = (&after[index], whole[index]);
                        let (a, b) = (payload(&a.unit, &a.event), payload(&b.unit, &b.event));
                        format!("{} vs {}", kind(&a), kind(&b))
                    }
                    None => format!("{} units against {}", after.len(), whole.len()),
                };
                *report.resume_mismatches.entry(key).or_default() += 1;
            }
        }
    });
    write_report(&report, args.out_file.as_deref());
}
