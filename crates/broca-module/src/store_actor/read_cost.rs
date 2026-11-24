//! The read cost bar: what each read of a session kept in the session store costs, served by its
//! actor (open), from a kept view (closed, cached), and from a fresh load (closed, cold), on
//! synthetic sessions of 1,000 to 500,000 messages. Ignored: it writes gigabytes and runs for
//! minutes. Run it on a release build:
//!
//! ```text
//! READ_COST_OUT=target/scratch/read-cost.md cargo test --release -p broca-module --lib \
//!     store_actor::read_cost -- --ignored --nocapture
//! ```
//!
//! `READ_COST_SIZES` (default `1000,150000,500000`) lists the sizes, and `READ_COST_SEALED`
//! (default `150000`) the sizes also measured after a seal that leaves about a thousand messages
//! in the window.
//!
//! A session is built with the store's own writer from template runs the new run loop wrote: a
//! user input, one to four tool turns whose results are one to six kilobytes, and a final answer.
//! Each run is a template run's records with its run id, its messages' ordinals and its message
//! ids moved to where the run lands. The session's last run is left between two steps, so it is
//! the active run.

use std::sync::Arc;
use std::time::{Duration, Instant};

use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason};
use broca_protocol::{MessageId, NormalizedEvent, ProviderControl, RunId, Usage};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::record::Record;
use broca_session_store::session::{Session, SessionView};

use super::tests::{actor_with, bind, deployment, executor, idle, send_with, text_step, triple};
use crate::actor::SendKey;
use crate::store_reads::{
    answer_in_memory, execute, PageRequest, ReadContext, StoreReadOutcome, StoreReadRequest,
    ViewCache,
};
use crate::store_status::SettledRuns;

/// The run id template runs are written under. Every run built from a template takes an id of
/// the same length, so the id can be replaced in a record's encoded bytes.
const TEMPLATE_RUN: &str = "tplrun0000";

/// The address the cost bar's sessions are written at.
const SESSION: &str = "0123456789abcdef";

fn run_id(index: usize) -> String {
    let id = format!("r{index:09}");
    assert_eq!(id.len(), TEMPLATE_RUN.len());
    id
}

/// A step that calls the `read` tool once, with call id `call`, after a sentence of text.
fn tool_step(call: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(format!("m-{call}")),
                content: vec![
                    ContentBlock::Text {
                        text: "Let me look at the file that defines this before changing it."
                            .into(),
                    },
                    ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                        thought_signature: None,
                        provider_state: None,
                        tool_call_id: broca_protocol::ToolCallId(call.into()),
                        tool_name: "read".into(),
                        input: serde_json::json!({"path": "crates/example/src/lib.rs"}).into(),
                    }),
                ],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(12_000),
                output_tokens: Some(80),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

/// A request whose run may take as many steps as a template asks for.
fn request(text: &str) -> broca_protocol::request::SubmissionRequest {
    serde_json::from_value(serde_json::json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": text}]}],
        "tools": [{
            "name": "read",
            "description": "reads a file",
            "input_schema": {"type": "object"},
        }],
        "policy_allowed": ["read"],
        "stop_when": [{"type": "step_count", "n": 50}],
    }))
    .unwrap()
}

/// One run the new loop wrote: its records from its start on, the ordinal of its first message,
/// and how many messages it placed.
struct Template {
    records: Vec<Record>,
    first_ordinal: u64,
    messages: u64,
}

/// A run of `turns` tool turns whose results are `result_bytes` long, written by the new loop
/// through a store actor, with its setup parts kept in `parts`.
async fn template(turns: usize, result_bytes: usize, parts: &SharedParts) -> Template {
    let mut steps: Vec<_> = (1..=turns).map(|n| tool_step(&format!("c{n}"))).collect();
    let answer = "The change is in place: the reader now keeps the view it loaded and serves it \
                  again until the session moves on. "
        .repeat(4);
    steps.push(text_step("final", &answer));
    let mut executor = executor(steps, None);
    executor.tool_text = "fn example() -> u64 { 42 }\n".repeat(result_bytes / 27 + 1);
    let (commands, served, _) = actor_with(executor, parts.clone()).await;
    let outcome = send_with(
        &commands,
        TEMPLATE_RUN,
        request("Please find where the reader reloads the session and fix it."),
        SendKey::Minted,
    )
    .await;
    assert!(
        matches!(outcome, crate::actor::SendOutcome::Active { .. }),
        "{outcome:?}"
    );
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let all = session.records_from(1).await.unwrap();
    let mut start = all
        .iter()
        .position(|(_, record)| matches!(record, Record::RunStart(_)))
        .expect("the run's start");
    // The setup the run froze, written before its start because the session had none yet. Only
    // a session's first run carries it when every later run freezes the same setup.
    if start > 0 && matches!(all[start - 1].1, Record::SetupStub(_)) {
        start -= 1;
    }
    let records: Vec<Record> = all[start..]
        .iter()
        .map(|(_, record)| record.clone())
        .filter(|record| !matches!(record, Record::WriterOpened(_) | Record::ChangeAborted(_)))
        .collect();
    let ordinals: Vec<u64> = records
        .iter()
        .filter_map(|record| match record {
            Record::EntryFacts(facts) => Some(facts.ordinal),
            _ => None,
        })
        .collect();
    let first_ordinal = *ordinals.iter().min().expect("the run placed messages");
    let messages = ordinals.iter().max().unwrap() - first_ordinal + 1;
    Template {
        records,
        first_ordinal,
        messages,
    }
}

/// `record` from a template run, as run `run` writes it when its first message takes ordinal
/// `first_ordinal` and the record itself takes the seq whose message id is `mid`.
fn moved(record: &Record, run: &str, ordinal_shift: i64, mid: &str) -> Record {
    let mut body = record.encode_body();
    let (from, to) = (TEMPLATE_RUN.as_bytes(), run.as_bytes());
    let mut at = 0;
    while let Some(found) = body[at..]
        .windows(from.len())
        .position(|window| window == from)
    {
        body[at + found..at + found + to.len()].copy_from_slice(to);
        at += found + to.len();
    }
    let mut record = Record::decode(record.kind(), &body).expect("a moved record decodes");
    let shift = |ordinal: u64| u64::try_from(ordinal as i64 + ordinal_shift).unwrap();
    match &mut record {
        Record::EntryFacts(facts) => {
            let facts = std::sync::Arc::make_mut(facts);
            facts.ordinal = shift(facts.ordinal);
            facts.mid = mid.to_owned();
        }
        Record::TurnFacts(turn) => {
            let turn = std::sync::Arc::make_mut(turn);
            turn.ordinal = shift(turn.ordinal);
        }
        // The builder names each message by the seq of its own facts, so no step or batch start
        // announces an id.
        Record::StepStart(start) => start.mid = None,
        Record::BatchStart(batch) => batch.mid = None,
        _ => {}
    }
    record
}

/// A built session: where it is, its runs' ids, and the parts its runs name.
struct Built {
    root: std::path::PathBuf,
    addr: SessionAddr,
    messages: u64,
    active: String,
    finished: String,
}

/// Writes a session of about `messages` messages under `root`, from `templates` in turn, the
/// last run left between two steps. With `sealed`, history is sealed at a run's input about a
/// thousand messages before the end.
async fn build(
    root: &std::path::Path,
    messages: u64,
    templates: &[Template],
    sealed: bool,
) -> Built {
    let _ = std::fs::remove_dir_all(root);
    let addr = SessionAddr(SESSION.into());
    let mut session = Session::create(
        LocalBackend::new(root.to_path_buf()),
        addr.clone(),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let mut index = 0;
    let mut sealed_at = None;
    loop {
        let template = &templates[index % templates.len()];
        let last = session.conversation.placed + template.messages >= messages;
        let run = run_id(index);
        let shift = session.conversation.placed as i64 + 1 - template.first_ordinal as i64;
        let records: &[Record] = if last {
            // Left between its last two steps: everything before its final step's start.
            let end = template
                .records
                .iter()
                .rposition(|record| matches!(record, Record::StepStart(_)))
                .expect("the run's steps");
            &template.records[..end]
        } else {
            &template.records
        };
        let seal_here = sealed
            && !last
            && sealed_at.is_none()
            && session.conversation.placed + template.messages + 1_000 >= messages;
        let batch: Vec<Record> = records
            .iter()
            .filter(|record| index == 0 || !matches!(record, Record::SetupStub(_)))
            .enumerate()
            .map(|(offset, record)| moved(record, &run, shift, &session.message_id(offset as u64)))
            .collect();
        let first = session
            .append(&batch, index % 64 == 0 || last || seal_here)
            .await
            .unwrap();
        if seal_here {
            // The cut falls at this run's input, the record after its start: everything before
            // the run is sealed, as a compaction cut there seals it.
            let input = batch
                .iter()
                .position(|record| matches!(record, Record::Input(_)))
                .expect("the run's input") as u64;
            session.seal(Some(first + input), 1).await.unwrap();
            sealed_at = Some(first + input);
        }
        if last {
            break;
        }
        index += 1;
    }
    let built = Built {
        root: root.to_path_buf(),
        addr,
        messages: session.conversation.placed,
        active: run_id(index),
        finished: run_id(index.saturating_sub(1)),
    };
    session.close().await.unwrap();
    built
}

/// Nanosecond samples, reported as p50 and p99 in milliseconds.
fn percentiles(mut samples: Vec<Duration>) -> String {
    samples.sort();
    let at = |q: f64| {
        let index = ((samples.len() as f64 - 1.0) * q).round() as usize;
        samples[index].as_secs_f64() * 1_000.0
    };
    format!("{:.3} / {:.3}", at(0.5), at(0.99))
}

/// The resident memory of this process, in bytes (Linux only; 0 elsewhere).
fn resident() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map_or(0, |pages| pages * 4096)
}

#[tokio::test]
#[ignore = "the read cost bar: writes gigabytes and runs for minutes; see the module's docs"]
async fn read_cost() {
    let sizes: Vec<u64> = std::env::var("READ_COST_SIZES")
        .unwrap_or_else(|_| "1000,150000,500000".into())
        .split(',')
        .map(|size| size.trim().parse().unwrap())
        .collect();
    let sealed: Vec<u64> = std::env::var("READ_COST_SEALED")
        .unwrap_or_else(|_| "150000".into())
        .split(',')
        .filter(|size| !size.trim().is_empty())
        .map(|size| size.trim().parse().unwrap())
        .collect();
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let mut templates = Vec::new();
    for (turns, kb) in [(1, 2), (3, 1), (2, 6), (4, 3)] {
        templates.push(template(turns, kb * 1024, &parts).await);
    }
    let scratch =
        broca_test_support::scratch_root().join(format!("read-cost-{}", std::process::id()));
    let mut out = String::new();
    let mut configs: Vec<(u64, bool)> = sizes.iter().map(|size| (*size, false)).collect();
    configs.extend(sealed.iter().map(|size| (*size, true)));
    for (size, sealed) in configs {
        let started = Instant::now();
        let built = build(
            &scratch.join(format!("{size}-{sealed}")),
            size,
            &templates,
            sealed,
        )
        .await;
        let written = started.elapsed();
        println!(
            "built {} messages (sealed: {sealed}) in {:.1} s",
            built.messages,
            written.as_secs_f64()
        );
        let section = measure(&built, &parts, sealed, written).await;
        println!("{section}");
        out.push_str(&section);
        if let Ok(path) = std::env::var("READ_COST_OUT") {
            std::fs::write(path, &out).unwrap();
        }
        let _ = std::fs::remove_dir_all(&built.root);
    }
}

/// The reads the cost bar measures, by name.
fn reads(built: &Built) -> Vec<(&'static str, StoreReadRequest, bool)> {
    vec![
        (
            "run.status, active run",
            StoreReadRequest::RunStatusReply(RunId(built.active.clone())),
            true,
        ),
        (
            "run.status, finished run (kept facts)",
            StoreReadRequest::RunStatusReply(RunId(built.finished.clone())),
            true,
        ),
        (
            "run.status, finished run (walked)",
            StoreReadRequest::RunStatusReply(RunId(built.finished.clone())),
            false,
        ),
        ("session.head", StoreReadRequest::Head, true),
        (
            "session.read, newest page",
            StoreReadRequest::Page(PageRequest::default()),
            true,
        ),
        (
            "entries page (newest 100)",
            StoreReadRequest::Entries {
                from: built.messages.saturating_sub(99).max(1),
                limit: 100,
            },
            true,
        ),
    ]
}

async fn measure(built: &Built, parts: &SharedParts, sealed: bool, written: Duration) -> String {
    // Fewer samples where each one is slow: a projection or a load of a long session takes
    // seconds.
    let (warm, slow, cold) = if built.messages > 10_000 {
        (50, 5, 3)
    } else {
        (500, 50, 20)
    };
    let backend = || LocalBackend::new(built.root.clone());
    let addr = built.addr.0.clone();
    let session_name = bind().session;
    let live = crate::store_reads::LiveStep {
        run_id: built.active.clone(),
        step: None,
    };
    // An active run's session is held by its actor, so every read counts the session held, as
    // the actor's own reads and those it hands back count it.
    fn context<'a>(
        parts: &'a SharedParts,
        addr: &'a str,
        session_name: &'a str,
        live: &'a crate::store_reads::LiveStep,
        open: bool,
        settled: &'a SettledRuns,
        stream_head: Option<Option<broca_core::Cursor>>,
    ) -> ReadContext<'a> {
        ReadContext {
            labels: Default::default(),
            token_counts: None,
            parts,
            held: true,
            open,
            settled: Some((settled, addr)),
            session_name,
            live_step: open.then_some(live),
            stream_head,
            entry_speaks_for: crate::store_entries::EntryAttribution::Known(false),
            class: None,
        }
    }
    let (session_triple, key) = (triple(), deployment());
    let load = || Session::view(backend(), built.addr.clone(), &session_triple, &key);
    let mut rows = Vec::new();

    // Open: the actor's own session, as the module opens it.
    let mut writer = Session::load(backend(), built.addr.clone(), &triple(), &deployment())
        .await
        .unwrap();
    // The actor seeds its live projection when it opens, in the background, reading the open
    // log from its checkpoint; its reads then take `session.read`'s head from that projection.
    let mut seedings = Vec::new();
    let mut seed = None;
    for _ in 0..slow.max(10) {
        let started = Instant::now();
        seed = Some(crate::store_fanout::measure_seed(&writer).await.unwrap());
        seedings.push(started.elapsed());
    }
    let (seed_bytes, stream_head) = seed.unwrap();
    let seeded = percentiles(seedings);
    // Kept whatever its size, to measure a read served from a kept view; whether the module's
    // own bound keeps a view this large is reported beside the table.
    let cache = ViewCache::new(4, u64::MAX);
    for (name, request, kept) in reads(built) {
        let settled = SettledRuns::default();
        let (mut held, mut total, mut stored) = (Vec::new(), Vec::new(), false);
        for round in 0..=warm {
            let walked = SettledRuns::default();
            let ctx = context(
                parts,
                &addr,
                &session_name,
                &live,
                true,
                if kept { &settled } else { &walked },
                Some(stream_head),
            );
            let started = Instant::now();
            let outcome = answer_in_memory(&mut writer, &ctx, &request).await;
            let held_for = started.elapsed();
            if let StoreReadOutcome::Stored { position, .. } = outcome {
                stored = true;
                let view = cache
                    .view(
                        &addr,
                        position,
                        "test",
                        Session::view_at(
                            backend(),
                            built.addr.clone(),
                            &triple(),
                            &deployment(),
                            &position,
                        ),
                    )
                    .await
                    .unwrap();
                std::hint::black_box(execute(&view, &ctx, &request).await);
            }
            if round > 0 {
                held.push(held_for);
                total.push(started.elapsed());
            }
        }
        rows.push((
            name,
            format!(
                "{}{}",
                percentiles(total),
                if stored {
                    " (handed back: read from a view at the actor's position)"
                } else {
                    ""
                }
            ),
            percentiles(held),
        ));
    }
    let notice_ctx = broca_session_loop::compaction::Ctx {
        session: Arc::default(),
        provider: "compactor".into(),
        preset: "default".into(),
        earlier: None,
        window: Default::default(),
    };
    let notice = |samples: &mut Vec<Duration>, started: Instant| samples.push(started.elapsed());
    let mut open_notice = Vec::new();
    for _ in 0..slow {
        let started = Instant::now();
        let transcript = broca_session_loop::compaction::transcript(&writer, &notice_ctx)
            .await
            .unwrap();
        std::hint::black_box(broca_core::compaction_v2::views::newest(
            &transcript.entries,
        ));
        notice(&mut open_notice, started);
    }
    writer.close().await.unwrap();

    // Closed, cached: the position probed, the kept view served.
    let mut cached_rows = Vec::new();
    // Kept whatever its size, to measure a read served from a kept view; whether the module's
    // own bound keeps a view this large is reported beside the table.
    let cache = ViewCache::new(4, u64::MAX);
    for (_, request, kept) in reads(built) {
        let settled = SettledRuns::default();
        let mut samples = Vec::new();
        for round in 0..=warm {
            let walked = SettledRuns::default();
            let ctx = context(
                parts,
                &addr,
                &session_name,
                &live,
                false,
                if kept { &settled } else { &walked },
                None,
            );
            let started = Instant::now();
            let position = Session::stored_position(&backend(), &built.addr, &deployment())
                .await
                .unwrap()
                .unwrap();
            let view = cache
                .view(
                    &addr,
                    position,
                    "test",
                    Session::view_at(
                        backend(),
                        built.addr.clone(),
                        &triple(),
                        &deployment(),
                        &position,
                    ),
                )
                .await
                .unwrap();
            std::hint::black_box(execute(&view, &ctx, &request).await);
            if round > 0 {
                samples.push(started.elapsed());
            }
        }
        cached_rows.push(percentiles(samples));
    }
    let mut cached_notice = Vec::new();
    {
        let position = Session::stored_position(&backend(), &built.addr, &deployment())
            .await
            .unwrap()
            .unwrap();
        for round in 0..=slow {
            let started = Instant::now();
            let view = cache
                .view(
                    &addr,
                    position,
                    "test",
                    Session::view_at(
                        backend(),
                        built.addr.clone(),
                        &triple(),
                        &deployment(),
                        &position,
                    ),
                )
                .await
                .unwrap();
            let transcript = broca_session_loop::compaction::transcript(&**view, &notice_ctx)
                .await
                .unwrap();
            std::hint::black_box(broca_core::compaction_v2::views::newest(
                &transcript.entries,
            ));
            if round > 0 {
                notice(&mut cached_notice, started);
            }
        }
    }
    drop(cache);

    // Closed, cold: a fresh load per read, as every read cost before views were kept.
    let mut cold_rows = Vec::new();
    for (_, request, _) in reads(built) {
        let mut samples = Vec::new();
        for _ in 0..cold {
            let walked = SettledRuns::default();
            let ctx = context(parts, &addr, &session_name, &live, false, &walked, None);
            let started = Instant::now();
            let view = load().await.unwrap();
            std::hint::black_box(execute(&view, &ctx, &request).await);
            samples.push(started.elapsed());
        }
        cold_rows.push(percentiles(samples));
    }
    let mut cold_notice = Vec::new();
    let mut loads = Vec::new();
    for _ in 0..cold {
        let started = Instant::now();
        let view = load().await.unwrap();
        loads.push(started.elapsed());
        let transcript = broca_session_loop::compaction::transcript(&*view, &notice_ctx)
            .await
            .unwrap();
        std::hint::black_box(broca_core::compaction_v2::views::newest(
            &transcript.entries,
        ));
        notice(&mut cold_notice, started);
    }

    // The resident memory one kept view takes, against its open log's length.
    let resident_view = view_memory(&built.root);
    let view: SessionView<LocalBackend> = load().await.unwrap();
    let log_len = view.position().len;
    let kept = view.kept_records();
    drop(view);

    let mut section = format!(
        "\n### {} messages{}\n\nWritten in {:.1} s. Open log: {:.1} MB, {} records kept by a view. \
         Cold load p50 / p99: {} ms. Resident memory of one loaded view: {:.1} MB \
         ({:.2} bytes per log byte). The module's view cache {} a view this large.\n\n\
         | read | open: total p50 / p99 ms | open: actor held p50 / p99 ms | closed, cached p50 / p99 ms | closed, cold p50 / p99 ms |\n\
         |---|---|---|---|---|\n",
        built.messages,
        if sealed { ", sealed at a cut about 1,000 messages from the end" } else { "" },
        written.as_secs_f64(),
        log_len as f64 / 1e6,
        kept,
        percentiles(loads),
        resident_view as f64 / 1e6,
        resident_view as f64 / log_len.max(1) as f64,
        if resident_view <= ViewCache::MAX_BYTES {
            "keeps"
        } else {
            "never keeps"
        },
    );
    for (((name, open, held), cached), cold) in rows.into_iter().zip(cached_rows).zip(cold_rows) {
        section.push_str(&format!(
            "| {name} | {open} | {held} | {cached} | {cold} |\n"
        ));
    }
    section.push_str(&format!(
        "| compaction notice body (transcript projection) | {} | {} | {} | {} |\n",
        percentiles(open_notice.clone()),
        percentiles(open_notice),
        percentiles(cached_notice),
        percentiles(cold_notice),
    ));
    section.push_str(&format!(
        "\nLive projection seeding at the actor's open (off the actor): {seeded} ms p50 / p99; \
         projector state held afterwards: about {:.1} KB.\n",
        seed_bytes as f64 / 1e3,
    ));
    section
}

/// The resident memory one view of the session under `root` takes, measured in a process of its
/// own: in this one, memory freed by earlier loads is reused by the next, which would hide most
/// of it.
fn view_memory(root: &std::path::Path) -> u64 {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "store_actor::read_cost::view_memory_of",
            "--ignored",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("READ_COST_MEMORY_OF", root)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    match stdout.lines().find_map(|line| {
        line.split("view_memory ")
            .nth(1)?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    }) {
        Some(bytes) => bytes,
        None => {
            println!(
                "the measuring process reported no memory: {}\nstdout: {stdout}\nstderr: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
            0
        }
    }
}

/// Loads the view [`view_memory`] asks for, and prints the resident memory it added.
#[tokio::test]
#[ignore = "run by the read cost bar in a process of its own"]
async fn view_memory_of() {
    let Ok(root) = std::env::var("READ_COST_MEMORY_OF") else {
        return;
    };
    let before = resident();
    let view = Session::view(
        LocalBackend::new(std::path::PathBuf::from(root)),
        SessionAddr(SESSION.into()),
        &triple(),
        &deployment(),
    )
    .await
    .unwrap();
    let after = resident();
    println!("view_memory {}", after.saturating_sub(before));
    drop(view);
}
