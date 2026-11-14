//! The two-path load check over synthetic sessions: every shape a session's history takes
//! (seals, a cut, a revert, a recovery cut, a torn tail, debris, an enrollment, and the records
//! only native runs write), in both cipher modes, loads alike; a planted difference is named by
//! its field; and a store's encrypted sessions are listed, not checked, without the key.

use broca_protocol::caller::{PauseReason, RunFinishReason};
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use broca_session_store::backend::{LocalBackend, MemoryBackend, SessionAddr, SessionBackend};
use broca_session_store::codec::Digest;
use broca_session_store::head::SessionTriple;
use broca_session_store::idindex::IdKind;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::record::{
    Assistant, BatchEnd, BatchStart, CallOutcome, CompactionEnrolled, EnrolledEntries, EntryFacts,
    Event, Input, Pause, Placement, QueuedSend, Record, Resumed, RunEnd, RunStart, Steer, StepEnd,
    StepStart, ToolHeld, ToolIntent, ToolOutcome, ToolReply, Warmed,
};
use broca_session_store::session::{Carry, Recovery, Reverted, Session, SessionError};
use futures::executor::block_on;

use super::{check, check_with, copy_from_memory, copy_from_root, Outcome, Report, SessionCheck};

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "s".into(),
    }
}

/// The test deployment key, creating sessions in mode `cipher`.
fn deployment(cipher: Cipher) -> DeploymentKey {
    DeploymentKey::new(1, &[5; 32]).creating(cipher)
}

fn addr() -> SessionAddr {
    SessionAddr("0123456789abcdef".into())
}

fn model() -> ModelHandle {
    serde_json::from_value(serde_json::json!({"provider_module_id": "p", "model_id": "m"})).unwrap()
}

fn start(run: &str) -> Record {
    Record::RunStart(Box::new(RunStart {
        named_speaker: None,
        imported_ids: None,
        message_marks: None,
        run_id: run.into(),
        model: model(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: None,
        written_at_ms: None,
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: None,
        refusal_schedule: None,
    }))
}

fn end(run: &str) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run.into(),
        reason: RunFinishReason::Completed,
        usage: None,
        ts_ms: None,
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

fn input(run: &str, text: &str) -> Record {
    Record::Input(Input {
        media: Vec::new(),
        run_id: run.into(),
        placement: Placement::RunInput,
        message: std::sync::Arc::new(Message::new(
            Role::User,
            vec![ContentBlock::Text { text: text.into() }],
        )),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

fn event(name: &str) -> Record {
    Record::Event(Event {
        name: name.into(),
        run_id: None,
        body: None,
    })
}

/// A run's start, sent as the queued send `submission` when one is given.
fn sent_start(run: &str, submission: Option<&str>) -> Record {
    let mut record = start(run);
    if let Record::RunStart(start) = &mut record {
        start.submission_id = submission.map(str::to_owned);
    }
    record
}

/// A send queued while a run runs, which the next run starts from.
fn queued(submission: &str) -> Record {
    Record::QueuedSend(QueuedSend {
        submission_id: submission.into(),
        send_id: Some(format!("send-{submission}")),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: format!("queued {submission}"),
            }],
        )],
        setup: Vec::new(),
        written_at_ms: None,
        ts_ms: None,
        request: None,
        receipt: None,
        delivery: None,
        media: Vec::new(),
        message_marks: None,
        named_speaker: None,
    })
}

/// The entry facts of the message at `ordinal`.
fn facts(ordinal: u64) -> Record {
    Record::EntryFacts(std::sync::Arc::new(EntryFacts {
        send_id: None,
        named_speaker: None,
        client_message_id: None,
        sender: None,
        marks: None,
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        runner_tag_base: Some(ordinal),
        legacy_view_seq: None,
        ordinal,
        written_at_ms: Some(ordinal),
        tag_base: ordinal,
        notice: None,
        usage: None,
        generated_under: None,
        native: true,
        render_tags: false,
        clean_reply: false,
        mid: format!("mfeedface.{}", ordinal - 1),
        run_id: None,
        created_at_ms: None,
        delivery_mode: None,
        delivery_tool_call_id: None,
        notices: Vec::new(),
    }))
}

fn assistant(run: &str, step: u64, content: Vec<ContentBlock>) -> Record {
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: step,
        provider_message_id: MessageId(format!("{run}-m{step}")),
        message: std::sync::Arc::new(Message::assistant_from(content, model())),
        usage: None,
        written_at_ms: None,
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

fn step_start(run: &str, step: u64) -> Record {
    Record::StepStart(StepStart {
        run_id: run.into(),
        step_id: step,
        mid: None,
    })
}

fn step_end(run: &str, step: u64, calls: u32) -> Record {
    Record::StepEnd(StepEnd {
        run_id: run.into(),
        step_id: step,
        usage: None,
        finish_reason: None,
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
        call_count: calls,
        fallback: None,
        hooks: None,
        budget_finalize: None,
        total_usage: None,
        stop_met: None,
        limiting_stop_met: None,
        by_model: Vec::new(),
    })
}

/// The call key of run `run`'s call in step `step`.
fn call_key(run: &str, step: u64) -> String {
    format!("key-{run}-{step}")
}

/// One tool step of run `run`: the step, the assistant's call with its facts, the batch with the
/// call's intent and outcome, and the call's reply.
fn tool_step(run: &str, step: u64, ordinal: &mut u64) -> Vec<Vec<Record>> {
    let call = format!("{run}-c{step}");
    let batch = format!("{run}-b{step}");
    *ordinal += 1;
    let assistant_ordinal = *ordinal;
    // The tool-results message takes the next ordinal; it carries no facts of its own here.
    *ordinal += 1;
    let tool_call: ContentBlock = serde_json::from_value(serde_json::json!({
        "type": "tool_call", "tool_call_id": call, "tool_name": "read",
        "input": {"path": format!("/src/{call}.rs"), "limit": 200}
    }))
    .unwrap();
    let reply = Record::ToolReply(ToolReply {
        run_id: run.into(),
        batch_id: Some(batch.clone()),
        result: serde_json::from_value(serde_json::json!({
            "tool_call_id": call,
            "output": {"kind": "text", "text": format!("{call}: {}", "x".repeat(1500))}
        }))
        .unwrap(),
        finished_at_ms: None,
        tags_v1: None,
        media: Vec::new(),
    });
    vec![
        vec![
            step_start(run, step),
            event(&format!("{run}-started-{step}")),
        ],
        vec![
            assistant(run, step, vec![tool_call]),
            facts(assistant_ordinal),
            step_end(run, step, 1),
        ],
        vec![
            Record::BatchStart(BatchStart {
                run_id: run.into(),
                batch_id: batch.clone(),
                step_id: step,
                planned: vec![ToolCallId(call.clone())],
                mid: None,
            }),
            Record::ToolIntent(ToolIntent {
                run_id: run.into(),
                batch_id: batch.clone(),
                tool_call_id: ToolCallId(call.clone()),
                tool_name: Some("read".into()),
                module_id: Some("tools".into()),
                args_digest: Digest([7; 32]),
                args: None,
                call_key: Some(call_key(run, step)),
                approval_digest: None,
                dispatched_at_ms: Some(step),
                schema_pin: None,
                hooks: None,
                key_derived: None,
            }),
            event(&format!("{run}-dispatched-{step}")),
        ],
        vec![
            Record::ToolOutcome(ToolOutcome {
                run_id: run.into(),
                batch_id: batch.clone(),
                tool_call_id: ToolCallId(call),
                outcome: CallOutcome::Completed,
                result_digest: Digest([9; 32]),
                is_error: false,
                reason: None,
                finished_at_ms: Some(step),
                detail: None,
                hooks: None,
                call_key: None,
            }),
            reply,
            Record::BatchEnd(BatchEnd {
                run_id: run.into(),
                batch_id: batch,
            }),
            event(&format!("{run}-progress-{step}")),
        ],
    ]
}

/// Appends run `run` of `steps` tool steps and an answer: started from the queued send
/// `submission` when one is given, queueing `queue` after its first step, and ended when `end`.
/// `ordinal` is the last message placed so far.
async fn tool_run<B: SessionBackend>(
    session: &mut Session<B>,
    run: &str,
    steps: u64,
    ordinal: &mut u64,
    submission: Option<&str>,
    queue: Option<&str>,
    end: bool,
) {
    session
        .append(&[sent_start(run, submission)], true)
        .await
        .unwrap();
    *ordinal += 1;
    session
        .append(&[input(run, &format!("ask {run}")), facts(*ordinal)], true)
        .await
        .unwrap();
    for step in 1..=steps {
        for batch in tool_step(run, step, ordinal) {
            session.append(&batch, true).await.unwrap();
        }
        if step == 1 {
            if let Some(queue) = queue {
                session.append(&[queued(queue)], true).await.unwrap();
            }
        }
    }
    let answer = steps + 1;
    *ordinal += 1;
    session
        .append(&[step_start(run, answer)], true)
        .await
        .unwrap();
    session
        .append(
            &[
                assistant(
                    run,
                    answer,
                    vec![ContentBlock::Text {
                        text: format!("answer {run}"),
                    }],
                ),
                facts(*ordinal),
                step_end(run, answer, 0),
            ],
            true,
        )
        .await
        .unwrap();
    if end {
        session.append(&[end_of(run)], true).await.unwrap();
    }
}

fn end_of(run: &str) -> Record {
    end(run)
}

/// A session of `runs` runs of `steps` tool steps each, every run after the first started from a
/// send queued during the run before it, the last one still running. Returns the session and the
/// messages it placed.
async fn tool_history<B: SessionBackend>(
    backend: B,
    cipher: Cipher,
    runs: usize,
    steps: u64,
) -> (Session<B>, u64) {
    let mut session = Session::create(backend, addr(), triple(), &deployment(cipher), false)
        .await
        .unwrap();
    let mut ordinal = 0;
    for index in 0..runs {
        let run = format!("r{index}");
        let submission = (index > 0).then(|| format!("sub-r{index}"));
        let queue = (index + 1 < runs).then(|| format!("sub-r{}", index + 1));
        tool_run(
            &mut session,
            &run,
            steps,
            &mut ordinal,
            submission.as_deref(),
            queue.as_deref(),
            index + 1 < runs,
        )
        .await;
    }
    (session, ordinal)
}

/// A steer sent into the running run.
fn steer(text: &str) -> Record {
    Record::Steer(Steer {
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: text.into() }],
        )],
        written_at_ms: Some(10),
        tags_v1: None,
        receipt: Some(serde_json::json!({"send_id": format!("steer-{text}")}).into()),
        media: Vec::new(),
        message_marks: None,
        named_speaker: None,
    })
}

/// An idle warm-up of run `run`'s step `step`.
fn warmed(run: &str, step: u64) -> Record {
    Record::Warmed(Warmed {
        run_id: run.into(),
        step_id: step,
        trigger: Some("keep_warm".into()),
        request_body_sha256: Digest([1; 32]),
        warm_body_sha256: Digest([2; 32]),
        usage: Default::default(),
        served_account_ref: None,
        sent_at_ms: 20,
        ttl_ms: Some(300_000),
        expires_at_ms: None,
        attempt_id: None,
        billing: None,
        ratelimit_headers: None,
        reached_cache: Some(true),
        requested_cache_ttl_ms: None,
    })
}

/// Run `run`'s step `step` with its one call held by its provider, then answered late.
fn held_step(run: &str, step: u64, ordinal: &mut u64) -> Vec<Vec<Record>> {
    let mut batches = tool_step(run, step, ordinal);
    let call = format!("{run}-c{step}");
    batches[2].push(Record::ToolHeld(ToolHeld {
        batch_id: format!("{run}-b{step}"),
        tool_call_id: ToolCallId(call),
        call_key: call_key(run, step),
        module: "tools".into(),
        reason: "held".into(),
        detail_digest: None,
        held_at_ms: 30,
        sequential: false,
    }));
    batches
}

/// An account-limit pause of run `run`, and its resumption.
fn paused_and_resumed(run: &str, pause_seq: u64) -> [Record; 2] {
    [
        Record::Pause(Pause {
            run_id: run.into(),
            reason: PauseReason::AccountLimit,
            ts_ms: Some(40),
            error: None,
            account_limit: Some(serde_json::json!({"resets_at_ms": 50}).into()),
        }),
        Record::Resumed(Resumed {
            run_id: run.into(),
            resumed_from_seq: pause_seq,
            messages: 0,
            model_steps: 0,
            tool_results: 0,
            redone: Vec::new(),
            indeterminate: Vec::new(),
        }),
    ]
}

/// The record kinds the converter never writes from a legacy log, which only a native session
/// holds, and the native shapes of kinds it does write.
const NATIVE_KINDS: &[&str] = &[
    "WriterOpened",
    "ChangeIntent",
    "ChangeAborted",
    "TranscriptReverted",
    "RecoveryCut",
    "CompactionEnrolled",
    "Steer",
    "Warmed",
    "ToolHeld",
    "Pause",
    "Resumed",
];

fn kind_of(record: &Record) -> String {
    let debug = format!("{record:?}");
    debug.split('(').next().unwrap_or_default().to_owned()
}

/// A session whose history holds every shape the check must load alike, built as a writer
/// writes it: an enrollment in the current compaction contract at its first run, a send queued
/// from it, a ledger seal, a cut, a revert, a running run with a steer, an idle warm-up, a held
/// call answered late and an account-limit pause and resumption, a recovery of the debris a
/// crash left, a seal that crashed after its intent and was undone by the next load, and a run
/// still running after all of it. Returns the backend.
fn native_history(cipher: Cipher) -> (MemoryBackend, Vec<Snapshot>) {
    let backend = MemoryBackend::default();
    let mut snapshots = Vec::new();
    block_on(async {
        let deployment = deployment(cipher);
        let mut session = Session::create(&backend, addr(), triple(), &deployment, false)
            .await
            .unwrap();
        let mut ordinal = 0;

        // The first run enrolls, with no earlier message to decide about.
        let overlay = session
            .put_enrollment(&EnrolledEntries {
                entries: Vec::new(),
            })
            .await
            .unwrap();
        session.append(&[start("r0")], true).await.unwrap();
        ordinal += 1;
        session
            .append(
                &[
                    input("r0", "ask r0"),
                    facts(ordinal),
                    Record::CompactionEnrolled(CompactionEnrolled {
                        through_ordinal: 0,
                        tag_floor: 1,
                        tag_high: 0,
                        overlay,
                    }),
                ],
                true,
            )
            .await
            .unwrap();
        for step in 1..=2 {
            for batch in tool_step("r0", step, &mut ordinal) {
                session.append(&batch, true).await.unwrap();
            }
        }
        session.append(&[queued("sub-r1")], true).await.unwrap();
        session.append(&[end("r0")], true).await.unwrap();
        snapshots.push(snapshot("enrolled", &backend, cipher).await);
        session.seal(None, 1_000).await.unwrap();

        tool_run(
            &mut session,
            "r1",
            2,
            &mut ordinal,
            Some("sub-r1"),
            None,
            true,
        )
        .await;
        session
            .seal_at_entry(ordinal - 1, &Carry::default(), 2_000)
            .await
            .unwrap();

        // A run, reverted to its input and sent again.
        tool_run(&mut session, "r2", 1, &mut ordinal, None, None, true).await;
        // The run's input took the ordinal before its call, its results and its answer.
        let target = format!("mfeedface.{}", ordinal - 4);
        let mut resend = sent_start("r2b", None);
        if let Record::RunStart(start) = &mut resend {
            start.send_id = Some("send-r2b".into());
        }
        let reverted = session
            .revert(
                &target,
                vec![resend, input("r2b", "ask again"), facts(1)],
                session.operations.on_contract().then_some(100),
                3_000,
            )
            .await
            .unwrap();
        let Reverted::Applied {
            ordinal: placed, ..
        } = reverted
        else {
            panic!("the revert applied")
        };
        ordinal = placed;

        // The running run: a steer, an idle warm-up, a held call, a pause and its resumption.
        for batch in tool_step("r2b", 1, &mut ordinal) {
            session.append(&batch, true).await.unwrap();
        }
        session.append(&[steer("hurry")], true).await.unwrap();
        session.append(&[warmed("r2b", 1)], true).await.unwrap();
        for batch in held_step("r2b", 2, &mut ordinal) {
            session.append(&batch, true).await.unwrap();
        }
        let pause_seq = session.next_seq();
        session
            .append(&paused_and_resumed("r2b", pause_seq), true)
            .await
            .unwrap();

        snapshots.push(snapshot("reverted and running", &backend, cipher).await);

        // A step written without a barrier, damaged before it was synced: debris, recovered.
        let from = session.open_log().1;
        for batch in tool_step("r2b", 3, &mut ordinal) {
            session.append(&batch, false).await.unwrap();
        }
        drop(session);
        flip(&backend, from + 40, cipher).await;
        assert!(matches!(
            Session::load(&backend, addr(), &triple(), &deployment).await,
            Err(SessionError::NeedsRecovery)
        ));
        let recovered = Session::recover(&backend, addr(), &triple(), &deployment, 4_000).await;
        assert!(
            matches!(recovered, Ok(Recovery::Recovered(_))),
            "the debris was recovered"
        );
        snapshots.push(snapshot("recovered", &backend, cipher).await);
        let mut session = Session::load(&backend, addr(), &triple(), &deployment)
            .await
            .unwrap();
        ordinal = session.conversation.placed;
        if session
            .operations
            .run
            .as_ref()
            .is_some_and(|run| run.finished.is_none())
        {
            session.append(&[end("r2b")], true).await.unwrap();
        }

        // A seal that crashes after its intent: the next load undoes it.
        backend.crash_after(Some(1));
        assert!(session.seal(None, 5_000).await.is_err());
        backend.crash_after(None);
        drop(session);
        let mut session = Session::load(&backend, addr(), &triple(), &deployment)
            .await
            .unwrap();

        // A run after all of it, still running.
        tool_run(&mut session, "r3", 2, &mut ordinal, None, None, false).await;
        snapshots.push(snapshot("after all of it", &backend, cipher).await);
    });
    (backend, snapshots)
}

/// One check of [`native_history`] as it stood at a point: its name, the check, and the record
/// kinds of the open log there and of its whole history.
type Snapshot = (
    &'static str,
    SessionCheck,
    std::collections::BTreeSet<String>,
    std::collections::BTreeSet<String>,
);

/// Checks the session as `backend` holds it now, a copy taken so the writer working on it goes
/// on unaffected.
async fn snapshot(name: &'static str, backend: &MemoryBackend, cipher: Cipher) -> Snapshot {
    let copy = copy_from_memory(backend, &addr()).await.unwrap();
    let view = Session::view(&copy, addr(), &triple(), &deployment(cipher))
        .await
        .unwrap();
    let open = view
        .open_log_records()
        .await
        .unwrap()
        .iter()
        .map(|(_, record)| kind_of(record))
        .collect();
    let history = view
        .records_from(0)
        .await
        .unwrap()
        .iter()
        .map(|(_, record)| kind_of(record))
        .collect();
    drop(view);
    (
        name,
        check(&copy, &addr(), &deployment(cipher)).await,
        open,
        history,
    )
}

/// Flips one bit of the open log's byte `at`.
async fn flip(backend: &MemoryBackend, at: u64, cipher: Cipher) {
    let (stored, _) = backend.read_head(&addr()).await.unwrap().unwrap();
    let (head, _) = broca_session_store::head::open(&stored, &deployment(cipher)).unwrap();
    let log = head.open_log.unwrap().change;
    let end = backend.log_end(&addr(), &log).await.unwrap();
    let mut bytes = backend.read_log(&addr(), &log, 0..end).await.unwrap();
    bytes[at as usize] ^= 0x40;
    backend.set_log(&addr(), &log, bytes);
}

/// Cuts the open log back by `by` bytes, as a write cut short leaves it.
fn torn(backend: &MemoryBackend, by: u64, cipher: Cipher) {
    block_on(async {
        let (stored, _) = backend.read_head(&addr()).await.unwrap().unwrap();
        let (head, _) = broca_session_store::head::open(&stored, &deployment(cipher)).unwrap();
        let log = head.open_log.unwrap().change;
        let end = backend.log_end(&addr(), &log).await.unwrap();
        let bytes = backend.read_log(&addr(), &log, 0..end - by).await.unwrap();
        backend.set_log(&addr(), &log, bytes);
    });
}

fn checked(backend: &MemoryBackend, deployment: &DeploymentKey) -> SessionCheck {
    block_on(async {
        let copy = copy_from_memory(backend, &addr()).await.unwrap();
        check(&copy, &addr(), deployment).await
    })
}

fn assert_agrees(check: &SessionCheck, what: &str) {
    assert_eq!(check.outcome, Outcome::Opened, "{what}");
    assert!(
        check.differences.is_empty(),
        "{what}: {:?}",
        check.differences
    );
    assert!(check.ids_looked_up > 0, "{what}: ids looked up");
    assert!(
        check.run_starts_looked_up > 0,
        "{what}: run starts looked up"
    );
    assert!(
        check.ledger_records_looked_up > 0,
        "{what}: ledger records looked up"
    );
    assert_eq!(check.surfaces_compared, 2, "{what}: surfaces compared");
}

/// Every native shape loads alike through the two paths, encrypted and plain: the session is
/// checked at each point of its history, and every record kind the converter never writes is in
/// the open log the load folds at one of those checks. A revert's `TranscriptReverted` and a
/// recovery's `RecoveryCut` are the exception by design: only the change's own sealed ledger
/// segment holds them, and the checkpoint of the log the change creates carries what they fold
/// to, which both paths read alike. They must be in the history, and the checks after them agree.
#[test]
fn a_native_history_loads_alike_in_both_modes() {
    const SEALED_ONLY: [&str; 2] = ["TranscriptReverted", "RecoveryCut"];
    for cipher in Cipher::ALL {
        let (_, snapshots) = native_history(cipher);
        let mut folded = std::collections::BTreeSet::new();
        for (name, check, open, _) in &snapshots {
            assert_agrees(check, &format!("{}, {name}", cipher.label()));
            let native: Vec<&&str> = NATIVE_KINDS
                .iter()
                .filter(|kind| open.contains(**kind))
                .collect();
            println!(
                "{}, {name}: native kinds in the open log: {native:?}",
                cipher.label()
            );
            folded.extend(open.iter().cloned());
        }
        let (_, _, _, history) = snapshots.last().unwrap();
        for kind in NATIVE_KINDS {
            if SEALED_ONLY.contains(kind) {
                assert!(
                    history.contains(*kind),
                    "{}: the history holds {kind}",
                    cipher.label()
                );
                assert!(
                    !folded.contains(*kind),
                    "{}: no open log holds {kind}",
                    cipher.label()
                );
            } else {
                assert!(
                    folded.contains(*kind),
                    "{}: an open log the check folded holds {kind}",
                    cipher.label()
                );
            }
        }
    }
}

/// A log ending in a torn tail opens alike on both paths; debris refuses alike; neither is a
/// difference.
#[test]
fn a_torn_tail_and_debris_load_alike() {
    for cipher in Cipher::ALL {
        let backend = MemoryBackend::default();
        let (mut session, mut ordinal) = block_on(tool_history(&backend, cipher, 3, 2));
        let from = session.open_log().1;
        block_on(async {
            for batch in tool_step("r2", 3, &mut ordinal) {
                session.append(&batch, false).await.unwrap();
            }
        });
        let written = session.open_log().1 - from;
        drop(session);

        torn(&backend, written / 3, cipher);
        assert_agrees(&checked(&backend, &deployment(cipher)), "torn tail");

        block_on(flip(&backend, from + 40, cipher));
        let debris = checked(&backend, &deployment(cipher));
        assert_eq!(debris.outcome, Outcome::FailedAlike("NeedsRecovery".into()));
        assert!(debris.differences.is_empty(), "{:?}", debris.differences);
    }
}

/// A difference planted in the reference is reported under the field it touches, for both
/// paths, and makes the report fail.
#[test]
fn a_planted_difference_is_named_by_its_field() {
    let backend = MemoryBackend::default();
    drop(block_on(tool_history(
        &backend,
        Cipher::XChaCha20Poly1305,
        3,
        2,
    )));
    let check = block_on(async {
        let copy = copy_from_memory(&backend, &addr()).await.unwrap();
        // The reference forgets one call key's record, as an index that lost it would.
        check_with(
            &copy,
            &addr(),
            &deployment(Cipher::XChaCha20Poly1305),
            &|reference| {
                let key = reference
                    .ids
                    .ids
                    .keys()
                    .find(|(kind, _)| *kind == IdKind::CallKey)
                    .cloned()
                    .unwrap();
                reference.ids.ids.remove(&key);
            },
        )
        .await
    });
    let fields: std::collections::BTreeSet<&str> = check
        .differences
        .iter()
        .map(|(field, _)| field.as_str())
        .collect();
    assert_eq!(
        fields,
        ["view.find_id", "writer.find_id"].into_iter().collect(),
        "{:?}",
        check.differences
    );
    let mut report = Report::default();
    report.add("planted", check);
    assert!(!report.passed());
    assert_eq!(report.differed, 1);
    assert_eq!(report.differences["view.find_id"], 1);
    assert!(report.difference_examples["writer.find_id"][0].starts_with("planted: CallKey #"));
}

/// A store on disk: its plain session is copied out and checked whole, its encrypted one is
/// listed as needing the key and never counted as agreeing, and with the key it agrees too.
/// Nothing under the store changes.
#[test]
fn a_stores_encrypted_sessions_wait_for_the_key() {
    let root =
        broca_test_support::scratch_root().join(format!("store-load-check-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let addrs = [
        SessionAddr("plainplainplain0".into()),
        SessionAddr("encryptedsession".into()),
    ];
    block_on(async {
        for (addr, cipher) in addrs.iter().zip([Cipher::None, Cipher::XChaCha20Poly1305]) {
            let mut session = Session::create(
                LocalBackend::new(&root),
                addr.clone(),
                triple(),
                &deployment(cipher),
                false,
            )
            .await
            .unwrap();
            let mut ordinal = 0;
            tool_run(
                &mut session,
                "r0",
                2,
                &mut ordinal,
                None,
                Some("sub-r1"),
                true,
            )
            .await;
            session.seal(None, 1_000).await.unwrap();
            tool_run(
                &mut session,
                "r1",
                2,
                &mut ordinal,
                Some("sub-r1"),
                None,
                false,
            )
            .await;
        }
    });
    let before = tree(&root);
    let check_store = |deployment: &DeploymentKey| {
        let mut report = Report::default();
        for addr in super::stored_sessions(&root).unwrap() {
            let copy = copy_from_root(&root, &addr).unwrap();
            report.add(&addr.0, block_on(check(&copy, &addr, deployment)));
        }
        report
    };
    let keyless = check_store(&DeploymentKey::unloaded(1));
    assert_eq!(keyless.sessions, 2);
    assert_eq!(keyless.agreed, 1);
    assert_eq!(
        keyless.key_unavailable_sessions,
        vec!["encryptedsession".to_owned()]
    );
    assert!(keyless.passed());
    let keyed = check_store(&deployment(Cipher::XChaCha20Poly1305));
    assert_eq!((keyed.agreed, keyed.key_unavailable), (2, 0), "{keyed:?}");
    assert_eq!(tree(&root), before, "the store is read, never written");
    let _ = std::fs::remove_dir_all(&root);
}

/// Every file under `root` with its bytes.
fn tree(root: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut files = Vec::new();
    let mut folders = vec![root.to_path_buf()];
    while let Some(folder) = folders.pop() {
        for entry in std::fs::read_dir(&folder).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                folders.push(path);
            } else {
                files.push((path.clone(), std::fs::read(&path).unwrap()));
            }
        }
    }
    files.sort();
    files
}
