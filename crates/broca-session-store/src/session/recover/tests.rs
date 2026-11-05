use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;

use super::{Recovery, REMOVED_KEEP_MS};
use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::{Digest, Message as _};
use crate::head::SessionTriple;
use crate::keys::DeploymentKey;
use crate::record::{
    Assistant, BatchStart, CallOutcome, Input, Placement, Record, RunEnd, RunStart, StepStart,
    ToolIntent, ToolOutcome, ToolReply,
};
use crate::session::{Session, SessionError};

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "s".into(),
    }
}

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[5; 32])
}

fn addr() -> SessionAddr {
    SessionAddr("0123456789abcdef".into())
}

fn model() -> ModelHandle {
    serde_json::from_value(serde_json::json!({"provider_module_id": "p", "model_id": "m"})).unwrap()
}

fn start(run: &str) -> Record {
    Record::RunStart(Box::new(RunStart {
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
        render_config: None,
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
        run_id: run.into(),
        placement: Placement::RunInput,
        message: Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }]),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

fn assistant(run: &str, step: u64, content: Vec<ContentBlock>) -> Record {
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: step,
        provider_message_id: MessageId(format!("m-{run}-{step}")),
        message: Message::assistant_from(content, model()),
        usage: None,
        written_at_ms: None,
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

fn text(text: &str) -> ContentBlock {
    ContentBlock::Text { text: text.into() }
}

fn call(id: &str) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: "bash".into(),
        input: serde_json::json!({}),
        thought_signature: None,
    })
}

fn step(run: &str, step_id: u64) -> Record {
    Record::StepStart(StepStart {
        run_id: run.into(),
        step_id,
    })
}

fn dispatched(run: &str, id: &str) -> [Record; 2] {
    [
        Record::BatchStart(BatchStart {
            run_id: run.into(),
            batch_id: "b".into(),
            step_id: 1,
            planned: vec![ToolCallId(id.into())],
        }),
        Record::ToolIntent(ToolIntent {
            run_id: run.into(),
            batch_id: "b".into(),
            tool_call_id: ToolCallId(id.into()),
            tool_name: Some("bash".into()),
            module_id: None,
            args_digest: Digest([0; 32]),
            args: None,
            call_key: Some(format!("key-{id}")),
            approval_digest: None,
            dispatched_at_ms: None,
            schema_pin: None,
            hooks: None,
        }),
    ]
}

fn answered(run: &str, id: &str) -> [Record; 2] {
    [
        Record::ToolReply(ToolReply {
            run_id: run.into(),
            batch_id: Some("b".into()),
            result: ToolResult {
                tool_call_id: ToolCallId(id.into()),
                output: ToolOutput::Text {
                    text: "done".into(),
                },
                is_error: false,
            },
            finished_at_ms: None,
            tags_v1: None,
        }),
        Record::ToolOutcome(ToolOutcome {
            run_id: run.into(),
            batch_id: "b".into(),
            tool_call_id: ToolCallId(id.into()),
            outcome: CallOutcome::Completed,
            result_digest: Digest([0; 32]),
            is_error: false,
            reason: None,
            finished_at_ms: None,
            detail: None,
            hooks: None,
        }),
    ]
}

fn texts(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .flat_map(|m| &m.content)
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.clone()),
            ContentBlock::ToolResult(result) => Some(match &result.output {
                ToolOutput::Text { text } => text.clone(),
                other => format!("{other:?}"),
            }),
            _ => None,
        })
        .collect()
}

/// A finished run, then a run whose call was dispatched (all synced). Then, unsynced: the call's
/// result and outcome, and the next step's start. Returns the session and the offsets where the
/// unsynced result batch starts and ends.
async fn history<B: SessionBackend>(backend: &B) -> (Session<'_, B>, u64, u64) {
    let mut session = Session::create(backend, addr(), triple(), &deployment(), false)
        .await
        .unwrap();
    session.append(&[start("a")], true).await.unwrap();
    session
        .append(
            &[
                input("a", "ask a"),
                assistant("a", 1, vec![text("answer a")]),
            ],
            true,
        )
        .await
        .unwrap();
    session.append(&[end("a")], true).await.unwrap();
    session.append(&[start("b")], true).await.unwrap();
    session
        .append(
            &[
                input("b", "ask b"),
                step("b", 1),
                assistant("b", 1, vec![call("c1")]),
            ],
            true,
        )
        .await
        .unwrap();
    session.append(&dispatched("b", "c1"), true).await.unwrap();
    let from = session.open_log().1;
    session.append(&answered("b", "c1"), false).await.unwrap();
    let to = session.open_log().1;
    session.append(&[step("b", 2)], false).await.unwrap();
    (session, from, to)
}

fn damage(backend: &MemoryBackend, at: u64) {
    let (log, end) = block_on(async {
        let (stored, _) = backend.read_head(&addr()).await.unwrap().unwrap();
        let (head, _) = crate::head::open(&stored, &deployment()).unwrap();
        let log = head.open_log.unwrap().change;
        (log, backend.log_end(&addr(), &log).await.unwrap())
    });
    let mut bytes = block_on(backend.read_log(&addr(), &log, 0..end)).unwrap();
    bytes[at as usize] ^= 0x40;
    backend.set_log(&addr(), &log, bytes);
}

fn recover(backend: &MemoryBackend, now: u64) -> Result<Recovery, SessionError> {
    block_on(Session::recover(
        backend,
        addr(),
        &triple(),
        &deployment(),
        now,
    ))
}

fn load(backend: &MemoryBackend) -> Result<Session<'_, MemoryBackend>, SessionError> {
    block_on(Session::load(backend, addr(), &triple(), &deployment()))
}

/// A lost tool result and outcome: the call may have run, so its run ends interrupted, the call
/// gets `outcome_unknown` in both streams, the model is warned, and the transcript change is owed to
/// the compaction provider. The session then loads, takes appends, and reloads the same.
#[test]
fn a_lost_tool_result_closes_open_work_and_warns_the_model() {
    let backend = MemoryBackend::default();
    let (session, from, to) = block_on(history(&backend));
    drop(session);
    // Both frames of the batch: the transcript result and the ledger outcome.
    damage(&backend, from + 60);
    damage(&backend, to - 20);
    assert!(matches!(load(&backend), Err(SessionError::NeedsRecovery)));

    let Recovery::Recovered(recovered) = recover(&backend, 1_000).unwrap() else {
        panic!("nothing recovered");
    };
    assert!(recovered.loss_aware);
    assert_eq!(recovered.interrupted_runs, ["b"]);
    assert_eq!(recovered.unknown_calls, [ToolCallId("c1".into())]);
    assert_eq!(recovered.history_version, Some(1));

    let mut session = load(&backend).unwrap();
    assert!(session.head().next_seq > recovered.removed_through);
    let window = texts(&session.conversation.messages);
    assert_eq!(&window[..3], ["ask a", "answer a", "ask b"]);
    assert!(window[3].contains("outcome_unknown"), "{window:?}");
    assert!(
        window[4].starts_with("Storage damage removed records"),
        "{window:?}"
    );
    assert_eq!(window.len(), 5);
    let run = session.operations.run.as_ref().unwrap();
    assert_eq!(run.run_id, "b");
    assert_eq!(run.finished, Some(RunFinishReason::Interrupted));
    assert_eq!(session.operations.history_version, 1);
    assert_eq!(session.head().history_version, 1);
    // The next entry after every kept one is where the loss can change what was served.
    assert_eq!(session.operations.transcript_changed, Some(4));
    assert!(session.operations.unattributed_risk);
    let run_b = session
        .find_id(crate::idindex::IdKind::RunId, "b")
        .await_blocking()
        .unwrap();
    assert!(run_b.len() >= 2, "{run_b:?}");

    session
        .append(&[start("c")], true)
        .await_blocking()
        .unwrap();
    let state = (session.conversation.encode(), session.operations.encode());
    block_on(session.close()).unwrap();
    let reloaded = load(&backend).unwrap();
    assert_eq!(
        (reloaded.conversation.encode(), reloaded.operations.encode()),
        state
    );

    // The damaged bytes are kept a week for an operator, then deleted.
    let saved = format!("{}/removed", crate::codec::hex(&recovered.change.0));
    assert!(backend.object_names(&addr()).contains(&saved));
    let mut reloaded = reloaded;
    block_on(reloaded.collect(1_000 + REMOVED_KEEP_MS)).unwrap();
    assert!(!backend.object_names(&addr()).contains(&saved));
}

/// When only the transcript result was lost, the kept ledger outcome says the call finished:
/// its closing result says so, and it isn't `outcome_unknown`.
#[test]
fn a_kept_outcome_says_the_call_finished() {
    let backend = MemoryBackend::default();
    let (session, from, _) = block_on(history(&backend));
    drop(session);
    damage(&backend, from + 60);
    let Recovery::Recovered(recovered) = recover(&backend, 1_000).unwrap() else {
        panic!("nothing recovered");
    };
    assert!(recovered.unknown_calls.is_empty());
    assert_eq!(recovered.interrupted_runs, ["b"]);
    let session = load(&backend).unwrap();
    let window = texts(&session.conversation.messages);
    assert!(
        window[3].contains("finished, but storage damage removed its output"),
        "{window:?}"
    );
}

/// A kept transcript result whose ledger outcome was lost gets its outcome from the result. A
/// call answered before a ledger seal is settled: its outcome is sealed, not lost, even though
/// the open log still holds its message and result.
#[test]
fn only_outcomes_lost_with_the_log_are_derived() {
    let backend = MemoryBackend::default();
    let mut session = block_on(Session::create(
        &backend,
        addr(),
        triple(),
        &deployment(),
        false,
    ))
    .unwrap();
    block_on(async {
        session.append(&[start("a")], true).await.unwrap();
        session
            .append(
                &[
                    input("a", "ask a"),
                    step("a", 1),
                    assistant("a", 1, vec![call("c0")]),
                ],
                true,
            )
            .await
            .unwrap();
        session.append(&dispatched("a", "c0"), true).await.unwrap();
        session.append(&answered("a", "c0"), true).await.unwrap();
        session.append(&[end("a")], true).await.unwrap();
        session.seal(None, 500).await.unwrap();
        session.append(&[start("b")], true).await.unwrap();
        session
            .append(
                &[
                    input("b", "ask b"),
                    step("b", 1),
                    assistant("b", 1, vec![call("c1")]),
                ],
                true,
            )
            .await
            .unwrap();
        session.append(&dispatched("b", "c1"), true).await.unwrap();
    });
    let from = session.open_log().1;
    block_on(session.append(&answered("b", "c1"), false)).unwrap();
    let to = session.open_log().1;
    block_on(session.append(&[step("b", 2)], false)).unwrap();
    drop(session);
    // Only the ledger outcome's frame.
    damage(&backend, to - 20);
    let Recovery::Recovered(recovered) = recover(&backend, 1_000).unwrap() else {
        panic!("nothing recovered");
    };
    let _ = from;
    assert_eq!(recovered.derived_outcomes, [ToolCallId("c1".into())]);
    assert!(recovered.unknown_calls.is_empty());
}

/// Damage to the last batch loses the highest seqs the log held. The recovery's own records,
/// and every later append, take seqs above all of them.
#[test]
fn seqs_after_a_recovery_are_above_everything_the_damage_removed() {
    let backend = MemoryBackend::default();
    let (session, _, to) = block_on(history(&backend));
    let written_through = session.encoder.next_seq() - 1;
    drop(session);
    damage(&backend, to + 60);
    let Recovery::Recovered(recovered) = recover(&backend, 1_000).unwrap() else {
        panic!("nothing recovered");
    };
    assert!(
        recovered.removed_through >= written_through,
        "{} < {written_through}",
        recovered.removed_through
    );
    let mut session = load(&backend).unwrap();
    let first = block_on(session.append(&[start("c")], true)).unwrap();
    assert!(first > recovered.removed_through);
}

/// Transcript records after the first one that may be missing are dropped, even though they
/// authenticate: a conversation never skips a message.
#[test]
fn transcript_records_after_a_possible_loss_are_dropped() {
    let backend = MemoryBackend::default();
    let (mut session, from, _) = block_on(history(&backend));
    block_on(session.append(&[input("b", "after the loss")], false)).unwrap();
    drop(session);
    damage(&backend, from + 60);
    let Recovery::Recovered(recovered) = recover(&backend, 1_000).unwrap() else {
        panic!("nothing recovered");
    };
    assert_eq!(recovered.transcript_dropped, 1);
    let loaded = load(&backend).unwrap();
    assert!(!texts(&loaded.conversation.messages)
        .iter()
        .any(|text| text == "after the loss"));
}

/// Damage in bytes a barrier acknowledged is corruption: loading and recovery refuse, and
/// nothing changes.
#[test]
fn damage_to_acknowledged_bytes_refuses_and_changes_nothing() {
    let backend = MemoryBackend::default();
    let (session, from, _) = block_on(history(&backend));
    drop(session);
    damage(&backend, from - 20);
    let logs = backend.log_names(&addr());
    assert!(matches!(load(&backend), Err(SessionError::Corrupt { .. })));
    assert!(matches!(
        recover(&backend, 1_000),
        Err(SessionError::Corrupt { .. })
    ));
    assert_eq!(backend.log_names(&addr()), logs);
    assert!(backend.object_names(&addr()).is_empty());
}

/// A crash at any point of a recovery leaves the session needing recovery, or recovered. Either
/// way, recovering again and loading gives the same conversation and run state as a recovery
/// that never crashed, with nothing left of the crashed attempt.
#[test]
fn a_crash_at_any_point_of_a_recovery_recovers_again() {
    let expected = {
        let backend = MemoryBackend::default();
        let (session, from, _) = block_on(history(&backend));
        drop(session);
        damage(&backend, from + 60);
        recover(&backend, 1_000).unwrap();
        let session = load(&backend).unwrap();
        (
            texts(&session.conversation.messages),
            session.operations.run.as_ref().unwrap().finished,
        )
    };
    let mut crashed = 0;
    for crash in 0..30 {
        let backend = MemoryBackend::default();
        let (session, from, _) = block_on(history(&backend));
        drop(session);
        damage(&backend, from + 60);
        backend.crash_after(Some(crash));
        if recover(&backend, 1_000).is_err() {
            crashed += 1;
        }
        backend.crash_after(None);
        recover(&backend, 1_000).unwrap();
        let session = load(&backend).unwrap();
        let got = (
            texts(&session.conversation.messages),
            session.operations.run.as_ref().unwrap().finished,
        );
        assert_eq!(got, expected, "crash {crash}");
        // Only the committed recovery's objects remain: its segment, directory pages, index and
        // saved bytes, all under one change.
        let changes: std::collections::BTreeSet<String> = backend
            .object_names(&addr())
            .iter()
            .map(|name| crate::backend::object_change(name).to_string())
            .collect();
        assert_eq!(changes.len(), 1, "crash {crash}: {changes:?}");
    }
    assert!(
        crashed >= 5,
        "only {crashed} crash points reached the recovery"
    );
}

trait Blocking: std::future::Future + Sized {
    fn await_blocking(self) -> Self::Output {
        block_on(self)
    }
}

impl<F: std::future::Future> Blocking for F {}
