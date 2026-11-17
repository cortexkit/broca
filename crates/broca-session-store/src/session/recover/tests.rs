use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;

use super::{Recovery, REMOVED_KEEP_MS};
use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::{Digest, Message as _};
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{
    Assistant, BatchStart, CallOutcome, Input, Placement, Record, RunEnd, RunStart, StepStart,
    ToolIntent, ToolOutcome, ToolReply,
};
use crate::session::{Session, SessionError};

cipher_tests!(
    a_lost_tool_result_closes_open_work_and_warns_the_model,
    a_kept_outcome_says_the_call_finished,
    only_outcomes_lost_with_the_log_are_derived,
    seqs_after_a_recovery_are_above_everything_the_damage_removed,
    transcript_records_after_a_possible_loss_are_dropped,
    damage_to_acknowledged_bytes_refuses_and_changes_nothing,
    a_crash_at_any_point_of_a_recovery_recovers_again,
    a_damaged_checkpoint_is_rebuilt_and_loses_nothing,
    a_damaged_checkpoint_without_its_copy_refuses,
    a_rebuilt_checkpoint_keeps_frozen_and_post_move_activity,
);

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

fn assistant(run: &str, step: u64, content: Vec<ContentBlock>) -> Record {
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: step,
        provider_message_id: MessageId(format!("m-{run}-{step}")),
        message: std::sync::Arc::new(Message::assistant_from(content, model())),
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
        input: serde_json::json!({}).into(),
        thought_signature: None,
    })
}

fn step(run: &str, step_id: u64) -> Record {
    Record::StepStart(StepStart {
        run_id: run.into(),
        step_id,
        mid: None,
    })
}

fn dispatched(run: &str, id: &str) -> [Record; 2] {
    [
        Record::BatchStart(BatchStart {
            run_id: run.into(),
            batch_id: "b".into(),
            step_id: 1,
            planned: vec![ToolCallId(id.into())],
            mid: None,
            shown_tags: Vec::new(),
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
            key_derived: None,
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
            media: Vec::new(),
        }),
        Record::ToolOutcome(ToolOutcome {
            yielded_by: None,
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
            call_key: None,
        }),
    ]
}

fn texts(messages: &[std::sync::Arc<Message>]) -> Vec<String> {
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
async fn history<B: SessionBackend>(backend: &B, cipher: Cipher) -> (Session<&B>, u64, u64) {
    let mut session = Session::create(backend, addr(), triple(), &deployment(cipher), false)
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

fn damage(backend: &MemoryBackend, at: u64, cipher: Cipher) {
    let (log, end) = block_on(async {
        let (stored, _) = backend.read_head(&addr()).await.unwrap().unwrap();
        let (head, _) = crate::head::open(&stored, &deployment(cipher)).unwrap();
        let log = head.open_log.unwrap().change;
        (log, backend.log_end(&addr(), &log).await.unwrap())
    });
    let mut bytes = block_on(backend.read_log(&addr(), &log, 0..end)).unwrap();
    bytes[at as usize] ^= 0x40;
    backend.set_log(&addr(), &log, bytes);
}

fn recover(backend: &MemoryBackend, now: u64, cipher: Cipher) -> Result<Recovery, SessionError> {
    block_on(Session::recover(
        backend,
        addr(),
        &triple(),
        &deployment(cipher),
        now,
    ))
}

fn load(backend: &MemoryBackend, cipher: Cipher) -> Result<Session<&MemoryBackend>, SessionError> {
    block_on(Session::load(
        backend,
        addr(),
        &triple(),
        &deployment(cipher),
    ))
}

/// A lost tool result and outcome: the call may have run, so its run ends interrupted, the call
/// gets `outcome_unknown` in both streams, the model is warned, and the transcript change is owed to
/// the compaction provider. The session then loads, takes appends, and reloads the same.
fn a_lost_tool_result_closes_open_work_and_warns_the_model(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (session, from, to) = block_on(history(&backend, cipher));
    drop(session);
    // Both frames of the batch: the transcript result and the ledger outcome.
    damage(&backend, from + 60, cipher);
    damage(&backend, to - 20, cipher);
    assert!(matches!(
        load(&backend, cipher),
        Err(SessionError::NeedsRecovery)
    ));

    let Recovery::Recovered(recovered) = recover(&backend, 1_000, cipher).unwrap() else {
        panic!("nothing recovered");
    };
    assert!(recovered.loss_aware);
    assert_eq!(recovered.interrupted_runs, ["b"]);
    assert_eq!(recovered.unknown_calls, [ToolCallId("c1".into())]);
    assert_eq!(recovered.history_version, Some(1));

    let mut session = load(&backend, cipher).unwrap();
    assert!(session.head().next_seq > recovered.removed_through);
    let window = texts(&session.conversation.held_messages().unwrap());
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
    let reloaded = load(&backend, cipher).unwrap();
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
fn a_kept_outcome_says_the_call_finished(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (session, from, _) = block_on(history(&backend, cipher));
    drop(session);
    damage(&backend, from + 60, cipher);
    let Recovery::Recovered(recovered) = recover(&backend, 1_000, cipher).unwrap() else {
        panic!("nothing recovered");
    };
    assert!(recovered.unknown_calls.is_empty());
    assert_eq!(recovered.interrupted_runs, ["b"]);
    let session = load(&backend, cipher).unwrap();
    let window = texts(&session.conversation.held_messages().unwrap());
    assert!(
        window[3].contains("finished, but storage damage removed its output"),
        "{window:?}"
    );
}

/// A kept transcript result whose ledger outcome was lost gets its outcome from the result. A
/// call answered before a ledger seal is settled: its outcome is sealed, not lost, even though
/// the open log still holds its message and result.
fn only_outcomes_lost_with_the_log_are_derived(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let mut session = block_on(Session::create(
        &backend,
        addr(),
        triple(),
        &deployment(cipher),
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
    damage(&backend, to - 20, cipher);
    let Recovery::Recovered(recovered) = recover(&backend, 1_000, cipher).unwrap() else {
        panic!("nothing recovered");
    };
    let _ = from;
    assert_eq!(recovered.derived_outcomes, [ToolCallId("c1".into())]);
    assert!(recovered.unknown_calls.is_empty());
}

/// Damage to the last batch loses the highest seqs the log held. The recovery's own records,
/// and every later append, take seqs above all of them.
fn seqs_after_a_recovery_are_above_everything_the_damage_removed(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (session, _, to) = block_on(history(&backend, cipher));
    let written_through = session.encoder.next_seq() - 1;
    drop(session);
    damage(&backend, to + 60, cipher);
    let Recovery::Recovered(recovered) = recover(&backend, 1_000, cipher).unwrap() else {
        panic!("nothing recovered");
    };
    assert!(
        recovered.removed_through >= written_through,
        "{} < {written_through}",
        recovered.removed_through
    );
    let mut session = load(&backend, cipher).unwrap();
    let first = block_on(session.append(&[start("c")], true)).unwrap();
    assert!(first > recovered.removed_through);
}

/// Transcript records after the first one that may be missing are dropped, even though they
/// authenticate: a conversation never skips a message.
fn transcript_records_after_a_possible_loss_are_dropped(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (mut session, from, _) = block_on(history(&backend, cipher));
    block_on(session.append(&[input("b", "after the loss")], false)).unwrap();
    drop(session);
    damage(&backend, from + 60, cipher);
    let Recovery::Recovered(recovered) = recover(&backend, 1_000, cipher).unwrap() else {
        panic!("nothing recovered");
    };
    assert_eq!(recovered.transcript_dropped, 1);
    let loaded = load(&backend, cipher).unwrap();
    assert!(!texts(&loaded.conversation.held_messages().unwrap())
        .iter()
        .any(|text| text == "after the loss"));
}

/// Damage in bytes a barrier acknowledged is corruption: loading and recovery refuse, and
/// nothing changes.
fn damage_to_acknowledged_bytes_refuses_and_changes_nothing(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (session, from, _) = block_on(history(&backend, cipher));
    drop(session);
    damage(&backend, from - 20, cipher);
    let logs = backend.log_names(&addr());
    assert!(matches!(
        load(&backend, cipher),
        Err(SessionError::Corrupt { .. })
    ));
    assert!(matches!(
        recover(&backend, 1_000, cipher),
        Err(SessionError::Corrupt { .. })
    ));
    assert_eq!(backend.log_names(&addr()), logs);
    assert!(backend.object_names(&addr()).is_empty());
}

/// A crash at any point of a recovery leaves the session needing recovery, or recovered. Either
/// way, recovering again and loading gives the same conversation and run state as a recovery
/// that never crashed, with nothing left of the crashed attempt.
fn a_crash_at_any_point_of_a_recovery_recovers_again(cipher: Cipher) {
    let expected = {
        let backend = MemoryBackend::default();
        let (session, from, _) = block_on(history(&backend, cipher));
        drop(session);
        damage(&backend, from + 60, cipher);
        recover(&backend, 1_000, cipher).unwrap();
        let session = load(&backend, cipher).unwrap();
        (
            texts(&session.conversation.held_messages().unwrap()),
            session.operations.run.as_ref().unwrap().finished,
        )
    };
    let mut crashed = 0;
    for crash in 0..30 {
        let backend = MemoryBackend::default();
        let (session, from, _) = block_on(history(&backend, cipher));
        drop(session);
        damage(&backend, from + 60, cipher);
        backend.crash_after(Some(crash));
        if recover(&backend, 1_000, cipher).is_err() {
            crashed += 1;
        }
        backend.crash_after(None);
        recover(&backend, 1_000, cipher).unwrap();
        let session = load(&backend, cipher).unwrap();
        let got = (
            texts(&session.conversation.held_messages().unwrap()),
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

/// Two finished runs, all synced. `seal` adds a ledger seal after the first, so the open log is
/// one a change created rather than the session's first.
fn finished_runs(
    backend: &MemoryBackend,
    seal: bool,
    cipher: Cipher,
) -> (Vec<String>, Option<RunFinishReason>) {
    block_on(async {
        let mut session = Session::create(backend, addr(), triple(), &deployment(cipher), false)
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
        if seal {
            session.seal(None, 1_000).await.unwrap();
        }
        session.append(&[start("b")], true).await.unwrap();
        session
            .append(
                &[
                    input("b", "ask b"),
                    assistant("b", 1, vec![text("answer b")]),
                ],
                true,
            )
            .await
            .unwrap();
        session.append(&[end("b")], true).await.unwrap();
        (
            texts(&session.conversation.held_messages().unwrap()),
            session.operations.run.as_ref().unwrap().finished,
        )
    })
}

/// A damaged checkpoint is rebuilt, never cut: from empty state for the session's first log, and
/// from the copy the creating change sealed for a later log. Nothing else was damaged, so the
/// recovery closes nothing and the session loads with the conversation and run state it had.
fn a_damaged_checkpoint_is_rebuilt_and_loses_nothing(cipher: Cipher) {
    for seal in [false, true] {
        let backend = MemoryBackend::default();
        let expected = finished_runs(&backend, seal, cipher);
        damage(&backend, 60, cipher);
        assert!(
            matches!(load(&backend, cipher), Err(SessionError::NeedsRecovery)),
            "seal {seal}"
        );
        let Recovery::Recovered(recovered) = recover(&backend, 1_000, cipher).unwrap() else {
            panic!("seal {seal}: nothing recovered");
        };
        assert!(!recovered.loss_aware, "seal {seal}");
        assert!(recovered.interrupted_runs.is_empty(), "seal {seal}");
        assert_eq!(recovered.history_version, None, "seal {seal}");
        let session = load(&backend, cipher).unwrap();
        let got = (
            texts(&session.conversation.held_messages().unwrap()),
            session.operations.run.as_ref().unwrap().finished,
        );
        assert_eq!(got, expected, "seal {seal}");
        assert!(!session.operations.unattributed_risk, "seal {seal}");
    }
}

fn a_rebuilt_checkpoint_keeps_frozen_and_post_move_activity(cipher: Cipher) {
    for moved in [false, true] {
        for later in [false, true] {
            let backend = MemoryBackend::default();
            let expected = block_on(async {
                let mut session =
                    Session::create(&backend, addr(), triple(), &deployment(cipher), false)
                        .await
                        .unwrap();
                let mut asked = input("a", "converted input");
                if let Record::Input(input) = &mut asked {
                    input.written_at_ms = Some(2_003);
                }
                session
                    .append(
                        &[
                            start("a"),
                            asked,
                            assistant("a", 1, vec![text("answer")]),
                            end("a"),
                        ],
                        true,
                    )
                    .await
                    .unwrap();
                if moved {
                    session
                        .append(
                            &[Record::Moved(crate::record::Moved {
                                source_lineage: None,
                                move_id: "store-move:test".into(),
                                source_activity_ms: 2_002,
                            })],
                            true,
                        )
                        .await
                        .unwrap();
                }
                session.seal(None, 5_000).await.unwrap();
                if later {
                    let mut started = start("b");
                    if let Record::RunStart(start) = &mut started {
                        start.written_at_ms = Some(3_000);
                    }
                    session
                        .append(
                            &[
                                started,
                                input("b", "new send"),
                                assistant("b", 1, vec![text("later")]),
                                end("b"),
                            ],
                            true,
                        )
                        .await
                        .unwrap();
                }
                let expected = if later {
                    3_000
                } else if moved {
                    2_002
                } else {
                    2_003
                };
                assert_eq!(session.open_log_activity_ms(), Some(expected));
                session.close().await.unwrap();
                // Opening a writer supplies a ledger back-link after the checkpoint. Recovery
                // can then prove that only the checkpoint was lost, without adding a loss notice
                // that would itself be new session activity.
                let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
                    .await
                    .unwrap();
                assert_eq!(loaded.open_log_activity_ms(), Some(expected));
                loaded.close().await.unwrap();
                expected
            });
            damage(&backend, 60, cipher);
            let Recovery::Recovered(recovered) = recover(&backend, 7_000, cipher).unwrap() else {
                panic!("the checkpoint must be rebuilt");
            };
            assert!(
                !recovered.loss_aware,
                "moved={moved}, later={later}: {recovered:?}"
            );
            let loaded = load(&backend, cipher).unwrap();
            assert_eq!(
                loaded.open_log_activity_ms(),
                Some(expected),
                "moved={moved}, later={later}"
            );
        }
    }
}

/// A ledger segment whose copy names another log can't stand in for this log's checkpoint:
/// recovery refuses and writes nothing.
fn a_damaged_checkpoint_without_its_copy_refuses(cipher: Cipher) {
    let backend = MemoryBackend::default();
    finished_runs(&backend, true, cipher);
    // Seal again: the newest ledger segment now belongs to the change that created the current
    // log. Then point the head back at the log the first seal created.
    let (first_log, logs) = block_on(async {
        let (stored, _) = backend.read_head(&addr()).await.unwrap().unwrap();
        let (head, _) = crate::head::open(&stored, &deployment(cipher)).unwrap();
        (head.open_log.unwrap().change, backend.log_names(&addr()))
    });
    let mut session = load(&backend, cipher).unwrap();
    block_on(session.seal(None, 2_000)).unwrap();
    drop(session);
    block_on(async {
        let (stored, version) = backend.read_head(&addr()).await.unwrap().unwrap();
        let (mut head, key) = crate::head::open(&stored, &deployment(cipher)).unwrap();
        let mut open = head.open_log.clone().unwrap();
        open.change = first_log;
        let len = backend.log_end(&addr(), &first_log).await.unwrap();
        open.synced_len = len;
        head.open_log = Some(open);
        backend
            .write_head(
                &addr(),
                &crate::head::seal(&head, &key),
                Some(version),
                version + 1,
            )
            .await
            .unwrap();
    });
    assert!(logs.contains(&first_log));
    damage(&backend, 60, cipher);
    let objects = backend.object_names(&addr());
    assert!(matches!(
        recover(&backend, 3_000, cipher),
        Err(SessionError::CheckpointLost)
    ));
    assert_eq!(backend.object_names(&addr()), objects);
}
