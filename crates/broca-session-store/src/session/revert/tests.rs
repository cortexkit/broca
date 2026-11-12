use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::ids::MessageId;
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;

use super::{RevertRefusal, Reverted};
use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::Message as _;
use crate::head::SessionTriple;
use crate::idindex::IdKind;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{
    Assistant, EntryFacts, Input, Placement, QueuedSend, Record, RunEnd, RunStart,
};
use crate::session::{Session, SessionError, GRACE_MS};

cipher_tests!(
    a_revert_drops_what_the_session_kept_of_its_sealed_history,
    entries_by_mid_reads_named_entries_in_order,
    a_revert_in_the_open_log_keeps_the_messages_before_it,
    a_revert_into_sealed_history_cuts_the_directory_and_leaves_no_window,
    a_revert_refuses_open_work_and_wrong_targets,
    a_revert_that_fails_before_its_commit_undoes_itself,
    a_crash_at_any_point_of_a_revert_loads_to_before_or_after_it,
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

fn start(run: &str, send: &str) -> Record {
    Record::RunStart(Box::new(RunStart {
        named_speaker: None,
        imported_ids: None,
        message_marks: None,
        run_id: run.into(),
        model: model(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: Some(send.into()),
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

fn answer(run: &str, text: &str) -> Record {
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: 1,
        provider_message_id: MessageId(format!("m-{text}")),
        message: std::sync::Arc::new(Message::assistant_from(
            vec![ContentBlock::Text { text: text.into() }],
            model(),
        )),
        usage: None,
        written_at_ms: None,
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

fn facts(ordinal: u64, mid: String) -> Record {
    Record::EntryFacts(std::sync::Arc::new(EntryFacts {
        send_id: None,
        named_speaker: None,
        client_message_id: None,
        sender: None,
        marks: None,
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        runner_tag_base: None,
        legacy_view_seq: None,
        ordinal,
        written_at_ms: Some(ordinal),
        tag_base: 0,
        notice: None,
        usage: None,
        generated_under: None,
        native: true,
        render_tags: false,
        clean_reply: false,
        mid,
        run_id: None,
        created_at_ms: None,
        delivery_mode: None,
        delivery_tool_call_id: None,
        notices: Vec::new(),
    }))
}

/// The resend a revert carries: a new run's start with a send id, its input, and the input's
/// entry facts, whose ordinal and id the revert fills in.
fn resend(run: &str, send: &str, text: &str) -> Vec<Record> {
    vec![start(run, send), input(run, text), facts(0, String::new())]
}

fn texts(messages: &[std::sync::Arc<Message>]) -> Vec<String> {
    messages
        .iter()
        .flat_map(|m| &m.content)
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

/// Runs `from..to`, each its run start, its input, and its answer, with entry facts, then its
/// end. Run `n` places messages `2n + 1` (input) and `2n + 2` (answer). Returns each message's
/// id, and the seq of each input record, where a cut can fall.
async fn runs<B: SessionBackend>(
    session: &mut Session<B>,
    from: usize,
    to: usize,
) -> (Vec<String>, Vec<u64>) {
    let (mut mids, mut inputs) = (Vec::new(), Vec::new());
    for n in from..to {
        let (run, ordinal) = (format!("r{n}"), 2 * n as u64 + 1);
        let mid = session.message_id(2);
        let first = session
            .append(
                &[
                    start(&run, &format!("s{n}")),
                    input(&run, &format!("ask {n}")),
                    facts(ordinal, mid.clone()),
                ],
                true,
            )
            .await
            .unwrap();
        mids.push(mid);
        inputs.push(first + 1);
        let mid = session.message_id(1);
        session
            .append(
                &[
                    answer(&run, &format!("answer {n}")),
                    facts(ordinal + 1, mid.clone()),
                ],
                true,
            )
            .await
            .unwrap();
        mids.push(mid);
        session.append(&[end(&run)], true).await.unwrap();
    }
    (mids, inputs)
}

/// What a session keeps of its sealed history goes when a revert moves the history version:
/// after a revert into sealed history, a sealed entry the revert cut away is read fresh and not
/// found, instead of being answered from what was kept, and a value derived from sealed history
/// is gone. Both are kept again under the new version.
fn a_revert_drops_what_the_session_kept_of_its_sealed_history(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let (mids, inputs) = runs(&mut session, 0, 4).await;
        session.seal(Some(inputs[3]), 1_000).await.unwrap();
        assert_eq!(session.conversation.window_start(), 7);
        let kept = session.entry_by_mid(&mids[4]).await.unwrap().unwrap();
        assert_eq!(kept.ordinal, 5);
        assert_eq!(session.kept_sealed_entries(), 1);
        session.keep_sealed_derived(std::sync::Arc::new(7u32));
        assert!(session.sealed_derived().is_some());

        session
            .revert(&mids[2], resend("r9", "s9", "edited"), None, 2_000)
            .await
            .unwrap();
        assert_eq!(session.kept_sealed_entries(), 0);
        assert!(session.sealed_derived().is_none());
        assert_eq!(session.entry_by_mid(&mids[4]).await.unwrap(), None);
        assert_eq!(
            session
                .entry_by_mid(&mids[1])
                .await
                .unwrap()
                .unwrap()
                .ordinal,
            2
        );
        assert_eq!(session.kept_sealed_entries(), 1);
    });
}

/// Entries read by message id come back once each, in ordinal order, from sealed history and
/// the window alike; an id no entry has is left out.
fn entries_by_mid_reads_named_entries_in_order(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let (mids, inputs) = runs(&mut session, 0, 4).await;
        session.seal(Some(inputs[3]), 1_000).await.unwrap();
        let named = [
            mids[7].clone(),
            mids[0].clone(),
            "g0000000000000000.99".into(),
            mids[4].clone(),
            mids[0].clone(),
        ];
        let read = session.entries_by_mid(&named).await.unwrap();
        let ordinals: Vec<u64> = read.iter().map(|entry| entry.ordinal).collect();
        assert_eq!(ordinals, [1, 5, 8]);
        assert_eq!(read[1].state.facts.as_ref().unwrap().mid, mids[4]);
    });
}

/// A revert at a message in the open log: the window keeps the messages before it, then the
/// resend at its ordinal. The reverted message's id and every later one resolve to nothing, the
/// resend's resolves to the ordinal, and the ledger keeps every abandoned run. The fold owes a
/// `transcript_changed` from the ordinal and moves the history version, the head too, and a
/// reload rebuilds the same state. A retry is answered from the resend's send id.
fn a_revert_in_the_open_log_keeps_the_messages_before_it(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let (mids, _) = runs(&mut session, 0, 4).await;
        let reverted = session
            .revert(&mids[4], resend("r9", "s9", "edited"), None, 1_000)
            .await
            .unwrap();
        let Reverted::Applied {
            ordinal,
            history_version,
            mid,
            window_pending,
            ..
        } = reverted
        else {
            panic!("{reverted:?}");
        };
        assert_eq!((ordinal, history_version, window_pending), (5, 1, false));
        assert_eq!(
            texts(&session.conversation.messages),
            ["ask 0", "answer 0", "ask 1", "answer 1", "edited"]
        );

        async fn check(session: &Session<&MemoryBackend>, mids: &[String], mid: &str) {
            assert_eq!(session.conversation.placed, 5);
            assert_eq!(session.head().history_version, 1);
            assert_eq!(session.operations.history_version, 1);
            assert_eq!(session.operations.transcript_changed, Some(5));
            for (index, old) in mids.iter().enumerate() {
                let expected = (index < 4).then_some(index as u64 + 1);
                assert_eq!(session.find_message(old).await.unwrap(), expected, "{old}");
            }
            assert_eq!(session.find_message(mid).await.unwrap(), Some(5));
            let entries = session.entries(1, usize::MAX).await.unwrap();
            assert_eq!(entries.len(), 5);
            assert_eq!(entries[4].state.facts.as_ref().unwrap().mid, mid);
            // The ledger is never cut: the abandoned runs are all still found.
            for run in ["r2", "r3"] {
                let found = session.find_id(IdKind::RunId, run).await.unwrap();
                assert_eq!(found.len(), 2, "{run}");
            }
        }
        check(&session, &mids, &mid).await;
        assert!(matches!(
            session
                .revert(&mids[4], resend("r9", "s9", "edited"), None, 1_000)
                .await,
            Ok(Reverted::Already { .. })
        ));
        session.close().await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        check(&loaded, &mids, &mid).await;
    });
}

/// A revert at a sealed message cuts the transcript directory there and leaves the session
/// window-pending: the messages before it are read from sealed segments, the resend is the whole
/// window. After the grace period, the abandoned segments and pages are deleted, and reads, a
/// reload and a later seal all still work.
fn a_revert_into_sealed_history_cuts_the_directory_and_leaves_no_window(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let (mut mids, inputs) = runs(&mut session, 0, 4).await;
        session.seal(Some(inputs[2]), 1_000).await.unwrap();
        let (more, _) = runs(&mut session, 4, 6).await;
        mids.extend(more);
        session.seal(Some(inputs[3]), 1_500).await.unwrap();
        assert_eq!(session.conversation.window_start(), 7);
        let objects = backend.object_names(&addr()).len();

        let reverted = session
            .revert(&mids[2], resend("r9", "s9", "edited"), None, 2_000)
            .await
            .unwrap();
        let Reverted::Applied {
            ordinal,
            window_pending,
            mid,
            ..
        } = reverted
        else {
            panic!("{reverted:?}");
        };
        assert_eq!((ordinal, window_pending), (3, true));
        assert!(session.conversation.window_pending);
        assert_eq!(texts(&session.conversation.messages), ["edited"]);
        assert_eq!(session.conversation.window_start(), 3);

        async fn check(session: &Session<&MemoryBackend>, mids: &[String], mid: &str) {
            let entries = session.entries(1, usize::MAX).await.unwrap();
            let read: Vec<String> = entries
                .iter()
                .flat_map(|e| texts(std::slice::from_ref(&e.message)))
                .collect();
            assert_eq!(read, ["ask 0", "answer 0", "edited"]);
            assert_eq!(
                session.message(2).await.unwrap().map(|m| texts(&[m])),
                Some(vec!["answer 0".to_string()])
            );
            assert_eq!(session.message(4).await.unwrap(), None);
            for (index, old) in mids.iter().enumerate() {
                let expected = (index < 2).then_some(index as u64 + 1);
                assert_eq!(session.find_message(old).await.unwrap(), expected, "{old}");
            }
            assert_eq!(session.find_message(mid).await.unwrap(), Some(3));
        }
        check(&session, &mids, &mid).await;
        assert!(session.collect(2_000 + GRACE_MS).await.unwrap() > 0);
        assert!(
            backend.object_names(&addr()).len() < objects,
            "abandoned objects were deleted"
        );
        check(&session, &mids, &mid).await;
        session.close().await.unwrap();
        let mut loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert!(loaded.conversation.window_pending);
        check(&loaded, &mids, &mid).await;
        loaded.append(&[end("r9")], true).await.unwrap();
        loaded.seal(None, 3_000).await.unwrap();
        check(&loaded, &mids, &mid).await;
    });
}

/// A revert refuses, writing nothing, while a run is open, while a send waits in the queue, for
/// an id no message on the branch has, for a message that isn't a run's own input, and for a
/// resend of the wrong shape.
fn a_revert_refuses_open_work_and_wrong_targets(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let (mids, _) = runs(&mut session, 0, 3).await;
        let refused = |result: Result<Reverted, SessionError>| match result {
            Err(SessionError::RevertRefused(refusal)) => refusal,
            other => panic!("{other:?}"),
        };
        let unchanged = |session: &Session<&MemoryBackend>| {
            (session.conversation.encode(), session.operations.encode())
        };
        let before = unchanged(&session);
        assert_eq!(
            refused(
                session
                    .revert("g0000000000000000.3", resend("r9", "s9", "x"), None, 1)
                    .await
            ),
            RevertRefusal::UnknownMessage
        );
        assert_eq!(
            refused(
                session
                    .revert(&mids[1], resend("r9", "s9", "x"), None, 1)
                    .await
            ),
            RevertRefusal::NotRunInput
        );
        let mut wrong = resend("r9", "s9", "x");
        wrong.pop();
        assert!(matches!(
            refused(session.revert(&mids[0], wrong, None, 1).await),
            RevertRefusal::BadResend(_)
        ));
        let mut wrong = resend("r9", "s9", "x");
        wrong[1] = input("other", "x");
        assert!(matches!(
            refused(session.revert(&mids[0], wrong, None, 1).await),
            RevertRefusal::BadResend(_)
        ));
        assert_eq!(unchanged(&session), before);

        session
            .append(
                &[Record::QueuedSend(QueuedSend {
                    named_speaker: None,
                    media: Vec::new(),
                    submission_id: "sub".into(),
                    send_id: Some("queued".into()),
                    input: Vec::new(),
                    setup: Vec::new(),
                    written_at_ms: None,
                    ts_ms: None,
                    request: None,
                    receipt: None,
                    delivery: None,
                    message_marks: None,
                })],
                true,
            )
            .await
            .unwrap();
        assert_eq!(
            refused(
                session
                    .revert(&mids[0], resend("r9", "s9", "x"), None, 1)
                    .await
            ),
            RevertRefusal::QueueNotEmpty
        );

        let mut session = Session::create(
            &backend,
            SessionAddr("1111222233334444".into()),
            triple(),
            &deployment(cipher),
            false,
        )
        .await
        .unwrap();
        let (mids, _) = runs(&mut session, 0, 2).await;
        session
            .append(&[start("open", "s-open")], true)
            .await
            .unwrap();
        assert_eq!(
            refused(
                session
                    .revert(&mids[0], resend("r9", "s9", "x"), None, 1)
                    .await
            ),
            RevertRefusal::RunOpen
        );
    });
}

/// A revert that fails before its commit undoes itself: nothing it created is left, the state
/// is unchanged, and the log accepts records again. A failed commit poisons the writer instead,
/// since it can't tell whether the head moved, and the next load undoes the revert.
fn a_revert_that_fails_before_its_commit_undoes_itself(cipher: Cipher) {
    let (mut undone, mut at_commit) = (0, 0);
    for failing in 1..20 {
        let backend = MemoryBackend::default();
        block_on(async {
            let mut session =
                Session::create(&backend, addr(), triple(), &deployment(cipher), false)
                    .await
                    .unwrap();
            let (mids, _) = runs(&mut session, 0, 3).await;
            let before = (session.conversation.encode(), session.operations.encode());
            let logs = backend.log_names(&addr());
            backend.fail_once(failing);
            if session
                .revert(&mids[2], resend("r9", "s9", "edited"), None, 1_000)
                .await
                .is_ok()
            {
                return;
            }
            if matches!(
                session.append(&[start("next", "s-next")], true).await,
                Err(SessionError::Poisoned)
            ) {
                at_commit += 1;
                let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
                    .await
                    .unwrap();
                assert_eq!(
                    (loaded.conversation.encode(), loaded.operations.encode()),
                    before,
                    "failing {failing}"
                );
                assert!(
                    backend.object_names(&addr()).is_empty(),
                    "failing {failing}"
                );
                return;
            }
            undone += 1;
            assert!(
                backend.object_names(&addr()).is_empty(),
                "failing {failing}"
            );
            assert_eq!(backend.log_names(&addr()), logs, "failing {failing}");
        });
    }
    assert!(
        undone >= 5,
        "only {undone} failure points reached the revert"
    );
    assert_eq!(at_commit, 1, "exactly one failure point is the commit");
}

/// A crash at any point of a revert loads to the session before it or after it.
fn a_crash_at_any_point_of_a_revert_loads_to_before_or_after_it(cipher: Cipher) {
    let mut committed = 0;
    for crash in 0..20 {
        let backend = MemoryBackend::default();
        let (before, mids) = block_on(async {
            let mut session =
                Session::create(&backend, addr(), triple(), &deployment(cipher), false)
                    .await
                    .unwrap();
            let (mids, _) = runs(&mut session, 0, 3).await;
            let before = texts(&session.conversation.messages);
            backend.crash_after(Some(crash));
            let _ = session
                .revert(&mids[2], resend("r9", "s9", "edited"), None, 1_000)
                .await;
            (before, mids)
        });
        backend.crash_after(None);
        block_on(async {
            let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
                .await
                .unwrap();
            let window = texts(&loaded.conversation.messages);
            if window == before {
                assert!(backend.object_names(&addr()).is_empty(), "crash {crash}");
                assert_eq!(loaded.head().history_version, 0);
                assert_eq!(loaded.find_message(&mids[2]).await.unwrap(), Some(3));
            } else {
                committed += 1;
                assert_eq!(window, ["ask 0", "answer 0", "edited"], "crash {crash}");
                assert_eq!(loaded.head().history_version, 1);
                assert_eq!(loaded.operations.transcript_changed, Some(3));
                assert_eq!(loaded.find_message(&mids[2]).await.unwrap(), None);
            }
        });
    }
    assert!((1..20).contains(&committed), "{committed}");
}
