use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::ids::MessageId;
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;

use super::{Carry, GRACE_MS};
use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::{hex, Message as _};
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{Assistant, Input, Placement, Record, RunEnd, RunStart};
use crate::segment::Segment;
use crate::session::{Session, SessionError};

cipher_tests!(
    a_cut_seals_below_it_and_the_session_loads_back_the_same,
    sealed_records_read_back_and_finished_runs_lose_their_opaque_reasoning,
    a_ledger_seal_keeps_the_whole_transcript_open,
    a_cut_that_does_not_start_an_entry_refuses_and_changes_nothing,
    the_replaced_log_is_deleted_only_after_the_grace_period,
    a_session_no_writer_holds_is_collected_once_each_grace_is_over,
    a_seal_that_fails_before_its_commit_undoes_itself,
    a_crash_at_any_point_of_a_seal_loads_to_before_or_after_it,
    a_seal_carries_ledger_records_into_the_new_log,
    a_seal_at_an_entry_cuts_where_that_entry_is_placed,
    a_seal_preserves_unmoved_activity_when_ledger_times_leave_the_log,
    a_seal_preserves_a_move_boundary_and_later_activity,
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
        message: Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }]),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

/// An assistant answer carrying opaque reasoning before its text.
fn answer(run: &str, text: &str) -> Record {
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: 1,
        provider_message_id: MessageId(format!("m-{text}")),
        message: Message::assistant_from(
            vec![
                ContentBlock::RedactedReasoning {
                    data: format!("opaque-{text}"),
                    provider_format: None,
                },
                ContentBlock::Text { text: text.into() },
            ],
            model(),
        ),
        usage: None,
        written_at_ms: None,
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

fn texts(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .flat_map(|m| &m.content)
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

fn state<B: SessionBackend>(session: &Session<B>) -> (Vec<u8>, Vec<u8>) {
    (session.conversation.encode(), session.operations.encode())
}

/// Two finished runs, then a third still running. Returns the session and the seq of the third
/// run's input, where a cut can fall.
async fn history<B: SessionBackend>(backend: &B, cipher: Cipher) -> (Session<&B>, u64) {
    let mut session = Session::create(backend, addr(), triple(), &deployment(cipher), false)
        .await
        .unwrap();
    for run in ["a", "b"] {
        session.append(&[start(run)], true).await.unwrap();
        session
            .append(&[input(run, &format!("ask {run}")), answer(run, run)], true)
            .await
            .unwrap();
        session.append(&[end(run)], true).await.unwrap();
    }
    session.append(&[start("c")], true).await.unwrap();
    let cut = session
        .append(&[input("c", "ask c"), answer("c", "c")], true)
        .await
        .unwrap();
    (session, cut)
}

/// A cut seals the transcript below it and every ledger record. The window keeps only what's
/// above the cut, a reload rebuilds exactly the state the writer holds, and seqs continue.
fn a_cut_seals_below_it_and_the_session_loads_back_the_same(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, cut) = history(&backend, cipher).await;
        assert_eq!(
            texts(&session.conversation.messages),
            ["ask a", "a", "ask b", "b", "ask c", "c"]
        );
        let sealed = session.seal(Some(cut), 1_000).await.unwrap();
        assert_eq!(sealed.transcript_records, 4);
        assert_eq!(sealed.kept_records, 2);
        assert_eq!(texts(&session.conversation.messages), ["ask c", "c"]);
        // Run c's input and answer were appended as one batch, and the new log keeps them as
        // one: a crash can't leave one without the other there either.
        let (log, length) = session.open_log();
        let bytes = backend.read_log(&addr(), &log, 0..length).await.unwrap();
        let read = crate::log::read(
            &bytes,
            &session.key.frame_key(),
            &crate::session::log_bind(&session.head().session_id, &log),
            0,
            |body| {
                let checkpoint = crate::checkpoint::Checkpoint::decode(body).unwrap();
                Ok(crate::log::ChainBase {
                    transcript: checkpoint.transcript_base,
                    ledger: checkpoint.ledger_base,
                })
            },
        )
        .unwrap();
        assert_eq!(read.end, crate::log::LogEnd::Clean);
        assert_eq!(read.batches, vec![0..2]);
        assert_eq!(read.records[0].0, cut);
        let written = state(&session);
        let next = session.append(&[end("c")], true).await.unwrap();

        let mut loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(texts(&loaded.conversation.messages), ["ask c", "c"]);
        assert!(loaded.operations.run.as_ref().unwrap().finished.is_some());
        assert_ne!(state(&loaded), written, "run c ended after the seal");
        assert!(loaded.append(&[start("d")], true).await.unwrap() > next);
    });
}

/// The sealed transcript holds the records below the cut, and the finished runs' opaque
/// reasoning is pruned from them; the running run's would not be.
fn sealed_records_read_back_and_finished_runs_lose_their_opaque_reasoning(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, cut) = history(&backend, cipher).await;
        let change = session.seal(Some(cut), 1_000).await.unwrap().change;
        let name = format!("{}/t", hex(&change.0));
        let bytes = backend.get_object(&addr(), &name).await.unwrap().unwrap();
        let key = session.key.frame_key();
        let segment = Segment::open(&bytes, &key, name.as_bytes()).unwrap();
        let records: Vec<(u64, Record)> = (0..segment.index.blocks.len())
            .flat_map(|position| segment.block(position).unwrap())
            .collect();
        assert_eq!(records.len(), 4);
        for (_, record) in &records {
            let Record::Assistant(assistant) = record else {
                continue;
            };
            assert_eq!(assistant.pruned.len(), 1);
            assert!(matches!(
                &assistant.message.content[0],
                ContentBlock::RedactedReasoning { data, .. } if data.is_empty()
            ));
        }
        // Run c is still running, so its reasoning stays in the open log unpruned.
        let open = session
            .conversation
            .messages
            .iter()
            .flat_map(|m| &m.content)
            .any(|block| matches!(block, ContentBlock::RedactedReasoning { data, .. } if data == "opaque-c"));
        assert!(open);
    });
}

/// A ledger seal moves only the ledger: the whole transcript stays open.
fn a_ledger_seal_keeps_the_whole_transcript_open(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, _) = history(&backend, cipher).await;
        let before = state(&session);
        let sealed = session.seal(None, 1_000).await.unwrap();
        assert_eq!(sealed.transcript_records, 0);
        assert_eq!(sealed.kept_records, 6);
        assert_eq!(state(&session), before);
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(
            texts(&loaded.conversation.messages),
            ["ask a", "a", "ask b", "b", "ask c", "c"]
        );
    });
}

fn a_seal_preserves_unmoved_activity_when_ledger_times_leave_the_log(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mut started = start("a");
        if let Record::RunStart(start) = &mut started {
            start.written_at_ms = Some(1_000);
        }
        let mut asked = input("a", "ask");
        if let Record::Input(input) = &mut asked {
            input.written_at_ms = Some(1_001);
        }
        let mut ended = end("a");
        if let Record::RunEnd(end) = &mut ended {
            end.ts_ms = Some(2_003);
        }
        session
            .append(&[started, asked, answer("a", "answer"), ended], true)
            .await
            .unwrap();
        assert_eq!(session.open_log_activity_ms(), Some(2_003));
        session.seal(None, 5_000).await.unwrap();
        assert_eq!(
            session.open_log_activity_ms(),
            Some(2_003),
            "sealing is not activity"
        );
        let mut loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(loaded.open_log_activity_ms(), Some(2_003));
        loaded.seal(None, 6_000).await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(loaded.open_log_activity_ms(), Some(2_003));
    });
}

fn a_seal_preserves_a_move_boundary_and_later_activity(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
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
                    answer("a", "answer"),
                    end("a"),
                    Record::Moved(crate::record::Moved {
                        move_id: "store-move:test".into(),
                        source_activity_ms: 2_002,
                    }),
                ],
                true,
            )
            .await
            .unwrap();
        assert_eq!(session.open_log_activity_ms(), Some(2_002));
        session.seal(None, 5_000).await.unwrap();
        assert_eq!(session.open_log_activity_ms(), Some(2_002));
        let mut loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(
            loaded.open_log_activity_ms(),
            Some(2_002),
            "the copied prefix cannot outrank Moved"
        );
        let mut later = start("b");
        if let Record::RunStart(start) = &mut later {
            start.written_at_ms = Some(3_000);
        }
        loaded
            .append(
                &[
                    later,
                    input("b", "new send"),
                    answer("b", "later"),
                    end("b"),
                ],
                true,
            )
            .await
            .unwrap();
        assert_eq!(loaded.open_log_activity_ms(), Some(3_000));
        loaded.seal(None, 6_000).await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(loaded.open_log_activity_ms(), Some(3_000));
    });
}

/// A cut must name the first record of an entry; anything else refuses before the log is
/// touched.
fn a_cut_that_does_not_start_an_entry_refuses_and_changes_nothing(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, cut) = history(&backend, cipher).await;
        let before = state(&session);
        let (_, length) = session.open_log();
        // `cut - 1` is run c's start, a ledger record; nothing holds seq 9999.
        for bad in [cut - 1, 9_999] {
            assert!(matches!(
                session.seal(Some(bad), 1_000).await,
                Err(SessionError::BadCut(at)) if at == bad
            ));
        }
        assert_eq!(state(&session), before);
        assert_eq!(session.open_log().1, length);
    });
}

/// What a change stopped naming stays for the grace period, then goes.
fn the_replaced_log_is_deleted_only_after_the_grace_period(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, cut) = history(&backend, cipher).await;
        let (old, _) = session.open_log();
        session.seal(Some(cut), 1_000).await.unwrap();
        let (new, _) = session.open_log();
        assert_eq!(session.collect(1_000 + GRACE_MS - 1).await.unwrap(), 0);
        assert!(backend.log_names(&addr()).contains(&old));
        assert_eq!(session.collect(1_000 + GRACE_MS).await.unwrap(), 1);
        assert_eq!(backend.log_names(&addr()), [new]);
        assert!(session.head().pending.is_empty());
    });
}

/// A session no writer holds is collected from its stored head: what each change stopped naming
/// stays until that change's grace period is over, then goes, and the head is rewritten only
/// when something was deleted.
fn a_session_no_writer_holds_is_collected_once_each_grace_is_over(cipher: Cipher) {
    use crate::session::{collect_stored, stored_due};
    let backend = MemoryBackend::default();
    let deployment = deployment(cipher);
    block_on(async {
        let (mut session, cut) = history(&backend, cipher).await;
        let (created, _) = session.open_log();
        session.seal(Some(cut), 1_000).await.unwrap();
        let (first, _) = session.open_log();
        session.append(&[end("c")], true).await.unwrap();
        session.seal(None, 2_000).await.unwrap();
        let (open, _) = session.open_log();
        // The second seal rewrote the directory pages the first one wrote.
        let replaced = session.head().pending[1].objects.clone();
        assert!(!replaced.is_empty());
        session.close().await.unwrap();
        let at = addr();
        let stored = || async { backend.read_head(&at).await.unwrap().unwrap() };
        let collect = |now_ms| collect_stored(&backend, &at, &deployment, now_ms);

        // Inside both grace periods: nothing is due, and nothing is written.
        let (head, version) = stored().await;
        assert!(!stored_due(&head, 1_000 + GRACE_MS - 1).unwrap());
        assert_eq!(collect(1_000 + GRACE_MS - 1).await.unwrap(), 0);
        assert_eq!(stored().await.1, version);
        assert!(backend.log_names(&addr()).contains(&created));

        // Past the first seal's grace, inside the second's: only the first's log goes.
        assert!(stored_due(&head, 2_000 + GRACE_MS - 1).unwrap());
        assert_eq!(collect(2_000 + GRACE_MS - 1).await.unwrap(), 1);
        let logs = backend.log_names(&addr());
        assert!(!logs.contains(&created));
        assert!(logs.contains(&first));
        let objects = backend.object_names(&addr());
        assert!(replaced.iter().all(|object| objects.contains(object)));
        assert_eq!(stored().await.1, version + 1);

        // Past the second's: its log and the pages it replaced go too.
        assert_eq!(collect(2_000 + GRACE_MS).await.unwrap(), 1);
        assert_eq!(backend.log_names(&addr()), [open]);
        let objects = backend.object_names(&addr());
        assert!(replaced.iter().all(|object| !objects.contains(object)));
        let (head, version) = stored().await;
        assert!(!stored_due(&head, u64::MAX).unwrap());
        assert_eq!(collect(u64::MAX).await.unwrap(), 0);
        assert_eq!(stored().await.1, version);

        let loaded = Session::load(&backend, addr(), &triple(), &deployment)
            .await
            .unwrap();
        assert!(loaded.head().pending.is_empty());
        assert_eq!(loaded.open_log().0, open);
    });
}

/// A failure in the middle of a seal undoes it at once: nothing it created is left, and the
/// log accepts records again.
fn a_seal_that_fails_before_its_commit_undoes_itself(cipher: Cipher) {
    // The seal's mutations: the intent, two segments and their directory pages, the id index's
    // data page, ref page and root, the new log, its bytes, the object sync, then the head.
    for failing in 1..11 {
        let backend = MemoryBackend::default();
        block_on(async {
            let (mut session, cut) = history(&backend, cipher).await;
            let before = state(&session);
            let logs = backend.log_names(&addr());
            backend.fail_once(failing);
            assert!(
                session.seal(Some(cut), 1_000).await.is_err(),
                "failing {failing}"
            );
            assert!(
                backend.object_names(&addr()).is_empty(),
                "failing {failing}"
            );
            assert_eq!(backend.log_names(&addr()), logs, "failing {failing}");
            assert_eq!(state(&session), before, "failing {failing}");
            session.append(&[end("c")], true).await.unwrap();
        });
    }
}

/// A crash at any point of a seal leaves a session that loads to the state before the seal or
/// after it, never between, with nothing left of a change that didn't commit.
fn a_crash_at_any_point_of_a_seal_loads_to_before_or_after_it(cipher: Cipher) {
    // The seal's mutations: the intent, two segments and their directory pages, the id index's
    // data page, ref page and root, the new log, its bytes, the object sync, then the head. So
    // the last three crash points come after the commit.
    let mut committed = 0;
    for crash in 0..15 {
        let backend = MemoryBackend::default();
        let (before, after) = block_on(async {
            let (mut session, cut) = history(&backend, cipher).await;
            let before = texts(&session.conversation.messages);
            backend.crash_after(Some(crash));
            let sealed = session.seal(Some(cut), 1_000).await;
            (before, sealed.is_ok())
        });
        backend.crash_after(None);
        block_on(async {
            let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
                .await
                .unwrap();
            let window = texts(&loaded.conversation.messages);
            let objects = backend.object_names(&addr());
            if window == before {
                assert!(!after, "crash {crash}: a committed seal must load sealed");
                assert!(objects.is_empty(), "crash {crash}: leftovers {objects:?}");
                assert!(loaded.head().pending.is_empty());
            } else {
                committed += 1;
                assert_eq!(window, ["ask c", "c"], "crash {crash}");
                assert!(!objects.is_empty());
                assert_eq!(loaded.head().pending.len(), 1);
            }
        });
    }
    assert!((1..15).contains(&committed), "{committed}");
}

/// A seal can carry ledger records into the new open log: they take fresh seqs after every
/// record the seal read, fold into the state it installs, and a reload finds them in the open
/// log, which a compaction view's full copy needs after its earlier changes are sealed out.
fn a_seal_carries_ledger_records_into_the_new_log(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, cut) = history(&backend, cipher).await;
        let applied = Record::ViewApplied(crate::record::ViewApplied {
            step_id: 7,
            version: 3,
            checksum: "c3".into(),
        });
        let before = session.next_seq();
        session
            .seal_carrying(
                Some(cut),
                &Carry {
                    records: std::slice::from_ref(&applied),
                    ..Default::default()
                },
                1_000,
            )
            .await
            .unwrap();
        let carried = session
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .filter(|(_, record)| matches!(record, Record::ViewApplied(_)))
            .collect::<Vec<_>>();
        assert_eq!(carried.len(), 1);
        // Above the change's own intent, which took `before`.
        assert!(carried[0].0 > before);
        assert_eq!(
            session
                .operations
                .view_applied
                .as_ref()
                .map(|slot| slot.view.version),
            Some(3)
        );
        let written = state(&session);
        let mut loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(state(&loaded), written);
        // Only ledger records can be carried: the transcript a seal keeps is the cut's to decide.
        assert!(matches!(
            session
                .seal_carrying(
                    None,
                    &Carry {
                        records: &[input("c", "late")],
                        ..Default::default()
                    },
                    2_000,
                )
                .await,
            Err(super::SessionError::Fold(_))
        ));
        // The reloaded session holds the writer now, and names an applied view, so a seal must
        // carry a copy of it and a `ViewApplied` naming it. Without them it refuses, and writes
        // nothing.
        let before = loaded.next_seq();
        assert!(matches!(
            loaded.seal_carrying(None, &Carry::default(), 2_000).await,
            Err(super::SessionError::ViewNotCarried)
        ));
        assert!(matches!(
            loaded
                .seal_carrying(
                    None,
                    &Carry {
                        records: std::slice::from_ref(&applied),
                        ..Default::default()
                    },
                    2_000,
                )
                .await,
            Err(super::SessionError::ViewNotCarried)
        ));
        assert_eq!(loaded.next_seq(), before);
        // With both carried, the seal goes ahead.
        let view = Record::View(crate::record::View {
            version: 3,
            checksum: "c3".into(),
            edits: serde_json::json!([]),
            built_through: None,
            new_signed: None,
            applied_by: None,
        });
        loaded
            .seal_carrying(
                None,
                &Carry {
                    records: &[view, applied.clone()],
                    ..Default::default()
                },
                2_000,
            )
            .await
            .unwrap();
    });
}

/// A seal at an entry's ordinal cuts at the record that places it: the third run's input is
/// entry 5 (after two runs of an input and an answer each).
fn a_seal_at_an_entry_cuts_where_that_entry_is_placed(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, cut) = history(&backend, cipher).await;
        let sealed = session
            .seal_at_entry(5, &Carry::default(), 1_000)
            .await
            .unwrap();
        assert_eq!(sealed.transcript_records, 4);
        assert_eq!(texts(&session.conversation.messages), ["ask c", "c"]);
        let (records, _, _) = session.read_open_log().await.unwrap();
        assert_eq!(records[0].0, cut);
        // An answer starts an entry too; an ordinal past the log places nothing.
        let other_backend = MemoryBackend::default();
        let (mut other, _) = history(&other_backend, cipher).await;
        assert!(other
            .seal_at_entry(99, &Carry::default(), 1_000)
            .await
            .is_err());
    });
}
