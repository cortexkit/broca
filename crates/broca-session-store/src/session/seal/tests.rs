use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::ids::MessageId;
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;

use super::GRACE_MS;
use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::{hex, Message as _};
use crate::head::SessionTriple;
use crate::keys::DeploymentKey;
use crate::record::{Assistant, Input, Placement, Record, RunEnd, RunStart};
use crate::segment::Segment;
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

fn state<B: SessionBackend>(session: &Session<'_, B>) -> (Vec<u8>, Vec<u8>) {
    (session.conversation.encode(), session.operations.encode())
}

/// Two finished runs, then a third still running. Returns the session and the seq of the third
/// run's input, where a cut can fall.
async fn history<B: SessionBackend>(backend: &B) -> (Session<'_, B>, u64) {
    let mut session = Session::create(backend, addr(), triple(), &deployment(), false)
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
#[test]
fn a_cut_seals_below_it_and_the_session_loads_back_the_same() {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, cut) = history(&backend).await;
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

        let mut loaded = Session::load(&backend, addr(), &triple(), &deployment())
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
#[test]
fn sealed_records_read_back_and_finished_runs_lose_their_opaque_reasoning() {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, cut) = history(&backend).await;
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
#[test]
fn a_ledger_seal_keeps_the_whole_transcript_open() {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, _) = history(&backend).await;
        let before = state(&session);
        let sealed = session.seal(None, 1_000).await.unwrap();
        assert_eq!(sealed.transcript_records, 0);
        assert_eq!(sealed.kept_records, 6);
        assert_eq!(state(&session), before);
        let loaded = Session::load(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        assert_eq!(
            texts(&loaded.conversation.messages),
            ["ask a", "a", "ask b", "b", "ask c", "c"]
        );
    });
}

/// A cut must name the first record of an entry; anything else refuses before the log is
/// touched.
#[test]
fn a_cut_that_does_not_start_an_entry_refuses_and_changes_nothing() {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, cut) = history(&backend).await;
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
#[test]
fn the_replaced_log_is_deleted_only_after_the_grace_period() {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, cut) = history(&backend).await;
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

/// A failure in the middle of a seal undoes it at once: nothing it created is left, and the
/// log accepts records again.
#[test]
fn a_seal_that_fails_before_its_commit_undoes_itself() {
    // The seal's mutations: the intent, two segments and their directory pages, the id index's
    // data page, ref page and root, the new log, its bytes, the object sync, then the head.
    for failing in 1..11 {
        let backend = MemoryBackend::default();
        block_on(async {
            let (mut session, cut) = history(&backend).await;
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
#[test]
fn a_crash_at_any_point_of_a_seal_loads_to_before_or_after_it() {
    // The seal's mutations: the intent, two segments and their directory pages, the id index's
    // data page, ref page and root, the new log, its bytes, the object sync, then the head. So
    // the last three crash points come after the commit.
    let mut committed = 0;
    for crash in 0..15 {
        let backend = MemoryBackend::default();
        let (before, after) = block_on(async {
            let (mut session, cut) = history(&backend).await;
            let before = texts(&session.conversation.messages);
            backend.crash_after(Some(crash));
            let sealed = session.seal(Some(cut), 1_000).await;
            (before, sealed.is_ok())
        });
        backend.crash_after(None);
        block_on(async {
            let loaded = Session::load(&backend, addr(), &triple(), &deployment())
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
