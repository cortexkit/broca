//! The records a session keeps in memory, reads from memory only, syncing an unsynced tail, and
//! views loaded at a position.

use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;

use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::head::SessionTriple;
use crate::idindex::IdKind;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{Event, Input, Placement, Record, RunEnd, RunStart};
use crate::session::{Session, SessionError};

cipher_tests!(
    a_writer_keeps_its_latest_runs_records,
    a_view_keeps_every_record_it_read,
    a_seal_drops_the_kept_records_and_memory_only_refuses_sealed_history,
    a_sync_makes_an_unsynced_tail_durable_once,
    a_failed_sync_leaves_the_tail_unsynced,
    a_view_at_a_position_reads_only_that_far,
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
        imported_ids: None,
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

fn event(name: &str) -> Record {
    Record::Event(Event {
        name: name.into(),
        run_id: None,
        body: None,
    })
}

/// Two finished runs, then a third still running. Returns the session and the seq of each run's
/// start; the last run's input, where a cut can fall, follows its start.
async fn history<B: SessionBackend>(backend: &B, cipher: Cipher) -> (Session<&B>, Vec<u64>) {
    let mut session = Session::create(backend, addr(), triple(), &deployment(cipher), false)
        .await
        .unwrap();
    let mut starts = Vec::new();
    for run in ["a", "b", "c"] {
        starts.push(session.append(&[start(run)], true).await.unwrap());
        session
            .append(&[input(run, &format!("ask {run}")), event(run)], true)
            .await
            .unwrap();
        if run != "c" {
            session.append(&[end(run)], true).await.unwrap();
        }
    }
    (session, starts)
}

async fn view<B: SessionBackend>(backend: B, cipher: Cipher) -> crate::session::SessionView<B> {
    Session::view(backend, addr(), &triple(), &deployment(cipher))
        .await
        .unwrap()
}

/// A writer keeps the records of its latest run, from the run's start on: reading them from
/// memory only gives exactly what a fresh view reads from storage, and a run before it reads
/// from storage, which memory only refuses.
fn a_writer_keeps_its_latest_runs_records(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (session, starts) = history(&backend, cipher).await;
        assert_eq!(session.kept_run_start("c"), Some(starts[2]));
        assert_eq!(session.kept_run_start("b"), None);
        let fresh = view(&backend, cipher).await;
        let from_storage = fresh.records_from(starts[2]).await.unwrap();
        let in_memory = {
            let _memory = session.in_memory();
            session.records_from(starts[2]).await.unwrap()
        };
        assert_eq!(in_memory, from_storage);
        assert_eq!(in_memory.len(), 3);
        {
            let _memory = session.in_memory();
            assert!(matches!(
                session.records_between(starts[1], starts[2]).await,
                Err(SessionError::NotInMemory)
            ));
        }
        // Outside it, the writer reads the log as before.
        assert_eq!(
            session
                .records_between(starts[1], starts[2] - 1)
                .await
                .unwrap(),
            fresh
                .records_between(starts[1], starts[2] - 1)
                .await
                .unwrap()
        );
    });
}

/// A view keeps every record it read, so even reading from memory only it serves any run's
/// records, and never reads the log again.
fn a_view_keeps_every_record_it_read(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (session, starts) = history(&backend, cipher).await;
        let fresh = view(&backend, cipher).await;
        assert_eq!(fresh.kept_run_start("c"), Some(starts[2]));
        let expected = fresh.records_from(1).await.unwrap();
        // A record appended after the view was loaded is not part of it.
        let mut session = session;
        session.append(&[event("later")], true).await.unwrap();
        let _memory = fresh.in_memory();
        assert_eq!(fresh.records_from(1).await.unwrap(), expected);
        assert_eq!(fresh.open_log_records().await.unwrap(), expected);
    });
}

/// After a seal the sealed history is read from storage: reading from memory only refuses it, and
/// the records kept before the seal are dropped, since the new log was copied, not appended. A run
/// started after the seal is kept again.
fn a_seal_drops_the_kept_records_and_memory_only_refuses_sealed_history(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, starts) = history(&backend, cipher).await;
        session.seal(Some(starts[2] + 1), 1_000).await.unwrap();
        assert_eq!(session.kept_records(), 0);
        assert_eq!(session.kept_run_start("c"), None);
        {
            let _memory = session.in_memory();
            assert!(matches!(
                session.entries(1, 1).await,
                Err(SessionError::NotInMemory)
            ));
            assert!(matches!(
                session.find_id(IdKind::RunId, "a").await,
                Err(SessionError::NotInMemory)
            ));
            // The window is in memory.
            let window = session.conversation.window_start();
            assert_eq!(session.entries(window, 1).await.unwrap().len(), 1);
        }
        // The guard is gone: storage is read again.
        assert_eq!(session.entries(1, 1).await.unwrap().len(), 1);

        session.append(&[end("c")], true).await.unwrap();
        let d = session.append(&[start("d")], true).await.unwrap();
        session.append(&[input("d", "ask d")], true).await.unwrap();
        assert_eq!(session.kept_run_start("d"), Some(d));
        let in_memory = {
            let _memory = session.in_memory();
            session.records_from(d).await.unwrap()
        };
        assert_eq!(
            in_memory,
            view(&backend, cipher).await.records_from(d).await.unwrap()
        );
        assert_eq!(in_memory.len(), 2);
    });
}

/// A batch appended without a barrier leaves the log unsynced until a sync writes a claim frame
/// with one; a second sync with nothing new writes nothing.
fn a_sync_makes_an_unsynced_tail_durable_once(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, _) = history(&backend, cipher).await;
        assert!(!session.unsynced());
        session.append(&[event("unsynced")], false).await.unwrap();
        assert!(session.unsynced());
        session.sync().await.unwrap();
        assert!(!session.unsynced());
        let (_, synced_len) = session.open_log();
        session.sync().await.unwrap();
        assert_eq!(
            session.open_log().1,
            synced_len,
            "nothing to sync, nothing written"
        );
        // The log still loads, with the record the sync made durable.
        let loaded = view(&backend, cipher).await;
        assert!(loaded
            .open_log_records()
            .await
            .unwrap()
            .iter()
            .any(|(_, record)| matches!(record, Record::Event(e) if e.name == "unsynced")));
    });
}

/// A sync that fails leaves the tail unsynced and the writer poisoned: nothing is answered from
/// the session as though it were durable.
fn a_failed_sync_leaves_the_tail_unsynced(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, _) = history(&backend, cipher).await;
        session.append(&[event("unsynced")], false).await.unwrap();
        backend.fail_once(0);
        assert!(session.sync().await.is_err());
        assert!(session.unsynced());
        assert!(matches!(session.sync().await, Err(SessionError::Poisoned)));
    });
}

/// A view loaded at a position reads the log only as far as the position: what was appended since
/// isn't in it. Once the head moves, as a seal moves it, the position refuses.
fn a_view_at_a_position_reads_only_that_far(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let (mut session, starts) = history(&backend, cipher).await;
        let position = Session::stored_position(&&backend, &addr(), &deployment(cipher))
            .await
            .unwrap()
            .unwrap();
        let before = view(&backend, cipher).await.records_from(1).await.unwrap();
        session.append(&[event("after")], true).await.unwrap();
        let at = Session::view_at(&backend, addr(), &triple(), &deployment(cipher), &position)
            .await
            .unwrap();
        assert_eq!(at.records_from(1).await.unwrap(), before);
        let now = Session::stored_position(&&backend, &addr(), &deployment(cipher))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(now.version, position.version);
        assert!(now.len > position.len);

        session.seal(Some(starts[2] + 1), 1_000).await.unwrap();
        assert!(matches!(
            Session::view_at(&backend, addr(), &triple(), &deployment(cipher), &now).await,
            Err(SessionError::Moved)
        ));
    });
}
