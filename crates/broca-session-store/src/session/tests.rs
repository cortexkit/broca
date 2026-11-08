use broca_protocol::content::{ContentBlock, Message, Role};
use futures::executor::block_on;

use super::{Session, SessionError};
use crate::backend::{LocalBackend, MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::Message as _;
use crate::head::SessionTriple;
use crate::keys::DeploymentKey;
use crate::record::{Event, Input, Placement, Record};

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

fn input(text: &str) -> Record {
    Record::Input(Input {
        media: Vec::new(),
        run_id: "r".into(),
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

/// The state a session holds, as bytes, so two sessions can be compared.
fn state<B: SessionBackend>(session: &Session<B>) -> (Vec<u8>, Vec<u8>) {
    (session.conversation.encode(), session.operations.encode())
}

fn create_and_write<B: SessionBackend>(backend: &B) -> (Vec<u8>, Vec<u8>) {
    block_on(async {
        let mut session = Session::create(backend, addr(), triple(), &deployment(), false)
            .await
            .unwrap();
        session.append(&[input("one")], true).await.unwrap();
        session
            .append(&[event("a"), input("two")], false)
            .await
            .unwrap();
        session.append(&[event("b")], true).await.unwrap();
        let written = state(&session);
        session.close().await.unwrap();
        written
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

#[test]
fn a_created_session_loads_back_with_the_same_state() {
    let backend = MemoryBackend::default();
    let written = create_and_write(&backend);
    block_on(async {
        let mut session = Session::load(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        assert_eq!(state(&session), written);
        assert_eq!(texts(&session.conversation.messages), ["one", "two"]);
        // Seqs continue: creation's WriterOpened took 1, the three batches 2 to 5, and this
        // load's WriterOpened 6.
        assert_eq!(session.append(&[input("three")], true).await.unwrap(), 7);
    });
}

#[test]
fn the_same_holds_on_local_files() {
    let root = std::env::temp_dir().join(format!("broca-session-test-{}", std::process::id()));
    let backend = LocalBackend::new(&root);
    let written = create_and_write(&backend);
    // A fresh backend, as another process after a restart would have.
    let reopened = LocalBackend::new(&root);
    let loaded = block_on(async {
        let session = Session::load(&reopened, addr(), &triple(), &deployment())
            .await
            .unwrap();
        state(&session)
    });
    std::fs::remove_dir_all(&root).unwrap();
    assert_eq!(loaded, written);
}

/// A writer whose head was replaced by another writer's can't append: the backend refuses it.
#[test]
fn a_replaced_writer_is_fenced() {
    let backend = MemoryBackend::default();
    create_and_write(&backend);
    block_on(async {
        let mut first = Session::load(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        let mut second = Session::load(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        assert!(matches!(
            first.append(&[input("late")], true).await,
            Err(SessionError::Backend(crate::backend::BackendError::Fenced))
        ));
        // A fenced writer is poisoned.
        assert!(matches!(
            first.append(&[input("later")], true).await,
            Err(SessionError::Poisoned)
        ));
        second.append(&[input("current")], true).await.unwrap();
    });
}

/// A head that moved with no append to the log (another writer's head-only update) still fences
/// the stale writer, even though the log ends where it expects.
#[test]
fn a_moved_head_alone_fences_the_writer() {
    let backend = MemoryBackend::default();
    create_and_write(&backend);
    block_on(async {
        let mut session = Session::load(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        let (stored, version) = backend.read_head(&addr()).await.unwrap().unwrap();
        backend
            .write_head(&addr(), &stored, Some(version), version + 1)
            .await
            .unwrap();
        assert!(matches!(
            session.append(&[input("late")], true).await,
            Err(SessionError::Backend(crate::backend::BackendError::Fenced))
        ));
    });
}

/// An append cut off mid-write is dropped, the log cut back, and the writer continues after
/// the last complete batch.
#[test]
fn a_torn_tail_is_cut_on_load() {
    let backend = MemoryBackend::default();
    let (log, end) = block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(), false)
            .await
            .unwrap();
        session.append(&[input("one")], true).await.unwrap();
        let synced = session.open_log();
        session.append(&[input("unsynced")], false).await.unwrap();
        synced
    });
    let bytes = block_on(backend.read_log(&addr(), &log, 0..end + 40)).unwrap();
    backend.set_log(&addr(), &log, bytes);
    block_on(async {
        let mut session = Session::load(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        assert_eq!(texts(&session.conversation.messages), ["one"]);
        session.append(&[input("two")], true).await.unwrap();
    });
    let session = block_on(Session::load(&backend, addr(), &triple(), &deployment())).unwrap();
    assert_eq!(texts(&session.conversation.messages), ["one", "two"]);
}

/// A flipped byte in the last batch is damage. No later frame claims that batch durable, so it is
/// debris that only a recovery may repair: loading refuses without changing anything.
#[test]
fn damage_refuses_and_changes_nothing() {
    let backend = MemoryBackend::default();
    create_and_write(&backend);
    let (stored, _) = block_on(backend.read_head(&addr())).unwrap().unwrap();
    let (head, _) = crate::head::open(&stored, &deployment()).unwrap();
    let log = head.open_log.unwrap().change;
    let end = block_on(backend.log_end(&addr(), &log)).unwrap();
    let mut bytes = block_on(backend.read_log(&addr(), &log, 0..end)).unwrap();
    let flip = bytes.len() - 60;
    bytes[flip] ^= 1;
    backend.set_log(&addr(), &log, bytes.clone());
    let error = block_on(Session::load(&backend, addr(), &triple(), &deployment())).err();
    assert!(
        matches!(error, Some(SessionError::NeedsRecovery)),
        "{error:?}"
    );
    assert_eq!(
        block_on(backend.read_log(&addr(), &log, 0..end)).unwrap(),
        bytes
    );
    let (after, _) = block_on(backend.read_head(&addr())).unwrap().unwrap();
    assert_eq!(after, stored);
}

#[test]
fn another_triple_or_deployment_key_refuses() {
    let backend = MemoryBackend::default();
    create_and_write(&backend);
    let mut other = triple();
    other.session = "t".into();
    assert!(matches!(
        block_on(Session::load(&backend, addr(), &other, &deployment())),
        Err(SessionError::TripleMismatch)
    ));
    assert!(matches!(
        block_on(Session::load(
            &backend,
            addr(),
            &triple(),
            &DeploymentKey::new(1, &[6; 32])
        )),
        Err(SessionError::Head(_))
    ));
}

/// A view reads what a writer still holding the session wrote, writes nothing (not even the
/// generation a writer's open commits), and reads a torn tail as not yet written without
/// cutting it.
#[test]
fn a_view_reads_a_held_session_and_writes_nothing() {
    let backend = MemoryBackend::default();
    let (log, synced) = block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(), false)
            .await
            .unwrap();
        session.append(&[input("one")], true).await.unwrap();
        session.append(&[input("two")], true).await.unwrap();
        let view = Session::view(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        assert_eq!(texts(&view.conversation.messages), ["one", "two"]);
        assert_eq!(view.next_seq(), session.next_seq());
        session.open_log()
    });
    let (head_before, _) = block_on(backend.read_head(&addr())).unwrap().unwrap();
    // A write cut short past the last batch.
    let mut bytes = block_on(backend.read_log(&addr(), &log, 0..synced)).unwrap();
    bytes.extend_from_slice(&[0xA5; 40]);
    backend.set_log(&addr(), &log, bytes.clone());
    let view = block_on(Session::view(&backend, addr(), &triple(), &deployment())).unwrap();
    assert_eq!(texts(&view.conversation.messages), ["one", "two"]);
    drop(view);
    assert_eq!(
        block_on(backend.read_log(&addr(), &log, 0..synced + 40)).unwrap(),
        bytes,
        "the view left the torn tail in place"
    );
    let (head_after, _) = block_on(backend.read_head(&addr())).unwrap().unwrap();
    assert_eq!(head_after, head_before, "the view committed no head");
}

/// A view refuses a log only a recovery may repair, and changes nothing.
#[test]
fn a_view_of_a_damaged_log_refuses_and_changes_nothing() {
    let backend = MemoryBackend::default();
    create_and_write(&backend);
    let (stored, _) = block_on(backend.read_head(&addr())).unwrap().unwrap();
    let (head, _) = crate::head::open(&stored, &deployment()).unwrap();
    let log = head.open_log.unwrap().change;
    let end = block_on(backend.log_end(&addr(), &log)).unwrap();
    let mut bytes = block_on(backend.read_log(&addr(), &log, 0..end)).unwrap();
    let flip = bytes.len() - 60;
    bytes[flip] ^= 1;
    backend.set_log(&addr(), &log, bytes.clone());
    let error = block_on(Session::view(&backend, addr(), &triple(), &deployment())).err();
    assert!(
        matches!(error, Some(SessionError::NeedsRecovery)),
        "{error:?}"
    );
    assert_eq!(
        block_on(backend.read_log(&addr(), &log, 0..end)).unwrap(),
        bytes
    );
}

/// The open log's newest activity time is the newest time any of its records carries, whatever
/// order they were appended in, and a load for a read finds the same.
#[test]
fn the_open_logs_newest_activity_is_kept_and_read_back() {
    let backend = MemoryBackend::default();
    let timed = |text: &str, at: u64| {
        let mut record = input(text);
        if let Record::Input(input) = &mut record {
            input.written_at_ms = Some(at);
        }
        record
    };
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(), false)
            .await
            .unwrap();
        assert_eq!(session.open_log_activity_ms(), None);
        session
            .append(&[timed("one", 9), event("a")], true)
            .await
            .unwrap();
        session.append(&[timed("two", 5)], true).await.unwrap();
        assert_eq!(session.open_log_activity_ms(), Some(9));
        session.close().await.unwrap();
        let view = Session::view(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        assert_eq!(view.open_log_activity_ms(), Some(9));
    });
}

/// The observer of a session's appends hears of a batch only once its append returned, under
/// the seqs its records took: a batch the backend refuses is never told.
#[test]
fn an_append_is_observed_only_once_it_returned() {
    use std::sync::{Arc, Mutex};
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(), false)
            .await
            .unwrap();
        let told = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&told);
        session.observe_appends(Box::new(move |appended| {
            if let super::Appended::Records { first, records } = appended {
                seen.lock().unwrap().push((first, records.len()));
            }
        }));
        let first = session
            .append(&[input("one"), event("a")], true)
            .await
            .unwrap();
        backend.fail_once(0);
        assert!(session.append(&[input("two")], true).await.is_err());
        assert_eq!(*told.lock().unwrap(), [(first, 2)]);
    });
}
