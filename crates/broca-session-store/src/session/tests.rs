use broca_protocol::content::{ContentBlock, Message, Role};
use futures::executor::block_on;

use super::{Session, SessionError};
use crate::backend::{LocalBackend, MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::Message as _;
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{Event, Input, Placement, Record};

cipher_tests!(
    a_created_session_loads_back_with_the_same_state,
    the_same_holds_on_local_files,
    a_replaced_writer_is_fenced,
    a_moved_head_alone_fences_the_writer,
    a_torn_tail_is_cut_on_load,
    damage_refuses_and_changes_nothing,
    another_triple_or_deployment_key_refuses,
    a_view_reads_a_held_session_and_writes_nothing,
    a_view_of_a_damaged_log_refuses_and_changes_nothing,
    the_open_logs_newest_activity_is_kept_and_read_back,
    an_append_is_observed_only_once_it_returned,
    an_append_without_a_barrier_is_told_durable_once_synced,
    a_discarded_session_reads_as_never_created,
    a_deleted_session_keeps_no_file_and_a_moved_head_refuses,
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

fn create_and_write<B: SessionBackend>(backend: &B, cipher: Cipher) -> (Vec<u8>, Vec<u8>) {
    block_on(async {
        let mut session = Session::create(backend, addr(), triple(), &deployment(cipher), false)
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

#[test]
fn unpublished_staging_has_byte_identical_final_artifacts() {
    use crate::backend::unpublished::UnpublishedBackend;
    use crate::codec::hex;
    use sha2::{Digest, Sha256};

    fn tree(root: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
        fn walk(
            root: &std::path::Path,
            dir: &std::path::Path,
            files: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
        ) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(root, &path, files);
                } else {
                    files.insert(
                        path.strip_prefix(root).unwrap().to_owned(),
                        std::fs::read(path).unwrap(),
                    );
                }
            }
        }
        let mut files = std::collections::BTreeMap::new();
        walk(root, root, &mut files);
        files
    }

    fn build<B: SessionBackend>(backend: &B, cipher: Cipher) {
        crate::keys::test_entropy::replay(|| {
            block_on(async {
                let deployment = deployment(cipher);
                let mut session = Session::create_with_history_version(
                    backend,
                    addr(),
                    triple(),
                    &deployment,
                    true,
                    519,
                )
                .await
                .unwrap();
                let setup_key = deployment.setup_key_for(cipher).unwrap();
                crate::setup::SetupStore::new(backend, &setup_key)
                    .put(crate::record::PartKind::Tools, b"[]")
                    .await
                    .unwrap();
                let bytes = b"native image bytes";
                let reference = serde_json::from_value(serde_json::json!({"kind":"image", "mime":"image/png", "byte_size":bytes.len(), "sha256":hex(&Sha256::digest(bytes)), "width":1,"height":1})).unwrap();
                let Record::Input(mut photo) = input("look") else {
                    unreachable!()
                };
                photo.message.content.push(ContentBlock::Media(reference));
                session
                    .append_input_media(
                        &[Record::Input(photo)],
                        vec![super::NewInputMedia {
                            record: 0,
                            message: 0,
                            block: 1,
                            part: None,
                            bytes: bytes.to_vec(),
                        }],
                        true,
                    )
                    .await
                    .unwrap();
                session.append(&[event("complete")], true).await.unwrap();
                assert_eq!(session.head().history_version, 519);
                assert_eq!(session.version, 3);
                session.close().await.unwrap();
            })
        });
    }

    for cipher in Cipher::ALL {
        let root = broca_test_support::scratch_root().join(format!(
            "staging-bytes-{}-{}",
            cipher.label(),
            hex(&crate::keys::random_id().0)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let live = root.join("live");
        let staged = root.join("staged");
        build(&LocalBackend::new(&live), cipher);
        let backend = UnpublishedBackend::staging(&staged, None);
        build(&backend, cipher);
        backend.finish().unwrap();
        let mut expected = tree(&live);
        for version in [1, 2] {
            expected
                .remove(&std::path::PathBuf::from(format!(
                    "heads/{}/v/{version}",
                    addr().0
                )))
                .unwrap();
        }
        let actual = tree(&staged);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in actual.keys() {
                assert_eq!(
                    std::fs::metadata(staged.join(path))
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600,
                    "{}",
                    path.display()
                );
            }
        }
        assert!(actual.keys().any(|path| path.starts_with("objects")));
        assert!(actual
            .keys()
            .any(|path| path.starts_with(if cipher == Cipher::None {
                "setup-plain"
            } else {
                "setup"
            })));
        assert_eq!(actual, expected, "{}", cipher.label());
        block_on(async {
            let view = Session::view(
                LocalBackend::new(&staged),
                addr(),
                &triple(),
                &deployment(cipher),
            )
            .await
            .unwrap();
            assert_eq!(texts(&view.conversation.messages), ["look"]);
        });
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// A byte 23 bytes into the claim-only frame `close` writes last, which is inside that frame's
/// header, in a log `len` bytes long whose frames are sealed in mode `cipher`. A claim frame
/// has an empty body, so it is exactly the mode's frame overhead long.
fn in_closing_claim(cipher: Cipher, len: usize) -> usize {
    let claim = crate::frame::FrameKey::for_tests(cipher, 1, &[0; 32]).frame_overhead();
    len - claim + 23
}

/// A discarded session reads as one that was never created: loading it finds nothing, on local
/// files no folder of it is left, and a session can be created at its address again. A writer
/// another writer replaced can't discard the session.
fn a_discarded_session_reads_as_never_created(cipher: Cipher) {
    let root = std::env::temp_dir().join(format!(
        "broca-session-discard-{}-{}",
        std::process::id(),
        cipher.label()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let backend = LocalBackend::new(&root);
    block_on(async {
        let session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        session.discard().await.unwrap();
        assert!(matches!(
            Session::load(&backend, addr(), &triple(), &deployment(cipher)).await,
            Err(SessionError::Missing)
        ));
        for folder in ["heads", "logs", "objects"] {
            assert!(
                !root.join(folder).join(&addr().0).exists(),
                "{folder} of the discarded session is left"
            );
        }
        let stale = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mut current = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert!(matches!(
            stale.discard().await,
            Err(SessionError::Backend(crate::backend::BackendError::Fenced))
        ));
        current.append(&[input("kept")], true).await.unwrap();
    });
    std::fs::remove_dir_all(&root).unwrap();

    let memory = MemoryBackend::default();
    block_on(async {
        let session = Session::create(&memory, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        session.discard().await.unwrap();
        assert_eq!(memory.read_head(&addr()).await.unwrap(), None);
        assert!(memory.log_names(&addr()).is_empty());
    });
}

/// A session deleted by a caller that opened no writer, at the head version it read, reads as one
/// that was never created, and on local files nothing of it is left: not its head, its kept head
/// versions, its logs, nor its objects (sealed segments, directory and id-index pages). A version
/// the head has moved past refuses and deletes nothing.
fn a_deleted_session_keeps_no_file_and_a_moved_head_refuses(cipher: Cipher) {
    let root = std::env::temp_dir().join(format!(
        "broca-session-delete-{}-{}",
        std::process::id(),
        cipher.label()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let backend = LocalBackend::new(&root);
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        session.append(&[input("one")], true).await.unwrap();
        session.seal(None, 1_000).await.unwrap();
        session.append(&[input("two")], true).await.unwrap();
        session.close().await.unwrap();
        for folder in ["heads", "logs", "objects"] {
            assert!(root.join(folder).join(&addr().0).exists(), "{folder}");
        }
        assert!(backend.head_versions(&addr()).unwrap().len() > 1);
        let position = Session::stored_position(&backend, &addr(), &deployment(cipher))
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(
            Session::delete(&backend, &addr(), position.version - 1).await,
            Err(SessionError::Backend(crate::backend::BackendError::Fenced))
        ));
        assert!(
            Session::view(&backend, addr(), &triple(), &deployment(cipher))
                .await
                .is_ok()
        );

        Session::delete(&backend, &addr(), position.version)
            .await
            .unwrap();
        assert!(matches!(
            Session::view(&backend, addr(), &triple(), &deployment(cipher)).await,
            Err(SessionError::Missing)
        ));
        for folder in ["heads", "logs", "objects"] {
            assert!(
                !root.join(folder).join(&addr().0).exists(),
                "{folder} of the deleted session is left"
            );
        }
        assert!(backend.stored_sessions().unwrap().is_empty());
    });
    std::fs::remove_dir_all(&root).unwrap();
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

fn a_created_session_loads_back_with_the_same_state(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let written = create_and_write(&backend, cipher);
    block_on(async {
        let mut session = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(state(&session), written);
        assert_eq!(texts(&session.conversation.messages), ["one", "two"]);
        // Seqs continue: creation's WriterOpened took 1, the three batches 2 to 5, and this
        // load's WriterOpened 6.
        assert_eq!(session.append(&[input("three")], true).await.unwrap(), 7);
    });
}

fn the_same_holds_on_local_files(cipher: Cipher) {
    let root = std::env::temp_dir().join(format!(
        "broca-session-test-{}-{}",
        std::process::id(),
        cipher.label()
    ));
    let backend = LocalBackend::new(&root);
    let written = create_and_write(&backend, cipher);
    // A fresh backend, as another process after a restart would have.
    let reopened = LocalBackend::new(&root);
    let loaded = block_on(async {
        let session = Session::load(&reopened, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        state(&session)
    });
    std::fs::remove_dir_all(&root).unwrap();
    assert_eq!(loaded, written);
}

#[cfg(unix)]
#[test]
fn a_session_with_legacy_unix_permissions_still_loads() {
    use std::os::unix::fs::PermissionsExt;

    fn legacy_modes(path: &std::path::Path, modes: &mut Vec<(std::path::PathBuf, u32)>) {
        let mode = if path.is_dir() { 0o755 } else { 0o644 };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
        modes.push((path.to_owned(), mode));
        if path.is_dir() {
            for entry in std::fs::read_dir(path).unwrap() {
                legacy_modes(&entry.unwrap().path(), modes);
            }
        }
    }

    for cipher in Cipher::ALL {
        let root = std::env::temp_dir().join(format!(
            "broca-session-legacy-{}",
            crate::codec::hex(&crate::keys::random_id().0)
        ));
        let written = create_and_write(&LocalBackend::new(&root), cipher);
        // The byte format is unchanged; only the creation modes differ from older writers.
        let mut modes = Vec::new();
        legacy_modes(&root, &mut modes);
        let reopened = LocalBackend::new(&root);
        let loaded = block_on(async {
            let session = Session::load(&reopened, addr(), &triple(), &deployment(cipher))
                .await
                .unwrap();
            state(&session)
        });
        assert_eq!(loaded, written);
        for (path, mode) in modes {
            // Loading as a writer replaces the head to claim it. Other existing objects are
            // opened in place, so their legacy permissions must not be silently repaired.
            let expected = if path == root.join("heads").join(&addr().0).join("head") {
                0o600
            } else {
                mode
            };
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                expected,
                "unexpected mode for {}",
                path.display()
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// A writer whose head was replaced by another writer's can't append: the backend refuses it.
fn a_replaced_writer_is_fenced(cipher: Cipher) {
    let backend = MemoryBackend::default();
    create_and_write(&backend, cipher);
    block_on(async {
        let mut first = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        let mut second = Session::load(&backend, addr(), &triple(), &deployment(cipher))
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
fn a_moved_head_alone_fences_the_writer(cipher: Cipher) {
    let backend = MemoryBackend::default();
    create_and_write(&backend, cipher);
    block_on(async {
        let mut session = Session::load(&backend, addr(), &triple(), &deployment(cipher))
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
fn a_torn_tail_is_cut_on_load(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (log, end) = block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
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
        let mut session = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(texts(&session.conversation.messages), ["one"]);
        session.append(&[input("two")], true).await.unwrap();
    });
    let session = block_on(Session::load(
        &backend,
        addr(),
        &triple(),
        &deployment(cipher),
    ))
    .unwrap();
    assert_eq!(texts(&session.conversation.messages), ["one", "two"]);
}

/// A flipped byte in the last batch is damage. No later frame claims that batch durable, so it is
/// debris that only a recovery may repair: loading refuses without changing anything.
fn damage_refuses_and_changes_nothing(cipher: Cipher) {
    let backend = MemoryBackend::default();
    create_and_write(&backend, cipher);
    let (stored, _) = block_on(backend.read_head(&addr())).unwrap().unwrap();
    let (head, _) = crate::head::open(&stored, &deployment(cipher)).unwrap();
    let log = head.open_log.unwrap().change;
    let end = block_on(backend.log_end(&addr(), &log)).unwrap();
    let mut bytes = block_on(backend.read_log(&addr(), &log, 0..end)).unwrap();
    let flip = in_closing_claim(cipher, bytes.len());
    bytes[flip] ^= 1;
    backend.set_log(&addr(), &log, bytes.clone());
    let error = block_on(Session::load(
        &backend,
        addr(),
        &triple(),
        &deployment(cipher),
    ))
    .err();
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

/// Another triple refuses in both modes. Another deployment key refuses an encrypted session,
/// which can't unwrap its data key; a plain session needs no deployment key, so it opens with
/// any.
fn another_triple_or_deployment_key_refuses(cipher: Cipher) {
    let backend = MemoryBackend::default();
    create_and_write(&backend, cipher);
    let mut other = triple();
    other.session = "t".into();
    assert!(matches!(
        block_on(Session::load(&backend, addr(), &other, &deployment(cipher))),
        Err(SessionError::TripleMismatch)
    ));
    let loaded = block_on(Session::load(
        &backend,
        addr(),
        &triple(),
        &DeploymentKey::new(1, &[6; 32]),
    ));
    match cipher {
        Cipher::XChaCha20Poly1305 => assert!(matches!(loaded, Err(SessionError::Head(_)))),
        Cipher::None => assert!(loaded.is_ok()),
    }
}

/// A view reads what a writer still holding the session wrote, writes nothing (not even the
/// generation a writer's open commits), and reads a torn tail as not yet written without
/// cutting it.
fn a_view_reads_a_held_session_and_writes_nothing(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (log, synced) = block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        session.append(&[input("one")], true).await.unwrap();
        session.append(&[input("two")], true).await.unwrap();
        let view = Session::view(&backend, addr(), &triple(), &deployment(cipher))
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
    let view = block_on(Session::view(
        &backend,
        addr(),
        &triple(),
        &deployment(cipher),
    ))
    .unwrap();
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
fn a_view_of_a_damaged_log_refuses_and_changes_nothing(cipher: Cipher) {
    let backend = MemoryBackend::default();
    create_and_write(&backend, cipher);
    let (stored, _) = block_on(backend.read_head(&addr())).unwrap().unwrap();
    let (head, _) = crate::head::open(&stored, &deployment(cipher)).unwrap();
    let log = head.open_log.unwrap().change;
    let end = block_on(backend.log_end(&addr(), &log)).unwrap();
    let mut bytes = block_on(backend.read_log(&addr(), &log, 0..end)).unwrap();
    let flip = in_closing_claim(cipher, bytes.len());
    bytes[flip] ^= 1;
    backend.set_log(&addr(), &log, bytes.clone());
    let error = block_on(Session::view(
        &backend,
        addr(),
        &triple(),
        &deployment(cipher),
    ))
    .err();
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
fn the_open_logs_newest_activity_is_kept_and_read_back(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let timed = |text: &str, at: u64| {
        let mut record = input(text);
        if let Record::Input(input) = &mut record {
            input.written_at_ms = Some(at);
        }
        record
    };
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
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
        let view = Session::view(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(view.open_log_activity_ms(), Some(9));
    });
}

/// The observer of a session's appends hears of a batch only once its append returned, under
/// the seqs its records took: a batch the backend refuses is never told.
fn an_append_is_observed_only_once_it_returned(cipher: Cipher) {
    use std::sync::{Arc, Mutex};
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let told = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&told);
        session.observe_appends(Box::new(move |appended| match appended {
            super::Appended::Records {
                first,
                records,
                durable,
            } => seen
                .lock()
                .unwrap()
                .push(format!("{first}+{} {durable}", records.len())),
            super::Appended::Synced => seen.lock().unwrap().push("synced".into()),
            super::Appended::Replaced => seen.lock().unwrap().push("replaced".into()),
        }));
        let first = session
            .append(&[input("one"), event("a")], true)
            .await
            .unwrap();
        backend.fail_once(0);
        assert!(session.append(&[input("two")], true).await.is_err());
        assert_eq!(*told.lock().unwrap(), [format!("{first}+2 true")]);
    });
}

/// A batch appended without a barrier is told as not yet durable, and the sync that makes it
/// durable is told after it.
fn an_append_without_a_barrier_is_told_durable_once_synced(cipher: Cipher) {
    use std::sync::{Arc, Mutex};
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let told = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&told);
        session.observe_appends(Box::new(move |appended| match appended {
            super::Appended::Records { durable, .. } => {
                seen.lock()
                    .unwrap()
                    .push(if durable { "durable" } else { "written" })
            }
            super::Appended::Synced => seen.lock().unwrap().push("synced"),
            super::Appended::Replaced => seen.lock().unwrap().push("replaced"),
        }));
        session.append(&[event("a")], false).await.unwrap();
        session.sync().await.unwrap();
        session.sync().await.unwrap();
        assert_eq!(*told.lock().unwrap(), ["written", "synced"]);
    });
}

/// A session's mode is chosen when it is created and kept in its head: a deployment key that
/// creates plain sessions creates the next one plain, and an encrypted session created before
/// stays encrypted when it loads under that key. A plain session loads before the deployment key
/// has loaded; an encrypted one refuses until it has.
#[test]
fn the_mode_is_chosen_at_creation_and_kept() {
    let encrypted_addr = SessionAddr("encrypted0000000".into());
    let plain_addr = SessionAddr("plain00000000000".into());
    let backend = MemoryBackend::default();
    let stored_mode = |addr: &SessionAddr| {
        let (stored, _) = block_on(backend.read_head(addr)).unwrap().unwrap();
        crate::head::stored_cipher(&stored).unwrap()
    };
    block_on(async {
        let session = Session::create(
            &backend,
            encrypted_addr.clone(),
            triple(),
            &deployment(Cipher::XChaCha20Poly1305),
            false,
        )
        .await
        .unwrap();
        session.close().await.unwrap();
        // The deployment now creates plain sessions.
        let plain_now = deployment(Cipher::None);
        let session = Session::create(&backend, plain_addr.clone(), triple(), &plain_now, false)
            .await
            .unwrap();
        assert_eq!(session.head().cipher(), Cipher::None);
        assert!(session.head().keys.is_empty());
        session.close().await.unwrap();
        let session = Session::load(&backend, encrypted_addr.clone(), &triple(), &plain_now)
            .await
            .unwrap();
        assert_eq!(session.head().cipher(), Cipher::XChaCha20Poly1305);
        session.close().await.unwrap();
    });
    assert_eq!(stored_mode(&encrypted_addr), Cipher::XChaCha20Poly1305);
    assert_eq!(stored_mode(&plain_addr), Cipher::None);

    let unloaded = DeploymentKey::unloaded(1);
    let plain = block_on(Session::load(&backend, plain_addr, &triple(), &unloaded)).unwrap();
    assert_eq!(plain.head().cipher(), Cipher::None);
    assert!(matches!(
        block_on(Session::load(
            &backend,
            encrypted_addr,
            &triple(),
            &unloaded
        )),
        Err(SessionError::Head(crate::head::HeadError::NotLoaded))
    ));
    // An unloaded key can't create an encrypted session.
    assert!(matches!(
        block_on(Session::create(
            &backend,
            SessionAddr("refused000000000".into()),
            triple(),
            &DeploymentKey::unloaded(1).creating(Cipher::XChaCha20Poly1305),
            false,
        )),
        Err(SessionError::Head(crate::head::HeadError::NotLoaded))
    ));
}
