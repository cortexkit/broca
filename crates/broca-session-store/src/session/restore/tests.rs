use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, MediaRef, Message, Role};
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;
use sha2::{Digest as _, Sha256};

use super::{check, restore, restore_with, Finding, Restored};
use crate::backend::{LocalBackend, SessionAddr};
use crate::codec::{hex, Message as _};
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{Input, PartKind, Placement, Record, RunEnd, RunStart};
use crate::session::{NewInputMedia, Session, SessionError};
use crate::setup::SetupStore;

cipher_tests!(
    a_walk_that_passed_a_seal_rolls_back_to_the_version_before_it,
    a_walk_that_passed_a_new_medium_cuts_the_log_before_its_record,
    a_walk_that_passed_a_new_setup_part_cuts_the_log_before_its_record,
    a_log_read_in_the_middle_of_a_batch_is_kept_as_a_torn_tail,
    a_complete_session_is_kept_and_nothing_is_written,
    a_file_every_version_names_leaves_the_session_unrecoverable_and_unchanged,
    damage_that_is_not_a_lacking_file_is_refused_and_unchanged,
);

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "s".into(),
    }
}

fn deployment(cipher: Cipher) -> DeploymentKey {
    DeploymentKey::new(1, &[5; 32]).creating(cipher)
}

fn addr() -> SessionAddr {
    SessionAddr("0123456789abcdef".into())
}

/// A store root of its own, removed when the test ends.
struct Root(PathBuf);

impl Root {
    fn new(name: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "broca-restore-{name}-{}",
            hex(&crate::keys::random_id().0)
        )))
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Every file under `root`, relative to it.
fn files(root: &Path) -> Vec<PathBuf> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.push(path.strip_prefix(root).unwrap().to_path_buf());
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// Every file under `root` with its bytes, to compare a store before and after.
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    files(root)
        .into_iter()
        .map(|file| {
            let bytes = std::fs::read(root.join(&file)).unwrap();
            (file, bytes)
        })
        .collect()
}

/// A backup's walk of a live store, as Engram makes one: it lists every file first, then reads
/// each later. A file created after the listing isn't in the backup, and a file listed is stored
/// as it reads when it is read, a log grown and a head replaced since included.
struct Walk {
    from: PathBuf,
    listed: Vec<PathBuf>,
}

impl Walk {
    fn list(from: &Path) -> Self {
        Self {
            from: from.to_path_buf(),
            listed: files(from),
        }
    }

    /// The backup, read now, restored into `to`.
    fn restore_into(&self, to: &Path) {
        for file in &self.listed {
            let Ok(bytes) = std::fs::read(self.from.join(file)) else {
                continue;
            };
            let target = to.join(file);
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            std::fs::write(target, bytes).unwrap();
        }
    }
}

fn model() -> ModelHandle {
    serde_json::from_value(serde_json::json!({"provider_module_id": "p", "model_id": "m"})).unwrap()
}

fn start(run: &str, setup: Vec<crate::record::PartRef>) -> Record {
    Record::RunStart(Box::new(RunStart {
        named_speaker: None,
        imported_ids: None,
        message_marks: None,
        run_id: run.into(),
        model: model(),
        setup,
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

fn input(run: &str, message: Message) -> Record {
    Record::Input(Input {
        media: Vec::new(),
        run_id: run.into(),
        placement: Placement::RunInput,
        message,
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

fn text(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn image(bytes: &[u8]) -> MediaRef {
    serde_json::from_value(serde_json::json!({
        "kind": "image",
        "mime": "image/png",
        "byte_size": bytes.len(),
        "sha256": hex(&Sha256::digest(bytes)),
        "width": 1,
        "height": 1,
    }))
    .unwrap()
}

/// The state a session holds, as bytes, so two sessions can be compared.
fn state<B: crate::backend::SessionBackend>(session: &Session<B>) -> (Vec<u8>, Vec<u8>) {
    (session.conversation.encode(), session.operations.encode())
}

/// Two finished runs, the second's input carrying an image.
async fn history(backend: &LocalBackend, cipher: Cipher) -> Session<&LocalBackend> {
    let mut session = Session::create(backend, addr(), triple(), &deployment(cipher), false)
        .await
        .unwrap();
    session
        .append(&[start("a", Vec::new())], true)
        .await
        .unwrap();
    session
        .append(&[input("a", text("ask a"))], true)
        .await
        .unwrap();
    session.append(&[end("a")], true).await.unwrap();
    session
        .append(&[start("b", Vec::new())], true)
        .await
        .unwrap();
    session
        .append_input_media(
            &[input("b", photo("ask b", b"first image"))],
            vec![image_bytes(b"first image")],
            true,
        )
        .await
        .unwrap();
    session.append(&[end("b")], true).await.unwrap();
    session
}

/// A message holding `text` and then the image `bytes`.
fn photo(text: &str, bytes: &[u8]) -> Message {
    Message::new(
        Role::User,
        vec![
            ContentBlock::Text { text: text.into() },
            ContentBlock::Media(image(bytes)),
        ],
    )
}

/// The bytes of a `photo`'s image, for the first record of a batch.
fn image_bytes(bytes: &[u8]) -> NewInputMedia {
    NewInputMedia {
        record: 0,
        message: 0,
        block: 1,
        part: None,
        bytes: bytes.to_vec(),
    }
}

/// The state of the session loaded from `root` as its next writer would load it.
fn load(root: &Path, cipher: Cipher) -> Result<(Vec<u8>, Vec<u8>), SessionError> {
    let backend = LocalBackend::new(root);
    block_on(async {
        let session = Session::load(&backend, addr(), &triple(), &deployment(cipher)).await?;
        Ok(state(&session))
    })
}

fn restore_at(root: &Path, cipher: Cipher) -> Restored {
    let backend = LocalBackend::new(root);
    block_on(restore(&backend, &addr(), &deployment(cipher), 42)).unwrap()
}

/// A walk lists the store, then a seal commits, then the walk reads what it listed: the backup
/// holds the head the seal wrote, but not the log and segments the seal created. A load of it
/// refuses by name, older versions notwithstanding. Restore rolls back to the version before the
/// seal, and the session loads back exactly as it stood then. Without kept versions, the same
/// backup is unrecoverable.
fn a_walk_that_passed_a_seal_rolls_back_to_the_version_before_it(cipher: Cipher) {
    let live = Root::new("live");
    let backend = LocalBackend::new(&live.0);
    let (walk, before, unsealed) = block_on(async {
        let mut session = history(&backend, cipher).await;
        let before = state(&session);
        let unsealed = backend.read_stored_head(&addr()).unwrap().unwrap().1;
        let walk = Walk::list(&live.0);
        session.seal(None, 1_000).await.unwrap();
        (walk, before, unsealed)
    });
    let sealed = backend.read_stored_head(&addr()).unwrap().unwrap().1;
    let copy = Root::new("copy");
    walk.restore_into(&copy.0);
    let unversioned = Root::new("unversioned");
    walk.restore_into(&unversioned.0);
    std::fs::remove_dir_all(unversioned.0.join("heads").join(&addr().0).join("v")).unwrap();

    let heads = snapshot(&copy.0.join("heads"));
    assert!(matches!(
        load(&copy.0, cipher),
        Err(SessionError::Backend(
            crate::backend::BackendError::NoSuchLog
        ))
    ));
    assert_eq!(snapshot(&copy.0.join("heads")), heads);

    assert_eq!(
        restore_at(&copy.0, cipher),
        Restored::Repaired {
            from: sealed,
            to: unsealed,
            written: Some(sealed + 1),
            cut: None,
        }
    );
    let loaded = load(&copy.0, cipher).unwrap();
    assert_eq!(loaded, before);

    let Restored::Unrecoverable { lacking } = restore_at(&unversioned.0, cipher) else {
        panic!("a backup without kept versions has nothing to roll back to");
    };
    assert_eq!(lacking.len(), 1);
    assert_eq!(lacking[0].0, sealed);
    assert!(lacking[0].1.starts_with("log "), "{lacking:?}");
}

/// A walk lists the store, then a run stores a new medium and appends the input naming it: the
/// backup's log holds the input, but not the medium. No version can drop the input, since every
/// version names that log; restore cuts the log back to where the input's batch starts, keeps
/// the cut bytes beside it, and the session loads back exactly as it stood before. Restore that
/// could only choose a version finds the same backup unrecoverable.
fn a_walk_that_passed_a_new_medium_cuts_the_log_before_its_record(cipher: Cipher) {
    let live = Root::new("live");
    let backend = LocalBackend::new(&live.0);
    let (walk, before, log, at, seq) = block_on(async {
        let mut session = history(&backend, cipher).await;
        session
            .append(&[start("c", Vec::new())], true)
            .await
            .unwrap();
        let before = state(&session);
        let (log, at) = session.open_log();
        let walk = Walk::list(&live.0);
        let (seq, _) = session
            .append_input_media(
                &[input("c", photo("ask c", b"second image"))],
                vec![image_bytes(b"second image")],
                true,
            )
            .await
            .unwrap();
        session.append(&[end("c")], true).await.unwrap();
        (walk, before, log, at, seq)
    });
    let version = backend.read_stored_head(&addr()).unwrap().unwrap().1;
    let copy = Root::new("copy");
    walk.restore_into(&copy.0);
    let uncut = Root::new("uncut");
    walk.restore_into(&uncut.0);
    let log_path = Path::new("logs").join(&addr().0).join(hex(&log.0));
    let copied_log = std::fs::read(copy.0.join(&log_path)).unwrap();

    let uncut_backend = LocalBackend::new(&uncut.0);
    let Restored::Unrecoverable { lacking } = block_on(restore_with(
        &uncut_backend,
        &addr(),
        &deployment(cipher),
        42,
        false,
    ))
    .unwrap() else {
        panic!("only a cut drops the input naming the medium");
    };
    assert_eq!(lacking[0].0, version);
    assert!(lacking[0].1.starts_with("medium "), "{lacking:?}");

    let Restored::Repaired {
        from,
        to,
        written: None,
        cut: Some(cut),
    } = restore_at(&copy.0, cipher)
    else {
        panic!("the log is cut, and the head stays");
    };
    assert_eq!((from, to), (version, version));
    assert_eq!(
        (cut.log, cut.record, cut.first_cut, cut.at, cut.was),
        (log, seq, seq, at, copied_log.len() as u64)
    );
    assert!(cut.lacking.starts_with("medium "), "{cut:?}");
    assert_eq!(
        cut.sidecar,
        copy.0
            .join("logs")
            .join(&addr().0)
            .join(format!("{}.restore-cut-42", hex(&log.0)))
    );
    assert_eq!(
        std::fs::read(&cut.sidecar).unwrap(),
        copied_log[at as usize..]
    );
    assert_eq!(
        std::fs::read(copy.0.join(&log_path)).unwrap(),
        copied_log[..at as usize]
    );
    let loaded = load(&copy.0, cipher).unwrap();
    assert_eq!(loaded, before);
}

/// The same for a setup part: the walk listed the setup store before a run stored a new part
/// and appended its start naming it. Restore cuts the log back to the start's batch.
fn a_walk_that_passed_a_new_setup_part_cuts_the_log_before_its_record(cipher: Cipher) {
    let live = Root::new("live");
    let backend = LocalBackend::new(&live.0);
    let setup_key = deployment(cipher).setup_key_for(cipher).unwrap();
    let (walk, before, log, at, seq) = block_on(async {
        let mut session = history(&backend, cipher).await;
        let before = state(&session);
        let (log, at) = session.open_log();
        let walk = Walk::list(&live.0);
        let (part, stored) = SetupStore::new(&backend, &setup_key)
            .put(PartKind::SystemText, b"answer briefly")
            .await
            .unwrap();
        assert!(stored);
        let seq = session
            .append(&[start("c", vec![part])], true)
            .await
            .unwrap();
        session
            .append(&[input("c", text("ask c"))], true)
            .await
            .unwrap();
        (walk, before, log, at, seq)
    });
    let copy = Root::new("copy");
    walk.restore_into(&copy.0);
    let uncut = Root::new("uncut");
    walk.restore_into(&uncut.0);

    let uncut_backend = LocalBackend::new(&uncut.0);
    let Restored::Unrecoverable { lacking } = block_on(restore_with(
        &uncut_backend,
        &addr(),
        &deployment(cipher),
        42,
        false,
    ))
    .unwrap() else {
        panic!("only a cut drops the start naming the part");
    };
    assert!(lacking[0].1.starts_with("setup part "), "{lacking:?}");

    let Restored::Repaired {
        written: None,
        cut: Some(cut),
        ..
    } = restore_at(&copy.0, cipher)
    else {
        panic!("the log is cut, and the head stays");
    };
    assert_eq!(
        (cut.log, cut.record, cut.first_cut, cut.at),
        (log, seq, seq, at)
    );
    assert!(cut.lacking.starts_with("setup part "), "{cut:?}");
    let loaded = load(&copy.0, cipher).unwrap();
    assert_eq!(loaded, before);
}

/// A walk reads a log while a batch is being appended, and stores it cut in the middle of that
/// batch. That is a torn tail, which loading cuts: restore keeps the session and writes nothing,
/// and the session loads back as it stood before the batch.
fn a_log_read_in_the_middle_of_a_batch_is_kept_as_a_torn_tail(cipher: Cipher) {
    let live = Root::new("live");
    let backend = LocalBackend::new(&live.0);
    let (before, log, at, end_len) = block_on(async {
        let mut session = history(&backend, cipher).await;
        let before = state(&session);
        let (log, at) = session.open_log();
        session
            .append(
                &[start("c", Vec::new()), input("c", text("ask c")), end("c")],
                true,
            )
            .await
            .unwrap();
        (before, log, at, session.open_log().1)
    });
    let version = backend.read_stored_head(&addr()).unwrap().unwrap().1;
    let copy = Root::new("copy");
    Walk::list(&live.0).restore_into(&copy.0);
    std::fs::File::options()
        .write(true)
        .open(copy.0.join("logs").join(&addr().0).join(hex(&log.0)))
        .unwrap()
        .set_len(at + (end_len - at) / 2)
        .unwrap();
    let stored = snapshot(&copy.0);
    assert_eq!(restore_at(&copy.0, cipher), Restored::Kept { version });
    assert_eq!(snapshot(&copy.0), stored);
    let loaded = load(&copy.0, cipher).unwrap();
    assert_eq!(loaded, before);
}

/// A session whose head and everything it names came back whole is kept, and nothing is written.
fn a_complete_session_is_kept_and_nothing_is_written(cipher: Cipher) {
    let live = Root::new("live");
    let backend = LocalBackend::new(&live.0);
    let written = block_on(async {
        let mut session = history(&backend, cipher).await;
        session.seal(None, 1_000).await.unwrap();
        session
            .append(&[input("c", text("after the seal"))], true)
            .await
            .unwrap();
        let written = state(&session);
        session.close().await.unwrap();
        written
    });
    let version = backend.read_stored_head(&addr()).unwrap().unwrap().1;
    let copy = Root::new("copy");
    Walk::list(&live.0).restore_into(&copy.0);
    let stored = snapshot(&copy.0);
    let copied = LocalBackend::new(&copy.0);
    assert_eq!(
        block_on(check(&copied, &addr(), &deployment(cipher))).unwrap(),
        None
    );
    assert_eq!(restore_at(&copy.0, cipher), Restored::Kept { version });
    assert_eq!(snapshot(&copy.0), stored);
    let loaded = load(&copy.0, cipher).unwrap();
    assert_eq!(loaded, written);
}

/// A file every kept version names, such as a sealed segment once the versions before its seal
/// are pruned, leaves no version to choose: the session is unrecoverable, reported with what
/// each version lacks, and nothing changes.
fn a_file_every_version_names_leaves_the_session_unrecoverable_and_unchanged(cipher: Cipher) {
    let live = Root::new("live");
    let backend = LocalBackend::new(&live.0);
    let change = block_on(async {
        let mut session = history(&backend, cipher).await;
        session.seal(None, 1_000).await.unwrap().change
    });
    let sealed = backend.read_stored_head(&addr()).unwrap().unwrap().1;
    let copy = Root::new("copy");
    Walk::list(&live.0).restore_into(&copy.0);
    let versions = copy.0.join("heads").join(&addr().0).join("v");
    for version in LocalBackend::new(&copy.0).head_versions(&addr()).unwrap() {
        if version < sealed {
            std::fs::remove_file(versions.join(version.to_string())).unwrap();
        }
    }
    let segment = format!("{}/l", hex(&change.0));
    std::fs::remove_file(copy.0.join("objects").join(&addr().0).join(&segment)).unwrap();
    let stored = snapshot(&copy.0);
    assert_eq!(
        restore_at(&copy.0, cipher),
        Restored::Unrecoverable {
            lacking: vec![(sealed, format!("sealed segment {segment}"))],
        }
    );
    assert_eq!(snapshot(&copy.0), stored);
}

/// Damage that isn't a file the backup lacks, here acknowledged bytes of the open log changed,
/// isn't restore's to repair: it is refused by its reason, and nothing changes.
fn damage_that_is_not_a_lacking_file_is_refused_and_unchanged(cipher: Cipher) {
    let live = Root::new("live");
    let backend = LocalBackend::new(&live.0);
    let log = block_on(async {
        let session = history(&backend, cipher).await;
        let (log, _) = session.open_log();
        session.close().await.unwrap();
        log
    });
    let copy = Root::new("copy");
    Walk::list(&live.0).restore_into(&copy.0);
    let path = copy.0.join("logs").join(&addr().0).join(hex(&log.0));
    let mut bytes = std::fs::read(&path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();
    let stored = snapshot(&copy.0);
    let copied = LocalBackend::new(&copy.0);
    assert!(matches!(
        block_on(check(&copied, &addr(), &deployment(cipher))).unwrap(),
        Some(Finding::Refused(_))
    ));
    assert!(matches!(
        restore_at(&copy.0, cipher),
        Restored::Refused { .. }
    ));
    assert_eq!(snapshot(&copy.0), stored);
}
