use super::*;
use crate::{
    record::{Input, PartKind, Placement, Record, RunEnd, RunStart},
    session::{Session, SessionError},
    setup::SetupStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{ContentBlock, Message, Role},
};
use futures::executor::block_on;

struct Root(std::path::PathBuf);
impl Root {
    fn new() -> Self {
        Self(
            std::path::PathBuf::from("target/scratch")
                .join(format!("store-move-{}", hex(&crate::keys::random_id().0))),
        )
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn addr() -> SessionAddr {
    SessionAddr("0123456789abcdef".into())
}
fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/synthetic".into(),
        harness: "test".into(),
        session: "s".into(),
    }
}

async fn history<'a>(
    store: &'a LocalBackend,
    deployment: &DeploymentKey,
) -> Session<&'a LocalBackend> {
    let setup_key = deployment.setup_key_for(deployment.new_sessions()).unwrap();
    let (setup, _) = SetupStore::new(store, &setup_key)
        .put(PartKind::SystemText, b"synthetic system")
        .await
        .unwrap();
    let mut session = Session::create(store, addr(), triple(), deployment, false)
        .await
        .unwrap();
    let model =
        serde_json::from_value(serde_json::json!({"provider_module_id":"p","model_id":"m"}))
            .unwrap();
    let start = RunStart {
        run_id: "run".into(),
        model,
        setup: vec![setup],
        renderer_version: 1,
        named_speaker: None,
        imported_ids: None,
        message_marks: None,
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
    };
    session
        .append(&[Record::RunStart(Box::new(start))], true)
        .await
        .unwrap();
    let media = b"synthetic image";
    let image = serde_json::from_value(serde_json::json!({"kind":"image", "mime":"image/png", "byte_size":media.len(), "sha256":hex(&Sha256::digest(media)), "width":1, "height":1})).unwrap();
    session
        .append_input_media(
            &[Record::Input(Input {
                run_id: "run".into(),
                placement: Placement::RunInput,
                message: Message::new(
                    Role::User,
                    vec![
                        ContentBlock::Text {
                            text: "synthetic question".into(),
                        },
                        ContentBlock::Media(image),
                    ],
                ),
                written_at_ms: None,
                submission_id: None,
                notice: None,
                tags_v1: None,
                receipt_seq: None,
                pruned: Vec::new(),
                media: Vec::new(),
            })],
            vec![crate::session::NewInputMedia {
                record: 0,
                message: 0,
                block: 1,
                part: None,
                bytes: media.to_vec(),
            }],
            true,
        )
        .await
        .unwrap();
    session
        .append(
            &[Record::RunEnd(RunEnd {
                run_id: "run".into(),
                reason: RunFinishReason::Completed,
                usage: None,
                ts_ms: None,
                retries_used: 0,
                interrupt_reason: None,
                error: None,
            })],
            true,
        )
        .await
        .unwrap();
    session.seal(None, 100).await.unwrap();
    session
}

#[test]
fn ciphertext_and_plain_exports_roundtrip_with_setup_deduplication() {
    block_on(async {
        for cipher in [Cipher::XChaCha20Poly1305, Cipher::None] {
            let source = Root::new();
            let staging = Root::new();
            let destination = Root::new();
            let deployment = DeploymentKey::new(7, &[4; 32]).creating(cipher);
            let store = LocalBackend::new(&source.0);
            let session = history(&store, &deployment).await;
            let before = (
                session.conversation.encode(),
                session.operations.encode(),
                session.records_from(0).await.unwrap(),
            );
            drop(session);
            let (_, version) = store.read_head(&addr()).await.unwrap().unwrap();
            let export = Export::pinned(&store, &addr(), version, "durable", &deployment)
                .await
                .unwrap();
            let bytes = export.encode();
            if cipher != Cipher::None {
                assert!(!bytes
                    .windows(b"synthetic question".len())
                    .any(|bytes| bytes == b"synthetic question"));
                assert!(!bytes
                    .windows(b"synthetic system".len())
                    .any(|bytes| bytes == b"synthetic system"));
            }
            freeze(&store, &addr(), version, "cut", &deployment)
                .await
                .unwrap();
            assert_eq!(
                crate::session::collect_stored(&store, &addr(), &deployment, u64::MAX)
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(
                Export::pinned(&store, &addr(), version, "durable", &deployment)
                    .await
                    .unwrap()
                    .encode(),
                bytes
            );
            let incoming = Export::decode(&bytes, &addr(), &deployment).unwrap();
            incoming
                .stage(&staging.0, &addr(), &deployment)
                .await
                .unwrap();
            let setup_path = incoming
                .files
                .keys()
                .find(|path| path.starts_with("setup/") || path.starts_with("setup-plain/"))
                .unwrap();
            let setup_file = destination.0.join(setup_path);
            broca_owner_only::create_dir_all(setup_file.parent().unwrap()).unwrap();
            std::fs::write(&setup_file, &incoming.files[setup_path]).unwrap();
            let modified = setup_file.metadata().unwrap().modified().unwrap();
            incoming
                .activate(&staging.0, &destination.0, &addr())
                .unwrap();
            incoming
                .activate(&staging.0, &destination.0, &addr())
                .unwrap();
            assert_eq!(setup_file.metadata().unwrap().modified().unwrap(), modified);
            let receiver = LocalBackend::new(&destination.0);
            let view = Session::view(&receiver, addr(), &triple(), &deployment)
                .await
                .unwrap();
            assert_eq!(
                (
                    view.conversation.encode(),
                    view.operations.encode(),
                    view.records_from(0).await.unwrap()
                ),
                before
            );
            assert_eq!(
                Export::pinned(&receiver, &addr(), version, "durable", &deployment)
                    .await
                    .unwrap()
                    .encode(),
                bytes
            );
        }
    });
}

#[test]
fn missing_deployment_key_refuses_before_staging_and_checksums_refuse_damage() {
    block_on(async {
        let source = Root::new();
        let staging = Root::new();
        let deployment = DeploymentKey::new(7, &[4; 32]);
        let store = LocalBackend::new(&source.0);
        drop(history(&store, &deployment).await);
        let (_, version) = store.read_head(&addr()).await.unwrap().unwrap();
        let export = Export::pinned(&store, &addr(), version, "durable", &deployment)
            .await
            .unwrap();
        let mut bytes = export.encode();
        for key in [DeploymentKey::unloaded(7), DeploymentKey::new(8, &[4; 32])] {
            assert!(matches!(
                Export::decode(&bytes, &addr(), &key),
                Err(Error::DeploymentKeyMissing(7))
            ));
            assert!(matches!(
                export.stage(&staging.0, &addr(), &key).await,
                Err(Error::DeploymentKeyMissing(7))
            ));
            assert!(!staging.0.exists());
        }
        *bytes.last_mut().unwrap() ^= 1;
        assert!(
            matches!(Export::decode(&bytes, &addr(), &deployment), Err(Error::Invalid(reason)) if reason.contains("SHA-256"))
        );
    });
}

#[test]
fn export_header_names_the_loaded_deployment_wrapper_not_the_first_wrapper() {
    block_on(async {
        let source = Root::new();
        let store = LocalBackend::new(&source.0);
        let deployment = DeploymentKey::new(7, &[4; 32]);
        drop(history(&store, &deployment).await);
        let (stored, version) = store.read_head(&addr()).await.unwrap().unwrap();
        let (mut head, key) = head::open(&stored, &deployment).unwrap();
        let mut other = head.keys[0].clone();
        other.deployment = 8;
        head.keys.insert(0, other);
        store
            .write_head(
                &addr(),
                &head::seal(&head, &key),
                Some(version),
                version + 1,
            )
            .await
            .unwrap();
        let export = Export::pinned(&store, &addr(), version + 1, "durable", &deployment)
            .await
            .unwrap();
        assert_eq!(export.header.deployment_key, Some(7));
    });
}

#[test]
fn frozen_reads_work_writers_refuse_release_fences_both_and_abort_is_cut_owned() {
    block_on(async {
        let source = Root::new();
        let store = LocalBackend::new(&source.0);
        let deployment = DeploymentKey::new(7, &[4; 32]);
        drop(history(&store, &deployment).await);
        let (_, version) = store.read_head(&addr()).await.unwrap().unwrap();
        freeze(&store, &addr(), version, "cut", &deployment)
            .await
            .unwrap();
        abort(&store, &addr(), "other", &deployment).await.unwrap();
        assert!(Session::view(&store, addr(), &triple(), &deployment)
            .await
            .is_ok());
        assert!(
            matches!(Session::load(&store, addr(), &triple(), &deployment).await, Err(SessionError::SessionMoving { cut_id }) if cut_id == "cut")
        );
        abort(&store, &addr(), "cut", &deployment).await.unwrap();
        let (_, version) = store.read_head(&addr()).await.unwrap().unwrap();
        freeze(&store, &addr(), version, "cut2", &deployment)
            .await
            .unwrap();
        for kept in store.head_versions(&addr()).unwrap() {
            std::fs::File::open(source.0.join(format!("heads/{}/v/{kept}", addr().0)))
                .unwrap()
                .set_modified(std::time::SystemTime::UNIX_EPOCH)
                .unwrap();
        }
        release(&store, &addr(), "cut2", "receiver", &deployment)
            .await
            .unwrap();
        assert!(store.read_head_version(&addr(), version).unwrap().is_some());
        release(&store, &addr(), "cut2", "receiver", &deployment)
            .await
            .unwrap();
        abort(&store, &addr(), "cut2", &deployment).await.unwrap();
        assert!(
            matches!(Session::view(&store, addr(), &triple(), &deployment).await, Err(SessionError::SessionMoved { holder }) if holder == "receiver")
        );
        assert!(
            matches!(Session::load(&store, addr(), &triple(), &deployment).await, Err(SessionError::SessionMoved { holder }) if holder == "receiver")
        );
    });
}

#[test]
fn activation_resumes_after_each_file_rename_and_rejects_unreachable_files_before_staging() {
    block_on(async {
        let source = Root::new();
        let store = LocalBackend::new(&source.0);
        let deployment = DeploymentKey::new(7, &[4; 32]);
        drop(history(&store, &deployment).await);
        let (_, version) = store.read_head(&addr()).await.unwrap().unwrap();
        let mut export = Export::pinned(&store, &addr(), version, "durable", &deployment)
            .await
            .unwrap();
        let head_path = format!("heads/{}/head", addr().0);
        let paths: Vec<_> = export
            .files
            .keys()
            .filter(|path| **path != head_path)
            .chain(std::iter::once(&head_path))
            .cloned()
            .collect();
        for cut in 0..=paths.len() {
            let staging = Root::new();
            let destination = Root::new();
            export
                .stage(&staging.0, &addr(), &deployment)
                .await
                .unwrap();
            // A crash loses the stack after any subset of completed renames. No head is
            // published until the last one, and repeating activation completes the rest.
            for path in paths.iter().take(cut) {
                let destination = destination.0.join(path);
                broca_owner_only::create_dir_all(destination.parent().unwrap()).unwrap();
                std::fs::rename(staging.0.join(path), destination).unwrap();
            }
            if cut < paths.len() {
                assert!(!destination.0.join(&head_path).exists());
            }
            export
                .activate(&staging.0, &destination.0, &addr())
                .unwrap();
            let receiver = LocalBackend::new(&destination.0);
            assert!(
                crate::session::restore::check(&receiver, &addr(), &deployment)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        export.files.insert(
            format!("objects/{}/unreachable/file", addr().0),
            b"must not be written".to_vec(),
        );
        let staging = Root::new();
        assert!(export
            .stage(&staging.0, &addr(), &deployment)
            .await
            .is_err());
        assert!(!staging.0.exists());
        export.files.insert("../escaped".into(), vec![]);
        assert!(Export::decode(&export.encode(), &addr(), &deployment).is_err());
    });
}

#[test]
fn stream_frames_skip_old_exports_and_preserve_partial_admission_tails() {
    block_on(async {
        let source = Root::new();
        let store = LocalBackend::new(&source.0);
        let deployment = DeploymentKey::new(7, &[4; 32]);
        drop(history(&store, &deployment).await);
        let (_, version) = store.read_head(&addr()).await.unwrap().unwrap();
        let mut export = Export::pinned(&store, &addr(), version, "durable", &deployment)
            .await
            .unwrap();
        let first = append_stream(&[], &export);
        export.checks.insert("head".into(), "newest".into());
        let second = append_stream(&first, &export);
        assert_eq!(last_export(&second).unwrap(), export.encode());
        assert_eq!(covered_stream(&second, first.len() as u64).unwrap(), first);
        let partial = first.len() + (second.len() - first.len()) / 2;
        assert_eq!(covered_stream(&second, partial as u64).unwrap(), second);
        let path = source.0.join("stream");
        std::fs::write(&path, &second).unwrap();
        assert_eq!(
            stream_prefix(&path, partial as u64).unwrap(),
            second.len() as u64
        );
        let (latest, end, digest) = stream_file(&path, u64::MAX).unwrap();
        assert_eq!(latest, append_stream(&[], &export));
        assert_eq!(end, second.len() as u64);
        assert_eq!(digest, hex(&Sha256::digest(&second)));
        let planned = planned_stream(&path, first.len() as u64, &latest).unwrap();
        let written = append_stream_file(&path, first.len() as u64, &latest).unwrap();
        assert_eq!(planned, written);
        assert_eq!(std::fs::read(&path).unwrap(), second);
        std::fs::write(&path, &second[..partial]).unwrap();
        assert!(stream_prefix(&path, partial as u64).is_err());
        assert!(covered_stream(&second[..partial], partial as u64).is_err());
        assert!(covered_stream(&first, partial as u64).is_err());
        assert!(last_export(&second[..second.len() - 1]).is_err());
        let mut corrupted = second.clone();
        *corrupted.last_mut().unwrap() ^= 1;
        assert!(covered_stream(&corrupted, partial as u64).is_err());
        assert!(last_export(&corrupted).is_err());
        assert_eq!(
            covered_stream(&corrupted, first.len() as u64).unwrap(),
            first
        );
        assert!(last_export(&[second.as_slice(), b"trailing"].concat()).is_err());
    });
}
