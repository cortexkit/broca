use super::*;
use broca_protocol::{ContentBlock, Message, Role};
use broca_wal::{FileWal, Wal, WalRecord};
use std::sync::Arc;

const BYTES: &[u8] = b"%PDF-1.7\nmedia-store fixture\n%%EOF";

fn reference() -> MediaRef {
    crate::media_validation::validate_media(BYTES, "application/pdf").unwrap()
}

fn identity() -> BindIdentity {
    BindIdentity {
        project_root: PathBuf::from("/project/a"),
        harness: "harness-a".into(),
        session: "same-name".into(),
    }
}

fn fixture() -> (PathBuf, MediaStore, MediaManifest) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "media-store-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    broca_owner_only::create_dir_all(&root).unwrap();
    let store = MediaStore::open(&root, MediaStoreClass::Durable).unwrap();
    let manifest = store.session(&identity()).unwrap();
    (root, store, manifest)
}

fn record(reference: &MediaRef) -> WalRecord {
    WalRecord::Steered {
        retention: None,
        ts_ms: None,
        send_id: None,
        request_identity_digest: None,
        delivery: None,
        title: None,
        hooks: None,
        tags: None,
        input: Arc::new(vec![Message::new(
            Role::User,
            vec![ContentBlock::Media(reference.clone())],
        )]),
    }
}

#[derive(Default)]
struct Probe {
    events: Vec<Step>,
    fail: Option<Step>,
    remove_at_manifest: Option<PathBuf>,
}

impl StorageSteps for Probe {
    fn reached(&mut self, step: Step) -> io::Result<()> {
        self.events.push(step);
        if step == Step::ManifestSynced {
            if let Some(path) = self.remove_at_manifest.take() {
                fs::remove_file(path)?;
            }
        }
        if self.fail == Some(step) {
            return Err(io::Error::other(format!("injected {step:?}")));
        }
        Ok(())
    }
}

#[test]
fn registration_orders_all_barriers_before_real_wal_append() {
    let (root, store, mut manifest) = fixture();
    let reference = reference();
    let mut wal = FileWal::open(root.join("wal").join("session.wal"))
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
    let mut probe = Probe::default();
    manifest
        .register_with(
            &reference,
            BYTES,
            || {
                assert_eq!(store.read_verified(&reference).unwrap(), BYTES);
                assert_eq!(
                    fs::read_to_string(
                        store
                            .root
                            .join("refs")
                            .join(crate::wal_writer::session_addr(&identity()))
                    )
                    .unwrap(),
                    format!("{}\n", reference.sha256)
                );
                wal.append(1, &record(&reference), true)
                    .map_err(io::Error::other)
            },
            &mut probe,
        )
        .unwrap();
    assert_eq!(
        probe.events,
        [
            Step::ObjectFileSynced,
            Step::ObjectPublished,
            Step::ManifestSynced,
            Step::RefsSynced,
            Step::WalAppended
        ]
    );
    assert_eq!(wal.replay().unwrap().len(), 1);
    broca_owner_only::validate(store.root.join("objects").join(&reference.sha256)).unwrap();
    assert_eq!(fs::read_dir(store.root.join("objects")).unwrap().count(), 1);
}

#[test]
fn failed_barriers_never_enter_the_wal_callback() {
    for fail in [
        Step::ObjectFileSynced,
        Step::ObjectPublished,
        Step::ManifestSynced,
        Step::RefsSynced,
    ] {
        let (root, _, mut manifest) = fixture();
        let mut wal = FileWal::open(root.join("wal").join("session.wal"))
            .unwrap()
            .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
        let reference = reference();
        let mut probe = Probe {
            fail: Some(fail),
            ..Probe::default()
        };
        assert!(manifest
            .register_with(
                &reference,
                BYTES,
                || {
                    wal.append(1, &record(&reference), true)
                        .map_err(io::Error::other)
                },
                &mut probe
            )
            .is_err());
        assert!(
            wal.replay().unwrap().is_empty(),
            "{fail:?} must refuse WAL reference"
        );
    }
}

#[test]
fn failed_refs_sync_is_retried_and_every_open_owes_a_sync() {
    let (_, store, mut manifest) = fixture();
    let reference = reference();
    let mut probe = Probe {
        fail: Some(Step::RefsSynced),
        ..Probe::default()
    };
    assert!(manifest
        .register_with(&reference, BYTES, || Ok(()), &mut probe)
        .is_err());
    assert!(!manifest.refs_synced, "failed refs sync must stay pending");
    probe.fail = None;
    probe.events.clear();
    manifest
        .register_with(&reference, BYTES, || Ok(()), &mut probe)
        .unwrap();
    assert_eq!(
        probe
            .events
            .iter()
            .filter(|&&step| step == Step::RefsSynced)
            .count(),
        1
    );
    probe.events.clear();
    manifest
        .register_with(&reference, BYTES, || Ok(()), &mut probe)
        .unwrap();
    assert!(
        !probe.events.contains(&Step::RefsSynced),
        "a successful sync discharges this open"
    );
    drop(manifest);
    let mut reopened = store.session(&identity()).unwrap();
    probe.events.clear();
    reopened
        .register_with(&reference, BYTES, || Ok(()), &mut probe)
        .unwrap();
    assert!(
        probe.events.contains(&Step::RefsSynced),
        "reopening owes a fresh refs barrier"
    );
}

#[test]
fn registration_republishes_an_object_lost_after_manifest_fsync() {
    let (_, store, mut manifest) = fixture();
    let reference = reference();
    let mut probe = Probe {
        remove_at_manifest: Some(store.root.join("objects").join(&reference.sha256)),
        ..Probe::default()
    };
    manifest
        .register_with(
            &reference,
            BYTES,
            || {
                assert_eq!(store.read_verified(&reference).unwrap(), BYTES);
                Ok(())
            },
            &mut probe,
        )
        .unwrap();
    assert_eq!(
        probe
            .events
            .iter()
            .filter(|&&step| step == Step::ObjectPublished)
            .count(),
        2
    );
}

#[test]
fn render_rereads_bytes_and_names_missing_or_corrupt_digest() {
    let (_, store, mut manifest) = fixture();
    let reference = reference();
    manifest
        .register_and_append(&reference, BYTES, || Ok(()))
        .unwrap();
    assert_eq!(store.read_verified(&reference).unwrap(), BYTES);
    let object = store.root.join("objects").join(&reference.sha256);
    fs::write(&object, vec![b'x'; BYTES.len()]).unwrap();
    let error = store.read_verified(&reference).unwrap_err();
    assert_eq!(error.code(), Some("media_object_corrupt"));
    assert!(error.to_string().contains(&reference.sha256));
    fs::remove_file(&object).unwrap();
    let error = store.read_verified(&reference).unwrap_err();
    assert_eq!(error.code(), Some("media_object_missing"));
    assert!(error.to_string().contains(&reference.sha256));
    manifest
        .register_and_append(&reference, BYTES, || Ok(()))
        .unwrap();
    assert_eq!(store.read_verified(&reference).unwrap(), BYTES);
}

#[test]
fn bad_bytes_or_digest_are_refused_before_any_registration_write() {
    let (_, store, mut manifest) = fixture();
    for digest in [reference().sha256, "../outside".into()] {
        let mut reference = reference();
        reference.sha256 = digest;
        assert!(manifest
            .register_and_append(&reference, b"not the object", || -> io::Result<()> {
                panic!("must not append")
            })
            .is_err());
    }
    assert_eq!(fs::read_dir(store.root.join("objects")).unwrap().count(), 0);
    assert_eq!(manifest.file.metadata().unwrap().len(), 0);
}

#[test]
fn manifests_use_the_full_triple_and_classes_have_separate_objects() {
    let (root, store, mut first) = fixture();
    let reference = reference();
    first
        .register_and_append(&reference, BYTES, || Ok(()))
        .unwrap();
    for identity in [
        BindIdentity {
            project_root: PathBuf::from("/project/b"),
            ..identity()
        },
        BindIdentity {
            harness: "harness-b".into(),
            ..identity()
        },
    ] {
        let mut other = store.session(&identity).unwrap();
        assert_eq!(other.file.metadata().unwrap().len(), 0);
        other
            .register_and_append(&reference, BYTES, || Ok(()))
            .unwrap();
    }
    assert_eq!(fs::read_dir(store.root.join("refs")).unwrap().count(), 3);
    assert_eq!(fs::read_dir(store.root.join("objects")).unwrap().count(), 1);
    for class in [MediaStoreClass::NonDurable, MediaStoreClass::Ephemeral] {
        let separate = MediaStore::open(&root, class).unwrap();
        assert_eq!(
            separate.read_verified(&reference).unwrap_err().code(),
            Some("media_object_missing")
        );
        separate
            .session(&identity())
            .unwrap()
            .register_and_append(&reference, BYTES, || Ok(()))
            .unwrap();
        assert_eq!(separate.read_verified(&reference).unwrap(), BYTES);
    }
}

#[test]
fn registration_holds_shared_store_lock_through_wal_append() {
    let (_, store, mut manifest) = fixture();
    manifest
        .register_and_append(&reference(), BYTES, || {
            let contender = store.lock()?;
            assert!(
                contender.try_lock().is_err(),
                "a sweep cannot enter during registration"
            );
            Ok(())
        })
        .unwrap();
    store.with_exclusive_lock(|| Ok(())).unwrap();
}

#[test]
fn partial_manifest_write_cannot_swallow_a_later_registration() {
    let (_, store, mut manifest) = fixture();
    let reference = reference();
    manifest
        .register_and_append(&reference, BYTES, || Ok(()))
        .unwrap();
    manifest.file.write_all(b"deadbeef").unwrap();
    manifest
        .register_and_append(&reference, BYTES, || Ok(()))
        .unwrap();
    let text = fs::read_to_string(
        store
            .root
            .join("refs")
            .join(crate::wal_writer::session_addr(&identity())),
    )
    .unwrap();
    assert_eq!(text, format!("{0}\n{0}\n", reference.sha256));
}

#[test]
fn live_manifest_and_objects_are_readable_from_separate_handles() {
    let (_, store, mut manifest) = fixture();
    let reference = reference();
    let path = store
        .root
        .join("refs")
        .join(crate::wal_writer::session_addr(&identity()));
    manifest
        .register_and_append(&reference, BYTES, || {
            assert_eq!(
                fs::read_to_string(&path)?,
                format!("{}\n", reference.sha256)
            );
            assert_eq!(store.read_verified(&reference).unwrap(), BYTES);
            Ok(())
        })
        .unwrap();
    let mut reader = broca_owner_only::OpenOptions::new()
        .read(true)
        .open(&path)
        .unwrap();
    manifest
        .register_and_append(&reference, BYTES, || Ok(()))
        .unwrap();
    let mut text = String::new();
    reader.read_to_string(&mut text).unwrap();
    assert_eq!(text, format!("{0}\n{0}\n", reference.sha256));
    assert_eq!(store.read_verified(&reference).unwrap(), BYTES);
    broca_owner_only::validate(&path).unwrap();
    broca_owner_only::validate(store.root.join("objects").join(&reference.sha256)).unwrap();
}

struct CrashAt(Step);
impl StorageSteps for CrashAt {
    fn reached(&mut self, step: Step) -> io::Result<()> {
        if step == self.0 {
            println!("MEDIA_CUT_READY");
            io::stdout().flush()?;
            // A bounded wait gives the parent time to kill without leaving a
            // runaway child if its test fails before delivering the signal.
            std::thread::sleep(std::time::Duration::from_secs(20));
            panic!("parent did not kill media storage child");
        }
        Ok(())
    }
}

#[test]
fn crash_child() {
    let Some(root) = std::env::var_os("BROCA_MEDIA_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let store = MediaStore::open(&root, MediaStoreClass::Durable).unwrap();
    let mut manifest = store.session(&identity()).unwrap();
    let reference = reference();
    let cut = match std::env::var("BROCA_MEDIA_CRASH_STEP").unwrap().as_str() {
        "object" => Step::ObjectPublished,
        "manifest" => Step::ManifestSynced,
        "refs" => Step::RefsSynced,
        "wal" => Step::WalAppended,
        _ => panic!("unknown cut"),
    };
    let mut wal = FileWal::open(root.join("wal").join("session.wal"))
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
    manifest
        .register_with(
            &reference,
            BYTES,
            || {
                wal.append(1, &record(&reference), true)
                    .map_err(io::Error::other)
            },
            &mut CrashAt(cut),
        )
        .unwrap();
}

#[test]
fn kill_at_each_storage_step_never_leaves_a_dangling_wal_reference() {
    use std::io::BufRead;
    for cut in ["object", "manifest", "refs", "wal"] {
        let (root, store, manifest) = fixture();
        drop(manifest);
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "media_store::tests::crash_child", "--nocapture"])
            .env("BROCA_MEDIA_CRASH_ROOT", &root)
            .env("BROCA_MEDIA_CRASH_STEP", cut)
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut ready = false;
        for line in io::BufReader::new(child.stdout.take().unwrap()).lines() {
            if line.unwrap().contains("MEDIA_CUT_READY") {
                ready = true;
                break;
            }
        }
        if ready {
            child.kill().unwrap();
        }
        let status = child.wait().unwrap();
        assert!(ready, "child never reached {cut}: {status}");
        assert!(!status.success());
        let wal = FileWal::open(root.join("wal").join("session.wal"))
            .unwrap()
            .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), usize::from(cut == "wal"));
        for frame in records {
            let WalRecord::Steered { input, .. } = frame.record else {
                panic!("wrong record");
            };
            let ContentBlock::Media(reference) = &input[0].content[0] else {
                panic!("missing media");
            };
            assert_eq!(store.read_verified(reference).unwrap(), BYTES, "cut {cut}");
        }
    }
}
