use super::*;
use broca_protocol::{ContentBlock, Message, Role};
use broca_wal::{FileWal, Wal, WalRecord};
use std::sync::{mpsc, Arc};

const BYTES: &[u8] = b"%PDF-1.7\naccepted\n%%EOF";
const REFUSED: &[u8] = b"%PDF-1.7\nrefused\n%%EOF";

fn fixture() -> (PathBuf, MediaStore, BindIdentity) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "media-lifecycle-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    broca_owner_only::create_dir_all(&root).unwrap();
    let store = MediaStore::open(&root, MediaStoreClass::Durable).unwrap();
    let session = BindIdentity::new("/project/a", "harness", "same-name");
    (root, store, session)
}

fn reference(bytes: &[u8]) -> MediaRef {
    crate::media_validation::validate_media(bytes, "application/pdf").unwrap()
}

fn record(reference: &MediaRef) -> WalRecord {
    WalRecord::Steered {
        written_at_ms: None,
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

fn accepted(root: &Path, store: &MediaStore, session: &BindIdentity) -> PathBuf {
    let path = root
        .join("wal-durable")
        .join(format!("{}.wal", session_addr(session)));
    let mut wal = FileWal::open(&path)
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
    store
        .session(session)
        .unwrap()
        .register_and_append(&reference(BYTES), BYTES, || {
            wal.append(1, &record(&reference(BYTES)), true)
                .map_err(io::Error::other)
        })
        .unwrap();
    path
}

fn manifest(store: &MediaStore, session: &BindIdentity) -> PathBuf {
    store.root.join("refs").join(session_addr(session))
}

#[test]
fn live_lease_preserves_refused_entries_then_first_unleased_sweep_frees_only_unaccepted_digest() {
    let (root, store, session) = fixture();
    let leases = FileLeaseStore::new(root.join("leases"));
    let lease = leases.acquire(&lease_key_for(&session)).unwrap();
    accepted(&root, &store, &session);
    let mut handle = store.session(&session).unwrap();
    // The same digest may belong to both an accepted input and a refused steer.
    handle
        .register_and_append(&reference(BYTES), BYTES, || Ok(()))
        .unwrap();
    handle
        .register_and_append(&reference(REFUSED), REFUSED, || {
            Err::<(), _>(io::Error::other("request_cap"))
        })
        .unwrap_err();
    drop(handle);
    let report = sweep(&root).unwrap();
    assert_eq!(report.leased, 1);
    assert_eq!(
        digests(&fs::read(manifest(&store, &session)).unwrap())
            .unwrap()
            .len(),
        2
    );
    assert_eq!(store.read_verified(&reference(REFUSED)).unwrap(), REFUSED);
    drop(lease);
    assert_eq!(sweep(&root).unwrap().objects_removed, 1);
    assert!(matches!(
        store.read_verified(&reference(REFUSED)),
        Err(MediaReadError::Missing(_))
    ));
    assert_eq!(store.read_verified(&reference(BYTES)).unwrap(), BYTES);
    assert_eq!(
        fs::read_to_string(manifest(&store, &session)).unwrap(),
        format!("{}\n", reference(BYTES).sha256)
    );
}

#[test]
fn same_named_sessions_in_different_projects_survive_each_others_deletion() {
    let (root, store, first) = fixture();
    let second = BindIdentity::new("/project/b", "harness", "same-name");
    let first_wal = accepted(&root, &store, &first);
    let second_wal = accepted(&root, &store, &second);
    assert!(store.remove_session_after_wal_removal(&first).is_err());
    assert!(manifest(&store, &first).exists());
    fs::remove_file(first_wal).unwrap();
    store.remove_session_after_wal_removal(&first).unwrap();
    sweep(&root).unwrap();
    assert!(!manifest(&store, &first).exists());
    assert!(manifest(&store, &second).exists());
    assert_eq!(store.read_verified(&reference(BYTES)).unwrap(), BYTES);
    assert_eq!(
        broca_wal::replay_file(&second_wal, 1_000_000)
            .unwrap()
            .len(),
        1
    );
    fs::remove_file(second_wal).unwrap();
    store.remove_session_after_wal_removal(&second).unwrap();
    assert_eq!(sweep(&root).unwrap().objects_removed, 1);
}

#[test]
fn orphan_manifest_is_reclaimed_only_after_its_lease_is_released() {
    let (root, store, session) = fixture();
    let leases = FileLeaseStore::new(root.join("leases"));
    let lease = leases.acquire(&lease_key_for(&session)).unwrap();
    store
        .session(&session)
        .unwrap()
        .register_and_append(&reference(BYTES), BYTES, || Ok(()))
        .unwrap();
    assert_eq!(sweep(&root).unwrap().leased, 1);
    assert!(manifest(&store, &session).exists());
    drop(lease);
    assert_eq!(sweep(&root).unwrap().manifests_removed, 1);
    assert!(!manifest(&store, &session).exists());
    assert!(matches!(
        store.read_verified(&reference(BYTES)),
        Err(MediaReadError::Missing(_))
    ));
}

#[test]
fn wal_references_survive_even_if_the_manifest_is_missing() {
    let (root, store, session) = fixture();
    accepted(&root, &store, &session);
    fs::remove_file(manifest(&store, &session)).unwrap();
    assert_eq!(sweep(&root).unwrap().objects_removed, 0);
    assert_eq!(store.read_verified(&reference(BYTES)).unwrap(), BYTES);
}

#[test]
fn wal_in_another_class_does_not_keep_an_unreferenced_copy() {
    let (root, store, session) = fixture();
    accepted(&root, &store, &session);
    let ordinary = MediaStore::open(&root, MediaStoreClass::NonDurable).unwrap();
    ordinary
        .session(&session)
        .unwrap()
        .register_and_append(&reference(BYTES), BYTES, || Ok(()))
        .unwrap();
    fs::remove_file(manifest(&ordinary, &session)).unwrap();
    assert_eq!(sweep(&root).unwrap().objects_removed, 1);
    assert_eq!(store.read_verified(&reference(BYTES)).unwrap(), BYTES);
    assert!(matches!(
        ordinary.read_verified(&reference(BYTES)),
        Err(MediaReadError::Missing(_))
    ));
}

#[test]
fn archive_fold_keeps_manifest_and_object() {
    let (root, store, session) = fixture();
    let path = accepted(&root, &store, &session);
    let directory = root.join("wal-archive-durable");
    broca_owner_only::create_dir_all(&directory).unwrap();
    let mut archive = broca_wal::archive::ArchiveWriter::create(directory.join("fold-1.ark"))
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
    archive
        .append(&session_addr(&session), &fs::read(&path).unwrap())
        .unwrap();
    archive.seal().unwrap();
    fs::remove_file(&path).unwrap();
    assert!(store.remove_session_after_wal_removal(&session).is_err());
    sweep(&root).unwrap();
    assert!(manifest(&store, &session).exists());
    assert_eq!(store.read_verified(&reference(BYTES)).unwrap(), BYTES);
}

#[test]
fn startup_sweep_does_not_wait_for_a_busy_store_or_block_admission() {
    let (root, store, session) = fixture();
    store
        .session(&session)
        .unwrap()
        .register_and_append(&reference(BYTES), BYTES, || Ok(()))
        .unwrap();
    let lock = store.lock().unwrap();
    lock.lock_shared().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        spawn_startup_sweep(root).unwrap().join().unwrap();
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(5))
        .expect("startup maintenance must not wait on a store lock");
    assert_eq!(store.read_verified(&reference(BYTES)).unwrap(), BYTES);
}

#[test]
fn startup_dispatch_returns_before_slow_maintenance_finishes() {
    let (release, waiting) = mpsc::channel();
    let (returned, admission) = mpsc::channel();
    let caller = std::thread::spawn(move || {
        let worker = spawn_sweep(move || {
            waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        })
        .unwrap();
        returned.send(()).unwrap();
        worker.join().unwrap();
    });
    let result = admission.recv_timeout(Duration::from_secs(1));
    // Always release the worker, including on the failure path, so a broken
    // synchronous dispatcher reports under this test instead of hanging a suite.
    release.send(()).unwrap();
    caller.join().unwrap();
    result.expect("admission waited for maintenance to finish");
}

#[test]
fn unreadable_wal_aborts_collection_instead_of_treating_references_as_empty() {
    let (root, store, session) = fixture();
    let path = accepted(&root, &store, &session);
    let mut bytes = fs::read(&path).unwrap();
    bytes[0] ^= 0xff;
    fs::write(path, bytes).unwrap();
    assert!(sweep(&root).is_err());
    assert!(manifest(&store, &session).exists());
    assert_eq!(store.read_verified(&reference(BYTES)).unwrap(), BYTES);
}

#[test]
fn interrupted_retention_deletion_keeps_manifest_and_object() {
    let (root, store, session) = fixture();
    let path = accepted(&root, &store, &session);
    let deleting = root
        .join("wal-ephemeral")
        .join(format!("{}.interrupted.deleting", session_addr(&session)));
    broca_owner_only::create_dir_all(deleting.parent().unwrap()).unwrap();
    fs::rename(path, deleting).unwrap();
    sweep(&root).unwrap();
    assert!(manifest(&store, &session).exists());
    assert_eq!(store.read_verified(&reference(BYTES)).unwrap(), BYTES);
}

#[test]
fn sweep_lock_is_exclusive_and_deadline_prevents_unbounded_work() {
    let (root, store, session) = fixture();
    accepted(&root, &store, &session);
    let lock = store.lock().unwrap();
    lock.lock_shared().unwrap();
    assert!(
        sweep(&root).is_err(),
        "a shared holder must exclude sweeping"
    );
    drop(lock);
    assert_eq!(
        sweep_until(&root, Some(Instant::now()), true)
            .unwrap_err()
            .kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(store.read_verified(&reference(BYTES)).unwrap(), BYTES);
}

#[test]
fn unchanged_manifest_is_not_rewritten_but_a_changed_one_is_reconciled() {
    let (root, store, session) = fixture();
    accepted(&root, &store, &session);
    assert_eq!(sweep(&root).unwrap().reconciled, 1);
    assert_eq!(sweep(&root).unwrap().reconciled, 0);
    store
        .session(&session)
        .unwrap()
        .register_and_append(&reference(REFUSED), REFUSED, || Ok(()))
        .unwrap();
    assert_eq!(sweep(&root).unwrap().reconciled, 1);
    assert_eq!(
        fs::read_to_string(manifest(&store, &session)).unwrap(),
        format!("{}\n", reference(BYTES).sha256)
    );
}

#[test]
fn failed_wal_removal_barrier_keeps_manifest_for_retry() {
    struct FailBarrier;
    impl StorageSteps for FailBarrier {
        fn reached(&mut self, step: Step) -> io::Result<()> {
            if step == Step::WalDirectorySynced {
                return Err(io::Error::other("injected WAL directory barrier"));
            }
            Ok(())
        }
    }
    let (root, store, session) = fixture();
    let path = accepted(&root, &store, &session);
    fs::remove_file(path).unwrap();
    assert!(store
        .remove_session_with(&session, &mut FailBarrier)
        .is_err());
    assert!(manifest(&store, &session).exists());
    store.remove_session_after_wal_removal(&session).unwrap();
    assert!(!manifest(&store, &session).exists());
}

#[test]
fn dropping_one_session_manifest_does_not_replay_unrelated_wals() {
    let (root, store, session) = fixture();
    let path = accepted(&root, &store, &session);
    let other = BindIdentity::new("/project/b", "harness", "unrelated");
    let unrelated = accepted(&root, &store, &other);
    fs::write(unrelated, b"unreadable WAL").unwrap();
    fs::remove_file(path).unwrap();
    store.remove_session_after_wal_removal(&session).unwrap();
    assert!(!manifest(&store, &session).exists());
    assert!(manifest(&store, &other).exists());
}

#[test]
fn changed_wal_cannot_reuse_a_previous_manifest_cache_stamp() {
    let (root, store, session) = fixture();
    let path = accepted(&root, &store, &session);
    sweep(&root).unwrap();
    let digest = &reference(BYTES).sha256;
    fs::write(manifest(&store, &session), format!("{digest}\n{digest}\n")).unwrap();
    fs::write(path, []).unwrap();
    sweep(&root).unwrap();
    assert!(fs::read(manifest(&store, &session)).unwrap().is_empty());
    assert!(matches!(
        store.read_verified(&reference(BYTES)),
        Err(MediaReadError::Missing(_))
    ));
}

#[test]
fn sweep_skips_empty_stores_without_touching_the_wal_corpus() {
    let (root, store, session) = fixture();
    let path = accepted(&root, &store, &session);
    fs::remove_file(store.root.join("objects").join(&reference(BYTES).sha256)).unwrap();
    fs::write(path, b"unrelated to an empty media store").unwrap();
    let _archive = crate::lease::acquire_archive_pass(&root).unwrap();
    let before = fs::read(manifest(&store, &session)).unwrap();
    let report = sweep(&root).unwrap();
    assert_eq!(report.wal_reads, 0);
    assert_eq!(report.reconciled, 0);
    assert_eq!(fs::read(manifest(&store, &session)).unwrap(), before);
    assert!(!store.root.join("swept").exists());
}

#[test]
fn sweep_reads_each_manifest_candidate_once_and_ignores_other_wals() {
    let (root, store, session) = fixture();
    accepted(&root, &store, &session);
    for class in [MediaStoreClass::NonDurable, MediaStoreClass::Ephemeral] {
        MediaStore::open(&root, class)
            .unwrap()
            .session(&session)
            .unwrap()
            .register_and_append(&reference(BYTES), BYTES, || Ok(()))
            .unwrap();
    }
    broca_owner_only::create_dir_all(root.join("wal")).unwrap();
    for n in 0..50 {
        fs::write(
            root.join("wal").join(format!("{n:016x}.wal")),
            b"unreadable unrelated WAL",
        )
        .unwrap();
    }
    let report = sweep(&root).unwrap();
    assert_eq!(
        report.wal_reads, 1,
        "all populated classes must share one candidate inventory"
    );
    assert_eq!(report.reconciled, 3);
    assert_eq!(report.objects_removed, 0);
    assert_eq!(store.read_verified(&reference(BYTES)).unwrap(), BYTES);
}

#[test]
fn damaged_manifest_retains_every_object_in_its_class() {
    let (root, store, session) = fixture();
    accepted(&root, &store, &session);
    fs::write(store.root.join("objects/orphan"), b"orphan").unwrap();
    fs::write(manifest(&store, &session), b"incomplete digest").unwrap();
    let report = sweep(&root).unwrap();
    assert_eq!(report.retained_classes, 1);
    assert_eq!(report.wal_reads, 0);
    assert_eq!(report.objects_removed, 0);
    assert_eq!(store.read_verified(&reference(BYTES)).unwrap(), BYTES);
    assert_eq!(
        fs::read(store.root.join("objects/orphan")).unwrap(),
        b"orphan"
    );
}

#[test]
fn preview_preserves_all_files_and_lease_epochs_and_respects_live_writers() {
    fn snapshot(path: &Path, held_lease: Option<&Path>) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut result = BTreeMap::new();
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                result.extend(snapshot(&path, held_lease));
            } else if Some(path.as_path()) != held_lease {
                let bytes = fs::read(&path)
                    .unwrap_or_else(|error| panic!("snapshot {}: {error:?}", path.display()));
                result.insert(path, bytes);
            }
        }
        result
    }
    let (root, store, session) = fixture();
    let leases = FileLeaseStore::new(root.join("leases"));
    let lease = leases.acquire(&lease_key_for(&session)).unwrap();
    let epoch = lease.epoch();
    let lease_files: Vec<_> = fs::read_dir(root.join("leases"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(lease_files.len(), 1);
    let lease_path = &lease_files[0];
    accepted(&root, &store, &session);
    store
        .session(&session)
        .unwrap()
        .register_and_append(&reference(REFUSED), REFUSED, || Ok(()))
        .unwrap();
    // Windows forbids reading through another handle while the writer holds the
    // lease's byte-range lock. Compare every other file now, then include the
    // lease bytes after release and check its persisted epoch on reacquisition.
    let before = snapshot(&root, Some(lease_path));
    let report = preview_sweep(&root).unwrap();
    assert_eq!(report.leased, 1);
    assert_eq!(report.objects_removed, 0);
    assert_eq!(snapshot(&root, Some(lease_path)), before);
    drop(lease);
    assert_eq!(snapshot(&root, Some(lease_path)), before);
    let before = snapshot(&root, None);
    let report = preview_sweep(&root).unwrap();
    assert_eq!(report.leased, 0);
    assert_eq!(report.objects_removed, 1);
    assert_eq!(snapshot(&root, None), before);
    let lease = leases.acquire(&lease_key_for(&session)).unwrap();
    assert_eq!(lease.epoch(), epoch + 1, "preview changed the lease epoch");
    drop(lease);
    assert_eq!(sweep(&root).unwrap().objects_removed, 1);
}

/// Counts the WAL directory barriers a removal performs. Production emits this step from
/// `sync_wal_directories_with`, so a zero here is a real observation, not a silent seam.
#[derive(Default)]
struct CountingSteps {
    synced: usize,
}

impl StorageSteps for CountingSteps {
    fn reached(&mut self, step: Step) -> io::Result<()> {
        if step == Step::WalDirectorySynced {
            self.synced += 1;
        }
        Ok(())
    }
}

#[test]
fn no_media_skips_wal_checks_and_barrier() {
    let (root, store, session) = fixture();
    let wal_dir = root.join("wal-ephemeral");
    broca_owner_only::create_dir_all(&wal_dir).unwrap();
    // A live WAL used to make this refuse after scanning every WAL directory. With no
    // media entries there is nothing to drop, so removal returns before any WAL check.
    let live = wal_dir.join(format!("{}.wal", session_addr(&session)));
    fs::write(&live, b"live").unwrap();
    let mut steps = CountingSteps::default();
    store.remove_session_with(&session, &mut steps).unwrap();
    assert_eq!(
        steps.synced, 0,
        "no-media deletion must skip the WAL barrier"
    );
}

#[test]
fn with_media_runs_the_barrier_and_drops_the_manifest() {
    let (root, store, session) = fixture();
    let path = accepted(&root, &store, &session);
    fs::remove_file(path).unwrap();
    let mut steps = CountingSteps::default();
    store.remove_session_with(&session, &mut steps).unwrap();
    assert!(
        steps.synced > 0,
        "the WAL barrier must still run before dropping media entries"
    );
    assert!(!manifest(&store, &session).exists());
}

#[test]
fn deleting_exact_check_refuses_while_deleting_exists() {
    let (root, store, session) = fixture();
    let path = accepted(&root, &store, &session);
    fs::remove_file(path).unwrap();
    let address = session_addr(&session);
    let lineage = "testlineage";
    let expired = root
        .join("wal-ephemeral")
        .join(format!("{address}.expired"));
    broca_owner_only::create_dir_all(&expired).unwrap();
    fs::write(expired.join(lineage), br#"{"lineage_id":"testlineage"}"#).unwrap();
    let deleting = root
        .join("wal-ephemeral")
        .join(format!("{address}.{lineage}.deleting"));
    fs::write(&deleting, b"deleting").unwrap();
    let mut steps = CountingSteps::default();
    assert!(
        store.remove_session_with(&session, &mut steps).is_err(),
        "an exact .deleting match must refuse like the old prefix scan"
    );
    fs::remove_file(&deleting).unwrap();
    let mut steps = CountingSteps::default();
    store.remove_session_with(&session, &mut steps).unwrap();
    assert!(!manifest(&store, &session).exists());
}
