use super::*;
use std::sync::mpsc;

const BYTES: &[u8] = b"%PDF-1.7\naccepted\n%%EOF";
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
