use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use broca_core::{lease_key_for, session_lease_file, FileLeaseStore, LeaseStore};
use broca_protocol::BindIdentity;
use broca_records::session_storage::StorageClass;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;

use super::{sweep_once, Swept, SWEPT};

fn state_root(name: &str) -> PathBuf {
    let root = broca_test_support::scratch_root()
        .join(format!("lease-sweep-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("leases")).unwrap();
    root
}

fn key(root: &Path, name: &str) -> BindIdentity {
    BindIdentity::new(root.join("project"), "runner", name)
}

/// Creates the session `key` in the root of `class`, as the session store files one.
async fn store(root: &Path, class: StorageClass, key: &BindIdentity) {
    let backend = LocalBackend::new(class.store_root(root));
    let triple = SessionTriple {
        project_root: key.project_root.display().to_string(),
        harness: key.harness.clone(),
        session: key.session.clone(),
    };
    let session = Session::create(
        &backend,
        SessionAddr(broca_core::session_addr(key)),
        triple,
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    session.close().await.unwrap();
}

/// Takes and lets go of `key`'s lease, as the file store leaves it: its file stays.
fn leave_lease_file(root: &Path, key: &BindIdentity) {
    drop(
        FileLeaseStore::new(root.join("leases"))
            .acquire(&lease_key_for(key))
            .unwrap(),
    );
}

fn lease_files(root: &Path) -> Vec<String> {
    let mut files: Vec<String> = std::fs::read_dir(root.join("leases"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    files.sort();
    files
}

/// The pass removes the lease files of sessions the store no longer holds, in any root, and
/// keeps those of sessions it holds. A file changed since the pass began is left alone, and so
/// is one `remove` reports it could not remove. Once the pass has run, a later start skips it.
#[tokio::test]
async fn the_pass_removes_only_the_lease_files_of_sessions_that_are_gone() {
    let root = state_root("gone");
    let durable = key(&root, "durable");
    let ordinary = key(&root, "ordinary");
    store(&root, StorageClass::Durable, &durable).await;
    store(&root, StorageClass::Ordinary, &ordinary).await;
    for kept in [&durable, &ordinary] {
        leave_lease_file(&root, kept);
    }
    let gone = key(&root, "gone");
    let held = key(&root, "held");
    for orphan in [&gone, &held] {
        leave_lease_file(&root, orphan);
    }
    // The pass begins after every file above was last written.
    std::thread::sleep(Duration::from_millis(20));
    let began = SystemTime::now();
    std::thread::sleep(Duration::from_millis(20));
    // Taken after the pass began: a session being created now.
    let new = key(&root, "new");
    leave_lease_file(&root, &new);

    let asked = RefCell::new(Vec::new());
    let swept = sweep_once(
        &root,
        began,
        |name| {
            asked.borrow_mut().push(name.to_owned());
            if name == session_lease_file(&held) {
                return Ok(false);
            }
            std::fs::remove_file(root.join("leases").join(name))?;
            Ok(true)
        },
        || false,
    )
    .unwrap();
    assert_eq!(
        swept,
        Some(Swept {
            kept: 2,
            removed: 1,
            skipped: 2,
        })
    );
    let mut asked = asked.into_inner();
    asked.sort();
    let mut orphans = vec![session_lease_file(&gone), session_lease_file(&held)];
    orphans.sort();
    assert_eq!(
        asked, orphans,
        "only unchanged files of sessions that are gone"
    );
    let mut left = vec![
        session_lease_file(&durable),
        session_lease_file(&ordinary),
        session_lease_file(&held),
        session_lease_file(&new),
    ];
    left.sort();
    assert_eq!(lease_files(&root), left);

    assert!(root.join(SWEPT).exists());
    let again = sweep_once(
        &root,
        SystemTime::now(),
        |_| panic!("a second pass removes nothing"),
        || false,
    );
    assert_eq!(again.unwrap(), None);
    let _ = std::fs::remove_dir_all(&root);
}

/// Through broca's own lease store, the pass removes a session's left-behind file and leaves one
/// a live holder has locked, though neither session is stored any more.
#[tokio::test]
async fn through_the_lease_store_a_held_file_is_never_removed() {
    let root = state_root("store");
    let gone = key(&root, "gone");
    let held_key = key(&root, "held");
    leave_lease_file(&root, &gone);
    let files = FileLeaseStore::new(root.join("leases"));
    let held = files.acquire(&lease_key_for(&held_key)).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let began = SystemTime::now();
    let leases = broca_core::session_leases(&root);
    let swept = sweep_once(&root, began, |name| leases.remove_if_free(name), || false)
        .unwrap()
        .unwrap();
    assert_eq!((swept.removed, swept.skipped), (1, 1));
    assert_eq!(lease_files(&root), vec![session_lease_file(&held_key)]);
    drop(held);
    let _ = std::fs::remove_dir_all(&root);
}

/// A pass the module's stop interrupts removes nothing more, records nothing, and runs again at
/// the next start.
#[tokio::test]
async fn a_stopped_pass_records_nothing_and_runs_again() {
    let root = state_root("stopped");
    let gone = key(&root, "gone");
    leave_lease_file(&root, &gone);
    std::thread::sleep(Duration::from_millis(20));
    let began = SystemTime::now();
    let stopped = sweep_once(&root, began, |_| panic!("nothing is removed"), || true);
    assert_eq!(stopped.unwrap_err().kind(), std::io::ErrorKind::Interrupted);
    assert!(!root.join(SWEPT).exists());
    let swept = sweep_once(
        &root,
        began,
        |name| {
            std::fs::remove_file(root.join("leases").join(name))?;
            Ok(true)
        },
        || false,
    )
    .unwrap();
    assert_eq!(swept.map(|swept| swept.removed), Some(1));
    let _ = std::fs::remove_dir_all(&root);
}

/// A head the pass can't read stops it before it removes anything, and leaves it to run again.
#[tokio::test]
async fn an_unreadable_head_stops_the_pass_before_it_removes_anything() {
    let root = state_root("unreadable");
    let gone = key(&root, "gone");
    leave_lease_file(&root, &gone);
    let damaged = StorageClass::Ordinary
        .store_root(&root)
        .join("heads")
        .join("0123456789abcdef");
    std::fs::create_dir_all(&damaged).unwrap();
    std::fs::write(damaged.join("head"), b"not a head").unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let result = sweep_once(
        &root,
        SystemTime::now(),
        |_| panic!("nothing is removed"),
        || false,
    );
    assert!(result.is_err());
    assert_eq!(lease_files(&root), vec![session_lease_file(&gone)]);
    assert!(!root.join(SWEPT).exists());
    let _ = std::fs::remove_dir_all(&root);
}
