// Tests of the one-time step that makes the session store's single-folder tree its durable root.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use broca_protocol::BindIdentity;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::record::PartKind;
use broca_session_store::session::Session;
use broca_session_store::setup::SetupStore;
use broca_wal::StorageClass;

use super::{class_single_folder_tree, Layout};

/// An executor for a runtime whose test never runs anything.
struct NeverRuns;

#[async_trait::async_trait]
impl broca_module::executor::RunExecutor for NeverRuns {}

// A colon would be a valid move id but not a valid folder name on Windows.
const MOVE_ID: &str = "store-move-00000000-0000-4000-8000-000000000000";
const SETUP_PART: &[u8] = br#"{"system":"a setup part"}"#;

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[7; 32])
}

fn state_root(name: &str) -> PathBuf {
    let root = broca_test_support::scratch_root()
        .join(format!("store-layout-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn session() -> BindIdentity {
    BindIdentity::new("/project", "runner", "head")
}

/// Every file under `root`, by its path relative to `root`, with its bytes.
fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut found = BTreeMap::new();
    let mut folders = vec![root.to_path_buf()];
    while let Some(folder) = folders.pop() {
        for entry in std::fs::read_dir(&folder).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                folders.push(path);
            } else {
                found.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(&path).unwrap(),
                );
            }
        }
    }
    found
}

/// The tree the session store kept in its single folder: one session (its head, log, an object
/// and a setup part), and a move journal and staged session left by a move that never finished.
async fn single_folder_tree(state: &Path) {
    let single = state.join(broca_wal::SESSION_STORAGE_DIR);
    let key = session();
    let addr = broca_core::session_addr(&key);
    let backend = LocalBackend::new(&single);
    Session::create(
        LocalBackend::new(&single),
        SessionAddr(addr.clone()),
        SessionTriple {
            project_root: key.project_root.display().to_string(),
            harness: key.harness.clone(),
            session: key.session.clone(),
        },
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let setup_key = deployment()
        .setup_key_for(Cipher::XChaCha20Poly1305)
        .unwrap();
    SetupStore::new(&backend, &setup_key)
        .put(PartKind::SystemText, SETUP_PART)
        .await
        .unwrap();
    let object = single.join("objects").join(&addr).join("object");
    std::fs::create_dir_all(object.parent().unwrap()).unwrap();
    std::fs::write(&object, b"an object").unwrap();
    let journal = single.join("moves").join("00000000000000aa");
    std::fs::create_dir_all(journal.parent().unwrap()).unwrap();
    std::fs::write(&journal, b"{\"move_id\":\"a journal\"}").unwrap();
    let staged = single
        .join("staging")
        .join(MOVE_ID)
        .join("logs")
        .join("00000000000000aa");
    std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
    std::fs::write(&staged, b"a staged log").unwrap();
}

/// The single-folder tree becomes the durable root with every file intact, its journal and
/// staged session in the moves folder, and its session opens from the durable root with its
/// setup part. A second start changes nothing.
#[tokio::test]
async fn the_single_folder_tree_becomes_the_durable_root_intact() {
    let state = state_root("classed");
    single_folder_tree(&state).await;
    let single = state.join(broca_wal::SESSION_STORAGE_DIR);
    let before = files(&single);
    assert!(before.keys().any(|path| path.starts_with("heads")));
    assert!(before.keys().any(|path| path.starts_with("logs")));
    assert!(before.keys().any(|path| path.starts_with("setup")));

    assert_eq!(
        class_single_folder_tree(&state).unwrap(),
        Layout::Classed {
            journals: 1,
            staged: 1
        }
    );

    assert!(
        !single.exists(),
        "the single folder was renamed, not copied"
    );
    let durable = state.join(broca_wal::SESSION_STORAGE_DURABLE_DIR);
    let moves = broca_wal::session_storage_moves(&state);
    let mut after = files(&durable);
    for (path, bytes) in files(&moves.join(broca_wal::MOVE_JOURNAL_DIR)) {
        after.insert(Path::new("moves").join(path), bytes);
    }
    for (path, bytes) in files(&moves.join(broca_wal::MOVE_STAGING_DIR)) {
        after.insert(Path::new("staging").join(path), bytes);
    }
    assert_eq!(after, before, "every file of the tree, byte for byte");
    assert!(!durable.join("moves").exists() && !durable.join("staging").exists());

    // The session opens from the durable root, its setup part with it.
    let key = session();
    assert_eq!(
        broca_wal::session_storage_class(&state, &broca_core::session_addr(&key)).unwrap(),
        Some(StorageClass::Durable)
    );
    let runtime = broca_module::store_open::StoreRuntime::open(
        &state,
        deployment(),
        Arc::new(NeverRuns),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    assert_eq!(runtime.class_of(&key).unwrap(), Some(StorageClass::Durable));
    let part = broca_session_store::record::PartRef {
        kind: PartKind::SystemText,
        digest: broca_session_store::setup::digest(SETUP_PART),
    };
    use broca_session_loop::parts::Parts as _;
    assert_eq!(
        runtime
            .parts_for(StorageClass::Durable, Cipher::XChaCha20Poly1305)
            .get(&part)
            .unwrap(),
        serde_json::from_slice::<serde_json::Value>(SETUP_PART).unwrap()
    );
    Session::load(
        LocalBackend::new(&durable),
        SessionAddr(broca_core::session_addr(&key)),
        &SessionTriple {
            project_root: key.project_root.display().to_string(),
            harness: key.harness.clone(),
            session: key.session.clone(),
        },
        &deployment(),
    )
    .await
    .unwrap();

    // Opening the runtime prepared the new ordinary class root's recovery directory.
    // A second start must leave both that root and the migrated durable files unchanged.
    let durable_files = files(&durable);
    let moves_files = files(&moves);
    let ordinary_entries: Vec<_> = std::fs::read_dir(&single)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(
        ordinary_entries,
        vec![std::ffi::OsString::from("recovery-candidates")]
    );
    assert_eq!(class_single_folder_tree(&state).unwrap(), Layout::Unchanged);
    assert_eq!(files(&durable), durable_files);
    assert_eq!(files(&moves), moves_files);
    assert!(single.join("recovery-candidates").is_dir());
    assert_eq!(std::fs::read_dir(&single).unwrap().count(), 1);
    let _ = std::fs::remove_dir_all(&state);
}

/// A state root that already has a durable root is left alone, whatever the ordinary root
/// holds: from then on the folder is the ordinary root, not the single-folder tree.
#[tokio::test]
async fn a_state_root_with_a_durable_root_is_left_alone() {
    let state = state_root("durable-present");
    single_folder_tree(&state).await;
    let durable = state.join(broca_wal::SESSION_STORAGE_DURABLE_DIR);
    std::fs::create_dir(&durable).unwrap();
    let single = state.join(broca_wal::SESSION_STORAGE_DIR);
    let before = files(&single);
    assert_eq!(class_single_folder_tree(&state).unwrap(), Layout::Unchanged);
    assert_eq!(files(&single), before);
    assert_eq!(std::fs::read_dir(&durable).unwrap().count(), 0);
    assert!(!broca_wal::session_storage_moves(&state).exists());
    let _ = std::fs::remove_dir_all(&state);
}

/// An empty single folder, which startup created before any session was placed in it, and a
/// state root with none, have nothing to keep.
#[test]
fn an_empty_or_absent_single_folder_changes_nothing() {
    let state = state_root("empty");
    assert_eq!(class_single_folder_tree(&state).unwrap(), Layout::Unchanged);
    std::fs::create_dir(state.join(broca_wal::SESSION_STORAGE_DIR)).unwrap();
    assert_eq!(class_single_folder_tree(&state).unwrap(), Layout::Unchanged);
    assert!(state.join(broca_wal::SESSION_STORAGE_DIR).is_dir());
    assert!(!state.join(broca_wal::SESSION_STORAGE_DURABLE_DIR).exists());
    assert!(!broca_wal::session_storage_moves(&state).exists());
    let _ = std::fs::remove_dir_all(&state);
}

/// A journal whose name the moves folder already holds is never replaced: the step refuses,
/// and the single folder keeps its sessions until an operator looks.
#[tokio::test]
async fn a_journal_the_moves_folder_already_holds_is_refused() {
    let state = state_root("collision");
    single_folder_tree(&state).await;
    let taken = broca_wal::session_storage_moves(&state)
        .join(broca_wal::MOVE_JOURNAL_DIR)
        .join("00000000000000aa");
    std::fs::create_dir_all(taken.parent().unwrap()).unwrap();
    std::fs::write(&taken, b"another journal").unwrap();
    let error = class_single_folder_tree(&state).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists, "{error}");
    assert_eq!(std::fs::read(&taken).unwrap(), b"another journal");
    let single = state.join(broca_wal::SESSION_STORAGE_DIR);
    assert!(single.join("moves").join("00000000000000aa").is_file());
    assert!(single.join("heads").is_dir());
    assert!(!state.join(broca_wal::SESSION_STORAGE_DURABLE_DIR).exists());
    let _ = std::fs::remove_dir_all(&state);
}
