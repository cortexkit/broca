//! Today's readers that don't go through `check_move`, against a session the new session storage
//! owns: its head exists, or a move journal names it while a move is in flight. Each skips the
//! session or refuses it, and none reads, expires, recovers or projects its WAL.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use broca_core::{FileLeaseStore, FileWalStore, LeaseStore, ServedHistory, WalStore};
use broca_protocol::{BindIdentity, RunId};

use super::{LineageRegistry, OpenError};
use crate::opener::DefaultOpener;
use crate::serve::{ServeError, ServeHandler, SessionReadRequest};

const MOVE_ID: &str = "store-move:00000000-0000-4000-8000-000000000001";

struct NoExecution;

#[async_trait::async_trait]
impl crate::executor::RunExecutor for NoExecution {
    async fn execute(
        &self,
        _: crate::executor::RunJob,
        _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        panic!("a reader of a session the new storage owns must not run anything")
    }
}

fn scratch(tag: &str) -> std::path::PathBuf {
    let root = broca_test_support::scratch_root()
        .join(format!("registry-store-owner-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("project")).unwrap();
    root
}

/// A registry over `root` reading today's WALs directly, with no new storage of its own.
fn registry(root: &std::path::Path, wal: Arc<FileWalStore>) -> LineageRegistry {
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(root.join("leases"))),
        wal.clone(),
        Arc::new(NoExecution),
    );
    LineageRegistry::new_with_reader(Arc::new(opener), wal, None)
}

/// Writes one finished episode of `key` to today's storage.
fn written(root: &std::path::Path, wal: &FileWalStore, key: &BindIdentity) {
    let lease = FileLeaseStore::new(root.join("leases"))
        .acquire(&broca_core::lease_key_for(key))
        .unwrap();
    let mut writer = wal.open(key).unwrap();
    crate::move_install::tests::episode(writer.as_mut(), key, "run");
    drop(writer);
    drop(lease);
}

/// Names `key` in a move journal under `root`, as a move does before it touches anything else.
pub(crate) fn plant_journal(root: &std::path::Path, key: &BindIdentity) {
    let address = broca_core::session_addr(key);
    let path = broca_wal::session_storage_move_journal(root, &address);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let journal = broca_wal::MoveJournal {
        move_id: MOVE_ID.into(),
        address,
        project_root: key.project_root.display().to_string(),
        harness: key.harness.clone(),
        session: key.session.clone(),
        lineage_id: None,
        wal: "wal/x.wal".into(),
        staging: format!("staging/{MOVE_ID}"),
        frozen: None,
        appends_in_conversation: None,
        class: Some(broca_wal::StorageClass::Ordinary),
    };
    std::fs::write(path, serde_json::to_vec(&journal).unwrap()).unwrap();
}

/// Plants `key`'s head in the new storage under `root`: today's readers look only at whether
/// it exists.
pub(crate) fn plant_head(root: &std::path::Path, key: &BindIdentity) {
    let head = broca_wal::session_storage_head(
        root,
        broca_wal::StorageClass::Ordinary,
        &broca_core::session_addr(key),
    );
    std::fs::create_dir_all(head.parent().unwrap()).unwrap();
    std::fs::write(head, b"head").unwrap();
}

fn moving<T>(result: Result<T, OpenError>) {
    match result {
        Err(OpenError::SessionMoving { cut_id }) => assert_eq!(cut_id, MOVE_ID),
        Err(other) => panic!("expected the move's refusal, got {other:?}"),
        Ok(_) => panic!("expected the move's refusal, got an answer"),
    }
}

/// While a move journal names the session, today's page, tag and run-state readers refuse it
/// as moving under the move's id, and the expiry check leaves it alone. Each read succeeds
/// before the journal exists, so the refusal is the journal's doing.
#[tokio::test]
async fn todays_reads_refuse_a_session_a_move_is_taking() {
    let root = scratch("moving-reads");
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let key = BindIdentity::new(root.join("project"), "runner", "moving");
    written(&root, &wal, &key);
    let registry = registry(&root, wal.clone());
    assert!(registry
        .route_read_page(&key, None, None, false)
        .await
        .is_ok());
    assert!(registry.route_tag_location(&key, 1).await.is_ok());
    assert!(registry
        .route_run_state(&key, RunId("run".into()))
        .await
        .unwrap()
        .is_some());

    plant_journal(&root, &key);
    assert!(registry.check_expired(&key, None).await.is_ok());
    moving(registry.route_read_page(&key, None, None, false).await);
    moving(registry.route_read(&key, None, None).await);
    moving(registry.route_tag_location(&key, 1).await);
    moving(registry.route_run_state(&key, RunId("run".into())).await);
    assert!(registry.open_sessions().is_empty(), "no actor was opened");
    let _ = std::fs::remove_dir_all(&root);
}

/// The startup crash-orphan pass neither marks nor opens a session the new storage owns: its
/// runs live there, and opening it here would recover a run on the WAL it no longer owns.
#[tokio::test]
async fn the_crash_orphan_pass_leaves_out_sessions_the_new_storage_owns() {
    let root = scratch("crash-orphans");
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let held = BindIdentity::new(root.join("project"), "runner", "held");
    let moving = BindIdentity::new(root.join("project"), "runner", "moving");
    let today = BindIdentity::new(root.join("project"), "runner", "today");
    for key in [&held, &moving, &today] {
        let mut writer = wal.open(key).unwrap();
        writer
            .append(1, &crate::move_install::tests::started(key, "run"), true)
            .unwrap();
    }
    plant_head(&root, &held);
    plant_journal(&root, &moving);
    let registry = registry(&root, wal.clone());
    let later = SystemTime::now() + Duration::from_secs(3600);
    let candidates = [&held, &moving, &today]
        .into_iter()
        .map(|key| (RunId("run".into()), key.clone()))
        .collect();
    let sweep = registry.mark_crash_orphans(wal.as_ref(), candidates, later);
    assert_eq!(sweep.marked, 1, "only today's session is marked");
    assert_eq!(sweep.not_live, 2);
    assert!(registry
        .inner
        .crash_orphan_marks
        .lock()
        .unwrap()
        .contains_key(&today));

    // A session that moves after the marking pass is left out of the sealing pass too.
    plant_journal(&root, &today);
    let sweep = registry.seal_marked_crash_orphans(sweep).await;
    assert_eq!(sweep.not_live, 3);
    assert_eq!(sweep.open_failed, 0);
    assert!(registry.inner.crash_orphan_marks.lock().unwrap().is_empty());
    assert!(registry.open_sessions().is_empty(), "no actor was opened");
    let _ = std::fs::remove_dir_all(&root);
}

/// Boot doesn't open a session the new storage holds to finish a history rewrite: it has no
/// writer here, and opening it would fail the whole boot pass.
#[tokio::test]
async fn boot_leaves_a_rewrite_of_a_session_the_new_storage_holds_alone() {
    let root = scratch("history-rewrite");
    let key = BindIdentity::new(root.join("project"), "runner", "fenced");
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let index = Arc::new(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
    written(&root, &wal, &key);
    let lineage = broca_wal::read_records_range(
        wal.wal_path(&key),
        broca_wal::DEFAULT_MAX_READ_BYTES,
        0,
        None,
        None,
    )
    .unwrap()
    .lineage_id
    .map(|id| id.to_string());
    index.history_rewritten(&key, lineage.as_deref()).unwrap();
    plant_head(&root, &key);
    wal.set_served_history(index.clone());
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(root.join("leases"))),
        wal.clone(),
        Arc::new(NoExecution),
    )
    .with_materialized_store(index.clone());
    let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal, Some(index.clone()));
    assert_eq!(registry.finish_history_rewrites_at_boot().await.unwrap(), 0);
    assert_eq!(index.rewriting_sessions().unwrap(), vec![key.clone()]);
    assert!(registry.open_sessions().is_empty(), "no actor was opened");
    let _ = std::fs::remove_dir_all(&root);
}

/// A read that takes no lease checks for the session's head, finds none, and reads today's
/// storage. When a move publishes the head in between, today's storage refuses the session as
/// kept in the new storage, and the read is served once more from there instead of failing.
#[tokio::test]
async fn a_read_that_meets_a_head_published_meanwhile_is_served_from_the_new_storage() {
    let root = scratch("reroute");
    let key = BindIdentity::new(root.join("project"), "runner", "rerouted");
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    written(&root, &wal, &key);
    let deployment = broca_session_store::keys::DeploymentKey::new(1, &[7; 32]);
    // The session as a move stages it, ready to be published by renaming its folder in.
    let staged = root.join("staged");
    let session = broca_session_store::session::Session::create(
        broca_session_store::backend::LocalBackend::new(staged.join("session-store")),
        broca_session_store::backend::SessionAddr(broca_core::session_addr(&key)),
        broca_session_store::head::SessionTriple {
            project_root: key.project_root.display().to_string(),
            harness: key.harness.clone(),
            session: key.session.clone(),
        },
        &deployment,
        false,
    )
    .await
    .unwrap();
    session.close().await.unwrap();
    let executor: Arc<dyn crate::executor::RunExecutor> = Arc::new(NoExecution);
    let runtime = crate::store_open::StoreRuntime::open(
        &root,
        deployment,
        Arc::clone(&executor),
        Duration::from_secs(600),
    )
    .await
    .unwrap();
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(root.join("leases"))),
        wal.clone(),
        executor,
    )
    .with_store_runtime(Arc::new(runtime));
    let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal, None);
    let today = registry
        .read(SessionReadRequest::new(key.clone(), None, None))
        .await
        .unwrap();

    // Publish between the read's head check and its read of today's storage.
    let (from, to) = (staged.join("session-store"), root.join("session-store"));
    *registry.inner.store_owner_probe.lock().unwrap() = Some(Box::new(move || {
        std::fs::rename(from, to).unwrap();
    }));
    let served = registry
        .read(SessionReadRequest::new(key.clone(), None, None))
        .await
        .unwrap();
    assert!(registry.inner.store_owner_probe.lock().unwrap().is_none());
    // The answer is the new storage's, exactly as a read that finds the head serves it, and
    // not today's copy.
    let direct = registry
        .read(SessionReadRequest::new(key.clone(), None, None))
        .await
        .unwrap();
    assert_eq!(served, direct);
    assert_ne!(served.incarnation, today.incarnation);
    // A refusal for any other reason is not re-routed.
    assert!(registry
        .store_after_refusal(&key, &ServeError::HistoryRewriting)
        .unwrap()
        .is_none());
    let _ = std::fs::remove_dir_all(&root);
}
