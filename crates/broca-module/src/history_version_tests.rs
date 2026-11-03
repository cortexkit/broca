//! The serving and boot paths share a durable history-version fence.

use std::sync::Arc;

use crate::executor::{RunExecutor, RunJob};
use crate::serve::{ServeError, ServeHandler, SessionReadRequest};
use crate::{DefaultOpener, LineageRegistry};
use broca_core::OutputEvent;
use broca_core::{FileLeaseStore, FileWalStore, LeaseStore, ServedHistory, WalStore};
use broca_protocol::BindIdentity;

struct NoExecution;
#[async_trait::async_trait]
impl RunExecutor for NoExecution {
    async fn execute(
        &self,
        _: RunJob,
        _: tokio::sync::mpsc::Sender<OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        panic!("history recovery must not call a model or execute a run")
    }
}

fn registry(
    root: &std::path::Path,
    wal: Arc<FileWalStore>,
    index: Arc<broca_store::SqliteStore>,
) -> LineageRegistry {
    wal.set_served_history(index.clone());
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(root.join("leases"))),
        wal.clone(),
        Arc::new(NoExecution),
    )
    .with_materialized_store(index.clone());
    LineageRegistry::new_with_reader(Arc::new(opener), wal, Some(index))
}

#[tokio::test]
async fn an_unwritten_read_creates_neither_a_log_nor_a_history_row() {
    let root =
        broca_test_support::scratch_root().join(format!("history-empty-{}", std::process::id()));
    std::fs::create_dir_all(root.join("project")).unwrap();
    let key = BindIdentity::new(root.join("project"), "runner", "empty");
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let index = Arc::new(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
    let registry = registry(&root, wal.clone(), index.clone());
    let head = registry.head(&key).await.unwrap();
    let page = registry
        .read(SessionReadRequest::new(key.clone(), None, None))
        .await
        .unwrap();
    assert_eq!(head.incarnation, "unwritten");
    assert_eq!(page.incarnation, "unwritten");
    assert_eq!(head.history_version, 0);
    assert_eq!(page.history_version, 0);
    assert!(!wal.wal_path(&key).exists());
    assert_eq!(
        index
            .history_read_version(&broca_core::session_addr(&key))
            .unwrap(),
        None
    );
    assert!(registry.open_sessions().is_empty());
}

#[tokio::test]
async fn boot_finishes_a_crashed_history_fence_without_a_session_sweep() {
    let root = broca_test_support::scratch_root()
        .join(format!("history-fence-boot-{}", std::process::id()));
    std::fs::create_dir_all(root.join("project")).unwrap();
    let key = BindIdentity::new(root.join("project"), "runner", "fenced");
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let index = Arc::new(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
    let lineage;
    let before;
    {
        let leases = FileLeaseStore::new(root.join("leases"));
        let lease = leases.acquire(&broca_core::lease_key_for(&key)).unwrap();
        let mut writer = wal.open(&key).unwrap();
        let mut start: broca_wal::WalRecord = serde_json::from_str(
            include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        if let broca_wal::WalRecord::RunStarted { session, .. } = &mut start {
            *session = key.clone();
        }
        writer.append(lease.epoch(), &start, false).unwrap();
        writer.append(lease.epoch(), &serde_json::from_value(serde_json::json!({"type":"run_finished","reason":"completed","usage":{},"retries_used":0})).unwrap(), false).unwrap();
        writer.sync().unwrap();
        lineage = writer.lineage_id().map(|id| id.to_string());
        before = index
            .served_snapshot(
                &broca_core::session_addr(&key),
                true,
                lineage.as_deref(),
                None,
            )
            .unwrap()
            .history_version;
        index.history_rewritten(&key, lineage.as_deref()).unwrap();
    }
    // Drop the original database and writer before reopening, as after a process crash.
    drop(index);
    drop(wal);
    let index = Arc::new(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let registry = registry(&root, wal, index.clone());
    assert_eq!(index.rewriting_sessions().unwrap(), vec![key.clone()]);
    assert!(matches!(
        registry.head(&key).await,
        Err(ServeError::HistoryRewriting)
    ));
    assert!(matches!(
        registry
            .read(SessionReadRequest::new(key.clone(), None, None))
            .await,
        Err(ServeError::HistoryRewriting)
    ));
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            registry.finish_history_rewrites_at_boot()
        )
        .await
        .unwrap()
        .unwrap(),
        1
    );
    assert!(index.rewriting_sessions().unwrap().is_empty());
    let head = registry.head(&key).await.unwrap();
    assert!(head.history_version > before);
    assert_eq!(head.incarnation, lineage.unwrap());
    assert!(registry.stop_and_seal().await);
}
