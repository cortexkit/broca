use super::*;
use crate::registry::{LineageOpener, LineageRegistry, OpenError};
use broca_core::{FileLeaseStore, FileWalStore, LeaseStore};
use broca_protocol::BindIdentity;
use broca_wal::{FramedRecord, Wal, WalError};
use std::sync::{atomic::AtomicBool, Mutex, Weak};
use tokio::sync::Notify;

struct FailsWhenArmed {
    inner: Box<dyn Wal + Send>,
    fail: Arc<AtomicBool>,
}
impl Wal for FailsWhenArmed {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        self.inner.append(fence, record, sync)
    }
    fn sync(&self) -> Result<(), WalError> {
        if self.fail.load(Ordering::Acquire) {
            Err(WalError::Io(std::io::Error::other(
                "actor final fsync failed",
            )))
        } else {
            self.inner.sync()
        }
    }
    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        self.inner.replay()
    }
    fn tail_after_seq(&self, after: u64) -> Result<Vec<FramedRecord>, WalError> {
        self.inner.tail_after_seq(after)
    }
    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }
    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.inner.lineage_id()
    }
}

#[derive(Default)]
struct CloseNotice(Notify);
#[async_trait::async_trait]
impl RunExecutor for CloseNotice {
    async fn execute(
        &self,
        _: RunJob,
        _: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("the test opens an idle actor without running a turn")
    }
    async fn on_session_close(&self, _: &BindIdentity) {
        self.0.notify_one();
    }
}
struct ShutdownOpener {
    leases: FileLeaseStore,
    wal: Arc<FileWalStore>,
    fail: Arc<AtomicBool>,
    writer: Mutex<Weak<WalHandle>>,
    executor: Arc<CloseNotice>,
}
#[async_trait::async_trait]
impl LineageOpener for ShutdownOpener {
    async fn open(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        let lease = self
            .leases
            .acquire(&broca_core::lease_key_for(key))
            .unwrap();
        let (fanout, observer) = crate::fanout::Fanout::new(self.wal.clone(), key.clone());
        let writer = Arc::new(
            WalHandle::spawn_observed(
                Box::new(FailsWhenArmed {
                    inner: self.wal.open(key).unwrap(),
                    fail: self.fail.clone(),
                }),
                lease.epoch(),
                Some(observer),
            )
            .unwrap(),
        );
        *self.writer.lock().unwrap() = Arc::downgrade(&writer);
        Ok(LineageActor::spawn(
            key.clone(),
            gen,
            lease,
            writer,
            self.executor.clone(),
            self.wal.clone(),
            registry,
            Duration::from_secs(60),
            fanout,
            None,
            None,
        ))
    }
}

#[tokio::test]
async fn actor_reports_final_writer_shutdown_failure() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-actor-final-sync-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("project")).unwrap();
    let key = BindIdentity::new(root.join("project"), "h", "s");
    let executor = Arc::new(CloseNotice::default());
    let opener = Arc::new(ShutdownOpener {
        leases: FileLeaseStore::new(root.join("leases")),
        wal: Arc::new(FileWalStore::new(root.join("wal"))),
        fail: Arc::new(AtomicBool::new(false)),
        writer: Mutex::new(Weak::new()),
        executor: executor.clone(),
    });
    let registry = LineageRegistry::new(opener.clone());
    let _actor = registry.actor_for(&key).await.unwrap();
    assert_eq!(
        registry.route_status(&key).await.unwrap(),
        LineageStatus::Idle
    );
    // Startup recovery has succeeded. Leave one append dirty and fail only its close sync.
    let writer = opener.writer.lock().unwrap().upgrade().unwrap();
    writer.record(WalRecord::Unknown).into_result().unwrap();
    drop(writer);
    opener.fail.store(true, Ordering::Release);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(10), executor.0.notified())
        .await
        .unwrap();
    let stopped = registry.stop_and_seal().await;
    assert!(
        !stopped,
        "final writer sync failure must make shutdown unsuccessful"
    );
    drop(
        opener
            .leases
            .acquire(&broca_core::lease_key_for(&key))
            .expect("failed shutdown still joined and released the writer lease"),
    );
    std::fs::remove_dir_all(root).unwrap();
}
