use super::*;
use broca_protocol::{provider::NormalizedEvent, CallOptions, StepId};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tokio::sync::Notify;

struct FinalSyncWal {
    seq: u64,
    syncs: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
    panic_on_final: bool,
}

impl Drop for FinalSyncWal {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

impl Wal for FinalSyncWal {
    fn append(&mut self, _: u64, _: &WalRecord, _: bool) -> Result<u64, WalError> {
        self.seq += 1;
        Ok(self.seq)
    }
    fn sync(&self) -> Result<(), WalError> {
        if self.syncs.fetch_add(1, Ordering::SeqCst) == 2 {
            if self.panic_on_final {
                panic!("final sync panic");
            }
            return Err(WalError::Io(std::io::Error::other("final fsync failed")));
        }
        Ok(())
    }
    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        Ok(vec![])
    }
    fn tail_after_seq(&self, _: u64) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        Ok(vec![])
    }
    fn last_seq(&self) -> u64 {
        self.seq
    }
}

#[tokio::test]
async fn shutdown_reports_final_fsync_failure() {
    let syncs = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let writer = WalHandle::spawn(
        Box::new(FinalSyncWal {
            seq: 0,
            syncs: syncs.clone(),
            dropped: dropped.clone(),
            panic_on_final: false,
        }),
        1,
    )
    .unwrap();
    writer.record(WalRecord::Unknown).into_result().unwrap();
    assert_eq!(writer.sync().await.unwrap(), 1);
    writer.record(WalRecord::Unknown).into_result().unwrap();
    let poison = writer.poisoned.clone();
    let head = writer.durable_head();
    let error = writer
        .shutdown()
        .await
        .expect_err("final fsync failure must reach the owner");
    assert!(error.to_string().contains("final fsync failed"));
    assert!(current_poison(&poison)
        .unwrap()
        .contains("final fsync failed"));
    assert_eq!(
        head.load(Ordering::Acquire),
        1,
        "failed sync never advances durability"
    );
    assert_eq!(
        syncs.load(Ordering::Acquire),
        3,
        "only the shutdown sync failed"
    );
    assert!(
        dropped.load(Ordering::Acquire),
        "shutdown joins even on sync failure"
    );
}

#[tokio::test]
async fn shutdown_reports_writer_join_failure() {
    let syncs = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let writer = WalHandle::spawn(
        Box::new(FinalSyncWal {
            seq: 0,
            syncs: syncs.clone(),
            dropped: dropped.clone(),
            panic_on_final: true,
        }),
        1,
    )
    .unwrap();
    writer.record(WalRecord::Unknown).into_result().unwrap();
    writer.sync().await.unwrap();
    writer.record(WalRecord::Unknown).into_result().unwrap();
    let error = writer
        .shutdown()
        .await
        .expect_err("a writer panic must reach the owner");
    assert!(
        error.to_string().contains("writer thread panicked"),
        "join cause must be preserved: {error}"
    );
    assert!(dropped.load(Ordering::Acquire));
}

struct IoGate {
    entered: Notify,
    release_tx: std_mpsc::Sender<()>,
    release_rx: Mutex<std_mpsc::Receiver<()>>,
}
impl Default for IoGate {
    fn default() -> Self {
        let (release_tx, release_rx) = std_mpsc::channel();
        Self {
            entered: Notify::new(),
            release_tx,
            release_rx: Mutex::new(release_rx),
        }
    }
}
impl IoGate {
    fn hold(&self) -> Result<(), WalError> {
        self.entered.notify_one();
        self.release_rx
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(10))
            .map_err(|error| {
                WalError::Io(std::io::Error::other(format!(
                    "I/O gate timed out: {error}"
                )))
            })
    }
    async fn wait(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(10), self.entered.notified())
            .await
            .expect("I/O reached its explicit gate");
    }
    fn release(&self) {
        let _ = self.release_tx.send(());
    }
}

#[derive(Default)]
struct IoState {
    append: IoGate,
    sync: IoGate,
    exited: Notify,
    synced: AtomicBool,
}
struct BlockedWal {
    seq: u64,
    state: Arc<IoState>,
    step_dirty: bool,
}
impl Drop for BlockedWal {
    fn drop(&mut self) {
        self.state.exited.notify_one();
    }
}
impl Wal for BlockedWal {
    fn append(&mut self, _: u64, record: &WalRecord, _: bool) -> Result<u64, WalError> {
        if matches!(record, WalRecord::StepStarted { .. }) {
            self.state.append.hold()?;
            self.step_dirty = true;
        }
        self.seq += 1;
        Ok(self.seq)
    }
    fn sync(&self) -> Result<(), WalError> {
        if self.step_dirty {
            self.state.sync.hold()?;
            self.state.synced.store(true, Ordering::Release);
        }
        Ok(())
    }
    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        Ok(vec![])
    }
    fn tail_after_seq(&self, _: u64) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        Ok(vec![])
    }
    fn last_seq(&self) -> u64 {
        self.seq
    }
}
struct BlockedStore(Arc<IoState>);
impl WalStore for BlockedStore {
    fn open(&self, _: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(Box::new(BlockedWal {
            seq: 0,
            state: self.0.clone(),
            step_dirty: false,
        }))
    }
    fn read_records(&self, _: &BindIdentity) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        Ok(vec![])
    }
}
struct PendingProvider;
#[async_trait::async_trait]
impl crate::Provider for PendingProvider {
    async fn step(
        &self,
        _: StepId,
        _: &CallOptions,
    ) -> tokio::sync::mpsc::Receiver<NormalizedEvent> {
        std::future::pending().await
    }
}

struct ObserveCancellation<F> {
    future: std::pin::Pin<Box<F>>,
    cancelling: Arc<Notify>,
}
impl<F: std::future::Future> std::future::Future for ObserveCancellation<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}
impl<F> Drop for ObserveCancellation<F> {
    fn drop(&mut self) {
        self.cancelling.notify_one();
    }
}

#[derive(Debug)]
struct NotifyingLease {
    lease: Option<Box<dyn LeaseHandle>>,
    released: Arc<Notify>,
}
impl LeaseHandle for NotifyingLease {
    fn epoch(&self) -> u64 {
        self.lease.as_ref().unwrap().epoch()
    }
    fn key(&self) -> &cortexkit_lease::LeaseKey {
        self.lease.as_ref().unwrap().key()
    }
}
impl Drop for NotifyingLease {
    fn drop(&mut self) {
        drop(self.lease.take());
        self.released.notify_one();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_run_keeps_lease_until_blocked_append_and_final_sync_finish() {
    use crate::{lease_key_for, LeaseStore};
    let root = broca_test_support::scratch_root()
        .join(format!("broca-cancelled-writer-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let leases = Arc::new(crate::FileLeaseStore::new(root.join("lease")));
    let session = BindIdentity::new(root.join("project"), "h", "s");
    let key = lease_key_for(&session);
    let state = Arc::new(IoState::default());
    let cancelling = Arc::new(Notify::new());
    let owner = {
        let leases = leases.clone();
        let state = state.clone();
        tokio::spawn(ObserveCancellation {
            cancelling: cancelling.clone(),
            future: Box::pin(async move {
                let req = crate::RunRequest::from_submission(
                    serde_json::from_value(serde_json::json!({
                        "model": {"provider_module_id": "stub", "model_id": "m"},
                        "input": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]
                    }))
                    .unwrap(),
                    broca_protocol::RunId("r".into()),
                    session,
                    broca_protocol::SubmissionId("s".into()),
                );
                let (out, _events) = tokio::sync::mpsc::channel(64);
                crate::run_loop(
                    req,
                    &PendingProvider,
                    &crate::StubToolPlane::new(),
                    leases.as_ref(),
                    &crate::AllowAll,
                    &BlockedStore(state),
                    &out,
                )
                .await
            }),
        })
    };
    state.append.wait().await;
    owner.abort();
    tokio::time::timeout(std::time::Duration::from_secs(10), cancelling.notified())
        .await
        .unwrap();
    let append_held = matches!(leases.acquire(&key), Err(crate::LeaseError::Held { .. }));
    state.append.release();
    let reached_final_sync = tokio::select! {
        _ = state.sync.wait() => true,
        _ = state.exited.notified() => false,
    };
    let sync_held = matches!(leases.acquire(&key), Err(crate::LeaseError::Held { .. }));
    state.sync.release();
    let ended = tokio::time::timeout(std::time::Duration::from_secs(10), owner)
        .await
        .unwrap();
    assert!(ended.unwrap_err().is_cancelled());
    assert!(
        append_held,
        "cancelled run released its lease during a blocked append"
    );
    assert!(
        reached_final_sync,
        "dropping the owner must perform the final sync"
    );
    assert!(
        sync_held,
        "cancelled run released its lease before the final sync finished"
    );
    assert!(state.synced.load(Ordering::Acquire));
    let replacement = leases
        .acquire(&key)
        .expect("writer exited before lease release");
    drop(replacement);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn cancelled_shutdown_keeps_lease_until_writer_exit() {
    use crate::{lease_key_for, LeaseStore};
    let root = broca_test_support::scratch_root()
        .join(format!("broca-cancelled-shutdown-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let leases = crate::FileLeaseStore::new(root.join("lease"));
    let key = lease_key_for(&BindIdentity::new(root.join("project"), "h", "s"));
    let state = Arc::new(IoState::default());
    let released = Arc::new(Notify::new());
    let writer = WalHandle::spawn_with_lease(
        Box::new(BlockedWal {
            seq: 0,
            state: state.clone(),
            step_dirty: false,
        }),
        Box::new(NotifyingLease {
            lease: Some(leases.acquire(&key).unwrap()),
            released: released.clone(),
        }),
    )
    .unwrap();
    writer
        .record(WalRecord::StepStarted { step_id: StepId(1) })
        .into_result()
        .unwrap();
    state.append.wait().await;
    let owner = tokio::spawn(writer.shutdown());
    state.append.release();
    state.sync.wait().await;
    owner.abort();
    assert!(owner.await.unwrap_err().is_cancelled());
    let held = matches!(leases.acquire(&key), Err(crate::LeaseError::Held { .. }));
    state.sync.release();
    state.exited.notified().await;
    released.notified().await;
    assert!(held, "cancelling shutdown released a live writer's lease");
    assert!(state.synced.load(Ordering::Acquire));
    drop(
        leases
            .acquire(&key)
            .expect("writer exit releases its lease"),
    );
    std::fs::remove_dir_all(root).unwrap();
}
