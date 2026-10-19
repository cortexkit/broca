use super::*;
use crate::registry::{LineageOpener, LineageRegistry, OpenError, SealFailureStage};
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
struct CloseNotice(Notify, AtomicBool, Notify);
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
        if self.1.load(Ordering::Acquire) {
            self.2.notified().await;
        }
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
            .map_err(|error| OpenError::Open(error.to_string()))?,
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

struct Fixture {
    root: std::path::PathBuf,
    key: BindIdentity,
    opener: Arc<ShutdownOpener>,
    registry: LineageRegistry,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-actor-final-sync-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("project")).unwrap();
        let key = BindIdentity::new(root.join("project"), "h", "s");
        let opener = Arc::new(ShutdownOpener {
            leases: FileLeaseStore::new(root.join("leases")),
            wal: Arc::new(FileWalStore::new(root.join("wal"))),
            fail: Arc::new(AtomicBool::new(false)),
            writer: Mutex::new(Weak::new()),
            executor: Arc::new(CloseNotice::default()),
        });
        let registry = LineageRegistry::new(opener.clone());
        Self {
            root,
            key,
            opener,
            registry,
        }
    }

    async fn open_dirty(&self, key: &BindIdentity) {
        self.registry.actor_for(key).await.unwrap();
        assert_eq!(
            self.registry.route_status(key).await.unwrap(),
            LineageStatus::Idle
        );
        // Startup recovery has succeeded. Fail only the next durability boundary.
        let writer = self.opener.writer.lock().unwrap().upgrade().unwrap();
        writer
            .record(WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            })
            .into_result()
            .unwrap();
    }

    async fn fail_idle_close(&self, key: &BindIdentity) {
        self.open_dirty(key).await;
        self.opener.fail.store(true, Ordering::Release);
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::time::resume();
        broca_test_support::event_timeout(
            "idle close releases slot",
            async {
                self.opener.executor.0.notified().await;
                while self.registry.slot_counts().closing != 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || {
                format!(
                    "closing={}, open={}",
                    self.registry.slot_counts().closing,
                    self.registry.slot_counts().open
                )
            },
        )
        .await
        .expect("idle close releases the slot");
        assert_eq!(self.registry.slot_counts().open, 0);
        drop(
            self.opener
                .leases
                .acquire(&broca_core::lease_key_for(key))
                .expect("failed shutdown still joined and released the writer lease"),
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn actor_reports_final_writer_shutdown_failure() {
    let f = Fixture::new("idle-debt");
    f.fail_idle_close(&f.key).await;
    let failures = f.registry.seal_failures();
    assert_eq!(
        failures.len(),
        1,
        "idle close failure remains a historical event"
    );
    assert_eq!(failures[0].session, f.key.session);
    assert_eq!(failures[0].stage, SealFailureStage::WriterShutdown);
    assert!(failures[0].error.contains("actor final fsync failed"));
    assert!(failures[0].timestamp <= std::time::SystemTime::now());
    assert_eq!(f.registry.recovery_debt().len(), 1);

    // The earlier idle close failed, but no slot remains for this stop to seal. Its
    // success must not erase the unrecovered session or claim the older WAL is safe.
    assert!(
        f.registry.stop_and_seal().await,
        "an empty current stop is clean"
    );
    let report = f.registry.shutdown_report().unwrap();
    assert!(report.complete);
    assert!(report.current_failures.is_empty());
    assert_eq!(report.recovery_debt.len(), 1);
    assert_eq!(report.recovery_debt[0].session, f.key.session);
    assert_eq!(report.lifetime_failures, 1);
    assert!(report.to_string().contains("recovery_debt_sessions=1"));
    assert!(report.to_string().contains("actor final fsync failed"));
}

#[tokio::test]
async fn recovered_idle_writer_failure_does_not_taint_later_stop() {
    let f = Fixture::new("recovered");
    f.fail_idle_close(&f.key).await;
    let failed_at = f.registry.seal_failures()[0].timestamp;
    f.opener.fail.store(false, Ordering::Release);
    f.registry.actor_for(&f.key).await.unwrap();
    assert_eq!(
        f.registry.route_status(&f.key).await.unwrap(),
        LineageStatus::Idle
    );
    assert!(
        f.registry.recovery_debt().is_empty(),
        "replay plus sync discharges this session"
    );
    assert!(
        f.registry.stop_and_seal().await,
        "recovered idle failure must not taint a later stop"
    );
    let report = f.registry.shutdown_report().unwrap();
    assert!(report.complete);
    assert!(report.current_failures.is_empty());
    assert!(report.recovery_debt.is_empty());
    assert_eq!(report.lifetime_failures, 1);
    let earlier = report.latest_failure.unwrap();
    assert_eq!(earlier.timestamp, failed_at);
    assert_eq!(earlier.session, f.key.session);
    assert!(earlier.error.contains("actor final fsync failed"));
    assert_eq!(f.registry.seal_failures().len(), 1);
}

#[tokio::test]
async fn writer_failure_during_stop_is_incomplete() {
    let f = Fixture::new("during-stop");
    f.open_dirty(&f.key).await;
    f.opener.fail.store(true, Ordering::Release);
    assert!(
        !f.registry.stop_and_seal().await,
        "a fresh final-sync failure still fails the stop"
    );
    let report = f.registry.shutdown_report().unwrap();
    assert!(!report.complete);
    assert_eq!(report.cause, "fresh final-sync or writer shutdown failure");
    assert!(report.remaining_sessions.is_empty());
    assert!(report
        .current_failures
        .iter()
        .any(|event| event.stage == SealFailureStage::FinalSync));
    assert_eq!(report.recovery_debt.len(), 1);
    assert!(report.to_string().contains("actor final fsync failed"));
    drop(
        f.opener
            .leases
            .acquire(&broca_core::lease_key_for(&f.key))
            .expect("fresh failure still joins before lease release"),
    );
}

#[tokio::test]
async fn recovery_clears_only_the_recovered_session_debt() {
    let f = Fixture::new("session-debt");
    f.fail_idle_close(&f.key).await;
    let other = BindIdentity::new(f.root.join("project"), "h", "other");
    f.opener.fail.store(false, Ordering::Release);
    f.fail_idle_close(&other).await;
    f.opener.fail.store(false, Ordering::Release);
    f.registry.actor_for(&f.key).await.unwrap();
    assert_eq!(
        f.registry.route_status(&f.key).await.unwrap(),
        LineageStatus::Idle
    );
    assert!(f.registry.stop_and_seal().await);
    let report = f.registry.shutdown_report().unwrap();
    assert_eq!(report.lifetime_failures, 2);
    assert_eq!(report.recovery_debt.len(), 1);
    assert_eq!(report.recovery_debt[0].session, "other");
}

#[tokio::test]
async fn failed_recovery_does_not_clear_session_debt() {
    let f = Fixture::new("failed-recovery");
    f.fail_idle_close(&f.key).await;
    // The opener's recovery sync fails while armed, so no replacement slot is published.
    let error = f
        .registry
        .actor_for(&f.key)
        .await
        .err()
        .expect("recovery fsync fails the open");
    assert!(error.to_string().contains("actor final fsync failed"));
    assert_eq!(f.registry.recovery_debt().len(), 1);
    assert!(f.registry.stop_and_seal().await);
    assert_eq!(f.registry.shutdown_report().unwrap().recovery_debt.len(), 1);
}

#[tokio::test]
async fn failing_slot_still_closing_at_stop_is_incomplete() {
    let f = Fixture::new("closing-at-stop");
    f.open_dirty(&f.key).await;
    f.opener.executor.1.store(true, Ordering::Release);
    f.opener.fail.store(true, Ordering::Release);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(10), f.opener.executor.0.notified())
        .await
        .unwrap();
    assert_eq!(f.registry.slot_counts().closing, 1);
    assert_eq!(f.registry.seal_failures().len(), 1);
    // The error predates the stop, but teardown still owns the slot and lease. It is
    // part of this stop's outcome, unlike a fully closed, earlier actor generation.
    let (complete, ()) = tokio::join!(f.registry.stop_and_seal(), async {
        f.opener.executor.2.notify_one();
    });
    assert!(!complete);
    assert_eq!(
        f.registry.shutdown_report().unwrap().current_failures.len(),
        1
    );
}
