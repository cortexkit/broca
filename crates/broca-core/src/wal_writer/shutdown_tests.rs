use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize};

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

/// Records, in order, when a writer's WAL and its lease are dropped.
type DropLog = Arc<Mutex<Vec<&'static str>>>;

struct OrderWal {
    log: DropLog,
    panic_in_preparation: bool,
}

impl Drop for OrderWal {
    fn drop(&mut self) {
        self.log.lock().unwrap().push("wal");
    }
}

impl Wal for OrderWal {
    fn append(&mut self, _: u64, _: &WalRecord, _: bool) -> Result<u64, WalError> {
        Ok(1)
    }
    fn sync(&self) -> Result<(), WalError> {
        Ok(())
    }
    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        Ok(vec![])
    }
    fn tail_after_seq(&self, _: u64) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        Ok(vec![])
    }
    fn last_seq(&self) -> u64 {
        0
    }
    fn append_budget(&self) -> Option<(u64, u64, u64)> {
        // Read while the writer handle is prepared, after recovery is proven and before
        // the writer thread exists.
        if self.panic_in_preparation {
            panic!("injected panic while preparing the writer handle");
        }
        None
    }
}

#[derive(Debug)]
struct OrderLease {
    key: cortexkit_lease::LeaseKey,
    log: DropLog,
}

impl LeaseHandle for OrderLease {
    fn epoch(&self) -> u64 {
        1
    }
    fn key(&self) -> &cortexkit_lease::LeaseKey {
        &self.key
    }
}

impl Drop for OrderLease {
    fn drop(&mut self) {
        self.log.lock().unwrap().push("lease");
    }
}

fn order_parts(panic_in_preparation: bool) -> (DropLog, Box<OrderWal>, Box<OrderLease>) {
    let log = DropLog::default();
    let wal = Box::new(OrderWal {
        log: log.clone(),
        panic_in_preparation,
    });
    let lease = Box::new(OrderLease {
        key: cortexkit_lease::LeaseKey::new("broca", "wal", "order-test"),
        log: log.clone(),
    });
    (log, wal, lease)
}

#[test]
fn writer_preparation_panic_closes_the_wal_before_the_lease() {
    let (log, wal, lease) = order_parts(true);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        WalHandle::spawn_with_lease(wal, lease)
    }));
    assert!(outcome.is_err(), "the injected preparation panic must fire");
    assert_eq!(*log.lock().unwrap(), vec!["wal", "lease"]);
}

#[test]
fn writer_thread_spawn_failure_closes_the_wal_before_the_lease() {
    let (log, wal, lease) = order_parts(false);
    FAIL_NEXT_WRITER_SPAWN.with(|fail| fail.set(true));
    let error = match WalHandle::spawn_with_lease(wal, lease) {
        Ok(_) => panic!("the injected spawn failure must refuse the writer"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("spawn failed"), "{error}");
    assert_eq!(*log.lock().unwrap(), vec!["wal", "lease"]);
}
