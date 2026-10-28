//! Content-free, bounded hints for retention work, never evidence of WAL absence.
use super::*;
use std::collections::HashSet;

const CAPACITY: usize = 4096;

/// Writers queue a retained session's address here when they close, and again when the
/// session lease they were opened under is released, so the next sweep can enroll it without
/// a directory walk. It performs no I/O; overflow requests a recovery walk, and process loss
/// is covered by boot recovery.
#[derive(Default)]
pub(super) struct Changes {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    addresses: HashSet<Arc<str>>,
    recovery: bool,
}

impl Changes {
    fn changed(&self, address: &Arc<str>) {
        let mut state = self.state.lock().unwrap();
        if state.recovery || state.addresses.contains(address) {
            return;
        }
        if state.addresses.len() == CAPACITY {
            state.recovery = true;
            return;
        }
        state.addresses.insert(address.clone());
    }

    pub(super) fn take(&self) -> (Vec<String>, bool) {
        let mut state = self.state.lock().unwrap();
        let recovery = std::mem::take(&mut state.recovery);
        let addresses = state.addresses.drain().map(|s| s.to_string()).collect();
        (addresses, recovery)
    }
}

pub(super) struct ObservedWal {
    pub inner: Box<dyn Wal + Send>,
    pub address: Arc<str>,
    pub changes: Arc<Changes>,
    /// Shared with the writer's `ReleaseHintLease`, if any. Set at open when the WAL lives
    /// under the ephemeral root, or when a run start with a retention policy is appended to a
    /// default-placement WAL (that first append places the WAL under the ephemeral root). The
    /// flag is set even if that append fails; the next sweep drops a hint whose WAL is absent.
    pub retained: Arc<AtomicBool>,
    pub session: BindIdentity,
    pub durable_observer: Option<Arc<DurableWalObserver>>,
}

impl Drop for ObservedWal {
    fn drop(&mut self) {
        // Hints are queued only once the writer is gone, so a sweep does not reopen a session
        // that is still being written. A writer opened through `WalStore::open_writer` queues
        // the address again once its lease is released.
        if self.retained.load(Ordering::Acquire) {
            self.changes.changed(&self.address);
        }
    }
}

/// A session lease that queues its retained address after the lease itself is released.
///
/// Writers close their WAL before they release the session lease, and enrollment needs that
/// lease. A sweep that runs between those two steps consumes the address queued when the WAL
/// closed, finds the lease held and drops the address. Queuing it again after the release
/// lets the next sweep enroll the session without a recovery walk.
pub(super) struct ReleaseHintLease {
    /// Always `Some` until `drop`, which releases the lease before queuing the address.
    pub inner: Option<Box<dyn LeaseHandle>>,
    pub address: Arc<str>,
    pub changes: Arc<Changes>,
    /// The flag of the writer opened under this lease, read when the lease is released.
    pub retained: Arc<AtomicBool>,
}

impl std::fmt::Debug for ReleaseHintLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReleaseHintLease")
            .field("inner", &self.inner)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl LeaseHandle for ReleaseHintLease {
    fn epoch(&self) -> u64 {
        self.inner.as_ref().expect("lease held until drop").epoch()
    }
    fn key(&self) -> &cortexkit_lease::LeaseKey {
        self.inner.as_ref().expect("lease held until drop").key()
    }
}

impl Drop for ReleaseHintLease {
    fn drop(&mut self) {
        // Release first: a sweep that takes the address must be able to acquire the lease.
        drop(self.inner.take());
        if self.retained.load(Ordering::Acquire) {
            self.changes.changed(&self.address);
        }
    }
}

impl ObservedWal {
    fn durable(&self) {
        if let Some(observer) = &self.durable_observer {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observer(
                    &self.session,
                    self.inner.last_seq(),
                    self.inner.lineage_id(),
                );
            }))
            .is_err()
            {
                tracing::warn!(target: "wal", "durable WAL observer panicked; WAL remains committed");
            }
        }
    }

    /// A default-placement WAL becomes retained when its first run carries a retention
    /// policy. Only the flag is set here; the address is queued when the writer closes.
    fn note_retention(&self, record: &WalRecord) {
        if let WalRecord::RunStarted {
            retention: Some(_), ..
        } = record
        {
            self.retained.store(true, Ordering::Release);
        }
    }
}

impl Wal for ObservedWal {
    fn writer_id(&self) -> Option<[u8; 16]> {
        self.inner.writer_id()
    }
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        let result = self.inner.append(fence, record, sync);
        self.note_retention(record);
        if sync && result.is_ok() {
            self.durable();
        }
        result
    }

    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        let result = self.inner.append_preencoded(fence, record, sync, frame);
        self.note_retention(record);
        if sync && result.is_ok() {
            self.durable();
        }
        result
    }
    fn append_v4(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        prepared: broca_wal::PreparedV4Frame,
        appended_ms: u64,
    ) -> Result<u64, WalError> {
        let result = self
            .inner
            .append_v4(fence, record, sync, prepared, appended_ms);
        self.note_retention(record);
        if sync && result.is_ok() {
            self.durable();
        }
        result
    }
    fn wal_config(&self) -> broca_wal::WalConfig {
        self.inner.wal_config()
    }

    fn sync(&self) -> Result<(), WalError> {
        self.inner.sync()?;
        self.durable();
        Ok(())
    }
    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        self.inner.replay()
    }
    fn tail_after_seq(&self, after_seq: u64) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        self.inner.tail_after_seq(after_seq)
    }
    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }
    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.inner.lineage_id()
    }
    fn append_budget(&self) -> Option<(u64, u64, u64)> {
        self.inner.append_budget()
    }
    fn recovery_byte_cap(&self) -> Option<u64> {
        self.inner.recovery_byte_cap()
    }
    fn append_budget_scope(&self) -> broca_wal::size_limit::SizeScope {
        self.inner.append_budget_scope()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BarrierWal {
        synced: Arc<std::sync::atomic::AtomicBool>,
        fail: bool,
    }
    impl Wal for BarrierWal {
        fn append(
            &mut self,
            _fence: u64,
            _record: &WalRecord,
            _sync: bool,
        ) -> Result<u64, WalError> {
            unreachable!()
        }
        fn sync(&self) -> Result<(), WalError> {
            if self.fail {
                return Err(WalError::Io(io::Error::other("injected fsync failure")));
            }
            self.synced.store(true, Ordering::Release);
            Ok(())
        }
        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(vec![])
        }
        fn tail_after_seq(&self, _after: u64) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(vec![])
        }
        fn last_seq(&self) -> u64 {
            7
        }
    }

    #[test]
    fn run_events_observe_only_successful_durable_barriers() {
        let synced = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::new(Mutex::new(Vec::new()));
        for fail in [true, false] {
            let flag = synced.clone();
            let output = observed.clone();
            let wal = ObservedWal {
                inner: Box::new(BarrierWal {
                    synced: synced.clone(),
                    fail,
                }),
                address: Arc::from("address"),
                changes: Arc::default(),
                retained: Arc::default(),
                session: BindIdentity::new("/", "test", "s"),
                durable_observer: Some(Arc::new(move |_, seq, _| {
                    output
                        .lock()
                        .unwrap()
                        .push((seq, flag.load(Ordering::Acquire)))
                })),
            };
            assert_eq!(wal.sync().is_err(), fail);
        }
        assert_eq!(*observed.lock().unwrap(), vec![(7, true)]);
    }

    #[test]
    fn overflow_requests_recovery_without_unbounded_growth() {
        let changes = Changes::default();
        for n in 0..=CAPACITY {
            changes.changed(&Arc::from(format!("{n:016x}")));
        }
        let (addresses, recovery) = changes.take();
        assert_eq!(addresses.len(), CAPACITY);
        assert!(recovery);
        assert_eq!(changes.take(), (Vec::new(), false));
    }

    #[test]
    #[ignore = "explicit writer hot-path microbenchmark"]
    fn retention_change_queue_measurement() {
        let changes = Changes::default();
        let address = Arc::from("0123456789abcdef");
        let start = std::time::Instant::now();
        for _ in 0..1_000_000 {
            changes.changed(std::hint::black_box(&address));
        }
        eprintln!("retention queue: 1000000 hints in {:?}", start.elapsed());
        assert_eq!(changes.take(), (vec![address.to_string()], false));
    }
}
