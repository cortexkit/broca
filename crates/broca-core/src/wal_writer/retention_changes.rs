//! Content-free, bounded hints for retention work, never evidence of WAL absence.
use super::*;
use std::collections::HashSet;

const CAPACITY: usize = 4096;

/// Raw maintenance writers and actor writers share this queue. It performs no I/O;
/// overflow requests a recovery walk, and process loss is covered by boot recovery.
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
    pub retained: bool,
}

impl ObservedWal {
    fn observe(&mut self, record: &WalRecord) {
        if let WalRecord::RunStarted {
            retention: Some(_), ..
        } = record
        {
            self.retained = true;
        }
        if self.retained {
            self.changes.changed(&self.address);
        }
    }
}

impl Wal for ObservedWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        let result = self.inner.append(fence, record, sync);
        self.observe(record);
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
        self.observe(record);
        result
    }

    fn sync(&self) -> Result<(), WalError> {
        self.inner.sync()
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
