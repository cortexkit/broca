use broca_core::WalStore;
use broca_protocol::BindIdentity;
use broca_wal::{FramedRecord, Wal, WalError, WalRecord};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A logic-test store: barriers publish records without involving disk latency.
/// Each session has its own buffer, and readers see only the last completed sync.
#[derive(Default)]
pub(super) struct MemoryWalStore(Mutex<HashMap<BindIdentity, MemoryWal>>);

#[derive(Clone, Default)]
struct MemoryWal(Arc<Mutex<Records>>);

#[derive(Default)]
struct Records {
    pending: Vec<FramedRecord>,
    synced: Vec<FramedRecord>,
}

impl Wal for MemoryWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        let seq = {
            let mut records = self.0.lock().unwrap();
            let seq = records.pending.len() as u64 + 1;
            records.pending.push(FramedRecord {
                seq,
                fence,
                record: record.clone(),
            });
            seq
        };
        if sync {
            self.sync()?;
        }
        Ok(seq)
    }

    fn sync(&self) -> Result<(), WalError> {
        let mut records = self.0.lock().unwrap();
        records.synced = records.pending.clone();
        Ok(())
    }

    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self.0.lock().unwrap().synced.clone())
    }

    fn tail_after_seq(&self, after: u64) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self
            .replay()?
            .into_iter()
            .filter(|frame| frame.seq > after)
            .collect())
    }

    fn last_seq(&self) -> u64 {
        self.0.lock().unwrap().pending.len() as u64
    }
}

impl WalStore for MemoryWalStore {
    fn open(&self, key: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(Box::new(
            self.0
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_default()
                .clone(),
        ))
    }

    fn read_records(&self, key: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        match self.0.lock().unwrap().get(key) {
            Some(wal) => wal.replay(),
            None => Ok(Vec::new()),
        }
    }
}

/// Keep the file store's complete/absent range classification, but skip fsync:
/// these tests check record ordering rather than survival across a crash.
pub(super) struct NoFsyncWalStore {
    inner: broca_core::FileWalStore,
    refuse_sync: Arc<std::sync::atomic::AtomicBool>,
}

impl NoFsyncWalStore {
    pub(super) fn new(inner: broca_core::FileWalStore) -> Self {
        Self {
            inner,
            refuse_sync: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub(super) fn refuse_physical_sync(&self) {
        self.refuse_sync
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

struct NoFsyncWal(Box<dyn Wal + Send>);

// Arm this fault after setup. If NoFsyncWal forwards a sync to its underlying WAL,
// fail explicitly instead of depending on the runner's disk speed.
struct RefusePhysicalSync(Box<dyn Wal + Send>, Arc<std::sync::atomic::AtomicBool>);

impl Wal for RefusePhysicalSync {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        if sync {
            self.sync()?;
        }
        self.0.append(fence, record, false)
    }
    fn sync(&self) -> Result<(), WalError> {
        if self.1.load(std::sync::atomic::Ordering::SeqCst) {
            Err(WalError::Io(std::io::Error::other(
                "physical fsync is unavailable in this logic-test store",
            )))
        } else {
            self.0.sync()
        }
    }
    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        self.0.replay()
    }
    fn tail_after_seq(&self, after: u64) -> Result<Vec<FramedRecord>, WalError> {
        self.0.tail_after_seq(after)
    }
    fn last_seq(&self) -> u64 {
        self.0.last_seq()
    }
    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.0.lineage_id()
    }
}

impl Wal for NoFsyncWal {
    fn append(&mut self, fence: u64, record: &WalRecord, _: bool) -> Result<u64, WalError> {
        self.0.append(fence, record, false)
    }

    fn sync(&self) -> Result<(), WalError> {
        Ok(())
    }
    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        self.0.replay()
    }
    fn tail_after_seq(&self, after: u64) -> Result<Vec<FramedRecord>, WalError> {
        self.0.tail_after_seq(after)
    }
    fn last_seq(&self) -> u64 {
        self.0.last_seq()
    }
    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.0.lineage_id()
    }
}

impl WalStore for NoFsyncWalStore {
    fn open(&self, key: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(Box::new(NoFsyncWal(Box::new(RefusePhysicalSync(
            self.inner.open(key)?,
            self.refuse_sync.clone(),
        )))))
    }

    fn read_records(&self, key: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        self.inner.read_records(key)
    }

    fn read_records_range(
        &self,
        key: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, WalError> {
        self.inner.read_records_range(key, after, through, anchor)
    }
}
