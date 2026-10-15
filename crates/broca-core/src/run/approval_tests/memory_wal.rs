use broca_wal::{FramedRecord, Wal, WalError, WalLineageId, WalRecord};
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

// Reads see only completed barriers, not buffered appends. This keeps the approval
// rig's durable-frame assertions meaningful without depending on filesystem fsync.
#[derive(Clone)]
pub(super) struct MemoryWal {
    state: Arc<Mutex<State>>,
    lineage: WalLineageId,
}

#[derive(Default)]
struct State {
    pending: Vec<FramedRecord>,
    synced: Vec<FramedRecord>,
    completed_syncs: Vec<u64>,
}

impl MemoryWal {
    #[cfg(test)]
    pub(super) fn new() -> Self {
        // Decode a lineage frame without opening a file. Separate rigs must still
        // have distinct identities when they share an owner-scoped consent feed.
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let identity = NEXT.fetch_add(1, Ordering::SeqCst);
        let mut frame = [0x5a; 69];
        frame[..4].copy_from_slice(&16u32.to_le_bytes());
        frame[4] = 2;
        frame[5..21].fill(0);
        frame[53..61].copy_from_slice(&identity.to_le_bytes());
        let digest = Sha256::digest([&frame[4..21], &frame[53..69]].concat());
        frame[21..53].copy_from_slice(&digest);
        let broca_wal::framing::DecodeStep::Lineage { lineage_id, .. } =
            broca_wal::framing::decode_next(&frame)
        else {
            panic!("fixture must contain a valid lineage frame")
        };
        Self {
            state: Arc::new(Mutex::new(State::default())),
            lineage: lineage_id,
        }
    }

    pub(super) fn completed_syncs(&self) -> Vec<u64> {
        self.state.lock().unwrap().completed_syncs.clone()
    }
}

impl Wal for MemoryWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        let seq = {
            let mut state = self.state.lock().unwrap();
            let seq = state.pending.len() as u64 + 1;
            state.pending.push(FramedRecord {
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
        let mut state = self.state.lock().unwrap();
        state.synced = state.pending.clone();
        let head = state.synced.len() as u64;
        state.completed_syncs.push(head);
        Ok(())
    }

    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self.state.lock().unwrap().synced.clone())
    }

    fn tail_after_seq(&self, after_seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self
            .replay()?
            .into_iter()
            .filter(|frame| frame.seq > after_seq)
            .collect())
    }

    fn last_seq(&self) -> u64 {
        self.state.lock().unwrap().pending.len() as u64
    }

    fn lineage_id(&self) -> Option<WalLineageId> {
        Some(self.lineage)
    }
}

// Store-assisted resume must read the same synced prefix as the approval probes,
// rather than accidentally consulting an empty file store beside the memory WAL.
impl crate::WalStore for MemoryWal {
    fn open(&self, _: &broca_protocol::BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(Box::new(self.clone()))
    }

    fn read_records(
        &self,
        _: &broca_protocol::BindIdentity,
    ) -> Result<Vec<FramedRecord>, WalError> {
        self.replay()
    }
}
