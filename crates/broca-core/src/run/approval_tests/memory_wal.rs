use broca_wal::{FramedRecord, Wal, WalError, WalLineageId, WalRecord};
use std::sync::{Arc, Mutex};

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
        // Decode a fixed lineage frame instead of opening a file to mint an identity.
        // The opaque identity's bytes do not matter to this single-lineage fixture.
        let mut frame = [0x5a; 69];
        frame[..4].copy_from_slice(&16u32.to_le_bytes());
        frame[4] = 2;
        frame[5..21].fill(0);
        frame[21..53].copy_from_slice(&[
            0xb8, 0xc1, 0xc2, 0x6f, 0x10, 0xd3, 0xd6, 0x36, 0xff, 0x9d, 0x9d, 0x83, 0x61, 0x97,
            0x15, 0x8a, 0x04, 0x82, 0xd1, 0x39, 0x75, 0xcc, 0x1b, 0x7c, 0x52, 0xa3, 0xf1, 0x43,
            0xac, 0x76, 0x45, 0x80,
        ]);
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
