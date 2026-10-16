//! Derived, process-local identity inventory for the move participant.
use super::*;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

mod persistent;
#[cfg(test)]
mod tests;
pub use persistent::MoveInventoryBuildStats;

#[derive(Default)]
pub struct MoveInventory {
    pub sessions: BTreeMap<String, (String, BindIdentity)>,
    pub unreadable: Vec<String>,
}

#[derive(Default)]
struct State {
    started: bool,
    finished: bool,
    inventory: MoveInventory,
    stats: MoveInventoryBuildStats,
}

#[derive(Default)]
pub(super) struct InventoryState {
    state: Mutex<State>,
    ready: tokio::sync::Notify,
}

fn insert(inventory: &mut MoveInventory, address: &str, session: &BindIdentity) {
    // Re-keyed logs retain historical triples, but those triples do not prove
    // ownership at this address. Match the participant's original discovery rule.
    if session_addr(session) == address {
        inventory.sessions.insert(
            cortexkit_role_llm_runner::session_ref(
                &session.harness,
                &session.project_root.to_string_lossy(),
                &session.session,
            ),
            (address.to_owned(), session.clone()),
        );
    }
}

impl FileWalStore {
    /// Start once at boot, never from reference resolution. Until publication,
    /// missing refs are unknown. Interrupted builds finish explicitly incomplete.
    pub fn start_move_inventory(self: &Arc<Self>, cancel: CancellationToken) {
        let mut state = self.move_inventory.state.lock().unwrap();
        if state.started {
            return;
        }
        state.started = true;
        drop(state);
        let store = self.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("move-inventory".into())
            .spawn(move || store.run_inventory_build(|| persistent::build(&store, &cancel)))
        {
            let mut state = self.move_inventory.state.lock().unwrap();
            state
                .inventory
                .unreadable
                .push(format!("cannot start move inventory thread: {error}"));
            state.finished = true;
            drop(state);
            self.move_inventory.ready.notify_waiters();
        }
    }

    fn run_inventory_build(&self, build: impl FnOnce()) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(build));
        let mut state = self
            .move_inventory
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if result.is_err() || !state.finished {
            let reason = if result.is_err() {
                "move inventory build panicked"
            } else {
                "move inventory build cancelled before completion"
            };
            tracing::error!(reason, "move inventory finished incomplete");
            state.inventory.unreadable.push(reason.into());
            state.finished = true;
        }
        self.move_inventory.state.clear_poison();
        drop(state);
        self.move_inventory.ready.notify_waiters();
    }

    /// Wait for the boot scan, including a scan that finished with unreadable
    /// locations. Callers still must check `MoveInventory::unreadable`.
    pub async fn wait_move_inventory(&self) {
        loop {
            let ready = self.move_inventory.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if self.move_inventory.state.lock().unwrap().finished {
                return;
            }
            ready.await;
        }
    }

    /// Copy only the requested entries. No directory listing or WAL read occurs.
    pub fn move_session_refs<'a>(&self, refs: impl Iterator<Item = &'a str>) -> MoveInventory {
        let state = self.move_inventory.state.lock().unwrap();
        let mut inventory = MoveInventory {
            sessions: refs
                .filter_map(|reference| {
                    state
                        .inventory
                        .sessions
                        .get(reference)
                        .map(|entry| (reference.to_owned(), entry.clone()))
                })
                .collect(),
            unreadable: state.inventory.unreadable.clone(),
        };
        if !state.finished {
            inventory
                .unreadable
                .push("move identity inventory is not ready".into());
        }
        inventory
    }

    /// Installed WALs bypass the ordinary writer. Publish their durable records
    /// before releasing move exclusion, including every eligible RunStarted triple.
    pub fn publish_move_records(&self, address: &str, records: &[broca_wal::FramedRecord]) {
        let mut state = self.move_inventory.state.lock().unwrap();
        for frame in records {
            if let WalRecord::RunStarted { session, .. } = &frame.record {
                insert(&mut state.inventory, address, session);
            }
        }
    }

    /// A failed out-of-writer installation may have published bytes without its
    /// identities. Keep absence unknown until the next process boot inventory.
    pub fn invalidate_move_inventory(&self, cause: String) {
        self.move_inventory
            .state
            .lock()
            .unwrap()
            .inventory
            .unreadable
            .push(cause);
    }

    /// Cumulative participant WAL snapshots/identity reads, for cost diagnostics.
    pub fn move_read_count(&self) -> u64 {
        self.move_reads.load(Ordering::Relaxed)
    }

    pub fn move_inventory_build_stats(&self) -> MoveInventoryBuildStats {
        self.move_inventory.state.lock().unwrap().stats
    }

    /// Process-local build progress, not an assertion of WAL durability.
    pub fn move_inventory_health(&self) -> (bool, usize) {
        let state = self
            .move_inventory
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        (state.finished, state.inventory.unreadable.len())
    }
}

pub(super) struct IndexedWal {
    wal: FileWal,
    address: String,
    inventory: Arc<InventoryState>,
}

impl IndexedWal {
    pub(super) fn new(wal: FileWal, address: String, inventory: Arc<InventoryState>) -> Self {
        Self {
            wal,
            address,
            inventory,
        }
    }

    fn publishing(&self, record: &WalRecord) {
        // This is a conservative candidate set, not a durability claim: requests
        // verify the actual bytes. Publish before writing so an unsynced drop or
        // failed append cannot hide an identity that may have reached disk.
        if let WalRecord::RunStarted { session, .. } = record {
            let mut state = self.inventory.state.lock().unwrap();
            insert(&mut state.inventory, &self.address, session);
        }
    }
}

impl Wal for IndexedWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        self.publishing(record);
        self.wal.append(fence, record, sync)
    }
    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        self.publishing(record);
        self.wal.append_preencoded(fence, record, sync, frame)
    }
    fn sync(&self) -> Result<(), WalError> {
        self.wal.sync()
    }
    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        self.wal.replay()
    }
    fn tail_after_seq(&self, seq: u64) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        self.wal.tail_after_seq(seq)
    }
    fn last_seq(&self) -> u64 {
        self.wal.last_seq()
    }
    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.wal.lineage_id()
    }
    fn append_budget(&self) -> Option<(u64, u64, u64)> {
        self.wal.append_budget()
    }
    fn recovery_byte_cap(&self) -> Option<u64> {
        self.wal.recovery_byte_cap()
    }
    fn append_budget_scope(&self) -> broca_wal::size_limit::SizeScope {
        self.wal.append_budget_scope()
    }
}
