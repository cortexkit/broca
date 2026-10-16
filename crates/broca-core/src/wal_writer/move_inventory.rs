//! Derived, process-local identity inventory for the move participant.
use super::*;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

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
    /// missing refs are unknown. Cancellation leaves that incomplete state intact.
    pub fn start_move_inventory(self: &Arc<Self>, cancel: CancellationToken) {
        let mut state = self.move_inventory.state.lock().unwrap();
        if state.started {
            return;
        }
        state.started = true;
        drop(state);
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let start = std::time::Instant::now();
            let mut inventory = MoveInventory::default();
            match store.address_inventory(true, &|| cancel.is_cancelled()) {
                Ok((addresses, unreadable)) => {
                    inventory.unreadable = unreadable;
                    for address in addresses {
                        if cancel.is_cancelled() {
                            return;
                        }
                        // RunStarted can occur after arbitrary run records, including
                        // under another triple. A first-record shortcut misses identities.
                        match store.read_address_records_inner(&address, true) {
                            Ok(records) => {
                                for record in records {
                                    if let WalRecord::RunStarted { session, .. } = record.record {
                                        insert(&mut inventory, &address, &session);
                                    }
                                }
                            }
                            Err(error) => {
                                tracing::error!(address, %error, "move inventory cannot read WAL");
                                inventory.unreadable.push(address);
                            }
                        }
                    }
                }
                Err(error) => inventory.unreadable.push(error.to_string()),
            }
            if cancel.is_cancelled() {
                return;
            }
            let mut state = store.move_inventory.state.lock().unwrap();
            // Durable appends and installations may have published while the scan
            // ran. Merge them last so no admission can be lost at publication.
            inventory.sessions.append(&mut state.inventory.sessions);
            inventory.unreadable.append(&mut state.inventory.unreadable);
            tracing::info!(
                refs = inventory.sessions.len(),
                unreadable = inventory.unreadable.len(),
                elapsed_ms = start.elapsed().as_millis() as u64,
                "move identity inventory built"
            );
            state.inventory = inventory;
            state.finished = true;
            drop(state);
            store.move_inventory.ready.notify_waiters();
        });
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
}

pub(super) struct IndexedWal {
    wal: FileWal,
    address: String,
    inventory: Arc<InventoryState>,
    pending: Mutex<Vec<BindIdentity>>,
}

impl IndexedWal {
    pub(super) fn new(wal: FileWal, address: String, inventory: Arc<InventoryState>) -> Self {
        Self {
            wal,
            address,
            inventory,
            pending: Mutex::default(),
        }
    }

    fn appended(&self, record: &WalRecord, sync: bool) {
        if let WalRecord::RunStarted { session, .. } = record {
            self.pending.lock().unwrap().push(session.clone());
        }
        if sync {
            self.publish();
        }
    }

    fn publish(&self) {
        let mut pending = self.pending.lock().unwrap();
        if pending.is_empty() {
            return;
        }
        let mut state = self.inventory.state.lock().unwrap();
        for session in pending.drain(..) {
            insert(&mut state.inventory, &self.address, &session);
        }
    }

    fn uncertain_write(&self, record: Option<&WalRecord>) {
        if matches!(record, Some(WalRecord::RunStarted { .. }))
            || !self.pending.lock().unwrap().is_empty()
        {
            self.inventory
                .state
                .lock()
                .unwrap()
                .inventory
                .unreadable
                .push(format!(
                    "{}: identity durability could not be confirmed",
                    self.address
                ));
        }
    }
}

impl Wal for IndexedWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        let seq = self
            .wal
            .append(fence, record, sync)
            .inspect_err(|_| self.uncertain_write(Some(record)))?;
        self.appended(record, sync);
        Ok(seq)
    }
    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        let seq = self
            .wal
            .append_preencoded(fence, record, sync, frame)
            .inspect_err(|_| self.uncertain_write(Some(record)))?;
        self.appended(record, sync);
        Ok(seq)
    }
    fn sync(&self) -> Result<(), WalError> {
        self.wal
            .sync()
            .inspect_err(|_| self.uncertain_write(None))?;
        self.publish();
        Ok(())
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
