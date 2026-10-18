//! Durable session discovery boundary; WAL bytes remain authoritative.
use super::*;

pub fn agent_session_ref(session: &BindIdentity) -> String {
    cortexkit_role_llm_runner::session_ref(
        &session.harness,
        &session.project_root.to_string_lossy(),
        &session.session,
    )
}

/// A candidate is persisted before its WAL can become durable. Failed writes
/// may leave extra candidates, which readers must verify against the actual WAL.
pub trait SessionRefIndex: Send + Sync {
    fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String>;
}

impl FileWalStore {
    pub fn set_session_ref_index(&self, index: Arc<dyn SessionRefIndex>) {
        *self.session_index.lock().unwrap() = Some(index);
    }

    /// Installation persists candidates before publishing any incoming WAL bytes.
    pub fn record_session_before_install(
        &self,
        address: &str,
        session: &BindIdentity,
    ) -> Result<(), String> {
        if let Some(index) = self.session_index.lock().unwrap().as_ref() {
            index.record_session(address, session)?;
        }
        Ok(())
    }

    pub fn move_read_count(&self) -> u64 {
        self.move_reads.load(Ordering::Relaxed)
    }
}

pub(super) struct AdmissionWal {
    wal: FileWal,
    address: String,
    index: Arc<Mutex<Option<Arc<dyn SessionRefIndex>>>>,
    placement: Option<(PathBuf, PathBuf, PathBuf, Vec<String>)>,
}

impl AdmissionWal {
    pub(super) fn new(
        wal: FileWal,
        address: String,
        index: Arc<Mutex<Option<Arc<dyn SessionRefIndex>>>>,
    ) -> Self {
        Self {
            wal,
            address,
            index,
            placement: None,
        }
    }

    pub(super) fn with_placement(
        mut self,
        ordinary: PathBuf,
        ephemeral: PathBuf,
        durable: PathBuf,
        classes: Vec<String>,
    ) -> Self {
        self.placement = Some((ordinary, ephemeral, durable, classes));
        self
    }

    fn before_append(&mut self, record: &WalRecord) -> Result<(), WalError> {
        if let WalRecord::RunStarted {
            session,
            retention,
            session_class,
            ..
        } = record
        {
            // Persist before append, not after sync: a crash between WAL durability
            // and a projection write must not turn a held session into an absent ref.
            if let Some(index) = self.index.lock().unwrap().as_ref() {
                index
                    .record_session(&self.address, session)
                    .map_err(|error| WalError::Io(std::io::Error::other(error)))?;
            }
            if self.wal.last_seq() == 0 {
                if let Some((ordinary, ephemeral, durable, classes)) = &self.placement {
                    let path = if retention.is_some() {
                        ephemeral
                    } else if session_class
                        .as_ref()
                        .is_some_and(|class| classes.contains(class))
                    {
                        durable
                    } else {
                        ordinary
                    };
                    self.wal.relocate_unstarted(path)?;
                }
            }
        }
        Ok(())
    }
}

impl Wal for AdmissionWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        self.before_append(record)?;
        self.wal.append(fence, record, sync)
    }
    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        self.before_append(record)?;
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
