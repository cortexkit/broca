//! Durable session discovery boundary; WAL bytes remain authoritative.
use super::*;

fn record_candidate(
    index: Option<&dyn SessionRefIndex>,
    root: &Path,
    address: &str,
    session: &BindIdentity,
) -> Result<Option<ProjectionAdmission>, String> {
    if let Some(index) = index {
        if let Err(error) = index.record_session(address, session) {
            let admission = ProjectionAdmission::begin(root)
                .map_err(|marker| format!("session-ref projection failed ({error}); cannot persist incomplete marker: {marker}"))?;
            tracing::warn!(%error, "session-ref projection unavailable; durable incomplete marker preserves WAL admission");
            return Ok(Some(admission));
        }
    }
    Ok(None)
}

pub fn agent_session_ref(session: &BindIdentity) -> String {
    cortexkit_role_llm_runner::session_ref(&cortexkit_role_llm_runner::SessionIdentity {
        runner: "broca",
        harness: &session.harness,
        session: &session.session,
    })
}

/// Folder-based reference retained for old handles and durable move fences.
/// Keep this derivation independent of the external reference's version.
pub fn legacy_session_ref(harness: &str, project_root: &str, session: &str) -> String {
    use sha2::{Digest, Sha256};
    let bytes = format!(
        r#"{{"harness":{},"project_root":{},"session":{}}}"#,
        serde_json::to_string(harness).unwrap(),
        serde_json::to_string(project_root).unwrap(),
        serde_json::to_string(session).unwrap(),
    );
    Sha256::digest(bytes.as_bytes())[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Attempt discovery persistence before WAL durability; a durable gap marker
/// preserves availability when SQLite cannot accept the conservative candidate.
pub trait SessionRefIndex: Send + Sync {
    fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String>;
}

impl FileWalStore {
    pub fn set_session_ref_index(&self, index: Arc<dyn SessionRefIndex>) {
        *self.session_index.lock().unwrap() = Some(index);
    }

    /// Keep a gap guard alive through installation if SQLite was unavailable.
    pub fn record_session_before_install(
        &self,
        address: &str,
        session: &BindIdentity,
    ) -> Result<Option<ProjectionAdmission>, String> {
        let index = self.session_index.lock().unwrap().clone();
        record_candidate(
            index.as_deref(),
            self.root.parent().unwrap_or(&self.root),
            address,
            session,
        )
    }

    pub fn move_read_count(&self) -> u64 {
        self.move_reads.load(Ordering::Relaxed)
    }
}

pub(super) struct AdmissionWal {
    wal: FileWal,
    address: String,
    state_root: PathBuf,
    index: Arc<Mutex<Option<Arc<dyn SessionRefIndex>>>>,
    placement: Option<(PathBuf, PathBuf, PathBuf, Vec<String>)>,
    /// The session's WAL path, whose `.moved`/`.moving` marker files and pending cut files
    /// are checked before every append, with the session's v1 and v2 references: a pending
    /// cut that names either reference refuses the append.
    legacy_fence: (PathBuf, [String; 2]),
}

impl AdmissionWal {
    pub(super) fn new(
        wal: FileWal,
        address: String,
        state_root: PathBuf,
        index: Arc<Mutex<Option<Arc<dyn SessionRefIndex>>>>,
        legacy_fence: (PathBuf, [String; 2]),
    ) -> Self {
        Self {
            wal,
            address,
            state_root,
            index,
            placement: None,
            legacy_fence,
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

    fn before_append(
        &mut self,
        record: &WalRecord,
    ) -> Result<Option<ProjectionAdmission>, WalError> {
        let [legacy, current] = &self.legacy_fence.1;
        broca_wal::migration::guard_refs(&self.legacy_fence.0, &[legacy, current], true)?;
        let mut admission = None;
        if let WalRecord::RunStarted {
            session,
            retention,
            session_class,
            ..
        } = record
        {
            let index = self.index.lock().unwrap().clone();
            admission =
                record_candidate(index.as_deref(), &self.state_root, &self.address, session)
                    .map_err(|error| WalError::Io(std::io::Error::other(error)))?;
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
                    self.legacy_fence.0 = path.clone();
                }
            }
        }
        Ok(admission)
    }
}

impl Wal for AdmissionWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        let _projection_admission = self.before_append(record)?;
        self.wal.append(fence, record, sync)
    }
    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        let _projection_admission = self.before_append(record)?;
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
