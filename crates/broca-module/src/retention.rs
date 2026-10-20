//! WAL-authoritative retention decisions and lease-scoped deletion.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use broca_core::{Clock, FileWalStore, LeaseHandle, LeaseStore, RunIndexRow, WalStore};
use broca_protocol::BindIdentity;
use broca_store::retention::{RetentionLineageRow, RetentionRunRow, RunRetention};
use broca_store::SqliteStore;
use broca_wal::{FramedRecord, WalRecord};
use serde::{Deserialize, Serialize};

pub const MAX_RETENTION_SECONDS: u64 = 2_592_000;
pub const SWEEP_INTERVAL_MS: u64 = 300_000;

/// Backends store expiry deadlines and run accounting without transcript content.
pub trait RetentionIndex: Send + Sync {
    fn runs(&self, _lineage: &str) -> Result<Vec<RetentionRunRow>, String> {
        Err("retention index cannot read run accounting".into())
    }
    fn live(&self, session: &BindIdentity) -> Result<Option<RetentionLineageRow>, String>;
    fn lineage(&self, id: &str) -> Result<Option<RetentionLineageRow>, String>;
    fn candidates(&self, now_ms: u64) -> Result<Vec<RetentionLineageRow>, String>;
    fn run(&self, id: &str) -> Result<Option<RunRetention>, String>;
    fn upsert(&self, row: &RetentionLineageRow, runs: &[RetentionRunRow]) -> Result<(), String>;
    fn live_inventory(&self) -> Result<Option<Vec<RetentionLineageRow>>, String> {
        Ok(None)
    }
    fn pending_deletions(&self) -> Result<Option<Vec<BindIdentity>>, String> {
        Ok(None)
    }
    fn deletion_pending(
        &self,
        _id: &str,
        _key: &BindIdentity,
        _pending: bool,
    ) -> Result<(), String> {
        Ok(())
    }
    fn retained_sessions(&self) -> Result<Vec<BindIdentity>, String> {
        Ok(Vec::new())
    }
    fn expire_with_withdrawals(
        &self,
        row: &RetentionLineageRow,
        runs: &[RetentionRunRow],
        questions: &[broca_store::retention_withdrawals::RetentionWithdrawal],
    ) -> Result<(), String> {
        if !questions.is_empty() {
            return Err("retention index cannot persist content-free withdrawals".into());
        }
        self.upsert(row, runs)
    }
    fn pending_withdrawals(
        &self,
    ) -> Result<Vec<broca_store::retention_withdrawals::RetentionWithdrawal>, String> {
        Err("retention index cannot read content-free withdrawals".into())
    }
    fn settle_withdrawal(&self, _lineage: &str, _key: &str) -> Result<bool, String> {
        Err("retention index cannot settle content-free withdrawals".into())
    }
    fn discovery_lineages(&self) -> Result<Vec<BindIdentity>, String> {
        Ok(Vec::new())
    }
}

impl RetentionIndex for SqliteStore {
    fn live_inventory(&self) -> Result<Option<Vec<RetentionLineageRow>>, String> {
        self.retention_live_inventory()
            .map(Some)
            .map_err(|e| e.to_string())
    }
    fn pending_deletions(&self) -> Result<Option<Vec<BindIdentity>>, String> {
        self.retention_pending_deletions()
            .map(Some)
            .map_err(|e| e.to_string())
    }
    fn deletion_pending(&self, id: &str, key: &BindIdentity, pending: bool) -> Result<(), String> {
        self.retention_deletion_pending(id, key, pending)
            .map_err(|e| e.to_string())
    }
    fn runs(&self, lineage: &str) -> Result<Vec<RetentionRunRow>, String> {
        self.retention_run_rows(lineage)
    }
    fn retained_sessions(&self) -> Result<Vec<BindIdentity>, String> {
        SqliteStore::retained_sessions(self).map_err(|e| e.to_string())
    }
    fn expire_with_withdrawals(
        &self,
        row: &RetentionLineageRow,
        runs: &[RetentionRunRow],
        questions: &[broca_store::retention_withdrawals::RetentionWithdrawal],
    ) -> Result<(), String> {
        self.upsert_retention_lineage_with_withdrawals(row, runs, questions)
            .map_err(|e| e.to_string())
    }
    fn pending_withdrawals(
        &self,
    ) -> Result<Vec<broca_store::retention_withdrawals::RetentionWithdrawal>, String> {
        self.pending_retention_withdrawals()
            .map_err(|e| e.to_string())
    }
    fn settle_withdrawal(&self, lineage: &str, key: &str) -> Result<bool, String> {
        self.settle_retention_withdrawal(lineage, key)
            .map_err(|e| e.to_string())
    }
    fn discovery_lineages(&self) -> Result<Vec<BindIdentity>, String> {
        self.retention_lineages_with_discovery()
            .map_err(|e| e.to_string())
    }
    fn live(&self, session: &BindIdentity) -> Result<Option<RetentionLineageRow>, String> {
        self.lookup_live_retention_lineage(session)
            .map_err(|e| e.to_string())
    }
    fn lineage(&self, id: &str) -> Result<Option<RetentionLineageRow>, String> {
        self.lookup_retention_lineage(id).map_err(|e| e.to_string())
    }
    fn candidates(&self, now_ms: u64) -> Result<Vec<RetentionLineageRow>, String> {
        self.retention_candidates(now_ms).map_err(|e| e.to_string())
    }
    fn run(&self, id: &str) -> Result<Option<RunRetention>, String> {
        self.lookup_run_retention(id).map_err(|e| e.to_string())
    }
    fn upsert(&self, row: &RetentionLineageRow, runs: &[RetentionRunRow]) -> Result<(), String> {
        self.upsert_retention_lineage(row, runs)
            .map_err(|e| e.to_string())
    }
}

pub use cortexkit_role_llm_runner::errors::RetentionInvalidDetail as InvalidRetention;
pub use cortexkit_role_llm_runner::errors::{ExpiredDetail, EXPIRED};

pub fn validate_retention(
    requested: Option<u64>,
    current: Option<u64>,
    exists: bool,
) -> Result<Option<u64>, InvalidRetention> {
    let mut request = cortexkit_role_llm_runner::send::SendRequest::new("", "");
    request.retention = requested;
    let limits =
        cortexkit_role_llm_runner::describe::Retention::new(MAX_RETENTION_SECONDS, 3_600_000);
    request.check_retention(Some(&limits), exists.then_some(current))?;
    Ok(requested.filter(|seconds| current != Some(*seconds)))
}

/// Only sends and lifecycle records advance the retention clock. Holding a pending queue,
/// subscription or warm open cannot prevent expiry; paused and active episodes have no deadline.
pub fn policy(records: &[FramedRecord]) -> Option<(u64, u64, Option<u64>)> {
    if !broca_core::is_retention_lineage(records) {
        return None;
    }
    let mut seconds = 0;
    let mut activity = 0;
    let mut nonterminal = false;
    for frame in records {
        match &frame.record {
            WalRecord::RunStarted {
                retention, ts_ms, ..
            } => {
                if let Some(value) = retention {
                    seconds = *value;
                }
                activity = ts_ms.unwrap_or(activity);
                nonterminal = true;
            }
            WalRecord::Queued {
                retention, ts_ms, ..
            }
            | WalRecord::Steered {
                retention, ts_ms, ..
            } => {
                if let Some(value) = retention {
                    seconds = *value;
                }
                if let Some(time) = ts_ms {
                    activity = *time;
                }
            }
            WalRecord::RunFinished { ts_ms, .. } => {
                activity = ts_ms.unwrap_or(activity);
                nonterminal = false;
            }
            WalRecord::RunPaused { ts_ms, .. } => {
                activity = ts_ms.unwrap_or(activity);
                nonterminal = true;
            }
            _ => {}
        }
    }
    let deadline = (!nonterminal).then_some(activity.saturating_add(seconds.saturating_mul(1000)));
    Some((seconds, activity, deadline))
}

fn derive(
    session: &BindIdentity,
    lineage: String,
    records: &[FramedRecord],
    wal_bytes: u64,
    expired_at_ms: Option<u64>,
) -> Option<(RetentionLineageRow, Vec<RetentionRunRow>)> {
    let (retention_s, last_activity_ms, expires_at_ms) = policy(records)?;
    let withdrawals = broca_core::projection_plan::retained_withdrawal_projection(
        None,
        records,
        Some(&lineage),
        None,
    )
    .ok()?;
    let raw: Vec<_> = records.iter().map(|frame| frame.record.clone()).collect();
    let mut runs = BTreeMap::new();
    for frame in records {
        if let WalRecord::Retracted {
            submission_id,
            failure: Some(failure),
        } = &frame.record
        {
            let digest = records.iter().find_map(|frame| match &frame.record {
                WalRecord::Queued {
                    submission_id: id,
                    request_identity_digest,
                    ..
                } if id == submission_id => request_identity_digest.clone(),
                _ => None,
            });
            runs.insert(
                failure.run_id.0.clone(),
                RetentionRunRow {
                    run: RunIndexRow {
                        run_id: failure.run_id.0.clone(),
                        session: session.clone(),
                        submission_id: Some(submission_id.0.clone()),
                        request_identity_digest: digest,
                        state: broca_core::RunIndexState::Error,
                        usage: None,
                        final_step_finish_reason: None,
                        retries_used: 0,
                        title: None,
                        withdrawals_owed: false,
                    },
                    state_changed_ms: None,
                },
            );
        }
        if let WalRecord::RunStarted {
            run_id,
            submission_id,
            request_identity_digest,
            ..
        } = &frame.record
        {
            let state = broca_core::reconcile_run_state(&raw, &run_id.0)?;
            let episode = records
                .iter()
                .skip_while(|f| f.seq < frame.seq)
                .skip(1)
                .take_while(|f| !matches!(f.record, WalRecord::RunStarted { .. }));
            let state_changed_ms = episode
                .filter_map(|f| match &f.record {
                    WalRecord::RunFinished { ts_ms, .. } | WalRecord::RunPaused { ts_ms, .. } => {
                        *ts_ms
                    }
                    _ => None,
                })
                .last()
                .or(match &frame.record {
                    WalRecord::RunStarted { ts_ms, .. } => *ts_ms,
                    _ => None,
                });
            runs.insert(
                run_id.0.clone(),
                RetentionRunRow {
                    run: RunIndexRow {
                        run_id: run_id.0.clone(),
                        session: session.clone(),
                        submission_id: submission_id.as_ref().map(|id| id.0.clone()),
                        request_identity_digest: request_identity_digest.clone(),
                        state: state.state,
                        usage: state.usage,
                        final_step_finish_reason: state.final_step_finish_reason,
                        retries_used: state.retries_used,
                        title: None,
                        withdrawals_owed: expired_at_ms.is_none()
                            && withdrawals.as_ref().is_some_and(|metadata| {
                                metadata.withdrawals_owed_runs.contains(&run_id.0)
                            }),
                    },
                    state_changed_ms,
                },
            );
        }
    }
    Some((
        RetentionLineageRow {
            lineage_id: lineage,
            session: session.clone(),
            retention_s,
            last_activity_ms,
            expires_at_ms: if expired_at_ms.is_some() {
                None
            } else {
                expires_at_ms
            },
            wal_bytes,
            expired_at_ms,
        },
        runs.into_values().collect(),
    ))
}

/// Content-free durable deletion progress. The parent directory identifies the address.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tombstone {
    pub lineage_id: String,
    pub expired_at_ms: u64,
    pub seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
    /// The runner tag the deleted lineage's next part would have had (and at least every earlier
    /// tombstone's). A lineage created at this address afterwards starts counting here, so no
    /// tag is reused across the deletion. Absent for an untagged lineage, one whose WAL could
    /// not be read, and every tombstone written before tags existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_tag: Option<u64>,
    /// The compaction notice sequence number the deleted lineage's next notice would have had
    /// (and at least every earlier tombstone's). Notices are numbered per session, not per
    /// lineage, so a lineage created at this address afterwards numbers its notices from here.
    /// Absent for a lineage that recorded no notice, one whose WAL could not be read, and every
    /// tombstone written before notices existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_notice_seq: Option<u64>,
}

/// Each failed write increments the address's repair generation so an older repair cannot
/// remove a newer failure, even when the address was already awaiting repair.
#[derive(Default)]
pub(crate) struct Repairs(Mutex<HashMap<BindIdentity, u64>>);
impl Repairs {
    pub(crate) fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
    pub(crate) fn insert(&self, key: &BindIdentity) {
        let mut entries = self.0.lock().unwrap();
        let gen = entries.entry(key.clone()).or_default();
        *gen = gen.checked_add(1).expect("repair generation exhausted");
    }
    pub(crate) fn snapshot(&self) -> Vec<(BindIdentity, u64)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
    pub(crate) fn remove_if_unchanged(&self, key: &BindIdentity, generation: u64) {
        let mut entries = self.0.lock().unwrap();
        if entries.get(key) == Some(&generation) {
            entries.remove(key);
        }
    }
}

type EvictCaches = dyn Fn(&BindIdentity) + Send + Sync;

type AcquireLease = dyn Fn(&BindIdentity) -> Result<Box<dyn LeaseHandle>, String> + Send + Sync;

/// Shared by actors and the registry. Content mutations require the caller's session lease;
/// expiry reads may refresh the content-free retention summary, but never run state.
pub struct RetentionService {
    pub(crate) withdrawal_adapter:
        std::sync::OnceLock<Arc<dyn crate::retention_withdrawals::RetentionWithdrawalAdapter>>,
    pub(crate) withdrawal_wake: tokio::sync::Notify,
    unreadable: Mutex<std::collections::HashSet<String>>,
    enrollment: Mutex<Enrollment>,
    deletion_recovery: Mutex<(bool, std::collections::HashSet<BindIdentity>)>,
    #[cfg(test)]
    file_work: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    directory_walks: std::sync::atomic::AtomicUsize,
    pub wal: Arc<FileWalStore>,
    pub index: Arc<dyn RetentionIndex>,
    pub clock: Clock,
    evict: std::sync::OnceLock<Arc<EvictCaches>>,
    acquire: Arc<AcquireLease>,
    export: Arc<dyn broca_core::ExportIndex>,
    resolver: Arc<dyn broca_core::ChargeBasisResolver + Send + Sync>,
    #[cfg(test)]
    before_enroll: Mutex<Option<Arc<EnrollmentHook>>>,
}

#[derive(Default)]
struct Enrollment {
    recovered: bool,
    retry: std::collections::HashSet<PathBuf>,
    deferred_races: u64,
}

// RetentionIndex returns `String`, erasing rusqlite's error code, so enrollment uses these known
// SQLite busy messages to preserve the transient-store classification.
const STORE_BUSY_ERROR_MARKERS: [&str; 4] = [
    "database is locked",
    "database is busy",
    "store busy",
    "sqlite_busy",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EnrollmentFailureKind {
    VanishedFile,
    MidWrite,
    UnreadableFile,
    StoreBusy,
    MigrationInProgress,
}

impl EnrollmentFailureKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::VanishedFile => "vanished_file",
            Self::MidWrite => "mid_write",
            Self::UnreadableFile => "unreadable_file",
            Self::StoreBusy => "store_busy",
            Self::MigrationInProgress => "migration_in_progress",
        }
    }

    fn is_benign_race(self) -> bool {
        matches!(
            self,
            Self::VanishedFile | Self::StoreBusy | Self::MigrationInProgress
        )
    }
}

struct EnrollmentFailure {
    kind: EnrollmentFailureKind,
    error: String,
}

impl EnrollmentFailure {
    fn new(kind: EnrollmentFailureKind, error: impl Into<String>) -> Self {
        Self {
            kind,
            error: error.into(),
        }
    }

    fn wal(error: broca_wal::WalError) -> Self {
        let kind = match &error {
            broca_wal::WalError::Io(error) if error.kind() == io::ErrorKind::NotFound => {
                EnrollmentFailureKind::VanishedFile
            }
            broca_wal::WalError::SessionMoving { .. }
            | broca_wal::WalError::SessionMoved { .. } => {
                EnrollmentFailureKind::MigrationInProgress
            }
            broca_wal::WalError::Corrupt {
                source: broca_wal::framing::FrameError::NonTailIncompleteFrame,
                ..
            } => EnrollmentFailureKind::MidWrite,
            _ => EnrollmentFailureKind::UnreadableFile,
        };
        Self::new(kind, error.to_string())
    }

    fn store(error: String) -> Self {
        let lower = error.to_ascii_lowercase();
        let kind = if STORE_BUSY_ERROR_MARKERS
            .iter()
            .any(|marker| lower.contains(marker))
        {
            EnrollmentFailureKind::StoreBusy
        } else {
            EnrollmentFailureKind::UnreadableFile
        };
        Self::new(kind, error)
    }
}

impl From<String> for EnrollmentFailure {
    fn from(error: String) -> Self {
        Self::new(EnrollmentFailureKind::UnreadableFile, error)
    }
}

#[cfg(test)]
type EnrollmentHook = dyn Fn(&Path) + Send + Sync;

impl RetentionService {
    pub fn new<L: LeaseStore + 'static>(
        wal: Arc<FileWalStore>,
        index: Arc<dyn RetentionIndex>,
        leases: Arc<L>,
        export: Arc<dyn broca_core::ExportIndex>,
        resolver: Arc<dyn broca_core::ChargeBasisResolver + Send + Sync>,
    ) -> io::Result<Self> {
        let clock = Clock::for_state_root(wal.ephemeral_root().parent().unwrap_or(Path::new("")))?;
        Ok(Self {
            withdrawal_adapter: std::sync::OnceLock::new(),
            withdrawal_wake: tokio::sync::Notify::new(),
            evict: std::sync::OnceLock::new(),
            unreadable: Mutex::new(std::collections::HashSet::new()),
            enrollment: Mutex::default(),
            deletion_recovery: Mutex::default(),
            #[cfg(test)]
            file_work: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            directory_walks: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            before_enroll: Mutex::new(None),
            wal,
            index,
            clock,
            export,
            resolver,
            acquire: Arc::new(move |session| {
                leases
                    .acquire(&broca_core::lease_key_for(session))
                    .map_err(|e| e.to_string())
            }),
        })
    }

    pub(crate) fn set_evictor(&self, evict: Arc<EvictCaches>) {
        let _ = self.evict.set(evict);
    }

    pub(crate) fn lease(&self, key: &BindIdentity) -> Result<Box<dyn LeaseHandle>, String> {
        (self.acquire)(key)
    }

    fn tombstones(&self, key: &BindIdentity) -> Result<Vec<Tombstone>, String> {
        #[cfg(test)]
        self.file_work
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = self.wal.tombstone_path(key, "");
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.to_string()),
        };
        let mut stones = Vec::new();
        for entry in entries {
            let path = entry.map_err(|e| e.to_string())?.path();
            if path.is_file() && path.extension().is_none() {
                let bytes = match fs::read(&path) {
                    Ok(bytes) => bytes,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error.to_string()),
                };
                let stone: Tombstone = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
                if path.file_name().and_then(|s| s.to_str()) != Some(&stone.lineage_id) {
                    return Err("tombstone identity mismatch".into());
                }
                stones.push(stone);
            }
        }
        Ok(stones)
    }

    fn write_tombstone(&self, key: &BindIdentity, stone: &Tombstone) -> Result<(), String> {
        if stone.completed_at_ms.is_none() {
            self.index.deletion_pending(&stone.lineage_id, key, true)?;
        }
        let path = self.wal.tombstone_path(key, &stone.lineage_id);
        let parent = path.parent().expect("tombstone parent");
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        let temp = path.with_extension("tmp");
        let mut file = File::create(&temp).map_err(|e| e.to_string())?;
        file.write_all(&serde_json::to_vec(stone).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        fs::rename(temp, &path).map_err(|e| e.to_string())?;
        sync_dir(parent)?;
        sync_dir(parent.parent().expect("ephemeral root"))?;
        if stone.completed_at_ms.is_some() {
            self.index.deletion_pending(&stone.lineage_id, key, false)?;
        }
        Ok(())
    }

    fn read_path(&self, path: &Path) -> Result<broca_wal::WalRangeRead, String> {
        #[cfg(test)]
        self.file_work
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        broca_wal::read_records_range(path, self.wal.max_read_bytes(), 0, None, None)
            .map_err(|e| e.to_string())
    }

    fn read_enrollment_path(
        &self,
        path: &Path,
    ) -> Result<broca_wal::WalRangeRead, EnrollmentFailure> {
        #[cfg(test)]
        self.file_work
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        broca_wal::read_records_range(path, self.wal.max_read_bytes(), 0, None, None)
            .map_err(EnrollmentFailure::wal)
    }

    fn initial_lineage(&self, path: &Path) -> Result<String, String> {
        let mut bytes = [0; broca_wal::framing::LINEAGE_FRAME_LEN];
        File::open(path)
            .and_then(|mut file| file.read_exact(&mut bytes))
            .map_err(|e| e.to_string())?;
        match broca_wal::framing::decode_next(&bytes) {
            broca_wal::framing::DecodeStep::Lineage { lineage_id, .. } => {
                Ok(lineage_id.to_string())
            }
            _ => Err("retention WAL has no verified initial identity".into()),
        }
    }

    pub fn unreadable_sessions(&self) -> usize {
        self.unreadable.lock().unwrap().len()
    }

    fn current_indexed(
        &self,
        key: &BindIdentity,
        path: &Path,
        id: Option<&str>,
    ) -> Result<RetentionLineageRow, String> {
        let row = match id {
            Some(id) => self.index.lineage(id)?,
            None => self.index.live(key)?,
        }
        .ok_or("unreadable WAL has no retention row")?;
        // Corruption after records that the index has not yet copied can prevent deletion
        // within one hour after the session expires. An indexed deadline from before a new
        // active run would delete it too early, so unreadable logs require matching length and identity.
        if row.session != *key
            || row.wal_bytes != fs::metadata(path).map_err(|e| e.to_string())?.len()
            || row.lineage_id != self.initial_lineage(path)?
        {
            return Err("unreadable WAL has no current retention projection".into());
        }
        Ok(row)
    }

    fn mark_unreadable(&self, key: &BindIdentity) {
        self.mark_unreadable_address(broca_core::session_addr(key));
    }

    fn mark_unreadable_address(&self, address: String) {
        if self.unreadable.lock().unwrap().insert(address.clone()) {
            tracing::error!(target: "retention", session = %address,
                "retention deletion blocked by unreadable WAL with no current projection");
        }
    }

    fn withdrawal_export(
        &self,
        key: &BindIdentity,
        read: &broca_wal::WalRangeRead,
    ) -> Result<Vec<broca_store::retention_withdrawals::RetentionWithdrawal>, String> {
        use broca_store::retention_withdrawals::{
            RetentionQuestionId, RetentionScopeIdentity, RetentionWithdrawal,
        };
        let broca_wal::WalRangeRecords::Complete(records) = read.records() else {
            return Err("retention export requires complete durable records".into());
        };
        let projection = crate::approval::projection_from_frames(read.lineage_id, records)?;
        let mut rows = Vec::new();
        for owed in projection.obligations().withdrawals {
            let WalRecord::ToolCallDeferred { scope, ask, .. } = &owed.prepared.record else {
                continue;
            };
            rows.push(RetentionWithdrawal {
                lineage_id: projection.lineage_id.clone(),
                session: key.clone(),
                call_key: owed.withdrawal.call_key,
                scope: RetentionScopeIdentity::from_scope(scope),
                question: owed
                    .withdrawal
                    .elicitation_id
                    .map_or(RetentionQuestionId::Unknown, RetentionQuestionId::Known),
                provider_id: "cingulate".into(),
                provider_incarnation: None,
                expires_at_ms: ask.expires_at_ms,
            });
        }
        Ok(rows)
    }

    fn deleting(&self, key: &BindIdentity, id: &str) -> PathBuf {
        self.wal
            .ephemeral_root()
            .join(format!("{}.{id}.deleting", broca_core::session_addr(key)))
    }

    /// Live WALs in the portable or durable directory cannot expire: retention is chosen
    /// on the first send and requires the ephemeral directory. Retained summaries are usable
    /// only at the live WAL's byte length; stale summaries are replayed and tombstones win.
    pub fn expired(&self, key: &BindIdentity, run: Option<&str>) -> Result<Option<u64>, String> {
        self.expired_head(key, run, None)
    }

    pub(crate) fn expired_head(
        &self,
        key: &BindIdentity,
        run: Option<&str>,
        head: Option<u64>,
    ) -> Result<Option<u64>, String> {
        let run_meta = run.map(|run| self.index.run(run)).transpose()?.flatten();
        if let Some(meta) = &run_meta {
            if meta.expired_at_ms.is_some() {
                return Ok(meta.expired_at_ms);
            }
            if let Some(id) = &meta.lineage_id {
                if let Some(stone) = self
                    .tombstones(key)?
                    .into_iter()
                    .find(|s| &s.lineage_id == id)
                {
                    return Ok(Some(stone.expired_at_ms));
                }
            }
        }
        self.wal.check_move(key, false).map_err(|e| e.to_string())?;
        let path = self.wal.resolve_live_path(key).map_err(|e| e.to_string())?;
        // Deletion may move the resolved path aside before it is opened. Let the
        // ordinary range reader resolve absence/tombstones instead of failing the read.
        let file = path
            .as_ref()
            .map(|path| open_live_wal(path))
            .transpose()?
            .flatten();
        let path = path.filter(|_| file.is_some());
        // Retention cannot be added after the first send. Retained lineages are always
        // placed in wal-ephemeral, so portable/durable logs need no expiry replay.
        if path
            .as_ref()
            .is_some_and(|path| *path != self.wal.ephemeral_wal_path(key))
        {
            return Ok(None);
        }
        if head.is_none() {
            self.wal.flush_for_read(key).map_err(|e| e.to_string())?;
        }
        let stones = self.tombstones(key)?;
        if let Some(file) = &file {
            let bytes = file.metadata().map_err(|e| e.to_string())?.len();
            if let Some(row) = self.index.live(key)?.filter(|row| row.wal_bytes == bytes) {
                if let Some(stone) = stones
                    .iter()
                    .find(|stone| stone.lineage_id == row.lineage_id)
                {
                    return Ok(Some(stone.expired_at_ms));
                }
                // An actor's durable head may lag the physical tail. Never let a summary
                // that includes that tail replace the actor's narrower read proof.
                let covers_head = head
                    .map(|head| {
                        durable_prefix_bytes_from(
                            file.try_clone().map_err(|e| e.to_string())?,
                            head,
                        )
                    })
                    .transpose()?
                    .is_none_or(|prefix| prefix == bytes);
                if covers_head
                    && (run.is_none()
                        || run_meta.as_ref().is_some_and(|meta| {
                            meta.lineage_id.as_deref() == Some(row.lineage_id.as_str())
                        }))
                {
                    return Ok(row
                        .expires_at_ms
                        .filter(|deadline| *deadline <= self.clock.now_ms()));
                }
            }
        }
        let read = self
            .wal
            .read_records_range(key, 0, head, None)
            .map_err(|e| e.to_string())?;
        if matches!(read.records(), broca_wal::WalRangeRecords::Complete(_)) {
            if let Some(stone) = stones.iter().find(|s| {
                Some(s.lineage_id.as_str())
                    == read.lineage_id.as_ref().map(|id| id.to_string()).as_deref()
            }) {
                return Ok(Some(stone.expired_at_ms));
            }
            let policy = policy(frames(&read));
            if let (Some(path), Some(id), Some((retention_s, last_activity_ms, expires_at_ms))) =
                (&path, read.lineage_id, policy)
            {
                // Publish only a complete, unchanged prefix. The store serializes this
                // summary update and refuses to revive an expired lineage. Do not project
                // run rows here: a concurrent writer may already have advanced their state.
                if let Some(file) = open_live_wal(path)? {
                    let physical_bytes = file.metadata().map_err(|e| e.to_string())?.len();
                    let bytes = durable_prefix_bytes_from(
                        file,
                        frames(&read).last().map_or(0, |frame| frame.seq),
                    )?;
                    if physical_bytes == bytes {
                        self.index.upsert(
                            &RetentionLineageRow {
                                lineage_id: id.to_string(),
                                session: key.clone(),
                                retention_s,
                                last_activity_ms,
                                expires_at_ms,
                                wal_bytes: bytes,
                                expired_at_ms: None,
                            },
                            &[],
                        )?;
                    }
                }
            }
            if run.is_some_and(|id| {
                !frames(&read).iter().any(
                    |f| matches!(&f.record, WalRecord::RunStarted { run_id, .. } if run_id.0 == id),
                )
            }) {
                return Ok(None);
            }
            return Ok(policy
                .and_then(|(_, _, deadline)| deadline)
                .filter(|deadline| *deadline <= self.clock.now_ms()));
        }
        if run.is_some() {
            return Ok(None);
        }
        if self
            .wal
            .lineage_exists(key, None)
            .map_err(|e| e.to_string())?
        {
            return Ok(None);
        }
        Ok(stones
            .into_iter()
            .max_by_key(|stone| stone.seq)
            .map(|stone| stone.expired_at_ms))
    }

    /// Reuse the actor's already validated durable prefix instead of decoding it twice.
    pub(crate) fn rederive_records(
        &self,
        key: &BindIdentity,
        id: broca_wal::WalLineageId,
        records: &[FramedRecord],
        head: u64,
    ) -> Result<(), String> {
        self.wal.check_move(key, true).map_err(|e| e.to_string())?;
        let path = self
            .wal
            .resolve_live_path(key)
            .map_err(|e| e.to_string())?
            .ok_or("retention WAL disappeared before projection")?;
        let bytes = durable_prefix_bytes(&path, head)?;
        self.project_records(key, id, records, bytes)
    }

    fn project_records(
        &self,
        key: &BindIdentity,
        id: broca_wal::WalLineageId,
        records: &[FramedRecord],
        bytes: u64,
    ) -> Result<(), String> {
        if let Some((row, runs)) = derive(key, id.to_string(), records, bytes, None) {
            self.remember_session(key)?;
            self.index.upsert(&row, &runs)?;
        }
        Ok(())
    }

    pub(crate) fn rederive(&self, key: &BindIdentity) -> Result<(), String> {
        self.rederive_head(key, None)
    }

    /// Discovery can be stale even when the WAL byte length did not change. Re-read its
    /// durable facts periodically, under the lease, instead of waiting for new traffic.
    pub(crate) fn repair_discovery_flags(&self) -> Result<(), String> {
        for key in self.index.discovery_lineages()? {
            let Ok(_lease) = self.lease(&key) else {
                continue;
            };
            if self.rederive(&key).is_err() {
                let path = self.wal.resolve_live_path(&key).ok().flatten();
                if let Some(path) = path.filter(|path| self.read_path(path).is_err()) {
                    if self.current_indexed(&key, &path, None).is_err() {
                        self.mark_unreadable(&key);
                    }
                    continue;
                }
                tracing::warn!(target: "retention", session = %broca_core::session_addr(&key),
                    "discovery repair deferred");
            }
        }
        Ok(())
    }

    pub(crate) fn rederive_head(
        &self,
        key: &BindIdentity,
        head: Option<u64>,
    ) -> Result<(), String> {
        self.wal.check_move(key, true).map_err(|e| e.to_string())?;
        #[cfg(test)]
        self.file_work
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = self.wal.resolve_live_path(key).map_err(|e| e.to_string())?;
        let Some(path) = path else { return Ok(()) };
        let read =
            broca_wal::read_records_range(&path, broca_wal::DEFAULT_MAX_READ_BYTES, 0, head, None)
                .map_err(|e| e.to_string())?;
        let Some(id) = read.lineage_id else {
            return Err("retention WAL has no identity".into());
        };
        let bytes = match head {
            Some(seq) => durable_prefix_bytes(&path, seq)?,
            None => fs::metadata(&path).map_err(|e| e.to_string())?.len(),
        };
        self.project_records(key, id, frames(&read), bytes)
    }
}

fn sync_dir(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|e| e.to_string())
    }
    #[cfg(not(unix))]
    {
        // Portable directory handles are unavailable on Windows. Match the file WAL's
        // best-effort directory flush; tombstone file contents are always synced first.
        if let Ok(file) = File::open(path) {
            file.sync_all().map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

impl RetentionService {
    /// Explicit integrity repair also verifies identity sidecars for indexed WALs.
    pub fn enroll(&self) -> Result<usize, String> {
        self.enroll_work(true, true)
    }

    pub(crate) fn recover_enrollment(&self) -> Result<usize, String> {
        self.enroll_work(true, false)
    }

    pub(crate) fn enroll_changes(&self) -> Result<usize, String> {
        self.enroll_work(false, false)
    }

    fn enroll_work(&self, force_recovery: bool, inspect_known: bool) -> Result<usize, String> {
        let mut state = self.enrollment.lock().unwrap();
        let (addresses, overflow) = self.wal.take_retention_changes();
        state.retry.extend(
            addresses
                .into_iter()
                .map(|address| self.wal.ephemeral_root().join(format!("{address}.wal"))),
        );
        if overflow || force_recovery {
            state.recovered = false;
        }
        let inventory = if !state.recovered {
            let inventory = self.index.live_inventory()?.map(|rows| {
                rows.into_iter()
                    .map(|row| (broca_core::session_addr(&row.session), row.wal_bytes))
                    .collect::<HashMap<_, _>>()
            });
            state.retry.extend(self.ephemeral_files("wal")?);
            state.recovered = inventory.is_some();
            inventory
        } else {
            None
        };
        let paths = std::mem::take(&mut state.retry);
        let mut replayed = 0;
        for path in paths {
            #[cfg(test)]
            if let Some(hook) = self.before_enroll.lock().unwrap().as_ref() {
                hook(&path);
            }
            // An inventory hit is only a recovery optimization. Missing rows still
            // go through WAL discovery; they are never evidence of an absent log.
            if let Some(bytes) = inventory.as_ref().and_then(|rows| {
                path.file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|address| rows.get(address))
            }) {
                if !inspect_known && fs::metadata(&path).is_ok_and(|meta| meta.len() == *bytes) {
                    continue;
                }
            }
            match self.enroll_path(&path) {
                Ok(true) => replayed += 1,
                Ok(false) => {}
                Err(failure) => {
                    if failure.kind.is_benign_race() {
                        state.deferred_races = state.deferred_races.saturating_add(1);
                        tracing::debug!(target: "retention.enrollment", session = %path.file_stem().unwrap_or_default().to_string_lossy(),
                            kind = failure.kind.as_str(), error = %failure.error, counter = state.deferred_races,
                            "enrollment deferred");
                    } else {
                        tracing::warn!(target: "retention", session = %path.file_stem().unwrap_or_default().to_string_lossy(),
                            kind = failure.kind.as_str(), error = %failure.error,
                            "enrollment deferred");
                    }
                    if failure.kind != EnrollmentFailureKind::VanishedFile {
                        state.retry.insert(path);
                    }
                }
            }
        }
        Ok(replayed)
    }

    fn enroll_path(&self, path: &Path) -> Result<bool, EnrollmentFailure> {
        #[cfg(test)]
        self.file_work
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes = match fs::metadata(path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(EnrollmentFailure::new(
                    EnrollmentFailureKind::VanishedFile,
                    error.to_string(),
                ));
            }
            Err(error) => return Err(error.to_string().into()),
        };
        let portable =
            self.wal
                .root()
                .join(path.file_name().ok_or_else(|| {
                    EnrollmentFailure::from("retention WAL filename".to_string())
                })?);
        if portable
            .with_extension("moving")
            .try_exists()
            .map_err(|error| error.to_string())?
            || portable
                .with_extension("moved")
                .try_exists()
                .map_err(|error| error.to_string())?
        {
            return Err(EnrollmentFailure::new(
                EnrollmentFailureKind::MigrationInProgress,
                "retention enrollment deferred by migration marker",
            ));
        }
        // The hashed address is not reversible. Writers leave a content-free identity
        // sidecar; older WALs without one need replay to discover their session.
        let (first, discovered) = match self.read_first_session(path) {
            Ok(found) => found,
            Err(error) => {
                if path.try_exists().is_ok_and(|exists| !exists) {
                    return Err(EnrollmentFailure::new(
                        EnrollmentFailureKind::VanishedFile,
                        error.error,
                    ));
                }
                if error.kind == EnrollmentFailureKind::UnreadableFile {
                    self.mark_unreadable_address(
                        path.file_stem()
                            .ok_or_else(|| {
                                EnrollmentFailure::from("retention address missing".to_string())
                            })?
                            .to_string_lossy()
                            .into(),
                    );
                }
                return Err(error);
            }
        };
        let Some(key) = first else { return Ok(false) };
        self.wal
            .check_move(&key, true)
            .map_err(EnrollmentFailure::wal)?;
        let matching = self
            .index
            .live(&key)
            .map_err(EnrollmentFailure::store)?
            .is_some_and(|row| row.wal_bytes == bytes);
        if matching && !discovered {
            return Ok(false);
        }
        let _lease = self.lease(&key).map_err(EnrollmentFailure::store)?;
        if discovered {
            self.remember_session(&key)
                .map_err(EnrollmentFailure::store)?;
        }
        if !matching {
            if let Err(error) = self.rederive(&key) {
                if path.try_exists().is_ok_and(|exists| !exists) {
                    return Err(EnrollmentFailure::new(
                        EnrollmentFailureKind::VanishedFile,
                        error,
                    ));
                }
                if self.read_path(path).is_err() {
                    self.mark_unreadable(&key);
                }
                return Err(EnrollmentFailure::store(error));
            }
        }
        Ok(true)
    }

    fn ephemeral_files(&self, extension: &str) -> Result<Vec<PathBuf>, String> {
        #[cfg(test)]
        self.directory_walks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        #[cfg(test)]
        self.file_work
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let entries = match fs::read_dir(self.wal.ephemeral_root()) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.to_string()),
        };
        entries
            .filter_map(|entry| match entry {
                Ok(entry) if entry.path().extension().is_some_and(|ext| ext == extension) => {
                    Some(Ok(entry.path()))
                }
                Ok(_) => None,
                Err(e) => Some(Err(e.to_string())),
            })
            .collect()
    }

    fn read_first_session(
        &self,
        path: &Path,
    ) -> Result<(Option<BindIdentity>, bool), EnrollmentFailure> {
        #[cfg(test)]
        self.file_work
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // A bounded range still validates the log, so discovery is cached in a content-free
        // sidecar. Once written, boot enrollment needs only a stat and indexed row lookup.
        let sidecar = path.with_extension("session");
        if let Ok(bytes) = fs::read(&sidecar) {
            if let Ok(session) = serde_json::from_slice::<BindIdentity>(&bytes) {
                if path.file_stem().and_then(|stem| stem.to_str())
                    == Some(broca_core::session_addr(&session).as_str())
                {
                    return Ok((Some(session), false));
                }
            }
        }
        let read = self.read_enrollment_path(path)?;
        Ok((
            frames(&read).iter().find_map(|frame| match &frame.record {
                WalRecord::RunStarted { session, .. } => Some(session.clone()),
                _ => None,
            }),
            true,
        ))
    }

    pub(crate) fn remember_session(&self, key: &BindIdentity) -> Result<(), String> {
        let path = self.wal.ephemeral_wal_path(key).with_extension("session");
        let bytes = serde_json::to_vec(key).map_err(|e| e.to_string())?;
        if fs::read(&path).is_ok_and(|current| current == bytes) {
            return Ok(());
        }
        let temp = path.with_extension("session.tmp");
        let mut file = File::create(&temp).map_err(|e| e.to_string())?;
        file.write_all(&bytes).map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        fs::rename(temp, &path).map_err(|e| e.to_string())?;
        sync_dir(path.parent().expect("ephemeral root"))
    }

    /// Confirm expiry from the WAL and export terminal runs before writing the tombstone.
    /// The caller holds the session lease throughout.
    pub(crate) fn delete_expired(
        &self,
        key: &BindIdentity,
        now_ms: u64,
    ) -> Result<Option<String>, String> {
        self.wal.check_move(key, true).map_err(|e| e.to_string())?;
        for stone in self
            .tombstones(key)?
            .into_iter()
            .filter(|s| s.completed_at_ms.is_none())
        {
            self.complete(key, stone, now_ms)?;
        }
        let Some(path) = self.wal.resolve_live_path(key).map_err(|e| e.to_string())? else {
            return Ok(None);
        };
        let read = self.read_path(&path);
        let (id, deadline) = match &read {
            Ok(read) => {
                let Some((_, _, Some(deadline))) = policy(frames(read)) else {
                    self.rederive(key)?;
                    return Ok(None);
                };
                if deadline > now_ms {
                    self.rederive(key)?;
                    tracing::info!(target: "retention", "WAL confirmation skipped unexpired candidate");
                    return Ok(None);
                }
                (
                    read.lineage_id
                        .ok_or("retention WAL has no identity")?
                        .to_string(),
                    deadline,
                )
            }
            Err(_) => {
                let row = match self.current_indexed(key, &path, None) {
                    Ok(row) => row,
                    Err(_) => {
                        self.mark_unreadable(key);
                        return Ok(None);
                    }
                };
                let Some(deadline) = row.expires_at_ms.filter(|deadline| *deadline <= now_ms)
                else {
                    return Ok(None);
                };
                (row.lineage_id, deadline)
            }
        };
        self.unreadable
            .lock()
            .unwrap()
            .remove(&broca_core::session_addr(key));
        let mut failed_exports = Vec::new();
        if let Ok(read) = &read {
            let raw: Vec<_> = frames(read).iter().map(|f| f.record.clone()).collect();
            for frame in frames(read) {
                if let WalRecord::RunStarted { run_id, .. } = &frame.record {
                    if broca_core::reconcile_run_state(&raw, &run_id.0)
                        .is_some_and(|state| state.state.is_terminal())
                        && self
                            .export
                            .emit_run(frames(read), run_id, self.resolver.as_ref())
                            .is_err()
                    {
                        failed_exports.push(run_id.0.clone());
                    }
                }
            }
        } else {
            let discovered = self.export.missing_terminal_runs().map(|runs| {
                runs.into_iter()
                    .filter(|run| run.session == *key)
                    .map(|run| run.run_id.0)
                    .collect()
            });
            failed_exports = match discovered {
                Ok(runs) => runs,
                Err(_) => self
                    .index
                    .runs(&id)?
                    .into_iter()
                    .map(|run| run.run.run_id)
                    .collect(),
            };
        }
        if !failed_exports.is_empty() {
            // Retry billing facts normally emitted at run completion, but delete expired
            // conversation content even if that retry fails. Do not log content-bearing causes.
            tracing::warn!(target: "retention", session = %broca_core::session_addr(key), runs = ?failed_exports,
                "retention billing catch-up failed; deleting content anyway");
        }
        let earlier = self.tombstones(key)?;
        let seq = earlier
            .iter()
            .map(|s| s.seq)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or("tombstone sequence exhausted")?;
        let own_next_tag = read.as_ref().ok().and_then(|read| {
            broca_core::tags::lineage_tags(frames(read).iter().map(|frame| &frame.record)).next()
        });
        let next_tag = earlier
            .iter()
            .filter_map(|stone| stone.next_tag)
            .chain(own_next_tag)
            .max();
        let own_next_notice_seq = read.as_ref().ok().and_then(|read| {
            frames(read)
                .iter()
                .filter_map(|frame| match &frame.record {
                    WalRecord::CompactionNotice { seq, .. } => seq.checked_add(1),
                    _ => None,
                })
                .max()
        });
        let next_notice_seq = earlier
            .iter()
            .filter_map(|stone| stone.next_notice_seq)
            .chain(own_next_notice_seq)
            .max();
        let stone = Tombstone {
            lineage_id: id.clone(),
            expired_at_ms: deadline,
            seq,
            completed_at_ms: None,
            next_tag,
            next_notice_seq,
        };
        self.remember_session(key)?;
        self.write_tombstone(key, &stone)?;
        self.complete(key, stone, now_ms)?;
        Ok(Some(id))
    }

    /// Recovery infers progress from disk and never moves a different lineage's live WAL.
    fn complete(
        &self,
        key: &BindIdentity,
        mut stone: Tombstone,
        now_ms: u64,
    ) -> Result<(), String> {
        self.wal.check_move(key, true).map_err(|e| e.to_string())?;
        let moved = self.deleting(key, &stone.lineage_id);
        if let Some(path) = self.wal.resolve_live_path(key).map_err(|e| e.to_string())? {
            let identity = match self.read_path(&path) {
                Ok(read) => read.lineage_id.map(|id| id.to_string()),
                Err(_) => Some(self.initial_lineage(&path)?),
            };
            if identity.as_deref() == Some(stone.lineage_id.as_str()) {
                fs::rename(path, &moved).map_err(|e| e.to_string())?;
                sync_dir(&self.wal.ephemeral_root())?;
            }
        }
        if moved.exists() {
            let read = self.read_path(&moved);
            let identity = match &read {
                Ok(read) => read.lineage_id.map(|id| id.to_string()),
                Err(_) => Some(self.initial_lineage(&moved)?),
            };
            if identity.as_deref() != Some(stone.lineage_id.as_str()) {
                return Err("deleting WAL identity mismatch".into());
            }
            let bytes = fs::metadata(&moved).map_err(|e| e.to_string())?.len();
            let (row, runs) = match &read {
                Ok(read) => derive(
                    key,
                    stone.lineage_id.clone(),
                    frames(read),
                    bytes,
                    Some(stone.expired_at_ms),
                )
                .ok_or("deleting WAL has no retention")?,
                Err(_) => {
                    let mut row = self.current_indexed(key, &moved, Some(&stone.lineage_id))?;
                    row.expired_at_ms = Some(stone.expired_at_ms);
                    row.expires_at_ms = None;
                    let mut runs = self.index.runs(&stone.lineage_id)?;
                    if runs.iter().any(|run| !run.run.state.is_terminal()) {
                        return Err("current retention projection is not terminal".into());
                    }
                    for run in &mut runs {
                        run.run.withdrawals_owed = false;
                        run.run.title = None;
                    }
                    (row, runs)
                }
            };
            // The approval index only records which lineage and runs may owe withdrawals;
            // it stores no scope or question identities before expiry. Recover those identities
            // from the WAL when possible. If corruption prevents recovery, drop the withdrawal
            // obligations: prefrontal-core expires the questions on their own deadlines.
            let questions = match read
                .as_ref()
                .map_err(|_| ())
                .and_then(|read| self.withdrawal_export(key, read).map_err(|_| ()))
            {
                Ok(questions) => questions,
                Err(()) => {
                    tracing::warn!(target: "retention", session = %broca_core::session_addr(key),
                        "retention withdrawal identities unavailable; dropping obligations");
                    Vec::new()
                }
            };
            self.index
                .expire_with_withdrawals(&row, &runs, &questions)?;
            self.withdrawal_wake.notify_one();
            fs::remove_file(&moved).map_err(|e| e.to_string())?;
            sync_dir(&self.wal.ephemeral_root())?;
        } else {
            // Once the WAL is deleted, the transaction committed before deletion is the
            // remaining proof of the lineage's expiry and each run's terminal state.
            let row = self
                .index
                .lineage(&stone.lineage_id)?
                .ok_or("deleted WAL has no committed retention row")?;
            if row.expired_at_ms != Some(stone.expired_at_ms) {
                return Err("deleted WAL has no committed expiry".into());
            }
            self.index.upsert(&row, &[])?;
        }
        if let Some(evict) = self.evict.get() {
            evict(key);
        }
        if self
            .wal
            .resolve_live_path(key)
            .map_err(|e| e.to_string())?
            .is_none()
        {
            let session = self.wal.ephemeral_wal_path(key).with_extension("session");
            for side in [session.clone(), session.with_extension("session.tmp")] {
                match fs::remove_file(side) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
            sync_dir(&self.wal.ephemeral_root())?;
        }
        // Completion is durable only after identity sidecars are removed and the directory is synced.
        // A crash or removal failure before this point leaves an incomplete tombstone to retry.
        stone.completed_at_ms = Some(now_ms);
        self.write_tombstone(key, &stone)?;
        Ok(())
    }

    /// Finish incomplete tombstones, retrying failed keys on later sweeps without
    /// preventing unrelated sessions from completing their deletion.
    pub fn resume_deletions(&self, now_ms: u64) -> Result<Vec<BindIdentity>, String> {
        let mut recovery = self.deletion_recovery.lock().unwrap();
        let pending = match self.index.pending_deletions()? {
            Some(keys) => keys,
            // An index without a recovery inventory cannot vouch for completed work.
            None => self.known_sessions()?,
        };
        let mut keys: std::collections::HashSet<_> = pending.into_iter().collect();
        if !recovery.0 {
            keys.extend(self.known_sessions()?);
            recovery.0 = true;
        }
        keys.extend(std::mem::take(&mut recovery.1));
        let mut completed = Vec::new();
        for key in keys {
            let result = (|| {
                let stones = self.tombstones(&key)?;
                if stones.iter().all(|s| s.completed_at_ms.is_some()) {
                    for stone in stones {
                        self.index
                            .deletion_pending(&stone.lineage_id, &key, false)?;
                    }
                    return Ok(false);
                }
                let _lease = self.lease(&key)?;
                for stone in stones.into_iter().filter(|s| s.completed_at_ms.is_none()) {
                    self.complete(&key, stone, now_ms)?;
                }
                Ok::<_, String>(true)
            })();
            match result {
                Ok(true) => completed.push(key),
                Ok(false) => {}
                Err(_) => {
                    tracing::warn!(target: "retention", session = %broca_core::session_addr(&key),
                    "deletion recovery deferred");
                    recovery.1.insert(key);
                }
            }
        }
        Ok(completed)
    }

    fn known_sessions(&self) -> Result<Vec<BindIdentity>, String> {
        let mut sessions = std::collections::HashSet::new();
        let paths = self.ephemeral_files("expired")?;
        let addresses: std::collections::HashSet<_> = paths
            .iter()
            .filter_map(|path| path.file_stem().and_then(|s| s.to_str()).map(str::to_owned))
            .collect();
        for path in paths {
            let bytes = match fs::read(path.with_extension("session")) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => {
                    tracing::warn!(target: "retention", session = %path.file_stem().unwrap_or_default().to_string_lossy(),
                        "session discovery deferred");
                    continue;
                }
            };
            match serde_json::from_slice::<BindIdentity>(&bytes) {
                Ok(key) => {
                    sessions.insert(key);
                }
                Err(_) => {
                    tracing::warn!(target: "retention", session = %path.file_stem().unwrap_or_default().to_string_lossy(),
                    "session discovery deferred")
                }
            }
        }
        // Durable identity-only rows discover interrupted tombstone completion even
        // after deletion has removed the content-free sidecar.
        if !addresses.is_empty() {
            sessions.extend(
                self.index
                    .retained_sessions()?
                    .into_iter()
                    .filter(|key| addresses.contains(&broca_core::session_addr(key))),
            );
        }
        Ok(sessions.into_iter().collect())
    }
}

fn frames(read: &broca_wal::WalRangeRead) -> &[FramedRecord] {
    match read.records() {
        broca_wal::WalRangeRecords::Complete(records)
        | broca_wal::WalRangeRecords::Partial(records)
        | broca_wal::WalRangeRecords::Unknown(records) => records,
        broca_wal::WalRangeRecords::Absent => &[],
    }
}

#[cfg(test)]
#[path = "retention_tests.rs"]
mod tests;

/// Count file bytes through the actor's highest fsynced sequence, including the lineage-ID
/// metadata between records but excluding later records appended by a concurrent run.
/// Each append includes trailing lineage metadata, so the record's byte anchor alone
/// does not describe the full durable prefix.
fn durable_prefix_bytes(path: &Path, head: u64) -> Result<u64, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    durable_prefix_bytes_from(file, head)
}

fn open_live_wal(path: &Path) -> Result<Option<File>, String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    if !file.metadata().map_err(|e| e.to_string())?.is_file() {
        return Err("retention live WAL is not a regular file".into());
    }
    Ok(Some(file))
}

fn durable_prefix_bytes_from(mut file: File, head: u64) -> Result<u64, String> {
    use std::io::{Seek, SeekFrom};
    let bytes = file.metadata().map_err(|error| error.to_string())?.len();
    if bytes > broca_wal::DEFAULT_MAX_READ_BYTES {
        return Err("retention WAL exceeds read limit".into());
    }
    // The caller already validated these records (or an unchanged summary describes
    // them). Locate the sequence boundary using headers, without decoding payloads again.
    let mut offset = 0u64;
    let mut found = head == 0;
    while offset < bytes {
        let mut header = [0; broca_wal::framing::HEADER_LEN];
        if bytes - offset < header.len() as u64 {
            break;
        }
        file.read_exact(&mut header)
            .map_err(|error| error.to_string())?;
        let len = u32::from_le_bytes(header[..4].try_into().unwrap());
        let seq = u64::from_le_bytes(header[5..13].try_into().unwrap());
        let end = offset + header.len() as u64 + u64::from(len);
        if len > broca_wal::framing::MAX_PAYLOAD_LEN || end > bytes {
            break;
        }
        if seq > head {
            return if found {
                Ok(offset)
            } else {
                Err("retention durable head is absent".into())
            };
        }
        found |= seq == head;
        offset = end;
        file.seek(SeekFrom::Start(offset))
            .map_err(|error| error.to_string())?;
    }
    if found {
        Ok(offset)
    } else {
        Err("retention durable head is absent".into())
    }
}
