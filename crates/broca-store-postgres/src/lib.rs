//! Postgres-backed materialized store.
//!
//! This crate is the PostgreSQL peer of `broca-store`'s SQLite backend. It
//! implements the stable `MaterializedStore` + `Persister` traits while keeping
//! the runner loop database-agnostic: no Postgres types appear outside this crate.
#![forbid(unsafe_code)]

use std::num::TryFromIntError;
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;

use broca_core::projection_plan::ProjectionSnapshot;
use broca_core::{
    decode_admission_checkpoint, last_control_cursor_through, AdmissionStoreSnapshot, Cursor,
    MaterializedStore, Persister, ProjectionSeed, RunIndex, RunIndexRow, RunIndexState,
    StorePageRow, StorePageSnapshot, StorePrefix, WalStore,
};
#[cfg(test)]
use broca_core::{encode_admission_checkpoint, AdmissionCheckpointV1};
use broca_protocol::{content::Message, BindIdentity};
use broca_protocol::{FinishReason, Usage};
use broca_wal::{FramedRecord, WalByteAnchor, WalRangeRead};
use postgres::{Client, IsolationLevel, NoTls, Transaction};

pub mod export_index;

/// Auditable outcome of one explicit legacy page-usage backfill.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageUsageBackfillCounts {
    pub scanned: u64,
    pub converted: u64,
    pub empty: u64,
    pub declined: u64,
}

/// Auditable outcome of one explicit legacy control-head repair.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ControlHeadRepairCounts {
    pub scanned: u64,
    pub repaired: u64,
    pub declined: u64,
}

#[derive(Debug)]
pub enum StoreError {
    AttributionStale,
    Postgres(postgres::Error),
    Json(serde_json::Error),
    Int(TryFromIntError),
    LockPoisoned,
    InconsistentSnapshot(&'static str),
    /// The WAL being projected is a corrupt lineage (replay refused to fabricate a
    /// prefix). Surfaced rather than silently materializing a wrong projection.
    Replay(broca_core::ReplayError),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::AttributionStale => {
                write!(f, "store message run attribution requires WAL backfill")
            }
            StoreError::Postgres(e) => write!(f, "postgres: {e}"),
            StoreError::Json(e) => write!(f, "json: {e}"),
            StoreError::Int(e) => write!(f, "integer conversion: {e}"),
            StoreError::LockPoisoned => write!(f, "postgres connection mutex poisoned"),
            StoreError::InconsistentSnapshot(msg) => write!(f, "inconsistent snapshot: {msg}"),
            StoreError::Replay(e) => write!(f, "replay: {e}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<broca_core::projection_plan::ProjectionPlanError> for StoreError {
    fn from(error: broca_core::projection_plan::ProjectionPlanError) -> Self {
        use broca_core::projection_plan::ProjectionPlanError;
        match error {
            ProjectionPlanError::Json(e) => Self::Json(e),
            ProjectionPlanError::Replay(e) => Self::Replay(e),
            ProjectionPlanError::Int(e) => Self::Int(e),
            ProjectionPlanError::InconsistentSnapshot(message) => {
                Self::InconsistentSnapshot(message)
            }
        }
    }
}

impl From<postgres::Error> for StoreError {
    fn from(e: postgres::Error) -> Self {
        StoreError::Postgres(e)
    }
}
impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::Json(e)
    }
}
impl From<broca_core::ReplayError> for StoreError {
    fn from(e: broca_core::ReplayError) -> Self {
        StoreError::Replay(e)
    }
}
impl From<TryFromIntError> for StoreError {
    fn from(e: TryFromIntError) -> Self {
        StoreError::Int(e)
    }
}

/// PostgreSQL-backed materialized store. The synchronous client is wrapped in a
/// mutex so one store value can satisfy the `Send + Sync` session-store traits.
/// Projection keeps WAL replay, checkpoint folding, and JSON serialization outside that mutex.
/// Callers must keep database access on `spawn_blocking` until this backend moves to
/// `tokio-postgres`.
/// Wall-clock millis for a run-index state transition. Only ever compared against other
/// wall-clock readings to answer "how long has this row been in this state", never used for
/// ordering or identity, so clock adjustment cannot corrupt anything durable.
fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub struct PostgresStore {
    client: Mutex<Client>,
    pub(crate) export_high_water: AtomicU64,
    pub(crate) export_source_instance_id: String,
    pub(crate) export_source_epoch: u64,
}

const OPTIMISTIC_PROJECTION_ATTEMPTS: usize = 3;
const MESSAGE_INSERT_BATCH_SIZE: usize = 500;

enum ProjectionPlan {
    NoChange {
        expected_watermark: u64,
        expected_lineage_id: Option<String>,
    },
    Reset {
        expected_watermark: u64,
        expected_lineage_id: Option<String>,
    },
    RetentionMetadata {
        expected_watermark: u64,
        expected_lineage_id: Option<String>,
        metadata: broca_core::projection_plan::RetainedWithdrawalProjection,
    },
    Write(Box<PreparedProjection>),
}

impl ProjectionPlan {
    fn expected_state(&self) -> (u64, Option<&str>) {
        match self {
            Self::NoChange {
                expected_watermark,
                expected_lineage_id,
            }
            | Self::Reset {
                expected_watermark,
                expected_lineage_id,
            }
            | Self::RetentionMetadata {
                expected_watermark,
                expected_lineage_id,
                ..
            } => (*expected_watermark, expected_lineage_id.as_deref()),
            Self::Write(prepared) => (
                prepared.expected_watermark,
                prepared.expected_lineage_id.as_deref(),
            ),
        }
    }
}

struct PreparedProjection {
    expected_watermark: u64,
    expected_lineage_id: Option<String>,
    reset: bool,
    lineage_id: Option<String>,
    new_watermark: u64,
    new_watermark_i64: i64,
    next_ord: i64,
    truncate_prefix_to: Option<i64>,
    message_json: Vec<String>,
    message_run_json: Vec<String>,
    final_run_json: Vec<(i64, String)>,
    completed_steps: i64,
    usage_json: String,
    retries_used: i64,
    last_step_usage_json: Option<String>,
    last_step_finish_reason_json: Option<String>,
    last_run_id: Option<String>,
    last_title: Option<String>,
    last_episode_usage_json: String,
    checkpoint_json: String,
    anchor_file_id: Option<String>,
    anchor_seq: Option<i64>,
    anchor_byte_end: Option<i64>,
    control_head_wal_seq: Option<i64>,
    control_head_sub_index: Option<i64>,
    deferred_calls_json: Option<String>,
    withdrawals_owed_runs: Vec<String>,
}

enum ProjectionCommit {
    Applied(u64),
    Conflict,
}

impl PostgresStore {
    /// Connect to Postgres and create the materialized-store tables if needed.
    pub fn connect(params: &str) -> Result<Self, StoreError> {
        let mut client = Client::connect(params, NoTls)?;
        let (source_instance_id, source_epoch, high_water) = Self::init(&mut client)?;
        Ok(Self {
            client: Mutex::new(client),
            export_high_water: AtomicU64::new(high_water),
            export_source_instance_id: source_instance_id,
            export_source_epoch: source_epoch,
        })
    }

    /// Populate page-usage metadata for legacy rows that still have SQL `NULL`.
    ///
    /// This is deliberately an explicit operation rather than part of [`Self::connect`]. A row is
    /// changed only when its checkpoint decodes and its last episode belongs to `last_run_id`.
    /// Missing, malformed, and mismatched checkpoints remain SQL `NULL`, preserving page reads'
    /// checkpoint fallback.
    pub fn backfill_page_usage(&self) -> Result<PageUsageBackfillCounts, StoreError> {
        self.backfill_page_usage_inner(true)
    }

    /// Report the page-usage backfill counts without committing any writes.
    pub fn preview_page_usage_backfill(&self) -> Result<PageUsageBackfillCounts, StoreError> {
        self.backfill_page_usage_inner(false)
    }

    fn backfill_page_usage_inner(
        &self,
        commit: bool,
    ) -> Result<PageUsageBackfillCounts, StoreError> {
        let mut client = self.client.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut tx = client.transaction()?;
        let legacy_rows = tx.query(
            "SELECT session, last_run_id, admission_checkpoint_json
               FROM meta
              WHERE last_episode_usage_json IS NULL
                FOR UPDATE",
            &[],
        )?;

        let mut counts = PageUsageBackfillCounts {
            scanned: u64::try_from(legacy_rows.len())?,
            ..PageUsageBackfillCounts::default()
        };
        for row in legacy_rows {
            let session = row.get::<_, String>(0);
            let last_run_id = row.get::<_, Option<String>>(1);
            let checkpoint_json = row.get::<_, Option<String>>(2);
            let Some(checkpoint) = checkpoint_json
                .as_deref()
                .and_then(decode_admission_checkpoint)
            else {
                counts.declined += 1;
                continue;
            };
            let Some(last_episode) = checkpoint.last_episode else {
                counts.declined += 1;
                continue;
            };
            if last_run_id.as_deref() != Some(last_episode.run_id.0.as_str()) {
                counts.declined += 1;
                continue;
            }

            let usage_json = serde_json::to_string(&last_episode.usage)?;
            tx.execute(
                "UPDATE meta SET last_episode_usage_json = $2 WHERE session = $1",
                &[&session, &usage_json],
            )?;
            if last_episode.usage.is_some() {
                counts.converted += 1;
            } else {
                counts.empty += 1;
            }
        }
        if commit {
            tx.commit()?;
        } else {
            tx.rollback()?;
        }
        Ok(counts)
    }

    /// Populate control-head metadata for legacy projected rows by replaying their WALs.
    ///
    /// This is deliberately an explicit operation rather than part of [`Self::connect`]. Missing
    /// or unreadable WALs and WALs with no derivable cursor remain unchanged, preserving the loud
    /// full-replay fallback instead of storing a guessed stream position.
    pub fn repair_control_heads(
        &self,
        wal_store: &dyn WalStore,
    ) -> Result<ControlHeadRepairCounts, StoreError> {
        self.repair_control_heads_inner(wal_store, true)
    }

    /// Report control-head repair counts without committing any writes.
    pub fn preview_control_head_repair(
        &self,
        wal_store: &dyn WalStore,
    ) -> Result<ControlHeadRepairCounts, StoreError> {
        self.repair_control_heads_inner(wal_store, false)
    }

    fn repair_control_heads_inner(
        &self,
        wal_store: &dyn WalStore,
        commit: bool,
    ) -> Result<ControlHeadRepairCounts, StoreError> {
        let mut client = self.client.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut tx = client.transaction()?;
        let legacy_rows = tx.query(
            "SELECT session, watermark_seq
               FROM meta
              WHERE watermark_seq > 0 AND control_head_wal_seq IS NULL
                FOR UPDATE",
            &[],
        )?;

        let mut counts = ControlHeadRepairCounts {
            scanned: u64::try_from(legacy_rows.len())?,
            ..ControlHeadRepairCounts::default()
        };
        for row in legacy_rows {
            let key = row.get::<_, String>(0);
            let watermark = i64_to_u64(row.get::<_, i64>(1))?;
            let derived = bind_identity_from_store_key(&key)
                .and_then(|session| wal_store.read_records(&session).ok())
                .and_then(|records| last_control_cursor_through(&records, Some(watermark)));
            let Some(cursor) = derived else {
                counts.declined += 1;
                continue;
            };
            let wal_seq = u64_to_i64(cursor.wal_seq)?;
            let sub_index = i64::from(cursor.sub_index);

            tx.execute(
                "UPDATE meta
                    SET control_head_wal_seq = $2, control_head_sub_index = $3
                  WHERE session = $1 AND control_head_wal_seq IS NULL",
                &[&key, &wal_seq, &sub_index],
            )?;
            counts.repaired += 1;
        }
        if commit {
            tx.commit()?;
        } else {
            tx.rollback()?;
        }
        Ok(counts)
    }

    fn init(client: &mut Client) -> Result<(String, u64, u64), StoreError> {
        client.batch_execute(
            "CREATE TABLE IF NOT EXISTS meta (
                 session TEXT PRIMARY KEY,
                 watermark_seq BIGINT NOT NULL,
                 lineage_id TEXT,
                 control_head_wal_seq BIGINT,
                 control_head_sub_index BIGINT,
                 context_epoch BIGINT NOT NULL,
                  completed_steps BIGINT NOT NULL DEFAULT 0,
                  usage_json TEXT NOT NULL DEFAULT '{}',
                  retries_used BIGINT NOT NULL DEFAULT 0,
                 last_step_usage_json TEXT,
                  last_step_finish_reason_json TEXT,
                  last_run_id TEXT,
                  last_episode_usage_json TEXT,
                  wal_anchor_file_id TEXT,
                 wal_anchor_seq BIGINT,
                 wal_anchor_byte_end BIGINT,
                 prefix_len BIGINT,
                 admission_checkpoint_json TEXT
             );
              CREATE TABLE IF NOT EXISTS retention_lineage (
                  lineage_id TEXT PRIMARY KEY,
                  session TEXT NOT NULL,
                  retention_s BIGINT NOT NULL,
                  last_activity_ms BIGINT NOT NULL,
                  expires_at_ms BIGINT,
                  wal_bytes BIGINT NOT NULL,
                  expired_at_ms BIGINT
              );
              CREATE INDEX IF NOT EXISTS retention_lineage_expires_idx
                  ON retention_lineage(expires_at_ms) WHERE expires_at_ms IS NOT NULL;
              CREATE UNIQUE INDEX IF NOT EXISTS retention_lineage_live_session_idx
                  ON retention_lineage(session) WHERE expired_at_ms IS NULL;
             CREATE TABLE IF NOT EXISTS message (
                 session TEXT NOT NULL,
                 ord     BIGINT NOT NULL,
                 json    TEXT NOT NULL,
                 PRIMARY KEY (session, ord)
             );
              CREATE TABLE IF NOT EXISTS run_index (
                  run_id                        TEXT PRIMARY KEY,
                  session                       TEXT NOT NULL,
                  submission_id                 TEXT,
                  request_identity_digest       TEXT,
                  state                         TEXT NOT NULL,
                  usage_json                    TEXT,
                  final_step_finish_reason_json TEXT,
                  retries_used                  BIGINT NOT NULL DEFAULT 0
              );
               ALTER TABLE meta ADD COLUMN IF NOT EXISTS lineage_id TEXT;
               ALTER TABLE meta ADD COLUMN IF NOT EXISTS control_head_wal_seq BIGINT;
               ALTER TABLE meta ADD COLUMN IF NOT EXISTS control_head_sub_index BIGINT;
               ALTER TABLE meta ADD COLUMN IF NOT EXISTS retries_used BIGINT NOT NULL DEFAULT 0;
               ALTER TABLE meta ADD COLUMN IF NOT EXISTS last_step_usage_json TEXT;
              ALTER TABLE meta ADD COLUMN IF NOT EXISTS last_step_finish_reason_json TEXT;
               ALTER TABLE meta ADD COLUMN IF NOT EXISTS last_run_id TEXT;
              ALTER TABLE meta ADD COLUMN IF NOT EXISTS last_episode_usage_json TEXT;
              ALTER TABLE meta ADD COLUMN IF NOT EXISTS wal_anchor_file_id TEXT;
             ALTER TABLE meta ADD COLUMN IF NOT EXISTS wal_anchor_seq BIGINT;
             ALTER TABLE meta ADD COLUMN IF NOT EXISTS wal_anchor_byte_end BIGINT;
               ALTER TABLE meta ADD COLUMN IF NOT EXISTS prefix_len BIGINT;
               ALTER TABLE meta ADD COLUMN IF NOT EXISTS admission_checkpoint_json TEXT;
               ALTER TABLE meta ADD COLUMN IF NOT EXISTS last_title TEXT;
                 ALTER TABLE meta ADD COLUMN IF NOT EXISTS deferred_calls_json TEXT;
                 ALTER TABLE meta ADD COLUMN IF NOT EXISTS run_attribution_version BIGINT NOT NULL DEFAULT 0;
                 ALTER TABLE message ADD COLUMN IF NOT EXISTS run_json TEXT;
               ALTER TABLE run_index ADD COLUMN IF NOT EXISTS usage_json TEXT;
               ALTER TABLE run_index ADD COLUMN IF NOT EXISTS final_step_finish_reason_json TEXT;
               ALTER TABLE run_index ADD COLUMN IF NOT EXISTS request_identity_digest TEXT;
                ALTER TABLE run_index ADD COLUMN IF NOT EXISTS retries_used BIGINT NOT NULL DEFAULT 0;
                ALTER TABLE run_index ADD COLUMN IF NOT EXISTS state_changed_ms BIGINT;
                 ALTER TABLE run_index ADD COLUMN IF NOT EXISTS lineage_id TEXT;
                 ALTER TABLE run_index ADD COLUMN IF NOT EXISTS expired_at_ms BIGINT;
                ALTER TABLE run_index ADD COLUMN IF NOT EXISTS title TEXT;
                 ALTER TABLE run_index ADD COLUMN IF NOT EXISTS withdrawals_owed BOOLEAN NOT NULL DEFAULT FALSE;",
         )?;
        let (source_instance_id, source_epoch, high_water) = export_index::init_schema(client)?;
        Ok((
            source_instance_id,
            u64::try_from(source_epoch)?,
            u64::try_from(high_water)?,
        ))
    }

    fn key(session: &BindIdentity) -> String {
        format!(
            "{}\u{1f}{}\u{1f}{}",
            session.project_root.display(),
            session.harness,
            session.session
        )
    }

    /// Project the newest replay-safe prefix from either a complete WAL read or a seeded suffix.
    pub fn project(
        &self,
        session: &BindIdentity,
        records: &[FramedRecord],
    ) -> Result<u64, StoreError> {
        self.project_records(session, records, None)
    }

    fn project_records(
        &self,
        session: &BindIdentity,
        records: &[FramedRecord],
        range: Option<&WalRangeRead>,
    ) -> Result<u64, StoreError> {
        let key = Self::key(session);
        for _ in 0..OPTIMISTIC_PROJECTION_ATTEMPTS {
            let snapshot = self.projection_snapshot(&key)?;
            let plan = Self::prepare_projection(snapshot, records, range)?;
            match self.try_commit_projection(&key, plan)? {
                ProjectionCommit::Applied(watermark) => return Ok(watermark),
                ProjectionCommit::Conflict => continue,
            }
        }

        // The served path has one writer-scoped, enqueue-coalesced projector per session, and
        // imports are serialized through that session's actor. Bound retries for direct store
        // callers that violate that discipline, then compute under the lock to guarantee progress.
        self.project_records_locked(&key, records, range)
    }

    fn projection_snapshot(&self, key: &str) -> Result<ProjectionSnapshot, StoreError> {
        let mut client = self.client.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .start()?;
        let snapshot = Self::projection_snapshot_from_tx(&mut tx, key)?;
        tx.commit()?;
        Ok(snapshot)
    }

    fn projection_snapshot_from_tx(
        tx: &mut Transaction<'_>,
        key: &str,
    ) -> Result<ProjectionSnapshot, StoreError> {
        let seed = projection_seed_from_tx(tx, key)?;
        let (watermark, next_ord) = current_projection_position(tx, key)?;
        let lineage_id = current_projection_lineage_id(tx, key)?;
        let previous_control_head = current_control_head(tx, key)?;
        Ok(ProjectionSnapshot {
            attribution_complete: attribution_complete(tx, key)?,
            seed,
            watermark,
            lineage_id,
            next_ord,
            previous_control_head,
        })
    }

    fn prepare_projection(
        snapshot: ProjectionSnapshot,
        records: &[FramedRecord],
        range: Option<&WalRangeRead>,
    ) -> Result<ProjectionPlan, StoreError> {
        use broca_core::projection_plan::ProjectionPlan as NeutralPlan;
        Ok(
            match broca_core::projection_plan::plan_projection(snapshot, records, range)? {
                NeutralPlan::NoChange {
                    expected_watermark,
                    expected_lineage_id,
                } => ProjectionPlan::NoChange {
                    expected_watermark,
                    expected_lineage_id,
                },
                NeutralPlan::Reset {
                    expected_watermark,
                    expected_lineage_id,
                } => ProjectionPlan::Reset {
                    expected_watermark,
                    expected_lineage_id,
                },
                NeutralPlan::RetentionMetadata {
                    expected_watermark,
                    expected_lineage_id,
                    metadata,
                } => ProjectionPlan::RetentionMetadata {
                    expected_watermark,
                    expected_lineage_id,
                    metadata,
                },
                NeutralPlan::Write(prepared) => {
                    ProjectionPlan::Write(Box::new(PreparedProjection {
                        expected_watermark: prepared.expected_watermark,
                        expected_lineage_id: prepared.expected_lineage_id,
                        reset: prepared.reset,
                        lineage_id: prepared.lineage_id,
                        new_watermark: prepared.new_watermark,
                        next_ord: prepared.next_ord,
                        truncate_prefix_to: prepared.truncate_prefix_to,
                        message_json: prepared.message_json,
                        message_run_json: prepared.message_run_json,
                        final_run_json: prepared.final_run_json,
                        completed_steps: prepared.completed_steps,
                        usage_json: prepared.usage_json,
                        retries_used: prepared.retries_used,
                        last_step_usage_json: prepared.last_step_usage_json,
                        last_step_finish_reason_json: prepared.last_step_finish_reason_json,
                        last_run_id: prepared.last_run_id,
                        last_title: prepared.last_title,
                        last_episode_usage_json: prepared.last_episode_usage_json,
                        checkpoint_json: prepared.checkpoint_json,
                        deferred_calls_json: prepared.deferred_calls_json,
                        withdrawals_owed_runs: prepared.withdrawals_owed_runs,
                        anchor_file_id: prepared
                            .anchor
                            .as_ref()
                            .map(|anchor| anchor.file_id.to_string()),
                        anchor_seq: prepared
                            .anchor
                            .as_ref()
                            .map(|anchor| broca_core::projection_plan::u64_to_i64(anchor.seq))
                            .transpose()?,
                        anchor_byte_end: prepared
                            .anchor
                            .as_ref()
                            .map(|anchor| broca_core::projection_plan::u64_to_i64(anchor.byte_end))
                            .transpose()?,
                        new_watermark_i64: broca_core::projection_plan::u64_to_i64(
                            prepared.new_watermark,
                        )?,
                        control_head_wal_seq: prepared
                            .control_head
                            .map(|cursor| broca_core::projection_plan::u64_to_i64(cursor.wal_seq))
                            .transpose()?,
                        control_head_sub_index: prepared
                            .control_head
                            .map(|cursor| i64::from(cursor.sub_index)),
                    }))
                }
            },
        )
    }

    fn try_commit_projection(
        &self,
        key: &str,
        plan: ProjectionPlan,
    ) -> Result<ProjectionCommit, StoreError> {
        let mut client = self.client.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut tx = client.transaction()?;
        let current_state = current_projection_state(&mut tx, key)?;
        let conflict = {
            let expected_state = plan.expected_state();
            current_state.0 != expected_state.0 || current_state.1.as_deref() != expected_state.1
        };
        if conflict {
            tx.commit()?;
            return Ok(ProjectionCommit::Conflict);
        }
        Self::commit_projection_plan(tx, key, plan).map(ProjectionCommit::Applied)
    }

    fn project_records_locked(
        &self,
        key: &str,
        records: &[FramedRecord],
        range: Option<&WalRangeRead>,
    ) -> Result<u64, StoreError> {
        let mut client = self.client.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut tx = client.transaction()?;
        let snapshot = Self::projection_snapshot_from_tx(&mut tx, key)?;
        let plan = Self::prepare_projection(snapshot, records, range)?;
        Self::commit_projection_plan(tx, key, plan)
    }

    fn commit_projection_plan(
        mut tx: Transaction<'_>,
        key: &str,
        plan: ProjectionPlan,
    ) -> Result<u64, StoreError> {
        let prepared = match plan {
            ProjectionPlan::NoChange {
                expected_watermark, ..
            } => {
                tx.commit()?;
                return Ok(expected_watermark);
            }
            ProjectionPlan::Reset { .. } => {
                tx.execute("DELETE FROM message WHERE session = $1", &[&key])?;
                tx.execute("DELETE FROM meta WHERE session = $1", &[&key])?;
                tx.commit()?;
                return Ok(0);
            }
            ProjectionPlan::RetentionMetadata {
                expected_watermark,
                metadata,
                ..
            } => {
                Self::commit_retention_metadata(&mut tx, key, &metadata)?;
                tx.commit()?;
                return Ok(expected_watermark);
            }
            ProjectionPlan::Write(prepared) => *prepared,
        };

        if prepared.reset {
            tx.execute("DELETE FROM message WHERE session = $1", &[&key])?;
            tx.execute("DELETE FROM meta WHERE session = $1", &[&key])?;
        } else if let Some(truncate_to) = prepared.truncate_prefix_to {
            tx.execute(
                "DELETE FROM message WHERE session = $1 AND ord >= $2",
                &[&key, &truncate_to],
            )?;
        }
        let mut next_ord = prepared.next_ord;
        for (json_batch, run_batch) in prepared
            .message_json
            .chunks(MESSAGE_INSERT_BATCH_SIZE)
            .zip(prepared.message_run_json.chunks(MESSAGE_INSERT_BATCH_SIZE))
        {
            let batch_len = i64::try_from(json_batch.len())?;
            let end_ord =
                next_ord
                    .checked_add(batch_len)
                    .ok_or(StoreError::InconsistentSnapshot(
                        "projected message ordinal overflow",
                    ))?;
            let ordinals: Vec<i64> = (next_ord..end_ord).collect();
            tx.execute(
                "INSERT INTO message (session, ord, json, run_json)
                 SELECT $1, batch.ord, batch.json, batch.run_json
                   FROM UNNEST($2::BIGINT[], $3::TEXT[], $4::TEXT[]) AS batch(ord, json, run_json)
                 ON CONFLICT (session, ord) DO UPDATE SET json = EXCLUDED.json, run_json = EXCLUDED.run_json",
                &[&key, &ordinals, &json_batch, &run_batch],
            )?;
            next_ord = end_ord;
        }
        for (ordinal, run_json) in prepared.final_run_json {
            let run: broca_core::RunAttribution = serde_json::from_str(&run_json)?;
            tx.execute("UPDATE message SET run_json=$3 WHERE session=$1 AND ord=$2 AND (run_json::jsonb)->>'run_id'=$4", &[&key, &ordinal, &run_json, &run.run_id])?;
        }

        tx.execute(
            "INSERT INTO meta (
                  session, watermark_seq, lineage_id, context_epoch, completed_steps, usage_json,
                  retries_used, last_step_usage_json, last_step_finish_reason_json, last_run_id,
                  last_episode_usage_json, prefix_len, admission_checkpoint_json, wal_anchor_file_id,
                  wal_anchor_seq, wal_anchor_byte_end, control_head_wal_seq, control_head_sub_index,
                  last_title
                ) VALUES ($1, $2, $3, 0, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                ON CONFLICT (session) DO UPDATE SET
                     watermark_seq = EXCLUDED.watermark_seq,
                     lineage_id = EXCLUDED.lineage_id,
                     completed_steps = EXCLUDED.completed_steps,
                     usage_json = EXCLUDED.usage_json,
                     retries_used = EXCLUDED.retries_used,
                     last_step_usage_json = EXCLUDED.last_step_usage_json,
                     last_step_finish_reason_json = EXCLUDED.last_step_finish_reason_json,
                     last_run_id = EXCLUDED.last_run_id,
                     last_episode_usage_json = EXCLUDED.last_episode_usage_json,
                     prefix_len = EXCLUDED.prefix_len,
                     admission_checkpoint_json = EXCLUDED.admission_checkpoint_json,
                     wal_anchor_file_id = EXCLUDED.wal_anchor_file_id,
                     wal_anchor_seq = EXCLUDED.wal_anchor_seq,
                     wal_anchor_byte_end = EXCLUDED.wal_anchor_byte_end,
                     control_head_wal_seq = EXCLUDED.control_head_wal_seq,
                     control_head_sub_index = EXCLUDED.control_head_sub_index,
                     last_title = COALESCE(EXCLUDED.last_title, meta.last_title)",
            &[
                &key,
                &prepared.new_watermark_i64,
                &prepared.lineage_id,
                &prepared.completed_steps,
                &prepared.usage_json,
                &prepared.retries_used,
                &prepared.last_step_usage_json,
                &prepared.last_step_finish_reason_json,
                &prepared.last_run_id,
                &prepared.last_episode_usage_json,
                &next_ord,
                &prepared.checkpoint_json,
                &prepared.anchor_file_id,
                &prepared.anchor_seq,
                &prepared.anchor_byte_end,
                &prepared.control_head_wal_seq,
                &prepared.control_head_sub_index,
                // A batch with no title keeps the existing `last_title`. A projection reset (the
                // WAL's lineage changed) deleted the session's meta row above before reprojecting
                // from sequence one, so this insert starts from no title and nothing stale survives.
                &prepared.last_title,
            ],
        )?;
        tx.execute(
            "UPDATE meta SET deferred_calls_json=$2, run_attribution_version=1 WHERE session=$1",
            &[&key, &prepared.deferred_calls_json],
        )?;
        for run_id in prepared.withdrawals_owed_runs {
            tx.execute(
                "UPDATE run_index SET withdrawals_owed=TRUE WHERE run_id=$1",
                &[&run_id],
            )?;
        }
        tx.commit()?;
        Ok(prepared.new_watermark)
    }

    fn commit_retention_metadata(
        tx: &mut Transaction<'_>,
        key: &str,
        metadata: &broca_core::projection_plan::RetainedWithdrawalProjection,
    ) -> Result<(), StoreError> {
        let session = bind_identity_from_store_key(key).ok_or(StoreError::InconsistentSnapshot(
            "withdrawal metadata requires a session triple",
        ))?;
        let session_json = serde_json::to_string(&session)?;
        for run_id in &metadata.withdrawals_owed_runs {
            tx.execute(
                "UPDATE run_index SET withdrawals_owed=TRUE WHERE run_id=$1 AND session=$2
                 AND (lineage_id=$3 OR lineage_id IS NULL)",
                &[run_id, &session_json, &metadata.lineage_id],
            )?;
        }
        Ok(())
    }

    fn reproject_if_lineage_mismatch_inner(
        &self,
        session: &BindIdentity,
        stale_lineage_id: &str,
        complete_wal: &WalRangeRead,
    ) -> Result<(), StoreError> {
        let incoming_lineage_id = complete_wal.lineage_id.map(|id| id.to_string());
        if incoming_lineage_id
            .as_deref()
            .is_none_or(|id| id == stale_lineage_id)
        {
            return Err(StoreError::InconsistentSnapshot(
                "lineage repair requires a different identified WAL",
            ));
        }

        let key = Self::key(session);
        for _ in 0..OPTIMISTIC_PROJECTION_ATTEMPTS {
            let snapshot = self.projection_snapshot(&key)?;
            if snapshot.lineage_id.as_deref() != Some(stale_lineage_id) {
                return Ok(());
            }
            let plan = Self::prepare_projection(
                snapshot,
                match complete_wal.records() {
                    broca_wal::WalRangeRecords::Complete(records)
                    | broca_wal::WalRangeRecords::Partial(records)
                    | broca_wal::WalRangeRecords::Unknown(records) => records,
                    broca_wal::WalRangeRecords::Absent => &[],
                },
                Some(complete_wal),
            )?;
            match self.try_commit_projection(&key, plan)? {
                ProjectionCommit::Applied(_) => return Ok(()),
                ProjectionCommit::Conflict => continue,
            }
        }

        let mut client = self.client.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut tx = client.transaction()?;
        let snapshot = Self::projection_snapshot_from_tx(&mut tx, &key)?;
        if snapshot.lineage_id.as_deref() != Some(stale_lineage_id) {
            tx.commit()?;
            return Ok(());
        }
        let plan = Self::prepare_projection(
            snapshot,
            match complete_wal.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
            Some(complete_wal),
        )?;
        Self::commit_projection_plan(tx, &key, plan)?;
        Ok(())
    }

    fn try_read_prefix_with_watermark(
        &self,
        session: &BindIdentity,
    ) -> Result<StorePrefix, StoreError> {
        let key = Self::key(session);
        let mut client = self.client.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()?;

        let meta = tx
            .query_opt(
                "SELECT watermark_seq, context_epoch, completed_steps, usage_json,
                   COALESCE(retries_used, 0), last_step_usage_json, last_step_finish_reason_json
                  FROM meta WHERE session = $1",
                &[&key],
            )?
            .map(|row| {
                Ok::<_, StoreError>((
                    i64_to_u64(row.get::<_, i64>(0))?,
                    i64_to_u64(row.get::<_, i64>(1))?,
                    i64_to_u64(row.get::<_, i64>(2))?,
                    row.get::<_, String>(3),
                    row.get::<_, i64>(4),
                    row.get::<_, Option<String>>(5),
                    row.get::<_, Option<String>>(6),
                ))
            })
            .transpose()?;

        let encoded_prefix = read_message_json(&mut tx, &key)?;
        tx.commit()?;
        drop(client);

        // Match projection's short critical section: the transaction fixes and copies the
        // snapshot, while whole-lineage JSON decoding happens after releasing the shared
        // connection used by every session.
        let prefix = decode_messages(encoded_prefix)?;
        let snapshot = match meta {
            Some((
                watermark_seq,
                context_epoch,
                completed_steps,
                usage_json,
                retries_used,
                last_step_usage_json,
                last_step_finish_reason_json,
            )) => StorePrefix {
                prefix,
                completed_steps,
                usage: serde_json::from_str(&usage_json)?,
                retries_used: u32::try_from(retries_used)?,
                last_step_usage: last_step_usage_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?,
                last_step_finish_reason: last_step_finish_reason_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?,
                watermark_seq,
                context_epoch,
            },
            None if prefix.is_empty() => StorePrefix::empty(),
            None => {
                return Err(StoreError::InconsistentSnapshot(
                    "store snapshot is missing the meta row for materialized messages",
                ))
            }
        };
        Ok(snapshot)
    }

    fn try_read_admission_snapshot(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<AdmissionStoreSnapshot>, StoreError> {
        let key = Self::key(session);
        let mut client = self.client.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()?;
        let snapshot = tx
            .query_opt(
                "SELECT watermark_seq, lineage_id, wal_anchor_file_id, wal_anchor_seq, wal_anchor_byte_end,
                 admission_checkpoint_json FROM meta WHERE session = $1",
                &[&key],
            )?
            .map(|row| {
                let watermark_seq = i64_to_u64(row.get::<_, i64>(0))?;
                Ok::<_, StoreError>(AdmissionStoreSnapshot {
                    watermark_seq,
                    lineage_id: row.get::<_, Option<String>>(1),
                    anchor: wal_anchor(
                        watermark_seq,
                        row.get::<_, Option<String>>(2),
                        row.get::<_, Option<i64>>(3),
                        row.get::<_, Option<i64>>(4),
                    ),
                    checkpoint: row
                        .get::<_, Option<String>>(5)
                        .as_deref()
                        .and_then(decode_admission_checkpoint),
                })
            })
            .transpose()?;
        tx.commit()?;
        Ok(snapshot)
    }

    fn try_read_page_with_watermark(
        &self,
        session: &BindIdentity,
        from_ordinal: Option<u64>,
        limit: u64,
        view: broca_core::context::ReadView,
    ) -> Result<StorePageSnapshot, StoreError> {
        let key = Self::key(session);
        let mut client = self.client.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .start()?;
        let (watermark_seq, prefix_len) = current_projection_position(&mut tx, &key)?;
        if !attribution_complete(&mut tx, &key)? {
            return Err(StoreError::AttributionStale);
        }
        let control_head = current_control_head(&mut tx, &key)?;
        if watermark_seq > 0 && control_head.is_none() {
            return Err(StoreError::InconsistentSnapshot(
                "store page metadata predates control-head tracking",
            ));
        }
        let prefix_len = i64_to_u64(prefix_len)?;
        let meta = tx
            .query_opt(
                "SELECT lineage_id, last_run_id, wal_anchor_file_id, wal_anchor_seq, wal_anchor_byte_end, \
                  completed_steps, last_episode_usage_json, \
                  CASE WHEN last_episode_usage_json IS NULL THEN admission_checkpoint_json END \
                  FROM meta WHERE session = $1",
                &[&key],
            )?
            .map(|row| {
                Ok::<_, StoreError>((
                    row.get::<_, Option<String>>(0),
                    row.get::<_, Option<String>>(1),
                    wal_anchor(
                        watermark_seq,
                        row.get::<_, Option<String>>(2),
                        row.get::<_, Option<i64>>(3),
                        row.get::<_, Option<i64>>(4),
                    ),
                    i64_to_u64(row.get::<_, i64>(5))?,
                    row.get::<_, Option<String>>(6),
                    row.get::<_, Option<String>>(7),
                ))
            })
            .transpose()?;
        let (lineage_id, last_run_id, anchor, completed_steps, last_episode_usage) = match meta {
            Some((_lineage_id, None, _anchor, _, _, _)) if prefix_len > 0 => {
                return Err(StoreError::InconsistentSnapshot(
                    "store page metadata predates last-run tracking",
                ))
            }
            Some((
                lineage_id,
                run_id,
                anchor,
                completed_steps,
                usage_json,
                fallback_checkpoint_json,
            )) => {
                let usage = match usage_json {
                    Some(json) => serde_json::from_str::<Option<Usage>>(&json)?,
                    None => fallback_checkpoint_json
                        .as_deref()
                        .and_then(decode_admission_checkpoint)
                        .and_then(|checkpoint| checkpoint.last_episode)
                        .filter(|episode| run_id.as_deref() == Some(episode.run_id.0.as_str()))
                        .and_then(|episode| episode.usage),
                };
                (
                    lineage_id,
                    run_id.map(broca_protocol::RunId),
                    anchor,
                    completed_steps,
                    usage,
                )
            }
            None if prefix_len == 0 => (None, None, None, 0, None),
            None => {
                return Err(StoreError::InconsistentSnapshot(
                    "store snapshot is missing the meta row for materialized messages",
                ))
            }
        };

        let last_title = tx
            .query_opt("SELECT last_title FROM meta WHERE session = $1", &[&key])?
            .and_then(|row| row.get::<_, Option<String>>(0));

        let start = from_ordinal.unwrap_or_else(|| prefix_len.saturating_sub(limit));
        let end = start.saturating_add(limit).min(prefix_len);
        let start_i64 = i64::try_from(start).unwrap_or(i64::MAX);
        let end_i64 = i64::try_from(end).unwrap_or(i64::MAX);
        let encoded = tx.query(
            "SELECT ord, json, run_json FROM message
             WHERE session = $1 AND ord >= $2 AND ord < $3 ORDER BY ord ASC",
            &[&key, &start_i64, &end_i64],
        )?;
        let mut rows = Vec::with_capacity(encoded.len());
        for row in encoded {
            let ordinal = i64_to_u64(row.get::<_, i64>(0))?;
            let mut message: Message = serde_json::from_str(&row.get::<_, String>(1))?;
            message.ordinal = Some(ordinal);
            let run_json = row
                .get::<_, Option<String>>(2)
                .ok_or(StoreError::AttributionStale)?;
            let run = serde_json::from_str(&run_json)?;
            rows.push(StorePageRow {
                ordinal,
                message,
                run,
            });
        }

        // A projectable boundary ends in an assistant message or one consolidated tool
        // message, so the producing assistant origin is always among the final two rows.
        // Use the origin decoded from the final two rows only for the page that contains the
        // projection boundary. For earlier pages, read the actual final origin so replay starts
        // from the projection boundary rather than an intermediate page.
        let trailing_origin = if end == prefix_len {
            rows.iter()
                .rev()
                .take(2)
                .find_map(|row| row.message.origin.clone())
        } else {
            None
        };
        let trailing_origin = match trailing_origin {
            Some(origin) => Some(origin),
            None => trailing_origin_from_tx(&mut tx, &key)?,
        };
        let deferred_calls_json = tx
            .query_opt(
                "SELECT deferred_calls_json FROM meta WHERE session=$1",
                &[&key],
            )?
            .and_then(|row| row.get::<_, Option<String>>(0));
        if watermark_seq > 0 && deferred_calls_json.is_none() {
            return Err(StoreError::InconsistentSnapshot(
                "store page metadata predates approval tracking",
            ));
        }
        let deferred_calls = deferred_calls_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?
            .flatten();
        // Only model pages need the potentially large replacement payload. Decode it in the
        // same transaction as the rows so the view cannot come from a newer projection.
        let compaction = if view == broca_core::context::ReadView::Model {
            tx.query_opt(
                "SELECT admission_checkpoint_json FROM meta WHERE session = $1",
                &[&key],
            )?
            .and_then(|row| row.get::<_, Option<String>>(0))
            .as_deref()
            .and_then(decode_admission_checkpoint)
            .filter(|checkpoint| checkpoint.at_seq == watermark_seq)
            .and_then(|checkpoint| checkpoint.compaction)
        } else {
            None
        };
        tx.commit()?;
        Ok(StorePageSnapshot {
            deferred_calls,
            compaction,
            watermark_seq,
            lineage_id,
            control_head,
            anchor,
            prefix_len,
            completed_steps,
            rows,
            trailing_origin,
            last_run_id,
            last_episode_usage,
            last_title,
        })
    }
}

impl MaterializedStore for PostgresStore {
    fn read_page_with_attribution(
        &self,
        session: &BindIdentity,
        from_ordinal: Option<u64>,
        limit: u64,
        view: broca_core::ReadView,
    ) -> Result<StorePageSnapshot, broca_core::StorePageReadError> {
        self.try_read_page_with_watermark(session, from_ordinal, limit, view)
            .map_err(|error| match error {
                StoreError::AttributionStale => broca_core::StorePageReadError::AttributionStale,
                error => broca_core::StorePageReadError::Failure(error.to_string()),
            })
    }
    fn read_prefix_with_watermark(&self, session: &BindIdentity) -> Result<StorePrefix, String> {
        self.try_read_prefix_with_watermark(session)
            .map_err(|e| e.to_string())
    }

    fn read_admission_snapshot(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<AdmissionStoreSnapshot>, String> {
        self.try_read_admission_snapshot(session)
            .map_err(|error| error.to_string())
    }

    fn read_page_with_watermark(
        &self,
        session: &BindIdentity,
        from_ordinal: Option<u64>,
        limit: u64,
        view: broca_core::context::ReadView,
    ) -> Result<StorePageSnapshot, String> {
        self.try_read_page_with_watermark(session, from_ordinal, limit, view)
            .map_err(|e| e.to_string())
    }

    fn reproject_if_lineage_mismatch(
        &self,
        session: &BindIdentity,
        stale_lineage_id: &str,
        complete_wal: &WalRangeRead,
    ) -> Result<(), String> {
        self.reproject_if_lineage_mismatch_inner(session, stale_lineage_id, complete_wal)
            .map_err(|error| error.to_string())
    }
}

impl Persister for PostgresStore {
    fn project_retention_metadata(
        &self,
        session: &BindIdentity,
        metadata: &broca_core::projection_plan::RetainedWithdrawalProjection,
    ) -> Result<(), String> {
        let mut client = self.client.lock().unwrap();
        let mut tx = client.transaction().map_err(|error| error.to_string())?;
        Self::commit_retention_metadata(&mut tx, &Self::key(session), metadata)
            .map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())
    }
    fn projection_seed(&self, session: &BindIdentity) -> Result<Option<ProjectionSeed>, String> {
        let key = Self::key(session);
        let mut client = self
            .client
            .lock()
            .map_err(|_| StoreError::LockPoisoned)
            .map_err(|error| error.to_string())?;
        let mut tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(|error| error.to_string())?;
        let seed = projection_seed_from_tx(&mut tx, &key).map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())?;
        Ok(seed)
    }

    fn project(&self, session: &BindIdentity, records: &[FramedRecord]) -> Result<u64, String> {
        PostgresStore::project(self, session, records).map_err(|e| e.to_string())
    }

    fn project_range(&self, session: &BindIdentity, range: &WalRangeRead) -> Result<u64, String> {
        self.project_records(
            session,
            match range.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
            Some(range),
        )
        .map_err(|error| error.to_string())
    }

    fn persist_wal_anchor(
        &self,
        session: &BindIdentity,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<(), String> {
        let Some(anchor) = anchor else {
            return Ok(());
        };
        let seq = u64_to_i64(anchor.seq).map_err(|error| error.to_string())?;
        let byte_end = u64_to_i64(anchor.byte_end).map_err(|error| error.to_string())?;
        let key = Self::key(session);
        let file_id = anchor.file_id.to_string();
        let mut client = self
            .client
            .lock()
            .map_err(|_| StoreError::LockPoisoned)
            .map_err(|error| error.to_string())?;
        client
            .execute(
                "UPDATE meta SET wal_anchor_file_id=$2, wal_anchor_seq=$3, wal_anchor_byte_end=$4 \
                 WHERE session=$1 AND watermark_seq=$3",
                &[&key, &file_id, &seq, &byte_end],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl RunIndex for PostgresStore {
    fn index_run_active(&self, row: &RunIndexRow) -> Result<(), String> {
        let session_json = serde_json::to_string(&row.session).map_err(|e| e.to_string())?;
        let usage_json = row
            .usage
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| e.to_string())?;
        let finish_reason_json = row
            .final_step_finish_reason
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| e.to_string())?;
        // postgres has no ToSql for u32; bind as BIGINT.
        let retries_used = i64::from(row.retries_used);
        let mut client = self.client.lock().unwrap();
        client
            .execute(
                "INSERT INTO run_index \
                     (run_id, session, submission_id, request_identity_digest, state, usage_json, \
                       final_step_finish_reason_json, retries_used, state_changed_ms, title, withdrawals_owed) \
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
                 ON CONFLICT (run_id) DO UPDATE SET \
                    session = EXCLUDED.session, submission_id = EXCLUDED.submission_id, \
                      request_identity_digest = COALESCE(EXCLUDED.request_identity_digest, run_index.request_identity_digest), state = EXCLUDED.state, usage_json = EXCLUDED.usage_json, \
                     final_step_finish_reason_json = EXCLUDED.final_step_finish_reason_json, retries_used = EXCLUDED.retries_used, \
                     state_changed_ms = EXCLUDED.state_changed_ms, \
                     title = COALESCE(run_index.title, EXCLUDED.title), withdrawals_owed = run_index.withdrawals_owed OR EXCLUDED.withdrawals_owed \
                  WHERE run_index.state IN ('active', 'paused')",
                &[
                    &row.run_id,
                    &session_json,
                    &row.submission_id,
                    &row.request_identity_digest,
                    &row.state.as_str(),
                    &usage_json,
                     &finish_reason_json,
                     &retries_used,
                     &now_epoch_ms(),
                      &row.title,
                      &row.withdrawals_owed,
                 ],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn index_run_terminal(
        &self,
        run_id: &str,
        state: RunIndexState,
        usage: Option<Usage>,
        final_step_finish_reason: Option<FinishReason>,
        retries_used: u32,
    ) -> Result<(), String> {
        let usage_json = usage
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| e.to_string())?;
        let finish_reason_json = final_step_finish_reason
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| e.to_string())?;
        // postgres has no ToSql for u32; bind as BIGINT.
        let retries_used = i64::from(retries_used);
        let mut client = self.client.lock().unwrap();
        client
            .execute(
                "UPDATE run_index \
                  SET state = $2, usage_json = $3, final_step_finish_reason_json = $4, retries_used = $5, \
                      state_changed_ms = $6 \
                  WHERE run_id = $1",
                 &[&run_id, &state.as_str(), &usage_json, &finish_reason_json, &retries_used, &now_epoch_ms()],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn index_run_withdrawals_owed(&self, run_id: &str, owed: bool) -> Result<(), String> {
        let mut client = self.client.lock().map_err(|e| e.to_string())?;
        client
            .execute(
                "UPDATE run_index SET withdrawals_owed=$2 WHERE run_id=$1",
                &[&run_id, &owed],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn runs_with_withdrawals_owed(&self) -> Result<Vec<broca_core::WithdrawalRun>, String> {
        let mut client = self.client.lock().map_err(|e| e.to_string())?;
        client.query("SELECT run_id, session FROM run_index WHERE withdrawals_owed=TRUE ORDER BY run_id ASC", &[])
            .map_err(|e| e.to_string())?.into_iter().map(|row| {
                Ok(broca_core::WithdrawalRun { run_id: broca_protocol::RunId(row.get(0)), session: serde_json::from_str(&row.get::<_, String>(1)).map_err(|e| e.to_string())? })
            }).collect()
    }

    fn lookup_run(&self, run_id: &str) -> Result<Option<RunIndexRow>, String> {
        let mut client = self.client.lock().unwrap();
        let row = client
            .query_opt(
                "SELECT session, submission_id, request_identity_digest, state, usage_json, \
                          final_step_finish_reason_json, retries_used, title, withdrawals_owed \
                       FROM run_index WHERE run_id = $1",
                &[&run_id],
            )
            .map_err(|e| e.to_string())?;
        match row {
            None => Ok(None),
            Some(r) => {
                let session_json: String = r.get(0);
                let submission_id: Option<String> = r.get(1);
                let request_identity_digest: Option<String> = r.get(2);
                let state_str: String = r.get(3);
                let usage_json: Option<String> = r.get(4);
                let finish_reason_json: Option<String> = r.get(5);
                let retries_used: i64 = r.get(6);
                let title: Option<String> = r.get(7);
                let session: BindIdentity =
                    serde_json::from_str(&session_json).map_err(|e| e.to_string())?;
                let state = RunIndexState::parse(&state_str)
                    .ok_or_else(|| format!("unknown run-index state '{state_str}'"))?;
                let usage = usage_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| e.to_string())?;
                let final_step_finish_reason = finish_reason_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| e.to_string())?;
                Ok(Some(RunIndexRow {
                    run_id: run_id.to_string(),
                    session,
                    submission_id,
                    request_identity_digest,
                    state,
                    usage,
                    final_step_finish_reason,
                    retries_used: u32::try_from(retries_used).map_err(|e| e.to_string())?,
                    title,
                    withdrawals_owed: r.get(8),
                }))
            }
        }
    }
}

fn bind_identity_from_store_key(key: &str) -> Option<BindIdentity> {
    let mut fields = key.split('\u{1f}');
    let identity = BindIdentity::new(fields.next()?, fields.next()?, fields.next()?);
    fields.next().is_none().then_some(identity)
}

fn current_control_head(tx: &mut Transaction<'_>, key: &str) -> Result<Option<Cursor>, StoreError> {
    let head = tx.query_opt(
        "SELECT control_head_wal_seq, control_head_sub_index FROM meta WHERE session = $1",
        &[&key],
    )?;
    match head {
        Some(row) => match (row.get::<_, Option<i64>>(0), row.get::<_, Option<i64>>(1)) {
            (Some(wal_seq), Some(sub_index)) => Ok(Some(Cursor::new(
                i64_to_u64(wal_seq)?,
                u32::try_from(sub_index)?,
            ))),
            (None, None) => Ok(None),
            _ => Err(StoreError::InconsistentSnapshot(
                "control head cursor is partially populated",
            )),
        },
        None => Ok(None),
    }
}

fn current_projection_state(
    tx: &mut Transaction<'_>,
    key: &str,
) -> Result<(u64, Option<String>), StoreError> {
    tx.query_opt(
        "SELECT watermark_seq, lineage_id FROM meta WHERE session = $1",
        &[&key],
    )?
    .map(|row| {
        Ok((
            i64_to_u64(row.get::<_, i64>(0))?,
            row.get::<_, Option<String>>(1),
        ))
    })
    .transpose()
    .map(|state| state.unwrap_or((0, None)))
}

fn current_projection_lineage_id(
    tx: &mut Transaction<'_>,
    key: &str,
) -> Result<Option<String>, StoreError> {
    Ok(tx
        .query_opt("SELECT lineage_id FROM meta WHERE session = $1", &[&key])?
        .and_then(|row| row.get::<_, Option<String>>(0)))
}

fn current_projection_position(
    tx: &mut Transaction<'_>,
    key: &str,
) -> Result<(u64, i64), StoreError> {
    let meta = tx.query_opt(
        "SELECT watermark_seq, prefix_len FROM meta WHERE session = $1",
        &[&key],
    )?;
    match meta {
        Some(row) => {
            let watermark = i64_to_u64(row.get::<_, i64>(0))?;
            if let Some(prefix_len) = row.get::<_, Option<i64>>(1) {
                return Ok((watermark, prefix_len));
            }
            let prefix_len = tx
                .query_one("SELECT COUNT(*) FROM message WHERE session = $1", &[&key])?
                .get::<_, i64>(0);
            tx.execute(
                "UPDATE meta SET prefix_len = $2 WHERE session = $1 AND prefix_len IS NULL",
                &[&key, &prefix_len],
            )?;
            Ok((watermark, prefix_len))
        }
        None => {
            let prefix_len = tx
                .query_one("SELECT COUNT(*) FROM message WHERE session = $1", &[&key])?
                .get::<_, i64>(0);
            Ok((0, prefix_len))
        }
    }
}

fn attribution_complete(tx: &mut Transaction<'_>, key: &str) -> Result<bool, StoreError> {
    let version = tx
        .query_opt(
            "SELECT run_attribution_version FROM meta WHERE session=$1",
            &[&key],
        )?
        .map(|row| row.get::<_, i64>(0));
    if version.is_some_and(|version| version != 1) {
        return Ok(false);
    }
    let unknown: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM message WHERE session=$1 AND run_json IS NULL LIMIT 1)",
            &[&key],
        )?
        .get(0);
    Ok(!unknown)
}

fn projection_seed_from_tx(
    tx: &mut Transaction<'_>,
    key: &str,
) -> Result<Option<ProjectionSeed>, StoreError> {
    if !attribution_complete(tx, key)? {
        return Ok(None);
    }
    let (watermark_seq, prefix_len) = current_projection_position(tx, key)?;
    let meta = tx.query_opt(
         "SELECT completed_steps, usage_json, COALESCE(retries_used, 0), last_run_id, wal_anchor_file_id,
          wal_anchor_seq, wal_anchor_byte_end, last_step_usage_json,
          last_step_finish_reason_json, admission_checkpoint_json FROM meta WHERE session = $1",
        &[&key],
    )?;
    let Some(meta) = meta else {
        return Ok((prefix_len == 0).then(ProjectionSeed::empty));
    };
    let Some(admission_checkpoint) = meta
        .get::<_, Option<String>>(9)
        .as_deref()
        .and_then(decode_admission_checkpoint)
        // Checkpoints that did not capture compaction state lack the earlier setup and
        // replacement records; replaying only records after the watermark cannot recover them.
        .filter(|checkpoint| {
            checkpoint.at_seq == watermark_seq && checkpoint.compaction_complete()
        })
    else {
        return Ok(None);
    };
    let checkpoint_run_id = admission_checkpoint
        .last_episode
        .as_ref()
        .map(|episode| episode.run_id.clone());
    let last_run_id = meta.get::<_, Option<String>>(3).map(broca_protocol::RunId);
    if last_run_id != checkpoint_run_id {
        return Ok(None);
    }
    let anchor = wal_anchor(
        watermark_seq,
        meta.get::<_, Option<String>>(4),
        meta.get::<_, Option<i64>>(5),
        meta.get::<_, Option<i64>>(6),
    );
    let trailing_origin = if prefix_len == 0 {
        None
    } else {
        let Some(origin) = trailing_origin_from_tx(tx, key)? else {
            return Ok(None);
        };
        Some(origin)
    };
    let deferred_calls_json = tx
        .query_one(
            "SELECT deferred_calls_json FROM meta WHERE session=$1",
            &[&key],
        )?
        .get::<_, Option<String>>(0);
    let Some(deferred_calls_json) = deferred_calls_json else {
        return Ok(None);
    };
    Ok(Some(ProjectionSeed {
        deferred_calls: serde_json::from_str(&deferred_calls_json)?,
        watermark_seq,
        anchor,
        prefix_len: i64_to_u64(prefix_len)?,
        completed_steps: i64_to_u64(meta.get::<_, i64>(0))?,
        usage: serde_json::from_str(&meta.get::<_, String>(1))?,
        retries_used: u32::try_from(meta.get::<_, i64>(2))?,
        last_step_usage: meta
            .get::<_, Option<String>>(7)
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        last_step_finish_reason: meta
            .get::<_, Option<String>>(8)
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        trailing_origin,
        last_run_id,
        admission_checkpoint,
    }))
}

fn wal_anchor(
    watermark_seq: u64,
    file_id: Option<String>,
    seq: Option<i64>,
    byte_end: Option<i64>,
) -> Option<WalByteAnchor> {
    let anchor = WalByteAnchor {
        file_id: file_id?.parse().ok()?,
        seq: u64::try_from(seq?).ok()?,
        byte_end: u64::try_from(byte_end?).ok()?,
    };
    (anchor.seq == watermark_seq).then_some(anchor)
}

fn trailing_origin_from_tx(
    tx: &mut Transaction<'_>,
    key: &str,
) -> Result<Option<broca_protocol::request::ModelHandle>, StoreError> {
    let rows = tx.query(
        "SELECT json FROM message WHERE session = $1 ORDER BY ord DESC LIMIT 2",
        &[&key],
    )?;
    for row in rows {
        let message: Message = serde_json::from_str(&row.get::<_, String>(0))?;
        if message.origin.is_some() {
            return Ok(message.origin);
        }
    }
    Ok(None)
}

fn read_message_json(tx: &mut Transaction<'_>, key: &str) -> Result<Vec<String>, StoreError> {
    let rows = tx.query(
        "SELECT json FROM message WHERE session = $1 ORDER BY ord ASC",
        &[&key],
    )?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

fn decode_messages(encoded: Vec<String>) -> Result<Vec<Message>, StoreError> {
    encoded
        .into_iter()
        .map(|json| serde_json::from_str(&json).map_err(Into::into))
        .collect()
}

fn u64_to_i64(n: u64) -> Result<i64, StoreError> {
    Ok(i64::try_from(n)?)
}

fn i64_to_u64(n: i64) -> Result<u64, StoreError> {
    Ok(u64::try_from(n)?)
}

#[cfg(test)]
impl broca_store::conformance::PageUsageConformance for PostgresStore {
    fn erase_attribution_for_conformance(
        &self,
        session: &BindIdentity,
        upgraded: bool,
    ) -> Result<(), String> {
        let key = Self::key(session);
        let mut client = self.client.lock().map_err(|e| e.to_string())?;
        let mut tx = client.transaction().map_err(|e| e.to_string())?;
        tx.execute(
            "UPDATE meta SET run_attribution_version=$2 WHERE session=$1",
            &[&key, &i64::from(upgraded)],
        )
        .map_err(|e| e.to_string())?;
        tx.execute("UPDATE message SET run_json=NULL WHERE session=$1 AND ord=(SELECT MAX(ord) FROM message WHERE session=$1)", &[&key]).map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }
    fn reset_projection_for_conformance(&self, session: &BindIdentity) -> Result<(), String> {
        let key = Self::key(session);
        let mut client = self.client.lock().map_err(|e| e.to_string())?;
        let mut tx = client.transaction().map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM message WHERE session=$1", &[&key])
            .map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM meta WHERE session=$1", &[&key])
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }
    fn replace_admission_checkpoint_for_conformance(
        &self,
        session: &BindIdentity,
        checkpoint: &AdmissionCheckpointV1,
    ) -> Result<(), String> {
        let key = Self::key(session);
        let checkpoint_json =
            encode_admission_checkpoint(checkpoint).map_err(|error| error.to_string())?;
        let mut client = self.client.lock().map_err(|error| error.to_string())?;
        let changed = client
            .execute(
                "UPDATE meta SET admission_checkpoint_json = $2 WHERE session = $1",
                &[&key, &checkpoint_json],
            )
            .map_err(|error| error.to_string())?;
        (changed == 1)
            .then_some(())
            .ok_or_else(|| "conformance session metadata is missing".to_string())
    }

    fn replace_admission_checkpoint_json_for_conformance(
        &self,
        session: &BindIdentity,
        checkpoint_json: Option<&str>,
    ) -> Result<(), String> {
        let key = Self::key(session);
        let mut client = self.client.lock().map_err(|error| error.to_string())?;
        let changed = client
            .execute(
                "UPDATE meta SET admission_checkpoint_json = $2 WHERE session = $1",
                &[&key, &checkpoint_json],
            )
            .map_err(|error| error.to_string())?;
        (changed == 1)
            .then_some(())
            .ok_or_else(|| "conformance session metadata is missing".to_string())
    }

    fn clear_last_episode_usage_for_conformance(
        &self,
        session: &BindIdentity,
    ) -> Result<(), String> {
        let key = Self::key(session);
        let mut client = self.client.lock().map_err(|error| error.to_string())?;
        let changed = client
            .execute(
                "UPDATE meta SET last_episode_usage_json = NULL WHERE session = $1",
                &[&key],
            )
            .map_err(|error| error.to_string())?;
        (changed == 1)
            .then_some(())
            .ok_or_else(|| "conformance session metadata is missing".to_string())
    }

    fn stored_last_episode_usage_for_conformance(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<String>, String> {
        let key = Self::key(session);
        let mut client = self.client.lock().map_err(|error| error.to_string())?;
        client
            .query_one(
                "SELECT last_episode_usage_json FROM meta WHERE session = $1",
                &[&key],
            )
            .map(|row| row.get(0))
            .map_err(|error| error.to_string())
    }

    fn backfill_page_usage_for_conformance(
        &self,
    ) -> Result<broca_store::conformance::PageUsageBackfillCounts, String> {
        PostgresStore::backfill_page_usage(self)
            .map(|counts| broca_store::conformance::PageUsageBackfillCounts {
                scanned: counts.scanned,
                converted: counts.converted,
                empty: counts.empty,
                declined: counts.declined,
            })
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
impl broca_store::conformance::ControlHeadConformance for PostgresStore {
    fn clear_control_head_for_conformance(&self, session: &BindIdentity) -> Result<(), String> {
        let key = Self::key(session);
        let mut client = self.client.lock().map_err(|error| error.to_string())?;
        let changed = client
            .execute(
                "UPDATE meta
                    SET control_head_wal_seq = NULL, control_head_sub_index = NULL
                  WHERE session = $1",
                &[&key],
            )
            .map_err(|error| error.to_string())?;
        (changed == 1)
            .then_some(())
            .ok_or_else(|| "conformance session metadata is missing".to_string())
    }

    fn stored_control_head_for_conformance(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<Cursor>, String> {
        let key = Self::key(session);
        let mut client = self.client.lock().map_err(|error| error.to_string())?;
        let mut tx = client.transaction().map_err(|error| error.to_string())?;
        let head = current_control_head(&mut tx, &key).map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())?;
        Ok(head)
    }

    fn repair_control_heads_for_conformance(
        &self,
        wal_store: &dyn WalStore,
    ) -> Result<broca_store::conformance::ControlHeadRepairCounts, String> {
        PostgresStore::repair_control_heads(self, wal_store)
            .map(|counts| broca_store::conformance::ControlHeadRepairCounts {
                scanned: counts.scanned,
                repaired: counts.repaired,
                declined: counts.declined,
            })
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use broca_core::latest_projection_boundary;
    use broca_protocol::{
        caller::RunFinishReason,
        content::{ContentBlock, Message, Role},
        request::ModelHandle,
        BindIdentity, RunId, Usage,
    };
    use broca_wal::{FramedRecord, RunConfig, RunOrigin, SeedOriginKind, WalRecord};

    use super::{PostgresStore, ProjectionPlan};

    const RUN_COMMAND: &str =
        "DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres cargo test -p broca-store-postgres --features live-postgres -- --nocapture";

    #[cfg(feature = "live-postgres")]
    fn with_billing_floor_store(test: impl FnOnce(&dyn Fn() -> PostgresStore)) {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let url = std::env::var("DATABASE_URL").expect("billing floor tests require DATABASE_URL");
        let schema = format!(
            "billing_floor_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        client
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .unwrap();
        let separator = if url.contains('?') { '&' } else { '?' };
        let isolated = format!("{url}{separator}options=-csearch_path%3D{schema}");
        test(&|| PostgresStore::connect(&isolated).unwrap());
        client
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .unwrap();
    }

    #[cfg(feature = "live-postgres")]
    #[test]
    fn billing_floor_blocks_imported_emission_after_reopen_and_projection_drop() {
        with_billing_floor_store(|make| {
            broca_store::conformance::billing_floor_blocks_emission(make)
        });
    }

    #[cfg(feature = "live-postgres")]
    #[test]
    fn billing_floor_excludes_imported_runs_from_backfill_and_retention() {
        with_billing_floor_store(|make| {
            broca_store::conformance::billing_floor_excludes_backfill(make)
        });
    }

    #[cfg(feature = "live-postgres")]
    #[test]
    fn billing_floor_bills_resumed_local_and_post_activation_runs() {
        with_billing_floor_store(|make| {
            broca_store::conformance::billing_floor_preserves_local_and_new_runs(make)
        });
    }

    #[cfg(feature = "live-postgres")]
    #[test]
    fn retention_and_approval_migrations_commute_and_are_idempotent() {
        use broca_store::conformance::migration_order::{
            common_schema, deferred_schema, retention_schema,
        };
        use std::collections::BTreeMap;
        if !cfg!(feature = "live-postgres") {
            eprintln!("SKIP retention_and_approval_migrations_commute_and_are_idempotent: enable live-postgres and set DATABASE_URL");
            return;
        }
        let url =
            std::env::var("DATABASE_URL").expect("migration compatibility requires DATABASE_URL");

        fn schema(client: &mut postgres::Client) -> BTreeMap<String, String> {
            let mut result = BTreeMap::new();
            for row in client.query("SELECT table_name, column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema=current_schema() ORDER BY table_name, column_name", &[]).unwrap() {
                let table: String = row.get(0);
                let column: String = row.get(1);
                let ty: String = row.get(2);
                let nullable: String = row.get(3);
                let default: Option<String> = row.get(4);
                // Column ordinals retain upgrade history; named definitions must converge.
                result.insert(format!("{table}.{column}"), format!("{ty}/{nullable}/{default:?}"));
            }
            let namespace: String = client
                .query_one("SELECT current_schema()", &[])
                .unwrap()
                .get(0);
            for row in client.query("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname=current_schema() ORDER BY indexname", &[]).unwrap() {
                let name: String = row.get(0);
                let definition: String = row.get(1);
                result.insert(format!("index.{name}"), definition.replace(&format!("{namespace}."), ""));
            }
            result
        }

        let mut layouts = Vec::new();
        for retention_first in [true, false] {
            let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
            client
                .batch_execute(
                    "CREATE TEMP TABLE bootstrap(id BIGINT); SET search_path TO pg_temp;",
                )
                .unwrap();
            client
                .batch_execute(&common_schema("BIGINT", "BIGINT NOT NULL DEFAULT 0"))
                .unwrap();
            if retention_first {
                client.batch_execute(&retention_schema("BIGINT")).unwrap();
            } else {
                client
                    .batch_execute(&deferred_schema("BOOLEAN NOT NULL DEFAULT FALSE"))
                    .unwrap();
                client
                    .execute(
                        "UPDATE run_index SET withdrawals_owed=TRUE WHERE run_id='existing'",
                        &[],
                    )
                    .unwrap();
            }
            PostgresStore::init(&mut client).unwrap();
            let first = schema(&mut client);
            assert_eq!(first["run_index.lineage_id"], "text/YES/None");
            assert_eq!(first["run_index.expired_at_ms"], "bigint/YES/None");
            assert_eq!(
                first["run_index.withdrawals_owed"],
                "boolean/NO/Some(\"false\")"
            );
            assert_eq!(first["meta.deferred_calls_json"], "text/YES/None");
            assert_eq!(
                first
                    .keys()
                    .filter(|key| key.starts_with("retention_lineage."))
                    .count(),
                7
            );
            assert!(
                first["index.retention_lineage_expires_idx"].contains("expires_at_ms IS NOT NULL")
            );
            assert!(first["index.retention_lineage_live_session_idx"].contains("UNIQUE INDEX"));
            assert!(
                first["index.retention_lineage_live_session_idx"].contains("expired_at_ms IS NULL")
            );
            PostgresStore::init(&mut client).unwrap();
            assert_eq!(schema(&mut client), first);
            let row = client
                .query_one(
                    "SELECT title, withdrawals_owed FROM run_index WHERE run_id='existing'",
                    &[],
                )
                .unwrap();
            assert_eq!(row.get::<_, String>(0), "keep this title");
            assert_eq!(row.get::<_, bool>(1), !retention_first);
            if retention_first {
                assert_eq!(client.query_one("SELECT COUNT(*) FROM retention_lineage WHERE lineage_id='old' AND wal_bytes=1024", &[]).unwrap().get::<_, i64>(0), 1);
            }
            layouts.push(first);
        }
        assert_eq!(layouts[0], layouts[1]);
    }

    #[cfg(feature = "live-postgres")]
    #[test]
    fn retention_migrations_and_expired_export_filter() {
        use broca_core::ExportIndex;
        use std::sync::{atomic::AtomicU64, Mutex};

        let url = std::env::var("DATABASE_URL").expect("live-postgres requires DATABASE_URL");
        let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        // A connection-private schema exercises the real migrations without changing shared
        // fixtures or the operator's tables. Closing the connection removes all these tables.
        client
            .batch_execute(
                "CREATE TEMP TABLE retention_fixture_bootstrap (id BIGINT);
             SET search_path TO pg_temp;
             CREATE TABLE run_index (run_id TEXT PRIMARY KEY, session TEXT NOT NULL,
                 submission_id TEXT, state TEXT NOT NULL);",
            )
            .unwrap();
        let session = BindIdentity::new("/retention-migration", "test", "session");
        let session_json = serde_json::to_string(&session).unwrap();
        client
            .execute(
                "INSERT INTO run_index VALUES ('legacy', $1, NULL, 'completed')",
                &[&session_json],
            )
            .unwrap();
        let (source_instance_id, source_epoch, high_water) =
            PostgresStore::init(&mut client).unwrap();
        PostgresStore::init(&mut client).unwrap();
        let columns = client
            .query(
                "SELECT column_name FROM information_schema.columns
             WHERE table_schema=current_schema() AND table_name='retention_lineage'
             ORDER BY ordinal_position",
                &[],
            )
            .unwrap()
            .into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>();
        assert_eq!(
            columns,
            [
                "lineage_id",
                "session",
                "retention_s",
                "last_activity_ms",
                "expires_at_ms",
                "wal_bytes",
                "expired_at_ms"
            ]
        );
        let legacy = client
            .query_one(
                "SELECT lineage_id, expired_at_ms FROM run_index WHERE run_id='legacy'",
                &[],
            )
            .unwrap();
        assert_eq!(legacy.get::<_, Option<String>>(0), None);
        assert_eq!(legacy.get::<_, Option<i64>>(1), None);
        for column in ["lineage_id", "expired_at_ms"] {
            let nullable: String = client
                .query_one(
                    "SELECT is_nullable FROM information_schema.columns
                 WHERE table_schema=current_schema() AND table_name='run_index' AND column_name=$1",
                    &[&column],
                )
                .unwrap()
                .get(0);
            assert_eq!(nullable, "YES");
        }
        let indexes = client
            .query(
                "SELECT indexdef FROM pg_indexes WHERE schemaname=current_schema()
             AND tablename='retention_lineage'",
                &[],
            )
            .unwrap()
            .into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>();
        assert!(indexes
            .iter()
            .any(|s| s.contains("(expires_at_ms)")
                && s.contains("WHERE (expires_at_ms IS NOT NULL)")));
        assert!(indexes.iter().any(|s| s.contains("UNIQUE INDEX")
            && s.contains("(session)")
            && s.contains("WHERE (expired_at_ms IS NULL)")));
        client
            .batch_execute(
                "INSERT INTO retention_lineage VALUES ('a', 'triple', 60, 100, 60100, 1024, NULL);",
            )
            .unwrap();
        assert!(client
            .batch_execute(
                "INSERT INTO retention_lineage VALUES ('b', 'triple', 60, 100, 60100, 1024, NULL);",
            )
            .is_err());
        client.batch_execute(
            "UPDATE retention_lineage SET expired_at_ms=60100, expires_at_ms=NULL WHERE lineage_id='a';
             INSERT INTO retention_lineage VALUES ('b', 'triple', 60, 100, 60100, 1024, NULL);",
        ).unwrap();
        client
            .execute(
                "INSERT INTO run_index (run_id, session, state, lineage_id, expired_at_ms)
             VALUES ('expired', $1, 'completed', 'a', 60100)",
                &[&session_json],
            )
            .unwrap();
        let store = PostgresStore {
            client: Mutex::new(client),
            export_source_instance_id: source_instance_id,
            export_source_epoch: source_epoch,
            export_high_water: AtomicU64::new(high_water),
        };
        let missing = store.missing_terminal_runs().unwrap();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].run_id.0, "legacy");
    }

    fn batch_projection_records(session: &BindIdentity, message_count: usize) -> Vec<FramedRecord> {
        let input = (0..message_count)
            .map(|ordinal| {
                Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: format!("message-{ordinal}"),
                    }],
                )
            })
            .collect::<Vec<_>>()
            .into();
        vec![
            FramedRecord {
                seq: 1,
                fence: 1,
                record: WalRecord::RunStarted {
                    retention: None,
                    hooks: None,
                    title: None,
                    on_restart: None,
                    keep_warm: None,
                    marker_order_version: None,
                    manifest: None,
                    scope: None,
                    send_id: None,
                    request_identity_digest: None,
                    project_id: None,
                    principal: None,
                    ts_ms: None,
                    run_id: RunId("batched-import".into()),
                    session: session.clone(),
                    config: Box::new(RunConfig {
                        model: ModelHandle::new("import", "import"),
                        tools: Vec::new().into(),
                        tool_choice: Default::default(),
                        generation: Default::default(),
                        response_format: Default::default(),
                        provider_options: serde_json::Value::Null,
                        cache_tiers: serde_json::Value::Null,
                        context_epoch: 0,
                        renderer_version: 1,
                        frozen_render_config: serde_json::Value::Null,
                        auth_selection: None,
                        tools_were_omitted: false,
                        budget: None,
                        context_limit: None,
                        work_class: None,
                        variant: None,
                        service_tier: None,
                        stop_when: Vec::new(),
                        context_management: None,
                    }),
                    input,
                    origin: RunOrigin::SeededFrom {
                        origin_kind: SeedOriginKind::Import,
                        source_ref: "fixture:batched-import".into(),
                        origin_device_pseudonym: None,
                        base_seq: None,
                    },
                    submission_id: None,
                },
            },
            FramedRecord {
                seq: 2,
                fence: 1,
                record: WalRecord::RunFinished {
                    ts_ms: None,
                    reason: RunFinishReason::Completed,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            },
        ]
    }

    #[test]
    fn batched_projection_crosses_boundaries_and_updates_conflicts() {
        if !cfg!(feature = "live-postgres") {
            eprintln!(
                "SKIP batched_projection_crosses_boundaries_and_updates_conflicts: enable the live-postgres feature and set DATABASE_URL. Example: {RUN_COMMAND}"
            );
            return;
        }
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!(
                "SKIP batched_projection_crosses_boundaries_and_updates_conflicts: DATABASE_URL is not set. Example: {RUN_COMMAND}"
            );
            return;
        };

        const MESSAGE_COUNT: usize = 1_501;
        // Two different collisions to avoid, so two different sources. WITHIN a run, the
        // counter separates concurrent tests: the wall clock advertises nanoseconds and
        // does not tick at that resolution, so a clock reading alone lets two tests in the
        // same tick share a session key -- and against a shared Postgres instance that is
        // writing into each other's rows, not merely colliding on a temp path. ACROSS runs,
        // the counter restarts at zero and the pid may be recycled, so the clock reading
        // stays as the coarse separator it was already serving as.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = format!(
            "{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let session = BindIdentity::new(
            "/postgres-batch-projection-test",
            "conformance",
            format!("{}-{unique}", std::process::id()),
        );
        let key = PostgresStore::key(&session);
        let records = batch_projection_records(&session, MESSAGE_COUNT);
        let expected_watermark =
            latest_projection_boundary(&records, 0).expect("terminal import has a boundary");
        let store = PostgresStore::connect(&url).expect("connect to Postgres");

        // Prepare the same range before the first commit so the second plan exercises the SQL
        // conflict-update arm instead of the watermark no-change fast path.
        let mut reprojection_plan = PostgresStore::prepare_projection(
            store
                .projection_snapshot(&key)
                .expect("read initial projection snapshot"),
            &records,
            None,
        )
        .expect("prepare duplicate projection range");
        let ProjectionPlan::Write(prepared) = &mut reprojection_plan else {
            panic!("fresh import must produce a write plan");
        };
        // The initial import replaces any prior prefix. Disable that delete only on the duplicate
        // plan so its existing ordinals reach the INSERT conflict-update branch.
        prepared.truncate_prefix_to = None;

        assert_eq!(
            store.project(&session, &records).expect("project import"),
            expected_watermark
        );

        {
            let mut client = store.client.lock().expect("lock Postgres client");
            client
                .execute(
                    "UPDATE message SET json = 'stale' WHERE session = $1 AND ord = 750",
                    &[&key],
                )
                .expect("seed conflicting message row");
            let tx = client.transaction().expect("begin duplicate projection");
            assert_eq!(
                PostgresStore::commit_projection_plan(tx, &key, reprojection_plan)
                    .expect("re-project the same range through upsert"),
                expected_watermark
            );
        }

        assert_eq!(
            store
                .project(&session, &records)
                .expect("idempotently re-project import"),
            expected_watermark
        );

        let mut client = store.client.lock().expect("lock Postgres client");
        let rows = client
            .query(
                "SELECT ord, json FROM message WHERE session = $1 ORDER BY ord ASC",
                &[&key],
            )
            .expect("read projected messages");
        assert_eq!(rows.len(), MESSAGE_COUNT);
        for (expected_ordinal, row) in rows.iter().enumerate() {
            assert_eq!(row.get::<_, i64>(0), expected_ordinal as i64);
            let message: Message =
                serde_json::from_str(&row.get::<_, String>(1)).expect("decode projected message");
            assert!(matches!(
                &message.content[..],
                [ContentBlock::Text { text }] if text == &format!("message-{expected_ordinal}")
            ));
        }
        let metadata = client
            .query_one(
                "SELECT watermark_seq, prefix_len FROM meta WHERE session = $1",
                &[&key],
            )
            .expect("read projection metadata");
        assert_eq!(
            metadata.get::<_, i64>(0),
            i64::try_from(expected_watermark).expect("watermark fits Postgres BIGINT")
        );
        assert_eq!(metadata.get::<_, i64>(1), MESSAGE_COUNT as i64);
    }

    #[test]
    fn projection_callers_preserve_identical_messages_checkpoint_watermark_and_cursor() {
        use broca_core::projection_plan::ProjectionSnapshot;
        let session = BindIdentity::new("/projection-callers", "test", "same-input");
        let records = batch_projection_records(&session, 2);
        let snapshot = ProjectionSnapshot {
            attribution_complete: true,
            seed: None,
            watermark: 0,
            lineage_id: None,
            next_ord: 0,
            previous_control_head: None,
        };
        let sqlite =
            broca_store::projection_caller_output_for_conformance(snapshot.clone(), &records, None)
                .unwrap();
        let ProjectionPlan::Write(prepared) =
            PostgresStore::prepare_projection(snapshot, &records, None).unwrap()
        else {
            panic!("import must produce a write");
        };
        let control_head = prepared
            .control_head_wal_seq
            .map(|wal_seq| broca_core::Cursor {
                wal_seq: u64::try_from(wal_seq).unwrap(),
                sub_index: u32::try_from(prepared.control_head_sub_index.unwrap()).unwrap(),
            });
        let postgres = serde_json::json!({
            "messages": prepared.message_json,
            "checkpoint": prepared.checkpoint_json,
            "watermark": prepared.new_watermark_i64,
            "control_head": control_head.map(|cursor| serde_json::json!({"wal_seq": cursor.wal_seq, "sub_index": cursor.sub_index})),
        });
        assert_eq!(sqlite, postgres);
        // Imported input commits at RunStarted; the terminal remains in the tail.
        assert_eq!(postgres["watermark"], 1);
        assert_eq!(postgres["messages"].as_array().unwrap().len(), 2);
        assert_eq!(
            postgres["control_head"],
            serde_json::json!({"wal_seq": 1, "sub_index": 0})
        );
    }

    #[test]
    fn export_writers_lock_before_allocating_sequences() {
        if !cfg!(feature = "live-postgres") {
            eprintln!("SKIP export_writers_lock_before_allocating_sequences: enable live-postgres and set DATABASE_URL. Example: {RUN_COMMAND}");
            return;
        }
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP export_writers_lock_before_allocating_sequences: DATABASE_URL is not set. Example: {RUN_COMMAND}");
            return;
        };
        let session = BindIdentity::new("postgres-export-order", "test", "test");
        let mut records = batch_projection_records(&session, 0);
        if let WalRecord::RunStarted { origin, .. } = &mut records[0].record {
            *origin = RunOrigin::Fresh;
        }
        super::export_index::assert_writers_lock_before_allocating_sequences(&url, records);
    }

    #[test]
    fn postgres_store_conformance() {
        if !cfg!(feature = "live-postgres") {
            eprintln!(
                "SKIP postgres_store_conformance: enable the live-postgres feature and set DATABASE_URL to run against a real Postgres. Example: {RUN_COMMAND}"
            );
            return;
        }

        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!(
                "SKIP postgres_store_conformance: DATABASE_URL is not set. Example: {RUN_COMMAND}"
            );
            return;
        };

        super::export_index::assert_session_column_migration_is_idempotent(&url);
        super::export_index::assert_redundant_fact_id_index_is_dropped(&url);
        broca_store::conformance::assert_conformance(|| {
            PostgresStore::connect(&url).expect("connect to Postgres for conformance test")
        });
        // The durable run index, against the same real Postgres.
        broca_store::conformance::assert_run_index_conformance(|| {
            PostgresStore::connect(&url).expect("connect to Postgres for run-index conformance")
        });
        broca_store::conformance::assert_export_conformance(|| {
            PostgresStore::connect(&url).expect("connect to Postgres for export conformance")
        });
        super::export_index::assert_populated_export_steps_dropped_on_reopen(&url);
        super::export_index::assert_stale_high_water_candidate_cannot_regress(&url);
    }
}
