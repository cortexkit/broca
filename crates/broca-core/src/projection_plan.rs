//! Semantic planning for materialized WAL projections. Stores load a consistent
//! snapshot and atomically apply the batch only if its expected state still matches.

use crate::{
    encode_admission_checkpoint, last_control_cursor_through, latest_projection_boundary, replay,
    replay_projection_tail, AdmissionCheckpointV1, Cursor, ProjectionSeed, ReplayError,
};
use broca_wal::{FramedRecord, WalByteAnchor, WalRangeRead, WalRecord};

#[derive(Debug)]
pub enum ProjectionPlanError {
    Json(serde_json::Error),
    Replay(ReplayError),
    Int(std::num::TryFromIntError),
    InconsistentSnapshot(&'static str),
}

impl std::fmt::Display for ProjectionPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(e) => write!(f, "json: {e}"),
            Self::Replay(e) => write!(f, "replay: {e}"),
            Self::Int(e) => write!(f, "integer conversion: {e}"),
            Self::InconsistentSnapshot(message) => write!(f, "inconsistent snapshot: {message}"),
        }
    }
}
impl std::error::Error for ProjectionPlanError {}
impl From<serde_json::Error> for ProjectionPlanError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}
impl From<ReplayError> for ProjectionPlanError {
    fn from(e: ReplayError) -> Self {
        Self::Replay(e)
    }
}
impl From<std::num::TryFromIntError> for ProjectionPlanError {
    fn from(e: std::num::TryFromIntError) -> Self {
        Self::Int(e)
    }
}

/// The common signed-integer capacity of the materialized stores. Conversion is
/// fallible even for WAL offsets and prefix lengths that cannot grow this large today.
pub fn u64_to_i64(value: u64) -> Result<i64, ProjectionPlanError> {
    Ok(i64::try_from(value)?)
}

#[derive(Debug, Clone)]
pub struct ProjectionSnapshot {
    pub attribution_complete: bool,
    pub seed: Option<ProjectionSeed>,
    pub watermark: u64,
    pub lineage_id: Option<String>,
    pub next_ord: i64,
    pub previous_control_head: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionPlan {
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
        metadata: RetainedWithdrawalProjection,
    },
    Write(Box<PreparedProjection>),
}

/// The only deferred-approval facts a retained lineage may persist. Unlike
/// `DeferredCallProjection`, this contains no Prepared arguments or late results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedWithdrawalProjection {
    pub lineage_id: String,
    pub withdrawals_owed_runs: Vec<String>,
}

pub fn retained_withdrawal_projection(
    seed: Option<&crate::replay::DeferredCallProjection>,
    records: &[FramedRecord],
    lineage: Option<&str>,
    last_run_id: Option<&broca_protocol::RunId>,
) -> Result<Option<RetainedWithdrawalProjection>, ProjectionPlanError> {
    let view = crate::replay::project_deferred_calls(seed, records, lineage, last_run_id);
    if view.is_none()
        && records
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
    {
        return Err(ProjectionPlanError::InconsistentSnapshot(
            "Prepared call projection requires a WAL lineage identity",
        ));
    }
    let Some(view) = view else {
        return Ok(None);
    };
    let mut runs = std::collections::BTreeSet::new();
    for owed in view.obligations().withdrawals {
        let key = format!("{}:{}", view.lineage_id, owed.prepared.seq);
        if let Some(run_id) = view.calls.get(&key).and_then(|call| call.run_id.as_ref()) {
            runs.insert(run_id.0.clone());
        }
    }
    Ok((!runs.is_empty()).then(|| RetainedWithdrawalProjection {
        lineage_id: view.lineage_id,
        withdrawals_owed_runs: runs.into_iter().collect(),
    }))
}

impl ProjectionPlan {
    pub fn expected_state(&self) -> (u64, Option<&str>) {
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

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedProjection {
    pub expected_watermark: u64,
    pub expected_lineage_id: Option<String>,
    pub reset: bool,
    pub lineage_id: Option<String>,
    pub new_watermark: u64,
    pub next_ord: i64,
    pub truncate_prefix_to: Option<i64>,
    pub message_json: Vec<String>,
    pub message_run_json: Vec<String>,
    /// Each message's runner tags as JSON (`null` for an untagged message), beside
    /// `message_json`: the store keeps them in their own column, since a message's own
    /// serialized form never carries them.
    pub message_tags_json: Vec<Option<String>>,
    pub final_run_json: Vec<(i64, String)>,
    pub completed_steps: i64,
    pub usage_json: String,
    pub retries_used: i64,
    pub last_step_usage_json: Option<String>,
    pub last_step_finish_reason_json: Option<String>,
    pub last_run_id: Option<String>,
    /// The newest caller title among the records this batch folds (`RunStarted`, `Queued` or
    /// `Steered`, newest by WAL position). `None` means the batch carries none, so the store
    /// keeps the title it already holds; a reset batch replays from sequence zero, so `None`
    /// there means the lineage has none.
    pub last_title: Option<String>,
    pub last_episode_usage_json: String,
    pub checkpoint_json: String,
    pub anchor: Option<WalByteAnchor>,
    pub control_head: Option<Cursor>,
    pub deferred_calls_json: Option<String>,
    /// Runs whose full supplied WAL view still owes withdrawals. Projection only sets the
    /// index flag; the actor clears it after durable acknowledgements cover every owed call.
    pub withdrawals_owed_runs: Vec<String>,
}

pub fn plan_projection(
    snapshot: ProjectionSnapshot,
    records: &[FramedRecord],
    range: Option<&WalRangeRead>,
) -> Result<ProjectionPlan, ProjectionPlanError> {
    let expected_watermark = snapshot.watermark;
    let expected_lineage_id = snapshot.lineage_id.clone();
    let incoming_lineage_id = range
        .and_then(|range| range.lineage_id)
        .map(|lineage_id| lineage_id.to_string());
    let reset = !snapshot.attribution_complete
        || snapshot
            .seed
            .as_ref()
            .and_then(|seed| seed.deferred_calls.as_ref())
            .is_some_and(|calls| calls.stopped_runs.is_none())
        || matches!(
            (expected_lineage_id.as_deref(), incoming_lineage_id.as_deref()),
            (Some(stored), Some(incoming)) if stored != incoming
        )
        || (incoming_lineage_id.is_some() && expected_lineage_id.is_none());
    // A retained lineage has no materialized prefix. The deferred-call summary
    // also contains Prepared arguments and late tool results, so it stays WAL-only.
    // Only the content-free withdrawal discovery flags may cross this fence.
    let unidentified_replacement = incoming_lineage_id.is_some() && expected_lineage_id.is_none();
    let prior_policy = if reset || unidentified_replacement {
        None
    } else {
        snapshot.seed.as_ref().and_then(|seed| {
            seed.admission_checkpoint
                .lineage_retention
                .map(|policy| (policy, seed.watermark_seq))
        })
    };
    let policy = crate::retention::projection_policy(records, prior_policy);
    if matches!(policy, Some(crate::LineageRetention::Retained { .. })) {
        let seed = snapshot.seed.as_ref().filter(|_| !reset);
        let metadata = retained_withdrawal_projection(
            seed.and_then(|seed| seed.deferred_calls.as_ref()),
            records,
            incoming_lineage_id
                .as_deref()
                .or(expected_lineage_id.as_deref()),
            seed.and_then(|seed| seed.last_run_id.as_ref()),
        )?;
        if let Some(metadata) = metadata {
            return Ok(ProjectionPlan::RetentionMetadata {
                expected_watermark,
                expected_lineage_id,
                metadata,
            });
        }
        return Ok(ProjectionPlan::NoChange {
            expected_watermark,
            expected_lineage_id,
        });
    }
    if reset && !records.is_empty() && !crate::retention::starts_at_lineage_origin(records) {
        return Err(ProjectionPlanError::InconsistentSnapshot(
            "WAL lineage changed and projection requires a sequence-zero replay",
        ));
    }

    let (seed, watermark, mut next_ord, previous_control_head) = if reset {
        (None, 0, 0, None)
    } else {
        (
            snapshot.seed,
            snapshot.watermark,
            snapshot.next_ord,
            snapshot.previous_control_head,
        )
    };
    let new_boundary = latest_projection_boundary(records, watermark);
    let terminal_update = watermark > 0
        && records
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::RunFinished { .. }));
    let Some(new_watermark) = new_boundary.or_else(|| terminal_update.then_some(watermark)) else {
        return Ok(if reset {
            ProjectionPlan::Reset {
                expected_watermark,
                expected_lineage_id,
            }
        } else {
            ProjectionPlan::NoChange {
                expected_watermark,
                expected_lineage_id,
            }
        });
    };

    match policy {
        Some(crate::LineageRetention::Unstarted) | None => {
            return Err(ProjectionPlanError::InconsistentSnapshot(
                "content projection requires the first RunStarted retention policy from the WAL",
            ))
        }
        Some(_) => {}
    }

    let metadata_seed = seed.as_ref().and_then(|seed| seed.deferred_calls.as_ref());
    let lineage = incoming_lineage_id
        .as_deref()
        .or(expected_lineage_id.as_deref());
    let metadata_run_id = seed.as_ref().and_then(|seed| seed.last_run_id.as_ref());
    let prefix_frames: Vec<_> = records
        .iter()
        .filter(|frame| {
            (metadata_seed.is_none() || frame.seq > watermark) && frame.seq <= new_watermark
        })
        .cloned()
        .collect();
    let deferred_calls = crate::replay::project_deferred_calls(
        metadata_seed,
        &prefix_frames,
        lineage,
        metadata_run_id,
    );
    if deferred_calls.is_none()
        && prefix_frames
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
    {
        return Err(ProjectionPlanError::InconsistentSnapshot(
            "Prepared call projection requires a WAL lineage identity",
        ));
    }
    let full_view_frames: Vec<_> = records
        .iter()
        .filter(|frame| metadata_seed.is_none() || frame.seq > watermark)
        .cloned()
        .collect();
    let full_deferred_view = crate::replay::project_deferred_calls(
        metadata_seed,
        &full_view_frames,
        lineage,
        metadata_run_id,
    );
    let mut withdrawals_owed_runs = std::collections::BTreeSet::new();
    if let Some(view) = &full_deferred_view {
        for owed in view.obligations().withdrawals {
            let key = format!("{}:{}", view.lineage_id, owed.prepared.seq);
            if let Some(run_id) = view.calls.get(&key).and_then(|call| call.run_id.as_ref()) {
                withdrawals_owed_runs.insert(run_id.0.clone());
            }
        }
    }
    let deferred_calls_json = Some(serde_json::to_string(&deferred_calls)?);

    let budgeted = seed
        .as_ref()
        .and_then(|seed| seed.admission_checkpoint.last_episode.as_ref())
        .is_some_and(|episode| episode.config.budget.is_some())
        || records
            .iter()
            .rev()
            .find_map(|framed| match &framed.record {
                WalRecord::RunStarted { config, .. } => Some(config.budget.is_some()),
                _ => None,
            })
            == Some(true);
    let (
        truncate_prefix_to,
        new_messages,
        message_runs,
        completed_steps,
        usage,
        last_step_usage,
        last_step_finish_reason,
        retries_used,
        last_run_id,
        admission_checkpoint,
    ) = if let Some(seed) = seed.filter(|seed| {
        // A seed stored before the ownership summary existed (or before it recorded the frozen
        // plan manifest) cannot carry it forward: the summary covers every episode, and the
        // seed's earlier ones are not in this tail.
        seed.admission_checkpoint.ownership_complete()
            && (!budgeted || crate::budget_replay_seed_is_valid(seed))
    }) {
        let tail: Vec<FramedRecord> = records
            .iter()
            .filter(|framed| {
                (seed.prefix_len == 0 || framed.seq > watermark) && framed.seq <= new_watermark
            })
            .cloned()
            .collect();
        let delta = replay_projection_tail(&seed, &tail)?;
        (
            delta.truncate_prefix_to,
            delta.messages,
            delta.message_runs,
            delta.completed_steps,
            delta.usage,
            delta.last_step_usage,
            delta.last_step_finish_reason,
            delta.retries_used,
            delta.last_run_id,
            delta.admission_checkpoint,
        )
    } else {
        // Legacy or malformed checkpoint metadata asks the caller for a complete WAL read;
        // recomputing every projection field repairs that row at commit.
        let up_to_frames: Vec<FramedRecord> = records
            .iter()
            .filter(|framed| framed.seq <= new_watermark)
            .cloned()
            .collect();
        let up_to: Vec<WalRecord> = up_to_frames
            .iter()
            .map(|framed| framed.record.clone())
            .collect();
        let prefix_state = replay(&up_to)?;
        let admission_checkpoint = AdmissionCheckpointV1::from_frames(&up_to_frames)?;
        let last_run_id = admission_checkpoint
            .last_episode
            .as_ref()
            .map(|episode| episode.run_id.clone());
        (
            Some(0),
            prefix_state.prompt,
            prefix_state.message_runs,
            prefix_state.completed_steps,
            prefix_state.usage,
            prefix_state.last_step_usage,
            prefix_state.last_step_finish_reason,
            prefix_state.retries_used,
            last_run_id,
            admission_checkpoint,
        )
    };

    if last_run_id.as_ref()
        != admission_checkpoint
            .last_episode
            .as_ref()
            .map(|episode| &episode.run_id)
    {
        return Err(ProjectionPlanError::InconsistentSnapshot(
            "last_run_id disagrees with the admission checkpoint",
        ));
    }

    // Every record up to the new watermark that this batch carries: the seeded tail and the
    // full-replay prefix both lie inside it, and a record at or below the old watermark repeats
    // a title the store already folded, so including one cannot make the answer older.
    let last_title = records
        .iter()
        .rev()
        .filter(|framed| framed.seq <= new_watermark)
        .find_map(|framed| framed.record.title())
        .map(str::to_owned);

    let truncate_prefix_to = truncate_prefix_to.map(u64_to_i64).transpose()?;
    if let Some(truncate_to) = truncate_prefix_to {
        next_ord = truncate_to;
    }
    let message_json = new_messages
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?;
    let message_run_json = message_runs
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?;
    let message_tags_json = new_messages
        .iter()
        .map(|message| message.part_tags().map(serde_json::to_string).transpose())
        .collect::<Result<Vec<_>, _>>()?;
    // Keep RunFinished beyond the last materialized record: older resume code reads it
    // from the WAL to know the run ended. Replay those unmaterialized records too, persisting
    // their final-message metadata without moving the watermark past that terminal record.
    let final_seed = ProjectionSeed {
        watermark_seq: new_watermark,
        prefix_len: (next_ord as u64).saturating_add(new_messages.len() as u64),
        completed_steps,
        usage,
        retries_used,
        last_step_usage,
        last_step_finish_reason,
        last_run_id: last_run_id.clone(),
        admission_checkpoint: admission_checkpoint.clone(),
        ..ProjectionSeed::empty()
    };
    let suffix: Vec<_> = records
        .iter()
        .filter(|frame| frame.seq > new_watermark)
        .cloned()
        .collect();
    let episode = final_seed.admission_checkpoint.last_episode.as_ref();
    let final_state = crate::replay::replay_seeded(
        suffix.iter().map(|frame| &frame.record),
        episode.map(|episode| episode.config.model.clone()),
        final_seed.last_run_id.clone(),
        episode.and_then(|episode| episode.config.budget.clone()),
        episode.and_then(|episode| episode.budget_replay_state.clone()),
        final_seed.prefix_len,
        // Legacy budget firings can reconstruct their text from the frozen config. Only
        // metadata is persisted here; the ordinary body projection still validates new firings.
        false,
        // Only run metadata of this replay is persisted, never its messages or their tags.
        crate::tags::TagSeed::default(),
    )?;
    let final_run_json = final_state
        .run_finals
        .into_iter()
        .filter(|(ordinal, _)| *ordinal < final_seed.prefix_len)
        .map(|(ordinal, run)| Ok((u64_to_i64(ordinal)?, serde_json::to_string(&run)?)))
        .collect::<Result<Vec<_>, ProjectionPlanError>>()?;
    let last_episode_usage_json = serde_json::to_string(
        &admission_checkpoint
            .last_episode
            .as_ref()
            .and_then(|episode| episode.usage),
    )?;
    let checkpoint_json = encode_admission_checkpoint(&admission_checkpoint)?;
    let usage_json = serde_json::to_string(&usage)?;
    let last_step_usage_json = last_step_usage
        .map(|value| serde_json::to_string(&value))
        .transpose()?;
    let last_step_finish_reason_json = last_step_finish_reason
        .map(|value| serde_json::to_string(&value))
        .transpose()?;
    let control_head =
        last_control_cursor_through(records, Some(new_watermark)).or(previous_control_head);
    let anchor = range.and_then(|range| range.anchor_at(new_watermark));
    // Both stores use signed 64-bit columns. Reject out-of-range values before
    // returning a batch, rather than letting a spawned projector panic or wrap.
    u64_to_i64(new_watermark)?;
    if let Some(anchor) = anchor {
        u64_to_i64(anchor.seq)?;
        u64_to_i64(anchor.byte_end)?;
    }
    if let Some(cursor) = control_head {
        u64_to_i64(cursor.wal_seq)?;
    }
    let lineage_id = incoming_lineage_id.or_else(|| expected_lineage_id.clone());

    Ok(ProjectionPlan::Write(Box::new(PreparedProjection {
        expected_watermark,
        expected_lineage_id,
        reset,
        lineage_id,
        new_watermark,
        next_ord,
        truncate_prefix_to,
        message_json,
        message_run_json,
        message_tags_json,
        final_run_json,
        completed_steps: u64_to_i64(completed_steps)?,
        usage_json,
        retries_used: i64::from(retries_used),
        last_step_usage_json,
        last_step_finish_reason_json,
        last_run_id: last_run_id.map(|run_id| run_id.0),
        last_title,
        last_episode_usage_json,
        checkpoint_json,
        anchor,
        control_head,
        deferred_calls_json,
        withdrawals_owed_runs: withdrawals_owed_runs.into_iter().collect(),
    })))
}
