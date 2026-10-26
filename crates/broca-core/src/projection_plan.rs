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
    pub head_json: String,
    pub message_times: Vec<Option<u64>>,
    pub episode_spans: Vec<std::ops::Range<u64>>,
    pub open_episode_span: Option<u64>,
    /// Every episode's render inputs, in lineage order. Read by a resumed run under an applied
    /// view, which cannot take them from the discarded transcript. `None` when an incremental
    /// projection continued a store that never computed them: they stay absent (read as "replay
    /// the WAL") until a complete projection fills them.
    pub episodes: Option<Vec<crate::context::EpisodeRenderProjection>>,
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
    pub compaction_setup_seen: bool,
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
    let incremental_seed = seed.clone().filter(|seed| {
        seed.admission_checkpoint.ownership_complete()
            && seed.admission_checkpoint.head.is_some()
            && (!budgeted || crate::budget_replay_seed_is_valid(seed))
    });
    let time_placement = incremental_seed
        .as_ref()
        .and_then(crate::projection::ControlProjectionState::from_projection_seed)
        .map(|state| state.placement().clone())
        .unwrap_or_default();
    let times = crate::message_times::written_times_with_queued(
        records
            .iter()
            .filter(|frame| {
                frame.seq <= new_watermark
                    && incremental_seed
                        .as_ref()
                        .is_none_or(|seed| seed.prefix_len == 0 || frame.seq > seed.watermark_seq)
            })
            .map(|frame| &frame.record),
        time_placement,
        incremental_seed
            .as_ref()
            .and_then(|seed| seed.admission_checkpoint.head.as_ref())
            .map(|head| head.queued_times().clone())
            .unwrap_or_default(),
    )
    .map_err(|_| ProjectionPlanError::InconsistentSnapshot("message time placement failed"))?;
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
        episode_spans,
        open_episode_span,
        episode_metadata,
    ) = if let Some(seed) = incremental_seed {
        // A seed stored before the ownership summary existed (or before it recorded the frozen
        // plan manifest) cannot carry it forward: the summary covers every episode, and the
        // seed's earlier ones are not in this tail.
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
            delta.episode_spans,
            delta.open_episode_span,
            delta.episodes,
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
        let (episode_spans, open_episode_span) =
            crate::replay::projected_episode_spans(&prefix_state, 0, None);
        // The stored span list may start mid-episode, so the opened episode's start is the
        // episode projection's, not the first span's. A store that never recorded one takes its
        // coordinates from the seed's own open episode, whose start and frozen config are
        // durable facts of the run that wrote this prefix.
        let mut episode_metadata = crate::replay::projected_episode_metadata(&prefix_state);
        if let (Some(seed), Some(open)) = (seed.as_ref(), episode_metadata.last_mut()) {
            if seed.projection_episodes.is_none() {
                if let Some(previous) = seed.open_episode.as_ref() {
                    open.start = previous.start;
                    open.first_user = previous.first_user;
                    open.render_config = previous.render_config.clone();
                }
            }
        }
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
            episode_spans,
            open_episode_span,
            Some(episode_metadata),
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
    let message_times = (0..new_messages.len())
        .map(|index| times.get(&(next_ord as u64 + index as u64)).copied())
        .collect();
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
    let anchor = range
        .and_then(|range| range.anchor_at(new_watermark))
        .or_else(|| {
            // A terminal-only tail updates final-message metadata without advancing the
            // boundary. The range omits that old boundary, not its valid stored anchor.
            // Advancing or resetting without a fresh anchor must still clear the old one.
            if new_watermark == watermark {
                seed.as_ref()
                    .and_then(|seed| seed.anchor)
                    .filter(|anchor| anchor.seq == new_watermark)
            } else {
                None
            }
        });
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
        message_times,
        head_json: serde_json::to_string(&admission_checkpoint.head)?,
        episode_spans,
        open_episode_span,
        episodes: episode_metadata,
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
        compaction_setup_seen: admission_checkpoint
            .compaction
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.state().is_some()),
        checkpoint_json,
        anchor,
        control_head,
        deferred_calls_json,
        withdrawals_owed_runs: withdrawals_owed_runs.into_iter().collect(),
    })))
}

/// The serialized placement seed belongs only to the rebuildable database, never
/// to a WAL record. Bump when Placement, Batch, or OffsetSeed changes shape.
pub const OFFSET_SEED_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct OffsetAdmissionSeed {
    pub run_started: Option<u64>,
    pub refreeze: Option<u64>,
    pub auth_refreeze: Option<u64>,
    pub queued: std::collections::BTreeMap<String, u64>,
    pub staged: std::collections::BTreeMap<String, u64>,
    pub compaction: Vec<u64>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct OffsetSource {
    pub lo_seq: u64,
    pub lo_off: u64,
    pub hi_off: u64,
    pub input_start: u64,
    pub input_end: u64,
    pub tag_base: Option<u64>,
    pub tag_n: u64,
    pub tag_high: u64,
    /// 1 = stored tags, 2 = computed tags, 4 = render, 8 = clean.
    pub flags: u32,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct OffsetEpisode {
    pub n: u64,
    pub run_id: Option<String>,
    pub run_started_seq: Option<u64>,
    pub run_started_off: Option<u64>,
    pub first_ord: u64,
    pub step_first_ord: Option<u64>,
    pub end_ord: u64,
    pub tag_seed: u64,
    pub terminal_off: Option<u64>,
    pub terminal_kind: Option<String>,
    pub state: String,
    pub state_seq: u64,
    pub last_step_off: Option<u64>,
    pub submission_id: Option<String>,
    pub request_identity_digest: Option<String>,
}

/// A small continuation of the ordinal fold. Held inputs and open tool turns
/// retain source offsets and tag counts, never messages, arguments or results.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct OffsetSeed {
    placement: crate::placement::Placement,
    pub episode: Option<OffsetEpisode>,
    pub admission: OffsetAdmissionSeed,
    held_steers: Vec<OffsetSource>,
    held_assistant: Option<OffsetSource>,
    tool_parts: Vec<OffsetSource>,
    pub tag_high: u64,
    tag_rules: Option<(bool, bool)>,
    budget_digest: Option<Vec<u8>>,
    #[serde(skip)]
    budget_config: Option<broca_protocol::request::BudgetConfig>,
    import_tag_high: Option<u64>,
    pub trailing_origin: Option<u64>,
    pub title_off: Option<u64>,
    pub frozen_policy: Option<u64>,
    pub last_activity_ms: Option<u64>,
    pub last_write_ms: Option<u64>,
}

#[derive(Debug, Default)]
pub struct OffsetDelta {
    pub messages: Vec<(u64, OffsetSource)>,
    pub truncate: Option<u64>,
    pub closed_episode: Option<OffsetEpisode>,
    pub reanchor: Option<(u64, u64)>,
}

impl OffsetSeed {
    pub fn head_ord(&self) -> u64 {
        self.placement.next_ordinal()
    }

    pub fn encode(&self) -> Result<String, ProjectionPlanError> {
        Ok(serde_json::to_string(&(OFFSET_SEED_VERSION, self))?)
    }

    /// Invalid or stale derived state is a cache miss, not a partial seed.
    pub fn decode(
        bytes: &str,
        budget: Option<broca_protocol::request::BudgetConfig>,
    ) -> Option<Self> {
        let (version, mut seed): (u32, Self) = serde_json::from_str(bytes).ok()?;
        if version != OFFSET_SEED_VERSION
            || seed.budget_digest != offset_budget_digest(budget.as_ref()).ok()?
        {
            return None;
        }
        seed.budget_config = budget.clone();
        seed.placement = crate::placement::Placement::from_index_seed(seed.placement, budget);
        Some(seed)
    }

    fn number(&mut self, source: &mut OffsetSource) -> Result<(), ProjectionPlanError> {
        if source.flags & 2 != 0 {
            source.tag_base = Some(self.tag_high);
            self.tag_high = self.tag_high.checked_add(source.tag_n).ok_or(
                ProjectionPlanError::InconsistentSnapshot("tag numbering overflow"),
            )?;
            source.tag_high = self.tag_high;
        } else {
            self.tag_high = self.tag_high.max(source.tag_high);
        }
        Ok(())
    }

    pub fn observe(
        &mut self,
        frame: &FramedRecord,
        off: u64,
        end: u64,
    ) -> Result<OffsetDelta, ProjectionPlanError> {
        use crate::placement::{AppendKind, HoldKind, PlacementAction};
        let record = &frame.record;
        let mut delta = OffsetDelta::default();
        if let WalRecord::RunStarted {
            run_id,
            submission_id,
            request_identity_digest,
            retention,
            config,
            ..
        } = record
        {
            self.budget_digest = offset_budget_digest(config.budget.as_ref())?;
            self.budget_config = config.budget.clone();
            self.import_tag_high = matches!(
                record,
                WalRecord::RunStarted {
                    origin: broca_wal::RunOrigin::SeededFrom {
                        origin_kind: broca_wal::SeedOriginKind::Import,
                        ..
                    },
                    ..
                }
            )
            .then_some(self.tag_high);
            delta.closed_episode = self.episode.take();
            let n = delta.closed_episode.as_ref().map_or(0, |e| e.n + 1);
            self.tag_rules = crate::tags::computed_rules(config).map(|r| (r.render, r.clean));
            if let Some(tags) = config.computed_tags {
                self.tag_high = self.tag_high.max(tags.checkpoint);
            }
            self.episode = Some(OffsetEpisode {
                n,
                run_id: Some(run_id.0.clone()),
                run_started_seq: Some(frame.seq),
                run_started_off: Some(off),
                first_ord: self.head_ord(),
                end_ord: self.head_ord(),
                tag_seed: self.tag_high,
                state: "active".into(),
                state_seq: frame.seq,
                submission_id: submission_id.as_ref().map(|id| id.0.clone()),
                request_identity_digest: request_identity_digest.clone(),
                ..OffsetEpisode::default()
            });
            self.held_steers.clear();
            self.held_assistant = None;
            self.tool_parts.clear();
            self.admission.run_started = Some(off);
            self.admission.refreeze = None;
            self.admission.auth_refreeze = None;
            if let Some(id) = submission_id {
                self.admission.queued.remove(&id.0);
                self.admission.staged.remove(&id.0);
            }
            self.frozen_policy = match (self.frozen_policy, retention) {
                (Some(old), Some(new)) => Some(old.min(*new)),
                (old, new) => old.or(*new),
            };
        }
        if self.episode.is_none() {
            self.episode = Some(OffsetEpisode {
                first_ord: self.head_ord(),
                state: "unknown".into(),
                ..OffsetEpisode::default()
            });
        }
        if let WalRecord::Queued {
            retention: Some(retention),
            ..
        }
        | WalRecord::Steered {
            retention: Some(retention),
            ..
        } = record
        {
            self.frozen_policy = Some(self.frozen_policy.unwrap_or(*retention).min(*retention));
        }
        if record.title().is_some() {
            self.title_off = Some(off);
        }
        if let Some(time) = crate::context::record_time_ms(record) {
            self.last_write_ms = Some(time);
            self.last_activity_ms = Some(self.last_activity_ms.unwrap_or(0).max(time));
        }
        let tag_rules = self
            .tag_rules
            .map(|(render, clean)| broca_protocol::tags::ComputedRules { render, clean });
        match record {
            WalRecord::Queued { submission_id, .. } => {
                self.admission.queued.insert(submission_id.0.clone(), off);
            }
            WalRecord::Retracted { submission_id, .. }
            | WalRecord::SendEnded { submission_id, .. }
            | WalRecord::SendDelivered { submission_id, .. } => {
                self.admission.queued.remove(&submission_id.0);
                self.admission.staged.remove(&submission_id.0);
            }
            WalRecord::DeliveryOrdered { submission_id, .. } => {
                self.admission.staged.insert(submission_id.0.clone(), off);
            }
            WalRecord::RunConfigRefrozen { .. } => self.admission.refreeze = Some(off),
            WalRecord::RunAuthRefrozen { .. } => self.admission.auth_refreeze = Some(off),
            WalRecord::StepStarted { .. } => {
                if let Some(ep) = &mut self.episode {
                    ep.state = "active".into();
                    ep.state_seq = frame.seq;
                }
            }
            WalRecord::ModelStepFinished { .. } => {
                self.trailing_origin = Some(off);
                if let Some(ep) = &mut self.episode {
                    ep.last_step_off = Some(off);
                }
            }
            WalRecord::ToolBatchStarted { .. } => {
                self.tool_parts.clear();
            }
            WalRecord::ToolResult { result, .. } => {
                let message = offset_tool_message(result);
                let source = offset_source(frame, off, end, 0, Some(&message), tag_rules)?;
                self.tool_parts.push(source);
            }
            WalRecord::ToolNoDispatch {
                tool_call_id,
                reason,
                ..
            } => {
                let message =
                    offset_tool_message(&crate::replay::no_dispatch_result(tool_call_id, reason));
                let source = offset_source(frame, off, end, 0, Some(&message), tag_rules)?;
                self.tool_parts.push(source);
            }
            WalRecord::ApprovalRecorded {
                closing_result: Some(result),
                ..
            } => {
                let message = offset_tool_message(result);
                let source = offset_source(frame, off, end, 0, Some(&message), tag_rules)?;
                self.tool_parts.push(source);
            }
            WalRecord::RunFinished { reason, .. } => {
                if let Some(ep) = &mut self.episode {
                    ep.terminal_off = Some(off);
                    ep.terminal_kind = Some(serde_json::to_string(reason)?);
                    ep.state = reason.as_str().to_owned();
                    ep.state_seq = frame.seq;
                }
            }
            WalRecord::RunPaused { .. } => {
                if let Some(ep) = &mut self.episode {
                    ep.state = "paused".into();
                    ep.state_seq = frame.seq;
                }
            }
            WalRecord::CompactionMessage {
                kind,
                tag_checkpoint,
                ..
            } => {
                self.admission.compaction.push(off);
                if let Some(checkpoint) = tag_checkpoint
                    .as_ref()
                    .filter(|_| *kind == broca_wal::CompactionMessageKind::Setup)
                {
                    if let Some(cut) = checkpoint
                        .cut
                        .strip_prefix('m')
                        .and_then(|n| n.parse::<u64>().ok())
                    {
                        delta.reanchor = Some((cut, checkpoint.tag_checkpoint));
                    }
                }
            }
            WalRecord::CompactionAnswer { .. } => self.admission.compaction.push(off),
            _ => {}
        }
        let assistant_message = match record {
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } => Some(broca_protocol::Message::new(
                broca_protocol::Role::Assistant,
                assistant_message.content.clone(),
            )),
            _ => None,
        };
        let budget_message = if let (
            WalRecord::BudgetFired {
                kind,
                threshold,
                caller_text,
                ..
            },
            Some(config),
        ) = (record, &self.budget_config)
        {
            let text = match kind {
                broca_protocol::request::BudgetKind::Nudge => config
                    .nudges
                    .iter()
                    .find(|n| n.at_tool_calls == *threshold)
                    .map(|n| &n.text),
                broca_protocol::request::BudgetKind::Finalize => Some(&config.finalize_text),
            }
            .ok_or(ProjectionPlanError::InconsistentSnapshot(
                "budget firing has no frozen text",
            ))?;
            if caller_text.as_ref().is_some_and(|given| given != text) {
                return Err(ProjectionPlanError::InconsistentSnapshot(
                    "budget firing text differs from frozen config",
                ));
            }
            Some(broca_protocol::Message::new(
                broca_protocol::Role::User,
                vec![broca_protocol::ContentBlock::Text { text: text.clone() }],
            ))
        } else {
            None
        };
        let actions = self
            .placement
            .observe(record)
            .map_err(|_| ProjectionPlanError::InconsistentSnapshot("offset placement failed"))?;
        for action in actions {
            match action {
                PlacementAction::Hold {
                    count,
                    kind: HoldKind::Steer,
                } => {
                    for i in 0..count {
                        self.held_steers.push(offset_source(
                            frame,
                            off,
                            end,
                            i,
                            input_message(record, i),
                            tag_rules,
                        )?);
                    }
                }
                PlacementAction::Hold {
                    kind: HoldKind::Assistant,
                    ..
                } => {
                    let message = match record {
                        WalRecord::ModelStepFinished { .. } => assistant_message.as_ref(),
                        _ => None,
                    };
                    self.held_assistant =
                        Some(offset_source(frame, off, end, 0, message, tag_rules)?);
                }
                PlacementAction::PlaceHeld { first, count } => {
                    let sources = std::mem::take(&mut self.held_steers);
                    if sources.len() as u64 != count {
                        return Err(ProjectionPlanError::InconsistentSnapshot(
                            "held steer sources disagree with placement",
                        ));
                    }
                    for (i, mut source) in sources.into_iter().enumerate() {
                        self.number(&mut source)?;
                        delta.messages.push((first + i as u64, source));
                    }
                }
                PlacementAction::Truncate { to } => {
                    delta.truncate = Some(to);
                    if let Some(high) = self.import_tag_high {
                        self.tag_high = high;
                    }
                }
                PlacementAction::Append { first, count, kind } => {
                    if kind == AppendKind::ToolTurn && count != 0 {
                        let mut assistant = self.held_assistant.take().ok_or(
                            ProjectionPlanError::InconsistentSnapshot(
                                "tool turn lacks assistant source",
                            ),
                        )?;
                        assistant.hi_off = end;
                        assistant.input_end = 1;
                        let mut tools = assistant.clone();
                        tools.input_start = 1;
                        tools.input_end = 2;
                        tools.tag_n = self.tool_parts.iter().map(|s| s.tag_n).sum();
                        tools.tag_high = self
                            .tool_parts
                            .iter()
                            .map(|s| s.tag_high)
                            .max()
                            .unwrap_or(0);
                        tools.tag_base = self.tool_parts.iter().filter_map(|s| s.tag_base).min();
                        self.number(&mut assistant)?;
                        self.number(&mut tools)?;
                        delta.messages.push((first, assistant));
                        delta.messages.push((first + 1, tools));
                        self.tool_parts.clear();
                    } else {
                        for i in 0..count {
                            let message = match record {
                                WalRecord::ModelStepFinished { .. } => assistant_message.as_ref(),
                                WalRecord::BudgetFired { .. } => budget_message.as_ref(),
                                _ => input_message(record, i),
                            };
                            let mut source = offset_source(frame, off, end, i, message, tag_rules)?;
                            self.number(&mut source)?;
                            delta.messages.push((first + i, source));
                        }
                    }
                    if matches!(
                        kind,
                        AppendKind::Assistant | AppendKind::Finalize | AppendKind::ToolTurn
                    ) && count != 0
                    {
                        if let Some(ep) = &mut self.episode {
                            ep.step_first_ord.get_or_insert(first);
                        }
                    }
                }
            }
        }
        let head = self.head_ord();
        if let Some(ep) = &mut self.episode {
            ep.end_ord = head;
        }
        Ok(delta)
    }
}

fn offset_budget_digest(
    budget: Option<&broca_protocol::request::BudgetConfig>,
) -> Result<Option<Vec<u8>>, ProjectionPlanError> {
    use sha2::Digest;
    budget
        .map(|budget| serde_jcs::to_vec(budget).map(|bytes| sha2::Sha256::digest(bytes).to_vec()))
        .transpose()
        .map_err(ProjectionPlanError::Json)
}

fn offset_tool_message(result: &broca_protocol::ToolResult) -> broca_protocol::Message {
    broca_protocol::Message::new(
        broca_protocol::Role::Tool,
        vec![broca_protocol::ContentBlock::ToolResult(result.clone())],
    )
}

fn input_message(record: &WalRecord, i: u64) -> Option<&broca_protocol::Message> {
    match record {
        WalRecord::RunStarted { input, .. }
        | WalRecord::Steered { input, .. }
        | WalRecord::SendDelivered {
            input: Some(input), ..
        } => input.get(i as usize),
        _ => None,
    }
}

fn offset_source(
    frame: &FramedRecord,
    off: u64,
    end: u64,
    i: u64,
    message: Option<&broca_protocol::Message>,
    rules: Option<broca_protocol::tags::ComputedRules>,
) -> Result<OffsetSource, ProjectionPlanError> {
    let mut source = OffsetSource {
        lo_seq: frame.seq,
        lo_off: off,
        hi_off: end,
        input_start: i,
        input_end: i + 1,
        ..OffsetSource::default()
    };
    if let Some(tags) = frame.record.runner_tags().get(i as usize) {
        source.flags = 1;
        source.tag_base = tags
            .parts
            .iter()
            .copied()
            .min()
            .map(|n| n.saturating_sub(1));
        source.tag_high = tags.max_tag().unwrap_or(0);
        source.tag_n = tags.parts.len() as u64;
    } else if let (Some(message), Some(rules)) = (message, rules) {
        let mut message = message.clone();
        broca_protocol::tags::number_message(&mut message, rules, &mut source.tag_n)
            .map_err(|_| ProjectionPlanError::InconsistentSnapshot("tag count overflow"))?;
        source.flags = 2 | if rules.render { 4 } else { 0 } | if rules.clean { 8 } else { 0 };
    }
    Ok(source)
}
