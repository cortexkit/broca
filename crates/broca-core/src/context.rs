//! Effective context = materialized-store prefix ⊕ in-WAL tail.
//!
//! The persister tails the WAL into the materialized store ASYNCHRONOUSLY, so the
//! store LAGS during an active run. The effective context the next provider call
//! needs is therefore the store's materialized prefix PLUS the WAL records the
//! store hasn't projected yet — merged by **WAL SEQ watermark**, NOT `step_id`
//! (a step_id watermark is too coarse: the persister can materialize part of a
//! step and advance while that step's tool records are still WAL-only, dropping
//! or double-including them).
//!
//! The watermark is read ATOMICALLY with the prefix from ONE store transaction —
//! never two racing calls. The persister only advances the watermark at a TURN
//! boundary (it materializes whole committed turns), so the WAL tail past the
//! watermark always begins at a clean turn boundary and [`replay`] reconstructs
//! it coherently.

use std::collections::HashMap;

use crate::billing::ChargeBasisResolver;
use crate::wal_writer::WalStore;
use broca_protocol::FinishReason;
use broca_protocol::{
    caller::{PauseReason, RunFinishReason},
    content::Message,
    error::ProviderError,
    BindIdentity, RunId, Usage,
};
use broca_wal::{FramedRecord, WalByteAnchor, WalRangeRead, WalRecord};

/// The default `session.read` page size.
pub const SESSION_READ_DEFAULT_LIMIT: u64 = 200;

/// The maximum `session.read` page size accepted by the server.
pub const SESSION_READ_MAX_LIMIT: u64 = 500;

/// Selects whether an atomic store read needs the checkpoint's compaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadView {
    Raw,
    Model,
}

mod model_page;
pub use model_page::{
    read_session_model_page, ModelEntry, ModelPage, ModelPageError, ModelPageRequest,
};

/// One atomic read from the materialized store: the reconstructed prefix + the
/// counters it reflects + the WAL seq watermark + the context epoch.
#[derive(Debug, Clone, PartialEq)]
pub struct StorePrefix {
    /// WAL identity captured in the same transaction as the messages and watermark.
    /// Readers must compare it with the authoritative WAL before merging this prefix.
    pub lineage_id: Option<String>,
    /// The provider-valid prompt prefix materialized so far (committed turns).
    pub prefix: Vec<Message>,
    /// Durable model steps folded into `prefix` (so resume continues StepId
    /// numbering without re-reading the whole WAL).
    pub completed_steps: u64,
    /// Accumulated usage folded into `prefix` (cost-correct resume).
    pub usage: Usage,
    /// Provider retries folded into `prefix` (cost/cooldown observability on resume).
    pub retries_used: u32,
    /// Usage from the last projected model step, used by pressure-sensitive transforms.
    pub last_step_usage: Option<Usage>,
    /// Provider finish reason from the last projected model step.
    pub last_step_finish_reason: Option<broca_protocol::FinishReason>,
    /// The highest WAL seq folded into `prefix`. The WAL tail to merge is every
    /// record with `seq > watermark_seq`. The persister only advances this at a
    /// TURN boundary, so the tail always begins at a clean turn boundary.
    pub watermark_seq: u64,
    /// Bumped only when the durable context is rewritten. Render-only compaction does not
    /// longer matches the run's epoch, the materialized prefix is stale.
    pub context_epoch: u64,
}

impl StorePrefix {
    /// An empty prefix at watermark 0 — the cold-start / no-materialization case
    /// (the merge then degenerates to a full-WAL replay).
    pub fn empty() -> Self {
        Self {
            lineage_id: None,
            prefix: Vec::new(),
            completed_steps: 0,
            usage: Usage::default(),
            retries_used: 0,
            last_step_usage: None,
            last_step_finish_reason: None,
            watermark_seq: 0,
            context_epoch: 0,
        }
    }
}

/// The materialized rows a resumed run holds under an applied compaction view (see
/// [`MaterializedStore::read_held_prefix`]): every system message, the view's keep ranges and
/// every message from its coverage end on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldRowsRequest {
    /// The view's keep ranges, each `[from, to)` in transcript ordinals.
    pub keep: Vec<std::ops::Range<u64>>,
    /// The view's coverage end (`covers_through.ordinal + 1`): every message at or past it is
    /// held.
    pub suffix_from: u64,
}

impl HeldRowsRequest {
    /// Whether a non-system message at `ordinal` is held.
    pub fn holds(&self, ordinal: u64) -> bool {
        ordinal >= self.suffix_from || self.keep.iter().any(|range| range.contains(&ordinal))
    }
}

/// One projected episode a held resume needs, with the next projected episode's start.
#[derive(Debug, Clone, PartialEq)]
pub struct HeldEpisode {
    pub projection: EpisodeRenderProjection,
    /// Where the next projected episode starts, which is where this one ends. `None` for the
    /// newest projected episode, which ends where the WAL tail's first episode starts, or at
    /// the session's extent.
    pub next_start: Option<u64>,
}

/// An offset snapshot's compaction selection. The base is the first ordinal after the
/// recorded view's coverage, or the end of the applied range when no view exists;
/// it is not the resident row count.
/// Only locators, coordinates and seeds belong in the index, never hydrated message copies.
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetPromptWindow {
    pub base: u64,
    /// All system-message ordinals below the projection's prefix length, from the same
    /// index snapshot. Systems remain visible even inside a replaced range.
    pub systems: Vec<u64>,
    /// Every episode containing a selected row, and the newest projected episode. Ends
    /// are explicit because unselected episodes may lie between selected ones.
    pub episodes: Vec<HeldEpisode>,
    /// Fold state before retained ranges that skip earlier model steps, and at the newest
    /// projected episode's end when it has steps. Missing state never means empty counters.
    pub guidance: Vec<OffsetGuidanceCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetGuidanceCheckpoint {
    /// First ordinal after the model steps represented by this observer, within its episode.
    pub ordinal: u64,
    /// The projection boundary that published this state, validated before it is used.
    pub at_seq: u64,
    pub fold: crate::tool_repetition::RepetitionCheckpoint,
}

fn offset_window_rows(
    checkpoint: &crate::compaction::CompactionCheckpoint,
) -> Result<HeldRowsRequest, crate::replay::SeedFallback> {
    let bad = || crate::replay::SeedFallback {
        reason: "offset prompt window has no complete compaction checkpoint",
    };
    let state = checkpoint
        .state()
        .filter(|s| s.thinking_boundary_complete())
        .ok_or_else(bad)?;
    if let Some(view) = &state.view {
        let suffix_from = view
            .view
            .covers_through
            .ordinal
            .checked_add(1)
            .ok_or_else(bad)?;
        for range in &view.view.ranges {
            let (from, to) = match range {
                crate::compaction_provider::view::ViewRange::Keep { from, to, .. }
                | crate::compaction_provider::view::ViewRange::Replace { from, to, .. } => {
                    (*from, *to)
                }
                _ => return Err(bad()),
            };
            if from > to || to > suffix_from {
                return Err(bad());
            }
        }
        Ok(HeldRowsRequest {
            keep: view
                .view
                .ranges
                .iter()
                .filter_map(|r| match r {
                    crate::compaction_provider::view::ViewRange::Keep { from, to, .. } => {
                        Some(*from..*to)
                    }
                    _ => None,
                })
                .collect(),
            suffix_from,
        })
    } else {
        let applied = state.applied.as_ref().ok_or_else(bad)?;
        Ok(HeldRowsRequest {
            keep: Vec::new(),
            suffix_from: applied.range.to,
        })
    }
}

pub(crate) fn validate_offset_prompt_window(
    window: &OffsetPromptWindow,
    rows: &[Message],
    seed: &crate::replay::ProjectionSeed,
    boundary: &crate::replay::SeedBoundary,
) -> Result<(), crate::replay::SeedFallback> {
    let bad = || crate::replay::SeedFallback {
        reason: "offset prompt window selection is incomplete",
    };
    let checkpoint = seed
        .admission_checkpoint
        .compaction
        .as_ref()
        .ok_or_else(bad)?;
    if checkpoint
        .state()
        .and_then(|s| s.view.as_ref())
        .is_some_and(|v| Some(v.view.lineage_id.as_str()) != boundary.lineage_id.as_deref())
    {
        return Err(crate::replay::SeedFallback {
            reason: "offset prompt window belongs to another lineage",
        });
    }
    let held = offset_window_rows(checkpoint)?;
    if window.base != held.suffix_from
        || window.base > seed.prefix_len
        || window.systems.windows(2).any(|w| w[0] >= w[1])
        || window.systems.iter().any(|o| *o >= seed.prefix_len)
    {
        return Err(bad());
    }
    if window
        .guidance
        .windows(2)
        .any(|w| w[0].ordinal >= w[1].ordinal)
        || window
            .guidance
            .iter()
            .any(|g| g.at_seq != seed.watermark_seq || g.ordinal > seed.prefix_len)
    {
        return Err(bad());
    }
    let mut ranges = held.keep;
    ranges.push(window.base..seed.prefix_len);
    if seed.prefix_len > 0 {
        ranges.push(seed.prefix_len - 1..seed.prefix_len);
    }
    ranges.extend(window.systems.iter().map(|o| *o..*o + 1));
    ranges.sort_by_key(|r| r.start);
    let mut merged: Vec<std::ops::Range<u64>> = Vec::new();
    for range in ranges {
        if range.start > range.end || range.end > seed.prefix_len {
            return Err(bad());
        }
        if range.is_empty() {
            continue;
        }
        if let Some(last) = merged.last_mut().filter(|last| last.end >= range.start) {
            last.end = last.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    let mut actual = rows.iter();
    for ordinal in merged.into_iter().flatten() {
        let row = actual.next().ok_or_else(bad)?;
        if row.ordinal != Some(ordinal)
            || (row.role == broca_protocol::Role::System)
                != window.systems.binary_search(&ordinal).is_ok()
        {
            return Err(bad());
        }
    }
    if actual.next().is_some() {
        return Err(bad());
    }
    let Some(newest) = window.episodes.last() else {
        return Err(bad());
    };
    if newest.next_start.is_some()
        || newest.projection != seed.open_episode.clone().ok_or_else(bad)?
    {
        return Err(bad());
    }
    Ok(())
}

pub(crate) fn finish_offset_prompt_window(
    state: &mut crate::ReplayState,
    window: OffsetPromptWindow,
    seed: &crate::replay::ProjectionSeed,
    checkpoint: &crate::AdmissionCheckpointV1,
    projected: &[EpisodeRenderProjection],
) -> Result<(), crate::replay::SeedFallback> {
    let bad = || crate::replay::SeedFallback {
        reason: "offset prompt window episode metadata is incomplete",
    };
    let before = seed
        .admission_checkpoint
        .compaction
        .as_ref()
        .and_then(|c| c.state())
        .ok_or_else(bad)?;
    let after = checkpoint
        .compaction
        .as_ref()
        .and_then(|c| c.state())
        .ok_or_else(bad)?;
    if before.applied != after.applied || before.view != after.view || before.strips != after.strips
    {
        return Err(crate::replay::SeedFallback {
            reason: "offset prompt window changed in the tail",
        });
    }
    let render = crate::run::episode_render_inputs(projected)
        .map_err(|_| bad())?
        .ok_or_else(bad)?;
    let mut episodes = Vec::with_capacity(projected.len());
    for (i, projection) in projected.iter().enumerate() {
        let start = projection.start().ok_or_else(bad)?;
        let next = projected
            .get(i + 1)
            .and_then(|e| e.start())
            .unwrap_or(state.extent);
        let end = window
            .episodes
            .get(i)
            .and_then(|e| e.next_start)
            .unwrap_or(next);
        if end < start || end > next || end > state.extent {
            return Err(bad());
        }
        let first_step = projection.first_step().ok_or_else(bad)?;
        if projection
            .first_user()
            .ok_or_else(bad)?
            .is_some_and(|first| first < start || first >= end)
        {
            return Err(bad());
        }
        if first_step.is_some_and(|first| first < start || first >= end) {
            return Err(bad());
        }
        episodes.push(crate::run::held_prompt::GuidanceEpisode {
            render: render[i].clone(),
            first_step,
            end,
        });
    }
    if state.prompt.iter().any(|m| {
        m.ordinal.is_none_or(|o| {
            !episodes
                .iter()
                .any(|e| e.render.prompt_start as u64 <= o && o < e.end)
        })
    }) {
        return Err(bad());
    }
    state.compaction = checkpoint.compaction.clone().ok_or_else(bad)?;
    // The validated prefix already contains Setup, including when the tail opens an episode.
    state.tags_await_setup = false;
    state.compaction_history = after.history.clone();
    crate::run::restore_offset_prompt_guidance(state, episodes, seed, &window)
}

/// The held rows of a materialized prefix, read with the prefix's watermark, identity, length,
/// counters and seeds in ONE store transaction.
///
/// The seeds describe the whole prefix, not the rows read: `trailing_origin` and `tag_seed` come
/// from the prefix's last row (`prefix_len - 1`), exactly what a merge of the complete prefix
/// with the WAL tail would take from it.
#[derive(Debug, Clone, PartialEq)]
pub struct HeldPrefix {
    /// WAL identity captured in the same transaction; compared with the WAL before merging.
    pub lineage_id: Option<String>,
    pub watermark_seq: u64,
    /// How many transcript messages the materialized prefix holds.
    pub prefix_len: u64,
    pub context_epoch: u64,
    pub completed_steps: u64,
    pub usage: Usage,
    pub retries_used: u32,
    pub last_step_usage: Option<Usage>,
    pub last_step_finish_reason: Option<broca_protocol::FinishReason>,
    /// The requested rows below `prefix_len`, in increasing ordinal order, each carrying its
    /// ordinal and tags. The prefix's last row is always included as well (it is read for the
    /// seeds anyway): a resume whose WAL tail holds no message needs the session's newest
    /// message before its first step.
    pub rows: Vec<Message>,
    /// The producing-model origin a WAL tail continuing the prefix's last episode is seeded
    /// with.
    pub trailing_origin: Option<broca_protocol::request::ModelHandle>,
    /// Where tag numbering continues after the prefix.
    pub tag_seed: crate::tags::TagSeed,
    /// The projected episodes the held rows need: every episode containing a returned row, and
    /// the newest projected episode.
    /// `None` when the store holds no usable episode projection at this watermark; the reader
    /// then replays the WAL.
    pub episodes: Option<Vec<HeldEpisode>>,
}

/// One materialized message row, preserving its lineage ordinal.
#[derive(Debug, Clone, PartialEq)]
pub struct StorePageRow {
    pub written_at: Option<u64>,
    pub ordinal: u64,
    pub message: Message,
    pub run: Option<crate::RunAttribution>,
}

/// One episode's durable render inputs, with the state of every value separated from the value
/// itself.
///
/// A resumed run under an applied compaction view reads these from the store projection instead
/// of replaying the whole WAL (see `ModelHistory::episodes`). Each coordinate is an absolute
/// transcript ordinal and states which of three things it is: a known value (a `Some(Some(_))`
/// for the optional coordinates), a known absence (`Some(None)`), or a value nobody has computed
/// yet (`None`), which sends the reader back to the WAL for that episode instead of guessing.
/// Read through the accessors, which never let a stored zero stand in for an unknown ordinal.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EpisodeRenderProjection {
    /// The episode's first message, the ordinal its `RunStarted` input starts at.
    pub start: Option<u64>,
    /// The episode's original admitted first user message, `Some(None)` when its admitted input
    /// carried none (a later steer is never this).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "known_ordinal"
    )]
    pub first_user: Option<Option<u64>>,
    /// The episode's first model-produced message: its first model step's start. `Some(None)`
    /// while the episode has committed no model step.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "known_ordinal"
    )]
    pub first_step: Option<Option<u64>>,
    /// The episode's frozen render config, the bytes its `RunStarted` recorded.
    #[serde(default)]
    pub render_config: Option<serde_json::Value>,
}

impl EpisodeRenderProjection {
    pub fn start(&self) -> Option<u64> {
        self.start
    }

    pub fn first_user(&self) -> Option<Option<u64>> {
        self.first_user
    }

    pub fn first_step(&self) -> Option<Option<u64>> {
        self.first_step
    }

    pub fn render_config(&self) -> Option<&serde_json::Value> {
        self.render_config.as_ref()
    }

    /// Whether every value a reader needs was computed for this episode. `false` means the
    /// reader must take this episode's metadata from the WAL.
    pub fn is_complete(&self) -> bool {
        self.start.is_some()
            && self.first_user.is_some()
            && self.first_step.is_some()
            && self.render_config.is_some()
    }
}

/// The stored form of an ordinal whose absence is itself a known fact.
///
/// Plain serde writes both a known absence (`Some(None)`) and a value nobody computed (`None`) as
/// `null`, which would read back as uncomputed. A known absence is written as the string
/// `"none"` instead, an uncomputed value is omitted, and a stored `null` (written before this
/// encoding) still reads as uncomputed, so it sends the reader to the WAL rather than claiming an
/// absence nobody recorded. A build that predates this encoding fails to parse `"none"` and
/// replays the WAL as well.
mod known_ordinal {
    use serde::{Deserialize, Deserializer, Serializer};

    const KNOWN_NONE: &str = "none";

    pub fn serialize<S: Serializer>(
        value: &Option<Option<u64>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(Some(ordinal)) => serializer.serialize_u64(*ordinal),
            Some(None) => serializer.serialize_str(KNOWN_NONE),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Option<u64>>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Stored {
            Ordinal(u64),
            Marker(String),
        }
        match Option::<Stored>::deserialize(deserializer)? {
            None => Ok(None),
            Some(Stored::Ordinal(ordinal)) => Ok(Some(Some(ordinal))),
            Some(Stored::Marker(marker)) if marker == KNOWN_NONE => Ok(Some(None)),
            Some(Stored::Marker(marker)) => Err(serde::de::Error::custom(format!(
                "unknown episode ordinal marker {marker:?}"
            ))),
        }
    }
}

/// The projected episodes a held resume needs, read from a stored episode projection (the JSON
/// array of [`EpisodeRenderProjection`] the store keeps): every episode containing one of
/// `held` (the ordinals of the rows read), and the newest projected episode.
///
/// Only the selected episodes are decoded whole; the rest are scanned for their coordinates.
/// `None` when the projection cannot be trusted for this, so the reader replays the WAL: an
/// episode without a start, starts out of order, or a first model step outside its own episode
/// (which is how a misaligned step list shows).
pub fn select_held_episodes(
    projection_json: &str,
    held: impl IntoIterator<Item = u64>,
) -> Option<Vec<HeldEpisode>> {
    #[derive(serde::Deserialize)]
    struct Bounds {
        start: Option<u64>,
        #[serde(default, deserialize_with = "known_ordinal::deserialize")]
        first_step: Option<Option<u64>>,
    }
    let bounds: Vec<Bounds> = serde_json::from_str(projection_json).ok()?;
    let entries: Vec<&serde_json::value::RawValue> = serde_json::from_str(projection_json).ok()?;
    let starts = bounds
        .iter()
        .map(|episode| episode.start)
        .collect::<Option<Vec<u64>>>()?;
    if starts.windows(2).any(|pair| pair[0] >= pair[1]) {
        return None;
    }
    for (index, episode) in bounds.iter().enumerate() {
        if let Some(Some(first_step)) = episode.first_step {
            let end = starts.get(index + 1).copied().unwrap_or(u64::MAX);
            if first_step < starts[index] || first_step >= end {
                return None;
            }
        }
    }
    let mut selected = vec![false; starts.len()];
    if let Some(last) = selected.last_mut() {
        *last = true;
    }
    for ordinal in held {
        let containing = starts.partition_point(|start| *start <= ordinal);
        if let Some(index) = containing.checked_sub(1) {
            selected[index] = true;
        }
    }
    selected
        .iter()
        .enumerate()
        .filter(|(_, selected)| **selected)
        .map(|(index, _)| {
            Some(HeldEpisode {
                projection: serde_json::from_str(entries.get(index)?.get()).ok()?,
                next_start: starts.get(index + 1).copied(),
            })
        })
        .collect()
}

/// A boxed future of an episode projection read, shared by the store seams that serve a
/// running loop without blocking its runtime thread.
pub type EpisodeProjectionRead<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<Option<Vec<EpisodeRenderProjection>>, String>>
            + Send
            + 'a,
    >,
>;

/// Rebuildable model-guidance coordinates captured with a page's rows and watermark.
/// Missing metadata means replay, not an episode without repeated calls.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelHistory {
    pub spans: Vec<std::ops::Range<u64>>,
    pub open_span: Option<u64>,
    /// Every episode's render inputs, in lineage order, for a session with an applied view.
    /// `None` when the store holds no complete episode metadata; readers then replay the WAL.
    pub episodes: Option<Vec<EpisodeRenderProjection>>,
    /// The exact ordinal ranges the store read, including episode history and lookahead.
    pub row_ranges: Vec<std::ops::Range<u64>>,
    /// How many transcript messages the materialized prefix holds: the session's extent below
    /// the WAL tail, and the end of the episode that is open at the watermark.
    pub prefix_len: u64,
}

/// The metadata and requested rows captured by one store transaction for `session.read`.
#[derive(Debug, Clone, PartialEq)]
pub struct StorePageSnapshot {
    pub head: Option<crate::head::HeadProjection>,
    pub model_history: Option<ModelHistory>,
    pub watermark_seq: u64,
    /// WAL identity represented by the materialized rows. Without a known matching
    /// WAL identity, readers must discard these rows and replay the complete WAL.
    pub lineage_id: Option<String>,
    /// Last control-stream unit represented by the materialized watermark state.
    pub control_head: Option<crate::Cursor>,
    pub anchor: Option<WalByteAnchor>,
    pub prefix_len: u64,
    /// Model steps represented by the materialized prefix.
    pub completed_steps: u64,
    pub rows: Vec<StorePageRow>,
    /// Model origin needed when the WAL tail begins in the middle of an episode.
    pub trailing_origin: Option<broca_protocol::request::ModelHandle>,
    /// Last episode already represented by the materialized prefix.
    pub last_run_id: Option<RunId>,
    /// Terminal usage for `last_run_id`, if the materialized episode has reported it.
    pub last_episode_usage: Option<Usage>,
    /// The newest caller title (from `RunStarted`, `Queued` or `Steered`) in the materialized
    /// prefix. A title in the WAL tail past `watermark_seq` is newer and overrides it.
    pub last_title: Option<String>,
    /// Approval call states and late answers represented by this exact watermark.
    pub deferred_calls: Option<crate::DeferredCallProjection>,
    /// Captured at the watermark on model-view reads only. Absent forces a complete read
    /// for a compaction session; the raw page path never decodes or consumes this state.
    pub compaction: Option<crate::compaction::CompactionCheckpoint>,
    /// A durable Setup is already covered by this prefix. Only the lineage's first Setup
    /// may renumber tags, so raw reads need this fact even without its replacement payload.
    pub compaction_setup_seen: bool,
    /// Where tag numbering continues after the materialized prefix: read from its last row,
    /// whose stored tags carry the number before it and its run's computed rules.
    pub tag_seed: crate::tags::TagSeed,
}

impl StorePageSnapshot {
    pub fn empty() -> Self {
        Self {
            head: Some(crate::head::HeadProjection::default()),
            model_history: None,
            last_title: None,
            deferred_calls: None,
            watermark_seq: 0,
            lineage_id: None,
            control_head: None,
            anchor: None,
            prefix_len: 0,
            completed_steps: 0,
            rows: Vec::new(),
            trailing_origin: None,
            last_run_id: None,
            last_episode_usage: None,
            compaction: None,
            compaction_setup_seen: false,
            tag_seed: crate::tags::TagSeed::default(),
        }
    }
}

/// Admission metadata captured atomically from one materialized-store transaction.
#[derive(Debug, Clone, PartialEq)]
pub struct AdmissionStoreSnapshot {
    pub watermark_seq: u64,
    /// WAL identity represented by the checkpoint. `None` is legacy/unknown.
    pub lineage_id: Option<String>,
    pub anchor: Option<WalByteAnchor>,
    /// `None` means the row predates checkpoint support or its version/payload is unusable.
    pub checkpoint: Option<crate::AdmissionCheckpointV1>,
}

/// A page-read failure that distinguishes expected attribution migration from store faults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorePageReadError {
    /// The WAL can recover attribution, but this projection has not stored it yet.
    AttributionStale,
    /// Existing store errors retain their diagnostic text without implying a repair class.
    Failure(String),
}

impl std::fmt::Display for StorePageReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AttributionStale => {
                f.write_str("store message run attribution requires WAL backfill")
            }
            Self::Failure(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for StorePageReadError {}

impl From<String> for StorePageReadError {
    fn from(message: String) -> Self {
        Self::Failure(message)
    }
}

impl From<&str> for StorePageReadError {
    fn from(message: &str) -> Self {
        Self::Failure(message.into())
    }
}

/// Where a session's materialized transcript holds a runner-tagged part, from the store's tag
/// index (see [`MaterializedStore::tag_location`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TagLocation {
    /// The part's `(transcript ordinal, block index)`, when a materialized message holds it.
    pub found: Option<(u64, usize)>,
    /// How many transcript messages the index covers: the materialized prefix. A tag not
    /// found can only be in the transcript past this ordinal, which the WAL after the store's
    /// watermark holds.
    pub indexed_through: u64,
}

/// The materialized store read seam. The loop depends only on this trait.
pub trait MaterializedStore: Send + Sync {
    fn read_retained_head(
        &self,
        _session: &BindIdentity,
        _lineage_id: &str,
    ) -> Result<Option<crate::head::RetainedHeadCheckpoint>, String> {
        Ok(None)
    }
    fn checkpoint_retained_head(
        &self,
        _session: &BindIdentity,
        _checkpoint: &crate::head::RetainedHeadCheckpoint,
    ) -> Result<(), String> {
        Ok(())
    }
    /// Look a runner tag up in the store's tag index, without reading any message. `None`
    /// when this store keeps no tag index; the caller then has to read the whole transcript.
    fn tag_location(
        &self,
        _session: &BindIdentity,
        _tag: u64,
    ) -> Result<Option<TagLocation>, String> {
        Ok(None)
    }
    /// How many transcript messages the store holds for the session (its materialized prefix),
    /// read from metadata without reading any message. `None` when this store keeps no such
    /// count. Messages recorded in the WAL past the store's watermark are not included.
    fn materialized_message_count(&self, _session: &BindIdentity) -> Result<Option<u64>, String> {
        Ok(None)
    }
    /// Claim-check events are session-owned data, even though the broker carries only routing metadata.
    fn read_run_event(&self, _event_key: &str) -> Result<Option<(BindIdentity, Vec<u8>)>, String> {
        Ok(None)
    }
    /// Paged reads with a typed migration signal. Legacy implementations keep their existing
    /// errors; only a backend that knows its attribution is uncomputed may classify it stale.
    fn read_page_with_attribution(
        &self,
        session: &BindIdentity,
        from_ordinal: Option<u64>,
        limit: u64,
        view: ReadView,
    ) -> Result<StorePageSnapshot, StorePageReadError> {
        self.read_page_with_watermark(session, from_ordinal, limit, view)
            .map_err(StorePageReadError::Failure)
    }
    /// Read the materialized prefix + counters + watermark + epoch in ONE
    /// consistent transaction (never two racing reads). Any decode or component-read
    /// failure must surface as an error so the caller can fall back to a full WAL replay
    /// instead of merging a partial snapshot.
    /// Messages carry their absolute store-row ordinals; merging never renumbers loaded rows.
    fn read_prefix_with_watermark(&self, session: &BindIdentity) -> Result<StorePrefix, String>;

    /// Read only the rows a run resumed under an applied compaction view holds (`held`), with
    /// the prefix's watermark, identity, length, counters, seeds and the episode projection the
    /// rows need, all in ONE transaction. The rows of the discarded range are never read.
    ///
    /// The default implementation keeps no such read: `Ok(None)` makes the caller merge the
    /// complete prefix instead. A read failure is `Err`, and the caller replays the WAL.
    fn read_held_prefix(
        &self,
        _session: &BindIdentity,
        _held: &HeldRowsRequest,
    ) -> Result<Option<HeldPrefix>, String> {
        Ok(None)
    }

    /// Read the session's projected episode render inputs (start, original first user, first
    /// model step and frozen render config per episode) without reading a transcript message.
    ///
    /// The default implementation keeps no such projection. A store that does returns
    /// `Ok(Some(episodes))` only when every episode's values were computed at the projection's
    /// materialization watermark. `Ok(None)` means they were not: a store written before the
    /// column existed, or one whose projection advanced without recomputing them. A caller that
    /// needs episode coordinates then replays the WAL rather than render from an absence, and
    /// the projection fills the values on its next write. `Err` is a read failure, answered the
    /// same way.
    fn read_episode_projection(
        &self,
        _session: &BindIdentity,
    ) -> Result<Option<Vec<EpisodeRenderProjection>>, String> {
        Ok(None)
    }

    /// The same projection read for a running session's own loop, which must not wait on a
    /// store lock. `Ok(None)` means the store cannot supply the values: it keeps no projection,
    /// the projection is stale or absent, or it belongs to another lineage. The caller then uses
    /// its WAL replay, and the projection fills the values on its next write.
    fn read_episode_projection_async(&self, _session: &BindIdentity) -> EpisodeProjectionRead<'_> {
        Box::pin(async { Ok(None) })
    }

    /// Read the watermark, matching byte anchor, and admission checkpoint in one transaction.
    /// `None` means there is no metadata row; callers use the complete WAL.
    fn read_admission_snapshot(
        &self,
        _session: &BindIdentity,
    ) -> Result<Option<AdmissionStoreSnapshot>, String> {
        Ok(None)
    }

    /// Read page metadata and rows in one transaction. `None` requests the newest
    /// `limit` prefix rows; the caller trims them after deriving the WAL-tail length.
    fn read_page_with_watermark(
        &self,
        session: &BindIdentity,
        _from_ordinal: Option<u64>,
        _limit: u64,
        _view: ReadView,
    ) -> Result<StorePageSnapshot, String> {
        let prefix = self.read_prefix_with_watermark(session)?;
        if prefix.prefix.is_empty() && prefix.watermark_seq == 0 {
            Ok(StorePageSnapshot::empty())
        } else {
            Err("materialized store does not implement atomic paged reads".into())
        }
    }

    /// Atomically replace a projection only while its identity equals `stale_lineage_id`.
    /// `None` repairs a prefix whose identity was never recorded.
    /// `complete_wal` starts at sequence zero and belongs to the replacement lineage.
    fn reproject_if_lineage_mismatch(
        &self,
        _session: &BindIdentity,
        _stale_lineage_id: Option<&str>,
        _complete_wal: &WalRangeRead,
    ) -> Result<(), String> {
        Err("materialized store does not support lineage reprojection".into())
    }
}

/// The WAL→store projection seam. Defined in the loop crate (so `run_loop` can
/// drive it without depending on the SQLite impl); `SqliteStore` implements it.
pub trait Persister: Send + Sync {
    /// Retained transcripts stay WAL-only; only content-free head facts may be checkpointed.
    fn project_retained_head(
        &self,
        _session: &BindIdentity,
        _lineage_id: &str,
        _records: &[FramedRecord],
    ) -> Result<(), String> {
        Ok(())
    }
    /// Read the complete seed for a suffix-only projection. `None` asks the caller to provide
    /// the full WAL because metadata from an older schema cannot safely seed incremental replay.
    /// The default preserves compatibility for persisters that only support full-prefix replay.
    fn projection_seed(
        &self,
        _session: &BindIdentity,
    ) -> Result<Option<crate::ProjectionSeed>, String> {
        Ok(None)
    }

    /// Project newly-COMPLETE turns from `records` (the durable WAL up to a
    /// fsynced point, in seq order) into the store, in ONE transaction. `records` may be either
    /// the complete prefix or the suffix selected from [`Self::projection_seed`]. Idempotent
    /// (re-projecting is a no-op past the watermark). Returns the new watermark.
    /// MUST only advance the watermark at a TURN boundary (so `run_finished` and a
    /// natural-end terminal turn stay WAL-only and the tail carries the trailing
    /// state on resume). A failure is non-fatal (the full-WAL fallback covers it).
    fn project(&self, session: &BindIdentity, records: &[FramedRecord]) -> Result<u64, String>;

    /// Persist only run-level withdrawal discovery for a retained lineage. The
    /// input is deliberately content-free; ordinary projection never receives
    /// that lineage's messages, checkpoint or deferred-call record summary.
    fn project_retention_metadata(
        &self,
        _session: &BindIdentity,
        _metadata: &crate::projection_plan::RetainedWithdrawalProjection,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Project a range while co-committing the anchor for the resulting watermark. Backends with
    /// transactional metadata override this; the compatibility default retains the older
    /// best-effort two-write behavior.
    fn project_range(&self, session: &BindIdentity, range: &WalRangeRead) -> Result<u64, String> {
        let watermark = self.project(
            session,
            match range.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
        )?;
        let anchor = range.anchor_at(watermark);
        self.persist_wal_anchor(session, anchor.as_ref())?;
        Ok(watermark)
    }

    /// Persist a derived byte anchor after projection advances its watermark.
    /// Implementations may ignore `None`, which is the expected value on non-Unix platforms.
    fn persist_wal_anchor(
        &self,
        _session: &BindIdentity,
        _anchor: Option<&WalByteAnchor>,
    ) -> Result<(), String> {
        Ok(())
    }
}

/// The combined session-store seam `run_loop` accepts (read + project). A single
/// `SqliteStore` implements both halves.
pub trait SessionStore: MaterializedStore + Persister {}
impl<T: MaterializedStore + Persister> SessionStore for T {}

/// A run's coarse lifecycle state in the durable run index. This is the index's view —
/// a DERIVED projection of the WAL, NOT the source of truth. An `Active` row can go STALE
/// across a crash (a crash between a run's durable `RunFinished` and the index terminal
/// write leaves the row `Active` for a finished run), and a `Paused` row can go stale after
/// a later resume, so a run-keyed lookup must treat only true terminals as authoritative
/// without WAL reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunIndexState {
    Active,
    Paused,
    Interrupted,
    Completed,
    MaxSteps,
    BudgetExhausted,
    ContextCeiling,
    Cancelled,
    Error,
    /// The prompt-reduction transform was unreachable, so the turn could not render. A
    /// terminal state distinct from `Error` (a provider/loop failure) for the status surface.
    TransformUnavailable,
}

impl RunIndexState {
    /// Whether this state is terminal (the run reached an end; the row won't go stale).
    pub fn is_terminal(&self) -> bool {
        !matches!(self, RunIndexState::Active | RunIndexState::Paused)
    }

    /// The wire/storage string for this state.
    pub fn as_str(&self) -> &'static str {
        match self {
            RunIndexState::Active => "active",
            RunIndexState::Paused => "paused",
            RunIndexState::Interrupted => "interrupted",
            RunIndexState::Completed => "completed",
            RunIndexState::MaxSteps => "max_steps",
            RunIndexState::BudgetExhausted => "budget_exhausted",
            RunIndexState::ContextCeiling => "context_ceiling",
            RunIndexState::Cancelled => "cancelled",
            RunIndexState::Error => "error",
            RunIndexState::TransformUnavailable => "transform_unavailable",
        }
    }

    /// Parse a stored state string (the inverse of [`as_str`](Self::as_str)).
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "active" => RunIndexState::Active,
            "paused" => RunIndexState::Paused,
            "interrupted" => RunIndexState::Interrupted,
            "completed" => RunIndexState::Completed,
            "max_steps" => RunIndexState::MaxSteps,
            "budget_exhausted" => RunIndexState::BudgetExhausted,
            "context_ceiling" => RunIndexState::ContextCeiling,
            "cancelled" => RunIndexState::Cancelled,
            "error" => RunIndexState::Error,
            "transform_unavailable" => RunIndexState::TransformUnavailable,
            _ => return None,
        })
    }
}

/// One row of the durable run index: a run_id resolved to its session + lifecycle state,
/// so run-keyed ops (`run.cancel`/`run.status`) resolve without scanning every WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunIndexRow {
    pub run_id: String,
    pub session: BindIdentity,
    /// The submission this run consumed, if it was a queued submission (else `None`).
    pub submission_id: Option<String>,
    /// Canonical caller-request identity for deterministic send-id retries. `None` identifies a
    /// nonce-minted run or a legacy row whose identity cannot be verified.
    pub request_identity_digest: Option<String>,
    pub state: RunIndexState,
    /// Run-total usage persisted with a terminal state. `None` marks active, paused, or
    /// legacy terminal rows that still require WAL reconciliation.
    pub usage: Option<Usage>,
    /// The provider's final model-step reason when one was observed.
    pub final_step_finish_reason: Option<FinishReason>,
    /// Retries spent across the episode's provider calls.
    pub retries_used: u32,
    /// The caller's title for the send behind this run, written at admission from the same
    /// value the run's `RunStarted` records. `None` for a send without one and for rows written
    /// before the column existed; a reader then falls back to the WAL.
    pub title: Option<String>,
    /// Durable discovery flag for unanswered questions closed without dispatch.
    pub withdrawals_owed: bool,
}

/// The durable run index seam: `run_id → {session, submission_id, state}` in `store.db`, so
/// run-keyed ops survive a restart and resolve a run_id to its session without scanning.
/// OWNED BY THE COORDINATOR (the actor writes it at admission + on terminal) — the loop
/// stays pure-WAL. A DERIVED projection: see [`RunIndexState`] for the stale-Active rule.
pub trait RunIndex: Send + Sync {
    /// Record a run as `Active` at admission (before the coordinator replies `active`), so a
    /// cancel/status racing the reply always resolves the run_id.
    fn index_run_active(&self, row: &RunIndexRow) -> Result<(), String>;

    /// Update a run to its terminal `state` and metadata before the coordinator emits the
    /// terminal. Metadata is optional so legacy and synthetic terminal paths remain representable.
    fn index_run_terminal(
        &self,
        run_id: &str,
        state: RunIndexState,
        usage: Option<Usage>,
        final_step_finish_reason: Option<FinishReason>,
        retries_used: u32,
    ) -> Result<(), String>;

    /// Publish content-free durable evidence atomically with the lifecycle state. Older index
    /// implementations may omit attribution, but must still publish the terminal normally.
    fn index_run_attributed(
        &self,
        run_id: &str,
        state: RunIndexState,
        usage: Option<Usage>,
        final_step_finish_reason: Option<FinishReason>,
        retries_used: u32,
        _attribution: &RunIndexAttribution,
    ) -> Result<(), String> {
        self.index_run_terminal(run_id, state, usage, final_step_finish_reason, retries_used)
    }

    /// Look up a run by id. `None` = unknown run (the lookup falls back to the WAL).
    fn lookup_run(&self, run_id: &str) -> Result<Option<RunIndexRow>, String>;

    /// Only the session actor clears this flag, after deriving empty debt from the full WAL.
    fn index_run_withdrawals_owed(&self, _run_id: &str, _owed: bool) -> Result<(), String> {
        Err("run index does not track withdrawal debt".into())
    }

    /// Boot discovery, including terminal sessions that ordinary restart scans never open.
    fn runs_with_withdrawals_owed(&self) -> Result<Vec<WithdrawalRun>, String> {
        Err("run index does not track withdrawal debt".into())
    }
}

/// Scalar evidence from a run's frozen selection and durable lifecycle records. No provider
/// messages, render configuration, prompts or tool payloads belong in the run-index evidence.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RunIndexAttribution {
    pub provider_module_id: Option<String>,
    pub credential_id: Option<String>,
    pub error_class: Option<String>,
    pub error_code: Option<String>,
    pub pause_reason: Option<String>,
    pub interrupt_reason: Option<String>,
    pub at_ms: Option<u64>,
}

impl RunIndexAttribution {
    /// Attributing writers share this fold of durable lifecycle records. A start resets the
    /// evidence; a later pause or terminal keeps only the frozen selection and its own facts.
    pub fn observe(&mut self, record: &broca_wal::WalRecord) {
        use broca_wal::WalRecord;
        let (error, at_ms) = match record {
            WalRecord::RunStarted { config, .. } => {
                *self = Self {
                    provider_module_id: Some(config.model.provider_module_id.clone()),
                    credential_id: config
                        .auth_selection
                        .as_ref()
                        .map(|s| s.credential_id.clone()),
                    ..Self::default()
                };
                return;
            }
            WalRecord::RunPaused {
                reason,
                error,
                ts_ms,
                ..
            } => {
                self.pause_reason = Some(reason.as_str().into());
                self.interrupt_reason = None;
                (error, ts_ms)
            }
            WalRecord::RunFinished {
                error,
                ts_ms,
                interrupt_reason,
                ..
            } => {
                self.pause_reason = None;
                self.interrupt_reason = interrupt_reason
                    .as_deref()
                    .filter(|reason| {
                        matches!(
                            *reason,
                            "runner_shutdown"
                                | "runner_crash"
                                | "caller_stop"
                                | "caller_cancel"
                                | "cancelled"
                        )
                    })
                    .map(str::to_owned);
                (error, ts_ms)
            }
            _ => return,
        };
        self.at_ms = *at_ms;
        if let Some(credential) = error
            .as_ref()
            .and_then(|error| error.credential_id.as_ref())
        {
            self.credential_id = Some(credential.clone());
        }
        self.error_class = error.as_ref().map(|error| {
            broca_protocol::OpenErrorClass::from(error.class)
                .as_str()
                .to_owned()
        });
        // Provider codes are an open string and may echo request content. Only the closed
        // operational vocabulary is safe to copy into a content-free index.
        self.error_code = error
            .as_ref()
            .and_then(|error| error.provider_code.as_deref())
            .filter(|code| {
                matches!(
                    *code,
                    "account_limit_exhausted"
                        | "upstream_api_key_rejected"
                        | "rate_limit_exceeded"
                        | "rate_limited"
                        | "invalid_api_key"
                        | "incorrect_api_key"
                        | "context_length_exceeded"
                        | "context_window_exceeded"
                        | "billing_exhausted"
                        | "credit_insufficient"
                        | "credit_balance_exhausted"
                        | "organization_spend_limit_exceeded"
                        | "project_spend_limit_exceeded"
                        | "usage_not_included"
                        | "invalid_prompt"
                        | "cyber_policy"
                        | "bio_policy"
                        | "misalignment_policy_violation"
                        | "usage_limit_reached"
                        | "content_filter"
                        | "server_error"
                        | "stream_too_slow"
                        | "response_deadline"
                        | "output_ceiling"
                        | "previous_process_ended_unsealed"
                        | "websocket_auth_rejected"
                        | "request_invalid"
                        | "previous_response_not_found"
                        | "ENTITLEMENT_ERROR"
                )
            })
            .map(str::to_owned);
    }
}

#[cfg(test)]
mod diagnostics_attribution_tests {
    use super::RunIndexAttribution;
    use broca_wal::WalRecord;
    use serde_json::json;

    #[test]
    fn diagnostics_shared_derivation_handles_start_pause_terminal_and_unrelated_records() {
        let start: WalRecord = serde_json::from_value(json!({
            "type":"run_started","run_id":"run","session":{"project_root":"/project","harness":"test","session":"session"},
            "input":[],"config":{"model":{"provider_module_id":"openai","model_id":"model"},"tools":[],
                "tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},
                "context_epoch":0,"renderer_version":1,"tools_were_omitted":false,
                "auth_selection":{"credential_id":"credential-one"}}
        })).unwrap();
        let mut a = RunIndexAttribution::default();
        a.observe(&start);
        assert_eq!(a.provider_module_id.as_deref(), Some("openai"));
        assert_eq!(a.credential_id.as_deref(), Some("credential-one"));
        let pause:WalRecord=serde_json::from_value(json!({"type":"run_paused","run_id":"run","reason":"auth_required","ts_ms":10,
            "error":{"class":"auth_required","message":"PRIVATE TEXT","provider_code":"PRIVATE CODE","credential_id":"credential-two"}})).unwrap();
        a.observe(&pause);
        assert_eq!(a.pause_reason.as_deref(), Some("auth_required"));
        assert_eq!(a.error_class.as_deref(), Some("auth_required"));
        assert_eq!(a.credential_id.as_deref(), Some("credential-two"));
        assert_eq!(a.error_code, None);
        assert_eq!(a.at_ms, Some(10));
        let finish: WalRecord = serde_json::from_value(
            json!({"type":"run_finished","reason":"interrupted","ts_ms":20,
            "usage":{},"interrupt_reason":"runner_shutdown"}),
        )
        .unwrap();
        a.observe(&finish);
        assert_eq!(a.pause_reason, None);
        assert_eq!(a.error_class, None);
        assert_eq!(a.interrupt_reason.as_deref(), Some("runner_shutdown"));
        assert_eq!(a.at_ms, Some(20));
        let prior = a.clone();
        a.observe(&WalRecord::WriterClosed { writer_id: [0; 16] });
        assert_eq!(a, prior);
    }
}

/// A run whose session actor must service durable question withdrawals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithdrawalRun {
    pub run_id: RunId,
    pub session: BindIdentity,
}

/// A source-bound position in the daemon-wide usage export stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExportCursor {
    pub source_instance_id: String,
    pub source_epoch: u64,
    pub export_seq: u64,
}

/// One immutable usage fact in the daemon-wide export stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportFact {
    pub export_seq: u64,
    pub segment: crate::system_one_fact::UsageExportSegment,
}

/// A consistent page read from the daemon-wide usage export index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportPage {
    pub source_instance_id: String,
    pub source_epoch: u64,
    pub facts: Vec<ExportFact>,
    pub next: Option<ExportCursor>,
    pub high_water: u64,
}

/// A terminal run-index row that has not yet produced any usage-export facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingExportRun {
    pub run_id: RunId,
    pub session: BindIdentity,
}

/// An export read or emission failure. Epoch changes are a typed, recoverable condition: the
/// consumer discards its cursor and reconciles immutable segments using each segment's `fact_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportError {
    EpochChanged {
        source_instance_id: String,
        source_epoch: u64,
        high_water: u64,
    },
    Storage(String),
}

impl std::fmt::Display for ExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EpochChanged {
                source_instance_id,
                source_epoch,
                high_water,
            } => write!(
                f,
                "export source changed: instance {source_instance_id} epoch {source_epoch} (high_water {high_water})"
            ),
            Self::Storage(message) => write!(f, "usage export storage error: {message}"),
        }
    }
}

impl std::error::Error for ExportError {}

/// The durable usage export index. The coordinator calls `emit_run` only after a terminal WAL
/// record and the run-index terminal update are durable. The serve adapter uses `read_page` for
/// a lease-free, daemon-scoped pull.
pub trait ExportIndex: Send + Sync {
    /// Durably mark run ids executed on another machine so they are not billed
    /// again here. This additive set (the billing floor) is scoped by the session
    /// and the move's `cut_id`; repeats preserve all previously recorded ids.
    /// The ids must come from [`crate::imported_run_ids`] against the local WAL
    /// before replacement. Commit before installing the received WAL so a crash
    /// cannot expose imported episodes to local billing. Projection resets and
    /// retention must never remove the set.
    ///
    /// Backends without durable floor support refuse activation rather than risk
    /// producing local usage facts for runs billed by the source machine.
    fn record_imported_runs(
        &self,
        _session: &BindIdentity,
        _cut_id: &str,
        _run_ids: &[RunId],
    ) -> Result<(), ExportError> {
        Err(ExportError::Storage(
            "export index does not support an imported-run billing floor".into(),
        ))
    }

    /// List terminal run-index rows that have no corresponding export fact. The durable run
    /// index acts as the catch-up outbox for the normal terminal-write-before-export ordering.
    /// Runs recorded as imported are excluded: their facts belong to the source
    /// machine, so absence here is not a missed local export.
    fn missing_terminal_runs(&self) -> Result<Vec<MissingExportRun>, ExportError>;

    fn emit_run(
        &self,
        records: &[FramedRecord],
        run_id: &RunId,
        resolver: &dyn ChargeBasisResolver,
    ) -> Result<(), ExportError>;

    fn read_page(&self, since: Option<ExportCursor>, limit: u64)
        -> Result<ExportPage, ExportError>;

    fn high_water(&self) -> u64;

    /// Immutable source identity cached when the export store opens. These getters must not
    /// perform storage I/O because budget admission calls them synchronously.
    fn source_epoch(&self) -> u64;

    fn source_instance_id(&self) -> &str;
}

/// Re-derive usage facts for terminal run-index rows missed by the live export path.
///
/// Callers run this off the async runtime's blocking pool. Missing rows are grouped by
/// session so one WAL read heals every missed run in that lineage. Sessions are handled one at
/// a time, in the order the index first lists them, and each WAL is dropped before the next is
/// read: after an export outage many sessions can be missing rows, and holding every one of
/// their WALs at once at boot could exhaust memory. Within a session, runs are emitted in the
/// index's order. `fact_id` uniqueness makes overlapping live emission and repeated catch-up
/// harmless, including a retry after a later session's read failed.
pub fn catch_up_usage_exports(
    index: &dyn ExportIndex,
    wal_store: &dyn WalStore,
    resolver: &dyn ChargeBasisResolver,
) -> Result<usize, ExportError> {
    let missing = index.missing_terminal_runs()?;
    let count = missing.len();
    let mut sessions: Vec<(BindIdentity, Vec<RunId>)> = Vec::new();
    let mut position_by_session = HashMap::new();
    for run in missing {
        let position = *position_by_session
            .entry(run.session.clone())
            .or_insert_with(|| {
                sessions.push((run.session.clone(), Vec::new()));
                sessions.len() - 1
            });
        sessions[position].1.push(run.run_id);
    }

    for (session, run_ids) in sessions {
        let records = wal_store.read_records(&session).map_err(|error| {
            ExportError::Storage(format!(
                "read WAL for missed export run {}: {error}",
                run_ids[0].0
            ))
        })?;
        for run_id in &run_ids {
            index.emit_run(&records, run_id, resolver)?;
            // Each emit has released the store connection. Give admissions and the
            // census a chance to acquire it before the next catch-up transaction.
            std::thread::yield_now();
        }
    }
    Ok(count)
}

/// Export every terminal run in this WAL before deleting its lineage, including
/// runs with existing facts. Passing the whole lineage preserves later warm facts;
/// the backend's immutable fact-id uniqueness makes repeating emission harmless.
/// This deliberately does not consult the missing-facts outbox or run index.
pub fn emit_lineage_usage_exports(
    index: &dyn ExportIndex,
    records: &[FramedRecord],
    resolver: &dyn ChargeBasisResolver,
) -> Result<usize, ExportError> {
    let mut current = None;
    let mut emitted = std::collections::HashSet::new();
    for frame in records {
        match &frame.record {
            WalRecord::RunStarted { run_id, .. } => current = Some(run_id),
            WalRecord::RunFinished { .. } => {
                let Some(run_id) = current.take() else {
                    continue;
                };
                if emitted.insert(run_id.clone()) {
                    index.emit_run(records, run_id, resolver)?;
                }
            }
            _ => {}
        }
    }
    Ok(emitted.len())
}

/// One message returned by `session.read`: the normal serialized message plus its durable
/// prompt ordinal, which is kept out of [`Message`]'s serde shape for render byte stability.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionReadMessage {
    pub written_at: Option<u64>,
    pub ordinal: u64,
    pub message: Message,
    pub run: Option<crate::RunAttribution>,
}

/// The lifecycle vocabulary returned beside a transcript page. The names match the run-status
/// wire state strings, with `Idle` added for a session whose WAL has no episode yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionReadStateKind {
    Idle,
    Active,
    Paused,
    Interrupted,
    Completed,
    MaxSteps,
    BudgetExhausted,
    ContextCeiling,
    Cancelled,
    Error,
    TransformUnavailable,
}

impl SessionReadStateKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionReadStateKind::Idle => "idle",
            SessionReadStateKind::Active => "active",
            SessionReadStateKind::Paused => "paused",
            SessionReadStateKind::Interrupted => "interrupted",
            SessionReadStateKind::Completed => "completed",
            SessionReadStateKind::MaxSteps => "max_steps",
            SessionReadStateKind::BudgetExhausted => "budget_exhausted",
            SessionReadStateKind::ContextCeiling => "context_ceiling",
            SessionReadStateKind::Cancelled => "cancelled",
            SessionReadStateKind::Error => "error",
            SessionReadStateKind::TransformUnavailable => "transform_unavailable",
        }
    }
}

/// Why the returned lineage state reached the reported state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionReadReason {
    ContextCeilingWait,
    /// A rewrite ride's reported wait (llm-runner 0.5.2 §6).
    CompactionWait,
    Terminal(RunFinishReason),
    Pause(PauseReason),
}

impl SessionReadReason {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionReadReason::ContextCeilingWait => "context_ceiling_wait",
            SessionReadReason::CompactionWait => {
                cortexkit_role_llm_runner::run::RUN_REASON_COMPACTION_WAIT
            }
            SessionReadReason::Terminal(RunFinishReason::Completed) => "completed",
            SessionReadReason::Terminal(RunFinishReason::MaxSteps) => "max_steps",
            SessionReadReason::Terminal(RunFinishReason::BudgetExhausted) => "budget_exhausted",
            SessionReadReason::Terminal(RunFinishReason::ContextCeiling) => "context_ceiling",
            SessionReadReason::Terminal(RunFinishReason::Cancelled) => "cancelled",
            SessionReadReason::Terminal(RunFinishReason::Interrupted) => "interrupted",
            SessionReadReason::Terminal(RunFinishReason::Error) => "error",
            SessionReadReason::Terminal(RunFinishReason::TransformUnavailable) => {
                "transform_unavailable"
            }
            SessionReadReason::Pause(PauseReason::AuthRequired) => "auth_required",
            SessionReadReason::Pause(PauseReason::Restart) => "restart",
            SessionReadReason::Pause(PauseReason::ToolHeld) => "tool_held",
        }
    }
}

/// The lineage state paired with a transcript page, derived from the same WAL snapshot used
/// to assemble that page.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionReadLineageState {
    pub last_run_id: Option<RunId>,
    pub state: SessionReadStateKind,
    pub reason: Option<SessionReadReason>,
    pub error: Option<ProviderError>,
    pub usage: Option<Usage>,
}

/// A `session.read` page over the effective prompt.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionReadPage {
    pub messages: Vec<SessionReadMessage>,
    pub next_from_ordinal: Option<u64>,
    /// Last durable control unit covered by the exact store-plus-WAL snapshot used for this page.
    pub head: Option<crate::Cursor>,
    /// Durable identity of the WAL object, absent for a legacy lineage that has not been written.
    pub lineage_id: Option<broca_wal::WalLineageId>,
    pub lineage_state: SessionReadLineageState,
    /// How many messages the whole transcript holds: one past its newest ordinal.
    pub prompt_len: u64,
    pub deferred_calls: Option<crate::DeferredCallProjection>,
    pub late_approvals: Vec<crate::LateApprovalView>,
    pub withdrawals_owed: bool,
    /// The running episode's tag numbers wait for its compaction Setup answer, which may
    /// anchor them at a checkpoint; a read refuses rather than show numbers that could change.
    pub tags_await_setup: bool,
}

/// Assemble one transcript page from atomically selected store rows plus the bounded WAL tail.
pub fn read_session_page(
    snapshot: &StorePageSnapshot,
    framed_tail: &[FramedRecord],
    from_ordinal: Option<u64>,
    limit: Option<u64>,
) -> Result<SessionReadPage, crate::ReplayError> {
    read_session_page_from_rows(
        snapshot,
        framed_tail,
        from_ordinal,
        limit,
        |discard_materialized_seed, start, end| {
            snapshot
                .rows
                .iter()
                .filter(|row| {
                    !discard_materialized_seed && row.ordinal >= start && row.ordinal < end
                })
                .cloned()
                .map(|row| SessionReadMessage {
                    written_at: row.written_at,
                    ordinal: row.ordinal,
                    message: row.message,
                    run: row.run,
                })
                .collect()
        },
    )
    .map(|(page, _)| page)
}

/// Assemble one transcript page while consuming the store snapshot so its decoded messages move
/// directly into the response.
pub fn read_session_page_owned(
    snapshot: StorePageSnapshot,
    framed_tail: &[FramedRecord],
    from_ordinal: Option<u64>,
    limit: Option<u64>,
) -> Result<SessionReadPage, crate::ReplayError> {
    read_session_page_owned_with_dispatches(snapshot, framed_tail, from_ordinal, limit)
        .map(|(page, _)| page)
}

/// [`read_session_page_owned`], also answering the dispatch facts of every tool turn the WAL
/// tail commits, keyed by ordinal. They come from the same tail replay that builds the page, so
/// they cost nothing beyond it; a turn already in the materialized prefix is not among them.
pub fn read_session_page_owned_with_dispatches(
    mut snapshot: StorePageSnapshot,
    framed_tail: &[FramedRecord],
    from_ordinal: Option<u64>,
    limit: Option<u64>,
) -> Result<
    (
        SessionReadPage,
        std::collections::BTreeMap<u64, Vec<ToolCallDispatch>>,
    ),
    crate::ReplayError,
> {
    let rows = std::mem::take(&mut snapshot.rows);
    read_session_page_from_rows(
        &snapshot,
        framed_tail,
        from_ordinal,
        limit,
        move |discard_materialized_seed, start, end| {
            rows.into_iter()
                .filter(|row| {
                    !discard_materialized_seed && row.ordinal >= start && row.ordinal < end
                })
                .map(|row| SessionReadMessage {
                    written_at: row.written_at,
                    ordinal: row.ordinal,
                    message: row.message,
                    run: row.run,
                })
                .collect()
        },
    )
}

fn read_session_page_from_rows(
    snapshot: &StorePageSnapshot,
    framed_tail: &[FramedRecord],
    from_ordinal: Option<u64>,
    limit: Option<u64>,
    materialized_rows: impl FnOnce(bool, u64, u64) -> Vec<SessionReadMessage>,
) -> Result<
    (
        SessionReadPage,
        std::collections::BTreeMap<u64, Vec<ToolCallDispatch>>,
    ),
    crate::ReplayError,
> {
    let capped_limit = limit
        .unwrap_or(SESSION_READ_DEFAULT_LIMIT)
        .clamp(1, SESSION_READ_MAX_LIMIT);
    let times = crate::message_times::written_times_with_queued(
        framed_tail.iter().map(|frame| &frame.record),
        crate::placement::Placement::new(snapshot.prefix_len),
        snapshot
            .head
            .as_ref()
            .map(|head| head.queued_times().clone())
            .unwrap_or_default(),
    )
    .map_err(|reason| crate::ReplayError::CorruptLineage {
        episode_run_id: snapshot
            .last_run_id
            .clone()
            .unwrap_or_else(|| RunId(String::new())),
        reason,
    })?;
    let discard_materialized_seed = snapshot.completed_steps == 0
        && crate::replay::first_session_record_is_interrupted(
            framed_tail.iter().map(|framed| &framed.record),
        );
    let mut tail_state = crate::replay::replay_seeded(
        framed_tail.iter().map(|framed| &framed.record),
        snapshot.trailing_origin.clone(),
        snapshot.last_run_id.clone(),
        None,
        None,
        snapshot.prefix_len,
        false,
        snapshot.tag_seed,
    )?;
    if tail_state.run_id.is_none() {
        tail_state.run_id.clone_from(&snapshot.last_run_id);
    }

    // Reads expose an assistant turn once a durable intent makes it survive recovery.
    // While its tools are running, replay keeps it
    // out of the provider-valid prompt until the batch closes; attribution must not wait
    // for that closure, or an in-flight dispatch intent would be invisible to readers.
    let mut read_turns = tail_state.committed_tool_turns.clone();
    if let Some((message, step_record)) = tail_state.open_tool_turn.take() {
        read_turns.push((tail_state.prompt.len(), step_record));
        tail_state.prompt.push(message);
        tail_state
            .message_runs
            .push(tail_state.open_tool_run.take());
    }
    let tail_len = tail_state.prompt.len() as u64;
    let prefix_len = if discard_materialized_seed {
        0
    } else {
        snapshot.prefix_len
    };
    let prompt_len = prefix_len.saturating_add(tail_len);
    let start = match from_ordinal {
        Some(from) => from.min(prompt_len),
        None => prompt_len.saturating_sub(capped_limit),
    };
    let end = start.saturating_add(capped_limit).min(prompt_len);
    let mut messages = materialized_rows(discard_materialized_seed, start, end);

    let tail_start = start.saturating_sub(prefix_len).min(tail_len);
    let tail_end = end.saturating_sub(prefix_len).min(tail_len);
    messages.extend(
        tail_state.prompt[tail_start as usize..tail_end as usize]
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, mut message)| {
                let ordinal = prefix_len + tail_start + index as u64;
                message.ordinal = Some(ordinal);
                SessionReadMessage {
                    written_at: times.get(&ordinal).copied(),
                    ordinal,
                    message,
                    run: tail_state.message_runs[(tail_start + index as u64) as usize].clone(),
                }
            }),
    );
    for message in &mut messages {
        if let Some(run) = &mut message.run {
            if let Some(final_run) = tail_state
                .run_finals
                .get(&message.ordinal)
                .filter(|final_run| final_run.run_id == run.run_id)
            {
                *run = final_run.clone();
            }
        }
    }
    let next_from_ordinal = (end < prompt_len).then_some(end);
    let has_completed_step = tail_state.completed_steps > 0
        || (snapshot.completed_steps > 0
            && !framed_tail
                .iter()
                .any(|framed| matches!(framed.record, WalRecord::RunStarted { .. })));
    let mut lineage_state =
        lineage_state_from_replay_and_records(&tail_state, framed_tail, has_completed_step);
    if lineage_state.usage.is_none()
        && !matches!(
            lineage_state.state,
            SessionReadStateKind::Idle | SessionReadStateKind::Active
        )
    {
        lineage_state.usage = (lineage_state.last_run_id == snapshot.last_run_id)
            .then_some(snapshot.last_episode_usage)
            .flatten()
            .filter(|_| has_completed_step);
    }
    if discard_materialized_seed
        && matches!(
            tail_state.resolution,
            crate::ReplayResolution::Interrupted { .. }
        )
    {
        lineage_state.state = SessionReadStateKind::Interrupted;
        lineage_state.reason = Some(SessionReadReason::Terminal(RunFinishReason::Interrupted));
    }

    let head = crate::last_control_cursor_through(framed_tail, None).or(snapshot.control_head);
    let deferred_calls = crate::replay::project_deferred_calls(
        snapshot.deferred_calls.as_ref(),
        framed_tail,
        snapshot.lineage_id.as_deref(),
        snapshot.last_run_id.as_ref(),
    );
    let late_approvals = deferred_calls
        .as_ref()
        .map_or_else(Vec::new, |view| view.late_approvals());
    let withdrawals_owed = deferred_calls
        .as_ref()
        .is_some_and(|view| !view.obligations().withdrawals.is_empty());
    let dispatches = dispatches_of_turns(framed_tail, &read_turns, prefix_len);
    Ok((
        SessionReadPage {
            messages,
            next_from_ordinal,
            head,
            lineage_id: None,
            lineage_state,
            prompt_len,
            deferred_calls: deferred_calls.filter(|view| !view.calls.is_empty()),
            late_approvals,
            withdrawals_owed,
            tags_await_setup: tail_state.tags_await_setup && !snapshot.compaction_setup_seen,
        },
        dispatches,
    ))
}

/// What `session.read` reports about one tool call of an assistant message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallDispatch {
    /// The model's id for the call. Display only: a model may reuse one id in several steps.
    pub tool_call_id: String,
    /// The call's dispatch-intent sequence, used with the log identity only when
    /// the record carries no prepared key; see [`tool_call_key`].
    pub intent_seq: Option<u64>,
    /// A prepared key preserved by the dispatch intent, when consent preparation
    /// made the call durable before the intent was written.
    pub recorded_call_key: Option<String>,
    /// The module the call was dispatched to, as its intent recorded it. Absent for a call never
    /// dispatched, and for one whose intent was written before broca recorded the module: that
    /// is never guessed.
    pub dispatched_to: Option<String>,
    /// The call has a durable dispatch intent and no result, so it may or may not have run.
    pub indeterminate: bool,
}

/// An ordinary call's fallback key, from its own `ToolDispatchIntent`.
///
/// Ordinary calls use their own intent: sibling calls share a step record but have
/// distinct intents. A synced sequence is never reissued, and the log identity
/// distinguishes replacements at one address. Consent preparation can make a call
/// durable earlier; its key is then recorded on the later intent and preserved by
/// [`tool_call_key`]. Model ids may repeat without reusing either kind of key.
/// The spelling is printable ASCII, below the tool-provider role's 256-byte bound.
pub fn call_key(lineage_id: broca_wal::WalLineageId, intent_seq: u64) -> String {
    tool_call_key(Some(&lineage_id.to_string()), intent_seq, None)
        .expect("a known log identity yields a key")
}

/// Use the durable key the provider received. Approved calls retain their earlier
/// preparation key; only records without one derive a key from their intent.
pub fn tool_call_key(
    lineage_id: Option<&str>,
    intent_seq: u64,
    recorded: Option<&str>,
) -> Option<String> {
    recorded
        .map(str::to_owned)
        .or_else(|| lineage_id.map(|lineage| format!("{lineage}:{intent_seq}")))
}

/// The dispatch facts of the committed tool turns `turns` of a replay over `framed`, keyed by
/// the ordinal of each turn's assistant message (`ordinal_base` plus its index in the replayed
/// prompt), its calls in their order.
///
/// Each turn names the record index of the step that produced it, so its calls are tied to
/// that step's own dispatch records by position: the records after the step up to its batch's
/// `ToolBatchFinished`. Nothing is matched by content, so an imported message holding the same
/// calls as a real step never borrows that step's facts. Only each turn's own records are walked,
/// so the cost follows the turns asked about, not the length of `framed`.
pub fn dispatches_of_turns(
    framed: &[FramedRecord],
    turns: &[(usize, usize)],
    ordinal_base: u64,
) -> std::collections::BTreeMap<u64, Vec<ToolCallDispatch>> {
    use std::collections::{HashMap, HashSet};
    let mut out = std::collections::BTreeMap::new();
    for &(prompt_index, step_index) in turns {
        let Some(WalRecord::ModelStepFinished {
            assistant_message, ..
        }) = framed.get(step_index).map(|frame| &frame.record)
        else {
            continue;
        };
        let mut intents: HashMap<&str, (u64, Option<String>, Option<String>)> = HashMap::new();
        let mut resolved: HashSet<&str> = HashSet::new();
        for frame in &framed[step_index + 1..] {
            match &frame.record {
                WalRecord::ToolDispatchIntent {
                    tool_call_id,
                    module_id,
                    call_key,
                    ..
                } => {
                    intents.entry(tool_call_id.0.as_str()).or_insert((
                        frame.seq,
                        module_id.clone(),
                        call_key.clone(),
                    ));
                }
                WalRecord::ToolResult { result, .. } => {
                    resolved.insert(result.tool_call_id.0.as_str());
                }
                WalRecord::ToolNoDispatch { tool_call_id, .. } => {
                    resolved.insert(tool_call_id.0.as_str());
                }
                WalRecord::ToolBatchFinished { .. }
                | WalRecord::ModelStepFinished { .. }
                | WalRecord::RunStarted { .. } => break,
                _ => {}
            }
        }
        let entries = assistant_message
            .tool_calls()
            .map(|call| {
                let id = call.tool_call_id.0.as_str();
                let intent = intents.get(id);
                ToolCallDispatch {
                    tool_call_id: id.to_owned(),
                    intent_seq: intent.map(|(seq, _, _)| *seq),
                    recorded_call_key: intent.and_then(|(_, _, key)| key.clone()),
                    dispatched_to: intent.and_then(|(_, module, _)| module.clone()),
                    indeterminate: intent.is_some() && !resolved.contains(id),
                }
            })
            .collect();
        out.insert(ordinal_base + prompt_index as u64, entries);
    }
    out
}

/// The dispatch facts of every committed tool turn of the transcript `framed` (a session's
/// whole WAL, in order) replays to, keyed by ordinal. See [`dispatches_of_turns`].
pub fn tool_call_dispatches(
    framed: &[FramedRecord],
) -> Result<std::collections::BTreeMap<u64, Vec<ToolCallDispatch>>, crate::ReplayError> {
    let replayed = crate::replay::replay_seeded(
        framed.iter().map(|frame| &frame.record),
        None,
        None,
        None,
        None,
        0,
        false,
        crate::tags::TagSeed::default(),
    )?;
    Ok(dispatches_of_turns(
        framed,
        &replayed.committed_tool_turns,
        0,
    ))
}

/// When `record` was written, in milliseconds since the Unix epoch, for a record that says.
pub fn record_time_ms(record: &WalRecord) -> Option<u64> {
    match record {
        WalRecord::RunStarted { ts_ms, .. }
        | WalRecord::RunFinished { ts_ms, .. }
        | WalRecord::RunPaused { ts_ms, .. }
        | WalRecord::RunConfigRefrozen { ts_ms, .. }
        | WalRecord::RunAuthRefrozen { ts_ms, .. } => *ts_ms,
        WalRecord::ModelAttemptFinished { ts_ms, .. } => Some(*ts_ms),
        _ => None,
    }
}

/// The transcript `framed` (a session's whole WAL, in order) replays to, as a `session.read`
/// page numbers it: index `i` is ordinal `i`.
pub fn replay_transcript(framed: &[FramedRecord]) -> Result<Vec<Message>, crate::ReplayError> {
    Ok(crate::replay::replay_seeded(
        framed.iter().map(|frame| &frame.record),
        None,
        None,
        None,
        None,
        0,
        false,
        crate::tags::TagSeed::default(),
    )?
    .prompt)
}

/// Full-transcript implementation retained to prove the paged path returns identical results.
#[cfg(test)]
fn read_session_page_full(
    prefix: &StorePrefix,
    framed: &[FramedRecord],
    from_ordinal: Option<u64>,
    limit: Option<u64>,
) -> Result<SessionReadPage, crate::ReplayError> {
    let capped_limit = limit
        .unwrap_or(SESSION_READ_DEFAULT_LIMIT)
        .clamp(1, SESSION_READ_MAX_LIMIT);
    let tail: Vec<WalRecord> = framed
        .iter()
        .filter(|f| f.seq > prefix.watermark_seq)
        .map(|f| f.record.clone())
        .collect();
    let replay = resume_state_from_store(
        prefix,
        &tail,
        recover_run_config(framed),
        recover_run_id(framed),
        None,
    )?;
    let prompt_len = replay.prompt.len() as u64;
    let start = from_ordinal
        .map(|from| from.min(prompt_len))
        .unwrap_or_else(|| prompt_len.saturating_sub(capped_limit));
    let end = start.saturating_add(capped_limit).min(prompt_len);
    let runs = crate::replay(
        &framed
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .unwrap()
    .message_runs;
    let messages = replay.prompt[start as usize..end as usize]
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, message)| SessionReadMessage {
            written_at: None,
            run: runs[(start + index as u64) as usize].clone(),
            ordinal: message.ordinal.unwrap_or(start + index as u64),
            message,
        })
        .collect();
    Ok(SessionReadPage {
        messages,
        next_from_ordinal: (end < prompt_len).then_some(end),
        head: crate::project_control(framed)
            .last()
            .map(|(cursor, _)| *cursor),
        lineage_id: None,
        lineage_state: lineage_state_from_replay_and_records(
            &replay,
            framed,
            replay.completed_steps > 0,
        ),
        prompt_len,
        deferred_calls: crate::replay::project_deferred_calls(None, framed, None, None),
        late_approvals: crate::replay::project_deferred_calls(None, framed, None, None)
            .map_or_else(Vec::new, |view| view.late_approvals()),
        withdrawals_owed: crate::replay::project_deferred_calls(None, framed, None, None)
            .is_some_and(|view| !view.obligations().withdrawals.is_empty()),
        tags_await_setup: replay.tags_await_setup,
    })
}

/// Merge the materialized prefix with the in-WAL tail (records past the
/// watermark) into the effective context.
///
/// `tail` MUST be exactly the WAL records with `seq > prefix.watermark_seq`
/// (i.e. `wal.tail_after_seq(prefix.watermark_seq)`), in seq order. Because the
/// watermark sits at a projectable boundary, replaying the tail reconstructs only
/// messages not already committed to the prefix.
pub(crate) fn materialized_zero_step_seed_was_interrupted(
    completed_steps: u64,
    tail: &[WalRecord],
) -> bool {
    completed_steps == 0 && crate::replay::first_session_record_is_interrupted(tail)
}

pub fn effective_context(
    prefix: &StorePrefix,
    tail: &[WalRecord],
) -> Result<Vec<Message>, crate::ReplayError> {
    let mut prompt = prefix.prefix.clone();
    let abandoned = materialized_zero_step_seed_was_interrupted(prefix.completed_steps, tail);
    if abandoned {
        prompt.clear();
    }
    // The tail may begin mid-episode (its `RunStarted` already materialized into the
    // prefix), so seed the tail replay's producing-model origin from the prefix's trailing
    // assistant turn — otherwise the tail's leading assistant turns would stamp `origin:
    // None` and diverge from a full replay.
    prompt.extend(
        crate::replay::replay_seeded(
            tail,
            trailing_origin(&prefix.prefix),
            None,
            None,
            None,
            if abandoned {
                0
            } else {
                prefix.prefix.len() as u64
            },
            false,
            crate::tags::TagSeed::from_prefix(&prefix.prefix),
        )?
        .prompt,
    );
    Ok(prompt)
}

/// The producing-model origin of the last assistant message in `prefix` (the model the
/// in-progress episode is running under), used to seed a mid-episode tail replay.
///
/// CORRECT IFF THE WATERMARK ADVANCES ONLY AT A TURN BOUNDARY (never mid-turn). The
/// persister advances the materialization watermark only on `turn_finished`, so a tail
/// that begins mid-episode E implies at least one turn of E finished and materialized
/// before the watermark — hence at least one E-produced assistant turn is in `prefix`, and
/// its origin IS E's model. The seed is therefore always THIS episode's model, never a
/// stale prior episode's. If the watermark ever advanced mid-turn, this could pull the
/// previous episode's origin and silently diverge resume bytes — the
/// `merge_equals_full_replay_at_every_turn_boundary` test is the standing proof the
/// invariant holds.
fn trailing_origin(prefix: &[Message]) -> Option<broca_protocol::request::ModelHandle> {
    prefix.iter().rev().find_map(|m| m.origin.clone())
}

/// Build the full resume state from `store-prefix ⊕ WAL-tail` (the fast path that
/// reads only the tail from disk, not the whole WAL).
///
/// The watermark leaves the records needed to classify the episode in `tail`. At a
/// completed no-tool boundary, the assistant message is already in the prefix while its
/// `RunFinished` remains in the tail; at a tool-turn boundary, all later episode state is
/// in the tail. The prompt = prefix ⊕ tail's prompt; counters = prefix's + the tail's.
/// Equivalent to a full-WAL replay, which remains the correctness ground truth / fallback.
///
/// `run_config` is passed in EXPLICITLY: once the watermark has advanced, the tail
/// EXCLUDES the `RunStarted` that carries it, so `replay(tail).run_config` would be `None`.
/// Admission recovers the effective typed config from its checkpoint plus WAL tail and threads it
/// here, avoiding a full-WAL read while preserving refrozen renderer state.
pub fn resume_state_from_store(
    prefix: &StorePrefix,
    tail: &[WalRecord],
    run_config: Option<crate::RunConfig>,
    run_id: Option<crate::RunId>,
    budget_state: Option<crate::BudgetReplayStateV1>,
) -> Result<crate::ReplayState, crate::ReplayError> {
    let seed = PrefixSeed {
        prefix_len: prefix.prefix.len() as u64,
        completed_steps: prefix.completed_steps,
        usage: prefix.usage,
        retries_used: prefix.retries_used,
        last_step_usage: prefix.last_step_usage,
        last_step_finish_reason: prefix.last_step_finish_reason,
        trailing_origin: trailing_origin(&prefix.prefix),
        tag_seed: crate::tags::TagSeed::from_prefix(&prefix.prefix),
    };
    merge_prefix_and_tail(
        prefix.prefix.clone(),
        &seed,
        tail,
        run_config,
        run_id,
        budget_state,
    )
    .map(|(state, _)| state)
}

/// What a resume takes from a materialized prefix besides the rows it holds. Every value
/// describes the WHOLE prefix, so a merge that loads only some of its rows (see
/// [`MaterializedStore::read_held_prefix`]) seeds the tail exactly as a merge of all of them.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PrefixSeed {
    /// How many transcript messages the prefix holds: the tail's first ordinal.
    pub prefix_len: u64,
    pub completed_steps: u64,
    pub usage: Usage,
    pub retries_used: u32,
    pub last_step_usage: Option<Usage>,
    pub last_step_finish_reason: Option<broca_protocol::FinishReason>,
    /// The producing-model origin of the prefix's trailing assistant turn.
    pub trailing_origin: Option<broca_protocol::request::ModelHandle>,
    /// Where tag numbering continues after the prefix.
    pub tag_seed: crate::tags::TagSeed,
}

/// What the tail replay of a merge knew about episodes, in the tail's own coordinates, which a
/// merge of the complete prefix discards because the prefix carries no episode boundaries.
#[derive(Debug, Clone, Default)]
pub(crate) struct TailEpisodes {
    /// The tail's episode step spans, as indexes into the tail's own prompt.
    pub spans: Vec<std::ops::Range<usize>>,
    /// The render inputs of every episode the tail starts, at absolute ordinals.
    pub render: Vec<broca_protocol::request::EpisodeRenderInput>,
    /// How many messages the tail placed: the merged prompt's last ones.
    pub prompt_len: usize,
}

/// The rows of a materialized prefix a merge loaded: all of them, or only a held set.
struct LoadedRows {
    prefix: Vec<Message>,
}

/// Merge `rows` (messages of a materialized prefix, possibly only some of them) with the WAL
/// `tail` past the prefix's watermark. `seed` describes the complete prefix. The returned
/// prompt is `rows` followed by the tail's messages.
pub(crate) fn merge_prefix_and_tail(
    rows: Vec<Message>,
    seed: &PrefixSeed,
    tail: &[WalRecord],
    run_config: Option<crate::RunConfig>,
    run_id: Option<crate::RunId>,
    budget_state: Option<crate::BudgetReplayStateV1>,
) -> Result<(crate::ReplayState, TailEpisodes), crate::ReplayError> {
    let abandoned = materialized_zero_step_seed_was_interrupted(seed.completed_steps, tail);
    // Seed the mid-episode tail's origin from the prefix's trailing assistant turn (see
    // `effective_context`) so a resumed turn stamps the same model a full replay would.
    let tail_state = crate::replay::replay_seeded(
        tail,
        seed.trailing_origin.clone(),
        run_id.clone(),
        run_config.as_ref().and_then(|config| config.budget.clone()),
        budget_state,
        if abandoned { 0 } else { seed.prefix_len },
        seed.prefix_len > 0,
        seed.tag_seed,
    )?;
    // A view the prefix's leading records applied or replaced: a replacement at the transcript's
    // start, so the prompt no longer leads with the lineage's first message and the tail's
    // episode coordinates are not the lineage's either.
    let leading_view = tail
        .iter()
        .fold(
            crate::compaction::CompactionCheckpoint::None,
            |mut checkpoint, record| {
                checkpoint.fold(record);
                checkpoint
            },
        )
        .state()
        .is_some_and(|state| {
            state
                .applied
                .as_ref()
                .is_some_and(|applied| applied.range.from == 0)
                || state.view.as_ref().is_some_and(|applied| {
                    applied.view.ranges.iter().any(|range| match range {
                        crate::compaction_provider::view::ViewRange::Keep { from, .. }
                        | crate::compaction_provider::view::ViewRange::Replace { from, .. } => {
                            *from == 0
                        }
                        // A range mark this build does not know cannot be shown to start the
                        // transcript, so the tail's episode coordinates are not trusted either.
                        _ => true,
                    })
                })
        });
    // Leave room for the resumed run's next turns. Cloning the exact-capacity prefix would
    // otherwise copy the entire transcript again when the first new message is appended.
    let prefix = LoadedRows { prefix: rows };
    let mut prompt = Vec::with_capacity(prefix.prefix.len() + tail_state.prompt.len() + 256);
    prompt.extend(prefix.prefix);
    if abandoned {
        prompt.clear();
    }
    let mut tail_episodes = TailEpisodes {
        spans: Vec::new(),
        render: Vec::new(),
        prompt_len: tail_state.prompt.len(),
    };
    // Episode boundaries are only known for what the tail replay itself walked. With an empty
    // prefix the tail IS the whole lineage, so its spans index the merged prompt exactly;
    // otherwise the materialized messages carry no episode boundaries. The decision keys on the
    // prefix's length, never on how many of its rows were loaded.
    let episode_step_spans = if seed.prefix_len == 0 {
        tail_state.episode_step_spans
    } else {
        tail_episodes.spans = tail_state.episode_step_spans.unwrap_or_default();
        None
    };
    prompt.extend(tail_state.prompt);
    let starts_new_episode = tail
        .iter()
        .any(|record| matches!(record, WalRecord::RunStarted { .. }));
    let (
        completed_steps,
        usage,
        retries_used,
        last_step_usage,
        last_step_finish_reason,
        provider_call_started,
    ) = if starts_new_episode {
        (
            tail_state.completed_steps,
            tail_state.usage,
            tail_state.retries_used,
            tail_state.last_step_usage,
            tail_state.last_step_finish_reason,
            tail_state.provider_call_started,
        )
    } else {
        let mut usage = seed.usage;
        usage.accumulate(&tail_state.usage);
        let tail_retries = if tail
            .iter()
            .any(|record| matches!(record, WalRecord::RunFinished { .. }))
        {
            tail_state.retries_used.saturating_sub(seed.retries_used)
        } else {
            tail_state.retries_used
        };
        (
            seed.completed_steps + tail_state.completed_steps,
            usage,
            seed.retries_used.saturating_add(tail_retries),
            tail_state.last_step_usage.or(seed.last_step_usage),
            if tail.iter().any(|record| {
                matches!(
                    record,
                    WalRecord::ModelStepFinished { .. }
                        | WalRecord::Retracted {
                            failure: Some(_),
                            ..
                        }
                )
            }) {
                tail_state.last_step_finish_reason
            } else {
                seed.last_step_finish_reason
            },
            seed.completed_steps > 0 || tail_state.provider_call_started,
        )
    };
    tail_episodes.render = tail_state.render_episodes.clone().unwrap_or_default();
    let state = crate::ReplayState {
        extent: tail_state.extent,
        tags_await_setup: tail_state.tags_await_setup,
        input_budget: tail_state.input_budget,
        cancel_at: tail_state.cancel_at,
        // This prefix supplies model input rather than transcript reads, so it carries no run metadata.
        message_runs: vec![None; prompt.len()],
        run_finals: tail_state.run_finals,
        open_tool_run: tail_state.open_tool_run,
        compaction: tail_state.compaction,
        compaction_history: tail_state.compaction_history,
        output_reservation: tail_state.output_reservation,
        compaction_notice_floor: 1,
        manifest: None,
        on_restart: tail_state.on_restart,
        keep_warm: tail_state.keep_warm,
        open_tool_turn: tail_state.open_tool_turn,
        marker_order_version: tail_state.marker_order_version,
        // The checkpoint's effective config wins; fall back to the tail's only if the caller
        // did not supply one (for example, when the tail still includes RunStarted).
        run_config: run_config.or(tail_state.run_config),
        // Likewise, the checkpoint supplies the last episode's run id independent of the
        // materialization watermark; fall back to the tail's.
        run_id: run_id.or(tail_state.run_id),
        prompt,
        completed_steps,
        tool_call_count: tail_state.tool_call_count,
        budget_nudges_fired: tail_state.budget_nudges_fired,
        budget_finalize_fired: tail_state.budget_finalize_fired,
        budget_finalize_pending: tail_state.budget_finalize_pending,
        usage,
        retries_used,
        // Last-step metadata is committed atomically with the projection watermark, so a
        // pressure-sensitive transform sees the same context signal after materialized resume.
        last_step_usage,
        last_step_finish_reason,
        provider_call_started,
        run_finished_durable: tail_state.run_finished_durable,
        episode_step_spans,
        render_episodes: if seed.prefix_len == 0 || leading_view {
            tail_state.render_episodes
        } else {
            None
        },
        open_tool_batch: tail_state.open_tool_batch,
        // The store's watermark is never placed with a steer pending across it (see
        // `latest_projection_boundary`), so every unplaced steer is in the tail.
        pending_steers: tail_state.pending_steers,
        resolution: tail_state.resolution,
        // Only what the tail recorded: the materialized prefix keeps no schedule.
        refusal_fallback: tail_state.refusal_fallback,
        refused_steps: tail_state.refused_steps,
        committed_tool_turns: Vec::new(),
        held: None,
    };
    Ok((state, tail_episodes))
}

fn lineage_state_from_replay_and_records(
    replay: &crate::ReplayState,
    framed: &[FramedRecord],
    has_completed_step: bool,
) -> SessionReadLineageState {
    if framed.is_empty() && replay.run_id.is_none() {
        return SessionReadLineageState {
            last_run_id: None,
            state: SessionReadStateKind::Idle,
            reason: None,
            error: None,
            usage: None,
        };
    }

    if replay.compaction.state().is_some_and(|state| {
        matches!(
            state.ceiling_check,
            Some(crate::compaction::CeilingCheck::Waiting { .. })
        )
    }) {
        return SessionReadLineageState {
            last_run_id: replay.run_id.clone(),
            state: SessionReadStateKind::Active,
            reason: Some(SessionReadReason::ContextCeilingWait),
            error: None,
            usage: None,
        };
    }
    if replay
        .compaction
        .state()
        .is_some_and(|state| state.ride_wait.is_some())
    {
        return SessionReadLineageState {
            last_run_id: replay.run_id.clone(),
            state: SessionReadStateKind::Active,
            reason: Some(SessionReadReason::CompactionWait),
            error: None,
            usage: None,
        };
    }
    match &replay.resolution {
        crate::ReplayResolution::LineageIdle { last_reason } => {
            let terminal = last_terminal_for_run(framed, replay.run_id.as_ref());
            SessionReadLineageState {
                last_run_id: replay.run_id.clone(),
                state: state_for_terminal(*last_reason),
                reason: Some(SessionReadReason::Terminal(*last_reason)),
                error: terminal.as_ref().and_then(|(_, _, error)| error.clone()),
                usage: terminal.and_then(|(_, usage, _)| has_completed_step.then_some(usage)),
            }
        }
        crate::ReplayResolution::Interrupted { .. }
            if zero_step_seeded_episode(framed, replay.run_id.as_ref()) =>
        {
            SessionReadLineageState {
                last_run_id: replay.run_id.clone(),
                state: SessionReadStateKind::Interrupted,
                reason: Some(SessionReadReason::Terminal(RunFinishReason::Interrupted)),
                error: None,
                usage: None,
            }
        }
        crate::ReplayResolution::Interrupted { .. } => SessionReadLineageState {
            last_run_id: replay.run_id.clone(),
            state: SessionReadStateKind::Active,
            reason: None,
            error: None,
            usage: None,
        },
        crate::ReplayResolution::Resume { .. } => SessionReadLineageState {
            last_run_id: replay.run_id.clone(),
            state: SessionReadStateKind::Active,
            reason: None,
            error: None,
            usage: None,
        },
        crate::ReplayResolution::ProviderHeld { run_id, .. } => SessionReadLineageState {
            last_run_id: Some(run_id.clone()),
            state: SessionReadStateKind::Paused,
            reason: Some(SessionReadReason::Pause(PauseReason::ToolHeld)),
            error: None,
            usage: None,
        },
        crate::ReplayResolution::AuthRequired {
            run_id,
            reason,
            error,
            ..
        } => SessionReadLineageState {
            last_run_id: Some(run_id.clone()),
            state: SessionReadStateKind::Paused,
            reason: Some(SessionReadReason::Pause(*reason)),
            error: error.as_deref().cloned(),
            usage: None,
        },
    }
}

fn zero_step_seeded_episode(framed: &[FramedRecord], run_id: Option<&RunId>) -> bool {
    let Some(run_id) = run_id else {
        return false;
    };
    let mut seeded = false;
    for frame in framed {
        match &frame.record {
            WalRecord::RunStarted {
                run_id: durable_run_id,
                origin,
                ..
            } => {
                seeded = durable_run_id == run_id
                    && matches!(origin, broca_wal::RunOrigin::SeededFrom { .. });
            }
            WalRecord::StepStarted { .. } | WalRecord::ModelStepFinished { .. } if seeded => {
                seeded = false;
            }
            _ => {}
        }
    }
    seeded
}

fn state_for_terminal(reason: RunFinishReason) -> SessionReadStateKind {
    match reason {
        RunFinishReason::Completed => SessionReadStateKind::Completed,
        RunFinishReason::MaxSteps => SessionReadStateKind::MaxSteps,
        RunFinishReason::BudgetExhausted => SessionReadStateKind::BudgetExhausted,
        RunFinishReason::ContextCeiling => SessionReadStateKind::ContextCeiling,
        RunFinishReason::Cancelled => SessionReadStateKind::Cancelled,
        RunFinishReason::Interrupted => SessionReadStateKind::Interrupted,
        RunFinishReason::Error => SessionReadStateKind::Error,
        RunFinishReason::TransformUnavailable => SessionReadStateKind::TransformUnavailable,
    }
}

fn last_terminal_for_run(
    framed: &[FramedRecord],
    target: Option<&RunId>,
) -> Option<(RunFinishReason, Usage, Option<ProviderError>)> {
    let mut active_run: Option<RunId> = None;
    let mut saw_run_started = false;
    let mut terminal = None;
    for framed in framed {
        match &framed.record {
            WalRecord::RunStarted { run_id, .. } => {
                saw_run_started = true;
                active_run = Some(run_id.clone());
            }
            WalRecord::RunFinished {
                reason,
                usage,
                error,
                ..
            } if target.is_none_or(|target| active_run.as_ref() == Some(target)) => {
                terminal = Some((*reason, *usage, error.clone()));
            }
            _ => {}
        }
    }
    if terminal.is_none() && !saw_run_started {
        terminal = framed.iter().rev().find_map(|framed| match &framed.record {
            WalRecord::RunFinished {
                reason,
                usage,
                error,
                ..
            } => Some((*reason, *usage, error.clone())),
            _ => None,
        });
    }
    terminal
}

#[cfg(test)]
fn recover_run_config(framed: &[FramedRecord]) -> Option<crate::RunConfig> {
    let mut config = None;
    for framed in framed {
        match &framed.record {
            WalRecord::RunStarted {
                config: started, ..
            } => config = Some((**started).clone()),
            WalRecord::RunConfigRefrozen {
                renderer_version,
                frozen_render_config,
                ..
            } => {
                if let Some(current) = config.as_mut() {
                    current.renderer_version = *renderer_version;
                    current.frozen_render_config = frozen_render_config.clone();
                }
            }
            WalRecord::RunAuthRefrozen { auth_selection, .. } => {
                if let Some(current) = config.as_mut() {
                    current.auth_selection = Some(auth_selection.clone());
                }
            }
            _ => {}
        }
    }
    config
}

#[cfg(test)]
fn recover_run_id(framed: &[FramedRecord]) -> Option<RunId> {
    framed.iter().rev().find_map(|f| match &f.record {
        WalRecord::RunStarted { run_id, .. } => Some(run_id.clone()),
        _ => None,
    })
}

#[cfg(test)]
mod catch_up_tests {
    use std::sync::Mutex;

    use super::*;
    use broca_wal::{Wal, WalError};

    struct CountingWalStore {
        reads: Mutex<HashMap<BindIdentity, usize>>,
    }

    thread_local! {
        /// WAL reads and export emissions made on this thread, in order.
        static EVENTS: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    fn note_event(event: String) {
        EVENTS.with(|events| events.borrow_mut().push(event));
    }

    impl WalStore for CountingWalStore {
        fn open(&self, _session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
            Err(WalError::Io(std::io::Error::other(
                "catch-up never opens an append handle",
            )))
        }

        fn read_records(&self, session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
            note_event(format!("read {}", session.project_root.display()));
            *self
                .reads
                .lock()
                .expect("read counter lock")
                .entry(session.clone())
                .or_default() += 1;
            Ok(Vec::new())
        }
    }

    struct RecordingExportIndex {
        missing: Vec<MissingExportRun>,
        emitted: Mutex<Vec<RunId>>,
        seen_frames: Mutex<Vec<usize>>,
        fail_run: Option<RunId>,
    }

    impl ExportIndex for RecordingExportIndex {
        fn missing_terminal_runs(&self) -> Result<Vec<MissingExportRun>, ExportError> {
            Ok(self.missing.clone())
        }

        fn emit_run(
            &self,
            records: &[FramedRecord],
            run_id: &RunId,
            _resolver: &dyn ChargeBasisResolver,
        ) -> Result<(), ExportError> {
            if self.fail_run.as_ref() == Some(run_id) {
                return Err(ExportError::Storage("injected export failure".into()));
            }
            note_event(format!("emit {}", run_id.0));
            self.seen_frames.lock().unwrap().push(records.len());
            self.emitted
                .lock()
                .expect("emitted runs lock")
                .push(run_id.clone());
            Ok(())
        }

        fn read_page(
            &self,
            _since: Option<ExportCursor>,
            _limit: u64,
        ) -> Result<ExportPage, ExportError> {
            Err(ExportError::Storage("not used by catch-up".into()))
        }

        fn high_water(&self) -> u64 {
            0
        }

        fn source_epoch(&self) -> u64 {
            1
        }

        fn source_instance_id(&self) -> &str {
            "catch-up-test"
        }
    }

    /// Each session's WAL is read, its missing runs are emitted, and only then is the next
    /// session's WAL read, so at most one session's WAL is resident at a time. Runs keep the
    /// index's order within their session.
    #[test]
    fn catch_up_reads_each_missing_session_once_and_emits_it_before_reading_the_next() {
        let first = BindIdentity::new("/tmp/first", "runner", "session");
        let second = BindIdentity::new("/tmp/second", "runner", "session");
        let index = RecordingExportIndex {
            seen_frames: Mutex::new(Vec::new()),
            fail_run: None,
            missing: vec![
                MissingExportRun {
                    run_id: RunId("r1".into()),
                    session: first.clone(),
                },
                MissingExportRun {
                    run_id: RunId("r2".into()),
                    session: second.clone(),
                },
                MissingExportRun {
                    run_id: RunId("r3".into()),
                    session: first.clone(),
                },
            ],
            emitted: Mutex::new(Vec::new()),
        };
        let wal = CountingWalStore {
            reads: Mutex::new(HashMap::new()),
        };
        let resolver = |_: &str, _: &str| (crate::ChargeBasis::Unknown, 1);

        EVENTS.with(|events| events.borrow_mut().clear());
        assert_eq!(catch_up_usage_exports(&index, &wal, &resolver), Ok(3));
        assert_eq!(
            *wal.reads.lock().expect("read counter lock"),
            HashMap::from([(first, 1), (second, 1)])
        );
        assert_eq!(
            EVENTS.with(|events| events.borrow().clone()),
            [
                "read /tmp/first",
                "emit r1",
                "emit r3",
                "read /tmp/second",
                "emit r2"
            ],
            "every missing run of one session is emitted before the next session's WAL is read"
        );
    }

    #[test]
    fn lineage_export_emits_every_terminal_run_including_existing_facts_and_late_warms() {
        let mut records = crate::retention::tests::frames(Some(3600));
        let mut second = crate::retention::tests::frames(None);
        if let WalRecord::RunStarted { run_id, .. } = &mut second[0].record {
            *run_id = RunId("r2".into());
        }
        records.append(&mut second);
        records.push(FramedRecord {
            seq: 11,
            fence: 1,
            record: WalRecord::CacheWarmed {
                attempt_id: None,
                reservation: None,
                envelope: None,
                trigger: None,
                run_id: RunId("r1".into()),
                step_id: broca_protocol::StepId(1),
                request_body_sha256: "aa".into(),
                warm_body_sha256: "bb".into(),
                usage: Usage::default(),
                served_account_ref: None,
                ratelimit_headers: Default::default(),
                sent_at_ms: 3000,
                ttl_ms: None,
                expires_at_ms: None,
            },
        });
        records.push(FramedRecord {
            seq: 12,
            fence: 1,
            record: crate::retention::tests::started("paused", None),
        });
        records.push(FramedRecord {
            seq: 13,
            fence: 1,
            record: WalRecord::RunPaused {
                ts_ms: Some(4000),
                reason: broca_protocol::PauseReason::Restart,
                run_id: RunId("paused".into()),
                error: None,
            },
        });
        for (index, frame) in records.iter_mut().enumerate() {
            frame.seq = index as u64 + 1;
        }
        let index = RecordingExportIndex {
            // r1 already has facts and therefore is absent from the catch-up outbox.
            missing: vec![MissingExportRun {
                run_id: RunId("r2".into()),
                session: BindIdentity::new("/retention-test", "runner", "session"),
            }],
            emitted: Mutex::new(Vec::new()),
            seen_frames: Mutex::new(Vec::new()),
            fail_run: None,
        };
        let resolver = |_: &str, _: &str| (crate::ChargeBasis::Unknown, 1);
        assert_eq!(
            emit_lineage_usage_exports(&index, &records, &resolver),
            Ok(2)
        );
        assert_eq!(
            *index.emitted.lock().unwrap(),
            vec![RunId("r1".into()), RunId("r2".into())]
        );
        assert_eq!(
            *index.seen_frames.lock().unwrap(),
            vec![records.len(); 2],
            "every emitter receives the late warm, not an episode prefix"
        );
        // Repeat emission still visits both runs; the backend deduplicates fact ids.
        assert_eq!(
            emit_lineage_usage_exports(&index, &records, &resolver),
            Ok(2)
        );
        let failing = RecordingExportIndex {
            fail_run: Some(RunId("r1".into())),
            ..index
        };
        assert!(
            emit_lineage_usage_exports(&failing, &records, &resolver).is_err(),
            "deletion must not proceed after export failure"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::replay;
    use broca_protocol::{
        caller::RunFinishReason,
        content::{AssistantMessage, ContentBlock, Role, ToolCall, ToolOutput, ToolResult},
        BatchId, ErrorClass, MessageId, RunId, StepId, ToolCallId, Usage,
    };
    use broca_wal::{AuthSelection, DispatchOutcome, FramedRecord, RunConfig, WalRecord};

    #[test]
    fn resume_prompt_reserves_room_for_new_turns() {
        let mut prefix = StorePrefix::empty();
        prefix.prefix = vec![Message::new(Role::User, vec![]); 1000];
        let mut state = resume_state_from_store(&prefix, &[], None, None, None).unwrap();
        assert!(state.prompt.capacity() >= state.prompt.len() + 256);
        let original = state.prompt.as_ptr();
        state
            .prompt
            .extend(vec![Message::new(Role::Assistant, vec![]); 256]);
        assert_eq!(state.prompt.as_ptr(), original);
    }

    fn interrupted() -> WalRecord {
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Interrupted,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        }
    }

    /// An informational record at the head of a zero-step tail must not keep a seed that the
    /// Interrupted terminal behind it discards. Both context.rs sites now ask the one shared
    /// question replay asks, so this pins all three to one answer. The control arms matter: a
    /// seed with completed steps is never discarded, and a non-Interrupted first record keeps it.
    #[test]
    fn an_informational_record_first_does_not_hide_an_interrupted_zero_step_seed() {
        assert!(materialized_zero_step_seed_was_interrupted(
            0,
            &[WalRecord::Unknown, interrupted()]
        ));
        assert!(materialized_zero_step_seed_was_interrupted(
            0,
            &[interrupted()]
        ));
        assert!(!materialized_zero_step_seed_was_interrupted(
            1,
            &[WalRecord::Unknown, interrupted()]
        ));
        assert!(!materialized_zero_step_seed_was_interrupted(
            0,
            &[WalRecord::Unknown, WalRecord::Unknown]
        ));
    }

    // A small multi-turn WAL: user → (tool turn: step1 + batch) → (terminal no-tool
    // step2) → run_finished. The terminal step intentionally has no TurnFinished.
    fn sample_wal() -> Vec<WalRecord> {
        vec![
            WalRecord::RunStarted {
                written_at_ms: None,
                tags: None,
                session_class: None,
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
                run_id: RunId("r".into()),
                session: BindIdentity::new("/tmp/p", "h", "s"),
                ts_ms: None,
                config: Box::new(RunConfig {
                    turn_directives_v1: None,
                    render_tags: false,
                    computed_tags: None,
                    model: broca_protocol::request::ModelHandle::new("stub", "m"),
                    tools: vec![].into(),
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
                input: vec![Message {
                    cache_prefix_blocks: None,
                    role: Role::User,
                    content: vec![ContentBlock::Text { text: "hi".into() }],
                    origin: None,
                    ordinal: None,
                    tags: Default::default(),
                }]
                .into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(1),
                assistant_message: AssistantMessage {
                    message_id: MessageId("m1".into()),
                    content: vec![ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: ToolCallId("t1".into()),
                        tool_name: "edit".into(),
                        input: serde_json::json!({}),
                    })],
                }
                .into(),
                usage: Usage::default(),
                finish_reason: None,
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::ToolBatchStarted {
                batch_id: BatchId("b1".into()),
                step_id: StepId(1),
                planned_calls: vec![ToolCallId("t1".into())],
            },
            WalRecord::ToolDispatchIntent {
                dispatched_at_ms: None,
                hooks: None,
                batch_id: BatchId("b1".into()),
                tool_call_id: ToolCallId("t1".into()),
                tool_name: Some("edit".into()),
                args: serde_json::json!({}).into(),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
            WalRecord::ToolResult {
                finished_at_ms: None,
                tags: None,
                hooks: None,
                batch_id: BatchId("b1".into()),
                result: ToolResult {
                    tool_call_id: ToolCallId("t1".into()),
                    output: ToolOutput::Text {
                        text: "edited".into(),
                    },
                    is_error: false,
                }
                .into(),
                outcome: DispatchOutcome::Completed,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
            },
            WalRecord::ToolBatchFinished {
                batch_id: BatchId("b1".into()),
            },
            WalRecord::TurnFinished { step_id: StepId(1) },
            WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(2),
                assistant_message: AssistantMessage {
                    message_id: MessageId("m2".into()),
                    content: vec![ContentBlock::Text {
                        text: "done".into(),
                    }],
                }
                .into(),
                usage: Usage::default(),
                finish_reason: None,
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ]
    }

    /// The core merge invariant: for every projectable watermark, prefix ⊕ tail
    /// equals the full-WAL replay. A materialization watermark anywhere must never
    /// drop or duplicate a turn.
    #[test]
    fn merge_equals_full_replay_at_every_projection_boundary() {
        let wal = sample_wal();
        let full = replay(&wal).expect("replay").prompt;
        let framed: Vec<FramedRecord> = wal
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, record)| FramedRecord {
                seq: (i + 1) as u64,
                fence: 1,
                record,
            })
            .collect();

        // Include every ordinary turn boundary and the terminal no-tool boundary selected by
        // the shared projection rule. Also test watermark 0 (cold start, nothing folded).
        let mut boundaries: Vec<u64> = framed
            .iter()
            .filter_map(|f| matches!(f.record, WalRecord::TurnFinished { .. }).then_some(f.seq))
            .collect();
        boundaries.push(
            crate::latest_projection_boundary(&framed, 0)
                .expect("terminal no-tool step is projectable"),
        );
        boundaries.sort_unstable();
        boundaries.dedup();

        for &wm in std::iter::once(&0u64).chain(boundaries.iter()) {
            // The store has materialized records with seq <= wm = the first `wm`.
            let materialized = replay(&wal[..wm as usize]).expect("replay").prompt;
            let prefix = StorePrefix {
                prefix: materialized,
                watermark_seq: wm,
                ..StorePrefix::empty()
            };
            // The tail = records with seq > wm = skip the first `wm`.
            let tail: Vec<WalRecord> = wal.iter().skip(wm as usize).cloned().collect();
            let merged = effective_context(&prefix, &tail).expect("merge");
            assert_eq!(
                merged, full,
                "merge at watermark {wm} must equal full replay"
            );
        }
    }

    fn assert_resume_clears_finish_reason(tail: Vec<WalRecord>) {
        use broca_protocol::FinishReason;

        let mut records = sample_wal()[..7].to_vec();
        if let WalRecord::ModelStepFinished { finish_reason, .. } = &mut records[1] {
            *finish_reason = Some(FinishReason::ToolCalls);
        }
        let prior = replay(&records).expect("prefix replay");
        assert_eq!(prior.last_step_finish_reason, Some(FinishReason::ToolCalls));
        let prefix = StorePrefix {
            prefix: prior.prompt.clone(),
            watermark_seq: records.len() as u64,
            completed_steps: prior.completed_steps,
            usage: prior.usage,
            retries_used: prior.retries_used,
            last_step_usage: prior.last_step_usage,
            last_step_finish_reason: prior.last_step_finish_reason,
            ..StorePrefix::empty()
        };
        let mut seed = crate::ProjectionSeed::empty();
        seed.prefix_len = prefix.prefix.len() as u64;
        seed.completed_steps = prefix.completed_steps;
        seed.usage = prefix.usage;
        seed.retries_used = prefix.retries_used;
        seed.last_step_usage = prefix.last_step_usage;
        seed.last_step_finish_reason = prefix.last_step_finish_reason;
        seed.last_run_id = prior.run_id.clone();
        seed.admission_checkpoint
            .fold_frames(&framed(&records))
            .expect("checkpoint prefix");

        let unchanged = resume_state_from_store(
            &prefix,
            &[WalRecord::Unknown],
            prior.run_config.clone(),
            prior.run_id.clone(),
            None,
        )
        .expect("untouched tail");
        assert_eq!(
            unchanged.last_step_finish_reason,
            Some(FinishReason::ToolCalls)
        );

        let merged = resume_state_from_store(&prefix, &tail, prior.run_config, prior.run_id, None)
            .expect("resume merge");
        let mut tail_frames = framed(&tail);
        for frame in &mut tail_frames {
            frame.seq += prefix.watermark_seq;
        }
        let projected =
            crate::replay_projection_tail(&seed, &tail_frames).expect("projection replay");
        records.extend(tail);
        let full = replay(&records).expect("full replay");
        assert_eq!(full.last_step_finish_reason, None, "full replay must clear");
        assert_eq!(
            projected.last_step_finish_reason, None,
            "projection must clear"
        );
        assert_eq!(merged.last_step_finish_reason, None, "resume must clear");
    }

    #[test]
    fn resume_merge_clears_omitted_step_finish_reason() {
        assert_resume_clears_finish_reason(vec![sample_wal()[7].clone()]);
    }

    #[test]
    fn resume_merge_failed_retraction_clears_prefix_finish_reason() {
        assert_resume_clears_finish_reason(vec![WalRecord::Retracted {
            submission_id: broca_protocol::SubmissionId("next-submission".into()),
            failure: Some(broca_wal::QueuedAdmissionFailure {
                run_id: RunId("next-run".into()),
                error: broca_protocol::error::ProviderError {
                    provider_details: None,
                    provider_subcode: None,
                    class: ErrorClass::Permanent,
                    message: "unknown model".into(),
                    refusal: None,
                    status: None,
                    retry_after_secs: None,
                    provider_code: Some("queued_admission_failed".into()),
                    resets_at_ms: None,
                    resets_at_source: None,
                    limit_window: None,
                    account_ref: None,
                    credential_id: None,
                    affordable_max_tokens: None,
                },
            }),
        }]);
    }

    #[test]
    fn empty_prefix_degenerates_to_full_replay() {
        let wal = sample_wal();
        let merged = effective_context(&StorePrefix::empty(), &wal).expect("merge");
        assert_eq!(merged, replay(&wal).expect("replay").prompt);
    }

    /// Non-vacuousness: a full replay stamps EVERY message with a durable ordinal — 0, 1, 2…
    /// in lineage order — not `None`. (The merge-equality tests prove the fast path stamps
    /// IDENTICALLY, but both could be `None`-but-equal without this.)
    #[test]
    fn replay_stamps_sequential_ordinals() {
        let wal = sample_wal();
        let prompt = replay(&wal).expect("replay").prompt;
        assert!(!prompt.is_empty(), "the sample lineage produces messages");
        for (i, m) in prompt.iter().enumerate() {
            assert_eq!(
                m.ordinal,
                Some(i as u64),
                "message {i} must carry ordinal {i}"
            );
            // The opaque mid is the `m<ordinal>` form, `#`-free.
            assert_eq!(m.mid().as_deref(), Some(format!("m{i}").as_str()));
        }
    }

    /// The store-merge fast path assigns the SAME ordinals as a full replay at every
    /// turn-boundary watermark — the durable-identity analog of the merge-prompt invariant.
    /// This is the "cross read-path mid identity" obligation, made a standing gate: since
    /// ordinal is a `Message` field, whole-message equality (already asserted by
    /// `merge_equals_full_replay_at_every_turn_boundary`) covers it — this test pins the
    /// ordinal component explicitly so a regression names the cause.
    #[test]
    fn merge_and_full_replay_assign_identical_ordinals() {
        let wal = sample_wal();
        let full = replay(&wal).expect("replay").prompt;
        let full_ords: Vec<Option<u64>> = full.iter().map(|m| m.ordinal).collect();

        let turn_boundaries = wal.iter().enumerate().filter_map(|(i, r)| {
            matches!(r, WalRecord::TurnFinished { .. }).then_some((i + 1) as u64)
        });
        for wm in std::iter::once(0u64).chain(turn_boundaries) {
            let materialized = replay(&wal[..wm as usize]).expect("replay").prompt;
            let prefix = StorePrefix {
                prefix: materialized,
                watermark_seq: wm,
                ..StorePrefix::empty()
            };
            let tail: Vec<WalRecord> = wal.iter().skip(wm as usize).cloned().collect();
            let merged = effective_context(&prefix, &tail).expect("merge");
            let merged_ords: Vec<Option<u64>> = merged.iter().map(|m| m.ordinal).collect();
            assert_eq!(
                merged_ords, full_ords,
                "ordinals at watermark {wm} must match the full replay's"
            );
        }
    }

    fn framed(records: &[WalRecord]) -> Vec<FramedRecord> {
        records
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, record)| FramedRecord {
                seq: i as u64 + 1,
                fence: 1,
                record,
            })
            .collect()
    }

    fn paged_differential_corpus() -> (Vec<FramedRecord>, StorePrefix, u64) {
        let mut records = sample_wal();
        // A completed no-tool step immediately before RunFinished is a safe materialization
        // watermark; the terminal record remains in the WAL tail for state derivation.
        let watermark = records.len() as u64 - 1;
        let config = match &records[0] {
            WalRecord::RunStarted { config, .. } => config.clone(),
            _ => unreachable!("sample starts an episode"),
        };
        records.extend([
            WalRecord::RunStarted {
                written_at_ms: None,
                tags: None,
                session_class: None,
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
                run_id: RunId("r2".into()),
                session: BindIdentity::new("/tmp/p", "h", "s"),
                config,
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "episode two".into(),
                    }],
                )]
                .into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(3),
                assistant_message: (AssistantMessage {
                    message_id: MessageId("m3".into()),
                    content: vec![ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: ToolCallId("t2".into()),
                        tool_name: "read".into(),
                        input: serde_json::json!({}),
                    })],
                })
                .into(),
                usage: Usage::default(),
                finish_reason: None,
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::ToolBatchStarted {
                batch_id: BatchId("b2".into()),
                step_id: StepId(3),
                planned_calls: vec![ToolCallId("t2".into())],
            },
            WalRecord::ToolResult {
                finished_at_ms: None,
                tags: None,
                hooks: None,
                batch_id: BatchId("b2".into()),
                result: ToolResult {
                    tool_call_id: ToolCallId("t2".into()),
                    output: ToolOutput::Text {
                        text: "read".into(),
                    },
                    is_error: false,
                }
                .into(),
                outcome: DispatchOutcome::Completed,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
            },
            WalRecord::ToolBatchFinished {
                batch_id: BatchId("b2".into()),
            },
            WalRecord::TurnFinished { step_id: StepId(3) },
            // This naturally terminal no-tool step is durable but has no RunFinished, so the
            // corpus ends with a non-empty, not-yet-materialized tail.
            WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(4),
                assistant_message: (AssistantMessage {
                    message_id: MessageId("m4".into()),
                    content: vec![ContentBlock::Text {
                        text: "tail terminal".into(),
                    }],
                })
                .into(),
                usage: Usage::default(),
                finish_reason: None,
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
        ]);
        let framed = framed(&records);
        let committed: Vec<WalRecord> = framed
            .iter()
            .filter(|record| record.seq <= watermark)
            .map(|record| record.record.clone())
            .collect();
        let committed_state = replay(&committed).expect("committed corpus replays");
        let prefix = committed_state.prompt;
        (
            framed,
            StorePrefix {
                lineage_id: None,
                prefix,
                completed_steps: committed_state.completed_steps,
                usage: committed_state.usage,
                retries_used: committed_state.retries_used,
                last_step_usage: committed_state.last_step_usage,
                last_step_finish_reason: committed_state.last_step_finish_reason,
                watermark_seq: watermark,
                context_epoch: 0,
            },
            watermark,
        )
    }

    fn snapshot_for_request(
        prefix: &StorePrefix,
        framed: &[FramedRecord],
        from_ordinal: Option<u64>,
        limit: Option<u64>,
    ) -> StorePageSnapshot {
        let capped_limit = limit
            .unwrap_or(SESSION_READ_DEFAULT_LIMIT)
            .clamp(1, SESSION_READ_MAX_LIMIT);
        let prefix_len = prefix.prefix.len() as u64;
        let start = from_ordinal.unwrap_or_else(|| prefix_len.saturating_sub(capped_limit));
        let end = start.saturating_add(capped_limit).min(prefix_len);
        let runs = crate::replay(
            &framed
                .iter()
                .filter(|frame| frame.seq <= prefix.watermark_seq)
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .message_runs;
        let rows = prefix
            .prefix
            .iter()
            .cloned()
            .enumerate()
            .filter(|(index, _)| (*index as u64) >= start && (*index as u64) < end)
            .map(|(index, mut message)| {
                let ordinal = index as u64;
                message.ordinal = Some(ordinal);
                StorePageRow {
                    written_at: None,
                    ordinal,
                    message,
                    run: runs[index].clone(),
                }
            })
            .collect();
        StorePageSnapshot {
            head: None,
            model_history: None,
            last_title: None,
            deferred_calls: None,
            compaction: None,
            watermark_seq: prefix.watermark_seq,
            compaction_setup_seen: false,
            lineage_id: None,
            control_head: crate::project_control(
                &framed
                    .iter()
                    .filter(|record| record.seq <= prefix.watermark_seq)
                    .cloned()
                    .collect::<Vec<_>>(),
            )
            .last()
            .map(|(cursor, _)| *cursor),
            anchor: None,
            prefix_len,
            completed_steps: prefix.completed_steps,
            rows,
            trailing_origin: trailing_origin(&prefix.prefix),
            last_run_id: Some(RunId("r".into())),
            last_episode_usage: None,
            tag_seed: Default::default(),
        }
    }

    #[test]
    fn store_paged_read_is_differentially_equivalent_to_full_assembly() {
        let (framed, prefix, watermark) = paged_differential_corpus();
        let tail: Vec<FramedRecord> = framed
            .iter()
            .filter(|record| record.seq > watermark)
            .cloned()
            .collect();
        let prompt_len = replay(
            &framed
                .iter()
                .map(|record| record.record.clone())
                .collect::<Vec<_>>(),
        )
        .expect("full corpus replays")
        .prompt
        .len() as u64;
        let limits = std::iter::once(None)
            .chain((1..=prompt_len + 2).map(Some))
            .collect::<Vec<_>>();
        let offsets = std::iter::once(None)
            .chain((0..=prompt_len + 2).map(Some))
            .collect::<Vec<_>>();

        for limit in limits {
            for from_ordinal in offsets.iter().copied() {
                let expected =
                    read_session_page_full(&prefix, &framed, from_ordinal, limit).expect("oracle");
                let snapshot = snapshot_for_request(&prefix, &framed, from_ordinal, limit);
                let actual = read_session_page(&snapshot, &tail, from_ordinal, limit)
                    .expect("store-paged read");
                assert_eq!(
                    actual, expected,
                    "page differs at from={from_ordinal:?}, limit={limit:?}"
                );
            }
        }
    }

    fn assert_owned_page_matches_borrowed(
        snapshot: StorePageSnapshot,
        tail: &[FramedRecord],
        from_ordinal: Option<u64>,
        limit: Option<u64>,
        case: &str,
    ) -> SessionReadPage {
        let expected = read_session_page(&snapshot, tail, from_ordinal, limit)
            .expect("borrowed page assembly");
        let actual = read_session_page_owned(snapshot, tail, from_ordinal, limit)
            .expect("owned page assembly");
        assert_eq!(actual, expected, "owned page differs for {case}");
        actual
    }

    #[test]
    fn owned_store_paged_read_is_differentially_equivalent_for_edge_ranges() {
        let (framed, prefix, watermark) = paged_differential_corpus();
        let tail: Vec<FramedRecord> = framed
            .iter()
            .filter(|record| record.seq > watermark)
            .cloned()
            .collect();
        let prefix_len = prefix.prefix.len() as u64;

        let mixed_from = prefix_len.saturating_sub(2);
        let mixed_limit = Some(4);
        let mixed_snapshot = snapshot_for_request(&prefix, &framed, Some(mixed_from), mixed_limit);
        let mixed = assert_owned_page_matches_borrowed(
            mixed_snapshot,
            &tail,
            Some(mixed_from),
            mixed_limit,
            "a range spanning the materialized prefix and WAL tail",
        );
        let mixed_oracle = read_session_page_full(&prefix, &framed, Some(mixed_from), mixed_limit)
            .expect("mixed-range oracle");
        assert_eq!(mixed, mixed_oracle);
        assert!(mixed
            .messages
            .iter()
            .any(|message| message.ordinal < prefix_len));
        assert!(mixed
            .messages
            .iter()
            .any(|message| message.ordinal >= prefix_len));
        assert!(mixed.next_from_ordinal.is_some());

        let mut interrupted_snapshot =
            snapshot_for_request(&prefix, &framed, Some(0), Some(prefix_len));
        interrupted_snapshot.completed_steps = 0;
        assert!(!interrupted_snapshot.rows.is_empty());
        let interrupted_tail = self::framed(&[WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Interrupted,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        }]);
        let interrupted = assert_owned_page_matches_borrowed(
            interrupted_snapshot,
            &interrupted_tail,
            Some(0),
            Some(prefix_len),
            "an interrupted zero-step materialized seed",
        );
        assert!(interrupted.messages.is_empty());

        let empty = assert_owned_page_matches_borrowed(
            StorePageSnapshot::empty(),
            &[],
            Some(0),
            Some(1),
            "an empty page",
        );
        assert!(empty.messages.is_empty());
        assert_eq!(empty.next_from_ordinal, None);

        let tail_only_snapshot = snapshot_for_request(&prefix, &framed, Some(prefix_len), Some(2));
        assert!(tail_only_snapshot.rows.is_empty());
        let tail_only = assert_owned_page_matches_borrowed(
            tail_only_snapshot,
            &tail,
            Some(prefix_len),
            Some(2),
            "a range entirely past the materialized prefix",
        );
        assert!(!tail_only.messages.is_empty());
        assert!(tail_only
            .messages
            .iter()
            .all(|message| message.ordinal >= prefix_len));

        let rows = (0..=SESSION_READ_MAX_LIMIT)
            .map(|ordinal| {
                let mut message = Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: format!("row-{ordinal}"),
                    }],
                );
                message.ordinal = Some(ordinal);
                StorePageRow {
                    written_at: None,
                    ordinal,
                    message,
                    run: None,
                }
            })
            .collect();
        let clamped_snapshot = StorePageSnapshot {
            prefix_len: SESSION_READ_MAX_LIMIT + 1,
            completed_steps: 1,
            rows,
            ..StorePageSnapshot::empty()
        };
        let clamped = assert_owned_page_matches_borrowed(
            clamped_snapshot,
            &[],
            Some(0),
            Some(SESSION_READ_MAX_LIMIT + 1),
            "a limit above the server maximum",
        );
        assert_eq!(clamped.messages.len(), SESSION_READ_MAX_LIMIT as usize);
        assert_eq!(clamped.next_from_ordinal, Some(SESSION_READ_MAX_LIMIT));
    }

    #[test]
    fn store_paged_terminal_only_tail_preserves_lineage_state() {
        let mut wal = sample_wal();
        let terminal_usage = Usage {
            input_tokens: Some(11),
            cached_input_tokens: Some(2),
            cache_write_tokens: Some(3),
            output_tokens: Some(5),
            reasoning_tokens: Some(1),
        };
        let terminal_error = ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Permanent,
            message: "terminal error".into(),
            status: Some(500),
            retry_after_secs: None,
            provider_code: Some("terminal".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        *wal.last_mut().expect("sample has RunFinished") = WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Error,
            usage: terminal_usage,
            error: Some(terminal_error.clone()),
            retries_used: 0,
        };
        let framed = framed(&wal);
        let watermark = framed.len() as u64 - 1;
        let committed = replay(&wal[..watermark as usize]).expect("committed prefix replays");
        let prefix = StorePrefix {
            lineage_id: None,
            prefix: committed.prompt,
            completed_steps: committed.completed_steps,
            usage: committed.usage,
            retries_used: committed.retries_used,
            last_step_usage: committed.last_step_usage,
            last_step_finish_reason: committed.last_step_finish_reason,
            watermark_seq: watermark,
            context_epoch: 0,
        };
        let snapshot = snapshot_for_request(&prefix, &framed, None, Some(10));
        let tail = &framed[watermark as usize..];
        let actual = read_session_page(&snapshot, tail, None, Some(10)).expect("paged read");
        let expected =
            read_session_page_full(&prefix, &framed, None, Some(10)).expect("full oracle");
        assert_eq!(actual, expected);
        assert_eq!(actual.lineage_state.last_run_id, Some(RunId("r".into())));
        assert_eq!(actual.lineage_state.error, Some(terminal_error));
        assert_eq!(actual.lineage_state.usage, Some(terminal_usage));
    }

    /// A page is an ordinal range and splits a tool call from its result.
    ///
    /// CKTUI holds unmatched halves across page boundaries in both directions and could not tell,
    /// from outside, whether that logic compensated for an intended property or masked a defect.
    /// It compensates for an intended one -- and this asserts the split so a future change that
    /// quietly started aligning pages to turn boundaries would announce itself rather than
    /// silently making their reunion logic dead code that still looks necessary.
    ///
    /// The direction matters as much as the split: back-fill walks backward, so a consumer
    /// commonly receives the RESULT one page before the CALL it answers.
    #[test]
    fn a_page_boundary_can_split_a_tool_call_from_its_result() {
        let wal = sample_wal();
        let full = replay(&wal).expect("replay").prompt;

        // Find an adjacent (assistant-with-tool-call, tool-result) pair in the transcript.
        let split_at = full
            .windows(2)
            .position(|pair| {
                pair[0]
                    .content
                    .iter()
                    .any(|block| matches!(block, broca_protocol::ContentBlock::ToolCall { .. }))
                    && pair[1].role == broca_protocol::Role::Tool
            })
            .map(|index| index + 1)
            .expect("the fixture must contain a tool call answered by the next message");

        // A page ending exactly between them is a legal request and returns the call without the
        // result: nothing in the paging contract prevents it.
        let call_page = read_session_page(
            &StorePageSnapshot::empty(),
            &framed(&wal),
            Some(0),
            Some(split_at as u64),
        )
        .expect("page ending at the split");
        let last = call_page
            .messages
            .last()
            .expect("the page carries the call");
        assert!(
            last.message
                .content
                .iter()
                .any(|block| { matches!(block, broca_protocol::ContentBlock::ToolCall { .. }) }),
            "the page must end on the unanswered tool call"
        );
        assert!(
            call_page.next_from_ordinal.is_some(),
            "a page ending before the transcript end must offer a continuation"
        );

        // The answering result is the first message of the following page, so a consumer walking
        // backward sees it before the call it belongs to.
        let result_page = read_session_page(
            &StorePageSnapshot::empty(),
            &framed(&wal),
            Some(split_at as u64),
            Some(1),
        )
        .expect("page starting at the split");
        assert_eq!(
            result_page.messages.first().map(|entry| entry.message.role),
            Some(broca_protocol::Role::Tool),
            "the result must land on the next page, not be pulled back to join its call"
        );
    }

    #[test]
    fn session_read_pages_by_message_ordinal() {
        let wal = sample_wal();
        let full = replay(&wal).expect("replay").prompt;
        let page1 = read_session_page(&StorePageSnapshot::empty(), &framed(&wal), Some(0), Some(2))
            .expect("read first page");
        assert_eq!(
            page1.messages.iter().map(|m| m.ordinal).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(page1.next_from_ordinal, Some(2));

        let page2 = read_session_page(
            &StorePageSnapshot::empty(),
            &framed(&wal),
            page1.next_from_ordinal,
            Some(2),
        )
        .expect("read second page");
        assert_eq!(page2.next_from_ordinal, None);
        let combined: Vec<_> = page1
            .messages
            .into_iter()
            .chain(page2.messages)
            .map(|m| m.message)
            .collect();
        assert_eq!(combined, full);

        let last_page =
            read_session_page(&StorePageSnapshot::empty(), &framed(&wal), None, Some(2))
                .expect("read newest-last default page");
        assert_eq!(last_page.next_from_ordinal, None);
        assert_eq!(
            last_page
                .messages
                .iter()
                .map(|m| m.ordinal)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn session_read_hides_open_tool_turn_before_first_dispatch_intent() {
        let complete = sample_wal();
        // Both the step-only cut and the opened batch without intents are dropped by
        // recovery. Readers must not see a message that sealing the episode removes.
        for end in [2, 3] {
            let mut wal = complete[..end].to_vec();
            let before = read_session_page(
                &StorePageSnapshot::empty(),
                &framed(&wal),
                Some(0),
                Some(10),
            )
            .expect("read before first intent");
            assert_eq!(before.messages.len(), 1, "no intent means no open turn");
            assert_eq!(before.messages[0].message.role, Role::User);
            wal.push(WalRecord::RunFinished {
                interrupt_reason: None,
                reason: RunFinishReason::Interrupted,
                ts_ms: None,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            });
            let sealed = read_session_page(
                &StorePageSnapshot::empty(),
                &framed(&wal),
                Some(0),
                Some(10),
            )
            .expect("read sealed cut");
            let mut expected = before.messages;
            // Sealing changes only the last message's final bit, not its body or identity.
            expected.last_mut().unwrap().run.as_mut().unwrap().is_final = true;
            assert_eq!(
                expected, sealed.messages,
                "sealing preserves the visible transcript except for durable finality"
            );
        }
    }

    #[test]
    fn session_read_exposes_open_tool_turn_without_putting_it_in_model_prompt() {
        let mut wal = sample_wal();
        // Keep only the first four records, so the test leaves the tool turn open and
        // uncommitted.
        wal.truncate(4);
        let (page, dispatches) = read_session_page_owned_with_dispatches(
            StorePageSnapshot::empty(),
            &framed(&wal),
            Some(0),
            Some(10),
        )
        .expect("read open turn");
        let calls = &dispatches[&1];
        assert_eq!(calls.len(), 1);
        assert!(calls[0].indeterminate);
        assert!(calls[0].intent_seq.is_some());
        assert_eq!(
            page.messages.len(),
            2,
            "the durable open assistant turn is visible to readers"
        );
        assert_eq!(page.messages[0].message.role, Role::User);
        assert_eq!(page.messages[1].message.role, Role::Assistant);
        assert_eq!(
            replay(&wal).expect("replay").prompt.len(),
            1,
            "model input still excludes the unanswered tool turn"
        );
        assert_eq!(page.lineage_state.state, SessionReadStateKind::Active);
    }

    #[test]
    fn recovered_context_config_uses_the_latest_auth_refreeze() {
        let mut wal = sample_wal();
        wal.insert(
            1,
            WalRecord::RunAuthRefrozen {
                ts_ms: Some(50),
                auth_selection: AuthSelection {
                    auth_method: None,
                    credential_id: "chatgpt:openai".into(),
                    account_ref: Some("resumed-account".into()),
                    resolved_credential_id: None,
                    vault_request: None,
                    account_decision: None,
                },
            },
        );

        let recovered = recover_run_config(&framed(&wal)).expect("sample has a run config");

        assert_eq!(
            recovered.auth_selection,
            Some(AuthSelection {
                auth_method: None,
                credential_id: "chatgpt:openai".into(),
                account_ref: Some("resumed-account".into()),
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            })
        );
    }

    #[test]
    fn session_read_lineage_state_carries_typed_terminal_error() {
        let mut wal = sample_wal();
        let error = ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Permanent,
            message: "provider exploded".into(),
            status: Some(500),
            retry_after_secs: None,
            provider_code: Some("boom".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        let last = wal.last_mut().expect("sample has terminal");
        *last = WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Error,
            usage: Usage::default(),
            error: Some(error.clone()),
            retries_used: 0,
        };

        let page = read_session_page(&StorePageSnapshot::empty(), &framed(&wal), None, Some(10))
            .expect("read errored lineage");
        assert_eq!(page.lineage_state.state, SessionReadStateKind::Error);
        assert_eq!(
            page.lineage_state.reason,
            Some(SessionReadReason::Terminal(RunFinishReason::Error))
        );
        assert_eq!(page.lineage_state.error, Some(error));
    }
}

#[cfg(test)]
mod episode_merge_regression {
    use super::*;
    use broca_protocol::{
        content::{ContentBlock, Message, Role},
        request::ModelHandle,
        BindIdentity, RunId, StepId,
    };
    use broca_wal::{RunConfig, RunOrigin, WalRecord};

    fn config(model: &str) -> RunConfig {
        RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("stub", model),
            tools: Vec::new().into(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::json!({"model": model}),
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        }
    }

    fn start(run: &str, model: &str, text: &str) -> WalRecord {
        WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
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
            run_id: RunId(run.into()),
            session: BindIdentity::new("/tmp/p", "h", "s"),
            config: Box::new(config(model)),
            input: vec![Message {
                cache_prefix_blocks: None,
                role: Role::User,
                content: vec![ContentBlock::Text { text: text.into() }],
                origin: None,
                ordinal: None,
                tags: Default::default(),
            }]
            .into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        }
    }

    #[test]
    fn store_merge_resets_prior_episode_counters_when_tail_crosses_run_started() {
        use broca_protocol::{
            caller::RunFinishReason, content::AssistantMessage, MessageId, Usage,
        };

        let a_usage = Usage {
            input_tokens: Some(40),
            output_tokens: Some(5),
            ..Usage::default()
        };
        let wal = vec![
            start("a", "a-model", "episode a"),
            WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(1),
                assistant_message: AssistantMessage {
                    message_id: MessageId("a-answer".into()),
                    content: vec![ContentBlock::Text {
                        text: "a done".into(),
                    }],
                }
                .into(),
                usage: a_usage,
                finish_reason: None,
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: a_usage,
                error: None,
                retries_used: 0,
            },
            start("b", "b-model", "episode b"),
            WalRecord::StepStarted { step_id: StepId(1) },
        ];
        let projected = crate::replay(&wal[..2]).expect("episode A prefix replays");
        let stored_messages = projected.prompt;
        let prefix = StorePrefix {
            lineage_id: None,
            prefix: stored_messages,
            completed_steps: projected.completed_steps,
            usage: projected.usage,
            retries_used: projected.retries_used,
            last_step_usage: projected.last_step_usage,
            last_step_finish_reason: projected.last_step_finish_reason,
            watermark_seq: 2,
            context_epoch: 0,
        };
        let tail = &wal[2..];
        let merged = resume_state_from_store(
            &prefix,
            tail,
            Some(config("b-model")),
            Some(RunId("b".into())),
            None,
        )
        .expect("store merge");
        let full = crate::replay(&wal).expect("full replay");

        assert_eq!(merged.prompt, full.prompt);
        assert_eq!(merged.completed_steps, full.completed_steps);
        assert_eq!(merged.usage, full.usage);
        assert_eq!(merged.last_step_usage, full.last_step_usage);
        assert_eq!(merged.last_step_finish_reason, full.last_step_finish_reason);
        assert_eq!(merged.provider_call_started, full.provider_call_started);
        assert_eq!(
            merged.completed_steps, 0,
            "episode B numbering starts at zero"
        );
        assert_eq!(
            merged.usage,
            Usage::default(),
            "episode A usage must not leak"
        );
    }
}

#[cfg(test)]
mod held_resume_tests {
    use super::*;

    fn episode(
        start: u64,
        first_user: Option<u64>,
        first_step: Option<u64>,
    ) -> EpisodeRenderProjection {
        EpisodeRenderProjection {
            start: Some(start),
            first_user: Some(first_user),
            first_step: Some(first_step),
            render_config: Some(serde_json::json!({"episode": start})),
        }
    }

    #[test]
    fn a_known_absence_survives_the_stored_json_and_an_old_null_reads_as_uncomputed() {
        let known = vec![episode(0, None, None), episode(4, Some(4), Some(5))];
        let json = serde_json::to_string(&known).unwrap();
        let read: Vec<EpisodeRenderProjection> = serde_json::from_str(&json).unwrap();
        assert_eq!(read, known);
        assert!(read.iter().all(EpisodeRenderProjection::is_complete));
        // An uncomputed value is omitted, and a payload written before the encoding (plain
        // `null`) cannot claim a known absence.
        let uncomputed = EpisodeRenderProjection {
            first_step: None,
            ..episode(0, Some(0), None)
        };
        let json = serde_json::to_string(&uncomputed).unwrap();
        assert!(!json.contains("first_step"), "{json}");
        let legacy: EpisodeRenderProjection = serde_json::from_str(
            r#"{"start":0,"first_user":null,"first_step":null,"render_config":{}}"#,
        )
        .unwrap();
        assert_eq!((legacy.first_user, legacy.first_step), (None, None));
    }

    #[test]
    fn held_episodes_are_the_ones_holding_a_row_and_the_newest() {
        let json = serde_json::to_string(&vec![
            episode(0, Some(0), Some(1)),
            episode(10, Some(10), Some(11)),
            episode(20, Some(20), Some(21)),
            episode(30, Some(30), None),
        ])
        .unwrap();
        let held = select_held_episodes(&json, [0, 21, 22]).unwrap();
        assert_eq!(
            held.iter()
                .map(|held| (held.projection.start, held.next_start))
                .collect::<Vec<_>>(),
            vec![(Some(0), Some(10)), (Some(20), Some(30)), (Some(30), None)]
        );
        assert_eq!(held[2].projection.first_step, Some(None));
    }

    #[test]
    fn a_first_step_outside_its_episode_distrusts_the_whole_projection() {
        // An episode with no model step took the next episode's first step: the step list was
        // shifted, so the newest episode's "no step" cannot be trusted either.
        let json = serde_json::to_string(&vec![
            episode(0, Some(0), Some(12)),
            episode(10, Some(10), None),
        ])
        .unwrap();
        assert_eq!(select_held_episodes(&json, [11]), None);
        let json = serde_json::to_string(&vec![
            episode(10, Some(10), Some(11)),
            episode(10, Some(10), Some(12)),
        ])
        .unwrap();
        assert_eq!(
            select_held_episodes(&json, [11]),
            None,
            "starts out of order"
        );
    }
}

#[cfg(test)]
mod call_identity_tests {
    #[test]
    fn recorded_preparation_key_wins_and_ordinary_keys_keep_their_bytes() {
        let lineage = "00000000-0000-4000-8000-000000000001";
        assert_eq!(
            super::tool_call_key(Some(lineage), 19, None).as_deref(),
            Some("00000000-0000-4000-8000-000000000001:19")
        );
        let prepared = "00000000-0000-4000-8000-000000000001:5";
        assert_eq!(
            super::tool_call_key(Some(lineage), 21, Some(prepared)).as_deref(),
            Some(prepared)
        );
        assert_eq!(
            super::tool_call_key(None, 21, Some(prepared)).as_deref(),
            Some(prepared)
        );
        assert!(super::tool_call_key(None, 19, None).is_none());
    }
}
