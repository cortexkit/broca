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

use crate::billing::ChargeBasisResolver;
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
pub use model_page::ModelEntry;

/// One atomic read from the materialized store: the reconstructed prefix + the
/// counters it reflects + the WAL seq watermark + the context epoch.
#[derive(Debug, Clone, PartialEq)]
pub struct StorePrefix {
    /// Refusal state at this watermark. None means an older writer did not record it;
    /// the prior must then be reconstructed from the complete WAL, not an empty seed.
    pub refusal: Option<crate::refusal_fallback::RefusalReplayState>,
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

impl StorePrefix {}

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

impl HeldRowsRequest {}

/// One projected episode a held resume needs, with the next projected episode's start.
#[derive(Debug, Clone, PartialEq)]
pub struct HeldEpisode {
    pub projection: EpisodeRenderProjection,
    /// Where the next projected episode starts, which is where this one ends. `None` for the
    /// newest projected episode, which ends where the WAL tail's first episode starts, or at
    /// the session's extent.
    pub next_start: Option<u64>,
}

/// The held rows of a materialized prefix, read with the prefix's watermark, identity, length,
/// counters and seeds in ONE store transaction.
///
/// The seeds describe the whole prefix, not the rows read: `trailing_origin` and `tag_seed` come
/// from the prefix's last row (`prefix_len - 1`), exactly what a merge of the complete prefix
/// with the WAL tail would take from it.
#[derive(Debug, Clone, PartialEq)]
pub struct HeldPrefix {
    pub refusal: Option<crate::refusal_fallback::RefusalReplayState>,
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
    pub created_at: Option<u64>,
    pub written_at: Option<u64>,
    pub completed_at: Option<u64>,
    pub ordinal: u64,
    pub message: Message,
    pub run: Option<crate::RunAttribution>,
}

/// A bounded neutral-entry page and the exact store boundary it came from.
/// Native messages are not loaded for this read, so compact tool entries stay compact.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionStorePage {
    pub entries: Vec<crate::compaction_v2::protocol::read::Entry>,
    pub head: Option<crate::head::HeadProjection>,
    pub watermark_seq: u64,
    pub lineage_id: Option<String>,
    pub anchor: Option<WalByteAnchor>,
    pub prefix_len: u64,
    pub has_more: bool,
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

impl EpisodeRenderProjection {}

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

/// The bodies of the run events broca published on the bus (`run.event.get`), which its
/// outbox keeps under each event's key. A body is session-owned data, even though the bus
/// message carries only routing metadata.
pub trait RunEventBodies: Send + Sync {
    /// The session that owns the event `event_key`, and its body; `None` when none is kept.
    fn read_run_event(&self, event_key: &str) -> Result<Option<(BindIdentity, Vec<u8>)>, String>;
}

/// The materialized store read seam. The loop depends only on this trait.
pub trait MaterializedStore: Send + Sync {
    /// Durable snapshot identity is separate from rebuildable transcript rows.
    fn served_history(&self) -> Option<&dyn crate::ServedHistory> {
        None
    }

    /// Cache only the first-run mode proof, never rebuilding legacy message timestamps.
    fn cache_history_mode(&self, _session: &BindIdentity, _mode3: bool) -> Result<(), String> {
        Ok(())
    }

    /// A neutral page from the same transcript projection as native reads. A
    /// backend without this projection returns None rather than scanning history.
    fn read_compaction_page(
        &self,
        _session: &BindIdentity,
        _request: &crate::compaction_v2::protocol::read::ReadRequest,
    ) -> Result<Option<CompactionStorePage>, crate::compaction_v2::read::ReadError> {
        Ok(None)
    }

    /// Invalidate rebuildable views durably before physical WAL recovery cuts.
    /// Usage export facts are independent authority and must not be changed.
    fn invalidate_wal_recovery(
        &self,
        _session: &BindIdentity,
        _cut: &broca_wal::recovery::RecoveryCut,
    ) -> Result<(), String> {
        Err("materialized store cannot durably invalidate a WAL recovery".into())
    }
    fn needs_wal_recovery_projection(
        &self,
        _session: &BindIdentity,
        _cut_ids: &std::collections::BTreeSet<String>,
    ) -> Result<bool, String> {
        Ok(true)
    }
    fn reproject_wal_recovery(
        &self,
        _session: &BindIdentity,
        _range: &WalRangeRead,
    ) -> Result<(), String> {
        Err("materialized store cannot reproject a WAL recovery".into())
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
    /// Implementations may ignore `None`, which is expected when the backend has no file identity.
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

/// Identity candidates are useful even when discovery is incomplete. Only a complete
/// snapshot can establish absence or uniqueness; candidates still require WAL checks.
#[derive(Default, Debug)]
pub struct SessionRefs {
    pub sessions: std::collections::BTreeMap<String, (String, BindIdentity)>,
    pub complete: bool,
}

pub trait SessionRefResolver: Send + Sync {
    fn resolve_refs(&self, refs: &[&str]) -> Result<SessionRefs, String>;
    fn named_refs(&self, name: &str, limit: u32) -> Result<SessionRefs, String>;
}

/// Identity discovery and projection maintenance do not authorize moving a WAL
/// or record whether an ownership transfer completed.
pub trait SessionInventory: SessionRefResolver + crate::SessionRefIndex {
    /// Synchronous preparation for offline maintenance. Serving starts repair
    /// through shared readiness instead of blocking on this operation. An empty
    /// or unfinished census must remain incomplete until repair has finished.
    fn prepare_inventory(&self, root: &std::path::Path) -> Result<(), String>;
    fn rebuild_inventory(&self, root: &std::path::Path) -> Result<(), String>;
    fn retained_sessions(&self) -> Result<Vec<BindIdentity>, String>;
    fn forget_projection(&self, session: &BindIdentity) -> Result<(), String>;
}

/// Identity-only billing exclusions durable before a moved session can be served
/// or billed. The move coordinator keeps its irreversible activation intent
/// fenced until installation and cut completion recover; the SQL commit does not
/// include the filesystem rename or cut completion. Abort before activation writes
/// no exclusions. Projection replacement and retention preserve these facts.
pub trait ImportedRunExclusions: Send + Sync {
    fn record_imported_runs(
        &self,
        session: &BindIdentity,
        cut_id: &str,
        run_ids: &[RunId],
    ) -> Result<(), ExportError>;
    fn is_imported_run(&self, session: &BindIdentity, run_id: &RunId) -> Result<bool, ExportError>;
}

impl<T: ExportIndex + ?Sized> ImportedRunExclusions for T {
    fn record_imported_runs(
        &self,
        session: &BindIdentity,
        cut_id: &str,
        run_ids: &[RunId],
    ) -> Result<(), ExportError> {
        ExportIndex::record_imported_runs(self, session, cut_id, run_ids)
    }

    fn is_imported_run(&self, session: &BindIdentity, run_id: &RunId) -> Result<bool, ExportError> {
        ExportIndex::is_imported_run(self, session, run_id)
    }
}
impl<T: MaterializedStore + Persister> SessionStore for T {}

/// A run's coarse lifecycle state, as status reads report it from the session's records.
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

    /// Whether this state is terminal: the run reached an end.
    pub fn is_terminal(&self) -> bool {
        !matches!(self, RunIndexState::Active | RunIndexState::Paused)
    }
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

/// The durable usage export index. Billing calls `emit_run` only after a run's terminal record is
/// durable. Readers page the facts with `read_page`, which takes no session lease, so a reader
/// never waits on a running session.
pub trait ExportIndex: Send + Sync {
    fn is_imported_run(
        &self,
        _session: &BindIdentity,
        _run_id: &RunId,
    ) -> Result<bool, ExportError> {
        Err(ExportError::Storage(
            "export index cannot read imported-run exclusions".into(),
        ))
    }

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

    fn emit_run(
        &self,
        records: &[FramedRecord],
        run_id: &RunId,
        resolver: &dyn ChargeBasisResolver,
    ) -> Result<(), ExportError>;

    /// [`Self::emit_run`], with a usage fact for each of the run's model attempts in `attempts`
    /// (its attempt records in seq order), and the run's segments marked as counted in them
    /// ([`crate::SpendSegment::usage_in_attempts`]). An index that writes no attempt facts
    /// writes the segments unmarked, as `emit_run` does.
    fn emit_run_with_attempts(
        &self,
        records: &[FramedRecord],
        run_id: &RunId,
        _attempts: &[crate::billing::attempt::AttemptEvidence],
        resolver: &dyn ChargeBasisResolver,
    ) -> Result<(), ExportError> {
        self.emit_run(records, run_id, resolver)
    }

    fn read_page(&self, since: Option<ExportCursor>, limit: u64)
        -> Result<ExportPage, ExportError>;

    fn high_water(&self) -> u64;

    /// Immutable source identity cached when the export store opens. These getters must not
    /// perform storage I/O because budget admission calls them synchronously.
    fn source_epoch(&self) -> u64;

    fn source_instance_id(&self) -> &str;
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
    pub created_at: Option<u64>,
    pub written_at: Option<u64>,
    pub completed_at: Option<u64>,
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
            SessionReadReason::Pause(PauseReason::AccountLimit) => "account_limit",
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

/// What `session.read` reports about one tool call of an assistant message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallDispatch {
    /// The model's id for the call. Display only: a model may reuse one id in several steps.
    pub tool_call_id: String,
    /// The actual dispatch-intent sequence, absent when the call was never dispatched.
    pub intent_seq: Option<u64>,
    /// A per-call key anchor, which may instead be consent preparation or a singular
    /// negative terminal. A key anchor alone is never evidence of dispatch.
    pub key_seq: Option<u64>,
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
        crate::replay::TranscriptSeed {
            tags: crate::tags::TagSeed::default(),
            mode3: Some(crate::is_mode3_log(
                framed.iter().map(|frame| &frame.record),
            )),
        },
    )?
    .prompt)
}

/// The `session.read` state of a session whose latest run ended for `reason`.
pub fn state_for_terminal(reason: RunFinishReason) -> SessionReadStateKind {
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
mod tests {
    use super::*;
    use crate::replay::replay;

    use broca_wal::{AuthSelection, DispatchOutcome, FramedRecord, RunConfig, WalRecord};
    use {
        broca_protocol::caller::RunFinishReason, broca_protocol::content::AssistantMessage,
        broca_protocol::content::ContentBlock, broca_protocol::content::Role,
        broca_protocol::content::ToolCall, broca_protocol::content::ToolOutput,
        broca_protocol::content::ToolResult, broca_protocol::BatchId, broca_protocol::MessageId,
        broca_protocol::RunId, broca_protocol::StepId, broca_protocol::ToolCallId,
        broca_protocol::Usage,
    };

    // A small multi-turn WAL: user → (tool turn: step1 + batch) → (terminal no-tool
    // step2) → run_finished. The terminal step intentionally has no TurnFinished.
    fn sample_wal() -> Vec<WalRecord> {
        vec![
            WalRecord::RunStarted {
                message_marks: None,
                compaction_plan_version: None,
                sent_by: None,
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
                    account_route: None,
                    global_system: None,
                    route_decision_id: None,
                    served_provider: None,
                    account_limit_pause: false,
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
                    turn_system: false,
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
                raw_finish_reason: None,
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(1),
                assistant_message: AssistantMessage {
                    message_id: MessageId("m1".into()),
                    content: vec![ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        provider_state: None,
                        tool_call_id: ToolCallId("t1".into()),
                        tool_name: "edit".into(),
                        input: serde_json::json!({}).into(),
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
                raw_finish_reason: None,
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

    #[test]
    fn recovered_context_config_uses_the_latest_auth_refreeze() {
        let mut wal = sample_wal();
        wal.insert(
            1,
            WalRecord::RunAuthRefrozen {
                ts_ms: Some(50),
                auth_selection: AuthSelection {
                    billing_vault_facts: None,
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
                billing_vault_facts: None,
                auth_method: None,
                credential_id: "chatgpt:openai".into(),
                account_ref: Some("resumed-account".into()),
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            })
        );
    }
}
