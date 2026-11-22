//! Types the session store, the run index and the serve layer share: `session.read`'s page
//! shapes and limits, the session-ref lookup and inventory traits, the reader of published
//! run-event bodies, and the usage export index with its facts, pages and cursor.

use crate::billing::ChargeBasisResolver;
use broca_protocol::{
    caller::{PauseReason, RunFinishReason},
    content::Message,
    error::ProviderError,
    BindIdentity, RunId, Usage,
};

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
    /// Runs the census over the sessions stored under `root`. An empty or unfinished census
    /// leaves the index incomplete until repair finishes.
    fn rebuild_inventory(&self, root: &std::path::Path) -> Result<(), String>;
}

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

impl RunIndexState {}

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
        records: &[crate::billing::BillingRecord],
        run_id: &RunId,
        resolver: &dyn ChargeBasisResolver,
    ) -> Result<(), ExportError>;

    /// [`Self::emit_run`], with a usage fact for each of the run's model attempts in `attempts`
    /// (its attempt records in seq order), and the run's segments marked as counted in them
    /// ([`crate::SpendSegment::usage_in_attempts`]). An index that writes no attempt facts
    /// writes the segments unmarked, as `emit_run` does.
    fn emit_run_with_attempts(
        &self,
        records: &[crate::billing::BillingRecord],
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
    pub lineage_state: SessionReadLineageState,
    /// How many messages the whole transcript holds: one past its newest ordinal.
    pub prompt_len: u64,
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
