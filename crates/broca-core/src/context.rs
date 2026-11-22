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
#[cfg(test)]
use broca_records::record::WalRecord;
#[cfg(test)]
use broca_wal::FramedRecord;

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

/// Export every terminal run in this WAL before deleting its lineage, including
/// runs with existing facts. Passing the whole lineage preserves later warm facts;
/// the backend's immutable fact-id uniqueness makes repeating emission harmless.
/// This deliberately does not consult the missing-facts outbox or run index.
pub fn emit_lineage_usage_exports(
    index: &dyn ExportIndex,
    records: &[crate::billing::BillingRecord],
    resolver: &dyn ChargeBasisResolver,
) -> Result<usize, ExportError> {
    use crate::billing::BillingEvent;
    let mut current = None;
    let mut emitted = std::collections::HashSet::new();
    for frame in records {
        match &frame.event {
            BillingEvent::RunStarted { run_id, .. } => current = Some(run_id),
            BillingEvent::RunFinished { .. } => {
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

    use broca_records::{
        record::AuthSelection, record::DispatchOutcome, record::RunConfig, record::WalRecord,
    };
    use broca_wal::FramedRecord;
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
                origin: broca_records::record::RunOrigin::Fresh,
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
