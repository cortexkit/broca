//! TURN-granular WAL replay.
//!
//! Reconstructs the PROVIDER-VALID prompt prefix from the WAL and classifies the
//! trailing state for resume. The load-bearing rule: a `model_step_finished`
//! assistant message WITH tool calls is NOT committed to the prompt until its
//! batch COMPLETES (every planned call has a terminal) — a bare `tool_use` with
//! no matching `tool_result` is provider-invalid.
//!
//! Per-planned-call classification, keyed on the durable plan records:
//!  - a `tool_result` terminal → include the result. Each result is tagged with
//!    how the dispatch ended: only `Unknown` (the request was sent but no result
//!    came back) triggers the interrupt below; `Completed`/`Interrupted`/`MaxSteps`
//!    are definite terminals.
//!  - a `tool_no_dispatch` terminal → include the reconstructed is_error result,
//!    NEVER re-dispatch (the call was refused before it ran).
//!  - intent recorded but NO terminal → the request may have run with its result
//!    lost in the crash → ambiguous → stop the run for review (do NOT auto-resume,
//!    re-running could duplicate a side effect).
//!  - Prepared with no terminal → retain Deferred/Approved separately; never re-gate.
//!  - no Prepared, intent or terminal → safe to re-dispatch.

pub use crate::held_calls::HeldCallsProjection;
pub use crate::step_usage::{
    content_timing_intervals, reported_step_usage, step_finish, ModelShare, RunUsageSteps,
    StepServed,
};
pub use crate::structured_output::ended_structured_output;
pub use crate::tool_results::no_dispatch_result;

use broca_protocol::PauseReason;
use broca_protocol::{error::ProviderError, FinishReason, RunId, ToolCallId, Usage};
/// WAL-derived identity of the run that produced one transcript message.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RunAttribution {
    pub run_id: String,
    pub episode: String,
    #[serde(rename = "final")]
    pub is_final: bool,
    /// Projection metadata stored with the row, exposed beside (not inside) its wire run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<broca_protocol::tags::DeliveryAttribution>,
}
/// A replay could not produce a coherent state.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplayError {
    /// A legacy materialized prefix predates refusal-state projection and cannot seed replay.
    UnrecordedRefusalSchedule,
    /// A new episode (`RunStarted`) opened while the PREVIOUS episode had no durable
    /// terminal (`RunFinished`). The lease + one-active-run-per-lineage invariant
    /// guarantee a new run only starts after the prior episode reaches a durable
    /// terminal, so this is a corrupt lineage WAL — never fabricate a conversation
    /// prefix from it; surface it for operator review (a real, distinct state from a
    /// transient WAL IO error).
    CorruptLineage {
        /// The run_id of the unfinished episode that a new `RunStarted` tried to
        /// open over.
        episode_run_id: RunId,
        reason: String,
    },
    /// Numbering the transcript's tags would pass `u64::MAX` (llm-runner
    /// `tags::try_number_parts`): the session is refused rather than shown two parts with one
    /// number.
    TagNumbering(broca_protocol::tags::NumberingOverflow),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::UnrecordedRefusalSchedule => {
                write!(f,
                "materialized prefix has no recorded refusal state; complete WAL replay required")
            }
            ReplayError::CorruptLineage {
                episode_run_id,
                reason,
            } => write!(
                f,
                "corrupt lineage WAL at episode '{}': {reason}",
                episode_run_id.0
            ),
            ReplayError::TagNumbering(overflow) => write!(f, "{overflow}"),
        }
    }
}

impl std::error::Error for ReplayError {}

/// The durable state of one planned call of an open tool batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenCallState {
    /// A terminal is durable (a result of any outcome, or a no-dispatch record).
    Terminated,
    /// An intent or recovery tombstone is durable and no terminal is: the call may have run.
    Dispatched,
    /// The provider durably accepted responsibility for returning a late result.
    ProviderHeld,
    /// A sequential intent behind an open or closed-out held call; it was never sent.
    HeldSuccessor,
    /// No Prepared, intent, recovery tombstone or terminal: no attributed dispatch occurred.
    NotSent,
    /// Prepared arguments are durable, but no approval or terminal is durable.
    Deferred,
    /// Approval is durable, but neither a dispatch intent nor a terminal is durable.
    Approved,
}

/// The `CorruptLineage` reason replay gives when a tool batch or tool-calling step would
/// replace an uncommitted batch that holds call evidence. Read-only scanners match on it to
/// tell this refusal from other corrupt lineages.
pub const OVERLAPPING_BATCH_REASON: &str = "a tool batch opened over an uncommitted batch that \
     already holds dispatch intents, Prepared calls or results";

/// A run state reconciled from the WAL, including terminal metadata absent from the index.
///
/// Not `Eq`: `ProviderError` carries an `f64` retry hint.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconciledRunState {
    /// Last completed model step's normalized native reason; display only.
    pub finish: Option<cortexkit_role_llm_runner::finish::Finish>,
    pub account_limit_pause: Option<Box<broca_records::record::AccountLimitPause>>,
    pub budget_detail: Option<broca_protocol::BudgetExhaustedDetail>,
    pub context_ceiling: Option<broca_protocol::ContextCeilingDetail>,
    pub ceiling_deadline_ms: Option<u64>,
    /// The absolute deadline of the rewrite ride the active run's step waits for, once that wait
    /// is reported (`CompactionRideWait`, llm-runner 0.5.2 `compaction_wait`); `None` otherwise.
    pub ride_wait_deadline_ms: Option<u64>,
    /// On an ended run admitted with structured output: what it produced, as `run.result` and
    /// `run.status` report it. Read from the run's durable result record when it completed;
    /// derived as `absent` from its frozen settings when it ended any other way.
    pub structured_output: Option<broca_protocol::structured_output::StructuredOutputResult>,
    /// Only the live actor fills this; WAL reconciliation never guesses an in-flight step.
    pub live_step: Option<broca_protocol::provider::InFlightStep>,
    /// Provider-held calls without a durable terminal, in held-record order.
    pub held_calls: Vec<cortexkit_role_llm_runner::run::HeldCall>,
    pub state: crate::RunIndexState,
    /// Durable steer send ids with no later StepStarted to give their input to the model,
    /// in WAL order. Reported only after the run ends: adding the input to the transcript
    /// at RunFinished does not mean a model step received it. A present empty list means
    /// WAL replay determined that no steers are undelivered; absence means not determined.
    pub undelivered_steers: Option<Vec<String>>,
    pub final_step_finish_reason: Option<FinishReason>,
    pub indeterminate_tool_calls: Option<Vec<ToolCallId>>,
    pub usage: Option<Usage>,
    /// Completed-step totals across every episode bearing this run id.
    pub run_usage: broca_protocol::usage::RunUsage,
    pub retries_used: u32,
    /// The typed error carried verbatim by the record that settled `state`: the run's
    /// `RunFinished`, its `RunPaused`, or a queued-admission `Retracted` failure. `None` when
    /// that record carries no error, which is distinct from the run being unknown.
    pub error: Option<ProviderError>,
    /// Why the run is paused, taken from its `RunPaused`. Set only when `state` is `Paused`.
    pub pause_reason: Option<PauseReason>,
    /// Authentication selection from this run's durable start, never from current routing.
    pub auth_selection: Option<broca_records::record::AuthSelection>,
    /// The caller's title recorded for this run: its `RunStarted`'s, or, for a queued send that
    /// never started, its `Queued` record's. `None` when the send carried none.
    pub title: Option<String>,
}
