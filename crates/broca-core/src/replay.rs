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

use std::{collections::BTreeMap, sync::Arc};

use broca_protocol::PauseReason;
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, Message, ToolResult},
    error::ProviderError,
    BatchId, FinishReason, RunId, StepId, ToolCallId, Usage,
};
use broca_records::{
    record::DeferredDecision, record::RunConfig, record::WalRecord, record::WithdrawElicitation,
};
use broca_wal::FramedRecord;
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

/// The result of replaying a session's WAL — a LINEAGE of one or more episodes.
#[derive(Debug, Clone)]
pub struct ReplayState {
    /// First-run mode proof from a complete log or its cached head. A bare tail
    /// cannot establish this value and leaves it unknown.
    pub mode3: Option<bool>,
    /// Timed recovery evidence from the current pause, never inferred from an error code.
    pub account_limit_pause: Option<Box<broca_records::record::AccountLimitPause>>,
    /// Session-scoped recovery tombstones survive episode boundaries.
    pub recovered_calls: Vec<broca_records::recovery::RecoveryRun>,
    /// Next absolute transcript ordinal, including messages not held in the prompt.
    pub extent: u64,
    /// Retained read-side field from the v1 provider-owned numbering protocol.
    /// The current writer leaves it false: legacy numbers are frozen and mode-3
    /// numbering is anchored by host markers, never a Setup answer.
    pub tags_await_setup: bool,
    pub input_budget: crate::input_budget::InputTokenBudget,
    pub cancel_at: Option<broca_records::record::RunCancelAt>,
    /// Run metadata beside the prompt, never part of model input or WAL message bytes.
    pub message_runs: Vec<Option<RunAttribution>>,
    /// Last transcript position produced by each run whose RunFinished is durable. Positions
    /// include the materialized prefix when replaying only records above a store watermark.
    pub run_finals: std::collections::BTreeMap<u64, RunAttribution>,
    pub compaction: crate::compaction::CompactionCheckpoint,
    pub compaction_history: crate::compaction::CompactionHistory,
    /// The measured step and recorded output cap the run loop's output reservation reads.
    pub output_reservation: crate::output_reservation::OutputReservationHistory,
    /// The lowest sequence number this lineage's next compaction notice may take: 1, or, for a
    /// lineage at an address whose earlier lineages retention deleted, the number their
    /// tombstones recorded. Replay alone cannot know it; admission sets it from the WAL store.
    pub compaction_notice_floor: u64,
    /// The first episode's WAL-frozen plan, restored by admission for tool dispatch.
    pub manifest: Option<std::sync::Arc<broca_records::record::PlanManifest>>,
    pub keep_warm: Option<broca_protocol::request::KeepWarm>,
    pub on_restart: Option<broca_protocol::request::OnRestart>,
    /// Every committed tool turn in the replayed transcript, as `(index in prompt of its
    /// assistant message, index among the replayed records of the step record that produced
    /// it)`. A message no step produced (an imported one, say) has no entry, so a reader can
    /// tie each call to its own step and dispatch records by position, never by content.
    pub committed_tool_turns: Vec<(usize, usize)>,
    /// The unfinished episode's open assistant tool turn with a dispatch intent or Prepared call, and its step
    /// record index, for reads.
    /// It is not in `prompt`: model requests must not contain calls without their results.
    pub open_tool_turn: Option<(Message, usize)>,
    pub open_tool_run: Option<RunAttribution>,
    /// Cache marker priority recorded by the latest episode and inherited by later episodes.
    pub marker_order_version: Option<u32>,
    /// The LAST episode's authoritative request config (the one frozen at that
    /// episode's run start), if any episode has started.
    pub run_config: Option<RunConfig>,
    /// The LAST episode's run_id — resume reuses the original run_id rather than
    /// minting a new one.
    pub run_id: Option<RunId>,
    /// The reconstructed PROVIDER-VALID prompt prefix: every CLOSED episode's
    /// committed turns (the immutable conversation history) ⊕ the last episode's
    /// committed turns.
    pub prompt: Vec<Message>,
    /// Count of durable model steps in the LAST episode (each was billed). Per
    /// episode, not a lineage total — resume continues the last episode's numbering.
    pub completed_steps: u64,
    /// Cumulative tool calls emitted by durable model steps in the last episode.
    pub tool_call_count: u64,
    /// Budget nudge indexes already injected in the last episode.
    pub budget_nudges_fired: Vec<usize>,
    /// Whether the finalize budget message has fired in the last episode.
    pub budget_finalize_fired: bool,
    /// Whether the next model step is the one allowed finalize turn.
    pub budget_finalize_pending: bool,
    /// Accumulated usage across the LAST episode's committed steps.
    pub usage: Usage,
    /// Provider retries spent by the LAST episode's committed steps.
    pub retries_used: u32,
    /// The LAST completed model step in the LAST episode; it resets when a new episode starts.
    pub last_step_usage: Option<Usage>,
    /// The provider finish reason recorded on that last step, if present in the WAL.
    pub last_step_finish_reason: Option<FinishReason>,
    /// Whether any provider call began in the last episode. An auth-at-start pause has no
    /// `StepStarted`, so its incomplete render config may be replaced exactly once after auth.
    pub provider_call_started: bool,
    /// Whether the last episode's `run_finished` record is durable. `false` with a
    /// `LineageIdle` resolution means the episode ended naturally but wasn't finalized
    /// (resume must WRITE `run_finished`); `true` means it's already finalized.
    pub run_finished_durable: bool,
    /// For each episode that committed at least one model step, the `prompt` range running from
    /// its first model-produced message to the end of that episode (exclusive of the next
    /// episode's input), in lineage order.
    ///
    /// The run loop needs these to re-render every earlier episode's tool results exactly as they
    /// were first sent: model-only repetition guidance is folded per episode, so the fold must
    /// restart at each episode's first model step and must not see a later episode's input.
    /// `None` when the prompt was assembled from a materialized store prefix, whose messages do
    /// not record where episodes began; the caller must then fall back to a full WAL replay when it
    /// needs them.
    pub episode_step_spans: Option<Vec<std::ops::Range<usize>>>,
    /// All admitted episode render inputs; absent for a materialized prefix without WAL history.
    pub render_episodes: Option<Vec<broca_protocol::request::EpisodeRenderInput>>,
    /// Where kept signed reasoning stops being sent because a run's tools changed
    /// (`crate::signed_floor`), folded from every `RunStarted` when the replay starts at the
    /// lineage's first record; unknown for a replay of a WAL tail.
    pub signed_floor: crate::signed_floor::FloorFold,
    /// The last episode's uncommitted tool batch, backed by `tool_batch_started`
    /// or recovery attribution, with each planned call's durable state.
    ///
    /// A resume that seals the episode because a call is indeterminate reads this to write the
    /// results that close the batch, so the next send renders every call paired with a result.
    pub open_tool_batch: Option<OpenToolBatch>,
    /// Steered messages the last episode recorded that no step boundary has placed yet, in
    /// WAL order. They are NOT in `prompt`: a resumed run appends them at its first step
    /// boundary, exactly where a replay of the continued WAL will place them.
    pub pending_steers: Vec<Message>,
    /// What to do next.
    pub resolution: ReplayResolution,
    /// The session's client-side refusal-fallback schedule: the last one a `model_step_refused`
    /// or `model_step_finished` recorded, cleared when an episode is admitted with another model
    /// or a day after the schedule last changed. `None` when no refusal armed one. A replay
    /// that starts from a materialized prefix seeds this state before folding its tail.
    pub refusal_fallback: Option<broca_records::record::RefusalFallbackSchedule>,
    /// The last episode's steps a `model_step_refused` re-sent to the fallback model.
    pub refused_steps: std::collections::BTreeSet<StepId>,
}

/// An opened, uncommitted tool batch as the WAL left it.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenToolBatch {
    /// Recovery retained the model turn but removed this batch's start record.
    pub needs_start: bool,
    pub batch_id: BatchId,
    /// The model step, or zero for close-out-only attribution whose step was lost.
    pub step_id: StepId,
    /// Every planned call in plan order, with its durable state.
    pub calls: Vec<(ToolCallId, OpenCallState)>,
}

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

/// The trailing-state decision after replaying the lineage — about the LAST episode.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplayResolution {
    /// The last episode reached a durable terminal, so the lineage is IDLE and
    /// APPENDABLE — a later turn may open a new episode. This is NOT "lineage sealed"
    /// (the name is deliberately not `Finished`, which the pre-lineage code read as
    /// sealed — keeping that name here would re-load the exact bug). `last_reason`
    /// carries the terminal reason for `run.status`.
    LineageIdle { last_reason: RunFinishReason },
    /// One or more calls had an ambiguous outcome (the request left but its result
    /// wasn't durably recorded, so the side effect MAY have applied). Stop the run
    /// for review — do NOT auto-resume, since re-running could duplicate the effect.
    Interrupted { indeterminate: Vec<ToolCallId> },
    /// A clean resume point. `pending` is the trailing tool-turn that needs
    /// completing (None ⇒ resume re-runs the model from `prompt`).
    Resume { pending: Option<PendingTurn> },
    /// A provider-held batch, including one whose delivered terminals still owe its barrier.
    /// This is a runner-owned wait, never an authentication pause.
    ProviderHeld {
        run_id: RunId,
        pending: PendingTurn,
        open_held: Vec<ToolCallId>,
    },
    /// The last episode is PAUSED awaiting (re-)auth — a NON-terminal, resumable
    /// state (a `RunPaused` record, distinct from a terminal `RunFinished`). Resume
    /// continues the SAME run_id from the last durable boundary; no new episode starts
    /// until the pause resolves or is cancelled.
    AuthRequired {
        run_id: RunId,
        reason: PauseReason,
        /// Boxed because it is the enum's largest field and is read only to report the pause;
        /// kept inline, it would make every resolution as large as this variant.
        error: Option<Box<ProviderError>>,
        /// A restart pause can retain an open batch without committing its tool turn.
        pending: Option<PendingTurn>,
    },
}

/// A trailing tool-turn whose batch isn't complete on disk — resume must finish
/// it WITHOUT re-running the (durable, billed) model step. Reconstructed from the
/// WAL so a second crash mid-resume still converges.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingTurn {
    /// The step that produced the assistant message (do NOT regenerate it).
    pub step_id: StepId,
    /// The assistant message (its tool calls carry name+input for re-dispatch).
    pub assistant: AssistantMessage,
    /// `Some` if a `tool_batch_started` is already durable (resume must NOT write
    /// another — it'd reset the batch on re-replay); `None` ⇒ the batch was never
    /// opened (crash between model_step_finished and batch_started) and resume
    /// opens it fresh.
    pub batch_id: Option<BatchId>,
    /// All planned call ids in source order (from the durable batch if started,
    /// else from the assistant's tool calls).
    pub planned: Vec<ToolCallId>,
    /// Already-durable terminals, reconstructed (real results, no_dispatch, or
    /// hard-stop) — included as-is, NEVER re-dispatched.
    pub existing: Vec<(ToolCallId, ToolResult)>,
    /// Unprepared calls with no dispatch intent or terminal result record. Only these calls
    /// may be dispatched on recovery, and none may be dispatched after the input cap is spent.
    pub redispatch: Vec<ToolCallId>,
    /// Prepared calls still awaiting an answer; their mutate hooks must never run again.
    pub deferred: Vec<PreparedCall>,
    /// Approved calls awaiting second validation, never ordinary redispatch.
    pub approved: Vec<PreparedCall>,
    /// Durable held replies to rejoin without sending the call again.
    pub provider_held: Vec<ProviderHeldCall>,
    /// Intents already written at barrier 1 but kept behind a sequential hold.
    pub held_successors: Vec<ToolCallId>,
    /// The assistant message's runner tags, when the session has tags.
    pub assistant_tags: Option<broca_protocol::tags::MessageTags>,
    /// The runner tag of each already-durable terminal in `existing`, by call id in planned
    /// order, for the terminals that have one.
    pub existing_tags: Vec<(ToolCallId, u64)>,
}

/// A Prepared call retained by replay, with the records needed to resume its approval gate.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedCall {
    /// Index of the Prepared record in this replay's input. A framed reader derives the filing
    /// key from that frame's sequence and lineage, not from the model's reusable call id.
    pub record_index: usize,
    pub prepared: Arc<WalRecord>,
    pub approval: Option<Arc<WalRecord>>,
    /// Latest filing marker's id, if filing returned before the cut.
    pub elicitation_id: Option<String>,
}

/// The durable metadata needed to rejoin a provider's waiting slot without another send.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderHeldCall {
    pub tool_call_id: ToolCallId,
    pub call_key: String,
    pub module: String,
    pub reason: String,
    pub detail_digest: Option<String>,
    pub held_at_ms: u64,
    pub sequential: bool,
}

/// A closed Prepared call whose unanswered question still needs a withdrawal.
#[derive(Debug, Clone, PartialEq)]
pub struct OwedWithdrawal {
    pub prepared: FramedRecord,
    pub withdrawal: WithdrawElicitation,
}

/// Approval work that must remain discoverable in the live WAL after a run ends.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DeferredCallObligations {
    pub withdrawals: Vec<OwedWithdrawal>,
    pub expired_without_late_record: Vec<String>,
    pub pending_late_execution: Vec<String>,
}

/// The approval metadata retained across a projection watermark. Conversation rendering uses
/// ordinary replay; this summary retains only facts needed for call display and outstanding work.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeferredCallProjection {
    pub lineage_id: String,
    pub last_run_id: Option<RunId>,
    pub calls: BTreeMap<String, ProjectedDeferredCall>,
    /// Runs cancelled or non-resumably interrupted while they still own open calls.
    /// This cache is rebuilt from RunFinished records, not stored in the WAL itself.
    /// Missing legacy data cannot authorize post-expiry tool execution; replay all
    /// WAL records from the lineage's origin before deciding whether a run stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped_runs: Option<std::collections::BTreeSet<String>>,
    /// Physical recovery made these run/call identities outcome-unknown. A bare
    /// model-facing ID remains usable by a different run.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub recovery_tombstones: std::collections::BTreeSet<(String, String)>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub recovery_call_keys: std::collections::BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recovery_identities: Vec<broca_records::recovery::RecoveryRun>,
    /// Latest generation in the folded WAL prefix. Each projected deferred call
    /// retains the generation that preceded its Prepared frame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_generation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProjectedDeferredCall {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_generation: Option<String>,
    pub run_id: Option<RunId>,
    pub prepared_seq: u64,
    pub prepared_fence: u64,
    pub prepared: Arc<WalRecord>,
    pub state: OpenCallState,
    pub decision: Option<DeferredDecision>,
    pub elicitation_id: Option<String>,
    pub withdrawal_terminal: bool,
    pub withdrawal_acknowledged: bool,
    pub late_approval: Option<Arc<WalRecord>>,
    pub late_dispatch_intent: Option<Arc<WalRecord>>,
    pub late_result: Option<Arc<WalRecord>>,
}

/// A late answer and its isolated execution result, never part of a model prompt.
#[derive(Debug, Clone, PartialEq)]
pub struct LateApprovalView {
    pub call_key: String,
    pub approval: Arc<WalRecord>,
    pub outcome: Option<broca_records::record::LateOutcome>,
    pub result: Option<Arc<ToolResult>>,
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
