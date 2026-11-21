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
pub use crate::step_usage::RunUsageProjection;
pub use crate::step_usage::{
    content_timing_intervals, reported_step_usage, step_finish, ModelShare, RunUsageSteps,
    StepServed,
};
pub use crate::structured_output::ended_structured_output;
pub use crate::tool_results::no_dispatch_result;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use crate::placement::{AppendKind, HoldKind, Placement, PlacementAction};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolResult},
    error::ProviderError,
    request::BudgetConfig,
    BatchId, FinishReason, RunId, StepId, ToolCallId, Usage,
};
use broca_wal::{
    DeferredDecision, DispatchOutcome, FramedRecord, LateDecision, LateExecution, NoDispatchReason,
    PauseReason, RunConfig, WalLineageId, WalRecord, WithdrawElicitation,
};
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
fn producing_run(id: &RunId) -> RunAttribution {
    RunAttribution {
        run_id: id.0.clone(),
        // RunStarted opens one user turn, possibly with many model steps. Continuing it
        // after a process restart reuses that record's run id rather than minting a new one,
        // so the run and episode coordinates identify the same durable turn.
        episode: id.0.clone(),
        is_final: false,
        delivery: None,
    }
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
    pub account_limit_pause: Option<Box<broca_wal::record::AccountLimitPause>>,
    /// Session-scoped recovery tombstones survive episode boundaries.
    pub recovered_calls: Vec<broca_records::recovery::RecoveryRun>,
    /// Next absolute transcript ordinal, including messages not held in the prompt.
    pub extent: u64,
    /// Retained read-side field from the v1 provider-owned numbering protocol.
    /// The current writer leaves it false: legacy numbers are frozen and mode-3
    /// numbering is anchored by host markers, never a Setup answer.
    pub tags_await_setup: bool,
    pub input_budget: crate::input_budget::InputTokenBudget,
    pub cancel_at: Option<broca_wal::record::RunCancelAt>,
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
    pub manifest: Option<std::sync::Arc<broca_wal::PlanManifest>>,
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
    pub refusal_fallback: Option<broca_wal::RefusalFallbackSchedule>,
    /// The last episode's steps a `model_step_refused` re-sent to the fallback model.
    pub refused_steps: std::collections::BTreeSet<StepId>,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct TranscriptSeed {
    pub tags: crate::tags::TagSeed,
    pub mode3: Option<bool>,
}

impl From<crate::tags::TagSeed> for TranscriptSeed {
    fn from(tags: crate::tags::TagSeed) -> Self {
        Self { tags, mode3: None }
    }
}

impl TranscriptSeed {}

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

impl ProviderHeldCall {}

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

impl DeferredCallObligations {
    pub fn is_empty(&self) -> bool {
        self.withdrawals.is_empty()
            && self.expired_without_late_record.is_empty()
            && self.pending_late_execution.is_empty()
    }
}

/// Derive approval debt from a complete WAL, never from a projection tail or a pending marker.
/// Withdrawal requires exactly: Prepared, no approval of any decision, a withdrawal or reader
/// not-dispatched terminal, and no acknowledgement. Approval and late records do not enter prompts.
pub fn deferred_call_obligations(
    lineage_id: WalLineageId,
    records: &[FramedRecord],
) -> DeferredCallObligations {
    let mut projection = DeferredCallProjection::new(lineage_id);
    projection.fold_frames(records);
    projection.obligations()
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

fn deferred_effect_identity(
    lineage: &str,
    call: &ProjectedDeferredCall,
) -> Option<broca_records::recovery::RecoveryCall> {
    let WalRecord::ToolCallDeferred {
        call_id, batch_id, ..
    } = call.prepared.as_ref()
    else {
        return None;
    };
    let key = physical_call_key(
        lineage,
        call.prepared_seq,
        call.recovery_generation.as_deref(),
    );
    let batch = if call.decision == Some(DeferredDecision::Expired) {
        BatchId(format!("late:{key}"))
    } else {
        batch_id.clone()
    };
    Some(broca_records::recovery::RecoveryCall {
        tool_call_id: call_id.clone(),
        batch_id: Some(batch),
        step_id: None,
        call_keys: vec![key],
    })
}

/// A late answer and its isolated execution result, never part of a model prompt.
#[derive(Debug, Clone, PartialEq)]
pub struct LateApprovalView {
    pub call_key: String,
    pub approval: Arc<WalRecord>,
    pub outcome: Option<broca_wal::LateOutcome>,
    pub result: Option<Arc<ToolResult>>,
}

impl DeferredCallProjection {
    pub fn new(lineage_id: impl std::fmt::Display) -> Self {
        Self {
            lineage_id: lineage_id.to_string(),
            last_run_id: None,
            calls: BTreeMap::new(),
            stopped_runs: Some(std::collections::BTreeSet::new()),
            recovery_tombstones: std::collections::BTreeSet::new(),
            recovery_call_keys: std::collections::BTreeSet::new(),
            recovery_identities: Vec::new(),
            recovery_generation: None,
        }
    }

    /// Fold a suffix into the same summary used by complete-WAL debt discovery. Call ids are
    /// scoped to their batch; the Prepared frame, not a reused model id, owns the call's key.
    pub fn fold_frames(&mut self, records: &[FramedRecord]) {
        for frame in records {
            match &frame.record {
                WalRecord::RunStarted { run_id, .. } => self.last_run_id = Some(run_id.clone()),
                WalRecord::RunFinished {
                    reason: RunFinishReason::Cancelled | RunFinishReason::Interrupted,
                    ..
                } => {
                    if let (Some(run), Some(stopped)) = (&self.last_run_id, &mut self.stopped_runs)
                    {
                        stopped.insert(run.0.clone());
                    }
                }
                WalRecord::ToolCallDeferred { .. } => {
                    let key = physical_call_key(
                        &self.lineage_id,
                        frame.seq,
                        self.recovery_generation.as_deref(),
                    );
                    self.calls
                        .entry(key)
                        .or_insert_with(|| ProjectedDeferredCall {
                            recovery_generation: self.recovery_generation.clone(),
                            run_id: self.last_run_id.clone(),
                            prepared_seq: frame.seq,
                            prepared_fence: frame.fence,
                            prepared: Arc::new(frame.record.clone()),
                            state: OpenCallState::Deferred,
                            decision: None,
                            elicitation_id: None,
                            withdrawal_terminal: false,
                            withdrawal_acknowledged: false,
                            late_approval: None,
                            late_dispatch_intent: None,
                            late_result: None,
                        });
                }
                WalRecord::ApprovalRecorded {
                    call_key,
                    decision,
                    closing_result,
                    elicitation_id,
                    ..
                } => {
                    if let Some(call) = self.calls.get_mut(call_key) {
                        call.decision = Some(*decision);
                        call.elicitation_id = elicitation_id.clone();
                        if closing_result.is_some() {
                            call.state = OpenCallState::Terminated;
                        } else if *decision == DeferredDecision::Approved {
                            call.state = OpenCallState::Approved;
                        }
                    }
                }
                WalRecord::AwaitingApproval { pending, .. } => {
                    for entry in pending {
                        if let Some(call) = self.calls.get_mut(&entry.call_key) {
                            // Recovery can learn an id after crash close-out. Updating identity
                            // metadata must never reopen the closed call.
                            if entry.elicitation_id.is_some()
                                && (call.state == OpenCallState::Deferred
                                    || (call.withdrawal_terminal
                                        && !call.withdrawal_acknowledged
                                        && call.decision.is_none()))
                            {
                                call.elicitation_id = entry.elicitation_id.clone();
                            }
                        }
                    }
                }
                WalRecord::ToolDispatchIntent {
                    batch_id,
                    tool_call_id,
                    call_key,
                    ..
                } => {
                    let key = call_key
                        .clone()
                        .or_else(|| self.anchor(batch_id, tool_call_id));
                    if let Some(call) = key.and_then(|key| self.calls.get_mut(&key)) {
                        if call.state != OpenCallState::Terminated {
                            call.state = OpenCallState::Dispatched;
                        }
                    }
                }
                WalRecord::ToolResult {
                    batch_id,
                    result,
                    outcome,
                    withdraw_elicitation,
                    ..
                } => {
                    let key = withdraw_elicitation
                        .as_ref()
                        .map(|withdrawal| withdrawal.call_key.clone())
                        .or_else(|| self.anchor(batch_id, &result.tool_call_id));
                    if let Some(call) = key.and_then(|key| self.calls.get_mut(&key)) {
                        call.state = OpenCallState::Terminated;
                        call.withdrawal_terminal = withdraw_elicitation.is_some()
                            || (*outcome == DispatchOutcome::Interrupted
                                && **result
                                    == crate::resume::not_dispatched_result(&result.tool_call_id));
                        if let Some(withdrawal) = withdraw_elicitation {
                            call.elicitation_id = withdrawal.elicitation_id.clone();
                        }
                    }
                }
                WalRecord::ToolNoDispatch {
                    batch_id,
                    tool_call_id,
                    ..
                } => {
                    if let Some(key) = self.anchor(batch_id, tool_call_id) {
                        self.calls.get_mut(&key).unwrap().state = OpenCallState::Terminated;
                    }
                }
                WalRecord::QuestionWithdrawalAcknowledged { call_key, .. } => {
                    if let Some(call) = self.calls.get_mut(call_key) {
                        call.withdrawal_acknowledged = true;
                    }
                }
                WalRecord::LateApprovalRecorded {
                    call_key,
                    elicitation_id,
                    ..
                } => {
                    if let Some(call) = self.calls.get_mut(call_key) {
                        call.elicitation_id = Some(elicitation_id.clone());
                        call.late_approval = Some(Arc::new(frame.record.clone()));
                    }
                }
                WalRecord::LateDispatchIntent { call_key, .. } => {
                    if let Some(call) = self.calls.get_mut(call_key) {
                        call.late_dispatch_intent = Some(Arc::new(frame.record.clone()));
                    }
                }
                WalRecord::LateResult { call_key, .. } => {
                    if let Some(call) = self.calls.get_mut(call_key) {
                        call.late_result = Some(Arc::new(frame.record.clone()));
                    }
                }
                WalRecord::RecoveryCallClosed {
                    run_id,
                    call_key,
                    tool_call_id,
                    batch_id,
                    step_id,
                    ..
                } => {
                    let mut terminal = broca_records::recovery::RecoveryCall {
                        tool_call_id: tool_call_id.clone(),
                        batch_id: batch_id.clone(),
                        step_id: *step_id,
                        call_keys: call_key.iter().cloned().collect(),
                    };
                    for run in &self.recovery_identities {
                        if &run.run_id == run_id {
                            for call in &run.calls {
                                if call.terminal_matches(&terminal) {
                                    terminal.call_keys.extend(call.call_keys.iter().cloned());
                                }
                            }
                        }
                    }
                    terminal.call_keys.sort();
                    terminal.call_keys.dedup();
                    self.recovery_tombstones
                        .insert((run_id.0.clone(), tool_call_id.0.clone()));
                    self.recovery_call_keys.extend(call_key.iter().cloned());
                    self.recovery_identities
                        .push(broca_records::recovery::RecoveryRun {
                            run_id: run_id.clone(),
                            calls: vec![terminal.clone()],
                        });
                    for call in self.calls.values_mut() {
                        if call.run_id.as_ref() == Some(run_id)
                            && deferred_effect_identity(&self.lineage_id, call)
                                .is_some_and(|identity| identity.terminal_matches(&terminal))
                        {
                            call.state = OpenCallState::Terminated;
                            call.late_result = Some(Arc::new(frame.record.clone()));
                        }
                    }
                }
                WalRecord::RecoveryCut { recovery } => {
                    self.recovery_generation = Some(recovery.cut_id.clone());
                    for run in &recovery.runs {
                        self.recovery_identities.push(run.clone());
                        self.recovery_tombstones.extend(
                            run.calls
                                .iter()
                                .map(|call| (run.run_id.0.clone(), call.tool_call_id.0.clone())),
                        );
                        self.recovery_call_keys.extend(
                            run.calls
                                .iter()
                                .flat_map(|call| call.call_keys.iter().cloned()),
                        );
                    }
                    self.recovery_call_keys.extend(
                        recovery
                            .unowned_calls
                            .iter()
                            .flat_map(|call| call.call_keys.iter().cloned()),
                    );
                }
                _ => {}
            }
            let open_runs: std::collections::BTreeSet<_> = self
                .calls
                .values()
                .filter(|call| {
                    matches!(
                        call.state,
                        OpenCallState::Deferred | OpenCallState::Approved
                    ) || Self::late_open(call)
                })
                .filter_map(|call| call.run_id.as_ref().map(|run| run.0.clone()))
                .collect();
            if let Some(stopped) = &mut self.stopped_runs {
                stopped.retain(|run| open_runs.contains(run));
            }
        }
    }

    pub fn late_open(call: &ProjectedDeferredCall) -> bool {
        call.decision == Some(DeferredDecision::Expired)
            && call.late_result.is_none()
            && matches!(
                call.late_approval.as_deref(),
                None | Some(WalRecord::LateApprovalRecorded {
                    decision: LateDecision::Approved,
                    late_execution: Some(LateExecution::Execute),
                    ..
                })
            )
    }

    pub fn late_allowed(&self, call: &ProjectedDeferredCall) -> bool {
        !self.recovery_tombstoned(call)
            && self.stopped_runs.as_ref().is_some_and(|stopped| {
                call.run_id
                    .as_ref()
                    .is_some_and(|run| !stopped.contains(&run.0))
            })
    }

    pub(crate) fn effect_identity(
        &self,
        call: &ProjectedDeferredCall,
    ) -> Option<broca_records::recovery::RecoveryCall> {
        deferred_effect_identity(&self.lineage_id, call)
    }

    pub fn recovery_tombstoned(&self, call: &ProjectedDeferredCall) -> bool {
        let Some(run_id) = &call.run_id else {
            return false;
        };
        let Some(identity) = self.effect_identity(call) else {
            return false;
        };
        let call_id = &identity.tool_call_id;
        let key = &identity.call_keys[0];
        if self.recovery_call_keys.contains(key) {
            return true;
        }
        let mut detailed = false;
        for run in &self.recovery_identities {
            if &run.run_id != run_id {
                continue;
            }
            for recovered in &run.calls {
                if recovered.tool_call_id != *call_id {
                    continue;
                }
                detailed = true;
                if !recovered.has_effect_identity() || recovered.matches_effect(&identity) {
                    return true;
                }
            }
        }
        // Older materialized summaries can only refuse conservatively until a
        // complete fold restores their batch/key evidence.
        !detailed
            && self
                .recovery_tombstones
                .contains(&(run_id.0.clone(), call_id.0.clone()))
    }

    fn anchor(&self, batch: &BatchId, id: &ToolCallId) -> Option<String> {
        self.calls
            .iter()
            .find_map(|(key, call)| match &*call.prepared {
                WalRecord::ToolCallDeferred {
                    batch_id, call_id, ..
                } if batch_id == batch && call_id == id => Some(key.clone()),
                _ => None,
            })
    }

    /// The sole withdrawal predicate, shared by full WAL readers and prefix-plus-tail readers.
    pub fn obligations(&self) -> DeferredCallObligations {
        let mut obligations = DeferredCallObligations::default();
        for (key, call) in &self.calls {
            if call.decision.is_none() && call.withdrawal_terminal && !call.withdrawal_acknowledged
            {
                obligations.withdrawals.push(OwedWithdrawal {
                    prepared: FramedRecord {
                        seq: call.prepared_seq,
                        fence: call.prepared_fence,
                        record: (*call.prepared).clone(),
                    },
                    withdrawal: WithdrawElicitation {
                        call_key: key.clone(),
                        elicitation_id: call.elicitation_id.clone(),
                    },
                });
            }
            if self.late_allowed(call)
                && call.decision == Some(DeferredDecision::Expired)
                && call.late_approval.is_none()
                && call.late_result.is_none()
            {
                obligations.expired_without_late_record.push(key.clone());
            }
            if matches!(
                call.late_approval.as_deref(),
                Some(WalRecord::LateApprovalRecorded {
                    decision: LateDecision::Approved,
                    late_execution: Some(LateExecution::Execute),
                    ..
                })
            ) && call.late_result.is_none()
                && self.late_allowed(call)
            {
                obligations.pending_late_execution.push(key.clone());
            }
        }
        obligations
    }
}

/// One terminal for a planned call.
#[derive(Clone)]
enum Terminal {
    /// The result, its outcome, and the result's runner tag when the session has tags.
    Result(Arc<ToolResult>, DispatchOutcome, Option<u64>),
    NoDispatch(NoDispatchReason, Option<u64>),
}

/// The single tag of a one-part record's tags (a tool result, a no-dispatch, a budget message).
fn single_tag(tags: Option<&broca_protocol::tags::MessageTags>) -> Option<u64> {
    tags.and_then(|tags| (tags.parts.len() == 1).then(|| tags.parts[0]))
}

/// `message` with its record's `tags` attached (see [`crate::tags::attach`]).
fn tagged(mut message: Message, tags: Option<&broca_protocol::tags::MessageTags>) -> Message {
    crate::tags::attach(&mut message, tags);
    message
}

/// State of the currently-open tool batch during the linear walk.
struct OpenBatch {
    started: bool,
    recovered: HashSet<ToolCallId>,
    /// The model step whose tool calls this batch runs, from its `tool_batch_started`.
    step_id: StepId,
    planned: Vec<ToolCallId>,
    intents: HashSet<ToolCallId>,
    terminals: Vec<Option<Terminal>>,
    prepared: Vec<Option<PreparedCall>>,
    provider_held: HashMap<ToolCallId, ProviderHeldCall>,
}

impl OpenBatch {
    fn new(step_id: StepId, planned: Vec<ToolCallId>) -> Self {
        Self {
            started: true,
            recovered: HashSet::new(),
            step_id,
            terminals: vec![None; planned.len()],
            prepared: vec![None; planned.len()],
            planned,
            intents: HashSet::new(),
            provider_held: HashMap::new(),
        }
    }

    /// Whether any record after the batch start says something about one of its calls: a
    /// dispatch intent, a Prepared call, a recovery tombstone, or a terminal.
    /// Replacing such a batch would forget
    /// that a call may have run, or already ran, and let a resume send it again.
    fn holds_call_evidence(&self) -> bool {
        !self.intents.is_empty()
            || !self.recovered.is_empty()
            || self.terminals.iter().any(Option::is_some)
            || self.prepared.iter().any(Option::is_some)
    }

    /// The reconstructed result for a planned call (from its terminal).
    fn result_for(&self, position: usize) -> Option<ToolResult> {
        let id = &self.planned[position];
        self.terminals[position].as_ref().map(|t| match t {
            Terminal::Result(r, _, _) => r.as_ref().clone(),
            Terminal::NoDispatch(reason, _) => no_dispatch_result(id, reason),
        })
    }

    /// The runner tag of a planned call's terminal, when it has one.
    fn tag_for(&self, position: usize) -> Option<u64> {
        self.terminals[position].as_ref().and_then(|t| match t {
            Terminal::Result(_, _, tag) | Terminal::NoDispatch(_, tag) => *tag,
        })
    }

    /// The tags of the tool-role message holding every terminated call's result in planned
    /// order. Each result is its own part with its own record's tag. `None` only when no
    /// result carries a tag: a lineage admitted before tags existed.
    fn result_tags(&self) -> Option<broca_protocol::tags::PartTags> {
        let parts: Vec<Option<u64>> = (0..self.planned.len())
            .filter(|position| self.terminals[*position].is_some())
            .map(|position| self.tag_for(position))
            .collect();
        parts
            .iter()
            .any(Option::is_some)
            .then(|| broca_protocol::tags::PartTags::new(parts))
    }

    // The runner writes terminals in source order for each repeated id. An
    // intent belongs only to the first copy: later copies never dispatch.
    fn insert_terminal(&mut self, id: &ToolCallId, terminal: Terminal) {
        if let Some(position) = self
            .planned
            .iter()
            .enumerate()
            .find_map(|(position, planned)| {
                (planned == id && self.terminals[position].is_none()).then_some(position)
            })
        {
            self.terminals[position] = Some(terminal);
        }
    }

    fn has_intent(&self, position: usize) -> bool {
        let id = &self.planned[position];
        (self.intents.contains(id) || self.recovered.contains(id))
            && !self.planned[..position].contains(id)
    }

    /// Calls whose outcome is ambiguous: an `Unknown` terminal, or an intent or
    /// recovery tombstone with no terminal (the result may have been lost).
    fn indeterminate(&self) -> Vec<ToolCallId> {
        self.planned
            .iter()
            .enumerate()
            .filter(|(position, _)| match &self.terminals[*position] {
                Some(Terminal::Result(_, DispatchOutcome::Unknown, _)) => true,
                Some(_) => false,
                None if matches!(
                    self.call_state(*position),
                    OpenCallState::ProviderHeld | OpenCallState::HeldSuccessor
                ) =>
                {
                    false
                }
                None => self.has_intent(*position), // intent ∧ ¬terminal
            })
            .map(|(_, id)| id.clone())
            .collect()
    }

    fn call_state(&self, position: usize) -> OpenCallState {
        if self.terminals[position].is_some() {
            OpenCallState::Terminated
        } else if self.recovered.contains(&self.planned[position])
            && !self.planned[..position].contains(&self.planned[position])
        {
            OpenCallState::Dispatched
        } else if self.has_intent(position) {
            if self.provider_held.contains_key(&self.planned[position]) {
                OpenCallState::ProviderHeld
            } else if self.planned[..position]
                .iter()
                .enumerate()
                .any(|(index, id)| {
                    self.provider_held.get(id).is_some_and(|held| {
                        held.sequential
                            && matches!(
                                self.terminals[index],
                                None | Some(Terminal::Result(
                                    _,
                                    DispatchOutcome::Interrupted | DispatchOutcome::MaxSteps,
                                    _
                                ))
                            )
                    })
                })
            {
                OpenCallState::HeldSuccessor
            } else {
                OpenCallState::Dispatched
            }
        } else if let Some(prepared) = &self.prepared[position] {
            if prepared.approval.is_some() {
                OpenCallState::Approved
            } else {
                OpenCallState::Deferred
            }
        } else {
            OpenCallState::NotSent
        }
    }

    fn prepared_in_state(&self, state: OpenCallState) -> Vec<PreparedCall> {
        self.prepared
            .iter()
            .enumerate()
            .filter_map(|(position, prepared)| {
                (self.call_state(position) == state)
                    .then(|| prepared.clone())
                    .flatten()
            })
            .collect()
    }

    /// Only unprepared calls with neither intent nor terminal can be re-gated.
    fn redispatchable(&self) -> Vec<ToolCallId> {
        self.planned
            .iter()
            .enumerate()
            .filter(|(position, _)| self.call_state(*position) == OpenCallState::NotSent)
            .map(|(_, id)| id.clone())
            .collect()
    }

    /// The already-terminated calls' reconstructed results, in planned order —
    /// included as-is on resume (never re-dispatched).
    fn existing_tags(&self) -> Vec<(ToolCallId, u64)> {
        self.planned
            .iter()
            .enumerate()
            .filter_map(|(position, id)| self.tag_for(position).map(|tag| (id.clone(), tag)))
            .collect()
    }

    fn existing_results(&self) -> Vec<(ToolCallId, ToolResult)> {
        self.planned
            .iter()
            .enumerate()
            .filter_map(|(position, id)| self.result_for(position).map(|r| (id.clone(), r)))
            .collect()
    }

    /// If this is a HARD-STOP repair batch — every planned call terminated with a
    /// synthetic stop outcome (`MaxSteps` or `Interrupted`, never a real dispatch) — the
    /// run-finish reason it represents. Such a committed batch means the run hit its stop
    /// limit before dispatching, so even if a crash lost the trailing `RunFinished`,
    /// replay must FINISH the lineage here, never re-run the model past the limit.
    /// `None` for any normal batch (a real `Completed`/`Unknown` terminal, or an
    /// incomplete batch) — those resume as usual.
    fn hard_stop_reason(&self) -> Option<RunFinishReason> {
        if self.planned.is_empty() {
            return None;
        }
        let mut saw_max_steps = false;
        for terminal in &self.terminals {
            match terminal {
                Some(Terminal::Result(_, DispatchOutcome::MaxSteps, _)) => saw_max_steps = true,
                Some(Terminal::Result(_, DispatchOutcome::Interrupted, _)) => {}
                // Any real/ambiguous/missing terminal → not a hard-stop batch.
                _ => return None,
            }
        }
        // The reason mirrors how the loop classifies the stop: a step-count limit →
        // MaxSteps; any other interrupting stop → Completed.
        Some(if saw_max_steps {
            RunFinishReason::MaxSteps
        } else {
            RunFinishReason::Completed
        })
    }
}

/// The per-episode transient state — everything that resets when a new episode
/// (`RunStarted`) opens. Only the lineage-cumulative `prompt` lives outside this.
#[derive(Default)]
struct Episode {
    recovery_interrupted: bool,
    attributable: bool,
    seeded_last_ordinal: Option<u64>,
    /// The index among the replayed records of the step record holding the open tool turn.
    open_step_record: Option<usize>,
    /// Provider-free SeededFrom episodes are imports/forks, not resumable model runs.
    seeded_from: bool,
    /// The model that produced THIS episode's assistant turns — the episode's frozen
    /// `RunConfig.model`. Stamped onto each assistant `Message` this episode commits, so a
    /// later episode under a DIFFERENT model can detect the cross-model boundary at render
    /// (and strip a signature the new model can't echo). Reproduces byte-identically on
    /// resume because it comes from the durable per-episode config.
    origin: Option<broca_protocol::request::ModelHandle>,
    /// Whether a provider call began in THIS episode. Unlike `completed_steps`, this also
    /// catches an auth failure during the first attempted call.
    provider_call_started: bool,
    /// Prompt index of the first message a model step of THIS episode committed; the start of
    /// this episode's entry in `ReplayState::episode_step_spans`.
    first_step_message: Option<usize>,
    /// Durable model steps in THIS episode (each billed). Resets per episode.
    completed_steps: u64,
    /// Whether this episode has already replaced its pre-call render config.
    config_refrozen: bool,
    /// Usage accumulated across THIS episode's steps. Resets per episode.
    usage: Usage,
    /// Provider retries spent across THIS episode's steps. Resets per episode.
    retries_used: u32,
    /// The LAST completed model step in THIS episode. Resets per episode.
    last_step_usage: Option<Usage>,
    /// The provider finish reason recorded on that step. Resets per episode.
    last_step_finish_reason: Option<FinishReason>,
    /// This episode's durable terminal, if reached. A new `RunStarted` may only open
    /// over an episode whose `finished.is_some()`.
    finished: Option<RunFinishReason>,
    /// This episode's non-terminal pause (auth-required), if any. Carries the record's
    /// own run_id so the resolution is self-contained.
    paused: Option<(RunId, PauseReason, Option<ProviderError>)>,
    account_limit_pause: Option<Box<broca_wal::record::AccountLimitPause>>,
    /// The trailing tool-turn (assistant w/ tool calls) + its batch, held until the
    /// batch completes (then committed to the prompt).
    open_assistant: Option<Arc<AssistantMessage>>,
    /// The runner tags of `open_assistant`, from its `ModelStepFinished`.
    open_assistant_tags: Option<broca_protocol::tags::MessageTags>,
    /// The model that answered `open_assistant` when it was not the episode's own (a step
    /// routed to the refusal-fallback model).
    open_assistant_origin: Option<broca_protocol::request::ModelHandle>,
    /// Steps of THIS episode a `model_step_refused` re-sent; a second refusal of one is corrupt.
    refused_steps: std::collections::BTreeSet<StepId>,
    open_batch: Option<OpenBatch>,
    open_step: Option<StepId>,
    open_batch_id: Option<BatchId>,
    /// True iff the last model step was a terminal turn (assistant with NO tool
    /// calls): such a step ends the loop, so a missing `run_finished`
    /// after it means the episode ended naturally and resume must NOT re-run the model.
    natural_end: bool,
    /// Stop reason evaluated from the durable terminal assistant message and frozen policy.
    /// Kept only for natural-end recovery; other terminal classifications take precedence.
    natural_stop_reason: Option<RunFinishReason>,
    /// Ambiguous calls (an `Unknown` terminal, or intent/recovery attribution without one). Stays
    /// set even after the batch commits, so it survives `batch_finished`.
    indeterminate: Vec<ToolCallId>,
    /// Set when a committed batch was a HARD-STOP repair (all synthetic stop terminals).
    /// Such a turn means the run hit its stop limit, so the lineage finishes here even if a
    /// crash lost the trailing `RunFinished` — analogous to `natural_end` above (which marks
    /// a no-tool terminal turn as a clean finish without a durable `RunFinished`).
    hard_stopped: Option<RunFinishReason>,
    /// Steered messages written in this episode and not yet placed in the prompt. They are
    /// placed at the next step boundary: the next `StepStarted`, or the episode's
    /// `RunFinished`. See [`WalRecord::Steered`].
    pending_steers: Vec<Message>,
    /// Whether the last no-tool step could have been extended by a steer: it was not a budget
    /// finalize turn and no stop condition ended the loop on it. The live loop runs one more
    /// step after such a step when a steer is pending, so a steer pending here means the
    /// episode did not end at that step.
    natural_end_extendable: bool,
}

impl Episode {
    /// Place every pending steered message at the end of `prompt`: the step boundary the
    /// replay has just reached.
    fn place_steers(&mut self, prompt: &mut Vec<Message>, actions: &[PlacementAction]) {
        for action in actions {
            if let PlacementAction::PlaceHeld { count, .. } = *action {
                prompt.extend(self.pending_steers.drain(..count as usize));
            }
        }
    }
}

/// Replay a session's lineage WAL (a sequence of episodes, in seq order) into a
/// [`ReplayState`] describing the LAST episode + the cumulative conversation prefix.
///
/// Each `RunStarted` closes the previous episode (validating it reached a durable
/// terminal, then resetting all per-episode transient state) and opens a new one; only
/// the last episode is a resume/continue target. Fails with [`ReplayError`] on a
/// corrupt lineage (a new episode opened over an unfinished prior one).
pub fn replay(records: &[WalRecord]) -> Result<ReplayState, ReplayError> {
    let mode3 = records
        .iter()
        .any(|record| matches!(record, WalRecord::RunStarted { .. }))
        .then(|| crate::is_mode3_log(records));
    replay_seeded(
        records,
        None,
        None,
        None,
        None,
        0,
        false,
        TranscriptSeed {
            tags: crate::tags::TagSeed::default(),
            mode3,
        },
    )
}

/// Persisted state needed to project a WAL suffix without replaying the materialized prefix.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionSeed {
    pub refusal: crate::refusal_fallback::RefusalReplayState,
    /// First model-produced message of the episode open at the watermark, if it has one.
    pub open_episode_span: Option<u64>,
    /// The episode open at the watermark, as the projection holds it, and every earlier
    /// episode's render inputs.
    pub open_episode: Option<crate::context::EpisodeRenderProjection>,
    pub earlier_episodes: Vec<crate::context::EpisodeRenderProjection>,
    /// Every episode the projection holds, when it computed them and its reader may reuse any.
    /// `None` while they are absent or superseded, which is when a projection refills them.
    pub projection_episodes: Option<Vec<crate::context::EpisodeRenderProjection>>,
    pub watermark_seq: u64,
    pub prefix_len: u64,
    pub completed_steps: u64,
    pub usage: Usage,
    pub retries_used: u32,
    pub last_step_usage: Option<Usage>,
    pub last_step_finish_reason: Option<FinishReason>,
    pub trailing_origin: Option<broca_protocol::request::ModelHandle>,
    pub last_run_id: Option<RunId>,
    pub admission_checkpoint: crate::AdmissionCheckpointV1,
    /// Approval metadata for the materialized prefix, including answered calls and late work.
    pub deferred_calls: Option<DeferredCallProjection>,
    /// Where tag numbering continues after the materialized prefix.
    pub tag_seed: crate::tags::TagSeed,
}

impl ProjectionSeed {}

/// The store-ready result of folding records after a [`ProjectionSeed`].
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionDelta {
    pub refusal: crate::refusal_fallback::RefusalReplayState,
    pub episode_spans: Vec<std::ops::Range<u64>>,
    pub open_episode_span: Option<u64>,
    /// Every episode's render inputs after the tail, in lineage order; `None` while the seed's
    /// projection holds none (see [`ProjectionSeed::projection_episodes`]).
    pub episodes: Option<Vec<crate::context::EpisodeRenderProjection>>,
    pub messages: Vec<Message>,
    pub message_runs: Vec<Option<RunAttribution>>,
    pub run_finals: BTreeMap<u64, RunAttribution>,
    /// Replace a materialized suffix before appending `messages`.
    pub truncate_prefix_to: Option<u64>,
    pub completed_steps: u64,
    pub usage: Usage,
    pub retries_used: u32,
    pub last_step_usage: Option<Usage>,
    pub last_step_finish_reason: Option<FinishReason>,
    pub last_run_id: Option<RunId>,
    pub admission_checkpoint: crate::AdmissionCheckpointV1,
    /// Approval metadata for the materialized prefix, including answered calls and late work.
    pub deferred_calls: Option<DeferredCallProjection>,
}

/// The `CorruptLineage` reason replay gives when a tool batch or tool-calling step would
/// replace an uncommitted batch that holds call evidence. Read-only scanners match on it to
/// tell this refusal from other corrupt lineages.
pub const OVERLAPPING_BATCH_REASON: &str = "a tool batch opened over an uncommitted batch that \
     already holds dispatch intents, Prepared calls or results";

/// A tool batch or tool-calling step that would replace an open batch holding call evidence.
fn overlapping_batch_error(run_id: Option<&RunId>) -> ReplayError {
    ReplayError::CorruptLineage {
        episode_run_id: run_id.cloned().unwrap_or_else(|| RunId("unknown".into())),
        reason: OVERLAPPING_BATCH_REASON.into(),
    }
}

/// Replay records that may BEGIN mid-episode (a store-merge WAL tail), seeding the
/// initial episode's producing-model origin from `seed_origin`.
///
/// A full-WAL replay starts at a `RunStarted` (which sets the episode origin), but the
/// store-merge fast path replays only the WAL TAIL past the materialization watermark —
/// and once the watermark advances, the tail no longer includes the episode's
/// `RunStarted`. So the tail's leading assistant turns belong to an episode whose model
/// identity lives in the already-materialized prefix. `seed_origin` carries that model
/// (the prefix's trailing assistant origin) so those turns are stamped identically to a
/// full replay; a `RunStarted` appearing later in the tail overrides it for its episode.
/// Passing `None` is the full-replay case (the first `RunStarted` sets the origin).
///
/// Steers held at the indexed sequence are seeded through the validated boundary
/// entry point [`replay_seeded_at_validated_boundary`], which checks the stored
/// boundary identity before replaying. This entry point carries no held steers, so
/// existing prefix-merge callers keep their behaviour.
#[allow(clippy::too_many_arguments)]
pub(crate) fn replay_seeded<'a>(
    records: impl IntoIterator<Item = &'a WalRecord>,
    seed_origin: Option<broca_protocol::request::ModelHandle>,
    seed_run_id: Option<RunId>,
    seed_budget: Option<BudgetConfig>,
    seed_budget_state: Option<crate::BudgetReplayStateV1>,
    ordinal_base: u64,
    require_caller_text: bool,
    tag_seed: TranscriptSeed,
) -> Result<ReplayState, ReplayError> {
    replay_seeded_with_refusal(
        records,
        seed_origin,
        seed_run_id,
        seed_budget,
        seed_budget_state,
        ordinal_base,
        require_caller_text,
        tag_seed,
        Default::default(),
    )
}

/// Continue refusal state from a materialized boundary before applying the tail's admissions.
#[allow(clippy::too_many_arguments)]
pub(crate) fn replay_seeded_with_refusal<'a>(
    records: impl IntoIterator<Item = &'a WalRecord>,
    seed_origin: Option<broca_protocol::request::ModelHandle>,
    seed_run_id: Option<RunId>,
    seed_budget: Option<BudgetConfig>,
    seed_budget_state: Option<crate::BudgetReplayStateV1>,
    ordinal_base: u64,
    require_caller_text: bool,
    tag_seed: TranscriptSeed,
    refusal: crate::refusal_fallback::RefusalReplayState,
) -> Result<ReplayState, ReplayError> {
    replay_seeded_with_compaction(
        records,
        seed_origin,
        seed_run_id,
        seed_budget,
        seed_budget_state,
        (ordinal_base, require_caller_text),
        None,
        tag_seed,
        Vec::new(),
        refusal,
    )
}

#[allow(clippy::too_many_arguments)]
fn replay_seeded_with_compaction<'a>(
    records: impl IntoIterator<Item = &'a WalRecord>,
    seed_origin: Option<broca_protocol::request::ModelHandle>,
    seed_run_id: Option<RunId>,
    seed_budget: Option<BudgetConfig>,
    seed_budget_state: Option<crate::BudgetReplayStateV1>,
    position: (u64, bool),
    seed_compaction: Option<&crate::compaction::CompactionCheckpoint>,
    tag_seed: TranscriptSeed,
    held_steers: Vec<Message>,
    refusal: crate::refusal_fallback::RefusalReplayState,
) -> Result<ReplayState, ReplayError> {
    let (ordinal_base, require_caller_text) = position;
    let mut placement = Placement::seeded(
        ordinal_base,
        seed_run_id.clone(),
        seed_budget.clone(),
        seed_budget_state.clone().unwrap_or_default(),
        0,
    );
    // Held steers recorded before the tail are pending input of the episode open at
    // the cut. Replay them through placement as synthetic steers so the tail's
    // first step boundary places held and new steers together, exactly as a full
    // replay would. A `RunStarted` in the tail opens a new episode and clears
    // them, matching the index which drops held steers when a new run starts.
    // Synthetic records carry only the input length placement reads; no budget,
    // delivery or tag state is synthesized.
    for held in &held_steers {
        let synthetic = WalRecord::Steered {
            message_marks: None,
            sent_by: None,
            written_at_ms: None,
            input: vec![held.clone()].into(),
            tags: None,
            retention: None,
            ts_ms: None,
            title: None,
            send_id: None,
            request_identity_digest: None,
            delivery: None,
            hooks: None,
        };
        placement
            .observe(&synthetic)
            .map_err(|reason| ReplayError::CorruptLineage {
                episode_run_id: seed_run_id.clone().unwrap_or_else(|| RunId(String::new())),
                reason,
            })?;
    }
    // Tag numbers under the computed rules, given in the order the transcript renders: each
    // message as it is placed in the prompt, a run's rules from its `RunStarted` on.
    let mode3 = tag_seed.mode3;
    let mut numbering = crate::tags::TagNumbering::new(tag_seed.tags, 0);
    // Lineage-cumulative: the conversation prefix across ALL episodes. The ONLY state
    // that survives an episode boundary.
    let mut prompt: Vec<Message> = Vec::new();
    let mut message_runs: Vec<Option<RunAttribution>> = Vec::new();
    let mut ordered_submissions = std::collections::HashSet::new();
    let mut run_finals = std::collections::BTreeMap::new();
    let mut compaction = seed_compaction
        .cloned()
        .unwrap_or(crate::compaction::CompactionCheckpoint::None);
    let mut compaction_history = compaction
        .state()
        .map(|state| state.history.clone())
        .unwrap_or_default();
    let mut compaction_session = false;
    // Last-episode-wins: overwritten on each `RunStarted`, so they hold the LAST
    // episode's values at end of walk.
    let mut run_config: Option<RunConfig> = None;
    let mut keep_warm = None;
    let mut on_restart = None;
    let mut marker_order_version = None;
    let mut run_id = seed_run_id;
    let mut cancel_at = None;
    let mut seeded_budget = seed_budget;
    // Known only for a walk from the lineage's first record: a tail doesn't see the tool sets of
    // the runs before it.
    let mut signed_floor = if ordinal_base == 0 && run_id.is_none() {
        crate::signed_floor::FloorFold::Known(Default::default())
    } else {
        crate::signed_floor::FloorFold::Unknown
    };
    // A tail can begin after the current episode's RunStarted. Only a RunStarted observed in
    // this walk may require a durable terminal before another episode opens.
    let mut saw_run_started = false;
    // Session-scoped, unlike the episode state below: a schedule carries across episodes of
    // the same source model.
    let mut refusal_fallback = refusal.schedule;
    // The current episode's transient state — reset on each new `RunStarted`. Seed the origin
    // and durable budget facts for a tail whose RunStarted is in the materialized prefix.
    let mut seeded_nudge_indices = seed_budget_state
        .as_ref()
        .map_or_else(Vec::new, |state| state.fired_nudge_indices.clone());
    let mut ep = Episode {
        attributable: run_id.as_ref().is_some_and(|id| !id.0.is_empty()),
        seeded_last_ordinal: ordinal_base.checked_sub(1),
        origin: seed_origin,
        pending_steers: held_steers,
        refused_steps: refusal.refused_steps,
        ..Episode::default()
    };
    let mut recovered_calls: Vec<broca_records::recovery::RecoveryRun> = Vec::new();
    let mut episode_step_spans: Vec<std::ops::Range<usize>> = Vec::new();
    let mut render_episodes = Vec::new();
    let mut committed_tool_turns: Vec<(usize, usize)> = Vec::new();

    let mut input_budget = crate::input_budget::InputTokenBudget::default();
    let mut output_reservation = crate::output_reservation::OutputReservationHistory::default();
    for (record_index, rec) in records.into_iter().enumerate() {
        output_reservation.fold(rec);
        input_budget
            .fold(rec)
            .map_err(|reason| ReplayError::CorruptLineage {
                episode_run_id: run_id.clone().unwrap_or_else(|| RunId(String::new())),
                reason,
            })?;
        if let WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } = rec
        {
            compaction_session |= crate::compaction::has_compaction(manifest);
        }
        if let WalRecord::RunStarted { config, .. } = rec {
            numbering
                .number_new(&mut prompt)
                .map_err(ReplayError::TagNumbering)?;
            numbering.start_run(config);
        }
        let extent_before = placement.next_ordinal();
        compaction.fold_at(rec, extent_before);
        if compaction_session || compaction.state().is_some() {
            compaction_history.fold(rec);
            if let crate::compaction::CompactionCheckpoint::State(state) = &mut compaction {
                state.history = compaction_history.clone();
            }
        }
        let expected_budget_text = matches!(rec, WalRecord::BudgetFired { .. })
            .then(|| {
                run_config
                    .as_ref()
                    .and_then(|config| config.budget.as_ref())
                    .or(seeded_budget.as_ref())
            })
            .flatten()
            .and_then(|budget| crate::budget_kernel::next_due(placement.budget(), budget))
            .map(|due| due.text);
        let actions = placement
            .observe(rec)
            .map_err(|reason| ReplayError::CorruptLineage {
                episode_run_id: run_id.clone().unwrap_or_else(|| RunId(String::new())),
                reason,
            })?;
        match rec {
            WalRecord::RunStarted {
                on_restart: on_restart_policy,
                keep_warm: policy,
                run_id: rid,
                marker_order_version: version,
                config,
                input,
                origin,
                submission_id,
                ts_ms: admitted_at_ms,
                // `..` deliberately covers `manifest`: replay decodes a frozen plan manifest but
                // does not act on it, and renders from the episode's own frozen config and input,
                // the same as for an episode without one.
                ..
            } => {
                // Closing the previous episode (if this isn't the first `RunStarted`
                // SEEN IN THIS WALK): it MUST have reached a durable terminal. The
                // lease + one-active-run-per-lineage invariant guarantee a new run only
                // starts after the prior episode wrote `RunFinished`; anything else (a
                // zero-step crash-then-restart, a natural end without `RunFinished`, a
                // `RunStarted` directly after a `RunPaused`) is a corrupt lineage —
                // never fabricate a prefix from it.
                //
                // The check keys on a prior RunStarted seen IN THIS WALK (`run_id`),
                // not on `ep.finished`, so a tail-replay (store-merge path) that begins
                // with an ORPHAN `RunFinished` — the prior, already-materialized
                // episode's terminal, with its `RunStarted` in the prefix — does NOT
                // trip the guard.
                if saw_run_started && ep.finished.is_none() {
                    return Err(ReplayError::CorruptLineage {
                        episode_run_id: run_id.clone().unwrap_or_else(|| rid.clone()),
                        reason: "a new RunStarted opened before the previous episode \
                                 reached a durable RunFinished"
                            .into(),
                    });
                }
                // The closing episode's model-step history ends here, before this
                // episode's input is appended.
                if let Some(start) = ep.first_step_message {
                    episode_step_spans.push(start..prompt.len());
                }
                // Reset every per-episode transient on the new episode; KEEP the
                // cumulative prompt. Unconditional (not only when a prior RunStarted
                // was seen) so a tail-replay's orphan leading `RunFinished` is
                // discarded here rather than leaking its `finished`/state into this
                // episode.
                ep = Episode::default();
                seeded_nudge_indices.clear();
                ep.seeded_from = matches!(origin, broca_wal::RunOrigin::SeededFrom { .. });
                // Import/fork inputs describe historical messages, not messages produced by the
                // synthetic seed run. A missing or empty producer id is never invented.
                ep.attributable = !ep.seeded_from && !rid.0.is_empty();
                // Stamp this episode's assistant turns with the model that produced them,
                // so a later episode under a different model can strip a cross-model
                // signature at render. From the durable per-episode config → reproduces on
                // resume.
                ep.origin = Some(config.model.clone());
                refusal_fallback = crate::refusal_fallback::on_episode_admitted(
                    refusal_fallback.take(),
                    &config.model,
                    *admitted_at_ms,
                );
                marker_order_version = *version;
                keep_warm = policy.clone();
                on_restart = *on_restart_policy;
                run_config = Some((**config).clone());
                run_id = Some(rid.clone());
                cancel_at = None;
                seeded_budget = None;
                saw_run_started = true;
                render_episodes.push(broca_protocol::request::EpisodeRenderInput {
                    prompt_start: extent_before as usize,
                    first_user: input
                        .iter()
                        .position(|m| m.role == Role::User)
                        .map(|i| extent_before as usize + i),
                    frozen_config: config.frozen_render_config.clone(),
                });
                // A seeded run (an import's) never sends a request, so its tools carry no
                // reasoning and can't invalidate any.
                if !matches!(origin, broca_wal::RunOrigin::SeededFrom { .. }) {
                    signed_floor.observe_run(
                        extent_before,
                        crate::signed_floor::tool_set_digest(
                            &config.tools,
                            &config.frozen_render_config,
                        ),
                    );
                }
                let input_tags = rec.runner_tags();
                let idle_delivery = submission_id
                    .as_ref()
                    .is_some_and(|id| ordered_submissions.remove(id));
                let count = actions
                    .iter()
                    .find_map(|action| match action {
                        PlacementAction::Append {
                            count,
                            kind: AppendKind::Input,
                            ..
                        } => Some(*count),
                        _ => None,
                    })
                    .expect("RunStarted places input");
                prompt.extend(input.iter().take(count as usize).enumerate().map(
                    |(index, message)| {
                        let mut message = tagged(message.clone(), input_tags.get(index));
                        if idle_delivery {
                            message
                                .tags
                                .set_delivery(broca_protocol::tags::DeliveryAttribution {
                                    mode: broca_protocol::DeliveryMode::IdleStart,
                                    tool_call_id: None,
                                });
                        }
                        message
                    },
                ));
            }
            WalRecord::ModelStepFinished {
                step_id,
                assistant_message,
                finish_reason,
                retries_used,
                tags: step_tags,
                fallback,
                ..
            } => {
                let step_tags = step_tags.as_deref();
                // A step routed to the refusal-fallback model is that model's message.
                let step_origin = crate::refusal_fallback::step_origin(fallback)
                    .cloned()
                    .or_else(|| ep.origin.clone());
                if let Some(schedule) = &fallback.refusal_fallback {
                    refusal_fallback = Some(schedule.clone());
                }
                let usage = crate::refusal_fallback::total_step_usage(rec).unwrap();
                ep.usage.accumulate(&usage);
                ep.retries_used = ep.retries_used.saturating_add(*retries_used);
                ep.last_step_usage = Some(usage);
                ep.last_step_finish_reason = *finish_reason;
                ep.completed_steps = placement.completed_steps();
                ep.natural_stop_reason = None;
                let was_finalize_turn = actions.iter().any(|action| {
                    matches!(
                        action,
                        PlacementAction::Append {
                            kind: AppendKind::Finalize,
                            ..
                        }
                    )
                });
                if was_finalize_turn {
                    // A finalize turn is terminal even if a provider emits undeclared calls.
                    // Preserve its answer text in future history, but omit tool calls because
                    // the tool-disabled turn never dispatches them and therefore has no results.
                    ep.first_step_message.get_or_insert(prompt.len());
                    // The finalize turn's tool calls are never dispatched, so neither the
                    // transcript nor any request holds them. The kept blocks keep their own
                    // tags. The dropped calls' tags stay assigned in the WAL record and are
                    // never reused, but no transcript part holds them, so no read or
                    // `session.resolve` answers them.
                    let kept_tags = step_tags.map(|tags| {
                        broca_protocol::tags::MessageTags::new(
                            assistant_message
                                .content
                                .iter()
                                .zip(&tags.parts)
                                .filter(|(block, _)| !matches!(block, ContentBlock::ToolCall(_)))
                                .map(|(_, tag)| *tag)
                                .collect(),
                        )
                    });
                    prompt.push(tagged(
                        Message {
                            turn_system: false,
                            cache_prefix_blocks: None,
                            role: Role::Assistant,
                            content: assistant_message
                                .content
                                .iter()
                                .filter(|block| !matches!(block, ContentBlock::ToolCall(_)))
                                .cloned()
                                .collect(),
                            origin: step_origin.clone(),
                            ordinal: None,
                            tags: Default::default(),
                        },
                        kept_tags.as_ref(),
                    ));
                    ep.open_assistant = None;
                    ep.open_batch = None;
                    ep.natural_end = true;
                    ep.natural_end_extendable = false;
                } else if actions.iter().any(|action| {
                    matches!(
                        action,
                        PlacementAction::Hold {
                            kind: HoldKind::Assistant,
                            ..
                        }
                    )
                }) {
                    // Each tool turn opens and commits its own batch. A batch still open from an
                    // earlier turn cannot belong to this one: fail closed if it holds call
                    // evidence, and otherwise drop it so this turn is never paired with another
                    // turn's planned calls.
                    if ep
                        .open_batch
                        .as_ref()
                        .is_some_and(OpenBatch::holds_call_evidence)
                    {
                        return Err(overlapping_batch_error(run_id.as_ref()));
                    }
                    ep.open_batch = None;
                    ep.open_assistant = Some(assistant_message.clone()); // tool turn — held
                    ep.open_assistant_tags = step_tags.cloned();
                    ep.open_assistant_origin = step_origin.clone();
                    ep.open_step = Some(*step_id);
                    ep.open_step_record = Some(record_index);
                    ep.open_batch_id = None; // a new tool turn; its batch hasn't opened yet
                    ep.natural_end = false;
                } else if actions.iter().any(|action| {
                    matches!(
                        action,
                        PlacementAction::Append {
                            kind: AppendKind::Assistant,
                            ..
                        }
                    )
                }) {
                    // Terminal turn (no tools) — commit immediately. This step ends
                    // the loop, so the episode ended naturally here.
                    ep.first_step_message.get_or_insert(prompt.len());
                    prompt.push(tagged(
                        Message {
                            turn_system: false,
                            cache_prefix_blocks: None,
                            role: Role::Assistant,
                            content: assistant_message.content.clone(),
                            origin: step_origin.clone(),
                            ordinal: None,
                            tags: Default::default(),
                        },
                        step_tags,
                    ));
                    ep.natural_end = true;
                    // Match live evaluation, using the original assistant message: a step that
                    // requests no tools ends the run `completed`, even when it is also the
                    // `max_steps` limit, because the limit cut nothing off. Budget-finalize turns
                    // above bypass stop_when in both paths.
                    ep.natural_stop_reason = run_config.as_ref().and_then(|config| {
                        crate::run::stop_satisfied(
                            &config.stop_when,
                            ep.completed_steps,
                            assistant_message,
                        )
                        .map(|_| RunFinishReason::Completed)
                    });
                    ep.natural_end_extendable = run_config.as_ref().is_none_or(|config| {
                        !crate::run::limiting_stop_satisfied(
                            &config.stop_when,
                            ep.completed_steps,
                            assistant_message,
                        )
                    });
                }
            }
            WalRecord::RecoveryCut { recovery } => {
                if recovery.active_run.is_some()
                    && recovery.active_run == run_id
                    && ep.finished.is_none()
                {
                    ep.recovery_interrupted = true;
                }
                for recovered in &recovery.runs {
                    recovered_calls.push(recovered.clone());
                    if Some(&recovered.run_id) != run_id.as_ref() {
                        continue;
                    }
                    for call in &recovered.calls {
                        if !ep.indeterminate.contains(&call.tool_call_id) {
                            ep.indeterminate.push(call.tool_call_id.clone());
                        }
                    }
                    if ep.open_batch.is_none() {
                        if let Some(assistant) = &ep.open_assistant {
                            let step = ep.open_step.unwrap_or(StepId(0));
                            ep.open_batch = Some(OpenBatch::new(
                                step,
                                assistant
                                    .tool_calls()
                                    .map(|c| c.tool_call_id.clone())
                                    .collect(),
                            ));
                            ep.open_batch.as_mut().expect("just created").started = false;
                            ep.open_batch_id =
                                Some(BatchId(format!("{}-b{}", recovered.run_id.0, step.0)));
                        }
                    }
                    if let Some(batch) = &mut ep.open_batch {
                        for call in &recovered.calls {
                            batch.recovered.insert(call.tool_call_id.clone());
                        }
                    }
                }
            }
            WalRecord::RecoveryCallClosed {
                run_id: owner,
                batch_id,
                step_id,
                call_key,
                tool_call_id,
                ..
            } => {
                recovered_calls.push(broca_records::recovery::RecoveryRun {
                    run_id: owner.clone(),
                    calls: vec![broca_records::recovery::RecoveryCall {
                        tool_call_id: tool_call_id.clone(),
                        step_id: *step_id,
                        batch_id: batch_id.clone(),
                        call_keys: call_key.iter().cloned().collect(),
                    }],
                });
                if run_id.as_ref() == Some(owner) && !ep.indeterminate.contains(tool_call_id) {
                    ep.indeterminate.push(tool_call_id.clone());
                }
            }
            WalRecord::ToolBatchStarted {
                batch_id,
                step_id,
                planned_calls,
            } => {
                // Every writer starts a batch at most once (a resume reuses the started batch).
                // A second start over an uncommitted batch that already holds call evidence
                // would erase it, so the lineage is refused instead of replayed.
                let inferred = ep.open_batch.as_ref().filter(|batch| {
                    !batch.started
                        && ep.open_batch_id.as_ref() == Some(batch_id)
                        && batch.step_id == *step_id
                        && batch.planned == *planned_calls
                        && !batch.terminals.iter().any(Option::is_some)
                });
                if ep
                    .open_batch
                    .as_ref()
                    .is_some_and(|batch| batch.holds_call_evidence() && inferred.is_none())
                {
                    return Err(overlapping_batch_error(run_id.as_ref()));
                }
                let mut batch = OpenBatch::new(*step_id, planned_calls.clone());
                if let Some(inferred) = inferred {
                    batch.recovered.extend(inferred.recovered.iter().cloned());
                }
                for recovered in &recovered_calls {
                    if Some(&recovered.run_id) == run_id.as_ref() {
                        for call in &recovered.calls {
                            let recovered_batch = call.batch_id.clone().unwrap_or_else(|| {
                                BatchId(format!("{}-recovery", recovered.run_id.0))
                            });
                            if &recovered_batch == batch_id {
                                batch.recovered.insert(call.tool_call_id.clone());
                            }
                        }
                    }
                }
                ep.open_batch = Some(batch);
                ep.open_batch_id = Some(batch_id.clone());
            }
            WalRecord::ToolDispatchIntent { tool_call_id, .. } => {
                if let Some(b) = &mut ep.open_batch {
                    b.intents.insert(tool_call_id.clone());
                }
            }
            WalRecord::ToolCallDeferred {
                batch_id, call_id, ..
            } => {
                if ep.open_batch_id.as_ref() == Some(batch_id) {
                    if let Some(b) = &mut ep.open_batch {
                        if let Some(position) = b.planned.iter().position(|id| id == call_id) {
                            // A surviving Prepared frame freezes the hooks' output once.
                            b.prepared[position].get_or_insert_with(|| PreparedCall {
                                record_index,
                                prepared: Arc::new(rec.clone()),
                                approval: None,
                                elicitation_id: None,
                            });
                        }
                    }
                }
            }
            WalRecord::ApprovalRecorded {
                call_id,
                decision,
                closing_result,
                elicitation_id,
                tags: closing_tags,
                ..
            } => {
                if let Some(b) = &mut ep.open_batch {
                    if let Some(position) = b.planned.iter().position(|id| id == call_id) {
                        if let Some(prepared) = &mut b.prepared[position] {
                            prepared.elicitation_id = elicitation_id.clone();
                            if let Some(result) = closing_result {
                                b.insert_terminal(
                                    call_id,
                                    Terminal::Result(
                                        result.clone(),
                                        DispatchOutcome::Completed,
                                        single_tag(closing_tags.as_deref()),
                                    ),
                                );
                            } else if *decision == DeferredDecision::Approved {
                                prepared.approval = Some(Arc::new(rec.clone()));
                            }
                        }
                    }
                }
            }
            WalRecord::AwaitingApproval { batch_id, pending } => {
                if ep.open_batch_id.as_ref() == Some(batch_id) {
                    if let Some(b) = &mut ep.open_batch {
                        for entry in pending {
                            if let Some(position) =
                                b.planned.iter().position(|id| *id == entry.call_id)
                            {
                                if let Some(prepared) = &mut b.prepared[position] {
                                    // The marker supplies display metadata only, never membership.
                                    prepared.elicitation_id = entry.elicitation_id.clone();
                                }
                            }
                        }
                    }
                }
            }
            WalRecord::ToolNoDispatch {
                tool_call_id,
                reason,
                tags,
                ..
            } => {
                if let Some(b) = &mut ep.open_batch {
                    b.insert_terminal(
                        tool_call_id,
                        Terminal::NoDispatch(reason.clone(), single_tag(tags.as_deref())),
                    );
                }
            }
            WalRecord::ToolResult {
                result,
                outcome,
                tags,
                ..
            } => {
                if let Some(b) = &mut ep.open_batch {
                    b.insert_terminal(
                        &result.tool_call_id,
                        Terminal::Result(result.clone(), *outcome, single_tag(tags.as_deref())),
                    );
                }
                // An Unknown terminal is ambiguous regardless of batch commit.
                if *outcome == DispatchOutcome::Unknown {
                    ep.indeterminate.push(result.tool_call_id.clone());
                }
            }
            WalRecord::ToolBatchFinished { .. } => {
                // Commit the tool turn IFF complete (every planned call terminated).
                if let Some(b) = &ep.open_batch {
                    if actions.iter().any(|action| {
                        matches!(
                            action,
                            PlacementAction::Append {
                                kind: AppendKind::ToolTurn,
                                ..
                            }
                        )
                    }) {
                        // A committed all-synthetic-stop batch means the run hit its stop
                        // limit before dispatching → finish the lineage here even if the
                        // trailing RunFinished was lost in a crash (else replay would see a
                        // committed turn with no terminal and wrongly re-run the model past
                        // the limit).
                        if let Some(reason) = b.hard_stop_reason() {
                            ep.hard_stopped = Some(reason);
                        }
                        if let Some(assistant) = ep.open_assistant.take() {
                            ep.first_step_message.get_or_insert(prompt.len());
                            if let Some(step_record) = ep.open_step_record.take() {
                                committed_tool_turns.push((prompt.len(), step_record));
                            }
                            commit_tool_turn(
                                &mut prompt,
                                &assistant,
                                ep.open_assistant_tags.take().as_ref(),
                                b,
                                ep.open_assistant_origin
                                    .take()
                                    .or_else(|| ep.origin.clone()),
                            );
                        }
                        ep.open_batch = None;
                        ep.open_step = None;
                        ep.open_batch_id = None;
                    }
                }
            }
            WalRecord::RunCancelRequested {
                run_id: cancelled_run,
                at,
            } => {
                if run_id.as_ref() == Some(cancelled_run)
                    && ep.finished.is_none()
                    && cancel_at != Some(broca_wal::record::RunCancelAt::Now)
                {
                    cancel_at = Some(*at);
                }
            }
            WalRecord::TurnFinished { .. } => {}
            // A message added to the running turn: held until the next step boundary, so it
            // never lands between a tool call and its result or ahead of the step it arrived in.
            WalRecord::Steered { input, .. } => {
                let steer_tags = rec.runner_tags();
                let count = actions
                    .iter()
                    .find_map(|action| match action {
                        PlacementAction::Hold {
                            count,
                            kind: HoldKind::Steer,
                        } => Some(*count),
                        _ => None,
                    })
                    .expect("Steered holds input");
                ep.pending_steers.extend(
                    input
                        .iter()
                        .take(count as usize)
                        .enumerate()
                        .map(|(index, message)| tagged(message.clone(), steer_tags.get(index))),
                );
            }
            // An ordered send delivered as a steer is placed exactly like a steer. One delivered
            // with a tool result renders that result followed by the recorded text; the record
            // follows the batch's `ToolBatchFinished`, so the result is in the newest tool
            // message. The stored `ToolResult` record itself is never changed.
            WalRecord::SendDelivered {
                submission_id,
                ride,
                input,
                tool_call_id,
                appended_text,
                mode,
                ..
            } => {
                ordered_submissions.remove(submission_id);
                if let Some(input) = input {
                    let delivered_tags = rec.runner_tags();
                    let count = actions
                        .iter()
                        .find_map(|action| match action {
                            PlacementAction::Hold {
                                count,
                                kind: HoldKind::Steer,
                            } => Some(*count),
                            _ => None,
                        })
                        .expect("SendDelivered holds its input");
                    ep.pending_steers
                        .extend(input.iter().take(count as usize).enumerate().map(
                            |(index, message)| {
                                let mut message =
                                    tagged(message.clone(), delivered_tags.get(index));
                                message.tags.set_delivery(
                                    broca_protocol::tags::DeliveryAttribution {
                                        mode: *mode,
                                        tool_call_id: None,
                                    },
                                );
                                message
                            },
                        ));
                }
                if let (Some(tool_call_id), Some(text)) = (tool_call_id, appended_text) {
                    if mode3 != Some(true) {
                        if let Some(ride) = ride {
                            crate::delivery_order::apply_ride_to_prompt(
                                &mut prompt,
                                tool_call_id,
                                ride,
                                text,
                            );
                        } else {
                            crate::delivery_order::apply_to_prompt(&mut prompt, tool_call_id, text);
                        }
                    }
                    // Envelope rides belong to the carrying call, not the transcript
                    // message. Only legacy deliveries retain entry attribution.
                    if let Some(index) = prompt.iter().rposition(|message| {
                        message.content.iter().any(|block| matches!(block,
                            ContentBlock::ToolResult(result) if &result.tool_call_id == tool_call_id))
                    }).filter(|_| ride.is_none()) {
                        let delivery = broca_protocol::tags::DeliveryAttribution {
                            mode: *mode, tool_call_id: Some(tool_call_id.0.clone()),
                        };
                        prompt[index].tags.set_delivery(delivery.clone());
                        if let Some(Some(run)) = message_runs.get_mut(index) {
                            run.delivery = Some(delivery);
                        }
                    }
                }
            }
            // The order and the end of a queued send belong to the queue, not the prompt.
            WalRecord::DeliveryOrdered { submission_id, .. } => {
                ordered_submissions.insert(submission_id.clone());
            }
            WalRecord::SendEnded { submission_id, .. } => {
                ordered_submissions.remove(submission_id);
            }
            WalRecord::BudgetFired {
                run_id: fired_run_id,
                caller_text,
                ..
            } => {
                if !actions.iter().any(|action| {
                    matches!(
                        action,
                        PlacementAction::Append {
                            kind: AppendKind::Budget,
                            ..
                        }
                    )
                }) {
                    // Read-only transcript assembly has no checkpoint budget seed. Preserve its
                    // established best-effort behavior; admission and projection fail closed.
                    continue;
                }
                let expected_text = expected_budget_text.expect("accepted firing was due");
                let text = match caller_text {
                    Some(text) if text == &expected_text => text.clone(),
                    Some(_) => {
                        return Err(ReplayError::CorruptLineage {
                            episode_run_id: fired_run_id.clone(),
                            reason: "BudgetFired caller text differs from the frozen budget config"
                                .into(),
                        });
                    }
                    None if require_caller_text => {
                        return Err(ReplayError::CorruptLineage {
                            episode_run_id: fired_run_id.clone(),
                            reason: "BudgetFired is missing fire-time caller text".into(),
                        });
                    }
                    None => expected_text,
                };
                prompt.push(tagged(
                    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }]),
                    rec.runner_tags().first(),
                ));
            }
            WalRecord::RunFinished {
                reason,
                retries_used,
                ..
            } => {
                for action in &actions {
                    if let PlacementAction::Truncate { to } = action {
                        prompt.truncate(to.saturating_sub(ordinal_base) as usize);
                    }
                }
                // A steer no later step rendered stays in the transcript exactly once, at the
                // episode's close, after anything the terminal's barrier committed.
                ep.place_steers(&mut prompt, &actions);
                ep.finished = Some(*reason);
                ep.retries_used = *retries_used;
                // A terminal SEALS any earlier pause in this episode (e.g. a cancel of a
                // paused-for-auth run writes RunPaused then RunFinished{Interrupted}) —
                // last-writer-wins, so the run is terminal, not still paused.
                ep.paused = None;
                ep.account_limit_pause = None;
            }
            // A non-terminal pause (auth-required) — distinct from a terminal
            // RunFinished. Clears any earlier finished in this episode (last-writer-wins),
            // though in practice a pause is the last record of its episode.
            WalRecord::RunPaused {
                run_id: rid,
                reason,
                error,
                account_limit,
                ..
            } => {
                ep.paused = Some((rid.clone(), *reason, error.clone()));
                ep.account_limit_pause = account_limit.clone();
                ep.finished = None;
            }
            WalRecord::RunConfigRefrozen {
                renderer_version,
                frozen_render_config,
                ..
            } => {
                if let Some(config) = run_config.as_mut() {
                    let episode_run_id = run_id.clone().unwrap_or_else(|| RunId("unknown".into()));
                    if ep.provider_call_started {
                        return Err(ReplayError::CorruptLineage {
                            episode_run_id,
                            reason: "RunConfigRefrozen occurred after a provider call started"
                                .into(),
                        });
                    }
                    if ep.config_refrozen {
                        return Err(ReplayError::CorruptLineage {
                            episode_run_id,
                            reason: "RunConfigRefrozen occurred more than once in one episode"
                                .into(),
                        });
                    }
                    config.renderer_version = *renderer_version;
                    config.frozen_render_config = frozen_render_config.clone();
                    ep.config_refrozen = true;
                }
            }
            WalRecord::StructuredOutputDowngraded {
                run_id: downgraded_run,
            } => {
                if run_id.as_ref() != Some(downgraded_run) {
                    return Err(ReplayError::CorruptLineage {
                        episode_run_id: downgraded_run.clone(),
                        reason: "structured-output downgrade names a different episode".into(),
                    });
                }
                if let Some(config) = &mut run_config {
                    broca_protocol::structured_output::FrozenStructuredOutput::downgrade_render_config(&mut config.frozen_render_config);
                }
            }
            WalRecord::RunAuthRefrozen { auth_selection, .. } => {
                if let Some(config) = run_config.as_mut() {
                    config.auth_selection = Some(auth_selection.clone());
                }
            }
            // The step boundary proves continuation after a pause and that the provider was
            // attempted, even when the call failed before ModelStepFinished became durable.
            WalRecord::StepStarted { .. } => {
                ep.paused = None;
                ep.account_limit_pause = None;
                ep.provider_call_started = true;
                // Pending steers are rendered by this step, after everything before it.
                ep.place_steers(&mut prompt, &actions);
                // A step after a no-tool step means that step did not end the episode: the live
                // loop continued to render a steer.
                ep.natural_end = false;
                ep.natural_end_extendable = false;
            }
            // A failed queued admission projects as a synthetic episode with no model step.
            // Mirror that state reset even though queue records do not affect the prompt.
            WalRecord::Retracted {
                failure: Some(_), ..
            } => {
                ep.last_step_finish_reason = None;
            }
            // Durable submission-queue + approval records don't contribute to the
            // reconstructed prompt.
            WalRecord::Queued { .. }
            | WalRecord::Retracted { failure: None, .. }
            | WalRecord::ApprovalRequested { .. }
            | WalRecord::ApprovalResolved { .. } => {}
            // A record type written by a newer build. By the contract on `WalRecord`, such a
            // record is informational only, so it contributes nothing to the prompt or the
            // episode state: replay is identical to the same WAL without it.
            WalRecord::WriterOpened { .. }
            | WalRecord::WriterClosed { .. }
            | WalRecord::VisibilityLate { .. }
            | WalRecord::CompactionViewV2 { .. }
            | WalRecord::CompactionMarker { .. }
            | WalRecord::CompactionViewAppliedV2 { .. }
            | WalRecord::Unknown => {}
            // Compaction is folded separately above; the transcript remains append-only.
            WalRecord::CompactionRequest { .. }
            | WalRecord::CompactionMessage { .. }
            | WalRecord::CompactionAnswer { .. }
            | WalRecord::CompactionWait { .. }
            | WalRecord::CompactionViewApplied { .. }
            | WalRecord::ContextCeilingWait { .. }
            | WalRecord::ContextCeilingDecided { .. }
            | WalRecord::OutputReservationClamped { .. }
            | WalRecord::CompactionRideWait { .. }
            | WalRecord::CompactionRideWaitEnded { .. }
            | WalRecord::CompactionNotice { .. } => {}
            // Provider attempt evidence: known to this build, but informational only under the
            // same contract, so replay skips it exactly as it skips `Unknown`. The step's retry
            // count still comes from `ModelStepFinished.retries_used`.
            WalRecord::ModelAttemptFinished { .. } => {}
            WalRecord::AttemptReserved { .. }
            | WalRecord::AttemptFinished { .. }
            | WalRecord::AttemptReleased { .. }
            | WalRecord::AttemptCorrected { .. } => {}
            // A refused step contributes nothing to the prompt: its content was never stored,
            // and the same step id commits the fallback model's answer. Its usage was billed, so
            // it counts toward the episode's totals, and it records the schedule it armed.
            WalRecord::ModelStepRefused {
                step_id, schedule, ..
            } => {
                if !ep.refused_steps.insert(*step_id) {
                    return Err(ReplayError::CorruptLineage {
                        episode_run_id: run_id.clone().unwrap_or_else(|| RunId(String::new())),
                        reason: format!(
                            "a second model_step_refused for step {}: a refused step is re-sent \
                             once, to the fallback model",
                            step_id.0
                        ),
                    });
                }
                ep.usage
                    .accumulate(&crate::refusal_fallback::total_step_usage(rec).unwrap());
                refusal_fallback = Some(schedule.clone());
            }
            // A resume's own account of what it found. Informational under the same contract:
            // it never enters the prompt, and replay is identical to the same WAL without it.
            WalRecord::ResumeStarted { .. } => {}
            // A cache warm re-sent an earlier request and added nothing to the conversation:
            // informational, so replay is identical to the same WAL without it.
            WalRecord::CacheWarmed { .. } => {}
            // Late answers belong to session reads and isolated execution, not conversation history.
            WalRecord::LateApprovalRecorded { .. }
            | WalRecord::LateDispatchIntent { .. }
            | WalRecord::LateResult { .. } => {}
            // A held call stays open until its late result is written as its terminal; replay
            // keeps it out of the indeterminate set so recovery never seals or re-sends it.
            WalRecord::ToolCallProviderHeld {
                tool_call_id,
                call_key,
                module,
                reason,
                detail_digest,
                held_at_ms,
                sequential,
                ..
            } => {
                if let Some(batch) = &mut ep.open_batch {
                    batch.provider_held.insert(
                        tool_call_id.clone(),
                        ProviderHeldCall {
                            tool_call_id: tool_call_id.clone(),
                            call_key: call_key.clone(),
                            module: module.clone(),
                            reason: reason.clone(),
                            detail_digest: detail_digest.clone(),
                            held_at_ms: *held_at_ms,
                            sequential: *sequential,
                        },
                    );
                }
            }
            // These metadata records are omitted from prompt construction: they contain no messages.
            WalRecord::InputTokenCapFrozen { .. }
            | WalRecord::InputTokenBudgetExhausted { .. }
            | WalRecord::StructuredOutputResult { .. }
            | WalRecord::QuestionWithdrawalAcknowledged { .. } => {}
            // A mid-session change is folded by the session-change projection, not by replay:
            // it contributes no message to the prompt, and earlier episodes keep resuming from
            // their own manifests, byte-identical.
            WalRecord::SessionRefreshAccepted { .. }
            | WalRecord::SessionRefreshPolicyChanged { .. }
            | WalRecord::SessionPrefixFlushed { .. } => {}
        }
        // A model-facing ID may be reused by another run. Only the owning run's
        // recovered call can turn NotSent into an unknown dispatch here.
        if matches!(
            rec,
            WalRecord::ModelStepFinished { .. } | WalRecord::ToolBatchStarted { .. }
        ) {
            if let (Some(assistant), Some(current_run)) = (&ep.open_assistant, &run_id) {
                let planned: Vec<_> = assistant
                    .tool_calls()
                    .map(|call| call.tool_call_id.clone())
                    .collect();
                let recovered: Vec<_> = planned
                    .iter()
                    .filter(|id| {
                        recovered_calls
                            .iter()
                            .filter(|run| &run.run_id == current_run)
                            .flat_map(|run| &run.calls)
                            .any(|call| &call.tool_call_id == *id)
                    })
                    .cloned()
                    .collect();
                if !recovered.is_empty() {
                    let step = ep.open_step.unwrap_or(StepId(0));
                    let batch = ep
                        .open_batch
                        .get_or_insert_with(|| OpenBatch::new(step, planned));
                    batch.recovered.extend(recovered);
                    ep.open_batch_id
                        .get_or_insert_with(|| BatchId(format!("{}-b{}", current_run.0, step.0)));
                }
            }
        }
        // Placement is the authority for transcript positions, including a seeded prefix and
        // steers held until a boundary. Assign its ordinals as each record places its messages.
        let placed = actions
            .iter()
            .map(|action| match action {
                PlacementAction::Append { count, .. }
                | PlacementAction::PlaceHeld { count, .. } => *count as usize,
                _ => 0,
            })
            .sum::<usize>();
        let placed_start = prompt.len() - placed;
        let mut newly_placed = prompt[placed_start..].iter_mut();
        for action in &actions {
            if let PlacementAction::Append { first, count, .. }
            | PlacementAction::PlaceHeld { first, count } = *action
            {
                for ordinal in first..first + count {
                    newly_placed.next().expect("placement message").ordinal = Some(ordinal);
                }
            }
        }
        message_runs.truncate(prompt.len());
        for message in &prompt[message_runs.len()..] {
            let mut attribution = run_id
                .as_ref()
                .filter(|_| ep.attributable)
                .map(producing_run);
            if let Some(run) = &mut attribution {
                run.delivery = message.tags.delivery().cloned();
            }
            message_runs.push(attribution);
        }
        if matches!(rec, WalRecord::RunFinished { .. }) && ep.attributable {
            if let (Some(id), Some(ordinal)) = (
                run_id.as_ref(),
                message_runs
                    .last()
                    .and_then(|run| run.as_ref())
                    .filter(|run| Some(&run.run_id) == run_id.as_ref().map(|id| &id.0))
                    .and_then(|_| placement.next_ordinal().checked_sub(1))
                    .or_else(|| {
                        prompt
                            .is_empty()
                            .then_some(ep.seeded_last_ordinal)
                            .flatten()
                    }),
            ) {
                let mut run = message_runs
                    .last()
                    .and_then(|run| run.clone())
                    .filter(|run| run.run_id == id.0)
                    .unwrap_or_else(|| producing_run(id));
                run.is_final = true;
                run_finals.insert(ordinal, run);
                if let Some(Some(run)) = message_runs.last_mut().filter(|_| !prompt.is_empty()) {
                    if run.run_id == id.0 {
                        run.is_final = true;
                    }
                }
            }
        }
        numbering
            .number_new(&mut prompt)
            .map_err(ReplayError::TagNumbering)?;
        if let WalRecord::CompactionMessage {
            kind: broca_wal::CompactionMessageKind::Setup,
            tag_checkpoint: Some(checkpoint),
            ..
        } = rec
        {
            numbering
                .anchor_setup(&mut prompt, ordinal_base, &[], "", checkpoint)
                .map_err(ReplayError::TagNumbering)?;
        }
    }
    numbering
        .number_new(&mut prompt)
        .map_err(ReplayError::TagNumbering)?;

    // Fold any still-open batch's indeterminate calls (intent∧¬terminal) into the
    // list, then classify the LAST episode's trailing state.
    if let Some(b) = &ep.open_batch {
        for id in b.indeterminate() {
            if !ep.indeterminate.contains(&id) {
                ep.indeterminate.push(id);
            }
        }
    }
    if let Some(start) = ep.first_step_message {
        episode_step_spans.push(start..prompt.len());
    }
    let run_finished_durable = ep.finished.is_some();
    let provider_call_started = ep.provider_call_started;
    let completed_steps = ep.completed_steps;
    let tool_call_count = placement.budget().tool_call_count;
    // Checkpoints encode sorted indexes; the replay API retains its seed prefix followed
    // by new firings in threshold order. This is an output representation, not budget state.
    let mut tail_nudge_indices: Vec<_> = placement
        .budget()
        .fired_nudge_indices
        .iter()
        .copied()
        .filter(|index| !seeded_nudge_indices.contains(index))
        .collect();
    if let Some(budget) = run_config
        .as_ref()
        .and_then(|config| config.budget.as_ref())
        .or(seeded_budget.as_ref())
    {
        tail_nudge_indices.sort_by_key(|index| budget.nudges[*index].at_tool_calls);
    }
    let budget_nudges_fired = seeded_nudge_indices
        .into_iter()
        .chain(tail_nudge_indices)
        .collect();
    let budget_finalize_fired = placement.budget().finalize_fired;
    let budget_finalize_pending = placement.budget().finalize_pending;
    let usage = ep.usage;
    let retries_used = ep.retries_used;
    let last_step_usage = ep.last_step_usage;
    let last_step_finish_reason = ep.last_step_finish_reason;
    let open_tool_batch = match (&ep.open_batch, &ep.open_batch_id) {
        (Some(batch), Some(batch_id)) => Some(OpenToolBatch {
            needs_start: !batch.started,
            batch_id: batch_id.clone(),
            step_id: batch.step_id,
            calls: batch
                .planned
                .iter()
                .enumerate()
                .map(|(position, id)| (id.clone(), batch.call_state(position)))
                .collect(),
        }),
        _ => None,
    };
    let pending_steers = ep.pending_steers.clone();
    // An intent or a Prepared frame makes the turn durable work: show it while recovery
    // retains its calls, even when no call was dispatched before the approval wait.
    let has_open_call =
        ep.open_batch.as_ref().is_some_and(|batch| {
            batch.planned.iter().enumerate().any(|(position, _)| {
                batch.has_intent(position) || batch.prepared[position].is_some()
            })
        });
    let open_tool_turn = if ep.finished.is_none() && has_open_call {
        ep.open_assistant
            .as_ref()
            .zip(ep.open_step_record)
            .map(|(assistant, step_record)| {
                let mut message = tagged(
                    Message {
                        turn_system: false,
                        cache_prefix_blocks: None,
                        role: Role::Assistant,
                        content: assistant.content.clone(),
                        origin: ep
                            .open_assistant_origin
                            .clone()
                            .or_else(|| ep.origin.clone()),
                        ordinal: None,
                        tags: Default::default(),
                    },
                    ep.open_assistant_tags.as_ref(),
                );
                // Reads expose this turn before its batch commits. Commit appends it right
                // after the numbered prompt, so number it now exactly as commit will: an
                // exposed part that later gained a number would be a renumbering within the
                // lineage.
                numbering
                    .number_pending(&mut message)
                    .map_err(ReplayError::TagNumbering)?;
                Ok::<_, ReplayError>((message, step_record))
            })
            .transpose()?
    } else {
        None
    };
    let open_tool_run = open_tool_turn
        .as_ref()
        .and(run_id.as_ref())
        .filter(|_| ep.attributable)
        .map(producing_run);
    if input_budget.exhausted().is_some() {
        // A response can be durable before its terminal barrier. Preserve the budget reason
        // for natural ends and completed repair batches, while leaving open calls resumable
        // so recovery can write their never-executed results before closing the episode.
        ep.natural_stop_reason = Some(RunFinishReason::BudgetExhausted);
        ep.natural_end_extendable = false;
        if ep.hard_stopped.is_some() {
            ep.hard_stopped = Some(RunFinishReason::BudgetExhausted);
        }
    }
    let refused_steps = std::mem::take(&mut ep.refused_steps);
    let account_limit_pause = ep.account_limit_pause.clone();
    let mut resolution = classify(ep, run_id.as_ref(), cancel_at.is_some());
    if input_budget.exhausted().is_some() {
        match &mut resolution {
            ReplayResolution::Resume {
                pending: Some(pending),
            }
            | ReplayResolution::AuthRequired {
                pending: Some(pending),
                ..
            }
            | ReplayResolution::ProviderHeld { pending, .. } => {
                // Recovery still needs the assistant and existing results to close the turn,
                // but none of its unexecuted calls is authorized for redispatch.
                pending.redispatch.clear();
            }
            ReplayResolution::Resume { pending: None } => {
                resolution = ReplayResolution::LineageIdle {
                    last_reason: RunFinishReason::BudgetExhausted,
                };
            }
            _ => {}
        }
    }
    // A torn import's repair truncates the prompt; a turn past the end no longer exists.
    committed_tool_turns.retain(|(prompt_index, _)| *prompt_index < prompt.len());
    // No shipping session waits for a numbering checkpoint from its compaction provider:
    // sessions on the current compaction protocol number tags with broca's own markers, and
    // sessions compacted under the earlier protocol keep the numbers their records froze.
    let tags_await_setup = false;
    Ok(ReplayState {
        mode3,
        account_limit_pause,
        recovered_calls,
        extent: placement.next_ordinal(),
        tags_await_setup,
        input_budget,
        cancel_at,
        message_runs,
        run_finals,
        compaction,
        compaction_history,
        output_reservation,
        compaction_notice_floor: 1,
        manifest: None,
        committed_tool_turns,
        open_tool_turn,
        open_tool_run,
        marker_order_version,
        on_restart,
        keep_warm,
        run_config,
        run_id,
        prompt,
        completed_steps,
        tool_call_count,
        budget_nudges_fired,
        budget_finalize_fired,
        budget_finalize_pending,
        usage,
        retries_used,
        last_step_usage,
        last_step_finish_reason,
        provider_call_started,
        run_finished_durable,
        episode_step_spans: Some(episode_step_spans),
        render_episodes: Some(render_episodes),
        signed_floor,
        open_tool_batch,
        pending_steers,
        resolution,
        refusal_fallback,
        refused_steps,
    })
}

/// A run state reconciled from the WAL, including terminal metadata absent from the index.
///
/// Not `Eq`: `ProviderError` carries an `f64` retry hint.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconciledRunState {
    /// Last completed model step's normalized native reason; display only.
    pub finish: Option<cortexkit_role_llm_runner::finish::Finish>,
    pub account_limit_pause: Option<Box<broca_wal::record::AccountLimitPause>>,
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
    pub auth_selection: Option<broca_wal::AuthSelection>,
    /// The caller's title recorded for this run: its `RunStarted`'s, or, for a queued send that
    /// never started, its `Queued` record's. `None` when the send carried none.
    pub title: Option<String>,
}

fn interrupted_recovery_state() -> ReconciledRunState {
    ReconciledRunState {
        finish: None,
        budget_detail: None,
        context_ceiling: None,
        ceiling_deadline_ms: None,
        ride_wait_deadline_ms: None,
        structured_output: None,
        live_step: None,
        held_calls: Vec::new(),
        state: crate::RunIndexState::Interrupted,
        undelivered_steers: None,
        final_step_finish_reason: None,
        indeterminate_tool_calls: None,
        usage: None,
        run_usage: Default::default(),
        retries_used: 0,
        error: None,
        pause_reason: None,
        account_limit_pause: None,
        auth_selection: None,
        title: None,
    }
}

/// Walk the WAL to resolve a run and retain terminal metadata that the durable index omits.
pub fn reconcile_run_state<'a>(
    records: impl IntoIterator<Item = &'a WalRecord>,
    run_id: &str,
) -> Option<ReconciledRunState> {
    use crate::{projection::IndeterminateToolCalls, RunIndexState};

    let mut run_usage = RunUsageProjection::new(RunId(run_id.to_owned()));
    let mut active_run: Option<String> = None;
    let mut active_zero_step_seed = false;
    let mut has_completed_step = false;
    let mut total_usage = Usage::default();
    let mut total_retries_used = 0u32;
    let mut last_step_finish_reason = None;
    let mut indeterminate_tool_calls = IndeterminateToolCalls::default();
    let mut pending_steer_ids = Vec::new();
    let mut found: Option<ReconciledRunState> = None;
    let mut auth_selection = None;
    let mut title: Option<String> = None;
    let mut budget_detail = None;
    let mut context_ceiling = None;
    let mut held_calls = HeldCallsProjection::default();
    // The structured-output settings the target run froze, and the result it recorded.
    let mut frozen_structured_output = None;
    let mut frozen_finish_config = serde_json::Value::Null;
    let mut structured_output_result = None;
    // A queued send's submission id is `sub-<run id>` (see the coordinator's `queue_submission`),
    // so its `Queued` record can name this run before any `RunStarted` does.
    let queued_submission = format!("sub-{run_id}");
    for rec in records {
        run_usage.observe(rec);
        let observes_held_calls = match rec {
            WalRecord::RunStarted { run_id: rid, .. } => rid.0 == run_id,
            _ => active_run.as_deref() == Some(run_id),
        };
        if observes_held_calls {
            let _ = held_calls.observe(rec);
        }
        match rec {
            WalRecord::RunStarted {
                run_id: rid,
                origin,
                config,
                title: started_title,
                ..
            } => {
                if rid.0 == run_id {
                    frozen_finish_config = config.frozen_render_config.clone();
                    auth_selection = config.auth_selection.clone();
                    title = started_title.clone();
                    frozen_structured_output =
                        broca_protocol::structured_output::FrozenStructuredOutput::from_frozen_render_config(
                            Some(&config.frozen_render_config),
                        );
                }
                active_run = Some(rid.0.clone());
                budget_detail = None;
                context_ceiling = None;
                pending_steer_ids.clear();
                active_zero_step_seed = matches!(origin, broca_wal::RunOrigin::SeededFrom { .. });
                has_completed_step = false;
                total_usage = Usage::default();
                total_retries_used = 0;
                last_step_finish_reason = None;
                indeterminate_tool_calls.reset_episode();
                if rid.0 == run_id {
                    // Seen this run start; default to Active until a terminal/pause appears.
                    found = Some(ReconciledRunState {
                        finish: None,
                        account_limit_pause: None,
                        budget_detail: None,
                        context_ceiling: None,
                        ceiling_deadline_ms: None,
                        structured_output: None,
                        live_step: None,
                        held_calls: Vec::new(),
                        title: None,
                        state: RunIndexState::Active,
                        undelivered_steers: None,
                        final_step_finish_reason: None,
                        indeterminate_tool_calls: None,
                        usage: None,
                        run_usage: Default::default(),
                        retries_used: 0,
                        error: None,
                        pause_reason: None,
                        auth_selection: None,
                        ride_wait_deadline_ms: None,
                    });
                }
            }
            WalRecord::ContextCeilingWait { deadline_ms, .. }
                if active_run.as_deref() == Some(run_id) =>
            {
                if let Some(state) = &mut found {
                    state.ceiling_deadline_ms = Some(*deadline_ms);
                }
            }
            WalRecord::CompactionRideWait { deadline_ms, .. }
                if active_run.as_deref() == Some(run_id) =>
            {
                if let Some(state) = &mut found {
                    state.ride_wait_deadline_ms = Some(*deadline_ms);
                }
            }
            WalRecord::CompactionRideWaitEnded { .. } if active_run.as_deref() == Some(run_id) => {
                if let Some(state) = &mut found {
                    state.ride_wait_deadline_ms = None;
                }
            }
            WalRecord::ContextCeilingDecided { detail, .. }
                if active_run.as_deref() == Some(run_id) =>
            {
                context_ceiling = detail.clone();
                if let Some(state) = &mut found {
                    state.ceiling_deadline_ms = None;
                    state.context_ceiling = detail.clone();
                }
            }
            WalRecord::InputTokenBudgetExhausted { detail } => budget_detail = Some(detail.clone()),
            WalRecord::StructuredOutputDowngraded { run_id: rid } if rid.0 == run_id => {
                if let Some(so) = &mut frozen_structured_output {
                    so.downgrade_grammar();
                }
            }
            WalRecord::StructuredOutputResult {
                run_id: rid,
                result,
            } if rid.0 == run_id => structured_output_result = Some(result.clone()),
            WalRecord::ModelStepFinished {
                finish_reason,
                retries_used,
                raw_finish_reason,
                assistant_message,
                fallback,
                ..
            } => {
                active_zero_step_seed = false;
                has_completed_step = true;
                total_usage.accumulate(&crate::refusal_fallback::total_step_usage(rec).unwrap());
                total_retries_used = total_retries_used.saturating_add(*retries_used);
                last_step_finish_reason = *finish_reason;
                // A step that finished no longer waits for its ride.
                if active_run.as_deref() == Some(run_id) {
                    if let Some(state) = &mut found {
                        state.finish = step_finish(
                            &frozen_finish_config,
                            fallback.refusal_fallback.is_some(),
                            raw_finish_reason.as_deref(),
                            assistant_message.tool_calls().next().is_some(),
                        );
                        state.ride_wait_deadline_ms = None;
                    }
                }
            }
            WalRecord::ModelStepRefused { .. } => {
                total_usage.accumulate(&crate::refusal_fallback::total_step_usage(rec).unwrap());
            }
            WalRecord::RecoveryCut { recovery } => {
                let calls: Vec<_> = recovery
                    .runs
                    .iter()
                    .filter(|run| run.run_id.0 == run_id)
                    .flat_map(|run| run.calls.iter().map(|call| call.tool_call_id.clone()))
                    .collect();
                if recovery.runs.iter().any(|run| run.run_id.0 == run_id)
                    || (recovery
                        .active_run
                        .as_ref()
                        .is_some_and(|run| run.0 == run_id))
                {
                    for call in &calls {
                        indeterminate_tool_calls.record_dispatch_terminal(call, true);
                    }
                    let state = found.get_or_insert_with(interrupted_recovery_state);
                    if matches!(state.state, RunIndexState::Active | RunIndexState::Paused) {
                        state.state = RunIndexState::Interrupted;
                    }
                    let known = state.indeterminate_tool_calls.get_or_insert_with(Vec::new);
                    for call in calls {
                        if !known.contains(&call) {
                            known.push(call);
                        }
                    }
                }
            }
            WalRecord::RecoveryCallClosed {
                run_id: owner,
                tool_call_id,
                ..
            } if owner.0 == run_id => {
                indeterminate_tool_calls.record_dispatch_terminal(tool_call_id, true);
                let state = found.get_or_insert_with(interrupted_recovery_state);
                if matches!(state.state, RunIndexState::Active | RunIndexState::Paused) {
                    state.state = RunIndexState::Interrupted;
                }
                let known = state.indeterminate_tool_calls.get_or_insert_with(Vec::new);
                if !known.contains(tool_call_id) {
                    known.push(tool_call_id.clone());
                }
            }
            WalRecord::ToolBatchStarted { .. } => indeterminate_tool_calls.reset_batch(),
            WalRecord::ToolDispatchIntent { tool_call_id, .. } => {
                indeterminate_tool_calls.record_dispatch_intent(tool_call_id);
            }
            WalRecord::ToolNoDispatch { tool_call_id, .. } => {
                indeterminate_tool_calls.record_dispatch_terminal(tool_call_id, false);
            }
            WalRecord::ToolResult {
                result, outcome, ..
            } => {
                indeterminate_tool_calls
                    .record_dispatch_terminal(&result.tool_call_id, outcome.is_indeterminate());
            }
            WalRecord::Steered {
                send_id: Some(send_id),
                ..
            } => pending_steer_ids.push(send_id.clone()),
            WalRecord::StepStarted { .. } => {
                active_zero_step_seed = false;
                pending_steer_ids.clear();
            }
            // The cause is read from the same record that settles the state, so the reported
            // state and cause can never come from different runs. `session.read` attributes a
            // terminal to its run the same way (the `RunFinished` inside that run's episode).
            WalRecord::RunFinished {
                reason,
                usage,
                retries_used,
                error,
                ..
            } => {
                active_zero_step_seed = false;
                if active_run.as_deref() == Some(run_id) {
                    let state = match reason {
                        RunFinishReason::Completed => RunIndexState::Completed,
                        RunFinishReason::MaxSteps => RunIndexState::MaxSteps,
                        RunFinishReason::BudgetExhausted => RunIndexState::BudgetExhausted,
                        RunFinishReason::ContextCeiling => RunIndexState::ContextCeiling,
                        RunFinishReason::Cancelled => RunIndexState::Cancelled,
                        RunFinishReason::Interrupted => RunIndexState::Interrupted,
                        RunFinishReason::Error => RunIndexState::Error,
                        RunFinishReason::TransformUnavailable => {
                            RunIndexState::TransformUnavailable
                        }
                    };
                    let structured_output = ended_structured_output(
                        structured_output_result.clone(),
                        frozen_structured_output.as_ref(),
                    );
                    found = Some(ReconciledRunState {
                        account_limit_pause: None,
                        budget_detail: budget_detail.clone(),
                        finish: found.as_ref().and_then(|state| state.finish.clone()),
                        context_ceiling: context_ceiling.clone(),
                        ceiling_deadline_ms: None,
                        structured_output,
                        live_step: None,
                        held_calls: Vec::new(),
                        title: None,
                        state,
                        undelivered_steers: Some(pending_steer_ids.clone()),
                        final_step_finish_reason: last_step_finish_reason,
                        indeterminate_tool_calls: (*reason == RunFinishReason::Interrupted)
                            .then(|| indeterminate_tool_calls.snapshot()),
                        usage: has_completed_step.then_some(*usage),
                        run_usage: Default::default(),
                        retries_used: *retries_used,
                        error: error.clone(),
                        pause_reason: None,
                        auth_selection: None,
                        ride_wait_deadline_ms: None,
                    });
                }
            }
            WalRecord::RunPaused {
                run_id: rid,
                reason,
                error,
                account_limit,
                ..
            } if rid.0 == run_id => {
                found = Some(ReconciledRunState {
                    account_limit_pause: account_limit.clone(),
                    budget_detail: None,
                    finish: None,
                    context_ceiling: None,
                    ceiling_deadline_ms: None,
                    structured_output: None,
                    live_step: None,
                    held_calls: Vec::new(),
                    title: None,
                    state: RunIndexState::Paused,
                    undelivered_steers: None,
                    final_step_finish_reason: None,
                    indeterminate_tool_calls: None,
                    usage: Some(total_usage),
                    run_usage: Default::default(),
                    retries_used: total_retries_used,
                    error: error.clone(),
                    pause_reason: Some(*reason),
                    auth_selection: auth_selection.clone(),
                    ride_wait_deadline_ms: None,
                });
            }
            WalRecord::Queued {
                submission_id,
                title: queued_title,
                ..
            } if submission_id.0 == queued_submission && title.is_none() => {
                title = queued_title.clone();
            }
            WalRecord::Retracted {
                failure: Some(failure),
                ..
            } if failure.run_id.0 == run_id => {
                found = Some(ReconciledRunState {
                    account_limit_pause: None,
                    budget_detail: None,
                    context_ceiling: None,
                    finish: None,
                    ceiling_deadline_ms: None,
                    structured_output: None,
                    live_step: None,
                    held_calls: Vec::new(),
                    title: None,
                    state: RunIndexState::Error,
                    undelivered_steers: Some(Vec::new()),
                    final_step_finish_reason: None,
                    indeterminate_tool_calls: None,
                    usage: None,
                    run_usage: Default::default(),
                    retries_used: 0,
                    error: Some(failure.error.clone()),
                    pause_reason: None,
                    auth_selection: None,
                    ride_wait_deadline_ms: None,
                });
            }
            _ => {}
        }
    }
    if active_run.as_deref() == Some(run_id) && active_zero_step_seed {
        return Some(ReconciledRunState {
            finish: None,
            account_limit_pause: None,
            budget_detail: None,
            context_ceiling: None,
            ceiling_deadline_ms: None,
            structured_output: None,
            live_step: None,
            held_calls: Vec::new(),
            state: RunIndexState::Interrupted,
            undelivered_steers: Some(pending_steer_ids.clone()),
            final_step_finish_reason: None,
            indeterminate_tool_calls: Some(Vec::new()),
            usage: None,
            run_usage: run_usage.usage,
            retries_used: 0,
            error: None,
            pause_reason: None,
            auth_selection,
            title,
            ride_wait_deadline_ms: None,
        });
    }
    found.map(|state| ReconciledRunState {
        held_calls: if matches!(state.state, RunIndexState::Active | RunIndexState::Paused) {
            held_calls.status().held_calls
        } else {
            Vec::new()
        },
        auth_selection,
        title,
        run_usage: run_usage.usage,
        ..state
    })
}

/// Classify the LAST episode's trailing state into a resolution. Order matters
/// (first match wins): guarded pause → terminal → ambiguous → natural-end → resume. A pause is
/// resumable only when no dispatch is indeterminate: intent without a terminal has unknown side
/// effects, so exactly-once execution requires `Interrupted`, never a retry. The producer currently
/// pauses only at a step-entry boundary after the previous batch finished, but replay enforces this
/// fail-closed rule as defense in depth.
fn classify(ep: Episode, run_id: Option<&RunId>, cancelling: bool) -> ReplayResolution {
    if ep.recovery_interrupted && ep.finished.is_none() {
        return ReplayResolution::Interrupted {
            indeterminate: ep.indeterminate,
        };
    }
    let closing = ep.open_batch.as_ref().is_some_and(|batch| {
        batch.provider_held.keys().any(|id| {
            batch
                .planned
                .iter()
                .position(|planned| planned == id)
                .is_some_and(|position| {
                    matches!(
                        batch.terminals[position],
                        Some(Terminal::Result(
                            _,
                            DispatchOutcome::Interrupted | DispatchOutcome::MaxSteps,
                            _
                        ))
                    )
                })
        })
    });
    if closing && ep.finished.is_none() {
        return ReplayResolution::Interrupted {
            indeterminate: ep.indeterminate,
        };
    }
    if ep.finished.is_none() && ep.indeterminate.is_empty() && !cancelling {
        if let Some(batch) = &ep.open_batch {
            if !batch.provider_held.is_empty()
                && (batch.terminals.iter().all(Option::is_some)
                    || batch.planned.iter().enumerate().any(|(position, _)| {
                        batch.call_state(position) == OpenCallState::ProviderHeld
                    }))
            {
                if let (Some(run_id), Some(pending)) = (run_id, pending_turn(&ep)) {
                    let open_held = pending
                        .provider_held
                        .iter()
                        .map(|reply| reply.tool_call_id.clone())
                        .collect();
                    return ReplayResolution::ProviderHeld {
                        run_id: run_id.clone(),
                        pending,
                        open_held,
                    };
                }
            }
        }
    }
    // A non-terminal pause is resumable, not finished — check it FIRST so a paused
    // episode is never read as terminal (which would seal a resumable run).
    if ep
        .paused
        .as_ref()
        .is_some_and(|(_, reason, _)| *reason != PauseReason::ToolHeld)
        && ep.indeterminate.is_empty()
    {
        let pending = pending_turn(&ep);
        let (run_id, reason, error) = match ep.paused {
            Some(paused) => paused,
            None => unreachable!("pause checked above"),
        };
        return ReplayResolution::AuthRequired {
            run_id,
            reason,
            error: error.map(Box::new),
            pending,
        };
    }
    // RunFinished is the last-writer-wins episode terminal. In particular, recovery seals an
    // ambiguous dispatch by appending only RunFinished{Interrupted}; the unresolved batch stays
    // uncommitted, but the terminal makes the lineage idle so a later episode can append.
    if let Some(last_reason) = ep.finished {
        return ReplayResolution::LineageIdle { last_reason };
    }
    // Without a later terminal, any ambiguous call stops for review because re-running it could
    // duplicate a side effect. This must remain ahead of all inferred/natural finish states.
    //
    // This arm also outranks a durable RunPaused, where admission says Paused and is right about
    // its own question -- pinned by
    // `admission_paused_but_replay_interrupted_with_ambiguous_call_is_intentional`. The episode IS
    // paused AND has an unresolved dispatch; replay's answer is the richer one because it is the
    // one that must not re-dispatch a call that may already have run.
    if !ep.indeterminate.is_empty() {
        return ReplayResolution::Interrupted {
            indeterminate: ep.indeterminate,
        };
    }
    // A committed hard-stop repair batch with no durable RunFinished (a crash lost the
    // terminal between the batch and the RunFinished record): the run hit its stop limit,
    // so finish the lineage here — NEVER re-run the model past the limit.
    if let Some(last_reason) = ep.hard_stopped {
        return ReplayResolution::LineageIdle { last_reason };
    }
    // The last durable step was a TERMINAL turn (no tool calls) but `run_finished`
    // is missing (crash between the final step and run_finished). The episode ended
    // naturally — finish it WITHOUT re-running the model (a re-run would generate a
    // divergent extra turn). Resume just re-writes run_finished.
    //
    // ADMISSION DISAGREES WITH THIS ARM ON PURPOSE and the disagreement is pinned:
    // `admission_running_but_replay_completed_without_durable_terminal_is_intentional`
    // in admission.rs. Admission answers "is there an unterminated episode, may I admit a
    // new send" and correctly says Running, because no terminal record exists and something
    // must seal it. This arm answers "what must the loop DO next" and says the episode is
    // over. Both are true of the same records. IF YOU ARE HERE TO MAKE THEM AGREE, READ THAT
    // TEST FIRST -- collapsing the two loses one of the two questions.
    //
    // One exception: a steer still pending after a no-tool step that no limiting stop or
    // budget finalize ended. The live loop does not finish on such a step, it runs one more step
    // to render the steer. So the episode had not ended, and it resumes (or, after a crash, is
    // sealed) like any running one.
    let extended_by_steer = ep.natural_end_extendable && !ep.pending_steers.is_empty();
    if ep.natural_end && !extended_by_steer {
        return ReplayResolution::LineageIdle {
            last_reason: ep.natural_stop_reason.unwrap_or(RunFinishReason::Completed),
        };
    }
    // A provider-free seed with no terminal can only be repaired append-only. It must never
    // enter Resume/AuthRequired because its import-marker config cannot drive a provider.
    //
    // Admission says Running for these same records; pinned by
    // `admission_running_but_replay_interrupted_for_unsealed_provider_free_seed_is_intentional`.
    // The empty indeterminate set is load-bearing: nothing was ever dispatched, so there is no
    // ambiguous side effect -- this is Interrupted-with-nothing, not Interrupted-carrying-calls.
    if ep.seeded_from && ep.completed_steps == 0 {
        return ReplayResolution::Interrupted {
            indeterminate: Vec::new(),
        };
    }
    // No trailing tool turn → clean boundary; resume re-runs the model.
    ReplayResolution::Resume {
        pending: pending_turn(&ep),
    }
}

fn pending_turn(ep: &Episode) -> Option<PendingTurn> {
    let assistant = ep.open_assistant.as_ref()?.as_ref().clone();
    let step_id = ep.open_step.unwrap_or(StepId(0));
    let pending = match &ep.open_batch {
        // Batch was started but isn't complete: finish it (existing terminals +
        // re-dispatch the never-left calls), reusing the durable batch_id.
        Some(b) => PendingTurn {
            step_id,
            assistant,
            batch_id: ep.open_batch_id.clone(),
            planned: b.planned.clone(),
            existing: b.existing_results(),
            redispatch: b.redispatchable(),
            deferred: b.prepared_in_state(OpenCallState::Deferred),
            approved: b.prepared_in_state(OpenCallState::Approved),
            provider_held: b
                .planned
                .iter()
                .enumerate()
                .filter_map(|(position, id)| {
                    (b.call_state(position) == OpenCallState::ProviderHeld)
                        .then(|| b.provider_held.get(id).cloned())
                        .flatten()
                })
                .collect(),
            held_successors: b
                .planned
                .iter()
                .enumerate()
                .filter(|(position, _)| b.call_state(*position) == OpenCallState::HeldSuccessor)
                .map(|(_, id)| id.clone())
                .collect(),
            assistant_tags: ep.open_assistant_tags.clone(),
            existing_tags: b.existing_tags(),
        },
        // model_step_finished with NO batch started → the batch never opened;
        // re-dispatch ALL the assistant's tool calls fresh (none ever ran).
        None => {
            let planned: Vec<ToolCallId> = assistant
                .tool_calls()
                .map(|c| c.tool_call_id.clone())
                .collect();
            PendingTurn {
                step_id,
                assistant,
                batch_id: None,
                redispatch: planned.clone(),
                planned,
                existing: Vec::new(),
                deferred: Vec::new(),
                approved: Vec::new(),
                provider_held: Vec::new(),
                held_successors: Vec::new(),
                assistant_tags: ep.open_assistant_tags.clone(),
                existing_tags: Vec::new(),
            }
        }
    };
    Some(pending)
}

/// Push a complete tool turn: the assistant message + the tool-role message with
/// every planned call's result in PLANNED ORDER (provider-valid). `origin` is the
/// producing model stamped on the assistant turn (the tool-role result carries none).
fn commit_tool_turn(
    prompt: &mut Vec<Message>,
    assistant: &AssistantMessage,
    assistant_tags: Option<&broca_protocol::tags::MessageTags>,
    batch: &OpenBatch,
    origin: Option<broca_protocol::request::ModelHandle>,
) {
    prompt.push(tagged(
        Message {
            turn_system: false,
            cache_prefix_blocks: None,
            role: Role::Assistant,
            content: assistant.content.clone(),
            origin,
            ordinal: None,
            tags: Default::default(),
        },
        assistant_tags,
    ));
    let content: Vec<ContentBlock> = batch
        .planned
        .iter()
        .enumerate()
        .filter_map(|(position, _)| batch.result_for(position).map(ContentBlock::ToolResult))
        .collect();
    prompt.push(Message {
        turn_system: false,
        cache_prefix_blocks: None,
        role: Role::Tool,
        content,
        origin: None,
        ordinal: None,
        tags: Default::default(),
    });
    crate::tags::attach_parts(prompt.last_mut().expect("just pushed"), batch.result_tags());
}

/// The key a recovered call is filed under: its log's lineage, the generation when the log was
/// rewritten, and the call's record number.
fn physical_call_key(lineage: &str, seq: u64, generation: Option<&str>) -> String {
    match generation {
        Some(generation) => format!("{lineage}~{generation}:{seq}"),
        None => format!("{lineage}:{seq}"),
    }
}

#[cfg(test)]
mod steer_tests;

#[cfg(test)]
pub(crate) mod deferred_tests {
    use super::*;
    use broca_wal::{DeferredAsk, DeferredExecutor, OnExpiry};

    pub(crate) fn prepared(id: &str, batch: &str) -> WalRecord {
        WalRecord::ToolCallDeferred {
            deferred_at_ms: None,
            batch_id: BatchId(batch.into()),
            call_id: ToolCallId(id.into()),
            executor: DeferredExecutor::Broca,
            module: Some("tools".into()),
            tool: "edit".into(),
            schema_pin: None,
            args_jcs: "{\"n\":1}".into(),
            args_hash: "hash".into(),
            scope: serde_json::from_value(
                serde_json::json!({"owner":{"kind":"reserved", "module_id":"agent"}, "ref":"s", "scope_epoch":1}),
            )
            .unwrap(),
            ask: DeferredAsk {
                prompt: "Allow?".into(),
                options: vec![],
                expires_at_ms: 1000,
                on_expiry: OnExpiry::Deny,
                material_damage: false,
                late_execution: LateExecution::Execute,
            },
        }
    }
}
