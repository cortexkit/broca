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

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolOutput, ToolResult},
    error::ProviderError,
    request::BudgetConfig,
    BatchId, FinishReason, RunId, StepId, ToolCallId, Usage,
};
use broca_wal::{
    DeferredDecision, DispatchOutcome, FramedRecord, LateDecision, LateExecution, NoDispatchReason,
    PauseReason, RunConfig, WalByteAnchor, WalLineageId, WalRecord, WithdrawElicitation,
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
    /// The last episode renders tags under the computed rules on a compaction-managed session,
    /// is still running, and no Setup answer is recorded yet: the answer may carry a tag
    /// checkpoint that renumbers the run's parts from its cut (llm-runner §9.6), so no read
    /// may expose those numbers until it is durable.
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
    /// The last episode's tool batch that was opened (`tool_batch_started` is durable) but
    /// never committed, with each planned call's durable state. `None` when no batch is open.
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
}

/// An opened, uncommitted tool batch as the WAL left it.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenToolBatch {
    pub batch_id: BatchId,
    /// The model step whose tool calls this batch runs.
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
    /// A dispatch intent is durable and no terminal is: the call may or may not have run.
    Dispatched,
    /// Neither Prepared, intent nor terminal is durable: the call never left the process.
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
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProjectedDeferredCall {
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
                    let key = format!("{}:{}", self.lineage_id, frame.seq);
                    self.calls
                        .entry(key)
                        .or_insert_with(|| ProjectedDeferredCall {
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
        self.stopped_runs.as_ref().is_some_and(|stopped| {
            call.run_id
                .as_ref()
                .is_some_and(|run| !stopped.contains(&run.0))
        })
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

    pub fn late_approvals(&self) -> Vec<LateApprovalView> {
        self.calls
            .iter()
            .filter_map(|(key, call)| {
                let approval = call.late_approval.clone()?;
                let (outcome, result) = match call.late_result.as_deref() {
                    Some(WalRecord::LateResult {
                        outcome, result, ..
                    }) => (Some(*outcome), Some(result.clone())),
                    _ if call.late_dispatch_intent.is_some() => {
                        (Some(broca_wal::LateOutcome::Unknown), None)
                    }
                    _ => (None, None),
                };
                Some(LateApprovalView {
                    call_key: key.clone(),
                    approval,
                    outcome,
                    result,
                })
            })
            .collect()
    }
}

/// Compose the materialized approval summary with a WAL suffix. A complete range supplies its
/// identity; legacy direct projection callers can recover it from an explicit Prepared key.
pub fn project_deferred_calls(
    seed: Option<&DeferredCallProjection>,
    frames: &[FramedRecord],
    lineage_id: Option<&str>,
    last_run_id: Option<&RunId>,
) -> Option<DeferredCallProjection> {
    let lineage_id = lineage_id
        .map(str::to_owned)
        .or_else(|| seed.map(|seed| seed.lineage_id.clone()))
        .or_else(|| {
            frames.iter().find_map(|frame| {
                let key = match &frame.record {
                    WalRecord::ApprovalRecorded { call_key, .. }
                    | WalRecord::LateApprovalRecorded { call_key, .. }
                    | WalRecord::LateDispatchIntent { call_key, .. }
                    | WalRecord::LateResult { call_key, .. }
                    | WalRecord::QuestionWithdrawalAcknowledged { call_key, .. } => {
                        call_key.as_str()
                    }
                    WalRecord::ToolDispatchIntent {
                        call_key: Some(key),
                        ..
                    } => key,
                    WalRecord::AwaitingApproval { pending, .. } => &pending.first()?.call_key,
                    WalRecord::ToolResult {
                        withdraw_elicitation: Some(withdrawal),
                        ..
                    } => &withdrawal.call_key,
                    _ => return None,
                };
                Some(key.rsplit_once(':')?.0.to_owned())
            })
        })?;
    let origin = frames.first().is_some_and(|frame| frame.seq == 1)
        && frames
            .windows(2)
            .all(|pair| pair[0].seq.checked_add(1) == Some(pair[1].seq));
    let accepted_seed = seed
        .filter(|seed| seed.lineage_id == lineage_id && (seed.stopped_runs.is_some() || !origin));
    let mut projection = accepted_seed
        .cloned()
        .unwrap_or_else(|| DeferredCallProjection::new(&lineage_id));
    if accepted_seed.is_none() && !origin && !(frames.is_empty() && last_run_id.is_none()) {
        projection.stopped_runs = None;
    }
    if projection.last_run_id.is_none() {
        projection.last_run_id = last_run_id.cloned();
    }
    projection.fold_frames(frames);
    Some(projection)
}

/// The shared `NoDispatchReason → ToolResult` rebuild — used BOTH by the live loop
/// and on replay, so a reconstructed no-dispatch result is BYTE-IDENTICAL to what
/// the live run produced (one function is the single source of truth for the
/// result bytes, so the two paths can't drift).
pub fn no_dispatch_result(tool_call_id: &ToolCallId, reason: &NoDispatchReason) -> ToolResult {
    let output = match reason {
        NoDispatchReason::Denied { reason } => ToolOutput::Denied {
            reason: reason.clone(),
        },
        NoDispatchReason::ValidationFailed { detail }
        | NoDispatchReason::Unavailable { detail }
        | NoDispatchReason::PolicyBlocked { detail } => ToolOutput::Error {
            message: detail.clone(),
        },
    };
    ToolResult {
        tool_call_id: tool_call_id.clone(),
        output,
        is_error: true,
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
    /// The model step whose tool calls this batch runs, from its `tool_batch_started`.
    step_id: StepId,
    planned: Vec<ToolCallId>,
    intents: HashSet<ToolCallId>,
    terminals: Vec<Option<Terminal>>,
    prepared: Vec<Option<PreparedCall>>,
}

impl OpenBatch {
    fn new(step_id: StepId, planned: Vec<ToolCallId>) -> Self {
        Self {
            step_id,
            terminals: vec![None; planned.len()],
            prepared: vec![None; planned.len()],
            planned,
            intents: HashSet::new(),
        }
    }

    fn all_terminated(&self) -> bool {
        self.terminals.iter().all(Option::is_some)
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
        self.intents.contains(id) && !self.planned[..position].contains(id)
    }

    /// Calls whose outcome is ambiguous: an `Unknown` terminal, or an intent with
    /// no terminal at all (the result was lost in the crash).
    fn indeterminate(&self) -> Vec<ToolCallId> {
        self.planned
            .iter()
            .enumerate()
            .filter(|(position, _)| match &self.terminals[*position] {
                Some(Terminal::Result(_, DispatchOutcome::Unknown, _)) => true,
                Some(_) => false,
                None => self.has_intent(*position), // intent ∧ ¬terminal
            })
            .map(|(_, id)| id.clone())
            .collect()
    }

    fn call_state(&self, position: usize) -> OpenCallState {
        if self.terminals[position].is_some() {
            OpenCallState::Terminated
        } else if self.has_intent(position) {
            OpenCallState::Dispatched
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
    attributable: bool,
    seeded_last_ordinal: Option<u64>,
    /// The index among the replayed records of the step record holding the open tool turn.
    open_step_record: Option<usize>,
    /// Provider-free SeededFrom episodes are imports/forks, not resumable model runs.
    seeded_from: bool,
    /// Prompt length before an import's input, used to discard a torn import during repair.
    seeded_prompt_start: Option<usize>,
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
    budget: crate::BudgetReplayStateV1,
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
    /// The trailing tool-turn (assistant w/ tool calls) + its batch, held until the
    /// batch completes (then committed to the prompt).
    open_assistant: Option<Arc<AssistantMessage>>,
    /// The runner tags of `open_assistant`, from its `ModelStepFinished`.
    open_assistant_tags: Option<broca_protocol::tags::MessageTags>,
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
    /// Ambiguous calls (an `Unknown` terminal, or an intent with no terminal). Stays
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
    fn place_steers(&mut self, prompt: &mut Vec<Message>) {
        prompt.append(&mut self.pending_steers);
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
    let mut state = replay_seeded(
        records,
        None,
        None,
        None,
        None,
        0,
        false,
        crate::tags::TagSeed::default(),
    )?;
    // Stamp the fully-assembled prompt with each message's durable lineage position. Done at
    // this top-level finalization point (not inside `replay_seeded`, which produces sub-lists
    // like a store-merge tail) so the position is over the WHOLE prompt. The store-merge fast
    // path stamps the same way in `effective_context`/`resume_state_from_store`, so live and
    // resume assign identical ordinals — asserted by `merge_equals_full_replay`.
    broca_protocol::content::stamp_ordinals(&mut state.prompt);
    Ok(state)
}

/// Persisted state needed to project a WAL suffix without replaying the materialized prefix.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionSeed {
    pub watermark_seq: u64,
    pub anchor: Option<WalByteAnchor>,
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

impl ProjectionSeed {
    pub fn empty() -> Self {
        Self {
            watermark_seq: 0,
            anchor: None,
            prefix_len: 0,
            completed_steps: 0,
            usage: Usage::default(),
            retries_used: 0,
            last_step_usage: None,
            last_step_finish_reason: None,
            trailing_origin: None,
            last_run_id: None,
            admission_checkpoint: crate::AdmissionCheckpointV1::empty(),
            deferred_calls: None,
            tag_seed: crate::tags::TagSeed::default(),
        }
    }
}

/// The store-ready result of folding records after a [`ProjectionSeed`].
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionDelta {
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

/// Replay only a projection tail and compose it with persisted per-episode counters.
///
/// Messages are intentionally left unstamped because stores assign their durable ordinals while
/// appending them. A `RunStarted` anywhere in the tail opens a new episode, so counters from the
/// seed must not carry into the result; prompt positions and producing-model lineage still do.
pub fn replay_projection_tail(
    seed: &ProjectionSeed,
    frames: &[FramedRecord],
) -> Result<ProjectionDelta, ReplayError> {
    let seeded_episode = seed.admission_checkpoint.last_episode.as_ref();
    let seeded_budget = seeded_episode.and_then(|episode| episode.config.budget.clone());
    let seeded_budget_state =
        seeded_episode.and_then(|episode| episode.budget_replay_state.clone());
    if seeded_budget.is_some() && !budget_replay_seed_is_valid(seed) {
        return Err(invalid_budget_seed_error(seed));
    }
    // The decoded frames remain live for this call, so replay can borrow their records instead of
    // cloning each record's owned metadata and allocating a temporary vector.
    let tail_state = replay_seeded_with_compaction(
        frames.iter().map(|frame| &frame.record),
        seed.trailing_origin.clone(),
        seeded_episode.map(|episode| episode.run_id.clone()),
        seeded_budget,
        seeded_budget_state,
        (seed.prefix_len, seed.prefix_len > 0),
        seed.admission_checkpoint.compaction.as_ref(),
        seed.tag_seed,
    )?;
    let mut starts_new_episode = false;
    let mut tail_updates_last_step_usage = false;
    let mut tail_updates_last_step_finish_reason = false;
    let mut tail_has_run_finished = false;
    for frame in frames {
        match &frame.record {
            WalRecord::RunStarted { .. } => starts_new_episode = true,
            WalRecord::ModelStepFinished { .. } => {
                tail_updates_last_step_usage = true;
                tail_updates_last_step_finish_reason = true;
            }
            WalRecord::Retracted {
                failure: Some(_), ..
            } => tail_updates_last_step_finish_reason = true,
            WalRecord::RunFinished { .. } => tail_has_run_finished = true,
            _ => {}
        }
    }

    let (completed_steps, usage, retries_used, last_step_usage, last_step_finish_reason) =
        if starts_new_episode {
            (
                tail_state.completed_steps,
                tail_state.usage,
                tail_state.retries_used,
                tail_state.last_step_usage,
                tail_state.last_step_finish_reason,
            )
        } else {
            let mut usage = seed.usage;
            usage.accumulate(&tail_state.usage);
            let tail_retries = if tail_has_run_finished {
                tail_state.retries_used.saturating_sub(seed.retries_used)
            } else {
                tail_state.retries_used
            };
            (
                seed.completed_steps + tail_state.completed_steps,
                usage,
                seed.retries_used.saturating_add(tail_retries),
                if tail_updates_last_step_usage {
                    tail_state.last_step_usage
                } else {
                    seed.last_step_usage
                },
                if tail_updates_last_step_finish_reason {
                    tail_state.last_step_finish_reason
                } else {
                    seed.last_step_finish_reason
                },
            )
        };

    let truncate_prefix_to = (seed.completed_steps == 0
        && first_session_record_is_interrupted(frames.iter().map(|frame| &frame.record)))
    .then_some(0);
    let mut admission_checkpoint = seed.admission_checkpoint.clone();
    admission_checkpoint.fold_frames(frames)?;
    if admission_checkpoint.compaction.is_some() {
        admission_checkpoint.compaction = Some(tail_state.compaction.clone());
    }
    let last_run_id = tail_state.run_id.or_else(|| seed.last_run_id.clone());
    debug_assert_eq!(
        last_run_id.as_ref(),
        admission_checkpoint
            .last_episode
            .as_ref()
            .map(|episode| &episode.run_id)
    );
    Ok(ProjectionDelta {
        messages: tail_state.prompt,
        message_runs: tail_state.message_runs,
        run_finals: tail_state.run_finals,
        truncate_prefix_to,
        completed_steps,
        usage,
        retries_used,
        last_step_usage,
        last_step_finish_reason,
        last_run_id,
        admission_checkpoint,
        deferred_calls: project_deferred_calls(
            seed.deferred_calls.as_ref(),
            frames,
            None,
            seed.last_run_id.as_ref(),
        ),
    })
}

/// Validate a budgeted projection/admission seed before it can affect replay.
///
/// A malformed seed must force a complete WAL replay; it may never silently alter which
/// budget nudge becomes due.
pub fn budget_replay_seed_is_valid(seed: &ProjectionSeed) -> bool {
    let Some(episode) = seed.admission_checkpoint.last_episode.as_ref() else {
        return false;
    };
    let Some(budget) = episode.config.budget.as_ref() else {
        return true;
    };
    let Some(state) = episode.budget_replay_state.as_ref() else {
        return false;
    };
    if seed.last_run_id.as_ref() != Some(&episode.run_id) || !state.is_valid_for(budget) {
        return false;
    }
    let max_fired_threshold = state
        .fired_nudge_indices
        .iter()
        .filter_map(|index| budget.nudges.get(*index))
        .map(|nudge| nudge.at_tool_calls)
        .chain(
            state
                .finalize_fired
                .then_some(budget.finalize_at_tool_calls),
        )
        .max();
    max_fired_threshold.is_none_or(|threshold| state.tool_call_count >= threshold)
}

fn invalid_budget_seed_error(seed: &ProjectionSeed) -> ReplayError {
    ReplayError::CorruptLineage {
        episode_run_id: seed
            .last_run_id
            .clone()
            .unwrap_or_else(|| RunId("unknown".into())),
        reason: "invalid budget replay seed".into(),
    }
}

/// Whether the first record with session meaning in `records` is `RunFinished{Interrupted}`:
/// the shape a zero-step seed leaves when its run was cut before any step committed, which
/// decides whether the materialized seed is discarded.
///
/// Informational records (a type from a newer build, attempt evidence) are skipped, per the
/// contract on `WalRecord::is_informational`. Every site asking this question calls THIS, not
/// its own `.first()`: the question used to be answered in three places, and two of them read
/// the literal first record, so an informational record at the head of a tail would have kept a
/// seed that replay discarded. That was unreachable at the time (attempt records only follow a
/// StepStarted, which clears the zero-step seed), but only because of where the watermark falls.
pub(crate) fn first_session_record_is_interrupted<'a>(
    records: impl IntoIterator<Item = &'a WalRecord>,
) -> bool {
    records
        .into_iter()
        .find(|record| !record.is_informational())
        .is_some_and(|record| {
            matches!(
                record,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Interrupted,
                    ..
                }
            )
        })
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
#[allow(clippy::too_many_arguments)]
pub(crate) fn replay_seeded<'a>(
    records: impl IntoIterator<Item = &'a WalRecord>,
    seed_origin: Option<broca_protocol::request::ModelHandle>,
    seed_run_id: Option<RunId>,
    seed_budget: Option<BudgetConfig>,
    seed_budget_state: Option<crate::BudgetReplayStateV1>,
    ordinal_base: u64,
    require_caller_text: bool,
    tag_seed: crate::tags::TagSeed,
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
    tag_seed: crate::tags::TagSeed,
) -> Result<ReplayState, ReplayError> {
    let (ordinal_base, require_caller_text) = position;
    // Tag numbers under the computed rules, given in the order the transcript renders: each
    // message as it is placed in the prompt, a run's rules from its `RunStarted` on.
    let mut numbering = crate::tags::TagNumbering::new(tag_seed, 0);
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
    // A tail can begin after the current episode's RunStarted. Only a RunStarted observed in
    // this walk may require a durable terminal before another episode opens.
    let mut saw_run_started = false;
    // The current episode's transient state — reset on each new `RunStarted`. Seed the origin
    // and durable budget facts for a tail whose RunStarted is in the materialized prefix.
    let mut seeded_nudge_indices = seed_budget_state
        .as_ref()
        .map_or_else(Vec::new, |state| state.fired_nudge_indices.clone());
    let mut ep = Episode {
        attributable: run_id.as_ref().is_some_and(|id| !id.0.is_empty()),
        seeded_last_ordinal: ordinal_base.checked_sub(1),
        origin: seed_origin,
        budget: seed_budget_state.unwrap_or_default(),
        ..Episode::default()
    };
    let mut episode_step_spans: Vec<std::ops::Range<usize>> = Vec::new();
    let mut render_episodes = Vec::new();
    let mut committed_tool_turns: Vec<(usize, usize)> = Vec::new();

    let mut input_budget = crate::input_budget::InputTokenBudget::default();
    for (record_index, rec) in records.into_iter().enumerate() {
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
        compaction.fold_at(rec, ordinal_base.saturating_add(prompt.len() as u64));
        if compaction_session || compaction.state().is_some() {
            compaction_history.fold(rec);
            if let crate::compaction::CompactionCheckpoint::State(state) = &mut compaction {
                state.history = compaction_history.clone();
            }
        }
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
                ep.seeded_prompt_start = matches!(
                    origin,
                    broca_wal::RunOrigin::SeededFrom {
                        origin_kind: broca_wal::SeedOriginKind::Import,
                        ..
                    }
                )
                .then_some(prompt.len());
                // Stamp this episode's assistant turns with the model that produced them,
                // so a later episode under a different model can strip a cross-model
                // signature at render. From the durable per-episode config → reproduces on
                // resume.
                ep.origin = Some(config.model.clone());
                marker_order_version = *version;
                keep_warm = *policy;
                on_restart = *on_restart_policy;
                run_config = Some((**config).clone());
                run_id = Some(rid.clone());
                cancel_at = None;
                seeded_budget = None;
                saw_run_started = true;
                render_episodes.push(broca_protocol::request::EpisodeRenderInput {
                    prompt_start: prompt.len(),
                    first_user: input
                        .iter()
                        .position(|m| m.role == Role::User)
                        .map(|i| prompt.len() + i),
                    frozen_config: config.frozen_render_config.clone(),
                });
                let input_tags = rec.runner_tags();
                let idle_delivery = submission_id
                    .as_ref()
                    .is_some_and(|id| ordered_submissions.remove(id));
                prompt.extend(input.iter().enumerate().map(|(index, message)| {
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
                }));
            }
            WalRecord::ModelStepFinished {
                step_id,
                assistant_message,
                usage: u,
                finish_reason,
                retries_used,
                tags: step_tags,
                ..
            } => {
                let step_tags = step_tags.as_deref();
                ep.usage.accumulate(u);
                ep.retries_used = ep.retries_used.saturating_add(*retries_used);
                ep.last_step_usage = Some(*u);
                ep.last_step_finish_reason = *finish_reason;
                ep.completed_steps += 1;
                ep.natural_stop_reason = None;
                let was_finalize_turn = ep.budget.finalize_pending;
                crate::budget_kernel::apply(
                    crate::budget_kernel::State {
                        facts: &mut ep.budget,
                        config: None,
                        run_id: run_id.as_ref(),
                    },
                    rec,
                )
                .expect("model steps cannot invalidate budget facts");
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
                            cache_prefix_blocks: None,
                            role: Role::Assistant,
                            content: assistant_message
                                .content
                                .iter()
                                .filter(|block| !matches!(block, ContentBlock::ToolCall(_)))
                                .cloned()
                                .collect(),
                            origin: ep.origin.clone(),
                            ordinal: None,
                            tags: Default::default(),
                        },
                        kept_tags.as_ref(),
                    ));
                    ep.open_assistant = None;
                    ep.open_batch = None;
                    ep.natural_end = true;
                    ep.natural_end_extendable = false;
                } else if assistant_message.tool_calls().next().is_some() {
                    ep.open_assistant = Some(assistant_message.clone()); // tool turn — held
                    ep.open_assistant_tags = step_tags.cloned();
                    ep.open_step = Some(*step_id);
                    ep.open_step_record = Some(record_index);
                    ep.open_batch_id = None; // a new tool turn; its batch hasn't opened yet
                    ep.natural_end = false;
                } else {
                    // Terminal turn (no tools) — commit immediately. This step ends
                    // the loop, so the episode ended naturally here.
                    ep.first_step_message.get_or_insert(prompt.len());
                    prompt.push(tagged(
                        Message {
                            cache_prefix_blocks: None,
                            role: Role::Assistant,
                            content: assistant_message.content.clone(),
                            origin: ep.origin.clone(),
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
            WalRecord::ToolBatchStarted {
                batch_id,
                step_id,
                planned_calls,
            } => {
                ep.open_batch = Some(OpenBatch::new(*step_id, planned_calls.clone()));
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
                    if b.all_terminated() {
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
                                ep.origin.clone(),
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
                ep.pending_steers.extend(
                    input
                        .iter()
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
                input,
                tool_call_id,
                appended_text,
                mode,
                ..
            } => {
                ordered_submissions.remove(submission_id);
                if let Some(input) = input {
                    let delivered_tags = rec.runner_tags();
                    ep.pending_steers
                        .extend(input.iter().enumerate().map(|(index, message)| {
                            let mut message = tagged(message.clone(), delivered_tags.get(index));
                            message
                                .tags
                                .set_delivery(broca_protocol::tags::DeliveryAttribution {
                                    mode: *mode,
                                    tool_call_id: None,
                                });
                            message
                        }));
                }
                if let (Some(tool_call_id), Some(text)) = (tool_call_id, appended_text) {
                    crate::delivery_order::apply_to_prompt(&mut prompt, tool_call_id, text);
                    if let Some(index) = prompt.iter().rposition(|message| {
                        message.content.iter().any(|block| matches!(block,
                            ContentBlock::ToolResult(result) if &result.tool_call_id == tool_call_id))
                    }) {
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
                ordinal,
                caller_text,
                ..
            } => {
                let budget = run_config
                    .as_ref()
                    .and_then(|config| config.budget.as_ref())
                    .or(seeded_budget.as_ref());
                let Some(budget) = budget else {
                    // Read-only transcript assembly has no checkpoint budget seed. Preserve its
                    // established best-effort behavior; admission and projection fail closed.
                    continue;
                };
                let expected_ordinal = ordinal_base.saturating_add(prompt.len() as u64);
                if *ordinal != expected_ordinal {
                    return Err(ReplayError::CorruptLineage {
                        episode_run_id: fired_run_id.clone(),
                        reason: format!(
                            "budget message ordinal {ordinal} does not match prompt position {expected_ordinal}"
                        ),
                    });
                }

                let expected_text = crate::budget_kernel::decide(&ep.budget, budget)
                    .first()
                    .map(|due| due.text.clone());
                crate::budget_kernel::apply(
                    crate::budget_kernel::State {
                        facts: &mut ep.budget,
                        config: Some(budget),
                        run_id: run_id.as_ref(),
                    },
                    rec,
                )
                .map_err(|reason| ReplayError::CorruptLineage {
                    episode_run_id: fired_run_id.clone(),
                    reason,
                })?;
                let expected_text = expected_text.expect("accepted firing was due");
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
                    Message::new(Role::User, vec![ContentBlock::Text { text }]),
                    rec.runner_tags().first(),
                ));
            }
            WalRecord::RunFinished {
                reason,
                retries_used,
                ..
            } => {
                if ep.completed_steps == 0 && *reason == RunFinishReason::Interrupted {
                    if let Some(start) = ep.seeded_prompt_start {
                        prompt.truncate(start);
                    }
                }
                // A steer no later step rendered stays in the transcript exactly once, at the
                // episode's close, after anything the terminal's barrier committed.
                ep.place_steers(&mut prompt);
                ep.finished = Some(*reason);
                ep.retries_used = *retries_used;
                // A terminal SEALS any earlier pause in this episode (e.g. a cancel of a
                // paused-for-auth run writes RunPaused then RunFinished{Interrupted}) —
                // last-writer-wins, so the run is terminal, not still paused.
                ep.paused = None;
            }
            // A non-terminal pause (auth-required) — distinct from a terminal
            // RunFinished. Clears any earlier finished in this episode (last-writer-wins),
            // though in practice a pause is the last record of its episode.
            WalRecord::RunPaused {
                run_id: rid,
                reason,
                error,
                ..
            } => {
                ep.paused = Some((rid.clone(), *reason, error.clone()));
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
                ep.provider_call_started = true;
                // Pending steers are rendered by this step, after everything before it.
                ep.place_steers(&mut prompt);
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
            WalRecord::Unknown => {}
            // Compaction is folded separately above; the transcript remains append-only.
            WalRecord::CompactionRequest { .. }
            | WalRecord::CompactionMessage { .. }
            | WalRecord::CompactionAnswer { .. }
            | WalRecord::CompactionWait { .. }
            | WalRecord::CompactionViewApplied { .. }
            | WalRecord::ContextCeilingWait { .. }
            | WalRecord::ContextCeilingDecided { .. }
            | WalRecord::CompactionNotice { .. } => {}
            // Provider attempt evidence: known to this build, but informational only under the
            // same contract, so replay skips it exactly as it skips `Unknown`. The step's retry
            // count still comes from `ModelStepFinished.retries_used`.
            WalRecord::ModelAttemptFinished { .. } => {}
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
            // These metadata records are omitted from prompt construction: they contain no messages.
            WalRecord::InputTokenCapFrozen { .. }
            | WalRecord::InputTokenBudgetExhausted { .. }
            | WalRecord::StructuredOutputResult { .. }
            | WalRecord::QuestionWithdrawalAcknowledged { .. } => {}
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
                    .and_then(|_| {
                        ordinal_base
                            .checked_add(prompt.len() as u64)?
                            .checked_sub(1)
                    })
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
    let tool_call_count = ep.budget.tool_call_count;
    // Checkpoints encode sorted indexes; the replay API retains its seed prefix followed
    // by new firings in threshold order. This is an output representation, not budget state.
    let mut tail_nudge_indices: Vec<_> = ep
        .budget
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
    let budget_finalize_fired = ep.budget.finalize_fired;
    let budget_finalize_pending = ep.budget.finalize_pending;
    let usage = ep.usage;
    let retries_used = ep.retries_used;
    let last_step_usage = ep.last_step_usage;
    let last_step_finish_reason = ep.last_step_finish_reason;
    let open_tool_batch = match (&ep.open_batch, &ep.open_batch_id) {
        (Some(batch), Some(batch_id)) => Some(OpenToolBatch {
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
                (
                    tagged(
                        Message {
                            cache_prefix_blocks: None,
                            role: Role::Assistant,
                            content: assistant.content.clone(),
                            origin: ep.origin.clone(),
                            ordinal: None,
                            tags: Default::default(),
                        },
                        ep.open_assistant_tags.as_ref(),
                    ),
                    step_record,
                )
            })
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
    let mut resolution = classify(ep);
    if input_budget.exhausted().is_some() {
        match &mut resolution {
            ReplayResolution::Resume {
                pending: Some(pending),
            }
            | ReplayResolution::AuthRequired {
                pending: Some(pending),
                ..
            } => {
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
    let tags_await_setup = saw_run_started
        && !run_finished_durable
        && compaction_session
        && compaction.state().is_none()
        && run_config
            .as_ref()
            .is_some_and(|config| config.render_tags && config.computed_tags.is_some());
    Ok(ReplayState {
        tags_await_setup,
        input_budget,
        cancel_at,
        message_runs,
        run_finals,
        compaction,
        compaction_history,
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
        open_tool_batch,
        pending_steers,
        resolution,
    })
}

/// Return the newest WAL sequence after `watermark` that a materialized store may
/// safely fold into its projection.
///
/// This helper is the single authority for projection boundaries so every store advances
/// its watermark under the same replay-domain rule. A `TurnFinished` is always safe. A
/// no-tool `ModelStepFinished` becomes safe only when a later `RunFinished` closes the same
/// episode; the boundary remains at the step so the terminal record stays in the WAL tail.
pub fn latest_projection_boundary(
    records: &[broca_wal::FramedRecord],
    watermark: u64,
) -> Option<u64> {
    let mut latest = None;
    let mut pending_no_tool_steps = Vec::new();
    let mut pending_zero_step_seed = None;
    // Whether a steered message has been written but not yet placed by a step boundary. A
    // boundary with a steer pending across it is never offered: the store would fold the
    // records before it without the steer, and a tail replay starting after it would never
    // see the steer's record, so the message would be lost from the materialized transcript.
    let mut steer_pending = false;

    for framed in records {
        match &framed.record {
            WalRecord::RunStarted { origin, .. } => {
                pending_no_tool_steps.clear();
                pending_zero_step_seed =
                    matches!(origin, broca_wal::RunOrigin::SeededFrom { .. }).then_some(framed.seq);
            }
            WalRecord::Steered { .. } | WalRecord::SendDelivered { input: Some(_), .. } => {
                steer_pending = true
            }
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } if assistant_message.tool_calls().next().is_none() => {
                pending_zero_step_seed = None;
                if !steer_pending {
                    pending_no_tool_steps.push(framed.seq);
                }
            }
            WalRecord::StepStarted { .. } => {
                pending_zero_step_seed = None;
                steer_pending = false;
            }
            WalRecord::ModelStepFinished { .. } => {
                pending_zero_step_seed = None;
            }
            WalRecord::RunFinished { .. } => {
                steer_pending = false;
                if let Some(seed_seq) = pending_zero_step_seed.take().filter(|seq| *seq > watermark)
                {
                    // Fold the imported prompt but leave RunFinished in the WAL tail so the
                    // store-merge path retains the episode's durable idle classification.
                    latest = Some(latest.map_or(seed_seq, |current: u64| current.max(seed_seq)));
                }
                for seq in pending_no_tool_steps.drain(..) {
                    if seq > watermark {
                        latest = Some(latest.map_or(seq, |current: u64| current.max(seq)));
                    }
                }
            }
            WalRecord::TurnFinished { .. } if !steer_pending && framed.seq > watermark => {
                latest = Some(latest.map_or(framed.seq, |current: u64| current.max(framed.seq)));
            }
            _ => {}
        }
    }

    latest
}

/// A completed-step usage fold for one run id. The same durable records feed
/// cold reconciliation and the live writer's small status projection.
#[derive(Debug, Clone)]
pub struct RunUsageProjection {
    run_id: RunId,
    active: bool,
    model: Option<(broca_protocol::request::ModelHandle, Option<String>)>,
    pub usage: broca_protocol::usage::RunUsage,
}

impl RunUsageProjection {
    pub fn new(run_id: RunId) -> Self {
        Self {
            run_id,
            active: false,
            model: None,
            usage: Default::default(),
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn observe(&mut self, record: &WalRecord) {
        match record {
            WalRecord::RunStarted { run_id, config, .. } => {
                self.active = run_id == &self.run_id;
                if self.active {
                    self.model = Some((config.model.clone(), config.variant.clone()));
                }
            }
            WalRecord::ModelStepFinished { usage, .. } if self.active => {
                if let Some((model, variant)) = &self.model {
                    let mut step_usage = cortexkit_role_llm_runner::usage::StepUsage::unreported();
                    if let Some(tokens) = usage.input_tokens {
                        step_usage = step_usage.with_input_tokens(tokens);
                    }
                    if let Some(tokens) = usage.cached_input_tokens {
                        step_usage = step_usage.with_cached_input_tokens(tokens);
                    }
                    if let Some(tokens) = usage.cache_write_tokens {
                        step_usage = step_usage.with_cache_write_tokens(tokens);
                    }
                    if let Some(tokens) = usage.output_tokens {
                        step_usage = step_usage.with_output_tokens(tokens);
                    }
                    if let Some(tokens) = usage.reasoning_tokens {
                        step_usage = step_usage.with_reasoning_tokens(tokens);
                    }
                    self.usage.record_step(
                        &model.provider_module_id,
                        &model.model_id,
                        variant.as_deref().filter(|value| !value.is_empty()),
                        step_usage,
                    );
                }
            }
            _ => {}
        }
    }
}

/// A run state reconciled from the WAL, including terminal metadata absent from the index.
///
/// Not `Eq`: `ProviderError` carries an `f64` retry hint.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconciledRunState {
    pub budget_detail: Option<broca_protocol::BudgetExhaustedDetail>,
    pub context_ceiling: Option<broca_protocol::ContextCeilingDetail>,
    pub ceiling_deadline_ms: Option<u64>,
    /// On an ended run admitted with structured output: what it produced, as `run.result` and
    /// `run.status` report it. Read from the run's durable result record when it completed;
    /// derived as `absent` from its frozen settings when it ended any other way.
    pub structured_output: Option<broca_protocol::structured_output::StructuredOutputResult>,
    /// Only the live actor fills this; WAL reconciliation never guesses an in-flight step.
    pub live_step: Option<broca_protocol::provider::InFlightStep>,
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
    /// Authentication selection from this run's durable start, never from today's routing.
    pub auth_selection: Option<broca_wal::AuthSelection>,
    /// The caller's title recorded for this run: its `RunStarted`'s, or, for a queued send that
    /// never started, its `Queued` record's. `None` when the send carried none.
    pub title: Option<String>,
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
    // The structured-output settings the target run froze, and the result it recorded.
    let mut frozen_structured_output = None;
    let mut structured_output_result = None;
    // A queued send's submission id is `sub-<run id>` (see the coordinator's `queue_submission`),
    // so its `Queued` record can name this run before any `RunStarted` does.
    let queued_submission = format!("sub-{run_id}");
    for rec in records {
        run_usage.observe(rec);
        match rec {
            WalRecord::RunStarted {
                run_id: rid,
                origin,
                config,
                title: started_title,
                ..
            } => {
                if rid.0 == run_id {
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
                        budget_detail: None,
                        context_ceiling: None,
                        ceiling_deadline_ms: None,
                        structured_output: None,
                        live_step: None,
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
                usage,
                finish_reason,
                retries_used,
                ..
            } => {
                active_zero_step_seed = false;
                has_completed_step = true;
                total_usage.accumulate(usage);
                total_retries_used = total_retries_used.saturating_add(*retries_used);
                last_step_finish_reason = *finish_reason;
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
                    // A completed run recorded its result in the commit that ended it. Any
                    // other end produced no value, which the frozen settings report as absent.
                    let structured_output = structured_output_result.clone().or_else(|| {
                        frozen_structured_output.as_ref().map(|frozen| {
                            broca_protocol::structured_output::result(
                                frozen,
                                false,
                                None,
                                None,
                                Vec::new(),
                            )
                        })
                    });
                    found = Some(ReconciledRunState {
                        budget_detail: budget_detail.clone(),
                        context_ceiling: context_ceiling.clone(),
                        ceiling_deadline_ms: None,
                        structured_output,
                        live_step: None,
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
                    });
                }
            }
            WalRecord::RunPaused {
                run_id: rid,
                reason,
                error,
                ..
            } if rid.0 == run_id => {
                found = Some(ReconciledRunState {
                    budget_detail: None,
                    context_ceiling: None,
                    ceiling_deadline_ms: None,
                    structured_output: None,
                    live_step: None,
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
                    budget_detail: None,
                    context_ceiling: None,
                    ceiling_deadline_ms: None,
                    structured_output: None,
                    live_step: None,
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
                });
            }
            _ => {}
        }
    }
    if active_run.as_deref() == Some(run_id) && active_zero_step_seed {
        return Some(ReconciledRunState {
            budget_detail: None,
            context_ceiling: None,
            ceiling_deadline_ms: None,
            structured_output: None,
            live_step: None,
            state: RunIndexState::Interrupted,
            undelivered_steers: Some(pending_steer_ids.clone()),
            final_step_finish_reason: None,
            indeterminate_tool_calls: Some(Vec::new()),
            usage: None,
            run_usage: run_usage.usage,
            retries_used: 0,
            error: None,
            pause_reason: None,
            auth_selection: None,
            title,
        });
    }
    found.map(|state| ReconciledRunState {
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
fn classify(ep: Episode) -> ReplayResolution {
    // A non-terminal pause is resumable, not finished — check it FIRST so a paused
    // episode is never read as terminal (which would seal a resumable run).
    if ep.paused.is_some() && ep.indeterminate.is_empty() {
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
        cache_prefix_blocks: None,
        role: Role::Tool,
        content,
        origin: None,
        ordinal: None,
        tags: Default::default(),
    });
    crate::tags::attach_parts(prompt.last_mut().expect("just pushed"), batch.result_tags());
}

#[cfg(test)]
mod steer_tests;

#[cfg(test)]
pub(crate) mod projection_boundary_tests {
    use broca_protocol::{
        caller::{PauseReason, RunFinishReason},
        content::{AssistantMessage, ContentBlock, FinishReason},
        error::{ErrorClass, ProviderError},
        request::{BudgetConfig, BudgetKind, BudgetNudge, ModelHandle},
        BindIdentity, MessageId, RunId, StepId, SubmissionId, Usage,
    };
    use broca_wal::{FramedRecord, QueuedAdmissionFailure, RunConfig, RunOrigin, WalRecord};

    use super::{
        budget_replay_seed_is_valid, latest_projection_boundary, replay_projection_tail,
        ProjectionSeed,
    };

    fn framed(seq: u64, record: WalRecord) -> FramedRecord {
        FramedRecord {
            seq,
            fence: 1,
            record,
        }
    }

    fn turn_finished(seq: u64, step_id: u64) -> FramedRecord {
        framed(
            seq,
            WalRecord::TurnFinished {
                step_id: StepId(step_id),
            },
        )
    }

    fn no_tool_step(seq: u64, step_id: u64) -> FramedRecord {
        no_tool_step_with_usage(seq, step_id, Usage::default())
    }

    fn no_tool_step_with_usage(seq: u64, step_id: u64, usage: Usage) -> FramedRecord {
        framed(
            seq,
            WalRecord::ModelStepFinished {
                tags: None,
                hooks: None,
                step_id: StepId(step_id),
                assistant_message: (AssistantMessage {
                    message_id: MessageId(format!("m{step_id}")),
                    content: vec![ContentBlock::Text {
                        text: "done".into(),
                    }],
                })
                .into(),
                usage,
                finish_reason: None,
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
        )
    }

    #[test]
    fn paused_reconciliation_retains_only_the_requested_runs_frozen_selection() {
        let mut first = run_started(1, "first").record;
        let selection = broca_wal::AuthSelection {
            credential_id: "frozen-key".into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        };
        if let WalRecord::RunStarted { config, .. } = &mut first {
            config.auth_selection = Some(selection.clone());
        }
        for reason in [PauseReason::Restart, PauseReason::AuthRequired] {
            let pause = WalRecord::RunPaused {
                run_id: RunId("first".into()),
                ts_ms: None,
                reason,
                error: None,
            };
            let records = vec![first.clone(), pause, run_started(3, "second").record];
            assert_eq!(
                super::reconcile_run_state(&records, "first")
                    .unwrap()
                    .auth_selection,
                Some(selection.clone())
            );
            assert!(super::reconcile_run_state(&records, "second")
                .unwrap()
                .auth_selection
                .is_none());
        }
    }

    pub(crate) fn run_started(seq: u64, run_id: &str) -> FramedRecord {
        framed(
            seq,
            WalRecord::RunStarted {
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
                run_id: RunId(run_id.into()),
                session: BindIdentity::new("/tmp/project", "runner", "session"),
                config: Box::new(RunConfig {
                    turn_directives_v1: None,
                    render_tags: false,
                    computed_tags: None,
                    model: ModelHandle::new("stub", "m"),
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
                input: vec![].into(),
                origin: RunOrigin::Fresh,
                submission_id: None,
            },
        )
    }

    fn run_finished(seq: u64) -> FramedRecord {
        framed(
            seq,
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        )
    }

    fn projection_seed() -> ProjectionSeed {
        let WalRecord::RunStarted { config, .. } = run_started(0, "r1").record else {
            unreachable!("fixture starts a run")
        };
        ProjectionSeed {
            deferred_calls: None,
            watermark_seq: 8,
            anchor: None,
            prefix_len: 3,
            completed_steps: 2,
            retries_used: 0,
            usage: Usage {
                input_tokens: Some(20),
                output_tokens: Some(4),
                ..Usage::default()
            },
            last_step_usage: Some(Usage {
                input_tokens: Some(10),
                output_tokens: Some(2),
                ..Usage::default()
            }),
            last_step_finish_reason: None,
            trailing_origin: Some(ModelHandle::new("stub", "old-model")),
            last_run_id: Some(RunId("r1".into())),
            admission_checkpoint: crate::AdmissionCheckpointV1 {
                has_deferred_calls: None,
                tags: None,
                pending_order: None,
                input_budget: Some(crate::input_budget::InputTokenBudget::default()),
                session_class: None,
                lineage_retention: None,
                compaction: None,
                effort_history: None,
                ownership: Some(crate::SessionOwnershipV1::default()),
                at_seq: 8,
                last_episode: Some(crate::LastEpisodeV1 {
                    cancel_at: None,
                    on_restart: None,
                    pause_reason: None,
                    keep_warm: None,
                    marker_order_version: None,
                    request_identity_digest: None,
                    run_id: RunId("r1".into()),
                    config: *config,
                    input: Vec::new(),
                    provider_call_started: true,
                    config_refrozen: false,
                    disposition: crate::EpisodeDisposition::Finished,
                    usage: Some(Usage {
                        input_tokens: Some(20),
                        output_tokens: Some(4),
                        ..Usage::default()
                    }),
                    retries_used: 0,
                    budget_replay_state: None,
                }),
                pending_fifo: std::sync::Arc::new(Vec::new()),
            },
            tag_seed: Default::default(),
        }
    }

    #[test]
    fn budgeted_seeded_tail_uses_full_lineage_ordinal_and_fire_time_text() {
        let mut seed = projection_seed();
        let episode = seed
            .admission_checkpoint
            .last_episode
            .as_mut()
            .expect("projection seed has an episode");
        episode.config.budget = Some(BudgetConfig {
            max_input_tokens: None,
            nudges: vec![BudgetNudge {
                at_tool_calls: 0,
                text: "exact fire-time text".into(),
            }],
            finalize_at_tool_calls: 9,
            finalize_text: "finalize".into(),
        });
        episode.budget_replay_state = Some(crate::BudgetReplayStateV1 {
            version: crate::BUDGET_REPLAY_STATE_VERSION,
            tool_call_count: 0,
            fired_nudge_indices: Vec::new(),
            finalize_fired: false,
            finalize_pending: false,
            budget_last_threshold: None,
        });
        let delta = replay_projection_tail(
            &seed,
            &[framed(
                9,
                WalRecord::BudgetFired {
                    tags: None,
                    run_id: RunId("r1".into()),
                    kind: BudgetKind::Nudge,
                    threshold: 0,
                    ordinal: 3,
                    caller_text: Some("exact fire-time text".into()),
                },
            )],
        )
        .expect("seeded budget tail replays");
        assert_eq!(
            delta.messages[0].content,
            vec![ContentBlock::Text {
                text: "exact fire-time text".into(),
            }]
        );
    }

    #[test]
    fn seeded_budget_state_never_replays_an_already_fired_nudge() {
        let mut seed = projection_seed();
        let episode = seed
            .admission_checkpoint
            .last_episode
            .as_mut()
            .expect("projection seed has an episode");
        episode.config.budget = Some(BudgetConfig {
            max_input_tokens: None,
            nudges: vec![
                BudgetNudge {
                    at_tool_calls: 1,
                    text: "already fired".into(),
                },
                BudgetNudge {
                    at_tool_calls: 1,
                    text: "not yet fired".into(),
                },
            ],
            finalize_at_tool_calls: 9,
            finalize_text: "finalize".into(),
        });
        episode.budget_replay_state = Some(crate::BudgetReplayStateV1 {
            version: crate::BUDGET_REPLAY_STATE_VERSION,
            tool_call_count: 1,
            fired_nudge_indices: vec![0],
            finalize_fired: false,
            finalize_pending: false,
            budget_last_threshold: Some(1),
        });
        let tail = [framed(
            9,
            WalRecord::BudgetFired {
                tags: None,
                run_id: RunId("r1".into()),
                kind: BudgetKind::Nudge,
                threshold: 1,
                ordinal: 3,
                caller_text: Some("not yet fired".into()),
            },
        )];
        let delta = replay_projection_tail(&seed, &tail).expect("replay only the remaining nudge");
        assert_eq!(
            delta
                .admission_checkpoint
                .last_episode
                .and_then(|episode| episode.budget_replay_state)
                .expect("budget state survives the tail")
                .fired_nudge_indices,
            vec![0, 1],
            "the tail WAL record is resolved as the not-yet-fired config index"
        );
    }

    #[test]
    fn malformed_default_budget_state_is_rejected_before_seeded_replay() {
        let mut seed = projection_seed();
        let episode = seed
            .admission_checkpoint
            .last_episode
            .as_mut()
            .expect("projection seed has an episode");
        episode.config.budget = Some(BudgetConfig {
            max_input_tokens: None,
            nudges: vec![BudgetNudge {
                at_tool_calls: 1,
                text: "nudge".into(),
            }],
            finalize_at_tool_calls: 9,
            finalize_text: "finalize".into(),
        });
        episode.budget_replay_state = Some(crate::BudgetReplayStateV1 {
            version: crate::BUDGET_REPLAY_STATE_VERSION,
            tool_call_count: 0,
            fired_nudge_indices: vec![0],
            finalize_fired: false,
            finalize_pending: false,
            budget_last_threshold: None,
        });
        assert!(
            !budget_replay_seed_is_valid(&seed),
            "a default-looking state that claims a firing must not seed a double-fire-prone replay"
        );
        assert!(replay_projection_tail(&seed, &[]).is_err());
    }

    #[test]
    fn projection_tail_continuing_episode_accumulates_seed_counters() {
        let tail_usage = Usage {
            input_tokens: Some(7),
            output_tokens: Some(2),
            ..Usage::default()
        };
        let record = no_tool_step_with_usage(9, 3, tail_usage);

        let delta = replay_projection_tail(&projection_seed(), &[record]).expect("replay");

        assert_eq!(delta.completed_steps, 3);
        assert_eq!(delta.usage.input_tokens, Some(27));
        assert_eq!(delta.usage.output_tokens, Some(6));
        assert_eq!(delta.last_run_id, Some(RunId("r1".into())));
        assert_eq!(
            delta.messages[0].origin,
            Some(ModelHandle::new("stub", "old-model"))
        );
    }

    #[test]
    fn projection_tail_legacy_none_finish_reason_overwrites_seed() {
        let mut seed = projection_seed();
        seed.last_step_finish_reason = Some(FinishReason::ToolCalls);
        let tail_usage = Usage {
            input_tokens: Some(7),
            output_tokens: Some(2),
            ..Usage::default()
        };

        let delta = replay_projection_tail(&seed, &[no_tool_step_with_usage(9, 3, tail_usage)])
            .expect("replay legacy-shaped step");

        assert_eq!(delta.last_step_usage, Some(tail_usage));
        assert_eq!(
            delta.last_step_finish_reason, None,
            "an explicit legacy None is an overwrite, not a missing tail update"
        );
    }

    #[test]
    fn projection_tail_failed_retraction_clears_seed_finish_reason() {
        let mut seed = projection_seed();
        seed.last_step_finish_reason = Some(FinishReason::ToolCalls);
        let retracted = framed(
            9,
            WalRecord::Retracted {
                submission_id: SubmissionId("sub-r2".into()),
                failure: Some(QueuedAdmissionFailure {
                    run_id: RunId("r2".into()),
                    error: ProviderError {
                        provider_details: None,
                        provider_subcode: None,
                        refusal: None,
                        class: ErrorClass::Permanent,
                        message: "unknown model".into(),
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
            },
        );

        let delta = replay_projection_tail(&seed, &[retracted]).expect("replay tombstone");

        assert_eq!(delta.last_step_finish_reason, None);
        assert_eq!(delta.last_step_usage, seed.last_step_usage);
    }

    /// A queued submission that failed admission never ran, so its tombstone is the only
    /// record of why; run-status reconciliation must carry that error rather than only use it
    /// to recognise the run.
    #[test]
    fn reconciled_failed_retraction_carries_its_error() {
        let error = ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Permanent,
            message: "unknown model".into(),
            status: None,
            retry_after_secs: None,
            provider_code: Some("queued_admission_failed".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        let records = [WalRecord::Retracted {
            submission_id: SubmissionId("sub-r2".into()),
            failure: Some(QueuedAdmissionFailure {
                run_id: RunId("r2".into()),
                error: error.clone(),
            }),
        }];

        let state = super::reconcile_run_state(&records, "r2").expect("retracted run resolves");

        assert_eq!(state.state, crate::RunIndexState::Error);
        assert_eq!(state.error, Some(error));
        assert_eq!(state.pause_reason, None);
    }

    #[test]
    fn projection_tail_opening_episode_resets_seed_counters() {
        let tail_usage = Usage {
            input_tokens: Some(7),
            output_tokens: Some(2),
            ..Usage::default()
        };
        let started = run_started(9, "r2");
        let step = no_tool_step_with_usage(10, 1, tail_usage);

        let delta = replay_projection_tail(&projection_seed(), &[started, step]).expect("replay");

        assert_eq!(delta.completed_steps, 1);
        assert_eq!(delta.usage, tail_usage);
        assert_eq!(delta.last_run_id, Some(RunId("r2".into())));
        assert_eq!(
            delta.messages[0].origin,
            Some(ModelHandle::new("stub", "m")),
            "the new episode model overrides the trailing seed origin"
        );
    }

    #[test]
    fn only_turn_finished_is_projectable() {
        let records = vec![
            framed(1, WalRecord::StepStarted { step_id: StepId(1) }),
            turn_finished(2, 1),
            framed(
                3,
                WalRecord::RunFinished {
                    interrupt_reason: None,
                    ts_ms: None,
                    reason: RunFinishReason::Completed,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            ),
        ];

        assert_eq!(latest_projection_boundary(&records, 0), Some(2));
    }

    #[test]
    fn boundary_is_reported_at_turn_finished_sequence() {
        let records = vec![turn_finished(4, 1), turn_finished(8, 2)];

        assert_eq!(latest_projection_boundary(&records, 4), Some(8));
        assert_eq!(latest_projection_boundary(&records, 8), None);
    }

    #[test]
    fn multi_turn_batch_uses_latest_boundary() {
        let records = vec![
            turn_finished(3, 1),
            turn_finished(6, 2),
            turn_finished(10, 3),
        ];

        assert_eq!(latest_projection_boundary(&records, 0), Some(10));
    }

    #[test]
    fn records_without_boundary_are_not_projectable() {
        let records = vec![
            framed(1, WalRecord::StepStarted { step_id: StepId(1) }),
            run_finished(2),
        ];

        assert_eq!(latest_projection_boundary(&records, 0), None);
    }

    #[test]
    fn finished_episode_projects_no_tool_step_at_step_sequence() {
        let records = vec![no_tool_step(4, 1), run_finished(7)];

        assert_eq!(latest_projection_boundary(&records, 0), Some(4));
        assert_eq!(latest_projection_boundary(&records, 4), None);
    }

    #[test]
    fn no_tool_step_without_run_finished_is_not_projectable() {
        assert_eq!(latest_projection_boundary(&[no_tool_step(4, 1)], 0), None);
    }

    #[test]
    fn run_paused_does_not_license_no_tool_projection() {
        let records = vec![
            no_tool_step(4, 1),
            framed(
                5,
                WalRecord::RunPaused {
                    run_id: RunId("r1".into()),
                    ts_ms: None,
                    reason: PauseReason::AuthRequired,
                    error: None,
                },
            ),
        ];

        assert_eq!(
            latest_projection_boundary(&records, 0),
            None,
            "a paused run re-reads its no-tool step from the WAL tail on resume"
        );
    }

    #[test]
    fn later_episode_run_finished_does_not_license_earlier_dangling_step() {
        let records = vec![no_tool_step(2, 1), run_started(3, "r2"), run_finished(4)];

        assert_eq!(
            latest_projection_boundary(&records, 0),
            None,
            "RunStarted prevents a later episode's terminal from licensing the dangling step"
        );
    }
}

#[cfg(test)]
pub(crate) mod deferred_tests {
    use super::*;
    use broca_protocol::{content::ToolCall, MessageId};
    use broca_wal::{DeferredAsk, DeferredExecutor, OnExpiry};

    pub(crate) fn prepared(id: &str, batch: &str) -> WalRecord {
        WalRecord::ToolCallDeferred {
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

    pub(crate) fn result(id: &str, text: &str) -> Arc<ToolResult> {
        Arc::new(ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: text.into() },
            is_error: false,
        })
    }

    pub(crate) fn approval(id: &str, key: &str, decision: DeferredDecision) -> WalRecord {
        WalRecord::ApprovalRecorded {
            tags: None,
            call_key: key.into(),
            call_id: ToolCallId(id.into()),
            elicitation_id: Some(format!("q-{id}")),
            decision,
            approval_digest: (decision == DeferredDecision::Approved).then(|| "digest".into()),
            closing_result: (decision != DeferredDecision::Approved).then(|| {
                result(
                    id,
                    match decision {
                        DeferredDecision::Declined => "pre_tool_declined",
                        DeferredDecision::Expired => "pre_tool_expired",
                        DeferredDecision::Unavailable => "pre_tool_unavailable",
                        _ => unreachable!(),
                    },
                )
            }),
        }
    }

    pub(crate) fn batch(ids: &[&str]) -> Vec<WalRecord> {
        vec![
            super::projection_boundary_tests::run_started(1, "r").record,
            WalRecord::ModelStepFinished {
                tags: None,
                hooks: None,
                step_id: StepId(1),
                assistant_message: Arc::new(AssistantMessage {
                    message_id: MessageId("m".into()),
                    content: ids
                        .iter()
                        .map(|id| {
                            ContentBlock::ToolCall(ToolCall {
                                thought_signature: None,
                                tool_call_id: ToolCallId((*id).into()),
                                tool_name: "edit".into(),
                                input: serde_json::json!({"n":1}),
                            })
                        })
                        .collect(),
                }),
                usage: Usage::default(),
                finish_reason: Some(FinishReason::ToolCalls),
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::ToolBatchStarted {
                batch_id: BatchId("b".into()),
                step_id: StepId(1),
                planned_calls: ids.iter().map(|id| ToolCallId((*id).into())).collect(),
            },
        ]
    }

    pub(crate) fn terminal(id: &str, withdrawal: Option<WithdrawElicitation>) -> WalRecord {
        WalRecord::ToolResult {
            tags: None,
            hooks: None,
            batch_id: BatchId("b".into()),
            result: Arc::new(crate::resume::not_dispatched_result(&ToolCallId(id.into()))),
            outcome: DispatchOutcome::Interrupted,
            reason: None,
            withdraw_elicitation: withdrawal,
            provider_held: None,
        }
    }

    pub(crate) fn finished() -> Vec<WalRecord> {
        vec![
            WalRecord::ToolBatchFinished {
                batch_id: BatchId("b".into()),
            },
            WalRecord::TurnFinished { step_id: StepId(1) },
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: Some(1),
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ]
    }

    pub(crate) fn marker(ids: &[&str]) -> WalRecord {
        WalRecord::AwaitingApproval {
            batch_id: BatchId("b".into()),
            pending: ids
                .iter()
                .map(|id| broca_wal::AwaitingApprovalEntry {
                    call_key: format!("key-{id}"),
                    call_id: ToolCallId((*id).into()),
                    elicitation_id: Some(format!("q-{id}")),
                    expires_at_ms: 1000,
                })
                .collect(),
        }
    }

    #[test]
    fn prepared_and_approved_are_never_redispatchable() {
        let mut records = batch(&["deferred", "approved", "fresh"]);
        records.extend([
            prepared("deferred", "b"),
            prepared("approved", "b"),
            approval("approved", "key-approved", DeferredDecision::Approved),
            marker(&["deferred", "approved"]),
        ]);
        let state = replay(&records).unwrap();
        assert!(state.open_tool_turn.is_some());
        assert_eq!(
            state
                .open_tool_batch
                .unwrap()
                .calls
                .iter()
                .map(|(_, state)| *state)
                .collect::<Vec<_>>(),
            [
                OpenCallState::Deferred,
                OpenCallState::Approved,
                OpenCallState::NotSent
            ]
        );
        let ReplayResolution::Resume {
            pending: Some(pending),
        } = state.resolution
        else {
            panic!("pending batch missing")
        };
        assert_eq!(pending.redispatch, [ToolCallId("fresh".into())]);
        assert_eq!(pending.deferred.len(), 1);
        assert_eq!(pending.approved.len(), 1);
        assert_eq!(pending.deferred[0].record_index, 3);
        assert_eq!(*pending.deferred[0].prepared, records[3]);
        assert_eq!(
            pending.deferred[0].elicitation_id.as_deref(),
            Some("q-deferred")
        );
        assert_eq!(pending.approved[0].approval.as_deref(), Some(&records[5]));
    }

    #[test]
    fn restart_pause_carries_prepared_calls_and_terminal_siblings() {
        let mut records = batch(&["deferred", "approved", "done"]);
        records.extend([
            prepared("deferred", "b"),
            prepared("approved", "b"),
            approval("approved", "key-approved", DeferredDecision::Approved),
            terminal("done", None),
            WalRecord::RunPaused {
                run_id: RunId("r".into()),
                ts_ms: Some(1),
                reason: PauseReason::Restart,
                error: None,
            },
        ]);
        let ReplayResolution::AuthRequired {
            pending: Some(pending),
            reason: PauseReason::Restart,
            ..
        } = replay(&records).unwrap().resolution
        else {
            panic!("restart lost its pending batch")
        };
        assert_eq!(pending.batch_id, Some(BatchId("b".into())));
        assert_eq!(pending.deferred.len(), 1);
        assert_eq!(pending.approved.len(), 1);
        assert_eq!(pending.existing.len(), 1);
        assert_eq!(pending.planned.len(), 3);
    }

    #[test]
    fn approval_terminals_and_withdrawal_commit_in_plan_order_despite_stale_marker() {
        let mut records = batch(&["declined", "expired", "unavailable", "withdrawn"]);
        for id in ["declined", "expired", "unavailable", "withdrawn"] {
            records.push(prepared(id, "b"));
        }
        records.extend([
            approval("expired", "key-expired", DeferredDecision::Expired),
            terminal(
                "withdrawn",
                Some(WithdrawElicitation {
                    call_key: "key-withdrawn".into(),
                    elicitation_id: None,
                }),
            ),
            approval(
                "unavailable",
                "key-unavailable",
                DeferredDecision::Unavailable,
            ),
            approval("declined", "key-declined", DeferredDecision::Declined),
            marker(&["declined", "expired", "unavailable", "withdrawn"]),
        ]);
        let state = replay(&records).unwrap();
        let ReplayResolution::Resume {
            pending: Some(pending),
        } = state.resolution
        else {
            panic!("pending missing")
        };
        assert!(
            pending.deferred.is_empty()
                && pending.approved.is_empty()
                && pending.redispatch.is_empty()
        );
        assert_eq!(
            pending
                .existing
                .iter()
                .map(|(id, _)| id.0.as_str())
                .collect::<Vec<_>>(),
            ["declined", "expired", "unavailable", "withdrawn"]
        );
        records.extend(finished());
        let state = replay(&records).unwrap();
        assert_eq!(state.prompt.len(), 2);
        assert_eq!(
            state.prompt[1].content,
            vec![
                ContentBlock::ToolResult((*result("declined", "pre_tool_declined")).clone()),
                ContentBlock::ToolResult((*result("expired", "pre_tool_expired")).clone()),
                ContentBlock::ToolResult((*result("unavailable", "pre_tool_unavailable")).clone()),
                ContentBlock::ToolResult(crate::resume::not_dispatched_result(&ToolCallId(
                    "withdrawn".into()
                ))),
            ]
        );
    }

    #[test]
    fn approved_dispatch_without_result_remains_indeterminate() {
        let mut records = batch(&["approved"]);
        records.extend([
            prepared("approved", "b"),
            approval("approved", "key-approved", DeferredDecision::Approved),
            WalRecord::ToolDispatchIntent {
                hooks: None,
                batch_id: BatchId("b".into()),
                tool_call_id: ToolCallId("approved".into()),
                tool_name: Some("edit".into()),
                args: Arc::new(serde_json::json!({"n":1})),
                module_id: None,
                schema_pin: None,
                approval_digest: Some("digest".into()),
                call_key: Some("key-approved".into()),
            },
        ]);
        let state = replay(&records).unwrap();
        assert_eq!(
            state.open_tool_batch.unwrap().calls[0].1,
            OpenCallState::Dispatched
        );
        assert_eq!(
            state.resolution,
            ReplayResolution::Interrupted {
                indeterminate: vec![ToolCallId("approved".into())]
            }
        );
    }

    #[test]
    fn late_records_leave_finished_prompt_byte_identical() {
        let mut records = batch(&["expired"]);
        records.extend([
            prepared("expired", "b"),
            approval("expired", "key-expired", DeferredDecision::Expired),
        ]);
        records.extend(finished());
        let before = serde_json::to_vec(&replay(&records).unwrap().prompt).unwrap();
        for late in [
            WalRecord::LateApprovalRecorded {
                call_key: "key-expired".into(),
                elicitation_id: "q-expired".into(),
                decision: LateDecision::Approved,
                approval_digest: Some("digest".into()),
                late_execution: Some(LateExecution::Execute),
            },
            WalRecord::LateDispatchIntent {
                call_key: "key-expired".into(),
                approval_digest: "digest".into(),
                batch_id: BatchId("late:key-expired".into()),
            },
            WalRecord::LateResult {
                call_key: "key-expired".into(),
                outcome: broca_wal::LateOutcome::Completed,
                result: result("expired", "late output"),
            },
        ] {
            records.push(late);
            assert_eq!(
                serde_json::to_vec(&replay(&records).unwrap().prompt).unwrap(),
                before
            );
        }
    }
}
