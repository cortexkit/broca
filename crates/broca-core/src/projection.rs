//! The control-stream projection: durable WAL records → the ordered control units a
//! subscriber sees, each addressed by a durable cursor.
//!
//! This is a DIFFERENT output than [`crate::replay`]: replay produces the RESUME state
//! (one prompt + a resolution for the loop); projection produces the STREAM (an ordered
//! list of control units for subscribers). They read the same WAL but answer different
//! questions, so they're separate modules — and the two POSITIONS they use are kept
//! apart by type (see [`Cursor`] vs the store's materialization watermark).
//!
//! ## Determinism
//! `project_control` is a pure function of the records: the same records always produce
//! the same `(Cursor, ControlUnit)` sequence. A record can expand to SEVERAL ordered
//! units (e.g. a model step → an assistant message then a step-finished), disambiguated
//! by `sub_index`. `sub_index` is a pure function of the record's own content and a
//! fixed per-record-type ordering — it NEVER depends on map iteration order or anything
//! ambient. Where multiple units come from a collection (a batch's tool calls), they are
//! ordered by the collection's own order (the `planned_calls` Vec / the assistant
//! message's content order), never a map. The render golden pins this.
//!
//! ## Two positions, never conflated
//! A [`Cursor`] is `(wal_seq, sub_index)`: a position in the lineage-global control
//! stream, monotonic over fsynced seqs, valid at ANY fsynced unit (including
//! mid-episode). It is what a subscriber holds and resubscribes with. It is DISTINCT
//! from the store's materialization watermark (a turn-boundary-anchored seq for resume):
//! different type, no implicit conversion. The projection is computed from a record
//! RANGE addressed by cursors, never "from the materialization watermark forward".

use std::sync::Arc;

use broca_protocol::{
    caller::{ControlUnit, RunFinishReason, SharedProviderError},
    content::ContentBlock,
    FinishReason, RunId, ToolCallId,
};
use broca_wal::{FramedRecord, WalRecord};

/// A durable position in the control stream: the WAL seq of the source record plus the
/// 0-based index of this unit WITHIN that record's expansion (one record can project to
/// several ordered units). Lineage-global and monotonic over fsynced seqs; it never
/// resets at an episode boundary (WAL seq is monotonic across the whole lineage file).
///
/// This is NOT the materialization watermark (which is a turn-boundary-anchored seq the
/// store uses for resume). Distinct type on purpose, so a code path can't pass one where
/// the other belongs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cursor {
    pub wal_seq: u64,
    pub sub_index: u32,
}

impl Cursor {
    pub fn new(wal_seq: u64, sub_index: u32) -> Self {
        Self { wal_seq, sub_index }
    }
}

/// How a `ControlUnit` variant relates to the projection — the completeness contract that
/// makes the WAL projection the SOLE source of the control stream.
///
/// When the coordinator's fan-out sources control from the projection (and ignores control
/// on the run's live `out` channel to avoid double-publishing), ANY control unit the loop
/// emits that is NOT projectable from a durable WAL record would be SILENTLY LOST from the
/// control stream — on both the live AND the resubscribe path (both use `project_control`).
/// So every control unit the loop emits MUST be `ControlProjectable`.
///
/// This classification is EXHAUSTIVE (no catch-all arm), so adding a new `ControlUnit`
/// variant is a COMPILE ERROR until it's deliberately classified — the completeness
/// invariant can't silently rot. The accompanying test asserts every variant the loop
/// actually emits is `ControlProjectable`.
#[derive(Debug, PartialEq, Eq)]
pub enum ControlClass {
    /// Emitted by the loop AND produced by `project_control` from a durable record — safe
    /// to source from the projection (the fan-out's reliable control stream).
    ControlProjectable,
    /// A display-lane / display-state concern, live-only and never projected (lossy). Not
    /// part of the reliable control stream.
    DisplayLiveOnly,
    /// Never emitted by the run loop to the caller's control stream (it belongs to a
    /// different surface — e.g. the approval round-trip), so it needs no projection.
    NotEmittedByLoop,
}

/// Classify a `ControlUnit` variant for the completeness contract above. EXHAUSTIVE on
/// purpose: a new variant won't compile until classified, forcing the "is this projectable
/// from a durable record?" decision rather than letting a new control unit silently fall
/// out of the projected stream.
pub fn classify_control_unit(unit: &ControlUnit) -> ControlClass {
    match unit {
        // Projected from durable records (see `project_control`):
        //   RunStarted→RunStarted, StepStarted→StepStarted,
        //   ModelStepFinished→{AssistantMessage, ToolCall*, StepFinished},
        //   ToolResult/ToolNoDispatch→ToolResult, RunFinished→{Error?, RunFinished},
        //   RunPaused→Paused.
        ControlUnit::RunStarted { .. }
        | ControlUnit::StepStarted { .. }
        | ControlUnit::BudgetFired { .. }
        | ControlUnit::AssistantMessage { .. }
        | ControlUnit::ToolCall { .. }
        | ControlUnit::ToolResult { .. }
        | ControlUnit::StepFinished { .. }
        | ControlUnit::Error { .. }
        | ControlUnit::Paused { .. }
        | ControlUnit::RunFinished { .. } => ControlClass::ControlProjectable,
        // Display-state discard: paired with a display-lane reset, never a reliable unit.
        ControlUnit::DisplayReset => ControlClass::DisplayLiveOnly,
        // The approval round-trip is a separate request/response surface (wired by the
        // coordinator), not the per-run control stream — the loop never emits it to `out`.
        ControlUnit::ApprovalRequest { .. } => ControlClass::NotEmittedByLoop,
    }
}

/// Project a range of durable WAL records into the ordered control units a subscriber
/// sees, each tagged with its durable [`Cursor`]. Pure + deterministic: the same records
/// always yield the same sequence. `records` are framed (carry their seq); only the
/// record CONTENT + the framed seq are read — never any external state — so the result
/// is identical whether produced live-side or after a coordinator re-open.
///
/// Display deltas are NOT projected: they are live-only and lossy (a finished turn shows
/// its assembled text, not re-typed tokens), so the control stream is semantically
/// equivalent to the live stream, not token-identical.
pub fn project_control(records: &[FramedRecord]) -> Vec<(Cursor, ControlUnit)> {
    project_control_with_state(records, &mut ControlProjectionState::default())
}

/// Episode-local tracking shared by control projection and run-status reconciliation. A dispatch
/// intent remains unresolved until a definite terminal arrives; an indeterminate terminal stays in
/// the reported set because the tool may already have applied its side effect.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct IndeterminateToolCalls {
    unresolved_dispatches: Vec<ToolCallId>,
    indeterminate_outcomes: Vec<ToolCallId>,
}

impl IndeterminateToolCalls {
    pub(crate) fn reset_episode(&mut self) {
        self.unresolved_dispatches.clear();
        self.indeterminate_outcomes.clear();
    }

    pub(crate) fn reset_batch(&mut self) {
        self.unresolved_dispatches.clear();
    }

    pub(crate) fn record_dispatch_intent(&mut self, tool_call_id: &ToolCallId) {
        if !self.unresolved_dispatches.contains(tool_call_id) {
            self.unresolved_dispatches.push(tool_call_id.clone());
        }
    }

    pub(crate) fn record_dispatch_terminal(
        &mut self,
        tool_call_id: &ToolCallId,
        indeterminate: bool,
    ) {
        self.unresolved_dispatches
            .retain(|pending| pending != tool_call_id);
        if indeterminate && !self.indeterminate_outcomes.contains(tool_call_id) {
            self.indeterminate_outcomes.push(tool_call_id.clone());
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<ToolCallId> {
        let mut calls = self.indeterminate_outcomes.clone();
        for tool_call_id in &self.unresolved_dispatches {
            if !calls.contains(tool_call_id) {
                calls.push(tool_call_id.clone());
            }
        }
        calls
    }
}

/// Episode-local context needed when records are projected incrementally.
///
/// Batch replay can construct this state internally, while the live projector keeps one instance
/// across observed records so a later `RunFinished` can describe its last model step and dispatches.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ControlProjectionState {
    /// The run this episode's records belong to, so the TERMINAL can name it.
    ///
    /// `RunFinished` is the only run-lifecycle control unit that did not carry its run,
    /// which left a subscriber attaching mid-stream unable to attribute it. The WAL
    /// record does not carry one either, so it is tracked here from `RunStarted`.
    ///
    /// Stays `None` when the state is restored from a persisted watermark: the
    /// `RunStarted` may sit before the watermark, and this projector does not read back
    /// to find it. `None` therefore means "this producer does not know", which is exactly
    /// what the optional wire field expresses -- not "the terminal has no run".
    run_id: Option<RunId>,
    final_step_finish_reason: Option<FinishReason>,
    has_completed_step: bool,
    indeterminate_tool_calls: IndeterminateToolCalls,
}

impl ControlProjectionState {
    /// Restore the episode-local fields needed to continue control projection from a persisted
    /// materialization watermark.
    pub fn from_completed_steps_and_final_step_finish_reason(
        completed_steps: u64,
        final_step_finish_reason: Option<FinishReason>,
        // THE SEED MUST SUPPLY THIS. A restored projection that left it `None` produced a
        // terminal naming no run, while a full replay of the SAME records produced one
        // naming it -- so two subscribers saw different terminals for the same run
        // depending on where they attached. Projection parity is the invariant the fanout
        // suite exists to hold, and it caught exactly that.
        run_id: Option<RunId>,
    ) -> Self {
        Self {
            run_id,
            final_step_finish_reason,
            has_completed_step: completed_steps > 0,
            indeterminate_tool_calls: IndeterminateToolCalls::default(),
        }
    }

    fn reset_episode(&mut self) {
        self.run_id = None;
        self.final_step_finish_reason = None;
        self.has_completed_step = false;
        self.indeterminate_tool_calls.reset_episode();
    }
}

/// Project records while retaining episode context across calls.
pub fn project_control_with_state(
    records: &[FramedRecord],
    state: &mut ControlProjectionState,
) -> Vec<(Cursor, ControlUnit)> {
    let mut sink = UnitSink::default();
    for framed in records {
        project_control_record(framed, state, &mut sink);
    }
    sink.units
}

/// Advance projection state and return the final cursor without constructing control payloads.
pub fn advance_control_projection_state(
    records: &[FramedRecord],
    state: &mut ControlProjectionState,
) -> Option<Cursor> {
    let mut sink = LastCursorSink::default();
    for framed in records {
        project_control_record(framed, state, &mut sink);
    }
    sink.last
}

/// Return the final projected cursor, optionally bounded to records at or before `through_seq`.
///
/// This is the allocation-free metadata counterpart to [`project_control`]. Both paths run the
/// same exhaustive per-record projection; the cursor-only sink deliberately leaves each lazy
/// `ControlUnit` constructor unevaluated, so payloads are neither built nor cloned.
pub fn last_control_cursor_through(
    records: &[FramedRecord],
    through_seq: Option<u64>,
) -> Option<Cursor> {
    let mut state = ControlProjectionState::default();
    let mut sink = LastCursorSink::default();
    for framed in records
        .iter()
        .filter(|framed| through_seq.is_none_or(|through| framed.seq <= through))
    {
        project_control_record(framed, &mut state, &mut sink);
    }
    sink.last
}

trait ControlProjectionSink {
    fn push<F>(&mut self, cursor: Cursor, make_unit: F)
    where
        F: FnOnce() -> ControlUnit;
}

struct RecordSink<'a, S> {
    sink: &'a mut S,
    seq: u64,
    next_sub_index: u32,
}

impl<'a, S: ControlProjectionSink> RecordSink<'a, S> {
    fn new(sink: &'a mut S, seq: u64) -> Self {
        Self {
            sink,
            seq,
            next_sub_index: 0,
        }
    }

    fn push<F>(&mut self, make_unit: F)
    where
        F: FnOnce() -> ControlUnit,
    {
        let cursor = Cursor::new(self.seq, self.next_sub_index);
        self.next_sub_index += 1;
        self.sink.push(cursor, make_unit);
    }
}

#[derive(Default)]
struct UnitSink {
    units: Vec<(Cursor, ControlUnit)>,
}

impl ControlProjectionSink for UnitSink {
    fn push<F>(&mut self, cursor: Cursor, make_unit: F)
    where
        F: FnOnce() -> ControlUnit,
    {
        self.units.push((cursor, make_unit()));
    }
}

#[derive(Default)]
struct LastCursorSink {
    last: Option<Cursor>,
}

impl ControlProjectionSink for LastCursorSink {
    fn push<F>(&mut self, cursor: Cursor, _make_unit: F)
    where
        F: FnOnce() -> ControlUnit,
    {
        self.last = Some(cursor);
    }
}

/// Expand one record into its cursor-addressed units. The full projection and metadata-only
/// cursor scan both use this single exhaustive match, so adding or removing an emitted unit
/// changes its `sub_index` in both paths at the same source site.
fn project_control_record<S: ControlProjectionSink>(
    framed: &FramedRecord,
    state: &mut ControlProjectionState,
    sink: &mut S,
) {
    let mut record_sink = RecordSink::new(sink, framed.seq);
    macro_rules! push {
        ($unit:expr) => {
            record_sink.push(|| $unit)
        };
    }

    match &framed.record {
        WalRecord::RunStarted {
            run_id,
            submission_id,
            ..
        } => {
            state.reset_episode();
            // Remember it for the TERMINAL, which carries no run in its WAL record and
            // could not otherwise name one.
            state.run_id = Some(run_id.clone());
            push!(ControlUnit::RunStarted {
                run_id: run_id.clone(),
                submission_id: submission_id.clone(),
            });
        }
        WalRecord::StepStarted { step_id } => {
            push!(ControlUnit::StepStarted { step_id: *step_id });
        }
        WalRecord::BudgetFired {
            run_id,
            kind,
            threshold,
            ordinal,
            ..
        } => {
            push!(ControlUnit::BudgetFired {
                run_id: run_id.clone(),
                kind: *kind,
                threshold: *threshold,
                ordinal: *ordinal,
            });
        }
        WalRecord::ModelStepFinished {
            step_id,
            assistant_message,
            usage,
            finish_reason,
            ..
        } => {
            state.final_step_finish_reason = *finish_reason;
            state.has_completed_step = true;
            // Fixed order, matching the live loop's emission: the assembled assistant message,
            // then one ToolCall per tool call in source order, then the step-finished marker.
            push!(ControlUnit::AssistantMessage {
                message: Arc::clone(assistant_message),
            });
            for call in assistant_message.tool_calls() {
                push!(ControlUnit::ToolCall { call: call.clone() });
            }
            push!(ControlUnit::StepFinished {
                step_id: *step_id,
                usage: *usage,
                finish_reason: finish_reason
                    .unwrap_or_else(|| message_shape_finish_reason(assistant_message)),
            });
        }
        WalRecord::ToolResult {
            result, outcome, ..
        } => {
            state
                .indeterminate_tool_calls
                .record_dispatch_terminal(&result.tool_call_id, outcome.is_indeterminate());
            push!(ControlUnit::ToolResult {
                result: result.as_ref().clone(),
            });
        }
        WalRecord::ToolNoDispatch {
            tool_call_id,
            reason,
            ..
        } => {
            state
                .indeterminate_tool_calls
                .record_dispatch_terminal(tool_call_id, false);
            // A no-dispatch terminal's reconstructed result is what the prompt uses; surface it
            // through the same deterministic rebuild as replay.
            push!(ControlUnit::ToolResult {
                result: crate::replay::no_dispatch_result(tool_call_id, reason),
            });
        }
        WalRecord::RunFinished {
            reason,
            usage,
            error,
            retries_used,
            ..
        } => {
            // A typed terminal error is surfaced before RunFinished.
            if let Some(error) = error {
                push!(ControlUnit::Error {
                    error: error.clone().into(),
                });
            }
            push!(ControlUnit::RunFinished {
                run_id: state.run_id.clone(),
                reason: *reason,
                usage: state.has_completed_step.then_some(*usage),
                final_step_finish_reason: state.final_step_finish_reason,
                indeterminate_tool_calls: (*reason == RunFinishReason::Interrupted)
                    .then(|| state.indeterminate_tool_calls.snapshot()),
                retries_used: *retries_used,
            });
        }
        WalRecord::RunPaused {
            run_id,
            reason,
            error,
            ..
        } => {
            push!(ControlUnit::Paused {
                run_id: run_id.clone(),
                reason: *reason,
                error: error.clone().map(SharedProviderError::from),
            });
        }
        WalRecord::Retracted {
            submission_id,
            failure: Some(failure),
        } => {
            // A permanently unadmittable queued send resolves atomically in this tombstone.
            // Expand it to the same visible bracket as an admitted run that fails terminally.
            state.reset_episode();
            push!(ControlUnit::RunStarted {
                run_id: failure.run_id.clone(),
                submission_id: Some(submission_id.clone()),
            });
            push!(ControlUnit::Error {
                error: failure.error.clone().into(),
            });
            push!(ControlUnit::RunFinished {
                run_id: Some(failure.run_id.clone()),
                reason: RunFinishReason::Error,
                usage: None,
                final_step_finish_reason: None,
                indeterminate_tool_calls: None,
                retries_used: 0,
            });
        }
        // These records carry no caller-facing control unit: batch brackets and dispatch intent
        // are internal durability records; ordinary queue/retract and approval records belong to
        // other surfaces.
        WalRecord::ToolBatchStarted { .. } => {
            state.indeterminate_tool_calls.reset_batch();
        }
        WalRecord::ToolDispatchIntent { tool_call_id, .. } => {
            state
                .indeterminate_tool_calls
                .record_dispatch_intent(tool_call_id);
        }
        WalRecord::ToolBatchFinished { .. }
        | WalRecord::TurnFinished { .. }
        | WalRecord::RunConfigRefrozen { .. }
        | WalRecord::RunAuthRefrozen { .. }
        | WalRecord::Queued { .. }
        | WalRecord::Retracted { failure: None, .. }
        | WalRecord::ApprovalRequested { .. }
        | WalRecord::ApprovalResolved { .. } => {}
        // A steered message is caller input, and the control stream carries no caller input
        // (a `RunStarted` unit does not carry its prompt either). It reaches readers through
        // `session.read`, placed where the model saw it.
        WalRecord::Steered { .. } => {}
        // The request changes recovery, not lifecycle: only RunFinished publishes a terminal.
        WalRecord::RunCancelRequested { .. } => {}
        // A record type written by a newer build: informational by the contract on
        // `WalRecord`, so it emits no control unit and leaves the projection state untouched.
        WalRecord::Unknown => {}
        // This reader release decodes compaction records but does not act on them yet.
        WalRecord::CompactionRequest { .. }
        | WalRecord::CompactionMessage { .. }
        | WalRecord::CompactionAnswer { .. }
        | WalRecord::CompactionWait { .. } => {}
        // Provider attempt evidence is known to this build but carries no session semantics:
        // it is skipped exactly like `Unknown`, so the control stream is identical without it.
        WalRecord::ModelAttemptFinished { .. } => {}
        // A resume's own account of what it found: informational, so the control stream is
        // identical without it. The results a resume writes are ordinary `ToolResult` records.
        WalRecord::ResumeStarted { .. } => {}
        // A cache warm is never a transcript message and ends no run: informational, so the
        // control stream and the store are identical without it.
        WalRecord::CacheWarmed { .. } => {}
        WalRecord::ApprovalRecorded { closing_result, .. } => {
            if let Some(result) = closing_result {
                state
                    .indeterminate_tool_calls
                    .record_dispatch_terminal(&result.tool_call_id, false);
                push!(ControlUnit::ToolResult {
                    result: result.as_ref().clone()
                });
            }
        }
        // Prepared state and pending markers affect approval membership, not the control lane.
        // Late execution is isolated from the conversation and must not emit prompt/control units.
        WalRecord::ToolCallDeferred { .. }
        | WalRecord::AwaitingApproval { .. }
        | WalRecord::LateApprovalRecorded { .. }
        | WalRecord::LateDispatchIntent { .. }
        | WalRecord::LateResult { .. } => {}
        // An informational record, skipped exactly like `Unknown`.
        WalRecord::InputTokenCapFrozen { .. }
        | WalRecord::InputTokenBudgetExhausted { .. }
        | WalRecord::StructuredOutputResult { .. }
        | WalRecord::QuestionWithdrawalAcknowledged { .. } => {}
    }
}

/// The finish reason implied by an assistant message's SHAPE, used only when the record
/// has no recorded provider finish_reason (older-shaped record). A message with tool
/// calls implies `ToolCalls`; otherwise `Stop`.
fn message_shape_finish_reason(
    msg: &broca_protocol::content::AssistantMessage,
) -> broca_protocol::content::FinishReason {
    use broca_protocol::content::FinishReason;
    if msg
        .content
        .iter()
        .any(|b| matches!(b, ContentBlock::ToolCall(_)))
    {
        FinishReason::ToolCalls
    } else {
        FinishReason::Stop
    }
}
