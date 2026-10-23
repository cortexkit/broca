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
        | ControlUnit::ContextCeilingWait { .. }
        | ControlUnit::CompactionWait { .. }
        | ControlUnit::BudgetFired { .. }
        | ControlUnit::AssistantMessage { .. }
        | ControlUnit::ToolCall { .. }
        | ControlUnit::ToolResult { .. }
        | ControlUnit::MessageBoundary { .. }
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

/// Context needed when records are projected incrementally. Placement keeps absolute
/// lineage ordinals across episodes; the other fields describe the current episode.
///
/// Batch replay can construct this state internally, while the live projector keeps one instance
/// across observed records so a later `RunFinished` can describe its last model step and dispatches.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ControlProjectionState {
    placement: crate::placement::Placement,
    placement_error: Option<String>,
    assistant_ordinal: Option<u64>,
    tool_result_ordinal: Option<u64>,
    assistant_message: Option<Arc<broca_protocol::AssistantMessage>>,
    assistant_started: bool,
    started_tools: std::collections::HashSet<broca_protocol::ToolCallId>,
    cancelled: bool,
    pending_inputs: Vec<Arc<Vec<broca_protocol::Message>>>,
    /// The recorded episode identity, including when restored from a store seed.
    /// `RunFinished` carries no run id in the WAL, so its control unit needs this context.
    run_id: Option<RunId>,
    final_step_finish_reason: Option<FinishReason>,
    has_completed_step: bool,
    indeterminate_tool_calls: IndeterminateToolCalls,
}

impl ControlProjectionState {
    /// Restore legacy control attributes without a message-position seed. Use
    /// `from_projection_seed` when projecting stored-message coordinates after a watermark.
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
            placement: Default::default(),
            placement_error: None,
            assistant_ordinal: None,
            tool_result_ordinal: None,
            assistant_message: None,
            assistant_started: false,
            started_tools: Default::default(),
            cancelled: false,
            pending_inputs: Vec::new(),
        }
    }

    /// A materialization boundary has no pending steer or open tool batch. Restore the
    /// absolute next ordinal and frozen budget facts rather than inferring them from a tail.
    pub fn from_projection_seed(seed: &crate::ProjectionSeed) -> Option<Self> {
        if !crate::budget_replay_seed_is_valid(seed) {
            return None;
        }
        let episode = seed.admission_checkpoint.last_episode.as_ref()?;
        let mut state = Self::from_completed_steps_and_final_step_finish_reason(
            seed.completed_steps,
            seed.last_step_finish_reason,
            seed.last_run_id.clone(),
        );
        // Budgeted seeds supplied validated firing facts above. Without a budget, missing
        // firing facts cannot change placement and need no full-prefix reconstruction.
        state.placement = crate::placement::Placement::seeded(
            seed.prefix_len,
            seed.last_run_id.clone(),
            episode.config.budget.clone(),
            episode.budget_replay_state.clone().unwrap_or_default(),
            seed.completed_steps,
        );
        state.cancelled = episode.cancel_at.is_some()
            && !matches!(episode.disposition, crate::EpisodeDisposition::Finished);
        Some(state)
    }

    pub fn placement(&self) -> &crate::placement::Placement {
        &self.placement
    }

    pub fn placement_error(&self) -> Option<&str> {
        self.placement_error.as_deref()
    }

    fn reset_episode(&mut self) {
        self.run_id = None;
        self.final_step_finish_reason = None;
        self.has_completed_step = false;
        self.indeterminate_tool_calls.reset_episode();
        self.assistant_ordinal = None;
        self.tool_result_ordinal = None;
        self.assistant_message = None;
        self.assistant_started = false;
        self.started_tools.clear();
        self.cancelled = false;
        self.pending_inputs.clear();
    }
}

/// A control payload's stored-message coordinate, from the same placement walk as replay.
#[derive(Debug, Clone)]
pub struct PlacedControlUnit {
    pub cursor: Cursor,
    pub unit: ControlUnit,
    pub message_ordinal: Option<u64>,
    pub event: cortexkit_role_llm_runner::live::LiveEvent,
}

impl PlacedControlUnit {
    pub fn mid(&self) -> Option<String> {
        self.message_ordinal.map(|ordinal| format!("m{ordinal}"))
    }
}

pub fn project_control_with_mids(
    records: &[FramedRecord],
    state: &mut ControlProjectionState,
) -> Result<Vec<PlacedControlUnit>, String> {
    let mut sink = PlacedUnitSink::default();
    for framed in records {
        project_control_record(framed, state, &mut sink);
        if let Some(error) = &state.placement_error {
            return Err(error.clone());
        }
    }
    Ok(sink.units)
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
    fn push<F, E>(&mut self, cursor: Cursor, ordinal: Option<u64>, make_unit: F, make_event: E)
    where
        F: FnOnce() -> ControlUnit,
        E: FnOnce() -> cortexkit_role_llm_runner::live::LiveEvent;
}

struct RecordSink<'a, S> {
    sink: &'a mut S,
    seq: u64,
    next_sub_index: u32,
    ordinal: Option<u64>,
}

impl<'a, S: ControlProjectionSink> RecordSink<'a, S> {
    fn new(sink: &'a mut S, seq: u64) -> Self {
        Self {
            sink,
            seq,
            next_sub_index: 0,
            ordinal: None,
        }
    }

    fn push<F>(&mut self, make_unit: F)
    where
        F: FnOnce() -> ControlUnit,
    {
        // Older payload-only markers have no independent lifecycle event. Use the defined
        // conservative reset instead of inventing an unknown no-op event type.
        self.push_with_event(make_unit, || {
            cortexkit_role_llm_runner::live::LiveEvent::DisplayReset
        });
    }

    fn push_with_event<F, E>(&mut self, make_unit: F, make_event: E)
    where
        F: FnOnce() -> ControlUnit,
        E: FnOnce() -> cortexkit_role_llm_runner::live::LiveEvent,
    {
        let cursor = Cursor::new(self.seq, self.next_sub_index);
        self.next_sub_index += 1;
        self.sink.push(cursor, self.ordinal, make_unit, make_event);
    }

    fn boundary<E>(&mut self, ordinal: u64, make_event: E)
    where
        E: FnOnce() -> cortexkit_role_llm_runner::live::LiveEvent,
    {
        self.ordinal = Some(ordinal);
        self.push_with_event(
            || ControlUnit::MessageBoundary {
                mid: format!("m{ordinal}"),
            },
            make_event,
        );
    }
}

#[derive(Default)]
struct UnitSink {
    units: Vec<(Cursor, ControlUnit)>,
}

impl ControlProjectionSink for UnitSink {
    fn push<F, E>(&mut self, cursor: Cursor, _ordinal: Option<u64>, make_unit: F, _make_event: E)
    where
        F: FnOnce() -> ControlUnit,
        E: FnOnce() -> cortexkit_role_llm_runner::live::LiveEvent,
    {
        self.units.push((cursor, make_unit()));
    }
}

#[derive(Default)]
struct PlacedUnitSink {
    units: Vec<PlacedControlUnit>,
}

impl ControlProjectionSink for PlacedUnitSink {
    fn push<F, E>(&mut self, cursor: Cursor, ordinal: Option<u64>, make_unit: F, make_event: E)
    where
        F: FnOnce() -> ControlUnit,
        E: FnOnce() -> cortexkit_role_llm_runner::live::LiveEvent,
    {
        self.units.push(PlacedControlUnit {
            cursor,
            unit: make_unit(),
            message_ordinal: ordinal,
            event: make_event(),
        });
    }
}

#[derive(Default)]
struct LastCursorSink {
    last: Option<Cursor>,
}

impl ControlProjectionSink for LastCursorSink {
    fn push<F, E>(&mut self, cursor: Cursor, _ordinal: Option<u64>, _make_unit: F, _make_event: E)
    where
        F: FnOnce() -> ControlUnit,
        E: FnOnce() -> cortexkit_role_llm_runner::live::LiveEvent,
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
    use crate::placement::{AppendKind, PlacementAction};
    use cortexkit_role_llm_runner::live::{self, LiveEvent};
    let actions = match state.placement.observe(&framed.record) {
        Ok(actions) => actions,
        Err(error) => {
            state.placement_error = Some(error);
            Vec::new()
        }
    };
    let mut record_sink = RecordSink::new(sink, framed.seq);
    macro_rules! push {
        ($unit:expr, $event:expr) => {
            record_sink.push_with_event(|| $unit, || $event)
        };
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
            push!(
                ControlUnit::RunStarted {
                    run_id: run_id.clone(),
                    submission_id: submission_id.clone(),
                },
                LiveEvent::RunStarted(live::RunStarted::new(run_id.0.clone()))
            );
        }
        WalRecord::StepStarted { step_id } => {
            state.assistant_ordinal = Some(state.placement.next_ordinal());
            state.assistant_message = None;
            state.assistant_started = true;
            state.started_tools.clear();
            record_sink.ordinal = state.assistant_ordinal;
            push!(
                ControlUnit::StepStarted { step_id: *step_id },
                message_event(state.assistant_ordinal, false)
            );
        }
        WalRecord::BudgetFired {
            run_id,
            kind,
            threshold,
            ordinal,
            ..
        } => {
            record_sink.ordinal = actions.iter().find_map(|action| match action {
                PlacementAction::Append {
                    first,
                    kind: AppendKind::Budget,
                    ..
                } => Some(*first),
                _ => None,
            });
            let budget_ordinal = record_sink.ordinal;
            push!(
                ControlUnit::BudgetFired {
                    run_id: run_id.clone(),
                    kind: *kind,
                    threshold: *threshold,
                    ordinal: *ordinal,
                },
                message_event(budget_ordinal, false)
            );
        }
        WalRecord::ModelStepFinished {
            step_id,
            assistant_message,
            usage,
            finish_reason,
            ..
        } => {
            state.assistant_ordinal = actions
                .iter()
                .find_map(|action| match action {
                    PlacementAction::Append {
                        first,
                        kind: AppendKind::Assistant | AppendKind::Finalize,
                        ..
                    } => Some(*first),
                    _ => None,
                })
                .or_else(|| state.placement.held_assistant_ordinal());
            state.tool_result_ordinal = state.placement.held_tool_result_ordinal();
            state.assistant_message = Some(Arc::clone(assistant_message));
            state.started_tools.clear();
            let already_started = std::mem::take(&mut state.assistant_started);
            record_sink.ordinal = state.assistant_ordinal;
            state.final_step_finish_reason = *finish_reason;
            state.has_completed_step = true;
            // Fixed order, matching the live loop's emission: the assembled assistant message,
            // then one ToolCall per tool call in source order, then the step-finished marker.
            push!(
                ControlUnit::AssistantMessage {
                    message: Arc::clone(assistant_message),
                },
                message_event(state.assistant_ordinal, already_started)
            );
            for call in assistant_message.tool_calls() {
                push!(ControlUnit::ToolCall { call: call.clone() });
            }
            push!(ControlUnit::StepFinished {
                step_id: *step_id,
                usage: *usage,
                finish_reason: finish_reason
                    .unwrap_or_else(|| message_shape_finish_reason(assistant_message)),
            });
            if !already_started {
                if let Some(ordinal) = state.assistant_ordinal {
                    record_sink.boundary(ordinal, || message_event(Some(ordinal), true));
                }
            }
        }
        WalRecord::ToolResult {
            result,
            outcome,
            finished_at_ms,
            ..
        } => {
            record_sink.ordinal = state.tool_result_ordinal;
            let assistant = state.assistant_message.clone();
            let call = assistant.as_ref().and_then(|message| {
                message
                    .tool_calls()
                    .find(|call| call.tool_call_id == result.tool_call_id)
            });
            let owner = state.assistant_ordinal.filter(|_| call.is_some());
            let first_start =
                call.is_some() && state.started_tools.insert(result.tool_call_id.clone());
            let outcome_name = tool_outcome(*outcome, result, state.cancelled);
            state
                .indeterminate_tool_calls
                .record_dispatch_terminal(&result.tool_call_id, outcome.is_indeterminate());
            push!(
                ControlUnit::ToolResult {
                    result: result.as_ref().clone(),
                },
                match (owner, call) {
                    (Some(ordinal), Some(call)) if first_start =>
                        tool_started_event(ordinal, call, None, None, *finished_at_ms),
                    (Some(ordinal), Some(_)) => tool_finished_event(
                        ordinal,
                        &result.tool_call_id,
                        outcome_name,
                        result,
                        *finished_at_ms
                    ),
                    _ => LiveEvent::DisplayReset,
                }
            );
            // A synthetic terminal may be a call's first durable lifecycle observation.
            // Both lifecycle events then use that record's one recorded observation.
            if first_start {
                if let Some(ordinal) = owner {
                    record_sink.boundary(ordinal, || {
                        tool_finished_event(
                            ordinal,
                            &result.tool_call_id,
                            outcome_name,
                            result,
                            *finished_at_ms,
                        )
                    });
                }
            }
        }
        WalRecord::ToolNoDispatch {
            tool_call_id,
            reason,
            dispatched_at_ms,
            ..
        } => {
            record_sink.ordinal = state.tool_result_ordinal;
            let assistant = state.assistant_message.clone();
            let call = assistant.as_ref().and_then(|message| {
                message
                    .tool_calls()
                    .find(|call| &call.tool_call_id == tool_call_id)
            });
            let owner = state.assistant_ordinal.filter(|_| call.is_some());
            let first_start = call.is_some() && state.started_tools.insert(tool_call_id.clone());
            let outcome_name = match reason {
                broca_wal::NoDispatchReason::Denied { .. }
                | broca_wal::NoDispatchReason::PolicyBlocked { .. } => "denied",
                _ => "error",
            };
            state
                .indeterminate_tool_calls
                .record_dispatch_terminal(tool_call_id, false);
            // A no-dispatch terminal's reconstructed result is what the prompt uses; surface it
            // through the same deterministic rebuild as replay.
            push!(
                ControlUnit::ToolResult {
                    result: crate::replay::no_dispatch_result(tool_call_id, reason),
                },
                match (owner, call) {
                    (Some(ordinal), Some(call)) if first_start =>
                        tool_started_event(ordinal, call, None, None, *dispatched_at_ms),
                    (Some(ordinal), Some(_)) => tool_finished_event(
                        ordinal,
                        tool_call_id,
                        outcome_name,
                        &crate::replay::no_dispatch_result(tool_call_id, reason),
                        *dispatched_at_ms
                    ),
                    _ => LiveEvent::DisplayReset,
                }
            );
            if first_start {
                if let Some(ordinal) = owner {
                    record_sink.boundary(ordinal, || {
                        tool_finished_event(
                            ordinal,
                            tool_call_id,
                            outcome_name,
                            &crate::replay::no_dispatch_result(tool_call_id, reason),
                            *dispatched_at_ms,
                        )
                    });
                }
            }
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
            push!(
                ControlUnit::RunFinished {
                    run_id: state.run_id.clone(),
                    reason: *reason,
                    usage: state.has_completed_step.then_some(*usage),
                    final_step_finish_reason: state.final_step_finish_reason,
                    indeterminate_tool_calls: (*reason == RunFinishReason::Interrupted)
                        .then(|| state.indeterminate_tool_calls.snapshot()),
                    retries_used: *retries_used,
                },
                state
                    .run_id
                    .as_ref()
                    .map_or(LiveEvent::DisplayReset, |run_id| LiveEvent::RunFinished(
                        live::RunEnded::new(run_id.0.clone(), reason.as_str())
                    ))
            );
            state.assistant_ordinal = None;
            state.tool_result_ordinal = None;
            state.assistant_message = None;
            state.assistant_started = false;
            state.started_tools.clear();
            state.cancelled = false;
        }
        WalRecord::RunPaused {
            run_id,
            reason,
            error,
            ..
        } => {
            push!(
                ControlUnit::Paused {
                    run_id: run_id.clone(),
                    reason: *reason,
                    error: error.clone().map(SharedProviderError::from),
                },
                LiveEvent::RunPaused(live::RunEnded::new(run_id.0.clone(), reason.as_str()))
            );
        }
        WalRecord::Retracted {
            submission_id,
            failure: Some(failure),
        } => {
            // A permanently unadmittable queued send resolves atomically in this tombstone.
            // Expand it to the same visible bracket as an admitted run that fails terminally.
            state.reset_episode();
            push!(
                ControlUnit::RunStarted {
                    run_id: failure.run_id.clone(),
                    submission_id: Some(submission_id.clone()),
                },
                LiveEvent::RunStarted(live::RunStarted::new(failure.run_id.0.clone()))
            );
            push!(ControlUnit::Error {
                error: failure.error.clone().into(),
            });
            push!(
                ControlUnit::RunFinished {
                    run_id: Some(failure.run_id.clone()),
                    reason: RunFinishReason::Error,
                    usage: None,
                    final_step_finish_reason: None,
                    indeterminate_tool_calls: None,
                    retries_used: 0,
                },
                LiveEvent::RunFinished(live::RunEnded::new(failure.run_id.0.clone(), "error"))
            );
        }
        // These records add lifecycle boundaries after their existing units, without moving
        // any cursor belonging to an older payload.
        WalRecord::ToolBatchStarted { .. } => {
            state.indeterminate_tool_calls.reset_batch();
            if let Some(ordinal) = state.tool_result_ordinal {
                record_sink.boundary(ordinal, || message_event(Some(ordinal), false));
            }
        }
        WalRecord::ToolDispatchIntent {
            tool_call_id,
            tool_name,
            args,
            dispatched_at_ms,
            ..
        } => {
            state
                .indeterminate_tool_calls
                .record_dispatch_intent(tool_call_id);
            let assistant = state.assistant_message.clone();
            let call = assistant.as_ref().and_then(|message| {
                message
                    .tool_calls()
                    .find(|call| &call.tool_call_id == tool_call_id)
            });
            if let (Some(ordinal), Some(call)) = (state.assistant_ordinal, call) {
                if state.started_tools.insert(tool_call_id.clone()) {
                    record_sink.boundary(ordinal, || {
                        tool_started_event(
                            ordinal,
                            call,
                            tool_name.as_deref(),
                            Some(args),
                            *dispatched_at_ms,
                        )
                    });
                }
            }
        }
        WalRecord::ToolBatchFinished { .. } => {}
        WalRecord::TurnFinished { .. }
        | WalRecord::RunConfigRefrozen { .. }
        | WalRecord::RunAuthRefrozen { .. }
        | WalRecord::Queued { .. }
        | WalRecord::Retracted { failure: None, .. }
        | WalRecord::ApprovalRequested { .. }
        | WalRecord::ApprovalResolved { .. } => {}
        // Hold only references to the durable input; Placement decides when its messages land.
        WalRecord::Steered { input, .. } => state.pending_inputs.push(Arc::clone(input)),
        WalRecord::SendDelivered {
            input: Some(input), ..
        } => state.pending_inputs.push(Arc::clone(input)),
        WalRecord::DeliveryOrdered { .. }
        | WalRecord::SendEnded { .. }
        | WalRecord::SendDelivered { input: None, .. } => {}
        // The request changes recovery, not lifecycle: only RunFinished publishes a terminal.
        WalRecord::RunCancelRequested { run_id, .. } => {
            state.cancelled |= state.run_id.as_ref() == Some(run_id);
        }
        // A record type written by a newer build: informational by the contract on
        // `WalRecord`, so it emits no control unit and leaves the projection state untouched.
        WalRecord::Unknown => {}
        WalRecord::ContextCeilingWait {
            deadline_ms,
            unavailable: None,
            ..
        } => {
            if let Some(run_id) = &state.run_id {
                push!(ControlUnit::ContextCeilingWait {
                    run_id: run_id.clone(),
                    reason: "context_ceiling_wait".into(),
                    deadline_ms: *deadline_ms,
                });
            }
        }
        WalRecord::ContextCeilingWait { .. } | WalRecord::ContextCeilingDecided { .. } => {}
        WalRecord::CompactionRideWait { deadline_ms, .. } => {
            if let Some(run_id) = &state.run_id {
                push!(ControlUnit::CompactionWait {
                    run_id: run_id.clone(),
                    reason: cortexkit_role_llm_runner::run::RUN_REASON_COMPACTION_WAIT.into(),
                    deadline_ms: *deadline_ms,
                });
            }
        }
        // The wait's end is visible as the step moving on; it emits no unit of its own.
        WalRecord::CompactionRideWaitEnded { .. } => {}
        // Compaction changes model requests but these records emit no control unit.
        WalRecord::CompactionRequest { .. }
        | WalRecord::CompactionMessage { .. }
        | WalRecord::CompactionAnswer { .. }
        | WalRecord::CompactionWait { .. }
        | WalRecord::CompactionViewApplied { .. }
        | WalRecord::CompactionNotice { .. } => {}
        // Provider attempt evidence is known to this build but carries no session semantics:
        // it is skipped exactly like `Unknown`, so the control stream is identical without it.
        WalRecord::ModelAttemptFinished { .. } => {}
        // A resume's own account of what it found: informational, so the control stream is
        // identical without it. The results a resume writes are ordinary `ToolResult` records.
        WalRecord::ResumeStarted { .. } => {}
        // A cache warm is never a transcript message and ends no run: informational, so the
        // control stream and the store are identical without it.
        WalRecord::CacheWarmed { .. } => {}
        WalRecord::ApprovalRecorded {
            call_id,
            closing_result,
            closed_at_ms,
            ..
        } => {
            if let Some(result) = closing_result {
                record_sink.ordinal = state.tool_result_ordinal;
                let assistant = state.assistant_message.clone();
                let call = assistant.as_ref().and_then(|message| {
                    message
                        .tool_calls()
                        .find(|call| &call.tool_call_id == call_id)
                });
                let owner = state.assistant_ordinal.filter(|_| call.is_some());
                let first_start = call.is_some() && state.started_tools.insert(call_id.clone());
                state
                    .indeterminate_tool_calls
                    .record_dispatch_terminal(&result.tool_call_id, false);
                push!(
                    ControlUnit::ToolResult {
                        result: result.as_ref().clone()
                    },
                    match (owner, call) {
                        (Some(ordinal), Some(call)) if first_start =>
                            tool_started_event(ordinal, call, None, None, *closed_at_ms),
                        (Some(ordinal), Some(_)) =>
                            tool_finished_event(ordinal, call_id, "denied", result, *closed_at_ms),
                        _ => LiveEvent::DisplayReset,
                    }
                );
                if first_start {
                    if let Some(ordinal) = owner {
                        record_sink.boundary(ordinal, || {
                            tool_finished_event(ordinal, call_id, "denied", result, *closed_at_ms)
                        });
                    }
                }
            }
        }
        WalRecord::ToolCallDeferred {
            call_id,
            tool,
            args_jcs,
            deferred_at_ms,
            ..
        } => {
            let assistant = state.assistant_message.clone();
            let call = assistant.as_ref().and_then(|message| {
                message
                    .tool_calls()
                    .find(|call| &call.tool_call_id == call_id)
            });
            if let (Some(ordinal), Some(call)) = (state.assistant_ordinal, call) {
                if state.started_tools.insert(call_id.clone()) {
                    record_sink.boundary(ordinal, || {
                        let mut event = live::ToolStarted::new(
                            format!("m{ordinal}"),
                            call.tool_call_id.0.clone(),
                            tool.clone(),
                            &serde_json::Value::Null,
                            None,
                            *deferred_at_ms,
                        );
                        event.arguments = args_jcs.clone();
                        LiveEvent::ToolStarted(event)
                    });
                }
            }
        }
        // Late execution is isolated from the conversation and must not emit prompt/control units.
        WalRecord::AwaitingApproval { .. }
        | WalRecord::LateApprovalRecorded { .. }
        | WalRecord::LateDispatchIntent { .. }
        | WalRecord::LateResult { .. } => {}
        // An informational record, skipped exactly like `Unknown`.
        WalRecord::InputTokenCapFrozen { .. }
        | WalRecord::InputTokenBudgetExhausted { .. }
        | WalRecord::StructuredOutputResult { .. }
        | WalRecord::StructuredOutputDowngraded { .. }
        | WalRecord::QuestionWithdrawalAcknowledged { .. } => {}
    }
    // Appending boundaries here keeps every older payload at its original sub_index.
    for action in actions {
        match action {
            PlacementAction::Append {
                first,
                count,
                kind: AppendKind::Input,
            } => {
                if let WalRecord::RunStarted { input, .. } = &framed.record {
                    input_lifecycles(&mut record_sink, first, input.iter().take(count as usize));
                }
            }
            PlacementAction::PlaceHeld { first, count } => {
                let inputs = std::mem::take(&mut state.pending_inputs);
                input_lifecycles(
                    &mut record_sink,
                    first,
                    inputs
                        .iter()
                        .flat_map(|input| input.iter())
                        .take(count as usize),
                );
            }
            PlacementAction::Append {
                first,
                count,
                kind: AppendKind::Budget,
            } => {
                for offset in 0..count {
                    let ordinal = first.saturating_add(offset);
                    record_sink.boundary(ordinal, || message_event(Some(ordinal), true));
                }
            }
            PlacementAction::Append {
                first,
                count,
                kind: AppendKind::ToolTurn,
            } if count != 0 => {
                let ordinal = first.saturating_add(count - 1);
                record_sink.boundary(ordinal, || message_event(Some(ordinal), true));
                state.assistant_ordinal = None;
                state.tool_result_ordinal = None;
                state.assistant_message = None;
                state.started_tools.clear();
            }
            _ => {}
        }
    }
}

fn message_event(
    ordinal: Option<u64>,
    finished: bool,
) -> cortexkit_role_llm_runner::live::LiveEvent {
    use cortexkit_role_llm_runner::live::{LiveEvent, MessageEvent};
    match ordinal {
        Some(ordinal) if finished => {
            LiveEvent::MessageFinished(MessageEvent::new(format!("m{ordinal}")))
        }
        Some(ordinal) => LiveEvent::MessageStarted(MessageEvent::new(format!("m{ordinal}"))),
        None => LiveEvent::DisplayReset,
    }
}

fn tool_started_event(
    ordinal: u64,
    call: &broca_protocol::ToolCall,
    name: Option<&str>,
    arguments: Option<&serde_json::Value>,
    observed_at: Option<u64>,
) -> cortexkit_role_llm_runner::live::LiveEvent {
    use cortexkit_role_llm_runner::live::{LiveEvent, ToolStarted};
    LiveEvent::ToolStarted(ToolStarted::new(
        format!("m{ordinal}"),
        call.tool_call_id.0.clone(),
        name.unwrap_or(&call.tool_name),
        arguments.unwrap_or(&call.input),
        None,
        observed_at,
    ))
}

fn tool_finished_event(
    ordinal: u64,
    call_id: &ToolCallId,
    outcome: &str,
    result: &broca_protocol::ToolResult,
    observed_at: Option<u64>,
) -> cortexkit_role_llm_runner::live::LiveEvent {
    use broca_protocol::ToolOutput;
    use cortexkit_role_llm_runner::live::{LiveEvent, ToolFinished};
    let bytes = match &result.output {
        ToolOutput::Text { text } => text.len(),
        ToolOutput::Json { value } => serde_json::to_vec(value)
            .expect("JSON values serialize")
            .len(),
        ToolOutput::Denied { reason } => reason.len(),
        ToolOutput::Error { message } => message.len(),
    };
    LiveEvent::ToolFinished(ToolFinished::new(
        format!("m{ordinal}"),
        call_id.0.clone(),
        outcome,
        bytes as u64,
        observed_at,
    ))
}

fn tool_outcome(
    outcome: broca_wal::DispatchOutcome,
    result: &broca_protocol::ToolResult,
    cancelled: bool,
) -> &'static str {
    use broca_wal::DispatchOutcome;
    match outcome {
        DispatchOutcome::Interrupted if cancelled => "cancelled",
        DispatchOutcome::Interrupted | DispatchOutcome::Unknown => "interrupted",
        DispatchOutcome::MaxSteps => "error",
        DispatchOutcome::Completed
            if matches!(result.output, broca_protocol::ToolOutput::Denied { .. }) =>
        {
            "denied"
        }
        DispatchOutcome::Completed if result.is_error => "error",
        DispatchOutcome::Completed => "ok",
    }
}

fn input_lifecycles<'a, S: ControlProjectionSink>(
    sink: &mut RecordSink<'_, S>,
    first: u64,
    messages: impl Iterator<Item = &'a broca_protocol::Message>,
) {
    let mut owners = std::collections::HashMap::new();
    for (index, message) in messages.enumerate() {
        let ordinal = first.saturating_add(index as u64);
        sink.boundary(ordinal, || message_event(Some(ordinal), false));
        for block in &message.content {
            match block {
                broca_protocol::ContentBlock::ToolCall(call) => {
                    owners.insert(call.tool_call_id.clone(), (ordinal, call));
                    sink.boundary(ordinal, || {
                        tool_started_event(ordinal, call, None, None, None)
                    });
                }
                broca_protocol::ContentBlock::ToolResult(result) => {
                    if let Some((owner, _)) = owners.remove(&result.tool_call_id) {
                        sink.boundary(owner, || {
                            tool_finished_event(
                                owner,
                                &result.tool_call_id,
                                tool_outcome(broca_wal::DispatchOutcome::Completed, result, false),
                                result,
                                None,
                            )
                        });
                    }
                }
                _ => {}
            }
        }
        sink.boundary(ordinal, || message_event(Some(ordinal), true));
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

#[cfg(test)]
mod placement_tests;

#[cfg(test)]
mod live_tests;
