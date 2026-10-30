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
        | ControlUnit::Status { .. }
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveCallIdentity {
    seq: u64,
    schema_pin: Option<String>,
    recorded_key: Option<String>,
    dispatched: bool,
}

/// Context for incremental projection. Placement and log identity survive episode
/// boundaries; call facts are bounded to the current tool batch.
///
/// Batch replay constructs this state internally, while the live projector keeps
/// an instance so later records retain their run, message and call identities.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ControlProjectionState {
    wal_lineage_id: Option<broca_wal::WalLineageId>,
    call_identities: std::collections::HashMap<ToolCallId, LiveCallIdentity>,
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
    held_calls: crate::replay::HeldCallsProjection,
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
            wal_lineage_id: None,
            call_identities: Default::default(),
            run_id,
            final_step_finish_reason,
            has_completed_step: completed_steps > 0,
            indeterminate_tool_calls: IndeterminateToolCalls::default(),
            held_calls: Default::default(),
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

    /// The state a projection holds just before an episode's `RunStarted`, given the
    /// transcript ordinal where that episode's input starts.
    ///
    /// A `RunStarted` resets every episode-local field, and placement keeps only the absolute
    /// next ordinal across episodes (its batch, held steers and budget facts reset at the run
    /// start too). So projecting an episode from its `RunStarted` with this state yields the
    /// same units and the same final state as projecting the whole log, as long as the
    /// ordinal is the episode's own first ordinal. Use it only immediately before a
    /// `RunStarted` record; anywhere else the episode-local fields would be missing.
    pub fn at_episode_start(first_ord: u64) -> Self {
        Self {
            placement: crate::placement::Placement::new(first_ord),
            ..Self::default()
        }
    }

    pub fn placement(&self) -> &crate::placement::Placement {
        &self.placement
    }

    /// Supply the log header identity read with the records, never a file-system
    /// inode or the separate visible-history lineage.
    pub fn set_wal_lineage_id(&mut self, lineage: Option<broca_wal::WalLineageId>) {
        if lineage.is_some() {
            self.wal_lineage_id = lineage;
        }
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
        self.call_identities.clear();
        self.cancelled = false;
        self.pending_inputs.clear();
        self.held_calls = Default::default();
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
        let first = sink.units.len();
        project_control_record(framed, state, &mut sink);
        for frame in &mut sink.units[first..] {
            let (id, key, pin) = match &mut frame.event {
                cortexkit_role_llm_runner::live::LiveEvent::ToolStarted(event) => (
                    &event.tool_call_id,
                    &mut event.call_key,
                    &mut event.schema_pin,
                ),
                cortexkit_role_llm_runner::live::LiveEvent::ToolFinished(event) => (
                    &event.tool_call_id,
                    &mut event.call_key,
                    &mut event.schema_pin,
                ),
                _ => continue,
            };
            if let Some(identity) = state.call_identities.get(&ToolCallId(id.clone())) {
                let lineage = state.wal_lineage_id.map(|lineage| lineage.to_string());
                *key = crate::context::tool_call_key(
                    lineage.as_deref(),
                    identity.seq,
                    identity.recorded_key.as_deref(),
                );
                pin.clone_from(&identity.schema_pin);
            }
        }
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
            manifest,
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
                LiveEvent::RunStarted({
                    let mut started = live::RunStarted::new(run_id.0.clone());
                    started.rich_ui = manifest
                        .as_ref()
                        .and_then(|manifest| manifest.rich_ui.clone());
                    started
                })
            );
        }
        WalRecord::StepStarted { step_id } => {
            state.assistant_ordinal = Some(state.placement.next_ordinal());
            state.assistant_message = None;
            state.assistant_started = true;
            state.started_tools.clear();
            state.call_identities.clear();
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
            state.call_identities.clear();
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
            state.call_identities.clear();
            state.cancelled = false;
        }
        WalRecord::RunPaused {
            run_id,
            reason,
            error,
            ..
        } if *reason != broca_protocol::PauseReason::ToolHeld => {
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
            schema_pin,
            call_key,
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
                let schema = schema_pin.as_ref().and_then(|pin| {
                    cortexkit_role_tool_provider::call::SchemaPin::new(
                        tool_name.as_deref().unwrap_or(&call.tool_name),
                        &pin.schema_digest,
                        pin.semantics,
                    )
                    .encode()
                    .ok()
                });
                let identity = LiveCallIdentity {
                    seq: framed.seq,
                    schema_pin: schema,
                    recorded_key: call_key.clone(),
                    dispatched: true,
                };
                match state.call_identities.entry(tool_call_id.clone()) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(identity);
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry)
                        if !entry.get().dispatched =>
                    {
                        entry.insert(identity);
                    }
                    _ => {}
                }
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
        WalRecord::WriterOpened { .. }
        | WalRecord::WriterClosed { .. }
        | WalRecord::VisibilityLate { .. }
        | WalRecord::Unknown => {}
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
        WalRecord::ContextCeilingWait { .. }
        | WalRecord::ContextCeilingDecided { .. }
        | WalRecord::OutputReservationClamped { .. } => {}
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
        WalRecord::AttemptReserved { .. }
        | WalRecord::AttemptFinished { .. }
        | WalRecord::AttemptReleased { .. }
        | WalRecord::AttemptCorrected { .. } => {}
        // A refused step is re-sent under the same step id and its refused content is never
        // stored, so a subscriber replaying the WAL sees only the step's `StepStarted` and the
        // answer that committed it.
        WalRecord::ModelStepRefused { .. } => {}
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
                // Preparation has a durable key, but no dispatched definition yet.
                // The later intent supplies the pin without changing the prepared key.
                state
                    .call_identities
                    .entry(call_id.clone())
                    .or_insert(LiveCallIdentity {
                        seq: framed.seq,
                        schema_pin: None,
                        recorded_key: None,
                        dispatched: false,
                    });
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
        // The internal park preserves recovery's waiting slot; it is not a caller pause.
        // The held record and its terminal below publish the provider-held status instead.
        WalRecord::RunPaused { .. } | WalRecord::ToolCallProviderHeld { .. } => {}
        // An informational record, skipped exactly like `Unknown`.
        WalRecord::InputTokenCapFrozen { .. }
        | WalRecord::InputTokenBudgetExhausted { .. }
        | WalRecord::StructuredOutputResult { .. }
        | WalRecord::StructuredOutputDowngraded { .. }
        | WalRecord::QuestionWithdrawalAcknowledged { .. } => {}
        // A mid-session change emits no prompt or control unit: the next `RunStarted` that
        // freezes its manifest renders the new tools, and earlier episodes resume byte-identical.
        WalRecord::SessionRefreshAccepted { .. }
        | WalRecord::SessionRefreshPolicyChanged { .. }
        | WalRecord::SessionPrefixFlushed { .. } => {}
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
                state.call_identities.clear();
            }
            _ => {}
        }
    }
    // Append status after older units so their durable cursors stay unchanged, including
    // a ToolResult's lifecycle boundaries when the final held call leaves the list.
    if let Some(status) = state.held_calls.observe(&framed.record) {
        if let Some(run_id) = &state.run_id {
            push!(ControlUnit::Status {
                run_id: run_id.clone(),
                status: Box::new(status),
            });
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
        // A multi-part result counts its text and the stored size of each media object, so a
        // screenshot reads as the bytes the tool returned rather than its short marker text.
        ToolOutput::Parts { parts } => parts
            .iter()
            .map(|part| match part {
                broca_protocol::ToolOutputPart::Text { text } => text.len(),
                broca_protocol::ToolOutputPart::Media { media } => media.byte_size as usize,
            })
            .sum(),
        ToolOutput::Denied { reason } => reason.len(),
        ToolOutput::Error { message, .. } => message.len(),
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

// Episode-bounded reads from the offset index.
//
// On overhaul state roots the offset index records where each run episode starts. The model
// view and a live attach then decode only the frames of the episodes they show, folding at
// most one episode's records to rebuild state, instead of replaying the whole log. These
// helpers check everything they take from the index against the log before relying on it, and
// a read that fails any check replays the whole log instead.

/// One run episode as the offset index's `episode` row locates it.
///
/// Each field mirrors a column the indexer wrote, so episode-bounded reads never depend on the
/// store crate. None is trusted on its own: [`replay_from_episode_start`] re-reads the episode's
/// `RunStarted` frame and checks the ordinal where its input starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedEpisode {
    /// WAL sequence of the episode's `RunStarted`; `None` for records written before the first run.
    pub run_started_seq: Option<u64>,
    /// Byte offset where that `RunStarted` frame starts.
    pub run_started_off: Option<u64>,
    /// Transcript ordinal where the episode's input starts.
    pub first_ord: u64,
    /// Transcript ordinal just past the episode's last placed message.
    pub end_ord: u64,
}

/// The index's tail seed at the committed projection boundary an episode read starts from.
///
/// The boundary is the last qualified `projection_boundary` row before the episode's
/// `RunStarted`; the other fields are the tail seed the index holds at that boundary: exactly
/// the inputs [`crate::replay::replay_seeded_at_validated_boundary`] takes, which validates the
/// boundary frame before anything is replayed.
#[derive(Debug, Clone)]
pub struct EpisodeSeed {
    pub boundary: crate::replay::SeedBoundary,
    /// Transcript length the boundary covers.
    pub ordinal_base: u64,
    /// Steers recorded before the boundary that no step boundary has placed yet.
    pub held_steers: Vec<broca_protocol::Message>,
    pub tag_seed: crate::tags::TagSeed,
    /// Model, run id and budget of the episode open at the boundary.
    pub seed_origin: Option<broca_protocol::request::ModelHandle>,
    pub seed_run_id: Option<RunId>,
    pub seed_budget: Option<broca_protocol::request::BudgetConfig>,
}

/// A replay that starts at a validated position and reaches an episode's `RunStarted`.
#[derive(Debug, Clone)]
pub struct EpisodeReplay {
    /// Replay of every frame given, seeded at the boundary or from the log's first record.
    pub state: crate::replay::ReplayState,
    /// Transcript ordinal of `state.prompt[0]`.
    pub ordinal_base: u64,
    /// Index, among the frames given, of the episode's `RunStarted`.
    pub run_started: usize,
}

/// Byte offset of the log's first record: past the header frame when the log has one.
pub fn first_record_offset(
    read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
) -> std::io::Result<u64> {
    log_header_info(read_at).map(|(offset, _)| offset)
}

/// Read the initial log header once, keeping its identity beside the record offset.
/// Headerless legacy sources retain offset zero and an unknown identity.
pub fn log_header_info(
    read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
) -> std::io::Result<(u64, Option<broca_wal::WalLineageId>)> {
    let header = broca_wal::access_counter::read_at(
        read_at,
        0,
        broca_wal::framing::LINEAGE_FRAME_LEN as u64,
    )?;
    Ok(match broca_wal::access_counter::decode_next(&header) {
        broca_wal::framing::DecodeStep::Lineage {
            consumed,
            lineage_id,
        } => (consumed as u64, Some(lineage_id)),
        _ => (0, None),
    })
}

/// Decode the frames in the byte range `from..to`, which must start and end on frame
/// boundaries, counting each decoded frame when `decode_counter` is given.
///
/// The range is read once through `read_at`. A log shorter than the range, a torn or corrupt
/// frame, or a frame from a newer writer is an error: the index named a range the log does not
/// hold, and the caller replays the whole log instead. With counting on, tests assert that a
/// read decodes only the frames of the range it asked for, never the whole log.
pub fn decode_positioned_frames(
    read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    from: u64,
    to: u64,
    decode_counter: Option<&std::cell::Cell<u64>>,
) -> Result<Vec<crate::replay::PositionedFrame>, String> {
    use broca_wal::access_counter::decode_next;
    use broca_wal::framing::DecodeStep;
    let len = to
        .checked_sub(from)
        .ok_or_else(|| format!("WAL range {from}..{to} is reversed"))?;
    let bytes = broca_wal::access_counter::read_at(read_at, from, len)
        .map_err(|error| format!("WAL read at {from}: {error}"))?;
    if (bytes.len() as u64) < len {
        return Err(format!(
            "WAL ends at {} before the range end {to}",
            from + bytes.len() as u64
        ));
    }
    let mut frames = Vec::new();
    let mut off = 0usize;
    while off < bytes.len() {
        let at = from + off as u64;
        match decode_next(&bytes[off..]) {
            DecodeStep::Record { framed, consumed } => {
                if let Some(counter) = decode_counter {
                    counter.set(counter.get() + 1);
                }
                frames.push(crate::replay::PositionedFrame {
                    off: at,
                    framed: *framed,
                });
                off += consumed;
            }
            DecodeStep::Lineage { consumed, .. } => {
                if let Some(counter) = decode_counter {
                    counter.set(counter.get() + 1);
                }
                off += consumed;
            }
            DecodeStep::TornTail => return Err(format!("torn WAL frame at {at}")),
            DecodeStep::Corrupt(error) => return Err(format!("corrupt WAL at {at}: {error}")),
            DecodeStep::NewerWriter { ver, .. } => {
                return Err(format!("newer WAL writer version {ver} at {at}"))
            }
        }
    }
    Ok(frames)
}

/// The episode whose records hold WAL sequence `seq`: the last one started at or before it.
///
/// `episodes` are the session's `episode` rows in episode order. `None` when `seq` precedes
/// every run start.
pub fn episode_holding_seq(episodes: &[IndexedEpisode], seq: u64) -> Option<usize> {
    episodes
        .iter()
        .rposition(|episode| episode.run_started_seq.is_some_and(|start| start <= seq))
}

/// Where the episode's `RunStarted` sits among `frames`, if the frame at its recorded sequence
/// is a `RunStarted` at its recorded byte offset.
pub fn episode_start_index(
    episode: &IndexedEpisode,
    frames: &[crate::replay::PositionedFrame],
) -> Option<usize> {
    let seq = episode.run_started_seq?;
    let index = frames
        .iter()
        .position(|positioned| positioned.framed.seq == seq)?;
    let frame = &frames[index];
    (matches!(frame.framed.record, WalRecord::RunStarted { .. })
        && episode.run_started_off == Some(frame.off))
    .then_some(index)
}

/// Replay `frames` from a validated position through an episode's `RunStarted` and beyond.
///
/// With a seed, `frames` must continue its boundary, and the replay goes through
/// [`crate::replay::replay_seeded_at_validated_boundary`], which re-reads the boundary frame and
/// checks its sequence, hash, lineage and qualification and the frames' contiguity. Without a
/// seed, `frames` must begin at the log's first record, so nothing stored is relied on. Either
/// way the frames must hold the episode's `RunStarted` at its recorded sequence and offset, and
/// the replay must place that episode's input at the episode's recorded first ordinal. Any
/// mismatch returns the [`crate::replay::SeedFallback`] reason, and the caller replays the whole
/// log instead.
pub fn replay_from_episode_start(
    read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    seed: Option<&EpisodeSeed>,
    episode: &IndexedEpisode,
    frames: &[crate::replay::PositionedFrame],
) -> Result<EpisodeReplay, crate::replay::SeedFallback> {
    use crate::replay::SeedFallback;
    let run_started = episode_start_index(episode, frames)
        .ok_or_else(|| SeedFallback::new("episode start not in the frames read"))?;
    let (state, ordinal_base) = match seed {
        Some(seed) => (
            crate::replay::replay_seeded_at_validated_boundary(
                read_at,
                &seed.boundary,
                frames,
                seed.seed_origin.clone(),
                seed.seed_run_id.clone(),
                seed.seed_budget.clone(),
                None,
                seed.ordinal_base,
                false,
                seed.tag_seed,
                seed.held_steers.clone(),
            )?,
            seed.ordinal_base,
        ),
        None => {
            let start =
                first_record_offset(read_at).map_err(|_| SeedFallback::new("log unreadable"))?;
            // `run_started` was found, so `frames` is not empty.
            if frames[0].framed.seq != 1 || frames[0].off != start {
                return Err(SeedFallback::new(
                    "frames do not start at the log's first record",
                ));
            }
            if frames
                .windows(2)
                .any(|pair| pair[0].framed.seq + 1 != pair[1].framed.seq)
            {
                return Err(SeedFallback::new("frames have a sequence gap"));
            }
            let state = crate::replay::replay_seeded(
                frames.iter().map(|positioned| &positioned.framed.record),
                None,
                None,
                None,
                None,
                0,
                false,
                crate::tags::TagSeed::default(),
            )
            .map_err(|_| SeedFallback::new("replay refused the frames"))?;
            (state, 0)
        }
    };
    // Replay records one render episode per `RunStarted`, at the ordinal where its input starts.
    let starts_before = frames[..run_started]
        .iter()
        .filter(|positioned| matches!(positioned.framed.record, WalRecord::RunStarted { .. }))
        .count();
    let prompt_start = state
        .render_episodes
        .as_deref()
        .and_then(|episodes| episodes.get(starts_before))
        .map(|rendered| rendered.prompt_start as u64);
    if prompt_start != Some(episode.first_ord) {
        return Err(SeedFallback::new("episode first ordinal mismatch"));
    }
    Ok(EpisodeReplay {
        state,
        ordinal_base,
        run_started,
    })
}

#[cfg(test)]
mod placement_tests;

#[cfg(test)]
mod live_tests;

#[cfg(test)]
mod access_counter_tests {
    #[test]
    fn projection_source_reads_and_decodes_use_the_shared_counter() {
        use broca_wal::access_counter::{ReadClass, WalAccessCounter};
        let bytes = broca_wal::framing::encode(
            1,
            1,
            &broca_wal::WalRecord::TurnFinished {
                step_id: broca_protocol::StepId(1),
            },
        )
        .unwrap();
        let reader = |offset: u64, length: u64| {
            Ok(bytes
                .iter()
                .skip(offset as usize)
                .take(length as usize)
                .copied()
                .collect::<Vec<_>>())
        };
        let counter = WalAccessCounter::default();
        counter.operation("projection source").run(|| {
            assert_eq!(super::log_header_info(&reader).unwrap(), (0, None));
            let frames =
                super::decode_positioned_frames(&reader, 0, bytes.len() as u64, None).unwrap();
            assert_eq!(frames.len(), 1);
        });
        let snapshot = counter.snapshot();
        assert_eq!(snapshot[0].reads.len(), 2);
        assert_eq!(snapshot[0].reads[0].class, ReadClass::LineageHeader);
        assert_eq!(snapshot[0].reads[1].class, ReadClass::Origin);
        assert_eq!(snapshot[0].reads[1].length, bytes.len() as u64);
        assert_eq!(snapshot[0].frames_decoded, 1);
    }
}
