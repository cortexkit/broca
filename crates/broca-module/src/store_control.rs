//! The control stream of a session kept in the new storage: its records projected to today's
//! `session.subscribe` payloads, unit for unit.
//!
//! Today's projection (`broca_core::project_control_with_mids`) walks WAL records; this one walks
//! the new storage's records, which split today's records into a transcript and a ledger. The
//! payload of each unit is built by the same shared rules (`broca_core::projection`), so a unit's
//! content can't differ between the two storages; only the walk that decides which units each
//! record yields, and where, is new here.
//!
//! **Where units sit.** A unit's cursor names the record that yields it: its seq, and its index
//! among that record's units. Units sit on ledger records and on `EntryFacts`, never on a record
//! a seal may rewrite (an `Input` or `Assistant` loses its opaque payloads when it leaves the open
//! log), so a cursor keeps naming the same bytes for the session's whole life:
//! - a run's start, its end, a pause and a held call on their own records;
//! - a step's start, which announces its assistant message by the id it records
//!   (`StepStart::mid`), and its end, which carries the assistant message;
//! - a batch's start, which announces the tool-results message (`BatchStart::mid`), each call's
//!   dispatch and outcome, and the batch's end, which finishes that message;
//! - an input message's lifecycle on the `EntryFacts` that names it, since only those facts carry
//!   its id; a budget firing's units wait for its reminder's facts the same way.
//!
//! **Ids.** Message ids come from the records (`StepStart::mid`, `BatchStart::mid`,
//! `EntryFacts::mid`), never derived from positions, and are the ids `session.read` serves. A
//! call's key and schema pin come from its `ToolIntent`; a call answered without one takes its
//! key from its `ToolOutcome`.
//!
//! **State.** What a record needs from earlier records is the two folds' state (kept here as
//! copies, the conversation without its window) plus a few facts of the current run, step and
//! batch, which [`StoreControl::at_checkpoint`] rebuilds from the folds at a log's start.
//!
//! **Notices.** Text a send delivered with a tool result is a notice under the send id and
//! sender its `ResultAppend` carries. One whose record names no send id is never shown: its unit
//! exists, so a cursor naming it still resumes, but it is marked not visible.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use broca_core::held_calls::HeldCallsProjection;
use broca_core::projection::{
    boundary_unit, deferred_tool_started_event, input_units, message_lifecycle_event,
    message_shape_finish_reason, no_dispatch_outcome, notice_delivered_event, paused_unit,
    run_finished_unit, run_started_unit, schema_pin_text, tool_finished_event_at, tool_outcome,
    tool_started_event_at, IndeterminateToolCalls, RunEndFacts,
};
use broca_protocol::caller::{ControlUnit, RunFinishReason};
use broca_protocol::content::{AssistantMessage, FinishReason, Message, ToolCall, ToolResult};
use broca_protocol::{BatchId, RunId, StepId, ToolCallId};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::held::HeldBytes;
use broca_session_store::record::{CallOutcome, Placement, Record, Stream};
use cortexkit_role_llm_runner::live::LiveEvent;

/// One unit of a store session's control stream. Its cursor is `(seq, sub_index)`; the caller
/// adds the session's cursor identity.
#[derive(Debug, Clone)]
pub struct StoreUnit {
    pub seq: u64,
    pub sub_index: u32,
    pub unit: ControlUnit,
    pub event: LiveEvent,
    /// Whether a subscriber is sent it. Only a notice whose record names no send is hidden; a
    /// resume checks a cursor against every unit, hidden ones included.
    pub visible: bool,
}

/// The units one record yields, numbered in order.
struct Units<'a> {
    seq: u64,
    out: &'a mut Vec<StoreUnit>,
}

impl Units<'_> {
    fn push(&mut self, unit: ControlUnit, event: LiveEvent) {
        self.push_visible(unit, event, true);
    }

    fn push_visible(&mut self, unit: ControlUnit, event: LiveEvent, visible: bool) {
        let sub_index = self.out.len() as u32;
        self.out.push(StoreUnit {
            seq: self.seq,
            sub_index,
            unit,
            event,
            visible,
        });
    }

    /// A unit with no lifecycle event of its own, which today's stream pairs with a reset.
    fn plain(&mut self, unit: ControlUnit) {
        self.push(unit, LiveEvent::DisplayReset);
    }

    fn boundary(&mut self, mid: &str, event: LiveEvent) {
        self.push(boundary_unit(mid.to_owned()), event);
    }
}

/// What a call's dispatch recorded that its start and end events carry.
#[derive(Debug, Clone, Default)]
struct CallIdentity {
    key: Option<String>,
    schema_pin: Option<String>,
}

impl CallIdentity {
    fn stamp(&self, event: &mut LiveEvent) {
        match event {
            LiveEvent::ToolStarted(event) => {
                event.call_key.clone_from(&self.key);
                event.schema_pin.clone_from(&self.schema_pin);
            }
            LiveEvent::ToolFinished(event) => {
                event.call_key.clone_from(&self.key);
                event.schema_pin.clone_from(&self.schema_pin);
            }
            _ => {}
        }
    }
}

/// Units that wait for the `EntryFacts` naming the message they speak of.
#[derive(Debug, Clone)]
enum Waiting {
    /// An input message placed whose message id its entry facts haven't given yet, shared with
    /// the record that placed it.
    Input(std::sync::Arc<Message>),
    /// A budget firing whose reminder message this is.
    Budget(ControlUnit),
    /// A step's end whose assistant message had no announced id.
    StepEnd(Box<StepEndUnits>),
}

impl broca_session_store::held::HeldBytes for CallIdentity {
    fn heap_bytes(&self) -> usize {
        self.key.heap_bytes() + self.schema_pin.heap_bytes()
    }
}

impl broca_session_store::held::HeldBytes for Waiting {
    fn heap_bytes(&self) -> usize {
        match self {
            Waiting::Input(message) => message.heap_bytes(),
            // A control unit is a wire type of many shapes; its compact JSON stands in for its
            // heap. A budget firing waits for one record at most.
            Waiting::Budget(unit) => serde_json::to_vec(unit).map_or(0, |bytes| bytes.len()),
            Waiting::StepEnd(units) => {
                std::mem::size_of::<StepEndUnits>() + units.message.heap_bytes()
            }
        }
    }
}

/// What a step's end yields, kept until its assistant message's id is known.
#[derive(Debug, Clone)]
struct StepEndUnits {
    message: Arc<AssistantMessage>,
    step_id: u64,
    usage: broca_protocol::Usage,
    finish_reason: Option<FinishReason>,
    already_started: bool,
}

/// The projection of one store session's records, in seq order.
#[derive(Debug, Clone)]
pub struct StoreControl {
    /// The conversation fold without its window: how many messages are placed, and the
    /// assistant message held for its tool results.
    conversation: Conversation,
    operations: Operations,
    /// Ledger records at or below this seq are already folded into `operations`: a log's
    /// checkpoint can cover records its log still holds.
    covers_through: u64,
    /// The assistant message of the step that just ended, kept from its transcript record for
    /// the step's ledger end.
    step_message: Option<Arc<AssistantMessage>>,
    /// The calls of the current step's assistant message, in the order it made them.
    assistant_calls: Vec<ToolCall>,
    /// The id of the current step's assistant message.
    assistant_mid: Option<String>,
    /// Whether the current step's start announced its message, so its end finishes it.
    step_started: bool,
    /// The id of the open batch's tool-results message, while its assistant message is held.
    results_mid: Option<String>,
    started_tools: HashSet<ToolCallId>,
    calls: HashMap<ToolCallId, CallIdentity>,
    indeterminate: IndeterminateToolCalls,
    held: HeldCallsProjection,
    wait: crate::store_awaits::RunWait,
    has_completed_step: bool,
    final_step_finish_reason: Option<FinishReason>,
    cancelled: bool,
    /// The calls of the batch that just finished, each with its tool-results message, its place
    /// among the assistant message's calls and its key: where delivered text attaches.
    carriers: HashMap<ToolCallId, (String, u32, Option<String>)>,
    /// The tool reply waiting for the ledger record that says how it ended, with when it
    /// finished.
    reply: Option<(ToolResult, Option<u64>)>,
    /// Calls made by input messages of the placement under way, for results that answer them.
    input_owners: HashMap<ToolCallId, String>,
    /// Whether the previous record was an input message: consecutive inputs are one placement.
    last_was_input: bool,
    /// Units waiting for the facts of the message at an ordinal.
    waiting: Vec<(u64, Waiting)>,
}

impl Default for StoreControl {
    fn default() -> Self {
        Self::at_checkpoint(&Conversation::new(false), &Operations::default(), 0)
    }
}

impl StoreControl {
    pub(crate) fn with_labels(mut self, labels: crate::store_awaits::Labels) -> Self {
        self.wait.set_labels(labels);
        self
    }
    /// An estimate of the memory this projection holds, walked when asked (see
    /// `broca_session_store::held`): its copies of the folds, and the facts it keeps of the
    /// current run, step and batch. It never holds the units it made. The shared call-state
    /// projections it keeps from `broca_core` count at their inline size: they hold a few ids of
    /// the open batch at most. Reported by the memory report and by the store actor's read-cost
    /// measure (`store_actor::read_cost`).
    pub fn held_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.conversation.heap_bytes()
            + self.operations.heap_bytes()
            + self.step_message.heap_bytes()
            + self.assistant_calls.heap_bytes()
            + self.assistant_mid.heap_bytes()
            + self.results_mid.heap_bytes()
            + self.started_tools.heap_bytes()
            + self.calls.heap_bytes()
            + self.carriers.heap_bytes()
            + self.reply.heap_bytes()
            + self.input_owners.heap_bytes()
            + self.waiting.heap_bytes()
    }
    /// The projection at the start of a log whose checkpoint holds `conversation` and
    /// `operations`, folded through `covers_through`. At a session's first log they are empty.
    ///
    /// The run's facts this projection keeps besides the folds are rebuilt from them: the held
    /// assistant message and its id, the open batch's calls, their keys and what is held. A
    /// log starts between steps, so no step's start is waiting for its end.
    pub fn at_checkpoint(
        conversation: &Conversation,
        operations: &Operations,
        covers_through: u64,
    ) -> Self {
        let mut control = Self {
            conversation: conversation.without_window(),
            operations: operations.clone(),
            covers_through,
            step_message: None,
            assistant_calls: Vec::new(),
            assistant_mid: None,
            step_started: false,
            results_mid: None,
            started_tools: HashSet::new(),
            calls: HashMap::new(),
            indeterminate: IndeterminateToolCalls::default(),
            held: HeldCallsProjection::default(),
            wait: Default::default(),
            has_completed_step: false,
            final_step_finish_reason: None,
            cancelled: false,
            carriers: HashMap::new(),
            reply: None,
            input_owners: HashMap::new(),
            last_was_input: false,
            waiting: Vec::new(),
        };
        if let Some((_, message, entry)) = conversation.open_entry() {
            control.assistant_calls = calls_of(message);
            control.assistant_mid = entry.facts.as_ref().map(|facts| facts.mid.clone());
        }
        let Some(run) = operations.run.as_ref() else {
            return control;
        };
        if let Some(start) = &run.start {
            let _ = control
                .wait
                .record(&Record::RunStart(Box::new(start.clone())));
        }
        control.has_completed_step = run.completed_steps > 0;
        control.final_step_finish_reason = run.last_finish_reason;
        control.cancelled = run.cancel.is_some() && run.finished.is_none();
        for call in &run.unknown {
            control.indeterminate.record_dispatch_terminal(call, true);
        }
        if let Some(batch) = &run.open_batch {
            if conversation.open_tool_turn().is_some() {
                control.results_mid.clone_from(&batch.results_mid);
            }
            control.held.step_finished(control.assistant_calls.iter());
            for call in &batch.calls {
                if call.intent || call.terminal.is_some() {
                    control.started_tools.insert(call.id.clone());
                }
                if call.intent && call.terminal.is_none() {
                    control.indeterminate.record_dispatch_intent(&call.id);
                }
                // A call's key is its intent's, or its outcome's when it had no intent.
                if call.intent || call.key.is_some() {
                    control.calls.insert(
                        call.id.clone(),
                        CallIdentity {
                            key: call.key.clone(),
                            schema_pin: None,
                        },
                    );
                }
                if let Some(held) = call.held_reply.as_ref().filter(|_| call.terminal.is_none()) {
                    let _ = control.wait.record(&Record::ToolHeld(held.clone()));
                    control.held.held(
                        &BatchId(held.batch_id.clone()),
                        &held.tool_call_id,
                        &held.call_key,
                        &held.module,
                        &held.reason,
                        held.held_at_ms,
                    );
                }
            }
        }
        control
    }

    fn run_id(&self) -> Option<RunId> {
        self.operations
            .run
            .as_ref()
            .map(|run| RunId(run.run_id.clone()))
    }

    /// Projects the record at `seq`. `next` is the record after it, which a budget firing looks
    /// at to tell whether it placed a reminder.
    pub fn project(
        &mut self,
        seq: u64,
        record: &Record,
        next: Option<&Record>,
    ) -> Result<Vec<StoreUnit>, String> {
        self.wait.record(record)?;
        let mut out = Vec::new();
        let mut units = Units { seq, out: &mut out };
        let is_input = matches!(record, Record::Input(_));
        if is_input && !self.last_was_input {
            self.input_owners.clear();
        }
        self.last_was_input = is_input;
        self.before_folds(record, next, &mut units)?;
        match record.stream() {
            Stream::Transcript => {
                self.conversation
                    .apply(record)
                    .map_err(|error| format!("conversation: {error:?}"))?;
                self.conversation.clear_window();
            }
            // A seal's or a revert's change records close the log they end; the projection goes
            // on into the next log, so its copy of the fold never closes.
            Stream::Ledger
                if seq > self.covers_through
                    && !matches!(record, Record::ChangeIntent(_) | Record::ChangeAborted(_)) =>
            {
                self.operations
                    .apply_at(Some(seq), record)
                    .map_err(|error| format!("operations: {error:?}"))?;
            }
            Stream::Ledger => {}
        }
        self.after_folds(record, &mut units);
        // Error and finished controls from one record use the same clock and wait snapshot.
        let wait = self
            .wait
            .snapshot(crate::store_awaits::now_ms(), None, None, None);
        for unit in &mut out {
            match &mut unit.unit {
                ControlUnit::Status { status, .. } => {
                    if matches!(status.state.as_str(), "active" | "paused") {
                        status.state = wait.state.into();
                    }
                    status.awaits = wait.awaits.clone();
                    status.reason = wait.reason.clone().map(|reason| reason.into());
                }
                ControlUnit::Paused { awaits, .. }
                | ControlUnit::Error { awaits, .. }
                | ControlUnit::RunFinished { awaits, .. } => {
                    *awaits = wait.awaits.clone().map(Box::new)
                }
                _ => {}
            }
        }
        Ok(out)
    }

    /// The units a record yields from the state before it is folded.
    fn before_folds(
        &mut self,
        record: &Record,
        next: Option<&Record>,
        units: &mut Units<'_>,
    ) -> Result<(), String> {
        match record {
            Record::RunStart(start) => {
                self.reset_run();
                self.held.run_started();
                let rich_ui = start
                    .run_choices
                    .as_ref()
                    .and_then(|choices| choices.get("manifest"))
                    .and_then(|manifest| manifest.get("rich_ui"))
                    .and_then(|rich_ui| serde_json::from_value(rich_ui.clone()).ok());
                let (unit, event) = run_started_unit(
                    &RunId(start.run_id.clone()),
                    start
                        .submission_id
                        .clone()
                        .map(broca_protocol::SubmissionId),
                    rich_ui,
                );
                units.push(unit, event);
            }
            Record::StepStart(start) => {
                self.step_message = None;
                self.assistant_calls.clear();
                self.assistant_mid.clone_from(&start.mid);
                self.step_started = true;
                self.started_tools.clear();
                self.calls.clear();
                self.carriers.clear();
                self.held.step_started();
                units.push(
                    ControlUnit::StepStarted {
                        step_id: StepId(start.step_id),
                    },
                    message_lifecycle_event(start.mid.clone(), false),
                );
            }
            Record::Assistant(assistant) => {
                self.step_message = Some(Arc::new(AssistantMessage {
                    message_id: assistant.provider_message_id.clone(),
                    content: assistant.message.content.clone(),
                }));
            }
            Record::StepEnd(end) => self.step_end(end, units),
            Record::BatchStart(batch) => {
                self.indeterminate.reset_batch();
                self.results_mid = batch
                    .mid
                    .clone()
                    .filter(|_| self.conversation.open_tool_turn().is_some());
                if let Some(mid) = self.results_mid.clone() {
                    units.boundary(&mid, message_lifecycle_event(Some(mid.clone()), false));
                }
            }
            Record::ToolIntent(intent) => self.intent(intent, units),
            Record::ToolReply(reply) => {
                self.reply = Some((reply.result.clone(), reply.finished_at_ms));
            }
            Record::ToolOutcome(outcome) => {
                let result = self
                    .reply
                    .take()
                    .map(|(result, _)| result)
                    .filter(|result| result.tool_call_id == outcome.tool_call_id);
                if let Some(result) = result {
                    let name = match outcome.outcome {
                        CallOutcome::NotDispatched => outcome
                            .detail
                            .as_ref()
                            .and_then(|detail| {
                                serde_json::from_value::<broca_wal::NoDispatchReason>(
                                    detail.clone().parse(),
                                )
                                .ok()
                            })
                            .map_or("error", |reason| no_dispatch_outcome(&reason)),
                        other => tool_outcome(dispatch_outcome(other), &result, self.cancelled),
                    };
                    self.indeterminate.record_dispatch_terminal(
                        &outcome.tool_call_id,
                        outcome.outcome == CallOutcome::Unknown,
                    );
                    // A copy answered without a dispatch intent has its own key on its outcome,
                    // apart from any other copy of its id.
                    let own = outcome.call_key.as_ref().map(|key| CallIdentity {
                        key: Some(key.clone()),
                        schema_pin: None,
                    });
                    self.terminal(&result, name, outcome.finished_at_ms, own, units);
                }
                let status = self
                    .held
                    .terminal(&BatchId(outcome.batch_id.clone()), &outcome.tool_call_id);
                self.status(status, units);
            }
            Record::ToolHeld(held) => {
                let status = self.held.held(
                    &BatchId(held.batch_id.clone()),
                    &held.tool_call_id,
                    &held.call_key,
                    &held.module,
                    &held.reason,
                    held.held_at_ms,
                );
                self.status(status, units);
            }
            Record::BatchEnd(_) => self.batch_end(units),
            Record::RunEnd(end) => {
                if let Some(error) = &end.error {
                    let error: broca_protocol::ProviderError =
                        serde_json::from_value(error.clone().parse())
                            .map_err(|e| format!("a run end's error: {e}"))?;
                    units.plain(ControlUnit::Error {
                        awaits: None,
                        error: error.into(),
                    });
                }
                let run_id = self.run_id();
                let (unit, event) = run_finished_unit(RunEndFacts {
                    run_id: run_id.as_ref(),
                    reason: end.reason,
                    usage: end.usage.unwrap_or_default(),
                    has_completed_step: self.has_completed_step,
                    final_step_finish_reason: self.final_step_finish_reason,
                    indeterminate: &self.indeterminate,
                    retries_used: end.retries_used,
                });
                units.push(unit, event);
                self.end_turn();
                self.cancelled = false;
                self.carriers.clear();
                let status = self.held.run_finished(end.reason);
                self.status(status, units);
            }
            Record::Pause(pause) => {
                if pause.reason != broca_protocol::PauseReason::ToolHeld {
                    let error = pause
                        .error
                        .as_ref()
                        .map(|error| serde_json::from_value(error.clone().parse()))
                        .transpose()
                        .map_err(|e| format!("a pause's error: {e}"))?;
                    let resume_not_before_ms = pause
                        .account_limit
                        .as_ref()
                        .and_then(|limit| limit.parse().get("resume_not_before_ms")?.as_u64());
                    let (unit, event) = paused_unit(
                        &RunId(pause.run_id.clone()),
                        pause.reason,
                        resume_not_before_ms,
                        error,
                    );
                    units.push(unit, event);
                }
                self.held.paused(pause.reason);
            }
            Record::CancelRequest(cancel) => {
                self.cancelled |= self
                    .operations
                    .run
                    .as_ref()
                    .is_some_and(|run| run.run_id == cancel.run_id);
            }
            Record::Retraction(retraction) => {
                if let Some(failure) = &retraction.failure {
                    let failure: broca_wal::QueuedAdmissionFailure =
                        serde_json::from_value(failure.clone())
                            .map_err(|e| format!("a queued send's admission failure: {e}"))?;
                    self.reset_run();
                    let (unit, event) = run_started_unit(
                        &failure.run_id,
                        Some(broca_protocol::SubmissionId(
                            retraction.submission_id.clone(),
                        )),
                        None,
                    );
                    units.push(unit, event);
                    units.plain(ControlUnit::Error {
                        awaits: None,
                        error: failure.error.into(),
                    });
                    let (unit, event) = run_finished_unit(RunEndFacts {
                        run_id: Some(&failure.run_id),
                        reason: RunFinishReason::Error,
                        usage: Default::default(),
                        has_completed_step: false,
                        final_step_finish_reason: None,
                        indeterminate: &IndeterminateToolCalls::default(),
                        retries_used: 0,
                    });
                    units.push(unit, event);
                }
            }
            Record::BudgetFire(fire) => {
                let unit = ControlUnit::BudgetFired {
                    run_id: RunId(fire.run_id.clone()),
                    kind: match fire.firing {
                        broca_session_store::record::BudgetFiring::Nudge => {
                            broca_protocol::request::BudgetKind::Nudge
                        }
                        broca_session_store::record::BudgetFiring::Finalize => {
                            broca_protocol::request::BudgetKind::Finalize
                        }
                    },
                    threshold: fire.threshold,
                    ordinal: fire.ordinal,
                };
                let reminder = matches!(
                    next,
                    Some(Record::Input(input)) if input.placement == Placement::BudgetNudge
                );
                if reminder {
                    // The reminder takes the next ordinal; its facts name it.
                    self.waiting
                        .push((self.conversation.placed + 1, Waiting::Budget(unit)));
                } else {
                    units.plain(unit);
                }
            }
            // A ride delivered with its envelope is a notice, once per ride, under the key, send
            // id and sender its envelope recorded, and with the agent its sender spoke for, as
            // today's control stream names it. Text delivered before envelopes existed is no
            // notice, and yields nothing.
            Record::ResultAppend(append) => {
                let carrier = self.carriers.get(&append.tool_call_id).cloned();
                if let (Some((mid, part, _)), Some(ride), Some(send_id)) =
                    (carrier, &append.ride, &append.send_id)
                {
                    if let Some(mut event) = notice_delivered_event(
                        mid.clone(),
                        part,
                        Some(ride.call_key.clone()),
                        send_id.clone(),
                        ride.sender.clone(),
                        &append.text,
                    ) {
                        if let cortexkit_role_llm_runner::live::LiveEvent::NoticeDelivered(notice) =
                            &mut event
                        {
                            notice.speaks_for =
                                crate::store_read::speaks_for(ride.speaks_for.as_ref());
                        }
                        units.boundary(&mid, event);
                    }
                }
            }
            Record::EntryFacts(facts) => self.facts(facts, units),
            Record::Event(event) => self.event(event, units)?,
            _ => {}
        }
        Ok(())
    }

    /// What a record yields from the state after it is folded: a placed input message waits
    /// for its facts.
    fn after_folds(&mut self, record: &Record, _units: &mut Units<'_>) {
        if let Record::Input(input) = record {
            if input.placement != Placement::BudgetNudge {
                self.waiting.push((
                    self.conversation.placed,
                    Waiting::Input(std::sync::Arc::clone(&input.message)),
                ));
            }
        }
    }

    fn reset_run(&mut self) {
        self.end_turn();
        self.step_started = false;
        self.indeterminate.reset_episode();
        self.has_completed_step = false;
        self.final_step_finish_reason = None;
        self.cancelled = false;
        self.carriers.clear();
        self.reply = None;
    }

    /// The step's assistant message and its tool-results message are done with.
    fn end_turn(&mut self) {
        self.step_message = None;
        self.assistant_calls.clear();
        self.assistant_mid = None;
        self.results_mid = None;
        self.started_tools.clear();
        self.calls.clear();
    }

    fn status(
        &self,
        status: Option<cortexkit_role_llm_runner::run::RunStatus>,
        units: &mut Units<'_>,
    ) {
        if let (Some(status), Some(run_id)) = (status, self.run_id()) {
            units.plain(ControlUnit::Status {
                run_id,
                status: Box::new(status),
            });
        }
    }

    fn step_end(&mut self, end: &broca_session_store::record::StepEnd, units: &mut Units<'_>) {
        let Some(message) = self.step_message.take() else {
            return;
        };
        let already_started = std::mem::take(&mut self.step_started);
        self.final_step_finish_reason = end.finish_reason;
        self.has_completed_step = true;
        self.held.step_finished(message.tool_calls());
        self.assistant_calls = message.tool_calls().cloned().collect();
        self.started_tools.clear();
        self.calls.clear();
        self.carriers.clear();
        let step = StepEndUnits {
            message,
            step_id: end.step_id,
            usage: end.usage.unwrap_or_default(),
            finish_reason: end.finish_reason,
            already_started,
        };
        match self.assistant_mid.clone() {
            Some(mid) => emit_step_end(&mid, &step, units),
            // Without an announced id, the message's facts name it: they follow at once.
            None => {
                let ordinal = match self.conversation.open_tool_turn() {
                    Some(_) => self.conversation.placed + 1,
                    None => self.conversation.placed,
                };
                self.waiting
                    .push((ordinal, Waiting::StepEnd(Box::new(step))));
            }
        }
    }

    fn intent(&mut self, intent: &broca_session_store::record::ToolIntent, units: &mut Units<'_>) {
        self.indeterminate
            .record_dispatch_intent(&intent.tool_call_id);
        self.held
            .intent(&intent.tool_call_id, intent.tool_name.as_deref());
        let Some(call) = self
            .assistant_calls
            .iter()
            .find(|call| call.tool_call_id == intent.tool_call_id)
            .cloned()
        else {
            return;
        };
        let Some(mid) = self.assistant_mid.clone() else {
            return;
        };
        let name = intent.tool_name.as_deref().unwrap_or(&call.tool_name);
        let identity = CallIdentity {
            key: intent.call_key.clone(),
            schema_pin: intent
                .schema_pin
                .as_ref()
                .and_then(|pin| {
                    serde_json::from_value::<broca_wal::DispatchSchemaPin>(pin.clone().parse()).ok()
                })
                .and_then(|pin| schema_pin_text(name, &pin)),
        };
        if self.started_tools.insert(intent.tool_call_id.clone()) {
            let mut event = tool_started_event_at(
                mid.clone(),
                &call,
                intent.tool_name.as_deref(),
                Some(&(*intent.args.as_ref().unwrap_or(&call.input)).parse()),
                intent.dispatched_at_ms,
            );
            identity.stamp(&mut event);
            units.boundary(&mid, event);
        }
        self.calls.insert(intent.tool_call_id.clone(), identity);
    }

    /// A call's terminal: its result unit, and its lifecycle in the assistant message that made
    /// it. A call with no recorded dispatch starts and ends here. `own` is the identity of a copy
    /// answered without an intent, which its outcome recorded; any other copy takes the identity
    /// its intent gave its id.
    fn terminal(
        &mut self,
        result: &ToolResult,
        outcome: &str,
        observed_at: Option<u64>,
        own: Option<CallIdentity>,
        units: &mut Units<'_>,
    ) {
        let id = &result.tool_call_id;
        let call = self
            .assistant_calls
            .iter()
            .find(|call| &call.tool_call_id == id)
            .cloned();
        let owner = self.assistant_mid.clone().filter(|_| call.is_some());
        let first_start = call.is_some() && self.started_tools.insert(id.clone());
        let identity = own.unwrap_or_else(|| self.calls.get(id).cloned().unwrap_or_default());
        let finished = |owner: &str| {
            let mut event =
                tool_finished_event_at(owner.to_owned(), id, outcome, result, observed_at);
            identity.stamp(&mut event);
            event
        };
        let event = match (&owner, &call) {
            (Some(owner), Some(call)) if first_start => {
                let mut event = tool_started_event_at(owner.clone(), call, None, None, observed_at);
                identity.stamp(&mut event);
                event
            }
            (Some(owner), Some(_)) => finished(owner),
            _ => LiveEvent::DisplayReset,
        };
        units.push(
            ControlUnit::ToolResult {
                result: result.clone(),
            },
            event,
        );
        if first_start {
            if let Some(owner) = &owner {
                units.boundary(owner, finished(owner));
            }
        }
    }

    fn batch_end(&mut self, units: &mut Units<'_>) {
        self.held.batch_finished();
        let closed = self
            .operations
            .run
            .as_ref()
            .and_then(|run| run.open_batch.as_ref())
            .is_some_and(|batch| batch.calls.iter().all(|call| call.terminal.is_some()));
        let Some(results) = self.results_mid.clone() else {
            return;
        };
        for (part, call) in self.assistant_calls.iter().enumerate() {
            if let Ok(part) = u32::try_from(part) {
                let key = self
                    .calls
                    .get(&call.tool_call_id)
                    .and_then(|identity| identity.key.clone());
                self.carriers
                    .insert(call.tool_call_id.clone(), (results.clone(), part, key));
            }
        }
        if closed {
            units.boundary(
                &results,
                message_lifecycle_event(Some(results.clone()), true),
            );
            self.end_turn();
        }
    }

    fn facts(&mut self, facts: &broca_session_store::record::EntryFacts, units: &mut Units<'_>) {
        let (named, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.waiting)
            .into_iter()
            .partition(|(ordinal, _)| *ordinal == facts.ordinal);
        self.waiting = waiting;
        let mid = facts.mid.as_str();
        for (_, waiting) in named {
            match waiting {
                Waiting::Input(message) => {
                    for unit in input_units(&mut self.input_owners, mid.to_owned(), &message) {
                        let event = unit.event(|mid: &String| mid.clone());
                        units.boundary(unit.position(), event);
                    }
                }
                Waiting::Budget(unit) => {
                    units.push(unit, message_lifecycle_event(Some(mid.to_owned()), false));
                    units.boundary(mid, message_lifecycle_event(Some(mid.to_owned()), true));
                }
                Waiting::StepEnd(step) => {
                    self.assistant_mid = Some(mid.to_owned());
                    emit_step_end(mid, &step, units);
                }
            }
        }
    }

    /// A ledger event whose kind only converted sessions hold: today's record, carried whole
    /// apart from what the conversion moved into transcript records.
    fn event(
        &mut self,
        event: &broca_session_store::record::Event,
        units: &mut Units<'_>,
    ) -> Result<(), String> {
        use serde_json::Value;
        let body = event.body.as_ref().unwrap_or(&Value::Null);
        let u64_of = |key: &str| body.get(key).and_then(Value::as_u64);
        let str_of = |key: &str| body.get(key).and_then(Value::as_str);
        let run_id = self.run_id();
        match event.name.as_str() {
            "context_ceiling_wait" => {
                let waits = body.get("unavailable").is_none_or(Value::is_null);
                if let (true, Some(run_id), Some(deadline_ms)) =
                    (waits, run_id, u64_of("deadline_ms"))
                {
                    units.plain(ControlUnit::ContextCeilingWait {
                        run_id,
                        reason: "context_ceiling_wait".into(),
                        deadline_ms,
                    });
                }
            }
            "compaction_ride_wait" => {
                if let (Some(run_id), Some(deadline_ms)) = (run_id, u64_of("deadline_ms")) {
                    units.plain(ControlUnit::CompactionWait {
                        run_id,
                        reason: cortexkit_role_llm_runner::run::RUN_REASON_COMPACTION_WAIT.into(),
                        deadline_ms,
                    });
                }
            }
            "tool_call_deferred" => {
                let call = str_of("call_id").and_then(|id| {
                    self.assistant_calls
                        .iter()
                        .find(|call| call.tool_call_id.0 == id)
                        .cloned()
                });
                if let (Some(mid), Some(call), Some(tool), Some(args)) = (
                    self.assistant_mid.clone(),
                    call,
                    str_of("tool"),
                    str_of("args_jcs"),
                ) {
                    if self.started_tools.insert(call.tool_call_id.clone()) {
                        units.boundary(
                            &mid,
                            deferred_tool_started_event(
                                mid.clone(),
                                &call,
                                tool,
                                args,
                                u64_of("deferred_at_ms"),
                            ),
                        );
                    }
                }
            }
            // An approval's closing result is the transcript reply just before it.
            "approval_recorded" => {
                if let Some((result, closed_at)) = self.reply.take() {
                    self.indeterminate
                        .record_dispatch_terminal(&result.tool_call_id, false);
                    self.terminal(&result, "denied", closed_at, None, units);
                }
            }
            // A recovery's close-out result is the transcript reply just before it; it yields
            // no result unit, only the end of the call's hold.
            "recovery_call_closed" => {
                self.reply = None;
                let closed_run = event.run_id.clone().map(RunId);
                if let (Some(closed_run), Some(tool_call_id)) = (
                    closed_run,
                    str_of("tool_call_id").map(|id| ToolCallId(id.to_owned())),
                ) {
                    if run_id.as_ref() == Some(&closed_run) {
                        self.indeterminate
                            .record_dispatch_terminal(&tool_call_id, true);
                    }
                    let call_key = str_of("call_key").map(str::to_owned);
                    let status = self.held.recovery_call_closed(
                        &closed_run,
                        call_key.as_ref(),
                        &tool_call_id,
                        str_of("batch_id").map(|id| BatchId(id.to_owned())),
                        u64_of("step_id").map(StepId),
                    );
                    self.status(status, units);
                }
            }
            "recovery_cut" => {
                let runs: Vec<broca_wal::recovery::RecoveryRun> = body
                    .get("recovery")
                    .and_then(|recovery| recovery.get("runs"))
                    .map(|runs| serde_json::from_value(runs.clone()))
                    .transpose()
                    .map_err(|e| format!("a carried recovery cut: {e}"))?
                    .unwrap_or_default();
                for run in &runs {
                    if run_id.as_ref() == Some(&run.run_id) {
                        for call in &run.calls {
                            self.indeterminate
                                .record_dispatch_terminal(&call.tool_call_id, true);
                        }
                    }
                }
                self.held.recovery_cut(&runs);
            }
            _ => {}
        }
        Ok(())
    }
}

/// The units of a step's end: the assistant message, each call it made, the step's end, and,
/// when the step's start didn't announce the message, its finish.
fn emit_step_end(mid: &str, step: &StepEndUnits, units: &mut Units<'_>) {
    units.push(
        ControlUnit::AssistantMessage {
            message: Arc::clone(&step.message),
        },
        message_lifecycle_event(Some(mid.to_owned()), step.already_started),
    );
    for call in step.message.tool_calls() {
        units.plain(ControlUnit::ToolCall { call: call.clone() });
    }
    units.plain(ControlUnit::StepFinished {
        step_id: StepId(step.step_id),
        usage: step.usage,
        finish_reason: step
            .finish_reason
            .unwrap_or_else(|| message_shape_finish_reason(&step.message)),
    });
    if !step.already_started {
        units.boundary(mid, message_lifecycle_event(Some(mid.to_owned()), true));
    }
}

fn calls_of(message: &Message) -> Vec<ToolCall> {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            broca_protocol::ContentBlock::ToolCall(call) => Some(call.clone()),
            _ => None,
        })
        .collect()
}

fn dispatch_outcome(outcome: CallOutcome) -> broca_wal::DispatchOutcome {
    match outcome {
        CallOutcome::Completed | CallOutcome::NotDispatched => {
            broca_wal::DispatchOutcome::Completed
        }
        CallOutcome::Unknown => broca_wal::DispatchOutcome::Unknown,
        CallOutcome::Interrupted => broca_wal::DispatchOutcome::Interrupted,
        CallOutcome::MaxSteps => broca_wal::DispatchOutcome::MaxSteps,
    }
}

/// Projects `records`, in seq order, from `control`. Each record's successor is the next one.
pub fn project_all(
    control: &mut StoreControl,
    records: &[(u64, Record)],
) -> Result<Vec<StoreUnit>, String> {
    let mut units = Vec::new();
    for (at, (seq, record)) in records.iter().enumerate() {
        let next = records.get(at + 1).map(|(_, next)| next);
        units.extend(control.project(*seq, record, next)?);
    }
    Ok(units)
}
