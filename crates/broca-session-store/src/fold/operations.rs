//! The operational fold: the state of a session's latest run, rebuilt from ledger records alone.
//!
//! It answers what a resume needs to decide what to do next: whether the run ended, paused or
//! is still open; which of its tool calls may have run without a recorded result; how far its
//! step and tool-call budgets have gone; and which steered messages still wait for a step
//! boundary.
//!
//! The ledger records every decision the run loop made, so the fold never re-derives one. A
//! budget firing names the nudge that fired, and a step records whether it met a stop
//! condition. The fold only follows the records.
//!
//! This covers the run, step, tool and budget records. Approvals, recovery after damage,
//! refused steps and the input-token budget aren't folded yet: a log holding one of their
//! records is refused by name, never folded as if the record weren't there.

use broca_protocol::caller::{PauseReason, RunFinishReason};
use broca_protocol::content::FinishReason;
use broca_protocol::ids::ToolCallId;
use broca_protocol::usage::Usage;

use crate::fold::Bound;
use crate::record::{BudgetFiring, CallOutcome, CancelPoint, Record, Stream};

/// Why the ledger can't be folded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldError {
    /// A transcript record was handed to the operational fold.
    TranscriptRecord(&'static str),
    /// A record this fold doesn't fold yet, named by its old type.
    NotFolded(String),
    /// A tool batch started over another batch that still holds evidence of a call.
    OverlappingBatch,
    /// A record arrived after a change's intent closed the log, other than that change's abort;
    /// or an abort with no open change.
    LogClosed(&'static str),
}

/// The ledger events that change a run's state but aren't folded yet.
const NOT_FOLDED: &[&str] = &[
    "approval_recorded",
    "awaiting_approval",
    "input_token_budget_exhausted",
    "model_step_refused",
    "recovery_call_closed",
    "recovery_cut",
    "tool_call_deferred",
];

crate::message! {
    /// One planned call of an open tool batch, with what the ledger recorded about it.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Call {
        1 req id: ToolCallId,
        /// A dispatch intent was recorded: the call may have left.
        2 req intent: bool,
        /// A provider took the call to answer later; `Some(true)` when later calls wait for it.
        3 opt held: bool,
        4 opt terminal: CallOutcome,
        /// The key its intent recorded, which a resume sends it again with.
        5 opt key: String,
        /// The provider's held reply, which a resume rejoins the call's wait with.
        6 opt held_reply: crate::record::ToolHeld,
        /// The input its intent recorded when it differs from the model's message (an approval
        /// policy narrowed it); a resume that sends the call again sends this.
        7 opt args: serde_json::Value,
    }
}

/// What a resume may do with one call of an open batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallState {
    /// Its outcome is recorded.
    Terminated,
    /// It may have run: an intent and no outcome.
    Dispatched,
    /// A provider holds it and will answer later.
    ProviderHeld,
    /// It waits behind a sequential held call and was never sent.
    HeldSuccessor,
    /// No intent and no outcome: it never left.
    NotSent,
}

state! {
    /// A tool batch whose calls don't all have outcomes yet.
    #[derive(Debug, Clone, PartialEq)]
    pub struct OpenBatch {
        1 req pub batch_id: String => { owner: "tool router", reset: "batch_start", bound: Bound::Fixed },
        2 req pub step_id: u64 => { owner: "tool router", reset: "batch_start", bound: Bound::Fixed },
        3 rep pub calls: Call => {
            owner: "tool router",
            reset: "batch_start",
            bound: Bound::OpenWork("one step's tool calls"),
        },
    }
}

impl OpenBatch {
    fn holds_call_evidence(&self) -> bool {
        self.calls
            .iter()
            .any(|call| call.intent || call.terminal.is_some())
    }

    /// The first planned copy of `id` that has no outcome yet. A call id can repeat within a
    /// batch; outcomes are recorded in plan order, and only the first copy is ever dispatched.
    fn first_open(&mut self, id: &ToolCallId) -> Option<&mut Call> {
        self.calls
            .iter_mut()
            .find(|call| &call.id == id && call.terminal.is_none())
    }

    fn first_copy(&self, position: usize) -> bool {
        !self.calls[..position]
            .iter()
            .any(|call| call.id == self.calls[position].id)
    }

    /// The id of the planned call at `position`.
    pub fn id(&self, position: usize) -> &ToolCallId {
        &self.calls[position].id
    }

    /// The key the intent of the planned call at `position` recorded, if it has one.
    pub fn key(&self, position: usize) -> Option<&str> {
        self.calls[position].key.as_deref()
    }

    /// The input the intent of the planned call at `position` recorded, when it differs from the
    /// model's message.
    pub fn args(&self, position: usize) -> Option<&serde_json::Value> {
        self.calls[position].args.as_ref()
    }

    /// The provider's held reply for the planned call at `position`, if a provider holds it.
    pub fn held_reply(&self, position: usize) -> Option<&crate::record::ToolHeld> {
        self.calls[position].held_reply.as_ref()
    }

    /// The state of the planned call `id`, if the batch plans it.
    pub fn state_of(&self, id: &ToolCallId) -> Option<CallState> {
        let position = self.calls.iter().position(|call| &call.id == id)?;
        Some(self.state(position))
    }

    pub fn state(&self, position: usize) -> CallState {
        let call = &self.calls[position];
        if call.terminal.is_some() {
            return CallState::Terminated;
        }
        if !(call.intent && self.first_copy(position)) {
            return CallState::NotSent;
        }
        if call.held.is_some() {
            return CallState::ProviderHeld;
        }
        let behind_held = self.calls[..position].iter().any(|earlier| {
            earlier.held == Some(true)
                && matches!(
                    earlier.terminal,
                    None | Some(CallOutcome::Interrupted | CallOutcome::MaxSteps)
                )
        });
        if behind_held {
            CallState::HeldSuccessor
        } else {
            CallState::Dispatched
        }
    }

    /// Calls that may have run without a recorded result.
    fn indeterminate(&self) -> impl Iterator<Item = &ToolCallId> {
        (0..self.calls.len())
            .filter(|position| self.state(*position) == CallState::Dispatched)
            .map(|position| &self.calls[position].id)
    }

    /// When every call ended with a stop limit's synthetic outcome, the run hit that limit
    /// before dispatching: the reason it ended with.
    fn hard_stop(&self) -> Option<RunFinishReason> {
        let mut max_steps = false;
        for call in &self.calls {
            match call.terminal {
                Some(CallOutcome::MaxSteps) => max_steps = true,
                Some(CallOutcome::Interrupted) => {}
                _ => return None,
            }
        }
        (!self.calls.is_empty()).then_some(if max_steps {
            RunFinishReason::MaxSteps
        } else {
            RunFinishReason::Completed
        })
    }
}

crate::message! {
    /// Messages accepted into the running run that wait for its next step boundary: a steer's,
    /// or a queued send's delivered as user or turn-scoped system messages. The record that
    /// accepted them keeps them; the boundary places copies in the transcript.
    #[derive(Debug, Clone, PartialEq)]
    pub struct PendingInput {
        /// The seq of the record that accepted the messages; absent when folded without seqs.
        1 opt seq: u64,
        2 req placement: crate::record::Placement,
        3 rep messages: broca_protocol::content::Message,
        4 opt written_at_ms: u64,
        /// The v1 part numbers, one entry per message.
        5 opt tags_v1: serde_json::Value,
        /// The queued send a delivery came from.
        6 opt submission_id: String,
    }
}

impl PendingInput {
    /// The v1 part numbers of the `index`th message, when the record lists one entry per
    /// message.
    pub fn tags_at(&self, index: usize) -> Option<serde_json::Value> {
        match &self.tags_v1 {
            Some(serde_json::Value::Array(list)) if list.len() == self.messages.len() => {
                list.get(index).cloned()
            }
            _ => None,
        }
    }
}

crate::message! {
    /// A step that made no tool call, which ends the run unless a steer extends it. A steer is a
    /// message a caller sent into the running run; it reaches the conversation at the next step
    /// boundary.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct NaturalEnd {
        /// `Completed` when one of the run's stop conditions was met.
        1 opt stop: RunFinishReason,
        /// Whether a steered message that arrived during the step makes the run take one more
        /// step.
        2 req extendable: bool,
    }
}

state! {
    /// The tool-call budget of the run, as far as it has gone.
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct Budget {
        /// The nudges that fired, by their index in the run's frozen budget.
        1 rep pub nudges_fired: u64 => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Config("the frozen budget's nudge list"),
        },
        2 req pub finalize_fired: bool => { owner: "run loop", reset: "run_start", bound: Bound::Fixed },
        /// The finalize firing asked for one last step, and it hasn't happened yet.
        3 req pub finalize_pending: bool => { owner: "run loop", reset: "run_start", bound: Bound::Fixed },
    }
}

state! {
    /// The latest run.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Run {
        1 req pub run_id: String => { owner: "admission", reset: "run_start", bound: Bound::Fixed },
        2 opt pub finished: RunFinishReason => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        3 opt pub paused: PauseReason => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        /// Seeded from an import or fork: it never calls a provider.
        4 req pub seeded: bool => { owner: "admission", reset: "run_start", bound: Bound::Fixed },
        5 req pub completed_steps: u64 => { owner: "run loop", reset: "run_start", bound: Bound::Fixed },
        6 req pub tool_call_count: u64 => { owner: "run loop", reset: "run_start", bound: Bound::Fixed },
        7 req pub usage: Usage => { owner: "run loop", reset: "run_start", bound: Bound::Fixed },
        8 req pub retries_used: u32 => { owner: "run loop", reset: "run_start", bound: Bound::Fixed },
        9 opt pub last_finish_reason: FinishReason => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        10 opt pub natural_end: NaturalEnd => {
            owner: "run loop",
            reset: "step_start",
            bound: Bound::Fixed,
        },
        11 opt pub open_batch: OpenBatch => {
            owner: "tool router",
            reset: "batch_end",
            bound: Bound::OpenWork("one step's tool calls"),
        },
        /// Calls whose outcome is unknown, from closed batches.
        12 rep pub unknown: ToolCallId => {
            owner: "tool router",
            reset: "run_start",
            bound: Bound::PerRun("one step's tool calls"),
        },
        /// Set when a closed batch was all stop-limit outcomes.
        13 opt pub hard_stopped: RunFinishReason => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        /// Steered or delivered messages waiting for a step boundary, in the order accepted.
        14 rep pub pending_steers: PendingInput => {
            owner: "queue",
            reset: "step_start",
            bound: Bound::OpenWork("messages accepted since the last step boundary"),
        },
        15 opt pub cancel: CancelPoint => {
            owner: "admission",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        16 req pub budget: Budget => { owner: "run loop", reset: "run_start", bound: Bound::Declared },
        /// The whole usage of the latest completed step.
        17 opt pub last_step_usage: Usage => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        /// A step started, so a provider was attempted even if no step finished.
        18 req pub provider_call_started: bool => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        /// Owned by the run loop: why and until when a pause for an account limit waits. Cleared
        /// when the run continues or ends.
        19 opt pub account_limit_pause: serde_json::Value => {
            owner: "run loop",
            reset: "step_start",
            bound: Bound::Fixed,
        },
        /// Owned by admission: the run's keep-warm and restart policies, as frozen.
        20 opt pub keep_warm: serde_json::Value => {
            owner: "admission",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        21 opt pub on_restart: serde_json::Value => {
            owner: "admission",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        /// The run's start as written: its frozen choices and render config, which a resume
        /// continues under.
        22 opt pub start: crate::record::RunStart => {
            owner: "admission",
            reset: "run_start",
            bound: Bound::Config("the run's frozen start: its choices, render config and setup parts"),
        },
        /// The context-ceiling detail the run ended on, which `run.status` reports.
        23 opt pub context_ceiling: serde_json::Value => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        /// The lowered output cap a step of this run recorded, until that step ends: a resume
        /// that sends the step again sends it with this cap.
        24 opt pub output_cap: OutputCap => {
            owner: "run loop",
            reset: "a step's end",
            bound: Bound::Fixed,
        },
        /// The run's strict structured-output schema was downgraded after the provider refused
        /// its grammar: every later request renders it without the strict flag.
        25 req pub structured_downgraded: bool => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        /// The warms the run's own keep-warm sent during the tool wait after its newest step,
        /// which that wait's spending cap counts after a restart. Bounded by the cap: a wait
        /// stops warming before it spends what one cache rewrite costs.
        26 rep pub warms: WarmSpend => {
            owner: "run loop",
            reset: "a step's start",
            bound: Bound::OpenWork("the warms of one tool wait"),
        },
    }
}

crate::message! {
    /// One warm a tool wait sent, with what its price is read from.
    #[derive(Debug, Clone, PartialEq)]
    pub struct WarmSpend {
        1 req step_id: u64,
        2 req reached_cache: bool,
        3 req usage: broca_protocol::usage::Usage,
        /// Owned by billing: the warm's reservation and envelope, when it carries them.
        4 opt billing: serde_json::Value,
        5 opt ttl_ms: u64,
    }
}

crate::message! {
    /// A step's lowered output cap.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct OutputCap {
        1 req step_id: u64,
        2 req max_output_tokens: u32,
    }
}

crate::message! {
    /// A step's measured input: what the provider reported as its full input, the body length of
    /// the attempt that produced it, and the model that measured it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct OutputMeasure {
        1 req input_tokens: u64,
        2 req request_bytes: u64,
        3 req model: broca_protocol::request::ModelHandle,
    }
}

/// What a resume of the latest run must do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// The run is over; the session takes a new run.
    Idle(RunFinishReason),
    /// Calls may have run without a recorded result. The run stops for review and never
    /// sends them again.
    Interrupted(Vec<ToolCallId>),
    /// A provider holds calls and answers later; the run waits for those answers.
    ProviderHeld(Vec<ToolCallId>),
    /// The run paused and resumes as the same run.
    Paused(PauseReason),
    /// The run continues from its last durable boundary.
    Resume,
}

impl Run {
    fn new(start: &crate::record::RunStart) -> Self {
        Self {
            run_id: start.run_id.clone(),
            finished: None,
            paused: None,
            seeded: start.seeded == Some(true),
            completed_steps: 0,
            tool_call_count: 0,
            usage: Usage::default(),
            retries_used: 0,
            last_finish_reason: None,
            natural_end: None,
            open_batch: None,
            unknown: Vec::new(),
            hard_stopped: None,
            pending_steers: Vec::new(),
            cancel: None,
            budget: Budget::default(),
            last_step_usage: None,
            provider_call_started: false,
            account_limit_pause: None,
            context_ceiling: None,
            output_cap: None,
            structured_downgraded: false,
            warms: Vec::new(),
            keep_warm: start.keep_warm.clone(),
            on_restart: start.on_restart.clone(),
            start: Some(start.clone()),
        }
    }

    /// Every call that may have run without a recorded result.
    pub fn indeterminate(&self) -> Vec<ToolCallId> {
        let mut calls = self.unknown.clone();
        if let Some(batch) = &self.open_batch {
            for id in batch.indeterminate() {
                if !calls.contains(id) {
                    calls.push(id.clone());
                }
            }
        }
        calls
    }

    /// What a resume must do. The order matters: the first rule that applies decides.
    /// How many steered or delivered messages wait for the next step boundary.
    pub fn pending_messages(&self) -> usize {
        self.pending_steers
            .iter()
            .map(|input| input.messages.len())
            .sum()
    }

    pub fn resolution(&self) -> Resolution {
        let indeterminate = self.indeterminate();
        let open = self.finished.is_none();
        if let Some(batch) = self.open_batch.as_ref().filter(|_| open) {
            // A held call that a cancel or limit cut off leaves the run with nothing to wait
            // for: it stops.
            let cut_off = batch.calls.iter().any(|call| {
                call.held.is_some()
                    && matches!(
                        call.terminal,
                        Some(CallOutcome::Interrupted | CallOutcome::MaxSteps)
                    )
            });
            if cut_off {
                return Resolution::Interrupted(indeterminate);
            }
            let any_held = batch.calls.iter().any(|call| call.held.is_some());
            let waiting: Vec<ToolCallId> = (0..batch.calls.len())
                .filter(|position| batch.state(*position) == CallState::ProviderHeld)
                .map(|position| batch.calls[position].id.clone())
                .collect();
            let all_ended = batch.calls.iter().all(|call| call.terminal.is_some());
            if any_held
                && indeterminate.is_empty()
                && self.cancel.is_none()
                && (all_ended || !waiting.is_empty())
            {
                return Resolution::ProviderHeld(waiting);
            }
        }
        if let Some(reason) = self
            .paused
            .filter(|reason| *reason != PauseReason::ToolHeld)
        {
            if indeterminate.is_empty() {
                return Resolution::Paused(reason);
            }
        }
        if let Some(reason) = self.finished {
            return Resolution::Idle(reason);
        }
        if !indeterminate.is_empty() {
            return Resolution::Interrupted(indeterminate);
        }
        if let Some(reason) = self.hard_stopped {
            return Resolution::Idle(reason);
        }
        if let Some(end) = self.natural_end {
            if !(end.extendable && self.pending_messages() > 0) {
                return Resolution::Idle(end.stop.unwrap_or(RunFinishReason::Completed));
            }
        }
        if self.seeded && self.completed_steps == 0 {
            return Resolution::Interrupted(Vec::new());
        }
        Resolution::Resume
    }
}

crate::message! {
    /// A compaction view, by its version and checksum.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ViewSlot {
        1 req version: u64,
        2 req checksum: String,
    }
}

crate::message! {
    /// The view a step rendered with.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct AppliedView {
        1 req step_id: u64,
        2 req view: ViewSlot,
    }
}

crate::message! {
    /// One send waiting in the queue.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Pending {
        1 req submission_id: String,
        /// Queued by an interrupting send: it drains ahead of every plain send.
        2 req interrupt: bool,
        /// Queued with a delivery order (its `DeliveryOrdered` record): a running run may
        /// deliver it at a boundary, by the first mode in its order possible there.
        3 req ordered: bool,
    }
}

state! {
    /// The operational state rebuilt so far.
    #[derive(Debug, Clone, Default)]
    pub struct Operations {
        /// Only the latest run: an earlier run's state is replaced when the next one starts.
        1 opt pub run: Run => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Declared,
        },
        /// Owned by the run loop: the refusal-fallback schedule. Session-scoped: each run start
        /// records the schedule it starts with, and steps record its changes.
        2 opt pub refusal_schedule: serde_json::Value => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        /// Sends waiting to run or to be delivered, in drain order: interrupts first, then
        /// plain sends, each in the order they were queued.
        3 rep pub queue: Pending => {
            owner: "queue",
            reset: "never",
            bound: Bound::OpenWork("sends waiting to run"),
        },
        /// The newest compaction view the provider handed over, which applies at the next step
        /// boundary, as its version and checksum. Its edits stay in its record.
        4 opt pub view_accepted: ViewSlot => {
            owner: "compaction",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// The view the latest step rendered with, and that step.
        5 opt pub view_applied: AppliedView => {
            owner: "compaction",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// The change whose intent closed the log, until it aborts. A committed change starts a
        /// new log whose checkpoint has none.
        6 opt pub open_change: crate::codec::Id128 => {
            owner: "writer",
            reset: "change_aborted",
            bound: Bound::Fixed,
        },
        /// After a revert: the ordinal from which the compaction provider must read the
        /// transcript again, owed in the next notice as `transcript_changed`. Two reverts before
        /// a notice owe the lower one.
        7 opt pub transcript_changed: u64 => {
            owner: "compaction",
            reset: "a notice carrying it answered",
            bound: Bound::Fixed,
        },
        /// The session's history version, which every revert moves. The head holds it too; the
        /// fold carries it so a rebuild from records reproduces it.
        8 req pub history_version: u64 => {
            owner: "writer",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// A recovery couldn't rule out that the ledger lost records of actions no surviving
        /// record identifies, so an action may have run with no record of it. Only an operator
        /// clears it.
        9 req pub unattributed_risk: bool => {
            owner: "recovery",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// The setup the latest run started with. A run whose setup equals it starts without a
        /// new `SetupStub`.
        10 opt pub setup: SetupInForce => {
            owner: "admission",
            reset: "run_start",
            bound: Bound::Declared,
        },
        /// The send a `DeliveryOrdered` record just named. Its `QueuedSend` follows in the same
        /// batch and takes the mark.
        11 opt pub ordered_next: String => {
            owner: "queue",
            reset: "queued_send",
            bound: Bound::Fixed,
        },
        /// What compaction estimates the next request's size from: the newest step and the
        /// request body lengths around it. Session-scoped, so a new run's first estimate still
        /// counts from the previous run's last step.
        12 opt pub estimate: Estimate => {
            owner: "compaction",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// The run of the newest request the provider accepted: a step's end, or an attempt
        /// that reached the cache. A compaction ride compares that run's model, tier, account
        /// and surface with the current run's.
        13 opt pub last_send_run: String => {
            owner: "compaction",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// The newest request that reached the provider's cache, by when it was sent: the cache's
        /// lifetime counts from it.
        14 opt pub last_cache_send: CacheSend => {
            owner: "compaction",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// The ledger holds a record of the first compaction protocol (its notices, waits and
        /// applied views stay generic events under their old names; the current contract's are
        /// typed). Only today's run loop renders such a session, so the new run loop refuses it.
        15 req pub first_protocol_compaction: bool => {
            owner: "compaction",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// The newest step whose input the provider measured, which the run loop estimates the
        /// next request's input from when it decides whether a step's output cap must be
        /// lowered. A step without a measurement leaves the older one in place. Session-scoped:
        /// an earlier run's step still measures this run's model.
        16 opt pub output_measure: OutputMeasure => {
            owner: "run loop",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// The body length of the newest attempt no step's end has closed yet, which the next
        /// step's end measures against.
        17 opt pub output_attempt_bytes: u64 => {
            owner: "run loop",
            reset: "run_start",
            bound: Bound::Fixed,
        },
        /// The request digest of the newest attempt that reached the cache. A warm of the
        /// source model's cache counts as reaching the cache only when it refreshed that
        /// request, as today's cache clock reads it.
        18 opt pub last_cache_attempt_digest: crate::codec::Digest => {
            owner: "compaction",
            reset: "never",
            bound: Bound::Fixed,
        },
    }
}

crate::message! {
    /// The facts compaction's request-size estimate reads, as today's loop folds them.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct Estimate {
        /// The current run's model.
        1 opt run_model: broca_protocol::request::ModelHandle,
        /// The newest step's end: its run's model, its usage and its fallback (owned by the run
        /// loop: the model that served it and any server-side iterations).
        2 opt step_model: broca_protocol::request::ModelHandle,
        3 opt step_usage: broca_protocol::usage::Usage,
        4 opt step_fallback: serde_json::Value,
        /// Whether a step has ended in this session at all.
        5 req stepped: bool,
        /// The newest attempt's request body length, and the one before the newest step's end.
        6 opt current_request_bytes: u64,
        7 opt previous_request_bytes: u64,
    }
}

crate::message! {
    /// When a request that reached the provider's cache was sent, and its run.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct CacheSend {
        1 req sent_at_ms: u64,
        2 req run_id: String,
        /// The cache lifetime that request asked for, when it asked for one the provider
        /// honours; only then can the cache be proven expired.
        3 opt requested_ttl_ms: u64,
    }
}

state! {
    /// A run's setup as a run start names it: its parts, sorted, and its model.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct SetupInForce {
        1 rep pub parts: crate::record::PartRef => {
            owner: "admission",
            reset: "run_start",
            bound: Bound::Config("the run's setup parts: its tool lists, formats, system text and plan answers"),
        },
        2 opt pub model: broca_protocol::request::ModelHandle => {
            owner: "admission",
            reset: "run_start",
            bound: Bound::Fixed,
        },
    }
}

impl Operations {
    /// A queued send joins the queue; a run that consumes it, a retraction, an ended send or a
    /// delivery into a running run removes it.
    fn apply_queue(&mut self, record: &Record) {
        let removed = match record {
            Record::DeliveryOrdered(ordered) => {
                self.ordered_next = Some(ordered.submission_id.clone());
                return;
            }
            Record::QueuedSend(send) => {
                let ordered =
                    self.ordered_next.take().as_deref() == Some(send.submission_id.as_str());
                let pending = Pending {
                    submission_id: send.submission_id.clone(),
                    interrupt: send.delivery.as_deref() == Some("interrupt"),
                    ordered,
                };
                let at = if pending.interrupt {
                    self.queue
                        .iter()
                        .position(|queued| !queued.interrupt)
                        .unwrap_or(self.queue.len())
                } else {
                    self.queue.len()
                };
                self.queue.insert(at, pending);
                return;
            }
            Record::RunStart(start) => start.submission_id.as_deref(),
            Record::Retraction(retraction) => Some(retraction.submission_id.as_str()),
            Record::SendEnded(ended) => Some(ended.submission_id.as_str()),
            Record::Delivery(delivery) => Some(delivery.submission_id.as_str()),
            _ => None,
        };
        if let Some(id) = removed {
            self.queue.retain(|pending| pending.submission_id != id);
        }
    }

    /// Whether a run starting with `parts` and `model` changes the setup in force, and so
    /// starts with a `SetupStub`.
    pub fn setup_changed(
        &self,
        parts: &[crate::record::PartRef],
        model: &broca_protocol::request::ModelHandle,
    ) -> bool {
        let mut sorted = parts.to_vec();
        sorted.sort();
        match &self.setup {
            Some(setup) => setup.parts != sorted || setup.model.as_ref() != Some(model),
            None => true,
        }
    }

    pub fn apply(&mut self, record: &Record) -> Result<(), FoldError> {
        self.apply_at(None, record)
    }

    /// The session's latest sends: a run's start names the current model, an attempt its
    /// request's size and whether it reached the cache, and a step's end the newest step.
    fn apply_sends(&mut self, record: &Record) {
        match record {
            Record::RunStart(start) => {
                self.estimate
                    .get_or_insert_with(Estimate::default)
                    .run_model = Some(start.model.clone());
                self.output_attempt_bytes = None;
            }
            Record::Attempt(attempt) => {
                self.output_attempt_bytes = Some(attempt.request_body_len);
                self.estimate
                    .get_or_insert_with(Estimate::default)
                    .current_request_bytes = Some(attempt.request_body_len);
                if attempt.reached_cache == Some(true) {
                    self.last_cache_attempt_digest = Some(attempt.request_body_sha256);
                    self.last_send_run = Some(attempt.run_id.clone());
                    if let Some(sent_at_ms) = attempt.sent_at_ms {
                        if self
                            .last_cache_send
                            .as_ref()
                            .is_none_or(|latest| sent_at_ms >= latest.sent_at_ms)
                        {
                            self.last_cache_send = Some(CacheSend {
                                sent_at_ms,
                                run_id: attempt.run_id.clone(),
                                requested_ttl_ms: attempt.requested_cache_ttl_ms,
                            });
                        }
                    }
                }
            }
            Record::Warmed(warmed) => {
                // A warm that reached the cache restarts its lifetime, as today's cache clock
                // dates it; a warm of the source model's cache only if it refreshed the newest
                // request that reached the cache.
                let refreshed_newest = warmed.trigger.as_deref() != Some("source_keep_warm")
                    || self
                        .last_cache_attempt_digest
                        .is_none_or(|digest| digest == warmed.request_body_sha256);
                if warmed.reached_cache == Some(true)
                    && refreshed_newest
                    && self
                        .last_cache_send
                        .as_ref()
                        .is_none_or(|latest| warmed.sent_at_ms >= latest.sent_at_ms)
                {
                    self.last_cache_send = Some(CacheSend {
                        sent_at_ms: warmed.sent_at_ms,
                        run_id: warmed.run_id.clone(),
                        requested_ttl_ms: warmed.requested_cache_ttl_ms,
                    });
                }
            }
            Record::StepEnd(step) => {
                self.measure_step(step);
                let estimate = self.estimate.get_or_insert_with(Estimate::default);
                estimate.step_model = estimate.run_model.clone();
                estimate.step_usage = step.usage;
                estimate.step_fallback = step.fallback.clone();
                estimate.stepped = true;
                estimate.previous_request_bytes = estimate.current_request_bytes;
                self.last_send_run = Some(step.run_id.clone());
            }
            _ => {}
        }
    }

    /// A step's end measures its input when the provider reported it, an attempt's body length
    /// is known and the model that served the measured call is: the model the step was routed
    /// to, else the run's, with a server-side handoff measured on the last model that served it.
    /// Today's `OutputReservationHistory::note_served_step` decides the same.
    fn measure_step(&mut self, step: &crate::record::StepEnd) {
        let bytes = self.output_attempt_bytes.take();
        let fallback = step.fallback.as_ref();
        let routed: Option<broca_protocol::request::ModelHandle> = fallback
            .and_then(|fallback| fallback.get("route"))
            .and_then(|route| route.get("model"))
            .and_then(|model| serde_json::from_value(model.clone()).ok());
        let Some(requested) = routed.or_else(|| {
            self.estimate
                .as_ref()
                .and_then(|estimate| estimate.run_model.clone())
        }) else {
            return;
        };
        let served = fallback
            .and_then(|fallback| fallback.get("served_model"))
            .and_then(|model| model.as_str());
        let iterations: Option<Vec<broca_protocol::usage::UsageIteration>> = fallback
            .and_then(|fallback| fallback.get("usage_iterations"))
            .and_then(|iterations| serde_json::from_value(iterations.clone()).ok());
        let measured = broca_protocol::usage::model_usage(
            served.unwrap_or(&requested.model_id),
            step.usage.unwrap_or_default(),
            iterations.as_deref(),
        )
        .last()
        .map(|(model, usage)| {
            (
                *usage,
                broca_protocol::request::ModelHandle::new(&requested.provider_module_id, *model),
            )
        });
        if let (Some((usage, model)), Some(request_bytes)) = (measured, bytes) {
            if let Some(input_tokens) = usage.fill_tokens() {
                self.output_measure = Some(OutputMeasure {
                    input_tokens,
                    request_bytes,
                    model,
                });
            }
        }
    }

    /// Folds `record`, which has `seq` in its log. A pending input remembers the seq of the
    /// record that accepted it, so the copy a boundary places can name it.
    pub fn apply_at(&mut self, seq: Option<u64>, record: &Record) -> Result<(), FoldError> {
        if record.stream() != Stream::Ledger {
            return Err(FoldError::TranscriptRecord(record.name()));
        }
        self.apply_queue(record);
        match (record, self.open_change) {
            (Record::ChangeAborted(aborted), Some(open)) if aborted.change == open => {
                self.open_change = None;
                return Ok(());
            }
            (_, Some(_)) => return Err(FoldError::LogClosed(record.name())),
            (Record::ChangeIntent(intent), None) => {
                self.open_change = Some(intent.change);
                return Ok(());
            }
            (Record::ChangeAborted(_), None) => return Err(FoldError::LogClosed(record.name())),
            _ => {}
        }
        match record {
            Record::View(view) => {
                self.view_accepted = Some(ViewSlot {
                    version: view.version,
                    checksum: view.checksum.clone(),
                });
            }
            Record::RecoveryCut(cut) => {
                if let Some(ordinal) = cut.ordinal {
                    self.transcript_changed = Some(
                        self.transcript_changed
                            .map_or(ordinal, |owed| owed.min(ordinal)),
                    );
                }
                if let Some(version) = cut.history_version {
                    self.history_version = version;
                }
                self.unattributed_risk |= cut.unattributed_risk;
            }
            Record::TranscriptReverted(reverted) => {
                self.transcript_changed = Some(
                    self.transcript_changed
                        .map_or(reverted.ordinal, |owed| owed.min(reverted.ordinal)),
                );
                self.history_version = reverted.history_version;
            }
            Record::ViewApplied(applied) => {
                self.view_applied = Some(AppliedView {
                    step_id: applied.step_id,
                    view: ViewSlot {
                        version: applied.version,
                        checksum: applied.checksum.clone(),
                    },
                });
            }
            _ => {}
        }
        self.apply_sends(record);
        if let Record::RunStart(start) = record {
            self.run = Some(Run::new(start));
            self.refusal_schedule = start.refusal_schedule.clone();
            let mut parts = start.setup.clone();
            parts.sort();
            self.setup = Some(SetupInForce {
                parts,
                model: Some(start.model.clone()),
            });
            return Ok(());
        }
        if let Record::StepEnd(step) = record {
            if let Some(next) = step
                .fallback
                .as_ref()
                .and_then(|fallback| fallback.get("refusal_fallback"))
                .filter(|next| !next.is_null())
            {
                self.refusal_schedule = Some(next.clone());
            }
        }
        if let Record::Event(event) = record {
            if NOT_FOLDED.contains(&event.name.as_str()) {
                return Err(FoldError::NotFolded(event.name.clone()));
            }
            self.first_protocol_compaction |= event.name.starts_with("compaction_");
        }
        let Some(run) = self.run.as_mut() else {
            return Ok(());
        };
        match record {
            Record::StepStart(_) => {
                run.warms.clear();
                run.provider_call_started = true;
                run.account_limit_pause = None;
                run.paused = None;
                run.natural_end = None;
                run.pending_steers.clear();
            }
            Record::StepEnd(step) => {
                run.output_cap = None;
                let usage = step.total_usage.or(step.usage).unwrap_or_default();
                run.usage.accumulate(&usage);
                run.last_step_usage = Some(usage);
                run.retries_used = run.retries_used.saturating_add(step.retries_used);
                run.last_finish_reason = step.finish_reason;
                run.completed_steps += 1;
                run.tool_call_count += u64::from(step.call_count);
                run.budget.finalize_pending = false;
                if step.budget_finalize == Some(true) {
                    run.open_batch = None;
                    run.natural_end = Some(NaturalEnd {
                        stop: None,
                        extendable: false,
                    });
                } else if step.call_count > 0 {
                    if run
                        .open_batch
                        .as_ref()
                        .is_some_and(OpenBatch::holds_call_evidence)
                    {
                        return Err(FoldError::OverlappingBatch);
                    }
                    run.open_batch = None;
                    run.natural_end = None;
                } else {
                    run.natural_end = Some(NaturalEnd {
                        stop: (step.stop_met == Some(true)).then_some(RunFinishReason::Completed),
                        extendable: step.limiting_stop_met != Some(true),
                    });
                }
            }
            Record::BatchStart(batch) => {
                if run
                    .open_batch
                    .as_ref()
                    .is_some_and(OpenBatch::holds_call_evidence)
                {
                    return Err(FoldError::OverlappingBatch);
                }
                run.open_batch = Some(OpenBatch {
                    batch_id: batch.batch_id.clone(),
                    step_id: batch.step_id,
                    calls: batch
                        .planned
                        .iter()
                        .map(|id| Call {
                            id: id.clone(),
                            intent: false,
                            held: None,
                            key: None,
                            held_reply: None,
                            args: None,
                            terminal: None,
                        })
                        .collect(),
                });
            }
            Record::ToolIntent(intent) => {
                if let Some(batch) = run.open_batch.as_mut() {
                    for call in batch
                        .calls
                        .iter_mut()
                        .filter(|c| c.id == intent.tool_call_id)
                    {
                        call.intent = true;
                        call.key = intent.call_key.clone();
                        call.args = intent.args.clone();
                    }
                }
            }
            Record::ToolHeld(held) => {
                if let Some(batch) = run.open_batch.as_mut() {
                    for call in batch.calls.iter_mut().filter(|c| c.id == held.tool_call_id) {
                        call.held = Some(held.sequential);
                        call.held_reply = Some(held.clone());
                    }
                }
            }
            Record::ToolOutcome(outcome) => {
                if let Some(call) = run
                    .open_batch
                    .as_mut()
                    .and_then(|batch| batch.first_open(&outcome.tool_call_id))
                {
                    call.terminal = Some(outcome.outcome);
                }
                if outcome.outcome == CallOutcome::Unknown {
                    run.unknown.push(outcome.tool_call_id.clone());
                }
            }
            Record::BatchEnd(_) => {
                let closed = run
                    .open_batch
                    .as_ref()
                    .is_some_and(|batch| batch.calls.iter().all(|call| call.terminal.is_some()));
                if closed {
                    if let Some(reason) = run.open_batch.as_ref().and_then(OpenBatch::hard_stop) {
                        run.hard_stopped = Some(reason);
                    }
                    run.open_batch = None;
                }
            }
            // Today's replay keeps the latest decision of the run still going.
            Record::CeilingDecided(decided) if run.finished.is_none() => {
                run.context_ceiling = decided.detail.clone();
            }
            Record::StructuredDowngraded(downgraded) if downgraded.run_id == run.run_id => {
                run.structured_downgraded = true;
            }
            Record::Warmed(warmed)
                if warmed.trigger.as_deref() == Some("keep_warm")
                    && warmed.run_id == run.run_id =>
            {
                run.warms.push(WarmSpend {
                    step_id: warmed.step_id,
                    reached_cache: warmed.reached_cache == Some(true),
                    usage: warmed.usage,
                    billing: warmed.billing.clone(),
                    ttl_ms: warmed.ttl_ms,
                });
            }
            Record::OutputCapLowered(lowered) => {
                run.output_cap = Some(OutputCap {
                    step_id: lowered.step_id,
                    max_output_tokens: lowered.max_output_tokens,
                });
            }
            Record::CancelRequest(cancel) => {
                if cancel.run_id == run.run_id
                    && run.finished.is_none()
                    && run.cancel != Some(CancelPoint::Now)
                {
                    run.cancel = Some(cancel.at);
                }
            }
            Record::Steer(steer) => run.pending_steers.push(PendingInput {
                seq,
                placement: crate::record::Placement::Steer,
                messages: steer.input.clone(),
                written_at_ms: steer.written_at_ms,
                tags_v1: steer.tags_v1.clone(),
                submission_id: None,
            }),
            Record::Delivery(delivery) if !delivery.input.is_empty() => {
                run.pending_steers.push(PendingInput {
                    seq,
                    placement: if delivery.mode == "turn_system" {
                        crate::record::Placement::DeliveredTurnSystem
                    } else {
                        crate::record::Placement::Delivered
                    },
                    messages: delivery.input.clone(),
                    written_at_ms: delivery.written_at_ms,
                    tags_v1: delivery.tags_v1.clone(),
                    submission_id: Some(delivery.submission_id.clone()),
                })
            }
            Record::BudgetFire(fire) => match fire.firing {
                BudgetFiring::Nudge => {
                    if let Some(index) = fire.nudge_index {
                        run.budget.nudges_fired.push(index);
                    }
                }
                BudgetFiring::Finalize => {
                    run.budget.finalize_fired = true;
                    run.budget.finalize_pending = true;
                }
            },
            Record::RunEnd(end) => {
                run.account_limit_pause = None;
                run.finished = Some(end.reason);
                run.retries_used = end.retries_used;
                run.pending_steers.clear();
                run.paused = None;
            }
            Record::Pause(pause) => {
                run.account_limit_pause = pause.account_limit.clone();
                run.paused = Some(pause.reason);
                run.finished = None;
            }
            Record::Retraction(retraction) if retraction.failure.is_some() => {
                // A send that failed admission ends as a run of its own with no step, so the
                // latest step's finish reason no longer describes the latest run.
                run.last_finish_reason = None;
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
