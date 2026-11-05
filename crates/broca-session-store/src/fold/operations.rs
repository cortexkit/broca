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
        /// Steered or delivered messages waiting for a step boundary.
        14 req pub pending_steers: usize => {
            owner: "queue",
            reset: "step_start",
            bound: Bound::Fixed,
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
            pending_steers: 0,
            cancel: None,
            budget: Budget::default(),
            last_step_usage: None,
            provider_call_started: false,
            account_limit_pause: None,
            keep_warm: start.keep_warm.clone(),
            on_restart: start.on_restart.clone(),
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
            if !(end.extendable && self.pending_steers > 0) {
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
    }
}

impl Operations {
    /// A queued send joins the queue; a run that consumes it, a retraction, an ended send or a
    /// delivery into a running run removes it.
    fn apply_queue(&mut self, record: &Record) {
        let removed = match record {
            Record::QueuedSend(send) => {
                let pending = Pending {
                    submission_id: send.submission_id.clone(),
                    interrupt: send.delivery.as_deref() == Some("interrupt"),
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

    pub fn apply(&mut self, record: &Record) -> Result<(), FoldError> {
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
        if let Record::RunStart(start) = record {
            self.run = Some(Run::new(start));
            self.refusal_schedule = start.refusal_schedule.clone();
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
        }
        let Some(run) = self.run.as_mut() else {
            return Ok(());
        };
        match record {
            Record::StepStart(_) => {
                run.provider_call_started = true;
                run.account_limit_pause = None;
                run.paused = None;
                run.natural_end = None;
                run.pending_steers = 0;
            }
            Record::StepEnd(step) => {
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
                    }
                }
            }
            Record::ToolHeld(held) => {
                if let Some(batch) = run.open_batch.as_mut() {
                    for call in batch.calls.iter_mut().filter(|c| c.id == held.tool_call_id) {
                        call.held = Some(held.sequential);
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
            Record::CancelRequest(cancel) => {
                if cancel.run_id == run.run_id
                    && run.finished.is_none()
                    && run.cancel != Some(CancelPoint::Now)
                {
                    run.cancel = Some(cancel.at);
                }
            }
            Record::Steer(steer) => run.pending_steers += steer.input.len(),
            Record::Delivery(delivery) => run.pending_steers += delivery.input.len(),
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
                run.pending_steers = 0;
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
