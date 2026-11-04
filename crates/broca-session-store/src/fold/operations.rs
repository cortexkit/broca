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

/// One planned call of an open tool batch, with what the ledger recorded about it.
#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    pub id: ToolCallId,
    /// A dispatch intent was recorded: the call may have left.
    pub intent: bool,
    /// A provider took the call to answer later; `Some(true)` when later calls wait for it.
    pub held: Option<bool>,
    pub terminal: Option<CallOutcome>,
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

/// A tool batch whose calls don't all have outcomes yet.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenBatch {
    pub batch_id: String,
    pub step_id: u64,
    pub calls: Vec<Call>,
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

/// A step that made no tool call, which ends the run unless a steer extends it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NaturalEnd {
    /// `Completed` when one of the run's stop conditions was met.
    pub stop: Option<RunFinishReason>,
    /// Whether a steer that arrived during the step makes the run take one more step.
    pub extendable: bool,
}

/// The tool-call budget of the run, as far as it has gone.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Budget {
    /// The nudges that fired, by their index in the run's frozen budget.
    pub nudges_fired: Vec<u64>,
    pub finalize_fired: bool,
    /// The finalize firing asked for one last step, and it hasn't happened yet.
    pub finalize_pending: bool,
}

/// The latest run.
#[derive(Debug, Clone, PartialEq)]
pub struct Run {
    pub run_id: String,
    pub finished: Option<RunFinishReason>,
    pub paused: Option<PauseReason>,
    /// Seeded from an import or fork: it never calls a provider.
    pub seeded: bool,
    pub completed_steps: u64,
    pub tool_call_count: u64,
    pub usage: Usage,
    pub retries_used: u32,
    pub last_finish_reason: Option<FinishReason>,
    pub natural_end: Option<NaturalEnd>,
    pub open_batch: Option<OpenBatch>,
    /// Calls whose outcome is unknown, from closed batches.
    pub unknown: Vec<ToolCallId>,
    /// Set when a closed batch was all stop-limit outcomes.
    pub hard_stopped: Option<RunFinishReason>,
    /// Steered or delivered messages waiting for a step boundary.
    pub pending_steers: usize,
    pub cancel: Option<CancelPoint>,
    pub budget: Budget,
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
    fn new(run_id: String, seeded: bool) -> Self {
        Self {
            run_id,
            finished: None,
            paused: None,
            seeded,
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

/// The operational state rebuilt so far.
#[derive(Debug, Clone, Default)]
pub struct Operations {
    pub run: Option<Run>,
}

impl Operations {
    pub fn apply(&mut self, record: &Record) -> Result<(), FoldError> {
        if record.stream() != Stream::Ledger {
            return Err(FoldError::TranscriptRecord(record.name()));
        }
        if let Record::RunStart(start) = record {
            self.run = Some(Run::new(start.run_id.clone(), start.seeded == Some(true)));
            return Ok(());
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
                run.paused = None;
                run.natural_end = None;
                run.pending_steers = 0;
            }
            Record::StepEnd(step) => {
                run.usage.accumulate(
                    step.total_usage
                        .as_ref()
                        .or(step.usage.as_ref())
                        .unwrap_or(&Usage::default()),
                );
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
                run.finished = Some(end.reason);
                run.retries_used = end.retries_used;
                run.pending_steers = 0;
                run.paused = None;
            }
            Record::Pause(pause) => {
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
