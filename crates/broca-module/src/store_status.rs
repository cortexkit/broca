//! Run and send facts of a session kept in the new storage: `run.status` (for one run, and the
//! queued send a run id may still name), `run.result`, `send.status` and `session.send_status`.
//! The no-run `status` stays on the registry's own path (`route_status`), which asks the
//! session's store actor, opening it when the session needs recovering.
//!
//! A reply equals today's for the same history. It is built from the session read-only (see
//! `store_reads`, which serves it from the session's actor or from a kept view): the operational
//! fold for the latest run, and for any run the run's own
//! records, found through the id index and read from its start to its end. Today's reconciliation
//! (`broca_core::reconcile_run_state`) walks today's records; the walk here is the same walk over
//! the store's records, and every rule it applies to values (the step's normalized finish, the
//! structured-output result of an ended run, the run's usage arithmetic, the indeterminate-call
//! tracking) is the one function both walks call.
//!
//! Differences from today's reply, by design:
//! - an active run's `step` (the in-flight model step) is served only when the session's store
//!   actor answers the read: it is a live hint the actor keeps in memory, as today's actor does,
//!   and nothing durable holds it;
//! - `awaiting_approval` is always absent: the new storage takes no approval questions;
//! - a final message's `mid` is the id the session stored when it placed the message;
//! - a final message's text is cleaned of tag notation whenever the message's entry facts say its
//!   run cleans replies, as `session.entries` serves it. Today's `run.result` cleans by the run's
//!   own tag rules only, and so serves the notation on a run that the compaction view showed tag
//!   numbers while its own `render_tags` was off.
//!
//! A run that the fold shows unfinished on a session no store actor holds was cut off by a crash,
//! and the session's next open records its end (`store_actor::recovery_mode`). A read serves
//! that end only when the fold alone decides it, so a terminal state served here never changes
//! later; any other such run is refused by name, `run_end_unrecorded`, until the open records it.
//!
//! Facts this storage can't serve yet are refused by name, permanently, never guessed: a run
//! waiting on calls a provider holds (`held_calls_unserved`), and a completed run whose final
//! message a revert has since removed (`final_message_reverted`). `run.tool_calls` isn't served
//! here.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use broca_core::{ReconciledRunState, RunIndexState};
use broca_protocol::{RunFinishReason, RunId, ToolCallId};
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::fold::operations::{CallState, Resolution, Run};
use broca_session_store::idindex::IdKind;
use broca_session_store::record::{CallOutcome, Record, RunStart};
use broca_session_store::session::{Session, SessionError};
use serde_json::Value;

pub mod todays;

/// Why a run or send fact of a session kept in the new storage wasn't served.
#[derive(Debug)]
pub enum StatusError {
    /// The session couldn't be read.
    Session(SessionError),
    /// The session's records contradict each other, or name a value no build writes.
    Damaged(String),
    /// Today's reply carries a fact this storage can't serve yet. Permanent: `code` names it.
    Unserved { code: &'static str, message: String },
    /// The run's end isn't recorded, and only the session's next open decides it. Transient.
    EndUnrecorded { run_id: String },
}

impl From<SessionError> for StatusError {
    fn from(error: SessionError) -> Self {
        StatusError::Session(error)
    }
}

impl From<StatusError> for crate::serve::ServeError {
    fn from(error: StatusError) -> Self {
        use crate::serve::ServeError;
        use broca_protocol::ErrorClass;
        match error {
            StatusError::Session(error) => {
                let class = match error {
                    SessionError::Backend(_) => ErrorClass::Transient,
                    _ => ErrorClass::Permanent,
                };
                ServeError::Open(
                    format!("the session could not be read from the new storage: {error:?}"),
                    Some(class),
                )
            }
            StatusError::Damaged(message) => ServeError::Open(
                format!("the session's stored run facts can't be read: {message}"),
                Some(ErrorClass::Permanent),
            ),
            StatusError::Unserved { code, message } => {
                ServeError::Open(format!("{code}: {message}"), Some(ErrorClass::Permanent))
            }
            StatusError::EndUnrecorded { run_id } => ServeError::Open(
                format!(
                    "run_end_unrecorded: the run {run_id} was cut off before its end was \
                     recorded; the session's next open records it"
                ),
                Some(ErrorClass::Transient),
            ),
        }
    }
}

fn damaged(what: impl Into<String>) -> StatusError {
    StatusError::Damaged(what.into())
}

fn unserved(code: &'static str, message: impl Into<String>) -> StatusError {
    StatusError::Unserved {
        code,
        message: message.into(),
    }
}

/// Decodes a value a record keeps as JSON, owned by another component, into its typed form.
fn decode<T: serde::de::DeserializeOwned>(value: &Value, what: &str) -> Result<T, StatusError> {
    serde_json::from_value(value.clone()).map_err(|error| damaged(format!("{what}: {error}")))
}

/// What the store says about one run, as today's reconciliation reports it.
#[derive(Debug, Clone)]
pub struct RunFacts {
    pub state: ReconciledRunState,
    /// Why an interrupted run was interrupted, as its end recorded it.
    pub interrupt_reason: Option<String>,
    /// Whether the run's start asked for the trailing `wake` directive of its final message.
    pub wake_directive: bool,
    /// For a completed run: its final message, as `run.result` serves it, or why it can't be read.
    pub final_message: Option<Result<broca_wire::FinalMessage, FinalError>>,
    /// The unfinished run waits on calls a provider holds. Today's status lists them
    /// (`held_calls`), which this storage doesn't serve yet.
    pub holds_calls: bool,
}

impl RunFacts {
    /// Refuses a status that would have to list calls a provider holds.
    fn check_held(&self) -> Result<(), StatusError> {
        if self.holds_calls {
            return Err(unserved(
                "held_calls_unserved",
                "the run waits on tool calls a provider holds, which this storage doesn't serve yet",
            ));
        }
        Ok(())
    }

    /// Whether the run has ended, so its facts never change.
    fn settled(&self) -> bool {
        !matches!(
            self.state.state,
            RunIndexState::Active | RunIndexState::Paused
        )
    }
}

/// The facts of runs that have ended, kept so a polled `run.status` of a finished run doesn't
/// read its records again. A run that has ended never changes, so an entry never goes stale,
/// except through a revert, which can take a finished run's final message out of the transcript:
/// a revert moves the session's history version, which is part of the key, as is the session's
/// incarnation, which a session deleted and created again under the same id doesn't share.
/// Bounded: the oldest entry makes room for a new one.
pub struct SettledRuns {
    inner: Mutex<SettledInner>,
    capacity: usize,
}

#[derive(Default)]
struct SettledInner {
    facts: HashMap<SettledKey, RunFacts>,
    order: VecDeque<SettledKey>,
}

/// The session's address, its incarnation, its history version, and the run id.
type SettledKey = (String, [u8; 16], u64, String);

impl SettledRuns {
    /// How many finished runs a module keeps the facts of.
    pub const CAPACITY: usize = 4096;

    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(SettledInner::default()),
            capacity: capacity.max(1),
        }
    }

    fn get(&self, key: &SettledKey) -> Option<RunFacts> {
        self.inner.lock().ok()?.facts.get(key).cloned()
    }

    fn put(&self, key: SettledKey, facts: RunFacts) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        if inner.facts.insert(key.clone(), facts).is_none() {
            inner.order.push_back(key);
        }
        while inner.order.len() > self.capacity {
            if let Some(oldest) = inner.order.pop_front() {
                inner.facts.remove(&oldest);
            }
        }
    }

    /// How many runs' facts are kept.
    pub fn len(&self) -> usize {
        self.inner.lock().map_or(0, |inner| inner.facts.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for SettledRuns {
    fn default() -> Self {
        Self::new(Self::CAPACITY)
    }
}

/// A run state with nothing known yet but `state`, as today's reconciliation starts one.
fn fresh_state(state: RunIndexState) -> ReconciledRunState {
    ReconciledRunState {
        finish: None,
        account_limit_pause: None,
        budget_detail: None,
        context_ceiling: None,
        ceiling_deadline_ms: None,
        ride_wait_deadline_ms: None,
        structured_output: None,
        live_step: None,
        held_calls: Vec::new(),
        state,
        undelivered_steers: None,
        final_step_finish_reason: None,
        indeterminate_tool_calls: None,
        usage: None,
        run_usage: Default::default(),
        retries_used: 0,
        error: None,
        pause_reason: None,
        auth_selection: None,
        title: None,
    }
}

/// The run-index state a run's end reason maps to.
fn ended_state(reason: RunFinishReason) -> RunIndexState {
    match reason {
        RunFinishReason::Completed => RunIndexState::Completed,
        RunFinishReason::MaxSteps => RunIndexState::MaxSteps,
        RunFinishReason::BudgetExhausted => RunIndexState::BudgetExhausted,
        RunFinishReason::ContextCeiling => RunIndexState::ContextCeiling,
        RunFinishReason::Cancelled => RunIndexState::Cancelled,
        RunFinishReason::Interrupted => RunIndexState::Interrupted,
        RunFinishReason::Error => RunIndexState::Error,
        RunFinishReason::TransformUnavailable => RunIndexState::TransformUnavailable,
    }
}

/// How a run ended, as its end record states it, or as the session's next open will.
struct End {
    reason: RunFinishReason,
    usage: broca_protocol::Usage,
    retries_used: u32,
    interrupt_reason: Option<String>,
    error: Option<broca_protocol::ProviderError>,
}

/// One run's own records walked in seq order, as `broca_core::reconcile_run_state` walks today's
/// records for it. Records of other runs inside the range are walked too, by the same rules, so a
/// run id that names several starts reads as today's does.
struct Walk<'a> {
    run_id: &'a str,
    parts: &'a dyn Parts,
    /// The usage of the run's completed steps, over every start bearing its id.
    usage: broca_protocol::usage::RunUsage,
    steps: broca_core::replay::RunUsageSteps,
    /// Whether the records walked now belong to one of this run's starts.
    usage_active: bool,
    active_run: Option<String>,
    active_zero_step_seed: bool,
    has_completed_step: bool,
    total_usage: broca_protocol::Usage,
    total_retries_used: u32,
    last_step_finish_reason: Option<broca_protocol::content::FinishReason>,
    indeterminate: broca_core::projection::IndeterminateToolCalls,
    pending_steer_ids: Vec<String>,
    found: Option<ReconciledRunState>,
    auth_selection: Option<broca_wal::AuthSelection>,
    title: Option<String>,
    budget_detail: Option<broca_protocol::BudgetExhaustedDetail>,
    context_ceiling: Option<broca_protocol::ContextCeilingDetail>,
    frozen_structured_output: Option<broca_protocol::structured_output::FrozenStructuredOutput>,
    frozen_finish_config: Value,
    structured_output_result: Option<broca_protocol::structured_output::StructuredOutputResult>,
    interrupt_reason: Option<String>,
    wake_directive: bool,
    /// Whether the run's frozen tag rules clean the model's text of tag notation on every
    /// exposure (`broca_core::tags::computed_rules`).
    cleans_text: bool,
    /// Calls of this run a provider holds with no outcome recorded yet.
    held: Vec<ToolCallId>,
    /// The call a tool reply just before a recovery event answered.
    last_reply: Option<ToolCallId>,
    /// The messages this run placed, by ordinal: each one's id.
    placed: std::collections::BTreeMap<u64, String>,
}

impl<'a> Walk<'a> {
    fn new(run_id: &'a str, parts: &'a dyn Parts) -> Self {
        Self {
            run_id,
            parts,
            usage: Default::default(),
            steps: Default::default(),
            usage_active: false,
            active_run: None,
            active_zero_step_seed: false,
            has_completed_step: false,
            total_usage: Default::default(),
            total_retries_used: 0,
            last_step_finish_reason: None,
            indeterminate: Default::default(),
            pending_steer_ids: Vec::new(),
            found: None,
            auth_selection: None,
            title: None,
            budget_detail: None,
            context_ceiling: None,
            frozen_structured_output: None,
            frozen_finish_config: Value::Null,
            structured_output_result: None,
            interrupt_reason: None,
            wake_directive: false,
            cleans_text: false,
            held: Vec::new(),
            last_reply: None,
            placed: Default::default(),
        }
    }

    /// Whether the records walked now are this run's.
    fn ours(&self) -> bool {
        self.active_run.as_deref() == Some(self.run_id)
    }

    fn record(&mut self, record: &Record) -> Result<(), StatusError> {
        let ours = self.ours();
        match record {
            Record::RunStart(start) => self.run_start(start)?,
            Record::Event(event) => self.event(event)?,
            Record::CeilingDecided(decided) if ours => {
                let detail: Option<broca_protocol::ContextCeilingDetail> = decided
                    .detail
                    .as_ref()
                    .map(|detail| decode(detail, "a context-ceiling decision"))
                    .transpose()?;
                self.context_ceiling.clone_from(&detail);
                if let Some(state) = &mut self.found {
                    state.ceiling_deadline_ms = None;
                    state.context_ceiling = detail;
                }
            }
            Record::StructuredDowngraded(downgraded) if downgraded.run_id == self.run_id => {
                if let Some(frozen) = &mut self.frozen_structured_output {
                    frozen.downgrade_grammar();
                }
            }
            Record::StructuredResult(result) if result.run_id == self.run_id => {
                self.structured_output_result =
                    Some(decode(&result.result, "a structured-output result")?);
            }
            Record::StepEnd(end) => {
                self.active_zero_step_seed = false;
                self.has_completed_step = true;
                self.total_usage
                    .accumulate(&end.total_usage.or(end.usage).unwrap_or_default());
                self.total_retries_used = self.total_retries_used.saturating_add(end.retries_used);
                self.last_step_finish_reason = end.finish_reason;
                if ours {
                    let fallback = end.fallback.as_ref();
                    let refusal_fallback = fallback
                        .and_then(|fallback| fallback.get("refusal_fallback"))
                        .is_some_and(|value| !value.is_null());
                    let raw = fallback
                        .and_then(|fallback| fallback.get("raw_finish_reason"))
                        .and_then(Value::as_str);
                    if let Some(state) = &mut self.found {
                        state.finish = broca_core::replay::step_finish(
                            &self.frozen_finish_config,
                            refusal_fallback,
                            raw,
                            end.call_count > 0,
                        );
                        state.ride_wait_deadline_ms = None;
                    }
                }
                if self.usage_active {
                    let shares = shares(&end.by_model);
                    self.steps.step_ended(
                        &mut self.usage,
                        Some(broca_core::replay::StepServed {
                            step_id: broca_protocol::StepId(end.step_id),
                            service_tier_used: end.service_tier_used.as_deref(),
                            speed_used: end.speed_used.as_deref(),
                        }),
                        Some(&shares),
                    );
                }
            }
            Record::StepRefused(refused) => {
                let iterations: Option<Vec<broca_protocol::usage::UsageIteration>> = refused
                    .usage_iterations
                    .as_ref()
                    .map(|value| decode(value, "a refused step's usage iterations"))
                    .transpose()?;
                self.total_usage
                    .accumulate(&broca_protocol::usage::step_usage(
                        refused.usage,
                        iterations.as_deref(),
                    ));
                if self.usage_active {
                    let shares = shares(&refused.by_model);
                    self.steps.step_ended(&mut self.usage, None, Some(&shares));
                }
            }
            Record::Attempt(attempt) if self.usage_active => {
                let envelope = attempt
                    .billing
                    .as_ref()
                    .and_then(|billing| billing.get("envelope"));
                let served = envelope.and_then(|envelope| {
                    envelope
                        .get("speed_used")
                        .and_then(Value::as_str)
                        .or_else(|| envelope.get("service_tier_used").and_then(Value::as_str))
                });
                self.steps.attempt(
                    broca_protocol::StepId(attempt.step_id),
                    attempt.sent_at_ms,
                    attempt.first_content_at_ms,
                    attempt.last_content_at_ms,
                    served,
                );
            }
            Record::StepStart(_) => {
                self.active_zero_step_seed = false;
                self.pending_steer_ids.clear();
                if self.usage_active {
                    self.steps.boundary();
                }
            }
            Record::BatchStart(_) => self.indeterminate.reset_batch(),
            Record::ToolIntent(intent) => self
                .indeterminate
                .record_dispatch_intent(&intent.tool_call_id),
            Record::ToolOutcome(outcome) => {
                self.indeterminate.record_dispatch_terminal(
                    &outcome.tool_call_id,
                    outcome.outcome == CallOutcome::Unknown,
                );
                self.held.retain(|held| held != &outcome.tool_call_id);
            }
            Record::ToolHeld(held) if ours => self.held.push(held.tool_call_id.clone()),
            Record::ToolReply(reply) => self.last_reply = Some(reply.result.tool_call_id.clone()),
            Record::Steer(steer) => {
                if let Some(send_id) = steer
                    .receipt
                    .as_ref()
                    .and_then(|receipt| receipt.get("send_id"))
                    .and_then(Value::as_str)
                {
                    self.pending_steer_ids.push(send_id.to_owned());
                }
            }
            Record::EntryFacts(facts) if facts.run_id.as_deref() == Some(self.run_id) => {
                self.placed.insert(facts.ordinal, facts.mid.clone());
            }
            Record::RunEnd(end) => {
                self.active_zero_step_seed = false;
                if ours {
                    let error = end
                        .error
                        .as_ref()
                        .map(|error| decode(error, "a run end's error"))
                        .transpose()?;
                    self.end(End {
                        reason: end.reason,
                        usage: end.usage.unwrap_or_default(),
                        retries_used: end.retries_used,
                        interrupt_reason: end.interrupt_reason.clone(),
                        error,
                    });
                }
            }
            Record::Pause(pause) if pause.run_id == self.run_id => {
                let mut state = fresh_state(RunIndexState::Paused);
                state.account_limit_pause = pause
                    .account_limit
                    .as_ref()
                    .map(|pause| decode(pause, "an account-limit pause").map(Box::new))
                    .transpose()?;
                state.usage = Some(self.total_usage);
                state.retries_used = self.total_retries_used;
                state.error = pause
                    .error
                    .as_ref()
                    .map(|error| decode(error, "a pause's error"))
                    .transpose()?;
                state.pause_reason = Some(pause.reason);
                state.auth_selection.clone_from(&self.auth_selection);
                self.found = Some(state);
            }
            Record::QueuedSend(send)
                if send.submission_id == format!("sub-{}", self.run_id) && self.title.is_none() =>
            {
                self.title = receipt_title(send.receipt.as_ref());
            }
            Record::Retraction(retraction) => {
                if let Some(failure) = retraction.failure.as_ref() {
                    let failure: broca_wal::QueuedAdmissionFailure =
                        decode(failure, "a queued send's admission failure")?;
                    if failure.run_id.0 == self.run_id {
                        self.found = Some(admission_failed(failure.error));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn run_start(&mut self, start: &RunStart) -> Result<(), StatusError> {
        let mine = start.run_id == self.run_id;
        self.usage_active = mine;
        if mine {
            let config = broca_session_loop::run::run_config_of_start(start, self.parts)
                .map_err(|error| damaged(format!("the run's start: {error:?}")))?;
            self.steps
                .run_started(&mut self.usage, &config.frozen_render_config);
            self.frozen_structured_output =
                broca_protocol::structured_output::FrozenStructuredOutput::from_frozen_render_config(
                    Some(&config.frozen_render_config),
                );
            self.wake_directive = config.turn_directives_v1.as_ref().is_some_and(|names| {
                names.contains(&cortexkit_role_llm_runner::directives::DirectiveName::Wake)
            });
            self.cleans_text =
                broca_core::tags::computed_rules(&config).is_some_and(|rules| rules.clean);
            self.frozen_finish_config = config.frozen_render_config;
            self.auth_selection = config.auth_selection;
            self.title.clone_from(&start.title);
        }
        self.steps.boundary();
        self.active_run = Some(start.run_id.clone());
        self.budget_detail = None;
        self.context_ceiling = None;
        self.pending_steer_ids.clear();
        self.active_zero_step_seed = start.seeded == Some(true);
        self.has_completed_step = false;
        self.total_usage = Default::default();
        self.total_retries_used = 0;
        self.last_step_finish_reason = None;
        self.indeterminate.reset_episode();
        if mine {
            self.found = Some(fresh_state(RunIndexState::Active));
        }
        Ok(())
    }

    /// Records of today's that the new format keeps whole, by today's record name.
    fn event(&mut self, event: &broca_session_store::record::Event) -> Result<(), StatusError> {
        let ours = self.ours();
        let body = event.body.as_ref().unwrap_or(&Value::Null);
        let deadline = || body.get("deadline_ms").and_then(Value::as_u64);
        match event.name.as_str() {
            "context_ceiling_wait" if ours => {
                if let Some(state) = &mut self.found {
                    state.ceiling_deadline_ms = deadline();
                }
            }
            "compaction_ride_wait" if ours => {
                if let Some(state) = &mut self.found {
                    state.ride_wait_deadline_ms = deadline();
                }
            }
            "compaction_ride_wait_ended" if ours => {
                if let Some(state) = &mut self.found {
                    state.ride_wait_deadline_ms = None;
                }
            }
            "input_token_budget_exhausted" => {
                self.budget_detail = Some(decode(
                    body.get("detail").unwrap_or(&Value::Null),
                    "an input-token budget's detail",
                )?);
            }
            "recovery_cut" => {
                let recovery = body.get("recovery").unwrap_or(&Value::Null);
                let runs = recovery
                    .get("runs")
                    .and_then(Value::as_array)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                let named =
                    |run: &Value| run.get("run_id").and_then(Value::as_str) == Some(self.run_id);
                let calls: Vec<ToolCallId> = runs
                    .iter()
                    .filter(|run| named(run))
                    .flat_map(|run| {
                        run.get("calls")
                            .and_then(Value::as_array)
                            .map(Vec::as_slice)
                            .unwrap_or_default()
                    })
                    .filter_map(|call| call.get("tool_call_id").and_then(Value::as_str))
                    .map(|id| ToolCallId(id.to_owned()))
                    .collect();
                let active =
                    recovery.get("active_run").and_then(Value::as_str) == Some(self.run_id);
                if runs.iter().any(named) || active {
                    for call in &calls {
                        self.indeterminate.record_dispatch_terminal(call, true);
                    }
                    self.recovered(calls);
                }
            }
            "recovery_call_closed" if event.run_id.as_deref() == Some(self.run_id) => {
                let call = self.last_reply.clone().ok_or_else(|| {
                    damaged("a closed recovery call without the reply that closed it")
                })?;
                self.indeterminate.record_dispatch_terminal(&call, true);
                self.recovered(vec![call]);
            }
            _ => {}
        }
        Ok(())
    }

    /// A recovery closed `calls` of this run, which may have run: the run reads as interrupted
    /// unless it already ended, and the calls are indeterminate.
    fn recovered(&mut self, calls: Vec<ToolCallId>) {
        let state = self
            .found
            .get_or_insert_with(|| fresh_state(RunIndexState::Interrupted));
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

    /// The run ended, as its end states.
    fn end(&mut self, end: End) {
        let structured_output = broca_core::replay::ended_structured_output(
            self.structured_output_result.clone(),
            self.frozen_structured_output.as_ref(),
        );
        let mut state = fresh_state(ended_state(end.reason));
        state.budget_detail.clone_from(&self.budget_detail);
        state.finish = self.found.as_ref().and_then(|state| state.finish.clone());
        state.context_ceiling.clone_from(&self.context_ceiling);
        state.structured_output = structured_output;
        state.undelivered_steers = Some(self.pending_steer_ids.clone());
        state.final_step_finish_reason = self.last_step_finish_reason;
        state.indeterminate_tool_calls =
            (end.reason == RunFinishReason::Interrupted).then(|| self.indeterminate.snapshot());
        state.usage = self.has_completed_step.then_some(end.usage);
        state.retries_used = end.retries_used;
        state.error = end.error;
        if end.reason == RunFinishReason::Interrupted {
            self.interrupt_reason = end.interrupt_reason;
        }
        self.found = Some(state);
    }

    /// The run's state once every record is walked, as today's reconciliation finishes it.
    fn finish(self) -> Option<(ReconciledRunState, WalkRest)> {
        let rest = WalkRest {
            interrupt_reason: self.interrupt_reason,
            wake_directive: self.wake_directive,
            cleans_text: self.cleans_text,
            held: self.held,
            placed: self.placed,
        };
        if self.active_run.as_deref() == Some(self.run_id) && self.active_zero_step_seed {
            let mut state = fresh_state(RunIndexState::Interrupted);
            state.undelivered_steers = Some(self.pending_steer_ids);
            state.indeterminate_tool_calls = Some(Vec::new());
            state.run_usage = self.usage;
            state.auth_selection = self.auth_selection;
            state.title = self.title;
            return Some((state, rest));
        }
        let state = self.found?;
        Some((
            ReconciledRunState {
                auth_selection: self.auth_selection,
                title: self.title,
                run_usage: self.usage,
                ..state
            },
            rest,
        ))
    }
}

/// What a walk found besides the run's state.
struct WalkRest {
    interrupt_reason: Option<String>,
    wake_directive: bool,
    cleans_text: bool,
    held: Vec<ToolCallId>,
    placed: std::collections::BTreeMap<u64, String>,
}

/// A step's usage by model as the shared usage arithmetic takes it.
fn shares(
    by_model: &[broca_session_store::record::ModelStepUsage],
) -> Vec<broca_core::replay::ModelShare<'_>> {
    by_model
        .iter()
        .map(|share| {
            (
                share.provider.as_str(),
                share.model.as_str(),
                share.variant.as_deref(),
                share.usage,
            )
        })
        .collect()
}

/// The title a queued send's receipt carries.
fn receipt_title(receipt: Option<&Value>) -> Option<String> {
    receipt?.get("title")?.as_str().map(str::to_owned)
}

/// The state of a run whose queued send failed its admission, as today's reconciliation reads
/// its `Retracted` record with a failure: an error, with no steer left undelivered.
fn admission_failed(error: broca_protocol::ProviderError) -> ReconciledRunState {
    let mut state = fresh_state(RunIndexState::Error);
    state.undelivered_steers = Some(Vec::new());
    state.error = Some(error);
    state
}

/// Why a completed run's final message couldn't be read.
#[derive(Debug, Clone)]
pub enum FinalError {
    /// A revert removed the run's messages from the transcript, so the message the run ended
    /// with is no longer the one at its place. Today's storage still reads it from its log.
    Reverted,
    Damaged(String),
}

impl From<FinalError> for StatusError {
    fn from(error: FinalError) -> Self {
        match error {
            FinalError::Reverted => unserved(
                "final_message_reverted",
                "the run's final message was reverted out of the transcript",
            ),
            FinalError::Damaged(message) => damaged(message),
        }
    }
}

/// The facts of the run `run_id`, or `None` when the session holds no run with that id and no
/// queued send minted from it that failed its admission. `live` says whether a store actor holds
/// the session open, so an unfinished latest run is running, or paused as the fold shows. With
/// none, an unfinished run that isn't paused was cut off by a crash: its end is the one the
/// session's next open records, when the fold alone decides it.
///
/// `settled` keeps the facts of runs that have ended, keyed by the session's address `addr`.
pub async fn run_facts<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    run_id: &str,
    live: bool,
    settled: Option<(&SettledRuns, &str)>,
) -> Result<Option<RunFacts>, StatusError> {
    let key = settled.map(|(_, addr)| {
        (
            addr.to_owned(),
            session.head().incarnation.0,
            session.head().history_version,
            run_id.to_owned(),
        )
    });
    if let (Some((cache, _)), Some(key)) = (settled, &key) {
        if let Some(facts) = cache.get(key) {
            return Ok(Some(facts));
        }
    }
    let latest = session
        .operations
        .run
        .as_ref()
        .filter(|run| run.run_id == run_id);
    // The latest run's start, when the session keeps it in memory, is the one the id index would
    // find: a run id names one run start. Reading it from memory leaves the index unread.
    let kept_start = latest.and_then(|_| session.kept_run_start(run_id));
    let named = match kept_start {
        Some(_) => Vec::new(),
        None => session.find_id(IdKind::RunId, run_id).await?,
    };
    let first = kept_start.or_else(|| {
        named
            .iter()
            .find(|(_, record)| matches!(record, Record::RunStart(_)))
            .map(|(seq, _)| *seq)
    });
    let records = match (first, latest) {
        // No run started under this id: it may name a queued send that failed its admission.
        (None, _) => {
            session
                .find_id(IdKind::SubmissionId, &format!("sub-{run_id}"))
                .await?
        }
        (Some(first), Some(_)) => session.records_from(first).await?,
        (Some(first), None) => {
            let last = named
                .iter()
                .filter(|(_, record)| matches!(record, Record::RunEnd(_)))
                .map(|(seq, _)| *seq)
                .max()
                .ok_or_else(|| {
                    damaged(format!(
                        "the run {run_id} has no end, yet a later run started"
                    ))
                })?;
            session.records_between(first, last).await?
        }
    };
    let mut walk = Walk::new(run_id, parts);
    for (_, record) in &records {
        walk.record(record)?;
    }
    let unfinished = latest.filter(|run| run.finished.is_none() && run.paused.is_none());
    let mut predicted = false;
    if let Some(run) = unfinished.filter(|_| !live) {
        if let Some(end) = predicted_end(session, run, &mut walk)? {
            walk.active_zero_step_seed = false;
            walk.end(end);
            predicted = true;
        }
    }
    let Some((mut state, rest)) = walk.finish() else {
        return Ok(None);
    };
    if unfinished.is_some() && !predicted {
        // Running in the open actor, or waiting on calls a provider holds: the run is live, even
        // if it paused before and was resumed since.
        state.state = RunIndexState::Active;
        state.pause_reason = None;
        state.account_limit_pause = None;
        state.error = None;
    }
    let holds_calls = matches!(state.state, RunIndexState::Active | RunIndexState::Paused)
        && !rest.held.is_empty();
    let final_message = if state.state == RunIndexState::Completed {
        Some(final_message(session, &rest.placed, rest.cleans_text).await)
    } else {
        None
    };
    let facts = RunFacts {
        state,
        interrupt_reason: rest.interrupt_reason,
        wake_directive: rest.wake_directive,
        final_message,
        holds_calls,
    };
    // A final message that couldn't be read is read again next time rather than kept refused.
    let complete = !matches!(facts.final_message, Some(Err(_)));
    if let (Some((cache, _)), Some(key)) = (settled, key) {
        if facts.settled() && !predicted && complete {
            cache.put(key, facts.clone());
        }
    }
    Ok(Some(facts))
}

/// How the session's next open ends its unfinished latest run `run`, when the fold alone decides
/// it: the same decision the open makes (`store_actor::recovery_mode`), and the records the run
/// loop then writes (`broca_session_loop::run::seal` and `resume`), folded into `walk`. `None`
/// when the open leaves the run as it is. An end that depends on anything else (a resume that
/// would call the provider or dispatch a call first) is refused `run_end_unrecorded`.
fn predicted_end<B: SessionBackend>(
    session: &Session<B>,
    run: &Run,
    walk: &mut Walk<'_>,
) -> Result<Option<End>, StatusError> {
    use crate::executor::StoreRunMode;
    use broca_core::resume::SealCause;
    let unrecorded = || StatusError::EndUnrecorded {
        run_id: run.run_id.clone(),
    };
    let end = |reason, interrupt_reason, error| End {
        reason,
        usage: run.usage,
        retries_used: run.retries_used,
        interrupt_reason,
        error,
    };
    match crate::store_actor::recovery_mode(run) {
        None => Ok(None),
        Some(StoreRunMode::Seal(SealCause::CrashOrphan)) => {
            if !close_open_batch(session, run, walk) {
                // With no open batch, a tool turn whose calls were never dispatched closes in a
                // batch of its own.
                let planned = session.conversation.open_tool_turn().is_some_and(|open| {
                    open.content.iter().any(|block| {
                        matches!(block, broca_protocol::content::ContentBlock::ToolCall(_))
                    })
                });
                if planned {
                    walk.indeterminate.reset_batch();
                }
            }
            Ok(Some(end(
                RunFinishReason::Interrupted,
                Some("runner_crash".into()),
                Some(broca_core::resume::crash_orphan_error()),
            )))
        }
        Some(StoreRunMode::Resume) => match run.resolution() {
            // The run's last step ended it; the resume writes only its end.
            Resolution::Idle(reason) => Ok(Some(end(reason, None, None))),
            // A cancel waits on a run whose calls may have run: the resume closes them and ends
            // the run interrupted, sending nothing.
            Resolution::Interrupted(_) => {
                close_open_batch(session, run, walk);
                Ok(Some(end(RunFinishReason::Interrupted, None, None)))
            }
            // The resume checks the provider's renderer and may send the step again before it
            // reaches the cancel.
            _ => Err(unrecorded()),
        },
        Some(_) => Err(unrecorded()),
    }
}

/// Folds into `walk` the outcomes a crash resume records for the run's open batch, sending
/// nothing: a call sent with no result recorded has an unknown outcome, and every other unanswered
/// call didn't run. `false` when the run has no open batch.
fn close_open_batch<B: SessionBackend>(
    session: &Session<B>,
    run: &Run,
    walk: &mut Walk<'_>,
) -> bool {
    let Some(batch) = run.open_batch.as_ref() else {
        return false;
    };
    let calls = session
        .conversation
        .open_tool_turn()
        .into_iter()
        .flat_map(|open| open.content.iter())
        .filter_map(|block| match block {
            broca_protocol::content::ContentBlock::ToolCall(call) => Some(&call.tool_call_id),
            _ => None,
        });
    for id in calls {
        let unknown = match batch.state_of(id) {
            Some(CallState::Dispatched) => true,
            Some(CallState::NotSent | CallState::HeldSuccessor | CallState::ProviderHeld) => false,
            Some(CallState::Terminated) | None => continue,
        };
        walk.indeterminate.record_dispatch_terminal(id, unknown);
        walk.held.retain(|held| held != id);
    }
    true
}

/// The final message of a completed run that placed the messages `placed` (ordinal to id): the
/// last assistant message of the transcript as it stood when the run ended. `cleans_text` says
/// whether the run's tag rules clean the text of the messages it wrote.
async fn final_message<B: SessionBackend>(
    session: &Session<B>,
    placed: &std::collections::BTreeMap<u64, String>,
    cleans_text: bool,
) -> Result<broca_wire::FinalMessage, FinalError> {
    let read = |error: SessionError| FinalError::Damaged(format!("{error:?}"));
    let Some((&last, _)) = placed.iter().next_back() else {
        return Err(FinalError::Damaged(
            "the completed run placed no message".into(),
        ));
    };
    let mut at = last;
    loop {
        let from = at.saturating_sub(31).max(1);
        let entries = session
            .entries(from, usize::try_from(at - from + 1).unwrap_or(usize::MAX))
            .await
            .map_err(read)?;
        if at == last && entries.last().map(|entry| entry.ordinal) != Some(last) {
            return Err(FinalError::Reverted);
        }
        for entry in entries.iter().rev() {
            let mid = entry.state.facts.as_ref().map(|facts| facts.mid.as_str());
            if placed
                .get(&entry.ordinal)
                .is_some_and(|expected| Some(expected.as_str()) != mid)
            {
                return Err(FinalError::Reverted);
            }
            if entry.message.role == broca_protocol::content::Role::Assistant {
                let facts = entry.state.facts.as_ref().ok_or_else(|| {
                    FinalError::Damaged(format!("message {} has no entry facts", entry.ordinal))
                })?;
                // The message's tag rules aren't attached to it here. Its entry facts say
                // whether the run that wrote it cleans the model's text on a session on the
                // current compaction contract; on a session converted from an older one, the
                // writing run's frozen rules say it.
                let ours = placed.contains_key(&entry.ordinal);
                return Ok(broca_wire::FinalMessage {
                    ordinal: entry.ordinal,
                    mid: facts.mid.clone(),
                    text: crate::serve::joined_text(
                        &entry.message,
                        facts.clean_reply || (ours && cleans_text),
                    ),
                });
            }
        }
        if from == 1 {
            return Err(FinalError::Damaged(
                "the completed run has no assistant message".into(),
            ));
        }
        at = from - 1;
    }
}

/// The no-run `status` of a store session that no actor holds asks the actor anyway when its
/// latest run is unfinished and not paused, or when sends wait in its queue: opening the actor
/// recovers the run and drains the queue, as today's `status` wakes a session a crash left so.
pub async fn status_needs_actor(
    store: &crate::store_open::StoreRuntime,
    key: &broca_protocol::BindIdentity,
) -> Result<bool, crate::registry::OpenError> {
    let Some(session) = store.current_view(key).await? else {
        return Ok(false);
    };
    let unfinished = session
        .operations
        .run
        .as_ref()
        .is_some_and(|run| run.finished.is_none() && run.paused.is_none());
    Ok(unfinished || !session.operations.queue.is_empty())
}

/// A read of a store session's run and send facts, served by its actor when one holds it open,
/// else from the session as stored (see `store_reads`).
pub struct StoreRead<'a> {
    pub store: &'a crate::store_open::StoreRuntime,
    pub key: &'a broca_protocol::BindIdentity,
    registry: &'a crate::LineageRegistry,
}

impl<'a> StoreRead<'a> {
    /// A read of the session `key` kept in `store`, under `registry`'s slots.
    pub fn new(
        store: &'a crate::store_open::StoreRuntime,
        key: &'a broca_protocol::BindIdentity,
        registry: &'a crate::LineageRegistry,
    ) -> Self {
        Self {
            store,
            key,
            registry,
        }
    }
}

impl StoreRead<'_> {
    async fn read(
        &self,
        request: crate::store_reads::StoreReadRequest,
    ) -> Result<Option<crate::store_reads::StoreReadReply>, crate::serve::ServeError> {
        self.store.read(self.registry, self.key, request).await
    }

    /// `run.status` for the run `run_id`: its own status, `unknown` when no run carries the id.
    pub async fn run_status(
        &self,
        run_id: &RunId,
    ) -> Result<broca_wire::RunStatusResponse, crate::serve::ServeError> {
        use crate::store_reads::{StoreReadReply, StoreReadRequest};
        match self
            .read(StoreReadRequest::RunStatus(run_id.clone()))
            .await?
        {
            None => Ok(broca_wire::RunStatusResponse::Unknown),
            Some(StoreReadReply::RunStatus(reply)) => reply,
            Some(_) => Err(crate::store_reads::mismatched()),
        }
    }

    /// `run.status` for the run id `run_id` as the wire answers it (see
    /// [`Reader::run_status_reply`]).
    pub async fn run_status_reply(
        &self,
        run_id: &RunId,
    ) -> Result<broca_wire::RunStatusReply, crate::serve::ServeError> {
        use crate::store_reads::{StoreReadReply, StoreReadRequest};
        match self
            .read(StoreReadRequest::RunStatusReply(run_id.clone()))
            .await?
        {
            None => Ok(broca_wire::RunStatusResponse::Unknown.into()),
            Some(StoreReadReply::RunStatusReply(reply)) => reply,
            Some(_) => Err(crate::store_reads::mismatched()),
        }
    }

    /// `run.result` for the run `run_id` (see [`Reader::run_result`]).
    pub async fn run_result(
        &self,
        run_id: &RunId,
    ) -> Result<broca_wire::RunResultResponse, crate::serve::ServeError> {
        use crate::store_reads::{StoreReadReply, StoreReadRequest};
        match self
            .read(StoreReadRequest::RunResult(run_id.clone()))
            .await?
        {
            None => Err(crate::serve::ServeError::UnknownRun(run_id.0.clone())),
            Some(StoreReadReply::RunResult(reply)) => reply,
            Some(_) => Err(crate::store_reads::mismatched()),
        }
    }

    /// `send.status` and `session.send_status` (see [`Reader::send_status`]).
    pub async fn send_status(
        &self,
        own_run: RunId,
        send_id: &str,
        principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Result<broca_wire::SendReplyWire, crate::serve::ServeError> {
        use crate::store_reads::{StoreReadReply, StoreReadRequest};
        let request = StoreReadRequest::SendStatus {
            own_run,
            send_id: send_id.to_owned(),
            principal: principal.cloned(),
        };
        match self.read(request).await? {
            None => Err(crate::serve::ServeError::UnknownSend),
            Some(StoreReadReply::SendStatus(reply)) => reply,
            Some(_) => Err(crate::store_reads::mismatched()),
        }
    }
}

/// Serves run and send facts from a loaded session kept in the new storage: the module through
/// [`StoreRead`], and the converter's check over a converted session.
#[derive(Clone, Copy)]
pub struct Reader<'a> {
    /// The deployment's setup parts, which a run's start names.
    pub parts: &'a dyn Parts,
    /// Whether a store actor holds the session, so an unfinished run is running (see
    /// [`run_facts`]).
    pub live: bool,
    /// Where finished runs' facts are kept, and the session's address that keys them.
    pub settled: Option<(&'a SettledRuns, &'a str)>,
    /// The session's id, which a reply too large to send names.
    pub session_name: &'a str,
    /// The in-flight model step of the run the session's actor is running, which an active
    /// run's status serves as today's does. `None` where no actor answers.
    pub live_step: Option<&'a crate::store_reads::LiveStep>,
}

impl Reader<'_> {
    async fn facts<B: SessionBackend>(
        &self,
        session: &Session<B>,
        run_id: &str,
    ) -> Result<Option<RunFacts>, StatusError> {
        run_facts(session, self.parts, run_id, self.live, self.settled).await
    }

    /// The run's `run.status` from its facts, with the actor's in-flight step when the run is
    /// the one the actor is running and still active.
    fn status(
        &self,
        facts: Option<RunFacts>,
        run_id: &str,
    ) -> Result<broca_wire::RunStatusResponse, crate::serve::ServeError> {
        let live_step = self
            .live_step
            .filter(|live| live.run_id == run_id)
            .and_then(|live| live.step.clone());
        status_response(facts, run_id, live_step)
    }

    /// `run.status` for the run `run_id`: its own status, `unknown` when no run carries the id.
    pub async fn run_status<B: SessionBackend>(
        &self,
        session: &Session<B>,
        run_id: &RunId,
    ) -> Result<broca_wire::RunStatusResponse, crate::serve::ServeError> {
        self.status(self.facts(session, &run_id.0).await?, &run_id.0)
    }

    /// `run.status` for the run id `run_id` as the wire answers it: the run's status, or, for a
    /// run id a queued send was minted from and no run carries, what became of that send.
    pub async fn run_status_reply<B: SessionBackend>(
        &self,
        session: &Session<B>,
        run_id: &RunId,
    ) -> Result<broca_wire::RunStatusReply, crate::serve::ServeError> {
        use broca_core::delivery_order::QueuedSendFate;
        use broca_wire::{QueuedSendState, RunStatusReply};
        if let Some(facts) = self.facts(session, &run_id.0).await? {
            return self.status(Some(facts), &run_id.0).map(Into::into);
        }
        // No run carries this id. It may still be the id a queued send was minted from, which
        // callers poll until the send is a run; the session's records say what became of it.
        Ok(match queued_send_fate(session, &run_id.0).await? {
            None => broca_wire::RunStatusResponse::Unknown.into(),
            Some(QueuedSendFate::Pending) => RunStatusReply::Send(QueuedSendState::Pending),
            Some(QueuedSendFate::Ended(reason)) => RunStatusReply::Send(match reason {
                broca_wal::record::SendEndReason::Superseded { .. } => QueuedSendState::Superseded,
                broca_wal::record::SendEndReason::Expired => QueuedSendState::Expired,
            }),
            Some(QueuedSendFate::Retracted) => RunStatusReply::Send(QueuedSendState::Retracted),
            // Delivered into a run that was already going: the send is part of that run now,
            // so its status is that run's, with the receipt naming the run.
            Some(QueuedSendFate::Delivered { run_id: into, .. }) => RunStatusReply::Run {
                status: Box::new(self.status(self.facts(session, &into.0).await?, &into.0)?),
                delivered: Some(
                    cortexkit_role_llm_runner::send::Delivered::new(
                        cortexkit_role_llm_runner::send::delivered_as::STEP,
                    )
                    .with_ref(into.0),
                ),
            },
            // A run naming the send started: the lookup above found no run with this id, so it
            // started under another, which the send's own receipt names.
            Some(QueuedSendFate::Started) => broca_wire::RunStatusResponse::Unknown.into(),
        })
    }

    /// `run.result` for the run `run_id`: its state and, once it completed, its final message.
    pub async fn run_result<B: SessionBackend>(
        &self,
        session: &Session<B>,
        run_id: &RunId,
    ) -> Result<broca_wire::RunResultResponse, crate::serve::ServeError> {
        use crate::serve::ServeError;
        let unknown = || ServeError::UnknownRun(run_id.0.clone());
        let facts = self.facts(session, &run_id.0).await?.ok_or_else(unknown)?;
        facts.check_held()?;
        let RunFacts {
            state,
            interrupt_reason,
            wake_directive,
            final_message,
            ..
        } = facts;
        let auth_selection = crate::serve::frozen_auth_selection(&state);
        let detail = state.budget_detail.clone();
        let context_ceiling = state.context_ceiling.clone();
        let structured_output = state.structured_output.clone();
        let usage = state.run_usage.clone();
        let finish = state.finish.clone();
        let status = crate::serve::run_status_response_with_reader_policy(Some(state), false);
        let (state, mut reason, error) = crate::serve::result_state(status).ok_or_else(unknown)?;
        if state == "interrupted" {
            reason = interrupt_reason;
        }
        let mut directives = None;
        let final_message = if state == "completed" {
            let message = final_message
                .ok_or_else(|| damaged("a completed run's final message wasn't read"))?
                .map_err(StatusError::from)?;
            if wake_directive {
                directives = crate::serve::wake_directives(&message.text);
            }
            Some(message)
        } else {
            None
        };
        crate::serve::checked_run_result(
            broca_wire::RunResultResponse {
                finish: if matches!(state.as_str(), "active" | "paused") {
                    None
                } else {
                    finish
                },
                auth_selection,
                directives,
                usage,
                detail,
                context_ceiling,
                run_id: run_id.0.clone(),
                state,
                reason,
                error,
                final_message,
                structured_output,
            },
            self.session_name,
        )
    }
}

/// A run's `run.status` from its facts, as today's builder answers it. `live_step` is the
/// in-flight step an active run serves, from the actor running it.
pub(crate) fn status_response(
    facts: Option<RunFacts>,
    run_id: &str,
    live_step: Option<broca_protocol::provider::InFlightStep>,
) -> Result<broca_wire::RunStatusResponse, crate::serve::ServeError> {
    use broca_wire::RunStatusResponse;
    let Some(mut facts) = facts else {
        return Ok(RunStatusResponse::Unknown);
    };
    facts.check_held()?;
    if facts.state.state == RunIndexState::Active {
        facts.state.live_step = live_step;
    }
    let mut response =
        crate::serve::run_status_response_with_reader_policy(Some(facts.state), false);
    match &mut response {
        RunStatusResponse::Interrupted { reason, .. } => *reason = facts.interrupt_reason,
        RunStatusResponse::Completed { directives, .. } if facts.wake_directive => {
            *directives = facts
                .final_message
                .and_then(Result::ok)
                .and_then(|message| crate::serve::wake_directives(&message.text));
        }
        _ => {}
    }
    crate::serve::checked_run_status(response, run_id)
}

/// What became of the queued send minted from `run_id` (submission `sub-<run_id>`), from the
/// records naming that submission, as `broca_core::delivery_order::queued_send_fate` reads
/// today's: the first record that settles it wins. `None` when no queued send has that
/// submission.
pub async fn queued_send_fate<B: SessionBackend>(
    session: &Session<B>,
    run_id: &str,
) -> Result<Option<broca_core::delivery_order::QueuedSendFate>, StatusError> {
    use broca_core::delivery_order::QueuedSendFate;
    let submission = format!("sub-{run_id}");
    let mut records = session.find_id(IdKind::SubmissionId, &submission).await?;
    // A send that started its own run directly is named by that run's start.
    records.extend(
        session
            .find_id(IdKind::RunId, run_id)
            .await?
            .into_iter()
            .filter(|(_, record)| matches!(record, Record::RunStart(_))),
    );
    records.sort_by_key(|(seq, _)| *seq);
    records.dedup_by_key(|(seq, _)| *seq);
    let mut fate = None;
    for (_, record) in records {
        let settled = match &record {
            Record::QueuedSend(send) if send.submission_id == submission => {
                fate.get_or_insert(QueuedSendFate::Pending);
                continue;
            }
            Record::Delivery(delivery) if delivery.submission_id == submission => {
                QueuedSendFate::Delivered {
                    run_id: RunId(delivery.run_id.clone()),
                    mode: decode(&Value::String(delivery.mode.clone()), "a delivery's mode")?,
                }
            }
            Record::RunStart(start)
                if start.run_id == run_id
                    || start.submission_id.as_deref() == Some(submission.as_str()) =>
            {
                QueuedSendFate::Started
            }
            Record::SendEnded(ended) if ended.submission_id == submission => {
                QueuedSendFate::Ended(decode(
                    ended.reason.as_ref().unwrap_or(&Value::Null),
                    "why a queued send ended",
                )?)
            }
            Record::Retraction(retraction) if retraction.submission_id == submission => {
                QueuedSendFate::Retracted
            }
            _ => continue,
        };
        match fate {
            None | Some(QueuedSendFate::Pending) => fate = Some(settled),
            Some(_) => {}
        }
    }
    Ok(fate)
}

/// The records `send.status` reads a send's fate from, looked up by id as they are needed, as
/// `crate::send_fate` reads today's: a send's queued submission, the run it started, and a steer
/// sent under its key.
struct SendLookup<'a, B: SessionBackend> {
    session: &'a Session<B>,
}

impl<B: SessionBackend> SendLookup<'_, B> {
    /// Every record naming the queued submission minted from `run`, in seq order.
    async fn submission(&self, run: &RunId) -> Result<Vec<(u64, Record)>, StatusError> {
        Ok(self
            .session
            .find_id(IdKind::SubmissionId, &format!("sub-{}", run.0))
            .await?)
    }

    /// The first start of the run `run`, with its seq.
    async fn run_start(&self, run: &RunId) -> Result<Option<(u64, RunStart)>, StatusError> {
        Ok(self
            .session
            .find_id(IdKind::RunId, &run.0)
            .await?
            .into_iter()
            .find_map(|(seq, record)| match record {
                Record::RunStart(start) => Some((seq, *start)),
                _ => None,
            }))
    }

    /// The first steer sent under `send_id`, by its seq.
    async fn steer(&self, send_id: &str) -> Result<Option<u64>, StatusError> {
        Ok(self
            .session
            .find_id(IdKind::SendId, send_id)
            .await?
            .into_iter()
            .find_map(|(seq, record)| match record {
                Record::Steer(steer)
                    if steer
                        .receipt
                        .as_ref()
                        .and_then(|receipt| receipt.get("send_id"))
                        .and_then(Value::as_str)
                        == Some(send_id) =>
                {
                    Some(seq)
                }
                _ => None,
            }))
    }

    /// The run the steer at `seq` joined: the run whose start precedes it.
    async fn joined_run(&self, seq: u64) -> Result<Option<RunId>, StatusError> {
        let mut last = seq;
        while last > 1 {
            let first = last.saturating_sub(STEP_SPAN).max(1);
            let records = self.session.records_between(first, last - 1).await?;
            if let Some(start) = records.iter().rev().find_map(|(_, record)| match record {
                Record::RunStart(start) => Some(RunId(start.run_id.clone())),
                _ => None,
            }) {
                return Ok(Some(start));
            }
            last = first;
        }
        Ok(None)
    }

    /// Where the steer at `seq`, which joined `joined`, was delivered, as
    /// `crate::actor::steer_delivery` reads today's records: the first model step after it
    /// rendered it, in the run it joined (`step`) or a later one (`turn`). `None` while no step
    /// has.
    async fn steer_delivery(
        &self,
        seq: u64,
        joined: &RunId,
    ) -> Result<Option<cortexkit_role_llm_runner::send::Delivered>, StatusError> {
        use cortexkit_role_llm_runner::send::{delivered_as, Delivered};
        let end = self.session.next_seq();
        let mut first = seq + 1;
        while first < end {
            let last = first.saturating_add(STEP_SPAN).min(end - 1);
            for (_, record) in self.session.records_between(first, last).await? {
                if let Record::StepStart(step) = record {
                    let kind = if step.run_id == joined.0 {
                        delivered_as::STEP
                    } else {
                        delivered_as::TURN
                    };
                    return Ok(Some(Delivered::new(kind).with_ref(step.run_id)));
                }
            }
            first = last + 1;
        }
        Ok(None)
    }

    /// Who sent the queued submission whose records are `records`, as
    /// `send_fate::recorded_submission_sender` reads it: a delegated send's `DeliveryOrdered`
    /// sender, else its queued record's principal. `None` when its records repeat.
    fn submission_sender(
        records: &[(u64, Record)],
        submission: &str,
    ) -> Result<Option<broca_protocol::BindPrincipal>, StatusError> {
        let mut queued = false;
        let mut ordered = false;
        let mut principal = None;
        let mut sent_by = None;
        for (_, record) in records {
            match record {
                Record::DeliveryOrdered(order) if order.submission_id == submission => {
                    if ordered || queued {
                        return Ok(None);
                    }
                    ordered = true;
                    sent_by = principal_of(order.sent_by.as_ref())?;
                }
                Record::QueuedSend(send) if send.submission_id == submission => {
                    if queued {
                        return Ok(None);
                    }
                    queued = true;
                    principal = principal_of(receipt_principal(send.receipt.as_ref()))?;
                }
                _ => {}
            }
        }
        Ok(queued.then(|| sent_by.or(principal)).flatten())
    }

    /// Who sent the run `run`: `None` when the session holds no send of it, else the sender
    /// recorded, as `send_fate::run_sender` reads it.
    async fn run_sender(
        &self,
        run: &RunId,
    ) -> Result<Option<Option<broca_protocol::BindPrincipal>>, StatusError> {
        let submission = format!("sub-{}", run.0);
        let records = self.submission(run).await?;
        if records.iter().any(
            |(_, record)| matches!(record, Record::QueuedSend(send) if send.submission_id == submission),
        ) {
            return Ok(Some(Self::submission_sender(&records, &submission)?));
        }
        match self.run_start(run).await? {
            Some((_, start)) => Ok(Some(principal_of(start.principal.as_ref())?)),
            None => Ok(None),
        }
    }

    /// The run id a delegated sender's send under `send_id` took, as
    /// `send_fate::resolve_sender_run_id` decides it.
    async fn sender_run_id(
        &self,
        base: &RunId,
        send_id: &str,
        sender: &broca_protocol::BindPrincipal,
    ) -> Result<Result<RunId, String>, StatusError> {
        let qualified = match crate::send_fate::qualified_sender_run_id(base, sender) {
            Ok(qualified) => qualified,
            Err(error) => return Ok(Err(error)),
        };
        if let Some(recorded) = self.run_sender(&qualified).await? {
            return Ok(if recorded.as_ref() == Some(sender) {
                Ok(qualified)
            } else {
                Err("the qualified send ID has no matching durable sender".into())
            });
        }
        Ok(match self.run_sender(base).await? {
            Some(Some(recorded)) if &recorded == sender => Ok(base.clone()),
            Some(_) => Ok(qualified),
            None if self.steer(send_id).await?.is_some() => Ok(qualified),
            None => Ok(base.clone()),
        })
    }

    /// The run id and sender a retry by `sender` under `send_id` names, as
    /// `send_fate::resolve_retry_run_id` decides it: a successor sender may answer for its
    /// predecessor's send.
    async fn retry_run_id(
        &self,
        base: &RunId,
        send_id: &str,
        sender: &broca_protocol::BindPrincipal,
        senders: &[cortexkit_role_llm_runner::delivery_order::PlanSender],
    ) -> Result<Result<(RunId, broca_protocol::BindPrincipal), String>, StatusError> {
        if let broca_protocol::BindPrincipal::Reserved { module_id } = sender {
            if let Some(module) =
                cortexkit_role_llm_runner::sender_succession::predecessor(senders, module_id)
            {
                let predecessor = broca_protocol::BindPrincipal::Reserved {
                    module_id: module.into(),
                };
                let id = match self.sender_run_id(base, send_id, &predecessor).await? {
                    Ok(id) => id,
                    Err(error) => return Ok(Err(error)),
                };
                if self.run_sender(&id).await?.flatten().as_ref() == Some(&predecessor)
                    || self.legacy_steer_sender(&id, send_id).await?.as_ref() == Some(&predecessor)
                {
                    return Ok(Ok((id, predecessor)));
                }
            }
        }
        Ok(self
            .sender_run_id(base, send_id, sender)
            .await?
            .map(|id| (id, sender.clone())))
    }

    /// Whether a steer's own receipt may answer for `own_run`: no delivery order names its
    /// submission, so its key is the owner's, as `send_fate::legacy_steers_allowed` decides.
    async fn legacy_steers_allowed(&self, own_run: &RunId) -> Result<bool, StatusError> {
        let submission = format!("sub-{}", own_run.0);
        Ok(!self.submission(own_run).await?.iter().any(
            |(_, record)| matches!(record, Record::DeliveryOrdered(order) if order.submission_id == submission),
        ))
    }

    async fn legacy_steer_sender(
        &self,
        own_run: &RunId,
        send_id: &str,
    ) -> Result<Option<broca_protocol::BindPrincipal>, StatusError> {
        if self.legacy_steers_allowed(own_run).await? && self.steer(send_id).await?.is_some() {
            self.recorded_sender(own_run).await
        } else {
            Ok(None)
        }
    }

    /// The durable sender a receipt for `own_run` is authorized against, as
    /// `send_fate::recorded_send_sender` reads it: the first record of its delegated order,
    /// queued submission or start, else the session's first run's principal.
    async fn recorded_sender(
        &self,
        own_run: &RunId,
    ) -> Result<Option<broca_protocol::BindPrincipal>, StatusError> {
        let submission = format!("sub-{}", own_run.0);
        let mut records = self.submission(own_run).await?;
        if let Some((seq, start)) = self.run_start(own_run).await? {
            records.push((seq, Record::RunStart(Box::new(start))));
        }
        records.sort_by_key(|(seq, _)| *seq);
        for (_, record) in &records {
            match record {
                Record::DeliveryOrdered(order)
                    if order.submission_id == submission && order.sent_by.is_some() =>
                {
                    return principal_of(order.sent_by.as_ref());
                }
                Record::QueuedSend(send) if send.submission_id == submission => {
                    return principal_of(receipt_principal(send.receipt.as_ref()));
                }
                Record::RunStart(start) if start.run_id == own_run.0 => {
                    return principal_of(start.principal.as_ref());
                }
                _ => {}
            }
        }
        principal_of(
            self.session
                .operations
                .owner
                .first_start
                .as_ref()
                .and_then(|start| start.principal.as_ref()),
        )
    }
}

/// How far a forward or backward search for a run boundary reads at a time.
const STEP_SPAN: u64 = 256;

/// The principal a queued send's receipt records.
fn receipt_principal(receipt: Option<&Value>) -> Option<&Value> {
    receipt?.get("principal").filter(|value| !value.is_null())
}

fn principal_of(
    value: Option<&Value>,
) -> Result<Option<broca_protocol::BindPrincipal>, StatusError> {
    value
        .filter(|value| !value.is_null())
        .map(|value| decode(value, "a recorded principal"))
        .transpose()
}

impl Reader<'_> {
    /// `send.status` and `session.send_status`: what became of the send `send_id`, whose run id
    /// the serve layer derived as `own_run`, asked by `principal`. A send recorded under another
    /// sender reads as unknown, as today's does.
    pub async fn send_status<B: SessionBackend>(
        &self,
        session: &Session<B>,
        own_run: RunId,
        send_id: &str,
        principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Result<broca_wire::SendReplyWire, crate::serve::ServeError> {
        use crate::serve::ServeError;
        let lookup = SendLookup { session };
        let first_start = session.operations.owner.first_start.clone();
        let admission = |start: &RunStart| {
            broca_session_loop::run::admission_of_start(start, self.parts)
                .map_err(|error| damaged(format!("the session's first run's start: {error:?}")))
        };
        let (own_run, lookup_sender) =
            if let Some(sender @ broca_protocol::BindPrincipal::Reserved { .. }) = principal {
                let senders = match &first_start {
                    Some(start) => admission(start)?
                        .manifest
                        .as_deref()
                        .map(broca_core::admission::manifest_senders)
                        .unwrap_or_default(),
                    None => Vec::new(),
                };
                let (id, sender) = lookup
                    .retry_run_id(&own_run, send_id, sender, &senders)
                    .await?
                    .map_err(|_| ServeError::UnknownSend)?;
                (id, Some(sender))
            } else {
                (own_run, principal.cloned())
            };
        if lookup
            .recorded_sender(&own_run)
            .await?
            .is_some_and(|sender| Some(&sender) != lookup_sender.as_ref())
        {
            return Err(ServeError::UnknownSend);
        }
        let legacy_steers = lookup.legacy_steers_allowed(&own_run).await?;
        let outcome = self
            .send_outcome(session, &lookup, &own_run, send_id, legacy_steers)
            .await?
            .ok_or(ServeError::UnknownSend)?;
        let baseline = match first_start
            .as_ref()
            .filter(|start| start.run_id == own_run.0)
        {
            Some(start) => {
                let admission = admission(start)?;
                Some(match admission.manifest.as_deref() {
                    Some(manifest) => crate::serve::planned_baseline(manifest),
                    None => broca_wire::BaselineWire::plan_less(
                        admission
                            .tools
                            .iter()
                            .map(|tool| tool.name.clone())
                            .collect(),
                    ),
                })
            }
            None => None,
        };
        crate::serve::send_status_reply(outcome, &own_run, || baseline)
    }

    /// The recorded fate of the send `send_id` whose own run id is `own_run`, as
    /// `send_fate::recorded_send_outcome` reads it from today's records. `None` when the session
    /// holds no such send.
    async fn send_outcome<B: SessionBackend>(
        &self,
        session: &Session<B>,
        lookup: &SendLookup<'_, B>,
        own_run: &RunId,
        send_id: &str,
        legacy_steers: bool,
    ) -> Result<Option<crate::actor::SendOutcome>, StatusError> {
        use crate::actor::SendOutcome;
        use broca_core::delivery_order::QueuedSendFate;
        use cortexkit_role_llm_runner::send::{delivered_as, Delivered};
        if legacy_steers {
            if let Some(seq) = lookup.steer(send_id).await? {
                let Some(target) = lookup.joined_run(seq).await? else {
                    return Ok(None);
                };
                let delivered = lookup.steer_delivery(seq, &target).await?;
                return Ok(Some(
                    self.run_outcome(session, &target, delivered, false).await?,
                ));
            }
        }
        let submission_id = broca_protocol::SubmissionId(format!("sub-{}", own_run.0));
        for (_, record) in lookup.submission(own_run).await? {
            if let Record::Retraction(retraction) = record {
                if let Some(failure) = retraction.failure.as_ref() {
                    let failure: broca_wal::QueuedAdmissionFailure =
                        decode(failure, "a queued send's admission failure")?;
                    if retraction.submission_id == submission_id.0 && &failure.run_id == own_run {
                        return Ok(Some(self.run_outcome(session, own_run, None, false).await?));
                    }
                }
            }
        }
        let Some(fate) = queued_send_fate(session, &own_run.0).await? else {
            return Ok(None);
        };
        Ok(Some(match fate {
            QueuedSendFate::Pending => SendOutcome::Pending { submission_id },
            QueuedSendFate::Ended(reason) => SendOutcome::SendEnded {
                submission_id,
                reason,
            },
            QueuedSendFate::Retracted => SendOutcome::Retracted { submission_id },
            QueuedSendFate::Delivered { run_id, .. } => {
                let delivered = Delivered::new(delivered_as::STEP).with_ref(run_id.0.clone());
                self.run_outcome(session, &run_id, Some(delivered), true)
                    .await?
            }
            QueuedSendFate::Started => {
                let ordered = lookup.submission(own_run).await?.iter().any(|(_, record)| {
                    matches!(record, Record::DeliveryOrdered(order) if order.submission_id == submission_id.0)
                });
                let delivered =
                    ordered.then(|| Delivered::new(delivered_as::TURN).with_ref(own_run.0.clone()));
                self.run_outcome(session, own_run, delivered, true).await?
            }
        }))
    }

    /// A recorded send's outcome from the state of the run carrying it, as `send_fate`'s
    /// `run_outcome` reads it: today's reconciliation, which reports an unfinished run as
    /// active (or paused, when `report_pause`), whether or not anything runs it.
    async fn run_outcome<B: SessionBackend>(
        &self,
        session: &Session<B>,
        run_id: &RunId,
        delivered: Option<cortexkit_role_llm_runner::send::Delivered>,
        report_pause: bool,
    ) -> Result<crate::actor::SendOutcome, StatusError> {
        use crate::actor::SendOutcome;
        // Today's reconciliation reads an unfinished run as running whether or not anything
        // runs it.
        let reader = Reader {
            live: true,
            ..*self
        };
        let facts = reader.facts(session, &run_id.0).await?;
        let Some(state) = facts.map(|facts| facts.state) else {
            return Ok(SendOutcome::Error(
                "the run carrying a recorded send is missing from the session".into(),
            ));
        };
        Ok(
            if let Some(reason) = crate::actor::indexed_finish_reason(&state.state) {
                SendOutcome::Finished {
                    run_id: run_id.clone(),
                    reason,
                    delivered,
                }
            } else if let Some(reason) = state.pause_reason.filter(|_| report_pause) {
                SendOutcome::Paused {
                    run_id: run_id.clone(),
                    reason,
                    delivered,
                }
            } else {
                SendOutcome::Active {
                    run_id: run_id.clone(),
                    delivered,
                }
            },
        )
    }
}
