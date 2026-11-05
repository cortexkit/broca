//! The driver: admit a run, then alternate model steps and tool batches until a decision ends
//! the run.
//!
//! Each phase reads fold state from the writer, calls one seam, and appends what happened:
//! - **Admit:** the run's setup parts, `SetupStub` when the setup changed, `RunStart` and the
//!   run's input, as one durable batch before any model call.
//! - **Step:** `StepStart`, the model call, then the assistant message and `StepEnd`.
//! - **Decide:** finish with no tool calls; stop before dispatch when a stop condition holds;
//!   otherwise dispatch.
//! - **Batch:** `BatchStart` and every call's intent as one durable batch, then dispatch, then
//!   every reply and outcome and `BatchEnd` as one durable batch.
//! - **Finish:** `RunEnd`, durable before the run reports its end.

use std::sync::Arc;

use broca_core::capability::ToolManifest;
use broca_core::provider::{Provider, StepRuntime};
use broca_core::run::{limiting_stop_satisfied, stop_satisfied, OutputEvent};
use broca_core::tags::TagNumbering;
use broca_core::tool_plane::{BatchCtx, DispatchResult, ToolPlane};
use broca_protocol::caller::{PauseReason, RunFinishReason};
use broca_protocol::content::{
    stamp_ordinals_from, AssistantMessage, Message, ToolCall, ToolOutput, ToolResult,
};
use broca_protocol::error::ProviderError;
use broca_protocol::provider::{ModelAttempt, NormalizedEvent, ProviderControl};
use broca_protocol::request::EpisodeRenderInput;
use broca_protocol::request::{
    CallOptions, ExecutionMode, ModelHandle, OnRestart, ResponseFormat, StepContext, StopCondition,
    Tool, ToolChoice,
};
use broca_protocol::tags::{clean_computed_into, ComputedRules};
use broca_protocol::{BatchId, BindIdentity, FinishReason, GenerationConfig, RunId, StepId, Usage};
use broca_session_store::codec::{Digest, Message as _};
use broca_session_store::fold::operations::{CallState, Operations, Resolution};
use broca_session_store::record::{
    Assistant, Attempt, BatchEnd, BatchStart, CallOutcome, Input, PartKind, Pause, Placement,
    Record, Redo, Resumed, RunEnd, RunStart, SetupStub, StepEnd, StepStart, ToolIntent,
    ToolOutcome, ToolReply,
};
use broca_session_store::session::SessionError;
use broca_wal::ComputedTagRules;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::parts::{canonical, Parts};
use crate::writer::Writer;

/// The step limit a run gets when it names no stop condition, as today's loop.
pub const DEFAULT_MAX_STEPS: u64 = 100;

/// The cache-marker placement every new run freezes, as today's loop.
pub const MARKER_ORDER_VERSION: u32 = 1;

/// What admission decided for a run. The loop freezes all of it into the run's start.
#[derive(Debug, Clone)]
pub struct Admission {
    pub run_id: String,
    pub model: ModelHandle,
    /// The messages this run adds to the conversation.
    pub input: Vec<Message>,
    /// The tools the run may call, in the order the model sees them.
    pub tools: Vec<Tool>,
    pub tool_choice: ToolChoice,
    pub generation: GenerationConfig,
    pub response_format: ResponseFormat,
    pub provider_options: Value,
    /// The provider's frozen render config, opaque to the loop.
    pub render_config: Value,
    pub renderer_version: u32,
    /// Termination predicates, OR'd. Empty means a [`DEFAULT_MAX_STEPS`] step limit.
    pub stop_when: Vec<StopCondition>,
    /// What the run does when the runner stops while it runs: end interrupted (the default),
    /// or pause and continue as the same run after the runner starts again.
    pub on_restart: Option<OnRestart>,
    /// When the run was admitted and when its input was written.
    pub ts_ms: Option<u64>,
    pub written_at_ms: Option<u64>,
}

/// The seams a run calls. None of them is owned by the run.
pub struct Seams<'a, P, T> {
    pub provider: &'a P,
    pub tools: &'a T,
    /// The bind the session's tool calls are routed under.
    pub bind: BindIdentity,
    /// Where control and display events go, if anyone listens.
    pub out: Option<&'a mpsc::Sender<OutputEvent>>,
    /// Cancels the run now: a model step in flight stops and no further call is sent.
    pub cancel: CancellationToken,
    /// Asks the run to stop starting work because the runner is shutting down. Unlike
    /// `cancel`, it cuts nothing in flight: the run stops at its next step boundary, where it
    /// pauses or ends interrupted as its restart policy says.
    pub stop: CancellationToken,
    /// Milliseconds since the epoch, for the times records carry.
    pub now_ms: &'a (dyn Fn() -> u64 + Sync),
    /// The session's address, given to the provider for its diagnostics only.
    pub session_address: Option<String>,
}

/// How a run ended.
#[derive(Debug, Clone, PartialEq)]
pub struct Finished {
    pub reason: RunFinishReason,
    pub usage: Option<Usage>,
    pub retries_used: u32,
    /// Why the run paused, when it paused rather than ended. A paused run reports
    /// `Interrupted` as its reason, as today's loop does, and resumes as the same run.
    pub paused: Option<PauseReason>,
}

#[derive(Debug)]
pub enum LoopError {
    Session(SessionError),
    Parts(String),
    /// A case this build of the loop doesn't handle yet.
    Unsupported(&'static str),
}

impl From<SessionError> for LoopError {
    fn from(error: SessionError) -> Self {
        LoopError::Session(error)
    }
}

/// Runs `admission` to its end on the session behind `writer`.
pub async fn run<W, P, T>(
    writer: &mut W,
    parts: &mut dyn Parts,
    admission: Admission,
    seams: &Seams<'_, P, T>,
) -> Result<Finished, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    admit(writer, parts, &admission).await?;
    drive(writer, parts, &admission, seams).await
}

/// Continues the session's latest run from its last durable record, after a crash or a
/// restart. What happens next is what the operational fold says the run must do; a call that
/// may have run is never sent again.
pub async fn resume<W, P, T>(
    writer: &mut W,
    parts: &dyn Parts,
    seams: &Seams<'_, P, T>,
) -> Result<Finished, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    let run = writer
        .operations()
        .run
        .as_ref()
        .ok_or(LoopError::Unsupported("a session with no run to resume"))?;
    let admission = frozen_admission(run, parts)?;
    if let Some(reason) = run.finished {
        return Ok(finished(writer, reason));
    }
    let resolution = run.resolution();
    let resumed = resumed_record(writer, &resolution);
    writer.append(&[Record::Resumed(resumed)], false).await?;
    match resolution {
        Resolution::Resume => drive(writer, parts, &admission, seams).await,
        Resolution::Idle(reason) => finish(writer, seams, &admission.run_id, reason).await,
        Resolution::Interrupted(_) => close_interrupted(writer, seams, &admission.run_id).await,
        Resolution::ProviderHeld(_) => Err(LoopError::Unsupported("calls a provider holds")),
        Resolution::Paused(PauseReason::AuthRequired | PauseReason::Restart) => {
            drive(writer, parts, &admission, seams).await
        }
        Resolution::Paused(_) => Err(LoopError::Unsupported("a paused run")),
    }
}

/// The result a resume records for a call that was sent with no result recorded.
const OUTCOME_UNKNOWN: &str = "outcome_unknown: this tool call was sent, but no result was \
recorded before the run stopped, so it may or may not have run. It will not be sent again.";

/// The result a resume records for a planned call the crash stopped before it was sent.
const NOT_RUN_INTERRUPTED: &str =
    "tool not run: the run was interrupted before this call was dispatched";

/// Ends a run a crash left with calls that may have run: every call of the open batch gets a
/// result (unknown outcome if it was sent, not run if it wasn't), and the batch and the run's
/// end are durable together. No call is sent.
async fn close_interrupted<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
) -> Result<Finished, LoopError> {
    let now = (seams.now_ms)();
    let mut records = Vec::new();
    if let Some(batch) = run_state(writer).open_batch.as_ref() {
        // The calls still waiting for a result, in the order the model asked for them. With
        // every call answered the turn is no longer open, and only the batch needs closing.
        let open = writer.conversation().open_tool_turn();
        for block in open.iter().flat_map(|open| open.content.iter()) {
            let broca_protocol::content::ContentBlock::ToolCall(call) = block else {
                continue;
            };
            let (message, outcome) = match batch.state_of(&call.tool_call_id) {
                Some(CallState::Dispatched) => (OUTCOME_UNKNOWN, CallOutcome::Unknown),
                Some(CallState::NotSent) => (NOT_RUN_INTERRUPTED, CallOutcome::Interrupted),
                Some(CallState::Terminated) | None => continue,
                Some(CallState::ProviderHeld | CallState::HeldSuccessor) => {
                    return Err(LoopError::Unsupported("calls a provider holds"))
                }
            };
            let result = ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::error(message.to_string()),
                is_error: true,
            };
            terminal(&mut records, run_id, &batch.batch_id, result, outcome, now);
        }
        records.push(Record::BatchEnd(BatchEnd {
            run_id: run_id.into(),
            batch_id: batch.batch_id.clone(),
        }));
    }
    records.push(run_end(writer, run_id, RunFinishReason::Interrupted, now));
    append_end(writer, run_id, records).await?;
    Ok(finished(writer, RunFinishReason::Interrupted))
}

/// The calls of the run's open tool turn that were never sent: all of them when the step's
/// batch never started, else those with no intent and no outcome. `None` without an open turn.
///
/// A call with an intent and no outcome never reaches this: the fold resolves such a run as
/// interrupted, and the resume closes it without sending anything. Leaving such calls out here
/// keeps this filter right on its own.
fn unsent_calls<W: Writer>(writer: &W) -> Option<Vec<ToolCall>> {
    let open = writer.conversation().open_tool_turn()?;
    let batch = run_state(writer).open_batch.as_ref();
    Some(
        open.content
            .iter()
            .filter_map(|block| match block {
                broca_protocol::content::ContentBlock::ToolCall(call) => Some(call.clone()),
                _ => None,
            })
            .filter(|call| {
                batch.is_none_or(|batch| {
                    batch.state_of(&call.tool_call_id) == Some(CallState::NotSent)
                })
            })
            .collect(),
    )
}

/// What a resume rebuilt, what it does again, and which calls it will never send again.
/// What a resume rebuilt, what it does again, and which calls it will never send again.
///
/// While a tool batch is open, its step's tool turn isn't complete: the counted messages stop
/// before the step's assistant message, and only the batch's unsent calls are done again; the
/// next model step follows the batch and isn't counted as done again.
fn resumed_record<W: Writer>(writer: &W, resolution: &Resolution) -> Resumed {
    let run = run_state(writer);
    let conversation = writer.conversation();
    let tool_results = run.open_batch.as_ref().map_or(0, |batch| {
        (0..batch.calls.len())
            .filter(|position| batch.state(*position) == CallState::Terminated)
            .count()
    });
    let mut messages = conversation.messages.len();
    if run.open_batch.is_some() && conversation.open_tool_turn().is_none() {
        // Every call answered: the turn's assistant message and its results are placed.
        messages = messages.saturating_sub(2);
    }
    let mut redone = Vec::new();
    if *resolution == Resolution::Resume {
        let unsent = unsent_calls(writer).unwrap_or_default();
        redone.extend(unsent.into_iter().map(|call| Redo {
            step_id: None,
            tool_call_id: Some(call.tool_call_id),
        }));
        if run.open_batch.is_none() && conversation.open_tool_turn().is_none() {
            // The next model step has no durable answer: it is asked for again.
            redone.push(Redo {
                step_id: Some(run.completed_steps + 1),
                tool_call_id: None,
            });
        }
    }
    Resumed {
        run_id: run.run_id.clone(),
        resumed_from_seq: writer.next_seq().saturating_sub(1),
        messages: messages as u64,
        model_steps: run.completed_steps,
        tool_results: tool_results as u64,
        redone,
        indeterminate: run.indeterminate(),
    }
}

fn decode<T: serde::de::DeserializeOwned>(value: Option<Value>) -> Result<Option<T>, LoopError> {
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| LoopError::Parts(e.to_string()))
}

/// A run's admission as its start record froze it, read back from the fold and the setup
/// store. Its input is already in the conversation, so it is left empty.
fn frozen_admission(
    run: &broca_session_store::fold::operations::Run,
    parts: &dyn Parts,
) -> Result<Admission, LoopError> {
    let start = run.start.as_ref().ok_or(LoopError::Unsupported(
        "a run whose start isn't in the fold",
    ))?;
    let part = |kind: PartKind| -> Result<Option<Value>, LoopError> {
        start
            .setup
            .iter()
            .find(|part| part.kind == kind)
            .map(|part| parts.get(part).map_err(LoopError::Parts))
            .transpose()
    };
    let config = start
        .run_choices
        .as_ref()
        .and_then(|choices| choices.get("config"))
        .cloned()
        .unwrap_or(Value::Null);
    let member = |key: &str| config.get(key).cloned();
    Ok(Admission {
        run_id: start.run_id.clone(),
        model: start.model.clone(),
        input: Vec::new(),
        tools: decode(part(PartKind::Tools)?)?.unwrap_or_default(),
        tool_choice: decode(member("tool_choice"))?.unwrap_or_default(),
        generation: decode(member("generation"))?.unwrap_or_default(),
        response_format: decode(part(PartKind::ResponseFormat)?)?.unwrap_or_default(),
        provider_options: member("provider_options").unwrap_or(Value::Null),
        render_config: decode::<Value>(part(PartKind::RenderConfig)?)?.unwrap_or(Value::Null),
        renderer_version: start.renderer_version,
        stop_when: decode(member("stop_when"))?.unwrap_or_default(),
        on_restart: decode(start.on_restart.clone())?,
        ts_ms: start.ts_ms,
        written_at_ms: start.written_at_ms,
    })
}

/// Model steps and tool batches from the run's current state until a decision ends the run.
async fn drive<W, P, T>(
    writer: &mut W,
    parts: &dyn Parts,
    admission: &Admission,
    seams: &Seams<'_, P, T>,
) -> Result<Finished, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    // Each window run's place in the prompt and render config. A run adds no episode once
    // its input is placed, so these hold for the whole run.
    let episodes = render_episodes(writer, parts)?;
    let tools = Arc::new(admission.tools.clone());
    let manifest = ToolManifest::from_tools(admission.tools.clone());
    let stop_when = if admission.stop_when.is_empty() {
        vec![StopCondition::StepCount {
            n: DEFAULT_MAX_STEPS,
        }]
    } else {
        admission.stop_when.clone()
    };
    // A crash after every call of a batch had its result but before the batch closed: it
    // closes now.
    if let Some(batch) = run_state(writer).open_batch.as_ref() {
        let ended =
            (0..batch.calls.len()).all(|position| batch.state(position) == CallState::Terminated);
        if ended {
            let record = Record::BatchEnd(BatchEnd {
                run_id: admission.run_id.clone(),
                batch_id: batch.batch_id.clone(),
            });
            writer.append(&[record], true).await?;
        }
    }
    // A crash after a step asked for tools and before every call was sent: the calls with no
    // durable intent never left, so they are dispatched now, in the step's batch.
    if let Some(unsent) = unsent_calls(writer).filter(|unsent| !unsent.is_empty()) {
        let step_id = run_state(writer).completed_steps;
        let started = run_state(writer).open_batch.is_some();
        if batch(
            writer,
            seams,
            &admission.run_id,
            step_id,
            &unsent,
            &manifest,
            started,
        )
        .await?
        {
            return finish(
                writer,
                seams,
                &admission.run_id,
                RunFinishReason::Interrupted,
            )
            .await;
        }
    }
    loop {
        let completed_steps = run_state(writer).completed_steps;
        // Step ids count from 1 within a run, as today's loop numbers them.
        let step_id = completed_steps + 1;
        // The boundary: messages steered or delivered since the last one enter the conversation
        // here, in the same batch as either the step's start or, when a caller cancelled the run
        // now or at this boundary, the run's end. The run's state is read as the batch is built,
        // so a cancel recorded while the last step ran ends the run here.
        //
        // A stop request (the runner is shutting down) is checked after the cancel: the run
        // pauses here if its restart policy says to resume, with the steered messages left for
        // the resume; otherwise it ends interrupted.
        let run_id = admission.run_id.clone();
        let cancelled_now = seams.cancel.is_cancelled();
        let stopping = seams.stop.is_cancelled();
        let now = (seams.now_ms)();
        let mut cancelled_end = Some(run_end(writer, &run_id, RunFinishReason::Cancelled, now));
        let mut stopped_end = Some(run_end(writer, &run_id, RunFinishReason::Interrupted, now));
        if let Some(Record::RunEnd(end)) = stopped_end.as_mut() {
            end.interrupt_reason = Some("runner_shutdown".into());
        }
        let mut restart_pause = Some(Record::Pause(Pause {
            run_id: run_id.clone(),
            reason: PauseReason::Restart,
            ts_ms: Some(now),
            error: None,
            account_limit: None,
        }));
        let resumes = admission.on_restart == Some(OnRestart::Resume);
        let boundary = writer
            .append_with(
                &mut |_, operations| {
                    let run = operations.run.as_ref().expect("a run is active");
                    if !(cancelled_now || run.cancel.is_some()) && stopping && resumes {
                        return restart_pause.take().into_iter().collect();
                    }
                    let mut batch = placed_copies(operations, &run_id);
                    if cancelled_now || run.cancel.is_some() {
                        batch.extend(cancelled_end.take());
                    } else if stopping {
                        batch.extend(stopped_end.take());
                    } else {
                        batch.push(Record::StepStart(StepStart {
                            run_id: run_id.clone(),
                            step_id,
                        }));
                    }
                    batch
                },
                false,
            )
            .await?;
        match boundary.last() {
            Some(Record::RunEnd(end)) => return Ok(finished(writer, end.reason)),
            Some(Record::Pause(_)) => {
                let mut paused = finished(writer, RunFinishReason::Interrupted);
                paused.paused = Some(PauseReason::Restart);
                return Ok(paused);
            }
            _ => {}
        }
        let opts = call_options(writer, admission, &tools, &episodes, seams, step_id)?;
        let outcome = model_step(writer, seams, &admission.run_id, step_id, opts).await?;
        let step = match outcome {
            Ok(step) => step,
            Err(StepFailure::Cancelled) => {
                return finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await;
            }
            Err(StepFailure::Error(error)) => {
                // A failed sign-in pauses the run: after the credential is refreshed, it resumes
                // as the same run, asking for this step again.
                if error.class == broca_protocol::ErrorClass::Auth {
                    return pause(
                        writer,
                        seams,
                        &admission.run_id,
                        PauseReason::AuthRequired,
                        Some(*error),
                    )
                    .await;
                }
                let now = (seams.now_ms)();
                let mut record = run_end(writer, &admission.run_id, RunFinishReason::Error, now);
                if let Record::RunEnd(end) = &mut record {
                    end.error = Some(
                        serde_json::to_value(&*error)
                            .map_err(|e| LoopError::Parts(e.to_string()))?,
                    );
                }
                append_end(writer, &admission.run_id, vec![record]).await?;
                return Ok(finished(writer, RunFinishReason::Error));
            }
        };
        let calls: Vec<ToolCall> = step.message.tool_calls().cloned().collect();
        let stop = stop_satisfied(&stop_when, completed_steps + 1, &step.message).cloned();
        let limiting = limiting_stop_satisfied(&stop_when, completed_steps + 1, &step.message);
        let now = (seams.now_ms)();
        let assistant =
            Message::assistant_from(step.message.content.clone(), admission.model.clone());
        writer
            .append(
                &[
                    Record::Assistant(Assistant {
                        run_id: admission.run_id.clone(),
                        step_id,
                        provider_message_id: step.message.message_id.clone(),
                        message: assistant,
                        usage: Some(step.usage),
                        written_at_ms: Some(now),
                        last_content_at_ms: None,
                        tags_v1: None,
                        budget_finalize: None,
                        pruned: Vec::new(),
                    }),
                    Record::StepEnd(StepEnd {
                        run_id: admission.run_id.clone(),
                        step_id,
                        usage: Some(step.usage),
                        finish_reason: Some(step.finish_reason),
                        retries_used: step.retries_used,
                        service_tier_used: None,
                        speed_used: None,
                        call_count: calls.len() as u32,
                        fallback: None,
                        hooks: None,
                        budget_finalize: None,
                        total_usage: None,
                        stop_met: stop.is_some().then_some(true),
                        limiting_stop_met: limiting.then_some(true),
                    }),
                ],
                calls.is_empty(),
            )
            .await?;
        if calls.is_empty() {
            // A message steered in during the step makes the run take one more step, unless a
            // stop condition that limits the run was met. Otherwise the run ends here.
            let end = run_end(
                writer,
                &admission.run_id,
                RunFinishReason::Completed,
                (seams.now_ms)(),
            );
            let run_id = admission.run_id.clone();
            let mut end = Some(end);
            let appended = writer
                .append_with(
                    &mut |_, operations| {
                        let run = operations.run.as_ref().expect("a run is active");
                        let extends = run.natural_end.is_some_and(|end| end.extendable)
                            && run.pending_messages() > 0;
                        if extends {
                            return Vec::new();
                        }
                        let mut batch = placed_copies(operations, &run_id);
                        batch.extend(end.take());
                        batch
                    },
                    true,
                )
                .await?;
            if appended.is_empty() {
                continue;
            }
            return Ok(finished(writer, RunFinishReason::Completed));
        }
        if let Some(stop) = stop {
            return hard_stop(writer, seams, &admission.run_id, step_id, &calls, &stop).await;
        }
        if seams.stop.is_cancelled() {
            return stopped_batch(writer, seams, &admission.run_id, step_id, &calls).await;
        }
        if batch(
            writer,
            seams,
            &admission.run_id,
            step_id,
            &calls,
            &manifest,
            false,
        )
        .await?
        {
            return finish(
                writer,
                seams,
                &admission.run_id,
                RunFinishReason::Interrupted,
            )
            .await;
        }
    }
}

/// Stores the run's setup parts and appends its start and input as one durable batch.
async fn admit<W: Writer>(
    writer: &mut W,
    parts: &mut dyn Parts,
    admission: &Admission,
) -> Result<(), LoopError> {
    let mut setup = Vec::new();
    for (kind, value) in [
        (PartKind::Tools, serde_json::to_value(&admission.tools)),
        (
            PartKind::ResponseFormat,
            serde_json::to_value(&admission.response_format),
        ),
        (PartKind::RenderConfig, Ok(admission.render_config.clone())),
    ] {
        let value = value.map_err(|e| LoopError::Parts(e.to_string()))?;
        if value.is_null() {
            continue;
        }
        setup.push(parts.put(kind, &value).map_err(LoopError::Parts)?);
    }
    let tool_names = admission
        .tools
        .iter()
        .map(|tool| tool.name.clone())
        .collect();
    let mut config = serde_json::Map::new();
    config.insert("tool_choice".into(), json!(admission.tool_choice));
    config.insert("generation".into(), json!(admission.generation));
    if !admission.provider_options.is_null() {
        config.insert(
            "provider_options".into(),
            admission.provider_options.clone(),
        );
    }
    // Today's computed tag rules, starting after the transcript's highest tag (none yet in a
    // new session).
    config.insert("computed_tags".into(), json!({ "checkpoint": 0 }));
    config.insert("context_epoch".into(), json!(0));
    if !admission.stop_when.is_empty() {
        config.insert("stop_when".into(), json!(admission.stop_when));
    }
    let mut records = Vec::new();
    if writer.operations().setup_changed(&setup, &admission.model) {
        records.push(Record::SetupStub(SetupStub {
            parts: setup.clone(),
            model: admission.model.clone(),
            tool_names,
        }));
    }
    records.push(Record::RunStart(Box::new(RunStart {
        run_id: admission.run_id.clone(),
        model: admission.model.clone(),
        setup,
        renderer_version: admission.renderer_version,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: admission.ts_ms,
        written_at_ms: admission.written_at_ms,
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        auth_selection: None,
        run_choices: Some(json!({
            "config": config,
            "record": { "marker_order_version": MARKER_ORDER_VERSION },
        })),
        origin: Some(json!({ "kind": "fresh" })),
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: admission
            .on_restart
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| LoopError::Parts(e.to_string()))?,
        seeded: None,
        refusal_schedule: None,
    })));
    for message in &admission.input {
        records.push(Record::Input(Input {
            run_id: admission.run_id.clone(),
            placement: Placement::RunInput,
            message: message.clone(),
            written_at_ms: admission.written_at_ms,
            submission_id: None,
            notice: None,
            tags_v1: None,
            receipt_seq: None,
            pruned: Vec::new(),
        }));
    }
    writer.append(&records, true).await?;
    Ok(())
}

/// Every window run's place in the prompt (its first message's index, and its first user
/// message's) and the render config it ran under, as requests carry them.
fn render_episodes<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
) -> Result<Vec<EpisodeRenderInput>, LoopError> {
    let conversation = writer.conversation();
    let start = conversation.window_start();
    conversation
        .episodes
        .iter()
        .map(|episode| {
            let index = |ordinal: u64| (ordinal - start) as usize;
            Ok(EpisodeRenderInput {
                prompt_start: index(episode.start),
                first_user: episode.first_user.map(index),
                frozen_config: match &episode.render_config {
                    Some(part) => parts.get(part).map_err(LoopError::Parts)?,
                    None => Value::Null,
                },
            })
        })
        .collect()
}

/// The request for step `step_id`: the conversation as the folds hold it, under the run's
/// frozen settings, with the render inputs today's loop gives a request: each message's
/// position, its part numbers under the run's tag rules, and the run's place in the prompt.
fn call_options<W: Writer, P, T>(
    writer: &W,
    admission: &Admission,
    tools: &Arc<Vec<Tool>>,
    episodes: &[EpisodeRenderInput],
    seams: &Seams<'_, P, T>,
    step_id: u64,
) -> Result<CallOptions, LoopError> {
    let mut prompt = writer.conversation().messages.clone();
    stamp_ordinals_from(&mut prompt, 0);
    TagNumbering::for_run(
        &[],
        Some(ComputedRules {
            render: false,
            clean: false,
        }),
        Some(ComputedTagRules { checkpoint: 0 }),
    )
    .number_new(&mut prompt)
    .map_err(|_| LoopError::Unsupported("part numbers past the numbering limit"))?;
    clean_computed_into(&mut prompt);
    let mut opts = CallOptions::new(prompt);
    opts.marker_order_version = Some(MARKER_ORDER_VERSION);
    opts.render_episodes = Arc::new(episodes.to_vec().into());
    opts.tools = Arc::clone(tools);
    opts.tool_choice = Arc::new(admission.tool_choice.clone());
    opts.response_format = Arc::new(admission.response_format.clone());
    opts.generation = Arc::new(admission.generation.clone());
    opts.provider_options = Arc::new(admission.provider_options.clone());
    opts.target_model = Some(admission.model.clone());
    opts.step_context = Some(StepContext {
        run_id: admission.run_id.clone(),
        step_index: step_id,
        session_address: seams.session_address.clone(),
        credential_id: None,
        account_ref: None,
        max_output_tokens: None,
    });
    Ok(opts)
}

struct Step {
    message: AssistantMessage,
    usage: Usage,
    finish_reason: FinishReason,
    retries_used: u32,
}

/// The error a step fails with when the provider's stream ends without an answer and its final
/// `Finished`, as today's loop records it.
fn incomplete_stream() -> ProviderError {
    ProviderError {
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        class: broca_protocol::ErrorClass::ProviderUnavailable,
        message:
            "provider stream ended without an assembled assistant message and Finished control"
                .into(),
        status: None,
        retry_after_secs: None,
        provider_code: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

enum StepFailure {
    Cancelled,
    Error(Box<ProviderError>),
}

/// Runs one model call and assembles its answer. Display events go straight to the listener.
/// Asks the provider for one step. Each HTTP attempt's evidence is appended as the attempt
/// ends, without a barrier, as today's loop records it: billing and cache evidence read it.
/// The outer error is the store's; the inner one is the step's.
async fn model_step<W: Writer, P: Provider, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    opts: CallOptions,
) -> Result<Result<Step, StepFailure>, LoopError> {
    let runtime = StepRuntime {
        cancel: seams.cancel.clone(),
        deadline: None,
        seal: None,
        progress: None,
        first_attempt: 0,
        attempt_hook: None,
    };
    let mut events = seams
        .provider
        .step_with_runtime(StepId(step_id), Arc::new(opts), runtime)
        .await;
    let mut message = None;
    loop {
        let event = tokio::select! {
            event = events.recv() => event,
            _ = seams.cancel.cancelled() => return Ok(Err(StepFailure::Cancelled)),
        };
        match event {
            Some(NormalizedEvent::Display(display)) => {
                if let Some(out) = seams.out {
                    let _ = out.send(OutputEvent::Display(display)).await;
                }
            }
            Some(NormalizedEvent::Control(ProviderControl::AssistantMessage { message: m })) => {
                message = Some(m);
            }
            Some(NormalizedEvent::Control(ProviderControl::Finished {
                usage,
                finish_reason,
                retries_used,
            })) => {
                let Some(message) = message.take() else {
                    return Ok(Err(StepFailure::Error(Box::new(incomplete_stream()))));
                };
                return Ok(Ok(Step {
                    message,
                    usage,
                    finish_reason,
                    retries_used,
                }));
            }
            Some(NormalizedEvent::Control(ProviderControl::Error { error, .. })) => {
                return Ok(Err(StepFailure::Error(Box::new(error))))
            }
            Some(NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt })) => {
                writer
                    .append(&[attempt_record(run_id, step_id, attempt)?], false)
                    .await?;
            }
            None => return Ok(Err(StepFailure::Error(Box::new(incomplete_stream())))),
            Some(NormalizedEvent::Control(_)) => {}
        }
    }
}

/// Dispatches one assistant message's tool calls and records every result. Returns whether a
/// call may have run without its result coming back: the run must then stop, because feeding
/// that call's failure to the model would invite it to run the call again.
async fn batch<W, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    calls: &[ToolCall],
    manifest: &ToolManifest,
    started: bool,
) -> Result<bool, LoopError>
where
    W: Writer,
    T: ToolPlane,
{
    let batch_id = format!("{run_id}-b{step_id}");
    let ctx = BatchCtx {
        batch_id: BatchId(batch_id.clone()),
        bind: seams.bind.clone(),
        run_id: RunId(run_id.into()),
        scope: None,
        media_capture: None,
    };
    let mut records = Vec::new();
    if !started {
        records.push(Record::BatchStart(BatchStart {
            run_id: run_id.into(),
            batch_id: batch_id.clone(),
            step_id,
            planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
        }));
    }
    for call in calls {
        let args = canonical(&call.input).map_err(LoopError::Parts)?.1;
        records.push(Record::ToolIntent(ToolIntent {
            run_id: run_id.into(),
            batch_id: batch_id.clone(),
            tool_call_id: call.tool_call_id.clone(),
            tool_name: Some(call.tool_name.clone()),
            module_id: seams.tools.dispatch_target(&ctx, &call.tool_call_id),
            args_digest: args,
            args: None,
            call_key: None,
            approval_digest: None,
            dispatched_at_ms: Some((seams.now_ms)()),
            schema_pin: None,
            hooks: None,
        }));
    }
    writer.append(&records, true).await?;

    let results = match seams.tools.batch_started(&ctx, calls, manifest).await {
        Ok(()) => dispatch_all(seams, &ctx, calls, manifest).await,
        Err(_) => return Err(LoopError::Unsupported("a tool plane refusing a batch")),
    };
    let _ = seams.tools.batch_finished(&ctx).await;
    let unknown = results
        .iter()
        .any(|(_, outcome)| *outcome == CallOutcome::Unknown);
    let mut records = Vec::new();
    for (result, outcome) in results {
        terminal(
            &mut records,
            run_id,
            &batch_id,
            result,
            outcome,
            (seams.now_ms)(),
        );
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    writer.append(&records, true).await?;
    Ok(unknown)
}

/// A call's reply in the transcript and its outcome in the ledger.
fn terminal(
    records: &mut Vec<Record>,
    run_id: &str,
    batch_id: &str,
    result: ToolResult,
    outcome: CallOutcome,
    now: u64,
) {
    let digest = Digest(Sha256::digest(result.encode()).into());
    let tool_call_id = result.tool_call_id.clone();
    let is_error = result.is_error;
    records.push(Record::ToolReply(ToolReply {
        run_id: run_id.into(),
        batch_id: Some(batch_id.into()),
        result,
        finished_at_ms: Some(now),
        tags_v1: None,
    }));
    records.push(Record::ToolOutcome(ToolOutcome {
        run_id: run_id.into(),
        batch_id: batch_id.into(),
        tool_call_id,
        outcome,
        result_digest: digest,
        is_error,
        reason: None,
        finished_at_ms: Some(now),
        detail: None,
        hooks: None,
    }));
}

/// A stop condition held on a step that asked for tools. The calls are never dispatched: each
/// gets a result saying so, and the batch and the run's end are durable together, so a crash
/// either loses both (the resumed run takes the step again) or keeps both (the run is over).
async fn hard_stop<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    calls: &[ToolCall],
    stop: &StopCondition,
) -> Result<Finished, LoopError> {
    let (outcome, message, reason) = match stop {
        StopCondition::StepCount { .. } => (
            CallOutcome::MaxSteps,
            "tool not run: max steps reached before dispatch",
            RunFinishReason::MaxSteps,
        ),
        _ => (
            CallOutcome::Interrupted,
            "tool not run: interrupted before dispatch",
            RunFinishReason::Completed,
        ),
    };
    let batch_id = format!("{run_id}-b{step_id}");
    let mut records = vec![Record::BatchStart(BatchStart {
        run_id: run_id.into(),
        batch_id: batch_id.clone(),
        step_id,
        planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
    })];
    let now = (seams.now_ms)();
    for call in calls {
        let result = ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::error(message.to_string()),
            is_error: true,
        };
        terminal(&mut records, run_id, &batch_id, result, outcome, now);
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    records.push(run_end(writer, run_id, reason, now));
    append_end(writer, run_id, records).await?;
    Ok(finished(writer, reason))
}

/// The runner asked the run to stop before its batch was dispatched. No call is sent: each
/// gets a result saying so, the tool plane is never told the batch started, and the run ends
/// interrupted whatever its restart policy, as today's loop does. The batch and the run's end
/// are two barriers, as there.
async fn stopped_batch<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    calls: &[ToolCall],
) -> Result<Finished, LoopError> {
    let batch_id = format!("{run_id}-b{step_id}");
    let now = (seams.now_ms)();
    let mut records = vec![Record::BatchStart(BatchStart {
        run_id: run_id.into(),
        batch_id: batch_id.clone(),
        step_id,
        planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
    })];
    for call in calls {
        let result = ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::error(NOT_DISPATCHED.to_string()),
            is_error: true,
        };
        terminal(
            &mut records,
            run_id,
            &batch_id,
            result,
            CallOutcome::Interrupted,
            now,
        );
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    writer.append(&records, true).await?;
    let mut end = run_end(writer, run_id, RunFinishReason::Interrupted, now);
    if let Record::RunEnd(end) = &mut end {
        end.interrupt_reason = Some("runner_shutdown".into());
    }
    append_end(writer, run_id, vec![end]).await?;
    Ok(finished(writer, RunFinishReason::Interrupted))
}

/// The result a call gets when the run stopped before the call was dispatched.
const NOT_DISPATCHED: &str =
    "tool not run: the run was interrupted before this call was dispatched";

/// Dispatches every call, in parallel unless one of them must run alone, and returns the
/// results in the calls' order.
async fn dispatch_all<P, T: ToolPlane>(
    seams: &Seams<'_, P, T>,
    ctx: &BatchCtx,
    calls: &[ToolCall],
    manifest: &ToolManifest,
) -> Vec<(ToolResult, CallOutcome)> {
    let sequential = calls.iter().any(|call| {
        manifest
            .get(&call.tool_name)
            .is_some_and(|tool| tool.execution_mode == ExecutionMode::Sequential)
    });
    let mut results = Vec::with_capacity(calls.len());
    if sequential {
        for call in calls {
            results.push(resolve(call, seams.tools.dispatch(ctx, call).await));
        }
    } else {
        let all = calls
            .iter()
            .map(|call| async move { resolve(call, seams.tools.dispatch(ctx, call).await) });
        results = futures::future::join_all(all).await;
    }
    results
}

/// A call's result and outcome from what the tool plane said:
/// - a result is the tool's own answer;
/// - never sent is safe: the model gets an error result and may try again;
/// - sent with no result may have had its effect: the model gets an error result, the outcome
///   is unknown, and the run stops.
fn resolve(call: &ToolCall, result: DispatchResult) -> (ToolResult, CallOutcome) {
    let error = |message: String| ToolResult {
        tool_call_id: call.tool_call_id.clone(),
        output: ToolOutput::error(message),
        is_error: true,
    };
    match result {
        DispatchResult::Completed(result) => (result, CallOutcome::Completed),
        DispatchResult::NotSent { reason } => (
            error(format!("tool not dispatched: {reason}")),
            CallOutcome::Completed,
        ),
        DispatchResult::SentButOutcomeUnknown { reason } => (
            error(format!(
                "tool outcome unknown (interrupted for review): {reason}"
            )),
            CallOutcome::Unknown,
        ),
        // A drained scope route ends the run differently; not handled by this build.
        DispatchResult::ScopeDrained { reason, .. } => (
            error(format!(
                "tool outcome unknown (interrupted for review): {reason}"
            )),
            CallOutcome::Unknown,
        ),
    }
}

/// The run's state as the operational fold holds it. Admission appended its start, so it
/// exists for the whole run.
fn run_state<W: Writer>(writer: &W) -> &broca_session_store::fold::operations::Run {
    writer
        .operations()
        .run
        .as_ref()
        .expect("the run's start is appended before any step")
}

/// The run's end record. Its usage is the run's total, present once a step completed.
fn run_end<W: Writer>(writer: &W, run_id: &str, reason: RunFinishReason, now: u64) -> Record {
    let run = run_state(writer);
    Record::RunEnd(RunEnd {
        run_id: run_id.into(),
        reason,
        usage: (run.completed_steps > 0).then_some(run.usage),
        ts_ms: Some(now),
        retries_used: run.retries_used,
        interrupt_reason: None,
        error: None,
    })
}

/// How the run ended, as its end record states it.
fn finished<W: Writer>(writer: &W, reason: RunFinishReason) -> Finished {
    let run = run_state(writer);
    Finished {
        reason,
        usage: (run.completed_steps > 0).then_some(run.usage),
        retries_used: run.retries_used,
        paused: None,
    }
}

/// Pauses the run at a boundary, durably, before the run reports it. A caller's cancel wins
/// until the pause is durable: the run then ends cancelled instead, so nobody is left with a
/// paused run they already abandoned. Messages steered in stay unplaced, for the resume.
async fn pause<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    reason: PauseReason,
    error: Option<ProviderError>,
) -> Result<Finished, LoopError> {
    let error = error
        .map(|error| serde_json::to_value(error).map_err(|e| LoopError::Parts(e.to_string())))
        .transpose()?;
    let cancelled_now = seams.cancel.is_cancelled();
    let mut cancelled_end = Some(run_end(
        writer,
        run_id,
        RunFinishReason::Cancelled,
        (seams.now_ms)(),
    ));
    let mut pause = Some(Record::Pause(Pause {
        run_id: run_id.into(),
        reason,
        ts_ms: Some((seams.now_ms)()),
        error,
        account_limit: None,
    }));
    let owned_run_id = run_id.to_string();
    let batch = writer
        .append_with(
            &mut |_, operations| {
                let run = operations.run.as_ref().expect("a run is active");
                if cancelled_now || run.cancel.is_some() {
                    let mut batch = placed_copies(operations, &owned_run_id);
                    batch.extend(cancelled_end.take());
                    batch
                } else {
                    pause.take().into_iter().collect()
                }
            },
            true,
        )
        .await?;
    if matches!(batch.last(), Some(Record::RunEnd(_))) {
        return Ok(finished(writer, RunFinishReason::Cancelled));
    }
    let mut paused = finished(writer, RunFinishReason::Interrupted);
    paused.paused = Some(reason);
    Ok(paused)
}

/// Appends the run's end, durable before the run reports it.
async fn finish<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    reason: RunFinishReason,
) -> Result<Finished, LoopError> {
    let record = run_end(writer, run_id, reason, (seams.now_ms)());
    append_end(writer, run_id, vec![record]).await?;
    Ok(finished(writer, reason))
}

/// Appends `records`, which end with the run's end, as one durable batch. Messages steered or
/// delivered since the last boundary enter the conversation just before the end: the run ends
/// with them in its conversation, as today's loop leaves them.
async fn append_end<W: Writer>(
    writer: &mut W,
    run_id: &str,
    records: Vec<Record>,
) -> Result<(), LoopError> {
    let run_id = run_id.to_string();
    let mut records = Some(records);
    writer
        .append_with(
            &mut |_, operations| {
                let mut batch = records.take().expect("one batch");
                let end = batch.pop().expect("the batch ends with the run's end");
                batch.extend(placed_copies(operations, &run_id));
                batch.push(end);
                batch
            },
            true,
        )
        .await?;
    Ok(())
}

/// Copies of the messages steered or delivered since the last boundary, as transcript
/// entries. Each names the seq of the record that accepted it.
fn placed_copies(operations: &Operations, run_id: &str) -> Vec<Record> {
    let Some(run) = operations.run.as_ref() else {
        return Vec::new();
    };
    run.pending_steers
        .iter()
        .flat_map(|pending| {
            pending
                .messages
                .iter()
                .enumerate()
                .map(move |(index, message)| {
                    Record::Input(Input {
                        run_id: run_id.to_string(),
                        placement: pending.placement,
                        message: message.clone(),
                        written_at_ms: pending.written_at_ms,
                        submission_id: pending.submission_id.clone(),
                        notice: None,
                        tags_v1: pending.tags_at(index),
                        receipt_seq: pending.seq,
                        pruned: Vec::new(),
                    })
                })
        })
        .collect()
}

/// One HTTP attempt's record, from the provider's evidence. Billing and the folds read the typed
/// fields; every other field the provider measured is kept as diagnostics telemetry under the
/// name it has on the provider's evidence. Billing details are absent: no attempt in this loop
/// is billed through a reservation yet.
fn attempt_record(run_id: &str, step_id: u64, attempt: ModelAttempt) -> Result<Record, LoopError> {
    let invalid = |what: &str| LoopError::Parts(format!("attempt evidence: {what}"));
    let mut value = serde_json::to_value(&attempt).map_err(|e| invalid(&e.to_string()))?;
    let fields = value
        .as_object_mut()
        .ok_or_else(|| invalid("not an object"))?;
    let mut take = |key: &str| fields.remove(key);
    let digest = |value: Option<Value>, key: &str| -> Result<Digest, LoopError> {
        let text = value.as_ref().and_then(Value::as_str).unwrap_or_default();
        hex_digest(text).ok_or_else(|| invalid(&format!("{key} is not a SHA-256 hex digest")))
    };
    let outcome = take("outcome")
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| invalid("outcome"))?;
    take("attempt");
    take("request_body_len");
    take("response_body_len");
    take("http_status");
    take("ts_ms");
    take("sent_at_ms");
    take("first_content_at_ms");
    take("last_content_at_ms");
    take("transport");
    take("served_account_ref");
    let request_body_sha256 = digest(take("request_body_sha256"), "request_body_sha256")?;
    let response_body_sha256 = digest(take("response_body_sha256"), "response_body_sha256")?;
    let telemetry = (!fields.is_empty()).then(|| Value::Object(std::mem::take(fields)));
    Ok(Record::Attempt(Attempt {
        run_id: run_id.into(),
        step_id,
        attempt: attempt.attempt,
        attempt_id: None,
        outcome,
        http_status: attempt.http_status.map(u32::from),
        request_body_len: attempt.request_body_len,
        request_body_sha256,
        response_body_len: attempt.response_body_len,
        response_body_sha256,
        ts_ms: attempt.ts_ms,
        sent_at_ms: attempt.sent_at_ms,
        first_content_at_ms: attempt.content_progress.first_content_at_ms,
        last_content_at_ms: attempt.content_progress.last_content_at_ms,
        transport: attempt.transport.clone(),
        served_account_ref: attempt.served_account_ref.clone(),
        billing: None,
        telemetry,
    }))
}

/// The digest a 64-character hex string spells, or `None`.
fn hex_digest(text: &str) -> Option<Digest> {
    if text.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (index, pair) in text.as_bytes().chunks(2).enumerate() {
        out[index] = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(Digest(out))
}
