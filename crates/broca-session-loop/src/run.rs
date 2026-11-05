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

use broca_core::capability::ApprovalPolicy;
use broca_core::capability::ToolManifest;
use broca_core::delivery_order::{appended_text, choose_mode, Boundary};
use broca_core::provider::{Provider, StepRuntime};
use broca_core::run::{limiting_stop_satisfied, stop_satisfied, OutputEvent};
use broca_core::tags::TagNumbering;
use broca_core::tool_plane::{
    BatchCtx, DispatchKey, DispatchRegistration, DispatchResult, ProviderHeldLifecycle,
    ProviderHeldRecorder, ProviderHeldReply, ToolOpenGuard, ToolPlane,
};
use broca_protocol::approval::ApprovalResponse;
use broca_protocol::caller::{PauseReason, RunFinishReason};
use broca_protocol::content::{
    stamp_ordinals_from, AssistantMessage, Message, Role, ToolCall, ToolOutput, ToolResult,
};
use broca_protocol::error::ProviderError;
use broca_protocol::provider::{ModelAttempt, NormalizedEvent, ProviderControl};
use broca_protocol::request::EpisodeRenderInput;
use broca_protocol::request::{
    CallOptions, ExecutionMode, ModelHandle, OnRestart, ResponseFormat, StepContext, StopCondition,
    Tool, ToolChoice,
};
use broca_protocol::tags::{clean_computed_into, ComputedRules};
use broca_protocol::{
    BatchId, BindIdentity, FinishReason, GenerationConfig, RunId, StepId, ToolCallId, Usage,
};
use broca_protocol::{DeliveryMode, DeliveryOrder};
use broca_session_store::codec::{Digest, Message as _};
use broca_session_store::fold::operations::{CallState, Operations, Resolution};
use broca_session_store::record::{
    Assistant, Attempt, BatchEnd, BatchStart, CallOutcome, Delivery, Input, PartKind, Pause,
    Placement, Record, Redo, Resumed, RunEnd, RunStart, SendEnded, SetupStub, StepEnd, StepStart,
    ToolHeld, ToolIntent, ToolOutcome, ToolReply,
};
use broca_session_store::session::SessionError;
use broca_wal::ComputedTagRules;
use broca_wal::NoDispatchReason;
use futures::channel::oneshot;
use futures::StreamExt as _;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::entries::{Placed, Tags};
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
    /// The session's frozen plan manifest, for a session admitted with a fetch plan: the tools
    /// its providers answered with, its system text and the hooks it declares.
    pub manifest: Option<std::sync::Arc<broca_wal::PlanManifest>>,
    /// For a session on the current compaction contract, the system text every request renders
    /// above the conversation, which is never part of its transcript: the plan's system text for
    /// a planned run, otherwise the system messages the caller sent, or the previous run's when
    /// it sent none. [`run`] takes it out of `input`; a caller leaves it empty.
    pub global_system: Vec<Message>,
    /// For a session's first run: the compaction plan version the session starts under, so it
    /// records and renders under the current compaction contract; `None` keeps the established
    /// layout. Today's actor asks for it when the plan names a compaction provider. A later run
    /// keeps its session's layout whatever this says.
    pub compaction_plan_version: Option<u32>,
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
    /// The late-result runtime's waiting slots for calls a provider holds. Without it a held
    /// call is never answered, as in today's loop, and its run pauses.
    pub held: Option<&'a dyn ProviderHeldLifecycle>,
    /// Decides each valid call before it is sent: allow it, narrow its input, or deny it.
    pub approval: &'a dyn ApprovalPolicy,
    /// Calls the hooks a frozen plan declares. A run whose plan declares hooks refuses to start
    /// or resume without it, as today's loop does.
    pub hook_plane: Option<Arc<dyn broca_core::hooks::HookPlane>>,
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
    /// The tool plane refused something the run can't go on without, such as registering a
    /// batch's call keys for late results.
    Tools(String),
    /// A run sent system text other than the session's; nothing was written.
    SystemPromptChanged,
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
    // A session keeps the layout its first run started under; a new run on it freezes the view
    // library's current plan version, as today's loop does.
    let mut admission = admission;
    let contract = match writer
        .operations()
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
    {
        Some(start) => start.compaction_plan_version.is_some(),
        None => admission.compaction_plan_version.is_some(),
    };
    admission.compaction_plan_version = contract.then_some(cortexkit_compaction_view::PLAN_VERSION);
    if contract {
        let mut input = std::mem::take(&mut admission.input);
        admission.global_system = global_system(writer, parts, &mut input, &admission)?;
        admission.input = input;
    }
    let mut placed = Placed {
        facts: contract
            .then(|| Tags::for_run(admission.manifest.as_deref(), &admission.render_config)),
        inner: writer,
        view: None,
    };
    admit(&mut placed, parts, &admission).await?;
    drive(&mut placed, parts, &admission, seams).await
}

/// Takes the system messages out of a run's input and decides the system text its requests
/// render, as today's loop does for a session on the current compaction contract: a planned
/// run renders its plan's system text and ignores what it was sent; any other run renders what
/// it was sent, or the previous run's text when it sent none, and is refused when it sent text
/// that differs from the previous run's.
fn global_system<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    input: &mut Vec<Message>,
    admission: &Admission,
) -> Result<Vec<Message>, LoopError> {
    let supplied = broca_core::compaction_v2::systems::take_globals(input);
    if let Some(manifest) = &admission.manifest {
        return Ok(broca_core::compaction_v2::systems::from_manifest(manifest));
    }
    let previous = match writer
        .operations()
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
        .and_then(|start| {
            start
                .setup
                .iter()
                .find(|part| part.kind == PartKind::GlobalSystem)
        }) {
        Some(part) => decode::<Vec<Message>>(Some(parts.get(part).map_err(LoopError::Parts)?))?,
        None => None,
    };
    match previous {
        Some(previous) if supplied.is_empty() => Ok(previous),
        Some(previous) if previous != supplied => Err(LoopError::SystemPromptChanged),
        _ => Ok(supplied),
    }
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
    let mut placed = Placed {
        facts: admission
            .compaction_plan_version
            .map(|_| Tags::for_run(admission.manifest.as_deref(), &admission.render_config)),
        inner: writer,
        view: None,
    };
    let writer = &mut placed;
    let resumed = resumed_record(writer, &resolution);
    writer.append(&[Record::Resumed(resumed)], false).await?;
    match resolution {
        Resolution::Resume => drive(writer, parts, &admission, seams).await,
        Resolution::Idle(reason) => finish(writer, seams, &admission.run_id, reason).await,
        Resolution::Interrupted(_) => close_interrupted(writer, seams, &admission.run_id).await,
        // A held call's late result arrived: the run goes on from its open batch, waiting on
        // each held call again rather than sending it.
        Resolution::ProviderHeld(_) => drive(writer, parts, &admission, seams).await,
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
/// result (unknown outcome if it was sent, not run if it wasn't, the provider-held close result
/// if a provider holds it), and the batch and the run's end are durable together. No call is
/// sent. Each provider still holding a call is asked to withdraw it once the end is durable.
async fn close_interrupted<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
) -> Result<Finished, LoopError> {
    let now = (seams.now_ms)();
    let mut records = Vec::new();
    let mut withdrawn = Vec::new();
    if let Some(batch) = run_state(writer).open_batch.as_ref() {
        // The calls still waiting for a result, in the order the model asked for them. With
        // every call answered the turn is no longer open, and only the batch needs closing.
        let open = writer.conversation().open_tool_turn();
        for block in open.iter().flat_map(|open| open.content.iter()) {
            let broca_protocol::content::ContentBlock::ToolCall(call) = block else {
                continue;
            };
            let id = &call.tool_call_id;
            let (result, outcome, reason) = match batch.state_of(id) {
                Some(CallState::Dispatched) => (
                    error_result(id, OUTCOME_UNKNOWN),
                    CallOutcome::Unknown,
                    None,
                ),
                Some(CallState::NotSent | CallState::HeldSuccessor) => (
                    error_result(id, NOT_RUN_INTERRUPTED),
                    CallOutcome::Interrupted,
                    None,
                ),
                Some(CallState::ProviderHeld) => {
                    let (result, _, code) = broca_core::resume::provider_held_close_result(
                        id,
                        RunFinishReason::Interrupted,
                    );
                    let position = (0..batch.calls.len())
                        .find(|position| batch.id(*position) == id)
                        .expect("a held call is planned");
                    if let Some(held) = batch.held_reply(position) {
                        withdrawn.push(ProviderHeldReply {
                            tool_call_id: held.tool_call_id.clone(),
                            call_key: held.call_key.clone(),
                            module: held.module.clone(),
                            reason: held.reason.clone(),
                            detail_digest: held.detail_digest.clone(),
                        });
                    }
                    (result, CallOutcome::Interrupted, Some(code.to_string()))
                }
                Some(CallState::Terminated) | None => continue,
            };
            terminal_with_reason(
                &mut records,
                run_id,
                &batch.batch_id,
                result,
                outcome,
                reason,
                Some(now),
            );
        }
        records.push(Record::BatchEnd(BatchEnd {
            run_id: run_id.into(),
            batch_id: batch.batch_id.clone(),
        }));
    }
    records.push(run_end(writer, run_id, RunFinishReason::Interrupted, now));
    append_end(writer, run_id, records).await?;
    withdraw(seams, &withdrawn).await;
    Ok(finished(writer, RunFinishReason::Interrupted))
}

/// An error result for call `id` saying `message`.
fn error_result(id: &ToolCallId, message: &str) -> ToolResult {
    ToolResult {
        tool_call_id: id.clone(),
        output: ToolOutput::error(message.to_string()),
        is_error: true,
    }
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
                    matches!(
                        batch.state_of(&call.tool_call_id),
                        Some(
                            CallState::NotSent | CallState::ProviderHeld | CallState::HeldSuccessor
                        )
                    )
                })
            })
            .collect(),
    )
}

/// Every call the open tool turn's assistant message asked for.
fn planned_calls<W: Writer>(writer: &W) -> Vec<ToolCall> {
    writer
        .conversation()
        .open_tool_turn()
        .map(|open| {
            open.content
                .iter()
                .filter_map(|block| match block {
                    broca_protocol::content::ContentBlock::ToolCall(call) => Some(call.clone()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The open batch's calls that already have intents and are still to be answered: a call a
/// provider holds, with its held reply, and a call queued behind one, never sent.
fn known_calls<W: Writer>(writer: &W) -> Vec<Known> {
    let Some(batch) = run_state(writer).open_batch.as_ref() else {
        return Vec::new();
    };
    (0..batch.calls.len())
        .filter(|position| {
            matches!(
                batch.state(*position),
                CallState::ProviderHeld | CallState::HeldSuccessor
            )
        })
        .map(|position| Known {
            id: batch.id(position).clone(),
            key: batch.key(position).map(str::to_owned),
            args: batch.args(position).cloned(),
            held: batch.held_reply(position).map(|held| ProviderHeldReply {
                tool_call_id: held.tool_call_id.clone(),
                call_key: held.call_key.clone(),
                module: held.module.clone(),
                reason: held.reason.clone(),
                detail_digest: held.detail_digest.clone(),
            }),
        })
        .collect()
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

/// Stores a plan manifest's shared members as setup parts, in a fixed order: the plan, each
/// provider's answer, the manifest's tools, its system text and its hook declarations. Returns
/// what remains of the manifest, or `None` when nothing does.
fn manifest_parts(
    manifest: &broca_wal::PlanManifest,
    parts: &mut dyn Parts,
    setup: &mut Vec<broca_session_store::record::PartRef>,
) -> Result<Option<Value>, LoopError> {
    let Value::Object(mut members) =
        serde_json::to_value(manifest).map_err(|e| LoopError::Parts(e.to_string()))?
    else {
        return Err(LoopError::Parts("a manifest isn't an object".into()));
    };
    let mut put = |kind: PartKind, value: &Value| -> Result<(), LoopError> {
        setup.push(parts.put(kind, value).map_err(LoopError::Parts)?);
        Ok(())
    };
    if let Some(plan) = members.remove("plan") {
        put(PartKind::Plan, &plan)?;
    }
    if let Some(Value::Array(fetches)) = members.remove("fetches") {
        for fetch in &fetches {
            put(PartKind::ProviderAnswer, fetch)?;
        }
    }
    for (key, kind) in [
        ("tools", PartKind::Tools),
        ("system_text", PartKind::SystemText),
        ("declarations", PartKind::Declarations),
    ] {
        if let Some(value) = members.remove(key) {
            put(kind, &value)?;
        }
    }
    Ok((!members.is_empty()).then_some(Value::Object(members)))
}

/// The plan manifest a run's start froze, rebuilt from the setup parts from its plan on and the
/// remainder its choices keep. `None` for a run admitted without a plan.
fn frozen_manifest(
    start: &broca_session_store::record::RunStart,
    parts: &dyn Parts,
) -> Result<Option<std::sync::Arc<broca_wal::PlanManifest>>, LoopError> {
    let Some(first) = start
        .setup
        .iter()
        .position(|part| part.kind == PartKind::Plan)
    else {
        return Ok(None);
    };
    let mut members = match start
        .run_choices
        .as_ref()
        .and_then(|choices| choices.get("manifest"))
    {
        Some(Value::Object(rest)) => rest.clone(),
        _ => serde_json::Map::new(),
    };
    let mut fetches = Vec::new();
    for part in &start.setup[first..] {
        let value = parts.get(part).map_err(LoopError::Parts)?;
        match part.kind {
            PartKind::Plan => {
                members.insert("plan".into(), value);
            }
            PartKind::ProviderAnswer => fetches.push(value),
            PartKind::Tools => {
                members.insert("tools".into(), value);
            }
            PartKind::SystemText => {
                members.insert("system_text".into(), value);
            }
            PartKind::Declarations => {
                members.insert("declarations".into(), value);
            }
            _ => {}
        }
    }
    members.insert("fetches".into(), Value::Array(fetches));
    serde_json::from_value(Value::Object(members))
        .map(|manifest| Some(std::sync::Arc::new(manifest)))
        .map_err(|e| LoopError::Parts(format!("the frozen manifest: {e}")))
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
    // An unfinished run on the current compaction contract renders exactly as it started, so it
    // resumes only under the plan version it froze.
    if let (Some(version), None) = (start.compaction_plan_version, run.finished) {
        broca_core::compaction_v2::conditions::check_plan_version(version)
            .map_err(LoopError::Parts)?;
    }
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
        global_system: match (
            start.compaction_plan_version,
            frozen_manifest(start, parts)?,
        ) {
            (None, _) => Vec::new(),
            (Some(_), Some(manifest)) => {
                broca_core::compaction_v2::systems::from_manifest(&manifest)
            }
            (Some(_), None) => decode(part(PartKind::GlobalSystem)?)?.unwrap_or_default(),
        },
        compaction_plan_version: start.compaction_plan_version,
        manifest: frozen_manifest(start, parts)?,
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
    let hook_plan = hook_plan(admission, seams)?;
    let hooks = hook_plan
        .as_ref()
        .map(|plan| broca_core::hooks::HookRuntime {
            plan,
            plane: seams.hook_plane.as_ref().expect("checked with the plan"),
            bind: &seams.bind,
            scope: None,
            cancel: Some(&seams.cancel),
            stop: Some(&seams.stop),
            seal: None,
        });
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
        let known = known_calls(writer);
        let planned = planned_calls(writer);
        match batch(
            writer,
            seams,
            &admission.run_id,
            step_id,
            &planned,
            &unsent,
            &known,
            &manifest,
            hooks.as_ref(),
            started,
        )
        .await?
        {
            BatchOutcome::Closed { unknown: false } => {}
            BatchOutcome::Closed { unknown: true } => {
                return finish(
                    writer,
                    seams,
                    &admission.run_id,
                    RunFinishReason::Interrupted,
                )
                .await;
            }
            BatchOutcome::Held(finished) => return Ok(finished),
            BatchOutcome::Cancelled(closed) => {
                let finished =
                    finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await?;
                withdraw(seams, &closed).await;
                return Ok(finished);
            }
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
        // Queued sends with a delivery order are read before the batch is built, since their
        // records may be sealed. One queued after this read waits for the next boundary.
        let ordered = ordered_sends(writer).await?;
        let boundary = writer
            .append_with(
                &mut |_, operations, first_seq| {
                    let run = operations.run.as_ref().expect("a run is active");
                    if !(cancelled_now || run.cancel.is_some()) && stopping && resumes {
                        return restart_pause.take().into_iter().collect();
                    }
                    if cancelled_now || run.cancel.is_some() {
                        let mut batch = placed_copies(operations, &run_id);
                        batch.extend(cancelled_end.take());
                        return batch;
                    }
                    if stopping {
                        let mut batch = placed_copies(operations, &run_id);
                        batch.extend(stopped_end.take());
                        return batch;
                    }
                    // A step starts: ordered sends whose first possible mode here is `steer`
                    // are delivered, and expired ones end, in queue order. Their records come
                    // first, then every waiting message's copy (steers before deliveries), then
                    // the step's start.
                    let mut batch = Vec::new();
                    let mut delivered = Vec::new();
                    for pending in operations.queue.iter().filter(|pending| pending.ordered) {
                        let Some(send) = ordered.get(&pending.submission_id) else {
                            continue;
                        };
                        if send.order.expired_at(now) {
                            batch.push(Record::SendEnded(SendEnded {
                                submission_id: pending.submission_id.clone(),
                                reason: Some(json!({"kind": "expired"})),
                            }));
                            continue;
                        }
                        let has_text = appended_text(&send.input).is_some();
                        if choose_mode(&send.order, Boundary::STEP, has_text)
                            != Some(DeliveryMode::Steer)
                        {
                            continue;
                        }
                        let seq = first_seq + batch.len() as u64;
                        delivered.extend(send.input.iter().map(|message| {
                            Record::Input(Input {
                                run_id: run_id.clone(),
                                placement: Placement::Delivered,
                                message: message.clone(),
                                written_at_ms: send.written_at_ms,
                                submission_id: Some(pending.submission_id.clone()),
                                notice: None,
                                tags_v1: None,
                                receipt_seq: Some(seq),
                                pruned: Vec::new(),
                            })
                        }));
                        batch.push(Record::Delivery(Delivery {
                            submission_id: pending.submission_id.clone(),
                            run_id: run_id.clone(),
                            mode: "steer".into(),
                            input: send.input.clone(),
                            tool_call_id: None,
                            appended_text: None,
                            written_at_ms: send.written_at_ms,
                            tags_v1: None,
                            input_listed: Some(true),
                        }));
                    }
                    batch.extend(placed_copies(operations, &run_id));
                    batch.extend(delivered);
                    batch.push(Record::StepStart(StepStart {
                        run_id: run_id.clone(),
                        step_id,
                    }));
                    batch
                },
                false,
            )
            .await?;
        match decision(&boundary) {
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
        // The post-assistant hooks edit the answer's text before anything decides on it, so
        // the stop conditions, the record and the next request all see the edited answer.
        let mut step = step;
        let step_hooks = match hooks.as_ref() {
            Some(runtime) => post_assistant(runtime, step_id, &mut step.message).await,
            None => None,
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
                        hooks: step_hooks,
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
                    &mut |_, operations, _| {
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
        // A stop, or a caller's cancel that arrived after the model's answer, refuses the batch
        // before any call is gated, so no pre-tool hook runs and the plane is never told.
        if seams.stop.is_cancelled() || seams.cancel.is_cancelled() {
            let cancelled = seams.cancel.is_cancelled();
            return stopped_batch(writer, seams, &admission.run_id, step_id, &calls, cancelled)
                .await;
        }
        match batch(
            writer,
            seams,
            &admission.run_id,
            step_id,
            &calls,
            &calls,
            &[],
            &manifest,
            hooks.as_ref(),
            false,
        )
        .await?
        {
            BatchOutcome::Closed { unknown: false } => {}
            BatchOutcome::Closed { unknown: true } => {
                return finish(
                    writer,
                    seams,
                    &admission.run_id,
                    RunFinishReason::Interrupted,
                )
                .await;
            }
            BatchOutcome::Held(finished) => return Ok(finished),
            BatchOutcome::Cancelled(closed) => {
                let finished =
                    finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await?;
                withdraw(seams, &closed).await;
                return Ok(finished);
            }
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
    // A planned run's system text is part of its manifest; any other run's is its own part.
    let global_system = (admission.manifest.is_none() && !admission.global_system.is_empty())
        .then(|| serde_json::to_value(&admission.global_system))
        .transpose()
        .map_err(|e| LoopError::Parts(e.to_string()))?
        .unwrap_or(Value::Null);
    for (kind, value) in [
        (PartKind::Tools, serde_json::to_value(&admission.tools)),
        (
            PartKind::ResponseFormat,
            serde_json::to_value(&admission.response_format),
        ),
        (PartKind::GlobalSystem, Ok(global_system)),
        (PartKind::RenderConfig, Ok(admission.render_config.clone())),
    ] {
        let value = value.map_err(|e| LoopError::Parts(e.to_string()))?;
        if value.is_null() {
            continue;
        }
        setup.push(parts.put(kind, &value).map_err(LoopError::Parts)?);
    }
    // A manifest's shared members are setup parts after the run config's, in the order the
    // resume reads them back; what remains of it stays with the run's choices.
    let manifest_rest = match &admission.manifest {
        Some(manifest) => manifest_parts(manifest, parts, &mut setup)?,
        None => None,
    };
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
    let mut choices = serde_json::Map::new();
    choices.insert("config".into(), Value::Object(config));
    if let Some(rest) = manifest_rest {
        choices.insert("manifest".into(), rest);
    }
    choices.insert(
        "record".into(),
        json!({ "marker_order_version": MARKER_ORDER_VERSION }),
    );
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
        compaction_plan_version: admission.compaction_plan_version,
        auth_selection: None,
        run_choices: Some(Value::Object(choices)),
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
    repetition_guidance(&mut prompt, episodes);
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
    // A session on the current compaction contract renders its numbers from the entries below;
    // one on the established layout renders them in place.
    let contract = admission.compaction_plan_version.is_some();
    if !contract {
        clean_computed_into(&mut prompt);
    }
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
    if !contract {
        return Ok(opts);
    }
    // On the current compaction contract a request renders from the entries, as today's loop
    // renders one: text delivered with a tool result is added to the result's copy, and the
    // system text goes above the conversation.
    let conversation = writer.conversation();
    let entries = broca_session_store::session::window_entries(conversation, 1, usize::MAX)
        .iter()
        .map(crate::entries::project)
        .collect::<Result<Vec<_>, _>>()
        .map_err(LoopError::Parts)?;
    let prompt = broca_core::compaction_v2::render::deliveries_only(&opts.prompt, &entries)
        .map_err(LoopError::Parts)?;
    let opts = broca_core::compaction_v2::render::options(&opts, prompt);
    Ok(
        broca_core::compaction_v2::systems::view(&opts, &admission.global_system)
            .map_err(LoopError::Parts)?
            .unwrap_or(opts),
    )
}

/// Adds today's repetition guidance to the copies of tool results the request sends, never to
/// what's stored: each run's model steps (from its first answer up to the next run's input) are
/// folded with fresh counters, as today's loop counts within one run.
fn repetition_guidance(prompt: &mut [Message], episodes: &[EpisodeRenderInput]) {
    for (index, episode) in episodes.iter().enumerate() {
        let end = episodes
            .get(index + 1)
            .map_or(prompt.len(), |next| next.prompt_start)
            .min(prompt.len());
        let first_step = (episode.prompt_start..end)
            .find(|position| prompt[*position].role == broca_protocol::content::Role::Assistant);
        if let Some(first_step) = first_step {
            broca_core::run::restore_repetition_guidance(&mut prompt[first_step..end]);
        }
    }
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

/// A call of a batch that already has an intent, from before the run paused or crashed: it is
/// sent with the key its intent recorded, and a call a provider holds is waited on again, never
/// sent again.
pub(crate) struct Known {
    pub id: ToolCallId,
    pub key: Option<String>,
    pub held: Option<ProviderHeldReply>,
    /// The input its intent recorded, when an approval policy narrowed it; sent instead of the
    /// model's.
    pub args: Option<serde_json::Value>,
}

/// How a tool batch ended.
enum BatchOutcome {
    /// Every call has its outcome; `unknown` when one may have run without its result coming
    /// back, which stops the run, because feeding that call's failure to the model would invite
    /// it to run the call again.
    Closed { unknown: bool },
    /// Only calls a provider holds were still waiting, so the run paused for their late
    /// results.
    Held(Finished),
    /// The caller cancelled while a provider held calls: they were closed and the run must end
    /// cancelled, then ask each provider to withdraw its call.
    Cancelled(Vec<ProviderHeldReply>),
}

/// Dispatches one assistant message's tool calls and records every result. `planned` is every
/// call the message asked for, which the plane is told of; `calls` are the ones still to be
/// answered, all of them unless the batch is resumed. `known` names those that already have
/// intents; every other call gets its intent here.
#[allow(clippy::too_many_arguments)]
async fn batch<W, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    planned: &[ToolCall],
    calls: &[ToolCall],
    known: &[Known],
    manifest: &ToolManifest,
    hooks: Option<&broca_core::hooks::HookRuntime<'_>>,
    started: bool,
) -> Result<BatchOutcome, LoopError>
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
        // A result keeps only media captured here for that exact call (`checked`).
        media_capture: writer.media(),
    };
    // The plane learns of the batch first, so it can say where each call goes; the intents then
    // record that, with each call's key, before anything is sent. A plane that refuses the batch
    // leaves every call unsent, with a terminal saying why.
    let guard = ToolOpenGuard::new(Some(&seams.cancel), Some(&seams.stop));
    let refusal = seams
        .tools
        .batch_started_guarded(&ctx, planned, manifest, &guard)
        .await
        .err();
    let sequential = calls.iter().any(|call| {
        manifest
            .get(&call.tool_name)
            .is_some_and(|tool| tool.execution_mode == ExecutionMode::Sequential)
    });
    let known_of = |call: &ToolCall| known.iter().find(|known| known.id == call.tool_call_id);
    // The policy may narrow a call's input, so the batch works from its own copy of the calls. A
    // call a resume already knows goes out with the input its intent recorded.
    let mut calls = calls.to_vec();
    for call in &mut calls {
        if let Some(args) = known_of(call).and_then(|known| known.args.clone()) {
            call.input = args;
        }
    }
    let plans = plan_calls(
        &mut calls,
        known,
        refusal.as_ref(),
        manifest,
        seams.approval,
        hooks,
        step_id,
    )
    .await?;
    let calls = &calls[..];
    let session = writer.session_id();
    let now = (seams.now_ms)();
    let mut prepared = Vec::with_capacity(calls.len());
    for (call, plan) in calls.iter().zip(&plans) {
        let intent = match plan {
            CallPlan::Dispatch(_) if known_of(call).is_none() => {
                // Arguments are stored again only when they differ from the input the message
                // last gave this call id, as today's converted records hold them.
                let last = planned
                    .iter()
                    .rev()
                    .find(|other| other.tool_call_id == call.tool_call_id)
                    .map(|other| &other.input);
                Some((
                    canonical(&call.input).map_err(LoopError::Parts)?.1,
                    (last != Some(&call.input)).then(|| call.input.clone()),
                    seams.tools.dispatch_target(&ctx, &call.tool_call_id),
                    seams
                        .tools
                        .dispatch_schema_pin(&ctx, &call.tool_call_id, &call.tool_name),
                ))
            }
            _ => None,
        };
        prepared.push(intent);
    }
    let dispatching = plans
        .iter()
        .any(|plan| matches!(plan, CallPlan::Dispatch(_)));
    let written = writer
        .append_with(
            &mut |_, _, first_seq| {
                let mut records = Vec::with_capacity(calls.len() + 1);
                if !started {
                    records.push(Record::BatchStart(BatchStart {
                        run_id: run_id.into(),
                        batch_id: batch_id.clone(),
                        step_id,
                        planned: planned
                            .iter()
                            .map(|call| call.tool_call_id.clone())
                            .collect(),
                    }));
                }
                for ((call, plan), intent) in calls.iter().zip(&plans).zip(&prepared) {
                    match (plan, intent) {
                        (
                            CallPlan::Dispatch(hooks),
                            Some((digest, args, module_id, schema_pin)),
                        ) => {
                            // The key names the intent record itself, so it never repeats in
                            // this session and a late result or withdrawal finds exactly this
                            // call.
                            let seq = first_seq + records.len() as u64;
                            records.push(Record::ToolIntent(ToolIntent {
                                run_id: run_id.into(),
                                batch_id: batch_id.clone(),
                                tool_call_id: call.tool_call_id.clone(),
                                tool_name: Some(call.tool_name.clone()),
                                module_id: module_id.clone(),
                                args_digest: *digest,
                                args: args.clone(),
                                call_key: Some(format!("{session}:{seq}")),
                                approval_digest: None,
                                dispatched_at_ms: Some(now),
                                schema_pin: schema_pin.as_ref().map(|pin| {
                                    serde_json::to_value(pin).expect("a pin serializes")
                                }),
                                hooks: hooks.clone(),
                                key_derived: None,
                            }));
                        }
                        (CallPlan::NoDispatch(reason, hooks), _) => {
                            no_dispatch(
                                &mut records,
                                run_id,
                                &batch_id,
                                &call.tool_call_id,
                                reason,
                                hooks.clone(),
                                now,
                            );
                        }
                        (
                            CallPlan::HookTerminal {
                                result,
                                reason,
                                hooks,
                            },
                            _,
                        ) => hook_terminal(
                            &mut records,
                            run_id,
                            &batch_id,
                            result.clone(),
                            reason.clone(),
                            hooks.clone(),
                            now,
                        ),
                        // A batch that sends nothing records its other terminals in the same
                        // barrier; one that sends records them with the answers.
                        (CallPlan::Synthetic { result, reason }, _) if !dispatching => {
                            terminal_with_reason(
                                &mut records,
                                run_id,
                                &batch_id,
                                result.clone(),
                                CallOutcome::Completed,
                                reason.clone(),
                                Some(now),
                            );
                        }
                        _ => {}
                    }
                }
                if !dispatching {
                    records.push(Record::BatchEnd(BatchEnd {
                        run_id: run_id.into(),
                        batch_id: batch_id.clone(),
                    }));
                }
                records
            },
            true,
        )
        .await?;
    if !dispatching {
        let _ = seams.tools.batch_finished(&ctx).await;
        return Ok(BatchOutcome::Closed { unknown: false });
    }
    // The calls that are sent, with their keys: a known call's from its intent, a new call's
    // from the intent just written.
    let sending: Vec<ToolCall> = calls
        .iter()
        .zip(&plans)
        .filter(|(_, plan)| matches!(plan, CallPlan::Dispatch(_)))
        .map(|(call, _)| call.clone())
        .collect();
    let keys: Vec<(ToolCallId, String)> = sending
        .iter()
        .filter_map(|call| {
            let key = match known_of(call) {
                Some(known) => known.key.clone(),
                None => written.iter().find_map(|record| match record {
                    Record::ToolIntent(intent) if intent.tool_call_id == call.tool_call_id => {
                        intent.call_key.clone()
                    }
                    _ => None,
                }),
            };
            key.map(|key| (call.tool_call_id.clone(), key))
        })
        .collect();
    let registrations: Vec<DispatchRegistration> = keys
        .iter()
        .map(|(tool_call_id, call_key)| DispatchRegistration {
            tool_call_id: tool_call_id.clone(),
            call_key: call_key.clone(),
            approved: false,
        })
        .collect();
    if !registrations.is_empty() {
        seams
            .tools
            .register_dispatch_group(&ctx, &registrations)
            .await
            .map_err(|error| LoopError::Tools(format!("late-results registration: {error}")))?;
    }

    let held: Vec<Option<&ProviderHeldReply>> = sending
        .iter()
        .map(|call| known_of(call).and_then(|known| known.held.as_ref()))
        .collect();
    let plan = DispatchPlan {
        ctx: &ctx,
        calls: &sending,
        keys: &keys,
        held: &held,
        guard: &guard,
        batch_id: &batch_id,
        run_id,
        sequential,
        hooks,
        step_id,
    };
    let dispatched = dispatch_all(writer, seams, &plan).await?;
    let (answers, paused, closed) = match dispatched {
        Dispatched::All { answers, closed } => (answers, false, closed),
        Dispatched::Held(answers) => (answers, true, Vec::new()),
    };
    // The answers, back in the message's order, with each synthesized terminal in its place.
    let mut answers = answers.into_iter();
    let mut records = Vec::new();
    let mut unknown = false;
    for plan in &plans {
        match plan {
            CallPlan::Dispatch(_) => {
                let Some(answer) = answers.next().flatten() else {
                    continue;
                };
                unknown |= answer.outcome == CallOutcome::Unknown;
                if !answer.durable {
                    let now = (seams.now_ms)();
                    terminal_with_reason(
                        &mut records,
                        run_id,
                        &batch_id,
                        answer.result,
                        answer.outcome,
                        answer.reason,
                        Some(now),
                    );
                    if let Some(Record::ToolOutcome(outcome)) = records.last_mut() {
                        outcome.hooks = answer.hooks;
                    }
                }
            }
            CallPlan::Synthetic { result, reason } => terminal_with_reason(
                &mut records,
                run_id,
                &batch_id,
                result.clone(),
                CallOutcome::Completed,
                reason.clone(),
                Some((seams.now_ms)()),
            ),
            CallPlan::NoDispatch(..) | CallPlan::HookTerminal { .. } => {}
        }
    }
    if paused {
        // The answers that came in are durable before the run pauses; the batch stays open.
        if !records.is_empty() {
            writer.append(&records, true).await?;
        }
        let finished = pause(writer, seams, run_id, PauseReason::ToolHeld, None).await?;
        return Ok(BatchOutcome::Held(finished));
    }
    // A batch that closed held calls isn't finished on the plane: the provider still holds them
    // until it's asked to withdraw them.
    if closed.is_empty() {
        let _ = seams.tools.batch_finished(&ctx).await;
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    writer.append(&records, true).await?;
    if !closed.is_empty() {
        return Ok(BatchOutcome::Cancelled(closed));
    }
    Ok(BatchOutcome::Closed { unknown })
}

/// What a batch does with one call, decided before anything is written.
enum CallPlan {
    /// Sent to the tool plane; its intent records what the pre-tool hooks did, if any.
    Dispatch(Option<Value>),
    /// Never sent, durable at once: the plane can't be reached, the call is invalid, or the
    /// approval policy denied it. The outcome records what the pre-tool hooks did, if any.
    NoDispatch(NoDispatchReason, Option<Value>),
    /// Never sent because a pre-tool hook refused it, durable at once with the refusal's result
    /// and reason code, and what the hooks did.
    HookTerminal {
        result: ToolResult,
        reason: String,
        hooks: Option<Value>,
    },
    /// Never sent, answered by the loop with `result`: a repeated call id, or a refusal the
    /// plane named.
    Synthetic {
        result: ToolResult,
        reason: Option<String>,
    },
}

/// Decides each call's plan in the message's order, as today's gate does: a call a resume
/// already knows is sent as recorded; a repeated call id gets an error; a refused batch refuses
/// every call; a call the run's manifest doesn't allow is never sent. A valid call then goes
/// through the pre-tool mutate hooks, a second manifest check, the approval policy and the
/// validate hooks; its input in `calls` becomes what those left, so its intent records it.
#[allow(clippy::too_many_arguments)]
async fn plan_calls(
    calls: &mut [ToolCall],
    known: &[Known],
    refusal: Option<&broca_core::tool_plane::ToolPlaneError>,
    manifest: &ToolManifest,
    approval: &dyn ApprovalPolicy,
    hooks: Option<&broca_core::hooks::HookRuntime<'_>>,
    step_id: u64,
) -> Result<Vec<CallPlan>, LoopError> {
    let mut seen = std::collections::HashSet::new();
    let mut plans = Vec::with_capacity(calls.len());
    for call in calls.iter_mut() {
        let first = seen.insert(call.tool_call_id.clone());
        let plan = match gate_call(call, first, known, refusal, manifest) {
            Some(plan) => plan,
            None => gate_hooked(call, manifest, approval, hooks, step_id).await?,
        };
        plans.push(plan);
    }
    Ok(plans)
}

/// A valid call through the pre-tool hooks and the approval policy.
async fn gate_hooked(
    call: &mut ToolCall,
    manifest: &ToolManifest,
    approval: &dyn ApprovalPolicy,
    hooks: Option<&broca_core::hooks::HookRuntime<'_>>,
    step_id: u64,
) -> Result<CallPlan, LoopError> {
    use broca_core::hooks::{Subject, Trace};
    use cortexkit_role_step_transform_provider::subscription::Phase;
    let original = call.input.clone();
    let mut trace = Trace::default();
    let metadata = |trace: &Trace, input: &Value, intentless: bool| {
        trace
            .metadata(original.clone(), input.clone(), intentless)
            .map(|hooks| serde_json::to_value(hooks).expect("hook metadata serializes"))
    };
    if let Some(runtime) = hooks {
        let output = runtime
            .chain(
                broca_core::run::pre_tool_subject(StepId(step_id), call, Phase::Mutate),
                trace,
                |_| true,
            )
            .await;
        trace = output.trace;
        if output.cancellation.is_some() {
            return Err(LoopError::Unsupported(
                "a pre-tool hook cut short by a stop",
            ));
        }
        if let Subject::PreTool { input, .. } = output.subject {
            call.input = input;
        }
        if let Some((provider, text, unavailable)) = output.refusal {
            return Ok(hook_refused(
                call,
                &trace,
                &provider,
                &text,
                unavailable,
                &metadata,
            ));
        }
    }
    if let Err(invalid) = manifest.validate_call(call) {
        return Ok(CallPlan::NoDispatch(
            NoDispatchReason::ValidationFailed {
                detail: invalid.to_string(),
            },
            metadata(&trace, &call.input, true),
        ));
    }
    match approval.decide(call).await {
        ApprovalResponse::Allow { .. } => {}
        ApprovalResponse::AllowNarrowed { input, .. } => call.input = input,
        ApprovalResponse::Deny { reason, .. } => {
            return Ok(CallPlan::NoDispatch(
                NoDispatchReason::Denied { reason },
                metadata(&trace, &call.input, true),
            ));
        }
    }
    if let Some(runtime) = hooks {
        let output = runtime
            .chain(
                broca_core::run::pre_tool_subject(StepId(step_id), call, Phase::Validate),
                trace,
                |_| true,
            )
            .await;
        trace = output.trace;
        if output.cancellation.is_some() {
            return Err(LoopError::Unsupported(
                "a pre-tool hook cut short by a stop",
            ));
        }
        if let Some((provider, text, unavailable)) = output.refusal {
            return Ok(hook_refused(
                call,
                &trace,
                &provider,
                &text,
                unavailable,
                &metadata,
            ));
        }
    }
    Ok(CallPlan::Dispatch(metadata(&trace, &call.input, false)))
}

/// The plan for a call a pre-tool hook refused: today's refusal result and reason code.
fn hook_refused(
    call: &ToolCall,
    trace: &broca_core::hooks::Trace,
    provider: &str,
    text: &str,
    unavailable: bool,
    metadata: &dyn Fn(&broca_core::hooks::Trace, &Value, bool) -> Option<Value>,
) -> CallPlan {
    let (result, reason) = broca_core::run::pre_tool_refusal(call, provider, text, unavailable);
    CallPlan::HookTerminal {
        result,
        reason: reason.into(),
        hooks: metadata(trace, &call.input, true),
    }
}

/// The run's hook plan, from its frozen manifest. A plan that declares hooks without a plane to
/// call them refuses the run, as today's loop does; one whose hooks ask a person to approve a
/// call isn't supported yet.
fn hook_plan<P, T>(
    admission: &Admission,
    seams: &Seams<'_, P, T>,
) -> Result<Option<broca_core::hooks::HookPlan>, LoopError> {
    let Some(manifest) = admission
        .manifest
        .as_deref()
        .filter(|manifest| broca_core::hooks::has_transforms(manifest))
    else {
        return Ok(None);
    };
    let plan = broca_core::hooks::HookPlan::from_manifest(manifest).map_err(LoopError::Parts)?;
    if seams.hook_plane.is_none() {
        return Err(LoopError::Unsupported(
            "a plan that declares hooks, without a hook plane",
        ));
    }
    if plan.approves() {
        return Err(LoopError::Unsupported(
            "hooks that ask a person to approve a call",
        ));
    }
    Ok(Some(plan))
}

/// The checks that come before the approval policy. `None` means the call reaches the policy. A
/// call a resume already knows is sent as it was, without asking the policy again.
fn gate_call(
    call: &ToolCall,
    first: bool,
    known: &[Known],
    refusal: Option<&broca_core::tool_plane::ToolPlaneError>,
    manifest: &ToolManifest,
) -> Option<CallPlan> {
    use broca_core::tool_plane::ToolPlaneError;
    if known.iter().any(|known| known.id == call.tool_call_id) {
        return Some(CallPlan::Dispatch(None));
    }
    if !first {
        return Some(CallPlan::Synthetic {
            result: error_result(&call.tool_call_id, "duplicate tool_call_id from provider"),
            reason: None,
        });
    }
    match refusal {
        None => {}
        Some(ToolPlaneError::Permanent { code, detail }) => {
            return Some(CallPlan::Synthetic {
                result: ToolResult {
                    tool_call_id: call.tool_call_id.clone(),
                    output: broca_core::tool_plane::permanent_refusal_output(code, detail.as_ref()),
                    is_error: true,
                },
                reason: Some(code.clone()),
            });
        }
        Some(ToolPlaneError::Unavailable(_)) => {
            // The stable message, which sessions already on disk hold as their reason.
            return Some(CallPlan::NoDispatch(
                NoDispatchReason::Unavailable {
                    detail: broca_protocol::TOOL_PLANE_UNAVAILABLE_MESSAGE.into(),
                    module: None,
                    code: None,
                    refusal_detail: None,
                },
                None,
            ));
        }
        Some(ToolPlaneError::ModuleUnavailable {
            module,
            code,
            detail,
        }) => {
            return Some(CallPlan::NoDispatch(
                NoDispatchReason::Unavailable {
                    detail: broca_protocol::TOOL_PLANE_UNAVAILABLE_MESSAGE.into(),
                    module: Some(module.clone()),
                    code: Some(code.clone()),
                    refusal_detail: (!detail.is_empty()).then(|| detail.clone()),
                },
                None,
            ));
        }
        Some(error @ ToolPlaneError::ScopeRefused { .. }) => {
            return Some(CallPlan::NoDispatch(
                NoDispatchReason::Unavailable {
                    detail: error.to_string(),
                    module: None,
                    code: None,
                    refusal_detail: None,
                },
                None,
            ));
        }
        Some(ToolPlaneError::ScopeDrained { reason }) => {
            return Some(CallPlan::Synthetic {
                result: broca_core::run::route_drained_result(&call.tool_call_id, reason),
                reason: Some(reason.clone()),
            });
        }
    }
    if let Err(invalid) = manifest.validate_call(call) {
        return Some(CallPlan::NoDispatch(
            NoDispatchReason::ValidationFailed {
                detail: invalid.to_string(),
            },
            None,
        ));
    }
    None
}

/// A call that was never sent: today's no-dispatch result in the transcript and its reason in
/// the ledger.
fn no_dispatch(
    records: &mut Vec<Record>,
    run_id: &str,
    batch_id: &str,
    id: &ToolCallId,
    reason: &NoDispatchReason,
    hooks: Option<Value>,
    now: u64,
) {
    let result = broca_core::replay::no_dispatch_result(id, reason);
    let digest = Digest(Sha256::digest(result.encode()).into());
    let is_error = result.is_error;
    records.push(Record::ToolReply(ToolReply {
        run_id: run_id.into(),
        batch_id: Some(batch_id.into()),
        result,
        finished_at_ms: Some(now),
        tags_v1: None,
        media: Vec::new(),
    }));
    records.push(Record::ToolOutcome(ToolOutcome {
        run_id: run_id.into(),
        batch_id: batch_id.into(),
        tool_call_id: id.clone(),
        outcome: CallOutcome::NotDispatched,
        result_digest: digest,
        is_error,
        reason: None,
        finished_at_ms: Some(now),
        detail: serde_json::to_value(reason).ok(),
        hooks,
    }));
}

/// A call a pre-tool hook refused: its result in the transcript and its outcome, completed with
/// the refusal's reason code, in the ledger.
fn hook_terminal(
    records: &mut Vec<Record>,
    run_id: &str,
    batch_id: &str,
    result: ToolResult,
    reason: String,
    hooks: Option<Value>,
    now: u64,
) {
    terminal_with_reason(
        records,
        run_id,
        batch_id,
        result,
        CallOutcome::Completed,
        Some(reason),
        Some(now),
    );
    if let Some(Record::ToolOutcome(outcome)) = records.last_mut() {
        outcome.hooks = hooks;
    }
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
    terminal_with_reason(records, run_id, batch_id, result, outcome, None, Some(now));
}

/// `terminal`, with the reason code the outcome records, such as why a held call was closed.
/// A terminal today's loop writes without a time, such as a synthesized one, has none here.
fn terminal_with_reason(
    records: &mut Vec<Record>,
    run_id: &str,
    batch_id: &str,
    result: ToolResult,
    outcome: CallOutcome,
    reason: Option<String>,
    now: Option<u64>,
) {
    let digest = Digest(Sha256::digest(result.encode()).into());
    let tool_call_id = result.tool_call_id.clone();
    let is_error = result.is_error;
    records.push(Record::ToolReply(ToolReply {
        run_id: run_id.into(),
        batch_id: Some(batch_id.into()),
        result,
        finished_at_ms: now,
        tags_v1: None,
        media: Vec::new(),
    }));
    records.push(Record::ToolOutcome(ToolOutcome {
        run_id: run_id.into(),
        batch_id: batch_id.into(),
        tool_call_id,
        outcome,
        result_digest: digest,
        is_error,
        reason,
        finished_at_ms: now,
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

/// The runner asked the run to stop, or the caller cancelled it, before its batch was
/// dispatched. No call is sent: each gets a result saying so, the tool plane is never told the
/// batch started, and the run ends cancelled (`cancelled`) or interrupted whatever its restart
/// policy, as today's loop does. The batch and the run's end are two barriers, as there.
async fn stopped_batch<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    calls: &[ToolCall],
    cancelled: bool,
) -> Result<Finished, LoopError> {
    let batch_id = format!("{run_id}-b{step_id}");
    let now = (seams.now_ms)();
    let mut records = vec![Record::BatchStart(BatchStart {
        run_id: run_id.into(),
        batch_id: batch_id.clone(),
        step_id,
        planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
    })];
    let mut seen = std::collections::HashSet::new();
    for call in calls {
        // A repeated call id still gets today's duplicate error, not "not run".
        if !seen.insert(call.tool_call_id.clone()) {
            terminal_with_reason(
                &mut records,
                run_id,
                &batch_id,
                error_result(&call.tool_call_id, "duplicate tool_call_id from provider"),
                CallOutcome::Completed,
                None,
                Some(now),
            );
            continue;
        }
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
    // A caller's cancel ends the run cancelled; a stop ends it interrupted by the shutdown.
    let reason = if cancelled {
        RunFinishReason::Cancelled
    } else {
        RunFinishReason::Interrupted
    };
    let mut end = run_end(writer, run_id, reason, now);
    if let Record::RunEnd(end) = &mut end {
        if !cancelled {
            end.interrupt_reason = Some("runner_shutdown".into());
        }
    }
    append_end(writer, run_id, vec![end]).await?;
    Ok(finished(writer, reason))
}

/// A queued send with a delivery order, as a boundary delivers it.
struct OrderedSend {
    order: DeliveryOrder,
    /// The user messages a `steer` delivery places: the input after the send's `pre_user`
    /// hooks, without system messages, as today's queue holds it.
    input: Vec<Message>,
    written_at_ms: Option<u64>,
}

/// The queue's sends that carry a delivery order, read from their records. Only `steer` and
/// `idle_start` orders are delivered by this loop yet: an order naming `tool_result` or
/// `turn_system` refuses by name rather than delivering it differently from today.
async fn ordered_sends<W: Writer>(
    writer: &mut W,
) -> Result<std::collections::BTreeMap<String, OrderedSend>, LoopError> {
    let ids: Vec<String> = writer
        .operations()
        .queue
        .iter()
        .filter(|pending| pending.ordered)
        .map(|pending| pending.submission_id.clone())
        .collect();
    let mut sends = std::collections::BTreeMap::new();
    for id in ids {
        let records = writer.send_records(&id).await?;
        let order = records.iter().find_map(|record| match record {
            Record::DeliveryOrdered(ordered) => Some(ordered.order.clone()),
            _ => None,
        });
        let queued = records.iter().find_map(|record| match record {
            Record::QueuedSend(queued) => Some(queued),
            _ => None,
        });
        let (Some(order), Some(queued)) = (order, queued) else {
            return Err(LoopError::Parts(format!(
                "queued send {id} is missing its order or its request"
            )));
        };
        let order: DeliveryOrder =
            serde_json::from_value(order).map_err(|e| LoopError::Parts(e.to_string()))?;
        if order
            .modes
            .iter()
            .any(|mode| matches!(mode, DeliveryMode::ToolResult | DeliveryMode::TurnSystem))
        {
            return Err(LoopError::Unsupported(
                "a queued send delivered as a tool result or turn-scoped system message",
            ));
        }
        let hooked = queued
            .receipt
            .as_ref()
            .and_then(|receipt| receipt.get("hooks"))
            .and_then(|hooks| hooks.get("final_value"))
            .cloned()
            .and_then(|value| serde_json::from_value::<Vec<Message>>(value).ok());
        let input = hooked
            .unwrap_or_else(|| queued.input.clone())
            .into_iter()
            .filter(|message| message.role != Role::System)
            .collect();
        sends.insert(
            id,
            OrderedSend {
                order,
                input,
                written_at_ms: queued.written_at_ms,
            },
        );
    }
    Ok(sends)
}

/// The result a call gets when the run stopped before the call was dispatched.
const NOT_DISPATCHED: &str =
    "tool not run: the run was interrupted before this call was dispatched";

/// A held reply on its way to the batch's writer, with where to say it's durable.
type HeldRequest = (ProviderHeldReply, oneshot::Sender<Result<(), String>>);

/// The held-reply recorder the loop hands the plane. The plane's dispatch future runs while
/// the batch driver owns the writer, so the recorder doesn't write: it asks the driver to
/// append the held record durably and waits for the answer.
struct HeldChannel {
    requests: futures::channel::mpsc::UnboundedSender<HeldRequest>,
}

#[async_trait::async_trait]
impl ProviderHeldRecorder for HeldChannel {
    async fn sync_held(&self, reply: &ProviderHeldReply) -> Result<(), String> {
        let (answer, answered) = oneshot::channel();
        self.requests
            .unbounded_send((reply.clone(), answer))
            .map_err(|_| "the batch no longer takes held replies".to_string())?;
        answered
            .await
            .map_err(|_| "the batch stopped before the held reply was durable".to_string())?
    }
}

/// What a batch's dispatch works from.
struct DispatchPlan<'p> {
    ctx: &'p BatchCtx,
    calls: &'p [ToolCall],
    keys: &'p [(ToolCallId, String)],
    /// Per call, in order: the provider's held reply when the call was held before the run
    /// paused; that call waits for its late result again instead of being sent.
    held: &'p [Option<&'p ProviderHeldReply>],
    guard: &'p ToolOpenGuard,
    batch_id: &'p str,
    run_id: &'p str,
    sequential: bool,
    hooks: Option<&'p broca_core::hooks::HookRuntime<'p>>,
    step_id: u64,
}

/// What a call resolved to, before the batch takes it: the result and outcome, the code the
/// outcome records, and what its post-tool hooks did.
struct Resolved {
    result: ToolResult,
    outcome: CallOutcome,
    reason: Option<String>,
    hooks: Option<Value>,
}

impl From<(ToolResult, CallOutcome)> for Resolved {
    fn from((result, outcome): (ToolResult, CallOutcome)) -> Self {
        Self {
            result,
            outcome,
            reason: None,
            hooks: None,
        }
    }
}

/// One call's answer.
struct Answer {
    result: ToolResult,
    outcome: CallOutcome,
    /// The code its outcome records: a held call a cancel closed, or a result a post-tool hook
    /// withheld.
    reason: Option<String>,
    /// What its post-tool hooks did, recorded on its outcome.
    hooks: Option<Value>,
    /// Already appended: a held call's answer is durable as soon as it arrives.
    durable: bool,
}

/// How a batch's dispatch ended. Answers are in call order; `None` for a call still waiting.
enum Dispatched {
    /// Every call answered. `closed` are the held calls a cancel closed, whose providers are
    /// asked to withdraw them once the run's end is durable.
    All {
        answers: Vec<Option<Answer>>,
        closed: Vec<ProviderHeldReply>,
    },
    /// Only calls a provider holds were still waiting.
    Held(Vec<Option<Answer>>),
}

/// Dispatches every call, in parallel unless one of them must run alone, and collects their
/// answers, matching today's held-batch scheduling:
/// - each call carries its key and the batch's held-reply recorder; a held reply is appended,
///   durably, while the other calls keep running;
/// - a held call's answer is appended, durably, as soon as it arrives, before any call queued
///   behind it is sent, and the plane is then told it's durable;
/// - after a call whose outcome is unknown, no later call is sent: each gets the interrupted
///   result;
/// - when every call still running is held, the dispatch stops and the run pauses.
async fn dispatch_all<W: Writer, P, T: ToolPlane>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    plan: &DispatchPlan<'_>,
) -> Result<Dispatched, LoopError> {
    use futures::stream::FuturesUnordered;
    let calls = plan.calls;
    let (requests, mut held_requests) = futures::channel::mpsc::unbounded::<HeldRequest>();
    let recorder = HeldChannel { requests };
    let recorder = &recorder;
    let key_of = |call: &ToolCall| {
        plan.keys
            .iter()
            .find(|(id, _)| *id == call.tool_call_id)
            .map(|(_, key)| key.as_str())
            .expect("every dispatched call has an intent with a key")
    };
    let mut open_held: Vec<ToolCallId> = calls
        .iter()
        .zip(plan.held)
        .filter(|(_, held)| held.is_some())
        .map(|(call, _)| call.tool_call_id.clone())
        .collect();
    let mut answers: Vec<Option<Answer>> = calls.iter().map(|_| None).collect();
    let mut jobs = FuturesUnordered::new();
    let mut next = 0;
    let mut tripped = false;
    let mut closed: Vec<ProviderHeldReply> = Vec::new();
    loop {
        // A caller's cancel closes every call a provider holds: it gets the close result, durable
        // at once, and its provider is asked to withdraw it after the run's end. Calls still
        // running finish as usual.
        if seams.cancel.is_cancelled() && !open_held.is_empty() {
            for id in std::mem::take(&mut open_held) {
                let index = calls
                    .iter()
                    .position(|call| call.tool_call_id == id)
                    .expect("a held call is one of the batch's");
                let (result, outcome, code) = held_close_result(&id);
                let mut records = Vec::new();
                terminal_with_reason(
                    &mut records,
                    plan.run_id,
                    plan.batch_id,
                    result.clone(),
                    outcome,
                    Some(code.into()),
                    Some((seams.now_ms)()),
                );
                writer.append(&records, true).await?;
                if let Some(lifecycle) = seams.held {
                    lifecycle.terminal_synced(plan.ctx, &id).await;
                }
                closed.push(held_reply_of(writer, plan, &id));
                answers[index] = Some(Answer {
                    result,
                    outcome,
                    reason: Some(code.into()),
                    hooks: None,
                    durable: true,
                });
            }
        }
        while next < calls.len() && (!plan.sequential || jobs.is_empty()) {
            let index = next;
            next += 1;
            let call = &calls[index];
            let held = plan.held[index];
            let interrupted = tripped && held.is_none();
            let key = key_of(call);
            jobs.push(async move {
                let answer: Resolved = if interrupted {
                    (
                        interrupted_result(&call.tool_call_id),
                        CallOutcome::Interrupted,
                    )
                        .into()
                } else if let Some(reply) = held {
                    match seams.held {
                        Some(lifecycle) => resolve(
                            call,
                            checked(plan.ctx, lifecycle.wait(plan.ctx, call, reply).await),
                        )
                        .into(),
                        None => std::future::pending().await,
                    }
                } else {
                    dispatch_one(seams.tools, plan, call, key, recorder).await
                };
                (index, answer)
            });
        }
        if answers.iter().all(Option::is_some) {
            return Ok(Dispatched::All { answers, closed });
        }
        if !tripped && !open_held.is_empty() && jobs.len() == open_held.len() {
            // Only held calls wait. Anything already answered is taken first, as today's
            // loop does, then the run pauses.
            if let Some(Some((index, answer))) = futures::FutureExt::now_or_never(jobs.next()) {
                settle(
                    writer,
                    seams,
                    plan,
                    &mut open_held,
                    &mut answers,
                    &mut tripped,
                    index,
                    answer,
                )
                .await?;
                continue;
            }
            // A cancel that came in meanwhile closes the held calls instead of pausing on them.
            if seams.cancel.is_cancelled() {
                continue;
            }
            return Ok(Dispatched::Held(answers));
        }
        // While a provider holds a call, a cancel also wakes the dispatch, to close it.
        let hold_open = !open_held.is_empty();
        let cancel = async {
            if !hold_open {
                std::future::pending::<()>().await;
            }
            seams.cancel.cancelled().await;
        };
        let cancel = std::pin::pin!(cancel);
        let woke = futures::future::select(
            futures::future::select(jobs.next(), held_requests.next()),
            cancel,
        )
        .await;
        let futures::future::Either::Left((woke, _)) = woke else {
            continue;
        };
        match woke {
            futures::future::Either::Left((Some((index, answer)), _)) => {
                if answers[index].is_some() {
                    // A held call a cancel already closed: its late answer is ignored.
                    continue;
                }
                settle(
                    writer,
                    seams,
                    plan,
                    &mut open_held,
                    &mut answers,
                    &mut tripped,
                    index,
                    answer,
                )
                .await?;
            }
            futures::future::Either::Left((None, _)) => {
                unreachable!("jobs run until every call has an answer")
            }
            futures::future::Either::Right((Some((reply, answered)), _)) => {
                let record = Record::ToolHeld(ToolHeld {
                    batch_id: plan.batch_id.into(),
                    tool_call_id: reply.tool_call_id.clone(),
                    call_key: reply.call_key.clone(),
                    module: reply.module.clone(),
                    reason: reply.reason.clone(),
                    detail_digest: reply.detail_digest.clone(),
                    held_at_ms: (seams.now_ms)(),
                    sequential: plan.sequential,
                });
                let durable = writer
                    .append(&[record], true)
                    .await
                    .map(|_| ())
                    .map_err(|error| format!("{error:?}"));
                if durable.is_ok() {
                    open_held.push(reply.tool_call_id.clone());
                }
                let _ = answered.send(durable);
            }
            futures::future::Either::Right((None, _)) => {
                unreachable!("the recorder lives as long as the dispatches")
            }
        }
    }
}

/// Takes call `index`'s answer. A held call's answer is appended durably at once, and the plane
/// told so, before any call behind it is sent.
#[allow(clippy::too_many_arguments)]
async fn settle<W: Writer, P, T: ToolPlane>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    plan: &DispatchPlan<'_>,
    open_held: &mut Vec<ToolCallId>,
    answers: &mut [Option<Answer>],
    tripped: &mut bool,
    index: usize,
    resolved: Resolved,
) -> Result<(), LoopError> {
    let Resolved {
        result,
        outcome,
        reason,
        hooks,
    } = resolved;
    *tripped |= outcome == CallOutcome::Unknown;
    let id = plan.calls[index].tool_call_id.clone();
    let durable = open_held.contains(&id);
    if durable {
        let mut records = Vec::new();
        terminal(
            &mut records,
            plan.run_id,
            plan.batch_id,
            result.clone(),
            outcome,
            (seams.now_ms)(),
        );
        writer.append(&records, true).await?;
        open_held.retain(|held| *held != id);
        if let Some(lifecycle) = seams.held {
            lifecycle.terminal_synced(plan.ctx, &id).await;
        }
    }
    answers[index] = Some(Answer {
        result,
        outcome,
        reason,
        hooks,
        durable,
    });
    Ok(())
}

/// Asks each held call's provider to withdraw it, once the run's end is durable. Best effort:
/// a failure can't reopen the call, and a late answer for it is already ignored.
async fn withdraw<P, T>(seams: &Seams<'_, P, T>, closed: &[ProviderHeldReply]) {
    if let Some(lifecycle) = seams.held {
        for reply in closed {
            let _ = lifecycle.withdraw_held(reply).await;
        }
    }
}

/// The result, outcome and code a held call gets when a cancel closes it, as today's loop
/// closes one.
fn held_close_result(id: &ToolCallId) -> (ToolResult, CallOutcome, &'static str) {
    let (result, _, code) =
        broca_core::resume::provider_held_close_result(id, RunFinishReason::Cancelled);
    (result, CallOutcome::Interrupted, code)
}

/// The provider's held reply for the open held call `id`, as the run recorded it.
fn held_reply_of<W: Writer>(
    writer: &W,
    plan: &DispatchPlan<'_>,
    id: &ToolCallId,
) -> ProviderHeldReply {
    if let Some(reply) = plan
        .held
        .iter()
        .flatten()
        .find(|reply| reply.tool_call_id == *id)
    {
        return (*reply).clone();
    }
    let batch = run_state(writer)
        .open_batch
        .as_ref()
        .expect("a held call's batch is open");
    let position = (0..batch.calls.len())
        .find(|position| batch.id(*position) == id)
        .expect("a held call is planned");
    let held = batch.held_reply(position).expect("the call is held");
    ProviderHeldReply {
        tool_call_id: held.tool_call_id.clone(),
        call_key: held.call_key.clone(),
        module: held.module.clone(),
        reason: held.reason.clone(),
        detail_digest: held.detail_digest.clone(),
    }
}

/// The result a call gets when an earlier call of its batch may have run without an answer,
/// so it was never sent.
fn interrupted_result(id: &ToolCallId) -> ToolResult {
    ToolResult {
        tool_call_id: id.clone(),
        output: ToolOutput::error("tool not run: batch interrupted"),
        is_error: true,
    }
}

/// Sends one call with its key and the batch's held-reply recorder, and resolves the answer. A
/// result the provider returned goes through the post-tool hooks the plan subscribes for its
/// tool, before the batch takes it.
async fn dispatch_one<T: ToolPlane>(
    tools: &T,
    plan: &DispatchPlan<'_>,
    call: &ToolCall,
    call_key: &str,
    recorder: &HeldChannel,
) -> Resolved {
    let key = DispatchKey {
        call_key,
        held_recorder: Some(recorder),
    };
    match checked(
        plan.ctx,
        tools
            .dispatch_guarded(plan.ctx, call, Some(key), plan.guard)
            .await,
    ) {
        DispatchResult::Completed(result) => {
            post_tool(plan.hooks, plan.step_id, call, call_key, result).await
        }
        other => resolve(call, other).into(),
    }
}

/// A completed result keeps only the media the batch's capture holds for that exact call; every
/// other media part becomes a provenance marker, before hooks or the record see the result. A
/// reference to stored bytes, or one with the right digest and other metadata, isn't evidence
/// that this call produced them. Today's loop does the same in `resolve`.
fn checked(ctx: &BatchCtx, mut answer: DispatchResult) -> DispatchResult {
    if let DispatchResult::Completed(result) = &mut answer {
        let captured = ctx
            .media_capture
            .as_ref()
            .map(|capture| capture.captured_for(&ctx.batch_id, &result.tool_call_id))
            .unwrap_or_default();
        broca_core::tool_media::enforce_provenance(result, &captured);
    }
    answer
}

/// A model answer's text through the post-assistant hooks: each accepted edit is applied to the
/// answer's text parts in place. Returns what the hooks did, for the step's end record, or
/// `None` when they did nothing. The trace records the answer as the provider gave it.
async fn post_assistant(
    runtime: &broca_core::hooks::HookRuntime<'_>,
    step_id: u64,
    message: &mut broca_protocol::content::AssistantMessage,
) -> Option<Value> {
    use broca_protocol::content::ContentBlock;
    let original = serde_json::to_value(&*message).expect("an answer serializes");
    let subject = broca_core::hooks::Subject::PostAssistant {
        step_id: step_id.to_string(),
        blocks: message
            .content
            .iter()
            .filter_map(|part| match part {
                ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                    Some(text.clone())
                }
                _ => None,
            })
            .collect(),
    };
    let output = runtime
        .chain(subject, broca_core::hooks::Trace::default(), |_| true)
        .await;
    broca_core::hooks::apply_message_ops(&mut message.content, &output.operations)
        .expect("checked ops");
    output
        .trace
        .metadata(
            original,
            serde_json::to_value(&*message).expect("an answer serializes"),
            false,
        )
        .map(|hooks| serde_json::to_value(hooks).expect("hook metadata serializes"))
}

/// A provider's result through the post-tool hooks subscribed for its tool, as today's loop
/// runs them: a JSON result stays hookless, and a hook may edit the text or, when it was
/// required and unavailable, withhold the result.
async fn post_tool(
    hooks: Option<&broca_core::hooks::HookRuntime<'_>>,
    step_id: u64,
    call: &ToolCall,
    call_key: &str,
    result: ToolResult,
) -> Resolved {
    let subscribed = !matches!(result.output, ToolOutput::Json { .. })
        && hooks.is_some_and(|runtime| runtime.plan.post_tool(&call.tool_name));
    let Some(runtime) = hooks.filter(|_| subscribed) else {
        return (result, CallOutcome::Completed).into();
    };
    let output = runtime
        .chain(
            broca_core::hooks::Subject::PostTool {
                step_id: step_id.to_string(),
                tool: call.tool_name.clone(),
                tool_call_id: result.tool_call_id.0.clone(),
                call_key: Some(call_key.to_owned()),
                blocks: vec![broca_core::run::post_tool_text(&result)],
                is_error: result.is_error,
            },
            broca_core::hooks::Trace::default(),
            |_| true,
        )
        .await;
    let (result, reason, hooks) = broca_core::run::transformed_tool_result(&result, &output);
    Resolved {
        result,
        outcome: CallOutcome::Completed,
        reason,
        hooks: hooks.map(|hooks| serde_json::to_value(hooks).expect("hook metadata serializes")),
    }
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
/// A batch's last record apart from the entry facts after it: what a boundary batch decided,
/// whether the run's end, a pause or the step's start.
fn decision(records: &[Record]) -> Option<&Record> {
    records
        .iter()
        .rev()
        .find(|record| !matches!(record, Record::EntryFacts(_)))
}

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
    // Today's end record always carries the run's usage, empty when it took no step.
    Record::RunEnd(RunEnd {
        run_id: run_id.into(),
        reason,
        usage: Some(run.usage),
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
            &mut |_, operations, _| {
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
    if matches!(decision(&batch), Some(Record::RunEnd(_))) {
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
            &mut |_, operations, _| {
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
        reached_cache: broca_core::cache_warm::attempt_reached_the_cache(
            attempt.http_status,
            &attempt.outcome,
        )
        .then_some(true),
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
