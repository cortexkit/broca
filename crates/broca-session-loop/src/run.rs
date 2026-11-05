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
use broca_core::run::{stop_satisfied, OutputEvent};
use broca_core::tool_plane::{BatchCtx, DispatchResult, ToolPlane};
use broca_protocol::caller::RunFinishReason;
use broca_core::tags::TagNumbering;
use broca_protocol::content::{
    stamp_ordinals_from, AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput,
    ToolResult,
};
use broca_protocol::request::EpisodeRenderInput;
use broca_protocol::tags::{clean_computed_into, ComputedRules};
use broca_wal::ComputedTagRules;
use broca_protocol::provider::{NormalizedEvent, ProviderControl};
use broca_protocol::request::{
    CallOptions, ExecutionMode, ModelHandle, ResponseFormat, StepContext, StopCondition, Tool,
    ToolChoice,
};
use broca_protocol::{BatchId, BindIdentity, FinishReason, GenerationConfig, RunId, StepId, Usage};
use broca_session_store::codec::{Digest, Message as _};
use broca_session_store::record::{
    Assistant, BatchEnd, BatchStart, CallOutcome, Input, PartKind, Placement, Record, RunEnd,
    RunStart, SetupStub, StepEnd, StepStart, ToolIntent, ToolOutcome, ToolReply,
};
use broca_session_store::session::SessionError;
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
    let prompt_start = writer.conversation().messages.len();
    admit(writer, parts, &admission).await?;
    // This run's place in the prompt, a render input. Only the session's first run is
    // supported yet: a later run also needs every earlier run's place and render config.
    if prompt_start != 0 {
        return Err(LoopError::Unsupported("a session's second run"));
    }
    let episode = EpisodeRenderInput {
        prompt_start,
        first_user: admission
            .input
            .iter()
            .position(|message| message.role == Role::User)
            .map(|index| prompt_start + index),
        frozen_config: admission.render_config.clone(),
    };
    let tools = Arc::new(admission.tools.clone());
    let manifest = ToolManifest::from_tools(admission.tools.clone());
    let stop_when = if admission.stop_when.is_empty() {
        vec![StopCondition::StepCount {
            n: DEFAULT_MAX_STEPS,
        }]
    } else {
        admission.stop_when.clone()
    };
    loop {
        if seams.cancel.is_cancelled() {
            return finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await;
        }
        let completed_steps = run_state(writer).completed_steps;
        // Step ids count from 1 within a run, as today's loop numbers them.
        let step_id = completed_steps + 1;
        writer
            .append(
                &[Record::StepStart(StepStart {
                    run_id: admission.run_id.clone(),
                    step_id,
                })],
                false,
            )
            .await?;
        let opts = call_options(writer, &admission, &tools, &episode, seams, step_id)?;
        let outcome = model_step(seams, step_id, opts).await;
        let step = match outcome {
            Ok(step) => step,
            Err(StepFailure::Cancelled) => {
                return finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await;
            }
            Err(StepFailure::Error) => {
                return finish(writer, seams, &admission.run_id, RunFinishReason::Error).await;
            }
        };
        let calls: Vec<ToolCall> = step.message.tool_calls().cloned().collect();
        let stop = stop_satisfied(&stop_when, completed_steps + 1, &step.message).cloned();
        let now = (seams.now_ms)();
        let assistant = Message::assistant_from(step.message.content.clone(), admission.model.clone());
        writer
            .append(
                &[
                    Record::Assistant(Assistant {
                        run_id: admission.run_id.clone(),
                        step_id,
                        provider_message_id: step.message.message_id.clone(),
                        message: assistant,
                        usage: Some(step.usage.clone()),
                        written_at_ms: Some(now),
                        last_content_at_ms: None,
                        tags_v1: None,
                        budget_finalize: None,
                        pruned: Vec::new(),
                    }),
                    Record::StepEnd(StepEnd {
                        run_id: admission.run_id.clone(),
                        step_id,
                        usage: Some(step.usage.clone()),
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
                        limiting_stop_met: None,
                    }),
                ],
                calls.is_empty(),
            )
            .await?;
        if calls.is_empty() {
            return finish(writer, seams, &admission.run_id, RunFinishReason::Completed).await;
        }
        if stop.is_some() {
            return Err(LoopError::Unsupported("a stop condition with tool calls pending"));
        }
        batch(writer, seams, &admission.run_id, step_id, &calls, &manifest).await?;
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
    ] {
        let value = value.map_err(|e| LoopError::Parts(e.to_string()))?;
        setup.push(parts.put(kind, &value).map_err(LoopError::Parts)?);
    }
    let tool_names = admission.tools.iter().map(|tool| tool.name.clone()).collect();
    let mut config = serde_json::Map::new();
    config.insert("tool_choice".into(), json!(admission.tool_choice));
    config.insert("generation".into(), json!(admission.generation));
    if !admission.provider_options.is_null() {
        config.insert("provider_options".into(), admission.provider_options.clone());
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
        render_config: (!admission.render_config.is_null()).then(|| admission.render_config.clone()),
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
        on_restart: None,
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

/// The request for step `step_id`: the conversation as the folds hold it, under the run's
/// frozen settings, with the render inputs today's loop gives a request: each message's
/// position, its part numbers under the run's tag rules, and the run's place in the prompt.
fn call_options<W: Writer, P, T>(
    writer: &W,
    admission: &Admission,
    tools: &Arc<Vec<Tool>>,
    episode: &EpisodeRenderInput,
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
    opts.render_episodes = Arc::new(vec![episode.clone()].into());
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

enum StepFailure {
    Cancelled,
    Error,
}

/// Runs one model call and assembles its answer. Display events go straight to the listener.
async fn model_step<P: Provider, T>(
    seams: &Seams<'_, P, T>,
    step_id: u64,
    opts: CallOptions,
) -> Result<Step, StepFailure> {
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
            _ = seams.cancel.cancelled() => return Err(StepFailure::Cancelled),
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
                    return Err(StepFailure::Error);
                };
                return Ok(Step {
                    message,
                    usage,
                    finish_reason,
                    retries_used,
                });
            }
            Some(NormalizedEvent::Control(ProviderControl::Error { .. })) | None => {
                return Err(StepFailure::Error)
            }
            Some(NormalizedEvent::Control(_)) => {}
        }
    }
}

/// Dispatches one assistant message's tool calls and records every result.
async fn batch<W, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    calls: &[ToolCall],
    manifest: &ToolManifest,
) -> Result<(), LoopError>
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
    let mut records = vec![Record::BatchStart(BatchStart {
        run_id: run_id.into(),
        batch_id: batch_id.clone(),
        step_id,
        planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
    })];
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
    let mut records = Vec::new();
    for result in results {
        let finished = Some((seams.now_ms)());
        let digest = Digest(Sha256::digest(result.encode()).into());
        records.push(Record::ToolOutcome(ToolOutcome {
            run_id: run_id.into(),
            batch_id: batch_id.clone(),
            tool_call_id: result.tool_call_id.clone(),
            outcome: CallOutcome::Completed,
            result_digest: digest,
            is_error: result.is_error,
            reason: None,
            finished_at_ms: finished,
            detail: None,
            hooks: None,
        }));
        records.insert(
            records.len() - 1,
            Record::ToolReply(ToolReply {
                run_id: run_id.into(),
                batch_id: Some(batch_id.clone()),
                result,
                finished_at_ms: finished,
                tags_v1: None,
            }),
        );
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    writer.append(&records, true).await?;
    Ok(())
}

/// Dispatches every call, in parallel unless one of them must run alone, and returns the
/// results in the calls' order.
async fn dispatch_all<P, T: ToolPlane>(
    seams: &Seams<'_, P, T>,
    ctx: &BatchCtx,
    calls: &[ToolCall],
    manifest: &ToolManifest,
) -> Vec<ToolResult> {
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
        let all = calls.iter().map(|call| async move {
            resolve(call, seams.tools.dispatch(ctx, call).await)
        });
        results = futures::future::join_all(all).await;
    }
    results
}

fn resolve(call: &ToolCall, result: DispatchResult) -> ToolResult {
    match result {
        DispatchResult::Completed(result) => result,
        DispatchResult::NotSent { reason }
        | DispatchResult::SentButOutcomeUnknown { reason }
        | DispatchResult::ScopeDrained { reason, .. } => ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::error(reason),
            is_error: true,
        },
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

/// Appends the run's end. Its usage is the run's total, present once a step completed.
async fn finish<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    reason: RunFinishReason,
) -> Result<Finished, LoopError> {
    let run = run_state(writer);
    let usage = (run.completed_steps > 0).then(|| run.usage.clone());
    let retries_used = run.retries_used;
    writer
        .append(
            &[Record::RunEnd(RunEnd {
                run_id: run_id.into(),
                reason,
                usage: usage.clone(),
                ts_ms: Some((seams.now_ms)()),
                retries_used,
                interrupt_reason: None,
                error: None,
            })],
            true,
        )
        .await?;
    Ok(Finished {
        reason,
        usage,
        retries_used,
    })
}

#[allow(dead_code)]
fn text_of(message: &Message) -> String {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}
