//! Hook subjects and synchronous record hand-over for the run loop.
use super::*;

/// The seal and loop enqueue under the same lock; losing the hand-over must not advance
/// to dispatch or any later record, even while the actor has not aborted the task yet.
pub(super) async fn record_model_hook(
    seal: Option<&crate::ShutdownSeal>,
    hooked: bool,
    writer: &WalHandle,
    record: WalRecord,
) {
    if let Some(seal) = seal.filter(|_| hooked) {
        record_hook_stage(
            seal,
            &crate::shutdown_seal::HookStage::PostAssistant,
            writer,
            record,
        )
        .await;
    } else {
        writer.record(record);
    }
}

pub(super) async fn restream_message(
    message: &AssistantMessage,
    out: &mpsc::Sender<OutputEvent>,
    clean_reply: bool,
) {
    let _ = send(out, OutputEvent::Display(DisplayEvent::DisplayReset)).await;
    for (index, part) in message.content.iter().enumerate() {
        let event = match part {
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                DisplayEvent::TextDelta {
                    mid: None,
                    part: None,
                    message_id: message.message_id.clone(),
                    tool_call_id: None,
                    delta: broca_protocol::tags::exposed_model_text(clean_reply, text).into_owned(),
                }
            }
            ContentBlock::Reasoning { text, .. } => DisplayEvent::ReasoningDelta {
                mid: None,
                part: None,
                message_id: message.message_id.clone(),
                delta: text.to_string(),
            },
            ContentBlock::ToolCall(call) => DisplayEvent::ToolInputDelta {
                mid: None,
                part: None,
                tool_call_id: call.tool_call_id.clone(),
                tool_name: Some(call.tool_name.clone()),
                delta: call.input.to_string(),
            },
            _ => continue,
        };
        let mut event = event;
        event.set_part(index as u64);
        let _ = send(out, OutputEvent::Display(event)).await;
    }
}

pub(super) fn pre_tool_terminal(
    call: &ToolCall,
    original: &serde_json::Value,
    trace: &crate::hooks::Trace,
    provider: &str,
    text: &str,
    unavailable: bool,
) -> CallPlan {
    let (result, reason) = pre_tool_refusal(call, provider, text, unavailable);
    CallPlan::HookTerminal {
        result: Arc::new(result),
        reason: reason.into(),
        hooks: trace.metadata(original.clone(), call.input.clone().parse(), true),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn resolve_hooked<T: ToolPlane>(
    plan: CallPlan,
    source_index: usize,
    step_id: StepId,
    ctx: &BatchCtx,
    tool_plane: &T,
    cancel: Option<&CancellationToken>,
    stop: Option<&CancellationToken>,
    hooks: Option<&crate::hooks::HookRuntime<'_>>,
) -> CallResolution {
    let (tool, call_key) = match &plan {
        CallPlan::Dispatch(call) => (call.tool_name.clone(), call.call_key.clone()),
        _ => (String::new(), None),
    };
    let mut resolution = resolve(plan, ctx, tool_plane, cancel, stop).await;
    // JSON results remain hookless; intake already enforced the provider-result size limit.
    let subscribed = resolution.plane_subject
        && !matches!(resolution.result.output, ToolOutput::Json { .. })
        && hooks.is_some_and(|runtime| runtime.plan.post_tool(&tool));
    if !subscribed {
        return resolution;
    }
    let runtime = hooks.expect("subscribed implies an installed runtime");
    let original = Arc::clone(&resolution.result);
    let text = post_tool_text(&original);
    let stage = crate::shutdown_seal::HookStage::PostTool {
        source_index,
        tool_call_id: original.tool_call_id.0.clone(),
    };
    let output = runtime
        .chain(
            crate::hooks::Subject::PostTool {
                step_id: step_id.0.to_string(),
                tool,
                tool_call_id: original.tool_call_id.0.clone(),
                call_key,
                blocks: vec![text],
                is_error: original.is_error,
            },
            crate::hooks::Trace::default(),
            |fallback| {
                let Some(seal) = runtime.seal else {
                    return true;
                };
                let (result, reason, hooks) = transformed_tool_result(&original, fallback);
                seal.deposit_hook(
                    stage.clone(),
                    WalRecord::ToolResult {
                        finished_at_ms: None,
                        tags: None,
                        batch_id: ctx.batch_id.clone(),
                        result: Arc::new(result),
                        hooks,
                        reason,
                        outcome: DispatchOutcome::Completed,
                        withdraw_elicitation: None,
                        provider_held: None,
                    },
                )
            },
        )
        .await;
    let (result, reason, metadata) = transformed_tool_result(&original, &output);
    resolution.result = Arc::new(result);
    resolution.reason = reason;
    resolution.hooks = metadata;
    resolution.hook_stage = runtime.seal.map(|_| stage);
    resolution
}

/// A lost stage hand-over never returns to code that could enqueue a later marker.
pub(super) async fn record_hook_stage(
    seal: &crate::ShutdownSeal,
    stage: &crate::shutdown_seal::HookStage,
    writer: &WalHandle,
    record: WalRecord,
) {
    if !seal.record_hook(stage, writer, record) {
        std::future::pending::<()>().await;
    }
}
