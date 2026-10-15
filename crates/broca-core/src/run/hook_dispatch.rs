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

pub(super) async fn restream_message(message: &AssistantMessage, out: &mpsc::Sender<OutputEvent>) {
    let _ = send(out, OutputEvent::Display(DisplayEvent::DisplayReset)).await;
    for part in &message.content {
        let event = match part {
            ContentBlock::Text { text } => DisplayEvent::TextDelta {
                message_id: message.message_id.clone(),
                tool_call_id: None,
                delta: text.clone(),
            },
            ContentBlock::Reasoning { text, .. } => DisplayEvent::ReasoningDelta {
                message_id: message.message_id.clone(),
                delta: text.clone(),
            },
            ContentBlock::ToolCall(call) => DisplayEvent::ToolInputDelta {
                tool_call_id: call.tool_call_id.clone(),
                delta: call.input.to_string(),
            },
            _ => continue,
        };
        let _ = send(out, OutputEvent::Display(event)).await;
    }
}

pub(super) fn pre_tool_subject(
    step_id: StepId,
    call: &ToolCall,
    phase: cortexkit_role_step_transform_provider::subscription::Phase,
) -> crate::hooks::Subject {
    crate::hooks::Subject::PreTool {
        step_id: step_id.0.to_string(),
        phase,
        tool: call.tool_name.clone(),
        tool_call_id: call.tool_call_id.0.clone(),
        call_key: None,
        input: call.input.clone(),
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
    let reason = if unavailable {
        "pre_tool_unavailable"
    } else {
        "pre_tool_denied"
    };
    let message = if unavailable {
        format!("{reason}: {provider}; the tool was not executed")
    } else {
        text.into()
    };
    CallPlan::HookTerminal {
        result: Arc::new(error_result(call, &message)),
        reason: reason.into(),
        hooks: trace.metadata(original.clone(), call.input.clone(), true),
    }
}

fn transformed_tool_result(
    original: &ToolResult,
    output: &crate::hooks::ChainOutput,
) -> (ToolResult, Option<String>, Option<Box<broca_wal::Hooks>>) {
    let mut result = original.clone();
    let mut reason = None;
    if let Some((provider, _, _)) = &output.refusal {
        reason = Some("post_tool_unavailable".into());
        result.output = ToolOutput::Error {
            message: format!(
                "post_tool_unavailable: {provider}; the call executed, but its result is withheld"
            ),
        };
        result.is_error = true;
    } else if let crate::hooks::Subject::PostTool { blocks, .. } = &output.subject {
        if !output.operations.is_empty() {
            let text = blocks[0].clone();
            result.output = match &original.output {
                ToolOutput::Error { .. } => ToolOutput::Error { message: text },
                ToolOutput::Denied { .. } if text.starts_with("denied: ") => ToolOutput::Denied {
                    reason: text[8..].into(),
                },
                _ => ToolOutput::Text { text },
            };
        }
    }
    let metadata = output.trace.metadata(
        serde_json::to_value(original).unwrap(),
        serde_json::to_value(&result).unwrap(),
        false,
    );
    (result, reason, metadata)
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
    // Eligibility is decided BEFORE capping, so even an oversized Json remains hookless.
    let subscribed = resolution.plane_subject
        && !matches!(resolution.result.output, ToolOutput::Json { .. })
        && hooks.is_some_and(|runtime| runtime.plan.post_tool(&tool));
    if resolution.outcome.is_some() {
        cap_tool_result(
            Arc::make_mut(&mut resolution.result),
            &ctx.run_id,
            &tool,
            subscribed,
        );
    }
    if !subscribed {
        return resolution;
    }
    let runtime = hooks.expect("subscribed implies an installed runtime");
    let original = Arc::clone(&resolution.result);
    let text = match &original.output {
        ToolOutput::Text { text } => text.clone(),
        ToolOutput::Error { message } => message.clone(),
        ToolOutput::Denied { reason } => format!("denied: {reason}"),
        ToolOutput::Json { .. } => unreachable!(),
    };
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
