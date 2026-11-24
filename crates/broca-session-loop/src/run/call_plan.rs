//! What a batch does with each call, decided before anything is written: the pre-tool hooks,
//! the run's manifest, the approval policy and a repeated or already-known call id.

use super::*;

/// What a batch does with one call, decided before anything is written.
pub(super) enum CallPlan {
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

/// Decides each call's plan in the message's order, or `None` when a stop or the caller's
/// cancel cuts a pre-tool hook short: a repeated call id gets an error; a call a resume already
/// knows is sent as recorded; a call to a tool a pending plan change removes (`removed`) is
/// refused with its removal wording; a refused batch refuses
/// every call; a call the run's manifest doesn't allow is never sent. A valid call then goes
/// through the pre-tool mutate hooks, a second manifest check, the approval policy and the
/// validate hooks; its input in `calls` becomes what those left, so its intent records it.
#[allow(clippy::too_many_arguments)]
pub(super) async fn plan_calls(
    calls: &mut [ToolCall],
    known: &[Known],
    refusal: Option<&broca_core::tool_plane::ToolPlaneError>,
    manifest: &ToolManifest,
    approval: &dyn ApprovalPolicy,
    hooks: Option<&broca_core::hooks::HookRuntime<'_>>,
    step_id: u64,
    removed: &std::collections::BTreeSet<String>,
    // Ids whose first planned copy already has its result, so every copy of them planned here is
    // a repeat.
    answered_first: &std::collections::HashSet<ToolCallId>,
) -> Result<Option<Vec<CallPlan>>, LoopError> {
    let mut seen = answered_first.clone();
    let mut plans = Vec::with_capacity(calls.len());
    for call in calls.iter_mut() {
        let first = seen.insert(call.tool_call_id.clone());
        let plan = match gate_call(call, first, known, refusal, manifest, removed) {
            Some(plan) => plan,
            None => match gate_hooked(call, manifest, approval, hooks, step_id).await? {
                Some(plan) => plan,
                // A stop or the caller's cancel cut a pre-tool hook short: no call of the
                // batch is sent.
                None => return Ok(None),
            },
        };
        plans.push(plan);
    }
    Ok(Some(plans))
}

/// A valid call through the pre-tool hooks and the approval policy.
pub(super) async fn gate_hooked(
    call: &mut ToolCall,
    manifest: &ToolManifest,
    approval: &dyn ApprovalPolicy,
    hooks: Option<&broca_core::hooks::HookRuntime<'_>>,
    step_id: u64,
) -> Result<Option<CallPlan>, LoopError> {
    use broca_core::hooks::{Subject, Trace};
    use cortexkit_role_step_transform_provider::subscription::Phase;
    let original = call.input.clone();
    let mut trace = Trace::default();
    let metadata = |trace: &Trace, input: &Value, intentless: bool| {
        trace
            .metadata(original.clone().parse(), input.clone(), intentless)
            .map(|hooks| serde_json::to_value(hooks).expect("hook metadata serializes"))
    };
    if let Some(runtime) = hooks {
        let output = runtime
            .chain(
                broca_core::tool_results::pre_tool_subject(StepId(step_id), call, Phase::Mutate),
                trace,
                |_| true,
            )
            .await;
        trace = output.trace;
        if output.cancellation.is_some() {
            return Ok(None);
        }
        if let Subject::PreTool { input, .. } = output.subject {
            call.input = input.into();
        }
        if let Some((provider, text, unavailable)) = output.refusal {
            return Ok(Some(hook_refused(
                call,
                &trace,
                &provider,
                &text,
                unavailable,
                &metadata,
            )));
        }
    }
    if let Err(invalid) = manifest.validate_call(call) {
        return Ok(Some(CallPlan::NoDispatch(
            NoDispatchReason::ValidationFailed {
                detail: invalid.to_string(),
            },
            metadata(&trace, &(call.input).parse(), true),
        )));
    }
    match approval.decide(call).await {
        ApprovalResponse::Allow { .. } => {}
        ApprovalResponse::AllowNarrowed { input, .. } => call.input = input.into(),
        ApprovalResponse::Deny { reason, .. } => {
            return Ok(Some(CallPlan::NoDispatch(
                NoDispatchReason::Denied { reason },
                metadata(&trace, &(call.input).parse(), true),
            )));
        }
    }
    if let Some(runtime) = hooks {
        let output = runtime
            .chain(
                broca_core::tool_results::pre_tool_subject(StepId(step_id), call, Phase::Validate),
                trace,
                |_| true,
            )
            .await;
        trace = output.trace;
        if output.cancellation.is_some() {
            return Ok(None);
        }
        if let Some((provider, text, unavailable)) = output.refusal {
            return Ok(Some(hook_refused(
                call,
                &trace,
                &provider,
                &text,
                unavailable,
                &metadata,
            )));
        }
    }
    Ok(Some(CallPlan::Dispatch(metadata(
        &trace,
        &(call.input).parse(),
        false,
    ))))
}

/// The plan for a call a pre-tool hook refused: its refusal result and reason code.
pub(super) fn hook_refused(
    call: &ToolCall,
    trace: &broca_core::hooks::Trace,
    provider: &str,
    text: &str,
    unavailable: bool,
    metadata: &dyn Fn(&broca_core::hooks::Trace, &Value, bool) -> Option<Value>,
) -> CallPlan {
    let (result, reason) =
        broca_core::tool_results::pre_tool_refusal(call, provider, text, unavailable);
    CallPlan::HookTerminal {
        result,
        reason: reason.into(),
        hooks: metadata(trace, &(call.input).parse(), true),
    }
}

/// The run's hook plan, from its frozen manifest. A plan that declares hooks without a plane to
/// call them refuses the run; one whose hooks ask a person to approve a
/// call isn't supported yet.
pub(super) fn hook_plan<P, T>(
    admission: &Admission,
    seams: &Seams<'_, P, T>,
) -> Result<Option<broca_core::hooks::HookPlan>, LoopError> {
    let Some(plan) = broca_core::fresh_start::admitted_hook_plan(
        admission.manifest.as_ref(),
        seams.hook_plane.is_some(),
    )
    .map_err(LoopError::Refused)?
    else {
        return Ok(None);
    };
    if plan.approves() {
        return Err(LoopError::Unsupported(
            "hooks that ask a person to approve a call",
        ));
    }
    Ok(Some(plan))
}

/// The checks that come before the approval policy. `None` means the call reaches the policy. A
/// call a resume already knows is sent as it was, without asking the policy again.
pub(super) fn gate_call(
    call: &ToolCall,
    first: bool,
    known: &[Known],
    refusal: Option<&broca_core::tool_plane::ToolPlaneError>,
    manifest: &ToolManifest,
    removed: &std::collections::BTreeSet<String>,
) -> Option<CallPlan> {
    use broca_core::tool_plane::ToolPlaneError;
    // A repeat of an id is never sent, even when a resume knows the id: what it knows is the
    // first copy's dispatch.
    if !first {
        return Some(CallPlan::Synthetic {
            result: error_result(&call.tool_call_id, "duplicate tool_call_id from provider"),
            reason: None,
        });
    }
    if known.iter().any(|known| known.id == call.tool_call_id) {
        return Some(CallPlan::Dispatch(None));
    }
    // The applied plan's tool list still renders a tool its pending change removes, until the
    // change applies in full; the model was told not to call it, and a call is refused with the
    // same words (llm-runner `plan_notes`).
    if removed.contains(&call.tool_name) {
        return Some(CallPlan::Synthetic {
            result: error_result(
                &call.tool_call_id,
                &broca_core::plan_notes::tool_removal(&call.tool_name),
            ),
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
                result: broca_core::tool_results::route_drained_result(&call.tool_call_id, reason),
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
