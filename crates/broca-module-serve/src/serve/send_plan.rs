//! Validate and fetch a send's plan before opening the session actor.

use broca_module::serve::ServeHandler;
use subc_protocol::ErrorBody;

pub(super) enum SendPlanRefusal {
    Plan(broca_fetch_plan::Refusal),
    ApproveRequiresScope,
    WorkerPresetRequiresScope,
}

impl From<broca_fetch_plan::Refusal> for SendPlanRefusal {
    fn from(refusal: broca_fetch_plan::Refusal) -> Self {
        Self::Plan(refusal)
    }
}

impl SendPlanRefusal {
    pub(super) fn wire_body(&self) -> ErrorBody {
        match self {
            Self::Plan(refusal) => {
                ErrorBody::new(refusal.code(), refusal.message()).with_detail(refusal.detail())
            }
            Self::ApproveRequiresScope => approve_scope_error(),
            Self::WorkerPresetRequiresScope => worker_scope_error(),
        }
    }
}

pub(super) fn worker_scope_error() -> ErrorBody {
    ErrorBody::new(
        "scope_required",
        "a plan with a worker tool preset requires a daemon-attested session scope",
    )
    .with_detail(serde_json::json!({
        "field": "plan.tool_items",
        "preset": broca_fetch_plan::CONFINED_TOOL_PRESET,
    }))
}

pub(super) fn approve_scope_error() -> ErrorBody {
    ErrorBody::new(
        "approve_requires_scope",
        "an approve subscription requires a daemon-attested session scope",
    )
    .with_detail(serde_json::json!({"field": "plan.step_transform_items", "phase": "approve"}))
}

/// Check the send's plan before fetching its providers or opening a session actor.
/// Later plans are only compared with the plan digest frozen by the session's first run.
pub(super) async fn admit_send_plan<H: ServeHandler + ?Sized>(
    handler: &H,
    session: &broca_module::LineageKey,
    params: &serde_json::Value,
    plan: &serde_json::Value,
    scope: Option<&broca_protocol::SessionScope>,
    model: Option<&crate::wire::ModelParams>,
    check_new_yield: impl FnOnce(&[broca_fetch_plan::PlanSender], Option<&str>) -> Result<(), String>,
) -> Result<broca_module::actor::SendPlan, SendPlanRefusal> {
    // A plan item's `output_refs` member is refused unless the admitter can freeze sessions
    // that opt into output references (llm-runner §9.13: a runner without the group refuses it).
    let output_refs = if handler
        .plan_admitter()
        .is_some_and(|admitter| admitter.admits_output_refs())
    {
        broca_fetch_plan::OutputRefsSupport::Supported
    } else {
        broca_fetch_plan::OutputRefsSupport::Unsupported
    };
    let support = if handler.compaction_supported() {
        broca_fetch_plan::CompactionSupport::Supported
    } else {
        broca_fetch_plan::CompactionSupport::Unsupported
    };
    let frozen_scope = handler.frozen_scope(session).await;
    let scope_owner = frozen_scope
        .as_ref()
        .or(scope)
        .map(|scope| scope.owner.canonical_owner_string());
    if let Some(plan) = plan.as_object() {
        broca_fetch_plan::check_plan_call_yield(plan, scope_owner.as_deref())?;
    }
    let parsed = match broca_fetch_plan::parse_plan_with(plan, support, output_refs) {
        Ok(parsed) => parsed,
        // A plan whose compaction item names no preset is refused as that, ahead of whatever
        // else the plan lacks, so the caller learns the one member every new plan must carry.
        Err(_)
            if plan.get("compaction_item").is_some_and(|item| {
                item.is_object() && !item.get("preset").is_some_and(serde_json::Value::is_string)
            }) =>
        {
            return Err(broca_fetch_plan::Refusal::invalid("plan.compaction_item.preset").into());
        }
        Err(refusal) => return Err(refusal.into()),
    };
    let plan_state = handler.plan_state(session).await;
    let opener = frozen_scope
        .as_ref()
        .and_then(broca_module::serve::scope_plan_opener)
        .or_else(|| scope.and_then(broca_module::serve::scope_plan_opener));
    cortexkit_role_llm_runner::scope_admission::check_plan_senders(
        opener.as_deref(),
        &parsed.senders,
    )
    .map_err(broca_fetch_plan::Refusal::invalid)?;
    // A new plan names its compaction preset before any other send member is weighed, so a
    // missing preset is refused as itself (llm-runner 0.8 kit `compaction_item_preset_required`).
    // A re-send of the session's frozen plan, including a legacy one without a preset, passes.
    let new_plan = match &plan_state {
        broca_module::serve::PlanState::Admitted { frozen } => {
            frozen.as_deref() != Some(parsed.identity.as_str())
        }
        _ => true,
    };
    if new_plan {
        broca_fetch_plan::require_compaction_preset(&parsed)?;
    }
    if let Some(params) = params.as_object() {
        broca_fetch_plan::check_send_members_with(params, output_refs)?;
    }
    // Filing an approval question and executing its tool call must use the scope
    // identity supplied by the daemon on the inbound route, never a caller's payload.
    // Check before fetching providers or opening an actor, so refusal writes no log record.
    if scope.is_none()
        && parsed.step_items.iter().any(|item| {
            item.subscriptions.iter().any(|subscription| {
                subscription.phase
                    == Some(cortexkit_role_step_transform_provider::subscription::Phase::Approve)
            })
        })
    {
        return Err(SendPlanRefusal::ApproveRequiresScope);
    }
    // A worker's tools are confined by the scope on its route. Without one, a provider would
    // run them under head defaults, so refuse before any fetch or log write instead of trusting
    // the caller never to send that combination.
    if scope.is_none()
        && parsed
            .tool_items
            .iter()
            .any(|item| item.preset.as_deref() == Some(broca_fetch_plan::CONFINED_TOOL_PRESET))
    {
        return Err(SendPlanRefusal::WorkerPresetRequiresScope);
    }
    // A reader build must not admit a plan it cannot execute, even when its log decoder
    // understands hook records. Refuse before fetching providers or allocating a lineage.
    if !parsed.step_items.is_empty() && !handler.hook_plane_available() {
        return Err(broca_fetch_plan::Refusal::InvalidParams {
            detail: serde_json::json!({"field": "plan.step_transform_items"}),
        }
        .into());
    }
    // Even an unchanged frozen plan can carry persona text for a different model on a later send.
    // A send that left its model to the session is checked by the session's actor instead, once
    // the inherited model is resolved; the plan travels with the send for that.
    let model_check = match model {
        Some(model) => {
            broca_fetch_plan::admit::check_plan_model(
                &parsed,
                &model.provider,
                &model.model,
                model.variant.as_deref(),
            )?;
            None
        }
        None => Some(std::sync::Arc::new(parsed.clone())),
    };
    match plan_state {
        broca_module::serve::PlanState::Admitted { frozen } => {
            // While a change is pending, a send whose plan is the effective plan or the latest
            // accepted one is not drift. Sessions that have never received a refresh still
            // refuse a send with a different plan.
            if let Some((effective, accepted)) = handler.session_change_identities(session).await {
                cortexkit_role_llm_runner::session_change::check_send_drift(
                    effective.as_deref(),
                    accepted.as_deref(),
                    &parsed.identity,
                )
                .map_err(|detail| broca_fetch_plan::Refusal::PlanDrift {
                    frozen: detail.frozen,
                    sent: detail.sent,
                })?;
            } else {
                broca_fetch_plan::check_later_plan(frozen.as_deref(), &parsed.identity)?;
            }
            Ok(broca_module::actor::SendPlan {
                identity: parsed.identity,
                manifest: None,
                model_check,
            })
        }
        broca_module::serve::PlanState::NotAdmitted | broca_module::serve::PlanState::Unknown => {
            check_new_yield(&parsed.senders, scope_owner.as_deref())
                .map_err(broca_fetch_plan::Refusal::invalid)?;
            let Some(admitter) = handler.plan_admitter() else {
                return Err(broca_fetch_plan::Refusal::InvalidParams {
                    detail: serde_json::json!({"field": "plan"}),
                }
                .into());
            };
            let admitted = admitter.admit(&parsed, session, scope).await?;
            Ok(broca_module::actor::SendPlan {
                identity: parsed.identity,
                manifest: Some(std::sync::Arc::new(admitted.manifest)),
                model_check,
            })
        }
    }
}
