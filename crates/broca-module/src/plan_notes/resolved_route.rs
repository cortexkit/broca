use broca_core::RunControl;
use broca_protocol::{session_change::NoteKind, tool_addition::ToolAdditionForm, ModelHandle};
use broca_provider::{turn_system, wire::FrozenRenderConfig};

pub(crate) fn form(model: &ModelHandle, frozen: &serde_json::Value) -> Option<ToolAdditionForm> {
    let frozen: FrozenRenderConfig = serde_json::from_value(frozen.clone()).ok()?;
    turn_system::tool_addition::frozen_form(&frozen, &model.provider_module_id)
}

pub(crate) fn text_form(frozen: &serde_json::Value) -> Option<bool> {
    serde_json::from_value::<FrozenRenderConfig>(frozen.clone())
        .ok()
        .map(|frozen| turn_system::supported(&frozen))
}

/// The input was already prepared under the previous plan. It is the accepted user input,
/// especially for a queued send, so a late apply changes system instructions and tools without
/// running the hook that prepared the user's input again.
pub fn reconcile_note_route(
    control: &mut RunControl,
    req: &mut broca_core::RunRequest,
    plan: Option<&broca_session_store::fold::operations::Plan>,
    parts: &dyn broca_session_loop::parts::Parts,
    frozen: &serde_json::Value,
) -> Result<(), broca_core::RunError> {
    use broca_session_store::record::PlanNoteKind;
    let Some(apply) = control.note_route.take() else {
        return Ok(());
    };
    let Ok(config) = serde_json::from_value::<FrozenRenderConfig>(frozen.clone()) else {
        control.plan_notes = None;
        return Ok(());
    };
    let mut proposed = control
        .plan_notes
        .iter()
        .flat_map(|delivery| &delivery.notes);
    let mut active = plan
        .into_iter()
        .flat_map(|plan| plan.latest_notes().map(|(_, note)| note));
    let supports = |native: bool, form: &str| {
        turn_system::tool_addition::carries_form(
            &config,
            &req.model.provider_module_id,
            native.then_some(form),
        )
    };
    let compatible = proposed.all(|note| supports(note.kind == NoteKind::ToolAddition, &note.form))
        && active.all(|note| supports(note.kind == PlanNoteKind::ToolAddition, &note.form));
    if compatible
        && apply
            .family
            .as_deref()
            .is_none_or(|family| frozen["family_id"] == family)
    {
        return Ok(());
    }
    use broca_fetch_plan::session_change::{Applying, EffectiveState, PendingState};
    let Some(pending) = plan.and_then(|plan| plan.pending.as_ref()) else {
        control.plan_notes = None;
        return Ok(());
    };
    let read_manifest = || broca_session_loop::run::stored_manifest(&pending.manifest, parts);
    let manifest = read_manifest()
        .or_else(|_| read_manifest())
        .map_err(|error| {
            broca_core::RunError::Transient(format!(
                "the late plan apply could not read the pending manifest: {error:?}"
            ))
        })?;
    let valid_scope =
        crate::plan_episode::require_confining_scope(Some(&manifest), req.scope.as_ref()).is_ok();
    let valid_model = broca_fetch_plan::session_change::check_applying_model(
        &PendingState {
            generation: pending.generation,
            policy: pending.policy.clone(),
            manifest: manifest.clone(),
        },
        &req.model,
        req.variant.as_deref(),
    )
    .is_ok();
    if !valid_scope || !valid_model {
        control.plan_notes = None;
        return Ok(());
    }
    let mut change = EffectiveState::of(
        &manifest,
        Applying {
            generation: pending.generation,
            applied_by: "fold",
        },
    )
    .applied_change();
    if !compatible {
        change.reason = Some("no_note_form".into());
    }
    if !control.tools_were_omitted {
        req.tools = manifest.tools.as_ref().clone();
        req.policy_allowed = req.tools.iter().map(|tool| tool.name.clone()).collect();
    }
    control.new_log_compaction_plan_version =
        crate::plan_episode::plan_compaction_version(Some(&manifest));
    control.manifest = Some(manifest);
    control.applies_refresh = Some(change);
    control.plan_notes = None;
    control.rebuild_point = true;
    Ok(())
}
