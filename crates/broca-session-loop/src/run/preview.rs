use super::*;

/// Find still-renderable note anchors for the next run's delivery decision. This is a
/// preview of the selected view, not permission to insert a note partway through a run.
pub async fn next_note_anchors<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    bind: &broca_protocol::BindIdentity,
) -> Result<std::collections::BTreeSet<u64>, LoopError> {
    next_note_anchors_for(writer, parts, bind, None).await
}

pub async fn next_note_anchors_for<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    bind: &broca_protocol::BindIdentity,
    route: Option<(&ModelHandle, &Value)>,
) -> Result<std::collections::BTreeSet<u64>, LoopError> {
    let Some(plan) = writer
        .operations()
        .plan
        .as_ref()
        .filter(|plan| plan.has_rendered_notes())
    else {
        return Ok(Default::default());
    };
    let step = writer
        .operations()
        .run
        .as_ref()
        .map_or(1, |run| run.completed_steps + 1);
    let (opts, admission) = Box::pin(base_request(
        writer,
        parts,
        bind,
        step,
        true,
        true,
        Some(route),
    ))
    .await?;
    let mut frozen: broca_provider::wire::FrozenRenderConfig =
        serde_json::from_value(admission.render_config.as_ref().clone())
            .map_err(|error| LoopError::Parts(format!("the next plan-note route: {error}")))?;
    // Until routing identifies the next renderer, don't assume it will echo reasoning.
    // Keeping a spare delivery does not change the request when the original still renders.
    if route.is_none() && frozen.family_id != broca_provider::spec::WireFamilyId::AnthropicMessages
    {
        return Ok(Default::default());
    }
    if route.is_none() {
        frozen.reasoning = broca_provider::policy::ReasoningPolicy::None;
    }
    let visible = plan_notes::rendering_anchors(Some(&frozen), &opts, false);
    Ok(plan
        .notes
        .iter()
        .map(|delivery| delivery.anchor)
        .filter(|anchor| visible.contains(anchor))
        .collect())
}

/// A synthetic next request uses the prospective view but does not write a run or a delivery.
pub async fn offline_next_media_request<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    bind: &broca_protocol::BindIdentity,
    step: u64,
) -> Result<CallOptions, LoopError> {
    let (opts, admission) = base_request(writer, parts, bind, step, true, true, None).await?;
    let opts = project_step(writer, &admission, step, opts)?;
    let route = broca_core::provider_errors::recorded_render_route(
        &admission.render_config,
        opts.target_model.as_ref(),
    );
    if broca_core::provider_errors::legacy_anthropic_echo_on_route(route) {
        return Err(LoopError::Unsupported(
            broca_core::provider_errors::LEGACY_ANTHROPIC_ECHO,
        ));
    }
    Ok(opts)
}

pub(super) async fn offline_request_for<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    bind: &broca_protocol::BindIdentity,
    step_id: u64,
    defer_media_tags: bool,
) -> Result<CallOptions, LoopError> {
    let (opts, admission) =
        base_request(writer, parts, bind, step_id, defer_media_tags, false, None).await?;
    let opts = project_step(writer, &admission, step_id, opts)?;
    let route = broca_core::provider_errors::recorded_render_route(
        &admission.render_config,
        opts.target_model.as_ref(),
    );
    if broca_core::provider_errors::legacy_anthropic_echo_on_route(route) {
        return Err(LoopError::Unsupported(
            broca_core::provider_errors::LEGACY_ANTHROPIC_ECHO,
        ));
    }
    Ok(opts)
}

fn project_step<W: Writer>(
    writer: &W,
    admission: &Admission,
    step: u64,
    opts: CallOptions,
) -> Result<CallOptions, LoopError> {
    let has_notes = writer
        .operations()
        .plan
        .as_ref()
        .is_some_and(|plan| plan.has_rendered_notes());
    let source = if has_notes {
        plan_notes::project(writer, admission, opts.clone())?
    } else {
        opts.clone()
    };
    if let Some(route) = crate::refusal::recorded_route(
        writer.operations(),
        &admission.render_config,
        step,
        &source,
    )? {
        let fallback = plan_notes::project_for(writer, admission, opts, Some(&route.model))?;
        if plan_notes::fallback_carries(writer, admission)
            && crate::refusal::carries_notes(&admission.render_config, &fallback)
        {
            return Ok(fallback);
        }
    }
    Ok(source)
}

async fn base_request<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    bind: &broca_protocol::BindIdentity,
    step_id: u64,
    defer_media_tags: bool,
    prospective_view: bool,
    prospective_run: Option<Option<(&ModelHandle, &Value)>>,
) -> Result<(CallOptions, Admission), LoopError> {
    refuse_first_protocol_compaction(writer)?;
    let run = writer
        .operations()
        .run
        .as_ref()
        .ok_or(LoopError::Unsupported("a session with no run"))?;
    let mut admission = frozen_admission(run, parts)?;
    if let Some(Some((model, config))) = prospective_run {
        admission.model = model.clone();
        admission.render_config = Arc::new(config.clone());
    }
    let compacted = match (
        admission.compaction_plan_version,
        admission.manifest.as_deref(),
    ) {
        (Some(_), Some(manifest)) => broca_core::compaction::item(manifest)
            .map_err(LoopError::Parts)?
            .is_some(),
        _ => false,
    };
    let episodes = if prospective_run.is_some()
        && admission.render_config["family_id"] == "anthropic_messages"
    {
        let config = Arc::make_mut(&mut admission.render_config);
        config["renderer_version"] = broca_provider::families::ANTHROPIC_RENDERER_VERSION.into();
        if prospective_run == Some(None) && config["reasoning"]["mode"] == "anthropic_interleaved" {
            config["reasoning"]["echo_scope"] =
                broca_core::provider_errors::ANTHROPIC_ECHO_SCOPE.into();
        }
        let mut history = RenderHistory::from(render_episodes(writer, parts)?);
        history.signed_floor =
            signed_floor::preview_floor(writer, parts, &admission.model, config).await?;
        Arc::new(history)
    } else {
        Arc::new(render_history(writer, parts, &mut ToolSetCache::default()).await?)
    };
    let run_rules = run_tag_rules(writer, &episodes, &mut TagRulesCache::default()).await?;
    let tools = Arc::clone(&admission.tools);
    let enrollment = writer.enrollment().await?;
    let mut opts = call_options(
        writer,
        &admission,
        &tools,
        &episodes,
        &run_rules,
        None,
        step_id,
        compacted,
        defer_media_tags,
        enrollment.as_ref(),
        None,
        None,
    )?;
    if crate::responses_reasoning::ResponsesReasoningRemovals::applies(&admission.render_config) {
        writer
            .responses_reasoning_removals(&admission.run_id, None)
            .await?
            .apply(&mut opts, step_id);
    }
    crate::budget::finalize_tool_choice(writer.operations(), &mut opts);
    if !compacted {
        let durable_system: Vec<Message> = opts.prompt.first().cloned().into_iter().collect();
        let opts = with_plan_system(writer, &admission, &durable_system, opts);
        return Ok((opts, admission));
    }
    let opts = crate::compaction::offline_prompt_with_view(
        writer,
        &admission,
        bind,
        step_id,
        opts,
        prospective_view,
    )
    .await?;
    Ok((opts, admission))
}
