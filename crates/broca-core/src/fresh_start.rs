//! The rules that freeze a new episode's start: what a fresh run, or a new episode appended to
//! an idle session, records in its `RunStarted` and refuses before writing anything.
//!
//! Today's run loop calls each rule at the point of admission it always had, and a session
//! actor on another storage calls the same rules in the same order. That way one copy decides
//! what a new episode freezes, whichever storage holds the session. Each rule reads only what
//! it's given: the request, the run's control, and the few facts about the session so far that
//! it names. Resumes keep what the episode froze and don't come here.

use std::sync::Arc;

use broca_protocol::content::{Message, Role};
use broca_protocol::request::{
    BudgetConfig, CacheRequest, CacheTail, GenerationConfig, ModelHandle, ResponseFormat,
    StopCondition, Tool, ToolChoice,
};
use broca_protocol::RunId;
use broca_wal::{ComputedTagRules, PlanManifest, RunConfig, WalRecord};

use crate::provider::Provider;
use crate::run::{RunControl, RunError, RunRequest};

/// What a run's requests are built from: the model, its tools in request order, and the
/// generation settings. A new episode takes them from its request; a resumed one from the run
/// config its episode froze.
pub struct RequestConfig {
    /// The run's model. On a resume it is the frozen `run_config.model`, so the cross-model
    /// reasoning check compares each assistant message's recorded model with the same identity.
    pub model: ModelHandle,
    pub budget: Option<BudgetConfig>,
    pub stop_when: Vec<StopCondition>,
    pub tools: Arc<Vec<Tool>>,
    pub tool_choice: ToolChoice,
    pub generation: GenerationConfig,
    pub response_format: ResponseFormat,
    pub provider_options: serde_json::Value,
}

/// A new episode's request config, taken out of `req`: the request's tools that its policy
/// allows, in request order, and its model and generation settings. Refuses a zero step limit,
/// a zero context limit, settings the provider refuses, and a tool choice the allowed tools
/// can't satisfy, before anything is written.
pub fn request_config<P: Provider>(
    req: &mut RunRequest,
    provider: &P,
) -> Result<RequestConfig, RunError> {
    if req
        .stop_when
        .iter()
        .any(|condition| matches!(condition, StopCondition::StepCount { n: 0 }))
    {
        return Err(RunError::Config(
            "stop_when[].n must be at least 1 for type 'step_count'".into(),
        ));
    }
    if req.context_limit == Some(0) {
        return Err(RunError::Config(
            "context_limit=0 is invalid; omit context_limit to use the catalog value, or supply a value of at least 1"
                .into(),
        ));
    }
    if let Some(message) = provider.fresh_admission_config_error(&req.generation, &req.tool_choice)
    {
        return Err(RunError::Config(message));
    }
    let mut tools = std::mem::take(&mut req.tools);
    tools.retain(|tool| req.policy_allowed.contains(&tool.name));
    if matches!(&req.tool_choice, ToolChoice::Required) && tools.is_empty() {
        return Err(RunError::Config(
            "tool_choice 'required' needs at least one policy-allowed tool".into(),
        ));
    }
    if let ToolChoice::Tool { name } = &req.tool_choice {
        if !tools.iter().any(|t| t.name == *name) {
            return Err(RunError::Config(format!(
                "tool_choice names '{name}' which is not among the policy-allowed tools"
            )));
        }
    }
    // `cache.tail = volatile` tells the renderer to withhold the cache breakpoint on the final
    // message. On a run that can call tools that breakpoint is not waste, it is the entire
    // mechanism: each step re-sends the previous step's messages, so the anchor one step plants
    // on its last message is exactly what the next step reads back. Honouring the declaration
    // there would quietly make caching WORSE on the one leg where it already pays off, and
    // quietly is the problem -- so refuse, and name which of the two the caller should drop.
    //
    // Judged on the tools actually admitted (after the policy intersection) rather than the
    // ones requested: a run whose tools were all filtered out makes exactly one provider call,
    // and the declaration is sound for it.
    if tail_is_volatile(req.cache.as_ref()) && !tools.is_empty() {
        return Err(RunError::Config(
            "cache.tail 'volatile' conflicts with tools: a tool-bearing run re-sends each step's \
             messages, so the final-message breakpoint it withholds is what the next step reads \
             back; drop cache.tail or drop tools"
                .into(),
        ));
    }
    Ok(RequestConfig {
        model: std::mem::replace(&mut req.model, ModelHandle::new("", "")),
        budget: std::mem::take(&mut req.budget),
        stop_when: std::mem::take(&mut req.stop_when),
        tools: Arc::new(tools),
        tool_choice: std::mem::take(&mut req.tool_choice),
        generation: std::mem::take(&mut req.generation),
        response_format: ResponseFormat::default(),
        provider_options: serde_json::Value::Null,
    })
}

/// Whether the caller declared its final message varies between requests. The refusal above is
/// the only consumer in the loop; the placement it implies is resolved and frozen by the module
/// before the run starts, so nothing here reads it again on a resume.
fn tail_is_volatile(cache: Option<&CacheRequest>) -> bool {
    matches!(
        cache.and_then(|cache| cache.tail),
        Some(CacheTail::Volatile)
    )
}

/// A new episode's frozen run config: its request config, the request's routing and directive
/// fields, the run control's admission choices, and the provider's renderer identity, which a
/// resume checks before rendering again.
pub fn run_config<P: Provider>(
    req: &RunRequest,
    control: &RunControl,
    provider: &P,
    request: &RequestConfig,
    tools: Arc<Vec<Tool>>,
    global_system: Option<Arc<Vec<Message>>>,
    (render_tags, computed_tags): (bool, Option<ComputedTagRules>),
) -> RunConfig {
    RunConfig {
        global_system,
        route_decision_id: req.route_decision_id.clone(),
        account_route: req.account_route.clone(),
        account_limit_pause: control.account_limit_pause,
        turn_directives_v1: req.directives.clone(),
        render_tags,
        computed_tags,
        model: request.model.clone(),
        tools,
        tool_choice: request.tool_choice.clone(),
        generation: request.generation.clone(),
        response_format: request.response_format.clone(),
        provider_options: request.provider_options.clone(),
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: provider.renderer_version(),
        frozen_render_config: provider.render_config_blob(),
        // Freeze the resolved credential id so a resume retrieves the same credential, never
        // choosing its method again from configuration or account routing. The frozen
        // credential id is metadata, not a provider render input.
        auth_selection: control.auth_selection.clone(),
        tools_were_omitted: control.tools_were_omitted,
        budget: request.budget.clone(),
        context_limit: control.admitted_context_limit,
        work_class: req.work_class.clone(),
        variant: req.variant.clone(),
        service_tier: req.service_tier,
        stop_when: request.stop_when.clone(),
        // Before any record is written, accepting the send resolves context management into
        // req.context_management: the session's inherited mode, or on its first send the
        // caller's choice, project/global broca.jsonc defaults, then "none", in that order.
        context_management: req.context_management.clone(),
    }
}

/// What a new episode's run start records beside its request and run control.
pub struct EpisodeStart {
    pub run_id: RunId,
    /// Whether the session follows the current compaction contract: its first run recorded a
    /// compaction plan version (`RunStarted.compaction_plan_version`).
    pub mode3: bool,
    /// The cache marker order: the session's, or the current one on a session's first episode.
    pub marker_order_version: Option<u32>,
    /// When the run was admitted. The caller reads the clock once and also hands this value to
    /// `crate::refusal_fallback::on_episode_admitted`, which decides whether a content-filter
    /// fallback schedule carries into this run, so the live run and replay decide from the same
    /// time.
    pub admitted_at_ms: Option<u64>,
    pub config: Box<RunConfig>,
    pub input: Arc<Vec<Message>>,
}

/// When a new episode's input was written: the time the send recorded, else, for a run started
/// directly rather than from a queued send, `now` (read only then). A zero time is no time.
pub fn written_at_ms(req: &RunRequest, now: impl FnOnce() -> Option<u64>) -> Option<u64> {
    req.input_written_at_ms
        .or_else(|| req.submission_id.is_none().then(now).flatten())
        .filter(|at| *at != 0)
}

/// A new episode's `RunStarted`: one record carrying its config and input, so a torn start never
/// leaves one without the other. A resume treats it as the run's authority.
pub fn run_started(req: &RunRequest, control: &RunControl, start: EpisodeStart) -> WalRecord {
    WalRecord::RunStarted {
        compaction_plan_version: start
            .mode3
            .then_some(cortexkit_compaction_view::PLAN_VERSION),
        sent_by: control.display_sent_by.clone(),
        written_at_ms: written_at_ms(req, || control.now_ms()),
        tags: None,
        session_class: req.session_class.clone(),
        retention: req.retention,
        hooks: control.input_hooks.clone().map(|mut hooks| {
            // RunStarted.input holds the rendered final value, unlike Queued.request.
            hooks.final_value = None;
            hooks
        }),
        on_restart: req.on_restart,
        keep_warm: req.keep_warm,
        marker_order_version: start.marker_order_version,
        // Must-understand: a manifest makes this a gated frame requiring `plan-manifest/v1`, and
        // the WAL raises the feature floor to that name before writing it.
        manifest: control.manifest.as_deref().cloned().map(Box::new),
        // Must-understand: the WAL raises the feature floor before this frame is written, and a
        // scoped record is always framed as a gated frame requiring `scope/v1`.
        scope: req.scope.clone(),
        send_id: req.send_id.clone(),
        request_identity_digest: req.request_identity_digest.clone(),
        // Informational: recorded for dashboards and never read back into a request.
        title: req.title.clone(),
        project_id: req.project_id.clone(),
        principal: req.principal.clone(),
        ts_ms: start.admitted_at_ms,
        run_id: start.run_id,
        session: req.session.clone(),
        config: start.config,
        input: start.input,
        origin: broca_wal::RunOrigin::Fresh,
        // If this run consumes a durable queued submission, its id marks the submission
        // consumed: the pending queue excludes a submission once a RunStarted names it, so a
        // crash after this record's barrier never drains it again. `None` for a run started
        // directly.
        submission_id: req.submission_id.clone(),
    }
}

/// Whether a new episode shows runner tags to the model, and the computed numbering rules it is
/// admitted under. It renders tags when its plan asks: the plan the send carries, else the one
/// the session froze. Its checkpoint is the highest number the session has already shown, so
/// numbering continues without renumbering anything a reader has seen.
pub fn admitted_tags(
    plan: Option<&PlanManifest>,
    checkpoint: u64,
) -> (bool, Option<ComputedTagRules>) {
    (
        plan.is_some_and(crate::tags::plan_renders_tags),
        Some(ComputedTagRules { checkpoint }),
    )
}

/// The hook plan of the plan a new episode freezes, when it declares hooks. A plan the hook
/// runtime can't read is refused, and so is one that needs hooks when no hook plane (the
/// connection that sends hook calls to the modules providing them) is installed: the caller hears
/// the refusal and nothing is written or sent.
pub fn admitted_hook_plan(
    plan: Option<&Arc<PlanManifest>>,
    hook_plane_installed: bool,
) -> Result<Option<crate::hooks::HookPlan>, RunError> {
    let hook_plan = plan
        .filter(|manifest| crate::hooks::has_transforms(manifest))
        .map(|manifest| crate::hooks::HookPlan::from_manifest(manifest))
        .transpose()
        .map_err(RunError::Wal)?;
    if hook_plan.is_some() && !hook_plane_installed {
        return Err(RunError::Wal(
            "plan.step_transform_items requires an installed hook plane".into(),
        ));
    }
    Ok(hook_plan)
}

/// The input a new episode starts with: the request's, or the final value its input hooks
/// recorded, which replaces it.
pub fn episode_input(
    input: Vec<Message>,
    final_value: Option<&serde_json::Value>,
) -> Result<Vec<Message>, RunError> {
    match final_value {
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|error| RunError::Wal(format!("invalid recorded input hooks: {error}"))),
        None => Ok(input),
    }
}

/// On a session under the current compaction contract (its first run recorded a compaction plan
/// version), the system text a new episode runs with, taken out of `input`. A frozen plan
/// supplies it. Otherwise it's the text the send supplied, or, when the send supplied none, the
/// previous episode's. Different text from the previous episode's is refused, since it would
/// change every later request.
pub fn new_episode_globals(
    input: &mut Vec<Message>,
    plan: Option<&PlanManifest>,
    previous: Option<&[Message]>,
) -> Result<Vec<Message>, RunError> {
    let supplied = crate::compaction_v2::systems::take_globals(input);
    if let Some(manifest) = plan {
        return Ok(crate::compaction_v2::systems::from_manifest(manifest));
    }
    if supplied.is_empty() {
        return Ok(previous.map(<[Message]>::to_vec).unwrap_or_default());
    }
    if previous.is_some_and(|old| old != supplied.as_slice()) {
        return Err(RunError::SystemPromptChanged);
    }
    Ok(supplied)
}

/// On a session whose first run recorded no compaction plan version, the system messages a new
/// episode appended to the session may carry. A client naturally sends the session's system prompt
/// again on every turn, so a byte-identical repeat is stripped before it's written, keeping one
/// instruction and a cache-stable prefix. Every system message renders into the top-level system
/// field, so all of them must be repeats. A session whose earlier history is an import has no
/// system prompt of its own yet, and the first client to continue the session sets it. Anything
/// else is refused.
pub fn append_system_messages(
    input: &mut Vec<Message>,
    prefix: &[Message],
    prior_is_import: bool,
) -> Result<(), RunError> {
    if crate::run::first_system_index(input).is_none() {
        return Ok(());
    }
    match crate::run::first_system_message(prefix) {
        Some(_) if crate::run::repeats_prefix_system_messages(prefix, input) => {
            input.retain(|message| message.role != Role::System);
            Ok(())
        }
        None if prior_is_import => Ok(()),
        Some(_) | None => Err(RunError::SystemPromptChanged),
    }
}

/// Whether `config` is the run start an import wrote, rather than a run's.
pub fn is_import(config: &RunConfig) -> bool {
    is_import_start(&config.model, &config.provider_options)
}

/// Whether a run start with this model and these provider options is an import's: an import
/// records the placeholder model `import/import` and the digest of the request it imported.
pub fn is_import_start(model: &ModelHandle, provider_options: &serde_json::Value) -> bool {
    model.provider_module_id == "import"
        && model.model_id == "import"
        && provider_options
            .get("import_request_digest")
            .is_some_and(serde_json::Value::is_string)
}

#[cfg(test)]
#[path = "fresh_start_tests.rs"]
mod tests;
