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
    BudgetConfig, GenerationConfig, ModelHandle, ResponseFormat, StopCondition, Tool, ToolChoice,
};
use broca_wal::{PlanManifest, RunConfig};

use crate::provider::Provider;
use crate::run::{RunError, RunRequest};

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
    req.generation = admitted_generation(&req.generation, provider)?;
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

fn admitted_generation<P: Provider>(
    generation: &GenerationConfig,
    provider: &P,
) -> Result<GenerationConfig, RunError> {
    if generation
        .temperature
        .is_some_and(|value| !value.is_finite() || !(0.0..=2.0).contains(&value))
    {
        return Err(RunError::Config(
            "generation.temperature must be a finite number from 0 to 2 inclusive".into(),
        ));
    }
    let mut effective = generation.clone();
    // Only the effective value belongs in the run's existing generation member. Catalog facts
    // are admission inputs, not render inputs: resuming a run or rebuilding its provider
    // request offline must not apply today's bounds again, including to older runs.
    effective.temperature = provider.admit_temperature(generation.temperature);
    Ok(effective)
}

/// When a new episode's input was written: the time the send recorded, else, for a run started
/// directly rather than from a queued send, `now` (read only then). A zero time is no time.
pub fn written_at_ms(req: &RunRequest, now: impl FnOnce() -> Option<u64>) -> Option<u64> {
    req.input_written_at_ms
        .or_else(|| req.submission_id.is_none().then(now).flatten())
        .filter(|at| *at != 0)
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
