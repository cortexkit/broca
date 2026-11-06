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
use broca_wal::{ComputedTagRules, PlanManifest, RunConfig};

use crate::run::RunError;

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

/// On the current compaction contract, the system text a new episode runs with, taken out of
/// `input`. A frozen plan supplies it. Otherwise it's the text the send supplied, or, when the
/// send supplied none, the previous episode's. Different text from the previous episode's is
/// refused, since it would change every later request.
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

/// On a session outside the current compaction contract, the system messages a new episode
/// appended to the session may carry. A client naturally sends the session's system prompt again
/// on every turn, so a byte-identical repeat is stripped before it's written, keeping one
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
    config.model.provider_module_id == "import"
        && config.model.model_id == "import"
        && config
            .provider_options
            .get("import_request_digest")
            .is_some_and(serde_json::Value::is_string)
}

#[cfg(test)]
#[path = "fresh_start_tests.rs"]
mod tests;
