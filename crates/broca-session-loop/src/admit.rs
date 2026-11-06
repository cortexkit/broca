//! A new run's admission, decided from its send's request and run control by the rules today's
//! loop calls at the same point (`broca_core::fresh_start`), so a new run starts alike on either
//! storage. Nothing here is written: a refusal leaves the session as it was.

use std::sync::Arc;

use broca_core::fresh_start;
use broca_core::provider::Provider;
use broca_core::{RunControl, RunError, RunRequest};
use broca_session_store::fold::operations::Operations;

use crate::run::Admission;

/// Whether the session follows the current compaction contract: as its runs started, or, before
/// its first run, as the first run asks.
pub fn session_contract(operations: &Operations, first_run_version: Option<u32>) -> bool {
    match operations.run.as_ref().and_then(|run| run.start.as_ref()) {
        Some(start) => start.compaction_plan_version.is_some(),
        None => first_run_version.is_some(),
    }
}

/// The admission of a new run of `req` on a session whose operational fold is `operations`.
/// The request's model, tools and input move out of `req`. Refuses what today's loop refuses
/// before it writes a new run's start, in the same order: a plan whose hooks can't run, then the
/// request's own settings, then a run on the current compaction contract whose plan names a
/// compaction provider but which has no context limit. `now_ms` is when the run is admitted.
pub fn admission<P: Provider>(
    req: &mut RunRequest,
    control: &RunControl,
    provider: &P,
    operations: &Operations,
    now_ms: Option<u64>,
) -> Result<Admission, RunError> {
    fresh_start::admitted_hook_plan(control.manifest.as_ref(), control.hook_plane.is_some())?;
    let request = fresh_start::request_config(req, provider)?;
    broca_core::compaction_v2::admission::check_context_limit(
        session_contract(operations, control.new_log_compaction_plan_version),
        control.manifest.as_deref(),
        control.admitted_context_limit,
    )?;
    let input = fresh_start::episode_input(
        std::mem::take(&mut req.input),
        control
            .input_hooks
            .as_ref()
            .and_then(|hooks| hooks.final_value.as_ref()),
    )?;
    Ok(Admission {
        run_id: req.run_id.0.clone(),
        model: request.model,
        input,
        tools: Arc::unwrap_or_clone(request.tools),
        tool_choice: request.tool_choice,
        generation: request.generation,
        response_format: request.response_format,
        provider_options: request.provider_options,
        render_config: provider.render_config_blob(),
        renderer_version: provider.renderer_version(),
        stop_when: request.stop_when,
        on_restart: req.on_restart,
        ts_ms: now_ms,
        written_at_ms: fresh_start::written_at_ms(req, || now_ms),
        manifest: control.manifest.clone(),
        // `run` decides it from the input and the session, by the shared rule.
        global_system: Vec::new(),
        context_limit: control.admitted_context_limit,
        compaction_plan_version: control.new_log_compaction_plan_version,
        // A new run takes it from its plan.
        render_tags: false,
        budget: request.budget,
        keep_warm: req.keep_warm,
        auth_selection: control.auth_selection.clone(),
        account_limit_pause: control.account_limit_pause,
    })
}
