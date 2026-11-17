//! A new run's admission, decided from its send's request and run control by the rules in
//! `broca_core::fresh_start`. The run loop on today's storage (`broca_core::run`) calls the same
//! rules at the same point, so a new run starts alike on either storage. Nothing here is written:
//! a refusal leaves the session as it was.

use std::sync::Arc;

use broca_core::fresh_start;
use broca_core::provider::Provider;
use broca_core::{RunControl, RunError, RunRequest};
use broca_session_store::fold::operations::Operations;

use crate::run::{Admission, SendFacts};

/// Whether a new run is on the current compaction contract: the session already is (its latest
/// run started on it, [`Operations::on_contract`]), or the plan the run freezes names a
/// compaction provider, which `plan_version` says (the caller sets it from that plan). A session
/// that ran outside the contract moves onto it at the first run whose plan names one, and never
/// moves back, since that provider can't be removed again (see `crate::enroll`).
pub fn session_contract(operations: &Operations, plan_version: Option<u32>) -> bool {
    operations.on_contract() || plan_version.is_some()
}

/// The admission of a new run of `req` on a session whose operational fold is `operations`.
/// The request's model, tools and input move out of `req`. Refuses, before anything is written,
/// what `broca_core::run` refuses before it writes a new run's start, in the same order: a plan
/// whose hooks can't run, then the request's own settings, then a run on the current compaction
/// contract whose plan names a compaction provider but which has no context limit. `now_ms` is
/// when the run is admitted.
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
        tools: request.tools,
        tool_choice: request.tool_choice,
        generation: request.generation,
        response_format: Arc::new(request.response_format),
        provider_options: request.provider_options,
        render_config: Arc::new(provider.render_config_blob()),
        renderer_version: provider.renderer_version(),
        stop_when: request.stop_when,
        on_restart: req.on_restart,
        ts_ms: now_ms,
        written_at_ms: fresh_start::written_at_ms(req, || now_ms),
        manifest: control.manifest.clone(),
        transport: Default::default(),
        // `run` decides it from the input and the session, by the shared rule.
        global_system: Arc::default(),
        context_limit: control.admitted_context_limit,
        compaction_plan_version: control.new_log_compaction_plan_version,
        // A new run takes it from its plan.
        render_tags: false,
        budget: request.budget,
        keep_warm: req.keep_warm.clone(),
        auth_selection: control.auth_selection.clone(),
        account_limit_pause: control.account_limit_pause,
        route_decision_id: req.route_decision_id.clone(),
        account_route: req.account_route.clone(),
        served_provider: control.served_provider.clone(),
        service_tier: req.service_tier,
        variant: req.variant.clone(),
        work_class: req.work_class.clone(),
        context_management: req.context_management.clone(),
        directives: req.directives.clone(),
        tools_were_omitted: control.tools_were_omitted,
        // The session's first run takes the current order; a later one follows its previous
        // run's (see `marker_order_for_next_run`).
        marker_order_version: match operations.run.as_ref().and_then(|run| run.start.as_ref()) {
            Some(previous) => marker_order_for_next_run(crate::run::frozen_marker_order(previous)),
            None => Some(crate::run::MARKER_ORDER_VERSION),
        },
        send: SendFacts {
            submission_id: req.submission_id.as_ref().map(|id| id.0.clone()),
            send_id: req.send_id.clone(),
            request_identity_digest: req.request_identity_digest.clone(),
            title: req.title.clone(),
            session_class: req.session_class.clone(),
            retention_secs: req.retention,
            principal: req.principal.clone(),
            sent_by: control.display_sent_by.clone(),
            scope: req.scope.clone(),
            // The input already holds the hooks' final value; the record keeps the rest.
            hooks: control.input_hooks.as_deref().cloned().map(|mut hooks| {
                hooks.final_value = None;
                hooks
            }),
            message_marks: control.display_message_marks.clone(),
        },
        pre_model_error: control.pre_model_error.clone(),
        applies_refresh: control.applies_refresh.clone(),
        head_turn: req.head_turn,
    })
}

/// The marker order a session's next run freezes, given the order its previous run froze.
/// Version 2 plans as version 1 except when a request ends with a message that carries no
/// block, where it adds the tail marker version 1 left out. A marker changes no prompt token, so
/// moving a session from 1 to 2 between runs loses no cached prefix, and each run still renders
/// every request under the one order it froze. A run frozen with no order (written before
/// orders existed) keeps none: that legacy placement marks other messages than orders 1 and 2
/// (see `plan_markers`), and a switch could leave the new markers beyond the provider's lookback
/// from the prefix the session last wrote.
pub(crate) fn marker_order_for_next_run(previous: Option<u32>) -> Option<u32> {
    match previous {
        Some(1) => Some(crate::run::MARKER_ORDER_VERSION),
        other => other,
    }
}

#[cfg(test)]
mod marker_order_tests {
    use super::marker_order_for_next_run;
    use crate::run::MARKER_ORDER_VERSION;

    /// A session on order 1 moves to the current order at its next run; one on the current
    /// order stays, and one written before orders existed keeps none.
    #[test]
    fn a_session_on_order_one_moves_to_the_current_order_at_its_next_run() {
        assert_eq!(
            marker_order_for_next_run(Some(1)),
            Some(MARKER_ORDER_VERSION)
        );
        assert_eq!(
            marker_order_for_next_run(Some(MARKER_ORDER_VERSION)),
            Some(MARKER_ORDER_VERSION)
        );
        assert_eq!(marker_order_for_next_run(None), None);
    }
}
