//! The request checks `session.refresh` makes before anything reaches the session.

use cortexkit_role_llm_runner::{
    capabilities,
    plan_notes::{self, PlanNotesRefreshRequest},
};
use std::collections::BTreeSet;

/// `params` decoded as a `session.refresh` request, or its first `invalid_params` refusal: the
/// field it names, when it names one, and the message. `output_refs_advertised` is whether this
/// runner declares the `output_refs` group. broca declares `plan_notes` and not
/// `refresh_relaunch`, so the request is llm-runner's plan-notes view of the body. Nothing is
/// written for a refused request.
pub(super) fn decode(
    params: &serde_json::Value,
    output_refs_advertised: bool,
) -> Result<PlanNotesRefreshRequest, (Option<String>, String)> {
    let request: PlanNotesRefreshRequest =
        serde_json::from_value(params.clone()).map_err(|error| {
            (
                broca_wire::failing_field::<PlanNotesRefreshRequest>(params, &error),
                error.to_string(),
            )
        })?;
    match params_refusal(&request, output_refs_advertised) {
        Some((field, message)) => Err((Some(field.into()), message)),
        None => Ok(request),
    }
}

/// The `invalid_params` refusal of a `session.refresh_policy` asking for `policy`, as the field
/// it names and the message, or `None` for a policy this runner serves. broca declares
/// `plan_notes`, so `notes_until_rebuild` is served beside the rungs.
pub(super) fn policy_refusal(policy: &str) -> Option<(&'static str, String)> {
    plan_notes::check_policy(policy, &[capabilities::PLAN_NOTES])
        .err()
        .map(|field| {
            let rungs = cortexkit_role_llm_runner::rungs::ALL.join(", ");
            let notes = plan_notes::NOTES_UNTIL_REBUILD;
            (field, format!("policy must be one of {rungs}, or {notes}"))
        })
}

/// The first refusal a decoded `request` earns, as the field it names and the message, or
/// `None` when the request may go to the session.
fn params_refusal(
    request: &PlanNotesRefreshRequest,
    output_refs_advertised: bool,
) -> Option<(&'static str, String)> {
    if request.required.is_some() {
        return Some((
            "required",
            "required needs the refresh_relaunch group".into(),
        ));
    }
    if let Err(field) = plan_notes::check_policy(&request.policy, &[capabilities::PLAN_NOTES]) {
        let rungs = cortexkit_role_llm_runner::rungs::ALL.join(", ");
        return Some((
            field,
            format!(
                "policy must be one of {rungs}, or {}",
                plan_notes::NOTES_UNTIL_REBUILD
            ),
        ));
    }
    // Whether a note is required depends on the fetched system text, which the session checks
    // where it writes; its policy and size don't.
    if let Some(note) = &request.system_note {
        if request.policy != plan_notes::NOTES_UNTIL_REBUILD {
            return Some((
                "system_note",
                format!(
                    "system_note needs policy {}",
                    plan_notes::NOTES_UNTIL_REBUILD
                ),
            ));
        }
        if note.len() > plan_notes::SYSTEM_NOTE_MAX {
            return Some((
                "system_note",
                format!("system_note is over {} bytes", plan_notes::SYSTEM_NOTE_MAX),
            ));
        }
    }
    // broca declares `tool_versions` (`broca_wire::role`), so `refetch` is served once its list
    // names the plan's own providers.
    if let Some(refetch) = &request.refetch {
        if let Err(message) = check_refetch_providers(&request.plan, refetch) {
            return Some(("refetch", message));
        }
    }
    if let Err(field) = cortexkit_role_llm_runner::output_refs::check_plan(
        &serde_json::Value::Object(request.plan.clone()),
        output_refs_advertised,
    ) {
        return Some((
            field,
            "a plan item carries output_refs on a runner without the group".into(),
        ));
    }
    None
}

/// `refetch` must name at least one provider, each once, and each a provider of one of the
/// plan's tool or system-text items: those are the catalogs a refresh reads.
fn check_refetch_providers(
    plan: &serde_json::Map<String, serde_json::Value>,
    refetch: &[String],
) -> Result<(), String> {
    if refetch.is_empty() {
        return Err("refetch names no provider".into());
    }
    let read: BTreeSet<&str> = ["tool_items", "system_text_items"]
        .into_iter()
        .filter_map(|items| plan.get(items).and_then(serde_json::Value::as_array))
        .flatten()
        .filter_map(|item| item.get("provider").and_then(serde_json::Value::as_str))
        .collect();
    let mut named = BTreeSet::new();
    for provider in refetch {
        if !named.insert(provider.as_str()) {
            return Err(format!("refetch names {provider} twice"));
        }
        if !read.contains(provider.as_str()) {
            return Err(format!(
                "refetch names {provider}, which no tool or system-text item of the plan names"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::params_refusal;
    use cortexkit_role_llm_runner::plan_notes::PlanNotesRefreshRequest;
    use serde_json::json;

    fn request(extra: serde_json::Value) -> PlanNotesRefreshRequest {
        let mut body = json!({
            "plan": {
                "tool_items": [{"provider": "aft"}],
                "system_text_items": [{"provider": "prefrontal"}],
            },
            "policy": "immediately",
            "generation": 1,
        });
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(body).unwrap()
    }

    fn refused_field(extra: serde_json::Value) -> Option<&'static str> {
        params_refusal(&request(extra), false).map(|(field, _)| field)
    }

    #[test]
    fn a_plain_refresh_passes() {
        assert_eq!(refused_field(json!({})), None);
    }

    #[test]
    fn refetch_may_name_the_plans_tool_and_system_text_providers() {
        assert_eq!(refused_field(json!({"refetch": ["aft"]})), None);
        assert_eq!(
            refused_field(json!({"refetch": ["prefrontal", "aft"]})),
            None
        );
    }

    #[test]
    fn refetch_is_refused_by_field_when_its_list_is_not_the_plans() {
        for refetch in [
            json!([]),
            json!(["cerebellum"]),
            json!(["aft", "aft"]),
            json!(["aft", "other"]),
        ] {
            assert_eq!(
                refused_field(json!({"refetch": refetch})),
                Some("refetch"),
                "{refetch}"
            );
        }
    }

    /// broca declares `plan_notes`: its policy is accepted, and a system note only with it and
    /// within llm-runner's size. `required` belongs to `refresh_relaunch`, which broca doesn't
    /// declare.
    #[test]
    fn notes_until_rebuild_and_its_system_note_pass_and_misuse_is_refused_by_field() {
        assert_eq!(
            refused_field(json!({"policy": "notes_until_rebuild"})),
            None
        );
        assert_eq!(
            refused_field(json!({"policy": "notes_until_rebuild", "system_note": "Be brief."})),
            None
        );
        assert_eq!(
            refused_field(json!({"system_note": "Be brief."})),
            Some("system_note")
        );
        assert_eq!(
            refused_field(json!({
                "policy": "notes_until_rebuild",
                "system_note": "x".repeat(8_193),
            })),
            Some("system_note")
        );
        assert_eq!(refused_field(json!({"required": true})), Some("required"));
    }

    #[test]
    fn an_unknown_policy_is_refused_before_refetch() {
        assert_eq!(
            refused_field(json!({"policy": "whenever", "refetch": ["cerebellum"]})),
            Some("policy")
        );
    }
}
