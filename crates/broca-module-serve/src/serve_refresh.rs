//! The request checks `session.refresh` makes before anything reaches the session.

use cortexkit_role_llm_runner::session_change::{check_refetch, RefreshRequest};

/// The first `invalid_params` refusal `request` earns, as the field it names and the message,
/// or `None` when the request may go to the session. `output_refs_advertised` is whether this
/// runner declares the `output_refs` group. Nothing is written for a refused request.
pub(super) fn params_refusal(
    request: &RefreshRequest,
    output_refs_advertised: bool,
) -> Option<(&'static str, String)> {
    if let Err(field) = request.check_policy() {
        let rungs = cortexkit_role_llm_runner::rungs::ALL.join(", ");
        return Some((field, format!("policy must be one of {rungs}")));
    }
    // broca does not declare `tool_versions`, so a request for fresh catalog reads is refused
    // instead of being accepted and silently not done.
    if let Err(field) = check_refetch(request.refetch.as_deref(), false) {
        return Some((field, "refetch requires tool_versions".into()));
    }
    if let Err(field) = request.check_output_refs(output_refs_advertised) {
        return Some((
            field,
            "a plan item carries output_refs on a runner without the group".into(),
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::params_refusal;
    use cortexkit_role_llm_runner::session_change::RefreshRequest;
    use serde_json::json;

    fn request(extra: serde_json::Value) -> RefreshRequest {
        let mut body = json!({"plan": {}, "policy": "immediately", "generation": 1});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(body).unwrap()
    }

    #[test]
    fn a_plain_refresh_passes() {
        assert_eq!(params_refusal(&request(json!({})), false), None);
    }

    #[test]
    fn refetch_is_refused_by_field_without_tool_versions() {
        for providers in [json!(["aft"]), json!([])] {
            let refused = params_refusal(&request(json!({"refetch": providers})), true);
            assert_eq!(refused.map(|(field, _)| field), Some("refetch"));
        }
    }

    #[test]
    fn an_unknown_policy_is_refused_before_refetch() {
        let refused = params_refusal(
            &request(json!({"policy": "whenever", "refetch": ["aft"]})),
            false,
        );
        assert_eq!(refused.map(|(field, _)| field), Some("policy"));
    }
}
