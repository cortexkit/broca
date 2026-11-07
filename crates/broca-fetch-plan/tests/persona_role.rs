use broca_fetch_plan::admit::check_plan_model;
use broca_fetch_plan::plan::{parse_plan, plan_identity};
use serde_json::{json, Value};

fn role_plan(role: &str) -> Value {
    let composition = json!({
        "providers": [{"provider": "prefrontal-core", "tools": []}],
        "step_transform_items": []
    });
    let mut params = json!({});
    if role == "head" {
        params["model"] = json!({"provider": "anthropic", "id": "head-model", "variant": "max"});
    }
    json!({
        "tool_items": [{"provider": "prefrontal-core", "preset": role, "params": {}}],
        "system_text_items": [{"provider": "prefrontal-core", "preset": role, "params": params}],
        "step_transform_items": [],
        "composition_digest": plan_identity(&composition).unwrap(),
        "composition": composition
    })
}

#[test]
fn head_and_worker_persona_text_is_never_optional() {
    for role in ["head", "worker"] {
        let mut value = role_plan(role);
        parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported).unwrap();
        value["system_text_items"][0]["optional"] = json!(true);
        let refusal =
            parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported).unwrap_err();
        assert_eq!(refusal.code(), "invalid_params", "{role}");
        assert_eq!(
            refusal.detail(),
            json!({"field": "plan.system_text_items[0].optional"}),
            "{role}"
        );
    }
}

#[test]
fn persona_model_is_checked_only_when_the_item_carries_one() {
    let head = parse_plan(
        &role_plan("head"),
        broca_fetch_plan::CompactionSupport::Unsupported,
    )
    .unwrap();
    check_plan_model(&head, "anthropic", "head-model", Some("max")).unwrap();
    let refusal = check_plan_model(&head, "anthropic", "other-model", Some("max")).unwrap_err();
    assert_eq!(refusal.code(), "plan_model_mismatch");
    assert_eq!(
        refusal.detail(),
        json!({
            "provider": "prefrontal-core",
            "plan_model": {"provider": "anthropic", "id": "head-model", "variant": "max"},
            "send_model": {"provider": "anthropic", "id": "other-model", "variant": "max"}
        })
    );
    let worker = parse_plan(
        &role_plan("worker"),
        broca_fetch_plan::CompactionSupport::Unsupported,
    )
    .unwrap();
    check_plan_model(&worker, "unrelated-provider", "unrelated-model", None).unwrap();
}

/// A tool item's `params` are its provider's own. The tool-catalog model hint
/// (`{provider, model}`, tool-provider `tool-catalog-model/v1`) on a head plan's tool item is
/// passed through unread, and so is any other `params.model` on a tool item. Only the persona
/// system-text item is held to the send's model.
#[test]
fn tool_item_params_model_passes_through_unchecked() {
    for tool_params in [
        // A tool item's params as a plan composer sends them for a head: the tool-catalog hint
        // beside the provider's other settings.
        json!({
            "behavior": "interactive",
            "model": {"model": "claude-opus-5-5", "provider": "anthropic"},
            "scope": "all",
            "tool_descs": "full"
        }),
        // Even the persona form naming another model is the tool provider's own value.
        json!({"model": {"provider": "other", "id": "another-model"}}),
    ] {
        let mut value = role_plan("head");
        value["tool_items"][0]["params"] = tool_params;
        let head = parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported).unwrap();
        check_plan_model(&head, "anthropic", "head-model", Some("max")).unwrap();
        // The persona system-text item is still checked on the same plan.
        let refusal = check_plan_model(&head, "anthropic", "other-model", Some("max")).unwrap_err();
        assert_eq!(refusal.code(), "plan_model_mismatch");
    }
}

#[test]
fn two_core_system_text_items_are_refused_regardless_of_preset() {
    for role in ["head", "worker"] {
        let mut value = role_plan(role);
        value["system_text_items"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "provider": "prefrontal-core", "preset": "reader", "params": {}
            }));
        let refusal =
            parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported).unwrap_err();
        assert_eq!(refusal.code(), "invalid_params", "{role}");
        assert_eq!(
            refusal.detail(),
            json!({"field": "plan.system_text_items"}),
            "{role}"
        );
    }
}

#[test]
fn legacy_persona_text_is_accepted_but_cannot_be_optional() {
    let mut value = role_plan("head");
    value["system_text_items"][0]["preset"] = json!("persona");
    parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported).unwrap();
    value["system_text_items"][0]["optional"] = json!(true);
    assert_eq!(
        parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap_err()
            .detail(),
        json!({"field": "plan.system_text_items[0].optional"})
    );
}
