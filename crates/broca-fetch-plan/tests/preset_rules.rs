//! Preset syntax is checked before any provider fetch or WAL write.
use serde_json::{json, Value};

fn plan() -> Value {
    let composition = json!({"providers": [{"provider": "aft", "tools": []}, {"provider": "prefrontal-core", "tools": []}], "step_transform_items": []});
    json!({"tool_items": [{"provider": "aft", "preset": "worker", "params": {}}], "system_text_items": [{"provider": "prefrontal-core", "preset": "worker", "params": {}}], "step_transform_items": [], "composition_digest": cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(), "composition": composition})
}

#[test]
fn absent_tool_preset_is_left_off_the_catalog_request() {
    let mut raw = plan();
    raw["tool_items"][0]
        .as_object_mut()
        .unwrap()
        .remove("preset");
    let parsed =
        broca_fetch_plan::parse_plan(&raw, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap();
    assert!(parsed.tool_items[0].preset.is_none());
    let request =
        broca_fetch_plan::admit::catalog_request(&parsed, Some(&parsed.tool_items[0]), None);
    let encoded = serde_json::to_value(request).unwrap();
    assert!(encoded.get("preset").is_none());
    assert_eq!(encoded["params"], json!({}));
    assert!(!broca_fetch_plan::names_confined_preset(&raw));
    assert!(broca_fetch_plan::names_confined_preset(&plan()));
}

#[test]
fn absent_system_text_preset_is_refused_by_name() {
    let mut raw = plan();
    raw["system_text_items"][0]
        .as_object_mut()
        .unwrap()
        .remove("preset");
    let refusal =
        broca_fetch_plan::parse_plan(&raw, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap_err();
    assert_eq!(refusal.code(), "invalid_params");
    assert_eq!(
        refusal.detail(),
        json!({"field":"plan.system_text_items[0].preset"})
    );
}

#[test]
fn daemon_preset_syntax_boundaries_are_checked_for_every_item_role() {
    for preset in ["a".to_owned(), "a".repeat(64), "az09_-".to_owned()] {
        let mut value = plan();
        value["tool_items"][0]["preset"] = json!(preset);
        broca_fetch_plan::parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap();
    }
    for role in [
        "tool_items",
        "system_text_items",
        "step_transform_items",
        "compaction_item",
    ] {
        for preset in [
            String::new(),
            "a".repeat(65),
            "A".into(),
            "a.b".into(),
            "a b".into(),
            "a\n".into(),
            "é".into(),
        ] {
            let mut value = plan();
            let item =
                json!({"provider": "aft", "preset": preset, "params": {}, "subscriptions": []});
            let field = if role == "compaction_item" {
                value[role] = item;
                "plan.compaction_item.preset".to_owned()
            } else {
                value[role] = json!([item]);
                format!("plan.{role}[0].preset")
            };
            let refusal = broca_fetch_plan::parse_plan(
                &value,
                broca_fetch_plan::CompactionSupport::Unsupported,
            )
            .unwrap_err();
            assert_eq!(refusal.code(), "invalid_params");
            assert_eq!(
                serde_json::to_string(&refusal.detail()).unwrap(),
                format!(r#"{{"field":"{field}"}}"#)
            );
        }
    }
}

#[test]
fn composition_compaction_without_an_item_is_refused_even_when_null() {
    for compaction in [Value::Null, json!({"provider": "aft"})] {
        let mut value = plan();
        value["composition"]["compaction"] = compaction;
        let refusal =
            broca_fetch_plan::parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported)
                .unwrap_err();
        assert_eq!(refusal.code(), "invalid_params");
        assert_eq!(
            serde_json::to_string(&refusal.detail()).unwrap(),
            r#"{"field":"plan.composition.compaction"}"#
        );
    }
}

#[test]
fn step_transform_items_are_accepted_with_or_without_subscriptions() {
    for subscriptions in [
        json!([]),
        json!([{
            "hook": "pre_tool",
            "phase": "mutate",
            "ops": [],
            "on_unavailable": "pass",
            "budget_ms": 1000
        }]),
        json!([{
            "hook": "pre_tool",
            "phase": "validate",
            "ops": [],
            "on_unavailable": "pass",
            "budget_ms": 1000
        }]),
    ] {
        let mut value = plan();
        let item = json!({
            "provider": "aft",
            "preset": "worker",
            "params": {},
            "subscriptions": subscriptions
        });
        value["step_transform_items"] = json!([item.clone()]);
        value["composition"]["step_transform_items"] = json!([item]);
        value["composition_digest"] = json!(
            cortexkit_role_tool_provider::catalog::composition_digest(&value["composition"])
                .unwrap()
        );

        let parsed =
            broca_fetch_plan::parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported)
                .unwrap();
        assert_eq!(parsed.step_items.len(), 1);
        assert_eq!(
            json!(parsed.step_items[0].subscriptions),
            value["step_transform_items"][0]["subscriptions"]
        );
    }
}

#[test]
fn compaction_refusal_is_preserved_for_approve_subscriptions() {
    let mut value = plan();
    let item = json!({
        "provider": "aft",
        "preset": "worker",
        "params": {},
        "subscriptions": [{"hook":"pre_tool", "phase":"approve", "ops":[], "on_unavailable":"refuse", "budget_ms":1000}]
    });
    value["step_transform_items"] = json!([item.clone()]);
    value["composition"]["step_transform_items"] = json!([item]);
    value["composition"]["compaction"] = json!({"provider": "aft"});
    value["composition_digest"] = json!(cortexkit_role_tool_provider::catalog::composition_digest(
        &value["composition"]
    )
    .unwrap());
    value["compaction_item"] = json!({
        "provider": "aft",
        "preset": "worker",
        "params": {}
    });

    // Approve support must not enable the unsupported compaction writer surface.
    let refusal =
        broca_fetch_plan::parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap_err();
    assert_eq!(refusal.code(), "invalid_params");
    assert_eq!(refusal.detail(), json!({"field": "plan.compaction_item"}));
}

fn step_plan(subscriptions: Value) -> Value {
    let mut value = plan();
    let item =
        json!({"provider":"aft", "preset":"worker", "params":{}, "subscriptions":subscriptions});
    value["step_transform_items"] = json!([item.clone()]);
    value["composition"]["step_transform_items"] = json!([item]);
    value["composition_digest"] = json!(cortexkit_role_tool_provider::catalog::composition_digest(
        &value["composition"]
    )
    .unwrap());
    value
}

#[test]
fn approve_is_admitted_after_shape_and_composition_checks() {
    let subscription = json!({"hook":"pre_tool", "phase":"approve", "ops":[], "on_unavailable":"refuse", "budget_ms":1000});
    let value = step_plan(json!([subscription]));
    broca_fetch_plan::parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported).unwrap();
    let mut malformed = value.clone();
    malformed["step_transform_items"][0]["subscriptions"][0]["ops"] = json!(["append"]);
    assert_eq!(
        broca_fetch_plan::parse_plan(&malformed, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap_err()
            .detail()["problem"],
        "ops_on_pre_tool"
    );
    let mut malformed = value;
    malformed["composition_digest"] = json!("wrong");
    assert_eq!(
        broca_fetch_plan::parse_plan(&malformed, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap_err()
            .detail(),
        json!({"field":"plan.composition_digest"})
    );
}

#[test]
fn hook_budget_cannot_exceed_the_execution_cap() {
    for (phase, on_unavailable) in [
        ("mutate", "pass"),
        ("validate", "pass"),
        ("approve", "refuse"),
    ] {
        for budget_ms in [30_000, 30_001] {
            let value = step_plan(
                json!([{"hook":"pre_tool", "phase":phase, "ops":[], "on_unavailable":on_unavailable, "budget_ms":budget_ms}]),
            );
            if budget_ms == 30_000 {
                broca_fetch_plan::parse_plan(
                    &value,
                    broca_fetch_plan::CompactionSupport::Unsupported,
                )
                .unwrap();
            } else {
                assert_eq!(
                    broca_fetch_plan::parse_plan(
                        &value,
                        broca_fetch_plan::CompactionSupport::Unsupported
                    )
                    .unwrap_err()
                    .detail(),
                    json!({"field":"plan.step_transform_items[0].subscriptions[0].budget_ms", "cap":30_000})
                );
            }
        }
    }
}

#[test]
fn user_grants_are_read_only_from_the_top_level_field() {
    let mut value = step_plan(json!([]));
    // These are malformed grants but ordinary provider data, not permission sources.
    value["tool_items"][0]["params"]["user_grants"] = json!("not grants");
    value["composition"]["user_grants"] = json!("not grants");
    value["composition_digest"] = json!(cortexkit_role_tool_provider::catalog::composition_digest(
        &value["composition"]
    )
    .unwrap());
    broca_fetch_plan::parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported).unwrap();
    value["user_grants"] = json!([{"module":"aft", "hook":"post_tool", "tools":["edit"]}]);
    broca_fetch_plan::parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported).unwrap();
    for malformed in [
        Value::Null,
        json!({}),
        json!("no"),
        json!([{"module":"aft", "hook":"post_tool"}]),
        json!([{"module":"aft", "hook":"unknown", "tools":[]}]),
    ] {
        value["user_grants"] = malformed;
        assert_eq!(
            broca_fetch_plan::parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported)
                .unwrap_err()
                .detail(),
            json!({"field":"plan.user_grants"})
        );
    }
}
