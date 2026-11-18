/// A catalog with an OpenAI Chat row whose documented default effort is `medium`, and the same
/// row without a documented default.
fn install_default_effort_catalog() -> TestCatalog {
    let mut value = serde_json::json!({
        "source": "test", "resolved_at_ms": 0, "catalog_version": 1,
        "models": {"openai/test-default-effort": {
            "existence": "present",
            "wire.npm": {"value": "@ai-sdk/openai-compatible", "from": "provider"},
            "wire.api": {"value": "https://api.openai.com/v1", "from": "curated"},
            "limit.context": 200000, "limit.output": 64000,
            "capability.attachment": false, "capability.reasoning": true,
            "capability.reasoning_options": [{"type": "effort", "values": ["low", "medium", "high"]}],
            "capability.reasoning_default": {"type": "effort", "value": "medium"},
            "capability.tool_call": true
        }}
    });
    let mut plain = value["models"]["openai/test-default-effort"].clone();
    plain.as_object_mut().unwrap().remove("capability.reasoning_default");
    value["models"]["openai/test-no-default-effort"] = plain;
    let response = serde_json::from_value(value).unwrap();
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache
        .install_for_test(response, broca_catalog::CatalogSource::Fusiform)
        .unwrap();
    install_catalog(cache)
}

/// `session.head` names the effort a run sends when its send names no variant
/// (`default_variant`), and only then: a send that names one shows that variant alone, and a
/// model with no documented default sends no effort and shows none. With
/// `BROCA_SERVICE_TIER_SAMPLES_DIR` set, the replies are written there as reader samples.
#[tokio::test]
async fn head_names_the_default_effort_a_send_without_a_variant_sends() {
    if !run_catalog_drift_child("head_names_the_default_effort_a_send_without_a_variant_sends") {
        return;
    }
    let _catalog = install_default_effort_catalog();
    let root = fresh_vault_root("head-default-effort");
    let http = resume_http_fixture().await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    let last_body = || -> serde_json::Value {
        serde_json::from_slice(&http.requests.lock().unwrap().last().unwrap().body).unwrap()
    };
    let model = |variant: Option<&str>| {
        let mut model = serde_json::json!({"provider": "openai", "model": "test-default-effort"});
        if let Some(variant) = variant {
            model["variant"] = variant.into();
        }
        model
    };

    let sent = rig.call("session.send", serde_json::json!({"send_id": "unnamed",
        "prompt": "hello", "model": model(None)})).await;
    selection_completed(&mut rig, &tier_run(&sent)).await;
    assert_eq!(last_body()["reasoning_effort"], "medium");
    let frame = rig.call("session.head", serde_json::json!({})).await;
    tier_sample("session.head-default-effort.json", &frame.body);
    let head = tier_reply(&frame);
    assert_eq!(
        head["result"]["model"],
        serde_json::json!({"provider": "openai", "model": "test-default-effort",
            "default_variant": "medium"}),
        "{head}"
    );

    let sent = rig.call("session.send", serde_json::json!({"send_id": "named",
        "prompt": "hello", "model": model(Some("high"))})).await;
    selection_completed(&mut rig, &tier_run(&sent)).await;
    assert_eq!(last_body()["reasoning_effort"], "high");
    let frame = rig.call("session.head", serde_json::json!({})).await;
    tier_sample("session.head-named-variant.json", &frame.body);
    let head = tier_reply(&frame);
    assert_eq!(head["result"]["model"], model(Some("high")), "{head}");

    let plain = serde_json::json!({"provider": "openai", "model": "test-no-default-effort"});
    let sent = rig.call("session.send", serde_json::json!({"send_id": "no-default",
        "prompt": "hello", "model": plain})).await;
    selection_completed(&mut rig, &tier_run(&sent)).await;
    assert!(last_body().get("reasoning_effort").is_none());
    let frame = rig.call("session.head", serde_json::json!({})).await;
    tier_sample("session.head-no-effort.json", &frame.body);
    let head = tier_reply(&frame);
    assert_eq!(head["result"]["model"], plain, "{head}");
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}
