use serde_json::Value;

/// Add the unchanged routing plane from a bounded Fusiform capture to a rig's authored
/// capability/limit rows. Missing capture entries fail the rig setup rather than inventing a
/// package, endpoint or backend tier. Synthetic models must state their own wire facts.
pub fn with_served_routing(mut response: Value) -> Value {
    let capture: Value =
        serde_json::from_str(include_str!("../fixtures/fusiform_rig_routing.json")).unwrap();
    for (id, row) in response["models"].as_object_mut().unwrap() {
        let row = row.as_object_mut().unwrap();
        if let Some(facts) = capture["models"].get(id) {
            row.extend(facts.as_object().unwrap().clone());
        } else {
            assert!(
                row.contains_key("wire.npm") && row.contains_key("wire.api"),
                "rig model '{id}' needs captured routing facts or an explicit synthetic route"
            );
        }
    }
    response
}

#[test]
fn daemon_rig_routes_use_served_packages_endpoints_and_backend_tiers() {
    let capture: Value =
        serde_json::from_str(include_str!("../fixtures/fusiform_rig_routing.json")).unwrap();
    let mut rows = serde_json::Map::new();
    for id in capture["models"].as_object().unwrap().keys() {
        rows.insert(id.clone(), serde_json::json!({"existence":"present"}));
    }
    let response = with_served_routing(serde_json::json!({
        "source":"fixture", "catalog_version":1, "resolved_at_ms":1, "models":rows
    }));
    let cache = broca_catalog::CatalogCache::empty();
    cache
        .refresh(serde_json::from_value(response).unwrap())
        .unwrap();
    let catalog = cache.snapshot().unwrap();
    use broca_provider::spec::WireFamilyId;
    for (provider, model, family) in [
        (
            "anthropic",
            "claude-opus-4-8",
            WireFamilyId::AnthropicMessages,
        ),
        (
            "anthropic",
            "claude-opus-5",
            WireFamilyId::AnthropicMessages,
        ),
        ("openai", "gpt-4o-mini", WireFamilyId::OpenAiResponses),
        ("openai", "gpt-5.5", WireFamilyId::OpenAiResponses),
        ("xai", "grok-4.7", WireFamilyId::OpenAiResponses),
        ("openrouter", "openai/gpt-4.1", WireFamilyId::OpenAiChat),
        ("google", "gemini-2.5-flash", WireFamilyId::Gemini),
        ("meta", "muse-spark-1.1", WireFamilyId::OpenAiResponses),
        (
            "meta",
            "muse-spark-1.3-contributor",
            WireFamilyId::OpenAiResponses,
        ),
    ] {
        let (actual, endpoint) = catalog.resolve_route(provider, model).unwrap();
        assert_eq!(actual, family, "{provider}/{model}");
        assert!(
            endpoint.starts_with("https://"),
            "{provider}/{model}: {endpoint}"
        );
    }
    assert_eq!(
        catalog
            .code_assist_route("google", "gemini-3.8-flash-high", None)
            .unwrap(),
        (
            "antigravity-gemini-3.8-flash".into(),
            "gemini-3.8-flash-high".into()
        )
    );
}
