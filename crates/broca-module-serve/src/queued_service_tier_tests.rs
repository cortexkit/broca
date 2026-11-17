fn install_queued_ultrafast_catalog() {
    let rate = |units: u64| {
        serde_json::json!({
            "currency": "USD", "exponent": 9, "state": "priced", "units": units
        })
    };
    let mut value = serde_json::json!({
        "source": "test", "resolved_at_ms": 0, "catalog_version": 1,
        "models": {"openai/test-ultrafast": {
            "existence": "present",
            "wire.npm": {"value": "@ai-sdk/openai-compatible", "from": "provider"},
            "wire.api": {"value": "https://api.openai.com/v1", "from": "curated"},
            "limit.context": 200000, "limit.output": 64000,
            "capability.attachment": false, "capability.reasoning": false,
            "capability.tool_call": true,
            "rate.input": rate(1_000_000_000), "rate.output": rate(2_000_000_000),
            "rate.input.mode.fast": rate(2_000_000_000),
            "rate.output.mode.fast": rate(3_000_000_000),
            "rate.input.mode.ultrafast": rate(3_000_000_000),
            "rate.output.mode.ultrafast": rate(4_000_000_000)
        }}
    });
    let mut standard = value["models"]["openai/test-ultrafast"].clone();
    standard.as_object_mut().unwrap().retain(|key, _| !key.contains(".mode."));
    value["models"]["openai/test-standard-only"] = standard;
    value["models"]["openai/test-other-fast"] = value["models"]["openai/test-ultrafast"].clone();
    let response = serde_json::from_value(value).unwrap();
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache
        .install_for_test(response, broca_catalog::CatalogSource::Fusiform)
        .unwrap();
    broca_catalog::install_global_catalog_cache(cache);
}

