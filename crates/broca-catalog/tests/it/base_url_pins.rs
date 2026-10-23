//! Endpoints belong to the served row; the overlay contains only explicit temporary holds.
use broca_catalog::{Catalog, CatalogCache, CatalogSource, FusiformCatalogResponse};
use broca_provider::{
    families::{renderer_version_for, resolve_frozen},
    registry::wire_family,
    spec::WireFamilyId,
    wire::AuthHandle,
};
use serde_json::Value;

#[test]
fn served_endpoints_equal_the_rows_facts_and_missing_endpoints_refuse() {
    let response: FusiformCatalogResponse =
        serde_json::from_str(include_str!("../fixtures/fusiform_opencode_go.json")).unwrap();
    let cache = CatalogCache::empty();
    cache
        .install_for_test(response.clone(), CatalogSource::Fusiform)
        .unwrap();
    let snapshot = cache.snapshot().unwrap();
    let mut compared = 0;
    let mut refused = 0;
    for (identity, facts) in &response.models {
        if facts.get("existence").and_then(Value::as_str) != Some("present") {
            continue;
        }
        let (provider, model) = identity.split_once('/').unwrap();
        match facts.get("wire.api") {
            Some(api) => {
                assert_eq!(
                    snapshot.resolve_route(provider, model).unwrap().1,
                    api["value"].as_str().unwrap(),
                    "{identity}"
                );
                compared += 1;
            }
            None => {
                assert!(
                    snapshot
                        .resolve_route(provider, model)
                        .unwrap_err()
                        .contains("missing wire.api"),
                    "{identity}"
                );
                refused += 1;
            }
        }
    }
    assert_eq!(compared, 29, "all Pi-routed Go rows are compared");
    assert_eq!(refused, 5, "every withheld Go endpoint refuses");
}

#[test]
fn overlay_endpoint_copies_are_limited_to_the_native_holds() {
    let overlay: Value = serde_json::from_str(include_str!("../../data/overlay.json")).unwrap();
    let mut endpoints = Vec::new();
    for (provider, patch) in overlay["patch"].as_object().unwrap() {
        assert!(
            patch["_comment"]
                .as_str()
                .is_some_and(|comment| !comment.is_empty()),
            "{provider} needs a reason"
        );
        if patch.get("base_url").is_some() {
            assert_eq!(patch["base_url_source"], "authored", "{provider}");
            endpoints.push(provider.as_str());
        }
        for (model, patch) in patch
            .get("models")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            assert!(
                patch.get("base_url").is_none(),
                "{provider}/{model}: endpoint belongs to Fusiform"
            );
        }
    }
    assert_eq!(endpoints, ["openai", "xai"]);
}

#[test]
fn provider_without_endpoint_pin_uses_the_family_default_endpoint() {
    let catalog = Catalog::from_json_str(include_str!("../../data/models.dev.json"), "{}").unwrap();
    let (provider, model) = catalog.model_for_serving("openai", "gpt-4.1").unwrap();
    assert!(provider.base_url.is_empty());
    let frozen = resolve_frozen(
        &provider,
        &model,
        4096,
        renderer_version_for(WireFamilyId::OpenAiChat),
    )
    .unwrap();
    assert_eq!(frozen.base_url, None);
    let request = wire_family(WireFamilyId::OpenAiChat).unwrap().endpoint(
        &frozen,
        &AuthHandle::None,
        Vec::new(),
    );
    assert_eq!(request.url, "https://api.openai.com/v1/chat/completions");
}
