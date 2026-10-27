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
fn served_apis_freeze_to_equivalent_full_request_urls() {
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
                let (family, base) = snapshot.resolve_route(provider, model).unwrap();
                let raw = api["value"].as_str().unwrap().trim_end_matches('/');
                let expected = match family {
                    WireFamilyId::AnthropicMessages if raw.ends_with("/v1") => {
                        format!("{raw}/messages")
                    }
                    WireFamilyId::AnthropicMessages => format!("{raw}/v1/messages"),
                    WireFamilyId::OpenAiChat => format!("{raw}/chat/completions"),
                    WireFamilyId::OpenAiResponses => format!("{raw}/responses"),
                    WireFamilyId::Gemini => {
                        format!("{raw}/models/{model}:streamGenerateContent?alt=sse")
                    }
                    _ => panic!("unexpected Go family for {identity}"),
                };
                let mut candidate =
                    broca_provider::families::frozen_config_for_family(family, model);
                candidate.base_url = Some(base);
                let candidate_url = wire_family(family)
                    .unwrap()
                    .endpoint(&candidate, &AuthHandle::None, Vec::new())
                    .url;
                assert_eq!(candidate_url, expected, "{identity}: served full URL");
                let (pinned, _) = snapshot.model_for_serving(provider, model).unwrap();
                let mut held =
                    broca_provider::families::frozen_config_for_family(pinned.wire_family, model);
                held.base_url = Some(pinned.base_url);
                let held_url = wire_family(held.family_id)
                    .unwrap()
                    .endpoint(&held, &AuthHandle::None, Vec::new())
                    .url;
                assert_eq!(held_url, expected, "{identity}: held full URL");
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
    assert_eq!(
        refused, 6,
        "every withheld Go endpoint refuses, including new unrouted models"
    );
}

#[test]
fn overlay_has_no_local_endpoint_or_model_format_pins() {
    let overlay: Value = serde_json::from_str(include_str!("../../data/overlay.json")).unwrap();
    assert!(
        overlay["patch"].as_object().unwrap().is_empty(),
        "published route facts must not be copied into a local overlay"
    );
    assert!(overlay["add"].as_object().unwrap().is_empty());
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
        renderer_version_for(WireFamilyId::OpenAiResponses),
    )
    .unwrap();
    assert_eq!(frozen.base_url, None);
    let request = wire_family(WireFamilyId::OpenAiResponses)
        .unwrap()
        .endpoint(&frozen, &AuthHandle::None, Vec::new());
    assert_eq!(request.url, "https://api.openai.com/v1/responses");
}
