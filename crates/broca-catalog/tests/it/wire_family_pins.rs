//! Renderer mappings consume upstream packages and shapes, never copied provider inventories.
use broca_catalog::{family::wire_family, CatalogCache, CatalogSource, FusiformCatalogResponse};
use broca_provider::spec::WireFamilyId;
use serde_json::Value;

#[test]
fn upstream_packages_and_shapes_choose_only_implemented_renderers() {
    for (npm, shape, expected) in [
        ("@ai-sdk/openai-compatible", None, WireFamilyId::OpenAiChat),
        ("@ai-sdk/openai", None, WireFamilyId::OpenAiResponses),
        (
            "@ai-sdk/openai",
            Some("completions"),
            WireFamilyId::OpenAiChat,
        ),
        (
            "@ai-sdk/openai-compatible",
            Some("responses"),
            WireFamilyId::OpenAiResponses,
        ),
        ("@ai-sdk/anthropic", None, WireFamilyId::AnthropicMessages),
        ("@ai-sdk/google", None, WireFamilyId::Gemini),
        (
            "@ai-sdk/amazon-bedrock",
            None,
            WireFamilyId::BedrockConverse,
        ),
        (
            "@openrouter/ai-sdk-provider",
            None,
            WireFamilyId::OpenAiChat,
        ),
        ("@ai-sdk/xai", None, WireFamilyId::OpenAiResponses),
    ] {
        assert_eq!(wire_family(npm, shape), Some(expected), "{npm} / {shape:?}");
    }
    assert_eq!(wire_family("unimplemented-package", None), None);
    assert_eq!(wire_family("@ai-sdk/openai", Some("unknown-shape")), None);
}

#[test]
fn go_rows_follow_their_published_packages_and_shapes() {
    let response: FusiformCatalogResponse =
        serde_json::from_str(include_str!("../fixtures/fusiform_opencode_go.json")).unwrap();
    let cache = CatalogCache::empty();
    cache
        .install_for_test(response.clone(), CatalogSource::Fusiform)
        .unwrap();
    let snapshot = cache.snapshot().unwrap();
    let mut compared = 0;
    for (identity, facts) in &response.models {
        if facts.get("existence").and_then(Value::as_str) != Some("present")
            || facts.get("wire.api").is_none()
        {
            continue;
        }
        let (provider, model) = identity.split_once('/').unwrap();
        let family = snapshot.resolve_route(provider, model).unwrap().0;
        let npm = facts["wire.npm"]["value"].as_str().unwrap();
        let shape = facts
            .get("wire.shape")
            .and_then(|shape| shape["value"].as_str());
        assert_eq!(Some(family), wire_family(npm, shape), "{identity}");
        compared += 1;
    }
    assert_eq!(compared, 29);
}

#[test]
fn overlay_renderer_choices_have_a_reason_and_no_generated_provider_pins() {
    let overlay: Value = serde_json::from_str(include_str!("../../data/overlay.json")).unwrap();
    for (provider, patch) in overlay["patch"].as_object().unwrap() {
        assert!(
            patch["_comment"]
                .as_str()
                .is_some_and(|comment| !comment.is_empty()),
            "{provider}"
        );
        if patch.get("wire_family").is_some() {
            assert!(
                ["openai", "xai"].contains(&provider.as_str()),
                "{provider}: provider grammar belongs to wire facts"
            );
            assert_eq!(patch["wire_family_source"], "authored");
        }
        for (model, patch) in patch
            .get("models")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            assert!(
                patch
                    .as_object()
                    .unwrap()
                    .keys()
                    .all(|key| ["wire_family", "_comment"].contains(&key.as_str())),
                "{provider}/{model}: model facts belong to Fusiform"
            );
        }
    }
    assert_eq!(
        overlay["patch"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["openai", "xai"]
    );
}
