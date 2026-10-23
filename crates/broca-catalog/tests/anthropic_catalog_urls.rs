//! Published API roots are adapted before freezing; renderer paths stay unchanged.
use broca_catalog::{CatalogCache, FusiformCatalogResponse};
use broca_provider::{
    families::{renderer_version_for, resolve_frozen},
    registry::wire_family,
    wire::{AuthHandle, FrozenRenderConfig},
};
use serde_json::json;

fn freeze(id: &str, npm: &str, api: &str) -> (FrozenRenderConfig, String) {
    let response: FusiformCatalogResponse = serde_json::from_value(json!({
        "source":"fixture", "catalog_version":1, "resolved_at_ms":0,
        "models": {id: {"existence":"present", "capability.reasoning":false,
            "wire.npm":{"value":npm,"from":"provider"}, "wire.api":{"value":api,"from":"curated"}}}
    }))
    .unwrap();
    let cache = CatalogCache::empty();
    cache.refresh(response).unwrap();
    let snapshot = cache.snapshot().unwrap();
    let (provider, model) = id.split_once('/').unwrap();
    let served_root = snapshot
        .routing_facts(provider, model)
        .unwrap()
        .route()
        .unwrap()
        .1;
    let (provider, model) = snapshot.model_for_serving(provider, model).unwrap();
    let frozen = resolve_frozen(
        &provider,
        &model,
        1024,
        renderer_version_for(provider.wire_family),
    )
    .unwrap();
    (frozen, served_root)
}

fn url(frozen: &FrozenRenderConfig) -> String {
    wire_family(frozen.family_id)
        .unwrap()
        .endpoint(frozen, &AuthHandle::None, Vec::new())
        .url
}

#[test]
fn served_anthropic_v1_is_stripped_before_freeze() {
    for api in [
        "https://api.anthropic.com/v1",
        "https://api.anthropic.com/v1/",
    ] {
        let (frozen, root) = freeze("sdk-fixture/model", "@ai-sdk/anthropic", api);
        assert_eq!(root, "https://api.anthropic.com");
        assert_eq!(
            frozen.base_url.as_deref(),
            Some("https://api.anthropic.com")
        );
        assert_eq!(url(&frozen), "https://api.anthropic.com/v1/messages");
    }
}

#[test]
fn go_anthropic_v1_produces_the_full_pi_messages_url() {
    let (frozen, root) = freeze(
        "opencode-go/claude-haiku-5-5",
        "@ai-sdk/anthropic",
        "https://opencode.ai/inference/go/v1",
    );
    assert_eq!(root, "https://opencode.ai/inference/go");
    assert_eq!(
        frozen.base_url.as_deref(),
        Some("https://opencode.ai/inference/go")
    );
    assert_eq!(url(&frozen), "https://opencode.ai/inference/go/v1/messages");
}

#[test]
fn an_anthropic_root_without_a_version_is_frozen_as_served() {
    let api = "https://api.minimax.cn/anthropic/";
    let (frozen, root) = freeze("sdk-fixture/model", "@ai-sdk/anthropic", api);
    assert_eq!(root, api);
    assert_eq!(frozen.base_url.as_deref(), Some(api));
    assert_eq!(url(&frozen), "https://api.minimax.cn/anthropic/v1/messages");
}

#[test]
fn openai_and_google_roots_keep_their_published_version() {
    let (openai, root) = freeze(
        "sdk-fixture/model",
        "@ai-sdk/openai",
        "https://router.example/v1",
    );
    assert_eq!(root, "https://router.example/v1");
    assert_eq!(
        openai.base_url.as_deref(),
        Some("https://router.example/v1")
    );
    assert_eq!(url(&openai), "https://router.example/v1/responses");
    let (google, root) = freeze(
        "sdk-fixture/model",
        "@ai-sdk/google",
        "https://router.example/v1beta",
    );
    assert_eq!(root, "https://router.example/v1beta");
    assert_eq!(
        google.base_url.as_deref(),
        Some("https://router.example/v1beta")
    );
    assert_eq!(
        url(&google),
        "https://router.example/v1beta/models/model:streamGenerateContent?alt=sse"
    );
}
