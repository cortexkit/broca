//! Unrouted Go rows refuse from the missing fact, without a local model-name hold.
#![cfg(feature = "live")]
use broca_catalog::{
    live::{AuthMode, EpisodeRenderConfig, ResolveOptions},
    CatalogCache, FusiformCatalogResponse,
};
use serde_json::json;
use std::sync::Arc;

#[test]
fn go_rows_without_wire_api_refuse_by_name_without_a_guessed_fallback() {
    let response: FusiformCatalogResponse = serde_json::from_value(json!({
        "source":"fixture", "catalog_version":1, "resolved_at_ms":1,
        "models":{
            "opencode-go/kimi-k3.1-pro":{"existence":"present","capability.reasoning":false,
                "wire.npm":{"value":"@ai-sdk/openai-compatible","from":"provider"}},
            "opencode-go/future-unrouted-model":{"existence":"present","capability.reasoning":false,
                "wire.npm":{"value":"@ai-sdk/openai-compatible","from":"provider"}}
        },
        "wire_corrections":[{"provider":"opencode-go","model":"kimi-k3.1-pro","withheld":true,
            "notes":"future diagnostic text","unknown_member":{"allowed":true}}]
    }))
    .unwrap();
    assert_eq!(
        response.wire_corrections[0]["notes"],
        "future diagnostic text"
    );
    assert_eq!(
        response.wire_corrections[0]["unknown_member"]["allowed"],
        true
    );
    let cache = Arc::new(CatalogCache::empty());
    cache.refresh(response).unwrap();
    broca_catalog::install_global_catalog_cache(cache);
    for model in [
        "opencode-go/kimi-k3.1-pro",
        "opencode-go/future-unrouted-model",
    ] {
        let result = broca_catalog::live::resolve_catalog_episode_with_client(
            model,
            Some(1024),
            &AuthMode::None,
            &ResolveOptions::default(),
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig {
                opencode_go_session_id: Some("missing-api"),
                ..Default::default()
            },
        );
        let error = result
            .err()
            .expect("a Go row without wire.api must not construct a provider");
        let message = error.to_string();
        assert!(
            message.contains(model) && message.contains("missing wire.api"),
            "{message}"
        );
        assert_eq!(
            broca_core::RunError::from(error).admission_error_class(),
            broca_protocol::ErrorClass::Permanent
        );
    }
}
