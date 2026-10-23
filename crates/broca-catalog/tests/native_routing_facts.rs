//! Native endpoint facts are authoritative for new runs, not already recorded bundles.
#![cfg(feature = "live")]
use broca_catalog::{
    live::{AuthMode, EpisodeRenderConfig, FrozenRenderConfig, ResolveOptions},
    CatalogCache, FusiformCatalogResponse,
};
use serde_json::json;
use std::sync::Arc;

fn resolve(
    recorded: Option<&FrozenRenderConfig>,
) -> Result<broca_catalog::live::ProviderResolution, broca_catalog::live::ResolutionError> {
    broca_catalog::live::resolve_catalog_episode_with_client(
        "openai/native-fixture",
        Some(1024),
        &AuthMode::None,
        &ResolveOptions::default(),
        broca_provider::build_provider_http_client(),
        EpisodeRenderConfig {
            recorded_config: recorded,
            ..Default::default()
        },
    )
}

#[test]
fn native_routes_follow_curated_facts_while_recorded_runs_keep_bytes() {
    let mut response: FusiformCatalogResponse = serde_json::from_value(json!({
        "source":"fixture","catalog_version":1,"resolved_at_ms":1,
        "models":{"openai/native-fixture":{"existence":"present","capability.reasoning":false,
            "wire.npm":{"value":"@ai-sdk/openai","from":"provider"}}}
    }))
    .unwrap();
    let cache = Arc::new(CatalogCache::empty());
    cache.refresh(response.clone()).unwrap();
    broca_catalog::install_global_catalog_cache(cache.clone());
    assert!(
        resolve(None)
            .err()
            .unwrap()
            .to_string()
            .contains("missing wire.api"),
        "native routes cannot guess SDK defaults after the hold is removed"
    );
    response.catalog_version += 1;
    response.resolved_at_ms += 1;
    response
        .models
        .get_mut("openai/native-fixture")
        .unwrap()
        .insert(
            "wire.api".into(),
            json!({"value":"https://api.openai.com/v1","from":"curated"}),
        );
    cache.refresh(response.clone()).unwrap();
    let admitted = resolve(None).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        admitted.base_url, "https://api.openai.com/v1",
        "native endpoint must come from served facts"
    );
    let frozen: FrozenRenderConfig =
        serde_json::from_value(admitted.provider.render_config_blob()).unwrap();
    response.catalog_version += 1;
    response.resolved_at_ms += 1;
    let facts = response.models.get_mut("openai/native-fixture").unwrap();
    facts.insert(
        "wire.api".into(),
        json!({"value":"https://published-edit.example/v1beta","from":"curated"}),
    );
    facts.insert(
        "wire.npm".into(),
        json!({"value":"@ai-sdk/google","from":"provider"}),
    );
    cache.refresh(response).unwrap();
    let fresh = resolve(None).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        fresh.base_url, "https://published-edit.example/v1beta",
        "native endpoint must come from served facts"
    );
    assert_eq!(fresh.family_id, broca_provider::spec::WireFamilyId::Gemini);
    let resumed = resolve(Some(&frozen)).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        resumed.provider.render_config_blob(),
        admitted.provider.render_config_blob()
    );
}
