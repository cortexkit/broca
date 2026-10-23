//! Native providers keep their existing request bytes until their curated endpoint facts arrive.
#![cfg(all(feature = "live", feature = "bundled-catalog"))]

use broca_catalog::{
    live::{AuthMode, ResolveOptions},
    Catalog, CatalogCache, FusiformCatalogResponse,
};
use broca_protocol::{
    content::{ContentBlock, Message, Role},
    CallOptions,
};
use std::sync::Arc;

#[test]
fn held_native_rows_render_byte_identically_without_endpoint_facts() {
    let mut overlay: serde_json::Value =
        serde_json::from_str(include_str!("../data/overlay.json")).unwrap();
    overlay["patch"]
        .as_object_mut()
        .unwrap()
        .retain(|provider, _| provider == "openai");
    let legacy = Catalog::from_json_str(&serde_json::json!({"openai":{"models":{"gpt-6-luna":{"reasoning":true,"tool_call":true,"limit":{"context":400000,"output":128000}}}}}).to_string(), &overlay.to_string()).unwrap();
    let legacy_cache = Arc::new(CatalogCache::empty());
    legacy_cache.install_parsed_for_test(&legacy, 0, broca_catalog::CatalogSource::Fusiform);
    broca_catalog::install_global_catalog_cache(legacy_cache);
    let prompt = CallOptions::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "Hello".into(),
        }],
    )]);
    let model = "openai/gpt-6-luna";
    let before = broca_catalog::live::resolve_provider(
        model,
        None,
        1024,
        &AuthMode::None,
        &ResolveOptions::default(),
    )
    .unwrap_or_else(|e| panic!("legacy route: {e}"));
    let response: FusiformCatalogResponse = serde_json::from_str(include_str!(
        "fixtures/routing-parity/fusiform-served-with-wire-facts.json"
    ))
    .unwrap();
    assert!(!response.models[model].contains_key("wire.api"));
    let cache = Arc::new(CatalogCache::empty());
    cache.refresh(response).unwrap();
    broca_catalog::install_global_catalog_cache(cache);
    let after = broca_catalog::live::resolve_provider(
        model,
        None,
        1024,
        &AuthMode::None,
        &ResolveOptions::default(),
    )
    .unwrap_or_else(|e| panic!("held native route: {e}"));
    assert_eq!(
        after.provider.render_config_blob(),
        before.provider.render_config_blob()
    );
    assert_eq!(
        after.provider.render_request(&prompt),
        before.provider.render_request(&prompt)
    );
}
