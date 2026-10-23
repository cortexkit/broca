use std::sync::Arc;

use broca_catalog::{install_global_catalog_cache, Catalog, CatalogCache, CatalogSource};

pub fn install_snapshot() -> Arc<Catalog> {
    let mut overlay: serde_json::Value =
        serde_json::from_str(include_str!("../../data/overlay.json")).unwrap();
    let response: broca_catalog::FusiformCatalogResponse = serde_json::from_str(include_str!(
        "../fixtures/routing-parity/fusiform-served-with-wire-facts.json"
    ))
    .unwrap();
    let facts = &response.models["google/antigravity-gemini-3.8-flash"];
    overlay["patch"]["google"]["models"]["antigravity-gemini-3.8-flash"] = serde_json::json!({
        "reasoning": true,
        "limit": {"context": facts["limit.context"], "output": facts["limit.output"]},
        "route.reasoning_wire_ids": facts["route.reasoning_wire_ids"],
        "route.reasoning_default": facts["route.reasoning_default"],
    });
    let catalog = Catalog::from_json_str(
        include_str!("../../data/models.dev.json"),
        &overlay.to_string(),
    )
    .expect("legacy test snapshot is well formed");
    let catalog = Arc::new(catalog);
    let cache = Arc::new(CatalogCache::empty());
    cache.install_parsed_for_test(&catalog, 0, CatalogSource::Fusiform);
    install_global_catalog_cache(cache);
    catalog
}
