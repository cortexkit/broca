use std::sync::Arc;

use broca_catalog::{install_global_catalog_cache, Catalog, CatalogCache, CatalogSource};

pub fn snapshot_cache(resolved_at_ms: i64, source: CatalogSource) -> Arc<CatalogCache> {
    let catalog = Catalog::from_json_str(
        include_str!("../../../broca-catalog/data/models.dev.json"),
        include_str!("../../../broca-catalog/data/overlay.json"),
    )
    .expect("legacy test snapshot is well formed");
    let cache = Arc::new(CatalogCache::empty());
    cache.install_parsed_for_test(&catalog, resolved_at_ms, source);
    cache
}

pub fn install_snapshot() {
    install_global_catalog_cache(snapshot_cache(0, CatalogSource::Fusiform));
}
