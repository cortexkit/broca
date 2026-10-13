use std::sync::Arc;

use broca_catalog::{install_global_catalog_cache, Catalog, CatalogCache, CatalogSource};

pub fn install_snapshot() -> Arc<Catalog> {
    let catalog = Catalog::from_json_str(
        include_str!("../../data/models.dev.json"),
        include_str!("../../data/overlay.json"),
    )
    .expect("legacy test snapshot is well formed");
    let catalog = Arc::new(catalog);
    let cache = Arc::new(CatalogCache::empty());
    cache.install_parsed_for_test(&catalog, 0, CatalogSource::Fusiform);
    install_global_catalog_cache(cache);
    catalog
}
