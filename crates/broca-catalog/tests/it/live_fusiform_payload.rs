//! Parse a captured LIVE `catalog.get` response with the shipped deserializer.
//!
//! Every other test in this crate builds a fusiform response by hand, so they prove the parser
//! accepts the shape I BELIEVE fusiform serves. This one proves it accepts the shape fusiform
//! ACTUALLY served: the fixture beside it was captured off the running daemon with the exact
//! `fact_prefixes` and `include_retired` the poll sends, not written here.
//!
//! It is a snapshot rather than a live call on purpose — CI has no daemon, and a test that
//! silently skips when one is absent would report success while checking nothing. Refresh the
//! fixture by re-running the probe in the header comment of the JSON file.

use broca_catalog::{CatalogCache, CatalogSource, FusiformCatalogResponse};

const LIVE_RESPONSE: &str = include_str!("../fixtures/fusiform_catalog_get_live.json");

#[test]
fn the_shipped_parser_accepts_a_real_fusiform_response() {
    let response: FusiformCatalogResponse =
        serde_json::from_str(LIVE_RESPONSE).expect("live fusiform response must deserialize");

    let cache = CatalogCache::empty();
    cache
        .install_for_test(response, CatalogSource::Fusiform)
        .expect("live response must install");

    let snapshot = cache.snapshot().expect("catalog present after install");

    // Non-vacuity: a fixture that parsed to an empty catalog would satisfy every assertion
    // above while proving nothing about the wire.
    assert!(
        snapshot.catalog_version() > 0,
        "the captured response carried no catalog version, so this fixture proves nothing"
    );
}
