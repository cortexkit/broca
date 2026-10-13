//! Build-time validation of the vendored catalog + overlay snapshots.
//!
//! A malformed vendored snapshot must fail the BUILD, never reach production. This
//! parses both embedded JSON files at compile time; a parse error aborts the build
//! with the offending file named.

use std::path::Path;

fn main() {
    for rel in ["data/models.dev.json", "data/overlay.json"] {
        println!("cargo:rerun-if-changed={rel}");
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
        let bytes = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("catalog snapshot {rel} is unreadable: {e}"));
        if let Err(e) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            panic!("catalog snapshot {rel} is not valid JSON: {e}");
        }
    }
}
