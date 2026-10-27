//! Pins the bytes of a frozen plan manifest as written before tool-provider declarations
//! (a provider's per-tool result format and its `late_results` capability) were frozen into it.
//!
//! The fixture is a `RunStarted` whose manifest uses every member a manifest could carry at that
//! point: answered fetches with and without system text, frozen result-operation permissions,
//! reply deadlines with and without a hold argument, an absent provider, and a step-transform
//! declaration. It was committed with the encoder that predates the declarations, and this test
//! passed against that encoder, so the file is that encoder's output. Any later field must keep
//! decoding these bytes and writing them back unchanged, and must not make such a record require
//! a feature it did not require then.

#![allow(clippy::disallowed_methods)] // Fixture I/O is not a production WAL path.

use broca_wal::WalRecord;

const FIXTURE: &str = include_str!("fixtures/plan_manifest_pre_declarations.json");

#[test]
fn a_manifest_written_before_provider_declarations_keeps_its_bytes() {
    let bytes = FIXTURE.trim_end();
    let record: WalRecord = serde_json::from_str(bytes).expect("the pinned record decodes");
    let WalRecord::RunStarted {
        manifest: Some(_), ..
    } = &record
    else {
        panic!("the pinned record is a RunStarted carrying a manifest");
    };
    assert_eq!(
        serde_json::to_string(&record).expect("the pinned record encodes"),
        bytes,
        "a decode and re-encode must give back the pinned bytes exactly"
    );
}

#[test]
fn a_manifest_written_before_provider_declarations_requires_only_what_it_did_then() {
    let record: WalRecord =
        serde_json::from_str(FIXTURE.trim_end()).expect("the pinned record decodes");
    let mut features = record.required_features().to_vec();
    features.sort_unstable();
    assert_eq!(features, ["plan-manifest/v1", "step-transforms/v1"]);
}
