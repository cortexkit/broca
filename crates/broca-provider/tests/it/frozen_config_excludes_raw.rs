//! The frozen render config must not carry the catalog's unbounded `raw` passthrough.
//!
//! `ProviderSpec.raw` preserves every catalog field broca does not yet model, so a schema
//! addition never silently drops data. That is deliberate, and it makes `ProviderSpec` an
//! accumulating bag of upstream fields — including ones nothing reads.
//!
//! What keeps that out of durable state is `resolve_frozen`, which narrows a spec into a
//! `FrozenRenderConfig` carrying only bounded typed fields. The frozen config is serialized into
//! every `RunStarted` record and is the sole render input on resume, so anything that reaches it
//! becomes both a durability-format field and part of the byte-identity domain that resume
//! depends on.
//!
//! Until now that exclusion was held by a doc comment. The dangerous edit is not a read of `raw`
//! — it is a widening of `FrozenRenderConfig` to include it, which looks like adding a feature
//! and fails nothing at the moment it is written. A peer found the same shape on their side and
//! turned it into a test; this is the same move.
//!
//! What this proves and what it does not: it asserts the SERIALIZED frozen config contains no
//! upstream passthrough. It cannot prove `raw` is unread elsewhere, and it is not trying to —
//! reading `raw` during ingestion is fine, and the render path already cannot see a spec at all.
//!
//! On its non-vacuity, stated precisely because the obvious mutation does not work. Adding a
//! `raw` field to `FrozenRenderConfig` does not compile: sixteen construction sites must name
//! every field, so the change fails at the compiler before this test runs. That is a stronger
//! guard than this file and it is worth knowing it exists — but a mutation that does not compile
//! is not a caught mutation, so it does not demonstrate that this test discriminates.
//!
//! What this test covers is the case the compiler does not: a widening that goes through, where
//! every site is updated and the value flows in from a spec. The compiler is satisfied and the
//! durable record silently grows an unbounded upstream field. That is the plausible version —
//! it arrives as a complete, tidy diff rather than a broken build.

use broca_provider::families::resolve_frozen;
use broca_provider::spec::{
    AuthKind, Capabilities, CapabilityFlag, Limits, ModelSpec, Overlay, ProviderSpec, WireFamilyId,
};
use serde_json::json;

/// A spec whose `raw` carries a distinctive marker, so a leak is identifiable rather than merely
/// detectable — a failure names the field that escaped instead of reporting a size change.
fn spec_with_marked_raw() -> ProviderSpec {
    ProviderSpec {
        id: "test-provider".to_string(),
        wire_family: WireFamilyId::OpenAiChat,
        base_url: "https://example.invalid/v1".to_string(),
        auth_kind: AuthKind::Bearer,
        static_headers: Default::default(),
        static_options: serde_json::Value::Null,
        quirks: Default::default(),
        // Shaped like the real thing: models.dev publishes these provider-level keys and broca
        // models none of them. `env` is the one that matters most — it names a credential
        // variable, so it is credential-adjacent even though it holds no secret.
        raw: json!({
            "npm": "@ai-sdk/openai-compatible",
            "env": ["RAW_PASSTHROUGH_MARKER_ENV"],
            "doc": "https://raw-passthrough-marker.invalid/docs",
            "name": "RAW_PASSTHROUGH_MARKER_NAME",
        }),
    }
}

/// The model spec carries its own `raw` passthrough, with its own marker — the provider-level
/// and model-level bags are separate fields and a widening could pick up either.
fn model_with_marked_raw() -> ModelSpec {
    ModelSpec {
        id: "test-model".to_string(),
        display_name: "Test Model".to_string(),
        family: None,
        capabilities: Capabilities {
            reasoning: CapabilityFlag::Known(false),
            ..Default::default()
        },
        limits: Limits::default(),
        cost: Default::default(),
        reasoning_options: Default::default(),
        input_modalities: Default::default(),
        release_date: None,
        status: None,
        overlay: Overlay::default(),
        raw: json!({ "knowledge": "RAW_PASSTHROUGH_MARKER_MODEL" }),
    }
}

#[test]
fn the_frozen_config_carries_no_catalog_passthrough() {
    let provider = spec_with_marked_raw();
    let model = model_with_marked_raw();
    let frozen = resolve_frozen(&provider, &model, 2048, 1).expect("known capability resolves");

    let serialized = serde_json::to_string(&frozen).expect("the frozen config serializes");

    // Assert on the marker values rather than on key names: a widening might rename or nest the
    // field, and the property is that the VALUES cannot reach durable state, not that a
    // particular key is absent.
    for marker in [
        "RAW_PASSTHROUGH_MARKER_ENV",
        "RAW_PASSTHROUGH_MARKER_NAME",
        "RAW_PASSTHROUGH_MARKER_MODEL",
        "raw-passthrough-marker.invalid",
        "@ai-sdk/openai-compatible",
    ] {
        assert!(
            !serialized.contains(marker),
            "the frozen render config leaked a catalog passthrough value ({marker}). It is \
             written into every RunStarted record and is the sole render input on resume, so an \
             unbounded upstream field reaching it becomes durable format and part of the \
             byte-identity domain."
        );
    }

    // The spec really did carry the markers, so the assertion above ran against a loaded input
    // rather than an empty one. Without this, deleting `raw` from the fixture would leave every
    // assertion passing over nothing.
    let spec_serialized = serde_json::to_string(&provider).expect("the spec serializes");
    let model_serialized = serde_json::to_string(&model).expect("the model spec serializes");
    assert!(
        spec_serialized.contains("RAW_PASSTHROUGH_MARKER_ENV")
            && model_serialized.contains("RAW_PASSTHROUGH_MARKER_MODEL"),
        "a fixture's raw passthrough is empty, so the exclusion assertions proved nothing"
    );
}
