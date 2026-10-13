//! Materialise the retired npm/api family inference as overlay pins, and lock
//! that no currently-served provider changes family once those rules are gone.
//!
//! The ignored generator writes `data/overlay.json`. The equality test computes
//! the pre-change answer from a frozen copy of the old rules — not from the
//! current overlay — so a bad pin cannot launder itself by also rewriting the
//! oracle.

#![cfg(feature = "bundled-catalog")]

use broca_catalog::family::{resolve_provider_family, FamilyResolution};
use broca_provider::spec::WireFamilyId;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;

const SNAPSHOT: &str = include_str!("../../data/models.dev.json");
const OVERLAY: &str = include_str!("../../data/overlay.json");

/// Distinguishes a person's family choice from an inherited inference run.
/// A bare `wire_family` value cannot tell those apart, and treating a generated
/// pin as authored would make every pin look equally authoritative.
const WIRE_FAMILY_SOURCE: &str = "wire_family_source";
const SOURCE_GENERATED: &str = "generated";
const SOURCE_AUTHORED: &str = "authored";

/// Overlay `wire_family` pins that already existed before the generated-pin
/// materialisation. Frozen so relabeling a generated pin as authored cannot
/// rewrite the before-oracle.
const PRE_MIGRATION_OVERLAY_PINS: &[(&str, WireFamilyId)] = &[
    ("cerebras", WireFamilyId::OpenAiChat),
    ("xai", WireFamilyId::OpenAiChat),
    ("openrouter", WireFamilyId::OpenAiChat),
    // kimi-for-coding (AnthropicMessages) was frozen here until upstream withdrew the provider
    // on 2026-09-18; its successor kimi-code-plan-global is declared openai-compatible by the
    // document and takes a generated pin.
];

/// Authored family choices that intentionally differ from the retired inference.
/// Each exception has a separate reason in the provider overlay entry.
const AUTHORED_OVERLAY_FAMILY_EXCEPTIONS: &[(&str, WireFamilyId)] =
    &[("meta", WireFamilyId::OpenAiResponses)];

/// The npm package models.dev assigns to providers that speak the OpenAI-compatible
/// `/chat/completions` shape — the long tail. Frozen copy of the retired rule 4.
const OPENAI_COMPATIBLE_NPM: &str = "@ai-sdk/openai-compatible";

/// EXACT npm packages whose grammar we handled — frozen copy of the retired
/// rule 3 allowlist. Exact match only; never a prefix.
const NPM_ALLOWLIST: &[(&str, WireFamilyId)] = &[
    ("@ai-sdk/anthropic", WireFamilyId::AnthropicMessages),
    ("@ai-sdk/openai", WireFamilyId::OpenAiChat),
    ("@ai-sdk/google", WireFamilyId::Gemini),
    ("@ai-sdk/amazon-bedrock", WireFamilyId::BedrockConverse),
];

/// Retired resolver: overlay pin, provider-id hand-table, npm allowlist, then
/// openai-compatible fallback. Kept here so the equality test's "before" answer
/// comes from the actual old rules, and so the ignored generator can propose
/// pins for a catalog refresh without putting that inference back on the serve
/// path.
fn resolve_provider_family_legacy(
    provider_id: &str,
    npm: Option<&str>,
    has_api: bool,
    overlay_family: Option<WireFamilyId>,
) -> FamilyResolution {
    if let Some(family) = overlay_family {
        return FamilyResolution::Supported {
            family,
            source: broca_catalog::family::ResolutionSource::Overlay,
        };
    }
    // The production hand-table is still live (rule 2). Re-run the current
    // resolver with no overlay pin so a hand-table edit cannot drift from this
    // copy.
    match resolve_provider_family(provider_id, None) {
        supported @ FamilyResolution::Supported { .. } => return supported,
        FamilyResolution::Pending { .. } => {}
    }
    if let Some(npm) = npm {
        if let Some((_, family)) = NPM_ALLOWLIST.iter().find(|(pkg, _)| *pkg == npm) {
            if has_api {
                return FamilyResolution::Supported {
                    family: *family,
                    source: broca_catalog::family::ResolutionSource::Overlay,
                };
            }
            return FamilyResolution::Pending {
                reason: format!(
                    "{provider_id}: npm={npm} is allowlisted but has no api (base_url) to reach"
                ),
            };
        }
    }
    if npm == Some(OPENAI_COMPATIBLE_NPM) {
        if has_api {
            return FamilyResolution::Supported {
                family: WireFamilyId::OpenAiChat,
                source: broca_catalog::family::ResolutionSource::Overlay,
            };
        }
        return FamilyResolution::Pending {
            reason: format!(
                "{provider_id}: openai-compatible npm but no api (base_url) in catalog"
            ),
        };
    }
    FamilyResolution::Pending {
        reason: format!(
            "{provider_id}: npm={} has no family mapping",
            npm.unwrap_or("<none>")
        ),
    }
}

fn family_of(r: &FamilyResolution) -> Option<WireFamilyId> {
    match r {
        FamilyResolution::Supported { family, .. } => Some(*family),
        FamilyResolution::Pending { .. } => None,
    }
}

fn parse_family(v: &Value) -> Option<WireFamilyId> {
    serde_json::from_value(v.clone()).ok()
}

fn family_tag(family: WireFamilyId) -> String {
    serde_json::to_value(family)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .expect("WireFamilyId serializes to a string")
}

fn snapshot_providers() -> BTreeMap<String, Value> {
    serde_json::from_str::<Value>(SNAPSHOT)
        .expect("vendored snapshot is JSON")
        .as_object()
        .expect("snapshot is an object")
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn overlay_value() -> Value {
    serde_json::from_str(OVERLAY).expect("overlay is JSON")
}

fn overlay_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/overlay.json")
}

fn provider_inputs(entry: &Value) -> (Option<&str>, bool) {
    let npm = entry.get("npm").and_then(Value::as_str);
    let has_api = entry
        .get("api")
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty());
    (npm, has_api)
}

fn pre_migration_overlay_family(provider_id: &str) -> Option<WireFamilyId> {
    PRE_MIGRATION_OVERLAY_PINS
        .iter()
        .find(|(id, _)| *id == provider_id)
        .map(|(_, family)| *family)
}

fn authored_overlay_family_exception(provider_id: &str) -> Option<WireFamilyId> {
    AUTHORED_OVERLAY_FAMILY_EXCEPTIONS
        .iter()
        .find(|(id, _)| *id == provider_id)
        .map(|(_, family)| *family)
}

fn current_overlay_family(patch: &Map<String, Value>, provider_id: &str) -> Option<WireFamilyId> {
    patch
        .get(provider_id)
        .and_then(|entry| entry.get("wire_family"))
        .and_then(parse_family)
}

fn pin_source<'a>(patch: &'a Map<String, Value>, provider_id: &str) -> Option<&'a str> {
    patch
        .get(provider_id)
        .and_then(|entry| entry.get(WIRE_FAMILY_SOURCE))
        .and_then(Value::as_str)
}

/// Providers the retired npm rules would serve, given only the pre-migration
/// overlay pins. These are the pins the generator must write.
fn npm_inferred_pins(providers: &BTreeMap<String, Value>) -> BTreeMap<String, WireFamilyId> {
    let mut pins = BTreeMap::new();
    for (id, entry) in providers {
        if pre_migration_overlay_family(id).is_some()
            || authored_overlay_family_exception(id).is_some()
        {
            continue;
        }
        let (npm, has_api) = provider_inputs(entry);
        let before = resolve_provider_family_legacy(id, npm, has_api, None);
        // Hand-table hits are not npm inference.
        if matches!(
            resolve_provider_family(id, None),
            FamilyResolution::Supported { .. }
        ) {
            continue;
        }
        if let Some(family) = family_of(&before) {
            pins.insert(id.clone(), family);
        }
    }
    pins
}

/// Writes generated `wire_family` pins into `data/overlay.json`. Ignored so a
/// normal test run cannot rewrite the overlay; run explicitly after a catalog
/// refresh when new providers need a proposed pin.
///
/// Chosen as an `#[ignore]`d test rather than a binary because this crate has
/// no existing bins, and the neighboring refresh artifact (`resolution_report`)
/// is already regenerated from a test.
#[test]
#[ignore = "writes data/overlay.json; run to regenerate generated wire_family pins"]
fn generate_wire_family_pins() {
    let providers = snapshot_providers();
    let mut overlay = serde_json::from_str::<Value>(
        &std::fs::read_to_string(overlay_path()).expect("read overlay.json"),
    )
    .expect("overlay is JSON");
    let patch = overlay
        .get_mut("patch")
        .and_then(Value::as_object_mut)
        .expect("overlay.patch is an object");

    // Mark every pre-existing (and any later hand-written) pin as authored
    // unless it is already tagged generated — never relabel a generated pin.
    for entry in patch.values_mut() {
        let Some(obj) = entry.as_object_mut() else {
            continue;
        };
        if !obj.contains_key("wire_family") {
            continue;
        }
        match obj.get(WIRE_FAMILY_SOURCE).and_then(Value::as_str) {
            Some(SOURCE_GENERATED) | Some(SOURCE_AUTHORED) => {}
            _ => {
                obj.insert(WIRE_FAMILY_SOURCE.to_string(), json!(SOURCE_AUTHORED));
            }
        }
    }

    let inferred = npm_inferred_pins(&providers);

    // Drop generated pins whose provider left the snapshot or no longer infers.
    let stale: Vec<String> = patch
        .iter()
        .filter(|(id, entry)| {
            entry.get(WIRE_FAMILY_SOURCE).and_then(Value::as_str) == Some(SOURCE_GENERATED)
                && !inferred.contains_key(id.as_str())
        })
        .map(|(id, _)| id.clone())
        .collect();
    for id in stale {
        patch.remove(&id);
    }

    for (id, family) in &inferred {
        match patch.get(id) {
            Some(existing)
                if existing.get(WIRE_FAMILY_SOURCE).and_then(Value::as_str)
                    == Some(SOURCE_AUTHORED) =>
            {
                panic!(
                    "refusing to overwrite authored pin for {id}; move it by hand if the family should change"
                );
            }
            Some(existing) => {
                let mut obj = existing.as_object().cloned().unwrap_or_default();
                obj.insert("wire_family".to_string(), json!(family_tag(*family)));
                obj.insert(WIRE_FAMILY_SOURCE.to_string(), json!(SOURCE_GENERATED));
                patch.insert(id.clone(), Value::Object(obj));
            }
            None => {
                let mut obj = Map::new();
                obj.insert("wire_family".to_string(), json!(family_tag(*family)));
                obj.insert(WIRE_FAMILY_SOURCE.to_string(), json!(SOURCE_GENERATED));
                patch.insert(id.clone(), Value::Object(obj));
            }
        }
    }

    // Rewrite patch with sorted keys so a regenerate is reviewable.
    let sorted = patch
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<BTreeMap<_, _>>();
    *patch = sorted.into_iter().collect();

    let mut out = serde_json::to_string_pretty(&overlay).expect("serialize overlay");
    out.push('\n');
    std::fs::write(overlay_path(), out).expect("write overlay.json");
    eprintln!(
        "wrote {} generated wire_family pins to {}",
        inferred.len(),
        overlay_path().display()
    );
}

#[test]
fn every_overlay_wire_family_pin_declares_its_source() {
    let overlay = overlay_value();
    let patch = overlay
        .get("patch")
        .and_then(Value::as_object)
        .expect("overlay.patch is an object");
    let mut unmarked = Vec::new();
    for (id, entry) in patch {
        if entry.get("wire_family").is_none() {
            continue;
        }
        match entry.get(WIRE_FAMILY_SOURCE).and_then(Value::as_str) {
            Some(SOURCE_GENERATED) | Some(SOURCE_AUTHORED) => {}
            other => unmarked.push(format!("{id}: {other:?}")),
        }
    }
    assert!(
        unmarked.is_empty(),
        "every overlay wire_family pin must declare wire_family_source as \
         generated or authored so an inherited inference cannot be mistaken \
         for a human decision: {unmarked:?}"
    );
}

#[test]
fn generated_pins_preserve_every_provider_family() {
    let providers = snapshot_providers();
    let overlay = overlay_value();
    let patch = overlay
        .get("patch")
        .and_then(Value::as_object)
        .expect("overlay.patch is an object");

    for (id, entry) in &providers {
        let (npm, has_api) = provider_inputs(entry);
        let before = authored_overlay_family_exception(id).or_else(|| {
            family_of(&resolve_provider_family_legacy(
                id,
                npm,
                has_api,
                pre_migration_overlay_family(id),
            ))
        });
        let after = family_of(&resolve_provider_family(
            id,
            current_overlay_family(patch, id),
        ));
        assert_eq!(
            after, before,
            "{id}: family after the overlay pins must equal the family the retired npm rules produced"
        );
    }
}

#[test]
fn generated_pins_are_exactly_the_retired_npm_inference() {
    let providers = snapshot_providers();
    let overlay = overlay_value();
    let patch = overlay
        .get("patch")
        .and_then(Value::as_object)
        .expect("overlay.patch is an object");
    let inferred = npm_inferred_pins(&providers);

    let mut generated = BTreeMap::new();
    for (id, entry) in patch {
        if entry.get(WIRE_FAMILY_SOURCE).and_then(Value::as_str) != Some(SOURCE_GENERATED) {
            continue;
        }
        let family = entry
            .get("wire_family")
            .and_then(parse_family)
            .unwrap_or_else(|| panic!("{id}: generated pin missing wire_family"));
        generated.insert(id.clone(), family);
    }

    assert_eq!(
        generated, inferred,
        "generated overlay pins must be exactly the providers the retired npm \
         rules would serve — no extras, no missing, no family drift"
    );
}

#[test]
fn pre_migration_overlay_pins_stay_authored() {
    let overlay = overlay_value();
    let patch = overlay
        .get("patch")
        .and_then(Value::as_object)
        .expect("overlay.patch is an object");
    for (id, family) in PRE_MIGRATION_OVERLAY_PINS {
        assert_eq!(
            pin_source(patch, id),
            Some(SOURCE_AUTHORED),
            "{id} was a person's pin before generation and must stay authored"
        );
        assert_eq!(
            current_overlay_family(patch, id),
            Some(*family),
            "{id} authored family must not change under generation"
        );
    }
}
