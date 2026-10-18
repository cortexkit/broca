//! Materialise provider endpoints from the models.dev document as overlay pins and
//! lock the serving path to the same endpoint answers after that source is retired.
//!
//! The ignored generator writes `data/overlay.json`. The equality test computes
//! the pre-change answer from the document plus a frozen copy of the three endpoint
//! overrides that predate this migration, so a bad pin cannot rewrite its own oracle.

#![cfg(feature = "bundled-catalog")]

use broca_catalog::Catalog;
use broca_provider::families::{renderer_version_for, resolve_frozen};
use broca_provider::registry::wire_family;
use broca_provider::spec::WireFamilyId;
use broca_provider::wire::AuthHandle;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;

const SNAPSHOT: &str = include_str!("../../data/models.dev.json");
const OVERLAY: &str = include_str!("../../data/overlay.json");

/// Distinguishes a person's endpoint choice from a copied document value.
/// A bare `base_url` cannot say whether a person reviewed the host.
const BASE_URL_SOURCE: &str = "base_url_source";
const SOURCE_GENERATED: &str = "generated";
const SOURCE_AUTHORED: &str = "authored";

/// Endpoint overrides that existed as overlay `api` fields before endpoint pins.
/// Freezing these values keeps generation and equality checks from recasting a
/// hand-written override.
///
/// `kimi-for-coding` was here (authored to `https://api.kimi.com/coding`, dropping the
/// document's `/v1` because its wire family was anthropic_messages). Upstream withdrew the
/// provider on 2026-09-18 and split it into `kimi-code-plan-global` / `-cn`, which the
/// document declares openai-compatible at `.../coding/v1` -- so the successor takes the
/// generated pin like any other post-migration provider, and a frozen entry for an id that
/// exists nowhere would guard nothing.
const PRE_MIGRATION_OVERLAY_PINS: &[(&str, &str)] = &[
    ("cerebras", "https://api.cerebras.ai/v1"),
    ("xai", "https://api.x.ai/v1"),
];

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

fn pre_migration_overlay_base_url(provider_id: &str) -> Option<&'static str> {
    PRE_MIGRATION_OVERLAY_PINS
        .iter()
        .find(|(id, _)| *id == provider_id)
        .map(|(_, base_url)| *base_url)
}

fn document_base_url(entry: &Value) -> Option<&str> {
    entry
        .get("api")
        .and_then(Value::as_str)
        .filter(|base_url| !base_url.is_empty())
}

/// Document-derived pins, excluding endpoints that were already authored in the
/// overlay. The generator must preserve those human choices rather than overwrite them.
fn generated_base_url_pins(providers: &BTreeMap<String, Value>) -> BTreeMap<String, String> {
    providers
        .iter()
        .filter(|(id, _)| pre_migration_overlay_base_url(id).is_none())
        .filter_map(|(id, entry)| {
            document_base_url(entry).map(|base_url| (id.clone(), base_url.to_string()))
        })
        .collect()
}

fn current_overlay_base_url<'a>(
    patch: &'a Map<String, Value>,
    provider_id: &str,
) -> Option<&'a str> {
    patch
        .get(provider_id)
        .and_then(|entry| entry.get("base_url"))
        .and_then(Value::as_str)
}

fn pin_source<'a>(patch: &'a Map<String, Value>, provider_id: &str) -> Option<&'a str> {
    patch
        .get(provider_id)
        .and_then(|entry| entry.get(BASE_URL_SOURCE))
        .and_then(Value::as_str)
}

/// Resolve one provider through the production mapper. For pending providers the
/// synthetic family pin makes only the mapper reachable; it does not alter endpoint
/// selection, which depends solely on the provider's `base_url` pin.
fn mapped_base_url(
    provider_id: &str,
    snapshot_entry: &Value,
    patch: &Map<String, Value>,
) -> String {
    let mut base = Map::new();
    base.insert(provider_id.to_string(), snapshot_entry.clone());

    let mut provider_patch = patch
        .get(provider_id)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    provider_patch.insert("wire_family".to_string(), json!("openai_chat"));
    let mut patches = Map::new();
    patches.insert(provider_id.to_string(), Value::Object(provider_patch));
    let overlay = json!({ "patch": Value::Object(patches) });

    Catalog::from_json_str(&Value::Object(base).to_string(), &overlay.to_string())
        .unwrap_or_else(|error| panic!("{provider_id}: synthetic catalog must parse: {error}"))
        .provider(provider_id)
        .unwrap_or_else(|| {
            panic!("{provider_id}: synthetic family pin must make provider serveable")
        })
        .base_url
}

/// Writes document-derived `base_url` pins into `data/overlay.json`. Ignored so a
/// normal test run cannot rewrite the overlay; run explicitly after a catalog refresh
/// when new provider endpoints need proposed pins.
///
/// Chosen as an `#[ignore]`d test to match the neighboring wire-family generator and
/// keep both catalog materialisation steps behind the same explicit workflow.
#[test]
#[ignore = "writes data/overlay.json; run to regenerate generated base_url pins"]
fn generate_base_url_pins() {
    let providers = snapshot_providers();
    let mut overlay = serde_json::from_str::<Value>(
        &std::fs::read_to_string(overlay_path()).expect("read overlay.json"),
    )
    .expect("overlay is JSON");
    let patch = overlay
        .get_mut("patch")
        .and_then(Value::as_object_mut)
        .expect("overlay.patch is an object");

    // Preserve every pre-existing or later hand-written endpoint as authored unless
    // it is already tagged generated. Generation must never relabel a human choice.
    for entry in patch.values_mut() {
        let Some(obj) = entry.as_object_mut() else {
            continue;
        };
        if !obj.contains_key("base_url") {
            continue;
        }
        match obj.get(BASE_URL_SOURCE).and_then(Value::as_str) {
            Some(SOURCE_GENERATED) | Some(SOURCE_AUTHORED) => {}
            _ => {
                obj.insert(BASE_URL_SOURCE.to_string(), json!(SOURCE_AUTHORED));
            }
        }
    }

    let document_pins: BTreeMap<String, String> = providers
        .iter()
        .filter_map(|(id, entry)| {
            document_base_url(entry).map(|base_url| (id.clone(), base_url.to_string()))
        })
        .collect();

    // Remove only stale endpoint fields. Provider entries also carry independent
    // wire-family and model patches that must survive an endpoint disappearing.
    for (id, entry) in patch.iter_mut() {
        let Some(obj) = entry.as_object_mut() else {
            continue;
        };
        if obj.get(BASE_URL_SOURCE).and_then(Value::as_str) == Some(SOURCE_GENERATED)
            && !document_pins.contains_key(id)
        {
            obj.remove("base_url");
            obj.remove(BASE_URL_SOURCE);
        }
    }

    for (id, base_url) in document_pins {
        let obj = patch
            .entry(id.clone())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .unwrap_or_else(|| panic!("{id}: overlay patch must be an object"));
        if obj.get(BASE_URL_SOURCE).and_then(Value::as_str) == Some(SOURCE_AUTHORED) {
            continue;
        }
        obj.insert("base_url".to_string(), json!(base_url));
        obj.insert(BASE_URL_SOURCE.to_string(), json!(SOURCE_GENERATED));
    }

    // Rewrite patch with sorted keys so a regenerate is reviewable.
    let sorted = patch
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<BTreeMap<_, _>>();
    *patch = sorted.into_iter().collect();

    let generated_count = patch
        .values()
        .filter(|entry| {
            entry.get(BASE_URL_SOURCE).and_then(Value::as_str) == Some(SOURCE_GENERATED)
        })
        .count();
    let mut out = serde_json::to_string_pretty(&overlay).expect("serialize overlay");
    out.push('\n');
    std::fs::write(overlay_path(), out).expect("write overlay.json");
    eprintln!(
        "wrote {generated_count} generated base_url pins to {}",
        overlay_path().display()
    );
}

#[test]
fn every_overlay_base_url_pin_declares_its_source_and_is_nonempty() {
    let overlay = overlay_value();
    let patch = overlay
        .get("patch")
        .and_then(Value::as_object)
        .expect("overlay.patch is an object");
    let mut invalid = Vec::new();
    for (id, entry) in patch {
        let Some(base_url) = entry.get("base_url") else {
            continue;
        };
        let source = entry.get(BASE_URL_SOURCE).and_then(Value::as_str);
        if base_url.as_str().is_none_or(str::is_empty)
            || !matches!(source, Some(SOURCE_GENERATED) | Some(SOURCE_AUTHORED))
        {
            invalid.push(format!("{id}: base_url={base_url:?}, source={source:?}"));
        }
    }
    assert!(
        invalid.is_empty(),
        "every overlay base_url pin must be a non-empty string and declare base_url_source as \
         generated or authored: {invalid:?}"
    );
}

#[test]
fn generated_base_url_pins_preserve_every_provider_base_url() {
    let providers = snapshot_providers();
    let overlay = overlay_value();
    let patch = overlay
        .get("patch")
        .and_then(Value::as_object)
        .expect("overlay.patch is an object");

    for (id, entry) in &providers {
        // The authored OpenAI pin now sets the Responses base URL to https://api.openai.com/v1.
        let before = (id == "openai")
            .then_some("https://api.openai.com/v1")
            .or_else(|| pre_migration_overlay_base_url(id))
            .or_else(|| entry.get("api").and_then(Value::as_str))
            .unwrap_or("");
        let after = mapped_base_url(id, entry, patch);
        assert_eq!(
            after, before,
            "{id}: base_url after overlay pins must equal the old merged entry.get(\"api\") answer"
        );
    }
}

#[test]
fn generated_pins_are_exactly_the_document_endpoints() {
    let providers = snapshot_providers();
    let overlay = overlay_value();
    let patch = overlay
        .get("patch")
        .and_then(Value::as_object)
        .expect("overlay.patch is an object");
    let expected = generated_base_url_pins(&providers);

    let generated = patch
        .iter()
        .filter(|(_, entry)| {
            entry.get(BASE_URL_SOURCE).and_then(Value::as_str) == Some(SOURCE_GENERATED)
        })
        .map(|(id, entry)| {
            let base_url = entry
                .get("base_url")
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("{id}: generated pin missing string base_url"));
            (id.clone(), base_url.to_string())
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        generated, expected,
        "generated overlay pins must exactly copy non-empty document api fields, excluding frozen authored overrides"
    );
}

#[test]
fn pre_migration_overlay_endpoints_stay_authored() {
    let overlay = overlay_value();
    let patch = overlay
        .get("patch")
        .and_then(Value::as_object)
        .expect("overlay.patch is an object");
    for (id, base_url) in PRE_MIGRATION_OVERLAY_PINS {
        assert_eq!(
            pin_source(patch, id),
            Some(SOURCE_AUTHORED),
            "{id}: an endpoint authored before generation must stay authored"
        );
        assert_eq!(
            current_overlay_base_url(patch, id),
            Some(*base_url),
            "{id}: authored endpoint must not change under generation"
        );
    }
}

#[test]
fn provider_without_endpoint_pin_uses_the_family_default_endpoint() {
    let providers = snapshot_providers();
    assert_eq!(
        document_base_url(providers.get("openai").expect("snapshot has openai")),
        None,
        "the fallback witness must have no document api"
    );
    // Exercise the no-pin fallback independently of the bundled overlay's explicit OpenAI host.
    let overlay = json!({"patch": {}});
    let patch = overlay
        .get("patch")
        .and_then(Value::as_object)
        .expect("overlay.patch is an object");
    assert_eq!(
        current_overlay_base_url(patch, "openai"),
        None,
        "the fallback witness must have no overlay endpoint pin"
    );

    let catalog = Catalog::from_json_str(SNAPSHOT, &overlay.to_string()).expect("catalog parses");
    let (provider, model) = catalog
        .model_for_serving("openai", "gpt-4.1")
        .expect("openai/gpt-4.1 is served");
    assert!(
        provider.base_url.is_empty(),
        "an absent pin remains the ProviderSpec fallback sentinel"
    );
    let frozen = resolve_frozen(
        &provider,
        &model,
        4096,
        renderer_version_for(WireFamilyId::OpenAiChat),
    )
    .expect("openai/gpt-4.1 freezes");
    assert_eq!(
        frozen.base_url, None,
        "the empty ProviderSpec sentinel must become no endpoint override"
    );
    let request = wire_family(WireFamilyId::OpenAiChat)
        .expect("OpenAI Chat family is registered")
        .endpoint(&frozen, &AuthHandle::None, Vec::new());
    assert_eq!(
        request.url, "https://api.openai.com/v1/chat/completions",
        "an absent pin must select the family default instead of forming a URL from an empty host"
    );
}
