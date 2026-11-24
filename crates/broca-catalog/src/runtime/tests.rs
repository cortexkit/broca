use super::*;
use std::fs;

thread_local! {
    pub(super) static AFTER_REFRESH_SNAPSHOT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[test]
fn overlapping_refresh_cannot_publish_an_older_catalog() {
    let path = temp_lkg("overlapping-refresh");
    let cache = Arc::new(CatalogCache::open(&path));
    let initial = response(100);
    cache.refresh(initial.clone()).unwrap();
    let mut older = initial.clone();
    older.catalog_version += 1;
    older.resolved_at_ms = 200;
    let mut newer = initial;
    newer.catalog_version += 2;
    newer.resolved_at_ms = 300;
    let winning_version = newer.catalog_version;
    let winning = Arc::clone(&cache);
    let (send, receive) = std::sync::mpsc::channel();
    AFTER_REFRESH_SNAPSHOT.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let serial = winning.refresh_writer.try_lock().is_err();
            let winner = std::thread::spawn(move || winning.refresh(newer).unwrap());
            // Without a writer guard the other refresh can finish while this one is paused
            // with a stale snapshot. With the guard, let both finish in publication order.
            if serial {
                send.send(Some(winner)).unwrap();
            } else {
                winner.join().unwrap();
                send.send(None).unwrap();
            }
        }));
    });

    let result = cache.refresh(older);
    if let Some(winner) = receive.recv().unwrap() {
        winner.join().unwrap();
    }
    let live = cache.snapshot().unwrap().catalog_version();
    let restarted = CatalogCache::open(&path)
        .snapshot()
        .unwrap()
        .catalog_version();
    assert_eq!((live, restarted), (winning_version, winning_version), "a slow refresh must not replace the newer catalog in memory or on disk; result: {result:?}");
}

#[test]
fn temperature_fusiform_real_shaped_facts_decode_without_other_fields() {
    use broca_core::provider::TemperaturePolicy;
    for (identity, facts, expected) in [
        (
            "anthropic/claude-opus-5-5",
            serde_json::json!({"capability.temperature": false}),
            TemperaturePolicy {
                supported: Some(false),
                min: None,
                max: None,
            },
        ),
        (
            "google/antigravity-claude-opus-5-5",
            serde_json::json!({"capability.temperature": false}),
            TemperaturePolicy {
                supported: Some(false),
                min: None,
                max: None,
            },
        ),
        (
            "google/gemini-4-carbon",
            serde_json::json!({"capability.temperature": false}),
            TemperaturePolicy {
                supported: Some(false),
                min: None,
                max: None,
            },
        ),
        (
            "google/antigravity-gemini-4-carbon",
            serde_json::json!({"capability.temperature": false}),
            TemperaturePolicy {
                supported: Some(false),
                min: None,
                max: None,
            },
        ),
        (
            "openai/gpt-4.1",
            serde_json::json!({"capability.temperature": true, "limit.temperature_min": 0, "limit.temperature_max": 2}),
            TemperaturePolicy {
                supported: Some(true),
                min: Some(0.0),
                max: Some(2.0),
            },
        ),
        (
            "deepseek/deepseek-v4-flash",
            serde_json::json!({"capability.temperature": true, "limit.temperature_min": 0, "limit.temperature_max": 2}),
            TemperaturePolicy {
                supported: Some(true),
                min: Some(0.0),
                max: Some(2.0),
            },
        ),
        (
            "xai/grok-4.7",
            serde_json::json!({"capability.temperature": true, "limit.temperature_min": 0, "limit.temperature_max": 2}),
            TemperaturePolicy {
                supported: Some(true),
                min: Some(0.0),
                max: Some(2.0),
            },
        ),
        (
            "google/gemini-range",
            serde_json::json!({"capability.temperature": true, "limit.temperature_min": 0.0, "limit.temperature_max": 2.0}),
            TemperaturePolicy {
                supported: Some(true),
                min: Some(0.0),
                max: Some(2.0),
            },
        ),
        (
            "anthropic/claude-range",
            serde_json::json!({"limit.temperature_min": 0.0, "limit.temperature_max": 1.0}),
            TemperaturePolicy {
                supported: None,
                min: Some(0.0),
                max: Some(1.0),
            },
        ),
    ] {
        let facts = serde_json::from_value(facts).unwrap();
        let model = serving_model_from_facts(identity, &facts).unwrap();
        assert_eq!(model.temperature, expected, "{identity}");
        assert_eq!(
            model.to_spec("model").capabilities.temperature,
            expected
                .supported
                .map_or(CapabilityFlag::Absent, CapabilityFlag::Known)
        );
    }
    assert_eq!(
        serving_model_from_facts("openai/unknown", &BTreeMap::new())
            .unwrap()
            .temperature,
        TemperaturePolicy::default()
    );
}

#[test]
fn temperature_malformed_fact_does_not_exclude_other_facts_or_the_model() {
    use broca_core::provider::TemperaturePolicy;
    let facts = serde_json::from_value(serde_json::json!({
        "capability.temperature": "false", "limit.temperature_min": 0.0,
        "limit.temperature_max": "2.0"
    }))
    .unwrap();
    assert_eq!(
        serving_model_from_facts("openai/gpt-4.1", &facts)
            .unwrap()
            .temperature,
        TemperaturePolicy {
            supported: None,
            min: Some(0.0),
            max: None
        }
    );
}

#[test]
fn temperature_newly_requested_facts_rebuild_an_equal_version_catalog() {
    for key in TEMPERATURE_FACTS {
        assert!(FUSIFORM_FACT_PREFIXES.contains(&key));
    }
    let cache = CatalogCache::empty();
    let mut page = response(100);
    cache.refresh(page.clone()).unwrap();
    let identity = "anthropic/claude-sonnet-4-5".to_owned();
    let (provider, model) = identity.split_once('/').unwrap();
    assert_eq!(
        cache
            .snapshot()
            .unwrap()
            .temperature_policy(provider, model)
            .supported,
        None
    );
    page.models
        .get_mut(&identity)
        .unwrap()
        .insert("capability.temperature".into(), Value::Bool(true));
    cache.refresh(page.clone()).unwrap();
    assert_eq!(
        cache
            .snapshot()
            .unwrap()
            .temperature_policy(provider, model)
            .supported,
        Some(true)
    );
    page.models
        .get_mut(&identity)
        .unwrap()
        .insert("limit.temperature_min".into(), serde_json::json!(0.0));
    page.models
        .get_mut(&identity)
        .unwrap()
        .insert("limit.temperature_max".into(), serde_json::json!(2.0));
    cache.refresh(page).unwrap();
    assert_eq!(
        cache
            .snapshot()
            .unwrap()
            .temperature_policy(provider, model)
            .max,
        Some(2.0)
    );
}
use broca_provider::families::{renderer_version_for, resolve_frozen};
use std::time::{SystemTime, UNIX_EPOCH};

fn response(resolved_at_ms: i64) -> FusiformCatalogResponse {
    FusiformCatalogResponse {
        source: "models.dev".into(),
        resolved_at_ms,
        catalog_version: 9,
        models: BTreeMap::from([
            (
                "anthropic/claude-sonnet-4-5".into(),
                BTreeMap::from([
                    ("existence".into(), serde_json::json!("present")),
                    ("limit.context".into(), serde_json::json!(200000)),
                    ("capability.attachment".into(), serde_json::Value::Null),
                    ("capability.reasoning".into(), serde_json::json!(true)),
                    ("capability.tool_call".into(), serde_json::json!(true)),
                ]),
            ),
            (
                "anthropic/claude-retired".into(),
                BTreeMap::from([("existence".into(), serde_json::json!("retired"))]),
            ),
        ]),
        withheld: Vec::new(),
        uncertain: Vec::new(),
        wire_corrections: Vec::new(),
    }
}

#[test]
fn live_fusiform_input_modalities_reach_serving_models() {
    let mut response = response(100);
    let facts = response
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap();
    facts.insert("capability.attachment".into(), serde_json::json!(false));
    facts.insert(
        "capability.input_modalities".into(),
        serde_json::json!(["text", "image", "pdf", "future-modality"]),
    );
    // Apply the catalog.get prefix filter so a missing request cannot pass this test.
    for facts in response.models.values_mut() {
        facts.retain(|fact, _| {
            FUSIFORM_FACT_PREFIXES
                .iter()
                .any(|prefix| fact.starts_with(prefix))
        });
    }
    let cache = CatalogCache::empty();
    cache.refresh(response).unwrap();
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.source(), CatalogSource::Fusiform);
    let (_, model) = snapshot
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .unwrap();
    assert_eq!(
        model.input_modalities,
        InputModalities {
            text: Some(true),
            image: Some(true),
            pdf: Some(true),
            audio: Some(false),
            video: Some(false),
        }
    );
    assert_eq!(model.capabilities.attachment, CapabilityFlag::Known(false));
}

#[test]
fn malformed_fusiform_input_modalities_still_serve_text() {
    for value in [
        None,
        Some(Value::Null),
        Some(serde_json::json!("image")),
        Some(serde_json::json!(true)),
        Some(serde_json::json!({"input": ["image"]})),
        Some(serde_json::json!(["image", 42])),
        Some(serde_json::json!([null])),
    ] {
        let cache = CatalogCache::empty();
        let mut before = response(100);
        before
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .unwrap()
            .insert(
                "capability.input_modalities".into(),
                serde_json::json!(["text", "image", "pdf"]),
            );
        cache.refresh(before).unwrap();
        let mut after = response(200);
        after.catalog_version += 1;
        let facts = after.models.get_mut("anthropic/claude-sonnet-4-5").unwrap();
        facts.insert("capability.attachment".into(), serde_json::json!(true));
        // A snapshot-shaped key is not a second source for a Fusiform row.
        facts.insert(
            "modalities".into(),
            serde_json::json!({"input": ["image", "pdf"]}),
        );
        if let Some(value) = value.clone() {
            facts.insert("capability.input_modalities".into(), value);
        }
        cache.refresh(after).unwrap();
        let snapshot = cache.snapshot().unwrap();
        let (provider, model) = snapshot
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .expect("unknown modalities must not exclude a text-serving row");
        assert_eq!(
            model.input_modalities,
            InputModalities::default(),
            "{value:?}"
        );
        assert_eq!(cache.health(200).unwrap().rows_excluded, 0);
        resolve_frozen(
            &provider,
            &model,
            4096,
            renderer_version_for(provider.wire_family),
        )
        .expect("text configuration remains usable with unknown modalities");
    }
}

#[test]
fn an_equal_version_poll_that_newly_carries_input_modalities_rebuilds_the_index() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).unwrap();
    let before = cache.snapshot().unwrap();
    let mut after = response(200);
    after
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap()
        .insert(
            "capability.input_modalities".into(),
            serde_json::json!(["text", "image", "pdf"]),
        );
    cache.refresh(after.clone()).unwrap();
    let snapshot = cache.snapshot().unwrap();
    assert!(!snapshot.shares_index_with(&before));
    assert_eq!(
        snapshot
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .unwrap()
            .1
            .input_modalities
            .pdf,
        Some(true)
    );
    cache.refresh(after).unwrap();
    assert!(cache.snapshot().unwrap().shares_index_with(&snapshot));
    assert!(before
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .unwrap()
        .1
        .input_modalities
        .is_unknown());
}

/// Fusiform's `capability.structured_output` maps like the other capability facts, except
/// that an omitted fact reads as the catalog not saying rather than refusing the row.
#[test]
fn structured_output_fact_maps_to_a_three_state_capability() {
    for (fact, expected) in [
        (Some(serde_json::json!(true)), CapabilityFlag::Known(true)),
        (Some(serde_json::json!(false)), CapabilityFlag::Known(false)),
        (Some(serde_json::Value::Null), CapabilityFlag::Unknown),
        (None, CapabilityFlag::Absent),
    ] {
        let mut response = response(10);
        let facts = response
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .unwrap();
        if let Some(fact) = fact.clone() {
            facts.insert("capability.structured_output".into(), fact);
        }
        let (catalog, _) =
            ServingCatalog::from_fusiform_with_overlay(&response.models, RuntimeOverlay::default())
                .unwrap();
        let (_, model) = catalog
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .unwrap();
        assert_eq!(model.capabilities.structured_output, expected, "{fact:?}");
    }
    let mut response = response(10);
    response
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap()
        .insert(
            "capability.structured_output".into(),
            serde_json::json!("yes"),
        );
    // A malformed value is still not read as a capability: the row is excluded instead.
    assert!(loaded_excludes_for_fact(
        &response,
        "capability.structured_output"
    ));
    assert!(FUSIFORM_FACT_PREFIXES.contains(&"capability.structured_output"));
}

#[test]
fn legacy_overridden_member_is_ignored() {
    let mut legacy = serde_json::to_value(response(100)).expect("serialize response");
    legacy.as_object_mut().expect("response object").insert(
        "overridden".into(),
        serde_json::json!([{
            "provider_id": "anthropic",
            "model_id": "claude-sonnet-4-5",
            "fact_key": "limit.context",
            "upstream_value": "1000000",
            "served_value": "200000",
            "authority": "https://example.invalid"
        }]),
    );

    let decoded: FusiformCatalogResponse =
        serde_json::from_value(legacy).expect("legacy catalog fields are ignored");
    assert_eq!(decoded.catalog_version, 9);
    assert_eq!(decoded.models.len(), 2);
}

#[test]
fn compact_index_retains_the_cause_of_an_overlay_removed_identity() {
    let response: FusiformCatalogResponse = serde_json::from_str(include_str!(
        "../../tests/fixtures/fusiform_catalog_get_live.json"
    ))
    .expect("captured live fusiform response must deserialize");
    // The captured fixture (2026-08-13) still lists deepseek-chat; a test-local remove gives
    // the RECORDING of a removal a subject without the production overlay having to carry a
    // suppression for a row upstream no longer serves.
    let overlay: RuntimeOverlay = serde_json::from_str(
        r#"{ "patch": { "deepseek": { "wire_family": "openai_chat" } },
             "remove": { "deepseek": ["deepseek-chat"] } }"#,
    )
    .expect("test overlay parses");
    let (catalog, _) = ServingCatalog::from_fusiform_with_overlay(&response.models, overlay)
        .expect("compact serving index materializes");

    assert!(
        catalog
            .model_for_serving("deepseek", "deepseek-chat")
            .is_none(),
        "the overlay-removed row must remain absent from the serving index"
    );
    assert!(catalog.is_overlay_removed("deepseek", "deepseek-chat"));
    assert!(!catalog.is_overlay_removed("deepseek", "no-such-model"));
    assert!(!catalog.is_withdrawn("deepseek", "deepseek-chat"));
}

#[test]
fn compact_index_freezes_every_live_fixture_model_like_the_legacy_catalog() {
    let response: FusiformCatalogResponse = serde_json::from_str(include_str!(
        "../../tests/fixtures/fusiform_catalog_get_live.json"
    ))
    .expect("captured live fusiform response must deserialize");
    assert_eq!(
        response.models.len(),
        6_328,
        "the all-model gate must not silently shrink with its fixture"
    );

    let legacy = legacy_catalog(&response).expect("legacy catalog materializes");
    let compact = loaded(&response, CatalogSource::Fusiform, None)
        .expect("compact serving index materializes");
    let freeze = |pair: Option<(ProviderSpec, ModelSpec)>| {
        pair.map(|(provider, model)| {
            resolve_frozen(
                &provider,
                &model,
                4_096,
                renderer_version_for(provider.wire_family),
            )
            .map_err(|error| error.to_string())
        })
    };
    let consumed_model_facts = |pair: &Option<(ProviderSpec, ModelSpec)>| {
        pair.as_ref().map(|(_, model)| {
            (
                model.limits.context,
                model.limits.max_output,
                model.capabilities.attachment,
                model.capabilities.reasoning,
                model.capabilities.tool_call,
            )
        })
    };

    let mut compared = 0usize;
    for identity in response.models.keys() {
        let (provider_id, model_id) = identity
            .split_once('/')
            .expect("fixture identities were validated by both materializers");
        let old_pair = legacy.model_for_serving(provider_id, model_id);
        let new_pair = compact.catalog.model_for_serving(provider_id, model_id);
        assert_eq!(
            consumed_model_facts(&new_pair),
            consumed_model_facts(&old_pair),
            "consumed model facts changed for {identity}"
        );
        assert_eq!(
            freeze(new_pair),
            freeze(old_pair),
            "frozen resolution changed for {identity}"
        );
        compared += 1;
    }
    assert_eq!(compared, 6_328, "the gate must compare every fixture row");

    assert!(legacy
        .model_for_serving("xai", "grok-composer-2.5-fast")
        .is_none());
    assert!(
        compact
            .catalog
            .model_for_serving("xai", "grok-composer-2.5-fast")
            .is_none(),
        "the overlay cannot reinstate a retired row"
    );
}

/// An omitted capability flag or context window loads as unstated rather than refusing
/// anything, and an explicit null stays distinct from an omission.
///
/// This used to pin the opposite: an omitted fact refused the whole catalog, so that
/// defaulting could never silently change a model. That refused every refresh from
/// 2026-10-21 because fusiform's hand-curated decision models (jev, clef, clairic) carry only
/// the facts their vendors state. An omission now reads as "not stated", which each consumer
/// already handles (see `optional_capability_fact` and `context_fact`), and a row whose fact
/// is MALFORMED is excluded on its own and counted on `health.check`.
#[test]
fn a_model_missing_a_capability_or_context_fact_loads_it_as_unstated() {
    for fact in [
        "capability.reasoning",
        "capability.attachment",
        "capability.tool_call",
        "limit.context",
    ] {
        let mut response = response(100);
        let model = response
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .expect("fixture model");
        model.remove(fact);

        let cache = CatalogCache::empty();
        cache
            .refresh(response)
            .unwrap_or_else(|error| panic!("omitted {fact} must not refuse: {error}"));
        let snapshot = cache.snapshot().expect("installed");
        let (_, model) = snapshot
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .unwrap_or_else(|| panic!("omitted {fact} must not exclude the row"));
        // Reasoning is the one flag a send can refuse on, so its omission reads as
        // unknown (like null) rather than Absent, which admission would read as false.
        let (read, expected) = match fact {
            "capability.reasoning" => (model.capabilities.reasoning, CapabilityFlag::Unknown),
            "capability.attachment" => (model.capabilities.attachment, CapabilityFlag::Absent),
            "capability.tool_call" => (model.capabilities.tool_call, CapabilityFlag::Absent),
            _ => {
                assert_eq!(model.limits.context, None, "omitted limit.context");
                continue;
            }
        };
        assert_eq!(read, expected, "omitted {fact}");
        assert_eq!(cache.health(100).unwrap().rows_excluded, 0, "{fact}");
    }

    // An EXPLICIT null is different: it is the producer saying "the upstream does not
    // state this", which `resolve_frozen` refuses for a send that would reason.
    let mut response = response(100);
    response
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .expect("fixture model")
        .insert("capability.reasoning".into(), serde_json::Value::Null);
    let cache = CatalogCache::empty();
    cache
        .refresh(response)
        .expect("an explicit null is a stated unknown");
    let (_, model) = cache
        .snapshot()
        .unwrap()
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .unwrap();
    assert_eq!(model.capabilities.reasoning, CapabilityFlag::Unknown);
}

/// A present chat row as fusiform serves one, for the per-row isolation tests.
fn chat_row(context: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("existence".into(), serde_json::json!("present")),
        ("limit.context".into(), serde_json::json!(context)),
        ("capability.attachment".into(), serde_json::json!(true)),
        ("capability.reasoning".into(), serde_json::json!(true)),
        ("capability.tool_call".into(), serde_json::json!(true)),
    ])
}

/// Fusiform's curated rows next to ordinary chat rows, the page shape that refused every
/// refresh on 2026-10-21. The refresh succeeds through the real refresh path, the chat rows
/// are unchanged, and no curated row is excluded: an omitted reasoning flag reads as Unknown,
/// an omitted tool-call or attachment flag as Absent, and an omitted window as `None`.
#[test]
fn curated_decision_rows_beside_chat_rows_refresh_without_refusing() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).expect("first install");
    let before = cache
        .snapshot()
        .unwrap()
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .unwrap()
        .1;

    let mut page = response(200);
    page.catalog_version += 1;
    page.models
        .insert("openai/gpt-chat".into(), chat_row(400_000));
    for (identity, facts) in curated_rows() {
        page.models.insert(identity, facts);
    }
    cache
        .refresh(page)
        .expect("curated rows must not refuse the refresh");

    let health = cache.health(200).unwrap();
    assert_eq!((health.consecutive_refusals, health.total_refusals), (0, 0));
    assert_eq!(health.rows_excluded, 0, "{:?}", health.rows_excluded_reason);

    let snapshot = cache.snapshot().unwrap();
    let after = snapshot
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .unwrap()
        .1;
    assert_eq!(
        (
            after.limits.context,
            after.capabilities.attachment,
            after.capabilities.reasoning,
            after.capabilities.tool_call,
        ),
        (
            before.limits.context,
            before.capabilities.attachment,
            before.capabilities.reasoning,
            before.capabilities.tool_call,
        ),
        "a chat row must be unchanged by its curated neighbours"
    );
    assert_eq!(snapshot.context_limit("openai", "gpt-chat"), Some(400_000));

    // Broca resolves no wire family for the `typesafe` provider, so jev is listed but not
    // servable whatever its facts say: it is a known identity, neither served nor excluded.
    // Its facts are read through the same assembly with a test overlay giving the provider
    // a family.
    assert!(!snapshot.contains_model("typesafe", "jev-1.13.0"));
    let overlay: RuntimeOverlay = serde_json::from_str(
        r#"{ "patch": { "typesafe": { "wire_family": "openai_chat" }, "cloudflare-workers-ai": { "wire_family": "openai_chat" } } }"#,
    )
    .expect("test overlay parses");
    let models = curated_rows();
    let assembled = ServingCatalog::assemble_with_overlay(&models, overlay, None)
        .expect("curated rows assemble");
    assert!(
        assembled.excluded_rows.is_empty(),
        "{:?}",
        assembled.excluded_rows
    );
    let (_, jev) = assembled
        .catalog
        .model_for_serving("typesafe", "jev-1.13.0")
        .expect("jev assembles");
    assert_eq!(jev.limits.context, None, "jev states no context window");
    assert_eq!(jev.capabilities.attachment, CapabilityFlag::Absent);
    assert_eq!(jev.capabilities.reasoning, CapabilityFlag::Unknown);
    assert_eq!(jev.capabilities.tool_call, CapabilityFlag::Absent);
    for model in ["@cf/cloudflare/clef", "@cf/cloudflare/clairic"] {
        assert!(
            snapshot
                .model_for_serving("cloudflare-workers-ai", model)
                .is_none(),
            "a row without routing facts has metadata but no chat route"
        );
        let (provider, row) = assembled
            .catalog
            .model_for_serving("cloudflare-workers-ai", model)
            .unwrap_or_else(|| panic!("{model} has a test renderer"));
        assert_eq!(row.limits.context, Some(65_536), "{model}");
        assert_eq!(row.capabilities.attachment, CapabilityFlag::Known(true));
        assert_eq!(row.capabilities.reasoning, CapabilityFlag::Unknown);
        assert_eq!(row.capabilities.tool_call, CapabilityFlag::Absent);
        // An unstated reasoning flag refuses only a send that would reason; this family
        // never reasons, so the curated row still admits.
        let frozen = resolve_frozen(
            &provider,
            &row,
            4_096,
            renderer_version_for(provider.wire_family),
        )
        .unwrap_or_else(|error| panic!("{model} must admit: {error}"));
        assert_eq!(
            frozen.reasoning,
            broca_provider::policy::ReasoningPolicy::None
        );
    }
}

/// A reasoning row whose `capability.reasoning` fusiform stops serving is refused for a send
/// that would reason, the same as an explicit null, rather than running without reasoning.
///
/// Read as `Absent`, admission would freeze `ReasoningPolicy::None` and the model would
/// silently stop reasoning: "could not tell" would have become a known `false`.
#[test]
fn a_reasoning_row_that_loses_its_flag_refuses_a_reasoning_send() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).expect("first install");
    let (provider, model) = cache
        .snapshot()
        .unwrap()
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .expect("served");
    let frozen = resolve_frozen(
        &provider,
        &model,
        4_096,
        renderer_version_for(provider.wire_family),
    )
    .expect("a stated reasoning model admits");
    assert_ne!(
        frozen.reasoning,
        broca_provider::policy::ReasoningPolicy::None,
        "the fixture family must reason, or the refusal below proves nothing"
    );

    let mut page = response(200);
    page.catalog_version += 1;
    page.models
        .get_mut("anthropic/claude-sonnet-4-5")
        .expect("fixture row")
        .remove("capability.reasoning");
    cache
        .refresh(page)
        .expect("an omitted flag does not refuse");
    let (provider, model) = cache
        .snapshot()
        .unwrap()
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .expect("still served");
    assert_eq!(model.capabilities.reasoning, CapabilityFlag::Unknown);
    let refused = resolve_frozen(
        &provider,
        &model,
        4_096,
        renderer_version_for(provider.wire_family),
    );
    assert!(
        matches!(&refused, Err(error) if error.capability == "reasoning"),
        "a reasoning send must be refused, not run without reasoning: {:?}",
        refused.map(|frozen| frozen.reasoning)
    );
}

/// A row the installed catalog served keeps its previous model when it arrives malformed,
/// and the refresh around it still succeeds and is not counted as a refusal.
#[test]
fn a_served_row_that_arrives_malformed_keeps_its_previous_model() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).expect("first install");

    let mut page = response(200);
    page.catalog_version += 1;
    page.models
        .insert("openai/gpt-chat".into(), chat_row(400_000));
    let row = page
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .expect("fixture row");
    // The window changes too, so serving it would show the malformed row was half-read.
    row.insert("limit.context".into(), serde_json::json!(1_000));
    row.insert("capability.tool_call".into(), serde_json::json!("yes"));
    cache
        .refresh(page.clone())
        .expect("one malformed row must not refuse the refresh");

    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.resolved_at_ms(), 200, "the refresh installed");
    assert_eq!(snapshot.context_limit("openai", "gpt-chat"), Some(400_000));
    let (_, kept) = snapshot
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .expect("the previously served row keeps serving");
    assert_eq!(kept.limits.context, Some(200_000), "the PREVIOUS model");
    assert_eq!(kept.capabilities.tool_call, CapabilityFlag::Known(true));

    let health = cache.health(200).unwrap();
    assert_eq!((health.consecutive_refusals, health.total_refusals), (0, 0));
    assert_eq!(health.rows_excluded, 1);
    let reason = health.rows_excluded_reason.expect("a reason");
    assert!(
        reason.contains("claude-sonnet-4-5")
            && reason.contains("capability.tool_call")
            && reason.contains("last-known-good"),
        "{reason}"
    );

    // Still malformed on the next refresh: the kept model is kept again, not lost.
    let mut next = page;
    next.catalog_version += 1;
    next.resolved_at_ms = 300;
    cache.refresh(next).expect("still not a refusal");
    let (_, kept) = cache
        .snapshot()
        .unwrap()
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .expect("kept across refreshes");
    assert_eq!(kept.limits.context, Some(200_000));
}

#[test]
fn a_kept_row_keeps_its_route_and_batch_facts_before_and_after_restart() {
    let path = temp_lkg("kept-route-and-batch-row");
    let cache = CatalogCache::open(&path);
    let mut page = response(100);
    let id = "mixed-provider/decision";
    let mut row = chat_row(10_000);
    row.insert("model.kind".into(), serde_json::json!("decision"));
    row.insert(
        "wire.npm".into(),
        serde_json::json!({"value":"@ai-sdk/openai-compatible", "from":"curated"}),
    );
    row.insert(
        "wire.api".into(),
        serde_json::json!({"value":"https://old.example/v1", "from":"curated"}),
    );
    page.models.insert(id.into(), row);
    cache.refresh(page.clone()).unwrap();
    let before = cache.snapshot().unwrap();
    let route = before.resolve_route("mixed-provider", "decision").unwrap();
    let batch_row = before.model_batch_row(id).unwrap();
    assert_eq!(batch_row.kind.as_deref(), Some("decision"));

    page.catalog_version += 1;
    let row = page.models.get_mut(id).unwrap();
    row.insert("capability.tool_call".into(), serde_json::json!("yes"));
    row.remove("wire.api");
    row.remove("wire.npm");
    cache.refresh(page).unwrap();

    for snapshot in [
        cache.snapshot().unwrap(),
        CatalogCache::open(&path).snapshot().unwrap(),
    ] {
        assert_eq!(
            (
                snapshot.resolve_route("mixed-provider", "decision"),
                snapshot.model_batch_row(id)
            ),
            (Ok(route.clone()), Some(batch_row.clone())),
            "the rejected row must not change the previous route or batch facts"
        );
        assert!(snapshot.contains_model("mixed-provider", "decision"));
    }
}

#[cfg(unix)]
#[test]
fn lkg_refresh_requires_a_directory_sync_after_rename() {
    use std::os::unix::fs::PermissionsExt;

    let path = temp_lkg("lkg-directory-sync");
    let cache = CatalogCache::open(&path);
    let parent = path.parent().unwrap();
    fs::create_dir_all(parent).unwrap();
    let old_permissions = fs::metadata(parent).unwrap().permissions();
    fs::set_permissions(parent, fs::Permissions::from_mode(0o300)).unwrap();
    let cannot_open_directory = fs::File::open(parent).is_err();
    let result = cache.refresh(response(100));
    fs::set_permissions(parent, old_permissions).unwrap();
    assert!(
        cannot_open_directory,
        "fixture requires a process without DAC override"
    );
    assert!(
        path.is_file(),
        "rename must succeed before the directory sync fails"
    );
    assert!(
        matches!(result, Err(CatalogRefreshError::Persist { .. })),
        "refresh accepted an LKG without a directory sync: {result:?}"
    );
    assert!(cache.snapshot().is_err());
}

/// The LKG a refresh writes is the catalog a restart serves: a row kept from the replaced
/// catalog because it arrived malformed keeps its earlier row on disk too, and a same-version
/// response a restart couldn't load at all is refused rather than written over a good LKG.
#[test]
fn the_lkg_a_refresh_writes_reopens_to_the_catalog_it_served() {
    let path = temp_lkg("kept-row-reopens");
    let cache = CatalogCache::open(&path);
    cache.refresh(response(100)).expect("first install");
    let mut page = response(200);
    page.catalog_version += 1;
    page.models
        .get_mut("anthropic/claude-sonnet-4-5")
        .expect("fixture row")
        .insert("capability.tool_call".into(), serde_json::json!("yes"));
    cache
        .refresh(page.clone())
        .expect("a malformed row isn't a refusal");
    let reopened = CatalogCache::open(&path);
    let (_, kept) = reopened
        .snapshot()
        .expect("the LKG reopens")
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .expect("the kept row serves after a restart");
    assert_eq!(kept.capabilities.tool_call, CapabilityFlag::Known(true));

    // Same version, same rows, but one row's existence is gone: the document as a whole is
    // unusable at boot, so it is refused and the good LKG stays.
    let mut broken = page;
    broken.resolved_at_ms = 300;
    broken
        .models
        .values_mut()
        .next()
        .expect("a row")
        .remove("existence");
    assert!(
        cache.refresh(broken).is_err(),
        "an unbootable document is refused"
    );
    assert!(
        CatalogCache::open(&path).snapshot().is_ok(),
        "the earlier LKG still opens"
    );
}

/// A row never served before that arrives malformed is left out, with one WARN naming the
/// model, the fact and the reason, and the health count; everything else serves.
#[test]
fn a_never_seen_malformed_row_is_excluded_with_a_warn_and_the_counter() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).expect("first install");

    let mut page = response(200);
    page.catalog_version += 1;
    let mut bad = chat_row(128_000);
    bad.insert("limit.context".into(), serde_json::json!("128k"));
    page.models.insert("openai/never-seen".into(), bad);
    page.models
        .insert("openai/gpt-chat".into(), chat_row(400_000));

    let capture = broca_test_support::LogCapture::new();
    capture
        .in_scope(|| cache.refresh(page))
        .expect("a malformed new row must not refuse the refresh");

    let snapshot = cache.snapshot().unwrap();
    assert!(!snapshot.contains_model("openai", "never-seen"));
    assert!(snapshot.contains_model("openai", "gpt-chat"));
    assert!(snapshot.contains_model("anthropic", "claude-sonnet-4-5"));

    let warns: Vec<_> = capture
        .events()
        .into_iter()
        .filter(|event| event.level == tracing::Level::WARN && event.target == "catalog")
        .collect();
    assert_eq!(warns.len(), 1, "one WARN per excluded row: {warns:?}");
    let warn = &warns[0];
    assert_eq!(warn.fields["model"], "openai/never-seen");
    assert_eq!(warn.fields["fact"], "limit.context");
    assert!(warn.fields["reason"].contains("expected unsigned integer"));
    assert_eq!(warn.fields["last_known_good_kept"], "false");

    let health = cache.health(200).unwrap();
    assert_eq!(
        (health.consecutive_refusals, health.total_refusals),
        (0, 0),
        "a refresh with exclusions is not a refusal"
    );
    assert_eq!(health.rows_excluded, 1);
    assert!(health
        .rows_excluded_reason
        .is_some_and(|reason| reason.contains("never-seen") && reason.contains("not served")));
}

/// An identity that does not split into provider and model excludes only that row.
#[test]
fn an_identity_that_does_not_split_excludes_only_its_row() {
    let mut page = response(100);
    page.models.insert("no-provider".into(), chat_row(1_000));
    let cache = CatalogCache::empty();
    cache
        .refresh(page)
        .expect("one bad identity must not refuse");
    let health = cache.health(100).unwrap();
    assert_eq!(health.rows_excluded, 1);
    assert!(health
        .rows_excluded_reason
        .is_some_and(|reason| reason.contains("no-provider") && reason.contains("identity")));
    assert!(cache
        .snapshot()
        .unwrap()
        .contains_model("anthropic", "claude-sonnet-4-5"));
}

/// Problems with the DOCUMENT still refuse the whole refresh even when a row is excluded,
/// and an excluded row counts as a known identity: losing it later is a vanished identity.
#[test]
fn document_level_problems_still_refuse_beside_an_excluded_row() {
    let cache = CatalogCache::empty();
    let mut first = response(100);
    let mut bad = chat_row(1_000);
    bad.insert("capability.reasoning".into(), serde_json::json!(1));
    first.models.insert("openai/bad".into(), bad);
    cache.refresh(first.clone()).expect("first install");
    assert_eq!(cache.health(100).unwrap().rows_excluded, 1);

    // Truncation: the response drops the excluded row's identity.
    let mut truncated = first.clone();
    truncated.catalog_version += 1;
    truncated.resolved_at_ms = 200;
    truncated.models.remove("openai/bad");
    assert!(
        matches!(
            cache.refresh(truncated).expect_err("a vanished identity refuses"),
            CatalogRefreshError::Incomplete { ref sample, .. } if sample == "openai/bad"
        ),
        "an excluded row must still count as a known identity"
    );

    // A vanished ordinary identity, with the bad row still present.
    let mut vanished = first.clone();
    vanished.catalog_version += 2;
    vanished.models.remove("anthropic/claude-retired");
    assert!(matches!(
        cache.refresh(vanished).expect_err("refused"),
        CatalogRefreshError::Incomplete { .. }
    ));

    // Qualified facts.
    let mut qualified = first;
    qualified.catalog_version += 3;
    qualified
        .withheld
        .push(serde_json::json!({"fact": "limit.context"}));
    assert!(matches!(
        cache.refresh(qualified).expect_err("refused"),
        CatalogRefreshError::Catalog(CatalogError::FusiformQualifiedFacts { .. })
    ));

    let health = cache.health(300).unwrap();
    assert_eq!(health.total_refusals, 3);
    assert_eq!(cache.snapshot().unwrap().resolved_at_ms(), 100);
}

/// The refusal names the instant the response resolved to, because that is what says WHO
/// should act.
///
/// A shrunken identity set has two causes that a missing-count alone cannot separate: the
/// producer's current view shrank (their problem), or this refresh asked for a historical
/// snapshot and legitimately got fewer models (ours). FUSI first told me the two responses
/// were byte-identical and later corrected it from their own source -- `resolved_at_ms` is
/// `at.unwrap_or(now)`, so it differs, and the discriminator was on the wire from the first
/// response. Nothing said to read it.
///
/// That is the detectability-versus-detection gap in its cheapest form: no new field, no
/// producer change, one value carried into the message a human actually reads.
#[test]
fn an_incomplete_refusal_names_the_instant_the_response_resolved_to() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).expect("first install");

    // A historical read: fewer identities, resolved to a past instant.
    let mut historical = response(200);
    historical.resolved_at_ms = 1_700_000_000_000;
    historical.catalog_version += 1;
    historical.models.remove("anthropic/claude-retired");
    let text = cache
        .refresh(historical)
        .expect_err("a shrunken response is refused")
        .to_string();
    assert!(
        text.contains("1700000000000"),
        "the refusal must carry the resolved instant so a reader can tell which cause \
         they are looking at, got: {text}"
    );
    assert!(
        text.contains("claude-retired"),
        "and it must still name what went missing, got: {text}"
    );
}

/// An unchanged poll reuses the serving index. Validation for a disk-backed cache still
/// assembles the document boot would load.
#[test]
fn an_unchanged_equal_version_poll_reuses_the_existing_index() {
    let cache = CatalogCache::empty();
    let first = response(100);
    let version = first.catalog_version;
    cache.refresh(first).expect("install");
    let before = cache.snapshot().expect("installed");

    let mut again = response(200);
    again.catalog_version = version;
    cache.refresh(again).expect("same-version refresh");
    let after = cache.snapshot().expect("installed");

    assert!(
        before.shares_index_with(&after),
        "an unchanged equal-version poll must republish the SAME index rather than rebuild it"
    );
    assert_eq!(
        after.resolved_at_ms(),
        200,
        "freshness still advances on a reused index"
    );
}

#[test]
fn equal_version_fact_changes_are_served_before_restart() {
    let path = temp_lkg("equal-version-facts");
    let cache = CatalogCache::open(&path);
    let mut page = response(100);
    let routed_row = |wire_id: &str| {
        let mut facts = chat_row(20_000);
        facts.insert(
            "wire.npm".into(),
            serde_json::json!({"value":"@ai-sdk/openai-compatible", "from":"curated"}),
        );
        facts.insert(
            "wire.api".into(),
            serde_json::json!({"value":"https://test.example/v1", "from":"curated"}),
        );
        if !wire_id.is_empty() {
            facts.insert(
                "route.mode_wire_ids".into(),
                serde_json::json!({"fast": wire_id}),
            );
        }
        facts
    };
    page.models
        .insert("openai/other".into(), routed_row("other-fast"));
    page.models.insert("openai/updated".into(), routed_row(""));
    cache.refresh(page.clone()).unwrap();
    let before = cache.snapshot().unwrap();
    assert!(before.mode_wire_ids("openai", "updated").is_empty());
    assert_eq!(
        before
            .mode_wire_ids("openai", "other")
            .get("fast")
            .map(String::as_str),
        Some("other-fast")
    );

    page.resolved_at_ms += 1;
    page.models
        .insert("openai/updated".into(), routed_row("updated-fast"));
    page.models
        .get_mut("openai/updated")
        .unwrap()
        .insert("limit.context".into(), serde_json::json!(10_000));
    cache.refresh(page).unwrap();
    let live = cache.snapshot().unwrap();
    let restarted = CatalogCache::open(&path).snapshot().unwrap();
    let facts = |snapshot: &CatalogSnapshot| {
        (
            snapshot.context_limit("openai", "updated"),
            snapshot.mode_wire_ids("openai", "updated"),
        )
    };
    assert_eq!(
        facts(&restarted),
        (
            Some(10_000),
            BTreeMap::from([("fast".into(), "updated-fast".into())])
        )
    );
    assert_eq!(
        facts(&live),
        facts(&restarted),
        "live and restarted catalogs disagree"
    );
}

/// An equal-version poll that is the first to carry routing facts rebuilds the index.
///
/// The LKG on disk was written before broca requested the fact, so the next poll arrives at
/// the same catalog version with the fact now present. Reusing the index there would serve
/// no output limits until fusiform happened to mint a new version.
#[test]
fn an_equal_version_poll_that_newly_carries_routing_facts_rebuilds_the_index() {
    let cache = CatalogCache::empty();
    let mut page = response(100);
    cache.refresh(page.clone()).unwrap();
    let before = cache.snapshot().unwrap();
    let facts = page.models.get_mut("anthropic/claude-sonnet-4-5").unwrap();
    facts.insert(
        "wire.npm".into(),
        serde_json::json!({"value":"@ai-sdk/anthropic", "from":"provider"}),
    );
    facts.insert(
        "wire.api".into(),
        serde_json::json!({"value":"https://served.example", "from":"curated"}),
    );
    page.resolved_at_ms += 1;
    cache.refresh(page).unwrap();
    let after = cache.snapshot().unwrap();
    assert!(
        !after.shares_index_with(&before),
        "new routing prefixes require a new serving index"
    );
    assert_eq!(
        after
            .resolve_route("anthropic", "claude-sonnet-4-5")
            .unwrap()
            .1,
        "https://served.example"
    );
}

/// An equal-version poll that is the first to carry `capability.server_side_fallback`
/// rebuilds the index.
///
/// Fusiform serves the fact on current reads only, so the last-known-good catalog on disk
/// was written before broca requested it and carries none, at the same version as the next
/// poll. Reusing that index would keep every row reading "no server-side fallback" until
/// fusiform happened to mint a new version.
#[test]
fn an_equal_version_poll_that_newly_carries_server_side_fallback_rebuilds_the_index() {
    let cache = CatalogCache::empty();
    let first = response(100);
    let version = first.catalog_version;
    cache.refresh(first).expect("install");
    let before = cache.snapshot().expect("installed");
    assert_eq!(
        before.server_side_fallback("anthropic", "claude-sonnet-4-5"),
        None
    );

    let mut upgraded = response(200);
    upgraded.catalog_version = version;
    upgraded
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .expect("fixture row")
        .insert(SERVER_SIDE_FALLBACK_FACT.into(), serde_json::json!(true));
    cache.refresh(upgraded).expect("same-version refresh");
    let after = cache.snapshot().expect("installed");
    assert!(
        !before.shares_index_with(&after),
        "a newly carried fact requires a new serving index"
    );
    assert_eq!(
        after.server_side_fallback("anthropic", "claude-sonnet-4-5"),
        Some(true)
    );
}

#[test]
fn turn_system_form_follows_the_model_family_on_a_mixed_provider() {
    let mut page = response(100);
    for (model, npm) in [
        ("a-chat", "@ai-sdk/openai-compatible"),
        ("b-responses", "@ai-sdk/openai"),
    ] {
        let mut row = chat_row(20_000);
        row.insert(
            "wire.npm".into(),
            serde_json::json!({"value":npm, "from":"curated"}),
        );
        row.insert(
            "wire.api".into(),
            serde_json::json!({"value":"https://mixed.example/v1", "from":"curated"}),
        );
        page.models.insert(format!("mixed-provider/{model}"), row);
    }
    let cache = CatalogCache::empty();
    cache.refresh(page).unwrap();
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(
        snapshot.wire_family("mixed-provider", "a-chat"),
        Some(WireFamilyId::OpenAiChat)
    );
    assert_eq!(
        snapshot.wire_family("mixed-provider", "b-responses"),
        Some(WireFamilyId::OpenAiResponses)
    );
    assert!(!snapshot.turn_system_form("mixed-provider", "a-chat"));
    assert!(
        snapshot.turn_system_form("mixed-provider", "b-responses"),
        "the Responses model supports system notes even when the provider defaults to Chat"
    );
}

/// The fact reaches the resolved row as a plain yes, no or nothing, and a malformed value
/// leaves the row serving with its route unchanged, reported by one WARN.
#[test]
fn mid_conversation_system_fact_is_optional_and_frozen_without_name_inference() {
    use broca_provider::{
        families::resolve_frozen, spec::WireFamilyId, turn_system::catalog_support,
    };
    assert!(FUSIFORM_FACT_PREFIXES.contains(&MID_CONVERSATION_SYSTEM_FACT));
    for (value, expected) in [
        (None, None),
        (Some(Value::Bool(false)), Some(false)),
        (Some(Value::Bool(true)), Some(true)),
        (Some(Value::Null), None),
        (Some(Value::String("true".into())), None),
    ] {
        let mut page = response(100);
        let facts = page.models.get_mut("anthropic/claude-sonnet-4-5").unwrap();
        facts.insert(
            "wire.npm".into(),
            serde_json::json!({"value":"@ai-sdk/anthropic", "from":"provider"}),
        );
        facts.insert(
            "wire.api".into(),
            serde_json::json!({"value":"https://api.anthropic.com/v1", "from":"curated"}),
        );
        if let Some(value) = value {
            facts.insert(MID_CONVERSATION_SYSTEM_FACT.into(), value);
        }
        let cache = CatalogCache::empty();
        cache
            .refresh(page)
            .expect("an optional fact must never exclude a route");
        let snapshot = cache.snapshot().unwrap();
        let (provider, model) = snapshot
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .unwrap();
        assert_eq!(model.mid_conversation_system, expected);
        let frozen = resolve_frozen(&provider, &model, 2048, 2).unwrap();
        assert_eq!(frozen.anthropic_turn_system, expected == Some(true));
        assert!(!catalog_support(
            WireFamilyId::AnthropicMessages,
            "compatible-host",
            &model
        ));
    }
    let mut page = response(100);
    let cache = CatalogCache::empty();
    cache.refresh(page.clone()).unwrap();
    page.models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap()
        .insert(MID_CONVERSATION_SYSTEM_FACT.into(), Value::Bool(true));
    cache.refresh(page).unwrap();
    assert_eq!(
        cache
            .snapshot()
            .unwrap()
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .unwrap()
            .1
            .mid_conversation_system,
        Some(true),
        "an equal-version poll must not reuse an index fetched without the fact"
    );
}

/// The snapshot reports a model's effort fact as true only when the catalog says so, and false
/// for an absent fact or a model it does not list.
#[test]
fn the_snapshot_answers_whether_an_effort_change_keeps_the_cache() {
    for (value, expected) in [
        (None, false),
        (Some(Value::Bool(false)), false),
        (Some(Value::Bool(true)), true),
    ] {
        let mut page = response(100);
        if let Some(value) = value {
            page.models
                .get_mut("anthropic/claude-sonnet-4-5")
                .unwrap()
                .insert(EFFORT_CHANGE_CACHE_FACT.into(), value);
        }
        let cache = CatalogCache::empty();
        cache.refresh(page).unwrap();
        let snapshot = cache.snapshot().unwrap();
        assert_eq!(
            snapshot.effort_change_keeps_cache("anthropic", "claude-sonnet-4-5"),
            expected
        );
        assert!(!snapshot.effort_change_keeps_cache("anthropic", "not-a-model"));
    }
}

#[test]
fn server_side_fallback_reaches_the_resolved_row() {
    let mut page = response(100);
    let facts = page
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .expect("fixture row");
    facts.insert(
        "wire.npm".into(),
        serde_json::json!({"value":"@ai-sdk/anthropic", "from":"provider"}),
    );
    facts.insert(
        "wire.api".into(),
        serde_json::json!({"value":"https://api.anthropic.com/v1", "from":"curated"}),
    );
    facts.insert(SERVER_SIDE_FALLBACK_FACT.into(), serde_json::json!(true));
    page.models.insert("anthropic/claude-opus-5-5".into(), {
        let mut row = chat_row(200_000);
        row.insert(SERVER_SIDE_FALLBACK_FACT.into(), serde_json::json!(false));
        row
    });
    let cache = CatalogCache::empty();
    cache.refresh(page).expect("install");
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(
        snapshot.server_side_fallback("anthropic", "claude-sonnet-4-5"),
        Some(true)
    );
    assert_eq!(
        snapshot.server_side_fallback("anthropic", "claude-opus-5-5"),
        Some(false)
    );
    assert_eq!(
        snapshot.server_side_fallback("anthropic", "not-a-row"),
        None
    );
    assert_eq!(
        snapshot
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .unwrap()
            .1
            .server_side_fallback,
        Some(true),
        "the fact is on the spec admission freezes from"
    );
}

/// A malformed value must not change what the row's route resolves to. The control install
/// carries the same row with no such fact, and the two resolutions must match.
#[test]
fn a_malformed_server_side_fallback_still_serves_the_same_route() {
    fn page(flag: Option<serde_json::Value>) -> FusiformCatalogResponse {
        let mut page = response(100);
        let facts = page
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .expect("fixture row");
        facts.insert(
            "wire.npm".into(),
            serde_json::json!({"value":"@ai-sdk/anthropic", "from":"provider"}),
        );
        facts.insert(
            "wire.api".into(),
            serde_json::json!({"value":"https://api.anthropic.com/v1", "from":"curated"}),
        );
        if let Some(flag) = flag {
            facts.insert(SERVER_SIDE_FALLBACK_FACT.into(), flag);
        }
        page
    }

    let control = CatalogCache::empty();
    control.refresh(page(None)).unwrap();
    let control_route = control
        .snapshot()
        .unwrap()
        .resolve_route("anthropic", "claude-sonnet-4-5")
        .expect("the row routes");

    for value in [
        serde_json::json!("true"),
        serde_json::json!(1),
        serde_json::json!([true]),
        serde_json::json!({"value": true}),
    ] {
        let cache = CatalogCache::empty();
        let capture = broca_test_support::LogCapture::new();
        capture
            .in_scope(|| cache.refresh(page(Some(value.clone()))))
            .expect("a malformed optional fact must not refuse the refresh");
        let snapshot = cache.snapshot().unwrap();
        assert_eq!(
            snapshot.resolve_route("anthropic", "claude-sonnet-4-5"),
            Ok(control_route.clone()),
            "{value}: the row must resolve the same route"
        );
        assert_eq!(
            snapshot.server_side_fallback("anthropic", "claude-sonnet-4-5"),
            None,
            "{value}: an unreadable value states nothing"
        );
        assert_eq!(
            cache.health(100).unwrap().rows_excluded,
            0,
            "{value}: the row is not excluded over an optional fact"
        );
        let warns: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|event| {
                event.level == tracing::Level::WARN
                    && event.target == "catalog"
                    && event.fields.get("fact").map(String::as_str)
                        == Some(SERVER_SIDE_FALLBACK_FACT)
            })
            .collect();
        assert_eq!(warns.len(), 1, "{value}: exactly one WARN: {warns:?}");
        assert_eq!(warns[0].fields["model"], "anthropic/claude-sonnet-4-5");
    }
}

/// An explicit `null` is the producer saying it does not state the fact, not a defect, so
/// it is silent.
#[test]
fn a_null_server_side_fallback_is_unstated_without_a_warning() {
    let mut page = response(100);
    page.models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap()
        .insert(SERVER_SIDE_FALLBACK_FACT.into(), serde_json::Value::Null);
    let cache = CatalogCache::empty();
    let capture = broca_test_support::LogCapture::new();
    capture.in_scope(|| cache.refresh(page)).unwrap();
    assert_eq!(
        cache
            .snapshot()
            .unwrap()
            .server_side_fallback("anthropic", "claude-sonnet-4-5"),
        None
    );
    assert!(
        capture
            .events()
            .iter()
            .all(|event| event.level != tracing::Level::WARN),
        "null is an answer, not a malformed value"
    );
}

#[test]
fn an_equal_version_poll_that_newly_carries_output_limits_rebuilds_the_index() {
    let cache = CatalogCache::empty();
    let first = response(100);
    let version = first.catalog_version;
    cache.refresh(first).expect("install");
    let before = cache.snapshot().expect("installed");
    assert_eq!(before.output_limit("anthropic", "claude-sonnet-4-5"), None);

    let mut upgraded = response(200);
    upgraded.catalog_version = version;
    upgraded
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .expect("fixture row")
        .insert("limit.output".into(), serde_json::json!(64000));
    cache.refresh(upgraded).expect("same-version refresh");
    let after = cache.snapshot().expect("installed");
    assert!(!before.shares_index_with(&after));
    assert_eq!(
        after.output_limit("anthropic", "claude-sonnet-4-5"),
        Some(64000)
    );
}

/// An equal-version poll that is the first to carry mode rates rebuilds the index, for the
/// same reason as `limit.output` above: otherwise every tier stays refused until fusiform
/// mints a new version.
#[test]
fn an_equal_version_poll_that_newly_carries_mode_rates_rebuilds_the_index() {
    let cache = CatalogCache::empty();
    let first = response(100);
    let version = first.catalog_version;
    cache.refresh(first).expect("install");
    let before = cache.snapshot().expect("installed");
    assert!(before
        .priced_modes("anthropic", "claude-sonnet-4-5")
        .is_empty());

    let mut upgraded = response(200);
    upgraded.catalog_version = version;
    let row = upgraded
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .expect("fixture row");
    row.insert("rate.input.mode.fast".into(), priced_rate(6_000_000_000));
    row.insert("rate.output.mode.fast".into(), priced_rate(30_000_000_000));
    cache.refresh(upgraded).expect("same-version refresh");
    let after = cache.snapshot().expect("installed");
    assert!(!before.shares_index_with(&after));
    assert_eq!(
        after.priced_modes("anthropic", "claude-sonnet-4-5"),
        vec!["fast".to_string()]
    );
}

#[test]
fn synthetic_overlay_model_retains_authored_warm_prices() {
    let mut response = response(10);
    let facts = response.models["anthropic/claude-sonnet-4-5"].clone();
    response.models.insert("xai/fixture-base".into(), facts);
    let overlay: RuntimeOverlay = serde_json::from_value(serde_json::json!({"patch":{"xai":{
        "wire_family":"openai_responses", "models":{"fixture-priced":{
            "name":"Synthetic priced model", "cost":{"input":0.5,"output":2.5,"cache_read":0.2}
        }}
    }}}))
    .unwrap();
    let (catalog, _) =
        ServingCatalog::from_fusiform_with_overlay(&response.models, overlay).unwrap();
    let (_, model) = catalog.model_for_serving("xai", "fixture-priced").unwrap();
    assert_eq!(model.cost.input, Some(0.5));
    assert_eq!(model.cost.output, Some(2.5));
    assert_eq!(model.cost.cache_read, Some(0.2));
    assert_eq!(model.cost.cache_write, None);
    assert_eq!(model.display_name, "Synthetic priced model");
}

#[test]
fn a_renderer_only_patch_cannot_invent_a_model_row() {
    let mut page = response(10);
    page.models.insert(
        "openai/fixture-present".into(),
        page.models["anthropic/claude-sonnet-4-5"].clone(),
    );
    let overlay: RuntimeOverlay = serde_json::from_value(serde_json::json!({"patch":{"openai":{
        "wire_family":"openai_responses", "models":{"gpt-4.1":{"wire_family":"openai_chat"}}
    }}}))
    .unwrap();
    let (catalog, _) = ServingCatalog::from_fusiform_with_overlay(&page.models, overlay).unwrap();
    assert!(
        !catalog.contains_model("openai", "gpt-4.1"),
        "a renderer pin is not evidence of model existence"
    );
}

#[test]
fn existing_model_overlay_replaces_prices_and_preserves_name_and_structured_output() {
    let mut response = response(10);
    let facts = response
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap();
    facts.insert("rate.input".into(), priced_rate(9_000_000));
    let overlay: RuntimeOverlay = serde_json::from_str(r#"{"patch":{"anthropic":{
        "base_url_source":"authored", "wire_family_source":"authored", "wire_family_reason":"first party",
        "models":{"claude-sonnet-4-5":{
            "id":"claude-sonnet-4-5", "name":"Authored name", "structured_output":true,
            "cost":{"input":1.5,"output":7.5,"cache_read":0.15,"cache_write":0,
                "reasoning":3,"input_audio":4,"output_audio":5,
                "tiers":[{"tier":{"type":"context","size":200000},"input":2,"output":10,"cache_read":0.2}]}
        }}}}}"#).unwrap();
    let (catalog, _) =
        ServingCatalog::from_fusiform_with_overlay(&response.models, overlay).unwrap();
    let cost = &catalog.providers["anthropic"].models["claude-sonnet-4-5"].cost;
    assert_eq!(cost.input, Some(1.5));
    assert_eq!(cost.output, Some(7.5));
    assert_eq!(cost.cache_read, Some(0.15));
    assert_eq!(cost.cache_write, Some(0.0));
    assert_eq!(cost.reasoning, Some(3.0));
    assert_eq!(cost.input_audio, Some(4.0));
    assert_eq!(cost.output_audio, Some(5.0));
    assert_eq!(cost.tiers.len(), 1);
    assert_eq!(cost.tiers[0].min_context, 200000);
    assert_eq!(cost.tiers[0].input, Some(2.0));
    assert_eq!(cost.tiers[0].output, Some(10.0));
    assert_eq!(cost.tiers[0].cache_read, Some(0.2));
    assert_eq!(cost.tiers[0].cache_write, None);
    let (_, model) = catalog
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .unwrap();
    assert_eq!(model.cost, *cost);
    assert_eq!(model.display_name, "Authored name");
    assert_eq!(
        model.capabilities.structured_output,
        CapabilityFlag::Known(true)
    );
}

#[test]
fn runtime_model_pin_selects_its_family_and_base_url() {
    let mut response = response(10);
    let facts = response.models["anthropic/claude-sonnet-4-5"].clone();
    response
        .models
        .insert("opencode-go/gpt-6-luna".into(), facts.clone());
    response
        .models
        .insert("opencode-go/minimax-m3".into(), facts);
    let overlay: RuntimeOverlay = serde_json::from_str(
        r#"{"patch":{"opencode-go":{
            "base_url":"https://opencode.ai/inference/go/v1",
            "wire_family":"openai_chat",
            "models":{
                "gpt-6-luna":{"wire_family":"openai_responses"},
                "minimax-m3":{
                    "wire_family":"anthropic_messages",
                    "base_url":"https://opencode.ai/inference/go"
                }
            }
        }}}"#,
    )
    .unwrap();
    let (catalog, _) =
        ServingCatalog::from_fusiform_with_overlay(&response.models, overlay).unwrap();
    let (provider, _) = catalog
        .model_for_serving("opencode-go", "gpt-6-luna")
        .unwrap();
    assert_eq!(provider.wire_family, WireFamilyId::OpenAiResponses);
    assert_eq!(provider.base_url, "https://opencode.ai/inference/go/v1");
    let (provider, _) = catalog
        .model_for_serving("opencode-go", "minimax-m3")
        .unwrap();
    assert_eq!(provider.wire_family, WireFamilyId::AnthropicMessages);
    assert_eq!(provider.base_url, "https://opencode.ai/inference/go");
}

#[test]
fn runtime_overlay_refuses_unknown_keys() {
    for overlay in [
        r#"{"patch":{"anthropic":{"models":{"m":{"coast":{"input":1}}}}}}"#,
        r#"{"patch":{"anthropic":{"base_urll":"https://example.test"}}}"#,
        r#"{"patchh":{}}"#,
        r#"{"patch":{"anthropic":{"models":{"m":{"limit":{"outpt":1}}}}}}"#,
        r#"{"patch":{"anthropic":{"models":{"m":{"cost":{"inpt":1}}}}}}"#,
        r#"{"patch":{"anthropic":{"models":{"m":{"cost":{"tiers":[{"inputt":1,"tier":{"size":1,"type":"context"}}]}}}}}}"#,
        r#"{"patch":{"anthropic":{"models":{"m":{"cost":{"tiers":[{"tier":{"size":1,"type":"context","typo":true}}]}}}}}}"#,
    ] {
        let error = serde_json::from_str::<RuntimeOverlay>(overlay)
            .err()
            .expect("unknown overlay field must be refused");
        assert!(error.to_string().contains("unknown field"), "{error}");
    }
}

#[test]
fn warm_prices_retain_vendored_base_context_facts_and_ignore_modes() {
    let response: FusiformCatalogResponse = serde_json::from_str(include_str!(
        "../../tests/fixtures/fusiform_service_tier_modes.json"
    ))
    .unwrap();
    let cache = CatalogCache::empty();
    cache.refresh(response).unwrap();
    let snapshot = cache.snapshot().unwrap();
    let prices = snapshot
        .warm_cost_schedule("openai", "gpt-5.4-mini")
        .unwrap();
    assert_eq!(prices.input, Some(0.2));
    assert_eq!(prices.cache_read, Some(0.02));
    assert_eq!(prices.cache_write, Some(0.25));
    assert_eq!(prices.output, Some(1.2));
    assert_eq!(prices.tiers.len(), 1);
    assert_eq!(prices.tiers[0].min_context, 272_001);
    assert_eq!(prices.tiers[0].input, Some(0.4));
    assert_eq!(prices.tiers[0].cache_read, Some(0.04));
    assert_eq!(prices.tiers[0].cache_write, Some(0.5));
    assert!(snapshot.warm_cost_schedule("missing", "m").is_none());
}

#[test]
fn an_equal_version_poll_that_newly_carries_warm_prices_rebuilds_the_index() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).unwrap();
    let before = cache.snapshot().unwrap();
    assert_eq!(
        before
            .warm_cost_schedule("anthropic", "claude-sonnet-4-5")
            .unwrap()
            .input,
        None
    );
    let mut upgraded = response(200);
    upgraded
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap()
        .insert("rate.input".into(), priced_rate(3_000_000_000));
    cache.refresh(upgraded).unwrap();
    let after = cache.snapshot().unwrap();
    assert!(!before.shares_index_with(&after));
    assert_eq!(
        after
            .warm_cost_schedule("anthropic", "claude-sonnet-4-5")
            .unwrap()
            .input,
        Some(3.0)
    );
    assert_eq!(
        before
            .warm_cost_schedule("anthropic", "claude-sonnet-4-5")
            .unwrap()
            .input,
        None
    );
}

#[test]
fn warm_rate_distinguishes_missing_unpriced_and_stated_zero() {
    assert_eq!(warm_rate(None), None);
    for value in [
        Value::Null,
        serde_json::json!(2),
        serde_json::json!({"state":"unpriced"}),
        serde_json::json!({"state":"unknown","currency":"USD","exponent":9,"units":0}),
        serde_json::json!({"state":"priced","currency":"EUR","exponent":9,"units":1}),
        serde_json::json!({"state":"stated_zero","currency":"USD","exponent":9,"units":1}),
    ] {
        assert_eq!(warm_rate(Some(&value)), None, "{value}");
    }
    assert_eq!(
        warm_rate(Some(
            &serde_json::json!({"state":"stated_zero","currency":"USD","exponent":9,"units":0})
        )),
        Some(0.0)
    );
    assert_eq!(
        warm_rate(Some(
            &serde_json::json!({"state":"priced","currency":"USD","exponent":6,"units":3000000})
        )),
        Some(3.0)
    );
    let facts = BTreeMap::from([
        ("rate.input.mode.fast".into(), priced_rate(9_000_000_000)),
        ("rate.cache_read.mode.fast".into(), priced_rate(450_000_000)),
    ]);
    assert_eq!(warm_cost_fact(&facts), CostSchedule::default());
}

/// fusiform serves a free output side as `{"state":"stated_zero"}` with no currency, units
/// or exponent. That is a known price of zero, and reading it as "no price" would make cache
/// maintenance refuse a model whose cost is fully published. A stated zero that does carry
/// an amount must still be a USD zero.
#[test]
fn an_amountless_stated_zero_rate_is_a_known_zero_price() {
    assert_eq!(
        warm_rate(Some(&serde_json::json!({"state": "stated_zero"}))),
        Some(0.0)
    );
    for value in [
        serde_json::json!({"state":"stated_zero","currency":"USD","exponent":9,"units":1}),
        serde_json::json!({"state":"stated_zero","units":1}),
        serde_json::json!({"state":"stated_zero","exponent":9}),
        serde_json::json!({"state":"unpriced"}),
    ] {
        assert_eq!(warm_rate(Some(&value)), None, "{value}");
    }

    // The same reading through the catalog, on fusiform's served Clef row.
    let cache = CatalogCache::empty();
    cache
        .refresh(curated_response(1))
        .expect("curated rows load");
    let schedule = cache
        .snapshot()
        .expect("installed")
        .warm_cost_schedule("cloudflare-workers-ai", "@cf/cloudflare/clef")
        .expect("clef is served");
    assert_eq!(
        schedule.output,
        Some(0.0),
        "a free output side is priced at zero"
    );
    assert_eq!(schedule.input, Some(0.24));
}

fn named_decision_response(version: i64) -> FusiformCatalogResponse {
    let mut response = curated_response(version);
    for (id, model, provider) in [
        ("typesafe/jev-1.13.0", "Jev 1.13", "TypeSafe"),
        (
            "cloudflare-workers-ai/@cf/cloudflare/clef",
            "Clef",
            "Cloudflare Workers AI",
        ),
    ] {
        let facts = response.models.get_mut(id).unwrap();
        facts.insert("model.name".into(), serde_json::json!(model));
        facts.insert("provider.name".into(), serde_json::json!(provider));
    }
    response
}

#[test]
fn requested_display_name_facts_reach_decision_rows_without_id_fallbacks() {
    let mut response = named_decision_response(1);
    for facts in response.models.values_mut() {
        facts.retain(|key, _| {
            FUSIFORM_FACT_PREFIXES
                .iter()
                .any(|prefix| key.starts_with(prefix))
        });
    }
    let cache = CatalogCache::empty();
    cache.refresh(response).unwrap();
    let snapshot = cache.snapshot().unwrap();
    let jev = snapshot.model_batch_row("typesafe/jev-1.13.0").unwrap();
    assert_eq!(jev.model_name.as_deref(), Some("Jev 1.13"));
    assert_eq!(jev.provider_name.as_deref(), Some("TypeSafe"));
    let clef = snapshot
        .model_batch_row("cloudflare-workers-ai/@cf/cloudflare/clef")
        .unwrap();
    assert_eq!(clef.model_name.as_deref(), Some("Clef"));
    assert_eq!(clef.provider_name.as_deref(), Some("Cloudflare Workers AI"));
    assert_eq!(
        snapshot
            .model_batch_row("flash-absent")
            .map(|row| row.provider_name),
        None
    );
    for value in [
        serde_json::json!(null),
        serde_json::json!(""),
        serde_json::json!(" "),
        serde_json::json!(123),
    ] {
        let mut response = named_decision_response(2);
        let facts = response.models.get_mut("typesafe/jev-1.13.0").unwrap();
        facts.insert("model.name".into(), value.clone());
        facts.insert("provider.name".into(), value);
        let cache = CatalogCache::empty();
        cache.refresh(response).unwrap();
        let row = cache
            .snapshot()
            .unwrap()
            .model_batch_row("typesafe/jev-1.13.0")
            .unwrap();
        assert_eq!(row.model_name, None);
        assert_eq!(row.provider_name, None);
    }
}

#[test]
fn same_version_names_rebuild_and_survive_nameless_polls_and_restart() {
    let path = temp_lkg("decision-display-names");
    let cache = CatalogCache::open(&path);
    cache.refresh(curated_response(7)).unwrap();
    let old = cache.snapshot().unwrap();
    assert_eq!(
        old.model_batch_row("typesafe/jev-1.13.0")
            .unwrap()
            .provider_name,
        None
    );
    cache.refresh(named_decision_response(7)).unwrap();
    let named = cache.snapshot().unwrap();
    assert!(!old.shares_index_with(&named));
    assert_eq!(
        named
            .model_batch_row("typesafe/jev-1.13.0")
            .unwrap()
            .provider_name
            .as_deref(),
        Some("TypeSafe")
    );
    let mut nameless = curated_response(7);
    nameless
        .models
        .get_mut("typesafe/jev-1.13.0")
        .unwrap()
        .insert("limit.output".into(), serde_json::json!(10));
    cache.refresh(nameless).unwrap();
    drop(cache);
    let reopened = CatalogCache::open(&path);
    let jev = reopened
        .snapshot()
        .unwrap()
        .model_batch_row("typesafe/jev-1.13.0")
        .unwrap();
    assert_eq!(jev.provider_name.as_deref(), Some("TypeSafe"));
    assert_eq!(jev.model_name.as_deref(), Some("Jev 1.13"));
    reopened.refresh(curated_response(8)).unwrap();
    assert_eq!(
        reopened
            .snapshot()
            .unwrap()
            .model_batch_row("typesafe/jev-1.13.0")
            .unwrap()
            .provider_name,
        None
    );
}

#[test]
fn unknown_provider_fact_is_ignored_without_excluding_a_served_row() {
    let cache = CatalogCache::empty();
    cache.refresh(named_decision_response(1)).unwrap();
    let before = cache
        .snapshot()
        .unwrap()
        .model_batch_row("typesafe/jev-1.13.0")
        .unwrap();
    let mut response = named_decision_response(2);
    response
        .models
        .get_mut("typesafe/jev-1.13.0")
        .unwrap()
        .insert(
            "provider.future_field".into(),
            serde_json::json!({"unknown":[1,2,3]}),
        );
    cache.refresh(response).unwrap();
    assert_eq!(cache.health(100).unwrap().rows_excluded, 0);
    assert_eq!(
        cache
            .snapshot()
            .unwrap()
            .model_batch_row("typesafe/jev-1.13.0"),
        Some(before)
    );
}

#[test]
fn pending_providers_neither_mask_nor_count_as_borrowable_catalog_routes() {
    use broca_module_serve::auth_routing::{catalog_provider, CatalogProvider, Unresolved};

    let mut page = response(100);
    page.models.extend(curated_rows());
    let cache = CatalogCache::empty();
    cache.refresh(page).unwrap();
    let snapshot = cache.snapshot().unwrap();
    assert!(snapshot.model_batch_row("typesafe/jev-1.13.0").is_some());
    assert!(snapshot.contains_provider("anthropic"));
    let listing = serde_json::from_value(serde_json::json!({
        "credentials": [
            { "id": "key:borrow", "type": "apikey", "categories": ["llm"], "state": "active", "record_version": 1, "operations": ["read"], "auth_method": "apikey", "provider_ids": ["typesafe"], "serves": ["anthropic"] },
            { "id": "key:pending", "type": "apikey", "categories": ["llm"], "state": "active", "record_version": 1, "operations": ["read"], "auth_method": "apikey", "provider_ids": ["gateway"], "serves": ["typesafe"] }
        ],
        "grants": 1, "grant_tuples": [], "view": "synthetic"
    })).unwrap();
    let resolve =
        |provider| catalog_provider(&listing, provider, |id| snapshot.serves_provider(id));
    assert_eq!(
        (resolve("typesafe"), resolve("gateway")),
        (
            CatalogProvider::Borrowed("anthropic".into()),
            CatalogProvider::Unresolved(Unresolved::NoServedProvider)
        )
    );
}

/// fusiform's served pages for its curated System One rows, vendored byte-for-byte from
/// fusiform 69a15bb `crates/fusiform-module/fixtures/served-payloads.json` (cases "a curated
/// decision model", "a curated extraction model" and "a provider served only through
/// curated rows"). Each case is one `catalog.get` page; this merges their model rows.
fn curated_rows() -> BTreeMap<String, BTreeMap<String, Value>> {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/fusiform-served-curated-system-one.json"
    ))
    .expect("vendored curated fixture parses");
    let mut rows = BTreeMap::new();
    for case in fixture.as_object().expect("named cases").values() {
        let page: FusiformCatalogResponse = serde_json::from_value(case["response"].clone())
            .expect("a served curated page decodes as a catalog response");
        rows.extend(page.models);
    }
    assert_eq!(rows.len(), 3, "clef, clairic and jev");
    rows
}

/// System One reads a row's kind, price and limits whether or not broca serves its provider
/// as chat: Jev's `typesafe` provider has no wire family, yet its row is readable, priced in
/// exact nanodollars, and states no context window; a stated-zero output is a zero, not
/// absent.
#[test]
fn model_batch_rows_are_read_without_a_chat_wire_family() {
    let cache = CatalogCache::empty();
    cache
        .refresh(curated_response(1))
        .expect("curated rows load");
    let snapshot = cache.snapshot().expect("installed");
    assert!(!snapshot.contains_model("typesafe", "jev-1.13.0"));
    let jev = snapshot
        .model_batch_row("typesafe/jev-1.13.0")
        .expect("jev's row is readable");
    assert_eq!(
        jev,
        ModelBatchRow {
            model_name: None,
            provider_name: None,
            kind: Some("decision".into()),
            context_tokens: None,
            attachment: false,
            input_rate_nanos: Some(42_000_000),
            output_rate_nanos: Some(0),
        }
    );
    let clef = snapshot
        .model_batch_row("cloudflare-workers-ai/@cf/cloudflare/clef")
        .expect("clef's row is readable");
    assert_eq!(clef.context_tokens, Some(65_536));
    assert!(clef.attachment);
    assert_eq!(clef.input_rate_nanos, Some(240_000_000));
    let decisions: Vec<String> = snapshot
        .model_batch_rows_of_kind("decision")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        decisions,
        [
            "cloudflare-workers-ai/@cf/cloudflare/clef",
            "typesafe/jev-1.13.0"
        ]
    );
    assert_eq!(snapshot.model_batch_row("typesafe/jev-9"), None);
    assert_eq!(
        batch_rate_nanos(Some(&serde_json::json!(
            {"state":"priced","currency":"USD","exponent":12,"units":1001}
        ))),
        Some(2),
        "a rate finer than a nanodollar rounds up"
    );
}

fn curated_response(catalog_version: i64) -> FusiformCatalogResponse {
    FusiformCatalogResponse {
        source: "models.dev".into(),
        resolved_at_ms: 1,
        catalog_version,
        models: curated_rows(),
        withheld: Vec::new(),
        uncertain: Vec::new(),
        wire_corrections: Vec::new(),
    }
}

/// Placing fusiform's curated decision and extraction rows in a page of ordinary chat rows
/// must not refuse the refresh, must not move any chat row, and must make the new ids
/// resolvable by their full catalog id.
///
/// The rows carry only what their source publishes: Clef and Clairic have no reasoning or
/// tool-call flag, and TypeSafe's Jev has no context window either. When an omitted fact
/// refused the whole catalog, this refresh was refused and the daemon kept its old catalog;
/// now those omissions decode as unstated and no curated row is excluded.
///
/// Identities split at the FIRST `/`: `cloudflare-workers-ai/@cf/cloudflare/clef` is
/// provider `cloudflare-workers-ai`, model `@cf/cloudflare/clef`.
#[test]
fn curated_system_one_rows_refresh_beside_chat_rows_without_moving_them() {
    let live: FusiformCatalogResponse = serde_json::from_str(include_str!(
        "../../tests/fixtures/fusiform_catalog_get_live.json"
    ))
    .expect("captured live fusiform response must deserialize");
    let cache = CatalogCache::empty();
    cache
        .refresh(live.clone())
        .expect("the live chat catalog loads");
    let before = cache.snapshot().expect("installed");

    let mut next = live.clone();
    next.catalog_version += 1;
    next.models.extend(curated_rows());
    assert_eq!(
        next.models.len(),
        live.models.len() + 3,
        "the curated ids are new"
    );
    cache
        .refresh(next)
        .expect("a page carrying curated System One rows must be accepted, not refused");
    let after = cache.snapshot().expect("installed");
    assert_eq!(after.catalog_version(), live.catalog_version + 1);
    assert!(
        after.0.excluded_rows.is_empty(),
        "{:?}",
        after.0.excluded_rows
    );
    assert!(
        !after.shares_index_with(&before),
        "the new page was materialized"
    );

    let mut compared = 0usize;
    for identity in live.models.keys() {
        let (provider_id, model_id) = identity.split_once('/').expect("catalog id");
        assert_eq!(
            after.model_for_serving(provider_id, model_id),
            before.model_for_serving(provider_id, model_id),
            "chat row {identity} moved"
        );
        assert_eq!(
            after.is_withdrawn(provider_id, model_id),
            before.is_withdrawn(provider_id, model_id)
        );
        assert_eq!(after.model_kind(provider_id, model_id), None);
        compared += 1;
    }
    assert_eq!(compared, 6_328, "every chat row was compared");

    for (model_id, kind) in [
        ("@cf/cloudflare/clef", "decision"),
        ("@cf/cloudflare/clairic", "extraction"),
    ] {
        let row = after
            .model_batch_row(&format!("cloudflare-workers-ai/{model_id}"))
            .unwrap_or_else(|| panic!("{model_id} resolves by its full catalog id"));
        assert!(
            after
                .model_for_serving("cloudflare-workers-ai", model_id)
                .is_none(),
            "routing facts are not stated for this non-chat row"
        );
        assert_eq!(row.context_tokens, Some(65_536));
        assert!(row.attachment);
        assert_eq!(
            after
                .model_kind("cloudflare-workers-ai", model_id)
                .as_deref(),
            Some(kind)
        );
    }
    // A split anywhere but the first slash names nothing.
    assert!(!after.contains_model("cloudflare-workers-ai/@cf", "cloudflare/clef"));
    assert!(!after.contains_model("cloudflare-workers-ai/@cf/cloudflare", "clef"));

    // TypeSafe has no wire-family pin, so like every unpinned provider it is pending: its
    // row is accepted and kept as an identity, but not served.
    assert!(!after.contains_provider("typesafe"));
    assert!(!after.contains_model("typesafe", "jev-1.13.0"));
    assert!(!after.is_withdrawn("typesafe", "jev-1.13.0"));
    assert!(after.0.model_identities.contains("typesafe/jev-1.13.0"));
}

/// `model.kind` decodes to `Some(kind)` when stated and `None` when absent or null; a value
/// of another type excludes only its row. The kind changes no other fact's reading: a row
/// stating `chat` that omits a chat fact decodes it as unstated exactly like any other row,
/// and a malformed fact on a non-chat row excludes that row like any other.
#[test]
fn model_kind_is_stated_or_absent_and_a_malformed_kind_excludes_only_its_row() {
    for (fact, expected) in [
        (None, None),
        (Some(Value::Null), None),
        (Some(serde_json::json!("chat")), Some("chat")),
        (Some(serde_json::json!("decision")), Some("decision")),
    ] {
        let mut page = response(10);
        if let Some(fact) = fact.clone() {
            page.models
                .get_mut("anthropic/claude-sonnet-4-5")
                .unwrap()
                .insert("model.kind".into(), fact);
        }
        let cache = CatalogCache::empty();
        cache.refresh(page).expect("loads");
        assert_eq!(
            cache
                .snapshot()
                .unwrap()
                .model_kind("anthropic", "claude-sonnet-4-5")
                .as_deref(),
            expected,
            "{fact:?}"
        );
    }
    let mut page = response(10);
    page.models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap()
        .insert("model.kind".into(), serde_json::json!(1));
    assert!(loaded_excludes_for_fact(&page, "model.kind"));
    let served = loaded(&page, CatalogSource::Fusiform, None).expect("one bad kind loads");
    assert!(!served
        .catalog
        .contains_model("anthropic", "claude-sonnet-4-5"));

    for (fact, read) in [
        (
            "capability.reasoning",
            (|model: &ModelSpec| model.capabilities.reasoning == CapabilityFlag::Unknown)
                as fn(&ModelSpec) -> bool,
        ),
        ("capability.tool_call", |model| {
            model.capabilities.tool_call == CapabilityFlag::Absent
        }),
        ("limit.context", |model| model.limits.context.is_none()),
    ] {
        let mut page = response(10);
        let row = page.models.get_mut("anthropic/claude-sonnet-4-5").unwrap();
        row.insert("model.kind".into(), serde_json::json!("chat"));
        row.remove(fact);
        let served = loaded(&page, CatalogSource::Fusiform, None)
            .unwrap_or_else(|error| panic!("a chat row missing {fact} loads: {error}"));
        assert!(served.excluded_rows.is_empty(), "{fact}");
        let (_, model) = served
            .catalog
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .unwrap_or_else(|| panic!("a chat row missing {fact} is served"));
        assert!(
            read(&model),
            "a chat row missing {fact} reads it as unstated"
        );
    }

    // A malformed fact a non-chat row does carry excludes that row.
    let mut rows = curated_rows();
    rows.get_mut("cloudflare-workers-ai/@cf/cloudflare/clef")
        .unwrap()
        .insert("capability.tool_call".into(), serde_json::json!("yes"));
    let mut page = curated_response(1);
    page.models = rows;
    assert!(loaded_excludes_for_fact(&page, "capability.tool_call"));

    // The kind survives the prefix filter the poll asks fusiform to apply.
    let mut filtered = curated_response(1);
    for facts in filtered.models.values_mut() {
        facts.retain(|fact, _| {
            FUSIFORM_FACT_PREFIXES
                .iter()
                .any(|prefix| fact.starts_with(prefix))
        });
    }
    let served = loaded(&filtered, CatalogSource::Fusiform, None).expect("filtered rows load");
    assert_eq!(
        CatalogSnapshot(Arc::new(served))
            .model_kind("cloudflare-workers-ai", "@cf/cloudflare/clef")
            .as_deref(),
        Some("decision")
    );
}

#[test]
fn an_equal_version_poll_that_newly_carries_model_kinds_rebuilds_the_index() {
    let cache = CatalogCache::empty();
    let first = response(100);
    let version = first.catalog_version;
    cache.refresh(first).expect("install");
    let before = cache.snapshot().expect("installed");
    assert_eq!(before.model_kind("anthropic", "claude-sonnet-4-5"), None);

    let mut upgraded = response(200);
    upgraded.catalog_version = version;
    upgraded
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .expect("fixture row")
        .insert("model.kind".into(), serde_json::json!("decision"));
    cache.refresh(upgraded).expect("same-version refresh");
    let after = cache.snapshot().expect("installed");
    assert!(!before.shares_index_with(&after));
    assert_eq!(
        after
            .model_kind("anthropic", "claude-sonnet-4-5")
            .as_deref(),
        Some("decision")
    );
}

fn priced_rate(units: u64) -> Value {
    serde_json::json!({"currency": "USD", "exponent": 9, "state": "priced", "units": units})
}

/// A mode is priced only when BOTH of its own rates are. The base rate never fills a missing
/// side, an unpriced or unknown state is unpriced, a zero the source states is a price, and a
/// row with mode rates but no base rate still prices its modes.
#[test]
fn a_mode_is_priced_only_by_its_own_input_and_output_rates() {
    let facts = |pairs: &[(&str, Value)]| -> BTreeMap<String, Value> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone()))
            .collect()
    };
    let base = [
        ("rate.input", priced_rate(1_000_000_000)),
        ("rate.output", priced_rate(8_000_000_000)),
    ];
    let cases: Vec<(&str, BTreeMap<String, Value>, Vec<&str>)> = vec![
        (
            "both sides priced",
            facts(&[
                base[0].clone(),
                base[1].clone(),
                ("rate.input.mode.fast", priced_rate(2)),
                ("rate.output.mode.fast", priced_rate(16)),
            ]),
            vec!["fast"],
        ),
        (
            "input side absent, base rates present",
            facts(&[
                base[0].clone(),
                base[1].clone(),
                ("rate.output.mode.fast", priced_rate(16)),
            ]),
            vec![],
        ),
        (
            "output side absent, base rates present",
            facts(&[
                base[0].clone(),
                base[1].clone(),
                ("rate.input.mode.priority", priced_rate(2)),
            ]),
            vec![],
        ),
        (
            "mode rates with no base rate",
            facts(&[
                ("rate.input.mode.fast", priced_rate(2)),
                ("rate.output.mode.fast", priced_rate(16)),
            ]),
            vec!["fast"],
        ),
        (
            "unpriced, null and unknown states",
            facts(&[
                (
                    "rate.input.mode.pro",
                    serde_json::json!({"state": "unpriced"}),
                ),
                (
                    "rate.output.mode.pro",
                    serde_json::json!({"state": "unpriced"}),
                ),
                ("rate.input.mode.flex", Value::Null),
                ("rate.output.mode.flex", priced_rate(1)),
                (
                    "rate.input.mode.odd",
                    serde_json::json!({"state": "estimated"}),
                ),
                ("rate.output.mode.odd", priced_rate(1)),
            ]),
            vec![],
        ),
        (
            "a stated zero is a price",
            facts(&[
                (
                    "rate.input.mode.priority",
                    serde_json::json!({"state": "stated_zero"}),
                ),
                ("rate.output.mode.priority", priced_rate(1)),
            ]),
            vec!["priority"],
        ),
    ];
    for (name, facts, expected) in cases {
        assert_eq!(
            priced_modes_fact("p/m", &facts).expect("well-formed"),
            expected,
            "{name}"
        );
    }
    let malformed = facts(&[("rate.input.mode.fast", serde_json::json!(2.0))]);
    assert!(priced_modes_fact("p/m", &malformed).is_err());
}

#[test]
fn catalog_image_dimensions_are_requested_and_survive_equal_version_refresh() {
    let cache = CatalogCache::empty();
    let first = response(100);
    let version = first.catalog_version;
    cache.refresh(first).unwrap();
    let before = cache.snapshot().unwrap();
    let model = |snapshot: &CatalogSnapshot| {
        snapshot
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .unwrap()
            .1
    };
    assert_eq!(model(&before).limits.image_long_edge_px, None);

    let mut upgraded = response(200);
    upgraded.catalog_version = version;
    let facts = upgraded
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap();
    for (key, value) in [
        ("limit.image_long_edge_px", 8_000),
        ("limit.image_long_edge_px.above_images.20", 3_000),
        ("limit.image_long_edge_px.above_images.2", 5_000),
    ] {
        facts.insert(key.into(), serde_json::json!(value));
    }
    for facts in upgraded.models.values_mut() {
        facts.retain(|fact, _| {
            FUSIFORM_FACT_PREFIXES
                .iter()
                .any(|prefix| fact.starts_with(prefix))
        });
    }
    cache.refresh(upgraded).unwrap();
    let after = cache.snapshot().unwrap();
    assert!(!before.shares_index_with(&after));
    let model = model(&after);
    assert_eq!(model.limits.image_long_edge_px, Some(8_000));
    assert_eq!(
        model.limits.image_long_edge_px_above_images,
        BTreeMap::from([(2, 5_000), (20, 3_000)])
    );
    let profile = broca_provider::media_profile::select_model_media_profile(
        WireFamilyId::AnthropicMessages,
        Some(broca_provider::media_profile::MediaQuirk::Anthropic),
        &model.id,
        &model,
    );
    assert_eq!(profile.max_image_side(2, 0), Some(8_000));
    assert_eq!(profile.max_image_side(3, 0), Some(5_000));
    assert_eq!(profile.max_image_side(20, 0), Some(5_000));
    assert_eq!(profile.max_image_side(21, 0), Some(3_000));
    assert_eq!(profile.max_image_side(1, 20), Some(3_000));
    assert_eq!(
        before
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .unwrap()
            .1
            .limits
            .image_long_edge_px,
        None
    );
}

#[test]
fn catalog_image_dimensions_null_is_unknown_and_malformed_facts_exclude_the_row() {
    let mut response = response(100);
    for key in [
        "limit.image_long_edge_px",
        "limit.image_long_edge_px.above_images.20",
    ] {
        response
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .unwrap()
            .insert(key.into(), Value::Null);
    }
    let installed = loaded(&response, CatalogSource::Fusiform, None).unwrap();
    let model = installed
        .catalog
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .unwrap()
        .1;
    assert_eq!(model.limits.image_long_edge_px, None);
    assert!(model.limits.image_long_edge_px_above_images.is_empty());

    for key in [
        "limit.image_long_edge_px",
        "limit.image_long_edge_px.above_images.20",
    ] {
        for malformed in [
            serde_json::json!(0),
            serde_json::json!(-1),
            serde_json::json!(1.5),
            serde_json::json!("3000"),
            serde_json::json!(true),
            serde_json::json!([]),
            serde_json::json!({}),
            serde_json::json!(u64::from(u32::MAX) + 1),
        ] {
            let mut malformed_response = response.clone();
            malformed_response
                .models
                .get_mut("anthropic/claude-sonnet-4-5")
                .unwrap()
                .insert(key.into(), malformed.clone());
            assert!(
                loaded_excludes_for_fact(&malformed_response, key),
                "{key}: {malformed}"
            );
            assert!(loaded(&malformed_response, CatalogSource::Fusiform, None)
                .unwrap()
                .catalog
                .model_for_serving("anthropic", "claude-sonnet-4-5")
                .is_none());
        }
    }
    for suffix in ["", "twenty", "-1", "1.5", "4294967296", "20.extra"] {
        let key = format!("limit.image_long_edge_px.above_images.{suffix}");
        let mut malformed_response = response.clone();
        malformed_response
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .unwrap()
            .insert(key.clone(), serde_json::json!(3_000));
        assert!(loaded_excludes_for_fact(&malformed_response, &key), "{key}");
    }
    let facts = response
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap();
    facts.insert(
        "limit.image_long_edge_px".into(),
        serde_json::json!(u32::MAX),
    );
    facts.insert(
        "limit.image_long_edge_px.above_images.0".into(),
        serde_json::json!(1),
    );
    facts.insert(
        format!("limit.image_long_edge_px.above_images.{}", u32::MAX),
        serde_json::json!(2),
    );
    let installed = loaded(&response, CatalogSource::Fusiform, None).unwrap();
    let model = installed
        .catalog
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .unwrap()
        .1;
    assert_eq!(model.limits.image_long_edge_px, Some(u32::MAX));
    assert_eq!(
        model.limits.image_long_edge_px_above_images,
        BTreeMap::from([(0, 1), (u32::MAX, 2)])
    );
}

#[test]
fn catalog_image_count_is_requested_and_survives_equal_version_refresh() {
    let cache = CatalogCache::empty();
    let first = response(100);
    let version = first.catalog_version;
    cache.refresh(first).unwrap();
    let before = cache.snapshot().unwrap();
    let model = |snapshot: &CatalogSnapshot| {
        snapshot
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .unwrap()
            .1
    };
    assert_eq!(model(&before).limits.images_per_request, None);

    let mut upgraded = response(200);
    upgraded.catalog_version = version;
    upgraded
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap()
        .insert("limit.images_per_request".into(), serde_json::json!(3600));
    for facts in upgraded.models.values_mut() {
        facts.retain(|fact, _| {
            FUSIFORM_FACT_PREFIXES
                .iter()
                .any(|prefix| fact.starts_with(prefix))
        });
    }
    cache.refresh(upgraded).unwrap();
    let after = cache.snapshot().unwrap();
    assert!(!before.shares_index_with(&after));
    assert_eq!(model(&after).limits.images_per_request, Some(3600));
    assert_eq!(model(&before).limits.images_per_request, None);
}

#[test]
fn catalog_image_count_null_is_unknown_and_malformed_counts_exclude_the_row() {
    let mut response = response(100);
    response
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .unwrap()
        .insert("limit.images_per_request".into(), Value::Null);
    let installed = loaded(&response, CatalogSource::Fusiform, None).unwrap();
    let model = installed
        .catalog
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .unwrap()
        .1;
    assert_eq!(model.limits.images_per_request, None);
    for malformed in [
        serde_json::json!(0),
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!("3600"),
        serde_json::json!(true),
        serde_json::json!([]),
        serde_json::json!({}),
        serde_json::json!(u64::from(u32::MAX) + 1),
    ] {
        response
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .unwrap()
            .insert("limit.images_per_request".into(), malformed.clone());
        assert!(
            loaded_excludes_for_fact(&response, "limit.images_per_request"),
            "{malformed}"
        );
        assert!(
            loaded(&response, CatalogSource::Fusiform, None)
                .unwrap()
                .catalog
                .model_for_serving("anthropic", "claude-sonnet-4-5")
                .is_none(),
            "{malformed}"
        );
    }
}

/// `limit.output: null` is fusiform's "unknown" and must decode to `None`, never a zero cap;
/// a number decodes as served; a pre-change row without the fact still loads, as `None`.
#[test]
fn a_served_null_output_limit_decodes_to_none_not_zero() {
    let mut response = response(100);
    let row = response
        .models
        .get("anthropic/claude-sonnet-4-5")
        .expect("fixture row")
        .clone();
    let mut null_row = row.clone();
    null_row.insert("limit.output".into(), Value::Null);
    let mut known_row = row.clone();
    known_row.insert("limit.output".into(), serde_json::json!(64000));
    response
        .models
        .insert("anthropic/null-output".into(), null_row);
    response
        .models
        .insert("anthropic/known-output".into(), known_row);
    // Through the wire decode, not just the typed map, so the JSON null is the input.
    let response: FusiformCatalogResponse =
        serde_json::from_value(serde_json::to_value(&response).expect("serialize"))
            .expect("decode");
    let loaded = loaded(&response, CatalogSource::Fusiform, None).expect("loads");
    let model = |id: &str| {
        loaded
            .catalog
            .model_for_serving("anthropic", id)
            .expect("served")
            .1
    };
    assert_eq!(model("null-output").limits.max_output, None);
    assert_eq!(model("known-output").limits.max_output, Some(64000));
    assert_eq!(model("claude-sonnet-4-5").limits.max_output, None);

    let mut malformed = response.clone();
    malformed
        .models
        .get_mut("anthropic/known-output")
        .expect("row")
        .insert("limit.output".into(), serde_json::json!("64000"));
    assert!(loaded_excludes_for_fact(&malformed, "limit.output"));
}

/// Whether loading `response` succeeds and excludes some row because of `fact`. A malformed
/// fact excludes its own row rather than refusing the catalog.
fn loaded_excludes_for_fact(response: &FusiformCatalogResponse, fact: &str) -> bool {
    loaded(response, CatalogSource::Fusiform, None)
        .is_ok_and(|loaded| loaded.excluded_rows.iter().any(|row| row.fact == fact))
}

/// The poll's prefix list must request `limit.output`, or no output limit ever arrives and
/// every omitted cap silently takes the fallback. This applies fusiform's prefix filter to a
/// row carrying more facts than broca needs, then loads what would have been served.
#[test]
fn the_requested_fact_prefixes_deliver_the_output_limit() {
    let mut full = response(100);
    let row = full
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .expect("fixture row");
    row.insert("limit.output".into(), serde_json::json!(64000));
    row.insert("rate.input".into(), serde_json::json!(3.0));
    for facts in full.models.values_mut() {
        facts.retain(|fact, _| {
            FUSIFORM_FACT_PREFIXES
                .iter()
                .any(|prefix| fact.starts_with(prefix))
        });
    }
    let served = loaded(&full, CatalogSource::Fusiform, None).expect("filtered response loads");
    assert_eq!(
        served
            .catalog
            .output_limit("anthropic", "claude-sonnet-4-5"),
        Some(64000),
        "limit.output must survive the prefix filter the poll asks fusiform to apply"
    );
}

/// An equal-version qualified response cannot replace either the serving state or its LKG.
///
/// The in-memory reuse path used to persist qualified responses without materializing them, so
/// the running process looked fresh while its next restart rejected the newly written LKG. A
/// run of these refusals must also remain visible through both health counters.
#[test]
fn an_equal_version_qualified_response_leaves_memory_and_lkg_unchanged() {
    let path = temp_lkg("equal-version-qualified");
    let cache = CatalogCache::open(&path);
    cache.refresh(response(100)).expect("first install");
    let installed = cache.snapshot().expect("installed");
    let lkg_before = fs::read(&path).expect("read clean LKG");

    let mut qualified = response(500);
    qualified.catalog_version = installed.catalog_version();
    qualified
        .withheld
        .push(serde_json::json!({"fact": "limit.context"}));

    for expected in 1..=3u32 {
        let error = cache
            .refresh(qualified.clone())
            .expect_err("qualified response must be refused at an equal version");
        assert!(
            matches!(
                error,
                CatalogRefreshError::Catalog(CatalogError::FusiformQualifiedFacts {
                    withheld: 1,
                    uncertain: 0,
                })
            ),
            "expected FusiformQualifiedFacts, got {error}"
        );

        let health = cache.health(500).expect("health remains available");
        assert_eq!(
            health.consecutive_refusals, expected,
            "qualified refusals must degrade health as a streak"
        );
        assert_eq!(
            health.total_refusals,
            u64::from(expected),
            "each qualified refusal must increment the lifetime counter exactly once"
        );
        assert!(
            health
                .refusal_reason
                .is_some_and(|reason| reason.contains("1 withheld and 0 uncertain")),
            "health must retain the qualification reason"
        );
    }

    let after = cache.snapshot().expect("prior catalog remains installed");
    assert!(
        installed.shares_index_with(&after),
        "refusal must keep serving the prior in-memory index"
    );
    assert_eq!(after.resolved_at_ms(), 100);
    assert!(after
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .is_some());
    assert_eq!(
        fs::read(&path).expect("read LKG after refusal"),
        lkg_before,
        "a refused response must not change the on-disk LKG"
    );

    let restarted = CatalogCache::open(&path);
    let recovered = restarted
        .snapshot()
        .expect("the unchanged LKG remains loadable after restart");
    assert_eq!(recovered.source(), CatalogSource::LastKnownGood);
    assert_eq!(recovered.resolved_at_ms(), 100);
    assert!(recovered
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .is_some());

    let _ = fs::remove_dir_all(path.parent().unwrap());
}

/// A cleared refusal stays visible in the total, because the streak forgets it.
///
/// FUSI's point that this gauge is the only instrument either of us has pointed at a real
/// producer restore is what forced this: an observer of a rare event is useless if reading
/// it requires being present while the event holds. Both conditions that trip these guards
/// are rare AND self-clearing, so the streak's most likely value during an investigation is
/// zero even when something happened hours earlier.
#[test]
fn a_recovered_refusal_is_still_visible_in_the_total() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).expect("first install");
    assert_eq!(cache.total_refusals(), 0, "a clean cache has seen nothing");

    let mut short = response(200);
    short.catalog_version += 1;
    short.models.remove("anthropic/claude-retired");
    for _ in 0..3 {
        let _ = cache.refresh(short.clone()).expect_err("refused");
    }
    assert_eq!(cache.consecutive_refusals().0, 3);
    assert_eq!(cache.total_refusals(), 3);

    // The producer recovers. The streak forgets, which is correct for a current-state gauge
    // -- and is exactly why the total must not.
    cache
        .refresh(response(300))
        .expect("a complete response is accepted");
    assert_eq!(
        cache.consecutive_refusals(),
        (0, None),
        "the streak reports the CURRENT state"
    );
    assert_eq!(
        cache.total_refusals(),
        3,
        "the total must survive recovery, or the event leaves no trace at all"
    );
}

/// The stale refusal SELF-HEALS and the completeness refusal does not, and a content-derived
/// producer version heals into the second rather than escaping it.
///
/// FUSI asked whether the stale guard is permanent the way the completeness guard is. It is
/// not, and the asymmetry is worth stating because it decides a design on their side: both
/// guards compare against a basis this cache freezes on refusal, but the PRODUCER's side of
/// the comparison moves in one case and not the other. Their version climbs with wall clock,
/// so a held high-water is eventually passed. Their identity set stays shrunk until the
/// missing models are re-observed, which for a model retired before the restore point may be
/// never.
///
/// So a version derived from content does not convert a permanent refusal into a bounded
/// one. It inserts a bounded stale phase in FRONT of the same permanent completeness
/// refusal. This test walks that exact sequence.
#[test]
fn a_rewound_version_heals_into_the_completeness_guard_rather_than_past_it() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).expect("first install");
    let held = cache.snapshot().expect("installed").catalog_version();

    // A restore under a content-derived version: the value rewinds to the restored
    // generation's newest era, and the store carries fewer identities.
    let mut restored = response(200);
    restored.catalog_version = held - 1_000;
    restored.models.remove("anthropic/claude-retired");
    let error = cache
        .refresh(restored.clone())
        .expect_err("rewound version is refused");
    assert!(
        matches!(error, CatalogRefreshError::Stale { .. }),
        "a rewound version must refuse on STALENESS, got {error}"
    );

    // The producer then writes an era at wall-clock, so the version climbs past the held
    // high-water. This is the self-heal: the stale gate stops refusing.
    let mut climbed = restored.clone();
    climbed.catalog_version = held + 1_000;
    let error = cache
        .refresh(climbed)
        .expect_err("the identity set is still short");
    assert!(
        matches!(error, CatalogRefreshError::Incomplete { .. }),
        "past the stale gate it must land on COMPLETENESS, got {error}"
    );

    // And that one does not heal: the basis never advances, so every later poll carrying the
    // restored store's smaller set refuses identically however high the version climbs.
    for step in 1..=3 {
        let mut later = restored.clone();
        later.catalog_version = held + 1_000 + step * 1_000;
        assert!(
            matches!(
                cache.refresh(later).expect_err("still short"),
                CatalogRefreshError::Incomplete { .. }
            ),
            "completeness refusal must persist at step {step}"
        );
    }
    assert_eq!(
        cache.snapshot().expect("still installed").catalog_version(),
        held,
        "no refused refresh may advance the installed version"
    );
}

/// A permanent refusal is visible as a rising count, and one success clears it.
///
/// Freshness cannot carry this: `catalog_age_ms` rises identically whether a poll missed
/// once or will never succeed again. The count is what separates them, so it has to survive
/// across polls and reset only on an accepted refresh.
#[test]
fn consecutive_refusals_accumulate_and_reset_on_the_first_success() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).expect("first install");
    assert_eq!(
        cache.consecutive_refusals().0,
        0,
        "a healthy cache reports zero rather than nothing"
    );

    let mut restored = response(200);
    restored.catalog_version = cache.snapshot().unwrap().catalog_version() + 5_000;
    restored.models.remove("anthropic/claude-retired");
    for expected in 1..=3u32 {
        let _ = cache.refresh(restored.clone()).expect_err("refused");
        let (count, reason) = cache.consecutive_refusals();
        assert_eq!(count, expected, "refusals must accumulate across polls");
        assert!(
            reason.is_some_and(|text| text.contains("claude-retired")),
            "the reason must name what is missing, so a reader can act on it"
        );
    }

    // A producer that recovers clears the state: the count reports the CURRENT streak, not a
    // lifetime total, because the question it answers is "is the catalog frozen right now".
    cache
        .refresh(response(300))
        .expect("a complete response is accepted again");
    assert_eq!(
        cache.consecutive_refusals(),
        (0, None),
        "one success must clear both the count and the stale reason"
    );
}

/// A newer-version response carrying FEWER identities is refused, and the refusal is
/// PERMANENT rather than one poll.
///
/// This is the interaction FUSI walked from their side and I could not have seen from mine.
/// Their catalog version is derived as max(now_ms, current + 1), so it is monotonic even
/// across a restore of their database. A restore therefore serves a version that is HIGHER
/// than anything held here while the CONTENT is older and the identity set is smaller --
/// the one combination that passes a version check and lands in a content check.
///
/// The completeness guard then compares against the identities THIS CACHE HOLDS, not against
/// the previous response, and a failed refresh does not install. So the held set never
/// shrinks, every subsequent poll compares against the same larger set, and the refusal
/// repeats for as long as fusiform serves the restored store.
///
/// Refusing is still correct: a legitimately smaller set and a truncated response are
/// indistinguishable by identity count, and serving a catalog that lost models is worse.
/// What this test pins is the PERMANENCE, because that is what decides whether the state
/// needs to be visible rather than merely logged.
#[test]
fn a_newer_response_with_fewer_identities_is_refused_permanently() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).expect("first install");
    let held = cache.snapshot().expect("installed").catalog_version();

    // A restore: higher version, smaller identity set.
    let mut restored = response(200);
    restored.catalog_version = held + 5_000;
    restored.models.remove("anthropic/claude-retired");

    for attempt in 1..=3 {
        let error = cache
            .refresh(restored.clone())
            .expect_err("a response missing held identities must be refused");
        assert!(
            matches!(
                error,
                CatalogRefreshError::Incomplete {
                    missing_count: 1,
                    ..
                }
            ),
            "attempt {attempt} expected Incomplete, got {error}"
        );
    }

    // The held document is untouched, which is why the refusal repeats: the comparison
    // basis never advances, so nothing about a later poll can differ.
    let snapshot = cache.snapshot().expect("still installed");
    assert_eq!(
        snapshot.catalog_version(),
        held,
        "a refused refresh must not advance the installed version"
    );
    assert_eq!(
        snapshot.resolved_at_ms(),
        100,
        "a refused refresh must not advance freshness either"
    );
}

/// An equal-version response reuses the index instead of rebuilding it, and must still
/// refuse an incomplete one.
///
/// Reuse is the whole point of the fast path, so the risk it introduces is that a truncated
/// response carrying the version of the document it was truncated from gets written to the
/// LKG while the in-memory index survives untouched. That damage is invisible until a
/// restart loads the short catalog.
#[test]
fn an_equal_version_response_is_reused_but_an_incomplete_one_is_still_refused() {
    let dir = temp_lkg("equal-version-reuse");
    let cache = CatalogCache::open(dir.join("lkg.json"));
    cache.refresh(response(100)).expect("first install");
    let installed = cache.snapshot().expect("installed");

    // Same version, complete: accepted, and resolved_at_ms moves so catalog_age_ms is fresh.
    let mut same = response(500);
    same.catalog_version = installed.catalog_version();
    cache.refresh(same).expect("an equal version is not stale");
    let refreshed = cache.snapshot().expect("still installed");
    assert_eq!(refreshed.catalog_version(), installed.catalog_version());
    assert_eq!(
        refreshed.resolved_at_ms(),
        500,
        "the reuse path must still publish the new resolved_at_ms"
    );

    // Same version, truncated: refused, and the complete catalog stays installed.
    let mut truncated = response(900);
    truncated.catalog_version = installed.catalog_version();
    truncated.models.remove("anthropic/claude-retired");
    let error = cache
        .refresh(truncated)
        .expect_err("an incomplete response must be refused even at the same version");
    assert!(
        matches!(
            error,
            CatalogRefreshError::Incomplete {
                missing_count: 1,
                ..
            }
        ),
        "expected Incomplete, got {error}"
    );
    assert_eq!(
        cache.snapshot().expect("unchanged").resolved_at_ms(),
        500,
        "the refusal must leave the previously installed document in place"
    );
}

/// A model row added without a catalog_version move, the way fusiform ships data compiled
/// into its own build.
fn new_row() -> (String, BTreeMap<String, Value>) {
    (
        "anthropic/claude-added-in-place".into(),
        BTreeMap::from([
            ("existence".into(), serde_json::json!("present")),
            ("limit.context".into(), serde_json::json!(100000)),
        ]),
    )
}

/// An equal-version response that gains a row installs it, in memory and on disk.
///
/// Reusing the index there leaves the new model without a row, so every send naming it is
/// refused until fusiform happens to mint a new version for some unrelated change.
#[test]
fn an_equal_version_response_that_gains_a_row_installs_it() {
    let path = temp_lkg("equal-version-gains-row");
    let cache = CatalogCache::open(&path);
    cache.refresh(response(100)).expect("first install");
    let before = cache.snapshot().expect("installed");
    assert!(
        before
            .model_for_serving("anthropic", "claude-added-in-place")
            .is_none(),
        "the fixture's new row must not be present before the poll that adds it"
    );

    let mut grown = response(200);
    grown.catalog_version = before.catalog_version();
    let (identity, facts) = new_row();
    grown.models.insert(identity.clone(), facts);
    cache
        .refresh(grown)
        .expect("a grown equal-version response");

    let after = cache.snapshot().expect("installed");
    assert!(
        !after.shares_index_with(&before),
        "a gained row requires a new serving index"
    );
    assert_eq!(after.catalog_version(), before.catalog_version());
    assert_eq!(after.resolved_at_ms(), 200);
    let (_, model) = after
        .model_for_serving("anthropic", "claude-added-in-place")
        .expect("the gained row must resolve");
    assert_eq!(model.limits.context, Some(100000));
    assert!(
        after
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .is_some(),
        "the rows already installed keep resolving"
    );

    let saved = load_lkg(&path)
        .expect("the saved copy reads")
        .expect("the saved copy exists");
    assert!(
        saved.models.contains_key(&identity),
        "the saved copy must hold the gained row"
    );
    let reopened = CatalogCache::open(&path);
    assert!(
        reopened
            .snapshot()
            .expect("the saved copy loads")
            .model_for_serving("anthropic", "claude-added-in-place")
            .is_some(),
        "the gained row must survive a restart"
    );
}

/// An equal-version response with the same identities keeps reusing the index, and its
/// saved copy holds the same rows.
#[test]
fn an_equal_version_response_with_the_same_rows_reuses_the_index_and_saves_them() {
    let path = temp_lkg("equal-version-same-rows");
    let cache = CatalogCache::open(&path);
    let mut first = response(100);
    let (identity, facts) = new_row();
    first.models.insert(identity.clone(), facts);
    cache.refresh(first.clone()).expect("first install");
    let before = cache.snapshot().expect("installed");

    let mut again = first;
    again.resolved_at_ms = 200;
    cache.refresh(again).expect("same-version refresh");
    let after = cache.snapshot().expect("installed");
    assert!(
        before.shares_index_with(&after),
        "an equal-version poll with no new identity must republish the SAME index"
    );
    assert_eq!(after.resolved_at_ms(), 200);

    let saved = load_lkg(&path)
        .expect("the saved copy reads")
        .expect("the saved copy exists");
    assert_eq!(saved.resolved_at_ms, 200, "the reuse path still saves");
    assert!(saved.models.contains_key(&identity));
}

/// An equal-version response that gains a row but lacks an installed one is still refused
/// as incomplete: the gained row does not buy a truncated response a way past the check.
#[test]
fn an_equal_version_response_that_gains_and_loses_rows_is_refused_as_incomplete() {
    let path = temp_lkg("equal-version-gains-and-loses");
    let cache = CatalogCache::open(&path);
    cache.refresh(response(100)).expect("first install");
    let before = cache.snapshot().expect("installed");

    let mut swapped = response(200);
    swapped.catalog_version = before.catalog_version();
    swapped.models.remove("anthropic/claude-retired");
    let (identity, facts) = new_row();
    swapped.models.insert(identity.clone(), facts);
    let error = cache
        .refresh(swapped)
        .expect_err("a response lacking an installed row must be refused");
    assert!(
        matches!(
            error,
            CatalogRefreshError::Incomplete {
                missing_count: 1,
                ..
            }
        ),
        "expected Incomplete, got {error}"
    );

    let after = cache.snapshot().expect("still installed");
    assert!(after.shares_index_with(&before));
    assert_eq!(after.resolved_at_ms(), 100);
    assert!(
        after
            .model_for_serving("anthropic", "claude-added-in-place")
            .is_none(),
        "a refused response must not install its gained row"
    );
    let saved = load_lkg(&path)
        .expect("the saved copy reads")
        .expect("the saved copy exists");
    assert_eq!(saved.resolved_at_ms, 100, "a refusal must not save");
    assert!(!saved.models.contains_key(&identity));
}

/// An older response must not replace a newer one, and the identity check cannot catch it.
///
/// Both snapshots carry the SAME identity set -- which is the normal case, because
/// include_retired keeps withdrawn models as rows -- and disagree only about whether one of
/// them exists. So the older response passes the identity guard and would install, putting a
/// model the newer catalog withdrew back on the admission path.
#[test]
fn refresh_refuses_a_response_older_than_the_installed_catalog() {
    let cache = CatalogCache::empty();

    let mut newer = response(1_000);
    newer.catalog_version = 20;
    newer
        .models
        .get_mut("anthropic/claude-sonnet-4-5")
        .expect("fixture model")
        .insert("existence".into(), serde_json::json!("absent"));
    cache.refresh(newer).expect("the newer response installs");
    assert!(
        cache
            .snapshot()
            .expect("installed")
            .catalog_version()
            .eq(&20),
        "the newer response did not install, so the rest of this test proves nothing"
    );

    // Older, complete, and it disagrees: it still calls the model present.
    let mut older = response(2_000);
    older.catalog_version = 19;
    let error = cache
        .refresh(older)
        .expect_err("an older catalog version must be refused");
    match error {
        CatalogRefreshError::Stale {
            installed,
            received,
        } => assert_eq!((installed, received), (20, 19)),
        other => panic!("expected Stale, got {other}"),
    }
    assert_eq!(
        cache.snapshot().expect("still installed").catalog_version(),
        20,
        "the refusal must leave the newer catalog in place"
    );
}

/// Equal versions still install: a re-poll at the same version is the same document, and
/// installing it is what moves catalog_age_ms without changing a fact.
#[test]
fn refresh_accepts_a_response_at_the_same_version() {
    let cache = CatalogCache::empty();
    cache.refresh(response(1_000)).expect("first install");
    cache
        .refresh(response(2_000))
        .expect("an equal version is not stale");
}

fn temp_lkg(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    broca_test_support::scratch_root()
        .join(format!(
            "broca-catalog-{label}-{}-{nonce}",
            std::process::id()
        ))
        .join("fusiform-lkg.json")
}

#[test]
fn lkg_survives_a_new_cache_after_memory_is_dropped() {
    let path = temp_lkg("restart");
    {
        let cache = CatalogCache::open(&path);
        cache.refresh(response(1234)).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(path.parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
        assert_eq!(cache.snapshot().unwrap().source(), CatalogSource::Fusiform);
    }

    let restarted = CatalogCache::open(&path);
    let snapshot = restarted
        .snapshot()
        .expect("restart loads the persisted LKG");
    assert_eq!(snapshot.source(), CatalogSource::LastKnownGood);
    assert_eq!(snapshot.resolved_at_ms(), 1234);
    assert_eq!(snapshot.catalog_version(), 9);
    assert!(snapshot
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .is_some());
    let _ = fs::remove_dir_all(path.parent().unwrap());
}

#[test]
fn a_response_that_drops_known_identities_cannot_replace_the_complete_catalog() {
    let cache = CatalogCache::empty();
    cache.refresh(response(100)).unwrap();
    let mut partial = response(200);
    partial.models.remove("anthropic/claude-retired");

    let error = cache
        .refresh(partial)
        .expect_err("a smaller response is not a complete refresh");
    assert!(matches!(
        error,
        CatalogRefreshError::Incomplete {
            missing_count: 1,
            ..
        }
    ));
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.resolved_at_ms(), 100);
    assert!(snapshot.is_withdrawn("anthropic", "claude-retired"));
}

#[test]
fn total_absence_is_a_typed_error_naming_memory_and_lkg() {
    let path = temp_lkg("absent");
    let cache = CatalogCache::open(&path);
    let error = match cache.snapshot() {
        Ok(_) => panic!("empty cache unexpectedly resolved"),
        Err(error) => error,
    };
    assert_eq!(error.lkg_path(), Some(path.as_path()));
    let text = error.to_string();
    assert!(text.contains("no in-memory fusiform catalog"));
    assert!(text.contains("no last-known-good catalog"));
    assert!(text.contains(path.to_string_lossy().as_ref()));
}

#[test]
fn fusiform_response_maps_present_models_and_preserves_unknown_and_retired_states() {
    let cache = CatalogCache::empty();
    cache
        .install_for_test(response(100), CatalogSource::Fusiform)
        .unwrap();
    let (_, model) = cache
        .snapshot()
        .unwrap()
        .model_for_serving("anthropic", "claude-sonnet-4-5")
        .expect("mapped model");
    assert_eq!(model.capabilities.attachment, CapabilityFlag::Unknown);
    assert_eq!(model.capabilities.reasoning, CapabilityFlag::Known(true));
    assert_eq!(model.capabilities.tool_call, CapabilityFlag::Known(true));
    assert_eq!(model.limits.context, Some(200000));
    assert!(cache
        .snapshot()
        .unwrap()
        .is_withdrawn("anthropic", "claude-retired"));
}

/// `capability.reasoning_options` as fusiform serves it (the row values are copied from
/// fusiform's served fixture at bc2fe0b): null, `[]` and a populated array stay three
/// distinct states, an array keeps its entries in order, and an older catalog that does not
/// serve the fact reads as unstated rather than refusing to load.
#[test]
fn served_reasoning_options_keep_null_empty_and_populated_distinct() {
    let mut served = response(100);
    let row = |options: Option<serde_json::Value>| {
        let mut facts = BTreeMap::from([
            ("existence".into(), serde_json::json!("present")),
            ("limit.context".into(), serde_json::json!(200000)),
            ("capability.attachment".into(), serde_json::json!(true)),
            ("capability.reasoning".into(), serde_json::json!(true)),
            ("capability.tool_call".into(), serde_json::json!(true)),
        ]);
        if let Some(options) = options {
            facts.insert("capability.reasoning_options".into(), options);
        }
        facts
    };
    let populated = serde_json::json!([
        {"type": "toggle"},
        {"type": "effort", "values": ["low", "medium", "high"]},
        {"max": 127999, "min": 1024, "type": "budget_tokens"}
    ]);
    for (id, options) in [
        ("anthropic/null-options", Some(serde_json::Value::Null)),
        ("anthropic/empty-options", Some(serde_json::json!([]))),
        ("anthropic/populated-options", Some(populated.clone())),
        ("anthropic/unserved-options", None),
    ] {
        served.models.insert(id.into(), row(options));
    }
    let cache = CatalogCache::empty();
    cache
        .install_for_test(served, CatalogSource::Fusiform)
        .unwrap();
    let snapshot = cache.snapshot().unwrap();
    let options = |model: &str| {
        snapshot
            .model_for_serving("anthropic", model)
            .expect("mapped model")
            .1
            .reasoning_options
    };
    assert_eq!(options("null-options"), ReasoningOptions::Unstated);
    assert_eq!(options("empty-options"), ReasoningOptions::NoOptions);
    assert_eq!(options("populated-options").to_value(), populated);
    assert_eq!(options("unserved-options"), ReasoningOptions::Unstated);
}

/// Rows serving the same reasoning options hold one parsed list between them, so a catalog of
/// thousands of rows parses each distinct list once; a different list stays its own.
#[test]
fn rows_with_the_same_reasoning_options_share_one_parsed_list() {
    let mut served = response(100);
    let effort = serde_json::json!([{"type": "effort", "values": ["low", "medium", "high"]}]);
    let toggle = serde_json::json!([{"type": "toggle"}]);
    for (id, options) in [
        ("anthropic/first", &effort),
        ("anthropic/second", &effort),
        ("anthropic/other", &toggle),
    ] {
        served.models.insert(
            id.into(),
            BTreeMap::from([
                ("existence".into(), serde_json::json!("present")),
                ("limit.context".into(), serde_json::json!(200000)),
                ("capability.reasoning".into(), serde_json::json!(true)),
                ("capability.reasoning_options".into(), options.clone()),
            ]),
        );
    }
    let cache = CatalogCache::empty();
    cache
        .install_for_test(served, CatalogSource::Fusiform)
        .unwrap();
    let snapshot = cache.snapshot().unwrap();
    let list = |model: &str| match snapshot
        .model_for_serving("anthropic", model)
        .expect("mapped model")
        .1
        .reasoning_options
    {
        ReasoningOptions::Entries(entries) => entries,
        other => panic!("{other:?}"),
    };
    assert!(std::sync::Arc::ptr_eq(&list("first"), &list("second")));
    assert!(!std::sync::Arc::ptr_eq(&list("first"), &list("other")));
    assert_eq!(ReasoningOptions::Entries(list("second")).to_value(), effort);
}

#[test]
fn stale_health_reports_age_and_lkg_source_without_a_degradation_state() {
    let cache = CatalogCache::empty();
    cache
        .install_for_test(response(100), CatalogSource::LastKnownGood)
        .unwrap();
    let health = cache.health(5100).unwrap();
    assert_eq!(health.age_ms, 5000);
    assert_eq!(health.source, CatalogSource::LastKnownGood);
}
