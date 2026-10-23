//! Compare complete renderer URLs against the immutable pre-migration route oracle.
#[cfg(not(feature = "live"))]
fn main() {
    panic!("full_url_parity requires --features live");
}
#[cfg(feature = "live")]
fn main() {
    audit::run();
}

#[cfg(feature = "live")]
mod audit {
    use broca_catalog::live::{
        AuthMethod, AuthMode, EpisodeRenderConfig, ResolveOptions, WireFamilyId,
    };
    use broca_catalog::{Catalog, CatalogCache, FusiformCatalogResponse};
    use broca_provider::{
        families::frozen_config_for_family,
        registry::wire_family,
        wire::{AuthHandle, FrozenRenderConfig},
    };
    use serde_json::{json, Value};
    use std::{
        collections::{BTreeMap, BTreeSet},
        env, fs,
        sync::Arc,
    };

    fn url(family: WireFamilyId, model: &str, base: Option<&str>) -> String {
        let mut frozen = frozen_config_for_family(family, model);
        frozen.base_url = base
            .filter(|base| !base.is_empty() && !base.starts_with('<'))
            .map(str::to_owned);
        frozen.opencode_go_session_id = Some("url-parity".into());
        if family == WireFamilyId::GoogleCodeAssist {
            frozen.project_id = Some("url-parity".into());
        }
        let mut options = broca_protocol::CallOptions::new(Vec::new());
        options.step_context = Some(broca_protocol::StepContext {
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: "url-parity".into(),
            step_index: 0,
        });
        let renderer = wire_family(family).unwrap();
        renderer
            .endpoint(
                &frozen,
                &AuthHandle::None,
                renderer.render(&frozen, &options),
            )
            .url
    }
    fn route(family: WireFamilyId, model: &str, base: Option<&str>) -> Value {
        json!({"family":family,"wire_model_id":model,"url":url(family,model,base)})
    }
    fn captured_route(value: &Value) -> Value {
        if let Some(error) = value.get("refused") {
            return json!({"refused":error});
        }
        route(
            serde_json::from_value(value["family"].clone()).unwrap(),
            value["wire_model_id"].as_str().unwrap(),
            value["base_url"].as_str(),
        )
    }
    fn pair_route(
        pair: Option<(
            broca_provider::spec::ProviderSpec,
            broca_provider::spec::ModelSpec,
        )>,
    ) -> Value {
        pair.map(|(provider, model)| {
            route(provider.wire_family, &model.id, Some(&provider.base_url))
        })
        .unwrap_or_else(|| json!({"refused":"no served row"}))
    }
    fn legacy(old: &FusiformCatalogResponse, pins: &Value) -> Catalog {
        let mut base = serde_json::Map::new();
        for (id, facts) in &old.models {
            if facts.get("existence").and_then(Value::as_str) != Some("present") {
                continue;
            }
            let (provider, model) = id.split_once('/').unwrap();
            let entry = base.entry(provider).or_insert_with(|| json!({"models":{}}));
            entry["models"][model] = json!({"id":model});
        }
        let mut overlay = pins.clone();
        overlay["patch"]
            .as_object_mut()
            .unwrap()
            .retain(|provider, _| base.contains_key(provider));
        Catalog::from_json_str(&Value::Object(base).to_string(), &overlay.to_string()).unwrap()
    }
    fn fresh(id: &str) -> Value {
        let (provider, model) = id.split_once('/').unwrap();
        let opts = ResolveOptions {
            auth_method: (provider == "meta").then_some(AuthMethod::Oauth),
            family_override: model
                .starts_with("antigravity-")
                .then_some(WireFamilyId::GoogleCodeAssist),
            project_id: Some("url-parity"),
            ..Default::default()
        };
        match broca_catalog::live::resolve_catalog_episode_with_client(
            id,
            Some(32000),
            &AuthMode::None,
            &opts,
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig {
                opencode_go_session_id: Some("url-parity"),
                ..Default::default()
            },
        ) {
            Ok(resolved) => {
                let frozen: FrozenRenderConfig =
                    serde_json::from_value(resolved.provider.render_config_blob()).unwrap();
                route(
                    frozen.family_id,
                    &frozen.wire_model_id,
                    frozen.base_url.as_deref(),
                )
            }
            Err(error) => json!({"refused":error.to_string()}),
        }
    }
    pub fn run() {
        let args: Vec<String> = env::args().collect();
        let report = check(&args);
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        assert_eq!(
            report["summary"]["go_full_url_differences"], 0,
            "Go full-URL differences block pin deletion"
        );
    }
    fn check(args: &[String]) -> Value {
        assert_eq!(
            args.len(),
            5,
            "full_url_parity <old-lkg> <served> <legacy-pins> <captured-66-before>"
        );
        let old: FusiformCatalogResponse =
            serde_json::from_str(&fs::read_to_string(&args[1]).unwrap()).unwrap();
        let served: FusiformCatalogResponse =
            serde_json::from_str(&fs::read_to_string(&args[2]).unwrap()).unwrap();
        let pins: Value = serde_json::from_str(&fs::read_to_string(&args[3]).unwrap()).unwrap();
        let before: Vec<Value> =
            serde_json::from_str(&fs::read_to_string(&args[4]).unwrap()).unwrap();
        assert_eq!(before.len(), 66);
        let baseline = legacy(&old, &pins);
        let cache = Arc::new(CatalogCache::empty());
        cache.refresh(served.clone()).unwrap();
        let snapshot = cache.snapshot().unwrap();
        broca_catalog::install_global_catalog_cache(cache);
        let mut production = Vec::new();
        let mut unexpected = 0;
        for row in &before {
            let id = row["usage"]["model"].as_str().unwrap();
            let (provider, _) = id.split_once('/').unwrap();
            let previous = captured_route(&row["route"]);
            let current = fresh(id);
            let status = if previous == current {
                "same full URL and family"
            } else if previous.get("refused").is_some() && current.get("refused").is_some() {
                "refused before and after"
            } else if id == "opencode/muse-spark-1.3-contributor-free"
                && served
                    .models
                    .get(id)
                    .and_then(|f| f.get("model.status"))
                    .and_then(Value::as_str)
                    == Some("deprecated")
            {
                "deprecated upstream; unresolved; no deprecation rule added"
            } else if previous["family"] == current["family"]
                && previous["wire_model_id"] == current["wire_model_id"]
                && [
                    "opencode",
                    "bailing",
                    "minimax-cn",
                    "minimax-cn-coding-plan",
                    "pioneer",
                ]
                .contains(&provider)
            {
                "approved upstream address correction"
            } else {
                unexpected += 1;
                "UNEXPECTED"
            };
            production.push(
                json!({"usage":row["usage"],"old":previous,"new":current,"comparison":status}),
            );
        }
        let mut go = Vec::new();
        let mut anthropic_pins = Vec::new();
        let mut go_candidate_matches = 0;
        let mut go_mismatches = 0;
        for (id, facts) in served
            .models
            .iter()
            .filter(|(id, _)| id.starts_with("opencode-go/"))
        {
            let (_, model) = id.split_once('/').unwrap();
            let old_pair = pair_route(baseline.model_for_serving("opencode-go", model));
            let inherited_pin = route(
                serde_json::from_value(pins["patch"]["opencode-go"]["wire_family"].clone())
                    .unwrap(),
                model,
                pins["patch"]["opencode-go"]["base_url"].as_str(),
            );
            let reference = if old_pair.get("url").is_some() {
                &old_pair
            } else {
                &inherited_pin
            };
            let presence = facts.get("existence").and_then(Value::as_str).unwrap();
            let candidate = if presence == "present" {
                match snapshot
                    .routing_facts("opencode-go", model)
                    .unwrap()
                    .route()
                {
                    Ok((family, base)) => route(family, model, Some(&base)),
                    Err(error) => json!({"refused":error}),
                }
            } else {
                json!({"refused":presence})
            };
            let actual = fresh(id);
            let status = if candidate.get("url").is_some() {
                if &candidate == reference {
                    go_candidate_matches += 1;
                    "full URL matches pre-migration route"
                } else {
                    go_mismatches += 1;
                    "unapproved full-URL difference"
                }
            } else {
                "no candidate URL; absent or withheld send refused"
            };
            if actual.get("url").is_some() {
                assert_eq!(
                    actual, *reference,
                    "{id}: full URL must match the pre-migration route"
                );
            } else {
                assert!(
                    candidate.get("refused").is_some(),
                    "{id}: a fact-complete Go row must remain resolvable"
                );
            }
            let entry = json!({"model":id,"existence":presence,"old_pin_url":old_pair,"inherited_provider_pin":inherited_pin,"baseline_present":old.models.contains_key(id),"served_api":facts.get("wire.api"),"candidate":candidate,"actual":actual,"comparison":status});
            if pins["patch"]["opencode-go"]["models"][model]["wire_family"] == "anthropic_messages"
            {
                anthropic_pins.push(entry.clone());
            }
            go.push(entry);
        }
        assert_eq!(
            anthropic_pins.len(),
            7,
            "all seven original Anthropic Go pins must be audited, including absent/withheld rows"
        );
        let mut google = Vec::new();
        let mut google_differences = 0;
        for (id, facts) in served.models.iter().filter(|(_, f)| {
            f.get("wire.npm")
                .and_then(|v| v.get("value"))
                .and_then(Value::as_str)
                == Some("@ai-sdk/google")
        }) {
            let (provider, model) = id.split_once('/').unwrap();
            let previous = pair_route(baseline.model_for_serving(provider, model));
            let current = pair_route(snapshot.model_for_serving(provider, model));
            if previous != current {
                google_differences += 1;
            }
            google.push(
                json!({"model":id,"old":previous,"new":current,"served_api":facts.get("wire.api")}),
            );
        }
        let production_ids: BTreeSet<_> = before
            .iter()
            .map(|r| r["usage"]["model"].as_str().unwrap())
            .collect();
        let google_production_differences: Vec<_> = google
            .iter()
            .filter(|r| {
                production_ids.contains(r["model"].as_str().unwrap()) && r["old"] != r["new"]
            })
            .collect();
        assert!(google_production_differences.is_empty());
        let summary: BTreeMap<&str, usize> = BTreeMap::from([
            ("production_models", production.len()),
            ("unexpected_production_differences", unexpected),
            ("go_rows", go.len()),
            ("go_candidate_full_url_matches", go_candidate_matches),
            ("go_full_url_differences", go_mismatches),
            ("anthropic_go_pins", anthropic_pins.len()),
            ("google_format_rows", google.len()),
            ("nonproduction_google_differences", google_differences),
        ]);
        eprintln!("full-URL parity: {summary:?}");
        assert_eq!(unexpected, 0, "the 66-model full-URL gate must be clean");
        json!({"summary":summary,"production":production,"go":go,"anthropic_go_pins":anthropic_pins,"google_format":google})
    }

    #[test]
    fn full_url_gate_covers_production_go_and_google_format_rows() {
        let fixtures = format!(
            "{}/tests/fixtures/routing-parity",
            env!("CARGO_MANIFEST_DIR")
        );
        let args = vec![
            "full_url_parity".into(),
            format!("{fixtures}/full-url-old-lkg.json"),
            format!("{fixtures}/full-url-served.json"),
            format!("{fixtures}/legacy-routing-pins.json"),
            format!("{fixtures}/before.json"),
        ];
        let report = check(&args);
        assert_eq!(report["summary"]["production_models"], 66);
        assert_eq!(report["summary"]["go_rows"], 55);
        assert_eq!(report["summary"]["go_candidate_full_url_matches"], 29);
        assert_eq!(report["summary"]["go_full_url_differences"], 0);
        assert_eq!(report["summary"]["google_format_rows"], 55);
        assert_eq!(report["summary"]["nonproduction_google_differences"], 16);
        assert_ne!(
            url(
                WireFamilyId::AnthropicMessages,
                "model",
                Some("https://same.example")
            ),
            url(
                WireFamilyId::OpenAiChat,
                "model",
                Some("https://same.example")
            ),
            "equal bases do not prove full-URL parity"
        );
    }
}
