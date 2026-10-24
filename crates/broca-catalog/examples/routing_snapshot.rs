//! Capture routing decisions for a supplied model-usage list without sending provider requests.
//! The output can be compared with a later catalog build before removing routing data.
//! This captures the tuple baseline; `full_url_parity` is the blocking URL gate because
//! equal bases do not prove that two family paths produce the same request URL.
#[cfg(not(feature = "live"))]
fn main() {
    panic!("routing_snapshot requires --features live");
}

#[cfg(feature = "live")]
fn main() {
    gated::run();
}

#[cfg(feature = "live")]
mod gated {
    use std::{env, fs, sync::Arc};

    use broca_catalog::live::{resolve_provider, AuthMode, ResolveOptions};
    use broca_catalog::{install_global_catalog_cache, CatalogCache};
    use broca_provider::spec::WireFamilyId;
    use serde_json::{json, Value};

    fn endpoint(route: &Value) -> Option<String> {
        let family: WireFamilyId = serde_json::from_value(route.get("family")?.clone()).ok()?;
        let mut frozen = broca_provider::families::frozen_config_for_family(
            family,
            route.get("wire_model_id")?.as_str()?,
        );
        frozen.base_url = route
            .get("base_url")
            .and_then(Value::as_str)
            .filter(|base| !base.is_empty() && !base.starts_with('<'))
            .map(str::to_owned);
        frozen.project_id = Some("parity-only".into());
        Some(
            broca_provider::registry::wire_family(family)?
                .endpoint(&frozen, &broca_provider::wire::AuthHandle::None, Vec::new())
                .url,
        )
    }

    fn comparison(id: &str, before: &Value, after: &Value, deprecated: bool) -> &'static str {
        if before == after {
            return "unchanged";
        }
        if before.get("refused").is_some() && after.get("refused").is_some() {
            return "refused before and after";
        }
        if id == "opencode/muse-spark-1.3-contributor-free" && deprecated {
            return "deprecated upstream; pin removed; unresolved (admission has no deprecation rule)";
        }
        let (provider, _) = id.split_once('/').unwrap();
        if before["family"] == after["family"] && before["wire_model_id"] == after["wire_model_id"]
        {
            if before["tiers"] == after["tiers"]
                && endpoint(before) == endpoint(after)
                && endpoint(before).is_some()
            {
                return "same full request URL";
            }
            if before["base_url"] == after["base_url"] && id.starts_with("google/antigravity-") {
                return "published tier reverse lookup; default bytes unchanged";
            }
            if before["tiers"] == after["tiers"]
                && [
                    "opencode",
                    "bailing",
                    "minimax-cn",
                    "minimax-cn-coding-plan",
                    "pioneer",
                    "opencode-go",
                ]
                .contains(&provider)
            {
                return "allowed upstream endpoint correction";
            }
        }
        "unexpected difference"
    }

    pub fn run() {
        let args: Vec<String> = env::args().collect();
        assert!(
            (3..=5).contains(&args.len()),
            "routing_snapshot <lkg.json> <models-used.json> [--facts [before.json]]"
        );
        let facts = args.get(3).is_some_and(|arg| arg == "--facts");
        let cache = Arc::new(CatalogCache::open(&args[1]));
        let snapshot = cache.snapshot().expect("readable catalog");
        install_global_catalog_cache(cache);
        let used: Vec<Value> =
            serde_json::from_str(&fs::read_to_string(&args[2]).unwrap()).unwrap();
        let mut rows = Vec::new();
        let before: Option<Vec<Value>> = args
            .get(4)
            .map(|path| serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap());
        let source: Value = serde_json::from_str(&fs::read_to_string(&args[1]).unwrap()).unwrap();
        if let Some(before) = &before {
            assert_eq!(before.len(), used.len());
        }
        let mut unexpected = 0;
        for usage in used {
            let id = usage["model"].as_str().unwrap();
            let (provider_id, model_id) = id.split_once('/').unwrap();
            let route = if model_id.starts_with("antigravity-") {
                let opts = ResolveOptions {
                    family_override: Some(WireFamilyId::GoogleCodeAssist),
                    project_id: Some("parity-only"),
                    ..Default::default()
                };
                match resolve_provider(id, None, 32000, &AuthMode::None, &opts) {
                    Ok(resolved) => {
                        let mut tiers = serde_json::Map::new();
                        for level in ["low", "medium", "high"] {
                            let opts = ResolveOptions {
                                variant: Some(level),
                                ..opts.clone()
                            };
                            let tier = resolve_provider(id, None, 32000, &AuthMode::None, &opts)
                                .map(|r| Value::String(r.wire_model))
                                .unwrap_or(Value::Null);
                            tiers.insert(level.into(), tier);
                        }
                        json!({"base_url": resolved.base_url, "family": resolved.family_id,
                           "wire_model_id": resolved.wire_model, "tiers": tiers})
                    }
                    Err(error) => json!({"refused": error.to_string()}),
                }
            } else {
                let route = if facts && snapshot.contains_model(provider_id, model_id) {
                    if provider_id == "meta" {
                        // Meta subscription credentials require its Responses client endpoint,
                        // independently of whether a model row has models.dev wire metadata.
                        Ok((
                            WireFamilyId::OpenAiResponses,
                            "https://api.meta.ai/v1".into(),
                        ))
                    } else {
                        snapshot.resolve_route(provider_id, model_id)
                    }
                } else {
                    snapshot
                        .model_for_serving(provider_id, model_id)
                        .map(|(provider, _)| (provider.wire_family, provider.base_url))
                        .ok_or_else(|| "no served catalog row".to_string())
                };
                match route {
                    Ok((family, api)) => json!({"base_url": api,
                    "family": family, "wire_model_id": model_id, "tiers": {}}),
                    Err(reason) => json!({"refused": reason}),
                }
            };
            let status = before.as_ref().map(|before| {
                let previous = before
                    .iter()
                    .find(|row| row["usage"]["model"] == id)
                    .expect("baseline contains every usage row");
                assert_eq!(previous["usage"], usage);
                comparison(
                    id,
                    &previous["route"],
                    &route,
                    source["models"][id]["model.status"] == "deprecated",
                )
            });
            unexpected += usize::from(status == Some("unexpected difference"));
            rows.push(json!({"usage": usage, "route": route, "comparison": status}));
        }
        println!("{}", serde_json::to_string_pretty(&rows).unwrap());
        if before.is_some() {
            eprintln!(
                "routing parity: {} models compared; {unexpected} unexpected differences",
                rows.len()
            );
            assert_eq!(
                unexpected, 0,
                "routing parity must be clean before routing data is deleted"
            );
        }
    }

    #[test]
    fn comparison_rejects_a_new_refusal_or_unapproved_endpoint_drift() {
        let before = json!({"base_url":"https://published.example", "family":"openai_chat", "wire_model_id":"model", "tiers":{}});
        let refused = json!({"refused":"missing wire.api"});
        assert_eq!(
            comparison("deepseek/model", &before, &refused, false),
            "unexpected difference"
        );
        let mut changed = before.clone();
        changed["base_url"] = json!("https://different.example");
        assert_eq!(
            comparison("deepseek/model", &before, &changed, false),
            "unexpected difference"
        );
        assert_eq!(
            comparison("opencode/model", &before, &changed, false),
            "allowed upstream endpoint correction"
        );
        changed["family"] = json!("openai_responses");
        assert_eq!(
            comparison("opencode/model", &before, &changed, false),
            "unexpected difference"
        );
    }
}
