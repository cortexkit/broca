//! An admitted episode keeps its output cap when the live catalog changes.

#[cfg(not(feature = "live"))]
#[test]
fn this_suite_needs_its_feature_enabled() {
    eprintln!("SKIP explicit_cap_resume: re-run with -F broca-catalog/live");
}

#[cfg(feature = "live")]
mod gated {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use broca_catalog::live::{
        resolve_catalog_episode_with_client, AuthMethod, AuthMode, EpisodeRenderConfig,
        FrozenRenderConfig, ResolveOptions,
    };
    use broca_catalog::{CatalogCache, CatalogSource, FusiformCatalogResponse};
    use serde_json::json;

    fn catalog(version: i64, output_limit: u64) -> FusiformCatalogResponse {
        FusiformCatalogResponse {
            source: "models.dev".into(),
            resolved_at_ms: version,
            catalog_version: version,
            models: BTreeMap::from([(
                "anthropic/explicit-resume".into(),
                BTreeMap::from([
                    ("existence".into(), json!("present")),
                    (
                        "wire.npm".into(),
                        json!({"value":"@ai-sdk/anthropic", "from":"provider"}),
                    ),
                    (
                        "wire.api".into(),
                        json!({"value":"https://api.anthropic.com", "from":"provider"}),
                    ),
                    ("limit.context".into(), json!(200_000)),
                    ("limit.output".into(), json!(output_limit)),
                    ("capability.attachment".into(), json!(false)),
                    ("capability.reasoning".into(), json!(true)),
                    ("capability.tool_call".into(), json!(true)),
                ]),
            )]),
            withheld: Vec::new(),
            uncertain: Vec::new(),
            wire_corrections: Vec::new(),
        }
    }

    #[test]
    fn an_explicit_cap_resumes_unchanged_after_the_catalog_maximum_moves() {
        // This target owns its process-global catalog, so replacing it cannot race
        // the other output-cap fixtures running in a different test executable.
        let cache = Arc::new(CatalogCache::empty());
        cache
            .install_for_test(catalog(1, 64_000), CatalogSource::Fusiform)
            .unwrap();
        broca_catalog::install_global_catalog_cache(cache.clone());
        let resolve = |recorded: Option<&FrozenRenderConfig>| {
            resolve_catalog_episode_with_client(
                "anthropic/explicit-resume",
                Some(100_000),
                &AuthMode::Secret("test-key".into()),
                &ResolveOptions {
                    auth_method: Some(AuthMethod::ApiKey),
                    ..Default::default()
                },
                broca_provider::build_provider_http_client(),
                EpisodeRenderConfig {
                    recorded_config: recorded,
                    ..Default::default()
                },
            )
            .expect("the episode resolves")
        };
        let admitted = resolve(None).provider.render_config_blob();
        assert_eq!(admitted["max_output_tokens"], 64_000);
        let frozen: FrozenRenderConfig = serde_json::from_value(admitted.clone()).unwrap();

        cache
            .install_for_test(catalog(2, 8_192), CatalogSource::Fusiform)
            .unwrap();
        assert_eq!(
            resolve(None).provider.render_config_blob()["max_output_tokens"],
            8_192,
            "a new admission must observe the lowered catalog maximum"
        );
        let resumed = resolve(Some(&frozen)).provider.render_config_blob();
        assert_eq!(
            resumed["max_output_tokens"], 64_000,
            "a resume must retain the cap frozen at admission, even with an explicit caller cap"
        );
        assert_eq!(resumed, admitted,
            "the complete frozen render config must remain equal so resume cannot refuse it as drift");
    }
}
