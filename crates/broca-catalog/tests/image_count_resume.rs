//! Catalog image counts are admission inputs, never resume-time render inputs.

#[cfg(feature = "live")]
mod gated {
    use std::{collections::BTreeMap, sync::Arc};

    use broca_catalog::live::{
        resolve_catalog_episode_with_client, AuthMethod, AuthMode, EpisodeRenderConfig,
        FrozenRenderConfig, ResolveOptions,
    };
    use broca_catalog::{CatalogCache, CatalogSource, FusiformCatalogResponse};
    use broca_protocol::{CallOptions, ContentBlock, Message, Role};
    use serde_json::json;

    fn catalog(version: i64, count: u32) -> FusiformCatalogResponse {
        FusiformCatalogResponse {
            source: "models.dev".into(),
            resolved_at_ms: version,
            catalog_version: version,
            models: BTreeMap::from([(
                "google/gemini-4-argon".into(),
                BTreeMap::from([
                    ("existence".into(), json!("present")),
                    (
                        "wire.npm".into(),
                        json!({"value":"@ai-sdk/google", "from":"provider"}),
                    ),
                    (
                        "wire.api".into(),
                        json!({"value":"https://generativelanguage.googleapis.com/v1beta", "from":"provider"}),
                    ),
                    ("limit.context".into(), json!(1_000_000)),
                    ("limit.output".into(), json!(64_000)),
                    ("limit.images_per_request".into(), json!(count)),
                    (
                        "capability.input_modalities".into(),
                        json!(["text", "image"]),
                    ),
                    ("capability.attachment".into(), json!(true)),
                    ("capability.reasoning".into(), json!(false)),
                    ("capability.tool_call".into(), json!(true)),
                ]),
            )]),
            withheld: Vec::new(),
            uncertain: Vec::new(),
            wire_corrections: Vec::new(),
        }
    }

    #[test]
    fn resumed_episode_keeps_its_frozen_image_count_after_the_catalog_changes() {
        // This executable owns its global catalog, so parallel fixtures cannot replace it.
        let cache = Arc::new(CatalogCache::empty());
        cache
            .install_for_test(catalog(1, 3_600), CatalogSource::Fusiform)
            .unwrap();
        broca_catalog::install_global_catalog_cache(cache.clone());
        let resolve = |recorded: Option<&FrozenRenderConfig>| {
            resolve_catalog_episode_with_client(
                "google/gemini-4-argon",
                None,
                &AuthMode::Secret("test-key".into()),
                &ResolveOptions {
                    auth_method: Some(AuthMethod::ApiKey),
                    media: true,
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
        let admitted = resolve(None);
        let recorded = admitted.provider.render_config_blob();
        assert_eq!(recorded["media"]["image"]["max_count"], 3_600);
        let frozen: FrozenRenderConfig = serde_json::from_value(recorded.clone()).unwrap();
        let call = CallOptions::new(vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
        )]);
        let expected_bytes = admitted.provider.render_request(&call);

        cache
            .install_for_test(catalog(2, 50), CatalogSource::Fusiform)
            .unwrap();
        assert_eq!(
            resolve(None).provider.render_config_blob()["media"]["image"]["max_count"],
            50,
            "fresh admissions must observe the changed catalog"
        );
        let resumed = resolve(Some(&frozen));
        assert_eq!(resumed.provider.render_config_blob(), recorded);
        assert_eq!(resumed.provider.render_request(&call), expected_bytes);
    }
}
