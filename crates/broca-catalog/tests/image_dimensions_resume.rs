//! Catalog pixel bounds are admission inputs, never resume-time render inputs.

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

    fn catalog(version: i64, base: u32, conditional: u32) -> FusiformCatalogResponse {
        FusiformCatalogResponse {
            source: "models.dev".into(),
            resolved_at_ms: version,
            catalog_version: version,
            models: BTreeMap::from([(
                "anthropic/claude-sonnet-4-5".into(),
                BTreeMap::from([
                    ("existence".into(), json!("present")),
                    (
                        "wire.npm".into(),
                        json!({"value":"@ai-sdk/anthropic", "from":"provider"}),
                    ),
                    (
                        "wire.api".into(),
                        json!({"value":"https://api.anthropic.com/v1", "from":"provider"}),
                    ),
                    ("limit.context".into(), json!(200_000)),
                    ("limit.output".into(), json!(64_000)),
                    ("limit.image_long_edge_px".into(), json!(base)),
                    (
                        "limit.image_long_edge_px.above_images.20".into(),
                        json!(conditional),
                    ),
                    (
                        "capability.input_modalities".into(),
                        json!(["text", "image", "pdf"]),
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
    fn resumed_episode_keeps_its_frozen_image_dimensions_after_the_catalog_changes() {
        // This executable owns its global catalog, so parallel fixtures cannot replace it.
        let cache = Arc::new(CatalogCache::empty());
        cache
            .install_for_test(catalog(1, 8_000, 3_000), CatalogSource::Fusiform)
            .unwrap();
        broca_catalog::install_global_catalog_cache(cache.clone());
        let resolve = |recorded: Option<&FrozenRenderConfig>| {
            resolve_catalog_episode_with_client(
                "anthropic/claude-sonnet-4-5",
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
        assert_eq!(
            recorded["media"]["dimension_limits"],
            json!([
                {"above_count": 0, "max_side_px": 8_000},
                {"above_count": 20, "max_side_px": 3_000, "counts_documents": true},
            ])
        );
        let frozen: FrozenRenderConfig = serde_json::from_value(recorded.clone()).unwrap();
        let call = CallOptions::new(vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
        )]);
        let expected_bytes = admitted.provider.render_request(&call);

        cache
            .install_for_test(catalog(2, 7_000, 2_500), CatalogSource::Fusiform)
            .unwrap();
        let fresh = resolve(None).provider.render_config_blob();
        assert_eq!(fresh["media"]["dimension_limits"][0]["max_side_px"], 7_000);
        assert_eq!(fresh["media"]["dimension_limits"][1]["max_side_px"], 2_500);
        let resumed = resolve(Some(&frozen));
        assert_eq!(resumed.provider.render_config_blob(), recorded);
        assert_eq!(resumed.provider.render_request(&call), expected_bytes);
    }
}
