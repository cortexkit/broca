//! The output cap a send gets when it sets none, proved on the frozen config a resolved provider
//! would write into RunStarted.
//!
//! The catalog is installed from a fusiform `catalog.get` document, so every case also goes
//! through the shipped decode of `limit.output`: a null there must reach resolution as "unknown"
//! and select the fallback, never a zero cap.
// A file-level `cfg` would delete this file when the feature is off, and a deleted file reports
// "running 0 tests ... ok". Gating a module leaves this shell so the absence announces itself.
#[cfg(not(feature = "live"))]
#[test]
fn this_suite_needs_its_feature_enabled() {
    eprintln!(
        "SKIP default_max_output: compiled without the live feature, so none of its cases exist. \
         Re-run with `-F broca-catalog/live` to execute them."
    );
}

#[cfg(feature = "live")]
mod gated {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Once};

    use broca_catalog::live::{
        resolve_chatgpt_pause_provider_with_client, resolve_provider_with_client, AuthMethod,
        AuthMode, ResolveOptions, WireFamilyId, FALLBACK_MAX_OUTPUT_TOKENS,
    };
    use broca_catalog::{CatalogCache, CatalogSource, FusiformCatalogResponse};
    use serde_json::{json, Value};

    const KNOWN_OUTPUT: u64 = 64_000;
    const CHATGPT_OUTPUT: u64 = 100_000;
    const NO_CONTEXT_OUTPUT: u64 = 50_000;

    fn row(context: Value, output: Value) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("existence".into(), json!("present")),
            ("limit.context".into(), context),
            ("limit.output".into(), output),
            ("capability.attachment".into(), json!(false)),
            ("capability.reasoning".into(), json!(true)),
            ("capability.tool_call".into(), json!(true)),
        ])
    }

    /// Every test here reads one process-global catalog, so it is installed once and never
    /// replaced; parallel tests then cannot swap it under each other.
    fn install() {
        static INSTALLED: Once = Once::new();
        INSTALLED.call_once(|| {
            let response = FusiformCatalogResponse {
                source: "models.dev".into(),
                resolved_at_ms: 1,
                catalog_version: 1,
                models: BTreeMap::from([
                    (
                        "anthropic/known-output".into(),
                        row(json!(200_000), json!(KNOWN_OUTPUT)),
                    ),
                    (
                        "anthropic/null-output".into(),
                        row(json!(200_000), Value::Null),
                    ),
                    (
                        "anthropic/output-equals-context".into(),
                        row(json!(500_000), json!(500_000)),
                    ),
                    (
                        "anthropic/output-above-context".into(),
                        row(json!(32_768), json!(64_000)),
                    ),
                    (
                        "anthropic/small-context-null-output".into(),
                        row(json!(16_384), Value::Null),
                    ),
                    (
                        "anthropic/small-context-zero-output".into(),
                        row(json!(16_384), json!(0)),
                    ),
                    (
                        "anthropic/small-context-placeholder-output".into(),
                        row(json!(16_384), json!(32_000)),
                    ),
                    (
                        "anthropic/unknown-context".into(),
                        row(Value::Null, json!(NO_CONTEXT_OUTPUT)),
                    ),
                    (
                        "anthropic/huge-output".into(),
                        row(json!(1_000_000), json!(393_216)),
                    ),
                    (
                        "openai/chatgpt-listed".into(),
                        row(json!(400_000), json!(CHATGPT_OUTPUT)),
                    ),
                ]),
                withheld: Vec::new(),
                uncertain: Vec::new(),
            };
            let cache = Arc::new(CatalogCache::empty());
            cache
                .install_for_test(response, CatalogSource::Fusiform)
                .expect("fixture catalog installs");
            broca_catalog::install_global_catalog_cache(cache);
        });
    }

    /// The `max_output_tokens` a plain catalog resolution freezes.
    fn frozen_cap(model: &str, caller: Option<u32>) -> Value {
        install();
        let resolved = resolve_provider_with_client(
            model,
            None,
            caller,
            &AuthMode::Secret("test-key".into()),
            &ResolveOptions {
                auth_method: Some(AuthMethod::ApiKey),
                ..Default::default()
            },
            broca_provider::build_provider_http_client(),
        )
        .unwrap_or_else(|error| panic!("{model} resolves: {error}"));
        resolved.provider.render_config_blob()["max_output_tokens"].clone()
    }

    #[test]
    fn an_omitted_cap_freezes_the_models_catalog_output_limit() {
        assert_eq!(frozen_cap("anthropic/known-output", None), KNOWN_OUTPUT);
    }

    /// A published limit above MAX_DEFAULT_OUTPUT_TOKENS is bounded when it is only a default,
    /// and sent as given when the caller asks for it. Both arms, so bounding explicit values too
    /// (or not bounding at all) fails here.
    #[test]
    fn an_unverified_large_catalog_limit_is_bounded_only_as_a_default() {
        assert_eq!(
            frozen_cap("anthropic/huge-output", None),
            broca_catalog::live::MAX_DEFAULT_OUTPUT_TOKENS
        );
        assert_eq!(frozen_cap("anthropic/huge-output", Some(393_216)), 393_216);
    }

    #[test]
    fn a_null_catalog_output_limit_freezes_the_fallback() {
        assert_eq!(
            frozen_cap("anthropic/null-output", None),
            FALLBACK_MAX_OUTPUT_TOKENS
        );
    }

    #[test]
    fn an_output_limit_not_below_the_context_window_is_a_placeholder() {
        assert_eq!(
            frozen_cap("anthropic/output-equals-context", None),
            FALLBACK_MAX_OUTPUT_TOKENS,
            "output == context would reserve the whole window for output"
        );
        assert_eq!(
            frozen_cap("anthropic/output-above-context", None),
            FALLBACK_MAX_OUTPUT_TOKENS,
            "output > context is not a cap the model can honour"
        );
    }

    #[test]
    fn default_output_cap_is_below_a_small_known_context() {
        for model in [
            "anthropic/small-context-null-output",
            "anthropic/small-context-zero-output",
            "anthropic/small-context-placeholder-output",
        ] {
            let cap = frozen_cap(model, None).as_u64().unwrap();
            assert_eq!(cap, 16_383, "{model}");
            assert!(cap < 16_384, "output must leave room in the context window");
            assert_eq!(frozen_cap(model, Some(100_000)), 100_000);
        }
        use broca_catalog::live::default_max_output_tokens;
        assert_eq!(default_max_output_tokens(None, Some(1)), 0);
        assert_eq!(
            default_max_output_tokens(None, Some(0)),
            FALLBACK_MAX_OUTPUT_TOKENS
        );
        assert_eq!(
            default_max_output_tokens(None, Some(u64::MAX)),
            FALLBACK_MAX_OUTPUT_TOKENS
        );
    }

    #[test]
    fn an_output_limit_with_no_known_context_is_used_as_served() {
        assert_eq!(
            frozen_cap("anthropic/unknown-context", None),
            NO_CONTEXT_OUTPUT
        );
    }

    #[test]
    fn an_explicit_caller_cap_wins_over_catalog_and_fallback_unclamped() {
        // Over a known catalog limit: sent as given, no downward clamp.
        assert_eq!(frozen_cap("anthropic/known-output", Some(100_000)), 100_000);
        // Under it: not raised to the model maximum either.
        assert_eq!(frozen_cap("anthropic/known-output", Some(1_024)), 1_024);
        // Where the fallback would apply.
        assert_eq!(frozen_cap("anthropic/null-output", Some(2_048)), 2_048);
    }

    #[test]
    fn the_base_url_override_has_no_catalog_model_and_uses_the_fallback() {
        install();
        let resolve = |caller| {
            resolve_provider_with_client(
                "anthropic/known-output",
                Some("http://127.0.0.1:9/v1"),
                caller,
                &AuthMode::None,
                &ResolveOptions::default(),
                broca_provider::build_provider_http_client(),
            )
            .unwrap_or_else(|error| panic!("override resolves: {error}"))
        };
        let resolved = resolve(None);
        assert_eq!(resolved.family_id, WireFamilyId::OpenAiChat);
        assert_eq!(
            resolved.provider.render_config_blob()["max_output_tokens"],
            FALLBACK_MAX_OUTPUT_TOKENS,
            "the override endpoint is opaque even when the id matches a catalog row"
        );
        assert_eq!(
            resolve(Some(777)).provider.render_config_blob()["max_output_tokens"],
            777
        );
    }

    #[test]
    fn the_family_override_path_reads_the_catalog_where_it_lists_the_model() {
        install();
        let try_resolve = |model: &str, caller| {
            resolve_chatgpt_pause_provider_with_client(
                model,
                caller,
                None,
                &ResolveOptions {
                    family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
                    auth_method: Some(AuthMethod::Chatgpt),
                    ..Default::default()
                },
                broca_provider::build_provider_http_client(),
            )
        };
        let resolve = |model: &str, caller| {
            try_resolve(model, caller)
                .unwrap_or_else(|error| panic!("{model} resolves: {error}"))
                .provider
                .render_config_blob()["max_output_tokens"]
                .clone()
        };
        assert_eq!(resolve("openai/chatgpt-listed", None), CHATGPT_OUTPUT);
        // A ChatGPT id the catalog does not list is no longer sent with a fallback cap: it is
        // refused as unknown, like an unlisted id on every other catalog path.
        let error = try_resolve("openai/not-in-catalog", None)
            .err()
            .expect("an unlisted ChatGPT id is refused");
        assert!(error
            .to_string()
            .contains("catalog has no served model 'openai/not-in-catalog'"));
        assert_eq!(resolve("openai/chatgpt-listed", Some(3_000)), 3_000);
    }

    #[test]
    fn a_restored_episode_cap_wins_over_the_catalog_but_not_over_the_caller() {
        install();
        let resolve = |caller, restored| {
            broca_catalog::live::resolve_catalog_episode_with_client(
                "anthropic/known-output",
                caller,
                &AuthMode::Secret("test-key".into()),
                &ResolveOptions {
                    auth_method: Some(AuthMethod::ApiKey),
                    ..Default::default()
                },
                broca_provider::build_provider_http_client(),
                broca_catalog::live::EpisodeRenderConfig {
                    base_url: None,
                    max_output_tokens: restored,
                    variant: None,
                    service_tier: None,
                    thinking_echo_scope: None,
                    thinking_cross_model: None,
                    responses_tools_non_strict: None,
                    chatgpt_effort_updates: None,
                },
            )
            .unwrap_or_else(|error| panic!("episode resolves: {error}"))
            .provider
            .render_config_blob()["max_output_tokens"]
                .clone()
        };
        assert_eq!(resolve(None, Some(8_192)), 8_192);
        assert_eq!(resolve(Some(2_048), Some(8_192)), 2_048);
        assert_eq!(resolve(None, None), KNOWN_OUTPUT);
    }
}
