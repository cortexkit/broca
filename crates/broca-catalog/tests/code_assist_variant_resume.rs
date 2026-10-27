//! A Code Assist episode admitted with a reasoning variant resumes on the same backend tier
//! after the catalog stops serving the alias row that let the variant be admitted.
//!
//! This is its own test binary because it replaces the process-global catalog mid-test, which
//! would race the other suites that share one installed catalog.

#[cfg(feature = "live")]
mod gated {
    use std::sync::Arc;

    use broca_catalog::live::{
        AuthMethod, AuthMode, EpisodeRenderConfig, FrozenRenderConfig, ResolutionError,
        ResolveOptions, WireFamilyId,
    };
    use broca_catalog::{CatalogCache, CatalogSource, FusiformCatalogResponse};
    use broca_protocol::{
        content::{ContentBlock, Message, Role},
        request::CallOptions,
        StepContext,
    };
    use broca_provider::families::GoogleCodeAssist;
    use broca_provider::wire::WireFamily;
    use serde_json::Value;

    const MODEL: &str = "google/antigravity-gemini-3.8-flash";

    /// The shared reasoning-options fixture, optionally without the 3.8-flash alias row.
    fn install(with_alias_row: bool) {
        let mut response: FusiformCatalogResponse =
            serde_json::from_str(include_str!("fixtures/fusiform_reasoning_options.json"))
                .expect("fixture is a catalog.get response");
        if !with_alias_row {
            assert!(response.models.remove(MODEL).is_some());
        }
        let cache = Arc::new(CatalogCache::empty());
        cache
            .install_for_test(response, CatalogSource::Fusiform)
            .expect("fixture catalog installs");
        broca_catalog::install_global_catalog_cache(cache);
    }

    fn resolve(
        variant: Option<&str>,
        restored: Option<&FrozenRenderConfig>,
    ) -> Result<FrozenRenderConfig, ResolutionError> {
        let options = ResolveOptions {
            auth_method: Some(AuthMethod::Antigravity),
            family_override: Some(WireFamilyId::GoogleCodeAssist),
            project_id: Some("test-project"),
            variant,
            ..ResolveOptions::default()
        };
        let render = EpisodeRenderConfig {
            recorded_config: restored,
            codeassist_identity: None,
            claude_code_persona: Default::default(),
            base_url: None,
            max_output_tokens: None,
            variant: None,
            service_tier: None,
            thinking_echo_scope: None,
            thinking_cross_model: None,
            responses_tools_non_strict: None,
            chatgpt_effort_updates: None,
            chatgpt_wire: None,
            reasoning_content_required: None,
            reasoning_echo: None,
            provider_text_state: None,
            structured_output: None,
            ..Default::default()
        };
        let resolved = broca_catalog::live::resolve_catalog_episode_with_client(
            MODEL,
            None,
            &AuthMode::Secret("test-key".into()),
            &options,
            broca_provider::build_provider_http_client(),
            render,
        )?;
        Ok(
            serde_json::from_value(resolved.provider.render_config_blob())
                .expect("resolved provider exposes its frozen config"),
        )
    }

    fn body(frozen: &FrozenRenderConfig) -> Value {
        let mut opts = CallOptions::new(vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "Say hi.".into(),
            }],
        }]);
        opts.step_context = Some(StepContext {
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: "run-variant-resume".into(),
            step_index: 1,
        });
        GoogleCodeAssist.shape(frozen, &opts)
    }

    #[test]
    fn a_resume_keeps_the_frozen_tier_after_the_alias_row_is_withdrawn() {
        install(true);
        let admitted = resolve(Some("high"), None).expect("variant admitted with a catalog row");
        assert_eq!(admitted.wire_model_id, "gemini-3.8-flash-high");
        assert!(admitted.variant.is_some(), "the variant is frozen");

        install(false);
        // Fresh variants require a catalog row, but resume uses the variant frozen at admission.
        let error = resolve(Some("high"), None).unwrap_err();
        let ResolutionError::VariantUnsupported(refusal) = error else {
            panic!("missing catalog row must refuse the fresh variant");
        };
        assert!(refusal.reason.contains("the catalog has no row"));
        assert!(refusal.defined.is_empty());

        let resumed = resolve(None, Some(&admitted)).expect("frozen variant resumes");
        assert_eq!(resumed.wire_model_id, "gemini-3.8-flash-high");
        assert_eq!(resumed.variant, admitted.variant);
        assert_eq!(
            serde_json::to_vec(&body(&resumed)).unwrap(),
            serde_json::to_vec(&body(&admitted)).unwrap()
        );
    }
}
