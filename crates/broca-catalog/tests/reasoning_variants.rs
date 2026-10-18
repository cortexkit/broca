//! A caller's reasoning variant, resolved against a model's catalog `reasoning_options` and
//! rendered by its wire family.
//!
//! The catalog is installed from `fixtures/fusiform_reasoning_options.json`, a fusiform
//! `catalog.get` response with a few clearly labelled Broca test rows (not Fusiform rows):
//!
//! - `anyapi/anthropic/claude-opus-4-6`, `anthropic/claude-sonnet-4-5`,
//!   `nebius/Qwen/Qwen3-Next-80B-A3B-Thinking` and `synthetic/unknown-reasoning` are copied from
//!   fusiform's served fixture (`crates/fusiform-module/fixtures/served-payloads.json` at fusiform
//!   commit bc2fe0b), keeping only the facts broca requests. The nebius row gains `existence`
//!   and null limits, which that capability-only request did not ask for.
//! - `anthropic/claude-opus-4-6` carries the anyapi row's options unchanged (the same upstream
//!   model), so the Anthropic family is exercised on toggle + effort + budget.
//! - `openai/gpt-5.6-luna` carries the options fusiform serves live for it:
//!   `[{"type":"effort","values":["none","low","medium","high","xhigh","max"]}]`.
//! - `sarvam/sarvam-30b` carries the shape fusiform's own test serves for it: a `null` inside
//!   `values`, a toggle, a budget, and an entry type nobody has invented yet.
//! - `google/gemini-3.5-flash` and `google/gemini-2.5-flash` are the vendored models.dev rows;
//!   `google/gemini-3.8-flash-high` is a Broca test row, not a Fusiform row, using the served
//!   3.8-flash alias options to exercise a live tier id with an agreeing or replacing variant.
//! - `google/antigravity-gemini-3.8-flash` is fusiform's served alias row verbatim (the
//!   "catalog.get — a curated alias" case of `served-payloads.json` at fusiform cf99add), and
//!   the top-level `aliased` array starts from that case's. The 3.5-flash alias row and the
//!   `antigravity-claude-opus-4-6-thinking` row are Broca test rows, not Fusiform rows: they
//!   exercise the live 3.5 route's tier limit and a live non-Gemini Antigravity route without
//!   tiers. `google/antigravity-gemini-3.6-flash` and
//!   `google/antigravity-claude-sonnet-4-6-thinking` are retained from the served fixture only
//!   for the retirement-before-variant test; both routes are now withdrawn.
//! - The Argon row projects antigravity-auth's grouped registry definition into catalog facts:
//!   low/medium/high effort, a 1,048,576 context window and a 65,536 output limit.
#[cfg(not(feature = "live"))]
#[test]
fn this_suite_needs_its_feature_enabled() {
    eprintln!(
        "SKIP reasoning_variants: compiled without the live feature, so none of its cases \
         exist. Re-run with `-F broca-catalog/live` to execute them."
    );
}

#[cfg(feature = "live")]
mod gated {
    use std::sync::{Arc, Once};

    use broca_catalog::live::{
        resolve_chatgpt_pause_provider_with_client, AuthMethod, AuthMode, EpisodeRenderConfig,
        FrozenRenderConfig, FrozenVariant, ResolveOptions, WireFamilyId,
    };
    use broca_catalog::{CatalogCache, CatalogSource, FusiformCatalogResponse};
    use broca_protocol::{
        content::{ContentBlock, Message, Role},
        request::CallOptions,
        StepContext,
    };
    use broca_provider::families::{
        AnthropicMessages, Gemini, GoogleCodeAssist, OpenAiChat, OpenAiResponses,
        OpenAiResponsesChatgpt,
    };
    use broca_provider::spec::ReasoningOptions;
    use broca_provider::wire::WireFamily;
    use serde_json::{json, Value};

    fn install() {
        static INSTALLED: Once = Once::new();
        INSTALLED.call_once(|| {
            let response: FusiformCatalogResponse =
                serde_json::from_str(include_str!("fixtures/fusiform_reasoning_options.json"))
                    .expect("fixture is a catalog.get response");
            let cache = Arc::new(CatalogCache::empty());
            cache
                .install_for_test(response, CatalogSource::Fusiform)
                .expect("fixture catalog installs");
            broca_catalog::install_global_catalog_cache(cache);
        });
    }

    fn frozen_of(resolved: broca_catalog::live::ProviderResolution) -> FrozenRenderConfig {
        serde_json::from_value(resolved.provider.render_config_blob())
            .expect("resolved provider exposes its frozen config")
    }

    /// Resolve with an API key (or the method given) and an optional variant.
    fn resolve_with(
        model: &str,
        method: AuthMethod,
        variant: Option<&str>,
        caller_max: Option<u32>,
        restored: Option<&FrozenVariant>,
    ) -> Result<FrozenRenderConfig, String> {
        resolve_with_typed(model, method, variant, caller_max, restored)
            .map_err(|error| error.to_string())
    }

    fn resolve_with_typed(
        model: &str,
        method: AuthMethod,
        variant: Option<&str>,
        caller_max: Option<u32>,
        restored: Option<&FrozenVariant>,
    ) -> Result<FrozenRenderConfig, broca_catalog::live::ResolutionError> {
        install();
        let family_override = match method {
            AuthMethod::Antigravity => Some(WireFamilyId::GoogleCodeAssist),
            AuthMethod::Chatgpt => Some(WireFamilyId::OpenAiResponsesChatgpt),
            AuthMethod::ApiKey | AuthMethod::Oauth => None,
        };
        let options = ResolveOptions {
            auth_method: Some(method),
            family_override,
            project_id: Some("test-project"),
            variant,
            ..ResolveOptions::default()
        };
        let render = EpisodeRenderConfig {
            codeassist_identity: None,
            claude_code_persona: Default::default(),
            base_url: None,
            max_output_tokens: None,
            variant: restored,
            service_tier: None,
            thinking_echo_scope: None,
            thinking_cross_model: None,
            responses_tools_non_strict: None,
            chatgpt_effort_updates: None,
            reasoning_content_required: None,
            reasoning_echo: None,
            provider_text_state: None,
            structured_output: None,
        };
        let resolved = if method == AuthMethod::Chatgpt {
            resolve_chatgpt_pause_provider_with_client(
                model,
                caller_max,
                None,
                &options,
                broca_provider::build_provider_http_client(),
            )
        } else {
            broca_catalog::live::resolve_catalog_episode_with_client(
                model,
                caller_max,
                &AuthMode::Secret("test-key".into()),
                &options,
                broca_provider::build_provider_http_client(),
                render,
            )
        };
        resolved.map(frozen_of)
    }

    fn resolve(
        model: &str,
        method: AuthMethod,
        variant: Option<&str>,
    ) -> Result<FrozenRenderConfig, String> {
        resolve_with(model, method, variant, None, None)
    }

    fn resolved(model: &str, method: AuthMethod, variant: Option<&str>) -> FrozenRenderConfig {
        resolve(model, method, variant)
            .unwrap_or_else(|error| panic!("{model} variant {variant:?} must resolve: {error}"))
    }

    fn refusal(model: &str, method: AuthMethod, variant: &str) -> String {
        resolve(model, method, Some(variant))
            .err()
            .unwrap_or_else(|| panic!("{model} variant '{variant}' must be refused"))
    }

    fn request() -> CallOptions {
        let mut opts = CallOptions::new(vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "Say hi.".into(),
            }],
        }]);
        opts.step_context = Some(StepContext {
            run_id: "run-variant-01".into(),
            step_index: 1,
        });
        opts
    }

    /// The request body the frozen config renders, through the family's real renderer.
    fn body(frozen: &FrozenRenderConfig) -> Value {
        let opts = request();
        match frozen.family_id {
            WireFamilyId::AnthropicMessages => AnthropicMessages.shape(frozen, &opts),
            WireFamilyId::OpenAiChat => OpenAiChat.shape(frozen, &opts),
            WireFamilyId::OpenAiResponses => OpenAiResponses.shape(frozen, &opts),
            WireFamilyId::OpenAiResponsesChatgpt => OpenAiResponsesChatgpt.shape(frozen, &opts),
            WireFamilyId::Gemini => Gemini.shape(frozen, &opts),
            WireFamilyId::GoogleCodeAssist => GoogleCodeAssist.shape(frozen, &opts),
            other => panic!("no body helper for {other:?}"),
        }
    }

    fn variant_body(model: &str, method: AuthMethod, variant: &str) -> Value {
        body(&resolved(model, method, Some(variant)))
    }

    #[test]
    fn served_options_keep_null_empty_and_populated_as_three_states() {
        install();
        let catalog = broca_catalog::catalog().expect("catalog installed");
        let options = |provider: &str, model: &str| {
            catalog
                .model_for_serving(provider, model)
                .unwrap_or_else(|| panic!("{provider}/{model} is served"))
                .1
                .reasoning_options
        };
        assert_eq!(
            options("synthetic", "unknown-reasoning"),
            ReasoningOptions::Unstated
        );
        assert_eq!(
            options("nebius", "Qwen/Qwen3-Next-80B-A3B-Thinking"),
            ReasoningOptions::NoOptions
        );
        assert_eq!(
            options("anthropic", "claude-sonnet-4-5").to_value(),
            json!([{"min": 1024, "type": "budget_tokens"}])
        );

        // Both empty states define no variant, and each says which statement the catalog made.
        let unstated = refusal("synthetic/unknown-reasoning", AuthMethod::ApiKey, "high");
        assert!(
            unstated.contains("publishes no reasoning options"),
            "{unstated}"
        );
        let empty = refusal(
            "nebius/Qwen/Qwen3-Next-80B-A3B-Thinking",
            AuthMethod::ApiKey,
            "high",
        );
        assert!(
            empty.contains("states it takes no reasoning options"),
            "{empty}"
        );
    }

    /// Anthropic, toggle + effort + budget (claude-opus-4-6): the effort entry wins, preceded by
    /// the toggle's `none`. 4.6 takes adaptive thinking alongside the effort.
    #[test]
    fn anthropic_effort_variants_render_adaptive_thinking_and_output_effort() {
        for method in [AuthMethod::Oauth, AuthMethod::ApiKey] {
            let high = variant_body("anthropic/claude-opus-4-6", method, "high");
            assert_eq!(
                high["thinking"],
                json!({"type": "adaptive", "display": "summarized"})
            );
            assert_eq!(high["output_config"], json!({"effort": "high"}));
            let low = variant_body("anthropic/claude-opus-4-6", method, "low");
            assert_eq!(low["output_config"], json!({"effort": "low"}));
            let none = variant_body("anthropic/claude-opus-4-6", method, "none");
            assert_eq!(none["thinking"], json!({"type": "disabled"}));
            assert!(none.get("output_config").is_none());
        }
        let error = refusal("anthropic/claude-opus-4-6", AuthMethod::Oauth, "max");
        assert_eq!(
            error,
            "model 'anthropic/claude-opus-4-6' does not define variant 'max'; its variants are \
             [none, low, medium, high]"
        );
    }

    /// Anthropic, budget only (claude-sonnet-4-5): `high` is half the maximum and `max` the
    /// maximum, bounded by OpenCode's 32,000 output ceiling and the model's 64,000 output limit.
    #[test]
    fn anthropic_budget_variants_render_enabled_thinking_budgets() {
        let high = variant_body("anthropic/claude-sonnet-4-5", AuthMethod::Oauth, "high");
        assert_eq!(
            high["thinking"],
            json!({"type": "enabled", "budget_tokens": 16000})
        );
        assert!(high.get("output_config").is_none());
        let max = variant_body("anthropic/claude-sonnet-4-5", AuthMethod::ApiKey, "max");
        assert_eq!(
            max["thinking"],
            json!({"type": "enabled", "budget_tokens": 31999})
        );
        let error = refusal("anthropic/claude-sonnet-4-5", AuthMethod::Oauth, "none");
        assert!(error.ends_with("its variants are [high, max]"), "{error}");
        // Anthropic rejects a thinking budget at or above max_tokens, so admission does too.
        let error = resolve_with(
            "anthropic/claude-sonnet-4-5",
            AuthMethod::Oauth,
            Some("high"),
            Some(8_000),
            None,
        )
        .expect_err("a budget above the output cap is refused");
        assert!(error.contains("output cap is 8000"), "{error}");
    }

    /// OpenAI Chat (anyapi's claude-opus-4-6 is served over the openai_chat family, as OpenCode
    /// serves it over openai-compatible): the effort values map to `reasoning_effort`, and the
    /// toggle adds nothing because that family has no toggle mapping.
    #[test]
    fn openai_chat_variants_render_reasoning_effort() {
        for level in ["low", "high"] {
            let body = variant_body(
                "anyapi/anthropic/claude-opus-4-6",
                AuthMethod::ApiKey,
                level,
            );
            assert_eq!(body["reasoning_effort"], json!(level));
            assert!(body.get("thinking").is_none());
        }
        let error = refusal(
            "anyapi/anthropic/claude-opus-4-6",
            AuthMethod::ApiKey,
            "none",
        );
        assert!(
            error.ends_with("its variants are [low, medium, high]"),
            "{error}"
        );
    }

    #[test]
    fn openai_api_key_variants_render_responses_reasoning() {
        for level in ["none", "high"] {
            let body = variant_body("openai/gpt-5.6-luna", AuthMethod::ApiKey, level);
            assert_eq!(body["reasoning"]["effort"], json!(level));
            assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
            assert_eq!(body["store"], false);
            assert!(body.get("reasoning_effort").is_none());
        }
    }

    /// ChatGPT codex (Responses): `reasoning.effort` with summary `auto`, plus the encrypted
    /// reasoning include.
    #[test]
    fn chatgpt_variants_render_responses_reasoning() {
        for level in ["xhigh", "max", "none"] {
            let body = variant_body("openai/gpt-5.6-luna", AuthMethod::Chatgpt, level);
            assert_eq!(
                body["reasoning"],
                json!({"effort": level, "summary": "auto"}),
                "{level}"
            );
            assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
        }
        let error = refusal("openai/gpt-5.6-luna", AuthMethod::Chatgpt, "minimal");
        assert!(
            error.ends_with("its variants are [none, low, medium, high, xhigh, max]"),
            "{error}"
        );
    }

    /// Gemini: effort values become `thinkingLevel`; toggle + budget give `none`, `high`, `max`.
    #[test]
    fn gemini_variants_render_thinking_config() {
        for level in ["low", "high"] {
            let body = variant_body("google/gemini-3.5-flash", AuthMethod::ApiKey, level);
            assert_eq!(
                body["generationConfig"]["thinkingConfig"],
                json!({"includeThoughts": true, "thinkingLevel": level})
            );
        }
        let none = variant_body("google/gemini-2.5-flash", AuthMethod::ApiKey, "none");
        assert_eq!(
            none["generationConfig"]["thinkingConfig"],
            json!({"includeThoughts": false, "thinkingBudget": 0})
        );
        let high = variant_body("google/gemini-2.5-flash", AuthMethod::ApiKey, "high");
        assert_eq!(
            high["generationConfig"]["thinkingConfig"],
            json!({"includeThoughts": true, "thinkingBudget": 12288})
        );
        let error = refusal("google/gemini-2.5-flash", AuthMethod::ApiKey, "low");
        assert!(
            error.ends_with("its variants are [none, high, max]"),
            "{error}"
        );
    }

    /// Code Assist: a tier id already selects thinking, and a variant naming the same thinking
    /// is applied and changes no byte.
    #[test]
    fn code_assist_variant_agreeing_with_the_tier_is_applied_without_changing_bytes() {
        let plain = resolved(
            "google/gemini-3.8-flash-high",
            AuthMethod::Antigravity,
            None,
        );
        let agreeing = resolved(
            "google/gemini-3.8-flash-high",
            AuthMethod::Antigravity,
            Some("high"),
        );
        assert_eq!(
            agreeing.variant.as_ref().map(|v| v.name.as_str()),
            Some("high")
        );
        assert_eq!(body(&agreeing), body(&plain));
        assert_eq!(
            body(&agreeing)["request"]["generationConfig"]["thinkingConfig"],
            json!({"includeThoughts": true, "thinkingBudget": -1})
        );
    }

    /// Without a catalog row the resolver cannot know a model's variants, even if the renderer
    /// knows some tiers. Sending no variant is still allowed through the explicit family route.
    #[test]
    fn code_assist_variant_without_a_catalog_row_is_refused_but_no_variant_is_admitted() {
        for (model, variant) in [
            ("google/antigravity-gemini-3.8-flash-low", "high"),
            ("google/antigravity-gemini-3.8-flash-medium", "low"),
            ("google/antigravity-gpt-oss-120b-medium", "high"),
            ("google/antigravity-gemini-3.8-flash-medium", "max"),
            ("google/antigravity-gemini-4-argon-high", "low"),
            ("google/antigravity-gemini-3.8-flash-low", "invented-level"),
        ] {
            install();
            let catalog = broca_catalog::catalog().expect("catalog installed");
            let (provider, id) = model.split_once('/').unwrap();
            assert!(
                catalog.model_for_serving(provider, id).is_none(),
                "{model} must have no catalog row for this case"
            );
            let plain = resolved(model, AuthMethod::Antigravity, None);
            assert!(plain.variant.is_none());
            assert_eq!(body(&plain)["model"], json!(plain.wire_model_id));
            let error =
                resolve_with_typed(model, AuthMethod::Antigravity, Some(variant), None, None)
                    .unwrap_err();
            let broca_core::RunError::VariantUnsupported(refusal) =
                broca_core::RunError::from(error)
            else {
                panic!("missing catalog row must be variant_unsupported");
            };
            assert_eq!(refusal.model, model);
            assert_eq!(refusal.variant, variant);
            assert!(refusal.defined.is_empty());
            assert_eq!(
                refusal.reason,
                format!(
                    "model '{model}' cannot use variant '{variant}': the catalog has no \
                         row for this model, so its variants cannot be known"
                )
            );
        }
        // The same kind of undefined variant on another family is still refused.
        let error = refusal("anthropic/claude-opus-4-6", AuthMethod::Oauth, "xhigh");
        assert!(error.contains("does not define variant 'xhigh'"), "{error}");
    }

    /// The Code Assist request for `model` with `variant`, serialized. The renderer emits
    /// canonical (sorted-key) JSON, so equal values are equal bytes.
    fn code_assist_bytes(model: &str, variant: Option<&str>) -> Vec<u8> {
        serde_json::to_vec(&body(&resolved(model, AuthMethod::Antigravity, variant))).unwrap()
    }

    /// On Code Assist a variant is rendered as the backend tier of its name. The row for
    /// `antigravity-gemini-3.8-flash` is fusiform's served alias row (gemini-3.8-flash's facts,
    /// levels [low, medium, high]); `high` sends the `gemini-3.8-flash-high` backend id with that
    /// tier's thinking settings, byte for byte the request the tier id itself produces.
    #[test]
    fn code_assist_variant_renders_the_backend_tier_it_names() {
        for (variant, tier, thinking) in [
            (
                "high",
                "gemini-3.8-flash-high",
                json!({"includeThoughts": true, "thinkingBudget": -1}),
            ),
            (
                "low",
                "gemini-3.8-flash-low",
                json!({"includeThoughts": true, "thinkingBudget": 1000}),
            ),
        ] {
            let frozen = resolved(
                "google/antigravity-gemini-3.8-flash",
                AuthMethod::Antigravity,
                Some(variant),
            );
            assert_eq!(frozen.wire_model_id, tier);
            assert_eq!(
                frozen.variant.as_ref().map(|v| v.name.as_str()),
                Some(variant),
                "the variant is frozen, so a resume renders the same tier"
            );
            let rendered = body(&frozen);
            assert_eq!(rendered["model"], json!(tier));
            assert_eq!(
                rendered["request"]["generationConfig"]["thinkingConfig"],
                thinking
            );
            // `antigravity-gemini-3.8-flash-<tier>` has no catalog row; sent without a variant
            // it is exactly the tier.
            assert_eq!(
                code_assist_bytes("google/antigravity-gemini-3.8-flash", Some(variant)),
                code_assist_bytes(&format!("google/antigravity-{tier}"), None),
                "{variant}"
            );
        }
        // A tier spelled in the id is replaced by the variant's: this case used to be tolerated
        // unapplied as a conflict with the id's fixed tier.
        let moved = resolved(
            "google/gemini-3.8-flash-high",
            AuthMethod::Antigravity,
            Some("low"),
        );
        assert_eq!(moved.wire_model_id, "gemini-3.8-flash-low");
        assert_eq!(
            body(&moved)["request"]["generationConfig"]["thinkingConfig"],
            json!({"includeThoughts": true, "thinkingBudget": 1000})
        );
    }

    /// No variant on an aliased Code Assist id: the bare id still renders its default tier
    /// (gemini-3.8-flash-medium) and nothing is frozen, byte-identical to the default-tier id.
    #[test]
    fn code_assist_without_a_variant_renders_the_default_tier_unchanged() {
        let frozen = resolved(
            "google/antigravity-gemini-3.8-flash",
            AuthMethod::Antigravity,
            None,
        );
        assert!(frozen.variant.is_none());
        assert!(serde_json::to_value(&frozen)
            .unwrap()
            .get("variant")
            .is_none());
        assert_eq!(frozen.wire_model_id, "gemini-3.8-flash-medium");
        let rendered = body(&frozen);
        assert_eq!(
            rendered["request"]["generationConfig"]["thinkingConfig"],
            json!({"includeThoughts": true, "thinkingBudget": 4000})
        );
        assert_eq!(
            code_assist_bytes("google/antigravity-gemini-3.8-flash", None),
            code_assist_bytes("google/antigravity-gemini-3.8-flash-medium", None)
        );
    }

    #[test]
    fn code_assist_argon_resolves_default_and_variants_to_native_tiers() {
        let model = "google/antigravity-gemini-4-argon";
        let plain = resolved(model, AuthMethod::Antigravity, None);
        assert_eq!(plain.wire_model_id, "gemini-4-argon-medium");
        assert!(plain.variant.is_none());
        for (name, wire_id, level) in [
            ("low", "gemini-4-argon-low", "LOW"),
            ("medium", "gemini-4-argon-medium", "MEDIUM"),
            ("high", "gemini-4-argon-high", "HIGH"),
        ] {
            let frozen = resolved(model, AuthMethod::Antigravity, Some(name));
            assert_eq!(frozen.wire_model_id, wire_id);
            assert_eq!(frozen.variant.as_ref().unwrap().name, name);
            let rendered = body(&frozen);
            assert_eq!(rendered["model"], json!(wire_id));
            assert_eq!(
                rendered["request"]["generationConfig"]["thinkingConfig"],
                json!({"includeThoughts": true, "thinkingLevel": level})
            );
            let resumed = resolve_with(
                model,
                AuthMethod::Antigravity,
                None,
                None,
                frozen.variant.as_ref(),
            )
            .expect("Argon resumes its frozen tier");
            assert_eq!(resumed, frozen);
        }
        let error = resolve_with_typed(model, AuthMethod::Antigravity, Some("xhigh"), None, None)
            .unwrap_err();
        let broca_core::RunError::VariantUnsupported(refusal) = broca_core::RunError::from(error)
        else {
            panic!("unknown Argon variant must be variant_unsupported");
        };
        assert_eq!(refusal.model, model);
        assert_eq!(refusal.variant, "xhigh");
        assert_eq!(refusal.defined, ["low", "medium", "high"]);
        assert!(refusal
            .reason
            .ends_with("its variants are [low, medium, high]"));
    }

    #[test]
    fn unsupported_variants_retain_structured_details_including_code_assist_tiers() {
        for (model, method, variant, defined) in [
            (
                "anthropic/claude-opus-4-6",
                AuthMethod::ApiKey,
                "max",
                vec!["none", "low", "medium", "high"],
            ),
            (
                "google/antigravity-gemini-3.5-flash",
                AuthMethod::Antigravity,
                "minimal",
                vec!["minimal", "low", "medium", "high"],
            ),
            (
                "google/antigravity-gemini-3.8-flash",
                AuthMethod::Antigravity,
                "xhigh",
                vec!["low", "medium", "high"],
            ),
        ] {
            let error = resolve_with_typed(model, method, Some(variant), None, None).unwrap_err();
            let broca_core::RunError::VariantUnsupported(refusal) =
                broca_core::RunError::from(error)
            else {
                panic!("unsupported variant was flattened");
            };
            assert_eq!(refusal.model, model);
            assert_eq!(refusal.variant, variant);
            assert_eq!(refusal.defined, defined);
        }
        let refusal = broca_provider::variant::resolve_variant(
            WireFamilyId::AnthropicMessages,
            "anthropic/no-options",
            "no-options",
            &broca_provider::spec::ReasoningOptions::NoOptions,
            None,
            "high",
        )
        .unwrap_err();
        assert_eq!(refusal.model, "anthropic/no-options");
        assert_eq!(refusal.variant, "high");
        assert!(refusal.defined.is_empty());
    }

    /// A level the row defines but the route has no tier for is refused at admission, and the
    /// refusal names the tiers the route has and says the limit is the route's, not the
    /// catalog's. The test row for antigravity-gemini-3.5-flash defines [minimal, low, medium,
    /// high], while Code Assist has only low and high tiers for it.
    #[test]
    fn code_assist_level_without_a_tier_is_refused_naming_the_tiers() {
        let error = refusal(
            "google/antigravity-gemini-3.5-flash",
            AuthMethod::Antigravity,
            "minimal",
        );
        assert_eq!(
            error,
            "model 'google/antigravity-gemini-3.5-flash' defines variant 'minimal', but the \
             Google Code Assist route cannot render it: this route sends a reasoning level as a \
             backend tier, and its tiers for 'gemini-3.5-flash' are [low, high]. This is \
             a limit of the route, not of the catalog; request one of those tiers or send no \
             variant"
        );
        // The same row's levels that do have tiers render them.
        let high = resolved(
            "google/antigravity-gemini-3.5-flash",
            AuthMethod::Antigravity,
            Some("high"),
        );
        assert_eq!(high.wire_model_id, "gemini-3-flash-agent");
    }

    /// A name the row does not define is refused on Code Assist as on every family.
    #[test]
    fn code_assist_variant_the_row_does_not_define_is_refused() {
        let error = refusal(
            "google/antigravity-gemini-3.8-flash",
            AuthMethod::Antigravity,
            "xhigh",
        );
        assert_eq!(
            error,
            "model 'google/antigravity-gemini-3.8-flash' does not define variant 'xhigh'; its \
             variants are [low, medium, high]"
        );
    }

    /// A catalog-defined effort must still be refused if Code Assist cannot render it as a tier.
    #[test]
    fn code_assist_variant_on_a_model_without_tiers_is_refused() {
        let model = "google/antigravity-claude-opus-4-6-thinking";
        let plain = resolved(model, AuthMethod::Antigravity, None);
        assert_eq!(plain.wire_model_id, "claude-opus-4-6-thinking");
        assert!(plain.variant.is_none());
        let error = resolve_with_typed(model, AuthMethod::Antigravity, Some("high"), None, None)
            .unwrap_err();
        let broca_core::RunError::VariantUnsupported(refusal) = broca_core::RunError::from(error)
        else {
            panic!("a route without tiers must be variant_unsupported");
        };
        assert_eq!(refusal.model, model);
        assert_eq!(refusal.variant, "high");
        assert_eq!(refusal.defined, ["low", "medium", "high", "max"]);
        assert_eq!(
            refusal.reason,
            format!(
                "model '{model}' cannot use variant 'high': the Google Code Assist route \
                     sends a reasoning level as a backend tier id and has no tiers for \
                     'claude-opus-4-6-thinking'"
            )
        );
        let error = self::refusal(model, AuthMethod::Antigravity, "xhigh");
        assert!(error.contains("does not define variant 'xhigh'"), "{error}");
    }

    #[test]
    fn retired_code_assist_route_is_refused_before_variant_validation() {
        let model = "google/antigravity-gemini-3.6-flash";
        install();
        let catalog = broca_catalog::catalog().expect("catalog installed");
        assert!(
            catalog
                .model_for_serving("google", "antigravity-gemini-3.6-flash")
                .is_some(),
            "the fixture deliberately retains the withdrawn route's row"
        );

        let error = resolve_with_typed(model, AuthMethod::Antigravity, Some("minimal"), None, None)
            .unwrap_err();
        assert!(matches!(
            error,
            broca_catalog::live::ResolutionError::Config(_)
        ));
        assert_eq!(
            error.to_string(),
            "model 'google/antigravity-gemini-3.6-flash' is unservable: Google withdrew this \
             Antigravity route; use 'google/antigravity-gemini-3.8-flash' (medium) instead"
        );
    }

    /// A `null` inside `values` never becomes a variant, the toggle adds nothing on openai_chat,
    /// the budget is ignored because an effort entry exists, and an unknown entry type
    /// contributes nothing.
    #[test]
    fn null_values_and_unknown_entry_types_define_no_variant() {
        let error = refusal("sarvam/sarvam-30b", AuthMethod::ApiKey, "null");
        assert!(
            error.ends_with("its variants are [low, medium, high]"),
            "{error}"
        );
        let low = variant_body("sarvam/sarvam-30b", AuthMethod::ApiKey, "low");
        assert_eq!(low["reasoning_effort"], json!("low"));
    }

    /// No variant: nothing is frozen and nothing is rendered, so the bytes are those of a run
    /// before variants existed.
    #[test]
    fn an_absent_variant_freezes_and_renders_nothing() {
        for (model, method) in [
            ("anthropic/claude-opus-4-6", AuthMethod::Oauth),
            ("openai/gpt-5.6-luna", AuthMethod::Chatgpt),
            ("anyapi/anthropic/claude-opus-4-6", AuthMethod::ApiKey),
            ("google/gemini-3.5-flash", AuthMethod::ApiKey),
        ] {
            let frozen = resolved(model, method, None);
            assert!(frozen.variant.is_none());
            let blob = serde_json::to_value(&frozen).unwrap();
            assert!(blob.get("variant").is_none(), "{model}");
            let body = body(&frozen);
            assert!(body.get("output_config").is_none(), "{model}");
            assert!(body.get("reasoning_effort").is_none(), "{model}");
        }
        // Anthropic OAuth keeps its fixed default thinking request.
        let oauth = body(&resolved(
            "anthropic/claude-opus-4-6",
            AuthMethod::Oauth,
            None,
        ));
        assert_eq!(
            oauth["thinking"],
            json!({"type": "enabled", "budget_tokens": 1024})
        );
    }

    /// A resume applies the variant its episode froze, as recorded, without consulting the
    /// catalog: here the catalog defines no variant at all for the model, as it would if the
    /// options had been withdrawn since admission, and the resumed render still carries it.
    #[test]
    fn a_resume_renders_from_the_frozen_variant_not_the_catalog() {
        let admitted = resolved(
            "anyapi/anthropic/claude-opus-4-6",
            AuthMethod::ApiKey,
            Some("high"),
        );
        let frozen_variant = admitted.variant.clone().expect("the variant is frozen");
        // Same model: re-resolving from the frozen variant reproduces the admitted config.
        let resumed = resolve_with(
            "anyapi/anthropic/claude-opus-4-6",
            AuthMethod::ApiKey,
            None,
            None,
            Some(&frozen_variant),
        )
        .expect("resume resolves");
        assert_eq!(resumed, admitted);
        // A model whose catalog row now defines nothing still renders the frozen variant.
        let after_catalog_change = resolve_with(
            "synthetic/unknown-reasoning",
            AuthMethod::ApiKey,
            None,
            None,
            Some(&frozen_variant),
        )
        .expect("a resume never re-checks the catalog");
        assert_eq!(
            body(&after_catalog_change)["reasoning_effort"],
            json!("high")
        );

        // On Code Assist the frozen name re-selects the same backend tier.
        let admitted = resolved(
            "google/antigravity-gemini-3.8-flash",
            AuthMethod::Antigravity,
            Some("high"),
        );
        let resumed = resolve_with(
            "google/antigravity-gemini-3.8-flash",
            AuthMethod::Antigravity,
            None,
            None,
            admitted.variant.as_ref(),
        )
        .expect("resume resolves");
        assert_eq!(resumed, admitted);
        assert_eq!(resumed.wire_model_id, "gemini-3.8-flash-high");
    }

    /// Resolve an Anthropic OAuth episode the way a resume does, restoring a recorded
    /// thinking-echo scope and optionally a recorded variant.
    fn resume_anthropic_oauth(
        scope: Option<broca_catalog::live::ThinkingEchoScope>,
        variant: Option<&FrozenVariant>,
    ) -> serde_json::Value {
        resume_anthropic_oauth_with(scope, None, variant)
    }

    /// Resolve an Anthropic OAuth episode the way a resume does, restoring a recorded
    /// thinking-echo scope, a recorded cross-model thinking rule and a recorded variant, each
    /// only when given.
    fn resume_anthropic_oauth_with(
        scope: Option<broca_catalog::live::ThinkingEchoScope>,
        cross_model: Option<broca_catalog::live::CrossModelThinking>,
        variant: Option<&FrozenVariant>,
    ) -> serde_json::Value {
        install();
        let options = ResolveOptions {
            auth_method: Some(AuthMethod::Oauth),
            ..ResolveOptions::default()
        };
        let resolved = broca_catalog::live::resolve_catalog_episode_with_client(
            "anthropic/claude-sonnet-4-5",
            None,
            &AuthMode::Secret("test-key".into()),
            &options,
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig {
                variant,
                thinking_echo_scope: scope,
                thinking_cross_model: cross_model,
                ..EpisodeRenderConfig::default()
            },
        )
        .expect("anthropic resolves");
        resolved.provider.render_config_blob()
    }

    /// A fresh Anthropic episode freezes the tool-loop echo scope. An episode recorded before
    /// the scope existed has no `echo_scope` key; restoring its scope on resume reproduces that
    /// recorded blob exactly (so the drift guard passes and the old bytes render), with or
    /// without a frozen variant. Without the restore the blob differs, which the guard refuses.
    #[test]
    fn a_resume_restores_the_recorded_thinking_echo_scope() {
        use broca_catalog::live::ThinkingEchoScope;

        let fresh = resume_anthropic_oauth(None, None);
        assert_eq!(
            fresh["reasoning"]["echo_scope"],
            json!("all_assistant_messages")
        );

        let mut recorded_legacy = fresh.clone();
        recorded_legacy["reasoning"]
            .as_object_mut()
            .unwrap()
            .remove("echo_scope");
        let recorded_scope = serde_json::from_value::<FrozenRenderConfig>(recorded_legacy.clone())
            .map(|frozen| match frozen.reasoning {
                broca_provider::policy::ReasoningPolicy::AnthropicInterleaved {
                    echo_scope,
                    ..
                } => echo_scope,
                other => panic!("expected interleaved thinking, got {other:?}"),
            })
            .unwrap();
        assert_eq!(recorded_scope, ThinkingEchoScope::LastAssistantMessage);

        assert_eq!(
            resume_anthropic_oauth(Some(recorded_scope), None),
            recorded_legacy
        );
        assert_ne!(resume_anthropic_oauth(None, None), recorded_legacy);

        let admitted = resolved(
            "anthropic/claude-sonnet-4-5",
            AuthMethod::Oauth,
            Some("high"),
        );
        let variant = admitted.variant.clone().expect("the variant is frozen");
        let with_variant = resume_anthropic_oauth(Some(recorded_scope), Some(&variant));
        assert!(with_variant["reasoning"].get("echo_scope").is_none());
        assert!(with_variant.get("variant").is_some());
    }

    /// A fresh Anthropic episode freezes the rule that echoes thinking across Anthropic's own
    /// models. An episode recorded before the rule existed has neither `cross_model` nor (if it
    /// is older still) `echo_scope`; restoring both on resume reproduces that recorded blob
    /// exactly, with or without a frozen variant, and without the restore the blob differs,
    /// which the drift guard refuses.
    #[test]
    fn a_resume_restores_the_recorded_cross_model_thinking_rule() {
        use broca_catalog::live::{CrossModelThinking, ThinkingEchoScope};

        let fresh = resume_anthropic_oauth(None, None);
        assert_eq!(
            fresh["reasoning"]["cross_model"],
            json!("any_anthropic_model")
        );

        let mut recorded_legacy = fresh.clone();
        recorded_legacy["reasoning"]
            .as_object_mut()
            .unwrap()
            .remove("cross_model");
        let recorded_rule = serde_json::from_value::<FrozenRenderConfig>(recorded_legacy.clone())
            .map(|frozen| match frozen.reasoning {
                broca_provider::policy::ReasoningPolicy::AnthropicInterleaved {
                    cross_model,
                    ..
                } => cross_model,
                other => panic!("expected interleaved thinking, got {other:?}"),
            })
            .unwrap();
        assert_eq!(recorded_rule, CrossModelThinking::SameModelOnly);

        assert_eq!(
            resume_anthropic_oauth_with(None, Some(recorded_rule), None),
            recorded_legacy
        );
        assert_ne!(resume_anthropic_oauth(None, None), recorded_legacy);

        // The oldest blobs lack both keys; both restores together reproduce them.
        let mut recorded_oldest = recorded_legacy.clone();
        recorded_oldest["reasoning"]
            .as_object_mut()
            .unwrap()
            .remove("echo_scope");
        assert_eq!(
            resume_anthropic_oauth_with(
                Some(ThinkingEchoScope::LastAssistantMessage),
                Some(recorded_rule),
                None
            ),
            recorded_oldest
        );

        let admitted = resolved(
            "anthropic/claude-sonnet-4-5",
            AuthMethod::Oauth,
            Some("high"),
        );
        let variant = admitted.variant.clone().expect("the variant is frozen");
        let with_variant = resume_anthropic_oauth_with(None, Some(recorded_rule), Some(&variant));
        assert!(with_variant["reasoning"].get("cross_model").is_none());
        assert!(with_variant.get("variant").is_some());
    }

    /// Resolve an Anthropic episode with `method`, offering `fresh` as a new episode's persona
    /// and restoring `episode` as a resume would. Returns the frozen blob and step-one bytes.
    fn resolve_anthropic_persona(
        method: AuthMethod,
        fresh: Option<&broca_catalog::live::ClaudeCodePersona>,
        episode: broca_catalog::live::EpisodePersona<'_>,
    ) -> (Value, String) {
        install();
        let options = ResolveOptions {
            auth_method: Some(method),
            claude_code_persona: fresh,
            ..ResolveOptions::default()
        };
        let resolved = broca_catalog::live::resolve_catalog_episode_with_client(
            "anthropic/claude-sonnet-4-5",
            None,
            &AuthMode::Secret("test-key".into()),
            &options,
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig {
                claude_code_persona: episode,
                ..EpisodeRenderConfig::default()
            },
        )
        .expect("anthropic resolves");
        let opts = CallOptions::new(vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: "hi".into() }],
        )]);
        let bytes = String::from_utf8(resolved.provider.render_request(&opts)).unwrap();
        (resolved.provider.render_config_blob(), bytes)
    }

    /// A fresh OAuth episode freezes the persona it is offered and renders the billing block. A
    /// resume restores what its episode recorded: an episode recorded without a persona keeps
    /// rendering without one even when a persona is on offer, and a recorded persona comes back
    /// unchanged. An api-key episode never takes one.
    #[test]
    fn the_persona_is_frozen_for_fresh_oauth_episodes_and_restored_as_recorded() {
        use broca_catalog::live::{ClaudeCodePersona, EpisodePersona};
        let persona = ClaudeCodePersona {
            version: "2.1.280".into(),
            device_id: "0".repeat(64),
            account_uuid: Some("00000000-0000-4000-8000-000000000000".into()),
            session_id: "11111111-1111-4111-8111-111111111111".into(),
            prompt_id: Some("33333333-3333-4333-8333-333333333333".into()),
            cc_version_suffix: None,
            prompt_ordinal: None,
        };
        let (fresh, fresh_bytes) =
            resolve_anthropic_persona(AuthMethod::Oauth, Some(&persona), EpisodePersona::Fresh);
        assert_eq!(
            fresh["claude_code_persona"],
            serde_json::to_value(&persona).unwrap()
        );
        assert!(fresh_bytes.contains("x-anthropic-billing-header: cc_version=2.1.280."));

        let (recorded_without, old_bytes) = resolve_anthropic_persona(
            AuthMethod::Oauth,
            Some(&persona),
            EpisodePersona::Recorded(None),
        );
        let mut expected_old = fresh.clone();
        expected_old
            .as_object_mut()
            .unwrap()
            .remove("claude_code_persona");
        assert_eq!(recorded_without, expected_old);
        assert!(!old_bytes.contains("x-anthropic-billing-header"));
        let (no_offer, no_offer_bytes) =
            resolve_anthropic_persona(AuthMethod::Oauth, None, EpisodePersona::Fresh);
        assert_eq!(no_offer, expected_old);
        assert_eq!(no_offer_bytes, old_bytes);

        let (recorded_with, recorded_bytes) = resolve_anthropic_persona(
            AuthMethod::Oauth,
            None,
            EpisodePersona::Recorded(Some(&persona)),
        );
        assert_eq!(recorded_with, fresh);
        assert_eq!(recorded_bytes, fresh_bytes);

        let (api_key, _) =
            resolve_anthropic_persona(AuthMethod::ApiKey, Some(&persona), EpisodePersona::Fresh);
        assert!(api_key.get("claude_code_persona").is_none());
    }
}
