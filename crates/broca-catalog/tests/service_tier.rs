//! A caller's `service_tier`, checked against what the model's provider family has bytes for
//! and what the catalog prices, then rendered by the family.
//!
//! The catalog is installed from `fixtures/fusiform_service_tier_modes.json`, a fusiform
//! `catalog.get` response. Provenance of each row:
//!
//! - `anyapi/anthropic/claude-opus-4-6` is copied verbatim from fusiform's served fixture
//!   (`crates/fusiform-module/fixtures/served-payloads.json` at fusiform commit 881b1e3,
//!   fusiform-protocol 0.27.0), case "catalog.get — a model priced only in a mode, with no base
//!   rate": fast-mode rates and no base rate, on a gateway provider broca has no tier bytes for.
//! - `openai/gpt-5.6-luna` carries every `rate.*` fact of that fixture's case "catalog.get —
//!   mode rates beside base rates, and a mode with no price" verbatim (fast-mode rates beside
//!   the base rates; `pro` produces nothing). That case was a rate-only request, so the row
//!   gains `existence`, limits and capability flags, which broca requires on a present row.
//! - `anthropic/claude-sonnet-4-5` is that fixture's "catalog.get — one model, current" row
//!   verbatim: base rates, no mode rates.
//! - DERIVED rows, not served by fusiform, each built from a verbatim row above so the wire
//!   shape is fusiform's own:
//!   - `anthropic/claude-opus-4-8`: the anyapi row's facts (minus reasoning options) on the
//!     first-party provider, so a mapped family meets a model priced only in a mode.
//!   - `openai/gpt-5.4-mini`: the luna row with `rate.input.mode.fast` removed, so fast mode
//!     is priced on one side only while both base rates are present.
//!   - `google/gemini-3.5-flash` and `xai/grok-4`: the sonnet row's flags, limits and base
//!     input/output rates; no mode rates (fusiform serves none for Gemini or xAI today).
//!   - `fireworks-ai/accounts/fireworks/models/kimi-k2-instruct`: the luna row's fast-mode
//!     rates renamed to fireworks' mode name `priority`, so a priced tier meets a provider with
//!     no broca mapping.
#[cfg(not(feature = "live"))]
#[test]
fn this_suite_needs_its_feature_enabled() {
    eprintln!(
        "SKIP service_tier: compiled without the live feature, so none of its cases exist. \
         Re-run with `-F broca-catalog/live` to execute them."
    );
}

#[cfg(feature = "live")]
mod gated {
    use std::sync::{Arc, Once};

    use broca_catalog::live::{
        resolve_chatgpt_pause_provider_with_client, AuthMethod, AuthMode, EpisodeRenderConfig,
        FrozenRenderConfig, ResolveOptions, ServiceTier, WireFamilyId,
    };
    use broca_catalog::{CatalogCache, CatalogSource, FusiformCatalogResponse};
    use broca_protocol::request::CallOptions;
    use broca_provider::families::AnthropicMessages;
    use broca_provider::wire::{AuthHandle, WireFamily};
    use serde_json::Value;

    fn install() {
        static INSTALLED: Once = Once::new();
        INSTALLED.call_once(|| {
            let response: FusiformCatalogResponse =
                serde_json::from_str(include_str!("fixtures/fusiform_service_tier_modes.json"))
                    .expect("fixture is a catalog.get response");
            let cache = Arc::new(CatalogCache::empty());
            cache
                .install_for_test(response, CatalogSource::Fusiform)
                .expect("fixture catalog installs");
            broca_catalog::install_global_catalog_cache(cache);
        });
    }

    struct Resolved {
        frozen: FrozenRenderConfig,
        body: Vec<u8>,
    }

    fn resolve_with(
        model: &str,
        method: AuthMethod,
        tier: Option<ServiceTier>,
        restored: Option<ServiceTier>,
    ) -> Result<Resolved, String> {
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
            service_tier: tier,
            ..ResolveOptions::default()
        };
        let resolved = if method == AuthMethod::Chatgpt {
            resolve_chatgpt_pause_provider_with_client(
                model,
                None,
                None,
                &options,
                broca_provider::build_provider_http_client(),
            )
        } else {
            broca_catalog::live::resolve_catalog_episode_with_client(
                model,
                None,
                &AuthMode::Secret("test-key".into()),
                &options,
                broca_provider::build_provider_http_client(),
                EpisodeRenderConfig {
                    service_tier: restored,
                    ..EpisodeRenderConfig::default()
                },
            )
        }
        .map_err(|error| error.to_string())?;
        let frozen = serde_json::from_value(resolved.provider.render_config_blob())
            .expect("resolved provider exposes its frozen config");
        let body = resolved
            .provider
            .render_request(&CallOptions::new(Vec::new()));
        Ok(Resolved { frozen, body })
    }

    fn resolved(model: &str, method: AuthMethod, tier: Option<ServiceTier>) -> Resolved {
        resolve_with(model, method, tier, None)
            .unwrap_or_else(|error| panic!("{model} tier {tier:?} must resolve: {error}"))
    }

    fn refusal(model: &str, method: AuthMethod, tier: ServiceTier) -> String {
        match resolve_with(model, method, Some(tier), None) {
            Ok(_) => panic!("{model} tier '{tier}' must be refused"),
            Err(error) => error,
        }
    }

    fn body_of(resolved: &Resolved) -> Value {
        serde_json::from_slice(&resolved.body).expect("rendered body is JSON")
    }

    fn anthropic_beta(frozen: &FrozenRenderConfig) -> Option<String> {
        AnthropicMessages
            .endpoint(frozen, &AuthHandle::None, Vec::new())
            .headers
            .into_iter()
            .find(|(name, _)| name == "anthropic-beta")
            .map(|(_, value)| value)
    }

    /// A send without a tier renders no tier bytes on any family this build maps, so it keeps
    /// exactly the request it had before the field existed (the family goldens pin the rest).
    #[test]
    fn an_absent_tier_renders_no_tier_bytes_on_any_family() {
        for (model, method) in [
            ("openai/gpt-5.6-luna", AuthMethod::ApiKey),
            ("openai/gpt-5.6-luna", AuthMethod::Chatgpt),
            ("anthropic/claude-opus-4-8", AuthMethod::ApiKey),
            ("google/gemini-3.5-flash", AuthMethod::ApiKey),
            ("xai/grok-4", AuthMethod::ApiKey),
        ] {
            let resolved = resolved(model, method, None);
            let body = body_of(&resolved);
            assert!(body.get("service_tier").is_none(), "{model} {method:?}");
            assert!(body.get("speed").is_none(), "{model} {method:?}");
            assert_eq!(resolved.frozen.service_tier, None);
            assert_eq!(resolved.frozen.speed, None);
            assert_eq!(resolved.frozen.service_tier_request, None);
        }
        let plain = resolved("anthropic/claude-opus-4-8", AuthMethod::ApiKey, None);
        assert!(anthropic_beta(&plain.frozen).is_none_or(|beta| !beta.contains("fast-mode")));
    }

    #[test]
    fn openai_renders_the_tier_name_as_service_tier() {
        let priority = resolved(
            "openai/gpt-5.6-luna",
            AuthMethod::ApiKey,
            Some(ServiceTier::Priority),
        );
        assert_eq!(priority.frozen.family_id, WireFamilyId::OpenAiChat);
        assert_eq!(body_of(&priority)["service_tier"], "priority");
        let default = resolved(
            "openai/gpt-5.6-luna",
            AuthMethod::ApiKey,
            Some(ServiceTier::Default),
        );
        assert_eq!(body_of(&default)["service_tier"], "default");
        // Only the tier field differs from a send without a tier.
        let mut with_tier = body_of(&priority);
        with_tier
            .as_object_mut()
            .unwrap()
            .remove("service_tier")
            .unwrap();
        assert_eq!(
            with_tier,
            body_of(&resolved("openai/gpt-5.6-luna", AuthMethod::ApiKey, None))
        );
    }

    /// On ChatGPT auth, `service_tier: "priority"` renders as the body field of the same name on
    /// the model the caller named, and changes nothing else in the request.
    #[test]
    fn chatgpt_priority_renders_service_tier_priority() {
        let tier = resolved(
            "openai/gpt-5.6-luna",
            AuthMethod::Chatgpt,
            Some(ServiceTier::Priority),
        );
        assert_eq!(tier.frozen.family_id, WireFamilyId::OpenAiResponsesChatgpt);
        assert_eq!(tier.frozen.wire_model_id, "gpt-5.6-luna");
        let mut body = body_of(&tier);
        assert_eq!(body["service_tier"], "priority");
        body.as_object_mut()
            .unwrap()
            .remove("service_tier")
            .unwrap();
        assert_eq!(
            body,
            body_of(&resolved("openai/gpt-5.6-luna", AuthMethod::Chatgpt, None))
        );
    }

    /// Anthropic priority is fast mode: body `speed: "fast"` plus the fast-mode beta, as
    /// documented at https://platform.claude.com/docs/en/build-with-claude/fast-mode. This row is
    /// priced ONLY in the mode (no base rate), and that is enough to offer the tier.
    #[test]
    fn anthropic_priority_is_fast_mode_and_default_sends_nothing() {
        let plain = resolved("anthropic/claude-opus-4-8", AuthMethod::ApiKey, None);
        let fast = resolved(
            "anthropic/claude-opus-4-8",
            AuthMethod::ApiKey,
            Some(ServiceTier::Priority),
        );
        let mut body = body_of(&fast);
        assert_eq!(body["speed"], "fast");
        assert!(body.get("service_tier").is_none());
        body.as_object_mut().unwrap().remove("speed");
        assert_eq!(body, body_of(&plain), "only `speed` is added");
        let beta = anthropic_beta(&fast.frozen).expect("fast mode sends a beta header");
        assert!(
            beta.split(',').any(|beta| beta == "fast-mode-2026-02-01"),
            "{beta}"
        );

        let default = resolved(
            "anthropic/claude-opus-4-8",
            AuthMethod::ApiKey,
            Some(ServiceTier::Default),
        );
        assert_eq!(default.body, plain.body);
        assert_eq!(
            anthropic_beta(&default.frozen),
            anthropic_beta(&plain.frozen)
        );
    }

    #[test]
    fn a_model_priced_only_in_a_mode_offers_that_tier() {
        // The row carries fast-mode rates and no base `rate.input` / `rate.output` at all.
        let mode_only = resolved(
            "anthropic/claude-opus-4-8",
            AuthMethod::ApiKey,
            Some(ServiceTier::Priority),
        );
        assert_eq!(mode_only.frozen.speed.as_deref(), Some("fast"));
    }

    /// Refused because broca has no bytes: the message says so, and lists what IS offered.
    #[test]
    fn a_tier_the_family_has_no_bytes_for_is_refused_naming_the_offered_tiers() {
        let flex = refusal(
            "anthropic/claude-opus-4-8",
            AuthMethod::ApiKey,
            ServiceTier::Flex,
        );
        assert!(
            flex.contains("has no service-tier mapping in broca"),
            "{flex}"
        );
        assert!(
            flex.contains("Tiers offered for this model: \"default\", \"priority\""),
            "{flex}"
        );

        // Priced upstream, but on gateways broca has no verified bytes for.
        for model in [
            "anyapi/anthropic/claude-opus-4-6",
            "fireworks-ai/accounts/fireworks/models/kimi-k2-instruct",
        ] {
            let error = refusal(model, AuthMethod::ApiKey, ServiceTier::Priority);
            assert!(
                error.contains("has no service-tier mapping in broca"),
                "{model}: {error}"
            );
            assert!(
                !error.contains("no rate in the catalog"),
                "{model}: {error}"
            );
            assert!(
                error.contains("Tiers offered for this model: none"),
                "{model}: {error}"
            );
        }
    }

    /// Refused because the catalog prices no matching mode, which is a different reason from a
    /// missing mapping and is reported as one.
    #[test]
    fn a_tier_with_no_catalog_mode_rate_is_refused_naming_the_offered_tiers() {
        for (model, tier) in [
            ("anthropic/claude-sonnet-4-5", ServiceTier::Priority),
            ("openai/gpt-5.6-luna", ServiceTier::Flex),
            ("google/gemini-3.5-flash", ServiceTier::Flex),
            ("google/gemini-3.5-flash", ServiceTier::Priority),
            ("xai/grok-4", ServiceTier::Priority),
        ] {
            let error = refusal(model, AuthMethod::ApiKey, tier);
            assert!(error.contains("no rate in the catalog"), "{model}: {error}");
            assert!(
                !error.contains("no service-tier mapping"),
                "{model}: {error}"
            );
            assert!(
                error.contains("Tiers offered for this model: \"default\""),
                "{model}: {error}"
            );
        }
    }

    /// `openai/gpt-5.4-mini` prices fast mode's OUTPUT and has both base rates, but no fast-mode
    /// INPUT rate. The absent side is unpriced; it is never filled from the base input rate.
    #[test]
    fn a_half_priced_mode_is_not_filled_from_the_base_rate() {
        let error = refusal(
            "openai/gpt-5.4-mini",
            AuthMethod::ApiKey,
            ServiceTier::Priority,
        );
        assert!(error.contains("no rate in the catalog"), "{error}");
    }

    /// Google Code Assist has no verified tier bytes, so no tier is offered there.
    #[test]
    fn code_assist_offers_no_tier() {
        let error = refusal(
            "google/gemini-3.5-flash",
            AuthMethod::Antigravity,
            ServiceTier::Default,
        );
        assert!(
            error.contains("has no service-tier mapping in broca"),
            "{error}"
        );
    }

    /// A resume re-applies the frozen tier from broca's table alone: it renders fast mode on a
    /// row whose catalog prices no fast mode (a rate withdrawn since admission), and ignores the
    /// resuming send's own tier.
    #[test]
    fn a_resume_renders_the_frozen_tier_without_the_catalog_rate() {
        let resumed = resolve_with(
            "anthropic/claude-sonnet-4-5",
            AuthMethod::ApiKey,
            Some(ServiceTier::Flex),
            Some(ServiceTier::Priority),
        )
        .expect("a restored tier is not re-checked against the catalog");
        assert_eq!(body_of(&resumed)["speed"], "fast");
        assert_eq!(
            resumed
                .frozen
                .service_tier_request
                .map(|request| request.tier),
            Some(ServiceTier::Priority)
        );
    }
}
