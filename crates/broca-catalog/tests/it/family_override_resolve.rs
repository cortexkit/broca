//! Proof that an AUTH-METHOD family override resolves end-to-end through `resolve_provider`'s
//! mode 2b: a `<provider>/<model>` id + a `ResolveOptions.family_override` produces a runnable
//! provider for the OVERRIDE family (its endpoint/auth/quirks), built from the override
//! family's own defaults — NOT the catalog provider's family. This is what makes
//! `antigravity:google` render the Code-Assist envelope and `chatgpt:openai` the codex
//! `/responses`, driven purely by the resolved method (no catalog overlay).
//!
//! No network: it inspects the resolved provider's RENDERED bytes (the boxed provider exposes
//! `render_request`), so it needs no live credential — a dummy token suffices.
// A file-level `cfg` DELETES this file when the feature is off, and a deleted file reports
// "running 0 tests ... test result: ok" -- indistinguishable at a glance from a suite that
// ran and passed, and impossible to instrument because no code survives to carry a notice.
// Gating a module instead leaves this shell compiled, so the absence can announce itself.
#[cfg(not(all(feature = "bundled-catalog", feature = "live")))]
#[test]
fn this_suite_needs_its_feature_enabled() {
    eprintln!(
        "SKIP family_override_resolve: compiled without its feature, so none of its cases exist. \
         This reports `ok` -- re-run with `-F broca-catalog/live` to execute them."
    );
}

#[cfg(all(feature = "bundled-catalog", feature = "live"))]
use crate::common;

#[cfg(all(feature = "bundled-catalog", feature = "live"))]
mod gated {

    use super::common;
    use broca_catalog::live::{
        resolve_chatgpt_pause_provider_with_client, resolve_provider, resolve_provider_with_client,
        AuthMethod, AuthMode, CacheClass, CachePolicy, CacheTtl, MarkerSyntax, ProviderResolution,
        ResolveOptions, WireFamilyId,
    };
    use broca_protocol::{
        content::{ContentBlock, Message, Role},
        request::CallOptions,
        StepContext,
    };

    /// `resolve_provider().expect(..)` can't use `.expect` (ProviderResolution holds a
    /// `Box<dyn Provider>`, no Debug), so unwrap-or-panic by hand.
    fn ok(
        r: Result<ProviderResolution, broca_catalog::live::ResolutionError>,
        what: &str,
    ) -> ProviderResolution {
        match r {
            Ok(v) => v,
            Err(e) => panic!("{what}: {e}"),
        }
    }

    // A JWT (alg:none) whose payload carries chatgpt_account_id — so the chatgpt override's
    // fail-loud account-id derivation succeeds with a dummy token (no live cred needed).
    const CHATGPT_JWT: &str =
        "eyJhbGciOiJub25lIn0.eyJjaGF0Z3B0X2FjY291bnRfaWQiOiAiYWNjdC10ZXN0In0.sig";

    fn opts() -> CallOptions {
        let mut o = CallOptions::new(vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "ping".into(),
            }],
        }]);
        // The antigravity family requires step identity for its deterministic requestId.
        o.step_context = Some(StepContext {
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: "run-ovr".into(),
            step_index: 1,
        });
        o
    }

    #[test]
    fn catalog_resolution_surfaces_the_model_context_limit() {
        common::install_snapshot();
        // A catalog-resolved model carries its context-window limit (the pressure ground truth
        // a prompt-reduction transform needs). gemini-2.5-flash has a large published context.
        let resolved = ok(
            resolve_provider(
                "google/gemini-2.5-flash",
                None,
                2048,
                &AuthMode::Bearer("dummy-token".into()),
                &ResolveOptions::default(),
            ),
            "catalog model resolves",
        );
        let limit = resolved
            .context_limit
            .expect("a catalog model surfaces its context limit");
        assert!(
            limit >= 100_000,
            "gemini-2.5-flash has a large context window, got {limit}"
        );
    }

    #[test]
    fn family_override_still_surfaces_the_model_context_limit() {
        common::install_snapshot();
        // An auth method that overrides the wire family changes the endpoint, auth shaping, and
        // quirks -- none of which alter how much context the MODEL accepts. A consumer reading this
        // as a capacity cannot distinguish an unknown limit from a full one, so dropping a limit the
        // catalog knows would understate a large window as an absent one.
        // Resolved through the pause-only entry point, which takes the same override branch with
        // auth construction skipped. The real path demands a ChatGPT subscription token carrying an
        // account claim, which no dummy credential can satisfy; the limit is read from the catalog
        // before any of that, so this reaches the code under test without a live credential.
        let opts = ResolveOptions {
            family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
            auth_method: Some(AuthMethod::Chatgpt),
            ..Default::default()
        };
        let resolved = ok(
            resolve_chatgpt_pause_provider_with_client(
                "openai/gpt-5.4-mini",
                Some(2048),
                None,
                &opts,
                broca_provider::build_provider_http_client(),
            ),
            "family-override model resolves",
        );
        assert_eq!(
            resolved.family_id,
            WireFamilyId::OpenAiResponsesChatgpt,
            "the override family is in effect, so this exercises the override path"
        );
        let limit = resolved
            .context_limit
            .expect("the override path still surfaces the catalog model's context limit");
        assert!(
            limit >= 100_000,
            "gpt-5.4-mini has a large context window, got {limit}"
        );
    }

    #[test]
    fn base_url_override_has_no_context_limit() {
        // The base_url override endpoint is opaque (no catalog model spec), so there is no
        // context limit to surface — the transform, if wired, falls back to its persisted value.
        let resolved = ok(
            resolve_provider(
                "some-model",
                Some("http://localhost:9999/v1"),
                2048,
                &AuthMode::None,
                &ResolveOptions::default(),
            ),
            "base_url override resolves",
        );
        assert_eq!(
            resolved.context_limit, None,
            "an opaque override endpoint surfaces no context limit"
        );
    }

    #[test]
    fn antigravity_override_resolves_codeassist_family_and_freezes_project() {
        common::install_snapshot();
        let resolved = ok(
            resolve_provider(
                "google/gemini-2.5-flash",
                None,
                2048,
                &AuthMode::Bearer("dummy-token".into()),
                &ResolveOptions {
                    claude_code_persona: None,
                    auth_method: None,
                    family_override: Some(WireFamilyId::GoogleCodeAssist),
                    project_id: Some("proj-override"),
                    session_id: None,
                    account_id: None,
                    cache_policy: None,
                    prompt_cache_key: None,
                    variant: None,
                    service_tier: None,
                    secret_refresher: None,
                    structured_output: None,
                    frozen_account_ref: None,
                    clock: None,
                },
            ),
            "antigravity override resolves",
        );

        assert_eq!(resolved.family_id, WireFamilyId::GoogleCodeAssist);
        // The rendered bytes are the cloudcode-pa envelope with the FROZEN project + the model.
        let bytes = resolved.provider.render_request(&opts());
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["project"], serde_json::json!("proj-override"));
        assert_eq!(v["userAgent"], serde_json::json!("antigravity"));
        assert_eq!(v["model"], serde_json::json!("gemini-2.5-flash"));
        // The inner gemini body is nested under `request`.
        assert!(v.get("request").and_then(|r| r.get("contents")).is_some());
    }

    /// Antigravity's caller-facing prefix and untiered routes resolve before freeze, so the wire
    /// carries the backend tier id while unrelated and already-tiered ids remain untouched.
    #[test]
    fn antigravity_override_resolves_the_frozen_wire_tier() {
        common::install_snapshot();
        let resolved = ok(
            resolve_provider(
                "google/antigravity-gemini-3.5-flash",
                None,
                2048,
                &AuthMode::Bearer("dummy-token".into()),
                &ResolveOptions {
                    claude_code_persona: None,
                    auth_method: None,
                    family_override: Some(WireFamilyId::GoogleCodeAssist),
                    project_id: Some("proj-override"),
                    session_id: None,
                    account_id: None,
                    cache_policy: None,
                    prompt_cache_key: None,
                    variant: None,
                    service_tier: None,
                    secret_refresher: None,
                    structured_output: None,
                    frozen_account_ref: None,
                    clock: None,
                },
            ),
            "aliased antigravity model resolves",
        );

        assert_eq!(resolved.wire_model, "gemini-3.5-flash-low");
        let bytes = resolved.provider.render_request(&opts());
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["model"], serde_json::json!("gemini-3.5-flash-low"));
        assert_eq!(
            v["request"]["generationConfig"]["maxOutputTokens"],
            serde_json::json!(2048),
            "the explicit caller cap must survive Antigravity tier resolution"
        );

        let unchanged = ok(
            resolve_provider(
                "google/antigravity-gemini-2.5-flash",
                None,
                2048,
                &AuthMode::Bearer("dummy-token".into()),
                &ResolveOptions {
                    claude_code_persona: None,
                    auth_method: None,
                    family_override: Some(WireFamilyId::GoogleCodeAssist),
                    project_id: Some("proj-override"),
                    session_id: None,
                    account_id: None,
                    cache_policy: None,
                    prompt_cache_key: None,
                    variant: None,
                    service_tier: None,
                    secret_refresher: None,
                    structured_output: None,
                    frozen_account_ref: None,
                    clock: None,
                },
            ),
            "untiered antigravity 2.5 model resolves",
        );
        assert_eq!(unchanged.wire_model, "gemini-2.5-flash");

        // A ChatGPT-family override must NOT strip anything: the prefix rule is scoped to
        // the codeassist family alone.
        let chatgpt = ok(
            resolve_provider(
                "openai/gpt-5.4-mini",
                None,
                2048,
                &AuthMode::Bearer(CHATGPT_JWT.into()),
                &ResolveOptions {
                    claude_code_persona: None,
                    auth_method: None,
                    family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
                    project_id: None,
                    session_id: Some("ses-x"),
                    account_id: None,
                    cache_policy: None,
                    prompt_cache_key: None,
                    variant: None,
                    service_tier: None,
                    secret_refresher: None,
                    structured_output: None,
                    frozen_account_ref: None,
                    clock: None,
                },
            ),
            "chatgpt override resolves",
        );
        assert_eq!(chatgpt.wire_model, "gpt-5.4-mini");
    }

    #[test]
    fn antigravity_override_uses_the_tier_output_default_when_the_caller_omits_a_cap() {
        common::install_snapshot();
        let resolved = ok(
            resolve_provider_with_client(
                "google/antigravity-gemini-3.5-flash",
                None,
                None,
                &AuthMode::Bearer("dummy-token".into()),
                &ResolveOptions {
                    claude_code_persona: None,
                    auth_method: None,
                    family_override: Some(WireFamilyId::GoogleCodeAssist),
                    project_id: Some("proj-override"),
                    session_id: None,
                    account_id: None,
                    cache_policy: None,
                    prompt_cache_key: None,
                    variant: None,
                    service_tier: None,
                    secret_refresher: None,
                    structured_output: None,
                    frozen_account_ref: None,
                    clock: None,
                },
                broca_provider::build_provider_http_client(),
            ),
            "aliased antigravity model without a caller cap resolves",
        );

        let rendered: serde_json::Value =
            serde_json::from_slice(&resolved.provider.render_request(&opts())).unwrap();
        assert_eq!(
            rendered["request"]["generationConfig"]["maxOutputTokens"],
            serde_json::json!(65_536)
        );
    }

    #[test]
    fn chatgpt_ids_resolve_verbatim_with_no_tier_suffixes() {
        common::install_snapshot();
        let options = ResolveOptions {
            claude_code_persona: None,
            auth_method: None,
            family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
            project_id: None,
            session_id: Some("ses-model-tier"),
            account_id: None,
            cache_policy: None,
            prompt_cache_key: None,
            variant: None,
            service_tier: None,
            secret_refresher: None,
            structured_output: None,
            frozen_account_ref: None,
            clock: None,
        };
        // A ChatGPT id is sent exactly as the catalog lists it: a real catalog `-pro` model is
        // sent as itself, with no effort or service tier attached.
        for canonical in ["openai/gpt-5.6-sol", "openai/gpt-5.5-pro"] {
            let resolved = ok(
                resolve_provider(
                    canonical,
                    None,
                    2048,
                    &AuthMode::Bearer(CHATGPT_JWT.into()),
                    &options,
                ),
                "chatgpt catalog model resolves",
            );

            let expected_wire = canonical.strip_prefix("openai/").unwrap();
            assert_eq!(resolved.wire_model, expected_wire);
            let frozen = resolved.provider.render_config_blob();
            assert_eq!(frozen["wire_model_id"], serde_json::json!(expected_wire));
            assert_eq!(frozen.get("reasoning_effort"), None);
            assert_eq!(frozen.get("service_tier"), None);
        }
        // Ids the catalog does not list are refused as unknown models. The `-pro` monikers here
        // used to resolve to the base model with effort `max`, now `model.variant`; the `-fast`
        // ones to the base model with the priority tier, now `service_tier: "priority"`.
        for canonical in [
            "openai/gpt-5.6-sol-pro",
            "openai/gpt-5.6-terra-pro",
            "openai/gpt-5.6-sol-mega",
            "openai/gpt-5.6-sol-fast",
            "openai/gpt-5.6-sol-pro-fast",
        ] {
            let error = resolve_provider(
                canonical,
                None,
                2048,
                &AuthMode::Bearer(CHATGPT_JWT.into()),
                &options,
            )
            .err()
            .unwrap_or_else(|| panic!("{canonical} must not resolve"));
            assert!(
                error
                    .to_string()
                    .contains(&format!("catalog has no served model '{canonical}'")),
                "{error}"
            );
            assert!(error.to_string().contains("service_tier"), "{error}");
        }
    }

    #[test]
    fn chatgpt_prompt_cache_key_clamps_long_session_ids_to_a_stable_digest() {
        common::install_snapshot();
        let long_session = "alfonso:consult-ct_00000000-0000-4001-98c0-59c9222bd568-fanout-r0-slot0-openai-gpt-5-6-sol-a2";
        assert!(long_session.len() > 64);

        let resolve = |session: &str| {
            ok(
                resolve_provider(
                    "openai/gpt-5.6-sol",
                    None,
                    2048,
                    &AuthMode::Bearer(CHATGPT_JWT.into()),
                    &ResolveOptions {
                        claude_code_persona: None,
                        auth_method: None,
                        family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
                        project_id: None,
                        session_id: Some(session),
                        account_id: None,
                        cache_policy: None,
                        prompt_cache_key: None,
                        variant: None,
                        service_tier: None,
                        secret_refresher: None,
                        structured_output: None,
                        frozen_account_ref: None,
                        clock: None,
                    },
                ),
                "chatgpt session resolves",
            )
        };

        let long_a = resolve(long_session);
        let long_b = resolve(long_session);
        let short = resolve("ses-short");

        let key = |r: &ProviderResolution| {
            r.provider.render_config_blob()["prompt_cache_key"]
                .as_str()
                .unwrap()
                .to_string()
        };

        // Over-long ids clamp to a stable digest: within the limit, deterministic per session.
        let clamped = key(&long_a);
        assert!(clamped.len() <= 64, "clamped key fits the backend limit");
        assert_eq!(clamped, key(&long_b), "same session, same key");
        // Short ids ride byte-identical.
        assert_eq!(key(&short), "ses-short");
    }

    #[test]
    fn caller_cache_scope_replaces_codex_session_scope_in_body_and_header() {
        common::install_snapshot();
        let caller_key = "alfonso-classify-cache-scope-".repeat(4);
        let expected = broca_provider::hash::clamp_prompt_cache_key(&caller_key);
        let resolved = ok(
            resolve_provider(
                "openai/gpt-5.6-sol",
                None,
                2048,
                &AuthMode::Bearer(CHATGPT_JWT.into()),
                &ResolveOptions {
                    claude_code_persona: None,
                    auth_method: None,
                    family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
                    project_id: None,
                    session_id: Some("different-session"),
                    account_id: None,
                    cache_policy: None,
                    prompt_cache_key: Some(&caller_key),
                    variant: None,
                    service_tier: None,
                    secret_refresher: None,
                    structured_output: None,
                    frozen_account_ref: None,
                    clock: None,
                },
            ),
            "caller cache scope resolves for codex",
        );
        let body: serde_json::Value =
            serde_json::from_slice(&resolved.provider.render_request(&opts())).unwrap();
        assert_eq!(body["prompt_cache_key"], expected);
    }

    #[test]
    fn chatgpt_override_resolves_codex_family() {
        common::install_snapshot();
        let resolved = ok(
            resolve_provider(
                "openai/gpt-5.3-codex",
                None,
                2048,
                &AuthMode::Bearer(CHATGPT_JWT.into()),
                &ResolveOptions {
                    claude_code_persona: None,
                    auth_method: None,
                    family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
                    project_id: None,
                    session_id: Some("ses-override"),
                    account_id: None,
                    cache_policy: None,
                    prompt_cache_key: None,
                    variant: None,
                    service_tier: None,
                    secret_refresher: None,
                    structured_output: None,
                    frozen_account_ref: None,
                    clock: None,
                },
            ),
            "chatgpt override resolves (JWT carries the account-id claim)",
        );

        assert_eq!(resolved.family_id, WireFamilyId::OpenAiResponsesChatgpt);
        assert_eq!(resolved.auth_desc, "chatgpt(openai)");
        // The body is the OpenAI-Responses shape with the codex model.
        let bytes = resolved.provider.render_request(&opts());
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["model"], serde_json::json!("gpt-5.3-codex"));
        assert!(
            v.get("input").is_some(),
            "responses-shaped body has `input`"
        );
    }

    #[test]
    fn chatgpt_pause_only_construction_skips_auth_but_ordinary_none_stays_strict() {
        common::install_snapshot();
        let options = ResolveOptions {
            claude_code_persona: None,
            auth_method: None,
            family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
            project_id: None,
            session_id: Some("ses-paused"),
            account_id: None,
            cache_policy: None,
            prompt_cache_key: None,
            variant: None,
            service_tier: None,
            secret_refresher: None,
            structured_output: None,
            frozen_account_ref: None,
            clock: None,
        };
        let paused = ok(
            resolve_chatgpt_pause_provider_with_client(
                "openai/gpt-5.3-codex",
                Some(2048),
                None,
                &options,
                broca_provider::build_provider_http_client(),
            ),
            "pause-only ChatGPT provider resolves without credentials",
        );
        assert_eq!(paused.family_id, WireFamilyId::OpenAiResponsesChatgpt);
        assert_eq!(paused.auth_desc, "chatgpt-pause-only(openai)");

        let error = match resolve_provider(
            "openai/gpt-5.3-codex",
            None,
            2048,
            &AuthMode::None,
            &options,
        ) {
            Ok(_) => panic!("ordinary ChatGPT resolution without credentials must fail"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("no ChatGPT token"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn chatgpt_override_fails_loud_without_account_claim() {
        common::install_snapshot();
        // A token with no chatgpt_account_id claim must FAIL resolution (not silently send a
        // request with an empty account header).
        let err = match resolve_provider(
            "openai/gpt-5.3-codex",
            None,
            2048,
            &AuthMode::Bearer("not-a-codex-token".into()),
            &ResolveOptions {
                claude_code_persona: None,
                auth_method: None,
                family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
                project_id: None,
                session_id: Some("ses-1"),
                account_id: None,
                cache_policy: None,
                prompt_cache_key: None,
                variant: None,
                service_tier: None,
                secret_refresher: None,
                structured_output: None,
                frozen_account_ref: None,
                clock: None,
            },
        ) {
            Ok(_) => panic!("a codex token with no account-id claim must FAIL resolution"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("chatgpt_account_id"),
            "fail-loud names the missing claim; got: {err}"
        );
    }

    #[test]
    fn no_override_uses_the_catalog_family() {
        common::install_snapshot();
        // Without a family override, a catalog provider resolves its OWN family (anthropic →
        // AnthropicMessages) — proving the override is opt-in, not always-on.
        let resolved = ok(
            resolve_provider(
                "anthropic/claude-opus-4-8",
                None,
                2048,
                &AuthMode::Bearer("dummy".into()),
                &ResolveOptions::default(),
            ),
            "catalog anthropic resolves",
        );
        assert_eq!(resolved.family_id, WireFamilyId::AnthropicMessages);
    }

    #[test]
    fn catalog_resolution_folds_marker_cache_policy_into_rendered_bytes() {
        common::install_snapshot();
        let resolved = ok(
            resolve_provider(
                "anthropic/claude-opus-4-8",
                None,
                2048,
                &AuthMode::Bearer("dummy".into()),
                &ResolveOptions {
                    cache_policy: Some(CachePolicy {
                        version: 1,
                        class: CacheClass::Markers {
                            syntax: MarkerSyntax::AnthropicCacheControl,
                            ttl: CacheTtl::OneHour,
                            max_breakpoints: 4,
                            lookback_blocks: 20,
                            tail: None,
                            omit_ttl: false,
                        },
                    }),
                    ..ResolveOptions::default()
                },
            ),
            "catalog anthropic resolves with a cache policy",
        );

        let bytes = resolved.provider.render_request(&CallOptions::new(vec![
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                tags: Default::default(),
                role: Role::System,
                content: vec![ContentBlock::Text {
                    text: "system prefix".into(),
                }],
            },
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                tags: Default::default(),
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "m0 stable prefix".into(),
                }],
            },
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                tags: Default::default(),
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "latest user boundary".into(),
                }],
            },
        ]));
        let rendered = String::from_utf8(bytes).expect("anthropic render is UTF-8 JSON");
        assert!(
            rendered.contains("cache_control"),
            "resolved marker policy must be present in rendered bytes: {rendered}"
        );
        assert!(rendered.contains("1h"));
    }
}
