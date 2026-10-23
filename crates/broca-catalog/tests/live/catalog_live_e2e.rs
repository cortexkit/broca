//! End-to-end proof of the FULL catalog → live path: a provider/model resolved
//! ENTIRELY from the vendored catalog (no hand-built frozen helper) is turned into a
//! running `LiveProvider` and answers a real request.
//!
//! Chain: `Catalog::model(provider, model)` → `resolve_frozen(spec, model, …,
//! renderer_version_for(family))` → `live_provider(frozen, auth)` → `step` →
//! assembled reply. This is what proves the catalog produces a RUNNABLE config, not
//! just a renderable one.
//!
//! Driven against several OpenAI-compatible providers the catalog resolves to
//! OpenAiChat purely from `base_url` (config, not code). Each target is gated on explicit
//! `BROCA_LIVE_TESTS` consent and an `api`-type key in the OpenCode auth file, and skips
//! cleanly (never a silent pass) when either is absent. Secrets are read from disk and never
//! echoed.
// A file-level `cfg` DELETES this file when the feature is off, and a deleted file reports
// "running 0 tests ... test result: ok" -- indistinguishable at a glance from a suite that
// ran and passed, and impossible to instrument because no code survives to carry a notice.
// Gating a module instead leaves this shell compiled, so the absence can announce itself.
#[cfg(not(all(feature = "bundled-catalog", feature = "live")))]
#[test]
fn this_suite_needs_its_feature_enabled() {
    eprintln!(
        "SKIP catalog_live_e2e: compiled without its feature, so none of its cases exist. \
         This reports `ok` -- re-run with `-F broca-catalog/live` to execute them."
    );
}

#[cfg(all(feature = "bundled-catalog", feature = "live"))]
use crate::common;

#[cfg(all(feature = "bundled-catalog", feature = "live"))]
mod gated {

    use super::common;
    use broca_catalog::live::{resolve_provider, AuthMode, ResolveOptions};
    use broca_protocol::{
        content::{ContentBlock, Message, Role},
        provider::ProviderControl,
        request::CallOptions,
        NormalizedEvent, StepId,
    };
    use broca_provider::families::{renderer_version_for, resolve_frozen};
    use broca_provider::live_provider;
    use broca_provider::wire::AuthHandle;

    fn live_calls_opted_in() -> bool {
        std::env::var("BROCA_LIVE_TESTS")
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
    }

    /// A catalog-served OpenAI-compatible target: a provider/model in the vendored
    /// catalog plus the auth-file key that reaches it.
    struct Target {
        provider_id: &'static str,
        model_id: &'static str,
        auth_key: &'static str,
        expect_base_url: &'static str,
    }

    /// Providers the catalog resolves to OpenAiChat via the openai-compatible rule, that
    /// we hold api-type keys for. (deepseek is the original proof; the others broaden the
    /// evidence that the catalog→live path generalizes beyond one provider.)
    const TARGETS: &[Target] = &[
        Target {
            provider_id: "deepseek",
            model_id: "deepseek-v4-flash",
            auth_key: "deepseek",
            expect_base_url: "https://api.deepseek.com",
        },
        // Note: these carry a TRAILING SLASH in the catalog (e.g. `.../v1/`). The
        // OpenAiChat endpoint trims it before appending `/chat/completions`, so the live
        // call still hits a single-slash URL — this is the real catalog value, verbatim.
        Target {
            provider_id: "fireworks-ai",
            model_id: "accounts/fireworks/models/deepseek-v4-flash",
            auth_key: "fireworks-ai",
            expect_base_url: "https://api.fireworks.ai/inference/v1/",
        },
        Target {
            provider_id: "inception",
            model_id: "mercury-2",
            auth_key: "inception",
            expect_base_url: "https://api.inceptionlabs.ai/v1/",
        },
    ];

    /// Read an `api`-type key from the OpenCode auth file (never logged).
    fn read_api_key(provider: &str) -> Option<String> {
        if !live_calls_opted_in() {
            return None;
        }
        let home = std::env::var("HOME").ok()?;
        let path = std::path::PathBuf::from(home).join(".local/share/opencode/auth.json");
        let bytes = std::fs::read(&path).ok()?;
        let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
        let entry = v.get(provider)?;
        if entry.get("type").and_then(|t| t.as_str()) != Some("api") {
            return None;
        }
        entry
            .get("key")
            .and_then(|k| k.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    }

    /// The outcome of one catalog-resolved live target.
    enum TargetOutcome {
        /// The provider answered — the assembled reply.
        Ran(String),
        /// Explicit live-call consent or an api-type key is absent.
        SkippedNoKey,
        /// A key is present but the account can't be exercised right now (billing suspended,
        /// payment required, quota exhausted) — not a code defect, so not a failure.
        SkippedAccountState(String),
    }

    /// Run one catalog-resolved target live. Panics only on a REAL failure (a wire/normalize
    /// regression); a missing key or an unusable-account provider error is a clean skip.
    async fn run_target(t: &Target) -> TargetOutcome {
        common::install_snapshot();
        let Some(key) = read_api_key(t.auth_key) else {
            return TargetOutcome::SkippedNoKey;
        };

        // Resolve provider + model ENTIRELY from the catalog — no hand-built config.
        let (provider_spec, model_spec) = common::install_snapshot()
            .model(t.provider_id, t.model_id)
            .unwrap_or_else(|| {
                panic!(
                    "{}/{} is in the vendored catalog",
                    t.provider_id, t.model_id
                )
            });

        let family = provider_spec.wire_family;
        // The catalog must have classified this as the openai-compatible OpenAiChat
        // family with the expected base_url — proving config-not-code.
        assert_eq!(
            format!("{family:?}"),
            "OpenAiChat",
            "{} should resolve to OpenAiChat",
            t.provider_id
        );

        let mut frozen = resolve_frozen(
            &provider_spec,
            &model_spec,
            64,
            renderer_version_for(family),
        )
        .expect("bundled catalog models publish known capability flags");
        assert_eq!(
            frozen.base_url.as_deref(),
            Some(t.expect_base_url),
            "base_url for {}",
            t.provider_id
        );
        assert_eq!(frozen.wire_model_id, t.model_id);
        frozen.max_output_tokens = 64;

        let provider = live_provider(frozen, AuthHandle::Bearer(key)).expect("OpenAiChat is wired");
        let opts = CallOptions::new(vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "Reply with exactly the word: pong".into(),
            }],
        }]);

        let mut rx = provider.step(StepId(1), &opts).await;
        let mut assembled = String::new();
        let mut finished = false;
        let mut error: Option<broca_protocol::ProviderError> = None;
        while let Some(ev) = rx.recv().await {
            match ev {
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    for b in &message.content {
                        if let ContentBlock::Text { text } = b {
                            assembled.push_str(text);
                        }
                    }
                }
                NormalizedEvent::Control(ProviderControl::Finished { .. }) => finished = true,
                NormalizedEvent::Control(ProviderControl::Error { error: e, .. }) => {
                    error = Some(e)
                }
                _ => {}
            }
        }

        if let Some(e) = error {
            // An ACCOUNT-STATE error (billing suspended / payment required / quota exhausted) is
            // not a code defect — it's the same "can't exercise this provider right now" class as
            // a missing key, so skip cleanly rather than fail the suite. A key present on a
            // billing-dead account must not turn the local live run red (CI has no keys → all
            // targets already skip there). Statuses: 402 Payment Required, 412 Precondition Failed
            // (Fireworks' suspended-account code), 429 quota/rate. Any OTHER error is a real
            // failure (a genuine wire/normalize regression) and still panics.
            let account_state = matches!(e.status, Some(402) | Some(412) | Some(429));
            if account_state {
                return TargetOutcome::SkippedAccountState(format!(
                    "{}: {}",
                    e.status.map(|s| s.to_string()).unwrap_or_default(),
                    e.message
                ));
            }
            panic!(
                "catalog-resolved {} provider errored ({:?}): {}",
                t.provider_id, e.status, e.message
            );
        }
        assert!(
            finished,
            "expected a Finished control unit from {}",
            t.provider_id
        );
        assert!(
            !assembled.trim().is_empty(),
            "expected a non-empty reply from {}",
            t.provider_id
        );
        TargetOutcome::Ran(assembled.trim().to_string())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catalog_resolved_providers_run_live() {
        let mut ran = 0;
        for t in TARGETS {
            match run_target(t).await {
                TargetOutcome::Ran(reply) => {
                    ran += 1;
                    eprintln!(
                        "catalog({}/{}) → OpenAiChat @ {} → assembled: {:?}",
                        t.provider_id, t.model_id, t.expect_base_url, reply
                    );
                }
                TargetOutcome::SkippedNoKey => eprintln!(
                    "SKIP {} — BROCA_LIVE_TESTS consent or api-type '{}' key is absent",
                    t.provider_id, t.auth_key
                ),
                TargetOutcome::SkippedAccountState(reason) => eprintln!(
                    "SKIP {} — provider account not usable ({reason})",
                    t.provider_id
                ),
            }
        }
        if ran == 0 {
            eprintln!(
            "SKIP catalog_resolved_providers_run_live: no target ran (no consent / key / usable account)"
        );
        }
    }

    /// The conservative-classification confirmation: a provider models.dev labels with a
    /// NATIVE npm we have neither allowlisted nor fully adapted is NOT served through the catalog,
    /// even though it's reachable in reality. This is `model()` returning `None` for a pending
    /// provider — the catalog refuses to guess a complete route. (A purely-local check; no network,
    /// no key needed.)
    #[test]
    fn catalog_does_not_serve_pending_native_npm_providers() {
        // cohere and groq lack a selected renderer. The Vertex providers have no Vertex
        // endpoint template or auth policy; serving them through a family's public defaults
        // would route requests and credentials to the wrong product.
        for pid in ["cohere", "groq", "google-vertex", "google-vertex-anthropic"] {
            assert!(
            common::install_snapshot().provider(pid).is_none(),
            "{pid} must stay pending until its complete renderer, endpoint, and auth policy are supported"
        );
            assert!(
                common::install_snapshot()
                    .pending_providers()
                    .iter()
                    .any(|p| p.id == pid),
                "{pid} should be listed pending"
            );
        }
    }

    #[test]
    fn vertex_resolution_returns_unsupported_before_provider_construction() {
        common::install_snapshot();
        let result = resolve_provider(
            "google-vertex/gemini-2.5-flash",
            None,
            64,
            &AuthMode::Secret("vertex-oauth-token".to_string()),
            &ResolveOptions::default(),
        );
        let Err(error) = result else {
            panic!("a pending Vertex model must not construct a live provider");
        };
        assert!(
            error.to_string().contains("catalog has no served model")
                && error.to_string().contains("provider is pending"),
            "unexpected pending-provider error: {error}"
        );
    }

    #[test]
    fn vertex_anthropic_resolution_returns_unsupported_before_provider_construction() {
        common::install_snapshot();
        let result = resolve_provider(
            "google-vertex-anthropic/claude-sonnet-4-6@default",
            None,
            64,
            &AuthMode::Secret("vertex-oauth-token".to_string()),
            &ResolveOptions::default(),
        );
        let Err(error) = result else {
            panic!("a pending Vertex Anthropic model must not construct a live provider");
        };
        assert!(
            error.to_string().contains("catalog has no served model")
                && error.to_string().contains("provider is pending"),
            "unexpected pending-provider error: {error}"
        );
    }

    /// Compatible packages with published endpoints resolve without generated overlay pins.
    #[test]
    fn catalog_serves_fact_mapped_openai_compatible_providers() {
        for pid in ["deepseek", "xai", "openrouter"] {
            assert!(
                common::install_snapshot().provider(pid).is_some(),
                "{pid} is overlaid to OpenAiChat and must be served"
            );
            assert!(
                !common::install_snapshot()
                    .pending_providers()
                    .iter()
                    .any(|p| p.id == pid),
                "{pid} must no longer be pending after the overlay"
            );
        }
    }
}
