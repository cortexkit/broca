//! Live proof that the catalog now resolves a NATIVE family (Anthropic) correctly and
//! runs end-to-end against the real API — the headline of the per-family-default-quirks
//! fix.
//!
//! Before the fix, a catalog-resolved Anthropic rendered WITHOUT the required
//! "You are Claude Code" identity-lead system block (the catalog hardcoded the
//! OpenAI-chat quirk shape), so the real OAuth API rejected EVERY request. This drives
//! the full catalog path — `Catalog::model("anthropic", …)` → `resolve_frozen` →
//! `live_provider` → real `/v1/messages` — and asserts a real reply assembles, which is
//! only possible if the identity-lead block is present.
//!
//! Gated on explicit `BROCA_LIVE_TESTS` consent and a valid Anthropic OAuth token in the
//! OpenCode auth file; skips cleanly (no silent pass) when consent is absent or the token is
//! absent/expired. Secrets are read from disk and never echoed.
// A file-level `cfg` DELETES this file when the feature is off, and a deleted file reports
// "running 0 tests ... test result: ok" -- indistinguishable at a glance from a suite that
// ran and passed, and impossible to instrument because no code survives to carry a notice.
// Gating a module instead leaves this shell compiled, so the absence can announce itself.
#[cfg(not(all(feature = "bundled-catalog", feature = "live")))]
#[test]
fn this_suite_needs_its_feature_enabled() {
    eprintln!(
        "SKIP catalog_native_live_e2e: compiled without its feature, so none of its cases exist. \
         This reports `ok` -- re-run with `-F broca-catalog/live` to execute them."
    );
}

#[cfg(all(feature = "bundled-catalog", feature = "live"))]
use crate::common;

#[cfg(all(feature = "bundled-catalog", feature = "live"))]
mod gated {

    use super::common;
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

    /// Read the Anthropic OAuth access token from the OpenCode auth file (never logged).
    /// Returns `None` (→ clean skip) if missing or expired.
    fn anthropic_oauth_token() -> Option<String> {
        if !live_calls_opted_in() {
            return None;
        }
        let home = std::env::var("HOME").ok()?;
        let path = std::path::PathBuf::from(home).join(".local/share/opencode/auth.json");
        let bytes = std::fs::read(&path).ok()?;
        let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
        let a = v.get("anthropic")?;
        let access = a
            .get("access")
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())?;
        // Skip if the token is expired (this path does not refresh).
        if let Some(exp) = a.get("expires").and_then(|e| e.as_u64()) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            if exp <= now + 5_000 {
                return None;
            }
        }
        Some(access.to_string())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catalog_resolved_anthropic_runs_live() {
        let Some(token) = anthropic_oauth_token() else {
            eprintln!(
                "SKIP catalog_resolved_anthropic_runs_live: BROCA_LIVE_TESTS consent or a valid anthropic OAuth token is absent"
            );
            return;
        };

        // A real catalog model id served on the Claude-subscription OAuth endpoint. The
        // wire model id is the part after the provider.
        let (provider_spec, model_spec) = common::install_snapshot()
            .model("anthropic", "claude-opus-4-8")
            .expect("anthropic/claude-opus-4-8 is in the vendored catalog");

        // The catalog must resolve Anthropic to its NATIVE family — not OpenAI-Chat.
        assert_eq!(
            format!("{:?}", provider_spec.wire_family),
            "AnthropicMessages",
            "anthropic must resolve to the AnthropicMessages family"
        );

        let family = provider_spec.wire_family;
        let mut frozen = resolve_frozen(
            &provider_spec,
            &model_spec,
            64,
            renderer_version_for(family),
        )
        .expect("bundled catalog models publish known capability flags");
        frozen.max_output_tokens = 64;

        // Sanity: the rendered request carries the required identity-lead block (the exact
        // thing the bug dropped). If this is absent the live call would 400.
        let provider =
            live_provider(frozen, AuthHandle::Bearer(token)).expect("AnthropicMessages wired");
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
        let rendered = String::from_utf8(provider.render_request(&opts)).expect("utf8 body");
        assert!(
            rendered.contains("You are Claude Code"),
            "catalog-resolved Anthropic must render the required identity-lead block"
        );

        // Drive the real provider and assemble the reply — only succeeds if the wire shape
        // (identity-lead included) is accepted by the real API.
        let mut rx = provider.step(StepId(1), &opts).await;
        let mut assembled = String::new();
        let mut finished = false;
        let mut error: Option<String> = None;
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
                    error = Some(e.message)
                }
                _ => {}
            }
        }

        if let Some(e) = error {
            // An overload (529) is a provider-side transient — skip clean, never fail.
            if e.contains("overload") || e.contains("Overloaded") || e.contains("529") {
                eprintln!("SKIP catalog_resolved_anthropic_runs_live: provider overloaded ({e})");
                return;
            }
            panic!("catalog-resolved anthropic errored: {e}");
        }
        assert!(finished, "expected a Finished control unit from anthropic");
        assert!(
            !assembled.trim().is_empty(),
            "expected a non-empty reply from a catalog-resolved anthropic run"
        );
        eprintln!(
            "catalog(anthropic/claude-opus-4-8) → AnthropicMessages → live reply: {assembled:?}"
        );
    }
}
