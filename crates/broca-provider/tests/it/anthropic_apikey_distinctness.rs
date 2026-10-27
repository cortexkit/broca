//! Proves the Anthropic api-key SHAPE is genuinely body-distinct from the Claude-subscription
//! OAuth shape — the reclassification deliverable.
//!
//! anthropic api-key and anthropic OAuth speak the IDENTICAL `/v1/messages` wire GRAMMAR, so
//! the api-key variant is a QUIRKS config of the AnthropicMessages family, NOT a separate wire
//! family. But "config, not family" is only correct if the distinction is real and
//! body-affecting (not a bare header swap). This test pins that: the two shapes render
//! DIFFERENT bytes (the OAuth identity lead is present in one, absent in the other), and the
//! `anthropic-beta: oauth-…` header is sent ONLY on the OAuth shape. If a future change
//! collapsed the two shapes, this fails.
//!
//! Feature-free: render is pure (no transport), so it needs no `live`.

use broca_protocol::{
    content::{Message, Role},
    request::CallOptions,
};
use broca_provider::families::{
    anthropic_apikey_frozen_config, anthropic_frozen_config, AnthropicMessages,
};
use broca_provider::wire::{AuthHandle, WireFamily};
use serde_json::Value;

/// A minimal request — a single system + user turn is enough to expose the system-block
/// difference (identity lead present vs absent).
fn request() -> CallOptions {
    CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::System,
            content: vec![broca_protocol::content::ContentBlock::Text {
                text: "Be concise.".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![broca_protocol::content::ContentBlock::Text {
                text: "Hello.".into(),
            }],
        },
    ])
}

#[test]
fn the_two_anthropic_shapes_render_different_bytes() {
    let oauth = anthropic_frozen_config("claude-opus-4-8");
    let apikey = anthropic_apikey_frozen_config("claude-opus-4-8");

    let oauth_bytes = AnthropicMessages.render(&oauth, &request());
    let apikey_bytes = AnthropicMessages.render(&apikey, &request());

    // The reclassification proof: the bodies are NOT byte-identical — the difference is real
    // and body-affecting (so "quirks config, not a header swap" is justified).
    assert_ne!(
        oauth_bytes, apikey_bytes,
        "the OAuth and api-key anthropic shapes must render different request bodies"
    );

    let oauth_json: Value = serde_json::from_slice(&oauth_bytes).unwrap();
    let apikey_json: Value = serde_json::from_slice(&apikey_bytes).unwrap();

    // The concrete difference: the OAuth shape leads `system` with the required Claude-Code
    // identity block; the api-key shape carries only the caller's system text.
    let oauth_system = oauth_json["system"].as_array().expect("system is an array");
    let apikey_system = apikey_json["system"]
        .as_array()
        .expect("system is an array");

    let oauth_lead = oauth_system[0]["text"].as_str().unwrap_or_default();
    assert!(
        oauth_lead.contains("Claude Code"),
        "the OAuth shape MUST lead system with the Claude-Code identity (got: {oauth_lead:?})"
    );
    let apikey_lead = apikey_system[0]["text"].as_str().unwrap_or_default();
    assert!(
        !apikey_lead.contains("Claude Code"),
        "the api-key shape MUST NOT carry the OAuth identity lead (got: {apikey_lead:?})"
    );
    // The api-key system is exactly the caller's one block (no injected lead).
    assert_eq!(
        apikey_system.len(),
        1,
        "the api-key shape's system is just the caller's block, no identity lead prepended"
    );
}

#[test]
fn oauth_beta_header_is_sent_only_on_the_oauth_shape() {
    let oauth = anthropic_frozen_config("claude-opus-4-8");
    let apikey = anthropic_apikey_frozen_config("claude-opus-4-8");

    // The endpoint attaches headers; auth is irrelevant to the beta-header decision (it's
    // policy-driven off the body shape), so a dummy handle is fine.
    let oauth_req = AnthropicMessages.endpoint(&oauth, &AuthHandle::None, b"{}".to_vec());
    let apikey_req = AnthropicMessages.endpoint(&apikey, &AuthHandle::None, b"{}".to_vec());

    let beta = |req: &broca_provider::wire::HttpRequest| -> Option<String> {
        req.headers
            .iter()
            .find(|(k, _)| k == "anthropic-beta")
            .map(|(_, v)| v.clone())
    };

    let oauth_beta = beta(&oauth_req).expect("OAuth shape sends an anthropic-beta header");
    assert!(
        oauth_beta.contains("oauth-2025-04-20"),
        "the OAuth shape MUST send the oauth client beta (got: {oauth_beta:?})"
    );

    // The api-key shape has neither the OAuth beta nor (with reasoning None) the interleaved
    // beta → no anthropic-beta header at all.
    assert!(
        beta(&apikey_req).is_none(),
        "the api-key shape MUST NOT send the oauth-2025-04-20 beta (it's an OAuth-client signal)"
    );
}

/// Pin the ordered wire headers before sharing the policy with other clients. Expected
/// values are literals, independent of the header builder and its beta constants.
#[test]
fn anthropic_auth_shapes_keep_exact_master_header_lists() {
    use broca_provider::policy::ReasoningPolicy;

    let cases = [
        (true, true, false, Some("oauth-2025-04-20,interleaved-thinking-2025-05-14,thinking-binding-controls-2026-08-01")),
        (true, false, false, Some("oauth-2025-04-20")),
        (false, false, false, None),
        (false, true, false, Some("interleaved-thinking-2025-05-14,thinking-binding-controls-2026-08-01")),
        (true, true, true, Some("oauth-2025-04-20,interleaved-thinking-2025-05-14,thinking-binding-controls-2026-08-01,fast-mode-2026-02-01")),
        (true, false, true, Some("oauth-2025-04-20,fast-mode-2026-02-01")),
        (false, false, true, Some("fast-mode-2026-02-01")),
        (false, true, true, Some("interleaved-thinking-2025-05-14,thinking-binding-controls-2026-08-01,fast-mode-2026-02-01")),
    ];
    for (oauth, thinking, fast, beta) in cases {
        let mut frozen = if oauth {
            anthropic_frozen_config("claude-opus-4-8")
        } else {
            anthropic_apikey_frozen_config("claude-opus-4-8")
        };
        frozen.reasoning = if thinking {
            anthropic_frozen_config("claude-opus-4-8").reasoning
        } else {
            ReasoningPolicy::None
        };
        frozen.speed = fast.then(|| "fast".to_owned());
        let auth = if oauth {
            AuthHandle::Bearer("synthetic-oauth-token".into())
        } else {
            AuthHandle::ApiKeyHeader {
                header: "x-api-key".into(),
                value: "synthetic-api-key".into(),
            }
        };
        let mut expected = vec![
            ("anthropic-version", "2023-06-01"),
            ("content-type", "application/json"),
            ("accept", "application/json"),
            (
                "user-agent",
                if oauth {
                    "claude-cli/2.1.280 (external, cli)"
                } else {
                    concat!("broca/", env!("CARGO_PKG_VERSION"))
                },
            ),
        ];
        if oauth {
            expected.push(("x-app", "cli"));
        }
        if let Some(beta) = beta {
            expected.push(("anthropic-beta", beta));
        }
        expected.push(if oauth {
            ("authorization", "Bearer synthetic-oauth-token")
        } else {
            ("x-api-key", "synthetic-api-key")
        });
        let expected: Vec<(String, String)> = expected
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        let request = AnthropicMessages.endpoint(&frozen, &auth, b"{}".to_vec());
        assert_eq!(
            request.headers, expected,
            "oauth={oauth}, thinking={thinking}, fast={fast}"
        );
    }
}

/// Server-side fallback is an OAuth capability: the api-key shape never asks the runtime to hand
/// a refusal on, even if the flag were somehow set on its config, because the same provider row
/// serves both auth methods and only the subscription route implements the handoff.
#[test]
fn api_key_never_sends_fallbacks() {
    let request = request();
    // The force-on case is applied before the api-key quirks, the order catalog resolution
    // uses: the quirks must clear it, not merely never set it.
    let mut apikey = anthropic_frozen_config("claude-opus-5-5");
    apikey.server_side_fallback = true;
    broca_provider::families::apply_anthropic_apikey_quirks(&mut apikey);
    let bytes = AnthropicMessages.render(&apikey, &request);
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        body.get("fallbacks").is_none(),
        "the api-key shape must not opt in to server-side fallback: {body}"
    );
    let endpoint = AnthropicMessages.endpoint(&apikey, &AuthHandle::None, bytes.clone());
    assert!(
        !endpoint
            .headers
            .iter()
            .any(|(_, value)| value.contains("server-side-fallback")),
        "the api-key shape must not send the fallback beta"
    );
    // The forcing flag alone decides nothing: clearing it leaves the same bytes.
    apikey.server_side_fallback = false;
    assert_eq!(AnthropicMessages.render(&apikey, &request), bytes);

    // The OAuth shape does send it, so the rows above prove the api-key rule rather than a
    // renderer that ignores the flag.
    let mut oauth = anthropic_frozen_config("claude-opus-5-5");
    oauth.server_side_fallback = true;
    let oauth_body: Value =
        serde_json::from_slice(&AnthropicMessages.render(&oauth, &request)).unwrap();
    assert_eq!(oauth_body["fallbacks"], "default");
}
