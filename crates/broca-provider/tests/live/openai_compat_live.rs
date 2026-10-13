//! Live proof that the GENERIC transport driver drives a SECOND family end-to-end:
//! `LiveProvider<OpenAiChat>` against a real OpenAI-compatible provider (DeepSeek),
//! using only a configured `base_url` over the OpenAI-Chat family — i.e. the
//! openai-compat long tail is config, not code.
//!
//! Gated on explicit `BROCA_LIVE_TESTS` consent and an `api`-type provider key in the
//! OpenCode auth file; skips cleanly (clear message, never a silent pass) when either is
//! absent — the secret is read from disk and never echoed. This is the live regression guard that
//! the generic driver's `finish()`-on-EOF path actually assembles a message for a no-in-band-terminal
//! grammar.

#![cfg(feature = "live")]

use crate::live_common;

use broca_core::provider::Provider;
use broca_protocol::{
    content::{ContentBlock, Message, Role},
    provider::ProviderControl,
    request::CallOptions,
    NormalizedEvent, StepId,
};
use broca_provider::families::{openai_chat_frozen_config, OpenAiChat};
use broca_provider::wire::AuthHandle;
use broca_provider::LiveProvider;
use live_common::opencode_api_key;

/// One OpenAI-compatible provider we can reach with a key from the auth file.
struct CompatTarget {
    base_url: &'static str,
    model: &'static str,
    auth_key: &'static str,
}

const TARGETS: &[CompatTarget] = &[
    CompatTarget {
        base_url: "https://api.deepseek.com",
        model: "deepseek-v4-flash",
        auth_key: "deepseek",
    },
    CompatTarget {
        base_url: "https://api.cerebras.ai/v1",
        model: "llama-3.3-70b",
        auth_key: "cerebras",
    },
];

#[tokio::test(flavor = "multi_thread")]
async fn openai_chat_family_runs_live_against_a_compatible_provider() {
    // Pick the first target we have a key for.
    let Some((target, key)) = TARGETS
        .iter()
        .find_map(|t| opencode_api_key(t.auth_key).map(|k| (t, k)))
    else {
        eprintln!(
            "SKIP openai_chat_family_runs_live: explicit BROCA_LIVE_TESTS consent or an api-type key is absent for every {:?} target",
            TARGETS.iter().map(|t| t.auth_key).collect::<Vec<_>>()
        );
        return;
    };

    // Build LiveProvider<OpenAiChat> pointed at the compatible provider via base_url
    // — the SAME family that serves OpenAI, just a different host (config, not code).
    let mut frozen = openai_chat_frozen_config(target.model);
    frozen.base_url = Some(target.base_url.to_string());
    frozen.max_output_tokens = 64;
    let provider = LiveProvider::new(OpenAiChat, frozen, AuthHandle::Bearer(key));

    let opts = CallOptions::new(vec![Message {
        cache_prefix_blocks: None,
        origin: None,
        ordinal: None,
        role: Role::User,
        content: vec![ContentBlock::Text {
            text: "Reply with exactly the word: pong".into(),
        }],
    }]);

    let mut rx = provider.step(StepId(1), &opts).await;

    let mut assembled_text = String::new();
    let mut got_finished = false;
    let mut error: Option<String> = None;
    while let Some(ev) = rx.recv().await {
        match ev {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                for b in &message.content {
                    if let ContentBlock::Text { text } = b {
                        assembled_text.push_str(text);
                    }
                }
            }
            NormalizedEvent::Control(ProviderControl::Finished { .. }) => got_finished = true,
            NormalizedEvent::Control(ProviderControl::Error { error: e, .. }) => {
                error = Some(format!("{}: {}", e.class as u8, e.message));
            }
            _ => {}
        }
    }

    if let Some(e) = error {
        panic!("live provider {} errored: {e}", target.auth_key);
    }
    // The assembled message arrived via finish() on EOF (OpenAI chat has no in-band
    // terminal event) — proving the generic driver drives this family end-to-end.
    assert!(
        got_finished,
        "expected a Finished control unit from {}",
        target.auth_key
    );
    assert!(
        !assembled_text.trim().is_empty(),
        "expected non-empty assembled text from {}; got empty",
        target.auth_key
    );
    eprintln!(
        "live {} ({}) via LiveProvider<OpenAiChat> → assembled: {:?}",
        target.auth_key,
        target.model,
        assembled_text.trim()
    );
}
