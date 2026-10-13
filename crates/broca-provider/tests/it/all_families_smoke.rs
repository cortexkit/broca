//! Harness-first gate: a minimal text-only happy-path through ALL FIVE wire families
//! against aimock, proving the shared rig drives each family's full stack (render →
//! family-specific wire response → normalize → loop) before the per-family masons fan
//! out. Each mason then only authors fixtures + assertions for its family, never
//! fights transport/base_url/path — those are proven here.
//!
//! aimock encodes the same logical `{content}` fixture into each family's native wire
//! shape (OpenAI SSE deltas, Anthropic content_block events, Gemini candidates,
//! Bedrock BINARY vnd.amazon.eventstream), so a green run per family proves that
//! family's normalizer decoded real content end-to-end.

use crate::family_e2e;

use broca_protocol::caller::RunFinishReason;
use broca_provider::families::{
    anthropic_frozen_config, bedrock_frozen_config, gemini_frozen_config,
    openai_chat_frozen_config, openai_responses_frozen_config, AnthropicMessages, BedrockConverse,
    Gemini, OpenAiChat, OpenAiResponses,
};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};
use family_e2e::{run_family_e2e, RunOutcome, RunSpec};

const SENTINEL: &str = "FAMILY-SMOKE-OK";

fn text_only_fixtures() -> String {
    format!(
        r#"{{ "fixtures": [ {{ "match": {{}}, "response": {{ "content": "{SENTINEL}" }} }} ] }}"#
    )
}

/// Assert a text-only run assembled the sentinel content through the family's
/// normalizer (proving a real decode, not just a non-error).
fn assert_text_only(outcome: Option<RunOutcome>, family: &str) {
    let Some(outcome) = outcome else {
        eprintln!("SKIP {family}: aimock/node absent");
        return;
    };
    if let Some(err) = outcome.first_error() {
        panic!(
            "{family}: unexpected provider error ({:?}): {}",
            err.class, err.message
        );
    }
    assert_eq!(
        outcome.summary.reason,
        RunFinishReason::Completed,
        "{family}: run completed"
    );
    assert!(
        outcome.final_text().contains(SENTINEL),
        "{family}: normalizer decoded the content (got {:?})",
        outcome.final_text()
    );
}

async fn smoke<F: WireFamily + 'static>(family: F, frozen: FrozenRenderConfig, name: &str) {
    let spec = RunSpec::new(text_only_fixtures()).prompt("Say the sentinel.");
    let outcome = run_family_e2e(family, frozen, spec).await;
    assert_text_only(outcome, name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_chat_reaches_aimock() {
    smoke(
        OpenAiChat,
        openai_chat_frozen_config("mock-model"),
        "OpenAiChat",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_responses_reaches_aimock() {
    smoke(
        OpenAiResponses,
        openai_responses_frozen_config("mock-model"),
        "OpenAiResponses",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_reaches_aimock() {
    smoke(
        AnthropicMessages,
        anthropic_frozen_config("mock-model"),
        "AnthropicMessages",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gemini_reaches_aimock() {
    smoke(Gemini, gemini_frozen_config("mock-model"), "Gemini").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bedrock_reaches_aimock() {
    smoke(
        BedrockConverse,
        bedrock_frozen_config("mock-model"),
        "BedrockConverse",
    )
    .await;
}
