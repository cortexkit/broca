//! Anthropic Messages family e2e tests through the shared family rig.

use crate::family_e2e;

use broca_core::OutputEvent;
use broca_protocol::{
    caller::{ControlUnit, RunFinishReason},
    content::{AssistantMessage, ContentBlock},
};
use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
use family_e2e::{run_family_e2e, RunOutcome, RunSpec};
use serde_json::json;

async fn run_anthropic(spec: RunSpec) -> Option<RunOutcome> {
    run_family_e2e(
        AnthropicMessages,
        anthropic_frozen_config("mock-model"),
        spec,
    )
    .await
}

fn assert_completed(outcome: &RunOutcome) {
    if let Some(err) = outcome.first_error() {
        panic!(
            "unexpected provider error ({:?}): {}",
            err.class, err.message
        );
    }
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
}

fn assistant_messages(outcome: &RunOutcome) -> impl Iterator<Item = &AssistantMessage> {
    outcome.events.iter().filter_map(|event| match event {
        OutputEvent::Control(ControlUnit::AssistantMessage { message }) => Some(message.as_ref()),
        _ => None,
    })
}

fn latest_assistant_message(outcome: &RunOutcome) -> &AssistantMessage {
    assistant_messages(outcome)
        .last()
        .expect("run emitted an assembled assistant message")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_messages_text_only_through_aimock() {
    let fixtures = r#"{
      "fixtures": [
        { "match": {}, "response": { "content": "Hello from Anthropic Messages aimock." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures).prompt("Say hello.");

    let Some(outcome) = run_anthropic(spec).await else {
        return;
    };

    assert_completed(&outcome);
    assert!(
        outcome.tool_call_names().is_empty(),
        "no tool calls in a text-only run"
    );
    assert!(
        outcome
            .final_text()
            .contains("Hello from Anthropic Messages aimock."),
        "final answer: {:?}",
        outcome.final_text()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_messages_single_tool_call_through_aimock() {
    let fixtures = r#"{
      "fixtures": [
        { "match": { "hasToolResult": false },
          "response": {
            "reasoning": "Use the lookup_weather tool once.",
            "reasoningSignature": "single-tool-signature",
            "toolCalls": [{ "name": "lookup_weather", "arguments": { "city": "Seattle" } }]
          } },
        { "match": { "hasToolResult": true },
          "response": { "content": "The lookup_weather result was received." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures)
        .prompt("Check Seattle weather with the tool.")
        .tool("lookup_weather", "SEATTLE-RAIN");

    let Some(outcome) = run_anthropic(spec).await else {
        return;
    };

    assert_completed(&outcome);
    assert!(
        outcome
            .tool_call_names()
            .iter()
            .any(|n| n == "lookup_weather"),
        "model called lookup_weather: {:?}",
        outcome.tool_call_names()
    );
    assert!(
        outcome
            .tool_result_texts()
            .iter()
            .any(|t| t == "SEATTLE-RAIN"),
        "stub tool result fed back: {:?}",
        outcome.tool_result_texts()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_messages_multi_turn_tool_loop_through_aimock() {
    let fixtures = r#"{
      "fixtures": [
        { "match": { "hasToolResult": false },
          "response": {
            "reasoning": "The prompt asks for Madrid time, so call get_time.",
            "reasoningSignature": "multi-turn-tool-signature",
            "toolCalls": [{ "name": "get_time", "arguments": { "timezone": "Europe/Madrid" } }]
          } },
        { "match": { "hasToolResult": true },
          "response": { "content": "It is noon in Madrid after reading MADRID-12:00." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures)
        .prompt("What time is it in Madrid? Use the tool.")
        .tool("get_time", "MADRID-12:00");

    let Some(outcome) = run_anthropic(spec).await else {
        return;
    };

    assert_completed(&outcome);
    assert!(
        outcome.tool_call_names().iter().any(|n| n == "get_time"),
        "model called get_time: {:?}",
        outcome.tool_call_names()
    );
    assert!(
        outcome
            .tool_result_texts()
            .iter()
            .any(|t| t == "MADRID-12:00"),
        "stub tool result fed back: {:?}",
        outcome.tool_result_texts()
    );
    assert!(
        outcome.summary.steps >= 2,
        "tool turn + answer turn (got {})",
        outcome.summary.steps
    );
    assert!(
        outcome.final_text().contains("noon in Madrid"),
        "final answer present: {:?}",
        outcome.final_text()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_messages_streaming_text_accumulation_through_aimock() {
    let content = "First sentence from a long Anthropic stream. Second sentence arrives in later chunks. Third sentence proves message_stop assembled the full text.";
    let fixtures = json!({
        "fixtures": [{
            "match": {},
            "chunkSize": 11,
            "response": { "content": content }
        }]
    })
    .to_string();

    let spec = RunSpec::new(fixtures).prompt("Return several sentences.");

    let Some(outcome) = run_anthropic(spec).await else {
        return;
    };

    assert_completed(&outcome);
    assert_eq!(
        outcome.final_text(),
        content,
        "Anthropic text deltas accumulated into the full final text"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_messages_thinking_block_round_trip_through_aimock() {
    const REASONING_TEXT: &str = "Compare the prompt with the fixture before answering.";
    const REASONING_SIGNATURE: &str = "sig.v1:anthropic+signature/with=padding==";
    const REDACTED_DATA: &str = "redacted.v1:opaque+payload/with=padding==";

    let fixtures = json!({
        "fixtures": [{
            "match": {},
            "chunkSize": 7,
            "response": {
                "content": "The signed and redacted thinking blocks survived.",
                "reasoning": REASONING_TEXT,
                "reasoningSignature": REASONING_SIGNATURE,
                "redactedThinking": [REDACTED_DATA]
            }
        }]
    })
    .to_string();

    let spec = RunSpec::new(fixtures).prompt("Exercise Anthropic thinking blocks.");

    let Some(outcome) = run_anthropic(spec).await else {
        return;
    };

    assert_completed(&outcome);
    let message = latest_assistant_message(&outcome);

    let reasoning = message
        .content
        .iter()
        .find_map(|block| match block {
            ContentBlock::Reasoning { text, signature } => Some((text.as_str(), signature)),
            _ => None,
        })
        .expect("assistant message carries a signed reasoning block");
    assert_eq!(
        reasoning.0.as_bytes(),
        REASONING_TEXT.as_bytes(),
        "reasoning text round-tripped byte-identically"
    );
    assert_eq!(
        reasoning.1.as_ref().map(|signature| signature.as_bytes()),
        Some(REASONING_SIGNATURE.as_bytes()),
        "reasoning signature round-tripped byte-identically"
    );

    let redacted = message
        .content
        .iter()
        .find_map(|block| match block {
            ContentBlock::RedactedReasoning { data, .. } => Some(data.as_str()),
            _ => None,
        })
        .expect("assistant message carries a redacted reasoning block");
    assert_eq!(
        redacted.as_bytes(),
        REDACTED_DATA.as_bytes(),
        "redacted reasoning data round-tripped byte-identically"
    );
    assert!(
        outcome.final_text().contains("thinking blocks survived"),
        "final answer present: {:?}",
        outcome.final_text()
    );
}
