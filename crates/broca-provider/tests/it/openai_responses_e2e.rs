//! OpenAI-Responses (`/v1/responses`) family e2e — full provider stack through the
//! shared rig (render → aimock Responses SSE → normalize → loop). Responses differs
//! from Chat: `instructions`/`input` + `max_output_tokens` on the wire, and the
//! normalizer assembles `response.output_text.delta` / `response.function_call_*`
//! events (finalize-on-EOF when `response.completed` is absent).

use crate::family_e2e;

use broca_core::OutputEvent;
use broca_protocol::caller::{ControlUnit, RunFinishReason};
use broca_protocol::display::DisplayEvent;
use broca_provider::families::{openai_responses_frozen_config, OpenAiResponses};
use family_e2e::{run_family_e2e, RunSpec};

/// Text-only: `{}` → `{content}`; Completed + final text. Exercises Responses
/// finalize-on-EOF when the stream ends without `response.completed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_only() {
    let fixtures = r#"{
      "fixtures": [
        { "match": {}, "response": { "content": "Hello from Responses mock." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures).prompt("Say hello.");

    let Some(outcome) = run_family_e2e(
        OpenAiResponses,
        openai_responses_frozen_config("mock-model"),
        spec,
    )
    .await
    else {
        return;
    };

    if let Some(err) = outcome.first_error() {
        panic!(
            "unexpected provider error ({:?}): {}",
            err.class, err.message
        );
    }
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
    assert!(
        outcome.final_text().contains("Hello from Responses mock"),
        "final answer: {:?}",
        outcome.final_text()
    );
}

/// Single tool turn: `toolCalls` fixture → model calls tool, StubToolPlane result
/// feeds back, run completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_tool_call() {
    let fixtures = r#"{
      "fixtures": [
        { "match": { "hasToolResult": false },
          "response": { "toolCalls": [{ "name": "lookup", "arguments": { "id": "42" } }] } },
        { "match": { "hasToolResult": true },
          "response": { "content": "Lookup complete." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures)
        .prompt("Look up id 42 with the tool.")
        .tool("lookup", "RESULT-42");

    let Some(outcome) = run_family_e2e(
        OpenAiResponses,
        openai_responses_frozen_config("mock-model"),
        spec,
    )
    .await
    else {
        return;
    };

    if let Some(err) = outcome.first_error() {
        panic!(
            "unexpected provider error ({:?}): {}",
            err.class, err.message
        );
    }

    assert!(
        outcome.tool_call_names().iter().any(|n| n == "lookup"),
        "model called lookup: {:?}",
        outcome.tool_call_names()
    );
    assert!(
        outcome.tool_result_texts().iter().any(|t| t == "RESULT-42"),
        "stub tool result fed back: {:?}",
        outcome.tool_result_texts()
    );
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
}

/// Multi-turn tool loop: turn 0 toolCalls, turn 1 (after tool result) final content.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_turn_tool_loop() {
    let fixtures = r#"{
      "fixtures": [
        { "match": { "hasToolResult": false },
          "response": { "toolCalls": [{ "name": "get_time", "arguments": { "timezone": "Europe/Madrid" } }] } },
        { "match": { "hasToolResult": true },
          "response": { "content": "It is noon in Madrid." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures)
        .prompt("What time is it in Madrid? Use the tool.")
        .tool("get_time", "MADRID-12:00");

    let Some(outcome) = run_family_e2e(
        OpenAiResponses,
        openai_responses_frozen_config("mock-model"),
        spec,
    )
    .await
    else {
        return;
    };

    if let Some(err) = outcome.first_error() {
        panic!(
            "unexpected provider error ({:?}): {}",
            err.class, err.message
        );
    }

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
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
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

/// Longer scripted content assembled from multiple stream chunks (Responses
/// `response.output_text.delta` accumulation).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_accumulation() {
    let long = "Alpha segment. Beta segment. Gamma segment.";
    let fixtures = format!(
        r#"{{
      "fixtures": [
        {{ "match": {{}}, "response": {{ "content": "{long}" }} }}
      ]
    }}"#
    );

    let spec = RunSpec::new(fixtures).prompt("Stream a long reply.");

    let Some(outcome) = run_family_e2e(
        OpenAiResponses,
        openai_responses_frozen_config("mock-model"),
        spec,
    )
    .await
    else {
        return;
    };

    if let Some(err) = outcome.first_error() {
        panic!(
            "unexpected provider error ({:?}): {}",
            err.class, err.message
        );
    }
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
    let text = outcome.final_text();
    assert!(
        text.contains("Alpha segment")
            && text.contains("Beta segment")
            && text.contains("Gamma segment"),
        "accumulated full text: {:?}",
        text
    );
}

/// Responses wire grammar differs from Chat; assert aimock's Responses-shaped SSE
/// decodes through this family's normalizer (typed deltas + stream lifecycle).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn responses_vs_chat_divergence() {
    let fixtures = r#"{
      "fixtures": [
        { "match": {}, "response": { "content": "RESPONSES-WIRE-OK" } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures).prompt("Ping.");

    let Some(outcome) = run_family_e2e(
        OpenAiResponses,
        openai_responses_frozen_config("mock-model"),
        spec,
    )
    .await
    else {
        return;
    };

    if let Some(err) = outcome.first_error() {
        panic!(
            "unexpected provider error ({:?}): {}",
            err.class, err.message
        );
    }

    assert!(
        outcome.final_text().contains("RESPONSES-WIRE-OK"),
        "Responses normalizer decoded content: {:?}",
        outcome.final_text()
    );

    let saw_text_delta = outcome.events.iter().any(|e| {
        matches!(
            e,
            OutputEvent::Display(DisplayEvent::TextDelta { delta, .. })
                if delta.contains("RESPONSES") || !delta.is_empty()
        )
    });
    assert!(
        saw_text_delta,
        "expected Responses text deltas in the event stream (not Chat chunk grammar); event count: {}",
        outcome.events.len()
    );

    let saw_step_started = outcome
        .events
        .iter()
        .any(|e| matches!(e, OutputEvent::Control(ControlUnit::StepStarted { .. })));
    assert!(
        saw_step_started,
        "Responses stream should emit StepStarted after response.created decode"
    );
}
