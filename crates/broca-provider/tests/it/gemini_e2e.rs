//! Google Gemini family e2e — full provider stack (render → aimock SSE → normalize →
//! loop) via the shared family_e2e rig. Exercises Gemini's `candidates[].content.parts`
//! wire grammar and finalize-on-EOF assembly.

use crate::family_e2e;

use broca_core::OutputEvent;
use broca_protocol::caller::{ControlUnit, RunFinishReason};
use broca_protocol::content::{ContentBlock, ToolCall};
use broca_provider::families::{gemini_frozen_config, Gemini};
use family_e2e::{run_family_e2e, RunSpec};
use serde_json::json;

/// Text-only: catch-all fixture returns assistant text in one turn. Proves finalize-on-EOF
/// (Gemini emits the assembled message when the stream ends, with no in-band terminal).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_only() {
    let fixtures = r#"{
      "fixtures": [
        { "match": {}, "response": { "content": "Hello from the mock." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures).prompt("Say hello.");

    let Some(outcome) = run_family_e2e(Gemini, gemini_frozen_config("mock-model"), spec).await
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
        outcome.tool_call_names().is_empty(),
        "no tool calls in a text-only run"
    );
    assert!(
        outcome.final_text().contains("Hello from the mock"),
        "final answer: {:?}",
        outcome.final_text()
    );
}

/// Single turn: model emits a `functionCall` part (Gemini shape); stub tool result feeds
/// back and the run completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_tool_call() {
    let fixtures = r#"{
      "fixtures": [
        { "match": { "hasToolResult": false },
          "response": { "toolCalls": [{ "name": "lookup", "arguments": { "key": "alpha" } }] } },
        { "match": { "hasToolResult": true },
          "response": { "content": "Lookup complete." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures)
        .prompt("Look up alpha with the tool.")
        .tool("lookup", "VALUE-ALPHA");

    let Some(outcome) = run_family_e2e(Gemini, gemini_frozen_config("mock-model"), spec).await
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
        outcome
            .tool_result_texts()
            .iter()
            .any(|t| t == "VALUE-ALPHA"),
        "stub tool result fed back: {:?}",
        outcome.tool_result_texts()
    );
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
}

/// Two-step loop: turn without tool result → toolCalls; after tool result → final text.
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

    let Some(outcome) = run_family_e2e(Gemini, gemini_frozen_config("mock-model"), spec).await
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

/// Longer assistant text is streamed in multiple `parts[].text` chunks; normalizer
/// accumulates the full string by finalize-on-EOF.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_accumulation() {
    let long = "Part one. Part two. Part three — end.";
    let fixtures = format!(
        r#"{{
      "fixtures": [
        {{ "match": {{}}, "response": {{ "content": {long_json} }} }}
      ]
    }}"#,
        long_json = serde_json::to_string(long).expect("json string")
    );

    let spec = RunSpec::new(fixtures).prompt("Give a multi-part answer.");

    let Some(outcome) = run_family_e2e(Gemini, gemini_frozen_config("mock-model"), spec).await
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
        text.contains("Part one.") && text.contains("Part two.") && text.contains("Part three"),
        "accumulated full streamed text: {:?}",
        text
    );
}

/// Asserts the Gemini wire grammar end-to-end: aimock encodes `functionCall` parts and
/// text parts; the normalizer decodes tool name/args (SyntheticIndex → `call_0`) and text.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gemini_shape_quirk() {
    let fixtures = r#"{
      "fixtures": [
        { "match": { "hasToolResult": false },
          "response": {
            "content": "Decoded Gemini text part.",
            "toolCalls": [{ "name": "ping", "arguments": { "n": 42 } }]
          }
        },
        { "match": { "hasToolResult": true },
          "response": { "content": "Pong received." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures)
        .prompt("Ping with a short preamble.")
        .tool("ping", "pong");

    let Some(outcome) = run_family_e2e(Gemini, gemini_frozen_config("mock-model"), spec).await
    else {
        return;
    };

    if let Some(err) = outcome.first_error() {
        panic!(
            "unexpected provider error ({:?}): {}",
            err.class, err.message
        );
    }

    let decoded_gemini_parts = outcome.events.iter().any(|e| match e {
        OutputEvent::Control(ControlUnit::AssistantMessage { message }) => {
            let text_ok = message.content.iter().any(|b| {
                matches!(b, ContentBlock::Text { text } if text.contains("Decoded Gemini text part"))
            });
            let call_ok = message.content.iter().any(|b| {
                matches!(
                    b,
                    ContentBlock::ToolCall(ToolCall {
                        tool_name,
                        input,
                        ..
                    }) if tool_name == "ping" && *input == json!({"n": 42})
                )
            });
            text_ok && call_ok
        }
        _ => false,
    });
    assert!(
        decoded_gemini_parts,
        "expected text + functionCall parts in one assistant message; events: {:?}",
        outcome.events
    );
    assert!(
        outcome.tool_call_names().iter().any(|n| n == "ping"),
        "functionCall part decoded to tool name ping: {:?}",
        outcome.tool_call_names()
    );
    assert!(
        outcome.tool_result_texts().iter().any(|t| t == "pong"),
        "tool loop after functionCall: {:?}",
        outcome.tool_result_texts()
    );
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
}
