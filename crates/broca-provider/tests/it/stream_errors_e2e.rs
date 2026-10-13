//! Stream-interruption e2e tests for the OpenAI Chat carrier through aimock.

use crate::family_e2e;

use broca_protocol::caller::RunFinishReason;
use broca_protocol::ErrorClass;
use broca_provider::families::{openai_chat_frozen_config, OpenAiChat};
use family_e2e::{run_family_e2e, RunOutcome, RunSpec};

fn assert_provider_unavailable_terminal(outcome: &RunOutcome, scenario: &str) {
    let err = outcome
        .first_error()
        .unwrap_or_else(|| panic!("{scenario} should surface a provider error"));
    assert_eq!(err.class, ErrorClass::ProviderUnavailable, "{scenario}");
    assert_eq!(err.status, None, "{scenario} should not be an HTTP error");
    assert_eq!(outcome.summary.reason, RunFinishReason::Error, "{scenario}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncated_stream_mid_text_finalizes_on_eof() {
    let fixtures = r#"{
      "fixtures": [
        { "match": {},
          "chunkSize": 8,
          "latency": 50,
          "truncateAfterChunks": 2,
          "response": { "content": "TEXT-PARTIAL-EOF-SHOULD-BE-CUT" } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures).prompt("Return text that will be truncated.");
    let Some(outcome) =
        run_family_e2e(OpenAiChat, openai_chat_frozen_config("mock-model"), spec).await
    else {
        return; // local dev without aimock skips; CI panics inside run_family_e2e
    };

    // Current aimock truncation closes the HTTP body in a way reqwest reports as a
    // transport failure, so the OpenAI normalizer never reaches its EOF flush.
    assert_provider_unavailable_terminal(&outcome, "truncated text stream");
    assert!(
        outcome.tool_call_names().is_empty(),
        "truncated text run should not create tool calls: {:?}",
        outcome.tool_call_names()
    );
    assert_eq!(
        outcome.final_text(),
        "",
        "no assembled assistant message is emitted when transport fails before EOF"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncated_stream_mid_tool_call() {
    let fixtures = r#"{
      "fixtures": [
        { "match": { "hasToolResult": false },
          "chunkSize": 12,
          "latency": 50,
          "truncateAfterChunks": 3,
          "response": { "toolCalls": [
            { "name": "lookup", "arguments": "{\"query\":\"alpha-beta-gamma\",\"limit\":3}" }
          ] } },
        { "match": { "hasToolResult": true },
          "response": { "content": "Completed after the truncated tool call." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures)
        .prompt("Call the lookup tool, then summarize.")
        .tool("lookup", "LOOKUP-RESULT");
    let Some(outcome) =
        run_family_e2e(OpenAiChat, openai_chat_frozen_config("mock-model"), spec).await
    else {
        return; // local dev without aimock skips; CI panics inside run_family_e2e
    };

    assert_provider_unavailable_terminal(&outcome, "truncated tool-call stream");
    assert!(
        outcome.tool_call_names().is_empty(),
        "transport failure prevents finalizing or dispatching the partial tool call: {:?}",
        outcome.tool_call_names()
    );
    assert!(
        outcome.tool_result_texts().is_empty(),
        "no tool result should be produced without a finalized tool call: {:?}",
        outcome.tool_result_texts()
    );
    assert_eq!(outcome.final_text(), "");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mid_stream_disconnect_is_error() {
    // Timing margins matter here: the disconnect must land strictly BETWEEN the first
    // chunk and stream completion, or the test asserts the wrong failure mode. With a
    // per-chunk latency of 50ms and a 96-byte body in 8-byte chunks, the stream spans
    // ~50..600ms; disconnecting at 250ms sits ~200ms clear of both edges. (A 10ms margin
    // here flaked under CI load: the disconnect beat the first chunk, so the error
    // surfaced as a request-send failure instead of a mid-stream chunk error.)
    let fixtures = r#"{
      "fixtures": [
        { "match": {},
          "chunkSize": 8,
          "latency": 50,
          "disconnectAfterMs": 250,
          "response": { "content": "DISCONNECT-BEFORE-THIS-COMPLETES-DISCONNECT-BEFORE-THIS-COMPLETES-DISCONNECT-BEFORE" } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures).prompt("Return text, but disconnect mid-stream.");
    let Some(outcome) =
        run_family_e2e(OpenAiChat, openai_chat_frozen_config("mock-model"), spec).await
    else {
        return; // local dev without aimock skips; CI panics inside run_family_e2e
    };

    let err = outcome
        .first_error()
        .expect("mid-stream disconnect should surface a provider error");
    assert_eq!(err.class, ErrorClass::ProviderUnavailable);
    assert!(
        err.message.contains("stream chunk"),
        "disconnect should map through transport_error; message was {:?}",
        err.message
    );
    assert_eq!(outcome.summary.reason, RunFinishReason::Error);
}
