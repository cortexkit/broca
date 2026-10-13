//! OpenAI-Chat family e2e — the FIRST family through the shared rig, proving the
//! harness end-to-end (render → aimock SSE → normalize → loop) before the other
//! families fan out. The known-good family (already live-proven against DeepSeek),
//! so a failure here is the rig, not the family.

use crate::family_e2e;

use broca_protocol::caller::RunFinishReason;
use broca_provider::families::{openai_chat_frozen_config, OpenAiChat};
use family_e2e::{run_family_e2e, RunSpec};

/// Two-step tool loop: turn 0 emits a tool_call, turn 1 (after the tool result) emits
/// the final text. Proves the rig drives a real multi-turn loop through aimock with
/// the StubToolPlane supplying the deterministic tool result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_chat_tool_loop_through_aimock() {
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

    let Some(outcome) =
        run_family_e2e(OpenAiChat, openai_chat_frozen_config("mock-model"), spec).await
    else {
        return; // aimock/node absent → skipped cleanly
    };

    if let Some(err) = outcome.first_error() {
        panic!(
            "unexpected provider error ({:?}): {}",
            err.class, err.message
        );
    }

    // The model (aimock) emitted the tool call.
    assert!(
        outcome.tool_call_names().iter().any(|n| n == "get_time"),
        "model called get_time: {:?}",
        outcome.tool_call_names()
    );
    // The StubToolPlane's canned result flowed back into the loop.
    assert!(
        outcome
            .tool_result_texts()
            .iter()
            .any(|t| t == "MADRID-12:00"),
        "stub tool result fed back: {:?}",
        outcome.tool_result_texts()
    );
    // The loop completed with the final text turn after the tool result.
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

/// Text-only: no tool, the model answers directly in one turn. Proves the rig's
/// simplest path + the normalizer's finalize-on-EOF (OpenAI has no in-band terminal
/// event).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_chat_text_only_through_aimock() {
    let fixtures = r#"{
      "fixtures": [
        { "match": {}, "response": { "content": "Hello from the mock." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures).prompt("Say hello.");

    let Some(outcome) =
        run_family_e2e(OpenAiChat, openai_chat_frozen_config("mock-model"), spec).await
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
