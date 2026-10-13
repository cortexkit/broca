//! OpenAI-Chat response-shape e2e coverage for degenerate model output. These
//! cases drive the shared family rig against aimock, then assert the caller-visible
//! behavior the real loop exposes.

use crate::family_e2e;

use broca_core::OutputEvent;
use broca_protocol::{
    AssistantMessage, ControlUnit, DisplayEvent, FinishReason, NormalizedEvent, ProviderControl,
    RunFinishReason, StepId, ToolCall,
};
use broca_provider::families::{openai_chat_frozen_config, OpenAiChat};
use broca_provider::wire::WireFamily;
use family_e2e::{run_family_e2e, RunOutcome, RunSpec};
use serde_json::json;

async fn run_openai_chat(spec: RunSpec) -> RunOutcome {
    run_family_e2e(OpenAiChat, openai_chat_frozen_config("mock-model"), spec)
        .await
        .expect("aimock should start; run `npm ci` from the repo root before this test")
}

fn assert_no_provider_error(outcome: &RunOutcome) {
    if let Some(err) = outcome.first_error() {
        panic!(
            "unexpected provider error ({:?}): {}",
            err.class, err.message
        );
    }
}

fn assistant_messages(outcome: &RunOutcome) -> Vec<&AssistantMessage> {
    outcome
        .events
        .iter()
        .filter_map(|event| match event {
            OutputEvent::Control(ControlUnit::AssistantMessage { message }) => {
                Some(message.as_ref())
            }
            _ => None,
        })
        .collect()
}

fn tool_call_events(outcome: &RunOutcome) -> Vec<&ToolCall> {
    outcome
        .events
        .iter()
        .filter_map(|event| match event {
            OutputEvent::Control(ControlUnit::ToolCall { call }) => Some(call),
            _ => None,
        })
        .collect()
}

fn caller_step_finish_reasons(outcome: &RunOutcome) -> Vec<FinishReason> {
    outcome
        .events
        .iter()
        .filter_map(|event| match event {
            OutputEvent::Control(ControlUnit::StepFinished { finish_reason, .. }) => {
                Some(*finish_reason)
            }
            _ => None,
        })
        .collect()
}

fn text_deltas(outcome: &RunOutcome) -> Vec<&str> {
    outcome
        .events
        .iter()
        .filter_map(|event| match event {
            OutputEvent::Display(DisplayEvent::TextDelta { delta, .. }) => Some(delta.as_str()),
            _ => None,
        })
        .collect()
}

fn tool_input_delta_text(outcome: &RunOutcome) -> String {
    let mut rendered = String::new();
    for event in &outcome.events {
        match event {
            OutputEvent::Display(DisplayEvent::DisplayReset) => rendered.clear(),
            OutputEvent::Display(DisplayEvent::ToolInputDelta { delta, .. }) => {
                rendered.push_str(delta);
            }
            _ => {}
        }
    }
    rendered
}

fn provider_finish_reason_for_openai_chat(raw_finish_reason: &str) -> FinishReason {
    let recorded = format!(
        "data: {{\"id\":\"chatcmpl-finish\",\"choices\":[{{\"index\":0,\"delta\":{{\"role\":\"assistant\",\"content\":\"\"}},\"finish_reason\":null}}]}}\n\n\
         data: {{\"id\":\"chatcmpl-finish\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"done\"}},\"finish_reason\":null}}]}}\n\n\
         data: {{\"id\":\"chatcmpl-finish\",\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"{raw_finish_reason}\"}}]}}\n\n\
         data: [DONE]\n\n"
    );
    let frozen = openai_chat_frozen_config("mock-model");
    let mut normalizer = OpenAiChat.normalizer(&frozen, StepId(1));
    let mut events = Vec::new();
    for chunk in recorded.as_bytes().chunks(7) {
        events.extend(normalizer.push(chunk).expect("push OpenAI chat chunk"));
    }
    events.extend(normalizer.finish().expect("finish OpenAI chat stream"));

    events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::Finished { finish_reason, .. }) => {
                Some(*finish_reason)
            }
            _ => None,
        })
        .expect("provider normalizer emitted a Finished control event")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_content_response() {
    let fixtures = r#"{
      "fixtures": [
        { "match": {}, "response": { "content": "" } }
      ]
    }"#;

    let outcome =
        run_openai_chat(RunSpec::new(fixtures).prompt("Return an empty assistant turn.")).await;

    assert_no_provider_error(&outcome);
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
    assert_eq!(outcome.summary.steps, 1);
    assert_eq!(outcome.final_text(), "");
    assert!(
        outcome.tool_call_names().is_empty(),
        "empty content must not invent tool calls: {:?}",
        outcome.tool_call_names()
    );

    let messages = assistant_messages(&outcome);
    assert_eq!(messages.len(), 1, "one assistant message should be emitted");
    assert!(
        messages[0].content.is_empty(),
        "OpenAI normalizer drops empty content deltas; got {:?}",
        messages[0].content
    );
    assert!(
        text_deltas(&outcome).is_empty(),
        "empty content should not emit display text deltas: {:?}",
        text_deltas(&outcome)
    );
    assert_eq!(
        caller_step_finish_reasons(&outcome),
        vec![FinishReason::Stop]
    );
}

/// A non-empty terminal argument buffer must parse as JSON before any canonical
/// assistant message or tool-dispatch unit is emitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unparseable_tool_call_arguments() {
    let fixtures = r#"{
      "fixtures": [
        { "match": { "hasToolResult": false },
          "response": { "toolCalls": [{ "name": "malformed_args", "arguments": "{\"pat" }] } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures)
        .prompt("Call the malformed_args tool once.")
        .tool("malformed_args", "TOOL-OK");
    let outcome = run_openai_chat(spec).await;

    assert!(
        tool_call_events(&outcome).is_empty(),
        "an unparseable tool call must not reach dispatch"
    );
    assert!(
        assistant_messages(&outcome).is_empty(),
        "an unparseable tool call must not emit an assistant message"
    );
    assert!(
        outcome.tool_result_texts().is_empty(),
        "an unparseable tool call must not execute a tool"
    );
    let error = outcome
        .first_error()
        .expect("truncated arguments must produce a typed provider error");
    assert_eq!(error.class, broca_protocol::ErrorClass::Transient);
    assert!(
        error.message.contains("invalid terminal tool arguments"),
        "normalization error should identify terminal tool arguments: {}",
        error.message
    );
    assert_eq!(outcome.summary.reason, RunFinishReason::Error);
    assert_eq!(
        outcome
            .events
            .iter()
            .filter(|event| matches!(event, OutputEvent::Display(DisplayEvent::DisplayReset)))
            .count(),
        1,
        "the malformed response is retried once with a full display reset"
    );
    assert_eq!(
        tool_input_delta_text(&outcome),
        r#"{"pat"#,
        "display lane still carries the raw truncated argument bytes"
    );
    assert!(caller_step_finish_reasons(&outcome).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_tool_call_arguments_decode_as_an_empty_object() {
    let fixtures = r#"{
      "fixtures": [
        { "match": { "hasToolResult": false },
          "response": { "toolCalls": [{ "name": "no_args", "arguments": "" }] } },
        { "match": { "hasToolResult": true },
          "response": { "content": "Zero-argument call completed." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures)
        .prompt("Call the no_args tool once.")
        .tool("no_args", "TOOL-OK");
    let outcome = run_openai_chat(spec).await;

    assert_no_provider_error(&outcome);
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
    assert_eq!(outcome.final_text(), "Zero-argument call completed.");
    let calls = tool_call_events(&outcome);
    assert_eq!(
        calls.len(),
        1,
        "the zero-argument tool should dispatch once"
    );
    assert_eq!(calls[0].input, json!({}));
    assert_eq!(outcome.tool_result_texts(), vec!["TOOL-OK"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unexpected_finish_reason() {
    let fixtures = r#"{
      "fixtures": [
        { "match": {},
          "response": { "content": "Only part of the answer.", "finishReason": "length" } }
      ]
    }"#;

    let outcome =
        run_openai_chat(RunSpec::new(fixtures).prompt("Return a length finish reason.")).await;

    assert_no_provider_error(&outcome);
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
    assert_eq!(outcome.summary.steps, 1);
    assert_eq!(outcome.final_text(), "Only part of the answer.");
    assert_eq!(
        provider_finish_reason_for_openai_chat("length"),
        FinishReason::Length,
        "OpenAI map_finish_reason classifies raw `length` as canonical Length"
    );
    // The provider's terminal reason reaches the caller: a `length` stop (the model
    // hit the token ceiling) surfaces as Length, NOT a shape-recomputed Stop — so a
    // harness can tell "finished naturally" from "ran out of tokens" and act on it.
    assert_eq!(
        caller_step_finish_reasons(&outcome),
        vec![FinishReason::Length],
        "the provider's finish_reason is surfaced to the caller, not recomputed from shape"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runaway_tool_whitespace_returns_error_and_aimock_retries() {
    let fixtures = json!({"fixtures":[
        {"match":{"hasToolResult":false}, "response":{"toolCalls":[{"name":"lookup", "arguments":format!("{{\"key\":\"alpha\"}}{}", " ".repeat(4097))}]}},
        {"match":{"hasToolResult":true}, "response":{"content":"Retried after the malformed call."}},
    ]}).to_string();
    let outcome = run_openai_chat(
        RunSpec::new(&fixtures)
            .prompt("Call lookup.")
            .tool("lookup", "MUST-NOT-EXECUTE"),
    )
    .await;
    assert_no_provider_error(&outcome);
    assert_eq!(outcome.summary.steps, 2);
    assert_eq!(outcome.final_text(), "Retried after the malformed call.");
    let results = outcome.tool_result_texts();
    assert_eq!(results.len(), 1);
    assert!(results[0].starts_with("tool_args_trailing_garbage:"));
    assert!(results[0].contains("whitespace_bytes=4097"));
    assert!(!results[0].contains("MUST-NOT-EXECUTE"));
}
