//! Bedrock ConverseStream family e2e tests through the shared provider-stack rig.
//!
//! These tests drive the real run loop against aimock, which returns AWS binary
//! `application/vnd.amazon.eventstream` frames for ConverseStream responses.

use crate::family_e2e;

use broca_core::OutputEvent;
use broca_protocol::{DisplayEvent, RunFinishReason};
use broca_provider::families::{bedrock_frozen_config, BedrockConverse};
use family_e2e::{run_family_e2e, RunOutcome, RunSpec};

const TEXT_SENTINEL: &str = "BEDROCK-BINARY-DECODE-SENTINEL-17";
const SINGLE_TOOL_RESULT: &str = "BEDROCK-STUB-TOOL-RESULT-ONE";
const LOOP_TOOL_RESULT: &str = "BEDROCK-STUB-TOOL-RESULT-LOOP";
const LOOP_FINAL_TEXT: &str = "Bedrock loop saw the tool result and finished.";
const STREAMING_TEXT: &str = "Bedrock ConverseStream assembled alpha beta gamma delta epsilon zeta across many CRC-checked binary event frames.";

async fn run_bedrock(spec: RunSpec) -> Option<RunOutcome> {
    run_family_e2e(BedrockConverse, bedrock_frozen_config("mock-model"), spec).await
}

fn assert_no_provider_error(outcome: &RunOutcome) {
    if let Some(err) = outcome.first_error() {
        panic!(
            "unexpected provider error ({:?}): {}",
            err.class, err.message
        );
    }
}

fn assert_completed(outcome: &RunOutcome) {
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
}

fn assert_tool_called(outcome: &RunOutcome, tool_name: &str) {
    let names = outcome.tool_call_names();
    assert!(
        names.iter().any(|name| name == tool_name),
        "model called {tool_name}: {names:?}"
    );
}

fn assert_tool_result(outcome: &RunOutcome, expected: &str) {
    let results = outcome.tool_result_texts();
    assert!(
        results.iter().any(|text| text == expected),
        "stub tool result fed back: {results:?}"
    );
}

fn text_delta_chunks(outcome: &RunOutcome) -> Vec<&str> {
    outcome
        .events
        .iter()
        .filter_map(|event| match event {
            OutputEvent::Display(DisplayEvent::TextDelta { delta, .. }) => Some(delta.as_str()),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_only_decodes_real_content() {
    let fixtures = format!(
        r#"{{
          "fixtures": [
            {{ "match": {{}}, "response": {{ "content": "{TEXT_SENTINEL}" }} }}
          ]
        }}"#
    );

    let spec = RunSpec::new(fixtures).prompt("Return the Bedrock binary decode sentinel.");

    let Some(outcome) = run_bedrock(spec).await else {
        return;
    };

    assert_no_provider_error(&outcome);
    assert_completed(&outcome);
    assert!(
        outcome.tool_call_names().is_empty(),
        "text-only run should not call tools: {:?}",
        outcome.tool_call_names()
    );
    assert!(
        outcome.final_text().contains(TEXT_SENTINEL),
        "Bedrock binary event-stream frames must decode into the sentinel content, got {:?}",
        outcome.final_text()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_tool_call() {
    let fixtures = r#"{
      "fixtures": [
        { "match": { "hasToolResult": false },
          "response": { "toolCalls": [{ "name": "lookup_status", "arguments": { "ticket": "BR-42" } }] } },
        { "match": { "hasToolResult": true },
          "response": { "content": "The status lookup is complete." } }
      ]
    }"#;

    let spec = RunSpec::new(fixtures)
        .prompt("Look up ticket BR-42 with the tool.")
        .tool("lookup_status", SINGLE_TOOL_RESULT);

    let Some(outcome) = run_bedrock(spec).await else {
        return;
    };

    assert_no_provider_error(&outcome);
    assert_tool_called(&outcome, "lookup_status");
    assert_tool_result(&outcome, SINGLE_TOOL_RESULT);
    assert_completed(&outcome);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_turn_tool_loop() {
    let fixtures = format!(
        r#"{{
          "fixtures": [
            {{ "match": {{ "hasToolResult": false }},
              "response": {{ "toolCalls": [{{ "name": "check_ready", "arguments": {{ "ready": false }} }}] }} }},
            {{ "match": {{ "hasToolResult": true }},
              "response": {{ "content": "{LOOP_FINAL_TEXT}" }} }}
          ]
        }}"#
    );

    let spec = RunSpec::new(fixtures)
        .prompt("Check readiness with the tool before answering.")
        .tool("check_ready", LOOP_TOOL_RESULT);

    let Some(outcome) = run_bedrock(spec).await else {
        return;
    };

    assert_no_provider_error(&outcome);
    assert_tool_called(&outcome, "check_ready");
    assert_tool_result(&outcome, LOOP_TOOL_RESULT);
    assert!(
        outcome.final_text().contains(LOOP_FINAL_TEXT),
        "final text after tool loop: {:?}",
        outcome.final_text()
    );
    assert!(
        outcome.summary.steps >= 2,
        "tool call turn plus final answer turn (got {})",
        outcome.summary.steps
    );
    assert_completed(&outcome);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_accumulation() {
    let fixtures = format!(
        r#"{{
          "fixtures": [
            {{ "match": {{}}, "chunkSize": 9, "response": {{ "content": "{STREAMING_TEXT}" }} }}
          ]
        }}"#
    );

    let spec = RunSpec::new(fixtures).prompt("Return the long Bedrock streaming sentence.");

    let Some(outcome) = run_bedrock(spec).await else {
        return;
    };

    assert_no_provider_error(&outcome);
    assert_completed(&outcome);
    assert_eq!(outcome.final_text(), STREAMING_TEXT);

    let chunks = text_delta_chunks(&outcome);
    assert!(
        chunks.len() > 1,
        "chunkSize fixture should produce multiple contentBlockDelta frames: {chunks:?}"
    );
    assert_eq!(chunks.concat(), STREAMING_TEXT);
}
