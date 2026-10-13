//! Drives the Anthropic SSE→canonical normalizer against a synthetic stream
//! preserving a signature-only reasoning block, split text and one split tool call.
//! Any provider response used to refresh this fixture must have its real prompts,
//! identifiers and opaque signatures replaced with invented values before committing.

use broca_protocol::{
    content::{ContentBlock, FinishReason},
    display::DisplayEvent,
    ids::StepId,
    provider::ProviderControl,
};
use broca_provider::{AnthropicNormalizer, NormalizedEvent};

const FIXTURE: &str = include_str!("../fixtures/anthropic-tool-call.sse");

#[test]
fn normalizes_real_anthropic_tool_call_stream() {
    let events = AnthropicNormalizer::normalize_sse(StepId(1), FIXTURE).expect("normalize");

    // First two control units open the stream + step.
    let control: Vec<&ProviderControl> = events
        .iter()
        .filter_map(|e| match e {
            NormalizedEvent::Control(c) => Some(c),
            _ => None,
        })
        .collect();
    assert!(matches!(control[0], ProviderControl::StreamStart { .. }));
    assert!(matches!(
        control[1],
        ProviderControl::StepStarted { step_id: StepId(1) }
    ));

    // Display lane: assistant text deltas reassemble to the full text.
    let text: String = events
        .iter()
        .filter_map(|e| match e {
            NormalizedEvent::Display(DisplayEvent::TextDelta { delta, .. }) => Some(delta.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        text,
        "An invented blue-tile sample.An invented blue-tile sample."
    );

    // Display lane: tool-input deltas reassemble to valid JSON args.
    let tool_input: String = events
        .iter()
        .filter_map(|e| match e {
            NormalizedEvent::Display(DisplayEvent::ToolInputDelta { delta, .. }) => {
                Some(delta.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(tool_input, r#"{"timezone": "Europe/Madrid"}"#);

    // The ASSEMBLED assistant message: reasoning(sig) + text + tool_call, in order.
    let assembled = control
        .iter()
        .find_map(|c| match c {
            ProviderControl::AssistantMessage { message } => Some(message),
            _ => None,
        })
        .expect("assembled assistant message");

    assert_eq!(assembled.content.len(), 3, "reasoning + text + tool_call");
    match &assembled.content[0] {
        ContentBlock::Reasoning { text, signature } => {
            assert!(
                text.is_empty(),
                "this turn streamed only a signature, no thinking text"
            );
            assert!(
                signature
                    .as_deref()
                    .is_some_and(|s| s.starts_with("synthetic-opaque-")),
                "thinking signature preserved"
            );
        }
        other => panic!("expected reasoning first, got {other:?}"),
    }
    assert!(matches!(&assembled.content[1], ContentBlock::Text { .. }));
    match &assembled.content[2] {
        ContentBlock::ToolCall(call) => {
            assert_eq!(call.tool_name, "get_time");
            assert_eq!(call.tool_call_id.0, "00000000-0000-4000-8000-000000000024");
            assert_eq!(call.input["timezone"], "Europe/Madrid");
        }
        other => panic!("expected tool_call third, got {other:?}"),
    }

    // tool_calls() helper surfaces exactly the one call in source order.
    assert_eq!(assembled.tool_calls().count(), 1);

    // Step + stream finished with authoritative usage + tool-call stop reason.
    let step_fin = control
        .iter()
        .find_map(|c| match c {
            ProviderControl::StepFinished {
                usage,
                finish_reason,
                ..
            } => Some((usage, finish_reason)),
            _ => None,
        })
        .expect("step finished");
    assert_eq!(step_fin.0.input_tokens, Some(453));
    assert_eq!(step_fin.0.output_tokens, Some(149));
    assert_eq!(*step_fin.1, FinishReason::ToolCalls);

    assert!(control.iter().any(|c| matches!(
        c,
        ProviderControl::Finished {
            finish_reason: FinishReason::ToolCalls,
            ..
        }
    )));
}

/// Drives the OpenAI-Responses normalizer against a synthetic codex-backend stream
/// shaped like gpt-5.3-codex-spark, which sends NO `function_call_arguments.delta` events at
/// all: `output_item.added` carries `arguments:""` and the full argument string arrives
/// ONLY on `response.function_call_arguments.done` (+ the completed item). Those
/// authoritative events must populate the tool input even when there are no deltas;
/// otherwise the call receives EMPTY `{}` input and fails with missing-argument errors.
#[test]
fn normalizes_spark_done_only_tool_arguments() {
    use broca_provider::families::{openai_responses_frozen_config, OpenAiResponses};
    use broca_provider::wire::WireFamily;

    let sse = include_bytes!("../fixtures/codex_spark_toolcall_done_only.sse");
    let family = OpenAiResponses;
    let config = openai_responses_frozen_config("gpt-5.3-codex-spark");
    let mut normalizer = family.normalizer(&config, StepId(1));

    let mut events = Vec::new();
    for chunk in sse.chunks(113) {
        events.extend(
            normalizer
                .push(chunk)
                .expect("synthetic spark SSE normalizes"),
        );
    }
    events.extend(normalizer.finish().expect("clean EOF"));

    let assembled = events
        .iter()
        .find_map(|e| match e {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("stream assembles an assistant message");
    let tool_call = assembled
        .content
        .iter()
        .find_map(|block| match block {
            ContentBlock::ToolCall(tc) => Some(tc),
            _ => None,
        })
        .expect("the spark stream carries a tool call");
    assert_eq!(tool_call.tool_name, "read");
    assert_eq!(
        tool_call.input,
        serde_json::json!({"filePath": "/tmp/hello.txt"}),
        "arguments arriving only on the done event assemble into the tool input"
    );
}

fn responses_tool_input(delta: &str, output_item_done: &str) -> serde_json::Value {
    use broca_provider::families::{openai_responses_frozen_config, OpenAiResponses};
    use broca_provider::wire::WireFamily;

    let sse = format!(
        "data: {{\"type\":\"response.created\",\"response\":{{\"id\":\"resp-test\"}}}}\n\n\
         data: {{\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{{\"type\":\"function_call\",\"call_id\":\"call-1\",\"name\":\"ping\",\"arguments\":\"\"}}}}\n\n\
         data: {{\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":{delta}}}\n\n\
         data: {output_item_done}\n\n\
         data: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"resp-test\",\"status\":\"completed\"}}}}\n\n"
    );
    let family = OpenAiResponses;
    let config = openai_responses_frozen_config("test-model");
    let mut normalizer = family.normalizer(&config, StepId(7));
    let events = normalizer
        .push(sse.as_bytes())
        .expect("response SSE normalizes");
    let message = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("response assembles an assistant message");
    message
        .content
        .iter()
        .find_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(call.input.clone()),
            _ => None,
        })
        .expect("response assembles a tool call")
}

#[test]
fn output_item_done_present_empty_arguments_override_stale_deltas() {
    let input = responses_tool_input(
        r#""garbled""#,
        r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"call-1","name":"ping","arguments":""}}"#,
    );
    assert_eq!(input, serde_json::json!({}));
}

#[test]
fn output_item_done_absent_arguments_preserve_accumulated_value() {
    let input = responses_tool_input(
        r#""{}""#,
        r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"call-1","name":"ping"}}"#,
    );
    assert_eq!(input, serde_json::json!({}));
}
