//! Script output references through Messages, whose native content blocks preserve each
//! published sample's boundaries. Chat text deltas concatenate and cannot model that fixture.

use super::*;
use cortexkit_role_llm_runner_conformance::ScriptedTurn;

pub(super) const PROVIDER: &str = "anthropic";
pub(super) const MODEL: &str = "conformance-output-refs";

/// The context window is 1M tokens because `output_refs_snapshot_budget` stores a 1 MiB tool
/// result and the following request must fit the window. With a smaller one, broca correctly
/// ends the run as context_overflow before sending, as a provider would refuse that request.
pub(super) fn catalog_row() -> Value {
    json!({"capability.attachment":false,"capability.reasoning":true,
        "capability.tool_call":true,"existence":"present","limit.context":1000000,
        "wire.npm":{"value":"@ai-sdk/anthropic","from":"fixture"},
        "wire.api":{"value":"https://conformance.invalid/v1","from":"fixture"}})
}

pub(super) fn stream_for_model(id: &str, model: &str, turn: &ScriptedTurn) -> String {
    stream_signed(id, model, turn, None)
}

/// [`stream_for_model`], with each reasoning block signed by `signature` when one is given, as
/// Anthropic signs thinking with a `signature_delta` before the block stops.
pub(super) fn stream_signed(
    id: &str,
    model: &str,
    turn: &ScriptedTurn,
    signature: Option<&str>,
) -> String {
    let mut body = String::new();
    let mut event = |value: Value| {
        body.push_str(&format!(
            "event: {}\ndata: {value}\n\n",
            value["type"].as_str().unwrap()
        ));
    };
    // A turn's scripted usage, in Messages terms: input excludes the cache reads and writes.
    let (input, read, write, output) = turn.usage.as_ref().map_or((1, 0, 0, 1), |usage| {
        (
            usage.input_tokens,
            usage.cache_read_tokens,
            usage.cache_write_tokens,
            usage.output_tokens,
        )
    });
    event(
        json!({"type":"message_start", "message":{"id":id,"type":"message","role":"assistant","model":model,
        "content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":input,
        "cache_read_input_tokens":read,"cache_creation_input_tokens":write,"output_tokens":0}}}),
    );
    let mut calls = false;
    for (index, part) in turn.parts.iter().enumerate() {
        let (block, delta) = match part {
            ScriptedPart::Text(text) => (
                json!({"type":"text","text":""}),
                json!({"type":"text_delta","text":text}),
            ),
            ScriptedPart::Reasoning(text) => (
                json!({"type":"thinking","thinking":""}),
                json!({"type":"thinking_delta","thinking":text}),
            ),
            ScriptedPart::ToolCall(call) => {
                calls = true;
                (
                    json!({"type":"tool_use","id":call.tool_call_id,"name":call.tool,"input":{}}),
                    json!({"type":"input_json_delta","partial_json":call.arguments.to_string()}),
                )
            }
        };
        let signed = matches!(part, ScriptedPart::Reasoning(_))
            .then_some(signature)
            .flatten();
        event(json!({"type":"content_block_start","index":index,"content_block":block}));
        event(json!({"type":"content_block_delta","index":index,"delta":delta}));
        if let Some(signature) = signed {
            event(json!({"type":"content_block_delta","index":index,
                "delta":{"type":"signature_delta","signature":signature}}));
        }
        event(json!({"type":"content_block_stop","index":index}));
    }
    event(
        json!({"type":"message_delta","delta":{"stop_reason":if calls {"tool_use"} else {"end_turn"},"stop_sequence":null},"usage":{"output_tokens":output}}),
    );
    event(json!({"type":"message_stop"}));
    body
}

#[test]
fn the_messages_stand_in_reports_a_turns_scripted_usage() {
    let usage_of = |turn: &ScriptedTurn| {
        let body = stream_for_model("msg", MODEL, turn);
        let events: Vec<Value> = body
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|data| serde_json::from_str(data).unwrap())
            .collect();
        let start = &events[0]["message"]["usage"];
        let delta = &events[events.len() - 2]["usage"];
        (
            start["input_tokens"].as_u64(),
            start["cache_read_input_tokens"].as_u64(),
            start["cache_creation_input_tokens"].as_u64(),
            delta["output_tokens"].as_u64(),
        )
    };
    let text = || ScriptedTurn::new(vec![ScriptedPart::Text("answer".into())]);
    assert_eq!(
        usage_of(
            &text().with_usage(cortexkit_role_llm_runner_conformance::ScriptedUsage::new(
                17, 3, 0, 11
            ))
        ),
        (Some(17), Some(3), Some(0), Some(11))
    );
    assert_eq!(usage_of(&text()), (Some(1), Some(0), Some(0), Some(1)));
}

#[test]
fn the_media_stand_in_preserves_complete_oversize_tool_arguments() {
    use broca_protocol::{FinishReason, NormalizedEvent, ProviderControl, StepId};
    use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
    use broca_provider::wire::WireFamily;

    // The media suite uses both a payload just over the inline threshold and two much larger
    // siblings in one step. The normalizer must keep all the arguments for transcript reads.
    for sizes in [&[262_145][..], &[2_200_000, 2_200_000][..]] {
        let calls: Vec<_> = sizes
            .iter()
            .enumerate()
            .map(|(index, &size)| ScriptedToolCall {
                tool_call_id: format!("call-{index}"),
                tool: SCRIPTED_TOOL.into(),
                arguments: json!({"marker": index, "payload": "a".repeat(size)}),
                result: json!("unused"),
                hold: false,
            })
            .collect();
        let turn = ScriptedTurn::new(calls.iter().cloned().map(ScriptedPart::ToolCall).collect());
        let raw = stream_for_model("msg", MEDIA_FIXTURE_MODEL, &turn);
        let mut normalizer =
            AnthropicMessages.normalizer(&anthropic_frozen_config(MEDIA_FIXTURE_MODEL), StepId(1));
        let events = normalizer.push(raw.as_bytes()).unwrap();
        assert!(!events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::ToolArgsTrailingGarbage { .. })
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Finished {
                finish_reason: FinishReason::ToolCalls,
                ..
            })
        )));
        let message = events
            .iter()
            .find_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    Some(message)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(message.tool_calls().count(), calls.len());
        for (actual, expected) in message.tool_calls().zip(calls) {
            assert_eq!(actual.tool_call_id.0, expected.tool_call_id);
            assert!(actual.input.parse() == expected.arguments);
        }
    }
}
