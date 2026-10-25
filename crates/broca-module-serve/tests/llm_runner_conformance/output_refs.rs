//! Script output references through Messages, whose native content blocks preserve each
//! published sample's boundaries. Chat text deltas concatenate and cannot model that fixture.

use super::*;
use cortexkit_role_llm_runner_conformance::ScriptedTurn;

pub(super) const PROVIDER: &str = "anthropic";
pub(super) const MODEL: &str = "conformance-output-refs";

pub(super) fn catalog_row() -> Value {
    json!({"capability.attachment":false,"capability.reasoning":true,
        "capability.tool_call":true,"existence":"present","limit.context":128000,
        "wire.npm":{"value":"@ai-sdk/anthropic","from":"fixture"},
        "wire.api":{"value":"https://conformance.invalid/v1","from":"fixture"}})
}

pub(super) fn stream(id: &str, turn: &ScriptedTurn) -> String {
    let mut body = String::new();
    let mut event = |value: Value| {
        body.push_str(&format!(
            "event: {}\ndata: {value}\n\n",
            value["type"].as_str().unwrap()
        ));
    };
    event(
        json!({"type":"message_start", "message":{"id":id,"type":"message","role":"assistant","model":MODEL,
        "content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}),
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
        event(json!({"type":"content_block_start","index":index,"content_block":block}));
        event(json!({"type":"content_block_delta","index":index,"delta":delta}));
        event(json!({"type":"content_block_stop","index":index}));
    }
    event(
        json!({"type":"message_delta","delta":{"stop_reason":if calls {"tool_use"} else {"end_turn"},"stop_sequence":null},"usage":{"output_tokens":1}}),
    );
    event(json!({"type":"message_stop"}));
    body
}
