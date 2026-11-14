//! The retired trial placement of rides on Anthropic Messages: ride texts held beside a tool message
//! (`TagSlot::rides_after_results`) render as their own text blocks after every tool result of
//! the turn, and the cache marker rule treats the last of them as the turn's last block. Keep
//! this coverage until stored runs admitted during the 12 November trial no longer need rendering.

use broca_protocol::content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult};
use broca_protocol::request::CallOptions;
use broca_protocol::ToolCallId;
use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
use broca_provider::wire::{CacheClass, CachePolicy, CacheTtl, MarkerSyntax, WireFamily};
use serde_json::{json, Value};

fn call(id: &str) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: "read".into(),
        input: json!({ "path": id }).into(),
        thought_signature: None,
    })
}

fn result(id: &str, text: &str) -> ContentBlock {
    ContentBlock::ToolResult(ToolResult {
        tool_call_id: ToolCallId(id.into()),
        output: ToolOutput::Text { text: text.into() },
        is_error: false,
    })
}

/// A tool turn answering `c1` and `c2`, whose results' outputs are `outputs`, holding `held`
/// beside it.
fn prompt(outputs: [&str; 2], held: &[&str]) -> Vec<Message> {
    let mut tools = Message::new(
        Role::Tool,
        vec![result("c1", outputs[0]), result("c2", outputs[1])],
    );
    tools
        .tags
        .set_rides_after_results(held.iter().map(|text| (*text).to_owned()).collect());
    vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "read both".into(),
            }],
        ),
        Message::new(Role::Assistant, vec![call("c1"), call("c2")]),
        tools,
    ]
}

fn cached() -> broca_provider::wire::FrozenRenderConfig {
    let mut frozen = anthropic_frozen_config("m");
    frozen.cache = Some(CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl: CacheTtl::FiveMinutes,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    });
    frozen
}

fn tool_turn(prompt: Vec<Message>) -> String {
    let body: Value =
        serde_json::from_slice(&AnthropicMessages.render(&cached(), &CallOptions::new(prompt)))
            .unwrap();
    serde_json::to_string(&body["messages"][2]).unwrap()
}

/// One ride on the last result: today it ends the result's output; held, it is the turn's own
/// text block after the results, and carries the turn's cache marker.
#[test]
fn one_ride_renders_after_the_results() {
    assert_eq!(
        tool_turn(prompt(["one", "two\n\nnote q1"], &[])),
        r#"{"content":[{"content":"one","is_error":false,"tool_use_id":"c1","type":"tool_result"},{"cache_control":{"ttl":"5m","type":"ephemeral"},"content":"two\n\nnote q1","is_error":false,"tool_use_id":"c2","type":"tool_result"}],"role":"user"}"#
    );
    assert_eq!(
        tool_turn(prompt(["one", "two"], &["note q1"])),
        r#"{"content":[{"content":"one","is_error":false,"tool_use_id":"c1","type":"tool_result"},{"content":"two","is_error":false,"tool_use_id":"c2","type":"tool_result"},{"cache_control":{"ttl":"5m","type":"ephemeral"},"text":"note q1","type":"text"}],"role":"user"}"#
    );
}

/// Two rides on two different results: held, both follow every result, in the order given.
#[test]
fn two_rides_on_two_results_render_after_every_result() {
    assert_eq!(
        tool_turn(prompt(["one\n\nnote q1", "two\n\nnote q2"], &[])),
        r#"{"content":[{"content":"one\n\nnote q1","is_error":false,"tool_use_id":"c1","type":"tool_result"},{"cache_control":{"ttl":"5m","type":"ephemeral"},"content":"two\n\nnote q2","is_error":false,"tool_use_id":"c2","type":"tool_result"}],"role":"user"}"#
    );
    assert_eq!(
        tool_turn(prompt(["one", "two"], &["note q1", "note q2"])),
        r#"{"content":[{"content":"one","is_error":false,"tool_use_id":"c1","type":"tool_result"},{"content":"two","is_error":false,"tool_use_id":"c2","type":"tool_result"},{"text":"note q1","type":"text"},{"cache_control":{"ttl":"5m","type":"ephemeral"},"text":"note q2","type":"text"}],"role":"user"}"#
    );
}

/// A tool message holding no rides renders the bytes it always did, with or without cache
/// markers.
#[test]
fn a_tool_message_without_held_rides_is_unchanged() {
    let plain = prompt(["one", "two"], &[]);
    let mut held_none = prompt(["one", "two"], &[]);
    held_none[2].tags.set_rides_after_results(Vec::new());
    for frozen in [anthropic_frozen_config("m"), cached()] {
        assert_eq!(
            AnthropicMessages.render(&frozen, &CallOptions::new(plain.clone())),
            AnthropicMessages.render(&frozen, &CallOptions::new(held_none.clone()))
        );
    }
}

/// Held rides survive the message's tags being attached again, since they are not tags.
#[test]
fn held_rides_survive_retagging() {
    let mut tools = prompt(["one", "two"], &["note q1"]).remove(2);
    tools.set_tags(Some(std::sync::Arc::new(
        broca_protocol::tags::PartTags::all([4, 5]),
    )));
    assert_eq!(tools.tags.rides_after_results(), ["note q1"]);
}
