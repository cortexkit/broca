//! G2 — `ReasoningContentRequired` (codec.md §6.3), default-off mechanism.
//!
//! An OpenAI-compatible subset (DeepSeek-reasoner, Kimi, Moonshot) rejects a tool-call
//! assistant message that carries no `reasoning_content`. When the provider opts in, the
//! OpenAI-Chat render echoes stored reasoning on a tool-call assistant turn and supplies an
//! empty `reasoning_content: ""` only when that turn lacks stored reasoning. This is DEFAULT-OFF:
//! no catalog provider is opted in, so a normal OpenAI-Chat request is unchanged. These tests
//! prove (a) off = byte-identical to the legacy reasoning-dropping shape and (b) on = exact echo
//! with the empty fallback preserved.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall},
    request::CallOptions,
    ToolCallId,
};
use broca_provider::families::{openai_chat_frozen_config, OpenAiChat};
use broca_provider::wire::WireFamily;
use serde_json::{json, Value};

/// A prompt whose assistant turn calls a tool (the turn that needs `reasoning_content`),
/// plus a plain-text assistant turn (which must NOT get the field).
fn prompt() -> Vec<Message> {
    vec![
        Message::new(Role::User, vec![ContentBlock::Text { text: "hi".into() }]),
        Message::assistant_from(
            vec![ContentBlock::Text {
                text: "answer".into(),
            }],
            broca_protocol::request::ModelHandle::new("deepseek", "deepseek-v4-flash"),
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "now use a tool".into(),
            }],
        ),
        Message::assistant_from(
            vec![
                ContentBlock::Reasoning {
                    text: "I should check the clock exactly.".into(),
                    signature: None,
                },
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call_1".into()),
                    tool_name: "get_time".into(),
                    input: json!({}),
                }),
            ],
            broca_protocol::request::ModelHandle::new("deepseek", "deepseek-v4-flash"),
        ),
    ]
}

fn render(reasoning_content_required: bool) -> Value {
    let mut frozen = openai_chat_frozen_config("deepseek-v4-flash");
    frozen.reasoning_content_required = reasoning_content_required;
    serde_json::from_slice(&OpenAiChat.render(&frozen, &CallOptions::new(prompt()))).unwrap()
}

fn assistant_messages(body: &Value) -> Vec<Value> {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "assistant")
        .cloned()
        .collect()
}

#[test]
fn default_off_renders_no_reasoning_content() {
    let body = render(false);
    for m in assistant_messages(&body) {
        assert!(
            m.get("reasoning_content").is_none(),
            "default-off must NOT emit reasoning_content on any assistant turn: {m:#?}"
        );
    }
}

#[test]
fn enabled_echoes_stored_reasoning_only_on_tool_call_turns() {
    let body = render(true);
    let assistants = assistant_messages(&body);
    let tool_turn = assistants
        .iter()
        .find(|m| m.get("tool_calls").is_some())
        .expect("a tool-call assistant turn");
    assert_eq!(
        tool_turn["reasoning_content"],
        json!("I should check the clock exactly."),
        "an opted-in tool-call assistant turn must echo stored reasoning verbatim"
    );

    // The plain-text assistant turn (no tool_calls) must NOT get the field.
    let text_turn = assistants
        .iter()
        .find(|m| m.get("tool_calls").is_none())
        .expect("a text assistant turn");
    assert!(
        text_turn.get("reasoning_content").is_none(),
        "a non-tool-call assistant turn must NOT get reasoning_content: {text_turn:#?}"
    );
}

#[test]
fn enabled_keeps_empty_reasoning_fallback_for_tool_call_turn_without_reasoning() {
    let mut frozen = openai_chat_frozen_config("deepseek-v4-flash");
    frozen.reasoning_content_required = true;
    let body: Value = serde_json::from_slice(
        &OpenAiChat.render(&frozen, &CallOptions::new(tool_call_only_prompt())),
    )
    .unwrap();
    assert_eq!(
        tool_call_assistant(&body)["reasoning_content"],
        json!(""),
        "an opted-in tool-call turn without stored reasoning keeps the empty fallback"
    );
}

#[test]
fn default_off_reasoning_blocks_match_the_legacy_dropped_render_bytes() {
    let with_reasoning = prompt();
    let mut legacy_prompt = with_reasoning.clone();
    for message in &mut legacy_prompt {
        message
            .content
            .retain(|block| !matches!(block, ContentBlock::Reasoning { .. }));
    }

    let frozen = openai_chat_frozen_config("deepseek-v4-flash");
    assert_eq!(
        OpenAiChat.render(&frozen, &CallOptions::new(with_reasoning)),
        OpenAiChat.render(&frozen, &CallOptions::new(legacy_prompt)),
        "quirk-off renders must stay byte-identical to the legacy reasoning-dropping shape"
    );
}

fn tool_call_only_prompt() -> Vec<Message> {
    vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "use a tool".into(),
            }],
        ),
        Message::assistant_from(
            vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call_1".into()),
                tool_name: "get_time".into(),
                input: json!({}),
            })],
            broca_protocol::request::ModelHandle::new("ollama-cloud", "test"),
        ),
    ]
}

fn render_tool_call_content(tool_call_content_empty_string: bool) -> Value {
    let mut frozen = openai_chat_frozen_config("test-model");
    frozen.tool_call_content_empty_string = tool_call_content_empty_string;
    serde_json::from_slice(&OpenAiChat.render(&frozen, &CallOptions::new(tool_call_only_prompt())))
        .unwrap()
}

fn tool_call_assistant(body: &Value) -> &Value {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["role"] == "assistant" && m.get("tool_calls").is_some())
        .expect("tool-call assistant turn")
}

#[test]
fn tool_call_content_default_off_renders_null() {
    let body = render_tool_call_content(false);
    let turn = tool_call_assistant(&body);
    assert!(
        turn["content"].is_null(),
        "default-off must emit content: null on a tool-call-only assistant turn: {turn:#?}"
    );
}

#[test]
fn tool_call_content_enabled_renders_empty_string() {
    let body = render_tool_call_content(true);
    let turn = tool_call_assistant(&body);
    assert_eq!(
        turn["content"],
        json!(""),
        "opted-in provider must emit content: \"\" on a tool-call-only assistant turn"
    );
}
