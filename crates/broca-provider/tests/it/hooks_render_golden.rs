//! A preserving append edits an existing text part, never inserts a part beside a tool call.
//! A tool-call-only message has no text block to edit, so an append to it is refused
//! (block out of range: the hook is unavailable) and the message renders byte-identical.
use broca_protocol::{CallOptions, ContentBlock, Message, Role, ToolCall, ToolCallId};
use broca_provider::{
    families::{
        anthropic_frozen_config, gemini_frozen_config, openai_responses_frozen_config,
        AnthropicMessages, Gemini, OpenAiResponses,
    },
    wire::WireFamily,
};
use serde_json::json;

fn request() -> CallOptions {
    let mut content = vec![
        ContentBlock::Text {
            text: "first".into(),
        },
        ContentBlock::Text {
            text: "second".into(),
        },
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId("call_1".into()),
            tool_name: "read".into(),
            input: json!({"path":"a"}),
            thought_signature: None,
        }),
        ContentBlock::Text {
            text: "tail".into(),
        },
    ];
    let ops: Vec<_> =
        serde_json::from_value(json!([{"op":"append","block":1,"text":"+hook"}])).unwrap();
    broca_core::hooks::apply_message_ops(&mut content, &ops).unwrap();
    CallOptions::new(vec![Message {
        role: Role::Assistant,
        content,
        origin: None,
        ordinal: None,
        tags: Default::default(),
        cache_prefix_blocks: None,
    }])
}
/// A tool-call-only assistant turn. The append names text block 0, which does not exist,
/// so the splice refuses it and leaves every part untouched.
fn tool_call_only_request() -> CallOptions {
    let mut content = vec![ContentBlock::ToolCall(ToolCall {
        tool_call_id: ToolCallId("call_1".into()),
        tool_name: "read".into(),
        input: json!({"path":"a"}),
        thought_signature: None,
    })];
    let before = content.clone();
    let ops: Vec<_> =
        serde_json::from_value(json!([{"op":"append","block":0,"text":"+hook"}])).unwrap();
    assert!(broca_core::hooks::apply_message_ops(&mut content, &ops).is_err());
    assert_eq!(content, before);
    CallOptions::new(vec![Message {
        role: Role::Assistant,
        content,
        origin: None,
        ordinal: None,
        tags: Default::default(),
        cache_prefix_blocks: None,
    }])
}
fn golden(bytes: Vec<u8>, expected: &str) {
    assert_eq!(String::from_utf8(bytes).unwrap(), expected.trim_end());
}
#[test]
fn anthropic_text_plus_tool_append_golden() {
    golden(
        AnthropicMessages.render(&anthropic_frozen_config("claude-opus-4-8"), &request()),
        include_str!("../fixtures/hooks_anthropic.json"),
    );
}
#[test]
fn gemini_text_plus_tool_append_golden() {
    golden(
        Gemini.render(&gemini_frozen_config("gemini-2.0-flash"), &request()),
        include_str!("../fixtures/hooks_gemini.json"),
    );
}
#[test]
fn responses_text_plus_tool_append_golden() {
    golden(
        OpenAiResponses.render(&openai_responses_frozen_config("gpt-4.1"), &request()),
        include_str!("../fixtures/hooks_responses.json"),
    );
}
#[test]
fn anthropic_tool_call_only_append_golden() {
    golden(
        AnthropicMessages.render(
            &anthropic_frozen_config("claude-opus-4-8"),
            &tool_call_only_request(),
        ),
        include_str!("../fixtures/hooks_tool_only_anthropic.json"),
    );
}
#[test]
fn gemini_tool_call_only_append_golden() {
    golden(
        Gemini.render(
            &gemini_frozen_config("gemini-2.0-flash"),
            &tool_call_only_request(),
        ),
        include_str!("../fixtures/hooks_tool_only_gemini.json"),
    );
}
#[test]
fn responses_tool_call_only_append_golden() {
    golden(
        OpenAiResponses.render(
            &openai_responses_frozen_config("gpt-4.1"),
            &tool_call_only_request(),
        ),
        include_str!("../fixtures/hooks_tool_only_responses.json"),
    );
}
