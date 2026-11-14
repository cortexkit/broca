//! A family's `rendered_len` counts its request without writing it, and the loop's size
//! estimates read that count instead of rendering the request a second time. The count must be
//! the length `render` gives.

use std::sync::Arc;

use broca_protocol::{CallOptions, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult};
use broca_provider::{
    families::frozen_config_for_family, registry::wire_family, spec::WireFamilyId,
};

/// A transcript with what a count can get wrong: escapes and non-ASCII text, a tool input held
/// as JSON text, a JSON tool output, an error output and a declared tool.
fn options() -> CallOptions {
    let input = serde_json::json!({"command": "echo \"hi\"\n", "lines": [1, 2, 3], "nested": {"b": null, "a": "é😀"}});
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: "You are a helper.\tBe brief.".into(),
            }],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "quote \" backslash \\ control \u{1} accent é".into(),
            }],
        ),
        Message::new(
            Role::Assistant,
            vec![
                ContentBlock::Text {
                    text: "Running it.".into(),
                },
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("toolu_01".into()),
                    tool_name: "bash".into(),
                    input: (&input).into(),
                    thought_signature: None,
                }),
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("toolu_02".into()),
                    tool_name: "bash".into(),
                    input: (&input).into(),
                    thought_signature: None,
                }),
            ],
        ),
        Message::new(
            Role::Tool,
            vec![
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: broca_protocol::ToolCallId("toolu_01".into()),
                    output: ToolOutput::Json {
                        value: (&serde_json::json!({"exit": 0, "out": "hi\n"})).into(),
                    },
                    is_error: false,
                }),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: broca_protocol::ToolCallId("toolu_02".into()),
                    output: ToolOutput::Text {
                        text: "line one\nline two".into(),
                    },
                    is_error: true,
                }),
            ],
        ),
        Message::new(
            Role::Assistant,
            vec![ContentBlock::Text {
                text: "Done.".into(),
            }],
        ),
    ]);
    opts.tools = Arc::new(vec![serde_json::from_value(serde_json::json!({
        "name": "bash",
        "description": "Run a command.",
        "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}},
    }))
    .unwrap()]);
    opts
}

#[test]
fn every_family_counts_the_bytes_it_renders() {
    let opts = options();
    for family in [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::OpenAiChat,
        WireFamilyId::Gemini,
        WireFamilyId::BedrockConverse,
    ] {
        let frozen = frozen_config_for_family(family, "model");
        let renderer = wire_family(family).unwrap();
        let rendered = renderer.render(&frozen, &opts);
        assert!(rendered.len() > 500, "{family:?} rendered the transcript");
        assert_eq!(
            renderer.rendered_len(&frozen, &opts),
            rendered.len(),
            "{family:?}"
        );
    }
}
