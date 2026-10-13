//! Proves the refactored `AnthropicMessages` wire family renders BYTE-IDENTICAL
//! output to the pinned golden — i.e. the family extraction did not change a single
//! rendered byte (a render refactor that changes any byte is a cache + resume bust).
//!
//! This is feature-free: render is pure (no transport), so it needs no `live`.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, Tool, ToolChoice},
    ToolCallId,
};
use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
use broca_provider::wire::WireFamily;
use serde_json::json;

/// The SAME fixed request as the pre-refactor golden test, so the golden file is
/// the shared baseline both must reproduce.
fn fixed_request() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::System,
            content: vec![ContentBlock::Text {
                text: "Be concise.".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "What time is it in Madrid?".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::Assistant,
            content: vec![
                ContentBlock::Reasoning {
                    text: "older thinking".into(),
                    signature: Some("old-sig".into()),
                },
                ContentBlock::Text {
                    text: "Let me check.".into(),
                },
            ],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "Please.".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::Assistant,
            content: vec![
                ContentBlock::Reasoning {
                    text: "I should call get_time for Europe/Madrid.".into(),
                    signature: Some("sig-abc123".into()),
                },
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("toolu_01".into()),
                    tool_name: "get_time".into(),
                    input: json!({"timezone": "Europe/Madrid"}),
                }),
            ],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::Tool,
            content: vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("toolu_01".into()),
                output: ToolOutput::Text {
                    text: "14:32".into(),
                },
                is_error: false,
            })],
        },
    ]);
    opts.tools = vec![
        Tool {
            name: "get_time".into(),
            description: "Get the current time in an IANA timezone.".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "timezone": { "type": "string" } },
                "required": ["timezone"]
            }),
            module: None,
            execution_mode: Default::default(),
        },
        Tool {
            name: "get_weather".into(),
            description: "Get the weather for a city.".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "city": { "type": "string" } }
            }),
            module: None,
            execution_mode: Default::default(),
        },
    ]
    .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

const GOLDEN: &str = include_str!("../fixtures/anthropic_render_golden.json");

#[test]
fn anthropic_family_matches_pre_refactor_golden() {
    let frozen = anthropic_frozen_config("claude-opus-4-8");
    let bytes = AnthropicMessages.render(&frozen, &fixed_request());
    let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

    assert_eq!(
        rendered,
        GOLDEN.trim_end_matches('\n'),
        "The AnthropicMessages family render diverged from the pre-refactor golden — \
         the refactor changed the rendered bytes (a cache + resume bust)."
    );
}
