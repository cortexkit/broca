//! Golden-byte test for the OpenAI ChatGPT (codex) render. The body is the OpenAI-Responses
//! shape (the codex backend consumes it), so this golden also documents that the codex body
//! equals the platform Responses body. Regenerate with `BROCA_UPDATE_GOLDEN=1`.

use crate::render_golden_support;

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, Tool, ToolChoice},
    ToolCallId,
};
use broca_provider::families::{
    openai_responses_chatgpt_frozen_config, OpenAiResponsesChatgpt,
    OPENAI_RESPONSES_CHATGPT_RENDERER_VERSION,
};
use broca_provider::policy::ReasoningPolicy;
use broca_provider::wire::WireFamily;
use serde_json::json;

pub(crate) fn fixed_request() -> CallOptions {
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
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call_01".into()),
                tool_name: "get_time".into(),
                input: json!({"timezone": "Europe/Madrid"}),
            })],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::Tool,
            content: vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call_01".into()),
                output: ToolOutput::Text {
                    text: "14:32".into(),
                },
                is_error: false,
            })],
        },
    ]);
    opts.tools = vec![Tool {
        name: "get_time".into(),
        description: "Get the current time in an IANA timezone.".into(),
        input_schema: json!({
            "type": "object",
            "properties": { "timezone": { "type": "string" } },
            "required": ["timezone"]
        }),
        module: None,
        execution_mode: Default::default(),
    }]
    .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

pub(crate) const GOLDEN: &str =
    include_str!("../fixtures/openai_responses_chatgpt_render_golden.json");
pub(crate) const GOLDEN_VERSION: &str =
    include_str!("../fixtures/openai_responses_chatgpt_render_golden.json.version");

/// The frozen config the golden bytes are rendered with.
pub(crate) fn golden_frozen() -> broca_provider::wire::FrozenRenderConfig {
    let mut frozen = openai_responses_chatgpt_frozen_config("gpt-5-codex");
    frozen.reasoning = ReasoningPolicy::OpenAiResponsesReasoning {
        effort: "medium".into(),
        summary: None,
    };
    frozen
}

#[test]
fn openai_responses_chatgpt_render_matches_golden() {
    let frozen = golden_frozen();
    let bytes = OpenAiResponsesChatgpt.render(&frozen, &fixed_request());
    let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

    if render_golden_support::update_if_requested(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/openai_responses_chatgpt_render_golden.json"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/openai_responses_chatgpt_render_golden.json.version"
        ),
        rendered.as_bytes(),
        GOLDEN.as_bytes(),
        GOLDEN_VERSION,
        OPENAI_RESPONSES_CHATGPT_RENDERER_VERSION,
        "OpenAI Responses ChatGPT",
    ) {
        return;
    }

    render_golden_support::assert_matches_or_requires_bump(
        rendered.as_bytes(),
        GOLDEN.as_bytes(),
        GOLDEN_VERSION,
        OPENAI_RESPONSES_CHATGPT_RENDERER_VERSION,
        "OpenAI Responses ChatGPT",
    );
}
