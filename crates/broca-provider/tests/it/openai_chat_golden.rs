//! Golden-byte test for the OpenAI-Chat render. Pins the rendered bytes for a fixed
//! request to a committed golden; a change to the bytes (intended or not) fails this
//! test. An intentional change requires BROCA_UPDATE_GOLDEN=1 + bumping
//! OPENAI_CHAT_RENDERER_VERSION.

use crate::render_golden_support;

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, Tool, ToolChoice},
    ToolCallId,
};
use broca_provider::families::{
    openai_chat_frozen_config, OpenAiChat, OPENAI_CHAT_RENDERER_VERSION,
};
use broca_provider::wire::{CacheClass, CachePolicy, WireFamily};
use serde_json::json;

/// A fixed request exercising the OpenAI-specific shapes: a system message (leading
/// `system` role, not a top-level field), a user turn, an assistant turn with a tool
/// call (`tool_calls[]` with `function.arguments` as a STRING), a tool-result turn
/// (`role:"tool"`), and two tools (`{type:function, function:{...parameters}}`).
pub(super) fn fixed_request() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::System,
            content: vec![ContentBlock::Text {
                text: "Be concise.".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "What time is it in Madrid?".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
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
            tags: Default::default(),
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
            reply: None,
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
            reply: None,
        },
    ]
    .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

pub(super) const GOLDEN: &str = include_str!("../fixtures/openai_chat_render_golden.json");
const GOLDEN_VERSION: &str = include_str!("../fixtures/openai_chat_render_golden.json.version");

#[test]
fn openai_chat_render_matches_golden() {
    let frozen = openai_chat_frozen_config("gpt-4o");
    let bytes =
        render_golden_support::render_without_bundle(&OpenAiChat, &frozen, &fixed_request());
    let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

    if render_golden_support::update_if_requested(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/openai_chat_render_golden.json"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/openai_chat_render_golden.json.version"
        ),
        rendered.as_bytes(),
        GOLDEN.as_bytes(),
        GOLDEN_VERSION,
        OPENAI_CHAT_RENDERER_VERSION,
        "OpenAI-Chat",
    ) {
        return;
    }

    render_golden_support::assert_matches_or_requires_bump(
        rendered.as_bytes(),
        GOLDEN.as_bytes(),
        GOLDEN_VERSION,
        OPENAI_CHAT_RENDERER_VERSION,
        "OpenAI-Chat",
    );
}

fn hints_policy() -> CachePolicy {
    CachePolicy {
        version: 1,
        class: CacheClass::Hints {
            cache_key: "session-cache-key".into(),
            retention: Some("in-memory".into()),
        },
    }
}

const HINTS_GOLDEN: &str = include_str!("../fixtures/openai_chat_cache_hints_render_golden.json");
const HINTS_GOLDEN_VERSION: &str =
    include_str!("../fixtures/openai_chat_cache_hints_render_golden.json.version");
const CALLER_SCOPE_GOLDEN: &str =
    include_str!("../fixtures/openai_chat_caller_cache_scope_render_golden.json");
const CALLER_SCOPE_GOLDEN_VERSION: &str =
    include_str!("../fixtures/openai_chat_caller_cache_scope_render_golden.json.version");

fn assert_golden(rendered: String, golden: &str, version: &str, golden_path: &str, name: &str) {
    let version_path = format!("{golden_path}.version");
    if render_golden_support::update_if_requested(
        golden_path,
        &version_path,
        rendered.as_bytes(),
        golden.as_bytes(),
        version,
        OPENAI_CHAT_RENDERER_VERSION,
        name,
    ) {
        return;
    }

    render_golden_support::assert_matches_or_requires_bump(
        rendered.as_bytes(),
        golden.as_bytes(),
        version,
        OPENAI_CHAT_RENDERER_VERSION,
        name,
    );
}

#[test]
fn openai_chat_cache_hints_render_matches_golden() {
    let mut frozen = openai_chat_frozen_config("gpt-4o");
    frozen.cache = Some(hints_policy());
    assert_golden(
        String::from_utf8(OpenAiChat.render(&frozen, &fixed_request())).unwrap(),
        HINTS_GOLDEN,
        HINTS_GOLDEN_VERSION,
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/openai_chat_cache_hints_render_golden.json"
        ),
        "OpenAI-Chat cache hints",
    );
}

#[test]
fn openai_chat_caller_cache_scope_render_matches_golden() {
    let mut frozen = openai_chat_frozen_config("gpt-4o");
    frozen.prompt_cache_key = Some("alfonso-classify-v1".into());
    assert_golden(
        String::from_utf8(OpenAiChat.render(&frozen, &fixed_request())).unwrap(),
        CALLER_SCOPE_GOLDEN,
        CALLER_SCOPE_GOLDEN_VERSION,
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/openai_chat_caller_cache_scope_render_golden.json"
        ),
        "OpenAI-Chat caller cache scope",
    );
}

#[test]
fn caller_cache_scope_overrides_hints_key_but_keeps_retention() {
    let mut frozen = openai_chat_frozen_config("gpt-4o");
    frozen.cache = Some(hints_policy());
    frozen.prompt_cache_key = Some("alfonso-classify-v1".into());
    let body: serde_json::Value =
        serde_json::from_slice(&OpenAiChat.render(&frozen, &fixed_request())).unwrap();
    assert_eq!(body["prompt_cache_key"], json!("alfonso-classify-v1"));
    assert_eq!(body["prompt_cache_retention"], json!("in_memory"));
}

/// A frozen 24h retention renders as `prompt_cache_retention: "24h"` and changes nothing else:
/// the bytes are the in-memory golden with only the retention value swapped. A request that
/// names the 24h lifetime explicitly freezes this same policy, so it renders these same bytes.
#[test]
fn openai_chat_24h_retention_renders_verbatim() {
    let mut frozen = openai_chat_frozen_config("gpt-4o");
    frozen.cache = Some(CachePolicy {
        version: 1,
        class: CacheClass::Hints {
            cache_key: "session-cache-key".into(),
            retention: Some("24h".into()),
        },
    });
    let rendered = String::from_utf8(OpenAiChat.render(&frozen, &fixed_request())).unwrap();
    let expected = HINTS_GOLDEN.replace(
        r#""prompt_cache_retention":"in_memory""#,
        r#""prompt_cache_retention":"24h""#,
    );
    assert_ne!(
        expected, HINTS_GOLDEN,
        "the golden must carry a retention to swap"
    );
    assert_eq!(rendered, expected.trim_end_matches('\n'));
}
