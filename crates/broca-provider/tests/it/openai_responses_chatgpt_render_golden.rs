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
        reply: None,
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
#[test]
fn frozen_profile_round_trips_without_legacy_defaults() {
    use broca_provider::chatgpt_wire::ChatgptWire;
    use broca_provider::families::openai_responses_chatgpt_frozen_config;
    let wire: ChatgptWire = serde_json::from_str(include_str!(
        "../fixtures/chatgpt_codex_0_163_identity.json"
    ))
    .unwrap();
    let legacy = openai_responses_chatgpt_frozen_config("gpt-5.3-codex");
    let legacy_blob = serde_json::to_value(&legacy).unwrap();
    assert!(legacy_blob.get("chatgpt_wire").is_none());
    let restored: broca_provider::wire::FrozenRenderConfig =
        serde_json::from_value(legacy_blob.clone()).unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), legacy_blob);
    let mut admitted = legacy;
    admitted.chatgpt_wire = Some(wire.clone());
    let restored: broca_provider::wire::FrozenRenderConfig =
        serde_json::from_value(serde_json::to_value(admitted).unwrap()).unwrap();
    assert_eq!(restored.chatgpt_wire, Some(wire));
    assert_eq!(restored.renderer_version, 8);
}

#[test]
fn codex_profile_plain_body_fixture() {
    let mut frozen = openai_responses_chatgpt_frozen_config("gpt-5.3-codex");
    frozen.chatgpt_wire = Some(
        serde_json::from_str(include_str!(
            "../fixtures/chatgpt_codex_0_163_identity.json"
        ))
        .unwrap(),
    );
    frozen.prompt_cache_key = Some("caller-cache-key".into());
    let opts = serde_json::from_value(json!({"prompt":[
        {"role":"system","content":[{"type":"text","text":"Be concise."}]},
        {"role":"user","content":[{"type":"text","text":"Hello"}]}
    ]}))
    .unwrap();
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/chatgpt_codex_0_163_plain.json")).unwrap();
    assert_eq!(
        OpenAiResponsesChatgpt.render(&frozen, &opts),
        broca_protocol::canonical_json_bytes(&expected)
    );
}
