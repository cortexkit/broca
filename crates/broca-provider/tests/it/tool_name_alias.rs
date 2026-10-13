use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall},
    request::{CallOptions, Tool, ToolChoice},
    ToolCallId,
};
use broca_provider::{
    families::{
        anthropic_frozen_config, bedrock_frozen_config, gemini_frozen_config,
        openai_chat_frozen_config, openai_responses_frozen_config, AnthropicMessages,
        BedrockConverse, Gemini, OpenAiChat, OpenAiResponses,
    },
    tool_name_alias::{normalize, ToolNameMap},
    wire::WireFamily,
};
use serde_json::{json, Value};

const CANONICAL: &str = "mcp.server/list-files";

fn options() -> CallOptions {
    let mut options = CallOptions::new(vec![Message {
        cache_prefix_blocks: None,
        origin: None,
        ordinal: None,
        role: Role::Assistant,
        content: vec![ContentBlock::ToolCall(ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId("call-1".into()),
            tool_name: CANONICAL.into(),
            input: json!({}),
        })],
    }]);
    options.tools = vec![Tool {
        name: CANONICAL.into(),
        description: "List files".into(),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: Default::default(),
    }]
    .into();
    options.tool_choice = ToolChoice::Auto.into();
    options
}

fn body<F: WireFamily>(family: F, frozen: broca_provider::wire::FrozenRenderConfig) -> Value {
    serde_json::from_slice(&family.render(&frozen, &options())).expect("rendered JSON")
}

fn assert_alias(value: &Value, path: &[&str], alias: &str) {
    let mut current = value;
    for segment in path {
        current = if let Ok(index) = segment.parse::<usize>() {
            &current[index]
        } else {
            &current[*segment]
        };
    }
    assert_eq!(current, alias, "path {path:?}");
}

#[test]
fn invalid_name_is_sanitized_in_each_wire_family() {
    let alias = normalize(CANONICAL);
    assert_ne!(alias, CANONICAL);
    assert!(alias.len() <= 64);

    let anthropic = body(
        AnthropicMessages,
        anthropic_frozen_config("claude-sonnet-4-5"),
    );
    assert_alias(&anthropic, &["tools", "0", "name"], &alias);
    assert_alias(
        &anthropic,
        &["messages", "0", "content", "0", "name"],
        &alias,
    );

    let openai_chat = body(OpenAiChat, openai_chat_frozen_config("gpt-4o"));
    assert_alias(&openai_chat, &["tools", "0", "function", "name"], &alias);
    assert_alias(
        &openai_chat,
        &["messages", "0", "tool_calls", "0", "function", "name"],
        &alias,
    );

    let openai_responses = body(OpenAiResponses, openai_responses_frozen_config("gpt-4o"));
    assert_alias(&openai_responses, &["tools", "0", "name"], &alias);
    assert_alias(&openai_responses, &["input", "0", "name"], &alias);

    let gemini = body(Gemini, gemini_frozen_config("gemini-2.5-flash"));
    assert_alias(
        &gemini,
        &["tools", "0", "functionDeclarations", "0", "name"],
        &alias,
    );
    assert_alias(
        &gemini,
        &["contents", "0", "parts", "0", "functionCall", "name"],
        &alias,
    );

    let bedrock = body(BedrockConverse, bedrock_frozen_config("claude-3-5"));
    assert_alias(
        &bedrock,
        &["toolConfig", "tools", "0", "toolSpec", "name"],
        &alias,
    );
    assert_alias(
        &bedrock,
        &["messages", "0", "content", "0", "toolUse", "name"],
        &alias,
    );
}

#[test]
fn canonical_name_map_restores_a_model_echo_before_dispatch() {
    let map = ToolNameMap::from_names([CANONICAL]);
    let wire = map.wire_name(CANONICAL);
    assert_eq!(map.canonical_name(wire), CANONICAL);
}
