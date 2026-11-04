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
        turn_system: false,
        cache_prefix_blocks: None,
        origin: None,
        ordinal: None,
        tags: Default::default(),
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
        reply: None,
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

fn operator_options() -> CallOptions {
    let mut opts = options();
    let mut tool = opts.tools[0].clone();
    tool.name = "bash".into();
    let mut other = tool.clone();
    other.name = "read_file".into();
    opts.tools = vec![tool, other].into();
    let ContentBlock::ToolCall(call) = &mut opts.prompt[0].content[0] else {
        unreachable!()
    };
    call.tool_name = "bash".into();
    opts.tool_choice = ToolChoice::Tool {
        name: "bash".into(),
    }
    .into();
    opts
}

#[test]
fn operator_aliases_render_tools_history_and_named_choice_in_responses_and_chat() {
    let opts = operator_options();
    let mut responses = openai_responses_frozen_config("synthetic");
    responses
        .tool_name_aliases
        .insert("bash".into(), "client_bash".into());
    let body = OpenAiResponses.shape(&responses, &opts);
    assert_eq!(body["tools"][0]["name"], "client_bash");
    assert_eq!(body["tools"][1]["name"], "read_file");
    assert_eq!(body["input"][0]["name"], "client_bash");
    assert_eq!(body["tool_choice"]["name"], "client_bash");
    let mut chat = openai_chat_frozen_config("synthetic");
    chat.tool_name_aliases = responses.tool_name_aliases.clone();
    let body = OpenAiChat.shape(&chat, &opts);
    assert_eq!(body["tools"][0]["function"]["name"], "client_bash");
    assert_eq!(body["tools"][1]["function"]["name"], "read_file");
    assert_eq!(
        body["messages"][0]["tool_calls"][0]["function"]["name"],
        "client_bash"
    );
    assert_eq!(body["tool_choice"]["function"]["name"], "client_bash");
    assert_eq!(opts.tools[0].name, "bash");
}

#[test]
fn operator_aliases_restore_responses_stream_items_arguments_and_final_message() {
    use broca_protocol::{
        provider::{NormalizedEvent, ProviderControl},
        StepId,
    };
    let opts = operator_options();
    let mut frozen = openai_responses_frozen_config("synthetic");
    frozen
        .tool_name_aliases
        .insert("bash".into(), "client_bash".into());
    let map = ToolNameMap::build_with_aliases(&opts.tools, &opts.prompt, &frozen.tool_name_aliases);
    for item_event in ["response.output_item.added", "response.output_item.done"] {
        let mut normalizer = OpenAiResponses.normalizer(&frozen, StepId(1));
        let frames = [
            json!({"type":item_event,"output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"call-1","name":"client_bash","arguments":"{}"}}),
            json!({"type":"response.function_call_arguments.done","output_index":0,"item_id":"fc_1","name":"client_bash","arguments":"{}"}),
            json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"function_call","call_id":"call-1","name":"client_bash","arguments":"{}"}],"usage":{"input_tokens":1,"output_tokens":1}}}),
        ];
        let mut events = Vec::new();
        for frame in frames {
            let bytes = format!(
                "event: {}\ndata: {}\n\n",
                frame["type"].as_str().unwrap(),
                frame
            );
            events.extend(
                normalizer
                    .push(bytes.as_bytes())
                    .unwrap()
                    .into_iter()
                    .map(|event| map.restore_event(event)),
            );
        }
        events.extend(
            normalizer
                .finish()
                .unwrap()
                .into_iter()
                .map(|event| map.restore_event(event)),
        );
        let calls: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    Some(message.tool_calls())
                }
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(calls.len(), 1, "{item_event}");
        assert_eq!(calls[0].tool_name, "bash");
    }
}

#[test]
fn operator_aliases_empty_map_is_omitted_and_composes_with_grammar_aliases() {
    let frozen = openai_responses_frozen_config("synthetic");
    assert!(serde_json::to_value(&frozen)
        .unwrap()
        .get("tool_name_aliases")
        .is_none());
    let mut opts = operator_options();
    let mut tools = (*opts.tools).clone();
    tools.push(options().tools[0].clone());
    opts.tools = tools.into();
    let aliases = [("bash".into(), "client_bash".into())].into();
    let map = ToolNameMap::build_with_aliases(&opts.tools, &opts.prompt, &aliases);
    assert_eq!(map.wire_name("bash"), "client_bash");
    assert_eq!(map.wire_name(CANONICAL), normalize(CANONICAL));
    assert_eq!(map.canonical_name("client_bash"), "bash");
    assert_eq!(map.canonical_name("bash"), "bash");
    assert_eq!(map.canonical_name("unknown"), "unknown");
}

#[test]
fn operator_alias_validation_uses_the_selected_family_name_grammar() {
    use broca_provider::{spec::WireFamilyId, tool_name_alias::validate_operator_aliases};
    let opts = operator_options();
    let aliases = [("bash".into(), "1client_bash".into())].into();
    assert!(
        validate_operator_aliases(WireFamilyId::OpenAiResponses, &opts.tools, &aliases).is_ok()
    );
    for family in [WireFamilyId::Gemini, WireFamilyId::GoogleCodeAssist] {
        let error = validate_operator_aliases(family, &opts.tools, &aliases).unwrap_err();
        assert_eq!(error.tool, "bash");
        assert!(error.reason.contains("valid tool name"));
    }
}

#[test]
fn operator_aliases_cannot_lose_reverse_mapping_to_a_historical_tool() {
    let mut opts = operator_options();
    let ContentBlock::ToolCall(call) = &mut opts.prompt[0].content[0] else {
        unreachable!()
    };
    call.tool_name = "client_bash".into();
    let aliases = [("bash".into(), "client_bash".into())].into();
    let map = ToolNameMap::build_with_aliases(&opts.tools, &opts.prompt, &aliases);
    assert_eq!(map.canonical_name("client_bash"), "bash");
    assert_ne!(map.wire_name("client_bash"), "client_bash");
    assert_eq!(
        map.canonical_name(map.wire_name("client_bash")),
        "client_bash"
    );
}
