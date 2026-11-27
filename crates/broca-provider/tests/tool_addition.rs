use broca_core::plan_notes::{self, Note};
use broca_protocol::{
    tool_addition::{ToolAdditionForm, ToolDefinition},
    CallOptions, ContentBlock, Message, Role, Tool,
};
use broca_provider::{
    families::{
        anthropic_apikey_frozen_config, openai_chat_frozen_config, openai_responses_frozen_config,
        AnthropicMessages, OpenAiChat, OpenAiResponses,
    },
    wire::{AuthHandle, FrozenRenderConfig, WireFamily},
};
use serde_json::json;

fn tool(name: &str) -> ToolDefinition {
    ToolDefinition {
        name: name.into(),
        description: "Find a destination".into(),
        input_schema: json!({"type": "object"}).into(),
    }
}

fn options() -> CallOptions {
    let text =
        |role, text: &str| Message::new(role, vec![ContentBlock::Text { text: text.into() }]);
    let mut anchor = text(Role::User, "anchor");
    anchor.ordinal = Some(0);
    let mut opts = CallOptions::new(vec![text(Role::System, "Before"), anchor]);
    opts.tools = vec![Tool {
        name: "read".into(),
        description: "Read".into(),
        input_schema: json!({"type":"object"}).into(),
        module: None,
        execution_mode: Default::default(),
        reply: None,
    }]
    .into();
    opts
}

fn golden(
    family: &dyn WireFamily,
    config: &FrozenRenderConfig,
    notes: Vec<Note<'_>>,
    anchor: &str,
    note: &str,
) -> (Vec<u8>, Vec<u8>) {
    let base = options();
    let with_notes = plan_notes::view(&base, notes.into_iter().map(|note| (1, note))).unwrap();
    let before = family.render(config, &base);
    let before_str = std::str::from_utf8(&before).unwrap();
    assert_eq!(
        before_str.matches(anchor).count(),
        1,
        "the user anchor has pinned bytes"
    );
    let expected = before_str.replacen(anchor, &format!("{anchor},{note}"), 1);
    let after = family.render(config, &with_notes);
    assert_eq!(std::str::from_utf8(&after).unwrap(), expected);
    (before, after)
}

#[test]
fn responses_tool_addition_is_a_developer_additional_tools_item() {
    golden(
        &OpenAiResponses,
        &openai_responses_frozen_config("gpt-6-luna"),
        vec![Note::ToolAddition(
            ToolAdditionForm::ResponsesAdditionalTools,
            tool("find"),
        )],
        r#"{"content":"anchor","role":"user","type":"message"}"#,
        r#"{"role":"developer","tools":[{"description":"Find a destination","name":"find","parameters":{"type":"object"},"type":"function"}],"type":"additional_tools"}"#,
    );
}

#[test]
fn kimi_tool_addition_is_a_system_message_without_content() {
    let mut frozen = openai_chat_frozen_config("k3.1-pro");
    frozen.moonshot_schema_types = true;
    golden(
        &OpenAiChat,
        &frozen,
        vec![Note::ToolAddition(
            ToolAdditionForm::KimiSystemTools,
            tool("find"),
        )],
        r#"{"content":"anchor","role":"user"}"#,
        r#"{"role":"system","tools":[{"function":{"description":"Find a destination","name":"find","parameters":{"type":"object"}},"type":"function"}]}"#,
    );
}

const CLAUDE_ANCHOR: &str = r#"{"content":[{"text":"anchor","type":"text"}],"role":"user"}"#;

#[test]
fn anthropic_tool_addition_has_a_native_block_and_uses_the_beta() {
    let frozen = anthropic_apikey_frozen_config("claude-opus-5-5");
    let (before, after) = golden(
        &AnthropicMessages,
        &frozen,
        vec![Note::ToolAddition(
            ToolAdditionForm::AnthropicToolAddition,
            tool("find"),
        )],
        CLAUDE_ANCHOR,
        r#"{"content":[{"tool":{"definition":{"description":"Find a destination","input_schema":{"type":"object"},"name":"find"},"type":"tool_definition"},"type":"tool_addition"}],"role":"system"}"#,
    );
    let has_beta = |body, native| {
        let req =
            AnthropicMessages.endpoint_with_tool_notes(&frozen, &AuthHandle::None, body, native);
        req.headers.iter().any(|(name, value)| {
            name == "anthropic-beta"
                && value
                    .split(',')
                    .any(|beta| beta == "inline-tools-2026-09-15")
        })
    };
    assert!(!has_beta(before, false));
    assert!(has_beta(after, true));
}

#[test]
fn anthropic_tool_addition_groups_notes_at_one_anchor_in_note_order() {
    let frozen = anthropic_apikey_frozen_config("claude-opus-5-5");
    golden(
        &AnthropicMessages,
        &frozen,
        vec![
            Note::Text("New instructions"),
            Note::ToolAddition(ToolAdditionForm::AnthropicToolAddition, tool("find")),
            Note::Text("Tool removed"),
            Note::ToolAddition(ToolAdditionForm::AnthropicToolAddition, tool("locate")),
        ],
        CLAUDE_ANCHOR,
        concat!(
            r#"{"content":[{"text":"New instructions","type":"text"},"#,
            r#"{"tool":{"definition":{"description":"Find a destination","input_schema":{"type":"object"},"name":"find"},"type":"tool_definition"},"type":"tool_addition"},"#,
            r#"{"text":"Tool removed","type":"text"},"#,
            r#"{"tool":{"definition":{"description":"Find a destination","input_schema":{"type":"object"},"name":"locate"},"type":"tool_definition"},"type":"tool_addition"}],"role":"system"}"#,
        ),
    );
}

#[test]
fn grouped_owner_text_and_native_tool_stay_together_when_tool_choice_is_none() {
    use broca_protocol::request::ToolChoice;
    let mut opts = plan_notes::view(
        &options(),
        [
            (1, Note::Text("New instructions")),
            (
                1,
                Note::ToolAddition(ToolAdditionForm::AnthropicToolAddition, tool("find")),
            ),
            (1, Note::Text("Tool removed")),
        ],
    )
    .unwrap();
    opts.tool_choice = std::sync::Arc::new(ToolChoice::None);
    let mut frozen = anthropic_apikey_frozen_config("claude-opus-5-5");
    frozen.anthropic_turn_system = true;
    let body = AnthropicMessages.shape(&frozen, &opts);
    let parts = body["messages"][1]["content"].as_array().unwrap();
    assert_eq!(
        parts[0],
        serde_json::json!({"type":"text", "text":"New instructions"})
    );
    assert_eq!(parts[1]["type"], "tool_addition");
    assert_eq!(
        parts[2],
        serde_json::json!({"type":"text", "text":"Tool removed"})
    );
    assert_eq!(body["tools"].as_array().unwrap().len(), 1);
    assert_eq!(body["tool_choice"]["type"], "none");
}

#[test]
fn native_form_uses_catalog_system_support_instead_of_model_names() {
    use broca_provider::turn_system::tool_addition::frozen_form;
    let mut fable = anthropic_apikey_frozen_config("claude-fable-5-2");
    fable.anthropic_turn_system = true;
    assert_eq!(
        frozen_form(&fable, "anthropic"),
        Some(ToolAdditionForm::AnthropicToolAddition)
    );
    let mut unsupported = anthropic_apikey_frozen_config("claude-opus-5-5");
    unsupported.anthropic_turn_system = false;
    assert_eq!(frozen_form(&unsupported, "anthropic"), None);
}
