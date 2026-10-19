//! OpenAI ChatGPT (codex) family conformance gate. The body is the OpenAI-Responses shape,
//! so this proves the codex family passes the same render-determinism + freeze checks.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall},
    request::{CallOptions, Tool, ToolChoice},
    ToolCallId,
};
use broca_provider::conformance::{check_freeze_against_mutation, check_render_determinism};
use broca_provider::families::{
    openai_responses_chatgpt_frozen_config, resolve_frozen, OpenAiResponsesChatgpt,
    OPENAI_RESPONSES_CHATGPT_RENDERER_VERSION,
};
use broca_provider::policy::{
    Quirks, ReasoningPolicy, SystemPromptPolicy, TokenLimitField, UsagePolicy,
};
use broca_provider::spec::{
    AuthKind, Capabilities, CapabilityFlag, CostSchedule, Limits, ModelSpec, Overlay, ProviderSpec,
    WireFamilyId,
};
use broca_provider::wire::WireFamily;
use serde_json::json;

fn rich_request() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::System,
            content: vec![ContentBlock::Text {
                text: "Sys.".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text { text: "Hi".into() }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Assistant,
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call_1".into()),
                tool_name: "a_tool".into(),
                input: json!({"x": 1}),
            })],
        },
    ]);
    opts.tools = (0..12)
        .map(|i| Tool { name: format!("tool_{i:02}"), description: format!("tool number {i}"), input_schema: json!({"type": "object", "properties": {format!("p{i}"): {"type": "string"}}}), module: None, execution_mode: Default::default() })
        .collect::<Vec<_>>()
        .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

#[test]
fn openai_responses_chatgpt_passes_render_determinism() {
    let frozen = openai_responses_chatgpt_frozen_config("gpt-5-codex");
    let report = check_render_determinism(&OpenAiResponsesChatgpt, &frozen, &rich_request());
    assert!(
        report.passed(),
        "conformance failures: {:?}",
        report.failures()
    );
}

fn chatgpt_spec() -> (ProviderSpec, ModelSpec) {
    let provider = ProviderSpec {
        id: "openai".into(),
        wire_family: WireFamilyId::OpenAiResponsesChatgpt,
        base_url: String::new(),
        auth_kind: AuthKind::Bearer,
        static_headers: Default::default(),
        static_options: serde_json::Value::Null,
        quirks: Quirks {
            usage: UsagePolicy::OpenAiUsage,
            token_limit_field: TokenLimitField::MaxOutputTokens,
            system_prompt: SystemPromptPolicy::InstructionsField,
            ..Default::default()
        },
        raw: serde_json::Value::Null,
    };
    let model = ModelSpec {
        id: "gpt-5-codex".into(),
        display_name: "GPT-5 Codex".into(),
        family: Some("gpt-5".into()),
        capabilities: Capabilities {
            tool_call: CapabilityFlag::Known(true),
            temperature: CapabilityFlag::Known(true),
            ..Default::default()
        },
        limits: Limits::default(),
        cost: CostSchedule::default(),
        reasoning_options: Default::default(),
        release_date: None,
        status: None,
        overlay: Overlay::default(),
        raw: serde_json::Value::Null,
    };
    (provider, model)
}

#[test]
fn chatgpt_requests_ciphertext_only_when_reasoning_is_enabled() {
    let plain = openai_responses_chatgpt_frozen_config("gpt-5-codex");
    let plain_body: serde_json::Value =
        serde_json::from_slice(&OpenAiResponsesChatgpt.render(&plain, &rich_request())).unwrap();
    assert!(plain_body.get("include").is_none());

    let mut reasoning = plain;
    reasoning.reasoning = ReasoningPolicy::OpenAiResponsesReasoning {
        effort: "medium".into(),
        summary: None,
    };
    let reasoning_body: serde_json::Value =
        serde_json::from_slice(&OpenAiResponsesChatgpt.render(&reasoning, &rich_request()))
            .unwrap();
    assert_eq!(
        reasoning_body["include"],
        json!(["reasoning.encrypted_content"])
    );
}

#[test]
fn openai_responses_chatgpt_freeze_survives_catalog_mutation() {
    let mut spec = chatgpt_spec();
    let result = check_freeze_against_mutation(
        &OpenAiResponsesChatgpt,
        &mut spec,
        &rich_request(),
        |(p, m)| {
            resolve_frozen(p, m, 2048, OPENAI_RESPONSES_CHATGPT_RENDERER_VERSION)
                .expect("test specs publish known capability flags")
        },
        |(p, m)| {
            m.id = "gpt-5-codex-NEW".into();
            p.quirks.token_limit_field = TokenLimitField::MaxTokens;
            p.quirks.system_prompt = SystemPromptPolicy::SystemMessage;
            p.static_options = json!({"injected": "after-freeze"});
        },
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
}
