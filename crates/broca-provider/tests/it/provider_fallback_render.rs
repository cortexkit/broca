//! Anthropic's server-side fallback handoff (`ContentBlock::ProviderFallback`) is stored in the
//! transcript. Only an Anthropic request whose frozen config opted into server-side fallback may
//! echo it; every other target drops it, and must render a transcript holding the part byte for
//! byte as it renders the same transcript without it.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    ids::ToolCallId,
    request::{CallOptions, ModelHandle, StepContext},
};
use broca_provider::families::{
    anthropic_apikey_frozen_config, anthropic_frozen_config, bedrock_frozen_config,
    gemini_frozen_config, google_codeassist_frozen_config, openai_chat_frozen_config,
    openai_responses_chatgpt_frozen_config, openai_responses_frozen_config, AnthropicMessages,
    BedrockConverse, Gemini, GoogleCodeAssist, OpenAiChat, OpenAiResponses, OpenAiResponsesChatgpt,
};
use broca_provider::policy::ReasoningEchoPolicy;
use broca_provider::wire::{FrozenRenderConfig, WireFamily};

const TO_MODEL: &str = "claude-handoff-target-9";

fn handoff() -> ContentBlock {
    ContentBlock::ProviderFallback {
        from_model: "claude-handoff-source-9".into(),
        to_model: TO_MODEL.into(),
    }
}

/// A tool turn whose assistant message holds the handoff between two text parts, and a final
/// answer that starts with one.
fn prompt(with_handoff: bool) -> CallOptions {
    let origin = ModelHandle::new("anthropic", "claude-handoff-source-9");
    let mut first = vec![
        ContentBlock::Text {
            text: "Let me".into(),
        },
        ContentBlock::Text {
            text: " check.".into(),
        },
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId("call_1".into()),
            tool_name: "lookup".into(),
            input: serde_json::json!({"id": 1}),
            thought_signature: None,
        }),
    ];
    let mut last = vec![ContentBlock::Text {
        text: "Found it.".into(),
    }];
    if with_handoff {
        first.insert(1, handoff());
        last.insert(0, handoff());
    }
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Find record 1.".into(),
            }],
        ),
        Message::assistant_from(first, origin.clone()),
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call_1".into()),
                output: ToolOutput::Text {
                    text: "record 1".into(),
                },
                is_error: false,
            })],
        ),
        Message::assistant_from(last, origin),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Thanks.".into(),
            }],
        ),
    ]);
    // Code Assist derives its request id from the step; the other families ignore it.
    opts.step_context = Some(StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "run-handoff".into(),
        step_index: 3,
        max_output_tokens: None,
    });
    opts
}

fn assert_dropped<F: WireFamily>(family: &F, frozen: &FrozenRenderConfig, name: &str) {
    let with = family.render(frozen, &prompt(true));
    let without = family.render(frozen, &prompt(false));
    assert_eq!(
        String::from_utf8_lossy(&with),
        String::from_utf8_lossy(&without),
        "{name}: the handoff part must not change the rendered request"
    );
    assert!(
        !String::from_utf8_lossy(&with).contains(TO_MODEL),
        "{name}: the handoff's model id must not reach the request"
    );
}

#[test]
fn anthropic_oauth_without_the_opt_in_drops_the_handoff() {
    assert_dropped(
        &AnthropicMessages,
        &anthropic_frozen_config("claude-opus-5-5"),
        "anthropic oauth",
    );
}

#[test]
fn anthropic_with_the_opt_in_echoes_the_handoff_at_its_position() {
    // The opt-in and its render version are frozen together; a config opts in at v3.
    let mut frozen = anthropic_frozen_config("claude-opus-5-5");
    frozen.server_side_fallback = true;
    frozen.renderer_version = broca_provider::families::anthropic_renderer_version(true);
    let body: serde_json::Value =
        serde_json::from_slice(&AnthropicMessages.render(&frozen, &prompt(true))).unwrap();
    let message = &body["messages"][1];
    assert_eq!(message["role"], "assistant");
    let content = message["content"].as_array().unwrap();
    // The tool turn's text, the handoff between its two parts, then the tool call.
    assert_eq!(content[0]["text"], "Let me");
    assert_eq!(
        content[1],
        serde_json::json!({
            "type": "fallback",
            "from": { "model": "claude-handoff-source-9" },
            "to": { "model": TO_MODEL },
        })
    );
    assert_eq!(content[2]["text"], " check.");
    assert_eq!(content[3]["type"], "tool_use");
    // Every assistant message carrying it is rendered, not just the current turn's.
    let last = &body["messages"][3]["content"].as_array().unwrap()[0];
    assert_eq!(last["type"], "fallback");
}

#[test]
fn anthropic_api_key_drops_the_handoff() {
    assert_dropped(
        &AnthropicMessages,
        &anthropic_apikey_frozen_config("claude-opus-5-5"),
        "anthropic api key",
    );
}

#[test]
fn openai_chat_drops_the_handoff() {
    assert_dropped(
        &OpenAiChat,
        &openai_chat_frozen_config("gpt-5"),
        "openai chat",
    );
}

#[test]
fn openrouter_drops_the_handoff() {
    let mut frozen = openai_chat_frozen_config("anthropic/claude-opus-5-5");
    frozen.reasoning_echo = Some(ReasoningEchoPolicy::OPENROUTER);
    assert_dropped(&OpenAiChat, &frozen, "openrouter");
}

#[test]
fn openai_responses_drops_the_handoff() {
    assert_dropped(
        &OpenAiResponses,
        &openai_responses_frozen_config("gpt-5"),
        "openai responses",
    );
}

#[test]
fn chatgpt_codex_drops_the_handoff() {
    assert_dropped(
        &OpenAiResponsesChatgpt,
        &openai_responses_chatgpt_frozen_config("gpt-5-codex"),
        "chatgpt codex",
    );
}

#[test]
fn gemini_drops_the_handoff() {
    assert_dropped(&Gemini, &gemini_frozen_config("gemini-2.5-flash"), "gemini");
}

#[test]
fn google_codeassist_drops_the_handoff() {
    let mut frozen = google_codeassist_frozen_config("gemini-3-pro");
    frozen.project_id = Some("project-handoff".into());
    assert_dropped(&GoogleCodeAssist, &frozen, "google code assist");
}

#[test]
fn bedrock_drops_the_handoff() {
    assert_dropped(
        &BedrockConverse,
        &bedrock_frozen_config("anthropic.claude-opus-5-5"),
        "bedrock",
    );
}
