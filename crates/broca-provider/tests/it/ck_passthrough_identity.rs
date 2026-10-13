//! PASS-THROUGH IDENTITY — the load-bearing codec-seam gate for wiring the MC transform.
//!
//! broca is the transform PRODUCER: per provider call it lifts its canonical prompt to
//! a CK-Message array, an external transform reduces it, and broca projects the result
//! back and renders it. If `lift` then `project` is not byte-identity for UNTRANSFORMED
//! content, then merely WIRING the transform (with zero reductions applied) would change the
//! rendered request bytes — a silent whole-prefix prompt-cache bust on the first wired turn,
//! which would look like "the transform busted the cache" when it is actually the codec seam.
//!
//! This gate isolates the seam from the transform: for a representative corpus across ALL
//! families, it asserts `render(project(lift(prompt))) == render(prompt)`, byte-identical. A
//! failure means `lift` or `project` is lossy somewhere (not that a reduction is wrong), so
//! the live E2E gets clean attribution.

use broca_ck::{lift, project};
use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, ModelHandle},
    ToolCallId,
};
use broca_provider::families::{
    anthropic_frozen_config, bedrock_frozen_config, gemini_frozen_config,
    openai_chat_frozen_config, openai_responses_frozen_config, AnthropicMessages, BedrockConverse,
    Gemini, OpenAiChat, OpenAiResponses,
};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};

/// The producer supplies `api` from its frozen family knowledge; the round-trip render does
/// not depend on it (render reads the projected canonical, which has no `api`), so a stable
/// stand-in is fine for this seam test.
fn api_for(_h: &ModelHandle) -> String {
    "test-api".into()
}

/// A representative multi-turn conversation exercising every canonical block kind the owned
/// leg produces: system, user text, an assistant turn with signed + redacted reasoning and a
/// tool call, and a tool-result turn covering the text / json / denied / error outputs.
fn corpus() -> Vec<Message> {
    vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: "You are a helpful assistant.".into(),
            }],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Do the thing.".into(),
            }],
        ),
        Message::assistant_from(
            vec![
                ContentBlock::Reasoning {
                    text: "let me think".into(),
                    signature: Some("sig-abc".into()),
                },
                ContentBlock::RedactedReasoning {
                    data: "encrypted-blob".into(),
                    provider_format: None,
                },
                ContentBlock::Text {
                    text: "I'll call some tools.".into(),
                },
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call_text".into()),
                    tool_name: "read".into(),
                    input: serde_json::json!({"path": "a.txt"}),
                }),
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call_json".into()),
                    tool_name: "inspect".into(),
                    input: serde_json::json!({"target": "b"}),
                }),
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call_denied".into()),
                    tool_name: "delete".into(),
                    input: serde_json::json!({"path": "c"}),
                }),
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call_err".into()),
                    tool_name: "write".into(),
                    input: serde_json::json!({"path": "d"}),
                }),
            ],
            ModelHandle::new("anthropic", "claude-opus-4-8"),
        ),
        Message::new(
            Role::Tool,
            vec![
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("call_text".into()),
                    output: ToolOutput::Text {
                        text: "file contents".into(),
                    },
                    is_error: false,
                }),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("call_json".into()),
                    output: ToolOutput::Json {
                        value: serde_json::json!({"ok": true, "n": 42}),
                    },
                    is_error: false,
                }),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("call_denied".into()),
                    output: ToolOutput::Denied {
                        reason: "policy forbids delete".into(),
                    },
                    is_error: true,
                }),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("call_err".into()),
                    output: ToolOutput::Error {
                        message: "disk full".into(),
                    },
                    is_error: true,
                }),
            ],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Now summarize.".into(),
            }],
        ),
    ]
}

/// `render(project(lift(prompt)))` must equal `render(prompt)`, byte-identical, for a family.
fn assert_passthrough_identity<F: WireFamily>(name: &str, family: &F, frozen: &FrozenRenderConfig) {
    let prompt = corpus();
    let direct = family.render(frozen, &CallOptions::new(prompt.clone()));

    // Lift → (no transform) → project, then render the round-tripped prompt.
    let ck = lift(&prompt, &api_for);
    let round_tripped = project(&ck).expect("project must not fail on owned-canonical content");
    let through_codec = family.render(frozen, &CallOptions::new(round_tripped));

    assert_eq!(
        direct, through_codec,
        "{name}: render(project(lift(prompt))) must be byte-identical to render(prompt) — the \
         codec seam must not change untransformed bytes (else wiring the transform busts the \
         cache)"
    );
}

#[test]
fn anthropic_passthrough_identity() {
    assert_passthrough_identity(
        "anthropic",
        &AnthropicMessages,
        &anthropic_frozen_config("claude-opus-4-8"),
    );
}

#[test]
fn openai_chat_passthrough_identity() {
    assert_passthrough_identity(
        "openai_chat",
        &OpenAiChat,
        &openai_chat_frozen_config("gpt-4o"),
    );
}

#[test]
fn openai_responses_passthrough_identity() {
    assert_passthrough_identity(
        "openai_responses",
        &OpenAiResponses,
        &openai_responses_frozen_config("gpt-4o"),
    );
}

#[test]
fn gemini_passthrough_identity() {
    assert_passthrough_identity("gemini", &Gemini, &gemini_frozen_config("gemini-2.5-flash"));
}

#[test]
fn bedrock_passthrough_identity() {
    assert_passthrough_identity(
        "bedrock",
        &BedrockConverse,
        &bedrock_frozen_config("amazon.nova-lite-v1:0"),
    );
}
