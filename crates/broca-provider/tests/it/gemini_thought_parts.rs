//! Gemini thought-part normalization and Code-Assist parity regressions.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall},
    display::DisplayEvent,
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, ModelHandle},
    StepId, ToolCallId,
};
use broca_provider::families::{
    anthropic_frozen_config, bedrock_frozen_config, gemini_frozen_config,
    google_codeassist_frozen_config, AnthropicMessages, BedrockConverse, Gemini, GoogleCodeAssist,
};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};
use serde_json::{json, Value};

const MODEL: &str = "gemini-3.5-flash-low";
const THOUGHT_SUMMARY: &str = "**Analyzing the Problem**\n\nCheck the requested calculation.";
const SIGNED_REASONING: &str = "I should verify with tools.";

fn signed_text(text: &str, signature: &str) -> ContentBlock {
    ContentBlock::TextWithState {
        text: text.into(),
        provider_state: broca_protocol::content::TextProviderState {
            format: broca_protocol::content::TextProviderFormat::Gemini,
            data: json!({"thoughtSignature":signature}).to_string(),
        },
    }
}

fn normalize_response(
    family: &dyn WireFamily,
    frozen: &FrozenRenderConfig,
    response: &Value,
    wrapped: bool,
) -> Vec<NormalizedEvent> {
    let payload = if wrapped {
        json!({ "response": response }).to_string()
    } else {
        response.to_string()
    };
    let sse = format!("data: {payload}\n\n");
    let mut normalizer = family.normalizer(frozen, StepId(17));
    let mut events = Vec::new();
    for chunk in sse.as_bytes().chunks(11) {
        events.extend(normalizer.push(chunk).expect("normalize fixture chunk"));
    }
    events.extend(normalizer.finish().expect("finish fixture"));
    events
}

fn assistant_message(events: &[NormalizedEvent]) -> &broca_protocol::content::AssistantMessage {
    events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("assembled assistant message")
}

fn thought_parts_fixture() -> Value {
    serde_json::from_str(include_str!(
        "../fixtures/gemini_thought_parts_response.json"
    ))
    .expect("valid thought-parts fixture")
}

fn signature_only_fixture() -> Value {
    serde_json::from_str(include_str!(
        "../fixtures/gemini_signature_only_thought_response.json"
    ))
    .expect("valid signature-only fixture")
}

fn signature_only_prompt() -> Vec<Message> {
    let response = signature_only_fixture();
    let events = normalize_response(&Gemini, &gemini_frozen_config(MODEL), &response, false);
    let message = assistant_message(&events);
    vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "first question".into(),
            }],
        ),
        Message::assistant_from(message.content.clone(), ModelHandle::new("google", MODEL)),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "continue".into(),
            }],
        ),
    ]
}

fn assert_no_empty_message_arrays(body: &Value, messages_key: &str, content_key: &str) {
    assert!(body[messages_key]
        .as_array()
        .expect("rendered messages array")
        .iter()
        .all(|message| message[content_key]
            .as_array()
            .is_some_and(|content| !content.is_empty())));
}

#[test]
fn gemini_preserves_thought_answer_and_parallel_call_source_order() {
    let events = normalize_response(
        &Gemini,
        &gemini_frozen_config(MODEL),
        &thought_parts_fixture(),
        false,
    );
    let message = assistant_message(&events);

    assert_eq!(
        message.content,
        vec![
            ContentBlock::Reasoning {
                text: THOUGHT_SUMMARY.into(),
                signature: None,
            },
            signed_text("Answer: 391.", "discarded-answer-signature"),
            ContentBlock::Reasoning {
                text: SIGNED_REASONING.into(),
                signature: Some("signed-reasoning".into()),
            },
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("call_0".into()),
                tool_name: "lookup".into(),
                input: json!({"value": 391}),
                thought_signature: Some("signed-first-call".into()),
            }),
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("call_1".into()),
                tool_name: "explain".into(),
                input: json!({"value": 391}),
                thought_signature: None,
            }),
            ContentBlock::Text {
                text: " Done.".into(),
            },
        ]
    );

    let text_deltas: String = events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Display(DisplayEvent::TextDelta { delta, .. }) => Some(delta.as_str()),
            _ => None,
        })
        .collect();
    let canonical_text: String = message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(text_deltas, "Answer: 391. Done.");
    assert_eq!(canonical_text, text_deltas);
    assert!(!text_deltas.contains("Analyzing"));
    assert!(!text_deltas.contains("verify with tools"));
    assert!(matches!(
        message.content.last(),
        Some(ContentBlock::Text { text }) if text == " Done."
    ));

    let reasoning_deltas: String = events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Display(DisplayEvent::ReasoningDelta { delta, .. }) => {
                Some(delta.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        reasoning_deltas,
        format!("{THOUGHT_SUMMARY}{SIGNED_REASONING}")
    );
}

#[test]
fn codeassist_wrapped_fixture_matches_bare_gemini_events() {
    let response = thought_parts_fixture();
    let mut gemini = normalize_response(&Gemini, &gemini_frozen_config(MODEL), &response, false);
    let codeassist = normalize_response(
        &GoogleCodeAssist,
        &google_codeassist_frozen_config(MODEL),
        &response,
        true,
    );

    // Both endpoints share the content codec, but their opaque state has distinct owners.
    for event in &mut gemini {
        if let NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) = event {
            for block in &mut message.content {
                if let ContentBlock::TextWithState { provider_state, .. } = block {
                    assert_eq!(
                        provider_state.format,
                        broca_protocol::content::TextProviderFormat::Gemini
                    );
                    provider_state.format =
                        broca_protocol::content::TextProviderFormat::GoogleCodeAssist;
                }
            }
        }
    }
    assert_eq!(codeassist, gemini);
    assert_eq!(assistant_message(&codeassist), assistant_message(&gemini));
}

#[test]
fn signature_only_thought_is_redacted_and_renders_without_empty_text() {
    let response: Value = serde_json::from_str(include_str!(
        "../fixtures/gemini_signature_only_thought_response.json"
    ))
    .expect("valid signature-only fixture");
    let events = normalize_response(&Gemini, &gemini_frozen_config(MODEL), &response, false);
    assert!(
        events
            .iter()
            .all(|event| !matches!(event, NormalizedEvent::Display(_))),
        "a signature-only thought has no display text"
    );
    let message = assistant_message(&events);
    assert_eq!(
        message.content,
        vec![ContentBlock::RedactedReasoning {
            data: "opaque-signature-only-thought".into(),
            provider_format: None,
        }]
    );

    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "continue".into(),
            }],
        ),
        Message::assistant_from(message.content.clone(), ModelHandle::new("google", MODEL)),
    ]);
    opts.target_model = Some(ModelHandle::new("google", MODEL));
    let rendered: Value =
        serde_json::from_slice(&Gemini.render(&gemini_frozen_config(MODEL), &opts)).unwrap();
    let part = &rendered["contents"][1]["parts"][0];
    assert_eq!(
        part,
        &json!({
            "thought": true,
            "thoughtSignature": "opaque-signature-only-thought",
        })
    );
    assert!(part.get("text").is_none());
}

#[test]
fn signature_only_gemini_turn_is_omitted_for_anthropic_target() {
    let mut opts = CallOptions::new(signature_only_prompt());
    opts.target_model = Some(ModelHandle::new("anthropic", "claude-opus-4-8"));

    let body: Value = serde_json::from_slice(
        &AnthropicMessages.render(&anthropic_frozen_config("claude-opus-4-8"), &opts),
    )
    .unwrap();
    let messages = body["messages"].as_array().expect("messages array");

    assert_eq!(messages.len(), 2);
    assert!(messages
        .iter()
        .all(|message| message["role"] == json!("user")));
    assert_no_empty_message_arrays(&body, "messages", "content");
}

#[test]
fn signature_only_gemini_turn_stays_for_its_origin_model() {
    let mut opts = CallOptions::new(signature_only_prompt());
    opts.target_model = Some(ModelHandle::new("google", MODEL));

    let body: Value =
        serde_json::from_slice(&Gemini.render(&gemini_frozen_config(MODEL), &opts)).unwrap();
    let contents = body["contents"].as_array().expect("contents array");

    assert_eq!(contents.len(), 3);
    assert_eq!(
        contents[1]["parts"][0],
        json!({
            "thought": true,
            "thoughtSignature": "opaque-signature-only-thought",
        })
    );
    assert_no_empty_message_arrays(&body, "contents", "parts");
}

#[test]
fn signature_only_gemini_turn_is_omitted_for_foreign_gemini_target() {
    const TARGET: &str = "gemini-2.5-flash";
    let mut opts = CallOptions::new(signature_only_prompt());
    opts.target_model = Some(ModelHandle::new("google", TARGET));

    let body: Value =
        serde_json::from_slice(&Gemini.render(&gemini_frozen_config(TARGET), &opts)).unwrap();

    assert_eq!(body["contents"].as_array().unwrap().len(), 2);
    assert_no_empty_message_arrays(&body, "contents", "parts");
}

#[test]
fn signature_only_gemini_turn_is_omitted_for_bedrock_target() {
    const TARGET: &str = "amazon.nova-lite-v1:0";
    let mut opts = CallOptions::new(signature_only_prompt());
    opts.target_model = Some(ModelHandle::new("amazon-bedrock", TARGET));

    let body: Value =
        serde_json::from_slice(&BedrockConverse.render(&bedrock_frozen_config(TARGET), &opts))
            .unwrap();

    assert_eq!(body["messages"].as_array().unwrap().len(), 2);
    assert_no_empty_message_arrays(&body, "messages", "content");
}

#[test]
fn adjacent_unsigned_parts_coalesce_without_crossing_signed_boundaries() {
    let response = json!({
        "candidates": [{
            "content": {
                "role": "model",
                "parts": [
                    {"text": "plain "},
                    {"text": "answer"},
                    {"thought": true, "text": "think "},
                    {"thought": true, "text": "more"},
                    {"thought": true, "text": "signed", "thoughtSignature": "sig"},
                    {"thought": true, "text": "after"},
                    {"text": "signed answer", "thoughtSignature": "discard"},
                    {"text": "after answer"}
                ]
            },
            "finishReason": "STOP"
        }]
    });
    let events = normalize_response(&Gemini, &gemini_frozen_config(MODEL), &response, false);

    assert_eq!(
        assistant_message(&events).content,
        vec![
            ContentBlock::Text {
                text: "plain answer".into(),
            },
            ContentBlock::Reasoning {
                text: "think more".into(),
                signature: None,
            },
            ContentBlock::Reasoning {
                text: "signed".into(),
                signature: Some("sig".into()),
            },
            ContentBlock::Reasoning {
                text: "after".into(),
                signature: None,
            },
            signed_text("signed answer", "discard"),
            ContentBlock::Text {
                text: "after answer".into(),
            },
        ]
    );
}
