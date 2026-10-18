//! Synthetic fixtures follow the AWS ReasoningContentBlockDelta union (text, signature, or
//! base64 redactedContent). They exercise the binary event decoder, not a JSON-only substitute.
//! https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ReasoningContentBlockDelta.html

use broca_protocol::{
    content::{
        ContentBlock, EncryptedReasoningFormat, Message, Role, ToolCall, ToolOutput, ToolResult,
    },
    CallOptions, DisplayEvent, ModelHandle, NormalizedEvent, ProviderControl, StepId, ToolCallId,
};
use broca_provider::{
    conformance::normalize_recorded,
    families::{bedrock_frozen_config, BedrockConverse},
    wire::WireFamily,
};
use serde_json::{json, Value};

fn recorded_stream() -> Vec<u8> {
    let events: Vec<(String, Value)> =
        serde_json::from_str(include_str!("../fixtures/bedrock_reasoning_stream.json")).unwrap();
    events
        .into_iter()
        .flat_map(|(kind, payload)| test_event_stream_message(&kind, &payload))
        .collect()
}

fn canonical_blocks() -> Vec<ContentBlock> {
    vec![
        ContentBlock::Reasoning {
            text: "Think carefully.\n".into(),
            signature: Some("sig-opaque==".into()),
        },
        ContentBlock::Reasoning {
            text: String::new(),
            signature: Some("empty-sig".into()),
        },
        ContentBlock::RedactedReasoning {
            data: "AQL/".into(),
            provider_format: None,
        },
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId("call_1".into()),
            tool_name: "lookup".into(),
            input: json!({"key":"A"}),
            thought_signature: None,
        }),
        ContentBlock::Text {
            text: "Checking.".into(),
        },
    ]
}

#[test]
fn bedrock_reasoning_stream_preserves_text_signatures_redaction_and_order() {
    let frozen = bedrock_frozen_config("model");
    assert!(frozen.bedrock_reasoning);
    let recorded = recorded_stream();
    for chunk_size in [1, 5, recorded.len()] {
        let events =
            normalize_recorded(&BedrockConverse, &frozen, StepId(1), &recorded, chunk_size)
                .unwrap();
        let message = events
            .iter()
            .find_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    Some(message)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(message.content, canonical_blocks());
        let reasoning: String = events
            .iter()
            .filter_map(|event| match event {
                NormalizedEvent::Display(DisplayEvent::ReasoningDelta { delta, .. }) => {
                    Some(delta.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(reasoning, "Think carefully.\n");
    }
}

fn prompt() -> CallOptions {
    let model = ModelHandle::new("amazon-bedrock", "model");
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "question".into(),
            }],
        ),
        Message::assistant_from(canonical_blocks(), model.clone()),
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call_1".into()),
                output: ToolOutput::Text { text: "7".into() },
                is_error: false,
            })],
        ),
        Message::assistant_from(
            vec![
                ContentBlock::Reasoning {
                    text: "unsigned".into(),
                    signature: None,
                },
                ContentBlock::Text {
                    text: "answer".into(),
                },
            ],
            model.clone(),
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "next".into(),
            }],
        ),
    ]);
    opts.target_model = Some(model);
    opts
}

#[test]
fn bedrock_echoes_reasoning_on_tool_and_later_user_turns() {
    let frozen = bedrock_frozen_config("model");
    let opts = prompt();
    assert_eq!(
        BedrockConverse.render(&frozen, &opts),
        include_str!("../fixtures/bedrock_reasoning_echo.json")
            .trim_end()
            .as_bytes()
    );
}

#[test]
fn bedrock_legacy_normalizer_still_discards_reasoning() {
    let cases: Value =
        serde_json::from_str(include_str!("../fixtures/reasoning_context_legacy.json")).unwrap();
    let frozen = serde_json::from_value(cases[0]["frozen"].clone()).unwrap();
    let events =
        normalize_recorded(&BedrockConverse, &frozen, StepId(1), &recorded_stream(), 3).unwrap();
    let content = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(&message.content)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(content, &canonical_blocks()[3..]);
    assert!(!events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Display(DisplayEvent::ReasoningDelta { .. })
    )));
}

#[test]
fn bedrock_does_not_echo_foreign_reasoning_or_user_reasoning() {
    let frozen = bedrock_frozen_config("model");
    let mut opts = prompt();
    let mut messages = opts.prompt.clone();
    messages[0]
        .content
        .extend(canonical_blocks()[..3].iter().cloned());
    messages[1].origin = Some(ModelHandle::new("anthropic", "model"));
    messages[3].origin = None;
    messages[1].content.push(ContentBlock::RedactedReasoning {
        data: "responses-cipher".into(),
        provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
    });
    opts.prompt = messages;
    let body = BedrockConverse.shape(&frozen, &opts);
    assert!(!body.to_string().contains("reasoningContent"));
}

#[test]
fn bedrock_does_not_echo_reasoning_without_target() {
    let frozen = bedrock_frozen_config("model");
    let mut opts = prompt();
    opts.target_model = None;
    assert!(!BedrockConverse
        .shape(&frozen, &opts)
        .to_string()
        .contains("reasoningContent"));
}

#[test]
fn bedrock_rejects_invalid_redacted_blob_encoding() {
    let frozen = bedrock_frozen_config("model");
    let mut recorded = test_event_stream_message("messageStart", &json!({"role":"assistant"}));
    recorded.extend(test_event_stream_message("contentBlockDelta", &json!({"contentBlockIndex":0,"delta":{"reasoningContent":{"redactedContent":"not base64!"}}})));
    let error = normalize_recorded(&BedrockConverse, &frozen, StepId(1), &recorded, 5).unwrap_err();
    assert!(error.to_string().contains("redactedContent"));
}

fn test_event_stream_message(event_type: &str, payload: &Value) -> Vec<u8> {
    let mut headers = Vec::new();
    for (name, value) in [
        (":message-type", "event"),
        (":event-type", event_type),
        (":content-type", "application/json"),
    ] {
        headers.push(name.len() as u8);
        headers.extend(name.as_bytes());
        headers.push(7);
        headers.extend((value.len() as u16).to_be_bytes());
        headers.extend(value.as_bytes());
    }
    let payload = serde_json::to_vec(payload).unwrap();
    let total = 16 + headers.len() + payload.len();
    let mut frame = Vec::new();
    frame.extend((total as u32).to_be_bytes());
    frame.extend((headers.len() as u32).to_be_bytes());
    frame.extend(crc32(&frame).to_be_bytes());
    frame.extend(headers);
    frame.extend(payload);
    frame.extend(crc32(&frame).to_be_bytes());
    frame
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
