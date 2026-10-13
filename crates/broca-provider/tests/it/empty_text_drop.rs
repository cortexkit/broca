//! G1 — universal empty-text-drop (codec.md §F.8).
//!
//! Anthropic, Gemini, and Bedrock reject an empty text part (`{type:text, text:""}` /
//! `{text:""}`). The owned normalizers never produce an empty text block (they push text
//! only when non-empty), so this guard is defensive against FOREIGN CK input — it changes
//! no current render (the goldens are byte-identical), but a foreign harness that hands the
//! codec an empty text block must not produce a provider-rejected request.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    ids::ToolCallId,
    request::{CallOptions, ModelHandle},
};
use broca_provider::families::{
    anthropic_frozen_config, bedrock_frozen_config, gemini_frozen_config, AnthropicMessages,
    BedrockConverse, Gemini,
};
use broca_provider::wire::WireFamily;
use serde_json::Value;

/// A prompt with an EMPTY text block adjacent to a non-empty one (the foreign-input shape
/// the guard defends against), across a user + system turn.
fn prompt_with_empty_text() -> Vec<Message> {
    vec![
        Message::new(
            Role::System,
            vec![
                ContentBlock::Text { text: "".into() },
                ContentBlock::Text {
                    text: "real system".into(),
                },
            ],
        ),
        Message::new(
            Role::User,
            vec![
                ContentBlock::Text { text: "".into() },
                ContentBlock::Text {
                    text: "real user".into(),
                },
            ],
        ),
    ]
}

/// Walk every string value in a JSON tree; return true if any equals "".
fn has_empty_string(v: &Value) -> bool {
    match v {
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.iter().any(has_empty_string),
        Value::Object(o) => o.values().any(has_empty_string),
        _ => false,
    }
}

fn has_empty_text_field(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(has_empty_text_field),
        Value::Object(object) => object.iter().any(|(key, value)| {
            (key == "text" && value.as_str().is_some_and(str::is_empty))
                || has_empty_text_field(value)
        }),
        _ => false,
    }
}

/// Count non-empty text occurrences so we prove the guard dropped ONLY the empties (it
/// must not also drop the real text).
fn count_real_text(body: &Value) -> usize {
    fn walk(v: &Value, n: &mut usize) {
        match v {
            Value::String(s) if s == "real system" || s == "real user" => *n += 1,
            Value::Array(a) => a.iter().for_each(|x| walk(x, n)),
            Value::Object(o) => o.values().for_each(|x| walk(x, n)),
            _ => {}
        }
    }
    let mut n = 0;
    walk(body, &mut n);
    n
}

#[test]
fn anthropic_drops_empty_text_parts() {
    let body: Value = serde_json::from_slice(&AnthropicMessages.render(
        &anthropic_frozen_config("claude-opus-4-8"),
        &CallOptions::new(prompt_with_empty_text()),
    ))
    .unwrap();
    // The system field and the messages must carry the real text but NO empty text part.
    assert!(
        !has_empty_string(&body["messages"]),
        "no empty text part may reach the Anthropic messages body: {body:#?}"
    );
    assert!(
        !has_empty_string(&body["system"]),
        "no empty text part may reach the Anthropic system field: {body:#?}"
    );
    assert_eq!(
        count_real_text(&body),
        2,
        "both real text blocks must survive"
    );
}

#[test]
fn gemini_drops_empty_text_parts() {
    let body: Value = serde_json::from_slice(&Gemini.render(
        &gemini_frozen_config("gemini-2.5-flash"),
        &CallOptions::new(prompt_with_empty_text()),
    ))
    .unwrap();
    assert!(
        !has_empty_string(&body["contents"]),
        "no empty text part may reach the Gemini contents: {body:#?}"
    );
    assert!(
        !has_empty_string(&body["systemInstruction"]),
        "no empty text part may reach the Gemini systemInstruction: {body:#?}"
    );
    assert_eq!(
        count_real_text(&body),
        2,
        "both real text blocks must survive"
    );
}

#[test]
fn gemini_signature_only_thought_has_no_empty_text_field() {
    let model = ModelHandle::new("google", "gemini-3.5-flash-low");
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "continue".into(),
            }],
        ),
        Message::assistant_from(
            vec![ContentBlock::RedactedReasoning {
                data: "opaque-thought-signature".into(),
                provider_format: None,
            }],
            model.clone(),
        ),
    ]);
    opts.target_model = Some(model);

    let body: Value = serde_json::from_slice(
        &Gemini.render(&gemini_frozen_config("gemini-3.5-flash-low"), &opts),
    )
    .unwrap();
    let part = &body["contents"][1]["parts"][0];
    assert_eq!(
        part.get("thoughtSignature").and_then(Value::as_str),
        Some("opaque-thought-signature")
    );
    assert!(part.get("text").is_none());
    assert!(!has_empty_string(&body["contents"]));
}

#[test]
fn bedrock_drops_empty_text_parts() {
    let tool_call_id = ToolCallId("call_1".into());
    let mut prompt = prompt_with_empty_text();
    prompt.push(Message::new(
        Role::Assistant,
        vec![ContentBlock::ToolCall(ToolCall {
            tool_call_id: tool_call_id.clone(),
            tool_name: "lookup".into(),
            input: serde_json::json!({}),
            thought_signature: None,
        })],
    ));
    prompt.push(Message::new(
        Role::Tool,
        vec![ContentBlock::ToolResult(ToolResult {
            tool_call_id,
            output: ToolOutput::Text {
                text: String::new(),
            },
            is_error: false,
        })],
    ));

    let body: Value = serde_json::from_slice(&BedrockConverse.render(
        &bedrock_frozen_config("amazon.nova-lite-v1:0"),
        &CallOptions::new(prompt),
    ))
    .unwrap();
    assert!(
        !has_empty_text_field(&body["messages"]),
        "no empty text part may reach the Bedrock messages: {body:#?}"
    );
    assert!(
        !has_empty_string(&body["system"]),
        "no empty text part may reach the Bedrock system: {body:#?}"
    );
    let tool_result = &body["messages"][2]["content"][0]["toolResult"];
    assert_eq!(tool_result["content"], serde_json::json!([{ "json": "" }]));
    assert_eq!(tool_result["status"], "success");
    assert_eq!(
        count_real_text(&body),
        2,
        "both real text blocks must survive"
    );
}
