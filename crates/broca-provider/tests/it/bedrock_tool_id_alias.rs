use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::CallOptions,
    ModelHandle, ToolCallId,
};
use broca_provider::{
    families::{bedrock_frozen_config, BedrockConverse},
    wire::WireFamily,
};
use serde_json::{json, Value};

const BEDROCK_MODEL: &str = "anthropic.claude-3-5-sonnet-20240620-v1:0";

fn foreign_id(tag: &str) -> String {
    format!("call_{}|{tag}", "a".repeat(440))
}

fn cross_family_prompt(id: &str) -> Vec<Message> {
    vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "continue".into(),
            }],
        ),
        Message::assistant_from(
            vec![ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId(id.into()),
                tool_name: "get_time".into(),
                input: json!({"tz": "UTC"}),
                thought_signature: None,
            })],
            ModelHandle::new("openai", "gpt-5"),
        ),
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId(id.into()),
                output: ToolOutput::Text {
                    text: "12:00".into(),
                },
                is_error: false,
            })],
        ),
    ]
}

fn render(prompt: Vec<Message>) -> Value {
    let frozen = bedrock_frozen_config(BEDROCK_MODEL);
    let mut opts = CallOptions::new(prompt);
    opts.target_model = Some(ModelHandle::new("amazon-bedrock", BEDROCK_MODEL));
    serde_json::from_slice(&BedrockConverse.render(&frozen, &opts)).unwrap()
}

fn wire_ids(body: &Value) -> (String, String) {
    let mut tool_use_id = None;
    let mut tool_result_id = None;
    for message in body["messages"].as_array().unwrap() {
        for block in message["content"].as_array().unwrap() {
            if let Some(id) = block
                .get("toolUse")
                .and_then(|tool_use| tool_use.get("toolUseId"))
                .and_then(Value::as_str)
            {
                tool_use_id = Some(id.to_owned());
            }
            if let Some(id) = block
                .get("toolResult")
                .and_then(|tool_result| tool_result.get("toolUseId"))
                .and_then(Value::as_str)
            {
                tool_result_id = Some(id.to_owned());
            }
        }
    }
    (
        tool_use_id.expect("a toolUse block"),
        tool_result_id.expect("a toolResult block"),
    )
}

fn legacy_verbatim_ids(prompt: &[Message]) -> (String, String) {
    // Mirror the pre-fix Bedrock message conversion for the block kinds in this fixture. Keeping
    // the old mapper in the regression makes the invalid-before/valid-after contrast explicit.
    let messages = prompt
        .iter()
        .filter(|message| message.role != Role::System)
        .map(|message| {
            let content = message
                .content
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                        Some(json!({"text": text}))
                    }
                    ContentBlock::ToolCall(call) => Some(json!({
                        "toolUse": {
                            "toolUseId": call.tool_call_id.0,
                            "name": call.tool_name,
                            "input": call.input,
                        }
                    })),
                    ContentBlock::ToolResult(result) => Some(json!({
                        "toolResult": {
                            "toolUseId": result.tool_call_id.0,
                            "content": [{"text": "12:00"}],
                            "status": "success",
                        }
                    })),
                    ContentBlock::Reasoning { .. } | ContentBlock::RedactedReasoning { .. } => None,
                })
                .collect::<Vec<_>>();
            json!({
                "role": if message.role == Role::Assistant { "assistant" } else { "user" },
                "content": content,
            })
        })
        .collect::<Vec<_>>();
    wire_ids(&json!({"messages": messages}))
}

fn is_bedrock_valid(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

#[test]
fn foreign_id_aliases_pair_and_contrasts_legacy_verbatim_render() {
    let id = foreign_id("x");
    let prompt = cross_family_prompt(&id);
    let legacy = legacy_verbatim_ids(&prompt);
    assert_eq!(legacy, (id.clone(), id.clone()));
    assert!(!is_bedrock_valid(&legacy.0));

    let (tool_use_id, tool_result_id) = wire_ids(&render(prompt));
    assert!(is_bedrock_valid(&tool_use_id));
    assert_ne!(tool_use_id, id);
    assert_eq!(tool_use_id, tool_result_id);
}

#[test]
fn bedrock_id_length_boundary_preserves_64_and_aliases_65() {
    let valid_64 = format!("{}_.:-Z9", "a".repeat(58));
    assert_eq!(valid_64.len(), 64);
    assert!(is_bedrock_valid(&valid_64));
    let valid_wire = wire_ids(&render(cross_family_prompt(&valid_64)));
    assert_eq!(valid_wire, (valid_64.clone(), valid_64.clone()));

    let invalid_65 = format!("{valid_64}a");
    assert_eq!(invalid_65.len(), 65);
    assert!(!is_bedrock_valid(&invalid_65));
    let invalid_wire = wire_ids(&render(cross_family_prompt(&invalid_65)));
    assert!(is_bedrock_valid(&invalid_wire.0));
    assert_ne!(invalid_wire.0, invalid_65);
    assert_eq!(invalid_wire.0, invalid_wire.1);
}
