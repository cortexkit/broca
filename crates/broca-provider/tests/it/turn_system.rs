use broca_protocol::{
    CallOptions, ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use broca_provider::{
    families::{
        anthropic_frozen_config, gemini_frozen_config, openai_responses_frozen_config,
        AnthropicMessages, Gemini, OpenAiResponses,
    },
    turn_system,
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::{json, Value};

const VECTORS: &str = include_str!("../fixtures/turn-system-rendering.json");

fn text(role: Role, value: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: value.into() }])
}

fn system(value: &str) -> Message {
    let mut message = text(Role::System, value);
    message.turn_system = true;
    message
}

fn message(value: &Value) -> Message {
    let role = match value["role"].as_str().unwrap() {
        "assistant" => Role::Assistant,
        "system" => Role::System,
        _ => Role::User,
    };
    let content = value.get("content").or_else(|| value.get("parts")).unwrap();
    if let Some(content) = content.as_str() {
        return text(role, content);
    }
    let mut message = Message::new(role, Vec::new());
    for part in content.as_array().unwrap() {
        match part["type"].as_str() {
            Some("tool_use") => message.content.push(ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId(part["id"].as_str().unwrap().into()),
                tool_name: part["name"].as_str().unwrap().into(),
                input: part["input"].clone(),
                thought_signature: None,
            })),
            Some("tool_result") => {
                message.role = Role::Tool;
                message.content.push(ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId(part["tool_use_id"].as_str().unwrap().into()),
                    output: ToolOutput::Text {
                        text: part["content"].as_str().unwrap().into(),
                    },
                    is_error: false,
                }));
            }
            _ => message.content.push(ContentBlock::Text {
                text: part["text"].as_str().unwrap().into(),
            }),
        }
    }
    message
}

fn route(
    protocol: &str,
    supported: bool,
) -> (Box<dyn WireFamily>, FrozenRenderConfig, &'static str) {
    match protocol {
        "openai_responses" => (
            Box::new(OpenAiResponses),
            openai_responses_frozen_config("gpt-4o"),
            "input",
        ),
        "anthropic_messages" => {
            let mut frozen = anthropic_frozen_config("claude-opus-4-8");
            frozen.anthropic_turn_system = supported;
            (Box::new(AnthropicMessages), frozen, "messages")
        }
        "gemini" => (
            Box::new(Gemini),
            gemini_frozen_config("gemini-2.5-flash"),
            "contents",
        ),
        other => panic!("unknown vector protocol: {other}"),
    }
}

fn run(id: &str) {
    let vectors: Value = serde_json::from_str(VECTORS).unwrap();
    let case = vectors["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["id"] == id)
        .unwrap();
    let (family, frozen, key) = route(
        case["route"]["protocol"].as_str().unwrap(),
        case["route"]["turn_system_supported"].as_bool().unwrap(),
    );
    let mut requests = Vec::new();
    for attempt in case["attempts"].as_array().unwrap() {
        let mut prompt: Vec<_> = attempt["conversation"]
            .as_array()
            .unwrap()
            .iter()
            .map(message)
            .collect();
        let index = attempt["insertion_index"].as_u64().unwrap() as usize;
        let expected = &attempt["expect"];
        let possible = turn_system::possible(&frozen, &prompt[..index]);
        if expected["outcome"] == "not_possible_at_this_boundary" {
            assert!(!possible, "{id}");
            requests.push(Value::Null);
            continue;
        }
        let stored = case.get("stored_entry").is_some();
        if expected["authority"] == "system" {
            assert!(possible, "{id}");
        }
        let entry = if stored || possible {
            system(case["rendered_text"].as_str().unwrap())
        } else {
            // The vector's context-note wrapper is illustrative; broca's steer keeps plain text.
            text(Role::User, case["rendered_text"].as_str().unwrap())
        };
        let original = entry.clone();
        prompt.insert(index, entry);
        let opts = CallOptions::new(prompt);
        let bytes = family.render(&frozen, &opts);
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        let rendered = &body[key][index];
        let role = if expected["authority"] == "system" {
            if key == "input" {
                "developer"
            } else {
                "system"
            }
        } else {
            "user"
        };
        assert_eq!(rendered["role"], role, "{id}");
        let content = rendered
            .get("content")
            .or_else(|| rendered.get("parts"))
            .unwrap();
        let rendered_text = if let Some(text) = content.as_str() {
            text.to_owned()
        } else {
            content
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|part| part["text"].as_str())
                .collect::<Vec<_>>()
                .join("")
        };
        let expected_text = case["rendered_text"].as_str().unwrap();
        let expected_text = if stored && !possible {
            format!("<context-note>{expected_text}</context-note>")
        } else {
            expected_text.to_owned()
        };
        assert_eq!(rendered_text, expected_text, "{id}");
        assert_eq!(
            opts.prompt[index], original,
            "rendering must not mutate stored history"
        );
        // The llm-runner 0.7.36 test vectors pin authority, placement and text, not provider encoding.
        // For example broca's ordinary Responses input also carries type: "message".
        // Pin the real renderer's complete request separately, rather than rewrite legacy bytes.
        requests.push(body);
    }
    let golden = format!("{}\n", serde_json::to_string_pretty(&requests).unwrap());
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(format!("turn-system-{id}.json"));
    if std::env::var_os("UPDATE_GOLDENS").is_some() {
        std::fs::write(&path, &golden).unwrap();
    }
    assert_eq!(golden, std::fs::read_to_string(path).unwrap(), "{id}");
}

#[test]
fn openai_responses_developer_in_order() {
    run("openai-responses-developer-in-order");
}
#[test]
fn anthropic_native_after_tool_result() {
    run("anthropic-native-after-tool-result");
}
#[test]
fn anthropic_native_waits_after_model_reply() {
    run("anthropic-native-waits-after-model-reply");
}
#[test]
fn anthropic_native_never_between_tool_call_and_result() {
    run("anthropic-native-never-between-tool-call-and-result");
}
#[test]
fn unsupported_route_falls_through_at_user_authority() {
    run("unsupported-route-falls-through-at-user-authority");
}
#[test]
fn stored_entry_replayed_without_model_support() {
    run("stored-entry-replayed-without-model-support");
}

#[test]
fn earlier_request_prefix_is_unchanged_on_anthropic_responses_and_gemini() {
    for protocol in ["anthropic_messages", "openai_responses", "gemini"] {
        let (family, frozen, key) = route(protocol, true);
        let prompt = vec![
            text(Role::System, "Fixed instructions."),
            text(Role::User, "First turn."),
            text(Role::Assistant, "First reply."),
        ];
        let before = family.shape(&frozen, &CallOptions::new(prompt.clone()));
        let mut after = prompt;
        after.push(text(Role::User, "Second turn."));
        after.push(system("Keep the summary concise."));
        let after = family.shape(&frozen, &CallOptions::new(after));
        let earlier = before[key].as_array().unwrap();
        assert_eq!(
            broca_protocol::canonical_json_bytes(&json!(earlier)),
            broca_protocol::canonical_json_bytes(&json!(
                &after[key].as_array().unwrap()[..earlier.len()]
            )),
            "{protocol}"
        );
        for global in ["system", "instructions", "systemInstruction"] {
            assert_eq!(
                before.get(global),
                after.get(global),
                "{protocol}: {global}"
            );
        }
    }
}
