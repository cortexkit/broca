//! The request a fork's ask sends is the head's last request with the fork's messages appended
//! (llm-runner `fork_answer.base`): rendered through the head's own renderer and frozen config,
//! every byte outside the conversation, and every message of the head's, is unchanged. The one
//! exception is on a route that marks cache breakpoints explicitly: the head's last-message
//! breakpoint may move onto the fork's newest message, never adding one.

use broca_module::fork_answer::fork_request;
use broca_module::fork_state::Turn;
use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, Tool, ToolChoice},
    ToolCallId,
};
use broca_provider::families::{
    anthropic_frozen_config, openai_responses_frozen_config, AnthropicMessages, OpenAiResponses,
};
use broca_provider::wire::WireFamily;
use serde_json::{json, Value};

fn text(value: &str) -> ContentBlock {
    ContentBlock::Text { text: value.into() }
}

fn tool(name: &str) -> Tool {
    Tool {
        name: name.into(),
        description: format!("{name} a file"),
        input_schema: json!({"type": "object", "properties": {"path": {"type": "string"}}}),
        module: None,
        execution_mode: broca_protocol::request::ExecutionMode::Parallel,
        reply: None,
    }
}

/// A head's last request: a system prompt, the task, a tool round trip and its result.
fn head_request() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message::new(Role::System, vec![text("You are a careful code reader.")]),
        Message::new(Role::User, vec![text("Summarise src/lib.rs.")]),
        Message::new(
            Role::Assistant,
            vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                provider_state: None,
                tool_call_id: ToolCallId("toolu_1".into()),
                tool_name: "read".into(),
                input: json!({ "path": "src/lib.rs" }).into(),
            })],
        ),
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("toolu_1".into()),
                output: ToolOutput::Text {
                    text: "pub fn lib() {}".into(),
                },
                is_error: false,
            })],
        ),
    ]);
    opts.tools = std::sync::Arc::new(vec![tool("read"), tool("list")]);
    opts.tool_choice = std::sync::Arc::new(ToolChoice::Auto);
    opts
}

/// A fork's second ask on that request: its first question and answer, then a new question.
fn fork_of(base: &CallOptions) -> CallOptions {
    fork_request(
        base,
        &[Turn {
            question: "What are you reading?".into(),
            answer: "src/lib.rs".into(),
        }],
        "Which function is in it?",
    )
}

/// `value` with every `cache_control` marker removed, and the paths the markers sat at.
fn strip_markers(value: &Value, path: String, markers: &mut Vec<String>) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (key, child) in map {
                if key == "cache_control" {
                    markers.push(path.clone());
                } else {
                    out.insert(
                        key.clone(),
                        strip_markers(child, format!("{path}.{key}"), markers),
                    );
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .enumerate()
                .map(|(at, child)| strip_markers(child, format!("{path}[{at}]"), markers))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Checks that `fork` is `base` with messages appended to its conversation member
/// `conversation`, and returns the cache markers each holds.
fn appended_only(base: &[u8], fork: &[u8], conversation: &str) -> (Vec<String>, Vec<String>) {
    let (mut base_markers, mut fork_markers) = (Vec::new(), Vec::new());
    let base = strip_markers(
        &serde_json::from_slice(base).unwrap(),
        String::new(),
        &mut base_markers,
    );
    let fork = strip_markers(
        &serde_json::from_slice(fork).unwrap(),
        String::new(),
        &mut fork_markers,
    );
    let (mut base, mut fork) = (
        base.as_object().unwrap().clone(),
        fork.as_object().unwrap().clone(),
    );
    let base_messages = base.remove(conversation).unwrap();
    let fork_messages = fork.remove(conversation).unwrap();
    assert_eq!(
        base, fork,
        "every member but the conversation is the head's"
    );
    let (base_messages, fork_messages) = (
        base_messages.as_array().unwrap(),
        fork_messages.as_array().unwrap(),
    );
    assert!(fork_messages.len() > base_messages.len());
    assert_eq!(
        &fork_messages[..base_messages.len()],
        &base_messages[..],
        "every message of the head's is unchanged"
    );
    (base_markers, fork_markers)
}

#[test]
fn an_anthropic_fork_request_is_the_head_request_plus_its_messages_and_one_moved_breakpoint() {
    // Frozen as a fresh first-party Anthropic episode freezes it: caching by default.
    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.cache = broca_module_serve::resolve_cache_policy(
        broca_catalog::live::WireFamilyId::AnthropicMessages,
        "anthropic",
        None,
        &broca_module_serve::CacheTtlTable::shipped_defaults(),
        &broca_protocol::BindIdentity::new("/tmp/project", "runner", "session"),
    );
    let base = head_request();
    let base_bytes = AnthropicMessages.render(&frozen, &base);
    let fork_bytes = AnthropicMessages.render(&frozen, &fork_of(&base));
    let (base_markers, fork_markers) = appended_only(&base_bytes, &fork_bytes, "messages");
    let fork_body: Value = serde_json::from_slice(&fork_bytes).unwrap();
    let newest = fork_body["messages"].as_array().unwrap().len() - 1;
    let last_base = base_markers
        .iter()
        .filter(|path| path.starts_with(".messages"))
        .max()
        .expect("the head's request marks its last message")
        .clone();
    // The head's breakpoints, its last-message one moved onto the fork's newest message.
    let mut moved: Vec<String> = base_markers
        .iter()
        .filter(|path| **path != last_base)
        .cloned()
        .collect();
    moved.extend(
        fork_markers
            .iter()
            .filter(|path| path.starts_with(&format!(".messages[{newest}]")))
            .cloned(),
    );
    moved.sort();
    let mut fork_sorted = fork_markers.clone();
    fork_sorted.sort();
    assert_eq!(fork_sorted, moved, "only the last breakpoint moved");
    assert!(fork_markers.len() <= base_markers.len(), "{fork_markers:?}");
}

#[test]
fn an_openai_fork_request_is_the_head_request_plus_its_messages() {
    let frozen = openai_responses_frozen_config("gpt-5.4");
    let base = head_request();
    let base_bytes = OpenAiResponses.render(&frozen, &base);
    let fork_bytes = OpenAiResponses.render(&frozen, &fork_of(&base));
    let (base_markers, fork_markers) = appended_only(&base_bytes, &fork_bytes, "input");
    assert!(base_markers.is_empty() && fork_markers.is_empty());
}
