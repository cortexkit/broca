//! What an Anthropic send renders when it says nothing about prompt caching.
//!
//! Admission resolves the caller's cache request into the frozen render config, and the
//! renderer reads only that frozen value. These tests drive the same two steps the live
//! executor takes for a fresh episode -- `resolve_cache_policy`, then render -- so the bytes
//! pinned here are the bytes a real send without a `cache` field puts on the wire.

use broca_catalog::live::WireFamilyId;
use broca_module_serve::{resolve_cache_policy, CacheTtlTable};
use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CacheMode, CacheRequest, CacheRole, CallOptions, Tool, ToolChoice},
    BindIdentity, ToolCallId,
};
use broca_provider::families::{
    anthropic_frozen_config, bedrock_frozen_config, AnthropicMessages, BedrockConverse,
};
use broca_provider::wire::WireFamily;
use serde_json::{json, Value};

const GOLDEN: &str = "tests/fixtures/anthropic_no_cache_request_render_golden.json";
/// The bytes the same send rendered before first-party Anthropic cached by default, copied
/// unchanged from the previous version of `GOLDEN`. `cache.mode: none` must reproduce them.
const NO_MARKERS_GOLDEN: &str = "tests/fixtures/anthropic_cache_mode_none_render_golden.json";

fn message(role: Role, content: Vec<ContentBlock>) -> Message {
    Message::new(role, content)
}

fn text(value: &str) -> ContentBlock {
    ContentBlock::Text { text: value.into() }
}

fn tool_call(id: &str, path: &str) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId(id.into()),
        tool_name: "read".into(),
        input: json!({ "path": path }),
    })
}

fn tool_result(id: &str, body: &str) -> ContentBlock {
    ContentBlock::ToolResult(ToolResult {
        tool_call_id: ToolCallId(id.into()),
        output: ToolOutput::Text { text: body.into() },
        is_error: false,
    })
}

fn tool(name: &str) -> Tool {
    Tool {
        name: name.into(),
        description: format!("{name} a file"),
        input_schema: json!({"type": "object", "properties": {"path": {"type": "string"}}}),
        module: None,
        execution_mode: broca_protocol::request::ExecutionMode::Parallel,
    }
}

/// The third step of a tool loop: a system prompt, the user's task, and two completed
/// tool round trips, with two tools on offer.
fn tool_loop_step() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        message(Role::System, vec![text("You are a careful code reader.")]),
        message(
            Role::User,
            vec![text("Summarise src/lib.rs and src/main.rs.")],
        ),
        message(Role::Assistant, vec![tool_call("toolu_1", "src/lib.rs")]),
        message(Role::Tool, vec![tool_result("toolu_1", "pub fn lib() {}")]),
        message(Role::Assistant, vec![tool_call("toolu_2", "src/main.rs")]),
        message(Role::Tool, vec![tool_result("toolu_2", "fn main() {}")]),
    ]);
    opts.tools = std::sync::Arc::new(vec![tool("read"), tool("list")]);
    opts.tool_choice = std::sync::Arc::new(ToolChoice::Auto);
    opts
}

/// Render the step exactly as a fresh first-party Anthropic episode would: resolve the
/// caller's cache request against the shipped provider table, freeze the result, render.
fn render_admitted(provider: &str, request: Option<&CacheRequest>) -> Vec<u8> {
    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.cache = resolve(WireFamilyId::AnthropicMessages, provider, request);
    AnthropicMessages.render(&frozen, &tool_loop_step())
}

fn resolve(
    family: WireFamilyId,
    provider: &str,
    request: Option<&CacheRequest>,
) -> Option<broca_provider::wire::CachePolicy> {
    resolve_cache_policy(
        family,
        provider,
        request,
        &CacheTtlTable::shipped_defaults(),
        &BindIdentity::new("/tmp/project", "runner", "session"),
    )
    .expect("every request shape used here resolves")
}

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(name))
        .expect("golden fixture is committed")
}

/// Every `cache_control` object in a rendered body, labelled with where it sits.
fn markers(bytes: &[u8]) -> Vec<(String, Value)> {
    fn walk(value: &Value, path: String, out: &mut Vec<(String, Value)>) {
        match value {
            Value::Object(map) => {
                if let Some(marker) = map.get("cache_control") {
                    out.push((path.clone(), marker.clone()));
                }
                for (key, child) in map {
                    walk(child, format!("{path}.{key}"), out);
                }
            }
            Value::Array(items) => {
                for (index, child) in items.iter().enumerate() {
                    walk(child, format!("{path}[{index}]"), out);
                }
            }
            _ => {}
        }
    }
    let body: Value = serde_json::from_slice(bytes).expect("rendered body is JSON");
    let mut out = Vec::new();
    walk(&body, String::new(), &mut out);
    out
}

fn positions(bytes: &[u8]) -> Vec<String> {
    markers(bytes).into_iter().map(|(path, _)| path).collect()
}

/// The four breakpoints the shared planner places on this prompt: the end of the system
/// prompt (after the fixed identity lead), the first two messages, and the final message.
/// No tool definition is marked: tools precede the system prompt in Anthropic's cache
/// prefix, so the system breakpoint already covers them.
const PLANNED: [&str; 4] = [
    ".messages[0].content[0]",
    ".messages[1].content[0]",
    ".messages[4].content[0]",
    ".system[1]",
];

/// Pins the full request bytes of a send with no `cache` field. Set `BROCA_UPDATE_GOLDEN=1`
/// to rewrite the fixture after an intended change, and review the diff it produces.
#[test]
fn a_send_without_a_cache_request_renders_the_pinned_bytes() {
    let rendered = render_admitted("anthropic", None);
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(GOLDEN);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &rendered).expect("write golden");
    }
    let golden = std::fs::read(&path).expect("golden fixture is committed");
    assert_eq!(
        String::from_utf8_lossy(&rendered),
        String::from_utf8_lossy(&golden),
        "rendered bytes moved from {GOLDEN}"
    );
}

#[test]
fn a_send_without_a_cache_request_gets_four_default_markers_with_no_ttl() {
    let rendered = render_admitted("anthropic", None);
    assert_eq!(positions(&rendered), PLANNED);
    for (path, marker) in markers(&rendered) {
        assert_eq!(
            marker,
            json!({"type": "ephemeral"}),
            "{path}: the default writes no ttl, which Anthropic reads as 5 minutes"
        );
    }
}

#[test]
fn a_key_only_send_keeps_the_default_markers() {
    let key_only = CacheRequest {
        role: None,
        ttl: None,
        key: Some("caller-scope".into()),
        tail: None,
        mode: None,
    };
    assert_eq!(
        render_admitted("anthropic", Some(&key_only)),
        render_admitted("anthropic", None)
    );
}

#[test]
fn cache_mode_none_renders_the_bytes_from_before_the_default() {
    let none = CacheRequest {
        role: None,
        ttl: None,
        key: None,
        tail: None,
        mode: Some(CacheMode::None),
    };
    let rendered = render_admitted("anthropic", Some(&none));
    assert!(markers(&rendered).is_empty());
    assert_eq!(
        String::from_utf8_lossy(&rendered),
        String::from_utf8_lossy(&fixture(NO_MARKERS_GOLDEN)),
    );
}

#[test]
fn an_explicit_request_keeps_its_ttl_on_the_wire() {
    for (ttl, role, expected) in [
        (Some("1h"), None, "1h"),
        // No TTL named: the primary role's default, now 5m, still written out explicitly.
        (None, Some(CacheRole::Primary), "5m"),
    ] {
        let request = CacheRequest {
            role,
            ttl: ttl.map(str::to_string),
            key: None,
            tail: None,
            mode: None,
        };
        let rendered = render_admitted("anthropic", Some(&request));
        assert_eq!(
            positions(&rendered),
            PLANNED,
            "same planner, same placement"
        );
        for (path, marker) in markers(&rendered) {
            assert_eq!(
                marker,
                json!({"type": "ephemeral", "ttl": expected}),
                "{path}"
            );
        }
    }
}

#[test]
fn gateways_and_bedrock_without_a_cache_request_render_no_markers() {
    // Third-party providers speaking the Anthropic grammar keep today's behaviour.
    for gateway in ["minimax", "thinkingmachines", "freemodel"] {
        let rendered = render_admitted(gateway, None);
        assert!(
            markers(&rendered).is_empty(),
            "{gateway} got default markers: {:?}",
            positions(&rendered)
        );
    }
    let mut frozen = bedrock_frozen_config("anthropic.claude-opus-4-8");
    frozen.cache = resolve(WireFamilyId::BedrockConverse, "amazon-bedrock", None);
    assert_eq!(frozen.cache, None);
    let rendered = BedrockConverse.render(&frozen, &tool_loop_step());
    assert!(
        !String::from_utf8_lossy(&rendered).contains("cachePoint"),
        "Bedrock renders no cache points without a request"
    );
}
