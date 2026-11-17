//! Scratch measurement, not committed: what one render allocates against its request bytes.
use broca_protocol::{CallOptions, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult};
use broca_provider::{
    families::frozen_config_for_family, registry::wire_family, spec::WireFamilyId,
};
use broca_test_support::alloc::{measure, CountingAlloc};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn prompt(turns: usize) -> Vec<Message> {
    let text = "lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(30);
    let input = serde_json::json!({"command": "cargo test -p crate", "path": "/a/b/c.rs", "lines": [1,2,3], "nested": {"a": "x".repeat(200), "b": [{"c": 1}, {"d": "y".repeat(100)}]}});
    let mut out = vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: text.clone().into(),
        }],
    )];
    for n in 0..turns {
        let id = format!("toolu_{n:020}");
        out.push(Message::new(
            Role::Assistant,
            vec![
                ContentBlock::Reasoning {
                    text: text.clone().into(),
                    signature: Some("s".repeat(400).into()),
                },
                ContentBlock::Text {
                    text: text[..600].to_string().into(),
                },
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: broca_protocol::ToolCallId(id.clone()),
                    tool_name: "bash".into(),
                    input: (&input).into(),
                    thought_signature: None,
                }),
            ],
        ));
        out.push(Message::new(
            Role::User,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: broca_protocol::ToolCallId(id),
                output: ToolOutput::Text {
                    text: text.repeat(2).into(),
                },
                is_error: false,
            })],
        ));
    }
    out
}

#[test]
fn render_peak() {
    for family in [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::Gemini,
        WireFamilyId::BedrockConverse,
    ] {
        for turns in [100usize, 1000] {
            let opts = CallOptions::new(prompt(turns));
            let frozen = frozen_config_for_family(family, "measurement-model");
            let persona_frozen = (family == WireFamilyId::AnthropicMessages).then(|| {
                let mut f = broca_provider::families::anthropic_frozen_config("claude-opus-4-8");
                f.claude_code_persona = Some(broca_provider::wire::ClaudeCodePersona {
                    version: "2.1.302".into(),
                    device_id: "0".repeat(64),
                    account_uuid: None,
                    session_id: "11111111-1111-4111-8111-111111111111".into(),
                    prompt_id: None,
                    cc_version_suffix: Some("abc".into()),
                    prompt_ordinal: None,
                });
                f
            });
            if let Some(pf) = &persona_frozen {
                let renderer = wire_family(family).unwrap();
                let _ = renderer.render(pf, &opts);
                let (bytes, counts) = measure(|| renderer.render(pf, &opts));
                eprintln!(
                    "MEASURE persona turns={turns} request={} peak={} ({:.1}x) allocated={} ({:.1}x) allocations={}",
                    bytes.len(), counts.peak, counts.peak as f64 / bytes.len() as f64,
                    counts.allocated, counts.allocated as f64 / bytes.len() as f64, counts.allocations
                );
            }
            let renderer = wire_family(family).unwrap();
            let _ = renderer.render(&frozen, &opts);
            let (bytes, counts) = measure(|| renderer.render(&frozen, &opts));
            eprintln!(
                "MEASURE {family:?} turns={turns} request={} peak={} ({:.1}x) allocated={} ({:.1}x) allocations={}",
                bytes.len(), counts.peak, counts.peak as f64 / bytes.len() as f64,
                counts.allocated, counts.allocated as f64 / bytes.len() as f64, counts.allocations
            );
        }
    }
}
