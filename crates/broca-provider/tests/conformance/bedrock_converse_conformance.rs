//! Bedrock ConverseStream conformance: pure render invariants plus incremental
//! AWS event-stream decoding (binary framing, not SSE).

use broca_protocol::{
    content::{ContentBlock, FinishReason, Message, Role, ToolCall},
    request::{CallOptions, Tool, ToolChoice},
    NormalizedEvent, ProviderControl, StepId, ToolCallId,
};
use broca_provider::conformance::{
    check_freeze_against_mutation, check_render_determinism, normalize_recorded,
};
use broca_provider::families::{bedrock_frozen_config, resolve_frozen, BedrockConverse};
use broca_provider::policy::{Quirks, SystemPromptPolicy, UsagePolicy};
use broca_provider::spec::{
    AuthKind, Capabilities, CapabilityFlag, CostSchedule, Limits, ModelSpec, Overlay, ProviderSpec,
    WireFamilyId,
};
use broca_provider::wire::WireFamily;
use serde_json::{json, Value};

fn rich_request() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::System,
            content: vec![ContentBlock::Text {
                text: "Sys.".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text { text: "Hi".into() }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Assistant,
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("tooluse_1".into()),
                tool_name: "a_tool".into(),
                input: json!({"x": 1}),
            })],
        },
    ]);
    opts.tools = (0..12)
        .map(|i| Tool { name: format!("tool_{i:02}"), description: format!("tool number {i}"), input_schema: json!({"type": "object", "properties": {format!("p{i}"): {"type": "string"}}}), module: None, execution_mode: Default::default(), reply: None, })
        .collect::<Vec<_>>()
        .into();
    opts.tool_choice = ToolChoice::Auto.into();
    let mut generation = opts.generation.as_ref().clone();
    generation.temperature = Some(0.4);
    opts.generation = generation.into();
    opts
}

#[test]
fn bedrock_converse_passes_render_determinism() {
    let frozen = bedrock_frozen_config("anthropic.claude-3-5-sonnet-20240620-v1:0");
    let report = check_render_determinism(&BedrockConverse, &frozen, &rich_request());
    assert!(
        report.passed(),
        "conformance failures: {:?}",
        report.failures()
    );
}

fn bedrock_spec() -> (ProviderSpec, ModelSpec) {
    let provider = ProviderSpec {
        id: "aws-bedrock".into(),
        wire_family: WireFamilyId::BedrockConverse,
        base_url: "https://bedrock-runtime.us-west-2.amazonaws.com".into(),
        auth_kind: AuthKind::SigV4 {
            region: "us-west-2".into(),
        },
        static_headers: Default::default(),
        static_options: serde_json::Value::Null,
        quirks: Quirks {
            system_prompt: SystemPromptPolicy::TopLevelField,
            usage: UsagePolicy::BedrockUsage,
            ..Default::default()
        },
        raw: serde_json::Value::Null,
    };
    let model = ModelSpec {
        id: "anthropic.claude-3-5-sonnet-20240620-v1:0".into(),
        display_name: "Claude 3.5 Sonnet on Bedrock".into(),
        family: Some("claude".into()),
        capabilities: Capabilities {
            tool_call: CapabilityFlag::Known(true),
            temperature: CapabilityFlag::Known(true),
            ..Default::default()
        },
        limits: Limits::default(),
        cost: CostSchedule::default(),
        reasoning_options: Default::default(),
        server_side_fallback: None,
        input_modalities: Default::default(),
        release_date: None,
        status: None,
        overlay: Overlay::default(),
        raw: serde_json::Value::Null,
    };
    (provider, model)
}

#[test]
fn bedrock_converse_freeze_survives_catalog_mutation() {
    let mut spec = bedrock_spec();
    let result = check_freeze_against_mutation(
        &BedrockConverse,
        &mut spec,
        &rich_request(),
        |(p, m)| resolve_frozen(p, m, 2048, 1).expect("test specs publish known capability flags"),
        |(p, m)| {
            m.id = "anthropic.claude-3-haiku-20240307-v1:0".into();
            p.auth_kind = AuthKind::SigV4 {
                region: "eu-central-1".into(),
            };
            p.static_options = json!({"additionalModelRequestFields": {"after": "freeze"}});
        },
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
}

#[test]
fn bedrock_normalizer_decodes_binary_event_stream_incrementally() {
    let recorded = recorded_bedrock_event_stream();
    let frozen = bedrock_frozen_config("anthropic.claude-3-5-sonnet-20240620-v1:0");

    // Chunk size 5 splits the binary prelude, headers, payload, and CRC boundaries.
    let events = normalize_recorded(&BedrockConverse, &frozen, StepId(7), &recorded, 5)
        .expect("normalize ok");

    let msg = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("assembled message");

    assert!(
        msg.content.iter().any(
            |block| matches!(block, ContentBlock::Text { text } if text == "Hello from Bedrock.")
        ),
        "expected assembled text; got {:?}",
        msg.content
    );

    let call = msg
        .content
        .iter()
        .find_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(call),
            _ => None,
        })
        .expect("assembled tool call");
    assert_eq!(call.tool_call_id.0, "tooluse_abc");
    assert_eq!(call.tool_name, "get_time");
    assert_eq!(call.input, json!({"timezone": "Europe/Madrid"}));

    let terminal = events.iter().find_map(|event| match event {
        NormalizedEvent::Control(ProviderControl::StepFinished {
            usage,
            finish_reason,
            ..
        }) => Some((*usage, *finish_reason)),
        _ => None,
    });
    let (usage, finish_reason) = terminal.expect("step finished");
    assert_eq!(usage.input_tokens, Some(12));
    assert_eq!(usage.cached_input_tokens, Some(5));
    assert_eq!(usage.cache_write_tokens, Some(7));
    assert_eq!(usage.output_tokens, Some(8));
    assert_eq!(finish_reason, FinishReason::ToolCalls);
}

#[test]
fn bedrock_message_stop_without_metadata_is_truncation() {
    let recorded = recorded_bedrock_event_stream_through_message_stop();
    let frozen = bedrock_frozen_config("anthropic.claude-3-5-sonnet-20240620-v1:0");
    let mut normalizer = BedrockConverse.normalizer(&frozen, StepId(7));
    let mut events = Vec::new();

    for chunk in recorded.chunks(5) {
        events.extend(normalizer.push(chunk).expect("event stream frame"));
    }
    assert!(!events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(
            ProviderControl::AssistantMessage { .. }
                | ProviderControl::StepFinished { .. }
                | ProviderControl::Finished { .. }
        )
    )));

    let error = normalizer
        .finish()
        .expect_err("EOF before the metadata trailer must be rejected");
    assert!(
        error.to_string().contains("metadata trailer"),
        "unexpected error: {error}"
    );
}

#[test]
fn bedrock_started_without_message_stop_is_truncation() {
    let recorded = event_stream_message("messageStart", &json!({"role": "assistant"}));
    let frozen = bedrock_frozen_config("anthropic.claude-3-5-sonnet-20240620-v1:0");
    let mut normalizer = BedrockConverse.normalizer(&frozen, StepId(7));

    normalizer.push(&recorded).expect("message start");
    let error = normalizer
        .finish()
        .expect_err("EOF before messageStop must be rejected");
    assert!(
        error.to_string().contains("messageStop"),
        "unexpected error: {error}"
    );
}

fn recorded_bedrock_event_stream() -> Vec<u8> {
    let mut stream = recorded_bedrock_event_stream_through_message_stop();
    stream.extend(event_stream_message(
        "metadata",
        &json!({"usage": {"inputTokens": 12, "outputTokens": 8, "totalTokens": 20, "cacheReadInputTokens": 5, "cacheWriteInputTokens": 7}, "metrics": {"latencyMs": 42}}),
    ));
    stream
}

fn recorded_bedrock_event_stream_through_message_stop() -> Vec<u8> {
    let events = [
        ("messageStart", json!({"role": "assistant"})),
        (
            "contentBlockDelta",
            json!({"contentBlockIndex": 0, "delta": {"text": "Hello "}}),
        ),
        (
            "contentBlockDelta",
            json!({"contentBlockIndex": 0, "delta": {"text": "from Bedrock."}}),
        ),
        ("contentBlockStop", json!({"contentBlockIndex": 0})),
        (
            "contentBlockStart",
            json!({
                "contentBlockIndex": 1,
                "start": {"toolUse": {"toolUseId": "tooluse_abc", "name": "get_time"}}
            }),
        ),
        (
            "contentBlockDelta",
            json!({"contentBlockIndex": 1, "delta": {"toolUse": {"input": "{\"timezone\":"}}}),
        ),
        (
            "contentBlockDelta",
            json!({"contentBlockIndex": 1, "delta": {"toolUse": {"input": "\"Europe/Madrid\"}"}}}),
        ),
        ("contentBlockStop", json!({"contentBlockIndex": 1})),
        ("messageStop", json!({"stopReason": "tool_use"})),
    ];

    let mut stream = Vec::new();
    for (event_type, payload) in events {
        stream.extend(event_stream_message(event_type, &payload));
    }
    stream
}

pub(super) fn event_stream_message(event_type: &str, payload: &Value) -> Vec<u8> {
    let mut headers = Vec::new();
    string_header(&mut headers, ":message-type", "event");
    string_header(&mut headers, ":event-type", event_type);
    string_header(&mut headers, ":content-type", "application/json");
    let payload = serde_json::to_vec(payload).unwrap();

    let total_len = 12 + headers.len() + payload.len() + 4;
    let mut message = Vec::new();
    message.extend((total_len as u32).to_be_bytes());
    message.extend((headers.len() as u32).to_be_bytes());
    let prelude_crc = crc32(&message);
    message.extend(prelude_crc.to_be_bytes());
    message.extend(headers);
    message.extend(payload);
    let message_crc = crc32(&message);
    message.extend(message_crc.to_be_bytes());
    message
}

fn string_header(out: &mut Vec<u8>, name: &str, value: &str) {
    out.push(name.len() as u8);
    out.extend(name.as_bytes());
    out.push(7);
    out.extend((value.len() as u16).to_be_bytes());
    out.extend(value.as_bytes());
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0xedb8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}
