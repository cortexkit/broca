//! Broca records fresh input, cache reads, cache writes, output, and reasoning as separate
//! measurements. For a single step, the full prompt is input + cached input + cache writes;
//! unreported fields stay absent, and reasoning is a subset of output. To update the fixture,
//! review every raw/normalized pair, run `shasum -a 256 crates/broca-provider/tests/golden/usage_normalization.json`,
//! and replace `GOLDEN_SHA256` with the resulting digest.

use broca_protocol::{NormalizedEvent, ProviderControl, StepId, Usage};
use broca_provider::{
    families::{
        self, AnthropicMessages, BedrockConverse, Gemini, GoogleCodeAssist, OpenAiChat,
        OpenAiResponses, OpenAiResponsesChatgpt,
    },
    policy::{ReasoningEchoPolicy, UsagePolicy},
    spec::WireFamilyId,
    wire::WireFamily,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const USAGE_GOLDEN: &str = include_str!("../golden/usage_normalization.json");
const GOLDEN_SHA256: &str = "c6a01c88d410820d648d5a0d6c1c69f1b7aebfbf540a32b351971f5b0ef91f00";

#[derive(Debug, Deserialize)]
struct UsageVector {
    family: String,
    case: String,
    raw_usage: Value,
    normalized: Usage,
    #[serde(default)]
    provenance: Option<String>,
}

#[test]
fn usage_normalization_golden_file_sha256_is_pinned() {
    let actual = format!("{:x}", Sha256::digest(USAGE_GOLDEN.as_bytes()));
    assert_eq!(
        actual, GOLDEN_SHA256,
        "usage_normalization.json changed. Review the vectors, then update GOLDEN_SHA256 using \
         `shasum -a 256 crates/broca-provider/tests/golden/usage_normalization.json`."
    );
}

#[test]
fn usage_normalization_vectors_match_production_normalizers_byte_for_byte() {
    let vectors: Vec<UsageVector> =
        serde_json::from_str(USAGE_GOLDEN).expect("usage normalization golden is valid JSON");

    for vector in vectors {
        if vector.family == "bedrock_converse" {
            assert_eq!(
                vector.provenance.as_deref(),
                Some("unverified-raw-semantics"),
                "Bedrock input-token semantics are not proven"
            );
        }

        let actual = normalize_vector(&vector);
        let actual_json = serde_json::to_vec(&actual).expect("normalized usage serializes");
        let expected_json =
            serde_json::to_vec(&vector.normalized).expect("golden usage serializes");
        assert_eq!(
            actual_json.as_slice(),
            expected_json.as_slice(),
            "{} / {}: actual={} expected={}",
            vector.family,
            vector.case,
            String::from_utf8_lossy(&actual_json),
            String::from_utf8_lossy(&expected_json),
        );
    }
}

fn normalize_vector(vector: &UsageVector) -> Usage {
    let family_id = family_id(&vector.family);
    let family = family(family_id);
    let mut frozen = families::frozen_config_for_family(family_id, "usage-normalization-golden");
    match vector.family.as_str() {
        "openai_chat_deepseek" => {
            frozen.reasoning_echo = Some(ReasoningEchoPolicy::DEEPSEEK);
        }
        "openai_chat_reasoning_separate" => {
            frozen.usage = UsagePolicy::OpenAiUsageReasoningSeparate;
        }
        "kimi" => {
            frozen.reasoning_echo = Some(ReasoningEchoPolicy::KIMI);
        }
        _ => {}
    }

    let recorded = recorded_stream(&vector.family, &vector.raw_usage);
    let mut normalizer = family.normalizer(&frozen, StepId(1));
    let mut events = normalizer.push(&recorded).unwrap_or_else(|error| {
        panic!(
            "{} / {}: production normalizer rejected stream: {error}",
            vector.family, vector.case
        )
    });
    events.extend(normalizer.finish().unwrap_or_else(|error| {
        panic!(
            "{} / {}: production normalizer rejected terminal stream: {error}",
            vector.family, vector.case
        )
    }));
    events
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(usage),
            _ => None,
        })
        .unwrap_or_else(|| {
            panic!(
                "{} / {}: normalizer emitted no StepFinished usage",
                vector.family, vector.case
            )
        })
}

fn family_id(family: &str) -> WireFamilyId {
    match family {
        "anthropic_messages" => WireFamilyId::AnthropicMessages,
        "openai_responses" => WireFamilyId::OpenAiResponses,
        "chatgpt_codex" => WireFamilyId::OpenAiResponsesChatgpt,
        "openai_chat_deepseek"
        | "openai_chat_cached_tokens"
        | "openai_chat_reasoning_separate"
        | "kimi" => WireFamilyId::OpenAiChat,
        "gemini" => WireFamilyId::Gemini,
        "code_assist" => WireFamilyId::GoogleCodeAssist,
        "bedrock_converse" => WireFamilyId::BedrockConverse,
        other => panic!("unknown usage-golden family {other}"),
    }
}

fn family(family_id: WireFamilyId) -> &'static dyn WireFamily {
    match family_id {
        WireFamilyId::AnthropicMessages => &AnthropicMessages,
        WireFamilyId::OpenAiResponses => &OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt => &OpenAiResponsesChatgpt,
        WireFamilyId::OpenAiChat => &OpenAiChat,
        WireFamilyId::Gemini => &Gemini,
        WireFamilyId::GoogleCodeAssist => &GoogleCodeAssist,
        WireFamilyId::BedrockConverse => &BedrockConverse,
    }
}

fn recorded_stream(family: &str, raw_usage: &Value) -> Vec<u8> {
    match family {
        "anthropic_messages" => anthropic_stream(raw_usage),
        "openai_responses" | "chatgpt_codex" => responses_stream(raw_usage),
        "openai_chat_deepseek"
        | "openai_chat_cached_tokens"
        | "openai_chat_reasoning_separate"
        | "kimi" => openai_chat_stream(raw_usage),
        "gemini" => gemini_stream(raw_usage, false),
        "code_assist" => gemini_stream(raw_usage, true),
        "bedrock_converse" => bedrock_stream(raw_usage),
        other => panic!("unknown usage-golden family {other}"),
    }
}

fn append_sse_data(stream: &mut Vec<u8>, value: &Value) {
    stream.extend_from_slice(format!("data: {value}\n\n").as_bytes());
}

fn anthropic_stream(raw_usage: &Value) -> Vec<u8> {
    let mut stream = Vec::new();
    for event in [
        json!({"type":"message_start","message":{"id":"msg_usage","type":"message","role":"assistant","model":"claude-test","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":raw_usage}),
        json!({"type":"message_stop"}),
    ] {
        append_sse_data(&mut stream, &event);
    }
    stream
}

fn responses_stream(raw_usage: &Value) -> Vec<u8> {
    let mut stream = Vec::new();
    append_sse_data(
        &mut stream,
        &json!({"type":"response.created","response":{"id":"resp_usage","status":"in_progress"}}),
    );
    append_sse_data(
        &mut stream,
        &json!({"type":"response.completed","response":{"id":"resp_usage","status":"completed","usage":raw_usage}}),
    );
    stream
}

fn openai_chat_stream(raw_usage: &Value) -> Vec<u8> {
    let mut stream = Vec::new();
    append_sse_data(
        &mut stream,
        &json!({"id":"chatcmpl-usage","choices":[{"index":0,"delta":{"content":"ok"}}]}),
    );
    append_sse_data(
        &mut stream,
        &json!({"id":"chatcmpl-usage","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
    );
    append_sse_data(
        &mut stream,
        &json!({"id":"chatcmpl-usage","choices":[],"usage":raw_usage}),
    );
    stream.extend_from_slice(b"data: [DONE]\n\n");
    stream
}

fn gemini_stream(raw_usage: &Value, wrapped: bool) -> Vec<u8> {
    let response = json!({
        "candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"},"finishReason":"STOP","index":0}],
        "usageMetadata":raw_usage
    });
    let event = if wrapped {
        json!({"response":response})
    } else {
        response
    };
    let mut stream = Vec::new();
    append_sse_data(&mut stream, &event);
    stream
}

fn bedrock_stream(raw_usage: &Value) -> Vec<u8> {
    let events = [
        ("messageStart", json!({"role":"assistant"})),
        (
            "contentBlockDelta",
            json!({"contentBlockIndex":0,"delta":{"text":"ok"}}),
        ),
        ("contentBlockStop", json!({"contentBlockIndex":0})),
        ("metadata", json!({"usage":raw_usage})),
        ("messageStop", json!({"stopReason":"end_turn"})),
    ];
    let mut stream = Vec::new();
    for (event_type, payload) in events {
        stream.extend(crate::bedrock_converse_conformance::event_stream_message(
            event_type, &payload,
        ));
    }
    stream
}
