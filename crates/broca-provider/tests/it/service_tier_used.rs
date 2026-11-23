//! The service tier a provider REPORTS having served a step at, per wire family.
//!
//! Every family records the tier exactly as the provider spelled it, from wherever that
//! family reports it, and records nothing when the response reports nothing. It is never
//! taken from the request: providers downgrade a requested tier (OpenAI priority to default
//! on traffic ramp-up, Gemini priority to standard on congestion) and bill the tier they used.
//!
//! Synthetic fixtures preserve the provider field placement and terminal tier transitions
//! in tests/live/captures and tests/fixtures without retaining real response content.

use broca_protocol::{FinishReason, NormalizedEvent, ProviderControl, StepId};
use broca_provider::families::{
    anthropic_frozen_config, gemini_frozen_config, google_codeassist_frozen_config,
    openai_chat_frozen_config, openai_responses_chatgpt_frozen_config,
    openai_responses_frozen_config, AnthropicMessages, Gemini, GoogleCodeAssist, OpenAiChat,
    OpenAiResponses, OpenAiResponsesChatgpt,
};
use broca_provider::wire::{FrozenRenderConfig, ResponseHeaders, WireFamily};

/// What a step's `StepFinished` said about the tier and speed it was served at.
#[derive(Debug, PartialEq, Eq)]
struct Served {
    service_tier_used: Option<String>,
    speed_used: Option<String>,
}

/// Feed `headers` (as the transport does, before the body) and then `body` through a fresh
/// normalizer of `family`, and return what the single `StepFinished` reported.
fn served<F: WireFamily>(
    family: &F,
    frozen: &FrozenRenderConfig,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Served {
    let mut normalizer = family.normalizer(frozen, StepId(1));
    normalizer.observe_response_headers(&ResponseHeaders::new(headers.iter().copied()));
    let mut events = Vec::new();
    // Small chunks, so a value split across network reads is still read whole.
    for chunk in body.chunks(61) {
        events.extend(normalizer.push(chunk).expect("stream normalizes"));
    }
    events.extend(normalizer.finish().expect("stream ends cleanly"));
    let finished = events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished {
                service_tier_used,
                speed_used,
                finish_reason,
                ..
            }) => Some((
                service_tier_used.clone(),
                speed_used.clone(),
                *finish_reason,
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(finished.len(), 1, "exactly one StepFinished: {events:?}");
    let (service_tier_used, speed_used, finish_reason) = finished.into_iter().next().unwrap();
    assert_ne!(
        finish_reason,
        FinishReason::Other,
        "the stream completed normally, so the fixture really reached its terminal"
    );
    Served {
        service_tier_used,
        speed_used,
    }
}

fn tier(value: &str) -> Served {
    Served {
        service_tier_used: Some(value.into()),
        speed_used: None,
    }
}

fn nothing() -> Served {
    Served {
        service_tier_used: None,
        speed_used: None,
    }
}

/// The synthetic response body stored in the live-harness artifact format.
fn capture(relative: &str) -> String {
    let path = format!(
        "{}/tests/live/captures/{relative}",
        env!("CARGO_MANIFEST_DIR")
    );
    let capture: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("read capture"))
            .expect("capture is JSON");
    let text = capture["raw_response_text"]
        .as_str()
        .expect("capture has a UTF-8 body")
        .to_owned();
    assert!(!text.is_empty(), "{relative} carries a recorded body");
    text
}

fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!(
        "{}/tests/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("read fixture")
}

/// Drop one exact substring, asserting it was there, so a fixture variant differs from the
/// recording by that field alone.
fn without(body: &str, field: &str) -> String {
    assert!(body.contains(field), "the recording carries {field}");
    body.replacen(field, "", 1)
}

// ───────────────────────── Anthropic Messages: `usage.service_tier` ─────────────────────────

fn anthropic_stream(start_usage: &str, delta_usage: &str) -> String {
    let events = [
        (
            "message_start",
            format!(
                r#"{{"type":"message_start","message":{{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"m","usage":{start_usage}}}}}"#
            ),
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#
                .into(),
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}"#
                .into(),
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#.into(),
        ),
        (
            "message_delta",
            format!(
                r#"{{"type":"message_delta","delta":{{"stop_reason":"end_turn"}},"usage":{delta_usage}}}"#
            ),
        ),
        ("message_stop", r#"{"type":"message_stop"}"#.into()),
    ];
    events
        .iter()
        .map(|(event, data)| format!("event: {event}\ndata: {data}\n\n"))
        .collect()
}

#[test]
fn anthropic_records_usage_service_tier_from_a_recorded_stream() {
    // A synthetic claude-opus-4-8-shaped stream: `"service_tier":"standard"` inside message_start's usage.
    let body = capture("anthropic/signed_thinking.json");
    assert_eq!(
        served(
            &AnthropicMessages,
            &anthropic_frozen_config("m"),
            &[],
            body.as_bytes()
        ),
        tier("standard")
    );
}

#[test]
fn anthropic_records_speed_separately_from_the_tier() {
    // Fast mode is reported as `usage.speed`, not as a tier; the terminal usage carries it.
    let body = anthropic_stream(
        r#"{"input_tokens":3,"output_tokens":1,"service_tier":"standard"}"#,
        r#"{"output_tokens":2,"speed":"fast"}"#,
    );
    assert_eq!(
        served(
            &AnthropicMessages,
            &anthropic_frozen_config("m"),
            &[],
            body.as_bytes()
        ),
        Served {
            service_tier_used: Some("standard".into()),
            speed_used: Some("fast".into()),
        }
    );
}

#[test]
fn anthropic_without_a_reported_tier_records_none() {
    let body = anthropic_stream(
        r#"{"input_tokens":3,"output_tokens":1}"#,
        r#"{"output_tokens":2}"#,
    );
    assert_eq!(
        served(
            &AnthropicMessages,
            &anthropic_frozen_config("m"),
            &[],
            body.as_bytes()
        ),
        nothing()
    );
}

// ─────────────── OpenAI Responses (platform and ChatGPT codex): `response.service_tier` ───────────────

#[test]
fn responses_records_the_terminal_tier_not_the_created_echo() {
    // A synthetic gpt-4o-mini-shaped stream: response.created and response.in_progress say "auto" (the
    // request's value, echoed before the tier is decided); response.completed says "default".
    let body = capture("openai_responses/text_completes.json");
    assert!(body.contains(r#""service_tier":"auto""#));
    assert_eq!(
        served(
            &OpenAiResponses,
            &openai_responses_frozen_config("gpt-4o-mini"),
            &[],
            body.as_bytes()
        ),
        tier("default")
    );
}

#[test]
fn chatgpt_codex_records_the_terminal_tier_from_a_recorded_stream() {
    // A synthetic ChatGPT codex backend turn: created/in_progress echo "auto", completed reports
    // "default".
    let body = fixture("codex_reasoning_turn_response.sse");
    assert_eq!(
        served(
            &OpenAiResponsesChatgpt,
            &openai_responses_chatgpt_frozen_config("gpt-5.5"),
            &[],
            body.as_bytes()
        ),
        tier("default")
    );
}

#[test]
fn responses_without_a_reported_tier_records_none() {
    let body = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"hi\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
    );
    assert_eq!(
        served(
            &OpenAiResponses,
            &openai_responses_frozen_config("m"),
            &[],
            body.as_bytes()
        ),
        nothing()
    );
}

// ─────────────────── OpenAI Chat (and xAI): top-level `service_tier` on the chunks ───────────────────

fn chat_stream(service_tier: &str) -> String {
    [
        format!(
            r#"{{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-4o-mini","service_tier":{service_tier},"choices":[{{"index":0,"delta":{{"role":"assistant","content":"hi"}},"finish_reason":null}}]}}"#
        ),
        format!(
            r#"{{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-4o-mini","service_tier":{service_tier},"choices":[{{"index":0,"delta":{{}},"finish_reason":"stop"}}]}}"#
        ),
        format!(
            r#"{{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-4o-mini","service_tier":{service_tier},"choices":[],"usage":{{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}}}"#
        ),
        "[DONE]".to_string(),
    ]
    .iter()
    .map(|data| format!("data: {data}\n\n"))
    .collect()
}

#[test]
fn chat_records_the_chunk_service_tier() {
    // No recorded OpenAI or xAI chat stream is in the repository (the chat captures are
    // DeepSeek, which reports no tier), so this is the documented chunk shape.
    assert_eq!(
        served(
            &OpenAiChat,
            &openai_chat_frozen_config("gpt-4o-mini"),
            &[],
            chat_stream(r#""default""#).as_bytes()
        ),
        tier("default")
    );
}

#[test]
fn chat_without_a_reported_tier_records_none() {
    // A live DeepSeek stream over the chat grammar: no `service_tier` key anywhere.
    let recorded = capture("openai_chat/text_completes.json");
    assert!(!recorded.contains("service_tier"));
    assert_eq!(
        served(
            &OpenAiChat,
            &openai_chat_frozen_config("deepseek-v4-flash"),
            &[],
            recorded.as_bytes()
        ),
        nothing()
    );
    // A server that sends the key as null reported nothing either.
    assert_eq!(
        served(
            &OpenAiChat,
            &openai_chat_frozen_config("gpt-4o-mini"),
            &[],
            chat_stream("null").as_bytes()
        ),
        nothing()
    );
}

/// A route that serves fast as a model of its own (Kimi's `-highspeed` twin) reports no tier
/// field. A step was served fast when the response names the twin model the request went to;
/// a response naming another model reports no speed, and a run on the base model reports none.
#[test]
fn chat_records_fast_when_the_twin_model_served_the_step() {
    let twin = |model: &str| chat_stream("null").replace("gpt-4o-mini", model);
    let mut frozen = openai_chat_frozen_config("k3.1-pro-highspeed");
    frozen.wire_mode = Some("fast".into());
    let fast = Served {
        service_tier_used: None,
        speed_used: Some("fast".into()),
    };
    assert_eq!(
        served(
            &OpenAiChat,
            &frozen,
            &[],
            twin("k3.1-pro-highspeed").as_bytes()
        ),
        fast
    );
    assert_eq!(
        served(&OpenAiChat, &frozen, &[], twin("k3.1-pro").as_bytes()),
        nothing(),
        "a response from another model was not served at the twin's mode"
    );
    assert_eq!(
        served(
            &OpenAiChat,
            &openai_chat_frozen_config("k3.1-pro-highspeed"),
            &[],
            twin("k3.1-pro-highspeed").as_bytes()
        ),
        nothing(),
        "without a frozen wire mode, a model id alone claims no speed"
    );
}

// ─────────── Gemini API: the `x-gemini-service-tier` header, and `usageMetadata.serviceTier` ───────────

/// The synthetic Gemini stream's body tier field, which the header tests remove so
/// only the header can supply a tier.
const GEMINI_BODY_TIER: &str = r#","serviceTier":"standard""#;

#[test]
fn gemini_records_the_response_header_tier() {
    let body = without(&capture("gemini/text_completes.json"), GEMINI_BODY_TIER);
    let frozen = gemini_frozen_config("gemini-2.5-flash");
    assert_eq!(
        served(
            &Gemini,
            &frozen,
            &[("X-Gemini-Service-Tier", "standard")],
            body.as_bytes()
        ),
        tier("standard")
    );
    assert_eq!(served(&Gemini, &frozen, &[], body.as_bytes()), nothing());
}

#[test]
fn gemini_records_the_body_tier_from_a_recorded_stream() {
    // The synthetic stream carries `usageMetadata.serviceTier: "standard"`.
    let body = capture("gemini/text_completes.json");
    assert_eq!(
        served(
            &Gemini,
            &gemini_frozen_config("gemini-2.5-flash"),
            &[],
            body.as_bytes()
        ),
        tier("standard")
    );
}

#[test]
fn gemini_header_wins_over_the_body_when_they_differ() {
    // The header is where Google documents the tier used; the body field is undocumented.
    let body = capture("gemini/text_completes.json");
    assert_eq!(
        served(
            &Gemini,
            &gemini_frozen_config("gemini-2.5-flash"),
            &[("x-gemini-service-tier", "priority")],
            body.as_bytes()
        ),
        tier("priority")
    );
}

#[test]
fn code_assist_reads_the_same_header_and_records_none_without_it() {
    // No Code Assist response has been recorded. Its decode is Gemini's behind a `{response}`
    // unwrap that also accepts a bare response, so the recorded Gemini body stands in for it.
    let body = without(&capture("gemini/text_completes.json"), GEMINI_BODY_TIER);
    let frozen = google_codeassist_frozen_config("gemini-2.5-flash");
    assert_eq!(
        served(
            &GoogleCodeAssist,
            &frozen,
            &[("x-gemini-service-tier", "standard")],
            body.as_bytes()
        ),
        tier("standard")
    );
    assert_eq!(
        served(&GoogleCodeAssist, &frozen, &[], body.as_bytes()),
        nothing()
    );
}
