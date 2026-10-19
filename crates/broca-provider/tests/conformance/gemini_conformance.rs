//! Gemini family conformance gate.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall},
    request::{CallOptions, Tool, ToolChoice},
    DisplayEvent, NormalizedEvent, ProviderControl, StepId, ToolCallId, Usage,
};
use broca_provider::conformance::{
    check_freeze_against_mutation, check_render_determinism, normalize_recorded,
};
use broca_provider::families::{
    gemini_frozen_config, resolve_frozen, Gemini, GEMINI_RENDERER_VERSION,
};
use broca_provider::policy::{
    Quirks, SystemPromptPolicy, TokenLimitField, ToolIdPolicy, UsagePolicy,
};
use broca_provider::spec::{
    AuthKind, Capabilities, CapabilityFlag, CostSchedule, Limits, ModelSpec, Overlay, ProviderSpec,
    WireFamilyId,
};
use broca_provider::wire::WireFamily;
use serde_json::json;

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
                tool_call_id: ToolCallId("call_1".into()),
                tool_name: "a_tool".into(),
                input: json!({"x": 1}),
            })],
        },
    ]);
    opts.tools = (0..12)
        .map(|i| Tool { name: format!("tool_{i:02}"), description: format!("tool number {i}"), input_schema: json!({"type": "object", "properties": {format!("p{i}"): {"type": "string"}}}), module: None, execution_mode: Default::default() })
        .collect::<Vec<_>>()
        .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

#[test]
fn gemini_passes_render_determinism() {
    let frozen = gemini_frozen_config("gemini-2.0-flash");
    let report = check_render_determinism(&Gemini, &frozen, &rich_request());
    assert!(
        report.passed(),
        "conformance failures: {:?}",
        report.failures()
    );
}

fn gemini_spec() -> (ProviderSpec, ModelSpec) {
    let provider = ProviderSpec {
        id: "google".into(),
        wire_family: WireFamilyId::Gemini,
        base_url: "https://generativelanguage.googleapis.com".into(),
        auth_kind: AuthKind::ApiKeyHeader {
            header: "x-goog-api-key".into(),
        },
        static_headers: Default::default(),
        static_options: serde_json::Value::Null,
        quirks: Quirks {
            usage: UsagePolicy::GeminiUsageMetadata,
            token_limit_field: TokenLimitField::GeminiMaxOutputTokens,
            system_prompt: SystemPromptPolicy::SystemInstructionField,
            tool_id: ToolIdPolicy::SyntheticIndex,
            ..Default::default()
        },
        raw: serde_json::Value::Null,
    };
    let model = ModelSpec {
        id: "gemini-2.0-flash".into(),
        display_name: "Gemini 2.0 Flash".into(),
        family: Some("gemini".into()),
        capabilities: Capabilities {
            tool_call: CapabilityFlag::Known(true),
            temperature: CapabilityFlag::Known(true),
            ..Default::default()
        },
        limits: Limits::default(),
        cost: CostSchedule::default(),
        reasoning_options: Default::default(),
        release_date: None,
        status: None,
        overlay: Overlay::default(),
        raw: serde_json::Value::Null,
    };
    (provider, model)
}

#[test]
fn gemini_freeze_survives_catalog_mutation() {
    let mut spec = gemini_spec();
    let result = check_freeze_against_mutation(
        &Gemini,
        &mut spec,
        &rich_request(),
        |(p, m)| {
            resolve_frozen(p, m, 2048, GEMINI_RENDERER_VERSION)
                .expect("test specs publish known capability flags")
        },
        |(p, m)| {
            m.id = "gemini-2.5-flash".into();
            p.quirks.token_limit_field = TokenLimitField::MaxTokens;
            p.static_options = json!({"injected": "after-freeze"});
        },
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
}

#[test]
fn token_limit_field_max_output_tokens_policy() {
    let opts = rich_request();
    let mut frozen = gemini_frozen_config("gemini-2.0-flash");
    frozen.token_limit_field = TokenLimitField::GeminiMaxOutputTokens;
    let bytes = String::from_utf8(Gemini.render(&frozen, &opts)).unwrap();
    assert!(
        bytes.contains("\"generationConfig\"") && bytes.contains("\"maxOutputTokens\":2048"),
        "expected generationConfig.maxOutputTokens; got {bytes}"
    );

    let mut frozen_openai = gemini_frozen_config("gemini-2.0-flash");
    frozen_openai.token_limit_field = TokenLimitField::MaxTokens;
    let openai_style = String::from_utf8(Gemini.render(&frozen_openai, &opts)).unwrap();
    assert!(
        openai_style.contains("\"max_tokens\":2048"),
        "policy should change output; got {openai_style}"
    );
    assert!(!openai_style.contains("generationConfig"));
}

#[test]
fn gemini_normalizer_assembles_text_and_tool_call() {
    let recorded = concat!(
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"On it.\"}],\"role\":\"model\"},\"index\":0}]}\n\n",
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"get_time\",\"args\":{\"items\":[{\"patch\":\"@@ -1 +1 @@\\n-old\\n+新\",\"path\":\"src/é.rs\"}],\"label\":\"café\"}},\"thoughtSignature\":\"gemini-signed-call\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"usageMetadata\":{\"promptTokenCount\":12,\"cachedContentTokenCount\":3,\"candidatesTokenCount\":8}}\n\n",
    );
    let frozen = gemini_frozen_config("gemini-2.0-flash");

    let mut norm = Gemini.normalizer(&frozen, StepId(1));
    let mut events = Vec::new();
    for chunk in recorded.as_bytes().chunks(13) {
        events.extend(norm.push(chunk).expect("push"));
    }
    events.extend(norm.finish().expect("finish"));

    let msg = events
        .iter()
        .find_map(|e| match e {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("assembled message");

    assert!(
        msg.content
            .iter()
            .any(|b| matches!(b, ContentBlock::Text { text } if text == "On it.")),
        "expected text; got {:?}",
        msg.content
    );
    let tc = msg
        .content
        .iter()
        .find_map(|b| match b {
            ContentBlock::ToolCall(tc) => Some(tc),
            _ => None,
        })
        .expect("tool call");
    assert_eq!(tc.tool_call_id.0, "call_0");
    assert_eq!(tc.tool_name, "get_time");
    let expected_args = json!({
        "items": [{
            "patch": "@@ -1 +1 @@\n-old\n+新",
            "path": "src/é.rs"
        }],
        "label": "café"
    });
    assert_eq!(tc.input, expected_args);
    assert_eq!(tc.thought_signature.as_deref(), Some("gemini-signed-call"));

    let (display_id, display_delta) = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Display(DisplayEvent::ToolInputDelta {
                tool_call_id,
                delta,
            }) => Some((&tool_call_id.0, delta.as_str())),
            _ => None,
        })
        .expect("tool input display delta");
    assert_eq!(display_id, "call_0");
    assert_eq!(
        display_delta,
        r#"{"items":[{"patch":"@@ -1 +1 @@\n-old\n+新","path":"src/é.rs"}],"label":"café"}"#
    );

    let usage = events.iter().find_map(|e| match e {
        NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(*usage),
        _ => None,
    });
    let usage = usage.expect("usage");
    assert_eq!(usage.input_tokens, Some(9));
    assert_eq!(usage.cached_input_tokens, Some(3));
    assert_eq!(usage.fill_tokens(), Some(12));
    assert_eq!(usage.output_tokens, Some(8));
}

/// Drive a single Gemini `GenerateContentResponse` chunk's `usageMetadata` through the
/// normalizer and return the resulting step usage.
fn normalize_gemini_usage(usage_metadata: serde_json::Value) -> Usage {
    let recorded = format!(
        "data: {{\"candidates\":[{{\"content\":{{\"parts\":[{{\"text\":\"ok\"}}],\"role\":\"model\"}},\"finishReason\":\"STOP\",\"index\":0}}],\"usageMetadata\":{usage_metadata}}}\n\n",
    );
    let frozen = gemini_frozen_config("gemini-2.5-flash");
    let mut normalizer = Gemini.normalizer(&frozen, StepId(1));
    let mut events = normalizer
        .push(recorded.as_bytes())
        .expect("gemini usage stream");
    events.extend(normalizer.finish().expect("terminal gemini usage stream"));
    events
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(usage),
            _ => None,
        })
        .expect("step usage")
}

/// Gemini's `candidatesTokenCount` is the VISIBLE-ONLY output (answer text + tool
/// calls); `thoughtsTokenCount` is a SIBLING — the thinking tokens are ADDITIONAL,
/// not included in candidatesTokenCount. Our canonical `output_tokens` is the
/// inclusive billing total (reasoning_tokens is a subset of output_tokens), so the
/// normalizer sums candidates + thoughts into `output_tokens` and sets
/// `reasoning_tokens` = thoughts. This corrects a prior undercount for thinking
/// models. Absent `thoughtsTokenCount` (non-thinking models, or any non-terminal
/// chunk) leaves `output_tokens` = candidates alone and `reasoning_tokens` absent. `fill_tokens` is input-side only, so it is
/// unaffected either way.
#[test]
fn gemini_usage_sums_thoughts_into_inclusive_output_total() {
    for (wire, reasoning, output) in [
        // thoughtsTokenCount present: reasoning = thoughts, output = candidates + thoughts
        // (the inclusive total — thoughts are a SIBLING of candidates, not a subset).
        (
            json!({
                "promptTokenCount": 12,
                "cachedContentTokenCount": 3,
                "candidatesTokenCount": 100,
                "thoughtsTokenCount": 40
            }),
            Some(40u64),
            140u64,
        ),
        // No thoughtsTokenCount (non-thinking model / non-terminal chunk): output =
        // candidates alone, reasoning absent.
        (
            json!({
                "promptTokenCount": 12,
                "cachedContentTokenCount": 3,
                "candidatesTokenCount": 100
            }),
            None,
            100,
        ),
    ] {
        let usage = normalize_gemini_usage(wire);
        assert_eq!(usage.reasoning_tokens, reasoning, "reasoning split");
        assert_eq!(
            usage.output_tokens,
            Some(output),
            "output_tokens is candidates + thoughts (inclusive total)"
        );
        // Reasoning is output-side; it must never count toward the request-side fill.
        assert_eq!(
            usage.fill_tokens(),
            Some(12),
            "fill is input + cache-read only"
        );
    }
}

#[test]
fn gemini_normalize_recorded_helper_runs_clean() {
    let recorded = concat!(
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hi\"}],\"role\":\"model\"}}]}\n\n",
        "data: {\"candidates\":[{\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":1}}\n\n",
    );
    let frozen = gemini_frozen_config("gemini-2.0-flash");
    let events = normalize_recorded(&Gemini, &frozen, StepId(1), recorded.as_bytes(), 1)
        .expect("normalize ok");
    assert!(events.iter().any(|e| matches!(
        e,
        NormalizedEvent::Control(ProviderControl::StreamStart { .. })
    )));
}

#[test]
fn gemini_clean_eof_without_finish_reason_is_truncation() {
    let recorded = concat!(
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"part\"}],\"role\":\"model\"},\"index\":0}]}\n\n",
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"ial\"}],\"role\":\"model\"},\"index\":0}]}\n\n",
    );
    let frozen = gemini_frozen_config("gemini-2.0-flash");
    let mut normalizer = Gemini.normalizer(&frozen, StepId(1));
    let mut events = Vec::new();

    for chunk in recorded.as_bytes().chunks(13) {
        events.extend(normalizer.push(chunk).expect("content delta"));
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
        .expect_err("EOF without a finish reason must be rejected");
    assert!(
        error
            .to_string()
            .contains("Gemini stream truncated: ended before a terminal chunk"),
        "unexpected error: {error}"
    );
}
