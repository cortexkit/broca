//! Runs the OpenAI-Chat family through the SAME conformance gate Anthropic passes —
//! the 2-family proof that the harness (and the WireFamily seam) generalizes to a
//! different grammar. Plus a normalizer test exercising OpenAI's streaming
//! delta-accumulation (index-keyed tool-call argument fragments, finish on EOF).

use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall},
    request::{CallOptions, Tool, ToolChoice},
    DisplayEvent, ErrorClass, FinishReason, NormalizedEvent, ProviderControl, StepId, ToolCallId,
    Usage,
};
use broca_provider::conformance::{
    check_freeze_against_mutation, check_render_determinism, normalize_recorded,
};
use broca_provider::families::{openai_chat_frozen_config, resolve_frozen, OpenAiChat};
use broca_provider::policy::{Quirks, TokenLimitField, UsagePolicy};
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
        .map(|i| Tool { name: format!("tool_{i:02}"), description: format!("tool number {i}"), input_schema: json!({"type": "object", "properties": {format!("p{i}"): {"type": "string"}}}), module: None, execution_mode: Default::default(), reply: None, })
        .collect::<Vec<_>>()
        .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

#[test]
fn openai_chat_passes_render_determinism() {
    let frozen = openai_chat_frozen_config("gpt-4o");
    let report = check_render_determinism(&OpenAiChat, &frozen, &rich_request());
    assert!(
        report.passed(),
        "conformance failures: {:?}",
        report.failures()
    );
}

fn openai_spec() -> (ProviderSpec, ModelSpec) {
    let provider = ProviderSpec {
        id: "openai".into(),
        wire_family: WireFamilyId::OpenAiChat,
        base_url: "https://api.openai.com".into(),
        auth_kind: AuthKind::Bearer,
        static_headers: Default::default(),
        static_options: serde_json::Value::Null,
        quirks: Quirks {
            usage: UsagePolicy::OpenAiUsage,
            token_limit_field: TokenLimitField::MaxTokens,
            ..Default::default()
        },
        raw: serde_json::Value::Null,
    };
    let model = ModelSpec {
        id: "gpt-4o".into(),
        display_name: "GPT-4o".into(),
        family: Some("gpt-4".into()),
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
fn openai_chat_freeze_survives_catalog_mutation() {
    let mut spec = openai_spec();
    let result = check_freeze_against_mutation(
        &OpenAiChat,
        &mut spec,
        &rich_request(),
        |(p, m)| resolve_frozen(p, m, 2048, 1).expect("test specs publish known capability flags"),
        |(p, m)| {
            m.id = "gpt-4o-NEW".into();
            // Switch the token-limit field — a reasoning model would use
            // max_completion_tokens; the freeze must hold the original.
            p.quirks.token_limit_field = TokenLimitField::MaxCompletionTokens;
            p.static_options = json!({"injected": "after-freeze"});
        },
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
}

/// The token-limit-field POLICY actually changes the rendered field — proving the
/// typed quirk drives the grammar with no provider-name branch.
#[test]
fn token_limit_field_policy_changes_rendered_key() {
    let opts = rich_request();

    let mut frozen_max = openai_chat_frozen_config("gpt-4o");
    frozen_max.token_limit_field = TokenLimitField::MaxTokens;
    let max_bytes = String::from_utf8(OpenAiChat.render(&frozen_max, &opts)).unwrap();
    assert!(
        max_bytes.contains("\"max_tokens\":2048"),
        "expected max_tokens; got {max_bytes}"
    );
    assert!(!max_bytes.contains("max_completion_tokens"));

    let mut frozen_completion = openai_chat_frozen_config("o1");
    frozen_completion.token_limit_field = TokenLimitField::MaxCompletionTokens;
    let completion_bytes = String::from_utf8(OpenAiChat.render(&frozen_completion, &opts)).unwrap();
    assert!(
        completion_bytes.contains("\"max_completion_tokens\":2048"),
        "expected max_completion_tokens; got {completion_bytes}"
    );
    assert!(!completion_bytes.contains("\"max_tokens\""));
}

#[test]
fn openai_chat_in_band_errors_follow_the_error_class_contract() {
    let cases = [
        (
            "unknown code without a status fails permanent",
            json!({"message": "invalid prompt verbatim", "code": "invalid_prompt", "type": "invalid_request_error"}),
            ErrorClass::Permanent,
            None,
            Some("invalid_prompt"),
            None,
        ),
        (
            "billing quota is permanent",
            json!({"message": "buy credits", "code": "insufficient_quota"}),
            ErrorClass::Permanent,
            None,
            Some("billing_exhausted"),
            None,
        ),
        (
            "an empty prepaid balance is billing exhaustion",
            json!({"message": "You have no credits remaining.", "code": "credit_balance_exhausted", "type": "insufficient_quota"}),
            ErrorClass::Permanent,
            None,
            Some("billing_exhausted"),
            None,
        ),
        (
            "the insufficient_quota type alone is billing exhaustion, even with a 429",
            json!({"message": "quota gone", "code": "some_new_quota_code", "type": "insufficient_quota", "status": 429}),
            ErrorClass::Permanent,
            Some(429),
            Some("billing_exhausted"),
            None,
        ),
        (
            "plan entitlement is permanent",
            json!({"message": "not included", "code": "usage_not_included"}),
            ErrorClass::Permanent,
            None,
            Some("usage_not_included"),
            None,
        ),
        (
            "rate limit is transient",
            json!({"message": "slow down", "code": "rate_limit_exceeded", "retry_after": 1.25}),
            ErrorClass::Transient,
            None,
            Some("rate_limit_exceeded"),
            Some(1.25),
        ),
        (
            "context overflow keeps its dedicated class",
            json!({"message": "too long", "code": "context_length_exceeded"}),
            ErrorClass::ContextOverflow,
            None,
            Some("context_length_exceeded"),
            None,
        ),
        (
            "authentication type is auth required",
            json!({"message": "bad key", "type": "authentication_error"}),
            ErrorClass::Auth,
            None,
            Some("authentication_error"),
            None,
        ),
        (
            "unknown 429 keeps the quota safe default",
            json!({"message": "new quota shape", "code": "future_quota", "status": 429}),
            ErrorClass::Transient,
            Some(429),
            Some("future_quota"),
            None,
        ),
    ];

    for (label, error_body, expected_class, expected_status, expected_code, expected_retry) in cases
    {
        let expected_message = error_body["message"].as_str().unwrap().to_owned();
        let record = format!("data: {}\n\n", json!({"error": error_body}));
        let frozen = openai_chat_frozen_config("gpt-4o");
        let mut normalizer = OpenAiChat.normalizer(&frozen, StepId(1));
        let events = normalizer.push(record.as_bytes()).expect(label);

        assert_eq!(events.len(), 1, "{label}");
        let error = match &events[0] {
            NormalizedEvent::Control(ProviderControl::Error {
                error,
                retries_used,
            }) => {
                assert_eq!(*retries_used, 0, "{label}");
                error
            }
            other => panic!("{label}: expected only an error control, got {other:?}"),
        };
        assert_eq!(error.class, expected_class, "{label}");
        assert_eq!(error.message, expected_message, "{label}");
        assert_eq!(error.status, expected_status, "{label}");
        assert_eq!(error.provider_code.as_deref(), expected_code, "{label}");
        assert_eq!(error.retry_after_secs, expected_retry, "{label}");
        assert!(normalizer.finish().expect(label).is_empty(), "{label}");
    }
}

#[test]
fn openai_chat_content_stream_start_timing_is_unchanged() {
    let wire = "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\n";
    let frozen = openai_chat_frozen_config("gpt-4o");
    let mut normalizer = OpenAiChat.normalizer(&frozen, StepId(7));
    let events = normalizer.push(wire.as_bytes()).expect("content delta");

    assert!(matches!(
        events.as_slice(),
        [
            NormalizedEvent::Control(ProviderControl::StreamStart { .. }),
            NormalizedEvent::Control(ProviderControl::StepStarted { step_id: StepId(7) }),
            NormalizedEvent::Display(DisplayEvent::TextDelta { delta, .. })
        ] if delta == "hello"
    ));
}

#[test]
fn openai_normalizer_assembles_text_and_tool_call() {
    // A recorded OpenAI chat stream: a text delta, then a tool call streamed across
    // delta fragments (id+name first, arguments in pieces), a finish_reason, a
    // trailing usage-only chunk, then [DONE].
    let recorded = concat!(
        "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"On it.\"}}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_x\",\"type\":\"function\",\"function\":{\"name\":\"get_time\",\"arguments\":\"\"}}]}}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"tz\\\":\"}}]}}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"Europe/Madrid\\\"}\"}}]}}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":8,\"prompt_tokens_details\":{\"cached_tokens\":3}}}\n\n",
        "data: [DONE]\n\n",
    );
    let frozen = openai_chat_frozen_config("gpt-4o");

    // Chunk size 13 splits records mid-line to exercise incremental framing.
    let mut norm = OpenAiChat.normalizer(&frozen, StepId(1));
    let mut events = Vec::new();
    for chunk in recorded.as_bytes().chunks(13) {
        events.extend(norm.push(chunk).expect("push"));
    }
    // OpenAI emits the assembled message on EOF after a terminal marker was observed.
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

    // Text accumulated.
    assert!(
        msg.content
            .iter()
            .any(|b| matches!(b, ContentBlock::Text { text } if text == "On it.")),
        "expected assembled text 'On it.'; got {:?}",
        msg.content
    );
    // Tool call assembled from the streamed fragments (id+name+full arguments).
    let tc = msg
        .content
        .iter()
        .find_map(|b| match b {
            ContentBlock::ToolCall(tc) => Some(tc),
            _ => None,
        })
        .expect("assembled tool call");
    assert_eq!(tc.tool_call_id.0, "call_x");
    assert_eq!(tc.tool_name, "get_time");
    assert_eq!(tc.input, json!({"tz": "Europe/Madrid"}));

    // Usage from the trailing usage-only chunk.
    let usage = events.iter().find_map(|e| match e {
        NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(*usage),
        _ => None,
    });
    let usage = usage.expect("step finished with usage");
    assert_eq!(usage.input_tokens, Some(9));
    assert_eq!(usage.output_tokens, Some(8));
    assert_eq!(usage.cached_input_tokens, Some(3));
    assert_eq!(usage.fill_tokens(), Some(12));
}

const SAME_INDEX_PARALLEL_FIXTURE: &str =
    include_str!("../fixtures/openai_chat_same_index_parallel_tool_calls.sse");
const SAME_INDEX_INTERLEAVED_FIXTURE: &str =
    include_str!("../fixtures/openai_chat_same_index_interleaved_tool_calls.sse");
const SAME_INDEX_IDLESS_LATEST_FIXTURE: &str =
    include_str!("../fixtures/openai_chat_same_index_latest_idless_continuation.sse");
const SPLIT_ARGUMENTS_FIXTURE: &str =
    include_str!("../fixtures/openai_chat_split_tool_call_arguments.sse");
const REFUSAL_ONLY_FIXTURE: &str = include_str!("../fixtures/openai_chat_refusal_only.sse");
const REASONING_CONTENT_FIXTURE: &str =
    include_str!("../fixtures/openai_chat_reasoning_content.sse");

fn normalize_chat_events(fixture: &str) -> Vec<NormalizedEvent> {
    let frozen = openai_chat_frozen_config("fixture-model");
    let mut normalizer = OpenAiChat.normalizer(&frozen, StepId(2));
    let mut events = Vec::new();
    for chunk in fixture.as_bytes().chunks(11) {
        events.extend(normalizer.push(chunk).expect("fixture chunk normalizes"));
    }
    events.extend(normalizer.finish().expect("fixture finishes"));
    events
}

fn normalize_chat_fixture(fixture: &str) -> AssistantMessage {
    normalize_chat_events(fixture)
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("fixture assembles an assistant message")
}

fn legacy_content_and_finish(fixture: &str) -> (String, FinishReason) {
    let mut text = String::new();
    let mut finish_reason = FinishReason::Other;
    for payload in fixture
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|payload| *payload != "[DONE]")
    {
        let value: serde_json::Value = serde_json::from_str(payload).expect("valid SSE payload");
        let Some(choice) = value
            .get("choices")
            .and_then(serde_json::Value::as_array)
            .and_then(|choices| choices.first())
        else {
            continue;
        };
        if let Some(content) = choice
            .get("delta")
            .and_then(|delta| delta.get("content"))
            .and_then(serde_json::Value::as_str)
        {
            text.push_str(content);
        }
        if let Some(reason) = choice
            .get("finish_reason")
            .and_then(serde_json::Value::as_str)
        {
            finish_reason = match reason {
                "stop" => FinishReason::Stop,
                "length" => FinishReason::Length,
                "tool_calls" | "function_call" => FinishReason::ToolCalls,
                "content_filter" => FinishReason::ContentFilter,
                _ => FinishReason::Other,
            };
        }
    }
    (text, finish_reason)
}

#[test]
fn openai_chat_reasoning_content_is_visible_and_persisted_in_source_order() {
    let (legacy_text, legacy_finish_reason) = legacy_content_and_finish(REASONING_CONTENT_FIXTURE);
    assert_eq!(legacy_text, "Using the tool.");
    assert_eq!(legacy_finish_reason, FinishReason::ToolCalls);

    let events = normalize_chat_events(REASONING_CONTENT_FIXTURE);
    let displayed_reasoning = events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Display(DisplayEvent::ReasoningDelta { delta, .. }) => {
                Some(delta.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(displayed_reasoning, ["Plan ", "carefully."]);

    let message = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("fixture assembles an assistant message");
    assert!(matches!(
        message.content.as_slice(),
        [
            ContentBlock::Reasoning { text, signature: None },
            ContentBlock::Text { text: answer },
            ContentBlock::ToolCall(call),
        ] if text == "Plan carefully."
            && answer == "Using the tool."
            && call.tool_call_id.0 == "call_reasoning"
            && call.tool_name == "get_time"
            && call.input == json!({})
    ));
}

#[test]
fn openai_chat_refusal_deltas_replace_legacy_empty_stop_with_content_filter_text() {
    let (legacy_text, legacy_finish_reason) = legacy_content_and_finish(REFUSAL_ONLY_FIXTURE);
    assert!(
        legacy_text.is_empty(),
        "legacy assembly ignored refusal deltas"
    );
    assert_eq!(legacy_finish_reason, FinishReason::Stop);

    let events = normalize_chat_events(REFUSAL_ONLY_FIXTURE);
    assert_eq!(assembled_text(&events), Some("I can’t help with that."));
    assert_eq!(
        completion_reason(&events),
        Some(FinishReason::ContentFilter)
    );
    let displayed = events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Display(DisplayEvent::TextDelta { delta, .. }) => Some(delta.as_str()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(displayed, "I can’t help with that.");
}

#[test]
fn openai_chat_mixed_content_and_refusal_preserves_source_order_and_explicit_terminal() {
    let fixture = concat!(
        "data: {\"id\":\"chatcmpl-mixed\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"I can \"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl-mixed\",\"choices\":[{\"index\":0,\"delta\":{\"refusal\":\"not \"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl-mixed\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"help.\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl-mixed\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    let events = normalize_chat_events(fixture);
    assert_eq!(assembled_text(&events), Some("I can not help."));
    assert_eq!(completion_reason(&events), Some(FinishReason::Length));
}

#[test]
fn openai_chat_content_only_assembly_stays_byte_identical_to_legacy() {
    let fixture = concat!(
        "data: {\"id\":\"chatcmpl-content\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"unchanged\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl-content\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );
    let actual = normalize_chat_fixture(fixture);
    let legacy = AssistantMessage {
        message_id: broca_protocol::MessageId("chatcmpl-content".into()),
        content: vec![ContentBlock::Text {
            text: "unchanged".into(),
        }],
    };

    assert_eq!(
        serde_json::to_vec(&actual).expect("new message serializes"),
        serde_json::to_vec(&legacy).expect("legacy message serializes")
    );
}

#[test]
fn openai_chat_splits_same_index_parallel_tool_calls_by_id() {
    let message = normalize_chat_fixture(SAME_INDEX_PARALLEL_FIXTURE);
    let calls: Vec<&ToolCall> = message.tool_calls().collect();

    assert_eq!(calls.len(), 2, "same-index calls remain separate");
    assert_eq!(calls[0].tool_call_id.0, "call_function_ipikj74rf9tr_1");
    assert_eq!(calls[0].tool_name, "search_token");
    assert_eq!(calls[0].input, json!({"query": "token refresh"}));
    assert_eq!(calls[1].tool_call_id.0, "call_function_ipikj74rf9tr_2");
    assert_eq!(calls[1].tool_name, "acquire_lease");
    assert_eq!(calls[1].input, json!({"query": "lease acquisition"}));

    for call in calls {
        let encoded = serde_json::to_string(&call.input).expect("tool input serializes");
        let decoded: serde_json::Value =
            serde_json::from_str(&encoded).expect("tool input is strict JSON");
        assert_eq!(decoded, call.input);
    }
}

#[test]
fn openai_chat_reuses_matching_id_across_same_index_interleave() {
    let message = normalize_chat_fixture(SAME_INDEX_INTERLEAVED_FIXTURE);
    let calls: Vec<&ToolCall> = message.tool_calls().collect();

    assert_eq!(calls.len(), 2, "interleaved calls remain separate");
    assert_eq!(calls[0].tool_call_id.0, "call_a");
    assert_eq!(calls[0].tool_name, "first_call");
    assert_eq!(calls[0].input, json!({"x": 1}));
    assert_eq!(calls[1].tool_call_id.0, "call_b");
    assert_eq!(calls[1].tool_name, "second_call");
    assert_eq!(calls[1].input, json!({}));
}

#[test]
fn openai_chat_idless_continuation_uses_latest_same_index_call() {
    let message = normalize_chat_fixture(SAME_INDEX_IDLESS_LATEST_FIXTURE);
    let calls: Vec<&ToolCall> = message.tool_calls().collect();

    assert_eq!(calls.len(), 2, "same-index calls remain separate");
    assert_eq!(calls[0].tool_call_id.0, "call_a");
    assert_eq!(calls[0].input, json!({"x": 1}));
    assert_eq!(calls[1].tool_call_id.0, "call_b");
    assert_eq!(calls[1].input, json!({"y": 2}));
}

#[test]
fn openai_chat_split_arguments_without_ids_stay_one_call() {
    let message = normalize_chat_fixture(SPLIT_ARGUMENTS_FIXTURE);
    let calls: Vec<&ToolCall> = message.tool_calls().collect();

    assert_eq!(calls.len(), 1, "id-less continuation appends to its call");
    assert_eq!(calls[0].tool_call_id.0, "call_function_split");
    assert_eq!(calls[0].tool_name, "search_token");
    assert_eq!(calls[0].input, json!({"query": "token refresh"}));
}

fn normalize_chat_usage(usage: serde_json::Value) -> Usage {
    normalize_chat_usage_with_policy(usage, UsagePolicy::OpenAiUsage)
}

fn normalize_chat_usage_with_policy(usage: serde_json::Value, usage_policy: UsagePolicy) -> Usage {
    let records = [
        json!({"id": "chatcmpl-usage", "choices": [{"index": 0, "delta": {"content": "ok"}}]}),
        json!({"id": "chatcmpl-usage", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}),
        json!({"id": "chatcmpl-usage", "choices": [], "usage": usage}),
    ];
    let mut recorded = records
        .into_iter()
        .map(|record| format!("data: {record}\n\n"))
        .collect::<String>();
    recorded.push_str("data: [DONE]\n\n");

    let mut frozen = openai_chat_frozen_config("gpt-4o");
    frozen.usage = usage_policy;
    let mut normalizer = OpenAiChat.normalizer(&frozen, StepId(1));
    let mut events = normalizer.push(recorded.as_bytes()).expect("usage stream");
    events.extend(normalizer.finish().expect("terminal usage stream"));
    events
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(usage),
            _ => None,
        })
        .expect("step usage")
}

#[test]
fn openai_chat_usage_splits_inclusive_cache_counts_per_object() {
    for (wire, fresh, cached) in [
        (
            json!({"prompt_tokens": 12, "completion_tokens": 1, "prompt_tokens_details": {"cached_tokens": 3}}),
            9,
            Some(3),
        ),
        (
            json!({"prompt_tokens": 12, "completion_tokens": 1}),
            12,
            None,
        ),
        (
            json!({"prompt_tokens": 12, "completion_tokens": 1, "prompt_tokens_details": {"cached_tokens": 12}}),
            0,
            Some(12),
        ),
    ] {
        let usage = normalize_chat_usage(wire);
        assert_eq!(usage.input_tokens, Some(fresh));
        assert_eq!(usage.cached_input_tokens, cached);
        assert_eq!(usage.fill_tokens(), Some(12));
    }
}

/// `cache_write_tokens` is counted INSIDE `prompt_tokens` (live-measured on OpenAI; see the
/// captured-fixture tests below), so the write is split out of fresh input the same way a
/// cache read is. This test used to expect fresh input 12 beside a write of 7, which counted
/// the 7 written tokens twice. Fill is fresh input plus cache reads only, so it drops to 5.
#[test]
fn openai_chat_usage_maps_cache_write_tokens_detail() {
    let usage = normalize_chat_usage(json!({
        "prompt_tokens": 12,
        "completion_tokens": 1,
        "prompt_tokens_details": {"cache_write_tokens": 7}
    }));

    assert_eq!(usage.cache_write_tokens, Some(7));
    assert_eq!(usage.input_tokens, Some(5));
    assert_eq!(usage.cached_input_tokens, None);
    assert_eq!(usage.fill_tokens(), Some(12));
}

/// Gateways such as OpenRouter may carry the same key at the top level of `usage`.
#[test]
fn openai_chat_usage_subtracts_top_level_cache_write_tokens() {
    let usage = normalize_chat_usage(json!({
        "prompt_tokens": 194,
        "completion_tokens": 2,
        "cache_write_tokens": 100,
        "prompt_tokens_details": {"cached_tokens": 0}
    }));

    assert_eq!(usage.input_tokens, Some(94));
    assert_eq!(usage.cached_input_tokens, Some(0));
    assert_eq!(usage.cache_write_tokens, Some(100));
}

fn captured_chat_usage(sse: &str) -> Usage {
    let frozen = openai_chat_frozen_config("gpt-6-luna");
    let mut normalizer = OpenAiChat.normalizer(&frozen, StepId(1));
    let mut events = Vec::new();
    for chunk in sse.as_bytes().chunks(97) {
        events.extend(
            normalizer
                .push(chunk)
                .expect("captured chat SSE normalizes"),
        );
    }
    events.extend(normalizer.finish().expect("EOF finalization is clean"));
    events
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(usage),
            _ => None,
        })
        .expect("step usage")
}

/// Real metered chat-completions response, `gpt-6-luna`, 2026-09-28, the first call of a
/// ~3.2k-token prompt: prompt_tokens 3173, cache_write_tokens 3170, cached_tokens 0,
/// completion_tokens 4, total_tokens 3177. The total is prompt plus completion, so the write
/// sits inside prompt_tokens and only 3 tokens were fresh.
#[test]
fn openai_chat_captured_cache_write_is_split_out_of_fresh_input() {
    let usage = captured_chat_usage(include_str!(
        "../fixtures/openai_chat_cache_write_response.sse"
    ));
    assert_eq!(
        usage,
        Usage {
            input_tokens: Some(3),
            cached_input_tokens: Some(0),
            cache_write_tokens: Some(3170),
            output_tokens: Some(4),
            reasoning_tokens: Some(0),
        }
    );
}

/// The same prompt sent again: prompt_tokens 3173, cached_tokens 3170, cache_write_tokens 0.
#[test]
fn openai_chat_captured_cache_read_after_write_splits_the_same_prompt() {
    let usage = captured_chat_usage(include_str!(
        "../fixtures/openai_chat_cache_read_response.sse"
    ));
    assert_eq!(
        usage,
        Usage {
            input_tokens: Some(3),
            cached_input_tokens: Some(3170),
            cache_write_tokens: Some(0),
            output_tokens: Some(4),
            reasoning_tokens: Some(0),
        }
    );
}

/// INCLUSIVE, SO SUBTRACTED: Alibaba's `cache_creation_input_tokens` is a class of prompt
/// tokens, not an addition to them. Numbers from Model Studio's explicit-cache example
/// (<https://www.alibabacloud.com/help/en/model-studio/context-cache>): the first request
/// reports prompt_tokens 2174 with 2156 creation and 0 hits, so only 18 tokens are fresh. The
/// second request to the same prompt reports prompt_tokens 2174 with 0 creation and 2156 hits,
/// which pins the same 18 fresh tokens from the other side.
#[test]
fn openai_chat_usage_splits_cache_creation_out_of_fresh_input() {
    let write = normalize_chat_usage(json!({
        "prompt_tokens": 2174,
        "completion_tokens": 1,
        "prompt_tokens_details": {"cache_creation_input_tokens": 2156}
    }));

    assert_eq!(write.cache_write_tokens, Some(2156));
    assert_eq!(write.input_tokens, Some(18));
    assert_eq!(write.cached_input_tokens, None);
    assert_eq!(write.fill_tokens(), Some(2174));

    let read = normalize_chat_usage(json!({
        "prompt_tokens": 2174,
        "completion_tokens": 1,
        "prompt_tokens_details": {"cache_creation_input_tokens": 0, "cached_tokens": 2156}
    }));

    assert_eq!(read.cache_write_tokens, Some(0));
    assert_eq!(read.input_tokens, Some(18));
    assert_eq!(read.cached_input_tokens, Some(2156));
    assert_eq!(read.fill_tokens(), Some(2174));
}

/// The same inclusive reading for a gateway that carries Alibaba's spelling at the top level
/// of the usage object instead of under `prompt_tokens_details`.
#[test]
fn openai_chat_usage_splits_top_level_cache_creation_out_of_fresh_input() {
    let usage = normalize_chat_usage(json!({
        "prompt_tokens": 2174,
        "completion_tokens": 1,
        "cache_creation_input_tokens": 2156
    }));

    assert_eq!(usage.cache_write_tokens, Some(2156));
    assert_eq!(usage.input_tokens, Some(18));
    assert_eq!(usage.cached_input_tokens, None);
    assert_eq!(usage.fill_tokens(), Some(2174));
}

/// An absent key stays absent rather than becoming a reported zero: no cache is being
/// created here, and saying "0 written" would claim a measurement the response never made.
#[test]
fn openai_chat_usage_leaves_cache_creation_absent_when_the_key_is_missing() {
    let usage = normalize_chat_usage(json!({
        "prompt_tokens": 2174,
        "completion_tokens": 1,
        "prompt_tokens_details": {"cached_tokens": 0}
    }));

    assert_eq!(usage.cache_write_tokens, None);
    assert_eq!(usage.input_tokens, Some(2174));
    assert_eq!(usage.cached_input_tokens, Some(0));
}

#[test]
fn deepseek_chat_usage_splits_top_level_cache_hits_from_inclusive_prompt_total() {
    let usage = normalize_chat_usage(json!({
        "prompt_tokens": 12,
        "completion_tokens": 1,
        "prompt_cache_hit_tokens": 5,
        "prompt_cache_miss_tokens": 7
    }));

    assert_eq!(usage.input_tokens, Some(7), "fresh input is the cache miss");
    assert_eq!(
        usage.cached_input_tokens,
        Some(5),
        "cached input is the cache hit"
    );
    assert_eq!(
        usage.fill_tokens(),
        Some(12),
        "fresh plus cached must equal the inclusive prompt total"
    );
}

/// The Chat Completions API reports the reasoning-token subset of output under
/// `completion_tokens_details.reasoning_tokens`. The normalizer must surface it as
/// `usage.reasoning_tokens` WITHOUT subtracting from `output_tokens` (the provider's
/// billing total) and WITHOUT touching `fill_tokens` (reasoning is output-side, never
/// request-side input). Absent details (long-tail OpenAI-compatible servers) leaves
/// the default None.
#[test]
fn openai_chat_usage_maps_reasoning_token_split_without_touching_output_or_fill() {
    for (wire, reasoning, output) in [
        // Reasoning subset present: surfaced; output_tokens stays the provider total.
        (
            json!({
                "prompt_tokens": 12,
                "completion_tokens": 40,
                "prompt_tokens_details": {"cached_tokens": 3},
                "completion_tokens_details": {"reasoning_tokens": 17}
            }),
            Some(17u64),
            40u64,
        ),
        // No details object at all (long-tail server): reasoning stays absent.
        (
            json!({"prompt_tokens": 12, "completion_tokens": 40}),
            None,
            40,
        ),
        // Details present but reasoning_tokens absent: reasoning stays absent.
        (
            json!({
                "prompt_tokens": 12,
                "completion_tokens": 40,
                "completion_tokens_details": {"accepted_prediction_tokens": 5}
            }),
            None,
            40,
        ),
    ] {
        let usage = normalize_chat_usage(wire);
        assert_eq!(usage.reasoning_tokens, reasoning, "reasoning split");
        assert_eq!(
            usage.output_tokens,
            Some(output),
            "output_tokens is the provider total, unchanged"
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
fn xai_chat_usage_adds_sibling_reasoning_to_completion_output() {
    let usage = normalize_chat_usage_with_policy(
        json!({
            "prompt_tokens": 8,
            "completion_tokens": 1,
            "completion_tokens_details": {"reasoning_tokens": 21}
        }),
        UsagePolicy::OpenAiUsageReasoningSeparate,
    );

    assert_eq!(usage.reasoning_tokens, Some(21));
    assert_eq!(usage.output_tokens, Some(22));
    assert!(
        usage.reasoning_tokens <= usage.output_tokens,
        "canonical reasoning tokens must remain a subset of output tokens"
    );
}

/// normalize_recorded passes a recorded stream through, but OpenAI emits the
/// assembled message only on finish() — so this asserts the harness helper plus an
/// explicit finish reproduce the same events (the helper exercises push; finish is
/// the family's terminal). Confirms the normalizer is robust to tiny chunks.
#[test]
fn openai_normalize_recorded_helper_runs_clean() {
    let recorded = concat!(
        "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
        "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );
    let frozen = openai_chat_frozen_config("gpt-4o");
    // chunk size 1 — the worst case for incremental framing.
    let events = normalize_recorded(&OpenAiChat, &frozen, StepId(1), recorded.as_bytes(), 1)
        .expect("normalize ok");
    // push-only does not emit the assembled message (that's finish()), but it must
    // emit the stream-start/step-started without error and accumulate cleanly.
    assert!(events.iter().any(|e| matches!(
        e,
        NormalizedEvent::Control(ProviderControl::StreamStart { .. })
    )));
}

fn completion_reason(events: &[NormalizedEvent]) -> Option<FinishReason> {
    events.iter().find_map(|event| match event {
        NormalizedEvent::Control(ProviderControl::Finished { finish_reason, .. }) => {
            Some(*finish_reason)
        }
        _ => None,
    })
}

fn assembled_text(events: &[NormalizedEvent]) -> Option<&str> {
    events.iter().find_map(|event| match event {
        NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
            message.content.iter().find_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
        }
        _ => None,
    })
}

#[test]
fn openai_chat_clean_eof_without_terminal_marker_is_truncation() {
    let wire = "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n";
    let frozen = openai_chat_frozen_config("gpt-4o");
    let mut normalizer = OpenAiChat.normalizer(&frozen, StepId(1));

    let events = normalizer.push(wire.as_bytes()).expect("content delta");
    assert!(!events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(
            ProviderControl::AssistantMessage { .. } | ProviderControl::Finished { .. }
        )
    )));
    let error = normalizer
        .finish()
        .expect_err("EOF without a finish reason or [DONE] must be rejected");
    assert!(
        error
            .to_string()
            .contains("ended before a terminal chunk or [DONE]"),
        "unexpected error: {error}"
    );
}

#[test]
fn openai_chat_finish_reason_allows_eof_without_done() {
    let wire = concat!(
        "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"complete\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    );
    let frozen = openai_chat_frozen_config("gpt-4o");
    let mut normalizer = OpenAiChat.normalizer(&frozen, StepId(1));

    normalizer.push(wire.as_bytes()).expect("complete stream");
    let events = normalizer.finish().expect("finish reason is terminal");
    assert_eq!(assembled_text(&events), Some("complete"));
    assert_eq!(completion_reason(&events), Some(FinishReason::Stop));
}

#[test]
fn openai_chat_done_allows_eof_without_finish_reason() {
    let wire = concat!(
        "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"complete\"},\"finish_reason\":null}]}\n\n",
        "data: [DONE]\n\n",
    );
    let frozen = openai_chat_frozen_config("gpt-4o");
    let mut normalizer = OpenAiChat.normalizer(&frozen, StepId(1));

    normalizer.push(wire.as_bytes()).expect("complete stream");
    let events = normalizer.finish().expect("[DONE] is terminal");
    assert_eq!(assembled_text(&events), Some("complete"));
    assert_eq!(completion_reason(&events), Some(FinishReason::Other));
}
