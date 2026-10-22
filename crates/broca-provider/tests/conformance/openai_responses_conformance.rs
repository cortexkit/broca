//! Conformance gate for the OpenAI-Responses wire family.

use broca_protocol::{
    content::{ContentBlock, EncryptedReasoningFormat, FinishReason, Message, Role, ToolCall},
    request::{CallOptions, Tool, ToolChoice},
    ErrorClass, NormalizedEvent, ProviderControl, StepId, ToolCallId, Usage,
};
use broca_provider::conformance::{
    check_freeze_against_mutation, check_render_determinism, normalize_recorded,
};
use broca_provider::families::{
    openai_responses_frozen_config, resolve_frozen, OpenAiResponses,
    OPENAI_RESPONSES_RENDERER_VERSION,
};
use broca_provider::policy::{
    Quirks, ReasoningPolicy, SystemPromptPolicy, TokenLimitField, UsagePolicy,
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
        .map(|i| Tool { name: format!("tool_{i:02}"), description: format!("tool number {i}"), input_schema: json!({"type": "object", "properties": {format!("p{i}"): {"type": "string"}}}), module: None, execution_mode: Default::default(), reply: None, })
        .collect::<Vec<_>>()
        .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

#[test]
fn openai_responses_passes_render_determinism() {
    let frozen = openai_responses_frozen_config("gpt-4.1");
    let report = check_render_determinism(&OpenAiResponses, &frozen, &rich_request());
    assert!(
        report.passed(),
        "conformance failures: {:?}",
        report.failures()
    );
}

fn responses_spec() -> (ProviderSpec, ModelSpec) {
    let provider = ProviderSpec {
        id: "openai".into(),
        wire_family: WireFamilyId::OpenAiResponses,
        base_url: "https://api.openai.com".into(),
        auth_kind: AuthKind::Bearer,
        static_headers: Default::default(),
        static_options: serde_json::Value::Null,
        quirks: Quirks {
            usage: UsagePolicy::OpenAiUsage,
            token_limit_field: TokenLimitField::MaxOutputTokens,
            system_prompt: SystemPromptPolicy::InstructionsField,
            ..Default::default()
        },
        raw: serde_json::Value::Null,
    };
    let model = ModelSpec {
        id: "gpt-4.1".into(),
        display_name: "GPT-4.1".into(),
        family: Some("gpt-4".into()),
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
fn openai_responses_freeze_survives_catalog_mutation() {
    let mut spec = responses_spec();
    let result = check_freeze_against_mutation(
        &OpenAiResponses,
        &mut spec,
        &rich_request(),
        |(p, m)| {
            resolve_frozen(p, m, 2048, OPENAI_RESPONSES_RENDERER_VERSION)
                .expect("test specs publish known capability flags")
        },
        |(p, m)| {
            m.id = "gpt-4.1-NEW".into();
            p.quirks.token_limit_field = TokenLimitField::MaxTokens;
            p.quirks.system_prompt = SystemPromptPolicy::SystemMessage;
            p.static_options = json!({"injected": "after-freeze"});
        },
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
}

#[test]
fn reasoning_policy_changes_responses_rendered_body() {
    let opts = rich_request();
    let mut frozen = openai_responses_frozen_config("o3");
    frozen.reasoning = ReasoningPolicy::OpenAiResponsesReasoning {
        effort: "medium".into(),
        summary: None,
    };
    let bytes = String::from_utf8(OpenAiResponses.render(&frozen, &opts)).unwrap();
    assert!(
        bytes.contains("\"reasoning\":{\"effort\":\"medium\"}"),
        "expected reasoning object in render; got {bytes}"
    );
    assert!(
        bytes.contains("\"include\":[\"reasoning.encrypted_content\"]"),
        "reasoning-enabled request must ask for continuation ciphertext: {bytes}"
    );
}

#[test]
fn non_reasoning_responses_request_omits_encrypted_reasoning_include() {
    let frozen = openai_responses_frozen_config("gpt-4.1");
    let body: serde_json::Value =
        serde_json::from_slice(&OpenAiResponses.render(&frozen, &rich_request())).unwrap();
    assert!(body.get("include").is_none());
}

#[test]
fn token_limit_field_policy_changes_responses_rendered_key() {
    let opts = rich_request();

    let mut frozen_out = openai_responses_frozen_config("gpt-4.1");
    frozen_out.token_limit_field = TokenLimitField::MaxOutputTokens;
    let out_bytes = String::from_utf8(OpenAiResponses.render(&frozen_out, &opts)).unwrap();
    assert!(
        out_bytes.contains("\"max_output_tokens\":2048"),
        "expected max_output_tokens; got {out_bytes}"
    );
    assert!(!out_bytes.contains("\"max_tokens\""));

    let mut frozen_chat = openai_responses_frozen_config("o1");
    frozen_chat.token_limit_field = TokenLimitField::MaxCompletionTokens;
    let chat_bytes = String::from_utf8(OpenAiResponses.render(&frozen_chat, &opts)).unwrap();
    assert!(
        chat_bytes.contains("\"max_completion_tokens\":2048"),
        "expected max_completion_tokens; got {chat_bytes}"
    );
}

#[test]
fn openai_responses_normalizer_assembles_text_and_tool_call() {
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"},\"sequence_number\":0}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"On it.\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"sequence_number\":1}\n\n",
        "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_x\",\"name\":\"get_time\",\"arguments\":\"\"},\"output_index\":1,\"sequence_number\":2}\n\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"{\\\"tz\\\":\",\"item_id\":\"fc_1\",\"output_index\":1,\"sequence_number\":3}\n\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"\\\"Europe/Madrid\\\"}\",\"item_id\":\"fc_1\",\"output_index\":1,\"sequence_number\":4}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":12,\"output_tokens\":8,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0},\"total_tokens\":20}},\"sequence_number\":5}\n\n",
    );
    let frozen = openai_responses_frozen_config("gpt-4.1");

    let mut norm = OpenAiResponses.normalizer(&frozen, StepId(1));
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
        "expected assembled text; got {:?}",
        msg.content
    );
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

    let usage = events.iter().find_map(|e| match e {
        NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(*usage),
        _ => None,
    });
    let usage = usage.expect("step finished with usage");
    assert_eq!(usage.input_tokens, Some(12));
    assert_eq!(usage.cached_input_tokens, Some(0));
    assert_eq!(usage.fill_tokens(), Some(12));
    assert_eq!(usage.output_tokens, Some(8));
}

fn normalize_responses(recorded: &str, step_id: u64) -> Vec<NormalizedEvent> {
    let frozen = openai_responses_frozen_config("gpt-4.1");
    let mut normalizer = OpenAiResponses.normalizer(&frozen, StepId(step_id));
    let mut events = Vec::new();
    for chunk in recorded.as_bytes().chunks(11) {
        events.extend(normalizer.push(chunk).expect("response SSE normalizes"));
    }
    events.extend(normalizer.finish().expect("response EOF is clean"));
    events
}

fn response_error(events: &[NormalizedEvent]) -> &broca_protocol::ProviderError {
    events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => Some(error),
            _ => None,
        })
        .expect("response emits a typed provider error")
}

#[test]
fn top_level_error_preserves_permanent_code_and_message() {
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_error\"}}\n\n",
        "data: {\"type\":\"error\",\"code\":\"invalid_prompt\",\"message\":\"prompt violates validation\",\"param\":null,\"sequence_number\":1}\n\n",
    );
    let events = normalize_responses(recorded, 10);
    let error = response_error(&events);

    assert_eq!(error.class, ErrorClass::Permanent);
    assert_eq!(error.provider_code.as_deref(), Some("invalid_prompt"));
    assert_eq!(error.message, "prompt violates validation");
}

#[test]
fn response_failed_preserves_transient_nested_error() {
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_failed\"}}\n\n",
        "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_failed\",\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"generation backend failed\"}},\"sequence_number\":1}\n\n",
    );
    let events = normalize_responses(recorded, 11);
    let error = response_error(&events);

    assert_eq!(error.class, ErrorClass::Transient);
    assert_eq!(error.provider_code.as_deref(), Some("server_error"));
    assert_eq!(error.message, "generation backend failed");
}

#[test]
fn nested_error_event_preserves_transient_message() {
    let events = normalize_responses(
        "data: {\"type\":\"error\",\"error\":{\"type\":\"server_error\",\"code\":null,\"message\":\"An error occurred while processing your request.\"}}\n\n",
        12,
    );
    let error = response_error(&events);
    assert_eq!(error.class, ErrorClass::Transient);
    assert_eq!(error.provider_code, None);
    assert_eq!(
        error.message,
        "An error occurred while processing your request."
    );
}

#[test]
fn top_level_server_error_is_transient() {
    for nested in [json!(null), json!("unparsable")] {
        let wire = format!(
            "data: {}\n\n",
            json!({"type":"error", "error":nested, "code":"server_error", "message":"backend failed"})
        );
        let events = normalize_responses(&wire, 13);
        let error = response_error(&events);
        assert_eq!(error.class, ErrorClass::Transient);
        assert_eq!(error.provider_code.as_deref(), Some("server_error"));
        assert_eq!(error.message, "backend failed");
    }
}

#[test]
fn response_failed_without_error_is_transient_and_descriptive() {
    for response in [
        json!({"error":null}),
        json!({}),
        json!({"error":"unparsable"}),
    ] {
        let wire = format!(
            "data: {}\n\n",
            json!({"type":"response.failed", "response":response, "code":"invalid_prompt"})
        );
        let events = normalize_responses(&wire, 14);
        let error = response_error(&events);
        assert_eq!(error.class, ErrorClass::Transient);
        assert_eq!(error.provider_code, None);
        assert_eq!(
            error.message,
            "OpenAI Responses stream reported an error (event \"response.failed\", no code)"
        );
    }
}

#[test]
fn response_failed_context_length_is_context_overflow() {
    let wire = "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"context_length_exceeded\"}}}\n\n";
    let events = normalize_responses(wire, 15);
    let error = response_error(&events);
    assert_eq!(error.class, ErrorClass::ContextOverflow);
    assert_eq!(
        error.provider_code.as_deref(),
        Some("context_length_exceeded")
    );
}

#[test]
fn response_failed_known_fatal_codes_are_permanent() {
    for (code, expected_code) in [
        ("insufficient_quota", "billing_exhausted"),
        ("credit_balance_exhausted", "credit_balance_exhausted"),
        (
            "organization_spend_limit_exceeded",
            "organization_spend_limit_exceeded",
        ),
        (
            "project_spend_limit_exceeded",
            "project_spend_limit_exceeded",
        ),
        ("usage_not_included", "usage_not_included"),
        ("invalid_prompt", "invalid_prompt"),
        ("cyber_policy", "cyber_policy"),
        ("bio_policy", "bio_policy"),
        (
            "misalignment_policy_violation",
            "misalignment_policy_violation",
        ),
        ("usage_limit_reached", "usage_limit_reached"),
    ] {
        let wire = format!(
            "data: {}\n\n",
            json!({"type":"response.failed", "response":{"error":{"code":code}}})
        );
        let events = normalize_responses(&wire, 16);
        let error = response_error(&events);
        assert_eq!(error.class, ErrorClass::Permanent, "{code}");
        assert_eq!(error.provider_code.as_deref(), Some(expected_code));
    }
}

#[test]
fn unknown_response_error_code_is_transient() {
    let events = normalize_responses(
        "data: {\"type\":\"error\",\"error\":{\"code\":\"some_new_code\"}}\n\n",
        17,
    );
    let error = response_error(&events);
    assert_eq!(error.class, ErrorClass::Transient);
    assert_eq!(error.provider_code.as_deref(), Some("some_new_code"));
    assert_eq!(
        error.message,
        "OpenAI Responses stream reported an error (event \"error\", code \"some_new_code\")"
    );
}

fn normalize_responses_usage(usage: serde_json::Value) -> Usage {
    let recorded = format!(
        "data: {}\n\ndata: {}\n\n",
        json!({"type": "response.created", "response": {"id": "resp_usage", "status": "in_progress"}}),
        json!({"type": "response.completed", "response": {"id": "resp_usage", "status": "completed", "usage": usage}}),
    );
    normalize_responses(&recorded, 1)
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(usage),
            _ => None,
        })
        .expect("step usage")
}

#[test]
fn openai_responses_usage_splits_inclusive_cache_counts_per_object() {
    for (wire, fresh, cached) in [
        (
            json!({"input_tokens": 12, "output_tokens": 1, "input_tokens_details": {"cached_tokens": 3}}),
            9,
            Some(3),
        ),
        (json!({"input_tokens": 12, "output_tokens": 1}), 12, None),
        (
            json!({"input_tokens": 12, "output_tokens": 1, "input_tokens_details": {"cached_tokens": 12}}),
            0,
            Some(12),
        ),
    ] {
        let usage = normalize_responses_usage(wire);
        assert_eq!(usage.input_tokens, Some(fresh));
        assert_eq!(usage.cached_input_tokens, cached);
        assert_eq!(usage.fill_tokens(), Some(12));
    }
}

/// The Responses API reports the reasoning-token subset of output under
/// `output_tokens_details.reasoning_tokens`. The normalizer must surface it as
/// `usage.reasoning_tokens` WITHOUT subtracting from `output_tokens` (the provider's
/// billing total) and WITHOUT touching `fill_tokens` (reasoning is output-side, never
/// request-side input). Absent details (long-tail OpenAI-compatible servers) leaves
/// the default None.
#[test]
fn openai_responses_usage_maps_reasoning_token_split_without_touching_output_or_fill() {
    for (wire, reasoning, output) in [
        // Reasoning subset present: surfaced; output_tokens stays the provider total.
        (
            json!({
                "input_tokens": 12,
                "output_tokens": 40,
                "input_tokens_details": {"cached_tokens": 3},
                "output_tokens_details": {"reasoning_tokens": 17}
            }),
            Some(17u64),
            40u64,
        ),
        // No details object at all (long-tail server): reasoning stays absent.
        (json!({"input_tokens": 12, "output_tokens": 40}), None, 40),
        // Details present but reasoning_tokens absent: reasoning stays absent.
        (
            json!({
                "input_tokens": 12,
                "output_tokens": 40,
                "output_tokens_details": {"text_tokens": 5}
            }),
            None,
            40,
        ),
    ] {
        let usage = normalize_responses_usage(wire);
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

fn assembled_text(events: &[NormalizedEvent]) -> String {
    events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => Some(
                message
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        })
        .expect("response assembles an assistant message")
}

fn step_finish_reason(events: &[NormalizedEvent]) -> FinishReason {
    events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished { finish_reason, .. }) => {
                Some(*finish_reason)
            }
            _ => None,
        })
        .expect("response finishes the step")
}

#[test]
fn refusal_stream_preserves_reply_and_surfaces_content_filter() {
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_refusal\",\"status\":\"in_progress\"}}\n\n",
        "data: {\"type\":\"response.refusal.delta\",\"delta\":\"I can’t \"}\n\n",
        "data: {\"type\":\"response.refusal.delta\",\"delta\":\"help with that.\"}\n\n",
        "data: {\"type\":\"response.refusal.done\",\"refusal\":\"I can’t help with that.\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_refusal\",\"status\":\"completed\"}}\n\n",
    );
    let events = normalize_responses(recorded, 2);

    let display: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Display(broca_protocol::DisplayEvent::TextDelta { delta, .. }) => {
                Some(delta.as_str())
            }
            _ => None,
        })
        .collect();
    let text = assembled_text(&events);
    let finish_reason = step_finish_reason(&events);
    assert_eq!(
        (display, text.as_str(), finish_reason),
        (
            vec!["I can’t ", "help with that."],
            "I can’t help with that.",
            FinishReason::ContentFilter,
        )
    );
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Finished {
            finish_reason: FinishReason::ContentFilter,
            ..
        })
    )));
}

#[test]
fn refusal_done_is_authoritative_over_accumulated_deltas() {
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_refusal\"}}\n\n",
        "data: {\"type\":\"response.refusal.delta\",\"delta\":\"damaged partial\"}\n\n",
        "data: {\"type\":\"response.refusal.done\",\"refusal\":\"authoritative refusal\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_refusal\",\"status\":\"completed\"}}\n\n",
    );
    let events = normalize_responses(recorded, 3);

    assert_eq!(assembled_text(&events), "authoritative refusal");
    assert_eq!(step_finish_reason(&events), FinishReason::ContentFilter);
}

#[test]
fn output_text_done_replaces_incomplete_text_deltas() {
    // OpenAI emits a complete text value at `response.output_text.done`. It is the
    // durable source of truth even if a compatible endpoint sent incomplete deltas.
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_text_done\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"incomplete delta\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"text\":\"authoritative final text\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_text_done\",\"status\":\"completed\"}}\n\n",
    );

    assert_eq!(
        assembled_text(&normalize_responses(recorded, 4)),
        "authoritative final text"
    );
}

#[test]
fn output_text_done_keeps_each_output_item() {
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_two_texts\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"first incomplete\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"text\":\"first final\"}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":1,\"delta\":\"second incomplete\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"output_index\":1,\"text\":\"second final\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_two_texts\",\"status\":\"completed\"}}\n\n",
    );

    assert_eq!(
        assembled_text(&normalize_responses(recorded, 5)),
        "first finalsecond final"
    );
}

#[test]
fn output_text_done_keeps_every_content_part_of_one_item() {
    // ONE MESSAGE ITEM MAY CARRY SEVERAL TEXT PARTS. `content_index` names the part and a
    // `done` event finalises exactly that part, so keying the buffer on output_index alone
    // made the second `done` overwrite the first part. The display lane had already emitted
    // both, so the durable answer disagreed with what the user watched arrive -- silently,
    // on a completed run.
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_parts\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"FIRST\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":0,\"text\":\"FIRST_PART\"}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":1,\"delta\":\"SECOND\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":1,\"text\":\"SECOND_PART\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_parts\"}}\n\n",
    );

    assert_eq!(
        assembled_text(&normalize_responses(recorded, 5)),
        "FIRST_PARTSECOND_PART"
    );
}

#[test]
fn a_complete_message_item_does_not_duplicate_parts_already_streamed() {
    // The complete-item fallback exists for streams that omit granular events, and its text
    // is the WHOLE message. Writing it beside parts that already arrived would double the
    // answer, so it must insert only when the item carries no part at all.
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_dup\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"A\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":0,\"text\":\"AAA\"}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":1,\"delta\":\"B\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":1,\"text\":\"BBB\"}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"AAABBB\"}]}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_dup\"}}\n\n",
    );

    assert_eq!(assembled_text(&normalize_responses(recorded, 6)), "AAABBB");
}

#[test]
fn output_item_done_message_text_is_recovered_when_text_events_are_omitted() {
    // A backend can deliver the assistant's text only inside the completed message
    // item, omitting both `response.output_text.delta` and `response.output_text.done`.
    // The codex backend is observed to skip granular events for tool-call arguments
    // (complete value arrives only at completion); the same omission shape on the text
    // channel must not leave the run with no assistant text. The completed message item
    // already serves as the authoritative fallback for refusal content — the output_text
    // parts in the same content array are recovered the same way.
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_item_text\"}}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"text from the completed item\"}]}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_item_text\",\"status\":\"completed\"}}\n\n",
    );

    assert_eq!(
        assembled_text(&normalize_responses(recorded, 6)),
        "text from the completed item"
    );
}

#[test]
fn output_item_done_message_text_does_not_double_count_granular_events() {
    // When the granular text events DID fire, the completed message item repeats the
    // same text. The fallback must not append a second copy: the durable reply is the
    // text once, not twice.
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_item_text_dup\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"hello world\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"text\":\"hello world\"}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello world\"}]}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_item_text_dup\",\"status\":\"completed\"}}\n\n",
    );

    assert_eq!(
        assembled_text(&normalize_responses(recorded, 7)),
        "hello world"
    );
}
#[test]
fn completed_message_item_backs_up_done_only_refusal() {
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_refusal\"}}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_refusal\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"refusal\",\"refusal\":\"done-only refusal\"}]}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_refusal\",\"status\":\"completed\"}}\n\n",
    );
    let events = normalize_responses(recorded, 4);

    assert_eq!(assembled_text(&events), "done-only refusal");
    assert_eq!(step_finish_reason(&events), FinishReason::ContentFilter);
}

#[test]
fn explicit_incomplete_reason_wins_over_refusal_classification() {
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_refusal\"}}\n\n",
        "data: {\"type\":\"response.refusal.done\",\"refusal\":\"partial refusal\"}\n\n",
        "data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_refusal\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n",
    );
    let events = normalize_responses(recorded, 5);

    assert_eq!(assembled_text(&events), "partial refusal");
    assert_eq!(step_finish_reason(&events), FinishReason::Length);
}

#[test]
fn response_incomplete_assembles_partial_text_with_length_finish() {
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_partial\",\"status\":\"in_progress\"},\"sequence_number\":0}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Partial answer\",\"item_id\":\"msg_partial\",\"output_index\":0,\"content_index\":0,\"sequence_number\":1}\n\n",
        "data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_partial\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"output\":[{\"id\":\"msg_partial\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"incomplete\",\"content\":[{\"type\":\"output_text\",\"text\":\"Partial answer\"}]}],\"usage\":{\"input_tokens\":21,\"output_tokens\":7,\"input_tokens_details\":{\"cached_tokens\":3},\"output_tokens_details\":{\"reasoning_tokens\":0},\"total_tokens\":28}},\"sequence_number\":2}\n\n",
    );
    let frozen = openai_responses_frozen_config("gpt-4.1");
    let mut norm = OpenAiResponses.normalizer(&frozen, StepId(9));
    let mut events = Vec::new();
    for chunk in recorded.as_bytes().chunks(17) {
        events.extend(norm.push(chunk).expect("push"));
    }
    events.extend(norm.finish().expect("clean EOF"));

    let message = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("incomplete response assembles its partial assistant message");
    assert!(message
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::Text { text } if text == "Partial answer")));

    let (step_usage, step_reason) = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished {
                usage,
                finish_reason,
                ..
            }) => Some((*usage, *finish_reason)),
            _ => None,
        })
        .expect("incomplete response finishes the step");
    assert_eq!(step_reason, FinishReason::Length);
    assert_eq!(step_usage.input_tokens, Some(18));
    assert_eq!(step_usage.output_tokens, Some(7));
    assert_eq!(step_usage.cached_input_tokens, Some(3));
    assert_eq!(step_usage.fill_tokens(), Some(21));

    let (finished_usage, finished_reason) = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::Finished {
                usage,
                finish_reason,
                ..
            }) => Some((*usage, *finish_reason)),
            _ => None,
        })
        .expect("incomplete response finishes the provider stream");
    assert_eq!(finished_reason, FinishReason::Length);
    assert_eq!(finished_usage, step_usage);
}

#[test]
fn openai_responses_normalize_recorded_helper_runs_clean() {
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"status\":\"in_progress\"},\"sequence_number\":0}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\",\"item_id\":\"m\",\"output_index\":0,\"content_index\":0,\"sequence_number\":1}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0},\"total_tokens\":2}},\"sequence_number\":2}\n\n",
    );
    let frozen = openai_responses_frozen_config("gpt-4.1");
    let events = normalize_recorded(&OpenAiResponses, &frozen, StepId(1), recorded.as_bytes(), 1)
        .expect("normalize ok");
    assert!(events.iter().any(|e| matches!(
        e,
        NormalizedEvent::Control(ProviderControl::StreamStart { .. })
    )));
}

#[test]
fn reasoning_item_stays_before_its_paired_function_call() {
    let recorded = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_order\"}}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"rs_order\",\"type\":\"reasoning\",\"summary\":[],\"encrypted_content\":\"cipher\"}}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"call_id\":\"call_order\",\"name\":\"lookup\",\"arguments\":\"{}\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_order\",\"status\":\"completed\"}}\n\n",
    );
    let events = normalize_responses(recorded, 42);
    let message = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .unwrap();
    assert!(matches!(
        message.content.as_slice(),
        [
            ContentBlock::RedactedReasoning {
                provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
                ..
            },
            ContentBlock::ToolCall(call),
        ] if call.tool_call_id.0 == "call_order"
    ));
}
