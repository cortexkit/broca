fn regression_decode(
    name: &str,
    bytes: &[u8],
) -> (broca_wal::record::AttemptEnvelope, Vec<NormalizedEvent>) {
    let (id, family) = evidence_family(name);
    let frozen = families::frozen_config_for_family(id, "claude-opus-5-5");
    let mut legacy = family.normalizer(&frozen, StepId(1));
    let expected = legacy.push(bytes).unwrap();
    let mut normalizer = family.normalizer(&frozen, StepId(1));
    let evidence = crate::normalize::SharedResponseEvidence::default();
    normalizer.set_response_evidence(evidence.clone());
    let mut events = normalizer.push(bytes).unwrap();
    assert_eq!(format!("{events:?}"), format!("{expected:?}"));
    let expected_tail = legacy.finish();
    let tail = normalizer.finish();
    assert_eq!(format!("{tail:?}"), format!("{expected_tail:?}"));
    if let Ok(tail) = tail {
        events.extend(tail);
    }
    super::super::observe_response_finish(&Some(evidence.clone()), &events);
    let mut envelope = response_envelope(std::mem::take(&mut *evidence.lock().unwrap()), None);
    envelope.outcome = broca_wal::record::BillingAttemptOutcome::ProviderError;
    (envelope, events)
}

#[test]
fn response_evidence_regression_neutralized_handoff_keeps_provider_order() {
    let (envelope, _) = regression_decode(
        "anthropic_messages",
        include_bytes!("../tests/fixtures/content_filter/l3.sse"),
    );
    let models = envelope.usage_by_model.unwrap();
    assert_eq!(
        models
            .iter()
            .map(|model| model.model_used.as_str())
            .collect::<Vec<_>>(),
        ["claude-opus-5-5", "claude-opus-4-8"]
    );
}

fn synthetic_unresolved_handoff(iterations: Option<Value>) -> Vec<u8> {
    let mut usage = json!({"input_tokens":1,"output_tokens":9});
    if let Some(iterations) = iterations {
        usage["iterations"] = iterations;
    }
    [
        json!({"type":"message_start","message":{"id":"synthetic","model":"claude-opus-5-5","usage":{"input_tokens":1,"output_tokens":0}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"fallback","from":{"model":"claude-opus-5-5"},"to":{"model":"claude-opus-4-8"}}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":usage}),
        json!({"type":"message_stop"}),
    ].into_iter().map(|event| format!("data: {event}\n\n")).collect::<String>().into_bytes()
}

#[test]
fn response_evidence_regression_missing_iterations_do_not_label_handoff_as_source() {
    let (envelope, events) =
        regression_decode("anthropic_messages", &synthetic_unresolved_handoff(None));
    assert!(events.iter().any(|event| matches!(event, NormalizedEvent::Control(ProviderControl::StepFinished { served_model: Some(model), .. }) if model == "claude-opus-4-8")));
    assert_eq!(
        envelope.model_used, None,
        "a handoff is not a single source-model measurement"
    );
    assert_eq!(envelope.usage_raw.as_ref().unwrap()["output_tokens"], 9);
    assert_eq!(envelope.usage, None);
    assert_eq!(
        serde_json::to_string(envelope.usage_by_model.as_ref().unwrap()).unwrap(),
        r#"[{"model_used":"claude-opus-5-5","usage":{}},{"model_used":"claude-opus-4-8","usage":{}}]"#
    );
    serde_json::from_value::<broca_wal::record::AttemptEnvelope>(
        serde_json::to_value(envelope).unwrap(),
    )
    .unwrap();
}

#[test]
fn response_evidence_regression_partly_tagged_iterations_keep_known_facts_without_source_total() {
    let (envelope, _) = regression_decode(
        "anthropic_messages",
        &synthetic_unresolved_handoff(Some(json!([
            {"model":"claude-opus-5-5","input_tokens":4,"output_tokens":2},
            {"input_tokens":8,"output_tokens":6}
        ]))),
    );
    assert_eq!(
        envelope.model_used, None,
        "unknown iterations cannot be charged to the source"
    );
    assert_eq!(
        envelope.usage_raw.as_ref().unwrap()["iterations"][0]["output_tokens"],
        2
    );
    assert_eq!(
        envelope.usage_raw.as_ref().unwrap()["iterations"][1]["output_tokens"],
        6
    );
    assert_eq!(envelope.usage, None);
    let models = envelope.usage_by_model.as_ref().unwrap();
    assert_eq!(models[0].model_used, "claude-opus-5-5");
    assert_eq!(models[0].usage.input_tokens, Some(4));
    assert_eq!(models[0].usage.output_tokens, Some(2));
    assert_eq!(models[1].model_used, "claude-opus-4-8");
    assert_eq!(models[1].usage, Usage::default());
    assert_eq!(
        serde_json::to_string(&models[1]).unwrap(),
        r#"{"model_used":"claude-opus-4-8","usage":{}}"#
    );
}

async fn regression_created_then_disconnect(name: &str) {
    let (id, family) = evidence_family(name);
    let normalizer = family.normalizer(
        &families::frozen_config_for_family(id, "requested"),
        StepId(1),
    );
    let (tx, _rx) = mpsc::channel(256);
    let mut trace = AttemptTrace {
        attempt_hook: Some(Arc::new(TestAttemptHook::new(Default::default()))),
        ..Default::default()
    };
    let stream = futures::stream::iter([
        Ok(b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"synthetic\",\"model\":\"reported\",\"service_tier\":\"reported-tier\"}}\n\n".as_slice()),
        Err(std::io::Error::other("synthetic disconnect")),
    ]);
    assert!(
        drive_body_stream_traced(stream, normalizer, &tx, MAX_STREAM_BYTES, &mut trace)
            .await
            .is_err()
    );
    let envelope = take_evidence_envelope(&mut trace);
    assert_eq!(envelope.model_used.as_deref(), Some("reported"));
    assert_eq!(envelope.service_tier_used.as_deref(), Some("reported-tier"));
    assert_eq!(envelope.usage, None);
    assert_eq!(envelope.finish_reason, None);
}

#[tokio::test]
async fn response_evidence_regression_responses_created_metadata_survives_disconnect() {
    regression_created_then_disconnect("openai_responses").await;
}

#[tokio::test]
async fn response_evidence_regression_chatgpt_created_metadata_survives_disconnect() {
    regression_created_then_disconnect("chatgpt_codex").await;
}

#[tokio::test]
async fn response_evidence_regression_done_without_reported_reason_remains_absent() {
    let (mut trace, events) = normalize_evidence_stream("openai_chat_cached_tokens", b"data: {\"id\":\"synthetic\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n", true).await;
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Finished {
            finish_reason: FinishReason::Other,
            ..
        })
    )));
    assert_eq!(take_evidence_envelope(&mut trace).finish_reason, None);
}

#[tokio::test]
async fn response_evidence_regression_local_tool_cut_has_no_reported_finish_reason() {
    let body = format!(
        "data: {}\n\n",
        json!({"id":"synthetic","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":"f","arguments":format!("{{}}{}", " ".repeat(4097))}}]}}]})
    );
    let (mut trace, events) =
        normalize_evidence_stream("openai_chat_cached_tokens", body.as_bytes(), true).await;
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::ToolArgsTrailingGarbage { .. })
    )));
    assert_eq!(take_evidence_envelope(&mut trace).finish_reason, None);
}

fn regression_inband_error_usage(name: &str, event: Value) {
    let (envelope, _) = regression_decode(name, format!("data: {event}\n\n").as_bytes());
    assert_eq!(
        envelope.usage.and_then(|usage| usage.input_tokens),
        Some(7),
        "{name}"
    );
    assert_eq!(
        envelope.usage_raw.as_ref().unwrap()["future_vendor_key"],
        true
    );
}

#[test]
fn response_evidence_regression_gemini_inband_error_usage_is_retained() {
    regression_inband_error_usage(
        "gemini",
        json!({"error":{"code":400,"status":"INVALID_ARGUMENT","message":"synthetic refusal"},"usageMetadata":{"promptTokenCount":7,"future_vendor_key":true}}),
    );
}

#[test]
fn response_evidence_regression_codeassist_inband_error_usage_is_retained() {
    regression_inband_error_usage(
        "code_assist",
        json!({"response":{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"synthetic refusal"},"usageMetadata":{"promptTokenCount":7,"future_vendor_key":true}}}),
    );
}

#[test]
fn response_evidence_regression_chat_inband_error_usage_is_retained() {
    regression_inband_error_usage(
        "openai_chat_cached_tokens",
        json!({"error":{"type":"invalid_request_error","message":"synthetic refusal"},"usage":{"prompt_tokens":7,"future_vendor_key":true}}),
    );
}

#[test]
fn response_evidence_regression_anthropic_inband_error_usage_is_retained() {
    regression_inband_error_usage(
        "anthropic_messages",
        json!({"type":"error","error":{"type":"invalid_request_error","message":"synthetic refusal"},"usage":{"input_tokens":7,"future_vendor_key":true}}),
    );
}

#[test]
fn response_evidence_regression_socket_error_request_id_is_per_response_not_connection() {
    use super::super::responses_websocket;
    let frame = br#"{"type":"error","status":400,"headers":{"x-request-id":"synthetic-response-id"},"error":{"type":"invalid_request_error","message":"refused"}}"#;
    let mut trace = AttemptTrace {
        attempt_hook: Some(Arc::new(TestAttemptHook::new(Default::default()))),
        ..Default::default()
    };
    assert!(responses_websocket::wrapped_error(frame, &mut trace).is_some());
    assert_eq!(
        trace.vendor_request_id.as_deref(),
        Some("synthetic-response-id")
    );
    let mut legacy = AttemptTrace::default();
    assert!(responses_websocket::wrapped_error(frame, &mut legacy).is_some());
    assert_eq!(legacy.vendor_request_id, None);
    let mut next = AttemptTrace {
        attempt_hook: Some(Arc::new(TestAttemptHook::new(Default::default()))),
        ..Default::default()
    };
    assert!(responses_websocket::wrapped_error(
        br#"{"type":"error","status":400,"error":{"message":"refused"}}"#,
        &mut next
    )
    .is_some());
    assert_eq!(next.vendor_request_id, None);
}
