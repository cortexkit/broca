use super::super::{
    drive_body_stream_traced, response_envelope, vendor_request_id, MAX_STREAM_BYTES,
};
use super::*;
use crate::{families, policy::UsagePolicy, spec::WireFamilyId, wire::WireFamily};
use serde_json::json;

fn evidence_family(name: &str) -> (WireFamilyId, &'static dyn WireFamily) {
    use families::*;
    match name {
        "anthropic_messages" => (WireFamilyId::AnthropicMessages, &AnthropicMessages),
        "openai_responses" => (WireFamilyId::OpenAiResponses, &OpenAiResponses),
        "chatgpt_codex" => (
            WireFamilyId::OpenAiResponsesChatgpt,
            &OpenAiResponsesChatgpt,
        ),
        "gemini" => (WireFamilyId::Gemini, &Gemini),
        "code_assist" => (WireFamilyId::GoogleCodeAssist, &GoogleCodeAssist),
        "bedrock_converse" => (WireFamilyId::BedrockConverse, &BedrockConverse),
        _ => (WireFamilyId::OpenAiChat, &OpenAiChat),
    }
}

fn synthetic_evidence_stream(name: &str, usage: &Value) -> Vec<u8> {
    let events = match name {
        "anthropic_messages" => vec![
            json!({"type":"message_start","message":{"id":"synthetic","model":"model-reported","usage":usage}}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
            json!({"type":"message_stop"}),
        ],
        "openai_responses" | "chatgpt_codex" => vec![
            json!({"type":"response.created","response":{"id":"synthetic","service_tier":"auto"}}),
            json!({"type":"response.completed","response":{"id":"synthetic","status":"completed","model":"model-reported","service_tier":"provider-tier","usage":usage}}),
        ],
        "gemini" | "code_assist" => {
            let response = json!({"modelVersion":"model-reported","candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":usage});
            vec![if name == "code_assist" {
                json!({"response":response})
            } else {
                response
            }]
        }
        "bedrock_converse" => {
            let mut stream = Vec::new();
            for (kind, body) in [
                ("messageStart", json!({"role":"assistant"})),
                (
                    "contentBlockDelta",
                    json!({"contentBlockIndex":0,"delta":{"text":"ok"}}),
                ),
                ("contentBlockStop", json!({"contentBlockIndex":0})),
                (
                    "metadata",
                    json!({"usage":usage,"serviceTier":{"type":"provider-tier"}}),
                ),
                ("messageStop", json!({"stopReason":"end_turn"})),
            ] {
                stream.extend(families::bedrock_converse::test_event_stream_message(
                    kind,
                    &serde_json::to_vec(&body).unwrap(),
                ));
            }
            return stream;
        }
        _ => vec![
            json!({"id":"synthetic","model":"model-reported","service_tier":"provider-tier","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}),
            json!({"id":"synthetic","choices":[],"usage":usage}),
        ],
    };
    events
        .into_iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>()
        .into_bytes()
}

async fn normalize_evidence_stream(
    name: &str,
    bytes: &[u8],
    journaled: bool,
) -> (AttemptTrace, Vec<NormalizedEvent>) {
    let (id, family) = evidence_family(name);
    let mut frozen = families::frozen_config_for_family(id, "model-requested");
    if name == "openai_chat_reasoning_separate" {
        frozen.usage = UsagePolicy::OpenAiUsageReasoningSeparate;
    }
    let normalizer = family.normalizer(&frozen, StepId(1));
    let (tx, _rx) = mpsc::channel(256);
    let mut trace = AttemptTrace::default();
    if journaled {
        trace.attempt_hook = Some(Arc::new(TestAttemptHook::new(Default::default())));
    }
    let stream = futures::stream::iter(bytes.chunks(7).map(Ok::<_, std::io::Error>));
    let events = drive_body_stream_traced(stream, normalizer, &tx, MAX_STREAM_BYTES, &mut trace)
        .await
        .unwrap();
    (trace, events)
}

fn take_evidence_envelope(trace: &mut AttemptTrace) -> broca_wal::record::AttemptEnvelope {
    let raw = std::mem::take(&mut *trace.response_evidence.as_ref().unwrap().lock().unwrap());
    let mut envelope = response_envelope(raw, trace.vendor_request_id.take());
    envelope.outcome = broca_wal::record::BillingAttemptOutcome::Complete;
    envelope
}

#[tokio::test]
async fn synthetic_response_evidence_family_usage_goldens_preserve_legacy_events() {
    let vectors: Vec<Value> =
        serde_json::from_str(include_str!("../tests/golden/usage_normalization.json")).unwrap();
    assert_eq!(vectors.len(), 12);
    for vector in vectors {
        let name = vector["family"].as_str().unwrap();
        let mut raw = vector["raw_usage"].clone();
        raw["future_vendor_key"] = json!({"nested":[0,"verbatim",false]});
        let stream = synthetic_evidence_stream(name, &raw);
        let (legacy_trace, legacy_events) = normalize_evidence_stream(name, &stream, false).await;
        assert!(legacy_trace.response_evidence.is_none());
        let (mut trace, events) = normalize_evidence_stream(name, &stream, true).await;
        assert_eq!(
            format!("{legacy_events:?}"),
            format!("{events:?}"),
            "{name}"
        );
        let envelope = take_evidence_envelope(&mut trace);
        assert_eq!(
            serde_json::to_value(envelope.usage).unwrap(),
            vector["normalized"],
            "{name}"
        );
        assert_eq!(envelope.usage_raw.as_ref(), Some(&raw), "{name}");
        let tier = match name {
            "anthropic_messages" | "gemini" | "code_assist" => None,
            _ => Some("provider-tier"),
        };
        assert_eq!(envelope.service_tier_used.as_deref(), tier, "{name}");
        assert_eq!(envelope.speed_used, None);
        assert_eq!(envelope.finish_reason, Some(FinishReason::Stop));
        assert_eq!(
            envelope.vendor_request_id, None,
            "missing headers are unknown, not response object ids"
        );
        assert_eq!(
            envelope.model_used.as_deref(),
            (name != "bedrock_converse").then_some("model-reported")
        );
        assert_eq!(
            envelope.reasoning_included_in_output,
            vector["normalized"].get("reasoning_tokens").map(|_| true)
        );
        serde_json::from_value::<broca_wal::record::AttemptEnvelope>(
            serde_json::to_value(&envelope).unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
async fn real_metered_capture_openai_chat_write_and_read_response_evidence() {
    for (body, read, write) in [
        (
            include_bytes!("../tests/fixtures/openai_chat_cache_write_response.sse").as_slice(),
            0,
            3170,
        ),
        (
            include_bytes!("../tests/fixtures/openai_chat_cache_read_response.sse").as_slice(),
            3170,
            0,
        ),
    ] {
        let (mut trace, _) =
            normalize_evidence_stream("openai_chat_cached_tokens", body, true).await;
        let envelope = take_evidence_envelope(&mut trace);
        assert_eq!(
            envelope.usage,
            Some(broca_protocol::Usage {
                input_tokens: Some(3),
                cached_input_tokens: Some(read),
                cache_write_tokens: Some(write),
                output_tokens: Some(4),
                reasoning_tokens: Some(0)
            })
        );
        assert_eq!(envelope.usage_raw.as_ref().unwrap()["total_tokens"], 3177);
        assert_eq!(
            envelope.usage_raw.as_ref().unwrap()["completion_tokens_details"]
                ["rejected_prediction_tokens"],
            0
        );
        assert_eq!(envelope.model_used.as_deref(), Some("gpt-6-luna"));
        assert_eq!(envelope.service_tier_used.as_deref(), Some("default"));
        assert_eq!(envelope.vendor_request_id, None);
        assert_eq!(envelope.reasoning_included_in_output, Some(true));
    }
}

#[test]
fn synthetic_response_evidence_headers_remain_unknown_when_missing_or_invalid() {
    let mut headers = reqwest::header::HeaderMap::new();
    assert_eq!(vendor_request_id(&headers), None);
    headers.insert("request-id", "".parse().unwrap());
    assert_eq!(vendor_request_id(&headers), None);
    headers.insert(
        "request-id",
        reqwest::header::HeaderValue::from_bytes(b"\xff").unwrap(),
    );
    assert_eq!(vendor_request_id(&headers), None);
    for name in [
        "request-id",
        "x-request-id",
        "x-amzn-requestid",
        "x-goog-request-id",
    ] {
        headers.clear();
        headers.insert(name, "vendor-reported-id".parse().unwrap());
        assert_eq!(
            vendor_request_id(&headers).as_deref(),
            Some("vendor-reported-id")
        );
    }
}

#[test]
fn synthetic_response_evidence_retains_metadata_before_stream_completion() {
    let (id, family) = evidence_family("anthropic_messages");
    let mut normalizer = family.normalizer(
        &families::frozen_config_for_family(id, "requested"),
        StepId(1),
    );
    let evidence = crate::normalize::SharedResponseEvidence::default();
    normalizer.set_response_evidence(evidence.clone());
    normalizer
        .push(b"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n")
        .unwrap();
    assert_eq!(
        evidence.lock().unwrap().finish_reason,
        Some(FinishReason::Stop)
    );
    assert_eq!(evidence.lock().unwrap().usage, None);

    let (id, family) = evidence_family("bedrock_converse");
    let mut normalizer = family.normalizer(
        &families::frozen_config_for_family(id, "requested"),
        StepId(1),
    );
    let evidence = crate::normalize::SharedResponseEvidence::default();
    normalizer.set_response_evidence(evidence.clone());
    normalizer
        .push(&families::bedrock_converse::test_event_stream_message(
            "metadata",
            br#"{"serviceTier":{"type":"reported-tier"},"usage":{"outputTokens":0}}"#,
        ))
        .unwrap();
    assert_eq!(
        evidence.lock().unwrap().service_tier_used.as_deref(),
        Some("reported-tier")
    );
    assert_eq!(evidence.lock().unwrap().finish_reason, None);

    let (id, family) = evidence_family("gemini");
    let mut normalizer = family.normalizer(
        &families::frozen_config_for_family(id, "requested"),
        StepId(1),
    );
    let evidence = crate::normalize::SharedResponseEvidence::default();
    normalizer.set_response_evidence(evidence.clone());
    normalizer.observe_response_headers(&crate::wire::ResponseHeaders::new([(
        "x-gemini-service-tier",
        "reported-tier",
    )]));
    assert_eq!(
        evidence.lock().unwrap().service_tier_used.as_deref(),
        Some("reported-tier")
    );
    assert_eq!(evidence.lock().unwrap().usage, None);
}

#[tokio::test]
async fn synthetic_anthropic_response_evidence_merges_ttl_split_and_unknown_keys() {
    let start = json!({"input_tokens":3,"output_tokens":1,"cache_creation_input_tokens":12,"cache_creation":{"ephemeral_5m_input_tokens":5,"ephemeral_1h_input_tokens":7},"service_tier":"future-tier","speed":"future-speed","new_detail":{"a":false}});
    let terminal = json!({"output_tokens":9,"output_tokens_details":{"thinking_tokens":0},"new_detail":{"b":["kept"]}});
    let stream = format!(
        "data: {}\n\ndata: {}\n\ndata: {}\n\n",
        json!({"type":"message_start","message":{"id":"synthetic","model":"actual","usage":start}}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":terminal}),
        json!({"type":"message_stop"})
    );
    let (mut trace, _) =
        normalize_evidence_stream("anthropic_messages", stream.as_bytes(), true).await;
    let envelope = take_evidence_envelope(&mut trace);
    assert_eq!(
        envelope.usage,
        Some(Usage {
            input_tokens: Some(3),
            cache_write_tokens: Some(12),
            output_tokens: Some(9),
            reasoning_tokens: Some(0),
            ..Default::default()
        })
    );
    assert_eq!(envelope.cache_write_5m_tokens, Some(5));
    assert_eq!(envelope.cache_write_1h_tokens, Some(7));
    assert_eq!(envelope.service_tier_used.as_deref(), Some("future-tier"));
    assert_eq!(envelope.speed_used.as_deref(), Some("future-speed"));
    let raw = envelope.usage_raw.as_ref().unwrap();
    assert_eq!(raw["output_tokens"], 9);
    assert_eq!(raw["new_detail"], json!({"a":false,"b":["kept"]}));
    assert_eq!(raw["cache_creation"], start["cache_creation"]);
    assert_eq!(envelope.reasoning_included_in_output, Some(true));
    serde_json::from_value::<broca_wal::record::AttemptEnvelope>(
        serde_json::to_value(envelope).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn synthetic_anthropic_response_evidence_coalesces_models_without_top_level_double_count() {
    let raw = json!({"input_tokens":999,"output_tokens":999,"iterations":[
        {"model":"first","input_tokens":4,"output_tokens":3,"cache_creation_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}},
        {"model":"second","input_tokens":8,"output_tokens":6,"output_tokens_details":{"thinking_tokens":2}},
        {"model":"first","input_tokens":1,"output_tokens":2,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}
    ]});
    let stream = synthetic_evidence_stream("anthropic_messages", &raw);
    let (mut trace, _) = normalize_evidence_stream("anthropic_messages", &stream, true).await;
    let envelope = take_evidence_envelope(&mut trace);
    assert!(envelope.usage.is_none());
    assert!(envelope.model_used.is_none());
    assert!(envelope.cache_write_5m_tokens.is_none());
    assert!(envelope.cache_write_1h_tokens.is_none());
    let models = envelope.usage_by_model.as_ref().unwrap();
    assert_eq!(models.len(), 2);
    assert_eq!(models[0].model_used, "first");
    assert_eq!(models[0].usage.input_tokens, Some(5));
    assert_eq!(models[0].usage.output_tokens, Some(5));
    assert_eq!(models[0].cache_write_5m_tokens, Some(2));
    assert_eq!(models[0].cache_write_1h_tokens, Some(3));
    assert_eq!(models[1].usage.reasoning_tokens, Some(2));
    assert_eq!(envelope.usage_raw.as_ref(), Some(&raw));
    serde_json::from_value::<broca_wal::record::AttemptEnvelope>(
        serde_json::to_value(envelope).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn synthetic_partial_response_evidence_survives_chunk_error_without_completion() {
    let (_, family) = evidence_family("anthropic_messages");
    let normalizer = family.normalizer(
        &families::anthropic_frozen_config("not-evidence"),
        StepId(1),
    );
    let (tx, _rx) = mpsc::channel(256);
    let mut trace = AttemptTrace {
        attempt_hook: Some(Arc::new(TestAttemptHook::new(Default::default()))),
        ..Default::default()
    };
    let stream = futures::stream::iter([
        Ok(b"data: {\"type\":\"message_start\",\"message\":{\"id\":\"synthetic\",\"usage\":{\"input_tokens\":0}}}\n\n".as_slice()),
        Err(std::io::Error::other("synthetic disconnect")),
    ]);
    assert!(
        drive_body_stream_traced(stream, normalizer, &tx, MAX_STREAM_BYTES, &mut trace)
            .await
            .is_err()
    );
    let envelope = take_evidence_envelope(&mut trace);
    assert_eq!(envelope.usage.unwrap().input_tokens, Some(0));
    assert_eq!(envelope.usage.unwrap().output_tokens, None);
    assert_eq!(envelope.finish_reason, None);
    assert_eq!(envelope.model_used, None);
    assert_eq!(envelope.vendor_request_id, None);
    assert_eq!(envelope.cache_write_5m_tokens, None);
}

#[tokio::test]
async fn real_metered_capture_body_with_synthetic_http_headers_reaches_attempt_hook() {
    let body = include_bytes!("../tests/fixtures/openai_chat_cache_read_response.sse");
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 200,
        headers: vec![("x-request-id", "synthetic-request-id")],
        body,
    }])
    .await;
    let mut frozen = families::openai_chat_frozen_config("gpt-6-luna");
    frozen.base_url = Some(server.url.clone());
    let provider = LiveProvider::new(families::OpenAiChat, frozen, AuthHandle::None);
    let hook = Arc::new(TestAttemptHook::new(server.order.clone()));
    let mut stream = broca_core::Provider::step_with_runtime(
        &provider,
        StepId(1),
        Arc::new(CallOptions::new(vec![])),
        broca_core::provider::StepRuntime {
            cancel: CancellationToken::new(),
            deadline: None,
            seal: None,
            progress: None,
            first_attempt: 1,
            attempt_hook: Some(hook.clone()),
        },
    )
    .await;
    while stream.recv().await.is_some() {}
    let responses = hook.responses.lock().unwrap();
    assert_eq!(responses.len(), 1);
    let response = responses[0].as_ref().unwrap();
    assert_eq!(
        response.vendor_request_id.as_deref(),
        Some("synthetic-request-id")
    );
    assert_eq!(response.usage.unwrap().cached_input_tokens, Some(3170));
    assert_eq!(hook.finished.lock().unwrap()[0].1.http_status, Some(200));
    assert_eq!(
        *server.order.lock().unwrap(),
        ["durable", "write", "outcome"]
    );
}

#[tokio::test]
async fn synthetic_http_refusal_keeps_reported_usage_and_missing_header_unknown() {
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 400, headers: vec![],
        body: br#"{"error":{"message":"synthetic refusal","type":"invalid_request_error"},"usage":{"prompt_tokens":0,"completion_tokens":3,"vendor_detail":false}}"#,
    }]).await;
    let mut frozen = families::openai_chat_frozen_config("not-response-evidence");
    frozen.base_url = Some(server.url.clone());
    let provider = LiveProvider::new(families::OpenAiChat, frozen, AuthHandle::None);
    let hook = Arc::new(TestAttemptHook::new(server.order.clone()));
    let mut stream = broca_core::Provider::step_with_runtime(
        &provider,
        StepId(1),
        Arc::new(CallOptions::new(vec![])),
        broca_core::provider::StepRuntime {
            cancel: CancellationToken::new(),
            deadline: None,
            seal: None,
            progress: None,
            first_attempt: 1,
            attempt_hook: Some(hook.clone()),
        },
    )
    .await;
    while stream.recv().await.is_some() {}
    let responses = hook.responses.lock().unwrap();
    assert_eq!(responses.len(), 1);
    let response = responses[0].as_ref().unwrap();
    assert_eq!(response.usage.unwrap().input_tokens, Some(0));
    assert_eq!(response.usage.unwrap().output_tokens, Some(3));
    assert_eq!(response.usage_raw.as_ref().unwrap()["vendor_detail"], false);
    assert_eq!(response.model_used, None);
    assert_eq!(response.vendor_request_id, None);
    assert_eq!(hook.finished.lock().unwrap()[0].1.http_status, Some(400));
}

#[tokio::test]
async fn synthetic_warm_response_evidence_reaches_existing_envelope_without_legacy_changes() {
    use broca_core::WalStore;
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 200, headers: vec![("request-id", "synthetic-warm-id")],
        body: br#"{"model":"reported-model","stop_reason":"end_turn","usage":{"input_tokens":0,"output_tokens":1,"cache_creation_input_tokens":12,"cache_creation":{"ephemeral_5m_input_tokens":5,"ephemeral_1h_input_tokens":7},"service_tier":"future-tier","speed":"future-speed","unknown":false}}"#,
    }]).await;
    let root = attempt_scratch("warm-response-evidence");
    let (writer, store) = warm_writer(&root).await;
    let journal = Arc::new(broca_core::cache_warm::WarmJournal::new(
        writer.clone(),
        broca_core::ShutdownSeal::new(),
        warm_template(),
        "base".into(),
        None,
    ));
    let sent = super::super::send_warm(
        &build_provider_http_client(),
        &Clock::default(),
        warm_request(server.url.clone(), false),
        WireFamilyId::AnthropicMessages,
        None,
        Some(journal),
        None,
    )
    .await
    .unwrap();
    assert_eq!(sent.usage.cache_write_tokens, Some(12));
    Arc::try_unwrap(writer)
        .ok()
        .expect("journal releases writer")
        .shutdown()
        .await
        .unwrap();
    let frames = store.read_records(&attempt_session()).unwrap();
    let envelope = frames
        .iter()
        .find_map(|frame| match &frame.record {
            broca_wal::WalRecord::CacheWarmed { envelope, .. } => envelope.as_ref(),
            _ => None,
        })
        .unwrap();
    assert_eq!(envelope.http_status, Some(200));
    assert_eq!(
        envelope.vendor_request_id.as_deref(),
        Some("synthetic-warm-id")
    );
    assert_eq!(envelope.service_tier_used.as_deref(), Some("future-tier"));
    assert_eq!(envelope.speed_used.as_deref(), Some("future-speed"));
    assert_eq!(envelope.cache_write_5m_tokens, Some(5));
    assert_eq!(envelope.cache_write_1h_tokens, Some(7));
    assert_eq!(envelope.finish_reason, Some(FinishReason::Stop));
    assert_eq!(envelope.usage_raw.as_ref().unwrap()["unknown"], false);
}

#[tokio::test]
async fn synthetic_response_evidence_refresh_witness_is_sticky_but_served_account_is_retained() {
    let server = ScriptedServer::start(vec![
        UNAUTHORIZED,
        ScriptResponse::Http {
            status: 503,
            headers: vec![],
            body: b"busy",
        },
        ScriptResponse::Http {
            status: 200,
            headers: vec![],
            body: b"done",
        },
        ScriptResponse::Http {
            status: 200,
            headers: vec![],
            body: b"done",
        },
    ])
    .await;
    let mut frozen = families::openai_chat_frozen_config("retry-test");
    frozen.base_url = Some(server.url.clone());
    let refresher = Arc::new(FixedRefresher {
        account_ref: Some("quota-account".into()),
        calls: Default::default(),
    });
    let mut provider = LiveProvider::new(
        RetryTestFamily::new(0),
        frozen,
        AuthHandle::Bearer("rejected-test-token".into()),
    )
    .with_account_refs(Some("quota-account".into()), Some("quota-account".into()))
    .with_credential_refresher(refresher);
    use_virtual_retry_waits(&mut provider);
    let hook = Arc::new(TestAttemptHook::new(server.order.clone()));
    for _ in 0..2 {
        let mut stream = broca_core::Provider::step_with_runtime(
            &provider,
            StepId(1),
            Arc::new(CallOptions::new(vec![])),
            broca_core::provider::StepRuntime {
                cancel: CancellationToken::new(),
                deadline: None,
                seal: None,
                progress: None,
                first_attempt: 1,
                attempt_hook: Some(hook.clone()),
            },
        )
        .await;
        while stream.recv().await.is_some() {}
    }
    let sends = hook.sends.lock().unwrap();
    assert_eq!(
        sends
            .iter()
            .map(|(_, send)| send.auth_refreshed)
            .collect::<Vec<_>>(),
        [false, true, true, true]
    );
    assert!(sends
        .iter()
        .all(|(_, send)| send.account_ref.as_deref() == Some("quota-account")));
    let mut policy = broca_core::cache_warm::WarmSendPolicy::default();
    let _ = provider.prepare_warm_request(b"{}", Some(&mut policy));
    assert!(policy.auth_refreshed);
    assert_eq!(
        provider.current_auth().account_ref.as_deref(),
        Some("quota-account")
    );
}
