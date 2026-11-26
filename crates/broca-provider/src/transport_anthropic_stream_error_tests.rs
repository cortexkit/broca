use super::*;
use cortexkit_anthropic_policy::ANTHROPIC_CREDIT_BALANCE_MESSAGE;

async fn assert_anthropic_stream_error(message: &str, expected_code: &str) {
    let error = serde_json::json!({
        "type": "error",
        "error": {"type": "invalid_request_error", "message": message},
    });
    let body = format!(
        "{}event: error\ndata: {error}\n\n",
        r#"event: message_start
data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":5,"output_tokens":0}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial answer"}}

"#,
    );
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 200,
        headers: vec![("content-type", "text/event-stream")],
        body: Box::leak(body.into_boxed_str()).as_bytes(),
    }])
    .await;
    let mut frozen = frozen_config_for_family(WireFamilyId::AnthropicMessages, "model");
    frozen.base_url = Some(server.url.clone());
    let provider = LiveProvider::new(AnthropicMessages, frozen, AuthHandle::None);
    let mut rx = provider.start_step(
        StepId(1),
        Arc::new(CallOptions::new(Vec::new())),
        CancellationToken::new(),
        None,
    );
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
    }

    assert!(events
        .iter()
        .take_while(|event| !matches!(event, NormalizedEvent::Control(ProviderControl::Error { .. })))
        .any(|event| matches!(event,
            NormalizedEvent::Display(DisplayEvent::TextDelta { delta, .. }) if delta == "partial answer"
        )));
    let error = terminal_error(&events);
    assert_eq!(error.class, ErrorClass::Permanent);
    assert_eq!(error.provider_code.as_deref(), Some(expected_code));
    assert_eq!(error.message, message);
    assert_eq!(error.status, None);
    assert_eq!(server.request_bodies().len(), 1);
}

#[tokio::test]
async fn anthropic_stream_credit_balance_refusal_is_billing_exhausted() {
    assert_anthropic_stream_error(ANTHROPIC_CREDIT_BALANCE_MESSAGE, BILLING_EXHAUSTED).await;
}

#[tokio::test]
async fn anthropic_stream_credit_balance_near_match_stays_invalid_request_error() {
    assert_anthropic_stream_error(
        &format!("{ANTHROPIC_CREDIT_BALANCE_MESSAGE} "),
        "invalid_request_error",
    )
    .await;
}
