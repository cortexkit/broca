use super::*;

// RECONSTRUCTED, NOT CAPTURED. The shape is the production refusal OpenAI gave a structured-output
// request whose schema it rejects: HTTP 400 with `error.type` `invalid_request_error`, `error.code`
// `invalid_json_schema` and `error.param` `text.format.schema`. Only those three fields were
// recorded; the message text here is written for the test in OpenAI's usual wording.
const OPENAI_INVALID_SCHEMA_BODY: &str = r#"{"error":{"message":"Invalid schema for response_format 'result': In context=(), 'required' is required to be supplied and to be an array including every key in properties. Missing 'reason'.","type":"invalid_request_error","param":"text.format.schema","code":"invalid_json_schema"}}"#;
const ANTHROPIC_INVALID_BODY: &str = r#"{"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0.tool_use.input: Input should be a valid dictionary"}}"#;
const GOOGLE_INVALID_BODY: &str = r#"{"error":{"code":400,"message":"Invalid JSON payload received. Unknown name \"foo\" at 'generation_config': Cannot find field.","status":"INVALID_ARGUMENT"}}"#;
const OPENCODE_GO_MISSING_SESSION_ID_BODY: &str = r#"{"type":"error","error":{"type":"MissingSessionID","message":"Request is missing x-opencode-session and cannot be routed efficiently."}}"#;

/// Classify `body` as each family's live path does (the frozen family supplied) and as a caller
/// with only the body does (no family), and require both to agree.
fn classify_both(status: u16, body: &str, family: WireFamilyId) -> ProviderError {
    let live =
        super::super::classify_http_error_in_family(status, body, None, None, Some(family), None);
    assert_eq!(
        classify_http_error(status, body, None, None),
        live,
        "{family:?}"
    );
    live
}

fn assert_request_invalid(error: &ProviderError, subcode: Option<&str>) {
    assert_eq!(error.class, ErrorClass::Permanent);
    assert_eq!(error.provider_code.as_deref(), Some(REQUEST_INVALID));
    assert!(error.is_request_invalid());
    assert_eq!(error.provider_subcode.as_deref(), subcode);
    assert_eq!(error.status, Some(400));
    assert!(!error.class.step_reissuable());
    assert!(!error.is_credential_scoped());
}

#[test]
fn openai_and_chatgpt_invalid_request_400_is_request_invalid_with_the_provider_code() {
    for family in [
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::OpenAiChat,
    ] {
        let error = classify_both(400, OPENAI_INVALID_SCHEMA_BODY, family);
        assert_request_invalid(&error, Some("invalid_json_schema"));
    }
    // A null code leaves the typed code without a detail.
    let null_code = OPENAI_INVALID_SCHEMA_BODY.replace(r#""invalid_json_schema""#, "null");
    let error = classify_both(400, &null_code, WireFamilyId::OpenAiResponses);
    assert_request_invalid(&error, None);
}

#[test]
fn a_provider_code_outside_the_identifier_shape_is_dropped_not_rewritten() {
    for code in [
        "Invalid-JSON-Schema",
        "invalid json schema",
        "eyJhbGciOi.eyJzdWIi.sig",
    ] {
        let body =
            OPENAI_INVALID_SCHEMA_BODY.replace("invalid_json_schema", code);
        let error = classify_both(400, &body, WireFamilyId::OpenAiResponses);
        assert_request_invalid(&error, None);
    }
}

#[test]
fn anthropic_invalid_request_400_is_request_invalid_without_a_detail() {
    let error = classify_both(400, ANTHROPIC_INVALID_BODY, WireFamilyId::AnthropicMessages);
    assert_request_invalid(&error, None);
}

#[test]
fn gemini_and_codeassist_invalid_argument_400_is_request_invalid() {
    for family in [WireFamilyId::Gemini, WireFamilyId::GoogleCodeAssist] {
        for body in [
            GOOGLE_INVALID_BODY.to_owned(),
            format!("[{GOOGLE_INVALID_BODY}]"),
        ] {
            let error = classify_both(400, &body, family);
            // Google's `code` is the numeric status, not a short provider code.
            assert_request_invalid(&error, None);
        }
    }
}

#[test]
fn gemini_and_codeassist_in_band_invalid_argument_is_request_invalid() {
    for (family, id) in [
        (&Gemini as &dyn WireFamily, WireFamilyId::Gemini),
        (&GoogleCodeAssist as &dyn WireFamily, WireFamilyId::GoogleCodeAssist),
    ] {
        let value: serde_json::Value = serde_json::from_str(GOOGLE_INVALID_BODY).unwrap();
        let value = if id == WireFamilyId::GoogleCodeAssist {
            serde_json::json!({ "response": value })
        } else {
            value
        };
        let frozen = frozen_config_for_family(id, "model");
        let mut normalizer = family.normalizer(&frozen, StepId(1));
        let events = normalizer
            .push(format!("data: {value}\n\n").as_bytes())
            .unwrap();
        assert_request_invalid(terminal_error(&events), None);
    }
}

/// A 400 that an earlier, more specific rule classifies keeps that rule's code: a context
/// overflow in every family's shape, and Anthropic's out-of-credit refusal.
#[test]
fn more_specific_400_codes_are_not_relabelled() {
    let openai_overflow = r#"{"error":{"message":"This model's maximum context length is 128000 tokens.","type":"invalid_request_error","param":"messages","code":"context_length_exceeded"}}"#;
    let anthropic_overflow = r#"{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 1002096 tokens > 1000000 maximum"}}"#;
    let google_overflow = r#"{"error":{"code":400,"message":"The input token count (134123) exceeds the maximum number of tokens allowed (131072).","status":"INVALID_ARGUMENT"}}"#;
    for (family, body, code) in [
        (
            WireFamilyId::OpenAiResponses,
            openai_overflow,
            "context_length_exceeded",
        ),
        (
            WireFamilyId::OpenAiResponsesChatgpt,
            openai_overflow,
            "context_length_exceeded",
        ),
        (
            WireFamilyId::AnthropicMessages,
            anthropic_overflow,
            "context_length_exceeded",
        ),
        (WireFamilyId::Gemini, google_overflow, "INVALID_ARGUMENT"),
        (
            WireFamilyId::GoogleCodeAssist,
            google_overflow,
            "INVALID_ARGUMENT",
        ),
    ] {
        let error = classify_both(400, body, family);
        assert_eq!(error.class, ErrorClass::ContextOverflow, "{family:?}");
        assert_eq!(error.provider_code.as_deref(), Some(code), "{family:?}");
        assert_eq!(error.provider_subcode, None, "{family:?}");
    }
    let credit = r#"{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}"#;
    let error = classify_both(400, credit, WireFamilyId::AnthropicMessages);
    assert_eq!(error.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
    assert_eq!(error.provider_subcode, None);
    // A known policy code on a 400 stays the provider_code, with no subcode.
    let policy = OPENAI_INVALID_SCHEMA_BODY.replace("invalid_json_schema", "invalid_prompt");
    let error = classify_both(400, &policy, WireFamilyId::OpenAiResponses);
    assert_eq!(error.provider_code.as_deref(), Some("invalid_prompt"));
    assert_eq!(error.provider_subcode, None);
}

#[test]
fn opencode_go_missing_session_id_400_is_permanent_with_its_error_class() {
    let error = classify_both(
        400,
        OPENCODE_GO_MISSING_SESSION_ID_BODY,
        WireFamilyId::OpenAiChat,
    );
    assert_eq!(error.class, ErrorClass::Permanent);
    assert_eq!(error.provider_code.as_deref(), Some("MissingSessionID"));
    assert!(!error.class.step_reissuable());
    assert!(!error.is_request_invalid());
}

/// A 400 without the request-validation discriminator, and the discriminator on another status,
/// get no typed code.
#[test]
fn a_400_without_the_discriminator_or_another_status_gets_no_code() {
    for (family, body) in [
        (
            WireFamilyId::OpenAiResponses,
            r#"{"error":{"message":"denied","type":"permission_error","code":"bad_field"}}"#,
        ),
        (
            WireFamilyId::AnthropicMessages,
            r#"{"type":"error","error":{"type":"not_found_error","message":"model: x"}}"#,
        ),
        (
            WireFamilyId::Gemini,
            r#"{"error":{"code":400,"message":"precondition","status":"FAILED_PRECONDITION"}}"#,
        ),
        (
            WireFamilyId::OpenAiChat,
            r#"{"code":"some-client-error","error":"Bad request: messages are malformed"}"#,
        ),
        (WireFamilyId::OpenAiResponses, "Bad Request"),
    ] {
        let error = classify_both(400, body, family);
        assert_eq!(error.class, ErrorClass::Permanent, "{body}");
        assert_ne!(
            error.provider_code.as_deref(),
            Some(REQUEST_INVALID),
            "{body}"
        );
        assert_eq!(error.provider_subcode, None, "{body}");
    }
    for status in [403, 404, 422] {
        let error = classify_both(status, OPENAI_INVALID_SCHEMA_BODY, WireFamilyId::OpenAiResponses);
        assert_ne!(
            error.provider_code.as_deref(),
            Some(REQUEST_INVALID),
            "{status}"
        );
        assert_eq!(error.provider_subcode, None, "{status}");
    }
    // A 5xx with the same `invalid_request_error` body stays retryable and gets no typed code.
    let error = classify_both(500, OPENAI_INVALID_SCHEMA_BODY, WireFamilyId::OpenAiResponses);
    assert_ne!(error.provider_code.as_deref(), Some(REQUEST_INVALID));
}

/// Through the real HTTPS step: the refusal ends the step after one request, with the typed code
/// and the provider's own code as detail, even though the script holds a success for a retry.
#[tokio::test]
async fn an_openai_request_invalid_400_ends_the_step_without_retry() {
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status: 400,
            headers: vec![("content-type", "application/json")],
            body: OPENAI_INVALID_SCHEMA_BODY.as_bytes(),
        },
        ScriptResponse::Http {
            status: 200,
            headers: vec![("content-type", "text/event-stream")],
            body: concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_unused\"}}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_unused\",\"status\":\"completed\"}}\n\n",
            )
            .as_bytes(),
        },
    ])
    .await;
    let mut frozen = openai_responses_frozen_config("gpt-4.1");
    frozen.base_url = Some(server.url.clone());
    let provider = LiveProvider::new(OpenAiResponses, frozen, AuthHandle::None);
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
    assert_request_invalid(terminal_error(&events), Some("invalid_json_schema"));
    assert_eq!(server.request_bodies().len(), 1);
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Error {
            retries_used: 0,
            ..
        })
    )));
}

/// Go's missing-session refusal is terminal even when the next scripted response would succeed.
#[tokio::test]
async fn an_opencode_go_missing_session_id_400_ends_the_step_without_retry() {
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status: 400,
            headers: vec![("content-type", "application/json")],
            body: OPENCODE_GO_MISSING_SESSION_ID_BODY.as_bytes(),
        },
        ScriptResponse::Http {
            status: 200,
            headers: vec![("content-type", "text/event-stream")],
            body: concat!(
                "data: {\"type\":\"chunk\",\"choices\":[{\"delta\":{\"content\":\"unexpected retry\"},\"finish_reason\":null}]}\n\n",
                "data: [DONE]\n\n",
            )
            .as_bytes(),
        },
    ])
    .await;
    let mut frozen = crate::families::openai_chat_frozen_config("minimax-m3.1-flash-free");
    frozen.base_url = Some(server.url.clone());
    frozen.opencode_go_session_id = Some("3b2715c1-0437-47a0-8d08-1dd5bf847ca8".into());
    let provider = LiveProvider::new(crate::families::OpenAiChat, frozen, AuthHandle::None);
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
    let error = terminal_error(&events);
    assert_eq!(error.class, ErrorClass::Permanent);
    assert_eq!(error.provider_code.as_deref(), Some("MissingSessionID"));
    assert_eq!(server.request_bodies().len(), 1);
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Error {
            retries_used: 0,
            ..
        })
    )));
}
