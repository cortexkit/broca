use super::*;

const ANTHROPIC_BODY: &str = r#"{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 1002096 tokens > 1000000 maximum"}}"#;
// Google's API error reference documents 400 INVALID_ARGUMENT for input token limits:
// https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/api-errors
// The wording and Google RPC error envelope were captured in Gemini CLI issue 12214:
// https://github.com/google-gemini/gemini-cli/issues/12214
const GOOGLE_BODY: &str = r#"{"error":{"code":400,"message":"The input token count (134123) exceeds the maximum number of tokens allowed (131072).","errors":[{"message":"The input token count (134123) exceeds the maximum number of tokens allowed (131072).","domain":"global","reason":"badRequest"}],"status":"INVALID_ARGUMENT"}}"#;
// Captured AWS SDK refusal (ValidationException, HTTP 400, this exact message):
// https://github.com/aws-samples/assessment-generator/issues/2
const BEDROCK_BODY: &str =
    r#"{"__type":"ValidationException","message":"Input is too long for requested model."}"#;
const OPENAI_BODY: &str = r#"{"error":{"message":"This model's maximum context length is 128000 tokens. However, your messages resulted in 128001 tokens.","type":"invalid_request_error","param":"messages","code":"context_length_exceeded"}}"#;

fn assert_http_overflow(family: WireFamilyId, body: &str, expected_code: &str) {
    let error = classify_http_error(400, body, None, None);
    assert_eq!(error.class, ErrorClass::ContextOverflow);
    assert_eq!(error.provider_code.as_deref(), Some(expected_code));
    assert_eq!(error.status, Some(400));
    assert!(!error.class.step_reissuable());
    // The live path supplies the frozen family even when no service tier was requested.
    let live =
        super::super::classify_http_error_in_family(400, body, None, None, Some(family), None);
    assert_eq!(live, error);
}

#[test]
fn anthropic_http_prompt_too_long_is_context_overflow() {
    assert_http_overflow(
        WireFamilyId::AnthropicMessages,
        ANTHROPIC_BODY,
        "context_length_exceeded",
    );
}

#[test]
fn anthropic_other_invalid_request_stays_permanent() {
    let body = r#"{"type":"error","error":{"type":"invalid_request_error","message":"messages: roles must alternate between user and assistant"}}"#;
    let error = classify_http_error(400, body, None, None);
    assert_eq!(error.class, ErrorClass::Permanent);
    // Not an overflow, so it is the typed request-validation refusal rather than the raw type.
    assert_eq!(
        error.provider_code.as_deref(),
        Some(broca_protocol::error::REQUEST_INVALID)
    );
}

#[test]
fn gemini_and_codeassist_http_input_token_limit_is_context_overflow() {
    for family in [WireFamilyId::Gemini, WireFamilyId::GoogleCodeAssist] {
        assert_http_overflow(family, GOOGLE_BODY, "INVALID_ARGUMENT");
        assert_http_overflow(family, &format!("[{GOOGLE_BODY}]"), "INVALID_ARGUMENT");
    }
}

#[test]
fn bedrock_http_input_too_long_is_context_overflow() {
    assert_http_overflow(
        WireFamilyId::BedrockConverse,
        BEDROCK_BODY,
        "ValidationException",
    );
    assert_http_overflow(
        WireFamilyId::BedrockConverse,
        r#"{"__type":"com.amazon.bedrock#ValidationException","message":"too many input tokens for requested model"}"#,
        "ValidationException",
    );
}

#[test]
fn openai_http_context_length_codes_hold_for_all_carriers() {
    for family in [
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
    ] {
        assert_http_overflow(family, OPENAI_BODY, "context_length_exceeded");
        assert_http_overflow(
            family,
            &OPENAI_BODY.replace("context_length_exceeded", "context_window_exceeded"),
            "context_window_exceeded",
        );
    }
}

#[test]
fn overflow_message_matches_require_the_family_code_and_status() {
    for (family, body) in [
        (WireFamilyId::AnthropicMessages, ANTHROPIC_BODY),
        (WireFamilyId::Gemini, GOOGLE_BODY),
        (WireFamilyId::GoogleCodeAssist, GOOGLE_BODY),
        (WireFamilyId::BedrockConverse, BEDROCK_BODY),
    ] {
        for status in [401, 403, 413, 429, 500] {
            let error = super::super::classify_http_error_in_family(
                status,
                body,
                None,
                None,
                Some(family),
                None,
            );
            assert_ne!(
                error.class,
                ErrorClass::ContextOverflow,
                "{family:?} HTTP {status}"
            );
        }
        let wrong_code = body
            .replace("invalid_request_error", "permission_error")
            .replace("INVALID_ARGUMENT", "FAILED_PRECONDITION")
            .replace("ValidationException", "AccessDeniedException");
        assert_ne!(
            classify_http_error(400, &wrong_code, None, None).class,
            ErrorClass::ContextOverflow
        );
        let other_message = body
            .replace("prompt is too long:", "invalid field:")
            .replace("The input token count", "The output token count")
            .replace("Input is too long", "Invalid model identifier");
        assert_ne!(
            classify_http_error(400, &other_message, None, None).class,
            ErrorClass::ContextOverflow
        );
        let wrong_family = super::super::classify_http_error_in_family(
            400,
            body,
            None,
            None,
            Some(WireFamilyId::OpenAiChat),
            None,
        );
        assert_eq!(wrong_family.class, ErrorClass::Permanent);
    }
}

fn assert_stream_overflow(
    family: &dyn WireFamily,
    family_id: WireFamilyId,
    body: &[u8],
    code: &str,
) {
    let frozen = frozen_config_for_family(family_id, "model");
    let mut normalizer = family.normalizer(&frozen, StepId(1));
    let events = normalizer.push(body).unwrap();
    let error = terminal_error(&events);
    assert_eq!(error.class, ErrorClass::ContextOverflow);
    assert_eq!(error.provider_code.as_deref(), Some(code));
    assert!(!error.class.step_reissuable());
}

#[test]
fn anthropic_stream_prompt_too_long_is_context_overflow() {
    let body = format!("event: error\ndata: {ANTHROPIC_BODY}\n\n");
    assert_stream_overflow(
        &AnthropicMessages,
        WireFamilyId::AnthropicMessages,
        body.as_bytes(),
        "context_length_exceeded",
    );
}

#[test]
fn gemini_and_codeassist_stream_input_token_limit_is_context_overflow() {
    assert_stream_overflow(
        &Gemini,
        WireFamilyId::Gemini,
        format!("data: {GOOGLE_BODY}\n\n").as_bytes(),
        "INVALID_ARGUMENT",
    );
    for body in [
        GOOGLE_BODY.to_owned(),
        format!(r#"{{"response":{GOOGLE_BODY}}}"#),
    ] {
        assert_stream_overflow(
            &GoogleCodeAssist,
            WireFamilyId::GoogleCodeAssist,
            format!("data: {body}\n\n").as_bytes(),
            "INVALID_ARGUMENT",
        );
    }
}

#[test]
fn bedrock_stream_input_too_long_is_context_overflow() {
    let body = crate::families::bedrock_converse::test_event_stream_message(
        "validationException",
        br#"{"message":"Input is too long for requested model."}"#,
    );
    assert_stream_overflow(
        &BedrockConverse,
        WireFamilyId::BedrockConverse,
        &body,
        "validationException",
    );
}

#[test]
fn openai_chat_and_responses_stream_context_length_codes_hold() {
    assert_stream_overflow(
        &OpenAiChat,
        WireFamilyId::OpenAiChat,
        format!("data: {OPENAI_BODY}\n\n").as_bytes(),
        "context_length_exceeded",
    );
    for (family, id) in [
        (
            &OpenAiResponses as &dyn WireFamily,
            WireFamilyId::OpenAiResponses,
        ),
        (
            &OpenAiResponsesChatgpt as &dyn WireFamily,
            WireFamilyId::OpenAiResponsesChatgpt,
        ),
    ] {
        for code in ["context_length_exceeded", "context_window_exceeded"] {
            for body in [
                format!(
                    r#"{{"type":"error","code":"{code}","message":"Your input exceeds the context window of this model."}}"#
                ),
                format!(
                    r#"{{"type":"error","error":{{"code":"{code}","message":"Your input exceeds the context window of this model."}}}}"#
                ),
                format!(
                    r#"{{"type":"response.failed","response":{{"error":{{"code":"{code}","message":"Your input exceeds the context window of this model."}}}}}}"#
                ),
            ] {
                assert_stream_overflow(family, id, format!("data: {body}\n\n").as_bytes(), code);
            }
        }
    }
}

#[tokio::test]
async fn bedrock_header_validation_refusal_preserves_code_without_retry() {
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 400,
        headers: vec![("x-amzn-errortype", "ValidationException")],
        body: br#"{"message":"Input is too long for requested model."}"#,
    }])
    .await;
    let mut frozen = frozen_config_for_family(WireFamilyId::BedrockConverse, "model");
    frozen.base_url = Some(server.url.clone());
    let provider = LiveProvider::new(BedrockConverse, frozen, AuthHandle::None);
    let mut rx = provider.start_step(
        StepId(1),
        Arc::new(CallOptions::new(Vec::new())),
        CancellationToken::new(),
        None,
    );
    let events = tokio::time::timeout(Duration::from_secs(10), async {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    })
    .await
    .expect("Bedrock overflow must terminate without retry");
    let error = terminal_error(&events);
    assert_eq!(error.class, ErrorClass::ContextOverflow);
    assert_eq!(error.provider_code.as_deref(), Some("ValidationException"));
    assert_eq!(server.request_bodies().len(), 1);
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Error {
            retries_used: 0,
            ..
        })
    )));
}
