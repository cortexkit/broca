use super::*;
use crate::spec::WireFamilyId;
use broca_protocol::error::BILLING_EXHAUSTED;
use serde_json::{json, Value};

// Complete provider messages from decoded production refusals, not quoted tool output.
// Anthropic: six captures, 2026-10-16 through 2026-10-27; only request_id varied.
const ANTHROPIC_CREDIT: &str = "Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.";
// Kimi: captured HTTP 403 walls on the Chat and Anthropic wire families. The provider gives
// no absolute reset in these messages, so their window names cannot become reset timestamps.
const KIMI_WEEKLY: &str = "You've reached your weekly (7-day) usage limit. Your quota will reset when the current 7-day window ends. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/membership/subscription?tab=quota";
const KIMI_FIVE_HOUR: &str = "You've reached your 5-hour usage limit. Your quota will reset when the current 5-hour window ends. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/membership/subscription?tab=quota";
const KIMI_BILLING_PRICING: &str = "You've reached your usage limit for this billing cycle. Your quota will be refreshed in the next cycle. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/code/#pricing";
const KIMI_BILLING_MEMBERSHIP: &str = "You've reached your usage limit for this billing cycle. Your quota will be refreshed in the next cycle. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/membership/subscription?tab=quota";
// ChatGPT: the backend's rejected key is masked in the recorded 2026-09-25 capture.
const CHATGPT_KEY: &str = "Incorrect API key provided: sk-svcac***********fvMA. You can find your API key at https://platform.openai.com/account/api-keys.";

fn classify(family: WireFamilyId, status: u16, body: &Value) -> ProviderError {
    let body = body.to_string();
    let error = classify_http_error_in_family(status, &body, None, None, Some(family), None);
    let error = classify_chatgpt_upstream_key_rejection(family, status, &body, error);
    classify_account_limit_exhausted(
        family,
        status,
        &crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>()),
        &body,
        UNIX_EPOCH,
        error,
    )
}

fn kimi_body(family: WireFamilyId, message: &str) -> Value {
    let kind = if family == WireFamilyId::AnthropicMessages {
        "permission_error"
    } else {
        "access_terminated_error"
    };
    json!({"error":{"type":kind,"message":message}})
}

fn assert_kimi_capture(family: WireFamilyId, message: &str) {
    let error = classify(family, 403, &kimi_body(family, message));
    assert_eq!(error.class, ErrorClass::Transient);
    assert_eq!(
        error.provider_code.as_deref(),
        Some(ACCOUNT_LIMIT_EXHAUSTED)
    );
    assert_eq!(error.status, Some(403));
    assert_eq!(error.resets_at_ms, None);
    assert_eq!(error.resets_at_source, None);
    assert_eq!(error.limit_window, None);
}

#[test]
fn captured_anthropic_credit_message_is_billing_exhausted() {
    let body = json!({"type":"error","error":{
        "type":"invalid_request_error","message":ANTHROPIC_CREDIT
    }});
    let error = classify(WireFamilyId::AnthropicMessages, 400, &body);
    assert_eq!(error.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
    assert_eq!(error.class, ErrorClass::Permanent);
    assert_eq!(error.status, Some(400));
}

#[test]
fn anthropic_same_type_and_status_with_other_prose_is_not_a_credit_wall() {
    for message in [
        "credit balance is too low".into(),
        ANTHROPIC_CREDIT.to_uppercase(),
        format!("quoted: {ANTHROPIC_CREDIT}"),
        format!("{ANTHROPIC_CREDIT} extra advice"),
    ] {
        let body = json!({"type":"error","error":{
            "type":"invalid_request_error","message":message
        }});
        let error = classify(WireFamilyId::AnthropicMessages, 400, &body);
        assert!(!error.is_billing_exhausted(), "{message}");
        assert!(!error.is_account_limit_exhausted());
    }
}

#[test]
fn anthropic_credit_message_requires_its_captured_status_type_and_no_code() {
    let body = json!({"type":"error","error":{
        "type":"invalid_request_error","message":ANTHROPIC_CREDIT
    }});
    for status in [401, 403, 429, 500] {
        assert!(!classify(WireFamilyId::AnthropicMessages, status, &body).is_billing_exhausted());
    }
    for family in [WireFamilyId::OpenAiChat, WireFamilyId::OpenAiResponses] {
        assert!(!classify(family, 400, &body).is_billing_exhausted());
    }
    for kind in ["permission_error", "authentication_error"] {
        let mut other = body.clone();
        other["error"]["type"] = json!(kind);
        assert!(!classify(WireFamilyId::AnthropicMessages, 400, &other).is_billing_exhausted());
    }
    let mut other = body;
    other["error"]["code"] = json!("different_code");
    assert!(!classify(WireFamilyId::AnthropicMessages, 400, &other).is_billing_exhausted());
}

#[test]
fn captured_kimi_weekly_membership_message_is_an_account_wall() {
    assert_kimi_capture(WireFamilyId::OpenAiChat, KIMI_WEEKLY);
}

#[test]
fn captured_kimi_five_hour_message_is_an_account_wall_on_both_captured_families() {
    for family in [WireFamilyId::OpenAiChat, WireFamilyId::AnthropicMessages] {
        assert_kimi_capture(family, KIMI_FIVE_HOUR);
    }
}

#[test]
fn captured_kimi_billing_pricing_message_is_an_account_wall() {
    assert_kimi_capture(WireFamilyId::AnthropicMessages, KIMI_BILLING_PRICING);
}

#[test]
fn captured_kimi_billing_membership_message_is_an_account_wall() {
    assert_kimi_capture(WireFamilyId::AnthropicMessages, KIMI_BILLING_MEMBERSHIP);
}

#[test]
fn kimi_same_type_and_status_with_other_prose_is_not_an_account_wall() {
    for family in [WireFamilyId::OpenAiChat, WireFamilyId::AnthropicMessages] {
        for message in [
            "usage limit".into(),
            "The supplied usage limit option is invalid".into(),
            format!("quoted: {KIMI_FIVE_HOUR}"),
            format!("{KIMI_FIVE_HOUR} extra advice"),
            KIMI_FIVE_HOUR.to_uppercase(),
        ] {
            let error = classify(family, 403, &kimi_body(family, &message));
            assert!(!error.is_account_limit_exhausted(), "{family:?}: {message}");
            assert!(!error.is_billing_exhausted());
            assert_eq!(error.resets_at_ms, None);
        }
    }
}

#[test]
fn kimi_messages_require_their_captured_family_status_type_pairs() {
    for family in [WireFamilyId::OpenAiChat, WireFamilyId::AnthropicMessages] {
        let body = kimi_body(family, KIMI_FIVE_HOUR);
        for status in [400, 401, 429, 500] {
            assert!(!classify(family, status, &body).is_account_limit_exhausted());
        }
        let mut other = body.clone();
        other["error"]["type"] = json!("other_type");
        assert!(!classify(family, 403, &other).is_account_limit_exhausted());
        let mut other = body;
        other["error"]["code"] = json!("different_code");
        assert!(!classify(family, 403, &other).is_account_limit_exhausted());
    }
    for message in [KIMI_BILLING_PRICING, KIMI_BILLING_MEMBERSHIP] {
        assert!(!classify(
            WireFamilyId::OpenAiChat,
            403,
            &kimi_body(WireFamilyId::OpenAiChat, message)
        )
        .is_account_limit_exhausted());
    }
    assert!(!classify(
        WireFamilyId::AnthropicMessages,
        403,
        &kimi_body(WireFamilyId::AnthropicMessages, KIMI_WEEKLY)
    )
    .is_account_limit_exhausted());
}

fn meta_body(stamp: &str, seconds: u64) -> Value {
    json!({"error":{
        "type":"rate_limit_error","code":"rate_limit_exceeded","param":null,
        "message":format!("Subscription quota exhausted. Your usage window resets at {stamp}."),
        "resets_at":seconds
    }})
}

fn assert_meta_capture(stamp: &str, seconds: u64, expected_ms: u64) {
    let error = classify(
        WireFamilyId::OpenAiResponses,
        429,
        &meta_body(stamp, seconds),
    );
    assert_eq!(error.class, ErrorClass::Transient);
    assert_eq!(
        error.provider_code.as_deref(),
        Some(ACCOUNT_LIMIT_EXHAUSTED)
    );
    assert_eq!(error.resets_at_ms, Some(expected_ms));
    assert_eq!(error.resets_at_source.as_deref(), Some("body"));
}

// Meta's 64 captured messages differ only by these three UTC reset values.
#[test]
fn captured_meta_october_14_reset_message_is_an_account_wall() {
    assert_meta_capture("2026-10-14T22:55:57Z", 1_792_018_557, 1_792_018_557_000);
}

#[test]
fn captured_meta_october_26_reset_message_is_an_account_wall() {
    assert_meta_capture("2026-10-26T00:00:00Z", 1_792_972_800, 1_792_972_800_000);
}

#[test]
fn captured_meta_november_9_reset_message_is_an_account_wall() {
    assert_meta_capture("2026-11-09T00:00:00Z", 1_794_182_400, 1_794_182_400_000);
}

#[test]
fn meta_same_type_and_status_with_other_prose_is_not_an_account_wall() {
    for message in [
        "Subscription quota exhausted",
        "Request rate limit reached",
        "quoted: Subscription quota exhausted. Your usage window resets at 2026-10-26T00:00:00Z.",
        "Subscription quota exhausted. Your usage window resets at 2026-10-26T00:00:00Z. extra advice",
    ] {
        let mut body = meta_body("2026-10-26T00:00:00Z", 1_792_972_800);
        body["error"]["message"] = json!(message);
        let error = classify(WireFamilyId::OpenAiResponses, 429, &body);
        assert!(!error.is_account_limit_exhausted(), "{message}");
        assert!(error.is_rate_limited());
        assert_eq!(error.resets_at_ms, None);
    }
}

#[test]
fn meta_reset_slot_is_strict_utc_and_must_equal_the_structured_reset() {
    for stamp in [
        "2026-10-26T00:00:00Z ",
        "2026-10-26T00:00:00+00:00",
        "2026-10-26T00:00:00.000Z",
        "2026-10-26t00:00:00z",
        "2026-10-26T00:00:01Z",
        "2026-02-30T00:00:00Z",
        "2026-10-26T24:00:00Z",
        "2026-10-26T00:00:60Z",
        "2026-10-26T00:00:00Z. extra",
    ] {
        let error = classify(
            WireFamilyId::OpenAiResponses,
            429,
            &meta_body(stamp, 1_792_972_800),
        );
        assert!(!error.is_account_limit_exhausted(), "{stamp}");
        assert_eq!(error.resets_at_ms, None);
    }
    for reset in [json!("1792972800"), json!(-1), json!(u64::MAX), Value::Null] {
        let mut body = meta_body("2026-10-26T00:00:00Z", 1_792_972_800);
        body["error"]["resets_at"] = reset;
        assert!(!classify(WireFamilyId::OpenAiResponses, 429, &body).is_account_limit_exhausted());
    }
}

#[test]
fn meta_message_requires_the_captured_status_type_code_and_family() {
    let body = meta_body("2026-10-26T00:00:00Z", 1_792_972_800);
    for status in [400, 401, 403, 500] {
        assert!(
            !classify(WireFamilyId::OpenAiResponses, status, &body).is_account_limit_exhausted()
        );
    }
    for family in [WireFamilyId::OpenAiChat, WireFamilyId::AnthropicMessages] {
        assert!(!classify(family, 429, &body).is_account_limit_exhausted());
    }
    for field in ["type", "code"] {
        let mut other = body.clone();
        other["error"][field] = json!("other_discriminator");
        assert!(!classify(WireFamilyId::OpenAiResponses, 429, &other).is_account_limit_exhausted());
    }
}

fn chatgpt_body(message: &str) -> Value {
    json!({"error":{
        "type":"invalid_request_error","code":"invalid_api_key","param":null,
        "message":message
    }})
}

#[test]
fn captured_chatgpt_masked_key_message_is_an_upstream_rejection() {
    let error = classify(
        WireFamilyId::OpenAiResponsesChatgpt,
        401,
        &chatgpt_body(CHATGPT_KEY),
    );
    assert_eq!(error.class, ErrorClass::Transient);
    assert_eq!(
        error.provider_code.as_deref(),
        Some(CHATGPT_UPSTREAM_KEY_REJECTED)
    );
    assert_eq!(error.status, Some(401));
    assert!(!error.message.contains("sk-"));
    assert!(!error.is_account_limit_exhausted());
}

#[test]
fn chatgpt_same_type_and_status_with_other_prose_is_not_an_upstream_key_rejection() {
    for message in [
        "Incorrect API key provided: sk-123".into(),
        format!("quoted: {CHATGPT_KEY}"),
        format!("{CHATGPT_KEY} extra advice"),
        CHATGPT_KEY.replace("sk-svcac***********fvMA", "sess-token"),
        CHATGPT_KEY.replace("sk-svcac***********fvMA", "sk-key with spaces"),
        "Your authentication token has expired.".into(),
    ] {
        let error = classify(
            WireFamilyId::OpenAiResponsesChatgpt,
            401,
            &chatgpt_body(&message),
        );
        assert_ne!(
            error.provider_code.as_deref(),
            Some(CHATGPT_UPSTREAM_KEY_REJECTED),
            "{message}"
        );
        assert!(!error.is_account_limit_exhausted());
        assert!(!error.is_billing_exhausted());
        assert_eq!(error.class, ErrorClass::Auth);
    }
}

#[test]
fn chatgpt_key_message_requires_the_captured_family_status_type_and_code() {
    let body = chatgpt_body(CHATGPT_KEY);
    for status in [400, 403, 429, 500] {
        assert_ne!(
            classify(WireFamilyId::OpenAiResponsesChatgpt, status, &body)
                .provider_code
                .as_deref(),
            Some(CHATGPT_UPSTREAM_KEY_REJECTED)
        );
    }
    for family in [WireFamilyId::OpenAiChat, WireFamilyId::OpenAiResponses] {
        assert_ne!(
            classify(family, 401, &body).provider_code.as_deref(),
            Some(CHATGPT_UPSTREAM_KEY_REJECTED)
        );
    }
    for field in ["type", "code"] {
        let mut other = body.clone();
        other["error"][field] = json!("other_discriminator");
        assert_ne!(
            classify(WireFamilyId::OpenAiResponsesChatgpt, 401, &other)
                .provider_code
                .as_deref(),
            Some(CHATGPT_UPSTREAM_KEY_REJECTED)
        );
    }
}
