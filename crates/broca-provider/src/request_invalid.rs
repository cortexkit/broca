//! Recognize a provider's refusal of the REQUEST itself (a request-validation 400) and give it the
//! typed [`REQUEST_INVALID`] code, so a router can tell "this request shape is wrong" from "this
//! provider is failing".
//!
//! Without a typed code, such a refusal reached callers as a bare `permanent` error. A caller that
//! walls a provider after a run of consecutive terminal failures then walled a healthy provider
//! because one request shape (a structured-output schema OpenAI rejected with
//! `invalid_json_schema`) kept being refused.

use broca_protocol::error::REQUEST_INVALID;
use broca_protocol::{ErrorClass, ProviderError};
use serde_json::Value;

/// OpenAI's, the ChatGPT backend's and Anthropic's `error.type` for a request they rejected as
/// malformed or unsupported. Anthropic also uses it for a too-long prompt and for an exhausted
/// credit balance; both of those are recognized earlier and keep their own, more specific code.
const INVALID_REQUEST_ERROR: &str = "invalid_request_error";

/// Google's RPC `error.status` for a request with an invalid field (Gemini and Code Assist). The
/// input-token-limit refusal uses it too; that one is classed context overflow earlier.
const GOOGLE_INVALID_ARGUMENT: &str = "INVALID_ARGUMENT";

/// Relabel `error` as [`REQUEST_INVALID`] when it is a request-validation refusal that no more
/// specific code already describes.
///
/// `error_object` is the provider's structured `error` object from the response body (or the
/// Responses websocket error frame). The match requires all of:
///
/// - status 400, the status each of these providers gives a request-validation refusal;
/// - class `permanent`, so a refusal already classed context overflow, auth, or retryable is
///   left alone;
/// - no code broca already treats as meaningful: a code on the closed allowlist
///   ([`crate::safe_provider_code`], which holds `billing_exhausted`, `context_length_exceeded`,
///   `invalid_prompt`, the policy codes and so on) keeps its place. Only an absent code, the
///   generic type itself, or a provider code broca has no meaning for is replaced;
/// - the body's own discriminator: `error.type == "invalid_request_error"` or
///   `error.status == "INVALID_ARGUMENT"`. A 400 without one of those is not known to be about
///   the request, and keeps whatever it had.
///
/// The provider's own short code (`error.code`, such as `invalid_json_schema`) moves to
/// `provider_subcode` when it passes [`safe_provider_subcode`]. The message is never read, so a
/// backend that echoes request content or credentials in its message cannot leak it here.
pub(crate) fn classify_request_invalid(
    error: ProviderError,
    error_object: Option<&Value>,
) -> ProviderError {
    if error.status != Some(400) || error.class != ErrorClass::Permanent {
        return error;
    }
    if error
        .provider_code
        .as_deref()
        .is_some_and(crate::safe_provider_code)
    {
        return error;
    }
    let Some(object) = error_object.filter(|object| object.is_object()) else {
        return error;
    };
    let field = |name: &str| object.get(name).and_then(Value::as_str);
    let is_validation_refusal = field("type") == Some(INVALID_REQUEST_ERROR)
        || field("status") == Some(GOOGLE_INVALID_ARGUMENT);
    if !is_validation_refusal {
        return error;
    }
    let provider_subcode = field("code")
        .filter(|code| *code != INVALID_REQUEST_ERROR)
        .filter(|code| safe_provider_subcode(code))
        .map(str::to_owned);
    ProviderError {
        provider_code: Some(REQUEST_INVALID.to_owned()),
        provider_subcode,
        ..error
    }
}

/// Whether a provider-supplied short code may be kept as `provider_subcode`: 1 to 64 characters,
/// each a lowercase ASCII letter, a digit, or `_`. Anything else is dropped rather than
/// rewritten, because a rewritten value would be a code the provider never sent.
///
/// The shape is the safety property. The value comes from the provider, so it is an open
/// vocabulary, but a string limited to this alphabet and length is an identifier: it cannot hold
/// a sentence of request content, a URL, a JWT (which needs `.`) or a header value.
pub fn safe_provider_subcode(code: &str) -> bool {
    (1..=64).contains(&code.len())
        && code
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn permanent_400(provider_code: Option<&str>) -> ProviderError {
        ProviderError {
            provider_details: None,
            class: ErrorClass::Permanent,
            message: "400: body".into(),
            status: Some(400),
            retry_after_secs: None,
            provider_code: provider_code.map(str::to_owned),
            provider_subcode: None,
            refusal: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        }
    }

    #[test]
    fn subcode_admits_only_short_lowercase_identifiers() {
        for code in ["invalid_json_schema", "a", "x9_", &"a".repeat(64)] {
            assert!(safe_provider_subcode(code), "{code:?}");
        }
        for code in [
            "",
            &"a".repeat(65),
            "Invalid_Json",
            "INVALID_ARGUMENT",
            "invalid-json",
            "invalid json",
            "eyJhbGciOi.eyJzdWIi.sig",
            "sk-proj-abc",
            "code\n",
        ] {
            assert!(!safe_provider_subcode(code), "{code:?}");
        }
    }

    #[test]
    fn only_a_permanent_400_with_the_body_discriminator_is_relabelled() {
        let object = serde_json::json!({"type": "invalid_request_error", "code": "bad_field"});
        let relabelled = classify_request_invalid(permanent_400(Some("bad_field")), Some(&object));
        assert_eq!(relabelled.provider_code.as_deref(), Some(REQUEST_INVALID));
        assert_eq!(relabelled.provider_subcode.as_deref(), Some("bad_field"));

        let mut other_status = permanent_400(Some("bad_field"));
        other_status.status = Some(422);
        assert_eq!(
            classify_request_invalid(other_status.clone(), Some(&object)),
            other_status
        );
        let mut overflow = permanent_400(Some("bad_field"));
        overflow.class = ErrorClass::ContextOverflow;
        assert_eq!(
            classify_request_invalid(overflow.clone(), Some(&object)),
            overflow
        );
        for known in [
            "billing_exhausted",
            "context_length_exceeded",
            "invalid_prompt",
        ] {
            let error = permanent_400(Some(known));
            assert_eq!(
                classify_request_invalid(error.clone(), Some(&object)),
                error
            );
        }
        let untyped = serde_json::json!({"type": "permission_error", "code": "bad_field"});
        let error = permanent_400(Some("bad_field"));
        assert_eq!(
            classify_request_invalid(error.clone(), Some(&untyped)),
            error
        );
        assert_eq!(classify_request_invalid(error.clone(), None), error);
    }
}
