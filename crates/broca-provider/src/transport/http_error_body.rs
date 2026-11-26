use serde_json::Value;

use crate::spec::WireFamilyId;

/// Anthropic refuses OAuth by organization policy; a fresh token will not restore access.
pub const OAUTH_NOT_ALLOWED_FOR_ORGANIZATION: &str = "oauth_not_allowed_for_organization";

pub(super) fn normalize_oauth_policy_code(
    status: u16,
    family: Option<WireFamilyId>,
    body: Option<&Value>,
    provider_code: Option<String>,
) -> Option<String> {
    let error = body.and_then(|body| body.get("error"));
    let error_type = error
        .and_then(|error| error.get("type"))
        .and_then(Value::as_str);
    if status == 403
        && family == Some(WireFamilyId::AnthropicMessages)
        && error_type == Some("permission_error")
        && error
            .and_then(|error| error.pointer("/details/error_code"))
            .and_then(Value::as_str)
            == Some(OAUTH_NOT_ALLOWED_FOR_ORGANIZATION)
    {
        return Some(OAUTH_NOT_ALLOWED_FOR_ORGANIZATION.to_owned());
    }
    // A generic code field or header must not bypass the structured credential check.
    if provider_code.as_deref() == Some(OAUTH_NOT_ALLOWED_FOR_ORGANIZATION) {
        return error_type
            .filter(|kind| *kind != OAUTH_NOT_ALLOWED_FOR_ORGANIZATION)
            .map(str::to_owned);
    }
    provider_code
}

pub(super) fn extract_provider_code(text: &str) -> Option<String> {
    let body = parse_http_error_body(text)?;
    let error = body.get("error").filter(|v| v.is_object()).unwrap_or(&body);
    error
        .get("code")
        .and_then(Value::as_str)
        .or_else(|| error.get("type").and_then(Value::as_str))
        .or_else(|| error.get("status").and_then(Value::as_str))
        .or_else(|| body.get("__type").and_then(Value::as_str))
        .map(|code| code.rsplit('#').next().unwrap_or(code))
        .map(str::to_owned)
}

pub(super) fn parse_http_error_body(text: &str) -> Option<Value> {
    let mut body: Value = serde_json::from_str(text).ok()?;
    // Google's streaming endpoint can answer with a one-element array of RPC errors.
    if let Value::Array(errors) = &mut body {
        if errors.len() == 1 {
            return errors.pop();
        }
    }
    Some(body)
}
