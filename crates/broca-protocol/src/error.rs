//! Canonical error taxonomy — the provider-module → runner wire contract,
//! surfaced on the control lane as a terminal typed error.
//!
//! Four provider-response classes plus `ProviderUnavailable`, used when the model
//! transport or provider process becomes unavailable before a step completes.

use serde::{Deserialize, Serialize};

/// Consumer-decision error classes, per the cross-producer error-class contract
/// (docs/error-class-contract.md): the class is PRODUCED where the domain knowledge
/// lives; consumers branch on the class tag and structured sibling fields only, never
/// on status codes or message substrings. The shared wire vocabulary is the pinned
/// string set {transient, permanent, auth_required, context_overflow} — see the
/// conformance test pinning these spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    /// Retryable, but the provider module's retry budget is exhausted.
    Transient,
    /// Bad-request / not-found / content-filter / billing exhaustion — retrying this
    /// target with this input is futile until out-of-band action.
    Permanent,
    /// 401 after refresh failed → a credential must be re-established by a human.
    /// Wire spelling is `auth_required` (contract vocabulary); `auth` is accepted on
    /// read indefinitely so WAL records written before the rename replay clean.
    #[serde(rename = "auth_required", alias = "auth")]
    Auth,
    /// The prompt exceeded the model's context window. Remedy is a bounded
    /// reduce-and-retry via the store seam, NOT a loop-owned compaction.
    ContextOverflow,
    /// The provider transport became unavailable before the step completed. The transport
    /// resets the display and re-issues the same rendered request within the step's bounded retry
    /// budget (attempt count and wall time); exhaustion surfaces the last failure as the terminal step error. Re-issuing is
    /// safe because the model leg has no WAL record.
    ///
    /// NOT part of the shared contract vocabulary: its generic consumer decision is
    /// Transient's, so it maps to `transient` at shared boundaries (see
    /// [`ErrorClass::shared_class`]); bounded retry mechanics are transport detail.
    ProviderUnavailable,
}

impl ErrorClass {
    /// The shared-contract class for cross-producer consumers. Identity for the four
    /// contract arms; `ProviderUnavailable` collapses to `Transient` because a bounded
    /// transport resend may succeed and those mechanics are not a consumer concern.
    ///
    /// EXHAUSTIVE ON PURPOSE — do not add a catch-all arm. Every variant is listed so that
    /// adding a new producer-local class is a COMPILE ERROR here rather than a silent
    /// passthrough of a class the ratified vocabulary does not contain. A catch-all would make
    /// the omission invisible at exactly the boundary whose job is to prevent it: the new class
    /// would serialize to callers, decode cleanly (consumers read this field as an OPEN string
    /// by contract), and surface as an unrecognised word in someone else's UI or log.
    ///
    /// If the compiler sends you here after adding a variant: decide whether it is
    /// producer-local (map it to one of the four) or a genuine vocabulary extension (a contract
    /// amendment requiring the ratifying parties, per docs/error-class-contract.md). Do not
    /// resolve it by widening the match.
    pub fn shared_class(self) -> ErrorClass {
        match self {
            ErrorClass::ProviderUnavailable => ErrorClass::Transient,
            ErrorClass::Transient => ErrorClass::Transient,
            ErrorClass::Permanent => ErrorClass::Permanent,
            ErrorClass::Auth => ErrorClass::Auth,
            ErrorClass::ContextOverflow => ErrorClass::ContextOverflow,
        }
    }
}

impl ErrorClass {
    /// Whether the transport may re-issue the model step for this class (the model
    /// leg writes no WAL record, so re-running it is safe).
    pub fn step_reissuable(self) -> bool {
        matches!(
            self,
            ErrorClass::ProviderUnavailable | ErrorClass::Transient
        )
    }
}

/// Diagnostic provenance for a run ended by a provider's refusal, without enlarging common errors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderRefusal {
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_detail_code: Option<String>,
}

/// Bounded, content-free fields from Google's `google.rpc.Status.details` quota response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleRpcErrorDetails {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_info: Option<GoogleRpcErrorInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_failure: Option<GoogleRpcQuotaFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_info: Option<GoogleRpcRetryInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleRpcErrorInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleRpcQuotaFailure {
    pub violations: Vec<GoogleRpcQuotaViolation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleRpcQuotaViolation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_metric: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_id: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub quota_dimensions: std::collections::BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub future_quota_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleRpcRetryInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_delay: Option<String>,
}

// No `Eq`: `retry_after_secs` is an `f64`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderError {
    pub class: ErrorClass,
    pub message: String,
    /// HTTP status when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    /// `Retry-After` in seconds when the provider supplied one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_secs: Option<f64>,
    /// Provider-specific error code, for telemetry/observability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_code: Option<String>,
    /// The provider's own short code for an error broca has already given a typed
    /// `provider_code`, kept as a finer-grained diagnostic beneath it. Today it is set only on a
    /// [`REQUEST_INVALID`] error, where it carries the provider's reason for refusing the request
    /// (OpenAI's `invalid_json_schema`, for example), so a caller can see WHICH request field was
    /// wrong without broca keeping the provider's message, which may echo request content or
    /// credentials.
    ///
    /// It is an open vocabulary the provider defines, so a consumer must never route on it:
    /// routing reads `class` and `provider_code`. The producer only admits a value matching
    /// `[a-z0-9_]{1,64}` (anything else is dropped, never rewritten), so it is always a short
    /// identifier and never message text. `provider_subcode` is absent on every other error, so
    /// their serialized bytes are unchanged by this field's existence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_subcode: Option<String>,
    /// Refusal diagnostics are flattened alongside the role code in provider_code.
    #[serde(default, flatten, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<Box<ProviderRefusal>>,
    /// When the provider account's usage window refills, as an absolute UTC epoch in
    /// milliseconds. Set only on an [`ACCOUNT_LIMIT_EXHAUSTED`] error; absent on every other
    /// error, so their serialized bytes are unchanged by this field's existence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at_ms: Option<u64>,
    /// Origin of the account-limit reset time: `header` for an absolute response-header
    /// timestamp, `body` for an absolute response-body timestamp, or `retry_after` for a
    /// deadline derived from a relative wait hint. Absent without a reset or on older records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at_source: Option<String>,
    /// The provider's name for the exhausted usage window (Anthropic sends e.g. `seven_day` or
    /// `five_hour`), when the provider named one. Set only on an [`ACCOUNT_LIMIT_EXHAUSTED`]
    /// error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_window: Option<String>,
    /// The provider account whose limit is exhausted: the run's frozen
    /// `auth_selection.account_ref`. Set only on an [`ACCOUNT_LIMIT_EXHAUSTED`] error, and only
    /// when the run's selection named an account; the provider never reports it, the run
    /// stamps it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_ref: Option<String>,
    /// The full credential id the run froze in its `auth_selection` (for example
    /// `apikey:anthropic:work` or `oauth:anthropic:acct-c`), so a router can wall exactly the
    /// key or login that hit the limit rather than the whole provider. Unlike `account_ref` it is
    /// present for every auth method, api keys included. Set by the run, never by the provider,
    /// and only on an error [`ProviderError::is_credential_scoped`] accepts; absent when the run
    /// froze no credential selection. Content-free: it names the credential, never its secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<String>,
    /// The largest `max_tokens` the credential's remaining credit could pay for when the provider
    /// refused the request as unaffordable. Set only on a [`CREDIT_INSUFFICIENT`] error, and only
    /// when the provider stated the number. Named after the request's `max_tokens` because that
    /// is the field it bounds: OpenRouter prices a request by its maximum output, and its refusal
    /// reads "requires more credits, or fewer max_tokens ... can only afford N". A caller can
    /// re-send with `max_tokens` at most this value, or route elsewhere; it is not a count of
    /// prompt tokens or of tokens left on the balance. A `u32` rather than a `u64`: it bounds
    /// one request's output, and the eight bytes a `u64` adds to every error would push the enums that
    /// hold a `ProviderError` inline past clippy's size limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affordable_max_tokens: Option<u32>,
    /// Google quota detail fields, copied from typed RPC details before the provider message is
    /// truncated. WAL records carrying this field require `google-rpc-error-details/v1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_details: Option<Box<GoogleRpcErrorDetails>>,
}

/// The `provider_code` of a rate-limit refusal that means ONE provider account has used up a
/// usage window (for Anthropic subscriptions, a 429 whose `anthropic-ratelimit-unified-status`
/// header reads `rejected`), as opposed to the route being momentarily busy.
///
/// Class stays [`ErrorClass::Transient`]: the account works again after `resets_at_ms`. But the
/// transport never re-sends it within a step, because the wait is hours or days and retrying
/// sooner cannot succeed. A router should wall the ACCOUNT (`account_ref`) until
/// `resets_at_ms`, not the model route, since other accounts on the same route still have room.
pub const ACCOUNT_LIMIT_EXHAUSTED: &str = "account_limit_exhausted";

/// The `provider_code` of a credit or billing exhaustion refusal (such as xAI's
/// `personal-team-blocked:spending-limit` or OpenAI's `insufficient_quota`).
/// Class stays [`ErrorClass::Permanent`]; it carries no `resets_at_ms` and is never retried within
/// the run.
pub const BILLING_EXHAUSTED: &str = "billing_exhausted";

/// The `provider_code` of a provider's HTTP 400 that rejected the REQUEST as invalid (a request
/// validation refusal: OpenAI's and Anthropic's `invalid_request_error`, Google's
/// `INVALID_ARGUMENT`), when no more specific code applies. Class stays
/// [`ErrorClass::Permanent`] and the transport never re-sends it: the same bytes are refused the
/// same way.
///
/// It says the request shape was wrong, not that the provider is failing. A router that counts
/// consecutive failures against a provider's health should not count this one: the provider
/// answered correctly. The provider's own reason, when it gave a short code, rides in
/// [`ProviderError::provider_subcode`].
pub const REQUEST_INVALID: &str = "request_invalid";

/// The `provider_code` of a refusal because THIS request costs more than the credential's
/// remaining credit can pay for now (OpenRouter's HTTP 402 whose `error.metadata.limit_source` is
/// `openrouter_credits`). Distinct from [`BILLING_EXHAUSTED`], which says the balance or spending
/// allowance is gone: here some credit remains, and a smaller request (see
/// [`ProviderError::affordable_max_tokens`]) or a top-up can succeed. Class stays
/// [`ErrorClass::Permanent`]: re-sending the same request within the run cannot succeed. It
/// carries no `resets_at_ms`.
pub const CREDIT_INSUFFICIENT: &str = "credit_insufficient";

/// The `provider_code` a run gives its terminal error when it ended [`ErrorClass::Transient`]
/// because the provider rate-limited the request after the transport's in-step retries: an HTTP
/// 429, or a rate-limit error the provider sent without one (Anthropic's in-band SSE
/// `rate_limit_error`). It replaces the provider's own echoed code (`rate_limit_error`,
/// `rate_limit_exceeded`, Google's `RESOURCE_EXHAUSTED`, ...) so a consumer branches on one
/// word. A 429 already relabelled [`ACCOUNT_LIMIT_EXHAUSTED`] keeps that code.
pub const RATE_LIMITED: &str = "rate_limited";

/// Raw provider codes that mean "rate limited" even when the error arrived without an HTTP
/// status (a mid-stream error event).
const RAW_RATE_LIMIT_CODES: &[&str] = &["rate_limit_error", "rate_limit_exceeded", RATE_LIMITED];

impl ProviderError {
    /// Whether this is an [`ACCOUNT_LIMIT_EXHAUSTED`] refusal.
    pub fn is_account_limit_exhausted(&self) -> bool {
        self.provider_code.as_deref() == Some(ACCOUNT_LIMIT_EXHAUSTED)
    }

    /// Whether this is a [`BILLING_EXHAUSTED`] refusal.
    pub fn is_billing_exhausted(&self) -> bool {
        self.provider_code.as_deref() == Some(BILLING_EXHAUSTED)
    }

    /// Whether this is a [`REQUEST_INVALID`] refusal.
    pub fn is_request_invalid(&self) -> bool {
        self.provider_code.as_deref() == Some(REQUEST_INVALID)
    }

    /// Whether this is a [`CREDIT_INSUFFICIENT`] refusal.
    pub fn is_credit_insufficient(&self) -> bool {
        self.provider_code.as_deref() == Some(CREDIT_INSUFFICIENT)
    }

    /// Whether this is a transient rate limit on the request (an HTTP 429, or a raw rate-limit
    /// code with no status) that is not an exhausted account window. These are the errors a
    /// terminal relabels [`RATE_LIMITED`]. Server errors, overloads, stream cuts and timeouts
    /// are not rate limits and never match.
    pub fn is_rate_limited(&self) -> bool {
        self.class == ErrorClass::Transient
            && !self.is_account_limit_exhausted()
            && (self.status == Some(429)
                || self
                    .provider_code
                    .as_deref()
                    .is_some_and(|code| RAW_RATE_LIMIT_CODES.contains(&code)))
    }

    /// Whether this error is about the credential that made the request rather than about the
    /// request or the provider, so a terminal names that credential in `credential_id`: a
    /// billing refusal, an unaffordable request, an exhausted account window, or a rate limit.
    /// THE ONE PLACE this set is
    /// defined. Context overflow, policy refusals, bad requests, deadlines and every other error
    /// say nothing about which key to stop using, and never carry `credential_id`.
    pub fn is_credential_scoped(&self) -> bool {
        self.is_billing_exhausted()
            || self.is_credit_insufficient()
            || self.is_account_limit_exhausted()
            || self.is_rate_limited()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_provenance_error_bytes_remain_unchanged() {
        for bytes in [
            r#"{"class":"auth_required","message":"unauthorized","status":401}"#,
            r#"{"class":"transient","message":"account limit","status":429,"retry_after_secs":60.0,"provider_code":"account_limit_exhausted","resets_at_ms":123000,"limit_window":"seven_day","account_ref":"account-a"}"#,
        ] {
            let error: ProviderError = serde_json::from_str(bytes).unwrap();
            assert_eq!(serde_json::to_string(&error).unwrap(), bytes);
        }
    }

    /// The exact bytes of a terminal that names its credential: `credential_id` serializes last,
    /// after `account_ref`, and an api-key error carries it with no `account_ref` at all.
    #[test]
    fn credential_id_bytes_are_pinned() {
        for bytes in [
            r#"{"class":"permanent","message":"400: credit","status":400,"provider_code":"billing_exhausted","credential_id":"apikey:anthropic:work"}"#,
            r#"{"class":"transient","message":"429: limit","status":429,"provider_code":"account_limit_exhausted","account_ref":"acct-a","credential_id":"oauth:anthropic:work"}"#,
            r#"{"class":"transient","message":"429: slow down","status":429,"retry_after_secs":30.0,"provider_code":"rate_limited","credential_id":"apikey:anthropic"}"#,
            r#"{"class":"permanent","message":"402: credits","status":402,"provider_code":"credit_insufficient","credential_id":"apikey:openrouter","affordable_max_tokens":97335}"#,
        ] {
            let error: ProviderError = serde_json::from_str(bytes).unwrap();
            assert!(error.credential_id.is_some());
            assert!(error.is_credential_scoped());
            assert_eq!(serde_json::to_string(&error).unwrap(), bytes);
        }
    }

    /// The cross-producer error-class contract pins the shared wire vocabulary
    /// (docs/error-class-contract.md). Every producer in the fleet asserts this same
    /// string set; a spelling drift here breaks consumers that branch on the tag.
    #[test]
    fn shared_class_wire_strings_match_the_pinned_contract_set() {
        let spell = |class: ErrorClass| serde_json::to_value(class).unwrap();
        assert_eq!(spell(ErrorClass::Transient), "transient");
        assert_eq!(spell(ErrorClass::Permanent), "permanent");
        assert_eq!(spell(ErrorClass::Auth), "auth_required");
        assert_eq!(spell(ErrorClass::ContextOverflow), "context_overflow");
    }

    /// WAL records written before the auth→auth_required rename carry "auth"; the
    /// read-side alias must accept both spellings indefinitely so old sessions replay.
    #[test]
    fn auth_read_alias_accepts_both_spellings() {
        for spelling in ["\"auth\"", "\"auth_required\""] {
            let parsed: ErrorClass = serde_json::from_str(spelling).unwrap();
            assert_eq!(parsed, ErrorClass::Auth, "spelling {spelling} maps to Auth");
        }
    }

    /// ProviderUnavailable is runner-internal, not contract vocabulary: at shared
    /// boundaries it collapses to Transient; the four contract arms map to themselves.
    #[test]
    fn shared_class_collapses_only_provider_unavailable() {
        assert_eq!(
            ErrorClass::ProviderUnavailable.shared_class(),
            ErrorClass::Transient
        );
        for class in [
            ErrorClass::Transient,
            ErrorClass::Permanent,
            ErrorClass::Auth,
            ErrorClass::ContextOverflow,
        ] {
            assert_eq!(class.shared_class(), class);
        }
    }
}

#[cfg(test)]
mod refusal_bytes_tests {
    use super::*;
    #[test]
    fn ordinary_error_bytes_and_flattened_refusal_members_are_pinned() {
        let ordinary = r#"{"class":"permanent","message":"cannot compact","provider_code":"window_too_small"}"#;
        let mut error: ProviderError = serde_json::from_str(ordinary).unwrap();
        assert!(error.refusal.is_none());
        assert_eq!(serde_json::to_string(&error).unwrap(), ordinary);
        error.refusal = Some(Box::new(ProviderRefusal {
            provider: "magic-context".into(),
            reason: Some("cannot compact".into()),
            provider_detail_code: Some("mc:smaller".into()),
        }));
        let refused = r#"{"class":"permanent","message":"cannot compact","provider_code":"window_too_small","provider":"magic-context","reason":"cannot compact","provider_detail_code":"mc:smaller"}"#;
        assert_eq!(serde_json::to_string(&error).unwrap(), refused);
        assert_eq!(
            serde_json::from_str::<ProviderError>(refused).unwrap(),
            error
        );
        error.refusal.as_mut().unwrap().provider_detail_code = None;
        assert!(!serde_json::to_string(&error)
            .unwrap()
            .contains("provider_detail_code"));
    }
}
