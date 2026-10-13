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
    /// billing refusal, an exhausted account window, or a rate limit. THE ONE PLACE this set is
    /// defined. Context overflow, policy refusals, bad requests, deadlines and every other error
    /// say nothing about which key to stop using, and never carry `credential_id`.
    pub fn is_credential_scoped(&self) -> bool {
        self.is_billing_exhausted() || self.is_account_limit_exhausted() || self.is_rate_limited()
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
