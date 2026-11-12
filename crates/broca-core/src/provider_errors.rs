//! Provider refusals shared by run loops and their terminal reports.

use crate::provider::Provider;
use broca_protocol::{ErrorClass, ProviderError};

/// Why `provider` can't resume a run frozen with `renderer_version` and `frozen_render_config`
/// byte for byte, if it can't: either config is null, the provider doesn't reproduce that
/// renderer version, or it renders that version with another config. `None` when it can.
pub fn render_identity_error<P: Provider + ?Sized>(
    provider: &P,
    renderer_version: u32,
    frozen_render_config: &serde_json::Value,
) -> Option<ProviderError> {
    let current = provider.render_config_blob();
    let (message, provider_code) = if current.is_null() || frozen_render_config.is_null() {
        (
            "resume refused: the current provider or the recorded WAL has a null frozen render \
             config, so render identity cannot be verified; old WALs with a null frozen config \
             were never resume-safe and must be discarded",
            "frozen_render_config_unverifiable",
        )
    } else if !provider.reproduces_renderer_version(renderer_version) {
        (
            "resume refused: the provider renderer version differs from the version recorded at \
             run start, and the provider does not reproduce the recorded version's bytes",
            "renderer_version_drift",
        )
    } else if provider.render_config_blob_for_version(renderer_version) != *frozen_render_config {
        (
            "resume refused: the provider frozen render config differs from the config recorded \
             at run start",
            "frozen_render_config_drift",
        )
    } else {
        return None;
    };

    Some(ProviderError {
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Permanent,
        message: message.into(),
        status: None,
        retry_after_secs: None,
        provider_code: Some(provider_code.into()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    })
}

/// Name the credential behind a credential-scoped terminal, so every surface that reports the
/// terminal error (the durable `RunFinished`, the control lane's `Error` unit, `run.status`,
/// `session.read`) says WHICH key or account to stop using:
///
/// - an account-limit or billing refusal gets the run's provider account in `account_ref`
///   (`None` when the run recorded none, falling back to provider-wide treatment);
/// - every error [`ProviderError::is_credential_scoped`] accepts gets the run's frozen
///   credential id in `credential_id` (`None` when the run froze no selection);
/// - a rate limit gets the typed [`broca_protocol::error::RATE_LIMITED`] code in place of the
///   provider's own echo. This runs once, at the terminal, after the transport's in-step retries,
///   so it never changes a retry decision.
///
/// Every other error is returned unchanged.
pub fn stamp_credential(
    mut error: ProviderError,
    account_ref: Option<&str>,
    credential_id: Option<&str>,
) -> ProviderError {
    if error.is_account_limit_exhausted() || error.is_billing_exhausted() {
        error.account_ref = account_ref.map(str::to_owned);
    }
    if error.is_credential_scoped() {
        error.credential_id = credential_id.map(str::to_owned);
    }
    if error.is_rate_limited() {
        error.provider_code = Some(broca_protocol::error::RATE_LIMITED.into());
    }
    error
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `stamp_credential` sets `credential_id` only on credential-scoped terminals (a billing
    /// refusal, an exhausted account window, a rate limit) and on no other error. It rewrites
    /// only rate limits to the `rate_limited` code, and sets `account_ref` only on the billing
    /// and account-limit refusals.
    #[test]
    fn stamp_credential_names_the_credential_only_on_credential_scoped_errors() {
        let error = |class, status: Option<u16>, code: Option<&str>| ProviderError {
            provider_details: None,
            cause: None,
            provider_subcode: None,
            refusal: None,
            class,
            message: "m".into(),
            status,
            retry_after_secs: None,
            provider_code: code.map(str::to_owned),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        use broca_protocol::error::{ACCOUNT_LIMIT_EXHAUSTED, BILLING_EXHAUSTED, RATE_LIMITED};
        use ErrorClass::*;
        // (case, error, expected provider_code after stamping, expected account_ref stamped)
        let scoped = [
            (
                "billing",
                error(Permanent, Some(403), Some(BILLING_EXHAUSTED)),
                Some(BILLING_EXHAUSTED),
                true,
            ),
            (
                "billing on 400",
                error(Permanent, Some(400), Some(BILLING_EXHAUSTED)),
                Some(BILLING_EXHAUSTED),
                true,
            ),
            (
                "account limit",
                error(Transient, Some(429), Some(ACCOUNT_LIMIT_EXHAUSTED)),
                Some(ACCOUNT_LIMIT_EXHAUSTED),
                true,
            ),
            (
                "anthropic 429",
                error(Transient, Some(429), Some("rate_limit_error")),
                Some(RATE_LIMITED),
                false,
            ),
            (
                "openai 429",
                error(Transient, Some(429), Some("rate_limit_exceeded")),
                Some(RATE_LIMITED),
                false,
            ),
            (
                "google 429",
                error(Transient, Some(429), Some("RESOURCE_EXHAUSTED")),
                Some(RATE_LIMITED),
                false,
            ),
            (
                "bare 429",
                error(Transient, Some(429), None),
                Some(RATE_LIMITED),
                false,
            ),
            (
                "in-band rate limit",
                error(Transient, None, Some("rate_limit_error")),
                Some(RATE_LIMITED),
                false,
            ),
        ];
        for (case, raw, code, names_account) in scoped {
            let stamped =
                stamp_credential(raw.clone(), Some("acct-1"), Some("apikey:anthropic:work"));
            assert_eq!(
                stamped.credential_id.as_deref(),
                Some("apikey:anthropic:work"),
                "{case}"
            );
            assert_eq!(stamped.provider_code.as_deref(), code, "{case}");
            assert_eq!(
                stamped.account_ref.as_deref(),
                names_account.then_some("acct-1"),
                "{case}"
            );
            // A run that froze no credential selection names none.
            let bare = stamp_credential(raw, None, None);
            assert_eq!(bare.credential_id, None, "{case} without a selection");
        }
        let unscoped = [
            (
                "context overflow",
                error(ContextOverflow, Some(400), Some("context_length_exceeded")),
            ),
            (
                "bad request",
                error(Permanent, Some(400), Some("invalid_request_error")),
            ),
            (
                "policy refusal",
                error(Permanent, None, Some("cyber_policy")),
            ),
            (
                "content filter",
                error(Permanent, None, Some("content_filter")),
            ),
            (
                "tier refusal on 429",
                error(Permanent, Some(429), Some("service_tier_unavailable")),
            ),
            (
                "response deadline",
                error(Transient, None, Some("response_deadline")),
            ),
            (
                "server error",
                error(Transient, Some(500), Some("api_error")),
            ),
            (
                "overloaded",
                error(Transient, Some(529), Some("overloaded_error")),
            ),
            ("timeout", error(Transient, Some(408), None)),
            ("stream cut", error(ProviderUnavailable, None, None)),
            ("auth", error(Auth, Some(401), Some("authentication_error"))),
            (
                "crash orphan",
                error(Transient, None, Some("previous_process_ended_unsealed")),
            ),
        ];
        for (case, raw) in unscoped {
            let stamped =
                stamp_credential(raw.clone(), Some("acct-1"), Some("apikey:anthropic:work"));
            assert_eq!(stamped, raw, "{case} is returned unchanged");
        }
    }
}
