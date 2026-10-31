use broca_protocol::{ErrorClass, ModelAttempt, ProviderError, StepId};
use broca_wal::{AccountLimitPause, AuthSelection, WalRecord};
use std::sync::atomic::{AtomicU64, Ordering};

static STALE_RESETS: AtomicU64 = AtomicU64::new(0);

/// Lifetime refusals this process ended because their computed account reset was already past.
pub fn account_limit_stale_resets() -> u64 {
    STALE_RESETS.load(Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimingRefusal {
    Untimed,
    Invalid,
    Stale,
    Distant,
}

// A worker must not sleep until a weekly reset. Ending a longer wait lets Core revive the
// worker by starting a replacement that routing can assign to another model. Short waits can
// preserve the admitted selection and its prompt-cache opportunity.
pub(crate) const MAX_ACCOUNT_LIMIT_PAUSE_MS: u64 = 60 * 60 * 1000;

pub(crate) fn request_changed_error() -> ProviderError {
    ProviderError {
        class: ErrorClass::Permanent,
        message: "The paused account-limit request no longer renders to its recorded bytes".into(),
        provider_code: Some("account_limit_request_changed".into()),
        status: None,
        retry_after_secs: None,
        provider_subcode: None,
        refusal: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        provider_details: None,
    }
}

pub(crate) fn selection_changed_error() -> ProviderError {
    let mut error = request_changed_error();
    error.message =
        "A timed account pause cannot change its frozen credential or account selection".into();
    error.provider_code = Some("account_limit_selection_changed".into());
    error
}

pub(crate) fn resume_floor(
    paused_at_ms: u64,
    observed_at_ms: u64,
    resets_at_ms: Option<u64>,
    retry_after_secs: Option<f64>,
) -> Result<u64, TimingRefusal> {
    let retry_floor = match retry_after_secs {
        None => None,
        Some(seconds) => {
            let millis = (seconds * 1000.0).ceil();
            if !seconds.is_finite()
                || seconds < 0.0
                || !millis.is_finite()
                || millis >= u64::MAX as f64
            {
                return Err(TimingRefusal::Invalid);
            }
            Some(
                observed_at_ms
                    .checked_add(millis as u64)
                    .ok_or(TimingRefusal::Invalid)?,
            )
        }
    };
    let floor = match (resets_at_ms, retry_floor) {
        (Some(reset), Some(retry)) => reset.max(retry),
        (Some(reset), None) => reset,
        (None, Some(retry)) => retry,
        (None, None) => return Err(TimingRefusal::Untimed),
    };
    let wait = floor
        .checked_sub(paused_at_ms)
        .ok_or(TimingRefusal::Stale)?;
    if wait > MAX_ACCOUNT_LIMIT_PAUSE_MS {
        return Err(TimingRefusal::Distant);
    }
    Ok(floor)
}

pub(crate) fn attempt_record(step: StepId, attempt: ModelAttempt, enabled: bool) -> WalRecord {
    let disposition = enabled
        .then(|| {
            attempt
                .account_observation
                .limit
                .as_ref()
                .map(|limit| limit.disposition)
        })
        .flatten();
    let mut record = WalRecord::model_attempt_finished(step, attempt);
    if let WalRecord::ModelAttemptFinished {
        account_limit_disposition,
        ..
    } = &mut record
    {
        *account_limit_disposition = disposition;
    }
    record
}

/// Only source-classified attempt evidence can authorize a timed account pause.
pub(crate) struct FailedAccountLimitAttempt {
    pub seq: u64,
    pub attempt: ModelAttempt,
}

impl FailedAccountLimitAttempt {
    pub fn pause(
        &self,
        step: StepId,
        paused_at_ms: u64,
        error: &ProviderError,
        selection: Option<&AuthSelection>,
    ) -> Option<Box<AccountLimitPause>> {
        if error.class != ErrorClass::Transient {
            return None;
        }
        let source = self.attempt.account_observation.limit.as_ref()?;
        let selection = selection?;
        // A selection without auth_method is a broca lookup key, whose first component is
        // its method. With auth_method present the lookup id is opaque and is never parsed.
        let auth_method = selection.auth_method.clone().or_else(|| {
            selection
                .credential_id
                .split_once(':')
                .map(|(method, _)| method.to_owned())
        })?;
        let resume_not_before_ms = match resume_floor(
            paused_at_ms,
            source.observed_at_ms,
            source.resets_at_ms,
            error.retry_after_secs,
        ) {
            Ok(floor) => floor,
            Err(TimingRefusal::Stale) => {
                STALE_RESETS.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            Err(_) => return None,
        };
        Some(Box::new(AccountLimitPause {
            failed_step: step,
            failed_attempt_seq: self.seq,
            attempt_id: None,
            attempt: self.attempt.attempt,
            paused_at_ms,
            observed_at_ms: source.observed_at_ms,
            auth_method,
            credential_id: selection.credential_id.clone(),
            credential_ref: selection.resolved_credential_id.clone(),
            account_ref: self
                .attempt
                .served_account_ref
                .clone()
                .or_else(|| selection.account_ref.clone()),
            disposition: source.disposition,
            resets_at_ms: source.resets_at_ms,
            retry_after_secs: error.retry_after_secs,
            resume_not_before_ms,
            request_body_len: self.attempt.request_body_len,
            request_body_sha256: self.attempt.request_body_sha256.clone(),
            selection_round: 0,
            next_routing_check_ms: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_limit_timing_table_pins_both_edges_and_present_values() {
        let cases = [
            ("no timing", None, None, Err(TimingRefusal::Untimed)),
            ("lower edge", Some(1000), None, Ok(1000)),
            (
                "stale by one ms",
                Some(999),
                None,
                Err(TimingRefusal::Stale),
            ),
            ("upper edge", Some(3_601_000), None, Ok(3_601_000)),
            (
                "too far by one ms",
                Some(3_601_001),
                None,
                Err(TimingRefusal::Distant),
            ),
            (
                "weekly reset",
                Some(604_801_000),
                None,
                Err(TimingRefusal::Distant),
            ),
            ("relative only", None, Some(60.0), Ok(61_000)),
            ("fraction rounds up", None, Some(0.0001), Ok(1001)),
            ("zero retry is present", None, Some(0.0), Ok(1000)),
            (
                "absolute zero is present but stale",
                Some(0),
                None,
                Err(TimingRefusal::Stale),
            ),
            ("reset wins", Some(70_000), Some(60.0), Ok(70_000)),
            ("retry wins", Some(50_000), Some(60.0), Ok(61_000)),
            (
                "negative",
                Some(61_000),
                Some(-1.0),
                Err(TimingRefusal::Invalid),
            ),
            (
                "infinity",
                Some(61_000),
                Some(f64::INFINITY),
                Err(TimingRefusal::Invalid),
            ),
            (
                "not a number",
                Some(61_000),
                Some(f64::NAN),
                Err(TimingRefusal::Invalid),
            ),
            (
                "multiplication overflow",
                None,
                Some(f64::MAX),
                Err(TimingRefusal::Invalid),
            ),
        ];
        for (name, reset, retry, expected) in cases {
            assert_eq!(resume_floor(1000, 1000, reset, retry), expected, "{name}");
        }
        assert_eq!(
            resume_floor(2000, 1000, None, Some(1.0)),
            Ok(2000),
            "anchor at observation, not pause"
        );
        assert_eq!(
            resume_floor(u64::MAX - 2, u64::MAX - 2, None, Some(0.003)),
            Err(TimingRefusal::Invalid)
        );
        assert_eq!(
            resume_floor(u64::MAX - 2, 0, Some(u64::MAX), None),
            Ok(u64::MAX)
        );
    }

    #[test]
    fn account_limit_classification_requires_the_source_disposition_not_status_or_detail() {
        use broca_protocol::{provider::AccountLimitDisposition, AttemptAccountLimit};
        let selection: AuthSelection = serde_json::from_value(serde_json::json!({
            "credential_id":"opaque-vault-id", "auth_method":"oauth"
        }))
        .unwrap();
        for (name, class, source, reset, retry, expected) in [
            (
                "timed account",
                ErrorClass::Transient,
                true,
                Some(61_000),
                None,
                true,
            ),
            (
                "retry account",
                ErrorClass::Transient,
                true,
                None,
                Some(60.0),
                true,
            ),
            (
                "untimed account",
                ErrorClass::Transient,
                true,
                None,
                None,
                false,
            ),
            (
                "weekly account",
                ErrorClass::Transient,
                true,
                Some(604_801_000),
                None,
                false,
            ),
            (
                "generic 429 with retry",
                ErrorClass::Transient,
                false,
                None,
                Some(60.0),
                false,
            ),
            (
                "detail code alone",
                ErrorClass::Transient,
                false,
                Some(61_000),
                None,
                false,
            ),
            (
                "credits",
                ErrorClass::Permanent,
                false,
                Some(61_000),
                None,
                false,
            ),
            ("auth", ErrorClass::Auth, true, Some(61_000), None, false),
            (
                "context overflow",
                ErrorClass::ContextOverflow,
                true,
                Some(61_000),
                None,
                false,
            ),
            (
                "overload",
                ErrorClass::Transient,
                false,
                Some(61_000),
                None,
                false,
            ),
        ] {
            let mut attempt: ModelAttempt = serde_json::from_value(serde_json::json!({
                "attempt":0, "request_body_len":1, "request_body_sha256":"a".repeat(64),
                "response_body_len":0, "response_body_sha256":"0".repeat(64),
                "outcome":"http_error", "ts_ms":1000
            }))
            .unwrap();
            if source {
                attempt.account_observation.limit = Some(AttemptAccountLimit {
                    disposition: AccountLimitDisposition::ACCOUNT,
                    observed_at_ms: 1000,
                    resets_at_ms: reset,
                    resets_at_source: None,
                    limit_window: None,
                });
            }
            let error: ProviderError = serde_json::from_value(serde_json::json!({
                "class":class, "message":name, "status":429,
                "provider_code":"account_limit_exhausted", "resets_at_ms":reset,
                "retry_after_secs":retry
            }))
            .unwrap();
            let evidence = FailedAccountLimitAttempt { seq: 3, attempt };
            assert_eq!(
                evidence
                    .pause(StepId(1), 1000, &error, Some(&selection))
                    .is_some(),
                expected,
                "{name}"
            );
            assert!(evidence.pause(StepId(1), 1000, &error, None).is_none());
        }
    }
}
