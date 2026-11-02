use broca_protocol::{ErrorClass, ModelAttempt, ProviderError, StepId};
use broca_wal::{AccountLimitPause, AuthSelection, WalRecord};
use std::sync::atomic::{AtomicU64, Ordering};

static STALE_RESETS: AtomicU64 = AtomicU64::new(0);
static DECLINED: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];

/// Boot-configured upper bound on a same-account wait, additionally bounded by cache expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SameAccountWait {
    Minutes(u64),
    Max,
}

impl Default for SameAccountWait {
    fn default() -> Self {
        Self::Minutes(15)
    }
}

impl SameAccountWait {
    pub fn enabled(self) -> bool {
        self != Self::Minutes(0)
    }

    fn cap(self, now_ms: u64) -> Option<u64> {
        match self {
            Self::Minutes(minutes) => Some(now_ms.saturating_add(minutes.saturating_mul(60_000))),
            Self::Max => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeclineReason {
    Disabled,
    NoAcceptedResponse,
    CacheExpiresFirst,
    BeyondCap,
}

impl DeclineReason {
    fn name(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::NoAcceptedResponse => "no_accepted_response",
            Self::CacheExpiresFirst => "cache_expires_first",
            Self::BeyondCap => "beyond_cap",
        }
    }
}

/// Lifetime declined pause counts, including zero readings for every reason.
pub fn account_limit_pause_declined() -> serde_json::Value {
    let mut counts = serde_json::Map::new();
    for reason in [
        DeclineReason::Disabled,
        DeclineReason::NoAcceptedResponse,
        DeclineReason::CacheExpiresFirst,
        DeclineReason::BeyondCap,
    ] {
        counts.insert(
            reason.name().into(),
            DECLINED[reason as usize].load(Ordering::Relaxed).into(),
        );
    }
    counts.into()
}

pub(crate) struct PauseBudget {
    pub setting: SameAccountWait,
    pub cache_expiry_ms: Option<u64>,
}

impl PauseBudget {
    fn decide(&self, now_ms: u64, wake_ms: u64) -> Result<(), DeclineReason> {
        if !self.setting.enabled() {
            return Err(DeclineReason::Disabled);
        }
        let expiry = self
            .cache_expiry_ms
            .ok_or(DeclineReason::NoAcceptedResponse)?;
        let cap = self.setting.cap(now_ms);
        if wake_ms > expiry && cap.is_none_or(|cap| expiry <= cap) {
            return Err(DeclineReason::CacheExpiresFirst);
        }
        if cap.is_some_and(|cap| wake_ms > cap) {
            return Err(DeclineReason::BeyondCap);
        }
        Ok(())
    }
}

/// Cache expiry for a session/account's most recent accepted model attempt. Callers supply
/// that account's 2xx send time, not the response completion time or the latest refusal.
/// Unknown cache policies use five minutes; only explicit one-hour markers extend it.
/// No accepted send means no cache to protect.
pub fn session_account_cache_expiry_ms(
    last_accepted_at_ms: Option<u64>,
    frozen: &serde_json::Value,
) -> Option<u64> {
    let ttl_ms = if frozen
        .pointer("/cache/class")
        .and_then(|class| class.as_str())
        == Some("markers")
        && frozen.pointer("/cache/ttl").and_then(|ttl| ttl.as_str()) == Some("1h")
    {
        3_600_000
    } else {
        300_000
    };
    last_accepted_at_ms.map(|sent| sent.saturating_add(ttl_ms))
}

/// Lifetime refusals this process ended because their computed account reset was already past.
pub fn account_limit_stale_resets() -> u64 {
    STALE_RESETS.load(Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimingRefusal {
    Untimed,
    Invalid,
    Stale,
}

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

/// Compare identity, not routing or vault-request diagnostics that a resolver may refresh.
/// Known served IDs must match; missing IDs remain unknown without rewriting frozen config.
pub(crate) fn same_selection(actual: &AuthSelection, frozen: Option<&AuthSelection>) -> bool {
    fn known_ids_match(actual: &Option<String>, frozen: &Option<String>) -> bool {
        actual
            .as_ref()
            .zip(frozen.as_ref())
            .is_none_or(|(actual, frozen)| actual == frozen)
    }
    frozen.is_some_and(|frozen| {
        actual.credential_id == frozen.credential_id
            && actual.auth_method == frozen.auth_method
            && known_ids_match(&actual.account_ref, &frozen.account_ref)
            && known_ids_match(
                &actual.resolved_credential_id,
                &frozen.resolved_credential_id,
            )
    })
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
    floor
        .checked_sub(paused_at_ms)
        .ok_or(TimingRefusal::Stale)?;
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
        budget: &PauseBudget,
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
        if let Err(reason) = budget.decide(paused_at_ms, resume_not_before_ms) {
            DECLINED[reason as usize].fetch_add(1, Ordering::Relaxed);
            tracing::info!(reset = ?source.resets_at_ms, wake = resume_not_before_ms,
                cache_expiry = ?budget.cache_expiry_ms, cap = ?budget.setting.cap(paused_at_ms),
                reason = reason.name(), "account-limit pause declined");
            return None;
        }
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
    fn account_limit_pause_budget_covers_cache_cap_and_consent() {
        let now = 120_000;
        let frozen = serde_json::json!({"cache":{"class":"markers","ttl":"5m"}});
        let expiry = session_account_cache_expiry_ms(Some(0), &frozen);
        for (name, setting, cache_expiry_ms, wake, expected) in [
            (
                "before both",
                SameAccountWait::Minutes(15),
                expiry,
                240_000,
                Ok(()),
            ),
            (
                "cache expires first",
                SameAccountWait::Minutes(15),
                expiry,
                1_920_000,
                Err(DeclineReason::CacheExpiresFirst),
            ),
            (
                "beyond cap",
                SameAccountWait::Minutes(1),
                expiry,
                240_000,
                Err(DeclineReason::BeyondCap),
            ),
            (
                "disabled",
                SameAccountWait::Minutes(0),
                expiry,
                120_000,
                Err(DeclineReason::Disabled),
            ),
            (
                "no accepted response",
                SameAccountWait::Max,
                None,
                120_000,
                Err(DeclineReason::NoAcceptedResponse),
            ),
            (
                "cache equality",
                SameAccountWait::Max,
                expiry,
                300_000,
                Ok(()),
            ),
            (
                "cap equality",
                SameAccountWait::Minutes(1),
                expiry,
                180_000,
                Ok(()),
            ),
        ] {
            assert_eq!(
                PauseBudget {
                    setting,
                    cache_expiry_ms
                }
                .decide(now, wake),
                expected,
                "{name}"
            );
        }
        let hour = serde_json::json!({"cache":{"class":"markers","ttl":"1h"}});
        let budget = PauseBudget {
            setting: SameAccountWait::Max,
            cache_expiry_ms: session_account_cache_expiry_ms(Some(0), &hour),
        };
        assert_eq!(budget.decide(now, 3_600_000), Ok(()));
        assert_eq!(
            budget.decide(now, 3_600_001),
            Err(DeclineReason::CacheExpiresFirst)
        );
    }

    #[test]
    fn session_account_cache_expiry_uses_send_time_frozen_ttl_and_unknown_default() {
        for (frozen, ttl) in [
            (serde_json::Value::Null, 300_000),
            (
                serde_json::json!({"cache":{"class":"markers","ttl":"5m"}}),
                300_000,
            ),
            (
                serde_json::json!({"cache":{"class":"markers","ttl":"1h"}}),
                3_600_000,
            ),
            (
                serde_json::json!({"cache":{"class":"hints","retention":"24h"}}),
                300_000,
            ),
        ] {
            assert_eq!(
                session_account_cache_expiry_ms(Some(1000), &frozen),
                Some(1000 + ttl)
            );
            assert_eq!(session_account_cache_expiry_ms(None, &frozen), None);
        }
    }

    #[test]
    fn account_selection_identity_rejects_known_changes_and_keeps_unknown_witnesses_unknown() {
        let frozen: AuthSelection = serde_json::from_value(serde_json::json!({
            "credential_id":"opaque", "auth_method":"oauth", "account_ref":"acct-a", "resolved_credential_id":"cred-a"
        })).unwrap();
        assert!(same_selection(&frozen, Some(&frozen)));
        assert!(!same_selection(&frozen, None));
        for field in [
            "credential_id",
            "auth_method",
            "account_ref",
            "resolved_credential_id",
        ] {
            let mut changed = serde_json::to_value(&frozen).unwrap();
            changed[field] = serde_json::json!("other");
            assert!(
                !same_selection(&serde_json::from_value(changed).unwrap(), Some(&frozen)),
                "{field}"
            );
        }
        let unknown: AuthSelection = serde_json::from_value(
            serde_json::json!({"credential_id":"opaque", "auth_method":"oauth"}),
        )
        .unwrap();
        assert!(same_selection(&unknown, Some(&frozen)));
        assert!(same_selection(&frozen, Some(&unknown)));
    }

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
            ("no fixed ceiling", Some(3_601_001), None, Ok(3_601_001)),
            ("weekly reset", Some(604_801_000), None, Ok(604_801_000)),
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
            let budget = PauseBudget {
                setting: SameAccountWait::default(),
                cache_expiry_ms: Some(301_000),
            };
            assert_eq!(
                evidence
                    .pause(StepId(1), 1000, &error, Some(&selection), &budget)
                    .is_some(),
                expected,
                "{name}"
            );
            assert!(evidence
                .pause(StepId(1), 1000, &error, None, &budget)
                .is_none());
        }
    }
}
