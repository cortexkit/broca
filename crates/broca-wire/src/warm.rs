//! `session.warm` and the prompt-cache facts broca reports beside a session.
//!
//! A warm re-sends the session's most recent model request, shaped so the provider generates
//! nothing, which keeps the provider's cached copy of the request's prefix alive. The caller
//! decides when to warm (from `session.head`'s `cache`) and how often; broca performs each warm
//! exactly and reports what it cost. Every refusal is decided before anything is sent, so a
//! refused warm costs nothing and writes nothing.

use broca_protocol::caller::SharedProviderError;
use serde::{Deserialize, Serialize};

pub const OP_SESSION_WARM: &str = "session.warm";

/// The only `expiry_basis` broca reports: the provider's documented lifetime counted from when
/// broca started sending. No provider reports a cache's expiry itself.
pub const EXPIRY_BASIS_ESTIMATED: &str = "estimated";

/// What a session's frozen config declares about its prompt cache, on the reply to the send that
/// admitted the session and on `session.baseline`. Every member is absent when unknown.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheDeclarationWire {
    /// How long the provider keeps a cached prefix after the request that wrote or read it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    /// Every lifetime the provider offers, shortest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ttl_options_ms: Vec<u64>,
    /// Whether reading a cached prefix restarts its lifetime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_refreshes_ttl: Option<bool>,
    /// `supported` when `session.warm` can warm this session, `unsupported` when it refuses
    /// every warm with `warm_unsupported`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warm: Option<String>,
    /// Where `ttl_ms` comes from: `requested` (the session's cache request named it) or
    /// `provider_default` (the provider's own default applies).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis: Option<String>,
}

pub const WARM_SUPPORTED: &str = "supported";
pub const WARM_UNSUPPORTED: &str = "unsupported";

/// The `cache` field of `session.head`: when Broca last started sending this session's request
/// (a model step or a warm), and expiry estimated as that send time plus the declared TTL.
/// Read from memory only. After a restart, or after the session falls out of the in-memory
/// table, the clock is rebuilt from the session's write-ahead log by the next `session.warm` or
/// `keep_warm: "auto"` run; until then, or until the session sends again, `cache` is absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadCacheWire {
    pub last_sent_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    pub expiry_basis: String,
    /// Fresh input plus cache reads and writes for the most recent request. Absent when
    /// the provider reported no input component; missing components contribute nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_tokens: Option<u64>,
}

/// `session.warm` params. Strict: an unknown member is refused `invalid_params`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WarmParams {
    /// The lineage the caller believes is current. A warm naming any other is refused
    /// `lineage_changed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_id: Option<String>,
}

/// The reply to a warm the provider answered.
///
/// There is no hit flag: a warm can read part of the prefix from cache and write the rest, and
/// the three token counts say how much of each. Each count is absent when the provider did not
/// report it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmReply {
    /// Lowercase hex SHA-256 of the request whose prefix was warmed: the digest that request's
    /// own attempt records carry.
    pub request_body_sha256: String,
    /// Lowercase hex SHA-256 of the bytes the warm actually sent.
    pub warm_body_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    pub ttl_ms: u64,
    /// The warm's send start plus `ttl_ms`.
    pub expires_at_ms: u64,
    /// Always [`EXPIRY_BASIS_ESTIMATED`].
    pub expiry_basis: String,
}

/// The error codes `session.warm` refuses with. Every one but `warm_failed` is decided before
/// anything is sent.
pub const WARM_REFUSAL_CODES: [&str; 10] = [
    "lineage_changed",
    "warm_in_flight",
    "run_paused",
    "account_walled",
    "account_changed",
    "no_step_yet",
    "warm_unsupported",
    "cache_expired",
    "warm_prefix_unavailable",
    "warm_failed",
];

/// A refused `session.warm`, as an error body with subc's `ErrorBody` members. Callers branch on
/// `code`, which always equals `detail.kind`; `message` is for people and never parsed. The bytes
/// of each refusal are pinned by golden files under `tests/golden/`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WarmRefusal {
    pub code: String,
    pub message: String,
    pub detail: WarmRefusalDetail,
}

/// What a warm refusal names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WarmRefusalDetail {
    /// The warm named a lineage that is not the session's current one.
    LineageChanged { requested: String },
    /// Another warm of the session is still in flight; at most one runs at a time.
    WarmInFlight,
    /// The session's latest run is paused; nothing is warmed until it resumes or ends.
    RunPaused {
        run_id: String,
        reason: broca_protocol::PauseReason,
    },
    /// The latest attempt found the provider account exhausted, and it has not reset yet.
    AccountWalled { resets_at_ms: u64 },
    /// The credential now belongs to a different account than the one the session froze.
    AccountChanged {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        frozen_account: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        served_account: Option<String>,
    },
    /// The session has sent no model request, so there is nothing to warm.
    NoStepYet,
    /// The provider, or a shape the session froze, cannot be warmed.
    WarmUnsupported { reason: String },
    /// The cache's estimated expiry is unknown or too close: a warm would write the whole prefix
    /// instead of reading it.
    CacheExpired {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_at_ms: Option<u64>,
        reason: String,
    },
    /// The most recent request cannot be reproduced byte for byte, so a warm would read a
    /// different prefix.
    WarmPrefixUnavailable {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_body_sha256: Option<String>,
        reason: String,
    },
    /// The warm was sent and the provider refused it.
    WarmFailed { error: SharedProviderError },
}

impl WarmRefusal {
    pub fn new(detail: WarmRefusalDetail) -> Self {
        let (code, message) = match &detail {
            WarmRefusalDetail::LineageChanged { requested } => (
                "lineage_changed",
                format!("the session's lineage is not '{requested}'; re-read it from the tail"),
            ),
            WarmRefusalDetail::WarmInFlight => (
                "warm_in_flight",
                "another warm of this session is still in flight; nothing was sent".to_owned(),
            ),
            WarmRefusalDetail::RunPaused { run_id, reason } => (
                "run_paused",
                format!(
                    "run '{run_id}' is paused ({}); nothing is warmed until it resumes or ends",
                    reason.as_str()
                ),
            ),
            WarmRefusalDetail::AccountWalled { resets_at_ms } => (
                "account_walled",
                format!(
                    "the provider account is exhausted until {resets_at_ms} (Unix ms); nothing \
                     was sent"
                ),
            ),
            WarmRefusalDetail::AccountChanged { .. } => (
                "account_changed",
                "the credential now belongs to a different account than the one the session \
                 froze; nothing was sent"
                    .to_owned(),
            ),
            WarmRefusalDetail::NoStepYet => (
                "no_step_yet",
                "the session has sent no model request, so there is nothing to warm".to_owned(),
            ),
            WarmRefusalDetail::WarmUnsupported { reason } => (
                "warm_unsupported",
                format!("this session cannot be warmed: {reason}"),
            ),
            WarmRefusalDetail::CacheExpired { reason, .. } => (
                "cache_expired",
                format!("not warmed, nothing was sent: {reason}"),
            ),
            WarmRefusalDetail::WarmPrefixUnavailable { reason, .. } => (
                "warm_prefix_unavailable",
                format!(
                    "the session's most recent request cannot be reproduced exactly, so nothing \
                     was sent: {reason}"
                ),
            ),
            WarmRefusalDetail::WarmFailed { .. } => (
                "warm_failed",
                "the warm was sent and the provider refused it".to_owned(),
            ),
        };
        Self {
            code: code.to_owned(),
            message,
            detail,
        }
    }
}

#[cfg(test)]
mod golden {
    use super::*;
    use crate::scope_refusal_golden::assert_golden;

    fn refusal(detail: WarmRefusalDetail) -> WarmRefusal {
        WarmRefusal::new(detail)
    }

    /// One golden per refusal code, each pinned by digest. A changed body fails until both the
    /// file and its digest are changed on purpose.
    #[test]
    fn every_warm_refusal_has_pinned_bytes() {
        let cases: [(WarmRefusal, &str, &str); 10] = [
            (
                refusal(WarmRefusalDetail::WarmInFlight),
                "warm_refusal.warm_in_flight.json",
                WARM_IN_FLIGHT_SHA256,
            ),
            (
                refusal(WarmRefusalDetail::LineageChanged {
                    requested: "lin-old".into(),
                }),
                "warm_refusal.lineage_changed.json",
                LINEAGE_CHANGED_SHA256,
            ),
            (
                refusal(WarmRefusalDetail::RunPaused {
                    run_id: "run-1".into(),
                    reason: broca_protocol::PauseReason::AuthRequired,
                }),
                "warm_refusal.run_paused.json",
                RUN_PAUSED_SHA256,
            ),
            (
                refusal(WarmRefusalDetail::AccountWalled {
                    resets_at_ms: 1_790_740_800_000,
                }),
                "warm_refusal.account_walled.json",
                ACCOUNT_WALLED_SHA256,
            ),
            (
                refusal(WarmRefusalDetail::AccountChanged {
                    frozen_account: Some("acct-1".into()),
                    served_account: Some("acct-2".into()),
                }),
                "warm_refusal.account_changed.json",
                ACCOUNT_CHANGED_SHA256,
            ),
            (
                refusal(WarmRefusalDetail::NoStepYet),
                "warm_refusal.no_step_yet.json",
                NO_STEP_YET_SHA256,
            ),
            (
                refusal(WarmRefusalDetail::WarmUnsupported {
                    reason: "bedrock converse has no documented zero-output request".into(),
                }),
                "warm_refusal.warm_unsupported.json",
                WARM_UNSUPPORTED_SHA256,
            ),
            (
                refusal(WarmRefusalDetail::CacheExpired {
                    expires_at_ms: Some(1_790_000_300_000),
                    reason: "the cache's estimated expiry is less than 5000 ms away".into(),
                }),
                "warm_refusal.cache_expired.json",
                CACHE_EXPIRED_SHA256,
            ),
            (
                refusal(WarmRefusalDetail::WarmPrefixUnavailable {
                    request_body_sha256: Some("ab".repeat(32)),
                    reason: "the re-rendered request does not match its recorded digest".into(),
                }),
                "warm_refusal.warm_prefix_unavailable.json",
                WARM_PREFIX_UNAVAILABLE_SHA256,
            ),
            (
                refusal(WarmRefusalDetail::WarmFailed {
                    error: broca_protocol::error::ProviderError {
                        provider_details: None,
                        provider_subcode: None,
                        refusal: None,
                        class: broca_protocol::error::ErrorClass::Transient,
                        message: "529: overloaded".into(),
                        status: Some(529),
                        retry_after_secs: None,
                        provider_code: Some("overloaded_error".into()),
                        resets_at_ms: None,
                        resets_at_source: None,
                        limit_window: None,
                        account_ref: None,
                        credential_id: None,
                        affordable_max_tokens: None,
                    }
                    .into(),
                }),
                "warm_refusal.warm_failed.json",
                WARM_FAILED_SHA256,
            ),
        ];
        for (body, file, sha256) in cases {
            assert_eq!(
                body.code,
                serde_json::to_value(&body.detail).unwrap()["kind"]
            );
            assert!(WARM_REFUSAL_CODES.contains(&body.code.as_str()));
            assert_golden(body, file, sha256);
        }
    }

    #[test]
    fn a_warm_reply_has_pinned_bytes_and_keeps_unreported_counts_absent() {
        assert_golden(
            WarmReply {
                request_body_sha256: "ab".repeat(32),
                warm_body_sha256: "cd".repeat(32),
                cache_read_tokens: Some(48_213),
                cache_write_tokens: Some(0),
                input_tokens: Some(3),
                ttl_ms: 3_600_000,
                expires_at_ms: 1_790_003_600_000,
                expiry_basis: EXPIRY_BASIS_ESTIMATED.into(),
            },
            "warm_reply.json",
            WARM_REPLY_SHA256,
        );
        let unreported = WarmReply {
            request_body_sha256: "ab".repeat(32),
            warm_body_sha256: "cd".repeat(32),
            cache_read_tokens: None,
            cache_write_tokens: None,
            input_tokens: None,
            ttl_ms: 300_000,
            expires_at_ms: 1_790_000_300_000,
            expiry_basis: EXPIRY_BASIS_ESTIMATED.into(),
        };
        let json = serde_json::to_value(&unreported).unwrap();
        for absent in ["cache_read_tokens", "cache_write_tokens", "input_tokens"] {
            assert!(json.get(absent).is_none(), "{absent} must stay absent");
        }
    }

    #[test]
    fn head_cache_prefix_tokens_has_pinned_bytes_and_unknown_is_absent() {
        let mut cache = HeadCacheWire {
            last_sent_at_ms: 1_790_000_000_000,
            ttl_ms: Some(3_600_000),
            expires_at_ms: Some(1_790_003_600_000),
            expiry_basis: EXPIRY_BASIS_ESTIMATED.into(),
            prefix_tokens: Some(48_216),
        };
        assert_golden(
            cache.clone(),
            "head_cache.prefix_tokens.json",
            "7b58a5713715fcf9d5386837c5529152deb93e65c345feeb8a7a26748a33378c",
        );
        cache.prefix_tokens = None;
        assert!(serde_json::to_value(cache)
            .unwrap()
            .get("prefix_tokens")
            .is_none());
    }

    #[test]
    fn warm_params_are_strict() {
        assert!(serde_json::from_str::<WarmParams>(r#"{"lineage_id":"l"}"#).is_ok());
        assert!(serde_json::from_str::<WarmParams>(r#"{"lineage":"l"}"#).is_err());
    }

    const WARM_IN_FLIGHT_SHA256: &str =
        "d00b097d2a21fe820fb6ae69f5da26f0480308c3eece6a569b8aaba65e2cc70f";
    const LINEAGE_CHANGED_SHA256: &str =
        "211056317f7bf91078545ea8979869cd906d0ee25965895b7c1939d345bd8fe3";
    const RUN_PAUSED_SHA256: &str =
        "3617a85903e686c332eca2291f0de59c61ce37d2111ab815148dfa8136c7ac2b";
    const ACCOUNT_WALLED_SHA256: &str =
        "5dc91d84bc5572c0372b1fe93388b86c9cb6b7dc6e43cb8619140145e0c90d9a";
    const ACCOUNT_CHANGED_SHA256: &str =
        "b2efbd724752ff5f108bb2293964768dbdab4d72119499dae96a629a2826c88e";
    const NO_STEP_YET_SHA256: &str =
        "54585d82cd49b01824d8cc54edeb25e96b126f243b99b0e95b422840a0503f14";
    const WARM_UNSUPPORTED_SHA256: &str =
        "1bbe95b6a1abe248d42f12a433b4fb369802f934110cff2f62922297569de32e";
    const CACHE_EXPIRED_SHA256: &str =
        "12f12848b5c2dff0e2b5d0506383e4da8d5d5fb96827a6a91ad4e4d92d2f20ed";
    const WARM_PREFIX_UNAVAILABLE_SHA256: &str =
        "eb890f79a8c4683cb62b0fa658c2502108282cc1376e1014bc004ff77b3c7fe6";
    const WARM_FAILED_SHA256: &str =
        "91547063668573061dac1a38545a48fd1b4ef863b63c0f0e17c00aff8ea736e2";
    const WARM_REPLY_SHA256: &str =
        "67219ccf36a67e604c4769c61b9060d29d9c8926c37189e40e7b2aec3d9858f1";
}
