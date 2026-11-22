//! Keeping a session's provider prompt cache warm between rounds.
//!
//! A provider that caches a request's prefix keeps it for a fixed lifetime from the moment the
//! request was sent. A caller whose next round arrives after that lifetime pays to write the whole
//! prefix again. A warm re-sends the session's most recent model request, shaped so the provider
//! generates nothing, which reads the cached prefix and restarts its lifetime.
//!
//! This module holds the provider-agnostic half: the exact bytes of the request in flight (kept
//! in memory by the run's transport), the per-session clock that says when the cache was last
//! refreshed, and what a warm sent or why it failed.

use std::sync::{Arc, Mutex};

use broca_protocol::{error::ProviderError, AttemptOutcome, CallOptions, RunId, StepId, Usage};
use broca_records::record::RunConfig;

/// A warm is refused, and nothing sent, when less than this much of the cache's estimated
/// lifetime remains. The estimate is only that, so a warm at the very edge could arrive after the
/// provider dropped the prefix and pay for a full cache write instead of a read.
pub const WARM_EXPIRY_MARGIN_MS: u64 = 5_000;

/// The only basis broca has for a cache's expiry: the provider's documented lifetime counted from
/// when broca started sending. No provider reports the expiry itself.
pub const EXPIRY_BASIS_ESTIMATED: &str = "estimated";

/// New source-maintenance records opt into source-model billing attribution. Other trigger
/// values, including absence, retain the historical projection for the same WAL bytes.
pub const SOURCE_KEEP_WARM_TRIGGER: &str = "source_keep_warm";

/// The exact bytes of one rendered model request, as the transport sent them.
#[derive(Clone)]
pub struct SentRequest {
    /// The step the request belongs to.
    pub step_id: StepId,
    /// The request body, shared with the transport rather than copied.
    pub body: Arc<dyn AsRef<[u8]> + Send + Sync>,
    /// Lowercase hex SHA-256 of `body`, the digest the attempt's record carries.
    pub sha256: String,
    /// When the transport started sending it, in milliseconds since the Unix epoch.
    pub sent_at_ms: u64,
    /// The model the transport sent it to: the episode's own model, or the refusal-fallback
    /// model while a refusal round routes steps there. A warm of these bytes must be shaped and
    /// addressed for the same model, or it would refresh a cache the run is not using.
    pub route: crate::refusal_fallback::StepRoute,
}

impl SentRequest {
    pub fn bytes(&self) -> &[u8] {
        (*self.body).as_ref()
    }
}

impl std::fmt::Debug for SentRequest {
    // The body holds the prompt, so it is never printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SentRequest")
            .field("step_id", &self.step_id)
            .field("body", &format_args!("<{} bytes>", self.bytes().len()))
            .field("sha256", &self.sha256)
            .field("sent_at_ms", &self.sent_at_ms)
            .field("route", &self.route)
            .finish()
    }
}

/// When a session last sent a request whose prefix the provider caches, and how long the
/// provider keeps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheClockReading {
    pub last_sent_at_ms: u64,
    /// The cache lifetime, when the session's cache policy declares one.
    pub ttl_ms: Option<u64>,
    /// The most recent request's reported input occupancy, not cumulative run usage.
    pub prefix_tokens: Option<u64>,
}

impl CacheClockReading {
    /// The estimated expiry: the last send start plus the lifetime.
    pub fn expires_at_ms(&self) -> Option<u64> {
        self.ttl_ms
            .map(|ttl_ms| self.last_sent_at_ms.saturating_add(ttl_ms))
    }
}

/// One session's cache clock, shared by every run of the session and by its warms. It lives in
/// memory, so after a restart it starts empty; [`CacheClock::seed_if_unknown`] then rebuilds it
/// from the session's WAL.
#[derive(Clone, Default)]
pub struct CacheClock(Arc<Mutex<Option<CacheClockReading>>>);

impl CacheClock {
    /// Whether another handle currently shares this clock. A cache table may
    /// discard a table-only clock while holding the lock that protects cloning
    /// its entries; shared clocks must remain discoverable while owners update them.
    pub fn is_shared(&self) -> bool {
        Arc::strong_count(&self.0) > 1
    }

    /// A request started sending at `sent_at_ms`. The clock never moves backwards, so a slow
    /// attempt that reports late cannot undo a later send.
    pub fn note_sent(&self, sent_at_ms: u64, ttl_ms: Option<u64>) {
        let mut reading = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        match reading.as_mut() {
            Some(current) if current.last_sent_at_ms > sent_at_ms => {}
            _ => {
                *reading = Some(CacheClockReading {
                    last_sent_at_ms: sent_at_ms,
                    ttl_ms,
                    prefix_tokens: None,
                })
            }
        }
    }

    /// Attach usage only to the send it describes; a late response cannot label a newer send.
    pub fn note_usage(&self, sent_at_ms: u64, usage: Usage) {
        let mut reading = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(current) = reading.as_mut().filter(|r| r.last_sent_at_ms == sent_at_ms) {
            current.prefix_tokens = prefix_tokens(usage);
        }
    }

    pub fn read(&self) -> Option<CacheClockReading> {
        *self.0.lock().unwrap_or_else(|poison| poison.into_inner())
    }
}

/// Each reported input component contributes; no reported component means unknown, not zero.
pub fn prefix_tokens(usage: Usage) -> Option<u64> {
    let counts = [
        usage.input_tokens,
        usage.cached_input_tokens,
        usage.cache_write_tokens,
    ];
    counts.into_iter().flatten().reduce(u64::saturating_add)
}

// A standby source warm can finish after a fallback request. Its digest identifies another
// request and must not extend the active route's clock or replace its token-count evidence.

/// Whether a model request attempt reached the provider and was accepted for processing, so its
/// prompt prefix was read into or written to the provider's cache. A 2xx status says so, and so
/// does 101 with an answer: a request on a WebSocket connection records 101, the socket's upgrade
/// status, at the moment it is sent on the open socket, and its outcome says whether the provider
/// answered it. Before this counted them, every ChatGPT WebSocket request was left out, so the
/// session's last send looked hours old and its cache looked expired. Not counted:
/// - no status at all (`connect_failed`, `header_timeout`, `no_status`, or a cancel or shutdown
///   before the status line) means the provider may never have seen the request;
/// - a 4xx or 5xx status is a refusal before any generation, which leaves the cache alone;
/// - `provider_error` (an error event inside a 2xx body, such as an overload) and an outcome
///   this build does not know may also be refusals, so they are not counted either.
///
/// Counted: `complete`, and the 2xx or 101 attempts that broke off while the response streamed
/// (`eof_before_completion`, `chunk_error`, `idle_timeout`, `content_timeout`, `byte_limit`,
/// `decode_error`, `rate_floor`, `cancelled`, `sealed_in_flight`). Leaving out a doubtful attempt
/// only falls back to an earlier send, which errs early.
pub fn attempt_reached_the_cache(http_status: Option<u16>, outcome: &AttemptOutcome) -> bool {
    http_status.is_some_and(|status| (200..300).contains(&status) || status == 101)
        && !matches!(
            outcome,
            AttemptOutcome::ProviderError | AttemptOutcome::Unknown(_)
        )
}

/// Whether a warm sent at `now_ms` would still find the cached prefix. `Err` carries the reason
/// it would not: the expiry is unknown, or less than [`WARM_EXPIRY_MARGIN_MS`] of it remains.
pub fn check_warm_expiry(reading: Option<CacheClockReading>, now_ms: u64) -> Result<u64, String> {
    let Some(reading) = reading else {
        return Err(
            "no request of this session has been sent since this broca started, so the cache's \
             expiry is unknown"
                .into(),
        );
    };
    let Some(expires_at_ms) = reading.expires_at_ms() else {
        return Err("the session's cache policy declares no lifetime".into());
    };
    if expires_at_ms.saturating_sub(now_ms) < WARM_EXPIRY_MARGIN_MS {
        return Err(format!(
            "the cache's estimated expiry {expires_at_ms} is less than {WARM_EXPIRY_MARGIN_MS} ms \
             away (now {now_ms}); a warm would write the whole prefix again"
        ));
    }
    Ok(expires_at_ms)
}

/// What a sent warm reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmSent {
    /// Actual warm request body length, absent when the sender did not measure it.
    pub request_body_bytes: Option<u64>,
    /// Lowercase hex SHA-256 of the bytes actually sent.
    pub warm_body_sha256: String,
    /// When sending started, in milliseconds since the Unix epoch.
    pub sent_at_ms: u64,
    /// The usage the provider reported, each count absent when it reported none.
    pub usage: Usage,
    /// The account the credential that sent the warm belongs to, when known.
    pub served_account_ref: Option<String>,
    /// The provider's rate-limit headers on the response, verbatim (lowercase names).
    pub ratelimit_headers: Vec<(String, String)>,
    pub http_status: u16,
}

/// Missing or reported-zero counts consume no warm budget and require no price lookup.
/// Here `Usage.output_tokens` already includes reasoning tokens; do not add them again.
pub fn warm_budget_needs_pricing(envelope: &broca_records::record::AttemptEnvelope) -> bool {
    envelope.sent != broca_records::record::AttemptSent::No
        && envelope.usage.is_some_and(|usage| {
            [
                usage.input_tokens,
                usage.cached_input_tokens,
                usage.cache_write_tokens,
                usage.output_tokens,
                envelope.cache_write_5m_tokens,
                envelope.cache_write_1h_tokens,
            ]
            .into_iter()
            .flatten()
            .any(|tokens| tokens > 0)
        })
}

/// Warm-only send facts taken from the exact frozen render route used for the request.
#[derive(Debug, Clone, Default)]
pub struct WarmSendPolicy {
    /// Whether the served token was refreshed after admission recorded its resolved credential ID.
    pub auth_refreshed: bool,
    /// A routed fallback's catalog provider; the source provider is already in the reservation.
    pub provider: Option<String>,
    /// The selected frozen marker policy, never the episode's cache-clock estimate.
    pub ttl_ms: Option<u64>,
}

/// Why a provider did not warm.
#[derive(Debug, Clone, PartialEq)]
pub enum WarmFailure {
    /// The provider, or the shape the session froze, cannot be warmed. Nothing was sent.
    Unsupported(String),
    /// The credential now belongs to a different account than the one the session froze.
    /// Nothing was sent.
    AccountChanged {
        frozen: Option<String>,
        served: Option<String>,
    },
    /// The warm was sent and the provider refused it.
    Provider(Box<ProviderError>),
}

/// The most recent model request a session's WAL records, rebuilt as the run loop assembled it.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    pub run_id: RunId,
    pub step_id: StepId,
    /// The episode's config as it stood when the step was sent (a refreeze applied).
    pub config: RunConfig,
    pub opts: CallOptions,
    /// The digest the step's attempt records carry.
    pub request_body_sha256: String,
    /// Set when the request went to the refusal-fallback model instead of the episode's: the
    /// warm must render it for that model (`opts.target_model` already names it), and its
    /// digest only matches when it does. `None` for a request to the episode's own model.
    pub route: Option<broca_records::record::StepRoute>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_expiry_fence_refuses_under_five_seconds_and_unknown_clocks() {
        let reading = |ttl_ms| {
            Some(CacheClockReading {
                last_sent_at_ms: 1_000_000,
                ttl_ms,
                prefix_tokens: None,
            })
        };
        assert_eq!(
            check_warm_expiry(reading(Some(300_000)), 1_000_000 + 300_000 - 5_000),
            Ok(1_300_000)
        );
        assert!(check_warm_expiry(reading(Some(300_000)), 1_000_000 + 300_000 - 4_999).is_err());
        assert!(check_warm_expiry(reading(Some(300_000)), 2_000_000).is_err());
        assert!(check_warm_expiry(reading(None), 1_000_000).is_err());
        assert!(check_warm_expiry(None, 1_000_000).is_err());
    }

    #[test]
    fn prefix_tokens_counts_reported_input_components_only() {
        assert_eq!(prefix_tokens(Usage::default()), None);
        assert_eq!(
            prefix_tokens(Usage {
                output_tokens: Some(16),
                ..Usage::default()
            }),
            None
        );
        assert_eq!(
            prefix_tokens(Usage {
                cached_input_tokens: Some(40),
                ..Usage::default()
            }),
            Some(40)
        );
        assert_eq!(
            prefix_tokens(Usage {
                input_tokens: Some(3),
                cached_input_tokens: Some(40),
                cache_write_tokens: Some(7),
                output_tokens: Some(100),
                ..Usage::default()
            }),
            Some(50)
        );
        assert_eq!(
            prefix_tokens(Usage {
                input_tokens: Some(0),
                ..Usage::default()
            }),
            Some(0)
        );
    }
}
