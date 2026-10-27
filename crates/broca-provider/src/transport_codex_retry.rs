// Retry rules reproduced from OpenAI Codex, revision af91f6ef2c (Apache-2.0).
// Copyright 2025 OpenAI. See THIRD-PARTY-NOTICES.md and LICENSES/Apache-2.0.txt.

//! The retry decision for the two OpenAI Responses families, `openai_responses` (API key) and
//! `openai_responses_chatgpt` (the ChatGPT/Codex backend). Codex runs both through one retry
//! path, and broca reproduces that path for them. Every other family keeps the schedule above
//! `MAX_ATTEMPTS` in the transport.
//!
//! Codex retries in two layers, and so does this module:
//!
//! - The request layer (`codex-client/src/retry.rs:23-41,84-118`, configured at
//!   `model-provider-info/src/lib.rs:454-460`) wraps one HTTPS request. It re-sends a connect
//!   failure, a timeout, a network error or a 5xx answer up to `REQUEST_MAX_RETRIES` times,
//!   never a 429. It waits until the server's `Retry-After` deadline, or else
//!   [`request_backoff`]. Codex's socket connect has no request layer, so this one applies
//!   only to attempts that went out over HTTPS.
//! - The stream layer (`core/src/responses_retry.rs:57-176`) decides what happens once the
//!   request layer gives up, or after a failure the request layer does not handle (a
//!   mid-stream cut, a 429, a socket failure). Each stream-layer retry starts a new request
//!   layer, which is why the request count resets there.
//!
//! The stream layer, in Codex's order:
//!
//! 1. Whether the error retries at all, and its delay (`protocol/src/error.rs:406-460`). A 429
//!    that Codex does not name as terminal (its `RetryLimit`) and a `server_is_overloaded`
//!    answer retry only when the server gave a retry time. Every other retryable error waits
//!    for the server's time when there is one, and otherwise [`stream_backoff`].
//! 2. A failure to connect waits 5 s, doubling to a 60 s cap, with no limit on the number of
//!    retries. It ignores any server time and does not count against the stream retries
//!    (`core/src/responses_retry.rs:93-118`; the `unbounded_connection_retries` feature is
//!    Stable and on by default, `features/src/lib.rs:1467-1472`).
//! 3. Once `STREAM_MAX_RETRIES` retries are spent, a step still on the WebSocket switches the
//!    rest of the run to HTTPS, waits out the server's deadline if one was given, and starts a
//!    fresh retry count (`core/src/responses_retry.rs:120-139`).
//! 4. Otherwise it retries while retries remain, sleeping until exactly the server's deadline,
//!    or for the backoff (`core/src/responses_retry.rs:141-167`).
//! 5. Otherwise the error ends the step.
//!
//! The server's time is never capped and there is no total waiting budget. The caller's step
//! deadline still applies: the transport ends the step when a server-advised wait would pass it.

use super::*;

/// Codex `DEFAULT_STREAM_MAX_RETRIES` (`model-provider-info/src/lib.rs:67`).
pub(super) const STREAM_MAX_RETRIES: u64 = 5;
/// Codex `DEFAULT_REQUEST_MAX_RETRIES` (`model-provider-info/src/lib.rs:68`).
pub(super) const REQUEST_MAX_RETRIES: u64 = 4;
/// The request layer's `base_delay` (`model-provider-info/src/lib.rs:456`).
const REQUEST_BASE_DELAY: Duration = Duration::from_millis(200);
/// `INITIAL_DELAY_MS` and `BACKOFF_FACTOR` of Codex's shared backoff
/// (`async-utils/src/backoff.rs:7-8`).
const STREAM_INITIAL_DELAY_MS: u64 = 200;
const STREAM_BACKOFF_FACTOR: f64 = 2.0;
/// `INITIAL_CONNECTION_RETRY_DELAY` and `MAX_CONNECTION_RETRY_DELAY`
/// (`core/src/responses_retry.rs:23-24`).
pub(super) const INITIAL_CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(5);
pub(super) const MAX_CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(60);

/// Whether `family` takes this module's retry decision.
pub(super) fn applies(family: Option<crate::spec::WireFamilyId>) -> bool {
    matches!(
        family,
        Some(
            crate::spec::WireFamilyId::OpenAiResponses
                | crate::spec::WireFamilyId::OpenAiResponsesChatgpt
        )
    )
}

/// The jitter factor both Codex backoffs draw: uniform in `0.9..1.1`
/// (`async-utils/src/backoff.rs:15`, `codex-client/src/retry.rs:50`).
fn jitter() -> f64 {
    rand::rng().random_range(0.9..1.1)
}

/// Codex's stream-layer backoff for retry `retry_count` (the first retry is 1):
/// 200 ms doubling per retry, times `jitter`, with no cap (`async-utils/src/backoff.rs:12-17`).
/// Retries 0 and 1 both use the initial delay.
pub(super) fn stream_backoff(retry_count: u64, jitter: f64) -> Duration {
    let exp = STREAM_BACKOFF_FACTOR.powi(retry_count.saturating_sub(1) as i32);
    let base = (STREAM_INITIAL_DELAY_MS as f64 * exp) as u64;
    Duration::from_millis((base as f64 * jitter) as u64)
}

/// Codex's request-layer backoff before request retry `attempt` (the first retry is 1): the
/// 200 ms base doubling per retry, times `jitter`, with no cap (`codex-client/src/retry.rs:43-52`).
pub(super) fn request_backoff(attempt: u64, jitter: f64) -> Duration {
    if attempt == 0 {
        return REQUEST_BASE_DELAY;
    }
    let exp = 2u64.saturating_pow(attempt as u32 - 1);
    let millis = REQUEST_BASE_DELAY.as_millis() as u64;
    let raw = millis.saturating_mul(exp);
    Duration::from_millis((raw as f64 * jitter) as u64)
}

/// The server's retry time for these families, read from the `Retry-After` header only.
pub(super) fn retry_after_secs(headers: &reqwest::header::HeaderMap) -> Option<f64> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    crate::codex_retry_after::retry_after_value(value, SystemTime::now())
}

/// What one failed attempt tells the decision. Everything else about the failure (its class,
/// `x-should-retry`, a repeated decode failure, the response deadline) has already been through
/// the transport's vetoes, which end the step before this decision is asked.
pub(super) struct CodexFailure<'a> {
    pub(super) error: &'a ProviderError,
    pub(super) outcome: &'a AttemptOutcome,
    /// The attempt's request went out over HTTPS, the only transport Codex's request layer
    /// wraps.
    pub(super) https: bool,
}

/// What to do after a failed attempt.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum CodexDecision {
    /// Send the same request again after `delay`. `server_advised` says `delay` is the time
    /// the server asked for rather than a local backoff.
    Retry {
        delay: Duration,
        server_advised: bool,
        reason: &'static str,
    },
    /// The WebSocket retries are spent: the run has just switched to HTTPS for good, and sends
    /// the same request there after `delay`, which is the server's remaining time or zero.
    FallBackToHttps {
        delay: Duration,
        server_advised: bool,
    },
    /// End the step with this failure.
    Terminal { reason: &'static str },
}

/// How Codex treats an error once the request layer has given up on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorKind {
    /// Never retried.
    Terminal,
    /// Retried only when the server gave a retry time: Codex's `RetryLimit` and
    /// `ServerOverloaded` (`protocol/src/error.rs:438-440`).
    ServerTimeOnly,
    /// Retried after the server's time, or else the stream backoff
    /// (`protocol/src/error.rs:441-456`).
    Backoff,
}

/// The 429s Codex ends at once (`codex-api/src/api_bridge.rs:185-238`): `flex_unavailable`,
/// `usage_limit_reached`, `usage_not_included`, and the quota and credit codes. broca's own
/// labels for some of them (`account_limit_exhausted`, `billing_exhausted`,
/// `service_tier_unavailable`) stand in for the code they replaced.
fn terminal_429_code(code: &str) -> bool {
    matches!(
        code,
        "flex_unavailable"
            | "usage_limit_reached"
            | "usage_not_included"
            | "insufficient_quota"
            | "credit_balance_exhausted"
            | "organization_spend_limit_exceeded"
            | "project_spend_limit_exceeded"
            | "organization_usage_limit_exceeded"
            | broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED
            | broca_protocol::error::BILLING_EXHAUSTED
            | crate::service_tier::SERVICE_TIER_UNAVAILABLE
    )
}

fn error_kind(error: &ProviderError) -> ErrorKind {
    let code = error.provider_code.as_deref();
    match error.status {
        // Every 429 Codex does not name as terminal becomes `RetryLimit`
        // (`codex-api/src/api_bridge.rs:241-244`), wrapped socket refusals included
        // (`codex-api/src/endpoint/responses_websocket.rs:689-700`).
        Some(429) if code.is_some_and(terminal_429_code) => ErrorKind::Terminal,
        Some(429) => ErrorKind::ServerTimeOnly,
        // A 503 naming `server_is_overloaded` (`codex-api/src/api_bridge.rs:105-110`) and a
        // streamed `response.failed` with that code (`codex-api/src/sse/responses_error.rs:87-89`).
        Some(503) | None if code == Some("server_is_overloaded") => ErrorKind::ServerTimeOnly,
        // A streamed `flex_unavailable` (`codex-api/src/sse/responses.rs:349-352`,
        // `codex-api/src/sse/responses_error.rs:32-34`).
        None if code == Some("flex_unavailable") => ErrorKind::Terminal,
        _ => ErrorKind::Backoff,
    }
}

/// The failures Codex's request layer re-sends: connect, timeout and network errors before an
/// answer, and 5xx answers (`codex-client/src/retry.rs:23-41`; `retry_429: false`,
/// `retry_5xx: true`, `retry_transport: true` at `model-provider-info/src/lib.rs:454-460`).
fn request_layer_retries(failure: &CodexFailure<'_>) -> bool {
    match failure.outcome {
        AttemptOutcome::ConnectFailed
        | AttemptOutcome::HeaderTimeout
        | AttemptOutcome::NoStatus => true,
        AttemptOutcome::HttpError => failure
            .error
            .status
            .is_some_and(|status| (500..600).contains(&status)),
        _ => false,
    }
}

/// The retry counters for one step, Codex's `ResponsesStreamRetryState` plus the request
/// layer's attempt counter.
#[derive(Debug)]
pub(super) struct CodexRetryState {
    retries: u64,
    request_retries: u64,
    connection_retry_delay: Duration,
}

impl Default for CodexRetryState {
    fn default() -> Self {
        Self {
            retries: 0,
            request_retries: 0,
            connection_retry_delay: INITIAL_CONNECTION_RETRY_DELAY,
        }
    }
}

impl CodexRetryState {
    /// Decide what follows `failure`. `try_switch_to_https` is asked only when the stream
    /// retries are spent; it switches the run to HTTPS and answers whether this call made the
    /// switch, as Codex's `try_switch_fallback_transport` does (`core/src/client.rs:2454-2464`).
    pub(super) fn decide(
        &mut self,
        failure: &CodexFailure<'_>,
        try_switch_to_https: impl FnOnce() -> bool,
    ) -> CodexDecision {
        self.decide_with_jitter(failure, try_switch_to_https, jitter)
    }

    pub(super) fn decide_with_jitter(
        &mut self,
        failure: &CodexFailure<'_>,
        try_switch_to_https: impl FnOnce() -> bool,
        mut jitter: impl FnMut() -> f64,
    ) -> CodexDecision {
        let server = failure
            .error
            .retry_after_secs
            .and_then(|seconds| Duration::try_from_secs_f64(seconds.max(0.0)).ok());

        // The request layer (`codex-client/src/retry.rs:93-117`).
        if failure.https
            && request_layer_retries(failure)
            && self.request_retries < REQUEST_MAX_RETRIES
        {
            self.request_retries += 1;
            return CodexDecision::Retry {
                delay: server.unwrap_or_else(|| request_backoff(self.request_retries, jitter())),
                server_advised: server.is_some(),
                reason: "request_retry",
            };
        }
        // The request layer gave up or never applied; the next send starts a new one.
        self.request_retries = 0;

        // `core/src/responses_retry.rs:87-90`, with the delay rule of `protocol/src/error.rs`.
        let retry_count = self.retries.saturating_add(1);
        let (delay, server_advised) = match error_kind(failure.error) {
            ErrorKind::Terminal => {
                return CodexDecision::Terminal {
                    reason: "codex_terminal",
                }
            }
            ErrorKind::ServerTimeOnly => match server {
                Some(delay) => (delay, true),
                None => {
                    return CodexDecision::Terminal {
                        reason: "no_server_retry_time",
                    }
                }
            },
            ErrorKind::Backoff => match server {
                Some(delay) => (delay, true),
                None => (stream_backoff(retry_count, jitter()), false),
            },
        };

        // `core/src/responses_retry.rs:93-118`.
        if *failure.outcome == AttemptOutcome::ConnectFailed {
            let delay = self.connection_retry_delay;
            self.connection_retry_delay = delay.saturating_mul(2).min(MAX_CONNECTION_RETRY_DELAY);
            return CodexDecision::Retry {
                delay,
                server_advised: false,
                reason: "connection_failed",
            };
        }

        // `core/src/responses_retry.rs:120-139`.
        if self.retries >= STREAM_MAX_RETRIES && try_switch_to_https() {
            self.retries = 0;
            return CodexDecision::FallBackToHttps {
                delay: server.unwrap_or_default(),
                server_advised: server.is_some(),
            };
        }

        // `core/src/responses_retry.rs:141-167`.
        if self.retries < STREAM_MAX_RETRIES {
            self.retries = retry_count;
            return CodexDecision::Retry {
                delay,
                server_advised,
                reason: "transient_failure",
            };
        }

        CodexDecision::Terminal {
            reason: "attempt_limit",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error(
        status: Option<u16>,
        code: Option<&str>,
        retry_after_secs: Option<f64>,
    ) -> ProviderError {
        ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Transient,
            message: "failed".into(),
            status,
            retry_after_secs,
            provider_code: code.map(str::to_owned),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        }
    }

    fn decide(
        state: &mut CodexRetryState,
        error: &ProviderError,
        outcome: AttemptOutcome,
        https: bool,
        switch: bool,
    ) -> CodexDecision {
        state.decide_with_jitter(
            &CodexFailure {
                error,
                outcome: &outcome,
                https,
            },
            || switch,
            || 1.0,
        )
    }

    #[test]
    fn backoffs_match_codex_with_their_jitter_bounds() {
        let stream: Vec<_> = (1..=5).map(|n| stream_backoff(n, 1.0)).collect();
        assert_eq!(
            stream,
            [200, 400, 800, 1600, 3200].map(Duration::from_millis)
        );
        assert_eq!(stream_backoff(0, 1.0), Duration::from_millis(200));
        assert_eq!(stream_backoff(3, 0.9), Duration::from_millis(720));
        assert_eq!(stream_backoff(3, 1.0999), Duration::from_millis(879));
        let request: Vec<_> = (0..=4).map(|n| request_backoff(n, 1.0)).collect();
        assert_eq!(
            request,
            [200, 200, 400, 800, 1600].map(Duration::from_millis)
        );
        for _ in 0..100 {
            let factor = jitter();
            assert!((0.9..1.1).contains(&factor));
        }
    }

    #[test]
    fn a_429_retries_only_with_a_server_time_and_never_caps_it() {
        let mut state = CodexRetryState::default();
        let plain = error(Some(429), Some("turn_rate_limit_exceeded"), None);
        assert_eq!(
            decide(&mut state, &plain, AttemptOutcome::HttpError, true, false),
            CodexDecision::Terminal {
                reason: "no_server_retry_time"
            }
        );
        let timed = error(Some(429), Some("turn_rate_limit_exceeded"), Some(469.0));
        assert_eq!(
            decide(&mut state, &timed, AttemptOutcome::HttpError, true, false),
            CodexDecision::Retry {
                delay: Duration::from_secs(469),
                server_advised: true,
                reason: "transient_failure"
            }
        );
    }

    #[test]
    fn codex_terminal_429_codes_end_even_with_a_server_time() {
        for code in [
            "flex_unavailable",
            "usage_not_included",
            "organization_spend_limit_exceeded",
            "project_spend_limit_exceeded",
            "organization_usage_limit_exceeded",
            broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED,
            broca_protocol::error::BILLING_EXHAUSTED,
        ] {
            let mut state = CodexRetryState::default();
            let refused = error(Some(429), Some(code), Some(3.0));
            assert_eq!(
                decide(&mut state, &refused, AttemptOutcome::HttpError, true, false),
                CodexDecision::Terminal {
                    reason: "codex_terminal"
                },
                "{code}"
            );
        }
    }

    #[test]
    fn server_overloaded_needs_a_server_time_other_errors_back_off() {
        let mut state = CodexRetryState::default();
        let overloaded = error(None, Some("server_is_overloaded"), None);
        assert_eq!(
            decide(
                &mut state,
                &overloaded,
                AttemptOutcome::ProviderError,
                false,
                false
            ),
            CodexDecision::Terminal {
                reason: "no_server_retry_time"
            }
        );
        let cut = error(None, None, None);
        assert_eq!(
            decide(&mut state, &cut, AttemptOutcome::ChunkError, false, false),
            CodexDecision::Retry {
                delay: Duration::from_millis(200),
                server_advised: false,
                reason: "transient_failure"
            }
        );
    }

    #[test]
    fn request_layer_retries_https_5xx_four_times_before_the_stream_layer() {
        let mut state = CodexRetryState::default();
        let unavailable = error(Some(500), None, None);
        for (n, millis) in [200, 400, 800, 1600].into_iter().enumerate() {
            assert_eq!(
                decide(
                    &mut state,
                    &unavailable,
                    AttemptOutcome::HttpError,
                    true,
                    false
                ),
                CodexDecision::Retry {
                    delay: Duration::from_millis(millis),
                    server_advised: false,
                    reason: "request_retry"
                },
                "request retry {}",
                n + 1
            );
        }
        // The fifth failure reaches the stream layer as its first retry, which starts a new
        // request layer.
        assert_eq!(
            decide(
                &mut state,
                &unavailable,
                AttemptOutcome::HttpError,
                true,
                false
            ),
            CodexDecision::Retry {
                delay: Duration::from_millis(200),
                server_advised: false,
                reason: "transient_failure"
            }
        );
        assert!(matches!(
            decide(
                &mut state,
                &unavailable,
                AttemptOutcome::HttpError,
                true,
                false
            ),
            CodexDecision::Retry {
                reason: "request_retry",
                ..
            }
        ));
        // A 429 and a socket 5xx skip the request layer.
        let mut state = CodexRetryState::default();
        let socket_500 = error(Some(500), None, None);
        assert!(matches!(
            decide(
                &mut state,
                &socket_500,
                AttemptOutcome::HttpError,
                false,
                false
            ),
            CodexDecision::Retry {
                reason: "transient_failure",
                ..
            }
        ));
    }

    #[test]
    fn five_stream_retries_then_one_fallback_with_a_fresh_count() {
        let mut state = CodexRetryState::default();
        let limited = error(Some(429), None, Some(7.0));
        for _ in 0..STREAM_MAX_RETRIES {
            assert!(matches!(
                decide(&mut state, &limited, AttemptOutcome::HttpError, false, true),
                CodexDecision::Retry { .. }
            ));
        }
        assert_eq!(
            decide(&mut state, &limited, AttemptOutcome::HttpError, false, true),
            CodexDecision::FallBackToHttps {
                delay: Duration::from_secs(7),
                server_advised: true
            }
        );
        for _ in 0..STREAM_MAX_RETRIES {
            assert!(matches!(
                decide(&mut state, &limited, AttemptOutcome::HttpError, true, false),
                CodexDecision::Retry { .. }
            ));
        }
        // Already on HTTPS: the switch is not made again, so the step ends.
        assert_eq!(
            decide(&mut state, &limited, AttemptOutcome::HttpError, true, false),
            CodexDecision::Terminal {
                reason: "attempt_limit"
            }
        );
    }

    #[test]
    fn connect_failures_retry_without_limit_from_five_to_sixty_seconds() {
        let mut state = CodexRetryState::default();
        let refused = error(None, None, Some(1.0));
        let mut delays = Vec::new();
        for _ in 0..40 {
            if let CodexDecision::Retry {
                delay,
                reason: "connection_failed",
                server_advised,
            } = decide(
                &mut state,
                &refused,
                AttemptOutcome::ConnectFailed,
                true,
                true,
            ) {
                assert!(!server_advised, "a connect retry ignores the server's time");
                delays.push(delay);
            }
        }
        // Every fifth send reaches the stream layer; the four before it are request retries.
        assert_eq!(delays.len(), 8);
        assert_eq!(
            delays,
            [5, 10, 20, 40, 60, 60, 60, 60].map(Duration::from_secs)
        );
        assert_eq!(state.retries, 0, "connect retries do not count");
    }
}
