//! Pure retry decisions for the transport's attempt loop.
//! The driver supplies measured facts and performs the effects returned by this model.

use std::time::Duration;

use broca_protocol::{
    error::{ErrorClass, ProviderError},
    provider::AttemptOutcome,
};

use crate::transport::{
    codex_retry::{CodexDecision, CodexFailure, CodexRetryState},
    AttemptCause,
};

pub(crate) const MAX_ATTEMPTS: usize = 6;
pub(crate) const RETRY_WALL_BUDGET: Duration = Duration::from_secs(240);
const BASE_RETRY_DELAY: Duration = Duration::from_secs(2);
const MAX_BACKOFF_DELAY: Duration = Duration::from_secs(30);
pub(crate) const BACKOFF_JITTER: f64 = 0.2;
pub(crate) const MAX_RETRY_AFTER_DELAY: Duration = Duration::from_secs(120);

pub(crate) struct AttemptFacts<'a> {
    pub error: &'a ProviderError,
    pub cause: Option<AttemptCause>,
    pub outcome: AttemptOutcome,
    pub retry_forbidden: bool,
    pub over_https: bool,
    pub attempt_elapsed: Duration,
    pub got_response_bytes: bool,
    pub can_refresh: bool,
}

pub(crate) struct AttemptInputs<J, D> {
    /// Sampled only at a deadline veto, after delay computation and preceding effects.
    pub until_deadline: D,
    /// Drawn only by backoff computation: generic 0.8..=1.2, Codex 0.9..1.1.
    pub jitter: J,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AttemptState {
    /// Zero-based index of the attempt whose failure is being decided.
    attempt_index: usize,
    refreshes: usize,
    rejection_recovered: bool,
    decode_retries: usize,
    escape_used: bool,
    retry_wait_spent: Duration,
    codex: Option<CodexRetryState>,
}

impl AttemptState {
    pub(crate) fn new(use_codex: bool) -> Self {
        Self {
            codex: use_codex.then(CodexRetryState::default),
            ..Self::default()
        }
    }

    /// Success returns the current index, including a rejected-token re-send.
    pub(crate) fn retries_used(&self) -> u32 {
        self.attempt_index as u32
    }

    pub(crate) fn attempt_index(&self) -> usize {
        self.attempt_index
    }

    fn end(&self, reason: EndReason) -> AttemptDecision {
        AttemptDecision::EndStep {
            reason,
            retries_used: self.retries_used(),
            switch_to_https: false,
            note_unrecovered_rejection: false,
        }
    }

    fn resend(
        &mut self,
        facts: &AttemptFacts<'_>,
        delay: Duration,
        server_advised: bool,
        switch_to_https: bool,
        reason: &'static str,
    ) -> AttemptDecision {
        let escape_client =
            !self.escape_used && facts.cause == Some(AttemptCause::TransportTransient);
        self.escape_used |= escape_client;
        self.attempt_index += 1;
        AttemptDecision::Resend {
            delay,
            server_advised,
            escape_client,
            switch_to_https,
            reason,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndReason {
    Authentication,
    AccountChanged,
    UnchangedCredential,
    RetryVeto,
    AttemptLimit,
    WaitingBudget,
    RetryAfterExceedsDeadline,
    CodexTerminal,
    NoServerRetryTime,
    ConnectionAttemptLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AttemptDecision {
    RefreshAndResend,
    SwitchToHttps(PendingHttpsSwitch),
    Resend {
        delay: Duration,
        server_advised: bool,
        escape_client: bool,
        switch_to_https: bool,
        reason: &'static str,
    },
    EndStep {
        reason: EndReason,
        retries_used: u32,
        switch_to_https: bool,
        note_unrecovered_rejection: bool,
    },
}

/// Staged state is committed only after the driver's actual HTTPS-switch result is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingHttpsSwitch {
    state: AttemptState,
    delay: Duration,
    server_advised: bool,
}

pub(crate) fn decide_attempt<J, D>(
    state: &mut AttemptState,
    facts: &AttemptFacts<'_>,
    inputs: &mut AttemptInputs<J, D>,
) -> AttemptDecision
where
    J: FnMut() -> f64,
    D: FnMut() -> Option<Duration>,
{
    let original = *state;
    if !facts.got_response_bytes {
        state.retry_wait_spent += facts.attempt_elapsed;
    }

    // Token recovery precedes even an explicit retry veto in the driver.
    if facts.error.class == ErrorClass::Auth && facts.error.status == Some(401) {
        if !facts.can_refresh || state.rejection_recovered {
            let mut end = state.end(EndReason::Authentication);
            if let AttemptDecision::EndStep {
                note_unrecovered_rejection,
                ..
            } = &mut end
            {
                *note_unrecovered_rejection = facts.can_refresh && state.rejection_recovered;
            }
            return end;
        }
        state.rejection_recovered = true;
        return AttemptDecision::RefreshAndResend;
    }

    if !facts.error.class.step_reissuable()
        || facts.retry_forbidden
        || (facts.cause == Some(AttemptCause::DeterministicDecode) && state.decode_retries >= 1)
    {
        return state.end(EndReason::RetryVeto);
    }

    if let Some(codex) = state.codex.as_mut() {
        if facts.cause == Some(AttemptCause::DeterministicDecode) {
            state.decode_retries += 1;
        }
        let decision = codex.decide_before_switch(
            &CodexFailure {
                error: facts.error,
                outcome: &facts.outcome,
                https: facts.over_https,
            },
            &mut inputs.jitter,
        );
        let (delay, server_advised, switch_to_https, reason) = match decision {
            CodexDecision::SwitchToHttps {
                delay,
                server_advised,
            } => {
                let pending = PendingHttpsSwitch {
                    state: *state,
                    delay,
                    server_advised,
                };
                *state = original;
                return AttemptDecision::SwitchToHttps(pending);
            }
            CodexDecision::Terminal { reason } => {
                return state.end(match reason {
                    "attempt_limit" => EndReason::AttemptLimit,
                    "connection_attempt_limit" => EndReason::ConnectionAttemptLimit,
                    "codex_terminal" => EndReason::CodexTerminal,
                    "no_server_retry_time" => EndReason::NoServerRetryTime,
                    _ => unreachable!("unknown Codex terminal reason: {reason}"),
                });
            }
            CodexDecision::Retry {
                delay,
                server_advised,
                reason,
            } => (delay, server_advised, false, reason),
            CodexDecision::FallBackToHttps {
                delay,
                server_advised,
            } => (delay, server_advised, true, "socket_attempt_limit"),
        };
        if server_advised && (inputs.until_deadline)().is_some_and(|left| delay >= left) {
            return AttemptDecision::EndStep {
                reason: EndReason::RetryAfterExceedsDeadline,
                retries_used: state.retries_used(),
                switch_to_https,
                note_unrecovered_rejection: false,
            };
        }
        return state.resend(facts, delay, server_advised, switch_to_https, reason);
    }

    if state.attempt_index + 1 >= MAX_ATTEMPTS + state.refreshes {
        return state.end(EndReason::AttemptLimit);
    }
    if facts.cause == Some(AttemptCause::DeterministicDecode) {
        state.decode_retries += 1;
    }
    let delay = generic_delay(
        facts.error,
        state.attempt_index - state.refreshes,
        &mut inputs.jitter,
    );
    if facts.error.status == Some(429)
        && facts.error.retry_after_secs.is_some()
        && (inputs.until_deadline)().is_some_and(|left| delay >= left)
    {
        return state.end(EndReason::RetryAfterExceedsDeadline);
    }
    if state.retry_wait_spent + delay > RETRY_WALL_BUDGET {
        return state.end(EndReason::WaitingBudget);
    }
    state.resend(
        facts,
        delay,
        facts.error.retry_after_secs.is_some(),
        false,
        "transient_failure",
    )
}

pub(crate) fn generic_delay(
    error: &ProviderError,
    ordinary_index: usize,
    mut jitter: impl FnMut() -> f64,
) -> Duration {
    if let Some(seconds) = error.retry_after_secs {
        // Preserve the retry schedule's NaN-to-zero conversion; clamp alone preserves NaN.
        let capped = if seconds.is_nan() {
            0.0
        } else {
            seconds.clamp(0.0, MAX_RETRY_AFTER_DELAY.as_secs_f64())
        };
        return Duration::from_secs_f64(capped);
    }
    let doublings = u32::try_from(ordinary_index).unwrap_or(u32::MAX).min(16);
    BASE_RETRY_DELAY
        .saturating_mul(1u32 << doublings)
        .min(MAX_BACKOFF_DELAY)
        .mul_f64(jitter())
}

pub(crate) fn resolve_https_switch(
    state: &mut AttemptState,
    facts: &AttemptFacts<'_>,
    pending: PendingHttpsSwitch,
    switched: bool,
    until_deadline: impl FnOnce() -> Option<Duration>,
) -> AttemptDecision {
    *state = pending.state;
    let decision = state
        .codex
        .as_mut()
        .expect("only Codex requests an HTTPS switch")
        .resolve_switch(switched, pending.delay, pending.server_advised);
    match decision {
        CodexDecision::Terminal { .. } => state.end(EndReason::AttemptLimit),
        CodexDecision::FallBackToHttps {
            delay,
            server_advised,
        } => {
            if server_advised && until_deadline().is_some_and(|left| delay >= left) {
                AttemptDecision::EndStep {
                    reason: EndReason::RetryAfterExceedsDeadline,
                    retries_used: state.retries_used(),
                    switch_to_https: true,
                    note_unrecovered_rejection: false,
                }
            } else {
                state.resend(facts, delay, server_advised, true, "socket_attempt_limit")
            }
        }
        _ => unreachable!("a switch resolves to fallback or terminal"),
    }
}

/// Only a successful rejected-token refresh is charged here. Failed or unchanged refreshes
/// terminate before the driver adds their elapsed time. Pre-send refresh time is part of
/// attempt_elapsed, so it is charged only if that attempt fails without response bytes.
pub(crate) fn note_refresh_elapsed(state: &mut AttemptState, elapsed: Duration) {
    state.retry_wait_spent += elapsed;
}

pub(crate) fn note_waited(state: &mut AttemptState, slept: Duration) {
    state.retry_wait_spent += slept;
}

/// Sanitized refresh facts: the driver compares credentials and accounts without exposing tokens.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RejectedRefreshResult {
    NoCredential,
    AccountChanged,
    UnchangedCredential,
    Refreshed,
}

/// AccountChanged asks the driver to substitute its redacted account-change error, not to
/// return the rejected-token error. Account comparison must precede token equality.
pub(crate) fn decide_rejected_refresh(
    state: &mut AttemptState,
    facts: &AttemptFacts<'_>,
    result: RejectedRefreshResult,
) -> AttemptDecision {
    match result {
        RejectedRefreshResult::NoCredential => state.end(EndReason::Authentication),
        RejectedRefreshResult::AccountChanged => state.end(EndReason::AccountChanged),
        RejectedRefreshResult::UnchangedCredential => state.end(EndReason::UnchangedCredential),
        RejectedRefreshResult::Refreshed => {
            state.refreshes += 1;
            state.resend(facts, Duration::ZERO, false, false, "credential_refresh")
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
