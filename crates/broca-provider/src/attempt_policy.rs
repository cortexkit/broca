//! Pure retry decisions, not yet used by the transport.
//! A later trace test will verify this model against the unchanged driver before wiring it in.

use std::time::Duration;

use broca_protocol::{
    error::{ErrorClass, ProviderError},
    provider::AttemptOutcome,
};

use crate::transport::{
    codex_retry::{CodexDecision, CodexFailure, CodexRetryState},
    AttemptCause,
};

const MAX_ATTEMPTS: usize = 6;
const RETRY_WALL_BUDGET: Duration = Duration::from_secs(240);

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

pub(crate) struct AttemptInputs {
    pub until_deadline: Option<Duration>,
    /// The factor already drawn by the driver: generic 0.8..=1.2, Codex 0.9..1.1.
    pub jitter: f64,
    /// Whether the driver's lazy switch callback would successfully switch the session.
    pub can_switch_to_https: bool,
}

#[derive(Debug, Default)]
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

pub(crate) fn decide_attempt(
    state: &mut AttemptState,
    facts: &AttemptFacts<'_>,
    inputs: &AttemptInputs,
) -> AttemptDecision {
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
        let decision = codex.decide_with_jitter(
            &CodexFailure {
                error: facts.error,
                outcome: &facts.outcome,
                https: facts.over_https,
            },
            || inputs.can_switch_to_https,
            || inputs.jitter,
        );
        let (delay, server_advised, switch_to_https, reason) = match decision {
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
        if server_advised && inputs.until_deadline.is_some_and(|left| delay >= left) {
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
        inputs.jitter,
    );
    if facts.error.status == Some(429)
        && facts.error.retry_after_secs.is_some()
        && inputs.until_deadline.is_some_and(|left| delay >= left)
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

fn generic_delay(error: &ProviderError, ordinary_index: usize, jitter: f64) -> Duration {
    if let Some(seconds) = error.retry_after_secs {
        // The transport's max/min turns NaN into zero; clamp alone would preserve NaN.
        let capped = if seconds.is_nan() {
            0.0
        } else {
            seconds.clamp(0.0, 120.0)
        };
        return Duration::from_secs_f64(capped);
    }
    let doublings = u32::try_from(ordinary_index).unwrap_or(u32::MAX).min(16);
    Duration::from_secs(2)
        .saturating_mul(1u32 << doublings)
        .min(Duration::from_secs(30))
        .mul_f64(jitter)
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
mod tests;
