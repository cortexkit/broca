//! The oracle builds expectations from scripted wire facts, never from recorded driver facts.

use super::{
    attempt_test_hooks::{self, Event},
    *,
};
use crate::attempt_policy::{
    self as model, AttemptDecision, AttemptFacts, AttemptInputs, EndReason,
};

pub(super) const FIRST_ATTEMPT: u32 = 7;
pub(super) const SEEDS: [u8; 34] = [
    0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
    0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
    0x73, 0xd2,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Policy {
    Generic,
    CodexHttps,
    CodexWebsocket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Cut,
    Decode,
    Unauthorized,
    Forbidden,
    Permanent,
    Overflow,
    Veto,
    RateLimit,
    Overload,
    Flex,
    Server,
    Complete,
    Connect,
    Disconnect,
}

impl Kind {
    pub(super) fn status(self) -> Option<u16> {
        match self {
            Self::Unauthorized => Some(401),
            Self::Forbidden => Some(403),
            Self::Permanent | Self::Overflow => Some(400),
            Self::RateLimit | Self::Flex => Some(429),
            Self::Veto | Self::Overload | Self::Server => Some(503),
            _ => None,
        }
    }

    pub(super) fn code(self) -> Option<&'static str> {
        match self {
            Self::Overflow => Some("context_length_exceeded"),
            Self::Overload => Some("server_is_overloaded"),
            Self::Flex => Some("flex_unavailable"),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Attempt {
    pub kind: Kind,
    pub elapsed: Duration,
    pub server: Option<&'static str>,
    pub jitter: u8,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Refresh {
    Missing,
    Changed,
    Same,
    Fresh,
}

#[derive(Debug, Clone)]
pub(super) struct Case {
    pub seed: Option<u8>,
    pub policy: Policy,
    pub attempts: Vec<Attempt>,
    pub deadline: Option<Duration>,
    pub refresh: Option<Refresh>,
    pub refresh_elapsed: Duration,
    pub pre_changed_at: Option<usize>,
    pub cancel_before: bool,
    pub gzip: bool,
}

impl Case {
    pub(super) fn seeded(policy: Policy, seed: u8, sticky: bool) -> Self {
        let samples = model::tests::simulation::wire_samples(seed, if sticky { 1 } else { 16 });
        let attempts = samples
            .into_iter()
            .map(|(kind, ms, server, jitter)| Attempt {
                kind: match kind {
                    0 | 1 | 17 | 18 => Kind::Cut,
                    2 => Kind::Decode,
                    3 => Kind::Unauthorized,
                    4 => Kind::Forbidden,
                    5 => Kind::Permanent,
                    6 => Kind::Overflow,
                    7..=11 => Kind::Veto,
                    12 | 13 => Kind::RateLimit,
                    14 => Kind::Overload,
                    15 => Kind::Flex,
                    19 => Kind::Complete,
                    _ => Kind::Server,
                },
                elapsed: Duration::from_millis(u64::from(ms)),
                // Real wire scripts use finite header values. Non-finite facts have separate model
                // conversion tests, since header parsers need not preserve arbitrary f64 payloads.
                server: if kind == 13 {
                    None
                } else {
                    server
                        .filter(|s| s.is_finite() && *s >= 0.0)
                        .map(|s| match s as u64 % 3 {
                            0 => "0",
                            1 => "120",
                            _ => "469",
                        })
                },
                jitter,
            })
            .collect();
        Self {
            seed: Some(seed),
            policy,
            attempts,
            deadline: match seed % 4 {
                0 => Some(Duration::ZERO),
                1 => Some(Duration::from_secs(1)),
                2 => Some(Duration::from_secs(200)),
                _ => None,
            },
            refresh: (seed % 2 == 1).then_some(match seed % 8 {
                1 => Refresh::Missing,
                3 => Refresh::Changed,
                5 => Refresh::Same,
                _ => Refresh::Fresh,
            }),
            refresh_elapsed: Duration::from_millis(321),
            pre_changed_at: None,
            cancel_before: false,
            gzip: false,
        }
    }

    pub(super) fn repeating(policy: Policy, kind: Kind) -> Self {
        Self {
            seed: None,
            policy,
            attempts: vec![Attempt {
                kind,
                elapsed: Duration::ZERO,
                server: None,
                jitter: 100,
            }],
            deadline: None,
            refresh: None,
            refresh_elapsed: Duration::ZERO,
            pre_changed_at: None,
            cancel_before: false,
            gzip: false,
        }
    }

    pub(super) fn attempt(&self, index: usize) -> &Attempt {
        &self.attempts[index % self.attempts.len()]
    }

    pub(super) fn clock(&self) -> RetryClock {
        RetryClock {
            virtual_waits: Some(Arc::new(std::sync::Mutex::new(Vec::new()))),
            virtual_retry_attempts: Some(Arc::new(std::sync::Mutex::new(
                (0..128).map(|i| self.attempt(i).elapsed).collect(),
            ))),
            virtual_wait_overhead: Some(Duration::ZERO),
            ..Default::default()
        }
    }

    pub(super) fn jitters(&self) -> Vec<u8> {
        self.attempts.iter().map(|a| a.jitter).collect()
    }
}

fn decision(trace: &mut Vec<Event>, action: &str, reason: &str) {
    trace.push(Event::Decision {
        action: action.into(),
        reason: reason.into(),
    });
}

fn end(trace: &mut Vec<Event>, retries_used: u32, quiet: bool, failed: bool) -> Vec<Event> {
    trace.push(Event::End {
        retries_used,
        quiet,
        failed,
    });
    std::mem::take(trace)
}

pub(super) fn expected(case: &Case) -> Vec<Event> {
    let mut trace = Vec::new();
    let mut state = model::AttemptState::new(case.policy != Policy::Generic);
    let mut now = Duration::ZERO;
    let mut websocket = case.policy == Policy::CodexWebsocket;
    let mut pending_escape = false;
    for index in 0..128 {
        if case.cancel_before || case.deadline.is_some_and(|deadline| now >= deadline) {
            return end(&mut trace, 0, true, false);
        }
        if case.refresh.is_some() {
            let changed = case.pre_changed_at == Some(index);
            trace.push(Event::Refresh {
                rejected: false,
                credential: changed,
            });
            if changed {
                return end(&mut trace, (index as u32).saturating_sub(1), false, true);
            }
        }
        if pending_escape {
            trace.push(Event::EscapeClient);
        }
        let ordinal = FIRST_ATTEMPT + index as u32;
        trace.push(Event::Send {
            ordinal,
            index: 1,
            websocket,
        });
        if case.gzip && index == 0 {
            trace.push(Event::Send {
                ordinal,
                index: 2,
                websocket: false,
            });
        }
        let attempt = case.attempt(index);
        now += attempt.elapsed;
        let outcome = match attempt.kind {
            Kind::Complete => AttemptOutcome::Complete,
            Kind::Decode => AttemptOutcome::DecodeError,
            Kind::Cut if websocket || case.policy != Policy::CodexWebsocket => {
                AttemptOutcome::ChunkError
            }
            Kind::Cut => AttemptOutcome::EofBeforeCompletion,
            Kind::Connect => AttemptOutcome::ConnectFailed,
            Kind::Disconnect => AttemptOutcome::NoStatus,
            _ => AttemptOutcome::HttpError,
        };
        trace.push(Event::Attempt {
            ordinal,
            outcome: outcome.clone(),
        });
        if attempt.kind == Kind::Complete {
            return end(&mut trace, state.retries_used(), false, false);
        }
        let error = ProviderError {
            class: match attempt.kind {
                Kind::Unauthorized | Kind::Forbidden => ErrorClass::Auth,
                Kind::Permanent => ErrorClass::Permanent,
                Kind::Overflow => ErrorClass::ContextOverflow,
                _ => ErrorClass::Transient,
            },
            status: attempt.kind.status(),
            provider_code: attempt.kind.code().map(str::to_owned),
            // The WebSocket mock's HTTPS error control has no header channel. Its fallback
            // responses omit retry advice and retry-veto headers; socket frames carry both.
            retry_after_secs: if attempt.kind.status().is_none()
                || (case.policy == Policy::CodexWebsocket && !websocket)
            {
                None
            } else {
                attempt.server.map(|s| s.parse().unwrap())
            },
            ..transport_error("scripted failure")
        };
        let facts = AttemptFacts {
            error: &error,
            cause: match attempt.kind {
                Kind::Cut | Kind::Connect | Kind::Disconnect => {
                    Some(AttemptCause::TransportTransient)
                }
                Kind::Decode => Some(AttemptCause::DeterministicDecode),
                _ => None,
            },
            outcome,
            retry_forbidden: attempt.kind == Kind::Veto
                && (case.policy != Policy::CodexWebsocket || websocket),
            over_https: !websocket,
            attempt_elapsed: attempt.elapsed,
            got_response_bytes: !matches!(attempt.kind, Kind::Connect | Kind::Disconnect),
            can_refresh: case.refresh.is_some(),
        };
        let inputs = AttemptInputs {
            until_deadline: case.deadline.map(|d| d.saturating_sub(now)),
            jitter: if case.policy == Policy::Generic {
                0.8 + f64::from(attempt.jitter) / 500.0
            } else {
                0.9 + f64::from(attempt.jitter) / 1000.0
            },
            can_switch_to_https: websocket,
        };
        let mut next = model::decide_attempt(&mut state, &facts, &inputs);
        if next == AttemptDecision::RefreshAndResend {
            let refresh = case.refresh.unwrap();
            trace.push(Event::Refresh {
                rejected: true,
                credential: !matches!(refresh, Refresh::Missing),
            });
            now += case.refresh_elapsed;
            next = model::decide_rejected_refresh(
                &mut state,
                &facts,
                match refresh {
                    Refresh::Missing => model::RejectedRefreshResult::NoCredential,
                    Refresh::Changed => model::RejectedRefreshResult::AccountChanged,
                    Refresh::Same => model::RejectedRefreshResult::UnchangedCredential,
                    Refresh::Fresh => model::RejectedRefreshResult::Refreshed,
                },
            );
            if matches!(next, AttemptDecision::Resend { .. }) {
                model::note_refresh_elapsed(&mut state, case.refresh_elapsed);
            }
        }
        match next {
            AttemptDecision::EndStep {
                reason,
                retries_used,
                switch_to_https,
                ..
            } => {
                if switch_to_https {
                    trace.push(Event::SwitchToHttps);
                }
                let (action, reason) = match reason {
                    EndReason::Authentication => ("not_retryable", "authentication"),
                    EndReason::AccountChanged => ("not_retryable", "account_changed"),
                    EndReason::UnchangedCredential => ("not_retryable", "unchanged_credential"),
                    EndReason::RetryVeto => ("not_retryable", "retry_veto"),
                    EndReason::AttemptLimit => ("attempts_exhausted", "attempt_limit"),
                    EndReason::ConnectionAttemptLimit => {
                        ("attempts_exhausted", "connection_attempt_limit")
                    }
                    EndReason::WaitingBudget => ("wall_exhausted", "waiting_budget"),
                    EndReason::RetryAfterExceedsDeadline => {
                        ("not_retryable", "retry_after_exceeds_deadline")
                    }
                    EndReason::CodexTerminal => ("not_retryable", "codex_terminal"),
                    EndReason::NoServerRetryTime => ("not_retryable", "no_server_retry_time"),
                };
                decision(&mut trace, action, reason);
                return end(&mut trace, retries_used, false, true);
            }
            AttemptDecision::Resend {
                delay,
                escape_client,
                switch_to_https,
                reason,
                ..
            } => {
                if switch_to_https {
                    trace.push(Event::SwitchToHttps);
                    websocket = false;
                }
                decision(
                    &mut trace,
                    if switch_to_https {
                        "switching_to_https"
                    } else {
                        "retrying"
                    },
                    reason,
                );
                pending_escape = escape_client;
                if reason != "credential_refresh" {
                    trace.push(Event::Wait(delay));
                    now += delay;
                    model::note_waited(&mut state, delay);
                }
            }
            AttemptDecision::RefreshAndResend => unreachable!(),
        }
    }
    panic!("attempt model did not terminate: {case:?}");
}

pub(super) struct Normalizer {
    pub kind: Kind,
    pub inner: Box<dyn StreamNormalizer>,
}

impl StreamNormalizer for Normalizer {
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        match self.kind {
            Kind::Decode => Err(NormalizeError::Invalid(
                "scripted deterministic decode failure".into(),
            )),
            Kind::Cut => Ok(Vec::new()),
            _ => self.inner.push(chunk),
        }
    }
    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        if self.kind == Kind::Cut {
            Ok(Vec::new())
        } else {
            self.inner.finish()
        }
    }
    fn last_event_type(&self) -> Option<&str> {
        self.inner.last_event_type()
    }
}

fn auth(token: &str, account: &str) -> AuthHandle {
    AuthHandle::Headers(vec![
        ("authorization".into(), format!("Bearer {token}")),
        ("chatgpt-account-id".into(), account.into()),
        ("session-id".into(), "session".into()),
        ("originator".into(), "codex_cli_rs".into()),
    ])
}

fn credential(
    refresh: Refresh,
    rejected: bool,
) -> Option<crate::credential_refresh::RefreshedCredential> {
    let (token, account) = match refresh {
        Refresh::Missing => return None,
        Refresh::Changed => ("fresh-token", "other-account"),
        Refresh::Same => ("old-token", "account"),
        Refresh::Fresh => ("fresh-token", "account"),
    };
    Some(crate::credential_refresh::RefreshedCredential {
        auth: auth(token, account),
        account_ref: Some(account.into()),
        evidence: CredentialRefresh {
            vault_op: "get_scoped".into(),
            min_ttl_ms: None,
            force_refresh: rejected,
            record_version: Some(2),
            reason: Some(if rejected { "rejected" } else { "expiring" }.into()),
        },
    })
}

struct Refresher {
    result: Refresh,
    elapsed: Duration,
    pre_changed_at: Option<usize>,
    pre_calls: AtomicUsize,
}

impl CredentialRefresher for Refresher {
    fn refresh_expiring(
        &self,
    ) -> crate::credential_refresh::RefreshFuture<'_, crate::credential_refresh::RefreshedCredential>
    {
        let index = self.pre_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if self.pre_changed_at == Some(index) {
                credential(Refresh::Changed, false)
            } else {
                None
            }
        })
    }
    fn refresh_rejected(
        &self,
        _status: u16,
    ) -> crate::credential_refresh::RefreshFuture<'_, crate::credential_refresh::RefreshedCredential>
    {
        Box::pin(async move {
            attempt_test_hooks::advance(self.elapsed);
            credential(self.result, true)
        })
    }
}

fn rebuild(auth: &AuthHandle, request: &PreparedHttpRequest) -> PreparedHttpRequest {
    let mut request = request.clone();
    if let AuthHandle::Headers(headers) = auth {
        for (name, value) in headers {
            request
                .headers
                .retain(|(key, _)| !key.eq_ignore_ascii_case(name));
            request.headers.push((name.clone(), value.clone()));
        }
    }
    request
}

pub(super) fn recovery(case: &Case) -> Option<StepAuthRecovery<'static>> {
    case.refresh.map(|result| {
        let sent_auth = Arc::new(auth("old-token", "account"));
        StepAuthRecovery {
            recovery: AuthRecovery {
                frozen_account_ref: Some("account".into()),
                refresher: Arc::new(Refresher {
                    result,
                    elapsed: case.refresh_elapsed,
                    pre_changed_at: case.pre_changed_at,
                    pre_calls: AtomicUsize::new(0),
                }),
                current: Arc::new(std::sync::RwLock::new(ServedAuth {
                    auth_refreshed: false,
                    revision: 0,
                    auth: sent_auth.clone(),
                    account_ref: Some("account".into()),
                })),
            },
            sent_auth,
            rebuild: &rebuild,
        }
    })
}

pub(super) fn request(url: String, policy: Policy) -> PreparedHttpRequest {
    let AuthHandle::Headers(mut headers) = auth("old-token", "account") else {
        unreachable!()
    };
    headers.push(("content-type".into(), "application/json".into()));
    headers.push(("x-stainless-retry-count".into(), "0".into()));
    let mut request: PreparedHttpRequest = crate::wire::HttpRequest {
        method: crate::wire::HttpMethod::Post,
        url,
        headers,
        body: br#"{"model":"m","input":[],"stream":true}"#.to_vec(),
    }
    .into();
    request.family = match policy {
        Policy::Generic => None,
        Policy::CodexHttps => Some(crate::spec::WireFamilyId::OpenAiResponses),
        Policy::CodexWebsocket => Some(crate::spec::WireFamilyId::OpenAiResponsesChatgpt),
    };
    request
}

pub(super) fn seal() -> StepSeal<'static> {
    StepSeal {
        seal: None,
        step_id: StepId(1),
        cache_ttl_ms: None,
        first_attempt: FIRST_ATTEMPT,
        route: broca_core::refusal_fallback::StepRoute::Source,
    }
}

pub(super) fn assert_trace(case: &Case, actual: &[Event]) {
    assert_eq!(
        actual,
        expected(case),
        "wire/model trace mismatch (seed {:?}): {case:?}",
        case.seed
    );
    let mut in_flight = None;
    for event in actual {
        match event {
            Event::Send {
                ordinal, index: 1, ..
            } => {
                assert!(
                    in_flight.replace(*ordinal).is_none(),
                    "A6: send before prior record"
                );
            }
            Event::Send {
                ordinal, index: 2, ..
            } => assert_eq!(in_flight, Some(*ordinal)),
            Event::Attempt { ordinal, .. } => assert_eq!(in_flight.take(), Some(*ordinal)),
            Event::End { .. } => assert!(in_flight.is_none(), "A6: end before attempt record"),
            _ => {}
        }
    }
}
