use super::*;
use proptest::{
    prelude::*,
    test_runner::{Config, RngAlgorithm, TestRng, TestRunner},
};

#[derive(Debug, Clone)]
struct ScriptedAttempt {
    kind: u8,
    attempt_ms: u32,
    response_bytes: bool,
    refresh: u8,
    refresh_ms: u32,
    server_seconds: Option<f64>,
    jitter: u8,
    pre_send_account_changed: bool,
}

impl ScriptedAttempt {
    fn error(&self) -> ProviderError {
        let (status, code) = match self.kind {
            3 => (Some(401), None),
            4 => (Some(403), None),
            8 => (Some(429), Some("account_limit_exhausted")),
            9 => (Some(429), Some("billing_exhausted")),
            10 => (Some(429), Some("insufficient_credits")),
            11 => (Some(429), Some("service_tier_unavailable")),
            12 | 13 => (Some(429), None),
            14 => (Some(503), Some("server_is_overloaded")),
            15 => (Some(429), Some("flex_unavailable")),
            16 => (Some(503), None),
            _ => (None, None),
        };
        let mut error = error(
            status,
            code,
            if self.kind == 13 {
                None
            } else {
                self.server_seconds
            },
        );
        error.class = match self.kind {
            3 | 4 => ErrorClass::Auth,
            5 => ErrorClass::Permanent,
            6 => ErrorClass::ContextOverflow,
            _ => ErrorClass::Transient,
        };
        error
    }

    fn facts<'a>(&self, error: &'a ProviderError, https: bool) -> AttemptFacts<'a> {
        let mut facts = facts(error);
        facts.outcome = match self.kind {
            0 => AttemptOutcome::ConnectFailed,
            2 => AttemptOutcome::DecodeError,
            12..=16 => AttemptOutcome::HttpError,
            17 => AttemptOutcome::HeaderTimeout,
            18 => AttemptOutcome::NoStatus,
            _ => AttemptOutcome::ChunkError,
        };
        facts.cause = if self.kind == 2 {
            Some(AttemptCause::DeterministicDecode)
        } else if self.kind == 20 {
            None
        } else {
            Some(AttemptCause::TransportTransient)
        };
        facts.retry_forbidden = (7..=11).contains(&self.kind);
        facts.over_https = https;
        facts.attempt_elapsed = Duration::from_millis(u64::from(self.attempt_ms));
        facts.got_response_bytes = self.response_bytes;
        facts.can_refresh = self.refresh != 4;
        facts
    }
}

#[derive(Debug, Clone)]
struct Scenario {
    codex: bool,
    https: bool,
    deadline_ms: Option<u32>,
    attempts: Vec<ScriptedAttempt>,
}

// These are driver-kept outcomes, not failed-attempt decisions. In particular a local wait
// can cross the deadline and end quietly before another attempt starts.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DriverEnd {
    Failed,
    Completed { retries_used: u32 },
    DeadlineReached { retries_used: u32 },
    PreSendAccountChanged { retries_used: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Simulation {
    sent_accounts: Vec<u8>,
    decisions: Vec<AttemptDecision>,
    now: Duration,
    end: DriverEnd,
}

fn simulate(scenario: &Scenario) -> Simulation {
    let mut state = AttemptState::new(scenario.codex);
    let mut simulation = Simulation {
        sent_accounts: Vec::new(),
        decisions: Vec::new(),
        now: Duration::ZERO,
        end: DriverEnd::Failed,
    };
    let deadline = scenario
        .deadline_ms
        .map(|ms| Duration::from_millis(u64::from(ms)));
    let mut https = scenario.https;
    let mut switched = false;
    let mut rejected_resends = 0;
    let mut failed_connects = 0;
    let mut decode_retried = false;
    let mut total_waited = Duration::ZERO;
    let frozen_account = 1;
    let mut account = frozen_account;

    // Even two full Codex stream rounds, each with HTTPS request retries, plus five
    // connects and one token recovery fit well below this guard. A repeating script must
    // terminate by policy, not because the generator ran out of failures.
    for index in 0..128 {
        if deadline.is_some_and(|deadline| simulation.now >= deadline) {
            simulation.end = DriverEnd::DeadlineReached { retries_used: 0 };
            return simulation;
        }
        let script = &scenario.attempts[index % scenario.attempts.len()];
        if script.pre_send_account_changed {
            simulation.end = DriverEnd::PreSendAccountChanged {
                retries_used: state.retries_used().saturating_sub(1),
            };
            return simulation;
        }
        simulation.sent_accounts.push(account);
        assert_eq!(
            account, frozen_account,
            "A3: a refresh must not change the sent account"
        );
        simulation.now += Duration::from_millis(u64::from(script.attempt_ms));
        if script.kind == 19 {
            simulation.end = DriverEnd::Completed {
                retries_used: state.retries_used(),
            };
            assert_eq!(
                state.retries_used() as usize + 1,
                simulation.sent_accounts.len()
            );
            return simulation;
        }

        let error = script.error();
        let facts = script.facts(&error, https);
        let inputs = AttemptInputs {
            until_deadline: deadline.map(|deadline| deadline.saturating_sub(simulation.now)),
            jitter: if scenario.codex {
                0.9 + f64::from(script.jitter) / 1000.0
            } else {
                0.8 + f64::from(script.jitter) / 500.0
            },
            can_switch_to_https: !https && !switched,
        };
        let mut decision = decide_attempt(&mut state, &facts, &inputs);
        simulation.decisions.push(decision.clone());

        if facts.outcome == AttemptOutcome::ConnectFailed {
            failed_connects += 1;
        }
        if scenario.codex {
            assert!(
                failed_connects <= 6,
                "A1: six failed connects end a Codex step"
            );
        } else {
            assert!(
                simulation.sent_accounts.len() <= 7,
                "A1: generic send bound"
            );
        }

        let rejected = error.class == ErrorClass::Auth && error.status == Some(401);
        if rejected && facts.can_refresh && rejected_resends == 0 {
            assert_eq!(decision, AttemptDecision::RefreshAndResend);
        } else if !error.class.step_reissuable()
            || facts.retry_forbidden
            || (facts.cause == Some(AttemptCause::DeterministicDecode) && decode_retried)
        {
            assert!(
                matches!(decision, AttemptDecision::EndStep { .. }),
                "A2: vetoed failure retried"
            );
        }

        if decision == AttemptDecision::RefreshAndResend {
            let refreshed_account = if script.refresh == 1 {
                2
            } else {
                frozen_account
            };
            let result = match script.refresh {
                0 => RejectedRefreshResult::NoCredential,
                1 => RejectedRefreshResult::AccountChanged,
                2 => RejectedRefreshResult::UnchangedCredential,
                3 => RejectedRefreshResult::Refreshed,
                _ => unreachable!("no recovery should have ended before refresh"),
            };
            simulation.now += Duration::from_millis(u64::from(script.refresh_ms));
            decision = decide_rejected_refresh(&mut state, &facts, result);
            simulation.decisions.push(decision.clone());
            if let AttemptDecision::Resend { delay, .. } = decision {
                assert_eq!(delay, Duration::ZERO);
                rejected_resends += 1;
                assert!(
                    rejected_resends <= 1,
                    "A3: at most one rejected-token re-send"
                );
                account = refreshed_account;
                assert_eq!(account, frozen_account, "A3: another account was adopted");
                note_refresh_elapsed(
                    &mut state,
                    Duration::from_millis(u64::from(script.refresh_ms)),
                );
            }
        }

        match decision {
            AttemptDecision::EndStep {
                retries_used,
                switch_to_https,
                reason,
                ..
            } => {
                assert_eq!(retries_used as usize + 1, simulation.sent_accounts.len());
                if switch_to_https {
                    assert!(!https && !switched);
                    assert_eq!(reason, EndReason::RetryAfterExceedsDeadline);
                }
                return simulation;
            }
            AttemptDecision::Resend {
                delay,
                server_advised,
                switch_to_https,
                reason,
                ..
            } => {
                if switch_to_https {
                    assert!(!https && !switched);
                    https = true;
                    switched = true;
                }
                if facts.cause == Some(AttemptCause::DeterministicDecode) {
                    assert!(
                        !decode_retried,
                        "A2: a second deterministic decode was retried"
                    );
                    decode_retried = true;
                }
                if scenario.codex {
                    if server_advised {
                        // Independent of Codex's conversion: valid finite, nonnegative server
                        // seconds must survive without the generic cap or any shortened wait.
                        let raw = error.retry_after_secs.unwrap();
                        assert!(
                            raw <= 0.0 || raw.is_nan() || delay.as_secs_f64() == raw,
                            "A4: server time shortened"
                        );
                        assert!(
                            inputs.until_deadline.is_none_or(|left| delay < left),
                            "A4: wait reaches deadline"
                        );
                    } else {
                        assert!(delay <= seconds(60), "A1: local Codex delay cap");
                    }
                } else if reason != "credential_refresh" {
                    assert!(delay <= seconds(120), "A4: generic delay cap");
                    if error.status == Some(429) && error.retry_after_secs.is_some() {
                        assert!(
                            inputs.until_deadline.is_none_or(|left| delay < left),
                            "A4: 429 deadline veto"
                        );
                    }
                    assert!(
                        total_waited + delay <= seconds(240),
                        "A1: generic waiting budget"
                    );
                }
                // Exact virtual sleeps isolate the schedule. Measured oversleep accounting
                // is pinned separately, because the real budget does not prevent oversleep.
                simulation.now += delay;
                total_waited += delay;
                note_waited(&mut state, delay);
            }
            AttemptDecision::RefreshAndResend => unreachable!("refresh result was not decided"),
        }
    }
    panic!("A1: repeating script did not terminate: {scenario:?}");
}

fn attempt_strategy() -> impl Strategy<Value = ScriptedAttempt> {
    (
        0u8..21,
        0u32..20_000,
        any::<bool>(),
        0u8..5,
        0u32..5_000,
        prop_oneof![
            Just(None),
            (0u16..600).prop_map(|s| Some(f64::from(s))),
            Just(Some(f64::INFINITY)),
            Just(Some(f64::NAN)),
            Just(Some(-1.0))
        ],
        0u8..200,
        0u8..64,
    )
        .prop_map(
            |(
                kind,
                attempt_ms,
                response_bytes,
                refresh,
                refresh_ms,
                server_seconds,
                jitter,
                pre,
            )| ScriptedAttempt {
                kind,
                attempt_ms,
                response_bytes,
                refresh,
                refresh_ms,
                server_seconds,
                jitter,
                pre_send_account_changed: pre == 0,
            },
        )
}

fn scenario_strategy(sticky: bool) -> impl Strategy<Value = Scenario> {
    let attempts = if sticky {
        attempt_strategy()
            .prop_map(|mut attempt| {
                attempt.pre_send_account_changed = false;
                vec![attempt]
            })
            .boxed()
    } else {
        proptest::collection::vec(attempt_strategy(), 1..40).boxed()
    };
    (
        any::<bool>(),
        any::<bool>(),
        proptest::option::of(0u32..900_000),
        attempts,
    )
        .prop_map(|(codex, https, deadline_ms, attempts)| Scenario {
            codex,
            https,
            deadline_ms,
            attempts,
        })
}

fn runner(seed: u8) -> TestRunner {
    TestRunner::new_with_rng(
        Config {
            cases: 256,
            max_shrink_iters: 256,
            failure_persistence: None,
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &[seed; 32]),
    )
}

#[test]
fn sticky_seeded_simulation_checks_a1_through_a4() {
    for seed in [0x19, 0x73, 0xd2] {
        runner(seed)
            .run(&scenario_strategy(true), |scenario| {
                simulate(&scenario);
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn mixed_seeded_simulation_checks_a1_through_a4() {
    for seed in [0x19, 0x73, 0xd2] {
        runner(seed)
            .run(&scenario_strategy(false), |scenario| {
                simulate(&scenario);
                Ok(())
            })
            .unwrap();
    }
}

fn seeded_traces(seed: u8) -> Vec<Simulation> {
    let mut traces = Vec::new();
    // TestRunner's callback is Fn, so use an interior-mutable test-only collector.
    let traces_ref = std::cell::RefCell::new(&mut traces);
    for sticky in [true, false] {
        runner(seed)
            .run(&scenario_strategy(sticky), |scenario| {
                traces_ref.borrow_mut().push(simulate(&scenario));
                Ok(())
            })
            .unwrap();
    }
    traces
}

#[test]
fn same_seed_reproduces_simulated_decisions_a5() {
    let first = seeded_traces(0x73);
    assert_eq!(first, seeded_traces(0x73));
    assert_ne!(first, seeded_traces(0x19));
}

#[test]
fn fixed_seeds_cover_all_decision_variants_and_reasons() {
    let traces: Vec<_> = [0x19, 0x73, 0xd2]
        .into_iter()
        .flat_map(seeded_traces)
        .collect();
    let decisions: Vec<_> = traces.iter().flat_map(|trace| &trace.decisions).collect();
    assert!(decisions.contains(&&AttemptDecision::RefreshAndResend));
    for expected in [
        "credential_refresh",
        "transient_failure",
        "request_retry",
        "connection_failed",
        "socket_attempt_limit",
    ] {
        assert!(
            decisions.iter().any(|decision| matches!(decision,
            AttemptDecision::Resend { reason, .. } if *reason == expected)),
            "fixed seeds missed re-send reason {expected}"
        );
    }
    for expected in [
        EndReason::Authentication,
        EndReason::AccountChanged,
        EndReason::UnchangedCredential,
        EndReason::RetryVeto,
        EndReason::AttemptLimit,
        EndReason::WaitingBudget,
        EndReason::RetryAfterExceedsDeadline,
        EndReason::CodexTerminal,
        EndReason::NoServerRetryTime,
        EndReason::ConnectionAttemptLimit,
    ] {
        assert!(
            decisions.iter().any(|decision| matches!(decision,
            AttemptDecision::EndStep { reason, .. } if *reason == expected)),
            "fixed seeds missed end reason {expected:?}"
        );
    }
}

fn script(kind: u8) -> ScriptedAttempt {
    ScriptedAttempt {
        kind,
        attempt_ms: 0,
        response_bytes: false,
        refresh: 3,
        refresh_ms: 0,
        server_seconds: None,
        jitter: 100,
        pre_send_account_changed: false,
    }
}

#[test]
fn quiet_deadline_after_generic_non_429_wait_starts_no_next_send() {
    let mut attempt = script(16);
    attempt.server_seconds = Some(469.0);
    let simulation = simulate(&Scenario {
        codex: false,
        https: true,
        deadline_ms: Some(1000),
        attempts: vec![attempt],
    });
    assert_eq!(
        simulation.end,
        DriverEnd::DeadlineReached { retries_used: 0 }
    );
    assert_eq!(simulation.sent_accounts, vec![1]);
    assert_eq!(simulation.now, seconds(120));
}

#[test]
fn completed_and_pre_send_account_change_keep_driver_retry_numbers() {
    let simulation = simulate(&Scenario {
        codex: false,
        https: true,
        deadline_ms: None,
        attempts: vec![script(1), script(19)],
    });
    assert_eq!(simulation.end, DriverEnd::Completed { retries_used: 1 });
    let mut changed = script(1);
    changed.pre_send_account_changed = true;
    let simulation = simulate(&Scenario {
        codex: false,
        https: true,
        deadline_ms: None,
        attempts: vec![script(1), script(1), changed],
    });
    assert_eq!(
        simulation.end,
        DriverEnd::PreSendAccountChanged { retries_used: 1 }
    );
    assert_eq!(simulation.sent_accounts, vec![1, 1]);
}

#[test]
fn mixed_connect_failures_survive_refresh_and_https_fallback() {
    let mut attempts = vec![script(0), script(3)];
    attempts.extend((0..6).map(|_| script(1)));
    attempts.extend((0..5).map(|_| script(0)));
    let simulation = simulate(&Scenario {
        codex: true,
        https: false,
        deadline_ms: None,
        attempts,
    });
    assert!(simulation.decisions.iter().any(|decision| matches!(
        decision,
        AttemptDecision::Resend {
            switch_to_https: true,
            ..
        }
    )));
    assert!(matches!(
        simulation.decisions.last(),
        Some(AttemptDecision::EndStep {
            reason: EndReason::ConnectionAttemptLimit,
            retries_used: 12,
            ..
        })
    ));
}
