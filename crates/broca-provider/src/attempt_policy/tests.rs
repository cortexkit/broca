use super::*;

fn seconds(value: u64) -> Duration {
    Duration::from_secs(value)
}

fn error(status: Option<u16>, code: Option<&str>, retry_after_secs: Option<f64>) -> ProviderError {
    ProviderError {
        class: ErrorClass::Transient,
        message: "scripted failure".into(),
        status,
        provider_code: code.map(str::to_owned),
        retry_after_secs,
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

fn facts(error: &ProviderError) -> AttemptFacts<'_> {
    AttemptFacts {
        error,
        cause: Some(AttemptCause::TransportTransient),
        outcome: AttemptOutcome::ChunkError,
        retry_forbidden: false,
        over_https: false,
        attempt_elapsed: Duration::ZERO,
        got_response_bytes: true,
        can_refresh: true,
    }
}

fn inputs() -> AttemptInputs {
    AttemptInputs {
        until_deadline: None,
        jitter: 1.0,
        can_switch_to_https: false,
    }
}

fn end(reason: EndReason, retries_used: u32) -> AttemptDecision {
    AttemptDecision::EndStep {
        reason,
        retries_used,
        switch_to_https: false,
        note_unrecovered_rejection: false,
    }
}

fn resend(delay: Duration, reason: &'static str, escape_client: bool) -> AttemptDecision {
    AttemptDecision::Resend {
        delay,
        reason,
        escape_client,
        server_advised: false,
        switch_to_https: false,
    }
}

fn rejected() -> ProviderError {
    let mut error = error(Some(401), None, None);
    error.class = ErrorClass::Auth;
    error
}

#[test]
fn refresh_and_resend_precedes_retry_veto() {
    let error = rejected();
    let mut facts = facts(&error);
    facts.retry_forbidden = true;
    let mut state = AttemptState::default();
    assert_eq!(
        decide_attempt(&mut state, &facts, &inputs()),
        AttemptDecision::RefreshAndResend
    );
    assert_eq!(state.retries_used(), 0);
    assert_eq!(
        decide_rejected_refresh(&mut state, &facts, RejectedRefreshResult::Refreshed),
        resend(Duration::ZERO, "credential_refresh", true)
    );
    assert_eq!(state.retries_used(), 1);
}

#[test]
fn authentication_without_recovery_precedes_retry_veto() {
    let error = rejected();
    let mut facts = facts(&error);
    facts.can_refresh = false;
    facts.retry_forbidden = true;
    assert_eq!(
        decide_attempt(&mut AttemptState::default(), &facts, &inputs()),
        end(EndReason::Authentication, 0)
    );
}

#[test]
fn authentication_second_rejection_notes_unrecovered_rejection() {
    for use_codex in [false, true] {
        let mut state = AttemptState::new(use_codex);
        let error = rejected();
        let facts = facts(&error);
        assert_eq!(
            decide_attempt(&mut state, &facts, &inputs()),
            AttemptDecision::RefreshAndResend
        );
        decide_rejected_refresh(&mut state, &facts, RejectedRefreshResult::Refreshed);
        assert_eq!(
            decide_attempt(&mut state, &facts, &inputs()),
            AttemptDecision::EndStep {
                reason: EndReason::Authentication,
                retries_used: 1,
                switch_to_https: false,
                note_unrecovered_rejection: true,
            }
        );
    }
}

fn refresh_ends(result: RejectedRefreshResult, reason: EndReason) {
    let mut state = AttemptState::default();
    let error = rejected();
    let facts = facts(&error);
    assert_eq!(
        decide_attempt(&mut state, &facts, &inputs()),
        AttemptDecision::RefreshAndResend
    );
    assert_eq!(
        decide_rejected_refresh(&mut state, &facts, result),
        end(reason, 0)
    );
    assert_eq!(state.refreshes, 0);
}

#[test]
fn no_refreshed_credential_ends_as_authentication() {
    refresh_ends(
        RejectedRefreshResult::NoCredential,
        EndReason::Authentication,
    );
}

#[test]
fn account_changed_ends_with_substitute_error_reason() {
    refresh_ends(
        RejectedRefreshResult::AccountChanged,
        EndReason::AccountChanged,
    );
}

#[test]
fn unchanged_credential_ends_without_resending() {
    refresh_ends(
        RejectedRefreshResult::UnchangedCredential,
        EndReason::UnchangedCredential,
    );
}

#[test]
fn retry_veto_covers_classes_flags_and_second_decode_before_limits() {
    for use_codex in [false, true] {
        for class in [
            ErrorClass::Permanent,
            ErrorClass::ContextOverflow,
            ErrorClass::Auth,
        ] {
            let mut error = error(Some(403), None, None);
            error.class = class;
            assert_eq!(
                decide_attempt(&mut AttemptState::new(use_codex), &facts(&error), &inputs()),
                end(EndReason::RetryVeto, 0)
            );
        }
        // The error source sets retry_forbidden; the retry policy does not infer it
        // from these error codes.
        for code in [
            None,
            Some("account_limit_exhausted"),
            Some("billing_exhausted"),
            Some("insufficient_credits"),
            Some("service_tier_unavailable"),
        ] {
            let error = error(None, code, None);
            let mut facts = facts(&error);
            facts.retry_forbidden = true;
            assert_eq!(
                decide_attempt(&mut AttemptState::new(use_codex), &facts, &inputs()),
                end(EndReason::RetryVeto, 0)
            );
        }
        let error = error(None, None, None);
        let mut facts = facts(&error);
        facts.cause = Some(AttemptCause::DeterministicDecode);
        facts.outcome = AttemptOutcome::DecodeError;
        let mut state = AttemptState::new(use_codex);
        assert!(matches!(
            decide_attempt(&mut state, &facts, &inputs()),
            AttemptDecision::Resend { .. }
        ));
        state.attempt_index = 5;
        assert_eq!(
            decide_attempt(&mut state, &facts, &inputs()),
            end(EndReason::RetryVeto, 5)
        );
    }
}

#[test]
fn generic_transient_failure_doubles_caps_then_jitters_and_escapes_once() {
    let error = error(None, None, None);
    let facts = facts(&error);
    for jitter in [0.8, 1.0, 1.2] {
        let mut state = AttemptState::default();
        let inputs = AttemptInputs { jitter, ..inputs() };
        for (index, nominal) in [2, 4, 8, 16, 30].into_iter().enumerate() {
            assert_eq!(
                decide_attempt(&mut state, &facts, &inputs),
                resend(
                    seconds(nominal).mul_f64(jitter),
                    "transient_failure",
                    index == 0
                )
            );
        }
        assert_eq!(
            decide_attempt(&mut state, &facts, &inputs),
            end(EndReason::AttemptLimit, 5)
        );
    }
}

#[test]
fn rejected_token_adds_a_seventh_send_without_advancing_backoff() {
    let transient = error(None, None, None);
    let rejected = rejected();
    for refresh_at in 0..6 {
        let mut state = AttemptState::default();
        for ordinary in 0..6 {
            if ordinary == refresh_at {
                assert_eq!(
                    decide_attempt(&mut state, &facts(&rejected), &inputs()),
                    AttemptDecision::RefreshAndResend
                );
                decide_rejected_refresh(
                    &mut state,
                    &facts(&rejected),
                    RejectedRefreshResult::Refreshed,
                );
            }
            let decision = decide_attempt(&mut state, &facts(&transient), &inputs());
            if ordinary == 5 {
                assert_eq!(decision, end(EndReason::AttemptLimit, 6));
            } else {
                let expected = [2, 4, 8, 16, 30][ordinary];
                assert!(
                    matches!(decision, AttemptDecision::Resend { delay, .. } if delay == seconds(expected))
                );
            }
        }
    }
}

#[test]
fn generic_waiting_budget_uses_measured_attempt_refresh_and_sleep_with_strict_limit() {
    let error = error(None, None, Some(120.0));
    let mut facts = facts(&error);
    let mut state = AttemptState::default();
    note_waited(&mut state, seconds(80));
    note_refresh_elapsed(&mut state, seconds(30));
    facts.attempt_elapsed = seconds(10);
    facts.got_response_bytes = false;
    assert!(
        matches!(decide_attempt(&mut state, &facts, &inputs()), AttemptDecision::Resend { delay, .. } if delay == seconds(120))
    );
    // Deciding a wait does not charge it. Only its actual elapsed time does.
    assert_eq!(state.retry_wait_spent, seconds(120));
    note_waited(&mut state, seconds(121));
    facts.got_response_bytes = true;
    facts.attempt_elapsed = seconds(1000);
    assert_eq!(
        decide_attempt(&mut state, &facts, &inputs()),
        end(EndReason::WaitingBudget, 1)
    );
    assert_eq!(state.retry_wait_spent, seconds(241));
}

#[test]
fn generic_attempt_limit_precedes_decode_increment_deadline_and_budget() {
    let error = error(Some(429), None, Some(120.0));
    let mut facts = facts(&error);
    facts.cause = Some(AttemptCause::DeterministicDecode);
    let mut state = AttemptState {
        attempt_index: 5,
        retry_wait_spent: seconds(999),
        ..AttemptState::default()
    };
    let inputs = AttemptInputs {
        until_deadline: Some(seconds(1)),
        ..inputs()
    };
    assert_eq!(
        decide_attempt(&mut state, &facts, &inputs),
        end(EndReason::AttemptLimit, 5)
    );
    assert_eq!(state.decode_retries, 0);
}

#[test]
fn generic_retry_after_exceeds_deadline_only_for_429_and_precedes_budget() {
    for status in [429, 503] {
        let error = error(Some(status), None, Some(469.0));
        let mut state = AttemptState::default();
        let inputs = AttemptInputs {
            until_deadline: Some(seconds(120)),
            ..inputs()
        };
        let decision = decide_attempt(&mut state, &facts(&error), &inputs);
        if status == 429 {
            assert_eq!(decision, end(EndReason::RetryAfterExceedsDeadline, 0));
            state.retry_wait_spent = seconds(300);
            assert_eq!(
                decide_attempt(&mut state, &facts(&error), &inputs),
                end(EndReason::RetryAfterExceedsDeadline, 0)
            );
        } else {
            assert!(
                matches!(decision, AttemptDecision::Resend { delay, server_advised: true, .. } if delay == seconds(120))
            );
        }
    }
    let error = error(Some(429), None, None);
    let inputs = AttemptInputs {
        until_deadline: Some(seconds(1)),
        ..inputs()
    };
    assert!(matches!(
        decide_attempt(&mut AttemptState::default(), &facts(&error), &inputs),
        AttemptDecision::Resend { .. }
    ));
}

#[test]
fn raw_retry_after_conversion_matches_each_policy_including_non_finite_values() {
    for (raw, generic, codex) in [
        (-1.0, Duration::ZERO, Some(Duration::ZERO)),
        (f64::NAN, Duration::ZERO, Some(Duration::ZERO)),
        (f64::NEG_INFINITY, Duration::ZERO, Some(Duration::ZERO)),
        (f64::INFINITY, seconds(120), None),
        (469.0, seconds(120), Some(seconds(469))),
        (
            0.125,
            Duration::from_millis(125),
            Some(Duration::from_millis(125)),
        ),
    ] {
        let error = error(Some(429), None, Some(raw));
        for use_codex in [false, true] {
            let decision =
                decide_attempt(&mut AttemptState::new(use_codex), &facts(&error), &inputs());
            let expected = if use_codex { codex } else { Some(generic) };
            if let Some(expected) = expected {
                assert!(
                    matches!(decision, AttemptDecision::Resend { delay, server_advised: true, .. } if delay == expected)
                );
            } else {
                assert_eq!(decision, end(EndReason::NoServerRetryTime, 0));
            }
        }
    }
}

#[test]
fn codex_request_retry_precedes_server_overloaded_terminal_rule() {
    let error = error(Some(503), Some("server_is_overloaded"), None);
    let mut facts = facts(&error);
    facts.outcome = AttemptOutcome::HttpError;
    facts.over_https = true;
    let mut state = AttemptState::new(true);
    for (index, millis) in [200, 400, 800, 1600].into_iter().enumerate() {
        assert_eq!(
            decide_attempt(&mut state, &facts, &inputs()),
            resend(Duration::from_millis(millis), "request_retry", index == 0)
        );
    }
    assert_eq!(
        decide_attempt(&mut state, &facts, &inputs()),
        end(EndReason::NoServerRetryTime, 4)
    );
}

#[test]
fn codex_connection_failed_backoff_and_connection_attempt_limit_are_reused() {
    for https in [false, true] {
        let error = error(None, None, None);
        let mut facts = facts(&error);
        facts.outcome = AttemptOutcome::ConnectFailed;
        facts.over_https = https;
        let mut state = AttemptState::new(true);
        let expected_ms = if https {
            [200, 400, 800, 1600, 5000]
        } else {
            [5000, 10000, 20000, 40000, 60000]
        };
        for (index, millis) in expected_ms.into_iter().enumerate() {
            assert_eq!(
                decide_attempt(&mut state, &facts, &inputs()),
                resend(
                    Duration::from_millis(millis),
                    if https && index < 4 {
                        "request_retry"
                    } else {
                        "connection_failed"
                    },
                    index == 0
                )
            );
        }
        assert_eq!(
            decide_attempt(&mut state, &facts, &inputs()),
            end(EndReason::ConnectionAttemptLimit, 5)
        );
    }
}

#[test]
fn codex_terminal_codes_end_even_with_server_time() {
    for (status, code) in [
        (Some(429), "flex_unavailable"),
        (None, "flex_unavailable"),
        (Some(429), "usage_not_included"),
    ] {
        let error = error(status, Some(code), Some(469.0));
        assert_eq!(
            decide_attempt(&mut AttemptState::new(true), &facts(&error), &inputs()),
            end(EndReason::CodexTerminal, 0)
        );
    }
}

#[test]
fn codex_no_server_retry_time_ends_429_and_overload() {
    for (status, code) in [
        (Some(429), None),
        (Some(503), Some("server_is_overloaded")),
        (None, Some("server_is_overloaded")),
    ] {
        let error = error(status, code, None);
        assert_eq!(
            decide_attempt(&mut AttemptState::new(true), &facts(&error), &inputs()),
            end(EndReason::NoServerRetryTime, 0)
        );
    }
}

#[test]
fn codex_transient_failure_then_attempt_limit_without_switch() {
    let error = error(None, None, None);
    let mut state = AttemptState::new(true);
    for (index, millis) in [200, 400, 800, 1600, 3200].into_iter().enumerate() {
        assert_eq!(
            decide_attempt(&mut state, &facts(&error), &inputs()),
            resend(
                Duration::from_millis(millis),
                "transient_failure",
                index == 0
            )
        );
    }
    assert_eq!(
        decide_attempt(&mut state, &facts(&error), &inputs()),
        end(EndReason::AttemptLimit, 5)
    );
}

fn exhaust_stream(state: &mut AttemptState) {
    let error = error(None, None, None);
    for _ in 0..5 {
        assert!(matches!(
            decide_attempt(state, &facts(&error), &inputs()),
            AttemptDecision::Resend { .. }
        ));
    }
}

#[test]
fn codex_socket_attempt_limit_switches_once_with_fresh_stream_count() {
    let mut state = AttemptState::new(true);
    exhaust_stream(&mut state);
    let error = error(None, None, None);
    let inputs = AttemptInputs {
        can_switch_to_https: true,
        ..inputs()
    };
    assert_eq!(
        decide_attempt(&mut state, &facts(&error), &inputs),
        AttemptDecision::Resend {
            delay: Duration::ZERO,
            server_advised: false,
            escape_client: false,
            switch_to_https: true,
            reason: "socket_attempt_limit",
        }
    );
    exhaust_stream(&mut state);
    assert_eq!(
        decide_attempt(&mut state, &facts(&error), &super::tests::inputs()),
        end(EndReason::AttemptLimit, 11)
    );
}

#[test]
fn codex_retry_after_exceeds_deadline_retains_https_switch_before_veto() {
    for switch in [false, true] {
        let mut state = AttemptState::new(true);
        if switch {
            exhaust_stream(&mut state);
        }
        let error = error(Some(429), None, Some(469.0));
        let inputs = AttemptInputs {
            can_switch_to_https: switch,
            until_deadline: Some(seconds(469)),
            ..inputs()
        };
        assert_eq!(
            decide_attempt(&mut state, &facts(&error), &inputs),
            AttemptDecision::EndStep {
                reason: EndReason::RetryAfterExceedsDeadline,
                retries_used: if switch { 5 } else { 0 },
                switch_to_https: switch,
                note_unrecovered_rejection: false,
            }
        );
    }
}

#[test]
fn codex_server_advised_wait_is_uncapped_and_ignores_generic_wait_budget() {
    let error = error(Some(429), None, Some(469.0));
    let mut state = AttemptState::new(true);
    note_waited(&mut state, seconds(1000));
    assert_eq!(
        decide_attempt(&mut state, &facts(&error), &inputs()),
        AttemptDecision::Resend {
            delay: seconds(469),
            server_advised: true,
            escape_client: true,
            switch_to_https: false,
            reason: "transient_failure",
        }
    );
}

mod simulation;
