use super::*;
use std::time::{Duration, UNIX_EPOCH};

#[test]
fn beta_merger_preserves_order_and_deduplicates_exact_tokens() {
    assert_eq!(
        merge_beta_headers(["a,b,a", "b,c", "A,a"]).unwrap(),
        "a,b,c,A"
    );
}

#[test]
fn beta_merger_trims_whitespace_and_skips_empty_tokens() {
    assert_eq!(
        merge_beta_headers([" \ta , , b\t,", "", " c "]).unwrap(),
        "a,b,c"
    );
    assert_eq!(merge_beta_headers(Vec::<&str>::new()).unwrap(), "");
}

#[test]
fn beta_merger_refuses_invalid_header_values_without_dropping_tokens() {
    for invalid in [
        "café",
        "a\r\nb",
        "a\0b",
        "a\x7fb",
        "\na",
        "a\x0bb",
        "a\u{00a0}",
    ] {
        assert_eq!(
            merge_beta_headers(["oauth-2025-04-20", invalid]),
            Err(InvalidBetaHeader)
        );
        assert_eq!(
            remove_beta_token([invalid], OAUTH_BETA),
            Err(InvalidBetaHeader)
        );
    }
}

#[test]
fn beta_removal_preserves_unrelated_client_tokens() {
    assert_eq!(
        remove_beta_token(
            [
                "tool-search,oauth-2025-04-20,context-window",
                "oauth-2025-04-20,OAuth-2025-04-20",
            ],
            OAUTH_BETA
        )
        .unwrap(),
        "tool-search,context-window,OAuth-2025-04-20"
    );
    assert_eq!(remove_beta_token([OAUTH_BETA], OAUTH_BETA).unwrap(), "");
    assert_eq!(remove_beta_token(["a,b"], "absent").unwrap(), "a,b");
}

#[test]
fn auth_policy_keeps_identity_outside_shared_headers() {
    let oauth = header_policy(AuthKind::OAuth, Features::default());
    assert_eq!(
        oauth.credential,
        CredentialHeader {
            name: "authorization",
            prefix: "Bearer "
        }
    );
    assert_eq!(oauth.x_app, Some("cli"));
    assert_eq!(oauth.beta.as_deref(), Some("oauth-2025-04-20"));
    let key = header_policy(AuthKind::ApiKey, Features::default());
    assert_eq!(
        key.credential,
        CredentialHeader {
            name: "x-api-key",
            prefix: ""
        }
    );
    assert_eq!(key.x_app, None);
    assert_eq!(key.beta, None);
    for auth in [AuthKind::ApiKey, AuthKind::OAuth] {
        let policy = header_policy(
            auth,
            Features {
                interleaved_thinking: true,
                thinking_binding_controls: true,
                fast_mode: true,
            },
        );
        let expected = if auth == AuthKind::OAuth {
            "oauth-2025-04-20,interleaved-thinking-2025-05-14,thinking-binding-controls-2026-08-01,fast-mode-2026-02-01"
        } else {
            "interleaved-thinking-2025-05-14,thinking-binding-controls-2026-08-01,fast-mode-2026-02-01"
        };
        assert_eq!(policy.beta.as_deref(), Some(expected));
    }
    let binding = header_policy(
        AuthKind::ApiKey,
        Features {
            thinking_binding_controls: true,
            ..Features::default()
        },
    );
    assert_eq!(
        binding.beta.as_deref(),
        Some("thinking-binding-controls-2026-08-01")
    );
}

// Reuse broca-provider's verbatim transport fixtures so both consumers classify the
// same responses: a subscription's exhausted seven-day quota (captured 2026-09-26
// 01:59Z) and a short throttle whose account still has quota.
const ACCOUNT_LIMIT_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."},"request_id":"req_011CfRDPYNrfveajQccKCJ7o"}"#;
const CAPTURED_HEADERS: [(&str, &str); 14] = [
    ("x-should-retry", "true"),
    ("anthropic-ratelimit-unified-5h-status", "allowed"),
    (
        "anthropic-ratelimit-unified-representative-claim",
        "seven_day",
    ),
    (
        "anthropic-ratelimit-unified-overage-disabled-reason",
        "org_level_disabled",
    ),
    ("anthropic-ratelimit-unified-overage-status", "rejected"),
    ("anthropic-ratelimit-unified-reset", "1790740800"),
    ("anthropic-ratelimit-unified-5h-utilization", "0.21"),
    ("anthropic-ratelimit-unified-7d-reset", "1790740800"),
    ("anthropic-ratelimit-unified-5h-reset", "1790394000"),
    ("anthropic-ratelimit-unified-7d-status", "rejected"),
    ("anthropic-ratelimit-unified-7d-surpassed-threshold", "1.0"),
    ("retry-after", "352835"),
    ("anthropic-ratelimit-unified-7d-utilization", "1.0"),
    ("anthropic-ratelimit-unified-status", "rejected"),
];
const ORDINARY_HEADERS: [(&str, &str); 4] = [
    ("anthropic-ratelimit-unified-status", "allowed"),
    (
        "anthropic-ratelimit-unified-representative-claim",
        "five_hour",
    ),
    ("anthropic-ratelimit-unified-reset", "1790397000"),
    ("retry-after", "0"),
];

#[test]
fn captured_anthropic_weekly_limit_429_is_account_limit_exhausted() {
    let now = UNIX_EPOCH + Duration::from_secs(1_790_387_965);
    let result =
        classify_account_limit_exhausted(429, CAPTURED_HEADERS, ACCOUNT_LIMIT_BODY, now).unwrap();
    assert_eq!(result.resets_at_ms, Some(1_790_740_800_000));
    assert_eq!(result.resets_at_source, Some(ResetSource::Header));
    assert_eq!(result.limit_window.as_deref(), Some("seven_day"));
}

#[test]
fn only_rejected_status_on_429_is_an_account_wall() {
    let now = UNIX_EPOCH + Duration::from_secs(1_790_387_965);
    for headers in [&ORDINARY_HEADERS[..], &[("retry-after", "3")][..]] {
        assert_eq!(
            classify_account_limit_exhausted(429, headers.iter().copied(), ACCOUNT_LIMIT_BODY, now),
            None
        );
    }
    for status in [200, 400, 401, 403, 500, 529] {
        assert_eq!(
            classify_account_limit_exhausted(status, CAPTURED_HEADERS, ACCOUNT_LIMIT_BODY, now),
            None
        );
    }
    let headers = [(UNIFIED_STATUS, "allowed"), (UNIFIED_STATUS, "rejected")];
    assert_eq!(
        classify_account_limit_exhausted(429, headers, ACCOUNT_LIMIT_BODY, now),
        None
    );
    let headers = [("Anthropic-Ratelimit-Unified-Status", " ReJeCtEd ")];
    assert_eq!(
        classify_account_limit_exhausted(429, headers, "not JSON", now),
        Some(AccountLimitExhausted {
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
        })
    );
}

#[test]
fn missing_or_invalid_reset_uses_retry_after() {
    let now = UNIX_EPOCH + Duration::from_secs(1_790_387_965);
    for reset in [None, Some("bad"), Some("1.790740800e9")] {
        let mut headers: Vec<_> = CAPTURED_HEADERS
            .into_iter()
            .filter(|(name, _)| *name != UNIFIED_RESET)
            .collect();
        if let Some(reset) = reset {
            headers.push((UNIFIED_RESET, reset));
        }
        let result =
            classify_account_limit_exhausted(429, headers, ACCOUNT_LIMIT_BODY, now).unwrap();
        assert_eq!(result.resets_at_ms, Some((1_790_387_965 + 352_835) * 1_000));
        assert_eq!(result.resets_at_source, Some(ResetSource::RetryAfter));
        assert_eq!(result.limit_window.as_deref(), Some("seven_day"));
    }
}

#[test]
fn retry_hints_dates_duplicates_and_saturation_keep_transport_semantics() {
    let now = UNIX_EPOCH + Duration::from_secs(1_790_387_965);
    for (hint, expected_delay) in [
        (vec![("retry-after", "1.25")], Some(1_250)),
        (
            vec![("retry-after-ms", "1250"), ("retry-after", "9")],
            Some(1_250),
        ),
        (
            vec![("retry-after-ms", "bad"), ("retry-after", "9")],
            Some(9_000),
        ),
        (
            vec![("retry-after", "Sat, 26 Sep 2026 01:59:28 GMT")],
            Some(3_000),
        ),
        (
            vec![("retry-after", "Sat, 26 Sep 2026 01:59:00 GMT")],
            Some(0),
        ),
        (vec![("retry-after", "-1")], None),
        (vec![("retry-after", "NaN")], None),
        (vec![("retry-after", "inf")], None),
    ] {
        let headers = [(UNIFIED_STATUS, "rejected")].into_iter().chain(hint);
        let result =
            classify_account_limit_exhausted(429, headers, ACCOUNT_LIMIT_BODY, now).unwrap();
        assert_eq!(
            result.resets_at_ms,
            expected_delay.map(|delay| 1_790_387_965_000 + delay)
        );
    }

    let headers = [
        (UNIFIED_STATUS, "rejected"),
        (UNIFIED_RESET, "18446744073709551615"),
        (UNIFIED_REPRESENTATIVE_CLAIM, " "),
    ];
    let result = classify_account_limit_exhausted(429, headers, ACCOUNT_LIMIT_BODY, now).unwrap();
    assert_eq!(result.resets_at_ms, Some(u64::MAX));
    assert_eq!(result.limit_window, None);
}

// The assumed prepaid-credit refusal tests the narrow invalid-request/message rule.
// It is copied from Broca's transport test, not from captured provider traffic.
const CREDIT_BALANCE_BODY: &str = r#"{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}"#;

#[test]
fn assumed_credit_balance_error_is_billing_not_generic_invalid_request() {
    assert!(credit_balance_exhausted(CREDIT_BALANCE_BODY));
    assert!(credit_balance_exhausted(&CREDIT_BALANCE_BODY.replace(
        "credit balance is too low",
        "CREDIT BALANCE IS TOO LOW"
    )));
    for body in [
        "not JSON",
        "{}",
        r#"{"error":{"type":"invalid_request_error","message":"messages: text content blocks must be non-empty"}}"#,
    ] {
        assert!(!credit_balance_exhausted(body));
    }
    assert!(!credit_balance_exhausted(
        &CREDIT_BALANCE_BODY.replace("invalid_request_error", "authentication_error")
    ));
}

#[test]
fn quota_headers_allow_future_names_keep_first_values_and_lowercase_names() {
    let headers: [(&str, &[u8]); 6] = [
        ("Anthropic-Ratelimit-Unified-7d-Reset", b"1.79068680e9"),
        ("anthropic-ratelimit-unified-7d-reset", b"ignored"),
        (
            "anthropic-ratelimit-unified-some-future-window",
            b"x;q=1, 2",
        ),
        ("anthropic-ratelimit-unified-status", b"\xff"),
        ("x-anthropic-ratelimit-unified-fake", b"no"),
        ("authorization", b"secret"),
    ];
    let expected = BTreeMap::from([
        (
            "anthropic-ratelimit-unified-7d-reset".into(),
            "1.79068680e9".into(),
        ),
        (
            "anthropic-ratelimit-unified-some-future-window".into(),
            "x;q=1, 2".into(),
        ),
        (
            "anthropic-ratelimit-unified-status".into(),
            "\u{fffd}".into(),
        ),
    ]);
    assert_eq!(collect_unified_quota_headers(headers), expected);
}
