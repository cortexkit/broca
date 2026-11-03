use super::super::{
    attempt_test_hooks::{self, Event},
    attempt_trace_cases::{self as cases, Case, Kind, Policy, Refresh},
    *,
};
use super::*;

const COMPLETE_SSE: &[u8] = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"usage\":{\"input_tokens\":12,\"output_tokens\":1}}}\n\n";

fn reply(attempt: &cases::Attempt) -> ScriptResponse {
    match attempt.kind {
        Kind::Cut => ScriptResponse::MidChunkDisconnect,
        Kind::Disconnect => ScriptResponse::Disconnect,
        _ => {
            let mut headers = Vec::new();
            if let Some(server) = attempt.server {
                headers.push(("retry-after", server));
            }
            if attempt.kind == Kind::Veto {
                headers.push(("x-should-retry", "false"));
            }
            headers.push(("content-type", "text/event-stream"));
            ScriptResponse::Http {
                status: attempt.kind.status().unwrap_or(200), headers,
                body: match attempt.kind {
                    Kind::Overflow => br#"{"error":{"code":"context_length_exceeded","message":"context too long"}}"#,
                    Kind::Overload => br#"{"error":{"code":"server_is_overloaded","message":"overloaded"}}"#,
                    Kind::Flex => br#"{"error":{"code":"flex_unavailable","message":"unavailable"}}"#,
                    Kind::Complete | Kind::Decode => COMPLETE_SSE,
                    _ => br#"{"error":{"message":"scripted error"}}"#,
                },
            }
        }
    }
}

async fn run(case: &Case, delay_delta: Duration) -> Vec<Event> {
    let mut script: Vec<_> = (0..128)
        .filter(|&i| case.attempt(i).kind != Kind::Connect)
        .map(|i| reply(case.attempt(i)))
        .collect();
    if case.gzip {
        script.insert(
            0,
            ScriptResponse::Http {
                status: 415,
                headers: Vec::new(),
                body: b"",
            },
        );
    }
    let server = ScriptedServer::start(script).await;
    let address: std::net::SocketAddr =
        server.url.strip_prefix("http://").unwrap().parse().unwrap();
    let client = if case.gzip {
        ProviderHttpClient::resolving_anthropic_api_to(address, true)
    } else {
        build_provider_http_client()
    };
    if case.attempts.iter().all(|a| a.kind == Kind::Connect) {
        client.fail_connects.store(128, Ordering::SeqCst);
    }
    let url = if case.gzip {
        format!("http://api.anthropic.com:{}/messages", address.port())
    } else {
        format!("{}/responses", server.url)
    };
    let mut request = cases::request(url, case.policy);
    if case.gzip {
        request.body = Bytes::from(vec![b'x'; 100_000]);
    }
    let canonical = request.body.clone();
    let digest = RequestDigest::of(&request.body);
    let frozen = openai_responses_frozen_config("m");
    let (tx, mut rx) = mpsc::channel(1024);
    let cancel = CancellationToken::new();
    if case.cancel_before {
        cancel.cancel();
    }
    let now = Instant::now();
    let clock = case.clock();
    let mut index = 0;
    let trace = attempt_test_hooks::capture(
        now,
        cases::FIRST_ATTEMPT,
        case.jitters(),
        delay_delta,
        drive_step_attempts(
            &client,
            &Clock::default(),
            request,
            &digest,
            cases::recovery(case),
            Some("account".into()),
            || {
                let kind = case.attempt(index).kind;
                index += 1;
                cases::Normalizer {
                    kind,
                    inner: OpenAiResponses.normalizer(&frozen, StepId(1)),
                }
            },
            &tx,
            &cancel,
            case.deadline.map(|d| now + d),
            cases::seal(),
            &clock,
            None,
            &AtomicUsize::new(0),
        ),
    )
    .await;
    drop(tx);
    let mut published = Vec::new();
    while let Some(event) = rx.recv().await {
        if let NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) = event {
            assert_eq!(attempt.request_body_sha256, digest.sha256);
            published.push(Event::Attempt {
                ordinal: attempt.attempt,
                outcome: attempt.outcome,
            });
        }
    }
    assert_eq!(
        published,
        trace
            .iter()
            .filter(|e| matches!(e, Event::Attempt { .. }))
            .cloned()
            .collect::<Vec<_>>()
    );
    let sends = trace
        .iter()
        .filter(|e| matches!(e, Event::Send { .. }))
        .count();
    if !case.attempts.iter().any(|a| a.kind == Kind::Connect) {
        assert_eq!(
            server.request_bodies().len(),
            sends,
            "every physical send reaches the real harness"
        );
        if case.gzip {
            assert_eq!(
                server.request_header("content-encoding"),
                vec![Some("gzip".into()), None]
            );
            assert_eq!(server.request_bodies()[1], canonical);
        } else {
            assert!(server
                .request_bodies()
                .iter()
                .all(|body| body == canonical.as_ref()));
        }
        assert!(
            server
                .request_header("chatgpt-account-id")
                .iter()
                .all(|a| a.as_deref() == Some("account")),
            "A3: never send another account"
        );
    }
    trace
}

#[tokio::test]
async fn seeded_attempt_trace_equivalence_generic_and_codex_https() {
    for policy in [Policy::Generic, Policy::CodexHttps] {
        for seed in cases::SEEDS {
            for sticky in [false, true] {
                let case = Case::seeded(policy, seed, sticky);
                cases::assert_trace(&case, &run(&case, Duration::ZERO).await);
            }
        }
    }
}

#[tokio::test]
async fn attempt_trace_equivalence_pins_retry_limits_deadlines_refresh_and_records() {
    for policy in [Policy::Generic, Policy::CodexHttps] {
        for kind in [
            Kind::Cut,
            Kind::Connect,
            Kind::Disconnect,
            Kind::Decode,
            Kind::Server,
            Kind::Complete,
        ] {
            let case = Case::repeating(policy, kind);
            cases::assert_trace(&case, &run(&case, Duration::ZERO).await);
        }
        for refresh in [
            Refresh::Missing,
            Refresh::Changed,
            Refresh::Same,
            Refresh::Fresh,
        ] {
            let mut case = Case::repeating(policy, Kind::Unauthorized);
            case.refresh = Some(refresh);
            case.refresh_elapsed = Duration::from_secs(23);
            cases::assert_trace(&case, &run(&case, Duration::ZERO).await);
        }
        for pre_changed_at in [0, 2] {
            let mut case = Case::repeating(policy, Kind::Server);
            case.refresh = Some(Refresh::Fresh);
            case.pre_changed_at = Some(pre_changed_at);
            cases::assert_trace(&case, &run(&case, Duration::ZERO).await);
        }
        for kind in [Kind::RateLimit, Kind::Server] {
            let mut case = Case::repeating(policy, kind);
            case.attempts[0].server = Some("469");
            case.deadline = Some(Duration::from_secs(1));
            cases::assert_trace(&case, &run(&case, Duration::ZERO).await);
        }
        let mut case = Case::repeating(policy, Kind::Complete);
        case.cancel_before = true;
        cases::assert_trace(&case, &run(&case, Duration::ZERO).await);
    }
}

#[tokio::test]
async fn attempt_trace_negative_control_rejects_a_changed_requested_delay() {
    let case = Case::repeating(Policy::Generic, Kind::Cut);
    let actual = run(&case, Duration::from_millis(1)).await;
    let expected = cases::expected(&case);
    let mismatch = actual.iter().zip(&expected).find(|(a, b)| a != b).unwrap();
    assert!(
        matches!(mismatch, (Event::Wait(actual), Event::Wait(expected)) if *actual == *expected + Duration::from_millis(1)),
        "the same comparison used by the oracle must reject the transport delay mutation"
    );
}

#[tokio::test]
async fn attempt_trace_equivalence_pins_gzip_identity_send_inside_one_attempt() {
    let mut case = Case::repeating(Policy::Generic, Kind::Complete);
    case.gzip = true;
    let trace = run(&case, Duration::ZERO).await;
    cases::assert_trace(&case, &trace);
    assert!(matches!(
        trace.as_slice(),
        [
            Event::Send {
                ordinal: cases::FIRST_ATTEMPT,
                index: 1,
                ..
            },
            Event::Send {
                ordinal: cases::FIRST_ATTEMPT,
                index: 2,
                ..
            },
            Event::Attempt {
                ordinal: cases::FIRST_ATTEMPT,
                outcome: AttemptOutcome::Complete
            },
            Event::End {
                retries_used: 0,
                failed: false,
                ..
            },
        ]
    ));
}

#[tokio::test]
async fn attempt_trace_equivalence_pins_measured_waiting_budget_and_refresh_backoff() {
    for policy in [Policy::Generic, Policy::CodexHttps] {
        let mut case = Case::repeating(policy, Kind::Server);
        case.refresh = Some(Refresh::Fresh);
        case.refresh_elapsed = Duration::from_secs(181);
        let mut unauthorized = case.attempts[0].clone();
        unauthorized.kind = Kind::Unauthorized;
        unauthorized.elapsed = Duration::from_secs(9);
        let mut disconnect = unauthorized.clone();
        disconnect.kind = Kind::Disconnect;
        disconnect.elapsed = Duration::from_secs(40);
        case.attempts[0].server = Some("120");
        case.attempts = vec![unauthorized, disconnect, case.attempts[0].clone()];
        let trace = run(&case, Duration::ZERO).await;
        cases::assert_trace(&case, &trace);
        if policy == Policy::Generic {
            assert!(trace.iter().any(
                |e| matches!(e, Event::Decision { reason, .. } if reason == "waiting_budget")
            ));
            assert!(matches!(
                trace.last(),
                Some(Event::End {
                    retries_used: 2,
                    failed: true,
                    ..
                })
            ));
        }
    }
}
