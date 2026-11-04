use super::super::{
    attempt_test_hooks::{self, Event},
    attempt_trace_cases::{self as cases, Case, Kind, Policy, Refresh},
};
use super::*;

fn error_body(kind: Kind) -> Value {
    let mut error = serde_json::json!({"message":"scripted error"});
    if let Some(code) = kind.code() {
        error["code"] = code.into();
    }
    serde_json::json!({"error":error})
}

fn reply(attempt: &cases::Attempt) -> Vec<Value> {
    if let Some(status) = attempt.kind.status() {
        let mut frame = error_body(attempt.kind);
        frame["type"] = "error".into();
        frame["status"] = status.into();
        let mut headers = serde_json::Map::new();
        if let Some(server) = attempt.server {
            headers.insert("retry-after".into(), server.into());
        }
        if attempt.kind == Kind::Veto {
            headers.insert("x-should-retry".into(), "false".into());
        }
        frame["headers"] = headers.into();
        vec![frame]
    } else if attempt.kind == Kind::Cut {
        vec![serde_json::json!({"type":"response.output_text.delta","delta":"partial"})]
    } else {
        text_events()
    }
}

async fn run(case: &Case) -> Vec<Event> {
    let mock = Mock::start(
        (0..128).map(|i| reply(case.attempt(i))).collect(),
        true,
        false,
    )
    .await;
    let provider = provider(&mock);
    assert!(
        provider.websocket.can_switch_to_https(),
        "the read-only query must not switch"
    );
    assert!(!provider.websocket.forced_https());
    let request = cases::request(format!("{}/responses", mock.url), case.policy);
    let digest = RequestDigest::of(&request.body);
    let (tx, mut rx) = mpsc::channel(1024);
    let cancel = CancellationToken::new();
    let now = Instant::now();
    let clock = case.clock();
    let mut index = 0;
    let trace = attempt_test_hooks::capture(
        now,
        cases::FIRST_ATTEMPT,
        case.jitters(),
        Duration::ZERO,
        drive_step_attempts(
            &provider.client,
            &provider.clock,
            request,
            &digest,
            cases::recovery(case),
            Some("account".into()),
            || {
                let kind = case.attempt(index).kind;
                index += 1;
                *mock.http_error.lock().unwrap() = kind
                    .status()
                    .map(|status| (status, error_body(kind).to_string()));
                cases::Normalizer {
                    kind,
                    inner: provider.family.normalizer(&provider.frozen, StepId(1)),
                }
            },
            &tx,
            &cancel,
            case.deadline.map(|d| now + d),
            cases::seal(),
            &clock,
            Some(&provider.websocket),
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
    assert_eq!(
        provider.websocket.can_switch_to_https(),
        !trace.contains(&Event::SwitchToHttps)
    );
    let seen = mock.observed.lock().unwrap();
    assert_eq!(
        seen.requests.len() + seen.http_bodies.len(),
        trace
            .iter()
            .filter(|e| matches!(e, Event::Send { .. }))
            .count()
    );
    assert!(
        seen.headers
            .iter()
            .all(|h| h["chatgpt-account-id"] == "account"),
        "A3: account changes never reach the wire"
    );
    trace
}

#[tokio::test]
async fn seeded_attempt_trace_equivalence_codex_websocket_with_https_fallback() {
    for seed in cases::SEEDS {
        for sticky in [false, true] {
            let case = Case::seeded(Policy::CodexWebsocket, seed, sticky);
            cases::assert_trace(&case, &run(&case).await);
        }
    }
}

#[tokio::test]
async fn attempt_trace_equivalence_pins_https_switch_before_deadline_veto() {
    for kind in [Kind::Cut, Kind::Decode, Kind::Server, Kind::Complete] {
        let case = Case::repeating(Policy::CodexWebsocket, kind);
        cases::assert_trace(&case, &run(&case).await);
    }
    let mut case = Case::repeating(Policy::CodexWebsocket, Kind::RateLimit);
    case.attempts[0].server = Some("0");
    case.attempts = vec![case.attempts[0].clone(); 6];
    case.attempts[5].server = Some("469");
    case.deadline = Some(Duration::from_secs(469));
    let actual = run(&case).await;
    cases::assert_trace(&case, &actual);
    let switch = actual
        .iter()
        .position(|e| *e == Event::SwitchToHttps)
        .unwrap();
    assert!(
        matches!(&actual[switch + 1], Event::Decision { reason, .. } if reason == "retry_after_exceeds_deadline")
    );
    assert!(matches!(
        actual.last(),
        Some(Event::End {
            retries_used: 5,
            failed: true,
            ..
        })
    ));
    assert!(!actual[switch..]
        .iter()
        .any(|e| matches!(e, Event::Send { .. } | Event::Wait(_))));
}

#[tokio::test]
async fn attempt_trace_equivalence_pins_socket_rejected_and_pre_send_refreshes() {
    for refresh in [
        Refresh::Missing,
        Refresh::Changed,
        Refresh::Same,
        Refresh::Fresh,
    ] {
        let mut case = Case::repeating(Policy::CodexWebsocket, Kind::Unauthorized);
        case.refresh = Some(refresh);
        case.refresh_elapsed = Duration::from_secs(23);
        cases::assert_trace(&case, &run(&case).await);
    }
    for pre_changed_at in [0, 2] {
        let mut case = Case::repeating(Policy::CodexWebsocket, Kind::Server);
        case.refresh = Some(Refresh::Fresh);
        case.pre_changed_at = Some(pre_changed_at);
        cases::assert_trace(&case, &run(&case).await);
    }
}

#[tokio::test]
async fn attempt_trace_equivalence_pins_read_only_switch_eligibility() {
    let mut case = Case::repeating(Policy::CodexWebsocket, Kind::Cut);
    case.attempts = vec![case.attempts[0].clone(); 6];
    case.attempts[0].kind = Kind::Decode;
    case.attempts[5].kind = Kind::Decode;
    // Six socket failures force HTTPS, but the second decode veto runs before Codex's
    // explicit switch. The query must still report that the explicit switch is available.
    let actual = run(&case).await;
    cases::assert_trace(&case, &actual);
    assert!(!actual.contains(&Event::SwitchToHttps));

    let session = responses_websocket::Session::default();
    session.set_policy(Arc::new(|| true));
    assert!(!session.can_switch_to_https());
    assert!(!session.try_switch_to_https());
    session.set_policy(Arc::new(|| false));
    assert!(session.can_switch_to_https());
    assert!(session.try_switch_to_https());
    assert!(!session.can_switch_to_https());
}

#[tokio::test]
async fn attempt_trace_equivalence_refused_flex_tier_does_not_resend() {
    for seed in cases::SEEDS {
        let generated = Case::seeded(Policy::CodexWebsocket, seed, false);
        if let Some(attempt) = generated
            .attempts
            .iter()
            .find(|a| a.kind == Kind::Flex && a.server.is_some())
        {
            let mut case = Case::repeating(Policy::CodexWebsocket, Kind::Flex);
            case.seed = Some(seed);
            case.attempts = vec![attempt.clone()];
            cases::assert_trace(&case, &run(&case).await);
            return;
        }
    }
    panic!("fixed seeds missed a flex refusal with server advice");
}
