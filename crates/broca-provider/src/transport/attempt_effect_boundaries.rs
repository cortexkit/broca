use super::*;
use super::super::{
    attempt_effect_hooks::{self, Boundary},
    attempt_test_hooks::{self, Event},
    attempt_trace_cases::{self as cases, Case, Kind, Policy},
};
use std::sync::{atomic::AtomicBool, Barrier};

async fn boundary_run(
    initially_eligible: bool,
    finally_eligible: bool,
    server_time: &'static str,
    deadline: Option<Duration>,
    switch_elapsed: Duration,
) -> (Vec<Event>, usize) {
    let mut case = Case::repeating(Policy::CodexWebsocket, Kind::RateLimit);
    case.attempts[0].server = Some(server_time);
    case.attempts = vec![case.attempts[0].clone(); 7];
    case.attempts[6].kind = Kind::Complete;
    let replies = (0..7).map(|index| {
        if index == 6 {
            text_events()
        } else {
            vec![serde_json::json!({
                "type": "error", "status": 429,
                "error": {"message": "scripted error"},
                "headers": {"retry-after": server_time}
            })]
        }
    }).collect();
    let mock = Mock::start(replies, true, false).await;
    let provider = provider(&mock);
    let forced = Arc::new(AtomicBool::new(false));
    let in_switch = Arc::new(AtomicBool::new(false));
    let switch_calls = Arc::new(AtomicUsize::new(0));
    provider.websocket.set_policy({
        let forced = forced.clone();
        let in_switch = in_switch.clone();
        let switch_calls = switch_calls.clone();
        Arc::new(move || {
            if in_switch.swap(false, Ordering::SeqCst) {
                switch_calls.fetch_add(1, Ordering::SeqCst);
                attempt_test_hooks::advance(switch_elapsed);
            }
            forced.load(Ordering::SeqCst)
        })
    });
    let (signal, changed) = std::sync::mpsc::channel();
    let barrier = Arc::new(Barrier::new(2));
    let editor = {
        let barrier = barrier.clone();
        let forced = forced.clone();
        std::thread::spawn(move || {
            // Closing the sender also releases this thread when a broken driver ends early.
            if changed.recv().is_err() {
                return false;
            }
            forced.store(!finally_eligible, Ordering::SeqCst);
            barrier.wait();
            true
        })
    };
    let failures = AtomicUsize::new(0);
    let jitter_draws = Arc::new(AtomicUsize::new(0));
    let effect = {
        let jitter_draws = jitter_draws.clone();
        move |boundary| match boundary {
            Boundary::FailedAttempt => {
                if failures.fetch_add(1, Ordering::SeqCst) == 5 {
                    forced.store(!initially_eligible, Ordering::SeqCst);
                }
            }
            Boundary::Switch => {
                signal.send(()).unwrap();
                barrier.wait();
                in_switch.store(true, Ordering::SeqCst);
            }
            Boundary::Jitter => { jitter_draws.fetch_add(1, Ordering::SeqCst); }
        }
    };
    let request = cases::request(format!("{}/responses", mock.url), case.policy);
    let digest = RequestDigest::of(&request.body);
    let (tx, _rx) = mpsc::channel(1024);
    let cancel = CancellationToken::new();
    let now = Instant::now();
    let clock = case.clock();
    let mut index = 0;
    let trace = attempt_effect_hooks::scope(effect, attempt_test_hooks::capture(
        now, cases::FIRST_ATTEMPT, case.jitters(), Duration::ZERO,
        drive_step_attempts(
            &provider.client, &provider.clock, request, &digest, None, Some("account".into()),
            || {
                let kind = case.attempt(index).kind;
                index += 1;
                *mock.http_error.lock().unwrap() = None;
                cases::Normalizer {
                    kind,
                    inner: provider.family.normalizer(&provider.frozen, StepId(1)),
                }
            },
            &tx, &cancel, deadline.map(|d| now + d), cases::seal(), &clock,
            Some(&provider.websocket), &AtomicUsize::new(0),
        ),
    )).await;
    assert!(editor.join().unwrap(), "the driver must request the actual switch even when an earlier eligibility observation would be false");
    assert_eq!(switch_calls.load(Ordering::SeqCst), 1);
    (trace, jitter_draws.load(Ordering::SeqCst))
}

#[tokio::test]
async fn switch_eligibility_true_to_false_uses_actual_result() {
    let (trace, _) = boundary_run(true, false, "0", None, Duration::ZERO).await;
    assert!(!trace.contains(&Event::SwitchToHttps));
    assert_eq!(trace.last(), Some(&Event::End { retries_used: 5, quiet: false, failed: true }));
    assert_eq!(trace.iter().filter(|e| matches!(e, Event::Send { .. })).count(), 6);
}

#[tokio::test]
async fn switch_eligibility_false_to_true_uses_actual_result() {
    let (trace, _) = boundary_run(false, true, "0", None, Duration::ZERO).await;
    assert!(trace.contains(&Event::SwitchToHttps));
    assert_eq!(trace.last(), Some(&Event::End { retries_used: 6, quiet: false, failed: false }));
}

#[tokio::test]
async fn deadline_is_sampled_after_the_switch_effect() {
    let (trace, _) = boundary_run(true, true, "1", Some(Duration::from_millis(6500)), Duration::from_secs(1)).await;
    assert!(trace.contains(&Event::SwitchToHttps));
    assert_eq!(trace.last(), Some(&Event::End { retries_used: 5, quiet: false, failed: true }));
    assert_eq!(trace.iter().filter(|e| matches!(e, Event::Wait(_))).count(), 5);
}

#[tokio::test]
async fn server_advised_attempts_do_not_consume_jitter() {
    let (_, draws) = boundary_run(true, true, "0", None, Duration::ZERO).await;
    assert_eq!(draws, 0);
}
