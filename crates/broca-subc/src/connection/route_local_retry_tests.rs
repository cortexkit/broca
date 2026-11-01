//! A root-local retryable bind refusal must not pace other sessions through the module gate.

use super::*;
use std::sync::atomic::AtomicUsize;

const ROOT_BUSY: &str = "test_root_busy";

fn whole_envelope(envelope: Duration) -> Duration {
    envelope
}

fn busy() -> ConnError {
    ConnError::Rejected(RejectedMessage::structured(
        ROOT_BUSY.into(),
        "root busy".into(),
    ))
}

#[tokio::test(start_paused = true)]
async fn a_root_local_retry_leaves_the_module_gate_open_for_another_session() {
    assert!(route_open_is_retryable(ROOT_BUSY));
    let gate = Arc::new(ModuleGate::with_jitter(
        whole_envelope,
        CancellationToken::new(),
    ));
    let attempts = Arc::new(AtomicUsize::new(0));
    let first_refusal = Arc::new(tokio::sync::Notify::new());
    let start = time::Instant::now();
    let local = {
        let gate = gate.clone();
        let attempts = attempts.clone();
        let first_refusal = first_refusal.clone();
        tokio::spawn(async move {
            retry_route_open_jittered(
                RouteOpenRetry::within(Duration::from_secs(90), Duration::from_secs(5)),
                Some(OutageWait {
                    patience: Duration::from_secs(90),
                    max_interval: Duration::from_secs(5),
                    deadline: None,
                }),
                None,
                None,
                || Some(gate.clone()),
                &whole_envelope,
                || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    let first_refusal = first_refusal.clone();
                    async move {
                        if attempt == 0 {
                            first_refusal.notify_one();
                            Err(busy())
                        } else {
                            Ok(RouteHandle::synthetic(9))
                        }
                    }
                },
            )
            .await
        })
    };
    first_refusal.notified().await;
    assert!(
        !gate.is_closed(),
        "one busy root must not close the whole module's gate"
    );
    assert_eq!(
        gate.state.borrow().epoch,
        0,
        "a local refusal must not change module availability"
    );
    let sibling = retry_route_open(
        RouteOpenRetry::default_patience(),
        None,
        || Some(gate.clone()),
        || async { Ok(RouteHandle::synthetic(10)) },
    )
    .await
    .unwrap();
    assert_eq!(sibling.channel, 10);
    assert_eq!(
        start.elapsed(),
        Duration::ZERO,
        "the other session opens immediately"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "the busy route is still waiting independently"
    );
    assert_eq!(local.await.unwrap().unwrap().channel, 9);
    assert_eq!(start.elapsed(), Duration::from_millis(100));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(!gate.is_closed());
}

#[test]
fn only_the_four_whole_module_codes_close_the_gate() {
    let module_codes = [
        "target_unavailable",
        "module_warming",
        "module_reloading",
        "module_timeout",
    ];
    for code in module_codes.into_iter().chain([
        ROOT_BUSY,
        "scope_not_synced",
        "scope_changed",
        "session_address_unavailable",
        "bind_busy",
        "bind_blocked_by_reader",
        "unknown_code",
    ]) {
        assert_eq!(
            is_module_availability_refusal(code),
            module_codes.contains(&code),
            "{code}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_root_local_refusal_does_not_reopen_a_closed_module_gate() {
    let gate = Arc::new(ModuleGate::with_jitter(
        whole_envelope,
        CancellationToken::new(),
    ));
    let GateAdmission::Admitted(ticket) = gate.admit(None, None).await else {
        panic!("a healthy module admits the first open");
    };
    let module_down = ConnError::Rejected(RejectedMessage::structured(
        "target_unavailable".into(),
        "module down".into(),
    ));
    gate.observe(&Err(module_down), 0, None);
    let epoch = gate.state.borrow().epoch;
    ticket.report(&Err(busy()));
    assert!(
        gate.is_closed(),
        "a refusal about one root is not module-wide recovery evidence"
    );
    assert_eq!(gate.state.borrow().epoch, epoch);
    assert_eq!(gate.closed_refusal().unwrap().code(), "target_unavailable");
}

#[tokio::test(start_paused = true)]
async fn a_root_local_retry_uses_the_tool_patience_without_closing_the_gate() {
    let gate = Arc::new(ModuleGate::with_jitter(
        whole_envelope,
        CancellationToken::new(),
    ));
    let start = time::Instant::now();
    let attempts = AtomicUsize::new(0);
    let result = retry_route_open_jittered(
        RouteOpenRetry::default_patience(),
        Some(OutageWait {
            patience: Duration::from_secs(90),
            max_interval: Duration::from_secs(5),
            deadline: None,
        }),
        None,
        None,
        || Some(gate.clone()),
        &whole_envelope,
        || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err(busy()) }
        },
    )
    .await;
    assert!(matches!(result,
        Err(RouteOpenError::Conn(ConnError::Rejected(ref message))) if message.code() == ROOT_BUSY
    ));
    assert_eq!(start.elapsed(), Duration::from_secs(90));
    assert!(attempts.load(Ordering::SeqCst) > 2);
    assert!(!gate.is_closed());
    assert_eq!(gate.state.borrow().epoch, 0);
}
