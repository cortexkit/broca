use std::sync::atomic::AtomicUsize;

use super::*;
use subc_protocol::error_codes::TARGET_UNAVAILABLE;

fn refused(code: &str) -> ConnError {
    ConnError::Rejected(RejectedMessage::structured(code.into(), "not now".into()))
}

/// No jitter: each wait is its whole envelope, so the schedule is exact.
fn whole_envelope(envelope: Duration) -> Duration {
    envelope
}

fn patient(budget: Duration) -> RouteOpenRetry {
    RouteOpenRetry::within(budget, ROUTE_OPEN_RETRY_MAX_INTERVAL)
}

/// A gate closed by one availability refusal just now; its first probe turn is 100 ms out.
fn closed_gate(retired: CancellationToken) -> Arc<ModuleGate> {
    let gate = Arc::new(ModuleGate::with_jitter(whole_envelope, retired));
    gate.observe(&Err(refused(TARGET_UNAVAILABLE)), 0, None);
    assert!(gate.is_closed());
    gate
}

async fn admitted(gate: &Arc<ModuleGate>) -> GateTicket {
    match gate.admit(None, None).await {
        GateAdmission::Admitted(ticket) => ticket,
        _ => panic!("the gate must admit this open"),
    }
}

#[test]
fn the_default_patience_backs_off_to_five_seconds_within_three() {
    let retry = RouteOpenRetry::default_patience();
    assert_eq!(retry.budget, Duration::from_secs(3));
    assert_eq!(retry.max_interval, Duration::from_secs(5));
}

/// The default patience still gives up after 3 s on a module-availability refusal: the
/// 90 s wait is a tool-dispatch policy, and every other route kind (vault, catalog,
/// transform, routing, approval rechecks) keeps this bound.
#[tokio::test(start_paused = true)]
async fn the_default_patience_gives_up_after_three_seconds_on_module_refusals() {
    let start = time::Instant::now();
    let calls = AtomicUsize::new(0);
    let result = retry_route_open(RouteOpenRetry::default_patience(), None, no_gate, || {
        calls.fetch_add(1, Ordering::SeqCst);
        async { Err(refused("module_warming")) }
    })
    .await;
    assert!(
        matches!(result, Err(RouteOpenError::Conn(ConnError::Rejected(_)))),
        "the last refusal is returned unchanged: {result:?}"
    );
    let waited = start.elapsed();
    assert!(
        waited >= ROUTE_OPEN_RETRY_DEADLINE,
        "the open retries for the whole default budget: {waited:?}"
    );
    assert!(
        waited < Duration::from_secs(30),
        "and gives up long before any tool-dispatch patience: {waited:?}"
    );
    assert!(
        calls.load(Ordering::SeqCst) < 100,
        "a handful of attempts, not a storm: {}",
        calls.load(Ordering::SeqCst)
    );
}

/// Full jitter stays inside its envelope and actually varies: a wait never exceeds the
/// envelope, and draws spread over it rather than sitting at one value.
#[test]
fn full_jitter_draws_spread_over_the_envelope_and_never_exceed_it() {
    let envelope = Duration::from_secs(5);
    let draws: Vec<Duration> = (0..1000).map(|_| full_jitter(envelope)).collect();
    assert!(draws.iter().all(|wait| *wait <= envelope));
    assert!(draws.iter().any(|wait| *wait < Duration::from_secs(1)));
    assert!(draws.iter().any(|wait| *wait > Duration::from_secs(4)));
}

/// One open against a module that stays down: the waits between attempts double from
/// 100 ms and then hold at 5 s. They never shrink, except the last, which is cut short so
/// that no wait runs past the open's deadline.
#[tokio::test(start_paused = true)]
async fn the_wait_between_refused_attempts_doubles_to_five_seconds() {
    let start = time::Instant::now();
    let started_at = Mutex::new(Vec::new());
    let result = retry_route_open_jittered(
        patient(Duration::from_secs(30)),
        None,
        None,
        None,
        no_gate,
        &whole_envelope,
        || {
            started_at.lock().unwrap().push(start.elapsed());
            async { Err(refused(TARGET_UNAVAILABLE)) }
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(RouteOpenError::Conn(ConnError::Rejected(_)))
    ));
    let started_at = started_at.into_inner().unwrap();
    let waits: Vec<Duration> = started_at
        .windows(2)
        .map(|pair| pair[1] - pair[0])
        .collect();
    let ms = Duration::from_millis;
    assert_eq!(
        waits[..7],
        [
            ms(100),
            ms(200),
            ms(400),
            ms(800),
            ms(1600),
            ms(3200),
            ms(5000)
        ],
        "the waits must double from 100 ms up to the 5 s cap: {waits:?}"
    );
    assert!(waits
        .iter()
        .all(|wait| *wait <= ROUTE_OPEN_RETRY_MAX_INTERVAL));
    let (_, before_last) = waits.split_last().unwrap();
    assert!(
        before_last.windows(2).all(|pair| pair[1] >= pair[0]),
        "{waits:?}"
    );
    assert_eq!(
        *started_at.last().unwrap(),
        Duration::from_secs(30),
        "the last attempt is made at the deadline, not after it"
    );
}

/// Only evidence that the module is up opens the gate: a route, or a definite answer from
/// the daemon that is not about availability. A transport or protocol failure (broca's own
/// attempt timeout is a transport failure) says nothing either way, and a scope refusal is
/// about one caller's scope, so none of those opens it, and a scope refusal never closes it.
#[tokio::test(start_paused = true)]
async fn only_evidence_that_the_module_is_up_opens_the_gate() {
    let gate = ModuleGate::with_jitter(whole_envelope, CancellationToken::new());
    gate.observe(&Err(refused("scope_not_synced")), 0, None);
    assert!(!gate.is_closed(), "a scope refusal must not close the gate");
    gate.observe(&Err(refused(TARGET_UNAVAILABLE)), 0, None);
    assert!(gate.is_closed());
    for inconclusive in [
        ConnError::Transport("route.open got no reply within 15s".into()),
        ConnError::Protocol("unexpected route.open frame".into()),
        refused("scope_changed"),
    ] {
        gate.observe(&Err(inconclusive), 1, None);
        assert!(
            gate.is_closed(),
            "only evidence that the module is up opens the gate"
        );
    }
    gate.observe(&Err(refused("unknown_module")), 1, None);
    assert!(
        !gate.is_closed(),
        "a definite answer means the daemon reached the module"
    );
    gate.observe(&Err(refused("module_warming")), 2, None);
    assert!(gate.is_closed());
    gate.observe(&Ok(RouteHandle::synthetic(1)), 3, None);
    assert!(!gate.is_closed());
}

/// A probe that fails with a transport error leaves the gate closed, and the next probe
/// tick tests the module again.
#[tokio::test(start_paused = true)]
async fn a_transport_error_keeps_the_gate_closed_and_the_next_tick_probes_again() {
    let gate = closed_gate(CancellationToken::new());
    let start = time::Instant::now();
    // The first caller is the probe at 100 ms, and the daemon never answers it.
    let ticket = {
        time::sleep(Duration::from_millis(100)).await;
        admitted(&gate).await
    };
    ticket.report(&Err(ConnError::Transport(
        "route.open got no reply within 15s (ROUTE_OPEN_ATTEMPT_TIMEOUT)".into(),
    )));
    assert!(
        gate.is_closed(),
        "a transport error does not show the module is up"
    );
    let calls = AtomicUsize::new(0);
    let result = retry_route_open(
        patient(Duration::from_secs(30)),
        None,
        || Some(Arc::clone(&gate)),
        || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(RouteHandle::synthetic(7)) }
        },
    )
    .await;
    assert_eq!(result.unwrap().channel, 7);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        start.elapsed(),
        Duration::from_millis(300),
        "the next probe goes at the next tick (100 ms + a 200 ms envelope)"
    );
    assert!(!gate.is_closed());
}

/// A caller waiting at a closed gate leaves at its own deadline with the module's refusal,
/// without an attempt of its own reaching the daemon.
#[tokio::test(start_paused = true)]
async fn a_caller_waiting_at_a_closed_gate_leaves_at_its_own_deadline() {
    let gate = closed_gate(CancellationToken::new());
    time::sleep(Duration::from_secs(1)).await;
    // Someone else's probe is running and never ends during this test.
    let probe = admitted(&gate).await;
    let calls = AtomicUsize::new(0);
    let start = time::Instant::now();
    let result = retry_route_open(
        patient(Duration::from_secs(2)),
        None,
        || Some(Arc::clone(&gate)),
        || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(RouteHandle::synthetic(1)) }
        },
    )
    .await;
    assert_eq!(start.elapsed(), Duration::from_secs(2));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        result,
        Err(RouteOpenError::Conn(ConnError::Rejected(message))) if message.code() == TARGET_UNAVAILABLE
    ));
    drop(probe);
}

/// The caller holding the probe turn is dropped in the middle of its attempt (an outer
/// timeout): the turn passes at once to a caller still waiting, which probes and opens.
#[tokio::test(start_paused = true)]
async fn a_probe_holder_dropped_mid_attempt_hands_the_turn_on() {
    let gate = closed_gate(CancellationToken::new());
    let start = time::Instant::now();
    let gate_for = || Some(Arc::clone(&gate));
    // Takes the probe turn at 100 ms; its open never answers, and it is given up at 1 s.
    let abandoned = time::timeout(
        Duration::from_secs(1),
        retry_route_open(patient(Duration::from_secs(30)), None, gate_for, || {
            std::future::pending::<ConnResult<RouteHandle>>()
        }),
    );
    let calls = AtomicUsize::new(0);
    let waiting = async {
        // Arrives while the probe is running, so it waits for the turn.
        time::sleep(Duration::from_millis(150)).await;
        let result = retry_route_open(patient(Duration::from_secs(30)), None, gate_for, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(RouteHandle::synthetic(7)) }
        })
        .await;
        (result, start.elapsed())
    };
    let (abandoned, (result, opened_at)) = tokio::join!(abandoned, waiting);
    assert!(abandoned.is_err(), "the probe holder was given up");
    assert_eq!(result.unwrap().channel, 7);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        opened_at,
        Duration::from_secs(1),
        "the waiting caller takes the turn the moment the holder is dropped"
    );
}

/// A probe from an earlier closed period that is dropped late releases only its own turn:
/// the current period's probe keeps running alone, so a newcomer is not given a second one.
#[tokio::test(start_paused = true)]
async fn a_stale_probe_drop_does_not_let_two_probes_run() {
    let gate = closed_gate(CancellationToken::new());
    time::sleep(Duration::from_millis(100)).await;
    let stale = admitted(&gate).await;
    assert!(stale.probe.is_some());
    // Another open reports a route (the gate reopens), then one admitted in the new open
    // period is refused for availability (the gate closes again).
    gate.observe(&Ok(RouteHandle::synthetic(1)), 1, None);
    assert!(!gate.is_closed());
    let ordinary = admitted(&gate).await;
    ordinary.report(&Err(refused(TARGET_UNAVAILABLE)));
    assert!(gate.is_closed());
    time::sleep(Duration::from_millis(100)).await;
    let current = admitted(&gate).await;
    assert!(current.probe.is_some());
    drop(stale);
    assert!(
        matches!(gate.admit(None, None).await, GateAdmission::Refused(_)),
        "the current probe is still running, so no second probe may start"
    );
    drop(current);
}

/// An availability refusal from an open admitted before the gate's last reopen is older
/// news than what reopened it, so it does not close the gate again.
#[tokio::test(start_paused = true)]
async fn a_stale_refusal_does_not_close_a_reopened_gate() {
    let gate = Arc::new(ModuleGate::with_jitter(
        whole_envelope,
        CancellationToken::new(),
    ));
    let early = admitted(&gate).await;
    gate.observe(&Err(refused(TARGET_UNAVAILABLE)), 0, None);
    gate.observe(&Ok(RouteHandle::synthetic(1)), 1, None);
    assert!(!gate.is_closed());
    early.report(&Err(refused(TARGET_UNAVAILABLE)));
    assert!(
        !gate.is_closed(),
        "a refusal older than the reopen must not close it"
    );
}

/// A reconnect while callers wait at a closed gate: the old generation's gate retires, and
/// its waiters move to the new generation's gate instead of sleeping to their deadline and
/// returning the old generation's refusal. Here the new generation's gate is itself closed
/// with another caller's probe running, so the moved waiter must make no attempt until that
/// gate admits it.
#[tokio::test(start_paused = true)]
async fn a_reconnect_lets_waiters_on_the_old_gate_open_on_the_new_one() {
    let start = time::Instant::now();
    let old_generation = CancellationToken::new();
    let old_gate = closed_gate(old_generation.clone());
    let new_gate = closed_gate(CancellationToken::new());
    let current = Mutex::new(Arc::clone(&old_gate));
    time::sleep(Duration::from_millis(100)).await;
    // A probe is running on each gate, so neither gives the waiter a turn of its own.
    let old_probe = admitted(&old_gate).await;
    let new_probe = admitted(&new_gate).await;
    let attempts_at = Mutex::new(Vec::new());
    let waiter = retry_route_open(
        patient(Duration::from_secs(30)),
        None,
        || Some(Arc::clone(&current.lock().unwrap())),
        || {
            attempts_at.lock().unwrap().push(start.elapsed());
            async { Ok(RouteHandle::synthetic(9)) }
        },
    );
    let reconnect = async {
        time::sleep(Duration::from_millis(900)).await;
        // The replacement generation's gate is published, then the old generation ends.
        *current.lock().unwrap() = Arc::clone(&new_gate);
        old_generation.cancel();
        time::sleep(Duration::from_secs(1)).await;
        // The new generation's probe finds the module up, which opens its gate.
        new_probe.report(&Ok(RouteHandle::synthetic(1)));
    };
    let (result, ()) = tokio::join!(
        async {
            let result = waiter.await;
            (result, start.elapsed())
        },
        reconnect
    );
    let (result, opened_at) = result;
    assert_eq!(
        result
            .expect("the waiter opens on the new generation")
            .channel,
        9
    );
    assert_eq!(
        attempts_at.into_inner().unwrap(),
        [Duration::from_secs(2)],
        "one attempt, made only once the new generation's gate admitted it"
    );
    assert_eq!(opened_at, Duration::from_secs(2));
    drop(old_probe);
}

/// A waiter whose deadline passes while the gate is closed makes no attempt, even when the
/// gate has opened by the time it next runs: its budget is spent.
#[tokio::test(start_paused = true)]
async fn a_waiter_past_its_deadline_makes_no_attempt_when_the_gate_reopens() {
    let gate = closed_gate(CancellationToken::new());
    time::sleep(Duration::from_millis(100)).await;
    // Someone else's probe is running, so the waiter has no turn coming.
    let probe = admitted(&gate).await;
    let calls = AtomicUsize::new(0);
    let mut waiter = std::pin::pin!(retry_route_open(
        patient(Duration::from_secs(1)),
        None,
        || Some(Arc::clone(&gate)),
        || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(RouteHandle::synthetic(1)) }
        },
    ));
    assert!(
        futures::poll!(&mut waiter).is_pending(),
        "the waiter waits at the gate"
    );
    // The deadline passes and then the gate opens, all before the waiter runs again.
    time::advance(Duration::from_secs(2)).await;
    probe.report(&Ok(RouteHandle::synthetic(2)));
    assert!(!gate.is_closed());
    let result = waiter.await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "no attempt after the deadline"
    );
    assert!(matches!(
        result,
        Err(RouteOpenError::Conn(ConnError::Rejected(message))) if message.code() == TARGET_UNAVAILABLE
    ));
}

fn tool_dispatch_wait() -> OutageWait {
    OutageWait {
        patience: Duration::from_secs(90),
        max_interval: ROUTE_OPEN_RETRY_MAX_INTERVAL,
        deadline: None,
    }
}

/// A tool open checks the gate and finds it open, and another open's availability refusal
/// closes it before this open is admitted. The open meets the same outage a moment later,
/// so it waits it out like an open that found the gate closed: its patience starts there,
/// it takes the gate's next probe turn, and it opens. It is not refused without waiting.
#[tokio::test(start_paused = true)]
async fn an_outage_wait_that_meets_a_closing_gate_waits_like_one_that_met_it_closed() {
    let gate = Arc::new(ModuleGate::with_jitter(
        whole_envelope,
        CancellationToken::new(),
    ));
    // Another open's refusal lands just as this open reaches the gate.
    *gate.admit_hook.lock().unwrap() = Some(Box::new(|gate: &ModuleGate| {
        gate.observe(&Err(refused(TARGET_UNAVAILABLE)), 0, None);
    }));
    let start = time::Instant::now();
    let attempts_at = Mutex::new(Vec::new());
    let result = retry_route_open_waiting_out_outage(
        tool_dispatch_wait(),
        None,
        None,
        || Some(Arc::clone(&gate)),
        || {
            attempts_at.lock().unwrap().push(start.elapsed());
            async { Ok(RouteHandle::synthetic(4)) }
        },
    )
    .await;
    assert_eq!(
        result
            .expect("the open waits for the gate instead of being refused")
            .channel,
        4
    );
    assert_eq!(
        attempts_at.into_inner().unwrap(),
        [Duration::from_millis(100)],
        "one attempt, on the gate's first probe turn"
    );
    assert!(!gate.is_closed(), "the probe's route reopened the gate");
}

/// A tool open that joins a gate already closed by another open's refusal, and whose own
/// probe is still in flight when its patience runs out, ends with the gate's refusal: the
/// module's code and detail, proven undelivered. Not a bare transport failure, which would
/// lose the module and the code the caller reports.
#[tokio::test(start_paused = true)]
async fn an_outage_wait_that_joined_a_closed_gate_ends_with_the_gates_refusal() {
    let gate = closed_gate(CancellationToken::new());
    let start = time::Instant::now();
    let calls = AtomicUsize::new(0);
    let result = retry_route_open_waiting_out_outage(
        tool_dispatch_wait(),
        None,
        None,
        || Some(Arc::clone(&gate)),
        || {
            calls.fetch_add(1, Ordering::SeqCst);
            // This open's probe never answers.
            std::future::pending::<ConnResult<RouteHandle>>()
        },
    )
    .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "its own probe went out and was still in flight"
    );
    assert_eq!(
        start.elapsed(),
        Duration::from_secs(90),
        "the patience ran from joining the closed gate"
    );
    match result {
        Err(RouteOpenError::Conn(ConnError::Rejected(message))) => {
            assert_eq!(message.code(), TARGET_UNAVAILABLE);
            assert_eq!(message.detail(), "not now");
        }
        other => panic!("expected the gate's availability refusal, got {other:?}"),
    }
}

/// Fifty opens for fifty DIFFERENT routes to one module stopped for ten seconds, arriving
/// one every 10 ms: the module sees the first open and then one probe per backoff tick in
/// total, not one per route, and every route opens once the module is back.
#[tokio::test(start_paused = true)]
async fn fifty_routes_to_a_stopped_module_make_one_probe_per_tick_between_them() {
    let start = time::Instant::now();
    let up_at = start + Duration::from_secs(10);
    let started_at = Mutex::new(Vec::new());
    let attempt = || {
        let now = time::Instant::now();
        started_at.lock().unwrap().push(now);
        async move {
            time::sleep(Duration::from_millis(5)).await;
            if now >= up_at {
                Ok(RouteHandle::synthetic(7))
            } else {
                Err(refused(TARGET_UNAVAILABLE))
            }
        }
    };
    let gate = Arc::new(ModuleGate::with_jitter(
        whole_envelope,
        CancellationToken::new(),
    ));
    let gate_for = || Some(Arc::clone(&gate));
    let results = futures::future::join_all((0..50u64).map(|n| async move {
        time::sleep(Duration::from_millis(10 * n)).await;
        retry_route_open(patient(Duration::from_secs(30)), None, gate_for, attempt).await
    }))
    .await;
    let started_at = started_at.into_inner().unwrap();
    let while_down: Vec<_> = started_at
        .iter()
        .filter(|at| **at < up_at)
        .map(|at| *at - start)
        .collect();
    let after_return = started_at.iter().filter(|at| **at >= up_at).count();
    // Each failed open takes 5ms; the next shared probe starts after its backoff.
    assert_eq!(
        while_down,
        [0, 105, 310, 715, 1520, 3125, 6330].map(Duration::from_millis)
    );
    assert_eq!(
        after_return, 50,
        "each route makes one open of its own on return"
    );
    for result in results {
        assert_eq!(
            result
                .expect("every route opens once the module is back")
                .channel,
            7
        );
    }
    assert!(!gate.is_closed());
}
