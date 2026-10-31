use super::*;

const CONTROL_TEXT: &str = "worker-control-message";

#[derive(Clone, Copy, Debug)]
enum Control {
    Queue,
    Steer,
}

fn send(control: Control) -> (LineageCmd, oneshot::Receiver<SendOutcome>) {
    let request = serde_json::from_value(serde_json::json!({
        "model":{"provider_module_id":"fake","model_id":"fake"},
        "input":[{"role":"user","content":[{"type":"text","text":CONTROL_TEXT}]}]
    }))
    .unwrap();
    let (reply, receive) = oneshot::channel();
    (
        LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            conversation_key: None,
            title: None,
            run_id: RunId("nudge-run".into()),
            request: Box::new(request),
            kind: RunKind::FreshOrResume,
            key: SendKey::SendId("worker-control".into()),
            skip_materialization: false,
            delivery: match control {
                Control::Queue => broca_protocol::Delivery::Queue,
                Control::Steer => broca_protocol::Delivery::Steer,
            },
            plan: None,
            order: None,
            reply,
        },
        receive,
    )
}

async fn settle(actor: &mut LineageActor<ProbeWal>) {
    loop {
        let cmd = match actor.terminal_rx.try_recv() {
            Ok(cmd) => cmd,
            Err(_) if matches!(actor.state, RunState::Idle) => break,
            Err(_) => broca_test_support::event_timeout(
                "run-control terminal",
                actor.terminal_rx.recv(),
                || "the resumed run did not publish a terminal".into(),
            )
            .await
            .expect("the core loop terminates")
            .unwrap(),
        };
        assert!(!actor.handle_cmd(cmd).await);
    }
}

fn finishes(
    records: &[broca_wal::FramedRecord],
    target: &str,
) -> Vec<(broca_protocol::RunFinishReason, Option<String>)> {
    let mut current = None;
    let mut finished = Vec::new();
    for frame in records {
        match &frame.record {
            WalRecord::RunStarted { run_id, .. } => current = Some(run_id.0.as_str()),
            WalRecord::RunFinished { reason, error, .. } if current == Some(target) => {
                finished.push((
                    *reason,
                    error.as_ref().and_then(|error| error.provider_code.clone()),
                ));
            }
            _ => {}
        }
    }
    finished
}

async fn exercise_control(control: Control, media_on: bool) {
    let seed = if media_on {
        Seed::RunControlRestartPaused
    } else {
        Seed::RunControlOffRestartPaused
    };
    let mut fixture = Fixture::new(seed, true).await;
    fixture
        .executor
        .media_probe
        .store(if media_on { 1 } else { 2 }, Ordering::SeqCst);
    fixture
        .executor
        .gate_first_probe
        .store(true, Ordering::SeqCst);
    assert!(!fixture.actor.writer.as_ref().unwrap().is_overhaul());
    broca_core::run::seed_display_placement(
        fixture.actor.writer.as_ref().unwrap(),
        fixture.actor.prior_snapshot.as_ref().unwrap(),
        fixture.wal_store.as_ref(),
        None,
        &fixture.actor.key,
    );
    let executor = fixture.executor.clone();
    let tx = fixture.actor.self_tx.clone();
    let store = fixture.wal_store.clone();
    let key = fixture.actor.key.clone();
    let interleave = async move {
        executor.entered.notified().await;
        let (cmd, reply) = send(control);
        let status = queue_with_probe(&tx, cmd).await;
        status.await.unwrap();
        let own_reply = reply.await.unwrap();
        let records = store.read_records(&key).unwrap();
        println!("{control:?}, media={media_on}: own reply={own_reply:?}; head during held initialization={:?}", records.last().map(|frame| frame.seq));
        executor.release.notify_one();
        own_reply
    };
    let (recovery, own_reply) = broca_test_support::event_timeout(
        "boot recovery with worker control",
        async { tokio::join!(Box::pin(fixture.actor.recover_durable_work()), interleave) },
        || format!("{control:?}: recovery did not finish"),
    )
    .await
    .expect("the held initialization is released after the control is accepted");
    println!("{control:?}, media={media_on}: recovery reply={recovery:?}");
    assert!(recovery.is_ok());
    match (control, own_reply) {
        (Control::Queue, SendOutcome::Pending { .. }) => {}
        (Control::Steer, SendOutcome::Active { run_id, .. }) => assert_eq!(run_id.0, "prior"),
        (_, other) => panic!("{control:?}: unexpected own reply {other:?}"),
    }
    settle(&mut fixture.actor).await;
    let records = fixture.wal_store.read_records(&fixture.actor.key).unwrap();
    let original = finishes(&records, "prior");
    let requests = fixture.executor.requests.lock().unwrap().clone();
    let replay = fixture.executor.replay_requests.lock().unwrap().clone();
    let deliveries = requests
        .iter()
        .map(|bytes| String::from_utf8_lossy(bytes).matches(CONTROL_TEXT).count())
        .sum::<usize>();
    println!("{control:?}, media={media_on}: original finishes={original:?}; requests={}; deliveries={deliveries}; exact full-replay bytes={}", requests.len(), requests == replay);
    for (index, (actual, expected)) in requests.iter().zip(&replay).enumerate() {
        println!(
            "{control:?}, media={media_on}, request {index}: actual={}; full replay={}",
            String::from_utf8_lossy(actual),
            String::from_utf8_lossy(expected)
        );
    }
    let (retry, retry_reply) = send(control);
    assert!(!fixture.actor.handle_cmd(retry).await);
    println!(
        "{control:?}, media={media_on}: same-key caller retry={:?}",
        retry_reply.await.unwrap()
    );
    settle(&mut fixture.actor).await;
    assert_eq!(
        *fixture.executor.requests.lock().unwrap(),
        requests,
        "the same-key retry must not deliver again"
    );
    assert_eq!(original, [(broca_protocol::RunFinishReason::Completed, None)], "{control:?}: restart must not terminate the worker because its accepted control advanced the head");
    assert_eq!(
        deliveries, 1,
        "{control:?}: the accepted control must be delivered exactly once without caller retry"
    );
    assert_eq!(
        requests, replay,
        "{control:?}: every request must equal full replay"
    );
    fixture.close().await;
}

#[tokio::test]
async fn old_media_off_worker_controls_match_full_replay_exactly_once() {
    for control in [Control::Queue, Control::Steer] {
        exercise_control(control, false).await;
    }
}

#[tokio::test]
async fn crash_after_accepted_steer_restarts_with_one_replayed_delivery() {
    let mut fixture = Fixture::new(Seed::RunControlRestartPaused, true).await;
    fixture.executor.media_probe.store(1, Ordering::SeqCst);
    fixture
        .executor
        .gate_first_probe
        .store(true, Ordering::SeqCst);
    broca_core::run::seed_display_placement(
        fixture.actor.writer.as_ref().unwrap(),
        fixture.actor.prior_snapshot.as_ref().unwrap(),
        fixture.wal_store.as_ref(),
        None,
        &fixture.actor.key,
    );
    let tx = fixture.actor.self_tx.clone();
    let executor = fixture.executor.clone();
    let accept = async move {
        executor.entered.notified().await;
        let (cmd, reply) = send(Control::Steer);
        let status = queue_with_probe(&tx, cmd).await;
        status.await.unwrap();
        assert!(matches!(reply.await.unwrap(), SendOutcome::Active { .. }));
    };
    broca_test_support::event_timeout("steer accepted before crash", async {
        let mut recovery = Box::pin(fixture.actor.recover_durable_work());
        tokio::select! {
            () = accept => {},
            result = &mut recovery => panic!("the held admission ended before the crash: {result:?}"),
        }
    }, || "the steer was not accepted".into()).await.expect("the steer is durable while admission remains held");
    assert!(fixture.executor.requests.lock().unwrap().is_empty());
    // Lose the suspended executor and inbox without a terminal seal, as a process crash does.
    fixture.actor.stop_active_run().await;
    assert!(fixture.actor.shutdown_writer().await.clean);
    drop(fixture.actor.lease.take());
    let before = fixture.wal_store.read_records(&fixture.actor.key).unwrap();
    assert!(finishes(&before, "prior").is_empty());
    assert_eq!(
        before
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::Steered { .. }))
            .count(),
        1
    );
    let root = fixture.root.clone();
    drop(fixture);

    let mut restarted = Fixture::reopen(root, true).await;
    restarted.executor.media_probe.store(1, Ordering::SeqCst);
    restarted.executor.release.notify_one();
    broca_core::run::seed_display_placement(
        restarted.actor.writer.as_ref().unwrap(),
        restarted.actor.prior_snapshot.as_ref().unwrap(),
        restarted.wal_store.as_ref(),
        None,
        &restarted.actor.key,
    );
    Box::pin(restarted.actor.recover_durable_work())
        .await
        .unwrap();
    settle(&mut restarted.actor).await;
    let after = restarted
        .wal_store
        .read_records(&restarted.actor.key)
        .unwrap();
    assert_eq!(
        finishes(&after, "prior"),
        [(broca_protocol::RunFinishReason::Completed, None)]
    );
    {
        let requests = restarted.executor.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            String::from_utf8_lossy(&requests[0])
                .matches(CONTROL_TEXT)
                .count(),
            1
        );
        assert_eq!(
            *requests,
            *restarted.executor.replay_requests.lock().unwrap()
        );
    }
    restarted.close().await;
}
