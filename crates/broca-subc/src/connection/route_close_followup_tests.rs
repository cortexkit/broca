async fn followup_congested_reader_cleanup(cleanup: u8) {
    let conn_token = ConnToken(NEXT_CONNECTION_TOKEN.fetch_add(1, Ordering::Relaxed));
    let pending: PendingMap = Arc::default();
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
    let (_live, mut reply_rx) =
        PendingRegistration::new(Arc::clone(&pending), (0, 0, 2), false, None).unwrap();
    let (mut abandoned, open_rx) =
        PendingRegistration::new(Arc::clone(&pending), (0, 0, 1), true, None).unwrap();
    drop(open_rx);
    if cleanup == 1 {
        abandoned.arm_late_route_open_cleanup();
        drop(abandoned);
    }
    let trigger = Frame::build(
        FrameType::Response,
        control_flags(),
        0,
        0,
        1,
        serde_json::to_vec(&ClientControlResponse::RouteOpen {
            route_channel: 9,
            route_epoch: 4,
        })
        .unwrap(),
    )
    .unwrap();
    let (write_tx, mut write_rx) = mpsc::channel(1);
    write_tx.try_send(frame(FrameType::Ping, 0, 0, 99)).unwrap();
    let conn = test_connection(
        write_tx.clone(),
        pending.clone(),
        routes.clone(),
        conn_token,
        100,
    );
    let shared = Arc::clone(&conn.shared);
    let shutdown = shared.current.lock().unwrap().runtime.shutdown.clone();
    let plane = crate::subc_plane::SubcToolPlane::new(conn, "aft");
    let (mut client, server) = tokio::io::duplex(4096);
    let reader = tokio::spawn(reader_task(
        server,
        pending.clone(),
        routes.clone(),
        write_tx,
        Some(ReaderGeneration {
            shared: Arc::downgrade(&shared),
            conn: conn_token,
            shutdown,
            test_gate: None,
        }),
    ));
    write_frame(&mut client, &trigger).await.unwrap();
    write_frame(&mut client, &frame(FrameType::Response, 0, 0, 2))
        .await
        .unwrap();
    let reply = broca_test_support::wait_for(
        "unrelated ingress during full egress",
        reply_rx.recv(),
        || plane.tool_routes_health(&[]).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(reply.header.corr, 2);
    assert_eq!(
        plane.tool_routes_health(&[])["pending_goodbyes"],
        1,
        "a full reader cleanup must transfer its sole route ownership"
    );
    assert!(!routes.lock().unwrap().contains_key(&9));
    assert_eq!(
        write_rx.try_recv().unwrap().header.corr,
        99,
        "ingress never drained egress to make progress"
    );
    let goodbye =
        broca_test_support::wait_for("deferred late-open Goodbye", write_rx.recv(), || {
            plane.tool_routes_health(&[]).to_string()
        })
        .await
        .unwrap();
    assert_eq!(goodbye.header.ty, FrameType::Goodbye);
    assert_eq!((goodbye.header.channel, goodbye.header.epoch), (9, 4));
    assert_eq!(plane.tool_routes_health(&[])["pending_goodbyes"], 0);
    drop(client);
    reader.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn followup_full_egress_late_open_retains_goodbye_until_enqueued() {
    followup_congested_reader_cleanup(1).await;
}

#[tokio::test(start_paused = true)]
async fn followup_full_egress_abandoned_open_retains_goodbye_until_enqueued() {
    followup_congested_reader_cleanup(2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_reader_full_handoff_cannot_reenter_retired_generation() {
    let conn_token = ConnToken(NEXT_CONNECTION_TOKEN.fetch_add(1, Ordering::Relaxed));
    let pending: PendingMap = Arc::default();
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (mut registration, rx) =
        PendingRegistration::new(Arc::clone(&pending), (0, 0, 1), true, None).unwrap();
    drop(rx);
    registration.arm_late_route_open_cleanup();
    drop(registration);
    let (write_tx, mut write_rx) = mpsc::channel(1);
    write_tx.try_send(frame(FrameType::Ping, 0, 0, 99)).unwrap();
    let conn = test_connection(
        write_tx.clone(),
        pending.clone(),
        routes.clone(),
        conn_token,
        100,
    );
    let shared = Arc::clone(&conn.shared);
    let shutdown = shared.current.lock().unwrap().runtime.shutdown.clone();
    let plane = crate::subc_plane::SubcToolPlane::new(conn, "aft");
    let prepared = Arc::new(tokio::sync::Notify::new());
    let handed_off = Arc::new(tokio::sync::Notify::new());
    let admitted = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let (proceed, receiver) = std::sync::mpsc::sync_channel(0);
    let receiver = Mutex::new(receiver);
    let hook_prepared = Arc::clone(&prepared);
    let hook_handed_off = Arc::clone(&handed_off);
    let hook_admitted = Arc::clone(&admitted);
    *shared.late_goodbye_handoff.lock().unwrap() = Some(Arc::new(move |stage| match stage {
        LateGoodbyeHandoffStage::Prepared => {
            hook_prepared.notify_one();
            receiver
                .lock()
                .unwrap()
                .recv_timeout(broca_test_support::EVENT_HANG_GUARD)
                .expect("test releases the reader handoff after deactivation");
        }
        LateGoodbyeHandoffStage::Admitted(value) => {
            hook_admitted.store(value, Ordering::SeqCst);
            hook_handed_off.notify_one();
        }
    }));
    let (mut client, server) = tokio::io::duplex(4096);
    let reader = tokio::spawn(reader_task(
        server,
        pending,
        routes.clone(),
        write_tx,
        Some(ReaderGeneration {
            shared: Arc::downgrade(&shared),
            conn: conn_token,
            shutdown,
            test_gate: None,
        }),
    ));
    let response = Frame::build(
        FrameType::Response,
        control_flags(),
        0,
        0,
        1,
        serde_json::to_vec(&ClientControlResponse::RouteOpen {
            route_channel: 9,
            route_epoch: 4,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut client, &response).await.unwrap();
    broca_test_support::wait_for(
        "reader prepares a Full cleanup",
        prepared.notified(),
        || plane.tool_routes_health(&[]).to_string(),
    )
    .await;
    {
        let mut state = shared.current.lock().unwrap();
        assert!(state
            .runtime
            .closing_routes
            .lock()
            .unwrap()
            .contains_key(&(9, 4)));
        assert!(deactivate_locked(&mut state));
    }
    assert_eq!(plane.tool_routes_health(&[])["pending_goodbyes"], 0);
    proceed.send(()).unwrap();
    broca_test_support::wait_for(
        "reader finishes retired handoff",
        handed_off.notified(),
        || plane.tool_routes_health(&[]).to_string(),
    )
    .await;
    assert!(
        !admitted.load(Ordering::SeqCst),
        "retired reader handoff was admitted after the loss hook"
    );
    assert_eq!(plane.tool_routes_health(&[])["pending_goodbyes"], 0);
    assert!(routes.lock().unwrap().is_empty());
    assert_eq!(write_rx.try_recv().unwrap().header.ty, FrameType::Ping);
    assert!(write_rx.try_recv().is_err());
    drop(client);
    broca_test_support::wait_for("retired reader exits", reader, || {
        "reader still running".into()
    })
    .await
    .unwrap();
    let (replacement, _replacement_writer) = new_runtime();
    {
        let mut state = shared.current.lock().unwrap();
        *state = GenerationState::new(Arc::clone(&replacement));
    }
    let closer = route_closer_for(&shared);
    assert!(!closer.enqueue_failed(RouteHandle::new(9, 4, conn_token)));
    replacement.route_epochs.lock().unwrap().insert(9, 4);
    assert!(
        closer.enqueue_failed(RouteHandle::new(9, 4, replacement.conn)),
        "a replacement generation must still admit its own cleanup"
    );
}
