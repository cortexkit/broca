async fn followup_congested_reader_cleanup(cleanup: u8) {
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
        ConnToken(77),
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
            conn: ConnToken(77),
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
