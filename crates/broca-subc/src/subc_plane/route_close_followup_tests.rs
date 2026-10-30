// Focused ownership regressions use the real connection's permanently full writer queue.
fn followup_congested_plane() -> (
    Arc<SubcConnection>,
    SubcToolPlane,
    RouteHandle,
    crate::connection::InProcessModuleEnd,
) {
    let (conn, route, module) = SubcConnection::in_process_full_writer_for_test(7, 3);
    let conn = Arc::new(conn);
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    (conn, plane, route, module)
}

async fn followup_advance(seconds: u64) {
    tokio::time::advance(Duration::from_secs(seconds)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn followup_permanent_full_writer_has_a_counted_age_giveup() {
    let (_conn, plane, route, _module) = followup_congested_plane();
    assert!(!plane.closer.close(route).await);
    assert_eq!(plane.closer.pending(), 1);
    for _ in 0..26 {
        followup_advance(1).await;
    }
    assert_eq!(
        plane.closer.pending(),
        0,
        "permanent congestion must not retry forever"
    );
    assert_eq!(
        plane.tool_routes_health(&[])["goodbye_giveups_since_boot"],
        1
    );
}

#[tokio::test(start_paused = true)]
async fn followup_connection_loss_clears_retry_ownership_synchronously() {
    let (conn, plane, route, _module) = followup_congested_plane();
    assert!(!plane.closer.close(route).await);
    assert_eq!(plane.closer.pending(), 1);
    conn.deactivate_for_test();
    assert_eq!(
        plane.closer.pending(),
        0,
        "loss must clear ownership before a later retry tick"
    );
}

#[tokio::test(start_paused = true)]
async fn followup_retry_task_does_not_keep_the_connection_alive() {
    let (conn, plane, route, _module) = followup_congested_plane();
    assert!(!plane.closer.close(route).await);
    let weak = Arc::downgrade(&conn);
    drop(plane);
    drop(conn);
    assert!(
        weak.upgrade().is_none(),
        "close-only retries cannot retain the connection owner"
    );
}

#[tokio::test(start_paused = true)]
async fn followup_retry_backlog_is_capped_and_overflow_is_counted() {
    let (conn, plane, route, _module) = followup_congested_plane();
    let handles: Vec<_> = (1..=1025)
        .map(|channel| {
            let mut handle = route;
            handle.channel = channel;
            conn.install_route_for_test(handle);
            handle
        })
        .collect();
    let results = join_all(handles.into_iter().map(|handle| plane.closer.close(handle))).await;
    assert!(results.iter().all(|closed| !closed));
    assert_eq!(
        plane.closer.pending(),
        1024,
        "backlog must have an explicit finite cap"
    );
    assert_eq!(
        plane.tool_routes_health(&[])["goodbye_retry_overflows_since_boot"],
        1
    );
}

#[tokio::test(start_paused = true)]
async fn followup_retry_pass_uses_one_parallel_deadline() {
    let (conn, plane, route, _module) = followup_congested_plane();
    let mut second = route;
    second.channel = 8;
    conn.install_route_for_test(second);
    let (first, second) = tokio::join!(plane.closer.close(route), plane.closer.close(second));
    assert!(!first && !second);
    assert_eq!(plane.closer.failures(), 2);
    followup_advance(1).await;
    followup_advance(5).await;
    assert_eq!(
        plane.closer.failures(),
        4,
        "both retries must finish in one five-second pass"
    );
}

#[tokio::test]
async fn followup_stale_refusal_does_not_orphan_a_sibling_replacement() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![
            vec![catalog_entry("aft", &["echo"])],
            vec![catalog_entry("aft", &["echo"])],
        ],
        vec![
            Err(ConnError::Rejected(crate::connection::decode_error_body(
                br#"{"code":"module_reloading","message":"draining endpoint"}"#,
            ))),
            successful_tool_reply("fresh"),
        ],
    );
    let plane = Arc::new(SubcToolPlane::with_connection(conn.clone(), "aft"));
    let ctx = batch_ctx("stale-refusal");
    let call = call("echo");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &ToolManifest::from_tools(vec![]),
        )
        .await
        .unwrap();
    let old =
        plane.routes.lock().unwrap()[&SubcToolPlane::route_key(&ctx.bind, None, "aft")].handle;
    let gate = RouteOpenGate::new();
    *conn.send_gate.lock().unwrap() = Some(gate.clone());
    let dispatch = {
        let plane = plane.clone();
        let ctx = ctx.clone();
        let call = call.clone();
        tokio::spawn(async move { plane.dispatch(&ctx, &call).await })
    };
    broca_test_support::wait_for(
        "old call reaches refusal gate",
        gate.entered.notified(),
        || {
            format!(
                "opens={}, calls={}, finished={}",
                conn.route_open_calls.load(Ordering::SeqCst),
                conn.tool_calls.load(Ordering::SeqCst),
                dispatch.is_finished()
            )
        },
    )
    .await;
    conn.close(old);
    plane.invalidate_route(&ctx.bind);
    broca_test_support::wait_for(
        "reopened generation warms",
        plane.warm_bind(&ctx.bind, Duration::from_secs(1)),
        || format!("opens={}", conn.route_open_calls.load(Ordering::SeqCst)),
    )
    .await
    .unwrap();
    let replacement = plane.cached_route_channel(&ctx.bind).unwrap();
    gate.release.notify_one();
    let result = dispatch.await.unwrap();
    assert!(
        matches!(&result, DispatchResult::Completed(result) if !result.is_error),
        "{result:?}"
    );
    assert_eq!(
        plane.cached_route_channel(&ctx.bind),
        Some(replacement),
        "the old refusal cannot evict the sibling's fresh provider generation"
    );
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn followup_late_open_real_caller_preserves_the_reopened_generation() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![Vec::new(), Vec::new()],
        vec![successful_tool_reply("fresh")],
    );
    let open_gate = RouteOpenGate::new();
    conn.script_route_open_sequence(
        "aft",
        vec![
            RouteOpenScript {
                gate: Some(open_gate.clone()),
                ..Default::default()
            },
            RouteOpenScript::default(),
        ],
    );
    let plane = Arc::new(SubcToolPlane::with_connection(conn.clone(), "aft"));
    let ctx = batch_ctx("real-late-open");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    *plane.after_cache_gate.lock().unwrap() = Some(RouteCacheGate {
        entered: entered.clone(),
        release: release.clone(),
    });
    let old = {
        let plane = plane.clone();
        let bind = ctx.bind.clone();
        tokio::spawn(async move { plane.warm_bind(&bind, Duration::from_secs(10)).await })
    };
    broca_test_support::wait_for(
        "old route open reaches network gate",
        open_gate.entered.notified(),
        || {
            format!(
                "opens={}, finished={}",
                conn.route_open_calls.load(Ordering::SeqCst),
                old.is_finished()
            )
        },
    )
    .await;
    open_gate.release.notify_one();
    broca_test_support::wait_for(
        "old caller reaches post-cache gate",
        entered.notified(),
        || {
            format!(
                "opens={}, finished={}",
                conn.route_open_calls.load(Ordering::SeqCst),
                old.is_finished()
            )
        },
    )
    .await;
    plane.close_session_routes(&ctx.bind).await;
    broca_test_support::wait_for(
        "reopened generation warms",
        plane.warm_bind(&ctx.bind, Duration::from_secs(1)),
        || format!("opens={}", conn.route_open_calls.load(Ordering::SeqCst)),
    )
    .await
    .unwrap();
    let replacement = plane.cached_route_channel(&ctx.bind).unwrap();
    release.notify_one();
    assert!(matches!(
        old.await.unwrap(),
        Err(ToolPlaneError::Unavailable(_))
    ));
    assert_eq!(
        plane.cached_route_channel(&ctx.bind),
        Some(replacement),
        "real cancelled-open cleanup must preserve the reopened generation"
    );
    let call = call("echo");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &ToolManifest::from_tools(vec![]),
        )
        .await
        .unwrap();
    assert_completed_success(plane.dispatch(&ctx, &call).await);
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        plane.tool_routes_health(std::slice::from_ref(&ctx.bind))["routes_for_closed_sessions"],
        0
    );
}
