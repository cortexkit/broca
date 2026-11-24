// The queue probes inject already-open, role-less routes. Seed their matching catalog
// snapshot too, so only the enqueue boundary can consume the scripted transport.
fn cached_plane(conn: SubcConnection) -> SubcToolPlane {
    let plane = SubcToolPlane::new(conn, "aft");
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    plane
}
#[tokio::test]
async fn ordinary_send_certainty_tracks_the_real_enqueue_boundary() {
    for pre_enqueue in [true, false] {
        let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
        if pre_enqueue {
            module.close_writer_for_test();
        }
        let plane = cached_plane(conn);
        let ctx = BatchCtx {
            media_capture: None,
            batch_id: BatchId("ordinary".into()),
            bind: BindIdentity::new("/project", "test", "ordinary"),
            run_id: broca_protocol::RunId("run".into()),
            scope: None,
        };
        plane.routes.lock().unwrap().insert(
            SubcToolPlane::route_key(&ctx.bind, None, "aft"),
            RouteState {
                handle: route,
                sem: Arc::new(Semaphore::new(1)),
                stamp: None,
                last_used: Instant::now(),
            },
        );
        let call = ToolCall {
            tool_call_id: broca_protocol::ToolCallId("ordinary".into()),
            tool_name: "edit".into(),
            input: serde_json::json!({}).into(),
            thought_signature: None,
            provider_state: None,
        };
        let result = if pre_enqueue {
            plane.dispatch(&ctx, &call).await
        } else {
            let (result, ()) = tokio::join!(plane.dispatch(&ctx, &call), async {
                let request = module.next_request().await;
                assert!(module.reply(&request, b"invalid-json".to_vec()));
            });
            result
        };
        if pre_enqueue {
            assert!(
                matches!(result, DispatchResult::NotSent { .. }),
                "{result:?}"
            );
        } else {
            assert!(
                matches!(result, DispatchResult::SentButOutcomeUnknown { .. }),
                "{result:?}"
            );
        }
        assert_eq!(module.pending_count_for_test(), 0);
    }
}

use super::*;
use futures::FutureExt;

#[tokio::test]
async fn approved_setup_failures_are_not_sent() {
    for setup in ["raw-body", "registration", "frame-build"] {
        let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
        if setup == "registration" {
            module.fill_pending_for_test();
        }
        const {
            assert!(
                subc_protocol::MAX_FRAME_BODY_LEN <= 64 * 1024 * 1024,
                "oversized-frame probe must stay bounded"
            );
        }
        let oversized = if setup == "frame-build" {
            "x".repeat(subc_protocol::MAX_FRAME_BODY_LEN as usize + 1)
        } else {
            "edit".into()
        };
        let args = if setup == "raw-body" { "{" } else { "{}" };
        let result = conn
            .tool_call_raw_yieldable_guarded(
                &route,
                &oversized,
                args,
                Some("approved"),
                Duration::from_millis(20),
                false,
                None,
                Some("call-key"),
                None,
                &Default::default(),
                None,
            )
            .await;
        assert!(
            matches!(
                SendAttempt::approved_reply(result),
                SendAttempt::NotSent { .. }
            ),
            "{setup} failed before enqueue and must be recorded not-sent"
        );
        assert!(
            module.take_written_for_test().is_empty(),
            "{setup} must not enqueue a request or cancel"
        );
    }
}

#[tokio::test]
async fn approved_failure_after_real_push_remains_unknown() {
    let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
    let plane = cached_plane(conn);
    let ctx = BatchCtx {
        media_capture: None,
        batch_id: broca_protocol::BatchId("after-push".into()),
        bind: BindIdentity::new("/project", "test", "after-push"),
        run_id: broca_protocol::RunId("run".into()),
        scope: None,
    };
    plane.routes.lock().unwrap().insert(
        SubcToolPlane::route_key(&ctx.bind, None, "aft"),
        RouteState {
            handle: route,
            sem: Arc::new(Semaphore::new(1)),
            stamp: None,
            last_used: Instant::now(),
        },
    );
    let call = ToolCall {
        tool_call_id: broca_protocol::ToolCallId("approved".into()),
        tool_name: "edit".into(),
        input: serde_json::json!({}).into(),
        thought_signature: None,
        provider_state: None,
    };
    let signals = Default::default();
    let (result, ()) = tokio::join!(
        plane.dispatch_approved(&ctx, &call, "{}", "call-key", &signals),
        async {
            let request = module.next_request().await;
            assert!(module.reply(&request, b"invalid-json".to_vec()));
        }
    );
    assert!(
        matches!(result, DispatchResult::SentButOutcomeUnknown { .. }),
        "a malformed provider reply is after the real push: {result:?}"
    );
    assert!(module.take_written_for_test().is_empty());
}

#[tokio::test]
async fn approved_closed_writer_queue_is_not_sent() {
    let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
    module.close_writer_for_test();
    let plane = cached_plane(conn);
    let ctx = BatchCtx {
        media_capture: None,
        batch_id: broca_protocol::BatchId("closed-queue".into()),
        bind: BindIdentity::new("/project", "test", "closed-queue"),
        run_id: broca_protocol::RunId("run".into()),
        scope: None,
    };
    plane.routes.lock().unwrap().insert(
        SubcToolPlane::route_key(&ctx.bind, None, "aft"),
        RouteState {
            handle: route,
            sem: Arc::new(Semaphore::new(1)),
            stamp: None,
            last_used: Instant::now(),
        },
    );
    let call = ToolCall {
        tool_call_id: broca_protocol::ToolCallId("approved".into()),
        tool_name: "edit".into(),
        input: serde_json::json!({}).into(),
        thought_signature: None,
        provider_state: None,
    };
    let result = plane
        .dispatch_approved(&ctx, &call, "{}", "call-key", &Default::default())
        .await;
    assert!(
        matches!(result, DispatchResult::NotSent { ref reason } if reason == "subc transport: writer task closed before enqueue"),
        "a failed reservation never commits a send: {result:?}"
    );
    assert!(module.take_written_for_test().is_empty());
    assert_eq!(module.pending_count_for_test(), 0);
}

#[tokio::test]
async fn approved_cancel_and_stop_while_writer_queue_is_full_are_not_sent() {
    for cancel in [true, false] {
        let (conn, route, mut module) = SubcConnection::in_process_full_writer_for_test(5, 3);
        let plane = cached_plane(conn);
        let ctx = BatchCtx {
            media_capture: None,
            batch_id: broca_protocol::BatchId("queue".into()),
            bind: BindIdentity::new("/project", "test", "queue"),
            run_id: broca_protocol::RunId("run".into()),
            scope: None,
        };
        plane.routes.lock().unwrap().insert(
            SubcToolPlane::route_key(&ctx.bind, None, "aft"),
            RouteState {
                handle: route,
                sem: Arc::new(Semaphore::new(1)),
                stamp: None,
                last_used: Instant::now(),
            },
        );
        let call = ToolCall {
            tool_call_id: broca_protocol::ToolCallId("approved".into()),
            tool_name: "edit".into(),
            input: serde_json::json!({}).into(),
            thought_signature: None,
            provider_state: None,
        };
        let token = CancellationToken::new();
        let signals = broca_core::tool_plane::DispatchSignals::new(
            if cancel { Some(&token) } else { None },
            if cancel { None } else { Some(&token) },
        );
        let mut dispatch =
            Box::pin(plane.dispatch_approved(&ctx, &call, "{}", "call-key", &signals));
        assert!(dispatch.as_mut().now_or_never().is_none());
        assert_eq!(
            module.pending_count_for_test(),
            1,
            "the real tool request must already be waiting for writer capacity"
        );
        token.cancel();
        let result = tokio::time::timeout(Duration::from_millis(250), dispatch)
            .await
            .expect("cancelled approved enqueue must not wait for writer capacity");
        assert!(
            matches!(result, DispatchResult::NotSent { .. }),
            "pre-enqueue interruption is typed not-sent, never unknown: {result:?}"
        );
        let frames = module.take_written_for_test();
        assert_eq!(frames.len(), 256);
        assert!(
            frames
                .iter()
                .all(|frame| frame.header.ty == subc_protocol::FrameType::Ping),
            "no tool request or provider cancel may be enqueued before the send boundary"
        );
        assert_eq!(module.pending_count_for_test(), 0);
        tokio::task::yield_now().await;
        assert!(module.take_written_for_test().is_empty());
    }
}

/// An ordinary (not approved) call goes through the real writer queue with its run's
/// signals: a cancel or stop that trips while the request waits for writer capacity, after
/// the open and the permit, leaves it unsent, and no request frame is ever written.
#[tokio::test]
async fn ordinary_cancel_and_stop_while_writer_queue_is_full_are_not_sent() {
    for cancel in [true, false] {
        let (conn, route, mut module) = SubcConnection::in_process_full_writer_for_test(5, 3);
        let plane = cached_plane(conn);
        let ctx = BatchCtx {
            media_capture: None,
            batch_id: broca_protocol::BatchId("ordinary-queue".into()),
            bind: BindIdentity::new("/project", "test", "ordinary-queue"),
            run_id: broca_protocol::RunId("run".into()),
            scope: None,
        };
        plane.routes.lock().unwrap().insert(
            SubcToolPlane::route_key(&ctx.bind, None, "aft"),
            RouteState {
                handle: route,
                sem: Arc::new(Semaphore::new(1)),
                stamp: None,
                last_used: Instant::now(),
            },
        );
        let call = ToolCall {
            tool_call_id: broca_protocol::ToolCallId("ordinary".into()),
            tool_name: "read".into(),
            input: serde_json::json!({}).into(),
            thought_signature: None,
            provider_state: None,
        };
        let token = CancellationToken::new();
        let guard = ToolOpenGuard::new(
            if cancel { Some(&token) } else { None },
            if cancel { None } else { Some(&token) },
        );
        let mut dispatch = Box::pin(plane.dispatch_guarded(&ctx, &call, None, &guard));
        assert!(dispatch.as_mut().now_or_never().is_none());
        assert_eq!(
            module.pending_count_for_test(),
            1,
            "the real tool request must already be waiting for writer capacity"
        );
        token.cancel();
        let result = tokio::time::timeout(Duration::from_millis(250), dispatch)
            .await
            .expect("an interrupted enqueue must not wait for writer capacity");
        assert!(
            matches!(&result, DispatchResult::NotSent { reason } if reason == "dispatch_stopped_before_send"),
            "an interruption before the push is typed not-sent, never unknown: {result:?}"
        );
        let frames = module.take_written_for_test();
        assert_eq!(frames.len(), 256);
        assert!(
            frames
                .iter()
                .all(|frame| frame.header.ty == subc_protocol::FrameType::Ping),
            "no tool request may be enqueued once the run is interrupted"
        );
        assert_eq!(module.pending_count_for_test(), 0);
        tokio::task::yield_now().await;
        assert!(module.take_written_for_test().is_empty());
    }
}
