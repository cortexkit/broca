mod call_keepalive {
    use super::*;

    #[tokio::test]
    async fn graceful_stop_after_enqueue_keeps_awaiting_the_tool_reply() {
        for keepalive in [false, true] {
            let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
            let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 1)])));
            let (write_tx, mut writes) = mpsc::channel(16);
            let conn = test_connection(write_tx, pending.clone(), routes, ConnToken(1), 1);
            let mut route = RouteHandle::new(7, 1, ConnToken(1));
            route.call_keepalive = keepalive;
            let stop = CancellationToken::new();
            let signals = broca_core::tool_plane::DispatchSignals::new(None, Some(&stop));
            let call = conn.tool_call_raw_guarded(
                &route,
                "read",
                "{}",
                Some("draining"),
                Duration::from_secs(120),
                false,
                None,
                None,
                None,
                &signals,
            );
            tokio::pin!(call);
            assert!(futures::poll!(&mut call).is_pending());
            let request = writes.try_recv().unwrap();
            assert_eq!(request.header.ty, FrameType::Request);
            stop.cancel();
            assert!(
                futures::poll!(&mut call).is_pending(),
                "graceful stop must not abandon an enqueued call"
            );
            assert!(writes.try_recv().is_err(), "no CANCEL or re-send during grace");
            let sender = match pending.lock().unwrap().get(&(7, 1, request.header.corr)) {
                Some(PendingEntry::Live { sender, .. }) => sender.clone(),
                _ => panic!("the reply registration must survive graceful stop"),
            };
            sender
                .send(
                    Frame::build(
                        FrameType::Response,
                        data_flags(),
                        7,
                        1,
                        request.header.corr,
                        br#"{"content":[{"type":"text","text":"done"}]}"#.to_vec(),
                    )
                    .unwrap(),
                )
                .unwrap();
            assert_eq!(call.await.unwrap()["content"][0]["text"], "done");
            assert!(pending.lock().unwrap().is_empty());
            assert!(writes.try_recv().is_err(), "a completed call needs no CANCEL");
        }
    }

    #[derive(Default)]
    struct WakeCounter(std::sync::atomic::AtomicUsize);
    impl futures::task::ArcWake for WakeCounter {
        fn wake_by_ref(counter: &Arc<Self>) {
            counter.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn cancelled_call_late_frames_cross_the_real_reader_without_results_or_wakes() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 1)])));
        let (write_tx, mut writes) = mpsc::channel(16);
        let conn = test_connection(
            write_tx.clone(),
            pending.clone(),
            routes.clone(),
            ConnToken(1),
            1,
        );
        let route = RouteHandle::new(7, 1, ConnToken(1));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let (mut provider, _) = accepted.unwrap();
        provider.set_nodelay(true).unwrap();
        let (socket, _egress) = client.unwrap().into_split();
        let reader = tokio::spawn(reader_task(socket, pending.clone(), routes, write_tx, None));
        let signal = CancellationToken::new();
        let signals = broca_core::tool_plane::DispatchSignals::new(Some(&signal), None);
        let abandoned_wakes = Arc::new(WakeCounter::default());
        let waker = futures::task::waker(abandoned_wakes.clone());
        let mut context = std::task::Context::from_waker(&waker);
        let mut call = Box::pin(conn.tool_call_raw_guarded(
            &route,
            "read",
            "{}",
            Some("abandoned"),
            Duration::from_secs(120),
            false,
            None,
            None,
            None,
            &signals,
        ));
        assert!(std::future::Future::poll(call.as_mut(), &mut context).is_pending());
        let request = writes.try_recv().unwrap();
        assert_eq!(request.header.ty, FrameType::Request);
        signal.cancel();
        assert!(matches!(
            std::future::Future::poll(call.as_mut(), &mut context),
            std::task::Poll::Ready(Err(ApprovedCallError::AfterPush(ConnError::Transport(_))))
        ));
        drop(call);
        let cancel = writes.try_recv().unwrap();
        assert_eq!(cancel.header.ty, FrameType::Cancel);
        assert_cancel_for(&cancel, 7, 1, request.header.corr);
        assert!(pending.lock().unwrap().is_empty());
        abandoned_wakes.0.store(0, Ordering::SeqCst);

        let live_wakes = Arc::new(WakeCounter::default());
        let live_waker = futures::task::waker(live_wakes.clone());
        let mut live_context = std::task::Context::from_waker(&live_waker);
        let input = serde_json::json!({});
        let mut live = Box::pin(conn.tool_call_with_deadline(
            &route,
            "read",
            &input,
            Some("live"),
            Duration::from_secs(120),
        ));
        assert!(std::future::Future::poll(live.as_mut(), &mut live_context).is_pending());
        let second = writes.try_recv().unwrap();
        assert_ne!(second.header.corr, request.header.corr);
        live_wakes.0.store(0, Ordering::SeqCst);
        let (_barrier, mut barrier) =
            PendingRegistration::new(pending.clone(), (0, 0, 999), false, None).unwrap();
        let (_, dropped_before) = FRAME_DROPS.counts();
        for (kind, body) in [
            (FrameType::Push, br#"{"kind":"keepalive"}"#.to_vec()),
            (
                FrameType::Response,
                br#"{"content":[{"type":"text","text":"late"}]}"#.to_vec(),
            ),
            (
                FrameType::Error,
                br#"{"code":"cancelled","message":"late"}"#.to_vec(),
            ),
        ] {
            write_frame(
                &mut provider,
                &Frame::build(kind, data_flags(), 7, 1, request.header.corr, body).unwrap(),
            )
            .await
            .unwrap();
        }
        write_frame(
            &mut provider,
            &Frame::build(
                FrameType::Response,
                control_flags(),
                0,
                0,
                999,
                b"{}".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(barrier.recv().await.unwrap().header.corr, 999);
        let (_, dropped_after) = FRAME_DROPS.counts();
        assert!(
            dropped_after >= dropped_before + 2,
            "the socket reader processed the two late terminals"
        );
        assert_eq!(abandoned_wakes.0.load(Ordering::SeqCst), 0);
        assert_eq!(live_wakes.0.load(Ordering::SeqCst), 0);
        assert!(
            std::future::Future::poll(live.as_mut(), &mut live_context).is_pending(),
            "late frames produce no live-call result"
        );
        drop(live);
        reader.abort();
        let _ = reader.await;
    }

    #[tokio::test]
    async fn full_or_closed_cancel_queues_do_not_block_or_change_the_unknown_outcome() {
        for closed in [false, true] {
            let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
            let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 1)])));
            let (write_tx, mut writes) = mpsc::channel(1);
            let conn = test_connection(write_tx.clone(), pending.clone(), routes, ConnToken(1), 1);
            let route = RouteHandle::new(7, 1, ConnToken(1));
            let signal = CancellationToken::new();
            let signals = broca_core::tool_plane::DispatchSignals::new(Some(&signal), None);
            let call = conn.tool_call_raw_guarded(
                &route,
                "read",
                "{}",
                Some("call"),
                Duration::from_secs(120),
                false,
                None,
                None,
                None,
                &signals,
            );
            tokio::pin!(call);
            assert!(futures::poll!(&mut call).is_pending());
            assert_eq!(writes.try_recv().unwrap().header.ty, FrameType::Request);
            if closed {
                writes.close();
            } else {
                write_tx
                    .try_send(
                        Frame::build(FrameType::Push, data_flags(), 7, 1, 0, b"{}".to_vec())
                            .unwrap(),
                    )
                    .unwrap();
            }
            signal.cancel();
            let outcome = futures::poll!(&mut call);
            assert!(
                matches!(outcome, std::task::Poll::Ready(Err(ApprovedCallError::AfterPush(
                ConnError::Transport(ref message)))) if message == "tool call abandoned: run cancelled or stopped")
            );
            assert!(pending.lock().unwrap().is_empty());
            if !closed {
                assert_eq!(writes.try_recv().unwrap().header.ty, FrameType::Push);
            }
            tokio::task::yield_now().await;
            assert!(
                writes.try_recv().is_err(),
                "a lost best-effort CANCEL is not retried"
            );
        }
    }

    #[tokio::test]
    async fn dispatch_bind_adds_keepalive_only_when_requested_and_legacy_bytes_match() {
        for held in [false, true] {
            for result in [false, true] {
                let mut legacy: Option<String> = None;
                for keepalive in [false, true] {
                    let (conn, mut writes, pending) = route_open_test_connection();
                    let identity =
                        broca_protocol::BindIdentity::new("/project", "broca", "session");
                    let open = conn.route_open_dispatch_single_attempt(
                        "provider",
                        &identity,
                        None,
                        crate::connection::RouteShape {
                            tool_result_v1: result,
                            call_keepalive: keepalive,
                            ..Default::default()
                        },
                        held,
                    );
                    tokio::pin!(open);
                    let request = tokio::select! {
                        result = &mut open => panic!("premature result: {result:?}"),
                        request = writes.recv() => request.unwrap(),
                    };
                    let raw: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                    let wire = String::from_utf8(request.body.clone()).unwrap();
                    if keepalive {
                        let caps = raw["consumer_capabilities"].as_array().unwrap();
                        assert_eq!(caps.last().unwrap(), "call-keepalive/v1");
                        let restored = if caps.len() == 1 {
                            wire.replace(",\"consumer_capabilities\":[\"call-keepalive/v1\"]", "")
                        } else {
                            wire.replace(",\"call-keepalive/v1\"", "")
                        };
                        assert_eq!(restored, legacy.clone().unwrap());
                    } else {
                        let historical = if held {
                            include_str!(
                                "../../tests/fixtures/held_dispatch_route_held_legacy.json"
                            )
                        } else if result {
                            include_str!(
                                "../../tests/fixtures/held_dispatch_route_result_legacy.json"
                            )
                        } else {
                            include_str!("../../tests/fixtures/held_dispatch_route_legacy.json")
                        };
                        assert_eq!(request.body, historical.trim_end().as_bytes());
                        legacy = Some(wire);
                    }
                    answer_route_open(&pending, &request, 7);
                    assert_eq!(open.await.unwrap().call_keepalive, keepalive);
                }
            }
        }
    }

    #[tokio::test]
    async fn legacy_request_bytes_are_pinned_for_both_reply_timer_modes() {
        for declared in [false, true] {
            let (conn, route, mut provider) = SubcConnection::in_process_for_test(7, 1);
            let signals = broca_core::tool_plane::DispatchSignals::default();
            let call = conn.tool_call_raw_guarded(
                &route,
                "read",
                r#"{"path":"src/lib.rs"}"#,
                Some("provider-call"),
                Duration::from_secs(120),
                declared,
                None,
                None,
                None,
                &signals,
            );
            let response = async {
                let request = provider.next_request().await;
                assert_eq!(
                    request.body,
                    include_str!("../../tests/fixtures/tool_call_legacy.json")
                        .trim_end()
                        .as_bytes()
                );
                assert!(provider.reply(&request, br#"{"content":[],"isError":false}"#.to_vec()));
            };
            let (answer, ()) = tokio::join!(call, response);
            assert!(answer.is_ok());
            assert!(provider.take_written_for_test().is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn silence_is_90_seconds_only_on_negotiated_routes_and_any_push_resets_it() {
        for (keepalive, progress_at, expected) in
            [(true, None, 90), (false, None, 120), (true, Some(80), 170)]
        {
            let (conn, mut route, mut module) = SubcConnection::in_process_for_test(7, 1);
            route.call_keepalive = keepalive;
            let started = time::Instant::now();
            let call = tokio::spawn(async move {
                conn.tool_call_with_deadline(
                    &route,
                    "read",
                    &serde_json::json!({}),
                    Some("call"),
                    if keepalive {
                        Duration::from_secs(86_400)
                    } else {
                        TOOL_CALL_TIMEOUT
                    },
                )
                .await
            });
            let request = module.next_request().await;
            if let Some(seconds) = progress_at {
                time::sleep(Duration::from_secs(seconds)).await;
                assert!(module.push(&request, br#"{"progress":"advancing"}"#.to_vec()));
            }
            let error = call.await.unwrap().unwrap_err();
            assert_eq!(started.elapsed(), Duration::from_secs(expected));
            if keepalive {
                assert!(error.to_string().contains("silent for 90s"), "{error}");
            } else {
                assert!(
                    error.to_string().contains("timed out after 120s"),
                    "{error}"
                );
            }
            let frames = module.take_written_for_test();
            let cancels: Vec<_> = frames
                .iter()
                .filter(|f| f.header.ty == FrameType::Cancel)
                .collect();
            assert_eq!(cancels.len(), 1);
            assert_cancel_for(cancels[0], 7, 1, request.header.corr);
            assert_eq!(module.pending_count_for_test(), 0);
            assert!(!module.push(
                &request,
                cortexkit_role_tool_provider::keepalive_body().to_vec()
            ));
            assert!(!module.reply(&request, br#"{"content":[]}"#.to_vec()));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn keepalives_never_extend_the_terminal_reply_deadline() {
        let (conn, mut route, mut module) = SubcConnection::in_process_for_test(7, 1);
        route.call_keepalive = true;
        let started = time::Instant::now();
        let call = tokio::spawn(async move {
            conn.tool_call_with_deadline(
                &route,
                "read",
                &serde_json::json!({}),
                Some("call"),
                Duration::from_secs(120),
            )
            .await
        });
        let request = module.next_request().await;
        for _ in 0..3 {
            time::sleep(Duration::from_secs(30)).await;
            assert!(module.push(
                &request,
                cortexkit_role_tool_provider::keepalive_body().to_vec()
            ));
        }
        let error = call.await.unwrap().unwrap_err();
        assert!(
            error.to_string().contains("timed out after 120s"),
            "{error}"
        );
        assert_eq!(started.elapsed(), Duration::from_secs(120));
        let frames = module.take_written_for_test();
        let cancels: Vec<_> = frames
            .iter()
            .filter(|f| f.header.ty == FrameType::Cancel)
            .collect();
        assert_eq!(cancels.len(), 1);
        assert_cancel_for(cancels[0], 7, 1, request.header.corr);
        assert!(!module.reply(&request, br#"{"content":[]}"#.to_vec()));
    }
}
