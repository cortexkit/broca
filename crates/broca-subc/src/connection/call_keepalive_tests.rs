mod call_keepalive {
    use super::*;

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
                        "provider", &identity, None, result, held, keepalive,
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
                        legacy = Some(wire);
                    }
                    answer_route_open(&pending, &request, 7);
                    assert_eq!(open.await.unwrap().call_keepalive, keepalive);
                }
            }
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
