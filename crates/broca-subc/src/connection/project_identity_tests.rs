#[tokio::test]
async fn outgoing_route_open_bytes_never_carry_a_project_or_storage_namespace() {
    for root in ["broca-unscoped-v1", "/old/project", "", "relative/project"] {
      for target in [
        RouteTarget::ToolProvider { module_id: "aft".into() },
        RouteTarget::ManagementSurface { module_id: "claustrum".into() },
        RouteTarget::InternalService { module_id: "catalog".into(), service_id: "catalog".into() },
      ] {
        let (conn, mut write_rx, pending) = route_open_test_connection();
        let identity = BindIdentity::new(root, "broca", "session");
        let open = conn.route_open_target(target, &identity, None, None);
        tokio::pin!(open);
        let request = tokio::select! {
            result = &mut open => panic!("premature result: {result:?}"),
            request = write_rx.recv() => request.unwrap(),
        };
        let raw: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert!(raw["identity"].get("project_root").is_none(), "{raw}");
        assert!(raw["identity"].get("project_id").is_none(), "{raw}");
        assert!(raw.get("scope").is_none(), "{raw}");
        assert!(!String::from_utf8_lossy(&request.body).contains("broca-unscoped-v1"));
        answer_route_open(&pending, &request, 7);
        open.await.unwrap();
      }
    }
}

#[tokio::test(start_paused = true)]
async fn session_inventory_unavailability_retries_but_ambiguity_is_terminal() {
    for (code, expected_attempts) in [("session_address_unavailable", 2), ("session_address_ambiguous", 1)] {
        let attempts = AtomicUsize::new(0);
        let result = retry_route_open(
            RouteOpenRetry::within(Duration::from_secs(1), Duration::from_millis(10)),
            None,
            no_gate,
            || {
                let first = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
                std::future::ready(if first {
                    Err(ConnError::Rejected(RejectedMessage::structured(code.into(), "inventory refusal".into())))
                } else {
                    Ok(RouteHandle::synthetic(7))
                })
            },
        ).await;
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), expected_attempts, "{code}: {result:?}");
        assert_eq!(result.is_ok(), expected_attempts == 2, "{code}: {result:?}");
    }
}
