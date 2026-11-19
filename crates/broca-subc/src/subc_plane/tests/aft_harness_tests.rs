use super::*;

#[tokio::test]
async fn aft_snapshot_refresh_reenables_the_session_harness_without_per_open_catalog_calls() {
    let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
    let plane = SubcToolPlane::new(conn, "aft");
    let mut entry = CatalogEntry::new("aft", vec![], vec![]);
    entry.capabilities = Some(
        serde_json::from_value(serde_json::json!({
            "provides": ["tool-provider/v1", "scope-background-tasks/v1"]
        }))
        .unwrap(),
    );
    let catalog_reply = serde_json::to_vec(&subc_control::ClientControlResponse::CatalogList {
        generation: 1,
        modules: vec![entry],
        subc_ops: vec![],
    })
    .unwrap();
    let bind = BindIdentity::new("/project", "broca", "worker");
    let lifecycle = CancellationToken::new();
    let guard = ToolOpenGuard::default();
    let open = || {
        plane.route_open_for_lifecycle(
            &bind,
            None,
            "aft",
            &lifecycle,
            None,
            &guard,
            DEFAULT_ROUTE_OPEN_PATIENCE,
            RouteShape {
                tool_provider_v1: true,
                ..Default::default()
            },
        )
    };
    for refresh in [false, true] {
        let (result, ()) = tokio::join!(plane.refresh_catalog_snapshot(), async {
            let request = daemon.next_request().await;
            assert!(matches!(
                serde_json::from_slice::<subc_control::ClientControlRequest>(&request.body)
                    .unwrap(),
                subc_control::ClientControlRequest::CatalogList { .. }
            ));
            assert!(daemon.reply(&request, catalog_reply.clone()));
        });
        result.unwrap();
        let (result, ()) = tokio::join!(open(), async {
            let request = daemon.next_request().await;
            let value: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(value["identity"]["harness"], "broca");
            let request = if !refresh {
                assert!(daemon.refuse(&request, "old_harness_parser"));
                let request = daemon.next_request().await;
                let value: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                assert_eq!(value["identity"]["harness"], "runner");
                request
            } else {
                request
            };
            assert!(daemon.reply(
                &request,
                serde_json::to_vec(&subc_control::ClientControlResponse::RouteOpen {
                    route_channel: 7,
                    route_epoch: 1,
                                    request_window: None,
                                    watch_window: None,
                })
                .unwrap()
            ));
        });
        assert!(result.is_ok(), "route open should succeed");
        if !refresh {
            let (result, ()) = tokio::join!(open(), async {
                let request = daemon.next_request().await;
                let value: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                assert_eq!(value["identity"]["harness"], "runner");
                assert!(daemon.reply(
                    &request,
                    serde_json::to_vec(&subc_control::ClientControlResponse::RouteOpen {
                        route_channel: 8,
                        route_epoch: 1,
                                    request_window: None,
                                    watch_window: None,
                    })
                    .unwrap()
                ));
            });
            assert!(result.is_ok(), "remembered runner route should succeed");
        }
        assert!(daemon.take_written_for_test().is_empty());
    }
}
