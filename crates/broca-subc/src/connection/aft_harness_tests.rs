use super::*;
use crate::provider_catalog::ProviderCatalog;

pub(super) fn catalog(accepts_broca: bool) -> ProviderCatalog {
    let mut entry = CatalogEntry::new("aft", vec![], vec![]);
    let mut provides = vec!["tool-provider/v1"];
    if accepts_broca {
        provides.push("scope-background-tasks/v1");
    }
    entry.capabilities =
        Some(serde_json::from_value(serde_json::json!({"provides": provides})).unwrap());
    vec![entry].into()
}

async fn open(conn: &SubcConnection, snapshot: &ProviderCatalog) -> ConnResult<RouteHandle> {
    let scope = subc_protocol::scope::ScopeSelector::new(
        subc_protocol::Principal::Reserved {
            module_id: "owner".into(),
        },
        "scope",
        Some(3),
    );
    conn.route_open_dispatch_single_attempt(
        "aft",
        &BindIdentity::new("/project", "broca", "worker"),
        Some(&scope),
        RouteShape {
            tool_provider_v1: true,
            ..Default::default()
        },
        false,
        Some(Arc::clone(&snapshot.aft_harness)),
    )
    .await
}

async fn request_harness(daemon: &mut InProcessModuleEnd, expected: &str) -> Frame {
    let request = daemon.next_request().await;
    let value: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert!(
        matches!(
            serde_json::from_slice::<ClientControlRequest>(&request.body).unwrap(),
            ClientControlRequest::RouteOpen { .. }
        ),
        "no catalog RPC at route open"
    );
    assert_eq!(
        value["identity"],
        serde_json::json!({"harness": expected, "session": "worker"})
    );
    request
}

fn accept(daemon: &InProcessModuleEnd, request: &Frame, channel: u16) {
    assert!(daemon.reply(
        request,
        serde_json::to_vec(&ClientControlResponse::RouteOpen {
            route_channel: channel,
            route_epoch: 1,
        })
        .unwrap()
    ));
}

#[tokio::test]
async fn aft_tool_routes_require_the_catalog_capability() {
    for (snapshot, expected) in [
        (catalog(true), "broca"),
        (catalog(false), "runner"),
        (Vec::new().into(), "runner"),
    ] {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let (result, ()) = tokio::join!(open(&conn, &snapshot), async {
            let request = request_harness(&mut daemon, expected).await;
            accept(&daemon, &request, 7);
        });
        result.unwrap();
        assert!(daemon.take_written_for_test().is_empty());
    }
}

#[tokio::test]
async fn aft_harness_fallback_is_remembered_until_snapshot_replaced() {
    let (mut conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
    Arc::get_mut(&mut conn.shared).unwrap().consumer_identity =
        Some(subc_control::ConsumerIdentity {
            module_id: "broca".into(),
            launch_nonce: "test".into(),
        });
    let snapshot = catalog(true);
    let (_, events) = broca_test_support::capture_logs(async {
        let (result, ()) = tokio::join!(open(&conn, &snapshot), async {
            let first = request_harness(&mut daemon, "broca").await;
            assert!(daemon.refuse(&first, "bind_blocked_by_reader"));
            let fallback = request_harness(&mut daemon, "runner").await;
            let mut expected: serde_json::Value = serde_json::from_slice(&first.body).unwrap();
            expected["identity"]["harness"] = serde_json::json!("runner");
            assert_eq!(serde_json::from_slice::<serde_json::Value>(&fallback.body).unwrap(), expected,
                "fallback preserves the consumer identity, scope, session and role/capability declarations");
            accept(&daemon, &fallback, 7);
        });
        result.unwrap();
        let (result, ()) = tokio::join!(open(&conn, &snapshot), async {
            let next = request_harness(&mut daemon, "runner").await;
            accept(&daemon, &next, 8);
        });
        result.unwrap();
        assert!(!snapshot.aft_harness.accepts_broca());
        let refreshed = catalog(true);
        let (result, ()) = tokio::join!(open(&conn, &refreshed), async {
            let next = request_harness(&mut daemon, "broca").await;
            accept(&daemon, &next, 9);
        });
        result.unwrap();
    }).await;
    let warnings = events
        .iter()
        .filter(|event| event.message().contains("until the catalog refreshes"))
        .collect::<Vec<_>>();
    assert_eq!(warnings.len(), 1);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.message() == "AFT tool-route harness decision changed")
            .count(),
        2,
        "the remembered runner decision does not log again for each route"
    );
    assert_eq!(
        warnings[0].fields.get("module_id").map(String::as_str),
        Some("aft")
    );
    assert_eq!(
        warnings[0].fields.get("code").map(String::as_str),
        Some("bind_blocked_by_reader")
    );
    assert!(daemon.take_written_for_test().is_empty());
}

#[tokio::test]
async fn aft_unknown_bind_refusal_returns_the_original_when_runner_is_refused() {
    let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
    let snapshot = catalog(true);
    let first_body = serde_json::to_vec(&serde_json::json!({
        "code": "future_target_policy", "message": "first refusal with exact spelling: ß",
        "detail": {"raw": "\\u1234\n", "n": 12}
    }))
    .unwrap();
    let (result, ()) = tokio::join!(open(&conn, &snapshot), async {
        let first = request_harness(&mut daemon, "broca").await;
        assert!(daemon.terminal(&first, FrameType::Error, first_body.clone()));
        let fallback = request_harness(&mut daemon, "runner").await;
        assert!(daemon.refuse(&fallback, "different_runner_refusal"));
    });
    let Err(ConnError::Rejected(actual)) = result else {
        panic!("expected first refusal: {result:?}");
    };
    assert_eq!(actual, decode_error_body(&first_body));
    assert!(
        snapshot.aft_harness.accepts_broca(),
        "a refused runner open proves no compatibility"
    );
    assert!(daemon.take_written_for_test().is_empty());
}

#[tokio::test]
async fn aft_daemon_permanent_refusals_do_not_fallback() {
    for code in DAEMON_PERMANENT_OPEN_REFUSALS {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let snapshot = catalog(true);
        let (result, ()) = tokio::join!(open(&conn, &snapshot), async {
            let request = request_harness(&mut daemon, "broca").await;
            assert!(daemon.refuse(&request, code));
        });
        assert_eq!(result.unwrap_err().code(), Some(*code));
        assert!(snapshot.aft_harness.accepts_broca());
        assert!(daemon.take_written_for_test().is_empty());
    }
}

#[tokio::test]
async fn aft_temporary_refusals_stay_with_existing_retry() {
    let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
    let snapshot = catalog(true);
    let retry = retry_route_open(
        RouteOpenRetry::within(Duration::from_secs(1), Duration::from_millis(1)),
        None,
        no_gate,
        || open(&conn, &snapshot),
    );
    let (result, ()) = tokio::join!(retry, async {
        let request = request_harness(&mut daemon, "broca").await;
        assert!(daemon.refuse(&request, "module_warming"));
        let retried = request_harness(&mut daemon, "broca").await;
        accept(&daemon, &retried, 7);
    });
    result.unwrap();
    assert!(snapshot.aft_harness.accepts_broca());
    assert!(daemon.take_written_for_test().is_empty());
}

#[tokio::test]
async fn aft_transport_and_decode_failures_do_not_fallback() {
    for malformed in [false, true] {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let snapshot = catalog(true);
        let (result, ()) = tokio::join!(open(&conn, &snapshot), async {
            let request = request_harness(&mut daemon, "broca").await;
            if malformed {
                assert!(daemon.reply(&request, b"not JSON".to_vec()));
            } else {
                conn.deactivate_for_test();
            }
        });
        assert!(matches!(
            result,
            Err(ConnError::Transport(_) | ConnError::Protocol(_))
        ));
        assert!(snapshot.aft_harness.accepts_broca());
        assert!(daemon.take_written_for_test().is_empty());
    }
}
