mod aft_harness {
    include!("aft_harness_tests.rs");
}
include!("project_identity_tests.rs");
include!("route_close_followup_tests.rs");
include!("call_keepalive_tests.rs");
#[tokio::test]
async fn a_registered_call_waiting_for_egress_cannot_use_a_closing_route() {
    for guarded in [false, true] {
        let (conn, route, mut module) = SubcConnection::in_process_full_writer_for_test(7, 3);
        let conn = Arc::new(conn);
        let call = {
            let conn = Arc::clone(&conn);
            tokio::spawn(async move {
                if guarded {
                    matches!(
                        conn.tool_call_raw_yieldable_guarded(
                            &route,
                            "echo",
                            "{}",
                            None,
                            Duration::from_secs(10),
                            false,
                            None,
                            None,
                            None,
                            &broca_core::tool_plane::DispatchSignals::default(),
                            None,
                        )
                        .await,
                        Err(ApprovedCallError::BeforePush(
                            ApprovedSetupError::Connection(ConnError::StaleRouteHandle(_))
                        ))
                    )
                } else {
                    matches!(
                        conn.tool_call(&route, "echo", &serde_json::json!({}), None)
                            .await,
                        Err(ConnError::StaleRouteHandle(_))
                    )
                }
            })
        };
        broca_test_support::wait_until("call waits for egress", || async {
            if module.pending_count_for_test() == 1 {
                Ok(())
            } else {
                Err("no registered call".into())
            }
        })
        .await;
        let close = {
            let conn = Arc::clone(&conn);
            tokio::spawn(async move { conn.route_close(&route).await })
        };
        broca_test_support::wait_until("epoch is closing", || async {
            if !conn.route_is_open(&route) {
                Ok(())
            } else {
                Err("route still open".into())
            }
        })
        .await;
        let queued = module.take_written_for_test();
        assert!(queued
            .iter()
            .all(|frame| frame.header.ty == FrameType::Ping));
        assert!(
            call.await.unwrap(),
            "a closed epoch must be refused before enqueue"
        );
        close.await.unwrap().unwrap();
        let queued = module.take_written_for_test();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].header.ty, FrameType::Goodbye);
    }
}

#[cfg(feature = "test-route-close")]
#[tokio::test]
async fn a_closing_epoch_refuses_calls_but_retries_only_goodbye() {
    let (conn, route, mut module) = SubcConnection::in_process_for_test(7, 3);
    conn.fail_next_goodbye_for_test();
    assert!(matches!(
        conn.route_close(&route).await,
        Err(ConnError::Transport(_))
    ));
    assert!(!conn.route_is_open(&route));
    assert!(matches!(
        conn.tool_call(&route, "echo", &serde_json::json!({}), None)
            .await,
        Err(ConnError::StaleRouteHandle(_))
    ));
    assert!(
        module.take_written_for_test().is_empty(),
        "no call may use a closing epoch"
    );
    conn.route_close(&route).await.unwrap();
    let frames = module.take_written_for_test();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].header.ty, FrameType::Goodbye);
    assert_eq!(frames[0].header.epoch, route.epoch);
    assert!(matches!(
        conn.route_close(&route).await,
        Err(ConnError::StaleRouteHandle(_))
    ));
}
/// A dispatch route for a provider the plan fetch answered declares exactly
/// `tool-provider: v1`, keeps the tool-module bind harness, and records the role on its
/// handle, so the route cache can tell it from a route without the role.
#[tokio::test]
async fn a_fetched_providers_dispatch_route_declares_tool_provider_v1() {
    let (conn, mut write_rx, pending) = route_open_test_connection();
    let identity = broca_protocol::BindIdentity {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "session".into(),
    };
    let open = conn.route_open_dispatch_single_attempt(
        "provider",
        &identity,
        None,
        RouteShape {
            tool_provider_v1: true,
            ..RouteShape::default()
        },
        false,
        None,
    );
    tokio::pin!(open);
    let request = tokio::select! {
        result = &mut open => panic!("premature result: {result:?}"),
        request = write_rx.recv() => request.unwrap(),
    };
    let raw: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(
        raw["role_versions"],
        serde_json::json!({"tool-provider": "v1"})
    );
    assert_eq!(raw["identity"]["harness"], TOOL_MODULE_HARNESS);
    answer_route_open(&pending, &request, 7);
    let route = open.await.unwrap();
    assert!(route.tool_provider_v1);
    assert!(!route.session_identity);
}

/// The route to the session's own compaction provider binds the session's harness and
/// session, exactly as its compaction route does, with the role and the scope unchanged.
#[tokio::test]
async fn the_compaction_providers_tool_route_binds_the_sessions_own_harness() {
    let (conn, mut write_rx, pending) = route_open_test_connection();
    let identity = broca_protocol::BindIdentity {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "alfonso:bg_x".into(),
    };
    let open = conn.route_open_dispatch_single_attempt(
        "magic-context",
        &identity,
        None,
        RouteShape {
            tool_provider_v1: true,
            session_identity: true,
            ..RouteShape::default()
        },
        false,
        None,
    );
    tokio::pin!(open);
    let request = tokio::select! {
        result = &mut open => panic!("premature result: {result:?}"),
        request = write_rx.recv() => request.unwrap(),
    };
    let raw: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(
        raw["identity"],
        serde_json::json!({"harness": "broca", "session": "alfonso:bg_x"})
    );
    assert_eq!(
        raw["role_versions"],
        serde_json::json!({"tool-provider": "v1"})
    );
    answer_route_open(&pending, &request, 7);
    let route = open.await.unwrap();
    assert!(route.session_identity && route.tool_provider_v1);
}

#[tokio::test]
async fn every_session_tool_route_uses_one_provider_identity() {
    let (mut conn, mut write_rx, pending) = route_open_test_connection();
    Arc::get_mut(&mut conn.shared).unwrap().consumer_identity =
        Some(subc_control::ConsumerIdentity {
            module_id: "broca".into(),
            launch_nonce: "test".into(),
        });
    let scope = subc_protocol::scope::ScopeSelector::new(
        subc_protocol::Principal::Reserved {
            module_id: "scope-owner".into(),
        },
        "session-scope",
        Some(3),
    );
    let mut catalog = Vec::new();
    for module in ["compactor", "hooks", "aft", "other"] {
        let mut entry = CatalogEntry::new(module, Vec::new(), Vec::new());
        entry.capabilities = Some(
                serde_json::from_value(serde_json::json!({
                    "provides": if module == "aft" { vec!["tool-provider/v1", "scope-background-tasks/v1"] } else { vec!["tool-provider/v1"] }
                }))
                .unwrap(),
            );
        catalog.push(entry);
    }
    let catalog = crate::provider_catalog::ProviderCatalog::from(catalog);
    for session in ["session-one", "session-two"] {
        for (provider, harness, session_identity, expected_harness) in [
            ("compactor", "broca", true, "broca"),
            ("hooks", "custom", true, "custom"),
            ("aft", "broca", false, "broca"),
            ("aft", "custom", false, "runner"),
            ("other", "broca", false, "runner"),
        ] {
            let identity = BindIdentity::new("/project", harness, session);
            let policy =
                ToolProviderRoutePolicy::for_provider(&catalog, provider, session_identity);
            for kind in ["catalog", "dispatch", "withdraw", "user_command.invoke"] {
                let open = async {
                    if matches!(kind, "dispatch" | "user_command.invoke") {
                        conn.route_open_dispatch_single_attempt(
                            provider,
                            &identity,
                            Some(&scope),
                            RouteShape {
                                tool_provider_v1: policy.declares_role,
                                session_identity: policy.session_identity,
                                ..RouteShape::default()
                            },
                            false,
                            policy.aft_harness.clone(),
                        )
                        .await
                    } else {
                        conn.route_open_tool_provider_with_policy(
                            provider,
                            &identity,
                            Some(scope.clone()),
                            policy.clone(),
                        )
                        .await
                    }
                };
                tokio::pin!(open);
                let request = tokio::select! {
                    result = &mut open => panic!("premature result: {result:?}"),
                    request = write_rx.recv() => request.unwrap(),
                };
                let raw: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                assert_eq!(
                    raw["identity"],
                    serde_json::json!({
                        "harness": expected_harness,
                        "session": session,
                    }),
                    "{kind} to {provider}"
                );
                assert_eq!(
                    raw["role_versions"],
                    serde_json::json!({"tool-provider":"v1"})
                );
                assert_eq!(raw["scope"], serde_json::to_value(&scope).unwrap());
                assert_eq!(
                    raw["consumer_identity"],
                    serde_json::json!({"module_id": "broca", "launch_nonce": "test"})
                );
                answer_route_open(&pending, &request, 7);
                assert_eq!(open.await.unwrap().session_identity, session_identity);
            }
        }
    }
}

#[tokio::test]
async fn global_late_results_feed_keeps_its_maintenance_identity_even_at_a_compactor() {
    let (mut conn, mut write_rx, pending) = route_open_test_connection();
    Arc::get_mut(&mut conn.shared).unwrap().consumer_identity =
        Some(subc_control::ConsumerIdentity {
            module_id: "broca".into(),
            launch_nonce: "test".into(),
        });
    let identity = BindIdentity::new("/state-root", "broca", "late-results");
    let open = crate::LateResultsFeed::open(Arc::new(conn), "compactor", &identity);
    tokio::pin!(open);
    let catalog = tokio::select! {
        _ = &mut open => panic!("feed opened before discovery"),
        request = write_rx.recv() => request.unwrap(),
    };
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&catalog.body).unwrap()["op"],
        "catalog.list"
    );
    let mut entry = CatalogEntry::new("compactor", Vec::new(), Vec::new());
    entry.capabilities = Some(
        serde_json::from_value(
            serde_json::json!({"provides":["tool-provider/v1", "compaction-provider/v2"]}),
        )
        .unwrap(),
    );
    let sender = match pending.lock().unwrap().get(&(0, 0, catalog.header.corr)) {
        Some(PendingEntry::Live { sender, .. }) => sender.clone(),
        _ => panic!("catalog request not pending"),
    };
    sender
        .send(
            Frame::build(
                FrameType::Response,
                control_flags(),
                0,
                0,
                catalog.header.corr,
                serde_json::to_vec(&ClientControlResponse::CatalogList {
                    generation: 1,
                    modules: vec![entry],
                    subc_ops: Vec::new(),
                })
                .unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
    let request = tokio::select! {
        _ = &mut open => panic!("feed opened before route bind"),
        request = write_rx.recv() => request.unwrap(),
    };
    let raw: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(
        raw["identity"],
        serde_json::json!({"harness":"runner", "session":"late-results"})
    );
    assert_ne!(
        raw["identity"],
        serde_json::json!({"harness":"broca", "session":"conversation"})
    );
    assert_eq!(
        raw["role_versions"],
        serde_json::json!({"tool-provider":"v1"})
    );
    answer_route_open(&pending, &request, 7);
    assert!(open.await.is_ok());
}

#[tokio::test]
async fn dispatch_result_capability_is_independent_of_held_and_role_routes_stay_undeclared() {
    for (result_v1, installed) in [(false, false), (true, false), (false, true)] {
        let (conn, mut write_rx, pending) = route_open_test_connection();
        let identity = broca_protocol::BindIdentity {
            project_root: "/project".into(),
            harness: "broca".into(),
            session: "session".into(),
        };
        let open = conn.route_open_dispatch_single_attempt(
            "provider",
            &identity,
            None,
            RouteShape {
                tool_result_v1: result_v1,
                ..RouteShape::default()
            },
            installed,
            None,
        );
        tokio::pin!(open);
        let request = tokio::select! {
            result = &mut open => panic!("premature result: {result:?}"),
            request = write_rx.recv() => request.unwrap(),
        };
        let raw: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        if installed {
            assert_eq!(
                raw["consumer_capabilities"],
                serde_json::json!(["tool-result/v1", "held-reply/v1", "call-yield/v1"])
            );
        } else if result_v1 {
            assert_eq!(
                raw["consumer_capabilities"],
                serde_json::json!(["tool-result/v1"])
            );
        } else {
            assert_eq!(
                request.body,
                include_str!("../../tests/fixtures/held_dispatch_route_legacy.json")
                    .trim_end()
                    .as_bytes()
            );
        }
        assert!(raw
            .get("role_versions")
            .is_none_or(serde_json::Value::is_null));
        answer_route_open(&pending, &request, 7);
        let route = open.await.unwrap();
        assert_eq!(route.reads_held_reply, installed);
        assert_eq!(route.reads_tool_result, result_v1 || installed);
        assert!(!route.tool_provider_v1);
        assert!(!route.session_identity);
    }
    let (conn, mut write_rx, pending) = route_open_test_connection();
    let identity = broca_protocol::BindIdentity {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "session".into(),
    };
    let open = conn.route_open_tool_provider_v1("provider", &identity, None);
    tokio::pin!(open);
    let request = tokio::select! {
        result = &mut open => panic!("premature role result: {result:?}"),
        request = write_rx.recv() => request.unwrap(),
    };
    let raw: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert!(raw.get("consumer_capabilities").is_none());
    answer_route_open(&pending, &request, 8);
    assert!(!open.await.unwrap().reads_held_reply);
}
use std::sync::atomic::AtomicUsize;

use super::*;
use subc_control::CatalogEntry;
use tokio::net::TcpStream;

async fn read_capabilities_concurrently(
    conn: &SubcConnection,
    module: &mut InProcessModuleEnd,
    capabilities: &[&str],
) {
    tokio::time::timeout(Duration::from_secs(2), async {
        let reads = async {
            tokio::join!(
                conn.server_capabilities(),
                conn.server_capabilities(),
                conn.server_capabilities()
            )
        };
        tokio::pin!(reads);
        let mut requests = 0;
        let results = loop {
            tokio::select! {
                results = &mut reads => break results,
                request = module.next_request() => {
                    requests += 1;
                    let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                    assert_eq!(body["op"], "server.describe");
                    let reply = ClientControlResponse::ServerDescribe {
                        protocol_ver: 1, subc_ops: vec![],
                        capabilities: capabilities.iter().map(|item| (*item).into()).collect(),
                        connected_clients: 1, counters: None,
                        build_git_sha: None, build_lock_digest: None,
                        capability_requirements: vec![], machine_id: None, config_path: None,
                    };
                    assert!(module.reply(&request, serde_json::to_vec(&reply).unwrap()));
                }
            }
        };
        let expected: Vec<String> = capabilities.iter().map(|item| (*item).into()).collect();
        assert_eq!(results.0.unwrap(), expected);
        assert_eq!(results.1.unwrap(), expected);
        assert_eq!(results.2.unwrap(), expected);
        assert_eq!(requests, 1, "capability discovery must be single-flight");
        assert_eq!(conn.server_capabilities().await.unwrap(), expected);
        assert!(
            module.write_rx.try_recv().is_err(),
            "cached reads must not send RPCs"
        );
    })
    .await
    .expect("capability readers must terminate");
}

#[tokio::test]
async fn pinned_guarded_dispatch_preserves_pin_and_exact_argument_bytes() {
    let (conn, route, mut provider) = SubcConnection::in_process_for_test(5, 3);
    let signals = broca_core::tool_plane::DispatchSignals::default();
    let pin = format!("tp1:work:{}:7", "0".repeat(64));
    let arguments = r#"{ "value" : 1 }"#;
    let dispatch = conn.tool_call_raw_yieldable_guarded(
        &route,
        "work",
        arguments,
        Some("call"),
        Duration::from_secs(1),
        false,
        None,
        Some("lineage:7"),
        Some(&pin),
        &signals,
        None,
    );
    let receive = async {
        let request = provider.next_request().await;
        let value: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(value["schema_pin"], pin);
        assert_eq!(value["call_key"], "lineage:7");
        assert!(
            String::from_utf8_lossy(&request.body).contains(&format!("\"arguments\":{arguments}"))
        );
        assert!(provider.reply(&request, br#"{"content":[]}"#.to_vec()));
    };
    let (result, ()) = tokio::join!(dispatch, receive);
    result.unwrap();
}

#[tokio::test]
async fn capabilities_are_single_flight_and_reset_on_reconnect() {
    let (conn, _, mut module) = SubcConnection::in_process_for_test(1, 1);
    read_capabilities_concurrently(&conn, &mut module, &["scopes/v1"]).await;
    let (replacement, write_rx) = new_runtime();
    let mut new_module = InProcessModuleEnd {
        write_rx,
        pending: replacement.pending.clone(),
    };
    *conn.shared.current.lock().unwrap() = GenerationState::new(replacement);
    read_capabilities_concurrently(&conn, &mut new_module, &[]).await;
}

#[tokio::test]
async fn failed_capability_discovery_is_not_cached() {
    let (conn, _, mut module) = SubcConnection::in_process_for_test(1, 1);
    let (failed, ()) = tokio::join!(conn.server_capabilities(), async {
        let request = module.next_request().await;
        assert!(module.reply(&request, b"invalid describe".to_vec()));
    });
    assert!(matches!(failed, Err(ConnError::Protocol(_))));
    read_capabilities_concurrently(&conn, &mut module, &["scopes/v1"]).await;
}

/// The behavioural grace tests inject their own window to compress timeouts, so they prove
/// the mechanism and are blind to the shipped value.
///
/// The relations, not the literals, are what matter. The grace must cover the daemon's
/// relay window, so a caller that abandons a route.open early cannot get a healthy socket
/// convicted. And one route.open attempt must wait STRICTLY LONGER than that window, so a
/// bind the daemon completes near its bound is reported as the success it is. The second
/// relation used to be the opposite (the attempt waited only the 10s RESPONSE_TIMEOUT), which
/// turned an 11s bind into a failed open.
#[test]
fn bind_relay_grace_covers_the_daemon_relay_window() {
    const DAEMON_BIND_RELAY_WINDOW: Duration = Duration::from_secs(12);

    assert!(
        ROUTE_OPEN_BIND_RELAY_GRACE >= DAEMON_BIND_RELAY_WINDOW,
        "grace {ROUTE_OPEN_BIND_RELAY_GRACE:?} is shorter than the daemon's relay window \
             {DAEMON_BIND_RELAY_WINDOW:?}; a route.open abandoned while the daemon is still \
             relaying would convict a healthy socket"
    );
    assert!(
        ROUTE_OPEN_ATTEMPT_TIMEOUT > DAEMON_BIND_RELAY_WINDOW,
        "a route.open attempt waits {ROUTE_OPEN_ATTEMPT_TIMEOUT:?}, not longer than the \
             daemon's relay window {DAEMON_BIND_RELAY_WINDOW:?}; a bind that completes inside \
             the window would be reported as a failed open"
    );
}

/// A connection with no socket whose route.open replies the test delivers by hand, at a
/// time it chooses. Returns the connection, the queue of frames it wrote, and its pending
/// map (where a reply is delivered).
pub(super) fn route_open_test_connection() -> (SubcConnection, mpsc::Receiver<Frame>, PendingMap) {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (write_tx, write_rx) = mpsc::channel(64);
    let mut conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
    // The shipped grace, not the compressed one other tests use: these tests are about the
    // production clocks.
    Arc::get_mut(&mut conn.shared)
        .expect("the connection is not shared yet")
        .route_open_bind_relay_grace = ROUTE_OPEN_BIND_RELAY_GRACE;
    (conn, write_rx, pending)
}

/// Answer a route.open `request` the way the daemon does once its bind relay completes.
/// Panics when nobody is waiting any more, which is how an abandoned open looks.
fn answer_route_open(pending: &PendingMap, request: &Frame, channel: u16) {
    // Read the entry and RELEASE the lock before any panic below: panicking while holding it
    // poisons the map, and the connection's own drop then panics too and aborts the whole
    // test binary instead of failing this one test.
    let entry = match pending.lock().unwrap().get(&(0, 0, request.header.corr)) {
        Some(PendingEntry::Live { sender, .. }) => Ok(sender.clone()),
        Some(PendingEntry::LateRouteOpen { .. }) => Err(
            "the caller had already given up on this route.open; the daemon's answer would \
                 only be cleaned up",
        ),
        None => Err("no route.open is waiting for this request"),
    };
    let sender = entry.unwrap_or_else(|reason| panic!("{reason}"));
    let body = serde_json::to_vec(&ClientControlResponse::RouteOpen {
        route_channel: channel,
        route_epoch: 1,
        request_window: None,
        watch_window: None,
    })
    .unwrap();
    let response = Frame::build(
        FrameType::Response,
        control_flags(),
        0,
        0,
        request.header.corr,
        body,
    )
    .unwrap();
    sender.send(response).unwrap();
}

async fn next_route_open_request(write_rx: &mut mpsc::Receiver<Frame>) -> Frame {
    loop {
        let frame = write_rx.recv().await.expect("the writer queue stays open");
        if frame.header.channel == 0 && frame.header.ty == FrameType::Request {
            return frame;
        }
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test(start_paused = true)]
async fn route_open_start_is_recorded_before_a_delayed_receipt() {
    let (mut conn, _route, mut daemon) = SubcConnection::in_process_for_test(9, 1);
    let starts = Arc::new(Mutex::new(Vec::new()));
    conn.record_route_open_attempts_for_test(Arc::clone(&starts));
    let identity = BindIdentity::new(".", "broca", "vault");
    let budget = Duration::from_secs(3600);
    let started = time::Instant::now();
    let mut open = std::pin::pin!(conn.route_open_management_within(
        "credentials",
        &identity,
        RouteOpenRetry::within(budget, Duration::from_secs(5)),
    ));
    assert!(futures::poll!(&mut open).is_pending());
    assert_eq!(*starts.lock().unwrap(), vec![started]);
    let first = daemon.next_request().await;
    assert!(daemon.refuse(&first, "target_unavailable"));
    assert!(futures::poll!(&mut open).is_pending());
    time::advance(ROUTE_OPEN_RETRY_INTERVAL).await;
    assert!(futures::poll!(&mut open).is_pending());
    assert_eq!(starts.lock().unwrap().len(), 2);

    // Leave the retry in the writer queue until readiness expires. Poll the client again
    // only after delivering the refusal, so the independent reply timer cannot win the
    // forced interleaving before the daemon handles the queued frame.
    time::advance(budget).await;
    let expired = time::Instant::now();
    let retry = daemon.next_request().await;
    let received = time::Instant::now();
    assert!(daemon.refuse(&retry, "target_unavailable"));
    assert!(matches!(open.await, Err(ConnError::Rejected(ref message))
            if message.code() == "target_unavailable"));
    assert_eq!(starts.lock().unwrap().len(), 2);
    assert!(daemon.take_written_for_test().is_empty());
    assert!(starts
        .lock()
        .unwrap()
        .iter()
        .all(|at| *at < started + budget));
    assert!(
        received >= expired,
        "receipt time is not the attempt's start time"
    );
}

/// A BIND THE DAEMON COMPLETES AT 11s IS A SUCCESSFUL OPEN, on both route targets. The
/// daemon relays route.open to the module for up to 12s, so 11s is inside its contract; the
/// 10s control-RPC deadline used to report this open as a failure while the daemon committed
/// the route anyway. Paused time: the eleven seconds are virtual.
#[tokio::test(start_paused = true)]
async fn a_route_open_whose_bind_answers_at_eleven_seconds_succeeds() {
    const BIND_ANSWERS_AFTER: Duration = Duration::from_secs(11);
    let identity = BindIdentity::new("/tmp/slow-bind", "runner", "session");

    for management in [false, true] {
        let (conn, mut write_rx, pending) = route_open_test_connection();
        let conn = Arc::new(conn);
        let open = {
            let conn = Arc::clone(&conn);
            let identity = identity.clone();
            tokio::spawn(async move {
                if management {
                    conn.route_open_management("credentials", &identity).await
                } else {
                    conn.route_open("aft", &identity).await
                }
            })
        };
        let request = next_route_open_request(&mut write_rx).await;
        time::sleep(BIND_ANSWERS_AFTER).await;
        answer_route_open(&pending, &request, 9);

        let handle = open.await.unwrap().unwrap_or_else(|error| {
            panic!("an 11s bind must open (management={management}): {error}")
        });
        assert_eq!((handle.channel, handle.epoch), (9, 1));
    }
}

/// A route.open the daemon never answers fails AT the per-attempt deadline, not before it
/// and not later, and the error names that deadline.
#[tokio::test(start_paused = true)]
async fn a_route_open_that_never_answers_fails_at_the_attempt_deadline() {
    let (conn, mut write_rx, _pending) = route_open_test_connection();
    let identity = BindIdentity::new("/tmp/silent-bind", "runner", "session");
    let started = time::Instant::now();
    let open = tokio::spawn(async move {
        let result = conn.route_open("aft", &identity).await;
        (result, started.elapsed())
    });
    let _request = next_route_open_request(&mut write_rx).await;

    let (result, elapsed) = open.await.unwrap();
    let message = match result {
        Err(ConnError::Transport(message)) => message,
        other => panic!("a silent daemon must fail as a transport timeout, got {other:?}"),
    };
    assert!(
        message.contains("ROUTE_OPEN_ATTEMPT_TIMEOUT")
            && message.contains(&format!("{ROUTE_OPEN_ATTEMPT_TIMEOUT:?}")),
        "the error must name the route-open deadline: {message}"
    );
    assert!(
        elapsed >= ROUTE_OPEN_ATTEMPT_TIMEOUT
            && elapsed < ROUTE_OPEN_ATTEMPT_TIMEOUT + Duration::from_millis(100),
        "the open must fail at {ROUTE_OPEN_ATTEMPT_TIMEOUT:?}, failed after {elapsed:?}"
    );
}

fn target_unavailable() -> ConnResult<RouteHandle> {
    Err(ConnError::Rejected(RejectedMessage::structured(
        subc_protocol::error_codes::TARGET_UNAVAILABLE.into(),
        "supervised_not_registered".into(),
    )))
}

/// A refusal for one target of a module closes only that target's gate. The daemon answers
/// `target_unavailable` for an internal service the module does not provide while the
/// module goes on serving its tool and management routes, so those must stay open.
#[tokio::test]
async fn an_availability_refusal_for_one_target_leaves_the_modules_other_targets_open() {
    let (conn, _write_rx, _pending) = route_open_test_connection();
    let service = GateTarget::InternalService {
        module_id: "aft".into(),
        service_id: "not-registered".into(),
    };
    conn.module_gate(&service)
        .observe(&target_unavailable(), 0, None);
    assert!(conn.module_gate(&service).is_closed());
    assert!(!conn
        .module_gate(&GateTarget::tool_provider("aft"))
        .is_closed());
    assert!(!conn
        .module_gate(&GateTarget::management_surface("aft"))
        .is_closed());
}

/// A writer queue that never drains cannot hold a route.open attempt past its deadline. The
/// attempt holding a module gate's only probe turn gives up within
/// ROUTE_OPEN_ATTEMPT_TIMEOUT, having sent nothing and left no waiter behind, so the gate
/// lets a later probe through, and that one opens once the writer drains again.
#[tokio::test(start_paused = true)]
async fn a_writer_queue_that_never_drains_releases_the_probe_turn_within_the_attempt_bound() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (write_tx, mut write_rx) = mpsc::channel(2);
    // Fill the writer queue; nothing reads it until the first attempt has given up.
    for corr in 0..2 {
        write_tx
            .try_send(
                Frame::build(
                    FrameType::Ping,
                    control_flags(),
                    0,
                    0,
                    1_000 + corr,
                    Vec::new(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
    let identity = BindIdentity::new("/tmp/stalled-writer", "runner", "session");
    // The module is known to be down: the next open through its gate is the probe.
    conn.module_gate(&GateTarget::tool_provider("aft"))
        .observe(&target_unavailable(), 0, None);
    let retry = RouteOpenRetry::within(Duration::from_secs(60), ROUTE_OPEN_RETRY_MAX_INTERVAL);
    let start = time::Instant::now();
    let probe = async {
        let result = conn.route_open_within("aft", &identity, retry).await;
        let failed_at = start.elapsed();
        assert!(
            pending.lock().unwrap().is_empty(),
            "an attempt that never queued its request leaves no waiter behind"
        );
        // The writer drains again from here on; answer the next probe's route.open.
        while write_rx.try_recv().is_ok() {}
        let request = next_route_open_request(&mut write_rx).await;
        answer_route_open(&pending, &request, 9);
        (result, failed_at)
    };
    let later = async {
        time::sleep(Duration::from_secs(1)).await;
        let result = conn.route_open_within("aft", &identity, retry).await;
        (result, start.elapsed())
    };
    // Bounded, so a regression that leaves the attempt stuck fails here instead of hanging.
    let ((first, failed_at), (later, opened_at)) = time::timeout(Duration::from_secs(120), async {
        tokio::join!(probe, later)
    })
    .await
    .expect("the stalled attempt must give up and a later probe must open");
    match first {
        Err(ConnError::Transport(message)) => {
            assert!(message.contains("nothing was sent"), "{message}")
        }
        other => panic!("a stalled writer must fail the attempt locally, got {other:?}"),
    }
    // The probe turn came up within the first 100 ms envelope, and the attempt then gave up
    // at its own deadline.
    assert!(
        failed_at >= ROUTE_OPEN_ATTEMPT_TIMEOUT
            && failed_at <= ROUTE_OPEN_ATTEMPT_TIMEOUT + ROUTE_OPEN_RETRY_INTERVAL,
        "the stalled attempt must give up within its bound, gave up after {failed_at:?}"
    );
    assert_eq!(later.expect("the later probe opens").channel, 9);
    // A failed probe schedules the next one within the following 200 ms envelope.
    assert!(
        opened_at >= failed_at && opened_at <= failed_at + 2 * ROUTE_OPEN_RETRY_INTERVAL,
        "the later probe goes at the next tick, went at {opened_at:?}"
    );
}

/// The connection generation ends while a caller waits at a module's closed gate, before
/// any replacement generation exists: the caller is released at once and its open fails
/// like any open made during a reconnect, instead of sleeping on a gate nothing will probe
/// any more. This goes through the connection's own gate lookup and generation shutdown.
#[tokio::test(start_paused = true)]
async fn a_generation_ending_before_its_replacement_releases_gate_waiters_at_once() {
    let (conn, mut write_rx, _pending) = route_open_test_connection();
    let identity = BindIdentity::new("/tmp/ending-generation", "runner", "session");
    let gate = conn.module_gate(&GateTarget::tool_provider("aft"));
    gate.observe(&target_unavailable(), 0, None);
    time::sleep(ROUTE_OPEN_RETRY_INTERVAL).await;
    // Someone else's probe is running, so the waiter below has no turn coming.
    let probe = match gate.admit(None, None).await {
        GateAdmission::Admitted(ticket) => ticket,
        _ => panic!("the probe turn was due"),
    };
    let start = time::Instant::now();
    let waiter = async {
        let retry = RouteOpenRetry::within(Duration::from_secs(30), ROUTE_OPEN_RETRY_MAX_INTERVAL);
        let result = conn.route_open_within("aft", &identity, retry).await;
        (result, start.elapsed())
    };
    let end_generation = async {
        time::sleep(Duration::from_secs(1)).await;
        let mut state = conn.shared.current.lock().unwrap();
        assert!(deactivate_locked(&mut state));
    };
    let ((result, failed_at), ()) = tokio::join!(waiter, end_generation);
    match result {
        Err(ConnError::Transport(message)) => {
            assert!(message.contains("recovering"), "{message}")
        }
        other => panic!("the released waiter fails like any open mid-reconnect, got {other:?}"),
    }
    assert_eq!(
        failed_at,
        Duration::from_secs(1),
        "released the moment the generation ended"
    );
    assert!(write_rx.try_recv().is_err(), "nothing was sent");
    assert!(conn
        .module_gate(&GateTarget::tool_provider("aft"))
        .is_retired());
    drop(probe);
}

#[test]
fn route_open_retryable_codes_include_inventory_unavailability_not_permanent_errors() {
    use subc_protocol::error_codes::{is_retryable_route_open, *};

    for code in [
        MODULE_RELOADING,
        MODULE_WARMING,
        TARGET_UNAVAILABLE,
        MODULE_TIMEOUT,
        "session_address_unavailable",
    ] {
        assert!(is_retryable_route_open(code), "{code} should retry");
    }
    // UNKNOWN_MODULE IS TERMINAL, by subc-protocol 0.24.0's decision. It used to retry;
    // upstream narrowed it to mean only "no module of this id is registered", with a
    // module that exists but is not yet up reported as module_warming or
    // target_unavailable instead. So a route.open racing a target module's restart still
    // retries -- through those codes -- while a genuine typo in a module id now fails in
    // one round trip rather than spinning for the full 3-second window first.
    //
    // This arm is here because broca does not own the predicate (it is imported, not
    // mirrored), so a change to it lands with no diff in this repository. Asserting the
    // contract we depend on is what turns the next upstream move into a red test rather
    // than a quiet change in how long a failed tool dispatch takes to report.
    assert!(
        !is_retryable_route_open(UNKNOWN_MODULE),
        "unknown_module is terminal"
    );
    assert!(!is_retryable_route_open(MODULE_REMOVED));
    assert!(!is_retryable_route_open("session_address_ambiguous"));
    assert!(!is_retryable_route_open("some_future_code"));

    for code in [
        MODULE_RELOADING,
        MODULE_WARMING,
        TARGET_UNAVAILABLE,
        MODULE_TIMEOUT,
        SCOPE_NOT_SYNCED,
        SCOPE_CHANGED,
        "bind_blocked_by_reader",
        "some_future_code",
    ] {
        assert_eq!(
            is_module_availability_refusal(code),
            [
                TARGET_UNAVAILABLE,
                MODULE_WARMING,
                MODULE_RELOADING,
                MODULE_TIMEOUT
            ]
            .contains(&code),
            "only whole-module refusals belong in the shared gate: {code}"
        );
    }

    // AN UNDECODABLE BODY MUST BE TERMINAL, and this is the arm a future "robustness"
    // edit would break. `decode_error_body` falls back to the raw bytes when the body is
    // not an ErrorBody, so there is no " — " to split and `rejection_code` returns the
    // whole string -- which matches no code, so the route open fails fast.
    //
    // Pinned because the tempting change is a fallback arm treating an unrecognised
    // rejection as retryable "in case it is transient". That turns a malformed reply from
    // a permanent refusal into a retry loop against a daemon that is answering correctly.
    for raw in [
        "internal error: connection reset",
        "",
        "unknown_channel: 10",
    ] {
        assert!(
            !is_retryable_route_open(rejection_code(raw)),
            "an undecodable rejection ({raw:?}) must be terminal, not retried"
        );
    }
}

#[test]
fn route_rejection_code_extracts_the_prefix() {
    assert_eq!(
        rejection_code("module_warming — still starting"),
        "module_warming"
    );
    assert_eq!(rejection_code("module_warming"), "module_warming");
    assert_eq!(rejection_code(""), "");
}

#[test]
fn unstructured_rejections_never_supply_a_refusal_code() {
    let message = decode_error_body("module_reloading — diagnostic text".as_bytes());
    assert_eq!(message.code(), "");
    assert_eq!(ConnError::Rejected(message).code(), None);
}

#[test]
fn unknown_channel_rejection_uses_the_structured_code() {
    let decode = |code: &str, message: &str| {
        let body = serde_json::to_vec(&subc_protocol::ErrorBody::new(code, message)).unwrap();
        decode_error_body(&body)
    };
    let unknown = decode("unknown_channel", "unknown_channel — route gone");
    assert_eq!(unknown.code(), "unknown_channel");
    assert!(rejection_is_unknown_channel(&unknown));

    let misleading = decode("module_warming", "unknown_channel — route gone");
    assert_eq!(misleading.code(), "module_warming");
    assert!(!rejection_is_unknown_channel(&misleading));

    // Only a refusal without a structured ErrorBody code uses the legacy text fallback.
    let legacy: RejectedMessage = "unknown_channel — route gone".into();
    assert!(rejection_is_unknown_channel(&legacy));
    assert!(!rejection_is_unknown_channel(&"unknown_channel: 10".into()));
    assert!(!rejection_is_unknown_channel(
        &"unknown_channels — route gone".into()
    ));
}

/// The refusals the daemon or the provider sends before a call reaches the tool re-open the
/// route and send the call again; one that may come after the call ran never does.
#[test]
fn only_refusals_before_dispatch_reopen_a_tool_route() {
    let decode = |code: &str| {
        let body = serde_json::to_vec(&subc_protocol::ErrorBody::new(code, "refused")).unwrap();
        classify_tool_route_refusal(&decode_error_body(&body))
    };
    for code in [
        "unknown_channel",
        "stale_route_epoch",
        "route_not_bound",
        "route_reclaimed",
    ] {
        assert_eq!(decode(code), ToolRouteRefusal::Reopen, "{code}");
    }
    assert_eq!(
        decode("outcome_unknown_root_reclaimed"),
        ToolRouteRefusal::OutcomeUnknown
    );
    assert_eq!(decode("module_warming"), ToolRouteRefusal::Other);
}

use subc_transport::{
    authenticate_server,
    connection_file::{ConnectionInfo, Endpoint, SCHEMA_VERSION},
};
use tokio::{
    net::TcpListener,
    task::{JoinHandle, JoinSet},
};

/// Tool modules run a closed harness enum; broca's own harness is an opaque durable key
/// component it validates nothing about. Forwarding it verbatim made a caller's private key
/// choice break tool binds in a module they never named, and the runs went exploration-blind
/// while broca reported nothing wrong.
///
/// The alternative fix -- asking callers to change their bind -- would RE-KEY their sessions,
/// because harness is joined into the log, lease and store keys. That is why the translation
/// lives here and must not be "simplified" back to a clone.
#[test]
fn tool_module_bind_presents_the_contract_harness_not_the_callers_key_component() {
    for caller_harness in ["broca", "runner", "alfonso", "anything-a-caller-picks"] {
        let identity = BindIdentity {
            project_root: std::path::PathBuf::from("/tmp/p"),
            harness: caller_harness.to_string(),
            session: "s1".to_string(),
        };
        let outbound = to_subc_bind(
            &identity,
            &RouteTarget::ToolProvider {
                module_id: "other".to_string(),
            },
            None,
        );

        assert_eq!(
            outbound.harness, TOOL_MODULE_HARNESS,
            "a {caller_harness:?} bind must still attach as {TOOL_MODULE_HARNESS:?}"
        );
        // The session still carries the caller's identity; scope supplies project attribution.
        assert_eq!(outbound.session, identity.session);
    }
}

#[test]
fn aft_tool_bind_forwards_only_accepted_session_harnesses() {
    let catalog = aft_harness::catalog(true);
    for (caller_harness, expected_harness) in [
        ("broca", "broca"),
        ("runner", "runner"),
        ("opencode", "runner"),
        ("pi", "runner"),
        ("conformance", "runner"),
        ("mcp:client", "runner"),
        ("fed:client", "runner"),
        ("Broca", "runner"),
        ("broca-custom", "runner"),
        ("anything-a-caller-picks", "runner"),
    ] {
        let identity = BindIdentity::new("/project", caller_harness, "alfonso:bg_x");
        let target = RouteTarget::ToolProvider {
            module_id: "aft".into(),
        };
        let outbound =
            ToolProviderRoutePolicy::for_provider(&catalog, "aft", false).bind("aft", &identity);
        assert_eq!(outbound.harness, expected_harness, "{caller_harness}");
        assert_eq!(outbound.session, "alfonso:bg_x");
        assert_eq!(
            to_subc_bind(&identity, &target, None).harness,
            "runner",
            "an opener without a catalog snapshot retains the old identity"
        );

        // The session's own compaction and hook providers keep their existing policy,
        // even when that provider is AFT and the harness is outside the callback allowlist.
        assert_eq!(
            ToolProviderRoutePolicy {
                declares_role: true,
                session_identity: true,
                aft_harness: None,
            }
            .bind("aft", &identity)
            .harness,
            caller_harness,
        );
        for (role, version) in [
            (broca_core::compaction_provider::ROLE, "v1"),
            (
                cortexkit_role_step_transform_provider::ROLE,
                cortexkit_role_step_transform_provider::VERSION,
            ),
        ] {
            let roles = std::collections::BTreeMap::from([(role.to_owned(), version.into())]);
            let role_bind = to_subc_bind(&identity, &target, Some(&roles));
            assert_eq!(role_bind.harness, caller_harness, "{role}");
            assert_eq!(role_bind.session, "alfonso:bg_x");
        }
    }
}

/// The translation above is a TOOL-MODULE workaround and was applied to every route target,
/// so broca's own CLIs could not express their harness: `--harness broca` produced an export
/// fact reading `runner` (measured 2026-09-20 on the deployed daemon).
///
/// Both management consumers were checked before scoping. claustrum's `RouteBind`
/// destructures `{ route_channel, epoch, principal, .. }` and discards the identity; mc
/// stores it and reads it only on the MCP facade path, and answered "re-keys nothing on our
/// side and is safe to scope".
#[test]
fn a_management_bind_carries_the_callers_own_harness_rather_than_the_tool_constant() {
    for caller_harness in ["broca", "ck-app", "alfonso", "anything-a-caller-picks"] {
        let identity = BindIdentity {
            project_root: std::path::PathBuf::from("/tmp/p"),
            harness: caller_harness.to_string(),
            session: "s1".to_string(),
        };

        for target in [
            RouteTarget::ManagementSurface {
                module_id: "broca".to_string(),
            },
            RouteTarget::InternalService {
                module_id: "m".to_string(),
                service_id: "s".to_string(),
            },
        ] {
            let outbound = to_subc_bind(&identity, &target, None);
            assert_eq!(
                outbound.harness, caller_harness,
                "a management/internal bind must carry {caller_harness:?}, not the tool constant"
            );
        }
    }

    // NON-VACUITY: the two arms must actually differ, or this test passes under a build that
    // translates nothing and the tool test passes under one that translates everything only
    // because its callers happen to pass "runner".
    let identity = BindIdentity {
        project_root: std::path::PathBuf::from("/tmp/p"),
        harness: "ck-app".to_string(),
        session: "s1".to_string(),
    };
    let tool = to_subc_bind(
        &identity,
        &RouteTarget::ToolProvider {
            module_id: "aft".to_string(),
        },
        None,
    );
    let mgmt = to_subc_bind(
        &identity,
        &RouteTarget::ManagementSurface {
            module_id: "broca".to_string(),
        },
        None,
    );
    assert_ne!(
        tool.harness, mgmt.harness,
        "the whole point of the scoping is that these two disagree"
    );
}

/// A route that declares the compaction-provider role binds the session's real identity,
/// the one its Setup body reports. A provider calls back about the conversation under that
/// identity, and a `runner` bind beside a Setup naming the real harness gave one session
/// two identities on the wire: the provider's read-back found an empty, never-written
/// lineage. Other role routes (step-transform hooks) and plain tool routes keep `runner`,
/// because tool modules key per-session state on it.
#[test]
fn a_compaction_provider_route_binds_the_sessions_real_identity() {
    use std::collections::BTreeMap;
    let role = |name: &str| BTreeMap::from([(name.to_owned(), "v1".to_owned())]);
    let target = RouteTarget::ToolProvider {
        module_id: "magic-context".to_string(),
    };
    for caller_harness in ["broca", "conformance", "anything-a-caller-picks"] {
        let identity = BindIdentity {
            project_root: std::path::PathBuf::from("/tmp/p"),
            harness: caller_harness.to_string(),
            session: "s1".to_string(),
        };
        for version in ["v1", "v2"] {
            let compaction = BTreeMap::from([(
                broca_core::compaction_provider::ROLE.to_owned(),
                version.to_owned(),
            )]);
            let outbound = to_subc_bind(&identity, &target, Some(&compaction));
            assert_eq!(outbound.harness, caller_harness);
            assert_eq!(outbound.session, identity.session);
        }

        for other in [
            Some(role("step-transform-provider")),
            Some(role("tool-provider")),
            None,
        ] {
            assert_eq!(
                to_subc_bind(&identity, &target, other.as_ref()).harness,
                TOOL_MODULE_HARNESS,
                "{other:?} must keep the tool-module harness"
            );
        }
    }
}

fn frame(ty: FrameType, channel: u16, epoch: u32, corr: u64) -> Frame {
    Frame::build(ty, data_flags(), channel, epoch, corr, br#"{}"#.to_vec()).unwrap()
}

fn goodbye_frame(channel: u16, epoch: u32) -> Frame {
    Frame::build(
        FrameType::Goodbye,
        control_flags(),
        channel,
        epoch,
        0,
        Vec::new(),
    )
    .unwrap()
}

fn test_connection(
    write_tx: mpsc::Sender<Frame>,
    pending: PendingMap,
    route_epochs: RouteEpochs,
    conn: ConnToken,
    corr: u64,
) -> SubcConnection {
    let runtime = Arc::new(ConnectionRuntime {
        close_observer: Mutex::new(Weak::new()),
        closing_routes: Mutex::new(HashMap::new()),
        #[cfg(feature = "test-route-close")]
        fail_next_goodbye: std::sync::atomic::AtomicBool::new(false),
        write_tx,
        pending,
        route_epochs,
        write_confirmations: Arc::new(Mutex::new(HashMap::new())),
        conn,
        shutdown: CancellationToken::new(),
        capabilities: tokio::sync::OnceCell::new(),
    });
    let (reconnect_tx, _reconnect_rx) = mpsc::unbounded_channel();
    SubcConnection {
        shared: Arc::new(ConnectionShared {
            aft_harness_answer: Mutex::new(None),
            person_yields: Mutex::default(),
            catalog: Default::default(),
            #[cfg(test)]
            late_goodbye_handoff: Mutex::new(None),
            route_closer: std::sync::OnceLock::new(),
            connection_file_path: PathBuf::new(),
            current: Mutex::new(GenerationState::new(runtime)),
            corr: AtomicU64::new(corr),
            reconnect_tx,
            probe_timeout: Duration::from_millis(25),
            route_open_bind_relay_grace: Duration::from_millis(50),
            consumer_identity: None,
            reconnect_attempts: AtomicU64::new(0),
            probe_windows_started: AtomicU64::new(0),
        }),
        module_gates: ModuleGates::default(),
        #[cfg(feature = "test-clock")]
        route_open_attempts: None,
    }
}

async fn wait_for_probe_window(conn: &SubcConnection) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while conn.shared.probe_windows_started.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the confirmed Ping must open its conviction window");
}

#[derive(Clone, Copy)]
enum ControlStubReply {
    Blackhole,
    SlowRouteOpen(Duration),
}

struct ControlTcpStub {
    connection_file: PathBuf,
    accepts: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    pings: Arc<AtomicUsize>,
    errors: Arc<Mutex<Vec<String>>>,
    server: JoinHandle<()>,
}

impl ControlTcpStub {
    async fn start(reply: ControlStubReply) -> Self {
        static NEXT_STUB: AtomicUsize = AtomicUsize::new(1);

        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind control TCP stub");
        let port = listener.local_addr().expect("control stub address").port();
        let key = vec![0x5c; 32];
        let daemon_id = [0xc5; 16];
        let connection_file = broca_test_support::scratch_root().join(format!(
            "broca-control-timeout-stub-{}-{}.json",
            std::process::id(),
            NEXT_STUB.fetch_add(1, Ordering::Relaxed)
        ));
        subc_transport::connection_file::write_atomic(
            &connection_file,
            &ConnectionInfo {
                schema: SCHEMA_VERSION,
                wire_version: None,
                endpoints: vec![Endpoint {
                    host: "127.0.0.1".into(),
                    port,
                }],
                key: key.clone(),
                daemon_id,
                pid: std::process::id(),
                daemon_ver: "control-timeout-stub".into(),
            },
        )
        .expect("write control stub connection file");

        let accepts = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let pings = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let server = {
            let accepts = Arc::clone(&accepts);
            let requests = Arc::clone(&requests);
            let pings = Arc::clone(&pings);
            let errors = Arc::clone(&errors);
            tokio::spawn(async move {
                let mut handlers = JoinSet::new();
                loop {
                    let (stream, _) = match listener.accept().await {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            errors.lock().unwrap().push(error.to_string());
                            return;
                        }
                    };
                    accepts.fetch_add(1, Ordering::SeqCst);
                    let key = key.clone();
                    let requests = Arc::clone(&requests);
                    let pings = Arc::clone(&pings);
                    let errors = Arc::clone(&errors);
                    handlers.spawn(async move {
                        if let Err(error) = serve_control_stub_connection(
                            stream, daemon_id, key, reply, requests, pings,
                        )
                        .await
                        {
                            errors.lock().unwrap().push(error);
                        }
                    });
                }
            })
        };

        Self {
            connection_file,
            accepts,
            requests,
            pings,
            errors,
            server,
        }
    }

    fn accept_count(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }

    fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    fn ping_count(&self) -> usize {
        self.pings.load(Ordering::SeqCst)
    }

    fn assert_healthy(&self) {
        assert!(
            self.errors.lock().unwrap().is_empty(),
            "control TCP stub failed: {:?}",
            self.errors.lock().unwrap().as_slice()
        );
    }
}

impl Drop for ControlTcpStub {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_file(&self.connection_file);
    }
}

async fn serve_control_stub_connection(
    mut stream: TcpStream,
    daemon_id: [u8; 16],
    key: Vec<u8>,
    reply: ControlStubReply,
    requests: Arc<AtomicUsize>,
    pings: Arc<AtomicUsize>,
) -> Result<(), String> {
    authenticate_server(
        &mut stream,
        &key,
        &daemon_id,
        "control-timeout-stub",
        broca_test_support::STAND_IN_AUTH_DEADLINE,
    )
    .await
    .map_err(|error| format!("authenticate control stub client: {error}"))?;

    while let Some(frame) = read_frame(&mut stream)
        .await
        .map_err(|error| format!("read control stub frame: {error}"))?
    {
        match (frame.header.channel, frame.header.ty) {
            (0, FrameType::Request) => {
                requests.fetch_add(1, Ordering::SeqCst);
                if let ControlStubReply::SlowRouteOpen(delay) = reply {
                    time::sleep(delay).await;
                    let body = serde_json::to_vec(&ClientControlResponse::RouteOpen {
                        route_channel: 9,
                        route_epoch: 1,
                        request_window: None,
                        watch_window: None,
                    })
                    .map_err(|error| format!("encode route.open response: {error}"))?;
                    let response = Frame::build(
                        FrameType::Response,
                        control_flags(),
                        0,
                        0,
                        frame.header.corr,
                        body,
                    )
                    .map_err(|error| format!("build route.open response: {error}"))?;
                    write_frame(&mut stream, &response)
                        .await
                        .map_err(|error| format!("write route.open response: {error}"))?;
                }
            }
            (0, FrameType::Ping) => {
                pings.fetch_add(1, Ordering::SeqCst);
                if matches!(reply, ControlStubReply::SlowRouteOpen(_)) {
                    let pong = Frame::build(
                        FrameType::Pong,
                        frame.header.flags,
                        0,
                        0,
                        frame.header.corr,
                        Vec::new(),
                    )
                    .map_err(|error| format!("build control stub Pong: {error}"))?;
                    write_frame(&mut stream, &pong)
                        .await
                        .map_err(|error| format!("write control stub Pong: {error}"))?;
                }
            }
            (_, FrameType::Goodbye) => {}
            (_, other) => return Err(format!("unexpected control stub frame: {other:?}")),
        }
    }
    Ok(())
}

async fn connect_control_stub(
    stub: &ControlTcpStub,
    probe_timeout: Duration,
    bind_relay_grace: Duration,
) -> SubcConnection {
    SubcConnection::connect_with_timeouts(&stub.connection_file, probe_timeout, bind_relay_grace)
        .await
        .expect("connect to control TCP stub")
}

#[tokio::test]
async fn consumer_connect_tries_second_endpoint() {
    let stub = ControlTcpStub::start(ControlStubReply::Blackhole).await;
    let first = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut info = connection_file::read(&stub.connection_file).unwrap();
    info.endpoints.insert(
        0,
        Endpoint {
            host: "127.0.0.1".into(),
            port: first.local_addr().unwrap().port(),
        },
    );
    drop(first);
    connection_file::write_atomic(&stub.connection_file, &info).unwrap();
    let conn = SubcConnection::connect(&stub.connection_file)
        .await
        .unwrap();
    assert!(conn.shared.current.lock().unwrap().active);
    assert_eq!(stub.accept_count(), 1);
}

async fn wait_for_accept_count(stub: &ControlTcpStub, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while stub.accept_count() < expected {
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("TCP accept count did not reach {expected}"));
}

fn stream_data_frame(channel: u16, epoch: u32, corr: u64, sequence: u32) -> Frame {
    Frame::build(
        FrameType::StreamData,
        data_flags(),
        channel,
        epoch,
        corr,
        sequence.to_be_bytes().to_vec(),
    )
    .unwrap()
}

fn test_subscribe_stream(
    pending: &PendingMap,
    routes: &RouteEpochs,
    write_tx: &mpsc::Sender<Frame>,
    route: RouteHandle,
    corr: u64,
) -> SubscribeStream {
    let (registration, rx, lagged) =
        PendingRegistration::new_stream(Arc::clone(pending), (route.channel, route.epoch, corr))
            .unwrap();
    SubscribeStream {
        rx: Some(rx),
        lagged,
        registration: Some(registration),
        done: false,
        write_tx: write_tx.clone(),
        route_epochs: Arc::clone(routes),
        route,
        corr,
    }
}

fn encoded_body(name: &str, arguments: &serde_json::Value, id: Option<&str>) -> serde_json::Value {
    let bytes = serde_json::to_vec(&ToolCallRequest {
        name: name.to_string(),
        arguments: arguments.clone(),
        tool_call_id: id.map(str::to_string),
        progress_token: None,
        call_key: None,
        schema_pin: None,
        origin: None,
        preset: None,
        output_refs: None,
    })
    .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// WITHOUT an id the wire bytes are byte-identical to the pre-2026-09-03 envelope.
///
/// This is what made the migration to the shared `ToolCallRequest` safe to ship without a
/// coordinated window: every existing module reader sees exactly what it saw before, so
/// the change is inert until someone reads the new field. The two-key length check also
/// pins `call_key` as absent, not null, on the unkeyed API.
#[test]
fn tool_call_body_without_an_id_preserves_the_legacy_shape() {
    let name = "apply_patch_雪";
    let arguments = serde_json::json!({
        "changes": {
            "lines": ["naïve café", "雪", "🦀"],
            "metadata": { "attempt": 2, "enabled": true }
        }
    });
    let legacy = serde_json::json!({ "name": name, "arguments": &arguments });
    let direct = encoded_body(name, &arguments, None);

    assert_eq!(direct, legacy);
    let envelope = direct.as_object().unwrap();
    assert_eq!(envelope.len(), 2);
    assert!(envelope.contains_key("name"));
    assert!(envelope.contains_key("arguments"));
}

/// WITH an id the encoded body carries it, under the canonical snake spelling.
///
/// WHAT THIS DOES NOT COVER, stated because I proved it rather than assumed it: this builds
/// the `ToolCallRequest` itself, so mutating `tool_call`'s body construction to
/// `tool_call_id: None` leaves it GREEN. It pins the serialization contract -- snake
/// spelling, id present, progress_token absent rather than null -- and says nothing about
/// whether the production encode path uses its parameter.
///
/// The seam above it IS covered: subc_plane's `dispatch_puts_the_provider_id_on_the_wire`
/// reddens when the plane passes `None`. So the two ends are pinned and the middle -- the
/// four lines inside `tool_call` -- is not, because reaching it needs a live route and a
/// driven socket. That gap closes with the live-turn wire capture on the deployed binary,
/// which is the proof owed to SUBC and ALF anyway; a unit test that faked the transport
/// would assert less than the real dispatch does.
///
/// Paired with the test above ON PURPOSE. A suite that only ever encoded one optional shape
/// could not tell a working emit from a field that is silently skipped -- precisely the
/// state broca shipped in for months, with every module downstream either synthesizing an
/// identity or refusing the call.
#[test]
fn tool_call_body_carries_the_provider_id_when_one_is_supplied() {
    let arguments = serde_json::json!({"filePath": "Cargo.toml"});
    let direct = encoded_body("read", &arguments, Some("toolu_01ABC"));
    let envelope = direct.as_object().unwrap();

    assert_eq!(
        envelope
            .get("tool_call_id")
            .and_then(serde_json::Value::as_str),
        Some("toolu_01ABC"),
        "the provider id must reach the wire under the canonical snake spelling"
    );
    assert_eq!(
        envelope.len(),
        3,
        "progress_token must stay absent, not null"
    );
    assert!(!envelope.contains_key("progress_token"));
}

#[tokio::test]
async fn tool_call_envelope_sends_the_frozen_preset_or_omits_it() {
    for preset in [Some("worker"), None] {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (write_tx, mut write_rx) = mpsc::channel(1);
        let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
        let route = RouteHandle::new(9, 1, ConnToken(1));
        let task = tokio::spawn(async move {
            conn.tool_call_with_preset(
                &route,
                "read",
                &serde_json::json!({}),
                Some("call"),
                Duration::from_secs(1),
                preset,
            )
            .await
        });
        let request = write_rx.recv().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(
            body.get("preset").and_then(serde_json::Value::as_str),
            preset
        );
        assert_eq!(
            body.as_object().unwrap().contains_key("preset"),
            preset.is_some()
        );
        let sender = match pending.lock().unwrap().get(&(9, 1, request.header.corr)) {
            Some(PendingEntry::Live { sender, .. }) => sender.clone(),
            _ => panic!("call registered its reply waiter"),
        };
        sender
            .send(
                Frame::build(
                    FrameType::Response,
                    data_flags(),
                    9,
                    1,
                    request.header.corr,
                    br#"{"content":[]}"#.to_vec(),
                )
                .unwrap(),
            )
            .unwrap();
        task.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn tool_dispatch_log_identifies_route_request_and_operation_without_arguments() {
    let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
    let capture = broca_test_support::LogCapture::new();
    let signals = broca_core::tool_plane::DispatchSignals::default();
    let dispatch = capture.scope_future(conn.tool_call_raw_yieldable_guarded(
        &route,
        "lookup",
        r#"{"secret":"argument content"}"#,
        Some("provider-call"),
        Duration::from_secs(1),
        false,
        None,
        None,
        None,
        &signals,
        None,
    ));
    let receive = async {
        let request = module.next_request().await;
        assert!(module.reply(&request, br#"{"content":[]}"#.to_vec()));
        request
    };
    let (result, request) = tokio::join!(dispatch, receive);
    assert!(result.is_ok(), "{result:?}");

    let logs: Vec<_> = capture
        .events()
        .into_iter()
        .filter(|event| {
            event.target == "subc.wire"
                && event
                    .fields
                    .get("stage")
                    .is_some_and(|stage| stage == "tool_dispatch")
        })
        .collect();
    assert_eq!(logs.len(), 1, "{logs:?}");
    let event = &logs[0];
    assert_eq!(event.level, tracing::Level::DEBUG);
    assert_eq!(event.fields["stage"], "tool_dispatch");
    assert_eq!(event.fields["route_channel"], "5");
    assert_eq!(
        event.fields["correlation_id"],
        request.header.corr.to_string()
    );
    assert_eq!(event.fields["operation"], "lookup");
    let fields = format!("{:?}", event.fields);
    assert!(!fields.contains("secret"));
    assert!(!fields.contains("argument content"));
}

#[tokio::test]
async fn route_request_raw_returns_response_body_unchanged() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, mut write_rx) = mpsc::channel(1);
    let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
    let route = RouteHandle::new(9, 1, ConnToken(1));

    let task = tokio::spawn(async move {
        conn.route_request_raw(&route, br#"{"method":"run.status","params":{}}"#.to_vec())
            .await
    });
    let request = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
        .await
        .expect("route request should be queued")
        .expect("writer channel should remain open");
    let response_body = br#"{"result":{"z":1,"a":2}}"#.to_vec();
    let response_tx = {
        let waiters = pending.lock().unwrap();
        match waiters.get(&(9, 1, request.header.corr)) {
            Some(PendingEntry::Live { sender, .. }) => sender.clone(),
            _ => panic!("route request should register a live response waiter"),
        }
    };
    let response = Frame::build(
        FrameType::Response,
        data_flags(),
        9,
        1,
        request.header.corr,
        response_body.clone(),
    )
    .unwrap();
    response_tx.send(response).unwrap();

    assert_eq!(task.await.unwrap().unwrap(), response_body);
}

#[tokio::test]
async fn terminal_dispatch_observer_stays_clear_before_writer_enqueue() {
    use futures::FutureExt;
    let pending: PendingMap = Arc::default();
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, mut write_rx) = mpsc::channel(1);
    write_tx.try_send(frame(FrameType::Push, 0, 0, 0)).unwrap();
    let conn = test_connection(write_tx, pending.clone(), routes, ConnToken(1), 1);
    let route = RouteHandle::new(9, 1, ConnToken(1));
    let dispatched = std::sync::atomic::AtomicBool::new(false);
    {
        let request = conn.route_request_terminal_observed(
            &route,
            br#"{"method":"route.account_for","params":{"mode":"continue"}}"#.to_vec(),
            || dispatched.store(true, Ordering::Release),
        );
        tokio::pin!(request);
        assert!(request.as_mut().now_or_never().is_none());
        assert!(
            !dispatched.load(Ordering::Acquire),
            "a blocked queue is not dispatch evidence"
        );
    }
    assert_eq!(write_rx.try_recv().unwrap().header.ty, FrameType::Push);
    assert!(write_rx.try_recv().is_err());
    assert!(pending.lock().unwrap().is_empty());
}

#[tokio::test]
async fn terminal_dispatch_observer_fires_for_an_enqueued_unanswered_request() {
    use futures::FutureExt;
    let pending: PendingMap = Arc::default();
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, mut write_rx) = mpsc::channel(1);
    let conn = test_connection(write_tx, pending.clone(), routes, ConnToken(1), 1);
    let route = RouteHandle::new(9, 1, ConnToken(1));
    let dispatched = std::sync::atomic::AtomicBool::new(false);
    {
        let request = conn.route_request_terminal_observed(
            &route,
            br#"{"method":"route.account_for","params":{"mode":"continue"}}"#.to_vec(),
            || dispatched.store(true, Ordering::Release),
        );
        tokio::pin!(request);
        assert!(
            request.as_mut().now_or_never().is_none(),
            "the terminal reply is still absent"
        );
        assert!(dispatched.load(Ordering::Acquire));
        let queued = write_rx.try_recv().unwrap();
        assert_eq!(queued.header.ty, FrameType::Request);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&queued.body).unwrap()["params"]["mode"],
            "continue"
        );
        assert!(pending
            .lock()
            .unwrap()
            .contains_key(&(9, 1, queued.header.corr)));
    }
    assert!(pending.lock().unwrap().is_empty());
}

#[tokio::test]
async fn route_unary_bytes_preserves_request_and_response_buffers() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, mut write_rx) = mpsc::channel(1);
    let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
    let route = RouteHandle::new(9, 1, ConnToken(1));
    let body = br#"{"z":1,"a":2}"#.to_vec();
    let expected_body = body.clone();

    let task = tokio::spawn(async move {
        conn.route_unary_bytes(&route, body, Duration::from_secs(1))
            .await
    });
    let request = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
        .await
        .expect("unary request should be queued")
        .expect("writer channel should remain open");
    assert_eq!(request.body, expected_body);

    let response_tx = {
        let waiters = pending.lock().unwrap();
        match waiters.get(&(9, 1, request.header.corr)) {
            Some(PendingEntry::Live { sender, .. }) => sender.clone(),
            _ => panic!("unary request should register a live response waiter"),
        }
    };
    let response_body = br#"{"ck_messages":[],"diagnostic":{"z":1,"a":2}}"#.to_vec();
    let response = Frame::build(
        FrameType::Response,
        data_flags(),
        9,
        1,
        request.header.corr,
        response_body.clone(),
    )
    .unwrap();
    response_tx.send(response).unwrap();

    assert_eq!(task.await.unwrap().unwrap(), response_body);
}

#[tokio::test]
async fn reader_task_closes_pending_subscribe_streams_on_connection_close() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
    let route = RouteHandle::new(7, 2, ConnToken(1));
    let (write_tx, _write_rx) = mpsc::channel(4);
    let mut stream = test_subscribe_stream(&pending, &routes, &write_tx, route, 11);

    let (client, server) = tokio::io::duplex(1024);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));
    drop(client);

    let next = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("stream should observe connection teardown instead of hanging")
        .expect("connection close should end the stream cleanly");
    assert_eq!(next, None);
    reader.await.unwrap();
    assert!(pending.lock().unwrap().is_empty());
}

#[tokio::test]
async fn superseded_reader_cannot_stamp_replacement_inbound_watermark() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (write_tx, _write_rx) = mpsc::channel(4);
    let conn = test_connection(
        write_tx.clone(),
        Arc::clone(&pending),
        Arc::clone(&routes),
        ConnToken(1),
        1,
    );
    let old_runtime = {
        let state = conn.shared.current.lock().unwrap();
        Arc::clone(&state.runtime)
    };
    let (mut client, server) = tokio::io::duplex(1024);
    let reader = tokio::spawn(reader_task(
        server,
        pending,
        routes,
        write_tx,
        Some(ReaderGeneration {
            shared: Arc::downgrade(&conn.shared),
            conn: old_runtime.conn,
            shutdown: old_runtime.shutdown.clone(),
            test_gate: None,
        }),
    ));

    let (replacement_write_tx, _replacement_write_rx) = mpsc::channel(4);
    let replacement = Arc::new(ConnectionRuntime {
        close_observer: Mutex::new(Weak::new()),
        closing_routes: Mutex::new(HashMap::new()),
        #[cfg(feature = "test-route-close")]
        fail_next_goodbye: std::sync::atomic::AtomicBool::new(false),
        write_tx: replacement_write_tx,
        pending: Arc::new(Mutex::new(HashMap::new())),
        route_epochs: Arc::new(Mutex::new(HashMap::new())),
        write_confirmations: Arc::new(Mutex::new(HashMap::new())),
        conn: ConnToken(2),
        shutdown: CancellationToken::new(),
        capabilities: tokio::sync::OnceCell::new(),
    });
    let anchor = Instant::now() - Duration::from_secs(60);
    let mut replacement_state = GenerationState::new(replacement);
    replacement_state.last_inbound = anchor;
    *conn.shared.current.lock().unwrap() = replacement_state;

    write_frame(&mut client, &frame(FrameType::Response, 0, 0, 9))
        .await
        .unwrap();
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), reader)
        .await
        .expect("the superseded reader must stop on its next buffered frame")
        .unwrap();

    let state = conn.shared.current.lock().unwrap();
    assert_eq!(state.runtime.conn, ConnToken(2));
    assert_eq!(
        state.last_inbound, anchor,
        "an old socket's buffered frame cannot exonerate the current generation"
    );
}

#[tokio::test]
async fn inactive_current_reader_cannot_stamp_inbound_watermark() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (write_tx, _write_rx) = mpsc::channel(4);
    let conn = test_connection(
        write_tx.clone(),
        Arc::clone(&pending),
        Arc::clone(&routes),
        ConnToken(1),
        1,
    );
    let runtime = {
        let state = conn.shared.current.lock().unwrap();
        Arc::clone(&state.runtime)
    };
    let anchor = Instant::now() - Duration::from_secs(60);
    conn.shared.current.lock().unwrap().last_inbound = anchor;
    let gate = Arc::new(ReaderTestGate::default());
    let (mut client, server) = tokio::io::duplex(1024);
    let reader = tokio::spawn(reader_task(
        server,
        pending,
        routes,
        write_tx,
        Some(ReaderGeneration {
            shared: Arc::downgrade(&conn.shared),
            conn: runtime.conn,
            shutdown: runtime.shutdown.clone(),
            test_gate: Some(Arc::clone(&gate)),
        }),
    ));

    let frame_read = gate.frame_read.notified();
    write_frame(&mut client, &frame(FrameType::Response, 0, 0, 9))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), frame_read)
        .await
        .expect("the reader must reach the post-read generation check");
    {
        let mut state = conn.shared.current.lock().unwrap();
        assert!(deactivate_locked(&mut state));
    }
    gate.proceed.notify_one();
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), reader)
        .await
        .expect("the inactive reader must stop before stamping inbound")
        .unwrap();

    let state = conn.shared.current.lock().unwrap();
    assert!(!state.active);
    assert_eq!(
        state.last_inbound, anchor,
        "a frame read before teardown cannot exonerate an inactive generation"
    );
}

#[tokio::test]
async fn lagged_subscribe_detaches_without_blocking_unrelated_rpc() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2), (9, 1)])));
    let stream_route = RouteHandle::new(7, 2, ConnToken(1));
    let rpc_route = RouteHandle::new(9, 1, ConnToken(1));
    let stream_corr = 11;
    let (write_tx, mut write_rx) = mpsc::channel(4);
    let conn = test_connection(
        write_tx.clone(),
        Arc::clone(&pending),
        Arc::clone(&routes),
        ConnToken(1),
        20,
    );
    let mut stream = test_subscribe_stream(&pending, &routes, &write_tx, stream_route, stream_corr);

    let (mut client, server) = tokio::io::duplex(64 * 1024);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));
    let rpc = tokio::spawn(async move { conn.route_request(&rpc_route, b"{}".to_vec()).await });
    let request = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
        .await
        .expect("the unrelated RPC should send its request")
        .expect("the local writer should remain open");

    tokio::time::timeout(Duration::from_secs(5), async {
        for sequence in 0..=SUBSCRIBE_STREAM_BUFFER as u32 {
            write_frame(&mut client, &stream_data_frame(7, 2, stream_corr, sequence))
                .await
                .unwrap();
        }
        write_frame(
            &mut client,
            &frame(FrameType::Response, 9, 1, request.header.corr),
        )
        .await
        .unwrap();
    })
    .await
    .expect("a full subscribe queue must not block socket ingress");

    let response = tokio::time::timeout(Duration::from_secs(1), rpc)
        .await
        .expect("the unrelated RPC should complete while the subscriber is wedged")
        .unwrap()
        .unwrap();
    assert_eq!(response, serde_json::json!({}));

    let error = stream
        .next()
        .await
        .expect_err("overflow must detach the subscriber instead of draining stale frames");
    assert!(matches!(
        &error,
        ConnError::StreamLagged { buffered } if *buffered == SUBSCRIBE_STREAM_BUFFER
    ));
    assert!(error
        .to_string()
        .contains("resubscribe from your last durable cursor"));

    let goodbye = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
        .await
        .expect("overflow should queue a best-effort route close")
        .expect("the local writer should remain open");
    assert_eq!(goodbye.header.ty, FrameType::Goodbye);
    assert_eq!((goodbye.header.channel, goodbye.header.epoch), (7, 2));
    assert!(!pending.lock().unwrap().contains_key(&(7, 2, stream_corr)));
    assert!(!routes.lock().unwrap().contains_key(&7));

    drop(client);
    reader.await.unwrap();
}

#[tokio::test]
async fn subscribe_buffer_preserves_every_frame_below_the_limit() {
    const FRAME_COUNT: u32 = 128;

    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
    let route = RouteHandle::new(7, 2, ConnToken(1));
    let stream_corr = 11;
    let (write_tx, _write_rx) = mpsc::channel(4);
    let mut stream = test_subscribe_stream(&pending, &routes, &write_tx, route, stream_corr);
    let (mut client, server) = tokio::io::duplex(16 * 1024);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));

    for sequence in 0..FRAME_COUNT {
        write_frame(&mut client, &stream_data_frame(7, 2, stream_corr, sequence))
            .await
            .unwrap();
    }
    write_frame(&mut client, &frame(FrameType::StreamEnd, 7, 2, stream_corr))
        .await
        .unwrap();

    for expected in 0..FRAME_COUNT {
        let body = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("a buffered frame should be available")
            .unwrap()
            .expect("the stream should not end before all data frames are drained");
        assert_eq!(body, expected.to_be_bytes());
    }
    assert_eq!(stream.next().await.unwrap(), None);
    assert!(pending.lock().unwrap().is_empty());

    drop(stream);
    assert!(!routes.lock().unwrap().contains_key(&7));
    drop(client);
    reader.await.unwrap();
}

#[tokio::test]
async fn subscribe_stream_turns_a_route_gone_goodbye_into_an_error() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
    let route = RouteHandle::new(7, 2, ConnToken(1));
    let (write_tx, _write_rx) = mpsc::channel(4);
    let mut stream = test_subscribe_stream(&pending, &routes, &write_tx, route, 11);

    let (mut client, server) = tokio::io::duplex(1024);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));
    write_frame(&mut client, &goodbye_frame(7, 2))
        .await
        .unwrap();

    let next = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("stream should observe route teardown instead of hanging");
    assert!(matches!(next, Err(ConnError::Transport(message)) if message.contains("route gone")));
    drop(client);
    reader.await.unwrap();
}

#[tokio::test]
async fn local_route_close_wakes_sibling_calls_only_in_its_epoch() {
    use futures::FutureExt;
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2), (8, 3)])));
    let (write_tx, mut write_rx) = mpsc::channel(1);
    write_tx.try_send(frame(FrameType::Push, 0, 0, 0)).unwrap();
    let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
    let route = RouteHandle::new(7, 2, ConnToken(1));
    let (_, _first, mut first_rx) = conn.register_route(&route, (7, 2, 5)).unwrap();
    let (_, _sibling, mut sibling_rx) = conn.register_route(&route, (7, 2, 6)).unwrap();
    let (_old, mut old_rx) =
        PendingRegistration::new(Arc::clone(&pending), (7, 1, 5), false, true, None).unwrap();
    let (_, _other, mut other_rx) = conn
        .register_route(&RouteHandle::new(8, 3, ConnToken(1)), (8, 3, 5))
        .unwrap();

    let close = conn.route_close(&route);
    tokio::pin!(close);
    assert!(close.as_mut().now_or_never().is_none(), "egress is full");
    for rx in [&mut first_rx, &mut sibling_rx] {
        let terminal = rx
            .try_recv()
            .expect("local close must fan out before enqueue");
        assert_eq!(terminal.header.ty, FrameType::Goodbye);
        assert_eq!((terminal.header.channel, terminal.header.epoch), (7, 2));
    }
    assert!(matches!(
        old_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        other_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(!conn.route_is_open(&route));
    assert!(conn.register_route(&route, (7, 2, 7)).is_err());
    write_rx.recv().await.unwrap();
    close.await.unwrap();
    assert_eq!(write_rx.recv().await.unwrap().header.ty, FrameType::Goodbye);
}

#[tokio::test]
async fn route_gone_goodbye_fans_out_only_within_its_epoch() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
    let (_registration, mut rx) =
        PendingRegistration::new(Arc::clone(&pending), (7, 2, 5), false, true, None).unwrap();
    let (write_tx, _write_rx) = mpsc::channel(4);
    let (mut client, server) = tokio::io::duplex(1024);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));
    write_frame(&mut client, &goodbye_frame(7, 2))
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("the corr=0 GOODBYE must reach a waiter in the same route generation")
        .expect("a frame, not a channel close");
    assert_eq!(received.header.ty, FrameType::Goodbye);
    drop(client);
    reader.await.unwrap();
}

#[tokio::test]
async fn stale_epoch_ingress_does_not_settle_a_live_waiter() {
    // Deltas, not absolutes: the gauge is process-global by design, so a parallel test
    // dropping its own frames would break an equality on the total. `>= 1` still fails if
    // the stamp is missing, which is the property under test.
    let (stale_before, _) = FRAME_DROPS.counts();
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
    let (_registration, mut rx) =
        PendingRegistration::new(Arc::clone(&pending), (7, 2, 11), false, true, None).unwrap();
    let (write_tx, _write_rx) = mpsc::channel(4);
    let (mut client, server) = tokio::io::duplex(1024);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));

    write_frame(&mut client, &frame(FrameType::Response, 7, 1, 11))
        .await
        .unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(50), rx.recv())
        .await
        .is_err());

    write_frame(&mut client, &frame(FrameType::Response, 7, 2, 11))
        .await
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("the current epoch response should settle the waiter")
        .expect("a response frame");
    assert_eq!(received.header.epoch, 2);
    drop(client);
    reader.await.unwrap();

    let (stale_after, _) = FRAME_DROPS.counts();
    assert!(
        stale_after > stale_before,
        "the dropped stale-epoch frame must be counted; without it a client losing every \
             frame to a dead route generation reports exactly like an idle one"
    );
}

/// A terminal whose waiter has already gone is counted, not merely discarded.
///
/// This is the drop that means a reply outlived its request: the caller timed out, or was
/// cancelled, and the answer arrived afterwards. Silently discarding it is correct -- there
/// is nobody to give it to -- but a client where this happens constantly looks identical to
/// one where it never happens, and the difference is exactly what an investigation needs.
#[tokio::test]
async fn an_unmatched_terminal_is_counted() {
    let (_, unmatched_before) = FRAME_DROPS.counts();
    // Route 7 epoch 2 is live, so the frame passes the epoch gate and reaches the waiter
    // lookup -- the path under test. An empty pending map is a waiter that has already gone.
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
    let (write_tx, _write_rx) = mpsc::channel(4);
    let (mut client, server) = tokio::io::duplex(1024);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));

    write_frame(&mut client, &frame(FrameType::Response, 7, 2, 99))
        .await
        .unwrap();
    drop(client);
    reader.await.unwrap();

    let (_, unmatched_after) = FRAME_DROPS.counts();
    assert!(
        unmatched_after > unmatched_before,
        "a terminal with no waiter must be counted; uncounted, a reply that outlived its \
             request is indistinguishable from a peer that never answered"
    );
}

fn reverse_request(channel: u16, epoch: u32, corr: u64, method: &str) -> Frame {
    Frame::build(
        FrameType::Request,
        data_flags(),
        channel,
        epoch,
        corr,
        serde_json::to_vec(&serde_json::json!({ "method": method, "params": {} })).unwrap(),
    )
    .unwrap()
}

async fn next_written(write_rx: &mut mpsc::Receiver<Frame>, what: &str) -> Frame {
    tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
        .await
        .unwrap_or_else(|_| panic!("{what} should be written"))
        .expect("the writer channel should remain open")
}

fn assert_refusal(reply: &Frame, channel: u16, epoch: u32, corr: u64, family: &str) {
    assert_eq!(reply.header.ty, FrameType::Error);
    assert_eq!(
        (reply.header.channel, reply.header.epoch, reply.header.corr),
        (channel, epoch, corr),
        "the refusal must answer on the request's own channel, epoch and corr"
    );
    let body: subc_protocol::ErrorBody = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(body.code, REVERSE_REQUEST_UNHANDLED);
    assert_eq!(
        body.message,
        format!("no reverse-request handler is registered for {family}")
    );
}

/// A module's request on an open route gets the SDK's refusal instead of silence, so the
/// module is not left waiting out its own deadline for an answer broca will never send.
#[tokio::test]
async fn a_reverse_request_on_an_open_route_is_refused_with_reverse_request_unhandled() {
    let refused_before = FRAME_DROPS.reverse_requests_refused();
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
    let (write_tx, mut write_rx) = mpsc::channel(4);
    let (mut client, server) = tokio::io::duplex(1024);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));

    write_frame(
        &mut client,
        &reverse_request(7, 2, 42, "elicitation/create"),
    )
    .await
    .unwrap();
    let reply = next_written(&mut write_rx, "the refusal").await;
    assert_refusal(&reply, 7, 2, 42, "elicitation");

    drop(client);
    reader.await.unwrap();
    // A delta, because the gauge is process-global and other tests run in parallel.
    assert!(
        FRAME_DROPS.reverse_requests_refused() > refused_before,
        "a refused reverse request must be counted so an operator can see modules asking"
    );
}

/// Corrs are allocated independently in each direction, so a module may legally send a
/// request whose corr equals a call broca has in flight on the same route. The frame type
/// is what tells them apart: the request is refused, and the call still completes with its
/// own Response rather than taking the request as its terminal.
#[tokio::test]
async fn a_reverse_request_sharing_a_pending_calls_corr_does_not_complete_that_call() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, mut write_rx) = mpsc::channel(8);
    let conn = test_connection(
        write_tx.clone(),
        Arc::clone(&pending),
        Arc::clone(&routes),
        ConnToken(1),
        1,
    );
    let route = RouteHandle::new(9, 1, ConnToken(1));
    let (mut client, server) = tokio::io::duplex(4096);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));

    let call = tokio::spawn(async move {
        conn.tool_call_with_deadline(
            &route,
            "read",
            &serde_json::json!({}),
            None,
            Duration::from_secs(5),
        )
        .await
    });
    let request = next_written(&mut write_rx, "the tool call").await;
    assert_eq!(request.header.ty, FrameType::Request);
    let corr = request.header.corr;

    write_frame(
        &mut client,
        &reverse_request(9, 1, corr, "sampling/createMessage"),
    )
    .await
    .unwrap();
    let reply = next_written(&mut write_rx, "the refusal").await;
    assert_refusal(&reply, 9, 1, corr, "sampling");
    assert!(
        !call.is_finished(),
        "the module's request must not be taken as the pending call's terminal"
    );

    let result = Frame::build(
        FrameType::Response,
        data_flags(),
        9,
        1,
        corr,
        br#"{"content":[],"isError":false}"#.to_vec(),
    )
    .unwrap();
    write_frame(&mut client, &result).await.unwrap();
    let value = tokio::time::timeout(Duration::from_secs(1), call)
        .await
        .expect("the call should complete on its real Response")
        .unwrap()
        .expect("the call should succeed");
    assert_eq!(value, serde_json::json!({"content": [], "isError": false}));

    drop(client);
    reader.await.unwrap();
}

/// Only a Response or an Error ends a call. Anything else delivered to a waiter is reported
/// as a protocol violation instead of being handed back as the call's result.
#[tokio::test]
async fn a_non_terminal_frame_type_is_a_protocol_violation_not_a_terminal() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, mut write_rx) = mpsc::channel(1);
    let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
    let route = RouteHandle::new(9, 1, ConnToken(1));

    let task = tokio::spawn(async move {
        conn.route_unary_bytes(&route, b"{}".to_vec(), Duration::from_secs(1))
            .await
    });
    let request = next_written(&mut write_rx, "the unary request").await;
    let sender = {
        let waiters = pending.lock().unwrap();
        match waiters.get(&(9, 1, request.header.corr)) {
            Some(PendingEntry::Live { sender, .. }) => sender.clone(),
            _ => panic!("the unary request should register a live waiter"),
        }
    };
    sender
        .send(frame(FrameType::Request, 9, 1, request.header.corr))
        .unwrap();

    let error = task
        .await
        .unwrap()
        .expect_err("a Request is not a terminal");
    assert!(
        matches!(&error, ConnError::Protocol(message)
                if message.contains("Request") && message.contains("Response or Error")),
        "got {error:?}"
    );
}

#[tokio::test]
async fn late_route_open_response_queues_epoch_stamped_goodbye() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::from([(
        (0, 0, 5),
        PendingEntry::LateRouteOpen {
            blocks_probe_until: Instant::now(),
        },
    )])));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (write_tx, mut write_rx) = mpsc::channel(4);
    let (mut client, server) = tokio::io::duplex(1024);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));
    let body = serde_json::to_vec(&ClientControlResponse::RouteOpen {
        route_channel: 9,
        route_epoch: 4,
        request_window: None,
        watch_window: None,
    })
    .unwrap();
    let response = Frame::build(FrameType::Response, control_flags(), 0, 0, 5, body).unwrap();
    write_frame(&mut client, &response).await.unwrap();

    let goodbye = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
        .await
        .expect("late route.open must queue cleanup")
        .expect("a GOODBYE frame");
    assert_eq!(goodbye.header.ty, FrameType::Goodbye);
    assert_eq!((goodbye.header.channel, goodbye.header.epoch), (9, 4));
    assert!(pending.lock().unwrap().is_empty());
    assert!(routes.lock().unwrap().is_empty());
    drop(client);
    reader.await.unwrap();
}

async fn congested_egress_does_not_stop_ingress(cleanup: u8) {
    let pending: PendingMap = Arc::default();
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
    let (_live, mut reply_rx) =
        PendingRegistration::new(Arc::clone(&pending), (0, 0, 2), false, true, None).unwrap();
    let (mut abandoned, open_rx) =
        PendingRegistration::new(Arc::clone(&pending), (0, 0, 1), true, true, None).unwrap();
    drop(open_rx);
    if cleanup == 1 {
        abandoned.arm_late_route_open_cleanup();
        drop(abandoned);
    }
    let trigger = if cleanup == 0 {
        reverse_request(7, 2, 1, "unhandled")
    } else {
        Frame::build(
            FrameType::Response,
            control_flags(),
            0,
            0,
            1,
            serde_json::to_vec(&ClientControlResponse::RouteOpen {
                route_channel: 9,
                route_epoch: 4,
                request_window: None,
                watch_window: None,
            })
            .unwrap(),
        )
        .unwrap()
    };
    let (write_tx, mut write_rx) = mpsc::channel(1);
    write_tx.try_send(frame(FrameType::Ping, 0, 0, 99)).unwrap();
    let (mut client, server) = tokio::io::duplex(4096);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));
    write_frame(&mut client, &trigger).await.unwrap();
    write_frame(&mut client, &frame(FrameType::Response, 0, 0, 2))
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(1), reply_rx.recv())
        .await
        .expect("ingress must process a reply while egress remains full")
        .unwrap();
    assert_eq!(reply.header.corr, 2);
    assert_eq!(
        write_rx.try_recv().unwrap().header.corr,
        99,
        "the full queue was not drained to permit ingress"
    );
    assert!(
        !routes.lock().unwrap().contains_key(&9),
        "abandoned opens must still be invalidated when cleanup cannot enqueue"
    );
    drop(client);
    reader.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn full_egress_reverse_refusal_does_not_block_replies() {
    congested_egress_does_not_stop_ingress(0).await;
}

#[tokio::test(start_paused = true)]
async fn full_egress_late_open_cleanup_does_not_block_replies() {
    congested_egress_does_not_stop_ingress(1).await;
}

#[tokio::test(start_paused = true)]
async fn full_egress_abandoned_open_cleanup_does_not_block_replies() {
    congested_egress_does_not_stop_ingress(2).await;
}

#[tokio::test]
async fn foreign_connection_handle_fails_without_emitting_a_frame() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, mut write_rx) = mpsc::channel(1);
    let conn = test_connection(write_tx, pending, routes, ConnToken(2), 1);
    let foreign = RouteHandle::new(9, 1, ConnToken(1));

    let error = conn
        .route_request(&foreign, br#"{}"#.to_vec())
        .await
        .unwrap_err();
    assert!(matches!(error, ConnError::StaleRouteHandle(handle) if handle == foreign));
    assert!(write_rx.try_recv().is_err());
}

#[tokio::test]
async fn pending_waiter_limit_returns_a_typed_transport_error() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    for corr in 0..MAX_PENDING_REQUESTS as u64 {
        let (sender, _rx) = mpsc::unbounded_channel();
        pending.lock().unwrap().insert(
            (9, 1, corr),
            PendingEntry::Live {
                sender: PendingSender::Unary(sender),
                route_open: false,
                blocks_probe: true,
            },
        );
    }
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, _write_rx) = mpsc::channel(1);
    let conn = test_connection(
        write_tx,
        Arc::clone(&pending),
        routes,
        ConnToken(1),
        MAX_PENDING_REQUESTS as u64,
    );
    let route = RouteHandle::new(9, 1, ConnToken(1));

    let error = conn
        .route_request(&route, br#"{}"#.to_vec())
        .await
        .expect_err("the 4097th in-flight request must be rejected");
    assert!(matches!(error, ConnError::Transport(message) if message.contains("4096")));
    assert_eq!(pending.lock().unwrap().len(), MAX_PENDING_REQUESTS);
}

#[tokio::test]
async fn cancelled_request_deregisters_its_pending_waiter() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, _write_rx) = mpsc::channel(1);
    write_tx
        .send(frame(FrameType::Request, 99, 1, 1))
        .await
        .unwrap();
    let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
    let route = RouteHandle::new(9, 1, ConnToken(1));

    let task = tokio::spawn(async move {
        let _ = conn.route_request(&route, br#"{}"#.to_vec()).await;
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !pending.lock().unwrap().contains_key(&(9, 1, 1)) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request should register its waiter before cancellation");

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !pending.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelling the request should drop its pending waiter");
}

#[tokio::test]
async fn timed_out_control_blackhole_replaces_the_tcp_connection() {
    let stub = ControlTcpStub::start(ControlStubReply::Blackhole).await;
    let conn =
        connect_control_stub(&stub, Duration::from_millis(20), Duration::from_millis(70)).await;
    assert_eq!(stub.accept_count(), 1, "the test must begin on one socket");

    let _error = conn
        .control_rpc_deadline(Vec::new(), Duration::from_millis(40))
        .await
        .expect_err("the control-only stub intentionally withholds every response");
    wait_for_accept_count(&stub, 2).await;

    assert_eq!(
        stub.accept_count(),
        2,
        "a channel-zero timeout must replace the convicted TCP generation"
    );
    assert_eq!(
        stub.request_count(),
        1,
        "the first control RPC must be sent"
    );
    assert!(
        stub.ping_count() >= 1,
        "the deferred probe must send a Ping"
    );
    stub.assert_healthy();
}

#[tokio::test]
async fn retained_route_open_tombstone_stops_suppressing_after_grace() {
    let stub = ControlTcpStub::start(ControlStubReply::Blackhole).await;
    let conn =
        connect_control_stub(&stub, Duration::from_millis(20), Duration::from_millis(70)).await;
    assert_eq!(stub.accept_count(), 1, "the test must begin on one socket");

    let _error = conn
        .route_open_rpc_deadline(Vec::new(), Duration::from_millis(40))
        .await
        .expect_err("the dead daemon leaves a retained route.open tombstone");
    wait_for_accept_count(&stub, 2).await;

    assert_eq!(
        stub.accept_count(),
        2,
        "an expired cleanup tombstone must not preserve a dead TCP generation"
    );
    assert_eq!(stub.request_count(), 1, "the route.open must be sent once");
    assert!(
        stub.ping_count() >= 1,
        "grace expiry must release the probe"
    );
    stub.assert_healthy();
}

#[tokio::test]
async fn slow_live_route_open_is_not_convicted_during_bind_relay_grace() {
    // These compressed deadlines preserve the ordering of a caller that gives up on a
    // route.open before the daemon's relay window closes: the caller's wait ends, the daemon
    // is still relaying that same bind, and its healthy response arrives after a no-grace
    // probe window would already have convicted the socket.
    let stub =
        ControlTcpStub::start(ControlStubReply::SlowRouteOpen(Duration::from_millis(140))).await;
    let conn =
        connect_control_stub(&stub, Duration::from_millis(20), Duration::from_millis(180)).await;
    assert_eq!(stub.accept_count(), 1, "the test must begin on one socket");

    let _error = conn
        .route_open_rpc_deadline(Vec::new(), Duration::from_millis(80))
        .await
        .expect_err("the bind relay deliberately outlives the caller deadline");
    time::sleep(Duration::from_millis(250)).await;

    assert_eq!(
        stub.accept_count(),
        1,
        "late healthy route.open work must not convict the shared TCP generation"
    );
    assert_eq!(
        stub.request_count(),
        1,
        "the route.open must reach the daemon"
    );
    assert!(
        stub.ping_count() >= 1,
        "the deferred health decision must eventually probe the live socket"
    );
    stub.assert_healthy();
}

#[tokio::test]
async fn channel_zero_pending_request_defers_probe_until_registration_drops() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (write_tx, mut write_rx) = mpsc::channel(4);
    let conn = test_connection(write_tx, pending, routes, ConnToken(1), 1);
    let runtime = {
        let state = conn.shared.current.lock().unwrap();
        Arc::clone(&state.runtime)
    };
    let (_runtime, registration, response_rx) = conn
        .register_control((0, 0, 41), false)
        .expect("install an in-flight channel-zero request");

    conn.spawn_liveness_probe(&runtime);
    assert!(
        tokio::time::timeout(Duration::from_millis(60), write_rx.recv())
            .await
            .is_err(),
        "a probe must not enter the writer queue while channel zero is pending"
    );
    {
        let state = conn.shared.current.lock().unwrap();
        assert!(!state.probe_in_flight);
        assert!(state.probe_deferred, "the blocked probe must be remembered");
    }

    drop(response_rx);
    drop(registration);
    let ping = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
        .await
        .expect("draining the last live blocker must release the probe")
        .expect("the deferred probe must queue a Ping");
    assert_eq!(ping.header.ty, FrameType::Ping);
}

#[tokio::test]
async fn liveness_probe_is_single_flight_within_a_generation() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (write_tx, mut write_rx) = mpsc::channel(4);
    let conn = test_connection(write_tx, pending, routes, ConnToken(1), 1);
    let runtime = {
        let state = conn.shared.current.lock().unwrap();
        Arc::clone(&state.runtime)
    };

    conn.spawn_liveness_probe(&runtime);
    conn.spawn_liveness_probe(&runtime);
    let ping = write_rx
        .recv()
        .await
        .expect("the first probe must queue a Ping");
    assert_eq!(ping.header.ty, FrameType::Ping);
    assert!(
        tokio::time::timeout(Duration::from_millis(60), write_rx.recv())
            .await
            .is_err(),
        "a second timeout must share the generation's in-flight probe"
    );
}

#[tokio::test(start_paused = true)]
async fn probe_timeout_starts_only_after_confirmed_write() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (write_tx, mut write_rx) = mpsc::channel(4);
    let conn = test_connection(write_tx, pending, routes, ConnToken(1), 1);
    let runtime = {
        let state = conn.shared.current.lock().unwrap();
        Arc::clone(&state.runtime)
    };

    conn.shared.current.lock().unwrap().probe_in_flight = true;
    let probe = tokio::spawn(run_liveness_probe(
        Arc::downgrade(&conn.shared),
        Arc::clone(&runtime),
        1,
    ));
    let ping = write_rx.recv().await.expect("probe must queue its Ping");
    assert_eq!(ping.header.ty, FrameType::Ping);
    tokio::time::advance(Duration::from_millis(60)).await;
    assert!(
        conn.shared.current.lock().unwrap().active,
        "queue insertion alone must not start conviction"
    );

    runtime
        .write_confirmations
        .lock()
        .unwrap()
        .remove(&ping.header.corr)
        .expect("the Ping must await a writer receipt")
        .send(Ok(Instant::now()))
        .unwrap();
    wait_for_probe_window(&conn).await;
    tokio::time::advance(conn.shared.probe_timeout).await;
    probe.await.unwrap();
    assert!(
        !conn.shared.current.lock().unwrap().active,
        "silence after a confirmed Ping write must convict the generation"
    );
}

struct BlockedWriter(Arc<tokio::sync::Notify>);

impl AsyncWrite for BlockedWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.0.notify_one();
        std::task::Poll::Pending
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn blocked_ping_write_expires_and_reconnects() {
    let (write_tx, write_rx) = mpsc::channel(1);
    let mut conn = test_connection(write_tx, Arc::default(), Arc::default(), ConnToken(1), 1);
    let (reconnect_tx, mut reconnect_rx) = mpsc::unbounded_channel();
    Arc::get_mut(&mut conn.shared).unwrap().reconnect_tx = reconnect_tx;
    let runtime = Arc::clone(&conn.shared.current.lock().unwrap().runtime);
    let blocked = Arc::new(tokio::sync::Notify::new());
    let writer = tokio::spawn(writer_task(
        BlockedWriter(Arc::clone(&blocked)),
        write_rx,
        Arc::downgrade(&conn.shared),
        runtime.conn,
        runtime.shutdown.clone(),
        Arc::clone(&runtime.write_confirmations),
    ));
    let probe = tokio::spawn(run_liveness_probe(
        Arc::downgrade(&conn.shared),
        Arc::clone(&runtime),
        1,
    ));
    blocked.notified().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::timeout(Duration::from_secs(1), probe)
        .await
        .expect("egress deadline must finish the probe")
        .unwrap();
    assert!(!conn.shared.current.lock().unwrap().active);
    assert_eq!(
        reconnect_rx
            .try_recv()
            .expect("conviction must request reconnect"),
        runtime.conn
    );
    assert!(runtime.shutdown.is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .expect("generation shutdown must interrupt the blocked write")
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn blocked_ping_enqueue_expires() {
    use futures::FutureExt;
    let (write_tx, _write_rx) = mpsc::channel(1);
    write_tx.try_send(frame(FrameType::Push, 0, 0, 0)).unwrap();
    let conn = test_connection(write_tx, Arc::default(), Arc::default(), ConnToken(1), 1);
    let runtime = Arc::clone(&conn.shared.current.lock().unwrap().runtime);
    let probe = run_liveness_probe(Arc::downgrade(&conn.shared), Arc::clone(&runtime), 1);
    tokio::pin!(probe);
    assert!(probe.as_mut().now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(
        probe.now_or_never().is_some(),
        "queue send must share the egress budget"
    );
    assert!(!conn.shared.current.lock().unwrap().active);
    assert!(runtime.write_confirmations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn channel_zero_activity_during_probe_window_prevents_conviction() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (write_tx, mut write_rx) = mpsc::channel(4);
    let conn = test_connection(write_tx, pending, routes, ConnToken(1), 1);
    let runtime = {
        let state = conn.shared.current.lock().unwrap();
        Arc::clone(&state.runtime)
    };

    conn.spawn_liveness_probe(&runtime);
    let ping = write_rx.recv().await.expect("probe must queue its Ping");
    runtime
        .write_confirmations
        .lock()
        .unwrap()
        .remove(&ping.header.corr)
        .unwrap()
        .send(Ok(Instant::now()))
        .unwrap();
    wait_for_probe_window(&conn).await;
    let (_runtime, registration, response_rx) = conn
        .register_control((0, 0, 42), false)
        .expect("channel-zero work may appear during the probe window");
    drop(response_rx);
    drop(registration);
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert!(
        conn.shared.current.lock().unwrap().active,
        "even completed channel-zero activity in the window must acquit the socket"
    );
}

#[tokio::test]
async fn stale_probe_cannot_convict_replacement_generation() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (write_tx, mut write_rx) = mpsc::channel(4);
    let conn = test_connection(write_tx, pending, routes, ConnToken(1), 1);
    let old_runtime = {
        let state = conn.shared.current.lock().unwrap();
        Arc::clone(&state.runtime)
    };

    conn.spawn_liveness_probe(&old_runtime);
    let ping = write_rx.recv().await.expect("probe must queue its Ping");
    old_runtime
        .write_confirmations
        .lock()
        .unwrap()
        .remove(&ping.header.corr)
        .unwrap()
        .send(Ok(Instant::now()))
        .unwrap();
    wait_for_probe_window(&conn).await;

    let (new_write_tx, mut new_write_rx) = mpsc::channel(4);
    let replacement = Arc::new(ConnectionRuntime {
        close_observer: Mutex::new(Weak::new()),
        closing_routes: Mutex::new(HashMap::new()),
        #[cfg(feature = "test-route-close")]
        fail_next_goodbye: std::sync::atomic::AtomicBool::new(false),
        write_tx: new_write_tx,
        pending: Arc::new(Mutex::new(HashMap::new())),
        route_epochs: Arc::new(Mutex::new(HashMap::from([(17, 3)]))),
        write_confirmations: Arc::new(Mutex::new(HashMap::new())),
        conn: ConnToken(2),
        shutdown: CancellationToken::new(),
        capabilities: tokio::sync::OnceCell::new(),
    });
    let mut replacement_state = GenerationState::new(Arc::clone(&replacement));
    replacement_state.last_inbound = Instant::now() - Duration::from_secs(1);
    replacement_state.probe_in_flight = true;
    *conn.shared.current.lock().unwrap() = replacement_state;
    tokio::time::sleep(Duration::from_millis(60)).await;

    let state = conn.shared.current.lock().unwrap();
    assert_eq!(state.runtime.conn, ConnToken(2));
    assert!(state.active);
    assert_eq!(
        state.runtime.route_epochs.lock().unwrap().get(&17),
        Some(&3)
    );
    drop(state);
    assert!(new_write_rx.try_recv().is_err());
}

#[tokio::test]
async fn in_flight_tool_call_is_not_resent_across_generation_swap() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (old_write_tx, mut old_write_rx) = mpsc::channel(4);
    let conn = Arc::new(test_connection(
        old_write_tx,
        pending,
        routes,
        ConnToken(1),
        1,
    ));
    let route = RouteHandle::new(9, 1, ConnToken(1));
    let call = {
        let conn = Arc::clone(&conn);
        tokio::spawn(async move {
            conn.tool_call(&route, "mutate", &serde_json::json!({"value": 1}), None)
                .await
        })
    };
    let first = old_write_rx
        .recv()
        .await
        .expect("the tool dispatch must enter generation one");
    assert_eq!(first.header.ty, FrameType::Request);

    let (new_write_tx, mut new_write_rx) = mpsc::channel(4);
    let replacement = Arc::new(ConnectionRuntime {
        close_observer: Mutex::new(Weak::new()),
        closing_routes: Mutex::new(HashMap::new()),
        #[cfg(feature = "test-route-close")]
        fail_next_goodbye: std::sync::atomic::AtomicBool::new(false),
        write_tx: new_write_tx,
        pending: Arc::new(Mutex::new(HashMap::new())),
        route_epochs: Arc::new(Mutex::new(HashMap::new())),
        write_confirmations: Arc::new(Mutex::new(HashMap::new())),
        conn: ConnToken(2),
        shutdown: CancellationToken::new(),
        capabilities: tokio::sync::OnceCell::new(),
    });
    {
        let mut state = conn.shared.current.lock().unwrap();
        assert!(deactivate_locked(&mut state));
        *state = GenerationState::new(replacement);
    }

    let error = call
        .await
        .unwrap()
        .expect_err("the old generation's pending result is ambiguous");
    assert!(
        matches!(error, ConnError::Transport(message) if message == "connection closed mid-call")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(60), new_write_rx.recv())
            .await
            .is_err(),
        "an in-flight tool request must never be replayed on the replacement socket"
    );
}

/// VIRTUAL TIME, because this counts attempts inside a fixed window and that window used
/// to be wall-clock. On a shared box the supervisor task competes with every other seat's
/// build for CPU, so a starved task completes FEWER backoff attempts and the assertion
/// fails for a reason that has nothing to do with backoff: measured 5 failures in 6 runs
/// with six concurrent cargo processes, every one reporting `got 1`.
///
/// A timing test that is correct only when the machine is idle trains its readers to
/// re-run rather than to read it, which is how a real regression in the reconnect path
/// would arrive unnoticed. Paused time makes the count a function of the BACKOFF SCHEDULE
/// alone: tokio auto-advances the clock once every task is idle, so the window below
/// advances over the supervisor's own 100ms/200ms/400ms and over nothing else.
///
/// WHAT A REAL SPIN LOOKS LIKE HERE, measured by removing the backoff: the test HANGS
/// rather than reporting a large count. Paused time only advances when every task is
/// idle, and a zero-backoff supervisor is never idle, so the clock stops. That is still
/// a failure and still catches the defect -- but it arrives as a CI timeout rather than
/// as `got 847`, so a reader who sees this test hang should suspect the backoff itself
/// before suspecting the harness.
#[tokio::test(start_paused = true)]
async fn unreachable_reconnect_attempts_back_off_instead_of_spinning() {
    let (write_tx, _write_rx) = mpsc::channel(1);
    let runtime = Arc::new(ConnectionRuntime {
        close_observer: Mutex::new(Weak::new()),
        closing_routes: Mutex::new(HashMap::new()),
        #[cfg(feature = "test-route-close")]
        fail_next_goodbye: std::sync::atomic::AtomicBool::new(false),
        write_tx,
        pending: Arc::new(Mutex::new(HashMap::new())),
        route_epochs: Arc::new(Mutex::new(HashMap::new())),
        write_confirmations: Arc::new(Mutex::new(HashMap::new())),
        conn: ConnToken(77),
        shutdown: CancellationToken::new(),
        capabilities: tokio::sync::OnceCell::new(),
    });
    let (reconnect_tx, reconnect_rx) = mpsc::unbounded_channel();
    let shared = Arc::new(ConnectionShared {
        aft_harness_answer: Mutex::new(None),
        person_yields: Mutex::default(),
        catalog: Default::default(),
        #[cfg(test)]
        late_goodbye_handoff: Mutex::new(None),
        route_closer: std::sync::OnceLock::new(),
        connection_file_path: broca_test_support::scratch_root().join(format!(
            "broca-missing-connection-file-{}",
            std::process::id()
        )),
        current: Mutex::new(GenerationState::new(runtime)),
        corr: AtomicU64::new(1),
        reconnect_tx,
        probe_timeout: Duration::from_millis(25),
        route_open_bind_relay_grace: Duration::from_millis(50),
        consumer_identity: None,
        reconnect_attempts: AtomicU64::new(0),
        probe_windows_started: AtomicU64::new(0),
    });
    {
        let mut state = shared.current.lock().unwrap();
        assert!(deactivate_locked(&mut state));
    }
    let supervisor = reconnect_supervisor(Arc::downgrade(&shared), reconnect_rx);
    tokio::pin!(supervisor);
    shared.reconnect_tx.send(ConnToken(77)).unwrap();
    assert!(futures::poll!(&mut supervisor).is_pending());
    assert_eq!(shared.reconnect_attempts.load(Ordering::Relaxed), 1);
    tokio::time::advance(Duration::from_millis(99)).await;
    assert!(futures::poll!(&mut supervisor).is_pending());
    assert_eq!(shared.reconnect_attempts.load(Ordering::Relaxed), 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(futures::poll!(&mut supervisor).is_pending());
    assert_eq!(shared.reconnect_attempts.load(Ordering::Relaxed), 2);
    // After the first 100ms backoff, count subsequent attempts over another 450ms.
    let observation = async {
        tokio::time::sleep(Duration::from_millis(450)).await;
    };
    tokio::select! {
        _ = &mut supervisor => panic!("reconnect supervisor exited"),
        _ = observation => {}
    };

    let attempts = shared.reconnect_attempts.load(Ordering::Relaxed);
    assert!(
        (2..=4).contains(&attempts),
        "exponential backoff should permit only a few attempts, got {attempts}"
    );
}

#[test]
fn reconnect_backoff_doubles_to_a_cap() {
    assert_eq!(reconnect_backoff(1), Duration::from_millis(100));
    assert_eq!(reconnect_backoff(2), Duration::from_millis(200));
    assert_eq!(reconnect_backoff(3), Duration::from_millis(400));
    assert_eq!(reconnect_backoff(64), RECONNECT_MAX_BACKOFF);
}

/// Under a scope hold, the two scope refusals are held to the hold's deadline even when the
/// ordinary budget is shorter, while a module refusal keeps the ordinary budget.
#[tokio::test(start_paused = true)]
async fn the_scope_hold_governs_only_the_scope_refusals() {
    let hold = ScopeHold {
        deadline: time::Instant::now() + Duration::from_secs(10),
        max_interval: Duration::from_secs(2),
    };
    for (code, expected_at_least) in [
        ("scope_not_synced", Duration::from_secs(10)),
        ("module_warming", Duration::ZERO),
    ] {
        let attempts = AtomicUsize::new(0);
        let started = time::Instant::now();
        let result = retry_route_open_with_scope_hold(
            RouteOpenRetry::within(Duration::from_millis(300), Duration::from_millis(100)),
            Some(ScopeHold {
                deadline: time::Instant::now() + Duration::from_secs(10),
                ..hold
            }),
            None,
            no_gate,
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    Err(ConnError::Rejected(RejectedMessage::structured(
                        code.into(),
                        "not yet".into(),
                    )))
                }
            },
        )
        .await;
        let elapsed = started.elapsed();
        assert!(matches!(
            result,
            Err(RouteOpenError::Conn(ConnError::Rejected(_)))
        ));
        assert!(elapsed >= expected_at_least, "{code}: {elapsed:?}");
        if code == "module_warming" {
            assert!(elapsed < Duration::from_secs(1), "{code}: {elapsed:?}");
        } else {
            assert!(elapsed < Duration::from_secs(11), "{code}: {elapsed:?}");
        }
        assert!(attempts.load(Ordering::SeqCst) > 1, "{code} was retried");
    }
}

/// Every Cancel frame the connection wrote within `quiet` of the last frame it wrote.
/// Other frames (a liveness Ping after a timeout, the Request itself) are skipped.
async fn written_cancels(write_rx: &mut mpsc::Receiver<Frame>, quiet: Duration) -> Vec<Frame> {
    let mut cancels = Vec::new();
    while let Ok(Some(frame)) = tokio::time::timeout(quiet, write_rx.recv()).await {
        if frame.header.ty == FrameType::Cancel {
            cancels.push(frame);
        }
    }
    cancels
}

fn assert_cancel_for(cancel: &Frame, channel: u16, epoch: u32, corr: u64) {
    assert_eq!(
        (
            cancel.header.channel,
            cancel.header.epoch,
            cancel.header.corr
        ),
        (channel, epoch, corr),
        "the Cancel must name the abandoned call's route generation and corr"
    );
    assert!(
        cancel.body.is_empty(),
        "a data-plane Cancel carries no body"
    );
}

/// A tool call whose reply deadline expires tells the provider to stop. Without the
/// Cancel the provider keeps running the call with nobody waiting for it, which once held
/// a project lock for minutes after broca had given up.
///
/// The provider's late Response is then dropped by the real reader as an unmatched
/// terminal, and the recorded error is unchanged: still the timeout.
#[tokio::test]
async fn a_tool_call_past_its_deadline_sends_one_cancel_for_its_corr() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, mut write_rx) = mpsc::channel(16);
    let conn = test_connection(
        write_tx.clone(),
        Arc::clone(&pending),
        Arc::clone(&routes),
        ConnToken(1),
        1,
    );
    let route = RouteHandle::new(9, 1, ConnToken(1));
    let (mut client, server) = tokio::io::duplex(4096);
    let reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));

    let call = tokio::spawn(async move {
        conn.tool_call_with_deadline(
            &route,
            "grep",
            &serde_json::json!({}),
            None,
            Duration::from_millis(50),
        )
        .await
    });
    let request = next_written(&mut write_rx, "the tool call").await;
    assert_eq!(request.header.ty, FrameType::Request);
    let corr = request.header.corr;

    let error = tokio::time::timeout(Duration::from_secs(1), call)
        .await
        .expect("the call gives up at its deadline")
        .unwrap()
        .expect_err("no terminal arrived");
    assert!(
        matches!(&error, ConnError::Transport(message) if message.starts_with("timed out after")),
        "the caller still sees the timeout, not anything the Cancel implies: {error:?}"
    );

    let cancels = written_cancels(&mut write_rx, Duration::from_millis(100)).await;
    assert_eq!(
        cancels.len(),
        1,
        "exactly one Cancel follows the expired deadline"
    );
    assert_cancel_for(&cancels[0], 9, 1, corr);

    // The timeout also starts a liveness probe, and with no writer task here to confirm its
    // Ping the probe retires this test generation, which forgets the route. Put the route
    // back so the late reply is judged by the waiter lookup rather than the epoch gate.
    routes.lock().unwrap().insert(9, 1);
    let (_, unmatched_before) = FRAME_DROPS.counts();
    let late = Frame::build(
        FrameType::Response,
        data_flags(),
        9,
        1,
        corr,
        br#"{"content":[],"isError":false}"#.to_vec(),
    )
    .unwrap();
    write_frame(&mut client, &late).await.unwrap();
    drop(client);
    reader.await.unwrap();
    let (_, unmatched_after) = FRAME_DROPS.counts();
    assert!(
        unmatched_after > unmatched_before,
        "a Response that crossed the Cancel is dropped and counted as an unmatched terminal"
    );
}

/// Dropping a sent call (its run task aborted, its caller cancelled) also tells the
/// provider to stop, and does so exactly once.
#[tokio::test]
async fn dropping_a_sent_tool_call_sends_exactly_one_cancel() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, mut write_rx) = mpsc::channel(16);
    let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
    let route = RouteHandle::new(9, 1, ConnToken(1));

    let call = tokio::spawn(async move {
        conn.tool_call_with_deadline(
            &route,
            "grep",
            &serde_json::json!({}),
            None,
            Duration::from_secs(60),
        )
        .await
    });
    let request = next_written(&mut write_rx, "the tool call").await;
    assert_eq!(request.header.ty, FrameType::Request);
    let corr = request.header.corr;

    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());

    let cancels = written_cancels(&mut write_rx, Duration::from_millis(100)).await;
    assert_eq!(cancels.len(), 1, "exactly one Cancel follows the drop");
    assert_cancel_for(&cancels[0], 9, 1, corr);
    assert!(
        pending.lock().unwrap().is_empty(),
        "the dropped call's waiter is gone, so a late reply finds nobody"
    );
}

/// A call the provider answered has nothing left to cancel, whether the answer was a
/// Response or an Error.
#[tokio::test]
async fn a_tool_call_answered_before_its_deadline_sends_no_cancel() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    let (write_tx, mut write_rx) = mpsc::channel(16);
    let conn = Arc::new(test_connection(
        write_tx.clone(),
        Arc::clone(&pending),
        Arc::clone(&routes),
        ConnToken(1),
        1,
    ));
    let route = RouteHandle::new(9, 1, ConnToken(1));
    let (mut client, server) = tokio::io::duplex(4096);
    let _reader = tokio::spawn(reader_task(
        server,
        Arc::clone(&pending),
        Arc::clone(&routes),
        write_tx,
        None,
    ));

    for (terminal_ty, body) in [
        (
            FrameType::Response,
            br#"{"content":[],"isError":false}"#.to_vec(),
        ),
        (
            FrameType::Error,
            br#"{"code":"tool_failed","message":"no"}"#.to_vec(),
        ),
    ] {
        let call = {
            let conn = Arc::clone(&conn);
            tokio::spawn(async move {
                conn.tool_call_with_deadline(
                    &route,
                    "grep",
                    &serde_json::json!({}),
                    None,
                    Duration::from_secs(5),
                )
                .await
            })
        };
        let request = next_written(&mut write_rx, "the tool call").await;
        assert_eq!(request.header.ty, FrameType::Request);
        let terminal =
            Frame::build(terminal_ty, data_flags(), 9, 1, request.header.corr, body).unwrap();
        write_frame(&mut client, &terminal).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), call)
            .await
            .expect("the call completes on its terminal")
            .unwrap();
        assert_eq!(result.is_ok(), terminal_ty == FrameType::Response);

        let cancels = written_cancels(&mut write_rx, Duration::from_millis(100)).await;
        assert!(
            cancels.is_empty(),
            "a call answered with {terminal_ty:?} must not be cancelled: {cancels:?}"
        );
    }
}

/// A call dropped while its Request is still waiting for room in the writer queue was never
/// sent, so the provider has no work to stop and must not be sent a Cancel for a corr it
/// has never seen.
#[tokio::test]
async fn a_tool_call_dropped_before_its_request_is_sent_sends_no_cancel() {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
    // A one-slot queue already holding a frame: the call's send blocks until it is dropped.
    let (write_tx, mut write_rx) = mpsc::channel(1);
    write_tx
        .send(frame(FrameType::Push, 9, 1, 0))
        .await
        .unwrap();
    let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
    let route = RouteHandle::new(9, 1, ConnToken(1));

    let call = tokio::spawn(async move {
        conn.tool_call_with_deadline(
            &route,
            "grep",
            &serde_json::json!({}),
            None,
            Duration::from_secs(60),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while pending.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the call registers its waiter before blocking on the full queue");
    // Give the call a moment to reach the blocked send.
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Free the queue's slot, then abort before the call's task runs again. The slot is
    // free when the call is dropped, so a Cancel wrongly enqueued for this unsent call would
    // land in it and be seen below; with the slot still full it would fail silently and
    // this test could not tell the difference. (The test runtime is single-threaded, so
    // the call cannot be polled between this receive and the abort.)
    let filler = write_rx
        .try_recv()
        .expect("the frame that filled the queue");
    assert_eq!(filler.header.ty, FrameType::Push);
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    let mut later = Vec::new();
    while let Ok(Some(frame)) =
        tokio::time::timeout(Duration::from_millis(100), write_rx.recv()).await
    {
        later.push(frame.header.ty);
    }
    assert!(
        later.is_empty(),
        "neither the Request nor a Cancel may be written for a call that was never sent: \
             {later:?}"
    );
}
