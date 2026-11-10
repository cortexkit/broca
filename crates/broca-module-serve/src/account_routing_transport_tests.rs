use super::*;
use subc_control::{ClientControlRequest, ClientControlResponse};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{read_frame, write_frame};

struct RoutingDaemon {
    transport: Arc<SubcRoutingTransport>,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    opens: Arc<AtomicUsize>,
    opened_modules: Arc<Mutex<Vec<String>>>,
    hang_reopens: Arc<std::sync::atomic::AtomicBool>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for RoutingDaemon {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl RoutingDaemon {
    async fn start(reply: Result<Vec<u8>, Vec<u8>>, close_after_request: bool) -> Self {
        Self::start_with_opens(reply, close_after_request, Vec::new(), None).await
    }

    async fn start_with_opens(
        reply: Result<Vec<u8>, Vec<u8>>,
        close_after_request: bool,
        open_replies: Vec<(&'static str, Option<&'static str>)>,
        configured: Option<String>,
    ) -> Self {
        use subc_transport::connection_file::{ConnectionInfo, Endpoint, SCHEMA_VERSION};
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "account-routing-transport-{}",
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let connection_file = root.join("connection.json");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let key = vec![0x3b; 32];
        let daemon_id = [0x4d; 16];
        subc_transport::connection_file::write_atomic(
            &connection_file,
            &ConnectionInfo {
                schema: SCHEMA_VERSION,
                wire_version: None,
                endpoints: vec![Endpoint {
                    host: "127.0.0.1".into(),
                    port: listener.local_addr().unwrap().port(),
                }],
                key: key.clone(),
                daemon_id,
                pid: std::process::id(),
                daemon_ver: "routing-stand-in".into(),
            },
        )
        .unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let opens = Arc::new(AtomicUsize::new(0));
        let opened_modules = Arc::new(Mutex::new(Vec::new()));
        let hang_reopens = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server = {
            let requests = Arc::clone(&requests);
            let opens = Arc::clone(&opens);
            let opened_modules = Arc::clone(&opened_modules);
            let hang_reopens = Arc::clone(&hang_reopens);
            tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                subc_transport::authenticate_server(
                    &mut stream,
                    &key,
                    &daemon_id,
                    "routing-stand-in",
                    broca_test_support::STAND_IN_AUTH_DEADLINE,
                )
                .await
                .unwrap();
                while let Some(frame) = read_frame(&mut stream).await.unwrap() {
                    let (ty, body) = match (frame.header.channel, frame.header.ty) {
                        (0, FrameType::Ping) => (FrameType::Pong, Vec::new()),
                        (0, FrameType::Request) => {
                            let request: ClientControlRequest =
                                serde_json::from_slice(&frame.body).unwrap();
                            let ClientControlRequest::RouteOpen { target, .. } = request else {
                                panic!("expected route.open");
                            };
                            let subc_protocol::RouteTarget::ManagementSurface { module_id } =
                                target
                            else {
                                panic!("expected management route");
                            };
                            opened_modules.lock().unwrap().push(module_id.clone());
                            let index = opens.fetch_add(1, Ordering::SeqCst);
                            let channel = 10 + index as u16;
                            if channel > 10 && hang_reopens.load(Ordering::SeqCst) {
                                std::future::pending::<()>().await;
                            }
                            let refusal = if open_replies.is_empty() {
                                None
                            } else {
                                let (expected, refusal) = open_replies
                                    .get(index)
                                    .expect("unexpected extra route open");
                                assert_eq!(&module_id, expected);
                                *refusal
                            };
                            match refusal {
                                Some(code) => (FrameType::Error, serde_json::to_vec(&json!({
                                    "code": code,
                                    "message": "unknown_module target_unavailable module_warming"
                                })).unwrap()),
                                None => (FrameType::Response, serde_json::to_vec(&ClientControlResponse::RouteOpen {
                                    route_channel: channel,
                                    route_epoch: 1,
                                }).unwrap()),
                            }
                        }
                        (_, FrameType::Goodbye) => continue,
                        (channel, FrameType::Request) if channel != 0 => {
                            requests.lock().unwrap().push(frame.body.clone());
                            if close_after_request {
                                (FrameType::Goodbye, Vec::new())
                            } else {
                                match &reply {
                                    Ok(body) => (FrameType::Response, body.clone()),
                                    Err(body) => (FrameType::Error, body.clone()),
                                }
                            }
                        }
                        other => panic!("unexpected routing stand-in frame: {other:?}"),
                    };
                    let response = Frame::build(
                        ty,
                        Flags::new(false, Priority::Interactive, false),
                        frame.header.channel,
                        frame.header.epoch,
                        frame.header.corr,
                        body,
                    )
                    .unwrap();
                    write_frame(&mut stream, &response).await.unwrap();
                }
            })
        };
        let conn = Arc::new(
            broca_subc::SubcConnection::connect_with_consumer_identity(&connection_file, None)
                .await
                .unwrap(),
        );
        let transport = Arc::new(SubcRoutingTransport::new(conn, configured, session()));
        Self {
            transport,
            requests,
            opens,
            opened_modules,
            hang_reopens,
            server,
        }
    }

    async fn stale_cached_route(&self) {
        let route = self.transport.route().await.unwrap();
        self.transport.conn.route_close(&route).await.unwrap();
        assert!(!self.transport.conn.route_is_open(&route));
    }

    fn client(&self) -> RoutingClient {
        RoutingClient::new(self.transport.clone(), AccountSelectionConfig::default())
    }

    fn request_bodies(&self) -> Vec<Vec<u8>> {
        self.requests.lock().unwrap().clone()
    }
}

fn chosen_reply() -> Result<Vec<u8>, Vec<u8>> {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let reply = routing.reply.lock().unwrap().clone().unwrap().unwrap();
    reply
}

#[tokio::test]
async fn routing_default_legacy_cold_open_makes_one_attempt_and_keeps_the_route() {
    let daemon = RoutingDaemon::start_with_opens(
        chosen_reply(),
        false,
        vec![(DEFAULT_ROUTING_MODULE, None)],
        None,
    )
    .await;
    for _ in 0..2 {
        let choice = choose(
            &daemon.client(),
            "oauth:anthropic",
            false,
            Some(two_accounts()),
        )
        .await
        .unwrap();
        assert!(matches!(choice, AccountChoice::Routed { .. }));
    }
    assert_eq!(
        *daemon.opened_modules.lock().unwrap(),
        vec!["prefrontal-routing"]
    );
    assert_eq!(daemon.request_bodies().len(), 2);
}

#[tokio::test]
async fn routing_default_striatum_only_falls_through_unknown_module_and_keeps_the_route() {
    let daemon = RoutingDaemon::start_with_opens(
        chosen_reply(),
        false,
        vec![
            (DEFAULT_ROUTING_MODULE, Some("unknown_module")),
            (ROUTING_MODULE_FALLBACK, None),
        ],
        None,
    )
    .await;
    for _ in 0..2 {
        let choice = choose(
            &daemon.client(),
            "oauth:anthropic",
            false,
            Some(two_accounts()),
        )
        .await
        .unwrap();
        assert!(matches!(choice, AccountChoice::Routed { .. }));
    }
    assert_eq!(
        *daemon.opened_modules.lock().unwrap(),
        vec!["prefrontal-routing", "striatum"]
    );
    assert_eq!(daemon.request_bodies().len(), 2);
}

#[tokio::test]
async fn routing_default_warming_legacy_falls_through_to_striatum_at_once() {
    let daemon = RoutingDaemon::start_with_opens(
        chosen_reply(),
        false,
        vec![
            (DEFAULT_ROUTING_MODULE, Some("module_warming")),
            (ROUTING_MODULE_FALLBACK, None),
        ],
        None,
    )
    .await;
    daemon.transport.route().await.unwrap();
    assert_eq!(
        *daemon.opened_modules.lock().unwrap(),
        vec!["prefrontal-routing", "striatum"]
    );
}

#[tokio::test]
async fn routing_default_both_warming_retry_the_pair_until_legacy_registers() {
    let daemon = RoutingDaemon::start_with_opens(
        chosen_reply(),
        false,
        vec![
            (DEFAULT_ROUTING_MODULE, Some("module_warming")),
            (ROUTING_MODULE_FALLBACK, Some("module_warming")),
            (DEFAULT_ROUTING_MODULE, None),
        ],
        None,
    )
    .await;
    let choice = choose(
        &daemon.client(),
        "oauth:anthropic",
        false,
        Some(two_accounts()),
    )
    .await
    .unwrap();
    assert!(matches!(choice, AccountChoice::Routed { .. }));
    assert_eq!(
        *daemon.opened_modules.lock().unwrap(),
        vec!["prefrontal-routing", "striatum", "prefrontal-routing"]
    );
}

#[tokio::test]
async fn routing_default_final_refusal_surfaces_without_fallback_or_message_matching() {
    let daemon = RoutingDaemon::start_with_opens(
        chosen_reply(),
        false,
        vec![(DEFAULT_ROUTING_MODULE, Some("access_denied"))],
        None,
    )
    .await;
    let error = daemon.transport.route().await.unwrap_err();
    assert!(error.contains("access_denied"), "{error}");
    assert_eq!(
        *daemon.opened_modules.lock().unwrap(),
        vec!["prefrontal-routing"]
    );
    assert!(daemon.request_bodies().is_empty());
}

#[tokio::test]
async fn routing_explicit_legacy_id_never_falls_back() {
    let daemon = RoutingDaemon::start_with_opens(
        chosen_reply(),
        false,
        vec![(DEFAULT_ROUTING_MODULE, Some("unknown_module"))],
        Some(DEFAULT_ROUTING_MODULE.into()),
    )
    .await;
    let error = daemon.transport.route().await.unwrap_err();
    assert!(error.contains("unknown_module"), "{error}");
    assert_eq!(
        *daemon.opened_modules.lock().unwrap(),
        vec!["prefrontal-routing"]
    );
}

fn open_refusal(code: &str) -> broca_subc::ConnError {
    broca_subc::ConnError::Rejected(
        broca_subc::connection::RejectedMessage::from_error_frame_body_for_test(
            &serde_json::to_vec(&json!({"code":code,"message":"misleading unknown_module"}))
                .unwrap(),
        ),
    )
}

#[tokio::test(start_paused = true)]
async fn routing_cached_unknown_module_falls_through_without_another_wire_open() {
    let mut wire_opens = Vec::new();
    let result = open_routing_route(
        None,
        broca_subc::RouteOpenRetry::default_patience(),
        |id, retry| {
            assert_eq!(retry.budget, Duration::ZERO);
            std::future::ready(if id == DEFAULT_ROUTING_MODULE {
                // The injected attempt callback returns a cached refusal without an open request.
                Err(open_refusal("unknown_module"))
            } else {
                wire_opens.push(id);
                Ok(7)
            })
        },
    )
    .await
    .unwrap();
    assert_eq!(result, 7);
    assert_eq!(wire_opens, vec!["striatum"]);
}

#[tokio::test(start_paused = true)]
async fn routing_cached_warming_pairs_back_off_then_use_new_registration_within_deadline() {
    let start = tokio::time::Instant::now();
    let mut attempts = Vec::new();
    let result = open_routing_route(
        None,
        broca_subc::RouteOpenRetry::default_patience(),
        |id, retry| {
            assert_eq!(retry.budget, Duration::ZERO);
            attempts.push((id, tokio::time::Instant::now() - start));
            std::future::ready(if attempts.len() == 6 {
                Ok(7)
            } else {
                Err(open_refusal("module_warming"))
            })
        },
    )
    .await
    .unwrap();
    assert_eq!(result, 7);
    assert_eq!(
        attempts,
        vec![
            ("prefrontal-routing", Duration::ZERO),
            ("striatum", Duration::ZERO),
            ("prefrontal-routing", Duration::from_millis(100)),
            ("striatum", Duration::from_millis(100)),
            ("prefrontal-routing", Duration::from_millis(300)),
            ("striatum", Duration::from_millis(300)),
        ]
    );
    assert!(tokio::time::Instant::now() - start < broca_subc::ROUTE_OPEN_RETRY_DEADLINE);
}

#[tokio::test(start_paused = true)]
async fn routing_retry_pairs_stop_at_the_deadline_with_the_last_refusal_code() {
    let start = tokio::time::Instant::now();
    let mut attempts = Vec::new();
    let error = open_routing_route::<_, _, ()>(
        None,
        broca_subc::RouteOpenRetry::within(Duration::from_millis(250), Duration::from_secs(1)),
        |id, _| {
            attempts.push((id, tokio::time::Instant::now() - start));
            std::future::ready(Err(open_refusal("target_unavailable")))
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Some("target_unavailable"));
    assert_eq!(
        tokio::time::Instant::now() - start,
        Duration::from_millis(250)
    );
    assert_eq!(attempts.len(), 4);
}

#[tokio::test]
async fn routing_non_fallthrough_codes_are_final_on_either_id() {
    for terminal in [
        "access_denied",
        "module_reloading",
        "module_timeout",
        "scope_changed",
    ] {
        for final_on in [DEFAULT_ROUTING_MODULE, ROUTING_MODULE_FALLBACK] {
            let mut attempts = Vec::new();
            let error = open_routing_route::<_, _, ()>(
                None,
                broca_subc::RouteOpenRetry::default_patience(),
                |id, _| {
                    attempts.push(id);
                    std::future::ready(Err(open_refusal(if id == final_on {
                        terminal
                    } else {
                        "unknown_module"
                    })))
                },
            )
            .await
            .unwrap_err();
            assert_eq!(error.code(), Some(terminal));
            assert_eq!(attempts.last(), Some(&final_on));
            assert_eq!(
                attempts.len(),
                if final_on == DEFAULT_ROUTING_MODULE {
                    1
                } else {
                    2
                }
            );
        }
    }
}

#[tokio::test]
async fn stale_routing_handle_admits_send_with_exactly_one_delivered_request() {
    let daemon = RoutingDaemon::start(chosen_reply(), false).await;
    daemon.stale_cached_route().await;
    let client = daemon.client();
    let choice = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .expect("the undelivered stale request must not refuse the send");
    assert!(
        matches!(choice, AccountChoice::Routed { account_id, credential_id, .. }
        if account_id == "acct-b" && credential_id == "oauth:anthropic:acct-b")
    );
    let bodies = daemon.request_bodies();
    assert_eq!(bodies.len(), 1, "routing sees only the reopened request");
    let request: Value = serde_json::from_slice(&bodies[0]).unwrap();
    assert_eq!(request["method"], ACCOUNT_FOR_OP);
    assert_eq!(client.counters.get("unreachable"), 0);
    assert_eq!(daemon.opens.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn two_stale_routing_handles_refuse_send_routing_unavailable() {
    let daemon = RoutingDaemon::start(chosen_reply(), false).await;
    daemon.stale_cached_route().await;
    daemon
        .transport
        .stale_opens_for_test
        .store(1, Ordering::SeqCst);
    let client = daemon.client();
    let refusal = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .expect_err("the retry is bounded to one reopened route");
    assert_eq!(
        refusal,
        ChoiceRefusal::Unavailable(RoutingUnavailable::unreachable())
    );
    assert_eq!(
        broca_core::RunError::from(refusal).admission_error_class(),
        ErrorClass::Transient
    );
    assert_eq!(client.counters.get("unreachable"), 1);
    assert!(daemon.request_bodies().is_empty());
    assert_eq!(daemon.opens.load(Ordering::SeqCst), 2);
    assert!(matches!(
        *daemon.transport.route.lock().unwrap(),
        RouteSlot::Empty
    ));
}

#[tokio::test]
async fn stale_routing_cache_retries_all_management_bodies_unchanged() {
    let daemon = RoutingDaemon::start(Ok(br#"{"result":{}}"#.to_vec()), false).await;
    // Account operations and classifier discovery/selection/reporting share this cache.
    // Check the bytes, not just decoded JSON, so the retry cannot rebuild an identity.
    let methods = [
        ACCOUNT_FOR_OP,
        "route.account_status",
        ACCOUNT_OBSERVE_OP,
        "role.describe",
        "route.select",
        "route.report",
    ];
    let mut expected = Vec::new();
    for method in methods {
        daemon.stale_cached_route().await;
        let body = format!(r#"{{ "method": "{method}", "params": {{"event_id":"stable-id"}} }}"#)
            .into_bytes();
        assert!(daemon
            .transport
            .request(body.clone())
            .await
            .unwrap()
            .is_ok());
        expected.push(body);
    }
    assert_eq!(daemon.request_bodies(), expected);
    assert_eq!(daemon.opens.load(Ordering::SeqCst), methods.len() + 1);
}

#[tokio::test]
async fn stale_routing_capability_probe_caches_the_reopened_handle() {
    let daemon = RoutingDaemon::start(
        Ok(serde_json::to_vec(&json!({"result":{"capabilities":[
            "service_tier", "observe_backoff", "observe_credential_refused"
        ]}}))
        .unwrap()),
        false,
    )
    .await;
    daemon.stale_cached_route().await;
    assert!(daemon.transport.service_tier_supported().await.unwrap());
    assert!(daemon.transport.observe_backoff_supported().await.unwrap());
    assert!(daemon
        .transport
        .observe_credential_refused_supported()
        .await
        .unwrap());
    assert_eq!(
        daemon.request_bodies().len(),
        1,
        "the successful capability probe is cached"
    );
    let cached_handle = daemon
        .transport
        .capabilities
        .lock()
        .await
        .as_ref()
        .unwrap()
        .0;
    assert!(daemon.transport.conn.route_is_open(&cached_handle));
    assert_eq!(daemon.opens.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn routing_reply_errors_and_possible_delivery_are_never_retried() {
    let refusal = serde_json::to_vec(&json!({
        "code":"target_unavailable", "message":"stale subc route handle (channel 8, epoch 1)"
    }))
    .unwrap();
    for close_after_request in [false, true] {
        let daemon = RoutingDaemon::start(Err(refusal.clone()), close_after_request).await;
        let reply = daemon
            .transport
            .request(br#"{"method":"route.account_for","params":{}}"#.to_vec())
            .await;
        if close_after_request {
            assert!(
                reply.is_err(),
                "a Goodbye after delivery has no retry permission"
            );
        } else {
            assert_eq!(
                reply,
                Ok(Err(refusal.clone())),
                "refusal text cannot authorize a retry"
            );
        }
        assert_eq!(daemon.request_bodies().len(), 1);
        assert_eq!(daemon.opens.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn stale_routing_reopen_stays_inside_the_original_admission_deadline() {
    let daemon = RoutingDaemon::start(chosen_reply(), false).await;
    daemon.stale_cached_route().await;
    daemon.hang_reopens.store(true, Ordering::SeqCst);
    let client = daemon.client();
    let deadline = tokio::time::Instant::now() + ADMISSION_BUDGET;
    let outcome = client.account_for(json!({}), deadline).await;
    assert!(matches!(outcome, AccountForOutcome::NoDecisionWithinBudget));
    assert!(tokio::time::Instant::now() >= deadline);
    assert_eq!(
        daemon.opens.load(Ordering::SeqCst),
        2,
        "the retry reached route.open"
    );
    assert!(daemon.request_bodies().is_empty());
}
