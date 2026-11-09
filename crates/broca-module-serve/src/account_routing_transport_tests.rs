use super::*;
use subc_control::{ClientControlRequest, ClientControlResponse};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{read_frame, write_frame};

struct RoutingDaemon {
    transport: Arc<SubcRoutingTransport>,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    opens: Arc<AtomicUsize>,
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
        let hang_reopens = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server = {
            let requests = Arc::clone(&requests);
            let opens = Arc::clone(&opens);
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
                            assert!(matches!(request, ClientControlRequest::RouteOpen { .. }));
                            let channel = 10 + opens.fetch_add(1, Ordering::SeqCst) as u16;
                            if channel > 10 && hang_reopens.load(Ordering::SeqCst) {
                                std::future::pending::<()>().await;
                            }
                            let body = serde_json::to_vec(&ClientControlResponse::RouteOpen {
                                route_channel: channel,
                                route_epoch: 1,
                            })
                            .unwrap();
                            (FrameType::Response, body)
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
        let transport = Arc::new(SubcRoutingTransport::new(
            conn,
            DEFAULT_ROUTING_MODULE.into(),
            session(),
        ));
        Self {
            transport,
            requests,
            opens,
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
