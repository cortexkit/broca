use super::*;
use subc_control::{CatalogEntry, ClientControlRequest, ClientControlResponse};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{read_frame, write_frame};

struct RoutingDaemon {
    transport: Arc<SubcRoutingTransport>,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    opens: Arc<AtomicUsize>,
    opened_modules: Arc<Mutex<Vec<String>>>,
    catalog: Arc<Mutex<Vec<CatalogEntry>>>,
    catalog_reads: Arc<AtomicUsize>,
    hang_reopens: Arc<std::sync::atomic::AtomicBool>,
    hang_requests: Arc<std::sync::atomic::AtomicBool>,
    reply: Arc<Mutex<Result<Vec<u8>, Vec<u8>>>>,
    goodbyes: tokio::sync::mpsc::UnboundedSender<broca_subc::RouteHandle>,
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
        let catalog = Arc::new(Mutex::new(vec![routing_provider("routing-provider", true)]));
        let catalog_reads = Arc::new(AtomicUsize::new(0));
        let hang_reopens = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hang_requests = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reply = Arc::new(Mutex::new(reply));
        let (goodbyes, mut goodbye_rx) =
            tokio::sync::mpsc::unbounded_channel::<broca_subc::RouteHandle>();
        let server = {
            let reply = Arc::clone(&reply);
            let requests = Arc::clone(&requests);
            let opens = Arc::clone(&opens);
            let opened_modules = Arc::clone(&opened_modules);
            let catalog = Arc::clone(&catalog);
            let catalog_reads = Arc::clone(&catalog_reads);
            let hang_reopens = Arc::clone(&hang_reopens);
            let hang_requests = Arc::clone(&hang_requests);
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
                loop {
                    let frame = tokio::select! {
                        frame = read_frame(&mut stream) => match frame.unwrap() {
                            Some(frame) => frame,
                            None => break,
                        },
                        Some(route) = goodbye_rx.recv() => {
                            let goodbye = Frame::build(
                                FrameType::Goodbye,
                                Flags::new(false, Priority::Interactive, false),
                                route.channel, route.epoch, 0, Vec::new(),
                            ).unwrap();
                            write_frame(&mut stream, &goodbye).await.unwrap();
                            continue;
                        }
                    };
                    let (ty, body) = match (frame.header.channel, frame.header.ty) {
                        (0, FrameType::Ping) => (FrameType::Pong, Vec::new()),
                        (0, FrameType::Request) => {
                            let request: ClientControlRequest =
                                serde_json::from_slice(&frame.body).unwrap();
                            if matches!(request, ClientControlRequest::CatalogList { .. }) {
                                catalog_reads.fetch_add(1, Ordering::SeqCst);
                                let response = ClientControlResponse::CatalogList {
                                    generation: 1,
                                    modules: catalog.lock().unwrap().clone(),
                                    subc_ops: Vec::new(),
                                };
                                let response = Frame::build(
                                    FrameType::Response,
                                    Flags::new(false, Priority::Interactive, false),
                                    frame.header.channel,
                                    frame.header.epoch,
                                    frame.header.corr,
                                    serde_json::to_vec(&response).unwrap(),
                                )
                                .unwrap();
                                write_frame(&mut stream, &response).await.unwrap();
                                continue;
                            }
                            let ClientControlRequest::RouteOpen { target, .. } = request else {
                                panic!("expected catalog.list or route.open");
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
                                    request_window: None,
                                    watch_window: None,
                                }).unwrap()),
                            }
                        }
                        (_, FrameType::Goodbye) => continue,
                        (channel, FrameType::Request) if channel != 0 => {
                            requests.lock().unwrap().push(frame.body.clone());
                            if hang_requests.load(Ordering::SeqCst) {
                                continue;
                            }
                            if close_after_request {
                                (FrameType::Goodbye, Vec::new())
                            } else {
                                match &*reply.lock().unwrap() {
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
            catalog,
            catalog_reads,
            hang_reopens,
            hang_requests,
            reply,
            goodbyes,
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

fn capability_reply(declared: bool) -> Result<Vec<u8>, Vec<u8>> {
    describe_reply(if declared { &["account_continue"] } else { &[] })
}

fn describe_reply(capabilities: &[&str]) -> Result<Vec<u8>, Vec<u8>> {
    let mut describe = cortexkit_role_routing::describe::RoleDescribe::new(vec![], false);
    for capability in capabilities {
        describe = describe.with_capability(*capability);
    }
    Ok(serde_json::to_vec(&json!({"result": describe})).unwrap())
}

async fn advance_cache_age(duration: Duration) {
    // Pause only after network setup, and resume before another exchange, so socket I/O
    // does not accidentally auto-advance its authentication or request deadlines.
    tokio::time::pause();
    tokio::time::advance(duration).await;
    tokio::time::resume();
}

#[tokio::test]
async fn routing_capabilities_declared_and_cached_across_names() {
    use crate::routing_capabilities::CapabilitySupport::*;
    let daemon = RoutingDaemon::start(capability_reply(true), false).await;
    let client = daemon.client();
    assert_eq!(client.capability("account_continue").await, Declared);
    assert_eq!(client.capability("account_continue").await, Declared);
    assert_eq!(client.capability("not_a_group").await, NotDeclared);
    assert_eq!(
        daemon.request_bodies(),
        vec![br#"{"method":"role.describe","params":{}}"#.to_vec()]
    );
    assert_eq!(daemon.catalog_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn routing_capabilities_not_declared_is_distinct_from_unknown() {
    let daemon = RoutingDaemon::start(capability_reply(false), false).await;
    assert_eq!(
        daemon.client().capability("account_continue").await,
        crate::routing_capabilities::CapabilitySupport::NotDeclared
    );
}

#[tokio::test]
async fn routing_capabilities_unreachable_and_unresolved_are_unknown() {
    use crate::routing_capabilities::CapabilitySupport::Unknown;
    let daemon = RoutingDaemon::start(capability_reply(true), true).await;
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Unknown
    );
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Unknown
    );
    assert_eq!(
        daemon.request_bodies().len(),
        1,
        "unreachable answer is cached"
    );
    let daemon = RoutingDaemon::start(capability_reply(true), false).await;
    daemon.catalog.lock().unwrap().clear();
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Unknown
    );
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Unknown
    );
    assert_eq!(daemon.catalog_reads.load(Ordering::SeqCst), 1);
    assert!(daemon.request_bodies().is_empty());
}

#[tokio::test]
async fn routing_capabilities_undecodable_or_refused_describe_is_unknown() {
    use crate::routing_capabilities::CapabilitySupport::Unknown;
    for reply in [
        Ok(b"not JSON".to_vec()),
        Ok(br#"{"result":{"capabilities":["account_continue"]}}"#.to_vec()),
        Err(br#"{"code":"invalid_request"}"#.to_vec()),
    ] {
        let daemon = RoutingDaemon::start(reply, false).await;
        assert_eq!(
            daemon.client().capability("account_continue").await,
            Unknown
        );
        assert_eq!(
            daemon.client().capability("account_continue").await,
            Unknown
        );
        assert_eq!(daemon.request_bodies().len(), 1);
    }
}

#[tokio::test]
async fn routing_capabilities_reregistration_goodbye_drops_cached_description() {
    use crate::routing_capabilities::CapabilitySupport::*;
    let daemon = RoutingDaemon::start(capability_reply(true), false).await;
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Declared
    );
    let route = daemon.transport.route().await.unwrap();
    *daemon.reply.lock().unwrap() = capability_reply(false);
    daemon.goodbyes.send(route).unwrap();
    broca_test_support::wait_until("routing re-registration closes its old route", || async {
        (!daemon.transport.conn.route_is_open(&route))
            .then_some(())
            .ok_or_else(|| "old routing route is still open".to_owned())
    })
    .await;
    assert_eq!(
        daemon.client().capability("account_continue").await,
        NotDeclared
    );
    assert_eq!(daemon.request_bodies().len(), 2);
    assert_eq!(daemon.opens.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn routing_capabilities_age_bound_recovers_an_unknown_provider() {
    use crate::routing_capabilities::CapabilitySupport::*;
    let daemon = RoutingDaemon::start(capability_reply(true), false).await;
    daemon.catalog.lock().unwrap().clear();
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Unknown
    );
    *daemon.catalog.lock().unwrap() = vec![routing_provider("routing-provider", true)];
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Unknown
    );
    advance_cache_age(Duration::from_secs(4)).await;
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Unknown
    );
    assert_eq!(daemon.catalog_reads.load(Ordering::SeqCst), 1);
    advance_cache_age(Duration::from_secs(1)).await;
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Declared
    );
    assert_eq!(daemon.catalog_reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn routing_capabilities_known_description_is_read_again_after_sixty_seconds() {
    use crate::routing_capabilities::CapabilitySupport::*;
    let daemon = RoutingDaemon::start(capability_reply(true), false).await;
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Declared
    );
    *daemon.reply.lock().unwrap() = capability_reply(false);
    advance_cache_age(Duration::from_secs(59)).await;
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Declared
    );
    assert_eq!(daemon.request_bodies().len(), 1);
    advance_cache_age(Duration::from_secs(1)).await;
    assert_eq!(
        daemon.client().capability("account_continue").await,
        NotDeclared
    );
    assert_eq!(daemon.request_bodies().len(), 2);
}

#[tokio::test]
async fn routing_capabilities_failed_refresh_keeps_known_answers_on_an_open_route() {
    use crate::routing_capabilities::CapabilitySupport::*;
    let daemon = RoutingDaemon::start(describe_reply(&["service_tier"]), false).await;
    assert!(daemon.transport.service_tier_supported().await.unwrap());
    let route = daemon.transport.route().await.unwrap();
    advance_cache_age(Duration::from_secs(60)).await;
    *daemon.reply.lock().unwrap() = Err(br#"{"code":"unavailable"}"#.to_vec());
    assert!(daemon.transport.service_tier_supported().await.unwrap());
    assert_eq!(
        daemon.client().capability("observe_backoff").await,
        NotDeclared
    );
    assert_eq!(daemon.request_bodies().len(), 2);
    assert!(daemon.transport.conn.route_is_open(&route));
    advance_cache_age(Duration::from_secs(59)).await;
    assert!(daemon.transport.service_tier_supported().await.unwrap());
    assert_eq!(
        daemon.request_bodies().len(),
        2,
        "a failed refresh restarts the known age"
    );
}

#[tokio::test]
async fn routing_capabilities_timed_out_refresh_keeps_known_description() {
    use crate::routing_capabilities::CapabilitySupport::*;
    let daemon = RoutingDaemon::start(capability_reply(true), false).await;
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Declared
    );
    advance_cache_age(Duration::from_secs(60)).await;
    daemon.hang_requests.store(true, Ordering::SeqCst);
    let transport = Arc::clone(&daemon.transport);
    let refresh = tokio::spawn(async move { transport.capability("account_continue").await });
    broca_test_support::wait_until("capability refresh reached routing", || async {
        (daemon.request_bodies().len() == 2)
            .then_some(())
            .ok_or_else(|| "no refresh request".to_owned())
    })
    .await;
    advance_cache_age(ADMISSION_BUDGET).await;
    assert_eq!(refresh.await.unwrap(), Declared);
    assert_eq!(
        daemon.client().capability("account_continue").await,
        Declared
    );
    assert_eq!(daemon.request_bodies().len(), 2);
}

#[tokio::test]
async fn routing_capabilities_closed_route_drops_known_answers_even_if_refresh_fails() {
    use crate::routing_capabilities::CapabilitySupport::Unknown;
    let daemon = RoutingDaemon::start(describe_reply(&["service_tier"]), false).await;
    assert!(daemon.transport.service_tier_supported().await.unwrap());
    let route = daemon.transport.route().await.unwrap();
    *daemon.reply.lock().unwrap() = Err(br#"{"code":"unavailable"}"#.to_vec());
    daemon.goodbyes.send(route).unwrap();
    broca_test_support::wait_until("routing closed its old capability route", || async {
        (!daemon.transport.conn.route_is_open(&route))
            .then_some(())
            .ok_or_else(|| "old route still open".to_owned())
    })
    .await;
    assert!(daemon.transport.service_tier_supported().await.is_err());
    assert_eq!(daemon.client().capability("service_tier").await, Unknown);
    assert_eq!(daemon.request_bodies().len(), 2);
}

#[tokio::test]
async fn routing_capabilities_error_frame_is_unknown_for_all_callers_and_recovers_after_five_seconds(
) {
    use crate::routing_capabilities::CapabilitySupport::Unknown;
    let daemon = RoutingDaemon::start(Err(br#"{"code":"unavailable"}"#.to_vec()), false).await;
    let error = daemon.transport.service_tier_supported().await.unwrap_err();
    assert!(
        error.contains("routing capabilities could not be read"),
        "{error}"
    );
    assert_eq!(daemon.client().capability("service_tier").await, Unknown);
    assert!(daemon.transport.observe_backoff_supported().await.is_err());
    assert!(daemon
        .transport
        .observe_credential_refused_supported()
        .await
        .is_err());
    let outcome = daemon
        .client()
        .account_for(
            json!({"service_tier":"fast"}),
            tokio::time::Instant::now() + ADMISSION_BUDGET,
        )
        .await;
    assert!(
        matches!(outcome, AccountForOutcome::Unreachable(_)),
        "{outcome:?}"
    );
    assert_eq!(
        daemon.request_bodies().len(),
        1,
        "unknown capabilities must not silently downgrade a tier send"
    );
    *daemon.reply.lock().unwrap() = describe_reply(&[
        "service_tier",
        "observe_backoff",
        "observe_credential_refused",
    ]);
    advance_cache_age(Duration::from_secs(4)).await;
    assert!(daemon.transport.service_tier_supported().await.is_err());
    assert_eq!(daemon.request_bodies().len(), 1);
    advance_cache_age(Duration::from_secs(1)).await;
    assert!(daemon.transport.service_tier_supported().await.unwrap());
    assert!(daemon.transport.observe_backoff_supported().await.unwrap());
    assert!(daemon
        .transport
        .observe_credential_refused_supported()
        .await
        .unwrap());
    assert_eq!(
        daemon.request_bodies().len(),
        2,
        "all support APIs share the successful description"
    );
}

#[tokio::test]
async fn routing_capabilities_waiting_tier_caller_cancels_at_its_own_deadline() {
    let daemon = RoutingDaemon::start(describe_reply(&["service_tier"]), false).await;
    daemon.hang_requests.store(true, Ordering::SeqCst);
    let transport = Arc::clone(&daemon.transport);
    let first = tokio::spawn(async move { transport.service_tier_supported().await });
    broca_test_support::wait_until("first caller holds the capability refresh", || async {
        (daemon.request_bodies().len() == 1)
            .then_some(())
            .ok_or_else(|| "no describe request".to_owned())
    })
    .await;
    let outcome = daemon
        .client()
        .account_for(
            json!({"service_tier":"fast"}),
            tokio::time::Instant::now() + Duration::from_millis(20),
        )
        .await;
    assert_eq!(outcome, AccountForOutcome::NoDecisionWithinBudget);
    assert!(
        !first.is_finished(),
        "the waiting caller's deadline does not cancel the single flight"
    );
    assert_eq!(daemon.request_bodies().len(), 1);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    daemon.hang_requests.store(false, Ordering::SeqCst);
    assert!(daemon.transport.service_tier_supported().await.unwrap());
    assert_eq!(
        daemon.request_bodies().len(),
        2,
        "cancelled lock waiters leave no abandoned refresh"
    );
}

fn routing_provider(module: &str, verified: bool) -> CatalogEntry {
    CatalogEntry::new(module, Vec::new(), Vec::new())
        .with_capabilities(Some(subc_protocol::manifest::CapabilityDeclarations {
            provides: vec!["routing/v1".into()],
            requires: Vec::new(),
            must_never_reach: Vec::new(),
        }))
        .with_nonce_verified(verified)
}

fn chosen_reply() -> Result<Vec<u8>, Vec<u8>> {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let reply = routing.reply.lock().unwrap().clone().unwrap().unwrap();
    reply
}

#[tokio::test]
async fn routing_sole_verified_provider_resolves_and_caches_route_and_module() {
    let daemon = RoutingDaemon::start(chosen_reply(), false).await;
    // Unverified claimants and unrelated verified modules cannot become routing targets.
    let unrelated =
        CatalogEntry::new("other-role", Vec::new(), Vec::new()).with_nonce_verified(true);
    daemon
        .catalog
        .lock()
        .unwrap()
        .extend([routing_provider("unverified-claimant", false), unrelated]);
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
        vec!["routing-provider"]
    );
    assert_eq!(daemon.catalog_reads.load(Ordering::SeqCst), 1);
    daemon.stale_cached_route().await;
    assert!(daemon
        .transport
        .request(b"report".to_vec())
        .await
        .unwrap()
        .is_ok());
    assert_eq!(
        daemon.catalog_reads.load(Ordering::SeqCst),
        1,
        "a stale handle alone does not invalidate the provider"
    );
    assert_eq!(daemon.request_bodies().len(), 3);
}

#[tokio::test]
async fn routing_provider_rename_re_resolves_once_for_each_disappearance_code() {
    for code in ["unknown_module", "target_unavailable", "module_removed"] {
        let daemon = RoutingDaemon::start_with_opens(
            chosen_reply(),
            false,
            vec![
                ("routing-provider", None),
                ("routing-provider", Some(code)),
                ("renamed-provider", None),
            ],
            None,
        )
        .await;
        assert!(daemon
            .transport
            .request(b"first".to_vec())
            .await
            .unwrap()
            .is_ok());
        daemon.stale_cached_route().await;
        *daemon.catalog.lock().unwrap() = vec![routing_provider("renamed-provider", true)];
        for body in [b"second".to_vec(), b"third".to_vec()] {
            assert!(daemon.transport.request(body).await.unwrap().is_ok());
        }
        assert_eq!(
            *daemon.opened_modules.lock().unwrap(),
            vec!["routing-provider", "routing-provider", "renamed-provider"],
            "{code}"
        );
        assert_eq!(daemon.catalog_reads.load(Ordering::SeqCst), 2, "{code}");
        assert_eq!(
            daemon.request_bodies(),
            vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()]
        );
    }
}

#[tokio::test]
async fn routing_missing_or_unverified_provider_refuses_routing_unavailable() {
    for catalog in [Vec::new(), vec![routing_provider("self-registered", false)]] {
        let daemon = RoutingDaemon::start(chosen_reply(), false).await;
        *daemon.catalog.lock().unwrap() = catalog;
        let error = daemon.transport.route().await.unwrap_err();
        assert!(error.contains("routing_provider_unavailable"), "{error}");
        let refusal = choose(
            &daemon.client(),
            "oauth:anthropic",
            false,
            Some(two_accounts()),
        )
        .await
        .expect_err("no verified routing provider must refuse admission");
        assert_eq!(
            refusal,
            ChoiceRefusal::Unavailable(RoutingUnavailable::unreachable())
        );
        assert_eq!(
            broca_core::RunError::from(refusal).admission_error_class(),
            ErrorClass::Transient
        );
        assert_eq!(daemon.opens.load(Ordering::SeqCst), 0);
        assert!(daemon.request_bodies().is_empty());
    }
}

#[tokio::test]
async fn routing_two_verified_providers_refuse_with_distinct_diagnostic() {
    let daemon = RoutingDaemon::start(chosen_reply(), false).await;
    daemon
        .catalog
        .lock()
        .unwrap()
        .push(routing_provider("second-provider", true));
    let error = daemon.transport.route().await.unwrap_err();
    assert!(error.contains("routing_provider_ambiguous"), "{error}");
    assert!(
        error.contains("routing-provider, second-provider"),
        "{error}"
    );
    let refusal = choose(
        &daemon.client(),
        "oauth:anthropic",
        false,
        Some(two_accounts()),
    )
    .await
    .expect_err("ambiguous providers must not choose an account");
    assert_eq!(
        refusal,
        ChoiceRefusal::Unavailable(RoutingUnavailable::unreachable())
    );
    assert_eq!(daemon.opens.load(Ordering::SeqCst), 0);
    assert!(daemon.request_bodies().is_empty());
}

#[tokio::test]
async fn routing_explicit_id_is_used_as_is_without_catalog_or_fallback() {
    for refusal in [None, Some("unknown_module")] {
        let daemon = RoutingDaemon::start_with_opens(
            chosen_reply(),
            false,
            vec![(" explicit-id ", refusal)],
            Some(" explicit-id ".into()),
        )
        .await;
        let result = daemon.transport.route().await;
        if refusal.is_some() {
            assert!(result.unwrap_err().contains("unknown_module"));
        } else {
            result.unwrap();
        }
        assert_eq!(
            *daemon.opened_modules.lock().unwrap(),
            vec![" explicit-id "]
        );
        assert_eq!(daemon.catalog_reads.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn routing_disappearance_re_resolves_at_most_once_before_refusing() {
    for code in ["unknown_module", "target_unavailable", "module_removed"] {
        let daemon = RoutingDaemon::start_with_opens(
            chosen_reply(),
            false,
            vec![("gone-provider", Some(code)), ("gone-provider", Some(code))],
            None,
        )
        .await;
        *daemon.catalog.lock().unwrap() = vec![routing_provider("gone-provider", true)];
        let error = daemon.transport.route().await.unwrap_err();
        assert!(error.contains(code), "{error}");
        assert_eq!(daemon.catalog_reads.load(Ordering::SeqCst), 2, "{code}");
        assert!(daemon.opens.load(Ordering::SeqCst) <= 2, "{code}");
        assert!(daemon.transport.resolved_module.lock().unwrap().is_none());
        assert!(daemon.request_bodies().is_empty());
    }
}

#[tokio::test]
async fn routing_warming_provider_retries_without_re_resolving() {
    let daemon = RoutingDaemon::start_with_opens(
        chosen_reply(),
        false,
        vec![
            ("routing-provider", Some("module_warming")),
            ("routing-provider", None),
        ],
        None,
    )
    .await;
    daemon.transport.route().await.unwrap();
    assert_eq!(
        *daemon.opened_modules.lock().unwrap(),
        vec!["routing-provider", "routing-provider"]
    );
    assert_eq!(daemon.catalog_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn routing_warming_retries_stop_at_the_deadline() {
    let daemon = RoutingDaemon::start_with_opens(
        chosen_reply(),
        false,
        vec![("routing-provider", Some("module_warming"))],
        None,
    )
    .await;
    let start = tokio::time::Instant::now();
    let error = open_routing_route(
        &daemon.transport.conn,
        None,
        &daemon.transport.resolved_module,
        &session(),
        broca_subc::RouteOpenRetry::within(Duration::from_millis(50), Duration::from_secs(1)),
    )
    .await
    .unwrap_err();
    assert!(error.contains("module_warming"), "{error}");
    assert!(tokio::time::Instant::now() - start >= Duration::from_millis(50));
    assert_eq!(daemon.opens.load(Ordering::SeqCst), 1);
    assert_eq!(daemon.catalog_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn routing_other_open_codes_are_final_without_message_matching() {
    for terminal in [
        "access_denied",
        "module_reloading",
        "module_timeout",
        "scope_changed",
    ] {
        let daemon = RoutingDaemon::start_with_opens(
            chosen_reply(),
            false,
            vec![("routing-provider", Some(terminal))],
            None,
        )
        .await;
        let error = daemon.transport.route().await.unwrap_err();
        assert!(error.contains(terminal), "{error}");
        assert_eq!(
            *daemon.opened_modules.lock().unwrap(),
            vec!["routing-provider"]
        );
        assert_eq!(daemon.catalog_reads.load(Ordering::SeqCst), 1);
        assert!(daemon.request_bodies().is_empty());
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
        describe_reply(&[
            "service_tier",
            "observe_backoff",
            "observe_credential_refused",
        ]),
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
    let cached_handle = daemon.transport.route().await.unwrap();
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

/// A continue question whose route goes stale between the capability check and the send is
/// sent again only after the reopened route's description is read: here the routing module
/// behind it no longer declares continuation, so nothing is sent there and the answer is
/// NotDeclared.
#[tokio::test]
async fn a_continue_request_on_a_reopened_route_rechecks_the_capabilities() {
    use crate::routing_capabilities::{CapabilitySupport, ACCOUNT_CONTINUE_RESUME};
    let daemon = RoutingDaemon::start(
        describe_reply(&["account_continue", ACCOUNT_CONTINUE_RESUME]),
        false,
    )
    .await;
    let client = daemon.client();
    assert!(client.answers_moved_runs_with_current_account().await);
    // Routing registers again without continuation, and the route the question is about to go
    // out on goes stale after the (cached) capabilities were read for it.
    *daemon.reply.lock().unwrap() = describe_reply(&[]);
    daemon
        .transport
        .stale_before_checked_send_for_test
        .store(true, Ordering::SeqCst);
    let request = cortexkit_role_routing::account_continue::ContinueRequest::new(
        "run-1",
        "run-1",
        "broca-session:abc",
        "decision-a",
        cortexkit_role_routing::account_continue::ExhaustedAccount::new(
            "credential-a",
            "acct-a",
            "seven_day",
            7,
        ),
        "wall-1",
    );
    let outcome = client
        .account_continue(
            true,
            &request,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await;
    assert_eq!(outcome, ContinueOutcome::NotDeclared);
    let methods: Vec<String> = daemon
        .request_bodies()
        .iter()
        .map(|body| {
            serde_json::from_slice::<Value>(body).unwrap()["method"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        methods,
        ["role.describe", "role.describe"],
        "no continue request was sent"
    );
    assert_eq!(
        client.capability("account_continue").await,
        CapabilitySupport::NotDeclared
    );
}
