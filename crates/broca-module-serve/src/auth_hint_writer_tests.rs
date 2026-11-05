#[derive(Default)]
struct AuthHintRouting {
    requests: Mutex<Vec<serde_json::Value>>,
}

#[async_trait::async_trait]
impl broca_module_serve::account_routing::RoutingTransport for AuthHintRouting {
    async fn request(&self, body: Vec<u8>) -> broca_module_serve::account_routing::RoutingReply {
        self.requests.lock().unwrap().push(serde_json::from_slice(&body).unwrap());
        Ok(Err(serde_json::to_vec(&serde_json::json!({"code":"pool_exhausted","message":"fixture pool exhausted"})).unwrap()))
    }
}

async fn auth_hint_executor(root: &std::path::Path, daemon: &CapturingVaultDaemon, routing: Arc<AuthHintRouting>, url: &str) -> LiveLoopExecutor {
    let mut live = executor_with_capturing_vault(root, daemon).await;
    live.provider_overrides = broca_module_serve::provider_overrides::ProviderOverrides::parse(
        &serde_json::json!({"providers":{"openai":{"base_url":url}}}).to_string()).unwrap();
    let listing = serde_json::from_value(serde_json::json!({"credentials":[
        {"id":"oauth:openai","type":"oauth","categories":["llm"],"serves":["openai"],"state":"active","record_version":1,
            "operations":["read"],"provider_ids":["openai"],"auth_method":"oauth","account_id":"account-a"},
        {"id":"oauth:openai:spare","type":"oauth","categories":["llm"],"serves":["openai"],"state":"active","record_version":1,
            "operations":["read"],"provider_ids":["openai"],"auth_method":"oauth","account_id":"account-b"}
    ],"grants":1,"grant_tuples":[],"view":"auth-hints"})).unwrap();
    live.vault.as_ref().unwrap().listing.store(listing);
    live.routing = Some(Arc::new(broca_module_serve::account_routing::RoutingClient::new(routing,
        broca_module_serve::account_routing::AccountSelectionConfig::default())));
    live
}

#[tokio::test]
async fn routed_auth_pool_and_credential_preference_reach_account_for() {
    if !run_catalog_drift_child("routed_auth_pool_and_credential_preference_reach_account_for") { return; }
    install_queued_ultrafast_catalog();
    let root = fresh_vault_root("auth-hint-send");
    let daemon = CapturingVaultDaemon::start(&root).await;
    let http = resume_http_fixture().await;
    let routing = Arc::new(AuthHintRouting::default());
    let live = auth_hint_executor(&root, &daemon, routing.clone(), &http.url).await;
    let mut rig = TierServeRig::start_with_executor(&root, None, None, live, None).await;
    let reply = rig.call("session.send", serde_json::json!({"send_id":"pooled","prompt":"hello",
        "model":{"provider":"openai","model":"test-ultrafast"},
        "auth":{"method":"oauth","pool":"selected-pool","credential_id":"preferred-credential"}})).await;
    assert_eq!(reply.header.ty, subc_protocol::FrameType::Error);
    {
        let requests = routing.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let params = &requests[0]["params"];
        assert_eq!(params["pool"], "selected-pool");
        assert_eq!(params["requested_account"]["credential_id"], "preferred-credential");
        assert!(params.get("credential_id").is_none());
    }
    assert!(http.requests.lock().unwrap().is_empty());
    rig.close().await;
    drop(daemon);
    std::fs::remove_dir_all(root).unwrap();
}

async fn refuse_mixed_auth_hint(hint: &str) {
    use broca_core::WalStore as _;
    let root = fresh_vault_root("auth-hint-conflict");
    let daemon = CapturingVaultDaemon::start(&root).await;
    let http = resume_http_fixture().await;
    let routing = Arc::new(AuthHintRouting::default());
    let live = auth_hint_executor(&root, &daemon, routing.clone(), &http.url).await;
    let mut rig = TierServeRig::start_with_executor(&root, None, None, live, None).await;
    let before = rig.wal.live_wal_bytes(&rig.session).unwrap();
    let mut auth = serde_json::json!({"method":"oauth","account":"work"});
    auth[hint] = serde_json::json!("preference");
    let reply = rig.call("session.send", serde_json::json!({"send_id":"conflict","prompt":"hello",
        "model":{"provider":"openai","model":"test-ultrafast"},"auth":auth})).await;
    let value = tier_reply(&reply);
    assert_eq!(value["code"], "invalid_params");
    assert_eq!(value["detail"]["field"], "auth.account");
    assert!(value["message"].as_str().unwrap().contains("pin"));
    assert_eq!(rig.wal.live_wal_bytes(&rig.session).unwrap(), before);
    assert!(routing.requests.lock().unwrap().is_empty());
    assert!(daemon.route_open_starts.lock().unwrap().is_empty());
    assert!(http.requests.lock().unwrap().is_empty());
    rig.close().await;
    drop(daemon);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn routed_auth_pin_cannot_be_combined_with_pool() {
    if !run_catalog_drift_child("routed_auth_pin_cannot_be_combined_with_pool") { return; }
    install_queued_ultrafast_catalog();
    refuse_mixed_auth_hint("pool").await;
}

#[tokio::test]
async fn routed_auth_pin_cannot_be_combined_with_credential_preference() {
    if !run_catalog_drift_child("routed_auth_pin_cannot_be_combined_with_credential_preference") { return; }
    install_queued_ultrafast_catalog();
    refuse_mixed_auth_hint("credential_id").await;
}

#[tokio::test]
async fn routed_auth_queued_pool_survives_restart_and_drains_to_account_for() {
    use broca_core::WalStore as _;
    use broca_module_serve::wire::SendParamsExt as _;
    if !run_catalog_drift_child("routed_auth_queued_pool_survives_restart_and_drains_to_account_for") { return; }
    install_queued_ultrafast_catalog();
    let root = fresh_vault_root("auth-hint-queue");
    let session = drift_request(&root).session;
    {
        let params: broca_wire::SendParams = serde_json::from_value(serde_json::json!({
            "send_id":"queued-pool","prompt":"hello","model":{"provider":"openai","model":"test-ultrafast"},
            "auth":{"method":"oauth","pool":"persisted-pool","credential_id":"persisted-preference"}
        })).unwrap();
        let request = params.into_send_request(session.clone(), RunId("queued-pool".into())).unwrap();
        let queued: broca_wal::WalRecord = serde_json::from_value(serde_json::json!({
            "type":"queued","submission_id":"sub-queued-pool","request":request.request
        })).unwrap();
        let store = FileWalStore::new(root.join("wal"));
        let mut wal = store.open(&session).unwrap();
        wal.append(1, &queued, true).unwrap();
    }
    let daemon = CapturingVaultDaemon::start(&root).await;
    let http = resume_http_fixture().await;
    let routing = Arc::new(AuthHintRouting::default());
    let live = auth_hint_executor(&root, &daemon, routing.clone(), &http.url).await;
    let store = live.wal_store.clone();
    let sessions = live.session_store.clone().unwrap();
    let opener = broca_module::DefaultOpener::new(
        Arc::new(broca_core::FileLeaseStore::new(root.join("leases"))), store.clone(), Arc::new(live),
    ).with_run_index(sessions.clone()).with_materialized_store(sessions);
    let registry = broca_module::LineageRegistry::new(Arc::new(opener));
    registry.actor_for(&session).await.unwrap();
    broca_test_support::wait_until("reopened queued account request", || std::future::ready(
        if routing.requests.lock().unwrap().is_empty() { Err("no account request yet".to_owned()) } else { Ok(()) }
    )).await;
    {
        let requests = routing.requests.lock().unwrap();
        assert_eq!(requests[0]["params"]["pool"], "persisted-pool");
        assert_eq!(requests[0]["params"]["requested_account"]["credential_id"], "persisted-preference");
    }
    assert!(registry.stop_and_seal().await);
    drop(registry);
    drop(store);
    drop(daemon);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn auth_routing_hints_do_not_change_unlisted_method_submission_bytes_or_identity() {
    use broca_module_serve::wire::SendParamsExt;
    use broca_protocol::{BindIdentity, RunId};
    for method in ["apikey", "antigravity"] {
        let base = serde_json::json!({"prompt":"hello","model":{"provider":"anthropic","model":"fixture"},
            "auth":{"method":method,"account":"work"}});
        let mut hinted = base.clone();
        hinted["auth"]["pool"] = serde_json::json!("ignored-pool");
        hinted["auth"]["credential_id"] = serde_json::json!("ignored-credential");
        let convert = |value| serde_json::from_value::<broca_wire::SendParams>(value).unwrap()
            .into_send_request(BindIdentity::new("/private", "runner", "session"), RunId("run".into())).unwrap().request;
        let base = convert(base);
        let hinted = convert(hinted);
        assert_eq!(serde_json::to_vec(&base).unwrap(), serde_json::to_vec(&hinted).unwrap());
        assert_eq!(broca_core::submission_identity_digest(&base), broca_core::submission_identity_digest(&hinted));
    }
}
