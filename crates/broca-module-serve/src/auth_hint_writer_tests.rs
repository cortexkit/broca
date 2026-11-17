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
    let mut rig = TierServeRig::start_with_executor(&root, live).await;
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

#[tokio::test]
async fn account_route_admission_uses_routing_names_and_keeps_auth_hints() {
    if !run_catalog_drift_child("account_route_admission_uses_routing_names_and_keeps_auth_hints") { return; }
    install_queued_ultrafast_catalog();
    let root = fresh_vault_root("account-route-send");
    let daemon = CapturingVaultDaemon::start(&root).await;
    let http = resume_http_fixture().await;
    let routing = Arc::new(AuthHintRouting::default());
    let live = auth_hint_executor(&root, &daemon, routing.clone(), &http.url).await;
    let mut rig = TierServeRig::start_with_executor(&root, live).await;
    let reply = rig.call("session.send", serde_json::json!({"send_id":"routed-names","prompt":"hello",
        "model":{"provider":"openai","model":"test-ultrafast"},
        "account_route":{"provider":"routing-openai","model":"routing-model"},
        "route_decision_id":"caller-decision",
        "auth":{"method":"oauth","pool":"selected-pool","credential_id":"preferred-credential"}})).await;
    assert_eq!(reply.header.ty, subc_protocol::FrameType::Error);
    {
        let requests = routing.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let params = &requests[0]["params"];
        assert_eq!(params["provider"], "routing-openai");
        assert_eq!(params["model"], "routing-model");
        assert_eq!(params["affinity_model"], "routing-model");
        assert_eq!(params["decision_id"], "caller-decision");
        assert_eq!(params["pool"], "selected-pool");
        assert_eq!(params["requested_account"]["credential_id"], "preferred-credential");
    }
    assert!(http.requests.lock().unwrap().is_empty());
    rig.close().await;
    drop(daemon);
    std::fs::remove_dir_all(root).unwrap();
}

async fn refuse_mixed_auth_hint(hint: &str) {
    let root = fresh_vault_root("auth-hint-conflict");
    let daemon = CapturingVaultDaemon::start(&root).await;
    let http = resume_http_fixture().await;
    let routing = Arc::new(AuthHintRouting::default());
    let live = auth_hint_executor(&root, &daemon, routing.clone(), &http.url).await;
    let mut rig = TierServeRig::start_with_executor(&root, live).await;
    let before = rig.records().await;
    let mut auth = serde_json::json!({"method":"oauth","account":"work"});
    auth[hint] = serde_json::json!("preference");
    let reply = rig.call("session.send", serde_json::json!({"send_id":"conflict","prompt":"hello",
        "model":{"provider":"openai","model":"test-ultrafast"},"auth":auth})).await;
    let value = tier_reply(&reply);
    assert_eq!(value["code"], "invalid_params");
    assert_eq!(value["detail"]["field"], "auth.account");
    assert!(value["message"].as_str().unwrap().contains("pin"));
    assert_eq!(rig.records().await, before);
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

