// An `llm.complete/v1` call through the shipping session runtime: the real registry and
// executor, a provider stand-in over HTTP, and the flow scope Basal's route carries. The unit
// tests beside the tool use a stand-in runtime; this one proves the send it builds is admitted
// and run as written.

struct OneRoute(cortexkit_role_routing::select::SelectAnswer);

#[async_trait::async_trait]
impl broca_module_serve::llm_tools::complete::Select for OneRoute {
    async fn select(
        &self,
        _request: &cortexkit_role_routing::select::SelectRequest,
    ) -> Result<cortexkit_role_routing::select::SelectAnswer, broca_module_serve::account_routing::RoutingCallError>
    {
        Ok(self.0.clone())
    }
}

struct AnyProvider;

#[async_trait::async_trait]
impl broca_module_serve::llm_tools::complete::Gate for AnyProvider {
    async fn admit(
        &self,
        _scope: &broca_protocol::SendScope,
        _provider: &str,
    ) -> Result<(), Vec<u8>> {
        Ok(())
    }
}

#[tokio::test]
async fn llm_complete_runs_one_tool_free_run_through_the_session_runtime() {
    if !run_catalog_drift_child("llm_complete_runs_one_tool_free_run_through_the_session_runtime")
    {
        return;
    }
    use cortexkit_role_routing::price::{Price, RatesPerMtok};
    use cortexkit_role_routing::select::{RouteRef, RunnerRef, SelectAnswer};
    let _catalog = install_queued_ultrafast_catalog();
    let root = fresh_vault_root("llm-complete");
    let http = tier_http_fixture(vec![None], false, None).await;
    let rig = TierServeRig::start(&root, &http.url).await;
    // A completion's session has a retention policy, which the daemon's registry serves with its
    // retention service; this rig's registry has none until it is given one.
    rig.registry.set_retention_service(Arc::new(
        broca_module::retention::RetentionService::new(
            &root,
            Arc::new(broca_core::FileLeaseStore::new(root.join("leases"))),
        )
        .unwrap(),
    ));
    let route = RouteRef::new("openai", "test-ultrafast");
    let select = Arc::new(OneRoute(
        SelectAnswer::new(
            "decision-1",
            route.clone(),
            RunnerRef::new("broca", route),
            100_000,
            4_000,
            u64::MAX,
            "test",
        )
        .with_price(Price::metered(RatesPerMtok::new(3u64.into(), 15u64.into()))),
    ));
    let complete = Arc::new(
        broca_module_serve::llm_tools::complete::Complete::new(
            Arc::new(broca_store::system_one::SystemOneStore::open_in_memory().unwrap()),
            "test-owner",
            select,
            rig.registry.clone(),
            "broca",
        )
        .with_poll(Duration::from_millis(10)),
    );
    let basal = broca_protocol::BindPrincipal::Reserved {
        module_id: "basal".into(),
    };
    let scope = broca_protocol::SendScope::Stamped(broca_protocol::BindScope {
        scope: broca_protocol::SessionScope {
            owner: basal.clone(),
            agent_id_frozen: false,
            scope_ref: "flow-scope".into(),
            scope_epoch: 1,
            flow_id: Some("flow-1".into()),
            sender: None,
            agent_id: None,
            run_id: None,
        },
        owner_authorized: true,
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
    });
    let arguments = serde_json::value::to_raw_value(&serde_json::json!({
        "demand": {"iq": 50}, "prompt": "Say done.", "max_output_tokens": 64,
        "max_cost_usd": 0.01,
    }))
    .unwrap();
    let call = || complete.answer(Some(&basal), &scope, &arguments, Some("k1"), &AnyProvider);
    let answer = call().await.unwrap_or_else(|body| {
        panic!("the call was refused: {}", String::from_utf8_lossy(&body))
    });
    let result: serde_json::Value = serde_json::from_str(answer.get()).unwrap();
    assert_eq!(result["text"], "done");
    assert_eq!(
        result["model"],
        serde_json::json!({"provider": "openai", "model": "test-ultrafast"})
    );
    assert_eq!(result["usage"]["input_tokens"], 2);
    assert_eq!(result["usage"]["output_tokens"], 1);
    // 2 tokens at $3 and 1 at $15 per million.
    assert_eq!(result["cost_usd"], serde_json::json!(0.000021));
    {
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "one model call");
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert!(body.get("tools").is_none(), "no tools: {body}");
        let cap = body
            .get("max_completion_tokens")
            .or_else(|| body.get("max_tokens"));
        assert_eq!(cap, Some(&serde_json::json!(64)), "{body}");
    }
    // A repeat of the key replays the answer and sends nothing.
    let again = call().await.unwrap();
    assert_eq!(again.get(), answer.get());
    assert_eq!(http.requests.lock().unwrap().len(), 1);
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}
