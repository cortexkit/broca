fn install_queued_ultrafast_catalog() {
    let rate = |units: u64| {
        serde_json::json!({
            "currency": "USD", "exponent": 9, "state": "priced", "units": units
        })
    };
    let mut value = serde_json::json!({
        "source": "test", "resolved_at_ms": 0, "catalog_version": 1,
        "models": {"openai/test-ultrafast": {
            "existence": "present",
            "wire.npm": {"value": "@ai-sdk/openai-compatible", "from": "provider"},
            "wire.api": {"value": "https://api.openai.com/v1", "from": "curated"},
            "limit.context": 200000, "limit.output": 64000,
            "capability.attachment": false, "capability.reasoning": false,
            "capability.tool_call": true,
            "rate.input": rate(1_000_000_000), "rate.output": rate(2_000_000_000),
            "rate.input.mode.fast": rate(2_000_000_000),
            "rate.output.mode.fast": rate(3_000_000_000),
            "rate.input.mode.ultrafast": rate(3_000_000_000),
            "rate.output.mode.ultrafast": rate(4_000_000_000)
        }}
    });
    let mut standard = value["models"]["openai/test-ultrafast"].clone();
    standard.as_object_mut().unwrap().retain(|key, _| !key.contains(".mode."));
    value["models"]["openai/test-standard-only"] = standard;
    let response = serde_json::from_value(value).unwrap();
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache
        .install_for_test(response, broca_catalog::CatalogSource::Fusiform)
        .unwrap();
    broca_catalog::install_global_catalog_cache(cache);
}

#[tokio::test]
async fn queued_ultrafast_restart_drains_through_the_real_executor_without_changing_tier() {
    use broca_core::{FileLeaseStore, WalStore as _};
    use broca_module::{DefaultOpener, LineageRegistry};
    use broca_wal::WalRecord;

    if !run_catalog_drift_child(
        "queued_ultrafast_restart_drains_through_the_real_executor_without_changing_tier",
    ) {
        return;
    }
    install_queued_ultrafast_catalog();
    let root = fresh_vault_root("queued-ultrafast-reader");
    let http = resume_http_fixture().await;
    let (mut live, subc_server) = stand_in_executor(&root).await;
    live.provider_base_url = Some(http.url.clone());
    let wal_store = live.wal_store.clone();
    let sessions = live.session_store.clone().unwrap();
    let mut req = drift_request(&root);
    req.model = broca_protocol::ModelHandle::new("openai", "test-ultrafast");
    req.input = serde_json::from_value(serde_json::json!([
        {"role":"user", "content":[{"type":"text", "text":"hello"}]}
    ]))
    .unwrap();
    let key = req.session.clone();
    let mut request = serde_json::to_value(req.to_submission()).unwrap();
    request["service_tier_level"] = serde_json::json!("ultrafast");
    // Seed a tier-bearing durable record, then discard all in-memory queue state.
    let queued: WalRecord = serde_json::from_value(serde_json::json!({
        "type":"queued", "submission_id":"sub-reader-tier", "request":request
    }))
    .unwrap();
    let mut wal = wal_store.open(&key).unwrap();
    wal.append(1, &queued, true).unwrap();
    drop(wal);

    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(root.join("leases"))),
        wal_store.clone(),
        Arc::new(live),
    )
    .with_run_index(sessions.clone())
    .with_materialized_store(sessions);
    let registry = LineageRegistry::new(Arc::new(opener));
    registry.actor_for(&key).await.unwrap();
    let frames = broca_test_support::wait_until("queued ultrafast run completes", || {
        let frames = wal_store.read_records(&key).unwrap();
        let done = frames.iter().any(|frame| {
            matches!(
                &frame.record,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ..
                }
            )
        });
        std::future::ready(if done {
            Ok(frames)
        } else {
            Err(format!("{frames:?}"))
        })
    })
    .await;
    let config = frames
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::RunStarted {
                submission_id: Some(sub),
                config,
                ..
            } if sub.0 == "sub-reader-tier" => Some(config),
            _ => None,
        })
        .expect("the reopened actor consumes the durable queued submission");
    assert_eq!(config.service_tier, None);
    assert_eq!(config.frozen_render_config["service_tier"], "ultrafast");
    {
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["service_tier"], "ultrafast");
    }
    assert!(registry.stop_and_seal().await);
    drop(registry);
    subc_server.abort();
    let _ = subc_server.await;
    drop(http);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn production_resume_retains_the_frozen_ultrafast_wire_string() {
    if !run_catalog_drift_child("production_resume_retains_the_frozen_ultrafast_wire_string") {
        return;
    }
    broca_catalog::install_global_catalog_cache(Arc::new(broca_catalog::CatalogCache::empty()));
    let server = resume_http_fixture().await;
    let root = fresh_vault_root("recorded-ultrafast-reader");
    let (summary, _, records) = execute_recorded_fixture(&root, &server, |config| {
        config.frozen_render_config["service_tier"] = "ultrafast".into();
    })
    .await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["service_tier"], "ultrafast");
    assert!(!records
        .iter()
        .any(|frame| matches!(frame.record, broca_wal::WalRecord::RunConfigRefrozen { .. })));
    std::fs::remove_dir_all(root).unwrap();
}
