// Exercise the production executor from a durable RunStarted and a WAL-loaded checkpoint.

struct CapturedResumeRequest {
    headers: String,
    body: Vec<u8>,
}

struct ResumeHttpFixture {
    url: String,
    requests: Arc<Mutex<Vec<CapturedResumeRequest>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ResumeHttpFixture {
    fn drop(&mut self) { self.task.abort(); }
}

async fn resume_http_fixture() -> ResumeHttpFixture {
    resume_http_fixture_with_body(None).await
}

async fn resume_http_fixture_with_body(script: Option<&'static str>) -> ResumeHttpFixture {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buffer = [0u8; 4096];
            let split = loop {
                let n = stream.read(&mut buffer).await.unwrap();
                assert!(n > 0, "request must contain HTTP headers");
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(split) = bytes.windows(4).position(|w| w == b"\r\n\r\n") { break split + 4; }
            };
            let headers = String::from_utf8(bytes[..split].to_vec()).unwrap();
            let length: usize = headers.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse().unwrap())
            }).unwrap();
            while bytes.len() < split + length {
                let n = stream.read(&mut buffer).await.unwrap();
                assert!(n > 0, "request must contain the complete body");
                bytes.extend_from_slice(&buffer[..n]);
            }
            let anthropic = headers.lines().next().unwrap().contains("/messages ");
            captured.lock().unwrap().push(CapturedResumeRequest { headers, body: bytes[split..split + length].to_vec() });
            let body = if let Some(script) = script {
                script
            } else if anthropic {
                "data: {\"type\":\"message_start\",\"message\":{\"id\":\"resume\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":1}}}\n\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"done\"}}\n\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\ndata: {\"type\":\"message_stop\"}\n\n"
            } else {
                "data: {\"id\":\"resume\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"done\"},\"finish_reason\":null}]}\n\ndata: {\"id\":\"resume\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n"
            };
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    ResumeHttpFixture { url, requests, task }
}

async fn execute_frozen_fallback_auth_resume(
    root: &std::path::Path,
    server: &ResumeHttpFixture,
    api_key_refrozen: bool,
    prior_call: bool,
) -> (Result<RunSummary, broca_core::RunError>, Vec<broca_wal::FramedRecord>) {
    let _ = std::fs::remove_dir_all(root);
    std::fs::create_dir_all(root).unwrap();
    let vault = CapturingVaultDaemon::start(root).await;
    let executor = executor_with_capturing_vault(root, &vault).await;
    let mut req = drift_request(root);
    req.model.model_id = "claude-opus-5-5".into();
    let mut frozen = broca_provider::families::anthropic_frozen_config("claude-opus-5-5");
    frozen.server_side_fallback = true;
    frozen.renderer_version = broca_provider::families::anthropic_renderer_version(true);
    frozen.base_url = Some(server.url.clone());
    let mut fallback = frozen.clone();
    fallback.wire_model_id = "claude-opus-4-8".into();
    frozen.refusal_fallback = Some(Box::new(broca_provider::wire::RefusalFallbackConfig {
        model: broca_protocol::ModelHandle::new("anthropic", "claude-opus-4-8"),
        context_limit: Some(200_000), frozen: fallback,
    }));
    let store = Arc::clone(&executor.wal_store);
    let mut wal = store.open(&req.session).unwrap();
    let mut record = run_started_with_user_text(&req.session, &req.run_id.0, "");
    if let broca_wal::WalRecord::RunStarted { config, input, .. } = &mut record {
        config.model = req.model.clone();
        config.renderer_version = frozen.renderer_version;
        config.frozen_render_config = serde_json::to_value(&frozen).unwrap();
        config.auth_selection = Some(serde_json::from_value(serde_json::json!({
            "credential_id":"oauth:anthropic", "auth_method":"oauth"
        })).unwrap());
        *input = req.input.clone().into();
    } else { unreachable!() }
    wal.append(1, &record, true).unwrap();
    if prior_call {
        wal.append(1, &serde_json::from_value(serde_json::json!({"type":"step_started", "step_id":1})).unwrap(), true).unwrap();
    }
    if api_key_refrozen {
        wal.append(1, &broca_wal::WalRecord::RunAuthRefrozen { ts_ms: None,
            auth_selection: serde_json::from_value(serde_json::json!({
                "credential_id":"apikey:anthropic", "auth_method":"apikey"
            })).unwrap(),
        }, true).unwrap();
    }
    drop(wal);
    let writer = Arc::new(broca_core::WalHandle::spawn(store.open(&req.session).unwrap(), 1).unwrap());
    let prior_snapshot = broca_core::load_prior_wal_snapshot(store.as_ref(), &req.session,
        writer.durable_head().load(Ordering::Acquire), None::<&broca_store::SqliteStore>).unwrap();
    let session = req.session.clone();
    let job = RunJob { queued_service_tier: None, media: Vec::new(), media_profile: None, req, auth: None, conversation_key: None,
        writer: Arc::clone(&writer), kind: RunKind::FreshOrResume, skip_materialization: true,
        control: Default::default(), prior_snapshot, on_admitted: None };
    let (out, mut events) = mpsc::channel(128);
    let result = tokio::time::timeout(Duration::from_secs(15), executor.execute(job, out)).await
        .expect("fallback auth resume terminates");
    while events.recv().await.is_some() {}
    Arc::try_unwrap(writer).unwrap_or_else(|_| panic!("executor released the writer")).shutdown().await.unwrap();
    let records = store.read_records(&session).unwrap();
    vault.assert_healthy();
    drop(executor);
    drop(vault);
    (result, records)
}

#[tokio::test]
async fn production_resume_keeps_frozen_oauth_fallbacks_or_refuses_api_key_by_name() {
    if !run_catalog_drift_child("production_resume_keeps_frozen_oauth_fallbacks_or_refuses_api_key_by_name") { return; }
    broca_catalog::install_global_catalog_cache(Arc::new(broca_catalog::CatalogCache::empty()));
    for prior_call in [false, true] {
        let server = resume_http_fixture().await;
        let root = broca_test_support::scratch_root().join(format!("frozen-fallback-api-key-{prior_call}-{}", std::process::id()));
        let (result, records) = execute_frozen_fallback_auth_resume(&root, &server, true, prior_call).await;
        let error = result.expect_err("API-key auth cannot honor frozen OAuth fallback settings");
        assert!(error.to_string().contains("frozen_fallback_auth_mismatch"), "{error}");
        assert!(server.requests.lock().unwrap().is_empty());
        assert!(!records.iter().any(|record| matches!(record.record, broca_wal::WalRecord::RunConfigRefrozen { .. })));
        let _ = std::fs::remove_dir_all(root);
    }
    let server = resume_http_fixture().await;
    let root = broca_test_support::scratch_root().join(format!("frozen-fallback-oauth-{}", std::process::id()));
    let (result, records) = execute_frozen_fallback_auth_resume(&root, &server, false, true).await;
    assert_eq!(result.unwrap().reason, RunFinishReason::Completed);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let headers = requests[0].headers.to_ascii_lowercase();
    assert!(headers.contains("authorization: bearer stand-in-token"));
    assert!(!headers.contains("x-api-key:"));
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["fallbacks"], "default");
    assert!(!records.iter().any(|record| matches!(record.record, broca_wal::WalRecord::RunConfigRefrozen { .. })));
    let _ = std::fs::remove_dir_all(root);
}

/// Resolve the original opaque admission, persist its actual blob, load the real WAL
/// checkpoint and invoke LiveLoopExecutor::execute. No restore arguments are reconstructed.
async fn execute_recorded_fixture(
    root: &std::path::Path,
    server: &ResumeHttpFixture,
    alter: impl FnOnce(&mut broca_wal::RunConfig),
) -> (RunSummary, Vec<OutputEvent>, Vec<broca_wal::FramedRecord>) {
    let _ = std::fs::remove_dir_all(root);
    std::fs::create_dir_all(root).unwrap();
    // The resume reads its credential from the vault, as production does; the stand-in serves
    // `stand-in-token` for the recorded credential id. Include `apikey:anthropic` in the
    // vault listing so the resume reaches the check for its recorded renderer.
    let vault = CapturingVaultDaemon::start(root).await;
    let executor = executor_with_vault_addressing(root, &vault, Duration::from_secs(2),
        listed_credentials([("apikey:anthropic", "apikey:anthropic")]), supervised_broca_identity()).await;
    let mut req = drift_request(root);
    req.model.model_id = "opaque-resume".into();
    req.generation.max_output_tokens = Some(2048);
    let started = broca_catalog::live::resolve_provider(
        "anthropic/opaque-resume", Some(&server.url), 2048,
        &AuthMode::Secret("admission-fixture".into()),
        &broca_catalog::live::ResolveOptions { auth_method: Some(AuthMethod::ApiKey), ..Default::default() },
    ).unwrap_or_else(|error| panic!("opaque admission: {error}"));
    assert_eq!(started.family_id, WireFamilyId::OpenAiChat);
    let mut config = drift_run_config(&req, &started);
    config.auth_selection = Some(serde_json::from_value(serde_json::json!({"credential_id":"apikey:anthropic"})).unwrap());
    alter(&mut config);
    let store = Arc::clone(&executor.wal_store);
    let mut wal = store.open(&req.session).unwrap();
    let mut record = run_started_with_user_text(&req.session, &req.run_id.0, "");
    if let broca_wal::WalRecord::RunStarted { config: recorded, input, .. } = &mut record {
        **recorded = config;
        *input = req.input.clone().into();
    } else { unreachable!(); }
    wal.append(1, &record, true).unwrap();
    drop(wal);
    let writer = Arc::new(broca_core::WalHandle::spawn(store.open(&req.session).unwrap(), 1).unwrap());
    let prior_snapshot = broca_core::load_prior_wal_snapshot(store.as_ref(), &req.session, writer.durable_head().load(Ordering::Acquire), None::<&broca_store::SqliteStore>).unwrap();
    let session = req.session.clone();
    let job = RunJob { queued_service_tier: None, media: Vec::new(), media_profile: None, req, auth: None, conversation_key: None, writer: Arc::clone(&writer), kind: RunKind::FreshOrResume,
        skip_materialization: true, control: Default::default(), prior_snapshot, on_admitted: None };
    let (out, mut events) = mpsc::channel(128);
    let outcome = tokio::time::timeout(Duration::from_secs(15), executor.execute(job, out)).await;
    Arc::try_unwrap(writer).unwrap_or_else(|_| panic!("executor released the session writer")).shutdown().await.unwrap();
    drop(vault);
    let summary = outcome.expect("executor resume must terminate").expect("resume must reach core's typed guard");
    let mut emitted = Vec::new();
    while let Some(event) = events.recv().await { emitted.push(event); }
    let records = store.read_records(&session).unwrap();
    assert_eq!(records.iter().filter(|r| matches!(r.record, broca_wal::WalRecord::RunStarted { .. })).count(), 1,
        "resume must use the existing RunStarted, not admit a replacement");
    (summary, emitted, records)
}

#[tokio::test]
async fn production_executor_restores_opaque_bearer_auth_from_the_recorded_route() {
    if !run_catalog_drift_child("production_executor_restores_opaque_bearer_auth_from_the_recorded_route") { return; }
    broca_catalog::install_global_catalog_cache(Arc::new(broca_catalog::CatalogCache::empty()));
    let server = resume_http_fixture().await;
    let root = broca_test_support::scratch_root().join(format!("executor-opaque-resume-{}", std::process::id()));
    let (summary, _, records) = execute_recorded_fixture(&root, &server, |_| {}).await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "the resumed run must reach its recorded endpoint");
    let headers = requests[0].headers.to_ascii_lowercase();
    assert!(headers.contains("authorization: bearer stand-in-token"), "opaque resume must preserve Bearer auth: {headers}");
    assert!(!headers.contains("x-api-key:"), "model prefix must not select Anthropic auth: {headers}");
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["model"], "anthropic/opaque-resume");
    assert!(records.iter().any(|r| matches!(r.record, broca_wal::WalRecord::StepStarted { .. })));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn production_executor_refuses_unreproducible_renderer_before_any_model_call() {
    if !run_catalog_drift_child("production_executor_refuses_unreproducible_renderer_before_any_model_call") { return; }
    let server = resume_http_fixture().await;
    let root = broca_test_support::scratch_root().join(format!("executor-unreproducible-resume-{}", std::process::id()));
    assert!(!broca_provider::families::reproduces_renderer_version(WireFamilyId::OpenAiChat, 1));
    let (summary, events, records) = execute_recorded_fixture(&root, &server, |config| {
        config.renderer_version = 1;
        config.frozen_render_config["renderer_version"] = 1.into();
    }).await;
    assert_eq!(summary.reason, RunFinishReason::Interrupted, "unreproducible renderer must refuse resume");
    let errors: Vec<_> = events.iter().filter_map(|event| match event {
        OutputEvent::Control(broca_protocol::caller::ControlUnit::Error { error, .. }) => Some(error), _ => None,
    }).collect();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].provider_code.as_deref(), Some("renderer_version_drift"));
    assert_eq!(errors[0].class, ErrorClass::Permanent);
    assert!(records.iter().any(|r| matches!(&r.record, broca_wal::WalRecord::RunFinished { error: Some(error), .. }
        if error.provider_code.as_deref() == Some("renderer_version_drift") && error.class == ErrorClass::Permanent)),
        "the status projection must retain the same permanent renderer refusal");
    assert!(server.requests.lock().unwrap().is_empty(), "unreproducible renderer must make zero provider calls");
    assert!(!records.iter().any(|r| matches!(r.record, broca_wal::WalRecord::StepStarted { .. })));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn production_executor_preserves_unverifiable_blob_code_and_class() {
    if !run_catalog_drift_child("production_executor_preserves_unverifiable_blob_code_and_class") { return; }
    let server = resume_http_fixture().await;
    for (label, blob) in [("null", serde_json::Value::Null), ("undecodable", serde_json::json!({"codeassist_identity":{"version":"invalid"}}))] {
        let root = broca_test_support::scratch_root().join(format!("executor-{label}-resume-{}", std::process::id()));
        let (summary, events, records) = execute_recorded_fixture(&root, &server, |config| { config.frozen_render_config = blob; }).await;
        assert_eq!(summary.reason, RunFinishReason::Interrupted, "{label}");
        let errors: Vec<_> = events.iter().filter_map(|event| match event {
            OutputEvent::Control(broca_protocol::caller::ControlUnit::Error { error, .. }) => Some(error), _ => None,
        }).collect();
        assert_eq!(errors.len(), 1, "{label}");
        assert_eq!(errors[0].provider_code.as_deref(), Some("frozen_render_config_unverifiable"), "{label}");
        assert_eq!(errors[0].class, ErrorClass::Permanent, "{label}");
        assert!(records.iter().any(|r| matches!(&r.record, broca_wal::WalRecord::RunFinished { error: Some(error), .. }
            if error.provider_code.as_deref() == Some("frozen_render_config_unverifiable") && error.class == ErrorClass::Permanent)),
            "the status projection must retain the same permanent unverifiable refusal");
        assert!(server.requests.lock().unwrap().is_empty(), "an unverifiable bundle must make zero provider calls");
        assert!(!records.iter().any(|r| matches!(r.record, broca_wal::WalRecord::StepStarted { .. })));
        let _ = std::fs::remove_dir_all(root);
    }
}

/// Admit and run one fresh episode of `claude-haiku-4-5` under `provider` through the
/// production executor, its vault (`listing`) and an HTTP stand-in for Anthropic's host.
/// Returns the request the stand-in received and the session's records.
async fn borrowed_rows_run(
    label: &str,
    provider: &str,
    listing: Arc<broca_module_serve::account_routing::ListingCache>,
) -> (CapturedResumeRequest, Vec<broca_wal::FramedRecord>) {
    let root = fresh_vault_root(label);
    let server = resume_http_fixture().await;
    let vault = CapturingVaultDaemon::start(&root).await;
    let executor = executor_with_vault_addressing(&root, &vault, Duration::from_secs(2), listing,
        supervised_broca_identity()).await
        .with_provider_overrides(broca_module_serve::provider_overrides::ProviderOverrides::parse(
            &serde_json::json!({"providers": {"anthropic": {"base_url": server.url}}}).to_string(),
        ).unwrap());
    let mut req = drift_request(&root);
    req.run_id = broca_protocol::RunId("borrowed".into());
    req.model = broca_protocol::request::ModelHandle::new(provider, "claude-haiku-4-5");
    req.generation.max_output_tokens = Some(1024);
    req.input = vec![broca_protocol::Message::new(broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text { text: "hello".into() }])];
    let store = Arc::clone(&executor.wal_store);
    let writer = Arc::new(broca_core::WalHandle::spawn(store.open(&req.session).unwrap(), 0).unwrap());
    let prior_snapshot = broca_core::load_prior_wal_snapshot(store.as_ref(), &req.session,
        writer.durable_head().load(Ordering::Acquire), None::<&broca_store::SqliteStore>).unwrap();
    let session = req.session.clone();
    let job = RunJob { queued_service_tier: None, media: Vec::new(), media_profile: None, req,
        auth: Some(broca_protocol::AuthRequest { method: "apikey".into(), account: None, pool: None,
            credential_id: None }),
        conversation_key: None, writer: Arc::clone(&writer), kind: RunKind::FreshOrResume,
        skip_materialization: true, control: Default::default(), prior_snapshot, on_admitted: None };
    let (out, mut events) = mpsc::channel(128);
    let summary = tokio::time::timeout(Duration::from_secs(15), executor.execute(job, out)).await
        .expect("the run must end");
    Arc::try_unwrap(writer).unwrap_or_else(|_| panic!("executor released the session writer"))
        .shutdown().await.unwrap();
    vault.assert_healthy();
    let summary = summary.unwrap_or_else(|error| panic!("{provider}: {error:?}"));
    assert_eq!(summary.reason, RunFinishReason::Completed, "{provider}");
    while events.recv().await.is_some() {}
    let records = store.read_records(&session).unwrap();
    let request = server.requests.lock().unwrap().pop().expect("one model call");
    drop(executor);
    let _ = std::fs::remove_dir_all(root);
    (request, records)
}

/// A paid Anthropic key filed under `anthropic-platform`, a provider the catalog lacks, runs on
/// Anthropic's rows: the provider receives the same request a plain `anthropic` key run sends,
/// with the key in `x-api-key` and no Claude Code persona, the run freezes the borrowed provider
/// and its spend names `anthropic` with the pool key that paid.
#[tokio::test]
async fn a_pool_key_run_borrows_anthropic_rows_and_renders_the_same_request() {
    // It replaces the process-wide catalog, so it runs in a child process of its own.
    if !run_catalog_drift_child("a_pool_key_run_borrows_anthropic_rows_and_renders_the_same_request") {
        return;
    }
    install_test_catalog();
    let pool = {
        let cache = Arc::new(broca_module_serve::account_routing::ListingCache::default());
        cache.store(broca_subc::vault::ScopedList { credentials: vec![serde_json::from_value(
            serde_json::json!({"id": "apikey:anthropic:acct1", "type": "apikey", "categories": ["llm"],
                "serves": ["anthropic"], "state": "active", "record_version": 1,
                "operations": ["read"], "auth_method": "apikey",
                "provider_ids": ["anthropic-platform"]})).unwrap()],
            grants: 1, grant_tuples: vec![], view: "fixture".into() });
        cache
    };
    let (borrowed, records) = borrowed_rows_run("pool-borrowed", "anthropic-platform", pool).await;
    let (plain, _) = borrowed_rows_run("pool-plain", "anthropic",
        listed_credentials([("apikey:anthropic", "apikey:anthropic")])).await;

    assert_eq!(borrowed.body, plain.body, "the borrowed run renders Anthropic's request");
    let body: serde_json::Value = serde_json::from_slice(&borrowed.body).unwrap();
    assert_eq!(body["model"], "claude-haiku-4-5");
    let headers = borrowed.headers.to_ascii_lowercase();
    assert!(borrowed.headers.lines().next().unwrap().contains("/messages "), "{headers}");
    assert!(headers.contains("x-api-key:"), "a paid key goes in x-api-key");
    assert!(!headers.contains("authorization:"), "and never as a bearer token");
    assert!(!String::from_utf8_lossy(&borrowed.body).contains("Claude Code"),
        "a paid key run carries no Claude Code persona");

    let config = records.iter().find_map(|record| match &record.record {
        broca_wal::WalRecord::RunStarted { config, .. } => Some(config.as_ref().clone()),
        _ => None,
    }).expect("the run started");
    assert_eq!(config.model.provider_module_id, "anthropic-platform", "the caller's provider is kept");
    assert_eq!(config.served_provider.as_deref(), Some("anthropic"));
    let selection = config.auth_selection.as_ref().expect("a vault run freezes its selection");
    assert_eq!(selection.credential_id, "apikey:anthropic-platform", "keyed on the caller's route");
    assert_eq!(selection.resolved_credential_id.as_deref(), Some("apikey:anthropic:acct1"),
        "and paid by the pool key the vault lists");
    let segments = broca_core::billing::compute_segments(&records, "instance",
        &broca_module_serve::ChargeBasisTable::shipped_defaults());
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].provider, "anthropic");
    assert_eq!(segments[0].charge_basis, broca_core::ChargeBasis::MeteredApi);
    // Reader samples, written only when asked: the pool run's usage fact segment and its frozen
    // run config, through the serializers the export and the run log use.
    if let Some(dir) = std::env::var_os("BROCA_DESIGN32_SAMPLES") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("pool-run-export-fact-segment.json"),
            serde_json::to_vec_pretty(&segments[0]).unwrap()).unwrap();
        std::fs::write(dir.join("pool-run-frozen-run-config.json"),
            serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        std::fs::write(dir.join("pool-run-request-body.json"), &borrowed.body).unwrap();
        let steps: Vec<_> = records.iter().filter(|record| matches!(record.record,
            broca_wal::WalRecord::ModelStepFinished { .. } | broca_wal::WalRecord::RunFinished { .. }))
            .map(|record| &record.record).collect();
        std::fs::write(dir.join("pool-run-step-and-finish-records.json"),
            serde_json::to_vec_pretty(&steps).unwrap()).unwrap();
    }
}
