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
            captured.lock().unwrap().push(CapturedResumeRequest { headers, body: bytes[split..split + length].to_vec() });
            let body = "data: {\"id\":\"resume\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"done\"},\"finish_reason\":null}]}\n\ndata: {\"id\":\"resume\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n";
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    ResumeHttpFixture { url, requests, task }
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
    // `stand-in-token` for the recorded credential id.
    let vault = CapturingVaultDaemon::start(root).await;
    let executor = executor_with_capturing_vault(root, &vault).await;
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
    let job = RunJob { req, auth: None, conversation_key: None, writer: Arc::clone(&writer), kind: RunKind::FreshOrResume,
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
        OutputEvent::Control(broca_protocol::caller::ControlUnit::Error { error }) => Some(error), _ => None,
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
            OutputEvent::Control(broca_protocol::caller::ControlUnit::Error { error }) => Some(error), _ => None,
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
