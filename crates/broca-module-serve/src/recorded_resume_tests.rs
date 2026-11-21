// An HTTP stand-in for a model provider's host, and runs on the paid-key pool through the
// production executor.

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

/// Admit and run one fresh run of `claude-haiku-4-5` under `provider` through the production
/// executor, on a session in the new storage, with its vault (`listing`) and an HTTP stand-in
/// for Anthropic's host. Returns the request the stand-in received and the config the run froze.
async fn borrowed_rows_run(
    label: &str,
    provider: &str,
    listing: Arc<broca_module_serve::account_routing::ListingCache>,
) -> (CapturedResumeRequest, broca_core::RunConfig) {
    borrowed_rows_run_with(label, provider, listing, None).await
}

/// [`borrowed_rows_run`] with the reasoning variant the send names.
async fn borrowed_rows_run_with(
    label: &str,
    provider: &str,
    listing: Arc<broca_module_serve::account_routing::ListingCache>,
    variant: Option<&str>,
) -> (CapturedResumeRequest, broca_core::RunConfig) {
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
    req.variant = variant.map(str::to_owned);
    // A variant's thinking budget needs an output cap above it.
    req.generation.max_output_tokens = Some(if variant.is_some() { 32_000 } else { 1024 });
    req.input = vec![broca_protocol::Message::new(broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text { text: "hello".into() }])];
    let mut session = store_session().await;
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    // The send names the API-key method, as a pool route's caller does.
    let auth = broca_protocol::AuthRequest { method: "apikey".into(), account: None, pool: None,
        credential_id: None };
    let end = serve_store_run_with_control(&executor, &mut session, &parts, req,
        broca_module::executor::StoreRunMode::Fresh, Default::default(), Some(auth)).await;
    vault.assert_healthy();
    match end.result {
        Ok(finished) => assert_eq!(finished.reason, RunFinishReason::Completed, "{provider}"),
        Err(error) => panic!("{provider}: {error:?}"),
    }
    let (writer, _link) = broca_session_loop::shared::ActorWriter::new(&session, None);
    let config = store_history(&writer, &parts).await.unwrap().prior_run
        .expect("the run started").config;
    let request = server.requests.lock().unwrap().pop().expect("one model call");
    drop(executor);
    let _ = std::fs::remove_dir_all(root);
    (request, config)
}

/// A paid Anthropic key filed under `anthropic-platform`, a provider the catalog lacks, runs on
/// Anthropic's rows: the provider receives the same request a plain `anthropic` key run sends,
/// with the key in `x-api-key` and no Claude Code persona, the run freezes the borrowed provider
/// and the pool key that paid, and its spend names `anthropic`.
#[tokio::test]
async fn a_pool_key_run_borrows_anthropic_rows_and_renders_the_same_request() {
    // It replaces the process-wide catalog, so it runs in a child process of its own.
    if !run_catalog_drift_child("a_pool_key_run_borrows_anthropic_rows_and_renders_the_same_request") {
        return;
    }
    let _catalog = install_test_catalog();
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
    let (borrowed, config) = borrowed_rows_run("pool-borrowed", "anthropic-platform", pool).await;
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

    assert_eq!(config.model.provider_module_id, "anthropic-platform", "the caller's provider is kept");
    assert_eq!(config.served_provider.as_deref(), Some("anthropic"));
    let selection = config.auth_selection.as_ref().expect("a vault run freezes its selection");
    assert_eq!(selection.credential_id, "apikey:anthropic-platform", "keyed on the caller's route");
    assert_eq!(selection.resolved_credential_id.as_deref(), Some("apikey:anthropic:acct1"),
        "and paid by the pool key the vault lists");
    assert_eq!(config.billed_provider(), "anthropic", "its spend names the rows it borrowed");
}

/// A send on the paid-key pool route that names a reasoning variant sends it: the borrowed rows'
/// reasoning options resolve it as they do for the same send on `anthropic`.
#[tokio::test]
async fn a_pool_key_run_sends_the_variant_it_was_asked_for() {
    if !run_catalog_drift_child("a_pool_key_run_sends_the_variant_it_was_asked_for") {
        return;
    }
    let _catalog = install_test_catalog();
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
    let (borrowed, _) =
        borrowed_rows_run_with("pool-variant", "anthropic-platform", pool, Some("high")).await;
    let (plain, _) = borrowed_rows_run_with("plain-variant", "anthropic",
        listed_credentials([("apikey:anthropic", "apikey:anthropic")]), Some("high")).await;
    let body: serde_json::Value = serde_json::from_slice(&borrowed.body).unwrap();
    let plain_body: serde_json::Value = serde_json::from_slice(&plain.body).unwrap();
    assert!(plain_body["thinking"].is_object(), "the plain run sends the variant: {plain_body}");
    assert_eq!(body["thinking"], plain_body["thinking"]);
    assert_eq!(body["output_config"], plain_body["output_config"]);
}

/// A company AI gateway key, filed under the gateway's own provider id: the vault serves it with
/// the gateway's address, the header and scheme that carry it, and fixed headers.
#[tokio::test]
async fn a_gateway_key_run_goes_to_its_gateway_with_the_gateways_headers() {
    // It replaces the process-wide catalog, so it runs in a child process of its own.
    if !run_catalog_drift_child("a_gateway_key_run_goes_to_its_gateway_with_the_gateways_headers")
    {
        return;
    }
    let _catalog = install_test_catalog();
    let root = fresh_vault_root("gateway-key");
    let server = resume_http_fixture().await;
    // The stand-in answers under `/v1`; the gateway's root stands in for Anthropic's, so the
    // request path `/v1/messages` lands below it.
    let gateway_root = server.url.trim_end_matches("/v1").to_owned();
    let vault = CapturingVaultDaemon::start(&root).await;
    vault.record.lock().unwrap().credential_replies.insert(
        "apikey:anthropic-gw-acme".into(),
        std::collections::VecDeque::from([serde_json::json!({"result": {
            "payload": "gateway-key".as_bytes(), "expires_at_ms": null, "record_version": 1,
            "credential_id": "apikey:anthropic-gw-acme", "hosts": ["127.0.0.1"],
            "base_url": gateway_root, "auth": {"header": "authorization", "scheme": "bearer"},
            "headers": {"x-team": "engineering"}, "invalid_key": null,
        }})]),
    );
    let listing = Arc::new(broca_module_serve::account_routing::ListingCache::default());
    listing.store(broca_subc::vault::ScopedList {
        credentials: vec![serde_json::from_value(serde_json::json!({
            "id": "apikey:anthropic-gw-acme", "type": "apikey", "categories": ["llm-gateway"],
            "serves": ["anthropic"], "state": "active", "record_version": 1,
            "operations": ["read", "list"], "auth_method": "gateway",
            "provider_ids": ["anthropic-gw-acme"]}))
        .unwrap()],
        grants: 1,
        grant_tuples: vec![],
        view: "fixture".into(),
    });
    // No provider override: the gateway's address comes from its credential alone.
    let executor = executor_with_vault_addressing(
        &root,
        &vault,
        Duration::from_secs(2),
        listing,
        supervised_broca_identity(),
    )
    .await;
    let mut req = drift_request(&root);
    req.run_id = broca_protocol::RunId("gateway".into());
    req.model = broca_protocol::request::ModelHandle::new("anthropic-gw-acme", "claude-haiku-4-5");
    req.generation.max_output_tokens = Some(1024);
    req.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text { text: "hello".into() }],
    )];
    let mut session = store_session().await;
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    let auth = broca_protocol::AuthRequest {
        method: "gateway".into(),
        account: None,
        pool: None,
        credential_id: None,
    };
    let end = serve_store_run_with_control(
        &executor,
        &mut session,
        &parts,
        req,
        broca_module::executor::StoreRunMode::Fresh,
        Default::default(),
        Some(auth),
    )
    .await;
    vault.assert_healthy();
    match end.result {
        Ok(finished) => assert_eq!(finished.reason, RunFinishReason::Completed),
        Err(error) => panic!("{error:?}"),
    }
    let (writer, _link) = broca_session_loop::shared::ActorWriter::new(&session, None);
    let config = store_history(&writer, &parts)
        .await
        .unwrap()
        .prior_run
        .expect("the run started")
        .config;
    let gateway = server.requests.lock().unwrap().pop().expect("one model call");
    let (records, _) = broca_session_loop::billing::run_billing_inputs(
        &session,
        "gateway",
        &broca_protocol::BindIdentity::new(&root, "test", "session"),
        &parts,
    )
    .await
    .unwrap();
    let segments = broca_core::billing::compute_segments_for_run(
        &records,
        &broca_protocol::RunId("gateway".into()),
        "instance",
        &broca_module_serve::ChargeBasisTable::shipped_defaults(),
    );
    drop(executor);
    let _ = std::fs::remove_dir_all(root);
    let (plain, _) = borrowed_rows_run(
        "gateway-plain",
        "anthropic",
        listed_credentials([("apikey:anthropic", "apikey:anthropic")]),
    )
    .await;

    let request_line = gateway.headers.lines().next().unwrap().to_owned();
    assert!(request_line.contains(" /v1/messages "), "{request_line}");
    let headers = gateway.headers.to_ascii_lowercase();
    assert!(headers.contains("authorization: bearer gateway-key"), "the key in the gateway's header");
    assert!(headers.contains("x-team: engineering"), "with the gateway's fixed headers");
    assert!(!headers.contains("x-api-key:"), "and never in Anthropic's own header");
    assert_eq!(gateway.body, plain.body, "a gateway run renders a paid key's request");
    assert!(!String::from_utf8_lossy(&gateway.body).contains("Claude Code"));

    assert_eq!(config.model.provider_module_id, "anthropic-gw-acme");
    assert_eq!(config.served_provider.as_deref(), Some("anthropic"));
    assert_eq!(
        config.frozen_render_config["base_url"],
        serde_json::json!(gateway_root),
        "the run freezes the gateway's address, so a resume sends there"
    );
    let selection = config.auth_selection.as_ref().expect("a vault run freezes its selection");
    assert_eq!(selection.resolved_credential_id.as_deref(), Some("apikey:anthropic-gw-acme"));
    assert_eq!(config.billed_provider(), "anthropic", "its spend names the rows it borrowed");
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].provider, "anthropic", "the provider the spend is priced as");
    assert_eq!(
        segments[0].charge_basis,
        broca_core::ChargeBasis::MeteredApi,
        "priced at Anthropic's rates"
    );
    // Sample files for the modules that read these bytes, written only when asked: the
    // gateway run's spend segment (the usage export's `segment_json`), its frozen run config
    // and the request the gateway received, through the serializers the export and the session
    // store use. The key is synthetic.
    if let Some(dir) = std::env::var_os("BROCA_GATEWAY_SAMPLES") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        let write = |name: &str, bytes: &[u8]| std::fs::write(dir.join(name), bytes).unwrap();
        write(
            "gateway-run-export-fact-segment.json",
            &serde_json::to_vec_pretty(&segments[0]).unwrap(),
        );
        write(
            "gateway-run-frozen-run-config.json",
            &serde_json::to_vec_pretty(&config).unwrap(),
        );
        write("gateway-run-request-body.json", &gateway.body);
        write("gateway-run-request-headers.txt", gateway.headers.as_bytes());
    }
}
