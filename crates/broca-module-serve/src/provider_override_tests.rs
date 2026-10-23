// Included in executor::tests to call the same provider-resolution code used in production.
fn runtime_overrides(host: &str) -> broca_module_serve::provider_overrides::ProviderOverrides {
    broca_module_serve::provider_overrides::ProviderOverrides::parse(
        &serde_json::json!({"providers":{"deepseek":{"base_url":host},"anthropic":{"base_url":host}}}).to_string(),
    ).unwrap()
}

fn resolve_runtime_override(
    overrides: &broca_module_serve::provider_overrides::ProviderOverrides,
    frozen: Option<&serde_json::Value>,
) -> broca_catalog::live::ProviderResolution {
    install_test_catalog();
    let resolution = AuthResolution {
        mode: AuthMode::Secret("test-api-key".into()),
        pause: None,
        selection: Some(broca_core::AuthSelection {
            credential_id: "apikey:deepseek".into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        }),
        method: Some(AuthMethod::ApiKey),
        project_id: None,
        account_id: None,
        record_version: None,
        in_run_refresh: None,
    };
    resolve_provider_for_episode(
        "deepseek/deepseek-v4-flash",
        None,
        Some(2048),
        &resolution,
        &broca_catalog::live::ResolveOptions::default(),
        broca_provider::build_provider_http_client(),
        broca_catalog::live::EpisodeRenderConfig {
            codeassist_identity: None,
            claude_code_persona: Default::default(),
            base_url: match frozen {
                Some(recorded) => recorded.get("base_url").and_then(serde_json::Value::as_str),
                None => overrides.base_url("deepseek"),
            },
            max_output_tokens: frozen
                .and_then(|recorded| recorded["max_output_tokens"].as_u64())
                .map(|cap| cap as u32),
            variant: None,
            service_tier: None,
            thinking_echo_scope: None,
            thinking_cross_model: None,
            responses_tools_non_strict: None,
            chatgpt_effort_updates: None,
            chatgpt_wire: None,
            // As the executor restores it: a recorded blob without the field reads as false.
            reasoning_content_required: frozen.map(|recorded| {
                recorded
                    .get("reasoning_content_required")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
            }),
            reasoning_echo: frozen.map(|recorded| recorded.get("reasoning_echo").and_then(|v| serde_json::from_value(v.clone()).ok())),
            provider_text_state: None,
            structured_output: None,
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| panic!("runtime resolution: {error}"))
}

#[test]
fn runtime_override_moves_only_host() {
    let baseline = resolve_runtime_override(&Default::default(), None);
    let overridden = resolve_runtime_override(&runtime_overrides("http://127.0.0.1:4010/v1"), None);
    let mut actual = overridden.provider.render_config_blob();
    let expected = baseline.provider.render_config_blob();
    assert_ne!(actual["base_url"], expected["base_url"]);
    assert_eq!(actual["base_url"], "http://127.0.0.1:4010/v1");
    actual["base_url"] = expected["base_url"].clone();
    assert_eq!(
        serde_json::to_vec(&actual).unwrap(),
        serde_json::to_vec(&expected).unwrap()
    );
    assert_eq!(overridden.provider_tag, "deepseek");
    assert_eq!(overridden.wire_model, "deepseek-v4-flash");
    assert_eq!(overridden.auth_desc, baseline.auth_desc);
    assert_eq!(overridden.context_limit, baseline.context_limit);
}

/// Resolve the provider for `auth` the way an episode does, send one step to a local listener,
/// and return the `authorization` header it carried. That header is the only observable place
/// the resolved secret lands, so it is what shows which auth file the credential came from.
/// `None` when resolution fails (no usable credential) or no request arrives.
async fn read_opencode_authorization(auth: &AuthResolution) -> Option<String> {
    use tokio::io::AsyncReadExt;
    install_test_catalog();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let resolved = resolve_provider_for_episode(
        "deepseek/deepseek-v4-flash",
        None,
        Some(64),
        auth,
        &broca_catalog::live::ResolveOptions {
            auth_method: auth.method,
            ..Default::default()
        },
        broca_provider::build_provider_http_client(),
        broca_catalog::live::EpisodeRenderConfig {
            codeassist_identity: None,
            claude_code_persona: Default::default(),
            base_url: Some(&base_url),
            max_output_tokens: None,
            variant: None,
            service_tier: None,
            thinking_echo_scope: None,
            thinking_cross_model: None,
            responses_tools_non_strict: None,
            chatgpt_effort_updates: None,
            chatgpt_wire: None,
            reasoning_content_required: None,
            reasoning_echo: None,
            provider_text_state: None,
            structured_output: None,
            ..Default::default()
        },
    )
    .ok()?;
    // Accept on its own task so the capture does not depend on whether `step` sends before or
    // after it returns its event receiver.
    let capture = tokio::spawn(tokio::time::timeout(Duration::from_secs(10), async move {
        let (mut stream, _) = listener.accept().await.ok()?;
        let mut head = Vec::new();
        let mut chunk = [0u8; 4096];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).await.ok()?;
            if read == 0 {
                break;
            }
            head.extend_from_slice(&chunk[..read]);
        }
        Some(String::from_utf8_lossy(&head).into_owned())
    }));
    let events = resolved
        .provider
        .step(broca_protocol::StepId(1), &broca_protocol::CallOptions::new(vec![]))
        .await;
    let head = capture.await.ok()?.ok()??;
    drop(events);
    head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("authorization")
            .then(|| value.trim().to_string())
    })
}

#[test]
fn runtime_override_resume_keeps_frozen_host_and_bytes() {
    let started = resolve_runtime_override(&runtime_overrides("http://host-a.example/v1"), None);
    let frozen = started.provider.render_config_blob();
    let resumed = resolve_runtime_override(
        &runtime_overrides("http://host-b.example/v1"),
        Some(&frozen),
    );
    assert_eq!(resumed.base_url, "http://host-a.example/v1");
    assert_eq!(
        serde_json::to_vec(&resumed.provider.render_config_blob()).unwrap(),
        serde_json::to_vec(&frozen).unwrap()
    );
    let call = broca_protocol::CallOptions::new(vec![]);
    assert_eq!(
        resumed.provider.render_request(&call),
        started.provider.render_request(&call)
    );
}

#[tokio::test]
async fn runtime_override_preserves_auth_billing_and_cache_predicates() {
    // Only the tool-plane connection is stubbed. Auth and cache decisions below are the
    // executor's real methods; no daemon, vault process or real provider is needed.
    use subc_transport::connection_file::{ConnectionInfo, Endpoint, SCHEMA_VERSION};
    let root = broca_test_support::scratch_root().join(format!("broca-host-override-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    // With no vault the executor takes the OpenCode credential source. Point it at a fixture:
    // otherwise it reads the operator's real ~/.local/share/opencode/auth.json. The key is
    // deliberately one no real auth file holds, so finding it on the wire below proves the
    // credential came from this file and not from the host.
    const FIXTURE_KEY: &str = "broca-test-fixture-deepseek-key-not-a-real-credential";
    let opencode_auth = root.join("opencode-auth.json");
    std::fs::write(
        &opencode_auth,
        serde_json::json!({"deepseek": {"type": "api", "key": FIXTURE_KEY}}).to_string(),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let key = vec![0x5c; 32];
    let connection_file = root.join("connection.json");
    subc_transport::connection_file::write_atomic(
        &connection_file,
        &ConnectionInfo {
            schema: SCHEMA_VERSION,
            wire_version: None,
            endpoints: vec![Endpoint {
                host: "127.0.0.1".into(),
                port,
            }],
            key: key.clone(),
            daemon_id: [0xc5; 16],
            pid: std::process::id(),
            daemon_ver: "test".into(),
        },
    )
    .unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        subc_transport::authenticate_server(
            &mut stream,
            &key,
            &[0xc5; 16],
            "test",
            broca_test_support::STAND_IN_AUTH_DEADLINE,
        )
        .await
        .unwrap();
        std::future::pending::<()>().await;
    });
    let connection = broca_subc::SubcConnection::connect(&connection_file)
        .await
        .unwrap();
    let mut executor = LiveLoopExecutor::new(
        Arc::new(FileWalStore::new(root.join("wal"))),
        Arc::new(broca_store::SqliteStore::open(root.join("store.db")).unwrap()),
        Arc::new(SubcToolPlane::new(connection, "unused")),
        None,
    )
    .with_provider_overrides(runtime_overrides("http://127.0.0.1:4010/v1"))
    .with_opencode_auth_path(opencode_auth.clone());
    let auth = executor
        .decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None)
        .await
        .unwrap();
    assert!(
        matches!(&auth.mode, AuthMode::FromOpencodeFile(path) if *path == opencode_auth),
        "the OpenCode auth mode must carry the injected file"
    );
    // Compare without assert_eq: on a failure that would print the header, and when the
    // injection is ignored that header holds the operator's real key.
    let authorization = read_opencode_authorization(&auth).await;
    assert!(
        authorization.as_deref() == Some(format!("Bearer {FIXTURE_KEY}").as_str()),
        "the resolved credential must come from the fixture auth file (request carried an \
         authorization header: {}; value withheld because it may be a real credential)",
        authorization.is_some()
    );
    assert_eq!(
        auth.selection.as_ref().map(|s| s.credential_id.as_str()),
        Some("apikey:deepseek")
    );
    assert_eq!(
        broca_module_serve::ChargeBasisTable::shipped_defaults()
            .resolve("deepseek", auth.method.unwrap())
            .0,
        broca_core::ChargeBasis::MeteredApi
    );
    install_test_catalog();
    let mut req = broca_core::RunRequest { directives: None, session_class: None, retention: None, title: None, on_restart: None, keep_warm: None, send_id: None, request_identity_digest: None,
        scope: None,
        project_id: None,
        principal: None,
        run_id: broca_protocol::RunId("override".into()),
        session: broca_protocol::BindIdentity::new("project", "test", "session"),
        model: broca_protocol::request::ModelHandle {
            provider_module_id: "deepseek".into(),
            model_id: "deepseek-v4-flash".into(),
        },
        input: vec![],
        tools: vec![],
        policy_allowed: Default::default(),
        tool_choice: Default::default(),
        generation: Default::default(),
        cache: None,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: vec![],
        submission_id: None,
        context_management: None,
        inherit_frozen_cache: false, structured_output: None, };
    assert!(executor
        .cache_policy_for_episode(true, None, &req, None)
        .unwrap()
        .is_none());
    // DeepSeek caches automatically. Switch to the overridden Anthropic provider and
    // request cache markers explicitly to verify that mock routing does not disable them.
    req.model.provider_module_id = "anthropic".into();
    req.cache = Some(broca_protocol::request::CacheRequest {
        role: Some(broca_protocol::request::CacheRole::Primary),
        ttl: None,
        key: None,
        tail: None,
        mode: None,
    });
    assert!(executor
        .cache_policy_for_episode(true, Some(WireFamilyId::AnthropicMessages), &req, None)
        .unwrap()
        .is_some());
    assert_resume_replays_the_admitted_cache_policy(&executor, req.clone());
    assert_eq!(auth.selection.as_ref().unwrap().resolved_credential_id, None);
    executor.provider_base_url = Some("http://127.0.0.1:4010/v1".into());
    let mock = executor.decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None).await.unwrap();
    assert!(mock.selection.is_none(), "mock auth has no vault witness, vault request, or selection");
    drop(executor);
    server.abort();
    std::fs::remove_dir_all(root).unwrap();
}

/// A resume replays the cache policy its episode froze at admission and never re-resolves it,
/// even though the same send admitted fresh today would get first-party Anthropic's default.
/// Both halves matter: a run admitted before that default existed froze no policy and must keep
/// rendering no markers, and a run admitted with the default must keep it.
fn assert_resume_replays_the_admitted_cache_policy(
    executor: &LiveLoopExecutor,
    mut req: broca_core::RunRequest,
) {
    req.model.provider_module_id = "anthropic".into();
    req.cache = None;
    let family = Some(WireFamilyId::AnthropicMessages);
    let fresh = executor
        .cache_policy_for_episode(true, family, &req, None)
        .unwrap();
    assert!(
        matches!(
            fresh.as_ref().map(|policy| &policy.class),
            Some(broca_provider::wire::CacheClass::Markers { omit_ttl: true, .. })
        ),
        "a fresh first-party Anthropic episode with no cache request gets the default: {fresh:?}"
    );

    let admitted_with = |cache: Option<CachePolicy>| {
        let mut frozen = broca_provider::families::anthropic_frozen_config("claude-opus-4-8");
        frozen.cache = cache;
        let config: broca_wal::RunConfig = serde_json::from_value(serde_json::json!({
            "model": req.model,
            "tools": [],
            "tool_choice": broca_protocol::request::ToolChoice::default(),
            "generation": req.generation,
            "response_format": broca_protocol::request::ResponseFormat::default(),
            "context_epoch": 0,
            "renderer_version": frozen.renderer_version,
            "frozen_render_config": frozen,
        }))
        .expect("a minimal admitted run config");
        PriorRunView {
            run_id: broca_protocol::RunId("admitted".into()),
            config,
            idle: false,
        }
    };

    assert_eq!(
        executor
            .cache_policy_for_episode(false, family, &req, Some(&admitted_with(None)))
            .unwrap(),
        None,
        "a run admitted before the default froze no policy and resumes with no markers"
    );
    assert_eq!(
        executor
            .cache_policy_for_episode(false, family, &req, Some(&admitted_with(fresh.clone())))
            .unwrap(),
        fresh,
        "a run admitted with the default resumes with it"
    );
}

// The live catalog is process-global. Each catalog-replacement proof runs in its own test
// process so parallel tests cannot replace its two snapshots between resolution calls.
fn run_catalog_drift_child(test_name: &str) -> bool {
    const CHILD: &str = "BROCA_TEST_CATALOG_DRIFT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("executor::tests::{test_name}"),
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    false
}

/// Install a one-model catalog whose render-relevant facts are the parameters.
fn install_drift_revision(reasoning: bool, output: u64) {
    let json = serde_json::json!({"anthropic": {
        "npm": "@ai-sdk/anthropic",
        "models": {"resume-drift": {
            "reasoning": reasoning, "tool_call": true, "attachment": false,
            "limit": {"context": 200000, "output": output}
        }}
    }});
    let catalog = broca_catalog::Catalog::from_json_str(&json.to_string(), "{}").unwrap();
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache.install_parsed_for_test(&catalog, 0, broca_catalog::CatalogSource::Fusiform);
    broca_catalog::install_global_catalog_cache(cache);
}

/// Resolve the drift model the way the executor does. `recorded` is the prior episode's
/// RunConfig on a resume. Using the executor's decoder ensures this test and production
/// resume use the same complete recorded render bundle.
fn resolve_drift_revision(
    recorded: Option<&broca_wal::RunConfig>,
    caller_max_tokens: Option<u32>,
) -> broca_catalog::live::ProviderResolution {
    let auth = AuthResolution {
        mode: AuthMode::Bearer("test-oauth-token".into()),
        pause: None,
        selection: None,
        method: Some(AuthMethod::Oauth),
        project_id: None,
        account_id: None,
        record_version: None,
        in_run_refresh: None,
    };
    let recorded_render = recorded.map(decode_recorded_render_config).transpose().unwrap();
    resolve_provider_for_episode(
        "anthropic/resume-drift",
        None,
        caller_max_tokens,
        &auth,
        &broca_catalog::live::ResolveOptions {
            auth_method: Some(AuthMethod::Oauth),
            ..Default::default()
        },
        broca_provider::build_provider_http_client(),
        broca_catalog::live::EpisodeRenderConfig {
            recorded_config: recorded_render.as_ref(),
            codeassist_identity: None,
            claude_code_persona: Default::default(),
            base_url: Some(
                recorded
                    .and_then(|config| config.frozen_render_config["base_url"].as_str())
                    .unwrap_or("http://host-a.example"),
            ),
            max_output_tokens: recorded.and_then(frozen_max_output_tokens),
            variant: None,
            service_tier: None,
            thinking_echo_scope: None,
            thinking_cross_model: None,
            responses_tools_non_strict: None,
            chatgpt_effort_updates: None,
            chatgpt_wire: None,
            reasoning_content_required: None,
            reasoning_echo: None,
            provider_text_state: None,
            structured_output: None,
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| panic!("catalog resolution: {error}"))
}

fn drift_request(root: &std::path::Path) -> broca_core::RunRequest {
    broca_core::RunRequest { directives: None, session_class: None, retention: None, title: None, on_restart: None, keep_warm: None, send_id: None, request_identity_digest: None,
        scope: None,
        project_id: None,
        principal: None,
        run_id: broca_protocol::RunId("resume-drift".into()),
        session: broca_protocol::BindIdentity::new(root, "test", "session"),
        model: broca_protocol::request::ModelHandle {
            provider_module_id: "anthropic".into(),
            model_id: "resume-drift".into(),
        },
        input: vec![],
        tools: vec![],
        policy_allowed: Default::default(),
        tool_choice: Default::default(),
        generation: Default::default(),
        cache: None,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: vec![],
        submission_id: None,
        context_management: None,
        inherit_frozen_cache: false, structured_output: None, }
}

/// The RunConfig a run started with `started` would have written into RunStarted.
fn drift_run_config(
    req: &broca_core::RunRequest,
    started: &broca_catalog::live::ProviderResolution,
) -> broca_wal::RunConfig {
    broca_wal::RunConfig { turn_directives_v1: None, render_tags: false, computed_tags: None,
        model: req.model.clone(),
        tools: req.tools.clone().into(),
        tool_choice: req.tool_choice.clone(),
        generation: req.generation.clone(),
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: started.provider.renderer_version(),
        frozen_render_config: started.provider.render_config_blob(),
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: vec![],
        context_management: None, }
}

// Keep production rendering/config identity; replace only network IO. A masked drift
// guard would reach this step and complete, making the false acceptance observable.
struct NoNetworkProvider {
    inner: Box<dyn broca_core::Provider>,
    calls: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl broca_core::Provider for NoNetworkProvider {
    async fn step(
        &self,
        _id: broca_protocol::StepId,
        _opts: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<broca_protocol::NormalizedEvent> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(2);
        tx.send(broca_protocol::NormalizedEvent::Control(
            broca_protocol::ProviderControl::AssistantMessage {
                message: broca_protocol::content::AssistantMessage {
                    message_id: broca_protocol::MessageId("resume-response".into()),
                    content: vec![broca_protocol::content::ContentBlock::Text {
                        text: "done".into(),
                    }],
                },
            },
        ))
        .await
        .unwrap();
        tx.send(broca_protocol::NormalizedEvent::Control(
            broca_protocol::ProviderControl::Finished {
                usage: Default::default(),
                finish_reason: broca_protocol::FinishReason::Stop,
                retries_used: 0,
            },
        ))
        .await
        .unwrap();
        rx
    }
    fn render_config_blob(&self) -> serde_json::Value {
        self.inner.render_config_blob()
    }
    fn renderer_version(&self) -> u32 {
        self.inner.renderer_version()
    }
    fn reproduces_renderer_version(&self, recorded: u32) -> bool {
        self.inner.reproduces_renderer_version(recorded)
    }
    fn render_config_blob_for_version(&self, renderer_version: u32) -> serde_json::Value {
        self.inner.render_config_blob_for_version(renderer_version)
    }
    fn render_request(&self, opts: &broca_protocol::CallOptions) -> Vec<u8> {
        self.inner.render_request(opts)
    }
}

/// Write `recorded` as the run's RunStarted, then drive the real core loop against `resumed`.
/// Returns the error provider codes, the finish reason, and how many provider calls were made.
async fn resume_through_core(
    root: &std::path::Path,
    recorded: broca_wal::RunConfig,
    resumed: broca_catalog::live::ProviderResolution,
) -> (
    Vec<Option<String>>,
    broca_protocol::caller::RunFinishReason,
    usize,
) {
    let provider = NoNetworkProvider {
        inner: resumed.provider,
        calls: Default::default(),
    };
    let req = drift_request(root);
    let wal_store = FileWalStore::new(root.join("wal"));
    let mut wal = wal_store.open(&req.session).unwrap();
    wal.append(
        1,
        &broca_wal::WalRecord::RunStarted { tags: None, session_class: None, retention: None,
        hooks: None,
         title: None,
         on_restart: None,
         keep_warm: None, marker_order_version: None, send_id: None, request_identity_digest: None,
         manifest: None,
        scope: None,
        project_id: None,
        principal: None,
        ts_ms: None,
        run_id: req.run_id.clone(),
        session: req.session.clone(),
        config: Box::new(recorded),
        input: req.input.clone().into(),
        origin: broca_wal::RunOrigin::Fresh,
        submission_id: None, },
        true,
    )
    .unwrap();
    drop(wal);
    let (tx, mut rx) = mpsc::channel(128);
    let summary = broca_core::run_loop(
        req,
        &provider,
        &broca_core::StubToolPlane::new(),
        &broca_core::FileLeaseStore::new(root.join("leases")),
        &AllowAll,
        &wal_store,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    let mut codes = Vec::new();
    while let Some(event) = rx.recv().await {
        if let broca_core::OutputEvent::Control(broca_protocol::caller::ControlUnit::Error {
            error,
        }) = event
        {
            codes.push(error.provider_code.clone());
        }
    }
    let calls = provider.calls.load(std::sync::atomic::Ordering::SeqCst);
    (codes, summary.reason, calls)
}

/// A changed reasoning fact affects new admissions, not an episode whose complete render
/// bundle is already recorded. The resumed run must render its original bytes and proceed.
#[tokio::test]
async fn runtime_override_catalog_drift_resumes_recorded_bytes() {
    if !run_catalog_drift_child("runtime_override_catalog_drift_resumes_recorded_bytes") {
        return;
    }
    let root = broca_test_support::scratch_root()
        .join(format!("broca-catalog-drift-{}", std::process::id()));
    install_drift_revision(true, 8192);
    let started = resolve_drift_revision(None, None);
    let recorded = drift_run_config(&drift_request(&root), &started);
    install_drift_revision(false, 8192);
    assert_ne!(resolve_drift_revision(None, None).provider.render_config_blob(), started.provider.render_config_blob(), "the new catalog changes fresh admissions");
    let resumed = resolve_drift_revision(Some(&recorded), None);
    assert_eq!(resumed.provider.render_config_blob(), started.provider.render_config_blob());
    let prompt = broca_protocol::CallOptions::new(vec![]);
    assert_eq!(resumed.provider.render_request(&prompt), started.provider.render_request(&prompt));
    assert_eq!(
        started.provider.renderer_version(),
        resumed.provider.renderer_version()
    );
    assert_eq!(resumed.base_url, "http://host-a.example");

    let (codes, reason, calls) = resume_through_core(&root, recorded, resumed).await;
    eprintln!("resume outcome: {reason:?}; provider_codes={codes:?}; provider_calls={calls}");
    std::fs::remove_dir_all(root).unwrap();
    assert!(codes.is_empty(), "a catalog change cannot refuse a recorded episode: {codes:?}");
    assert_eq!(reason, broca_protocol::caller::RunFinishReason::Completed);
    assert_eq!(calls, 1);
}

/// A send with no cap freezes the catalog's limit.output (A). The catalog then moves that fact
/// to B, which is ordinary upstream churn. The resume must render with A and run, rather than
/// re-derive B and trip the drift guard.
#[tokio::test]
async fn moved_catalog_output_limit_resumes_with_the_frozen_default() {
    if !run_catalog_drift_child("moved_catalog_output_limit_resumes_with_the_frozen_default") {
        return;
    }
    const FROZEN: u64 = 8192;
    const MOVED: u64 = 16384;
    let root = broca_test_support::scratch_root()
        .join(format!("broca-output-drift-{}", std::process::id()));
    install_drift_revision(true, FROZEN);
    let started = resolve_drift_revision(None, None);
    assert_eq!(
        started.provider.render_config_blob()["max_output_tokens"],
        FROZEN,
        "an omitted cap freezes the model's catalog limit"
    );
    let recorded = drift_run_config(&drift_request(&root), &started);
    install_drift_revision(true, MOVED);
    // Non-vacuity: a fresh episode on the moved catalog DOES pick up B, so the equality below
    // is the restore at work rather than a catalog move that never took effect.
    assert_eq!(
        resolve_drift_revision(None, None).provider.render_config_blob()["max_output_tokens"],
        MOVED
    );
    let resumed = resolve_drift_revision(Some(&recorded), None);
    let call = broca_protocol::CallOptions::new(vec![]);
    assert_eq!(
        resumed.provider.render_request(&call),
        started.provider.render_request(&call),
        "the resume renders the bytes the episode started with"
    );
    let body: serde_json::Value =
        serde_json::from_slice(&resumed.provider.render_request(&call)).unwrap();
    assert_eq!(body["max_tokens"], FROZEN);

    let (codes, reason, calls) = resume_through_core(&root, recorded, resumed).await;
    eprintln!("resume outcome: {reason:?}; provider_codes={codes:?}; provider_calls={calls}");
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        codes.is_empty(),
        "a moved catalog output limit must not refuse the resume, got {codes:?}"
    );
    assert_eq!(calls, 1, "the resumed run reaches the provider");
    assert_eq!(reason, broca_protocol::caller::RunFinishReason::Completed);
}

/// Install a one-model fusiform catalog for `anthropic/resume-tier`, with or without priced
/// fast-mode rates (`rate.input.mode.fast` / `rate.output.mode.fast`).
fn install_tier_revision(fast_mode_priced: bool) {
    let rate = |units: u64| {
        serde_json::json!({"currency": "USD", "exponent": 9, "state": "priced", "units": units})
    };
    let mut row = serde_json::json!({
        "existence": "present",
        "wire.npm": {"value":"@ai-sdk/anthropic","from":"provider"},
        "wire.api": {"value":"https://api.anthropic.com/v1","from":"curated"},
        "limit.context": 200000,
        "limit.output": 64000,
        "capability.attachment": false,
        "capability.reasoning": false,
        "capability.tool_call": true,
        "rate.input": rate(5_000_000_000),
        "rate.output": rate(25_000_000_000),
    });
    if fast_mode_priced {
        row["rate.input.mode.fast"] = rate(10_000_000_000);
        row["rate.output.mode.fast"] = rate(50_000_000_000);
    }
    let response: broca_catalog::FusiformCatalogResponse = serde_json::from_value(
        serde_json::json!({
            "source": "test",
            "resolved_at_ms": 0,
            "catalog_version": 1,
            "models": {"anthropic/resume-tier": row},
        }),
    )
    .unwrap();
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache
        .install_for_test(response, broca_catalog::CatalogSource::Fusiform)
        .unwrap();
    broca_catalog::install_global_catalog_cache(cache);
}

/// Resolve the tier model as the executor does: a fresh episode checks `requested`; a resume
/// (`recorded` set) restores the tier through the executor's own reader and ignores
/// `requested`, exactly as `run_episode` filters the send's tier by `fresh_episode`.
fn resolve_tier_revision(
    recorded: Option<&broca_wal::RunConfig>,
    requested: Option<broca_catalog::live::ServiceTier>,
) -> Result<broca_catalog::live::ProviderResolution, broca_catalog::live::ResolutionError> {
    let auth = AuthResolution {
        mode: AuthMode::Bearer("test-key".into()),
        pause: None,
        selection: None,
        method: Some(AuthMethod::ApiKey),
        project_id: None,
        account_id: None,
        record_version: None,
        in_run_refresh: None,
    };
    resolve_provider_for_episode(
        "anthropic/resume-tier",
        None,
        None,
        &auth,
        &broca_catalog::live::ResolveOptions {
            auth_method: Some(AuthMethod::ApiKey),
            service_tier: requested.filter(|_| recorded.is_none()),
            ..Default::default()
        },
        broca_provider::build_provider_http_client(),
        broca_catalog::live::EpisodeRenderConfig {
            codeassist_identity: None,
            claude_code_persona: Default::default(),
            base_url: Some("http://host-a.example"),
            max_output_tokens: recorded.and_then(frozen_max_output_tokens),
            variant: None,
            service_tier: recorded.and_then(frozen_service_tier),
            thinking_echo_scope: None,
            thinking_cross_model: None,
            responses_tools_non_strict: None,
            chatgpt_effort_updates: None,
            chatgpt_wire: None,
            reasoning_content_required: None,
            reasoning_echo: None,
            provider_text_state: None,
            structured_output: None,
            ..Default::default()
        },
    )
}

/// A priority send on Anthropic freezes fast mode at admission. The catalog then withdraws the
/// fast-mode rate. A fresh priority send is now refused, but the admitted run resumes with the
/// frozen tier, byte-identical, even though the resuming send asks for a different tier, and
/// completes rather than being refused or silently dropped to the default.
#[tokio::test]
async fn withdrawn_mode_rate_resumes_with_the_frozen_service_tier() {
    if !run_catalog_drift_child("withdrawn_mode_rate_resumes_with_the_frozen_service_tier") {
        return;
    }
    use broca_catalog::live::ServiceTier;
    let root = broca_test_support::scratch_root()
        .join(format!("broca-tier-drift-{}", std::process::id()));
    install_tier_revision(true);
    let started =
        resolve_tier_revision(None, Some(ServiceTier::Priority)).expect("priority admitted");
    let mut recorded = drift_run_config(&drift_request(&root), &started);
    recorded.service_tier = Some(ServiceTier::Priority);
    assert_eq!(recorded.frozen_render_config["speed"], "fast");

    install_tier_revision(false);
    // Once the catalog stops pricing fast mode, a fresh priority send is refused ...
    let refused = resolve_tier_revision(None, Some(ServiceTier::Priority))
        .err()
        .expect("the moved catalog prices no fast mode");
    assert!(refused.to_string().contains("no rate in the catalog"), "{refused}");
    // ... and without the recorded tier the request would render default-speed bytes, so the
    // byte equality asserted below can only come from restoring the frozen priority tier.
    let unrestored = resolve_tier_revision(None, None).unwrap();
    let call = broca_protocol::CallOptions::new(vec![]);
    assert_ne!(
        unrestored.provider.render_request(&call),
        started.provider.render_request(&call)
    );

    // The resuming send asks for the default tier; the frozen priority wins.
    let resumed = resolve_tier_revision(Some(&recorded), Some(ServiceTier::Default))
        .expect("resume resolves");
    assert_eq!(
        resumed.provider.render_request(&call),
        started.provider.render_request(&call),
        "the resume renders the bytes the episode started with"
    );
    let body: serde_json::Value =
        serde_json::from_slice(&resumed.provider.render_request(&call)).unwrap();
    assert_eq!(body["speed"], "fast");

    let (codes, reason, calls) = resume_through_core(&root, recorded, resumed).await;
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        codes.is_empty(),
        "a withdrawn mode rate must not refuse the resume, got {codes:?}"
    );
    assert_eq!(calls, 1, "the resumed run reaches the provider");
    assert_eq!(reason, broca_protocol::caller::RunFinishReason::Completed);
}

/// A `session.send` naming a catalog model whose stated `model.kind` is not `chat` is refused
/// while the executor resolves the provider, before RunStarted, as a permanent configuration
/// error naming the model and its kind (`open_failed`, class `permanent`, on the wire). A chat
/// row with no stated kind in the same catalog resolves exactly as before.
#[tokio::test]
async fn a_non_chat_catalog_model_is_refused_at_admission() {
    if !run_catalog_drift_child("a_non_chat_catalog_model_is_refused_at_admission") {
        return;
    }
    // A chat row with no `model.kind`, beside fusiform's served Clef row.
    let mut response = broca_catalog::FusiformCatalogResponse {
        source: "test".into(),
        resolved_at_ms: 0,
        catalog_version: 1,
        models: Default::default(),
        withheld: vec![],
        uncertain: vec![],
        wire_corrections: vec![],
    };
    let tier_row = serde_json::json!({
        "existence": "present",
        "wire.npm": {"value":"@ai-sdk/anthropic","from":"provider"},
        "wire.api": {"value":"https://api.anthropic.com/v1","from":"curated"},
        "limit.context": 200000,
        "capability.attachment": false,
        "capability.reasoning": false,
        "capability.tool_call": true,
    });
    response.models.insert(
        "anthropic/resume-tier".into(),
        serde_json::from_value(tier_row).unwrap(),
    );
    response.models.insert(
        "cloudflare-workers-ai/@cf/cloudflare/clef".into(),
        serde_json::from_value(serde_json::json!({
            "capability.attachment": true,
            "existence": "present",
            "limit.context": 65536,
            "model.kind": "decision",
            "rate.input": {"currency": "USD", "exponent": 9, "state": "priced",
                           "unit_provenance": {"kind": "stated"}, "units": 240000000},
            "rate.output": {"state": "stated_zero"},
        }))
        .unwrap(),
    );
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache
        .install_for_test(response, broca_catalog::CatalogSource::Fusiform)
        .unwrap();
    broca_catalog::install_global_catalog_cache(cache);

    let auth = AuthResolution {
        mode: AuthMode::Bearer("test-key".into()),
        pause: None,
        selection: None,
        method: Some(AuthMethod::ApiKey),
        project_id: None,
        account_id: None,
        record_version: None,
        in_run_refresh: None,
    };
    let resolve = |model: &str| {
        resolve_provider_for_episode(
            model,
            None,
            None,
            &auth,
            &broca_catalog::live::ResolveOptions {
                auth_method: Some(AuthMethod::ApiKey),
                ..Default::default()
            },
            broca_provider::build_provider_http_client(),
            broca_catalog::live::EpisodeRenderConfig::default(),
        )
    };
    let error = match resolve("cloudflare-workers-ai/@cf/cloudflare/clef") {
        Err(error) => error,
        Ok(_) => panic!("a decision model must not resolve for a chat send"),
    };
    let message = error.to_string();
    assert!(
        message.contains("'cloudflare-workers-ai/@cf/cloudflare/clef'")
            && message.contains("decision model"),
        "{message}"
    );
    let run_error = RunError::from(error);
    assert!(matches!(run_error, RunError::Config(_)), "{run_error}");
    assert_eq!(
        run_error.admission_error_class(),
        broca_protocol::ErrorClass::Permanent
    );
    resolve("anthropic/resume-tier").expect("a chat row with no stated kind still resolves");
}

/// Install a one-model catalog whose only varying fact is its models.dev `reasoning_options`.
fn install_variant_revision(reasoning_options: serde_json::Value) {
    let json = serde_json::json!({"anthropic": {
        "npm": "@ai-sdk/anthropic",
        "models": {"resume-drift": {
            "reasoning": true, "tool_call": true, "attachment": false,
            "limit": {"context": 200000, "output": 64000},
            "reasoning_options": reasoning_options
        }}
    }});
    let catalog = broca_catalog::Catalog::from_json_str(&json.to_string(), "{}").unwrap();
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache.install_parsed_for_test(&catalog, 0, broca_catalog::CatalogSource::Fusiform);
    broca_catalog::install_global_catalog_cache(cache);
}

/// Resolve the drift model as the executor does for a variant send: a fresh episode resolves
/// `requested`; a resume (`recorded` set) restores the variant through the executor's reader.
fn resolve_variant_revision(
    recorded: Option<&broca_wal::RunConfig>,
    requested: Option<&str>,
) -> Result<broca_catalog::live::ProviderResolution, broca_catalog::live::ResolutionError> {
    let auth = AuthResolution {
        mode: AuthMode::Bearer("test-oauth-token".into()),
        pause: None,
        selection: None,
        method: Some(AuthMethod::Oauth),
        project_id: None,
        account_id: None,
        record_version: None,
        in_run_refresh: None,
    };
    let restored = recorded.and_then(frozen_variant);
    resolve_provider_for_episode(
        "anthropic/resume-drift",
        None,
        None,
        &auth,
        &broca_catalog::live::ResolveOptions {
            auth_method: Some(AuthMethod::Oauth),
            variant: requested.filter(|_| recorded.is_none()),
            ..Default::default()
        },
        broca_provider::build_provider_http_client(),
        broca_catalog::live::EpisodeRenderConfig {
            codeassist_identity: None,
            claude_code_persona: Default::default(),
            base_url: Some("http://host-a.example"),
            max_output_tokens: recorded.and_then(frozen_max_output_tokens),
            variant: restored.as_ref(),
            service_tier: None,
            thinking_echo_scope: None,
            thinking_cross_model: None,
            responses_tools_non_strict: None,
            chatgpt_effort_updates: None,
            chatgpt_wire: None,
            reasoning_content_required: None,
            reasoning_echo: None,
            provider_text_state: None,
            structured_output: None,
            ..Default::default()
        },
    )
}

/// A variant send freezes the parameters its catalog options gave at admission. The catalog
/// then withdraws the options entirely. A fresh send of the same variant is now refused, but the
/// admitted run resumes with the frozen parameters, byte-identical, and completes rather than
/// being refused or re-derived.
#[tokio::test]
async fn withdrawn_reasoning_options_resume_with_the_frozen_variant() {
    if !run_catalog_drift_child("withdrawn_reasoning_options_resume_with_the_frozen_variant") {
        return;
    }
    let root = broca_test_support::scratch_root()
        .join(format!("broca-variant-drift-{}", std::process::id()));
    install_variant_revision(serde_json::json!([{"min": 1024, "type": "budget_tokens"}]));
    let started = resolve_variant_revision(None, Some("high")).expect("variant admitted");
    let mut recorded = drift_run_config(&drift_request(&root), &started);
    recorded.variant = Some("high".into());
    assert_eq!(recorded.frozen_render_config["variant"]["name"], "high");

    install_variant_revision(serde_json::json!([]));
    // Non-vacuity: on the moved catalog a fresh send of the same variant is refused.
    let refused = resolve_variant_revision(None, Some("high"))
        .err()
        .expect("the moved catalog defines no variant");
    assert!(refused.to_string().contains("takes no reasoning options"), "{refused}");

    let resumed = resolve_variant_revision(Some(&recorded), None).expect("resume resolves");
    let call = broca_protocol::CallOptions::new(vec![]);
    assert_eq!(
        resumed.provider.render_request(&call),
        started.provider.render_request(&call),
        "the resume renders the bytes the episode started with"
    );
    let body: serde_json::Value =
        serde_json::from_slice(&resumed.provider.render_request(&call)).unwrap();
    assert_eq!(
        body["thinking"],
        serde_json::json!({"type": "enabled", "budget_tokens": 16000})
    );

    let (codes, reason, calls) = resume_through_core(&root, recorded, resumed).await;
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        codes.is_empty(),
        "withdrawn reasoning options must not refuse the resume, got {codes:?}"
    );
    assert_eq!(calls, 1, "the resumed run reaches the provider");
    assert_eq!(reason, broca_protocol::caller::RunFinishReason::Completed);
}

/// A resume restores the Anthropic thinking-echo scope from the recorded blob. A blob written
/// before the scope existed carries no `echo_scope` key and must read back as the legacy
/// last-assistant-message scope; a blob without interleaved thinking restores nothing.
#[test]
fn resume_reads_the_recorded_thinking_echo_scope() {
    use broca_catalog::live::ThinkingEchoScope;

    let root = broca_test_support::scratch_root()
        .join(format!("broca-echo-scope-{}", std::process::id()));
    let started = resolve_runtime_override(&runtime_overrides("http://127.0.0.1:9"), None);
    let mut recorded = drift_run_config(&drift_request(&root), &started);
    let with_reasoning = |reasoning: serde_json::Value| {
        serde_json::json!({ "family_id": "anthropic_messages", "reasoning": reasoning })
    };

    recorded.frozen_render_config = with_reasoning(serde_json::json!({
        "mode": "anthropic_interleaved", "budget_tokens": 1024, "last_turn_only": true
    }));
    assert_eq!(
        frozen_thinking_echo_scope(&recorded),
        Some(ThinkingEchoScope::LastAssistantMessage)
    );

    recorded.frozen_render_config = with_reasoning(serde_json::json!({
        "mode": "anthropic_interleaved", "budget_tokens": 1024, "last_turn_only": true,
        "echo_scope": "all_assistant_messages"
    }));
    assert_eq!(
        frozen_thinking_echo_scope(&recorded),
        Some(ThinkingEchoScope::AllAssistantMessages)
    );

    recorded.frozen_render_config = with_reasoning(serde_json::json!({ "mode": "none" }));
    assert_eq!(frozen_thinking_echo_scope(&recorded), None);
}

/// A resume restores the Claude Code persona from the recorded blob. A blob written before the
/// persona existed has no `claude_code_persona` key and reads back as `None`, which the resolver
/// keeps (`EpisodePersona::Recorded(None)`), so that episode resumes with its old bytes.
#[test]
fn resume_reads_the_recorded_claude_code_persona() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-persona-restore-{}", std::process::id()));
    let started = resolve_runtime_override(&runtime_overrides("http://127.0.0.1:9"), None);
    let mut recorded = drift_run_config(&drift_request(&root), &started);
    recorded.frozen_render_config = serde_json::json!({ "family_id": "anthropic_messages" });
    assert_eq!(frozen_claude_code_persona(&recorded), None);

    let persona = serde_json::json!({
        "version": "2.1.280",
        "device_id": "0".repeat(64),
        "session_id": "11111111-1111-4111-8111-111111111111",
        "prompt_id": "33333333-3333-4333-8333-333333333333"
    });
    recorded.frozen_render_config = serde_json::json!({
        "family_id": "anthropic_messages", "claude_code_persona": persona
    });
    let restored = frozen_claude_code_persona(&recorded).expect("the recorded persona restores");
    assert_eq!(serde_json::to_value(&restored).unwrap(), persona);
}

#[test]
fn resume_reads_the_recorded_responses_tool_strictness() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-tool-strictness-{}", std::process::id()));
    let started = resolve_runtime_override(&runtime_overrides("http://127.0.0.1:9"), None);
    let mut recorded = drift_run_config(&drift_request(&root), &started);
    recorded.frozen_render_config = serde_json::json!({});
    assert!(!frozen_responses_tools_non_strict(&recorded));
    recorded.frozen_render_config = serde_json::json!({"responses_tools_non_strict": true});
    assert!(frozen_responses_tools_non_strict(&recorded));
    recorded.frozen_render_config = serde_json::json!({"responses_tools_non_strict": false});
    assert!(!frozen_responses_tools_non_strict(&recorded));
}

#[test]
fn resume_reads_the_recorded_reasoning_content_required() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-reasoning-echo-{}", std::process::id()));
    let started = resolve_runtime_override(&runtime_overrides("http://127.0.0.1:9"), None);
    let mut recorded = drift_run_config(&drift_request(&root), &started);
    recorded.frozen_render_config = serde_json::json!({});
    assert!(!frozen_reasoning_content_required(&recorded));
    recorded.frozen_render_config = serde_json::json!({"reasoning_content_required": true});
    assert!(frozen_reasoning_content_required(&recorded));
}

#[test]
fn resume_reads_the_recorded_reasoning_echo_policy() {
    use broca_provider::policy::{ReasoningEchoPolicy, ReasoningEchoTurns};
    let root = broca_test_support::scratch_root()
        .join(format!("broca-echo-policy-{}", std::process::id()));
    let started = resolve_runtime_override(&runtime_overrides("http://127.0.0.1:9"), None);
    let mut recorded = drift_run_config(&drift_request(&root), &started);
    recorded.frozen_render_config = serde_json::json!({"reasoning_content_required": true});
    assert_eq!(frozen_reasoning_echo(&recorded), None);
    let policy = ReasoningEchoPolicy {
        turns: ReasoningEchoTurns::ToolCalls,
        ..ReasoningEchoPolicy::OPENROUTER
    };
    recorded.frozen_render_config["reasoning_echo"] = serde_json::to_value(policy).unwrap();
    assert_eq!(frozen_reasoning_echo(&recorded), Some(policy));
}

/// A conversation with a reasoning-carrying tool-call turn and a plain text turn, the two
/// assistant shapes whose rendering the DeepSeek reasoning echo changes.
fn deepseek_history() -> broca_protocol::CallOptions {
    use broca_protocol::{ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult};
    broca_protocol::CallOptions::new(vec![
        Message::new(Role::User, vec![ContentBlock::Text { text: "look".into() }]),
        Message::new(
            Role::Assistant,
            vec![
                ContentBlock::Reasoning { text: "read the file first".into(), signature: None },
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call_0".into()),
                    tool_name: "read".into(),
                    input: serde_json::json!({}),
                }),
            ],
        ),
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call_0".into()),
                output: ToolOutput::Text { text: "contents".into() },
                is_error: false,
            })],
        ),
        Message::new(Role::Assistant, vec![ContentBlock::Text { text: "done".into() }]),
        Message::new(Role::User, vec![ContentBlock::Text { text: "again".into() }]),
    ])
}

fn assistant_reasoning(body: &[u8]) -> Vec<Option<serde_json::Value>> {
    let body: serde_json::Value = serde_json::from_slice(body).unwrap();
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "assistant")
        .map(|message| message.get("reasoning_content").cloned())
        .collect()
}

/// A DeepSeek episode admitted before DeepSeek was opted into the reasoning echo recorded no
/// `reasoning_content_required`. Its resume restores that absence rather than taking today's
/// catalog value, so the resumed config equals the recorded blob (core's drift guard compares
/// the two and would otherwise refuse the resume) and renders without `reasoning_content`. A
/// newly admitted episode of the same model gets the echo on every assistant turn.
#[test]
fn deepseek_episode_admitted_before_the_reasoning_echo_resumes_with_its_bytes() {
    let fresh = resolve_runtime_override(&Default::default(), None);
    let fresh_blob = fresh.provider.render_config_blob();
    assert!(fresh_blob.get("reasoning_echo").is_some());
    assert_eq!(
        assistant_reasoning(&fresh.provider.render_request(&deepseek_history())),
        vec![
            Some(serde_json::json!("read the file first")),
            Some(serde_json::json!(""))
        ]
    );

    let mut recorded = fresh_blob;
    recorded
        .as_object_mut()
        .unwrap()
        .remove("reasoning_echo");
    let resumed = resolve_runtime_override(&Default::default(), Some(&recorded));
    assert_eq!(
        serde_json::to_vec(&resumed.provider.render_config_blob()).unwrap(),
        serde_json::to_vec(&recorded).unwrap(),
        "the resumed config must equal the recorded blob"
    );
    assert_eq!(
        assistant_reasoning(&resumed.provider.render_request(&deepseek_history())),
        vec![None, None],
        "a resumed pre-echo episode renders no reasoning_content"
    );

    // The intermediate frozen shape had the scalar flag but no policy object.
    recorded["reasoning_content_required"] = serde_json::json!(true);
    let resumed = resolve_runtime_override(&Default::default(), Some(&recorded));
    assert_eq!(resumed.provider.render_config_blob(), recorded);
    assert_eq!(
        resumed.provider.render_request(&deepseek_history()),
        fresh.provider.render_request(&deepseek_history()),
        "legacy true and the new DeepSeek policy render identical bytes"
    );
}

/// An executor whose tool plane is a stand-in subc connection that authenticates and then idles:
/// enough for admission-side methods, with no daemon, vault or provider.
async fn stand_in_executor(root: &std::path::Path) -> (LiveLoopExecutor, tokio::task::JoinHandle<()>) {
    use subc_transport::connection_file::{ConnectionInfo, Endpoint, SCHEMA_VERSION};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let key = vec![0x5d; 32];
    let connection_file = root.join("connection.json");
    subc_transport::connection_file::write_atomic(
        &connection_file,
        &ConnectionInfo {
            schema: SCHEMA_VERSION,
            wire_version: None,
            endpoints: vec![Endpoint {
                host: "127.0.0.1".into(),
                port,
            }],
            key: key.clone(),
            daemon_id: [0xd5; 16],
            pid: std::process::id(),
            daemon_ver: "test".into(),
        },
    )
    .unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        subc_transport::authenticate_server(
            &mut stream,
            &key,
            &[0xd5; 16],
            "test",
            broca_test_support::STAND_IN_AUTH_DEADLINE,
        )
        .await
        .unwrap();
        std::future::pending::<()>().await;
    });
    let connection = broca_subc::SubcConnection::connect(&connection_file)
        .await
        .unwrap();
    let executor = LiveLoopExecutor::new(
        Arc::new(FileWalStore::new(root.join("wal"))),
        Arc::new(broca_store::SqliteStore::open(root.join("store.db")).unwrap()),
        Arc::new(SubcToolPlane::new(connection, "unused")),
        None,
    );
    (executor, server)
}

/// A `RunStarted` record for `session` whose input is one user message of `text`.
fn run_started_with_user_text(
    session: &broca_protocol::BindIdentity,
    run_id: &str,
    text: &str,
) -> broca_wal::WalRecord {
    serde_json::from_value(serde_json::json!({
        "type": "run_started", "run_id": run_id, "session": session,
        "config": {
            "model": {"provider_module_id": "anthropic", "model_id": "claude-opus-4-8"},
            "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
            "response_format": {"type": "text"}, "context_epoch": 0, "renderer_version": 2
        },
        "input": [{"role": "user", "content": [{"type": "text", "text": text}]}],
        "origin": {"kind": "fresh"}
    }))
    .unwrap()
}

/// The session's first user message is read once, and only as far as the record carrying it:
/// a large later record is never read, and a second admission of the same session reads no WAL
/// at all, whether it inherits the suffix from the latest episode's recorded persona or (when
/// that episode recorded none) from this process's memory of the session.
#[tokio::test]
async fn the_first_user_message_is_read_once_and_only_as_far_as_its_record() {
    use broca_core::WalStore as _;
    use std::sync::atomic::Ordering;
    let root = broca_test_support::scratch_root()
        .join(format!("broca-suffix-read-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let (executor, server) = stand_in_executor(&root).await;

    let mut req = drift_request(&root);
    req.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "Now list the files to change.".into(),
        }],
    )];
    let store = FileWalStore::new(root.join("wal"));
    let mut wal = store.open(&req.session).unwrap();
    let first = "Summarise the README in two sentences.";
    wal.append(1, &run_started_with_user_text(&req.session, "r1", first), true)
        .unwrap();
    wal.append(
        1,
        &run_started_with_user_text(&req.session, "r2", &"x".repeat(1 << 20)),
        true,
    )
    .unwrap();
    drop(wal);
    let wal_len = std::fs::metadata(store.wal_path(&req.session)).unwrap().len();

    let expected = broca_module_serve::claude_code_identity::first_durable_cc_version_suffix(
        &[broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text { text: first.into() }],
        )],
        &[],
    );
    let pinned = executor.session_cc_version_suffix(&req, None, 2).await;
    assert_eq!(pinned, expected, "pinned from the session's first durable user message");
    let read = executor.cc_suffix_wal_bytes.load(Ordering::Relaxed);
    assert!(read > 0, "the first admission reads the WAL prefix");
    assert!(
        read < wal_len / 2,
        "only the first record is read ({read} of {wal_len} bytes)"
    );

    // The next episode inherits the suffix its predecessor recorded and reads nothing.
    let recorded = serde_json::json!({
        "claude_code_persona": {
            "version": broca_provider::wire::ClaudeCodePersona::CURRENT_VERSION,
            "device_id": "0", "session_id": "s", "cc_version_suffix": pinned.clone().unwrap()
        }
    });
    assert_eq!(
        executor.session_cc_version_suffix(&req, Some(&recorded), 2).await,
        pinned
    );
    // An episode after one that recorded no persona (another provider) is answered from memory.
    assert_eq!(
        executor.session_cc_version_suffix(&req, Some(&serde_json::json!({})), 2).await,
        pinned
    );
    assert_eq!(
        executor.cc_suffix_wal_bytes.load(Ordering::Relaxed),
        read,
        "a second admission reads no WAL bytes"
    );
    assert!(store.first_user_message(&req.session).unwrap().0.is_some());

    drop(executor);
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

/// A legacy/import migration is counted once. The next admitted episode carries its ordinal
/// in the existing admission snapshot, so its next prompt reads no WAL bytes at all.
#[tokio::test]
async fn later_persona_admissions_inherit_the_ordinal_without_reading_wal_bytes() {
    use std::sync::atomic::Ordering;
    use broca_core::WalStore as _;
    use broca_protocol::{ContentBlock, Message, Role};
    let root = broca_test_support::scratch_root().join(format!("broca-prompt-read-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let (executor, server) = stand_in_executor(&root).await;
    let mut req = drift_request(&root);
    let user = |text: &str| Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }]);
    req.input = vec![user("new prompt")];
    let store = FileWalStore::new(root.join("wal"));
    let mut wal = store.open(&req.session).unwrap();
    let mut imported = run_started_with_user_text(&req.session, "import", "first imported prompt");
    if let broca_wal::WalRecord::RunStarted { input, origin, .. } = &mut imported {
        *input = Arc::new(vec![user("first imported prompt"), user("second imported prompt")]);
        *origin = serde_json::from_value(serde_json::json!({"kind": "seeded_from"})).unwrap();
    }
    wal.append(1, &imported, true).unwrap();
    let tool = |n| broca_wal::WalRecord::ToolResult { tags: None,
        batch_id: broca_protocol::BatchId("batch".into()),
        result: Arc::new(broca_protocol::ToolResult { tool_call_id: broca_protocol::ToolCallId("tool".into()),
            output: broca_protocol::ToolOutput::Text { text: "x".repeat(n) }, is_error: false }),
        outcome: broca_wal::DispatchOutcome::Completed, hooks: None, reason: None,
        withdraw_elicitation: None, provider_held: None,
    };
    wal.append(1, &tool(1 << 20), true).unwrap();
    let legacy = serde_json::json!({"claude_code_persona": {"version": "2.1.280"}});
    let mut old = run_started_with_user_text(&req.session, "legacy", "third prompt");
    if let broca_wal::WalRecord::RunStarted { config, .. } = &mut old { config.frozen_render_config = legacy.clone(); }
    wal.append(1, &old, true).unwrap();
    let prefix_bytes = std::fs::metadata(store.wal_path(&req.session)).unwrap().len();
    wal.append(1, &tool(3 << 20), true).unwrap();
    assert_eq!(executor.session_prompt_ordinal(&req, Some(&legacy), 3).await.unwrap(), 4,
        "both imported prompts and the 2.1.280 episode count");
    let read = executor.cc_prompt_wal_bytes.load(Ordering::Relaxed);
    assert_eq!(read + broca_wal::framing::LINEAGE_FRAME_LEN as u64, prefix_bytes,
        "the read stops at the durable head, before its trailing lineage stamp");
    assert_eq!(executor.session_prompt_ordinal(&req, Some(&legacy), 3).await.unwrap(), 4);
    assert_eq!(executor.cc_prompt_wal_bytes.load(Ordering::Relaxed), read, "a memoized migration reads no bytes");
    let current = serde_json::json!({"claude_code_persona": {"version": "2.1.302", "prompt_ordinal": 4}});
    let mut new = run_started_with_user_text(&req.session, "current", "fourth prompt");
    if let broca_wal::WalRecord::RunStarted { config, .. } = &mut new { config.frozen_render_config = current.clone(); }
    wal.append(1, &new, true).unwrap();
    assert_eq!(executor.session_prompt_ordinal(&req, Some(&current), 5).await.unwrap(), 5);
    req.input.push(user("another message in the same send"));
    assert_eq!(executor.session_prompt_ordinal(&req, Some(&current), 5).await.unwrap(), 5);
    assert_eq!(executor.cc_prompt_wal_bytes.load(Ordering::Relaxed), read,
        "a later admission reads no WAL bytes: it inherits the durable ordinal");
    wal.append(1, &run_started_with_user_text(&req.session, "other-provider", "fifth prompt"), true).unwrap();
    let total = std::fs::metadata(store.wal_path(&req.session)).unwrap().len();
    assert_eq!(executor.session_prompt_ordinal(&req, None, 6).await.unwrap(), 6);
    #[cfg(unix)]
    assert_eq!(executor.cc_prompt_wal_bytes.load(Ordering::Relaxed) - read, total - prefix_bytes,
        "a provider switch reads only the suffix after the memoized anchor");
    #[cfg(not(unix))]
    assert_eq!(executor.cc_prompt_wal_bytes.load(Ordering::Relaxed) - read,
        total - broca_wal::framing::LINEAGE_FRAME_LEN as u64,
        "platforms without file-identity anchors retain the bounded selective fallback");
    assert!(executor.session_prompt_ordinal(&req, None, 7).await.is_err(), "an incomplete read fails closed");
    drop(wal);
    drop(executor);
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

/// A fresh Claude Code episode whose prompt history cannot be read is refused through the
/// admission ack with that read's permanent Config cause. The actor treats a dropped ack as
/// "run ended before admission", an unclassified infrastructure failure that hides the cause.
#[tokio::test]
async fn an_unreadable_prompt_history_is_refused_through_the_admission_ack() {
    use broca_core::WalStore as _;
    let root = broca_test_support::scratch_root()
        .join(format!("broca-prompt-ack-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let (executor, server) = stand_in_executor(&root).await;
    // An absent auth file: no operator credential is read, and the episode is not API-key auth,
    // so it speaks as Claude Code.
    let executor = executor
        .with_claude_code_install_id(Some("install-test".into()))
        .with_opencode_auth_path(root.join("absent-auth.json"));
    let mut req = drift_request(&root);
    req.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text { text: "hello".into() }],
    )];
    let store = FileWalStore::new(root.join("wal"));
    let wal = store.open(&req.session).unwrap();
    let mut prior_snapshot = broca_core::load_prior_wal_snapshot(
        &store,
        &req.session,
        0,
        None::<&broca_store::SqliteStore>,
    )
    .unwrap();
    // The snapshot claims durable history the empty WAL does not hold, so counting prompts
    // through it fails.
    prior_snapshot.head_seq = 5;
    let (ack, admitted) = tokio::sync::oneshot::channel();
    let job = RunJob {
        conversation_key: None,
        req,
        auth: None,
        writer: Arc::new(broca_core::WalHandle::spawn(wal, 1).unwrap()),
        kind: RunKind::AppendEpisode,
        skip_materialization: true,
        control: Default::default(),
        prior_snapshot,
        on_admitted: Some(ack),
    };
    let (out, _events) = tokio::sync::mpsc::channel(8);
    let result = executor.execute(job, out).await;
    assert!(
        matches!(&result, Err(RunError::Config(message)) if message.contains("prompt history read failed")),
        "the executor returns the read failure: {:?}",
        result.as_ref().err()
    );
    match admitted.await {
        Ok(Err(RunError::Config(message))) => assert!(
            message.contains("prompt history read failed"),
            "the ack carries the read failure: {message}"
        ),
        other => panic!("the admission ack must carry the Config refusal, got {other:?}"),
    }
    drop(executor);
    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

/// Resolve a ChatGPT (codex) episode for `openai/gpt-5.3-codex` the way the executor does. On a
/// resume, `recorded` is the episode's RunConfig and each restored input is read from it through
/// the executor's own reader; `restore_text_state` turns off the provider-text-state restore so
/// the test can show what the comparison sees without it.
fn resolve_chatgpt_episode(
    recorded: Option<&broca_wal::RunConfig>,
    restore_text_state: bool,
) -> broca_catalog::live::ProviderResolution {
    install_test_catalog();
    let options = broca_catalog::live::ResolveOptions {
        auth_method: Some(AuthMethod::Chatgpt),
        family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
        session_id: Some("chatgpt-reproduce"),
        account_id: Some("acct-reproduce"),
        ..Default::default()
    };
    let auth = AuthResolution {
        mode: AuthMode::Secret("chatgpt-test-token".into()),
        pause: None,
        selection: None,
        method: Some(AuthMethod::Chatgpt),
        project_id: None,
        account_id: Some("acct-reproduce".into()),
        record_version: None,
        in_run_refresh: None,
    };
    resolve_provider_for_episode(
        "openai/gpt-5.3-codex",
        None,
        Some(2048),
        &auth,
        &options,
        broca_provider::build_provider_http_client(),
        broca_catalog::live::EpisodeRenderConfig {
            max_output_tokens: recorded.and_then(frozen_max_output_tokens),
            responses_tools_non_strict: recorded.map(frozen_responses_tools_non_strict),
            chatgpt_effort_updates: recorded.map(|config| {
                config.frozen_render_config["chatgpt_effort_updates"]
                    .as_bool()
                    .unwrap_or(false)
            }),
            provider_text_state: recorded
                .filter(|_| restore_text_state)
                .map(frozen_provider_text_state),
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| panic!("ChatGPT resolution: {error}"))
}

/// The `RunConfig` that a release from before `provider_text_state` existed recorded for a
/// ChatGPT run: the config the current code records for the same admission, with
/// `renderer_version` set to the given earlier version and no `provider_text_state` field.
fn chatgpt_run_recorded_before_text_state(
    root: &std::path::Path,
    renderer_version: u32,
) -> broca_wal::RunConfig {
    let started = resolve_chatgpt_episode(None, true);
    assert_eq!(
        started.provider.renderer_version(),
        broca_provider::families::OPENAI_RESPONSES_CHATGPT_RENDERER_VERSION
    );
    let mut recorded = drift_run_config(&drift_request(root), &started);
    assert_eq!(recorded.frozen_render_config["provider_text_state"], true);
    let blob = recorded.frozen_render_config.as_object_mut().unwrap();
    blob.remove("provider_text_state");
    blob.insert("renderer_version".into(), renderer_version.into());
    recorded.renderer_version = renderer_version;
    recorded
}

/// A ChatGPT run admitted at renderer version 7, before provider text state existed, resumes
/// under the current code (version 8, which declares 7 reproducible) and completes, rendering
/// what its recorded config renders. Without restoring the recorded absence of the flag the
/// resolved config differs from the recorded one in exactly that flag, which would change the
/// bytes, so the comparison must not tolerate it.
#[tokio::test]
async fn chatgpt_run_recorded_at_version_7_resumes_under_version_8() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-chatgpt-v7-resume-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let recorded = chatgpt_run_recorded_before_text_state(&root, 7);

    // How core's frozen-config comparison sees the version-7 recorded config. When the resume
    // re-resolves the provider without restoring the recorded (absent) `provider_text_state`,
    // the provider's config gains `provider_text_state: true`. Its blob for version 7
    // (`render_config_blob_for_version(7)`) differs from the recorded blob in that field only;
    // its plain blob (`render_config_blob()`) also carries version 8.
    let unrestored = resolve_chatgpt_episode(Some(&recorded), false);
    let differing = |blob: serde_json::Value| {
        let mut blob = blob.as_object().unwrap().clone();
        blob.retain(|key, value| recorded.frozen_render_config.get(key) != Some(&*value));
        serde_json::Value::Object(blob)
    };
    assert_eq!(
        differing(unrestored.provider.render_config_blob_for_version(7)),
        serde_json::json!({"provider_text_state": true})
    );
    assert_eq!(
        differing(unrestored.provider.render_config_blob()),
        serde_json::json!({"provider_text_state": true, "renderer_version": 8})
    );

    let resumed = resolve_chatgpt_episode(Some(&recorded), true);
    assert_eq!(resumed.provider.renderer_version(), 8);
    assert!(resumed.provider.reproduces_renderer_version(7));
    assert_eq!(
        resumed.provider.render_config_blob_for_version(7),
        recorded.frozen_render_config
    );
    // The resumed provider renders what the recorded config renders, for a prompt whose
    // assistant text came from the run's own model.
    let model = broca_protocol::request::ModelHandle {
        provider_module_id: "openai".into(),
        model_id: "gpt-5.3-codex".into(),
    };
    let mut opts = broca_protocol::CallOptions::new(vec![
        broca_protocol::content::Message::new(
            broca_protocol::content::Role::User,
            vec![broca_protocol::ContentBlock::Text {
                text: "question".into(),
            }],
        ),
        broca_protocol::content::Message::assistant_from(
            vec![broca_protocol::ContentBlock::Text {
                text: "answer".into(),
            }],
            model.clone(),
        ),
    ]);
    opts.target_model = Some(model);
    let recorded_config: broca_provider::wire::FrozenRenderConfig =
        serde_json::from_value(recorded.frozen_render_config.clone()).unwrap();
    assert!(!recorded_config.provider_text_state);
    assert_eq!(
        resumed.provider.render_request(&opts),
        broca_provider::wire::WireFamily::render(
            &broca_provider::families::OpenAiResponsesChatgpt,
            &recorded_config,
            &opts
        )
    );

    let (codes, reason, calls) = resume_through_core(&root, recorded, resumed).await;
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(codes, Vec::<Option<String>>::new());
    assert_eq!(reason, broca_protocol::caller::RunFinishReason::Completed);
    assert_eq!(calls, 1);
}

/// The ChatGPT family does not declare version 6 reproducible, so a run recorded at it is still
/// refused with renderer_version_drift and makes no provider call.
#[tokio::test]
async fn chatgpt_run_recorded_at_undeclared_version_6_is_refused() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-chatgpt-v6-resume-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let recorded = chatgpt_run_recorded_before_text_state(&root, 6);
    let resumed = resolve_chatgpt_episode(Some(&recorded), true);
    assert!(!resumed.provider.reproduces_renderer_version(6));
    let (codes, reason, calls) = resume_through_core(&root, recorded, resumed).await;
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(codes, vec![Some("renderer_version_drift".to_string())]);
    assert_eq!(reason, broca_protocol::caller::RunFinishReason::Interrupted);
    assert_eq!(calls, 0);
}

#[test]
fn frozen_provider_text_state_reads_an_absent_field_as_false() {
    let mut recorded = drift_run_config(
        &drift_request(std::path::Path::new("project")),
        &resolve_chatgpt_episode(None, true),
    );
    assert!(frozen_provider_text_state(&recorded));
    recorded
        .frozen_render_config
        .as_object_mut()
        .unwrap()
        .remove("provider_text_state");
    assert!(!frozen_provider_text_state(&recorded));
}

#[tokio::test]
async fn chatgpt_profile_resumes_through_provider_and_core_without_host_discovery() {
    use broca_core::Provider;
    let route = DeclaredRoute {
        family: WireFamilyId::OpenAiResponsesChatgpt,
        model: "openai/gpt-5.3-codex",
        method: AuthMethod::Chatgpt,
        family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
        project_id: None,
    };
    for legacy in [false, true] {
        let root = broca_test_support::scratch_root().join(format!("chatgpt-profile-resume-{}-{legacy}", std::process::id()));
        let request = drift_request(&root);
        let wire = if legacy { None } else {
            broca_module_serve::chatgpt_identity::for_episode(route.family_override, None, || {
                broca_module_serve::chatgpt_identity::fresh("persistent-seed", &request.session, "admitted-run", 123)
            }).unwrap()
        };
        let started = resolve_declared_route(&route, broca_catalog::live::EpisodeRenderConfig { chatgpt_wire: wire.as_ref(), ..Default::default() });
        let recorded = drift_run_config(&request, &started);
        let options = broca_protocol::CallOptions::new(request.input.clone());
        let canonical = started.provider.render_request(&options);
        let restored = broca_module_serve::chatgpt_identity::for_episode(route.family_override, Some(&recorded.frozen_render_config), || Err("changed host must not be discovered".into())).unwrap();
        assert_eq!(restored, wire);
        let resumed = resolve_declared_route(&route, broca_catalog::live::EpisodeRenderConfig { chatgpt_wire: restored.as_ref(), ..restored_episode_render(&recorded, None) });
        assert_eq!(resumed.provider.render_config_blob(), recorded.frozen_render_config);
        assert_eq!(resumed.provider.render_request(&options), canonical);
        let (codes, reason, calls) = resume_through_core(&root, recorded, resumed).await;
        assert!(codes.is_empty());
        assert_eq!(reason, broca_protocol::caller::RunFinishReason::Completed);
        assert_eq!(calls, 1);
        let _ = std::fs::remove_dir_all(root);
    }
}

/// The render inputs the executor restores when it resumes `recorded`, each read through the
/// executor's own reader, as the episode admission in this file's executor builds them. The
/// variant and structured output are left out: the recorded configs below carry neither.
fn restored_episode_render<'a>(
    recorded: &'a broca_wal::RunConfig,
    codeassist_identity: Option<&'a broca_provider::wire::CodeAssistIdentity>,
) -> broca_catalog::live::EpisodeRenderConfig<'a> {
    broca_catalog::live::EpisodeRenderConfig {
        codeassist_identity,
        chatgpt_wire: None,
        base_url: recorded
            .frozen_render_config
            .get("base_url")
            .and_then(|value| value.as_str()),
        max_output_tokens: frozen_max_output_tokens(recorded),
        variant: None,
        service_tier: frozen_service_tier(recorded),
        thinking_echo_scope: frozen_thinking_echo_scope(recorded),
        thinking_cross_model: frozen_thinking_cross_model(recorded),
        chatgpt_effort_updates: Some(
            recorded.frozen_render_config["chatgpt_effort_updates"]
                .as_bool()
                .unwrap_or(false),
        ),
        reasoning_content_required: Some(frozen_reasoning_content_required(recorded)),
        reasoning_echo: Some(frozen_reasoning_echo(recorded)),
        provider_text_state: Some(frozen_provider_text_state(recorded)),
        responses_tools_non_strict: Some(frozen_responses_tools_non_strict(recorded)),
        structured_output: None,
        claude_code_persona: broca_catalog::live::EpisodePersona::Recorded(None),
        ..Default::default()
    }
}

/// One catalog route for each wire family that declares an earlier renderer version
/// reproducible: the model, the auth method and the family it selects.
struct DeclaredRoute {
    family: WireFamilyId,
    model: &'static str,
    method: AuthMethod,
    family_override: Option<WireFamilyId>,
    project_id: Option<&'static str>,
}

fn resolve_declared_route(
    route: &DeclaredRoute,
    render: broca_catalog::live::EpisodeRenderConfig<'_>,
) -> broca_catalog::live::ProviderResolution {
    install_test_catalog();
    let options = broca_catalog::live::ResolveOptions {
        auth_method: Some(route.method),
        family_override: route.family_override,
        project_id: route.project_id,
        session_id: Some("declared-reproduce"),
        account_id: Some("acct-reproduce"),
        ..Default::default()
    };
    let auth = AuthResolution {
        mode: AuthMode::Secret("declared-test-token".into()),
        pause: None,
        selection: None,
        method: Some(route.method),
        project_id: route.project_id.map(str::to_owned),
        account_id: Some("acct-reproduce".into()),
        record_version: None,
        in_run_refresh: None,
    };
    let resolved = resolve_provider_for_episode(
        route.model,
        None,
        Some(2048),
        &auth,
        &options,
        broca_provider::build_provider_http_client(),
        render,
    )
    .unwrap_or_else(|error| panic!("{} resolution: {error}", route.model));
    assert_eq!(resolved.family_id, route.family, "{}", route.model);
    resolved
}

/// Every declared reproducible renderer version resumes through the executor and core.
/// Reconstruct the fields that version could have recorded, then require exact frozen
/// blob equality as well as a completed run, so ABI bumps cannot silently lose an older lane.
#[tokio::test]
async fn every_declared_previous_version_resumes_through_the_executor_restore_path() {
    use broca_provider::families::{
        earlier_renderer_versions, renderer_version_for, EarlierRendererVersion,
    };
    let routes = [
        DeclaredRoute {
            family: WireFamilyId::OpenAiResponses,
            model: "openai/gpt-5.1",
            method: AuthMethod::ApiKey,
            family_override: None,
            project_id: None,
        },
        DeclaredRoute {
            family: WireFamilyId::OpenAiResponsesChatgpt,
            model: "openai/gpt-5.3-codex",
            method: AuthMethod::Chatgpt,
            family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
            project_id: None,
        },
        DeclaredRoute {
            family: WireFamilyId::Gemini,
            model: "google/gemini-2.5-flash",
            method: AuthMethod::ApiKey,
            family_override: None,
            project_id: None,
        },
        DeclaredRoute {
            family: WireFamilyId::GoogleCodeAssist,
            model: "google/gemini-2.5-flash",
            method: AuthMethod::Antigravity,
            family_override: Some(WireFamilyId::GoogleCodeAssist),
            project_id: Some("test-project"),
        },
    ];
    for route in &routes {
        let declared: Vec<u32> = earlier_renderer_versions(route.family)
            .iter()
            .filter(|(_, disposition)| *disposition == EarlierRendererVersion::Reproducible)
            .map(|(version, _)| *version)
            .collect();
        let expected = if route.family == WireFamilyId::GoogleCodeAssist { vec![5, 6] } else { vec![renderer_version_for(route.family) - 1] };
        assert_eq!(declared, expected, "{:?}", route.family);
        for previous in declared {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-declared-resume-{:?}-{previous}-{}",
            route.family,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let request = drift_request(&root);

        let fresh_identity =
            broca_module_serve::codeassist_identity::for_episode(&request.session, None, 0, 0)
                .unwrap();
        // This route represents an older admitted Code Assist run, not a fresh catalog send.
        // Its recorded backend id must remain reproducible even without a current tier row.
        let historical = (route.family == WireFamilyId::GoogleCodeAssist).then(|| {
            let mut config = broca_provider::families::google_codeassist_frozen_config("gemini-2.5-flash");
            config.project_id = route.project_id.map(str::to_owned);
            config.max_output_tokens = 2048;
            config.caller_max_output_tokens = Some(2048);
            config.codeassist_identity = fresh_identity.clone();
            config
        });
        let started = resolve_declared_route(
            route,
            broca_catalog::live::EpisodeRenderConfig {
                recorded_config: historical.as_ref(),
                codeassist_identity: fresh_identity.as_ref(),
                ..Default::default()
            },
        );
        let mut recorded = drift_run_config(&request, &started);
        let blob = recorded.frozen_render_config.as_object_mut().unwrap();
        let predates_text_state = route.family != WireFamilyId::GoogleCodeAssist || previous < 6;
        if predates_text_state {
            assert_eq!(blob.remove("provider_text_state"), Some(true.into()));
            assert_eq!(blob.remove("codeassist_identity").is_some(), route.family == WireFamilyId::GoogleCodeAssist, "{:?}", route.family);
        }
        blob.insert("renderer_version".into(), previous.into());
        recorded.renderer_version = previous;

        let identity = broca_module_serve::codeassist_identity::for_episode(
            &request.session,
            Some(&recorded.frozen_render_config),
            0,
            0,
        )
        .unwrap();
        assert_eq!(identity.is_none(), predates_text_state);
        let frozen = decode_recorded_render_config(&recorded).unwrap();
        let resumed = resolve_declared_route(route, broca_catalog::live::EpisodeRenderConfig {
            recorded_config: Some(&frozen),
            ..restored_episode_render(&recorded, identity.as_ref())
        });
        assert_eq!(resumed.provider.renderer_version(), renderer_version_for(route.family), "resume provider must identify the current implementation");
        assert!(resumed.provider.reproduces_renderer_version(previous));
        let stated = resumed.provider.render_config_blob_for_version(previous);
        assert_eq!(stated, recorded.frozen_render_config, "{:?}", route.family);
        for key in [
            "provider_text_state",
            "codeassist_identity",
            "reasoning_echo",
            "claude_code_persona",
        ] {
            if !predates_text_state && matches!(key, "provider_text_state" | "codeassist_identity") { continue; }
            assert!(stated.get(key).is_none(), "{:?} states {key}", route.family);
        }

        let (codes, reason, calls) = resume_through_core(&root, recorded, resumed).await;
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(codes, Vec::<Option<String>>::new(), "{:?}", route.family);
        assert_eq!(
            reason,
            broca_protocol::caller::RunFinishReason::Completed,
            "{:?}",
            route.family
        );
        assert_eq!(calls, 1, "{:?}", route.family);
        }
    }
}

// These readers construct historical partial-config fixtures. Production resume decodes
// the complete recorded bundle instead of rebuilding it from individual fields.
fn fixture_field<T: serde::de::DeserializeOwned>(config: &broca_wal::RunConfig, key: &str) -> Option<T> {
    serde_json::from_value(config.frozen_render_config.get(key)?.clone()).ok()
}
fn frozen_max_output_tokens(config: &broca_wal::RunConfig) -> Option<u32> { fixture_field(config, "max_output_tokens") }
fn frozen_variant(config: &broca_wal::RunConfig) -> Option<broca_catalog::live::FrozenVariant> { fixture_field(config, "variant") }
fn frozen_thinking_echo_scope(config: &broca_wal::RunConfig) -> Option<broca_catalog::live::ThinkingEchoScope> {
    match fixture_field(config, "reasoning")? {
        broca_provider::policy::ReasoningPolicy::AnthropicInterleaved { echo_scope, .. } => Some(echo_scope),
        _ => None,
    }
}
fn frozen_thinking_cross_model(config: &broca_wal::RunConfig) -> Option<broca_catalog::live::CrossModelThinking> {
    match fixture_field(config, "reasoning")? {
        broca_provider::policy::ReasoningPolicy::AnthropicInterleaved { cross_model, .. } => Some(cross_model),
        _ => None,
    }
}
fn frozen_claude_code_persona(config: &broca_wal::RunConfig) -> Option<broca_catalog::live::ClaudeCodePersona> { fixture_field(config, "claude_code_persona") }
fn frozen_responses_tools_non_strict(config: &broca_wal::RunConfig) -> bool { fixture_field(config, "responses_tools_non_strict").unwrap_or(false) }
fn frozen_reasoning_content_required(config: &broca_wal::RunConfig) -> bool { fixture_field(config, "reasoning_content_required").unwrap_or(false) }
fn frozen_reasoning_echo(config: &broca_wal::RunConfig) -> Option<broca_provider::policy::ReasoningEchoPolicy> { fixture_field(config, "reasoning_echo") }
fn frozen_provider_text_state(config: &broca_wal::RunConfig) -> bool { fixture_field(config, "provider_text_state").unwrap_or(false) }
fn frozen_service_tier(config: &broca_wal::RunConfig) -> Option<broca_catalog::live::ServiceTier> { config.service_tier }
