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
            // As the executor restores it: a recorded blob without the field reads as false.
            reasoning_content_required: frozen.map(|recorded| {
                recorded
                    .get("reasoning_content_required")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
            }),
            structured_output: None,
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
            claude_code_persona: Default::default(),
            base_url: Some(&base_url),
            max_output_tokens: None,
            variant: None,
            service_tier: None,
            thinking_echo_scope: None,
            thinking_cross_model: None,
            responses_tools_non_strict: None,
            chatgpt_effort_updates: None,
            reasoning_content_required: None,
            structured_output: None,
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
    let mut req = broca_core::RunRequest { session_class: None, retention: None, title: None, on_restart: None, keep_warm: None, send_id: None, request_identity_digest: None,
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
/// RunConfig on a resume, and both restored inputs (host and output cap) come from it through
/// the executor's own reader, so the test cannot restore something production does not.
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
            reasoning_content_required: None,
            structured_output: None,
        },
    )
    .unwrap_or_else(|error| panic!("catalog resolution: {error}"))
}

fn drift_request(root: &std::path::Path) -> broca_core::RunRequest {
    broca_core::RunRequest { session_class: None, retention: None, title: None, on_restart: None, keep_warm: None, send_id: None, request_identity_digest: None,
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
    broca_wal::RunConfig {
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
        context_management: None,
    }
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
        &broca_wal::WalRecord::RunStarted { session_class: None, retention: None,
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

/// The control for the output-cap restore: a catalog-owned field OTHER than the output limit
/// (reasoning) moved between start and resume, and the resume is still refused. Run with no
/// caller cap so the restored cap is in play, which proves the restore does not mask drift in
/// the rest of the frozen config.
#[tokio::test]
async fn runtime_override_catalog_drift_refuses_resume() {
    if !run_catalog_drift_child("runtime_override_catalog_drift_refuses_resume") {
        return;
    }
    let root = broca_test_support::scratch_root()
        .join(format!("broca-catalog-drift-{}", std::process::id()));
    install_drift_revision(true, 8192);
    let started = resolve_drift_revision(None, None);
    let recorded = drift_run_config(&drift_request(&root), &started);
    install_drift_revision(false, 8192);
    let resumed = resolve_drift_revision(Some(&recorded), None);
    assert_eq!(
        started.provider.renderer_version(),
        resumed.provider.renderer_version()
    );
    assert_eq!(resumed.base_url, "http://host-a.example");

    let (codes, reason, calls) = resume_through_core(&root, recorded, resumed).await;
    eprintln!("resume outcome: {reason:?}; provider_codes={codes:?}; provider_calls={calls}");
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        codes
            .iter()
            .any(|code| code.as_deref() == Some("frozen_render_config_drift")),
        "expected catalog-drift refusal, got {codes:?}; outcome={reason:?}; calls={calls}"
    );
    assert_eq!(reason, broca_protocol::caller::RunFinishReason::Interrupted);
    assert_eq!(calls, 0);
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
            claude_code_persona: Default::default(),
            base_url: Some("http://host-a.example"),
            max_output_tokens: recorded.and_then(frozen_max_output_tokens),
            variant: None,
            service_tier: recorded.and_then(frozen_service_tier),
            thinking_echo_scope: None,
            thinking_cross_model: None,
            responses_tools_non_strict: None,
            chatgpt_effort_updates: None,
            reasoning_content_required: None,
            structured_output: None,
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
            claude_code_persona: Default::default(),
            base_url: Some("http://host-a.example"),
            max_output_tokens: recorded.and_then(frozen_max_output_tokens),
            variant: restored.as_ref(),
            service_tier: None,
            thinking_echo_scope: None,
            thinking_cross_model: None,
            responses_tools_non_strict: None,
            chatgpt_effort_updates: None,
            reasoning_content_required: None,
            structured_output: None,
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
    assert_eq!(fresh_blob["reasoning_content_required"], true);
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
        .remove("reasoning_content_required");
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
    let tool = |n| broca_wal::WalRecord::ToolResult {
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
