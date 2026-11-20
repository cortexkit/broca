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
    let _catalog = install_test_catalog();
    let resolution = AuthResolution {
        credential_hosts: Default::default(),
        mode: AuthMode::Secret("test-api-key".into()),
        pause: None,
        selection: Some(broca_core::AuthSelection {
            billing_vault_facts: None,
            auth_method: None,
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
/// the resolved secret lands, so it proves the vault-served credential reached the wire.
/// `None` when resolution fails (no usable credential) or no request arrives.
async fn read_vault_authorization(auth: &AuthResolution) -> Option<String> {
    use tokio::io::AsyncReadExt;
    let _catalog = install_test_catalog();
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
    // The capturing vault serves a synthetic token over the real subc credential route.
    let root = fresh_vault_root("host-override");
    let daemon = CapturingVaultDaemon::start(&root).await;
    let mut executor = executor_with_capturing_vault(&root, &daemon)
        .await
        .with_provider_overrides(runtime_overrides("http://127.0.0.1:4010/v1"));
    let auth = executor
        .decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None)
        .await
        .unwrap();
    assert!(matches!(&auth.mode, AuthMode::Secret(_)));
    // A failed equality assertion must not reveal a credential header.
    let authorization = read_vault_authorization(&auth).await;
    assert!(
        authorization.as_deref() == Some(format!("Bearer {}", stand_in_token(1)).as_str()),
        "the resolved credential must come from the stand-in vault (request carried an \
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
    let _catalog = install_test_catalog();
    let mut req = broca_core::RunRequest { route_decision_id: None, account_route: None,
    input_written_at_ms: None, directives: None, session_class: None, retention: None, title: None, on_restart: None, keep_warm: None, send_id: None, request_identity_digest: None,
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
        inherit_frozen_cache: false, structured_output: None,
    head_turn: false,
    };
    assert!(executor
        .cache_policy_for_episode(true, None, &req, req.model.provider_module_id.as_str(), None)
        .unwrap()
        .is_none());
    // DeepSeek caches automatically. Switch to the overridden Anthropic provider and
    // request cache markers explicitly to verify that mock routing does not disable them.
    req.model.provider_module_id = "anthropic".into();
    req.cache = Some({
        let mut cache = cortexkit_role_llm_runner::cache::Cache::default();
        cache.ttl = Some(cortexkit_role_llm_runner::cache::Ttl::Short);
        cache
    });
    assert!(executor
        .cache_policy_for_episode(true, Some(WireFamilyId::AnthropicMessages), &req, req.model.provider_module_id.as_str(), None)
        .unwrap()
        .is_some());
    assert_resume_replays_the_admitted_cache_policy(&executor, req.clone());
    // Admission without account routing freezes the listed vault id even when the token reply omits it.
    assert_eq!(auth.selection.as_ref().unwrap().resolved_credential_id.as_deref(), Some("apikey:deepseek"));
    executor.provider_base_url = Some("http://127.0.0.1:4010/v1".into());
    let mock = executor.decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None).await.unwrap();
    assert!(mock.selection.is_none(), "mock auth has no vault witness, vault request, or selection");
    drop(executor);
    daemon.assert_healthy();
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
        .cache_policy_for_episode(true, family, &req, req.model.provider_module_id.as_str(), None)
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
            moved: false,
            records_account_change: false,
        }
    };

    assert_eq!(
        executor
            .cache_policy_for_episode(false, family, &req, req.model.provider_module_id.as_str(), Some(&admitted_with(None)))
            .unwrap(),
        None,
        "a run admitted before the default froze no policy and resumes with no markers"
    );
    assert_eq!(
        executor
            .cache_policy_for_episode(false, family, &req, req.model.provider_module_id.as_str(), Some(&admitted_with(fresh.clone())))
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
fn install_drift_revision(reasoning: bool, output: u64) -> TestCatalog {
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
    install_catalog(cache)
}

fn drift_request(root: &std::path::Path) -> broca_core::RunRequest {
    broca_core::RunRequest { route_decision_id: None, account_route: None,
    input_written_at_ms: None, directives: None, session_class: None, retention: None, title: None, on_restart: None, keep_warm: None, send_id: None, request_identity_digest: None,
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
        inherit_frozen_cache: false, structured_output: None,
    head_turn: false,
    }
}

// Keep production rendering/config identity; replace only network IO. A masked drift
// guard would reach this step and complete, making the false acceptance observable.
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
    let _catalog = install_catalog(cache);

    let auth = AuthResolution {
        credential_hosts: Default::default(),
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
                    provider_state: None,
                    tool_call_id: ToolCallId("call_0".into()),
                    tool_name: "read".into(),
                    input: serde_json::json!({}).into(),
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
    let (plane, server) = stand_in_plane(root).await;
    let executor = LiveLoopExecutor::new(
        Arc::new(FileWalStore::new(root.join("wal"))),
        plane,
        None,
    );
    (executor, server)
}

async fn stand_in_plane(root: &std::path::Path) -> (Arc<SubcToolPlane>, tokio::task::JoinHandle<()>) {
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
    (Arc::new(SubcToolPlane::new(connection, "unused")), server)
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

// These readers construct historical partial-config fixtures. Production resume decodes
// the complete recorded bundle instead of rebuilding it from individual fields.
