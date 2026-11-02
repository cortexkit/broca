// Exercise the render boundary with real HTTP normalization, core dispatch and session reads.

async fn alias_http_fixture(reply_name: &str, pause_first: bool) -> ResumeHttpFixture {
    alias_http_fixture_with_sibling(reply_name, pause_first, None).await
}

async fn alias_http_fixture_with_sibling(
    reply_name: &str,
    pause_first: bool,
    sibling: Option<&str>,
) -> ResumeHttpFixture {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let reply_name = reply_name.to_owned();
    let sibling = sibling.map(str::to_owned);
    let task = tokio::spawn(async move {
        for index in 0..(2 + usize::from(pause_first)) {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            let split = loop {
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(at) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    break at + 4;
                }
            };
            let headers = String::from_utf8(bytes[..split].to_vec()).unwrap();
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            while bytes.len() < split + length {
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            captured.lock().unwrap().push(CapturedResumeRequest {
                headers,
                body: bytes[split..split + length].to_vec(),
            });
            let (status, body) = if pause_first && index == 0 {
                (
                    "401 Unauthorized",
                    "{\"error\":{\"message\":\"reauthenticate\"}}".to_owned(),
                )
            } else {
                let call = index == usize::from(pause_first);
                let delta = if call {
                    let mut calls = vec![
                        serde_json::json!({"index":0,"id":"alias-call","type":"function","function":{"name":reply_name,"arguments":"{}"}}),
                    ];
                    if let Some(sibling) = &sibling {
                        calls.push(serde_json::json!({"index":1,"id":"alias-sibling","type":"function","function":{"name":sibling,"arguments":"{}"}}));
                    }
                    serde_json::json!({"tool_calls":calls})
                } else {
                    serde_json::json!({"content":"done"})
                };
                let first = serde_json::json!({"id":"alias","choices":[{"index":0,"delta":delta,"finish_reason":null}]});
                let last = serde_json::json!({"id":"alias","choices":[{"index":0,"delta":{},"finish_reason":if call {"tool_calls"} else {"stop"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}});
                (
                    "200 OK",
                    format!("data: {first}\n\ndata: {last}\n\ndata: [DONE]\n\n"),
                )
            };
            let response = format!("HTTP/1.1 {status}\r\nContent-Type: text/event-stream\r\nx-should-retry: false\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    ResumeHttpFixture {
        url,
        requests,
        task,
    }
}

fn alias_tools() -> Vec<broca_protocol::request::Tool> {
    ["bash", "read_file"]
        .into_iter()
        .map(|name| {
            serde_json::from_value(serde_json::json!({
                "name":name,"description":"synthetic","input_schema":{"type":"object"}
            }))
            .unwrap()
        })
        .collect()
}

struct AliasReadOnlyExecutor;
#[async_trait]
impl RunExecutor for AliasReadOnlyExecutor {
    async fn execute(
        &self,
        _: RunJob,
        _: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        panic!("reading an existing WAL never starts a run")
    }
}

fn alias_submit_request(root: &std::path::Path) -> broca_core::RunRequest {
    let mut req = drift_request(root);
    req.model = broca_protocol::ModelHandle::new("deepseek", "alias-model");
    req.tools = alias_tools();
    req.policy_allowed = req.tools.iter().map(|tool| tool.name.clone()).collect();
    req.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text { text: "run".into() }],
    )];
    req.structured_output = Some(
        serde_json::from_value(serde_json::json!({
            "mode":"submit_tool","schema":{"type":"object"}
        }))
        .unwrap(),
    );
    req
}

fn install_alias_catalog() {
    let catalog = broca_catalog::Catalog::from_json_str(
        &serde_json::json!({"deepseek":{
            "npm":"@ai-sdk/openai-compatible",
            "models":{"alias-model":{
                "tool_call":true,"limit":{"context":200000,"output":4096}
            }}
        }})
        .to_string(),
        "{}",
    )
    .unwrap();
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache.install_parsed_for_test(&catalog, 0, broca_catalog::CatalogSource::Fusiform);
    broca_catalog::install_global_catalog_cache(cache);
}

async fn execute_alias_job(
    executor: &LiveLoopExecutor,
    req: broca_core::RunRequest,
) -> Result<RunSummary, RunError> {
    let writer = Arc::new(
        broca_core::WalHandle::spawn(executor.wal_store.open(&req.session).unwrap(), 1).unwrap(),
    );
    let prior_snapshot = broca_core::load_prior_wal_snapshot(
        executor.wal_store.as_ref(),
        &req.session,
        writer.durable_head().load(Ordering::Acquire),
        None::<&broca_store::SqliteStore>,
    )
    .unwrap();
    broca_core::run::seed_display_placement(
        &writer,
        &prior_snapshot,
        executor.wal_store.as_ref(),
        None,
        &req.session,
    );
    let job = RunJob {
        queued_service_tier: None,
        media: vec![],
        media_profile: None,
        req,
        auth: None,
        conversation_key: None,
        writer: writer.clone(),
        kind: RunKind::FreshOrResume,
        skip_materialization: true,
        control: Default::default(),
        prior_snapshot,
        on_admitted: None,
    };
    let (out, mut events) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while events.recv().await.is_some() {} });
    let result = tokio::time::timeout(Duration::from_secs(15), executor.execute(job, out))
        .await
        .expect("aliased submit run terminates");
    collector.await.unwrap();
    Arc::try_unwrap(writer)
        .unwrap_or_else(|_| panic!("executor released writer"))
        .shutdown()
        .await
        .unwrap();
    result
}

async fn assert_submit_alias_round_trip(
    form: serde_json::Value,
    caller_name: &str,
    submit_name: &str,
) -> Vec<u8> {
    use broca_core::WalStore as _;
    use broca_module::ServeHandler as _;
    let root = broca_test_support::scratch_root().join(format!(
        "submit-alias-{caller_name}-{submit_name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let server = alias_http_fixture_with_sibling(submit_name, false, Some(caller_name)).await;
    let vault = CapturingVaultDaemon::start(&root).await;
    let config = if form.is_null() {
        serde_json::json!({})
    } else {
        serde_json::json!({"tool_name_aliases":{"apikey:deepseek":form}})
    };
    let executor = executor_with_capturing_vault(&root, &vault)
        .await
        .with_provider_overrides(runtime_overrides(&server.url))
        .with_tool_name_aliases(
            broca_module_serve::config::ToolNameAliasesConfig::parse(&config.to_string()).unwrap(),
        );
    let req = alias_submit_request(&root);
    let result = execute_alias_job(&executor, req.clone()).await;
    assert!(
        !server.requests.lock().unwrap().is_empty(),
        "provider must be called: {result:?}"
    );
    let bytes = server.requests.lock().unwrap()[0].body.clone();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["tools"][2]["function"]["name"], submit_name);
    assert_eq!(body["tools"][0]["function"]["name"], caller_name);
    assert_eq!(result.unwrap().reason, RunFinishReason::Completed);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    let records = executor.wal_store.read_records(&req.session).unwrap();
    assert!(!records.iter().any(|frame| matches!(
        frame.record,
        broca_wal::WalRecord::ToolDispatchIntent { .. }
    )));
    let state = broca_core::reconcile_run_state(
        &records
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
        &req.run_id.0,
    )
    .unwrap();
    let output = state.structured_output.unwrap();
    assert_eq!(output.validation, "passed");
    assert_eq!(output.value, Some(serde_json::json!({})));
    assert_eq!(
        output.skipped_siblings,
        Some(vec![
            broca_protocol::structured_output::SkippedSibling::new("alias-sibling", "bash")
        ])
    );
    let store = Arc::new(broca_store::SqliteStore::open(root.join("read.db")).unwrap());
    store.project(&req.session, &records).unwrap();
    let opener = broca_module::DefaultOpener::new(
        Arc::new(broca_core::FileLeaseStore::new(root.join("leases"))),
        executor.wal_store.clone(),
        Arc::new(AliasReadOnlyExecutor),
    )
    .with_materialized_store(store.clone());
    let registry = broca_module::LineageRegistry::new_with_reader(
        Arc::new(opener),
        executor.wal_store.clone(),
        Some(store),
    );
    let mut read = broca_module::SessionReadRequest::new(req.session, Some(0), Some(100));
    read.include_tools = true;
    let page = registry.read(read).await.unwrap();
    let names: Vec<_> = page
        .messages
        .iter()
        .flat_map(|message| message.message.content.iter())
        .filter_map(|block| match block {
            broca_protocol::ContentBlock::ToolCall(call) => Some(call.tool_name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(names, ["submit_result", "bash"]);
    assert!(!serde_json::to_string(&page).unwrap().contains("client_"));
    assert!(registry.stop_and_seal().await);
    vault.assert_healthy();
    drop(executor);
    drop(vault);
    let _ = std::fs::remove_dir_all(root);
    bytes
}

#[tokio::test]
async fn tool_name_aliases_prefix_all_submit_tool_round_trip() {
    if !run_catalog_drift_child("tool_name_aliases_prefix_all_submit_tool_round_trip") {
        return;
    }
    install_alias_catalog();
    assert_submit_alias_round_trip(
        serde_json::json!({"prefix_all":"client_"}),
        "client_bash",
        "client_submit_result",
    )
    .await;
}

#[tokio::test]
async fn tool_name_aliases_names_submit_tool_round_trip() {
    if !run_catalog_drift_child("tool_name_aliases_names_submit_tool_round_trip") {
        return;
    }
    install_alias_catalog();
    assert_submit_alias_round_trip(
        serde_json::json!({"names":{"submit_result":"client_submit_result"}}),
        "bash",
        "client_submit_result",
    )
    .await;
}

#[tokio::test]
async fn tool_name_aliases_unconfigured_submit_tool_keeps_legacy_bytes() {
    if !run_catalog_drift_child("tool_name_aliases_unconfigured_submit_tool_keeps_legacy_bytes") {
        return;
    }
    install_alias_catalog();
    let bytes =
        assert_submit_alias_round_trip(serde_json::Value::Null, "bash", "submit_result").await;
    // These are the complete request bytes without operator aliases, not a re-rendered expectation.
    let legacy = r#"{"max_tokens":4096,"messages":[{"content":"run","role":"user"}],"model":"alias-model","stream":true,"stream_options":{"include_usage":true},"tool_choice":"auto","tools":[{"function":{"description":"synthetic","name":"bash","parameters":{"type":"object"}},"type":"function"},{"function":{"description":"synthetic","name":"read_file","parameters":{"type":"object"}},"type":"function"},{"function":{"description":"Submit the final result of the task. Calling this tool ends the task: call it exactly once, when the work is done, with the result as its input.","name":"submit_result","parameters":{"type":"object"}},"type":"function"}]}"#;
    assert_eq!(bytes, legacy.as_bytes());
}

#[tokio::test]
async fn tool_name_aliases_legacy_submit_tool_resume_keeps_bytes() {
    if !run_catalog_drift_child("tool_name_aliases_legacy_submit_tool_resume_keeps_bytes") {
        return;
    }
    assert_frozen_submit_resume(true).await;
}

#[tokio::test]
async fn tool_name_aliases_frozen_submit_tool_resume_keeps_bytes() {
    if !run_catalog_drift_child("tool_name_aliases_frozen_submit_tool_resume_keeps_bytes") {
        return;
    }
    assert_frozen_submit_resume(false).await;
}

async fn assert_frozen_submit_resume(legacy: bool) {
    use broca_core::WalStore as _;
    install_alias_catalog();
    let root = broca_test_support::scratch_root().join(format!(
        "submit-alias-resume-{legacy}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let submit_name = if legacy {
        "submit_result"
    } else {
        "client_submit_result"
    };
    let server = alias_http_fixture_with_sibling(submit_name, true, Some("client_bash")).await;
    let vault = CapturingVaultDaemon::start_handling_reports(&root, ReportScript::NotApplied).await;
    // This is the concrete map prefix_all froze when only caller tools were considered.
    let initial_config = if legacy {
        r#"{"tool_name_aliases":{"apikey:deepseek":{"names":{"bash":"client_bash","read_file":"client_read_file"}}}}"#
    } else {
        r#"{"tool_name_aliases":{"apikey:deepseek":{"prefix_all":"client_"}}}"#
    };
    let executor = executor_with_capturing_vault(&root, &vault)
        .await
        .with_provider_overrides(runtime_overrides(&server.url))
        .with_tool_name_aliases(
            broca_module_serve::config::ToolNameAliasesConfig::parse(initial_config).unwrap(),
        );
    let req = alias_submit_request(&root);
    let first = execute_alias_job(&executor, req.clone()).await.unwrap();
    assert!(
        first.paused.is_some(),
        "401 leaves a resumable structured-output run"
    );
    let frozen = broca_core::read_prior_run_config(executor.wal_store.as_ref(), &req.session)
        .unwrap()
        .unwrap()
        .frozen_render_config;
    let mut expected_aliases = serde_json::json!({
        "bash":"client_bash","read_file":"client_read_file"
    });
    if !legacy {
        expected_aliases["submit_result"] = serde_json::json!("client_submit_result");
    }
    assert_eq!(frozen["tool_name_aliases"], expected_aliases);
    let executor = executor.with_tool_name_aliases(
        broca_module_serve::config::ToolNameAliasesConfig::parse(if legacy {
            r#"{"tool_name_aliases":{"apikey:deepseek":{"prefix_all":"client_"}}}"#
        } else {
            "{}"
        })
        .unwrap(),
    );
    let done = execute_alias_job(&executor, req.clone()).await.unwrap();
    assert_eq!(done.reason, RunFinishReason::Completed);
    assert_eq!(
        broca_core::read_prior_run_config(executor.wal_store.as_ref(), &req.session)
            .unwrap()
            .unwrap()
            .frozen_render_config,
        frozen
    );
    {
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].body, requests[1].body);
        let body: serde_json::Value = serde_json::from_slice(&requests[1].body).unwrap();
        assert_eq!(body["tools"][2]["function"]["name"], submit_name);
    }
    let records = executor.wal_store.read_records(&req.session).unwrap();
    let state = broca_core::reconcile_run_state(
        &records
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
        &req.run_id.0,
    )
    .unwrap();
    assert_eq!(
        state.structured_output.unwrap().value,
        Some(serde_json::json!({}))
    );
    vault.assert_healthy();
    drop(executor);
    drop(vault);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn tool_name_aliases_dispatch_and_store_canonical_names_in_wal_and_session_read() {
    use broca_core::WalStore as _;
    use broca_module::ServeHandler as _;
    for (form, reply_name, pause) in [
        (
            serde_json::json!({"names":{"bash":"client_bash"}}),
            "client_bash",
            false,
        ),
        (
            serde_json::json!({"prefix_all":"client_"}),
            "client_bash",
            true,
        ),
        (
            serde_json::json!({"names":{"bash":"client_bash"}}),
            "bash",
            false,
        ),
    ] {
        let root = broca_test_support::scratch_root().join(format!(
            "tool-alias-{reply_name}-{pause}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let server = alias_http_fixture(reply_name, pause).await;
        let config = broca_module_serve::config::ToolNameAliasesConfig::parse(
            &serde_json::json!({"tool_name_aliases":{"apikey:synthetic":form}}).to_string(),
        )
        .unwrap();
        let mut req = drift_request(&root);
        req.model = broca_protocol::ModelHandle::new("synthetic", "alias-model");
        req.tools = alias_tools();
        req.policy_allowed = req.tools.iter().map(|tool| tool.name.clone()).collect();
        req.input = vec![broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text { text: "run".into() }],
        )];
        let aliases = config.for_tools("apikey:synthetic", &req.tools);
        let resolved = broca_catalog::live::resolve_provider(
            "synthetic/alias-model",
            Some(&server.url),
            2048,
            &AuthMode::None,
            &broca_catalog::live::ResolveOptions {
                tool_name_aliases: Some((&req.tools, &aliases)),
                ..Default::default()
            },
        )
        .unwrap();
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let leases = Arc::new(broca_core::FileLeaseStore::new(root.join("leases")));
        let plane = broca_core::StubToolPlane::new().with_canned_text("bash", "canonical result");
        let (out, mut events) = mpsc::channel(4096);
        let first = broca_core::run_loop(
            req.clone(),
            &resolved.provider,
            &plane,
            leases.as_ref(),
            &broca_core::AllowAll,
            wal.as_ref(),
            &out,
        )
        .await
        .unwrap();
        if pause {
            assert!(first.paused.is_some(), "401 must leave a resumable run");
            let recorded = broca_core::read_prior_run_config(wal.as_ref(), &req.session)
                .unwrap()
                .unwrap();
            let frozen: broca_provider::wire::FrozenRenderConfig =
                serde_json::from_value(recorded.frozen_render_config).unwrap();
            for changed in [
                serde_json::json!({"tool_name_aliases":{"apikey:synthetic":{"prefix_all":"changed_"}}}),
                serde_json::json!({}),
            ] {
                let changed =
                    broca_module_serve::config::ToolNameAliasesConfig::parse(&changed.to_string())
                        .unwrap();
                let changed_aliases = changed.for_tools("apikey:synthetic", &req.tools);
                let resumed = broca_catalog::live::resolve_catalog_episode_with_client(
                    "synthetic/alias-model",
                    Some(2048),
                    &AuthMode::None,
                    &broca_catalog::live::ResolveOptions {
                        tool_name_aliases: Some((&req.tools, &changed_aliases)),
                        ..Default::default()
                    },
                    broca_provider::transport::build_provider_http_client(),
                    broca_catalog::live::EpisodeRenderConfig {
                        recorded_config: Some(&frozen),
                        ..Default::default()
                    },
                )
                .unwrap();
                assert_eq!(
                    resumed.provider.render_config_blob(),
                    serde_json::to_value(&frozen).unwrap()
                );
                if changed_aliases.is_empty() {
                    let done = broca_core::run_loop_resume_after_auth(
                        req.clone(),
                        &resumed.provider,
                        &plane,
                        leases.as_ref(),
                        &broca_core::AllowAll,
                        wal.as_ref(),
                        None,
                        &out,
                    )
                    .await
                    .unwrap();
                    assert_eq!(done.reason, RunFinishReason::Completed);
                }
            }
        } else {
            assert_eq!(first.reason, RunFinishReason::Completed);
        }
        drop(out);
        while events.recv().await.is_some() {}
        assert_eq!(
            plane.exec_count(&broca_protocol::ToolCallId("alias-call".into())),
            1
        );
        {
            let requests = server.requests.lock().unwrap();
            if pause {
                assert_eq!(
                    requests[0].body, requests[1].body,
                    "resume must reproduce aliased bytes"
                );
            }
            let first_body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
            assert_eq!(first_body["tools"][0]["function"]["name"], "client_bash");
            assert_eq!(
                first_body["tools"][1]["function"]["name"],
                if pause {
                    "client_read_file"
                } else {
                    "read_file"
                }
            );
            let history: serde_json::Value =
                serde_json::from_slice(&requests.last().unwrap().body).unwrap();
            assert!(history["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| message["tool_calls"][0]["function"]["name"] == "client_bash"));
        }
        let records = wal.read_records(&req.session).unwrap();
        assert!(records.iter().any(|frame| matches!(&frame.record, broca_wal::WalRecord::ToolDispatchIntent { tool_name, .. } if tool_name.as_deref() == Some("bash"))));
        let store = Arc::new(broca_store::SqliteStore::open(root.join("read.db")).unwrap());
        store.project(&req.session, &records).unwrap();
        let opener =
            broca_module::DefaultOpener::new(leases, wal.clone(), Arc::new(AliasReadOnlyExecutor))
                .with_materialized_store(store.clone());
        let registry = broca_module::LineageRegistry::new_with_reader(
            Arc::new(opener),
            wal.clone(),
            Some(store),
        );
        let mut read =
            broca_module::SessionReadRequest::new(req.session.clone(), Some(0), Some(100));
        read.include_tools = true;
        let page = registry.read(read).await.unwrap();
        let calls: Vec<_> = page
            .messages
            .iter()
            .flat_map(|message| message.message.content.iter())
            .filter_map(|block| match block {
                broca_protocol::ContentBlock::ToolCall(call) => Some(call.tool_name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(calls, ["bash"]);
        let text = serde_json::to_string(&page).unwrap();
        assert!(!text.contains("client_bash"));
        assert!(registry.stop_and_seal().await);
        let _ = std::fs::remove_dir_all(root);
    }
}

#[tokio::test]
async fn tool_name_aliases_invalid_admissions_write_nothing() {
    if !run_catalog_drift_child("tool_name_aliases_invalid_admissions_write_nothing") {
        return;
    }
    install_test_catalog();
    for (index, form, colliding_tool) in [
        (0, serde_json::json!({"names":{"bash":"bad.name"}}), false),
        (1, serde_json::json!({"names":{"bash":"read_file"}}), false),
        (
            2,
            serde_json::json!({"names":{"bash":"client_call","read_file":"client_call"}}),
            false,
        ),
        (3, serde_json::json!({"prefix_all":"client_"}), true),
        (4, serde_json::json!({"names":{"bash":""}}), false),
        (
            5,
            serde_json::json!({"names":{"bash":"x".repeat(65)}}),
            false,
        ),
        (
            6,
            serde_json::json!({"names":{"bash":"submit_result"}}),
            false,
        ),
        (
            7,
            serde_json::json!({"names":{"submit_result":"bash"}}),
            false,
        ),
        (8, serde_json::json!({"prefix_all":"client_"}), false),
    ] {
        let root = broca_test_support::scratch_root()
            .join(format!("tool-alias-refusal-{index}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let vault = CapturingVaultDaemon::start(&root).await;
        let executor = executor_with_capturing_vault(&root, &vault).await
            .with_provider_overrides(runtime_overrides("http://127.0.0.1:9/v1"))
            .with_tool_name_aliases(broca_module_serve::config::ToolNameAliasesConfig::parse(
                &serde_json::json!({"tool_name_aliases":{"oauth:anthropic":form,"apikey:anthropic":form}}).to_string()).unwrap());
        let mut req = drift_request(&root);
        req.model.model_id = "claude-opus-4-8".into();
        req.tools = alias_tools();
        if colliding_tool {
            req.tools[1].name = "client_bash".into();
        }
        if index >= 6 {
            req.structured_output = alias_submit_request(&root).structured_output;
        }
        if index == 8 {
            req.tools[1].name = "client_submit_result".into();
        }
        req.policy_allowed = req.tools.iter().map(|tool| tool.name.clone()).collect();
        let session = req.session.clone();
        let store = executor.wal_store.clone();
        let writer =
            Arc::new(broca_core::WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap());
        let prior_snapshot = broca_core::load_prior_wal_snapshot(
            store.as_ref(),
            &session,
            writer.durable_head().load(Ordering::Acquire),
            None::<&broca_store::SqliteStore>,
        )
        .unwrap();
        let (ack, admission) = tokio::sync::oneshot::channel();
        let job = RunJob {
            queued_service_tier: None,
            media: vec![],
            media_profile: None,
            req,
            auth: None,
            conversation_key: None,
            writer: writer.clone(),
            kind: RunKind::FreshOrResume,
            skip_materialization: true,
            control: Default::default(),
            prior_snapshot,
            on_admitted: Some(ack),
        };
        let (out, _events) = mpsc::channel(128);
        let error = executor.execute(job, out).await.unwrap_err();
        assert!(
            matches!(&error, RunError::ToolAliasInvalid(refusal) if ["bash", "read_file", "submit_result"].contains(&refusal.tool.as_str())),
            "{error}"
        );
        assert_eq!(error.admission_error_class(), ErrorClass::Permanent);
        assert!(matches!(
            admission.await.unwrap(),
            Err(RunError::ToolAliasInvalid(_))
        ));
        Arc::try_unwrap(writer)
            .unwrap_or_else(|_| panic!("executor released writer"))
            .shutdown()
            .await
            .unwrap();
        assert!(
            store.read_records(&session).unwrap().is_empty(),
            "invalid alias must write no WAL records"
        );
        vault.assert_healthy();
        drop(executor);
        drop(vault);
        let _ = std::fs::remove_dir_all(root);
    }
}
