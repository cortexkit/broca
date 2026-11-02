// Exercise the render boundary with real HTTP normalization, core dispatch and session reads.

async fn alias_http_fixture(reply_name: &str, pause_first: bool) -> ResumeHttpFixture {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let reply_name = reply_name.to_owned();
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
                    serde_json::json!({"tool_calls":[{"index":0,"id":"alias-call","type":"function","function":{"name":reply_name,"arguments":"{}"}}]})
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
            matches!(&error, RunError::ToolAliasInvalid(refusal) if refusal.tool == "bash" || refusal.tool == "read_file"),
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
