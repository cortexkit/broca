// Exercise the render boundary with real HTTP normalization, core dispatch and session reads.

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

fn install_alias_catalog() -> TestCatalog {
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
    install_catalog(cache)
}

async fn execute_alias_job(
    executor: &LiveLoopExecutor,
    req: broca_core::RunRequest,
) -> (
    broca_module::executor::StoreRunEnd,
    broca_session_store::session::Session<&'static broca_session_store::backend::MemoryBackend>,
) {
    let mut session = store_session().await;
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    let end = serve_store_run(
        executor,
        &mut session,
        &parts,
        req,
        broca_module::executor::StoreRunMode::Fresh,
    )
    .await;
    (end, session)
}

/// The names of the tool calls the session holds, in order.
fn stored_call_names(
    session: &broca_session_store::session::Session<&'static broca_session_store::backend::MemoryBackend>,
) -> Vec<String> {
    session
        .conversation
        .held_messages()
        .unwrap()
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            broca_protocol::ContentBlock::ToolCall(call) => Some(call.tool_name.clone()),
            _ => None,
        })
        .collect()
}

async fn assert_submit_alias_round_trip(
    form: serde_json::Value,
    caller_name: &str,
    submit_name: &str,
) -> Vec<u8> {
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
    let (end, session) = execute_alias_job(&executor, req.clone()).await;
    assert!(
        !server.requests.lock().unwrap().is_empty(),
        "provider must be called: {:?}",
        end.result
    );
    let bytes = server.requests.lock().unwrap()[0].body.clone();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["tools"][2]["function"]["name"], submit_name);
    assert_eq!(body["tools"][0]["function"]["name"], caller_name);
    match end.result {
        Ok(finished) => assert_eq!(finished.reason, RunFinishReason::Completed),
        Err(error) => panic!("the aliased submit run ends: {error:?}"),
    }
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    // The session keeps the names the caller gave; an alias exists only in the request.
    assert_eq!(stored_call_names(&session), ["submit_result", "bash"]);
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
    let _catalog = install_alias_catalog();
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
    let _catalog = install_alias_catalog();
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
    let _catalog = install_alias_catalog();
    let bytes =
        assert_submit_alias_round_trip(serde_json::Value::Null, "bash", "submit_result").await;
    // These are the complete request bytes without operator aliases, not a re-rendered expectation.
    let legacy = r#"{"max_tokens":4096,"messages":[{"content":"run","role":"user"}],"model":"alias-model","stream":true,"stream_options":{"include_usage":true},"tool_choice":"auto","tools":[{"function":{"description":"synthetic","name":"bash","parameters":{"type":"object"}},"type":"function"},{"function":{"description":"synthetic","name":"read_file","parameters":{"type":"object"}},"type":"function"},{"function":{"description":"Submit the final result of the task. Calling this tool ends the task: call it exactly once, when the work is done, with the result as its input.","name":"submit_result","parameters":{"type":"object"}},"type":"function"}]}"#;
    assert_eq!(bytes, legacy.as_bytes());
}

#[tokio::test]
async fn tool_name_aliases_invalid_admissions_write_nothing() {
    if !run_catalog_drift_child("tool_name_aliases_invalid_admissions_write_nothing") {
        return;
    }
    let _catalog = install_test_catalog();
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
        let (end, session) = execute_alias_job(&executor, req).await;
        let error = match end.result {
            Err(broca_module::executor::StoreRunError::Admission(error)) => error,
            other => panic!("an invalid alias is refused at admission: {other:?}"),
        };
        assert!(
            matches!(&error, RunError::ToolAliasInvalid(refusal) if ["bash", "read_file", "submit_result"].contains(&refusal.tool.as_str())),
            "{error}"
        );
        assert_eq!(error.admission_error_class(), ErrorClass::Permanent);
        assert!(
            session.conversation.messages.is_empty(),
            "an invalid alias writes nothing"
        );
        vault.assert_healthy();
        drop(executor);
        drop(vault);
        let _ = std::fs::remove_dir_all(root);
    }
}
