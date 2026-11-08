// Drives the serve executor's path for sessions kept in the new session storage: a run goes
// through `execute_on_store`, which makes `execute`'s admission decisions from the session's
// folds and runs the new run loop, its writes served on the session as the store actor serves
// them.

/// Serves a store run's writes on `session` until the run ends, as the store actor does, and
/// returns how it ended.
async fn serve_store_run(
    executor: &LiveLoopExecutor,
    session: &mut broca_session_store::session::Session<
        &'static broca_session_store::backend::MemoryBackend,
    >,
    parts: &broca_session_loop::parts::SharedParts,
    req: broca_core::RunRequest,
    mode: broca_module::executor::StoreRunMode,
) -> broca_module::executor::StoreRunEnd {
    let (writer, mut link) = broca_session_loop::shared::ActorWriter::new(session, None);
    let job = broca_module::executor::StoreRunJob {
        req,
        auth: None,
        control: Default::default(),
        writer,
        parts: Box::new(parts.clone()),
        mode,
    };
    let (out, mut events) = mpsc::channel(256);
    tokio::spawn(async move { while events.recv().await.is_some() {} });
    let run = executor.execute_on_store(job, out);
    // The run and the session's side are polled together, as the store actor's spawned run and
    // its own loop are: answering a hold waits for the run's next batch, so the run must keep
    // moving while a request is being answered.
    let mut own_parts = parts.clone();
    let served = std::sync::Mutex::new(Vec::new());
    let actor = async {
        while let Some(request) = link.next().await {
            served.lock().unwrap().push(request_kind(&request));
            let mut writer = broca_session_loop::durable::Durable::<
                _,
                broca_session_loop::durable::SetupParts<
                    &'static broca_session_store::backend::MemoryBackend,
                >,
            > {
                writer: &mut *session,
                parts: &mut own_parts,
                store: None,
            };
            link.serve(&mut writer, request).await;
        }
    };
    match tokio::time::timeout(Duration::from_secs(60), async { tokio::join!(run, actor).0 }).await
    {
        Ok(end) => end,
        Err(_) => panic!(
            "the store run never ended; the writer asked for {:?}",
            served.lock().unwrap()
        ),
    }
}

/// A writer request's kind, for a hung test's message.
fn request_kind(request: &broca_session_loop::shared::Request) -> &'static str {
    use broca_session_loop::shared::Request;
    match request {
        Request::Append { .. } => "append",
        Request::Hold { .. } => "hold",
        Request::Held { .. } => "held",
        Request::SendRecords { .. } => "send records",
        Request::OpenLogRecords { .. } => "open log records",
        Request::RunStart { .. } => "run start",
        Request::ToolIntent { .. } => "tool intent",
        Request::EntryByMid { .. } => "entry by mid",
        Request::EntriesByMid { .. } => "entries by mid",
        Request::SealedDerived { .. } => "sealed derived",
        Request::KeepSealedDerived { .. } => "keep sealed derived",
        Request::Enrollment { .. } => "enrollment",
        Request::PutEnrollment { .. } => "put enrollment",
        Request::Seal { .. } => "seal",
    }
}

/// A new store session for `root`'s test request.
async fn store_session(
) -> broca_session_store::session::Session<&'static broca_session_store::backend::MemoryBackend>
{
    let backend: &'static broca_session_store::backend::MemoryBackend =
        Box::leak(Box::default());
    broca_session_store::session::Session::create(
        backend,
        broca_session_store::backend::SessionAddr("0123456789abcdef".into()),
        broca_session_store::head::SessionTriple {
            project_root: "/project".into(),
            harness: "test".into(),
            session: "session".into(),
        },
        &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap()
}

/// The text of every message the session holds, in order.
fn store_texts(
    session: &broca_session_store::session::Session<
        &'static broca_session_store::backend::MemoryBackend,
    >,
) -> Vec<String> {
    session
        .conversation
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            broca_protocol::ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

/// A session in the new storage runs through the production executor: its admission resolves
/// the provider and credential the way `execute` does, the new run loop sends the
/// session's prompt once, and the answer and the run's end are in the session. A second run
/// on the same session is admitted from the folds, reading the first run's frozen config back
/// from its start record and setup parts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_store_session_runs_through_the_production_executor() {
    let server = resume_http_fixture().await;
    let root = broca_test_support::scratch_root()
        .join(format!("store-run-executor-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    // The provider resolves as production resolves it: from the catalog, its endpoint moved
    // to the fixture by the operator's provider override, its API key from the vault.
    install_alias_catalog();
    let daemon = CapturingVaultDaemon::start(&root).await;
    let executor = executor_with_capturing_vault(&root, &daemon)
        .await
        .with_provider_overrides(runtime_overrides(&server.url));
    let mut session = store_session().await;
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    for (run, text) in [("store-run-1", "hello"), ("store-run-2", "again")] {
        let mut req = drift_request(&root);
        req.model = broca_protocol::request::ModelHandle {
            provider_module_id: "deepseek".into(),
            model_id: "alias-model".into(),
        };
        req.run_id = broca_protocol::RunId(run.into());
        req.input = vec![broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text { text: text.into() }],
        )];
        let end = serve_store_run(
            &executor,
            &mut session,
            &parts,
            req,
            broca_module::executor::StoreRunMode::Fresh,
        )
        .await;
        let finished = match end.result {
            Ok(finished) => finished,
            Err(error) => panic!("{run} ends: {error:?}"),
        };
        assert_eq!(finished.reason, RunFinishReason::Completed, "{run}");
        assert!(finished.paused.is_none(), "{run}");
        // What the next run's admission reads: the run that just ended, idle, with the config
        // its start froze, rebuilt from the start record and its setup parts.
        let (writer, _link) = broca_session_loop::shared::ActorWriter::new(&session, None);
        let history = store_history(&writer, &parts).unwrap();
        let prior = history.prior_run.expect("the session's latest run");
        assert_eq!(prior.run_id.0, run);
        assert!(prior.idle, "{run} has ended");
        assert_eq!(prior.config.model.model_id, "alias-model");
        assert_eq!(
            prior.config.frozen_render_config["base_url"].as_str(),
            Some(server.url.as_str()),
            "{run} froze the provider the executor resolved"
        );
    }
    assert_eq!(store_texts(&session), ["hello", "done", "again", "done"]);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2, "each run sent its request once");
    let second: serde_json::Value = serde_json::from_slice(&requests[1].body).unwrap();
    let sent: Vec<&str> = second["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|message| message["content"].as_str())
        .collect();
    assert_eq!(
        sent,
        ["hello", "done", "again"],
        "the second run's request carries the first run's turn"
    );
    drop(requests);
    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(&root);
}

/// Routing that refuses every decision it is asked about as unknown, as it refuses a caller's
/// expired decision.
struct UnknownDecision;

#[async_trait::async_trait]
impl broca_module_serve::account_routing::RoutingTransport for UnknownDecision {
    async fn request(&self, _body: Vec<u8>) -> broca_module_serve::account_routing::RoutingReply {
        Ok(Err(serde_json::to_vec(&serde_json::json!({
            "code": "unknown_decision",
            "message": "fixture decision unknown",
        }))
        .unwrap()))
    }
}

/// A store session's fresh run whose caller names a routing decision routing doesn't know is
/// recorded as today's loop records it: the run's start is written, then the run ends with
/// routing's refusal, and no model request is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_store_run_refused_by_routing_starts_then_ends_with_the_refusal() {
    let server = resume_http_fixture().await;
    let root = broca_test_support::scratch_root()
        .join(format!("store-run-routing-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    install_alias_catalog();
    let daemon = CapturingVaultDaemon::start(&root).await;
    let mut executor = executor_with_capturing_vault(&root, &daemon)
        .await
        .with_provider_overrides(runtime_overrides(&server.url));
    // Routing is asked only once the vault has listed the session's candidate accounts.
    let listing = serde_json::from_value(serde_json::json!({
        "credentials": [], "grants": 1, "grant_tuples": [], "view": "store-run-routing",
    }))
    .unwrap();
    executor.vault.as_ref().unwrap().listing.store(listing);
    executor.routing = Some(Arc::new(
        broca_module_serve::account_routing::RoutingClient::new(
            Arc::new(UnknownDecision),
            broca_module_serve::account_routing::AccountSelectionConfig::default(),
        ),
    ));
    let mut session = store_session().await;
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    let mut req = drift_request(&root);
    req.model = broca_protocol::request::ModelHandle {
        provider_module_id: "deepseek".into(),
        model_id: "alias-model".into(),
    };
    req.run_id = broca_protocol::RunId("store-run-routed".into());
    req.route_decision_id = Some("expired-decision".into());
    req.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "hello".into(),
        }],
    )];
    let end = serve_store_run(
        &executor,
        &mut session,
        &parts,
        req,
        broca_module::executor::StoreRunMode::Fresh,
    )
    .await;
    let finished = match end.result {
        Ok(finished) => finished,
        Err(error) => panic!("the routed run ends: {error:?}"),
    };
    assert_eq!(finished.reason, RunFinishReason::Error);
    use broca_session_loop::writer::Writer as _;
    assert_eq!(
        session
            .operations()
            .run
            .as_ref()
            .map(|run| run.run_id.as_str()),
        Some("store-run-routed"),
        "the run's start is written"
    );
    let records = session.open_log_records().await.unwrap();
    let error = records
        .iter()
        .find_map(|(_, record)| match record {
            broca_session_store::record::Record::RunEnd(end) => Some(end.error.clone()),
            _ => None,
        })
        .expect("the run's end is written")
        .expect("the run's end carries routing's refusal");
    assert_eq!(error["provider_code"], "routing_decision_refused", "{error}");
    assert!(
        server.requests.lock().unwrap().is_empty(),
        "no model request is sent"
    );
    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(&root);
}
