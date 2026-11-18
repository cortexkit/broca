// Drives the serve executor's path for sessions kept in the new session storage: a run goes
// through `execute_on_store`, which makes `execute`'s admission decisions from the session's
// folds and runs the new run loop, its writes served on the session as the store actor serves
// them.

#[test]
#[allow(unreachable_code, clippy::diverging_sub_expression)]
fn store_run_parent_future_stays_small() {
    fn future_size<F>(_: impl FnOnce() -> F) -> usize {
        std::mem::size_of::<F>()
    }
    let bytes = future_size(|| {
        LiveLoopExecutor::<FileWalStore>::run_on_store(
            todo!(),
            todo!(),
            todo!(),
            todo!(),
            todo!(),
            todo!(),
            todo!(),
            todo!(),
            todo!(),
            todo!(),
            todo!(),
        )
    });
    eprintln!("run_on_store future: {bytes} bytes");
    // The executor is boxed by async_trait, but its poll frame must not also carry the large
    // run and resume bodies inline. Guard their caller rather than the already-boxed executor.
    assert!(
        bytes <= 24 * 1024,
        "run_on_store carries {bytes} bytes; box the loop futures it awaits"
    );
}

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
    serve_store_run_with_control(executor, session, parts, req, mode, Default::default(), None)
        .await
}

async fn serve_store_run_with_control(
    executor: &LiveLoopExecutor,
    session: &mut broca_session_store::session::Session<
        &'static broca_session_store::backend::MemoryBackend,
    >,
    parts: &broca_session_loop::parts::SharedParts,
    req: broca_core::RunRequest,
    mode: broca_module::executor::StoreRunMode,
    control: broca_core::run::RunControl,
    auth: Option<broca_protocol::AuthRequest>,
) -> broca_module::executor::StoreRunEnd {
    // The session's image index, handed to the run as its actor hands it over once built.
    let backend: &'static broca_session_store::backend::MemoryBackend = session.backend();
    let mut index = broca_module::store_media::SessionIndex::new(&*session, Some(backend));
    index.open(session, parts).await;
    assert!(index.ready(), "a session's image index is current once opened");
    let (media_sent, media) = tokio::sync::oneshot::channel();
    let _ = media_sent.send(index.for_run());
    let (writer, mut link) = broca_session_loop::shared::ActorWriter::new(session, None);
    let job = broca_module::executor::StoreRunJob {
        req,
        auth,
        control,
        writer,
        parts: Box::new(parts.clone()),
        mode,
        media: Some(media),
        queued_service_tier: None,
        keep_warm: None,
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
                media: None,
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
        Request::CaughtUp { .. } => "caught up",
        Request::SendRecords { .. } => "send records",
        Request::OpenLogRecords { .. } => "open log records",
        Request::AcceptedSend { .. } => "accepted send",
        Request::CachePredecessor { .. } => "cache predecessor",
        Request::RunStart { .. } => "run start",
        Request::WindowMessageWhole { .. } => "window message whole",
        Request::Release { .. } => "release",
        Request::ToolIntent { .. } => "tool intent",
        Request::EntryByMid { .. } => "entry by mid",
        Request::EntryAt { .. } => "entry at",
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
        .held_messages()
        .unwrap()
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            broca_protocol::ContentBlock::Text { text } => Some(text.to_string()),
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
        let history = store_history(&writer, &parts).await.unwrap();
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

/// On the new storage, a run on a paid key filed under `anthropic-platform` stores the provider
/// whose rows it borrowed with its config, so the next admission (and a resume, and the run's
/// spend) reads `anthropic` back rather than resolving it again; both runs go to Anthropic's
/// endpoint with the key in `x-api-key`.
#[tokio::test]
async fn a_store_run_on_a_pool_key_keeps_the_provider_whose_rows_it_borrowed() {
    // It replaces the process-wide catalog, so it runs in a child process of its own.
    if !run_catalog_drift_child("a_store_run_on_a_pool_key_keeps_the_provider_whose_rows_it_borrowed") {
        return;
    }
    let server = resume_http_fixture().await;
    let root = broca_test_support::scratch_root()
        .join(format!("store-run-borrowed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    install_test_catalog();
    let daemon = CapturingVaultDaemon::start(&root).await;
    let pool = Arc::new(broca_module_serve::account_routing::ListingCache::default());
    pool.store(broca_subc::vault::ScopedList {
        credentials: vec![serde_json::from_value(serde_json::json!({
            "id": "apikey:anthropic:acct1", "type": "apikey", "categories": ["llm"],
            "serves": ["anthropic"], "state": "active", "record_version": 1,
            "operations": ["read"], "auth_method": "apikey",
            "provider_ids": ["anthropic-platform"]}))
        .unwrap()],
        grants: 1,
        grant_tuples: vec![],
        view: "fixture".into(),
    });
    let executor = executor_with_vault_addressing(
        &root,
        &daemon,
        Duration::from_secs(2),
        pool,
        supervised_broca_identity(),
    )
    .await
    .with_provider_overrides(runtime_overrides(&server.url));
    let mut session = store_session().await;
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    for (run, text) in [("pool-run-1", "hello"), ("pool-run-2", "again")] {
        let mut req = drift_request(&root);
        req.model = broca_protocol::request::ModelHandle::new("anthropic-platform", "claude-haiku-4-5");
        req.generation.max_output_tokens = Some(1024);
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
        match end.result {
            Ok(finished) => assert_eq!(finished.reason, RunFinishReason::Completed, "{run}"),
            Err(error) => panic!("{run} ends: {error:?}"),
        }
        let (writer, _link) = broca_session_loop::shared::ActorWriter::new(&session, None);
        let prior = store_history(&writer, &parts)
            .await
            .unwrap()
            .prior_run
            .expect("the session's latest run");
        assert_eq!(prior.run_id.0, run);
        assert_eq!(prior.config.model.provider_module_id, "anthropic-platform");
        assert_eq!(prior.config.served_provider.as_deref(), Some("anthropic"), "{run}");
        assert_eq!(prior.config.billed_provider(), "anthropic");
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests.iter() {
        assert!(request.headers.lines().next().unwrap().contains("/messages "));
        assert!(request.headers.to_ascii_lowercase().contains("x-api-key:"));
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["model"], "claude-haiku-4-5");
    }
    drop(requests);
    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(&root);
}

/// A leftover trial selection file must not change fresh runs, even in the listed project.
/// Check the config persisted by the production executor, not just the catalog's default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fresh_trial_folder_run_ignores_the_retired_selection_file() {
    use std::io::Write;

    // It replaces the process-global catalog, so it runs in its own process.
    if !run_catalog_drift_child("a_fresh_trial_folder_run_ignores_the_retired_selection_file") {
        return;
    }
    let server = resume_http_fixture().await;
    let root = fresh_vault_root("retired-ride-selection");
    let trial_project = std::path::Path::new("/Users/Shared/ck-move-scratch-broca");
    let mut options = broca_owner_only::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    options
        .open(root.join("experiments.json"))
        .unwrap()
        .write_all(
            serde_json::to_string(&serde_json::json!({
                "ride_placement": {"sibling_project_roots": [trial_project]}
            }))
            .unwrap()
            .as_bytes(),
        )
        .unwrap();
    install_drift_revision(false, 4096);
    let daemon = CapturingVaultDaemon::start(&root).await;
    let executor = executor_with_capturing_vault(&root, &daemon)
        .await
        .with_provider_overrides(runtime_overrides(&server.url));
    let mut session = store_session().await;
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    for run in ["trial-folder-fresh", "trial-folder-next"] {
        let mut req = drift_request(trial_project);
        req.run_id = broca_protocol::RunId(run.into());
        req.input = vec![broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text { text: "hello".into() }],
        )];
        let end = serve_store_run(
            &executor,
            &mut session,
            &parts,
            req,
            broca_module::executor::StoreRunMode::Fresh,
        )
        .await;
        let finished = end.result.unwrap_or_else(|error| panic!("{run}: {error:?}"));
        assert_eq!(finished.reason, RunFinishReason::Completed, "{run}");
        let (writer, _link) = broca_session_loop::shared::ActorWriter::new(&session, None);
        let history = store_history(&writer, &parts).await.unwrap();
        let config = history.prior_run.unwrap().config.frozen_render_config;
        assert_eq!(config["family_id"], "anthropic_messages");
        assert!(
            config.get("ride_placement").is_none(),
            "{run} must freeze inside-the-result placement despite the retired file: {config}"
        );
    }
    assert_eq!(server.requests.lock().unwrap().len(), 2);
    daemon.assert_healthy();
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
            broca_session_store::record::Record::RunEnd(end) => Some(end.error.as_ref().map(|error| error.parse())),
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
