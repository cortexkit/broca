// Drive the shipping serve adapter and executor without a native daemon or live credentials.
use broca_protocol::RunId;

struct TierServeRig {
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    serve: tokio::task::JoinHandle<()>,
    registry: Arc<broca_module::LineageRegistry>,
    runtime: Arc<broca_module::store_open::StoreRuntime>,
    session: BindIdentity,
    corr: u64,
    subc: Option<tokio::task::JoinHandle<()>>,
}

impl TierServeRig {
    async fn start(root: &std::path::Path, url: &str) -> Self {
        let (live, subc) = stand_in_executor(root).await;
        Self::start_in(root, Some(url), live, Some(subc)).await
    }

    /// A rig whose runs `live` executes, as it is configured.
    async fn start_with_executor(root: &std::path::Path, live: LiveLoopExecutor) -> Self {
        Self::start_in(root, None, live, None).await
    }

    async fn start_in(
        root: &std::path::Path,
        url: Option<&str>,
        mut live: LiveLoopExecutor,
        subc: Option<tokio::task::JoinHandle<()>>,
    ) -> Self {
        use broca_module::{DefaultOpener, LineageRegistry};
        use subc_protocol::{
            Flags, Frame, FrameType, ModuleHelloAckBody, Priority, PROTOCOL_VERSION,
        };
        live.provider_base_url = url.map(str::to_owned);
        let store = Arc::new(broca_store::SqliteStore::open(root.join("store.db")).unwrap());
        broca_store::session_refs::migrate(
            &root.join("store.db"),
            &broca_store::session_refs::StoredSessions::new(root),
        )
        .unwrap();
        let executor: Arc<dyn RunExecutor> = Arc::new(live);
        let runtime = Arc::new(
            broca_module::store_open::StoreRuntime::open(
                root,
                broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
                Arc::clone(&executor),
                std::time::Duration::from_millis(100),
            )
            .await
            .unwrap(),
        );
        let opener = DefaultOpener::new(
            Arc::new(broca_core::FileLeaseStore::new(root.join("leases"))),
            executor,
            Arc::new(broca_module::store_open::StoreSlot::loaded(Arc::clone(&runtime))),
        );
        let registry = LineageRegistry::new(Arc::new(opener));
        let registry = Arc::new(registry);
        registry.set_session_address_inventory(store);
        let (client, server) = tokio::io::duplex(1024 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (read, write) = tokio::io::split(server);
        let handler = registry.clone();
        let serve = tokio::spawn(async move {
            broca_module_serve::serve_connection(
                read,
                write,
                "broca",
                handler.clone(),
                handler,
                broca_module_serve::serve::VaultGauge::not_in_use(),
            )
            .await
            .unwrap();
        });
        let hello = subc_transport::read_frame(&mut reader)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hello.header.ty, FrameType::Hello);
        let ack = Frame::build(
            FrameType::HelloAck,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            hello.header.corr,
            serde_json::to_vec(&ModuleHelloAckBody {
                resume_token: None,
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: vec![],
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        subc_transport::write_frame(&mut writer, &ack)
            .await
            .unwrap();
        // The address this rig's route actually reaches: the same resolution a served bind
        // performs, so a session recorded under another address before 0.34 is still found.
        let session = registry
            .resolve_session_address(&BindIdentity::new("", "tier-test", "session"), None, None)
            .await
            .expect("the session address resolves from the inventory, as a served bind does");
        let mut rig = Self {
            reader,
            writer,
            serve,
            registry,
            runtime,
            session,
            corr: 0,
            subc,
        };
        let bind = subc_protocol::session::ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch: 1,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: subc_protocol::BindIdentity::new("tier-test", "session"),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        let reply = rig.exchange(0, serde_json::to_value(bind).unwrap()).await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
        rig
    }

    async fn exchange(&mut self, channel: u16, value: serde_json::Value) -> subc_protocol::Frame {
        use subc_protocol::{Flags, Frame, FrameType, Priority};
        self.corr += 1;
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            channel,
            u32::from(channel != 0),
            self.corr,
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        subc_transport::write_frame(&mut self.writer, &frame)
            .await
            .unwrap();
        let reply = subc_transport::read_frame(&mut self.reader)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reply.header.corr, self.corr);
        reply
    }

    async fn call(&mut self, method: &str, params: serde_json::Value) -> subc_protocol::Frame {
        self.exchange(7, serde_json::json!({"method":method,"params":params}))
            .await
    }

    async fn send(&mut self, id: &str, tier: Option<&str>) -> subc_protocol::Frame {
        let mut params = serde_json::json!({"prompt":"hello","send_id":id,
            "model":{"provider":"openai","model":"test-ultrafast"}});
        if let Some(tier) = tier {
            params["service_tier"] = serde_json::json!(tier);
        }
        self.call("session.send", params).await
    }

    async fn ended(&mut self, run: &str) -> (subc_protocol::Frame, subc_protocol::Frame) {
        let registry = self.registry.clone();
        let session = self.session.clone();
        let run_id = RunId(run.to_owned());
        broca_test_support::wait_until("tier run public terminal", || {
            let registry = registry.clone();
            let session = session.clone();
            let run_id = run_id.clone();
            async move {
                let status =
                    broca_module::ServeHandler::run_status(registry.as_ref(), &session, run_id)
                        .await
                        .map_err(|error| error.to_string())?;
                let value = serde_json::to_value(status).unwrap();
                if matches!(value["state"].as_str(), Some("active" | "unknown")) {
                    Err(value.to_string())
                } else {
                    Ok(())
                }
            }
        })
        .await;
        let status = self
            .call("run.status", serde_json::json!({"run_id":run}))
            .await;
        let result = self
            .call("run.result", serde_json::json!({"run_id":run}))
            .await;
        (status, result)
    }

    /// The session's records in the session store, in log order; empty while it doesn't exist.
    async fn records(&self) -> Vec<broca_session_store::record::Record> {
        match self.runtime.current_view(&self.session).await {
            Ok(Some(view)) => view
                .open_log_records()
                .await
                .unwrap()
                .into_iter()
                .map(|(_, record)| record)
                .collect(),
            _ => Vec::new(),
        }
    }

    async fn close(self) {
        assert!(self.registry.stop_and_seal().await);
        drop(self.writer);
        drop(self.reader);
        self.serve.await.unwrap();
        if let Some(subc) = self.subc {
            subc.abort();
            let _ = subc.await;
        }
    }
}

fn tier_sample(name: &str, bytes: &[u8]) {
    if let Some(dir) = std::env::var_os("BROCA_SERVICE_TIER_SAMPLES_DIR") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(name), bytes).unwrap();
    }
}

#[tokio::test]
async fn head_selection_reader_samples() {
    if !run_catalog_drift_child("head_selection_reader_samples") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    let root = fresh_vault_root("head-selection-readers");
    let http = resume_http_fixture().await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    let describe = rig.call("role.describe", serde_json::json!({})).await;
    tier_sample("role.describe.json", &describe.body);
    let unwritten = rig.call("session.head", serde_json::json!({})).await;
    tier_sample("session.head-unwritten.json", &unwritten.body);
    let run = tier_run(&rig.send("first", Some("fast")).await);
    rig.ended(&run).await;
    let head = rig.call("session.head", serde_json::json!({})).await;
    assert_eq!(tier_reply(&head)["result"]["model"]["model"], "test-ultrafast");
    tier_sample("session.head.json", &head.body);
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

fn tier_reply(frame: &subc_protocol::Frame) -> serde_json::Value {
    serde_json::from_slice(&frame.body).unwrap()
}

fn tier_run(frame: &subc_protocol::Frame) -> String {
    assert_eq!(
        frame.header.ty,
        subc_protocol::FrameType::Response,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    let value = tier_reply(frame);
    value["result"]["run_id"]
        .as_str()
        .or_else(|| {
            value["result"]["submission_id"]
                .as_str()
                .and_then(|id| id.strip_prefix("sub-"))
        })
        .unwrap_or_else(|| panic!("admitted run identity: {value}"))
        .to_owned()
}

struct HeadSelectionAdmitter;

#[async_trait::async_trait]
impl broca_fetch_plan::PlanAdmitter for HeadSelectionAdmitter {
    async fn admit(
        &self,
        plan: &broca_fetch_plan::Plan,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
    ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
        Ok(broca_fetch_plan::Admitted {
            manifest: broca_records::record::PlanManifest {
                rich_ui: None,
                plan: broca_records::record::OpaqueJson::from_json_text(plan.text.clone()).unwrap(),
                plan_identity: plan.identity.clone(),
                composition_digest: cortexkit_role_tool_provider::catalog::composition_digest(
                    &serde_json::json!({"providers":[],"step_transform_items":[]}),
                )
                .unwrap(),
                fetches: vec![],
                tools: Arc::new(vec![]),
                system_text: String::new(),
                system_digest: String::new(),
                join_version: 1,
                role_versions: Default::default(),
                declarations: vec![],
            },
        })
    }
}

fn selection_plan(model: Option<serde_json::Value>) -> serde_json::Value {
    let composition = serde_json::json!({"providers":[],"step_transform_items":[]});
    let mut plan = serde_json::json!({"tool_items":[],"system_text_items":[],
        "step_transform_items":[],"composition":composition,
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap()});
    if let Some(model) = model {
        plan["model"] = model;
    }
    plan
}

fn fast_selection() -> serde_json::Value {
    serde_json::json!({"provider":"openai","model":"test-ultrafast","service_tier":"fast"})
}

async fn selection_first(rig: &mut TierServeRig, plan: serde_json::Value) -> String {
    rig.registry.set_plan_admitter(Arc::new(HeadSelectionAdmitter));
    let sent = rig.call("session.send", serde_json::json!({"send_id":"first","prompt":"hello",
        "model":{"provider":"openai","model":"test-standard-only"},"service_tier":"standard","plan":plan})).await;
    let run = tier_run(&sent);
    selection_completed(rig, &run).await;
    run
}

async fn selection_completed(rig: &mut TierServeRig, run: &str) {
    let (_, result) = rig.ended(run).await;
    assert_eq!(tier_reply(&result)["result"]["state"], "completed", "{result:?}");
}

#[tokio::test]
async fn head_selection_creating_send_plan_drives_first_run() {
    if !run_catalog_drift_child("head_selection_creating_send_plan_drives_first_run") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    let root = fresh_vault_root("selection-create");
    let http = resume_http_fixture().await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    rig.registry.set_plan_admitter(Arc::new(HeadSelectionAdmitter));
    let sent = rig.call("session.send", serde_json::json!({"send_id":"first","prompt":"hello",
        "plan":selection_plan(Some(fast_selection()))})).await;
    let run = tier_run(&sent);
    selection_completed(&mut rig, &run).await;
    let body: serde_json::Value = serde_json::from_slice(&http.requests.lock().unwrap()[0].body).unwrap();
    assert_eq!(body["model"], "test-ultrafast");
    assert_eq!(body["service_tier"], "priority");
    let head = tier_reply(&rig.call("session.head", serde_json::json!({})).await);
    assert_eq!(head["result"]["model"]["model"], "test-ultrafast");
    assert_eq!(head["result"]["service_tier"], "fast");
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn head_selection_refreshed_model_is_used_by_next_run() {
    if !run_catalog_drift_child("head_selection_refreshed_model_is_used_by_next_run") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    let root = fresh_vault_root("selection-refresh-run");
    let http = resume_http_fixture().await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    selection_first(&mut rig, selection_plan(None)).await;
    let refresh = rig.call("session.refresh", serde_json::json!({"plan":selection_plan(Some(fast_selection())),
        "generation":1,"policy":"on_prefix_rebuild"})).await;
    assert_eq!(refresh.header.ty, subc_protocol::FrameType::Response, "{refresh:?}");
    let head = rig.call("session.head", serde_json::json!({})).await;
    assert_eq!(tier_reply(&head)["result"]["model"]["model"], "test-ultrafast");
    assert_eq!(tier_reply(&head)["result"]["service_tier"], "fast");
    tier_sample("session.head-refreshed.json", &head.body);
    assert!(
        rig.records().await.iter().any(|record| matches!(
            record,
            broca_session_store::record::Record::SessionRefresh(refresh) if refresh.generation == 1
        )),
        "the refresh is recorded"
    );
    let absent = rig.call("session.refresh", serde_json::json!({"plan":selection_plan(None),
        "generation":2,"policy":"on_prefix_rebuild"})).await;
    assert_eq!(absent.header.ty, subc_protocol::FrameType::Response, "{absent:?}");
    assert_eq!(tier_reply(&rig.call("session.head", serde_json::json!({})).await)["result"]["service_tier"], "fast");
    rig.close().await;
    let mut reopened = TierServeRig::start(&root, &http.url).await;
    let sent = reopened.call("session.send", serde_json::json!({"send_id":"after-restart","prompt":"hello"})).await;
    let run = tier_run(&sent);
    selection_completed(&mut reopened, &run).await;
    let body: serde_json::Value = serde_json::from_slice(&http.requests.lock().unwrap().last().unwrap().body).unwrap();
    assert_eq!(body["model"], "test-ultrafast");
    assert_eq!(body["service_tier"], "priority");
    reopened.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn head_selection_send_model_overrides_plan_model() {
    if !run_catalog_drift_child("head_selection_send_model_overrides_plan_model") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    let root = fresh_vault_root("selection-send-model");
    let http = resume_http_fixture().await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    selection_first(&mut rig, selection_plan(Some(fast_selection()))).await;
    let body: serde_json::Value = serde_json::from_slice(&http.requests.lock().unwrap()[0].body).unwrap();
    assert_eq!(body["model"], "test-standard-only");
    assert_eq!(body["service_tier"], "default");
    let head = tier_reply(&rig.call("session.head", serde_json::json!({})).await);
    assert_eq!(head["result"]["model"]["model"], "test-ultrafast");
    assert_eq!(head["result"]["service_tier"], "fast");
    let sent = rig.call("session.send", serde_json::json!({"send_id":"own-model-default-tier","prompt":"hello",
        "model":{"provider":"openai","model":"test-other-fast"}})).await;
    selection_completed(&mut rig, &tier_run(&sent)).await;
    let body: serde_json::Value = serde_json::from_slice(&http.requests.lock().unwrap().last().unwrap().body).unwrap();
    assert_eq!(body["model"], "test-other-fast");
    assert_eq!(body["service_tier"], "priority");
    let head = tier_reply(&rig.call("session.head", serde_json::json!({})).await);
    assert_eq!(head["result"]["model"]["model"], "test-ultrafast");
    assert_eq!(head["result"]["service_tier"], "fast");
    let sent = rig.call("session.send", serde_json::json!({"send_id":"inherit-override","prompt":"hello"})).await;
    selection_completed(&mut rig, &tier_run(&sent)).await;
    let body: serde_json::Value = serde_json::from_slice(&http.requests.lock().unwrap().last().unwrap().body).unwrap();
    assert_eq!(body["model"], "test-ultrafast");
    assert_eq!(body["service_tier"], "priority");
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn head_selection_refresh_does_not_change_running_tier() {
    if !run_catalog_drift_child("head_selection_refresh_does_not_change_running_tier") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    let root = fresh_vault_root("selection-running-tier");
    let release = Arc::new(tokio::sync::Notify::new());
    let http = tier_http_fixture(vec![None, None], false, Some(release.clone())).await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    rig.registry.set_plan_admitter(Arc::new(HeadSelectionAdmitter));
    let sent = rig.call("session.send", serde_json::json!({"send_id":"first","prompt":"hello",
        "plan":selection_plan(Some(fast_selection()))})).await;
    let run = tier_run(&sent);
    broca_test_support::wait_until("held selection request", || std::future::ready(
        if http.requests.lock().unwrap().is_empty() { Err("not requested".to_owned()) } else { Ok(()) }
    )).await;
    let standard = selection_plan(Some(serde_json::json!({"provider":"openai","model":"test-ultrafast"})));
    let refreshed = rig.call("session.refresh", serde_json::json!({"plan":standard,"generation":1,"policy":"immediately"})).await;
    assert_eq!(refreshed.header.ty, subc_protocol::FrameType::Response, "{refreshed:?}");
    let refused = rig.call("session.send", serde_json::json!({"send_id":"change-tier","prompt":"hello",
        "delivery":"steer","service_tier":"standard"})).await;
    assert_eq!(tier_reply(&refused)["detail"], serde_json::json!({"field":"service_tier","reason":"run_tier_fixed"}));
    let steer = rig.call("session.send", serde_json::json!({"send_id":"join-tier","prompt":"hello","delivery":"steer"})).await;
    assert_eq!(steer.header.ty, subc_protocol::FrameType::Response, "{steer:?}");
    release.notify_one();
    let (_, result) = rig.ended(&run).await;
    assert_eq!(tier_reply(&result)["result"]["state"], "completed");
    assert_eq!(tier_reply(&result)["result"]["usage"]["service_tier_applied"], "fast");
    {
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["service_tier"], "priority");
        }
    }
    assert!(tier_reply(&rig.call("session.head", serde_json::json!({})).await)["result"].get("service_tier").is_none());
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

async fn selection_refusal(model: serde_json::Value, detail: serde_json::Value) {
    let root = fresh_vault_root("selection-refusal");
    let http = resume_http_fixture().await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    selection_first(&mut rig, selection_plan(None)).await;
    let before = rig.records().await;
    let head = rig.call("session.head", serde_json::json!({})).await;
    let response = rig.call("session.refresh", serde_json::json!({"plan":selection_plan(Some(model)),
        "generation":1,"policy":"immediately"})).await;
    assert_eq!(response.header.ty, subc_protocol::FrameType::Error, "{response:?}");
    let error = tier_reply(&response);
    assert_eq!(error["code"], "invalid_params");
    assert_eq!(error["detail"], detail);
    assert_eq!(rig.records().await, before);
    assert_eq!(rig.call("session.head", serde_json::json!({})).await.body, head.body);
    assert_eq!(http.requests.lock().unwrap().len(), 1);
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn head_selection_refuses_malformed_model_without_write() {
    if !run_catalog_drift_child("head_selection_refuses_malformed_model_without_write") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    selection_refusal(serde_json::json!({"provider":"openai","model":"test-ultrafast","unknown":true}),
        serde_json::json!({"field":"plan.model"})).await;
}

#[tokio::test]
async fn head_selection_refuses_unsupported_model_without_write() {
    if !run_catalog_drift_child("head_selection_refuses_unsupported_model_without_write") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    selection_refusal(serde_json::json!({"provider":"openai","model":"missing-model"}),
        serde_json::json!({"field":"plan.model","reason":"unsupported_model"})).await;
}

#[tokio::test]
async fn head_selection_missing_sign_in_is_unsupported_model_without_write() {
    if !run_catalog_drift_child("head_selection_missing_sign_in_is_unsupported_model_without_write") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    let root = fresh_vault_root("selection-no-sign-in");
    let (mut live, subc) = stand_in_executor(&root).await;
    live.provider_base_url = None;
    live.vault = None;
    let result = live.check_plan_selection(&selection_plan(Some(fast_selection()))).await;
    assert_eq!(result, Err(cortexkit_role_llm_runner::head_selection::PlanModelProblem::UnsupportedModel));
    subc.abort();
    // The executor owns the SQLite store; Windows cannot delete its open database files.
    drop(live);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn head_selection_refuses_unsupported_tier_without_write() {
    if !run_catalog_drift_child("head_selection_refuses_unsupported_tier_without_write") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    for tier in ["unknown-tier", "fast"] {
        selection_refusal(serde_json::json!({"provider":"openai","model":"test-standard-only","service_tier":tier}),
            serde_json::json!({"field":"plan.model.service_tier","reason":"unsupported_tier"})).await;
    }
}

#[tokio::test]
async fn head_selection_creating_refusals_do_not_create_a_session() {
    if !run_catalog_drift_child("head_selection_creating_refusals_do_not_create_a_session") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    for (model, detail) in [
        (serde_json::json!({"provider":"openai"}), serde_json::json!({"field":"plan.model"})),
        (serde_json::json!({"provider":"openai","model":"missing-model"}), serde_json::json!({"field":"plan.model","reason":"unsupported_model"})),
        (serde_json::json!({"provider":"openai","model":"test-standard-only","service_tier":"fast"}), serde_json::json!({"field":"plan.model.service_tier","reason":"unsupported_tier"})),
    ] {
        let root = fresh_vault_root("selection-create-refusal");
        let http = resume_http_fixture().await;
        let mut rig = TierServeRig::start(&root, &http.url).await;
        rig.registry.set_plan_admitter(Arc::new(HeadSelectionAdmitter));
        let response = rig.call("session.send", serde_json::json!({"send_id":"refused","prompt":"hello",
            "model":{"provider":"openai","model":"test-ultrafast"},"plan":selection_plan(Some(model))})).await;
        assert_eq!(response.header.ty, subc_protocol::FrameType::Error, "{response:?}");
        assert_eq!(tier_reply(&response)["detail"], detail);
        assert!(
            rig.runtime.current_view(&rig.session).await.unwrap().is_none(),
            "a refused creating send creates no session"
        );
        assert!(http.requests.lock().unwrap().is_empty());
        rig.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }
}

async fn cold_plan_send_rig(
    root: &std::path::Path,
    reply: ListingReply,
    provider_url: &str,
) -> (TierServeRig, CapturingVaultDaemon, Arc<broca_module_serve::account_routing::ListingCache>) {
    let daemon = CapturingVaultDaemon::start(root).await;
    daemon.record.lock().unwrap().listing = Some(reply);
    let listing = Arc::new(broca_module_serve::account_routing::ListingCache::default());
    let live = executor_with_vault_addressing(
        root, &daemon, crate::VAULT_READY_TIMEOUT, listing.clone(), supervised_broca_identity(),
    ).await.with_provider_overrides(runtime_overrides(provider_url));
    let rig = TierServeRig::start_with_executor(root, live).await;
    rig.registry.set_plan_admitter(Arc::new(HeadSelectionAdmitter));
    assert!(listing.get().is_none());
    (rig, daemon, listing)
}

async fn cold_plan_send(rig: &mut TierServeRig) -> subc_protocol::Frame {
    let plan = selection_plan(Some(serde_json::json!({"provider":"deepseek","model":"deepseek-v4-flash"})));
    rig.call("session.send", serde_json::json!({"send_id":"first","prompt":"hello","plan":plan})).await
}

#[tokio::test]
async fn creating_plan_send_waits_for_cold_vault_listing() {
    if !run_catalog_drift_child("creating_plan_send_waits_for_cold_vault_listing") {
        return;
    }
    let _catalog = install_test_catalog();
    let root = fresh_vault_root("cold-plan-send");
    let http = resume_http_fixture().await;
    let reply = ListingReply::Listed { listing: listed_credentials([]).get().unwrap().0, release: None };
    let (mut rig, daemon, _) = cold_plan_send_rig(&root, reply, &http.url).await;
    let sent = tokio::time::timeout(2 * crate::VAULT_READY_TIMEOUT, cold_plan_send(&mut rig)).await.unwrap();
    let run = tier_run(&sent);
    selection_completed(&mut rig, &run).await;
    let requests = daemon.requests();
    assert_eq!(methods(&requests), ["credential.list_scoped", "credential.get_scoped"]);
    assert_eq!(requests[1].1["credential_id"], "apikey:deepseek");
    daemon.assert_healthy();
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn creating_plan_send_refuses_transiently_when_vault_is_silent() {
    if !run_catalog_drift_child("creating_plan_send_refuses_transiently_when_vault_is_silent") {
        return;
    }
    let _catalog = install_test_catalog();
    let root = fresh_vault_root("cold-plan-silent");
    let http = resume_http_fixture().await;
    let (mut rig, daemon, listing) = cold_plan_send_rig(&root, ListingReply::Silent, &http.url).await;
    let reply = tokio::time::timeout(2 * crate::VAULT_READY_TIMEOUT, cold_plan_send(&mut rig)).await.unwrap();
    assert_eq!(reply.header.ty, subc_protocol::FrameType::Error);
    assert_eq!(tier_reply(&reply), serde_json::json!({
        "code": "transient", "message": "the vault credential listing has not loaded yet",
        "detail": {"class": "transient", "cause": "credentials_unavailable"},
    }));
    assert_eq!(methods(&daemon.requests()), ["credential.list_scoped"]);
    assert!(listing.get().is_none());
    assert!(rig.runtime.current_view(&rig.session).await.unwrap().is_none());
    daemon.assert_healthy();
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn creating_plan_send_refuses_unsupported_model_after_empty_vault_listing() {
    if !run_catalog_drift_child("creating_plan_send_refuses_unsupported_model_after_empty_vault_listing") {
        return;
    }
    let _catalog = install_test_catalog();
    let root = fresh_vault_root("cold-plan-no-credential");
    let http = resume_http_fixture().await;
    let release = Arc::new(tokio::sync::Notify::new());
    let reply = ListingReply::Listed {
        listing: Arc::new(broca_subc::vault::ScopedList {
            credentials: vec![], grants: 1, grant_tuples: vec![], view: "fixture".into(),
        }),
        release: Some(release.clone()),
    };
    let (mut rig, daemon, listing) = cold_plan_send_rig(&root, reply, &http.url).await;
    let reply = {
        let send = cold_plan_send(&mut rig);
        tokio::pin!(send);
        // Hold the vault reply briefly: an unsupported-model refusal may only follow the listing.
        assert!(tokio::time::timeout(Duration::from_millis(200), &mut send).await.is_err(),
            "the send answered before the vault released its listing");
        release.notify_one();
        tokio::time::timeout(2 * crate::VAULT_READY_TIMEOUT, send).await.unwrap()
    };
    assert_eq!(reply.header.ty, subc_protocol::FrameType::Error);
    assert_eq!(reply.body, br#"{"code":"invalid_params","message":"plan.model is not a supported model and tier selection","detail":{"field":"plan.model","reason":"unsupported_model"}}"#);
    assert!(listing.get().unwrap().0.credentials.is_empty());
    assert_eq!(methods(&daemon.requests()), ["credential.list_scoped"]);
    assert!(rig.runtime.current_view(&rig.session).await.unwrap().is_none());
    daemon.assert_healthy();
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

async fn tier_http_fixture(reports: Vec<Option<&'static str>>,
    two_steps: bool,
    hold: Option<Arc<tokio::sync::Notify>>,
) -> ResumeHttpFixture {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let task = tokio::spawn(async move {
        for (index, report) in reports.into_iter().enumerate() {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 4096];
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
            if index == 0 {
                if let Some(hold) = &hold {
                    hold.notified().await;
                }
            }
            let tool = two_steps && index == 0;
            let delta = if tool {
                serde_json::json!({"role":"assistant","tool_calls":[{"index":0,"id":"tier-call","type":"function","function":{"name":"not_offered","arguments":"{}"}}]})
            } else {
                serde_json::json!({"role":"assistant","content":"done"})
            };
            let first = serde_json::json!({"id":"tier","choices":[{"index":0,"delta":delta,"finish_reason":null}]});
            let mut last = serde_json::json!({"id":"tier","choices":[{"index":0,"delta":{},"finish_reason":if tool {"tool_calls"} else {"stop"}}],"usage":{"prompt_tokens":2,"completion_tokens":1}});
            if let Some(report) = report {
                last["service_tier"] = serde_json::json!(report);
            }
            let body = format!("data: {first}\n\ndata: {last}\n\ndata: [DONE]\n\n");
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    ResumeHttpFixture {
        url,
        requests,
        task,
    }
}

#[tokio::test]
async fn service_tier_usage_reports_standard_absence_and_conflict_after_reopen() {
    if !run_catalog_drift_child(
        "service_tier_usage_reports_standard_absence_and_conflict_after_reopen",
    ) {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    for (name, reports, expected) in [
        ("stated", vec![Some("default")], Some("standard")),
        (
            "missing-does-not-vote",
            vec![None, Some("priority")],
            Some("fast"),
        ),
        ("conflicting", vec![Some("default"), Some("priority")], None),
        (
            "open-served",
            vec![Some("provider-special")],
            Some("provider-special"),
        ),
    ] {
        let root = fresh_vault_root(&format!("tier-usage-{name}"));
        let http = tier_http_fixture(reports.clone(), reports.len() == 2, None).await;
        let mut rig = TierServeRig::start(&root, &http.url).await;
        let run = tier_run(&rig.send(name, Some("fast")).await);
        let (status, result) = rig.ended(&run).await;
        assert_eq!(http.requests.lock().unwrap().len(), reports.len());
        for frame in [&status, &result] {
            let value = tier_reply(frame);
            assert_eq!(value["result"]["usage"]["service_tier_applied"], "fast");
            assert_eq!(
                value["result"]["usage"]
                    .get("service_tier_served")
                    .and_then(serde_json::Value::as_str),
                expected,
                "{name}: {value}"
            );
        }
        if name == "stated" || name == "conflicting" {
            tier_sample(&format!("run.status-{name}.json"), &status.body);
            tier_sample(&format!("run.result-{name}.json"), &result.body);
        }
        rig.close().await;
        let mut reopened = TierServeRig::start(&root, &http.url).await;
        let (cold_status, cold_result) = reopened.ended(&run).await;
        assert_eq!(
            status.body, cold_status.body,
            "status must rebuild its tier consensus from the durable steps"
        );
        assert_eq!(
            result.body, cold_result.body,
            "result must rebuild its tier consensus from the durable steps"
        );
        reopened.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// A run's tier is fixed for its life: a steer naming another tier is refused, and a send
/// queued behind the run with no tier of its own records the run's tier and is sent at it once
/// it drains. Repeating the queued send answers it again with the same bytes.
#[tokio::test]
async fn service_tier_fixed_run_and_its_queued_send_keep_the_runs_tier() {
    if !run_catalog_drift_child("service_tier_fixed_run_and_its_queued_send_keep_the_runs_tier") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    let root = fresh_vault_root("tier-fixed-queue");
    let release = Arc::new(tokio::sync::Notify::new());
    let http = tier_http_fixture(vec![None, None], false, Some(release.clone())).await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    let run = tier_run(&rig.send("held-ultra", Some("ultrafast")).await);
    broca_test_support::wait_until("ultrafast request reaches HTTP", || {
        std::future::ready(if http.requests.lock().unwrap().len() == 1 {
            Ok(())
        } else {
            Err("not received".to_owned())
        })
    })
    .await;
    let refusal = rig.send("cannot-change-tier", Some("standard")).await;
    assert_eq!(
        tier_reply(&refusal)["detail"],
        serde_json::json!({"field":"service_tier","reason":"run_tier_fixed"})
    );
    let queued = rig.send("queued-ultra", None).await;
    assert_eq!(tier_reply(&queued)["result"]["state"], "pending");
    let repeated = rig.send("queued-ultra", None).await;
    assert_eq!(
        queued.body, repeated.body,
        "inheriting a tier must not alter caller retry identity"
    );
    let queued_request = rig
        .records()
        .await
        .into_iter()
        .find_map(|record| match record {
            broca_session_store::record::Record::QueuedSend(queued) => {
                queued.request.as_ref().map(|request| request.parse())
            }
            _ => None,
        })
        .expect("the queued send is recorded");
    assert_eq!(queued_request["service_tier_level"], "ultrafast", "{queued_request}");
    release.notify_one();
    rig.ended(&run).await;
    broca_test_support::wait_until("the queued send's run is sent", || {
        std::future::ready(if http.requests.lock().unwrap().len() == 2 {
            Ok(())
        } else {
            Err("not received".to_owned())
        })
    })
    .await;
    let drained = tier_reply(
        &rig.call("session.send_status", serde_json::json!({"send_id":"queued-ultra"}))
            .await,
    );
    let drained_run = drained["result"]["run_id"].as_str().unwrap().to_owned();
    let (_, result) = rig.ended(&drained_run).await;
    assert_eq!(tier_reply(&result)["result"]["state"], "completed", "{result:?}");
    for request in http.requests.lock().unwrap().iter() {
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["service_tier"], "ultrafast");
    }
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}



#[tokio::test]
async fn service_tier_writer_admits_maps_refuses_and_describes_through_serve() {
    if !run_catalog_drift_child(
        "service_tier_writer_admits_maps_refuses_and_describes_through_serve",
    ) {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    let root = fresh_vault_root("tier-writer-serve");
    let http = resume_http_fixture().await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    let describe = rig.call("role.describe", serde_json::json!({})).await;
    let value = tier_reply(&describe);
    let described = &value["result"];
    assert!(described["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c == "service_tier"));
    assert_eq!(
        described["service_tiers"]["openai.chatgpt_responses"]["levels"],
        serde_json::json!(["fast", "ultrafast"])
    );
    tier_sample("role.describe.json", &describe.body);
    for (id, tier, wire) in [
        ("fast", "fast", "priority"),
        ("ultra", "ultrafast", "ultrafast"),
        ("standard", "standard", "default"),
    ] {
        let sent = rig.send(id, Some(tier)).await;
        let run = tier_run(&sent);
        let (status, result) = rig.ended(&run).await;
        assert_eq!(
            tier_reply(&result)["result"]["state"],
            "completed",
            "{}",
            String::from_utf8_lossy(&result.body)
        );
        for frame in [&status, &result] {
            let reply = tier_reply(frame);
            let usage = &reply["result"]["usage"];
            assert_eq!(
                usage
                    .get("service_tier_applied")
                    .and_then(serde_json::Value::as_str),
                (tier != "standard").then_some(tier)
            );
            assert!(
                usage.get("service_tier_served").is_none(),
                "a missing report never confirms the applied tier"
            );
        }
        let requests = http.requests.lock().unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(&requests.last().unwrap().body).unwrap();
        assert_eq!(body["service_tier"], wire);
        if tier == "fast" {
            tier_sample("fast-request.json", &requests.last().unwrap().body);
            tier_sample("run.status-absent.json", &status.body);
            tier_sample("run.result-absent.json", &result.body);
        }
    }
    let fast = tier_run(&rig.send("fast-before-default", Some("fast")).await);
    rig.ended(&fast).await;
    let standard = tier_run(&rig.send("default-after-fast", None).await);
    let (_, result) = rig.ended(&standard).await;
    assert!(tier_reply(&result)["result"]["usage"]
        .get("service_tier_applied")
        .is_none());
    let body: serde_json::Value =
        serde_json::from_slice(&http.requests.lock().unwrap().last().unwrap().body).unwrap();
    assert!(
        body.get("service_tier").is_none(),
        "an idle send with no tier starts at standard, not the preceding run's fast tier"
    );
    let before = http.requests.lock().unwrap().len();
    for tier in ["future-tier", "priority", "flex"] {
        let refusal = rig.send(tier, Some(tier)).await;
        assert_eq!(refusal.header.ty, subc_protocol::FrameType::Error);
        let value = tier_reply(&refusal);
        assert_eq!(value["code"], "invalid_params");
        assert_eq!(
            value["detail"],
            serde_json::json!({"field":"service_tier","reason":"unsupported_tier"})
        );
        assert!(value["message"].as_str().unwrap().contains(tier));
        if tier == "future-tier" {
            tier_sample("unsupported-level.json", &refusal.body);
        }
    }
    assert_eq!(http.requests.lock().unwrap().len(), before);
    for tier in ["fast", "ultrafast"] {
        let refusal = rig
            .call(
                "session.send",
                serde_json::json!({"prompt":"hello","send_id":format!("unpriced-{tier}"),
            "model":{"provider":"openai","model":"test-standard-only"},"service_tier":tier}),
            )
            .await;
        let value = tier_reply(&refusal);
        assert_eq!(
            value["detail"],
            serde_json::json!({"field":"service_tier","reason":"unsupported_tier"})
        );
        assert!(value["message"].as_str().unwrap().contains(tier));
        assert!(value["message"]
            .as_str()
            .unwrap()
            .contains("test-standard-only"));
    }
    assert_eq!(http.requests.lock().unwrap().len(), before);
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

/// What a rig's session made of three service tier sends: a creating send whose plan's model
/// selection names `fast`, a later send naming `fast` itself, and a later send naming `fast` for
/// a model with no price at that tier. For each send, its answer, its run's end, and the model
/// and tier the provider was sent, with the run ids left out.
async fn tier_story() -> Vec<serde_json::Value> {
    let root = fresh_vault_root("tier-store");
    let http = resume_http_fixture().await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    rig.registry.set_plan_admitter(Arc::new(HeadSelectionAdmitter));
    let mut story = Vec::new();
    let sends = [
        serde_json::json!({"send_id":"first","prompt":"hello",
            "plan":selection_plan(Some(fast_selection()))}),
        serde_json::json!({"send_id":"second","prompt":"again",
            "model":{"provider":"openai","model":"test-ultrafast"},"service_tier":"fast"}),
        serde_json::json!({"send_id":"third","prompt":"once more",
            "model":{"provider":"openai","model":"test-standard-only"},"service_tier":"fast"}),
    ];
    for params in sends {
        let sent = rig.call("session.send", params).await;
        let mut answer = tier_reply(&sent);
        if sent.header.ty != subc_protocol::FrameType::Response {
            story.push(answer);
            continue;
        }
        let run = tier_run(&sent);
        let (_, result) = rig.ended(&run).await;
        if let Some(result) = answer["result"].as_object_mut() {
            result.retain(|key, _| key != "run_id" && key != "submission_id");
        }
        let body: serde_json::Value =
            serde_json::from_slice(&http.requests.lock().unwrap().last().unwrap().body).unwrap();
        story.push(serde_json::json!({
            "answer": answer,
            "state": tier_reply(&result)["result"]["state"],
            "sent": {"model": body["model"], "service_tier": body["service_tier"]},
        }));
    }
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
    story
}

/// A session admits and sends service tiers: a creating send whose plan's model selection
/// names `fast`, and a later send naming `fast`, are both sent at the provider's priority tier,
/// and a level the send's model has no price for is refused before anything is sent.
#[tokio::test]
async fn a_session_admits_and_sends_service_tiers() {
    if !run_catalog_drift_child("a_session_admits_and_sends_service_tiers") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let _catalog = install_queued_ultrafast_catalog();
    let story = tier_story().await;
    assert_eq!(
        story[0]["sent"],
        serde_json::json!({"model": "test-ultrafast", "service_tier": "priority"}),
        "{story:?}"
    );
    assert_eq!(story[1]["sent"], story[0]["sent"], "{story:?}");
    assert_eq!(
        story[2]["detail"],
        serde_json::json!({"field":"service_tier","reason":"unsupported_tier"}),
        "{story:?}"
    );
}
