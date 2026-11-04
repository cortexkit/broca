// Drive the shipping serve adapter and executor without a native daemon or live credentials.
use broca_protocol::RunId;

struct TierServeRig {
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    serve: tokio::task::JoinHandle<()>,
    registry: Arc<broca_module::LineageRegistry>,
    wal: Arc<FileWalStore>,
    session: BindIdentity,
    corr: u64,
    subc: tokio::task::JoinHandle<()>,
}

impl TierServeRig {
    async fn start(root: &std::path::Path, url: &str) -> Self {
        Self::start_with_plane(root, url, None).await
    }

    async fn start_with_plane(
        root: &std::path::Path,
        url: &str,
        plane: Option<Arc<TierCompaction>>,
    ) -> Self {
        use broca_module::{DefaultOpener, LineageRegistry};
        use subc_protocol::{
            Flags, Frame, FrameType, ModuleHelloAckBody, Priority, PROTOCOL_VERSION,
        };
        let (mut live, subc) = stand_in_executor(root).await;
        live.provider_base_url = Some(url.to_owned());
        let wal = live.wal_store.clone();
        let store = live.session_store.clone().expect("legacy fixture materializer");
        broca_store::session_refs::migrate(
            &root.join("store.db"),
            &broca_core::ReadOnlyWalReader::new(root),
        )
        .unwrap();
        let executor: Arc<dyn RunExecutor> = match plane {
            Some(plane) => Arc::new(TierFixtureExecutor { live, plane }),
            None => Arc::new(live),
        };
        let opener = DefaultOpener::new(
            Arc::new(broca_core::FileLeaseStore::new(root.join("leases"))),
            wal.clone(),
            executor,
        )
        .with_run_index(store.clone())
        .with_materialized_store(store.clone());
        let registry = Arc::new(LineageRegistry::new_with_reader(
            Arc::new(opener),
            wal.clone(),
            Some(store.clone()),
        ));
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
            .resolve_session_address(&BindIdentity::new("", "tier-test", "session"), None)
            .await
            .expect("the session address resolves from the inventory, as a served bind does");
        let mut rig = Self {
            reader,
            writer,
            serve,
            registry,
            wal,
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
        use broca_core::WalStore as _;
        broca_test_support::wait_until("tier run durable terminal", || {
            let frames = self.wal.read_records(&self.session).unwrap();
            let mut active = false;
            let mut ended = false;
            for frame in &frames {
                match &frame.record {
                    broca_wal::WalRecord::RunStarted { run_id, .. } => active = run_id.0 == run,
                    broca_wal::WalRecord::RunFinished { .. } if active => ended = true,
                    _ => {}
                }
            }
            std::future::ready(if ended {
                Ok(())
            } else {
                Err("no terminal".to_owned())
            })
        })
        .await;
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

    async fn close(self) {
        assert!(self.registry.stop_and_seal().await);
        drop(self.writer);
        drop(self.reader);
        self.serve.await.unwrap();
        self.subc.abort();
        let _ = self.subc.await;
    }
}

#[derive(Default)]
struct TierCompaction {
    rides: Mutex<Vec<broca_core::compaction_provider::ride::RideRequest>>,
    request_ride_counts: Mutex<Vec<usize>>,
}

#[async_trait::async_trait]
impl broca_core::CompactionPlane for TierCompaction {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        request: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::compaction::CompactionUnavailable,
    > {
        Ok(serde_json::from_value(serde_json::json!({"answer":"ready","request_id":request.request_id,
            "initial":{"compaction_id":"tier-head","version":1,"range":{"from":0,"to":0},"replacement":[]}})).unwrap())
    }

    async fn ride(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        request: broca_core::compaction_provider::ride::RideRequest,
    ) -> Result<
        broca_core::compaction_provider::ride::RideAnswer,
        broca_core::compaction::CompactionUnavailable,
    > {
        self.rides.lock().unwrap().push(request);
        Ok(broca_core::compaction_provider::ride::RideAnswer::nothing())
    }
}

struct TierFixtureExecutor {
    live: LiveLoopExecutor,
    plane: Arc<TierCompaction>,
}

#[async_trait::async_trait]
impl RunExecutor for TierFixtureExecutor {
    fn head_selection_wire(&self, model: &cortexkit_role_llm_runner::head_selection::PlanModel) -> Option<String> {
        self.live.head_selection_wire(model)
    }
    async fn check_plan_selection(&self, plan: &serde_json::Value) -> Result<Option<cortexkit_role_llm_runner::head_selection::PlanModel>, cortexkit_role_llm_runner::head_selection::PlanModelProblem> {
        self.live.check_plan_selection(plan).await
    }
    fn check_service_tier(
        &self,
        request: &broca_protocol::SubmissionRequest,
    ) -> Result<(), String> {
        self.live.check_service_tier(request)
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        Some(self.plane.clone())
    }

    async fn execute(
        &self,
        mut job: RunJob,
        out: mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<RunSummary, broca_core::RunError> {
        // Supply the already-fetched tool-free compaction plan at the executor seam. Model I/O,
        // tier changes, rides, notices and replies still use the shipping actor/core/serve path.
        job.control.manifest = Some(Arc::new(broca_wal::PlanManifest {
            rich_ui: None,
            plan: broca_wal::OpaqueJson::from_json_text(serde_json::json!({"compaction_item":{"provider":"tier-compaction","preset":"head","params":{}},"composition":{}}).to_string()).unwrap(),
            plan_identity: "tier-plan".into(), composition_digest: "tier-composition".into(),
            tools: Arc::new(vec![]), system_text: String::new(), system_digest: String::new(),
            join_version: 1, role_versions: Default::default(), fetches: vec![], declarations: vec![],
        }));
        self.live.execute(job, out).await
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
    install_queued_ultrafast_catalog();
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
            manifest: broca_wal::PlanManifest {
                rich_ui: None,
                plan: broca_wal::OpaqueJson::from_json_text(plan.text.clone()).unwrap(),
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
    install_queued_ultrafast_catalog();
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
    install_queued_ultrafast_catalog();
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
    {
        use broca_core::WalStore as _;
        let accepted = rig.wal.read_records(&rig.session).unwrap().into_iter().find_map(|frame| {
            matches!(frame.record, broca_wal::WalRecord::SessionRefreshAccepted { .. }).then_some(frame.record)
        }).unwrap();
        let bytes = serde_json::to_vec(&accepted).unwrap();
        let decoded: broca_wal::WalRecord = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, accepted);
        let mut without_selection = accepted.clone();
        if let broca_wal::WalRecord::SessionRefreshAccepted { manifest, .. } = &mut without_selection {
            let mut plan: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text()).unwrap();
            plan.as_object_mut().unwrap().remove("model");
            manifest.plan = broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap();
        }
        assert_eq!(accepted.required_features(), without_selection.required_features());
        tier_sample("session_refresh_accepted.json", &bytes);
    }
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
    install_queued_ultrafast_catalog();
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
    install_queued_ultrafast_catalog();
    let root = fresh_vault_root("selection-running-tier");
    let release = Arc::new(tokio::sync::Notify::new());
    let http = tier_http_fixture(vec![None, None], false, Some(release.clone()), None).await;
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
    use broca_core::WalStore as _;
    let root = fresh_vault_root("selection-refusal");
    let http = resume_http_fixture().await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    selection_first(&mut rig, selection_plan(None)).await;
    let before = rig.wal.read_records(&rig.session).unwrap();
    let head = rig.call("session.head", serde_json::json!({})).await;
    let response = rig.call("session.refresh", serde_json::json!({"plan":selection_plan(Some(model)),
        "generation":1,"policy":"immediately"})).await;
    assert_eq!(response.header.ty, subc_protocol::FrameType::Error, "{response:?}");
    let error = tier_reply(&response);
    assert_eq!(error["code"], "invalid_params");
    assert_eq!(error["detail"], detail);
    assert_eq!(rig.wal.read_records(&rig.session).unwrap(), before);
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
    install_queued_ultrafast_catalog();
    selection_refusal(serde_json::json!({"provider":"openai","model":"test-ultrafast","unknown":true}),
        serde_json::json!({"field":"plan.model"})).await;
}

#[tokio::test]
async fn head_selection_refuses_unsupported_model_without_write() {
    if !run_catalog_drift_child("head_selection_refuses_unsupported_model_without_write") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    install_queued_ultrafast_catalog();
    selection_refusal(serde_json::json!({"provider":"openai","model":"missing-model"}),
        serde_json::json!({"field":"plan.model","reason":"unsupported_model"})).await;
}

#[tokio::test]
async fn head_selection_missing_sign_in_is_unsupported_model_without_write() {
    if !run_catalog_drift_child("head_selection_missing_sign_in_is_unsupported_model_without_write") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    install_queued_ultrafast_catalog();
    let root = fresh_vault_root("selection-no-sign-in");
    let (mut live, subc) = stand_in_executor(&root).await;
    live.provider_base_url = None;
    live.vault = None;
    let result = live.check_plan_selection(&selection_plan(Some(fast_selection()))).await;
    assert_eq!(result, Err(cortexkit_role_llm_runner::head_selection::PlanModelProblem::UnsupportedModel));
    let session = BindIdentity::new(&root, "tier-test", "session");
    assert_eq!(broca_core::WalStore::live_wal_bytes(live.wal_store.as_ref(), &session).unwrap(), None);
    assert!(matches!(broca_core::WalStore::read_records(live.wal_store.as_ref(), &session),
        Err(broca_wal::WalError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound));
    subc.abort();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn head_selection_refuses_unsupported_tier_without_write() {
    if !run_catalog_drift_child("head_selection_refuses_unsupported_tier_without_write") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    install_queued_ultrafast_catalog();
    for tier in ["unknown-tier", "fast"] {
        selection_refusal(serde_json::json!({"provider":"openai","model":"test-standard-only","service_tier":tier}),
            serde_json::json!({"field":"plan.model.service_tier","reason":"unsupported_tier"})).await;
    }
}

#[tokio::test]
async fn head_selection_creating_refusals_do_not_create_a_wal() {
    use broca_core::WalStore as _;
    if !run_catalog_drift_child("head_selection_creating_refusals_do_not_create_a_wal") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    install_queued_ultrafast_catalog();
    for (model, detail) in [
        (serde_json::json!({"provider":"openai"}), serde_json::json!({"field":"plan.model"})),
        (serde_json::json!({"provider":"openai","model":"missing-model"}), serde_json::json!({"field":"plan.model","reason":"unsupported_model"})),
        (serde_json::json!({"provider":"openai","model":"test-standard-only","service_tier":"fast"}), serde_json::json!({"field":"plan.model.service_tier","reason":"unsupported_tier"})),
    ] {
        let root = fresh_vault_root("selection-create-refusal");
        let http = resume_http_fixture().await;
        let mut rig = TierServeRig::start(&root, &http.url).await;
        rig.registry.set_plan_admitter(Arc::new(HeadSelectionAdmitter));
        let before = rig.wal.live_wal_bytes(&rig.session).unwrap();
        let response = rig.call("session.send", serde_json::json!({"send_id":"refused","prompt":"hello",
            "model":{"provider":"openai","model":"test-ultrafast"},"plan":selection_plan(Some(model))})).await;
        assert_eq!(response.header.ty, subc_protocol::FrameType::Error, "{response:?}");
        assert_eq!(tier_reply(&response)["detail"], detail);
        assert_eq!(rig.wal.live_wal_bytes(&rig.session).unwrap(), before);
        assert!(matches!(rig.wal.read_records(&rig.session),
            Err(broca_wal::WalError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound));
        assert!(http.requests.lock().unwrap().is_empty());
        rig.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// The queue record a tiered send leaves behind when it waits for a run already in flight: the
/// same durable bytes the served path writes, taken from the WAL rather than composed by hand.
async fn served_queue_record(tier: &str, send_id: &str) -> broca_wal::WalRecord {
    use broca_core::WalStore as _;
    let root = fresh_vault_root(&format!("tier-served-{tier}"));
    let release = Arc::new(tokio::sync::Notify::new());
    let http = tier_http_fixture(vec![None], false, Some(release.clone()), None).await;
    let mut rig = TierServeRig::start(&root, &http.url).await;
    let run = tier_run(&rig.send("held", Some(tier)).await);
    broca_test_support::wait_until("held request reaches HTTP", || {
        std::future::ready(if http.requests.lock().unwrap().len() == 1 {
            Ok(())
        } else {
            Err("not received".to_owned())
        })
    })
    .await;
    let queued = rig.send(send_id, None).await;
    assert_eq!(tier_reply(&queued)["result"]["state"], "pending");
    broca_test_support::wait_until("served queue record is durable", || {
        let frames = rig.wal.read_records(&rig.session).unwrap();
        std::future::ready(
            if frames
                .iter()
                .any(|frame| matches!(&frame.record, broca_wal::WalRecord::Queued { .. }))
            {
                Ok(())
            } else {
                Err("queued record is not readable yet".to_owned())
            },
        )
    })
    .await;
    let record = rig
        .wal
        .read_records(&rig.session)
        .unwrap()
        .into_iter()
        .find_map(|frame| match frame.record {
            record @ broca_wal::WalRecord::Queued { .. } => Some(record),
            _ => None,
        })
        .unwrap();
    release.notify_one();
    rig.ended(&run).await;
    rig.close().await;
    std::fs::remove_dir_all(root).unwrap();
    record
}

async fn tier_http_fixture(reports: Vec<Option<&'static str>>,
    two_steps: bool,
    hold: Option<Arc<tokio::sync::Notify>>,
    plane: Option<Arc<TierCompaction>>,
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
            if let Some(plane) = &plane {
                let count = plane
                    .rides
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|ride| {
                        ride.cause
                            == broca_core::compaction_provider::ride::RideCause::ServiceTierChange
                    })
                    .count();
                plane.request_ride_counts.lock().unwrap().push(count);
            }
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
    install_queued_ultrafast_catalog();
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
        let http = tier_http_fixture(reports.clone(), reports.len() == 2, None, None).await;
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

#[tokio::test]
async fn service_tier_fixed_run_and_writer_queue_survive_a_restart_cut() {
    use broca_core::WalStore as _;
    if !run_catalog_drift_child("service_tier_fixed_run_and_writer_queue_survive_a_restart_cut") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    install_queued_ultrafast_catalog();
    let root = fresh_vault_root("tier-fixed-queue");
    let release = Arc::new(tokio::sync::Notify::new());
    let http = tier_http_fixture(vec![None, None, None], false, Some(release.clone()), None).await;
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
    // The retry below must find this queue record in the WAL, not in whatever the actor still
    // holds in memory, so wait for the record itself to be readable.
    broca_test_support::wait_until("queued record is durable", || {
        let frames = rig.wal.read_records(&rig.session).unwrap();
        std::future::ready(
            if frames
                .iter()
                .any(|frame| matches!(&frame.record, broca_wal::WalRecord::Queued { .. }))
            {
                Ok(())
            } else {
                Err("queued record is not readable yet".to_owned())
            },
        )
    })
    .await;
    let repeated = rig.send("queued-ultra", None).await;
    assert_eq!(
        queued.body, repeated.body,
        "inheriting a tier must not alter caller retry identity"
    );
    let queued_record = rig
        .wal
        .read_records(&rig.session)
        .unwrap()
        .into_iter()
        .find_map(|frame| match frame.record {
            record @ broca_wal::WalRecord::Queued { .. } => Some(record),
            _ => None,
        })
        .unwrap();
    let broca_wal::WalRecord::Queued { request, .. } = &queued_record else {
        unreachable!()
    };
    assert_eq!(request.service_tier_level.as_deref(), Some("ultrafast"));
    assert!(queued_record
        .required_features()
        .contains(&"service-tier-queue/v1"));
    release.notify_one();
    rig.ended(&run).await;
    broca_test_support::wait_until("queued ultrafast finishes", || {
        let frames = rig.wal.read_records(&rig.session).unwrap();
        std::future::ready(
            if frames
                .iter()
                .filter(|f| matches!(&f.record, broca_wal::WalRecord::RunFinished { .. }))
                .count()
                == 2
            {
                Ok(())
            } else {
                Err("queued run has not ended".to_owned())
            },
        )
    })
    .await;
    rig.close().await;

    // Reopen the exact durable queue record from the enqueue cut, with no actor cache. The cut
    // session is created through the rig's own bind, as a real client creates one: the session
    // gets the address the bind resolves and its inventory row. A WAL written under a
    // folder-derived address with no inventory row is a state production never produces, so it
    // would test the addressing rather than the queue drain.
    let cut_root = fresh_vault_root("tier-queue-cut");
    let cut = TierServeRig::start(&cut_root, &http.url).await;
    let mut writer = cut.wal.open(&cut.session).unwrap();
    writer.append(1, &queued_record, true).unwrap();
    drop(writer);
    let cut_session = cut.session.clone();
    cut.close().await;
    let reopened = TierServeRig::start(&cut_root, &http.url).await;
    assert_eq!(
        reopened.session, cut_session,
        "the reopened rig binds the session the cut wrote"
    );
    reopened
        .registry
        .actor_for(&reopened.session)
        .await
        .unwrap();
    broca_test_support::wait_until("reopened tier queue finishes", || {
        let frames = reopened.wal.read_records(&reopened.session).unwrap();
        std::future::ready(
            if frames
                .iter()
                .any(|f| matches!(&f.record, broca_wal::WalRecord::RunFinished { .. }))
            {
                Ok(frames)
            } else {
                Err("reopened run has not ended".to_owned())
            },
        )
    })
    .await
    .into_iter()
    .find_map(|frame| match frame.record {
        broca_wal::WalRecord::RunStarted { config, .. } => {
            assert_eq!(config.frozen_render_config["service_tier"], "ultrafast");
            Some(())
        }
        _ => None,
    })
    .expect("queue consumed into RunStarted");
    assert_eq!(http.requests.lock().unwrap().len(), 3);
    for request in http.requests.lock().unwrap().iter() {
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["service_tier"], "ultrafast");
    }
    reopened.close().await;
    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_dir_all(cut_root).unwrap();
}

/// A session written before subc-protocol 0.34 keeps the address it was given then. Its bind
/// carried a folder, so that address is derived from the folder and its session-ref row records
/// it. After the upgrade the bind carries no folder at all, so nothing about the bind can name
/// that address; the row is the only thing that can, and it must.
#[tokio::test]
async fn queued_tier_send_drains_after_upgrading_a_legacy_folder_addressed_session() {
    use broca_core::{SessionRefIndex as _, WalStore as _};
    if !run_catalog_drift_child(
        "queued_tier_send_drains_after_upgrading_a_legacy_folder_addressed_session",
    ) {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    install_queued_ultrafast_catalog();

    // The pre-0.34 session: a folder-derived address, its WAL, and the session-ref row that
    // recorded the pair then.
    let root = fresh_vault_root("tier-legacy-addressed");
    let legacy = BindIdentity::new(&root, "tier-test", "session");
    let wal = FileWalStore::new(root.join("wal"));
    let queued = served_queue_record("ultrafast", "legacy-queued").await;
    let broca_wal::WalRecord::Queued { .. } = &queued else {
        unreachable!("a served send records its queue entry")
    };
    let mut writer = wal.open(&legacy).unwrap();
    writer.append(1, &queued, true).unwrap();
    drop(writer);
    broca_store::SqliteStore::open(root.join("store.db"))
        .unwrap()
        .record_session(&broca_core::session_addr(&legacy), &legacy)
        .unwrap();

    // The upgrade: a rig on this line binds the same harness and session name with no folder.
    let http = tier_http_fixture(vec![None], false, None, None).await;
    let reopened = TierServeRig::start(&root, &http.url).await;
    assert_eq!(
        reopened.session, legacy,
        "the recorded pair resolves to the address the legacy session was written under"
    );
    assert_eq!(
        broca_core::wal_writer::session_addr(&reopened.session),
        broca_core::wal_writer::session_addr(&legacy)
    );
    reopened
        .registry
        .actor_for(&reopened.session)
        .await
        .unwrap();
    broca_test_support::wait_until("legacy queued send drains after the upgrade", || {
        let frames = reopened.wal.read_records(&reopened.session).unwrap();
        std::future::ready(
            if frames
                .iter()
                .any(|frame| matches!(&frame.record, broca_wal::WalRecord::RunFinished { .. }))
            {
                Ok(frames)
            } else {
                Err("legacy queued send has not drained".to_owned())
            },
        )
    })
    .await
    .into_iter()
    .find_map(|frame| match frame.record {
        broca_wal::WalRecord::RunStarted { config, .. } => {
            assert_eq!(config.frozen_render_config["service_tier"], "ultrafast");
            Some(())
        }
        _ => None,
    })
    .expect("the legacy queue entry was consumed into a RunStarted");
    assert_eq!(http.requests.lock().unwrap().len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&http.requests.lock().unwrap()[0].body)
        .unwrap();
    assert_eq!(body["service_tier"], "ultrafast");
    reopened.close().await;
    std::fs::remove_dir_all(root).unwrap();
}


#[test]
fn service_tier_switch_off_pins_master_log_discovery_usage_and_request_bytes() {
    use broca_module_serve::wire::SendParamsExt as _;
    use broca_provider::wire::WireFamily as _;
    if !run_catalog_drift_child(
        "service_tier_switch_off_pins_master_log_discovery_usage_and_request_bytes",
    ) {
        return;
    }
    assert!(!broca_protocol::service_tier_enabled());
    broca_protocol::configure_service_tier(false).unwrap();
    let params: broca_wire::SendParams =
        serde_json::from_value(serde_json::json!({"prompt":"hello",
        "model":{"provider":"openai","model":"test-ultrafast"}}))
        .unwrap();
    let req = params
        .into_send_request(
            BindIdentity::new("/project", "test", "session"),
            RunId("off-run".into()),
        )
        .unwrap();
    assert!(req.request.service_tier_level.is_none());
    let queued: broca_wal::WalRecord = serde_json::from_value(
        serde_json::json!({"type":"queued","submission_id":"sub-off","request":req.request}),
    )
    .unwrap();
    let legacy = r#"{"type":"queued","submission_id":"sub-off","request":{"model":{"provider_module_id":"openai","model_id":"test-ultrafast"},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#;
    assert_eq!(serde_json::to_string(&queued).unwrap(), legacy);
    assert!(queued.required_features().is_empty());

    // The golden's bytes are pinned by digest in broca-wire's role tests; this test proves the
    // switch-off description equals that golden.
    let golden = include_bytes!("../../broca-wire/tests/golden/role_describe.json");
    let described =
        broca_wire::broca_role_describe_with_writers("0.0.0-writer-golden", true, true, true);
    assert_eq!(
        serde_json::to_value(described).unwrap(),
        serde_json::from_slice::<serde_json::Value>(golden).unwrap()
    );

    let mut started = run_started_with_user_text(
        &BindIdentity::new("/project", "test", "session"),
        "off-run",
        "hello",
    );
    if let broca_wal::WalRecord::RunStarted { config, .. } = &mut started {
        config.model = broca_protocol::ModelHandle::new("mock", "mock-model");
        config.frozen_render_config =
            serde_json::json!({"family_id":"openai_chat","service_tier":"priority"});
    }
    let step: broca_wal::WalRecord = serde_json::from_value(serde_json::json!({"type":"model_step_finished","step_id":1,
        "assistant_message":{"message_id":"off-assistant","content":[{"type":"text","text":"done"}]},
        "usage":{},"service_tier_used":"priority"})).unwrap();
    let mut projection = broca_core::RunUsageProjection::new(RunId("off-run".into()));
    projection.observe(&started);
    projection.observe(&step);
    let usage = r#"{"steps":1,"unreported_steps":1,"by_model":[{"provider":"mock","model":"mock-model","steps":1,"unreported_steps":1}]}"#;
    assert_eq!(serde_json::to_string(&projection.usage).unwrap(), usage);
    let status = broca_wire::RunStatusResponse::Active {
        auth_selection: None,
        reason: None,
        deadline_ms: None,
        usage: Some(projection.usage.clone()),
        step: None,
        title: None,
        awaiting_approval: None,
        held_calls: vec![],
    };
    assert_eq!(
        serde_json::to_string(&status).unwrap(),
        format!(r#"{{"state":"active","usage":{usage}}}"#)
    );
    let result = broca_wire::RunResultResponse {
finish: None,
        auth_selection: None,
        directives: None,
        usage: projection.usage,
        detail: None,
        context_ceiling: None,
        run_id: "off-run".into(),
        state: "interrupted".into(),
        reason: Some("runner_shutdown".into()),
        error: None,
        final_message: None,
        structured_output: None,
    };
    assert_eq!(
        serde_json::to_string(&result).unwrap(),
        format!(
            r#"{{"usage":{usage},"run_id":"off-run","state":"interrupted","reason":"runner_shutdown"}}"#
        )
    );
    let frozen = broca_provider::families::frozen_config_for_family(
        WireFamilyId::OpenAiChat,
        "test-ultrafast",
    );
    let body = broca_provider::families::OpenAiChat
        .render(&frozen, &broca_protocol::CallOptions::new(vec![]));
    assert_eq!(
        std::str::from_utf8(&body).unwrap(),
        r#"{"max_tokens":2048,"messages":[],"model":"test-ultrafast","stream":true,"stream_options":{"include_usage":true}}"#
    );
}

#[test]
fn service_tier_billed_refusal_reports_and_anthropic_speed_vote_truthfully() {
    if !run_catalog_drift_child(
        "service_tier_billed_refusal_reports_and_anthropic_speed_vote_truthfully",
    ) {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    let mut started = run_started_with_user_text(
        &BindIdentity::new("/project", "test", "session"),
        "tier-run",
        "hello",
    );
    if let broca_wal::WalRecord::RunStarted { config, .. } = &mut started {
        config.frozen_render_config =
            serde_json::json!({"family_id":"anthropic_messages","speed":"fast"});
    }
    let attempt: broca_wal::WalRecord = serde_json::from_value(serde_json::json!({
        "type":"model_attempt_finished", "step_id":1, "attempt":0,
        "request_body_len":1,"request_body_sha256":"00","response_body_len":8,"response_body_sha256":"00",
        "outcome":"complete","ts_ms":1,
        "envelope":{"sent":true,"outcome":"complete","response_bytes":8,"service_tier_used":"priority","speed_used":"standard"}
    })).unwrap();
    let refused: broca_wal::WalRecord = serde_json::from_value(serde_json::json!({
        "type":"model_step_refused","step_id":1,"usage":{},"discarded_blocks":0,
        "schedule":{"source":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},
            "fallback":{"provider_module_id":"anthropic","model_id":"test-fallback"},
            "phase":{"kind":"fallback","remaining":20},"re_refusals":1}
    }))
    .unwrap();
    let completed: broca_wal::WalRecord = serde_json::from_value(serde_json::json!({
        "type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"answer","content":[]},
        "usage":{},"service_tier_used":"priority","speed_used":"fast"
    })).unwrap();
    let mut projection = broca_core::RunUsageProjection::new(RunId("tier-run".into()));
    for record in [&started, &attempt, &refused] {
        projection.observe(record);
    }
    assert_eq!(
        projection.usage.service_tier_applied.as_deref(),
        Some("fast")
    );
    assert_eq!(projection.usage.service_tier_served.as_deref(), Some("standard"), "speed, not an Anthropic priority-commitment label, states whether fast mode served the call");
    projection.observe(&completed);
    assert!(
        projection.usage.service_tier_served.is_none(),
        "the billed source and fallback reports conflict"
    );
    let mut cold = broca_core::RunUsageProjection::new(RunId("tier-run".into()));
    for record in [&started, &attempt, &refused, &completed] {
        cold.observe(record);
    }
    assert_eq!(cold.usage, projection.usage);
}

#[tokio::test]
async fn service_tier_enabled_resume_uses_frozen_bytes_with_no_catalog() {
    if !run_catalog_drift_child("service_tier_enabled_resume_uses_frozen_bytes_with_no_catalog") {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    broca_catalog::install_global_catalog_cache(Arc::new(broca_catalog::CatalogCache::empty()));
    let server = resume_http_fixture().await;
    let root = fresh_vault_root("writer-frozen-tier-resume");
    let (summary, _, records) = execute_recorded_fixture(&root, &server, |config| {
        config.frozen_render_config["service_tier"] = "ultrafast".into();
    })
    .await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    let body: serde_json::Value =
        serde_json::from_slice(&server.requests.lock().unwrap()[0].body).unwrap();
    assert_eq!(body["service_tier"], "ultrafast");
    assert!(!records
        .iter()
        .any(|frame| matches!(frame.record, broca_wal::WalRecord::RunConfigRefrozen { .. })));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn service_tier_compaction_first_fast_request_has_no_change_to_ride() {
    if !run_catalog_drift_child("service_tier_compaction_first_fast_request_has_no_change_to_ride")
    {
        return;
    }
    broca_protocol::configure_service_tier(true).unwrap();
    install_queued_ultrafast_catalog();
    let root = fresh_vault_root("tier-first-fast");
    let plane = Arc::new(TierCompaction::default());
    let http = tier_http_fixture(vec![None], false, None, Some(plane.clone())).await;
    let mut rig = TierServeRig::start_with_plane(&root, &http.url, Some(plane.clone())).await;
    let run = tier_run(&rig.send("first-fast", Some("fast")).await);
    rig.ended(&run).await;
    assert_eq!(*plane.request_ride_counts.lock().unwrap(), vec![0]);
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
    install_queued_ultrafast_catalog();
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
