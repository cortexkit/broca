use super::*;
use broca_module::ServeHandler;

const READER: &str = "synthetic-reader";
const STRANGER: &str = "synthetic-stranger";

pub(super) struct RunGate {
    held: std::sync::atomic::AtomicBool,
    pub(super) entered: tokio::sync::Semaphore,
    pub(super) release: tokio::sync::Semaphore,
}

impl RunGate {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            held: true.into(),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        })
    }

    /// Holds the next model request too: the gate holds only the first one it sees.
    pub(super) fn hold_next(&self) {
        self.held.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

pub(super) struct GatedProvider<'a> {
    pub provider: &'a broca_core::provider::StubProvider,
    pub gate: Option<&'a RunGate>,
    pub requests: &'a std::sync::Mutex<Vec<Vec<u8>>>,
    /// Whether each step reports the attempt a transport would, carrying the digest of the bytes
    /// the provider renders, so the step's request can be rebuilt and checked against it.
    pub attempts: bool,
}

#[async_trait]
impl broca_core::Provider for GatedProvider<'_> {
    async fn step(
        &self,
        step: broca_protocol::StepId,
        options: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        if let Some(gate) = self
            .gate
            .filter(|gate| gate.held.swap(false, std::sync::atomic::Ordering::SeqCst))
        {
            gate.entered.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
        let body = self.provider.render_request(options);
        let digest = broca_core::render::request_digest_hex(&body);
        self.requests.lock().unwrap().push(body);
        let inner = self.provider.step(step, options).await;
        if !self.attempts {
            return inner;
        }
        let attempt = serde_json::from_value(serde_json::json!({
            "attempt": 0,
            "request_body_len": 10,
            "request_body_sha256": digest,
            "http_status": 200,
            "response_body_len": 0,
            "response_body_sha256": "0".repeat(64),
            "outcome": "complete",
            "ts_ms": 1,
            "sent_at_ms": 1,
        }))
        .unwrap();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            let mut inner = inner;
            if tx
                .send(NormalizedEvent::Control(
                    broca_protocol::ProviderControl::AttemptFinished { attempt },
                ))
                .await
                .is_err()
            {
                return;
            }
            while let Some(event) = inner.recv().await {
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}
const READS: [&str; 7] = [
    "session.head",
    "session.read",
    "session.plan_view",
    "session.history",
    "run.status",
    "role.describe",
    "session.subscribe",
];

pub(super) fn principal(module: &str) -> Principal {
    Principal::Reserved {
        module_id: module.into(),
    }
}

fn readers_plan(readers: Option<Value>, compaction: bool) -> Value {
    let composition = if compaction {
        json!({"providers":[{"provider":PROVIDER,"tools":[]}],"compaction":{"provider":PROVIDER}})
    } else {
        json!({"providers":[]})
    };
    let digest = cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap();
    let mut plan = json!({"tool_items":[],"system_text_items":[],"step_transform_items":[],
        "composition":composition,"composition_digest":digest});
    if let Some(readers) = readers {
        plan["readers"] = readers;
    }
    if compaction {
        plan["compaction_item"] = json!({"provider":PROVIDER,"preset":"default","params":{}});
    }
    plan
}

struct EmptyAdmitter;

#[async_trait]
impl broca_fetch_plan::PlanAdmitter for EmptyAdmitter {
    async fn admit(
        &self,
        plan: &broca_fetch_plan::Plan,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
    ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
        Ok(broca_fetch_plan::Admitted {
            manifest: manifest(plan),
        })
    }
}

fn manifest(plan: &broca_fetch_plan::Plan) -> broca_records::record::PlanManifest {
    broca_records::record::PlanManifest {
        rich_ui: None,
        plan: broca_records::record::OpaqueJson::from_json_text(plan.text.clone()).unwrap(),
        plan_identity: plan.identity.clone(),
        composition_digest: plan.composition_digest.clone(),
        fetches: vec![],
        tools: Arc::new(vec![]),
        system_text: String::new(),
        system_digest: digest(b""),
        join_version: 1,
        role_versions: Default::default(),
        declarations: vec![],
    }
}

fn frozen(plan: &Value) -> SendPlan {
    let parsed =
        broca_fetch_plan::parse_plan(plan, broca_fetch_plan::CompactionSupport::Supported).unwrap();
    SendPlan {
        identity: parsed.identity.clone(),
        manifest: Some(Arc::new(manifest(&parsed))),
        model_check: None,
    }
}

async fn seed(rig: &Rig, plan: &Value, run: &str) {
    let mut req = request("synthetic prompt");
    req.principal = Some(broca_protocol::BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    });
    req.scope = broca_protocol::SendScope::DaemonWithoutScopes;
    let outcome = rig
        .registry
        .route_submission_with_order(
            &rig.key,
            RunId(run.into()),
            req,
            RunKind::FreshOrResume,
            SendKey::Minted,
            false,
            broca_protocol::Delivery::Queue,
            Some(frozen(plan)),
            None,
            None,
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    rig.settled(run).await;
}

fn params(op: &str) -> Value {
    match op {
        "run.status" => json!({"run_id":"r1"}),
        "session.send_status" => json!({"send_id":"owner-send"}),
        "session.subscribe" => json!({"from":"start"}),
        _ => json!({}),
    }
}

async fn metrics(rig: &mut Rig) -> Value {
    let frame = rig
        .request(
            0,
            serde_json::to_value(ModuleControlRequest::HealthCheck {}).unwrap(),
        )
        .await;
    let body: Value = serde_json::from_slice(&frame.body).unwrap();
    assert!(body["metrics"]["actor_opens"].is_number(), "{body}");
    body["metrics"].clone()
}

fn sample(name: &str, bytes: &[u8]) {
    if let Some(dir) = std::env::var_os("BROCA_APP_READERS_SAMPLES") {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(name), bytes).unwrap();
    }
}

#[tokio::test]
async fn app_readers_serve_matrix_live_and_stored() {
    // Lifetime IO and dispatch counters must not race other tests in a shared libtest process.
    if std::env::var_os("BROCA_APP_READERS_MATRIX_CHILD").is_none() {
        let output = broca_test_support::wait_for(
            "isolated reader admission matrix",
            tokio::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "store_ops_serve::app_readers::app_readers_serve_matrix_live_and_stored",
                    "--nocapture",
                ])
                .env("BROCA_APP_READERS_MATRIX_CHILD", "1")
                .kill_on_drop(true)
                .output(),
            || "reader matrix child did not exit".into(),
        )
        .await
        .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let mut rig = Rig::start(
        "app-readers-matrix",
        vec![text_step("a", "synthetic answer")],
    )
    .await;
    let plan = readers_plan(Some(json!([READER])), true);
    seed(&rig, &plan, "r1").await;
    rig.assert_store_only();
    rig.bind_session(1, core()).await;
    rig.bind_session(2, principal(READER)).await;
    rig.bind_session(3, provider()).await;
    rig.bind_session(4, Principal::Direct).await;
    rig.bind_session(5, principal(STRANGER)).await;
    for open in [false, true] {
        if open {
            rig.registry.actor_for(&rig.key).await.unwrap();
        } else {
            broca_test_support::wait_until("reader session closes", || async {
                let state = rig.registry.slot_state(&rig.key);
                if state.is_none() {
                    Ok(())
                } else {
                    Err(format!("slot: {state:?}"))
                }
            })
            .await;
        }
        for op in READS.into_iter().chain(["session.send_status"]) {
            let before = metrics(&mut rig).await;
            let page_reads = broca_session_store::metrics::snapshot()["read"].count;
            let refused = rig
                .request(
                    5,
                    json!({"method":op,"params":params(op),"principal":READER}),
                )
                .await;
            assert_eq!(refused.header.ty, FrameType::Error, "{op}");
            let error: Value = serde_json::from_slice(&refused.body).unwrap();
            assert_eq!(error["code"], "scope_owner_mismatch", "{op}: {error}");
            let after = metrics(&mut rig).await;
            assert!(before["management_ops"]
                .as_object()
                .is_some_and(|counts| !counts.is_empty()));
            assert_eq!(
                after["management_ops"], before["management_ops"],
                "{op} dispatched after refusal"
            );
            assert_eq!(
                after["actor_opens"], before["actor_opens"],
                "{op} opened an actor"
            );
            assert_eq!(
                after["session_reads"], before["session_reads"],
                "{op} read session contents"
            );
            assert_eq!(
                broca_session_store::metrics::snapshot()["read"].count,
                page_reads,
                "{op} reached the new-storage page handler"
            );
            if op == "session.read" {
                sample("session-read-unlisted-refusal.json", &refused.body);
            }
            if op == "session.send_status" {
                continue;
            }
            for channel in 1..=4 {
                let frame = rig
                    .request(channel, json!({"method":op,"params":params(op)}))
                    .await;
                assert!(
                    matches!(frame.header.ty, FrameType::Response | FrameType::StreamData),
                    "{op} caller {channel}: {}",
                    String::from_utf8_lossy(&frame.body)
                );
            }
        }
        let owner = rig
            .request(1, json!({"method":"session.read","params":{}}))
            .await;
        let listed = rig
            .request(2, json!({"method":"session.read","params":{}}))
            .await;
        assert_eq!(
            listed.body, owner.body,
            "reader policy changed transcript bytes"
        );
        sample("session-read-owner.json", &owner.body);
        sample("session-read-listed.json", &listed.body);
    }
}

#[tokio::test]
async fn app_readers_absence_preserves_admission_and_unattested_callers_fail_closed() {
    let mut rig = Rig::start("app-readers-absent", vec![text_step("a", "answer")]).await;
    let plan = readers_plan(None, false);
    seed(&rig, &plan, "r1").await;
    rig.bind_session(1, core()).await;
    rig.bind_session(2, principal(STRANGER)).await;
    rig.bind_session(3, Principal::Unverified).await;
    rig.bind_session(4, principal("basal")).await;
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 6,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new("runner", "head"),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    assert_eq!(
        rig.request(0, serde_json::to_value(bind).unwrap())
            .await
            .header
            .ty,
        FrameType::Response
    );
    for op in READS {
        for channel in [1, 2, 3, 6] {
            let frame = rig
                .request(channel, json!({"method":op,"params":params(op)}))
                .await;
            assert!(
                matches!(frame.header.ty, FrameType::Response | FrameType::StreamData),
                "{op}: {frame:?}"
            );
        }
        assert_eq!(
            rig.refused(4, op, params(op)).await,
            "sender_not_owner",
            "{op}"
        );
    }
    let restricted = readers_plan(Some(json!([])), false);
    rig.registry
        .route_session_refresh(
            &rig.key,
            1,
            "on_prefix_rebuild".into(),
            Box::new(Arc::try_unwrap(frozen(&restricted).manifest.unwrap()).unwrap()),
            Vec::new(),
        )
        .await
        .unwrap();
    for op in READS {
        for channel in [2, 3, 4, 6] {
            assert_eq!(
                rig.refused(channel, op, params(op)).await,
                "scope_owner_mismatch",
                "{op}"
            );
        }
    }
}

#[tokio::test]
async fn app_readers_empty_list_closes_a_plan_senders_general_reads_only() {
    let mut rig = Rig::start(
        "app-readers-empty-switch",
        vec![text_step("a", "synthetic answer")],
    )
    .await;
    rig.registry.set_plan_admitter(Arc::new(EmptyAdmitter));
    let mut plan = readers_plan(None, true);
    plan["senders"] = json!([{"module":"occipital"}]);
    seed(&rig, &plan, "r1").await;
    rig.bind_session(1, core()).await;
    rig.bind_session(2, principal("occipital")).await;
    rig.bind_session(3, provider()).await;
    rig.bind_session(4, Principal::Direct).await;
    for op in READS {
        let frame = rig
            .request(2, json!({"method":op,"params":params(op)}))
            .await;
        assert!(
            matches!(frame.header.ty, FrameType::Response | FrameType::StreamData),
            "a sender's general reads keep today's admission without readers: {op}: {frame:?}"
        );
    }
    plan["readers"] = json!([]);
    response(&refresh_wire(&mut rig, &plan, 1).await);
    for op in READS {
        assert_eq!(
            rig.refused(2, op, params(op)).await,
            "scope_owner_mismatch",
            "being a plan sender does not grant general reads under an empty reader list: {op}"
        );
        for channel in [1, 3, 4] {
            let frame = rig
                .request(channel, json!({"method":op,"params":params(op)}))
                .await;
            assert!(
                matches!(frame.header.ty, FrameType::Response | FrameType::StreamData),
                "implicit reader {channel} lost {op}: {frame:?}"
            );
        }
    }
}

struct SurfaceAdmitter;

#[async_trait]
impl broca_fetch_plan::PlanAdmitter for SurfaceAdmitter {
    async fn admit(
        &self,
        plan: &broca_fetch_plan::Plan,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
    ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
        let mut manifest = manifest(plan);
        manifest.system_text = "Stable synthetic system prefix.".into();
        manifest.system_digest = digest(manifest.system_text.as_bytes());
        manifest.tools = Arc::new(vec![serde_json::from_value(json!({
            "name":"read","description":"Read a synthetic file","module":"synthetic-surface",
            "input_schema":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}
        })).unwrap()]);
        Ok(broca_fetch_plan::Admitted { manifest })
    }
}

fn surface_plan() -> Value {
    let composition = json!({"providers":[{"provider":"synthetic-surface",
        "tools":[{"name":"read","capabilities":[]}]}],"step_transform_items":[]});
    json!({"tool_items":[{"provider":"synthetic-surface","params":{}}],
        "system_text_items":[{"provider":"synthetic-surface","preset":"head","params":{}}],
        "step_transform_items":[],"composition_digest":
        cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),"composition":composition})
}

async fn requests_around_reader_switch(policy: Option<&str>) -> Vec<Vec<u8>> {
    let gate = RunGate::new();
    let mut rig = Rig::start_gated(
        &format!("app-readers-request-{}", policy.unwrap_or("control")),
        vec![
            text_step("a", "same first answer"),
            text_step("b", "same next answer"),
        ],
        ReadyProvider::default(),
        None,
        Some(gate.clone()),
        false,
    )
    .await;
    rig.registry.set_plan_admitter(Arc::new(SurfaceAdmitter));
    rig.bind_session(1, core()).await;
    rig.bind_session(2, principal(READER)).await;
    rig.bind_session(3, principal(STRANGER)).await;
    let mut plan = surface_plan();
    let first = send_wire(&mut rig, 1, "first", Some(&plan), None).await;
    let first_run = response(&first)["run_id"].as_str().unwrap().to_owned();
    broca_test_support::wait_for(
        "first model request is held",
        gate.entered.acquire(),
        || "model has not entered".into(),
    )
    .await
    .unwrap()
    .forget();
    response(
        &rig.request(3, json!({"method":"session.read","params":{}}))
            .await,
    );
    assert!(rig.requests.lock().unwrap().is_empty());
    if let Some(policy) = policy {
        plan["readers"] = json!([READER]);
        let refreshed = rig
            .request(
                1,
                json!({"method":"session.refresh","params":{
            "plan":plan,"generation":1,"policy":policy}}),
            )
            .await;
        assert_eq!(response(&refreshed)["run_in_progress"], true);
        response(
            &rig.request(2, json!({"method":"session.read","params":{}}))
                .await,
        );
        assert_eq!(
            rig.refused(3, "session.read", json!({})).await,
            "scope_owner_mismatch",
            "the newly present reader list must govern before another run starts"
        );
    }
    assert!(
        rig.requests.lock().unwrap().is_empty(),
        "refresh issued a model request"
    );
    gate.release.add_permits(1);
    rig.settled(&first_run).await;
    let next = send_wire(&mut rig, 1, "next", None, None).await;
    let next_run = response(&next)["run_id"].as_str().unwrap().to_owned();
    rig.settled(&next_run).await;
    let sent = rig.requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 2, "one model request per run");
    for (index, bytes) in sent.iter().enumerate() {
        let request: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            request["tools"][0]["name"], "read",
            "the fixture must exercise nonempty tools"
        );
        assert_eq!(
            request["prompt"][0]["content"][0]["text"],
            "Stable synthetic system prefix."
        );
        assert_eq!(
            request["prompt"].as_array().unwrap().len(),
            if index == 0 { 2 } else { 4 },
            "no system note or replacement prefix message may enter the request"
        );
    }
    sent
}

#[tokio::test]
async fn app_readers_switch_on_at_acceptance_without_changing_model_request_bytes() {
    let control = requests_around_reader_switch(None).await;
    for policy in ["immediately", "on_history_rebuild"] {
        let refreshed = requests_around_reader_switch(Some(policy)).await;
        assert_eq!(refreshed,control,
            "a readers-only refresh changed the running or next run's exact provider request bytes under {policy}");
    }
}

pub(super) async fn send_wire(
    rig: &mut Rig,
    channel: u16,
    id: &str,
    plan: Option<&Value>,
    delivery: Option<Value>,
) -> Frame {
    let mut params =
        json!({"send_id":id,"prompt":"synthetic prompt","model":{"provider":"stub","model":"m"}});
    if let Some(plan) = plan {
        params["plan"] = plan.clone();
    }
    if let Some(delivery) = delivery {
        params["delivery"] = delivery;
    }
    rig.request(channel, json!({"method":"session.send","params":params}))
        .await
}

async fn refresh_wire(rig: &mut Rig, plan: &Value, generation: u64) -> Frame {
    rig.request(
        1,
        json!({"method":"session.refresh","params":{
        "plan":plan,"generation":generation,"policy":"on_history_rebuild"}}),
    )
    .await
}

pub(super) fn response(frame: &Frame) -> Value {
    assert_eq!(
        frame.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    serde_json::from_slice::<Value>(&frame.body).unwrap()["result"].clone()
}

#[tokio::test]
async fn app_readers_send_status_uses_current_senders_and_never_grants_writes() {
    let mut rig = Rig::start(
        "app-readers-send-status",
        vec![text_step("a", "owner"), text_step("b", "sender")],
    )
    .await;
    rig.registry.set_plan_admitter(Arc::new(EmptyAdmitter));
    rig.bind_session(1, core()).await;
    rig.bind_session(2, principal(READER)).await;
    rig.bind_session(3, provider()).await;
    rig.bind_session(4, principal("synthetic-sender")).await;
    rig.bind_session(5, principal("synthetic-successor")).await;
    let mut plan = readers_plan(Some(json!([READER])), true);
    plan["senders"] = json!([{"module":"synthetic-sender","idle_start":true}]);
    let first = send_wire(&mut rig, 1, "owner-send", Some(&plan), None).await;
    let run = response(&first)["run_id"].as_str().unwrap().to_owned();
    rig.settled(&run).await;
    response(
        &rig.request(
            1,
            json!({"method":"session.send_status","params":{"send_id":"owner-send"}}),
        )
        .await,
    );
    for channel in [2, 3, 4] {
        assert_eq!(
            rig.refused(
                channel,
                "session.send_status",
                json!({"send_id":"owner-send"})
            )
            .await,
            "scope_owner_mismatch"
        );
    }
    let sent = send_wire(
        &mut rig,
        4,
        "sender-send",
        None,
        Some(json!(["idle_start"])),
    )
    .await;
    let sender_run = response(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.settled(&sender_run).await;
    response(
        &rig.request(
            4,
            json!({"method":"session.send_status","params":{"send_id":"sender-send"}}),
        )
        .await,
    );
    plan["senders"] =
        json!([{"module":"synthetic-successor","succeeds":"synthetic-sender","idle_start":true}]);
    response(&refresh_wire(&mut rig, &plan, 1).await);
    response(
        &rig.request(
            5,
            json!({"method":"session.send_status","params":{"send_id":"sender-send"}}),
        )
        .await,
    );
    assert_eq!(
        rig.refused(4, "session.send_status", json!({"send_id":"sender-send"}))
            .await,
        "scope_owner_mismatch"
    );
    assert_eq!(
        rig.refused(2, "run.cancel", json!({"run_id":run,"at":"now"}))
            .await,
        "scope_owner_mismatch"
    );
    assert_eq!(
        rig.refused(
            2,
            "session.retract",
            json!({"submission_id":"sub-synthetic"})
        )
        .await,
        "scope_owner_mismatch"
    );
    let denied = send_wire(&mut rig, 2, "reader-write", None, None).await;
    assert_eq!(
        serde_json::from_slice::<Value>(&denied.body).unwrap()["code"],
        "scope_owner_mismatch"
    );
}

#[tokio::test]
async fn app_readers_malformed_admission_writes_nothing() {
    let mut rig = Rig::start("app-readers-shape", vec![text_step("a", "answer")]).await;
    rig.registry.set_plan_admitter(Arc::new(EmptyAdmitter));
    rig.bind_session(1, core()).await;
    for malformed in [
        Value::Null,
        json!("reader"),
        json!([1]),
        json!([READER, READER]),
    ] {
        let plan = readers_plan(Some(malformed), false);
        let refused = send_wire(&mut rig, 1, "malformed", Some(&plan), None).await;
        let body: Value = serde_json::from_slice(&refused.body).unwrap();
        assert_eq!(refused.header.ty, FrameType::Error, "{body}");
        assert_eq!(body["code"], "invalid_params");
        assert_eq!(body["detail"]["field"], "plan.readers");
        assert!(rig.registry.slot_state(&rig.key).is_none());
        assert!(broca_records::session_storage::session_storage_class(
            &rig.dir,
            &broca_core::session_addr(&rig.key)
        )
        .unwrap()
        .is_none());
        sample("plan-readers-invalid-refusal.json", &refused.body);
    }
    let valid = readers_plan(Some(json!([])), false);
    let first = send_wire(&mut rig, 1, "valid", Some(&valid), None).await;
    let run = response(&first)["run_id"].as_str().unwrap().to_owned();
    rig.settled(&run).await;
    let before = rig.registry.plan_view(&rig.key).await.unwrap().unwrap();
    for malformed in [Value::Null, json!([false]), json!([READER, READER])] {
        let refused = refresh_wire(&mut rig, &readers_plan(Some(malformed), false), 1).await;
        let error: Value = serde_json::from_slice(&refused.body).unwrap();
        assert_eq!(error["code"], "invalid_params");
        assert_eq!(error["detail"]["field"], "plan.readers");
        assert_eq!(
            rig.registry.plan_view(&rig.key).await.unwrap().unwrap(),
            before
        );
    }
}

#[test]
fn app_readers_plan_parts_preserve_absent_bytes_and_present_lists() {
    use broca_session_loop::parts::MemoryParts;
    use broca_session_store::record::PartKind;
    let mut stored = manifest(
        &broca_fetch_plan::parse_plan(
            &readers_plan(None, false),
            broca_fetch_plan::CompactionSupport::Supported,
        )
        .unwrap(),
    );
    stored.plan = broca_records::record::OpaqueJson::from_json_text("{\"tool_items\":[]}").unwrap();
    let mut parts = MemoryParts::default();
    let mut setup = vec![];
    broca_session_loop::run::manifest_parts(&stored, &mut parts, &mut setup).unwrap();
    let reference = setup
        .iter()
        .find(|part| part.kind == PartKind::Plan)
        .unwrap();
    assert_eq!(parts.parts[&reference.digest], br#""{\"tool_items\":[]}""#);
    stored.plan =
        broca_records::record::OpaqueJson::from_json_text("{\"tool_items\":[],\"readers\":[]}")
            .unwrap();
    let mut setup = vec![];
    let rest = broca_session_loop::run::manifest_parts(&stored, &mut parts, &mut setup).unwrap();
    let rebuilt = broca_session_loop::run::stored_manifest(
        &broca_session_store::fold::operations::ManifestRef { parts: setup, rest },
        &parts,
    )
    .unwrap();
    assert_eq!(rebuilt.plan, stored.plan);
}

async fn write_request(rig: &mut Rig, channel: u16, body: Value) -> u64 {
    rig.corr += 1;
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        channel,
        1,
        rig.corr,
        serde_json::to_vec(&body).unwrap(),
    )
    .unwrap();
    write_frame(&mut rig.writer, &frame).await.unwrap();
    rig.corr
}

async fn capture_until(
    rig: &mut Rig,
    pending: &mut Vec<Frame>,
    channel: u16,
    corr: u64,
    error_only: bool,
) -> Frame {
    broca_test_support::wait_for(
        "subscription frame",
        async {
            loop {
                if let Some(index) = pending.iter().position(|frame| {
                    frame.header.channel == channel
                        && frame.header.corr == corr
                        && (!error_only || frame.header.ty == FrameType::Error)
                }) {
                    return pending.remove(index);
                }
                let frame = read_frame(&mut rig.reader).await.unwrap().unwrap();
                if frame.header.channel == channel
                    && frame.header.corr == corr
                    && (!error_only || frame.header.ty == FrameType::Error)
                {
                    return frame;
                }
                pending.push(frame);
            }
        },
        || format!("channel {channel}, corr {corr}"),
    )
    .await
}

async fn captured_rpc(rig: &mut Rig, pending: &mut Vec<Frame>, body: Value) -> Frame {
    captured_rpc_on(rig, pending, 1, body).await
}

async fn captured_rpc_on(
    rig: &mut Rig,
    pending: &mut Vec<Frame>,
    channel: u16,
    body: Value,
) -> Frame {
    let corr = write_request(rig, channel, body).await;
    capture_until(rig, pending, channel, corr, false).await
}

#[tokio::test]
async fn app_readers_refresh_is_immediate_and_revokes_only_the_removed_stream() {
    let gate = RunGate::new();
    let mut rig = Rig::start_gated(
        "app-readers-refresh",
        vec![text_step("a", "first"), text_step("b", "next")],
        ReadyProvider::default(),
        None,
        Some(gate.clone()),
        false,
    )
    .await;
    rig.registry.set_plan_admitter(Arc::new(EmptyAdmitter));
    rig.bind_session(1, core()).await;
    rig.bind_session(2, principal(READER)).await;
    rig.bind_session(3, core()).await;
    rig.bind_session(4, principal(STRANGER)).await;
    rig.bind_session(5, provider()).await;
    let mut plan = readers_plan(Some(json!([READER])), false);
    let first = send_wire(&mut rig, 1, "running", Some(&plan), None).await;
    let run = response(&first)["run_id"].as_str().unwrap().to_owned();
    broca_test_support::wait_for("model is held", gate.entered.acquire(), || {
        "model has not entered".into()
    })
    .await
    .unwrap()
    .forget();
    let mut pending = vec![];
    let removed_stream = write_request(
        &mut rig,
        2,
        json!({"method":"session.subscribe","params":{"from":"start"}}),
    )
    .await;
    assert_eq!(
        capture_until(&mut rig, &mut pending, 2, removed_stream, false)
            .await
            .header
            .ty,
        FrameType::StreamData
    );
    let owner_stream = write_request(
        &mut rig,
        3,
        json!({"method":"session.subscribe","params":{"from":"start"}}),
    )
    .await;
    assert_eq!(
        capture_until(&mut rig, &mut pending, 3, owner_stream, false)
            .await
            .header
            .ty,
        FrameType::StreamData
    );
    plan["readers"] = json!([STRANGER]);
    let refreshed = captured_rpc(
        &mut rig,
        &mut pending,
        json!({"method":"session.refresh","params":{
        "plan":plan,"generation":1,"policy":"on_history_rebuild"}}),
    )
    .await;
    assert_eq!(response(&refreshed)["run_in_progress"], true);
    let removed = capture_until(&mut rig, &mut pending, 2, removed_stream, true).await;
    let terminal: Value = serde_json::from_slice(&removed.body).unwrap();
    assert_eq!(terminal["code"], "scope_owner_mismatch", "{terminal}");
    assert_eq!(
        rig.refused(2, "session.read", json!({})).await,
        "scope_owner_mismatch"
    );
    response(
        &rig.request(4, json!({"method":"session.read","params":{}}))
            .await,
    );
    assert_eq!(
        rig.refused(5, "session.read", json!({})).await,
        "scope_owner_mismatch"
    );
    let mut enrolled = readers_plan(Some(json!([READER, STRANGER])), true);
    enrolled["senders"] = json!([]);
    let refreshed = captured_rpc(
        &mut rig,
        &mut pending,
        json!({"method":"session.refresh","params":{
        "plan":enrolled,"generation":2,"policy":"immediately"}}),
    )
    .await;
    assert_eq!(response(&refreshed)["run_in_progress"], true);
    response(
        &rig.request(2, json!({"method":"session.read","params":{}}))
            .await,
    );
    assert_eq!(
        rig.refused(5, "session.read", json!({})).await,
        "scope_owner_mismatch",
        "acceptance must not grant the next run's compaction provider access"
    );
    gate.release.add_permits(1);
    rig.settled(&run).await;
    let next = captured_rpc(
        &mut rig,
        &mut pending,
        json!({"method":"session.send","params":{
        "send_id":"next","prompt":"next synthetic run","model":{"provider":"stub","model":"m"}}}),
    )
    .await;
    let next_run = response(&next)["run_id"].as_str().unwrap().to_owned();
    rig.settled(&next_run).await;
    response(
        &captured_rpc_on(
            &mut rig,
            &mut pending,
            5,
            json!({"method":"session.read","params":{}}),
        )
        .await,
    );
    // The owner's surviving stream receives the next run, not merely events buffered before
    // the refresh removed the other reader.
    let next_event = broca_test_support::wait_for(
        "owner subscription sees the next run",
        async {
            loop {
                if let Some(index) = pending.iter().position(|frame| {
                    frame.header.channel == 3
                        && frame.header.corr == owner_stream
                        && String::from_utf8_lossy(&frame.body).contains(&next_run)
                }) {
                    return pending.remove(index);
                }
                pending.push(read_frame(&mut rig.reader).await.unwrap().unwrap());
            }
        },
        || format!("owner stream {owner_stream}, next run {next_run}"),
    )
    .await;
    assert_eq!(next_event.header.ty, FrameType::StreamData);
}

#[test]
fn app_readers_describe_serializer_samples() {
    let describe =
        broca_wire::broca_role_describe_with_writers("0.0.0-writer-golden", true, true, true);
    let phase = std::env::var("BROCA_APP_READERS_SAMPLE_PHASE").unwrap_or_else(|_| "after".into());
    sample(
        &format!("role-describe-{phase}.json"),
        &serde_json::to_vec(&describe).unwrap(),
    );
    assert!(describe
        .role
        .capabilities
        .iter()
        .any(|group| group == "app_readers"));
    cortexkit_role_llm_runner::describe::check_describe(&serde_json::to_value(&describe).unwrap())
        .unwrap();
}
