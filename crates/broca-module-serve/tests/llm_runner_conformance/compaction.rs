//! Real SDK compaction provider and canonical translation of received OpenAI requests.
//! Filesystem controls freeze the daemon's clock, inject duplicate receipts and hold
//! crash barriers before model I/O; captured calls remain outside the daemon process.
use super::*;
use broca_core::WalStore;
use cortexkit_role_llm_runner::read::{EntrySource, ModelEntry};
use cortexkit_role_llm_runner_conformance::{CompactionMessage, CompactionSetup};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CompactionScript {
    pub setup: CompactionSetup,
    pub call_timeout_ms: u64,
    pub wait_cap_ms: u64,
    pub prompt_cache_lifetime_ms: u64,
    pub context_window: u64,
    pub output_limit: u64,
    pub safety_margin: u64,
    pub request_tokens: u64,
}

impl CompactionScript {
    pub fn new(initial: CompactionMessage) -> Self {
        Self {
            setup: CompactionSetup::Ready(initial),
            call_timeout_ms: 25,
            wait_cap_ms: 100,
            prompt_cache_lifetime_ms: 100,
            context_window: 1000,
            output_limit: 100,
            safety_margin: 50,
            request_tokens: 100,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CompactionCallKind {
    Setup,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CompactionCall {
    pub kind: CompactionCallKind,
    pub request_id: String,
    pub issued_at_ms: u64,
    pub deadline_ms: u64,
    /// The bind identity of the route that delivered this call, as the daemon bound it at this
    /// provider's ingress. It is recorded separately from the call's body so a test can compare
    /// what the route said about the session with what Setup said.
    pub route_identity: Option<broca_protocol::BindIdentity>,
    /// The `harness` and `session` the Setup body reported.
    pub setup_harness: String,
    pub setup_session: String,
}
pub(super) const MODULE: &str = "conformance-compaction";
pub(super) const MODEL_PREFIX: &str = "conformance-model-";
pub(super) fn known_session(shared: &Shared, session: &str) -> bool {
    shared.compaction.lock().unwrap().contains_key(session)
}

pub(super) struct Session {
    script: CompactionScript,
    start_ms: u64,
    pub emitted: usize,
    calls: Vec<CompactionCall>,
    raw: BTreeMap<u64, Value>,
    replacements: Vec<(EntrySource, Value)>,
    inputs: Vec<Vec<ModelEntry>>,
    pub gates: std::collections::VecDeque<Arc<HoldGate>>,
    pub view: Option<broca_core::compaction_provider::view::CompactionView>,
}

pub(super) struct Provider {
    pub shared: Arc<Shared>,
    /// Each route's bind identity, keyed by its channel and epoch, recorded when the daemon
    /// binds the route to this provider.
    pub binds: Mutex<HashMap<(u16, u32), broca_protocol::BindIdentity>>,
}

impl Session {
    /// The script's timing, what is held, and every provider call's identifiers and times on
    /// the injected clock. Never content.
    pub(super) fn describe(&self) -> String {
        let mut output = format!(
            "start_ms={} call_timeout_ms={} held model calls={} captured model inputs={}\n",
            self.start_ms,
            self.script.call_timeout_ms,
            self.gates.len(),
            self.inputs.len(),
        );
        for (index, call) in self.calls.iter().enumerate() {
            output.push_str(&format!(
                "call[{index}] kind={:?} request_id={} issued_at_ms={} deadline_ms={}\n",
                call.kind, call.request_id, call.issued_at_ms, call.deadline_ms
            ));
        }
        output
    }
}
#[async_trait]
impl subc_client_rs::ModuleHandler for Provider {
    async fn on_bind(
        &self,
        request: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        let identity = &request.identity;
        self.binds.lock().unwrap().insert(
            (request.handle.channel, request.handle.epoch),
            broca_protocol::BindIdentity::new(
                identity.project_root.clone(),
                identity.harness.as_str(),
                identity.session.as_str(),
            ),
        );
        subc_client_rs::BindDecision::accept()
    }
    async fn handle(
        &self,
        ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).expect("SDK request JSON");
        let method = request["method"].as_str().unwrap_or_default();
        if method == "role.describe" {
            return response(json!({"majors":[{"version":"compaction-provider/v1",
                "ops":["role.describe","compaction.setup","view.get","compaction.ride"],"stability":"alpha"}],
                "implementation_version":"conformance", "capabilities":[]}));
        }
        if method == "view.get" {
            let view = self
                .shared
                .compaction
                .lock()
                .unwrap()
                .values()
                .filter_map(|s| s.view.as_ref())
                .find(|v| {
                    request["params"]["session_ref"] == v.session_ref
                        && request["params"]["view_version"] == v.view_version
                })
                .cloned();
            return response(match view {
                Some(view) => json!({"answer":"view", "view":view}),
                None => json!({"answer":"refuse", "code":"unknown_view", "reason":"view not held"}),
            });
        }
        if method == "compaction.ride" {
            return response(json!({"answer":"nothing"}));
        }
        if method != "compaction.setup" {
            return subc_client_rs::HandlerOutcome::Error {
                code: "unknown_method".into(),
                message: format!("the scripted provider does not serve {method}"),
            };
        }
        let params = &request["params"];
        let session = params["session"].as_str().expect("provider session");
        let request_id = params["request_id"]
            .as_str()
            .expect("provider request id")
            .to_owned();
        let now = params["now"].as_u64().expect("actual runner clock");
        let route = ctx.route_handle();
        let route_identity = self
            .binds
            .lock()
            .unwrap()
            .get(&(route.channel, route.epoch))
            .cloned();
        let answer = {
            let mut sessions = self.shared.compaction.lock().unwrap();
            let state = sessions
                .get_mut(session)
                .expect("installed provider script");
            state.calls.push(CompactionCall {
                kind: CompactionCallKind::Setup,
                request_id: request_id.clone(),
                issued_at_ms: now - state.start_ms,
                deadline_ms: now - state.start_ms + state.script.call_timeout_ms,
                route_identity,
                setup_harness: params["harness"].as_str().unwrap_or_default().to_owned(),
                setup_session: session.to_owned(),
            });
            match state.script.setup.clone() {
                CompactionSetup::Unavailable => {
                    return subc_client_rs::HandlerOutcome::Error {
                        code: "scripted_unavailable".into(),
                        message: "actual provider call unavailable".into(),
                    }
                }
                CompactionSetup::Refuse {
                    code,
                    reason,
                    provider_code,
                } => json!({"answer":"refuse", "request_id":request_id,
                    "code":code.as_str(),"reason":reason,"provider_code":provider_code}),
                CompactionSetup::Ready(message) => {
                    let initial = native_compaction(&message);
                    remember_replacement(state, &initial);
                    json!({"answer":"ready","request_id":request_id,"initial":initial})
                }
            }
        };
        response(answer)
    }
}
fn response(value: Value) -> subc_client_rs::HandlerOutcome {
    subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&value).unwrap())
}
pub(super) fn manifest() -> subc_protocol::manifest::ModuleManifest {
    use subc_protocol::manifest::{Concurrency, ProviderRole};
    subc_protocol::manifest::ModuleManifest::builder(MODULE, env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![],
            identity_scope: vec![],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        .build()
}
fn native_compaction(message: &CompactionMessage) -> Value {
    json!({"compaction_id":message.compaction_id,"version":message.version,
        "range":{"from":message.from,"to":message.to},"replacement": message.replacement.iter()
        .map(|text|json!({"role":"user","content":[{"type":"text","text":text}]})).collect::<Vec<_>>()})
}
fn remember_replacement(state: &mut Session, value: &Value) {
    let source = EntrySource::Replacement {
        compaction_id: value["compaction_id"].as_str().unwrap().into(),
        version: value["version"].as_u64().unwrap(),
        from_ordinal: value["range"]["from"].as_u64().unwrap(),
        to_ordinal: value["range"]["to"].as_u64().unwrap(),
    };
    for message in value["replacement"].as_array().unwrap() {
        state.replacements.push((source.clone(), message.clone()));
    }
}
// OpenAI removes native position/origin fields and splits grouped tool results. Match received
// bytes against provider replacements and recorded raw messages before attaching the native
// metadata; unexpected wire content is never replaced with an expected message.
fn render_native(message: &Value) -> Result<Vec<Value>, String> {
    let role = message["role"].as_str().ok_or("native role absent")?;
    let content = message["content"]
        .as_array()
        .ok_or("native content absent")?;
    let text: String = content.iter().filter_map(|b| b["text"].as_str()).collect();
    match role {
        "user" | "system" => Ok(vec![json!({"role":role,"content":text})]),
        "assistant" => {
            let calls: Vec<_> = content
                .iter()
                .filter(|b| b["type"] == "tool_call")
                .map(|b| {
                    json!({"id":b["tool_call_id"],
                "type":"function","function":{"name":b["tool_name"],"arguments":b["input"].to_string()}})
                })
                .collect();
            let mut value = json!({"role":"assistant","content":null});
            if !text.is_empty() {
                value["content"] = json!(text);
            }
            if !calls.is_empty() {
                value["tool_calls"] = json!(calls);
            }
            Ok(vec![value])
        }
        "tool" => content
            .iter()
            .map(|b| {
                if b["type"] != "tool_result" {
                    return Err("unrecognized native tool block".into());
                }
                let output: broca_protocol::ToolOutput =
                    serde_json::from_value(b["output"].clone()).map_err(|e| e.to_string())?;
                let text = match output {
                    broca_protocol::ToolOutput::Text { text } => text,
                    broca_protocol::ToolOutput::Json { value } => value.to_string(),
                    broca_protocol::ToolOutput::Denied { reason } => format!("denied: {reason}"),
                    broca_protocol::ToolOutput::Error { message } => message,
                };
                Ok(json!({"role":"tool","tool_call_id":b["tool_call_id"],"content":text}))
            })
            .collect(),
        _ => Err(format!("unhandled native role {role}")),
    }
}
fn translate(state: &Session, wire: &[Value]) -> Result<Vec<ModelEntry>, String> {
    let candidates = state
        .replacements
        .iter()
        .cloned()
        .chain(state.raw.iter().map(|(ordinal, message)| {
            (
                EntrySource::Message {
                    ordinal: *ordinal,
                    mid: format!("m{ordinal}"),
                },
                message.clone(),
            )
        }))
        .collect::<Vec<_>>();
    let mut entries = Vec::new();
    let mut at = 0;
    while at < wire.len() {
        // Compaction plans carry session identity in the model id, never in system text.
        // Reject an unexpected system message instead of hiding it from capture comparison.
        if wire[at]["role"] == "system" {
            return Err("unexpected system message in compaction model input".into());
        }
        let found = candidates
            .iter()
            .find_map(|(source, message)| {
                let rendered = render_native(message).ok()?;
                (!rendered.is_empty()
                    && wire.get(at..at + rendered.len()) == Some(rendered.as_slice()))
                .then_some((source.clone(), message.clone(), rendered.len()))
            })
            .ok_or_else(|| {
                format!(
                    "received model input has no observed native source: {}",
                    wire[at]
                )
            })?;
        entries.push(
            serde_json::from_value(json!({"source":found.0,"message":found.1}))
                .expect("canonical model entry"),
        );
        at += found.2;
    }
    Ok(entries)
}
pub(super) fn capture(shared: &Shared, session: &str, messages: &[Value]) -> Option<Arc<HoldGate>> {
    let root = shared.session_roots.lock().unwrap().get(session)?.clone();
    let identity = shared
        .session_bindings
        .lock()
        .unwrap()
        .get(session)?
        .clone();
    let records = broca_core::FileWalStore::new(root.join("wal"))
        .read_records(&identity)
        .unwrap_or_else(|error| {
            panic!("cannot read native sources for received model input: {error}")
        });
    let records: Vec<_> = records.into_iter().map(|frame| frame.record).collect();
    let prior = broca_core::replay::replay(&records).expect("recorded model input sources replay");
    let mut states = shared.compaction.lock().unwrap();
    let state = states.get_mut(session)?;
    // The wire is the observation. WAL messages identify origins and ordinals omitted by
    // OpenAI, but each received message must still render-match the recorded content.
    state.raw = prior
        .prompt
        .iter()
        .enumerate()
        .map(|(ordinal, message)| {
            (
                ordinal as u64,
                serde_json::to_value(message).expect("native message"),
            )
        })
        .collect();
    let translated =
        translate(state, messages).unwrap_or_else(|e| panic!("wire capture translation: {e}"));
    state.inputs.push(translated);
    let gate = Arc::new(HoldGate::default());
    HoldGate::set(&gate.arrived, &gate.arrived_wake);
    state.gates.push_back(gate.clone());
    Some(gate)
}

fn atomic_write(path: &Path, value: &[u8]) -> Result<(), HarnessError> {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, value).map_err(|e| HarnessError::new(e.to_string()))?;
    std::fs::rename(temporary, path).map_err(|e| HarnessError::new(e.to_string()))
}

fn settled_episode(frames: &[broca_wal::FramedRecord]) -> Option<String> {
    use broca_wal::WalRecord as R;
    let start = frames
        .iter()
        .rposition(|frame| matches!(frame.record, R::RunStarted { .. }))?;
    let terminal = frames[start..]
        .iter()
        .rev()
        .find(|frame| matches!(frame.record, R::RunFinished { .. }))?;
    let R::RunStarted { run_id, .. } = &frames[start].record else {
        unreachable!()
    };
    let R::RunFinished { reason, .. } = &terminal.record else {
        unreachable!()
    };
    let following = frames.iter().filter(|frame| frame.seq > terminal.seq);
    Some(format!(
        "run {} finished {reason:?} at WAL seq {}; {}",
        run_id.0,
        terminal.seq,
        broca_test_support::record_kinds(following.map(|frame| &frame.record))
    ))
}

fn settlement_records() -> Vec<broca_wal::FramedRecord> {
    use broca_protocol::{BindIdentity, ModelHandle, RunFinishReason, RunId};
    use broca_wal::{FramedRecord, WalRecord as R};
    let started = |id: &str| R::RunStarted {
        run_id: RunId(id.into()),
        session: BindIdentity::new("/test", "runner", "settle"),
        config: Box::new(broca_wal::RunConfig {
            model: ModelHandle::new("mock", "m"),
            tools: Arc::new(vec![]),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: Value::Null,
            cache_tiers: Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![],
            context_management: None,
            render_tags: false,
            turn_directives_v1: None,
        }),
        input: Arc::new(vec![]),
        origin: Default::default(),
        tags: None,
        on_restart: None,
        keep_warm: None,
        project_id: None,
        principal: None,
        retention: None,
        session_class: None,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        title: None,
        scope: None,
        marker_order_version: None,
        manifest: None,
        hooks: None,
        ts_ms: None,
    };
    let frame = |seq, record| FramedRecord {
        seq,
        fence: 1,
        record,
    };
    vec![
        frame(1, started("sealed")),
        frame(
            2,
            R::RunFinished {
                reason: RunFinishReason::Interrupted,
                ts_ms: None,
                usage: Default::default(),
                error: None,
                retries_used: 0,
                interrupt_reason: None,
            },
        ),
        frame(
            3,
            R::CompactionViewApplied {
                provider: MODULE.into(),
                view_version: 2,
                view: broca_wal::OpaqueJson::from_json_text("{}").unwrap(),
                choice: broca_wal::CompactionViewChoice::Applied,
            },
        ),
        frame(
            4,
            R::CompactionNotice {
                seq: 2,
                headers: BTreeMap::new(),
            },
        ),
    ]
}

#[test]
fn settled_episode_accepts_idle_view_metadata_but_not_a_newer_unfinished_run() {
    use broca_wal::{FramedRecord, WalRecord as R};
    let mut records = settlement_records();
    let frame = |seq, record| FramedRecord {
        seq,
        fence: 1,
        record,
    };
    let reason = settled_episode(&records).expect("idle metadata must not hide the terminal");
    assert!(
        reason.contains("sealed finished Interrupted at WAL seq 2"),
        "{reason}"
    );
    assert!(
        reason.contains("CompactionViewApplied") && reason.contains("CompactionNotice"),
        "{reason}"
    );
    let mut active = records[0].record.clone();
    if let R::RunStarted { run_id, .. } = &mut active {
        *run_id = broca_protocol::RunId("active".into());
    }
    records.push(frame(5, active));
    assert_eq!(
        settled_episode(&records),
        None,
        "an earlier run's terminal settled new work"
    );
    records.push(frame(
        6,
        R::CompactionNotice {
            seq: 3,
            headers: BTreeMap::new(),
        },
    ));
    assert_eq!(
        settled_episode(&records),
        None,
        "metadata alone settled the active run"
    );
}

#[tokio::test]
async fn compaction_settle_poller_observes_terminal_before_idle_view_and_notice() {
    let root = broca_test_support::scratch_root().join("compaction-settle-metadata");
    std::fs::create_dir_all(&root).unwrap();
    let subject = BrocaSubject::new(&root).await;
    let session = "settle";
    let state = root.join("state");
    subject
        .shared
        .session_roots
        .lock()
        .unwrap()
        .insert(session.into(), state.clone());
    subject.install_setup(
        session,
        CompactionSetup::Ready(CompactionMessage {
            compaction_id: "head".into(),
            version: 1,
            from: 0,
            to: 0,
            replacement: vec![],
        }),
    );
    let identity = subject.bind_identity(session);
    let wal = broca_core::FileWalStore::new(state.join("wal"));
    let mut writer = wal.open(&identity).unwrap();
    for mut frame in settlement_records() {
        if let broca_wal::WalRecord::RunStarted { session, .. } = &mut frame.record {
            *session = identity.clone();
        }
        writer.append(frame.fence, &frame.record, false).unwrap();
    }
    writer.sync().unwrap();
    tokio::time::timeout(Duration::from_secs(5), subject.settle_compaction(session))
        .await
        .expect("poller ignored the current run's terminal after idle compaction metadata")
        .unwrap();
    let evidence = subject.shared.evidence.lock().unwrap();
    let observed = evidence.last().unwrap();
    assert!(
        observed.contains("sealed finished Interrupted at WAL seq 2"),
        "{observed}"
    );
    assert!(
        observed.contains("CompactionViewApplied") && observed.contains("CompactionNotice"),
        "{observed}"
    );
}
impl BrocaSubject {
    #[cfg(feature = "test-clock")]
    pub(super) fn offer_bus_view(
        &self,
        session: &str,
        view: broca_core::compaction_provider::view::CompactionView,
        replacement: Value,
    ) {
        let mut sessions = self.shared.compaction.lock().unwrap();
        let state = sessions.get_mut(session).unwrap();
        state.replacements.push((
            EntrySource::Replacement {
                compaction_id: "view".into(),
                version: view.view_version,
                from_ordinal: 0,
                to_ordinal: 1,
            },
            replacement,
        ));
        state.view = Some(view);
    }
    pub(super) fn install_setup(&self, session: &str, setup: CompactionSetup) {
        let mut sessions = self.shared.compaction.lock().unwrap();
        let state = sessions.entry(session.into()).or_insert_with(|| Session {
            script: CompactionScript::new(CompactionMessage {
                compaction_id: "initial".into(),
                version: 1,
                from: 0,
                to: 0,
                replacement: vec![],
            }),
            start_ms: 0,
            emitted: 0,
            calls: vec![],
            raw: BTreeMap::new(),
            replacements: vec![],
            inputs: vec![],
            gates: Default::default(),
            view: None,
        });
        state.script.setup = setup;
    }

    pub(super) async fn observe_compaction(
        &self,
        session: &str,
    ) -> Result<cortexkit_role_llm_runner_conformance::CompactionObservation, HarnessError> {
        self.settle_compaction(session).await?;
        let sessions = self.shared.compaction.lock().unwrap();
        let state = sessions
            .get(session)
            .ok_or_else(|| HarnessError::new("compaction session absent"))?;
        Ok(
            cortexkit_role_llm_runner_conformance::CompactionObservation {
                calls: state
                    .calls
                    .iter()
                    .map(
                        |call| cortexkit_role_llm_runner_conformance::CompactionCall {
                            request_id: call.request_id.clone(),
                        },
                    )
                    .collect(),
                model_inputs: state.inputs.clone(),
            },
        )
    }

    fn compaction_root(&self, session: &str) -> Result<PathBuf, HarnessError> {
        self.shared
            .session_roots
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .ok_or_else(|| HarnessError::new(format!("session {session} has no rig root")))
    }
    pub(super) fn compaction_fields(
        &self,
        session: &str,
        first: bool,
    ) -> Option<Map<String, Value>> {
        if !self.shared.compaction.lock().unwrap().contains_key(session) {
            return None;
        }
        let composition = json!({"providers":[
            {"provider":MODULE,"tools":[]},
            {"provider":TOOL_MODULE,"tools":[{"name":SCRIPTED_TOOL,"capabilities":[]}]}],
            "compaction":{"provider":MODULE},"step_transform_items":[]});
        // A distinct model id carries test identity without adding a message to the
        // transcript. The HTTP capture rejects unknown ids rather than guessing a session.
        let mut fields =
            json!({"model":{"provider":"mock","model":format!("{MODEL_PREFIX}{session}")}});
        if first {
            fields["plan"] = json!({"tool_items":[{"provider":TOOL_MODULE,"preset":"scripted","params":{}}],
                "system_text_items":[],
                "step_transform_items":[],"compaction_item":{"provider":MODULE,"preset":"scripted","params":{}},
                "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
                "composition":composition});
        }
        Some(fields.as_object().unwrap().clone())
    }
    pub(super) async fn settle_compaction(&self, session: &str) -> Result<(), HarnessError> {
        let root = self.compaction_root(session)?;
        // Each return names why the work counts as settled, so a failure dump can tell an early
        // return from a genuine one.
        let settled = broca_test_support::event_timeout(
            "compaction work settles",
            async {
                loop {
                    let ready = {
                        let states = self.shared.compaction.lock().unwrap();
                        let Some(state) = states.get(session) else {
                            return "no compaction script installed".to_owned();
                        };
                        (!state.gates.is_empty())
                            .then(|| format!("{} held model call(s)", state.gates.len()))
                    };
                    if let Some(reason) = ready {
                        return reason;
                    }
                    let frames = broca_core::FileWalStore::new(root.join("wal"))
                        .read_records(&self.bind_identity(session))
                        .unwrap_or_default();
                    // Idle compaction may append a view and notice after a terminal.
                    // Only a newer RunStarted makes that terminal irrelevant to settling.
                    if let Some(reason) = settled_episode(&frames) {
                        return reason;
                    }
                    if root.join("test-compaction-reached").exists() {
                        return "test-compaction-reached marker exists".to_owned();
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            },
            || {
                broca_test_support::record_kinds(
                    broca_core::FileWalStore::new(root.join("wal"))
                        .read_records(&self.bind_identity(session))
                        .unwrap_or_default()
                        .iter()
                        .map(|f| &f.record),
                )
            },
        )
        .await;
        match settled {
            Ok(reason) => {
                eprintln!("compaction settled session={session}: {reason}");
                self.shared.note(session, format_args!("settled: {reason}"));
                Ok(())
            }
            Err(_) => {
                self.shared
                    .note(session, "compaction settle hang guard expired");
                Err(HarnessError::new(format!(
                    "compaction work did not settle for {session}"
                )))
            }
        }
    }
    pub(super) async fn release_model_compaction(&self, session: &str) -> Result<(), HarnessError> {
        self.settle_compaction(session).await?;
        let gate = self
            .shared
            .compaction
            .lock()
            .unwrap()
            .get_mut(session)
            .unwrap()
            .gates
            .pop_front()
            .ok_or_else(|| HarnessError::new("no held model call"))?;
        HoldGate::set(&gate.released, &gate.released_wake);
        sleep(Duration::from_millis(20)).await;
        self.settle_compaction(session).await
    }
    pub(super) fn durable_setup(
        &self,
        handle: &Live,
        session: &str,
    ) -> Result<Option<CompactionMessage>, HarnessError> {
        let setups = self
            .records(handle, session)
            .into_iter()
            .filter_map(|record| {
                if let broca_wal::WalRecord::CompactionMessage {
                    kind: broca_wal::CompactionMessageKind::Setup,
                    compaction_id,
                    version,
                    range,
                    replacement,
                    ..
                } = record
                {
                    Some(CompactionMessage {
                        compaction_id,
                        version,
                        from: range.from,
                        to: range.to,
                        replacement: replacement
                            .iter()
                            .map(|m| {
                                m.content
                                    .iter()
                                    .filter_map(|b| {
                                        if let broca_protocol::ContentBlock::Text { text } = b {
                                            Some(text.as_str())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect()
                            })
                            .collect(),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if setups.len() > 1 {
            return Err(HarnessError::new("more than one durable Setup"));
        }
        Ok(setups.into_iter().next())
    }
}

pub(super) fn catalog(request: &Value) -> Value {
    let args = &request["arguments"];
    let composition = &args["composition"];
    let digest = cortexkit_role_tool_provider::catalog::composition_digest(composition).unwrap();
    let schema = json!({"type":"object"});
    json!({"generation":"1","catalog_digest":"conformance","composition_digest":digest,
        "tools":[{"name":SCRIPTED_TOOL,"schema_digest":cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),
            "semantics":1,"capabilities":[],"description":"Answers each call with its scripted result.","input_schema":schema}]})
}

#[test]
fn captured_wire_translation_preserves_tool_grouping_and_native_metadata_and_rejects_content_drift()
{
    let mut state = Session {
        script: CompactionScript::new(CompactionMessage {
            compaction_id: "c".into(),
            version: 1,
            from: 0,
            to: 0,
            replacement: vec![],
        }),
        start_ms: 0,
        emitted: 0,
        calls: vec![],
        raw: BTreeMap::new(),
        replacements: vec![],
        inputs: vec![],
        gates: Default::default(),
        view: None,
    };
    let messages = vec![
        json!({"role":"user","content":[{"type":"text","text":"ask"}]}),
        json!({"role":"assistant","origin":{"provider_module_id":"mock","model_id":"mock-model"},
            "content":[{"type":"tool_call","tool_call_id":"a","tool_name":"f","input":{"x":1}}]}),
        json!({"role":"tool","content":[{"type":"tool_result","tool_call_id":"a","output":{"kind":"text","text":"yes"}}]}),
    ];
    for (i, message) in messages.iter().enumerate() {
        state.raw.insert(i as u64, message.clone());
    }
    let mut wire = vec![
        json!({"role":"user","content":"ask"}),
        json!({"role":"assistant","content":null,"tool_calls":[{"id":"a","type":"function","function":{"name":"f","arguments":"{\"x\":1}"}}]}),
        json!({"role":"tool","tool_call_id":"a","content":"yes"}),
    ];
    let translated = translate(&state, &wire).unwrap();
    assert_eq!(
        translated
            .iter()
            .map(|entry| entry.message.clone())
            .collect::<Vec<_>>(),
        messages
    );
    assert_eq!(
        translated[1].source,
        EntrySource::Message {
            ordinal: 1,
            mid: "m1".into()
        }
    );
    wire[2]["content"] = json!("wrong");
    assert!(translate(&state, &wire).is_err());
    wire[2]["content"] = json!("yes");
    wire.insert(0, json!({"role":"system","content":"unexpected"}));
    assert!(translate(&state, &wire).is_err());
}

impl BrocaSubject {
    pub(super) async fn kill_compaction(
        &self,
        mut live: Live,
        point: &KillPoint,
        trigger: Trigger<'_>,
    ) -> Result<KillReport, HarnessError> {
        let root = live.state_root.clone();
        let gate = root.join("test-compaction-gate");
        let reached = root.join("test-compaction-reached");
        let _ = std::fs::remove_file(&reached);
        atomic_write(&gate, point.as_str().as_bytes())?;
        tokio::pin!(trigger);
        let mut done = false;
        let arrival = broca_test_support::event_timeout(
            "pre-model compaction kill point",
            async {
                loop {
                    if let Ok(bytes) = std::fs::read(&reached) {
                        break serde_json::from_slice::<Value>(&bytes).unwrap();
                    }
                    tokio::select! {
                        _ = &mut trigger, if !done => done=true,
                        _ = sleep(Duration::from_millis(5)) => {},
                    }
                }
            },
            || {
                format!(
                    "kill_point={point:?}, marker={:?}",
                    std::fs::metadata(&reached).map(|m| m.len())
                )
            },
        )
        .await
        .map_err(|_| HarnessError::new(format!("runner never reached pre-model {point}")))?;
        let session = arrival["session"].as_str().unwrap();
        assert_eq!(arrival["point"], point.as_str());
        self.holds_point(&live, session, point.as_str())
            .map_err(HarnessError::new)?;
        let captured = self
            .shared
            .compaction
            .lock()
            .unwrap()
            .get(session)
            .unwrap()
            .inputs
            .len();
        assert_eq!(
            captured,
            if point.as_str() == points::FOLD_RECORDED {
                0
            } else {
                1
            },
            "compaction crash barrier must precede corresponding model receipt"
        );
        if !live.kill_all() {
            return Err(HarnessError::new("no real ck-broca process to kill"));
        }
        // External observations stay intact. Only old waiters are cleared after SIGKILL,
        // so a restarted model call waits for a new release instead of inheriting one.
        self.shared.released.notify_waiters();
        let sessions = self
            .shared
            .session_roots
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, p)| **p == root)
            .map(|(s, _)| s.clone())
            .collect::<Vec<_>>();
        let mut states = self.shared.compaction.lock().unwrap();
        for session in sessions {
            if let Some(state) = states.get_mut(&session) {
                state.gates.clear();
            }
        }
        drop(states);
        std::fs::remove_file(gate).map_err(|e| HarnessError::new(e.to_string()))?;
        std::fs::remove_file(reached).map_err(|e| HarnessError::new(e.to_string()))?;
        drop(live);
        Ok(KillReport {
            point: point.clone(),
            mechanism: KillMechanism::ProcessSignal,
        })
    }
}

impl BrocaSubject {
    pub(super) async fn wait_compaction_send_record(
        &self,
        session: &str,
        send_id: &str,
    ) -> Result<(), HarnessError> {
        let root = self.compaction_root(session)?;
        // A previous episode's terminal is not evidence that a newly admitted send settled.
        // Wait for this send's real durable start before considering model/provider gates.
        broca_test_support::wait_until(&format!("durable start for compaction send {send_id}"), || async {
            let records = broca_core::FileWalStore::new(root.join("wal"))
                .read_records(&self.bind_identity(session)).unwrap_or_default();
            if records.iter().any(|frame| matches!(&frame.record, broca_wal::WalRecord::RunStarted { send_id: Some(id), .. } if id == send_id)) {
                Ok(())
            } else {
                Err(broca_test_support::record_kinds(records.iter().map(|f| &f.record)))
            }
        }).await;
        Ok(())
    }
}

/// One `session.read` from ordinal 0 on a fresh management route opened over `identity`.
async fn read_over(
    subject: &BrocaSubject,
    connection: &SubcConnection,
    identity: &broca_protocol::BindIdentity,
) -> Value {
    let route = subject.open(connection, identity).await.unwrap();
    let bytes = connection
        .route_request_raw(
            &route,
            serde_json::to_vec(&json!({"method":"session.read",
                "params":{"from_ordinal":0,"max_bytes":1048576}}))
            .unwrap(),
        )
        .await
        .unwrap();
    let _ = connection.route_close(&route).await;
    let reply: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(reply.get("error").is_none(), "session.read: {reply}");
    reply["result"].clone()
}

/// A compaction provider calls back about the conversation under the identity its route was
/// bound with, so that identity must be the session's real one: a read-back over it returns the
/// session's messages and lineage. A route bound with the tool-module `runner` harness while
/// Setup named the real harness gave one session two identities on the wire, and the
/// provider's read-back landed on a lineage nobody had written: no messages and no lineage id.
///
/// The read-back travels as the reserved `magic-context` principal, the module a real
/// compaction provider reads as, over the identity the stand-in provider recorded when the
/// daemon bound the Setup route to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compaction_provider_reads_the_session_back_over_the_identity_its_route_was_bound_with() {
    require_compaction_test_clock();
    if !super::session_class::rigs_present() {
        return;
    }
    tokio::time::timeout(Duration::from_secs(120), async {
        let scratch = super::session_class::scratch("compaction-read-back");
        let mut subject = BrocaSubject::new(&scratch).await;
        subject.reserved_reader = true;
        let session = "read-back";
        subject
            .install_script(
                session,
                Script {
                    turns: vec![cortexkit_role_llm_runner_conformance::ScriptedTurn::new(
                        vec![ScriptedPart::Text("read-back answer".into())],
                    )],
                },
            )
            .await
            .unwrap();
        subject.install_setup(
            session,
            CompactionSetup::Ready(CompactionMessage {
                compaction_id: "read-back-head".into(),
                version: 1,
                from: 0,
                to: 0,
                replacement: vec![],
            }),
        );
        let live = subject.start(&scratch.join("rig")).await.unwrap();
        let owner_stamp = subject.owner_stamp();
        let route = subject
            .session_route(&live, session, &owner_stamp)
            .await
            .unwrap();
        let mut fields = subject.send_fields(session, true);
        fields.insert("prompt".into(), json!("read-back question"));
        fields.insert("send_id".into(), json!("read-back-send"));
        let sent = match route
            .request("session.send", Value::Object(fields))
            .await
            .unwrap()
        {
            Reply::Response(sent) => sent,
            other => panic!("send refused: {other:?}"),
        };
        subject.release_model_compaction(session).await.unwrap();
        loop {
            let Reply::Response(status) = route
                .request("run.status", json!({"run_id": sent["run_id"]}))
                .await
                .unwrap()
            else {
                panic!("run.status refused");
            };
            if status["state"] != "active" && status["state"] != "pending" {
                assert_eq!(status["state"], "completed", "{status}");
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        let (setup_count, bound, setup_harness, setup_session) = {
            let sessions = subject.shared.compaction.lock().unwrap();
            let calls = &sessions[session].calls;
            (
                calls.len(),
                calls[0].route_identity.clone(),
                calls[0].setup_harness.clone(),
                calls[0].setup_session.clone(),
            )
        };
        assert_eq!(setup_count, 1, "exactly one Setup");
        let bound = bound.expect("Setup arrived on a route the stand-in provider bound");

        let owner = subject.connection(&live, &owner_stamp).await.unwrap();
        let written = read_over(&subject, &owner, &subject.bind_identity(session)).await;
        assert!(
            written["lineage_id"]
                .as_str()
                .is_some_and(|id| !id.is_empty())
                && written["messages"]
                    .as_array()
                    .is_some_and(|messages| messages.len() >= 2),
            "the session itself must hold the turn: {written}"
        );
        let reader = subject
            .connection(
                &live,
                &RouteStamp {
                    principal: "reserved:magic-context".into(),
                    scope: None,
                },
            )
            .await
            .unwrap();
        let read_back = read_over(&subject, &reader, &bound).await;
        assert!(
            read_back["lineage_id"]
                .as_str()
                .is_some_and(|id| !id.is_empty())
                && read_back["messages"]
                    .as_array()
                    .is_some_and(|messages| !messages.is_empty()),
            "a read-back over the compaction route's bind {bound:?} found no conversation: \
             {read_back}"
        );
        assert_eq!(
            read_back, written,
            "a read-back over the compaction route's bind must see the session's own transcript"
        );
        assert_eq!(
            bound,
            subject.bind_identity(session),
            "the compaction route must be bound with the session's own identity"
        );
        // The identity a provider uses for calls about the conversation is Setup's harness
        // and session with the route's project root. The route names that same session and
        // harness, so the session shows one identity on the wire.
        assert_eq!(
            (bound.harness.as_str(), bound.session.as_str()),
            (setup_harness.as_str(), setup_session.as_str()),
            "the Setup route's bind must name the session and harness Setup reports"
        );
        drop(route);
        drop(live);
        broca_test_support::remove_rig_dir(&scratch);
    })
    .await
    .expect("compaction read-back test timed out");
}
