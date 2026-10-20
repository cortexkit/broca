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
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
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
// OpenAI removes native position/origin fields and splits grouped tool results. Match the
// received wire bytes against messages actually observed in statuses or delivered answers;
// those observations supply only the metadata absent from the wire, never expected content.
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
    let mut states = shared.compaction.lock().unwrap();
    let state = states.get_mut(session)?;
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
                    if let Some(last) = frames.last() {
                        use broca_wal::WalRecord as R;
                        if matches!(last.record, R::RunFinished { .. }) {
                            return format!(
                                "RunFinished is the last WAL record (seq {})",
                                last.seq
                            );
                        }
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
