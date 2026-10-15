//! Real SDK compaction provider and canonical translation of received OpenAI requests.
//! Filesystem controls freeze the daemon's clock, inject duplicate receipts and hold
//! crash barriers before model I/O; captured calls remain outside the daemon process.
use super::*;
use broca_core::compaction_provider::{answer::StepAnswer, status::StepStatus};
use broca_core::WalStore;
use cortexkit_role_llm_runner::read::{EntrySource, ModelEntry};
use cortexkit_role_llm_runner_conformance::{
    CompactionAnswer, CompactionCall, CompactionCallKind, CompactionMessage, CompactionObservation,
    CompactionScript, CompactionSetup, CompactionStep,
};
use std::collections::BTreeMap;

pub(super) const MODULE: &str = "conformance-compaction";
pub(super) const MODEL_PREFIX: &str = "conformance-model-";
pub(super) fn known_session(shared: &Shared, session: &str) -> bool {
    shared.compaction.lock().unwrap().contains_key(session)
}

pub(super) struct Session {
    script: CompactionScript,
    step: usize,
    start_ms: u64,
    pub emitted: usize,
    calls: Vec<CompactionCall>,
    statuses: Vec<Option<StepStatus>>,
    senders: HashMap<usize, tokio::sync::oneshot::Sender<StepAnswer>>,
    raw: BTreeMap<u64, Value>,
    replacements: Vec<(EntrySource, Value)>,
    inputs: Vec<Vec<ModelEntry>>,
    pub gates: std::collections::VecDeque<Arc<HoldGate>>,
}

pub(super) struct Provider {
    pub shared: Arc<Shared>,
}

impl Session {
    /// The script's timing, what is held, and every provider call's identifiers and times on
    /// the injected clock. Never content.
    pub(super) fn describe(&self) -> String {
        let mut held: Vec<_> = self.senders.keys().copied().collect();
        held.sort_unstable();
        let mut output = format!(
            "start_ms={} call_timeout_ms={} wait_cap_ms={} prompt_cache_lifetime_ms={} \
             scripted steps={} next step={} held provider calls={held:?} held model calls={} \
             captured model inputs={}\n",
            self.start_ms,
            self.script.call_timeout_ms,
            self.script.wait_cap_ms,
            self.script.prompt_cache_lifetime_ms,
            self.script.steps.len(),
            self.step,
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
                "ops":["role.describe","compaction.setup","compaction.step"],"stability":"alpha"}],
                "implementation_version":"conformance", "capabilities":[]}));
        }
        let params = &request["params"];
        let session = params["session"].as_str().expect("provider session");
        let request_id = params["request_id"]
            .as_str()
            .expect("provider request id")
            .to_owned();
        let now = params["now"].as_u64().expect("actual runner clock");
        let mut held = None;
        let answer = {
            let mut sessions = self.shared.compaction.lock().unwrap();
            let state = sessions
                .get_mut(session)
                .expect("installed provider script");
            let setup = method == "compaction.setup";
            let index = state.calls.len();
            state.calls.push(CompactionCall {
                kind: if setup {
                    CompactionCallKind::Setup
                } else {
                    CompactionCallKind::Step
                },
                request_id: request_id.clone(),
                issued_at_ms: now - state.start_ms,
                deadline_ms: now - state.start_ms + state.script.call_timeout_ms,
            });
            if setup {
                state.statuses.push(None);
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
                        json!({"answer":"ready","request_id":request_id,"initial":initial,"stability":[]})
                    }
                }
            } else {
                assert_eq!(method, "compaction.step");
                let status: StepStatus =
                    serde_json::from_value(params.clone()).expect("real role status");
                for message in &status.messages {
                    state.raw.insert(message.ordinal, message.message.clone());
                }
                state.statuses.push(Some(status));
                let step = state
                    .script
                    .steps
                    .get(state.step)
                    .cloned()
                    .unwrap_or(CompactionStep::Answer(CompactionAnswer::Noop));
                state.step += 1;
                match step {
                    CompactionStep::Answer(answer) => {
                        let answer = wire_answer(&request_id, answer);
                        remember_answer(state, &answer);
                        serde_json::to_value(answer).unwrap()
                    }
                    CompactionStep::Hold(_) => {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        state.senders.insert(index, tx);
                        held = Some(rx);
                        Value::Null
                    }
                }
            }
        };
        if let Some(rx) = held {
            let answer = tokio::select! {
                answer = rx => match answer { Ok(answer) => answer, Err(_) => return killed() },
                _ = self.shared.released.notified() => return killed(),
            };
            response(serde_json::to_value(answer).unwrap())
        } else {
            response(answer)
        }
    }
}
fn killed() -> subc_client_rs::HandlerOutcome {
    subc_client_rs::HandlerOutcome::Error {
        code: "conformance_killed".into(),
        message: "runner killed".into(),
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
fn wire_answer(id: &str, answer: CompactionAnswer) -> StepAnswer {
    let value = match answer {
        CompactionAnswer::Noop => json!({"answer":"noop","request_id":id}),
        CompactionAnswer::Message(message) => {
            json!({"answer":"compaction_message","request_id":id,"compaction":native_compaction(&message)})
        }
        CompactionAnswer::Wait { reason, bound_ms } => {
            json!({"answer":"wait","request_id":id,"reason":reason,"bound_ms":bound_ms})
        }
        CompactionAnswer::Refuse {
            code,
            reason,
            provider_code,
        } => {
            json!({"answer":"refuse","request_id":id,"code":code.as_str(),"reason":reason,"provider_code":provider_code})
        }
    };
    serde_json::from_value(value).expect("valid role answer")
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
fn remember_answer(state: &mut Session, answer: &StepAnswer) {
    if let StepAnswer::CompactionMessage { compaction, .. } = answer {
        remember_replacement(state, &serde_json::to_value(compaction).unwrap());
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
    fn compaction_root(&self, session: &str) -> Result<PathBuf, HarnessError> {
        self.shared
            .session_roots
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .ok_or_else(|| HarnessError::new(format!("session {session} has no rig root")))
    }
    pub(super) async fn install_compaction(
        &self,
        session: &str,
        script: CompactionScript,
    ) -> Result<(), HarnessError> {
        let root = self.compaction_root(session)?;
        let directory = root.join("test-compaction").join(session);
        std::fs::create_dir_all(&directory).map_err(|e| HarnessError::new(e.to_string()))?;
        let now = *self
            .shared
            .compaction_clocks
            .lock()
            .unwrap()
            .entry(root.clone())
            .or_insert(0);
        atomic_write(
            &root.join("test-clock"),
            &serde_json::to_vec(&json!({"now_ms":now})).unwrap(),
        )?;
        self.shared.note(
            session,
            format_args!("install wrote test-clock now_ms={now}"),
        );
        atomic_write(&directory.join("config"),&serde_json::to_vec(&json!({
            "call_timeout_ms":script.call_timeout_ms,"wait_cap_ms":script.wait_cap_ms,
            "prompt_cache_lifetime_ms":script.prompt_cache_lifetime_ms,"context_window":script.context_window,
            "output_limit":script.output_limit,"safety_margin":script.safety_margin,"request_tokens":script.request_tokens,
        })).unwrap())?;
        let mut states = self.shared.compaction.lock().unwrap();
        if let Some(state) = states.get_mut(session) {
            state.script = script;
            state.step = 0;
        } else {
            states.insert(
                session.into(),
                Session {
                    script,
                    step: 0,
                    start_ms: now,
                    emitted: 0,
                    calls: vec![],
                    statuses: vec![],
                    senders: HashMap::new(),
                    raw: BTreeMap::new(),
                    replacements: vec![],
                    inputs: vec![],
                    gates: Default::default(),
                },
            );
        }
        Ok(())
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
            {"provider":TOOL_MODULE,"tools":[{"name":SCRIPTED_TOOL,"capabilities":[]}]}],"step_transform_items":[]});
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
    pub(super) async fn compaction_observe(
        &self,
        session: &str,
    ) -> Result<CompactionObservation, HarnessError> {
        self.settle_compaction(session).await?;
        let states = self.shared.compaction.lock().unwrap();
        let state = states
            .get(session)
            .ok_or_else(|| HarnessError::new("no compaction script"))?;
        Ok(CompactionObservation {
            calls: state.calls.clone(),
            model_inputs: state.inputs.clone(),
        })
    }
    pub(super) async fn settle_compaction(&self, session: &str) -> Result<(), HarnessError> {
        let root = self.compaction_root(session)?;
        // Each return names why the work counts as settled, so a failure dump can tell an early
        // return from a genuine one.
        let settled = broca_test_support::event_timeout(
            "compaction work settles",
            async {
                loop {
                    let now = *self
                        .shared
                        .compaction_clocks
                        .lock()
                        .unwrap()
                        .get(&root)
                        .unwrap_or(&0);
                    let ready = {
                        let states = self.shared.compaction.lock().unwrap();
                        let Some(state) = states.get(session) else {
                            return "no compaction script installed".to_owned();
                        };
                        if !state.gates.is_empty() {
                            Some(format!("{} held model call(s)", state.gates.len()))
                        } else {
                            state.calls.last().and_then(|call| {
                                (state.senders.contains_key(&(state.calls.len() - 1))
                                    && now - state.start_ms < call.deadline_ms)
                                    .then(|| {
                                        format!(
                                            "provider call {} held before its deadline {} \
                                         (adapter clock {})",
                                            state.calls.len() - 1,
                                            call.deadline_ms,
                                            now - state.start_ms
                                        )
                                    })
                            })
                        }
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
                        if let R::CompactionAnswer {
                            disposition: broca_wal::CompactionDisposition::Wait,
                            ..
                        } = last.record
                        {
                            let states = self.shared.compaction.lock().unwrap();
                            let state = states.get(session).unwrap();
                            let call = state.calls.last().unwrap();
                            let cap_end = call.issued_at_ms
                                + state
                                    .script
                                    .wait_cap_ms
                                    .min(state.script.prompt_cache_lifetime_ms);
                            if now - state.start_ms < cap_end {
                                return format!(
                                "WAIT is the last WAL record (seq {}); adapter clock {} is before \
                                 cap end {cap_end} of call {} issued at {}",
                                last.seq,
                                now - state.start_ms,
                                state.calls.len() - 1,
                                call.issued_at_ms
                            );
                            }
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
    pub(super) async fn advance_compaction(
        &self,
        session: &str,
        milliseconds: u64,
    ) -> Result<(), HarnessError> {
        self.settle_compaction(session).await?;
        let root = self.compaction_root(session)?;
        let now = {
            let mut clocks = self.shared.compaction_clocks.lock().unwrap();
            let now = clocks.get_mut(&root).unwrap();
            *now += milliseconds;
            *now
        };
        atomic_write(
            &root.join("test-clock"),
            &serde_json::to_vec(&json!({"now_ms":now})).unwrap(),
        )?;
        self.shared.note(
            session,
            format_args!("advance +{milliseconds} wrote test-clock now_ms={now}"),
        );
        // Polling advances the daemon's shared clock, which wakes its actual deadline futures.
        // Do not return just because the adapter's timestamp has changed.
        sleep(Duration::from_millis(120)).await;
        self.settle_compaction(session).await
    }
    pub(super) async fn deliver_compaction(
        &self,
        session: &str,
        index: usize,
        id: Option<String>,
        answer: CompactionAnswer,
    ) -> Result<(), HarnessError> {
        self.settle_compaction(session).await?;
        let (original, answer, sender) = {
            let mut states = self.shared.compaction.lock().unwrap();
            let state = states.get_mut(session).unwrap();
            let original = state
                .calls
                .get(index)
                .ok_or_else(|| HarnessError::new("no observed call at index"))?
                .request_id
                .clone();
            let answer = wire_answer(id.as_deref().unwrap_or(&original), answer);
            remember_answer(state, &answer);
            (original, answer, state.senders.remove(&index))
        };
        if let Some(sender) = sender {
            sender.send(answer).map_err(|_| {
                HarnessError::new("held real SDK call was cancelled before receipt")
            })?;
            sleep(Duration::from_millis(20)).await;
            self.settle_compaction(session).await
        } else {
            // Unary SDK responses cannot deliver duplicates. This debug-only filesystem
            // ingress injects the literal second answer into the same bounded run inbox;
            // only core's normal discard consumer records it, and it acknowledges fsync.
            let directory = self
                .compaction_root(session)?
                .join("test-compaction")
                .join(session);
            let _ = std::fs::remove_file(directory.join("ack"));
            atomic_write(
                &directory.join("receipt"),
                &serde_json::to_vec(&json!({"original_request_id":original,"answer":answer}))
                    .unwrap(),
            )?;
            broca_test_support::wait_until(
                "duplicate receipt durable discard acknowledgement",
                || async {
                    let ack = directory.join("ack");
                    ack.exists()
                        .then_some(())
                        .ok_or_else(|| format!("{} is absent", ack.display()))
                },
            )
            .await;
            Ok(())
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
        step: 0,
        start_ms: 0,
        emitted: 0,
        calls: vec![],
        statuses: vec![],
        senders: HashMap::new(),
        raw: BTreeMap::new(),
        replacements: vec![],
        inputs: vec![],
        gates: Default::default(),
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
                state.senders.clear();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_model_ids_route_only_to_the_exact_installed_session() {
    require_compaction_test_clock();
    let scratch = broca_test_support::scratch_root().join("compaction-model-routing");
    let subject = BrocaSubject::new(&scratch).await;
    let live = subject.start(&scratch.join("root")).await.unwrap();
    for session in ["model-routing-one", "model-routing-two"] {
        let route = subject
            .session_route(&live, session, &subject.owner_stamp())
            .await
            .unwrap();
        subject
            .install_script(
                session,
                Script {
                    turns: vec![cortexkit_role_llm_runner_conformance::ScriptedTurn {
                        parts: vec![ScriptedPart::Text("done".into())],
                    }],
                },
            )
            .await
            .unwrap();
        subject
            .install_compaction(
                session,
                CompactionScript::new(CompactionMessage {
                    compaction_id: session.into(),
                    version: 1,
                    from: 0,
                    to: 0,
                    replacement: vec![format!("head-{session}")],
                }),
            )
            .await
            .unwrap();
        let mut params = Value::Object(subject.send_fields(session, true));
        params["prompt"] = json!("hello");
        params["send_id"] = json!(session);
        let reply = route.request("session.send", params).await.unwrap();
        assert!(matches!(reply, Reply::Response(_)), "{reply:?}");
        let obs = subject.compaction_observe(session).await.unwrap();
        assert_eq!(
            obs.model_inputs.len(),
            1,
            "records={:?}",
            subject.records(&live, session)
        );
        assert_eq!(obs.calls.len(), 2);
        assert!(obs.model_inputs[0][0]
            .message
            .to_string()
            .contains(&format!("head-{session}")));
    }
    let before = subject
        .shared
        .compaction
        .lock()
        .unwrap()
        .iter()
        .map(|(session, state)| (session.clone(), state.inputs.len()))
        .collect::<BTreeMap<_, _>>();
    for request in [
        json!({"model":format!("{MODEL_PREFIX}uninstalled"),"messages":[]}),
        json!({"model":"unknown-model","messages":[{"role":"system","content":format!("{SESSION_MARKER}model-routing-one")}]}),
    ] {
        let mut stream = tokio::net::TcpStream::connect(
            subject
                .model_url
                .trim_start_matches("http://")
                .trim_end_matches("/v1"),
        )
        .await
        .unwrap();
        let body = request.to_string();
        stream.write_all(format!("POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).await.unwrap();
        assert!(reply.starts_with("HTTP/1.1 400"), "{reply}");
    }
    assert_eq!(
        subject
            .shared
            .compaction
            .lock()
            .unwrap()
            .iter()
            .map(|(session, state)| (session.clone(), state.inputs.len()))
            .collect::<BTreeMap<_, _>>(),
        before
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recorded_setup_survives_sender_driven_recovery_without_repeating_setup() {
    require_compaction_test_clock();
    let scratch = broca_test_support::scratch_root().join("compaction-sender-recovery");
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("root");
    let live = subject.start(&root).await.unwrap();
    let session = "sender-setup-recovery";
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    subject
        .install_script(
            session,
            Script {
                turns: vec![cortexkit_role_llm_runner_conformance::ScriptedTurn {
                    parts: vec![ScriptedPart::Text("recovered".into())],
                }],
            },
        )
        .await
        .unwrap();
    let head = CompactionMessage {
        compaction_id: "durable-setup".into(),
        version: 1,
        from: 0,
        to: 0,
        replacement: vec!["setup-head".into(), "setup-note".into()],
    };
    subject
        .install_compaction(session, CompactionScript::new(head.clone()))
        .await
        .unwrap();
    let mut first = Value::Object(subject.send_fields(session, true));
    first["prompt"] = json!("before-crash");
    first["send_id"] = json!("setup-cut");
    subject
        .kill_at(
            live,
            &KillPoint::new(points::FOLD_RECORDED),
            Box::pin(async {
                let _ = route.request("session.send", first).await;
            }),
        )
        .await
        .unwrap();
    let live = subject.restart(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let Reply::Response(old) = route.request("session.head", json!({})).await.unwrap() else {
        panic!("head refused")
    };
    assert_eq!(old["last_run_state"]["state"], "interrupted");
    assert_eq!(subject.durable_setup(&live, session).unwrap(), Some(head));
    assert!(
        subject
            .compaction_observe(session)
            .await
            .unwrap()
            .model_inputs
            .is_empty(),
        "process restart alone must never call the model"
    );
    let mut resume = Value::Object(subject.send_fields(session, false));
    resume["prompt"] = json!("sender-resume");
    resume["send_id"] = json!("fresh-resume");
    let Reply::Response(resumed) = route.request("session.send", resume).await.unwrap() else {
        panic!("resume refused")
    };
    assert_ne!(resumed["run_id"], old["last_run_state"]["run_id"]);
    let observation = subject.compaction_observe(session).await.unwrap();
    assert_eq!(
        observation
            .calls
            .iter()
            .filter(|c| c.kind == CompactionCallKind::Setup)
            .count(),
        1
    );
    assert_eq!(observation.model_inputs.len(), 1);
    let Reply::Response(page) = route
        .request("session.read", json!({"view":"model","from_ordinal":0}))
        .await
        .unwrap()
    else {
        panic!("model read refused")
    };
    assert_eq!(
        serde_json::to_value(&observation.model_inputs[0]).unwrap(),
        page["messages"]
    );
    assert_eq!(observation.model_inputs[0].len(), 4);
    assert!(observation.model_inputs[0][3]
        .message
        .to_string()
        .contains("sender-resume"));
    assert_eq!(
        observation.model_inputs[0][3].source,
        EntrySource::Message {
            ordinal: 1,
            mid: "m1".into()
        }
    );
}
