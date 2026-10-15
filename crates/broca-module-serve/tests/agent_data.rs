use async_trait::async_trait;
use broca_core::{ExportIndex, FileWalStore, WalStore};
use broca_module::{
    registry::{ActorHandle, LineageKey, LineageOpener, OpenError, RegistryHandle},
    LineageRegistry,
};
use broca_module_serve::{
    agent_data::{AgentDataService, SessionDoor},
    serve::{self, VaultGauge},
};
use broca_protocol::{BindIdentity, RunFinishReason, RunId, Usage};
use broca_subc::session_door::{DoorRead, SessionDoorError, MAX_READ_BYTES};
use broca_wal::{RunConfig, RunOrigin, WalRecord};
use broca_wire::agent_data as contract;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{read_frame, write_frame};

const CUT: &str = "0123456789abcdef0123456789abcdef";
const BACK: &str = "fedcba9876543210fedcba9876543210";
fn root(tag: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    broca_test_support::scratch_root().join(format!(
        "agent-data-{}-{}-{tag}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ))
}
struct NeverOpen;
#[async_trait]
impl LineageOpener for NeverOpen {
    async fn open(
        &self,
        _: &LineageKey,
        _: u64,
        _: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        panic!("agent-data opened an actor")
    }
}
#[derive(Default)]
struct FakeDoor {
    streams: Mutex<BTreeMap<String, Vec<u8>>>,
    sealed: Mutex<BTreeMap<(String, String), Vec<u8>>>,
    refusal: Mutex<Option<String>>,
    digest: Mutex<Option<String>>,
    calls: AtomicUsize,
    read_limit: AtomicUsize,
    drift: AtomicBool,
}
impl FakeDoor {
    fn seal(&self, cut: &str) {
        for (reference, bytes) in self.streams.lock().unwrap().iter() {
            self.sealed
                .lock()
                .unwrap()
                .insert((cut.into(), reference.clone()), bytes.clone());
        }
    }
    fn plant(&self, cut: &str, reference: &str, bytes: Vec<u8>) {
        self.sealed
            .lock()
            .unwrap()
            .insert((cut.into(), reference.into()), bytes);
    }
}
#[async_trait]
impl SessionDoor for FakeDoor {
    async fn cover(&self, reference: &str) -> Result<u64, SessionDoorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .streams
            .lock()
            .unwrap()
            .get(reference)
            .map_or(0, |b| b.len() as u64))
    }
    async fn export(&self, reference: &str, bytes: &[u8]) -> Result<(), SessionDoorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut streams = self.streams.lock().unwrap();
        let local = streams.entry(reference.into()).or_default();
        assert!(bytes.starts_with(local));
        *local = bytes.to_vec();
        Ok(())
    }
    async fn read(
        &self,
        reference: &str,
        offset: u64,
        cut: &str,
    ) -> Result<DoorRead, SessionDoorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(code) = self.refusal.lock().unwrap().clone() {
            return Err(SessionDoorError {
                door_code: Some(code),
                message: "injected door refusal".into(),
            });
        }
        let seals = self.sealed.lock().unwrap();
        let bytes = seals
            .get(&(cut.into(), reference.into()))
            .ok_or_else(|| SessionDoorError {
                door_code: Some("not_found".into()),
                message: "cut is not published".into(),
            })?;
        let configured = self.read_limit.load(Ordering::SeqCst);
        let limit = if configured == 0 {
            MAX_READ_BYTES
        } else {
            configured
        };
        let end = bytes.len().min(offset as usize + limit);
        Ok(DoorRead {
            bytes: bytes[offset as usize..end].to_vec(),
            from_offset: offset,
            end_offset: end as u64,
            stream_end: bytes.len() as u64,
            digest: if offset > 0 && self.drift.load(Ordering::SeqCst) {
                "f".repeat(64)
            } else {
                self.digest
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| broca_wal::migration::digest(bytes))
            },
        })
    }
}
struct Rig {
    root: PathBuf,
    wal: Arc<FileWalStore>,
    index: Arc<broca_store::SqliteStore>,
    service: Arc<AgentDataService>,
    registry: Arc<LineageRegistry>,
    door: Arc<FakeDoor>,
}
impl Rig {
    fn seed_tool_run(&self, session: &BindIdentity, run: &str) {
        self.seed_tool_episode(session, run, false);
    }
    fn seed_approval_debt(&self, session: &BindIdentity, run: &str) {
        self.seed_tool_episode(session, run, true);
    }
    fn seed_tool_episode(&self, session: &BindIdentity, run: &str, owes_withdrawal: bool) {
        use broca_protocol::content::{
            AssistantMessage, ContentBlock, ToolCall, ToolOutput, ToolResult,
        };
        use broca_protocol::{BatchId, MessageId, StepId, ToolCallId};
        let call = ToolCallId(format!("{run}-call"));
        let batch = BatchId(format!("{run}-batch"));
        let mut records = vec![
            started(session, run),
            WalRecord::ModelStepFinished {
                hooks: None,
                step_id: StepId(0),
                assistant_message: AssistantMessage {
                    message_id: MessageId(format!("{run}-assistant")),
                    content: vec![ContentBlock::ToolCall(ToolCall {
                        tool_call_id: call.clone(),
                        tool_name: "read_file".into(),
                        input: json!({"path":"fixture.txt"}),
                        thought_signature: None,
                    })],
                }
                .into(),
                usage: Usage {
                    input_tokens: Some(5),
                    output_tokens: Some(2),
                    ..Default::default()
                },
                finish_reason: Some(broca_protocol::FinishReason::ToolCalls),
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::ToolBatchStarted {
                batch_id: batch.clone(),
                step_id: StepId(0),
                planned_calls: vec![call.clone()],
            },
            WalRecord::ToolDispatchIntent {
                batch_id: batch.clone(),
                tool_call_id: call.clone(),
                tool_name: Some("read_file".into()),
                args: Arc::new(json!({"path":"fixture.txt"})),
                module_id: Some("aft".into()),
                schema_pin: None,
                hooks: None,
                approval_digest: None,
                call_key: None,
            },
            WalRecord::ToolResult {
                batch_id: batch.clone(),
                result: Arc::new(ToolResult {
                    tool_call_id: call,
                    output: ToolOutput::Text {
                        text: "fixture tool output".into(),
                    },
                    is_error: false,
                }),
                outcome: broca_wal::DispatchOutcome::Completed,
                hooks: None,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
            },
            WalRecord::ToolBatchFinished {
                batch_id: batch.clone(),
            },
            WalRecord::TurnFinished { step_id: StepId(0) },
            WalRecord::RunFinished {
                reason: RunFinishReason::Completed,
                ts_ms: Some(2),
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ];
        if owes_withdrawal {
            records[3] = WalRecord::ToolCallDeferred {
                batch_id: batch, call_id: ToolCallId(format!("{run}-call")), executor:broca_wal::DeferredExecutor::Broca,
                module:Some("aft".into()),tool:"read_file".into(),schema_pin:None,args_jcs:r#"{"path":"fixture.txt"}"#.into(),
                args_hash:broca_wal::migration::digest(br#"{"path":"fixture.txt"}"#),
                scope:serde_json::from_value(json!({"owner":{"kind":"reserved","module_id":"agent"},"ref":"approval-scope","scope_epoch":1})).unwrap(),
                ask:broca_wal::DeferredAsk {prompt:"Allow file read?".into(),options:vec![],expires_at_ms:1000,on_expiry:broca_wal::OnExpiry::Deny,material_damage:false,late_execution:broca_wal::LateExecution::Execute},
            };
        }
        let mut wal = self.wal.open(session).unwrap();
        let mut prepared_key = None;
        for mut record in records {
            if owes_withdrawal {
                if let WalRecord::ToolResult {
                    outcome,
                    withdraw_elicitation,
                    ..
                } = &mut record
                {
                    *outcome = broca_wal::DispatchOutcome::Interrupted;
                    *withdraw_elicitation = Some(broca_wal::WithdrawElicitation {
                        call_key: prepared_key.clone().unwrap(),
                        elicitation_id: Some("question".into()),
                    });
                }
            }
            let seq = wal.append(1, &record, true).unwrap();
            if matches!(record, WalRecord::ToolCallDeferred { .. }) {
                prepared_key = Some(format!("{}:{seq}", wal.lineage_id().unwrap()));
            }
        }
    }
    fn new(tag: &str, door: Arc<FakeDoor>) -> Self {
        let root = root(tag);
        broca_wal::migration::durable_dir(&root.join("wal")).unwrap();
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let index = Arc::new(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
        let registry = Arc::new(LineageRegistry::new_with_reader_and_caps(
            Arc::new(NeverOpen),
            Some(wal.clone()),
            Some(index.clone()),
            None,
        ));
        let service = Arc::new(AgentDataService::new(
            root.clone(),
            registry.as_ref().clone(),
            wal.clone(),
            index.clone(),
            door.clone(),
        ));
        Self {
            root,
            wal,
            index,
            service,
            registry,
            door,
        }
    }
    fn session(&self, name: &str) -> BindIdentity {
        BindIdentity::new("/same/project", "broca", name)
    }
    fn bytes(&self, session: &BindIdentity) -> Vec<u8> {
        std::fs::read(self.wal.wal_path(session)).unwrap()
    }
    fn params(&self, cut: &str, sessions: &[BindIdentity]) -> Value {
        json!({"cutId":cut,"refs":sessions.iter().map(|s|contract::session_reference(&s.harness,&s.project_root.to_string_lossy(),&s.session)).collect::<Vec<_>>()})
    }
    fn seed(&self, session: &BindIdentity, run: &str, terminal: bool) {
        let mut wal = self.wal.open(session).unwrap();
        wal.append(1, &started(session, run), true).unwrap();
        if terminal {
            wal.append(
                1,
                &WalRecord::ModelStepFinished {
                    hooks: None,
                    step_id: broca_protocol::StepId(0),
                    assistant_message: broca_protocol::content::AssistantMessage {
                        message_id: broca_protocol::MessageId(format!("{run}-reply")),
                        content: vec![broca_protocol::content::ContentBlock::Text {
                            text: "tool transcript result".into(),
                        }],
                    }
                    .into(),
                    usage: Usage {
                        input_tokens: Some(5),
                        output_tokens: Some(2),
                        ..Usage::default()
                    },
                    finish_reason: Some(broca_protocol::FinishReason::Stop),
                    retries_used: 0,
                    service_tier_used: None,
                    speed_used: None,
                },
                true,
            )
            .unwrap();
            wal.append(
                1,
                &WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(2),
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
                true,
            )
            .unwrap();
        }
    }
}
fn started(session: &BindIdentity, run: &str) -> WalRecord {
    WalRecord::RunStarted {
        retention: None,
        hooks: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        marker_order_version: None,
        manifest: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId(run.into()),
        session: session.clone(),
        ts_ms: Some(1),
        config: Box::new(RunConfig {
            model: broca_protocol::ModelHandle::new("stub", "m"),
            tools: vec![].into(),
            tool_choice: broca_protocol::ToolChoice::Auto,
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
        }),
        input: Default::default(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    }
}
fn flags() -> Flags {
    Flags::new(false, Priority::Interactive, false)
}
struct Wire {
    manifest: Value,
    stream: tokio::io::DuplexStream,
    serving: tokio::task::JoinHandle<Result<serve::ServeEnd, serve::ServeIoError>>,
    corr: u64,
}
impl Drop for Wire {
    fn drop(&mut self) {
        self.serving.abort();
    }
}
impl Wire {
    async fn bind_session(&mut self, session: &BindIdentity) {
        self.send(0, 0, 99, FrameType::Request, json!({"op":"route.bind","route_channel":7,"epoch":1,
            "target":{"kind":"management_surface","module_id":"broca"},"identity":session,"principal":null})).await;
        assert_eq!(self.next().await.header.ty, FrameType::Response);
    }
    async fn management(&mut self, method: &str, params: Value) -> Result<Value, Value> {
        self.raw(serde_json::to_vec(&json!({"method":method,"params":params})).unwrap())
            .await
    }
    async fn new(rig: &Rig, principal: Value, internal: bool) -> Self {
        Self::new_with_scope(rig, principal, internal, Value::Null).await
    }
    async fn new_with_scope(rig: &Rig, principal: Value, internal: bool, scope: Value) -> Self {
        let (stream, module) = tokio::io::duplex(64 * 1024);
        let (read, write) = tokio::io::split(module);
        let registry = rig.registry.clone();
        let service = rig.service.clone();
        let serving = tokio::spawn(async move {
            serve::serve_connection_with_agent_data(
                read,
                write,
                "broca",
                registry.clone(),
                registry,
                VaultGauge::not_in_use(),
                None,
                Default::default(),
                Default::default(),
                None,
                None,
                None,
                Some(service),
            )
            .await
        });
        let mut wire = Self {
            manifest: Value::Null,
            stream,
            serving,
            corr: 1,
        };
        let hello = wire.next().await;
        assert_eq!(hello.header.ty, FrameType::Hello);
        wire.manifest = serde_json::from_slice::<Value>(&hello.body).unwrap()["manifest"].clone();
        wire.send(
            0,
            0,
            hello.header.corr,
            FrameType::HelloAck,
            json!({"negotiated_ver":2,"subc_ops":[],"subc_capabilities":[]}),
        )
        .await;
        let target = if internal {
            json!({"kind":"internal_service","module_id":"broca","service_id":"agent-data"})
        } else {
            json!({"kind":"management_surface","module_id":"broca"})
        };
        wire.send(0, 0, 2, FrameType::Request, json!({"op":"route.bind","route_channel":7,"epoch":1,"target":target,
            "identity":{"project_root":"/untrusted","harness":"reserved:entorhinal","session":"forged"},"principal":principal,"scope":scope})).await;
        assert_eq!(wire.next().await.header.ty, FrameType::Response);
        wire
    }
    async fn next(&mut self) -> Frame {
        tokio::time::timeout(Duration::from_secs(10), read_frame(&mut self.stream))
            .await
            .expect("serve reply deadline")
            .unwrap()
            .unwrap()
    }
    async fn send(&mut self, channel: u16, epoch: u32, corr: u64, ty: FrameType, body: Value) {
        write_frame(
            &mut self.stream,
            &Frame::build(
                ty,
                flags(),
                channel,
                epoch,
                corr,
                serde_json::to_vec(&body).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    }
    async fn raw(&mut self, bytes: Vec<u8>) -> Result<Value, Value> {
        self.corr += 1;
        write_frame(
            &mut self.stream,
            &Frame::build(FrameType::Request, flags(), 7, 1, self.corr, bytes).unwrap(),
        )
        .await
        .unwrap();
        let frame = self.next().await;
        assert_eq!(frame.header.corr, self.corr);
        let body: Value = serde_json::from_slice(&frame.body).unwrap();
        match frame.header.ty {
            FrameType::Response => Ok(body["result"].clone()),
            FrameType::Error => Err(body),
            other => panic!("unexpected {other:?}"),
        }
    }
    async fn call(&mut self, op: &str, params: Value) -> Result<Value, Value> {
        self.raw(
            serde_json::to_vec(&json!({"method":format!("agent_data.{op}"),"params":params}))
                .unwrap(),
        )
        .await
    }
}
fn entorhinal() -> Value {
    json!({"kind":"reserved","module_id":"entorhinal"})
}
fn assert_code(result: Result<Value, Value>, code: &str) -> Value {
    let error = result.unwrap_err();
    assert_eq!(error["code"], code, "{error}");
    serde_json::from_value::<contract::errors::ErrorBody>(error.clone())
        .unwrap()
        .typed_detail()
        .unwrap();
    error
}

#[tokio::test]
async fn stamped_principal_admission_precedes_json_and_every_store_read() {
    let rig = Rig::new("admission", Arc::new(FakeDoor::default()));
    std::fs::write(
        rig.root.join("wal/not-a-valid-address.wal"),
        b"must not be read",
    )
    .unwrap();
    let forged = BindIdentity::new("/untrusted", "reserved:entorhinal", "forged");
    std::fs::write(
        rig.wal.wal_path(&forged),
        b"scope authorization must not read this WAL",
    )
    .unwrap();
    for principal in [
        Value::Null,
        json!({"kind":"direct"}),
        json!({"kind":"unverified"}),
        json!({"kind":"reserved","module_id":"broca"}),
    ] {
        for scope in [
            Value::Null,
            json!({"owner":{"kind":"reserved","module_id":"owner"},"ref":"scope","scope_epoch":1,"kind":"head","owner_authorized":true}),
        ] {
            let mut wire = Wire::new_with_scope(&rig, principal.clone(), true, scope).await;
            assert_code(
                wire.raw(b"not even JSON".to_vec()).await,
                "caller_forbidden",
            );
            for op in contract::REQUIRED_OPS {
                assert_code(
                    wire.raw(
                        serde_json::to_vec(
                            &json!({"method":op,"params":{"principal":"reserved:entorhinal"}}),
                        )
                        .unwrap(),
                    )
                    .await,
                    "caller_forbidden",
                );
            }
        }
    }
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
    assert!(!rig.root.join("agent-data").exists());
}

#[tokio::test]
async fn describe_is_internal_and_declares_the_agent_owned_store() {
    let rig = Rig::new("describe", Arc::new(FakeDoor::default()));
    let mut internal = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        internal.call("describe", json!({})).await.unwrap(),
        json!({"ref_kinds":["broca-session"],"stores":[{"name":"broca/session-wals","sync_class":"agent"}],"external":false})
    );
    let manifest = internal.manifest.clone();
    assert!(manifest["capabilities"]["provides"]
        .as_array()
        .unwrap()
        .contains(&json!("agent-data/v1")));
    let services = manifest["provides"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|role| role["role"] == "internal_service")
        .collect::<Vec<_>>();
    assert_eq!(
        services,
        vec![
            &json!({"role":"internal_service","service_id":"agent-data","transport":"bulk","agent_facing":false,"operations":contract::REQUIRED_OPS})
        ]
    );
    let mut management = Wire::new(&rig, entorhinal(), false).await;
    assert!(
        management.call("describe", json!({})).await.is_err(),
        "agent-data is not a management operation"
    );
}

#[tokio::test]
async fn move_and_move_back_replays_and_rejoins_only_a_prefix() {
    let door = Arc::new(FakeDoor::default());
    let source = Rig::new("source", door.clone());
    let receiver = Rig::new("receiver", door.clone());
    let session = source.session("moves");
    source.seed_tool_run(&session, "original-run");
    let resolver = |_: &str, _: &str| (broca_core::ChargeBasis::MeteredApi, 1);
    source
        .index
        .emit_run(
            &source.wal.read_records(&session).unwrap(),
            &RunId("original-run".into()),
            &resolver,
        )
        .unwrap();
    assert!(
        source.index.high_water() > 0,
        "the fixture produces a real billed fact"
    );
    let original = source.bytes(&session);
    let mut source_reader = Wire::new(&source, Value::Null, false).await;
    source_reader.bind_session(&session).await;
    let transcript = source_reader
        .management("session.read", json!({"include_tools":true}))
        .await
        .unwrap();
    let params = source.params(CUT, std::slice::from_ref(&session));
    let mut from = Wire::new(&source, entorhinal(), true).await;
    let mut to = Wire::new(&receiver, entorhinal(), true).await;
    assert_eq!(
        from.call("busy", json!({"refs":params["refs"]}))
            .await
            .unwrap(),
        json!({"refs":[{"state":"idle"}]})
    );
    let prepared = from.call("prepare", params.clone()).await.unwrap();
    assert_eq!(prepared["items"][0]["sessionRef"], params["refs"][0]["ref"]);
    assert_eq!(
        prepared["items"][0]["digest"],
        broca_wal::migration::digest(&original)
    );
    assert!(source.wal.check_move(&session, true).is_err());
    assert_code(
        source_reader.management("session.send", json!({})).await,
        "session_moving",
    );
    assert_eq!(source.bytes(&session), original);
    assert_eq!(
        from.call("prepare", params.clone()).await.unwrap(),
        prepared
    );
    let exported = from.call("export", params.clone()).await.unwrap();
    assert_eq!(from.call("export", params.clone()).await.unwrap(), exported);
    door.seal(CUT);
    assert_eq!(
        to.call("diverged", params.clone()).await.unwrap(),
        json!({"refs":[{"state":"at_cut"}]})
    );
    let imported = to.call("import", params.clone()).await.unwrap();
    let calls = door.calls.load(Ordering::SeqCst);
    *door.refusal.lock().unwrap() = Some("unavailable".into());
    assert_eq!(
        to.call("import", params.clone()).await.unwrap(),
        imported,
        "lost reply replay needs no door"
    );
    assert_eq!(
        to.call("activate", params.clone()).await.unwrap(),
        json!({})
    );
    assert_eq!(
        door.calls.load(Ordering::SeqCst),
        calls,
        "activation uses verified durable staging"
    );
    *door.refusal.lock().unwrap() = None;
    assert_eq!(receiver.bytes(&session), original);
    let mut receiver_reader = Wire::new(&receiver, Value::Null, false).await;
    receiver_reader.bind_session(&session).await;
    assert_eq!(
        receiver_reader
            .management("session.read", json!({"include_tools":true}))
            .await
            .unwrap(),
        transcript
    );
    receiver
        .index
        .emit_run(
            &receiver.wal.read_records(&session).unwrap(),
            &RunId("original-run".into()),
            &resolver,
        )
        .unwrap();
    assert_eq!(
        receiver.index.high_water(),
        0,
        "imported runs are not billed on the receiver"
    );
    let mut release = params.clone();
    release["holder"] = json!("receiver-mac");
    assert_eq!(
        from.call("release", release.clone()).await.unwrap(),
        json!({"enforced":true})
    );
    assert_eq!(
        from.call("release", release).await.unwrap(),
        json!({"enforced":true})
    );
    assert!(source.wal.check_move(&session, false).is_err());
    assert_code(
        source_reader.management("session.read", json!({})).await,
        "session_moved",
    );
    assert_code(
        source_reader.management("session.send", json!({})).await,
        "session_moved",
    );
    receiver.seed(&session, "receiver-run", true);
    receiver
        .index
        .emit_run(
            &receiver.wal.read_records(&session).unwrap(),
            &RunId("receiver-run".into()),
            &resolver,
        )
        .unwrap();
    assert!(
        receiver.index.high_water() > 0,
        "a receiver-local run is billed normally"
    );
    let extended = receiver.bytes(&session);
    assert!(extended.starts_with(&original));
    let back = source.params(BACK, std::slice::from_ref(&session));
    to.call("prepare", back.clone()).await.unwrap();
    to.call("export", back.clone()).await.unwrap();
    door.seal(BACK);
    assert_eq!(
        from.call("diverged", back.clone()).await.unwrap(),
        json!({"refs":[{"state":"at_cut"}]})
    );
    from.call("import", back.clone()).await.unwrap();
    from.call("activate", back).await.unwrap();
    assert_eq!(source.bytes(&session), extended);
    assert!(!source
        .wal
        .wal_path(&session)
        .with_extension("moved")
        .exists());
    assert!(source.wal.check_move(&session, true).is_ok());
    let fork = Rig::new("fork", door);
    fork.seed(&session, "fork-run", true);
    let mut fork_wire = Wire::new(&fork, entorhinal(), true).await;
    let params = fork.params(BACK, std::slice::from_ref(&session));
    let before = fork.bytes(&session);
    assert_eq!(
        fork_wire.call("diverged", params.clone()).await.unwrap(),
        json!({"refs":[{"state":"advanced"}]})
    );
    assert_code(fork_wire.call("import", params).await, "local_advanced");
    assert_eq!(fork.bytes(&session), before);
    assert!(!fork.root.join("wal-staging").exists());
}

#[tokio::test]
async fn busy_without_cut_id_and_unknown_abort_follow_the_contract() {
    let rig = Rig::new("busy", Arc::new(FakeDoor::default()));
    let session = rig.session("active");
    rig.seed(&session, "unfinished", false);
    let params = rig.params(CUT, &[session]);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("busy", json!({"refs":params["refs"]}))
            .await
            .unwrap()["refs"][0]["reason_code"],
        "run_active"
    );
    assert_code(wire.call("prepare", params.clone()).await, "busy");
    assert!(!rig
        .root
        .join("agent-data/cuts")
        .join(format!("{CUT}.json"))
        .exists());
    let missing = json!({"ref_kind":"broca-session","ref":"ffffffffffffffffffffffffffffffff"});
    let mut mixed = params.clone();
    mixed["refs"].as_array_mut().unwrap().push(missing.clone());
    assert_eq!(
        assert_code(
            wire.call("busy", json!({"refs":mixed["refs"]})).await,
            "unknown_ref"
        )["detail"],
        missing
    );
    wire.call("abort", mixed.clone()).await.unwrap();
    assert_eq!(
        wire.call("status", mixed.clone()).await.unwrap(),
        json!({"step":"aborted"})
    );
    wire.call("abort", mixed).await.unwrap();
}

#[tokio::test]
async fn retention_marker_never_reaches_the_door() {
    use broca_protocol::content::{ContentBlock, Message, Role};
    let rig = Rig::new("retained", Arc::new(FakeDoor::default()));
    let session = rig.session("private");
    let marker = "RETENTION-MARKER-MUST-NEVER-LEAVE";
    let mut record = started(&session, "retained");
    if let WalRecord::RunStarted {
        retention, input, ..
    } = &mut record
    {
        *retention = Some(60_000);
        *input = vec![Message {
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: marker.into(),
            }],
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
        }]
        .into();
    }
    let mut wal = rig.wal.open_with_retention(&session, Some(60_000)).unwrap();
    wal.append(1, &record, true).unwrap();
    wal.append(
        1,
        &WalRecord::RunFinished {
            reason: RunFinishReason::Completed,
            ts_ms: Some(2),
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
        true,
    )
    .unwrap();
    drop(wal);
    let before = std::fs::read(rig.wal.ephemeral_wal_path(&session)).unwrap();
    assert!(String::from_utf8_lossy(&before).contains(marker));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_code(
        wire.call("prepare", rig.params(CUT, std::slice::from_ref(&session)))
            .await,
        "not_exportable",
    );
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
    assert!(rig.door.streams.lock().unwrap().is_empty());
    assert_eq!(
        std::fs::read(rig.wal.ephemeral_wal_path(&session)).unwrap(),
        before
    );
}

fn incoming_rig(tag: &str) -> (Rig, Value, Vec<u8>) {
    let door = Arc::new(FakeDoor::default());
    let source = Rig::new("incoming-fixture", door.clone());
    let session = source.session("incoming");
    source.seed(&session, "foreign", true);
    let bytes = source.bytes(&session);
    let reference = contract::session_ref(
        &session.harness,
        &session.project_root.to_string_lossy(),
        &session.session,
    );
    door.plant(CUT, &reference, bytes.clone());
    let rig = Rig::new(tag, door);
    let params = rig.params(CUT, &[session]);
    (rig, params, bytes)
}
fn assert_nothing_staged(rig: &Rig) {
    assert!(!rig.root.join("wal-staging").exists());
    assert_eq!(std::fs::read_dir(rig.wal.root()).unwrap().count(), 0);
}
#[tokio::test]
async fn import_requires_the_doors_sealed_digest() {
    let (rig, params, _) = incoming_rig("no-digest");
    *rig.door.digest.lock().unwrap() = Some(String::new());
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_code(wire.call("import", params).await, "unavailable");
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn import_verifies_the_doors_sealed_digest() {
    let (rig, params, _) = incoming_rig("bad-digest");
    *rig.door.digest.lock().unwrap() = Some("0".repeat(64));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_code(wire.call("import", params).await, "digest_mismatch");
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn import_checks_all_frame_features_before_staging() {
    let (rig, params, mut bytes) = incoming_rig("format");
    let record = WalRecord::RunFinished {
        reason: RunFinishReason::Completed,
        ts_ms: Some(3),
        usage: Usage::default(),
        error: None,
        retries_used: 0,
    };
    bytes.extend_from_slice(
        &broca_wal::framing::encode_with_requires(4, 1, &["future-move-reader/v99"], &record)
            .unwrap(),
    );
    rig.door
        .plant(CUT, params["refs"][0]["ref"].as_str().unwrap(), bytes);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let error = assert_code(wire.call("import", params).await, "incompatible_version");
    assert!(error["detail"]["found"]
        .as_str()
        .unwrap()
        .contains("future-move-reader/v99"));
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn import_address_is_proved_by_the_wals_run_started_triple() {
    let (rig, mut params, bytes) = incoming_rig("address");
    let wrong = contract::session_ref("broca", "/same/project", "wrong");
    params["refs"][0]["ref"] = json!(wrong);
    rig.door.plant(CUT, &wrong, bytes);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_code(wire.call("import", params).await, "invalid_request");
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn import_maps_door_refusals_without_staging() {
    for (door_code, participant_code) in [
        ("not_found", "cut_not_ready"),
        ("verify_failed", "digest_mismatch"),
        ("cut_item_missing", "unavailable"),
        ("unavailable", "unavailable"),
    ] {
        let (rig, params, _) = incoming_rig(door_code);
        *rig.door.refusal.lock().unwrap() = Some(door_code.into());
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let error = assert_code(wire.call("import", params).await, participant_code);
        assert!(error["message"].as_str().unwrap().contains(door_code));
        assert_nothing_staged(&rig);
    }
}
#[tokio::test]
async fn whole_ref_import_preflight_leaves_every_local_and_staged_file_untouched() {
    let door = Arc::new(FakeDoor::default());
    let source = Rig::new("multi-source", door.clone());
    let receiver = Rig::new("multi-receiver", door.clone());
    let sessions = [source.session("a"), source.session("b")];
    for session in &sessions {
        source.seed(session, "source", true);
        door.plant(
            CUT,
            &contract::session_ref(
                &session.harness,
                &session.project_root.to_string_lossy(),
                &session.session,
            ),
            source.bytes(session),
        );
    }
    receiver.seed(&sessions[1], "fork", true);
    let before = receiver.bytes(&sessions[1]);
    let mut wire = Wire::new(&receiver, entorhinal(), true).await;
    assert_code(
        wire.call("import", source.params(CUT, &sessions)).await,
        "local_advanced",
    );
    assert!(!receiver.wal.wal_path(&sessions[0]).exists());
    assert_eq!(receiver.bytes(&sessions[1]), before);
    assert!(!receiver.root.join("wal-staging").exists());
}
#[tokio::test]
async fn status_keeps_a_partial_prepare_intent_byte_identical_until_repeat() {
    use broca_module::cut_state::{CutState, CutStore, Intent};
    let rig = Rig::new("pending-prepare", Arc::new(FakeDoor::default()));
    let sessions = [rig.session("a"), rig.session("b")];
    for session in &sessions {
        rig.seed(session, "source", true);
    }
    let params = rig.params(CUT, &sessions);
    let state = CutState {
        step: contract::step::Step::None,
        refs: Some(serde_json::from_value(params["refs"].clone()).unwrap()),
        intent: Some(Intent {
            op: "prepare".into(),
            request: params.clone(),
        }),
        replies: Default::default(),
    };
    let path = CutStore::new(&rig.root).path(CUT);
    broca_wal::migration::atomic_json(&path, &state).unwrap();
    let range = broca_wal::read_records_range(
        rig.wal.wal_path(&sessions[0]),
        broca_wal::DEFAULT_MAX_READ_BYTES,
        0,
        None,
        None,
    )
    .unwrap();
    broca_wal::migration::freeze(
        &rig.wal.wal_path(&sessions[0]),
        &broca_wal::migration::Freeze {
            cut_id: CUT.into(),
            end_offset: rig.bytes(&sessions[0]).len() as u64,
            lineage_id: range.lineage_id.unwrap().to_string(),
        },
    )
    .unwrap()
    .unwrap();
    let before = std::fs::read(&path).unwrap();
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("status", params.clone()).await.unwrap(),
        json!({"step":"none"})
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let reply = wire.call("prepare", params.clone()).await.unwrap();
    assert_eq!(reply["items"].as_array().unwrap().len(), 2);
    for session in &sessions {
        assert!(rig.wal.wal_path(session).with_extension("moving").exists());
    }
    assert_eq!(
        wire.call("status", params.clone()).await.unwrap(),
        json!({"step":"prepared"})
    );
    assert_eq!(wire.call("prepare", params).await.unwrap(), reply);
}

fn vectors(name: &str) -> Value {
    serde_json::from_str(cortexkit_role_agent_data::vectors::file(name).unwrap()).unwrap()
}
#[tokio::test]
async fn contract_request_vectors_pass_through_internal_serve() {
    let rig = Rig::new("request-vectors", Arc::new(FakeDoor::default()));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let cases = vectors("requests.json");
    assert_code(wire.raw(serde_json::to_vec(&json!({"method":"agent_data.describe","params":{},"principal":"reserved:entorhinal"})).unwrap()).await,"invalid_request");
    for case in cases["requests"].as_array().unwrap() {
        let mut params = case["params"].clone();
        if let Some(count) = case["expand_refs"].as_u64() {
            params["refs"] = json!((0..count)
                .map(|n| json!({"ref_kind":"broca-session","ref":format!("ref-{n}")}))
                .collect::<Vec<_>>());
        }
        let request =
            serde_json::to_vec(&json!({"method":case["method"],"params":params})).unwrap();
        if let Err(error) = wire.raw(request).await {
            // Shared request vectors may use ref kinds this broca does not
            // declare. Those valid requests can be refused as unsupported kinds
            // after decoding, but must not fail the request decoder.
            if error["code"] == "invalid_request" {
                assert_eq!(
                    error["message"], "broca does not hold this session ref",
                    "{}",
                    case["name"]
                );
            }
        }
    }
    // Invalid requests must be decoded before this deliberately unreadable store.
    std::fs::write(
        rig.root.join("wal/not-a-valid-address.wal"),
        b"invalid request must not read this",
    )
    .unwrap();
    for case in cases["refused"].as_array().unwrap() {
        let mut params = case["params"].clone();
        if let Some(count) = case["expand_refs"].as_u64() {
            params["refs"] = json!((0..count)
                .map(|n| json!({"ref_kind":"broca-session","ref":format!("ref-{n}")}))
                .collect::<Vec<_>>());
        }
        assert_code(
            wire.raw(
                serde_json::to_vec(&json!({"method":case["method"],"params":params})).unwrap(),
            )
            .await,
            case["code"].as_str().unwrap(),
        );
    }
}
#[tokio::test]
async fn contract_step_vectors_pass_through_internal_serve() {
    use broca_module::cut_state::{CutState, CutStore};
    let cases = vectors("steps.json");
    for case in cases["transitions"].as_array().unwrap() {
        let rig = Rig::new("step-vector", Arc::new(FakeDoor::default()));
        let state = CutState {
            step: serde_json::from_value(case["from"].clone()).unwrap(),
            refs: Some(contract::refs::Refs::new(vec![]).unwrap()),
            intent: None,
            replies: Default::default(),
        };
        broca_wal::migration::atomic_json(&CutStore::new(&rig.root).path(CUT), &state).unwrap();
        let before = std::fs::read(CutStore::new(&rig.root).path(CUT)).unwrap();
        let mut params = json!({"refs":[],"cutId":CUT});
        if case["op"] == "describe" {
            params = json!({});
        }
        if case["op"] == "busy" {
            params = json!({"refs":[]});
        }
        if case["op"] == "release" {
            params["holder"] = json!("receiver");
        }
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let result = wire.call(case["op"].as_str().unwrap(), params).await;
        if let Some(code) = case["refused"].as_str() {
            assert_code(result, code);
            assert_eq!(
                std::fs::read(CutStore::new(&rig.root).path(CUT)).unwrap(),
                before
            );
        } else {
            result.unwrap_or_else(|error| panic!("{}: {error}", case["op"]));
            assert_eq!(
                wire.call("status", json!({"refs":[],"cutId":CUT}))
                    .await
                    .unwrap()["step"],
                case["to"]
            );
        }
    }
}
#[tokio::test]
async fn contract_idempotency_vectors_replay_before_resolving_sessions() {
    use broca_module::cut_state::{CutState, CutStore, Reply};
    for case in vectors("idempotency.json")["cases"].as_array().unwrap() {
        let rig = Rig::new("idempotency-vector", Arc::new(FakeDoor::default()));
        let first = &case["first"];
        let mut state = CutState {
            refs: first["params"]
                .get("refs")
                .map(|r| serde_json::from_value(r.clone()).unwrap()),
            ..Default::default()
        };
        let witness = json!({"stored_first_reply":case["name"]});
        state.replies.insert(
            first["method"].as_str().unwrap().into(),
            Reply {
                request: first["params"].clone(),
                value: witness.clone(),
            },
        );
        if let Some(cut) = first["params"]["cutId"].as_str() {
            broca_wal::migration::atomic_json(&CutStore::new(&rig.root).path(cut), &state).unwrap();
        }
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let result = wire.raw(serde_json::to_vec(&case["again"]).unwrap()).await;
        match case["outcome"].as_str().unwrap() {
            "replay" => assert_eq!(result.unwrap(), witness, "{}", case["name"]),
            "id_reused" => {
                assert_code(result, "id_reused");
            }
            "exempt" | "new_key" => match result {
                Ok(reply) => assert_ne!(reply, witness),
                Err(error) => assert_ne!(error["code"], "id_reused", "{}", case["name"]),
            },
            other => panic!("unknown vector outcome {other}"),
        }
    }
}
#[tokio::test]
async fn contract_ref_set_vectors_are_checked_before_artifacts() {
    use broca_module::cut_state::{CutState, CutStore};
    for case in vectors("ref-sets.json")["cases"].as_array().unwrap() {
        let rig = Rig::new("ref-set-vector", Arc::new(FakeDoor::default()));
        let state = CutState {
            step: serde_json::from_value(case["step"].clone()).unwrap(),
            refs: case["started_with"]
                .as_array()
                .map(|_| serde_json::from_value(case["started_with"].clone()).unwrap()),
            intent: None,
            replies: Default::default(),
        };
        broca_wal::migration::atomic_json(&CutStore::new(&rig.root).path(CUT), &state).unwrap();
        let mut params = json!({"refs":case["refs"],"cutId":CUT});
        if case["op"] == "release" {
            params["holder"] = json!("receiver");
        }
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let result = wire.call(case["op"].as_str().unwrap(), params).await;
        if case["outcome"] == "ok" {
            if let Err(error) = result {
                assert_ne!(error["code"], "invalid_state", "{}", case["name"]);
            }
        } else {
            let error = assert_code(result, case["outcome"]["code"].as_str().unwrap());
            assert_eq!(
                error["detail"], case["outcome"]["detail"],
                "{}",
                case["name"]
            );
        }
    }
}
#[tokio::test]
async fn session_ref_vectors_are_resolved_by_the_serve_participant() {
    let mut cases = vectors("session-refs.json")["cases"]
        .as_array()
        .unwrap()
        .clone();
    cases.push(json!({"harness":"broca","project_root":"/Users/ufukaltinok/.local/share/cortexkit/alfonso/worktrees/c0c39eea197fcc68/pool-952","session":"alfonso:bg_de298dcca128a1e1","sessionRef":"cc8f376395c1f915b357326b6741ddcb"}));
    for case in cases {
        let rig = Rig::new("ref-vector", Arc::new(FakeDoor::default()));
        let session = BindIdentity::new(
            case["project_root"].as_str().unwrap(),
            case["harness"].as_str().unwrap(),
            case["session"].as_str().unwrap(),
        );
        rig.seed(&session, "vector", true);
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let reply = wire
            .call(
                "prepare",
                json!({"cutId":CUT,"refs":[{"ref_kind":"broca-session","ref":case["sessionRef"]}]}),
            )
            .await
            .unwrap();
        assert_eq!(reply["items"][0]["sessionRef"], case["sessionRef"]);
    }
}

#[cfg(unix)]
#[path = "agent_data/real_daemon.rs"]
mod real_daemon;

#[tokio::test]
async fn closed_session_with_question_withdrawal_debt_is_busy_before_prepare() {
    let rig = Rig::new("approval-debt", Arc::new(FakeDoor::default()));
    let session = rig.session("approval");
    rig.seed_approval_debt(&session, "approval-run");
    let params = rig.params(CUT, std::slice::from_ref(&session));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let reply = wire
        .call("busy", json!({"refs":params["refs"]}))
        .await
        .unwrap();
    assert_eq!(reply["refs"][0]["state"], "busy");
    assert_eq!(reply["refs"][0]["reason_code"], "approval_open");
    assert_code(wire.call("prepare", params).await, "busy");
    assert!(!rig.wal.wal_path(&session).with_extension("moving").exists());
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
}

async fn source_tombstone_case(op: contract::Op) {
    use broca_module::cut_state::{CutState, CutStore};
    let rig = Rig::new("source-moved-code", Arc::new(FakeDoor::default()));
    let session = rig.session("moved-source");
    rig.seed(&session, "original", true);
    let mut params = rig.params(CUT, std::slice::from_ref(&session));
    let state = CutState {
        step: match op {
            contract::Op::Prepare => contract::step::Step::None,
            contract::Op::Export => contract::step::Step::Prepared,
            contract::Op::Release => contract::step::Step::Exported,
            _ => unreachable!(),
        },
        refs: Some(serde_json::from_value(params["refs"].clone()).unwrap()),
        intent: None,
        replies: Default::default(),
    };
    broca_wal::migration::atomic_json(&CutStore::new(&rig.root).path(CUT), &state).unwrap();
    broca_wal::migration::atomic_json(
        &rig.wal.wal_path(&session).with_extension("moved"),
        &broca_wal::migration::Moved {
            cut_id: BACK.into(),
            holder: "other-mac".into(),
            lineage_id: "lineage".into(),
        },
    )
    .unwrap();
    if op == contract::Op::Release {
        params["holder"] = json!("new-mac");
    }
    let before = rig.bytes(&session);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let error = assert_code(
        wire.call(
            op.method().strip_prefix("agent_data.").unwrap(),
            params.clone(),
        )
        .await,
        "unknown_ref",
    );
    assert_eq!(error["detail"], params["refs"][0]);
    assert!(error["message"].as_str().unwrap().contains("other-mac"));
    assert!(error["message"].as_str().unwrap().contains(BACK));
    let error: contract::errors::ErrorBody = serde_json::from_value(error).unwrap();
    assert!(!contract::errors::driver_action_for(op, &error.code).retryable());
    assert!(contract::errors::may_refuse(op, &error.code));
    assert_eq!(rig.bytes(&session), before);
}
#[tokio::test]
async fn prepare_tombstone_is_terminal_unknown_ref() {
    source_tombstone_case(contract::Op::Prepare).await;
}
#[tokio::test]
async fn export_tombstone_is_terminal_unknown_ref() {
    source_tombstone_case(contract::Op::Export).await;
}
#[tokio::test]
async fn release_tombstone_is_terminal_unknown_ref() {
    source_tombstone_case(contract::Op::Release).await;
}

async fn contradictory_owner_case(op: contract::Op) {
    use broca_module::cut_state::{CutState, CutStore};
    let rig = Rig::new("foreign-owner", Arc::new(FakeDoor::default()));
    let session = rig.session("foreign-owned");
    rig.seed(&session, "original", true);
    let mut params = rig.params(CUT, std::slice::from_ref(&session));
    let state = CutState {
        step: if op == contract::Op::Export {
            contract::step::Step::Prepared
        } else {
            contract::step::Step::Exported
        },
        refs: Some(serde_json::from_value(params["refs"].clone()).unwrap()),
        intent: None,
        replies: Default::default(),
    };
    let authority = CutStore::new(&rig.root).path(CUT);
    broca_wal::migration::atomic_json(&authority, &state).unwrap();
    let sidecar = rig.wal.wal_path(&session).with_extension("moving");
    broca_wal::migration::atomic_json(
        &sidecar,
        &broca_wal::migration::Freeze {
            cut_id: BACK.into(),
            end_offset: rig.bytes(&session).len() as u64,
            lineage_id: "foreign-lineage".into(),
        },
    )
    .unwrap();
    let old_sidecar = std::fs::read(&sidecar).unwrap();
    let old_authority = std::fs::read(&authority).unwrap();
    if op == contract::Op::Release {
        params["holder"] = json!("receiver");
    }
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let error = assert_code(
        wire.call(op.method().strip_prefix("agent_data.").unwrap(), params)
            .await,
        "invalid_state",
    );
    assert_eq!(
        error["detail"]["step"],
        serde_json::to_value(state.step).unwrap()
    );
    assert!(error["message"].as_str().unwrap().contains(CUT));
    assert!(error["message"].as_str().unwrap().contains(BACK));
    let error: contract::errors::ErrorBody = serde_json::from_value(error).unwrap();
    assert!(!contract::errors::driver_action_for(op, &error.code).retryable());
    assert!(contract::errors::may_refuse(op, &error.code));
    assert_eq!(std::fs::read(sidecar).unwrap(), old_sidecar);
    assert_eq!(std::fs::read(authority).unwrap(), old_authority);
}
#[tokio::test]
async fn export_foreign_owner_is_terminal_invalid_state() {
    contradictory_owner_case(contract::Op::Export).await;
}
#[tokio::test]
async fn release_foreign_owner_is_terminal_invalid_state() {
    contradictory_owner_case(contract::Op::Release).await;
}

#[tokio::test]
#[cfg(feature = "test-clock")]
async fn import_refuses_a_cut_larger_than_the_local_wal_limit() {
    let (mut rig, params, _) = incoming_rig("cut-limit");
    rig.wal = Arc::new(FileWalStore::new(rig.root.join("wal")).with_read_cap_for_test(10));
    rig.service = Arc::new(AgentDataService::new(
        rig.root.clone(),
        rig.registry.as_ref().clone(),
        rig.wal.clone(),
        rig.index.clone(),
        rig.door.clone(),
    ));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let error = assert_code(wire.call("import", params).await, "unavailable");
    assert!(error["message"].as_str().unwrap().contains("size limit"));
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 1);
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn import_refuses_sealed_metadata_that_changes_between_pages() {
    let (rig, params, _) = incoming_rig("seal-drift");
    rig.door.read_limit.store(100, Ordering::SeqCst);
    rig.door.drift.store(true, Ordering::SeqCst);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let error = assert_code(wire.call("import", params).await, "unavailable");
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("metadata changed"));
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 2);
    assert_nothing_staged(&rig);
}

#[tokio::test]
async fn expired_retention_ledger_ref_is_not_exportable_even_without_wal_bytes() {
    let rig = Rig::new("expired-retention", Arc::new(FakeDoor::default()));
    let session = rig.session("expired");
    rig.index
        .upsert_retention_lineage(
            &broca_store::retention::RetentionLineageRow {
                lineage_id: "expired-lineage".into(),
                session: session.clone(),
                retention_s: 60,
                last_activity_ms: 100,
                expires_at_ms: None,
                wal_bytes: 0,
                expired_at_ms: Some(60_100),
            },
            &[broca_store::retention::RetentionRunRow {
                run: broca_core::RunIndexRow {
                    run_id: "expired-run".into(),
                    withdrawals_owed: false,
                    session: session.clone(),
                    submission_id: None,
                    request_identity_digest: None,
                    state: broca_core::RunIndexState::Completed,
                    usage: None,
                    final_step_finish_reason: Some(broca_protocol::FinishReason::Stop),
                    retries_used: 0,
                    title: None,
                },
                state_changed_ms: Some(100),
            }],
        )
        .unwrap();
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_code(
        wire.call("prepare", rig.params(CUT, std::slice::from_ref(&session)))
            .await,
        "not_exportable",
    );
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
    assert_nothing_staged(&rig);
    rig.seed(&session, "fresh-lineage", true);
    wire.call("prepare", rig.params(CUT, std::slice::from_ref(&session)))
        .await
        .unwrap();
}
#[test]
fn ordinary_address_reader_keeps_the_moved_fence() {
    let rig = Rig::new("normal-address-reader", Arc::new(FakeDoor::default()));
    let session = rig.session("moved");
    rig.seed(&session, "r", true);
    let address = broca_core::session_addr(&session);
    assert!(!rig.wal.read_address_records(&address).unwrap().is_empty());
    let before = rig.bytes(&session);
    broca_wal::migration::atomic_json(
        &rig.wal.wal_path(&session).with_extension("moved"),
        &broca_wal::migration::Moved {
            cut_id: CUT.into(),
            holder: "other-mac".into(),
            lineage_id: "lineage".into(),
        },
    )
    .unwrap();
    assert!(rig
        .wal
        .read_address_records(&address)
        .unwrap_err()
        .to_string()
        .contains("other-mac"));
    assert_eq!(rig.bytes(&session), before);
}

fn decode_reply(op: contract::Op, reply: &Value) -> Result<Value, serde_json::Error> {
    fn decode<T: serde::de::DeserializeOwned + serde::Serialize>(
        reply: &Value,
    ) -> Result<Value, serde_json::Error> {
        serde_json::to_value(serde_json::from_value::<T>(reply.clone())?)
    }
    use contract::*;
    match op {
        Op::Describe => decode::<describe::DescribeReply>(reply),
        Op::Busy => decode::<busy::BusyReply>(reply),
        Op::Status => decode::<status::StatusReply>(reply),
        Op::Prepare => decode::<prepare::PrepareReply>(reply),
        Op::Export => decode::<export::ExportReply>(reply),
        Op::Diverged => decode::<diverged::DivergedReply>(reply),
        Op::Import => decode::<import::ImportReply>(reply),
        Op::Activate => decode::<activate::ActivateReply>(reply),
        Op::Abort => decode::<abort::AbortReply>(reply),
        Op::Release => decode::<release::ReleaseReply>(reply),
    }
}
#[tokio::test]
async fn contract_reply_vectors_decode_the_real_internal_service_answers() {
    let file = vectors("replies.json");
    for case in file["replies"].as_array().unwrap() {
        let op = contract::Op::from_method(case["method"].as_str().unwrap()).unwrap();
        assert_eq!(
            decode_reply(op, &case["reply"]).unwrap(),
            case["reply"],
            "{}",
            case["name"]
        );
        let rig = Rig::new("reply-vector", Arc::new(FakeDoor::default()));
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let mut params = json!({"refs":[],"cutId":CUT});
        match op {
            contract::Op::Describe => params = json!({}),
            contract::Op::Busy => params = json!({"refs":[]}),
            contract::Op::Export => {
                wire.call("prepare", params.clone()).await.unwrap();
            }
            contract::Op::Activate => {
                wire.call("import", params.clone()).await.unwrap();
            }
            contract::Op::Release => {
                wire.call("prepare", params.clone()).await.unwrap();
                wire.call("export", params.clone()).await.unwrap();
                params["holder"] = json!("receiver");
            }
            _ => {}
        }
        let reply = wire
            .call(op.method().strip_prefix("agent_data.").unwrap(), params)
            .await
            .unwrap();
        assert_eq!(decode_reply(op, &reply).unwrap(), reply, "{}", case["name"]);
    }
    for case in file["refused"].as_array().unwrap() {
        let op = contract::Op::from_method(case["method"].as_str().unwrap()).unwrap();
        assert!(
            decode_reply(op, &case["reply"]).is_err(),
            "{}",
            case["name"]
        );
    }
}
#[tokio::test]
async fn contract_op_refusal_vectors_bound_served_decode_refusals() {
    for case in vectors("op-refusals.json")["ops"].as_array().unwrap() {
        let method = format!("agent_data.{}", case["op"].as_str().unwrap());
        let op = contract::Op::from_method(&method).unwrap();
        let declared = contract::errors::allowed_refusals(op)
            .iter()
            .map(|c| c.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let additions = vectors("digest-mismatch.json");
        let mut vector = case["codes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        for addition in additions["op_additions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| a["op"] == case["op"])
        {
            vector.extend(
                addition["codes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|code| code.as_str().unwrap()),
            );
        }
        assert_eq!(declared, vector);
        let rig = Rig::new("op-refusal-vector", Arc::new(FakeDoor::default()));
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let error = assert_code(
            wire.raw(
                serde_json::to_vec(&json!({"method":method,"params":{"unknown":true}})).unwrap(),
            )
            .await,
            "invalid_request",
        );
        assert!(vector.contains(error["code"].as_str().unwrap()));
    }
}

#[tokio::test]
async fn archived_source_exports_only_verified_member_bytes_without_restoring_live_wal() {
    let rig = Rig::new("archive-source", Arc::new(FakeDoor::default()));
    let session = rig.session("archived");
    rig.seed_tool_run(&session, "archived-run");
    let bytes = rig.bytes(&session);
    let archive_dir = rig.root.join("wal-archive");
    std::fs::create_dir_all(&archive_dir).unwrap();
    let container = archive_dir.join("fold-1.ark");
    let mut archive = broca_wal::archive::ArchiveWriter::create(&container)
        .unwrap()
        .without_feature_floor();
    archive
        .append(&broca_core::session_addr(&session), &bytes)
        .unwrap();
    archive.seal().unwrap();
    std::fs::remove_file(rig.wal.wal_path(&session)).unwrap();
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let params = rig.params(CUT, std::slice::from_ref(&session));
    let item = wire.call("prepare", params.clone()).await.unwrap();
    assert_eq!(item["items"][0]["endOffset"], bytes.len() as u64);
    assert_eq!(
        item["items"][0]["digest"],
        broca_wal::migration::digest(&bytes)
    );
    wire.call("export", params.clone()).await.unwrap();
    let reference = params["refs"][0]["ref"].as_str().unwrap();
    assert_eq!(
        rig.door.streams.lock().unwrap().get(reference).unwrap(),
        &bytes
    );
    assert_ne!(std::fs::read(container).unwrap(), bytes);
    assert!(!rig.wal.wal_path(&session).exists());
}

fn retained_copy(rig: &Rig, session: &BindIdentity) {
    let mut start = started(session, "private-local-run");
    if let WalRecord::RunStarted { retention, .. } = &mut start {
        *retention = Some(60);
    }
    let mut wal = rig.wal.open_with_retention(session, Some(60)).unwrap();
    wal.append(1, &start, true).unwrap();
    wal.append(
        1,
        &WalRecord::RunFinished {
            reason: RunFinishReason::Completed,
            ts_ms: Some(2),
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
        true,
    )
    .unwrap();
}
#[tokio::test]
async fn diverged_marks_a_retained_local_copy_advanced() {
    let (rig, params, _) = incoming_rig("retained-divergence");
    let session = rig.session("incoming");
    retained_copy(&rig, &session);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("diverged", params).await.unwrap(),
        json!({"refs":[{"state":"advanced"}]})
    );
    assert!(!rig.root.join("wal-staging").exists());
}
#[tokio::test]
async fn import_reports_local_advanced_when_retention_appears_after_at_cut() {
    let (rig, params, _) = incoming_rig("retained-import");
    let session = rig.session("incoming");
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("diverged", params.clone()).await.unwrap(),
        json!({"refs":[{"state":"at_cut"}]})
    );
    retained_copy(&rig, &session);
    let before = std::fs::read(rig.wal.ephemeral_wal_path(&session)).unwrap();
    let error = assert_code(wire.call("import", params).await, "local_advanced");
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("retained copy with a deletion deadline"));
    let error: contract::errors::ErrorBody = serde_json::from_value(error).unwrap();
    assert_eq!(
        contract::errors::driver_action_for(contract::Op::Import, &error.code),
        contract::errors::DriverAction::TerminalForStep
    );
    assert_eq!(
        std::fs::read(rig.wal.ephemeral_wal_path(&session)).unwrap(),
        before
    );
    assert!(!rig.wal.wal_path(&session).exists());
    assert!(!rig.root.join("wal-staging").exists());
}
#[tokio::test]
async fn activate_reports_local_advanced_when_retention_appears_after_import() {
    let (rig, params, bytes) = incoming_rig("retained-activate");
    let session = rig.session("incoming");
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("diverged", params.clone()).await.unwrap(),
        json!({"refs":[{"state":"at_cut"}]})
    );
    wire.call("import", params.clone()).await.unwrap();
    retained_copy(&rig, &session);
    let staged = broca_wal::migration::staging_path(
        rig.wal.root(),
        CUT,
        &broca_core::session_addr(&session),
    )
    .unwrap();
    let before = std::fs::read(rig.wal.ephemeral_wal_path(&session)).unwrap();
    let error = assert_code(
        wire.call("activate", params.clone()).await,
        "local_advanced",
    );
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("retained copy with a deletion deadline"));
    let error: contract::errors::ErrorBody = serde_json::from_value(error).unwrap();
    assert_eq!(
        contract::errors::driver_action_for(contract::Op::Activate, &error.code),
        contract::errors::DriverAction::TerminalForStep
    );
    assert_eq!(
        std::fs::read(rig.wal.ephemeral_wal_path(&session)).unwrap(),
        before
    );
    assert_eq!(std::fs::read(staged).unwrap(), bytes);
    assert!(!rig.wal.wal_path(&session).exists());
    assert_eq!(
        wire.call("status", params).await.unwrap(),
        json!({"step":"imported"})
    );
}
#[tokio::test]
async fn activation_corruption_is_a_staging_digest_fork_and_abort_discards_it() {
    for damage in ["checksum", "append", "truncate"] {
        let (rig, params, mut bytes) = incoming_rig("corrupt-activation");
        let session = rig.session("incoming");
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        wire.call("import", params.clone()).await.unwrap();
        if damage == "checksum" {
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
        } else if damage == "append" {
            bytes.extend_from_slice(
                &broca_wal::framing::encode(
                    4,
                    1,
                    &WalRecord::RunFinished {
                        reason: RunFinishReason::Completed,
                        ts_ms: Some(3),
                        usage: Usage::default(),
                        error: None,
                        retries_used: 0,
                    },
                )
                .unwrap(),
            );
        } else {
            bytes = b"not a WAL".to_vec();
        }
        let staged = broca_wal::migration::staging_path(
            rig.wal.root(),
            CUT,
            &broca_core::session_addr(&session),
        )
        .unwrap();
        std::fs::write(&staged, &bytes).unwrap();
        let error = assert_code(
            wire.call("activate", params.clone()).await,
            "digest_mismatch",
        );
        let error: contract::errors::ErrorBody = serde_json::from_value(error).unwrap();
        assert_eq!(
            error
                .digest_mismatch_detail()
                .unwrap()
                .unwrap()
                .reason
                .as_deref(),
            Some(contract::errors::digest_mismatch_reason::STAGING_DIGEST_MISMATCH)
        );
        assert_eq!(
            contract::errors::driver_action_for(contract::Op::Activate, &error.code),
            contract::errors::DriverAction::TerminalForStep
        );
        assert_eq!(std::fs::read(&staged).unwrap(), bytes);
        assert!(!rig.wal.wal_path(&session).exists());
        assert_eq!(
            wire.call("status", params.clone()).await.unwrap(),
            json!({"step":"imported"})
        );
        wire.call("abort", params.clone()).await.unwrap();
        assert!(!staged.exists());
        assert_eq!(
            wire.call("status", params).await.unwrap(),
            json!({"step":"aborted"})
        );
    }
}
#[tokio::test]
async fn activation_busy_freeze_has_typed_refs_and_preserves_staging() {
    let (rig, params, bytes) = incoming_rig("activation-freeze");
    let session = rig.session("incoming");
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.call("import", params.clone()).await.unwrap();
    std::fs::write(rig.wal.wal_path(&session), &bytes).unwrap();
    let range = broca_wal::read_records_range(
        rig.wal.wal_path(&session),
        broca_wal::DEFAULT_MAX_READ_BYTES,
        0,
        None,
        None,
    )
    .unwrap();
    broca_wal::migration::freeze(
        &rig.wal.wal_path(&session),
        &broca_wal::migration::Freeze {
            cut_id: BACK.into(),
            end_offset: bytes.len() as u64,
            lineage_id: range.lineage_id.unwrap().to_string(),
        },
    )
    .unwrap()
    .unwrap();
    let error = assert_code(wire.call("activate", params.clone()).await, "busy");
    assert_eq!(error["detail"]["refs"][0]["ref"], params["refs"][0]["ref"]);
    assert_eq!(error["detail"]["refs"][0]["reason_code"], "moving");
    let staged = broca_wal::migration::staging_path(
        rig.wal.root(),
        CUT,
        &broca_core::session_addr(&session),
    )
    .unwrap();
    assert_eq!(std::fs::read(staged).unwrap(), bytes);
    assert_eq!(rig.bytes(&session), bytes);
}

#[tokio::test]
async fn pending_import_abort_uses_only_owned_staging_and_never_the_door() {
    use broca_module::cut_state::{CutState, CutStore, Intent};
    let (rig, mut params, bytes) = incoming_rig("abort-partial-import");
    let session = rig.session("incoming");
    params["refs"]
        .as_array_mut()
        .unwrap()
        .push(json!({"ref_kind":"broca-session","ref":"ffffffffffffffffffffffffffffffff"}));
    let staged = broca_wal::migration::staging_path(
        rig.wal.root(),
        CUT,
        &broca_core::session_addr(&session),
    )
    .unwrap();
    broca_wal::migration::stage(&staged, &bytes, &broca_wal::migration::digest(&bytes)).unwrap();
    let other = rig
        .root
        .join("wal-staging")
        .join(BACK)
        .join("unrelated.wal");
    std::fs::create_dir_all(other.parent().unwrap()).unwrap();
    std::fs::write(&other, b"other cut").unwrap();
    let state = CutState {
        step: contract::step::Step::None,
        refs: Some(serde_json::from_value(params["refs"].clone()).unwrap()),
        intent: Some(Intent {
            op: "import".into(),
            request: params.clone(),
        }),
        replies: Default::default(),
    };
    let authority = CutStore::new(&rig.root).path(CUT);
    broca_wal::migration::atomic_json(&authority, &state).unwrap();
    *rig.door.refusal.lock().unwrap() = Some("unavailable".into());
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.call("abort", params.clone()).await.unwrap();
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
    assert!(!staged.exists());
    assert_eq!(std::fs::read(other).unwrap(), b"other cut");
    let state: CutState = serde_json::from_slice(&std::fs::read(authority).unwrap()).unwrap();
    assert_eq!(state.step, contract::step::Step::Aborted);
    assert!(state.intent.is_none());
    assert!(!state.replies.contains_key("agent_data.import"));
    rig.service.reconcile_boot().await.unwrap();
    assert_eq!(
        wire.call("status", params).await.unwrap(),
        json!({"step":"aborted"})
    );
}
#[tokio::test]
async fn boot_keeps_an_unreadable_cut_pending_and_status_read_only() {
    use broca_module::cut_state::{CutState, CutStore, Intent};
    let (rig, params, _) = incoming_rig("boot-unavailable");
    let state = CutState {
        step: contract::step::Step::None,
        refs: Some(serde_json::from_value(params["refs"].clone()).unwrap()),
        intent: Some(Intent {
            op: "import".into(),
            request: params.clone(),
        }),
        replies: Default::default(),
    };
    let authority = CutStore::new(&rig.root).path(CUT);
    broca_wal::migration::atomic_json(&authority, &state).unwrap();
    let before = std::fs::read(&authority).unwrap();
    *rig.door.refusal.lock().unwrap() = Some("unavailable".into());
    rig.service.reconcile_boot().await.unwrap();
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("status", params.clone()).await.unwrap(),
        json!({"step":"none"})
    );
    assert_eq!(std::fs::read(&authority).unwrap(), before);
    *rig.door.refusal.lock().unwrap() = None;
    wire.call("import", params.clone()).await.unwrap();
    assert_eq!(
        wire.call("status", params).await.unwrap(),
        json!({"step":"imported"})
    );
}
#[tokio::test]
async fn two_ref_activation_uses_each_imported_items_metadata_without_the_door() {
    let door = Arc::new(FakeDoor::default());
    let source = Rig::new("two-ref-source", door.clone());
    let receiver = Rig::new("two-ref-receiver", door.clone());
    let sessions = [source.session("a"), source.session("b")];
    for session in &sessions {
        source.seed_tool_run(session, &session.session);
        let reference = contract::session_ref(
            &session.harness,
            &session.project_root.to_string_lossy(),
            &session.session,
        );
        door.plant(CUT, &reference, source.bytes(session));
    }
    let mut params = source.params(CUT, &sessions);
    let mut wire = Wire::new(&receiver, entorhinal(), true).await;
    let first = wire.call("import", params.clone()).await.unwrap();
    assert_eq!(first["items"].as_array().unwrap().len(), 2);
    params["refs"].as_array_mut().unwrap().reverse();
    *door.refusal.lock().unwrap() = Some("unavailable".into());
    assert_eq!(wire.call("import", params.clone()).await.unwrap(), first);
    wire.call("activate", params).await.unwrap();
    for session in &sessions {
        assert_eq!(receiver.bytes(session), source.bytes(session));
    }
}

#[tokio::test]
async fn closed_session_with_a_queued_send_is_busy_before_prepare() {
    use broca_protocol::content::{ContentBlock, Message, Role};
    let rig = Rig::new("queued-send", Arc::new(FakeDoor::default()));
    let session = rig.session("queued");
    rig.seed(&session, "complete", true);
    let queued = WalRecord::Queued {
        retention: None,
        ts_ms: None,
        hooks: None,
        title: None,
        keep_warm: None,
        conversation_key: None,
        delivery: None,
        send_id: None,
        submission_id: broca_protocol::SubmissionId("pending".into()),
        request_identity_digest: None,
        principal: None,
        request: Box::new(broca_protocol::request::SubmissionRequest {
            on_restart: None,
            keep_warm: None,
            scope: Default::default(),
            project_id: None,
            principal: None,
            model: broca_protocol::ModelHandle::new("stub", "m"),
            input: vec![Message {
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "queued input".into(),
                }],
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
            }],
            tools: Some(vec![]),
            policy_allowed: Default::default(),
            tool_choice: broca_protocol::ToolChoice::Auto,
            generation: Default::default(),
            cache: None,
            stop_when: vec![],
            budget: None,
            auth: None,
            context_limit: None,
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen: Default::default(),
        }),
    };
    rig.wal
        .open(&session)
        .unwrap()
        .append(1, &queued, true)
        .unwrap();
    let params = rig.params(CUT, std::slice::from_ref(&session));
    let before = rig.bytes(&session);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let reply = wire
        .call("busy", json!({"refs":params["refs"]}))
        .await
        .unwrap();
    assert_eq!(reply["refs"][0]["reason_code"], "send_queued");
    assert_code(wire.call("prepare", params).await, "busy");
    assert_eq!(rig.bytes(&session), before);
    assert!(!rig.wal.wal_path(&session).with_extension("moving").exists());
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct ErrorEvents(Arc<Mutex<Vec<String>>>);
impl tracing::Subscriber for ErrorEvents {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::ERROR
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push_str(&format!(" {}={value:?}", field.name()));
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
#[tokio::test]
async fn diverged_unheld_ref_is_at_cut_even_when_import_will_refuse_its_seal() {
    let (rig, params, _) = incoming_rig("unheld-corrupt-cut");
    *rig.door.digest.lock().unwrap() = Some("0".repeat(64));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("diverged", params.clone()).await.unwrap(),
        json!({"refs":[{"state":"at_cut"}]})
    );
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
    assert_code(wire.call("import", params).await, "digest_mismatch");
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn diverged_unheld_ref_does_not_read_the_door_or_complete_a_pending_import() {
    use broca_module::cut_state::{CutState, CutStore, Intent};
    let (rig, params, _) = incoming_rig("unheld-no-door");
    *rig.door.refusal.lock().unwrap() = Some("verify_failed".into());
    let state = CutState {
        step: contract::step::Step::None,
        refs: Some(serde_json::from_value(params["refs"].clone()).unwrap()),
        intent: Some(Intent {
            op: "import".into(),
            request: params.clone(),
        }),
        replies: Default::default(),
    };
    let authority = CutStore::new(&rig.root).path(CUT);
    broca_wal::migration::atomic_json(&authority, &state).unwrap();
    let before = std::fs::read(&authority).unwrap();
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("diverged", params).await.unwrap(),
        json!({"refs":[{"state":"at_cut"}]})
    );
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(authority).unwrap(), before);
}
#[tokio::test]
async fn diverged_held_ref_with_a_bad_cut_is_unknown_and_logs_the_cause() {
    for cause in ["verify_failed", "sealed SHA mismatch"] {
        let (rig, params, _) = incoming_rig("held-bad-cut");
        let session = rig.session("incoming");
        rig.seed(&session, "local", true);
        let before = rig.bytes(&session);
        if cause == "verify_failed" {
            *rig.door.refusal.lock().unwrap() = Some(cause.into());
        } else {
            *rig.door.digest.lock().unwrap() = Some("0".repeat(64));
        }
        let events = ErrorEvents(Arc::default());
        let dispatch = tracing::Dispatch::new(events.clone());
        let _guard = tracing::dispatcher::set_default(&dispatch);
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        assert_eq!(
            wire.call("diverged", params.clone()).await.unwrap(),
            json!({"refs":[{"state":"unknown"}]})
        );
        assert_eq!(rig.door.calls.load(Ordering::SeqCst), 1);
        assert_eq!(rig.bytes(&session), before);
        assert!(!rig.root.join("wal-staging").exists());
        let logs = events.0.lock().unwrap().join("\n");
        assert!(logs.contains(cause), "{logs}");
        assert!(logs.contains(CUT), "{logs}");
        assert!(
            logs.contains(params["refs"][0]["ref"].as_str().unwrap()),
            "{logs}"
        );
        assert!(!logs.contains("tool transcript result"));
    }
}

#[tokio::test]
async fn import_refuses_an_unknown_wal_frame_version_before_staging() {
    let (rig, params, mut bytes) = incoming_rig("unknown-frame-version");
    bytes[4] = 99;
    rig.door
        .plant(CUT, params["refs"][0]["ref"].as_str().unwrap(), bytes);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let error = assert_code(wire.call("import", params).await, "incompatible_version");
    assert_eq!(error["detail"]["found"], "broca-wal/v99");
    assert_nothing_staged(&rig);
}
