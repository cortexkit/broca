//! Operations a session kept in the session store serves, reached through the real serve path:
//! each request goes over the wire into `serve_connection`, past its refusal of ops the new
//! storage doesn't serve, into the registry and on to the store. The session is created in the
//! new storage through the same registry the connection serves, by a send the store actor runs
//! over a scripted provider; it has no session log, so whatever a request reads, it read from the
//! store.
//!
//! The daemon side is played by the test: it sends the HELLO_ACK and binds each route.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{AllowAll, FileLeaseStore, OutputEvent};
use broca_module::actor::{SendKey, SendOutcome, SendPlan};
use broca_module::executor::{
    RunExecutor, RunKind, StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode,
};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{serve_connection, ServeEnd};
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason};
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, SubmissionRequest, Usage,
};
use serde_json::{json, Value};
use subc_protocol::session::ModuleControlRequest;
use subc_protocol::{
    BindIdentity as SubcBindIdentity, Flags, Frame, FrameType, ModuleHelloAckBody, Principal,
    Priority, RouteTarget, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::sync::mpsc;

mod app_readers;
mod send_status;

/// The compaction provider the planned session's plan names.
const PROVIDER: &str = "ck-mc";

fn core() -> Principal {
    Principal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

fn provider() -> Principal {
    Principal::Reserved {
        module_id: PROVIDER.into(),
    }
}

/// A compaction provider that is always ready and never hands over a view. Its catalog entry
/// declares entry attribution (`compaction-entry-speaks-for/v1`) when `declares` is set.
#[derive(Default)]
struct ReadyProvider {
    declares: bool,
}

#[async_trait]
impl broca_core::CompactionPlane for ReadyProvider {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        panic!("a session on the current compaction contract never calls the first protocol")
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        _: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        let answer = match method {
            "compaction.setup" => json!({"answer": "ready"}),
            "compaction.notice" => json!({"answer": "received"}),
            "compaction.ride" => json!({"answer": "nothing"}),
            other => panic!("unexpected compaction method {other}"),
        };
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }

    async fn provider_capabilities(&self, provider: &str) -> broca_core::ProviderCapabilities {
        assert_eq!(
            provider, PROVIDER,
            "the catalog entry read is the plan's provider's"
        );
        let mut provides = vec!["compaction-provider/v2".to_owned()];
        if self.declares {
            provides.push(
                broca_core::compaction_v2::protocol::COMPACTION_ENTRY_SPEAKS_FOR_PROVIDES.into(),
            );
        }
        broca_core::ProviderCapabilities::Declared(provides)
    }
}

/// Runs a store session's runs through the new loop over a scripted provider, as the module's
/// executor runs them. A run whose plan names a compaction provider freezes the compaction plan
/// version and a context limit, as the module's executor does, so its session is one whose
/// `compaction-runner/v1` route admits that provider.
struct StoreLoop {
    provider: broca_core::provider::StubProvider,
    gate: Option<Arc<app_readers::RunGate>>,
    requests: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    compaction: Arc<ReadyProvider>,
    /// The display marks every send's run records, as admission would have checked them.
    marks: Option<broca_protocol::MessageMarks>,
    /// Whether each step records the attempt a transport would (see `GatedProvider::attempts`).
    attempts: bool,
}

impl StoreLoop {
    fn compacts(control: &broca_core::RunControl) -> bool {
        control
            .manifest
            .as_deref()
            .is_some_and(|manifest| matches!(broca_core::compaction::item(manifest), Ok(Some(_))))
    }
}

#[async_trait]
impl RunExecutor for StoreLoop {
    async fn compaction_context_limit(
        &self,
        model: &cortexkit_role_llm_runner::head_selection::PlanModel,
    ) -> Result<Option<u64>, String> {
        assert_eq!(model.provider, "stub");
        assert_eq!(model.model, "m");
        Ok(Some(200_000))
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        Some(self.compaction.clone() as Arc<dyn broca_core::CompactionPlane>)
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        use broca_session_loop::writer::Writer;
        let StoreRunJob {
            mut req,
            mut control,
            mut writer,
            mut parts,
            mode,
            ..
        } = job;
        if Self::compacts(&control) {
            control.new_log_compaction_plan_version = Some(broca_core::compaction_v2::PLAN_VERSION);
            control.admitted_context_limit = Some(200_000);
        }
        if self.marks.is_some() {
            control.display_message_marks = self.marks.clone();
        }
        let result = async {
            let tools = broca_core::StubToolPlane::new().with_canned_text("read", "file contents");
            let now = || 1;
            let provider = app_readers::GatedProvider {
                provider: &self.provider,
                gate: self.gate.as_deref(),
                requests: &self.requests,
                attempts: self.attempts,
            };
            let seams = broca_session_loop::run::Seams {
                steer_yields: None,
                provider: &provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: Default::default(),
                held: None,
                approval: &AllowAll,
                hook_plane: None,
                now_ms: &now,
                session_address: None,
                compaction_plane: control.compaction_plane.as_deref(),
                compaction_session: None,
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
                step_progress: control.step_progress.clone(),
                accepting_drain: None,
                model_call_ready: None,
                pause_for_auth_at_start: None,
                account_continue: None,
            };
            match mode {
                StoreRunMode::Fresh => {
                    let admission = broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &self.provider,
                        writer.operations(),
                        Some(1),
                    )
                    .map_err(StoreRunError::Admission)?;
                    broca_session_loop::run::run(&mut writer, parts.as_mut(), admission, &seams)
                        .await
                }
                StoreRunMode::Seal(cause) => {
                    broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause).await
                }
                _ => broca_session_loop::run::resume(&mut writer, parts.as_ref(), &seams).await,
            }
            .map_err(StoreRunError::Loop)
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

/// A send carrying no tools of its own, as a planned session's sends do.
fn request(text: &str) -> SubmissionRequest {
    serde_json::from_value(json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": text}]}],
        "stop_when": [{"type": "step_count", "n": 3}],
    }))
    .unwrap()
}

/// A plan naming the compaction provider [`PROVIDER`].
fn compacting_plan() -> SendPlan {
    let plan = json!({"compaction_item": {"provider": PROVIDER, "preset": "default"}});
    SendPlan {
        identity: "plan-1".into(),
        manifest: Some(Arc::new(broca_records::record::PlanManifest {
            rich_ui: None,
            plan: broca_records::record::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
            plan_identity: "plan-1".into(),
            composition_digest: "composition".into(),
            tools: Arc::new(Vec::new()),
            system_text: "you are a worker".into(),
            system_digest: "system".into(),
            join_version: 1,
            role_versions: Default::default(),
            fetches: vec![],
            declarations: vec![],
        })),
        model_check: None,
    }
}

struct Rig {
    dir: PathBuf,
    registry: Arc<LineageRegistry>,
    /// The session the test binds, as the registry resolves the bind's identity.
    key: BindIdentity,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    _serve: tokio::task::JoinHandle<Result<ServeEnd, broca_module_serve::serve::ServeIoError>>,
    corr: u64,
    requests: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Rig {
    /// A module on a fresh state root whose new storage creates sessions, its runs scripted by
    /// `steps`.
    async fn start(tag: &str, steps: Vec<Vec<NormalizedEvent>>) -> Self {
        Self::start_with(tag, steps, ReadyProvider::default(), None).await
    }

    /// [`Rig::start`] with each step recording the attempt a transport would, so its request can
    /// be rebuilt and checked against the digest it recorded.
    async fn start_attempting(tag: &str, steps: Vec<Vec<NormalizedEvent>>) -> Self {
        Self::start_gated(tag, steps, ReadyProvider::default(), None, None, true).await
    }

    /// [`Rig::start`] with `compaction` as the compaction provider, and every run recording
    /// `marks` as its send's display marks.
    async fn start_with(
        tag: &str,
        steps: Vec<Vec<NormalizedEvent>>,
        compaction: ReadyProvider,
        marks: Option<broca_protocol::MessageMarks>,
    ) -> Self {
        Self::start_gated(tag, steps, compaction, marks, None, false).await
    }

    async fn start_gated(
        tag: &str,
        steps: Vec<Vec<NormalizedEvent>>,
        compaction: ReadyProvider,
        marks: Option<broca_protocol::MessageMarks>,
        gate: Option<Arc<app_readers::RunGate>>,
        attempts: bool,
    ) -> Self {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-store-ops-serve-{}-{tag}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        broca_core::media_uploads::create_upload_directories(&dir).unwrap();
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let executor: Arc<dyn RunExecutor> = Arc::new(StoreLoop {
            provider: broca_core::provider::StubProvider::new(steps),
            gate,
            requests: requests.clone(),
            compaction: Arc::new(compaction),
            attempts,
            marks,
        });
        let runtime = broca_module::store_open::StoreRuntime::open(
            &dir,
            broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
            executor.clone(),
            Duration::from_millis(100),
        )
        .await
        .unwrap();
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            executor,
            std::sync::Arc::new(broca_module::store_open::StoreSlot::loaded(Arc::new(
                runtime,
            ))),
        )
        .with_idle_ttl(Duration::from_millis(100));
        let registry = LineageRegistry::new(Arc::new(opener));
        // A bind resolves the session's address through the deployment's inventory, which
        // today's store keeps.
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        broca_store::session_refs::migrate(
            &dir.join("store.db"),
            &broca_store::session_refs::StoredSessions::new(&dir),
        )
        .unwrap();
        registry.set_session_address_inventory(store);
        let registry = Arc::new(registry);
        // The identity every bind below names, resolved as the serve path resolves it.
        let key = registry
            .resolve_session_address(&BindIdentity::new("", "runner", "head"), None, None)
            .await
            .unwrap();
        let (client, server) = tokio::io::duplex(256 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);
        let served = registry.clone();
        let serve = tokio::spawn(async move {
            serve_connection(
                server_read,
                server_write,
                "broca",
                served.clone(),
                served,
                broca_module_serve::serve::VaultGauge::not_in_use(),
            )
            .await
        });
        let hello = read_frame(&mut reader).await.unwrap().unwrap();
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
        write_frame(&mut writer, &ack).await.unwrap();
        Self {
            dir,
            registry,
            key,
            reader,
            writer,
            _serve: serve,
            corr: 1,
            requests,
        }
    }

    /// Sends `text` to the session through the registry the connection serves, and waits until
    /// its run has ended.
    async fn send(&self, run_id: &str, text: &str, plan: Option<SendPlan>) {
        let outcome = self
            .registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                request(text),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                plan,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("the session opens");
        assert!(
            matches!(
                outcome,
                SendOutcome::Active { .. } | SendOutcome::Pending { .. }
            ),
            "{outcome:?}"
        );
        self.settled(run_id).await;
    }

    /// Creates the session in the session store by importing, through the registry the connection
    /// serves, a head whose first message carries `image`, owned by core under its head scope.
    async fn import_with_image(&self, image: &[u8]) {
        use broca_module::store_import::{ImportCommand, ImportRoots, StoreCommand, StoreOutcome};
        let folder = self.dir.join("imports");
        std::fs::create_dir_all(&folder).unwrap();
        let export = serde_json::to_vec(&json!({
            "info": {"id": "ses_head"},
            "messages": [
                {"id": "msg_u1", "type": "user", "time": {"created": 1000}, "text": "Look.",
                 "files": [{"mime": "image/png", "data": base64(image)}]},
                {"id": "msg_a1", "type": "assistant", "time": {"created": 2000, "completed": 3000},
                 "content": [{"type": "text", "text": "Seen."}]}
            ]
        }))
        .unwrap();
        let path = folder.join("head.json");
        std::fs::write(&path, &export).unwrap();
        let params = serde_json::from_value(json!({
            "import_id": "imp-1",
            "source": {
                "harness": "opencode2",
                "path": path.display().to_string(),
                "session": "ses_head",
                "sha256": digest(&export),
            },
            "origin": {"kind": "import", "source_ref": "thalamus:ses_head"},
        }))
        .unwrap();
        let core = broca_protocol::BindPrincipal::Reserved {
            module_id: "prefrontal-core".into(),
        };
        let command = StoreCommand::Import(Box::new(ImportCommand {
            params,
            principal: core.clone(),
            // An import arrives on a route scoped to its owner's head.
            scope: Some(broca_protocol::SessionScope {
                owner: core,
                scope_ref: "head-1".into(),
                scope_epoch: 1,
                flow_id: None,
                sender: None,
                agent_id: None,
                run_id: None,
                agent_id_frozen: false,
            }),
            roots: Arc::new(ImportRoots::new(vec![folder.clone()], Some(folder))),
            now_ms: 9_000,
        }));
        let outcome = self
            .registry
            .route_store_command(&self.key, command)
            .await
            .unwrap();
        assert!(matches!(outcome, StoreOutcome::Imported(_)), "{outcome:?}");
    }

    async fn settled(&self, run_id: &str) {
        use broca_module::serve::{ServeHandler, SessionReadRequest};
        for _ in 0..2000 {
            let read = self
                .registry
                .read(SessionReadRequest::new(self.key.clone(), None, Some(1)))
                .await
                .expect("the session reads");
            if read.lineage_state.last_run_id.as_deref() == Some(run_id)
                && read.lineage_state.state != "active"
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("run {run_id} never settled");
    }

    /// The session is kept in the session store, and nothing of it is on today's storage.
    /// Installs retention over the rig's own leases and store, as the module installs it at
    /// startup: what deletes a session from the session store.
    fn install_retention(&self) {
        self.registry.set_retention_service(Arc::new(
            broca_module::retention::RetentionService::new(
                &self.dir,
                Arc::new(FileLeaseStore::new(self.dir.join("leases"))),
            )
            .unwrap(),
        ));
    }

    fn assert_store_only(&self) {
        assert!(
            broca_records::session_storage::session_storage_class(
                &self.dir,
                &broca_core::session_addr(&self.key)
            )
            .unwrap()
            .is_some(),
            "the session's head is in the new storage"
        );
    }

    async fn request(&mut self, channel: u16, body: Value) -> Frame {
        self.corr += 1;
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            channel,
            u32::from(channel != 0),
            self.corr,
            serde_json::to_vec(&body).unwrap(),
        )
        .unwrap();
        write_frame(&mut self.writer, &frame).await.unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let reply = read_frame(&mut self.reader).await.unwrap().unwrap();
                if reply.header.channel == channel && reply.header.corr == self.corr {
                    return reply;
                }
            }
        })
        .await
        .expect("a reply")
    }

    /// Binds `channel` to the session as the daemon relays it, stamped with `principal`, for the
    /// roles `roles` declares (the session's own route when `None`). Returns the bind's reply.
    async fn bind(
        &mut self,
        channel: u16,
        principal: Principal,
        roles: Option<BTreeMap<String, String>>,
    ) -> Frame {
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("runner", "head"),
            principal: Some(principal),
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: roles,
            call_mode: None,
        };
        self.request(0, serde_json::to_value(bind).unwrap()).await
    }

    /// Binds the session's own route for `principal`.
    async fn bind_session(&mut self, channel: u16, principal: Principal) {
        let reply = self.bind(channel, principal, None).await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
    }

    /// A request's reply body, which must be a response.
    async fn answered(&mut self, channel: u16, method: &str, params: Value) -> Value {
        let reply = self
            .request(channel, json!({"method": method, "params": params}))
            .await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{method}: {}",
            String::from_utf8_lossy(&reply.body)
        );
        serde_json::from_slice(&reply.body).unwrap()
    }

    /// The result an op on the session's own route answers with.
    async fn result(&mut self, channel: u16, method: &str, params: Value) -> Value {
        self.answered(channel, method, params).await["result"].clone()
    }

    /// The refusal code `method` answers with.
    async fn refused(&mut self, channel: u16, method: &str, params: Value) -> String {
        let reply = self
            .request(channel, json!({"method": method, "params": params}))
            .await;
        assert_eq!(
            reply.header.ty,
            FrameType::Error,
            "{method}: {}",
            String::from_utf8_lossy(&reply.body)
        );
        serde_json::from_slice::<Value>(&reply.body).unwrap()["code"]
            .as_str()
            .unwrap()
            .to_owned()
    }
}

/// The text of each message of a raw `session.history` page.
fn history_texts(page: &Value) -> Vec<String> {
    page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|entry| entry["message"]["content"].as_array().unwrap())
        .filter_map(|block| block["text"].as_str().map(str::to_owned))
        .collect()
}

/// `session.history` reaches the store through the serve path in both forms and pages the
/// session's messages. A request with no form is the raw form.
#[tokio::test]
async fn history_reaches_the_store_through_serve() {
    let mut rig = Rig::start(
        "history",
        vec![text_step("a1", "hello"), text_step("a2", "again")],
    )
    .await;
    rig.send("r1", "first", None).await;
    rig.send("r2", "second", None).await;
    rig.assert_store_only();
    rig.bind_session(1, core()).await;
    for params in [json!({}), json!({"form": "raw"})] {
        let page = rig.result(1, "session.history", params.clone()).await;
        assert_eq!(
            history_texts(&page),
            ["first", "hello", "second", "again"],
            "{params}: {page}"
        );
    }
    // The display form answers with its own page shape, read from the store.
    let display = rig
        .result(1, "session.history", json!({"form": "display"}))
        .await;
    assert!(
        display["messages"]
            .as_array()
            .is_some_and(|messages| !messages.is_empty()),
        "{display}"
    );
    let page = rig
        .result(1, "session.history", json!({"from_ordinal": 2, "limit": 2}))
        .await;
    assert_eq!(history_texts(&page), ["hello", "second"], "{page}");
}

/// Magic Context's `session.entries`, on the `compaction-runner/v1` route, reaches the store
/// through the serve path: the route binds only for the provider the session's plan froze, and
/// pages the session's entries from the store.
#[tokio::test]
async fn compaction_entries_reach_the_store_through_serve() {
    let mut rig = Rig::start(
        "entries",
        vec![text_step("a1", "hello"), text_step("a2", "again")],
    )
    .await;
    rig.send("r1", "first", Some(compacting_plan())).await;
    rig.send(
        "r2",
        "second",
        Some(SendPlan {
            manifest: None,
            ..compacting_plan()
        }),
    )
    .await;
    rig.assert_store_only();
    let compaction = || {
        Some(BTreeMap::from([(
            "compaction-runner".to_owned(),
            "v1".to_owned(),
        )]))
    };
    // Another module is refused at the bind, by the store's check of the frozen provider.
    let refused = rig.bind(2, core(), compaction()).await;
    assert_eq!(
        refused.header.ty,
        FrameType::Error,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.body).unwrap()["detail"]["reason"],
        "provider_mismatch",
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    let bound = rig.bind(1, provider(), compaction()).await;
    assert_eq!(
        bound.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&bound.body)
    );
    let page = rig
        .answered(
            1,
            "session.entries",
            json!({"select": {"by": "after"}, "detail": "full"}),
        )
        .await;
    let ordinals: Vec<u64> = page["entries"]
        .as_array()
        .unwrap_or_else(|| panic!("{page}"))
        .iter()
        .map(|entry| entry["ordinal"].as_u64().unwrap())
        .collect();
    assert_eq!(ordinals, [1, 2, 3, 4], "{page}");
    let page = rig
        .answered(
            1,
            "session.entries",
            json!({"select": {"by": "after", "after_ordinal": 2, "limit": 1}, "detail": "compact"}),
        )
        .await;
    assert_eq!(
        page["entries"].as_array().unwrap().len(),
        1,
        "a page from ordinal 3 holds one entry: {page}"
    );
    assert_eq!(page["entries"][0]["ordinal"], 3, "{page}");
}

/// A stored agent mark reaches the provider's `session.entries` only when its catalog entry
/// declares entry attribution, whether an actor holds the session or not, and never moves the
/// entry's digest.
#[tokio::test]
async fn compaction_entries_carry_a_stored_speaker_only_for_a_declaring_provider() {
    let speaker = json!({"agent_id": "agent-a", "name": "Ada"});
    let mut digests = Vec::new();
    for declares in [true, false] {
        let mut rig = Rig::start_with(
            &format!("entries-speaker-{declares}"),
            vec![text_step("a1", "hello")],
            ReadyProvider { declares },
            Some(serde_json::from_value(json!({"speaks_for": speaker})).unwrap()),
        )
        .await;
        rig.send("r1", "agent words", Some(compacting_plan())).await;
        rig.assert_store_only();
        let compaction = Some(BTreeMap::from([(
            "compaction-runner".to_owned(),
            "v1".to_owned(),
        )]));
        let bound = rig.bind(1, provider(), compaction).await;
        assert_eq!(bound.header.ty, FrameType::Response);
        let page = rig
            .answered(
                1,
                "session.entries",
                json!({"select": {"by": "after"}, "detail": "full"}),
            )
            .await;
        let first = &page["entries"][0];
        assert_eq!(first["role"], "user", "{page}");
        if declares {
            assert_eq!(first["speaks_for"], speaker, "{page}");
        } else {
            assert!(first.get("speaks_for").is_none(), "{page}");
        }
        digests.push(first["digest"].clone());
    }
    assert_eq!(digests[0], digests[1], "attribution moves no digest");
}

/// A one-pixel PNG.
const PNG: &str = "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000c49444154789c63f8cfc0000003010100c9fe92ef0000000049454e44ae426082";
/// A three-by-two PNG.
const WIDE: &str = "89504e470d0a1a0a0000000d49484452000000030000000208020000001216f14d0000001049444154789c63f8cfc000410c70160041d205fb87f0b9480000000049454e44ae426082";

fn bytes_of(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
        .collect()
}

fn digest(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    format!("{:x}", sha2::Sha256::digest(bytes))
}

fn base64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// `session.media_read` and the upload operations reach the store through the serve path. A
/// head imported into the session store carries an image, which reads back byte-equal, whole and
/// by byte range. An image uploaded to the session is staged and committed, and reading it is
/// refused while no record of the session names it.
#[tokio::test]
async fn media_read_and_uploads_reach_the_store_through_serve() {
    let mut rig = Rig::start("media", Vec::new()).await;
    let (image, unsent) = (bytes_of(PNG), bytes_of(WIDE));
    rig.import_with_image(&image).await;
    rig.assert_store_only();
    rig.bind_session(1, core()).await;
    let whole = rig
        .result(
            1,
            "session.media_read",
            json!({"sha256": digest(&image), "offset": 0, "length": image.len()}),
        )
        .await;
    assert_eq!(whole["data"], json!(base64(&image)), "{whole}");
    assert_eq!(whole["size"], json!(image.len()), "{whole}");
    assert_eq!(whole["mime"], "image/png", "{whole}");
    let range = rig
        .result(
            1,
            "session.media_read",
            json!({"sha256": digest(&image), "offset": 3, "length": 8}),
        )
        .await;
    assert_eq!(range["data"], json!(base64(&image[3..11])), "{range}");

    let begin = rig
        .result(
            1,
            "session.upload_begin",
            json!({"mime": "image/png", "bytes": unsent.len(), "sha256": digest(&unsent)}),
        )
        .await;
    let id = begin["upload_id"].as_str().unwrap().to_owned();
    let appended = rig
        .result(
            1,
            "session.upload_append",
            json!({"upload_id": id, "offset": 0, "data": base64(&unsent)}),
        )
        .await;
    assert_eq!(appended["received"], json!(unsent.len()), "{appended}");
    let committed = rig
        .result(1, "session.upload_commit", json!({"upload_id": id}))
        .await;
    assert_eq!(committed["state"], "committed", "{committed}");
    assert_eq!(committed["sha256"], json!(digest(&unsent)), "{committed}");
    let status = rig
        .result(1, "session.upload_status", json!({"upload_id": id}))
        .await;
    assert_eq!(status["state"], "committed", "{status}");
    assert_eq!(
        rig.refused(
            1,
            "session.media_read",
            json!({"sha256": digest(&unsent), "offset": 0, "length": 10}),
        )
        .await,
        "unknown_media",
        "an upload no record names is not readable"
    );
}

/// `session.warm` reaches the store through the serve path. A session the session store keeps is
/// answered by the store actor's warm rules (here: this module reads no session's cache policy),
/// never refused as an operation the session store doesn't serve.
#[tokio::test]
async fn warm_reaches_the_store_through_serve() {
    let mut rig = Rig::start("warm", vec![text_step("a1", "hello")]).await;
    rig.send("r1", "hi", None).await;
    rig.assert_store_only();
    rig.bind_session(1, core()).await;
    assert_eq!(
        rig.refused(1, "session.warm", json!({})).await,
        "warm_unsupported"
    );
}

/// Writes `body` as `name` into the folder `TAG_MAP_SAMPLES_DIR` names, when it names one: the
/// bytes the serve path sent, kept as samples of the operation's wire form.
fn tag_map_sample(name: &str, body: &[u8]) {
    if let Some(dir) = std::env::var_os("TAG_MAP_SAMPLES_DIR") {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(name), body).unwrap();
    }
}

fn plan_view_sample(name: &str, body: &[u8]) {
    if let Some(dir) = std::env::var_os("BROCA_PLAN_VIEW_SAMPLE_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(PathBuf::from(dir).join(name), body).unwrap();
    }
}

fn inspection_plan(label: &str) -> SendPlan {
    let mut plan = compacting_plan();
    plan.identity = label.into();
    let manifest = Arc::make_mut(plan.manifest.as_mut().unwrap());
    manifest.plan_identity = label.into();
    manifest.system_text = format!("Synthetic session: {label}");
    let mut providers = vec![
        json!({"provider":PROVIDER,"tools":[]}),
        json!({"provider":"synthetic-persona","tools":[]}),
    ];
    providers.sort_by(|a, b| a["provider"].as_str().cmp(&b["provider"].as_str()));
    let composition = json!({"providers":providers, "step_transform_items":[], "compaction":{"provider":PROVIDER}});
    let composition_digest =
        cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap();
    manifest.composition_digest = composition_digest.clone();
    manifest.plan = broca_records::record::OpaqueJson::from_json_text(
        json!({
        "compaction_item":{"provider":PROVIDER,"preset":"default","params":{}},
        "system_text_items":[{"provider":"synthetic-persona","params":{}}],
        "tool_items":[], "step_transform_items":[],
        "composition":composition, "composition_digest":composition_digest,
        })
        .to_string(),
    )
    .unwrap();
    manifest.fetches = vec![serde_json::from_value(json!({
        "provider":"synthetic-persona", "catalog_digest":"synthetic", "tools":[],
        "text":manifest.system_text,
    }))
    .unwrap()];
    plan
}

#[tokio::test]
async fn plan_view_wire_serves_applied_and_pending_plans_without_operator_requirement() {
    let mut rig = Rig::start(
        "synthetic-plan-view",
        vec![text_step("a", "first"), text_step("b", "second")],
    )
    .await;
    rig.send("r1", "hello", Some(inspection_plan("fresh")))
        .await;
    rig.assert_store_only();
    rig.bind_session(1, core()).await;
    let frame = rig
        .request(1, json!({"method":"session.plan_view","params":{}}))
        .await;
    assert_eq!(frame.header.ty, FrameType::Response);
    let fresh: Value = serde_json::from_slice(&frame.body).unwrap();
    assert_eq!(fresh["result"]["generation"], 0);
    assert_eq!(
        fresh["result"]["system"]["text"],
        "Synthetic session: fresh"
    );
    plan_view_sample("fresh.json", &frame.body);
    assert!(matches!(
        rig.registry
            .route_session_refresh(
                &rig.key,
                1,
                "immediately".into(),
                Box::new(Arc::try_unwrap(inspection_plan("applied").manifest.unwrap()).unwrap()),
                Vec::new(),
                None,
            )
            .await
            .unwrap(),
        broca_module::session_cmd::SessionRefreshOutcome::Accepted { .. }
    ));
    rig.send("r2", "next", None).await;
    let frame = rig
        .request(1, json!({"method":"session.plan_view","params":{}}))
        .await;
    assert_eq!(frame.header.ty, FrameType::Response);
    let applied: Value = serde_json::from_slice(&frame.body).unwrap();
    assert_eq!(applied["result"]["generation"], 1);
    assert_eq!(
        applied["result"]["system"]["text"],
        "Synthetic session: applied"
    );
    plan_view_sample("applied.json", &frame.body);
    assert!(matches!(
        rig.registry
            .route_session_refresh(
                &rig.key,
                2,
                "on_prefix_rebuild".into(),
                Box::new(Arc::try_unwrap(inspection_plan("pending").manifest.unwrap()).unwrap()),
                Vec::new(),
                None,
            )
            .await
            .unwrap(),
        broca_module::session_cmd::SessionRefreshOutcome::Accepted { .. }
    ));
    let frame = rig
        .request(1, json!({"method":"session.plan_view","params":{}}))
        .await;
    assert_eq!(frame.header.ty, FrameType::Response);
    let pending: Value = serde_json::from_slice(&frame.body).unwrap();
    assert_eq!(pending["result"]["system"], applied["result"]["system"]);
    assert_eq!(pending["result"]["pending"], json!({"generation":2}));
    plan_view_sample("pending.json", &frame.body);
    assert_eq!(
        rig.refused(1, "session.plan_view", json!({"generation":2}))
            .await,
        "invalid_params"
    );
}

#[tokio::test]
async fn plan_view_wire_no_plan_and_too_large_are_named_refusals() {
    let mut unplanned = Rig::start("synthetic-no-plan", vec![text_step("a", "reply")]).await;
    unplanned.send("r1", "hello", None).await;
    unplanned.bind_session(1, core()).await;
    let frame = unplanned
        .request(1, json!({"method":"session.plan_view","params":{}}))
        .await;
    assert_eq!(frame.header.ty, FrameType::Error);
    assert_eq!(
        serde_json::from_slice::<Value>(&frame.body).unwrap()["code"],
        "no_plan"
    );
    plan_view_sample("no-plan.json", &frame.body);

    let mut large = Rig::start("synthetic-large-plan", vec![text_step("a", "reply")]).await;
    let mut plan = inspection_plan("large");
    let manifest = Arc::make_mut(plan.manifest.as_mut().unwrap());
    manifest.system_text = "x".repeat(broca_wire::SUBC_MAX_FRAME_BYTES as usize);
    large.send("r1", "hello", Some(plan)).await;
    large.bind_session(1, core()).await;
    assert_eq!(
        large.refused(1, "session.plan_view", json!({})).await,
        "too_large"
    );
    assert_eq!(
        large.result(1, "session.read", json!({"limit":1})).await["messages"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "the oversize refusal preserves the connection"
    );
}

/// A step that asks for the `read` tool, which the scripted tool plane answers.
fn read_step(id: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("call-1".into()),
                    tool_name: "read".into(),
                    input: json!({"path": "notes.md"}).into(),
                    thought_signature: None,
                    provider_state: None,
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

/// `session.request_dump` reaches the store through the serve path on the operator's own route
/// only. It writes each step's request exactly as the provider received it, one file per step
/// named by the step, privately, and each file's SHA-256 is the digest that step recorded. Another
/// caller is refused before anything reads the session; a run the session never attempted, and an
/// option the operation doesn't have, are refused.
#[tokio::test]
async fn request_dump_writes_each_steps_request_for_the_operator_only() {
    let mut rig = Rig::start_attempting(
        "request-dump",
        vec![read_step("a1"), text_step("a2", "done")],
    )
    .await;
    rig.send("r1", "first", Some(compacting_plan())).await;
    rig.assert_store_only();
    let sent: Vec<Vec<u8>> = rig.requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 2, "the run took two steps");
    assert_ne!(sent[0], sent[1], "the second step carries the tool turn");

    rig.bind_session(1, core()).await;
    assert_eq!(
        rig.refused(1, "session.request_dump", json!({"run_id": "r1"}))
            .await,
        "operator_required"
    );

    rig.bind_session(2, Principal::Direct).await;
    let reply = rig
        .result(2, "session.request_dump", json!({"run_id": "r1"}))
        .await;
    assert_eq!(reply["run_id"], "r1", "{reply}");
    let folder = PathBuf::from(reply["folder"].as_str().unwrap());
    assert!(folder.starts_with(&rig.dir), "{reply}");
    let steps = reply["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 2, "{reply}");
    for (step, body) in steps.iter().zip(&sent) {
        assert_eq!(step["outcome"], "written", "{reply}");
        assert_eq!(step["bytes"], body.len(), "{reply}");
        assert_eq!(
            step["recorded_sha256"],
            broca_core::render::request_digest_hex(body),
            "the step recorded the digest of what the provider received"
        );
        let file = folder.join(format!("s{}.json", step["step"]));
        assert_eq!(&std::fs::read(&file).unwrap(), body, "{}", file.display());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&file).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "{}", file.display());
            let mode = std::fs::metadata(&folder).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700, "{}", folder.display());
        }
    }

    assert_eq!(
        rig.refused(2, "session.request_dump", json!({"run_id": "r9"}))
            .await,
        broca_wire::UNKNOWN_RUN
    );
    assert_eq!(
        rig.refused(
            2,
            "session.request_dump",
            json!({"run_id": "r1", "content": true})
        )
        .await,
        "invalid_params"
    );
}

/// A step the provider refused placed no reply, so nothing in the conversation marks where its
/// request ended. `session.request_dump` still writes that request as the provider received it:
/// everything the session held when the step was sent, after an earlier run's tool turn.
#[tokio::test]
async fn request_dump_writes_a_refused_steps_request() {
    let refused = vec![NormalizedEvent::Control(ProviderControl::Error {
        error: broca_protocol::ProviderError {
            provider_details: None,
            cause: None,
            provider_subcode: None,
            refusal: None,
            class: broca_protocol::ErrorClass::Permanent,
            message: "400: invalid request".into(),
            status: Some(400),
            retry_after_secs: None,
            provider_code: Some("request_invalid".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
            account_continue: None,
        },
        retries_used: 0,
    })];
    let mut rig = Rig::start_attempting(
        "request-dump-refused",
        vec![read_step("a1"), text_step("a2", "done"), refused],
    )
    .await;
    rig.send("r1", "first", Some(compacting_plan())).await;
    rig.send("r2", "second", None).await;
    let sent: Vec<Vec<u8>> = rig.requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 3, "the refused run sent one request");

    rig.bind_session(1, Principal::Direct).await;
    let reply = rig
        .result(1, "session.request_dump", json!({"run_id": "r2"}))
        .await;
    let steps = reply["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 1, "{reply}");
    assert_eq!(steps[0]["outcome"], "written", "{reply}");
    assert_eq!(
        steps[0]["recorded_sha256"],
        broca_core::render::request_digest_hex(&sent[2]),
        "{reply}"
    );
    let folder = PathBuf::from(reply["folder"].as_str().unwrap());
    assert_eq!(std::fs::read(folder.join("s1.json")).unwrap(), sent[2]);
}

/// `session.delete` reaches the store through the serve path on the operator's own route only.
/// While the run's actor holds the session open it answers `transient`; once the actor has closed
/// idle, the session is read, and its run's spend, which this rig never exports, refuses the
/// deletion `session_busy`, naming it, with the session kept. Another caller is refused before
/// anything reads the session, as is an option the operation lacks. (The deletion itself is
/// `retention::store`'s, tested there.)
#[tokio::test]
async fn delete_reaches_the_store_for_the_operator_only() {
    let mut rig = Rig::start_attempting("delete", vec![text_step("a1", "done")]).await;
    rig.install_retention();
    rig.send("r1", "first", Some(compacting_plan())).await;
    rig.assert_store_only();

    rig.bind_session(1, core()).await;
    assert_eq!(
        rig.refused(1, "session.delete", json!({})).await,
        "operator_required"
    );
    rig.bind_session(2, Principal::Direct).await;
    assert_eq!(
        rig.refused(2, "session.delete", json!({"all": true})).await,
        "invalid_params"
    );
    let mut held = 0;
    let refusal = loop {
        let reply = rig
            .request(2, json!({"method": "session.delete", "params": {}}))
            .await;
        assert_eq!(reply.header.ty, FrameType::Error);
        let body: Value = serde_json::from_slice(&reply.body).unwrap();
        if body["code"] != "transient" {
            break body;
        }
        held += 1;
        assert!(held < 200, "the actor never closed idle: {body}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(refusal["code"], "session_busy", "{refusal}");
    assert_eq!(
        refusal["message"],
        "the session can't be deleted: a run's spend is not exported yet"
    );
    rig.assert_store_only();
}

/// `session.tag_map` reaches the store through the serve path on the operator's own route only:
/// another caller is refused `operator_required` before anything reads the session. The operator
/// pages the map, a message at a time if asked, and its numbers are the ones the compaction
/// provider's `session.entries` serves. An option the operation doesn't have is refused.
#[tokio::test]
async fn tag_map_reaches_the_store_for_the_operator_only() {
    let mut rig = Rig::start(
        "tag-map",
        vec![text_step("a1", "hello"), text_step("a2", "again")],
    )
    .await;
    rig.send("r1", "first", Some(compacting_plan())).await;
    rig.send(
        "r2",
        "second",
        Some(SendPlan {
            manifest: None,
            ..compacting_plan()
        }),
    )
    .await;
    rig.assert_store_only();

    rig.bind_session(1, core()).await;
    let refused = rig
        .request(1, json!({"method": "session.tag_map", "params": {}}))
        .await;
    assert_eq!(refused.header.ty, FrameType::Error);
    let body: Value = serde_json::from_slice(&refused.body).unwrap();
    assert_eq!(body["code"], "operator_required", "{body}");
    tag_map_sample("refused-not-operator.json", &refused.body);

    rig.bind_session(2, Principal::Direct).await;
    let request = json!({"method": "session.tag_map", "params": {"limit": 2}});
    tag_map_sample("request.json", &serde_json::to_vec(&request).unwrap());
    let first = rig.request(2, request).await;
    assert_eq!(
        first.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&first.body)
    );
    tag_map_sample("reply.json", &first.body);
    let first: Value = serde_json::from_slice::<Value>(&first.body).unwrap()["result"].clone();
    assert_eq!(first["numbered"], true, "{first}");
    assert_eq!(first["next_after_ordinal"], 2, "{first}");
    let last = rig
        .result(
            2,
            "session.tag_map",
            json!({"after_ordinal": 2, "limit": 2}),
        )
        .await;
    assert!(last.get("next_after_ordinal").is_none(), "{last}");
    let rows: Vec<Value> = [&first, &last]
        .iter()
        .flat_map(|page| page["rows"].as_array().unwrap().clone())
        .collect();
    let ordinals: Vec<u64> = rows
        .iter()
        .map(|row| row["ordinal"].as_u64().unwrap())
        .collect();
    assert_eq!(ordinals, [1, 2, 3, 4], "{rows:?}");
    // Ids and numbers only.
    for row in &rows {
        let mut keys: Vec<&str> = row
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, ["mid", "ordinal", "part", "role", "tag"], "{row}");
    }

    let compaction = Some(BTreeMap::from([(
        "compaction-runner".to_owned(),
        "v1".to_owned(),
    )]));
    let bound = rig.bind(3, provider(), compaction).await;
    assert_eq!(bound.header.ty, FrameType::Response);
    let entries = rig
        .answered(
            3,
            "session.entries",
            json!({"select": {"by": "after"}, "detail": "full"}),
        )
        .await;
    let served: Vec<(String, u64, u64)> = entries["entries"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|entry| {
            entry["parts"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|part| {
                    Some((
                        entry["mid"].as_str()?.to_owned(),
                        part["id"].as_str()?.parse().ok()?,
                        part["tag"].as_u64()?,
                    ))
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let mapped: Vec<(String, u64, u64)> = rows
        .iter()
        .map(|row| {
            (
                row["mid"].as_str().unwrap().to_owned(),
                row["part"].as_u64().unwrap(),
                row["tag"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(mapped, served, "the map names session.entries' numbers");

    assert_eq!(
        rig.refused(2, "session.tag_map", json!({"content": true}))
            .await,
        "invalid_params"
    );
}
