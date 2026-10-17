//! The serve rig the delivery-mode tests share: the real serve path and core loop over a real
//! file WAL, with a scripted provider and tool plane a test can hold inside a call.
//!
//! Each test binary that includes this module uses a different part of it, so an item one
//! binary leaves unused is not dead code.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{
    run_loop_on_writer_admitted_with_prior_snapshot, BatchCtx, DispatchResult, FileLeaseStore,
    FileWalStore, OutputEvent, Provider, RunError, RunSummary, StartMode, ToolManifest, ToolPlane,
    ToolPlaneError,
};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{serve_connection, ServeEnd};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, ToolCall, ToolOutput, ToolResult},
    provider::{NormalizedEvent, ProviderControl},
    request::CallOptions,
    BindIdentity, FinishReason, MessageId, StepId, ToolCallId, Usage,
};
use broca_wal::WalRecord;
use subc_protocol::session::ModuleControlRequest;
use subc_protocol::{
    BindIdentity as SubcBindIdentity, Flags, Frame, FrameType, ModuleHelloAckBody, Priority,
    RouteTarget, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::sync::{mpsc, Notify};

/// One scripted model step.
#[derive(Clone)]
pub enum Step {
    /// Canonical provider-reported measurements, including explicit zero or absent input.
    Usage {
        content: Vec<ContentBlock>,
        usage: Usage,
    },
    /// A final answer with no tool calls.
    Text(&'static str),
    /// One call of the `work` tool.
    Call(&'static str),
    /// Sibling calls of the `work` tool in one step, in order.
    Calls(&'static [&'static str]),
    /// A final answer made of these blocks, in order.
    Blocks(Vec<ContentBlock>),
    /// A streamed turn with signed blocks and optional tool calls.
    Stream(Vec<ContentBlock>),
    /// Gemini's normalized steps have empty message ids on both lanes.
    StreamEmpty(Vec<ContentBlock>),
}

impl Step {
    fn events(&self) -> Vec<NormalizedEvent> {
        let call = |id: &str| {
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId(id.into()),
                tool_name: "work".into(),
                input: serde_json::json!({}),
                thought_signature: None,
            })
        };
        let content = match self {
            Step::Text(text) => vec![ContentBlock::Text {
                text: (*text).into(),
            }],
            Step::Call(id) => vec![call(id)],
            Step::Calls(ids) => ids.iter().map(|id| call(id)).collect(),
            Step::Usage {
                content: blocks, ..
            }
            | Step::Blocks(blocks)
            | Step::Stream(blocks)
            | Step::StreamEmpty(blocks) => blocks.clone(),
        };
        let finish_reason = match self {
            Step::Text(_) | Step::Blocks(_) => FinishReason::Stop,
            Step::Usage { .. } | Step::Stream(_) | Step::StreamEmpty(_) => {
                if content
                    .iter()
                    .any(|b| matches!(b, ContentBlock::ToolCall(_)))
                {
                    FinishReason::ToolCalls
                } else {
                    FinishReason::Stop
                }
            }
            Step::Call(_) | Step::Calls(_) => FinishReason::ToolCalls,
        };
        let mut events = Vec::new();
        let message_id = MessageId(
            if matches!(self, Step::StreamEmpty(_)) {
                ""
            } else {
                "m"
            }
            .into(),
        );
        if matches!(self, Step::Stream(_) | Step::StreamEmpty(_)) {
            for block in &content {
                if let ContentBlock::Text { text } = block {
                    events.push(NormalizedEvent::Display(
                        broca_protocol::DisplayEvent::TextDelta {
                            message_id: message_id.clone(),
                            tool_call_id: None,
                            delta: text.clone(),
                        },
                    ));
                }
            }
        }
        events.extend([
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id,
                    content,
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: match self {
                    Step::Usage { usage, .. } => *usage,
                    _ => Usage::default(),
                },
                finish_reason,
                retries_used: 0,
            }),
        ]);
        events
    }
}

/// What the provider and tool plane of the rig do, shared with the test.
#[derive(Default)]
pub struct Script {
    /// Model steps, consumed in order across every run. An exhausted script answers text.
    pub steps: Mutex<VecDeque<Step>>,
    /// The prompt each model step was called with, in call order.
    pub prompts: Mutex<Vec<Vec<Message>>>,
    /// The names of the tools each model step was offered, in call order.
    pub offered_tools: Mutex<Vec<Vec<String>>>,
    /// Model steps wait on this before answering while `hold_model` is set.
    pub hold_model: Mutex<bool>,
    pub model_gate: Notify,
    /// Tool calls wait on this before answering while `hold_tool` is set.
    pub hold_tool: Mutex<bool>,
    pub tool_gate: Notify,
    /// Signalled when a model step or a tool call has started and is holding.
    pub entered: Notify,
    /// How many times each tool call id actually ran.
    pub dispatched: Mutex<Vec<String>>,
    pub dispatched_inputs: Mutex<Vec<serde_json::Value>>,
    pub tool_results: Mutex<std::collections::HashMap<String, DispatchResult>>,
    pub deny_input: Mutex<Option<serde_json::Value>>,
    pub narrow_input: Mutex<Option<serde_json::Value>>,
    pub batch_finishes: Mutex<usize>,
    pub failures: Mutex<Vec<String>>,
    /// Seal tests hold work past the ordinary boundary-stop grace so the actor, rather than
    /// the loop's graceful stop, must collect and persist the hook fallback slots.
    pub ignore_graceful_stop: Mutex<bool>,
    /// When set, the next run pauses for re-authentication right after admission.
    pub pause_next_run: Mutex<bool>,
    /// Optional compaction driver; availability remains off until set_compaction_plane installs it.
    pub compaction_plane: Mutex<Option<Arc<dyn broca_core::compaction::CompactionPlane>>>,
    pub compaction_wired: Mutex<bool>,
    /// When set, each run executes with this auth selection, so a fresh run freezes it into
    /// its `RunStarted` the way a run resolved through the vault does. Unset, runs freeze none.
    pub auth_selection: Mutex<Option<broca_wal::AuthSelection>>,
}

impl Script {
    pub fn push(&self, steps: &[Step]) {
        self.steps.lock().unwrap().extend(steps.iter().cloned());
    }
    pub fn hold_model(&self, hold: bool) {
        *self.hold_model.lock().unwrap() = hold;
        if !hold {
            self.model_gate.notify_waiters();
        }
    }
    pub fn hold_tool(&self, hold: bool) {
        *self.hold_tool.lock().unwrap() = hold;
        if !hold {
            self.tool_gate.notify_waiters();
        }
    }
}

struct ScriptedProvider(Arc<Script>);

#[async_trait]
impl Provider for ScriptedProvider {
    async fn step(&self, _step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.0.prompts.lock().unwrap().push(opts.prompt.clone());
        self.0
            .offered_tools
            .lock()
            .unwrap()
            .push(opts.tools.iter().map(|tool| tool.name.clone()).collect());
        loop {
            let gate = self.0.model_gate.notified();
            if !*self.0.hold_model.lock().unwrap() {
                break;
            }
            self.0.entered.notify_one();
            gate.await;
        }
        let step = self
            .0
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Step::Text("done"));
        let (tx, rx) = mpsc::channel(8);
        for event in step.events() {
            tx.send(event).await.unwrap();
        }
        rx
    }
}

/// The module the rig's tool plane reports every call as dispatched to.
pub const WORK_MODULE: &str = "work-module";

struct ScriptedTools(Arc<Script>);

#[async_trait]
impl ToolPlane for ScriptedTools {
    async fn batch_started(
        &self,
        _ctx: &BatchCtx,
        _calls: &[ToolCall],
        _manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }

    fn dispatch_target(&self, _ctx: &BatchCtx, _tool_call_id: &ToolCallId) -> Option<String> {
        Some(WORK_MODULE.into())
    }

    async fn dispatch(&self, _ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        loop {
            let gate = self.0.tool_gate.notified();
            if !*self.0.hold_tool.lock().unwrap() {
                break;
            }
            self.0.entered.notify_one();
            gate.await;
        }
        self.0
            .dispatched
            .lock()
            .unwrap()
            .push(call.tool_call_id.0.clone());
        self.0
            .dispatched_inputs
            .lock()
            .unwrap()
            .push(call.input.clone());
        if let Some(result) = self
            .0
            .tool_results
            .lock()
            .unwrap()
            .get(&call.tool_call_id.0)
            .cloned()
        {
            return result;
        }
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: "worked".into(),
            },
            is_error: false,
        })
    }

    async fn batch_finished(&self, _ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        *self.0.batch_finishes.lock().unwrap() += 1;
        Ok(())
    }
}

struct ScriptedApproval(Arc<Script>);
#[async_trait]
impl broca_core::ApprovalPolicy for ScriptedApproval {
    async fn decide(&self, call: &ToolCall) -> broca_protocol::approval::ApprovalResponse {
        use broca_protocol::approval::ApprovalResponse;
        if self.0.deny_input.lock().unwrap().as_ref() == Some(&call.input) {
            ApprovalResponse::Deny {
                tool_call_id: call.tool_call_id.clone(),
                reason: "input denied".into(),
            }
        } else if let Some(input) = self.0.narrow_input.lock().unwrap().clone() {
            ApprovalResponse::AllowNarrowed {
                tool_call_id: call.tool_call_id.clone(),
                input,
            }
        } else {
            ApprovalResponse::Allow {
                tool_call_id: call.tool_call_id.clone(),
            }
        }
    }
}

/// The real core loop over the scripted provider and tool plane.
struct CoreExec {
    wal_store: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    script: Arc<Script>,
    hook_plane: Arc<Mutex<Option<Arc<dyn broca_core::HookPlane>>>>,
}

#[async_trait]
impl RunExecutor for CoreExec {
    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::compaction::CompactionPlane>> {
        if *self.script.compaction_wired.lock().unwrap() {
            self.script.compaction_plane.lock().unwrap().clone()
        } else {
            None
        }
    }
    fn hook_plane(&self) -> Option<Arc<dyn broca_core::HookPlane>> {
        self.hook_plane.lock().unwrap().clone()
    }

    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::FreshOrResume => StartMode::FreshOrResume,
        };
        let materialized_store =
            job.materialized_store(self.store.clone() as Arc<dyn broca_core::SessionStore>);
        let mut control = job.control;
        control.hook_plane = self.hook_plane();
        control.compaction_plane = self.script.compaction_plane.lock().unwrap().clone();
        if *self.script.ignore_graceful_stop.lock().unwrap() {
            control.stop_requested = None;
        }
        if let Some(selection) = self.script.auth_selection.lock().unwrap().clone() {
            control.auth_selection = Some(selection);
        }
        if std::mem::take(&mut *self.script.pause_next_run.lock().unwrap()) {
            // The run pauses for re-authentication right after admission, as a run whose
            // credential needs it does.
            control.pause_for_auth_at_start = Some(broca_protocol::ProviderError {
                refusal: None,
                class: broca_protocol::ErrorClass::Auth,
                message: "credential needs re-authentication".into(),
                status: None,
                retry_after_secs: None,
                provider_code: Some("needs_reauth".into()),
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
            });
        }
        run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            &ScriptedProvider(self.script.clone()),
            &ScriptedTools(self.script.clone()),
            &ScriptedApproval(self.script.clone()),
            self.wal_store.as_ref(),
            materialized_store,
            control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
        .inspect_err(|error| {
            self.script.failures.lock().unwrap().push(error.to_string());
        })
    }
}

pub struct Rig {
    pub dir: PathBuf,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    serve: tokio::task::JoinHandle<Result<ServeEnd, broca_module_serve::serve::ServeIoError>>,
    pub session: BindIdentity,
    pub wal_path: PathBuf,
    pub script: Arc<Script>,
    hook_plane: Arc<Mutex<Option<Arc<dyn broca_core::HookPlane>>>>,
    registry: Arc<LineageRegistry>,
    daemon_scopes: bool,
    corr: u64,
}

impl Rig {
    pub async fn start(tag: &str) -> Self {
        Self::start_with_idle_ttl(tag, broca_module::actor::IDLE_TTL).await
    }

    pub async fn start_with_scopes(tag: &str) -> Self {
        Self::start_fresh(tag, broca_module::actor::IDLE_TTL, true).await
    }

    pub async fn start_with_idle_ttl(tag: &str, idle_ttl: Duration) -> Self {
        Self::start_fresh(tag, idle_ttl, false).await
    }

    async fn start_fresh(tag: &str, idle_ttl: Duration, daemon_scopes: bool) -> Self {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-send-delivery-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        Self::open_existing(dir, idle_ttl, daemon_scopes).await
    }

    async fn open_existing(dir: PathBuf, idle_ttl: Duration, daemon_scopes: bool) -> Self {
        Self::open_existing_as(dir, idle_ttl, daemon_scopes, "runner").await
    }

    pub async fn start_as(tag: &str, harness: &str) -> Self {
        // The tag becomes a folder name. Refuse characters Windows forbids in file names, so a
        // Debug-formatted value (`Some("x")`) fails here on every platform, not only on
        // Windows CI.
        assert!(
            !tag.contains(['"', '<', '>', ':', '|', '?', '*', '\\', '/']),
            "rig tag {tag:?} has a character Windows forbids in a folder name"
        );
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-send-delivery-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        Self::open_existing_as(dir, broca_module::actor::IDLE_TTL, false, harness).await
    }

    async fn open_existing_as(
        dir: PathBuf,
        idle_ttl: Duration,
        daemon_scopes: bool,
        harness: &str,
    ) -> Self {
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        let script = Arc::new(Script::default());
        let hook_plane = Arc::new(Mutex::new(None));
        let exec = Arc::new(CoreExec {
            wal_store: wal.clone(),
            store: store.clone(),
            script: script.clone(),
            hook_plane: hook_plane.clone(),
        });
        let opener = Arc::new(
            DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                exec,
            )
            .with_idle_ttl(idle_ttl)
            .with_run_index(store.clone())
            .with_materialized_store(store.clone()),
        );
        let registry = Arc::new(LineageRegistry::new_with_reader(
            opener,
            wal.clone(),
            Some(store),
        ));
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);
        let serve_registry = registry.clone();
        let serve = tokio::spawn(async move {
            serve_connection(
                server_read,
                server_write,
                "broca",
                serve_registry.clone(),
                serve_registry,
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
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: if daemon_scopes {
                    vec![subc_protocol::scope::CAP_SCOPES_V1.into()]
                } else {
                    vec![]
                },
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut writer, &ack).await.unwrap();
        let session = BindIdentity::new(dir.join("proj"), harness, "delivery");
        let wal_path = wal.wal_path(&session);
        let mut rig = Self {
            dir,
            reader,
            writer,
            serve,
            session,
            wal_path,
            script,
            hook_plane,
            registry,
            daemon_scopes,
            corr: 1,
        };
        rig.bind(7).await;
        rig
    }

    pub async fn request(&mut self, channel: u16, body: serde_json::Value) -> Frame {
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
        loop {
            let reply = tokio::time::timeout(Duration::from_secs(20), read_frame(&mut self.reader))
                .await
                .expect("a reply")
                .unwrap()
                .unwrap();
            // A subscription opened by `subscribe` may still stream on its own correlation id;
            // its frames are not this request's reply.
            if reply.header.corr == self.corr {
                return reply;
            }
        }
    }

    /// Send `session.subscribe` with `params` on its own bound channel and collect every frame
    /// the stream sends until none arrives for `quiet`. The first frame of a refusal is its
    /// `ERROR` frame. Later frames of a still-open subscription are skipped by `request`.
    pub async fn subscribe_collect(
        &mut self,
        channel: u16,
        params: serde_json::Value,
        quiet: Duration,
    ) -> Vec<Frame> {
        self.subscribe_collect_after_attach(channel, params, quiet, || {})
            .await
    }

    /// `subscribe_collect`, calling `on_attach` once the subscription's first frame arrives.
    /// Subscribed with `{"from": "start"}`, the control lane replays the lineage's durable
    /// records at attach, so for a run that has started, that first frame proves the subscriber
    /// is attached. A live subscription (`{}`) sends nothing until something new happens, so its
    /// first frame proves nothing and `on_attach` would wait for an event it gates. A test that
    /// releases a held model from `on_attach` cannot stream display deltas before anyone
    /// listens; the display lane is lossy and never replayed.
    pub async fn subscribe_collect_after_attach(
        &mut self,
        channel: u16,
        params: serde_json::Value,
        quiet: Duration,
        on_attach: impl FnOnce(),
    ) -> Vec<Frame> {
        let mut on_attach = Some(on_attach);
        self.bind(channel).await;
        self.corr += 1;
        let corr = self.corr;
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            channel,
            u32::from(channel != 0),
            corr,
            serde_json::to_vec(
                &serde_json::json!({"method": "session.subscribe", "params": params}),
            )
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut self.writer, &frame).await.unwrap();
        let mut frames = Vec::new();
        while let Ok(next) = tokio::time::timeout(quiet, read_frame(&mut self.reader)).await {
            let next = next.unwrap().unwrap();
            if next.header.corr != corr {
                continue;
            }
            let ends = matches!(next.header.ty, FrameType::Error | FrameType::StreamEnd);
            frames.push(next);
            if let Some(attached) = on_attach.take() {
                attached();
            }
            if ends {
                break;
            }
        }
        frames
    }

    pub async fn bind(&mut self, channel: u16) {
        self.bind_as(channel, None).await;
    }

    /// The rig's session registry.
    pub fn registry(&self) -> &Arc<LineageRegistry> {
        &self.registry
    }

    /// Opt in to real hook routing; ordinary delivery tests retain their hookless executor.
    pub fn set_hook_plane(&self, plane: Arc<dyn broca_core::HookPlane>) {
        *self.hook_plane.lock().unwrap() = Some(plane);
    }

    pub fn set_compaction_plane(&self, plane: Arc<dyn broca_core::CompactionPlane>) {
        *self.script.compaction_plane.lock().unwrap() = Some(plane);
        *self.script.compaction_wired.lock().unwrap() = true;
    }

    /// Bind `channel` to another session of the rig's project, `session_id`, and return that
    /// session's identity.
    pub async fn bind_session(&mut self, channel: u16, session_id: &str) -> BindIdentity {
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(
                self.session.project_root.clone(),
                self.session.harness.clone(),
                session_id,
            ),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        let reply = self.request(0, serde_json::to_value(bind).unwrap()).await;
        assert_eq!(reply.header.ty, FrameType::Response);
        BindIdentity::new(
            self.session.project_root.clone(),
            self.session.harness.clone(),
            session_id,
        )
    }

    /// Bind `channel` to the rig's session with the daemon stamping `principal` on it.
    pub async fn bind_as(&mut self, channel: u16, principal: Option<subc_protocol::Principal>) {
        self.bind_with_scope(channel, principal, None).await;
    }

    pub async fn bind_with_scope(
        &mut self,
        channel: u16,
        principal: Option<subc_protocol::Principal>,
        scope: Option<subc_protocol::scope::ScopeStamp>,
    ) {
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(
                self.session.project_root.clone(),
                self.session.harness.clone(),
                "delivery",
            ),
            principal,
            consumer_capabilities: None,
            admission_facts: None,
            scope,
            role_versions: None,
            call_mode: None,
        };
        let reply = self.request(0, serde_json::to_value(bind).unwrap()).await;
        assert_eq!(reply.header.ty, FrameType::Response);
    }

    /// Send `prompt` under `send_id`, with `delivery` when it is `Some`. The send declares the
    /// scripted `work` tool so a scripted call is dispatched rather than refused.
    pub async fn send(&mut self, send_id: &str, prompt: &str, delivery: Option<&str>) -> Frame {
        self.send_with(send_id, prompt, delivery, serde_json::json!({}))
            .await
    }

    /// [`Rig::send`] with extra top-level params merged in (for example `stop_when`).
    pub async fn send_with(
        &mut self,
        send_id: &str,
        prompt: &str,
        delivery: Option<&str>,
        extra: serde_json::Value,
    ) -> Frame {
        let mut params = serde_json::json!({
            "prompt": prompt,
            "send_id": send_id,
            "model": {"provider": "fake", "model": "fake"},
            "tools": [{
                "name": "work",
                "description": "does work",
                "input_schema": {"type": "object"}
            }]
        });
        if let Some(delivery) = delivery {
            params["delivery"] = serde_json::json!(delivery);
        }
        if let Some(extra) = extra.as_object() {
            for (key, value) in extra {
                params[key] = value.clone();
            }
        }
        self.request(
            7,
            serde_json::json!({"method": "session.send", "params": params}),
        )
        .await
    }

    /// Wait until the session's actor reports no run in flight. A paused session is idle.
    pub async fn wait_idle(&mut self) {
        let wal_path = self.wal_path.clone();
        broca_test_support::wait_for(
            "session actor becomes idle",
            async {
                loop {
                    let reply = self
                        .request(7, serde_json::json!({"method": "run.status", "params": {}}))
                        .await;
                    if result(&reply)["state"] == "idle" {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || {
                format!(
                    "WAL bytes={:?}",
                    std::fs::metadata(&wal_path).map(|m| m.len())
                )
            },
        )
        .await
    }

    /// `run.cancel` for `run_id`; returns the reply's result.
    pub async fn cancel(&mut self, run_id: &str) -> serde_json::Value {
        let reply = self
            .request(
                7,
                serde_json::json!({"method": "run.cancel", "params": {"run_id": run_id}}),
            )
            .await;
        result(&reply)
    }

    /// The whole transcript through `session.read`, as `(role, first text or call/result)`.
    pub async fn transcript(&mut self) -> Vec<(String, String)> {
        let reply = self
            .request(
                7,
                serde_json::json!({"method": "session.read", "params": {"limit": 500}}),
            )
            .await;
        let page = result(&reply);
        page["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                let message = &entry["message"];
                let block = &message["content"][0];
                let label = match block["type"].as_str() {
                    Some("text") => block["text"].as_str().unwrap_or_default().to_owned(),
                    Some(other) => other.to_owned(),
                    None => String::new(),
                };
                (
                    message["role"].as_str().unwrap_or_default().to_owned(),
                    label,
                )
            })
            .collect()
    }

    pub fn records(&self) -> Vec<WalRecord> {
        broca_wal::replay_file(&self.wal_path, broca_wal::DEFAULT_MAX_READ_BYTES)
            .unwrap_or_default()
            .into_iter()
            .map(|frame| frame.record)
            .collect()
    }

    pub fn wal_bytes(&self) -> Vec<u8> {
        std::fs::read(&self.wal_path).unwrap_or_default()
    }

    /// Wait until the latest episode's step start is on disk. The provider can reach its hold
    /// before the run's `StepStarted` is written, so without this a WAL snapshot taken at the
    /// hold can still grow by that record.
    async fn wait_latest_step_started(&self) {
        broca_test_support::wait_for(
            "latest episode durable StepStarted",
            async {
                loop {
                    let records = self.records();
                    let last_start = records
                        .iter()
                        .rposition(|record| matches!(record, WalRecord::RunStarted { .. }));
                    if last_start.is_some_and(|start| {
                        records[start..]
                            .iter()
                            .any(|record| matches!(record, WalRecord::StepStarted { .. }))
                    }) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || broca_test_support::record_kinds(self.records()),
        )
        .await
    }

    /// Wait until the WAL holds at least `n` episode terminals.
    pub async fn wait_for_finished(&self, n: usize) -> Vec<WalRecord> {
        broca_test_support::wait_for(
            "durable episode terminals",
            async {
                loop {
                    let records = self.records();
                    let finished = records
                        .iter()
                        .filter(|record| {
                            matches!(
                                record,
                                WalRecord::RunFinished { .. } | WalRecord::RunPaused { .. }
                            )
                        })
                        .count();
                    if finished >= n {
                        return records;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || broca_test_support::record_kinds(self.records()),
        )
        .await
    }

    /// Wait until the provider or the tool plane is holding a call, and the latest episode's step
    /// start is on disk. Tests snapshot the WAL here to prove a later call writes nothing.
    pub async fn wait_entered(&self) {
        broca_test_support::event_timeout(
            "provider or tool reaches held call",
            self.script.entered.notified(),
            || broca_test_support::record_kinds(self.records()),
        )
        .await
        .expect("the run reached the held call");
        self.wait_latest_step_started().await;
    }

    /// Wait for idle lineage actors to release their leases before folding their WALs.
    pub async fn wait_closed(&self) {
        broca_test_support::event_timeout(
            "idle lineage actors release leases",
            async {
                loop {
                    let counts = self.registry.slot_counts();
                    if counts.open + counts.opening + counts.closing == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || {
                format!(
                    "open={}, opening={}, closing={}",
                    self.registry.slot_counts().open,
                    self.registry.slot_counts().opening,
                    self.registry.slot_counts().closing
                )
            },
        )
        .await
        .expect("idle session actors closed");
    }

    /// Gracefully stop at the held tool call's boundary, then reopen without boot resume.
    pub async fn restart_at_tool_boundary(self) -> Self {
        // Polling stop first requests the pause before the held tool is released.
        let (sealed, ()) = tokio::join!(self.registry.stop_and_seal(), async {
            self.script.hold_tool(false)
        });
        assert!(sealed, "the graceful stop sealed the session");
        self.restart().await
    }

    /// Recreate the serve connection, registry and store from the same durable session.
    pub async fn restart(self) -> Self {
        let Self {
            dir,
            reader,
            writer,
            serve,
            registry,
            daemon_scopes,
            session,
            ..
        } = self;
        drop(writer);
        drop(reader);
        let _ = tokio::time::timeout(Duration::from_secs(5), serve).await;
        drop(registry);
        Self::open_existing_as(
            dir,
            Duration::from_millis(100),
            daemon_scopes,
            &session.harness,
        )
        .await
    }

    pub async fn finish(self) {
        let Self {
            dir,
            reader,
            writer,
            serve,
            script,
            ..
        } = self;
        script.hold_model(false);
        script.hold_tool(false);
        drop(writer);
        drop(reader);
        let _ = tokio::time::timeout(Duration::from_secs(5), serve).await;
        let _ = std::fs::remove_dir_all(dir);
    }
}

pub fn golden_path(file: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden")
        .join(file)
}

/// The pinned reply bytes of `file`, with `@RUN_ID@` standing for the run id the reply names.
///
/// A send's run id is derived from its session, whose project root is a per-process scratch
/// directory, so the id itself cannot be pinned; every other byte of the reply is.
pub fn assert_golden(frame: &Frame, file: &str, run_id: &str) {
    let pinned = std::fs::read_to_string(golden_path(file)).unwrap();
    let expected = pinned.trim_end_matches('\n').replace("@RUN_ID@", run_id);
    assert_eq!(
        String::from_utf8_lossy(&frame.body),
        expected,
        "the reply bytes pinned in {file}"
    );
}

/// The bytes the session's baseline adds to the reply of the send that admitted the session (and
/// of every retry of that send): a session admitted without a fetch plan, running the rig's one
/// tool.
pub const ADMITTING_BASELINE: &str = r#","baseline":{"items":[],"tool_names":["work"],"accepted_generation":0,"applied_generation":0}"#;

/// [`assert_golden`] for the reply to the send that admitted the session: the pinned bytes of
/// `file` with exactly [`ADMITTING_BASELINE`] added at the end of `result`.
pub fn assert_golden_admitting(frame: &Frame, file: &str, run_id: &str) {
    let pinned = std::fs::read_to_string(golden_path(file)).unwrap();
    let pinned = pinned.trim_end_matches('\n').replace("@RUN_ID@", run_id);
    let expected = format!(
        "{}{ADMITTING_BASELINE}}}}}",
        pinned
            .strip_suffix("}}")
            .expect("a pinned reply ends its result and body")
    );
    assert_eq!(
        String::from_utf8_lossy(&frame.body),
        expected,
        "the reply bytes pinned in {file}, plus the admitting send's baseline"
    );
}

pub fn result(frame: &Frame) -> serde_json::Value {
    assert_eq!(
        frame.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()["result"].clone()
}
