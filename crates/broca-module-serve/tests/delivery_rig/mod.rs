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

#[path = "../../../broca-wire/tests/support/run_usage.rs"]
mod usage_compat;
use broca_core::WalStore;
use std::sync::atomic::Ordering;
pub use usage_compat::legacy_bytes;

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
            for (part, block) in content.iter().enumerate() {
                let mut event = match block {
                    ContentBlock::Text { text } => broca_protocol::DisplayEvent::TextDelta {
                        mid: None,
                        part: None,
                        message_id: message_id.clone(),
                        tool_call_id: None,
                        delta: text.clone(),
                    },
                    ContentBlock::Reasoning { text, .. } => {
                        broca_protocol::DisplayEvent::ReasoningDelta {
                            mid: None,
                            part: None,
                            message_id: message_id.clone(),
                            delta: text.clone(),
                        }
                    }
                    ContentBlock::ToolCall(call) => broca_protocol::DisplayEvent::ToolInputDelta {
                        mid: None,
                        part: None,
                        tool_call_id: call.tool_call_id.clone(),
                        delta: call.input.to_string(),
                    },
                    _ => continue,
                };
                event.set_part(part as u64);
                events.push(NormalizedEvent::Display(event));
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
    /// Optional real tool transport for serve-path tests; ordinary scripts keep their fake plane.
    pub tool_plane: Mutex<Option<Arc<dyn broca_core::ToolPlane>>>,
    /// A synthetic tool's catalog schema and semantics pin; absent for legacy direct tools.
    pub schema_pin: Mutex<Option<broca_wal::DispatchSchemaPin>>,
    pub attempt_time: Mutex<Option<u64>>,
    pub hold_stream: Mutex<bool>,
    pub stream_gate: Notify,
    /// Model steps, consumed in order across every run. An exhausted script answers text.
    pub steps: Mutex<VecDeque<Step>>,
    /// Family-normalized events take precedence over generated steps so wire fixtures retain
    /// their provider metadata through the real run loop and WAL writer.
    pub normalized_steps: Mutex<VecDeque<Vec<NormalizedEvent>>>,
    pub refusal_fallback_model: Mutex<Option<broca_protocol::ModelHandle>>,
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
    pub provider_output: Mutex<Vec<String>>,
    module_log: Mutex<Option<broca_test_support::LogCapture>>,
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
    /// What the rig's route reports about schema-constrained output, as a catalog row would.
    /// A run admitted with `structured_output` freezes the decision `decide` makes from this
    /// for an xAI OpenAI Chat route, the same decision a resolved provider freezes.
    pub structured_output_support: Mutex<broca_provider::spec::CapabilityFlag>,
    /// Each model step's request as the OpenAI Chat family renders it from the run's frozen
    /// structured-output decision, in call order.
    pub rendered: Mutex<Vec<Vec<u8>>>,
    /// Optional serving-family config for media tests; other scripts keep their legacy bytes.
    pub media_frozen: Mutex<Option<broca_provider::wire::FrozenRenderConfig>>,
}

impl Script {
    pub fn hold_stream(&self, hold: bool) {
        *self.hold_stream.lock().unwrap() = hold;
        if !hold {
            self.stream_gate.notify_waiters();
        }
    }
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

struct ScriptedProvider(
    Arc<Script>,
    Option<broca_protocol::structured_output::FrozenStructuredOutput>,
    std::sync::RwLock<Option<Arc<broca_core::provider::MediaRequestRenderer>>>,
);

impl ScriptedProvider {
    /// The OpenAI Chat render config this provider's requests are rendered with.
    fn frozen(&self) -> broca_provider::wire::FrozenRenderConfig {
        let mut frozen = broca_provider::families::openai_chat_frozen_config("fake");
        frozen.structured_output = self.1.clone();
        frozen
    }
}

#[async_trait]
impl Provider for ScriptedProvider {
    fn supports_media_renderer(&self) -> bool {
        broca_protocol::attachments_enabled() && self.0.media_frozen.lock().unwrap().is_some()
    }

    fn set_media_request_renderer(
        &self,
        renderer: Arc<broca_core::provider::MediaRequestRenderer>,
    ) {
        *self.2.write().unwrap() = Some(renderer);
    }

    fn try_render_request(
        &self,
        opts: &CallOptions,
    ) -> Result<Vec<u8>, Box<broca_protocol::ProviderError>> {
        if let Some(frozen) = self.0.media_frozen.lock().unwrap().as_ref() {
            if let Some(renderer) = self.2.read().unwrap().as_ref() {
                return renderer(&serde_json::to_value(frozen).unwrap(), opts, false, false);
            }
            return Ok(broca_provider::registry::wire_family(frozen.family_id)
                .unwrap()
                .render(frozen, opts));
        }
        Ok(self.render_request(opts))
    }
    fn refusal_fallback_model(&self) -> Option<broca_protocol::ModelHandle> {
        self.0.refusal_fallback_model.lock().unwrap().clone()
    }

    fn refusal_fallback_context_limit(&self) -> Option<u64> {
        self.refusal_fallback_model().map(|_| 1_000_000)
    }

    fn render_config_blob(&self) -> serde_json::Value {
        if let Some(frozen) = self.0.media_frozen.lock().unwrap().as_ref() {
            return serde_json::to_value(frozen).unwrap();
        }
        match &self.1 {
            None => serde_json::json!({ "render_config": "stateless" }),
            Some(frozen) => serde_json::json!({
                "render_config": "stateless",
                "structured_output": frozen,
            }),
        }
    }

    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        use broca_provider::wire::WireFamily;
        self.0.provider_output.lock().unwrap().push(format!(
            "step {} entered; hold_model={}",
            step_id.0,
            *self.0.hold_model.lock().unwrap()
        ));
        let rendered = if let Some(renderer) = self.2.read().unwrap().as_ref() {
            renderer(&self.render_config_blob(), opts, false, true).unwrap()
        } else if let Some(frozen) = self.0.media_frozen.lock().unwrap().as_ref() {
            broca_provider::registry::wire_family(frozen.family_id)
                .unwrap()
                .render(frozen, opts)
        } else {
            broca_provider::families::OpenAiChat.render(&self.frozen(), opts)
        };
        self.0.rendered.lock().unwrap().push(rendered);
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
        let (tx, rx) = mpsc::channel(8);
        let script = self.0.clone();
        let mut events = self
            .0
            .normalized_steps
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| {
                self.0
                    .steps
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(Step::Text("done"))
                    .events()
            });
        if let Some(at) = *self.0.attempt_time.lock().unwrap() {
            events.insert(0, NormalizedEvent::Control(ProviderControl::AttemptFinished {
                attempt: serde_json::from_value(serde_json::json!({"attempt":0,"outcome":"complete","ts_ms":at,
                    "request_body_len":0,"request_body_sha256":"a","response_body_len":0,"response_body_sha256":"b"})).unwrap(),
            }));
        }
        tokio::spawn(async move {
            for event in events {
                if matches!(
                    event,
                    NormalizedEvent::Control(ProviderControl::AssistantMessage { .. })
                ) {
                    loop {
                        let gate = script.stream_gate.notified();
                        if !*script.hold_stream.lock().unwrap() {
                            break;
                        }
                        script.entered.notify_one();
                        gate.await;
                    }
                }
                script
                    .provider_output
                    .lock()
                    .unwrap()
                    .push(format!("step {} output: {event:?}", step_id.0));
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
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
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        let real = self.0.tool_plane.lock().unwrap().clone();
        if let Some(real) = real {
            return real.batch_started(ctx, calls, manifest).await;
        }
        Ok(())
    }

    fn dispatch_target(&self, ctx: &BatchCtx, tool_call_id: &ToolCallId) -> Option<String> {
        if let Some(real) = self.0.tool_plane.lock().unwrap().as_ref() {
            return real.dispatch_target(ctx, tool_call_id);
        }
        Some(WORK_MODULE.into())
    }
    fn dispatch_schema_pin(
        &self,
        ctx: &BatchCtx,
        id: &ToolCallId,
        tool: &str,
    ) -> Option<broca_wal::DispatchSchemaPin> {
        if let Some(real) = self.0.tool_plane.lock().unwrap().as_ref() {
            return real.dispatch_schema_pin(ctx, id, tool);
        }
        self.0.schema_pin.lock().unwrap().clone()
    }

    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        let real = self.0.tool_plane.lock().unwrap().clone();
        if let Some(real) = real {
            return real.dispatch(ctx, call).await;
        }
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

    async fn batch_finished(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        let real = self.0.tool_plane.lock().unwrap().clone();
        if let Some(real) = real {
            return real.batch_finished(ctx).await;
        }
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
    async fn media_profile(
        &self,
        _request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<Option<Box<broca_provider::media_profile::MediaProfile>>, RunError> {
        Ok(self
            .script
            .media_frozen
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|frozen| frozen.media.as_ref())
            .cloned()
            .map(Box::new))
    }
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
        let provider = ScriptedProvider(self.script.clone(), None, std::sync::RwLock::new(None));
        if job.has_media_input() {
            if let Err(error) = job.admit_media(&provider, self.wal_store.uploads().as_ref()) {
                if let Some(ack) = job.on_admitted {
                    let _ = ack.send(Err(error.clone()));
                }
                return Err(error);
            }
        }
        if provider.supports_media_renderer() {
            let store = self
                .wal_store
                .uploads()
                .unwrap()
                .session_media_store(&job.req.session)
                .unwrap();
            let wal = self.wal_store.clone();
            let session = job.req.session.clone();
            let head = job.writer.durable_head();
            let checkpoint = match &job.prior_snapshot.checkpoint_at_head.media {
                Some(broca_core::media_request::DurableMediaSeed::Ready(seed)) => (**seed).clone(),
                Some(broca_core::media_request::DurableMediaSeed::Unreadable(_)) => {
                    panic!("media fixture seed is unreadable")
                }
                None => job
                    .writer
                    .empty_media_checkpoint(job.prior_snapshot.head_seq)
                    .unwrap(),
            };
            let media = broca_provider::media_request::LiveMedia::from_checkpoint(
                store,
                checkpoint,
                job.prior_snapshot
                    .checkpoint_at_head
                    .last_episode
                    .as_ref()
                    .and_then(|episode| episode.config.budget.clone()),
                self.script
                    .media_frozen
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|frozen| frozen.media.clone()),
                Box::new(move |after| {
                    wal.read_records_range(
                        &session,
                        after,
                        Some(head.load(Ordering::Acquire)),
                        None,
                    )
                    .map_err(|error| error.to_string())
                    .and_then(|range| match range.into_records() {
                        broca_wal::WalRangeRecords::Complete(records)
                        | broca_wal::WalRangeRecords::Unknown(records) => Ok(records),
                        _ => Err("incomplete media fixture WAL".into()),
                    })
                }),
            )
            .unwrap();
            provider.set_media_request_renderer(Arc::new(move |frozen, opts, tags, commit| {
                media.render(frozen, opts, tags, commit)
            }));
        }
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
                provider_details: None,
                provider_subcode: None,
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
                affordable_max_tokens: None,
            });
        }
        // A resume keeps the decision its episode froze; a fresh episode decides from its send,
        // as the module's provider resolution does.
        let structured_output = job
            .prior_snapshot
            .checkpoint_at_head
            .last_episode
            .as_ref()
            .filter(|episode| episode.run_id == job.req.run_id)
            .map(|episode| {
                broca_protocol::structured_output::FrozenStructuredOutput::from_frozen_render_config(
                    Some(&episode.config.frozen_render_config),
                )
            })
            .unwrap_or_else(|| {
                job.req.structured_output.as_ref().map(|request| {
                    broca_provider::structured_output::decide(
                        broca_provider::spec::WireFamilyId::OpenAiChat,
                        "xai",
                        None,
                        *self.script.structured_output_support.lock().unwrap(),
                        request,
                    )
                    .unwrap()
                })
            });
        let module_log = self.script.module_log.lock().unwrap().clone();
        let provider = ScriptedProvider(provider.0, structured_output, provider.2);
        let tools = ScriptedTools(self.script.clone());
        let approval = ScriptedApproval(self.script.clone());
        let run = run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            &provider,
            &tools,
            &approval,
            self.wal_store.as_ref(),
            materialized_store,
            control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        );
        let result = if let Some(log) = module_log {
            log.scope_future(run).await
        } else {
            run.await
        };
        result.inspect_err(|error| {
            self.script.failures.lock().unwrap().push(error.to_string());
        })
    }
}

pub struct Rig {
    pub store: Arc<broca_store::SqliteStore>,
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

pub struct FailureDiagnostics {
    script: Arc<Script>,
    wal_path: PathBuf,
}

impl FailureDiagnostics {
    pub fn snapshot(&self) -> String {
        format!(
            "--- broca in-process module log ---\n{:?}\nexecutor failures: {:?}\n--- scripted model provider output ---\n{:?}\nhold_model={}\n--- broca WAL ---\n{:?}",
            self.script.module_log.lock().unwrap().as_ref().map(|log| log.events()),
            self.script.failures.lock().unwrap(),
            self.script.provider_output.lock().unwrap(),
            self.script.hold_model.lock().unwrap(),
            broca_wal::replay_file(&self.wal_path, broca_wal::DEFAULT_MAX_READ_BYTES),
        )
    }
}

impl Drop for FailureDiagnostics {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("{}", self.snapshot());
        }
    }
}

impl Rig {
    /// The module and model provider run in-process, so daemon stdio cannot report their errors.
    pub fn failure_diagnostics(&self) -> FailureDiagnostics {
        *self.script.module_log.lock().unwrap() = Some(broca_test_support::LogCapture::new());
        FailureDiagnostics {
            script: self.script.clone(),
            wal_path: self.wal_path.clone(),
        }
    }

    pub async fn start(tag: &str) -> Self {
        Self::start_with_idle_ttl(tag, broca_module::actor::IDLE_TTL).await
    }

    /// Open a stable test root so two independently built releases can replay the same WAL.
    pub async fn open_fixture(dir: PathBuf) -> Self {
        std::fs::create_dir_all(&dir).unwrap();
        Self::open_existing(dir, Duration::from_millis(100), false).await
    }

    pub fn seed_fixture_wal(&self, bytes: &[u8]) {
        std::fs::create_dir_all(self.wal_path.parent().unwrap()).unwrap();
        std::fs::write(&self.wal_path, bytes).unwrap();
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
            Some(store.clone()),
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
            store,
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
        self.subscribe_collect_inner(channel, params, quiet, None::<fn()>, |_| true)
            .await
    }

    /// Subscribe on a route the test already bound with its caller and scope.
    pub async fn subscribe_collect_bound(
        &mut self,
        channel: u16,
        params: serde_json::Value,
        quiet: Duration,
    ) -> Vec<Frame> {
        self.corr += 1;
        let corr = self.corr;
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            channel,
            1,
            corr,
            serde_json::to_vec(&serde_json::json!({"method":"session.subscribe","params":params}))
                .unwrap(),
        )
        .unwrap();
        write_frame(&mut self.writer, &frame).await.unwrap();
        collect_subscription_frames_at(&mut self.reader, corr, quiet, None::<fn()>, |_| true).await
    }

    /// `subscribe_collect_bound`, but first waiting under the shared hang guard for a frame
    /// matching `until`. Only then does `quiet` start. A `{"from": "start"}` replay can take
    /// longer than any fixed quiet window on a slow runner, so a test that needs a particular
    /// replayed frame must wait for that frame rather than for the stream to go quiet.
    pub async fn subscribe_collect_bound_until(
        &mut self,
        channel: u16,
        params: serde_json::Value,
        quiet: Duration,
        until: impl Fn(&Frame) -> bool,
    ) -> Vec<Frame> {
        self.corr += 1;
        let corr = self.corr;
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            channel,
            1,
            corr,
            serde_json::to_vec(&serde_json::json!({"method":"session.subscribe","params":params}))
                .unwrap(),
        )
        .unwrap();
        write_frame(&mut self.writer, &frame).await.unwrap();
        collect_subscription_frames_at(&mut self.reader, corr, quiet, Some(|| {}), until).await
    }

    /// `subscribe_collect`, calling `on_attach` once the subscription's first frame arrives.
    /// Subscribed with `{"from": "start"}`, the control lane replays the lineage's durable
    /// records at attach, so its first control frame proves control replay has started, not
    /// that the later display snapshot has crossed serve. Display-sensitive callers use
    /// `subscribe_collect_after_display_reset` or `subscribe_release_after_part`. A live
    /// subscription (`{}`) receives its initial display reset even when the session is idle.
    /// Attachment uses the shared hang guard; `quiet` applies only after replay arrives.
    pub async fn subscribe_collect_after_attach(
        &mut self,
        channel: u16,
        params: serde_json::Value,
        quiet: Duration,
        on_attach: impl FnOnce(),
    ) -> Vec<Frame> {
        self.subscribe_collect_inner(channel, params, quiet, Some(on_attach), |_| true)
            .await
    }

    /// Wait for the display snapshot barrier, not merely a control replay frame, before
    /// releasing a provider. Subsequent original deltas and hook resets cannot be lost
    /// before the display receiver exists.
    pub async fn subscribe_collect_after_display_reset(
        &mut self,
        channel: u16,
        params: serde_json::Value,
        quiet: Duration,
        on_attach: impl FnOnce(),
    ) -> Vec<Frame> {
        self.subscribe_collect_inner(channel, params, quiet, Some(on_attach), |frame| {
            serde_json::from_slice::<serde_json::Value>(&frame.body).is_ok_and(|body| {
                body["kind"] == "display" && body["event"]["type"] == "display_reset"
            })
        })
        .await
    }

    async fn subscribe_collect_inner(
        &mut self,
        channel: u16,
        params: serde_json::Value,
        quiet: Duration,
        on_attach: Option<impl FnOnce()>,
        attach_condition: fn(&Frame) -> bool,
    ) -> Vec<Frame> {
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
        collect_subscription_frames_at(&mut self.reader, corr, quiet, on_attach, attach_condition)
            .await
    }

    /// Release a held provider only after the requested display part actually crossed serve,
    /// then collect through the run's `run_finished` control frame. A fixed quiet window alone
    /// can close between two steps on a slow runner, dropping the rest of the run.
    pub async fn subscribe_release_after_part(&mut self, channel: u16, part: u64) -> Vec<Frame> {
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
                &serde_json::json!({"method":"session.subscribe","params":{"from":"start"}}),
            )
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut self.writer, &frame).await.unwrap();
        let expected_part = part.to_string();
        let mut frames = broca_test_support::wait_for(
            "held display snapshot",
            async {
                let mut frames = Vec::new();
                loop {
                    let frame = read_frame(&mut self.reader)
                        .await
                        .unwrap()
                        .expect("stream stays open");
                    if frame.header.corr != corr {
                        continue;
                    }
                    let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
                    let reached = body["kind"] == "display"
                        && body["event"]["part"].as_str() == Some(expected_part.as_str());
                    frames.push(frame);
                    if reached {
                        break frames;
                    }
                }
            },
            || "waiting for the coalesced snapshot part before releasing its provider".into(),
        )
        .await;
        self.script.hold_stream(false);
        frames.extend(
            collect_subscription_frames_at(
                &mut self.reader,
                corr,
                Duration::from_millis(300),
                Some(|| {}),
                |frame| {
                    serde_json::from_slice::<serde_json::Value>(&frame.body).is_ok_and(|body| {
                        body["kind"] == "control" && body["unit"]["type"] == "run_finished"
                    })
                },
            )
            .await,
        );
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
        script.hold_stream(false);
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
    let actual = if file.starts_with("run_status.") {
        legacy_bytes(&frame.body, expected.as_bytes())
    } else {
        frame.body.to_vec()
    };
    assert_eq!(
        String::from_utf8_lossy(&actual),
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

async fn collect_subscription_frames(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
    corr: u64,
    quiet: Duration,
    on_attach: Option<impl FnOnce()>,
) -> Vec<Frame> {
    collect_subscription_frames_at(reader, corr, quiet, on_attach, |_| true).await
}

async fn collect_subscription_frames_at(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
    corr: u64,
    quiet: Duration,
    on_attach: Option<impl FnOnce()>,
    attach_condition: impl Fn(&Frame) -> bool,
) -> Vec<Frame> {
    let mut frames = Vec::new();
    if let Some(attached) = on_attach {
        // A slow first replay is not a quiet subscription: the caller may still hold the model.
        let first = broca_test_support::wait_for(
            "subscription attachment replay",
            async {
                let mut prefix = Vec::new();
                loop {
                    let next = read_frame(reader)
                        .await
                        .unwrap()
                        .expect("subscription stays open");
                    if next.header.corr == corr {
                        let attached =
                            next.header.ty != FrameType::StreamData || attach_condition(&next);
                        prefix.push(next);
                        if attached {
                            return prefix;
                        }
                    }
                }
            },
            || format!("waiting for first replay on correlation {corr}"),
        )
        .await;
        assert_eq!(
            first.last().unwrap().header.ty,
            FrameType::StreamData,
            "subscription did not attach: {first:?}"
        );
        frames.extend(first);
        attached();
    }
    while let Ok(next) = tokio::time::timeout(quiet, read_frame(reader)).await {
        let next = next.unwrap().unwrap();
        if next.header.corr != corr {
            continue;
        }
        let ends = matches!(next.header.ty, FrameType::Error | FrameType::StreamEnd);
        frames.push(next);
        if ends {
            break;
        }
    }
    frames
}

#[cfg(test)]
mod subscription_tests {
    use super::*;
    use std::future::Future;
    use std::task::Poll;

    #[tokio::test]
    async fn failure_dump_includes_module_log_and_scripted_provider_output() {
        const CHILD: &str = "BROCA_DELIVERY_DIAGNOSTICS_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let mut rig = Rig::start("failure-dump-evidence").await;
            println!("RIG_DIR={}", rig.dir.display());
            let _diagnostics = rig.failure_diagnostics();
            rig.script
                .module_log
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .in_scope(|| {
                    tracing::error!("BROCA_MODULE_LOG_MARKER");
                });
            rig.script
                .push(&[Step::Text("SCRIPTED_PROVIDER_OUTPUT_MARKER")]);
            result(&rig.send("dump-evidence", "hello", None).await);
            rig.wait_for_finished(1).await;
            panic!("intentional failure to verify in-process diagnostics");
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "delivery_rig::subscription_tests::failure_dump_includes_module_log_and_scripted_provider_output",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(!output.status.success(), "child must fail deliberately");
        let stderr = String::from_utf8_lossy(&output.stderr);
        for marker in [
            "BROCA_MODULE_LOG_MARKER",
            "SCRIPTED_PROVIDER_OUTPUT_MARKER",
            "RunFinished",
        ] {
            assert!(stderr.contains(marker), "missing {marker}: {stderr}");
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        // libtest prints `test <name> ... ` without a newline before the child's own output, so
        // the marker can sit mid-line rather than at its start.
        let dir = stdout
            .lines()
            .find_map(|line| line.split_once("RIG_DIR=").map(|(_, dir)| dir.trim()))
            .unwrap_or_else(|| panic!("child printed no RIG_DIR= line: {stdout}"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn display_barrier_keeps_control_prefix_and_releases_only_on_display_reset() {
        let script = Arc::new(Script::default());
        script.hold_model(true);
        let held = script.clone();
        let (mut reader, mut writer) = tokio::io::duplex(4096);
        let quiet = Duration::from_millis(500);
        let collect = collect_subscription_frames_at(
            &mut reader,
            9,
            quiet,
            Some(move || held.hold_model(false)),
            |frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body).is_ok_and(|body| {
                    body["kind"] == "display" && body["event"]["type"] == "display_reset"
                })
            },
        );
        tokio::pin!(collect);
        let control = Frame::build(
            FrameType::StreamData,
            Flags::new(false, Priority::Interactive, false),
            8,
            8,
            9,
            br#"{"kind":"control","event":{"type":"display_reset"}}"#.to_vec(),
        )
        .unwrap();
        write_frame(&mut writer, &control).await.unwrap();
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(collect.as_mut().poll(cx).is_pending())).await
        );
        assert!(
            *script.hold_model.lock().unwrap(),
            "control replay is not the display barrier"
        );
        let reset = Frame::build(
            FrameType::StreamData,
            Flags::new(false, Priority::Background, false),
            8,
            8,
            9,
            br#"{"kind":"display","event":{"type":"display_reset"}}"#.to_vec(),
        )
        .unwrap();
        write_frame(&mut writer, &reset).await.unwrap();
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(collect.as_mut().poll(cx).is_pending())).await
        );
        assert!(!*script.hold_model.lock().unwrap());
        tokio::time::advance(quiet).await;
        let frames = collect.await;
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].body, control.body);
        assert_eq!(frames[1].body, reset.body);
    }

    #[tokio::test(start_paused = true)]
    async fn attach_waits_for_first_replay_before_starting_quiet_window() {
        let script = Arc::new(Script::default());
        script.hold_model(true);
        let held = script.clone();
        let (mut reader, mut writer) = tokio::io::duplex(4096);
        let quiet = Duration::from_millis(500);
        let collect = collect_subscription_frames(
            &mut reader,
            9,
            quiet,
            Some(move || held.hold_model(false)),
        );
        tokio::pin!(collect);
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(collect.as_mut().poll(cx).is_pending())).await
        );

        // No replay can arrive until this test writes it; only the virtual clock advances.
        tokio::time::advance(quiet * 2).await;
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(collect.as_mut().poll(cx).is_pending())).await,
            "quiet collection ended before attachment; hold_model={}",
            script.hold_model.lock().unwrap()
        );
        assert!(*script.hold_model.lock().unwrap());
        let replay = Frame::build(
            FrameType::StreamData,
            Flags::new(false, Priority::Interactive, false),
            8,
            8,
            9,
            b"replayed StepStarted".to_vec(),
        )
        .unwrap();
        write_frame(&mut writer, &replay).await.unwrap();
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(collect.as_mut().poll(cx).is_pending())).await
        );
        assert!(
            !*script.hold_model.lock().unwrap(),
            "replay must release the held model"
        );
        tokio::time::advance(quiet).await;
        assert_eq!(
            collect.await.len(),
            1,
            "quiet collection still ends after attachment"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn collection_without_attach_can_end_without_frames() {
        let (mut reader, _writer) = tokio::io::duplex(4096);
        assert!(collect_subscription_frames(
            &mut reader,
            9,
            Duration::from_millis(500),
            None::<fn()>,
        )
        .await
        .is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn missing_attachment_replay_hits_shared_hang_guard() {
        let (mut reader, _writer) = tokio::io::duplex(4096);
        let collection = tokio::spawn(async move {
            collect_subscription_frames(
                &mut reader,
                9,
                Duration::from_millis(500),
                Some(|| panic!("model must not be released without replay")),
            )
            .await
        });
        let error = collection
            .await
            .expect_err("missing replay must trip the hang guard");
        let panic = error.into_panic();
        let message = panic.downcast_ref::<String>().unwrap();
        assert!(
            message.contains("timed out waiting for subscription attachment replay"),
            "{message}"
        );
    }
}
