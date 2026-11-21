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
    BatchCtx, DispatchResult, FileLeaseStore, OutputEvent, Provider, RunError, ToolManifest,
    ToolPlane, ToolPlaneError,
};
use broca_module::executor::RunExecutor;
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{serve_connection, ServeEnd};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, ToolCall, ToolOutput, ToolResult},
    provider::{NormalizedEvent, ProviderControl},
    request::CallOptions,
    BindIdentity, FinishReason, MessageId, StepId, ToolCallId, Usage,
};
use broca_session_store::record::Record;
use subc_protocol::session::ModuleControlRequest;
use subc_protocol::{
    BindIdentity as SubcBindIdentity, Flags, Frame, FrameType, ModuleHelloAckBody, Priority,
    RouteTarget, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::sync::{mpsc, Notify};

#[path = "../../../broca-wire/tests/support/run_usage.rs"]
mod usage_compat;
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
                input: serde_json::json!({}).into(),
                thought_signature: None,
                provider_state: None,
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
                        delta: text.to_string(),
                    },
                    ContentBlock::Reasoning { text, .. } => {
                        broca_protocol::DisplayEvent::ReasoningDelta {
                            mid: None,
                            part: None,
                            message_id: message_id.clone(),
                            delta: text.to_string(),
                        }
                    }
                    ContentBlock::ToolCall(call) => broca_protocol::DisplayEvent::ToolInputDelta {
                        mid: None,
                        part: None,
                        tool_call_id: call.tool_call_id.clone(),
                        tool_name: Some(call.tool_name.clone()),
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
    pub omit_media_profile: Mutex<bool>,
    /// The cache policy each fresh run was admitted with, in start order.
    pub run_caches: Mutex<Vec<RunCache>>,
}

/// The prompt-cache policy the store actor admitted one fresh run with.
#[derive(Clone, Debug, PartialEq)]
pub struct RunCache {
    /// The policy the run resolves its lifetime and key from.
    pub cache: Option<cortexkit_role_llm_runner::cache::Cache>,
    /// The warming the run froze.
    pub keep_warm: Option<broca_protocol::request::KeepWarm>,
    /// Whether the run takes the previous run's frozen policy instead of `cache`.
    pub inherit: bool,
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
        self.0.media_frozen.lock().unwrap().is_some()
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
                return renderer(
                    &serde_json::to_value(frozen).unwrap(),
                    opts,
                    false,
                    false,
                    None,
                );
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
            let mut frozen = frozen.clone();
            if *self.0.omit_media_profile.lock().unwrap() {
                frozen.media = None;
            }
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
            renderer(&self.render_config_blob(), opts, false, true, None).unwrap()
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
        let events = self
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
            .push(call.input.clone().parse());
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
        if self.0.deny_input.lock().unwrap().as_ref() == Some(&(call.input).parse()) {
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
            // The default scripted model is known to be text-only, not an unavailable catalog row.
            .or_else(|| {
                Some(broca_provider::media_profile::select_media_profile(
                    broca_provider::spec::WireFamilyId::OpenAiChat,
                    None,
                    "fake",
                    broca_provider::spec::InputModalities::from_value(&serde_json::json!(["text"])),
                    None,
                ))
            })
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

    /// A session kept in the session store (one a move took there) runs through the new loop over
    /// the same scripted provider, tools and approvals today's runs use.
    async fn execute_on_store(
        &self,
        job: broca_module::executor::StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> broca_module::executor::StoreRunEnd {
        use broca_module::executor::{StoreRunError, StoreRunMode};
        use broca_session_loop::writer::Writer as _;
        let broca_module::executor::StoreRunJob {
            mut req,
            mut control,
            mut writer,
            mut parts,
            mode,
            media,
            ..
        } = job;
        control.hook_plane = self.hook_plane();
        if matches!(mode, StoreRunMode::Fresh) {
            self.script.run_caches.lock().unwrap().push(RunCache {
                cache: req.cache.clone(),
                keep_warm: req.keep_warm.clone(),
                inherit: req.inherit_frozen_cache,
            });
        }
        if let Some(selection) = self.script.auth_selection.lock().unwrap().clone() {
            control.auth_selection = Some(selection);
        }
        if std::mem::take(&mut *self.script.pause_next_run.lock().unwrap()) {
            // The run pauses for re-authentication right after admission, as a run whose
            // credential needs it does.
            control.pause_for_auth_at_start = Some(broca_protocol::ProviderError {
                provider_details: None,
                cause: None,
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
                account_continue: None,
            });
        }
        // A fresh run decides structured output from its send, as the module's provider
        // resolution does; any other run keeps the decision its start froze in its render config.
        let structured_output = if matches!(mode, StoreRunMode::Fresh) {
            req.structured_output.as_ref().map(|request| {
                broca_provider::structured_output::decide(
                    broca_provider::spec::WireFamilyId::OpenAiChat,
                    "xai",
                    None,
                    *self.script.structured_output_support.lock().unwrap(),
                    request,
                )
                .unwrap()
            })
        } else {
            writer
                .operations()
                .run
                .as_ref()
                .and_then(|run| broca_session_loop::run::run_config_of_run(run, parts.as_ref()).ok())
                .and_then(|config| {
                    broca_protocol::structured_output::FrozenStructuredOutput::from_frozen_render_config(
                        Some(&config.frozen_render_config),
                    )
                })
        };
        let provider = ScriptedProvider(
            self.script.clone(),
            structured_output,
            std::sync::RwLock::new(None),
        );
        let tools = ScriptedTools(self.script.clone());
        let approval = ScriptedApproval(self.script.clone());
        let result = async {
            // The run's clock as the module's own runs read it: the session's retention clock,
            // else the wall clock. A restart pause is resumed only while it is recent, by the
            // same clock.
            let clock = control.clock.clone();
            let now = || {
                clock.as_ref().map_or_else(
                    || {
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |elapsed| elapsed.as_millis() as u64)
                    },
                    |clock| clock.now_ms(),
                )
            };
            // As the module's executor installs it for a store run that may carry media, when
            // the scripted model takes media: a renderer reading the session's own media
            // objects through its actor's image index, which each model call waits for until the
            // index is built (`StoreRunMediaWait`).
            let holds_media = req
                .input
                .iter()
                .chain(
                    writer
                        .conversation()
                        .held_messages()
                        .unwrap()
                        .iter()
                        .map(|message| &**message),
                )
                .any(|message| !broca_session_store::session::message_media(message).is_empty());
            let frozen = self.script.media_frozen.lock().unwrap().clone();
            let mut model_call_ready = None;
            if frozen.is_some()
                && !matches!(mode, StoreRunMode::Seal(_))
                && (holds_media || !req.tools.is_empty())
            {
                let media = media.ok_or_else(|| {
                    StoreRunError::Admission(broca_core::RunError::Wal(
                        "store_media_unavailable: the run was started without its media index"
                            .into(),
                    ))
                })?;
                let lazy = broca_module::store_media::LazyRenderer::new(
                    media,
                    Box::new(|media| Ok(media.renderer())),
                );
                provider.set_media_request_renderer(lazy.renderer());
                model_call_ready = Some(lazy as Arc<dyn broca_session_loop::run::ModelCallReady>);
            }
            let seams = broca_session_loop::run::Seams {
                person_steers: None,
                provider: &provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                // As the module's own runs do: a graceful stop ends the run at its next boundary.
                stop: control.stop_requested.clone().unwrap_or_default(),
                held: None,
                approval: &approval,
                hook_plane: control.hook_plane.clone(),
                now_ms: &now,
                session_address: None,
                compaction_plane: control.compaction_plane.as_deref(),
                compaction_session: control.compaction_session.clone(),
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: control.auth_selection.clone(),
                step_progress: control.step_progress.clone(),
                model_call_ready,
                pause_for_auth_at_start: control.pause_for_auth_at_start.clone(),
                account_continue: None,
            };
            match mode {
                StoreRunMode::Fresh => {
                    let admission = broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &provider,
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
                StoreRunMode::Resume | StoreRunMode::RecheckSignIn | StoreRunMode::ProbeSignIn => {
                    broca_session_loop::run::resume(&mut writer, parts.as_ref(), &seams).await
                }
            }
            .map_err(StoreRunError::Loop)
        }
        .await;
        if let Err(error) = &result {
            self.script
                .failures
                .lock()
                .unwrap()
                .push(format!("{error:?}"));
        }
        broca_module::executor::StoreRunEnd { parts, result }
    }
}

pub struct Rig {
    pub store: Arc<broca_store::SqliteStore>,
    pub dir: PathBuf,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    serve: tokio::task::JoinHandle<Result<ServeEnd, broca_module_serve::serve::ServeIoError>>,
    pub session: BindIdentity,
    pub script: Arc<Script>,
    hook_plane: Arc<Mutex<Option<Arc<dyn broca_core::HookPlane>>>>,
    registry: Arc<LineageRegistry>,
    daemon_scopes: bool,
    corr: u64,
    next_subscription_read_delay: Option<Duration>,
    /// Frames [`Rig::request`] read that weren't its reply, such as a still-open
    /// subscription's, in the order they arrived.
    pub skipped: Vec<Frame>,
}

pub struct FailureDiagnostics {
    script: Arc<Script>,
    store: Option<Arc<broca_module::store_open::StoreRuntime>>,
    session: BindIdentity,
}

impl FailureDiagnostics {
    pub fn snapshot(&self) -> String {
        format!(
            "--- broca in-process module log ---\n{:?}\nexecutor failures: {:?}\n--- scripted model provider output ---\n{:?}\nhold_model={}\n--- broca stored records ---\n{}",
            self.script.module_log.lock().unwrap().as_ref().map(|log| log.events()),
            self.script.failures.lock().unwrap(),
            self.script.provider_output.lock().unwrap(),
            self.script.hold_model.lock().unwrap(),
            self.stored_records(),
        )
    }

    /// The session's open log, read on a thread of its own: the snapshot is taken while the
    /// failing test unwinds on the runtime that drives the rig, so that runtime can't serve the
    /// read. A read that can't finish in time is reported, never waited out.
    fn stored_records(&self) -> String {
        let Some(store) = self.store.clone() else {
            return "the rig keeps no session store".into();
        };
        let session = self.session.clone();
        let (sent, read) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let records = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime.block_on(async {
                    match store.current_view(&session).await {
                        Ok(Some(view)) => format!("{:?}", view.open_log_records().await),
                        Ok(None) => "the session has no stored records".into(),
                        Err(error) => format!("the session can't be read: {error:?}"),
                    }
                }),
                Err(error) => format!("no runtime to read the session on: {error}"),
            };
            let _ = sent.send(records);
        });
        read.recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|_| "the session's records did not read within 10 s".into())
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
            store: self.registry.store_runtime(),
            session: self.session.clone(),
        }
    }

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
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        broca_store::session_refs::migrate(
            &dir.join("store.db"),
            &broca_store::session_refs::StoredSessions::new(&dir),
        )
        .unwrap();
        let script = Arc::new(Script::default());
        let hook_plane = Arc::new(Mutex::new(None));
        let exec = Arc::new(CoreExec {
            store: store.clone(),
            script: script.clone(),
            hook_plane: hook_plane.clone(),
        });
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            exec.clone(),
        )
        .with_idle_ttl(idle_ttl);
        let runtime = Arc::new(
            broca_module::store_open::StoreRuntime::open(
                &dir,
                broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
                exec.clone() as Arc<dyn RunExecutor>,
                idle_ttl,
            )
            .await
            .unwrap()
            .with_billing(broca_module::store_billing::StoreBilling {
                index: store.clone() as Arc<dyn broca_core::ExportIndex>,
                resolver: Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
            }),
        );
        // Every session is kept in the session store. It writes usage exports the way the module
        // does: one fact per finished run, into the run index's export table.
        let opener = opener
            .with_store_runtime(runtime.clone())
            .with_charge_basis_resolver(Arc::new(|_: &str, _: &str| {
                (broca_core::ChargeBasis::Unknown, 1)
            }));
        let opener = Arc::new(opener);
        let registry = Arc::new(LineageRegistry::new(opener));
        registry.set_session_address_inventory(store.clone());
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
        // Delivery fixtures use an already-recorded directory-root session address; record it explicitly.
        broca_core::SessionRefIndex::record_session(
            store.as_ref(),
            &broca_core::session_addr(&session),
            &session,
        )
        .unwrap();
        let mut rig = Self {
            store,
            dir,
            reader,
            writer,
            serve,
            session,
            script,
            hook_plane,
            registry,
            daemon_scopes,
            corr: 1,
            next_subscription_read_delay: None,
            skipped: Vec::new(),
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
            self.skipped.push(reply);
        }
    }

    /// Opens `session.subscribe` with `params` on `channel` and returns its correlation id
    /// without waiting for any of its frames: they arrive among [`Rig::skipped`] as later
    /// requests read past them.
    pub async fn subscribe_open(&mut self, channel: u16, params: serde_json::Value) -> u64 {
        self.request_open(
            channel,
            serde_json::json!({"method":"session.subscribe","params":params}),
        )
        .await
    }

    /// Writes the request `body` on `channel` and returns its correlation id without waiting
    /// for its reply, which [`Rig::reply_to`] reads.
    pub async fn request_open(&mut self, channel: u16, body: serde_json::Value) -> u64 {
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
        self.corr
    }

    /// The reply to the request [`Rig::request_open`] wrote as `corr`.
    pub async fn reply_to(&mut self, corr: u64) -> Frame {
        if let Some(at) = self
            .skipped
            .iter()
            .position(|frame| frame.header.corr == corr)
        {
            return self.skipped.remove(at);
        }
        loop {
            let reply = tokio::time::timeout(Duration::from_secs(20), read_frame(&mut self.reader))
                .await
                .expect("a reply")
                .unwrap()
                .unwrap();
            if reply.header.corr == corr {
                return reply;
            }
            self.skipped.push(reply);
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

    /// `subscribe_collect`, but first waiting under the shared hang guard for a frame matching
    /// `until`; only then does `quiet` start. A live display delta can reach a loaded runner
    /// after any fixed quiet window, so a test that needs a particular live frame must wait for
    /// that frame rather than for the stream to go quiet.
    pub async fn subscribe_collect_until(
        &mut self,
        channel: u16,
        params: serde_json::Value,
        quiet: Duration,
        until: fn(&Frame) -> bool,
    ) -> Vec<Frame> {
        self.subscribe_collect_inner(channel, params, quiet, Some(|| {}), until)
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

    /// Delay receipt, not production, of the next subscription's replay. This lets a
    /// regression exercise a slow transport without relying on runner contention.
    pub fn delay_next_subscription_reads(&mut self, delay: Duration) {
        self.next_subscription_read_delay = Some(delay);
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
        if let Some(delay) = self.next_subscription_read_delay.take() {
            let mut reader = DelayedSubscriptionReader {
                reader: &mut self.reader,
                delay: Box::pin(tokio::time::sleep(delay)),
            };
            return collect_subscription_frames_at(
                &mut reader,
                corr,
                quiet,
                on_attach,
                attach_condition,
            )
            .await;
        }
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

    pub async fn bind_compaction(&mut self, channel: u16, provider: &str) {
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(self.session.harness.clone(), "delivery"),
            principal: Some(subc_protocol::Principal::Reserved {
                module_id: provider.into(),
            }),
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: Some(std::collections::BTreeMap::from([(
                "compaction-runner".into(),
                "v1".into(),
            )])),
            call_mode: None,
        };
        let reply = self.request(0, serde_json::to_value(bind).unwrap()).await;
        assert_eq!(reply.header.ty, FrameType::Response, "{reply:?}");
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
        let session = BindIdentity::new(
            &self.session.project_root,
            &self.session.harness,
            session_id,
        );
        broca_core::SessionRefIndex::record_session(
            self.store.as_ref(),
            &broca_core::session_addr(&session),
            &session,
        )
        .unwrap();
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(self.session.harness.clone(), session_id),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        let reply = self.request(0, serde_json::to_value(bind).unwrap()).await;
        assert_eq!(reply.header.ty, FrameType::Response);
        session
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
            identity: SubcBindIdentity::new(self.session.harness.clone(), "delivery"),
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
        // New keys inherit the frozen surface; retries of the creating key repeat its
        // original declaration so the request identity stays identical after a restart too.
        // Explicit overrides below still exercise forbidden later declarations.
        let records = self.records().await;
        if records
            .iter()
            .find_map(|record| match record {
                Record::RunStart(start) => Some(start.send_id.as_deref()),
                _ => None,
            })
            .is_some_and(|first| first != Some(send_id))
        {
            params.as_object_mut().unwrap().remove("tools");
        }
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
            || "run.status never read idle".to_owned(),
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

    /// The session's records in the session store, in log order: what its runs and sends wrote.
    /// Empty while the session doesn't exist.
    /// Whether the rig's session has a log in the store.
    pub async fn session_stored(&self) -> bool {
        self.registry
            .store_runtime()
            .expect("the rig keeps its sessions in the new storage")
            .current_view(&self.session)
            .await
            .is_ok_and(|view| view.is_some())
    }

    pub async fn records(&self) -> Vec<Record> {
        let runtime = self
            .registry
            .store_runtime()
            .expect("the rig keeps its sessions in the new storage");
        match runtime.current_view(&self.session).await {
            Ok(Some(view)) => view
                .open_log_records()
                .await
                .expect("the session's log reads")
                .into_iter()
                .map(|(_, record)| record)
                .collect(),
            _ => Vec::new(),
        }
    }

    /// The frozen config of each run the session started, in the order the runs started, read
    /// back from each run's start record and the setup parts it names.
    pub async fn run_configs(&self) -> Vec<broca_wal::RunConfig> {
        self.run_configs_by_id()
            .await
            .into_iter()
            .map(|(_, config)| config)
            .collect()
    }

    /// The frozen config of the run `run_id` started with.
    pub async fn run_config_of(&self, run_id: &str) -> Option<broca_wal::RunConfig> {
        self.run_configs_by_id()
            .await
            .into_iter()
            .find_map(|(id, config)| (id == run_id).then_some(config))
    }

    /// Each run's id and frozen config, in the order the runs started.
    async fn run_configs_by_id(&self) -> Vec<(String, broca_wal::RunConfig)> {
        let runtime = self
            .registry
            .store_runtime()
            .expect("the rig keeps its sessions in the new storage");
        let class = runtime
            .class_of(&self.session)
            .unwrap()
            .expect("the session exists");
        let parts = runtime.parts_for(class, runtime.new_sessions());
        self.records()
            .await
            .iter()
            .filter_map(|record| match record {
                Record::RunStart(start) => Some((
                    start.run_id.clone(),
                    broca_session_loop::run::run_config_of_start(start, parts)
                        .expect("a run's start reads back its config"),
                )),
                _ => None,
            })
            .collect()
    }

    /// What each run the session started was admitted with (its model, input, tools, frozen
    /// plan manifest and the rest), in the order the runs started, read back from each run's
    /// start record and the setup parts it names.
    pub async fn admissions(&self) -> Vec<broca_session_loop::run::Admission> {
        let runtime = self
            .registry
            .store_runtime()
            .expect("the rig keeps its sessions in the new storage");
        let class = runtime
            .class_of(&self.session)
            .unwrap()
            .expect("the session exists");
        let parts = runtime.parts_for(class, runtime.new_sessions());
        self.records()
            .await
            .iter()
            .filter_map(|record| match record {
                Record::RunStart(start) => Some(
                    broca_session_loop::run::admission_of_start(start, parts)
                        .expect("a run's start reads back its admission"),
                ),
                _ => None,
            })
            .collect()
    }

    /// The request of each send the session queued, in the order they were queued, read back
    /// from each queued send's record and the setup parts it names.
    pub async fn queued_requests(&self) -> Vec<broca_protocol::request::SubmissionRequest> {
        let runtime = self
            .registry
            .store_runtime()
            .expect("the rig keeps its sessions in the new storage");
        let class = runtime
            .class_of(&self.session)
            .unwrap()
            .expect("the session exists");
        let parts = runtime.parts_for(class, runtime.new_sessions());
        self.records()
            .await
            .iter()
            .filter_map(|record| match record {
                Record::QueuedSend(send) => Some(
                    broca_session_loop::queue::request_of(send, parts)
                        .expect("a queued send reads back its request"),
                ),
                _ => None,
            })
            .collect()
    }

    /// The names of the session's records, for a failure message.
    async fn record_names(&self) -> String {
        broca_test_support::record_kinds(self.records().await.iter().map(Record::name))
    }

    /// Wait until the latest run's step start is written. The provider can reach its hold before
    /// the run's `StepStart` is written, so without this a snapshot of the records taken at the
    /// hold can still grow by that record.
    async fn wait_latest_step_started(&self) {
        let started = async {
            loop {
                let records = self.records().await;
                let last_start = records
                    .iter()
                    .rposition(|record| matches!(record, Record::RunStart(_)));
                if last_start.is_some_and(|start| {
                    records[start..]
                        .iter()
                        .any(|record| matches!(record, Record::StepStart(_)))
                }) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        if tokio::time::timeout(Duration::from_secs(60), started)
            .await
            .is_err()
        {
            panic!(
                "timed out waiting for the latest run's StepStart: {}",
                self.record_names().await
            );
        }
    }

    /// Wait until the session holds at least `n` run ends and pauses, and return its records.
    pub async fn wait_for_finished(&self, n: usize) -> Vec<Record> {
        let finished = async {
            loop {
                let records = self.records().await;
                let finished = records
                    .iter()
                    .filter(|record| matches!(record, Record::RunEnd(_) | Record::Pause(_)))
                    .count();
                // A run's spend facts are written after its end, and the record that settles
                // them follows: a test that snapshots the log once runs end must see it there.
                if finished >= n && !self.billing_owed().await {
                    return records;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        match tokio::time::timeout(Duration::from_secs(60), finished).await {
            Ok(records) => records,
            Err(_) => panic!(
                "timed out waiting for {n} run ends or pauses: {}",
                self.record_names().await
            ),
        }
    }

    /// Whether an ended run's spend facts are still to be settled in the session's log.
    async fn billing_owed(&self) -> bool {
        let runtime = self
            .registry
            .store_runtime()
            .expect("the rig keeps its sessions in the new storage");
        match runtime.current_view(&self.session).await {
            Ok(Some(view)) => !view.operations.billing_owed.is_empty(),
            _ => false,
        }
    }

    /// Wait until the provider or the tool plane is holding a call, and the latest run's step
    /// start is written. Tests snapshot the records here to prove a later call writes nothing.
    pub async fn wait_entered(&self) {
        if tokio::time::timeout(Duration::from_secs(60), self.script.entered.notified())
            .await
            .is_err()
        {
            panic!(
                "the run never reached the held call: {}",
                self.record_names().await
            );
        }
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
            store,
            ..
        } = self;
        drop(writer);
        drop(reader);
        let _ = tokio::time::timeout(Duration::from_secs(5), serve).await;
        drop(registry);
        drop(store);
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

/// `records` without the barrier the session's writer appends each time it opens. A send that
/// is refused or replayed writes nothing of its own, but the writer it opened to answer, after a
/// restart or an idle close, still appends that barrier first.
pub fn without_writer_opens(records: Vec<Record>) -> Vec<Record> {
    records
        .into_iter()
        .filter(|record| !matches!(record, Record::WriterOpened(_)))
        .collect()
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
pub const ADMITTING_BASELINE: &str = r#","baseline":{"items":[],"tool_names":["work"],"accepted_generation":0,"applied_generation":0,"rungs":{"tools":["on_prefix_rebuild","immediately"],"system_text":["on_prefix_rebuild","immediately"]}}"#;

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

struct DelayedSubscriptionReader<'a, R> {
    reader: &'a mut R,
    delay: std::pin::Pin<Box<tokio::time::Sleep>>,
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for DelayedSubscriptionReader<'_, R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        std::task::ready!(std::future::Future::poll(this.delay.as_mut(), cx));
        std::pin::Pin::new(&mut *this.reader).poll_read(cx, buf)
    }
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

/// Normalize only envelope cursor digests; leave message digests and byte order intact.
pub fn normalize_cursor_digests(body: &[u8], incarnation: &str) -> String {
    let mut text = String::from_utf8(body.to_vec()).unwrap();
    // Only envelope cursors vary with source-frame bytes; message digests remain pinned.
    let root: std::collections::BTreeMap<&str, &serde_json::value::RawValue> =
        serde_json::from_slice(body).unwrap();
    let mut cursors: Vec<_> = root.get("cursor").copied().into_iter().collect();
    if let Some(result) = root.get("result") {
        if let Ok(result) = serde_json::from_str::<
            std::collections::BTreeMap<&str, &serde_json::value::RawValue>,
        >(result.get())
        {
            cursors.extend(result.get("head").copied());
        }
    }
    let mut replacements = Vec::new();
    for raw in cursors {
        let value: serde_json::Value = serde_json::from_str(raw.get()).unwrap();
        if value.get("wal_seq").is_none() {
            continue;
        }
        let Some(digest) = value["digest"].as_str() else {
            continue;
        };
        assert!(
            digest.len() == 16
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        assert_eq!(value["incarnation"].as_str(), Some(incarnation));
        let old = format!("\"digest\":\"{digest}\"");
        assert!(raw.get().contains(&old));
        let start = raw.get().as_ptr() as usize - body.as_ptr() as usize;
        replacements.push((
            start,
            start + raw.get().len(),
            raw.get()
                .replacen(&old, "\"digest\":\"@CURSOR_DIGEST@\"", 1),
        ));
    }
    replacements.sort_by_key(|(start, _, _)| std::cmp::Reverse(*start));
    for (start, end, replacement) in replacements {
        text.replace_range(start..end, &replacement);
    }
    text
}

/// Normalize only minted snapshot identity, cursor proofs and wall-clock versions in replies.
/// Message content and request-rendering bytes are deliberately left intact.
pub fn normalize_snapshot_bytes(body: &[u8], run_id: &str, incarnation: &str) -> String {
    use base64::Engine;
    let mut text = normalize_cursor_digests(body, incarnation);
    let scope_prefix = format!(
        "h1.{}.",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(incarnation.as_bytes())
    );
    while let Some(start) = text.find(&scope_prefix) {
        let end = start
            + scope_prefix.len()
            + text[start + scope_prefix.len()..]
                .bytes()
                .take_while(u8::is_ascii_digit)
                .count();
        text.replace_range(start..end, "@SNAPSHOT@");
    }
    let version_prefix = "\"history_version\":";
    let mut from = 0;
    while let Some(at) = text[from..].find(version_prefix) {
        let start = from + at + version_prefix.len();
        let end = start + text[start..].bytes().take_while(u8::is_ascii_digit).count();
        if start != end {
            text.replace_range(start..end, "1");
        }
        from = start + 1;
    }
    let text = normalize_store_ids(&text);
    text.replace(run_id, "@RUN_ID@")
        .replace(incarnation, "@LINEAGE_ID@")
}

/// `text` with the ids the session store mints per session replaced by placeholders: message ids
/// (`g<writer generation's first 16 hex digits>.<seq>`, spelled `g@GENERATION@.<seq>`) and the
/// session id call keys begin with (`<32 hex digits>:<seq>`, spelled `@SESSION_ID@:<seq>`).
/// Only a quoted string that is exactly such an id is replaced, so content stays intact.
pub fn normalize_store_ids(text: &str) -> String {
    fn hex_run(bytes: &[u8]) -> usize {
        bytes
            .iter()
            .take_while(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
            .count()
    }
    /// Whether `rest` is `<prefix_len bytes><separator><digits>"`.
    fn whole(rest: &[u8], prefix_len: usize, separator: u8) -> bool {
        let digits = rest.get(prefix_len + 1..).map_or(0, |tail| {
            tail.iter().take_while(|byte| byte.is_ascii_digit()).count()
        });
        rest.get(prefix_len) == Some(&separator)
            && digits > 0
            && rest.get(prefix_len + 1 + digits) == Some(&b'"')
    }
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut copied = 0;
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'"' {
            let rest = &bytes[at + 1..];
            let mid =
                rest.first() == Some(&b'g') && hex_run(&rest[1..]) == 16 && whole(rest, 17, b'.');
            if mid {
                out.push_str(&text[copied..=at]);
                out.push_str("g@GENERATION@");
                at += 1 + 17;
                copied = at;
                continue;
            }
            if hex_run(rest) == 32 && whole(rest, 32, b':') {
                out.push_str(&text[copied..=at]);
                out.push_str("@SESSION_ID@");
                at += 1 + 32;
                copied = at;
                continue;
            }
        }
        at += 1;
    }
    out.push_str(&text[copied..]);
    out
}

#[test]
fn store_id_normalization_replaces_only_whole_minted_ids() {
    assert_eq!(
        normalize_store_ids(
            r#"{"mid":"g0123456789abcdef.17","call_key":"0123456789abcdef0123456789abcdef:11","text":"g0123456789abcdef.17 stays","short":"g0123.1"}"#
        ),
        r#"{"mid":"g@GENERATION@.17","call_key":"@SESSION_ID@:11","text":"g0123456789abcdef.17 stays","short":"g0123.1"}"#
    );
}

#[test]
fn snapshot_cursor_normalization_preserves_order_and_message_digests() {
    let body = br#"{"cursor":{"wal_seq":1,"sub_index":0,"incarnation":"lineage","digest":"0123456789abcdef"},"result":{"head":{"wal_seq":2,"sub_index":0,"incarnation":"lineage","digest":"0123456789abcdef"},"message":{"digest":"0123456789abcdef","head":{"wal_seq":3,"digest":"0123456789abcdef"}}}}"#;
    assert_eq!(
        normalize_snapshot_bytes(body, "run-id", "lineage"),
        r#"{"cursor":{"wal_seq":1,"sub_index":0,"incarnation":"@LINEAGE_ID@","digest":"@CURSOR_DIGEST@"},"result":{"head":{"wal_seq":2,"sub_index":0,"incarnation":"@LINEAGE_ID@","digest":"@CURSOR_DIGEST@"},"message":{"digest":"0123456789abcdef","head":{"wal_seq":3,"digest":"0123456789abcdef"}}}}"#
    );
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
            "RunEnd",
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
