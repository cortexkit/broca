//! Today's loop and the new loop, driven by the same scripted provider and tool plane, must
//! make the same run.
//!
//! Each scenario runs once through today's loop (`broca_core::run_loop`) on a real file log,
//! and once through the new loop on a new-format session. Today's log is converted with the
//! proven converter, so both runs are compared as new-format records. Three things must match:
//! - **the requests:** every `CallOptions` the provider was asked to send, compared whole,
//!   including the render inputs that never serialize;
//! - **the records:** kind by kind and field by field, apart from fields that hold a time or
//!   an id minted per log, which the harness normalizes by name;
//! - **the tool calls:** what the tool plane was asked to run, in order.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::capability::ToolManifest;
use broca_core::tool_plane::{
    BatchCtx, DispatchKey, DispatchRegistration, DispatchResult, ProviderHeldLifecycle,
    ProviderHeldReply, ToolOpenGuard, ToolPlane, ToolPlaneError,
};
use broca_core::{
    provider::Provider, run_loop, run_loop_on_writer, run_loop_with_control, AllowAll,
    FileLeaseStore, FileWalStore, RunControl, RunRequest, StartMode, SteerInbox, StubReply,
    StubToolPlane, WalHandle, WalStore,
};
use broca_crash_harness::wal::LogImage;
use broca_protocol::caller::PauseReason;
use broca_protocol::content::{
    AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult,
};
use broca_protocol::provider::{NormalizedEvent, ProviderControl};
use broca_protocol::request::{
    CallOptions, ExecutionMode, ModelHandle, OnRestart, StopCondition, Tool, ToolChoice,
};
use broca_protocol::{FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage};
use broca_session_loop::{resume, run, Admission, MemoryParts, Seams, Writer};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::{Session, SessionError};
use broca_wal::WalRecord;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Serves scripted steps in order and records every request it was asked to send.
#[derive(Clone)]
struct ScriptedProvider {
    scripts: Arc<Mutex<VecDeque<Vec<NormalizedEvent>>>>,
    sent: Arc<Mutex<Vec<CallOptions>>>,
}

impl ScriptedProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into())),
            sent: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl Provider for ScriptedProvider {
    async fn step(&self, _step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.sent.lock().unwrap().push(opts.clone());
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for event in script {
                let _ = tx.send(event).await;
            }
        });
        rx
    }
}

fn sent_requests(provider: &ScriptedProvider) -> Vec<CallOptions> {
    let sent = provider.sent.lock().unwrap();
    sent.clone()
}

fn finished(reason: FinishReason) -> NormalizedEvent {
    NormalizedEvent::Control(ProviderControl::Finished {
        usage: Usage {
            input_tokens: Some(100),
            output_tokens: Some(20),
            ..Usage::default()
        },
        finish_reason: reason,
        retries_used: 0,
    })
}

fn answer(id: &str, content: Vec<ContentBlock>, reason: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content,
            },
        }),
        finished(reason),
    ]
}

fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    answer(
        id,
        vec![ContentBlock::Text { text: text.into() }],
        FinishReason::Stop,
    )
}

fn call(id: &str, name: &str, input: Value) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId(id.into()),
        tool_name: name.into(),
        input,
    })
}

fn tool(name: &str, mode: ExecutionMode) -> Tool {
    Tool {
        name: name.into(),
        description: format!("{name} something"),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: mode,
        reply: None,
    }
}

/// The scripted tool plane, wrapped so every call a loop makes on it is logged in order, with
/// its arguments. Both loops must make the same calls: this sees what the stub's own log
/// can't, such as registration, the guarded and keyed entry points, and the call key each
/// dispatch carried. Keys are logged as `key#n`, numbered by first use, since each format mints
/// its own; `keys` keeps the real one per call, so a run can check it against the call's intent.
struct Spy {
    inner: StubToolPlane,
    log: Arc<Mutex<Vec<String>>>,
    keys: Mutex<Vec<(ToolCallId, String)>>,
    held: Arc<HeldScript>,
    /// The caller's cancel, which the `hold_cancel` tool's call triggers once its provider holds
    /// it: a caller who gives up on a run waiting on a held call.
    cancel_on_hold: Mutex<Option<CancellationToken>>,
}

/// A provider that holds calls: the `hold` tool's provider answers with a held marker and
/// delivers its late result only once the scenario deposits it; `hold_brief`'s result is
/// deposited as soon as it's held, so it arrives while the batch is still running. Both log
/// to the spy's log, in order with the plane's calls.
struct HeldScript {
    log: Arc<Mutex<Vec<String>>>,
    replies: Mutex<Vec<ProviderHeldReply>>,
    deposits: Mutex<Vec<ToolResult>>,
    /// Whether the run's end is durable yet, for the withdrawal's log entry.
    ended: Mutex<Option<Arc<dyn Fn() -> bool + Send + Sync>>>,
}

impl HeldScript {
    fn deposited(&self, id: &ToolCallId) -> Option<ToolResult> {
        self.deposits
            .lock()
            .unwrap()
            .iter()
            .find(|result| result.tool_call_id == *id)
            .cloned()
    }

    fn deposit(&self, id: &ToolCallId) {
        self.deposits.lock().unwrap().push(ToolResult {
            tool_call_id: id.clone(),
            output: ToolOutput::Text {
                text: format!("late result for {}", id.0),
            },
            is_error: false,
        });
    }

    /// Deposits the late result of every call still held, as a provider's feed would.
    fn deposit_all(&self) {
        let replies = self.replies.lock().unwrap().clone();
        for reply in replies {
            if self.deposited(&reply.tool_call_id).is_none() {
                self.deposit(&reply.tool_call_id);
            }
        }
    }
}

#[async_trait]
impl ProviderHeldLifecycle for HeldScript {
    fn deposited_result(&self, _ctx: &BatchCtx, reply: &ProviderHeldReply) -> Option<ToolResult> {
        self.deposited(&reply.tool_call_id)
    }

    async fn wait(
        &self,
        _ctx: &BatchCtx,
        call: &ToolCall,
        _reply: &ProviderHeldReply,
    ) -> DispatchResult {
        self.log
            .lock()
            .unwrap()
            .push(format!("held_wait {}", call.tool_call_id.0));
        // `hold_brief`'s provider answers promptly, also when a resumed run waits for it again.
        if call.tool_name == "hold_brief" && self.deposited(&call.tool_call_id).is_none() {
            self.deposit(&call.tool_call_id);
        }
        match self.deposited(&call.tool_call_id) {
            Some(result) => DispatchResult::Completed(result),
            None => std::future::pending().await,
        }
    }

    async fn terminal_synced(&self, _ctx: &BatchCtx, id: &ToolCallId) {
        self.log
            .lock()
            .unwrap()
            .push(format!("terminal_synced {}", id.0));
    }

    async fn withdraw_held(&self, call: &ProviderHeldReply) -> Result<(), String> {
        let ended = self
            .ended
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|ended| ended());
        self.log.lock().unwrap().push(format!(
            "withdraw {} after_run_end={ended}",
            call.tool_call_id.0
        ));
        Ok(())
    }
}

impl Spy {
    fn new(inner: StubToolPlane) -> Self {
        let log: Arc<Mutex<Vec<String>>> = Default::default();
        Self {
            inner,
            held: Arc::new(HeldScript {
                log: Arc::clone(&log),
                replies: Default::default(),
                deposits: Default::default(),
                ended: Mutex::new(None),
            }),
            log,
            keys: Mutex::new(Vec::new()),
            cancel_on_hold: Mutex::new(None),
        }
    }

    /// Makes `hold_cancel` cancel the run through `cancel` once its call is held.
    fn cancel_on_hold(&self, cancel: CancellationToken) {
        *self.cancel_on_hold.lock().unwrap() = Some(cancel);
    }

    /// The held calls' lifecycle, as the runtime installs it for a run.
    fn lifecycle(&self) -> Arc<dyn ProviderHeldLifecycle> {
        Arc::clone(&self.held) as Arc<dyn ProviderHeldLifecycle>
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    /// The key each dispatched call carried, by call id, in dispatch order.
    fn keys(&self) -> Vec<(ToolCallId, String)> {
        self.keys.lock().unwrap().clone()
    }

    fn note(&self, entry: String) {
        self.log.lock().unwrap().push(entry);
    }

    /// `key#n` for `key`, numbered by first use; remembers it for `call`.
    fn key(&self, call: &ToolCallId, key: &str) -> String {
        let mut keys = self.keys.lock().unwrap();
        let known: Vec<&String> = keys.iter().map(|(_, known)| known).collect();
        let n = match known.iter().position(|known| *known == key) {
            Some(at) => at + 1,
            None => known.len() + 1,
        };
        keys.push((call.clone(), key.to_owned()));
        format!("key#{n}")
    }
}

fn ids(calls: &[ToolCall]) -> Vec<&str> {
    calls
        .iter()
        .map(|call| call.tool_call_id.0.as_str())
        .collect()
}

#[async_trait]
impl ToolPlane for Spy {
    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        self.note(format!("batch_started {:?}", ids(calls)));
        self.inner.batch_started(ctx, calls, manifest).await
    }

    async fn batch_started_guarded(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
        guard: &ToolOpenGuard,
    ) -> Result<(), ToolPlaneError> {
        self.note(format!("batch_started_guarded {:?}", ids(calls)));
        self.inner
            .batch_started_guarded(ctx, calls, manifest, guard)
            .await
    }

    async fn register_dispatch_group(
        &self,
        ctx: &BatchCtx,
        calls: &[DispatchRegistration],
    ) -> Result<(), ToolPlaneError> {
        let entries: Vec<String> = calls
            .iter()
            .map(|call| {
                format!(
                    "{} {} approved={}",
                    call.tool_call_id.0,
                    self.key(&call.tool_call_id, &call.call_key),
                    call.approved
                )
            })
            .collect();
        self.note(format!("register {entries:?}"));
        self.inner.register_dispatch_group(ctx, calls).await
    }

    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.note(format!("dispatch {}", call.tool_call_id.0));
        self.inner.dispatch(ctx, call).await
    }

    async fn dispatch_keyed(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        key: DispatchKey<'_>,
    ) -> DispatchResult {
        let token = self.key(&call.tool_call_id, key.call_key);
        self.note(format!(
            "dispatch_keyed {} {token} recorder={}",
            call.tool_call_id.0,
            key.held_recorder.is_some()
        ));
        self.inner.dispatch_keyed(ctx, call, key).await
    }

    async fn dispatch_guarded(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        key: Option<DispatchKey<'_>>,
        guard: &ToolOpenGuard,
    ) -> DispatchResult {
        let keyed = key.map(|key| {
            format!(
                "{} recorder={}",
                self.key(&call.tool_call_id, key.call_key),
                key.held_recorder.is_some()
            )
        });
        self.note(format!(
            "dispatch_guarded {} {}",
            call.tool_call_id.0,
            keyed.as_deref().unwrap_or("unkeyed")
        ));
        if let (
            "hold" | "hold_brief" | "hold_cancel",
            Some(DispatchKey {
                call_key,
                held_recorder: Some(recorder),
            }),
        ) = (call.tool_name.as_str(), key)
        {
            // The provider answers with a held marker, which the run records durably before the
            // provider's late result is awaited.
            let reply = ProviderHeldReply {
                tool_call_id: call.tool_call_id.clone(),
                call_key: call_key.to_owned(),
                module: "provider".into(),
                reason: "running".into(),
                detail_digest: None,
            };
            if let Err(error) = recorder.sync_held(&reply).await {
                return DispatchResult::SentButOutcomeUnknown { reason: error };
            }
            self.held.replies.lock().unwrap().push(reply.clone());
            if call.tool_name == "hold_brief" {
                self.held.deposit(&call.tool_call_id);
            }
            if call.tool_name == "hold_cancel" {
                if let Some(cancel) = self.cancel_on_hold.lock().unwrap().as_ref() {
                    cancel.cancel();
                }
            }
            return self.held.wait(ctx, call, &reply).await;
        }
        self.inner.dispatch_guarded(ctx, call, key, guard).await
    }

    fn dispatch_target(&self, ctx: &BatchCtx, id: &ToolCallId) -> Option<String> {
        self.inner.dispatch_target(ctx, id)
    }

    fn dispatch_schema_pin(
        &self,
        ctx: &BatchCtx,
        id: &ToolCallId,
        tool: &str,
    ) -> Option<broca_wal::DispatchSchemaPin> {
        self.inner.dispatch_schema_pin(ctx, id, tool)
    }

    async fn batch_finished(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        self.note("batch_finished".into());
        self.inner.batch_finished(ctx).await
    }
}

/// A log's lineage id as the converter takes it, which today's call keys are made of.
fn lineage(image: &LogImage) -> Option<String> {
    image.lineage_id().map(|lineage| lineage.to_string())
}

/// One scenario: what the caller sends and what the provider and tools answer.
struct Scenario {
    input: Vec<Message>,
    tools: Vec<Tool>,
    stop_when: Vec<StopCondition>,
    steps: Vec<Vec<NormalizedEvent>>,
}

impl Scenario {
    fn plane(&self) -> Spy {
        Spy::new(
            StubToolPlane::new()
                .with_canned_text("read", "file contents")
                .with_tool("grep", |_| StubReply::ExecError("no match".into()))
                .with_tool("fetch", |_| {
                    StubReply::PlaneUnavailable("route closed".into())
                })
                .with_tool("write", |_| {
                    StubReply::OutcomeUnknown("connection reset".into())
                }),
        )
    }

    fn request(&self) -> RunRequest {
        self.request_for("r1", &self.input)
    }

    fn request_for(&self, run_id: &str, input: &[Message]) -> RunRequest {
        RunRequest {
            route_decision_id: None,
            input_written_at_ms: None,
            directives: None,
            session_class: None,
            retention: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId(run_id.into()),
            session: bind(),
            model: ModelHandle::new("stub", "m"),
            input: input.to_vec(),
            tools: self.tools.clone(),
            policy_allowed: self.tools.iter().map(|t| t.name.clone()).collect(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            stop_when: self.stop_when.clone(),
            submission_id: None,
            budget: None,
            context_limit: None,
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen_cache: false,
            structured_output: None,
        }
    }

    fn admission(&self, provider: &ScriptedProvider) -> Admission {
        self.admission_for(provider, "r1", &self.input)
    }

    fn admission_for(
        &self,
        provider: &ScriptedProvider,
        run_id: &str,
        input: &[Message],
    ) -> Admission {
        Admission {
            run_id: run_id.into(),
            model: ModelHandle::new("stub", "m"),
            input: input.to_vec(),
            tools: self.tools.clone(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: Default::default(),
            provider_options: Value::Null,
            render_config: provider.render_config_blob(),
            renderer_version: provider.renderer_version(),
            stop_when: self.stop_when.clone(),
            on_restart: None,
            ts_ms: Some(0),
            written_at_ms: Some(0),
        }
    }
}

/// What one loop did: its records as new-format records, the requests it sent, and the calls
/// its tool plane ran.
#[derive(Debug)]
struct Outcome {
    records: Vec<Value>,
    requests: Vec<CallOptions>,
    calls: Vec<String>,
}

async fn today(scenario: &Scenario) -> Outcome {
    today_from(scenario, None, scenario.steps.clone()).await.0
}

/// Runs today's loop, starting from the log bytes `start` when given (a crash's cut log, which
/// today's loop resumes), with the provider serving `steps`. Returns what it did, its whole
/// log, and the log's file name.
async fn today_from(
    scenario: &Scenario,
    start: Option<(&str, &[u8])>,
    steps: Vec<Vec<NormalizedEvent>>,
) -> (Outcome, LogImage, String) {
    let dir = broca_test_support::scratch_root().join(format!(
        "session-loop-differential-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("wal")).unwrap();
    if let Some((name, bytes)) = start {
        std::fs::write(dir.join("wal").join(name), bytes).unwrap();
    }
    let provider = ScriptedProvider::new(steps);
    let plane = scenario.plane();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    run_loop_with_control(
        scenario.request(),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl {
            provider_held_lifecycle: Some(plane.lifecycle()),
            ..Default::default()
        },
        &tx,
    )
    .await
    .expect("today's loop runs");
    drop(tx);
    drain.await.unwrap();
    let file = std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "wal"))
        .expect("today's loop wrote a log");
    let image = LogImage::read(&file);
    let name = file.file_name().unwrap().to_string_lossy().into_owned();
    let converted = broca_session_convert::convert(&image.framed(), lineage(&image).as_deref())
        .expect("the log converts");
    let _ = std::fs::remove_dir_all(&dir);
    (
        Outcome {
            records: normalized(&converted.records),
            requests: sent_requests(&provider),
            calls: plane
                .log()
                .iter()
                .map(|event| format!("{event:?}"))
                .collect(),
        },
        image,
        name,
    )
}

/// A session writer that also keeps every record it appended, for comparison.
struct Recording<'s, 'b> {
    session: &'s mut Session<'b, MemoryBackend>,
    records: Vec<Record>,
    /// Records another appender (the session actor, in production) added while the loop was
    /// waiting on a seam. They're appended, durably, before the loop's next batch.
    injected: Arc<Mutex<Vec<Record>>>,
    /// Set once a run's end is appended durably, so a provider can tell whether it was asked to
    /// withdraw a call before or after the run's end.
    ended: Arc<std::sync::atomic::AtomicBool>,
}

impl Recording<'_, '_> {
    async fn flush(&mut self) -> Result<(), SessionError> {
        let pending: Vec<Record> = std::mem::take(&mut *self.injected.lock().unwrap());
        if !pending.is_empty() {
            self.records.extend(pending.iter().cloned());
            self.session.append(&pending, true).await?;
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl Writer for Recording<'_, '_> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        self.flush().await?;
        self.records.extend_from_slice(records);
        let seq = self.session.append(records, barrier).await?;
        if barrier
            && records
                .iter()
                .any(|record| matches!(record, Record::RunEnd(_)))
        {
            self.ended.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(seq)
    }
    fn conversation(&self) -> &Conversation {
        &self.session.conversation
    }
    fn operations(&self) -> &Operations {
        &self.session.operations
    }
    fn session_id(&self) -> String {
        Writer::session_id(&*self.session)
    }
    async fn send_records(&mut self, submission_id: &str) -> Result<Vec<Record>, SessionError> {
        self.flush().await?;
        Writer::send_records(&mut *self.session, submission_id).await
    }

    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations, u64) -> Vec<Record>
                  + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError> {
        self.flush().await?;
        let records = build(
            &self.session.conversation,
            &self.session.operations,
            self.session.next_seq(),
        );
        if !records.is_empty() {
            let barrier = barrier || broca_session_loop::writer::ends_run(&records);
            self.append(&records, barrier).await?;
        }
        Ok(records)
    }
    fn next_seq(&self) -> u64 {
        self.session.next_seq()
    }
}

/// Both loops route tool calls under one bind, so their requests carry the same session
/// address and their tool calls the same bind.
fn bind() -> broca_protocol::BindIdentity {
    broca_protocol::BindIdentity::new("/differential/project", "differential", "s1")
}

async fn new_loop(scenario: &Scenario) -> Outcome {
    new_from(scenario, None, scenario.steps.clone()).await
}

/// Runs the new loop on a new session. With `start` (a crash's cut log, converted), the session
/// first holds those records and the loop resumes its run; the outcome's records are only what
/// the loop appended.
async fn new_from(
    scenario: &Scenario,
    start: Option<broca_session_convert::Converted>,
    steps: Vec<Vec<NormalizedEvent>>,
) -> Outcome {
    let bind = bind();
    let backend = MemoryBackend::default();
    let triple = SessionTriple {
        project_root: "/project".into(),
        harness: "differential".into(),
        session: "s1".into(),
    };
    let mut session = Session::create(
        &backend,
        SessionAddr("0123456789abcdef".into()),
        triple,
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    let mut parts = MemoryParts::default();
    let resuming = match start {
        Some(converted)
            if converted
                .records
                .iter()
                .any(|r| matches!(r, Record::RunStart(_))) =>
        {
            session.append(&converted.records, true).await.unwrap();
            parts.parts = converted.parts;
            true
        }
        _ => false,
    };
    let provider = ScriptedProvider::new(steps);
    let plane = scenario.plane();
    let lifecycle = plane.lifecycle();
    let mut writer = Recording {
        session: &mut session,
        records: Vec::new(),
        injected: Default::default(),
        ended: Default::default(),
    };
    let now = || 0u64;
    let seams = Seams {
        provider: &provider,
        tools: &plane,
        bind: bind.clone(),
        out: None,
        cancel: Default::default(),
        stop: Default::default(),
        held: Some(&*lifecycle),
        now_ms: &now,
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
    };
    if resuming {
        resume(&mut writer, &parts, &seams).await
    } else {
        run(
            &mut writer,
            &mut parts,
            scenario.admission(&provider),
            &seams,
        )
        .await
    }
    .expect("the new loop runs");
    let records = writer.records.clone();
    keys_match(writer.session, &plane).await;
    Outcome {
        records: normalized(&records),
        requests: sent_requests(&provider),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .collect(),
    }
}

fn rand_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// The fields that hold a time or an id minted per log, replaced so two logs compare.
const NORMALIZED: &[&str] = &[
    "ts_ms",
    "written_at_ms",
    "finished_at_ms",
    "dispatched_at_ms",
    "last_content_at_ms",
    "held_at_ms",
    "call_key",
    // A log position: each format numbers its own records.
    "resumed_from_seq",
    // The seq of the record that accepted a steered message, in each format's own numbering.
    "receipt_seq",
];

fn normalized(records: &[Record]) -> Vec<Value> {
    records
        .iter()
        .map(|record| {
            let mut value = serde_json::to_value(record).expect("records serialize");
            scrub(&mut value);
            json!({ record.name(): value })
        })
        .collect()
}

/// Every call the new loop sent carried a key that finds, through the session's own id lookup,
/// exactly one intent, that call's. A key the new loop minted names the intent's position:
/// `<session id>:<seq>`.
async fn keys_match(session: &Session<'_, MemoryBackend>, plane: &Spy) {
    let id = broca_session_store::codec::hex(&session.head().session_id.0);
    for (call, key) in plane.keys() {
        let found = session
            .find_id(broca_session_store::idindex::IdKind::CallKey, &key)
            .await
            .expect("the id lookup reads");
        // Other records may name the key too, such as a provider's held reply; exactly one
        // intent does.
        let intents: Vec<(u64, &broca_session_store::record::ToolIntent)> = found
            .iter()
            .filter_map(|(seq, record)| match record {
                Record::ToolIntent(intent) => Some((*seq, intent)),
                _ => None,
            })
            .collect();
        let [(seq, intent)] = intents.as_slice() else {
            panic!("key {key} of call {} finds {found:?}", call.0);
        };
        assert_eq!(intent.tool_call_id, call, "key {key} names another call");
        // A converted intent keeps the key today's runner sent, made from today's log; only a
        // key the new loop minted names the new log's position.
        if intent.key_derived != Some(true) {
            assert_eq!(key, format!("{id}:{seq}"), "key of call {}", call.0);
        }
    }
}

/// Fields only a conversion writes, which say how a value was found rather than what it is.
/// Today's records get them from the converter and the new loop's never do, so they're left out
/// of the comparison.
const CONVERSION_ONLY: &[&str] = &["key_derived"];

fn scrub(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|key, _| !CONVERSION_ONLY.contains(&key.as_str()));
            for (key, field) in map.iter_mut() {
                if NORMALIZED.contains(&key.as_str()) && !field.is_null() {
                    *field = json!("<normalized>");
                } else {
                    scrub(field);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(scrub),
        _ => {}
    }
}

async fn compare(scenario: Scenario) {
    let today = today(&scenario).await;
    let dump = std::env::var_os("DIFFERENTIAL_DUMP").is_some();
    if dump {
        for (index, record) in today.records.iter().enumerate() {
            eprintln!("today {index}: {record}");
        }
    }
    let new = new_loop(&scenario).await;
    if dump {
        for (index, record) in new.records.iter().enumerate() {
            eprintln!("new   {index}: {record}");
        }
    }
    assert_eq!(new.requests.len(), today.requests.len(), "request count");
    for (index, (new, today)) in new.requests.iter().zip(&today.requests).enumerate() {
        assert_eq!(new, today, "request {index}");
    }
    for (index, (new, today)) in new.records.iter().zip(&today.records).enumerate() {
        assert_eq!(new, today, "record {index}");
    }
    assert_eq!(new.records.len(), today.records.len(), "record count");
    assert_eq!(new.calls, today.calls, "tool calls");
}

fn user(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn system(text: &str) -> Message {
    Message::new(Role::System, vec![ContentBlock::Text { text: text.into() }])
}

#[tokio::test]
async fn a_text_answer() {
    compare(Scenario {
        input: vec![system("Be brief."), user("hello")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![text_step("a1", "hi")],
    })
    .await;
}

#[tokio::test]
async fn a_tool_call_then_an_answer() {
    compare(Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Sequential)],
        stop_when: vec![StopCondition::StepCount { n: 8 }],
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "read", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn parallel_calls_one_failing() {
    compare(Scenario {
        input: vec![user("look around")],
        tools: vec![
            tool("read", ExecutionMode::Parallel),
            tool("grep", ExecutionMode::Parallel),
        ],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    ContentBlock::Text {
                        text: "checking".into(),
                    },
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "grep", json!({"pattern": "x"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn sequential_calls_keep_their_order() {
    compare(Scenario {
        input: vec![user("read two files")],
        tools: vec![
            tool("read", ExecutionMode::Sequential),
            tool("grep", ExecutionMode::Parallel),
        ],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "grep", json!({"pattern": "b"})),
                    call("c3", "read", json!({"path": "c"})),
                ],
                FinishReason::ToolCalls,
            ),
            answer(
                "a2",
                vec![call("c4", "read", json!({"path": "d"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a3", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn a_step_limit_with_calls_pending() {
    compare(Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        stop_when: vec![StopCondition::StepCount { n: 1 }],
        steps: vec![answer(
            "a1",
            vec![call("c1", "read", json!({"path": "a"}))],
            FinishReason::ToolCalls,
        )],
    })
    .await;
}

#[tokio::test]
async fn a_call_that_was_never_sent() {
    compare(Scenario {
        input: vec![user("fetch a")],
        tools: vec![tool("fetch", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "fetch", json!({"url": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "it failed"),
        ],
    })
    .await;
}

#[tokio::test]
async fn a_call_whose_outcome_is_unknown() {
    compare(Scenario {
        input: vec![user("write a")],
        tools: vec![tool("write", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "write", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "should not be reached"),
        ],
    })
    .await;
}

/// Cuts today's log of `scenario` after each of its records, as a crash could, and resumes the
/// cut twice: today's loop resumes the cut log, and the new loop resumes a session holding the
/// same records converted. Both must send the same requests, run the same tool calls, and
/// append the same records.
async fn compare_every_cut(scenario: Scenario) {
    let (_, full, name) = today_from(&scenario, None, scenario.steps.clone()).await;
    let records = full.framed().len();
    assert!(records > 2, "the scenario wrote a log to cut");
    for cut in 1..records {
        let bytes = broca_crash_harness::cut_log(&full, cut, false);
        let cut_image = LogImage::parse(bytes.clone());
        let frames = cut_image.framed();
        // The provider serves the steps the cut log has no answer for yet.
        let answered = frames
            .iter()
            .filter(|frame| {
                serde_json::to_value(&frame.record).unwrap()["type"] == "model_step_finished"
            })
            .count();
        let steps = scenario.steps[answered..].to_vec();
        let converted = broca_session_convert::convert(&frames, lineage(&cut_image).as_deref())
            .expect("the cut converts");
        let cut_records = converted.records.clone();
        let before = converted.records.len();
        let (today, _, _) = today_from(&scenario, Some((&name, &bytes)), steps.clone()).await;
        eprintln!("cut after record {cut} of {records}");
        let new = new_from(&scenario, Some(converted), steps).await;
        let today_appended = &today.records[before.min(today.records.len())..];
        let context = format!("cut after record {cut} of {records}");
        if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
            for (index, record) in today_appended.iter().enumerate() {
                eprintln!("{context} today {index}: {record}");
            }
            for (index, record) in new.records.iter().enumerate() {
                eprintln!("{context} new   {index}: {record}");
            }
        }
        assert_eq!(
            new.requests.len(),
            today.requests.len(),
            "{context}: request count"
        );
        for (index, (new, today)) in new.requests.iter().zip(&today.requests).enumerate() {
            assert_eq!(new, today, "{context}: request {index}");
        }
        for (index, (new, today)) in new.records.iter().zip(today_appended).enumerate() {
            assert_eq!(new, today, "{context}: record {index}");
        }
        assert_eq!(
            new.records.len(),
            today_appended.len(),
            "{context}: record count"
        );
        let expected_calls = if resumes_an_answered_batch(&cut_records) {
            without_empty_resumed_batch(&today.calls)
        } else {
            today.calls.clone()
        };
        assert_eq!(new.calls, expected_calls, "{context}: tool calls");
    }
}

/// Whether a crash left the run's open tool batch with every call answered and the batch not
/// yet closed, read from the operational fold over the cut's records.
fn resumes_an_answered_batch(records: &[Record]) -> bool {
    let mut operations = Operations::default();
    for record in records {
        if record.stream() == broca_session_store::record::Stream::Ledger {
            operations.apply(record).expect("the cut folds");
        }
    }
    operations
        .run
        .as_ref()
        .and_then(|run| run.open_batch.as_ref())
        .is_some_and(|batch| {
            (0..batch.calls.len()).all(|position| {
                batch.state(position)
                    == broca_session_store::fold::operations::CallState::Terminated
            })
        })
}

/// Today's tool-plane calls without the one difference the new loop makes on purpose: on
/// resume, today's loop opens and closes a batch whose calls all have recorded results, sending
/// nothing; the new loop records the batch's end without calling the plane, since a plane can
/// open routes for a batch it never sends on. Only that pair, opening the resume, is left out.
fn without_empty_resumed_batch(calls: &[String]) -> Vec<String> {
    match calls {
        [started, finished, rest @ ..]
            if started.starts_with("\"batch_started_guarded ")
                && finished == "\"batch_finished\"" =>
        {
            rest.to_vec()
        }
        _ => calls.to_vec(),
    }
}

#[tokio::test]
async fn every_cut_of_a_tool_run_resumes_alike() {
    compare_every_cut(Scenario {
        input: vec![user("read two files")],
        tools: vec![
            tool("read", ExecutionMode::Sequential),
            tool("grep", ExecutionMode::Parallel),
        ],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "grep", json!({"pattern": "b"})),
                ],
                FinishReason::ToolCalls,
            ),
            answer(
                "a2",
                vec![call("c3", "read", json!({"path": "c"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a3", "done"),
        ],
    })
    .await;
}

fn tool_run() -> Scenario {
    Scenario {
        input: vec![user("look around")],
        tools: vec![
            tool("read", ExecutionMode::Parallel),
            tool("grep", ExecutionMode::Parallel),
        ],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    ContentBlock::Text {
                        text: "checking".into(),
                    },
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "grep", json!({"pattern": "x"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    }
}

#[tokio::test]
async fn every_cut_of_parallel_calls_resumes_alike() {
    compare_every_cut(tool_run()).await;
}

#[tokio::test]
async fn every_cut_of_a_text_answer_resumes_alike() {
    compare_every_cut(Scenario {
        input: vec![system("Be brief."), user("hello")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![text_step("a1", "hi")],
    })
    .await;
}

#[tokio::test]
async fn every_cut_of_a_step_limit_resumes_alike() {
    compare_every_cut(Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        stop_when: vec![StopCondition::StepCount { n: 1 }],
        steps: vec![answer(
            "a1",
            vec![call("c1", "read", json!({"path": "a"}))],
            FinishReason::ToolCalls,
        )],
    })
    .await;
}

#[tokio::test]
async fn every_cut_of_an_unknown_outcome_resumes_alike() {
    compare_every_cut(Scenario {
        input: vec![user("write a")],
        tools: vec![tool("write", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "write", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "should not be reached"),
        ],
    })
    .await;
}

#[tokio::test]
async fn every_cut_of_an_unsent_call_resumes_alike() {
    compare_every_cut(Scenario {
        input: vec![user("fetch a")],
        tools: vec![tool("fetch", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "fetch", json!({"url": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "it failed"),
        ],
    })
    .await;
}

/// Runs `runs` one after another on one session through today's loop (the first with
/// `run_loop`, later ones as appended episodes) and through the new loop, with one scripted
/// provider and tool plane per loop, and compares everything both did.
async fn compare_runs(scenario: Scenario, runs: &[(&str, Vec<Message>)]) {
    // Today's loop.
    let dir = broca_test_support::scratch_root().join(format!(
        "session-loop-differential-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let provider = ScriptedProvider::new(scenario.steps.clone());
    let plane = scenario.plane();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    for (index, (run_id, input)) in runs.iter().enumerate() {
        let (tx, mut rx) = mpsc::channel(512);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let request = scenario.request_for(run_id, input);
        if index == 0 {
            run_loop(request, &provider, &plane, &lease, &AllowAll, &wal, &tx)
                .await
                .expect("today's first run");
        } else {
            broca_core::run_loop_append_episode(
                request, &provider, &plane, &lease, &AllowAll, &wal, None, &tx,
            )
            .await
            .expect("today's later run");
        }
        drop(tx);
        drain.await.unwrap();
    }
    let file = std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "wal"))
        .expect("today's loop wrote a log");
    let converted = {
        let image = LogImage::read(&file);
        broca_session_convert::convert(&image.framed(), lineage(&image).as_deref())
    }
    .expect("the log converts");
    let _ = std::fs::remove_dir_all(&dir);
    let today = Outcome {
        records: normalized(&converted.records),
        requests: sent_requests(&provider),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .collect(),
    };

    // The new loop.
    let backend = MemoryBackend::default();
    let triple = SessionTriple {
        project_root: "/project".into(),
        harness: "differential".into(),
        session: "s1".into(),
    };
    let mut session = Session::create(
        &backend,
        SessionAddr("0123456789abcdef".into()),
        triple,
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    let provider = ScriptedProvider::new(scenario.steps.clone());
    let plane = scenario.plane();
    let mut writer = Recording {
        session: &mut session,
        records: Vec::new(),
        injected: Default::default(),
        ended: Default::default(),
    };
    let mut parts = MemoryParts::default();
    let now = || 0u64;
    let bind = bind();
    let seams = Seams {
        provider: &provider,
        tools: &plane,
        bind: bind.clone(),
        out: None,
        cancel: Default::default(),
        stop: Default::default(),
        held: None,
        now_ms: &now,
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
    };
    for (run_id, input) in runs {
        run(
            &mut writer,
            &mut parts,
            scenario.admission_for(&provider, run_id, input),
            &seams,
        )
        .await
        .expect("the new loop runs");
    }
    keys_match(writer.session, &plane).await;
    let new = Outcome {
        records: normalized(&writer.records),
        requests: sent_requests(&provider),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .collect(),
    };

    if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
        for (index, record) in today.records.iter().enumerate() {
            eprintln!("today {index}: {record}");
        }
        for (index, record) in new.records.iter().enumerate() {
            eprintln!("new   {index}: {record}");
        }
    }
    assert_eq!(new.requests.len(), today.requests.len(), "request count");
    for (index, (new, today)) in new.requests.iter().zip(&today.requests).enumerate() {
        assert_eq!(new, today, "request {index}");
    }
    for (index, (new, today)) in new.records.iter().zip(&today.records).enumerate() {
        assert_eq!(new, today, "record {index}");
    }
    assert_eq!(new.records.len(), today.records.len(), "record count");
    assert_eq!(new.calls, today.calls, "tool calls");
}

#[tokio::test]
async fn a_second_run_on_the_same_session() {
    compare_runs(
        Scenario {
            input: Vec::new(),
            tools: vec![tool("read", ExecutionMode::Sequential)],
            stop_when: vec![StopCondition::StepCount { n: 8 }],
            steps: vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done"),
                answer(
                    "a3",
                    vec![call("c2", "read", json!({"path": "b"}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a4", "done again"),
            ],
        },
        &[
            ("r1", vec![system("You are careful."), user("first task")]),
            ("r2", vec![user("second task")]),
        ],
    )
    .await;
}

/// What the session actor does while the provider answers a step.
#[derive(Clone)]
enum Action {
    /// Steers these messages into the run.
    Steer(Vec<Message>),
    /// Cancels the run at its next step boundary.
    CancelAtBoundary,
    /// Asks the run to stop, as the runner does when it shuts down.
    Stop,
    /// Queues a send with a delivery order, as the actor does for a running run: its
    /// `delivery_ordered` and `queued` records, in today's format.
    Ordered {
        send: Box<broca_core::delivery_order::OrderedSend>,
        records: Box<[WalRecord; 2]>,
    },
}

/// A send queued with delivery `modes`, expiring at `expires_at_ms` when given.
fn ordered(id: &str, modes: &[&str], text: &str, expires_at_ms: Option<u64>) -> Action {
    let mut order = json!({"modes": modes});
    if let Some(expires) = expires_at_ms {
        order["expires_at_ms"] = json!(expires);
    }
    let input = vec![user(text)];
    let send = broca_core::delivery_order::OrderedSend {
        admitted_at_ms: Some(0),
        submission_id: broca_protocol::SubmissionId(id.into()),
        order: serde_json::from_value(order.clone()).unwrap(),
        principal: None,
        input: input.clone(),
    };
    let records = [
        serde_json::from_value(json!({
            "type": "delivery_ordered", "submission_id": id, "order": order,
        }))
        .unwrap(),
        serde_json::from_value(json!({
            "type": "queued",
            "submission_id": id,
            "request": {
                "model": {"provider_module_id": "stub", "model_id": "m"},
                "input": input,
                "policy_allowed": [],
                "tool_choice": {"type": "auto"},
                "generation": {},
                "stop_when": [],
            },
            "written_at_ms": 0,
        }))
        .unwrap(),
    ];
    Action::Ordered {
        send: Box::new(send),
        records: Box::new(records),
    }
}

/// Where a scripted provider sends what the session actor does while a step is answered.
#[async_trait]
trait SteerSink: Send + Sync {
    /// Writes a `steered` record as the actor would.
    async fn steer(&self, record: WalRecord);
    /// Records the caller's cancel at the run's next step boundary, as the actor would.
    async fn cancel_at_boundary(&self);
    /// Asks the run to stop, as the runner does when it shuts down.
    fn stop(&self);
    /// Queues an ordered send into the running run, as the actor does.
    async fn ordered(&self, send: broca_core::delivery_order::OrderedSend, records: [WalRecord; 2]);
}

/// Today's actor: the record is written under the run's steer inbox lock and synced.
struct TodaySink {
    inbox: Arc<SteerInbox>,
    writer: Arc<WalHandle>,
    stop: CancellationToken,
}

#[async_trait]
impl SteerSink for TodaySink {
    async fn steer(&self, record: WalRecord) {
        let WalRecord::Steered { input, .. } = &record else {
            panic!("a steer");
        };
        let messages = input.as_ref().clone();
        let writer = Arc::clone(&self.writer);
        assert!(self.inbox.deliver(messages, move || {
            writer.record(record);
            None
        }));
        self.writer.sync().await.expect("the steer is durable");
    }

    async fn cancel_at_boundary(&self) {
        self.inbox
            .request_boundary_cancel(&self.writer, &RunId("r1".into()))
            .await
            .expect("the cancel is durable");
    }

    fn stop(&self) {
        self.stop.cancel();
    }

    async fn ordered(
        &self,
        send: broca_core::delivery_order::OrderedSend,
        records: [WalRecord; 2],
    ) {
        let writer = Arc::clone(&self.writer);
        let accepted = self.inbox.accept_ordered(send, move |superseded| {
            assert!(superseded.is_none(), "no send is replaced");
            for record in records {
                writer.record(record);
            }
        });
        assert!(
            matches!(
                accepted,
                broca_core::steer::OrderedAcceptance::Queued { .. }
            ),
            "the running turn takes the send"
        );
        self.writer.sync().await.expect("the send is durable");
    }
}

/// The new store's actor: the same steer as the converter writes it, appended before the
/// loop's next batch.
struct NewSink {
    injected: Arc<Mutex<Vec<Record>>>,
    stop: CancellationToken,
}

#[async_trait]
impl SteerSink for NewSink {
    async fn steer(&self, record: WalRecord) {
        let mut value = serde_json::to_value(&record).unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("type");
        let input = serde_json::from_value(object.remove("input").unwrap()).unwrap();
        let written_at_ms = object.remove("written_at_ms").and_then(|v| v.as_u64());
        let tags_v1 = object.remove("tags");
        let receipt = (!object.is_empty()).then(|| value.clone());
        self.injected
            .lock()
            .unwrap()
            .push(Record::Steer(broca_session_store::record::Steer {
                input,
                written_at_ms,
                tags_v1,
                receipt,
            }));
    }

    async fn cancel_at_boundary(&self) {
        self.injected.lock().unwrap().push(Record::CancelRequest(
            broca_session_store::record::CancelRequest {
                run_id: "r1".into(),
                at: broca_session_store::record::CancelPoint::StepBoundary,
            },
        ));
    }

    fn stop(&self) {
        self.stop.cancel();
    }

    async fn ordered(
        &self,
        _send: broca_core::delivery_order::OrderedSend,
        records: [WalRecord; 2],
    ) {
        let framed: Vec<broca_wal::FramedRecord> = records
            .into_iter()
            .enumerate()
            .map(|(index, record)| broca_wal::FramedRecord {
                seq: index as u64 + 1,
                fence: 0,
                record,
            })
            .collect();
        let converted =
            broca_session_convert::convert(&framed, None).expect("the send's records convert");
        self.injected.lock().unwrap().extend(converted.records);
    }
}

/// A scripted provider that steers messages in while answering given calls.
struct SteeringProvider {
    inner: ScriptedProvider,
    calls: std::sync::atomic::AtomicUsize,
    /// `(call, action)`: during the provider's `call`th request (from 1), the actor does this.
    steers: Mutex<Vec<(usize, Action)>>,
    sink: Arc<dyn SteerSink>,
}

#[async_trait]
impl Provider for SteeringProvider {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let due: Vec<Action> = {
            let mut steers = self.steers.lock().unwrap();
            let (due, rest) = steers.drain(..).partition(|(at, _)| *at == call);
            *steers = rest;
            due.into_iter().map(|(_, action)| action).collect()
        };
        for (index, action) in due.into_iter().enumerate() {
            match action {
                Action::Steer(messages) => self.sink.steer(steered(call, index, messages)).await,
                Action::CancelAtBoundary => self.sink.cancel_at_boundary().await,
                Action::Stop => self.sink.stop(),
                Action::Ordered { send, records } => self.sink.ordered(*send, *records).await,
            }
        }
        self.inner.step(step_id, opts).await
    }
}

fn steered(call: usize, index: usize, messages: Vec<Message>) -> WalRecord {
    WalRecord::Steered {
        sent_by: None,
        written_at_ms: Some(0),
        retention: None,
        ts_ms: Some(0),
        send_id: Some(format!("steer-{call}-{index}")),
        request_identity_digest: None,
        delivery: Some(broca_protocol::Delivery::Steer),
        title: None,
        input: Arc::new(messages),
        hooks: None,
        tags: None,
    }
}

/// Runs `scenario` once through each loop, steering `steers` in while the provider answers,
/// and compares everything both did.
async fn compare_steered(scenario: Scenario, steers: Vec<(usize, Action)>) {
    compare_actions(scenario, steers, None).await;
}

/// Like `compare_steered`, with the run's restart policy set to `on_restart`. A run that pauses
/// for a restart is resumed once, as the runner does after it starts again, and both loops'
/// reports for each run are compared too.
async fn compare_actions(
    scenario: Scenario,
    steers: Vec<(usize, Action)>,
    on_restart: Option<OnRestart>,
) {
    // Today's loop, on a writer this test owns so its actor side can write steers.
    let dir = broca_test_support::scratch_root().join(format!(
        "session-loop-differential-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let wal = FileWalStore::new(dir.join("wal"));
    let mut request = scenario.request();
    request.on_restart = on_restart;
    let writer = Arc::new(WalHandle::spawn(wal.open(&request.session).unwrap(), 1).unwrap());
    let inbox = SteerInbox::new();
    let stop = CancellationToken::new();
    let provider = SteeringProvider {
        inner: ScriptedProvider::new(scenario.steps.clone()),
        calls: Default::default(),
        steers: Mutex::new(steers.clone()),
        sink: Arc::new(TodaySink {
            inbox: Arc::clone(&inbox),
            writer: Arc::clone(&writer),
            stop: stop.clone(),
        }),
    };
    let plane = scenario.plane();
    let (tx, mut rx) = mpsc::channel(512);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let cancel = CancellationToken::new();
    plane.cancel_on_hold(cancel.clone());
    let wal_dir = dir.join("wal");
    *plane.held.ended.lock().unwrap() = Some(Arc::new(move || {
        std::fs::read_dir(&wal_dir).is_ok_and(|entries| {
            entries.flatten().any(|entry| {
                std::fs::read(entry.path()).is_ok_and(|bytes| {
                    bytes
                        .windows(br#""type":"run_finished""#.len())
                        .any(|window| window == br#""type":"run_finished""#)
                })
            })
        })
    }));
    let control = RunControl {
        steer: Some(Arc::clone(&inbox)),
        stop_requested: Some(stop),
        provider_held_lifecycle: Some(plane.lifecycle()),
        cancel: Some(cancel),
        ..Default::default()
    };
    let first = run_loop_on_writer(
        request.clone(),
        &writer,
        StartMode::FreshOrResume,
        &provider,
        &plane,
        &AllowAll,
        &wal,
        None,
        control,
        &tx,
    )
    .await
    .expect("today's loop runs");
    let mut summaries = vec![(first.reason, first.paused)];
    if first.paused.is_some() {
        // A run paused on held calls resumes when their late results arrive; any other pause
        // resumes when the runner starts again. Either way it's the same run.
        if first.paused == Some(PauseReason::ToolHeld) {
            plane.held.deposit_all();
        }
        let second = run_loop_on_writer(
            request,
            &writer,
            StartMode::FreshOrResume,
            &provider,
            &plane,
            &AllowAll,
            &wal,
            None,
            RunControl {
                provider_held_lifecycle: Some(plane.lifecycle()),
                ..Default::default()
            },
            &tx,
        )
        .await
        .expect("today's loop resumes");
        summaries.push((second.reason, second.paused));
    }
    drop(tx);
    drain.await.unwrap();
    // The provider's sink holds the writer too; keep only what the comparison reads.
    let sent = Arc::clone(&provider.inner.sent);
    drop(provider);
    Arc::try_unwrap(writer)
        .ok()
        .expect("the run released the writer")
        .shutdown()
        .await
        .unwrap();
    let file = std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "wal"))
        .expect("today's loop wrote a log");
    let converted = {
        let image = LogImage::read(&file);
        broca_session_convert::convert(&image.framed(), lineage(&image).as_deref())
    }
    .expect("the log converts");
    let _ = std::fs::remove_dir_all(&dir);
    let today = Outcome {
        records: normalized(&converted.records),
        requests: sent.lock().unwrap().clone(),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .collect(),
    };

    // The new loop.
    let backend = MemoryBackend::default();
    let triple = SessionTriple {
        project_root: "/project".into(),
        harness: "differential".into(),
        session: "s1".into(),
    };
    let mut session = Session::create(
        &backend,
        SessionAddr("0123456789abcdef".into()),
        triple,
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    let injected: Arc<Mutex<Vec<Record>>> = Default::default();
    let stop = CancellationToken::new();
    let provider = SteeringProvider {
        inner: ScriptedProvider::new(scenario.steps.clone()),
        calls: Default::default(),
        steers: Mutex::new(steers),
        sink: Arc::new(NewSink {
            injected: Arc::clone(&injected),
            stop: stop.clone(),
        }),
    };
    let plane = scenario.plane();
    let mut writer = Recording {
        session: &mut session,
        records: Vec::new(),
        injected,
        ended: Default::default(),
    };
    let mut parts = MemoryParts::default();
    let now = || 0u64;
    let bind = bind();
    let seams = Seams {
        provider: &provider,
        tools: &plane,
        bind: bind.clone(),
        out: None,
        cancel: Default::default(),
        stop: stop.clone(),
        held: None,
        now_ms: &now,
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
    };
    let mut admission = scenario.admission(&provider.inner);
    admission.on_restart = on_restart;
    let lifecycle = plane.lifecycle();
    let ended = Arc::clone(&writer.ended);
    *plane.held.ended.lock().unwrap() = Some(Arc::new(move || {
        ended.load(std::sync::atomic::Ordering::SeqCst)
    }));
    let cancel = CancellationToken::new();
    plane.cancel_on_hold(cancel.clone());
    let seams = Seams {
        held: Some(&*lifecycle),
        cancel,
        ..seams
    };
    let first = finishes(run(&mut writer, &mut parts, admission, &seams))
        .await
        .expect("the new loop runs");
    let mut reports = vec![(first.reason, first.paused)];
    if first.paused.is_some() {
        if first.paused == Some(PauseReason::ToolHeld) {
            plane.held.deposit_all();
        }
        let restarted = Seams {
            stop: Default::default(),
            ..seams
        };
        let second = finishes(resume(&mut writer, &parts, &restarted))
            .await
            .expect("the new loop resumes");
        reports.push((second.reason, second.paused));
    }
    assert_eq!(reports, summaries, "how each run reported");
    keys_match(writer.session, &plane).await;
    let new = Outcome {
        records: normalized(&writer.records),
        requests: sent_requests(&provider.inner),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .collect(),
    };
    assert_same(&today, &new);
}

/// A loop run that doesn't finish within ten seconds fails the comparison rather than hanging
/// it, such as a run left waiting on a held call it should have paused for.
async fn finishes<F: std::future::Future>(run: F) -> F::Output {
    tokio::time::timeout(std::time::Duration::from_secs(10), run)
        .await
        .expect("the loop finished")
}

fn assert_same(today: &Outcome, new: &Outcome) {
    if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
        for (index, record) in today.records.iter().enumerate() {
            eprintln!("today {index}: {record}");
        }
        for (index, record) in new.records.iter().enumerate() {
            eprintln!("new   {index}: {record}");
        }
    }
    assert_eq!(new.requests.len(), today.requests.len(), "request count");
    for (index, (new, today)) in new.requests.iter().zip(&today.requests).enumerate() {
        assert_eq!(new, today, "request {index}");
    }
    for (index, (new, today)) in new.records.iter().zip(&today.records).enumerate() {
        assert_eq!(new, today, "record {index}");
    }
    assert_eq!(new.records.len(), today.records.len(), "record count");
    assert_eq!(new.calls, today.calls, "tool calls");
}

#[tokio::test]
async fn a_steer_during_a_tool_step_enters_at_the_next_boundary() {
    compare_steered(
        Scenario {
            input: vec![user("read a")],
            tools: vec![tool("read", ExecutionMode::Sequential)],
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done"),
            ],
        },
        vec![(1, Action::Steer(vec![user("also read b")]))],
    )
    .await;
}

#[tokio::test]
async fn a_steer_during_the_last_answer_takes_one_more_step() {
    compare_steered(
        Scenario {
            input: vec![user("hello")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![text_step("a1", "hi"), text_step("a2", "and again")],
        },
        vec![(1, Action::Steer(vec![user("one more thing")]))],
    )
    .await;
}

#[tokio::test]
async fn a_steer_after_a_limiting_stop_ends_with_the_run() {
    compare_steered(
        Scenario {
            input: vec![user("hello")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: vec![StopCondition::StepCount { n: 1 }],
            steps: vec![text_step("a1", "hi")],
        },
        vec![(1, Action::Steer(vec![user("too late"), user("and this")]))],
    )
    .await;
}

#[tokio::test]
async fn a_steer_during_a_step_that_hits_the_step_limit_ends_with_the_run() {
    compare_steered(
        Scenario {
            input: vec![user("read a")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: vec![StopCondition::StepCount { n: 1 }],
            steps: vec![answer(
                "a1",
                vec![call("c1", "read", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            )],
        },
        vec![(1, Action::Steer(vec![user("never mind")]))],
    )
    .await;
}

#[tokio::test]
async fn a_boundary_cancel_during_a_tool_step_ends_after_its_batch() {
    compare_steered(
        Scenario {
            input: vec![user("read a")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "never asked for"),
            ],
        },
        vec![(1, Action::CancelAtBoundary)],
    )
    .await;
}

#[tokio::test]
async fn a_boundary_cancel_during_the_last_answer_lets_it_complete() {
    compare_steered(
        Scenario {
            input: vec![user("hello")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![text_step("a1", "hi")],
        },
        vec![(1, Action::CancelAtBoundary)],
    )
    .await;
}

#[tokio::test]
async fn a_boundary_cancel_with_a_steer_ends_at_the_extra_boundary() {
    compare_steered(
        Scenario {
            input: vec![user("hello")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![text_step("a1", "hi"), text_step("a2", "never asked for")],
        },
        vec![
            (1, Action::Steer(vec![user("wait")])),
            (1, Action::CancelAtBoundary),
        ],
    )
    .await;
}

/// A provider step that fails because the credential needs signing in again.
fn auth_failure() -> Vec<NormalizedEvent> {
    vec![NormalizedEvent::Control(ProviderControl::Error {
        error: serde_json::from_value(json!({
            "class": "auth",
            "message": "sign in again",
            "status": 401
        }))
        .unwrap(),
        retries_used: 0,
    })]
}

/// Runs `scenario` once through each loop: the run pauses for re-authentication, then resumes
/// after it, as the session actor would once the credential is refreshed. Compares
/// everything both did.
async fn compare_auth_pause(scenario: Scenario) {
    // Today's loop, pausing and resuming on one writer.
    let dir = broca_test_support::scratch_root().join(format!(
        "session-loop-differential-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let wal = FileWalStore::new(dir.join("wal"));
    let request = scenario.request();
    let writer = WalHandle::spawn(wal.open(&request.session).unwrap(), 1).unwrap();
    let provider = ScriptedProvider::new(scenario.steps.clone());
    let plane = scenario.plane();
    let (tx, mut rx) = mpsc::channel(512);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let mut summaries = Vec::new();
    for mode in [StartMode::FreshOrResume, StartMode::ResumeAfterAuth] {
        let summary = run_loop_on_writer(
            request.clone(),
            &writer,
            mode,
            &provider,
            &plane,
            &AllowAll,
            &wal,
            None,
            RunControl::default(),
            &tx,
        )
        .await
        .expect("today's loop runs");
        summaries.push((summary.reason, summary.paused));
    }
    drop(tx);
    drain.await.unwrap();
    writer.shutdown().await.unwrap();
    let file = std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "wal"))
        .expect("today's loop wrote a log");
    let converted = {
        let image = LogImage::read(&file);
        broca_session_convert::convert(&image.framed(), lineage(&image).as_deref())
    }
    .expect("the log converts");
    let _ = std::fs::remove_dir_all(&dir);
    let today = Outcome {
        records: normalized(&converted.records),
        requests: sent_requests(&provider),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .collect(),
    };

    // The new loop.
    let backend = MemoryBackend::default();
    let triple = SessionTriple {
        project_root: "/project".into(),
        harness: "differential".into(),
        session: "s1".into(),
    };
    let mut session = Session::create(
        &backend,
        SessionAddr("0123456789abcdef".into()),
        triple,
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    let provider = ScriptedProvider::new(scenario.steps.clone());
    let plane = scenario.plane();
    let mut writer = Recording {
        session: &mut session,
        records: Vec::new(),
        injected: Default::default(),
        ended: Default::default(),
    };
    let mut parts = MemoryParts::default();
    let now = || 0u64;
    let bind = bind();
    let seams = Seams {
        provider: &provider,
        tools: &plane,
        bind: bind.clone(),
        out: None,
        cancel: Default::default(),
        stop: Default::default(),
        held: None,
        now_ms: &now,
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
    };
    let first = run(
        &mut writer,
        &mut parts,
        scenario.admission(&provider),
        &seams,
    )
    .await
    .expect("the new loop runs");
    let second = resume(&mut writer, &parts, &seams)
        .await
        .expect("the new loop resumes");
    keys_match(writer.session, &plane).await;
    let new = Outcome {
        records: normalized(&writer.records),
        requests: sent_requests(&provider),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .collect(),
    };
    assert_eq!(
        [(first.reason, first.paused), (second.reason, second.paused)].as_slice(),
        summaries.as_slice(),
        "how each run reported"
    );
    assert_same(&today, &new);
}

#[tokio::test]
async fn a_failed_sign_in_pauses_and_resumes_as_the_same_run() {
    compare_auth_pause(Scenario {
        input: vec![user("hello")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![auth_failure(), text_step("a1", "hi")],
    })
    .await;
}

#[tokio::test]
async fn a_failed_sign_in_after_a_tool_step_resumes_with_its_results() {
    compare_auth_pause(Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "read", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            auth_failure(),
            text_step("a2", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn a_stop_before_a_batch_refuses_it_and_ends_the_run() {
    compare_actions(
        Scenario {
            input: vec![user("read a")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "never asked for"),
            ],
        },
        vec![(1, Action::Steer(vec![user("also b")])), (1, Action::Stop)],
        Some(OnRestart::Resume),
    )
    .await;
}

#[tokio::test]
async fn a_stop_at_a_boundary_pauses_and_the_resume_places_the_steer() {
    compare_actions(
        Scenario {
            input: vec![user("hello")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![text_step("a1", "hi"), text_step("a2", "and again")],
        },
        vec![
            (1, Action::Steer(vec![user("one more")])),
            (1, Action::Stop),
        ],
        Some(OnRestart::Resume),
    )
    .await;
}

#[tokio::test]
async fn a_stop_at_a_boundary_without_a_resume_policy_ends_the_run() {
    compare_actions(
        Scenario {
            input: vec![user("hello")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![text_step("a1", "hi"), text_step("a2", "never asked for")],
        },
        vec![
            (1, Action::Steer(vec![user("one more")])),
            (1, Action::Stop),
        ],
        None,
    )
    .await;
}

#[tokio::test]
async fn a_cancel_wins_over_a_stop_at_a_boundary() {
    compare_actions(
        Scenario {
            input: vec![user("hello")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![text_step("a1", "hi"), text_step("a2", "never asked for")],
        },
        vec![
            (1, Action::Steer(vec![user("one more")])),
            (1, Action::Stop),
            (1, Action::CancelAtBoundary),
        ],
        Some(OnRestart::Resume),
    )
    .await;
}

/// One HTTP attempt's evidence, as a provider reports it when the attempt ends.
fn attempt(number: u32, status: u16, outcome: &str) -> NormalizedEvent {
    NormalizedEvent::Control(ProviderControl::AttemptFinished {
        attempt: serde_json::from_value(json!({
            "attempt": number,
            "request_body_len": 1200 + number,
            "request_body_sha256": format!("{:064x}", number),
            "http_status": status,
            "content_type": "text/event-stream",
            "response_body_len": 340,
            "response_body_sha256": format!("{:064x}", 1000 + number),
            "outcome": outcome,
            "ts_ms": 50,
            "first_content_at_ms": 20,
            "last_content_at_ms": 45,
            "max_content_gap_ms": 9,
            "content_bytes": 310,
            "sent_at_ms": 10,
            "transport": "https",
            "cache_evidence": {"read": 80, "written": 0},
        }))
        .unwrap(),
    })
}

/// `step` with these attempts reported before its terminal event.
fn attempted(
    mut step: Vec<NormalizedEvent>,
    attempts: Vec<NormalizedEvent>,
) -> Vec<NormalizedEvent> {
    let terminal = step.pop().expect("a step ends with its terminal");
    step.extend(attempts);
    step.push(terminal);
    step
}

#[tokio::test]
async fn each_attempt_is_recorded_as_it_ends() {
    compare(Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![
            attempted(
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                vec![
                    attempt(1, 529, "eof_before_completion"),
                    attempt(2, 200, "complete"),
                ],
            ),
            attempted(text_step("a2", "done"), vec![attempt(1, 200, "complete")]),
        ],
    })
    .await;
}

#[tokio::test]
async fn a_failed_steps_attempts_are_recorded_before_the_pause() {
    compare_auth_pause(Scenario {
        input: vec![user("hello")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![
            attempted(auth_failure(), vec![attempt(1, 401, "complete")]),
            attempted(text_step("a1", "hi"), vec![attempt(1, 200, "complete")]),
        ],
    })
    .await;
}

#[tokio::test]
async fn an_ordered_steer_enters_at_the_next_boundary() {
    compare_steered(
        Scenario {
            input: vec![user("read a")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done"),
            ],
        },
        vec![(
            1,
            ordered("q1", &["steer", "idle_start"], "also read b", None),
        )],
    )
    .await;
}

#[tokio::test]
async fn an_expired_ordered_send_ends_at_the_boundary() {
    compare_steered(
        Scenario {
            input: vec![user("read a")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done"),
            ],
        },
        vec![(1, ordered("q1", &["steer"], "too late", Some(0)))],
    )
    .await;
}

#[tokio::test]
async fn an_idle_start_send_waits_for_the_run_to_end() {
    compare_steered(
        Scenario {
            input: vec![user("read a")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done"),
            ],
        },
        vec![(1, ordered("q1", &["idle_start"], "later", None))],
    )
    .await;
}

#[tokio::test]
async fn an_ordered_steer_during_the_last_answer() {
    compare_steered(
        Scenario {
            input: vec![user("hello")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![text_step("a1", "hi"), text_step("a2", "and again")],
        },
        vec![(1, ordered("q1", &["steer"], "one more", None))],
    )
    .await;
}

#[tokio::test]
async fn ordered_sends_deliver_in_queue_order_after_steers() {
    compare_steered(
        Scenario {
            input: vec![user("read a")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done"),
            ],
        },
        vec![
            (1, ordered("q1", &["steer"], "first queued", None)),
            (1, Action::Steer(vec![user("a plain steer")])),
            (1, ordered("q2", &["idle_start"], "waits", None)),
            (1, ordered("q3", &["steer"], "second queued", None)),
        ],
    )
    .await;
}

#[tokio::test]
async fn a_held_call_answered_while_its_batch_runs() {
    compare_steered(
        Scenario {
            input: vec![user("run both")],
            tools: vec![
                tool("hold_brief", ExecutionMode::Parallel),
                tool("read", ExecutionMode::Parallel),
            ],
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![
                        call("c1", "hold_brief", json!({"job": 1})),
                        call("c2", "read", json!({"path": "a"})),
                    ],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done"),
            ],
        },
        Vec::new(),
    )
    .await;
}

#[tokio::test]
async fn a_run_pauses_on_a_held_call_and_resumes_with_its_late_result() {
    compare_steered(
        Scenario {
            input: vec![user("run both")],
            tools: vec![
                tool("hold", ExecutionMode::Parallel),
                tool("read", ExecutionMode::Parallel),
            ],
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![
                        call("c1", "hold", json!({"job": 1})),
                        call("c2", "read", json!({"path": "a"})),
                    ],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done"),
            ],
        },
        Vec::new(),
    )
    .await;
}

#[tokio::test]
async fn a_sequential_held_call_keeps_its_successor_unsent_until_resume() {
    compare_steered(
        Scenario {
            input: vec![user("run in order")],
            tools: vec![
                tool("hold", ExecutionMode::Sequential),
                tool("read", ExecutionMode::Sequential),
            ],
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![
                        call("c1", "hold", json!({"job": 1})),
                        call("c2", "read", json!({"path": "a"})),
                    ],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done"),
            ],
        },
        Vec::new(),
    )
    .await;
}

#[tokio::test]
async fn a_cancel_while_a_provider_holds_a_call_closes_it_and_withdraws_after_the_end() {
    compare_steered(
        Scenario {
            input: vec![user("run both")],
            tools: vec![
                tool("hold_cancel", ExecutionMode::Parallel),
                tool("read", ExecutionMode::Parallel),
            ],
            stop_when: Vec::new(),
            steps: vec![answer(
                "a1",
                vec![
                    call("c1", "hold_cancel", json!({"job": 1})),
                    call("c2", "read", json!({"path": "a"})),
                ],
                FinishReason::ToolCalls,
            )],
        },
        Vec::new(),
    )
    .await;
}

#[tokio::test]
async fn every_cut_of_a_held_call_resumes_alike() {
    compare_every_cut(Scenario {
        input: vec![user("run both")],
        tools: vec![
            tool("hold_brief", ExecutionMode::Parallel),
            tool("read", ExecutionMode::Parallel),
        ],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "hold_brief", json!({"job": 1})),
                    call("c2", "read", json!({"path": "a"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn every_cut_of_a_sequential_held_call_resumes_alike() {
    compare_every_cut(Scenario {
        input: vec![user("run in order")],
        tools: vec![
            tool("hold_brief", ExecutionMode::Sequential),
            tool("read", ExecutionMode::Sequential),
        ],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "hold_brief", json!({"job": 1})),
                    call("c2", "read", json!({"path": "a"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}
