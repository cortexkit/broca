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
use broca_core::tool_media::MediaCapture;
use broca_core::tool_plane::{
    BatchCtx, DispatchKey, DispatchRegistration, DispatchResult, ProviderHeldLifecycle,
    ProviderHeldReply, ToolOpenGuard, ToolPlane, ToolPlaneError,
};
use broca_core::{
    provider::Provider, run_loop_on_writer, run_loop_with_control, FileLeaseStore, FileWalStore,
    RunControl, RunRequest, StartMode, SteerInbox, StubReply, StubToolPlane, WalHandle, WalStore,
};
use broca_crash_harness::wal::LogImage;
use broca_protocol::caller::PauseReason;
use broca_protocol::content::{
    AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolOutputPart, ToolResult,
};
use broca_protocol::provider::{NormalizedEvent, ProviderControl};
use broca_protocol::request::{
    CallOptions, ExecutionMode, ModelHandle, OnRestart, StopCondition, Tool, ToolChoice,
};
use broca_protocol::{FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage};
use broca_session_loop::{resume, run, Admission, MemoryParts, PendingMedia, Seams, Writer};
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
        // A batch with a `down` call meets a tool provider that can't be reached.
        if calls.iter().any(|call| call.tool_name == "down") {
            return Err(ToolPlaneError::Unavailable("route closed".into()));
        }
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
        self.note(format!("dispatch {} {}", call.tool_call_id.0, call.input));
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
            "dispatch_keyed {} {} {token} recorder={}",
            call.tool_call_id.0,
            call.input,
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
            "dispatch_guarded {} {} {}",
            call.tool_call_id.0,
            call.input,
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
        if call.tool_name == "photo" {
            return photo(ctx, call).await;
        }
        // A result big enough that the next request goes over a small window.
        if call.tool_name == "big" {
            return DispatchResult::Completed(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Text {
                    text: "x".repeat(8_000),
                },
                is_error: false,
            });
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

/// The images the `photo` tool returns: a fixture photo and a 1×1 PNG.
const PHOTO: &[u8] = include_bytes!("../../broca-provider/examples/media-live/fixtures/image.png");
const PIXEL: &[u8] = &[
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
    0xde, 0x00, 0x00, 0x00, 0x0c, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0xf8, 0xcf, 0xc0, 0x00,
    0x00, 0x03, 0x01, 0x01, 0x00, 0xc9, 0xfe, 0x92, 0xef, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e,
    0x44, 0xae, 0x42, 0x60, 0x82,
];

/// The `photo` tool's answer: a text part and four images. The first two were captured for this
/// call, through the batch's capture; the third names the first's bytes with other metadata, and
/// the fourth names bytes nobody captured. So a loop must keep the first two and mark the others.
async fn photo(ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
    let mut captured = Vec::new();
    for bytes in [PHOTO, PIXEL] {
        let reference = match &ctx.media_capture {
            Some(capture) => capture
                .capture(
                    &ctx.batch_id,
                    &call.tool_call_id,
                    bytes.to_vec(),
                    "image/png".into(),
                )
                .await
                .ok(),
            None => None,
        };
        captured.push(reference.unwrap_or_else(|| {
            broca_core::media_validation::validate_media(bytes, "image/png").expect("a valid PNG")
        }));
    }
    let mut altered = captured[0].clone();
    altered.byte_size += 1;
    let mut foreign = captured[0].clone();
    foreign.sha256 = "0".repeat(64);
    let mut parts = vec![ToolOutputPart::Text {
        text: "a photo".into(),
    }];
    parts.extend(
        captured
            .into_iter()
            .chain([altered, foreign])
            .map(|media| ToolOutputPart::Media { media }),
    );
    DispatchResult::Completed(ToolResult {
        tool_call_id: call.tool_call_id.clone(),
        output: ToolOutput::Parts { parts },
        is_error: false,
    })
}

/// Every media part in the window reads back from the session as the bytes it names. Only the
/// `photo` tool returns media. (A resumed run's earlier records come from a converted log, whose
/// media the converter doesn't copy yet, so this runs on whole runs only.)
async fn media_reads_back(session: &Session<'_, MemoryBackend>) {
    for message in &session.conversation.messages {
        for block in &message.content {
            let ContentBlock::ToolResult(result) = block else {
                continue;
            };
            let ToolOutput::Parts { parts } = &result.output else {
                continue;
            };
            for part in parts {
                if let ToolOutputPart::Media { media } = part {
                    let bytes = session
                        .window_media(media)
                        .await
                        .unwrap_or_else(|error| panic!("{media:?} doesn't read back: {error:?}"));
                    assert!(bytes == PHOTO || bytes == PIXEL, "{media:?}");
                    assert_eq!(
                        broca_core::media_validation::validate_media(&bytes, &media.mime).as_ref(),
                        Ok(media),
                        "{media:?}"
                    );
                }
            }
        }
    }
}

/// A log's lineage id as the converter takes it, which today's call keys are made of.
fn lineage(image: &LogImage) -> Option<String> {
    image.lineage_id().map(|lineage| lineage.to_string())
}

/// The model a run of the comparison uses: `m`, or the one a run id names after an `@`
/// (`r2@m2`), so a scenario can change the model between runs.
fn run_model(run_id: &str) -> &str {
    run_id.split_once('@').map_or("m", |(_, model)| model)
}

tokio::task_local! {
    /// The compaction provider a comparison's plan names, scripted: what it answers each ride
    /// and the window the run freezes. `None` runs without one.
    static COMPACTION: Option<Arc<CompactionScript>>;
}

/// The compaction script the comparison running in this task uses.
fn compaction() -> Option<Arc<CompactionScript>> {
    COMPACTION.try_with(Clone::clone).ok().flatten()
}

struct CompactionScript {
    context_limit: u64,
    /// What each ride is answered, in order; once they run out, `nothing`.
    rides: Vec<Ride>,
}

#[derive(Clone, Copy)]
enum Ride {
    /// A view that drops the whole entry at this ordinal.
    Drop(u64),
    /// A view that cuts through the entry at this ordinal: it and everything before it no
    /// longer render.
    Cut(u64),
}

/// A compaction provider that answers from a script and records every call, so both loops must
/// make the same calls. Message ids differ between the loops by design, so the provider learns
/// them from the entries it's told about, and a view names entries by those ids.
struct CompactionSpy {
    rides: Mutex<VecDeque<Ride>>,
    mids: Mutex<std::collections::BTreeMap<u64, String>>,
    version: std::sync::atomic::AtomicU64,
    calls: Mutex<Vec<(String, Value)>>,
}

impl CompactionSpy {
    fn new(script: &CompactionScript) -> Self {
        Self {
            rides: Mutex::new(script.rides.iter().copied().collect()),
            mids: Mutex::default(),
            version: Default::default(),
            calls: Mutex::default(),
        }
    }

    /// The calls made, in order, each with message ids, digests and checksums renamed by order of
    /// first appearance and times blanked, so two loops' calls compare equal exactly when they
    /// say the same thing.
    fn calls(&self) -> Vec<String> {
        let mut names = std::collections::HashMap::new();
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(method, params)| {
                let mut params = params.clone();
                rename(&mut params, &mut names);
                format!("{method} {params}")
            })
            .collect()
    }
}

/// Renames, in `value`, every message id, digest and checksum by its order of first appearance,
/// and blanks every time.
fn rename(value: &mut Value, names: &mut std::collections::HashMap<String, usize>) {
    match value {
        Value::Object(members) => {
            for (key, member) in members.iter_mut() {
                match (key.as_str(), &member) {
                    ("now" | "at_ms" | "written_at" | "completed_at", _) => {
                        *member = json!("<time>");
                    }
                    (
                        "mid" | "through" | "first_kept_mid" | "digest" | "checksum",
                        Value::String(text),
                    ) => {
                        let next = names.len();
                        let name = *names.entry(text.clone()).or_insert(next);
                        *member = json!(format!("<{key}#{name}>"));
                    }
                    _ => rename(member, names),
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|item| rename(item, names)),
        _ => {}
    }
}

/// Every `(ordinal, mid)` pair an entry in `value` names.
fn entry_mids(value: &Value, out: &mut std::collections::BTreeMap<u64, String>) {
    match value {
        Value::Object(members) => {
            if let (Some(ordinal), Some(mid)) = (
                members.get("ordinal").and_then(Value::as_u64),
                members.get("mid").and_then(Value::as_str),
            ) {
                out.insert(ordinal, mid.to_owned());
            }
            members.values().for_each(|member| entry_mids(member, out));
        }
        Value::Array(items) => items.iter().for_each(|item| entry_mids(item, out)),
        _ => {}
    }
}

#[async_trait::async_trait]
impl broca_core::CompactionPlane for CompactionSpy {
    async fn setup(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
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
        _: &broca_protocol::BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        params: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        entry_mids(&params, &mut self.mids.lock().unwrap());
        self.calls.lock().unwrap().push((method.into(), params));
        let answer = match method {
            "compaction.setup" => json!({"answer": "ready"}),
            "compaction.notice" => json!({"answer": "received"}),
            "compaction.ride" => match self.rides.lock().unwrap().pop_front() {
                None => json!({"answer": "nothing"}),
                Some(ride) => {
                    use cortexkit_role_compaction::view::{Edit, EditOp, View};
                    let (Ride::Drop(ordinal) | Ride::Cut(ordinal)) = ride;
                    let mid = self.mids.lock().unwrap()[&ordinal].clone();
                    let version = 1 + self
                        .version
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let op = match ride {
                        Ride::Drop(_) => EditOp::Drop { mid, part: None },
                        Ride::Cut(_) => EditOp::Cut { through: mid },
                    };
                    let view = View::seal(version, vec![Edit::new(version, op)]);
                    json!({"answer": "view", "view": view})
                }
            },
            other => panic!("unexpected compaction method {other}"),
        };
        Ok(serde_json::value::to_raw_value(&answer).expect("an answer serializes"))
    }
}

tokio::task_local! {
    /// The layout a comparison's session starts under: the current compaction contract (a plan
    /// version), or the established layout (`None`). Every comparison runs once in each.
    static LAYOUT: Option<u32>;
}

/// The layout the comparison running in this task uses.
fn layout() -> Option<u32> {
    LAYOUT.try_with(|layout| *layout).unwrap_or(None)
}

/// Both layouts a new session can start under. Today's actor starts a session under the
/// current compaction contract only when its plan names a compaction provider, but a later run
/// may drop it, so a session on the contract can run without one too.
const LAYOUTS: [Option<u32>; 2] = [None, Some(cortexkit_compaction_view::PLAN_VERSION)];

/// One scenario: what the caller sends and what the provider and tools answer.
#[derive(Clone)]
struct Scenario {
    input: Vec<Message>,
    tools: Vec<Tool>,
    stop_when: Vec<StopCondition>,
    /// The session's plan manifest, for a scenario admitted with a fetch plan.
    manifest: Option<Arc<broca_wal::PlanManifest>>,
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
            model: ModelHandle::new("stub", run_model(run_id)),
            input: input.to_vec(),
            tools: self.tools.clone(),
            policy_allowed: self.tools.iter().map(|t| t.name.clone()).collect(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            stop_when: self.stop_when.clone(),
            submission_id: None,
            budget: None,
            context_limit: compaction().map(|script| script.context_limit),
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
            model: ModelHandle::new("stub", run_model(run_id)),
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
            manifest: self.manifest.clone(),
            global_system: Vec::new(),
            compaction_plan_version: layout(),
            context_limit: compaction().map(|script| script.context_limit),
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
    let spy = compaction().map(|script| Arc::new(CompactionSpy::new(&script)));
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let cancel = CancellationToken::new();
    run_loop_with_control(
        scenario.request(),
        &provider,
        &plane,
        &lease,
        &HarnessPolicy,
        &wal,
        None,
        RunControl {
            provider_held_lifecycle: Some(plane.lifecycle()),
            manifest: scenario.manifest.clone(),
            cancel: Some(cancel.clone()),
            hook_plane: Some(harness_hooks_cancelling(cancel)),
            new_log_compaction_plan_version: layout(),
            compaction_plane: spy
                .clone()
                .map(|spy| spy as Arc<dyn broca_core::CompactionPlane>),
            admitted_context_limit: compaction().map(|script| script.context_limit),
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
                .chain(spy.map_or_else(Vec::new, |spy| spy.calls()))
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
    /// The media the tool plane captured, stored when the result naming it is appended.
    media: Arc<PendingMedia>,
}

impl Recording<'_, '_> {
    async fn flush(&mut self) -> Result<(), SessionError> {
        let pending: Vec<Record> = std::mem::take(&mut *self.injected.lock().unwrap());
        if !pending.is_empty() {
            self.records.extend(pending.iter().cloned());
            self.media.append(self.session, &pending, true).await?;
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl Writer for Recording<'_, '_> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        self.flush().await?;
        self.records.extend_from_slice(records);
        let seq = self.media.append(self.session, records, barrier).await?;
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

    fn message_id_prefix(&self) -> String {
        self.session.message_id_prefix()
    }

    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        self.session.open_log_records().await
    }

    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        Writer::run_start(&*self.session, run_id).await
    }

    async fn seal_at_entry(
        &mut self,
        ordinal: u64,
        carry: &[Record],
        now_ms: u64,
    ) -> Result<(), SessionError> {
        Writer::seal_at_entry(&mut *self.session, ordinal, carry, now_ms).await
    }

    async fn entry_by_mid(
        &self,
        mid: &str,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        Writer::entry_by_mid(&*self.session, mid).await
    }
    fn media(&self) -> Option<Arc<dyn MediaCapture>> {
        Some(Arc::clone(&self.media) as Arc<dyn MediaCapture>)
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
    // The records the session already held, so the appended ones' message ids number on from
    // theirs, as in today's whole log.
    let mut held = Vec::new();
    let resuming = match start {
        Some(converted)
            if converted
                .records
                .iter()
                .any(|r| matches!(r, Record::RunStart(_))) =>
        {
            session.append(&converted.records, true).await.unwrap();
            held = converted.records;
            parts.parts = converted.parts;
            true
        }
        _ => false,
    };
    let spy = compaction().map(|script| Arc::new(CompactionSpy::new(&script)));
    let provider = ScriptedProvider::new(steps);
    let plane = scenario.plane();
    let lifecycle = plane.lifecycle();
    let mut writer = Recording {
        session: &mut session,
        records: Vec::new(),
        injected: Default::default(),
        ended: Default::default(),
        media: Default::default(),
    };
    let now = || 0u64;
    let cancel = CancellationToken::new();
    let seams = Seams {
        provider: &provider,
        tools: &plane,
        bind: bind.clone(),
        out: None,
        cancel: cancel.clone(),
        stop: Default::default(),
        held: Some(&*lifecycle),
        approval: &HarnessPolicy,
        hook_plane: Some(harness_hooks_cancelling(cancel)),
        now_ms: &now,
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
        compaction_plane: spy
            .as_deref()
            .map(|spy| spy as &dyn broca_core::CompactionPlane),
        compaction_session: None,
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
    if !resuming {
        media_reads_back(writer.session).await;
    }
    let all: Vec<Record> = held.iter().chain(&records).cloned().collect();
    Outcome {
        records: normalized(&all)[held.len()..].to_vec(),
        requests: sent_requests(&provider),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .chain(spy.map_or_else(Vec::new, |spy| spy.calls()))
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

/// Records as the comparison reads them. Message ids differ by design: today's are
/// `m<lineage prefix>.<position>`, the new loop's `g<generation>.<seq>`, and a compaction view's
/// checksum covers the ids its edits name. So each id, anywhere in a record, becomes its order of
/// first appearance, and each view checksum likewise; two outcomes compare equal only if their
/// ids and views line up one to one.
fn normalized(records: &[Record]) -> Vec<Value> {
    let mut names: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    records
        .iter()
        .map(|record| {
            let mut value = serde_json::to_value(record).expect("records serialize");
            scrub(&mut value);
            rename_ids(&mut value, &mut names);
            json!({ record.name(): value })
        })
        .collect()
}

/// Renames, anywhere in `value`, every message id and view checksum by its order of first
/// appearance across the records.
fn rename_ids(value: &mut Value, names: &mut std::collections::HashMap<String, usize>) {
    match value {
        Value::Object(members) => {
            for (key, member) in members.iter_mut() {
                match (key.as_str(), member.as_str()) {
                    ("mid" | "first_kept_mid" | "through", Some(text)) => {
                        let next = names.len();
                        *member = json!(format!(
                            "mid#{}",
                            *names.entry(text.to_owned()).or_insert(next)
                        ));
                    }
                    ("checksum", Some(text)) => {
                        let next = names.len();
                        *member = json!(format!(
                            "checksum#{}",
                            *names.entry(text.to_owned()).or_insert(next)
                        ));
                    }
                    _ => rename_ids(member, names),
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|item| rename_ids(item, names)),
        _ => {}
    }
}

/// A scenario whose provider cuts: the new loop sealed the history before the cut, so the window
/// no longer starts at the first message, and a fresh load of the session holds exactly the
/// state the writer holds and rebuilds both compaction view slots from the new open log alone.
async fn sealed_at_the_cut(
    session: &Session<'_, MemoryBackend>,
    triple: &SessionTriple,
    bind: &broca_protocol::BindIdentity,
) {
    use broca_session_store::codec::Message as _;
    assert!(
        Writer::conversation(session).window_start() > 1,
        "the new loop sealed nothing at the cut"
    );
    let loaded = Session::load(
        session.backend(),
        SessionAddr("0123456789abcdef".into()),
        triple,
        &DeploymentKey::new(1, &[7; 32]),
    )
    .await
    .expect("the sealed session loads");
    assert_eq!(
        Writer::conversation(&loaded).encode(),
        Writer::conversation(session).encode()
    );
    assert_eq!(
        Writer::operations(&loaded).encode(),
        Writer::operations(session).encode()
    );
    let records = Writer::open_log_records(&loaded)
        .await
        .expect("the open log reads");
    let state =
        broca_session_loop::compaction::restore(&records, bind, Writer::operations(&loaded))
            .expect("the carried views rebuild the compaction state");
    assert!(state.applied().is_some(), "the applied view was carried");
}

/// A request as every renderer reads it. A store that seals history keeps only the runs whose
/// settings still shape a request, so each side's runs are reduced the way the provider crate
/// proves renders the same bytes (`reduce_episodes`, from the first message the request still
/// renders); everything else compares as sent.
fn as_rendered(opts: &CallOptions) -> CallOptions {
    let first_kept = opts
        .prompt
        .iter()
        .filter_map(|message| message.ordinal)
        .min()
        .unwrap_or(0) as usize;
    let mut opts = opts.clone();
    opts.render_episodes = Arc::new(
        broca_provider::families::openai_responses_chatgpt::reduce_episodes(
            &opts.render_episodes,
            first_kept,
        ),
    );
    opts
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

/// Runs [`compare_in`] in both layouts.
async fn compare(scenario: Scenario) {
    for layout in LAYOUTS {
        LAYOUT
            .scope(layout, Box::pin(compare_in(scenario.clone())))
            .await;
    }
}

async fn compare_in(scenario: Scenario) {
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
        assert_eq!(as_rendered(new), as_rendered(today), "request {index}");
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
        manifest: None,
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
        manifest: None,
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
        manifest: None,
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
        manifest: None,
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
        manifest: None,
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
        manifest: None,
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
        manifest: None,
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
/// Runs [`compare_every_cut_in`] in both layouts.
async fn compare_every_cut(scenario: Scenario) {
    for layout in LAYOUTS {
        LAYOUT
            .scope(layout, Box::pin(compare_every_cut_in(scenario.clone())))
            .await;
    }
}

async fn compare_every_cut_in(scenario: Scenario) {
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
        manifest: None,
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
        manifest: None,
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

/// A tool that returns two images it captured, and two it didn't: the captured ones reach the
/// record, the session's storage and the next request; the other two become provenance markers.
fn photo_run() -> Scenario {
    Scenario {
        input: vec![user("take a photo")],
        tools: vec![tool("photo", ExecutionMode::Parallel)],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "photo", json!({}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "nice"),
        ],
    }
}

#[tokio::test]
async fn a_tool_result_keeps_only_media_captured_for_its_call() {
    compare(photo_run()).await;
}

#[tokio::test]
async fn every_cut_of_a_photo_run_resumes_alike() {
    compare_every_cut(photo_run()).await;
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
        manifest: None,
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
        manifest: None,
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
        manifest: None,
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
        manifest: None,
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
/// Runs [`compare_runs_in`] in both layouts.
async fn compare_runs(scenario: Scenario, runs: &[(&str, Vec<Message>)]) {
    for layout in LAYOUTS {
        LAYOUT
            .scope(layout, Box::pin(compare_runs_in(scenario.clone(), runs)))
            .await;
    }
}

async fn compare_runs_in(scenario: Scenario, runs: &[(&str, Vec<Message>)]) {
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
    let spy = compaction().map(|script| Arc::new(CompactionSpy::new(&script)));
    // One compaction session state for the session's life, as the actor keeps it across runs.
    let compaction_session: Arc<broca_core::compaction_v2::session::Session> = Arc::default();
    let control = || RunControl {
        manifest: scenario.manifest.clone(),
        compaction_session: Some(Arc::clone(&compaction_session)),
        new_log_compaction_plan_version: layout(),
        compaction_plane: spy
            .clone()
            .map(|spy| spy as Arc<dyn broca_core::CompactionPlane>),
        admitted_context_limit: compaction().map(|script| script.context_limit),
        ..Default::default()
    };
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    for (index, (run_id, input)) in runs.iter().enumerate() {
        let (tx, mut rx) = mpsc::channel(512);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let request = scenario.request_for(run_id, input);
        if index == 0 {
            run_loop_with_control(
                request,
                &provider,
                &plane,
                &lease,
                &HarnessPolicy,
                &wal,
                None,
                control(),
                &tx,
            )
            .await
            .expect("today's first run");
        } else {
            broca_core::run_loop_append_episode_with_control(
                request,
                &provider,
                &plane,
                &lease,
                &HarnessPolicy,
                &wal,
                None,
                control(),
                &tx,
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
            .chain(spy.map_or_else(Vec::new, |spy| spy.calls()))
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
        triple.clone(),
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    let provider = ScriptedProvider::new(scenario.steps.clone());
    let plane = scenario.plane();
    let spy = compaction().map(|script| Arc::new(CompactionSpy::new(&script)));
    let mut writer = Recording {
        session: &mut session,
        records: Vec::new(),
        injected: Default::default(),
        ended: Default::default(),
        media: Default::default(),
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
        approval: &HarnessPolicy,
        hook_plane: Some(harness_hooks()),
        now_ms: &now,
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
        compaction_plane: spy
            .as_deref()
            .map(|spy| spy as &dyn broca_core::CompactionPlane),
        // One session state for the session's life, as the actor keeps it across runs.
        compaction_session: Some(Arc::default()),
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
    if compaction()
        .is_some_and(|script| script.rides.iter().any(|ride| matches!(ride, Ride::Cut(_))))
    {
        sealed_at_the_cut(writer.session, &triple, &bind).await;
    }
    let new = Outcome {
        records: normalized(&writer.records),
        requests: sent_requests(&provider),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .chain(spy.map_or_else(Vec::new, |spy| spy.calls()))
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
        assert_eq!(as_rendered(new), as_rendered(today), "request {index}");
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
            manifest: None,
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
/// Runs [`compare_actions_in`] in both layouts.
async fn compare_actions(
    scenario: Scenario,
    steers: Vec<(usize, Action)>,
    on_restart: Option<OnRestart>,
) {
    for layout in LAYOUTS {
        LAYOUT
            .scope(
                layout,
                Box::pin(compare_actions_in(
                    scenario.clone(),
                    steers.clone(),
                    on_restart,
                )),
            )
            .await;
    }
}

async fn compare_actions_in(
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
        new_log_compaction_plan_version: layout(),
        ..Default::default()
    };
    let first = run_loop_on_writer(
        request.clone(),
        &writer,
        StartMode::FreshOrResume,
        &provider,
        &plane,
        &HarnessPolicy,
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
            &HarnessPolicy,
            &wal,
            None,
            RunControl {
                provider_held_lifecycle: Some(plane.lifecycle()),
                new_log_compaction_plan_version: layout(),
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
        media: Default::default(),
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
        approval: &HarnessPolicy,
        hook_plane: Some(harness_hooks()),
        now_ms: &now,
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
        compaction_plane: None,
        compaction_session: None,
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
        assert_eq!(as_rendered(new), as_rendered(today), "request {index}");
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
/// Runs [`compare_auth_pause_in`] in both layouts.
async fn compare_auth_pause(scenario: Scenario) {
    for layout in LAYOUTS {
        LAYOUT
            .scope(layout, Box::pin(compare_auth_pause_in(scenario.clone())))
            .await;
    }
}

async fn compare_auth_pause_in(scenario: Scenario) {
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
            &HarnessPolicy,
            &wal,
            None,
            RunControl {
                new_log_compaction_plan_version: layout(),
                ..Default::default()
            },
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
        media: Default::default(),
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
        approval: &HarnessPolicy,
        hook_plane: Some(harness_hooks()),
        now_ms: &now,
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
        compaction_plane: None,
        compaction_session: None,
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
        manifest: None,
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
        manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
        manifest: None,
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
        manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
            manifest: None,
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
        manifest: None,
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
        manifest: None,
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
async fn a_call_to_a_tool_the_run_was_not_given() {
    compare(Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "delete", json!({"path": "a"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn the_same_call_id_twice_in_one_message() {
    compare(Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    call("c1", "read", json!({"path": "b"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn a_batch_the_tool_plane_refuses() {
    compare(Scenario {
        input: vec![user("read a")],
        tools: vec![
            tool("read", ExecutionMode::Parallel),
            tool("down", ExecutionMode::Parallel),
        ],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "down", json!({})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}

/// The approval policy both loops run under, keyed by tool name so a scenario picks it by the
/// tool it calls: `forbidden` is denied, `narrow` has its input narrowed to `{"path":"narrowed"}`,
/// and every other call is allowed.
struct HarnessPolicy;

#[async_trait::async_trait]
impl broca_core::capability::ApprovalPolicy for HarnessPolicy {
    async fn decide(&self, call: &ToolCall) -> broca_protocol::approval::ApprovalResponse {
        use broca_protocol::approval::ApprovalResponse;
        let tool_call_id = call.tool_call_id.clone();
        match call.tool_name.as_str() {
            "forbidden" => ApprovalResponse::Deny {
                tool_call_id,
                reason: "denied by the harness policy".into(),
            },
            "narrow" => ApprovalResponse::AllowNarrowed {
                tool_call_id,
                input: json!({"path": "narrowed"}),
            },
            _ => ApprovalResponse::Allow { tool_call_id },
        }
    }
}

#[tokio::test]
async fn the_approval_policy_denies_and_narrows() {
    compare(Scenario {
        input: vec![user("read a")],
        tools: vec![
            tool("read", ExecutionMode::Parallel),
            tool("forbidden", ExecutionMode::Parallel),
            tool("narrow", ExecutionMode::Parallel),
        ],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "forbidden", json!({"path": "a"})),
                    call("c3", "narrow", json!({"path": "everything"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn every_cut_of_a_narrowed_call_resumes_alike() {
    compare_every_cut(Scenario {
        input: vec![user("read a")],
        tools: vec![
            tool("narrow", ExecutionMode::Sequential),
            tool("forbidden", ExecutionMode::Sequential),
        ],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "narrow", json!({"path": "everything"})),
                    call("c2", "forbidden", json!({"path": "a"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn a_narrowed_call_queued_behind_a_held_call_goes_out_narrowed() {
    compare_steered(
        Scenario {
            input: vec![user("run in order")],
            tools: vec![
                tool("hold", ExecutionMode::Sequential),
                tool("narrow", ExecutionMode::Sequential),
            ],
            manifest: None,
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![
                        call("c1", "hold", json!({"job": 1})),
                        call("c2", "narrow", json!({"path": "everything"})),
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
async fn every_cut_of_a_narrowed_call_behind_a_held_call_resumes_alike() {
    compare_every_cut(Scenario {
        input: vec![user("run in order")],
        tools: vec![
            tool("hold_brief", ExecutionMode::Sequential),
            tool("narrow", ExecutionMode::Sequential),
        ],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "hold_brief", json!({"job": 1})),
                    call("c2", "narrow", json!({"path": "everything"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}

/// A plan manifest as a fetch plan freezes it: one tool provider that answered with `tools`,
/// and a system text.
fn planned_manifest(tools: &[Tool]) -> Arc<broca_wal::PlanManifest> {
    let tools = Arc::new(tools.to_vec());
    Arc::new(broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(
            json!({"tool_items": [{"provider": "provider"}]}).to_string(),
        )
        .unwrap(),
        plan_identity: "plan-1".into(),
        composition_digest: "composition-1".into(),
        fetches: vec![broca_wal::ProviderFetch {
            provider: "provider".into(),
            outcome: broca_wal::FetchOutcome::Answered {
                catalog_digest: "catalog-1".into(),
                tools: Arc::clone(&tools),
                text: None,
                result_ops: None,
                reply: None,
                declarations_frozen: None,
                reference_slots: None,
            },
        }],
        tools,
        system_text: "You work in a planned session.".into(),
        system_digest: "system-1".into(),
        join_version: 1,
        role_versions: [("tool-provider".to_string(), "v1".to_string())].into(),
        declarations: Vec::new(),
    })
}

fn planned_scenario() -> Scenario {
    let tools = vec![tool("read", ExecutionMode::Parallel)];
    Scenario {
        input: vec![user("read a")],
        manifest: Some(planned_manifest(&tools)),
        tools,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "read", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    }
}

#[tokio::test]
async fn a_run_admitted_with_a_fetch_plan() {
    compare(planned_scenario()).await;
}

#[tokio::test]
async fn every_cut_of_a_run_admitted_with_a_fetch_plan_resumes_alike() {
    compare_every_cut(planned_scenario()).await;
}

/// The hook provider both loops call, keyed by the tool a call names: the mutate hook rewrites
/// a `mutated` call's input to `{"path":"mutated"}`, the validate hook denies a `vetoed` call,
/// the provider is unavailable for a `hook_down` call, and every other call passes.
///
/// Its post-assistant hook cancels the run (through `cancel`, the loop's immediate cancel) when
/// the answer's text says "cancel me": a caller's cancel landing after the model's answer is
/// complete and before its tool calls are sent, at the same point in both loops.
struct HarnessHooks {
    cancel: Option<CancellationToken>,
}

#[async_trait::async_trait]
impl broca_core::hooks::HookPlane for HarnessHooks {
    async fn hook(
        &self,
        _provider: &str,
        _bind: &broca_protocol::BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
        call: cortexkit_role_step_transform_provider::hook::HookCall,
    ) -> Result<
        cortexkit_role_step_transform_provider::answer::HookAnswer,
        broca_core::hooks::HookUnavailable,
    > {
        use cortexkit_role_step_transform_provider::answer::HookAnswer;
        use cortexkit_role_step_transform_provider::hook::Subject;
        use cortexkit_role_step_transform_provider::subscription::Phase;
        if let Subject::PostAssistant { blocks, .. } = &call.subject {
            if blocks
                .first()
                .is_some_and(|text| text.contains("cancel me"))
            {
                if let Some(cancel) = &self.cancel {
                    cancel.cancel();
                }
                return Ok(HookAnswer::Pass);
            }
            if !blocks
                .first()
                .is_some_and(|text| text.contains("review me"))
            {
                return Ok(HookAnswer::Pass);
            }
            return Ok(HookAnswer::Ops {
                ops: vec![serde_json::from_value(
                    json!({"op": "append", "block": 0, "text": " [reviewed by the harness hook]"}),
                )
                .unwrap()],
            });
        }
        if let Subject::PostTool { tool, .. } = &call.subject {
            return match tool.as_str() {
                "post_down" => Err(broca_core::hooks::HookUnavailable),
                _ => Ok(HookAnswer::Ops {
                    ops: vec![serde_json::from_value(
                        json!({"op": "append", "block": 0, "text": " [checked by the harness hook]"}),
                    )
                    .unwrap()],
                }),
            };
        }
        let Subject::PreTool { tool, phase, .. } = &call.subject else {
            return Ok(HookAnswer::Pass);
        };
        match (tool.as_str(), phase) {
            ("hook_down", _) => Err(broca_core::hooks::HookUnavailable),
            ("mutated", Phase::Mutate) => Ok(HookAnswer::Mutate {
                input: json!({"path": "mutated"}),
                note: Some("rewritten by the harness hook".into()),
            }),
            ("vetoed", Phase::Validate) => Ok(HookAnswer::Deny {
                text: "vetoed by the harness hook".into(),
            }),
            _ => Ok(HookAnswer::Pass),
        }
    }
}

fn harness_hooks() -> Arc<dyn broca_core::hooks::HookPlane> {
    Arc::new(HarnessHooks { cancel: None })
}

/// The hook provider, able to cancel the loop that `cancel` belongs to.
fn harness_hooks_cancelling(cancel: CancellationToken) -> Arc<dyn broca_core::hooks::HookPlane> {
    Arc::new(HarnessHooks {
        cancel: Some(cancel),
    })
}

/// A planned manifest whose one hook provider subscribes to the pre-tool mutate phase (passing
/// when unavailable) and validate phase (refusing when unavailable), and to post-tool hooks for
/// `read` (passing when unavailable) and `post_down` (refusing when unavailable).
fn hooked_manifest(tools: &[Tool]) -> Arc<broca_wal::PlanManifest> {
    let subscriptions = json!([
        {"hook": "pre_tool", "phase": "mutate", "ops": [], "on_unavailable": "pass", "budget_ms": 500},
        {"hook": "pre_tool", "phase": "validate", "ops": [], "on_unavailable": "refuse", "budget_ms": 500},
        {"hook": "post_tool", "tools": ["read"], "ops": ["append", "prepend", "replace"], "on_unavailable": "pass", "budget_ms": 500},
        {"hook": "post_tool", "tools": ["post_down"], "ops": ["append", "prepend", "replace"], "on_unavailable": "refuse", "budget_ms": 500},
        {"hook": "post_assistant", "ops": ["append", "prepend"], "on_unavailable": "pass", "budget_ms": 500},
    ]);
    let mut manifest = (*planned_manifest(tools)).clone();
    manifest.plan = broca_wal::OpaqueJson::from_json_text(
        json!({
            "tool_items": [{"provider": "provider"}],
            "step_transform_items": [
                {"provider": "p0", "preset": "default", "params": {}, "subscriptions": subscriptions}
            ],
        })
        .to_string(),
    )
    .unwrap();
    manifest.declarations = vec![broca_wal::FrozenDeclaration {
        provider: "p0".into(),
        answer: broca_wal::OpaqueJson::from_json_text(
            json!({"subscriptions": subscriptions}).to_string(),
        )
        .unwrap(),
    }];
    Arc::new(manifest)
}

fn hooked_scenario() -> Scenario {
    let tools = vec![
        tool("read", ExecutionMode::Parallel),
        tool("mutated", ExecutionMode::Parallel),
        tool("vetoed", ExecutionMode::Parallel),
        tool("hook_down", ExecutionMode::Parallel),
    ];
    Scenario {
        input: vec![user("read a")],
        manifest: Some(hooked_manifest(&tools)),
        tools,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "mutated", json!({"path": "b"})),
                    call("c3", "vetoed", json!({"path": "c"})),
                    call("c4", "hook_down", json!({"path": "d"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    }
}

#[tokio::test]
async fn pre_tool_hooks_mutate_deny_and_refuse_when_unavailable() {
    compare(hooked_scenario()).await;
}

#[tokio::test]
async fn every_cut_of_a_run_with_pre_tool_hooks_resumes_alike() {
    compare_every_cut(hooked_scenario()).await;
}

fn post_tool_scenario(mode: ExecutionMode) -> Scenario {
    let tools = vec![tool("read", mode), tool("post_down", mode)];
    Scenario {
        input: vec![user("read a")],
        manifest: Some(hooked_manifest(&tools)),
        tools,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "post_down", json!({"path": "b"})),
                    call("c3", "read", json!({"path": "c"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    }
}

#[tokio::test]
async fn post_tool_hooks_append_and_withhold_when_unavailable() {
    compare(post_tool_scenario(ExecutionMode::Parallel)).await;
}

#[tokio::test]
async fn sequential_post_tool_hooks_run_before_the_next_call() {
    compare(post_tool_scenario(ExecutionMode::Sequential)).await;
}

#[tokio::test]
async fn every_cut_of_a_run_with_post_tool_hooks_resumes_alike() {
    compare_every_cut(post_tool_scenario(ExecutionMode::Parallel)).await;
}

#[tokio::test]
async fn every_cut_of_a_sequential_run_with_post_tool_hooks_resumes_alike() {
    compare_every_cut(post_tool_scenario(ExecutionMode::Sequential)).await;
}

fn post_assistant_scenario() -> Scenario {
    let tools = vec![tool("read", ExecutionMode::Parallel)];
    Scenario {
        input: vec![user("read a")],
        manifest: Some(hooked_manifest(&tools)),
        tools,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "read", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done, review me"),
        ],
    }
}

#[tokio::test]
async fn post_assistant_hooks_edit_the_answer() {
    compare(post_assistant_scenario()).await;
}

#[tokio::test]
async fn every_cut_of_a_run_with_post_assistant_hooks_resumes_alike() {
    compare_every_cut(post_assistant_scenario()).await;
}

fn repeated_failure_scenario() -> Scenario {
    Scenario {
        input: vec![user("find x")],
        manifest: None,
        tools: vec![tool("grep", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "grep", json!({"pattern": "x"}))],
                FinishReason::ToolCalls,
            ),
            answer(
                "a2",
                vec![call("c2", "grep", json!({"pattern": "x"}))],
                FinishReason::ToolCalls,
            ),
            answer(
                "a3",
                vec![call("c3", "grep", json!({"pattern": "x"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a4", "giving up"),
        ],
    }
}

#[tokio::test]
async fn a_call_repeated_with_the_same_failure_gets_guidance() {
    compare(repeated_failure_scenario()).await;
}

#[tokio::test]
async fn every_cut_of_a_repeated_failure_resumes_alike() {
    compare_every_cut(repeated_failure_scenario()).await;
}

#[tokio::test]
async fn repetition_guidance_counts_within_each_run() {
    let mut scenario = repeated_failure_scenario();
    // The second run repeats the first run's failing call once more: counted afresh, it gets no
    // guidance.
    scenario.steps.extend([
        answer(
            "a5",
            vec![call("c4", "grep", json!({"pattern": "x"}))],
            FinishReason::ToolCalls,
        ),
        answer(
            "a6",
            vec![call("c5", "grep", json!({"pattern": "x"}))],
            FinishReason::ToolCalls,
        ),
        text_step("a7", "still nothing"),
    ]);
    compare_runs(
        scenario,
        &[
            ("r1", vec![user("find x")]),
            ("r2", vec![user("try again")]),
        ],
    )
    .await;
}

#[tokio::test]
async fn a_later_run_that_fails_before_its_first_step() {
    let mut scenario = repeated_failure_scenario();
    scenario.steps.truncate(1);
    scenario.steps.push(text_step("a2", "done"));
    compare_runs(
        scenario,
        &[
            ("r1", vec![user("find x")]),
            ("r2", vec![user("try again")]),
        ],
    )
    .await;
}

/// A caller's cancel that lands after the model asked for tools and before they're sent: the
/// post-assistant hook fires it, so it lands at the same point in both loops.
fn cancel_before_batch_scenario() -> Scenario {
    let tools = vec![tool("read", ExecutionMode::Parallel)];
    Scenario {
        input: vec![user("read it")],
        manifest: Some(hooked_manifest(&tools)),
        tools,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    ContentBlock::Text {
                        text: "reading it, cancel me".into(),
                    },
                    call("c1", "read", json!({"path": "a"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "never reached"),
        ],
    }
}

#[tokio::test]
async fn a_cancel_before_the_batch_refuses_it() {
    compare(cancel_before_batch_scenario()).await;
}

#[tokio::test]
async fn every_cut_of_a_cancel_before_the_batch_resumes_alike() {
    compare_every_cut(cancel_before_batch_scenario()).await;
}

/// The same refused batch, with the model repeating a call id.
fn cancel_before_batch_with_a_repeated_id_scenario() -> Scenario {
    let mut scenario = cancel_before_batch_scenario();
    scenario.steps[0] = answer(
        "a1",
        vec![
            ContentBlock::Text {
                text: "reading it, cancel me".into(),
            },
            call("c1", "read", json!({"path": "a"})),
            call("c1", "read", json!({"path": "b"})),
        ],
        FinishReason::ToolCalls,
    );
    scenario
}

#[tokio::test]
async fn a_refused_batch_still_answers_a_repeated_call_id_as_a_duplicate() {
    compare(cancel_before_batch_with_a_repeated_id_scenario()).await;
}

/// Runs [`compare_in`] once, on the current compaction contract, with a compaction provider
/// scripted by `script` that both loops call.
async fn compare_compacted(scenario: Scenario, script: CompactionScript) {
    LAYOUT
        .scope(
            Some(cortexkit_compaction_view::PLAN_VERSION),
            COMPACTION.scope(Some(Arc::new(script)), Box::pin(compare_in(scenario))),
        )
        .await;
}

/// [`compare_compacted`] for every cut of today's log: both loops resume from each prefix, each
/// with a fresh provider following the same script, and must make the same calls and write the
/// same records. Resuming rebuilds the compaction state from the cut log, so this covers a
/// crash between a view being accepted and a step applying it.
async fn compare_compacted_every_cut(scenario: Scenario, script: CompactionScript) {
    LAYOUT
        .scope(
            Some(cortexkit_compaction_view::PLAN_VERSION),
            COMPACTION.scope(
                Some(Arc::new(script)),
                Box::pin(compare_every_cut_in(scenario)),
            ),
        )
        .await;
}

/// A fetch plan's manifest that also names a compaction provider (`compactor`, preset `head`),
/// with part numbers rendered into requests.
fn compacted_manifest(tools: &[Tool]) -> Arc<broca_wal::PlanManifest> {
    let mut manifest = planned_manifest(tools).as_ref().clone();
    manifest.plan = broca_wal::OpaqueJson::from_json_text(
        json!({
            "tool_items": [{"provider": "provider"}],
            "compaction_item": {"provider": "compactor", "preset": "head", "params": {}},
            "composition": {},
            "render_tags": true,
        })
        .to_string(),
    )
    .unwrap();
    Arc::new(manifest)
}

fn compacted_scenario(steps: Vec<Vec<NormalizedEvent>>) -> Scenario {
    let tools = vec![
        tool("read", ExecutionMode::Parallel),
        tool("big", ExecutionMode::Parallel),
    ];
    Scenario {
        input: vec![user("read a")],
        manifest: Some(compacted_manifest(&tools)),
        tools,
        stop_when: Vec::new(),
        steps,
    }
}

#[tokio::test]
async fn a_compacted_text_answer() {
    compare_compacted(
        compacted_scenario(vec![text_step("a1", "hello")]),
        CompactionScript {
            context_limit: 100_000,
            rides: Vec::new(),
        },
    )
    .await;
}

#[tokio::test]
async fn a_compacted_tool_call_then_an_answer() {
    compare_compacted(
        compacted_scenario(vec![
            answer(
                "a1",
                vec![call("c1", "read", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ]),
        CompactionScript {
            context_limit: 100_000,
            rides: Vec::new(),
        },
    )
    .await;
}

/// A tool result pushes the next request over the window; the step rides, the provider answers
/// with a view that drops the call's entry, and the step renders from that view.
#[tokio::test]
async fn an_over_limit_step_rides_and_renders_from_the_view_it_gets() {
    compare_compacted(
        compacted_scenario(vec![
            answer(
                "a1",
                vec![call("c1", "big", json!({}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ]),
        CompactionScript {
            context_limit: 1_200,
            rides: vec![Ride::Drop(2)],
        },
    )
    .await;
}

#[tokio::test]
async fn every_cut_of_an_over_limit_ride_resumes_alike() {
    compare_compacted_every_cut(
        compacted_scenario(vec![
            answer(
                "a1",
                vec![call("c1", "big", json!({}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ]),
        CompactionScript {
            context_limit: 1_200,
            rides: vec![Ride::Drop(2)],
        },
    )
    .await;
}

/// Two runs on one compacted session: the second run's first send compares the first run's
/// last accepted request with its own, which the new loop rebuilds from the first run's start.
#[tokio::test]
async fn a_second_compacted_run_compares_with_the_first_runs_request() {
    LAYOUT
        .scope(
            Some(cortexkit_compaction_view::PLAN_VERSION),
            COMPACTION.scope(
                Some(Arc::new(CompactionScript {
                    context_limit: 100_000,
                    rides: Vec::new(),
                })),
                Box::pin(compare_runs_in(
                    compacted_scenario(vec![
                        text_step("a1", "hello"),
                        text_step("a2", "hello again"),
                    ]),
                    &[("r1", vec![user("first")]), ("r2", vec![user("second")])],
                )),
            ),
        )
        .await;
}

/// A model change between two runs makes the second run's first step ride, and the provider
/// answers with a view that cuts through the first run. Today's loop keeps its whole log; the
/// new loop seals the first run's history before sending, and must still send the same requests,
/// write the same records and make the same calls.
#[tokio::test]
async fn a_cut_seals_the_history_before_it_and_requests_stay_the_same() {
    LAYOUT
        .scope(
            Some(cortexkit_compaction_view::PLAN_VERSION),
            COMPACTION.scope(
                Some(Arc::new(CompactionScript {
                    context_limit: 100_000,
                    rides: vec![Ride::Cut(2)],
                })),
                Box::pin(compare_runs_in(
                    compacted_scenario(vec![
                        text_step("a1", "hello"),
                        text_step("a2", "hello again"),
                    ]),
                    &[("r1", vec![user("first")]), ("r2@m2", vec![user("second")])],
                )),
            ),
        )
        .await;
}

/// Four runs, with a cut through the second run's answer during the third, so the first two runs
/// leave the window whole: the new loop keeps the first (the session's first run) and drops the
/// second (same render settings), and the fourth run's requests must carry the runs a renderer
/// reads as today's do.
#[tokio::test]
async fn a_cut_past_two_runs_keeps_only_the_runs_that_shape_requests() {
    LAYOUT
        .scope(
            Some(cortexkit_compaction_view::PLAN_VERSION),
            COMPACTION.scope(
                Some(Arc::new(CompactionScript {
                    context_limit: 100_000,
                    rides: vec![Ride::Cut(4)],
                })),
                Box::pin(compare_runs_in(
                    // The third run calls a tool, so its second request goes out after the seal.
                    compacted_scenario(vec![
                        text_step("a1", "one"),
                        text_step("a2", "two"),
                        answer(
                            "a3",
                            vec![call("c1", "read", json!({"path": "a"}))],
                            FinishReason::ToolCalls,
                        ),
                        text_step("a4", "three"),
                        text_step("a5", "four"),
                    ]),
                    // The fourth run starts after the seal, so its requests read the runs the
                    // sealed conversation kept.
                    &[
                        ("r1", vec![user("first")]),
                        ("r2", vec![user("second")]),
                        ("r3@m2", vec![user("third")]),
                        ("r4@m2", vec![user("fourth")]),
                    ],
                )),
            ),
        )
        .await;
}
