//! The new loop driven by a scripted provider and tool plane, checked against what it was pinned
//! to do (see `broca_test_support::pins`).
//!
//! These scenarios once compared the new loop with the previous runner, scenario by scenario,
//! and the pins hold what both did alike when the previous runner was removed. Each run pins:
//! - **the requests:** every `CallOptions` the provider was asked to send, whole, including the
//!   render inputs that never serialize;
//! - **the records:** kind by kind and field by field, apart from fields that hold a time or
//!   an id minted per log, which the harness normalizes by name;
//! - **the tool calls:** what the tool plane was asked to run, in order.
//!
//! A crash is a cut of the session's own log at the end of one of its batches: the session
//! reopens from the backend as that batch left it, and the loop resumes or seals the run.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::capability::ToolManifest;
use broca_core::output_events::OutputEvent;
use broca_core::tool_media::MediaCapture;
use broca_core::tool_plane::{
    BatchCtx, DispatchKey, DispatchRegistration, DispatchResult, ProviderHeldLifecycle,
    ProviderHeldReply, ToolOpenGuard, ToolPlane, ToolPlaneError,
};
use broca_core::{provider::Provider, RunControl, RunRequest, StubReply, StubToolPlane};
use broca_protocol::caller::{PauseReason, RunFinishReason};
use broca_protocol::content::{
    AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolOutputPart, ToolResult,
};
use broca_protocol::provider::{NormalizedEvent, ProviderControl};
use broca_protocol::request::{
    CallOptions, ExecutionMode, ModelHandle, OnRestart, StopCondition, Tool, ToolChoice,
};
use broca_protocol::{
    DisplayEvent, FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_session_loop::shared::{ActorWriter, Request};
use broca_session_loop::{
    resume, run, seal, AccountReports, Admission, MemoryParts, PendingMedia, Seams, Writer,
};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::codec::Message as _;
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::{Session, SessionError};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[path = "compaction/statement_seq.rs"]
mod statement_seq;

/// Serves scripted steps in order and records every request it was asked to send.
#[derive(Clone)]
struct ScriptedProvider {
    scripts: Arc<Mutex<VecDeque<Vec<NormalizedEvent>>>>,
    sent: Arc<Mutex<Vec<CallOptions>>>,
    /// Whether the run downgraded its strict structured-output schema; the frozen config the
    /// provider reports is downgraded from then on, as a real provider's is.
    downgraded: Arc<std::sync::atomic::AtomicBool>,
    /// Whether this provider has refused the strict schema's grammar.
    refused: Arc<std::sync::atomic::AtomicBool>,
    /// Whether this provider renders with another config than the run froze, as one rebuilt
    /// from a changed catalog would.
    drifted: Arc<std::sync::atomic::AtomicBool>,
    /// Where the loop sends the next step, when the run froze a refusal fallback.
    route: Arc<Mutex<Option<broca_core::refusal_fallback::StepRoute>>>,
    /// What the current run's requests wait for before they can be rendered, when the
    /// comparison gives the new loop one (see [`ScriptedProvider::model_call_ready`]).
    readiness: Arc<Mutex<Option<Arc<Readiness>>>>,
    /// The context window this provider refuses requests past, when a test gives it one; a
    /// comparison under a compaction script refuses past the script's limit.
    window: Arc<Mutex<Option<u64>>>,
}

/// Whether `script` is the provider refusing the strict schema's grammar.
fn is_grammar_refusal(script: &[NormalizedEvent]) -> bool {
    script.iter().any(|event| {
        matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Error { error, .. })
                if error.provider_code.as_deref() == Some(GRAMMAR_REFUSED)
        )
    })
}

impl ScriptedProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into())),
            sent: Arc::new(Mutex::new(Vec::new())),
            downgraded: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            refused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            drifted: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            route: Arc::new(Mutex::new(None)),
            readiness: Arc::new(Mutex::new(None)),
            window: Arc::new(Mutex::new(None)),
        }
    }

    /// From now on, refuses a request past `window` tokens as too long.
    fn set_window(&self, window: u64) {
        *self.window.lock().unwrap() = Some(window);
    }

    /// What a new-loop run or resume on this provider waits for before its requests can be
    /// rendered, when the comparison running in this task asks for one (`READY`): a new one
    /// for each call, not ready yet, as each store run gets a media renderer that renders
    /// nothing until its run waited for the session's media. Until it is ready, this provider
    /// refuses to render the run's requests.
    fn model_call_ready(&self) -> Option<Arc<dyn broca_session_loop::run::ModelCallReady>> {
        let script = READY.try_with(|script| *script).ok().flatten()?;
        let readiness = Arc::new(Readiness {
            script,
            ready: Default::default(),
        });
        *self.readiness.lock().unwrap() = Some(Arc::clone(&readiness));
        Some(readiness)
    }

    /// The render config the run freezes: the stateless default, or a real config for a cache
    /// comparison, carrying the run's structured output, downgraded once refused.
    fn frozen_blob(&self) -> Value {
        let mut blob = self.frozen_blob_without_fallback();
        if let Some(script) = fallback() {
            blob["refusal_fallback"] = json!({ "model": script.model });
        }
        blob
    }

    fn frozen_blob_without_fallback(&self) -> Value {
        // A comparison of cache expiry freezes a real first-party Anthropic config, whose cache
        // markers ask for a lifetime; this provider still renders requests its own way.
        if cache() {
            return serde_json::to_value(cached_config()).expect("a frozen config serializes");
        }
        // The ride placement probe's tests freeze a real route, with or without the placement.
        if let Some(route) = ride_placement::frozen_route() {
            return serde_json::to_value(route).expect("a frozen config serializes");
        }
        let mut blob = json!({ "render_config": "stateless" });
        if let Some(structured) = structured() {
            blob["structured_output"] = structured;
        }
        if self.downgraded.load(std::sync::atomic::Ordering::SeqCst) {
            broca_protocol::structured_output::FrozenStructuredOutput::downgrade_render_config(
                &mut blob,
            );
        }
        blob
    }

    /// From now on, renders with another config than the run froze when the comparison asks
    /// for it: called on the provider a resume uses.
    fn resuming(&self) {
        if drift() {
            self.drifted
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

/// The provider code a scripted grammar refusal carries.
const GRAMMAR_REFUSED: &str = "scripted_grammar_too_large";

#[async_trait]
impl Provider for ScriptedProvider {
    fn admit_temperature(&self, requested: Option<f64>) -> Option<f64> {
        temperature::POLICY
            .try_with(|policy| policy.apply(requested))
            .unwrap_or_else(|_| broca_core::provider::TemperaturePolicy::default().apply(requested))
    }
    /// Whether the route takes a turn-scoped system message here, by the provider's own rules for
    /// the route the comparison names; a comparison naming none takes none, as the stub route.
    fn turn_system_possible(&self, preceding: Option<&Message>, followed_by_user: bool) -> bool {
        turn_system_route().is_some_and(|frozen| {
            broca_provider::turn_system::possible_after(&frozen, preceding, followed_by_user)
        })
    }

    /// Adds the cache evidence a live provider would, so a loop that skips it records a
    /// different attempt.
    fn annotate_cache_evidence(&self, attempt: &mut broca_protocol::ModelAttempt) {
        attempt.cache_evidence = Some(
            serde_json::from_value(json!({"read": 7, "written": 3}))
                .expect("cache evidence decodes"),
        );
    }

    /// The session's last accepted send on an account is now, when the comparison routes one,
    /// so the session's cache on it outlives a pause until a reset that comes now.
    fn refusal_fallback_model(&self) -> Option<ModelHandle> {
        fallback().map(|script| script.model)
    }

    fn refusal_fallback_context_limit(&self) -> Option<u64> {
        fallback().and_then(|script| script.context_limit)
    }

    fn set_step_route(&self, route: broca_core::refusal_fallback::StepRoute) {
        *self.route.lock().unwrap() = Some(route);
    }

    fn last_accepted_at_ms(&self, _: &str) -> Option<u64> {
        account().map(|_| harness_now())
    }

    /// Refuses while the run's requests aren't ready to render, as a store run's media renderer
    /// refuses before its run waited for the session's media.
    fn try_render_request(
        &self,
        opts: &CallOptions,
    ) -> Result<Vec<u8>, Box<broca_protocol::ProviderError>> {
        if let Some(readiness) = self.readiness.lock().unwrap().as_ref() {
            if !readiness.ready.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(Box::new(not_ready_error(
                    "the session's media aren't ready to render yet",
                )));
            }
        }
        Ok(self.render_request(opts))
    }

    /// The default stateless config, carrying the run's structured output when the comparison
    /// freezes one: both loops read it from here.
    fn render_config_blob(&self) -> Value {
        let mut blob = self.frozen_blob();
        if self.drifted.load(std::sync::atomic::Ordering::SeqCst) {
            blob["drifted"] = json!(true);
        }
        blob
    }

    /// As a real provider decides: only a refusal of the run's own strict schema, before it
    /// was downgraded.
    fn structured_output_grammar_rejection(&self, error: &broca_protocol::ProviderError) -> bool {
        error.provider_code.as_deref() == Some(GRAMMAR_REFUSED)
            && !self.downgraded.load(std::sync::atomic::Ordering::SeqCst)
            && structured().is_some_and(|structured| structured["delivery"] == "strict")
    }

    fn downgrade_structured_output(&self) {
        self.downgraded
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    async fn step(&self, _step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        // The route the loop told the provider rides with each sent request, so a loop that
        // sends a fallback step without saying so sends a different request.
        let mut opts = opts.clone();
        if let Some(route) = *self.route.lock().unwrap() {
            opts.provider_options = Arc::new(json!({"routed": format!("{route:?}")}));
        }
        let opts = &opts;
        self.sent.lock().unwrap().push(opts.clone());
        let rendered = self.render_request(opts);
        // Under a compaction script the provider has a window, and refuses a request past it as
        // Anthropic does, before answering anything: the request's size is its rendered bytes at
        // two per token, this provider's own count. No scripted answer is used up.
        let window = (*self.window.lock().unwrap())
            .or_else(|| compaction().map(|script| script.context_limit));
        if let Some(window) = window {
            let tokens = (rendered.len() as u64).div_ceil(2);
            if tokens > window {
                let (tx, rx) = mpsc::channel(1);
                let _ = tx.try_send(too_long(tokens, window));
                return rx;
            }
        }
        let script = {
            let mut scripts = self.scripts.lock().unwrap();
            let downgraded = self.downgraded.load(std::sync::atomic::Ordering::SeqCst);
            let strict = structured().is_some_and(|structured| structured["delivery"] == "strict");
            // A downgraded provider no longer sends the strict schema, so the grammar refusal
            // scripted for it never comes; one that wasn't downgraded after a refusal still
            // sends it, and is refused again.
            if downgraded
                && scripts
                    .front()
                    .is_some_and(|script| is_grammar_refusal(script))
            {
                scripts.pop_front();
            }
            if strict && !downgraded && self.refused.load(std::sync::atomic::Ordering::SeqCst) {
                grammar_refused()
            } else {
                let script = scripts.pop_front().unwrap_or_default();
                if is_grammar_refusal(&script) {
                    self.refused
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
                script
            }
        };
        // An attempt scripted as `RENDERED` reports the request this call actually rendered,
        // as a live provider does; a resume that checks the request it sends again compares
        // against it.
        let script = script.into_iter().map(|mut event| {
            if let NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) =
                &mut event
            {
                if attempt.request_body_sha256 == RENDERED
                    || attempt.request_body_sha256 == RENDERED_OTHER
                {
                    use sha2::Digest as _;
                    // `RENDERED_OTHER` reports a request other than the one this call sent, as
                    // if the step had rendered differently before the refusal.
                    let mut reported = rendered.clone();
                    if attempt.request_body_sha256 == RENDERED_OTHER {
                        reported.extend_from_slice(b" ");
                    }
                    attempt.request_body_len = reported.len() as u64;
                    attempt.request_body_sha256 = format!("{:x}", sha2::Sha256::digest(&reported));
                    // An account limit is observed when it is refused, and resets shortly after.
                    if let Some(limit) = attempt.account_observation.limit.as_mut() {
                        limit.observed_at_ms = harness_now();
                        limit.resets_at_ms = Some(limit.observed_at_ms + RESETS_AFTER_MS);
                    }
                }
            }
            event
        });
        let script: Vec<_> = script.collect();
        let hold = HOLD_STREAM.try_with(|hold| *hold).unwrap_or(false);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for event in script {
                let _ = tx.send(event).await;
            }
            if hold {
                tx.closed().await;
            }
        });
        rx
    }
}

tokio::task_local! {
    /// Whether the new loop's runs wait for their requests to be ready to render, and whether
    /// that wait succeeds (see [`ScriptedProvider::model_call_ready`]). `None` gives no wait.
    static READY: Option<ReadyScript>;
}

#[derive(Clone, Copy)]
enum ReadyScript {
    /// The wait succeeds, after yielding once.
    Ready,
    /// The wait fails with [`READINESS_FAILED`].
    Fails,
}

/// The error a failed wait for a run's requests to be ready gives.
const READINESS_FAILED: &str = "the session's media fold can't be built";

fn not_ready_error(message: &str) -> broca_protocol::ProviderError {
    serde_json::from_value(json!({
        "class": "permanent",
        "message": message,
        "provider_code": "store_media_unavailable",
    }))
    .expect("a provider error decodes")
}

/// What one new-loop run or resume waits for before its requests can be rendered.
struct Readiness {
    script: ReadyScript,
    ready: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl broca_session_loop::run::ModelCallReady for Readiness {
    async fn ready(&self) -> Result<(), broca_protocol::ProviderError> {
        tokio::task::yield_now().await;
        match self.script {
            ReadyScript::Ready => {
                self.ready.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
            ReadyScript::Fails => Err(not_ready_error(READINESS_FAILED)),
        }
    }
}

/// How long after a scripted account-limit refusal the account resets.
const RESETS_AFTER_MS: u64 = 200;

/// The request digest a scripted attempt carries to report the request it was sent with.
const RENDERED: &str = "rendered";

/// The request digest a scripted attempt carries to report a request other than the one it was
/// sent with.
const RENDERED_OTHER: &str = "rendered-other";

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
        provider_state: None,
        tool_call_id: ToolCallId(id.into()),
        tool_name: name.into(),
        input: input.into(),
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
    manifest: Mutex<Option<ToolManifest>>,
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
                text: format!("late result for {}", id.0).into(),
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
            manifest: Mutex::new(None),
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

    // Observe the provider and preset at dispatch from the ToolManifest received at batch start.
    // Reading Scenario.manifest instead would hide a loop that omitted the plan's presets.
    fn note_route(&self, call: &ToolCall) {
        let manifest = self.manifest.lock().unwrap();
        if let Some(manifest) = manifest.as_ref() {
            if let Some(provider) = manifest
                .get(&call.tool_name)
                .and_then(|tool| tool.module.as_deref())
            {
                self.note(format!(
                    "dispatch_route {} provider={provider:?} preset={:?}",
                    call.tool_call_id.0,
                    manifest.provider_preset(provider)
                ));
            }
        }
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

/// The scope a batch's calls are routed under, as the spy logs it: nothing for an unscoped batch,
/// so scenarios without a scope log as they always have.
fn batch_scope(ctx: &BatchCtx) -> String {
    ctx.scope
        .as_ref()
        .map_or_else(String::new, |scope| format!(" scope={}", scope.scope_ref))
}

#[async_trait]
impl ToolPlane for Spy {
    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        *self.manifest.lock().unwrap() = Some(manifest.clone());
        self.note(format!(
            "batch_started {:?}{}",
            ids(calls),
            batch_scope(ctx)
        ));
        self.inner.batch_started(ctx, calls, manifest).await
    }

    async fn batch_started_guarded(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
        guard: &ToolOpenGuard,
    ) -> Result<(), ToolPlaneError> {
        *self.manifest.lock().unwrap() = Some(manifest.clone());
        self.note(format!(
            "batch_started_guarded {:?}{}",
            ids(calls),
            batch_scope(ctx)
        ));
        // A batch with a `down` call meets a tool provider that can't be reached.
        if calls.iter().any(|call| call.tool_name == "down") {
            return Err(ToolPlaneError::Unavailable("route closed".into()));
        }
        // A batch with a `scope_gone` call finds its scope ended before anything is sent.
        if calls.iter().any(|call| call.tool_name == "scope_gone") {
            return Err(ToolPlaneError::ScopeDrained {
                reason: "scope_ended".into(),
            });
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
        self.note_route(call);
        self.note(format!("dispatch {} {}", call.tool_call_id.0, call.input));
        self.inner.dispatch(ctx, call).await
    }

    async fn dispatch_keyed(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        key: DispatchKey<'_>,
    ) -> DispatchResult {
        self.note_route(call);
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
        self.note_route(call);
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
        // A call that went out and never answered: the caller cancels the run (`stall`), or the
        // runner stops (`stall_stop`), while it runs, so whether it took effect is never known.
        if let "stall" | "stall_stop" = call.tool_name.as_str() {
            let token = if call.tool_name == "stall" {
                guard.cancel_token()
            } else {
                guard.stop_token()
            };
            token.expect("the loop guards every dispatch").cancel();
            return DispatchResult::SentButOutcomeUnknown {
                reason: "cut short while it ran".into(),
            };
        }
        if call.tool_name == "photo" {
            return photo(ctx, call).await;
        }
        // A call its provider handed back for the person's steer: the interim content it
        // answered the hand-back with closes the call, and the work goes on at the provider.
        if call.tool_name == "handback" {
            return DispatchResult::Yielded {
                result: ToolResult {
                    tool_call_id: call.tool_call_id.clone(),
                    output: ToolOutput::Text {
                        text: "still building".into(),
                    },
                    is_error: false,
                },
                carrier: "provider".into(),
            };
        }
        // A call slow enough that keep-warm sends its warms while it runs.
        if call.tool_name == "slow" {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
        // A result big enough that the next request goes over a small window.
        if call.tool_name == "big" {
            return DispatchResult::Completed(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Text {
                    text: "x".repeat(8_000).into(),
                },
                is_error: false,
            });
        }
        // A structured result, which no ride rides on off the compaction contract.
        if call.tool_name == "json" {
            return DispatchResult::Completed(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Json {
                    value: json!({"rows": 2}).into(),
                },
                is_error: false,
            });
        }
        // An error result, which a ride may ride on.
        if call.tool_name == "broken" {
            return DispatchResult::Completed(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::error("no such file"),
                is_error: true,
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
    ) -> Option<broca_records::record::DispatchSchemaPin> {
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
async fn media_reads_back(session: &Session<&MemoryBackend>) {
    let whole = broca_session_loop::writer::whole_conversation(session)
        .await
        .expect("the window reads back");
    for message in &whole.held_messages().unwrap() {
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

/// The model a run of the comparison uses: `m`, or the one a run id names after an `@`
/// (`r2@m2`), so a scenario can change the model between runs.
fn run_model(run_id: &str) -> &str {
    run_id.split_once('@').map_or("m", |(_, model)| model)
}

tokio::task_local! {
    /// Whether the multi-run comparison seals the new loop's ledger after each run but the last.
    static LEDGER_SEAL_BETWEEN_RUNS: bool;
}

tokio::task_local! {
    /// When set, the multi-run comparison adds the new loop's requests here, in the order they
    /// were sent, after comparing them with today's, for a test to check more of them.
    static NEW_REQUESTS: Arc<Mutex<Vec<CallOptions>>>;
}

tokio::task_local! {
    /// The tools a run of a multi-run comparison declares in place of the scenario's, by run id.
    static RUN_TOOLS: std::collections::HashMap<String, Vec<Tool>>;
}

tokio::task_local! {
    /// When set, the multi-run comparison adds here, once per comparison, the calls the new loop
    /// made on its tool plane and compaction provider, after comparing them with today's (which
    /// leaves out the new loop's `last_prompt_view`; these calls keep it).
    static NEW_CALLS: Arc<Mutex<Vec<Vec<String>>>>;
}

tokio::task_local! {
    /// When set, the new side ends the cut log's run with this seal instead of resuming it.
    static SEALING: broca_core::seal::SealCause;
}

tokio::task_local! {
    /// When set, a run [`new_with_actions`] resumes finds its account signed out at the start of
    /// the resume, so it pauses for sign-in instead of going on.
    static SIGNED_OUT_ON_RESUME: ();
}

tokio::task_local! {
    /// The compaction provider a comparison's plan names, scripted: what it answers each ride
    /// and the window the run freezes. `None` runs without one.
    static COMPACTION: Option<Arc<CompactionScript>>;
}

tokio::task_local! {
    /// The structured output the provider's frozen render config carries, as a
    /// `FrozenStructuredOutput` value. `None` runs without structured output.
    static STRUCTURED: Option<Value>;
}

tokio::task_local! {
    /// The tool-call budget both loops freeze. `None` runs without one.
    static BUDGET: Option<broca_protocol::request::BudgetConfig>;
}

tokio::task_local! {
    /// Whether the send sets every field a run's start records about it (its key, labels,
    /// principal, scope, input hooks, routing decision, tier, variant and the rest), so the
    /// test's recorded expectations check that each one reaches the run's start record.
    static SEND_FACTS: bool;
}

fn send_facts() -> bool {
    SEND_FACTS.try_with(|set| *set).unwrap_or(false)
}

tokio::task_local! {
    /// Recorded owner-send marks and the notice presence the compaction contract promises.
    static RECORDED_MARKS: (Option<broca_protocol::MessageMarks>, bool);
}

/// The principal that sends in the send-facts scenario, and owns its scope.
fn owner() -> broca_protocol::BindPrincipal {
    broca_protocol::BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

/// The budget the comparison running in this task freezes.
fn budget() -> Option<broca_protocol::request::BudgetConfig> {
    BUDGET.try_with(Clone::clone).ok().flatten()
}

/// A budget with these nudges, each at a tool-call count, and a finalize reminder at
/// `finalize`.
fn budgeted(nudges: &[u64], finalize: Option<u64>) -> broca_protocol::request::BudgetConfig {
    broca_protocol::request::BudgetConfig {
        max_input_tokens: None,
        nudges: nudges
            .iter()
            .map(|at| broca_protocol::request::BudgetNudge {
                at_tool_calls: *at,
                text: format!("you have made {at} tool calls"),
            })
            .collect(),
        finalize_at_tool_calls: finalize.unwrap_or(u64::MAX),
        finalize_text: "finish now".into(),
    }
}

tokio::task_local! {
    /// Whether the comparison freezes a provider config whose requests ask for a cache
    /// lifetime, with both loops reading the wall clock, so a cache can be proven expired.
    static CACHE: bool;
}

fn cache() -> bool {
    CACHE.try_with(|cache| *cache).unwrap_or(false)
}

tokio::task_local! {
    /// Whether the comparison also compares each run's spend facts, as the usage export would
    /// hold them.
    static SPEND: bool;
}

fn spend_compared() -> bool {
    SPEND.try_with(|set| *set).unwrap_or(false)
}

/// Runs `comparison` with each run's spend facts compared at its end.
async fn compare_spent<F: std::future::Future<Output = ()>>(comparison: F) {
    SPEND.scope(true, comparison).await;
}

/// Pins the spend facts each run of `session` gets from the usage-export builder
/// (`compute_segments_for_run`), its records rebuilt as the store actor's billing rebuilds them.
/// The times the loop took from its clock, and the fact ids made from them, are set aside.
async fn pin_spend(session: &Session<&MemoryBackend>, parts: &MemoryParts) {
    struct UnknownCharge;
    impl broca_core::ChargeBasisResolver for UnknownCharge {
        fn resolve_charge_basis(&self, _: &str, _: &str) -> (broca_core::ChargeBasis, u32) {
            (broca_core::ChargeBasis::Unknown, 1)
        }
        fn charge_basis_overrides(&self) -> String {
            broca_core::NO_CHARGE_BASIS_OVERRIDES.into()
        }
    }
    fn timeless(facts: Vec<broca_core::billing::SpendSegment>) -> Vec<Value> {
        facts
            .iter()
            .map(|fact| {
                let mut value = serde_json::to_value(fact).unwrap();
                for field in ["fact_id", "occurred_at_ms", "pricing_at_ms"] {
                    if value.get(field).is_some() {
                        value[field] = json!("<normalized>");
                    }
                }
                value
            })
            .collect()
    }
    let records = session.open_log_records().await.unwrap();
    let mut runs: Vec<String> = Vec::new();
    for (_, record) in &records {
        if let Record::RunStart(start) = record {
            if !runs.contains(&start.run_id) {
                runs.push(start.run_id.clone());
            }
        }
    }
    assert!(!runs.is_empty(), "the session started no run");
    for run_id in runs {
        let rebuilt =
            broca_session_loop::billing::billing_records(&records, &run_id, &bind(), parts)
                .expect("the run's records rebuild");
        let facts = timeless(broca_core::billing::compute_segments_for_run(
            &rebuilt,
            &RunId(run_id.clone()),
            "differential",
            &UnknownCharge,
        ));
        assert!(!facts.is_empty(), "run {run_id} bills");
        broca_test_support::pin!(format!("run {run_id}'s spend facts"), facts);
    }
}

tokio::task_local! {
    /// Whether the comparison also reads both sessions' compaction entries (`session.entries`)
    /// on the compaction contract and compares them: set for the sends delivered into a run.
    static ENTRIES: bool;
}

fn entries_compared() -> bool {
    ENTRIES.try_with(|set| *set).unwrap_or(false)
}

/// Runs `comparison` with both sessions' compaction entries compared at its end.
async fn reading_entries(comparison: impl std::future::Future<Output = ()>) {
    ENTRIES.scope(true, comparison).await;
}

/// On the compaction contract, pins every compaction entry the session serves: a tool result
/// with its own output and, beside it, each ride (text a queued send delivered with that result,
/// which the contract keeps apart from the output), and each delivered message as it was placed. Message ids, call keys and other ids are pinned by order of first appearance; times
/// by whether they are there.
async fn pin_entries(session: &Session<&MemoryBackend>) {
    // Only a log on the compaction contract serves compaction entries.
    if LAYOUT.try_with(|layout| layout.is_none()).unwrap_or(true) {
        return;
    }
    let new: Vec<_> = session
        .entries(1, usize::MAX)
        .await
        .expect("the new session's entries read")
        .iter()
        .map(|entry| broca_session_loop::entries::project(entry).expect("an entry projects"))
        .collect();
    if let Ok((marks, notice)) = RECORDED_MARKS.try_with(Clone::clone) {
        let first = serde_json::to_value(&new[0]).unwrap();
        assert_eq!(first.get("notice").is_some(), notice, "{first}");
        if notice {
            assert_eq!(first["notice"], json!({"sender": "prefrontal-core"}));
        }
        // The speaker the marks name, display name included, rides on the entry outside its
        // digest.
        assert_eq!(
            first.get("speaks_for"),
            marks
                .as_ref()
                .and_then(|marks| marks.speaks_for.as_ref())
                .map(|speaker| serde_json::to_value(speaker).unwrap())
                .as_ref(),
            "{first}"
        );
        // Earlier writers stored a module origin even for marked agent speech.
        // Reads must decide from the recorded marks without rewriting those facts.
        for (mut stored, expected) in session
            .entries(1, usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .zip(&new)
        {
            if stored.message.role == Role::User {
                std::sync::Arc::make_mut(stored.state.facts.as_mut().unwrap()).notice =
                    if marks.is_some() && notice {
                        None
                    } else {
                        Some(
                            broca_session_store::record::Notice {
                                sender: "prefrontal-core".into(),
                                template: None,
                            }
                            .into(),
                        )
                    };
            }
            let served = broca_session_loop::entries::project(&stored).unwrap();
            assert_eq!(
                serde_json::to_value(served).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
    }
    fn masked(entries: &[cortexkit_role_compaction::read::Entry]) -> Value {
        fn rename(value: &mut Value, names: &mut std::collections::HashMap<String, usize>) {
            match value {
                Value::Object(members) => {
                    for (key, member) in members.iter_mut() {
                        match (key.as_str(), member.as_str()) {
                            ("mid" | "call_key" | "id", Some(text)) => {
                                let next = names.len();
                                let name = *names.entry(text.to_owned()).or_insert(next);
                                *member = json!(format!("{key}#{name}"));
                            }
                            _ if member.is_number()
                                && (key.ends_with("_at") || key.ends_with("_at_ms")) =>
                            {
                                *member = json!("<time>");
                            }
                            _ => rename(member, names),
                        }
                    }
                }
                Value::Array(items) => items.iter_mut().for_each(|item| rename(item, names)),
                _ => {}
            }
        }
        let mut value = serde_json::to_value(entries).unwrap();
        rename(&mut value, &mut Default::default());
        value
    }
    broca_test_support::pin!(
        "compaction entries",
        broca_test_support::pins::Text(serde_json::to_string_pretty(&masked(&new)).unwrap())
    );
}

tokio::task_local! {
    /// The route both loops' providers judge turn-scoped system messages by, for a comparison of
    /// sends delivered that way. `None` judges by the stub route, which takes none.
    static TURN_SYSTEM: Option<broca_provider::wire::FrozenRenderConfig>;
}

fn turn_system_route() -> Option<broca_provider::wire::FrozenRenderConfig> {
    TURN_SYSTEM.try_with(Clone::clone).unwrap_or(None)
}

/// A first-party Anthropic route, with or without confirmed mid-conversation system messages.
fn anthropic_route(turn_system: bool) -> broca_provider::wire::FrozenRenderConfig {
    let mut frozen = broca_provider::families::anthropic_frozen_config("m");
    frozen.anthropic_turn_system = turn_system;
    frozen
}

/// A first-party Anthropic config whose cache markers ask for five minutes.
fn cached_config() -> broca_provider::wire::FrozenRenderConfig {
    let mut frozen = broca_provider::families::anthropic_frozen_config("m");
    frozen.cache = Some(broca_provider::wire::CachePolicy {
        version: 1,
        class: broca_provider::wire::CacheClass::Markers {
            syntax: broca_provider::wire::MarkerSyntax::AnthropicCacheControl,
            ttl: broca_provider::wire::CacheTtl::FiveMinutes,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    });
    frozen
}

/// The provider the runs name: a cache-expiry comparison needs first-party Anthropic, whose
/// requests ask for a lifetime.
fn provider_id() -> &'static str {
    if cache() {
        "anthropic"
    } else {
        "stub"
    }
}

/// The time the harness stamps where it writes a time itself, for either loop. Not 0: a
/// session off the compaction contract reads a zero stamp as none, as today's
/// `broca_core::message_times` does, so a zero here would hide a time that a loop really recorded.
const HARNESS_TIME_MS: u64 = 1;

/// The time the new loop reads: today's loop reads the wall clock, so a comparison whose result
/// depends on it does too; every other comparison reads [`HARNESS_TIME_MS`] and normalizes the
/// times recorded. A scoped account clock overrides both loops' time for a comparison that must
/// remain on one side of a reset regardless of how long it takes to run.
fn harness_now() -> u64 {
    if let Ok(now) = ACCOUNT_CLOCK.try_with(|clock| clock.now_ms()) {
        return now;
    }
    if cache() || account().is_some() || fallback().is_some() {
        broca_core::clock::Clock::default().now_ms()
    } else {
        HARNESS_TIME_MS
    }
}

tokio::task_local! {
    /// The keep-warm both loops' runs ask for, with each loop's scripted driver. `None` runs
    /// without keep-warm.
    static WARM: Option<Arc<WarmPair>>;
}

/// One scripted driver per loop, so a comparison can check both sent the same warms.
#[derive(Default)]
struct WarmPair {
    new: Arc<WarmScript>,
}

fn warm_script() -> Option<Arc<WarmPair>> {
    WARM.try_with(Clone::clone).ok().flatten()
}

/// A scripted keep-warm driver both loops get. The cache clock says the last request went out
/// at time 0 and a warm is due a millisecond after it, so every warm is due at once; each costs
/// one unit against a cache rewrite of three and a half, so one tool wait sends exactly three
/// warms and stops on its spending cap. It logs every warm with the run and model it was
/// asked to refresh.
#[derive(Default)]
struct WarmScript {
    log: Mutex<Vec<String>>,
}

impl WarmScript {
    fn timing(&self) -> Result<broca_core::keep_warm_wait::WarmTiming, String> {
        Ok(broca_core::keep_warm_wait::WarmTiming {
            ttl_ms: 300_000,
            lead_ms: 30_000,
            after_send_ms: Some(1),
        })
    }
    fn costs(&self) -> Result<broca_core::keep_warm::WarmCosts, String> {
        // Three warms reach one rewrite's cost, so each wait sends three.
        Ok(broca_core::keep_warm::WarmCosts {
            warm: 1.0,
            bust: 3.0,
        })
    }
    fn note(&self, attempt: &broca_protocol::ModelAttempt) {
        self.log.lock().unwrap().push(format!(
            "note attempt {} {:?}",
            attempt.attempt, attempt.outcome
        ));
    }
    fn warm(&self, run_id: &RunId, model: &ModelHandle) -> broca_core::keep_warm_wait::WarmTick {
        self.log
            .lock()
            .unwrap()
            .push(format!("warm {} {}", run_id.0, model.model_id));
        broca_core::keep_warm_wait::WarmTick::Sent
    }
}

/// The script as the new loop calls it: with the run's admission.
struct NewWarmer(Arc<WarmScript>);

#[async_trait::async_trait]
impl broca_core::keep_warm_wait::KeepWarmDriver<Admission> for NewWarmer {
    fn timing(&self, _: &Admission) -> Result<broca_core::keep_warm_wait::WarmTiming, String> {
        self.0.timing()
    }
    fn last_sent_at_ms(&self) -> Option<u64> {
        Some(0)
    }
    fn note_attempt(&self, attempt: &broca_protocol::ModelAttempt) {
        self.0.note(attempt);
    }
    fn costs(&self, _: &Admission) -> Result<broca_core::keep_warm::WarmCosts, String> {
        self.0.costs()
    }
    async fn warm(
        &self,
        admission: &Admission,
        run_id: &RunId,
    ) -> broca_core::keep_warm_wait::WarmTick {
        self.0.warm(run_id, &admission.model)
    }
}

fn new_warmer() -> Option<Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver<Admission>>> {
    warm_script().map(|script| {
        Arc::new(NewWarmer(Arc::clone(&script.new)))
            as Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver<Admission>>
    })
}

tokio::task_local! {
    /// The refusal-fallback model the run froze, with its context window. `None` freezes none.
    static FALLBACK: Option<FallbackScript>;
}

#[derive(Clone)]
struct FallbackScript {
    model: ModelHandle,
    context_limit: Option<u64>,
}

fn fallback() -> Option<FallbackScript> {
    FALLBACK.try_with(Clone::clone).ok().flatten()
}

/// Runs `compare` with a run whose provider froze `fallback-model` as its refusal fallback.
async fn compare_with_fallback<F: std::future::Future<Output = ()>>(
    context_limit: Option<u64>,
    compare: F,
) {
    FALLBACK
        .scope(
            Some(FallbackScript {
                model: ModelHandle::new(provider_id(), "fallback-model"),
                context_limit,
            }),
            compare,
        )
        .await;
}

/// The run's own model refusing a step with a content filter.
fn refusal(id: &str) -> Vec<NormalizedEvent> {
    answer(
        id,
        vec![ContentBlock::Text {
            text: "I can't help with that.".into(),
        }],
        FinishReason::ContentFilter,
    )
}

tokio::task_local! {
    /// Whether a resume's provider renders with another config than the run froze.
    static DRIFT: bool;
}

fn drift() -> bool {
    DRIFT.try_with(|drift| *drift).unwrap_or(false)
}

/// Runs `compare` with every resume's provider rendering with a changed config.
async fn compare_drifted<F: std::future::Future<Output = ()>>(compare: F) {
    DRIFT.scope(true, compare).await;
}

tokio::task_local! {
    /// The provider account a comparison routes, with the boot cap on same-account waits.
    /// `None` routes no account, and no run may pause on one.
    static ACCOUNT: Option<AccountScript>;
    /// A frozen clock for comparisons that must stay before the account's reset under load.
    static ACCOUNT_CLOCK: broca_core::Clock;
}

#[derive(Clone)]
struct AccountScript {
    selection: broca_records::record::AuthSelection,
    wait: broca_core::SameAccountWait,
    /// Whether a pause comparison waits for the account to reset before resuming.
    wait_for_reset: bool,
}

fn account() -> Option<AccountScript> {
    ACCOUNT.try_with(Clone::clone).ok().flatten()
}

fn account_selection() -> Option<broca_records::record::AuthSelection> {
    account().map(|script| script.selection)
}

fn routed(account: &str) -> broca_records::record::AuthSelection {
    serde_json::from_value(json!({
        "credential_id": "oauth:anthropic",
        "account_ref": account,
    }))
    .expect("an auth selection decodes")
}

/// An attempt the provider refused because the account's limit is reached; the provider stamps
/// when, and the reset shortly after.
fn limited_attempt(number: u32) -> NormalizedEvent {
    let NormalizedEvent::Control(ProviderControl::AttemptFinished { mut attempt }) =
        attempt(number, 429, "http_error")
    else {
        unreachable!("attempt builds an attempt")
    };
    attempt.request_body_sha256 = RENDERED.into();
    attempt.account_observation.limit = Some(broca_protocol::provider::AttemptAccountLimit {
        disposition: broca_protocol::provider::AccountLimitDisposition {
            kind: broca_protocol::provider::AccountLimitKind::AccountLimit,
            scope: broca_protocol::provider::AccountLimitScope::Account,
        },
        observed_at_ms: harness_now(),
        resets_at_ms: Some(harness_now()),
        resets_at_source: None,
        limit_window: None,
    });
    NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt })
}

/// A step the provider refused for the account's limit.
fn account_limited() -> Vec<NormalizedEvent> {
    account_limited_reporting(RENDERED)
}

/// A step the provider refused for the account's limit, its attempt reporting `request` as
/// the request it was sent with.
fn account_limited_reporting(request: &str) -> Vec<NormalizedEvent> {
    let mut attempt = limited_attempt(1);
    if let NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) = &mut attempt {
        attempt.request_body_sha256 = request.into();
    }
    vec![
        attempt,
        NormalizedEvent::Control(ProviderControl::Error {
            error: serde_json::from_value(json!({
                "class": "transient",
                "message": "the account's usage limit is reached",
                "status": 429
            }))
            .unwrap(),
            retries_used: 0,
        }),
    ]
}

/// Every account-limit pause the new loop wrote names, by seq, the attempt record the provider
/// refused for the account's limit (the one recording the limit's disposition), as a resume's
/// timer reads it back. The comparison masks the
/// seq, since each format numbers its own records.
fn pause_names_its_attempt(writer: &Recording<'_, '_>) {
    let first_seq = writer.session.next_seq() - writer.records.len() as u64;
    for record in &writer.records {
        let Record::Pause(pause) = record else {
            continue;
        };
        let Some(limit) = pause
            .account_limit
            .as_ref()
            .map(broca_protocol::json::Json::parse)
        else {
            continue;
        };
        let seq = limit["failed_attempt_seq"]
            .as_u64()
            .expect("a pause names its attempt");
        let named = &writer.records[(seq - first_seq) as usize];
        assert!(
            matches!(named, Record::Attempt(attempt)
            if attempt.billing.as_ref().is_some_and(|billing| {
                !billing.parse()["account_limit_disposition"].is_null()
            })),
            "the pause names {named:?}"
        );
    }
}

/// Every key the new loop gave a call answered without a dispatch intent names the outcome that
/// carries it, by the writer's generation and that outcome's seq, so no two calls share one. The
/// comparison masks the key itself, since today's is made of the log's lineage and its own seq;
/// it still compares which outcomes carry one.
fn outcome_keys_name_their_place(writer: &Recording<'_, '_>) {
    let first_seq = writer.session.next_seq() - writer.records.len() as u64;
    let prefix = writer.session.message_id_prefix();
    for (index, record) in writer.records.iter().enumerate() {
        let Record::ToolOutcome(outcome) = record else {
            continue;
        };
        if let Some(key) = &outcome.call_key {
            assert_eq!(
                *key,
                format!("{prefix}.{}", first_seq + index as u64),
                "key of call {}",
                outcome.tool_call_id.0
            );
        }
    }
}

/// Waits until a scripted account limit has reset, when the comparison routes an account.
async fn reset_passes() {
    if account().is_some_and(|script| script.wait_for_reset) {
        tokio::time::sleep(std::time::Duration::from_millis(RESETS_AFTER_MS + 50)).await;
    }
}

/// Runs `compare` with `script` routing both loops' account.
async fn compare_routed<F: std::future::Future<Output = ()>>(script: AccountScript, compare: F) {
    ACCOUNT.scope(Some(script), compare).await;
}

tokio::task_local! {
    /// Each loop's account reports, logged so a comparison can check both reported the same.
    /// `None` runs without a reporter.
    static REPORTS: Option<Arc<ReportPair>>;
}

#[derive(Default)]
struct ReportPair {
    new: Arc<Mutex<Vec<String>>>,
}

fn reports() -> Option<Arc<ReportPair>> {
    REPORTS.try_with(Clone::clone).ok().flatten()
}

/// What an attempt report says, without the record it names: the two formats number records
/// differently.
fn reported_attempt(attempt: &broca_protocol::ModelAttempt) -> String {
    format!(
        "attempt {} {:?} {:?}",
        attempt.attempt, attempt.outcome, attempt.http_status
    )
}

/// The new loop's reports, which also checks each names its own record.
struct NewReports(Arc<Mutex<Vec<String>>>);

impl AccountReports for NewReports {
    fn run_started(&self, event_id: &str, seq: u64) {
        assert!(
            event_id.ends_with(&format!(":{seq}")),
            "a start's event id names its record"
        );
        self.0.lock().unwrap().push("run started".into());
    }
    fn attempt_recorded(&self, event_id: &str, seq: u64, attempt: &broca_protocol::ModelAttempt) {
        assert!(
            event_id.ends_with(&format!(":{seq}")),
            "an attempt's event id names its record"
        );
        self.0.lock().unwrap().push(reported_attempt(attempt));
    }
}

/// Runs `compare` with an account reporter on both loops, then checks both reported the same,
/// and that they reported at least `at_least` events.
async fn compare_reported<F: std::future::Future<Output = ()>>(at_least: usize, compare: F) {
    let pair = Arc::new(ReportPair::default());
    REPORTS.scope(Some(Arc::clone(&pair)), compare).await;
    let new = pair.new.lock().unwrap().clone();
    broca_test_support::pin!("account reports", new);
    assert!(new.len() >= at_least, "the new loop's reports: {new:?}");
}

/// The structured output the comparison running in this task freezes.
fn structured() -> Option<Value> {
    STRUCTURED.try_with(Clone::clone).ok().flatten()
}

/// Structured output in `mode`, delivered without provider enforcement, so its schema never
/// needs the strict-grammar downgrade.
fn unconstrained(mode: &str) -> Value {
    json!({
        "mode": mode,
        "name": "result",
        "schema_jcs": r#"{"properties":{"answer":{"type":"integer"}},"required":["answer"],"type":"object"}"#,
        "enforcement": "best_effort",
        "reason": "route_unsupported",
        "delivery": "unconstrained",
    })
}

/// The context limit both loops freeze: the compaction script's, else the output script's.
fn frozen_context_limit() -> Option<u64> {
    compaction().map(|script| script.context_limit)
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
    /// A view that shortens a tool output without removing the request's final turn.
    Trim(u64),
    /// A view that cuts through the entry at this ordinal: it and everything before it no
    /// longer render.
    Cut(u64),
    /// A view that cuts through the first ordinal and shortens the tool output at the second.
    CutAndTrim(u64, u64),
    /// No view: the ride is answered `nothing`, as an empty script answers every ride.
    Nothing,
}

/// A compaction provider that answers from a script and records every call, so both loops must
/// make the same calls. Message ids differ between the loops by design, so the provider learns
/// them from the entries it's told about, and a view names entries by those ids.
struct CompactionSpy {
    rides: Mutex<VecDeque<Ride>>,
    mids: Mutex<std::collections::BTreeMap<u64, String>>,
    tool_parts: Mutex<std::collections::BTreeMap<u64, (String, String)>>,
    version: std::sync::atomic::AtomicU64,
    calls: Mutex<Vec<(String, Value)>>,
    /// The one answer that says the provider lost its Setup, if the comparison scripts one.
    lost: Mutex<Option<Lost>>,
    notices: std::sync::atomic::AtomicUsize,
    rides_asked: std::sync::atomic::AtomicUsize,
}

tokio::task_local! {
    /// The answer, if any, with which the compaction provider says it lost the session's Setup,
    /// as a provider restarted mid-run does.
    static LOST: Lost;
}

tokio::task_local! {
    /// The entries a resumed run's compaction provider reads for itself after its Setup, as a
    /// real provider does with `session.entries`. A run resumed from a cut log sends no notice
    /// repeating the window Setup covers (compaction N6), so without this read the provider
    /// would never learn the ids of the entries written before the cut.
    static PROVIDER_READ: Option<Arc<Value>>;
}

/// Where a restarted compaction provider first hears from the run again.
#[derive(Clone, Copy, Debug)]
enum Lost {
    /// The `n`th notice (from 1) is answered `setup_required`.
    Notice(usize),
    /// The `n`th ride (from 1) is refused `unknown_session`.
    Ride(usize),
}

impl CompactionSpy {
    fn new(script: &CompactionScript) -> Self {
        let mut mids = std::collections::BTreeMap::new();
        let mut tool_parts = std::collections::BTreeMap::new();
        if let Ok(Some(read)) = PROVIDER_READ.try_with(Clone::clone) {
            remember_entries(&read, &mut mids, &mut tool_parts);
        }
        Self {
            rides: Mutex::new(script.rides.iter().copied().collect()),
            mids: Mutex::new(mids),
            tool_parts: Mutex::new(tool_parts),
            version: Default::default(),
            calls: Mutex::default(),
            lost: Mutex::new(LOST.try_with(|lost| *lost).ok()),
            notices: Default::default(),
            rides_asked: Default::default(),
        }
    }

    /// The spy as a provider that has read `session`'s entries for itself, as a real provider
    /// reads `session.entries` after its Setup. A Setup names the newest entry, so no notice
    /// repeats the window it covers (compaction N6); without this read a scripted view could not
    /// name an entry written before the run's Setup. Returns the entries read.
    async fn read_session(&self, session: &Session<&MemoryBackend>) -> Value {
        let entries: Vec<_> = session
            .compaction_entries(1, usize::MAX)
            .await
            .unwrap()
            .iter()
            .map(|stored| broca_session_loop::entries::project(stored).unwrap())
            .collect();
        let read = serde_json::to_value(entries).unwrap();
        remember_entries(
            &read,
            &mut self.mids.lock().unwrap(),
            &mut self.tool_parts.lock().unwrap(),
        );
        read
    }

    /// Whether this call is the one the script answers as a provider that lost its Setup; it is
    /// answered so once.
    fn loses_setup(&self, method: &str) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        let mut lost = self.lost.lock().unwrap();
        let hit = match (*lost, method) {
            (Some(Lost::Notice(n)), "compaction.notice") => {
                self.notices.fetch_add(1, SeqCst) + 1 == n
            }
            (Some(Lost::Ride(n)), "compaction.ride") => {
                self.rides_asked.fetch_add(1, SeqCst) + 1 == n
            }
            _ => false,
        };
        if hit {
            *lost = None;
        }
        hit
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

    /// [`Self::calls`] without the conditions' `last_prompt_view`, for comparing the new loop's
    /// calls with today's: today's loop never ties its reading to a view, so it never sends
    /// the member, and the new loop's value is checked by tests of its own. The member never
    /// comes without the reading it describes.
    fn calls_but_prompt_view(&self) -> Vec<String> {
        fn strip(value: &mut Value) {
            match value {
                Value::Object(members) => {
                    if members.remove("last_prompt_view").is_some() {
                        assert!(
                            members.contains_key("last_prompt_tokens"),
                            "a view named without a reading: {members:?}"
                        );
                    }
                    members.values_mut().for_each(strip);
                }
                Value::Array(items) => items.iter_mut().for_each(strip),
                _ => {}
            }
        }
        let mut names = std::collections::HashMap::new();
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(method, params)| {
                let mut params = params.clone();
                strip(&mut params);
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

/// Remember entry ids and the latest tool-part digests from notices, so a replacement names
/// the completed output it was built from rather than the earlier running call.
fn remember_entries(
    value: &Value,
    mids: &mut std::collections::BTreeMap<u64, String>,
    tool_parts: &mut std::collections::BTreeMap<u64, (String, String)>,
) {
    match value {
        Value::Object(members) => {
            if let (Some(ordinal), Some(mid)) = (
                members.get("ordinal").and_then(Value::as_u64),
                members.get("mid").and_then(Value::as_str),
            ) {
                mids.insert(ordinal, mid.to_owned());
                if let Some(parts) = members.get("parts").and_then(Value::as_array) {
                    if let Some(tool) = parts.iter().find(|part| part["kind"] == "tool") {
                        tool_parts.insert(
                            ordinal,
                            (
                                tool["id"].as_str().unwrap().to_owned(),
                                tool["digest"].as_str().unwrap().to_owned(),
                            ),
                        );
                    }
                }
            }
            members
                .values()
                .for_each(|member| remember_entries(member, mids, tool_parts));
        }
        Value::Array(items) => items
            .iter()
            .for_each(|item| remember_entries(item, mids, tool_parts)),
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

    /// The provider reads the signed floor, so both loops must report the same one.
    async fn provider_capabilities(&self, _: &str) -> broca_core::ProviderCapabilities {
        let mut provides = vec![cortexkit_role_compaction::COMPACTION_SIGNED_FLOOR_PROVIDES.into()];
        if statement_seq::declared() {
            provides.push(cortexkit_role_compaction::COMPACTION_STATEMENT_SEQ_PROVIDES.into());
        }
        broca_core::ProviderCapabilities::Declared(provides)
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        scope: Option<&broca_protocol::SessionScope>,
        method: &str,
        params: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        remember_entries(
            &params,
            &mut self.mids.lock().unwrap(),
            &mut self.tool_parts.lock().unwrap(),
        );
        // The scope the call is routed under is part of what both loops must do alike.
        let routed = match scope {
            Some(scope) => format!("{method} scope={}", scope.scope_ref),
            None => method.into(),
        };
        self.calls.lock().unwrap().push((routed, params));
        if self.loses_setup(method) {
            if method == "compaction.ride" {
                return Err(broca_core::compaction_v2::runtime::CallError::Refused(
                    cortexkit_role_compaction::wire::Refusal::unknown_session(),
                ));
            }
            return Ok(
                serde_json::value::to_raw_value(&json!({"answer": "setup_required"}))
                    .expect("an answer serializes"),
            );
        }
        let answer = match method {
            "compaction.setup" => json!({"answer": "ready"}),
            "compaction.notice" => json!({"answer": "received"}),
            "compaction.ride" => match self.rides.lock().unwrap().pop_front() {
                None | Some(Ride::Nothing) => json!({"answer": "nothing"}),
                Some(ride) => {
                    use cortexkit_role_compaction::view::{Edit, EditOp, View};
                    let mid = |ordinal: u64| self.mids.lock().unwrap()[&ordinal].clone();
                    let trim = |ordinal| {
                        let (part, expect) = self.tool_parts.lock().unwrap()[&ordinal].clone();
                        EditOp::Replace {
                            mid: mid(ordinal),
                            part,
                            field: "output".into(),
                            value: json!("compacted tool output"),
                            expect: expect.into(),
                        }
                    };
                    let version = 1 + self
                        .version
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let ops = match ride {
                        Ride::Drop(ordinal) => vec![EditOp::Drop {
                            mid: mid(ordinal),
                            part: None,
                        }],
                        Ride::Trim(ordinal) => vec![trim(ordinal)],
                        Ride::Cut(ordinal) => vec![EditOp::Cut {
                            through: mid(ordinal),
                        }],
                        Ride::CutAndTrim(cut, tool) => {
                            vec![EditOp::Cut { through: mid(cut) }, trim(tool)]
                        }
                        Ride::Nothing => unreachable!("answered above"),
                    };
                    let view = View::seal(
                        version,
                        ops.into_iter().map(|op| Edit::new(version, op)).collect(),
                    );
                    json!({"answer": "view", "view": view})
                }
            },
            other => panic!("unexpected compaction method {other}"),
        };
        Ok(serde_json::value::to_raw_value(&answer).expect("an answer serializes"))
    }
}

tokio::task_local! {
    /// When set, the new loop's run leaves here each window message whose content it released,
    /// by ordinal, read back whole.
    static RELEASED: std::cell::RefCell<Vec<(u64, Arc<Message>)>>;
}

tokio::task_local! {
    /// The layout a comparison's session starts under: the current compaction contract (a plan
    /// version), or the established layout (`None`). Every comparison runs once in each.
    static LAYOUT: Option<u32>;
}

tokio::task_local! {
    /// Whether the new loop writes through an `ActorWriter`, as it does in the module, instead of
    /// holding the session itself. Every comparison runs once each way.
    static ACTOR: bool;
}

fn actor_mode() -> bool {
    ACTOR.try_with(|actor| *actor).unwrap_or(false)
}

/// Runs `body` once with the new loop holding the session and once writing through an actor.
/// Every comparison body runs through this, whatever layout its caller chose.
async fn with_each_writer<F: std::future::Future<Output = ()>>(body: impl Fn() -> F) {
    for actor in [false, true] {
        ACTOR.scope(actor, body()).await;
    }
}

/// Runs one call of the new loop, `$call`, with `$w` bound to its writer. That's the recording
/// itself, or in actor mode an `ActorWriter` whose actor owns the recording for the call. The
/// actor appends what other appenders injected as its own records, before each request that
/// appends or reads queued sends: the same points the recording appends them when the loop holds
/// it, so both modes write the same records in the same order.
macro_rules! through {
    ($recording:expr, |$w:ident| $call:expr) => {{
        if actor_mode() {
            let recording = &mut $recording;
            let (mut client, mut link) = ActorWriter::new(&*recording, Writer::media(&*recording));
            let injected = Arc::clone(&recording.injected);
            let call = async {
                let $w = &mut client;
                $call
            };
            let actor = async {
                while let Some(request) = link.next().await {
                    if matches!(
                        request,
                        Request::Append { .. } | Request::Hold { .. } | Request::SendRecords { .. }
                    ) {
                        let pending: Vec<Record> = std::mem::take(&mut *injected.lock().unwrap());
                        if !pending.is_empty() {
                            let first = recording
                                .append(&pending, true)
                                .await
                                .expect("the actor appends what was injected");
                            link.note_own(first, &pending);
                        }
                    }
                    link.serve(&mut *recording, request).await;
                }
            };
            let output = {
                tokio::pin!(call);
                tokio::pin!(actor);
                tokio::select! {
                    biased;
                    output = &mut call => output,
                    () = &mut actor => unreachable!("the actor serves until the run's writer is gone"),
                }
            };
            // The run's copy of the folds must end equal to the session's own: requests and
            // records alone can match while the copy has drifted, for instance keeping a window
            // that a seal shortened.
            // Both copies release the same content, and compare whole, read back. The actor is
            // no longer served here, so the run's copy reads back through the session itself.
            assert_eq!(
                client.conversation().released_ordinals(),
                recording.session.conversation.released_ordinals(),
                "the run's copy released other content than the session's"
            );
            let mut copy = client.conversation().clone();
            for ordinal in copy.released_ordinals() {
                let whole = recording
                    .session
                    .window_message_whole(ordinal)
                    .await
                    .expect("the session reads back");
                assert!(copy.restore(ordinal, whole), "the run's copy restores {ordinal}");
            }
            assert_eq!(
                copy.encode(),
                broca_session_loop::writer::whole_conversation(&*recording.session)
                    .await
                    .expect("the session reads back")
                    .encode(),
                "the run's copy of the conversation drifted from the session's"
            );
            assert_eq!(
                client.operations().encode(),
                recording.session.operations.encode(),
                "the run's copy of the run state drifted from the session's"
            );
            output
        } else {
            let $w = &mut $recording;
            $call
        }
    }};
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
    manifest: Option<Arc<broca_records::record::PlanManifest>>,
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
                })
                .with_tool("drained", |_| StubReply::ScopeDrained {
                    reason: "scope_ended".into(),
                    sent: false,
                })
                .with_tool("drained_sent", |_| StubReply::ScopeDrained {
                    reason: "scope_ended".into(),
                    sent: true,
                }),
        )
    }

    fn request_for(&self, run_id: &str, input: &[Message]) -> RunRequest {
        let mut request = self.plain_request_for(run_id, input);
        if RECORDED_MARKS.try_with(|_| ()).is_ok() {
            request.principal = Some(owner());
        }
        if send_facts() {
            request.send_id = Some(format!("send-{run_id}"));
            request.request_identity_digest = Some(format!("digest-{run_id}"));
            request.title = Some("a titled send".into());
            request.session_class = Some("mason".into());
            request.principal = Some(owner());
            request.scope = Some(broca_protocol::SessionScope {
                owner: owner(),
                scope_ref: "scope-1".into(),
                scope_epoch: 3,
                flow_id: Some("flow-1".into()),
                sender: None,
                agent_id: None,
                run_id: None,
                agent_id_frozen: false,
            });
            request.route_decision_id = Some("decision-1".into());
            request.directives = Some(vec![broca_protocol::request::DirectiveName::Wake]);
            request.work_class = Some("coding".into());
            request.variant = Some("high".into());
            request.service_tier = Some(broca_protocol::request::ServiceTier::Priority);
            request.context_management = Some(broca_protocol::request::ContextManagement::None);
        }
        request
    }

    fn plain_request_for(&self, run_id: &str, input: &[Message]) -> RunRequest {
        let tools = RUN_TOOLS
            .try_with(|tools| tools.get(run_id).cloned())
            .ok()
            .flatten()
            .unwrap_or_else(|| self.tools.clone());
        RunRequest {
            route_decision_id: None,
            account_route: None,
            input_written_at_ms: None,
            directives: None,
            session_class: None,
            retention: None,
            retention_policy: None,
            title: None,
            on_restart: None,
            keep_warm: warm_script().map(|_| broca_protocol::request::KeepWarm::Auto),
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId(run_id.into()),
            session: bind(),
            model: ModelHandle::new(provider_id(), run_model(run_id)),
            input: input.to_vec(),
            policy_allowed: tools.iter().map(|t| t.name.clone()).collect(),
            tools,
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig {
                temperature: temperature::REQUESTED
                    .try_with(|value| *value)
                    .unwrap_or_default(),
                ..Default::default()
            },
            cache: None,
            stop_when: self.stop_when.clone(),
            submission_id: None,
            budget: budget(),
            context_limit: frozen_context_limit(),
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen_cache: false,
            structured_output: None,
            head_turn: false,
        }
    }

    fn admission(&self, provider: &ScriptedProvider) -> Admission {
        self.admission_for(provider, "r1", &self.input)
    }

    /// The new loop's admission of a run of `input`, derived by `admit::admission` from the
    /// same request ([`Scenario::request_for`]) and run control fields ([`admission_control`])
    /// that `broca_core::run`'s runs in this file are given. `ts_ms` and `written_at_ms` are
    /// wall-clock times the comparison normalizes.
    fn admission_for(
        &self,
        provider: &ScriptedProvider,
        run_id: &str,
        input: &[Message],
    ) -> Admission {
        let mut request = self.request_for(run_id, input);
        broca_session_loop::admit::admission(
            &mut request,
            &admission_control(self),
            provider,
            &Default::default(),
            Some(1),
        )
        .expect("the shared rules admit the run")
    }
}

/// The run control fields that decide a new run's admission. Every run of `broca_core::run` in
/// this file is given them, and the new loop's admission is derived from the same values.
fn admission_control(scenario: &Scenario) -> RunControl {
    RunControl {
        display_message_marks: RECORDED_MARKS.try_with(|(marks, _)| marks.clone()).ok().flatten(),
        auth_selection: account_selection(),
        account_limit_pause: account().is_some(),
        manifest: scenario.manifest.clone(),
        new_log_compaction_plan_version: layout(),
        admitted_context_limit: frozen_context_limit(),
        hook_plane: Some(harness_hooks()),
        display_sent_by: send_facts().then(|| broca_protocol::BindPrincipal::Reserved {
            module_id: "occipital".into(),
        }),
        tools_were_omitted: send_facts(),
        input_hooks: send_facts().then(|| {
            Box::new(
                serde_json::from_value(json!({
                    "original": [{"role": "user", "content": [{"type": "text", "text": "raw"}]}],
                    "final": scenario.input,
                    "ops": [{"module": "magic-context", "hook": "pre_user", "phase": null, "op": "replace"}],
                }))
                .expect("hooks decode"),
            )
        }),
        ..Default::default()
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

/// Pins what the new loop did, labelled with `context` (see `broca_test_support::pins`): how
/// many requests and records, each request as `rendered` shows it, each record, and the tool
/// calls. Each value pinned is one the comparison beside it found alike with today's loop when
/// the pins were written.
fn pin_outcome(
    context: &str,
    requests: &[CallOptions],
    records: &[Value],
    calls: &[String],
    rendered: impl Fn(&CallOptions) -> CallOptions,
) {
    broca_test_support::pin!(format!("{context}request count"), requests.len());
    for (index, request) in requests.iter().enumerate() {
        broca_test_support::pin!(format!("{context}request {index}"), rendered(request));
    }
    pin_records(context, records);
    broca_test_support::pin!(format!("{context}tool calls"), calls);
}

/// Pins `records`, as [`pin_outcome`] does: their count, and the records together as one
/// entry, one compact JSON record per line, which keeps a long run's pins small.
fn pin_records(context: &str, records: &[Value]) {
    broca_test_support::pin!(format!("{context}record count"), records.len());
    let lines: Vec<String> = records.iter().map(Value::to_string).collect();
    broca_test_support::pin!(
        format!("{context}records"),
        broca_test_support::pins::Text(lines.join("\n"))
    );
}

/// A session writer that also keeps every record it appended, for comparison.
struct Recording<'s, 'b> {
    session: &'s mut Session<&'b MemoryBackend>,
    records: Vec<Record>,
    /// How many of `records` each appended batch ends at. A crash keeps whole batches, so a log
    /// cut short ends at one of these.
    batch_ends: Vec<usize>,
    /// When set, the backend the session is stored on: after each batch the writer keeps a
    /// snapshot of it in `snapshots`, what a crash right then would leave behind.
    backend: Option<&'b MemoryBackend>,
    snapshots: Vec<MemoryBackend>,
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
    fn snapshot(&mut self) {
        if let Some(backend) = self.backend {
            self.snapshots.push(backend.snapshot());
        }
    }

    async fn flush(&mut self) -> Result<(), SessionError> {
        let pending: Vec<Record> = std::mem::take(&mut *self.injected.lock().unwrap());
        if !pending.is_empty() {
            self.records.extend(pending.iter().cloned());
            self.batch_ends.push(self.records.len());
            self.media.append(self.session, &pending, true).await?;
            self.snapshot();
        }
        Ok(())
    }
}

impl Writer for Recording<'_, '_> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        self.flush().await?;
        self.records.extend_from_slice(records);
        self.batch_ends.push(self.records.len());
        let seq = self.media.append(self.session, records, barrier).await?;
        self.snapshot();
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
    async fn caught_up(&mut self) -> Result<(), SessionError> {
        // As the actor answers a catch-up: with the records it appended meanwhile.
        self.flush().await
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

    fn retired_tag_floor(&self) -> u64 {
        self.session.retired_tag_floor()
    }

    fn message_id_prefix(&self) -> String {
        self.session.message_id_prefix()
    }

    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        self.session.open_log_records().await
    }

    async fn enrollment(
        &self,
    ) -> Result<Option<broca_session_store::session::EnrollmentOverlay>, SessionError> {
        Writer::enrollment(&*self.session).await
    }

    async fn put_enrollment(
        &mut self,
        entries: &broca_session_store::record::EnrolledEntries,
    ) -> Result<broca_session_store::record::EnrollmentOverlayRef, SessionError> {
        Writer::put_enrollment(&mut *self.session, entries).await
    }

    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        Writer::run_start(&*self.session, run_id).await
    }

    async fn window_message_whole(
        &self,
        ordinal: u64,
    ) -> Result<std::sync::Arc<broca_protocol::Message>, SessionError> {
        Writer::window_message_whole(&*self.session, ordinal).await
    }

    async fn release(&mut self, blocks: &[(u64, Vec<usize>)]) -> Result<(), SessionError> {
        Writer::release(&mut *self.session, blocks).await
    }

    async fn tool_intent(
        &self,
        call_key: &str,
    ) -> Result<Option<broca_session_store::record::ToolIntent>, SessionError> {
        Writer::tool_intent(&*self.session, call_key).await
    }

    async fn seal_at_entry(
        &mut self,
        ordinal: u64,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        Writer::seal_at_entry(&mut *self.session, ordinal, carry, now_ms).await
    }

    async fn seal_ledger(
        &mut self,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        Writer::seal_ledger(&mut *self.session, carry, now_ms).await
    }

    async fn entry_by_mid(
        &self,
        mid: &str,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        Writer::entry_by_mid(&*self.session, mid).await
    }

    async fn entry_at(
        &self,
        ordinal: u64,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        Writer::entry_at(&*self.session, ordinal).await
    }

    async fn moved_through(&self) -> Result<Option<(String, u64)>, SessionError> {
        Writer::moved_through(&*self.session).await
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

tokio::task_local! {
    static DISPLAY_OUT: mpsc::Sender<OutputEvent>;
    static HOLD_STREAM: bool;
    static TEST_CANCEL: CancellationToken;
    static EXPECT_UNSUPPORTED: bool;
}

async fn new_loop(scenario: &Scenario) -> Outcome {
    new_from(scenario, None, scenario.steps.clone()).await
}

/// Runs the new loop on a new session. With `start` (a crash's cut log, converted), the session
/// first holds those records and the loop resumes its run; the outcome's records are only what
/// the loop appended.
/// What a new-loop session holds before the run a comparison drives: the records already in
/// its log, the setup parts they name, and whether they came from today's converter rather than
/// from the new loop's own log cut short.
struct Start {
    records: Vec<Record>,
    parts: std::collections::BTreeMap<broca_session_store::codec::Digest, Vec<u8>>,
    converted: bool,
    /// The backend a crash left the session on, when the start is a cut: the session reopens
    /// from it as after a restart, rather than from `records` appended to a new session.
    snapshot: Option<MemoryBackend>,
}

/// The new loop's whole run of `scenario`, kept so a comparison can cut it: what the run did,
/// the records its session holds, and the setup parts they name.
struct Source {
    outcome: Outcome,
    records: Vec<Record>,
    /// How many of `records` each batch ends at, the places a crash can cut the log.
    batch_ends: Vec<usize>,
    /// The backend as each batch left it, beside `batch_ends`; empty when not kept.
    snapshots: Vec<MemoryBackend>,
    parts: std::collections::BTreeMap<broca_session_store::codec::Digest, Vec<u8>>,
}

impl Source {
    /// The session `cut` leaves when a crash stops it after the first `kept` records reached
    /// the disk.
    fn cut(&self, kept: usize) -> Start {
        let snapshot = self
            .batch_ends
            .iter()
            .position(|&end| end == kept)
            .and_then(|batch| self.snapshots.get(batch))
            .map(MemoryBackend::snapshot);
        Start {
            records: self.records[..kept].to_vec(),
            parts: self.parts.clone(),
            converted: false,
            snapshot,
        }
    }

    /// How many model steps the first `kept` records hold an answer for, so a resume is served
    /// only the steps after them.
    /// How many scripted provider steps the first `kept` records answer: each answered step,
    /// each step the provider refused, which the run then sent to its fallback, and each step whose
    /// failure paused the run for a sign-in or an account limit.
    fn answered(&self, kept: usize) -> usize {
        self.records[..kept]
            .iter()
            .filter(|record| match record {
                Record::Assistant(_) | Record::StepRefused(_) => true,
                Record::Pause(pause) => matches!(
                    pause.reason,
                    PauseReason::AuthRequired | PauseReason::AccountLimit
                ),
                _ => false,
            })
            .count()
    }
}

/// A session converted from today's storage, as `tests/fixtures/converted/<name>.json` keeps it:
/// each record's kind and encoded body, and each setup part by digest, all hex. The fixtures
/// were written by today's loop and converter before today's storage was removed, so these
/// tests still start from what production's converted sessions hold.
fn converted_fixture(name: &str) -> Start {
    fn bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
            .collect()
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/converted")
        .join(format!("{name}.json"));
    let fixture: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let records = fixture["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| {
            Record::decode(
                record["kind"].as_u64().unwrap() as u32,
                &bytes(record["body"].as_str().unwrap()),
            )
            .expect("a fixture record decodes")
        })
        .collect();
    let parts = fixture["parts"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(digest, part)| {
            let digest: [u8; 32] = bytes(digest).try_into().unwrap();
            (
                broca_session_store::codec::Digest(digest),
                bytes(part.as_str().unwrap()),
            )
        })
        .collect();
    Start {
        records,
        parts,
        converted: true,
        snapshot: None,
    }
}

/// The split points among `points` (indexes into `records`; `records.len()` splits after the
/// last) where loading from a checkpoint written there, then folding the rest, ends in another
/// state than folding every record: a log can start at any record, so every split must agree.
/// The states are compared by their encoded bytes, which cover every field of both folds.
fn check_splits(records: &[Record], points: &[usize]) -> Result<Vec<usize>, String> {
    use broca_session_store::checkpoint::Checkpoint;
    use broca_session_store::codec::Message as _;
    use broca_session_store::fold::conversation::Conversation;
    use broca_session_store::record::Stream;
    fn fold(
        conversation: &mut Conversation,
        operations: &mut Operations,
        record: &Record,
    ) -> Result<(), String> {
        match record.stream() {
            Stream::Transcript => conversation
                .apply(record)
                .map_err(|e| format!("conversation: {e:?}")),
            Stream::Ledger => operations
                .apply(record)
                .map_err(|e| format!("operations: {e:?}")),
        }
    }
    let (mut conversation, mut operations) = (Conversation::new(false), Operations::default());
    for record in records {
        fold(&mut conversation, &mut operations, record)?;
    }
    let expected = (conversation.encode(), operations.encode());
    let mut differing = Vec::new();
    let mut running = (Conversation::new(false), Operations::default());
    for split in 0..=records.len() {
        if points.contains(&split) {
            let checkpoint = Checkpoint {
                covers_through: split as u64,
                next_seq: split as u64 + 1,
                conversation: running.0.clone(),
                operational: running.1.clone(),
                transcript_base: None,
                ledger_base: None,
                activity_ms: None,
                media: None,
            };
            let loaded = Checkpoint::decode(&checkpoint.encode())
                .map_err(|e| format!("checkpoint at {split} doesn't decode: {e}"))?;
            let (mut conversation, mut operations) = (loaded.conversation, loaded.operational);
            for record in &records[split..] {
                fold(&mut conversation, &mut operations, record)?;
            }
            if (conversation.encode(), operations.encode()) != expected {
                differing.push(split);
            }
        }
        if let Some(record) = records.get(split) {
            fold(&mut running.0, &mut running.1, record)?;
        }
    }
    Ok(differing)
}

/// A charge-basis resolver that answers each provider and auth method differently, so a fact
/// resolved under the wrong provider or method shows as a difference.
fn charge_resolver(provider: &str, method: &str) -> (broca_core::ChargeBasis, u32) {
    let basis = match (provider.len() + 3 * method.len()) % 3 {
        0 => broca_core::ChargeBasis::MeteredApi,
        1 => broca_core::ChargeBasis::SubscriptionIncluded,
        _ => broca_core::ChargeBasis::Unknown,
    };
    (basis, (provider.len() * 7 + method.len()) as u32)
}

async fn new_from(
    scenario: &Scenario,
    start: Option<Start>,
    steps: Vec<Vec<NormalizedEvent>>,
) -> Outcome {
    new_source(scenario, start, steps).await.outcome
}

/// [`new_from`], keeping the session's records and setup parts as a [`Source`].
async fn new_source(
    scenario: &Scenario,
    mut start: Option<Start>,
    steps: Vec<Vec<NormalizedEvent>>,
) -> Source {
    let bind = bind();
    let snapshot = start.as_mut().and_then(|start| start.snapshot.take());
    let reopened = snapshot.is_some();
    let backend = snapshot.unwrap_or_default();
    let triple = SessionTriple {
        project_root: "/project".into(),
        harness: "differential".into(),
        session: "s1".into(),
    };
    // A session converted from today's log off the compaction contract keeps text delivered
    // before ride envelopes existed joined into its results, as the converter sets it up.
    let converted_off_contract = layout().is_none()
        && start.as_ref().is_some_and(|start| {
            start.converted
                && start
                    .records
                    .iter()
                    .any(|r| matches!(r, Record::RunStart(_)))
        });
    let mut session = if reopened {
        Session::load(
            &backend,
            SessionAddr("0123456789abcdef".into()),
            &triple,
            &DeploymentKey::new(1, &[7; 32]),
        )
        .await
        .expect("the cut session reopens")
    } else {
        Session::create(
            &backend,
            SessionAddr("0123456789abcdef".into()),
            triple,
            &DeploymentKey::new(1, &[7; 32]),
            converted_off_contract,
        )
        .await
        .unwrap()
    };
    let mut parts = MemoryParts::default();
    // The records the session already held, so the appended ones' message ids number on from
    // theirs.
    let mut held = Vec::new();
    let resuming = match start {
        Some(cut) if reopened => {
            held = cut.records;
            parts.parts = cut.parts;
            true
        }
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
    // A resumed run's compaction provider reads the entries the session already holds after
    // its Setup, as a real provider does with `session.entries`.
    let read = match PROVIDER_READ.try_with(Clone::clone) {
        Ok(Some(read)) => Some(read),
        _ if resuming && layout().is_some() => {
            let entries: Vec<_> = session
                .entries(1, usize::MAX)
                .await
                .expect("the cut session's entries read")
                .iter()
                .map(|entry| {
                    broca_session_loop::entries::project(entry).expect("an entry projects")
                })
                .collect();
            Some(Arc::new(serde_json::to_value(entries).unwrap()))
        }
        _ => None,
    };
    let spy = compaction()
        .map(|script| Arc::new(PROVIDER_READ.sync_scope(read, || CompactionSpy::new(&script))));
    let provider = ScriptedProvider::new(steps);
    if held
        .iter()
        .any(|record| matches!(record, Record::StructuredDowngraded(_)))
    {
        provider
            .downgraded
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    if !held.is_empty() {
        provider.resuming();
    }
    let plane = scenario.plane();
    let lifecycle = plane.lifecycle();
    let mut writer = Recording {
        session: &mut session,
        records: Vec::new(),
        batch_ends: Vec::new(),
        backend: Some(&backend),
        snapshots: Vec::new(),
        injected: Default::default(),
        ended: Default::default(),
        media: Default::default(),
    };
    let now = harness_now;
    let cancel = TEST_CANCEL.try_with(Clone::clone).unwrap_or_default();
    let display_out = DISPLAY_OUT.try_with(Clone::clone).ok();
    let seams = Seams {
        person_steers: None,
        provider: &provider,
        tools: &plane,
        bind: bind.clone(),
        out: display_out.as_ref(),
        cancel: cancel.clone(),
        stop: Default::default(),
        held: Some(&*lifecycle),
        approval: &HarnessPolicy,
        hook_plane: Some(harness_hooks_cancelling(cancel)),
        now_ms: &now,
        session_address: Some(broca_core::session_refs::session_addr(&bind)),
        compaction_plane: spy
            .as_deref()
            .map(|spy| spy as &dyn broca_core::CompactionPlane),
        compaction_session: None,
        // Today's loop (`broca_core::run`) here runs with no `RunControl::compaction_changes`,
        // so it sends no change notices outside its steps, and the new one sends none either.
        compaction_changes: false,
        keep_warm: new_warmer(),
        account_reports: reports().map(|reports| {
            Arc::new(NewReports(Arc::clone(&reports.new))) as Arc<dyn AccountReports>
        }),
        max_same_account_wait: account().map_or_else(Default::default, |script| script.wait),
        auth_selection: account_selection(),
        step_progress: None,
        model_call_ready: provider.model_call_ready(),
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    let sealing = SEALING.try_with(|cause| *cause).ok();
    // A crash-cut comparison nests this driver several frames deep. Heap-own the loop bodies,
    // as the production executor does for run and resume, rather than copying them there.
    let result = through!(
        writer,
        |w| if let (true, Some(cause)) = (resuming, sealing) {
            seal(w, &parts, &seams, cause).await
        } else if resuming {
            Box::pin(resume(w, &parts, &seams)).await
        } else {
            Box::pin(run(w, &mut parts, scenario.admission(&provider), &seams)).await
        }
    );
    if EXPECT_UNSUPPORTED
        .try_with(|expected| *expected)
        .unwrap_or(false)
    {
        assert!(
            matches!(
                result,
                Err(broca_session_loop::run::LoopError::Unsupported(_))
            ),
            "{result:?}"
        );
        return Source {
            outcome: Outcome {
                records: Vec::new(),
                requests: Vec::new(),
                calls: Vec::new(),
            },
            records: Vec::new(),
            batch_ends: Vec::new(),
            snapshots: Vec::new(),
            parts: parts.parts,
        };
    }
    if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
        eprintln!(
            "loop result: {:?}",
            result
                .as_ref()
                .map(|finished| (finished.reason, finished.paused))
        );
    }
    result.expect("the new loop runs");
    let records = writer.records.clone();
    if RELEASED.try_with(|_| ()).is_ok() {
        let mut whole = Vec::new();
        for ordinal in writer.conversation().released_ordinals() {
            let message = writer
                .session
                .window_message_whole(ordinal)
                .await
                .expect("a released message reads back");
            whole.push((ordinal, message));
        }
        RELEASED.with(|seen| *seen.borrow_mut() = whole);
    }
    keys_match(writer.session, &plane).await;
    ids_resolve(writer.session).await;
    turns_have_facts(writer.session).await;
    outcome_keys_name_their_place(&writer);
    if !resuming {
        media_reads_back(writer.session).await;
        if writer.operations().view_applied.is_none() {
            assert!(writer.conversation().marker.is_none());
            assert_eq!(
                writer.conversation().window_start(),
                1,
                "without an applied view the run must not seal history"
            );
        }
    }
    let all: Vec<Record> = held.iter().chain(&records).cloned().collect();
    let batch_ends = writer
        .batch_ends
        .iter()
        .map(|end| held.len() + end)
        .collect();
    Source {
        outcome: Outcome {
            records: normalized(&all)[held.len()..].to_vec(),
            requests: sent_requests(&provider),
            calls: plane
                .log()
                .iter()
                .map(|event| format!("{event:?}"))
                .chain(spy.map_or_else(Vec::new, |spy| spy.calls_but_prompt_view()))
                .collect(),
        },
        records: all,
        batch_ends,
        snapshots: std::mem::take(&mut writer.snapshots),
        parts: parts.parts,
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
    // When a message was accepted or its request sent: a wall-clock time each loop takes.
    // Whether a message has one is still compared.
    "created_at_ms",
    "finished_at_ms",
    "dispatched_at_ms",
    "last_content_at_ms",
    "held_at_ms",
    "call_key",
    // A log position: each format numbers its own records.
    "resumed_from_seq",
    // The seq of the record that accepted a steered message, in each format's own numbering.
    "receipt_seq",
    // An account-limit pause names its failed attempt's record by seq, in each format's own
    // numbering; `pause_names_its_attempt` checks the new loop's names the right record.
    "failed_attempt_seq",
    // When an account limit was observed, paused on and resets: wall-clock times the two loops
    // take at different moments, computed by the same function from them.
    "observed_at_ms",
    "paused_at_ms",
    "resets_at_ms",
    "resume_not_before_ms",
    // When a refusal-fallback schedule last changed: a wall-clock time each loop takes at its
    // own moment. A schedule that records no time stays null and is still compared.
    "updated_at_ms",
];

/// New attempts have random durable identities; converted legacy attempts may have none.
/// Event tests join these ids to their own records instead of comparing unrelated logs' ids.
const INDEPENDENT_DURABLE_FIELDS: &[&str] = &["attempt_id"];

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
    session: &Session<&MemoryBackend>,
    triple: &SessionTriple,
    bind: &broca_protocol::BindIdentity,
) {
    assert!(
        Writer::conversation(session).window_start() > 1,
        "the new loop sealed nothing at the cut"
    );
    reloads_with_its_views(session, triple, bind).await;
}

/// A fresh load of a sealed session holds exactly the state the writer holds, and rebuilds both
/// compaction view slots from the new open log alone.
async fn reloads_with_its_views(
    session: &Session<&MemoryBackend>,
    triple: &SessionTriple,
    bind: &broca_protocol::BindIdentity,
) {
    use broca_session_store::codec::Message as _;
    let loaded = Session::load(
        session.backend(),
        SessionAddr("0123456789abcdef".into()),
        triple,
        &DeploymentKey::new(1, &[7; 32]),
    )
    .await
    .expect("the sealed session loads");
    // The live session may have released content its requests no longer send, so the
    // assertion compares the loaded conversation with the live one read back whole.
    assert_eq!(
        Writer::conversation(&loaded).encode(),
        broca_session_loop::writer::whole_conversation(session)
            .await
            .expect("the live session reads back")
            .encode()
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
async fn keys_match(session: &Session<&MemoryBackend>, plane: &Spy) {
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

/// Every message the session holds is found again by the id it was stored with. A new loop's id
/// names the seq of the message's entry facts, so an id minted from a seq the writer guessed
/// before its batch went in names another record whenever another appender (the session actor,
/// here what the scenario injected) appended first. Records alone can't show that: the
/// comparison renames ids by their order of appearance.
async fn ids_resolve(session: &Session<&MemoryBackend>) {
    for entry in session
        .entries(1, usize::MAX)
        .await
        .expect("the entries read")
    {
        let Some(facts) = &entry.state.facts else {
            continue;
        };
        assert_eq!(
            session
                .find_message(&facts.mid)
                .await
                .expect("the id lookup reads"),
            Some(entry.ordinal),
            "message {} is stored as {}",
            entry.ordinal,
            facts.mid
        );
    }
}

/// Every tool turn the session placed carries the facts of its calls (`TurnFacts`), whichever
/// way the turn closed: its results arriving, a crash's close-out, a cancel, or a batch that
/// sent nothing. `session.read` serves each call's key and dispatch from them, and no two calls
/// of a turn share a key.
async fn turns_have_facts(session: &Session<&MemoryBackend>) {
    for entry in session
        .entries(1, usize::MAX)
        .await
        .expect("the entries read")
    {
        let calls = entry
            .message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolCall(_)));
        if calls && !entry.open {
            assert!(
                entry.state.turn.is_some(),
                "tool turn {} has no turn facts",
                entry.ordinal
            );
        }
        // Each occurrence of a call has its own key, a repeated call id's copies included.
        if let Some(turn) = &entry.state.turn {
            let mut keys: Vec<&str> = turn
                .calls
                .iter()
                .map(|call| call.call_key.as_str())
                .collect();
            keys.sort_unstable();
            keys.dedup();
            assert_eq!(
                keys.len(),
                turn.calls.len(),
                "tool turn {} shares a key between calls: {:?}",
                entry.ordinal,
                turn.calls
            );
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
            map.retain(|key, _| !INDEPENDENT_DURABLE_FIELDS.contains(&key.as_str()));
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
    with_each_writer(|| Box::pin(compare_in_once(scenario.clone()))).await;
}

async fn compare_in_once(scenario: Scenario) {
    let new = new_loop(&scenario).await;
    if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
        for (index, record) in new.records.iter().enumerate() {
            eprintln!("new   {index}: {record}");
        }
    }
    pin_outcome("", &new.requests, &new.records, &new.calls, as_rendered);
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

/// A call's provider state survives the session's storage: a run resumed from its log after
/// the call's batch, as after a restart, sends the call back with the state the provider gave
/// it. A restart resends the whole history, so this is when a provider that checks replayed
/// calls against its own copy sees the stored form (see `ToolCall::provider_state`).
#[tokio::test]
async fn a_calls_provider_state_survives_a_resume() {
    let state = broca_protocol::content::TextProviderState {
        format: broca_protocol::content::TextProviderFormat::OpenAiResponses,
        data: r#"{"status":"completed"}"#.into(),
    };
    let mut first = call("c1", "read", json!({"path": "a"}));
    if let ContentBlock::ToolCall(call) = &mut first {
        call.provider_state = Some(state.clone());
    }
    let scenario = Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Sequential)],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer("a1", vec![first], FinishReason::ToolCalls),
            text_step("a2", "done"),
        ],
    };
    for layout in LAYOUTS {
        let scenario = scenario.clone();
        let state = state.clone();
        LAYOUT
            .scope(
                layout,
                Box::pin(async move {
                    let source = new_source(&scenario, None, scenario.steps.clone()).await;
                    // The first cut that keeps the call's step: the resume reads it from storage.
                    let cut = source
                        .batch_ends
                        .iter()
                        .copied()
                        .find(|&cut| source.answered(cut) == 1)
                        .expect("a cut after the call's batch");
                    let start = source.cut(cut);
                    assert!(
                        start.snapshot.is_some(),
                        "the resume reopens the stored session"
                    );
                    let steps = scenario.steps[source.answered(cut)..].to_vec();
                    let resumed = Box::pin(new_from(&scenario, Some(start), steps)).await;
                    let sent: Vec<_> = resumed.requests[0]
                        .prompt
                        .iter()
                        .flat_map(|message| message.content.iter())
                        .filter_map(|block| match block {
                            ContentBlock::ToolCall(call) => Some(call.provider_state.clone()),
                            _ => None,
                        })
                        .collect();
                    assert_eq!(sent, [Some(state)]);
                }),
            )
            .await;
    }
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

/// Cuts the new loop's log of `scenario` after each of its records, as a crash could, resumes
/// the cut, and pins what the resume sends, runs and appends.
/// Runs [`compare_every_cut_in`] in both layouts.
async fn compare_every_cut(scenario: Scenario) {
    for layout in LAYOUTS {
        LAYOUT
            .scope(layout, Box::pin(compare_every_cut_in(scenario.clone())))
            .await;
    }
}

async fn compare_every_cut_in(scenario: Scenario) {
    with_each_writer(|| Box::pin(compare_every_cut_in_once(scenario.clone()))).await;
}

async fn compare_every_cut_in_once(scenario: Scenario) {
    // The whole run the cuts are taken from. Only the resumes are pinned, so its run's warms
    // and account reports go to a driver and reporter of their own; the run still freezes the
    // keep-warm its resumes read back.
    let source_warms = warm_script().map(|_| Arc::new(WarmPair::default()));
    let source_reports = reports().map(|_| Arc::new(ReportPair::default()));
    let source = WARM
        .scope(
            source_warms,
            REPORTS.scope(
                source_reports,
                new_source(&scenario, None, scenario.steps.clone()),
            ),
        )
        .await;
    let records = source.records.len();
    assert!(records > 2, "the scenario wrote a log to cut");
    if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
        for (index, record) in source.outcome.records.iter().enumerate() {
            eprintln!("source {index}: {record}");
        }
        for call in &source.outcome.calls {
            eprintln!("source call  {call}");
        }
    }
    for cut in source
        .batch_ends
        .iter()
        .copied()
        .filter(|&end| end < records)
    {
        // The provider serves the steps the cut log has no answer for yet.
        let steps = scenario.steps[source.answered(cut)..].to_vec();
        let resumed = Box::pin(new_source(&scenario, Some(source.cut(cut)), steps)).await;
        let context = format!("cut after record {cut} of {records}");
        if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
            for (index, record) in resumed.outcome.records.iter().enumerate() {
                eprintln!("{context} new   {index}: {record}");
            }
            for call in &resumed.outcome.calls {
                eprintln!("{context} call  {call}");
            }
        }
        resume_invariants(&context, &source, cut, &resumed, Requests::Same);
        let new = resumed.outcome;
        pin_outcome(
            &format!("{context}: "),
            &new.requests,
            &new.records,
            &new.calls,
            Clone::clone,
        );
    }
}

/// What a resume from a crash cut must do whatever its pins say, checked from the records and
/// the tool plane rather than from the loop's own reading of the log:
///
/// - It dispatches no call the cut log holds a dispatch intent for: that call may already have
///   run, so running it again could repeat its side effect.
/// - No call gets a second result, and no run a second end.
/// - A resume that leaves a call's outcome unknown sends no further request.
/// - With [`Requests::Same`], every request a resume sends is the one the uninterrupted run sent
///   for that step, so the provider sees the same conversation and its prompt cache still holds.
///
/// `source` is the uninterrupted run, `cut` how many of its records the crash kept, and
/// `resumed` the session after the resume, its records starting with the cut ones.
fn resume_invariants(
    context: &str,
    source: &Source,
    cut: usize,
    resumed: &Source,
    requests: Requests,
) {
    use std::collections::{BTreeMap, BTreeSet};
    let kept = &source.records[..cut];
    assert_eq!(
        &resumed.records[..cut],
        kept,
        "{context}: the resume starts from the cut log"
    );
    let added = &resumed.records[cut..];
    let intended: BTreeSet<&str> = kept
        .iter()
        .filter_map(|record| match record {
            Record::ToolIntent(intent) => Some(intent.tool_call_id.0.as_str()),
            _ => None,
        })
        .collect();
    // The scenario's tool plane (`Spy`) logs each dispatch as `dispatch… <call id> …`.
    for call in &resumed.outcome.calls {
        let mut words = call.split(' ');
        if words
            .next()
            .is_some_and(|word| word.starts_with("dispatch"))
        {
            let id = words.next().unwrap_or_default();
            assert!(
                !intended.contains(id),
                "{context}: the resume dispatched {id} again, though the cut log shows it may \
                 have run: {call}"
            );
        }
    }
    let mut results: BTreeMap<(Option<&str>, &str), usize> = BTreeMap::new();
    let mut ends: BTreeMap<&str, usize> = BTreeMap::new();
    for record in &resumed.records {
        match record {
            Record::ToolReply(reply) => {
                *results
                    .entry((
                        reply.batch_id.as_deref(),
                        reply.result.tool_call_id.0.as_str(),
                    ))
                    .or_default() += 1;
            }
            Record::RunEnd(end) => *ends.entry(end.run_id.as_str()).or_default() += 1,
            _ => {}
        }
    }
    for ((batch, call), count) in results {
        assert_eq!(
            count, 1,
            "{context}: call {call} of batch {batch:?} has {count} results"
        );
    }
    for (run, count) in ends {
        assert_eq!(count, 1, "{context}: run {run} ended {count} times");
    }
    if let Some(unknown) = added.iter().position(|record| {
        matches!(record, Record::ToolOutcome(outcome)
            if outcome.outcome == broca_session_store::record::CallOutcome::Unknown)
    }) {
        assert!(
            !added[unknown..]
                .iter()
                .any(|record| matches!(record, Record::Attempt(_))),
            "{context}: the resume asked the model again after a call's outcome became unknown"
        );
    }
    let requests = RESUME_REQUESTS
        .try_with(|requests| *requests)
        .unwrap_or(requests);
    if requests == Requests::MayDiffer {
        return;
    }
    let sent = &resumed.outcome.requests;
    let Some(first) = sent.first() else {
        return;
    };
    // The uninterrupted run sent the steps the cut log answered first, so the resume's requests
    // start at or after those. A refused or retried attempt leaves no answer, so where exactly
    // is found by the first request itself.
    let answered = source.answered(cut);
    let original = &source.outcome.requests;
    let start = (answered..original.len())
        .find(|&index| original[index] == *first)
        .unwrap_or_else(|| {
            panic!(
                "{context}: the resume's first request is none the uninterrupted run sent after \
                 its first {answered} answered step(s)"
            )
        });
    assert!(
        start + sent.len() <= original.len(),
        "{context}: the resume sent {} request(s) from request {start}; the uninterrupted run \
         sent {}",
        sent.len(),
        original.len()
    );
    for (index, resent) in sent.iter().enumerate() {
        if *resent != original[start + index] && std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
            eprintln!(
                "{context}: resent request\n{}\noriginal request\n{}",
                serde_json::to_string_pretty(resent).unwrap(),
                serde_json::to_string_pretty(&original[start + index]).unwrap()
            );
        }
        assert!(
            *resent == original[start + index],
            "{context}: request {} differs from the uninterrupted run's",
            start + index
        );
    }
}

/// Whether a resume's requests must match the uninterrupted run's (see [`resume_invariants`]).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Requests {
    /// The resume sees everything the uninterrupted run saw, so it sends the same requests.
    Same,
    /// The uninterrupted run was given something the resume isn't: a steer or a cancel that
    /// wasn't durable when the crash came, or a compaction view the restarted compaction
    /// provider builds afresh.
    MayDiffer,
}

tokio::task_local! {
    /// Set to [`Requests::MayDiffer`] around a crash-cut test whose uninterrupted run is given
    /// something its resumes aren't (see [`Requests`]).
    static RESUME_REQUESTS: Requests;
}

/// At every cut of the new loop's log of `scenario` where the run was still running (no pause,
/// no end), pins the records the loop's crash-orphan seal writes. Asserts some cut seals.
async fn compare_seal_every_cut(scenario: Scenario) {
    for layout in LAYOUTS {
        LAYOUT
            .scope(
                layout,
                Box::pin(compare_seal_every_cut_in(scenario.clone())),
            )
            .await;
    }
}

async fn compare_seal_every_cut_in(scenario: Scenario) {
    use broca_core::seal::SealCause;
    let source = new_source(&scenario, None, scenario.steps.clone()).await;
    let records = source.records.len();
    let mut sealed = 0;
    for cut in source
        .batch_ends
        .iter()
        .copied()
        .filter(|&end| end < records)
    {
        let start = source.cut(cut);
        let mut operations = Operations::default();
        for record in &start.records {
            if record.stream() == broca_session_store::record::Stream::Ledger {
                operations.apply(record).expect("the cut folds");
            }
        }
        // The loop seals a run that was running when the crash cut it: one it would resume, or
        // one it stops for review.
        let running = operations.run.as_ref().is_some_and(|run| {
            use broca_session_store::fold::operations::Resolution;
            run.finished.is_none()
                && matches!(
                    run.resolution(),
                    Resolution::Resume | Resolution::Interrupted(_)
                )
        });
        if !running {
            continue;
        }
        sealed += 1;
        let new = SEALING
            .scope(
                SealCause::CrashOrphan,
                new_from(&scenario, Some(start), Vec::new()),
            )
            .await;
        let context = format!("seal after record {cut} of {records}");
        if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
            for (index, record) in new.records.iter().enumerate() {
                eprintln!("{context} new   {index}: {record}");
            }
        }
        pin_records(&format!("{context}: "), &new.records);
        assert!(new.requests.is_empty(), "{context}: a seal sends nothing");
        assert!(
            new.calls.iter().all(|call| !call.contains("dispatch")),
            "{context}: a seal dispatches nothing: {:?}",
            new.calls
        );
    }
    assert!(sealed > 0, "some cut leaves a running run to seal");
}

#[tokio::test]
async fn every_cut_of_a_tool_run_seals_alike() {
    compare_seal_every_cut(sequential_tool_run()).await;
}

#[tokio::test]
async fn every_cut_of_an_unsent_call_seals_alike() {
    compare_seal_every_cut(unsent_call()).await;
}

#[tokio::test]
async fn every_cut_of_parallel_calls_seals_alike() {
    compare_seal_every_cut(tool_run()).await;
}

fn sequential_tool_run() -> Scenario {
    Scenario {
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
    }
}

#[tokio::test]
async fn every_cut_of_a_tool_run_resumes_alike() {
    compare_every_cut(sequential_tool_run()).await;
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

#[path = "differential/temperature.rs"]
mod temperature;

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

/// A run paused for a sign-in after its tool step: a resume from any cut reaches the pause or,
/// past it, the run's end, and never sends the tool call again.
#[tokio::test]
async fn every_cut_of_a_sign_in_pause_resumes_alike() {
    compare_every_cut(Scenario {
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

/// A run paused on its account's limit after its tool step, the limit resetting before the
/// session's cache expires: a resume from any cut reaches the pause or the run's end, and never
/// sends the tool call again.
#[tokio::test]
async fn every_cut_of_an_account_limit_pause_resumes_alike() {
    compare_routed(
        AccountScript {
            selection: routed("acct-1"),
            wait: broca_core::SameAccountWait::default(),
            wait_for_reset: true,
        },
        compare_every_cut(tool_then_limited(account_limited())),
    )
    .await;
}

/// A run the runner stops between its answer and its tool batch: the calls close as not run and
/// the run ends interrupted, both in one append, so a resume from any cut either runs the step
/// on or finds the run ended.
#[tokio::test]
async fn every_cut_of_a_stop_before_a_batch_resumes_alike() {
    compare_actions_cut(
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
        vec![(1, Action::Stop)],
        Cuts {
            on_restart: Some(OnRestart::Resume),
            ..Cuts::default()
        },
    )
    .await;
}

/// A run the runner stops at a step boundary, under a policy that resumes it after the restart,
/// with a message steered in for its next step: a resume from any cut reaches the pause or, past
/// it, runs the steered step.
#[tokio::test]
async fn every_cut_of_a_restart_pause_resumes_alike() {
    compare_actions_cut(
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
        Cuts {
            on_restart: Some(OnRestart::Resume),
            ..Cuts::default()
        },
    )
    .await;
}

/// A run that stops at a step calling a named tool: a crash after that step's answer resumes to
/// the same stop, closing the call as not run instead of sending it.
#[tokio::test]
async fn every_cut_of_a_tool_call_stop_resumes_alike() {
    compare_every_cut(Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        manifest: None,
        stop_when: vec![StopCondition::HasToolCall {
            name: Some("read".into()),
        }],
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

fn unsent_call() -> Scenario {
    Scenario {
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
    }
}

#[tokio::test]
async fn every_cut_of_an_unsent_call_resumes_alike() {
    compare_every_cut(unsent_call()).await;
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
    with_each_writer(|| Box::pin(compare_runs_in_once(scenario.clone(), runs))).await;
}

async fn compare_runs_in_once(scenario: Scenario, runs: &[(&str, Vec<Message>)]) {
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
        batch_ends: Vec::new(),
        backend: None,
        snapshots: Vec::new(),
        injected: Default::default(),
        ended: Default::default(),
        media: Default::default(),
    };
    let mut parts = MemoryParts::default();
    let now = harness_now;
    let bind = bind();
    let seams = Seams {
        person_steers: None,
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
        session_address: Some(broca_core::session_refs::session_addr(&bind)),
        compaction_plane: spy
            .as_deref()
            .map(|spy| spy as &dyn broca_core::CompactionPlane),
        // One session state for the session's life, as the actor keeps it across runs.
        compaction_session: Some(Arc::default()),
        compaction_changes: false,
        keep_warm: new_warmer(),
        account_reports: reports().map(|reports| {
            Arc::new(NewReports(Arc::clone(&reports.new))) as Arc<dyn AccountReports>
        }),
        max_same_account_wait: account().map_or_else(Default::default, |script| script.wait),
        auth_selection: account_selection(),
        step_progress: None,
        model_call_ready: None,
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    for (index, (run_id, input)) in runs.iter().enumerate() {
        through!(writer, |w| run(
            w,
            &mut parts,
            scenario.admission_for(&provider, run_id, input),
            &seams,
        )
        .await)
        .expect("the new loop runs");
        // A scenario may seal the new loop's ledger between runs, which today's loop never
        // does; the next run must still send the same requests.
        if LEDGER_SEAL_BETWEEN_RUNS
            .try_with(|seal| *seal)
            .unwrap_or(false)
            && index + 1 < runs.len()
        {
            broca_session_loop::compaction::seal_ledger(
                &mut writer,
                seams
                    .compaction_session
                    .as_deref()
                    .expect("a compaction session"),
                0,
            )
            .await
            .expect("the ledger seals");
        }
    }
    keys_match(writer.session, &plane).await;
    ids_resolve(writer.session).await;
    turns_have_facts(writer.session).await;
    outcome_keys_name_their_place(&writer);
    if compaction().is_some_and(|script| {
        script
            .rides
            .iter()
            .any(|ride| matches!(ride, Ride::Cut(_) | Ride::CutAndTrim(..)))
    }) {
        sealed_at_the_cut(writer.session, &triple, &bind).await;
    }
    if LEDGER_SEAL_BETWEEN_RUNS
        .try_with(|seal| *seal)
        .unwrap_or(false)
    {
        reloads_with_its_views(writer.session, &triple, &bind).await;
    }
    let new = Outcome {
        records: normalized(&writer.records),
        requests: sent_requests(&provider),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .chain(
                spy.as_ref()
                    .map_or_else(Vec::new, |spy| spy.calls_but_prompt_view()),
            )
            .collect(),
    };

    if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
        for (index, record) in new.records.iter().enumerate() {
            eprintln!("new   {index}: {record}");
        }
    }
    pin_outcome("", &new.requests, &new.records, &new.calls, as_rendered);
    let _ = NEW_REQUESTS.try_with(|seen| seen.lock().unwrap().extend(new.requests.clone()));
    // Every call as the new loop made it, `last_prompt_view` included.
    let _ = NEW_CALLS.try_with(|seen| {
        seen.lock().unwrap().push(
            plane
                .log()
                .iter()
                .map(|event| format!("{event:?}"))
                .chain(spy.map_or_else(Vec::new, |spy| spy.calls()))
                .collect(),
        )
    });
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

/// A run declaring tools that differ from the run before it (one tool's description refreshed,
/// then the original tools again) moves the signed floor to its first message. Both loops fold
/// the floor from the runs' starts and send the same requests, the floor included.
#[tokio::test]
async fn runs_whose_tools_change_move_the_signed_floor_alike() {
    let read = tool("read", ExecutionMode::Sequential);
    let mut refreshed = read.clone();
    refreshed.description = "read something, skipping binary content".into();
    let thought = |id: &str, text: &str| {
        answer(
            id,
            vec![
                ContentBlock::Reasoning {
                    text: format!("thinking for {id}").into(),
                    signature: Some(format!("sig-{id}").into()),
                },
                ContentBlock::Text { text: text.into() },
            ],
            FinishReason::Stop,
        )
    };
    let scenario = Scenario {
        input: Vec::new(),
        tools: vec![read],
        manifest: None,
        stop_when: vec![StopCondition::StepCount { n: 8 }],
        steps: vec![
            thought("a1", "one"),
            thought("a2", "two"),
            thought("a3", "three"),
            thought("a4", "four"),
        ],
    };
    let seen = Arc::new(Mutex::new(Vec::new()));
    RUN_TOOLS
        .scope(
            [("r2".to_owned(), vec![refreshed])].into_iter().collect(),
            NEW_REQUESTS.scope(
                Arc::clone(&seen),
                compare_runs(
                    scenario,
                    &[
                        ("r1", vec![user("first task")]),
                        ("r2", vec![user("second task")]),
                        ("r3", vec![user("third task")]),
                        ("r4", vec![user("fourth task")]),
                    ],
                ),
            ),
        )
        .await;
    // Each run places its input and one answer: r2 starts at 2, r3 (back to r1's tools) at 4,
    // and r4 keeps r3's tools.
    let seen = seen.lock().unwrap();
    assert!(!seen.is_empty());
    for opts in seen.iter() {
        let run = opts.step_context.as_ref().unwrap().run_id.as_str();
        let expected = match run {
            "r1" => None,
            "r2" => Some(2),
            "r3" | "r4" => Some(4),
            other => panic!("unexpected run {other}"),
        };
        assert_eq!(opts.render_episodes.signed_floor, expected, "{run}");
    }
}

/// A send that sets every field a run's start records about it: both loops record each one the
/// same way, across a tool call and a second run.
#[tokio::test]
async fn every_send_fact_reaches_the_run_start() {
    let scenario = Scenario {
        input: vec![user("go")],
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
            text_step("a3", "done again"),
        ],
    };
    SEND_FACTS
        .scope(true, async {
            compare(scenario.clone()).await;
            compare_runs(
                scenario,
                &[
                    ("r1", vec![user("first task")]),
                    ("r2", vec![user("second task")]),
                ],
            )
            .await;
        })
        .await;
}

#[tokio::test]
async fn recorded_send_marks_decide_notice_origins_alike() {
    for (marks, notice) in [
        (Some(json!({"notice": true})), true),
        (Some(json!({"speaks_for": {"agent_id": "Ada"}})), false),
        (
            Some(json!({"speaks_for": {"agent_id": "Ada"}, "notice": true})),
            true,
        ),
        (
            Some(json!({"speaks_for": {"agent_id": "agent-a", "name": "Ada"}})),
            false,
        ),
        (
            Some(json!({"speaks_for": {"agent_id": "agent-a", "name": "Ada"}, "notice": true})),
            true,
        ),
        (None, true),
    ] {
        let marks = marks.map(|value| serde_json::from_value(value).unwrap());
        RECORDED_MARKS
            .scope(
                (marks, notice),
                reading_entries(async {
                    compare(Scenario {
                        input: vec![user("owner words")],
                        tools: vec![],
                        manifest: None,
                        stop_when: vec![StopCondition::StepCount { n: 2 }],
                        steps: vec![text_step("a1", "answer")],
                    })
                    .await;
                    compare_steered(calling("read", 0), vec![a_note("owner steer")]).await;
                }),
            )
            .await;
    }
}

/// A later run that sends the session's system prompt again, as a client naturally does on every
/// turn, renders it once. Outside the compaction contract the repeat is stripped from the run's
/// input before it is written; on the contract it is the session's system text, accepted again.
#[tokio::test]
async fn a_later_run_resending_the_system_prompt_renders_it_once() {
    compare_runs(
        Scenario {
            input: Vec::new(),
            tools: vec![tool("read", ExecutionMode::Sequential)],
            manifest: None,
            stop_when: vec![StopCondition::StepCount { n: 8 }],
            steps: vec![text_step("a1", "done"), text_step("a2", "done again")],
        },
        &[
            ("r1", vec![system("You are careful."), user("first task")]),
            ("r2", vec![system("You are careful."), user("second task")]),
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
    /// Cancels the run now, as the actor does for an interrupting send, while the provider's
    /// call hasn't returned its stream: the provider returns it only a second later.
    CancelNow,
    /// Cancels the run while the provider's call streams, from a provider that reports attempt
    /// evidence: as a transport does once it sees the cancel, the provider then sends the
    /// call's scripted events (its attempt in flight) a moment later and closes the stream.
    CancelMidStream,
    /// Asks the run to stop, as the runner does when it shuts down.
    Stop,
    /// Queues a send with a delivery order, as the actor does for a running run: its
    /// `delivery_ordered` and `queued` records, in today's format, after the `send_ended` of
    /// the send it supersedes when it replaces one.
    Ordered {
        send: Box<broca_core::delivery_order::OrderedSend>,
        records: Vec<Record>,
    },
}

/// A send queued with delivery `modes`, expiring at `expires_at_ms` when given.
fn ordered(id: &str, modes: &[&str], text: &str, expires_at_ms: Option<u64>) -> Action {
    let mut order = json!({"modes": modes});
    if let Some(expires) = expires_at_ms {
        order["expires_at_ms"] = json!(expires);
    }
    ordered_with(id, order, text, None)
}

/// A template use a send names, for a send that may go out as turn-scoped system text: its
/// text is the template's rendering, which admission did.
fn template_use() -> Value {
    json!({"id": "nudge", "digest": "0".repeat(64), "slots": {}})
}

/// A send queued with the delivery order `order`, as the session actor records it. `supersedes`
/// names the undelivered send under the same dedupe key that it replaces, which ends first.
fn ordered_with(id: &str, order: Value, text: &str, supersedes: Option<&str>) -> Action {
    let input = vec![user(text)];
    let send = broca_core::delivery_order::OrderedSend {
        admitted_at_ms: Some(HARNESS_TIME_MS),
        submission_id: broca_protocol::SubmissionId(id.into()),
        order: serde_json::from_value(order.clone()).unwrap(),
        principal: None,
        input: input.clone(),
    };
    use broca_session_store::record::{DeliveryOrdered, QueuedSend, SendEnded};
    let mut records: Vec<Record> = supersedes
        .map(|old| {
            Record::SendEnded(SendEnded {
                submission_id: old.into(),
                reason: Some(
                    serde_json::to_value(broca_records::record::SendEndReason::Superseded {
                        by: broca_protocol::SubmissionId(id.into()),
                    })
                    .unwrap(),
                ),
            })
        })
        .into_iter()
        .collect();
    records.extend([
        // The order as the actor stores it: its typed form, serialized.
        Record::DeliveryOrdered(DeliveryOrdered {
            submission_id: id.into(),
            order: serde_json::to_value(&send.order).unwrap(),
            sent_by: None,
        }),
        Record::QueuedSend(QueuedSend {
            media: Vec::new(),
            named_speaker: None,
            kept_notice: None,
            message_marks: None,
            submission_id: id.into(),
            send_id: None,
            input,
            setup: Vec::new(),
            written_at_ms: Some(HARNESS_TIME_MS),
            ts_ms: None,
            request: Some(
                json!({
                    "model": {"provider_module_id": "stub", "model_id": "m"},
                    "policy_allowed": [],
                    "tool_choice": {"type": "auto"},
                    "generation": {},
                    "stop_when": [],
                })
                .into(),
            ),
            delivery: None,
            receipt: None,
        }),
    ]);
    Action::Ordered {
        send: Box::new(send),
        records,
    }
}

/// Where a scripted provider sends what the session actor does while a step is answered.
#[async_trait]
trait SteerSink: Send + Sync {
    /// Writes a steer record as the actor would.
    async fn steer(&self, steer: Steered);
    /// Records the caller's cancel at the run's next step boundary, as the actor would.
    async fn cancel_at_boundary(&self);
    /// Cancels the run now, as the actor does for an interrupting send: no record, only the
    /// run's cancel token.
    fn cancel_now(&self);
    /// Asks the run to stop, as the runner does when it shuts down.
    fn stop(&self);
    /// Queues an ordered send into the running run, as the actor does.
    async fn ordered(&self, send: broca_core::delivery_order::OrderedSend, records: Vec<Record>);
}

/// The new store's actor: the same steer as the converter writes it, appended before the
/// loop's next batch.
struct NewSink {
    injected: Arc<Mutex<Vec<Record>>>,
    stop: CancellationToken,
    cancel: CancellationToken,
}

#[async_trait]
impl SteerSink for NewSink {
    async fn steer(&self, steer: Steered) {
        // The receipt keeps the send's own facts, as the actor stores them beside the input.
        let mut receipt = serde_json::json!({
            "ts_ms": HARNESS_TIME_MS,
            "send_id": steer.send_id,
            "delivery": broca_protocol::Delivery::Steer,
        });
        if let Some(sent_by) = &steer.sent_by {
            receipt["sent_by"] = serde_json::to_value(sent_by).unwrap();
        }
        let input = steer.messages;
        let written_at_ms = Some(HARNESS_TIME_MS);
        let tags_v1 = None;
        let message_marks = steer
            .message_marks
            .map(|marks| serde_json::to_value(marks).unwrap());
        let receipt = Some(receipt);
        self.injected
            .lock()
            .unwrap()
            .push(Record::Steer(broca_session_store::record::Steer {
                named_speaker: None,
                kept_notice: None,
                media: Vec::new(),
                input,
                written_at_ms,
                tags_v1,
                receipt: receipt.map(Into::into),
                message_marks,
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

    fn cancel_now(&self) {
        self.cancel.cancel();
    }

    fn stop(&self) {
        self.stop.cancel();
    }

    async fn ordered(&self, _send: broca_core::delivery_order::OrderedSend, records: Vec<Record>) {
        self.injected.lock().unwrap().extend(records);
    }
}

/// A scripted provider that steers messages in while answering given calls.
struct SteeringProvider {
    inner: ScriptedProvider,
    calls: std::sync::atomic::AtomicUsize,
    /// `(call, action)`: during the provider's `call`th request (from 1), the actor does this.
    steers: Mutex<Vec<(usize, Action)>>,
    sink: Arc<dyn SteerSink>,
    /// Whether it reports attempt evidence, as a transport does: when one of its calls is
    /// cancelled mid-stream.
    reports_attempts: bool,
}

/// Whether `steers` cancel a call mid-stream.
fn cancels_mid_stream(steers: &[(usize, Action)]) -> bool {
    steers
        .iter()
        .any(|(_, action)| matches!(action, Action::CancelMidStream))
}

#[async_trait]
impl Provider for SteeringProvider {
    fn admit_temperature(&self, requested: Option<f64>) -> Option<f64> {
        self.inner.admit_temperature(requested)
    }
    fn turn_system_possible(&self, preceding: Option<&Message>, followed_by_user: bool) -> bool {
        self.inner.turn_system_possible(preceding, followed_by_user)
    }

    /// A provider whose calls are cancelled mid-stream reports its attempts as a transport
    /// does, so both loops read a cancelled call's stream for its attempt in flight.
    fn reports_attempt_evidence(&self) -> bool {
        self.reports_attempts
    }

    /// In a ride placement test, the config the inner provider froze, which a resume there
    /// checks against the run's. Every other comparison keeps the default stateless config
    /// here, as it always has.
    fn render_config_blob(&self) -> Value {
        match ride_placement::frozen_route() {
            Some(_) => self.inner.render_config_blob(),
            None => json!({ "render_config": "stateless" }),
        }
    }

    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let due: Vec<Action> = {
            let mut steers = self.steers.lock().unwrap();
            let (due, rest) = steers.drain(..).partition(|(at, _)| *at == call);
            *steers = rest;
            due.into_iter().map(|(_, action)| action).collect()
        };
        let mut slow = false;
        let mut mid_stream = false;
        for (index, action) in due.into_iter().enumerate() {
            match action {
                Action::Steer(messages) => self.sink.steer(steered(call, index, messages)).await,
                Action::CancelAtBoundary => self.sink.cancel_at_boundary().await,
                Action::CancelNow => {
                    self.sink.cancel_now();
                    slow = true;
                }
                Action::CancelMidStream => mid_stream = true,
                Action::Stop => self.sink.stop(),
                Action::Ordered { send, records } => self.sink.ordered(*send, records).await,
            }
        }
        if slow {
            // Not a wait for anything: the call this cancel meets answers late, so a loop that
            // waits for it instead of dropping it is caught recording the step's answer.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        let mut events = self.inner.step(step_id, opts).await;
        if !mid_stream {
            return events;
        }
        let sink = Arc::clone(&self.sink);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            sink.cancel_now();
            // Not a wait for anything: the attempt comes after the cancel, as a transport's
            // does, so a loop that stops reading at the cancel is caught missing it.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            while let Some(event) = events.recv().await {
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}

/// A message the caller steers into the running run: its input, the send that carried it,
/// and, in a marked scenario, the owner who sent it and its marks.
struct Steered {
    sent_by: Option<broca_protocol::BindPrincipal>,
    send_id: String,
    messages: Vec<Message>,
    message_marks: Option<broca_protocol::MessageMarks>,
}

fn steered(call: usize, index: usize, messages: Vec<Message>) -> Steered {
    Steered {
        sent_by: RECORDED_MARKS.try_with(|_| owner()).ok(),
        send_id: format!("steer-{call}-{index}"),
        messages,
        message_marks: RECORDED_MARKS
            .try_with(|(marks, _)| marks.clone())
            .ok()
            .flatten(),
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
    with_each_writer(|| {
        Box::pin(compare_actions_in_once(
            scenario.clone(),
            steers.clone(),
            on_restart,
        ))
    })
    .await;
}

/// What the new loop did running a scenario while the session actor does its actions: its
/// whole run as a [`Source`].
struct Actions {
    source: Source,
}

/// The new loop running `scenario` while the session actor does `steers`, resuming a run that
/// pauses as a restart would. With `pins`, it pins what the run did, as a comparison does.
async fn new_with_actions(
    scenario: &Scenario,
    steers: Vec<(usize, Action)>,
    on_restart: Option<OnRestart>,
    pins: bool,
) -> Actions {
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
    let cancel = CancellationToken::new();
    let provider = SteeringProvider {
        inner: ScriptedProvider::new(scenario.steps.clone()),
        calls: Default::default(),
        reports_attempts: cancels_mid_stream(&steers),
        steers: Mutex::new(steers),
        sink: Arc::new(NewSink {
            injected: Arc::clone(&injected),
            stop: stop.clone(),
            cancel: cancel.clone(),
        }),
    };
    let plane = scenario.plane();
    let mut writer = Recording {
        session: &mut session,
        records: Vec::new(),
        batch_ends: Vec::new(),
        backend: Some(&backend),
        snapshots: Vec::new(),
        injected,
        ended: Default::default(),
        media: Default::default(),
    };
    let mut parts = MemoryParts::default();
    let now = harness_now;
    let bind = bind();
    let seams = Seams {
        person_steers: None,
        provider: &provider,
        tools: &plane,
        bind: bind.clone(),
        out: None,
        cancel: cancel.clone(),
        stop: stop.clone(),
        held: None,
        approval: &HarnessPolicy,
        hook_plane: Some(harness_hooks()),
        now_ms: &now,
        session_address: Some(broca_core::session_refs::session_addr(&bind)),
        compaction_plane: None,
        compaction_session: None,
        compaction_changes: false,
        keep_warm: new_warmer(),
        account_reports: reports().map(|reports| {
            Arc::new(NewReports(Arc::clone(&reports.new))) as Arc<dyn AccountReports>
        }),
        max_same_account_wait: account().map_or_else(Default::default, |script| script.wait),
        auth_selection: account_selection(),
        step_progress: None,
        model_call_ready: None,
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    let mut admission = scenario.admission(&provider.inner);
    admission.on_restart = on_restart;
    let lifecycle = plane.lifecycle();
    let ended = Arc::clone(&writer.ended);
    *plane.held.ended.lock().unwrap() = Some(Arc::new(move || {
        ended.load(std::sync::atomic::Ordering::SeqCst)
    }));
    plane.cancel_on_hold(cancel.clone());
    let seams = Seams {
        person_steers: None,
        held: Some(&*lifecycle),
        cancel,
        ..seams
    };
    let first = through!(writer, |w| finishes(run(w, &mut parts, admission, &seams))
        .await)
    .expect("the new loop runs");
    let mut reports = vec![(first.reason, first.paused)];
    if first.paused.is_some() {
        if first.paused == Some(PauseReason::ToolHeld) {
            plane.held.deposit_all();
        }
        let restarted = Seams {
            person_steers: None,
            stop: Default::default(),
            pause_for_auth_at_start: SIGNED_OUT_ON_RESUME
                .try_with(|()| {
                    serde_json::from_value(json!({
                        "class": "auth_required",
                        "message": "the account is signed out",
                    }))
                    .expect("a provider error decodes")
                })
                .ok(),
            ..seams
        };
        let second = through!(writer, |w| finishes(resume(w, &parts, &restarted)).await)
            .expect("the new loop resumes");
        reports.push((second.reason, second.paused));
    }
    keys_match(writer.session, &plane).await;
    ids_resolve(writer.session).await;
    turns_have_facts(writer.session).await;
    outcome_keys_name_their_place(&writer);
    let new = Outcome {
        records: normalized(&writer.records),
        requests: sent_requests(&provider.inner),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .collect(),
    };
    if pins {
        pin_same(&new);
        if entries_compared() {
            pin_entries(writer.session).await;
        }
        if spend_compared() {
            pin_spend(writer.session, &parts).await;
        }
        broca_test_support::pin!("how each run reported", reports);
    }
    rides_name_their_carrier(&writer.records);
    // Reloading the session from a checkpoint at any record gives the state folding every
    // record does, rides kept beside their calls included.
    let points: Vec<usize> = (0..=writer.records.len()).collect();
    let check = check_splits(&writer.records, &points).expect("the session's records fold");
    assert!(
        check.is_empty(),
        "a checkpoint at {check:?} reloads another state than the whole fold"
    );
    Actions {
        source: Source {
            records: writer.records.clone(),
            batch_ends: writer.batch_ends.clone(),
            snapshots: std::mem::take(&mut writer.snapshots),
            parts: parts.parts.clone(),
            outcome: new,
        },
    }
}

async fn compare_actions_in_once(
    scenario: Scenario,
    steers: Vec<(usize, Action)>,
    on_restart: Option<OnRestart>,
) {
    Box::pin(new_with_actions(&scenario, steers, on_restart, true)).await;
}

/// Cuts the new loop's log of `scenario`, run while the actor does `steers`, after each of its
/// records, as a crash could, and resumes the cut, as `compare_every_cut` does, with the sends
/// it holds queued. Nothing the actor did after the cut happens again. Runs in both layouts and
/// both writer modes.
async fn compare_actions_every_cut(scenario: Scenario, steers: Vec<(usize, Action)>) {
    compare_actions_cut(scenario, steers, Cuts::default()).await;
}

/// Where a cut comparison cuts, and what the resumes run on.
#[derive(Clone, Default)]
struct Cuts {
    /// Cut only after the first record of this kind (`Record`'s variant name), where the
    /// scenario's interesting part begins.
    after: Option<&'static str>,
    /// The route the resumes judge and render turn-scoped system messages by, when it isn't the
    /// one the run delivered under, as after a switch to another model.
    resume_route: Option<Option<broca_provider::wire::FrozenRenderConfig>>,
    /// What the run does with itself when the runner stops it: pause for the restart, or end.
    on_restart: Option<OnRestart>,
}

async fn compare_actions_cut(scenario: Scenario, steers: Vec<(usize, Action)>, cuts: Cuts) {
    for layout in LAYOUTS {
        LAYOUT
            .scope(
                layout,
                Box::pin(with_each_writer(|| {
                    Box::pin(compare_actions_every_cut_once(
                        scenario.clone(),
                        steers.clone(),
                        cuts.clone(),
                    ))
                })),
            )
            .await;
    }
}

async fn compare_actions_every_cut_once(
    scenario: Scenario,
    steers: Vec<(usize, Action)>,
    cuts: Cuts,
) {
    let source = Box::pin(new_with_actions(&scenario, steers, cuts.on_restart, false))
        .await
        .source;
    let records = source.records.len();
    assert!(records > 2, "the scenario wrote a log to cut");
    if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
        for (index, record) in source.outcome.records.iter().enumerate() {
            eprintln!("source {index}: {record}");
        }
    }
    let first = match cuts.after {
        Some(kind) => {
            source
                .records
                .iter()
                .position(|record| serde_json::to_value(record).unwrap()["kind"] == kind)
                .unwrap_or_else(|| panic!("the scenario wrote a {kind} record"))
                + 1
        }
        None => 1,
    };
    let route = cuts.resume_route.clone().unwrap_or_else(turn_system_route);
    for cut in source
        .batch_ends
        .iter()
        .copied()
        .filter(|&end| end >= first && end < records)
    {
        // The provider serves the steps the cut log has no answer for yet.
        let steps = scenario.steps[source.answered(cut)..].to_vec();
        let resumed = TURN_SYSTEM
            .scope(
                route.clone(),
                Box::pin(new_source(&scenario, Some(source.cut(cut)), steps)),
            )
            .await;
        let context = format!("cut after record {cut} of {records}");
        // The actions aren't replayed into the resume, so its requests may lack what they added.
        resume_invariants(&context, &source, cut, &resumed, Requests::MayDiffer);
        let new = resumed.outcome;
        if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
            for (index, record) in new.records.iter().enumerate() {
                eprintln!("{context} new   {index}: {record}");
            }
        }
        pin_outcome(
            &format!("{context}: "),
            &new.requests,
            &new.records,
            &new.calls,
            Clone::clone,
        );
    }
}

/// Each ride names the key of the call whose result carries it: the key of that call's intent,
/// else of its outcome, as the session recorded them before the ride. Comparisons normalize keys,
/// so this is what checks the new loop's.
fn rides_name_their_carrier(records: &[Record]) {
    let mut keys: std::collections::HashMap<ToolCallId, String> = Default::default();
    for record in records {
        match record {
            Record::ToolIntent(intent) => {
                if let Some(key) = &intent.call_key {
                    keys.insert(intent.tool_call_id.clone(), key.clone());
                }
            }
            Record::ToolOutcome(outcome) => {
                if let Some(key) = &outcome.call_key {
                    keys.entry(outcome.tool_call_id.clone())
                        .or_insert_with(|| key.clone());
                }
            }
            Record::ResultAppend(append) => {
                if let Some(ride) = &append.ride {
                    assert_eq!(
                        Some(&ride.call_key),
                        keys.get(&append.tool_call_id),
                        "a ride names its carrier's key"
                    );
                }
            }
            _ => {}
        }
    }
}

/// A loop run that doesn't finish within ten seconds fails the comparison rather than hanging
/// it, such as a run left waiting on a held call it should have paused for. The run is boxed:
/// a loop's future is large enough to overflow a test thread's stack where it is built.
async fn finishes<F: std::future::Future>(run: F) -> F::Output {
    tokio::time::timeout(std::time::Duration::from_secs(10), Box::pin(run))
        .await
        .expect("the loop finished")
}

/// Pins what the new loop did, as [`pin_outcome`] does with requests as they render.
fn pin_same(new: &Outcome) {
    if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
        for (index, record) in new.records.iter().enumerate() {
            eprintln!("new   {index}: {record}");
        }
    }
    pin_outcome("", &new.requests, &new.records, &new.calls, as_rendered);
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

/// A cancel that meets a model call which hasn't returned its stream yet, as an interrupting
/// send's cancel can: both loops drop the call and end the run `cancelled`, with no answer and no
/// attempt recorded for it. Every record each loop writes is compared: the first call's step
/// start, and the run's end with its usage.
#[tokio::test]
async fn a_cancel_before_the_first_call_returns_its_stream_drops_the_call() {
    compare_steered(calling("read", 0), vec![(1, Action::CancelNow)]).await;
}

/// A cancel while a model call streams, from a provider that reports attempt evidence: the
/// provider sends its attempt in flight after the cancel and closes the stream. Both loops read
/// the stream for it before the run ends `cancelled`, so the attempt is recorded with its
/// billing evidence and reported to the account router. The records (the attempt's included)
/// and both loops' account reports are compared: a run start and the cancelled attempt for each
/// of the four runs. So are the run's spend facts, though today's builder reads no attempt
/// record: the cancelled run is billed alike.
#[tokio::test]
async fn a_cancel_mid_stream_records_the_attempt_in_flight() {
    let mut scenario = calling("read", 0);
    scenario.steps[0] = vec![attempt(1, 200, "cancelled")];
    compare_reported(
        8,
        compare_spent(compare_steered(
            scenario,
            vec![(1, Action::CancelMidStream)],
        )),
    )
    .await;
}

/// The same after a tool batch: the second call is dropped, the batch's results stay, and the
/// run's usage is the first step's.
#[tokio::test]
async fn a_cancel_before_a_later_call_returns_its_stream_drops_the_call() {
    compare_steered(calling("read", 0), vec![(2, Action::CancelNow)]).await;
}

/// How the run whose call `name` never answered ended: its reason and interrupt reason.
async fn end_after_a_call_in_doubt(name: &str) -> (RunFinishReason, Option<String>) {
    let scenario = Scenario {
        input: vec![user("run it")],
        tools: vec![tool(name, ExecutionMode::Parallel)],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", name, json!({"x": 1}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "never asked for"),
        ],
    };
    let actions = Box::pin(new_with_actions(&scenario, Vec::new(), None, false)).await;
    actions
        .source
        .records
        .iter()
        .find_map(|record| match record {
            Record::RunEnd(end) => Some((end.reason, end.interrupt_reason.clone())),
            _ => None,
        })
        .expect("the run ended")
}

/// A caller who cancels while a call runs gets a cancelled run, even though the call's outcome
/// is never known; interrupted would name a stop nobody asked for.
#[tokio::test]
async fn a_cancel_while_a_call_runs_ends_the_run_cancelled() {
    assert_eq!(
        end_after_a_call_in_doubt("stall").await,
        (RunFinishReason::Cancelled, None)
    );
}

/// The runner's stop while a call runs ends the run interrupted by the shutdown, as a stop at a
/// step boundary does.
#[tokio::test]
async fn a_stop_while_a_call_runs_ends_the_run_interrupted_by_the_shutdown() {
    assert_eq!(
        end_after_a_call_in_doubt("stall_stop").await,
        (RunFinishReason::Interrupted, Some("runner_shutdown".into()))
    );
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
    compare_auth_pause_switching_to(scenario, None).await;
}

/// [`compare_auth_pause`], with routing giving the resumed run `account` instead of the one
/// the run was admitted to.
async fn compare_auth_pause_switching(
    scenario: Scenario,
    account: broca_records::record::AuthSelection,
) {
    compare_auth_pause_switching_to(scenario, Some(account)).await;
}

async fn compare_auth_pause_switching_to(
    scenario: Scenario,
    resume_account: Option<broca_records::record::AuthSelection>,
) {
    for layout in LAYOUTS {
        LAYOUT
            .scope(
                layout,
                Box::pin(compare_auth_pause_in(
                    scenario.clone(),
                    resume_account.clone(),
                )),
            )
            .await;
    }
}

async fn compare_auth_pause_in(
    scenario: Scenario,
    resume_account: Option<broca_records::record::AuthSelection>,
) {
    with_each_writer(|| {
        Box::pin(compare_auth_pause_in_once(
            scenario.clone(),
            resume_account.clone(),
        ))
    })
    .await;
}

async fn compare_auth_pause_in_once(
    scenario: Scenario,
    resume_account: Option<broca_records::record::AuthSelection>,
) {
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
        batch_ends: Vec::new(),
        backend: None,
        snapshots: Vec::new(),
        injected: Default::default(),
        ended: Default::default(),
        media: Default::default(),
    };
    let mut parts = MemoryParts::default();
    let now = harness_now;
    let bind = bind();
    let seams = Seams {
        person_steers: None,
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
        session_address: Some(broca_core::session_refs::session_addr(&bind)),
        compaction_plane: None,
        compaction_session: None,
        compaction_changes: false,
        keep_warm: new_warmer(),
        account_reports: reports().map(|reports| {
            Arc::new(NewReports(Arc::clone(&reports.new))) as Arc<dyn AccountReports>
        }),
        max_same_account_wait: account().map_or_else(Default::default, |script| script.wait),
        auth_selection: account_selection(),
        step_progress: None,
        model_call_ready: provider.model_call_ready(),
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    let first = through!(writer, |w| run(
        w,
        &mut parts,
        scenario.admission(&provider),
        &seams
    )
    .await)
    .expect("the new loop runs");
    reset_passes().await;
    provider.resuming();
    let refrozen_to = resume_account.clone();
    let seams = Seams {
        person_steers: None,
        auth_selection: resume_account.or(seams.auth_selection.clone()),
        // A resume waits for its requests as a new run does.
        model_call_ready: provider.model_call_ready(),
        pause_for_auth_at_start: None,
        ..seams
    };
    let second =
        through!(writer, |w| resume(w, &parts, &seams).await).expect("the new loop resumes");
    keys_match(writer.session, &plane).await;
    ids_resolve(writer.session).await;
    turns_have_facts(writer.session).await;
    outcome_keys_name_their_place(&writer);
    pause_names_its_attempt(&writer);
    // A run the resume refroze onto another account keeps naming it when nobody routes one,
    // as a later resume or a rebuild of its requests reads the run back.
    if let (Some(account), Some(PauseReason::AuthRequired)) = (&refrozen_to, first.paused) {
        let rebuilt = broca_session_loop::run::offline_request(&writer, &parts, &bind, 1)
            .await
            .expect("the run's request rebuilds");
        assert_eq!(
            rebuilt.step_context.and_then(|context| context.account_ref),
            account.account_ref,
            "the account a rebuilt request names"
        );
    }
    let new = Outcome {
        records: normalized(&writer.records),
        requests: sent_requests(&provider),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .collect(),
    };
    pin_same(&new);
    broca_test_support::pin!(
        "how each run reported",
        [(first.reason, first.paused), (second.reason, second.paused)]
    );
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

/// A run that calls `first` once, with `calls` more calls of `read` beside it, then answers.
fn calling(first: &str, calls: usize) -> Scenario {
    let mut content = vec![call("c1", first, json!({"path": "a"}))];
    content.extend((0..calls).map(|index| {
        call(
            &format!("c{}", index + 2),
            "read",
            json!({"path": format!("b{index}")}),
        )
    }));
    Scenario {
        input: vec![user("read a")],
        tools: ["read", "json", "broken"]
            .into_iter()
            .map(|name| tool(name, ExecutionMode::Parallel))
            .collect(),
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer("a1", content, FinishReason::ToolCalls),
            text_step("a2", "done"),
        ],
    }
}

/// A send that may only ride on a tool result, queued while the model asks for a call.
fn a_note(id: &str) -> (usize, Action) {
    (
        1,
        ordered(id, &["tool_result"], &format!("note {id}"), None),
    )
}

/// A send rides on the result of the batch that follows it: its text goes out after the
/// result in the next request, the result itself is stored as the tool returned it, and the
/// ride's envelope names the carrying call's key.
#[tokio::test]
async fn a_ride_on_a_text_result() {
    reading_entries(compare_steered(calling("read", 0), vec![a_note("q1")])).await;
}

#[tokio::test]
async fn every_cut_of_a_ride_on_a_text_result_resumes_alike() {
    compare_actions_every_cut(calling("read", 0), vec![a_note("q1")]).await;
}

/// An error result carries a ride as a text result does.
#[tokio::test]
async fn a_ride_on_an_error_result() {
    reading_entries(compare_steered(calling("broken", 0), vec![a_note("q1")])).await;
}

#[tokio::test]
async fn every_cut_of_a_ride_on_an_error_result_resumes_alike() {
    compare_actions_every_cut(calling("broken", 0), vec![a_note("q1")]).await;
}

/// Several sends ride on one batch's carrier, in queue order.
#[tokio::test]
async fn several_rides_in_one_batch() {
    reading_entries(compare_steered(
        calling("read", 1),
        vec![a_note("q1"), a_note("q2")],
    ))
    .await;
}

#[tokio::test]
async fn every_cut_of_several_rides_in_one_batch_resumes_alike() {
    compare_actions_every_cut(calling("read", 1), vec![a_note("q1"), a_note("q2")]).await;
}

/// A batch with no result to carry text leaves the send queued, past the run's end. On the
/// compaction contract every result carries one, so there it rides.
#[tokio::test]
async fn a_batch_without_a_carrier_leaves_the_send_queued() {
    reading_entries(compare_steered(calling("json", 0), vec![a_note("q1")])).await;
}

#[tokio::test]
async fn every_cut_of_a_batch_without_a_carrier_resumes_alike() {
    compare_actions_every_cut(calling("json", 0), vec![a_note("q1")]).await;
}

/// A send that may go out as turn-scoped system text, else steer.
fn system_note(id: &str) -> (usize, Action) {
    let order = json!({"modes": ["turn_system", "steer"], "template": template_use()});
    (1, ordered_with(id, order, "nudge text", None))
}

/// On a route that takes a turn-scoped system message after the tool results, the send goes out
/// as one at the next step; on a route that doesn't, it steers.
#[tokio::test]
async fn a_system_or_steer_send_on_a_route_with_system_text() {
    TURN_SYSTEM
        .scope(
            Some(anthropic_route(true)),
            reading_entries(compare_steered(calling("read", 0), vec![system_note("q1")])),
        )
        .await;
}

#[tokio::test]
async fn a_system_or_steer_send_on_a_route_without_system_text() {
    TURN_SYSTEM
        .scope(
            Some(anthropic_route(false)),
            reading_entries(compare_steered(calling("read", 0), vec![system_note("q1")])),
        )
        .await;
}

#[tokio::test]
async fn every_cut_of_a_system_or_steer_send_resumes_alike() {
    for supported in [true, false] {
        TURN_SYSTEM
            .scope(
                Some(anthropic_route(supported)),
                compare_actions_every_cut(calling("read", 0), vec![system_note("q1")]),
            )
            .await;
    }
}

/// A step that announced its message's id and stopped before writing it keeps that minted id
/// when it runs again, even when a delivery lands before it at the retried step's boundary: a
/// minted id names the record that announced it, wherever the message lands.
#[tokio::test]
async fn a_retried_steps_minted_id_survives_a_delivery_before_it() {
    let scenario = calling("read", 0);
    for layout in LAYOUTS {
        let scenario = scenario.clone();
        let run = async move {
            let source = Box::pin(new_with_actions(
                &scenario,
                vec![system_note("q1")],
                None,
                false,
            ))
            .await
            .source;
            // The log up to the queued send: the run started, its first step announced its
            // message's minted id, and the send waits.
            let cut = source
                .records
                .iter()
                .position(|record| matches!(record, Record::QueuedSend(_)))
                .expect("the send was queued")
                + 1;
            let start = source.cut(cut);
            let index = start
                .records
                .iter()
                .position(|record| matches!(record, Record::StepStart(_)))
                .expect("the step started");
            let announced = normalized(&start.records)[index]["StepStart"]["mid"].clone();
            assert!(
                announced.is_string(),
                "the step announced a minted id: {announced}"
            );
            let new = new_from(&scenario, Some(start), scenario.steps.clone()).await;
            let retried = new
                .records
                .iter()
                .find(|record| record["StepStart"]["step_id"] == 1)
                .expect("the step ran again");
            assert_eq!(
                retried["StepStart"]["mid"], announced,
                "the minted id is kept"
            );
            assert!(
                new.records
                    .iter()
                    .any(|record| record["Input"]["placement"] == "DeliveredTurnSystem"),
                "the send was delivered before the retried step"
            );
        };
        LAYOUT
            .scope(
                layout,
                TURN_SYSTEM.scope(Some(anthropic_route(true)), Box::pin(run)),
            )
            .await;
    }
}

/// A turn-scoped system entry delivered on a route that takes one is sent again, after a switch
/// to a route that doesn't, as user-authority text the provider renders from the stored entry.
#[tokio::test]
async fn a_stored_system_entry_after_a_switch_to_a_route_without_it() {
    TURN_SYSTEM
        .scope(
            Some(anthropic_route(true)),
            compare_actions_cut(
                calling("read", 0),
                vec![system_note("q1")],
                Cuts {
                    after: Some("Delivery"),
                    resume_route: Some(Some(anthropic_route(false))),
                    ..Cuts::default()
                },
            ),
        )
        .await;
}

/// A send that can start a run waits while one runs, and stays queued at its end.
#[tokio::test]
async fn every_cut_of_an_idle_start_send_resumes_alike() {
    compare_actions_every_cut(
        calling("read", 0),
        vec![(1, ordered("q1", &["idle_start"], "later", None))],
    )
    .await;
}

/// A send that expired before the batch closed ends there instead of riding.
#[tokio::test]
async fn an_expired_send_ends_at_the_batch_it_would_ride() {
    compare_steered(
        calling("read", 0),
        vec![(1, ordered("q1", &["tool_result"], "too late", Some(0)))],
    )
    .await;
}

#[tokio::test]
async fn every_cut_of_an_expired_send_resumes_alike() {
    compare_actions_every_cut(
        calling("read", 0),
        vec![(1, ordered("q1", &["tool_result"], "too late", Some(0)))],
    )
    .await;
}

/// A send under the same dedupe key supersedes an undelivered one; only the newer one rides.
fn deduped() -> Vec<(usize, Action)> {
    let order = json!({"modes": ["tool_result"], "dedupe_key": "status"});
    vec![
        (1, ordered_with("q1", order.clone(), "old status", None)),
        (1, ordered_with("q2", order, "new status", Some("q1"))),
    ]
}

#[tokio::test]
async fn a_dedupe_key_supersedes_the_undelivered_send() {
    compare_steered(calling("read", 0), deduped()).await;
}

#[tokio::test]
async fn every_cut_of_a_superseded_send_resumes_alike() {
    compare_actions_every_cut(calling("read", 0), deduped()).await;
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
fn planned_manifest(tools: &[Tool]) -> Arc<broca_records::record::PlanManifest> {
    let tools = Arc::new(tools.to_vec());
    Arc::new(broca_records::record::PlanManifest {
        rich_ui: None,
        plan: broca_records::record::OpaqueJson::from_json_text(
            json!({"tool_items": [{"provider": "provider"}]}).to_string(),
        )
        .unwrap(),
        plan_identity: "plan-1".into(),
        composition_digest: "composition-1".into(),
        fetches: vec![broca_records::record::ProviderFetch {
            provider: "provider".into(),
            outcome: broca_records::record::FetchOutcome::Answered {
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

async fn assert_store_dispatch_preset(plan: Option<Value>, expected: Option<&str>) {
    let route = format!("dispatch_route c1 provider=\"provider\" preset={expected:?}");
    let expected_route = format!("{route:?}");
    let mut scenario = planned_scenario();
    scenario.tools[0].module = Some("provider".into());
    scenario.manifest = plan.map(|plan| {
        let mut manifest = planned_manifest(&scenario.tools);
        Arc::make_mut(&mut manifest).plan =
            broca_records::record::OpaqueJson::from_json_text(plan.to_string()).unwrap();
        manifest
    });
    for layout in LAYOUTS {
        LAYOUT
            .scope(
                layout,
                Box::pin(async {
                    with_each_writer(|| {
                        Box::pin(async {
                            let outcome = new_loop(&scenario).await;
                            let routes: Vec<_> = outcome
                                .calls
                                .iter()
                                .filter(|entry| entry.contains("dispatch_route "))
                                .collect();
                            assert_eq!(routes, [&expected_route]);
                        })
                    })
                    .await;
                }),
            )
            .await;
    }
}

#[tokio::test]
async fn store_dispatch_uses_the_frozen_provider_preset() {
    assert_store_dispatch_preset(
        Some(json!({"tool_items": [{"provider": "provider", "preset": "head"}]})),
        Some("head"),
    )
    .await;
}

#[tokio::test]
async fn store_dispatch_without_a_plan_preset_stays_presetless() {
    assert_store_dispatch_preset(
        Some(json!({"tool_items": [{"provider": "provider"}]})),
        None,
    )
    .await;
}

#[tokio::test]
async fn store_dispatch_without_a_plan_stays_presetless() {
    assert_store_dispatch_preset(None, None).await;
}

/// The hook provider both loops call, keyed by the tool a call names: the mutate hook rewrites
/// a `mutated` call's input to `{"path":"mutated"}`, the validate hook denies a `vetoed` call,
/// the provider is unavailable for a `hook_down` call, the mutate hook cancels the run and never
/// answers for a `cut_in_hook` call, and every other call passes.
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
            // The caller cancels while the mutate hook still runs, so the hook is cut short.
            ("cut_in_hook", Phase::Mutate) => {
                if let Some(cancel) = &self.cancel {
                    cancel.cancel();
                }
                std::future::pending().await
            }
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
fn hooked_manifest(tools: &[Tool]) -> Arc<broca_records::record::PlanManifest> {
    hooked_manifest_with(tools, &["read"])
}

/// [`hooked_manifest`], with the passing post-tool hook subscribed for `post_tools`.
fn hooked_manifest_with(
    tools: &[Tool],
    post_tools: &[&str],
) -> Arc<broca_records::record::PlanManifest> {
    let subscriptions = json!([
        {"hook": "pre_tool", "phase": "mutate", "ops": [], "on_unavailable": "pass", "budget_ms": 500},
        {"hook": "pre_tool", "phase": "validate", "ops": [], "on_unavailable": "refuse", "budget_ms": 500},
        {"hook": "post_tool", "tools": post_tools, "ops": ["append", "prepend", "replace"], "on_unavailable": "pass", "budget_ms": 500},
        {"hook": "post_tool", "tools": ["post_down"], "ops": ["append", "prepend", "replace"], "on_unavailable": "refuse", "budget_ms": 500},
        {"hook": "post_assistant", "ops": ["append", "prepend"], "on_unavailable": "pass", "budget_ms": 500},
    ]);
    let mut manifest = (*planned_manifest(tools)).clone();
    manifest.plan = broca_records::record::OpaqueJson::from_json_text(
        json!({
            "tool_items": [{"provider": "provider"}],
            "step_transform_items": [
                {"provider": "p0", "preset": "default", "params": {}, "subscriptions": subscriptions}
            ],
        })
        .to_string(),
    )
    .unwrap();
    manifest.declarations = vec![broca_records::record::FrozenDeclaration {
        provider: "p0".into(),
        answer: broca_records::record::OpaqueJson::from_json_text(
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

/// A caller's cancel that cuts a pre-tool hook short: no call of the batch is sent, each gets its
/// not-run result, the plane hears the batch ended, and the run ends cancelled.
#[tokio::test]
async fn a_cancel_that_cuts_a_pre_tool_hook_short_closes_the_batch_unsent() {
    let tools = vec![
        tool("cut_in_hook", ExecutionMode::Parallel),
        tool("read", ExecutionMode::Parallel),
    ];
    let scenario = Scenario {
        input: vec![user("run it")],
        manifest: Some(hooked_manifest(&tools)),
        tools,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "cut_in_hook", json!({"x": 1})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "never asked for"),
        ],
    };
    let outcome = TEST_CANCEL
        .scope(CancellationToken::new(), new_loop(&scenario))
        .await;
    assert!(
        !outcome.calls.iter().any(|call| call.contains("dispatch")),
        "nothing is sent: {:?}",
        outcome.calls
    );
    assert_eq!(
        outcome.calls.last().map(String::as_str),
        Some("\"batch_finished\""),
        "the plane hears the batch ended: {:?}",
        outcome.calls
    );
    let named = |name: &str| -> Vec<&Value> {
        outcome
            .records
            .iter()
            .filter_map(|record| record.get(name))
            .collect()
    };
    assert_eq!(
        named("ToolOutcome").len(),
        2,
        "each call has its outcome: {:?}",
        outcome.records
    );
    let end = named("RunEnd");
    assert_eq!(end.len(), 1, "{:?}", outcome.records);
    assert_eq!(end[0]["reason"], "cancelled", "{}", end[0]);
}

#[tokio::test]
async fn a_cancel_before_the_batch_refuses_it() {
    compare(cancel_before_batch_scenario()).await;
}

#[tokio::test]
async fn every_cut_of_a_cancel_before_the_batch_resumes_alike() {
    // The cancel is the hook's, made in memory during the step; a resume from a cut before the
    // run recorded it never sees it, and carries on.
    RESUME_REQUESTS
        .scope(
            Requests::MayDiffer,
            compare_every_cut(cancel_before_batch_scenario()),
        )
        .await;
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

/// A step that repeats a call id: the first copy is sent, and the second gets the duplicate
/// error without being sent. Both loops key each copy by its own record, the first by its intent
/// and the second by its outcome, so the turn's facts name two keys.
#[tokio::test]
async fn a_repeated_call_id_keys_each_copy_by_its_own_record() {
    compare(Scenario {
        input: vec![user("read a twice")],
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
fn compacted_manifest(tools: &[Tool]) -> Arc<broca_records::record::PlanManifest> {
    let mut manifest = planned_manifest(tools).as_ref().clone();
    manifest.plan = broca_records::record::OpaqueJson::from_json_text(
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

/// A compaction provider restarted mid-run says so the next time the run reaches it: a notice
/// answered `setup_required`, or a ride refused `unknown_session`. Both loops send Setup again
/// before anything else reaches the provider, so before the run's next step rather than at the
/// next run, and make the same calls throughout.
#[tokio::test]
async fn a_provider_that_lost_its_setup_mid_run_gets_setup_before_the_next_step() {
    let two_tool_steps = || {
        vec![
            answer(
                "a1",
                vec![call("c1", "read", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            answer(
                "a2",
                vec![call("c2", "read", json!({"path": "b"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a3", "done"),
        ]
    };
    let over_limit = || {
        vec![
            answer(
                "a1",
                vec![call("c1", "big", json!({}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ]
    };
    for (lost, steps, context_limit, method) in [
        (
            Lost::Notice(1),
            two_tool_steps(),
            100_000,
            "compaction.notice",
        ),
        (Lost::Ride(1), over_limit(), 1_200, "compaction.ride"),
    ] {
        let script = || CompactionScript {
            context_limit,
            rides: vec![Ride::Trim(2)],
        };
        let scenario = compacted_scenario(steps);
        LOST.scope(lost, compare_compacted(scenario.clone(), script()))
            .await;
        // The new loop's calls show the case is seen: the call after the lost answer is a Setup,
        // and a Setup was sent once before it.
        let calls = LOST
            .scope(
                lost,
                LAYOUT.scope(
                    Some(cortexkit_compaction_view::PLAN_VERSION),
                    COMPACTION.scope(Some(Arc::new(script())), new_loop(&scenario)),
                ),
            )
            .await
            .calls;
        let calls: Vec<&String> = calls
            .iter()
            .filter(|call| call.starts_with("compaction."))
            .collect();
        let at = calls
            .iter()
            .position(|call| call.starts_with(method))
            .unwrap_or_else(|| panic!("{lost:?}: no {method} in {calls:?}"));
        assert!(
            calls[..at]
                .iter()
                .any(|call| call.starts_with("compaction.setup")),
            "{lost:?}: the run set up before {calls:?}"
        );
        assert!(
            calls
                .get(at + 1)
                .is_some_and(|call| call.starts_with("compaction.setup")),
            "{lost:?}: Setup follows the lost answer in {calls:?}"
        );
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
/// with a view that shortens the output but keeps the final tool turn, and the step renders it.
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
            rides: vec![Ride::Trim(2)],
        },
    )
    .await;
}

#[tokio::test]
async fn every_cut_of_an_over_limit_ride_resumes_alike() {
    // The stand-in compaction provider reads the session's entries when a resume starts, not
    // after its Setup as Magic Context does. A resume that writes tool results before its Setup
    // leaves the stand-in unable to edit them, so its ride builds a different view.
    RESUME_REQUESTS
        .scope(
            Requests::MayDiffer,
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
                    rides: vec![Ride::Trim(2)],
                },
            ),
        )
        .await;
}

/// The over-limit ride with every send fact set: the compaction provider's Setup names the run's
/// variant and its rides compare the run's tier and account, and its routes carry the run's
/// scope, in both loops alike, over two runs.
#[tokio::test]
async fn a_compacted_run_with_every_send_fact_calls_compaction_alike() {
    SEND_FACTS
        .scope(true, async {
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
                    rides: vec![Ride::Trim(2)],
                },
            )
            .await;
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
        })
        .await;
}

/// On a compacted session whose model binds signed reasoning, a run whose tools differ from the
/// run before it (one tool's description refreshed) sets the signed floor through the newest
/// message before it. Both loops report that floor in Setup and in every notice and ride from
/// that run on, and none before it.
#[tokio::test]
async fn a_compacted_run_whose_tools_change_reports_the_signed_floor_alike() {
    let mut refreshed = compacted_scenario(Vec::new()).tools;
    refreshed[0].description = "read something, skipping binary content".into();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let model = "claude-opus-5-5";
    let (r1, r2, r3) = (
        format!("r1@{model}"),
        format!("r2@{model}"),
        format!("r3@{model}"),
    );
    LAYOUT
        .scope(
            Some(cortexkit_compaction_view::PLAN_VERSION),
            COMPACTION.scope(
                Some(Arc::new(CompactionScript {
                    context_limit: 100_000,
                    rides: Vec::new(),
                })),
                RUN_TOOLS.scope(
                    [(r2.clone(), refreshed)].into_iter().collect(),
                    NEW_CALLS.scope(
                        Arc::clone(&seen),
                        Box::pin(compare_runs_in(
                            compacted_scenario(vec![
                                text_step("a1", "one"),
                                text_step("a2", "two"),
                                text_step("a3", "three"),
                            ]),
                            &[
                                (r1.as_str(), vec![user("first")]),
                                (r2.as_str(), vec![user("second")]),
                                (r3.as_str(), vec![user("third")]),
                            ],
                        )),
                    ),
                ),
            ),
        )
        .await;
    let comparisons = seen.lock().unwrap();
    assert!(!comparisons.is_empty());
    for calls in comparisons.iter() {
        let compaction: Vec<_> = calls
            .iter()
            .filter(|call| call.starts_with("compaction."))
            .collect();
        let first_floor = compaction
            .iter()
            .position(|call| call.contains("\"signed_floor\""))
            .expect("a call reports the signed floor");
        assert!(
            first_floor > 0,
            "the first run reports none: {compaction:#?}"
        );
        // From the first call that reports it on, every Setup, notice and ride reports one: the
        // second run's, then the third run's, since going back to the first run's tools is a
        // change too.
        let floors: std::collections::BTreeSet<_> = compaction[first_floor..]
            .iter()
            .map(|call| {
                let at = call
                    .find("\"signed_floor\"")
                    .unwrap_or_else(|| panic!("a later call without the floor: {call}"));
                call[at..].split('}').next().unwrap().to_owned()
            })
            .collect();
        assert_eq!(floors.len(), 2, "{floors:?}");
    }
}

/// The signed floor goes on being stated after a cut seals the entry it goes through, when no
/// view names that entry. The second run's tools differ from the first's, so the floor goes
/// through the first run's answer (the second run's first step rides, and gets no view). The
/// second run's tool call returns more than the window holds, so the provider refuses its next
/// request as too long; the step rides `over_limit`, and the view cuts through the second run's
/// question and shortens the long output. The cut seals the first run's answer, and the view
/// names only the question and the call, so the floor's entry is in no transcript a call holds.
/// The second run spans the cut and stays in the window, so the floor doesn't move. From the
/// first compaction call that states a floor onward, every Setup, notice and ride states that
/// same floor.
#[tokio::test]
async fn the_signed_floor_is_stated_after_its_entry_is_sealed() {
    let mut refreshed = compacted_scenario(Vec::new()).tools;
    refreshed[0].description = "read something, skipping binary content".into();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let model = "claude-opus-5-5";
    let runs = ["r1", "r2", "r3"].map(|run| format!("{run}@{model}"));
    LAYOUT
        .scope(
            Some(cortexkit_compaction_view::PLAN_VERSION),
            COMPACTION.scope(
                Some(Arc::new(CompactionScript {
                    context_limit: 3_000,
                    rides: vec![Ride::Nothing, Ride::CutAndTrim(3, 4)],
                })),
                RUN_TOOLS.scope(
                    runs[1..]
                        .iter()
                        .map(|run| (run.clone(), refreshed.clone()))
                        .collect(),
                    NEW_CALLS.scope(
                        Arc::clone(&seen),
                        Box::pin(compare_runs_in(
                            compacted_scenario(vec![
                                text_step("a1", "one"),
                                answer(
                                    "a2",
                                    vec![call("c1", "big", json!({}))],
                                    FinishReason::ToolCalls,
                                ),
                                text_step("a3", "two"),
                                text_step("a4", "three"),
                            ]),
                            &[
                                (runs[0].as_str(), vec![user("first")]),
                                (runs[1].as_str(), vec![user("second")]),
                                (runs[2].as_str(), vec![user("third")]),
                            ],
                        )),
                    ),
                ),
            ),
        )
        .await;
    let comparisons = seen.lock().unwrap();
    assert!(!comparisons.is_empty());
    for calls in comparisons.iter() {
        let compaction: Vec<_> = calls
            .iter()
            .filter(|call| call.starts_with("compaction."))
            .collect();
        let first_floor = compaction
            .iter()
            .position(|call| call.contains("\"signed_floor\""))
            .expect("a call states the signed floor");
        // The cutting ride is the second; calls follow it, with the floor's entry sealed.
        let cut = compaction
            .iter()
            .enumerate()
            .filter(|(_, call)| call.starts_with("compaction.ride"))
            .nth(1)
            .map(|(at, _)| at)
            .expect("the second run rides over the limit");
        assert!(compaction.len() > cut + 1, "{compaction:#?}");
        let floors: std::collections::BTreeSet<_> = compaction[first_floor..]
            .iter()
            .map(|call| {
                let at = call
                    .find("\"signed_floor\"")
                    .unwrap_or_else(|| panic!("a later call without the floor: {call}"));
                call[at..].split('}').next().unwrap().to_owned()
            })
            .collect();
        // The tools change once, so there is one floor.
        assert_eq!(floors.len(), 1, "{floors:?}");
    }
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

/// The same four runs: the third run's first step applies the cut and seals the history before
/// it, then the run takes another step. That step's request carries the runs a run starting
/// after the seal reads (the session's first run, and the window's), not every run the history
/// held when the third run started; the comparison with today's loop shows its bytes are
/// unchanged.
#[tokio::test]
async fn a_seal_inside_a_run_bounds_the_runs_its_later_requests_carry() {
    let seen: Arc<Mutex<Vec<CallOptions>>> = Arc::default();
    NEW_REQUESTS
        .scope(
            Arc::clone(&seen),
            LAYOUT.scope(
                Some(cortexkit_compaction_view::PLAN_VERSION),
                COMPACTION.scope(
                    Some(Arc::new(CompactionScript {
                        context_limit: 100_000,
                        rides: vec![Ride::Cut(4)],
                    })),
                    Box::pin(compare_runs_in(
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
                        &[
                            ("r1", vec![user("first")]),
                            ("r2", vec![user("second")]),
                            ("r3@m2", vec![user("third")]),
                            ("r4@m2", vec![user("fourth")]),
                        ],
                    )),
                ),
            ),
        )
        .await;
    let seen = seen.lock().unwrap();
    // Five requests for each writer the comparison runs the new loop over.
    assert_eq!(seen.len() % 5, 0, "{} requests", seen.len());
    for requests in seen.chunks(5) {
        let runs = |at: usize| requests[at].render_episodes.len();
        assert_eq!(
            runs(2),
            3,
            "the third run's first request carries every run"
        );
        assert_eq!(
            runs(3),
            2,
            "after the seal the third run carries the first run and its own"
        );
        assert_eq!(runs(4), 3, "the fourth run carries those and its own");
        // The request after the seal renders the same bytes as it would with every run the
        // third run started with.
        let mut unsealed = requests[3].clone();
        unsealed.render_episodes = Arc::clone(&requests[2].render_episodes);
        let renderer = broca_provider::LiveProvider::new(
            broca_provider::families::AnthropicMessages,
            broca_provider::families::anthropic_frozen_config("claude-sonnet-4-5"),
            broca_provider::wire::AuthHandle::None,
        );
        let render = |opts: &CallOptions| {
            broca_core::provider::Provider::try_render_request(&renderer, opts)
                .expect("the request renders")
        };
        assert_eq!(render(&requests[3]), render(&unsealed));
    }
}

/// A run whose repeated failing call spans a cut: two failures, then a big result pushes the next
/// request over the window, and the provider cuts through the first failure and trims the big
/// output. Sealing that history mid-run must keep the repetition counters, so the third failure's
/// warning still counts the sealed first failure.
fn repetition_across_a_cut_scenario() -> Scenario {
    let tools = vec![
        tool("grep", ExecutionMode::Parallel),
        tool("big", ExecutionMode::Parallel),
    ];
    Scenario {
        input: vec![user("find x")],
        manifest: Some(compacted_manifest(&tools)),
        tools,
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
                vec![call("c3", "big", json!({}))],
                FinishReason::ToolCalls,
            ),
            answer(
                "a4",
                vec![call("c4", "grep", json!({"pattern": "x"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a5", "giving up"),
        ],
    }
}

fn repetition_cut() -> CompactionScript {
    CompactionScript {
        context_limit: 1_200,
        // Entries: the input (1), each call with its result, then its empty results entry
        // (2-3 and 4-5 the failures, 6-7 the big call).
        rides: vec![Ride::CutAndTrim(3, 6)],
    }
}

#[tokio::test]
async fn a_cut_through_a_run_keeps_its_repetition_counts() {
    compare_compacted(repetition_across_a_cut_scenario(), repetition_cut()).await;
}

/// The run the cut splits isn't the session's first, and another run follows it: the new run's
/// numbering continues after the highest part number shown, counted with the split run's own
/// rules for its messages at the window's start, and its first steps count repetitions afresh.
#[tokio::test]
async fn a_run_after_a_cut_through_an_earlier_run_continues_alike() {
    let mut scenario = repetition_across_a_cut_scenario();
    scenario.steps.insert(0, text_step("a0", "hello"));
    scenario.steps.extend([
        answer(
            "a6",
            vec![call("c6", "grep", json!({"pattern": "x"}))],
            FinishReason::ToolCalls,
        ),
        answer(
            "a7",
            vec![call("c7", "grep", json!({"pattern": "x"}))],
            FinishReason::ToolCalls,
        ),
        text_step("a8", "still nothing"),
    ]);
    LAYOUT
        .scope(
            Some(cortexkit_compaction_view::PLAN_VERSION),
            COMPACTION.scope(
                Some(Arc::new(CompactionScript {
                    // The first run's answer adds to the token estimate per request byte, so the
                    // window is a little larger than the single-run scenario's.
                    context_limit: 1_600,
                    // The first run's entries are 1-2, so the cut and trimmed tool move up by two.
                    rides: vec![Ride::CutAndTrim(5, 8)],
                })),
                Box::pin(compare_runs_in(
                    scenario,
                    &[
                        ("r0", vec![user("hi")]),
                        ("r1", vec![user("find x")]),
                        ("r2", vec![user("try again")]),
                    ],
                )),
            ),
        )
        .await;
}

#[tokio::test]
async fn every_cut_of_a_run_split_by_a_compaction_cut_resumes_alike() {
    // The stand-in compaction provider reads the session's entries when a resume starts, not
    // after its Setup as Magic Context does. A resume that writes tool results before its Setup
    // leaves the stand-in unable to edit them, so its ride builds a different view.
    RESUME_REQUESTS
        .scope(
            Requests::MayDiffer,
            compare_compacted_every_cut(repetition_across_a_cut_scenario(), repetition_cut()),
        )
        .await;
}

/// A view lands in the first run, then the new loop seals its ledger before the second run, which
/// moves the view's records out of the open log. The seal carries copies of the view, so the
/// second run restores the same compaction state and sends the same requests.
#[tokio::test]
async fn a_ledger_seal_between_runs_keeps_the_compaction_views() {
    LEDGER_SEAL_BETWEEN_RUNS
        .scope(
            true,
            LAYOUT.scope(
                Some(cortexkit_compaction_view::PLAN_VERSION),
                COMPACTION.scope(
                    Some(Arc::new(CompactionScript {
                        context_limit: 1_200,
                        rides: vec![Ride::Trim(2)],
                    })),
                    Box::pin(compare_runs_in(
                        compacted_scenario(vec![
                            answer(
                                "a1",
                                vec![call("c1", "big", json!({}))],
                                FinishReason::ToolCalls,
                            ),
                            text_step("a2", "done"),
                            text_step("a3", "again"),
                        ]),
                        &[("r1", vec![user("read a")]), ("r2", vec![user("more")])],
                    )),
                ),
            ),
        )
        .await;
}

/// Removing the final tool turn is refused, not accepted as a smaller request. Both loops
/// report the same rule and leave no view or cut to apply or seal.
#[tokio::test]
async fn a_view_dropping_the_final_tool_turn_is_refused() {
    LAYOUT
        .scope(
            Some(cortexkit_compaction_view::PLAN_VERSION),
            COMPACTION.scope(
                Some(Arc::new(CompactionScript {
                    context_limit: 1_200,
                    rides: vec![Ride::Drop(2)],
                })),
                with_each_writer(|| async {
                    let scenario = compacted_scenario(vec![
                        answer(
                            "a1",
                            vec![call("c1", "big", json!({}))],
                            FinishReason::ToolCalls,
                        ),
                        text_step("a2", "done"),
                    ]);
                    let new = Box::pin(new_loop(&scenario)).await;
                    pin_same(&new);
                    for outcome in [&new] {
                        let events: Vec<Value> = outcome
                            .calls
                            .iter()
                            .filter_map(|call| call.strip_prefix("compaction.notice "))
                            .map(|params| serde_json::from_str::<Value>(params).unwrap())
                            .flat_map(|params| {
                                params["events"].as_array().cloned().unwrap_or_default()
                            })
                            .collect();
                        assert_eq!(
                            events,
                            vec![json!({
                                "event": "view_refused",
                                "version": 1,
                                "refusal": {
                                    "code": "rule",
                                    "rule": "final_turn_removed",
                                    "index": 0,
                                },
                            })]
                        );
                        for record in &outcome.records {
                            for kind in ["View", "ViewApplied", "Marker"] {
                                assert!(record.get(kind).is_none(), "unexpected {record}");
                            }
                        }
                        // The first step's request, then the over-window one: refused, and
                        // refused again once the ride's view is refused too.
                        assert_eq!(
                            outcome.requests.len(),
                            3,
                            "the over-window request is sent twice"
                        );
                    }
                }),
            ),
        )
        .await;
}

/// A tool result pushes the next request over the window, and the provider answers with no view,
/// so the request still doesn't fit: the run ends at the context ceiling without sending it, with
/// the ceiling's decision recorded.
fn ceiling_scenario() -> Scenario {
    compacted_scenario(vec![
        answer(
            "a1",
            vec![call("c1", "big", json!({}))],
            FinishReason::ToolCalls,
        ),
        text_step("a2", "never sent"),
    ])
}

fn no_view() -> CompactionScript {
    CompactionScript {
        context_limit: 1_200,
        rides: Vec::new(),
    }
}

#[tokio::test]
async fn a_request_over_the_window_after_the_ride_ends_at_the_ceiling() {
    compare_compacted(ceiling_scenario(), no_view()).await;
}

#[tokio::test]
async fn every_cut_of_a_run_ending_at_the_ceiling_resumes_alike() {
    compare_compacted_every_cut(ceiling_scenario(), no_view()).await;
}

/// A plan with no compaction provider that asks for part numbers in the model's view: on the
/// established layout each run's user and tool-result text goes out with its numbers in front,
/// numbered by that run's own frozen rules. A run that sends this text without its numbers
/// fails it.
fn tagged_scenario() -> Scenario {
    let tools = vec![tool("read", ExecutionMode::Parallel)];
    let mut manifest = planned_manifest(&tools).as_ref().clone();
    manifest.plan = broca_records::record::OpaqueJson::from_json_text(
        json!({"tool_items": [{"provider": "provider"}], "render_tags": true}).to_string(),
    )
    .unwrap();
    Scenario {
        input: vec![user("read a")],
        manifest: Some(Arc::new(manifest)),
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
async fn a_run_frozen_with_part_numbers_sends_them() {
    compare(tagged_scenario()).await;
}

#[tokio::test]
async fn two_runs_frozen_with_part_numbers_number_on_from_the_first() {
    let mut scenario = tagged_scenario();
    scenario.steps.push(text_step("a3", "again"));
    compare_runs(
        scenario,
        &[
            ("r1", vec![user("read a")]),
            ("r2", vec![user("once more")]),
        ],
    )
    .await;
}

/// A new-loop run whose requests can't be rendered until it waited for something (as a store
/// run's media renderer renders nothing until its run waited for the session's media) waits
/// before its first compacted step is prepared and sent, and makes the run the test's recorded
/// expectations describe.
#[tokio::test]
async fn a_compacted_step_waits_for_its_request_to_be_ready_before_sending_it() {
    READY
        .scope(
            Some(ReadyScript::Ready),
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
            ),
        )
        .await;
}

fn submit_call(id: &str, answer: Value) -> ContentBlock {
    call(
        id,
        broca_protocol::structured_output::SUBMIT_RESULT_TOOL,
        answer,
    )
}

fn single(steps: Vec<Vec<NormalizedEvent>>) -> Scenario {
    Scenario {
        input: vec![user("answer as JSON")],
        manifest: None,
        tools: vec![tool("read", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps,
    }
}

#[tokio::test]
async fn a_final_text_run_records_its_valid_answer() {
    STRUCTURED
        .scope(
            Some(unconstrained("final_text")),
            compare(single(vec![text_step("a1", r#"{"answer":1}"#)])),
        )
        .await;
}

#[tokio::test]
async fn a_final_text_run_records_an_answer_that_is_not_json() {
    STRUCTURED
        .scope(
            Some(unconstrained("final_text")),
            compare(single(vec![text_step("a1", "one")])),
        )
        .await;
}

#[tokio::test]
async fn a_final_text_run_records_only_its_last_answer() {
    STRUCTURED
        .scope(
            Some(unconstrained("final_text")),
            compare(single(vec![
                answer(
                    "a1",
                    vec![
                        ContentBlock::Text {
                            text: "reading".into(),
                        },
                        call("c1", "read", json!({"path": "a"})),
                    ],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", r#"{"answer":2}"#),
            ])),
        )
        .await;
}

#[tokio::test]
async fn a_submit_ends_the_run_before_its_siblings_run() {
    STRUCTURED
        .scope(
            Some(unconstrained("submit_tool")),
            compare(single(vec![answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    submit_call("c2", json!({"answer": 3})),
                    submit_call("c3", json!({"answer": 4})),
                ],
                FinishReason::ToolCalls,
            )])),
        )
        .await;
}

#[tokio::test]
async fn a_submit_survives_every_crash_cut() {
    STRUCTURED
        .scope(
            Some(unconstrained("submit_tool")),
            compare_every_cut(single(vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                answer(
                    "a2",
                    vec![submit_call("c2", json!({"answer": "wrong type"}))],
                    FinishReason::ToolCalls,
                ),
            ])),
        )
        .await;
}

#[tokio::test]
async fn a_submit_tool_run_that_never_submits_records_no_value() {
    STRUCTURED
        .scope(
            Some(unconstrained("submit_tool")),
            compare(single(vec![text_step("a1", "done")])),
        )
        .await;
}

/// Structured output in `mode` delivered strictly, which a provider may refuse as too large a
/// grammar.
fn strict(mode: &str) -> Value {
    let mut structured = unconstrained(mode);
    structured["enforcement"] = json!("enforced");
    structured.as_object_mut().unwrap().remove("reason");
    structured["delivery"] = json!("strict");
    structured
}

/// The provider refuses a request of `tokens` as longer than its window of `limit`, in
/// Anthropic's words.
fn too_long(tokens: u64, limit: u64) -> NormalizedEvent {
    NormalizedEvent::Control(ProviderControl::Error {
        error: serde_json::from_value(json!({
            "class": "context_overflow",
            "message": format!("prompt is too long: {tokens} tokens > {limit} maximum"),
            "status": 400,
            "provider_code": "invalid_request_error",
        }))
        .unwrap(),
        retries_used: 0,
    })
}

/// The provider refuses the run's strict schema as too large a grammar, after one retry.
fn grammar_refused() -> Vec<NormalizedEvent> {
    vec![NormalizedEvent::Control(ProviderControl::Error {
        error: serde_json::from_value(json!({
            "class": "permanent",
            "message": "schema grammar too large",
            "status": 400,
            "provider_code": GRAMMAR_REFUSED,
        }))
        .unwrap(),
        retries_used: 1,
    })]
}

#[tokio::test]
async fn a_refused_strict_final_text_schema_is_downgraded_and_the_step_sent_again() {
    STRUCTURED
        .scope(
            Some(strict("final_text")),
            compare(single(vec![
                grammar_refused(),
                text_step("a1", r#"{"answer":5}"#),
            ])),
        )
        .await;
}

#[tokio::test]
async fn a_refused_strict_submit_schema_is_downgraded_and_the_submit_counted() {
    STRUCTURED
        .scope(
            Some(strict("submit_tool")),
            compare(single(vec![
                grammar_refused(),
                answer(
                    "a1",
                    vec![submit_call("c1", json!({"answer": 6}))],
                    FinishReason::ToolCalls,
                ),
            ])),
        )
        .await;
}

#[tokio::test]
async fn a_strict_schema_refused_twice_ends_the_run() {
    STRUCTURED
        .scope(
            Some(strict("final_text")),
            compare(single(vec![grammar_refused(), grammar_refused()])),
        )
        .await;
}

#[tokio::test]
async fn a_failed_sign_in_after_retries_keeps_its_retry_count() {
    let mut failure = auth_failure();
    if let Some(NormalizedEvent::Control(ProviderControl::Error { retries_used, .. })) =
        failure.last_mut()
    {
        *retries_used = 2;
    }
    compare_auth_pause(single(vec![failure, text_step("a1", "hi")])).await;
}

#[tokio::test]
async fn a_failed_step_after_retries_counts_them_in_the_run_end() {
    let mut failure = grammar_refused();
    if let Some(NormalizedEvent::Control(ProviderControl::Error { retries_used, .. })) =
        failure.last_mut()
    {
        *retries_used = 3;
    }
    compare(single(vec![failure])).await;
}

#[tokio::test]
async fn a_downgrade_survives_every_crash_cut() {
    STRUCTURED
        .scope(
            Some(strict("final_text")),
            compare_every_cut(single(vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                grammar_refused(),
                text_step("a2", r#"{"answer":7}"#),
            ])),
        )
        .await;
}

fn reads(steps: usize, last: Vec<NormalizedEvent>) -> Scenario {
    let mut scripted: Vec<Vec<NormalizedEvent>> = (0..steps)
        .map(|n| {
            answer(
                &format!("a{n}"),
                vec![call(&format!("c{n}"), "read", json!({"path": n}))],
                FinishReason::ToolCalls,
            )
        })
        .collect();
    scripted.push(last);
    single(scripted)
}

#[tokio::test]
async fn a_nudge_is_placed_after_the_batch_that_reaches_it() {
    BUDGET
        .scope(
            Some(budgeted(&[1], None)),
            compare(reads(2, text_step("z", "done"))),
        )
        .await;
}

/// The step after a finalize budget firing, rebuilt offline from a copy of the session cut at
/// that step's start, disables tool calls as the request the loop sent for it did, and keeps the
/// declared tools.
#[tokio::test]
async fn a_finalize_steps_offline_request_disables_calls_as_sent() {
    BUDGET
        .scope(Some(budgeted(&[], Some(1))), async {
            let scenario = reads(1, text_step("z", "done"));
            let source = new_source(&scenario, None, scenario.steps.clone()).await;
            assert_eq!(
                source.outcome.requests.len(),
                2,
                "a tool step, then the finalize step"
            );
            let sent = &source.outcome.requests[1];
            assert_eq!(*sent.tool_choice, broca_protocol::ToolChoice::None);
            let start = source
                .records
                .iter()
                .rposition(|record| matches!(record, Record::StepStart(_)))
                .expect("the finalize step started");
            let cut = source.cut(start + 1);
            let backend = MemoryBackend::default();
            let mut session = Session::create(
                &backend,
                SessionAddr("0123456789abcdef".into()),
                SessionTriple {
                    project_root: "/project".into(),
                    harness: "differential".into(),
                    session: "s1".into(),
                },
                &DeploymentKey::new(1, &[7; 32]),
                false,
            )
            .await
            .unwrap();
            session.append(&cut.records, true).await.unwrap();
            let parts = MemoryParts { parts: cut.parts };
            let rebuilt = broca_session_loop::run::offline_request(&session, &parts, &bind(), 2)
                .await
                .expect("the finalize step rebuilds");
            assert_eq!(rebuilt.tool_choice, sent.tool_choice, "the tool choice");
            assert_eq!(rebuilt.tools, sent.tools, "the declared tools");
        })
        .await;
}

/// A session whose run froze no cache marker order (one converted from a log written before
/// runs froze one) keeps rendering without one: a step rebuilt from it names none, and a new run
/// admitted on it freezes none. A session's first run takes the current order.
#[tokio::test]
async fn a_session_whose_run_froze_no_marker_order_keeps_none() {
    use broca_session_loop::run::MARKER_ORDER_VERSION;
    let scenario = Scenario {
        input: vec![user("hello")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![text_step("a1", "hi")],
    };
    let source = new_source(&scenario, None, scenario.steps.clone()).await;
    assert_eq!(
        source.outcome.requests[0].marker_order_version,
        Some(MARKER_ORDER_VERSION)
    );
    let mut converted = source.cut(source.records.len());
    // As a log written before runs froze a marker order: its start records none.
    for record in &mut converted.records {
        if let Record::RunStart(start) = record {
            let recorded = start
                .run_choices
                .as_mut()
                .and_then(|choices| choices.get_mut("record"))
                .and_then(Value::as_object_mut)
                .expect("the start records its run's members");
            assert!(recorded.remove("marker_order_version").is_some());
        }
    }
    let backend = MemoryBackend::default();
    let mut session = Session::create(
        &backend,
        SessionAddr("0123456789abcdef".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "differential".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        true,
    )
    .await
    .unwrap();
    session.append(&converted.records, true).await.unwrap();
    let parts = MemoryParts {
        parts: converted.parts,
    };
    let rebuilt = broca_session_loop::run::offline_request(&session, &parts, &bind(), 1)
        .await
        .expect("the step rebuilds");
    assert_eq!(rebuilt.marker_order_version, None, "the rebuilt step");
    let provider = ScriptedProvider::new(Vec::new());
    let admit = |operations| {
        let mut request = scenario.request_for("r2", &[user("again")]);
        broca_session_loop::admit::admission(
            &mut request,
            &admission_control(&scenario),
            &provider,
            operations,
            Some(2),
        )
        .expect("the run is admitted")
    };
    assert_eq!(
        admit(&session.operations).marker_order_version,
        None,
        "a later run"
    );
    assert_eq!(
        admit(&Default::default()).marker_order_version,
        Some(MARKER_ORDER_VERSION),
        "a session's first run"
    );
}

#[tokio::test]
async fn nudges_and_finalize_due_together_are_placed_in_order() {
    BUDGET
        .scope(
            Some(budgeted(&[2, 1], Some(2))),
            compare(reads(
                2,
                answer(
                    "z",
                    vec![
                        ContentBlock::Text {
                            text: "finishing".into(),
                        },
                        call("cz", "read", json!({"path": "z"})),
                    ],
                    FinishReason::ToolCalls,
                ),
            )),
        )
        .await;
}

#[tokio::test]
async fn a_budget_survives_every_crash_cut() {
    BUDGET
        .scope(
            Some(budgeted(&[1], Some(2))),
            compare_every_cut(reads(2, text_step("z", "done"))),
        )
        .await;
}

#[tokio::test]
async fn a_finalize_answer_is_the_structured_result() {
    STRUCTURED
        .scope(
            Some(unconstrained("final_text")),
            BUDGET.scope(
                Some(budgeted(&[], Some(1))),
                compare(reads(1, text_step("z", r#"{"answer":8}"#))),
            ),
        )
        .await;
}

#[tokio::test]
async fn a_hard_stop_writes_the_firings_its_calls_made_due() {
    let mut scenario = reads(1, text_step("z", "done"));
    scenario.stop_when = vec![StopCondition::StepCount { n: 1 }];
    BUDGET
        .scope(Some(budgeted(&[1], None)), compare(scenario))
        .await;
}

#[tokio::test]
async fn a_budget_starts_over_with_each_run() {
    let mut scenario = reads(1, text_step("z", "done"));
    scenario.steps.push(answer(
        "y1",
        vec![call("cy", "read", json!({"path": "y"}))],
        FinishReason::ToolCalls,
    ));
    scenario.steps.push(text_step("y2", "done again"));
    BUDGET
        .scope(
            Some(budgeted(&[1], None)),
            compare_runs(
                scenario,
                &[("r1", vec![user("read")]), ("r2", vec![user("read again")])],
            ),
        )
        .await;
}

#[tokio::test]
async fn a_batch_ending_in_an_unknown_outcome_still_writes_its_firings() {
    BUDGET
        .scope(
            Some(budgeted(&[1], None)),
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
            }),
        )
        .await;
}

/// One cache-reaching attempt sent at `sent_at_ms`.
fn sent_at(sent_at_ms: u64) -> NormalizedEvent {
    let mut event = attempt(1, 200, "complete");
    if let NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) = &mut event {
        attempt.sent_at_ms = Some(sent_at_ms);
    }
    event
}

fn cached_scenario(sent_at_ms: u64) -> Scenario {
    compacted_scenario(vec![
        attempted(
            answer(
                "a1",
                vec![call("c1", "read", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            vec![sent_at(sent_at_ms)],
        ),
        text_step("a2", "done"),
    ])
}

/// The first step's request reached the cache long ago, past the five minutes its markers ask
/// for, so the next step rides claiming the cache expired, in both loops.
#[tokio::test]
async fn a_ride_claims_a_cache_expired_by_the_lifetime_its_requests_asked_for() {
    assert_eq!(
        broca_provider::cache_warm::requested_ttl_ms("anthropic", &cached_config()),
        Some(300_000),
        "the frozen config asks for a cache lifetime"
    );
    CACHE
        .scope(
            true,
            compare_compacted(
                cached_scenario(10),
                CompactionScript {
                    context_limit: 100_000,
                    rides: Vec::new(),
                },
            ),
        )
        .await;
}

/// The same run with its cache written moments ago: nothing is claimed.
#[tokio::test]
async fn a_live_cache_is_not_claimed_expired() {
    // Both loops read one fixed clock, so the pinned run is the same every time. It is set
    // after today's date, so a cache sent at that time is live by the wall clock as well.
    const NOW_MS: u64 = 1_800_000_000_000;
    let dir = broca_test_support::scratch_root().join(format!(
        "live-cache-clock-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("test-clock"), format!(r#"{{"now_ms":{NOW_MS}}}"#)).unwrap();
    let clock = broca_core::Clock::for_state_root(&dir).unwrap();
    ACCOUNT_CLOCK
        .scope(
            clock,
            CACHE.scope(
                true,
                compare_compacted(
                    cached_scenario(NOW_MS),
                    CompactionScript {
                        context_limit: 100_000,
                        rides: Vec::new(),
                    },
                ),
            ),
        )
        .await;
}

#[tokio::test]
async fn every_cut_of_a_cache_expired_ride_resumes_alike() {
    CACHE
        .scope(
            true,
            compare_compacted_every_cut(
                cached_scenario(10),
                CompactionScript {
                    context_limit: 100_000,
                    rides: Vec::new(),
                },
            ),
        )
        .await;
}

/// Runs `compare` with keep-warm on, then checks the new loop sent some warms, `expect` of them
/// in each of the comparison's modes when the comparison knows, and pins them.
async fn compare_warmed<F: std::future::Future<Output = ()>>(expect: Option<usize>, compare: F) {
    let pair = Arc::new(WarmPair::default());
    WARM.scope(Some(Arc::clone(&pair)), compare).await;
    let new = pair.new.log.lock().unwrap().clone();
    broca_test_support::pin!("warms", new);
    assert!(!new.is_empty(), "the new loop sent no warm");
    if let Some(expect) = expect {
        assert_eq!(
            new.len(),
            // Each layout runs once per writer.
            expect * LAYOUTS.len() * 2,
            "the new loop's warms: {new:?}"
        );
    }
}

fn slow_scenario(steps: Vec<Vec<NormalizedEvent>>) -> Scenario {
    Scenario {
        input: vec![user("run it")],
        manifest: None,
        tools: vec![
            tool("slow", ExecutionMode::Parallel),
            tool("read", ExecutionMode::Parallel),
        ],
        stop_when: Vec::new(),
        steps,
    }
}

/// A slow call's wait sends warms until its spending cap stops them, in both loops.
#[tokio::test]
async fn a_slow_call_is_waited_on_with_the_cache_kept_warm() {
    compare_warmed(
        Some(3),
        compare(slow_scenario(vec![
            answer(
                "a1",
                vec![call("c1", "slow", json!({}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ])),
    )
    .await;
}

/// Each wait has its own cap: two slow batches send two rounds of warms.
#[tokio::test]
async fn each_tool_wait_has_its_own_spending_cap() {
    compare_warmed(
        Some(6),
        compare(slow_scenario(vec![
            answer(
                "a1",
                vec![call("c1", "slow", json!({}))],
                FinishReason::ToolCalls,
            ),
            answer(
                "a2",
                vec![call("c2", "slow", json!({}))],
                FinishReason::ToolCalls,
            ),
            text_step("a3", "done"),
        ])),
    )
    .await;
}

/// Resumed at every cut, a run still waiting on its slow call warms as it did: its keep-warm is
/// read back from its start.
#[tokio::test]
async fn every_cut_of_a_warmed_wait_resumes_alike() {
    compare_warmed(
        None,
        compare_every_cut(slow_scenario(vec![
            answer(
                "a1",
                vec![call("c1", "slow", json!({}))],
                FinishReason::ToolCalls,
            ),
            answer(
                "a2",
                vec![call("c2", "slow", json!({}))],
                FinishReason::ToolCalls,
            ),
            text_step("a3", "done"),
        ])),
    )
    .await;
}

fn attempted_scenario() -> Scenario {
    Scenario {
        input: vec![user("read a")],
        manifest: None,
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
    }
}

/// Both loops report the run's start and each attempt, failed or completed, in order: four
/// events per layout.
#[tokio::test]
async fn account_reports_name_the_start_and_every_attempt() {
    compare_reported(8, compare(attempted_scenario())).await;
}

/// Resumed at every cut, both loops report the same attempts; a resume reports no start.
#[tokio::test]
async fn every_cut_reports_alike() {
    compare_reported(1, compare_every_cut(attempted_scenario())).await;
}

/// Keep-warm learns every attempt in both loops.
#[tokio::test]
async fn keep_warm_is_told_every_attempt() {
    compare_warmed(
        None,
        compare(slow_scenario(vec![
            attempted(
                answer(
                    "a1",
                    vec![call("c1", "slow", json!({}))],
                    FinishReason::ToolCalls,
                ),
                vec![
                    attempt(1, 529, "eof_before_completion"),
                    attempt(2, 200, "complete"),
                ],
            ),
            text_step("a2", "done"),
        ])),
    )
    .await;
}

/// The provider refuses the account's limit, which resets before the session's cache on it
/// expires: both loops pause on the same account, then resume and send the step again.
#[tokio::test]
async fn an_account_limit_pauses_on_the_same_account_and_resumes() {
    compare_routed(
        AccountScript {
            selection: routed("acct-1"),
            wait: broca_core::SameAccountWait::default(),
            wait_for_reset: true,
        },
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
                account_limited(),
                text_step("a2", "done"),
            ],
        }),
    )
    .await;
}

/// With same-account waits switched off, both loops end the run on the account's limit.
#[tokio::test]
async fn an_account_limit_ends_the_run_when_waits_are_off() {
    compare_routed(
        AccountScript {
            selection: routed("acct-1"),
            wait: broca_core::SameAccountWait::Minutes(0),
            wait_for_reset: true,
        },
        compare(Scenario {
            input: vec![user("hello")],
            tools: Vec::new(),
            manifest: None,
            stop_when: Vec::new(),
            steps: vec![account_limited()],
        }),
    )
    .await;
}

fn tool_then_limited(limited: Vec<NormalizedEvent>) -> Scenario {
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
            limited,
            text_step("a2", "done"),
        ],
    }
}

/// Asked to resume before the account resets, both loops leave the run paused and send nothing.
#[tokio::test]
async fn an_account_limit_pause_stays_paused_before_the_reset() {
    let dir = broca_test_support::scratch_root().join(format!(
        "account-clock-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("test-clock"), br#"{"now_ms":1000000}"#).unwrap();
    let clock = broca_core::Clock::for_state_root(&dir).unwrap();
    ACCOUNT_CLOCK
        .scope(
            clock,
            compare_routed(
                AccountScript {
                    selection: routed("acct-1"),
                    wait: broca_core::SameAccountWait::default(),
                    wait_for_reset: false,
                },
                compare_auth_pause(tool_then_limited(account_limited())),
            ),
        )
        .await;
    std::fs::remove_dir_all(dir).unwrap();
}

/// A resume whose requests can't be rendered until it waited for something (as a store run's
/// media renderer renders nothing until its run waited for the session's media) waits before it
/// renders the step the account-limit pause refused, to check it is the same request, and resumes
/// as the test's recorded expectations describe.
#[tokio::test]
async fn an_account_limit_resume_waits_for_its_request_to_be_ready_before_checking_it() {
    READY
        .scope(
            Some(ReadyScript::Ready),
            compare_routed(
                AccountScript {
                    selection: routed("acct-1"),
                    wait: broca_core::SameAccountWait::default(),
                    wait_for_reset: true,
                },
                compare_auth_pause(tool_then_limited(account_limited())),
            ),
        )
        .await;
}

/// The step a pause resumes renders a request other than the one the provider refused: both
/// loops end the run interrupted rather than send it.
#[tokio::test]
async fn an_account_limit_resume_refuses_a_changed_request() {
    compare_routed(
        AccountScript {
            selection: routed("acct-1"),
            wait: broca_core::SameAccountWait::default(),
            wait_for_reset: true,
        },
        compare_auth_pause(tool_then_limited(account_limited_reporting(RENDERED_OTHER))),
    )
    .await;
}

/// Routing gives the resumed run another account: both loops end it interrupted.
#[tokio::test]
async fn an_account_limit_resume_on_another_account_ends_the_run() {
    let first = AccountScript {
        selection: routed("acct-1"),
        wait: broca_core::SameAccountWait::default(),
        wait_for_reset: true,
    };
    compare_routed(
        first,
        compare_auth_pause_switching(tool_then_limited(account_limited()), routed("acct-2")),
    )
    .await;
}

/// A run paused for sign-in resumes on the account routing gives it now: both loops name that
/// account on the resumed step, not the one the run was admitted to.
#[tokio::test]
async fn a_sign_in_resume_names_the_account_routing_gives_now() {
    compare_routed(
        AccountScript {
            selection: routed("acct-1"),
            wait: broca_core::SameAccountWait::default(),
            wait_for_reset: false,
        },
        compare_auth_pause_switching(
            Scenario {
                input: vec![user("hello")],
                tools: vec![tool("read", ExecutionMode::Parallel)],
                manifest: None,
                stop_when: Vec::new(),
                steps: vec![auth_failure(), text_step("a1", "hi")],
            },
            routed("acct-2"),
        ),
    )
    .await;
}

/// After a crash, a resume whose provider renders with another config than the run froze ends
/// the run interrupted, naming the drift, in both loops; it sends nothing.
#[tokio::test]
async fn a_crash_resume_on_a_changed_render_config_ends_the_run() {
    compare_drifted(compare_every_cut(Scenario {
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
    }))
    .await;
}

/// A run paused for sign-in after a provider call can't take a changed render config: both
/// loops end it interrupted on resume.
#[tokio::test]
async fn a_paused_run_on_a_changed_render_config_ends_on_resume() {
    compare_drifted(compare_auth_pause(Scenario {
        input: vec![user("hello")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![auth_failure(), text_step("a1", "hi")],
    }))
    .await;
}

/// At every cut of a held call, a resume on a changed render config ends the run without
/// sending anything: an open batch closes with what its calls hold.
#[tokio::test]
async fn every_cut_of_a_held_call_on_a_changed_render_config_ends_alike() {
    compare_drifted(compare_every_cut(Scenario {
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
    }))
    .await;
}

/// `step` with the provider's `StepFinished` before its terminal event, reporting `usage` and
/// how it served the step: the service tier, speed and raw finish reason, and the model that
/// answered with each model's usage when the provider handed the request on itself.
fn served(mut step: Vec<NormalizedEvent>, usage: Usage, facts: Value) -> Vec<NormalizedEvent> {
    let terminal = step.pop().expect("a step ends with its terminal");
    let NormalizedEvent::Control(ProviderControl::Finished { finish_reason, .. }) = &terminal
    else {
        panic!("a step ends with its Finished");
    };
    let text = |key: &str| facts[key].as_str().map(str::to_owned);
    step.push(NormalizedEvent::Control(ProviderControl::StepFinished {
        step_id: StepId(0),
        usage,
        finish_reason: *finish_reason,
        raw_finish_reason: text("raw_finish_reason"),
        service_tier_used: text("service_tier_used"),
        speed_used: text("speed_used"),
        served_model: text("served_model"),
        usage_iterations: (!facts["usage_iterations"].is_null()).then(|| {
            serde_json::from_value(facts["usage_iterations"].clone())
                .expect("usage iterations decode")
        }),
    }));
    step.push(terminal);
    step
}

fn usage(input: u64, output: u64) -> Usage {
    serde_json::from_value(json!({"input_tokens": input, "output_tokens": output}))
        .expect("usage decodes")
}

/// Both loops record what the provider said about how it served each step: its service tier,
/// speed and raw finish reason, and its step usage over the final event's.
#[tokio::test]
async fn each_step_records_how_the_provider_served_it() {
    compare(Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            served(
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                usage(1200, 40),
                json!({
                    "service_tier_used": "priority",
                    "raw_finish_reason": "tool_use",
                }),
            ),
            served(
                text_step("a2", "done"),
                usage(1300, 20),
                // A raw reason longer than 64 bytes is left out, not cut.
                json!({
                    "service_tier_used": "default",
                    "speed_used": "fast",
                    "raw_finish_reason": "x".repeat(65),
                }),
            ),
        ],
    })
    .await;
}

/// A step the provider handed to another model itself records the model that answered and
/// each model's usage, and counts their sum toward the run.
#[tokio::test]
async fn a_step_another_model_served_records_it_and_its_whole_usage() {
    compare(Scenario {
        input: vec![user("hello")],
        tools: Vec::new(),
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![served(
            text_step("a1", "hi"),
            usage(900, 30),
            json!({
                "served_model": "claude-opus-4-8",
                "raw_finish_reason": "end_turn",
                "usage_iterations": [
                    {"kind": "message", "model": "claude-opus-5-5", "input_tokens": 800, "output_tokens": 0},
                    {"kind": "message", "model": "claude-opus-4-8", "input_tokens": 900, "output_tokens": 30}
                ],
            }),
        )],
    })
    .await;
}

/// `step` with the provider reporting that it cut call `id`'s arguments: complete JSON followed
/// by runaway whitespace.
fn cut_call(mut step: Vec<NormalizedEvent>, id: &str, name: &str) -> Vec<NormalizedEvent> {
    let terminal = step.pop().expect("a step ends with its terminal");
    step.push(NormalizedEvent::Control(
        ProviderControl::ToolArgsTrailingGarbage {
            cut: broca_protocol::provider::ToolArgsTrailingGarbage {
                tool_call_id: ToolCallId(id.into()),
                tool_name: name.into(),
                json_bytes: 18,
                whitespace_bytes: 4096,
                omitted_tool_calls: 0,
            },
        },
    ));
    step.push(terminal);
    step
}

fn cut_scenario(calls: Vec<ContentBlock>, cut: &[&str]) -> Scenario {
    let mut first = answer("a1", calls, FinishReason::ToolCalls);
    for id in cut {
        first = cut_call(first, id, "read");
    }
    Scenario {
        input: vec![user("read a and b")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![first, text_step("a2", "done")],
    }
}

/// A call whose arguments the provider cut is answered with today's error and never sent; the
/// step's other calls run, in both loops.
#[tokio::test]
async fn a_cut_tool_call_is_answered_and_the_others_run() {
    compare(cut_scenario(
        vec![
            call("c1", "read", json!({"path": "a"})),
            call("c2", "read", json!({"path": "b"})),
        ],
        &["c2"],
    ))
    .await;
}

/// With every call cut, nothing is sent and the run goes on to its next step.
#[tokio::test]
async fn a_step_whose_every_call_was_cut_sends_nothing() {
    compare(cut_scenario(
        vec![call("c1", "read", json!({"path": "a"}))],
        &["c1"],
    ))
    .await;
}

/// Resumed at every cut, both loops answer the cut call once and never send it.
#[tokio::test]
async fn every_cut_of_a_step_with_a_cut_call_resumes_alike() {
    compare_every_cut(cut_scenario(
        vec![
            call("c1", "read", json!({"path": "a"})),
            call("c2", "read", json!({"path": "b"})),
        ],
        &["c2"],
    ))
    .await;
}

fn text_delta(id: &str, text: &str) -> NormalizedEvent {
    NormalizedEvent::Display(DisplayEvent::TextDelta {
        mid: None,
        part: None,
        message_id: MessageId(id.into()),
        tool_call_id: None,
        delta: text.into(),
    })
}

/// The display events the run sent, each delta's message id checked and then cleared: every
/// delta names the message its step writes, and the tests compare everything else.
fn display_events(events: &mut mpsc::Receiver<OutputEvent>) -> Vec<DisplayEvent> {
    raw_display_events(events)
        .into_iter()
        .map(without_mid)
        .collect()
}

/// `event` with its message id cleared, after checking a delta names one.
fn without_mid(mut event: DisplayEvent) -> DisplayEvent {
    if event.delta().is_some() {
        assert!(event.mid().is_some(), "a delta names no message: {event:?}");
        event.set_mid(None);
    }
    event
}

/// The display events the run sent, as it sent them.
fn raw_display_events(events: &mut mpsc::Receiver<OutputEvent>) -> Vec<DisplayEvent> {
    let mut display = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let OutputEvent::Display(event) = event {
            display.push(event);
        }
    }
    display
}

/// Each step's deltas name the message that step writes, not whichever message the session is
/// on when the display lane takes them: a delta still queued when the next step starts would
/// otherwise be shown as part of the next step's answer.
#[tokio::test]
async fn each_steps_deltas_name_the_message_that_step_writes() {
    let (out, mut events) = mpsc::channel(64);
    let mut first = answer(
        "a0",
        vec![
            ContentBlock::Text { text: "one".into() },
            call("c1", "read", json!({})),
        ],
        FinishReason::ToolCalls,
    );
    first.insert(0, text_delta("a0", "one"));
    let mut second = text_step("a1", "two");
    second.insert(0, text_delta("a1", "two"));
    DISPLAY_OUT
        .scope(out, async {
            new_loop(&single(vec![first, second])).await;
        })
        .await;
    let mids: Vec<_> = raw_display_events(&mut events)
        .into_iter()
        .filter(|event| event.delta().is_some())
        .map(|event| event.mid().map(str::to_owned))
        .collect();
    assert_eq!(mids.len(), 2, "{mids:?}");
    assert!(mids.iter().all(Option::is_some), "{mids:?}");
    assert_ne!(mids[0], mids[1], "each step's delta names its own message");
}

fn expected_delta(id: &str, text: &str) -> DisplayEvent {
    match text_delta(id, text) {
        NormalizedEvent::Display(event) => event,
        _ => unreachable!(),
    }
}

/// A call's input streamed as many fragments reaches the display lane joined: a few events
/// carrying the whole input in order, not one event per fragment, so a reader of a lane that
/// keeps only its newest events doesn't fall behind on one large input. Any other event first
/// releases the joined input, so the text after it stays after it. The provider names the tool
/// on every fragment, and only the first event of the call keeps the name. With
/// `BROCA_TOOL_NAME_SAMPLES_DIR` set, the call's display events are written there as reader
/// samples.
#[tokio::test]
async fn a_tool_input_streamed_in_fragments_reaches_the_display_lane_joined() {
    let (out, mut events) = mpsc::channel(4096);
    let input = json!({"path": "a", "content": "x".repeat(9000)});
    let text = input.to_string();
    let mut step: Vec<NormalizedEvent> = text
        .as_bytes()
        .chunks(4)
        .map(|piece| {
            NormalizedEvent::Display(DisplayEvent::ToolInputDelta {
                mid: None,
                part: None,
                tool_call_id: ToolCallId("c1".into()),
                tool_name: Some("read".into()),
                delta: String::from_utf8(piece.to_vec()).unwrap(),
            })
        })
        .collect();
    let fragments = step.len();
    step.push(text_delta("a1", "after"));
    step.extend(answer(
        "a1",
        vec![call("c1", "read", input)],
        FinishReason::ToolCalls,
    ));
    DISPLAY_OUT
        .scope(out, async {
            new_loop(&single(vec![step, text_step("a2", "done")])).await;
        })
        .await;
    let display = display_events(&mut events);
    let joined: Vec<&str> = display
        .iter()
        .take_while(|event| matches!(event, DisplayEvent::ToolInputDelta { .. }))
        .filter_map(DisplayEvent::delta)
        .collect();
    assert_eq!(joined.concat(), text, "the whole input, in order");
    assert!(
        joined.len() * 50 < fragments,
        "{} events carried {fragments} fragments",
        joined.len()
    );
    assert_eq!(display[joined.len()], expected_delta("a1", "after"));
    let names: Vec<Option<&str>> = display[..joined.len()]
        .iter()
        .map(|event| match event {
            DisplayEvent::ToolInputDelta { tool_name, .. } => tool_name.as_deref(),
            _ => unreachable!(),
        })
        .collect();
    assert!(names.len() > 1, "the input spans several events: {names:?}");
    assert_eq!(names[0], Some("read"));
    assert!(names[1..].iter().all(Option::is_none), "{names:?}");
    if let Some(dir) = std::env::var_os("BROCA_TOOL_NAME_SAMPLES_DIR") {
        let lines: Vec<String> = display[..joined.len()]
            .iter()
            .map(|event| serde_json::to_string(event).unwrap())
            .collect();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            std::path::Path::new(&dir).join("tool_input_delta.fragments.jsonl"),
            lines.join("\n") + "\n",
        )
        .unwrap();
    }
}

/// A provider that pauses mid-input doesn't hold back what it already streamed: a joined
/// fragment reaches the display lane when its hold ends, while the stream is still open.
#[tokio::test]
async fn a_tool_input_fragment_goes_out_while_its_provider_pauses() {
    let (out, mut events) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let fragment = DisplayEvent::ToolInputDelta {
        mid: None,
        part: None,
        tool_call_id: ToolCallId("c1".into()),
        tool_name: None,
        delta: r#"{"path":"#.into(),
    };
    let scenario = single(vec![vec![NormalizedEvent::Display(fragment.clone())]]);
    let run = DISPLAY_OUT.scope(
        out,
        HOLD_STREAM.scope(
            true,
            TEST_CANCEL.scope(cancel.clone(), async {
                new_loop(&scenario).await;
            }),
        ),
    );
    let observe = async {
        // The stream stays open after the fragment, so only the hold's end can release it.
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match events.recv().await {
                    Some(OutputEvent::Display(event)) => return event,
                    Some(_) => continue,
                    None => panic!("the run ended before the fragment reached the display lane"),
                }
            }
        })
        .await
        .expect("the held fragment goes out while its provider pauses");
        cancel.cancel();
        while events.recv().await.is_some() {}
        first
    };
    let (_, first) = tokio::join!(run, observe);
    assert_eq!(without_mid(first), fragment);
}

#[tokio::test]
async fn cancelled_stream_is_reset() {
    let (out, mut events) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let scenario = single(vec![vec![text_delta("a0", "abandoned")]]);
    let run = DISPLAY_OUT.scope(
        out,
        HOLD_STREAM.scope(
            true,
            TEST_CANCEL.scope(cancel.clone(), async {
                new_loop(&scenario).await;
            }),
        ),
    );
    let observe = async {
        let mut display = Vec::new();
        while let Some(event) = events.recv().await {
            if let OutputEvent::Display(event) = event {
                if matches!(event, DisplayEvent::TextDelta { .. }) {
                    cancel.cancel();
                }
                display.push(without_mid(event));
            }
        }
        display
    };
    let (_, display) = tokio::join!(run, observe);
    assert_eq!(
        display,
        vec![
            expected_delta("a0", "abandoned"),
            DisplayEvent::DisplayReset
        ]
    );
}

async fn check_errored_stream(retries: u32) {
    let (out, mut events) = mpsc::channel(64);
    let mut failure = grammar_refused();
    if let NormalizedEvent::Control(ProviderControl::Error { retries_used, .. }) = &mut failure[0] {
        *retries_used = retries;
    }
    failure.insert(0, text_delta("a0", "abandoned"));
    DISPLAY_OUT
        .scope(out, async {
            new_loop(&single(vec![failure])).await;
        })
        .await;
    assert_eq!(
        display_events(&mut events),
        vec![
            expected_delta("a0", "abandoned"),
            DisplayEvent::DisplayReset
        ]
    );
}

#[tokio::test]
async fn permanent_error_stream_is_reset() {
    check_errored_stream(0).await;
}

#[tokio::test]
async fn exhausted_retries_stream_is_reset() {
    check_errored_stream(3).await;
}

#[tokio::test]
async fn stored_stream_is_not_reset() {
    let (out, mut events) = mpsc::channel(64);
    let mut answer = text_step("a0", "saved");
    answer.insert(0, text_delta("a0", "saved"));
    DISPLAY_OUT
        .scope(out, async {
            new_loop(&single(vec![answer])).await;
        })
        .await;
    assert_eq!(
        display_events(&mut events),
        vec![expected_delta("a0", "saved")]
    );
}

#[tokio::test]
async fn provider_reset_is_not_duplicated_on_error() {
    let (out, mut events) = mpsc::channel(64);
    let mut failure = grammar_refused();
    failure.insert(0, NormalizedEvent::Display(DisplayEvent::DisplayReset));
    failure.insert(0, text_delta("a0", "abandoned"));
    DISPLAY_OUT
        .scope(out, async {
            new_loop(&single(vec![failure])).await;
        })
        .await;
    assert_eq!(
        display_events(&mut events),
        vec![
            expected_delta("a0", "abandoned"),
            DisplayEvent::DisplayReset
        ]
    );
}

#[tokio::test]
async fn unsupported_cut_stream_is_reset() {
    let (out, mut events) = mpsc::channel(64);
    let mut cut = cut_call(
        answer(
            "a0",
            vec![call("c1", "read", json!({}))],
            FinishReason::ToolCalls,
        ),
        "c1",
        "read",
    );
    cut.insert(0, text_delta("a0", "abandoned"));
    DISPLAY_OUT
        .scope(
            out,
            EXPECT_UNSUPPORTED.scope(
                true,
                STRUCTURED.scope(Some(unconstrained("final_text")), async {
                    new_loop(&single(vec![cut])).await;
                }),
            ),
        )
        .await;
    assert_eq!(
        display_events(&mut events),
        vec![
            expected_delta("a0", "abandoned"),
            DisplayEvent::DisplayReset
        ]
    );
}

#[tokio::test]
async fn unsupported_stopped_cut_stream_is_reset() {
    let (out, mut events) = mpsc::channel(64);
    let mut cut = cut_call(
        answer(
            "a0",
            vec![call("c1", "read", json!({}))],
            FinishReason::ToolCalls,
        ),
        "c1",
        "read",
    );
    cut.insert(0, text_delta("a0", "abandoned"));
    let mut scenario = single(vec![cut]);
    scenario.stop_when = vec![StopCondition::StepCount { n: 1 }];
    DISPLAY_OUT
        .scope(
            out,
            EXPECT_UNSUPPORTED.scope(true, async {
                new_loop(&scenario).await;
            }),
        )
        .await;
    assert_eq!(
        display_events(&mut events),
        vec![
            expected_delta("a0", "abandoned"),
            DisplayEvent::DisplayReset
        ]
    );
}

/// A finalize step drops the tool call its answer asked for, so the call's live words won't be
/// stored: the display lane is reset and given the answer as it is stored, each block at its
/// index in the stored message (the text moves from part 1 to part 0).
#[tokio::test]
async fn a_finalize_step_that_drops_a_call_restreams_the_stored_answer() {
    let (out, mut events) = mpsc::channel(64);
    let mut last = answer(
        "z",
        vec![
            call("cz", "read", json!({"path": "z"})),
            ContentBlock::Text {
                text: "final answer".into(),
            },
        ],
        FinishReason::ToolCalls,
    );
    let streamed_call = DisplayEvent::ToolInputDelta {
        mid: None,
        part: None,
        tool_call_id: ToolCallId("cz".into()),
        tool_name: None,
        delta: r#"{"path":"z"}"#.into(),
    };
    last.insert(0, text_delta("z", "final answer"));
    last.insert(0, NormalizedEvent::Display(streamed_call.clone()));
    BUDGET
        .scope(
            Some(budgeted(&[], Some(1))),
            DISPLAY_OUT.scope(out, async {
                new_loop(&reads(1, last)).await;
            }),
        )
        .await;
    let mut stored = expected_delta("z", "final answer");
    stored.set_part(0);
    assert_eq!(
        display_events(&mut events),
        vec![
            streamed_call,
            expected_delta("z", "final answer"),
            DisplayEvent::DisplayReset,
            stored
        ]
    );
}

#[tokio::test]
async fn grammar_downgrade_stream_is_reset_before_resend() {
    let (out, mut events) = mpsc::channel(64);
    let mut failure = grammar_refused();
    failure.insert(0, text_delta("a0", "abandoned"));
    let mut answer = text_step("a1", r#"{"answer":5}"#);
    answer.insert(0, text_delta("a1", "replacement"));
    DISPLAY_OUT
        .scope(
            out,
            STRUCTURED.scope(Some(strict("final_text")), async {
                new_loop(&single(vec![failure, answer])).await;
            }),
        )
        .await;
    assert_eq!(
        display_events(&mut events),
        vec![
            expected_delta("a0", "abandoned"),
            DisplayEvent::DisplayReset,
            expected_delta("a1", "replacement")
        ]
    );
}

#[tokio::test]
async fn refused_stream_is_reset_before_fallback_stream() {
    let (out, mut events) = mpsc::channel(64);
    let mut refused = refusal("a0");
    refused.insert(0, text_delta("a0", "refused words"));
    let mut fallback = text_step("a1", "fallback words");
    fallback.insert(0, text_delta("a1", "fallback words"));
    let scenario = Scenario {
        input: vec![user("hello")],
        tools: Vec::new(),
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![refused, fallback],
    };
    DISPLAY_OUT
        .scope(
            out,
            compare_with_fallback(None, async {
                new_loop(&scenario).await;
            }),
        )
        .await;
    assert_eq!(
        display_events(&mut events),
        vec![
            expected_delta("a0", "refused words"),
            DisplayEvent::DisplayReset,
            expected_delta("a1", "fallback words"),
        ]
    );
}

/// The run's own model refuses a step: the refusal is durable, the same step goes to the
/// fallback model, and the schedule sends the following steps there too, in both loops.
#[tokio::test]
async fn a_refused_step_is_resent_to_the_fallback_and_the_schedule_routes_the_next() {
    compare_with_fallback(
        None,
        compare(Scenario {
            input: vec![user("read a")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            manifest: None,
            stop_when: Vec::new(),
            steps: vec![
                refusal("a0"),
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done"),
            ],
        }),
    )
    .await;
}

/// Resumed at every cut, a durable refusal sends the step to the fallback exactly once, and a
/// cut before it sends the step to the run's own model again.
#[tokio::test]
async fn every_cut_of_a_refused_step_resumes_alike() {
    compare_with_fallback(
        None,
        compare_every_cut(Scenario {
            input: vec![user("read a")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            manifest: None,
            stop_when: Vec::new(),
            steps: vec![
                refusal("a0"),
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done"),
            ],
        }),
    )
    .await;
}

/// A refusal after a completed tool call commits as a tool step, its calls run once, and the
/// schedule sends the next step to the fallback.
#[tokio::test]
async fn a_refusal_after_a_tool_call_commits_and_arms_the_schedule() {
    compare_with_fallback(
        None,
        compare(Scenario {
            input: vec![user("read a")],
            tools: vec![tool("read", ExecutionMode::Parallel)],
            manifest: None,
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ContentFilter,
                ),
                text_step("a2", "done"),
            ],
        }),
    )
    .await;
}

/// When the prompt already fills the fallback model's window, a refusal commits as it is.
#[tokio::test]
async fn a_refusal_too_large_for_the_fallback_commits() {
    compare_with_fallback(
        Some(50),
        compare(Scenario {
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
                refusal("a2"),
            ],
        }),
    )
    .await;
}

/// The schedule carries into the session's next run under the same model, so its first step
/// goes to the fallback.
#[tokio::test]
async fn a_refusal_schedule_carries_into_the_next_run() {
    compare_with_fallback(
        None,
        compare_runs(
            Scenario {
                input: Vec::new(),
                tools: vec![tool("read", ExecutionMode::Parallel)],
                manifest: None,
                stop_when: Vec::new(),
                steps: vec![
                    refusal("a0"),
                    text_step("a1", "ok"),
                    text_step("a2", "again"),
                ],
            },
            &[
                ("r1", vec![user("first task")]),
                ("r2", vec![user("second task")]),
            ],
        ),
    )
    .await;
}

// A session that ran outside the current compaction contract gains a compaction provider at
// the first run whose plan names one: that run enrolls it, on its existing conversation, with
// its message ids unchanged. These drive the new loop alone, since today's storage can't enroll
// a session; each request is checked against what today's compaction code renders from the
// entries the session serves.

/// The context limit a run under a plan naming a compaction provider freezes here.
const ENROLLED_LIMIT: u64 = 200_000;

/// The system text the enrolling plan renders above the conversation.
const ENROLLED_SYSTEM: &str = "You compact.";

/// One run of an enrollment scenario: its id, its input, the plan it freezes and what the
/// provider answers its steps.
struct EnrollRun {
    id: &'static str,
    input: Vec<Message>,
    manifest: Option<Arc<broca_records::record::PlanManifest>>,
    steps: Vec<Vec<NormalizedEvent>>,
    /// The context limit a run under a plan naming a compaction provider freezes, when not
    /// [`ENROLLED_LIMIT`].
    limit: Option<u64>,
}

fn enrollment_tools() -> Vec<Tool> {
    vec![
        tool("read", ExecutionMode::Parallel),
        tool("rows", ExecutionMode::Parallel),
        tool("big", ExecutionMode::Parallel),
    ]
}

/// The tool plane: `read` answers text, `rows` answers JSON.
fn enrollment_plane() -> Spy {
    Spy::new(
        StubToolPlane::new()
            .with_canned_text("read", "file contents")
            .with_canned_text("rows", r#"{"rows": 2}"#),
    )
}

/// A plan naming a compaction provider, with its own system text.
fn enrolling_manifest() -> Arc<broca_records::record::PlanManifest> {
    let mut manifest = compacted_manifest(&enrollment_tools()).as_ref().clone();
    manifest.system_text = ENROLLED_SYSTEM.into();
    manifest.plan_identity = "plan-compacted".into();
    Arc::new(manifest)
}

/// A plan with no compaction provider that renders part numbers into the model's view.
fn tagging_manifest() -> Arc<broca_records::record::PlanManifest> {
    let mut manifest = planned_manifest(&enrollment_tools()).as_ref().clone();
    manifest.plan = broca_records::record::OpaqueJson::from_json_text(
        json!({"tool_items": [{"provider": "provider"}], "render_tags": true}).to_string(),
    )
    .unwrap();
    Arc::new(manifest)
}

fn enrollment_addr() -> SessionAddr {
    SessionAddr("0123456789abcdef".into())
}

fn enrollment_triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "differential".into(),
        session: "s1".into(),
    }
}

tokio::task_local! {
    /// The mode the enrollment tests create their sessions in.
    static ENROLL_CIPHER: broca_session_store::keys::Cipher;
}

/// Runs `body` once with sessions created encrypted and once with them plain: the enrollment
/// stores its decisions as a session object, and seals and reloads read it back, in either mode.
async fn in_each_cipher<F: std::future::Future<Output = ()>>(body: impl Fn() -> F) {
    for cipher in broca_session_store::keys::Cipher::ALL {
        ENROLL_CIPHER.scope(cipher, body()).await;
    }
}

fn enrollment_key() -> DeploymentKey {
    let cipher = ENROLL_CIPHER
        .try_with(|cipher| *cipher)
        .unwrap_or(broca_session_store::keys::Cipher::XChaCha20Poly1305);
    DeploymentKey::new(1, &[7; 32]).creating(cipher)
}

async fn enrollment_session(backend: &MemoryBackend) -> Session<&MemoryBackend> {
    Session::create(
        backend,
        enrollment_addr(),
        enrollment_triple(),
        &enrollment_key(),
        false,
    )
    .await
    .unwrap()
}

/// Runs `this` on `session` through the new loop, as the module runs a new run: admitted by the
/// shared rules from its request and run control, with `plane` as the compaction provider. A run
/// whose plan names a compaction provider asks for the current plan version and freezes a
/// context limit, as the store actor gives it. The requests the run sent.
async fn enroll_run(
    session: &mut Session<&MemoryBackend>,
    parts: &mut MemoryParts,
    plane: &dyn broca_core::CompactionPlane,
    this: &EnrollRun,
) -> Result<Vec<CallOptions>, broca_session_loop::LoopError> {
    let provider = ScriptedProvider::new(this.steps.clone());
    let finished = enroll_run_with(session, parts, plane, this, &provider).await?;
    assert!(finished.paused.is_none());
    Ok(sent_requests(&provider))
}

async fn enroll_run_with(
    session: &mut Session<&MemoryBackend>,
    parts: &mut MemoryParts,
    plane: &dyn broca_core::CompactionPlane,
    this: &EnrollRun,
    provider: &ScriptedProvider,
) -> Result<broca_session_loop::Finished, broca_session_loop::LoopError> {
    let compacted = this
        .manifest
        .as_deref()
        .is_some_and(broca_core::compaction::has_compaction);
    let scenario = Scenario {
        input: this.input.clone(),
        tools: enrollment_tools(),
        stop_when: Vec::new(),
        manifest: this.manifest.clone(),
        steps: Vec::new(),
    };
    let mut request = scenario.plain_request_for(this.id, &this.input);
    let control = RunControl {
        manifest: this.manifest.clone(),
        new_log_compaction_plan_version: compacted
            .then_some(cortexkit_compaction_view::PLAN_VERSION),
        admitted_context_limit: compacted.then_some(this.limit.unwrap_or(ENROLLED_LIMIT)),
        ..Default::default()
    };
    request.context_limit = control.admitted_context_limit;
    // The model provider refuses a request past the window the run froze, as a real one does.
    if let Some(window) = control.admitted_context_limit {
        provider.set_window(window);
    }
    let admission = broca_session_loop::admit::admission(
        &mut request,
        &control,
        provider,
        &session.operations,
        Some(1),
    )
    .map_err(broca_session_loop::LoopError::Refused)?;
    let tools = enrollment_plane();
    let lifecycle = tools.lifecycle();
    let bind = bind();
    let now = harness_now;
    let seams = Seams {
        person_steers: None,
        provider,
        tools: &tools,
        bind: bind.clone(),
        out: None,
        cancel: CancellationToken::new(),
        stop: Default::default(),
        held: Some(&*lifecycle),
        approval: &HarnessPolicy,
        hook_plane: None,
        now_ms: &now,
        session_address: Some(broca_core::session_refs::session_addr(&bind)),
        compaction_plane: Some(plane),
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        model_call_ready: provider.model_call_ready(),
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    let mut writer = Recording {
        session,
        records: Vec::new(),
        batch_ends: Vec::new(),
        backend: None,
        snapshots: Vec::new(),
        injected: Default::default(),
        ended: Default::default(),
        media: Default::default(),
    };
    // The run's future is large; kept on the heap, a test thread's stack holds it.
    Box::pin(async { through!(writer, |w| run(w, parts, admission, &seams).await) }).await
}

/// Resumes `session`'s latest run through the new loop, with `plane` as its compaction
/// provider and `steps` as what the provider answers.
async fn enroll_resume(
    session: &mut Session<&MemoryBackend>,
    parts: &MemoryParts,
    plane: &dyn broca_core::CompactionPlane,
    steps: Vec<Vec<NormalizedEvent>>,
) -> broca_session_loop::Finished {
    let provider = ScriptedProvider::new(steps);
    enroll_resume_with(session, parts, plane, &provider).await
}

async fn enroll_resume_with(
    session: &mut Session<&MemoryBackend>,
    parts: &MemoryParts,
    plane: &dyn broca_core::CompactionPlane,
    provider: &ScriptedProvider,
) -> broca_session_loop::Finished {
    provider.resuming();
    let tools = enrollment_plane();
    let lifecycle = tools.lifecycle();
    let bind = bind();
    let now = harness_now;
    let seams = Seams {
        person_steers: None,
        provider,
        tools: &tools,
        bind: bind.clone(),
        out: None,
        cancel: CancellationToken::new(),
        stop: Default::default(),
        held: Some(&*lifecycle),
        approval: &HarnessPolicy,
        hook_plane: None,
        now_ms: &now,
        session_address: Some(broca_core::session_refs::session_addr(&bind)),
        compaction_plane: Some(plane),
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        model_call_ready: provider.model_call_ready(),
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    let mut writer = Recording {
        session,
        records: Vec::new(),
        batch_ends: Vec::new(),
        backend: None,
        snapshots: Vec::new(),
        injected: Default::default(),
        ended: Default::default(),
        media: Default::default(),
    };
    Box::pin(async { through!(writer, |w| resume(w, parts, &seams).await) })
        .await
        .expect("the run resumes")
}

/// Three runs outside the compaction contract: system text, a tool call answered with text, and
/// one run that renders part numbers and calls a tool answered with JSON. A later run outside the
/// contract doesn't repeat the session's system text (`fresh_start::append_system_messages`).
fn earlier_runs() -> Vec<EnrollRun> {
    vec![
        EnrollRun {
            id: "r1",
            input: vec![system("You are terse."), user("read a")],
            manifest: None,
            steps: vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done with a"),
            ],
            limit: None,
        },
        EnrollRun {
            id: "r2",
            input: vec![user("count the rows")],
            manifest: Some(tagging_manifest()),
            steps: vec![
                answer(
                    "a3",
                    vec![call("c2", "rows", json!({}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a4", "two rows"),
            ],
            limit: None,
        },
        EnrollRun {
            id: "r3",
            input: vec![user("summarize")],
            manifest: None,
            steps: vec![text_step("a5", "a summary")],
            limit: None,
        },
    ]
}

/// The enrolling run: its plan names a compaction provider.
fn enrolling_run(steps: Vec<Vec<NormalizedEvent>>) -> EnrollRun {
    EnrollRun {
        id: "r4",
        input: vec![user("go on")],
        manifest: Some(enrolling_manifest()),
        steps,
        limit: None,
    }
}

/// The text of a message's text blocks, joined.
fn text_of(message: &Message) -> String {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Every part number the projected entries show, in entry order, each once.
fn shown_tags(entries: &[cortexkit_role_compaction::read::Entry]) -> Vec<u64> {
    let mut seen = std::collections::BTreeSet::new();
    entries
        .iter()
        .flat_map(|entry| entry.parts.iter().filter_map(|part| part.tag))
        .filter(|tag| seen.insert(*tag))
        .collect()
}

/// The session's entries as `session.entries` serves them, projected.
async fn served_entries(
    session: &Session<&MemoryBackend>,
) -> Vec<cortexkit_role_compaction::read::Entry> {
    session
        .compaction_entries(1, usize::MAX)
        .await
        .unwrap()
        .iter()
        .map(broca_session_loop::entries::project)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

/// The enrolling run, after three runs outside the contract:
/// - Setup is sent with no applied view and no earlier request to compare with, carrying the
///   prompt size the last step before the move reported;
/// - the entries cover every message, the suppressed global system messages as system entries
///   with no parts at their own ordinals, so the ordinals have no gaps; each has a time, numbered
///   in the projection's numbering from the highest number an earlier request showed, contiguous
///   and never repeating, the run's own after the enrollment's last;
/// - the JSON tool result's run shows that numbering split from the runner's;
/// - the request carries the plan's system text, not the session's;
/// - ids and every earlier `session.read` are unchanged, and a pre-enrollment id still reads.
#[tokio::test]
async fn an_enrolled_session_serves_and_renders_its_history_on_the_contract() {
    in_each_cipher(|| async {
        with_each_writer(|| async {
            let backend = MemoryBackend::default();
            let mut session = enrollment_session(&backend).await;
            let mut parts = MemoryParts::default();
            let spy = CompactionSpy::new(&CompactionScript {
                context_limit: ENROLLED_LIMIT,
                rides: Vec::new(),
            });
            for earlier in earlier_runs() {
                enroll_run(&mut session, &mut parts, &spy, &earlier)
                    .await
                    .expect("an earlier run");
            }
            assert!(spy.calls().is_empty(), "no compaction call before the move");
            assert!(!session.operations.on_contract());
            let read_before = session.entries(1, usize::MAX).await.unwrap();
            let through = session.conversation.placed;
            let requests = enroll_run(
                &mut session,
                &mut parts,
                &spy,
                &enrolling_run(vec![text_step("a6", "going on")]),
            )
            .await
            .expect("the enrolling run");

            let enrolled = session.operations.enrolled.clone().expect("enrolled");
            assert!(session.operations.on_contract());
            assert_eq!(enrolled.run_id, "r4");
            assert_eq!(enrolled.through_ordinal, through);
            assert!(enrolled.tag_floor > 0, "the tagging run showed numbers");
            assert!(enrolled.tag_high > enrolled.tag_floor);

            // Setup first, with nothing applied. The newest request before the move was outside
            // the contract, so the first ride compares with none: the run would otherwise fail for
            // want of that request's context.
            let calls = spy.calls();
            assert!(calls[0].starts_with("compaction.setup "), "{calls:?}");
            assert!(!calls[0].contains("applied_view"), "{}", calls[0]);
            // The step before the move reported a 100-token prompt, which stands for the fill.
            assert!(
                calls
                    .iter()
                    .any(|call| call.contains("\"last_prompt_tokens\":100")),
                "{calls:?}"
            );
            // That step ran before the session was on the contract, under no view: every call
            // naming its reading names view 0 beside it.
            let readings = prompt_readings(&spy);
            assert!(
                readings_only(&readings)
                    .iter()
                    .filter(|(_, tokens, _)| *tokens == Some(100))
                    .all(|(_, _, view)| *view == Some(0)),
                "{readings:?}"
            );
            assert!(
                calls
                    .iter()
                    .any(|call| call.contains("\"last_prompt_tokens\":100,\"last_prompt_view\":0")),
                "{calls:?}"
            );

            // The entries: every message, the global system messages with no parts, with times.
            let raw = session.entries(1, usize::MAX).await.unwrap();
            let globals: Vec<u64> = raw
                .iter()
                .filter(|entry| broca_core::compaction_v2::systems::is_global(&entry.message))
                .map(|entry| entry.ordinal)
                .collect();
            assert_eq!(globals, vec![1], "the first run's system text");
            assert!(globals.iter().all(|ordinal| *ordinal <= through));
            let served_stored = session.compaction_entries(1, usize::MAX).await.unwrap();
            let served_ordinals: Vec<u64> =
                served_stored.iter().map(|entry| entry.ordinal).collect();
            let expected_ordinals: Vec<u64> = (1..=raw.len() as u64).collect();
            assert_eq!(served_ordinals, expected_ordinals);
            for global in &globals {
                let served = &served_stored[*global as usize - 1];
                assert!(served.message.content.is_empty(), "{served:?}");
            }
            // The projection's numbering, from the floor, a tool-results message taking none.
            let mut high = enrolled.tag_floor;
            for entry in served_stored
                .iter()
                .filter(|entry| entry.ordinal <= through)
            {
                let facts = entry.state.facts.as_ref().unwrap();
                assert_eq!(facts.tag_base, high, "message {}", entry.ordinal);
                assert!(facts.written_at_ms.is_some_and(|at| at > 0));
                high += broca_core::compaction_v2::metadata::numbered_parts(&entry.message);
            }
            assert_eq!(high, enrolled.tag_high);
            let entries = served_entries(&session).await;
            let earlier: Vec<_> = entries
                .iter()
                .filter(|entry| entry.ordinal <= through)
                .cloned()
                .collect();
            let later: Vec<_> = entries
                .iter()
                .filter(|entry| entry.ordinal > through)
                .cloned()
                .collect();
            assert_eq!(
                shown_tags(&earlier),
                (enrolled.tag_floor + 1..=enrolled.tag_high).collect::<Vec<_>>()
            );
            let own = shown_tags(&later);
            assert!(!own.is_empty());
            assert_eq!(
                own,
                (enrolled.tag_high + 1..=enrolled.tag_high + own.len() as u64).collect::<Vec<_>>()
            );
            // The run that rendered numbers numbered its JSON tool result its own way: the runner's
            // numbering and the projection's split there.
            let r2: Vec<(u64, u64)> = served_stored
                .iter()
                .filter_map(|entry| {
                    let facts = entry.state.facts.as_ref()?;
                    (facts.run_id.as_deref() == Some("r2")).then_some(())?;
                    Some((facts.tag_base, facts.runner_tag_base?))
                })
                .collect();
            let raw_r2: Vec<(u64, u64)> = raw
                .iter()
                .filter_map(|entry| {
                    let facts = entry.state.facts.as_ref()?;
                    (facts.run_id.as_deref() == Some("r2")).then_some(())?;
                    Some((facts.tag_base, facts.runner_tag_base?))
                })
                .collect();
            assert!(r2.len() >= 3, "{r2:?}");
            let gaps: std::collections::BTreeSet<i64> = r2
                .iter()
                .map(|(served, runner)| *served as i64 - *runner as i64)
                .collect();
            assert!(
                gaps.len() > 1,
                "the JSON result splits the numberings: {r2:?}"
            );
            assert!(raw_r2.iter().all(|(stored, _)| *stored == 0), "{raw_r2:?}");

            // The request: the plan's system text above the conversation, and no earlier global
            // system message.
            let request = &requests[0];
            assert_eq!(request.prompt[0].role, Role::System);
            assert_eq!(text_of(&request.prompt[0]), ENROLLED_SYSTEM);
            assert!(request.prompt[1..]
                .iter()
                .all(|message| !broca_core::compaction_v2::systems::is_global(message)));
            assert!(request
                .prompt
                .iter()
                .all(|message| !text_of(message).contains("You are terse.")));
            let texts: Vec<String> = request.prompt.iter().map(text_of).collect();
            for expected in ["read a", "count the rows", "summarize", "go on"] {
                assert!(
                    texts.iter().any(|text| text.contains(expected)),
                    "{expected} in {texts:?}"
                );
            }

            // Ids and reads of the earlier messages are unchanged, and a pre-enrollment id reads.
            assert_eq!(&raw[..read_before.len()], &read_before[..]);
            let first_user = &read_before[1];
            let mid = &first_user.state.facts.as_ref().unwrap().mid;
            let by_id = session
                .entry_by_mid(mid)
                .await
                .unwrap()
                .expect("found by id");
            assert_eq!(by_id.message, first_user.message);
            let global_mid = &read_before[0].state.facts.as_ref().unwrap().mid;
            let global = session
                .entry_by_mid(global_mid)
                .await
                .unwrap()
                .expect("the suppressed message is found by id");
            assert_eq!(global, served_stored[0]);
        })
        .await;
    })
    .await;
}

/// A view the compaction provider hands over on the enrolling run, which cuts through history
/// before the enrollment point and trims the run's own big output, applies to the enrolled
/// session's entries, and its cut seals history through the enrollment point. Reloaded, the
/// session serves the same entries, sealed reads and numbers.
#[tokio::test]
async fn a_cut_and_a_view_on_the_enrolling_run_seal_through_the_enrollment_and_reload() {
    in_each_cipher(|| async {
        with_each_writer(|| async {
            let backend = MemoryBackend::default();
            let mut session = enrollment_session(&backend).await;
            let mut parts = MemoryParts::default();
            let quiet = CompactionSpy::new(&CompactionScript {
                context_limit: ENROLLED_LIMIT,
                rides: Vec::new(),
            });
            for earlier in earlier_runs() {
                enroll_run(&mut session, &mut parts, &quiet, &earlier)
                    .await
                    .expect("an earlier run");
            }
            let read_before = session.entries(1, usize::MAX).await.unwrap();
            let ordinal_of = |text: &str| {
                read_before
                    .iter()
                    .find(|entry| text_of(&entry.message) == text)
                    .map(|entry| entry.ordinal)
                    .unwrap()
            };
            let cut = ordinal_of("done with a");
            // The enrolling run's big tool result puts its second request over the window; the
            // provider answers the ride with a view that cuts through an earlier message and trims
            // the big output without removing the final tool turn, and the step renders it.
            let big_call = session.conversation.placed + 2;
            let spy = CompactionSpy::new(&CompactionScript {
                context_limit: 1_200,
                rides: vec![Ride::CutAndTrim(cut, big_call)],
            });
            spy.read_session(&session).await;
            let mut enrolling = enrolling_run(vec![
                answer(
                    "a6",
                    vec![call("c3", "big", json!({}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a7", "done"),
            ]);
            enrolling.limit = Some(1_200);
            let requests = enroll_run(&mut session, &mut parts, &spy, &enrolling)
                .await
                .expect("the enrolling run");
            let enrolled = session.operations.enrolled.clone().unwrap();
            assert!(session.operations.view_applied.is_some());
            assert!(
                session.conversation.window_start() > 1,
                "the cut sealed history: {:?}",
                spy.calls()
            );
            assert!(session.conversation.window_start() <= enrolled.through_ordinal);
            let texts =
                |request: &CallOptions| request.prompt.iter().map(text_of).collect::<Vec<_>>();
            // The first step's request, the one the provider refused as too long, and
            // the one sent again on the ride's view.
            assert_eq!(requests.len(), 3, "{:?}", spy.calls());
            assert!(texts(&requests[0])
                .iter()
                .any(|text| text.contains("read a")));
            assert!(!texts(&requests[2])
                .iter()
                .any(|text| text.contains("read a")));
            assert!(!texts(&requests[2])
                .iter()
                .any(|text| text.contains("done with a")));
            assert!(texts(&requests[2])
                .iter()
                .any(|text| text.contains("count the rows")));
            assert_eq!(text_of(&requests[2].prompt[0]), ENROLLED_SYSTEM);
            let blocks: Vec<_> = requests[2]
                .prompt
                .iter()
                .flat_map(|message| &message.content)
                .collect();
            assert!(blocks.iter().any(|block| {
                matches!(block, ContentBlock::ToolCall(call) if call.tool_call_id.0 == "c3")
            }));
            assert!(blocks.iter().any(|block| {
                matches!(block, ContentBlock::ToolResult(result)
                    if result.tool_call_id.0 == "c3"
                        && result.output == ToolOutput::Text { text: "compacted tool output".into() })
            }));

            let entries = served_entries(&session).await;
            let sealed_mid = read_before
                .iter()
                .find(|entry| entry.ordinal == cut)
                .and_then(|entry| entry.state.facts.as_ref())
                .map(|facts| facts.mid.clone())
                .unwrap();
            let sealed = session.entry_by_mid(&sealed_mid).await.unwrap().unwrap();
            let loaded = Session::load(
                &backend,
                enrollment_addr(),
                &enrollment_triple(),
                &enrollment_key(),
            )
            .await
            .unwrap();
            assert_eq!(loaded.operations.enrolled, Some(enrolled.clone()));
            assert_eq!(served_entries(&loaded).await, entries);
            assert_eq!(
                loaded.entry_by_mid(&sealed_mid).await.unwrap(),
                Some(sealed.clone())
            );
            // The sealed message keeps the number the enrollment gave it.
            let cut_tag = entries
                .iter()
                .find(|entry| entry.ordinal == cut)
                .and_then(|entry| entry.parts.iter().find_map(|part| part.tag))
                .expect("the cut message shows a number");
            assert_eq!(sealed.state.facts.as_ref().unwrap().tag_base + 1, cut_tag);
            // Reads of the earlier messages are as they were, sealed or not.
            assert_eq!(
                loaded.entries(1, read_before.len()).await.unwrap(),
                read_before
            );
        })
        .await;
    })
    .await;
}

/// A crash at any point of the enrolling run: before its start's batch is durable the session
/// isn't enrolled and the next send enrolls it; after it, the session is enrolled once and the
/// resumed run never renumbers.
#[tokio::test]
async fn a_crash_at_any_point_of_the_enrolling_run_enrolls_once_and_never_renumbers() {
    in_each_cipher(|| async {
        // The run's future is large; kept on the heap, a test thread's stack holds it.
        Box::pin(crash_at_any_point_of_the_enrolling_run()).await;
    })
    .await;
}

async fn crash_at_any_point_of_the_enrolling_run() {
    let quiet = || {
        CompactionSpy::new(&CompactionScript {
            context_limit: ENROLLED_LIMIT,
            rides: Vec::new(),
        })
    };
    let steps = || {
        vec![
            answer(
                "a6",
                vec![call("c3", "read", json!({}))],
                FinishReason::ToolCalls,
            ),
            text_step("a7", "done"),
        ]
    };
    let prepared = || async {
        let backend: &'static MemoryBackend = Box::leak(Box::default());
        let mut session = enrollment_session(backend).await;
        let mut parts = MemoryParts::default();
        for earlier in earlier_runs() {
            enroll_run(&mut session, &mut parts, &quiet(), &earlier)
                .await
                .expect("an earlier run");
        }
        (backend, session, parts)
    };
    let (_, mut reference, mut parts) = prepared().await;
    enroll_run(
        &mut reference,
        &mut parts,
        &quiet(),
        &enrolling_run(steps()),
    )
    .await
    .expect("the enrolling run");
    let expected = reference.operations.enrolled.clone().unwrap();
    let expected_entries = served_entries(&reference).await;
    let earlier_tags = |entries: &[cortexkit_role_compaction::read::Entry]| {
        entries
            .iter()
            .filter(|entry| entry.ordinal <= expected.through_ordinal)
            .map(|entry| {
                (
                    entry.ordinal,
                    entry.parts.iter().map(|part| part.tag).collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    };
    let mut crashes = 0;
    for mutations in 0.. {
        let (backend, mut session, mut parts) = prepared().await;
        backend.crash_after(Some(mutations));
        let outcome = enroll_run(&mut session, &mut parts, &quiet(), &enrolling_run(steps())).await;
        backend.crash_after(None);
        if outcome.is_ok() {
            break;
        }
        crashes += 1;
        drop(session);
        let mut loaded = Session::load(
            backend,
            enrollment_addr(),
            &enrollment_triple(),
            &enrollment_key(),
        )
        .await
        .unwrap();
        let started = loaded
            .operations
            .run
            .as_ref()
            .and_then(|run| run.start.as_ref())
            .is_some_and(|start| start.run_id == "r4");
        assert_eq!(
            started,
            loaded.operations.enrolled.is_some(),
            "after {mutations}"
        );
        if started {
            let finished = loaded.operations.run.as_ref().unwrap().finished.is_some();
            if !finished {
                enroll_resume(&mut loaded, &parts, &quiet(), vec![text_step("a7", "done")]).await;
            }
        } else {
            assert!(!loaded.operations.on_contract(), "after {mutations}");
            enroll_run(&mut loaded, &mut parts, &quiet(), &enrolling_run(steps()))
                .await
                .expect("the next send enrolls");
        }
        let enrolled = loaded.operations.enrolled.clone().expect("enrolled once");
        assert_eq!(
            (
                enrolled.through_ordinal,
                enrolled.tag_floor,
                enrolled.tag_high,
                enrolled.run_id.as_str()
            ),
            (
                expected.through_ordinal,
                expected.tag_floor,
                expected.tag_high,
                "r4"
            ),
            "after {mutations}"
        );
        assert_eq!(
            earlier_tags(&served_entries(&loaded).await),
            earlier_tags(&expected_entries),
            "after {mutations}"
        );
    }
    assert!(crashes > 3, "the run crashed at several points: {crashes}");
}

/// A compaction provider that refuses its first Setup: the run that asks ends unavailable.
struct SetupFailsOnce {
    inner: CompactionSpy,
    failed: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl broca_core::CompactionPlane for SetupFailsOnce {
    async fn setup(
        &self,
        module: &str,
        bind: &broca_protocol::BindIdentity,
        scope: Option<&broca_protocol::SessionScope>,
        request: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        self.inner.setup(module, bind, scope, request).await
    }

    async fn call_v2(
        &self,
        module: &str,
        bind: &broca_protocol::BindIdentity,
        scope: Option<&broca_protocol::SessionScope>,
        method: &str,
        params: Value,
        deadline: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        if method == "compaction.setup"
            && !self.failed.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner
                .calls
                .lock()
                .unwrap()
                .push((method.into(), params));
            // A refused Setup ends a run with no view at once; an unanswered one would be
            // re-sent within the run instead (compaction_v2's own tests cover that).
            return Err(broca_core::compaction_v2::runtime::CallError::Refused(
                broca_core::compaction_v2::protocol::wire::Refusal::new(
                    "unknown_preset",
                    "refused",
                    None,
                ),
            ));
        }
        self.inner
            .call_v2(module, bind, scope, method, params, deadline)
            .await
    }

    async fn provider_capabilities(&self, module: &str) -> broca_core::ProviderCapabilities {
        self.inner.provider_capabilities(module).await
    }
}

/// A refused Setup on the enrolling run ends it `compaction_unavailable`; the session stays
/// enrolled, and the next send asks for Setup again and runs on.
#[tokio::test]
async fn a_failed_setup_ends_the_enrolling_run_and_the_next_send_retries_it() {
    in_each_cipher(|| async {
        let backend = MemoryBackend::default();
        let mut session = enrollment_session(&backend).await;
        let mut parts = MemoryParts::default();
        let plane = SetupFailsOnce {
            inner: CompactionSpy::new(&CompactionScript {
                context_limit: ENROLLED_LIMIT,
                rides: Vec::new(),
            }),
            failed: Default::default(),
        };
        for earlier in earlier_runs() {
            enroll_run(&mut session, &mut parts, &plane.inner, &earlier)
                .await
                .expect("an earlier run");
        }
        let provider = ScriptedProvider::new(vec![text_step("a6", "never sent")]);
        let finished = enroll_run_with(
            &mut session,
            &mut parts,
            &plane,
            &enrolling_run(Vec::new()),
            &provider,
        )
        .await
        .expect("the run ends");
        assert_eq!(finished.reason, broca_protocol::RunFinishReason::Error);
        let ended = session
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .find_map(|(_, record)| match record {
                Record::RunEnd(end) if end.run_id == "r4" => end.error,
                _ => None,
            })
            .expect("the enrolling run ended with an error");
        assert!(
            ended.to_string().contains("compaction_unavailable"),
            "{ended}"
        );
        assert!(
            sent_requests(&provider).is_empty(),
            "no request without Setup"
        );
        let enrolled = session.operations.enrolled.clone().expect("still enrolled");
        let next = EnrollRun {
            id: "r5",
            input: vec![user("try again")],
            manifest: Some(enrolling_manifest()),
            steps: vec![text_step("a7", "back")],
            limit: None,
        };
        let requests = enroll_run(&mut session, &mut parts, &plane, &next)
            .await
            .expect("the next send");
        assert_eq!(text_of(&requests[0].prompt[0]), ENROLLED_SYSTEM);
        assert_eq!(session.operations.enrolled, Some(enrolled));
        let setups = plane
            .inner
            .calls()
            .iter()
            .filter(|call| call.starts_with("compaction.setup"))
            .count();
        assert_eq!(setups, 2, "the next send asked for Setup again");
    })
    .await;
}

/// The enrolling run under a refusal fallback renders its own request to estimate it, besides
/// its compaction estimate. When what its requests wait for before they can be rendered fails,
/// neither renders: the step fails with that error before anything is sent, and the run's end
/// records it.
#[tokio::test]
async fn an_enrolling_run_whose_request_cant_be_ready_ends_with_the_error_and_sends_nothing() {
    let fallback = FallbackScript {
        model: ModelHandle::new(provider_id(), "fallback-model"),
        context_limit: Some(1_000_000),
    };
    FALLBACK
        .scope(Some(fallback), async {
            let backend = MemoryBackend::default();
            let mut session = enrollment_session(&backend).await;
            let mut parts = MemoryParts::default();
            let spy = CompactionSpy::new(&CompactionScript {
                context_limit: ENROLLED_LIMIT,
                rides: Vec::new(),
            });
            let refused = EnrollRun {
                id: "r1",
                input: vec![system("You are terse."), user("first task")],
                manifest: None,
                steps: vec![refusal("a0"), text_step("a1", "ok")],
                limit: None,
            };
            enroll_run(&mut session, &mut parts, &spy, &refused)
                .await
                .expect("the refused run");
            let provider = ScriptedProvider::new(vec![text_step("a2", "never sent")]);
            let enrolling = EnrollRun {
                id: "r2",
                input: vec![user("second task")],
                manifest: Some(enrolling_manifest()),
                steps: Vec::new(),
                limit: None,
            };
            let finished = READY
                .scope(
                    Some(ReadyScript::Fails),
                    enroll_run_with(&mut session, &mut parts, &spy, &enrolling, &provider),
                )
                .await
                .expect("the enrolling run ends");
            assert!(session.operations.enrolled.is_some());
            assert_eq!(finished.reason, broca_protocol::RunFinishReason::Error);
            assert!(sent_requests(&provider).is_empty(), "nothing was sent");
            let ends: Vec<Value> = session
                .open_log_records()
                .await
                .unwrap()
                .into_iter()
                .filter_map(|(_, record)| match record {
                    Record::RunEnd(end) if end.run_id == "r2" => {
                        end.error.map(|error| error.parse())
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(ends.len(), 1, "{ends:?}");
            assert_eq!(ends[0]["message"], READINESS_FAILED, "{ends:?}");
        })
        .await;
}

/// A session converted from today's storage, which joined text delivered with a tool result into
/// that result, enrolls through the new loop: the earlier result keeps its stored bytes with the
/// delivered text joined, while text delivered with a result of the enrolling run on stays apart
/// from it and is joined only as a request renders, as the current compaction contract places
/// it. The converted log is one run's, with a hand-written delivery riding its tool result.
#[tokio::test]
async fn a_converted_session_with_a_joined_ride_enrolls_and_keeps_its_bytes() {
    in_each_cipher(|| async {
        // One run with a tool call, and text delivered with its result, as the old storage
        // wrote it and the converter converted it.
        let converted = converted_fixture("joined-ride");

        // The converted session joins delivered text into results.
        let backend = MemoryBackend::default();
        let mut session = Session::create(
            &backend,
            enrollment_addr(),
            enrollment_triple(),
            &enrollment_key(),
            true,
        )
        .await
        .unwrap();
        session.append(&converted.records, true).await.unwrap();
        let mut parts = MemoryParts::default();
        parts.parts = converted.parts;
        let result_text = |message: &Message| -> Option<String> {
            message.content.iter().find_map(|block| match block {
                ContentBlock::ToolResult(result) => match &result.output {
                    ToolOutput::Text { text } => Some(text.to_string()),
                    _ => None,
                },
                _ => None,
            })
        };
        let read_before = session.entries(1, usize::MAX).await.unwrap();
        let joined = read_before
            .iter()
            .find_map(|entry| result_text(&entry.message))
            .expect("the converted run's tool result");
        assert_eq!(joined, "file contents\n\nalso check b");

        // The enrolling run calls a tool too; a send rides that result after its batch ends.
        let spy = CompactionSpy::new(&CompactionScript {
            context_limit: ENROLLED_LIMIT,
            rides: Vec::new(),
        });
        let mut enrolling = enrolling_run(vec![
            answer(
                "a6",
                vec![call("c3", "read", json!({}))],
                FinishReason::ToolCalls,
            ),
            text_step("a7", "done"),
        ]);
        let requests = enroll_run(&mut session, &mut parts, &spy, &enrolling)
            .await
            .expect("the enrolling run");
        let enrolled = session.operations.enrolled.clone().expect("enrolled");
        assert_eq!(enrolled.through_ordinal, read_before.len() as u64);
        assert_eq!(text_of(&requests[0].prompt[0]), ENROLLED_SYSTEM);
        assert!(
            requests[0].prompt.iter().any(|message| result_text(message)
                .is_some_and(|text| text.ends_with(" file contents\n\nalso check b"))),
            "the earlier result goes out with its joined text, numbered"
        );
        session
            .append(
                &[Record::ResultAppend(
                    broca_session_store::record::ResultAppend {
                        run_id: "r4".into(),
                        tool_call_id: ToolCallId("c3".into()),
                        text: "\n\nalso check c".into(),
                        notice: Some(broca_session_store::record::Notice {
                            sender: "occipital".into(),
                            template: None,
                        }),
                        written_at_ms: Some(8),
                        send_id: Some("q2".into()),
                        ride: None,
                    },
                )],
                true,
            )
            .await
            .unwrap();
        let stored: Vec<String> = session
            .entries(1, usize::MAX)
            .await
            .unwrap()
            .iter()
            .filter_map(|entry| result_text(&entry.message))
            .collect();
        assert_eq!(
            stored,
            vec![
                "file contents\n\nalso check b".to_owned(),
                "file contents".to_owned()
            ],
            "the enrolling run's result keeps what the tool returned"
        );
        // The next request joins the later delivery as it renders; the earlier one stays as stored.
        enrolling = EnrollRun {
            id: "r5",
            input: vec![user("next")],
            manifest: Some(enrolling_manifest()),
            steps: vec![text_step("a8", "ok")],
            limit: None,
        };
        let next = enroll_run(&mut session, &mut parts, &spy, &enrolling)
            .await
            .expect("the next run");
        let rendered: Vec<String> = next[0].prompt.iter().map(text_of).collect();
        let joined_once = next[0]
            .prompt
            .iter()
            .filter_map(result_text)
            .filter(|text| text.contains("also check b"))
            .count();
        assert_eq!(joined_once, 1, "the earlier delivery isn't joined again");
        let body =
            String::from_utf8(ScriptedProvider::new(Vec::new()).render_request(&next[0])).unwrap();
        assert!(body.contains("also check c"), "{rendered:?}");
        assert_eq!(body.matches("also check b").count(), 1);
        // Ids and reads of the converted messages are unchanged.
        assert_eq!(
            &session.entries(1, read_before.len()).await.unwrap()[..],
            &read_before[..]
        );
    })
    .await;
}

/// A session converted from today's storage whose first message is its global system text, with
/// a turn-scoped note after the person's first message, from one run that calls a tool.
async fn converted_with_global_system(
    backend: &MemoryBackend,
) -> (Session<&MemoryBackend>, MemoryParts) {
    let converted = converted_fixture("global-system");
    let mut session = Session::create(
        backend,
        enrollment_addr(),
        enrollment_triple(),
        &enrollment_key(),
        true,
    )
    .await
    .unwrap();
    session.append(&converted.records, true).await.unwrap();
    let read = session.entries(1, usize::MAX).await.unwrap();
    assert!(broca_core::compaction_v2::systems::is_global(
        &read[0].message
    ));
    assert!(read[2].message.turn_system, "{:?}", read[2].message);
    let mut parts = MemoryParts::default();
    parts.parts = converted.parts;
    (session, parts)
}

/// The converted session of [`converted_with_global_system`] enrolls through the new loop. Its
/// global system message is suppressed: the plan's system text takes its place. The entries the
/// compaction provider reads still hold it, as a system entry with no parts at ordinal 1, since
/// the contract counts every message, so a view it builds can name that entry. A view that cuts through it,
/// besides trimming the run's big output, is accepted at the step where the provider answers
/// with it and applies; the requests, and the next run's, render byte for byte as under the same
/// view without the cut, since an entry with no parts renders nothing.
#[tokio::test]
async fn a_view_naming_a_suppressed_system_message_applies_and_renders_as_without_it() {
    in_each_cipher(|| async {
        let mut rendered = Vec::new();
        for cut in [false, true] {
            let backend = MemoryBackend::default();
            let (mut session, mut parts) = converted_with_global_system(&backend).await;
            // The enrolling run's big tool result puts its second request over the window; the
            // provider answers the ride with a view that trims the big output, and cuts through
            // the suppressed system message too in the second pass.
            let big_call = session.conversation.placed + 2;
            let ride = if cut {
                Ride::CutAndTrim(1, big_call)
            } else {
                Ride::Trim(big_call)
            };
            let spy = CompactionSpy::new(&CompactionScript {
                context_limit: 1_200,
                rides: vec![ride],
            });
            spy.read_session(&session).await;
            let mut enrolling = enrolling_run(vec![
                answer(
                    "a6",
                    vec![call("c3", "big", json!({}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a7", "done"),
            ]);
            enrolling.limit = Some(1_200);
            let requests = enroll_run(&mut session, &mut parts, &spy, &enrolling)
                .await
                .expect("the enrolling run");
            assert!(session.operations.enrolled.is_some());
            assert!(
                session.operations.view_applied.is_some(),
                "{:?}",
                spy.calls()
            );
            // The first step's request, the one the provider refused as too long, and
            // the one sent again on the ride's view.
            assert_eq!(requests.len(), 3, "{:?}", spy.calls());
            // Once the run enrolled the session, the entries the provider reads hold the
            // suppressed message's with no parts: the contract counts every message, so a view
            // can name it.
            let read = spy.read_session(&session).await;
            let told = told_entries(&read)
                .into_iter()
                .find(|entry| entry["ordinal"] == 1)
                .expect("the provider read ordinal 1");
            assert_eq!(told["role"], "system", "{told}");
            assert_eq!(told["parts"], json!([]), "{told}");
            assert!(spy.rides.lock().unwrap().is_empty(), "the ride was answered");
            for request in &requests {
                assert_eq!(text_of(&request.prompt[0]), ENROLLED_SYSTEM);
                assert!(request.prompt[1..]
                    .iter()
                    .all(|message| !broca_core::compaction_v2::systems::is_global(message)));
                assert!(request
                    .prompt
                    .iter()
                    .any(|message| text_of(message).contains("a turn-scoped note")));
            }
            assert!(requests[2].prompt.iter().any(|message| {
                message.content.iter().any(|block| {
                    matches!(block, ContentBlock::ToolResult(result)
                        if result.output == ToolOutput::Text { text: "compacted tool output".into() })
                })
            }));
            let next = EnrollRun {
                id: "r5",
                input: vec![user("next")],
                manifest: Some(enrolling_manifest()),
                steps: vec![text_step("a8", "ok")],
                limit: Some(1_200),
            };
            let next = enroll_run(&mut session, &mut parts, &spy, &next)
                .await
                .expect("the next run");
            let render = ScriptedProvider::new(Vec::new());
            rendered.push(
                requests
                    .iter()
                    .chain(&next)
                    .map(|request| render.render_request(request))
                    .collect::<Vec<_>>(),
            );
        }
        // The enrolling run's three requests (one refused as too long) and the next run's one.
        assert_eq!(rendered[0].len(), 4);
        assert!(
            rendered[0] == rendered[1],
            "a cut through the suppressed system message changed what was sent"
        );
    })
    .await;
}

/// Every entry a compaction call's params carry, wherever it sits in them.
fn told_entries(params: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    match params {
        Value::Object(members) => {
            if members.contains_key("ordinal") && members.contains_key("parts") {
                out.push(params.clone());
            }
            for member in members.values() {
                out.extend(told_entries(member));
            }
        }
        Value::Array(items) => {
            for item in items {
                out.extend(told_entries(item));
            }
        }
        _ => {}
    }
    out
}

/// The spend facts of the run that enrolls a session are those of the same run on the same
/// session without the compaction provider: the enrollment's records bill nothing.
#[tokio::test]
async fn the_enrolling_run_bills_as_the_same_run_without_compaction() {
    in_each_cipher(|| async {
        let spend = |compacted: bool| async move {
            let backend = MemoryBackend::default();
            let mut session = enrollment_session(&backend).await;
            let mut parts = MemoryParts::default();
            let spy = CompactionSpy::new(&CompactionScript {
                context_limit: ENROLLED_LIMIT,
                rides: Vec::new(),
            });
            for earlier in earlier_runs() {
                enroll_run(&mut session, &mut parts, &spy, &earlier)
                    .await
                    .expect("an earlier run");
            }
            let mut run = enrolling_run(vec![
                answer(
                    "a6",
                    vec![call("c3", "read", json!({}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a7", "done"),
            ]);
            if !compacted {
                // The same plan without its compaction provider.
                let mut plain = run.manifest.as_deref().unwrap().clone();
                let mut plan: Value = serde_json::from_str(plain.plan.as_json_text()).unwrap();
                plan["compaction_item"] = Value::Null;
                plain.plan =
                    broca_records::record::OpaqueJson::from_json_text(plan.to_string()).unwrap();
                run.manifest = Some(Arc::new(plain));
            }
            enroll_run(&mut session, &mut parts, &spy, &run)
                .await
                .expect("the run");
            assert_eq!(session.operations.enrolled.is_some(), compacted);
            let records = broca_session_loop::billing::run_records(&session, "r4", &bind(), &parts)
                .await
                .unwrap();
            broca_core::billing::compute_segments_for_run(
                &records,
                &RunId("r4".into()),
                "differential",
                &charge_resolver,
            )
            .into_iter()
            .map(|segment| serde_json::to_value(segment).unwrap())
            .collect::<Vec<_>>()
        };
        let enrolled = spend(true).await;
        let plain = spend(false).await;
        assert!(!enrolled.is_empty());
        assert_eq!(enrolled, plain);
    })
    .await;
}

/// A crash after the enrolling run's first step was refused, durably, and before the fallback
/// answered: the resume re-sends the step to the fallback, as today's resume does after a
/// durable refusal, and the session keeps the one enrollment its start's batch recorded.
#[tokio::test]
async fn a_crash_after_a_durable_refusal_on_the_enrolling_run_resumes_to_the_fallback() {
    in_each_cipher(|| async {
        FALLBACK
            .scope(
                Some(FallbackScript {
                    model: ModelHandle::new(provider_id(), "fallback-model"),
                    context_limit: Some(1_000_000),
                }),
                Box::pin(crash_after_a_durable_refusal()),
            )
            .await;
    })
    .await;
}

async fn crash_after_a_durable_refusal() {
    let quiet = || {
        CompactionSpy::new(&CompactionScript {
            context_limit: ENROLLED_LIMIT,
            rides: Vec::new(),
        })
    };
    let steps = || vec![refusal("a6"), text_step("a7", "from the fallback")];
    let mut resumed = 0;
    for mutations in 0.. {
        let backend: &'static MemoryBackend = Box::leak(Box::default());
        let mut session = enrollment_session(backend).await;
        let mut parts = MemoryParts::default();
        for earlier in earlier_runs() {
            enroll_run(&mut session, &mut parts, &quiet(), &earlier)
                .await
                .expect("an earlier run");
        }
        backend.crash_after(Some(mutations));
        let outcome = enroll_run(&mut session, &mut parts, &quiet(), &enrolling_run(steps())).await;
        backend.crash_after(None);
        if outcome.is_ok() {
            break;
        }
        drop(session);
        let mut loaded = Session::load(
            backend,
            enrollment_addr(),
            &enrollment_triple(),
            &enrollment_key(),
        )
        .await
        .unwrap();
        let refused = loaded
            .operations
            .run
            .as_ref()
            .filter(|run| run.finished.is_none())
            .and_then(|run| run.refused_step);
        let Some(step) = refused else {
            continue;
        };
        let enrolled = loaded.operations.enrolled.clone().expect("enrolled");
        let provider = ScriptedProvider::new(vec![text_step("a7", "from the fallback")]);
        enroll_resume_with(&mut loaded, &parts, &quiet(), &provider).await;
        assert_eq!(
            *provider.route.lock().unwrap(),
            Some(broca_core::refusal_fallback::StepRoute::Fallback),
            "after {mutations}: step {step} goes to the fallback"
        );
        assert_eq!(sent_requests(&provider).len(), 1, "after {mutations}");
        assert_eq!(
            loaded.operations.enrolled,
            Some(enrolled),
            "after {mutations}"
        );
        assert!(loaded.operations.run.as_ref().unwrap().finished.is_some());
        resumed += 1;
    }
    assert!(resumed > 0, "a cut fell after the durable refusal");
}

// Each reading of the session's fill names the view the request it measured was rendered under
// (`last_prompt_view`), so the compaction provider can tell a reading taken before the view it
// just built applied from one taken under it. These drive the new loop alone: today's loop
// names no view.

/// What each compaction call's conditions say of the newest measured request, in call order:
/// the call, the prompt size, the view it was rendered under, and the call's params as sent.
fn prompt_readings(spy: &CompactionSpy) -> Vec<(String, Option<u64>, Option<u64>, String)> {
    /// The conditions in a call's params: the one object that states a context window.
    fn conditions_in(value: &Value) -> Option<&Value> {
        match value {
            Value::Object(members) if members.contains_key("context_window") => Some(value),
            Value::Object(members) => members.values().find_map(conditions_in),
            Value::Array(items) => items.iter().find_map(conditions_in),
            _ => None,
        }
    }
    spy.calls
        .lock()
        .unwrap()
        .iter()
        .map(|(method, params)| {
            let conditions = conditions_in(params).unwrap_or(&Value::Null);
            (
                method.clone(),
                conditions["last_prompt_tokens"].as_u64(),
                conditions["last_prompt_view"].as_u64(),
                params.to_string(),
            )
        })
        .collect()
}

/// The prompt size and view of each call in `readings` but Setup, by method. Setup states no
/// reading (the contract's Setup has no member for it); the notice after it does.
fn readings_only(
    readings: &[(String, Option<u64>, Option<u64>, String)],
) -> Vec<(&str, Option<u64>, Option<u64>)> {
    readings
        .iter()
        .filter(|(method, .., params)| {
            let setup = method.starts_with("compaction.setup");
            assert!(!(setup && params.contains("last_prompt_")), "{params}");
            !setup
        })
        .map(|(method, tokens, view, _)| (method.as_str(), *tokens, *view))
        .collect()
}

/// Where the first call naming a reading is in `seen`, every call before it naming neither a
/// reading nor a view.
fn first_reading(seen: &[(&str, Option<u64>, Option<u64>)]) -> usize {
    let first = seen
        .iter()
        .position(|(_, tokens, _)| tokens.is_some())
        .expect("a call names a reading");
    assert!(
        seen[..first]
            .iter()
            .all(|(_, tokens, view)| tokens.is_none() && view.is_none()),
        "{seen:?}"
    );
    first
}

/// A run on the current compaction contract, with its own system text.
fn contract_run(id: &'static str, input: &str, steps: Vec<Vec<NormalizedEvent>>) -> EnrollRun {
    EnrollRun {
        id,
        input: vec![user(input)],
        manifest: Some(enrolling_manifest()),
        steps,
        limit: None,
    }
}

/// A text step whose request the provider measured at `prompt` tokens.
fn measured(id: &str, text: &str, prompt: u64) -> Vec<NormalizedEvent> {
    served(text_step(id, text), usage(prompt, 20), json!({}))
}

/// When the provider builds a view from a reading, until a request goes out under that view
/// every call repeats the reading with the older view (here none, `0`), so the provider doesn't
/// take it as fresh pressure and build another view on it. Once the first request under
/// the view is measured, the calls carry its reading with the new version. Closing the session
/// and opening it again, after the cut sealed the history before it, keeps the same value.
#[tokio::test]
async fn each_reading_names_the_view_its_request_was_rendered_under() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let mut session = enrollment_session(backend).await;
    let mut parts = MemoryParts::default();
    let spy = CompactionSpy::new(&CompactionScript {
        context_limit: ENROLLED_LIMIT,
        rides: vec![Ride::Cut(2)],
    });
    enroll_run(
        &mut session,
        &mut parts,
        &spy,
        &contract_run("r1", "first", vec![measured("a1", "one", 700)]),
    )
    .await
    .expect("the first run");
    // The model changes, so the next run's first step rides, and the provider answers with
    // view 1, which cuts through the first answer.
    enroll_run(
        &mut session,
        &mut parts,
        &spy,
        &contract_run("r2@m2", "second", vec![measured("a2", "two", 300)]),
    )
    .await
    .expect("the second run");
    let readings = prompt_readings(&spy);
    let seen = readings_only(&readings);
    assert_eq!(
        session
            .operations
            .view_applied
            .as_ref()
            .map(|applied| applied.view.version),
        Some(1),
        "{seen:?}"
    );
    // Before any reading, the calls name neither a reading nor a view.
    let first = first_reading(&seen);
    let ride = seen
        .iter()
        .position(|(method, _, _)| *method == "compaction.ride")
        .expect("the second run rides");
    let measured_under_view = seen
        .iter()
        .position(|(_, tokens, _)| *tokens == Some(300))
        .expect("a call reports the request sent under the view");
    assert!(ride < measured_under_view, "{seen:?}");
    // From the first reading until the first request under view 1 is measured, every call,
    // the ride that built the view and each call after the view was accepted included,
    // repeats the first reading with the view its request carried: none.
    assert!(
        seen[first..measured_under_view]
            .iter()
            .all(|(_, tokens, view)| (*tokens, *view) == (Some(700), Some(0))),
        "{seen:?}"
    );
    assert!(
        seen[ride + 1..measured_under_view]
            .iter()
            .any(|(method, _, _)| *method != "compaction.ride"),
        "a call after the view was accepted and before a request under it: {seen:?}"
    );
    // From then on, the new reading names the new view.
    assert!(
        seen[measured_under_view..]
            .iter()
            .all(|(_, tokens, view)| (*tokens, *view) == (Some(300), Some(1))),
        "{seen:?}"
    );
    // As the provider receives it, the reading and its view side by side.
    let notice = readings
        .iter()
        .find(|(method, tokens, ..)| method == "compaction.notice" && *tokens == Some(300))
        .map(|(.., params)| params)
        .expect("a notice after the measured request");
    assert!(
        notice.contains("\"last_prompt_tokens\":300,\"last_prompt_view\":1"),
        "{notice}"
    );
    let stale = readings
        .iter()
        .take_while(|(_, tokens, ..)| *tokens != Some(300))
        .filter(|(method, ..)| method == "compaction.notice")
        .last()
        .map(|(.., params)| params)
        .expect("a notice before the measured request");
    assert!(
        stale.contains("\"last_prompt_tokens\":700,\"last_prompt_view\":0"),
        "{stale}"
    );

    // Closed and opened again, the session's next Setup names the same reading and view.
    drop(session);
    let mut loaded = Session::load(
        backend,
        enrollment_addr(),
        &enrollment_triple(),
        &enrollment_key(),
    )
    .await
    .unwrap();
    assert!(
        loaded.conversation.window_start() > 1,
        "the cut sealed the history before it"
    );
    let reopened = CompactionSpy::new(&CompactionScript {
        context_limit: ENROLLED_LIMIT,
        rides: Vec::new(),
    });
    enroll_run(
        &mut loaded,
        &mut parts,
        &reopened,
        &contract_run("r3@m2", "third", vec![measured("a3", "three", 400)]),
    )
    .await
    .expect("the run after the reopen");
    let after = prompt_readings(&reopened);
    let after = readings_only(&after);
    assert_eq!((after[0].1, after[0].2), (Some(300), Some(1)), "{after:?}");
    assert_eq!(
        after.last().copied(),
        Some(("compaction.notice", Some(400), Some(1))),
        "{after:?}"
    );
}

/// A view applied to a step whose request fails without usage leaves the reading where it
/// was, with the view of the step that was measured: the next run's calls, the ride that
/// builds another view included, name the first step's reading and its view (none), not the
/// view the failed step applied.
#[tokio::test]
async fn a_reading_keeps_its_view_when_a_later_view_never_got_measured() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let mut session = enrollment_session(backend).await;
    let mut parts = MemoryParts::default();
    let spy = CompactionSpy::new(&CompactionScript {
        context_limit: ENROLLED_LIMIT,
        rides: vec![Ride::Cut(2), Ride::Cut(3)],
    });
    enroll_run(
        &mut session,
        &mut parts,
        &spy,
        &contract_run("r1", "first", vec![measured("a1", "one", 700)]),
    )
    .await
    .expect("the first run");
    let failure = vec![NormalizedEvent::Control(ProviderControl::Error {
        error: serde_json::from_value(json!({
            "class": "permanent",
            "message": "the request failed",
            "status": 400
        }))
        .unwrap(),
        retries_used: 0,
    })];
    // View 1 applies to this run's step, whose request fails without usage.
    let _ = enroll_run(
        &mut session,
        &mut parts,
        &spy,
        &contract_run("r2@m2", "second", vec![failure]),
    )
    .await;
    assert_eq!(
        session
            .operations
            .view_applied
            .as_ref()
            .map(|applied| applied.view.version),
        Some(1)
    );
    let before = spy.calls.lock().unwrap().len();
    enroll_run(
        &mut session,
        &mut parts,
        &spy,
        &contract_run("r3@m3", "third", vec![measured("a3", "three", 300)]),
    )
    .await
    .expect("the third run");
    let readings = prompt_readings(&spy);
    let seen = readings_only(&readings[before..]);
    assert_eq!(
        session
            .operations
            .view_applied
            .as_ref()
            .map(|applied| applied.view.version),
        Some(2),
        "{seen:?}"
    );
    let measured_under_view = seen
        .iter()
        .position(|(_, tokens, _)| *tokens == Some(300))
        .expect("a call reports the request sent under view 2");
    assert!(
        seen[..measured_under_view]
            .iter()
            .any(|(method, _, _)| *method == "compaction.ride"),
        "{seen:?}"
    );
    assert!(
        seen[..measured_under_view]
            .iter()
            .all(|(_, tokens, view)| (*tokens, *view) == (Some(700), Some(0))),
        "{seen:?}"
    );
    assert!(
        seen[measured_under_view..]
            .iter()
            .all(|(_, tokens, view)| (*tokens, *view) == (Some(300), Some(2))),
        "{seen:?}"
    );
}

/// On a session no view ever applied to, every reading names no view (`0`), and before the
/// first reading the calls name neither.
#[tokio::test]
async fn a_reading_with_no_view_ever_names_none() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let mut session = enrollment_session(backend).await;
    let mut parts = MemoryParts::default();
    let spy = CompactionSpy::new(&CompactionScript {
        context_limit: ENROLLED_LIMIT,
        rides: Vec::new(),
    });
    enroll_run(
        &mut session,
        &mut parts,
        &spy,
        &contract_run(
            "r1",
            "read a",
            vec![
                served(
                    answer(
                        "a1",
                        vec![call("c1", "read", json!({}))],
                        FinishReason::ToolCalls,
                    ),
                    usage(700, 20),
                    json!({}),
                ),
                measured("a2", "done", 800),
            ],
        ),
    )
    .await
    .expect("the run");
    enroll_run(
        &mut session,
        &mut parts,
        &spy,
        &contract_run("r2", "again", vec![measured("a3", "again", 900)]),
    )
    .await
    .expect("the second run");
    let readings = prompt_readings(&spy);
    let seen = readings_only(&readings);
    assert!(session.operations.view_applied.is_none());
    let first = first_reading(&seen);
    assert_eq!(seen[first].1, Some(700), "{seen:?}");
    for reading in [700, 800, 900] {
        assert!(
            seen.contains(&("compaction.notice", Some(reading), Some(0))),
            "{seen:?}"
        );
    }
    assert!(
        seen.iter()
            .all(|(_, tokens, view)| *view == tokens.map(|_| 0)),
        "{seen:?}"
    );
}

/// A session converted from today's storage, whose last step ran under a view: its entry
/// facts carry that view, so the first run in the session store names it with the step's reading.
#[tokio::test]
async fn a_converted_sessions_reading_names_the_view_today_rendered_it_under() {
    // A run that called a tool and answered, measured at 900 tokens, as the old storage wrote
    // it under view 1 and the converter converted it.
    let converted = converted_fixture("view-reading");
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let mut session = enrollment_session(backend).await;
    session.append(&converted.records, true).await.unwrap();
    let mut parts = MemoryParts::default();
    parts.parts = converted.parts;
    assert_eq!(
        session
            .operations
            .view_applied
            .as_ref()
            .map(|applied| applied.view.version),
        Some(1),
        "today's step ran under view 1"
    );
    let spy = CompactionSpy::new(&CompactionScript {
        context_limit: ENROLLED_LIMIT,
        rides: Vec::new(),
    });
    enroll_run(
        &mut session,
        &mut parts,
        &spy,
        &contract_run("r2", "more", vec![measured("a3", "more", 1_000)]),
    )
    .await
    .expect("the first run on the new storage");
    let readings = prompt_readings(&spy);
    let seen = readings_only(&readings);
    assert_eq!((seen[0].1, seen[0].2), (Some(900), Some(1)), "{seen:?}");
    assert_eq!(
        seen.last().copied(),
        Some(("compaction.notice", Some(1_000), Some(1))),
        "{seen:?}"
    );
}

/// The probe placement of rides after a tool turn's results, run on the new loop alone.
#[path = "differential/ride_placement.rs"]
mod ride_placement;

#[path = "differential/plan_notes.rs"]
mod plan_notes;

/// A call handed back for the person's steer is closed by the interim content its provider
/// answered with, and its outcome names that provider. That result is written as soon as the
/// hand-back is answered, without waiting for the other calls dispatched in the same step to
/// finish. The provider's later result for the call then reaches the session as a notice.
/// Nothing waits on it as a held call, and a call dispatched beside it ends as usual.
#[tokio::test]
async fn a_handed_back_call_is_closed_by_its_interim_content_and_names_its_provider() {
    let scenario = Scenario {
        input: vec![user("build it")],
        tools: vec![
            tool("handback", ExecutionMode::Parallel),
            tool("read", ExecutionMode::Parallel),
        ],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "handback", json!({})),
                    call("c2", "read", json!({"path": "a"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    };
    let outcome = new_loop(&scenario).await;
    let at = |name: &str, id: &str| {
        outcome
            .records
            .iter()
            .position(|record| {
                record
                    .get(name)
                    .is_some_and(|body| body["tool_call_id"] == id)
            })
            .unwrap_or_else(|| panic!("no {name} for {id}: {:?}", outcome.records))
    };
    let handed_back = &outcome.records[at("ToolOutcome", "c1")]["ToolOutcome"];
    assert_eq!(handed_back["yielded_by"], "provider", "{handed_back}");
    assert_eq!(handed_back["outcome"], "Completed", "{handed_back}");
    let beside = &outcome.records[at("ToolOutcome", "c2")]["ToolOutcome"];
    assert!(beside["yielded_by"].is_null(), "{beside}");
    let reply = outcome
        .records
        .iter()
        .filter_map(|record| record.get("ToolReply"))
        .find(|reply| reply["result"]["tool_call_id"] == "c1")
        .unwrap_or_else(|| panic!("no reply for c1: {:?}", outcome.records));
    assert!(reply.to_string().contains("still building"), "{reply}");
    assert!(
        at("ToolOutcome", "c1") < at("ToolOutcome", "c2"),
        "the handed-back call is durable as it answers, before the batch ends: {:?}",
        outcome.records
    );
    assert!(
        !outcome
            .records
            .iter()
            .any(|record| record.get("ToolHeld").is_some()),
        "a handed-back call is never held: {:?}",
        outcome.records
    );
}

/// A step whose view replaces a tool output leaves that output out of its request, so the run
/// releases it from memory once a step renders it as replaced and it is no longer the window's
/// newest message: here the third step, after the view the compaction provider sent at the
/// second step (which went over the context limit) replaced the first call's output. The tool message's content is released in the session's window, and reads back from
/// the open log whole.
#[tokio::test]
async fn a_replaced_tool_output_is_released_and_reads_back_whole() {
    let scenario = compacted_scenario(vec![
        answer(
            "a1",
            vec![call("c1", "big", json!({}))],
            FinishReason::ToolCalls,
        ),
        answer(
            "a2",
            vec![call("c2", "read", json!({"path": "b"}))],
            FinishReason::ToolCalls,
        ),
        text_step("a3", "done"),
    ]);
    let released = RELEASED
        .scope(std::cell::RefCell::new(Vec::new()), async {
            LAYOUT
                .scope(
                    Some(cortexkit_compaction_view::PLAN_VERSION),
                    COMPACTION.scope(
                        Some(Arc::new(CompactionScript {
                            context_limit: 1_200,
                            rides: vec![Ride::Trim(2)],
                        })),
                        Box::pin(new_loop(&scenario)),
                    ),
                )
                .await;
            RELEASED.with(|seen| seen.take())
        })
        .await;
    let ordinals: Vec<u64> = released.iter().map(|(ordinal, _)| *ordinal).collect();
    assert_eq!(
        ordinals,
        [3],
        "the tool message, and nothing else, is released"
    );
    let ContentBlock::ToolResult(result) = &released[0].1.content[0] else {
        panic!(
            "the released message is the tool result: {:?}",
            released[0].1
        );
    };
    assert_eq!(
        result.output,
        ToolOutput::Text {
            text: "x".repeat(8_000).into()
        },
        "the output reads back whole"
    );
}

/// A crash after the caller's stop-now was acknowledged, and before the step's calls were sent:
/// the resume closes them as not run and ends cancelled, even with a fresh cancel token. Sending
/// them would act after the caller was told the run stopped.
#[tokio::test]
async fn a_resume_after_an_acknowledged_stop_now_sends_none_of_its_calls() {
    use broca_session_store::record::{CancelPoint, CancelRequest};
    let scenario = unsent_call();
    let source = new_source(&scenario, None, scenario.steps.clone()).await;
    // The cut just after the step that asked for the call: its answer is durable, no batch.
    let cut = source
        .batch_ends
        .iter()
        .copied()
        .find(|&end| {
            let kept = &source.records[..end];
            kept.iter()
                .any(|record| matches!(record, Record::StepEnd(_)))
                && !kept
                    .iter()
                    .any(|record| matches!(record, Record::BatchStart(_)))
        })
        .expect("a cut after the call's step and before its batch");
    let run_id = source
        .records
        .iter()
        .find_map(|record| match record {
            Record::RunStart(start) => Some(start.run_id.clone()),
            _ => None,
        })
        .unwrap();
    let mut start = source.cut(cut);
    let backend = start.snapshot.take().expect("the cut keeps its backend");
    let stop = Record::CancelRequest(CancelRequest {
        run_id,
        at: CancelPoint::Now,
    });
    {
        let mut session = Session::load(
            &backend,
            SessionAddr("0123456789abcdef".into()),
            &SessionTriple {
                project_root: "/project".into(),
                harness: "differential".into(),
                session: "s1".into(),
            },
            &DeploymentKey::new(1, &[7; 32]),
        )
        .await
        .unwrap();
        session
            .append(std::slice::from_ref(&stop), true)
            .await
            .unwrap();
    }
    start.snapshot = Some(backend);
    start.records.push(stop);
    let resumed = new_source(&scenario, Some(start), scenario.steps[1..].to_vec()).await;
    assert!(
        resumed
            .outcome
            .calls
            .iter()
            .all(|call| !call.contains("dispatch")),
        "a call was sent after the acknowledged stop: {:?}",
        resumed.outcome.calls
    );
    assert!(
        resumed.outcome.requests.is_empty(),
        "the resume asked the model again"
    );
    let end = resumed
        .records
        .iter()
        .find_map(|record| match record {
            Record::RunEnd(end) => Some(end.reason),
            _ => None,
        })
        .expect("the resume ends the run");
    assert_eq!(end, RunFinishReason::Cancelled);
}

/// A held call's late result goes through the post-tool hooks subscribed for its tool, as a
/// result returned on the dispatch does: a hold must not let the model see a result a hook
/// would have edited or withheld.
#[tokio::test]
async fn a_held_result_goes_through_the_post_tool_hooks() {
    let tools = vec![
        tool("hold", ExecutionMode::Parallel),
        tool("read", ExecutionMode::Parallel),
    ];
    let scenario = Scenario {
        input: vec![user("run both")],
        manifest: Some(hooked_manifest_with(&tools, &["read", "hold"])),
        tools,
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
    };
    let ran = Box::pin(new_with_actions(&scenario, Vec::new(), None, false)).await;
    // A reply carries the result itself; an outcome only its digest.
    let outcomes: Vec<String> = ran
        .source
        .records
        .iter()
        .filter_map(|record| match record {
            Record::ToolReply(reply) => Some(format!("{reply:?}")),
            _ => None,
        })
        .collect();
    let held = outcomes
        .iter()
        .find(|outcome| outcome.contains("\"c1\""))
        .unwrap_or_else(|| panic!("no result for the held call: {outcomes:?}"));
    assert!(
        held.contains("[checked by the harness hook]"),
        "the held result skipped its post-tool hook: {held}"
    );
}

/// A late result already deposited when the resume finds the account signed out is written at
/// the sign-in pause, and through the same post-tool hooks as a result the wait takes.
#[tokio::test]
async fn a_held_result_taken_at_a_sign_in_pause_goes_through_the_post_tool_hooks() {
    let tools = vec![tool("hold", ExecutionMode::Parallel)];
    let scenario = Scenario {
        input: vec![user("hold it")],
        manifest: Some(hooked_manifest_with(&tools, &["hold"])),
        tools,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "hold", json!({"job": 1}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    };
    let ran = SIGNED_OUT_ON_RESUME
        .scope(
            (),
            Box::pin(new_with_actions(&scenario, Vec::new(), None, false)),
        )
        .await;
    let records = &ran.source.records;
    assert!(
        records.iter().any(|record| matches!(
            record,
            Record::Pause(pause) if pause.reason == PauseReason::AuthRequired
        )),
        "the resume didn't pause for sign-in: {records:?}"
    );
    // A reply carries the result itself; an outcome only its digest.
    let held = records
        .iter()
        .find_map(|record| match record {
            Record::ToolReply(reply) if reply.result.tool_call_id.0 == "c1" => {
                Some(format!("{reply:?}"))
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("the deposited result wasn't written: {records:?}"));
    assert!(
        held.contains("[checked by the harness hook]"),
        "the held result skipped its post-tool hook: {held}"
    );
}

/// A held call beside a call whose outcome is unknown: the run can't pause on the held call
/// (a late answer can't resume a run in doubt), so it closes the held call and ends interrupted
/// for review instead of waiting on it forever.
#[tokio::test]
async fn a_held_call_beside_an_unknown_outcome_ends_the_run_for_review() {
    let scenario = Scenario {
        input: vec![user("hold and write")],
        tools: vec![
            tool("hold", ExecutionMode::Parallel),
            tool("write", ExecutionMode::Parallel),
        ],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "hold", json!({"job": 1})),
                    call("c2", "write", json!({"path": "a"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "should not be reached"),
        ],
    };
    let ran = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        Box::pin(new_with_actions(&scenario, Vec::new(), None, false)),
    )
    .await
    .expect("the run waited forever on the held call");
    let records = &ran.source.records;
    let end = records
        .iter()
        .find_map(|record| match record {
            Record::RunEnd(end) => Some(end.reason),
            _ => None,
        })
        .unwrap_or_else(|| panic!("the run never ended: {records:?}"));
    assert_eq!(end, RunFinishReason::Interrupted);
    let held = records
        .iter()
        .filter_map(|record| match record {
            Record::ToolOutcome(outcome) => Some(format!("{outcome:?}")),
            _ => None,
        })
        .find(|outcome| outcome.contains("\"c1\""))
        .expect("the held call is closed");
    assert!(held.contains("provider_held_interrupted"), "{held}");
}

/// What a run of one step calling `tool_name` beside a `read`, then answering "done", left: its
/// outcome records, how it ended, and how many answers the model gave.
async fn ran_one_call(tool_name: &str) -> (Vec<String>, RunFinishReason, usize) {
    let scenario = Scenario {
        input: vec![user("call it")],
        tools: vec![
            tool(tool_name, ExecutionMode::Parallel),
            tool("read", ExecutionMode::Parallel),
        ],
        manifest: None,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", tool_name, json!({"x": 1})),
                    call("c2", "read", json!({"path": "a"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    };
    let ran = Box::pin(new_with_actions(&scenario, Vec::new(), None, false)).await;
    let records = &ran.source.records;
    let outcomes = records
        .iter()
        .filter_map(|record| match record {
            Record::ToolOutcome(outcome) => Some(format!("{outcome:?}")),
            _ => None,
        })
        .collect();
    let end = records
        .iter()
        .find_map(|record| match record {
            Record::RunEnd(end) => Some(end.reason),
            _ => None,
        })
        .expect("the run ends");
    let steps = records
        .iter()
        .filter(|record| matches!(record, Record::Assistant(_)))
        .count();
    (outcomes, end, steps)
}

/// A batch whose scope was gone when it started sends nothing: every call gets the error naming
/// why its route drained, and the run stops interrupted instead of asking the model again.
#[tokio::test]
async fn a_batch_whose_scope_was_gone_at_its_start_stops_the_run() {
    let (outcomes, end, steps) = ran_one_call("scope_gone").await;
    assert_eq!(end, RunFinishReason::Interrupted);
    assert_eq!(
        steps, 1,
        "the model was asked again after the drained batch"
    );
    assert_eq!(outcomes.len(), 2, "{outcomes:?}");
    // An outcome shows its result only by digest: the drained result is the error naming why
    // the route drained.
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.contains("is_error: true")
                && outcome.contains("reason: Some(\"scope_ended\")")),
        "{outcomes:?}"
    );
}

/// A call whose route drained before it was sent was definitely not run: the model gets the
/// error naming why, and the run goes on. Only a drain after the call was sent leaves its
/// outcome unknown and stops the run.
#[tokio::test]
async fn a_drained_call_is_unknown_only_when_it_was_sent() {
    let (outcomes, end, steps) = ran_one_call("drained").await;
    assert_eq!(end, RunFinishReason::Completed, "{outcomes:?}");
    assert_eq!(steps, 2);
    let unsent = outcomes
        .iter()
        .find(|outcome| outcome.contains("\"c1\""))
        .unwrap();
    assert!(
        unsent.contains("outcome: Completed")
            && unsent.contains("is_error: true")
            && unsent.contains("reason: Some(\"scope_ended\")"),
        "{unsent}"
    );

    let (outcomes, end, _) = ran_one_call("drained_sent").await;
    assert_eq!(end, RunFinishReason::Interrupted, "{outcomes:?}");
    let sent = outcomes
        .iter()
        .find(|outcome| outcome.contains("\"c1\""))
        .unwrap();
    assert!(
        sent.contains("outcome: Unknown") && sent.contains("reason: Some(\"scope_ended\")"),
        "{sent}"
    );
}

/// A crash after a `final_text` run's answer is durable and before its end: the resume ends the
/// run completed with the structured result the step would have written beside its end, not a
/// completed run with no result.
#[tokio::test]
async fn a_final_text_run_cut_before_its_end_keeps_its_result() {
    STRUCTURED
        .scope(Some(unconstrained("final_text")), async {
            let scenario = single(vec![text_step("a1", r#"{"answer":1}"#)]);
            let source = new_source(&scenario, None, scenario.steps.clone()).await;
            let cut = source
                .batch_ends
                .iter()
                .copied()
                .find(|&end| {
                    let kept = &source.records[..end];
                    kept.iter()
                        .any(|record| matches!(record, Record::StepEnd(_)))
                        && !kept.iter().any(|record| {
                            matches!(record, Record::RunEnd(_) | Record::StructuredResult(_))
                        })
                })
                .expect("a cut after the answer and before the run's end");
            let resumed = new_source(&scenario, Some(source.cut(cut)), Vec::new()).await;
            let result = resumed
                .records
                .iter()
                .find_map(|record| match record {
                    Record::StructuredResult(result) => Some(result.result.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("no structured result: {:?}", resumed.records));
            assert!(
                result.to_string().contains("answer"),
                "the result lost the answer: {result}"
            );
            assert!(
                resumed.outcome.requests.is_empty(),
                "the resume asked the model again"
            );
        })
        .await;
}

/// A reply that repeats a held call's id: the repeat's duplicate error must not be stored
/// before the held call's own result, or reading the session back would end the held call with it
/// and the provider's answer could no longer resume the run.
#[tokio::test]
async fn a_repeated_id_beside_a_held_call_waits_for_the_held_result() {
    let tools = vec![tool("hold", ExecutionMode::Parallel)];
    let scenario = Scenario {
        input: vec![user("hold twice")],
        manifest: None,
        tools,
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("h", "hold", json!({"job": 1})),
                    call("h", "hold", json!({"job": 2})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    };
    let ran = Box::pin(new_with_actions(&scenario, Vec::new(), None, false)).await;
    let records = &ran.source.records;
    // A reply carries the result itself; an outcome only its digest.
    let outcomes: Vec<String> = records
        .iter()
        .filter_map(|record| match record {
            Record::ToolReply(reply) => Some(format!("{reply:?}")),
            _ => None,
        })
        .collect();
    assert_eq!(outcomes.len(), 2, "{outcomes:?}");
    assert!(
        !outcomes[0].contains("duplicate tool_call_id"),
        "the duplicate error came before the held call's result: {outcomes:?}"
    );
    assert!(
        outcomes[1].contains("duplicate tool_call_id"),
        "{outcomes:?}"
    );
    let end = records
        .iter()
        .find_map(|record| match record {
            Record::RunEnd(end) => Some(end.reason),
            _ => None,
        })
        .expect("the run ends");
    assert_eq!(end, RunFinishReason::Completed);
}
