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
use broca_wal::WalRecord;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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
        }
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

    fn output_limits(&self) -> Option<broca_core::output_reservation::OutputLimits> {
        output().map(|script| script.limits)
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
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for event in script {
                let _ = tx.send(event).await;
            }
        });
        rx
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
        self.note(format!(
            "batch_started_guarded {:?}{}",
            ids(calls),
            batch_scope(ctx)
        ));
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
        // A call slow enough that keep-warm sends its warms while it runs.
        if call.tool_name == "slow" {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
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
        // A structured result, which no ride rides on off the compaction contract.
        if call.tool_name == "json" {
            return DispatchResult::Completed(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Json {
                    value: json!({"rows": 2}),
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
async fn media_reads_back(session: &Session<&MemoryBackend>) {
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
    /// Whether the multi-run comparison seals the new loop's ledger after each run but the last.
    static LEDGER_SEAL_BETWEEN_RUNS: bool;
}

tokio::task_local! {
    /// When set, the new side ends the cut log's run with this seal instead of resuming it.
    static SEALING: broca_core::resume::SealCause;
}

tokio::task_local! {
    /// The compaction provider a comparison's plan names, scripted: what it answers each ride
    /// and the window the run freezes. `None` runs without one.
    static COMPACTION: Option<Arc<CompactionScript>>;
}

tokio::task_local! {
    /// The output cap the provider's requests carry and the context limit the run freezes, for
    /// a comparison of how a step's cap is lowered. `None` runs with neither.
    static OUTPUT: Option<OutputScript>;
}

#[derive(Clone, Copy)]
struct OutputScript {
    limits: broca_core::output_reservation::OutputLimits,
    context_limit: u64,
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
    /// comparison checks each one reaches the start as today's loop records it.
    static SEND_FACTS: bool;
}

fn send_facts() -> bool {
    SEND_FACTS.try_with(|set| *set).unwrap_or(false)
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

/// Each run of today's log `today` gets the same spend facts from today's builder
/// (`compute_segments_for_run`) as the same run of `session` does, its records rebuilt in
/// today's form as the store actor's billing rebuilds them. The times each loop took from its
/// own clock, and the fact ids made from them, are set aside.
async fn spend_alike(today: &LogImage, session: &Session<&MemoryBackend>, parts: &MemoryParts) {
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
    let framed = today.framed();
    let mut runs: Vec<(RunId, broca_protocol::BindIdentity)> = Vec::new();
    for frame in &framed {
        if let WalRecord::RunStarted {
            run_id, session, ..
        } = &frame.record
        {
            if !runs.iter().any(|(known, _)| known == run_id) {
                runs.push((run_id.clone(), session.clone()));
            }
        }
    }
    assert!(!runs.is_empty(), "today's log started no run");
    let records = session.open_log_records().await.unwrap();
    for (run_id, identity) in runs {
        let old = broca_core::billing::compute_segments_for_run(
            &framed,
            &run_id,
            "differential",
            &UnknownCharge,
        );
        let rebuilt =
            broca_session_loop::billing::today_records(&records, &run_id.0, &identity, parts)
                .expect("the run's records rebuild");
        let new = broca_core::billing::compute_segments_for_run(
            &rebuilt,
            &run_id,
            "differential",
            &UnknownCharge,
        );
        assert!(!old.is_empty(), "today bills run {}", run_id.0);
        assert_eq!(
            timeless(new),
            timeless(old),
            "run {}'s spend facts",
            run_id.0
        );
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

/// On the compaction contract, every compaction entry the new session serves equals the one
/// today's storage serves for the same history (`compaction_v2::transcript::project`): a
/// carrying tool result with its own output and each ride beside it, and each delivered
/// message as it was placed. Message ids, call keys and other ids each storage mints its own
/// way are compared by order of first appearance; times by whether they are there.
async fn entries_alike(today: &LogImage, session: &Session<&MemoryBackend>) {
    // Only a log on the compaction contract serves compaction entries.
    if LAYOUT.try_with(|layout| layout.is_none()).unwrap_or(true) {
        return;
    }
    let framed = today.framed();
    assert!(
        broca_core::is_mode3_log(framed.iter().map(|frame| &frame.record)),
        "today's log is on the compaction contract"
    );
    let lineage = lineage(today).expect("today's log on the contract has a lineage");
    let old = broca_core::compaction_v2::transcript::project(&framed, &lineage)
        .expect("today's entries project")
        .entries;
    let new: Vec<_> = session
        .entries(1, usize::MAX)
        .await
        .expect("the new session's entries read")
        .iter()
        .map(|entry| broca_session_loop::entries::project(entry).expect("an entry projects"))
        .collect();
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
    assert_eq!(masked(&new), masked(&old), "the compaction entries differ");
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
/// times recorded.
fn harness_now() -> u64 {
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
    today: Arc<WarmScript>,
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
        Ok(broca_core::keep_warm::WarmCosts {
            warm: 1.0,
            bust: 3.5,
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

/// The script as today's loop calls it: with the run's `RunConfig`.
struct TodayWarmer(Arc<WarmScript>);

#[async_trait::async_trait]
impl broca_core::keep_warm_wait::KeepWarmDriver for TodayWarmer {
    fn timing(
        &self,
        _: &broca_wal::RunConfig,
    ) -> Result<broca_core::keep_warm_wait::WarmTiming, String> {
        self.0.timing()
    }
    fn last_sent_at_ms(&self) -> Option<u64> {
        Some(0)
    }
    fn note_attempt(&self, attempt: &broca_protocol::ModelAttempt) {
        self.0.note(attempt);
    }
    fn costs(&self, _: &broca_wal::RunConfig) -> Result<broca_core::keep_warm::WarmCosts, String> {
        self.0.costs()
    }
    async fn warm(
        &self,
        config: &broca_wal::RunConfig,
        run_id: &RunId,
    ) -> broca_core::keep_warm_wait::WarmTick {
        self.0.warm(run_id, &config.model)
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
}

#[derive(Clone)]
struct AccountScript {
    selection: broca_wal::AuthSelection,
    wait: broca_core::SameAccountWait,
    /// Whether a pause comparison waits for the account to reset before resuming.
    wait_for_reset: bool,
}

fn account() -> Option<AccountScript> {
    ACCOUNT.try_with(Clone::clone).ok().flatten()
}

fn account_selection() -> Option<broca_wal::AuthSelection> {
    account().map(|script| script.selection)
}

fn routed(account: &str) -> broca_wal::AuthSelection {
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
        let Some(limit) = &pause.account_limit else {
            continue;
        };
        let seq = limit["failed_attempt_seq"]
            .as_u64()
            .expect("a pause names its attempt");
        let named = &writer.records[(seq - first_seq) as usize];
        assert!(
            matches!(named, Record::Attempt(attempt)
            if attempt.billing.as_ref().is_some_and(|billing| {
                !billing["account_limit_disposition"].is_null()
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
    today: Arc<Mutex<Vec<String>>>,
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

struct TodayReports(Arc<Mutex<Vec<String>>>);

/// Today's loop's account reporter, when the comparison has one.
fn today_reporter() -> Option<Arc<dyn broca_core::AccountReporter>> {
    reports().map(|reports| {
        Arc::new(TodayReports(Arc::clone(&reports.today))) as Arc<dyn broca_core::AccountReporter>
    })
}

impl broca_core::AccountReporter for TodayReports {
    fn run_started(&self, _: Option<broca_wal::WalLineageId>, _: u64) {
        self.0.lock().unwrap().push("run started".into());
    }
    fn attempt_recorded(
        &self,
        _: Option<broca_wal::WalLineageId>,
        _: u64,
        attempt: &broca_protocol::ModelAttempt,
    ) {
        self.0.lock().unwrap().push(reported_attempt(attempt));
    }
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
    let today = pair.today.lock().unwrap().clone();
    let new = pair.new.lock().unwrap().clone();
    assert!(today.len() >= at_least, "today's reports: {today:?}");
    assert_eq!(new, today, "the new loop's reports");
}

/// Today's loop reads the cache lifetime a run's requests ask for from its keep-warm driver,
/// which production builds from the provider's cache declaration. This one only answers that,
/// as production's does, and never warms (no run in these comparisons asks to).
struct CacheLifetime;

#[async_trait::async_trait]
impl broca_core::keep_warm_wait::KeepWarmDriver for CacheLifetime {
    fn timing(
        &self,
        _: &broca_wal::RunConfig,
    ) -> Result<broca_core::keep_warm_wait::WarmTiming, String> {
        Err("not warming in this comparison".into())
    }
    fn requested_ttl_ms(&self, config: &broca_wal::RunConfig) -> Option<u64> {
        let frozen = serde_json::from_value(config.frozen_render_config.clone()).ok()?;
        broca_provider::cache_warm::requested_ttl_ms(&config.model.provider_module_id, &frozen)
    }
    fn last_sent_at_ms(&self) -> Option<u64> {
        None
    }
    fn costs(&self, _: &broca_wal::RunConfig) -> Result<broca_core::keep_warm::WarmCosts, String> {
        Err("not warming in this comparison".into())
    }
    async fn warm(
        &self,
        _: &broca_wal::RunConfig,
        _: &RunId,
    ) -> broca_core::keep_warm_wait::WarmTick {
        broca_core::keep_warm_wait::WarmTick::Stop {
            reason: "unsupported".into(),
            detail: None,
        }
    }
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

/// The output script the comparison running in this task uses.
fn output() -> Option<OutputScript> {
    OUTPUT.try_with(|script| *script).ok().flatten()
}

/// The context limit both loops freeze: the compaction script's, else the output script's.
fn frozen_context_limit() -> Option<u64> {
    compaction()
        .map(|script| script.context_limit)
        .or(output().map(|script| script.context_limit))
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
    /// A view that cuts through the entry at the first ordinal and drops the whole entry at the
    /// second.
    CutAndDrop(u64, u64),
}

/// A compaction provider that answers from a script and records every call, so both loops must
/// make the same calls. Message ids differ between the loops by design, so the provider learns
/// them from the entries it's told about, and a view names entries by those ids.
struct CompactionSpy {
    rides: Mutex<VecDeque<Ride>>,
    mids: Mutex<std::collections::BTreeMap<u64, String>>,
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
        Self {
            rides: Mutex::new(script.rides.iter().copied().collect()),
            mids: Mutex::default(),
            version: Default::default(),
            calls: Mutex::default(),
            lost: Mutex::new(LOST.try_with(|lost| *lost).ok()),
            notices: Default::default(),
            rides_asked: Default::default(),
        }
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
        scope: Option<&broca_protocol::SessionScope>,
        method: &str,
        params: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        entry_mids(&params, &mut self.mids.lock().unwrap());
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
                None => json!({"answer": "nothing"}),
                Some(ride) => {
                    use cortexkit_role_compaction::view::{Edit, EditOp, View};
                    let mid = |ordinal: u64| self.mids.lock().unwrap()[&ordinal].clone();
                    let version = 1 + self
                        .version
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let ops = match ride {
                        Ride::Drop(ordinal) => vec![EditOp::Drop {
                            mid: mid(ordinal),
                            part: None,
                        }],
                        Ride::Cut(ordinal) => vec![EditOp::Cut {
                            through: mid(ordinal),
                        }],
                        Ride::CutAndDrop(cut, drop) => vec![
                            EditOp::Cut { through: mid(cut) },
                            EditOp::Drop {
                                mid: mid(drop),
                                part: None,
                            },
                        ],
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
            assert_eq!(
                client.conversation().encode(),
                recording.session.conversation.encode(),
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
        let mut request = self.plain_request_for(run_id, input);
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
        RunRequest {
            route_decision_id: None,
            account_route: None,
            input_written_at_ms: None,
            directives: None,
            session_class: None,
            retention: None,
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
            tools: self.tools.clone(),
            policy_allowed: self.tools.iter().map(|t| t.name.clone()).collect(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
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
    // A real provider resumed after a downgrade is rebuilt from the downgraded frozen config.
    if start.is_some_and(|(_, bytes)| {
        bytes
            .windows(b"structured_output_downgraded".len())
            .any(|window| window == b"structured_output_downgraded")
    }) {
        provider
            .downgraded
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    if start.is_some() {
        provider.resuming();
    }
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
            auth_selection: account_selection(),
            account_limit_pause: account().is_some(),
            max_same_account_wait: account().map_or_else(Default::default, |script| script.wait),
            provider_held_lifecycle: Some(plane.lifecycle()),
            manifest: scenario.manifest.clone(),
            cancel: Some(cancel.clone()),
            hook_plane: Some(harness_hooks_cancelling(cancel)),
            new_log_compaction_plan_version: layout(),
            compaction_plane: spy
                .clone()
                .map(|spy| spy as Arc<dyn broca_core::CompactionPlane>),
            admitted_context_limit: frozen_context_limit(),
            account_reporter: reports().map(|reports| {
                Arc::new(TodayReports(Arc::clone(&reports.today)))
                    as Arc<dyn broca_core::AccountReporter>
            }),
            keep_warm: match warm_script() {
                Some(script) => Some(Arc::new(TodayWarmer(Arc::clone(&script.today)))
                    as Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver>),
                None => cache().then(|| {
                    Arc::new(CacheLifetime) as Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver>
                }),
            },
            ..admission_control(scenario)
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
    let converted =
        convert_unbilled(&image.framed(), lineage(&image).as_deref()).expect("the log converts");
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
    session: &'s mut Session<&'b MemoryBackend>,
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
    // A session converted from today's log off the compaction contract keeps text delivered
    // before ride envelopes existed joined into its results, as the converter sets it up.
    let converted_off_contract = layout().is_none()
        && start.as_ref().is_some_and(|converted| {
            converted
                .records
                .iter()
                .any(|r| matches!(r, Record::RunStart(_)))
        });
    let mut session = Session::create(
        &backend,
        SessionAddr("0123456789abcdef".into()),
        triple,
        &DeploymentKey::new(1, &[7; 32]),
        converted_off_contract,
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
        injected: Default::default(),
        ended: Default::default(),
        media: Default::default(),
    };
    let now = harness_now;
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
        model_call_ready: None,
    };
    let sealing = SEALING.try_with(|cause| *cause).ok();
    through!(
        writer,
        |w| if let (true, Some(cause)) = (resuming, sealing) {
            seal(w, &parts, &seams, cause).await
        } else if resuming {
            resume(w, &parts, &seams).await
        } else {
            run(w, &mut parts, scenario.admission(&provider), &seams).await
        }
    )
    .expect("the new loop runs");
    let records = writer.records.clone();
    keys_match(writer.session, &plane).await;
    ids_resolve(writer.session).await;
    turns_have_facts(writer.session).await;
    outcome_keys_name_their_place(&writer);
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

/// Today's log converted, without the `Billed` record the converter writes after each run's
/// end. That record settles the run's usage export, which the store actor writes after a run
/// ends; the run loop never writes one, so the two loops' records are compared without it.
fn convert_unbilled(
    frames: &[broca_wal::FramedRecord],
    lineage: Option<&str>,
) -> Result<broca_session_convert::Converted, broca_session_convert::ConvertError> {
    broca_session_convert::convert(frames, lineage).map(|mut converted| {
        converted
            .records
            .retain(|record| !matches!(record, Record::Billed(_)));
        converted
    })
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

/// `new`, with the one difference allowed between a resume of each loop. When today's loop
/// resumes a tool turn the refusal-fallback model answered, it commits the turn's message
/// under the run's own model (`complete_pending_turn` stamps `cfg.model`), although its live
/// path and its replay both name the fallback model. The new loop names the fallback model on
/// both paths. Only a message where the new loop names the fallback and today names the run's
/// model is aligned; every other difference still fails.
fn resumed_as_today(new: &CallOptions, today: &CallOptions) -> CallOptions {
    let mut new = new.clone();
    let Some(script) = fallback() else {
        return new;
    };
    let run_model = ModelHandle::new(provider_id(), "m");
    for (new, today) in new.prompt.iter_mut().zip(today.prompt.iter()) {
        if new.origin.as_ref() == Some(&script.model) && today.origin.as_ref() == Some(&run_model) {
            new.origin = today.origin.clone();
        }
    }
    new
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
    with_each_writer(|| Box::pin(compare_every_cut_in_once(scenario.clone()))).await;
}

async fn compare_every_cut_in_once(scenario: Scenario) {
    // The whole log the cuts are taken from. Only the resumes are compared, so its run's warms
    // and account reports go to a driver and reporter of their own; the run still freezes the
    // keep-warm its resumes read back.
    let source_warms = warm_script().map(|_| Arc::new(WarmPair::default()));
    let source_reports = reports().map(|_| Arc::new(ReportPair::default()));
    let (_, full, name) = WARM
        .scope(
            source_warms,
            REPORTS.scope(
                source_reports,
                today_from(&scenario, None, scenario.steps.clone()),
            ),
        )
        .await;
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
        let converted =
            convert_unbilled(&frames, lineage(&cut_image).as_deref()).expect("the cut converts");
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
            let new = resumed_as_today(new, today);
            assert_eq!(&new, today, "{context}: request {index}");
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

/// At every cut of `scenario` where the run was still running (no pause, no end) and today's
/// replay would resume it or stop it for review, the new loop's seal writes the same records as
/// today's seal plan (`broca_core::resume::interrupted_seal`, a crash-orphan seal as today's
/// actor writes it on open) appended to today's cut log and converted. Asserts some cut seals.
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
    use broca_core::resume::{crash_orphan_error, interrupted_seal, DeferredCloseKind, SealCause};
    let (_, full, _) = today_from(&scenario, None, scenario.steps.clone()).await;
    let records = full.framed().len();
    let mut sealed = 0;
    for cut in 1..records {
        let bytes = broca_crash_harness::cut_log(&full, cut, false);
        let image = LogImage::parse(bytes);
        let frames = image.framed();
        let wal: Vec<WalRecord> = frames.iter().map(|frame| frame.record.clone()).collect();
        let prior = broca_core::replay(&wal).expect("the cut replays");
        let running = prior.run_id.is_some()
            && !prior.run_finished_durable
            && matches!(
                prior.resolution,
                broca_core::ReplayResolution::Resume { .. }
                    | broca_core::ReplayResolution::Interrupted { .. }
            );
        if !running {
            continue;
        }
        sealed += 1;
        let lineage = lineage(&image);
        let converted = convert_unbilled(&frames, lineage.as_deref()).expect("the cut converts");
        let before = converted.records.len();
        // Today: the seal plan over the cut, appended as today's actor appends it.
        let head = frames.last().map_or(0, |frame| frame.seq);
        let plan = interrupted_seal(
            &prior,
            SealCause::CrashOrphan,
            None,
            false,
            Some(head),
            Some(crash_orphan_error()),
            |_| DeferredCloseKind::NotDispatched,
        );
        let today_frames = through_todays_writer(
            &image.bytes,
            plan.resume_started
                .into_iter()
                .chain(plan.close_out)
                .chain(std::iter::once(plan.terminal.record(Some(harness_now()))))
                .collect(),
            &wal,
        )
        .await;
        let today = convert_unbilled(&today_frames, lineage.as_deref())
            .expect("today's sealed log converts");
        let today_sealed = normalized(&today.records)[before..].to_vec();
        // New: the loop's seal over the converted cut.
        let new = SEALING
            .scope(
                SealCause::CrashOrphan,
                new_from(&scenario, Some(converted), Vec::new()),
            )
            .await;
        let context = format!("seal after record {cut} of {records}");
        if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
            for (index, record) in today_sealed.iter().enumerate() {
                eprintln!("{context} today {index}: {record}");
            }
            for (index, record) in new.records.iter().enumerate() {
                eprintln!("{context} new   {index}: {record}");
            }
        }
        for (index, (new, today)) in new.records.iter().zip(&today_sealed).enumerate() {
            assert_eq!(new, today, "{context}: record {index}");
        }
        assert_eq!(
            new.records.len(),
            today_sealed.len(),
            "{context}: record count"
        );
        assert!(new.requests.is_empty(), "{context}: a seal sends nothing");
        assert!(
            new.calls.iter().all(|call| !call.contains("dispatch")),
            "{context}: a seal dispatches nothing: {:?}",
            new.calls
        );
    }
    assert!(sealed > 0, "some cut leaves a running run to seal");
}

/// The log `bytes` holds with `records` appended through today's real WAL writer, which stamps
/// what a record leaves for it to stamp (a tool result's finish time), read back as frames.
async fn through_todays_writer(
    bytes: &[u8],
    records: Vec<WalRecord>,
    held: &[WalRecord],
) -> Vec<broca_wal::FramedRecord> {
    use broca_wal::{FileWal, Wal as _};
    let root = broca_test_support::scratch_root().join(format!(
        "seal-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("session.wal");
    std::fs::write(&path, bytes).unwrap();
    let floor = root.join("wal-feature-floor");
    let writer = broca_core::WalHandle::spawn(
        Box::new(FileWal::open(&path).unwrap().with_feature_floor(&floor)),
        2,
    )
    .unwrap();
    writer.seed_lineage_tags(Some(broca_core::tags::lineage_tags(held)));
    for record in records {
        writer.record(record).into_result().unwrap();
    }
    writer.sync().await.unwrap();
    writer.shutdown().await.unwrap();
    let frames = FileWal::open(&path).unwrap().replay().unwrap();
    let _ = std::fs::remove_dir_all(&root);
    frames
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
        auth_selection: account_selection(),
        account_limit_pause: account().is_some(),
        max_same_account_wait: account().map_or_else(Default::default, |script| script.wait),
        manifest: scenario.manifest.clone(),
        compaction_session: Some(Arc::clone(&compaction_session)),
        keep_warm: warm_script().map(|script| {
            Arc::new(TodayWarmer(Arc::clone(&script.today)))
                as Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver>
        }),
        account_reporter: reports().map(|reports| {
            Arc::new(TodayReports(Arc::clone(&reports.today)))
                as Arc<dyn broca_core::AccountReporter>
        }),
        new_log_compaction_plan_version: layout(),
        compaction_plane: spy
            .clone()
            .map(|spy| spy as Arc<dyn broca_core::CompactionPlane>),
        admitted_context_limit: frozen_context_limit(),
        ..admission_control(&scenario)
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
        convert_unbilled(&image.framed(), lineage(&image).as_deref())
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
    let now = harness_now;
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
        compaction_changes: false,
        keep_warm: new_warmer(),
        account_reports: reports().map(|reports| {
            Arc::new(NewReports(Arc::clone(&reports.new))) as Arc<dyn AccountReports>
        }),
        max_same_account_wait: account().map_or_else(Default::default, |script| script.wait),
        auth_selection: account_selection(),
        step_progress: None,
        model_call_ready: None,
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
            .any(|ride| matches!(ride, Ride::Cut(_) | Ride::CutAndDrop(..)))
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
        records: Vec<WalRecord>,
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

/// A send queued with the delivery order `order`, in today's form. `supersedes` names the
/// undelivered send under the same dedupe key that it replaces, which ends first.
fn ordered_with(id: &str, order: Value, text: &str, supersedes: Option<&str>) -> Action {
    let input = vec![user(text)];
    let send = broca_core::delivery_order::OrderedSend {
        admitted_at_ms: Some(HARNESS_TIME_MS),
        submission_id: broca_protocol::SubmissionId(id.into()),
        order: serde_json::from_value(order.clone()).unwrap(),
        principal: None,
        input: input.clone(),
    };
    let mut records: Vec<WalRecord> = supersedes
        .map(|old| {
            broca_core::delivery_order::ended(
                &broca_protocol::SubmissionId(old.into()),
                broca_wal::record::SendEndReason::Superseded {
                    by: broca_protocol::SubmissionId(id.into()),
                },
            )
        })
        .into_iter()
        .collect();
    records.extend([
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
            "written_at_ms": HARNESS_TIME_MS,
        }))
        .unwrap(),
    ]);
    Action::Ordered {
        send: Box::new(send),
        records,
    }
}

/// Where a scripted provider sends what the session actor does while a step is answered.
#[async_trait]
trait SteerSink: Send + Sync {
    /// Writes a `steered` record as the actor would.
    async fn steer(&self, record: WalRecord);
    /// Records the caller's cancel at the run's next step boundary, as the actor would.
    async fn cancel_at_boundary(&self);
    /// Cancels the run now, as the actor does for an interrupting send: no record, only the
    /// run's cancel token.
    fn cancel_now(&self);
    /// Asks the run to stop, as the runner does when it shuts down.
    fn stop(&self);
    /// Queues an ordered send into the running run, as the actor does.
    async fn ordered(&self, send: broca_core::delivery_order::OrderedSend, records: Vec<WalRecord>);
}

/// Today's actor: the record is written under the run's steer inbox lock and synced.
struct TodaySink {
    inbox: Arc<SteerInbox>,
    writer: Arc<WalHandle>,
    stop: CancellationToken,
    cancel: CancellationToken,
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

    fn cancel_now(&self) {
        self.cancel.cancel();
    }

    fn stop(&self) {
        self.stop.cancel();
    }

    async fn ordered(
        &self,
        send: broca_core::delivery_order::OrderedSend,
        records: Vec<WalRecord>,
    ) {
        let writer = Arc::clone(&self.writer);
        let accepted = self.inbox.accept_ordered(send, move |superseded| {
            let expected = match records.first() {
                Some(WalRecord::SendEnded { submission_id, .. }) => Some(submission_id),
                _ => None,
            };
            assert_eq!(
                superseded, expected,
                "the send replaces what the scenario says"
            );
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
    cancel: CancellationToken,
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
                media: Vec::new(),
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

    fn cancel_now(&self) {
        self.cancel.cancel();
    }

    fn stop(&self) {
        self.stop.cancel();
    }

    async fn ordered(
        &self,
        _send: broca_core::delivery_order::OrderedSend,
        records: Vec<WalRecord>,
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
        let converted = convert_unbilled(&framed, None).expect("the send's records convert");
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
    fn turn_system_possible(&self, preceding: Option<&Message>, followed_by_user: bool) -> bool {
        self.inner.turn_system_possible(preceding, followed_by_user)
    }

    /// A provider whose calls are cancelled mid-stream reports its attempts as a transport
    /// does, so both loops read a cancelled call's stream for its attempt in flight.
    fn reports_attempt_evidence(&self) -> bool {
        self.reports_attempts
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

fn steered(call: usize, index: usize, messages: Vec<Message>) -> WalRecord {
    WalRecord::Steered {
        sent_by: None,
        written_at_ms: Some(HARNESS_TIME_MS),
        retention: None,
        ts_ms: Some(HARNESS_TIME_MS),
        send_id: Some(format!("steer-{call}-{index}")),
        request_identity_digest: None,
        delivery: Some(broca_protocol::Delivery::Steer),
        title: None,
        input: Arc::new(messages),
        hooks: None,
        tags: None,
        message_marks: None,
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

/// The summary each of a comparison's runs reported: how it ended, and why it paused.
type Summaries = Vec<(broca_protocol::caller::RunFinishReason, Option<PauseReason>)>;

tokio::task_local! {
    /// Whether today's loop writes a ride's envelope (the carrying call's key, the sender and
    /// the send's forms), as production has since llm-runner 0.8.11. A comparison of rides
    /// recorded without one turns it off.
    static RIDES_WRITER: bool;
}

fn rides_writer() -> bool {
    RIDES_WRITER.try_with(|on| *on).unwrap_or(true)
}

/// Today's loop running `scenario` while the session actor does `steers`, on a writer this test
/// owns so its actor side can write steers and queue sends. With `start`, it resumes that log,
/// as today's actor resumes one: the run may deliver every ordered send the log holds queued.
/// Returns what it did, how each run reported, and its whole log with the log's file name.
async fn today_with_actions(
    scenario: &Scenario,
    start: Option<(&str, &[u8])>,
    steps: Vec<Vec<NormalizedEvent>>,
    steers: Vec<(usize, Action)>,
    on_restart: Option<OnRestart>,
) -> (Outcome, Summaries, LogImage, String) {
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
    let wal = FileWalStore::new(dir.join("wal"));
    let mut request = scenario.request();
    request.on_restart = on_restart;
    let writer = Arc::new(WalHandle::spawn(wal.open(&request.session).unwrap(), 1).unwrap());
    if rides_writer() {
        writer.enable_ride_writer_for_test();
    }
    let inbox = SteerInbox::new();
    if let Some((_, bytes)) = start {
        // Today's actor seeds a resumed run with the sends still queued: their ride facts, and
        // the ordered ones for the run's boundaries.
        let records: Vec<WalRecord> = LogImage::parse(bytes.to_vec())
            .framed()
            .into_iter()
            .map(|framed| framed.record)
            .collect();
        let pending = broca_core::queue::pending_submissions(&records);
        for queued in &pending {
            writer.note_queued_ride(queued);
        }
        inbox.seed_ordered(
            pending
                .iter()
                .filter_map(broca_core::delivery_order::OrderedSend::from_pending)
                .collect(),
        );
    }
    let stop = CancellationToken::new();
    let cancel = CancellationToken::new();
    let scripted = ScriptedProvider::new(steps);
    if start.is_some() {
        scripted.resuming();
    }
    let provider = SteeringProvider {
        inner: scripted,
        calls: Default::default(),
        reports_attempts: cancels_mid_stream(&steers),
        steers: Mutex::new(steers.clone()),
        sink: Arc::new(TodaySink {
            inbox: Arc::clone(&inbox),
            writer: Arc::clone(&writer),
            stop: stop.clone(),
            cancel: cancel.clone(),
        }),
    };
    let plane = scenario.plane();
    let (tx, mut rx) = mpsc::channel(512);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
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
        auth_selection: account_selection(),
        account_limit_pause: account().is_some(),
        max_same_account_wait: account().map_or_else(Default::default, |script| script.wait),
        steer: Some(Arc::clone(&inbox)),
        stop_requested: Some(stop),
        provider_held_lifecycle: Some(plane.lifecycle()),
        cancel: Some(cancel),
        new_log_compaction_plan_version: layout(),
        account_reporter: today_reporter(),
        ..admission_control(scenario)
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
                auth_selection: account_selection(),
                account_limit_pause: account().is_some(),
                max_same_account_wait: account()
                    .map_or_else(Default::default, |script| script.wait),
                provider_held_lifecycle: Some(plane.lifecycle()),
                new_log_compaction_plan_version: layout(),
                account_reporter: today_reporter(),
                ..admission_control(scenario)
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
    let image = LogImage::read(&file);
    let name = file.file_name().unwrap().to_string_lossy().into_owned();
    let converted =
        convert_unbilled(&image.framed(), lineage(&image).as_deref()).expect("the log converts");
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
    (today, summaries, image, name)
}

async fn compare_actions_in_once(
    scenario: Scenario,
    steers: Vec<(usize, Action)>,
    on_restart: Option<OnRestart>,
) {
    // Today's run is boxed: its future is large enough to overflow a test thread's stack where
    // it is built.
    let (today, summaries, full, _) = Box::pin(today_with_actions(
        &scenario,
        None,
        scenario.steps.clone(),
        steers.clone(),
        on_restart,
    ))
    .await;

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
        injected,
        ended: Default::default(),
        media: Default::default(),
    };
    let mut parts = MemoryParts::default();
    let now = harness_now;
    let bind = bind();
    let seams = Seams {
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
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
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
            stop: Default::default(),
            ..seams
        };
        let second = through!(writer, |w| finishes(resume(w, &parts, &restarted)).await)
            .expect("the new loop resumes");
        reports.push((second.reason, second.paused));
    }
    assert_eq!(reports, summaries, "how each run reported");
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
    assert_same(&today, &new);
    if entries_compared() {
        entries_alike(&full, writer.session).await;
    }
    if spend_compared() {
        spend_alike(&full, writer.session, &parts).await;
    }
    rides_name_their_carrier(&writer.records);
    // Reloading the session from a checkpoint at any record gives the state folding every
    // record does, rides kept beside their calls included.
    let points: Vec<usize> = (0..=writer.records.len()).collect();
    let check = broca_session_convert::split::check_splits(&writer.records, false, &points)
        .expect("the session's records fold");
    assert!(
        check.differing.is_empty(),
        "a checkpoint at {:?} reloads another state than the whole fold",
        check.differing
    );
}

/// Cuts today's log of `scenario`, run while the actor does `steers`, after each of its records,
/// as a crash could, and resumes the cut in both loops, as `compare_every_cut` does: today's loop
/// on the cut log, with the sends it holds queued, and the new loop on the same records
/// converted. Nothing the actor did after the cut happens again. Runs in both layouts and both
/// writer modes.
async fn compare_actions_every_cut(scenario: Scenario, steers: Vec<(usize, Action)>) {
    compare_actions_cut(scenario, steers, Cuts::default()).await;
}

/// Where a cut comparison cuts, and what the resumes run on.
#[derive(Clone, Default)]
struct Cuts {
    /// Cut only after the first record of this type: the loops write differently before it.
    after: Option<&'static str>,
    /// The route the resumes judge and render turn-scoped system messages by, when it isn't the
    /// one the run delivered under, as after a switch to another model.
    resume_route: Option<Option<broca_provider::wire::FrozenRenderConfig>>,
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
    let (_, _, full, name) = Box::pin(today_with_actions(
        &scenario,
        None,
        scenario.steps.clone(),
        steers,
        None,
    ))
    .await;
    let records = full.framed().len();
    assert!(records > 2, "the scenario wrote a log to cut");
    let first = match cuts.after {
        Some(kind) => {
            full.framed()
                .iter()
                .position(|frame| serde_json::to_value(&frame.record).unwrap()["type"] == kind)
                .unwrap_or_else(|| panic!("the scenario wrote a {kind} record"))
                + 1
        }
        None => 1,
    };
    let route = cuts.resume_route.clone().unwrap_or_else(turn_system_route);
    for cut in first..records {
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
        let envelope_lost = loses_ride_envelopes(&frames);
        let converted =
            convert_unbilled(&frames, lineage(&cut_image).as_deref()).expect("the cut converts");
        let cut_records = converted.records.clone();
        let before = converted.records.len();
        let (today, _, _, _) = TURN_SYSTEM
            .scope(
                route.clone(),
                Box::pin(today_with_actions(
                    &scenario,
                    Some((&name, &bytes)),
                    steps.clone(),
                    Vec::new(),
                    None,
                )),
            )
            .await;
        let new = TURN_SYSTEM
            .scope(
                route.clone(),
                Box::pin(new_from(&scenario, Some(converted), steps)),
            )
            .await;
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
            let new = resumed_as_today(new, today);
            assert_eq!(&new, today, "{context}: request {index}");
        }
        // Where today's resumed loop loses a ride's envelope it has no notice for the ride, so
        // nothing restates the carrying message's facts after it: the new loop's restatement
        // right after each ride is set aside.
        let new_records: Vec<Value> = new
            .records
            .iter()
            .enumerate()
            .filter(|(index, record)| {
                !(envelope_lost
                    && record["EntryFacts"].is_object()
                    && index.checked_sub(1).is_some_and(|before| {
                        new.records[before]["ResultAppend"]["ride"].is_object()
                    }))
            })
            .map(|(_, record)| record.clone())
            .collect();
        for (index, (new, today)) in new_records.iter().zip(today_appended).enumerate() {
            if envelope_lost && today["ResultAppend"]["ride"].is_null() {
                // Without its envelope today's ride is a delivery of the old kind, which names
                // the results message as delivered on its call; the new loop's ride, with its
                // envelope, names nothing on the message.
                let without = |record: &Value| {
                    let mut record = record.clone();
                    if let Some(append) = record["ResultAppend"].as_object_mut() {
                        append.remove("ride");
                        append.remove("send_id");
                    }
                    if let Some(facts) = record["EntryFacts"].as_object_mut() {
                        if facts["delivery_mode"] == "tool_result" {
                            facts.insert("delivery_mode".into(), Value::Null);
                            facts.insert("delivery_tool_call_id".into(), Value::Null);
                        }
                    }
                    record
                };
                assert_eq!(without(new), without(today), "{context}: record {index}");
                continue;
            }
            assert_eq!(new, today, "{context}: record {index}");
        }
        assert_eq!(
            new_records.len(),
            today_appended.len(),
            "{context}: record count"
        );
        let expected_calls = if resumes_an_answered_batch(&cut_records) {
            without_empty_resumed_batch(&today.calls)
        } else {
            today.calls.clone()
        };
        assert_eq!(new.calls, expected_calls, "{context}: tool calls");
        // On a route that can't take a turn-scoped system message, the provider renders a stored
        // one as user-authority text, never dropping it.
        if let Some(route) = route
            .as_ref()
            .filter(|route| !broca_provider::turn_system::supported(route))
        {
            for request in &new.requests {
                let rendered = broca_provider::turn_system::prepare(route, request);
                assert!(
                    !rendered
                        .prompt
                        .iter()
                        .any(|message| message.role == Role::System && message.turn_system),
                    "{context}: a turn-scoped system message goes out as one"
                );
                let stored = request
                    .prompt
                    .iter()
                    .filter(|message| message.turn_system)
                    .count();
                let notes = rendered
                    .prompt
                    .iter()
                    .filter(|message| {
                        message.role == Role::User
                            && message.content.iter().any(|block| {
                                matches!(block,
                                ContentBlock::Text { text } if text.starts_with("<context-note>"))
                            })
                    })
                    .count();
                assert_eq!(
                    notes, stored,
                    "{context}: each stored entry goes out as a note"
                );
            }
        }
    }
}

/// Whether today's loop resumes `frames` without the envelope of a ride it then writes: a cut
/// after a tool step's `model_step_finished` and before that step's batch commits
/// (`tool_batch_finished`). On a legacy state root today's writer replays no ride ledger when it
/// reopens a log, and its actor seeds the ledger again only with the queued sends, not the
/// step's calls, so the ride's carrier has no key there and the ride goes out with none. The new
/// loop reads the key from its own records and always writes the envelope. A gap in today's loop,
/// reached only by a crash in that window; the request bytes are the same either way.
fn loses_ride_envelopes(frames: &[broca_wal::FramedRecord]) -> bool {
    let kind = |frame: &broca_wal::FramedRecord| {
        serde_json::to_value(&frame.record).unwrap()["type"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    };
    let Some(step) = frames.iter().rposition(|frame| {
        matches!(
            &frame.record,
            WalRecord::ModelStepFinished { assistant_message, .. }
                if assistant_message.tool_calls().next().is_some()
        )
    }) else {
        return false;
    };
    !frames[step..]
        .iter()
        .any(|frame| kind(frame) == "tool_batch_finished")
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
async fn compare_auth_pause_switching(scenario: Scenario, account: broca_wal::AuthSelection) {
    compare_auth_pause_switching_to(scenario, Some(account)).await;
}

async fn compare_auth_pause_switching_to(
    scenario: Scenario,
    resume_account: Option<broca_wal::AuthSelection>,
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
    resume_account: Option<broca_wal::AuthSelection>,
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
    resume_account: Option<broca_wal::AuthSelection>,
) {
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
        if mode == StartMode::ResumeAfterAuth {
            reset_passes().await;
            provider.resuming();
        }
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
                auth_selection: match mode {
                    StartMode::ResumeAfterAuth => resume_account.clone().or_else(account_selection),
                    _ => account_selection(),
                },
                account_limit_pause: account().is_some(),
                max_same_account_wait: account()
                    .map_or_else(Default::default, |script| script.wait),
                new_log_compaction_plan_version: layout(),
                ..admission_control(&scenario)
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
        convert_unbilled(&image.framed(), lineage(&image).as_deref())
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
    let now = harness_now;
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
        compaction_changes: false,
        keep_warm: new_warmer(),
        account_reports: reports().map(|reports| {
            Arc::new(NewReports(Arc::clone(&reports.new))) as Arc<dyn AccountReports>
        }),
        max_same_account_wait: account().map_or_else(Default::default, |script| script.wait),
        auth_selection: account_selection(),
        step_progress: None,
        model_call_ready: None,
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
        auth_selection: resume_account.or(seams.auth_selection.clone()),
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

/// In a session that began in the new storage, a step that announced its message's id and
/// stopped before writing it keeps that minted id when it runs again, even when a delivery lands
/// before it at the retried step's boundary: a minted id names the record that announced it,
/// wherever the message lands. (A converted session's position id is derived again instead,
/// which `every_cut_of_a_system_or_steer_send_resumes_alike` checks.)
#[tokio::test]
async fn a_retried_steps_minted_id_survives_a_delivery_before_it() {
    let scenario = calling("read", 0);
    for layout in LAYOUTS {
        let scenario = scenario.clone();
        let run = async move {
            let (_, _, full, _) = Box::pin(today_with_actions(
                &scenario,
                None,
                scenario.steps.clone(),
                vec![system_note("q1")],
                None,
            ))
            .await;
            // The log up to the queued send: the run started, its first step announced its
            // message, and the send waits.
            let cut = full
                .framed()
                .iter()
                .position(|frame| serde_json::to_value(&frame.record).unwrap()["type"] == "queued")
                .expect("the send was queued")
                + 1;
            let image = LogImage::parse(broca_crash_harness::cut_log(&full, cut, false));
            let mut converted = convert_unbilled(&image.framed(), lineage(&image).as_deref())
                .expect("the cut converts");
            // As a session begun in the new storage holds it: the step announced a minted id,
            // naming its own record, which takes the seq after the session's `WriterOpened`.
            let index = converted
                .records
                .iter()
                .position(|record| matches!(record, Record::StepStart(_)))
                .expect("the step started");
            if let Record::StepStart(start) = &mut converted.records[index] {
                start.mid = Some(format!("g0123456789abcdef.{}", index + 2));
            }
            let announced = normalized(&converted.records)[index]["StepStart"]["mid"].clone();
            let new = new_from(&scenario, Some(converted), scenario.steps.clone()).await;
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
                    after: Some("send_delivered"),
                    resume_route: Some(Some(anthropic_route(false))),
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

/// A ride recorded before llm-runner 0.8.11, with no envelope, converts and goes out as today's
/// loop sends it, joined to its result, in every request after it.
#[tokio::test]
async fn a_ride_without_an_envelope_resumes_alike() {
    for tool in ["read", "broken"] {
        RIDES_WRITER
            .scope(
                false,
                compare_actions_cut(
                    calling(tool, 0),
                    vec![a_note("q1")],
                    Cuts {
                        after: Some("send_delivered"),
                        resume_route: None,
                    },
                ),
            )
            .await;
    }
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
            rides: vec![Ride::Drop(2)],
        };
        let scenario = compacted_scenario(steps);
        LOST.scope(lost, compare_compacted(scenario.clone(), script()))
            .await;
        // Today's calls, which the new loop's were just compared with, show the case is seen:
        // the call after the lost answer is a Setup, and a Setup was sent once before it.
        let calls = LOST
            .scope(
                lost,
                LAYOUT.scope(
                    Some(cortexkit_compaction_view::PLAN_VERSION),
                    COMPACTION.scope(Some(Arc::new(script())), today(&scenario)),
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
                    rides: vec![Ride::Drop(2)],
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

/// A run whose repeated failing call spans a cut: two failures, then a big result pushes the next
/// request over the window, and the provider cuts through the first failure and drops the big
/// call. The new loop seals that history mid-run, so the third failure's warning must still count
/// the sealed first one, from the counters the seal kept.
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
        rides: vec![Ride::CutAndDrop(3, 6)],
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
                    // The first run's entries are 1-2, so the cut and drop move up by two.
                    rides: vec![Ride::CutAndDrop(5, 8)],
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
    compare_compacted_every_cut(repetition_across_a_cut_scenario(), repetition_cut()).await;
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
                        rides: vec![Ride::Drop(2)],
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

/// A tool result pushes the next request over the window, and the provider answers with no view,
/// so the request still doesn't fit: the run ends at the context ceiling without sending it, with
/// the ceiling's decision recorded as today's loop records it.
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
/// numbered by that run's own frozen rules, as today's loop numbers it. The real-data request
/// check found the new loop sending these without their numbers.
fn tagged_scenario() -> Scenario {
    let tools = vec![tool("read", ExecutionMode::Parallel)];
    let mut manifest = planned_manifest(&tools).as_ref().clone();
    manifest.plan = broca_wal::OpaqueJson::from_json_text(
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

/// Two tool steps, then an answer. Each step reports an attempt with its body length, so a later
/// step's input can be estimated from the measured one before it.
fn measured_scenario() -> Scenario {
    let tools = vec![tool("read", ExecutionMode::Parallel)];
    Scenario {
        input: vec![user("read a and b")],
        manifest: None,
        tools,
        stop_when: Vec::new(),
        steps: vec![
            attempted(
                answer(
                    "a1",
                    vec![call("c1", "read", json!({"path": "a"}))],
                    FinishReason::ToolCalls,
                ),
                vec![attempt(1, 200, "complete")],
            ),
            attempted(
                answer(
                    "a2",
                    vec![call("c2", "read", json!({"path": "b"}))],
                    FinishReason::ToolCalls,
                ),
                vec![attempt(1, 200, "complete")],
            ),
            attempted(text_step("a3", "done"), vec![attempt(1, 200, "complete")]),
        ],
    }
}

/// A context limit whose room beside the estimated input is below the frozen cap and above the
/// floor: each step after the first has its cap lowered, durable before it is sent.
fn lowering() -> OutputScript {
    OutputScript {
        limits: broca_core::output_reservation::OutputLimits::new(32_000, 0),
        context_limit: 1_024 + 100 + 10_000,
    }
}

/// A context limit with no room for even the floor beside the estimated input: the second step
/// is never sent and the run ends with the overflow error.
fn overflowing() -> OutputScript {
    OutputScript {
        limits: broca_core::output_reservation::OutputLimits::new(32_000, 0),
        context_limit: 1_024 + 100 + 1_000,
    }
}

#[tokio::test]
async fn a_step_whose_cap_would_pass_the_context_limit_is_sent_with_a_lowered_cap() {
    OUTPUT
        .scope(Some(lowering()), compare(measured_scenario()))
        .await;
}

#[tokio::test]
async fn a_lowered_cap_survives_every_crash_cut() {
    OUTPUT
        .scope(Some(lowering()), compare_every_cut(measured_scenario()))
        .await;
}

#[tokio::test]
async fn a_step_with_no_room_for_output_ends_the_run_unsent() {
    OUTPUT
        .scope(Some(overflowing()), compare(measured_scenario()))
        .await;
}

#[tokio::test]
async fn a_compacted_step_whose_cap_would_pass_the_context_limit_is_lowered() {
    let script = CompactionScript {
        context_limit: lowering().context_limit,
        rides: Vec::new(),
    };
    OUTPUT
        .scope(
            Some(lowering()),
            compare_compacted(measured_scenario(), script),
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
    CACHE
        .scope(
            true,
            compare_compacted(
                cached_scenario(broca_core::clock::Clock::default().now_ms()),
                CompactionScript {
                    context_limit: 100_000,
                    rides: Vec::new(),
                },
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

/// Runs `compare` with keep-warm on in both loops, then checks both sent the same warms, and
/// that they sent some: `expect` is how many today's loop sent in each of the comparison's
/// modes, when the comparison knows.
async fn compare_warmed<F: std::future::Future<Output = ()>>(expect: Option<usize>, compare: F) {
    let pair = Arc::new(WarmPair::default());
    WARM.scope(Some(Arc::clone(&pair)), compare).await;
    let today = pair.today.log.lock().unwrap().clone();
    let new = pair.new.log.lock().unwrap().clone();
    assert!(!today.is_empty(), "today's loop sent no warm");
    if let Some(expect) = expect {
        assert_eq!(
            today.len(),
            // Each layout runs once per writer.
            expect * LAYOUTS.len() * 2,
            "today's warms: {today:?}"
        );
    }
    assert_eq!(new, today, "the new loop's warms");
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
    compare_routed(
        AccountScript {
            selection: routed("acct-1"),
            wait: broca_core::SameAccountWait::default(),
            wait_for_reset: false,
        },
        compare_auth_pause(tool_then_limited(account_limited())),
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
/// sending anything: an open batch closes with what its calls hold, as today's does.
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
    manifest: Option<Arc<broca_wal::PlanManifest>>,
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
fn enrolling_manifest() -> Arc<broca_wal::PlanManifest> {
    let mut manifest = compacted_manifest(&enrollment_tools()).as_ref().clone();
    manifest.system_text = ENROLLED_SYSTEM.into();
    manifest.plan_identity = "plan-compacted".into();
    Arc::new(manifest)
}

/// A plan with no compaction provider that renders part numbers into the model's view.
fn tagging_manifest() -> Arc<broca_wal::PlanManifest> {
    let mut manifest = planned_manifest(&enrollment_tools()).as_ref().clone();
    manifest.plan = broca_wal::OpaqueJson::from_json_text(
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
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
        compaction_plane: Some(plane),
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        model_call_ready: None,
    };
    let mut writer = Recording {
        session,
        records: Vec::new(),
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
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
        compaction_plane: Some(plane),
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        model_call_ready: None,
    };
    let mut writer = Recording {
        session,
        records: Vec::new(),
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
/// - the entries cover every earlier message but the suppressed global system messages, each
///   with a time, numbered in the projection's numbering from the highest number an earlier
///   request showed, contiguous and never repeating, the run's own after the enrollment's last;
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

            // The entries: every earlier message but the global system messages, with times.
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
            let expected_ordinals: Vec<u64> = raw
                .iter()
                .map(|entry| entry.ordinal)
                .filter(|ordinal| !globals.contains(ordinal))
                .collect();
            assert_eq!(served_ordinals, expected_ordinals);
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
            assert_eq!(session.entry_by_mid(global_mid).await.unwrap(), None);
        })
        .await;
    })
    .await;
}

/// A view the compaction provider hands over on the enrolling run, which cuts through history
/// before the enrollment point and drops the run's own big call, applies to the enrolled
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
            // provider answers the ride with a view that cuts through an earlier message and drops
            // the big call, and the step renders from it.
            let big_call = session.conversation.placed + 2;
            let spy = CompactionSpy::new(&CompactionScript {
                context_limit: 1_200,
                rides: vec![Ride::CutAndDrop(cut, big_call)],
            });
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
            assert!(
                session.conversation.window_start() > 1,
                "the cut sealed history: {:?}",
                spy.calls()
            );
            assert!(session.conversation.window_start() <= enrolled.through_ordinal);
            let texts =
                |request: &CallOptions| request.prompt.iter().map(text_of).collect::<Vec<_>>();
            assert_eq!(requests.len(), 2, "{:?}", spy.calls());
            assert!(texts(&requests[0])
                .iter()
                .any(|text| text.contains("read a")));
            assert!(!texts(&requests[1])
                .iter()
                .any(|text| text.contains("read a")));
            assert!(!texts(&requests[1])
                .iter()
                .any(|text| text.contains("done with a")));
            assert!(texts(&requests[1])
                .iter()
                .any(|text| text.contains("count the rows")));
            assert_eq!(text_of(&requests[1].prompt[0]), ENROLLED_SYSTEM);

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

/// A compaction provider whose first Setup fails: the run that asks ends unavailable.
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
            return Err(broca_core::compaction_v2::runtime::CallError::Unavailable);
        }
        self.inner
            .call_v2(module, bind, scope, method, params, deadline)
            .await
    }
}

/// A failed Setup on the enrolling run ends it `compaction_unavailable`, never in the old
/// layout: the session stays enrolled, and the next send asks for Setup again and runs on.
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

/// On the enrolling run, a scheduled refusal fallback takes the step only if the prepared
/// request's own estimate fits the fallback's window; the newest step's usage, from before the
/// request was rebuilt, doesn't decide. A window that holds it lets the step go there.
#[tokio::test]
async fn the_enrolling_run_falls_back_only_when_its_own_request_fits() {
    in_each_cipher(|| async {
        for (fallback_limit, expected) in [
            (150, broca_core::refusal_fallback::StepRoute::Source),
            (1_000_000, broca_core::refusal_fallback::StepRoute::Fallback),
        ] {
            FALLBACK
                .scope(
                    Some(FallbackScript {
                        model: ModelHandle::new(provider_id(), "fallback-model"),
                        context_limit: Some(fallback_limit),
                    }),
                    async {
                        let backend = MemoryBackend::default();
                        let mut session = enrollment_session(&backend).await;
                        let mut parts = MemoryParts::default();
                        let spy = CompactionSpy::new(&CompactionScript {
                            context_limit: ENROLLED_LIMIT,
                            rides: Vec::new(),
                        });
                        // The run's own model refuses its first step, which arms the schedule the
                        // next run carries.
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
                        let mut manifest = enrolling_manifest().as_ref().clone();
                        manifest.system_text = "Compact with care. ".repeat(200);
                        let provider = ScriptedProvider::new(vec![text_step("a2", "on")]);
                        enroll_run_with(
                            &mut session,
                            &mut parts,
                            &spy,
                            &EnrollRun {
                                id: "r2",
                                input: vec![user("second task")],
                                manifest: Some(Arc::new(manifest)),
                                steps: Vec::new(),
                                limit: None,
                            },
                            &provider,
                        )
                        .await
                        .expect("the enrolling run");
                        assert!(session.operations.enrolled.is_some());
                        assert_eq!(
                            *provider.route.lock().unwrap(),
                            Some(expected),
                            "a fallback window of {fallback_limit}"
                        );
                    },
                )
                .await;
        }
    })
    .await;
}

/// A session converted from today's storage, which joined text delivered with a tool result into
/// that result, enrolls through the new loop: the earlier result keeps its stored bytes with the
/// delivered text joined, while text delivered with a result of the enrolling run on stays apart
/// from it and is joined only as a request renders, as the current compaction contract places
/// it. Today's log is what today's loop wrote for one run, with a hand-written delivery riding
/// its tool result.
#[tokio::test]
async fn a_converted_session_with_a_joined_ride_enrolls_and_keeps_its_bytes() {
    in_each_cipher(|| async {
        // Today's log: one run with a tool call, and text delivered with its result.
        let today_run = Scenario {
            input: vec![system("You are terse."), user("read a")],
            tools: enrollment_tools(),
            manifest: None,
            stop_when: Vec::new(),
            steps: vec![
                answer(
                    "a1",
                    vec![call("c1", "read", json!({}))],
                    FinishReason::ToolCalls,
                ),
                text_step("a2", "done with a"),
            ],
        };
        let (_, image, _) = today_from(&today_run, None, today_run.steps.clone()).await;
        let mut frames = image.framed();
        let finished = frames
            .iter()
            .position(|frame| {
                matches!(frame.record, broca_wal::WalRecord::ToolBatchFinished { .. })
            })
            .expect("today's run finished its batch");
        let run_id = frames
            .iter()
            .find_map(|frame| match &frame.record {
                broca_wal::WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
                _ => None,
            })
            .unwrap();
        let delivered: broca_wal::WalRecord = serde_json::from_value(json!({
            "type": "send_delivered",
            "submission_id": "q1",
            "run_id": run_id,
            "mode": "tool_result",
            "tool_call_id": "c1",
            "appended_text": "\n\nalso check b",
            "written_at_ms": 7,
        }))
        .unwrap();
        frames.insert(
            finished + 1,
            broca_wal::FramedRecord {
                seq: 0,
                fence: frames[finished].fence,
                record: delivered,
            },
        );
        for (at, frame) in frames.iter_mut().enumerate() {
            frame.seq = at as u64 + 1;
        }
        let converted =
            convert_unbilled(&frames, lineage(&image).as_deref()).expect("the log converts");
        assert!(converted.lost.is_empty(), "{:?}", converted.lost);

        // The converted session joins delivered text into results, as today's storage did.
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
                    ToolOutput::Text { text } => Some(text.clone()),
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
                plain.plan = broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap();
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
                &broca_session_convert::billing::resolver,
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
