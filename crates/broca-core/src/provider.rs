//! The model-step seam. The loop runs each step through this trait, so the
//! deterministic stub (offline loop proof) and the real direct-socket provider
//! client are two impls of one contract.
//!
//! A step yields a STREAM of normalized events: display deltas (forwarded to the
//! caller untouched) interleaved with the provider-ASSEMBLED control units (the
//! only thing the loop consumes). The stream ends after `Finished` or `Error`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_protocol::{CallOptions, GenerationConfig, NormalizedEvent, StepId, ToolChoice};
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;

pub type LiveStepProgress = Arc<Mutex<Option<broca_protocol::provider::InFlightStep>>>;

/// Per-call runtime state, kept out of the frozen request and rendering/cache domain.
pub struct StepRuntime {
    pub cancel: CancellationToken,
    pub deadline: Option<Instant>,
    pub seal: Option<crate::ShutdownSeal>,
    pub progress: Option<LiveStepProgress>,
    /// The ordinal the step's first attempt in this call is recorded with. Zero for an
    /// ordinary step; a step re-sent within the same step id (to the refusal-fallback model)
    /// starts after the attempts already recorded, so ordinals stay unique within the step.
    pub first_attempt: u32,
}

#[async_trait]
pub trait Provider: Send + Sync {
    /// Exclusively acquire any in-memory connection retained for this full session bind.
    fn bind_run_transport(&self, _session: &broca_protocol::BindIdentity) {}

    /// Pin the freshly resolved credential record version before acquiring run transport.
    fn set_credential_revision(&self, _revision: u64) {}

    async fn step_with_runtime(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        runtime: StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        match runtime.seal {
            Some(seal) => {
                self.step_with_shutdown_seal(step_id, opts, runtime.cancel, runtime.deadline, seal)
                    .await
            }
            None => {
                self.step_with_shared_cancel(step_id, opts, runtime.cancel, runtime.deadline)
                    .await
            }
        }
    }
    /// Retire network state on cancellation, graceful stop, task abort or another abnormal exit.
    /// The loop calls this even if a caller retains the provider for a later run.
    fn close_run_transport(&self) {}

    /// Release transport after a durably completed run. Abnormal exits still close it.
    fn complete_run_transport(&self) {
        self.close_run_transport();
    }

    /// Whether a strict schema compiler refusal permits this run's one non-strict resend.
    fn structured_output_grammar_rejection(&self, _error: &broca_protocol::ProviderError) -> bool {
        false
    }

    /// Whether the model provider binds signed reasoning to the bytes before it, rejecting a
    /// request in which those bytes changed (Anthropic's thinking signatures). A compaction
    /// view that changes them is deferred or applied with the companion strip of that
    /// reasoning. The default, false, is a provider that echoes reasoning without such a bind.
    fn reasoning_is_prefix_bound(&self) -> bool {
        false
    }

    /// Apply only after the downgrade record is durable. Subsequent renders and steps must
    /// use this decision; a resumed provider is reconstructed from replay's updated config.
    fn downgrade_structured_output(&self) {}

    /// Runtime break-glass policy for the ChatGPT socket. Evaluated per attempt, off the
    /// rendering/cache domain; providers for other families ignore it.
    fn set_chatgpt_https_policy(&self, _policy: Arc<dyn Fn() -> bool + Send + Sync>) {}

    /// The model a step the source model refuses is re-sent to, when this episode froze a
    /// client-side refusal fallback at admission. `None`, the default, means the run is not
    /// eligible and a refusal commits as the step's answer.
    fn refusal_fallback_model(&self) -> Option<broca_protocol::request::ModelHandle> {
        None
    }

    /// The fallback model's context window, when [`Self::refusal_fallback_model`] is `Some` and
    /// the catalog knew it at admission.
    fn refusal_fallback_context_limit(&self) -> Option<u64> {
        None
    }

    /// Send the following steps, renders and warms to `route`'s model. Only the request this
    /// provider renders changes: the recorded render config, its renderer version and the
    /// frozen-config blob always describe the episode's own (source) model. A provider without
    /// a refusal fallback ignores it.
    fn set_step_route(&self, _route: crate::refusal_fallback::StepRoute) {}

    /// Run ONE model step for `opts`. The returned receiver streams normalized
    /// events until the provider's terminal `Finished`/`Error`.
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent>;

    /// Run one model step with caller-owned cancellation and an optional absolute watchdog
    /// deadline. Transport-backed providers use the deadline to avoid parking a rate-limit retry
    /// past the point where the caller will cancel it. The default preserves compatibility for
    /// providers that complete synchronously or only stream in-process events.
    async fn step_with_cancel(
        &self,
        step_id: StepId,
        opts: &CallOptions,
        _cancel: CancellationToken,
        _deadline: Option<Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.step(step_id, opts).await
    }

    /// Run one step from an immutable request snapshot. The default preserves the
    /// borrowed API for in-process providers; transport providers can retain the
    /// shared snapshot until blocking request rendering finishes.
    async fn step_with_shared_cancel(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        cancel: CancellationToken,
        deadline: Option<Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.step_with_cancel(step_id, &opts, cancel, deadline)
            .await
    }

    /// Run one step that the module's shutdown seal can stop through `seal`, a signal that is
    /// NOT the caller's cancel. A provider that honors it registers each HTTP attempt with the
    /// seal, and when the seal trips mid-attempt it hands that attempt's record to the seal
    /// instead of sending it on the stream, then keeps its stream open and silent until the run
    /// task is dropped. The default ignores the seal, which leaves the seal nothing to record.
    async fn step_with_shutdown_seal(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        cancel: CancellationToken,
        deadline: Option<Instant>,
        _seal: crate::ShutdownSeal,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.step_with_shared_cancel(step_id, opts, cancel, deadline)
            .await
    }

    /// Render `opts` to the EXACT, DETERMINISTIC request bytes this provider would
    /// send. This is the cache/resume hash domain — the loop hashes THESE bytes,
    /// and a real provider MUST use the same function to build its live request, so
    /// the hash can never drift from what's actually sent. The default is the
    /// shared canonical renderer; a provider with a bespoke wire shape overrides it
    /// (and MUST stay deterministic + bump its renderer version on any change).
    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        crate::render::canonical_request_bytes(opts)
    }

    /// This provider's renderer version (recorded in `run_config`; a change busts
    /// resume). Defaults to the shared canonical renderer's version.
    fn renderer_version(&self) -> u32 {
        crate::render::CANONICAL_RENDERER_VERSION
    }

    /// Whether this provider still renders the exact request bytes of a run that recorded
    /// `recorded` as its renderer version. A resume checks this instead of plain equality, so
    /// a deploy that bumped the version for a change gated on a frozen flag (absent from older
    /// recorded configs) does not refuse runs whose bytes it still reproduces. The default
    /// accepts only the provider's own version; a provider that accepts more must prove each
    /// extra version with a byte-equality test over pinned old configs and prompts.
    fn reproduces_renderer_version(&self, recorded: u32) -> bool {
        recorded == self.renderer_version()
    }

    /// The frozen render config blob this provider would have recorded had the run been
    /// admitted at `renderer_version`, a version for which [`Self::reproduces_renderer_version`]
    /// is true. A resume compares this, not [`Self::render_config_blob`], with the recorded
    /// blob, so a blob that carries the renderer version inside it can name the recorded one.
    /// Nothing else may differ from `render_config_blob`. The default is `render_config_blob`
    /// unchanged, which is right for any provider whose blob does not carry the version.
    fn render_config_blob_for_version(&self, _renderer_version: u32) -> serde_json::Value {
        self.render_config_blob()
    }

    /// This provider's frozen render config, serialized to an OPAQUE blob for the
    /// WAL. The loop stores it verbatim and never interprets it; on resume the
    /// provider is reconstructed FROM this blob so it renders byte-identical bytes.
    /// Providers with no configurable render state use a non-null stateless marker.
    /// The blob only needs to round-trip losslessly; it is NOT the cache-hash domain
    /// (that is the rendered request bytes).
    fn render_config_blob(&self) -> serde_json::Value {
        serde_json::json!({ "render_config": "stateless" })
    }

    /// Return a permanent configuration error when a fresh episode cannot be rendered with the
    /// provider state selected during resolution. The run loop calls this only before writing a
    /// new `RunStarted`; resumes keep using their previously admitted frozen configuration.
    fn fresh_admission_config_error(
        &self,
        _generation: &GenerationConfig,
        _tool_choice: &ToolChoice,
    ) -> Option<String> {
        None
    }

    /// Whether this provider sends a `ProviderControl::AttemptFinished` for every attempt,
    /// INCLUDING the in-flight one when its step is cancelled, and then closes its stream
    /// promptly. When true, the loop keeps reading a cancelled step's stream for a short grace
    /// period so that last attempt record reaches the WAL before the run's terminal. When
    /// false (the default, for in-process providers that may hold their stream open after a
    /// cancel), the loop stops reading at once, exactly as before attempt records existed.
    fn reports_attempt_evidence(&self) -> bool {
        false
    }

    /// Re-send `base`, an exact request body this provider rendered earlier, shaped so the
    /// provider generates nothing and only reads (and so keeps) the cached prefix. One attempt,
    /// never retried. The default cannot warm and sends nothing.
    async fn warm(&self, _base: &[u8]) -> Result<crate::WarmSent, crate::WarmFailure> {
        Err(crate::WarmFailure::Unsupported(
            "this provider cannot warm a prompt cache".into(),
        ))
    }
}

/// A boxed provider is itself a provider — it forwards every method to the inner
/// value. This lets a caller that chooses a provider at runtime (e.g. a factory that
/// returns `Box<dyn Provider>` for a catalog-resolved wire family) drive the generic
/// `run_loop` directly, without the loop needing to know the concrete type.
#[async_trait]
impl Provider for Box<dyn Provider> {
    fn bind_run_transport(&self, session: &broca_protocol::BindIdentity) {
        (**self).bind_run_transport(session);
    }

    fn set_credential_revision(&self, revision: u64) {
        (**self).set_credential_revision(revision);
    }

    async fn step_with_runtime(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        runtime: StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        (**self).step_with_runtime(step_id, opts, runtime).await
    }
    fn close_run_transport(&self) {
        (**self).close_run_transport();
    }

    fn complete_run_transport(&self) {
        (**self).complete_run_transport();
    }

    fn structured_output_grammar_rejection(&self, error: &broca_protocol::ProviderError) -> bool {
        (**self).structured_output_grammar_rejection(error)
    }

    fn reasoning_is_prefix_bound(&self) -> bool {
        (**self).reasoning_is_prefix_bound()
    }

    fn downgrade_structured_output(&self) {
        (**self).downgrade_structured_output();
    }

    fn set_chatgpt_https_policy(&self, policy: Arc<dyn Fn() -> bool + Send + Sync>) {
        (**self).set_chatgpt_https_policy(policy);
    }

    fn refusal_fallback_model(&self) -> Option<broca_protocol::request::ModelHandle> {
        (**self).refusal_fallback_model()
    }

    fn refusal_fallback_context_limit(&self) -> Option<u64> {
        (**self).refusal_fallback_context_limit()
    }

    fn set_step_route(&self, route: crate::refusal_fallback::StepRoute) {
        (**self).set_step_route(route);
    }

    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        (**self).step(step_id, opts).await
    }

    async fn step_with_cancel(
        &self,
        step_id: StepId,
        opts: &CallOptions,
        cancel: CancellationToken,
        deadline: Option<Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        (**self)
            .step_with_cancel(step_id, opts, cancel, deadline)
            .await
    }

    async fn step_with_shared_cancel(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        cancel: CancellationToken,
        deadline: Option<Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        (**self)
            .step_with_shared_cancel(step_id, opts, cancel, deadline)
            .await
    }

    async fn step_with_shutdown_seal(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        cancel: CancellationToken,
        deadline: Option<Instant>,
        seal: crate::ShutdownSeal,
    ) -> mpsc::Receiver<NormalizedEvent> {
        (**self)
            .step_with_shutdown_seal(step_id, opts, cancel, deadline, seal)
            .await
    }

    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        (**self).render_request(opts)
    }

    fn renderer_version(&self) -> u32 {
        (**self).renderer_version()
    }

    fn reproduces_renderer_version(&self, recorded: u32) -> bool {
        (**self).reproduces_renderer_version(recorded)
    }

    fn render_config_blob_for_version(&self, renderer_version: u32) -> serde_json::Value {
        (**self).render_config_blob_for_version(renderer_version)
    }

    fn render_config_blob(&self) -> serde_json::Value {
        (**self).render_config_blob()
    }

    fn fresh_admission_config_error(
        &self,
        generation: &GenerationConfig,
        tool_choice: &ToolChoice,
    ) -> Option<String> {
        (**self).fresh_admission_config_error(generation, tool_choice)
    }

    fn reports_attempt_evidence(&self) -> bool {
        (**self).reports_attempt_evidence()
    }

    async fn warm(&self, base: &[u8]) -> Result<crate::WarmSent, crate::WarmFailure> {
        (**self).warm(base).await
    }
}

/// A deterministic provider that replays a scripted event sequence per step
/// (offline loop proof). Scripts are consumed in order; a step past the last
/// script yields an empty stream (a protocol violation the loop surfaces).
#[derive(Clone)]
pub struct StubProvider {
    scripts: Arc<Mutex<VecDeque<Vec<NormalizedEvent>>>>,
    calls: Arc<Mutex<Vec<CallOptions>>>,
    refusal_fallback: Option<(broca_protocol::request::ModelHandle, Option<u64>)>,
    route: Arc<Mutex<crate::refusal_fallback::StepRoute>>,
    routes: Arc<Mutex<Vec<crate::refusal_fallback::StepRoute>>>,
}

impl StubProvider {
    /// `scripts[i]` is the event sequence the provider streams for step i.
    pub fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into())),
            calls: Arc::new(Mutex::new(Vec::new())),
            refusal_fallback: None,
            route: Arc::new(Mutex::new(crate::refusal_fallback::StepRoute::Source)),
            routes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Act as an episode that froze `model` (with `context_limit`) as its refusal fallback.
    pub fn with_refusal_fallback(
        mut self,
        model: broca_protocol::request::ModelHandle,
        context_limit: Option<u64>,
    ) -> Self {
        self.refusal_fallback = Some((model, context_limit));
        self
    }

    /// Provider call options in invocation order.
    pub fn calls(&self) -> Vec<CallOptions> {
        self.calls.lock().unwrap().clone()
    }

    /// The route each call was sent on, in invocation order.
    pub fn routes(&self) -> Vec<crate::refusal_fallback::StepRoute> {
        self.routes.lock().unwrap().clone()
    }
}

#[async_trait]
impl Provider for StubProvider {
    fn refusal_fallback_model(&self) -> Option<broca_protocol::request::ModelHandle> {
        self.refusal_fallback
            .as_ref()
            .map(|(model, _)| model.clone())
    }

    fn refusal_fallback_context_limit(&self) -> Option<u64> {
        self.refusal_fallback.as_ref().and_then(|(_, limit)| *limit)
    }

    fn set_step_route(&self, route: crate::refusal_fallback::StepRoute) {
        *self.route.lock().unwrap() = route;
    }

    async fn step(&self, _step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.calls.lock().unwrap().push(opts.clone());
        let route = *self.route.lock().unwrap();
        self.routes.lock().unwrap().push(route);
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for ev in script {
                if tx.send(ev).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}
