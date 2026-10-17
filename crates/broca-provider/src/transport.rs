// The 300-second idle limit is adapted from OpenAI Codex,
// revision a6baf8867cb4c9726213c0884a5c8b11f0cfd8bf (Apache-2.0). Copyright 2025 OpenAI.
// See THIRD-PARTY-NOTICES.md and LICENSES/Apache-2.0.txt. The ChatGPT usage-limit
// classifier, also adapted from Codex, is in the cortexkit-chatgpt-policy crate.
// broca applies these policies to its shared HTTP/WebSocket transport and account-limit errors.

//! HTTP/SSE driver for every wire family, with run-local Responses sockets for ChatGPT
//! (feature `live`). Rendering and Responses normalization are shared across both pipes.
//!
//! `LiveProvider<F>` turns a `(WireFamily, FrozenRenderConfig, AuthHandle)` into the
//! loop's [`broca_core::Provider`]: it asks the family to render the body and build
//! the HTTP shape, POSTs it with reqwest, and feeds the response bytes back through
//! the family's normalizer. ONE transport loop serves every grammar — the family
//! owns the wire-specific render + normalize, this owns only the network leg.
//!
//! The loop calls the normalizer's `finish()` at end-of-stream. That is required
//! for grammars with no in-band terminal event (OpenAI Chat/Responses, Gemini, and
//! Bedrock assemble and emit their final message there); a grammar that terminates
//! in-band — Anthropic on `message_stop` — leaves `finish()` a no-op.

// A failed attempt carries a whole `ProviderError` by value, which is past clippy's large-`Err`
// threshold now that the error can also name an exhausted account's reset time, window and
// account. Failures are the cold path of a network round trip, so moving a few more bytes there
// costs nothing measurable, and boxing every attempt failure would only add an allocation.
#![allow(clippy::result_large_err)]

use broca_diagnostics::{spawn_blocking, spawn_blocking_on, JobKind};

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Once, OnceLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use broca_core::ShutdownSeal;
use broca_core::{clock::Clock, provider::Provider};
use broca_protocol::{
    display::DisplayEvent,
    error::{
        ErrorClass, ProviderError, ACCOUNT_LIMIT_EXHAUSTED, BILLING_EXHAUSTED, CREDIT_INSUFFICIENT,
    },
    AttemptOutcome, CallOptions, CredentialRefresh, GenerationConfig, ModelAttempt,
    NormalizedEvent, ProviderControl, StepId, ToolChoice,
};
use bytes::Bytes;
use futures::StreamExt;
use rand::Rng;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    credential_refresh::CredentialRefresher,
    families::fresh_admission_config_error,
    normalize::classify_error_with_status_defaults,
    wire::{
        AuthHandle, FrozenRenderConfig, HttpRequest, NormalizeError, StreamNormalizer, WireFamily,
    },
};

#[cfg(test)]
#[path = "transport_websocket_tests.rs"]
mod websocket_tests;

#[cfg(test)]
#[path = "transport_retention_tests.rs"]
mod retention_tests;

#[path = "transport_websocket.rs"]
mod responses_websocket;

#[path = "transport_request_gzip.rs"]
pub mod request_gzip;

#[cfg(test)]
#[path = "transport_request_gzip_tests.rs"]
mod request_gzip_tests;

/// Shareable HTTP transport handle. Clones retain the same reqwest connection pools.
///
/// It holds two reqwest clients because reqwest decides response decoding per client, and a
/// client that decodes also adds an `accept-encoding` header to every request it sends.
/// Anthropic's first-party API gets Claude Code's `accept-encoding` and a client that decodes
/// what comes back; every other host gets a client without decoding, so its requests go out
/// exactly as they did before decoding existed.
#[derive(Clone, Debug)]
pub struct ProviderHttpClient {
    /// Every host except `api.anthropic.com`. No response decoding and no `accept-encoding`.
    identity: reqwest::Client,
    /// `api.anthropic.com` only. Decodes gzip, deflate, brotli and zstd responses as they stream.
    decoding: reqwest::Client,
    request_gzip: request_gzip::RequestGzipPolicy,
}

impl ProviderHttpClient {
    fn build(
        builder: impl Fn() -> reqwest::ClientBuilder,
        request_gzip: request_gzip::RequestGzipPolicy,
    ) -> Self {
        let build = |decode: bool| {
            builder()
                .gzip(decode)
                .deflate(decode)
                .brotli(decode)
                .zstd(decode)
                .build()
                .expect("the provider HTTP client configuration is valid")
        };
        Self {
            identity: build(false),
            decoding: build(true),
            request_gzip,
        }
    }

    /// The client for a request: the decoding one only for Anthropic's first-party host.
    fn for_host(&self, anthropic_api_host: bool) -> &reqwest::Client {
        if anthropic_api_host {
            &self.decoding
        } else {
            &self.identity
        }
    }

    /// The one-attempt HTTP/1.1 pool-escape client, with this client's request-gzip policy.
    fn http1_escape(&self) -> Self {
        Self::build(|| provider_client_builder().http1_only(), self.request_gzip)
    }

    /// The client that never decodes responses, for diagnostic tools that record a response
    /// exactly as it came off the connection.
    pub fn without_response_decoding(&self) -> &reqwest::Client {
        &self.identity
    }

    /// A client that sends `api.anthropic.com` to a local test server, with its own gzip latch.
    #[cfg(test)]
    fn resolving_anthropic_api_to(address: std::net::SocketAddr, gzip_enabled: bool) -> Self {
        Self::build(
            || provider_client_builder().resolve(request_gzip::ANTHROPIC_API_HOST, address),
            request_gzip::RequestGzipPolicy::isolated(gzip_enabled),
        )
    }
}

/// Tests drive single attempts with a plain reqwest client against local servers, which never
/// take the Anthropic host's path; request gzip stays off for them.
#[cfg(test)]
impl From<reqwest::Client> for ProviderHttpClient {
    fn from(client: reqwest::Client) -> Self {
        Self {
            identity: client.clone(),
            decoding: client,
            request_gzip: request_gzip::RequestGzipPolicy::isolated(false),
        }
    }
}

/// A live provider: any wire family + its frozen render config + a runtime auth
/// credential, driven over reqwest. Generic over the family so the same transport
/// loop runs every grammar.
pub struct LiveProvider<F: WireFamily + 'static> {
    pub(crate) client: ProviderHttpClient,
    clock: Clock,
    pub(crate) family: Arc<F>,
    pub(crate) frozen: Arc<FrozenRenderConfig>,
    /// The credential the next step is sent with. Behind a lock only so an in-step refresh can
    /// replace it: once a refreshed token has been accepted, later steps of this run must not
    /// go back to the one the provider rejected. The lock belongs to this provider instance, and
    /// a run builds its own provider, so a refresh never changes what another run sends.
    auth: Arc<std::sync::RwLock<ServedAuth>>,
    frozen_account_ref: Option<String>,
    /// Where a fresh token for the same credential comes from after an HTTP 401. `None` keeps
    /// the historical behavior: the 401 ends the step.
    pub(crate) refresher: Option<Arc<dyn CredentialRefresher>>,
    websocket: Arc<responses_websocket::Session>,
    #[cfg(test)]
    escape_client_builds: std::sync::Arc<AtomicUsize>,
    #[cfg(test)]
    retry_clock: RetryClock,
}

/// Owns the rendered body behind a reference count so retry requests do not copy the prompt.
#[derive(Clone)]
struct PreparedHttpRequest {
    /// Local diagnostic policy, never part of the rendered request or its digest.
    ephemeral_session: bool,
    url: String,
    headers: Vec<(String, String)>,
    body: Bytes,
    /// The request's wire family, so a non-2xx answer is classified by the family that sent it.
    /// `None` only for a request built outside a render job (tests). Read only on error.
    family: Option<crate::spec::WireFamilyId>,
    /// The wire family and the caller's service tier, when one was requested, so a non-2xx
    /// answer can be recognised as the provider refusing that tier. Read only on error.
    service_tier: Option<(
        crate::spec::WireFamilyId,
        crate::service_tier::ServiceTierRequest,
    )>,
}

impl From<HttpRequest> for PreparedHttpRequest {
    fn from(request: HttpRequest) -> Self {
        Self {
            ephemeral_session: false,
            url: request.url,
            headers: request.headers,
            body: Bytes::from(request.body),
            family: None,
            service_tier: None,
        }
    }
}

/// Immutable, whole-request input for one blocking render. All expensive input stays behind
/// shared handles; the closure is the sole owner that releases its prompt snapshot.
struct RenderJob<F: WireFamily + 'static> {
    family: Arc<F>,
    frozen: Arc<FrozenRenderConfig>,
    auth: Arc<AuthHandle>,
    opts: Arc<CallOptions>,
    step_id: StepId,
}

struct RenderedRequest {
    request: PreparedHttpRequest,
    name_map: Arc<crate::tool_name_alias::ToolNameMap>,
    request_digest: RequestDigest,
}

/// Length and SHA-256 of the exact request body. Every attempt of a step sends the same
/// rendered bytes, so this is computed once per step, on the blocking render worker.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RequestDigest {
    len: u64,
    sha256: String,
}

impl RequestDigest {
    fn of(body: &[u8]) -> Self {
        Self {
            len: body.len() as u64,
            sha256: lower_hex(&Sha256::digest(body)),
        }
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[derive(Clone)]
struct ChunkWarning {
    last_event_type: String,
    event_type_counts: String,
    error_chain: String,
}

/// What one provider attempt received, filled in while the attempt runs so it is complete at
/// whatever point the attempt ends, including a cancellation that drops it mid-read.
///
/// The digest covers the response body bytes as the HTTP client hands them over, before the
/// stream normalizer parses them: that is the only form a caller can compare with what the
/// provider says it sent. For every host but `api.anthropic.com` the client does no decoding,
/// so these are the bytes on the wire. Requests to `api.anthropic.com` accept compressed
/// answers, and there these are the decoded bytes, the same ones an uncompressed answer would
/// carry; the compressed size is not recorded. For sockets it covers UTF-8 text payloads before
/// SSE adaptation, not compressed frames or control-frame payloads.
#[derive(Clone, Default)]
struct AttemptTrace {
    ephemeral_session: bool,
    transport: Option<String>,
    websocket_request: Option<Box<broca_protocol::provider::WebSocketRequestDigest>>,
    websocket_close_code: Option<Arc<std::sync::atomic::AtomicU16>>,
    socket_generation: Option<u64>,
    socket_reused: Option<bool>,
    socket_age: Option<Duration>,
    socket_since_last_completed: Option<Duration>,
    output_seen: bool,
    defer_failure_warning: bool,
    chunk_warning: Option<ChunkWarning>,
    served_account_ref: Option<String>,
    /// When the request was handed to the HTTP client; see `ModelAttempt::sent_at_ms`.
    sent_at_ms: Option<u64>,
    sent_at: Option<std::time::Instant>,
    #[cfg(test)]
    virtual_attempt_elapsed: Option<Duration>,
    http_status: Option<u16>,
    content_type: Option<String>,
    response: Sha256,
    response_len: u64,
    /// The response-body reader already logged a detailed warning about where this attempt's response was cut off.
    chunk_error_warned: bool,
    response_cap: Option<u64>,
    content_progress: Box<broca_protocol::provider::ContentProgress>,
    /// Number of complete content records observed by the stream normalizer.
    content_event_count: u64,
    /// Elapsed from `sent_at` when the most recent complete content record was parsed.
    last_content_elapsed: Option<Duration>,
    keepalives_after_content: u64,
    /// The response's own report of how the provider altered the request, once the stream
    /// normalizer has read it.
    input_transformations: Option<Vec<broca_protocol::InputTransformation>>,
    /// The request did not ask for that report (see `ModelAttempt::input_transformations_unmeasured`).
    input_transformations_unmeasured: bool,
    /// Set before the attempt is sent when it re-sends the step with a refreshed token.
    credential_refresh: Option<CredentialRefresh>,
    /// The account evidence the response carried; see [`broca_protocol::AttemptAccountObservation`].
    account_observation: broca_protocol::AttemptAccountObservation,
}

impl AttemptTrace {
    fn socket_age_ms(&self) -> Option<u64> {
        self.socket_age
            .map(|age| (age + self.elapsed_since_send().unwrap_or_default()).as_millis() as u64)
    }

    fn socket_since_last_completed_ms(&self) -> Option<u64> {
        self.socket_since_last_completed
            .map(|age| (age + self.elapsed_since_send().unwrap_or_default()).as_millis() as u64)
    }

    fn elapsed_since_send(&self) -> Option<Duration> {
        self.sent_at.map(|sent| {
            #[cfg(test)]
            if let Some(elapsed) = self.virtual_attempt_elapsed {
                return elapsed;
            }
            sent.elapsed()
        })
    }

    fn observe(&mut self, raw: &[u8]) {
        self.response.update(raw);
        self.response_len += raw.len() as u64;
    }

    fn into_attempt(
        self,
        attempt: usize,
        request: &RequestDigest,
        outcome: AttemptOutcome,
    ) -> ModelAttempt {
        ModelAttempt {
            attempt: attempt as u32,
            request_body_len: request.len,
            request_body_sha256: request.sha256.clone(),
            http_status: self.http_status,
            content_type: self.content_type,
            response_body_len: self.response_len,
            response_body_sha256: lower_hex(&self.response.finalize()),
            response_body_cap: self.response_cap,
            outcome,
            ts_ms: now_millis(),
            content_progress: self.content_progress,
            sent_at_ms: self.sent_at_ms,
            transport: self.transport,
            websocket_request: self.websocket_request,
            websocket_close_code: self.websocket_close_code.and_then(|code| {
                let code = code.load(Ordering::Acquire);
                (code != 0).then_some(code)
            }),
            input_transformations: self.input_transformations,
            input_transformations_unmeasured: self.input_transformations_unmeasured,
            credential_refresh: self.credential_refresh.map(Box::new),
            served_account_ref: self.served_account_ref,
            account_observation: Box::new(self.account_observation),
        }
    }
}

/// Every `anthropic-ratelimit-unified-*` response header, by lowercase name, with its value
/// exactly as received. The names are matched by prefix so a header this build does not know
/// still travels; values are passed through as bytes (decoded as UTF-8, replacing only bytes
/// that are not), never parsed or rounded. A repeated name keeps its first value.
fn ratelimit_unified_headers(
    headers: &reqwest::header::HeaderMap,
) -> std::collections::BTreeMap<String, String> {
    cortexkit_anthropic_policy::collect_unified_quota_headers(
        headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )
}

impl<F: WireFamily + 'static> RenderJob<F> {
    fn render(self) -> RenderedRequest {
        // This scan is deliberately in the same blocking job as rendering: it walks the entire
        // prompt to preserve aliases from historical tool calls on every resumed step.
        let name_map = Arc::new(crate::tool_name_alias::ToolNameMap::build(
            &self.opts.tools,
            &self.opts.prompt,
        ));
        let body = self.family.render(&self.frozen, &self.opts);
        let mut request =
            PreparedHttpRequest::from(self.family.endpoint(&self.frozen, &self.auth, body));
        request.ephemeral_session = self.opts.ephemeral_session;
        request.family = Some(self.frozen.family_id);
        request.service_tier = self
            .frozen
            .service_tier_request
            .clone()
            .map(|tier| (self.frozen.family_id, tier));
        maybe_dump_rendered_request(&request.body, &self.opts, self.step_id);
        let request_digest = RequestDigest::of(&request.body);
        RenderedRequest {
            request,
            name_map,
            request_digest,
        }
    }
}

/// Restore model-echoed tool aliases at the transport boundary without changing the canonical
/// family normalizers or any persisted message. Each request builds one map and every retry shares
/// it through a fresh wrapper, so aliases are applied only to inbound rendered events.
struct ToolNameRestoringNormalizer {
    inner: Box<dyn StreamNormalizer>,
    names: Arc<crate::tool_name_alias::ToolNameMap>,
    live_progress: Option<broca_core::provider::LiveStepProgress>,
}

impl ToolNameRestoringNormalizer {
    fn report_content(&self) {
        if let Some(progress) = &self.live_progress {
            if let Some(step) = progress.lock().unwrap().as_mut() {
                let content = self.inner.content_progress();
                step.last_content_at_ms = content.last_content_at_ms;
                step.max_content_gap_ms = content.max_content_gap_ms;
                step.max_content_gap_ended_at_ms = content.max_content_gap_ended_at_ms;
            }
        }
    }
}

impl StreamNormalizer for ToolNameRestoringNormalizer {
    fn websocket_telemetry(&mut self) {
        self.inner.websocket_telemetry();
    }
    fn content_progress(&self) -> broca_protocol::provider::ContentProgress {
        self.inner.content_progress()
    }
    fn private_error_diagnostics(&mut self) {
        self.inner.private_error_diagnostics();
    }
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let result = self.inner.push(chunk);
        self.report_content();
        result.map(|events| {
            events
                .into_iter()
                .map(|event| self.names.restore_event(event))
                .collect()
        })
    }

    fn progress_marker(&self) -> u64 {
        self.inner.progress_marker()
    }

    fn observe_response_headers(&mut self, headers: &crate::wire::ResponseHeaders) {
        self.inner.observe_response_headers(headers);
    }

    fn last_event_type(&self) -> Option<&str> {
        self.inner.last_event_type()
    }

    fn keepalives_after_content(&self) -> Option<u64> {
        self.inner.keepalives_after_content()
    }

    fn event_type_counts(&self) -> Option<&[(String, u64)]> {
        self.inner.event_type_counts()
    }

    fn liveness_marker(&self) -> Option<u64> {
        self.inner.liveness_marker()
    }

    fn input_transformations(&self) -> Option<Vec<broca_protocol::InputTransformation>> {
        self.inner.input_transformations()
    }

    fn input_transformations_unmeasured(&self) -> bool {
        self.inner.input_transformations_unmeasured()
    }

    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let result = self.inner.finish();
        self.report_content();
        result.map(|events| {
            events
                .into_iter()
                .map(|event| self.names.restore_event(event))
                .collect()
        })
    }
}

fn build_retry_normalizer<F: WireFamily>(
    family: &F,
    frozen: &FrozenRenderConfig,
    step_id: StepId,
    names: &Arc<crate::tool_name_alias::ToolNameMap>,
) -> ToolNameRestoringNormalizer {
    ToolNameRestoringNormalizer {
        inner: family.normalizer(frozen, step_id),
        names: Arc::clone(names),
        live_progress: None,
    }
}

/// Build the HTTP client used by every live wire family.
///
/// Client-level settings are process-wide constants. Provider- and run-specific behavior lives
/// on each request instead: endpoints come from the frozen config, auth is attached as headers,
/// and stream deadlines wrap the request future. A long-lived caller can therefore share this
/// client to retain its connection pool without changing request bytes or policy.
pub fn build_provider_http_client() -> ProviderHttpClient {
    ProviderHttpClient::build(
        provider_client_builder,
        request_gzip::RequestGzipPolicy::from_env(),
    )
}

/// The connection settings both of a provider client's reqwest clients share.
fn provider_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder().connect_timeout(PROVIDER_CONNECT_TIMEOUT)
}

impl<F: WireFamily + 'static> LiveProvider<F> {
    /// Convenience constructor for one-shot callers that do not own a shared HTTP client.
    pub fn new(family: F, frozen: FrozenRenderConfig, auth: AuthHandle) -> Self {
        Self::new_with_clock(family, frozen, auth, Clock::default())
    }

    pub(crate) fn new_with_clock(
        family: F,
        frozen: FrozenRenderConfig,
        auth: AuthHandle,
        clock: Clock,
    ) -> Self {
        Self::with_client_and_clock(build_provider_http_client(), family, frozen, auth, clock)
    }

    pub(crate) fn with_client_and_clock(
        client: ProviderHttpClient,
        family: F,
        frozen: FrozenRenderConfig,
        auth: AuthHandle,
        clock: Clock,
    ) -> Self {
        Self {
            client,
            clock,
            family: Arc::new(family),
            frozen: Arc::new(frozen),
            // Wrapped HERE rather than taken as an Arc, so callers keep passing an owned
            // AuthHandle and the sharing stays inside this file. Each step takes a cheap Arc
            // snapshot of the current credential rather than a deep copy of the secret; only
            // an in-step refresh after a 401 ever replaces it.
            auth: Arc::new(std::sync::RwLock::new(ServedAuth {
                auth: Arc::new(auth),
                account_ref: None,
            })),
            frozen_account_ref: None,
            refresher: None,
            websocket: Arc::new(responses_websocket::Session::default()),
            #[cfg(test)]
            escape_client_builds: std::sync::Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            retry_clock: RetryClock::default(),
        }
    }

    /// Use the module's shared clock for relative provider reset deadlines.
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Let this provider recover from an HTTP 401 inside a step: ask `refresher` for a fresh
    /// token for the same credential and, when it differs from the rejected one, re-send the
    /// same request once with it.
    pub fn with_credential_refresher(mut self, refresher: Arc<dyn CredentialRefresher>) -> Self {
        self.refresher = Some(refresher);
        self
    }

    /// Keep the account selected when the run was admitted separate from the account
    /// associated with the token currently being used.
    pub fn with_account_refs(mut self, frozen: Option<String>, served: Option<String>) -> Self {
        self.frozen_account_ref = frozen;
        self.auth
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .account_ref = served;
        self
    }

    fn current_auth(&self) -> ServedAuth {
        self.auth
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn start_step(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        cancel: CancellationToken,
        deadline: Option<Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.start_step_under_seal(step_id, opts, cancel, deadline, None, None)
    }

    fn start_step_under_seal(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        cancel: CancellationToken,
        deadline: Option<Instant>,
        seal: Option<ShutdownSeal>,
        progress: Option<broca_core::provider::LiveStepProgress>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let (tx, rx) = mpsc::channel::<NormalizedEvent>(256);
        let served = self.current_auth();
        let error = account_changed_error(
            self.frozen_account_ref.as_deref(),
            served.account_ref.as_deref(),
        )
        .or_else(|| {
            (self.frozen.family_id == crate::spec::WireFamilyId::OpenAiResponsesChatgpt)
                .then(|| {
                    account_changed_error(
                        self.frozen_account_ref.as_deref(),
                        chatgpt_account(&served.auth),
                    )
                })
                .flatten()
        });
        if let Some(mut error) = error {
            redact_chatgpt_account_error(&mut error, Some(self.frozen.family_id));
            tokio::spawn(async move {
                let _ = tx
                    .send(NormalizedEvent::Control(ProviderControl::Error {
                        error,
                        retries_used: 0,
                    }))
                    .await;
            });
            return rx;
        }
        let render_job = RenderJob {
            family: Arc::clone(&self.family),
            frozen: Arc::clone(&self.frozen),
            auth: served.auth,
            opts,
            step_id,
        };
        #[cfg(test)]
        let retry_clock = self.retry_clock.clone();
        #[cfg(not(test))]
        let retry_clock = RetryClock::default();
        let recovery = self.refresher.as_ref().map(|refresher| AuthRecovery {
            refresher: Arc::clone(refresher),
            current: Arc::clone(&self.auth),
            frozen_account_ref: self.frozen_account_ref.clone(),
        });

        spawn_transport_task(
            self.client.clone(),
            self.clock.clone(),
            Arc::clone(&self.family),
            Arc::clone(&self.frozen),
            render_job,
            served.account_ref,
            recovery,
            step_id,
            tx,
            cancel,
            deadline,
            seal,
            progress,
            retry_clock,
            Arc::clone(&self.websocket),
            #[cfg(test)]
            self.escape_client_builds.clone(),
        );
        rx
    }
}

#[async_trait]
impl<F: WireFamily + 'static> Provider for LiveProvider<F> {
    async fn step_with_runtime(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        runtime: broca_core::provider::StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.start_step_under_seal(
            step_id,
            opts,
            runtime.cancel,
            runtime.deadline,
            runtime.seal,
            runtime.progress,
        )
    }
    fn close_run_transport(&self) {
        self.websocket.close();
    }

    fn set_chatgpt_https_policy(&self, policy: Arc<dyn Fn() -> bool + Send + Sync>) {
        self.websocket.set_policy(policy);
    }

    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.start_step(
            step_id,
            Arc::new(opts.clone()),
            CancellationToken::new(),
            None,
        )
    }

    async fn step_with_cancel(
        &self,
        step_id: StepId,
        opts: &CallOptions,
        cancel: CancellationToken,
        deadline: Option<Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.start_step(step_id, Arc::new(opts.clone()), cancel, deadline)
    }

    async fn step_with_shared_cancel(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        cancel: CancellationToken,
        deadline: Option<Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.start_step(step_id, opts, cancel, deadline)
    }

    /// Honors the seal: each attempt registers with it, and an attempt the seal stops hands
    /// its `sealed_in_flight` record to the seal rather than to the run loop.
    async fn step_with_shutdown_seal(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        cancel: CancellationToken,
        deadline: Option<Instant>,
        seal: ShutdownSeal,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.start_step_under_seal(step_id, opts, cancel, deadline, Some(seal), None)
    }

    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        self.family.render(&self.frozen, opts)
    }

    fn renderer_version(&self) -> u32 {
        self.frozen.renderer_version
    }

    fn render_config_blob(&self) -> Value {
        serde_json::to_value(self.frozen.as_ref()).expect("frozen render config must serialize")
    }

    fn fresh_admission_config_error(
        &self,
        generation: &GenerationConfig,
        tool_choice: &ToolChoice,
    ) -> Option<String> {
        fresh_admission_config_error(&self.frozen, generation, tool_choice)
    }

    /// The transport records every HTTP attempt, and on a cancel sends the in-flight
    /// attempt's `cancelled` record before its task ends and the stream closes.
    fn reports_attempt_evidence(&self) -> bool {
        true
    }

    /// One attempt with the credential this provider currently holds, after checking, as a step
    /// does, that the credential's account is still the one the session froze. Never retried: a
    /// warm that fails is reported, not repeated.
    async fn warm(&self, base: &[u8]) -> Result<broca_core::WarmSent, broca_core::WarmFailure> {
        let served = self.current_auth();
        if account_changed_error(
            self.frozen_account_ref.as_deref(),
            served.account_ref.as_deref(),
        )
        .is_some()
        {
            return Err(broca_core::WarmFailure::AccountChanged {
                frozen: self.frozen_account_ref.clone(),
                served: served.account_ref,
            });
        }
        let body = self
            .family
            .warm_body(&self.frozen, base)
            .map_err(broca_core::WarmFailure::Unsupported)?;
        send_warm(
            &self.client,
            &self.clock,
            self.family.endpoint(&self.frozen, &served.auth, body),
            self.frozen.family_id,
            served.account_ref,
        )
        .await
    }
}

/// How long a warm waits for its whole answer, including a capped Responses generation.
const WARM_TIMEOUT: Duration = Duration::from_secs(60);

/// Bound warm answers even when a backend streams generated content with its usage.
const MAX_WARM_RESPONSE_BYTES: usize = 1024 * 1024;

/// Send one prepared warm request and read its answer.
///
/// A warm to `api.anthropic.com` has the same wire shape as the step it warms: a compressed
/// body when the step's would be, and one uncompressed re-send if the compression is refused.
async fn send_warm(
    client: &ProviderHttpClient,
    clock: &Clock,
    request: HttpRequest,
    family: crate::spec::WireFamilyId,
    served_account_ref: Option<String>,
) -> Result<broca_core::WarmSent, broca_core::WarmFailure> {
    let warm_body_sha256 = lower_hex(&Sha256::digest(&request.body));
    let anthropic_api_host = request_gzip::is_anthropic_api_host(&request.url);
    let identity = Bytes::from(request.body);
    let failed = |message: String| {
        broca_core::WarmFailure::Provider(ProviderError {
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Transient,
            message,
            status: None,
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        })
    };
    let exchange = async {
        let mut gzip = encode_request_gzip(client, anthropic_api_host, &identity).await;
        let (sent_at_ms, status, header_map, body) = loop {
            let sent_at_ms = now_millis();
            let response = attempt_request_builder(
                client,
                &request.url,
                &request.headers,
                anthropic_api_host,
                gzip.is_some(),
            )
            .body(gzip.clone().unwrap_or_else(|| identity.clone()))
            .send()
            .await
            .map_err(|error| failed(format!("warm request failed: {error}")))?;
            let status = response.status();
            let header_map = response.headers().clone();
            let body = read_warm_body(response, &failed).await?;
            if gzip.is_some() {
                if let Some(rejection) = request_gzip::classify_rejection(
                    status.as_u16(),
                    &header_map,
                    &String::from_utf8_lossy(&body),
                ) {
                    client.request_gzip.latch(status.as_u16(), rejection);
                    gzip = None;
                    continue;
                }
            }
            break (sent_at_ms, status, header_map, body);
        };
        let retry_after_secs = parse_retry_after_header(&header_map);
        let headers = response_headers(&header_map);
        let ratelimit_headers: Vec<(String, String)> = header_map
            .iter()
            .filter(|(name, _)| {
                name.as_str()
                    .starts_with(crate::cache_warm::RATELIMIT_HEADER_PREFIX)
            })
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect();
        if !status.is_success() {
            let code = status.as_u16();
            let text = String::from_utf8_lossy(&body);
            let error = classify_http_error_in_family(
                code,
                &text,
                retry_after_secs,
                None,
                Some(family),
                None,
            );
            let error = classify_account_limit_exhausted(
                family,
                code,
                &headers,
                &text,
                UNIX_EPOCH + Duration::from_millis(clock.now_ms()),
                error,
            );
            return Err(broca_core::WarmFailure::Provider(error));
        }
        let usage = match family {
            crate::spec::WireFamilyId::OpenAiResponses
            | crate::spec::WireFamilyId::OpenAiResponsesChatgpt => {
                crate::cache_warm::responses_response_usage(&body)
            }
            _ => crate::cache_warm::anthropic_response_usage(&body),
        };
        if family == crate::spec::WireFamilyId::OpenAiResponsesChatgpt
            && usage.cached_input_tokens == Some(0)
        {
            tracing::info!(
                "Codex cache warm read zero cached tokens; best-effort re-prime, not retried"
            );
        }
        Ok(broca_core::WarmSent {
            warm_body_sha256,
            sent_at_ms,
            usage,
            served_account_ref,
            ratelimit_headers,
            http_status: status.as_u16(),
        })
    };
    tokio::time::timeout(WARM_TIMEOUT, exchange)
        .await
        .unwrap_or_else(|_| Err(failed("warm timed out waiting for its answer".into())))
}

/// Read a whole warm answer, up to its size limit.
async fn read_warm_body(
    response: reqwest::Response,
    failed: &impl Fn(String) -> broca_core::WarmFailure,
) -> Result<Vec<u8>, broca_core::WarmFailure> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| failed(format!("warm answer unreadable: {error}")))?;
        if body.len() + chunk.len() > MAX_WARM_RESPONSE_BYTES {
            return Err(failed("warm answer exceeded its size limit".into()));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The compressed body for this send, or `None` to send the identity body. Compressed only for
/// `api.anthropic.com`, under the client's request-gzip policy, with fresh padding each call so
/// every attempt, retries included, is padded and compressed anew. A failure to compress sends
/// the identity body.
async fn encode_request_gzip(
    client: &ProviderHttpClient,
    anthropic_api_host: bool,
    body: &Bytes,
) -> Option<Bytes> {
    if !anthropic_api_host || !client.request_gzip.should_gzip(body) {
        return None;
    }
    // A level-6 pass over a large prompt takes milliseconds, so it runs on the blocking pool
    // rather than on a runtime worker that is also driving other streams.
    let identity = body.clone();
    let encoded = spawn_blocking(JobKind::Render, move || {
        request_gzip::gzip_with_fresh_padding(&identity)
    })
    .await;
    match encoded {
        Ok(Ok(gzip)) => Some(Bytes::from(gzip.wire)),
        Ok(Err(error)) => {
            tracing::warn!(%error, "request gzip failed; sending the body uncompressed");
            None
        }
        Err(error) => {
            tracing::warn!(%error, "request gzip task failed; sending the body uncompressed");
            None
        }
    }
}

/// A POST with the family's headers, plus the wire-only headers for `api.anthropic.com`:
/// `content-encoding: gzip` when the body is compressed, and Claude Code's `accept-encoding`
/// unless the family set one. The family's headers come first and in order, as before.
fn attempt_request_builder(
    client: &ProviderHttpClient,
    url: &str,
    headers: &[(String, String)],
    anthropic_api_host: bool,
    gzip: bool,
) -> reqwest::RequestBuilder {
    let mut builder = client.for_host(anthropic_api_host).post(url);
    let mut has_accept_encoding = false;
    for (name, value) in headers {
        if gzip && name.eq_ignore_ascii_case("content-encoding") {
            continue;
        }
        has_accept_encoding |= name.eq_ignore_ascii_case("accept-encoding");
        builder = builder.header(name, value);
    }
    if gzip {
        builder = builder.header(reqwest::header::CONTENT_ENCODING, "gzip");
    }
    if anthropic_api_host && !has_accept_encoding {
        builder = builder.header(
            reqwest::header::ACCEPT_ENCODING,
            request_gzip::CLAUDE_CODE_ACCEPT_ENCODING,
        );
    }
    builder
}

static DUMP_REQUEST_DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
static DUMP_WARNING: Once = Once::new();
static DUMP_SWEEP_IN_FLIGHT: AtomicBool = AtomicBool::new(false);
static DUMP_SWEEP_WRITE_COUNT: AtomicUsize = AtomicUsize::new(0);
static DUMP_SWEEP_LAST_START_MS: AtomicU64 = AtomicU64::new(0);

const DEFAULT_DUMP_MAX_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_DUMP_MAX_AGE_DAYS: u64 = 7;
const DUMP_SWEEP_EVERY_WRITES: usize = 32;
const DUMP_SWEEP_MIN_INTERVAL_MS: u64 = 30_000;
/// Maximum provider error-body bytes read into memory before rejecting the response.
const MAX_HTTP_ERROR_BODY_READ_BYTES: usize = 1024 * 1024;
/// Maximum provider error-body bytes retained in the diagnostic message.
const MAX_ERROR_BODY_BYTES: usize = 512;
/// Maximum aggregate response bytes accepted during one provider step.
const MAX_STREAM_BYTES: usize = 256 * 1024 * 1024;
/// Deadline for establishing a provider connection; streaming requests have no total deadline.
const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Matches the external codex-rs client's 300-second semantic-progress limit. The same deadline covers response
/// headers and gaps between complete decoded records; raw bytes from an incomplete record do not
/// reset it. This is not a total stream deadline, so a long stream making real progress stays live.
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
/// How long a stream may carry ONLY keepalive records (SSE `: keepalive` comments) before it
/// is abandoned. Applies only to grammars that report a liveness marker; for them the
/// 300-second deadline above is reset by any complete record, comments included, and this one
/// is reset only by content.
///
/// MEASURED, NOT GUESSED. xai/grok-4.7, replaying a failing request byte for byte, reasons
/// without streaming its reasoning and sends nothing but `: keepalive` comments every 15 s
/// while it does. The silent span varied from 138 s to 579 s across samples for about 45k
/// reasoning tokens, and every sample then completed normally. A 300-second content limit cut
/// those healthy streams and the retry was cut the same way. A maximum-effort reasoning step
/// can run several times the longest measured span, so 1800 s leaves roughly 3x headroom over
/// 579 s while still bounding a peer that keeps the connection alive and never answers.
const STREAM_CONTENT_TIMEOUT: Duration = Duration::from_secs(1800);

/// Optional floor on how slowly a provider may stream while still counting as healthy, read from
/// `BROCA_MIN_STREAM_BYTES_PER_SEC` and `BROCA_MIN_STREAM_WINDOW_SECS`.
///
/// DEFAULT OFF, deliberately, for TWO independent reasons.
///
/// First: this is the only guard here that can abort work which would have finished, and the
/// failure it addresses is rare -- across 3,540 production episodes the median run streamed at
/// ~44 tok/s and only seven ran under 5 tok/s for more than ten seconds. Enabling it by default
/// would trade a rare failure for a common one.
///
/// Second, and the reason it differs from every other bound in this file: A BOUND USUALLY COSTS
/// NOTHING WHILE IT DOES NOT BIND. The 1800s watchdog and the 300s idle timeout are comparisons
/// that simply do not fire on a healthy stream, so they need no argument about how many runs
/// require them -- they protect the rare case and leave the common one untouched. This floor is
/// not free while dormant: it accumulates decoded byte counts and window timings on EVERY stream,
/// whether or not the floor is ever crossed. That puts it in the same class as an optimisation --
/// it owes an argument about the population it serves -- and the population is seven episodes.
///
/// It measures DECODED CONTENT BYTES, not tokens. Token counts arrive with the terminal usage
/// event, so a token rate is not observable mid-stream; the env var is named in bytes so it cannot
/// promise a precision it does not have. As a rough guide ~4 bytes is one token, making 8 bytes/sec
/// about 2 tok/s.
///
/// The window opens at the FIRST CONTENT BYTE and never before. A model that thinks for a long time
/// without emitting anything is silent, not slow, and silence is already the idle timeout's job --
/// starting the clock at request time would abort exactly those healthy runs. Firing therefore
/// requires bytes arriving steadily but below the floor across a full window, which is the trickle
/// signature and is disjoint from silence by construction.
fn stream_floor_from_env() -> Option<StreamFloor> {
    let bytes_per_sec = std::env::var("BROCA_MIN_STREAM_BYTES_PER_SEC")
        .ok()?
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|rate| rate.is_finite() && *rate > 0.0)?;
    let window_secs = std::env::var("BROCA_MIN_STREAM_WINDOW_SECS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(60);
    Some(StreamFloor {
        bytes_per_sec,
        window: Duration::from_secs(window_secs),
    })
}

#[derive(Debug, Clone, Copy)]
struct StreamFloor {
    bytes_per_sec: f64,
    window: Duration,
}

/// Tracks decoded content volume since the window opened, and reports the first full window whose
/// rate sits under the floor.
struct StreamRateWindow {
    floor: StreamFloor,
    /// None until the first content byte: the window has not opened yet.
    opened_at: Option<tokio::time::Instant>,
    content_bytes: usize,
}

impl StreamRateWindow {
    fn new(floor: StreamFloor) -> Self {
        Self {
            floor,
            opened_at: None,
            content_bytes: 0,
        }
    }

    /// Count the decoded content in one batch of events. Only text, reasoning and tool-input deltas
    /// count: control units are assembled bookkeeping that arrive at their own cadence, so counting
    /// them would let a stream emitting no content at all look productive.
    fn observe(&mut self, events: &[NormalizedEvent]) {
        let content: usize = events
            .iter()
            .map(|event| match event {
                NormalizedEvent::Display(DisplayEvent::TextDelta { delta, .. })
                | NormalizedEvent::Display(DisplayEvent::ReasoningDelta { delta, .. })
                | NormalizedEvent::Display(DisplayEvent::ToolInputDelta { delta, .. }) => {
                    delta.len()
                }
                _ => 0,
            })
            .sum();
        if content == 0 {
            return;
        }
        self.opened_at.get_or_insert_with(tokio::time::Instant::now);
        self.content_bytes += content;
    }

    /// The measured rate once a full window has elapsed, or None while the window is still open or
    /// has not started. Returning the rate rather than a bool keeps the number available for the
    /// error message, so a caller reading it can tell a trickle from a stall.
    fn breach(&self) -> Option<f64> {
        let opened_at = self.opened_at?;
        let elapsed = opened_at.elapsed();
        if elapsed < self.floor.window {
            return None;
        }
        let rate = self.content_bytes as f64 / elapsed.as_secs_f64();
        (rate < self.floor.bytes_per_sec).then_some(rate)
    }
}

// The retry schedule for one model step.
//
// A transient failure (a 429 included) is re-sent up to MAX_ATTEMPTS sends in total. Between
// sends the loop waits 2 s, doubling per failed attempt and capped at 30 s, each wait scaled by a
// random factor in 0.8..=1.2 so concurrent runs that failed together do not re-send together. A
// provider's Retry-After, when present, replaces the computed wait, up to MAX_RETRY_AFTER_DELAY.
//
// Worst case: the computed waits alone are 2 + 4 + 8 + 16 + 30 = 60 s nominal, 72 s with the
// jitter at its top, so they never reach RETRY_WALL_BUDGET. Retry-After waits can, and the budget
// stops them: failed attempts with no response bytes plus waits may consume at most 240 s.
// Streamed generation time is excluded: progress followed by a cut is not time spent waiting
// for an unavailable provider. This applies to every wire family and both transports.
// HTTPS recovery after socket exhaustion has its own bounded attempt and waiting allowance.
//
// Some failures are never re-sent at all, whatever the budget: permanent, auth and context
// overflow classes, `x-should-retry: false`, an exhausted account, a refused service tier, and a
// second deterministic decode failure.
//
// One exception adds a send: a 401 answered by a fresh token for the same credential is re-sent
// once, outside MAX_ATTEMPTS but inside RETRY_WALL_BUDGET.
// `drive_step_attempts` states the full rule where it applies it.
const MAX_ATTEMPTS: usize = 6;
const BASE_RETRY_DELAY: Duration = Duration::from_secs(2);
const MAX_BACKOFF_DELAY: Duration = Duration::from_secs(30);
const BACKOFF_JITTER: f64 = 0.2;
const MAX_RETRY_AFTER_DELAY: Duration = Duration::from_secs(120);
const RETRY_WALL_BUDGET: Duration = Duration::from_secs(240);
// All observed ChatGPT response cuts fell in 901.1–901.3 s after send; allow a small
// timing margin when recognizing the backend's 900 s response deadline.
const CHATGPT_RESPONSE_DEADLINE_THRESHOLD: Duration = Duration::from_secs(895);

/// Where the attempt loop waits between sends and reads how long it has been retrying.
///
/// In production this is plain tokio time. Tests can switch one provider to virtual waits, which
/// return at once and are counted as elapsed retry time, so a schedule worth minutes of waiting
/// runs in milliseconds against a real HTTP server. (Tokio's paused clock cannot do this: while an
/// attempt waits on socket IO the runtime looks idle and jumps straight to the header timeout.)
#[derive(Clone, Default)]
struct RetryClock {
    // Tests can measure a long attempt without advancing socket or retry deadlines.
    #[cfg(test)]
    virtual_attempt_elapsed: Option<Duration>,
    #[cfg(test)]
    virtual_waits: Option<Arc<std::sync::Mutex<Vec<Duration>>>>,
    #[cfg(test)]
    virtual_retry_attempts: Option<Arc<std::sync::Mutex<std::collections::VecDeque<Duration>>>>,
}

/// A point on a [`RetryClock`] from which retry time is measured.
#[derive(Clone, Copy)]
struct RetryMark {
    at: Instant,
    #[cfg(test)]
    virtual_waits_before: usize,
}

impl RetryClock {
    fn attempt_elapsed(&self, mark: RetryMark) -> Duration {
        #[cfg(test)]
        if let Some(attempts) = &self.virtual_retry_attempts {
            return attempts
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted attempt clock");
        }
        self.elapsed_since(mark)
    }
    fn mark(&self) -> RetryMark {
        RetryMark {
            at: Instant::now(),
            #[cfg(test)]
            virtual_waits_before: self
                .virtual_waits
                .as_ref()
                .map_or(0, |waits| waits.lock().unwrap().len()),
        }
    }

    fn elapsed_since(&self, mark: RetryMark) -> Duration {
        let elapsed = mark.at.elapsed();
        #[cfg(test)]
        if let Some(waits) = &self.virtual_waits {
            let waited: Duration = waits.lock().unwrap()[mark.virtual_waits_before..]
                .iter()
                .sum();
            return elapsed + waited;
        }
        elapsed
    }

    async fn sleep(&self, delay: Duration) {
        #[cfg(test)]
        if let Some(waits) = &self.virtual_waits {
            waits.lock().unwrap().push(delay);
            tokio::task::yield_now().await;
            return;
        }
        tokio::time::sleep(delay).await;
    }
}

/// Why a transient attempt failed. This tag stays inside the transport: public error classes do
/// not distinguish a broken stream from deterministic response decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptCause {
    TransportTransient,
    DeterministicDecode,
}

#[derive(Debug)]
struct AttemptFailure {
    error: ProviderError,
    /// The structured output-ceiling details, kept so the socket path can build its error message
    /// from them alone. Any other socket error text may echo provider bytes, including headers,
    /// so it is never passed on.
    output_ceiling: Option<Box<crate::wire::OutputCeilingCut>>,
    cause: Option<AttemptCause>,
    retry_forbidden: bool,
    /// How the attempt ended, for its attempt record. Named at each failure site rather than
    /// derived from `error`, because the public error classes do not distinguish them.
    outcome: AttemptOutcome,
}

impl AttemptFailure {
    fn transport(error: ProviderError, outcome: AttemptOutcome) -> Self {
        Self {
            error,
            cause: Some(AttemptCause::TransportTransient),
            output_ceiling: None,
            retry_forbidden: false,
            outcome,
        }
    }

    /// Transport failure that must surface without reissuing the same request to this provider.
    ///
    /// Ordinary transport blips use [`Self::transport`] and may burn the local attempt budget.
    /// This constructor is for cases where another attempt would only re-hit the same degraded
    /// endpoint (for example a streaming-rate floor breach): the error class is left untouched so a
    /// `Transient` health signal still reaches the caller's router, while `retry_forbidden` stops
    /// the attempt loop from retrying first.
    fn transport_without_reissue(error: ProviderError, outcome: AttemptOutcome) -> Self {
        Self {
            error,
            cause: Some(AttemptCause::TransportTransient),
            output_ceiling: None,
            retry_forbidden: true,
            outcome,
        }
    }

    fn decode(error: ProviderError, outcome: AttemptOutcome) -> Self {
        Self {
            error,
            cause: Some(AttemptCause::DeterministicDecode),
            output_ceiling: None,
            retry_forbidden: false,
            outcome,
        }
    }

    fn http(error: ProviderError, retry_forbidden: bool) -> Self {
        Self {
            error,
            cause: None,
            output_ceiling: None,
            retry_forbidden,
            outcome: AttemptOutcome::HttpError,
        }
    }

    /// An in-band error event on a 2xx stream.
    fn control(error: ProviderError) -> Self {
        Self {
            error,
            cause: None,
            output_ceiling: None,
            retry_forbidden: false,
            outcome: AttemptOutcome::ProviderError,
        }
    }

    fn normalize(error: NormalizeError, outcome: AttemptOutcome) -> Self {
        let output_ceiling = match &error {
            NormalizeError::OutputCeiling(cut) => Some(Box::new(cut.clone())),
            _ => None,
        };
        let mut failure = Self::decode(normalize_error(error), outcome);
        failure.output_ceiling = output_ceiling;
        failure
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DumpRetentionCaps {
    max_bytes: u64,
    max_age_days: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DumpFileMeta {
    path: PathBuf,
    size_bytes: u64,
    modified_ms: u64,
}

fn dump_request_dir() -> Option<&'static PathBuf> {
    DUMP_REQUEST_DIR
        .get_or_init(|| match std::env::var_os("BROCA_DUMP_REQUEST_DIR") {
            Some(value) if !value.is_empty() => Some(PathBuf::from(value)),
            _ => None,
        })
        .as_ref()
}

/// THE EARLY RETURN MUST STAY ABOVE EVERY LINE BELOW IT.
///
/// Nothing after this point is free: the filename is built, the path joined, and `body` is
/// refcount-bumped and moved into a closure handed to a blocking thread. All of that is work
/// whose only purpose is to be discarded when the dump is off, which is every production run --
/// this fires once per model step on the hot path.
///
/// The hazard is that the wrong version is a one-line edit no reviewer would flag: hoisting the
/// clone or the filename above the gate reads identically to leaving it below, and the dump file
/// produced is byte-identical either way. The artefact cannot witness the work done to produce
/// it, so nothing downstream would ever notice the regression -- hence this note rather than a
/// test, which would have to observe a refcount to see the difference at all.
fn maybe_dump_rendered_request(body: &Bytes, opts: &CallOptions, step_id: StepId) {
    if opts.ephemeral_session {
        return;
    }
    let Some(dir) = dump_request_dir() else {
        return;
    };
    DUMP_WARNING.call_once(|| {
        tracing::warn!(
            target: "transport",
            "WARNING: BROCA_DUMP_REQUEST_DIR '{}' receives unredacted request bodies containing prompts and tool arguments",
            dir.display()
        );
    });
    let dir = dir.to_path_buf();
    let fallback_run_id = now_millis().to_string();
    let filename = request_dump_filename(opts.step_context.as_ref(), &fallback_run_id, step_id.0);
    let path = dir.join(filename);
    let body = body.clone();
    let write_dump = move || {
        if let Err(error) = create_dump_dir(&dir) {
            tracing::warn!(
                target: "transport",
                "could not create BROCA_DUMP_REQUEST_DIR '{}': {error}",
                dir.display()
            );
            return;
        }
        if let Err(error) = write_dump_file(&path, &body) {
            tracing::warn!(
                target: "transport",
                "could not write rendered request dump '{}': {error}",
                path.display()
            );
            return;
        }
        maybe_schedule_dump_retention_sweep(&dir);
    };
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        std::mem::drop(spawn_blocking_on(&handle, JobKind::Dump, write_dump));
    } else {
        std::mem::drop(std::thread::spawn(write_dump));
    }
}

fn create_dump_dir(dir: &Path) -> std::io::Result<()> {
    broca_owner_only::create_dir_all(dir)
}

fn write_dump_file(path: &Path, body: &[u8]) -> std::io::Result<()> {
    let mut options = broca_owner_only::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    let mut file = options.open(path)?;
    std::io::Write::write_all(&mut file, body)
}

/// Request dumps are best-effort debugging output, so retention work must stay off the
/// provider hot path. Each write pays only a few atomic ops; the expensive directory scan
/// runs in `spawn_blocking` at most once per 32 writes or 30 seconds, whichever comes first.
fn maybe_schedule_dump_retention_sweep(dir: &Path) {
    let write_count = DUMP_SWEEP_WRITE_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    let now_ms = now_millis();
    let last_start_ms = DUMP_SWEEP_LAST_START_MS.load(Ordering::Relaxed);
    let due_by_count = write_count >= DUMP_SWEEP_EVERY_WRITES;
    let due_by_time =
        last_start_ms == 0 || now_ms.saturating_sub(last_start_ms) >= DUMP_SWEEP_MIN_INTERVAL_MS;
    if !due_by_count && !due_by_time {
        return;
    }
    if DUMP_SWEEP_IN_FLIGHT
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    DUMP_SWEEP_WRITE_COUNT.store(0, Ordering::Relaxed);
    DUMP_SWEEP_LAST_START_MS.store(now_ms, Ordering::Relaxed);

    let dir = dir.to_path_buf();
    let caps = dump_retention_caps();
    let sweep = move || {
        struct SweepGuard;
        impl Drop for SweepGuard {
            fn drop(&mut self) {
                DUMP_SWEEP_IN_FLIGHT.store(false, Ordering::Release);
            }
        }

        let _guard = SweepGuard;
        if let Err(error) = sweep_dump_dir(&dir, caps) {
            tracing::warn!(
                target: "transport",
                "could not sweep BROCA_DUMP_REQUEST_DIR '{}': {error}",
                dir.display()
            );
        }
    };

    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        std::mem::drop(spawn_blocking_on(&handle, JobKind::Retention, sweep));
    } else {
        std::mem::drop(std::thread::spawn(sweep));
    }
}

fn dump_retention_caps() -> DumpRetentionCaps {
    DumpRetentionCaps {
        max_bytes: parse_env_u64("BROCA_DUMP_MAX_BYTES").unwrap_or(DEFAULT_DUMP_MAX_BYTES),
        max_age_days: parse_env_u64("BROCA_DUMP_MAX_AGE_DAYS").unwrap_or(DEFAULT_DUMP_MAX_AGE_DAYS),
    }
}

fn parse_env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.trim().parse::<u64>().ok()
}

fn sweep_dump_dir(dir: &Path, caps: DumpRetentionCaps) -> std::io::Result<()> {
    let files = collect_dump_files(dir)?;
    for path in dump_files_to_delete(files, now_millis(), caps) {
        if let Err(error) = std::fs::remove_file(&path) {
            tracing::warn!(
                target: "transport",
                "could not prune rendered request dump '{}': {error}",
                path.display()
            );
        }
    }
    Ok(())
}

fn collect_dump_files(dir: &Path) -> std::io::Result<Vec<DumpFileMeta>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(
                    target: "transport",
                    "could not read an entry in BROCA_DUMP_REQUEST_DIR '{}': {error}",
                    dir.display()
                );
                continue;
            }
        };
        let path = entry.path();
        if !entry
            .file_name()
            .to_str()
            .is_some_and(is_request_dump_filename)
        {
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                tracing::warn!(
                    target: "transport",
                    "could not read dump entry type '{}': {error}",
                    path.display()
                );
                continue;
            }
        };
        if !file_type.is_file() {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                tracing::warn!(
                    target: "transport",
                    "could not stat rendered request dump '{}': {error}",
                    path.display()
                );
                continue;
            }
        };
        let modified_ms = match metadata.modified() {
            Ok(modified) => system_time_millis(modified),
            Err(error) => {
                tracing::warn!(
                    target: "transport",
                    "could not read rendered request dump mtime '{}': {error}",
                    path.display()
                );
                continue;
            }
        };
        files.push(DumpFileMeta {
            path,
            size_bytes: metadata.len(),
            modified_ms,
        });
    }
    Ok(files)
}

fn dump_files_to_delete(
    mut files: Vec<DumpFileMeta>,
    now_ms: u64,
    caps: DumpRetentionCaps,
) -> Vec<PathBuf> {
    files.sort_by(|a, b| {
        a.modified_ms
            .cmp(&b.modified_ms)
            .then_with(|| a.path.cmp(&b.path))
    });
    let max_age_ms = caps.max_age_days.saturating_mul(24 * 60 * 60 * 1000);
    let mut total_bytes = files
        .iter()
        .fold(0_u64, |total, file| total.saturating_add(file.size_bytes));
    let mut deletions = Vec::new();

    for file in files {
        let age_ms = now_ms.saturating_sub(file.modified_ms);
        let too_old = age_ms > max_age_ms;
        let too_large = total_bytes > caps.max_bytes;
        if too_old || too_large {
            total_bytes = total_bytes.saturating_sub(file.size_bytes);
            deletions.push(file.path);
        }
    }

    deletions
}

use crate::request_dump::is_request_dump_filename;
pub use crate::request_dump::request_dump_filename;

fn now_millis() -> u64 {
    system_time_millis(SystemTime::now())
}

fn system_time_millis(time: SystemTime) -> u64 {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

// The distinct handles keep cancellation, the immutable render job, and retry state visibly
// separated at the async handoff; grouping them would only hide their ownership boundaries.
#[allow(clippy::too_many_arguments)]
fn spawn_transport_task<F: WireFamily + 'static>(
    client: ProviderHttpClient,
    clock: Clock,
    family: Arc<F>,
    frozen: Arc<FrozenRenderConfig>,
    render_job: RenderJob<F>,
    served_account_ref: Option<String>,
    recovery: Option<AuthRecovery>,
    step_id: StepId,
    tx: mpsc::Sender<NormalizedEvent>,
    cancel: CancellationToken,
    deadline: Option<Instant>,
    seal: Option<ShutdownSeal>,
    progress: Option<broca_core::provider::LiveStepProgress>,
    retry_clock: RetryClock,
    websocket: Arc<responses_websocket::Session>,
    #[cfg(test)] escape_client_builds: std::sync::Arc<AtomicUsize>,
) -> JoinHandle<()> {
    // The credential this step's request is rendered with, kept so a 401 can be compared
    // against whatever the refresher serves next.
    let sent_auth = Arc::clone(&render_job.auth);
    tokio::spawn(async move {
        struct StepTransportGuard<'a> {
            websocket: &'a responses_websocket::Session,
            tx: &'a mpsc::Sender<NormalizedEvent>,
            cancel: &'a CancellationToken,
        }
        impl Drop for StepTransportGuard<'_> {
            fn drop(&mut self) {
                if self.cancel.is_cancelled() || self.tx.is_closed() {
                    self.websocket.close();
                }
            }
        }
        // Include early exits while rendering or refreshing credentials, not only stream cancel.
        let _transport_guard = StepTransportGuard {
            websocket: &websocket,
            tx: &tx,
            cancel: &cancel,
        };
        // Dropping this JoinHandle after cancel/deadline detaches the non-cancellable render.
        // RenderJob remains owned by the blocking closure, so its potentially large prompt Arc
        // is released on that worker rather than on this Tokio runtime task.
        //
        // Detaching is not free: a cancelled, deadlined, or receiver-closed step leaves this job
        // queued, and a worker later runs the whole render — alias scan, endpoint prep, and the
        // diagnostic dump — for a request that will never be sent. That is bounded waste rather
        // than a leak, because the pool drains orders of magnitude faster than steps are
        // submitted, so it is accepted here. Aborting instead would reclaim the queued work, but
        // only for jobs that have not started, and it moves the prompt's release back onto this
        // task — so it trades the paragraph above for the one below it, and needs both weighed
        // together rather than either fixed alone.
        let render = spawn_blocking(JobKind::Render, move || render_job.render());
        let rendered = match deadline {
            Some(deadline) => {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    _ = tx.closed() => return,
                    _ = tokio::time::sleep_until(deadline) => {
                        cancel.cancel();
                        return;
                    }
                    result = render => result,
                }
            }
            None => {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    _ = tx.closed() => return,
                    result = render => result,
                }
            }
        };
        let rendered = match rendered {
            Ok(rendered) => rendered,
            Err(join_error) => {
                // Render assertions, including tool-name alias invariants, now unwind through
                // the blocking handle. Keep that failure on the typed provider-error path instead
                // of closing the receiver as an unexplained missing assistant message.
                let error = ProviderError {
                    provider_subcode: None,
                    refusal: None,
                    class: ErrorClass::Permanent,
                    message: format!("provider request render task failed: {join_error}"),
                    status: None,
                    retry_after_secs: None,
                    provider_code: Some("render_task_failed".to_string()),
                    resets_at_ms: None,
                    resets_at_source: None,
                    limit_window: None,
                    account_ref: None,
                    credential_id: None,
                    affordable_max_tokens: None,
                };
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {}
                    _ = tx.send(NormalizedEvent::Control(ProviderControl::Error {
                        error,
                        retries_used: 0,
                    })) => {}
                }
                return;
            }
        };

        let mut attempt = 0;
        let normalizer_factory = || {
            if let Some(progress) = &progress {
                if let Some(step) = progress.lock().unwrap().as_mut() {
                    step.attempt = attempt;
                    step.attempt_started_at_ms = now_millis();
                    step.last_content_at_ms = None;
                    step.max_content_gap_ms = None;
                    step.max_content_gap_ended_at_ms = None;
                }
            }
            attempt += 1;
            let mut normalizer = build_retry_normalizer(
                family.as_ref(),
                frozen.as_ref(),
                step_id,
                &rendered.name_map,
            );
            normalizer.live_progress = progress.clone();
            normalizer
        };
        // Rebuilds only the HTTP shape (url and headers) for a refreshed credential. The body is
        // reused as-is: auth attaches after render, so the bytes sent do not change.
        let rebuild = |auth: &AuthHandle, sent: &PreparedHttpRequest| {
            let fresh = family.endpoint(frozen.as_ref(), auth, sent.body.to_vec());
            PreparedHttpRequest {
                ephemeral_session: sent.ephemeral_session,
                url: fresh.url,
                headers: fresh.headers,
                body: sent.body.clone(),
                family: sent.family,
                service_tier: sent.service_tier.clone(),
            }
        };
        let step_recovery = recovery.map(|recovery| StepAuthRecovery {
            recovery,
            sent_auth,
            rebuild: &rebuild,
        });
        // No cancellation arm here, deliberately: `drive_step_attempts` observes the token at
        // every await itself, so a cancel that lands mid-attempt still sends that attempt's
        // `cancelled` record before this task ends. An arm here would drop the attempt first
        // and lose the record. A closed receiver still ends everything at once, since nobody
        // is left to read a record.
        tokio::select! {
            biased;
            _ = tx.closed() => {}
            result = drive_step_attempts(
                &client,
                &clock,
                rendered.request,
                &rendered.request_digest,
                step_recovery,
                served_account_ref,
                normalizer_factory,
                &tx,
                &cancel,
                deadline,
                StepSeal {
                    seal: seal.as_ref(),
                    step_id,
                    cache_ttl_ms: crate::cache_warm::marker_ttl_ms(&frozen),
                },
                &retry_clock,
                Some(websocket.as_ref()),
                #[cfg(test)]
                &escape_client_builds,
            ) => {
                match result {
                    Ok((completion, retries_used)) => {
                        // Terminal step events are held behind this fence. Cancellation
                        // observed at any point before terminal publication wins over
                        // completion, so the loop never records the assembled step.
                        publish_completion(completion, retries_used, &tx, &cancel).await;
                    }
                    Err((error, retries_used)) => {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => {}
                            _ = tx.send(NormalizedEvent::Control(ProviderControl::Error {
                                error,
                                retries_used,
                            })) => {}
                        }
                    }
                }
            }
        }
    })
}

/// The header the Stainless SDK (which the Claude Code persona speaks as) numbers its attempts
/// in: 0 on the first send, then 1, 2, ... on each retry.
const STAINLESS_RETRY_COUNT: &str = "x-stainless-retry-count";

/// Number this attempt in a request that carries [`STAINLESS_RETRY_COUNT`], as the SDK does on
/// its own retries. Requests without the header are left alone. A resend of the same attempt
/// that is not a retry (an identity resend after a rejected request encoding) does not come
/// through here and keeps the count.
fn number_attempt(headers: &mut [(String, String)], attempt_index: usize) {
    for (name, value) in headers.iter_mut() {
        if name.eq_ignore_ascii_case(STAINLESS_RETRY_COUNT) {
            *value = attempt_index.to_string();
        }
    }
}

// clippy::too_many_arguments counts the request digest, passed beside the request because it is
// computed on the render worker; bundling it would only rename the same inputs.
#[allow(clippy::too_many_arguments)]
async fn drive_step_attempts<NormalizerFactory, Normalizer>(
    shared_client: &ProviderHttpClient,
    clock: &Clock,
    mut request: PreparedHttpRequest,
    request_digest: &RequestDigest,
    mut auth_recovery: Option<StepAuthRecovery<'_>>,
    mut served_account_ref: Option<String>,
    mut normalizer_factory: NormalizerFactory,
    tx: &mpsc::Sender<NormalizedEvent>,
    cancel: &CancellationToken,
    deadline: Option<Instant>,
    step_seal: StepSeal<'_>,
    retry_clock: &RetryClock,
    websocket: Option<&responses_websocket::Session>,
    #[cfg(test)] escape_client_builds: &AtomicUsize,
) -> Result<(Vec<NormalizedEvent>, u32), (ProviderError, u32)>
where
    NormalizerFactory: FnMut() -> Normalizer,
    Normalizer: StreamNormalizer + 'static,
{
    let mut previous_cause = None;
    let mut decode_retries = 0usize;
    let mut escape_used = false;
    let mut retry_wait_spent = Duration::ZERO;
    let mut phase_start = 0usize;
    let mut phase_refreshes = 0usize;
    let mut mid_response_closes = 0usize;
    let mut socket_phase_seen = false;
    let mut https_allowance_started = false;
    // How a token-refresh re-send counts (see the retry schedule above MAX_ATTEMPTS):
    // - Attempts: it is ONE send on top of MAX_ATTEMPTS, so the budget grows by one and a
    //   transient failure after the re-send still gets the full set of ordinary re-sends.
    // - Backoff: computed from the ordinal among ordinary attempts, so the curve is the one a
    //   step without a refresh would follow.
    // - Waiting: NOT exempt. Asking for a fresh token consumes the waiting allowance.
    // `credential_refresh` is the evidence the next attempt's record carries when it goes out
    // with a token refreshed inside this step, by either the 401 path or the pre-send check.
    let mut attempt_budget = MAX_ATTEMPTS;
    let mut credential_refresh: Option<CredentialRefresh> = None;
    // A step recovers from a 401 at most once, so a second 401 (even from the refreshed token)
    // ends the step. The expiry check before each attempt is not counted against this: it
    // replaces a token nobody rejected, and it runs at most once per attempt.
    let mut rejection_recovered = false;

    let mut attempt_index = 0usize;
    while attempt_index < attempt_budget {
        let attempt_started = retry_clock.mark();
        if cancel.is_cancelled() || tx.is_closed() {
            return Ok((Vec::new(), 0));
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            cancel.cancel();
            return Ok((Vec::new(), 0));
        }
        // A token that expires before this request and its stream are done would be refused
        // with a 401, costing an attempt and telling the credential source it was rejected
        // when it merely ran out. Ask for a fresh one first; the refresher answers `None`
        // straight away for a token with enough life left or one that never expires, and also
        // when it could not get a fresh one, in which case the held token goes out and the
        // 401 recovery below still covers it.
        if let Some(recovery) = auth_recovery.as_mut() {
            let refreshed = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok((Vec::new(), 0)),
                _ = tx.closed() => return Ok((Vec::new(), 0)),
                refreshed = recovery.recovery.refresher.refresh_expiring() => refreshed,
            };
            if let Some(refreshed) = refreshed {
                // The same account check as a 401 refresh: a token for another account is
                // never sent. Nothing was sent since the previous attempt, so the retries
                // used are those already spent.
                if let Some(mut error) = refreshed_account_changed_error(
                    recovery.recovery.frozen_account_ref.as_deref(),
                    &recovery.sent_auth,
                    &refreshed,
                ) {
                    redact_chatgpt_account_error(&mut error, request.family);
                    return Err((error, attempt_index.saturating_sub(1) as u32));
                }
                // Compared as a boolean only; neither side is ever formatted.
                if refreshed.auth != *recovery.sent_auth {
                    recovery.adopt(
                        refreshed.auth,
                        refreshed.account_ref,
                        &mut request,
                        &mut served_account_ref,
                    );
                    credential_refresh = Some(refreshed.evidence);
                }
            }
        }
        let normalizer = Box::new(normalizer_factory()) as Box<dyn StreamNormalizer>;

        let use_escape_client =
            !escape_used && previous_cause == Some(AttemptCause::TransportTransient);
        if use_escape_client {
            escape_used = true;
        }
        let escape_client = use_escape_client.then(|| {
            #[cfg(test)]
            escape_client_builds.fetch_add(1, Ordering::SeqCst);
            shared_client.http1_escape()
        });
        let client = escape_client.as_ref().unwrap_or(shared_client);

        // Every attempt that starts ends in exactly one attempt record, sent before anything
        // else about the step (the next attempt, the terminal completion, or the error).
        //
        // Once the shutdown seal has tripped no new attempt starts. The seal is aborting the
        // run task, so go quiet: ending the stream here would make the run loop read a failed
        // step and decide the run's terminal itself, and that terminal belongs to the seal.
        if !step_seal.begin_attempt() {
            tx.closed().await;
            return Ok((Vec::new(), 0));
        }
        number_attempt(&mut request.headers, attempt_index);
        step_seal.note_sent(&request.body, request_digest);
        let mut trace = AttemptTrace {
            transport: Some("https".into()),
            defer_failure_warning: true,
            #[cfg(test)]
            virtual_attempt_elapsed: retry_clock.virtual_attempt_elapsed,
            credential_refresh: credential_refresh.take(),
            served_account_ref: served_account_ref.clone(),
            // Known before anything is sent, so even an attempt that fails before its response
            // records that it could not have been measured.
            input_transformations_unmeasured: normalizer.input_transformations_unmeasured(),
            ..AttemptTrace::default()
        };
        let stop = tokio::select! {
            biased;
            _ = cancel.cancelled() => AttemptStop::Cancelled,
            _ = step_seal.tripped() => AttemptStop::Sealed,
            result = drive_selected_attempt(
                AttemptTransport {
                    client,
                    clock,
                    websocket,
                },
                &request,
                normalizer,
                tx,
                STREAM_IDLE_TIMEOUT,
                &mut trace,
            ) => AttemptStop::Ended(Box::new(result)),
        };
        let result = match stop {
            AttemptStop::Cancelled => {
                if let Some(websocket) = websocket {
                    websocket.close();
                }
                // A caller cancel is cooperative: the run loop keeps reading until this task
                // ends, so the in-flight attempt is recorded before the run's Cancelled
                // terminal.
                publish_attempt(
                    tx,
                    trace.into_attempt(attempt_index, request_digest, AttemptOutcome::Cancelled),
                )
                .await;
                step_seal.end_attempt();
                return Ok((Vec::new(), 0));
            }
            AttemptStop::Sealed => {
                if let Some(websocket) = websocket {
                    websocket.close();
                }
                // The seal, not the run loop, writes this record and then the run's
                // Interrupted terminal. Nothing is sent on the stream, and the stream stays
                // open until the aborted run task drops its receiver, so the run loop never
                // observes the stop.
                step_seal.hand_over(trace.into_attempt(
                    attempt_index,
                    request_digest,
                    AttemptOutcome::SealedInFlight,
                ));
                tx.closed().await;
                return Ok((Vec::new(), 0));
            }
            AttemptStop::Ended(result) => *result,
        };
        let outcome = match &result {
            // An empty completion means the receiver closed mid-stream: there is no one to
            // send a record to, and the attempt did not complete.
            Ok(completion) if completion.is_empty() => {
                step_seal.end_attempt();
                return Ok((Vec::new(), 0));
            }
            Ok(completion) if tool_args_cut(completion) => AttemptOutcome::ToolArgsTrailingGarbage,
            Ok(_) => AttemptOutcome::Complete,
            Err(failure) => failure.outcome.clone(),
        };
        let attempt_elapsed = retry_clock.attempt_elapsed(attempt_started);
        if trace.transport.as_deref() == Some("websocket") {
            socket_phase_seen = true;
        } else if socket_phase_seen && !https_allowance_started {
            // The session's six socket failures or ChatGPT HTTPS policy can select HTTPS
            // inside the driver. Start its own allowance even when socket sends remained.
            https_allowance_started = true;
            phase_start = attempt_index;
            phase_refreshes = 0;
            attempt_budget = phase_start + MAX_ATTEMPTS;
            retry_wait_spent = Duration::ZERO;
        }
        if result.is_err() && trace.response_len == 0 {
            retry_wait_spent += attempt_elapsed;
        }
        publish_attempt(
            tx,
            trace
                .clone()
                .into_attempt(attempt_index, request_digest, outcome),
        )
        .await;
        // Only after the record is queued to the run loop: a seal waiting on this attempt
        // then proceeds knowing the record took the ordinary path and is not its to write.
        step_seal.end_attempt();
        match result {
            Ok(completion) => return Ok((completion, attempt_index as u32)),
            Err(failure) => {
                if is_rejected_token(&failure.error) {
                    // Whatever happens next, a step recovers at most once, so a second 401
                    // (even from the refreshed token) ends the step.
                    let Some(recovery) = auth_recovery.as_mut().filter(|_| !rejection_recovered)
                    else {
                        warn_attempt_retry(&failure, &trace, "not_retryable", "authentication");
                        return Err((failure.error, attempt_index as u32));
                    };
                    rejection_recovered = true;
                    let refresh_started = retry_clock.mark();
                    let refreshed = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok((Vec::new(), 0)),
                        _ = tx.closed() => return Ok((Vec::new(), 0)),
                        refreshed = recovery.recovery.refresher.refresh_rejected() => refreshed,
                    };
                    let Some(refreshed) = refreshed else {
                        warn_attempt_retry(&failure, &trace, "not_retryable", "authentication");
                        return Err((failure.error, attempt_index as u32));
                    };
                    if let Some(mut error) = refreshed_account_changed_error(
                        recovery.recovery.frozen_account_ref.as_deref(),
                        &recovery.sent_auth,
                        &refreshed,
                    ) {
                        redact_chatgpt_account_error(&mut error, request.family);
                        warn_attempt_retry(&failure, &trace, "not_retryable", "account_changed");
                        return Err((error, attempt_index as u32));
                    }
                    // The same token again cannot succeed where it was just refused, so the
                    // 401 stands. Compared as a boolean only; neither side is ever formatted.
                    if refreshed.auth == *recovery.sent_auth {
                        warn_attempt_retry(
                            &failure,
                            &trace,
                            "not_retryable",
                            "unchanged_credential",
                        );
                        return Err((failure.error, attempt_index as u32));
                    }
                    recovery.adopt(
                        refreshed.auth,
                        refreshed.account_ref,
                        &mut request,
                        &mut served_account_ref,
                    );
                    credential_refresh = Some(refreshed.evidence);
                    attempt_budget += 1;
                    phase_refreshes += 1;
                    retry_wait_spent += retry_clock.elapsed_since(refresh_started);
                    warn_attempt_retry(&failure, &trace, "retrying", "credential_refresh");
                    // A new token needs no backoff; only the partial display is reset.
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok((Vec::new(), 0)),
                        result = tx.send(NormalizedEvent::Display(DisplayEvent::DisplayReset)) => {
                            if result.is_err() {
                                return Ok((Vec::new(), 0));
                            }
                        }
                    }
                    previous_cause = failure.cause;
                    attempt_index += 1;
                    continue;
                }
                // The ordinal among ordinary attempts, leaving out a token-refresh re-send, so
                // the backoff curve is the same with or without one.
                let ordinary_index = attempt_index - phase_start - phase_refreshes;
                if !failure.error.class.step_reissuable()
                    || failure.retry_forbidden
                    || (failure.cause == Some(AttemptCause::DeterministicDecode)
                        && decode_retries >= 1)
                {
                    warn_attempt_retry(&failure, &trace, "not_retryable", "retry_veto");
                    return Err((failure.error, attempt_index as u32));
                }

                let socket_attempt = trace.transport.as_deref() == Some("websocket")
                    && failure.cause == Some(AttemptCause::TransportTransient);
                if socket_attempt
                    && failure.outcome == AttemptOutcome::ChunkError
                    && trace.response_len > 0
                {
                    mid_response_closes += 1;
                } else {
                    mid_response_closes = 0;
                }
                let switching = socket_attempt
                    && (mid_response_closes >= 2 || attempt_index + 1 >= attempt_budget);
                if switching {
                    websocket
                        .expect("socket attempt has a session")
                        .switch_to_https();
                    // Keep the canonical request and global attempt ordinal; only the transport's
                    // bounded allowance restarts. Failed attempts never publish the assembled
                    // assistant message that the run loop uses to dispatch tools.
                    phase_start = attempt_index + 1;
                    https_allowance_started = true;
                    phase_refreshes = 0;
                    attempt_budget = phase_start + MAX_ATTEMPTS;
                    retry_wait_spent = Duration::ZERO;
                } else if attempt_index + 1 >= attempt_budget {
                    warn_attempt_retry(&failure, &trace, "attempts_exhausted", "attempt_limit");
                    return Err((failure.error, attempt_index as u32));
                }

                if failure.cause == Some(AttemptCause::DeterministicDecode) {
                    decode_retries += 1;
                }
                let delay = retry_delay(&failure.error, ordinary_index);
                if failure.error.status == Some(429)
                    && failure.error.retry_after_secs.is_some()
                    && deadline.is_some_and(|deadline| {
                        delay >= deadline.saturating_duration_since(Instant::now())
                    })
                {
                    warn_attempt_retry(
                        &failure,
                        &trace,
                        "not_retryable",
                        "retry_after_exceeds_deadline",
                    );
                    return Err((failure.error, attempt_index as u32));
                }
                if retry_wait_spent + delay > RETRY_WALL_BUDGET {
                    warn_attempt_retry(&failure, &trace, "wall_exhausted", "waiting_budget");
                    return Err((failure.error, attempt_index as u32));
                }

                warn_attempt_retry(
                    &failure,
                    &trace,
                    if switching {
                        "switching_to_https"
                    } else {
                        "retrying"
                    },
                    if switching && mid_response_closes >= 2 {
                        "consecutive_mid_response_closes"
                    } else if switching {
                        "socket_attempt_limit"
                    } else {
                        "transient_failure"
                    },
                );

                let wait_started = retry_clock.mark();
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok((Vec::new(), 0)),
                    _ = tx.closed() => return Ok((Vec::new(), 0)),
                    _ = retry_clock.sleep(delay) => {}
                }
                retry_wait_spent += retry_clock.elapsed_since(wait_started);
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok((Vec::new(), 0)),
                    result = tx.send(NormalizedEvent::Display(DisplayEvent::DisplayReset)) => {
                        if result.is_err() {
                            return Ok((Vec::new(), 0));
                        }
                    }
                }
                previous_cause = failure.cause;
            }
        }
        attempt_index += 1;
    }

    unreachable!("the fixed attempt set always returns from success or failure")
}

/// Whether a failed attempt means the provider rejected the token itself: an Auth-class HTTP
/// 401, including a ChatGPT socket upgrade refused with 401. A 403, on HTTPS or on a socket
/// upgrade, is not: an HTTPS 403 is an entitlement refusal, and a refused upgrade 403 is sent
/// again over HTTPS before it is classified. A ChatGPT backend 401 about its own upstream API key
/// is classified Transient before it gets here, so it does not qualify either.
fn is_rejected_token(error: &ProviderError) -> bool {
    error.class == ErrorClass::Auth && error.status == Some(401)
}

fn chatgpt_account(auth: &AuthHandle) -> Option<&str> {
    match auth {
        AuthHandle::Headers(headers) => headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("chatgpt-account-id"))
            .map(|(_, value)| value.as_str()),
        _ => None,
    }
}

fn redact_chatgpt_account_error(
    error: &mut ProviderError,
    family: Option<crate::spec::WireFamilyId>,
) {
    if family == Some(crate::spec::WireFamilyId::OpenAiResponsesChatgpt) {
        error.message = "credential account changed; re-admission required".into();
    }
}

/// A resumed step can receive a replacement token before its first request, just as a
/// running step can receive one after a 401. Both must preserve the account selected when
/// the run was admitted; unknown account metadata alone cannot establish a mismatch.
fn account_changed_error(frozen: Option<&str>, served: Option<&str>) -> Option<ProviderError> {
    let (Some(frozen), Some(served)) = (frozen, served) else {
        return None;
    };
    if frozen == served {
        return None;
    }
    Some(ProviderError {
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Auth,
        message: format!("credential account changed: frozen account {frozen}, served account {served}; re-admission required"),
        provider_code: Some("account_changed".into()),
        status: None,
        retry_after_secs: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    })
}

/// ChatGPT carries an account in its headers as well as in vault metadata. Neither may
/// change during a run: a re-login can replace the account behind the same credential id.
fn refreshed_account_changed_error(
    frozen: Option<&str>,
    sent_auth: &AuthHandle,
    refreshed: &crate::credential_refresh::RefreshedCredential,
) -> Option<ProviderError> {
    let sent_account = chatgpt_account(sent_auth);
    let frozen = frozen.or(sent_account);
    account_changed_error(frozen, refreshed.account_ref.as_deref()).or_else(|| {
        sent_account.and_then(|_| {
            // An absent JWT claim shapes an empty account header and must also be refused.
            account_changed_error(frozen, Some(chatgpt_account(&refreshed.auth).unwrap_or("")))
        })
    })
}

/// Keep each token together with its associated account when carrying auth between steps.
#[derive(Clone)]
struct ServedAuth {
    auth: Arc<AuthHandle>,
    account_ref: Option<String>,
}

/// Recovery after a 401 and the account selected at admission that recovery must preserve.
struct AuthRecovery {
    frozen_account_ref: Option<String>,
    refresher: Arc<dyn CredentialRefresher>,
    current: Arc<std::sync::RwLock<ServedAuth>>,
}

/// [`AuthRecovery`] for one step: the credential this step was sent with, and how to rebuild
/// the request's HTTP shape around a fresh one.
struct StepAuthRecovery<'a> {
    recovery: AuthRecovery,
    /// The token this step's request currently carries, moved by every in-step refresh.
    sent_auth: Arc<AuthHandle>,
    rebuild: &'a (dyn Fn(&AuthHandle, &PreparedHttpRequest) -> PreparedHttpRequest + Send + Sync),
}

impl StepAuthRecovery<'_> {
    /// Moves this step's request onto a freshly served token. Only the url and headers are
    /// rebuilt; the body bytes, and so the request digest, stay exactly as rendered. Later
    /// steps of this run go out with the fresh token too.
    fn adopt(
        &mut self,
        auth: AuthHandle,
        account_ref: Option<String>,
        request: &mut PreparedHttpRequest,
        served_account_ref: &mut Option<String>,
    ) {
        *request = (self.rebuild)(&auth, request);
        served_account_ref.clone_from(&account_ref);
        let auth = Arc::new(auth);
        self.sent_auth = Arc::clone(&auth);
        *self
            .recovery
            .current
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = ServedAuth { auth, account_ref };
    }
}

/// How one attempt's `select!` ended.
enum AttemptStop {
    /// The caller's cancel (or the watchdog) won.
    Cancelled,
    /// The module's shutdown seal won.
    Sealed,
    /// The attempt ran to its own end, successful or not.
    Ended(Box<Result<Vec<NormalizedEvent>, AttemptFailure>>),
}

/// The shutdown seal for one step, if the caller supplied one, plus the step id the seal's
/// record needs. Without a seal every method is a no-op and `tripped` never resolves.
#[derive(Clone, Copy)]
struct StepSeal<'a> {
    seal: Option<&'a ShutdownSeal>,
    step_id: StepId,
    /// The prompt-cache lifetime the frozen config's markers declare, which each send restarts.
    cache_ttl_ms: Option<u64>,
}

impl StepSeal<'_> {
    fn begin_attempt(&self) -> bool {
        self.seal.is_none_or(ShutdownSeal::begin_attempt)
    }

    /// Hand the seal the exact bytes an attempt is starting to send, so a cache warm during the
    /// run re-sends them and the session's cache clock restarts now.
    fn note_sent(&self, body: &Bytes, digest: &RequestDigest) {
        if let Some(seal) = self.seal {
            seal.note_sent(
                broca_core::SentRequest {
                    step_id: self.step_id,
                    body: Arc::new(body.clone()),
                    sha256: digest.sha256.clone(),
                    sent_at_ms: now_millis(),
                },
                self.cache_ttl_ms,
            );
        }
    }

    fn end_attempt(&self) {
        if let Some(seal) = self.seal {
            seal.end_attempt();
        }
    }

    async fn tripped(&self) {
        match self.seal {
            Some(seal) => seal.tripped().await,
            None => std::future::pending().await,
        }
    }

    fn hand_over(&self, attempt: ModelAttempt) {
        if let Some(seal) = self.seal {
            seal.hand_over(self.step_id, attempt);
        }
    }
}

fn retry_delay(error: &ProviderError, failed_attempt_index: usize) -> Duration {
    if let Some(seconds) = error.retry_after_secs {
        let capped_seconds = seconds.max(0.0).min(MAX_RETRY_AFTER_DELAY.as_secs_f64());
        return Duration::from_secs_f64(capped_seconds);
    }

    let doublings = u32::try_from(failed_attempt_index)
        .unwrap_or(u32::MAX)
        .min(16);
    let nominal = BASE_RETRY_DELAY
        .saturating_mul(1u32 << doublings)
        .min(MAX_BACKOFF_DELAY);
    let factor = rand::rng().random_range((1.0 - BACKOFF_JITTER)..=(1.0 + BACKOFF_JITTER));
    nominal.mul_f64(factor)
}

fn warn_attempt_retry(
    failure: &AttemptFailure,
    trace: &AttemptTrace,
    decision: &str,
    reason: &str,
) {
    if trace.ephemeral_session && trace.defer_failure_warning {
        // Ephemeral sessions prohibit retained content: log only typed error tags and local
        // timing, not provider-controlled stream diagnostics.
        if trace.chunk_warning.is_some() || trace.transport.as_deref() == Some("websocket") {
            warn_retry_metadata(failure, trace, decision, reason);
        }
        return;
    }
    if let Some(chunk) = &trace.chunk_warning {
        if trace.ephemeral_session {
            warn_private_failure(&failure.error, "model attempt ended chunk_error");
        } else {
            tracing::warn!(
                target: "provider",
                outcome = ?failure.outcome,
                class = ?failure.error.class,
                provider_code = failure.error.provider_code.as_deref().unwrap_or("none"),
                elapsed_ms = %trace.elapsed_since_send().map(|elapsed| elapsed.as_millis().to_string()).unwrap_or_else(|| "unknown".into()),
                response_bytes = trace.response_len,
                last_event_type = chunk.last_event_type,
                last_content_ms = %trace.last_content_elapsed.map(|elapsed| elapsed.as_millis().to_string()).unwrap_or_else(|| "none".into()),
                keepalives_after_content = trace.keepalives_after_content,
                event_count = trace.content_event_count,
                event_type_counts = chunk.event_type_counts,
                transport = trace.transport.as_deref().unwrap_or("https"),
                close_code = trace.websocket_close_code.as_ref().map(|code| code.load(Ordering::Acquire)).filter(|code| *code != 0),
                error_chain = chunk.error_chain,
                socket_generation = trace.socket_generation,
                socket_reused = trace.socket_reused,
                socket_age_ms = trace.socket_age_ms(),
                socket_since_last_completed_ms = trace.socket_since_last_completed_ms(),
                output_seen = trace.output_seen,
                retry_decision = decision,
                retry_reason = reason,
                "model attempt ended chunk_error"
            );
        }
    } else if trace.transport.as_deref() == Some("websocket") {
        responses_websocket::warn_failure_decision(failure, trace, decision, reason);
    } else if failure.outcome != AttemptOutcome::ProviderError {
        warn_retry_metadata(failure, trace, decision, reason);
    }
}

fn warn_retry_metadata(
    failure: &AttemptFailure,
    trace: &AttemptTrace,
    decision: &str,
    reason: &str,
) {
    tracing::warn!(target: "provider",
        outcome = ?failure.outcome,
        class = ?failure.error.class,
        transport = trace.transport.as_deref().unwrap_or("https"),
        socket_generation = trace.socket_generation,
        socket_reused = trace.socket_reused,
        socket_age_ms = trace.socket_age_ms(),
        socket_since_last_completed_ms = trace.socket_since_last_completed_ms(),
        output_seen = trace.output_seen,
        retry_decision = decision,
        retry_reason = reason,
        "model attempt ended"
    );
}

/// Send one attempt's evidence to the run loop. A closed receiver means nobody is left to
/// record it, so the send error is ignored.
async fn publish_attempt(tx: &mpsc::Sender<NormalizedEvent>, attempt: ModelAttempt) {
    let _ = tx
        .send(NormalizedEvent::Control(ProviderControl::AttemptFinished {
            attempt,
        }))
        .await;
}

async fn publish_completion(
    completion: Vec<NormalizedEvent>,
    retries_used: u32,
    tx: &mpsc::Sender<NormalizedEvent>,
    cancel: &CancellationToken,
) {
    if completion.is_empty() {
        return;
    }

    let completion = completion
        .into_iter()
        .map(|event| match event {
            NormalizedEvent::Control(ProviderControl::Finished {
                usage,
                finish_reason,
                ..
            }) => NormalizedEvent::Control(ProviderControl::Finished {
                usage,
                finish_reason,
                retries_used,
            }),
            event => event,
        })
        .collect::<Vec<_>>();

    // Reserve the assembled message and final Finished control as one batch. Cancellation while
    // waiting publishes nothing; after reservation, callers always receive both controls.
    let permits = tokio::select! {
        biased;
        _ = cancel.cancelled() => return,
        permits = tx.reserve_many(completion.len()) => match permits {
            Ok(permits) => permits,
            Err(_) => return,
        },
    };
    for (permit, event) in permits.zip(completion) {
        permit.send(event);
    }
}

#[cfg(test)]
async fn drive_stream(
    client: &ProviderHttpClient,
    request: &PreparedHttpRequest,
    normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    drive_stream_with_header_timeout(client, request, normalizer, tx, STREAM_IDLE_TIMEOUT).await
}

#[cfg(test)]
async fn drive_stream_with_header_timeout(
    client: &ProviderHttpClient,
    request: &PreparedHttpRequest,
    normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
    header_timeout: Duration,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    let mut trace = AttemptTrace::default();
    let clock = Clock::default();
    drive_attempt(
        client,
        request,
        &clock,
        normalizer,
        tx,
        header_timeout,
        &mut trace,
    )
    .await
}

/// Drive ONE HTTP attempt, recording what it received into `trace` as it goes.
///
/// Borrows the prepared request because every retry attempt drives it again. The url and
/// headers are only ever read here, so cloning them per attempt copied a ChatGPT bearer plus
/// its account, session, originator and user-agent headers for nothing. Only the body needs an
/// owned handle, and `Bytes` gives that with a refcount bump rather than a copy.
async fn drive_attempt(
    client: &ProviderHttpClient,
    request: &PreparedHttpRequest,
    clock: &Clock,
    mut normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
    header_timeout: Duration,
    trace: &mut AttemptTrace,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    trace.ephemeral_session = request.ephemeral_session;
    if request.ephemeral_session {
        normalizer.private_error_diagnostics();
    }
    trace.transport = Some("https".into());
    let anthropic_api_host = request_gzip::is_anthropic_api_host(&request.url);
    let gzip = encode_request_gzip(client, anthropic_api_host, &request.body).await;
    // What the trace held before anything was sent, so a refused compressed send leaves no
    // trace of itself in the attempt's record: the uncompressed re-send is the attempt.
    let unsent_trace = gzip.is_some().then(|| trace.clone());
    let mut resp = send_attempt(
        client,
        request,
        anthropic_api_host,
        gzip,
        header_timeout,
        trace,
    )
    .await?;
    // A 400 from Anthropic to a compressed request has its body read to tell a JSON parse
    // failure from an ordinary invalid request. When it is ordinary, that text is the error.
    let mut error_text = None;
    if let Some(unsent_trace) = unsent_trace {
        let status = resp.status().as_u16();
        let rejection = match request_gzip::screen_headers(status, resp.headers()) {
            request_gzip::HeaderVerdict::Pass => None,
            request_gzip::HeaderVerdict::Rejected(rejection) => Some(rejection),
            request_gzip::HeaderVerdict::NeedsBody => {
                let text = read_error_text(&mut resp, trace).await;
                let rejection = request_gzip::body_shows_invalid_json(&text)
                    .then_some(request_gzip::Rejection::InvalidJsonAtOrigin);
                if rejection.is_none() {
                    error_text = Some(text);
                }
                rejection
            }
        };
        if let Some(rejection) = rejection {
            // Claude Code's fallback: latch gzip off, then send the same request once more
            // with the unpadded identity body and no `content-encoding`. The re-send happens
            // inside this attempt, so it uses none of the step's retry attempts; its time
            // still counts toward the step's waiting allowance like any attempt time. A
            // network error on the compressed send never gets here: it fails the attempt.
            client.request_gzip.latch(status, rejection);
            drop(resp);
            *trace = unsent_trace;
            resp = send_attempt(
                client,
                request,
                anthropic_api_host,
                None,
                header_timeout,
                trace,
            )
            .await?;
        }
    }

    let status = resp.status();
    trace.http_status = Some(status.as_u16());
    trace.account_observation.ratelimit_headers = ratelimit_unified_headers(resp.headers());
    trace.content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
    if !status.is_success() {
        let code = status.as_u16();
        let retry_after_secs = parse_retry_after_header(resp.headers());
        // Read before the body consumes the response: an account-limit refusal is told apart
        // from an ordinary rate limit only by its headers.
        let headers = response_headers(resp.headers());
        let retry_forbidden = resp
            .headers()
            .get("x-should-retry")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("false"));
        let text = match error_text {
            Some(text) => text,
            None => read_error_text(&mut resp, trace).await,
        };
        let error = classify_http_error_in_family(
            code,
            &text,
            retry_after_secs,
            request
                .service_tier
                .as_ref()
                .map(|(family, tier)| (*family, tier)),
            request.family,
            headers.get("x-amzn-errortype"),
        );
        let error = match request.family {
            Some(family) => {
                let error = classify_chatgpt_upstream_key_rejection(family, code, &text, error);
                classify_account_limit_exhausted(
                    family,
                    code,
                    &headers,
                    &text,
                    UNIX_EPOCH + Duration::from_millis(clock.now_ms()),
                    error,
                )
            }
            None => error,
        };
        // An exhausted account refills in hours or days. Re-sending within the step cannot
        // succeed, and honoring the wait would park the run far past any step budget.
        let retry_forbidden = retry_forbidden
            || error.is_account_limit_exhausted()
            || error.is_billing_exhausted()
            || error.is_credit_insufficient();
        if error.is_account_limit_exhausted() {
            trace.account_observation.limit = Some(broca_protocol::AttemptAccountLimit {
                resets_at_ms: error.resets_at_ms,
                resets_at_source: error.resets_at_source.clone(),
                limit_window: error.limit_window.clone(),
            });
        }
        return Err(AttemptFailure::http(error, retry_forbidden));
    }

    let mut normalizer = normalizer;
    normalizer.observe_response_headers(&response_headers(resp.headers()));
    let result =
        drive_body_stream_traced(resp.bytes_stream(), normalizer, tx, MAX_STREAM_BYTES, trace)
            .await;
    apply_chatgpt_response_deadline(result, request, trace)
}

/// Send the request once and wait for its response headers. `gzip` is the compressed body to
/// send instead of the identity one; see [`attempt_request_builder`] for the headers.
async fn send_attempt(
    client: &ProviderHttpClient,
    request: &PreparedHttpRequest,
    anthropic_api_host: bool,
    gzip: Option<Bytes>,
    header_timeout: Duration,
    trace: &mut AttemptTrace,
) -> Result<reqwest::Response, AttemptFailure> {
    let builder = attempt_request_builder(
        client,
        &request.url,
        &request.headers,
        anthropic_api_host,
        gzip.is_some(),
    );
    let body = gzip.unwrap_or_else(|| request.body.clone());
    // Stamped as the request is handed to the client: a provider's prompt cache counts its
    // lifetime from the request, and after a restart this stamp is what dates it.
    trace.sent_at_ms = Some(now_millis());
    trace.sent_at = Some(std::time::Instant::now());
    tokio::time::timeout(header_timeout, builder.body(body).send())
        .await
        .map_err(|_| {
            AttemptFailure::transport(
                stream_timeout_error("waiting for response headers"),
                AttemptOutcome::HeaderTimeout,
            )
        })?
        .map_err(|e| {
            // A connect error means no request bytes reached a server. Anything else before a
            // status line leaves it unknown whether the body was written, so it is not guessed.
            let outcome = if e.is_connect() {
                AttemptOutcome::ConnectFailed
            } else {
                AttemptOutcome::NoStatus
            };
            AttemptFailure::transport(transport_error(&e.to_string()), outcome)
        })
}

/// Read a non-2xx body for its diagnostic text into `trace`, leaving the response's headers
/// readable.
async fn read_error_text(resp: &mut reqwest::Response, trace: &mut AttemptTrace) -> String {
    let chunks = futures::stream::unfold(resp, |resp| async move {
        resp.chunk().await.transpose().map(|chunk| (chunk, resp))
    });
    match collect_error_body_traced(chunks, MAX_HTTP_ERROR_BODY_READ_BYTES, trace).await {
        Ok(text) => text,
        Err(failure) => {
            // HTTP headers remain authoritative when the diagnostic body is broken.
            // Preserve their classification and retry metadata instead of treating the
            // body-read problem as a network failure.
            format!("<error body unreadable: {}>", failure.error.message)
        }
    }
}

fn apply_chatgpt_response_deadline(
    result: Result<Vec<NormalizedEvent>, AttemptFailure>,
    request: &PreparedHttpRequest,
    trace: &AttemptTrace,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    result.map_err(|failure| {
            if failure.outcome == AttemptOutcome::ChunkError
                && request.family == Some(crate::spec::WireFamilyId::OpenAiResponsesChatgpt)
                // The ChatGPT backend's 900-second response limit applies only to HTTPS responses;
                // a WebSocket response can stream past it, so a socket cut after 895 s is an ordinary transport cut.
                && matches!(trace.transport.as_deref(), None | Some("https"))
                && trace.elapsed_since_send().is_some_and(|elapsed| elapsed >= CHATGPT_RESPONSE_DEADLINE_THRESHOLD)
            {
                let mut error = transport_error("the ChatGPT backend ended the response at its 900 s limit; send a smaller turn or lower reasoning effort");
                error.class = ErrorClass::Transient;
                error.provider_code = Some("response_deadline".into());
                AttemptFailure::transport_without_reissue(error, AttemptOutcome::ChunkError)
            } else {
                failure
            }
        })
}

struct AttemptTransport<'a> {
    client: &'a ProviderHttpClient,
    clock: &'a Clock,
    websocket: Option<&'a responses_websocket::Session>,
}

async fn drive_selected_attempt(
    transport: AttemptTransport<'_>,
    request: &PreparedHttpRequest,
    normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
    header_timeout: Duration,
    trace: &mut AttemptTrace,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    if request.family == Some(crate::spec::WireFamilyId::OpenAiResponsesChatgpt) {
        if let Some(websocket) = transport.websocket {
            return responses_websocket::drive(
                websocket,
                transport.client,
                request,
                transport.clock,
                normalizer,
                tx,
                trace,
            )
            .await;
        }
    }
    drive_attempt(
        transport.client,
        request,
        transport.clock,
        normalizer,
        tx,
        header_timeout,
        trace,
    )
    .await
}

/// The response headers a normalizer may read, as lowercase names with UTF-8 values; a value
/// that is not valid UTF-8 is left out rather than guessed at.
fn response_headers(headers: &reqwest::header::HeaderMap) -> crate::wire::ResponseHeaders {
    crate::wire::ResponseHeaders::new(headers.iter().filter_map(|(name, value)| {
        value
            .to_str()
            .ok()
            .map(|value| (name.as_str().to_owned(), value.to_owned()))
    }))
}

#[cfg(test)]
async fn collect_error_body<S, B, E>(stream: S, limit: usize) -> Result<String, AttemptFailure>
where
    S: futures::Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::error::Error,
{
    collect_error_body_traced(stream, limit, &mut AttemptTrace::default()).await
}

/// Read a non-2xx body for its diagnostic text, stopping at `limit` bytes. When the cap stops
/// the read, the trace digests exactly the first `limit` bytes and records the cap, so the
/// evidence does not depend on where the network happened to split chunks.
///
/// Bounded by `STREAM_IDLE_TIMEOUT` (the transport silence bound used for response headers and
/// event delivery) so an upstream server that stalls while sending an error body does not block
/// the attempt indefinitely, allowing error classification and retry handling to proceed using
/// the response headers.
async fn collect_error_body_traced<S, B, E>(
    stream: S,
    limit: usize,
    trace: &mut AttemptTrace,
) -> Result<String, AttemptFailure>
where
    S: futures::Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::error::Error,
{
    let read_body = async {
        futures::pin_mut!(stream);
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| {
                AttemptFailure::transport(
                    transport_error(&format!("error body: {error}")),
                    AttemptOutcome::HttpError,
                )
            })?;
            let bytes = chunk.as_ref();
            let room = limit.saturating_sub(body.len());
            if bytes.len() > room {
                trace.observe(&bytes[..room]);
                trace.response_cap = Some(limit as u64);
                return Err(AttemptFailure::decode(
                    stream_limit_error(&format!(
                        "provider error body exceeds the {limit}-byte limit"
                    )),
                    AttemptOutcome::HttpError,
                ));
            }
            trace.observe(bytes);
            body.extend_from_slice(bytes);
        }
        Ok(String::from_utf8_lossy(&body).into_owned())
    };

    match tokio::time::timeout(STREAM_IDLE_TIMEOUT, read_body).await {
        Ok(result) => result,
        Err(_) => Err(AttemptFailure::transport(
            stream_timeout_error("reading error body"),
            AttemptOutcome::HttpError,
        )),
    }
}

#[cfg(test)]
async fn drive_body_stream<S, B, E>(
    stream: S,
    normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
) -> Result<Vec<NormalizedEvent>, AttemptFailure>
where
    S: futures::Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::error::Error,
{
    drive_body_stream_with_limit(stream, normalizer, tx, MAX_STREAM_BYTES).await
}

#[cfg(test)]
async fn drive_body_stream_with_limit<S, B, E>(
    stream: S,
    normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
    max_stream_bytes: usize,
) -> Result<Vec<NormalizedEvent>, AttemptFailure>
where
    S: futures::Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::error::Error,
{
    let mut trace = AttemptTrace::default();
    drive_body_stream_traced(stream, normalizer, tx, max_stream_bytes, &mut trace).await
}

async fn drive_body_stream_traced<S, B, E>(
    stream: S,
    mut normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
    max_stream_bytes: usize,
    trace: &mut AttemptTrace,
) -> Result<Vec<NormalizedEvent>, AttemptFailure>
where
    S: futures::Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::error::Error,
{
    futures::pin_mut!(stream);
    let mut stream_bytes = 0usize;
    if trace.transport.as_deref() == Some("websocket") {
        normalizer.websocket_telemetry();
    }
    let mut progress_marker = normalizer.progress_marker();
    // A grammar with a liveness marker gets two deadlines: `idle` resets on any complete
    // record including keepalive comments, `content` only on content. Without one, `idle` is
    // the single deadline reset by content, and `content` is never armed, which is the rule
    // those grammars have always had.
    let mut liveness_marker = normalizer.liveness_marker();
    let split_deadlines = liveness_marker.is_some();
    let idle = tokio::time::sleep(STREAM_IDLE_TIMEOUT);
    tokio::pin!(idle);
    let content = tokio::time::sleep(STREAM_CONTENT_TIMEOUT);
    tokio::pin!(content);
    let mut completion = Vec::new();
    let mut rate_window = stream_floor_from_env().map(StreamRateWindow::new);

    loop {
        let next = tokio::select! {
            biased;
            _ = &mut idle => return Err(AttemptFailure::transport(
                stream_timeout_error(if split_deadlines {
                    "waiting for any complete SSE record"
                } else {
                    "waiting for a complete event"
                }),
                AttemptOutcome::IdleTimeout,
            )),
            _ = &mut content, if split_deadlines => return Err(AttemptFailure::transport(
                content_timeout_error(),
                AttemptOutcome::ContentTimeout,
            )),
            next = stream.next() => next,
        };
        let Some(chunk) = next else { break };
        let bytes = chunk.map_err(|e| {
            let failure = AttemptFailure::transport(
                transport_error(&format!("stream chunk: {e}")),
                AttemptOutcome::ChunkError,
            );
            trace.chunk_warning = Some(ChunkWarning {
                last_event_type: crate::sse::safe_event_type_name(
                    normalizer.last_event_type().unwrap_or("unknown"),
                )
                .into(),
                event_type_counts: format_event_type_counts(normalizer.event_type_counts()),
                error_chain: if trace.ephemeral_session {
                    String::new()
                } else {
                    transport_error_chain(&e)
                },
            });
            if !trace.defer_failure_warning {
                warn_attempt_retry(&failure, trace, "not_retryable", "standalone_attempt");
            }
            trace.chunk_error_warned = true;
            failure
        })?;
        let chunk = bytes.as_ref();
        // Digest the raw chunk as received, before any limit or parse decision, so the record
        // covers every byte that arrived even when this chunk ends the attempt.
        trace.observe(chunk);
        if chunk.len() > max_stream_bytes.saturating_sub(stream_bytes) {
            return Err(AttemptFailure::decode(
                stream_limit_error(&format!(
                    "provider stream exceeds the {max_stream_bytes}-byte per-step limit"
                )),
                AttemptOutcome::ByteLimit,
            ));
        }
        stream_bytes += chunk.len();
        let websocket = trace.transport.as_deref() == Some("websocket");
        if websocket {
            if let Some(error) = responses_websocket::wrapped_error(chunk, trace) {
                return Err(error);
            }
        }
        let framed;
        let normalized = if websocket {
            framed = responses_websocket::frame_record(chunk);
            framed.as_slice()
        } else {
            chunk
        };
        let result = normalizer.push(normalized);
        let progress = normalizer.content_progress();
        if progress.content_bytes != trace.content_progress.content_bytes {
            trace.last_content_elapsed = trace.elapsed_since_send();
        }
        *trace.content_progress = progress;
        let events = result
            .map_err(|error| AttemptFailure::normalize(error, AttemptOutcome::DecodeError))?;
        trace.output_seen |= events.iter().any(|event| {
            matches!(event,
            NormalizedEvent::Display(DisplayEvent::TextDelta { delta, .. }
                | DisplayEvent::ReasoningDelta { delta, .. }
                | DisplayEvent::ToolInputDelta { delta, .. }) if !delta.is_empty())
        });
        if trace.input_transformations.is_none() {
            trace.input_transformations = normalizer.input_transformations();
        }
        trace.input_transformations_unmeasured = normalizer.input_transformations_unmeasured();
        let next_marker = normalizer.progress_marker();
        let made_progress = next_marker != progress_marker || !events.is_empty();
        if next_marker != progress_marker {
            trace.content_event_count = next_marker;
        }
        trace.keepalives_after_content = normalizer.keepalives_after_content().unwrap_or_default();
        progress_marker = next_marker;
        // Raw bytes are never liveness: only a marker that counts complete records moves.
        let next_liveness = normalizer.liveness_marker();
        let alive = made_progress || next_liveness != liveness_marker;
        liveness_marker = next_liveness;
        let now = tokio::time::Instant::now();
        if alive {
            idle.as_mut().reset(now + STREAM_IDLE_TIMEOUT);
        }
        if made_progress && split_deadlines {
            content.as_mut().reset(now + STREAM_CONTENT_TIMEOUT);
        }
        if let Some(window) = rate_window.as_mut() {
            window.observe(&events);
            if let Some(rate) = window.breach() {
                // Transient (so the router sees provider health) and non-reissuable (so this loop
                // does not burn the attempt budget on the same degraded endpoint first). The
                // measured rate rides in the message because a router deciding between providers
                // needs the magnitude, not just the verdict.
                return Err(AttemptFailure::transport_without_reissue(
                    stream_too_slow_error(rate, window.floor),
                    AttemptOutcome::RateFloor,
                ));
            }
        }
        let cut = tool_args_cut(&events);
        if !record_attempt_events(events, &mut completion, tx).await? {
            return Ok(Vec::new());
        }
        if cut {
            return validate_attempt_completion(completion);
        }
        if websocket
            && matches!(
                normalizer.last_event_type(),
                Some("response.completed" | "response.incomplete")
            )
        {
            break;
        }
    }

    // `finish` runs only at a clean end of body, and fails when the grammar cannot close the
    // step from what arrived (OpenAI Chat and Gemini report a missing terminal chunk here,
    // where Anthropic reports nothing and the completion check below catches it). Recording
    // both as `eof_before_completion` keeps a body cut at the same point reading the same
    // across wire families.
    let result = normalizer.finish();
    let progress = normalizer.content_progress();
    if progress.content_bytes != trace.content_progress.content_bytes {
        trace.last_content_elapsed = trace.elapsed_since_send();
    }
    *trace.content_progress = progress;
    let final_events = result
        .map_err(|error| AttemptFailure::normalize(error, AttemptOutcome::EofBeforeCompletion))?;
    if !record_attempt_events(final_events, &mut completion, tx).await? {
        return Ok(Vec::new());
    }
    validate_attempt_completion(completion)
}

async fn record_attempt_events(
    events: Vec<NormalizedEvent>,
    completion: &mut Vec<NormalizedEvent>,
    tx: &mpsc::Sender<NormalizedEvent>,
) -> Result<bool, AttemptFailure> {
    for event in events {
        match event {
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                return Err(AttemptFailure::control(error));
            }
            event if is_step_completion_event(&event) => completion.push(event),
            event => {
                if tx.send(event).await.is_err() {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

fn validate_attempt_completion(
    completion: Vec<NormalizedEvent>,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    let has_message = completion.iter().any(|event| {
        matches!(
            event,
            NormalizedEvent::Control(ProviderControl::AssistantMessage { .. })
        )
    });
    let has_finished = completion.iter().any(|event| {
        matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Finished { .. })
        )
    });
    if has_message && has_finished {
        return Ok(completion);
    }

    let missing = match (has_message, has_finished) {
        (false, false) => "an assembled assistant message and Finished control",
        (false, true) => "an assembled assistant message",
        (true, false) => "a Finished control",
        (true, true) => unreachable!(),
    };
    Err(AttemptFailure::transport(
        transport_error(&format!("provider stream ended without {missing}")),
        AttemptOutcome::EofBeforeCompletion,
    ))
}

fn is_step_completion_event(event: &NormalizedEvent) -> bool {
    matches!(
        event,
        NormalizedEvent::Control(
            ProviderControl::AssistantMessage { .. }
                | ProviderControl::ToolArgsTrailingGarbage { .. }
                | ProviderControl::StepFinished { .. }
                | ProviderControl::Finished { .. }
        )
    )
}

fn tool_args_cut(events: &[NormalizedEvent]) -> bool {
    events.iter().any(|event| {
        matches!(
            event,
            NormalizedEvent::Control(ProviderControl::ToolArgsTrailingGarbage { .. })
        )
    })
}

fn stream_timeout_error(activity: &str) -> ProviderError {
    ProviderError {
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Transient,
        message: format!("provider stream idle: {activity} for 300s"),
        status: None,
        retry_after_secs: None,
        provider_code: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

fn content_timeout_error() -> ProviderError {
    ProviderError {
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Transient,
        message: format!(
            "provider stream idle: waiting for content: only keepalive comments for {}s",
            STREAM_CONTENT_TIMEOUT.as_secs()
        ),
        status: None,
        retry_after_secs: None,
        provider_code: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

fn normalize_error(e: NormalizeError) -> ProviderError {
    // A response we could not parse — a truncated/garbled stream instance or an
    // over-limit buffer — is transient by the safe-direction rule: the next attempt
    // gets a fresh stream. Nothing durable recorded a partial step, so a retry is safe.
    //
    // The one exception is a tool call cut by the output-token ceiling, which the provider's
    // own stop reason proves. That class is permanent, and the attempt loop stops on it
    // because a permanent class is not step-reissuable: the retry would carry the same cap
    // and be cut at the same place. Its message names the ceiling instead of the JSON error,
    // so a caller can tell it from a malformed stream.
    let class = e.class();
    let provider_code = match &e {
        NormalizeError::OutputCeiling(_) => Some("output_ceiling".into()),
        _ => None,
    };
    let message = match &e {
        NormalizeError::OutputCeiling(cut) => cut.to_string(),
        _ => format!("normalize response: {e}"),
    };
    ProviderError {
        provider_subcode: None,
        refusal: None,
        class,
        message,
        status: None,
        retry_after_secs: None,
        provider_code,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

/// A provider that streamed real content, steadily, below the configured floor.
///
/// Classified `Transient` so it reaches the caller's router as a provider-health signal rather than
/// a run failure: the useful response is to route around this provider, which the router can only
/// do if it learns the provider is degraded. `ProviderUnavailable` would collapse to transient at
/// the shared boundary anyway and says less.
fn stream_too_slow_error(measured_bytes_per_sec: f64, floor: StreamFloor) -> ProviderError {
    ProviderError {
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Transient,
        message: format!(
            "provider stream below the configured floor: {measured_bytes_per_sec:.1} content \
             bytes/sec over {}s, floor {:.1}",
            floor.window.as_secs(),
            floor.bytes_per_sec
        ),
        status: None,
        retry_after_secs: None,
        provider_code: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

fn transport_error(message: &str) -> ProviderError {
    ProviderError {
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::ProviderUnavailable,
        message: message.to_owned(),
        status: None,
        retry_after_secs: None,
        provider_code: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

fn stream_limit_error(message: &str) -> ProviderError {
    ProviderError {
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Transient,
        message: message.to_owned(),
        status: None,
        retry_after_secs: None,
        provider_code: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

/// Parse `Retry-After` in either delay-seconds or HTTP-date form.
/// Read a retry delay from the response headers.
///
/// `retry-after-ms` is checked first. It is non-standard, but Anthropic and OpenAI both send
/// it and both official SDKs prefer it, because whole-second `Retry-After` cannot express a
/// sub-second delay and rounds a 300 ms hint up to a full second of idle waiting.
///
/// Deliberately NOT read here: the `x-ratelimit-*` and `anthropic-ratelimit-*` families. Those
/// carry quota telemetry (limit, remaining, and a reset instant for a window) rather than a
/// delay for THIS request, and their reset instant is when the window refills, not when a
/// retry would succeed. Treating one as a retry delay would park a run until a window boundary
/// that has no relationship to the refusal.
fn parse_retry_after_header(headers: &reqwest::header::HeaderMap) -> Option<f64> {
    if let Some(milliseconds) = headers
        .get("retry-after-ms")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|milliseconds| milliseconds.is_finite() && *milliseconds >= 0.0)
    {
        return Some(milliseconds / 1_000.0);
    }

    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    parse_retry_after_value(value, SystemTime::now())
}

fn parse_retry_after_value(value: &str, now: SystemTime) -> Option<f64> {
    let value = value.trim();
    if let Some(seconds) = value
        .parse::<f64>()
        .ok()
        .filter(|&seconds| seconds.is_finite() && seconds >= 0.0)
    {
        return Some(seconds);
    }

    let retry_at = httpdate::parse_http_date(value).ok()?;
    Some(
        retry_at
            .duration_since(now)
            .unwrap_or_default()
            .as_secs_f64(),
    )
}

fn extract_provider_code(text: &str) -> Option<String> {
    let body = parse_http_error_body(text)?;
    let error = body.get("error").filter(|v| v.is_object()).unwrap_or(&body);
    error
        .get("code")
        .and_then(Value::as_str)
        .or_else(|| error.get("type").and_then(Value::as_str))
        .or_else(|| error.get("status").and_then(Value::as_str))
        .or_else(|| body.get("__type").and_then(Value::as_str))
        .map(|code| code.rsplit('#').next().unwrap_or(code))
        .map(str::to_owned)
}

fn parse_http_error_body(text: &str) -> Option<Value> {
    let mut body: Value = serde_json::from_str(text).ok()?;
    // Google's streaming endpoint can answer with a one-element array of RPC errors.
    if let Value::Array(errors) = &mut body {
        if errors.len() == 1 {
            return errors.pop();
        }
    }
    Some(body)
}

fn http_error_message(code: u16, text: &str, class: ErrorClass) -> String {
    if class == ErrorClass::Auth {
        format!("{code}: auth rejected — re-authenticate to get a fresh token. body: {text}")
    } else {
        format!("{code}: {text}")
    }
}

/// Classify one non-2xx provider answer the way a live step does: the provider-code and status
/// table first, then, when the request carried a service tier, the tier's family matcher, which
/// turns a refusal of that tier into a permanent `service_tier_unavailable` (never
/// `auth_required`, whatever the status). Public so the credential-report decision downstream
/// can be tested against exactly what the transport produces.
pub fn classify_http_error(
    status: u16,
    body: &str,
    retry_after_secs: Option<f64>,
    service_tier: Option<(
        crate::spec::WireFamilyId,
        &crate::service_tier::ServiceTierRequest,
    )>,
) -> ProviderError {
    classify_http_error_in_family(status, body, retry_after_secs, service_tier, None, None)
}

fn classify_http_error_in_family(
    status: u16,
    body: &str,
    retry_after_secs: Option<f64>,
    service_tier: Option<(
        crate::spec::WireFamilyId,
        &crate::service_tier::ServiceTierRequest,
    )>,
    family: Option<crate::spec::WireFamilyId>,
    header_code: Option<&str>,
) -> ProviderError {
    let error = if family.is_none() && service_tier.is_none() && header_code.is_none() {
        http_error(status, body, retry_after_secs)
    } else {
        http_error_in_family(
            status,
            body,
            retry_after_secs,
            family.or(service_tier.map(|(family, _)| family)),
            header_code,
        )
    };
    match service_tier {
        Some((family, tier)) => {
            crate::service_tier::classify_tier_refusal(family, Some(tier), status, body, error)
        }
        None => error,
    }
}

/// A 401 from the ChatGPT/Codex backend that names an OpenAI API KEY is not a verdict on the
/// credential broca sent, and must never be reported to the vault.
///
/// broca authenticates to that backend with the account's OAuth access token, a JWT. It never
/// sends an `sk-` key there. So when the answer is `"Incorrect API key provided: sk-…"`, the key
/// being rejected is one on OpenAI's side of the backend, not ours. Measured on 2026-09-25 from
/// 22:41Z: 8 runs paused `auth_required` naming `sk-svcacct…fvMA`, while runs on the SAME token
/// seconds before and after completed. The vault confirmed the stored secret was a valid JWT, and
/// that each of broca's 16 reports in 30 minutes had forced a real refresh, rotating the refresh
/// token for a credential that was never the problem.
///
/// So it's classed transient, which the vault-report path never reports: the transport retries
/// it, since the same request seconds later succeeds, and a run that exhausts retries ends as an
/// error rather than parking for a re-login nobody needs. The match needs the ChatGPT family AND
/// the shared ChatGPT policy's verdict: a 401 whose message starts by naming an `sk-` key. A 401
/// that names nothing, or names a token, keeps its class.
fn classify_chatgpt_upstream_key_rejection(
    family: crate::spec::WireFamilyId,
    status: u16,
    body: &str,
    error: ProviderError,
) -> ProviderError {
    if family != crate::spec::WireFamilyId::OpenAiResponsesChatgpt {
        return error;
    }
    // The observation time only dates a usage-limit reset, which a 401 never carries.
    if cortexkit_chatgpt_policy::classify_response_with_retry_after(
        status,
        body,
        UNIX_EPOCH,
        error.retry_after_secs,
    ) != cortexkit_chatgpt_policy::Classification::UpstreamKeyRejection
    {
        return error;
    }
    ProviderError {
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Transient,
        message: format!(
            "{status}: {CHATGPT_UPSTREAM_KEY_REJECTED}: the ChatGPT backend rejected an OpenAI API key on its own side (broca sent \
             the account's OAuth token, not a key); not a credential failure."
        ),
        status: Some(status),
        retry_after_secs: error.retry_after_secs,
        provider_code: Some(CHATGPT_UPSTREAM_KEY_REJECTED.to_string()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

/// A 429 that says one ACCOUNT has used up a usage window, rather than that the route is
/// momentarily busy, becomes [`ACCOUNT_LIMIT_EXHAUSTED`]. Three providers say so, each in its own
/// way: Anthropic in its response headers (below), the ChatGPT/Codex backend in its body (see
/// [`chatgpt_usage_limit_reached`]), and Google's Gemini and Code Assist backends in a structured
/// error detail (see [`google_quota_exhausted`]). Every other family's 429 is returned unchanged.
///
/// For Anthropic, the body cannot tell them apart: an exhausted subscription answers with the same
/// `rate_limit_error` body as a short throttle. The headers can. Captured 2026-09-26 from an
/// account past its weekly limit, the 429 carried `anthropic-ratelimit-unified-status: rejected`,
/// `anthropic-ratelimit-unified-representative-claim: seven_day`,
/// `anthropic-ratelimit-unified-reset: 1790740800` (epoch seconds) and `retry-after: 352835`,
/// while a healthy account's 200 carried `unified-status: allowed`. So `rejected` on a 429 is the
/// discriminator; a 429 without it keeps its ordinary classification and retry.
///
/// `resets_at_ms` comes from the unified reset header, else from now plus the retry-after.
/// `account_ref` is left empty: the provider does not know it, and the run stamps it from its
/// frozen auth selection.
///
/// A refusal of the requested SERVICE TIER is left as it is, even if it arrived with `rejected`.
/// "Usage credits are required for fast mode" is a permanent statement about the tier, and
/// relabelling it a transient account wall would have routing wait for a reset that fixes
/// nothing. The live refusal did not carry `rejected` on 2026-09-26, so this guards a header
/// combination nobody has seen yet rather than one that happens today.
pub fn classify_account_limit_exhausted(
    family: crate::spec::WireFamilyId,
    status: u16,
    headers: &crate::wire::ResponseHeaders,
    body: &str,
    now: SystemTime,
    error: ProviderError,
) -> ProviderError {
    if status == 402 {
        if let Some(unaffordable) = openrouter_credit_insufficient(body, &error) {
            return unaffordable;
        }
    }
    if matches!(
        family,
        crate::spec::WireFamilyId::OpenAiChat | crate::spec::WireFamilyId::OpenAiResponses
    ) && (status == 402 || status == 403)
    {
        if let Some(exhausted) = xai_spending_limit_reached(body, status, &error) {
            return exhausted;
        }
    }
    if family == crate::spec::WireFamilyId::OpenAiChat && status == 403 {
        return kimi_weekly_usage_limit_reached(body, error);
    }
    if status != 429 {
        return error;
    }
    if error.provider_code.as_deref() == Some(crate::service_tier::SERVICE_TIER_UNAVAILABLE) {
        return error;
    }
    match family {
        crate::spec::WireFamilyId::AnthropicMessages => {
            let selected_headers = [
                ANTHROPIC_UNIFIED_STATUS,
                ANTHROPIC_UNIFIED_RESET,
                ANTHROPIC_UNIFIED_REPRESENTATIVE_CLAIM,
            ]
            .into_iter()
            .filter_map(|name| headers.get(name).map(|value| (name, value)));
            match cortexkit_anthropic_policy::classify_account_limit_exhausted_with_retry_after(
                status,
                selected_headers,
                body,
                now,
                error.retry_after_secs,
            ) {
                Some(limit) => ProviderError {
                    refusal: None,
                    class: ErrorClass::Transient,
                    provider_code: Some(ACCOUNT_LIMIT_EXHAUSTED.to_string()),
                    resets_at_ms: limit.resets_at_ms,
                    resets_at_source: limit
                        .resets_at_source
                        .map(|source| source.as_str().to_owned()),
                    limit_window: limit.limit_window,
                    account_ref: None,
                    ..error
                },
                None => error,
            }
        }
        crate::spec::WireFamilyId::OpenAiResponsesChatgpt => {
            chatgpt_usage_limit_reached(status, body, now, error)
        }
        crate::spec::WireFamilyId::OpenAiChat => kimi_weekly_usage_limit_reached(body, error),
        // Code Assist wraps the Gemini request but answers errors in the same Google RPC shape,
        // so both families share one decoder.
        crate::spec::WireFamilyId::Gemini | crate::spec::WireFamilyId::GoogleCodeAssist => {
            google_quota_exhausted(body, now, error)
        }
        _ => error,
    }
}

/// The ChatGPT/Codex backend's account usage limit (its plan allowance is exhausted): a 429 whose
/// body's `error.type` is `usage_limit_reached`. The account's plan allowance is used up until a stated reset time, so
/// re-sending within the step cannot succeed.
///
/// The discriminator is taken from production traffic, not documentation. Every such refusal
/// broca recorded (39 runs, 2026-08-27..09-26) was a 429 with a body of this exact shape:
/// `{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached",
/// "plan_type":"pro","resets_at":1790021043,"eligible_promo":null,"resets_in_seconds":441721}}`.
/// The open-source codex client keys on the same field and status (`codex-api/src/api_bridge.rs`,
/// the `TOO_MANY_REQUESTS` arm matching `error_type == "usage_limit_reached"`), and reads
/// `resets_at` as epoch seconds.
///
/// The shared ChatGPT policy (`cortexkit_chatgpt_policy::classify_response_with_retry_after`)
/// makes that decision and computes the reset: `resets_at_ms` comes from `resets_at`, else from
/// now plus `resets_in_seconds`, else from now plus the retry-after. Both relative fallbacks are
/// marked `retry_after`, not a stated absolute reset from the body. `limit_window` is left empty:
/// the captured bodies name the plan but not which window ran out, and inventing a window name
/// would mislead a router. A ChatGPT 429 with any other body (an ordinary throttle) is returned
/// unchanged and keeps retrying.
fn chatgpt_usage_limit_reached(
    status: u16,
    body: &str,
    now: SystemTime,
    error: ProviderError,
) -> ProviderError {
    let cortexkit_chatgpt_policy::Classification::AccountLimitExhausted {
        resets_at_ms,
        resets_at_source,
    } = cortexkit_chatgpt_policy::classify_response_with_retry_after(
        status,
        body,
        now,
        error.retry_after_secs,
    )
    else {
        return error;
    };
    ProviderError {
        refusal: None,
        class: ErrorClass::Transient,
        provider_code: Some(ACCOUNT_LIMIT_EXHAUSTED.to_string()),
        resets_at_ms,
        resets_at_source: resets_at_source.map(|source| source.as_str().to_owned()),
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        ..error
    }
}

/// OpenRouter's refusal of a request its credential's remaining credit cannot pay for: HTTP 402
/// whose `error.metadata.limit_source` is `openrouter_credits`. The structured field decides,
/// never the message wording. A production refusal read: `{"error":{"message":"This request
/// requires more credits, or fewer max_tokens. You requested up to 128000 tokens, but can only
/// afford 97335. To increase, visit https://openrouter.ai/settings/credits and add more
/// credits","code":402,"metadata":{"limit_source":"openrouter_credits", ...}}}`.
///
/// It becomes a permanent `credit_insufficient`, not `billing_exhausted`: the balance is not
/// gone, this request is just too expensive for it, and the same request re-sent within the run
/// cannot succeed. The affordable `max_tokens` is read from the message only to fill the
/// optional `affordable_max_tokens`; a message without the number leaves it absent.
fn openrouter_credit_insufficient(body: &str, error: &ProviderError) -> Option<ProviderError> {
    let detail = parse_http_error_body(body)?.get("error")?.clone();
    if detail
        .pointer("/metadata/limit_source")
        .and_then(Value::as_str)
        != Some("openrouter_credits")
    {
        return None;
    }
    let affordable_max_tokens = detail
        .get("message")
        .and_then(Value::as_str)
        .and_then(|message| message.split_once("can only afford "))
        .and_then(|(_, rest)| {
            let digits = rest.split(|c: char| !c.is_ascii_digit()).next()?;
            digits.parse::<u64>().ok()
        });
    Some(ProviderError {
        refusal: None,
        class: ErrorClass::Permanent,
        provider_code: Some(CREDIT_INSUFFICIENT.to_string()),
        retry_after_secs: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens,
        ..error.clone()
    })
}

/// xAI's spending limit refusal: HTTP 402 or 403 with code `personal-team-blocked:spending-limit`.
/// Class is permanent, with no resets_at_ms and no retry within the run.
fn xai_spending_limit_reached(
    body: &str,
    status: u16,
    error: &ProviderError,
) -> Option<ProviderError> {
    if status != 402 && status != 403 {
        return None;
    }
    let code = match error.provider_code.as_deref() {
        Some(code) => Some(code.to_string()),
        None => extract_provider_code(body),
    }?;
    if code == "personal-team-blocked:spending-limit" || code == BILLING_EXHAUSTED {
        Some(ProviderError {
            refusal: None,
            class: ErrorClass::Permanent,
            provider_code: Some(BILLING_EXHAUSTED.to_string()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
            ..error.clone()
        })
    } else {
        None
    }
}

/// Kimi's weekly allowance refusal has no reset timestamp, so preserve the unknown reset rather
/// than inventing a retry time from the wording of its message.
fn kimi_weekly_usage_limit_reached(body: &str, error: ProviderError) -> ProviderError {
    let Some(detail) = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|body| body.get("error").cloned())
    else {
        return error;
    };
    let matches = detail.get("type").and_then(Value::as_str) == Some("access_terminated_error")
        && detail
            .get("message")
            .and_then(Value::as_str)
            .is_some_and(|message| message.to_ascii_lowercase().contains("usage limit"));
    if !matches {
        return error;
    }
    ProviderError {
        refusal: None,
        class: ErrorClass::Transient,
        provider_code: Some(ACCOUNT_LIMIT_EXHAUSTED.to_owned()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        ..error
    }
}

/// Google's account quota wall: a 429 whose body's `error.details[]` holds a
/// `google.rpc.ErrorInfo` with `reason: "QUOTA_EXHAUSTED"`.
///
/// Taken from production traffic: an Antigravity (Code Assist) run on 2026-10-03 got this 429
/// with `status: "RESOURCE_EXHAUSTED"`, an ErrorInfo of reason `QUOTA_EXHAUSTED` from domain
/// `cloudcode-pa.googleapis.com`, and metadata `quotaResetDelay: "21m0.959250039s"` plus a
/// `quotaResetTimeStamp`. Retrying it six times over a minute changed nothing, because the
/// quota refills only after the stated reset.
///
/// `status: "RESOURCE_EXHAUSTED"` alone is NOT the discriminator: Google uses it for its
/// per-minute rate limits too, and those clear within seconds and must keep retrying. Neither is
/// the English message ("Individual quota reached..."), which is prose and may change. The
/// ErrorInfo reason is the machine-readable field Google defines for this distinction.
///
/// `resets_at_ms` comes from `quotaResetTimeStamp` (RFC 3339, source `body`), else from now plus
/// `quotaResetDelay` (a Go duration such as `21m0.959250039s`, source `retry_after`, the same
/// label the ChatGPT path gives its relative `resets_in_seconds`), else it is left empty. A
/// malformed value is skipped rather than failing the classification: the wall is still a wall
/// even when its reset time cannot be read. `limit_window` is left empty because Google does not
/// name the window that ran out.
fn google_quota_exhausted(body: &str, now: SystemTime, error: ProviderError) -> ProviderError {
    let Ok(body) = serde_json::from_str::<Value>(body) else {
        return error;
    };
    let Some(info) = body
        .get("error")
        .and_then(|detail| detail.get("details"))
        .and_then(Value::as_array)
        .and_then(|details| {
            details.iter().find(|detail| {
                detail.get("@type").and_then(Value::as_str) == Some(GOOGLE_RPC_ERROR_INFO_TYPE)
                    && detail.get("reason").and_then(Value::as_str)
                        == Some(GOOGLE_QUOTA_EXHAUSTED_REASON)
            })
        })
    else {
        return error;
    };
    let metadata = info.get("metadata");
    let field = |name: &str| {
        metadata
            .and_then(|metadata| metadata.get(name))
            .and_then(Value::as_str)
    };
    let resets_at_ms = field("quotaResetTimeStamp")
        .and_then(|stamp| chrono::DateTime::parse_from_rfc3339(stamp.trim()).ok())
        .and_then(|stamp| u64::try_from(stamp.timestamp_millis()).ok())
        .map(|time| (time, "body"))
        .or_else(|| {
            field("quotaResetDelay")
                .and_then(parse_go_duration_millis)
                .map(|delay| (system_time_millis(now).saturating_add(delay), "retry_after"))
        });
    ProviderError {
        refusal: None,
        class: ErrorClass::Transient,
        provider_code: Some(ACCOUNT_LIMIT_EXHAUSTED.to_string()),
        resets_at_ms: resets_at_ms.map(|(time, _)| time),
        resets_at_source: resets_at_ms.map(|(_, source)| source.to_owned()),
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        ..error
    }
}

/// Parse a non-negative Go `time.Duration` string (`21m0.959250039s`, `1h30m`, `500ms`, `0`)
/// into whole milliseconds, rounding a fractional millisecond UP so a reset computed from it is
/// never earlier than the one Google stated. Returns `None` for anything Go would reject, and for
/// a negative duration, which cannot describe a future reset.
fn parse_go_duration_millis(text: &str) -> Option<u64> {
    let text = text.trim();
    let text = text.strip_prefix('+').unwrap_or(text);
    if text == "0" {
        return Some(0);
    }
    if text.is_empty() {
        return None;
    }
    let mut rest = text;
    let mut total_nanos: u128 = 0;
    while !rest.is_empty() {
        let number_len = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let (number, after_number) = rest.split_at(number_len);
        let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
        if (whole.is_empty() && fraction.is_empty()) || fraction.contains('.') {
            return None;
        }
        let unit_len = after_number
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(after_number.len());
        let (unit, after_unit) = after_number.split_at(unit_len);
        let unit_nanos: u128 = match unit {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return None,
        };
        let whole: u128 = if whole.is_empty() {
            0
        } else {
            whole.parse().ok()?
        };
        // Only the first nine fraction digits can matter at nanosecond resolution; the rest
        // are dropped instead of overflowing the integer arithmetic.
        let fraction = &fraction[..fraction.len().min(9)];
        let fraction_nanos = if fraction.is_empty() {
            0
        } else {
            let scale = 10u128.pow(fraction.len() as u32);
            fraction.parse::<u128>().ok()? * unit_nanos / scale
        };
        total_nanos = total_nanos
            .checked_add(whole.checked_mul(unit_nanos)?)?
            .checked_add(fraction_nanos)?;
        rest = after_unit;
    }
    u64::try_from(total_nanos.div_ceil(1_000_000)).ok()
}

const GOOGLE_RPC_ERROR_INFO_TYPE: &str = "type.googleapis.com/google.rpc.ErrorInfo";
const GOOGLE_QUOTA_EXHAUSTED_REASON: &str = "QUOTA_EXHAUSTED";

use cortexkit_anthropic_policy::{
    anthropic_credit_balance_too_low,
    UNIFIED_REPRESENTATIVE_CLAIM as ANTHROPIC_UNIFIED_REPRESENTATIVE_CLAIM,
    UNIFIED_RESET as ANTHROPIC_UNIFIED_RESET, UNIFIED_STATUS as ANTHROPIC_UNIFIED_STATUS,
};

/// The provider code for [`classify_chatgpt_upstream_key_rejection`].
pub const CHATGPT_UPSTREAM_KEY_REJECTED: &str = "chatgpt_upstream_key_rejected";

/// Classify from the full structured provider response before constructing the display
/// message. Only the persisted diagnostic message is bounded — classification must see
/// the complete body (a provider code can sit past any truncation point).
fn http_error(code: u16, text: &str, retry_after_secs: Option<f64>) -> ProviderError {
    http_error_in_family(code, text, retry_after_secs, None, None)
}

fn http_error_in_family(
    code: u16,
    text: &str,
    retry_after_secs: Option<f64>,
    family: Option<crate::spec::WireFamilyId>,
    header_code: Option<&str>,
) -> ProviderError {
    use crate::spec::WireFamilyId;
    let provider_code = extract_provider_code(text)
        .or_else(|| header_code.map(|code| code.split(':').next().unwrap_or(code).to_owned()));
    let body = parse_http_error_body(text);
    let message = body
        .as_ref()
        .and_then(|body| {
            body.pointer("/error/message")
                .or_else(|| body.get("error").filter(|v| v.is_string()))
                .or_else(|| body.get("message"))
        })
        .and_then(Value::as_str)
        .unwrap_or("");
    // Public callers may have only an error body. Infer a family only from its structured
    // discriminator; live transport always supplies the family frozen on the request.
    let family = family.or_else(|| match provider_code.as_deref() {
        Some("invalid_request_error") if body.as_ref()?.get("type")?.as_str()? == "error" => {
            Some(WireFamilyId::AnthropicMessages)
        }
        Some("INVALID_ARGUMENT") => Some(WireFamilyId::Gemini),
        Some("ValidationException" | "validationException") => Some(WireFamilyId::BedrockConverse),
        Some("personal-team-blocked:spending-limit") => Some(WireFamilyId::OpenAiChat),
        _ => None,
    });
    let provider_code = match provider_code.as_deref() {
        Some("insufficient_quota") => Some(BILLING_EXHAUSTED.to_string()),
        Some("personal-team-blocked:spending-limit")
            if (code == 402 || code == 403)
                && matches!(
                    family,
                    Some(WireFamilyId::OpenAiChat | WireFamilyId::OpenAiResponses)
                ) =>
        {
            Some(BILLING_EXHAUSTED.to_string())
        }
        Some("invalid_request_error")
            if family == Some(WireFamilyId::AnthropicMessages)
                && anthropic_credit_balance_too_low(message) =>
        {
            Some(BILLING_EXHAUSTED.to_string())
        }
        _ => provider_code,
    };
    let overflow_code = family.and_then(|family| {
        crate::normalize::context_overflow_code(
            family,
            provider_code.as_deref(),
            message,
            Some(code),
        )
    });
    let class = if overflow_code.is_some() {
        ErrorClass::ContextOverflow
    } else {
        classify_error_with_status_defaults(provider_code.as_deref(), Some(code))
    };
    let provider_code = overflow_code.map(str::to_owned).or(provider_code);
    let error = ProviderError {
        provider_subcode: None,
        refusal: None,
        class,
        message: http_error_message(code, truncate_error_body(text), class),
        status: Some(code),
        retry_after_secs,
        provider_code,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    };
    // Run after the context-overflow and billing classification above, so a 400 those give a
    // more specific code keeps it.
    crate::request_invalid::classify_request_invalid(
        error,
        body.as_ref().and_then(|body| body.get("error")),
    )
}

fn truncate_error_body(text: &str) -> &str {
    if text.len() <= MAX_ERROR_BODY_BYTES {
        return text;
    }

    let mut end = MAX_ERROR_BODY_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use broca_protocol::error::{
        ACCOUNT_LIMIT_EXHAUSTED, BILLING_EXHAUSTED, CREDIT_INSUFFICIENT, REQUEST_INVALID,
    };
    use broca_protocol::{
        content::{AssistantMessage, ContentBlock, Role, ToolCall},
        display::DisplayEvent,
        AttemptOutcome, CallOptions, ErrorClass, FinishReason, Message, MessageId, ModelHandle,
        NormalizedEvent, ProviderControl, ProviderError, StepContext, StepId, Tool, ToolCallId,
        ToolChoice, Usage,
    };
    use serde_json::{Map, Value};

    use crate::{
        families::{
            frozen_config_for_family, gemini_frozen_config, openai_chat_frozen_config,
            openai_responses_frozen_config, AnthropicMessages, BedrockConverse, Gemini,
            GoogleCodeAssist, OpenAiChat, OpenAiResponses, OpenAiResponsesChatgpt,
        },
        spec::WireFamilyId,
        wire::{AuthHandle, HttpMethod, HttpRequest, NormalizeError, StreamNormalizer, WireFamily},
    };
    use bytes::Bytes;
    use futures::StreamExt;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{mpsc, oneshot},
        time::Instant,
    };
    use tokio_util::sync::CancellationToken;

    use super::{
        build_provider_http_client, classify_account_limit_exhausted, classify_http_error,
        collect_error_body, drive_body_stream, drive_body_stream_with_limit, drive_stream,
        drive_stream_with_header_timeout, dump_files_to_delete, extract_provider_code, http_error,
        parse_retry_after_header, parse_retry_after_value, publish_completion,
        request_dump_filename, retry_delay, write_dump_file, DumpFileMeta, DumpRetentionCaps,
        LiveProvider, PreparedHttpRequest, MAX_ERROR_BODY_BYTES, MAX_HTTP_ERROR_BODY_READ_BYTES,
        STREAM_CONTENT_TIMEOUT, STREAM_IDLE_TIMEOUT,
    };
    // Directory mode is only asserted on Unix, so keep that helper import under the same cfg.
    #[cfg(unix)]
    use super::create_dump_dir;

    const DAY_MS: u64 = 24 * 60 * 60 * 1000;

    mod context_overflow {
        include!("transport_context_overflow_tests.rs");
    }

    mod gemini_errors {
        include!("transport_gemini_error_tests.rs");
    }

    mod request_invalid {
        include!("transport_request_invalid_tests.rs");
    }

    async fn assert_offloaded_render_matches_direct<F>(
        family: F,
        frozen: crate::wire::FrozenRenderConfig,
        opts: CallOptions,
    ) where
        F: WireFamily + 'static,
    {
        let direct = family.render(&frozen, &opts);
        let job = super::RenderJob {
            family: Arc::new(family),
            frozen: Arc::new(frozen),
            auth: Arc::new(AuthHandle::None),
            opts: Arc::new(opts),
            step_id: StepId(7),
        };
        let offloaded = tokio::task::spawn_blocking(move || job.render())
            .await
            .expect("render job joins");
        assert_eq!(
            offloaded.request.body,
            Bytes::from(direct),
            "offloaded render changed request bytes"
        );
    }

    fn comprehensive_render_options() -> CallOptions {
        let mut provider_options = Map::new();
        // Deliberately insert in reverse lexical order: canonical rendering, not insertion order,
        // remains the byte contract after the job crosses a thread boundary.
        provider_options.insert("z-last".into(), Value::String("z".into()));
        provider_options.insert("a-first".into(), Value::String("a".into()));
        let mut opts = CallOptions::new(vec![
            Message::new(
                Role::System,
                vec![ContentBlock::Text {
                    text: "system survives transformed snapshots".into(),
                }],
            ),
            Message::assistant_from(
                vec![
                    ContentBlock::Reasoning {
                        text: "signed by another model".into(),
                        signature: Some("foreign-signature".into()),
                    },
                    ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId("historic-call".into()),
                        tool_name: "historical tool/name".into(),
                        input: serde_json::json!({"nested": {"retained": true}}),
                        thought_signature: Some("call-signature".into()),
                    }),
                ],
                ModelHandle::new("other-provider", "other-model"),
            ),
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    // A large request catches ownership paths that accidentally special-case
                    // small golden fixtures.
                    text: "x".repeat(512 * 1024),
                }],
            ),
        ]);
        opts.tools = Arc::new(vec![Tool {
            name: "current tool/name".into(),
            description: "nested schema exercises provider tool conversion".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "outer": {
                        "type": "object",
                        "properties": {"inner": {"type": "array", "items": {"type": "string"}}}
                    }
                }
            }),
            module: None,
            execution_mode: Default::default(),
        }]);
        opts.tool_choice = Arc::new(ToolChoice::Auto);
        opts.provider_options = Arc::new(Value::Object(provider_options));
        opts.step_context = Some(StepContext {
            run_id: "render-job-run".into(),
            step_index: 7,
        });
        opts.target_model = Some(ModelHandle::new("target-provider", "target-model"));
        opts
    }

    fn frozen_for_render_job(id: WireFamilyId) -> crate::wire::FrozenRenderConfig {
        let mut frozen = frozen_config_for_family(id, "target-model");
        if id == WireFamilyId::GoogleCodeAssist {
            frozen.project_id = Some("render-job-project".into());
        }
        frozen
    }

    #[tokio::test]
    async fn every_family_keeps_direct_render_bytes_when_offloaded() {
        let opts = comprehensive_render_options();
        let transformed = opts.with_prompt(vec![
            Message::new(
                Role::System,
                vec![ContentBlock::Text {
                    text: "transformed system".into(),
                }],
            ),
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "transformed request body".into(),
                }],
            ),
        ]);

        macro_rules! check_family {
            ($family:expr, $id:expr) => {
                assert_offloaded_render_matches_direct(
                    $family,
                    frozen_for_render_job($id),
                    opts.clone(),
                )
                .await;
                assert_offloaded_render_matches_direct(
                    $family,
                    frozen_for_render_job($id),
                    transformed.clone(),
                )
                .await;
            };
        }

        check_family!(AnthropicMessages, WireFamilyId::AnthropicMessages);
        check_family!(OpenAiChat, WireFamilyId::OpenAiChat);
        check_family!(OpenAiResponses, WireFamilyId::OpenAiResponses);
        check_family!(Gemini, WireFamilyId::Gemini);
        check_family!(BedrockConverse, WireFamilyId::BedrockConverse);
        check_family!(GoogleCodeAssist, WireFamilyId::GoogleCodeAssist);
        check_family!(OpenAiResponsesChatgpt, WireFamilyId::OpenAiResponsesChatgpt);
    }

    #[tokio::test]
    async fn render_panic_is_reported_as_a_provider_error() {
        let provider = LiveProvider::new(
            GoogleCodeAssist,
            frozen_config_for_family(WireFamilyId::GoogleCodeAssist, "target-model"),
            AuthHandle::None,
        );
        let mut opts = comprehensive_render_options();
        opts.step_context = Some(StepContext {
            run_id: "panic-translation".into(),
            step_index: 1,
        });
        let mut rx = provider.start_step(StepId(1), Arc::new(opts), CancellationToken::new(), None);
        let event = rx
            .recv()
            .await
            .expect("render panic publishes a typed error");
        match event {
            NormalizedEvent::Control(ProviderControl::Error {
                error,
                retries_used,
            }) => {
                assert_eq!(error.class, ErrorClass::Permanent);
                assert_eq!(error.provider_code.as_deref(), Some("render_task_failed"));
                assert_eq!(retries_used, 0);
            }
            other => panic!("expected render error, got {other:?}"),
        }
    }

    struct CountingNormalizer(Arc<AtomicUsize>);

    impl StreamNormalizer for CountingNormalizer {
        fn push(&mut self, _chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct ProgressNormalizer(u64);

    impl StreamNormalizer for ProgressNormalizer {
        fn push(&mut self, _chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            self.0 += 1;
            Ok(Vec::new())
        }

        fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            Ok(test_completion("progress-complete"))
        }

        fn progress_marker(&self) -> u64 {
            self.0
        }
    }

    struct CompletionNormalizer;

    impl StreamNormalizer for CompletionNormalizer {
        fn push(&mut self, _chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            Ok(Vec::new())
        }

        fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            Ok(test_completion("completion"))
        }
    }

    fn test_completion(message_id: &str) -> Vec<NormalizedEvent> {
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId(message_id.into()),
                    content: vec![ContentBlock::Text {
                        text: "complete".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ]
    }

    #[derive(Clone, Copy)]
    enum RetryAttemptBehavior {
        DecodeFailure,
        EmptyCompletion,
        AnthropicOverloaded,
        FreshnessProbe,
        /// Each body chunk becomes a one-byte text delta so the stream-rate floor can observe content.
        TextTrickle,
        Success,
    }

    struct RetryTestFamily {
        render_calls: Arc<AtomicUsize>,
        normalizer_index: Arc<AtomicUsize>,
        attempt_behaviors: Vec<RetryAttemptBehavior>,
    }

    impl RetryTestFamily {
        fn new(decode_failures: usize) -> Self {
            Self::with_behaviors(vec![RetryAttemptBehavior::DecodeFailure; decode_failures])
        }

        fn with_behaviors(attempt_behaviors: Vec<RetryAttemptBehavior>) -> Self {
            Self {
                render_calls: Arc::new(AtomicUsize::new(0)),
                normalizer_index: Arc::new(AtomicUsize::new(0)),
                attempt_behaviors,
            }
        }
    }

    impl WireFamily for RetryTestFamily {
        fn shape(
            &self,
            _frozen: &crate::wire::FrozenRenderConfig,
            _opts: &CallOptions,
        ) -> serde_json::Value {
            self.render_calls.fetch_add(1, Ordering::SeqCst);
            serde_json::json!({"retry_test": "byte-identical"})
        }

        fn endpoint(
            &self,
            frozen: &crate::wire::FrozenRenderConfig,
            _auth: &AuthHandle,
            body: Vec<u8>,
        ) -> HttpRequest {
            HttpRequest {
                method: HttpMethod::Post,
                url: format!("{}/retry", frozen.base_url.as_deref().unwrap()),
                // The Claude Code persona's attempt counter, which the transport numbers.
                headers: vec![
                    ("content-type".into(), "application/json".into()),
                    ("x-stainless-retry-count".into(), "0".into()),
                ],
                body,
            }
        }

        fn normalizer(
            &self,
            _frozen: &crate::wire::FrozenRenderConfig,
            step_id: StepId,
        ) -> Box<dyn StreamNormalizer> {
            let index = self.normalizer_index.fetch_add(1, Ordering::SeqCst);
            match self
                .attempt_behaviors
                .get(index)
                .copied()
                .unwrap_or(RetryAttemptBehavior::Success)
            {
                RetryAttemptBehavior::DecodeFailure => Box::new(FailedAttemptNormalizer {
                    step_id,
                    emitted: false,
                }),
                RetryAttemptBehavior::EmptyCompletion => Box::new(EmptyCompletionNormalizer),
                RetryAttemptBehavior::AnthropicOverloaded => {
                    Box::new(AnthropicOverloadedNormalizer)
                }
                RetryAttemptBehavior::FreshnessProbe => Box::new(FreshAttemptNormalizer::default()),
                RetryAttemptBehavior::TextTrickle => Box::new(TextTrickleNormalizer),
                RetryAttemptBehavior::Success => Box::new(SuccessAttemptNormalizer),
            }
        }
    }

    struct FailedAttemptNormalizer {
        step_id: StepId,
        emitted: bool,
    }

    impl StreamNormalizer for FailedAttemptNormalizer {
        fn push(&mut self, _chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            self.emitted = true;
            Ok(vec![
                NormalizedEvent::Display(DisplayEvent::DisplayGap { dropped: 99 }),
                NormalizedEvent::Control(ProviderControl::StepFinished {
                    step_id: self.step_id,
                    usage: Usage {
                        output_tokens: Some(999),
                        ..Usage::default()
                    },
                    finish_reason: FinishReason::Stop,
                    service_tier_used: None,
                    speed_used: None,
                }),
            ])
        }

        fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            assert!(self.emitted, "test response body must reach the normalizer");
            Err(NormalizeError::Invalid("injected malformed stream".into()))
        }
    }

    struct EmptyCompletionNormalizer;

    impl StreamNormalizer for EmptyCompletionNormalizer {
        fn push(&mut self, _chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct FreshAttemptNormalizer {
        saw_partial_output: bool,
        saw_successful_output: bool,
    }

    impl StreamNormalizer for FreshAttemptNormalizer {
        fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            let dropped = match chunk.first().copied() {
                Some(b'P') => {
                    self.saw_partial_output = true;
                    99
                }
                Some(b'S') => {
                    self.saw_successful_output = true;
                    if self.saw_partial_output {
                        99
                    } else {
                        1
                    }
                }
                _ => return Ok(Vec::new()),
            };
            Ok(vec![NormalizedEvent::Display(DisplayEvent::DisplayGap {
                dropped,
            })])
        }

        fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            if self.saw_successful_output {
                return Ok(test_completion("fresh-retry-success"));
            }
            assert!(
                self.saw_partial_output,
                "the failed attempt must consume partial output"
            );
            Err(NormalizeError::Invalid("injected partial stream".into()))
        }
    }

    struct AnthropicOverloadedNormalizer;

    impl StreamNormalizer for AnthropicOverloadedNormalizer {
        fn push(&mut self, _chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            Ok(vec![NormalizedEvent::Control(ProviderControl::Error {
                error: ProviderError {
                    provider_subcode: None,
                    refusal: None,
                    class: ErrorClass::Transient,
                    message: "overloaded".into(),
                    status: None,
                    retry_after_secs: None,
                    provider_code: Some("overloaded_error".into()),
                    resets_at_ms: None,
                    resets_at_source: None,
                    limit_window: None,
                    account_ref: None,
                    credential_id: None,
                    affordable_max_tokens: None,
                },
                retries_used: 0,
            })])
        }
    }

    /// Emits one content byte per body chunk so the optional stream-rate floor has something to measure.
    struct TextTrickleNormalizer;

    impl StreamNormalizer for TextTrickleNormalizer {
        fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            if chunk.is_empty() {
                return Ok(Vec::new());
            }
            Ok(vec![NormalizedEvent::Display(DisplayEvent::TextDelta {
                message_id: MessageId("trickle".into()),
                tool_call_id: None,
                // One content byte per chunk keeps the measured rate equal to the chunk arrival rate.
                delta: "x".into(),
            })])
        }
    }

    struct SuccessAttemptNormalizer;

    impl StreamNormalizer for SuccessAttemptNormalizer {
        fn push(&mut self, _chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            Ok(vec![NormalizedEvent::Display(DisplayEvent::DisplayGap {
                dropped: 1,
            })])
        }

        fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            Ok(vec![
                NormalizedEvent::Control(ProviderControl::AssistantMessage {
                    message: AssistantMessage {
                        message_id: MessageId("retry-success".into()),
                        content: vec![ContentBlock::Text {
                            text: "complete".into(),
                        }],
                    },
                }),
                NormalizedEvent::Control(ProviderControl::Finished {
                    usage: Usage {
                        output_tokens: Some(22),
                        ..Usage::default()
                    },
                    finish_reason: FinishReason::Stop,
                    retries_used: 0,
                }),
            ])
        }
    }

    #[derive(Clone)]
    enum ScriptResponse {
        Http {
            status: u16,
            headers: Vec<(&'static str, &'static str)>,
            body: &'static [u8],
        },
        TruncatedHttp {
            status: u16,
            headers: Vec<(&'static str, &'static str)>,
            body: &'static [u8],
        },
        MidChunkDisconnect,
        Disconnect,
    }

    struct ScriptedServer {
        url: String,
        requests: Arc<Mutex<Vec<Vec<u8>>>>,
        heads: Arc<Mutex<Vec<String>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for ScriptedServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl ScriptedServer {
        async fn start(script: Vec<ScriptResponse>) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let heads = Arc::new(Mutex::new(Vec::new()));
            let captured = requests.clone();
            let captured_heads = heads.clone();
            let task = tokio::spawn(async move {
                let mut script = script.into_iter();
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let (head, body) = read_http_request(&mut socket).await;
                    captured.lock().unwrap().push(body);
                    captured_heads.lock().unwrap().push(head);
                    match script.next().unwrap_or(ScriptResponse::Http {
                        status: 500,
                        headers: Vec::new(),
                        body: br#"{"error":{"message":"unexpected extra attempt"}}"#,
                    }) {
                        ScriptResponse::Http {
                            status,
                            headers,
                            body,
                        } => write_http_response(&mut socket, status, &headers, body).await,
                        ScriptResponse::TruncatedHttp {
                            status,
                            headers,
                            body,
                        } => {
                            write_truncated_http_response(&mut socket, status, &headers, body).await
                        }
                        ScriptResponse::MidChunkDisconnect => {
                            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n100\r\ncut").await.unwrap();
                            socket.flush().await.unwrap();
                        }
                        ScriptResponse::Disconnect => {}
                    }
                }
            });
            Self {
                url: format!("http://{address}"),
                requests,
                heads,
                task,
            }
        }

        fn request_bodies(&self) -> Vec<Vec<u8>> {
            self.requests.lock().unwrap().clone()
        }

        /// The value of header `name` on each request received, in order.
        fn request_header(&self, name: &str) -> Vec<Option<String>> {
            self.heads
                .lock()
                .unwrap()
                .iter()
                .map(|head| {
                    head.lines().find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.trim()
                            .eq_ignore_ascii_case(name)
                            .then(|| value.trim().to_string())
                    })
                })
                .collect()
        }
    }

    async fn read_http_request_body(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
        read_http_request(socket).await.1
    }

    /// One request's header block (as text) and body.
    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        let header_end = loop {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(
                read, 0,
                "client disconnected before request headers completed"
            );
            request.extend_from_slice(&chunk[..read]);
            if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        while request.len() < header_end + content_length {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client disconnected before request body completed");
            request.extend_from_slice(&chunk[..read]);
        }
        (
            String::from_utf8_lossy(&request[..header_end]).into_owned(),
            request[header_end..header_end + content_length].to_vec(),
        )
    }

    async fn write_http_response(
        socket: &mut tokio::net::TcpStream,
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) {
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            408 => "Request Timeout",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            503 => "Service Unavailable",
            _ => "Injected",
        };
        let mut response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n",
            body.len()
        );
        for (name, value) in headers {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(body).await.unwrap();
    }

    async fn write_truncated_http_response(
        socket: &mut tokio::net::TcpStream,
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) {
        let mut response = format!(
            "HTTP/1.1 {status} Injected\r\nContent-Length: {}\r\nConnection: close\r\n",
            body.len() + 64
        );
        for (name, value) in headers {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(body).await.unwrap();
    }

    fn retry_test_provider(
        server: &ScriptedServer,
        decode_failures: usize,
    ) -> LiveProvider<RetryTestFamily> {
        let mut frozen = openai_chat_frozen_config("retry-test");
        frozen.base_url = Some(server.url.clone());
        let provider = LiveProvider::new(
            RetryTestFamily::new(decode_failures),
            frozen,
            AuthHandle::None,
        );
        // This fixture speaks only HTTP; keep its retry/deadline assertions on the SSE path.
        broca_core::Provider::set_chatgpt_https_policy(&provider, Arc::new(|| true));
        provider
    }

    fn retry_test_provider_with_behaviors(
        server: &ScriptedServer,
        attempt_behaviors: Vec<RetryAttemptBehavior>,
    ) -> LiveProvider<RetryTestFamily> {
        let mut frozen = openai_chat_frozen_config("retry-test");
        frozen.base_url = Some(server.url.clone());
        LiveProvider::new(
            RetryTestFamily::with_behaviors(attempt_behaviors),
            frozen,
            AuthHandle::None,
        )
    }

    async fn collect_step_events(
        provider: &LiveProvider<RetryTestFamily>,
        cancel: CancellationToken,
        deadline: Option<Instant>,
    ) -> Vec<NormalizedEvent> {
        let mut rx = provider.start_step(
            StepId(1),
            Arc::new(CallOptions::new(Vec::new())),
            cancel,
            deadline,
        );
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    }

    fn terminal_error(events: &[NormalizedEvent]) -> &ProviderError {
        events
            .iter()
            .find_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::Error { error, .. }) => Some(error),
                _ => None,
            })
            .expect("step should emit a terminal provider error")
    }

    #[test]
    fn prepared_retry_requests_share_rendered_body_allocation() {
        let body = vec![b'x'; 1024 * 1024];
        let request = PreparedHttpRequest::from(HttpRequest {
            method: HttpMethod::Post,
            url: "https://example.invalid".into(),
            headers: Vec::new(),
            body,
        });
        let retry = request.clone();
        let dump = request.body.clone();

        assert_eq!(request.body, retry.body);
        assert_eq!(request.body.as_ptr(), retry.body.as_ptr());
        assert_eq!(request.body.as_ptr(), dump.as_ptr());
    }

    #[tokio::test]
    async fn request_dump_content_matches_the_shared_bytes_sent_over_http() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 200,
            headers: Vec::new(),
            body: b"successful-generation",
        }])
        .await;
        let expected_body = br#"{"prompt":"exact rendered body"}"#.to_vec();
        let request = PreparedHttpRequest::from(HttpRequest {
            method: HttpMethod::Post,
            url: format!("{}/dump-content", server.url),
            headers: vec![("content-type".into(), "application/json".into())],
            body: expected_body,
        });
        let dump_body = request.body.clone();
        assert_eq!(request.body.as_ptr(), dump_body.as_ptr());

        let dir = broca_test_support::scratch_root().join(format!(
            "broca-provider-dump-content-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create request dump test directory");
        let path = dir.join("request.json");
        write_dump_file(&path, &dump_body).expect("write request dump from shared bytes");

        let (tx, _rx) = mpsc::channel(256);
        drive_stream(
            &build_provider_http_client(),
            &request,
            Box::new(SuccessAttemptNormalizer),
            &tx,
        )
        .await
        .expect("send the prepared request");

        let requests = server.request_bodies();
        assert_eq!(requests.len(), 1);
        assert_eq!(std::fs::read(&path).unwrap(), requests[0]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retry_normalizers_share_the_same_tool_name_map() {
        let names = Arc::new(crate::tool_name_alias::ToolNameMap::from_names([
            "read", "bad:name",
        ]));
        let family = RetryTestFamily::with_behaviors(vec![
            RetryAttemptBehavior::Success;
            super::MAX_ATTEMPTS
        ]);
        let frozen = openai_chat_frozen_config("retry-map-sharing");
        let normalizer_factory =
            || super::build_retry_normalizer(&family, &frozen, StepId(1), &names);

        for _ in 0..super::MAX_ATTEMPTS {
            let normalizer = normalizer_factory();
            assert!(Arc::ptr_eq(&normalizer.names, &names));
        }
    }

    #[tokio::test]
    async fn successful_first_attempt_constructs_exactly_one_normalizer() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 200,
            headers: Vec::new(),
            body: b"successful-generation",
        }])
        .await;
        let provider = retry_test_provider(&server, 0);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;

        assert!(terminal_error_if_any(&events).is_none());
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    NormalizedEvent::Control(ProviderControl::Finished {
                        retries_used: 0,
                        ..
                    })
                ))
                .count(),
            1
        );
        assert_eq!(server.request_bodies().len(), 1);
        assert_eq!(provider.family.normalizer_index.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_constructs_fresh_normalizer_after_partial_output() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"Partial output",
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"Successful output",
            },
        ])
        .await;
        let provider = retry_test_provider_with_behaviors(
            &server,
            vec![
                RetryAttemptBehavior::FreshnessProbe,
                RetryAttemptBehavior::FreshnessProbe,
            ],
        );

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let dropped: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                NormalizedEvent::Display(DisplayEvent::DisplayGap { dropped }) => Some(*dropped),
                _ => None,
            })
            .collect();

        assert_eq!(
            dropped,
            vec![99, 1],
            "the successful attempt must not retain the failed parser's partial state"
        );
        assert_eq!(server.request_bodies().len(), 2);
        assert_eq!(provider.family.normalizer_index.load(Ordering::SeqCst), 2);
        assert!(matches!(
            events.iter().find(|event| matches!(
                event,
                NormalizedEvent::Control(ProviderControl::Finished { .. })
            )),
            Some(NormalizedEvent::Control(ProviderControl::Finished {
                retries_used: 1,
                ..
            }))
        ));
    }

    /// Each retry of a step numbers itself in `x-stainless-retry-count` (0, then 1), as the SDK
    /// the Claude Code persona speaks as does, while the body is re-sent unchanged.
    #[tokio::test]
    async fn retries_number_their_attempt_in_the_stainless_retry_count() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 503,
                headers: Vec::new(),
                body: br#"{"error":{"message":"retry me"}}"#,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"successful-generation",
            },
        ])
        .await;
        let provider = retry_test_provider(&server, 0);
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert_eq!(
            server.request_header("x-stainless-retry-count"),
            vec![Some("0".to_string()), Some("1".to_string())]
        );
        let bodies = server.request_bodies();
        assert_eq!(bodies[0], bodies[1], "the retry re-sends the same bytes");
    }

    #[tokio::test]
    async fn http_503_retry_success_resets_display_and_resends_identical_bytes() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 503,
                headers: Vec::new(),
                body: br#"{"error":{"message":"retry me"}}"#,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"successful-generation",
            },
        ])
        .await;
        let provider = retry_test_provider(&server, 0);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    NormalizedEvent::Display(DisplayEvent::DisplayReset)
                ))
                .count(),
            1,
            "one reset separates the failed and successful generations"
        );
        let reset = events.iter().position(|event| {
            matches!(event, NormalizedEvent::Display(DisplayEvent::DisplayReset))
        });
        let successful_display = events.iter().position(|event| {
            matches!(
                event,
                NormalizedEvent::Display(DisplayEvent::DisplayGap { dropped: 1 })
            )
        });
        assert!(reset < successful_display);

        let finished: Vec<Option<u64>> = events
            .iter()
            .filter_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::Finished { usage, .. }) => {
                    Some(usage.output_tokens)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            finished,
            vec![Some(22)],
            "exactly one completion is published"
        );
        assert!(matches!(
            events.iter().find(|event| matches!(
                event,
                NormalizedEvent::Control(ProviderControl::Finished { .. })
            )),
            Some(NormalizedEvent::Control(ProviderControl::Finished {
                retries_used: 1,
                ..
            }))
        ));

        let requests = server.request_bodies();
        assert_eq!(requests.len(), 2, "the 503 is retried once");
        assert_eq!(
            provider.family.render_calls.load(Ordering::SeqCst),
            1,
            "the request body is rendered once for all attempts"
        );
        assert_eq!(
            requests[0], requests[1],
            "retry body bytes must be identical"
        );
    }

    #[tokio::test]
    async fn anthropic_overloaded_control_is_retried_before_publication() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"anthropic overloaded event",
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"successful-generation",
            },
        ])
        .await;
        let provider = retry_test_provider_with_behaviors(
            &server,
            vec![RetryAttemptBehavior::AnthropicOverloaded],
        );

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert_eq!(server.request_bodies().len(), 2);
        assert!(events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::AssistantMessage { .. })
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Finished { .. })
        )));
    }

    #[tokio::test]
    async fn clean_eof_without_completion_is_retried() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"clean empty eof",
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"successful-generation",
            },
        ])
        .await;
        let provider = retry_test_provider_with_behaviors(
            &server,
            vec![RetryAttemptBehavior::EmptyCompletion],
        );

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert_eq!(server.request_bodies().len(), 2);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    NormalizedEvent::Display(DisplayEvent::DisplayReset)
                ))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn failed_attempt_usage_is_discarded_before_success() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"failed-generation",
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"successful-generation",
            },
        ])
        .await;
        let provider = retry_test_provider(&server, 1);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let published_usage: Vec<Option<u64>> = events
            .iter()
            .filter_map(|event| match event {
                NormalizedEvent::Control(
                    ProviderControl::Finished { usage, .. }
                    | ProviderControl::StepFinished { usage, .. },
                ) => Some(usage.output_tokens),
                _ => None,
            })
            .collect();
        assert_eq!(
            published_usage,
            vec![Some(22)],
            "the failed attempt's buffered 999-token usage must be discarded"
        );
        assert_eq!(server.request_bodies().len(), 2);
    }

    /// Six sends, and between them the computed backoff: 2 s doubling to a 30 s cap, each wait
    /// within 20% of its nominal value. Virtual waits, so the minute of backoff costs nothing.
    #[tokio::test]
    async fn six_transport_failures_exhaust_the_attempt_budget() {
        let server = ScriptedServer::start(vec![ScriptResponse::Disconnect; 6]).await;
        let mut provider = retry_test_provider(&server, 0);
        let waits = use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert_eq!(
            terminal_error(&events).class,
            ErrorClass::ProviderUnavailable
        );
        assert!(matches!(
            events.iter().find(|event| matches!(
                event,
                NormalizedEvent::Control(ProviderControl::Error { .. })
            )),
            Some(NormalizedEvent::Control(ProviderControl::Error {
                retries_used: 5,
                ..
            }))
        ));
        assert_eq!(
            server.request_bodies().len(),
            6,
            "revert-check: the bounded loop must perform six sends, not one"
        );
        let waits = waits.lock().unwrap().clone();
        assert_eq!(waits.len(), 5);
        for (wait, nominal_secs) in waits.iter().zip([2.0, 4.0, 8.0, 16.0, 30.0]) {
            let secs = wait.as_secs_f64();
            assert!(
                (nominal_secs * 0.8..=nominal_secs * 1.2).contains(&secs),
                "wait {secs}s is outside 20% of {nominal_secs}s: {waits:?}"
            );
        }
    }

    #[tokio::test]
    async fn https_long_stream_cuts_do_not_consume_the_waiting_wall() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::MidChunkDisconnect,
            ScriptResponse::MidChunkDisconnect,
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"complete",
            },
        ])
        .await;
        let mut provider = retry_test_provider(&server, 0);
        use_virtual_retry_waits(&mut provider);
        provider.retry_clock.virtual_retry_attempts = Some(Arc::new(std::sync::Mutex::new(
            [305, 533, 1].into_iter().map(Duration::from_secs).collect(),
        )));
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        let requests = server.request_bodies();
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|body| body == &requests[0]));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    NormalizedEvent::Control(ProviderControl::AssistantMessage { .. })
                ))
                .count(),
            1
        );
    }

    async fn deadline_chunk_error_events(
        family: crate::spec::WireFamilyId,
        elapsed: Duration,
        response: ScriptResponse,
    ) -> (Vec<NormalizedEvent>, usize, usize) {
        let server = ScriptedServer::start(vec![response; 6]).await;
        let mut provider = retry_test_provider(&server, 0);
        Arc::make_mut(&mut provider.frozen).family_id = family;
        let waits = use_virtual_retry_waits(&mut provider);
        // Only attempt elapsed time is overridden; socket IO and retry-budget time stay independent.
        provider.retry_clock.virtual_attempt_elapsed = Some(elapsed);
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let wait_count = waits.lock().unwrap().len();
        (events, server.request_bodies().len(), wait_count)
    }

    fn assert_deadline_retry_count(events: &[NormalizedEvent], expected: u32) {
        assert!(events.iter().any(|event| matches!(event,
            NormalizedEvent::Control(ProviderControl::Error { retries_used, .. })
                if *retries_used == expected
        )));
    }

    #[tokio::test]
    async fn chatgpt_response_deadline_stops_retries() {
        for elapsed_secs in [895, 901] {
            let (events, sends, waits) = deadline_chunk_error_events(
                crate::spec::WireFamilyId::OpenAiResponsesChatgpt,
                Duration::from_secs(elapsed_secs),
                ScriptResponse::MidChunkDisconnect,
            )
            .await;
            let error = terminal_error(&events);
            assert_eq!(error.class, ErrorClass::Transient);
            assert_eq!(error.provider_code.as_deref(), Some("response_deadline"));
            assert!(error.message.contains("900 s limit"));
            assert!(error
                .message
                .contains("smaller turn or lower reasoning effort"));
            assert_deadline_retry_count(&events, 0);
            assert_eq!(sends, 1);
            assert_eq!(waits, 0);
        }
    }

    #[tokio::test]
    async fn chatgpt_response_deadline_early_chunk_errors_still_retry() {
        for elapsed_secs in [60, 894] {
            let (events, sends, waits) = deadline_chunk_error_events(
                crate::spec::WireFamilyId::OpenAiResponsesChatgpt,
                Duration::from_secs(elapsed_secs),
                ScriptResponse::MidChunkDisconnect,
            )
            .await;
            assert_eq!(
                terminal_error(&events).class,
                ErrorClass::ProviderUnavailable
            );
            assert_eq!(terminal_error(&events).provider_code, None);
            assert_deadline_retry_count(&events, 5);
            assert_eq!(sends, 6);
            assert_eq!(waits, 5);
        }
    }

    #[tokio::test]
    async fn chatgpt_response_deadline_other_family_still_retries() {
        let (events, sends, waits) = deadline_chunk_error_events(
            crate::spec::WireFamilyId::OpenAiChat,
            Duration::from_secs(901),
            ScriptResponse::MidChunkDisconnect,
        )
        .await;
        assert_eq!(
            terminal_error(&events).class,
            ErrorClass::ProviderUnavailable
        );
        assert_eq!(terminal_error(&events).provider_code, None);
        assert_deadline_retry_count(&events, 5);
        assert_eq!(sends, 6);
        assert_eq!(waits, 5);
    }

    #[tokio::test]
    async fn chatgpt_response_deadline_http_errors_still_retry() {
        let (events, sends, waits) = deadline_chunk_error_events(
            crate::spec::WireFamilyId::OpenAiResponsesChatgpt,
            Duration::from_secs(901),
            ScriptResponse::TruncatedHttp {
                status: 503,
                headers: Vec::new(),
                body: b"unavailable",
            },
        )
        .await;
        assert_eq!(terminal_error(&events).class, ErrorClass::Transient);
        assert_eq!(terminal_error(&events).status, Some(503));
        assert_ne!(
            terminal_error(&events).provider_code.as_deref(),
            Some("response_deadline")
        );
        assert_deadline_retry_count(&events, 5);
        assert_eq!(sends, 6);
        assert_eq!(waits, 5);
    }

    /// Make one test provider's retry waits virtual: they return at once and count as elapsed
    /// retry time. Returns the waits the step asked for, in order.
    fn use_virtual_retry_waits(
        provider: &mut LiveProvider<RetryTestFamily>,
    ) -> Arc<Mutex<Vec<Duration>>> {
        let waits = Arc::new(Mutex::new(Vec::new()));
        provider.retry_clock = super::RetryClock {
            virtual_waits: Some(Arc::clone(&waits)),
            ..super::RetryClock::default()
        };
        waits
    }

    /// Process-global lock for the stream-floor env vars so parallel tests cannot clobber each other.
    static STREAM_FLOOR_ENV_LOCK: Mutex<()> = Mutex::new(());

    struct StreamFloorEnvGuard {
        prev_bytes: Option<String>,
        prev_window: Option<String>,
    }

    impl StreamFloorEnvGuard {
        fn apply(
            bytes_per_sec: &str,
            window_secs: &str,
        ) -> (std::sync::MutexGuard<'static, ()>, Self) {
            let guard = STREAM_FLOOR_ENV_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let prev_bytes = std::env::var("BROCA_MIN_STREAM_BYTES_PER_SEC").ok();
            let prev_window = std::env::var("BROCA_MIN_STREAM_WINDOW_SECS").ok();
            std::env::set_var("BROCA_MIN_STREAM_BYTES_PER_SEC", bytes_per_sec);
            std::env::set_var("BROCA_MIN_STREAM_WINDOW_SECS", window_secs);
            (
                guard,
                Self {
                    prev_bytes,
                    prev_window,
                },
            )
        }
    }

    impl Drop for StreamFloorEnvGuard {
        fn drop(&mut self) {
            match &self.prev_bytes {
                Some(value) => std::env::set_var("BROCA_MIN_STREAM_BYTES_PER_SEC", value),
                None => std::env::remove_var("BROCA_MIN_STREAM_BYTES_PER_SEC"),
            }
            match &self.prev_window {
                Some(value) => std::env::set_var("BROCA_MIN_STREAM_WINDOW_SECS", value),
                None => std::env::remove_var("BROCA_MIN_STREAM_WINDOW_SECS"),
            }
        }
    }

    /// A stream that trickles below the configured floor must fail once as `Transient` and must not
    /// reissue the same request to the same provider. Retrying would only re-bill the degraded
    /// endpoint; the router needs the health signal immediately.
    ///
    /// Real wall time (not `start_paused`): the client parks on socket IO while the server drips,
    /// so virtual time would never auto-advance past the drip sleeps.
    #[tokio::test]
    // std Mutex across await is intentional: the floor env vars are process-global, so the lock
    // must cover the whole test body. No other task on this runtime re-acquires it.
    #[allow(clippy::await_holding_lock)]
    async fn stream_floor_breach_is_transient_and_not_reissued() {
        let (_lock, _env) = StreamFloorEnvGuard::apply("50", "1");

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let _ = socket.set_nodelay(true);
                let body = read_http_request_body(&mut socket).await;
                captured.lock().unwrap().push(body);
                // Fixed length large enough that a non-reissuing client disconnects mid-stream.
                let headers = concat!(
                    "HTTP/1.1 200 OK\r\n",
                    "Content-Type: text/plain\r\n",
                    "Content-Length: 64\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                );
                if socket.write_all(headers.as_bytes()).await.is_err() {
                    continue;
                }
                // ~4 content bytes/sec against a 50 byte/sec floor: breaches once a full 1s window elapses.
                for _ in 0..64 {
                    if socket.write_all(b"x").await.is_err() {
                        break;
                    }
                    let _ = socket.flush().await;
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        });

        let mut frozen = openai_chat_frozen_config("floor-no-reissue");
        frozen.base_url = Some(format!("http://{address}"));
        let provider = LiveProvider::new(
            RetryTestFamily::with_behaviors(vec![
                RetryAttemptBehavior::TextTrickle;
                super::MAX_ATTEMPTS
            ]),
            frozen,
            AuthHandle::None,
        );

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert_eq!(
            error.class,
            ErrorClass::Transient,
            "floor breach must stay Transient so the router can route around the provider"
        );
        assert!(
            error
                .message
                .contains("provider stream below the configured floor"),
            "unexpected floor message: {}",
            error.message
        );
        assert_eq!(
            requests.lock().unwrap().len(),
            1,
            "floor breach must not reissue the same request to the same provider"
        );
        assert!(matches!(
            events.iter().find(|event| matches!(
                event,
                NormalizedEvent::Control(ProviderControl::Error { .. })
            )),
            Some(NormalizedEvent::Control(ProviderControl::Error {
                retries_used: 0,
                ..
            }))
        ));
        server.abort();
    }

    #[tokio::test]
    async fn permanent_chat_stream_error_short_circuits_without_retry() {
        const ERROR_STREAM: &[u8] = concat!(
            "data: {\"error\":{\"message\":\"prompt violates validation\",",
            "\"type\":\"invalid_request_error\",\"code\":\"invalid_prompt\"}}\n\n",
        )
        .as_bytes();
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 200,
                headers: vec![("content-type", "text/event-stream")],
                body: ERROR_STREAM,
            },
            // The second response makes the legacy behavior deterministic: when the Chat
            // normalizer ignored `error`, EOF became a transient decode failure and resent the
            // same request. The fixed path must leave this response unused.
            ScriptResponse::Http {
                status: 200,
                headers: vec![("content-type", "text/event-stream")],
                body: ERROR_STREAM,
            },
        ])
        .await;
        let mut frozen = openai_chat_frozen_config("gpt-4o");
        frozen.base_url = Some(server.url.clone());
        let provider = LiveProvider::new(OpenAiChat, frozen, AuthHandle::None);
        let mut rx = provider.start_step(
            StepId(1),
            Arc::new(CallOptions::new(Vec::new())),
            CancellationToken::new(),
            None,
        );
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }

        let error = terminal_error(&events);
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(error.provider_code.as_deref(), Some("invalid_prompt"));
        assert_eq!(error.message, "prompt violates validation");
        assert_eq!(error.status, None);
        assert_eq!(server.request_bodies().len(), 1);
        assert!(events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Error {
                retries_used: 0,
                ..
            })
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(
                ProviderControl::StreamStart { .. } | ProviderControl::StepStarted { .. }
            ) | NormalizedEvent::Display(DisplayEvent::DisplayReset)
        )));
    }

    #[tokio::test]
    async fn nested_responses_stream_error_retries_and_completes() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 200,
                headers: vec![("content-type", "text/event-stream")],
                body: b"data: {\"type\":\"error\",\"error\":{\"type\":\"server_error\",\"code\":null,\"message\":\"An error occurred while processing your request.\"}}\n\n",
            },
            ScriptResponse::Http {
                status: 200,
                headers: vec![("content-type", "text/event-stream")],
                body: concat!(
                    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_success\"}}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_success\",\"status\":\"completed\"}}\n\n",
                ).as_bytes(),
            },
        ]).await;
        let mut frozen = openai_responses_frozen_config("gpt-4.1");
        frozen.base_url = Some(server.url.clone());
        let provider = LiveProvider::new(OpenAiResponses, frozen, AuthHandle::None);
        let mut rx = provider.start_step(
            StepId(1),
            Arc::new(CallOptions::new(Vec::new())),
            CancellationToken::new(),
            None,
        );
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        let bodies = server.request_bodies();
        assert_eq!(bodies.len(), 2);
        assert_eq!(bodies[0], bodies[1]);
        assert!(events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Finished {
                retries_used: 1,
                ..
            })
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Error { .. })
        )));
    }

    #[tokio::test]
    async fn permanent_responses_stream_error_short_circuits_without_retry() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 200,
            headers: vec![("content-type", "text/event-stream")],
            body: concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_error\"}}\n\n",
                "data: {\"type\":\"error\",\"code\":\"invalid_prompt\",\"message\":\"prompt violates validation\",\"param\":null,\"sequence_number\":1}\n\n",
            )
            .as_bytes(),
        }])
        .await;
        let mut frozen = openai_responses_frozen_config("gpt-4.1");
        frozen.base_url = Some(server.url.clone());
        let provider = LiveProvider::new(OpenAiResponses, frozen, AuthHandle::None);
        let mut rx = provider.start_step(
            StepId(1),
            Arc::new(CallOptions::new(Vec::new())),
            CancellationToken::new(),
            None,
        );
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }

        let error = terminal_error(&events);
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(error.provider_code.as_deref(), Some("invalid_prompt"));
        assert_eq!(error.message, "prompt violates validation");
        assert_eq!(server.request_bodies().len(), 1);
        assert!(events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Error {
                retries_used: 0,
                ..
            })
        )));
    }

    #[tokio::test]
    async fn permanent_http_error_short_circuits_without_retry() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 400,
            headers: Vec::new(),
            body: br#"{"error":{"message":"bad request"}}"#,
        }])
        .await;
        let provider = retry_test_provider(&server, 0);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(error.status, Some(400));
        assert_eq!(server.request_bodies().len(), 1);
    }

    #[tokio::test]
    async fn unreadable_forbidden_body_keeps_permanent_http_classification() {
        let server = ScriptedServer::start(vec![ScriptResponse::TruncatedHttp {
            status: 403,
            headers: Vec::new(),
            body: br#"{"error":{"message":"partial"}}"#,
        }])
        .await;
        let provider = retry_test_provider(&server, 0);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(error.status, Some(403));
        assert!(error.message.contains("error body unreadable"));
        assert_eq!(
            server.request_bodies().len(),
            1,
            "revert-check: a broken 403 body must not become a retryable transport failure"
        );
        assert_eq!(provider.escape_client_builds.load(Ordering::SeqCst), 0);
    }

    /// A live step that requested a tier and got the provider's refusal of it on a 403 ends
    /// permanent with the stable tier code, after one attempt; the same 403 on a step that
    /// requested no tier keeps its ordinary classification.
    #[tokio::test]
    async fn a_refused_service_tier_ends_the_step_with_the_tier_code() {
        const BODY: &[u8] = br#"{"error":{"message":"Your organization does not have access to priority processing.","type":"invalid_request_error","param":"service_tier","code":null}}"#;
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 403,
            headers: Vec::new(),
            body: BODY,
        }])
        .await;
        let mut provider = retry_test_provider(&server, 0);
        let mut frozen = (*provider.frozen).clone();
        crate::service_tier::freeze_service_tier(
            &mut frozen,
            "openai",
            crate::service_tier::ServiceTier::Priority,
        )
        .unwrap();
        provider.frozen = Arc::new(frozen);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(
            error.provider_code.as_deref(),
            Some("service_tier_unavailable")
        );
        assert!(error.message.contains("\"priority\""), "{}", error.message);
        assert!(error.message.contains("'openai'"), "{}", error.message);
        assert!(
            error
                .message
                .contains("does not have access to priority processing"),
            "{}",
            error.message
        );
        assert_eq!(server.request_bodies().len(), 1);

        // Control: the identical answer to a step without a tier is not a tier refusal.
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 403,
            headers: Vec::new(),
            body: BODY,
        }])
        .await;
        let provider = retry_test_provider(&server, 0);
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert_ne!(
            terminal_error(&events).provider_code.as_deref(),
            Some("service_tier_unavailable")
        );
    }

    /// Through the real step: a ChatGPT-family step whose first answer is the backend's own
    /// rejected API key retries and completes, rather than ending `auth_required`. The same answer
    /// on a metered-API step is a real dead key and ends auth after one attempt. This pins the
    /// wiring (the family recorded on the request), which the classifier's unit test cannot.
    #[tokio::test]
    async fn a_chatgpt_upstream_key_rejection_retries_through_the_real_step() {
        const BODY: &[u8] = br#"{"error":{"message":"Incorrect API key provided: sk-svcac***fvMA. You can find your API key at https://platform.openai.com/account/api-keys.","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}"#;
        let script = || {
            vec![
                ScriptResponse::Http {
                    status: 401,
                    headers: Vec::new(),
                    body: BODY,
                },
                ScriptResponse::Http {
                    status: 200,
                    headers: Vec::new(),
                    body: b"success",
                },
            ]
        };

        let server = ScriptedServer::start(script()).await;
        let mut provider = retry_test_provider(&server, 0);
        let mut frozen = (*provider.frozen).clone();
        frozen.family_id = crate::spec::WireFamilyId::OpenAiResponsesChatgpt;
        provider.frozen = Arc::new(frozen);
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(
            terminal_error_if_any(&events).is_none(),
            "a ChatGPT upstream key rejection must retry, not end the step"
        );
        assert_eq!(server.request_bodies().len(), 2);

        // Control: the metered API family's identical 401 is our own dead key.
        let server = ScriptedServer::start(script()).await;
        let provider = retry_test_provider(&server, 0);
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert_eq!(terminal_error(&events).class, ErrorClass::Auth);
        assert_eq!(server.request_bodies().len(), 1);
    }

    #[tokio::test]
    async fn unreadable_rate_limit_body_keeps_retry_after_pacing() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::TruncatedHttp {
                status: 429,
                headers: vec![("Retry-After", "0.1")],
                body: br#"{"error":{"message":"partial"}}"#,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            },
        ])
        .await;
        let provider = retry_test_provider(&server, 0);
        let started = Instant::now();

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert!(started.elapsed() >= Duration::from_millis(100));
        assert_eq!(server.request_bodies().len(), 2);
        assert_eq!(provider.escape_client_builds.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unreadable_http_body_keeps_explicit_retry_veto() {
        let server = ScriptedServer::start(vec![ScriptResponse::TruncatedHttp {
            status: 503,
            headers: vec![("x-should-retry", "false")],
            body: br#"{"error":{"message":"partial"}}"#,
        }])
        .await;
        let provider = retry_test_provider(&server, 0);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert_eq!(terminal_error(&events).status, Some(503));
        assert_eq!(server.request_bodies().len(), 1);
        assert_eq!(provider.escape_client_builds.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn request_timeout_http_408_is_retried() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 408,
                headers: Vec::new(),
                body: br#"{"error":{"message":"request timeout"}}"#,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            },
        ])
        .await;
        let provider = retry_test_provider(&server, 0);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert_eq!(server.request_bodies().len(), 2);
    }

    #[tokio::test]
    async fn retry_after_two_seconds_paces_the_second_attempt() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 429,
                headers: vec![("Retry-After", "2")],
                body: br#"{"error":{"message":"rate limited"}}"#,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            },
        ])
        .await;
        let provider = retry_test_provider(&server, 0);
        let started = Instant::now();

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert!(
            started.elapsed() >= Duration::from_secs(2),
            "Retry-After must delay the resend by at least two seconds"
        );
        assert_eq!(server.request_bodies().len(), 2);
    }

    #[test]
    fn in_band_retry_after_without_http_status_sets_the_delay_floor() {
        let error = ProviderError {
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Transient,
            message: "rate limited".into(),
            status: None,
            retry_after_secs: Some(2.5),
            provider_code: Some("rate_limit_exceeded".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        assert_eq!(retry_delay(&error, 0), Duration::from_millis(2_500));
    }

    #[test]
    fn retry_after_delay_is_capped_at_one_hundred_twenty_seconds() {
        let error = ProviderError {
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Transient,
            message: "rate limited".into(),
            status: Some(429),
            retry_after_secs: Some(900.0),
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        assert_eq!(retry_delay(&error, 0), Duration::from_secs(120));
    }

    #[test]
    fn computed_backoff_doubles_from_two_seconds_to_a_thirty_second_cap() {
        let error = ProviderError {
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::ProviderUnavailable,
            message: "connection reset".into(),
            status: None,
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        for (failed_attempt_index, nominal_secs) in [
            (0, 2.0),
            (1, 4.0),
            (2, 8.0),
            (3, 16.0),
            (4, 30.0),
            (5, 30.0),
            (40, 30.0),
        ] {
            for _ in 0..50 {
                let secs = retry_delay(&error, failed_attempt_index).as_secs_f64();
                assert!(
                    (nominal_secs * 0.8..=nominal_secs * 1.2).contains(&secs),
                    "attempt {failed_attempt_index}: {secs}s is outside 20% of {nominal_secs}s"
                );
            }
        }
    }

    /// A rate-limited step gets the same six sends as any other transient failure; there is no
    /// separate, smaller budget once a 429 is seen.
    #[tokio::test]
    async fn a_rate_limited_step_uses_the_full_attempt_budget() {
        let rate_limited = ScriptResponse::Http {
            status: 429,
            headers: vec![("Retry-After", "0")],
            body: br#"{"error":{"message":"rate limited"}}"#,
        };
        let server = ScriptedServer::start(vec![rate_limited; 6]).await;
        let mut provider = retry_test_provider(&server, 0);
        use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert_eq!(terminal_error(&events).status, Some(429));
        assert_eq!(server.request_bodies().len(), 6);
    }

    /// A provider asking for 120 s between every send would take ten minutes over six sends. The
    /// retry wall budget stops it: the first 120 s wait fits in 240 s, a second one does not.
    #[tokio::test]
    async fn a_long_retry_after_chain_stops_at_the_retry_wall_budget() {
        let rate_limited = ScriptResponse::Http {
            status: 429,
            headers: vec![("Retry-After", "120")],
            body: br#"{"error":{"message":"rate limited"}}"#,
        };
        let server = ScriptedServer::start(vec![rate_limited; 6]).await;
        let mut provider = retry_test_provider(&server, 0);
        let waits = use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert_eq!(error.status, Some(429));
        assert_eq!(error.retry_after_secs, Some(120.0));
        assert_eq!(server.request_bodies().len(), 2);
        assert_eq!(*waits.lock().unwrap(), vec![Duration::from_secs(120)]);
    }

    /// Serves one fixed fresh credential on every refresh request and counts the requests.
    struct FixedRefresher {
        account_ref: Option<String>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl super::CredentialRefresher for FixedRefresher {
        fn refresh_rejected(
            &self,
        ) -> crate::credential_refresh::RefreshFuture<
            '_,
            crate::credential_refresh::RefreshedCredential,
        > {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Some(crate::credential_refresh::RefreshedCredential {
                    account_ref: self.account_ref.clone(),
                    auth: AuthHandle::Bearer("fresh-test-token".into()),
                    evidence: super::CredentialRefresh {
                        vault_op: "get".into(),
                        min_ttl_ms: None,
                        force_refresh: true,
                        record_version: Some(2),
                        reason: Some(super::CredentialRefresh::REASON_REJECTED.into()),
                    },
                })
            })
        }
    }

    const UNAUTHORIZED: ScriptResponse = ScriptResponse::Http {
        status: 401,
        headers: Vec::new(),
        body: br#"{"error":{"message":"invalid token"}}"#,
    };

    /// A provider admitted with a token the script rejects first, plus the refresher it asks.
    fn refreshing_retry_test_provider(
        server: &ScriptedServer,
    ) -> (LiveProvider<RetryTestFamily>, Arc<FixedRefresher>) {
        let mut frozen = openai_chat_frozen_config("retry-test");
        frozen.base_url = Some(server.url.clone());
        let refresher = Arc::new(FixedRefresher {
            account_ref: None,
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let provider = LiveProvider::new(
            RetryTestFamily::new(0),
            frozen,
            AuthHandle::Bearer("rejected-test-token".into()),
        )
        .with_credential_refresher(refresher.clone());
        (provider, refresher)
    }

    /// A 401 recovered with a fresh token, whose re-send then meets a run of 503s: the re-send
    /// is one send on top of the ordinary budget, so the step still gets all its ordinary
    /// re-sends after it, with the backoff curve of a step that never refreshed, and all of it
    /// inside the retry wall.
    #[tokio::test]
    async fn a_refreshed_resend_that_meets_a_503_keeps_the_full_retry_budget() {
        let unavailable = ScriptResponse::Http {
            status: 503,
            headers: Vec::new(),
            body: br#"{"error":{"message":"overloaded"}}"#,
        };
        let mut script = vec![UNAUTHORIZED];
        // One more 503 than the budget allows, so an over-long retry would be served and seen.
        script.extend(std::iter::repeat_n(unavailable, super::MAX_ATTEMPTS + 1));
        let server = ScriptedServer::start(script).await;
        let (mut provider, refresher) = refreshing_retry_test_provider(&server);
        let waits = use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert_eq!(terminal_error(&events).status, Some(503));
        assert_eq!(refresher.calls.load(Ordering::SeqCst), 1);
        // The 401, the refreshed re-send, then the five ordinary re-sends MAX_ATTEMPTS leaves.
        assert_eq!(server.request_bodies().len(), super::MAX_ATTEMPTS + 1);
        let attempts = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    NormalizedEvent::Control(ProviderControl::AttemptFinished { .. })
                )
            })
            .count();
        assert_eq!(attempts, super::MAX_ATTEMPTS + 1, "one record per send");
        let waits = waits.lock().unwrap().clone();
        assert_eq!(
            waits.len(),
            super::MAX_ATTEMPTS - 1,
            "no wait before the re-send"
        );
        for (wait, nominal_secs) in waits.iter().zip([2.0, 4.0, 8.0, 16.0, 30.0]) {
            let secs = wait.as_secs_f64();
            assert!(
                (nominal_secs * 0.8..=nominal_secs * 1.2).contains(&secs),
                "wait {secs}s is outside 20% of {nominal_secs}s: {waits:?}"
            );
        }
        assert!(waits.iter().sum::<Duration>() <= super::RETRY_WALL_BUDGET);
    }

    /// The wall still binds a step that refreshed: a re-send answered by 120 s Retry-Afters
    /// gets one wait and one more send, exactly like a step without a refresh.
    #[tokio::test]
    async fn a_refreshed_resend_that_meets_long_retry_afters_stops_at_the_wall() {
        let rate_limited = ScriptResponse::Http {
            status: 429,
            headers: vec![("Retry-After", "120")],
            body: br#"{"error":{"message":"rate limited"}}"#,
        };
        let mut script = vec![UNAUTHORIZED];
        script.extend(std::iter::repeat_n(rate_limited, super::MAX_ATTEMPTS));
        let server = ScriptedServer::start(script).await;
        let (mut provider, refresher) = refreshing_retry_test_provider(&server);
        let waits = use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert_eq!(terminal_error(&events).status, Some(429));
        assert_eq!(refresher.calls.load(Ordering::SeqCst), 1);
        assert_eq!(server.request_bodies().len(), 3, "401, re-send, one retry");
        assert_eq!(*waits.lock().unwrap(), vec![Duration::from_secs(120)]);
    }

    /// The verbatim body production recorded for an Anthropic subscription account past its
    /// weekly limit. It is byte-identical to an ordinary short rate limit's body.
    const ANTHROPIC_ACCOUNT_LIMIT_BODY: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."},"request_id":"req_011CfRDPYNrfveajQccKCJ7o"}"#;

    /// The rate-limit headers of a real 429 captured 2026-09-26 01:59Z from an Anthropic
    /// subscription account whose seven-day window was used up (other headers omitted).
    const CAPTURED_EXHAUSTED_HEADERS: [(&str, &str); 14] = [
        ("x-should-retry", "true"),
        ("anthropic-ratelimit-unified-5h-status", "allowed"),
        (
            "anthropic-ratelimit-unified-representative-claim",
            "seven_day",
        ),
        (
            "anthropic-ratelimit-unified-overage-disabled-reason",
            "org_level_disabled",
        ),
        ("anthropic-ratelimit-unified-overage-status", "rejected"),
        ("anthropic-ratelimit-unified-reset", "1790740800"),
        ("anthropic-ratelimit-unified-5h-utilization", "0.21"),
        ("anthropic-ratelimit-unified-7d-reset", "1790740800"),
        ("anthropic-ratelimit-unified-5h-reset", "1790394000"),
        ("anthropic-ratelimit-unified-7d-status", "rejected"),
        ("anthropic-ratelimit-unified-7d-surpassed-threshold", "1.0"),
        ("retry-after", "352835"),
        ("anthropic-ratelimit-unified-7d-utilization", "1.0"),
        ("anthropic-ratelimit-unified-status", "rejected"),
    ];

    /// A 429 whose unified status is `allowed`: the account has room, so the refusal is an
    /// ordinary short throttle.
    const ORDINARY_RATE_LIMIT_HEADERS: [(&str, &str); 4] = [
        ("anthropic-ratelimit-unified-status", "allowed"),
        (
            "anthropic-ratelimit-unified-representative-claim",
            "five_hour",
        ),
        ("anthropic-ratelimit-unified-reset", "1790397000"),
        ("retry-after", "0"),
    ];

    fn classify_anthropic_429(
        family: WireFamilyId,
        headers: &[(&'static str, &'static str)],
        now: SystemTime,
    ) -> ProviderError {
        let headers = crate::wire::ResponseHeaders::new(headers.iter().copied());
        let retry_after_secs = headers
            .get("retry-after")
            .and_then(|value| parse_retry_after_value(value, now));
        let error = classify_http_error(429, ANTHROPIC_ACCOUNT_LIMIT_BODY, retry_after_secs, None);
        classify_account_limit_exhausted(
            family,
            429,
            &headers,
            ANTHROPIC_ACCOUNT_LIMIT_BODY,
            now,
            error,
        )
    }

    #[test]
    fn captured_anthropic_weekly_limit_429_is_account_limit_exhausted() {
        // 2026-09-26T01:59:25Z, the capture's own Date header.
        let now = UNIX_EPOCH + Duration::from_secs(1_790_387_965);
        let error = classify_anthropic_429(
            WireFamilyId::AnthropicMessages,
            &CAPTURED_EXHAUSTED_HEADERS,
            now,
        );
        assert_eq!(error.class, ErrorClass::Transient);
        assert_eq!(
            error.provider_code.as_deref(),
            Some(ACCOUNT_LIMIT_EXHAUSTED)
        );
        assert!(error.is_account_limit_exhausted());
        assert_eq!(error.status, Some(429));
        assert_eq!(error.retry_after_secs, Some(352_835.0));
        assert_eq!(error.resets_at_ms, Some(1_790_740_800_000));
        assert_eq!(error.resets_at_source.as_deref(), Some("header"));
        assert_eq!(error.limit_window.as_deref(), Some("seven_day"));
        assert_eq!(
            error.account_ref, None,
            "the provider does not know the account; the run stamps it"
        );
        assert_eq!(
            error.message,
            format!("429: {ANTHROPIC_ACCOUNT_LIMIT_BODY}")
        );
    }

    #[test]
    fn the_same_body_with_an_ordinary_short_limit_stays_rate_limit_error() {
        let now = UNIX_EPOCH + Duration::from_secs(1_790_387_965);
        for headers in [
            &ORDINARY_RATE_LIMIT_HEADERS[..],
            // No unified rate-limit headers at all (an API-key account's 429 carries none), so
            // there is no evidence the account is exhausted.
            &[("retry-after", "3")][..],
        ] {
            let error = classify_anthropic_429(WireFamilyId::AnthropicMessages, headers, now);
            assert_eq!(error.class, ErrorClass::Transient);
            assert_eq!(error.provider_code.as_deref(), Some("rate_limit_error"));
            assert_eq!(error.resets_at_ms, None);
            assert_eq!(error.limit_window, None);
            assert_eq!(error.account_ref, None);
        }
        // The unified headers are Anthropic's, so a 429 from any other family keeps its ordinary
        // classification even when it carries them.
        let error =
            classify_anthropic_429(WireFamilyId::OpenAiChat, &CAPTURED_EXHAUSTED_HEADERS, now);
        assert_eq!(error.provider_code.as_deref(), Some("rate_limit_error"));
    }

    /// A refused service tier stays a permanent tier refusal even if its 429 also says the account
    /// window is `rejected`: the reset would not make the tier available.
    #[test]
    fn a_refused_service_tier_is_not_relabelled_an_exhausted_account() {
        let now = UNIX_EPOCH + Duration::from_secs(1_790_387_965);
        let headers = crate::wire::ResponseHeaders::new(CAPTURED_EXHAUSTED_HEADERS.iter().copied());
        let tier_refusal = ProviderError {
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Permanent,
            message: "429: provider 'anthropic' refused service_tier \"priority\" for this account: Usage credits are required for fast mode.".into(),
            status: Some(429),
            retry_after_secs: None,
            provider_code: Some(crate::service_tier::SERVICE_TIER_UNAVAILABLE.into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        let error = classify_account_limit_exhausted(
            WireFamilyId::AnthropicMessages,
            429,
            &headers,
            ANTHROPIC_ACCOUNT_LIMIT_BODY,
            now,
            tier_refusal,
        );
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(
            error.provider_code.as_deref(),
            Some(crate::service_tier::SERVICE_TIER_UNAVAILABLE)
        );
        assert_eq!(error.resets_at_ms, None);
    }

    #[test]
    fn an_exhausted_account_without_a_reset_header_resets_after_the_retry_after() {
        let now = UNIX_EPOCH + Duration::from_secs(1_790_387_965);
        let error = classify_anthropic_429(
            WireFamilyId::AnthropicMessages,
            &CAPTURED_EXHAUSTED_HEADERS
                .into_iter()
                .filter(|(name, _)| *name != "anthropic-ratelimit-unified-reset")
                .collect::<Vec<_>>(),
            now,
        );
        assert_eq!(
            error.provider_code.as_deref(),
            Some(ACCOUNT_LIMIT_EXHAUSTED)
        );
        assert_eq!(
            error.resets_at_ms,
            Some((1_790_387_965 + 352_835) * 1_000),
            "now plus the retry-after"
        );
        assert_eq!(error.resets_at_source.as_deref(), Some("retry_after"));
        assert_eq!(error.limit_window.as_deref(), Some("seven_day"));
    }

    #[test]
    fn an_account_limit_refusal_is_never_an_auth_failure() {
        let error = classify_anthropic_429(
            WireFamilyId::AnthropicMessages,
            &CAPTURED_EXHAUSTED_HEADERS,
            SystemTime::now(),
        );
        assert_ne!(error.class, ErrorClass::Auth);
    }

    fn anthropic_retry_test_provider(server: &ScriptedServer) -> LiveProvider<RetryTestFamily> {
        let mut frozen = openai_chat_frozen_config("retry-test");
        frozen.base_url = Some(server.url.clone());
        // Only the family id matters here: it decides which family's error vocabulary
        // classifies a non-2xx answer.
        frozen.family_id = WireFamilyId::AnthropicMessages;
        LiveProvider::new(RetryTestFamily::new(0), frozen, AuthHandle::None)
    }

    #[tokio::test]
    async fn an_exhausted_account_ends_the_step_after_one_attempt_without_sleeping() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 429,
            headers: CAPTURED_EXHAUSTED_HEADERS.to_vec(),
            body: ANTHROPIC_ACCOUNT_LIMIT_BODY.as_bytes(),
        }])
        .await;
        let mut provider = anthropic_retry_test_provider(&server);
        let waits = use_virtual_retry_waits(&mut provider);
        let started = Instant::now();

        // No deadline: without the veto, the 98-hour retry-after would park the step for the
        // 120-second cap and then re-send.
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert_eq!(
            error.provider_code.as_deref(),
            Some(ACCOUNT_LIMIT_EXHAUSTED)
        );
        assert_eq!(error.resets_at_ms, Some(1_790_740_800_000));
        assert_eq!(error.resets_at_source.as_deref(), Some("header"));
        assert_eq!(error.limit_window.as_deref(), Some("seven_day"));
        assert_eq!(
            server.request_bodies().len(),
            1,
            "never re-sent within the step"
        );
        assert!(waits.lock().unwrap().is_empty(), "no retry wait");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "no retry-after park"
        );
    }

    #[tokio::test]
    async fn an_ordinary_anthropic_429_is_still_retried() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 429,
                headers: ORDINARY_RATE_LIMIT_HEADERS.to_vec(),
                body: ANTHROPIC_ACCOUNT_LIMIT_BODY.as_bytes(),
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            },
        ])
        .await;
        let mut provider = anthropic_retry_test_provider(&server);
        use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert_eq!(server.request_bodies().len(), 2);
    }

    /// A real ChatGPT/Codex 429 body for an account whose plan allowance was used up, copied byte
    /// for byte from the terminal error broca's write-ahead log recorded for a production run on
    /// 2026-09-16 (WAL file `4430787e24105d49.wal`). Using the captured bytes rather than a
    /// hand-written body pins the shape the backend actually sends.
    const CHATGPT_USAGE_LIMIT_BODY: &str = r#"{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"pro","resets_at":1790021043,"eligible_promo":null,"resets_in_seconds":441721}}"#;

    const KIMI_WEEKLY_USAGE_LIMIT_BODY: &str = r#"{"error":{"message":"You've reached your weekly (7-day) usage limit. Your quota will reset when the current 7-day window ends. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/membership/subscription?tab=quota","type":"access_terminated_error"}}"#;

    fn classify_chatgpt_429(family: WireFamilyId, body: &str, now: SystemTime) -> ProviderError {
        let headers = crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>());
        let error = classify_http_error(429, body, None, None);
        classify_account_limit_exhausted(family, 429, &headers, body, now, error)
    }

    #[test]
    fn captured_chatgpt_usage_limit_429_is_account_limit_exhausted() {
        // Now = when that run recorded the error (2026-09-16T17:22:02Z), as it would be live.
        let now = UNIX_EPOCH + Duration::from_millis(1_789_579_322_081);
        let error = classify_chatgpt_429(
            WireFamilyId::OpenAiResponsesChatgpt,
            CHATGPT_USAGE_LIMIT_BODY,
            now,
        );
        assert_eq!(error.class, ErrorClass::Transient);
        assert!(error.is_account_limit_exhausted());
        assert_eq!(error.status, Some(429));
        assert_eq!(error.resets_at_ms, Some(1_790_021_043_000));
        assert_eq!(error.resets_at_source.as_deref(), Some("body"));
        assert_eq!(
            error.limit_window, None,
            "the body does not name a window, so none is guessed"
        );
        assert_eq!(error.account_ref, None);
        assert_eq!(error.retry_after_secs, None);
        assert_eq!(error.message, format!("429: {CHATGPT_USAGE_LIMIT_BODY}"));
        assert_ne!(error.class, ErrorClass::Auth);
    }

    #[test]
    fn a_chatgpt_usage_limit_without_resets_at_resets_after_resets_in_seconds() {
        let now = UNIX_EPOCH + Duration::from_secs(1_789_579_322);
        let body = r#"{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","resets_in_seconds":441721}}"#;
        let error = classify_chatgpt_429(WireFamilyId::OpenAiResponsesChatgpt, body, now);
        assert!(error.is_account_limit_exhausted());
        assert_eq!(error.resets_at_ms, Some((1_789_579_322 + 441_721) * 1_000));
        assert_eq!(error.resets_at_source.as_deref(), Some("retry_after"));
    }

    #[test]
    fn captured_kimi_weekly_usage_limit_403_is_account_limit_exhausted() {
        let error = classify_account_limit_exhausted(
            WireFamilyId::OpenAiChat,
            403,
            &crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>()),
            KIMI_WEEKLY_USAGE_LIMIT_BODY,
            UNIX_EPOCH,
            classify_http_error(403, KIMI_WEEKLY_USAGE_LIMIT_BODY, None, None),
        );
        assert_eq!(error.class, ErrorClass::Transient);
        assert!(error.is_account_limit_exhausted());
        assert_eq!(
            error.provider_code.as_deref(),
            Some(ACCOUNT_LIMIT_EXHAUSTED)
        );
        assert_eq!(error.resets_at_ms, None);
        assert_eq!(error.limit_window, None);
        assert_eq!(
            error.message,
            format!("403: {KIMI_WEEKLY_USAGE_LIMIT_BODY}")
        );
    }

    #[test]
    fn kimi_access_termination_requires_a_usage_limit_message() {
        let classify = |body: &str| {
            classify_account_limit_exhausted(
                WireFamilyId::OpenAiChat,
                403,
                &crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>()),
                body,
                UNIX_EPOCH,
                classify_http_error(403, body, None, None),
            )
        };
        let banned =
            classify(r#"{"error":{"type":"access_terminated_error","message":"Account banned"}}"#);
        assert_eq!(banned.class, ErrorClass::Permanent);
        assert_eq!(
            banned.provider_code.as_deref(),
            Some("access_terminated_error")
        );
        let wrong_type =
            classify(r#"{"error":{"type":"other_error","message":"Weekly usage limit reached"}}"#);
        assert_eq!(wrong_type.class, ErrorClass::Permanent);
    }

    #[test]
    fn only_the_chatgpt_usage_limit_body_on_the_chatgpt_family_is_an_account_wall() {
        let now = UNIX_EPOCH + Duration::from_secs(1_789_579_322);
        // A metered OpenAI API 429 keeps its ordinary classification, even with this body.
        for family in [WireFamilyId::OpenAiResponses, WireFamilyId::OpenAiChat] {
            let error = classify_chatgpt_429(family, CHATGPT_USAGE_LIMIT_BODY, now);
            assert_eq!(error.provider_code.as_deref(), Some("usage_limit_reached"));
            assert_eq!(error.resets_at_ms, None);
        }
        // An ordinary ChatGPT throttle is not a plan wall.
        let throttle = r#"{"error":{"type":"rate_limit_exceeded","message":"Rate limit reached"}}"#;
        let error = classify_chatgpt_429(WireFamilyId::OpenAiResponsesChatgpt, throttle, now);
        assert!(!error.is_account_limit_exhausted());
        assert_eq!(error.provider_code.as_deref(), Some("rate_limit_exceeded"));
        assert_eq!(error.resets_at_ms, None);
    }

    /// Every ChatGPT-family HTTP refusal, run through the same chain the live HTTPS transport
    /// runs: the provider-code and status table, then the upstream-key overlay, then the
    /// account-limit overlay. Each row pins the complete recorded error, field by field, so a
    /// change in where the ChatGPT decisions live cannot change what a run records.
    #[test]
    fn chatgpt_http_refusals_keep_their_recorded_errors() {
        use super::{classify_chatgpt_upstream_key_rejection, CHATGPT_UPSTREAM_KEY_REJECTED};
        const UPSTREAM_KEY_BODY: &str = r#"{"error":{"message":"Incorrect API key provided: sk-svcac***********fvMA. You can find your API key at https://platform.openai.com/account/api-keys.","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}"#;
        const TOKEN_EXPIRED_BODY: &str = r#"{"error":{"message":"Your authentication token has expired.","type":"invalid_request_error","code":"token_expired"}}"#;
        const UPSTREAM_KEY_MESSAGE: &str = concat!(
            "401: chatgpt_upstream_key_rejected: the ChatGPT backend rejected an OpenAI API key ",
            // Split so this copy is not a second match for the diagnostic's mutation anchor.
            "on its own side (broca sent the account's OAuth token, not a key); ",
            "not a credential failure."
        );
        let family = WireFamilyId::OpenAiResponsesChatgpt;
        let now_ms = 1_789_579_322_081u64;
        let now = UNIX_EPOCH + Duration::from_millis(now_ms);
        let classify = |status: u16, headers: &[(&str, &str)], body: &str, retry: Option<f64>| {
            let headers = crate::wire::ResponseHeaders::new(headers.iter().copied());
            let error = super::classify_http_error_in_family(
                status,
                body,
                retry,
                None,
                Some(family),
                headers.get("x-amzn-errortype"),
            );
            let error = classify_chatgpt_upstream_key_rejection(family, status, body, error);
            classify_account_limit_exhausted(family, status, &headers, body, now, error)
        };
        let expected = |status: u16,
                        class: ErrorClass,
                        message: String,
                        retry_after_secs: Option<f64>,
                        provider_code: Option<&str>,
                        resets: Option<(u64, &str)>| ProviderError {
            provider_subcode: None,
            refusal: None,
            class,
            message,
            status: Some(status),
            retry_after_secs,
            provider_code: provider_code.map(str::to_owned),
            resets_at_ms: resets.map(|(time, _)| time),
            resets_at_source: resets.map(|(_, source)| source.to_owned()),
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        let usage_no_reset = r#"{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached"}}"#;
        let usage_string_reset = r#"{"error":{"type":"usage_limit_reached","resets_at":"1790021043","resets_in_seconds":60}}"#;
        let usage_not_included_code =
            r#"{"error":{"message":"not in plan","code":"usage_not_included"}}"#;
        let usage_not_included_type =
            r#"{"error":{"type":"usage_not_included","message":"not in plan"}}"#;
        let not_an_sk_key = r#"{"error":{"message":"Incorrect API key provided: sess-123","code":"invalid_api_key"}}"#;
        let indented_sk_key = r#"{"error":{"message":" Incorrect API key provided: sk-123"}}"#;
        let throttle = r#"{"error":{"type":"rate_limit_exceeded","message":"Rate limit reached"}}"#;
        let overloaded = r#"{"error":{"message":"overloaded"}}"#;
        let anthropic_wall_headers = [
            ("anthropic-ratelimit-unified-status", "rejected"),
            ("anthropic-ratelimit-unified-reset", "1790740800"),
        ];
        let cases: Vec<(&str, ProviderError, ProviderError)> = vec![
            (
                "captured usage limit: the body's absolute reset wins over Retry-After",
                classify(429, &[], CHATGPT_USAGE_LIMIT_BODY, Some(30.0)),
                expected(
                    429,
                    ErrorClass::Transient,
                    format!("429: {CHATGPT_USAGE_LIMIT_BODY}"),
                    Some(30.0),
                    Some(ACCOUNT_LIMIT_EXHAUSTED),
                    Some((1_790_021_043_000, "body")),
                ),
            ),
            (
                "usage limit without body resets falls back to Retry-After",
                classify(429, &[], usage_no_reset, Some(30.5)),
                expected(
                    429,
                    ErrorClass::Transient,
                    format!("429: {usage_no_reset}"),
                    Some(30.5),
                    Some(ACCOUNT_LIMIT_EXHAUSTED),
                    Some((now_ms + 30_500, "retry_after")),
                ),
            ),
            (
                "usage limit without any reset keeps the reset unknown",
                classify(429, &[], usage_no_reset, None),
                expected(
                    429,
                    ErrorClass::Transient,
                    format!("429: {usage_no_reset}"),
                    None,
                    Some(ACCOUNT_LIMIT_EXHAUSTED),
                    None,
                ),
            ),
            (
                "a non-numeric resets_at is skipped for resets_in_seconds",
                classify(429, &[], usage_string_reset, None),
                expected(
                    429,
                    ErrorClass::Transient,
                    format!("429: {usage_string_reset}"),
                    None,
                    Some(ACCOUNT_LIMIT_EXHAUSTED),
                    Some((now_ms + 60_000, "retry_after")),
                ),
            ),
            (
                "the usage-limit body on a 403 is not an account wall",
                classify(403, &[], CHATGPT_USAGE_LIMIT_BODY, None),
                expected(
                    403,
                    ErrorClass::Permanent,
                    format!("403: {CHATGPT_USAGE_LIMIT_BODY}"),
                    None,
                    Some("usage_limit_reached"),
                    None,
                ),
            ),
            (
                "usage_not_included as a code",
                classify(429, &[], usage_not_included_code, None),
                expected(
                    429,
                    ErrorClass::Permanent,
                    format!("429: {usage_not_included_code}"),
                    None,
                    Some("usage_not_included"),
                    None,
                ),
            ),
            (
                "usage_not_included as a type",
                classify(429, &[], usage_not_included_type, None),
                expected(
                    429,
                    ErrorClass::Permanent,
                    format!("429: {usage_not_included_type}"),
                    None,
                    Some("usage_not_included"),
                    None,
                ),
            ),
            (
                "captured upstream sk- key rejection keeps its Retry-After",
                classify(401, &[], UPSTREAM_KEY_BODY, Some(2.0)),
                expected(
                    401,
                    ErrorClass::Transient,
                    UPSTREAM_KEY_MESSAGE.to_owned(),
                    Some(2.0),
                    Some(CHATGPT_UPSTREAM_KEY_REJECTED),
                    None,
                ),
            ),
            (
                "the sk- body on a 403 is not an upstream key rejection",
                classify(403, &[], UPSTREAM_KEY_BODY, None),
                expected(
                    403,
                    // The invalid_api_key code outranks the 403 status default.
                    ErrorClass::Auth,
                    format!("403: auth rejected — re-authenticate to get a fresh token. body: {UPSTREAM_KEY_BODY}"),
                    None,
                    Some("invalid_api_key"),
                    None,
                ),
            ),
            (
                "a 401 naming a key that is not sk- is our credential",
                classify(401, &[], not_an_sk_key, None),
                expected(
                    401,
                    ErrorClass::Auth,
                    format!("401: auth rejected — re-authenticate to get a fresh token. body: {not_an_sk_key}"),
                    None,
                    Some("invalid_api_key"),
                    None,
                ),
            ),
            (
                "the sk- prefix must start the message",
                classify(401, &[], indented_sk_key, None),
                expected(
                    401,
                    ErrorClass::Auth,
                    format!("401: auth rejected — re-authenticate to get a fresh token. body: {indented_sk_key}"),
                    None,
                    None,
                    None,
                ),
            ),
            (
                "an expired ChatGPT token is a credential failure",
                classify(401, &[], TOKEN_EXPIRED_BODY, None),
                expected(
                    401,
                    ErrorClass::Auth,
                    format!("401: auth rejected — re-authenticate to get a fresh token. body: {TOKEN_EXPIRED_BODY}"),
                    None,
                    Some("token_expired"),
                    None,
                ),
            ),
            (
                "an ordinary throttle stays a retryable rate limit",
                classify(429, &[], throttle, Some(2.0)),
                expected(
                    429,
                    ErrorClass::Transient,
                    format!("429: {throttle}"),
                    Some(2.0),
                    Some("rate_limit_exceeded"),
                    None,
                ),
            ),
            (
                "Anthropic's account-wall headers mean nothing on the ChatGPT family",
                classify(429, &anthropic_wall_headers, throttle, None),
                expected(
                    429,
                    ErrorClass::Transient,
                    format!("429: {throttle}"),
                    None,
                    Some("rate_limit_exceeded"),
                    None,
                ),
            ),
            (
                "a non-JSON 429 is an ordinary throttle",
                classify(429, &[], "busy", None),
                expected(429, ErrorClass::Transient, "429: busy".to_owned(), None, None, None),
            ),
            (
                "an overloaded 503 is retryable",
                classify(503, &[], overloaded, None),
                expected(
                    503,
                    ErrorClass::Transient,
                    format!("503: {overloaded}"),
                    None,
                    None,
                    None,
                ),
            ),
        ];
        for (label, actual, expected) in cases {
            assert_eq!(actual, expected, "{label}");
        }
    }

    /// Run one answer through broca's ChatGPT HTTPS classification chain, as `drive_attempt`
    /// does: Retry-After parsing, the status and code table, then both ChatGPT overlays.
    fn run_chatgpt_chain(
        status: u16,
        headers: &[(String, String)],
        body: &str,
        now: SystemTime,
    ) -> ProviderError {
        use super::classify_chatgpt_upstream_key_rejection;
        let family = WireFamilyId::OpenAiResponsesChatgpt;
        let mut header_map = reqwest::header::HeaderMap::new();
        for (name, value) in headers {
            header_map.append(
                reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        let retry = parse_retry_after_header(&header_map);
        let headers = crate::wire::ResponseHeaders::new(
            headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        );
        let error = super::classify_http_error_in_family(
            status,
            body,
            retry,
            None,
            Some(family),
            headers.get("x-amzn-errortype"),
        );
        let error = classify_chatgpt_upstream_key_rejection(family, status, body, error);
        classify_account_limit_exhausted(family, status, &headers, body, now, error)
    }

    /// The shared ChatGPT policy's golden vectors, run through broca's transport chain: each
    /// vector's class must be what broca records for that answer.
    #[test]
    fn shared_chatgpt_response_vectors_agree_with_the_transport() {
        use super::CHATGPT_UPSTREAM_KEY_REJECTED;
        let document: Value = serde_json::from_str(include_str!(
            "../../cortexkit-chatgpt-policy/test-vectors/responses.json"
        ))
        .unwrap();
        let vectors = document["vectors"].as_array().unwrap();
        assert!(vectors.len() >= 7);
        for vector in vectors {
            let name = vector["name"].as_str().unwrap();
            let status = vector["status"].as_u64().unwrap() as u16;
            let headers: Vec<(String, String)> = vector["headers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|pair| {
                    (
                        pair[0].as_str().unwrap().to_owned(),
                        pair[1].as_str().unwrap().to_owned(),
                    )
                })
                .collect();
            let body = vector["body"].as_str().unwrap();
            let now = UNIX_EPOCH + Duration::from_millis(vector["now_ms"].as_u64().unwrap());
            let error = run_chatgpt_chain(status, &headers, body, now);
            let expected = &vector["expected"];
            let account_wall = error.is_account_limit_exhausted();
            let upstream = error.provider_code.as_deref() == Some(CHATGPT_UPSTREAM_KEY_REJECTED);
            match expected["class"].as_str().unwrap() {
                "account_limit_exhausted" => {
                    assert!(account_wall, "{name}");
                    assert_eq!(
                        error.resets_at_ms,
                        expected["resets_at_ms"].as_u64(),
                        "{name}"
                    );
                    assert_eq!(
                        error.resets_at_source.as_deref(),
                        expected["resets_at_source"].as_str(),
                        "{name}"
                    );
                }
                "usage_not_included" => {
                    assert_eq!(error.class, ErrorClass::Permanent, "{name}");
                    assert_eq!(
                        error.provider_code.as_deref(),
                        Some("usage_not_included"),
                        "{name}"
                    );
                }
                "auth_failure" => assert_eq!(error.class, ErrorClass::Auth, "{name}"),
                "upstream_key_rejection" => {
                    assert!(upstream, "{name}");
                    assert_eq!(error.class, ErrorClass::Transient, "{name}");
                }
                "rate_limited" | "overloaded" => {
                    assert_eq!(error.class, ErrorClass::Transient, "{name}");
                    assert!(!account_wall && !upstream, "{name}");
                }
                other => panic!("{name}: unexpected class {other}"),
            }
        }
    }

    /// Over every vector body at every refusal status, the shared policy and broca's transport
    /// agree on the classes broca acts on: an account wall, the backend's own key rejection, a
    /// credential failure, and a plan without the requested use.
    #[test]
    fn the_shared_chatgpt_policy_agrees_with_the_transport_on_every_status() {
        use super::CHATGPT_UPSTREAM_KEY_REJECTED;
        use cortexkit_chatgpt_policy::Classification;
        let document: Value = serde_json::from_str(include_str!(
            "../../cortexkit-chatgpt-policy/test-vectors/responses.json"
        ))
        .unwrap();
        let now = UNIX_EPOCH + Duration::from_millis(1_789_579_322_081);
        let mut bodies: Vec<&str> = document["vectors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|vector| vector["body"].as_str().unwrap())
            .collect();
        bodies.extend([
            "not json",
            "{}",
            r#"{"error":{"type":"api_error"}}"#,
            r#"{"error":{"type":"overloaded_error"}}"#,
            r#"{"error":{"code":"insufficient_quota"}}"#,
            r#"{"error":{"code":"authentication_error"}}"#,
            r#"{"error":{"type":"permission_error"}}"#,
            r#"[{"error":{"code":"usage_not_included"}}]"#,
        ]);
        for status in [400, 401, 403, 404, 408, 410, 429, 500, 502, 503, 529] {
            for body in &bodies {
                let error = run_chatgpt_chain(status, &[], body, now);
                let class = cortexkit_chatgpt_policy::classify_response(
                    status,
                    std::iter::empty::<(&str, &str)>(),
                    body,
                    now,
                );
                let label = format!("{status} {body}");
                assert_eq!(
                    matches!(class, Classification::AccountLimitExhausted { .. }),
                    error.is_account_limit_exhausted(),
                    "{label}"
                );
                assert_eq!(
                    class == Classification::UpstreamKeyRejection,
                    error.provider_code.as_deref() == Some(CHATGPT_UPSTREAM_KEY_REJECTED),
                    "{label}"
                );
                assert_eq!(
                    class == Classification::AuthFailure,
                    error.class == ErrorClass::Auth,
                    "{label}"
                );
                assert_eq!(
                    class == Classification::UsageNotIncluded,
                    error.provider_code.as_deref() == Some("usage_not_included"),
                    "{label}"
                );
                if matches!(
                    class,
                    Classification::RateLimited | Classification::Overloaded
                ) {
                    assert_eq!(error.class, ErrorClass::Transient, "{label}");
                }
            }
        }
    }

    #[tokio::test]
    async fn a_kimi_weekly_usage_limit_ends_the_run_after_one_attempt() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 403,
            headers: Vec::new(),
            body: KIMI_WEEKLY_USAGE_LIMIT_BODY.as_bytes(),
        }])
        .await;
        let mut provider = retry_test_provider(&server, 0);
        let mut frozen = (*provider.frozen).clone();
        frozen.family_id = WireFamilyId::OpenAiChat;
        provider.frozen = Arc::new(frozen);
        let waits = use_virtual_retry_waits(&mut provider);
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert!(error.is_account_limit_exhausted());
        assert_eq!(
            error.provider_code.as_deref(),
            Some(ACCOUNT_LIMIT_EXHAUSTED)
        );
        assert_eq!(error.resets_at_ms, None);
        assert_eq!(server.request_bodies().len(), 1);
        assert!(waits.lock().unwrap().is_empty());
    }

    const XAI_SPENDING_LIMIT_BODY: &str = r#"{"code":"personal-team-blocked:spending-limit","error":"You have run out of credits or need a Grok subscription..."}"#;

    #[test]
    fn xai_captured_production_403_is_billing_exhausted_on_both_routes() {
        // Captured from production: the refusal text is abbreviated, but its code is complete.
        let body = r#"{"code":"personal-team-blocked:spending-limit","error":"You have run out of credits or need a Grok subscription. …"}"#;
        let headers = crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>());
        for family in [WireFamilyId::OpenAiChat, WireFamilyId::OpenAiResponses] {
            let error =
                super::classify_http_error_in_family(403, body, None, None, Some(family), None);
            assert!(error.is_billing_exhausted());
            assert_eq!(error.class, ErrorClass::Permanent);
            assert!(
                !super::is_rejected_token(&error),
                "a spending limit must not refresh the login"
            );
            let classified =
                classify_account_limit_exhausted(family, 403, &headers, body, UNIX_EPOCH, error);
            assert!(classified.is_billing_exhausted());
            assert_eq!(classified.status, Some(403));
            assert!(!super::is_rejected_token(&classified));
        }
    }

    #[test]
    fn extract_provider_code_reads_top_level_code_when_error_is_string() {
        assert_eq!(
            extract_provider_code(XAI_SPENDING_LIMIT_BODY).as_deref(),
            Some("personal-team-blocked:spending-limit")
        );
        let top_level_type = r#"{"type":"some_type","error":"failure message"}"#;
        assert_eq!(
            extract_provider_code(top_level_type).as_deref(),
            Some("some_type")
        );
    }

    #[test]
    fn xai_spending_limit_403_is_billing_exhausted() {
        let headers = crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>());
        for status in [402, 403] {
            let error = classify_http_error(status, XAI_SPENDING_LIMIT_BODY, None, None);
            assert_eq!(error.class, ErrorClass::Permanent);
            assert!(error.is_billing_exhausted());
            assert_eq!(error.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
            assert_eq!(error.resets_at_ms, None);
            assert_eq!(error.resets_at_source, None);
            assert_eq!(error.limit_window, None);
            assert_eq!(error.status, Some(status));

            let raw_error = ProviderError {
                provider_subcode: None,
                class: ErrorClass::Permanent,
                message: "403: forbidden".into(),
                status: Some(status),
                retry_after_secs: None,
                provider_code: Some("personal-team-blocked:spending-limit".into()),
                refusal: None,
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            };
            let classified = classify_account_limit_exhausted(
                WireFamilyId::OpenAiChat,
                status,
                &headers,
                XAI_SPENDING_LIMIT_BODY,
                UNIX_EPOCH,
                raw_error,
            );
            assert_eq!(classified.class, ErrorClass::Permanent);
            assert!(classified.is_billing_exhausted());
            assert_eq!(classified.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
            assert_eq!(classified.resets_at_ms, None);
        }
    }

    /// OpenRouter's 402 for a request its credit cannot pay for. RECONSTRUCTED from a production
    /// refusal: the message is verbatim, but the production `metadata` held more fields than the
    /// one kept here, and those were not recorded.
    const OPENROUTER_CREDIT_402_BODY: &str = r#"{"error":{"message":"This request requires more credits, or fewer max_tokens. You requested up to 128000 tokens, but can only afford 97335. To increase, visit https://openrouter.ai/settings/credits and add more credits","code":402,"metadata":{"limit_source":"openrouter_credits"}}}"#;

    fn classify_openrouter(status: u16, body: &str) -> ProviderError {
        let headers = crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>());
        let error = super::classify_http_error_in_family(
            status,
            body,
            None,
            None,
            Some(WireFamilyId::OpenAiChat),
            None,
        );
        classify_account_limit_exhausted(
            WireFamilyId::OpenAiChat,
            status,
            &headers,
            body,
            UNIX_EPOCH,
            error,
        )
    }

    #[test]
    fn openrouter_credit_402_is_credit_insufficient_with_the_affordable_max_tokens() {
        // Today's code before this classification: a plain permanent with no provider code.
        let unclassified = classify_http_error(402, OPENROUTER_CREDIT_402_BODY, None, None);
        assert_eq!(unclassified.provider_code, None);
        let error = classify_openrouter(402, OPENROUTER_CREDIT_402_BODY);
        assert_eq!(error.class, ErrorClass::Permanent);
        assert!(error.is_credit_insufficient());
        assert_eq!(error.provider_code.as_deref(), Some(CREDIT_INSUFFICIENT));
        assert!(
            !error.is_billing_exhausted(),
            "an unaffordable request is not an exhausted balance"
        );
        assert!(error.is_credential_scoped());
        assert_eq!(error.affordable_max_tokens, Some(97_335));
        assert_eq!(error.status, Some(402));
        assert_eq!(error.resets_at_ms, None);
        assert_eq!(error.retry_after_secs, None);
        assert!(crate::safe_provider_code(CREDIT_INSUFFICIENT));

        // The number is optional: a refusal whose message does not state it keeps the code.
        let unnumbered = OPENROUTER_CREDIT_402_BODY.replace("can only afford 97335", "cannot");
        let error = classify_openrouter(402, &unnumbered);
        assert!(error.is_credit_insufficient());
        assert_eq!(error.affordable_max_tokens, None);
    }

    #[test]
    fn credit_insufficient_needs_status_402_and_the_openrouter_credit_source() {
        let other_source = OPENROUTER_CREDIT_402_BODY.replace("openrouter_credits", "key_limit");
        let no_source = OPENROUTER_CREDIT_402_BODY
            .replace(r#","metadata":{"limit_source":"openrouter_credits"}"#, "");
        for (status, body) in [
            (403, OPENROUTER_CREDIT_402_BODY),
            (429, OPENROUTER_CREDIT_402_BODY),
            (402, other_source.as_str()),
            // The message alone, however telling, does not decide.
            (402, no_source.as_str()),
        ] {
            let error = classify_openrouter(status, body);
            assert!(!error.is_credit_insufficient(), "{status} {body}");
            assert_eq!(error.affordable_max_tokens, None);
        }
    }

    /// Anthropic's out-of-credit refusal, from its public error shape: assumed, not captured.
    /// Replace with the first real refusal once one is recorded.
    const ANTHROPIC_CREDIT_BALANCE_BODY: &str = r#"{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}"#;

    #[test]
    fn anthropic_credit_balance_400_is_billing_exhausted() {
        let shouted = ANTHROPIC_CREDIT_BALANCE_BODY
            .replace("credit balance is too low", "CREDIT BALANCE IS TOO LOW");
        for body in [ANTHROPIC_CREDIT_BALANCE_BODY, shouted.as_str()] {
            // Both classifiers relabel the refusal: `classify_http_error_in_family`, the live
            // path, which is told the wire family, and `classify_http_error`, the public entry
            // point, which infers the family from the Anthropic body shape.
            for error in [
                super::classify_http_error_in_family(
                    400,
                    body,
                    None,
                    None,
                    Some(WireFamilyId::AnthropicMessages),
                    None,
                ),
                classify_http_error(400, body, None, None),
            ] {
                assert_eq!(error.class, ErrorClass::Permanent);
                assert!(error.is_billing_exhausted());
                assert_eq!(error.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
                assert_eq!(error.status, Some(400));
                assert_eq!(error.resets_at_ms, None);
            }
        }
    }

    #[test]
    fn other_anthropic_invalid_request_400s_keep_their_class() {
        let generic = r#"{"type":"error","error":{"type":"invalid_request_error","message":"messages: text content blocks must be non-empty"}}"#;
        let overflow = r#"{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 210000 tokens > 200000 maximum"}}"#;
        let family = Some(WireFamilyId::AnthropicMessages);
        let error = super::classify_http_error_in_family(400, generic, None, None, family, None);
        assert_eq!(error.class, ErrorClass::Permanent);
        // Not an overflow or a billing refusal, so it gets the typed request-validation code.
        assert_eq!(error.provider_code.as_deref(), Some(REQUEST_INVALID));
        let error = super::classify_http_error_in_family(400, overflow, None, None, family, None);
        assert_eq!(error.class, ErrorClass::ContextOverflow);
        assert_eq!(
            error.provider_code.as_deref(),
            Some("context_length_exceeded")
        );
        // The phrase is an Anthropic rule only: another family's invalid request is not billing.
        let foreign = r#"{"error":{"type":"invalid_request_error","message":"Your credit balance is too low"}}"#;
        let error = super::classify_http_error_in_family(
            400,
            foreign,
            None,
            None,
            Some(WireFamilyId::OpenAiChat),
            None,
        );
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(error.provider_code.as_deref(), Some(REQUEST_INVALID));
    }

    #[tokio::test]
    async fn an_anthropic_credit_refusal_ends_the_step_after_one_attempt() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 400,
            headers: Vec::new(),
            body: ANTHROPIC_CREDIT_BALANCE_BODY.as_bytes(),
        }])
        .await;
        let mut provider = retry_test_provider(&server, 0);
        let mut frozen = (*provider.frozen).clone();
        frozen.family_id = WireFamilyId::AnthropicMessages;
        provider.frozen = Arc::new(frozen);
        let waits = use_virtual_retry_waits(&mut provider);
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert!(error.is_billing_exhausted());
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(server.request_bodies().len(), 1);
        assert!(waits.lock().unwrap().is_empty());
    }

    #[test]
    fn object_error_body_remains_unchanged() {
        let body = r#"{"error":{"code":"invalid_request_error","message":"bad parameter"}}"#;
        let error = classify_http_error(400, body, None, None);
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(
            error.provider_code.as_deref(),
            Some("invalid_request_error")
        );
        assert_eq!(error.status, Some(400));
    }

    #[test]
    fn openai_insufficient_quota_is_billing_exhausted() {
        let body = r#"{"error":{"code":"insufficient_quota","message":"You exceeded your current quota"}}"#;
        let error = classify_http_error(429, body, None, None);
        assert_eq!(error.class, ErrorClass::Permanent);
        assert!(error.is_billing_exhausted());
        assert_eq!(error.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
        assert_eq!(error.resets_at_ms, None);
    }

    #[test]
    fn http_403_with_unknown_code_is_plain_permanent_with_no_provider_code() {
        let headers = crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>());
        for body in [
            r#"{"error":"Forbidden"}"#,
            r#"{"error":{"message":"Access denied"}}"#,
            r#"{"message":"Forbidden"}"#,
        ] {
            let error = classify_http_error(403, body, None, None);
            assert_eq!(error.class, ErrorClass::Permanent);
            assert_eq!(error.provider_code, None);
            assert_eq!(error.status, Some(403));
            assert_eq!(error.resets_at_ms, None);

            let classified = classify_account_limit_exhausted(
                WireFamilyId::OpenAiChat,
                403,
                &headers,
                body,
                UNIX_EPOCH,
                error,
            );
            assert_eq!(classified.class, ErrorClass::Permanent);
            assert_eq!(classified.provider_code, None);
        }
    }

    #[tokio::test]
    async fn a_spending_limit_ends_the_run_after_one_attempt() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 403,
            headers: Vec::new(),
            body: XAI_SPENDING_LIMIT_BODY.as_bytes(),
        }])
        .await;
        let mut provider = retry_test_provider(&server, 0);
        let mut frozen = (*provider.frozen).clone();
        frozen.family_id = WireFamilyId::OpenAiChat;
        provider.frozen = Arc::new(frozen);
        let waits = use_virtual_retry_waits(&mut provider);
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert!(error.is_billing_exhausted());
        assert_eq!(error.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(error.resets_at_ms, None);
        assert_eq!(server.request_bodies().len(), 1);
        assert!(waits.lock().unwrap().is_empty());
    }

    fn chatgpt_retry_test_provider(server: &ScriptedServer) -> LiveProvider<RetryTestFamily> {
        let mut provider = retry_test_provider(server, 0);
        let mut frozen = (*provider.frozen).clone();
        frozen.family_id = WireFamilyId::OpenAiResponsesChatgpt;
        provider.frozen = Arc::new(frozen);
        provider
    }

    #[tokio::test]
    async fn a_chatgpt_usage_limit_ends_the_step_after_one_attempt() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 429,
            headers: Vec::new(),
            body: CHATGPT_USAGE_LIMIT_BODY.as_bytes(),
        }])
        .await;
        let mut provider = chatgpt_retry_test_provider(&server);
        let waits = use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert!(error.is_account_limit_exhausted());
        assert_eq!(error.resets_at_ms, Some(1_790_021_043_000));
        assert_eq!(error.resets_at_source.as_deref(), Some("body"));
        assert_eq!(
            server.request_bodies().len(),
            1,
            "never re-sent within the step"
        );
        assert!(waits.lock().unwrap().is_empty(), "no retry wait");
    }

    #[tokio::test]
    async fn an_ordinary_chatgpt_429_is_still_retried() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 429,
                headers: Vec::new(),
                body: br#"{"error":{"type":"rate_limit_exceeded","message":"Rate limit reached"}}"#,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            },
        ])
        .await;
        let mut provider = chatgpt_retry_test_provider(&server);
        use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert_eq!(server.request_bodies().len(), 2);
    }

    /// An Antigravity (Code Assist) quota-wall 429 body. Everything up to and including
    /// `"quotaResetTimeStamp": "2026-` is copied byte for byte from the terminal error broca's
    /// write-ahead log recorded for a production run on 2026-10-03 (WAL file
    /// `5224d2a1d3c2f34b.wal`); the log's message cap cut the body there. The rest is COMPLETED
    /// by hand: the timestamp `2026-10-03T03:40:39Z` (the last 429 at 03:19:38Z plus the stated
    /// 21m0.96s delay) and the closing brackets. The real timestamp value is unknown.
    const CODE_ASSIST_QUOTA_EXHAUSTED_BODY: &str = r#"{
  "error": {
    "code": 429,
    "message": "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 21m0s.",
    "status": "RESOURCE_EXHAUSTED",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "QUOTA_EXHAUSTED",
        "domain": "cloudcode-pa.googleapis.com",
        "metadata": {
          "model": "gemini-3.8-flash-high",
          "quotaResetDelay": "21m0.959250039s",
          "quotaResetTimeStamp": "2026-10-03T03:40:39Z"
        }
      }
    ]
  }
}"#;
    /// `2026-10-03T03:40:39Z` in epoch milliseconds.
    const CODE_ASSIST_QUOTA_RESET_MS: u64 = 1_790_998_839_000;

    /// The captured body with only the relative `quotaResetDelay` in its metadata.
    const CODE_ASSIST_QUOTA_DELAY_ONLY_BODY: &str = r#"{"error":{"code":429,"message":"Individual quota reached.","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED","domain":"cloudcode-pa.googleapis.com","metadata":{"model":"gemini-3.8-flash-high","quotaResetDelay":"21m0.959250039s"}}]}}"#;
    /// 21m0.959250039s, rounded up to the next whole millisecond.
    const CODE_ASSIST_QUOTA_DELAY_MS: u64 = 1_260_960;

    /// The captured body with neither reset field in its metadata.
    const CODE_ASSIST_QUOTA_NO_RESET_BODY: &str = r#"{"error":{"code":429,"message":"Individual quota reached.","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED","domain":"cloudcode-pa.googleapis.com","metadata":{"model":"gemini-3.8-flash-high"}}]}}"#;

    /// Google's per-minute rate limits share the `RESOURCE_EXHAUSTED` status but not the
    /// `QUOTA_EXHAUSTED` reason: one with no details at all, one with a different ErrorInfo reason.
    const GOOGLE_RATE_LIMIT_BODIES: [&str; 2] = [
        r#"{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED"}}"#,
        r#"{"error":{"code":429,"message":"Quota exceeded for quota metric 'Generate Content requests per minute'.","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"RATE_LIMIT_EXCEEDED","domain":"googleapis.com","metadata":{"quotaResetDelay":"20s"}}]}}"#,
    ];

    fn classify_google_429(family: WireFamilyId, body: &str, now: SystemTime) -> ProviderError {
        let headers = crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>());
        let error = classify_http_error(429, body, None, None);
        classify_account_limit_exhausted(family, 429, &headers, body, now, error)
    }

    #[test]
    fn captured_code_assist_quota_429_resets_at_its_stated_timestamp() {
        // Now = the last 429 of the recorded run (2026-10-03T03:19:38Z). The body also carries a
        // delay; the absolute timestamp wins over it.
        let now = UNIX_EPOCH + Duration::from_secs(1_790_997_578);
        for family in [WireFamilyId::GoogleCodeAssist, WireFamilyId::Gemini] {
            let error = classify_google_429(family, CODE_ASSIST_QUOTA_EXHAUSTED_BODY, now);
            assert_eq!(error.class, ErrorClass::Transient);
            assert!(error.is_account_limit_exhausted(), "{family:?}");
            assert_eq!(error.status, Some(429));
            assert_eq!(error.resets_at_ms, Some(CODE_ASSIST_QUOTA_RESET_MS));
            assert_eq!(error.resets_at_source.as_deref(), Some("body"));
            assert_eq!(error.limit_window, None, "Google names no window");
            assert_eq!(error.account_ref, None, "the run stamps it");
        }
    }

    #[test]
    fn a_code_assist_quota_429_without_a_timestamp_resets_after_its_delay() {
        let now = UNIX_EPOCH + Duration::from_secs(1_790_997_578);
        let error = classify_google_429(
            WireFamilyId::GoogleCodeAssist,
            CODE_ASSIST_QUOTA_DELAY_ONLY_BODY,
            now,
        );
        assert!(error.is_account_limit_exhausted());
        assert_eq!(
            error.resets_at_ms,
            Some(1_790_997_578_000 + CODE_ASSIST_QUOTA_DELAY_MS)
        );
        assert_eq!(error.resets_at_source.as_deref(), Some("retry_after"));
    }

    #[test]
    fn a_code_assist_quota_429_without_any_reset_is_still_an_account_wall() {
        let now = UNIX_EPOCH + Duration::from_secs(1_790_997_578);
        let error = classify_google_429(
            WireFamilyId::GoogleCodeAssist,
            CODE_ASSIST_QUOTA_NO_RESET_BODY,
            now,
        );
        assert!(error.is_account_limit_exhausted());
        assert_eq!(error.resets_at_ms, None);
        assert_eq!(error.resets_at_source, None);
    }

    #[test]
    fn a_malformed_quota_reset_delay_leaves_the_reset_unknown_but_still_classifies_the_wall() {
        let now = UNIX_EPOCH + Duration::from_secs(1_790_997_578);
        let body = CODE_ASSIST_QUOTA_DELAY_ONLY_BODY.replace("21m0.959250039s", "21 minutes");
        let error = classify_google_429(WireFamilyId::GoogleCodeAssist, &body, now);
        assert!(error.is_account_limit_exhausted());
        assert_eq!(error.resets_at_ms, None);
        assert_eq!(error.resets_at_source, None);
    }

    #[test]
    fn go_durations_parse_to_whole_milliseconds_rounding_up() {
        let parse = super::parse_go_duration_millis;
        assert_eq!(parse("21m0.959250039s"), Some(1_260_960));
        assert_eq!(parse("1h30m"), Some(5_400_000));
        assert_eq!(parse("500ms"), Some(500));
        assert_eq!(parse("1.5s"), Some(1_500));
        assert_eq!(parse(".5s"), Some(500));
        assert_eq!(parse("1500us"), Some(2));
        assert_eq!(parse("1ns"), Some(1));
        assert_eq!(parse("0"), Some(0));
        for malformed in ["", "21", "21 minutes", "-5s", "1.2.3s", "s", ".s", "5d"] {
            assert_eq!(parse(malformed), None, "{malformed:?}");
        }
    }

    fn google_retry_test_provider(server: &ScriptedServer) -> LiveProvider<RetryTestFamily> {
        let mut provider = retry_test_provider(server, 0);
        let mut frozen = (*provider.frozen).clone();
        frozen.family_id = WireFamilyId::GoogleCodeAssist;
        provider.frozen = Arc::new(frozen);
        provider
    }

    #[tokio::test]
    async fn a_code_assist_quota_wall_ends_the_step_after_one_attempt() {
        for (body, expected_source) in [
            (CODE_ASSIST_QUOTA_EXHAUSTED_BODY, Some("body")),
            (CODE_ASSIST_QUOTA_DELAY_ONLY_BODY, Some("retry_after")),
            (CODE_ASSIST_QUOTA_NO_RESET_BODY, None),
        ] {
            let server = ScriptedServer::start(vec![ScriptResponse::Http {
                status: 429,
                headers: Vec::new(),
                body: body.as_bytes(),
            }])
            .await;
            let mut provider = google_retry_test_provider(&server);
            let waits = use_virtual_retry_waits(&mut provider);

            let before_ms = super::now_millis();
            let events = collect_step_events(&provider, CancellationToken::new(), None).await;
            let after_ms = super::now_millis();
            let error = terminal_error(&events);
            assert!(error.is_account_limit_exhausted(), "{expected_source:?}");
            assert_eq!(error.resets_at_source.as_deref(), expected_source);
            match expected_source {
                Some("body") => assert_eq!(error.resets_at_ms, Some(CODE_ASSIST_QUOTA_RESET_MS)),
                Some(_) => {
                    // The delay is added to the time the response was classified, which falls
                    // somewhere inside the step.
                    let resets_at_ms = error.resets_at_ms.expect("reset from the delay");
                    assert!(resets_at_ms >= before_ms + CODE_ASSIST_QUOTA_DELAY_MS);
                    assert!(resets_at_ms <= after_ms + CODE_ASSIST_QUOTA_DELAY_MS);
                }
                None => assert_eq!(error.resets_at_ms, None),
            }
            assert_eq!(error.limit_window, None);
            assert_eq!(
                server.request_bodies().len(),
                1,
                "never re-sent within the step ({expected_source:?})"
            );
            assert!(waits.lock().unwrap().is_empty(), "no retry wait");
        }
    }

    #[tokio::test]
    async fn a_code_assist_quota_delay_uses_the_injected_clock() {
        const FIXED_NOW_MS: u64 = 1_800_000_000_000;
        let root = broca_test_support::scratch_root().join(format!(
            "provider-injected-quota-clock-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("test-clock"),
            serde_json::json!({ "now_ms": FIXED_NOW_MS }).to_string(),
        )
        .unwrap();
        let clock = broca_core::clock::Clock::for_state_root(&root).unwrap();
        assert_eq!(clock.now_ms(), FIXED_NOW_MS);

        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 429,
            headers: Vec::new(),
            body: CODE_ASSIST_QUOTA_DELAY_ONLY_BODY.as_bytes(),
        }])
        .await;
        let mut provider = google_retry_test_provider(&server).with_clock(clock);
        let waits = use_virtual_retry_waits(&mut provider);
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);

        assert!(error.is_account_limit_exhausted());
        assert_eq!(error.resets_at_source.as_deref(), Some("retry_after"));
        assert_eq!(
            error.resets_at_ms,
            Some(FIXED_NOW_MS + CODE_ASSIST_QUOTA_DELAY_MS)
        );
        assert_eq!(server.request_bodies().len(), 1);
        assert!(waits.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_google_rate_limit_without_quota_exhausted_is_still_retried_and_ends_transient() {
        for body in GOOGLE_RATE_LIMIT_BODIES {
            let rate_limited = ScriptResponse::Http {
                status: 429,
                headers: Vec::new(),
                body: body.as_bytes(),
            };
            let server = ScriptedServer::start(vec![rate_limited; super::MAX_ATTEMPTS]).await;
            let mut provider = google_retry_test_provider(&server);
            use_virtual_retry_waits(&mut provider);

            let events = collect_step_events(&provider, CancellationToken::new(), None).await;
            let error = terminal_error(&events);
            assert!(!error.is_account_limit_exhausted(), "{body}");
            assert_eq!(error.class, ErrorClass::Transient);
            assert_eq!(error.status, Some(429));
            assert_eq!(error.resets_at_ms, None);
            assert_eq!(server.request_bodies().len(), super::MAX_ATTEMPTS);
        }
    }

    /// A 429 does not reduce the number of attempts available to the step, so a 503 after it is
    /// still retried under the same six-attempt budget.
    #[tokio::test]
    async fn a_503_after_a_429_is_still_retried() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 429,
                headers: vec![("Retry-After", "0")],
                body: br#"{"error":{"message":"rate limited"}}"#,
            },
            ScriptResponse::Http {
                status: 503,
                headers: Vec::new(),
                body: br#"{"error":{"message":"still unavailable"}}"#,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            },
        ])
        .await;
        let mut provider = retry_test_provider(&server, 0);
        use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert_eq!(server.request_bodies().len(), 3);
    }

    #[tokio::test]
    async fn retry_after_that_cannot_fit_watchdog_returns_typed_429_immediately() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 429,
            headers: vec![("Retry-After", "2")],
            body: br#"{"error":{"message":"rate limited"}}"#,
        }])
        .await;
        let provider = retry_test_provider(&server, 0);
        let started = Instant::now();

        let events = collect_step_events(
            &provider,
            CancellationToken::new(),
            Some(Instant::now() + Duration::from_millis(100)),
        )
        .await;
        let error = terminal_error(&events);
        assert_eq!(error.status, Some(429));
        assert_eq!(error.retry_after_secs, Some(2.0));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(server.request_bodies().len(), 1);
    }

    #[tokio::test]
    async fn cancellation_during_retry_after_park_stops_before_resend() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 429,
                headers: vec![("Retry-After", "60")],
                body: br#"{"error":{"message":"rate limited"}}"#,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"must not be sent",
            },
        ])
        .await;
        let provider = retry_test_provider(&server, 0);
        let cancel = CancellationToken::new();
        let mut rx = provider.start_step(
            StepId(1),
            Arc::new(CallOptions::new(Vec::new())),
            cancel.clone(),
            None,
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.request_bodies().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first attempt was not sent");

        let started = Instant::now();
        cancel.cancel();
        // Exactly one attempt record precedes the close: the one attempt that was sent. The
        // server seeing the request does not mean the client has read the 429 yet, so the
        // cancel lands either in that attempt (recorded `cancelled`) or in the backoff after
        // it (recorded as the 429). Either way nothing is resent.
        let first = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("cancelled backoff did not stop promptly");
        match first {
            Some(NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt })) => {
                assert_eq!(attempt.attempt, 0);
                assert!(
                    (attempt.outcome == AttemptOutcome::HttpError
                        && attempt.http_status == Some(429))
                        || attempt.outcome == AttemptOutcome::Cancelled,
                    "{attempt:?}"
                );
            }
            other => panic!("expected the 429 attempt record, got {other:?}"),
        }
        assert!(tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("cancelled backoff did not stop promptly")
            .is_none());
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(server.request_bodies().len(), 1);
    }

    #[tokio::test]
    async fn elapsed_deadline_cancels_step_before_first_send() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 200,
            headers: Vec::new(),
            body: b"must not be sent",
        }])
        .await;
        let provider = retry_test_provider(&server, 0);
        let cancel = CancellationToken::new();

        let events = collect_step_events(
            &provider,
            cancel.clone(),
            Some(Instant::now() - Duration::from_millis(1)),
        )
        .await;
        assert!(events.is_empty());
        assert!(cancel.is_cancelled());
        assert_eq!(server.request_bodies().len(), 0);
        assert_eq!(provider.escape_client_builds.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn explicit_should_retry_false_blocks_transient_http_retry() {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 503,
            headers: vec![("x-should-retry", "false")],
            body: br#"{"error":{"message":"do not retry"}}"#,
        }])
        .await;
        let provider = retry_test_provider(&server, 0);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert_eq!(error.status, Some(503));
        assert_eq!(server.request_bodies().len(), 1);
    }

    #[tokio::test]
    async fn h1_escape_client_is_used_only_for_first_transport_retry() {
        let transport_server = ScriptedServer::start(vec![
            ScriptResponse::Disconnect,
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            },
        ])
        .await;
        let transport_provider = retry_test_provider(&transport_server, 0);
        let events = collect_step_events(&transport_provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert_eq!(
            transport_provider
                .escape_client_builds
                .load(Ordering::SeqCst),
            1
        );

        let http_server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 503,
                headers: Vec::new(),
                body: br#"{"error":{"message":"retry normally"}}"#,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            },
        ])
        .await;
        let http_provider = retry_test_provider(&http_server, 0);
        let events = collect_step_events(&http_provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert_eq!(http_provider.escape_client_builds.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn h1_escape_follows_a_transport_failure_on_the_second_attempt() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 503,
                headers: Vec::new(),
                body: br#"{"error":{"message":"retry normally"}}"#,
            },
            ScriptResponse::Disconnect,
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            },
        ])
        .await;
        let provider = retry_test_provider(&server, 0);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none());
        assert_eq!(server.request_bodies().len(), 3);
        assert_eq!(
            provider.escape_client_builds.load(Ordering::SeqCst),
            1,
            "the third attempt must be the step's one H1 escape attempt"
        );
    }

    #[tokio::test]
    async fn deterministic_decode_failure_is_capped_at_one_retry() {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"malformed-one",
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"malformed-two",
            },
        ])
        .await;
        let provider = retry_test_provider(&server, 3);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert_eq!(error.class, ErrorClass::Transient);
        assert!(error.message.contains("injected malformed stream"));
        assert_eq!(server.request_bodies().len(), 2);
    }

    fn terminal_error_if_any(events: &[NormalizedEvent]) -> Option<&ProviderError> {
        events.iter().find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => Some(error),
            _ => None,
        })
    }

    #[tokio::test]
    async fn cancelling_transport_drops_the_stalled_connection() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_seen_tx, request_seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0, "client disconnected before sending the request");
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            request_seen_tx.send(()).unwrap();

            let mut byte = [0u8; 1];
            loop {
                match socket.read(&mut byte).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });

        let mut frozen = openai_chat_frozen_config("test-model");
        frozen.base_url = Some(format!("http://{address}"));
        let provider = LiveProvider::new(OpenAiChat, frozen, AuthHandle::None);
        let cancel = CancellationToken::new();
        let mut rx = provider.start_step(
            StepId(1),
            Arc::new(CallOptions::new(Vec::new())),
            cancel.clone(),
            None,
        );

        tokio::time::timeout(Duration::from_secs(2), request_seen_rx)
            .await
            .expect("provider request was not received")
            .expect("stall server stopped before receiving the request");
        cancel.cancel();
        // A cancel is cooperative: the stalled attempt is recorded as `cancelled`, with no
        // status line and no response bytes, and only then does the stream close.
        match tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("cancelled provider stream did not record the attempt promptly")
        {
            Some(NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt })) => {
                assert_eq!(attempt.attempt, 0);
                assert_eq!(attempt.outcome, AttemptOutcome::Cancelled);
                assert_eq!(attempt.http_status, None);
                assert_eq!(attempt.response_body_len, 0);
            }
            other => panic!("expected the cancelled attempt record, got {other:?}"),
        }
        assert!(tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("cancelled provider stream did not close promptly")
            .is_none());
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server did not observe the connection closing")
            .expect("stall server panicked");
    }

    #[tokio::test]
    async fn openai_chat_truncated_eof_is_a_transient_normalization_error() {
        let wire = b"data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n";
        let stream = futures::stream::iter([Ok::<_, std::io::Error>(wire.as_slice())]);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let frozen = openai_chat_frozen_config("test-model");

        let error = drive_body_stream(stream, OpenAiChat.normalizer(&frozen, StepId(1)), &tx)
            .await
            .expect_err("unterminated OpenAI chat stream must fail normalization");

        assert_eq!(error.error.class, ErrorClass::Transient);
        assert!(error
            .error
            .message
            .contains("ended before a terminal chunk or [DONE]"));
        while let Ok(event) = rx.try_recv() {
            assert!(!matches!(
                event,
                NormalizedEvent::Control(
                    ProviderControl::AssistantMessage { .. } | ProviderControl::Finished { .. }
                )
            ));
        }
    }

    #[tokio::test]
    async fn gemini_truncated_eof_is_a_transient_normalization_error() {
        let wire = b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"partial\"}],\"role\":\"model\"},\"index\":0}]}\n\n";
        let stream = futures::stream::iter([Ok::<_, std::io::Error>(wire.as_slice())]);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let frozen = gemini_frozen_config("test-model");

        let error = drive_body_stream(stream, Gemini.normalizer(&frozen, StepId(1)), &tx)
            .await
            .expect_err("unterminated Gemini stream must fail normalization");

        assert_eq!(error.error.class, ErrorClass::Transient);
        assert!(error
            .error
            .message
            .contains("Gemini stream truncated: ended before a terminal chunk"));
        while let Ok(event) = rx.try_recv() {
            assert!(!matches!(
                event,
                NormalizedEvent::Control(
                    ProviderControl::AssistantMessage { .. }
                        | ProviderControl::StepFinished { .. }
                        | ProviderControl::Finished { .. }
                )
            ));
        }
    }

    #[tokio::test]
    async fn cancellation_after_stream_eof_wins_before_terminal_publication() {
        let stream = futures::stream::iter([Ok::<_, std::io::Error>(b"last event".to_vec())]);
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let completion = drive_body_stream(stream, Box::new(CompletionNormalizer), &tx)
            .await
            .unwrap();
        assert_eq!(
            completion.len(),
            2,
            "the message and terminal event are held behind the fence"
        );

        tx.send(NormalizedEvent::Display(DisplayEvent::DisplayGap {
            dropped: 7,
        }))
        .await
        .unwrap();
        let cancel = CancellationToken::new();
        let publish_tx = tx.clone();
        let publish_cancel = cancel.clone();
        let publisher = tokio::spawn(async move {
            publish_completion(completion, 0, &publish_tx, &publish_cancel).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        publisher.await.unwrap();
        drop(tx);

        assert!(matches!(
            rx.recv().await,
            Some(NormalizedEvent::Display(DisplayEvent::DisplayGap {
                dropped: 7
            }))
        ));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn response_header_stall_uses_stream_timeout_classification() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_seen_tx, request_seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let read = socket.read(&mut chunk).await.unwrap();
                if read == 0 {
                    return;
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            request_seen_tx.send(()).unwrap();
            futures::future::pending::<()>().await;
        });
        let request = HttpRequest {
            method: HttpMethod::Post,
            url: format!("http://{address}/stall"),
            headers: Vec::new(),
            body: Vec::new(),
        };
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move {
            drive_stream_with_header_timeout(
                &reqwest::Client::new().into(),
                &PreparedHttpRequest::from(request),
                Box::new(CountingNormalizer(Arc::new(AtomicUsize::new(0)))),
                &tx,
                Duration::from_millis(50),
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(2), request_seen_rx)
            .await
            .expect("header-stall server did not receive the request")
            .unwrap();
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.error.class, ErrorClass::Transient);
        assert_eq!(
            error.error.message,
            "provider stream idle: waiting for response headers for 300s"
        );
        server.abort();
    }

    #[tokio::test]
    async fn non_2xx_error_body_stall_times_out_and_classifies_by_status() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (headers_sent_tx, headers_sent_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let read = socket.read(&mut chunk).await.unwrap();
                if read == 0 {
                    return;
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            socket
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 100\r\nContent-Type: text/plain\r\n\r\n",
                )
                .await
                .unwrap();
            socket.flush().await.unwrap();
            headers_sent_tx.send(()).unwrap();
            futures::future::pending::<()>().await;
        });

        let request = HttpRequest {
            method: HttpMethod::Post,
            url: format!("http://{address}/stall"),
            headers: Vec::new(),
            body: Vec::new(),
        };
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move {
            drive_stream(
                &reqwest::Client::new().into(),
                &PreparedHttpRequest::from(request),
                Box::new(CountingNormalizer(Arc::new(AtomicUsize::new(0)))),
                &tx,
            )
            .await
        });

        headers_sent_rx.await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::time::pause();

        let started = Instant::now();
        let outcome = tokio::time::timeout(STREAM_IDLE_TIMEOUT + Duration::from_secs(1), task)
            .await
            .expect("error body read did not time out within STREAM_IDLE_TIMEOUT bound")
            .unwrap();

        assert!(
            started.elapsed() <= STREAM_IDLE_TIMEOUT + Duration::from_millis(100),
            "expected completion within STREAM_IDLE_TIMEOUT bound, elapsed: {:?}",
            started.elapsed()
        );
        let error = outcome.unwrap_err();
        assert_eq!(error.error.status, Some(503));
        assert_eq!(error.error.class, ErrorClass::Transient);
        assert!(
            error.error.message.contains("<error body unreadable:"),
            "expected unreadable body note, got: {}",
            error.error.message
        );
        server.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn partial_record_drips_do_not_reset_the_idle_deadline() {
        let stream = futures::stream::unfold((), |_| async {
            tokio::time::sleep(Duration::from_secs(90)).await;
            Some((Ok::<_, std::io::Error>(vec![b'x']), ()))
        });
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let error = drive_body_stream(
            stream,
            Box::new(CountingNormalizer(Arc::new(AtomicUsize::new(0)))),
            &tx,
        )
        .await
        .unwrap_err();
        assert_eq!(error.error.class, ErrorClass::Transient);
        assert_eq!(
            error.error.message,
            "provider stream idle: waiting for a complete event for 300s"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn slow_complete_records_keep_the_stream_alive() {
        let stream = futures::stream::unfold(0usize, |index| async move {
            if index == 3 {
                None
            } else {
                tokio::time::sleep(STREAM_IDLE_TIMEOUT - Duration::from_secs(1)).await;
                Some((Ok::<_, std::io::Error>(vec![b'x']), index + 1))
            }
        });
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let completion = drive_body_stream(stream, Box::new(ProgressNormalizer::default()), &tx)
            .await
            .unwrap();
        assert_eq!(completion.len(), 2);
    }

    // KEEPALIVE LIVENESS. These run under paused tokio time, so they prove the deadline RULE
    // (which records reset which deadline, and which one fires first), not wall-clock
    // behaviour against a real socket. The stream shape is the one captured from xai/grok-4.7:
    // a few data records, then only `: keepalive` comments every 15 s while the model reasons
    // without streaming its reasoning, then content and `[DONE]`.

    const KEEPALIVE_EVERY: Duration = Duration::from_secs(15);

    fn chat_normalizer() -> Box<dyn StreamNormalizer> {
        OpenAiChat.normalizer(&openai_chat_frozen_config("grok-4.7"), StepId(1))
    }

    fn chat_role_chunk() -> Vec<u8> {
        br#"data: {"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}

"#
        .to_vec()
    }

    /// Each item is (virtual delay before it arrives, bytes).
    fn paced_stream(
        steps: Vec<(Duration, Vec<u8>)>,
    ) -> impl futures::Stream<Item = Result<Vec<u8>, std::io::Error>> {
        futures::stream::iter(steps).then(|(delay, bytes)| async move {
            tokio::time::sleep(delay).await;
            Ok(bytes)
        })
    }

    #[tokio::test(start_paused = true)]
    async fn keepalive_comments_carry_a_silently_reasoning_stream_past_the_liveness_deadline() {
        let mut steps = vec![(Duration::ZERO, chat_role_chunk())];
        // 600 s of keepalive-only silence: twice the 300 s liveness deadline, and above the
        // longest measured span (579 s).
        for _ in 0..(600 / KEEPALIVE_EVERY.as_secs()) {
            steps.push((KEEPALIVE_EVERY, b": keepalive\n\n".to_vec()));
        }
        steps.push((
            KEEPALIVE_EVERY,
            br#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}]}

"#
                .to_vec(),
        ));
        steps.push((Duration::ZERO, b"data: [DONE]\n\n".to_vec()));

        let started = Instant::now();
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let completion = drive_body_stream(paced_stream(steps), chat_normalizer(), &tx)
            .await
            .expect("a stream kept alive by keepalive comments must complete");
        assert!(started.elapsed() > Duration::from_secs(600));
        assert!(completion.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Finished {
                finish_reason: FinishReason::Stop,
                ..
            })
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn keepalive_only_stream_fails_at_the_content_deadline() {
        let head = futures::stream::iter([Ok::<_, std::io::Error>(chat_role_chunk())]);
        let keepalives = futures::stream::unfold((), |_| async {
            tokio::time::sleep(KEEPALIVE_EVERY).await;
            Some((Ok::<_, std::io::Error>(b": keepalive\n\n".to_vec()), ()))
        });
        let started = Instant::now();
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        // The outer bound only stops a broken rule from hanging the suite.
        let error = tokio::time::timeout(
            STREAM_CONTENT_TIMEOUT * 2,
            drive_body_stream(head.chain(keepalives), chat_normalizer(), &tx),
        )
        .await
        .expect("the content deadline must end a keepalive-only stream")
        .unwrap_err();
        assert_eq!(error.error.class, ErrorClass::Transient);
        assert_eq!(error.outcome, AttemptOutcome::ContentTimeout);
        assert_eq!(
            error.error.message,
            "provider stream idle: waiting for content: only keepalive comments for 1800s"
        );
        assert_eq!(started.elapsed(), STREAM_CONTENT_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn sse_bytes_that_never_complete_a_record_trip_the_liveness_deadline() {
        // A peer dripping the start of a comment and never terminating it: bytes arrive every
        // 90 s, but no record ever completes.
        let head = futures::stream::iter([Ok::<_, std::io::Error>(chat_role_chunk())]);
        let drip = futures::stream::unfold((), |_| async {
            tokio::time::sleep(Duration::from_secs(90)).await;
            Some((Ok::<_, std::io::Error>(b": keep".to_vec()), ()))
        });
        let started = Instant::now();
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let error = tokio::time::timeout(
            STREAM_CONTENT_TIMEOUT * 2,
            drive_body_stream(head.chain(drip), chat_normalizer(), &tx),
        )
        .await
        .expect("the liveness deadline must end a dripping stream")
        .unwrap_err();
        assert_eq!(error.error.class, ErrorClass::Transient);
        assert_eq!(error.outcome, AttemptOutcome::IdleTimeout);
        assert_eq!(
            error.error.message,
            "provider stream idle: waiting for any complete SSE record for 300s"
        );
        assert_eq!(started.elapsed(), STREAM_IDLE_TIMEOUT);
    }

    /// Which families report liveness: every SSE family does; Bedrock's binary event stream
    /// has no comment records and keeps the single idle deadline. An Anthropic `event: ping`
    /// arrives as a data record, so it stays content progress as it always was.
    #[test]
    fn sse_families_report_liveness_and_bedrock_keeps_the_single_deadline() {
        let step = StepId(1);
        let sse_families: Vec<(&str, Box<dyn StreamNormalizer>)> = vec![
            (
                "anthropic",
                AnthropicMessages.normalizer(
                    &frozen_config_for_family(WireFamilyId::AnthropicMessages, "m"),
                    step,
                ),
            ),
            ("openai_chat", chat_normalizer()),
            (
                "openai_responses",
                OpenAiResponses.normalizer(&openai_responses_frozen_config("m"), step),
            ),
            (
                "openai_responses_chatgpt",
                OpenAiResponsesChatgpt.normalizer(
                    &frozen_config_for_family(WireFamilyId::OpenAiResponsesChatgpt, "m"),
                    step,
                ),
            ),
            (
                "gemini",
                Gemini.normalizer(&gemini_frozen_config("m"), step),
            ),
            (
                "google_codeassist",
                GoogleCodeAssist.normalizer(
                    &frozen_config_for_family(WireFamilyId::GoogleCodeAssist, "m"),
                    step,
                ),
            ),
        ];
        for (name, mut normalizer) in sse_families {
            assert_eq!(normalizer.liveness_marker(), Some(0), "{name}");
            normalizer.push(b": keepalive\n\n").unwrap();
            assert_eq!(
                normalizer.liveness_marker(),
                Some(1),
                "{name}: comment is liveness"
            );
            assert_eq!(
                normalizer.progress_marker(),
                0,
                "{name}: comment is not content"
            );
        }

        let bedrock = BedrockConverse.normalizer(
            &frozen_config_for_family(WireFamilyId::BedrockConverse, "m"),
            step,
        );
        assert_eq!(bedrock.liveness_marker(), None);

        let mut anthropic = AnthropicMessages.normalizer(
            &frozen_config_for_family(WireFamilyId::AnthropicMessages, "m"),
            step,
        );
        anthropic
            .push(b"event: ping\ndata: {\"type\": \"ping\"}\n\n")
            .unwrap();
        assert_eq!(
            anthropic.progress_marker(),
            1,
            "an Anthropic ping is content progress"
        );
        assert_eq!(anthropic.liveness_marker(), Some(1));
    }

    #[tokio::test]
    async fn oversized_http_error_body_is_rejected_by_the_live_read_path() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0, "client disconnected before sending the request");
                request.extend_from_slice(&chunk[..read]);
            }

            let body = vec![b'x'; MAX_HTTP_ERROR_BODY_READ_BYTES + 1];
            let headers = format!(
                "HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            let _ = socket.write_all(&body).await;
        });

        let request = HttpRequest {
            method: HttpMethod::Post,
            url: format!("http://{address}/error"),
            headers: Vec::new(),
            body: Vec::new(),
        };
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let error = drive_stream(
            &reqwest::Client::builder().build().unwrap().into(),
            &PreparedHttpRequest::from(request),
            Box::new(CountingNormalizer(Arc::new(AtomicUsize::new(0)))),
            &tx,
        )
        .await
        .expect_err("an oversized live error response must be rejected");
        assert_eq!(error.error.class, ErrorClass::Transient);
        assert_eq!(error.error.status, Some(429));
        assert_eq!(error.cause, None);
        assert!(error.error.message.contains("error body exceeds"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn response_size_limits_return_transient_provider_errors() {
        let body = r#"{"error":{"code":"insufficient_quota"}}"#;
        let text = collect_error_body(
            futures::stream::iter([Ok::<_, std::io::Error>(body.as_bytes())]),
            body.len(),
        )
        .await
        .expect("body at the read limit is accepted");
        assert_eq!(http_error(429, &text, None).class, ErrorClass::Permanent);

        let error = collect_error_body(
            futures::stream::iter([Ok::<_, std::io::Error>(b"12345")]),
            4,
        )
        .await
        .expect_err("body above the read limit must be rejected");
        assert_eq!(error.error.class, ErrorClass::Transient);

        let stream = futures::stream::iter([
            Ok::<_, std::io::Error>(vec![0u8; 5]),
            Ok::<_, std::io::Error>(vec![0u8; 6]),
        ]);
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let error = drive_body_stream_with_limit(
            stream,
            Box::new(CountingNormalizer(Arc::new(AtomicUsize::new(0)))),
            &tx,
            10,
        )
        .await
        .expect_err("aggregate stream above the per-step limit must be rejected");
        assert_eq!(error.error.class, ErrorClass::Transient);
    }

    #[test]
    fn chatgpt_upstream_key_diagnostic_does_not_copy_body() {
        use super::{classify_chatgpt_upstream_key_rejection, CHATGPT_UPSTREAM_KEY_REJECTED};
        let marker = "PRIVATE_BODY_TAIL";
        let body = serde_json::json!({"error": {
            "message": format!("Incorrect API key provided: sk-private-key. {}{marker}", "x".repeat(600)),
            "code": "invalid_api_key"
        }}).to_string();
        assert!(body.find(marker).unwrap() > 512);
        let error = classify_chatgpt_upstream_key_rejection(
            WireFamilyId::OpenAiResponsesChatgpt,
            401,
            &body,
            classify_http_error(401, &body, None, None),
        );
        assert_eq!(error.class, ErrorClass::Transient);
        assert_eq!(error.status, Some(401));
        assert_eq!(
            error.provider_code.as_deref(),
            Some(CHATGPT_UPSTREAM_KEY_REJECTED)
        );
        assert!(!error.message.contains(marker));
        assert!(!error.message.contains("sk-"));
        assert!(error.message.contains("not a credential failure"));
    }

    /// The ChatGPT backend's own rejected API key, captured live, is transient and never an
    /// auth failure. The same body from the metered OpenAI API family is a real dead key and stays
    /// auth; a ChatGPT 401 that doesn't name an `sk-` key stays auth too.
    #[test]
    fn a_chatgpt_401_naming_an_api_key_is_upstream_not_our_credential() {
        use crate::spec::WireFamilyId;
        use crate::transport::{
            classify_chatgpt_upstream_key_rejection, classify_http_error,
            CHATGPT_UPSTREAM_KEY_REJECTED,
        };
        const CAPTURED: &str = r#"{
  "error": {
    "message": "Incorrect API key provided: sk-svcac***********fvMA. You can find your API key at https://platform.openai.com/account/api-keys.",
    "type": "invalid_request_error",
    "param": null,
    "code": "invalid_api_key"
  }
}"#;
        let classify = |family, body: &str| {
            classify_chatgpt_upstream_key_rejection(
                family,
                401,
                body,
                classify_http_error(401, body, None, None),
            )
        };

        let upstream = classify(WireFamilyId::OpenAiResponsesChatgpt, CAPTURED);
        assert_eq!(
            upstream.class,
            ErrorClass::Transient,
            "{}",
            upstream.message
        );
        assert_eq!(
            upstream.provider_code.as_deref(),
            Some(CHATGPT_UPSTREAM_KEY_REJECTED)
        );
        assert_eq!(upstream.status, Some(401));

        let metered = classify(WireFamilyId::OpenAiResponses, CAPTURED);
        assert_eq!(
            metered.class,
            ErrorClass::Auth,
            "an API-key family's dead key is ours"
        );

        let token = r#"{"error":{"message":"Your authentication token has expired.","type":"invalid_request_error","code":"token_expired"}}"#;
        let expired = classify(WireFamilyId::OpenAiResponsesChatgpt, token);
        assert_eq!(
            expired.class,
            ErrorClass::Auth,
            "a ChatGPT token failure is ours"
        );
    }

    #[test]
    fn error_classification_distinguishes_authentication_from_forbidden() {
        let cases = [
            ("HTTP 401", 401, "{}", ErrorClass::Auth),
            ("HTTP 403", 403, "{}", ErrorClass::Permanent),
            (
                "Anthropic permission_error",
                400,
                r#"{"error":{"type":"permission_error"}}"#,
                ErrorClass::Permanent,
            ),
            (
                "Anthropic authentication_error",
                400,
                r#"{"error":{"type":"authentication_error"}}"#,
                ErrorClass::Auth,
            ),
        ];

        for (label, status, body, expected) in cases {
            let actual = http_error(status, body, None).class;
            assert_eq!(
                actual, expected,
                "revert-check: {label} must classify as {expected:?}, got {actual:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn provider_stream_times_out_after_per_event_idle_limit() {
        let pushes = Arc::new(AtomicUsize::new(0));
        let stream = futures::stream::iter([Ok::<Vec<u8>, std::io::Error>(b"event".to_vec())])
            .chain(futures::stream::pending());
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let normalizer = Box::new(CountingNormalizer(pushes.clone()));
        let task = tokio::spawn(async move { drive_body_stream(stream, normalizer, &tx).await });

        tokio::task::yield_now().await;
        assert_eq!(pushes.load(Ordering::SeqCst), 1);
        tokio::time::advance(STREAM_IDLE_TIMEOUT + Duration::from_millis(1)).await;
        let error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("bounded outer timeout: idle guard did not finish")
            .expect("stream task did not panic")
            .expect_err("silent stream should time out");

        assert_eq!(error.error.class, ErrorClass::Transient);
        assert_eq!(
            error.error.message,
            "provider stream idle: waiting for a complete event for 300s"
        );
    }

    fn file(path: &str, size_bytes: u64, modified_ms: u64) -> DumpFileMeta {
        DumpFileMeta {
            path: PathBuf::from(path),
            size_bytes,
            modified_ms,
        }
    }

    fn path_strings(paths: Vec<PathBuf>) -> Vec<String> {
        paths
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect()
    }

    #[cfg(unix)]
    #[test]
    fn request_dump_directory_and_file_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = broca_test_support::scratch_root()
            .join(format!("broca-provider-secure-dump-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        create_dump_dir(&dir).expect("create secure dump directory");
        let path = dir.join("request.json");
        write_dump_file(&path, b"exact rendered body").expect("write secure dump file");

        let dir_mode = std::fs::metadata(&dir)
            .expect("dump directory metadata")
            .permissions()
            .mode()
            & 0o777;
        let file_mode = std::fs::metadata(&path)
            .expect("dump file metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700);
        assert_eq!(file_mode, 0o600);
        assert_eq!(std::fs::read(&path).unwrap(), b"exact rendered body");
        let _ = std::fs::remove_dir_all(&dir);
    }
    #[test]
    fn http_errors_use_structured_provider_codes_and_status_defaults() {
        let deep_quota_body = format!(
            r#"{{"error":{{"message":"{}","code":"insufficient_quota"}}}}"#,
            "x".repeat(1024)
        );
        let cases = [
            (
                429,
                deep_quota_body.as_str(),
                ErrorClass::Permanent,
                Some(BILLING_EXHAUSTED),
            ),
            (
                429,
                r#"{"error":{"message":"not in plan","code":"usage_not_included"}}"#,
                ErrorClass::Permanent,
                Some("usage_not_included"),
            ),
            (
                429,
                r#"{"error":{"message":"limited","code":"unrecognized_limit"}}"#,
                ErrorClass::Transient,
                Some("unrecognized_limit"),
            ),
            (
                529,
                r#"{"error":{"message":"busy","type":"overloaded_error"}}"#,
                ErrorClass::Transient,
                Some("overloaded_error"),
            ),
            (
                400,
                r#"{"error":{"message":"too long","code":"context_length_exceeded"}}"#,
                ErrorClass::ContextOverflow,
                Some("context_length_exceeded"),
            ),
            (503, "not json", ErrorClass::Transient, None),
        ];

        for (status, body, expected_class, expected_code) in cases {
            let error = http_error(status, body, None);
            assert_eq!(error.class, expected_class, "body: {body}");
            assert_eq!(
                error.provider_code.as_deref(),
                expected_code,
                "body: {body}"
            );
        }
    }

    #[test]
    fn retry_after_accepts_http_date_form() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let retry_at = now + Duration::from_secs(90);
        let value = httpdate::fmt_http_date(retry_at);
        assert_eq!(parse_retry_after_value(&value, now), Some(90.0));
    }

    #[test]
    fn sub_second_retry_after_ms_survives_instead_of_rounding_up_to_a_whole_second() {
        // Anthropic and OpenAI both send `retry-after-ms`, and both official SDKs prefer it,
        // because whole-second `Retry-After` cannot express a sub-second delay. The pair here
        // is deliberately CONTRADICTORY: a 300 ms hint alongside a 1-second one. Reading the
        // standard header would return 1.0 and look perfectly reasonable, so a test that sent
        // only the millisecond header could not tell the two readings apart.
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("retry-after-ms", "300".parse().unwrap());
        headers.insert(reqwest::header::RETRY_AFTER, "1".parse().unwrap());
        assert_eq!(parse_retry_after_header(&headers), Some(0.3));

        // Falls back when the millisecond form is absent, so adding it took nothing away.
        let mut only_standard = reqwest::header::HeaderMap::new();
        only_standard.insert(reqwest::header::RETRY_AFTER, "2".parse().unwrap());
        assert_eq!(parse_retry_after_header(&only_standard), Some(2.0));

        // A malformed millisecond value must not swallow a usable standard header.
        let mut malformed = reqwest::header::HeaderMap::new();
        malformed.insert("retry-after-ms", "soon".parse().unwrap());
        malformed.insert(reqwest::header::RETRY_AFTER, "5".parse().unwrap());
        assert_eq!(parse_retry_after_header(&malformed), Some(5.0));
    }

    #[test]
    fn quota_telemetry_headers_are_not_read_as_a_retry_delay() {
        // x-ratelimit-reset and anthropic-ratelimit-*-reset describe when a quota WINDOW
        // refills, not when this request may be retried. Reading one as a delay would park a
        // run until a window boundary unrelated to the refusal, so their presence alone must
        // yield no hint at all.
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-ratelimit-reset-requests", "60".parse().unwrap());
        headers.insert("x-ratelimit-remaining-requests", "0".parse().unwrap());
        headers.insert("anthropic-ratelimit-requests-reset", "120".parse().unwrap());
        assert_eq!(parse_retry_after_header(&headers), None);
    }

    #[test]
    fn persisted_http_error_body_is_bounded_after_full_body_classification() {
        let body = format!(
            r#"{{"error":{{"message":"{}","code":"insufficient_quota"}}}}"#,
            "x".repeat(10_000)
        );
        let error = http_error(429, &body, None);

        assert_eq!(error.class, ErrorClass::Permanent);
        assert!(error.message.len() <= "429: ".len() + MAX_ERROR_BODY_BYTES);
        assert!(!error.message.contains("insufficient_quota"));
    }

    #[test]
    fn request_dump_filename_uses_step_context_identity() {
        let filename = request_dump_filename(
            Some(&StepContext {
                run_id: "run-abc".into(),
                step_index: 7,
            }),
            "ignored",
            1,
        );
        assert_eq!(filename, "run-abc-s7.json");
    }

    #[test]
    fn request_dump_filename_falls_back_and_sanitizes() {
        let filename = request_dump_filename(None, "run/with spaces", 3);
        assert_eq!(filename, "run_with_spaces-s3.json");
    }

    #[test]
    fn dump_sweep_preserves_foreign_files_under_age_and_size_pressure() {
        use super::{create_dump_dir, sweep_dump_dir};
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-provider-mixed-dumps-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        create_dump_dir(&dir).unwrap();
        let foreign = [
            "notes.txt",
            "other.json",
            "run-s01.json",
            "run-s+1.json",
            "run-s18446744073709551616.json",
        ];
        for caps in [
            DumpRetentionCaps {
                max_bytes: u64::MAX,
                max_age_days: 0,
            },
            DumpRetentionCaps {
                max_bytes: 0,
                max_age_days: u64::MAX,
            },
        ] {
            for name in foreign.into_iter().chain(["run-abc-s7.json"]) {
                let path = dir.join(name);
                write_dump_file(&path, b"keep these exact bytes").unwrap();
                std::fs::File::options()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_times(
                        std::fs::FileTimes::new()
                            .set_modified(std::time::UNIX_EPOCH + Duration::from_secs(1)),
                    )
                    .unwrap();
            }
            sweep_dump_dir(&dir, caps).unwrap();
            assert!(
                !dir.join("run-abc-s7.json").exists(),
                "valid dump must be pruned"
            );
            for name in foreign {
                assert_eq!(
                    std::fs::read(dir.join(name)).unwrap(),
                    b"keep these exact bytes",
                    "{name}"
                );
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn dump_retention_deletes_age_expired_files_before_size_pruning() {
        let now_ms = 10 * DAY_MS;
        let deletions = dump_files_to_delete(
            vec![
                file("oldest.json", 50, now_ms - 9 * DAY_MS),
                file("older.json", 100, now_ms - 8 * DAY_MS),
                file("recent.json", 400, now_ms - 2 * DAY_MS),
            ],
            now_ms,
            DumpRetentionCaps {
                max_bytes: 512,
                max_age_days: 7,
            },
        );
        assert_eq!(
            path_strings(deletions),
            vec!["oldest.json", "older.json"],
            "files older than the retention window should be pruned first"
        );
    }

    #[test]
    fn dump_retention_prunes_oldest_remaining_files_until_total_size_fits() {
        let now_ms = 10 * DAY_MS;
        let deletions = dump_files_to_delete(
            vec![
                file("a.json", 200, now_ms - 3 * DAY_MS),
                file("b.json", 200, now_ms - 2 * DAY_MS),
                file("c.json", 200, now_ms - DAY_MS),
            ],
            now_ms,
            DumpRetentionCaps {
                max_bytes: 350,
                max_age_days: 7,
            },
        );
        assert_eq!(
            path_strings(deletions),
            vec!["a.json", "b.json"],
            "size pruning should delete the oldest remaining dumps first"
        );
    }

    #[test]
    fn dump_retention_keeps_files_at_the_exact_age_boundary() {
        let now_ms = 10 * DAY_MS;
        let deletions = dump_files_to_delete(
            vec![file("boundary.json", 1, now_ms - 7 * DAY_MS)],
            now_ms,
            DumpRetentionCaps {
                max_bytes: 512,
                max_age_days: 7,
            },
        );
        assert!(
            deletions.is_empty(),
            "a file exactly seven days old still satisfies the age <= 7 days contract"
        );
    }
}

#[cfg(test)]
mod stream_floor_tests {
    use super::{StreamFloor, StreamRateWindow};
    use broca_protocol::{display::DisplayEvent, MessageId, NormalizedEvent};
    use std::time::Duration;

    fn floor() -> StreamFloor {
        StreamFloor {
            bytes_per_sec: 8.0,
            window: Duration::from_secs(60),
        }
    }

    fn text(bytes: usize) -> NormalizedEvent {
        NormalizedEvent::Display(DisplayEvent::TextDelta {
            message_id: MessageId("m1".to_string()),
            tool_call_id: None,
            delta: "x".repeat(bytes),
        })
    }

    /// The trap this design exists to avoid: a model that reasons for a long time before emitting
    /// anything is SILENT, not slow, and silence belongs to the idle timeout. If the window opened
    /// at request time instead of at the first content byte, this is the case that would abort a
    /// perfectly healthy run -- so the assertion is that a long quiet stretch produces no reading
    /// at all, not merely a passing one.
    #[tokio::test(start_paused = true)]
    async fn silence_never_opens_the_window() {
        let mut window = StreamRateWindow::new(floor());
        for _ in 0..10 {
            window.observe(&[NormalizedEvent::Display(DisplayEvent::DisplayGap {
                dropped: 1,
            })]);
            tokio::time::advance(Duration::from_secs(60)).await;
        }
        assert_eq!(
            window.breach(),
            None,
            "ten minutes of non-content events must leave the window unopened"
        );
    }

    /// The signature the floor is FOR: content arriving steadily, well under the floor, for longer
    /// than a full window. ~1 byte/sec against a floor of 8.
    #[tokio::test(start_paused = true)]
    async fn a_steady_trickle_breaches_after_a_full_window() {
        let mut window = StreamRateWindow::new(floor());
        for _ in 0..61 {
            window.observe(&[text(1)]);
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        let rate = window
            .breach()
            .expect("a 1 byte/sec stream is under an 8 byte/sec floor");
        assert!(rate < 8.0, "reported rate {rate} should be below the floor");
    }

    /// A healthy stream must not trip, and the control matters: without it the trickle test above
    /// would pass against an implementation that fires on any stream at all.
    #[tokio::test(start_paused = true)]
    async fn a_healthy_stream_never_breaches() {
        let mut window = StreamRateWindow::new(floor());
        for _ in 0..61 {
            window.observe(&[text(400)]);
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        assert_eq!(
            window.breach(),
            None,
            "400 bytes/sec is far above the floor"
        );
    }

    /// A slow start must not fire before a full window has elapsed, or a stream that begins slowly
    /// and recovers would be killed on its first seconds.
    #[tokio::test(start_paused = true)]
    async fn a_partial_window_never_breaches() {
        let mut window = StreamRateWindow::new(floor());
        window.observe(&[text(1)]);
        tokio::time::advance(Duration::from_secs(59)).await;
        assert_eq!(
            window.breach(),
            None,
            "59s of a 60s window is not yet a measurement"
        );
    }
}

/// A tool call whose arguments the output-token ceiling cut off, driven through the live
/// transport for every wire family that streams tool arguments as a string. Each fixture is
/// the smallest stream carrying the family's own wire shape for "arguments started, output
/// stopped at the cap". The ceiling case must fail once, permanent, naming the ceiling; the
/// controls carry the same truncated arguments without a ceiling stop reason and must keep
/// the ordinary transient parse failure, including its single retry.
#[cfg(test)]
mod output_ceiling_tests {
    use std::sync::{Arc, Mutex};

    use broca_protocol::{
        AttemptOutcome, CallOptions, ErrorClass, NormalizedEvent, ProviderControl, ProviderError,
        StepId,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_util::sync::CancellationToken;

    use super::LiveProvider;
    use crate::{
        families::{
            anthropic_frozen_config, bedrock_converse::test_event_stream_message,
            bedrock_frozen_config, openai_chat_frozen_config,
            openai_responses_chatgpt_frozen_config, openai_responses_frozen_config,
            AnthropicMessages, BedrockConverse, OpenAiChat, OpenAiResponses,
            OpenAiResponsesChatgpt,
        },
        wire::{AuthHandle, FrozenRenderConfig, WireFamily},
    };

    /// The cap the requests are rendered with, which the ceiling error must name.
    const CAP: u32 = 4096;
    /// A tool-argument buffer cut mid-object, as a ceiling leaves it.
    const CUT_ARGS: &str = r#"{"path":"a.txt","content":"hel"#;
    const TOOL: &str = "write_file";

    /// JSON-escape the cut arguments so they can sit inside a JSON string field.
    fn args_json() -> String {
        serde_json::to_string(CUT_ARGS).unwrap()
    }

    struct Outcome {
        events: Vec<NormalizedEvent>,
        requests: usize,
    }

    impl Outcome {
        fn error(&self) -> &ProviderError {
            self.events
                .iter()
                .find_map(|event| match event {
                    NormalizedEvent::Control(ProviderControl::Error { error, .. }) => Some(error),
                    _ => None,
                })
                .expect("the step must end in a provider error")
        }

        fn attempt_outcomes(&self) -> Vec<AttemptOutcome> {
            self.events
                .iter()
                .filter_map(|event| match event {
                    NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                        Some(attempt.outcome.clone())
                    }
                    _ => None,
                })
                .collect()
        }
    }

    /// Serve `body` as a 200 to every request (so a retry, if the transport makes one, is
    /// observed rather than failing differently) and drive one step through the live transport.
    async fn run_step<F: WireFamily + 'static>(
        family: F,
        mut frozen: FrozenRenderConfig,
        content_type: &'static str,
        body: Vec<u8>,
    ) -> Outcome {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(0usize));
        let seen = requests.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                *seen.lock().unwrap() += 1;
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: {content_type}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(head.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
        });
        frozen.base_url = Some(format!("http://{address}"));
        frozen.max_output_tokens = CAP;
        let provider = LiveProvider::new(family, frozen, AuthHandle::None);
        // The ceiling fixture is an HTTP body, not a socket upgrade; its assertions stay unchanged.
        broca_core::Provider::set_chatgpt_https_policy(&provider, Arc::new(|| true));
        let mut rx = provider.start_step(
            StepId(1),
            Arc::new(CallOptions::new(Vec::new())),
            CancellationToken::new(),
            None,
        );
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        server.abort();
        let requests = *requests.lock().unwrap();
        Outcome { events, requests }
    }

    async fn read_request(socket: &mut tokio::net::TcpStream) {
        let mut request = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(
                read, 0,
                "client disconnected before request headers completed"
            );
            request.extend_from_slice(&chunk[..read]);
            if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
        let content_length = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .map(|value| value.trim().parse::<usize>().unwrap())
            .unwrap_or(0);
        while request.len() < header_end + content_length {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client disconnected before request body completed");
            request.extend_from_slice(&chunk[..read]);
        }
    }

    /// The ceiling case: one attempt, a permanent class, and a message that names the
    /// ceiling, the cap, the tool, and how much of its arguments arrived.
    fn assert_ceiling_cut(outcome: &Outcome, cap: &str, decode_outcome: AttemptOutcome) {
        let error = outcome.error();
        assert_eq!(
            error.class,
            ErrorClass::Permanent,
            "a proven ceiling cut must not be retried: {}",
            error.message
        );
        let expected_lead = format!(
            "output ceiling reached mid tool call: finish=length, max_output_tokens={cap} (tool {TOOL}, {} argument bytes received",
            CUT_ARGS.len()
        );
        assert!(
            error.message.starts_with(&expected_lead),
            "the error must name the ceiling, got: {}",
            error.message
        );
        assert!(
            !error.message.contains("EOF while parsing"),
            "the ceiling error must not read as a parse failure: {}",
            error.message
        );
        assert_eq!(
            outcome.requests, 1,
            "exactly one attempt reaches the provider"
        );
        assert_eq!(outcome.attempt_outcomes(), vec![decode_outcome]);
    }

    /// The control: today's parse failure, its transient class, and its single decode retry.
    fn assert_parse_failure(outcome: &Outcome, decode_outcome: AttemptOutcome) {
        let error = outcome.error();
        assert_eq!(error.class, ErrorClass::Transient, "{}", error.message);
        assert!(
            error.message.starts_with(
                "normalize response: normalize: invalid terminal tool arguments: EOF while parsing"
            ),
            "without a ceiling stop reason the parse failure stands, got: {}",
            error.message
        );
        assert!(!error.message.contains("output ceiling"));
        assert_eq!(
            outcome.requests, 2,
            "the unexplained parse failure keeps its one deterministic-decode retry"
        );
        assert_eq!(
            outcome.attempt_outcomes(),
            vec![decode_outcome.clone(), decode_outcome]
        );
    }

    // ───────────── anthropic_messages ─────────────

    fn anthropic_body(message_delta: Option<&str>) -> Vec<u8> {
        let mut body = String::new();
        let mut record = |data: String| body.push_str(&format!("data: {data}\n\n"));
        record(r#"{"type":"message_start","message":{"id":"msg_cut","usage":{"input_tokens":10,"output_tokens":1}}}"#.into());
        record(format!(
            r#"{{"type":"content_block_start","index":0,"content_block":{{"type":"tool_use","id":"toolu_1","name":"{TOOL}","input":{{}}}}}}"#
        ));
        record(format!(
            r#"{{"type":"content_block_delta","index":0,"delta":{{"type":"input_json_delta","partial_json":{}}}}}"#,
            args_json()
        ));
        record(r#"{"type":"content_block_stop","index":0}"#.into());
        if let Some(stop_reason) = message_delta {
            record(format!(
                r#"{{"type":"message_delta","delta":{{"stop_reason":"{stop_reason}"}},"usage":{{"output_tokens":4096}}}}"#
            ));
        }
        record(r#"{"type":"message_stop"}"#.into());
        body.into_bytes()
    }

    #[tokio::test]
    async fn anthropic_ceiling_cut_is_permanent_and_named() {
        let outcome = run_step(
            AnthropicMessages,
            anthropic_frozen_config("claude-test"),
            "text/event-stream",
            anthropic_body(Some("max_tokens")),
        )
        .await;
        assert_ceiling_cut(&outcome, "4096", AttemptOutcome::DecodeError);
        assert!(outcome
            .error()
            .message
            .contains("provider stop reason max_tokens"));
        assert!(outcome.error().message.contains("output_tokens=4096"));
    }

    #[tokio::test]
    async fn anthropic_cut_without_ceiling_stop_keeps_the_parse_failure() {
        for stop in [None, Some("tool_use"), Some("end_turn")] {
            let outcome = run_step(
                AnthropicMessages,
                anthropic_frozen_config("claude-test"),
                "text/event-stream",
                anthropic_body(stop),
            )
            .await;
            assert_parse_failure(&outcome, AttemptOutcome::DecodeError);
        }
    }

    // ───────────── openai_chat ─────────────

    fn openai_chat_body(finish_reason: Option<&str>) -> Vec<u8> {
        let mut body = String::new();
        let mut record = |data: String| body.push_str(&format!("data: {data}\n\n"));
        record(format!(
            r#"{{"id":"c1","choices":[{{"index":0,"delta":{{"role":"assistant","tool_calls":[{{"index":0,"id":"call_1","type":"function","function":{{"name":"{TOOL}","arguments":""}}}}]}},"finish_reason":null}}]}}"#
        ));
        record(format!(
            r#"{{"id":"c1","choices":[{{"index":0,"delta":{{"tool_calls":[{{"index":0,"function":{{"arguments":{}}}}}]}},"finish_reason":null}}]}}"#,
            args_json()
        ));
        if let Some(finish_reason) = finish_reason {
            record(format!(
                r#"{{"id":"c1","choices":[{{"index":0,"delta":{{}},"finish_reason":"{finish_reason}"}}]}}"#
            ));
        }
        record("[DONE]".into());
        body.into_bytes()
    }

    // OpenAI Chat assembles at EOF in `finish()`, so its decode failures record as
    // `eof_before_completion` today; the ceiling cut keeps that attempt outcome.
    #[tokio::test]
    async fn openai_chat_ceiling_cut_is_permanent_and_named() {
        let outcome = run_step(
            OpenAiChat,
            openai_chat_frozen_config("gpt-test"),
            "text/event-stream",
            openai_chat_body(Some("length")),
        )
        .await;
        assert_ceiling_cut(&outcome, "4096", AttemptOutcome::EofBeforeCompletion);
        assert!(outcome
            .error()
            .message
            .contains("provider stop reason length"));
    }

    #[tokio::test]
    async fn openai_chat_cut_without_ceiling_stop_keeps_the_parse_failure() {
        for finish in [None, Some("tool_calls"), Some("stop")] {
            let outcome = run_step(
                OpenAiChat,
                openai_chat_frozen_config("gpt-test"),
                "text/event-stream",
                openai_chat_body(finish),
            )
            .await;
            assert_parse_failure(&outcome, AttemptOutcome::EofBeforeCompletion);
        }
    }

    // ───────────── openai_responses and openai_responses_chatgpt ─────────────

    /// `terminal` is the terminal response object's status and incomplete_details fields.
    fn responses_body(terminal: &str) -> Vec<u8> {
        let mut body = String::new();
        let mut record = |data: String| body.push_str(&format!("data: {data}\n\n"));
        record(r#"{"type":"response.created","response":{"id":"resp_1"}}"#.into());
        record(format!(
            r#"{{"type":"response.output_item.added","output_index":0,"item":{{"type":"function_call","call_id":"call_1","name":"{TOOL}","arguments":""}}}}"#
        ));
        record(format!(
            r#"{{"type":"response.function_call_arguments.delta","output_index":0,"delta":{}}}"#,
            args_json()
        ));
        let kind = if terminal.contains("\"completed\"") {
            "response.completed"
        } else {
            "response.incomplete"
        };
        record(format!(
            r#"{{"type":"{kind}","response":{{"id":"resp_1",{terminal},"usage":{{"input_tokens":10,"output_tokens":4096}}}}}}"#
        ));
        body.into_bytes()
    }

    const RESPONSES_CEILING: &str =
        r#""status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}"#;
    /// Controls: a bare `incomplete` status does not say why, a content filter is a different
    /// reason, and `completed` is no stop at the ceiling at all.
    const RESPONSES_CONTROLS: [&str; 3] = [
        r#""status":"incomplete""#,
        r#""status":"incomplete","incomplete_details":{"reason":"content_filter"}"#,
        r#""status":"completed""#,
    ];

    #[tokio::test]
    async fn openai_responses_ceiling_cut_is_permanent_and_named() {
        let outcome = run_step(
            OpenAiResponses,
            openai_responses_frozen_config("gpt-test"),
            "text/event-stream",
            responses_body(RESPONSES_CEILING),
        )
        .await;
        assert_ceiling_cut(&outcome, "4096", AttemptOutcome::DecodeError);
        assert!(outcome
            .error()
            .message
            .contains("provider stop reason max_output_tokens"));
    }

    #[tokio::test]
    async fn openai_responses_cut_without_ceiling_stop_keeps_the_parse_failure() {
        for terminal in RESPONSES_CONTROLS {
            let outcome = run_step(
                OpenAiResponses,
                openai_responses_frozen_config("gpt-test"),
                "text/event-stream",
                responses_body(terminal),
            )
            .await;
            assert_parse_failure(&outcome, AttemptOutcome::DecodeError);
        }
    }

    // The codex backend rejects `max_output_tokens` in the body, so no cap is sent and the
    // error says the cap is unknown rather than naming one the provider never saw.
    #[tokio::test]
    async fn openai_responses_chatgpt_ceiling_cut_is_permanent_and_named() {
        let outcome = run_step(
            OpenAiResponsesChatgpt,
            openai_responses_chatgpt_frozen_config("gpt-test"),
            "text/event-stream",
            responses_body(RESPONSES_CEILING),
        )
        .await;
        assert_ceiling_cut(&outcome, "unknown", AttemptOutcome::DecodeError);
        assert!(outcome.error().message.contains("output_tokens=4096"));
    }

    #[tokio::test]
    async fn openai_responses_chatgpt_cut_without_ceiling_stop_keeps_the_parse_failure() {
        for terminal in RESPONSES_CONTROLS {
            let outcome = run_step(
                OpenAiResponsesChatgpt,
                openai_responses_chatgpt_frozen_config("gpt-test"),
                "text/event-stream",
                responses_body(terminal),
            )
            .await;
            assert_parse_failure(&outcome, AttemptOutcome::DecodeError);
        }
    }

    // ───────────── bedrock_converse ─────────────

    fn bedrock_body(message_stop: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend(test_event_stream_message(
            "messageStart",
            br#"{"role":"assistant"}"#,
        ));
        body.extend(test_event_stream_message(
            "contentBlockStart",
            format!(
                r#"{{"contentBlockIndex":0,"start":{{"toolUse":{{"toolUseId":"tooluse_1","name":"{TOOL}"}}}}}}"#
            )
            .as_bytes(),
        ));
        body.extend(test_event_stream_message(
            "contentBlockDelta",
            format!(
                r#"{{"contentBlockIndex":0,"delta":{{"toolUse":{{"input":{}}}}}}}"#,
                args_json()
            )
            .as_bytes(),
        ));
        body.extend(test_event_stream_message(
            "contentBlockStop",
            br#"{"contentBlockIndex":0}"#,
        ));
        body.extend(test_event_stream_message(
            "messageStop",
            message_stop.as_bytes(),
        ));
        body.extend(test_event_stream_message(
            "metadata",
            br#"{"usage":{"inputTokens":10,"outputTokens":4096}}"#,
        ));
        body
    }

    // The Bedrock normalizer does not see the per-step generation cap the body may render,
    // so it names the cap as unknown; the stop reason and reported output count prove the cut.
    #[tokio::test]
    async fn bedrock_ceiling_cut_is_permanent_and_named() {
        let outcome = run_step(
            BedrockConverse,
            bedrock_frozen_config("anthropic.claude-test"),
            "application/vnd.amazon.eventstream",
            bedrock_body(r#"{"stopReason":"max_tokens"}"#),
        )
        .await;
        assert_ceiling_cut(&outcome, "unknown", AttemptOutcome::DecodeError);
        assert!(outcome
            .error()
            .message
            .contains("provider stop reason max_tokens"));
        assert!(outcome.error().message.contains("output_tokens=4096"));
    }

    #[tokio::test]
    async fn bedrock_cut_without_ceiling_stop_keeps_the_parse_failure() {
        for message_stop in [
            "{}",
            r#"{"stopReason":"tool_use"}"#,
            r#"{"stopReason":"end_turn"}"#,
        ] {
            let outcome = run_step(
                BedrockConverse,
                bedrock_frozen_config("anthropic.claude-test"),
                "application/vnd.amazon.eventstream",
                bedrock_body(message_stop),
            )
            .await;
            assert_parse_failure(&outcome, AttemptOutcome::DecodeError);
        }
    }
}

/// Render only event discriminators and their bounded counts, never wire payloads.
fn format_event_type_counts(counts: Option<&[(String, u64)]>) -> String {
    let Some(counts) = counts.filter(|counts| !counts.is_empty()) else {
        return "none".to_owned();
    };
    let mut sorted: Vec<_> = counts.iter().collect();
    sorted.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    sorted
        .into_iter()
        .map(|(event_type, count)| format!("{event_type}={count}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Keep every transport cause: the outer HTTP error alone hides resets and protocol failures.
fn transport_error_chain(error: &dyn std::error::Error) -> String {
    let mut formatted = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        formatted.push_str(" -> ");
        formatted.push_str(&cause.to_string());
        source = cause.source();
    }
    formatted
}

fn warn_private_failure(error: &ProviderError, message: &'static str) {
    tracing::warn!(
        target: "provider",
        class = ?error.class,
        status = error.status,
        provider_code = error.provider_code.as_deref().filter(|code| crate::safe_provider_code(code)).unwrap_or("none"),
        message,
    );
}

#[cfg(test)]
#[path = "transport_chunk_error_tests.rs"]
mod chunk_error_tests;

#[cfg(test)]
#[path = "transport_tool_args_tests.rs"]
mod tool_args_tests;

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{assert_protected_private as assert_private, BroadParent};
    #[test]
    fn windows_request_dump_directory_and_file_are_private() {
        let fixture = BroadParent::new("dump");
        let dir = fixture.path.join("request-dumps");
        create_dump_dir(&dir).unwrap();
        let path = dir.join("request.json");
        write_dump_file(&path, b"request body").unwrap();
        assert_private(&dir);
        assert_private(&path);
        assert_eq!(std::fs::read(&path).unwrap(), b"request body");
    }
}
