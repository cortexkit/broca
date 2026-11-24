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
        ErrorClass, GoogleRpcErrorDetails, GoogleRpcErrorInfo, GoogleRpcQuotaFailure,
        GoogleRpcQuotaViolation, GoogleRpcRetryInfo, ProviderError, ACCOUNT_LIMIT_EXHAUSTED,
        BILLING_EXHAUSTED, CREDIT_INSUFFICIENT,
    },
    AttemptOutcome, CallOptions, CredentialRefresh, GenerationConfig, ModelAttempt,
    NormalizedEvent, ProviderControl, StepId, ToolChoice,
};
use broca_records::record::{AttemptEnvelope, AttemptId, BillingAttemptOutcome};
use bytes::Bytes;
use futures::StreamExt;
use rand::Rng;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    attempt_policy::{
        self, AttemptDecision, AttemptFacts, AttemptInputs, AttemptState, EndReason,
        RejectedRefreshResult, BACKOFF_JITTER, MAX_ATTEMPTS,
    },
    credential_hosts::CredentialHosts,
    credential_refresh::CredentialRefresher,
    families::fresh_admission_config_error,
    normalize::classify_error_with_status_defaults,
    wire::{
        AuthHandle, FrozenRenderConfig, HttpRequest, NormalizeError, StreamNormalizer, WireFamily,
    },
};

#[cfg(test)]
use crate::attempt_policy::{MAX_RETRY_AFTER_DELAY, RETRY_WALL_BUDGET};

#[cfg(test)]
#[path = "transport/websocket_tests.rs"]
mod websocket_tests;

#[cfg(test)]
#[path = "transport/attempt_test_hooks.rs"]
mod attempt_test_hooks;

#[cfg(test)]
#[path = "transport/attempt_effect_hooks.rs"]
mod attempt_effect_hooks;

#[cfg(test)]
#[path = "transport/attempt_trace_cases.rs"]
mod attempt_trace_cases;

macro_rules! attempt_deadline_now {
    () => {{
        #[cfg(test)]
        {
            attempt_test_hooks::deadline_now()
        }
        #[cfg(not(test))]
        {
            Instant::now()
        }
    }};
}

#[cfg(test)]
#[path = "transport_retention_tests.rs"]
mod retention_tests;

#[cfg(test)]
#[path = "transport_refusal_fallback_tests.rs"]
mod refusal_fallback_tests;

#[path = "transport_websocket.rs"]
mod responses_websocket;

#[path = "transport_cache_evidence.rs"]
mod cache_runtime;

#[path = "transport/content_deadline.rs"]
mod content_deadline;
mod tool_name_restore;
use tool_name_restore::ToolNameRestoringNormalizer;
#[path = "transport/completion.rs"]
mod completion;
#[path = "transport/content_gaps.rs"]
mod content_gaps;
#[path = "transport/retry_wait.rs"]
mod retry_wait;
use completion::{
    is_step_completion_event, step_closed, tool_args_cut, validate_attempt_completion,
};
use content_deadline::content_timeout_error;
pub(crate) use content_deadline::STREAM_CONTENT_TIMEOUT;
use content_gaps::{ContentGaps, SseObservationGuard};

pub use responses_websocket::Connections as ChatgptConnections;

#[path = "transport_codex_retry.rs"]
pub(crate) mod codex_retry;

#[path = "transport_request_gzip.rs"]
pub mod request_gzip;

#[cfg(test)]
#[path = "transport_request_gzip_tests.rs"]
mod request_gzip_tests;

/// Shareable HTTP transport handle. Clones retain the same reqwest connection pools.
///
/// Reqwest decides response decoding per client, and a
/// client that decodes also adds an `accept-encoding` header to every request it sends.
/// Anthropic's first-party API gets Claude Code's `accept-encoding` and a client that decodes
/// what comes back; every other host gets a client without decoding, so its requests go out
/// exactly as they did before decoding existed. Journaled model requests use separate pools
/// that never retry, so those pools never send a request a second time on their own. No pool
/// follows a redirect (see `provider_client_builder`).
#[derive(Clone, Debug)]
pub struct ProviderHttpClient {
    // A run-local clone also sends prompt-cache warm requests reconstructed from a saved
    // render config; those requests do not pass through the ordinary attempt loop.
    warm_quota_observer: Option<Arc<dyn crate::quota::QuotaObserver>>,
    /// Every host except `api.anthropic.com`. No response decoding and no `accept-encoding`.
    identity: reqwest::Client,
    /// `api.anthropic.com` only. Decodes gzip, deflate, brotli and zstd responses as they stream.
    decoding: reqwest::Client,
    journaled: Arc<JournaledHttpClients>,
    request_gzip: request_gzip::RequestGzipPolicy,
    chatgpt_connections: ChatgptConnections,
    cache_sessions: cache_runtime::Sessions,
    #[cfg(feature = "test-harness")]
    warm_reply: Option<Bytes>,
    /// Tests only: the number of upcoming sends to refuse immediately, without
    /// opening a socket. A refused localhost connect takes seconds on some platforms, so a
    /// test that needs many connect failures in a bounded window cannot dial a real closed
    /// port; this keeps the failure and its `ConnectFailed` outcome while removing the
    /// operating system from the timing.
    #[cfg(test)]
    fail_connects: Arc<AtomicUsize>,
}

struct JournaledHttpClients {
    clients: OnceLock<(reqwest::Client, reqwest::Client)>,
    builder: Box<dyn Fn() -> reqwest::ClientBuilder + Send + Sync>,
}

impl std::fmt::Debug for JournaledHttpClients {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JournaledHttpClients")
            .field("initialized", &self.clients.get().is_some())
            .finish_non_exhaustive()
    }
}

fn build_http_pool(builder: reqwest::ClientBuilder, decode: bool) -> reqwest::Client {
    builder
        .gzip(decode)
        .deflate(decode)
        .brotli(decode)
        .zstd(decode)
        .build()
        .expect("the provider HTTP client configuration is valid")
}

impl ProviderHttpClient {
    pub fn session_accepted_send_evidence(
        &self,
        session: &broca_protocol::BindIdentity,
        account: &str,
    ) -> broca_core::provider::AcceptedSendEvidence {
        self.cache_sessions.evidence(session, account)
    }

    /// Seed one account before routing admission, without acquiring its WebSocket lease.
    pub fn seed_session_accepted_send(
        &self,
        session: &broca_protocol::BindIdentity,
        account: &str,
        evidence: broca_core::provider::AcceptedSendEvidence,
    ) {
        self.cache_sessions
            .bind(session)
            .seed_accepted_send(account, evidence);
    }

    /// Read accepted-attempt evidence without loading a session or reading its log.
    /// `None` means not loaded in this process; `Some(None)` means loaded with no accepted
    /// send on this account. This distinction lets admission count conservative cold starts.
    pub fn session_last_accepted_at_ms(
        &self,
        session: &broca_protocol::BindIdentity,
        account: &str,
    ) -> Option<Option<u64>> {
        self.cache_sessions.accepted_at_ms(session, account)
    }

    fn build(
        builder: impl Fn() -> reqwest::ClientBuilder + Send + Sync + 'static,
        request_gzip: request_gzip::RequestGzipPolicy,
    ) -> Self {
        Self {
            identity: build_http_pool(builder(), false),
            warm_quota_observer: None,
            decoding: build_http_pool(builder(), true),
            journaled: Arc::new(JournaledHttpClients {
                clients: OnceLock::new(),
                builder: Box::new(builder),
            }),
            request_gzip,
            chatgpt_connections: ChatgptConnections::default(),
            cache_sessions: cache_runtime::Sessions::default(),
            #[cfg(feature = "test-harness")]
            warm_reply: None,
            #[cfg(test)]
            fail_connects: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The client for a request: the decoding one only for Anthropic's first-party host.
    pub fn for_host(&self, anthropic_api_host: bool) -> &reqwest::Client {
        if anthropic_api_host {
            &self.decoding
        } else {
            &self.identity
        }
    }

    fn for_model_host(&self, anthropic_api_host: bool, journaled: bool) -> &reqwest::Client {
        if !journaled {
            return self.for_host(anthropic_api_host);
        }
        // Sends without an attempt journal never initialize the no-retry pools, even when using
        // the HTTP/1.1 fallback client after a transient transport failure. Every pool already refuses
        // redirects (see `provider_client_builder`).
        let (identity, decoding) = self.journaled.clients.get_or_init(|| {
            (
                build_http_pool(
                    (self.journaled.builder)().retry(reqwest::retry::never()),
                    false,
                ),
                build_http_pool(
                    (self.journaled.builder)().retry(reqwest::retry::never()),
                    true,
                ),
            )
        });
        if anthropic_api_host {
            decoding
        } else {
            identity
        }
    }

    /// The one-attempt HTTP/1.1 pool-escape client, with this client's request-gzip policy.
    fn http1_escape(&self) -> Self {
        let escaped = Self::build(|| provider_client_builder().http1_only(), self.request_gzip);
        escaped.with_test_hooks_from(self)
    }

    /// Tests only: carry the test-only send hooks across the escape-client rebuild, so the
    /// step's one escape attempt fails the same way the primary pool does.
    #[cfg(test)]
    fn with_test_hooks_from(mut self, source: &Self) -> Self {
        self.fail_connects = source.fail_connects.clone();
        self
    }

    /// Production builds have no test-only send hooks, so the rebuilt client is already whole.
    #[cfg(not(test))]
    fn with_test_hooks_from(self, _source: &Self) -> Self {
        self
    }

    /// The client that never decodes responses, for diagnostic tools that record a response
    /// exactly as it came off the connection.
    pub fn without_response_decoding(&self) -> &reqwest::Client {
        &self.identity
    }

    /// The executor-owned, full-session-bind socket owner; never serialized or global.
    pub fn chatgpt_connections(&self) -> &ChatgptConnections {
        &self.chatgpt_connections
    }

    /// A client that sends `api.anthropic.com` to a local test server, with its own gzip latch.
    #[cfg(test)]
    fn resolving_anthropic_api_to(address: std::net::SocketAddr, gzip_enabled: bool) -> Self {
        Self::build(
            move || provider_client_builder().resolve(request_gzip::ANTHROPIC_API_HOST, address),
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
            warm_quota_observer: None,
            decoding: client,
            journaled: Arc::new(JournaledHttpClients {
                clients: OnceLock::new(),
                builder: Box::new(provider_client_builder),
            }),
            request_gzip: request_gzip::RequestGzipPolicy::isolated(false),
            chatgpt_connections: ChatgptConnections::default(),
            cache_sessions: cache_runtime::Sessions::default(),
            #[cfg(feature = "test-harness")]
            warm_reply: None,
            #[cfg(test)]
            fail_connects: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// [`LiveProvider::route`]: steps go to the episode's own model.
const ROUTE_SOURCE: u8 = 0;
/// [`LiveProvider::route`]: steps go to the episode's refusal-fallback model.
const ROUTE_FALLBACK: u8 = 1;

/// A live provider: any wire family + its frozen render config + a runtime auth
/// credential, driven over reqwest. Generic over the family so the same transport
/// loop runs every grammar.
/// Media rendering is installed only when the immutable boot setting enables it.
pub struct LiveProvider<F: WireFamily + 'static> {
    quota_observer: Option<Arc<dyn crate::quota::QuotaObserver>>,
    pub(crate) client: ProviderHttpClient,
    clock: Clock,
    pub(crate) family: Arc<F>,
    pub(crate) frozen: Arc<FrozenRenderConfig>,
    subscription_auth: bool,
    temperature_policy: broca_core::provider::TemperaturePolicy,
    grammar_downgraded: std::sync::atomic::AtomicBool,
    media_renderer: std::sync::RwLock<Option<Arc<broca_core::provider::MediaRequestRenderer>>>,
    media_render_tags: std::sync::atomic::AtomicBool,
    /// The refusal-fallback model's render config, taken from `frozen.refusal_fallback` when
    /// the episode froze one.
    fallback: Option<Arc<FrozenRenderConfig>>,
    /// Whether the next step goes to the fallback model ([`ROUTE_FALLBACK`]) or the episode's
    /// own ([`ROUTE_SOURCE`]). Always the source when `fallback` is `None`.
    route: std::sync::atomic::AtomicU8,
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
    websocket_lease: std::sync::Mutex<Option<responses_websocket::Lease>>,
    cache_session: std::sync::Mutex<Option<Arc<cache_runtime::Session>>>,
    cache_predecessor_loader:
        std::sync::Mutex<Option<Arc<dyn broca_core::provider::CachePredecessorLoader>>>,
    #[cfg(test)]
    escape_client_builds: std::sync::Arc<AtomicUsize>,
    #[cfg(any(test, feature = "test-harness"))]
    retry_clock: RetryClock,
}

/// Owns the rendered body behind a reference count so retry requests do not copy the prompt.
#[derive(Clone)]
struct PreparedHttpRequest {
    quota_observer: Option<Arc<dyn crate::quota::QuotaObserver>>,
    cache_session: Option<(Arc<cache_runtime::Session>, Arc<FrozenRenderConfig>)>,
    cache_model: Option<String>,
    cache_provenance: Option<Arc<broca_protocol::render_provenance::RenderProvenance>>,
    attempt_hook: Option<Arc<dyn broca_core::run::AttemptHook>>,
    /// Local diagnostic policy, never part of the rendered request or its digest.
    ephemeral_session: bool,
    codex_wire: bool,
    auth_revision: u64,
    completed_output: Option<CompletedOutputRenderer>,
    url: String,
    headers: Vec<(String, String)>,
    body: Bytes,
    /// The request's wire family, so a non-2xx answer is classified by the family that sent it.
    /// `None` only for a request built outside a render job (tests). Read only on error.
    family: Option<crate::spec::WireFamilyId>,
    subscription_auth: bool,
    /// The wire family and the caller's service tier, when one was requested, so a non-2xx
    /// answer can be recognised as the provider refusing that tier. Read only on error.
    service_tier: Option<(
        crate::spec::WireFamilyId,
        crate::service_tier::ServiceTierRequest,
    )>,
    session_address: Option<String>,
    run_id: Option<String>,
    credential_id: Option<String>,
    account_ref: Option<String>,
    wire_model_id: Option<String>,
    speed_requested: Option<String>,
}

type CompletedOutputRenderer =
    Arc<dyn Fn(&broca_protocol::AssistantMessage) -> Vec<Value> + Send + Sync>;

impl From<HttpRequest> for PreparedHttpRequest {
    fn from(request: HttpRequest) -> Self {
        Self {
            quota_observer: None,
            attempt_hook: None,
            cache_session: None,
            cache_model: None,
            cache_provenance: None,
            ephemeral_session: false,
            codex_wire: false,
            auth_revision: 0,
            completed_output: None,
            url: request.url,
            headers: request.headers,
            body: Bytes::from(request.body),
            family: None,
            subscription_auth: false,
            service_tier: None,
            session_address: None,
            run_id: None,
            credential_id: None,
            account_ref: None,
            wire_model_id: None,
            speed_requested: None,
        }
    }
}

/// Immutable, whole-request input for one blocking render. All expensive input stays behind
/// shared handles; the closure is the sole owner that releases its prompt snapshot.
struct RenderJob<F: WireFamily + 'static> {
    quota_observer: Option<Arc<dyn crate::quota::QuotaObserver>>,
    cache_session: Option<Arc<cache_runtime::Session>>,
    cache_provider: Option<String>,
    subscription_auth: bool,
    media_renderer: Option<Arc<broca_core::provider::MediaRequestRenderer>>,
    media_render_tags: bool,
    auth_revision: u64,
    family: Arc<F>,
    frozen: Arc<FrozenRenderConfig>,
    auth: Arc<AuthHandle>,
    credential_hosts: CredentialHosts,
    opts: Arc<CallOptions>,
    step_id: StepId,
}

struct RenderedRequest {
    request: PreparedHttpRequest,
    name_map: Arc<crate::tool_name_alias::ToolNameMap>,
    request_digest: RequestDigest,
}

/// Length and SHA-256 of the canonical full request. Computed once per step on the render
/// worker; retry framing and connection-local suffixes never change this digest domain.
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
    quota_observer: Option<Arc<dyn crate::quota::QuotaObserver>>,
    cache_work: cache_runtime::Pending,
    cache_evidence: Option<Box<Value>>,
    attempt_hook: Option<Arc<dyn broca_core::run::AttemptHook>>,
    response_evidence: Option<crate::normalize::SharedResponseEvidence>,
    vendor_request_id: Option<String>,
    auth_refreshed: bool,
    attempt_id: Option<AttemptId>,
    attempt_ordinal: u32,
    send_index: u32,
    attempt_cancel: Option<CancellationToken>,
    ephemeral_session: bool,
    transport: Option<String>,
    websocket_request: Option<Box<broca_protocol::provider::WebSocketRequestDigest>>,
    websocket_sends: Option<responses_websocket::PhysicalSends>,
    websocket_close_code: Option<Arc<std::sync::atomic::AtomicU16>>,
    websocket_observation:
        std::sync::Weak<std::sync::Mutex<responses_websocket::AttemptObservation>>,
    socket_generation: Option<u64>,
    socket_reused: Option<bool>,
    socket_age: Option<Duration>,
    socket_since_last_completed: Option<Duration>,
    output_seen: bool,
    defer_failure_warning: bool,
    chunk_warning: Option<ChunkWarning>,
    served_account_ref: Option<String>,
    /// Given to the step's shutdown seal once this attempt's request reaches a server.
    sent_note: Option<SentNote>,
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
    content_gaps: ContentGaps,
    keepalives_after_content: u64,
    /// The response's own report of how the provider altered the request, once the stream
    /// normalizer has read it.
    input_transformations: Option<Vec<broca_protocol::InputTransformation>>,
    /// The request did not ask for that report (see `ModelAttempt::input_transformations_unmeasured`).
    input_transformations_unmeasured: bool,
    /// Set before the attempt is sent when it re-sends the step with a refreshed token.
    credential_refresh: Option<CredentialRefresh>,
    /// The provider refusal a Responses websocket wrapped in an error frame, when one ended this
    /// attempt; see [`responses_websocket::WrappedErrorFrame`].
    wrapped_error_frame: Option<Box<responses_websocket::WrappedErrorFrame>>,
    /// The account evidence the response carried; see [`broca_protocol::AttemptAccountObservation`].
    account_observation: broca_protocol::AttemptAccountObservation,
    /// What the newest `codex.rate_limits` frame of this response on a ChatGPT Responses socket
    /// shows spent (see [`chatgpt_frame_window`]): `None` before any such frame, then the window
    /// that frame shows spent, if exactly one. A usage limit the socket reports later in the same
    /// response takes its window from here.
    chatgpt_frame_window: Option<Option<&'static str>>,
}

impl AttemptTrace {
    /// The request reached a server, as far as this side can tell: the step's seal learns of
    /// the send, dated by the attempt's send stamp.
    fn note_reached_server(&self) {
        if let (Some(note), Some(sent_at_ms)) = (&self.sent_note, self.sent_at_ms) {
            note.note(sent_at_ms);
        }
    }

    async fn reserve_send(
        &mut self,
        request: &PreparedHttpRequest,
        body_bytes: usize,
        compressed: bool,
    ) -> Result<(), AttemptFailure> {
        if let Some(hook) = &self.attempt_hook {
            let endpoint_host = reqwest::Url::parse(&request.url)
                .ok()
                .and_then(|url| url.host_str().map(str::to_owned))
                .unwrap_or_default();
            let id = hook
                .before_send(
                    broca_core::run::PhysicalSend {
                        attempt: self.attempt_ordinal,
                        send_index: self.send_index.max(1),
                        endpoint_host,
                        model_sent: request.wire_model_id.clone().unwrap_or_default(),
                        request_body_bytes: body_bytes as u64,
                        compressed,
                        account_ref: self.served_account_ref.clone(),
                        auth_refreshed: self.auth_refreshed,
                        speed_requested: request.speed_requested.clone(),
                        warm: None,
                    },
                    self.attempt_cancel.clone().unwrap_or_default(),
                )
                .await
                .map_err(|error| {
                    AttemptFailure::transport_without_reissue(error, AttemptOutcome::ProviderError)
                })?;
            self.attempt_id = Some(id);
        }
        Ok(())
    }

    async fn settle_send(
        &mut self,
        digest: &RequestDigest,
        outcome: AttemptOutcome,
        billing_outcome: Option<BillingAttemptOutcome>,
    ) -> Result<(), ProviderError> {
        if let (Some(hook), Some(id)) = (&self.attempt_hook, self.attempt_id.take()) {
            let billing_outcome = billing_outcome.unwrap_or_else(|| {
                // The forensic vocabulary is open; an unfamiliar transport outcome remains
                // unknown billing evidence rather than being guessed as a successful call.
                serde_json::from_value(serde_json::to_value(&outcome).expect("outcome serializes"))
                    .unwrap_or_default()
            });
            hook.finished(
                id,
                self.clone()
                    .into_attempt(self.attempt_ordinal as usize, digest, outcome),
                billing_outcome,
                Some(response_envelope(
                    self.response_evidence
                        .as_ref()
                        .map(|evidence| std::mem::take(&mut *evidence.lock().unwrap()))
                        .unwrap_or_default(),
                    self.vendor_request_id.take(),
                )),
            )
            .await?;
        }
        Ok(())
    }

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
        mut self,
        attempt: usize,
        request: &RequestDigest,
        outcome: AttemptOutcome,
    ) -> ModelAttempt {
        if let (Some(sends), Some(frame)) = (&self.websocket_sends, &mut self.websocket_request) {
            if let Some(codex) = frame.codex.as_mut() {
                codex.physical_send_count = sends.count();
            }
        }
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
            cache_evidence: self.cache_evidence,
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
    fn render(self) -> Result<RenderedRequest, Box<ProviderError>> {
        let mut provenance = broca_protocol::render_provenance::RenderProvenance::default();
        // This scan is deliberately in the same blocking job as rendering: it walks the entire
        // prompt to preserve aliases from historical tool calls on every resumed step.
        let mut tools = std::borrow::Cow::Borrowed(self.opts.tools.as_slice());
        if let Some(so) = &self.frozen.structured_output {
            if so.mode == broca_protocol::structured_output::Mode::SubmitTool {
                // The renderer and response restoration both receive this map, so the
                // structured-output declaration is allocated exactly once with the other tools.
                tools
                    .to_mut()
                    .push(crate::structured_output::submit_tool(so.schema()));
            }
        }
        let name_map = Arc::new(crate::tool_name_alias::map_for(
            &self.frozen,
            &tools,
            &self.opts.prompt,
        ));
        let body = if let Some(renderer) = &self.media_renderer {
            // A media render builds its own tool-name map: this callback API carries no shared map.
            renderer(
                &serde_json::to_value(&*self.frozen).expect("frozen render config serializes"),
                &self.opts,
                self.media_render_tags,
                true,
                Some(&mut provenance),
            )?
        } else {
            self.family.render_with_shared_name_map(
                &self.frozen,
                &self.opts,
                Some(&mut provenance),
                &name_map,
            )
        };
        let endpoint = self.family.endpoint(&self.frozen, &self.auth, body);
        self.credential_hosts.check(&self.auth, &endpoint.url)?;
        let mut request = PreparedHttpRequest::from(endpoint);
        request.ephemeral_session = self.opts.ephemeral_session;
        request.quota_observer = self.quota_observer;
        request.cache_provenance = Some(Arc::new(provenance));
        request.cache_session = self
            .cache_session
            .map(|session| (session, Arc::clone(&self.frozen)));
        request.codex_wire = self.frozen.chatgpt_wire.is_some();
        request.auth_revision = self.auth_revision;
        if request.codex_wire {
            let frozen = Arc::clone(&self.frozen);
            let target_model = self.opts.target_model.clone();
            let names = Arc::clone(&name_map);
            request.completed_output = Some(Arc::new(move |assistant| {
                crate::families::openai_responses::completed_output_as_input(
                    &frozen,
                    target_model.as_ref(),
                    &names,
                    assistant,
                )
            }));
        }
        request.family = Some(self.frozen.family_id);
        request.subscription_auth = self.subscription_auth;
        request.service_tier = self
            .frozen
            .service_tier_request
            .clone()
            .map(|tier| (self.frozen.family_id, tier));
        if let Some(context) = &self.opts.step_context {
            request.session_address = context.session_address.clone();
            request.run_id = Some(context.run_id.clone());
            request.credential_id = context.credential_id.clone();
            request.account_ref = context.account_ref.clone();
        }
        request.wire_model_id = Some(self.frozen.wire_model_id.clone());
        request.cache_model = self
            .cache_provider
            .map(|provider| format!("{provider}/{}", self.frozen.wire_model_id));
        maybe_dump_rendered_request(&request.body, &self.opts, self.step_id);
        let request_digest = RequestDigest::of(&request.body);
        Ok(RenderedRequest {
            request,
            name_map,
            request_digest,
        })
    }
}

/// `frozen` with the step's lowered output cap applied (see [`FrozenRenderConfig::for_step`]);
/// the same allocation when the step keeps the frozen cap.
fn step_frozen(frozen: Arc<FrozenRenderConfig>, opts: &CallOptions) -> Arc<FrozenRenderConfig> {
    match frozen.for_step(opts) {
        std::borrow::Cow::Borrowed(_) => frozen,
        std::borrow::Cow::Owned(lowered) => Arc::new(lowered),
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

/// The provider client for bounded operator probes. Redirects and automatic HTTP retries
/// would spend uncounted requests, so each explicit send must be a single attempt.
pub fn build_single_attempt_http_client() -> ProviderHttpClient {
    ProviderHttpClient::build(
        || provider_client_builder().retry(reqwest::retry::never()),
        request_gzip::RequestGzipPolicy::from_env(),
    )
}

/// The connection settings both of a provider client's reqwest clients share.
///
/// No provider connection follows a redirect. A request carries its credential in a header,
/// and reqwest drops only `Authorization` and cookies when a redirect changes host, never a key
/// header such as `x-api-key`, so following one could hand a key to a host nobody approved.
fn provider_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .connect_timeout(PROVIDER_CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
}

impl<F: WireFamily + 'static> LiveProvider<F> {
    #[cfg(feature = "test-harness")]
    pub fn with_virtual_retry_waits(mut self, waits: Arc<std::sync::Mutex<Vec<Duration>>>) -> Self {
        self.retry_clock.virtual_waits = Some(waits);
        self
    }

    /// Replace only legacy warm network I/O with a ready success for allocation accounting.
    #[cfg(feature = "test-harness")]
    pub fn with_warm_reply_for_allocation_test(mut self, body: Vec<u8>) -> Self {
        self.client.warm_reply = Some(Bytes::from(body));
        self
    }

    /// Allocation-test reference for the original warm path, without async-trait delegation.
    #[cfg(feature = "test-harness")]
    pub async fn warm_legacy_reference_for_test(
        &self,
        base: &[u8],
    ) -> Result<broca_core::WarmSent, broca_core::WarmFailure> {
        let (request, account) = self.prepare_warm_request(base, None)?;
        send_warm_legacy(
            &self.client,
            &self.clock,
            request,
            self.account_limit_context(),
            account,
        )
        .await
    }

    /// The account the served credential now belongs to, when it is not the one the session
    /// froze; a step and a warm both refuse to send then. Unknown account metadata on either
    /// side cannot establish a mismatch. ChatGPT also carries an account in its token's own
    /// `chatgpt-account-id` header, and a re-login can replace that account behind an
    /// unchanged vault record, so the header must match as well.
    fn changed_served_account<'a>(&self, served: &'a ServedAuth) -> Option<&'a str> {
        let frozen = self.frozen_account_ref.as_deref()?;
        let changed = |account: Option<&'a str>| account.filter(|account| *account != frozen);
        changed(served.account_ref.as_deref()).or_else(|| {
            (self.frozen.family_id == crate::spec::WireFamilyId::OpenAiResponsesChatgpt)
                .then(|| changed(chatgpt_account(&served.auth)))
                .flatten()
        })
    }

    fn prepare_warm_request(
        &self,
        base: &[u8],
        policy: Option<&mut broca_core::cache_warm::WarmSendPolicy>,
    ) -> Result<(HttpRequest, Option<String>), broca_core::WarmFailure> {
        let served = self.current_auth();
        // The same refusal a step makes: a warm sends the session's prompt with the held token,
        // so a token now belonging to another account must not carry it either.
        if let Some(served_account) = self.changed_served_account(&served) {
            return Err(broca_core::WarmFailure::AccountChanged {
                frozen: self.frozen_account_ref.clone(),
                served: Some(served_account.to_owned()),
            });
        }
        // Reuse the route of the request being warmed, including a refusal fallback.
        let fallback = self.routed_fallback();
        let warm_frozen = match fallback {
            Some(fallback) => Arc::clone(fallback),
            None => Arc::clone(&self.frozen),
        };
        if let Some(policy) = policy {
            policy.auth_refreshed = served.auth_refreshed;
            policy.provider = fallback.and_then(|_| {
                self.frozen
                    .refusal_fallback
                    .as_ref()
                    .map(|f| f.model.provider_module_id.clone())
            });
            policy.ttl_ms = warm_frozen
                .cache
                .as_ref()
                .and_then(|cache| match &cache.class {
                    crate::wire::CacheClass::Markers {
                        ttl: crate::wire::CacheTtl::FiveMinutes,
                        ..
                    } => Some(300_000),
                    crate::wire::CacheClass::Markers {
                        ttl: crate::wire::CacheTtl::OneHour,
                        ..
                    } => Some(3_600_000),
                    _ => None,
                });
        }
        let body = self
            .family
            .warm_body(&warm_frozen, base)
            .map_err(broca_core::WarmFailure::Unsupported)?;
        // A warm only refreshes the cache if it repeats the step's prefix exactly, so it is
        // encoded the way the transport sends a step over HTTPS: a Codex identity puts the
        // backend's routing fields first (see `send_attempt`).
        let body = if warm_frozen.chatgpt_wire.is_some() {
            crate::chatgpt_wire::request_bytes(&body).ok_or_else(|| {
                broca_core::WarmFailure::Unsupported("the warm body is not a JSON object".into())
            })?
        } else {
            body
        };
        // A zero-output cache read must stay on the addressed model, even when generation
        // opted into server-side fallback. The warm body has already removed that opt-in;
        // derive headers without the server-side fallback beta opt-in as well, leaving prompt bytes alone.
        let endpoint_frozen = warm_frozen.server_side_fallback.then(|| {
            let mut frozen = (*warm_frozen).clone();
            frozen.server_side_fallback = false;
            frozen
        });
        let request = self.family.endpoint(
            endpoint_frozen.as_ref().unwrap_or(&warm_frozen),
            &served.auth,
            body,
        );
        served
            .hosts
            .check(&served.auth, &request.url)
            .map_err(broca_core::WarmFailure::Provider)?;
        Ok((request, served.account_ref))
    }

    fn effective_frozen(&self) -> Arc<FrozenRenderConfig> {
        if !self.grammar_downgraded.load(Ordering::Acquire) {
            return Arc::clone(&self.frozen);
        }
        let mut frozen = (*self.frozen).clone();
        if let Some(so) = &mut frozen.structured_output {
            so.downgrade_grammar();
        }
        Arc::new(frozen)
    }

    /// The base config the next step is rendered with, before the step's own output cap is
    /// applied (see [`step_frozen`]): the refusal-fallback model's while the route says so,
    /// otherwise [`Self::effective_frozen`]. A grammar downgrade applies to the fallback's
    /// structured output too, which admission froze identical to the source's.
    fn route_frozen(&self) -> Arc<FrozenRenderConfig> {
        let Some(fallback) = self.routed_fallback() else {
            return self.effective_frozen();
        };
        if !self.grammar_downgraded.load(Ordering::Acquire) {
            return Arc::clone(fallback);
        }
        let mut frozen = (**fallback).clone();
        if let Some(so) = &mut frozen.structured_output {
            so.downgrade_grammar();
        }
        Arc::new(frozen)
    }
    /// The fallback model's config when the route currently sends steps there.
    fn routed_fallback(&self) -> Option<&Arc<FrozenRenderConfig>> {
        self.fallback
            .as_ref()
            .filter(|_| self.route.load(Ordering::Acquire) == ROUTE_FALLBACK)
    }

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
            quota_observer: None,
            clock,
            family: Arc::new(family),
            fallback: frozen
                .refusal_fallback
                .as_ref()
                .map(|fallback| Arc::new(fallback.frozen.clone())),
            route: std::sync::atomic::AtomicU8::new(ROUTE_SOURCE),
            frozen: Arc::new(frozen),
            subscription_auth: false,
            temperature_policy: Default::default(),
            grammar_downgraded: std::sync::atomic::AtomicBool::new(false),
            media_renderer: std::sync::RwLock::new(None),
            media_render_tags: std::sync::atomic::AtomicBool::new(false),
            // Wrapped HERE rather than taken as an Arc, so callers keep passing an owned
            // AuthHandle and the sharing stays inside this file. Each step takes a cheap Arc
            // snapshot of the current credential rather than a deep copy of the secret; only
            // an in-step refresh after a 401 ever replaces it.
            auth: Arc::new(std::sync::RwLock::new(ServedAuth {
                definite_auth_failure: None,
                revision: 0,
                auth_refreshed: false,
                auth: Arc::new(auth),
                account_ref: None,
                hosts: CredentialHosts::default(),
            })),
            frozen_account_ref: None,
            refresher: None,
            websocket: Arc::new(responses_websocket::Session::default()),
            websocket_lease: std::sync::Mutex::new(None),
            cache_session: std::sync::Mutex::new(None),
            cache_predecessor_loader: std::sync::Mutex::new(None),
            #[cfg(test)]
            escape_client_builds: std::sync::Arc::new(AtomicUsize::new(0)),
            #[cfg(any(test, feature = "test-harness"))]
            retry_clock: RetryClock::default(),
        }
    }

    /// Use the module's shared clock for relative provider reset deadlines.
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_quota_observer(
        mut self,
        observer: Option<Arc<dyn crate::quota::QuotaObserver>>,
    ) -> Self {
        self.client.warm_quota_observer = observer.clone();
        self.quota_observer = observer;
        self
    }

    /// Let this provider recover from an HTTP 401 inside a step: ask `refresher` for a fresh
    /// token for the same credential and, when it differs from the rejected one, re-send the
    /// same request once with it.
    pub fn with_credential_refresher(mut self, refresher: Arc<dyn CredentialRefresher>) -> Self {
        self.refresher = Some(refresher);
        self
    }

    /// Preserve the resolved credential method for refusals that mean different things for
    /// prepaid API keys and replenishing subscriptions. Bearer headers alone cannot tell them apart.
    pub fn with_auth_method(mut self, method: Option<&str>) -> Self {
        self.subscription_auth = method.is_some_and(|method| method != "apikey");
        self
    }

    /// Model facts are consumed only at admission; the loop persists the effective setting.
    pub fn with_temperature_policy(
        mut self,
        policy: broca_core::provider::TemperaturePolicy,
    ) -> Self {
        self.temperature_policy = policy;
        self
    }

    fn account_limit_context(&self) -> AccountLimitContext {
        AccountLimitContext {
            family: self.frozen.family_id,
            subscription_auth: self.subscription_auth,
        }
    }

    /// Keep the account selected when the run was admitted separate from the account
    /// associated with the token currently being used.
    /// The hosts the credential's vault record lists. Until this is called a request carrying
    /// a credential may go only to the local machine (see [`CredentialHosts::default`]).
    pub fn with_credential_hosts(self, hosts: CredentialHosts) -> Self {
        self.auth
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .hosts = hosts;
        self
    }

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
        self.start_step_under_seal(step_id, opts, cancel, deadline, None, None, 0, None)
    }

    // The step's identity, its shared request, cancellation, deadline, seal, progress slot and
    // first attempt ordinal are each a separate runtime input; grouping them would only rename
    // them.
    #[allow(clippy::too_many_arguments)]
    fn start_step_under_seal(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        cancel: CancellationToken,
        deadline: Option<Instant>,
        seal: Option<ShutdownSeal>,
        progress: Option<broca_core::provider::LiveStepProgress>,
        first_attempt: u32,
        attempt_hook: Option<Arc<dyn broca_core::run::AttemptHook>>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.auth
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .definite_auth_failure = None;
        let (tx, rx) = mpsc::channel::<NormalizedEvent>(256);
        let served = self.current_auth();
        let error = self
            .changed_served_account(&served)
            .and_then(|served_account| {
                account_changed_error(self.frozen_account_ref.as_deref(), Some(served_account))
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
            quota_observer: self.quota_observer.clone(),
            cache_session: self.cache_session.lock().unwrap().clone(),
            cache_provider: if self.routed_fallback().is_some() {
                self.frozen
                    .refusal_fallback
                    .as_ref()
                    .map(|f| f.model.provider_module_id.clone())
            } else {
                opts.target_model
                    .as_ref()
                    .map(|m| m.provider_module_id.clone())
            },
            subscription_auth: self.subscription_auth,
            media_renderer: self.media_renderer.read().unwrap().clone(),
            media_render_tags: self.media_render_tags.load(Ordering::Relaxed),
            auth_revision: served.revision,
            family: Arc::clone(&self.family),
            frozen: step_frozen(self.route_frozen(), &opts),
            auth: served.auth,
            credential_hosts: served.hosts,
            opts,
            step_id,
        };
        #[cfg(any(test, feature = "test-harness"))]
        let retry_clock = self.retry_clock.clone();
        #[cfg(not(any(test, feature = "test-harness")))]
        let retry_clock = RetryClock::default();
        let recovery = self.refresher.as_ref().map(|refresher| AuthRecovery {
            refresher: Arc::clone(refresher),
            current: Arc::clone(&self.auth),
            frozen_account_ref: self.frozen_account_ref.clone(),
        });

        let route = if self.routed_fallback().is_some() {
            broca_core::refusal_fallback::StepRoute::Fallback
        } else {
            broca_core::refusal_fallback::StepRoute::Source
        };
        spawn_transport_task(
            self.client.clone(),
            self.clock.clone(),
            Arc::clone(&self.family),
            // The normalizer reports an output-ceiling cut with the cap this step was sent with.
            // A fallback step's response is normalized, and its retries re-addressed, with the
            // config it was rendered with, so they name the model that answers it.
            if route == broca_core::refusal_fallback::StepRoute::Fallback {
                Arc::clone(&render_job.frozen)
            } else {
                step_frozen(Arc::clone(&self.frozen), &render_job.opts)
            },
            route,
            render_job,
            served.account_ref,
            recovery,
            step_id,
            first_attempt,
            tx,
            cancel,
            deadline,
            seal,
            progress,
            attempt_hook,
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
    fn turn_system_possible(
        &self,
        preceding: Option<&broca_protocol::Message>,
        followed_by_user: bool,
    ) -> bool {
        crate::turn_system::possible_after(&self.route_frozen(), preceding, followed_by_user)
    }
    fn history_edit_cost(
        &self,
    ) -> Option<broca_core::compaction_provider::notice::HistoryEditCost> {
        use broca_core::compaction_provider::notice::HistoryEditCost;
        let route = self.route_frozen();
        Some(
            if route.chatgpt_wire.is_some() && !self.websocket.forced_https() {
                HistoryEditCost::FullResend
            } else {
                HistoryEditCost::Suffix
            },
        )
    }

    fn prepare_chain_reset(&self) -> bool {
        self.websocket.prepare_chain_reset()
    }

    fn supports_media_renderer(&self) -> bool {
        true
    }

    fn set_media_request_renderer(
        &self,
        renderer: Arc<broca_core::provider::MediaRequestRenderer>,
    ) {
        *self.media_renderer.write().unwrap() = Some(renderer);
    }

    fn defer_media_tags(&self, render_tags: bool) -> bool {
        self.media_render_tags.store(render_tags, Ordering::Relaxed);
        self.media_renderer.read().unwrap().is_some()
    }
    fn set_credential_revision(&self, revision: u64) {
        self.auth.write().unwrap().revision = revision;
    }

    fn bind_run_transport(&self, session: &broca_protocol::BindIdentity) {
        let cache = self.client.cache_sessions.bind(session);
        cache.set_loader(self.cache_predecessor_loader.lock().unwrap().take());
        *self.cache_session.lock().unwrap() = Some(cache);
        if self.frozen.family_id == crate::spec::WireFamilyId::OpenAiResponsesChatgpt
            && self.frozen.chatgpt_wire.is_some()
        {
            let mut lease = self.websocket_lease.lock().unwrap();
            if lease.is_none() {
                *lease = self
                    .client
                    .chatgpt_connections
                    .acquire(session, &self.websocket);
            }
        }
    }

    fn set_cache_predecessor_loader(
        &self,
        loader: Arc<dyn broca_core::provider::CachePredecessorLoader>,
    ) {
        *self.cache_predecessor_loader.lock().unwrap() = Some(loader);
    }

    fn set_cache_evidence_view(&self, view: Option<u64>) {
        if let Some(session) = self.cache_session.lock().unwrap().as_ref() {
            session.set_view(view);
        }
    }

    fn annotate_cache_evidence(&self, attempt: &mut ModelAttempt) {
        if let Some(session) = self.cache_session.lock().unwrap().as_ref() {
            session.annotate(attempt);
        }
    }

    fn cache_attempt_recorded(&self, locator: Option<String>) {
        if let Some(session) = self.cache_session.lock().unwrap().as_ref() {
            session.recorded(locator);
        }
    }

    fn seed_accepted_send(
        &self,
        account: &str,
        evidence: broca_core::provider::AcceptedSendEvidence,
    ) {
        tracing::debug!(target: "provider", account, ?evidence, "loaded session accepted-send evidence");
        if let Some(session) = self.cache_session.lock().unwrap().as_ref() {
            session.seed_accepted_send(account, evidence);
        }
    }

    fn accepted_send_evidence(&self, account: &str) -> broca_core::provider::AcceptedSendEvidence {
        self.cache_session.lock().unwrap().as_ref().map_or(
            broca_core::provider::AcceptedSendEvidence::NotLoaded,
            |session| session.accepted_send_evidence(account),
        )
    }

    fn last_accepted_at_ms(&self, account: &str) -> Option<u64> {
        self.cache_session
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|session| session.last_accepted_at_ms(account))
    }

    fn reasoning_is_prefix_bound(&self) -> bool {
        self.frozen.family_id.thinking_is_prefix_bound()
    }

    fn structured_output_grammar_rejection(&self, error: &ProviderError) -> bool {
        self.frozen.family_id == crate::spec::WireFamilyId::AnthropicMessages
            && !self.grammar_downgraded.load(Ordering::Acquire)
            && self.frozen.structured_output.as_ref().is_some_and(|so| {
                so.delivery == broca_protocol::structured_output::SchemaDelivery::Strict
            })
            && crate::structured_output::grammar_rejection(error)
    }

    fn downgrade_structured_output(&self) {
        self.grammar_downgraded.store(true, Ordering::Release);
        if let Some(so) = &self.frozen.structured_output {
            crate::structured_output::remember_grammar_refusal(
                &self.frozen.wire_model_id,
                &so.schema_jcs,
            );
        }
    }
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
            runtime.first_attempt,
            runtime.attempt_hook,
        )
    }
    fn close_run_transport(&self) {
        if let Some(session) = self.cache_session.lock().unwrap().as_ref() {
            session.set_loader(None);
        }
        self.websocket.close();
        self.websocket_lease.lock().unwrap().take();
    }

    fn complete_run_transport(&self) {
        if let Some(session) = self.cache_session.lock().unwrap().as_ref() {
            session.set_loader(None);
        }
        if let Some(lease) = self.websocket_lease.lock().unwrap().take() {
            lease.complete(&self.websocket);
        }
        self.websocket.close();
    }

    fn set_chatgpt_https_policy(&self, policy: Arc<dyn Fn() -> bool + Send + Sync>) {
        self.websocket.set_policy(policy);
    }

    fn refusal_fallback_model(&self) -> Option<broca_protocol::request::ModelHandle> {
        self.frozen
            .refusal_fallback
            .as_ref()
            .map(|fallback| fallback.model.clone())
    }

    fn refusal_fallback_context_limit(&self) -> Option<u64> {
        self.frozen
            .refusal_fallback
            .as_ref()
            .and_then(|fallback| fallback.context_limit)
    }

    fn set_step_route(&self, route: broca_core::refusal_fallback::StepRoute) {
        let route = match route {
            broca_core::refusal_fallback::StepRoute::Fallback if self.fallback.is_some() => {
                ROUTE_FALLBACK
            }
            _ => ROUTE_SOURCE,
        };
        self.route.store(route, Ordering::Release);
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
        self.start_step_under_seal(step_id, opts, cancel, deadline, Some(seal), None, 0, None)
    }

    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        self.try_render_request(opts)
            .expect("provider request could not be prepared")
    }

    fn try_render_request(&self, opts: &CallOptions) -> Result<Vec<u8>, Box<ProviderError>> {
        let route = self.route_frozen();
        let frozen = route.for_step(opts);
        if let Some(renderer) = self.media_renderer.read().unwrap().as_ref() {
            return renderer(
                &serde_json::to_value(&frozen).expect("frozen render config serializes"),
                opts,
                self.media_render_tags.load(Ordering::Relaxed),
                false,
                None,
            );
        }
        Ok(self.family.render(&frozen, opts))
    }

    fn request_len(&self, opts: &CallOptions) -> Result<usize, Box<ProviderError>> {
        // A request carrying media is rendered: its payloads are read and placed as it is sent.
        if self.media_renderer.read().unwrap().is_some() {
            return self.try_render_request(opts).map(|bytes| bytes.len());
        }
        let route = self.route_frozen();
        Ok(self.family.rendered_len(&route.for_step(opts), opts))
    }

    fn output_limits(&self) -> Option<broca_core::output_reservation::OutputLimits> {
        self.route_frozen().output_limits()
    }

    fn renderer_version(&self) -> u32 {
        self.frozen.renderer_version
    }

    /// Only the current implementation's compatibility table can vouch for old bytes.
    /// A version copied into the frozen config is not evidence that this code reproduces it.
    /// An embedded refusal-fallback config keeps the version it was frozen at, and must be
    /// reproducible too.
    fn reproduces_renderer_version(&self, recorded: u32) -> bool {
        let family = self.frozen.family_id;
        crate::families::reproduces_renderer_version(family, recorded)
            && self.fallback.as_ref().is_none_or(|fallback| {
                crate::families::reproduces_renderer_version(
                    fallback.family_id,
                    fallback.renderer_version,
                )
            })
    }

    /// The frozen config carries its renderer version, so a run recorded at an earlier version
    /// this provider reproduces recorded the same config with that version in it. Every other
    /// field must still match the recorded blob exactly.
    fn render_config_blob_for_version(&self, renderer_version: u32) -> Value {
        if renderer_version == self.frozen.renderer_version
            || !self.reproduces_renderer_version(renderer_version)
        {
            return self.render_config_blob();
        }
        let mut frozen = self.effective_frozen().as_ref().clone();
        frozen.renderer_version = renderer_version;
        serde_json::to_value(&frozen).expect("frozen render config must serialize")
    }

    fn render_config_blob(&self) -> Value {
        serde_json::to_value(self.effective_frozen().as_ref())
            .expect("frozen render config must serialize")
    }

    fn fresh_admission_config_error(
        &self,
        generation: &GenerationConfig,
        tool_choice: &ToolChoice,
    ) -> Option<String> {
        fresh_admission_config_error(&self.frozen, generation, tool_choice)
    }

    fn admit_temperature(&self, requested: Option<f64>) -> Option<f64> {
        crate::families::admit_temperature(&self.frozen, self.temperature_policy, requested)
    }

    /// The transport records every HTTP attempt, and on a cancel sends the in-flight
    /// attempt's `cancelled` record before its task ends and the stream closes.
    fn reports_attempt_evidence(&self) -> bool {
        true
    }
    fn definite_auth_failure(&self, step_id: StepId) -> bool {
        self.auth
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .definite_auth_failure
            .is_some_and(|(step, _)| step == step_id)
    }

    /// One attempt with the credential this provider currently holds, after checking, as a step
    /// does, that the credential's account is still the one the session froze. Never retried: a
    /// warm that fails is reported, not repeated.
    async fn warm(&self, base: &[u8]) -> Result<broca_core::WarmSent, broca_core::WarmFailure> {
        let (request, account) = self.prepare_warm_request(base, None)?;
        send_warm_legacy(
            &self.client,
            &self.clock,
            request,
            self.account_limit_context(),
            account,
        )
        .await
    }

    async fn warm_with_hook(
        &self,
        base: &[u8],
        hook: Option<Arc<dyn broca_core::run::AttemptHook>>,
    ) -> Result<broca_core::WarmSent, broca_core::WarmFailure> {
        let mut policy = hook
            .as_ref()
            .map(|_| broca_core::cache_warm::WarmSendPolicy::default());
        let (request, account) = self.prepare_warm_request(base, policy.as_mut())?;
        send_warm(
            &self.client,
            &self.clock,
            request,
            self.account_limit_context(),
            account,
            hook,
            policy,
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
    family: impl Into<AccountLimitContext>,
    served_account_ref: Option<String>,
    hook: Option<Arc<dyn broca_core::run::AttemptHook>>,
    policy: Option<broca_core::cache_warm::WarmSendPolicy>,
) -> Result<broca_core::WarmSent, broca_core::WarmFailure> {
    let family = family.into();
    match hook {
        None => send_warm_legacy(client, clock, request, family, served_account_ref).await,
        Some(hook) => {
            send_warm_journaled(
                client,
                clock,
                request,
                family,
                served_account_ref,
                Some(hook),
                policy,
            )
            .await
        }
    }
}

/// Keep legacy requests on the original path: no billing parse, envelope or extra boxed call.
async fn send_warm_legacy(
    client: &ProviderHttpClient,
    clock: &Clock,
    request: HttpRequest,
    family: impl Into<AccountLimitContext>,
    served_account_ref: Option<String>,
) -> Result<broca_core::WarmSent, broca_core::WarmFailure> {
    let classification = family.into();
    let family = classification.family;
    let warm_body_sha256 = lower_hex(&Sha256::digest(&request.body));
    let anthropic_api_host = request_gzip::is_anthropic_api_host(&request.url);
    let identity = Bytes::from(request.body);
    let failed = |message: String| {
        broca_core::WarmFailure::Provider(Box::new(ProviderError {
            provider_details: None,
            cause: None,
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
            account_continue: None,
        }))
    };
    let exchange = async {
        let mut gzip = encode_request_gzip(client, anthropic_api_host, &identity).await;
        let (sent_at_ms, status, header_map, body) = loop {
            let sent_at_ms = now_millis();
            #[cfg(feature = "test-harness")]
            if let Some(body) = &client.warm_reply {
                break (
                    sent_at_ms,
                    reqwest::StatusCode::OK,
                    reqwest::header::HeaderMap::new(),
                    body.to_vec(),
                );
            }
            let response = attempt_request_builder(
                client,
                &request.url,
                &request.headers,
                anthropic_api_host,
                gzip.is_some(),
                false,
            )
            .body(gzip.clone().unwrap_or_else(|| identity.clone()))
            .send()
            .await
            .map_err(|error| failed(format!("warm request failed: {error}")))?;
            let status = response.status();
            let header_map = response.headers().clone();
            if let Some(observer) = &client.warm_quota_observer {
                observer.headers(&header_map, now_millis());
            }
            let body =
                read_warm_body_legacy(response, &failed, client.warm_quota_observer.as_deref())
                    .await?;
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
            log_opencode_go_refusal(&request.headers, code, &headers, &text);
            let error = classify_http_error_in_family(
                code,
                &text,
                retry_after_secs,
                None,
                Some(family),
                None,
            );
            let error = classify_account_limit_exhausted_with_auth(
                classification,
                code,
                &headers,
                &text,
                UNIX_EPOCH + Duration::from_millis(clock.now_ms()),
                error,
            );
            let error = opencode_payment_required(&request.url, code, error);
            let error = inception_payment_required(&request.url, code, error);
            return Err(broca_core::WarmFailure::Provider(Box::new(error)));
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
            request_body_bytes: Some(gzip.as_ref().unwrap_or(&identity).len() as u64),
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

async fn read_warm_body_legacy(
    response: reqwest::Response,
    failed: &impl Fn(String) -> broca_core::WarmFailure,
    observer: Option<&dyn crate::quota::QuotaObserver>,
) -> Result<Vec<u8>, broca_core::WarmFailure> {
    let mut quota_sse = crate::quota::SseTap::new(observer);
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| failed(format!("warm answer unreadable: {error}")))?;
        let received_at = quota_sse.as_ref().map(|_| now_millis());
        if body.len() + chunk.len() > MAX_WARM_RESPONSE_BYTES {
            return Err(failed("warm answer exceeded its size limit".into()));
        }
        body.extend_from_slice(&chunk);
        if let Some(tap) = &mut quota_sse {
            tap.push(&body, received_at.expect("quota tap timestamp"));
        }
    }
    if let Some(tap) = &mut quota_sse {
        tap.finish(&body);
    }
    Ok(body)
}

async fn send_warm_journaled(
    client: &ProviderHttpClient,
    clock: &Clock,
    request: HttpRequest,
    family: impl Into<AccountLimitContext>,
    served_account_ref: Option<String>,
    hook: Option<Arc<dyn broca_core::run::AttemptHook>>,
    policy: Option<broca_core::cache_warm::WarmSendPolicy>,
) -> Result<broca_core::WarmSent, broca_core::WarmFailure> {
    let classification = family.into();
    let family = classification.family;
    use broca_records::record::{AttemptEnvelope, AttemptSent, BillingAttemptOutcome as Outcome};
    let warm_body_sha256 = lower_hex(&Sha256::digest(&request.body));
    let anthropic_api_host = request_gzip::is_anthropic_api_host(&request.url);
    let identity = Bytes::from(request.body);
    let failed = |message: String| {
        broca_core::WarmFailure::Provider(Box::new(ProviderError {
            provider_details: None,
            cause: None,
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
            account_continue: None,
        }))
    };
    let mut id = None;
    let mut envelope = AttemptEnvelope::default();
    let mut send_index = 0;
    let model_sent = serde_json::from_slice::<Value>(&identity)
        .ok()
        .and_then(|body| body.get("model").and_then(Value::as_str).map(str::to_owned))
        .unwrap_or_default();
    let exchange = async {
        let mut gzip = encode_request_gzip(client, anthropic_api_host, &identity).await;
        let (sent_at_ms, status, header_map, body) = loop {
            send_index += 1;
            envelope = AttemptEnvelope::default();
            if let Some(hook) = &hook {
                id = Some(
                    tokio::time::timeout(
                        Duration::from_secs(30),
                        hook.before_send(
                            broca_core::run::PhysicalSend {
                                attempt: send_index - 1,
                                send_index,
                                endpoint_host: reqwest::Url::parse(&request.url)
                                    .ok()
                                    .and_then(|url| url.host_str().map(str::to_owned))
                                    .unwrap_or_default(),
                                model_sent: model_sent.clone(),
                                request_body_bytes: gzip.as_ref().unwrap_or(&identity).len() as u64,
                                compressed: gzip.is_some(),
                                account_ref: served_account_ref.clone(),
                                auth_refreshed: policy
                                    .as_ref()
                                    .is_some_and(|policy| policy.auth_refreshed),
                                speed_requested: serde_json::from_slice::<Value>(&identity)
                                    .ok()
                                    .and_then(|body| {
                                        body.get("speed").and_then(Value::as_str).map(str::to_owned)
                                    }),
                                warm: policy.clone(),
                            },
                            tokio_util::sync::CancellationToken::new(),
                        ),
                    )
                    .await
                    .map_err(|_| failed("warm reservation permission deadline expired".into()))?
                    .map_err(|error| broca_core::WarmFailure::Provider(Box::new(error)))?,
                );
            }
            let sent_at_ms = now_millis();
            envelope.sent_at_ms = Some(sent_at_ms);
            envelope.sent = AttemptSent::Yes;
            let response = attempt_request_builder(
                client,
                &request.url,
                &request.headers,
                anthropic_api_host,
                gzip.is_some(),
                hook.is_some(),
            )
            .body(gzip.clone().unwrap_or_else(|| identity.clone()))
            .send()
            .await
            .map_err(|error| {
                envelope.outcome = if error.is_connect() {
                    envelope.sent = AttemptSent::No;
                    envelope.sent_at_ms = None;
                    Outcome::ConnectFailed
                } else {
                    Outcome::NoStatus
                };
                failed(format!("warm request failed: {error}"))
            })?;
            let status = response.status();
            envelope.http_status = Some(status.as_u16());
            envelope.vendor_request_id = vendor_request_id(response.headers());
            let header_map = response.headers().clone();
            if let Some(observer) = &client.warm_quota_observer {
                observer.headers(&header_map, now_millis());
            }
            let body = read_warm_body(
                response,
                &failed,
                &mut envelope,
                client.warm_quota_observer.as_deref(),
            )
            .await?;
            warm_billing_usage(&body, family, &mut envelope);
            if gzip.is_some() {
                if let Some(rejection) = request_gzip::classify_rejection(
                    status.as_u16(),
                    &header_map,
                    &String::from_utf8_lossy(&body),
                ) {
                    client.request_gzip.latch(status.as_u16(), rejection);
                    envelope.outcome = Outcome::CompressionRefused;
                    envelope.finished_at_ms = Some(now_millis());
                    if let (Some(hook), Some(id)) = (&hook, id.take()) {
                        hook.warm_finished(id, envelope.clone(), None, warm_body_sha256.clone())
                            .await
                            .map_err(|error| broca_core::WarmFailure::Provider(Box::new(error)))?;
                    }
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
            envelope.outcome = Outcome::HttpError;
            let code = status.as_u16();
            let text = String::from_utf8_lossy(&body);
            log_opencode_go_refusal(&request.headers, code, &headers, &text);
            let error = classify_http_error_in_family(
                code,
                &text,
                retry_after_secs,
                None,
                Some(family),
                None,
            );
            let error = classify_account_limit_exhausted_with_auth(
                classification,
                code,
                &headers,
                &text,
                UNIX_EPOCH + Duration::from_millis(clock.now_ms()),
                error,
            );
            let error = opencode_payment_required(&request.url, code, error);
            let error = inception_payment_required(&request.url, code, error);
            return Err(broca_core::WarmFailure::Provider(Box::new(error)));
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
        envelope.outcome = Outcome::Complete;
        Ok(broca_core::WarmSent {
            request_body_bytes: Some(gzip.as_ref().unwrap_or(&identity).len() as u64),
            warm_body_sha256: warm_body_sha256.clone(),
            sent_at_ms,
            usage,
            served_account_ref: served_account_ref.clone(),
            ratelimit_headers,
            http_status: status.as_u16(),
        })
    };
    let result = match tokio::time::timeout(WARM_TIMEOUT, exchange).await {
        Ok(result) => result,
        Err(_) => {
            envelope.outcome = if envelope.http_status.is_some() {
                Outcome::IdleTimeout
            } else {
                Outcome::HeaderTimeout
            };
            Err(failed("warm timed out waiting for its answer".into()))
        }
    };
    envelope.finished_at_ms = Some(now_millis());
    if let (Some(hook), Some(id)) = (&hook, id.take()) {
        hook.warm_finished(
            id,
            envelope,
            result.as_ref().ok().cloned(),
            warm_body_sha256,
        )
        .await
        .map_err(|error| broca_core::WarmFailure::Provider(Box::new(error)))?;
    }
    result
}

/// Retain reported usage even on an HTTP refusal. An absent usage object remains unknown,
/// rather than turning a failed warm into a measured zero-token call.
fn warm_billing_usage(
    body: &[u8],
    family: crate::spec::WireFamilyId,
    envelope: &mut AttemptEnvelope,
) {
    let value = serde_json::from_slice::<Value>(body).ok().or_else(|| {
        std::str::from_utf8(body)
            .ok()?
            .lines()
            .filter_map(|line| {
                let value =
                    serde_json::from_str::<Value>(line.strip_prefix("data:")?.trim()).ok()?;
                value
                    .get("response")
                    .cloned()
                    .filter(|response| response.get("usage").is_some())
            })
            .next_back()
    });
    let Some(value) = value else {
        return;
    };
    let mut evidence = crate::normalize::ResponseEvidence::default();
    evidence.observe_response(&value);
    if let Some(raw) = value.get("usage").filter(|usage| usage.is_object()) {
        let usage = match family {
            crate::spec::WireFamilyId::OpenAiResponses
            | crate::spec::WireFamilyId::OpenAiResponsesChatgpt => {
                crate::cache_warm::responses_response_usage(body)
            }
            _ => crate::cache_warm::anthropic_response_usage(body),
        };
        evidence.observe_usage(raw, usage);
    }
    evidence.finish_reason = value
        .get("stop_reason")
        .and_then(Value::as_str)
        .map(crate::normalize::map_stop_reason)
        .or_else(|| {
            value
                .pointer("/incomplete_details/reason")
                .and_then(Value::as_str)
                .map(crate::families::openai_responses::map_incomplete_reason)
        })
        .or_else(|| {
            value
                .get("status")
                .and_then(Value::as_str)
                .map(crate::families::openai_responses::map_response_status)
        });
    *envelope = AttemptEnvelope {
        sent: envelope.sent,
        sent_at_ms: envelope.sent_at_ms,
        finished_at_ms: envelope.finished_at_ms,
        response_bytes: envelope.response_bytes,
        http_status: envelope.http_status,
        outcome: envelope.outcome,
        ..response_envelope(evidence, envelope.vendor_request_id.take())
    };
}

/// Read a whole warm answer, up to its size limit.
async fn read_warm_body(
    response: reqwest::Response,
    failed: &impl Fn(String) -> broca_core::WarmFailure,
    envelope: &mut AttemptEnvelope,
    observer: Option<&dyn crate::quota::QuotaObserver>,
) -> Result<Vec<u8>, broca_core::WarmFailure> {
    let mut quota_sse = crate::quota::SseTap::new(observer);
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            envelope.outcome = BillingAttemptOutcome::ChunkError;
            failed(format!("warm answer unreadable: {error}"))
        })?;
        envelope.response_bytes += chunk.len() as u64;
        let received_at = quota_sse.as_ref().map(|_| now_millis());
        if body.len() + chunk.len() > MAX_WARM_RESPONSE_BYTES {
            envelope.outcome = BillingAttemptOutcome::ByteLimit;
            return Err(failed("warm answer exceeded its size limit".into()));
        }
        body.extend_from_slice(&chunk);
        if let Some(tap) = &mut quota_sse {
            tap.push(&body, received_at.expect("quota tap timestamp"));
        }
    }
    if let Some(tap) = &mut quota_sse {
        tap.finish(&body);
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
    journaled: bool,
) -> reqwest::RequestBuilder {
    let mut builder = client
        .for_model_host(anthropic_api_host, journaled)
        .post(url);
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
/// Maximum aggregate response bytes accepted during one provider attempt.
const MAX_STREAM_BYTES: usize = 256 * 1024 * 1024;
/// Deadline for establishing a provider connection; streaming requests have no total deadline.
const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Matches the external codex-rs client's 300-second semantic-progress limit. The same deadline covers response
/// headers and gaps between complete decoded records; raw bytes from an incomplete record do not
/// reset it. This is not a total stream deadline, so a long stream making real progress stays live.
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
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
// for an unavailable provider.
//
// Some failures are never re-sent at all, whatever the budget: permanent, auth and context
// overflow classes, `x-should-retry: false`, an exhausted account, a refused service tier, and a
// second deterministic decode failure.
//
// One exception adds a send: a 401 answered by a fresh token for the same credential is re-sent
// once, outside MAX_ATTEMPTS but inside RETRY_WALL_BUDGET.
// `attempt_policy` owns the counters and decisions; `drive_step_attempts` performs their effects.
//
// The two OpenAI Responses families (`openai_responses` and `openai_responses_chatgpt`) do not
// use this schedule. They take Codex's retry decision instead: see `codex_retry`. For them
// only connection failures share MAX_ATTEMPTS across both retry layers; there is no cap on
// the server's time or waiting budget. The permanent-class, `x-should-retry: false`,
// exhausted-account and second-decode-failure vetoes and the 401
// re-send apply to them unchanged.
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
    #[cfg(any(test, feature = "test-harness"))]
    virtual_waits: Option<Arc<std::sync::Mutex<Vec<Duration>>>>,
    #[cfg(test)]
    virtual_retry_attempts: Option<Arc<std::sync::Mutex<std::collections::VecDeque<Duration>>>>,
    #[cfg(test)]
    virtual_wait_overhead: Option<Duration>,
}

/// A point on a [`RetryClock`] from which retry time is measured.
#[derive(Clone, Copy)]
struct RetryMark {
    at: Instant,
    #[cfg(test)]
    virtual_at: Option<Instant>,
    #[cfg(any(test, feature = "test-harness"))]
    virtual_waits_before: usize,
}

impl RetryClock {
    fn attempt_elapsed(&self, mark: RetryMark) -> Duration {
        #[cfg(test)]
        if let Some(attempts) = &self.virtual_retry_attempts {
            let elapsed = attempts
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted attempt clock");
            attempt_test_hooks::advance(elapsed);
            return elapsed;
        }
        self.elapsed_since(mark)
    }
    fn mark(&self) -> RetryMark {
        RetryMark {
            at: Instant::now(),
            #[cfg(test)]
            virtual_at: attempt_test_hooks::virtual_now(),
            #[cfg(any(test, feature = "test-harness"))]
            virtual_waits_before: self
                .virtual_waits
                .as_ref()
                .map_or(0, |waits| waits.lock().unwrap().len()),
        }
    }

    fn elapsed_since(&self, mark: RetryMark) -> Duration {
        #[cfg(test)]
        if let Some(at) = mark.virtual_at {
            return attempt_test_hooks::deadline_now().duration_since(at);
        }
        let elapsed = mark.at.elapsed();
        #[cfg(test)]
        let elapsed = self.virtual_wait_overhead.unwrap_or(elapsed);
        #[cfg(any(test, feature = "test-harness"))]
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
        let delay = attempt_test_hooks::delay(delay);
        #[cfg(test)]
        attempt_test_hooks::record(attempt_test_hooks::Event::Wait(delay));
        // Keep the marker on the retry path even when no virtual waits were configured:
        // release admission must reject any linked test-harness transport.
        #[cfg(feature = "test-harness")]
        tracing::info!("BROCA_TEST_HARNESS_VIRTUAL_RETRY_WAITS_V1");
        #[cfg(any(test, feature = "test-harness"))]
        if let Some(waits) = &self.virtual_waits {
            waits.lock().unwrap().push(delay);
            #[cfg(test)]
            attempt_test_hooks::advance(delay);
            tokio::task::yield_now().await;
            return;
        }
        tokio::time::sleep(delay).await;
    }
}

/// Why a transient attempt failed. This tag stays inside the transport: public error classes do
/// not distinguish a broken stream from deterministic response decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttemptCause {
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

pub(crate) fn now_millis() -> u64 {
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
    route: broca_core::refusal_fallback::StepRoute,
    render_job: RenderJob<F>,
    served_account_ref: Option<String>,
    recovery: Option<AuthRecovery>,
    step_id: StepId,
    first_attempt: u32,
    tx: mpsc::Sender<NormalizedEvent>,
    cancel: CancellationToken,
    deadline: Option<Instant>,
    seal: Option<ShutdownSeal>,
    progress: Option<broca_core::provider::LiveStepProgress>,
    attempt_hook: Option<Arc<dyn broca_core::run::AttemptHook>>,
    retry_clock: RetryClock,
    websocket: Arc<responses_websocket::Session>,
    #[cfg(test)] escape_client_builds: std::sync::Arc<AtomicUsize>,
) -> JoinHandle<()> {
    // The credential this step's request is rendered with, kept so a 401 can be compared
    // against whatever the refresher serves next.
    let sent_auth = Arc::clone(&render_job.auth);
    // A passed deadline stops the step by cancelling a token of the step's own. The caller's
    // token is cancelled only by its owner: a run loop reads it as an explicit cancel, and a
    // step that ran out of time was not cancelled by anyone.
    let cancel = match deadline {
        Some(_) => cancel.child_token(),
        None => cancel,
    };
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
        let mut rendered = match rendered {
            Ok(Ok(rendered)) => rendered,
            Ok(Err(error)) => {
                let _ = tx
                    .send(NormalizedEvent::Control(ProviderControl::Error {
                        error: *error,
                        retries_used: 0,
                    }))
                    .await;
                return;
            }
            Err(join_error) => {
                // Render assertions, including tool-name alias invariants, now unwind through
                // the blocking handle. Keep that failure on the typed provider-error path instead
                // of closing the receiver as an unexplained missing assistant message.
                let error = ProviderError {
                    provider_details: None,
                    cause: None,
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
                    account_continue: None,
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

        if attempt_hook.is_some() {
            rendered.request.speed_requested = frozen.speed.clone();
        }
        rendered.request.attempt_hook = attempt_hook;

        let mut attempt = first_attempt;
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
                quota_observer: sent.quota_observer.clone(),
                cache_session: sent.cache_session.clone(),
                cache_model: sent.cache_model.clone(),
                cache_provenance: sent.cache_provenance.clone(),
                attempt_hook: sent.attempt_hook.clone(),
                ephemeral_session: sent.ephemeral_session,
                codex_wire: sent.codex_wire,
                auth_revision: sent.auth_revision,
                completed_output: sent.completed_output.clone(),
                url: fresh.url,
                headers: fresh.headers,
                body: sent.body.clone(),
                family: sent.family,
                subscription_auth: sent.subscription_auth,
                service_tier: sent.service_tier.clone(),
                session_address: sent.session_address.clone(),
                run_id: sent.run_id.clone(),
                credential_id: sent.credential_id.clone(),
                account_ref: sent.account_ref.clone(),
                wire_model_id: sent.wire_model_id.clone(),
                speed_requested: sent.speed_requested.clone(),
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
                    progress: progress.as_ref(),
                    seal: seal.as_ref(),
                    step_id,
                    cache_ttl_ms: crate::cache_warm::marker_ttl_ms(&frozen),
                    first_attempt,
                    route,
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
    let use_codex = codex_retry::applies(request.family);
    let mut attempts = AttemptState::new(use_codex);
    let mut use_escape_client = false;
    // Every attempt sends the same body, so its cache-evidence fingerprint is computed once.
    let step_fingerprint = cache_runtime::StepFingerprint::default();
    // `credential_refresh` is the evidence the next attempt's record carries when it goes out
    // with a token refreshed inside this step, by either the 401 path or the pre-send check.
    let mut credential_refresh: Option<CredentialRefresh> = None;
    let mut refreshed_after_rejection = false;
    // Driver-kept exceptions: the model has no cancellation, receiver-close, shutdown-seal
    // or top-of-loop deadline outcome. These remain quiet ends with zero retries. Journal
    // settlement errors also bypass retry policy: a failed durability effect cannot be retried
    // as a provider failure. Resending a rejected gzip body without compression stays inside
    // one recorded attempt.
    loop {
        let attempt_index = attempts.attempt_index();
        if let Some(recovery) = &auth_recovery {
            recovery
                .recovery
                .current
                .write()
                .unwrap_or_else(|poison| poison.into_inner())
                .definite_auth_failure = None;
        }
        let attempt_started = retry_clock.mark();
        if cancel.is_cancelled() || tx.is_closed() {
            return Ok((Vec::new(), 0));
        }
        if deadline.is_some_and(|deadline| attempt_deadline_now!() >= deadline) {
            cancel.cancel();
            return Ok((Vec::new(), 0));
        }
        // A token that expires before this request and its stream are done would be refused
        // with a 401, costing an attempt and telling the credential source it was rejected
        // when it merely ran out. Ask for a fresh one first. An unavailable read keeps the
        // held token and the 401 recovery below still covers it. A definite sign-in loss
        // stops before sending only when the held token has already expired.
        if let Some(recovery) = auth_recovery.as_mut() {
            let refreshed = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok((Vec::new(), 0)),
                _ = tx.closed() => return Ok((Vec::new(), 0)),
                refreshed = recovery.recovery.refresher.refresh_expiring() => refreshed,
            };
            let refreshed = match refreshed {
                crate::credential_refresh::ExpiringRefresh::Token(token) => Some(token),
                crate::credential_refresh::ExpiringRefresh::NeedsReauth { held_expires_at_ms }
                    if held_expires_at_ms
                        .and_then(|expiry| u64::try_from(expiry).ok())
                        .is_none_or(|expiry| clock.now_ms() >= expiry) =>
                {
                    return Err((
                        crate::credential_refresh::needs_reauth_error(),
                        attempt_index.saturating_sub(1) as u32,
                    ));
                }
                _ => None,
            };
            #[cfg(test)]
            attempt_test_hooks::record(attempt_test_hooks::Event::Refresh {
                rejected: false,
                credential: refreshed.is_some(),
            });
            if let Some(refreshed) = refreshed {
                recovery
                    .recovery
                    .current
                    .write()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .auth_refreshed = true;
                // The same account check as a 401 refresh: a token for another account is
                // never sent. Nothing was sent since the previous attempt, so the retries
                // used are those already spent. This pre-send exception is not a rejected-token
                // recovery in the model, and retains its separate attempt_index - 1 count.
                if let Some(mut error) = refreshed_account_changed_error(
                    recovery.recovery.frozen_account_ref.as_deref(),
                    &recovery.sent_auth,
                    &refreshed,
                ) {
                    redact_chatgpt_account_error(&mut error, request.family);
                    return Err((error, attempt_index.saturating_sub(1) as u32));
                }
                // The fresh token's record may list other hosts than the held one's did.
                if let Err(error) = refreshed.hosts.check(&refreshed.auth, &request.url) {
                    return Err((*error, attempt_index.saturating_sub(1) as u32));
                }
                // Compared as a boolean only; neither side is ever formatted.
                if refreshed.auth != *recovery.sent_auth || request.codex_wire {
                    recovery.adopt(
                        refreshed.auth,
                        refreshed.account_ref,
                        refreshed.hosts,
                        &mut request,
                        &mut served_account_ref,
                    );
                    credential_refresh = Some(refreshed.evidence);
                }
            }
        }
        let normalizer = Box::new(normalizer_factory()) as Box<dyn StreamNormalizer>;

        let escape_client = use_escape_client.then(|| {
            #[cfg(test)]
            escape_client_builds.fetch_add(1, Ordering::SeqCst);
            #[cfg(test)]
            attempt_test_hooks::record(attempt_test_hooks::Event::EscapeClient);
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
        let mut trace = AttemptTrace {
            sent_note: step_seal.sent_note(&request.body, request_digest),
            cache_work: cache_runtime::Pending::for_step(&step_fingerprint),
            attempt_hook: request.attempt_hook.clone(),
            quota_observer: request.quota_observer.clone(),
            auth_refreshed: request.attempt_hook.is_some()
                && auth_recovery.as_ref().is_some_and(|recovery| {
                    recovery
                        .recovery
                        .current
                        .read()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .auth_refreshed
                }),
            attempt_ordinal: attempt_index as u32 + step_seal.first_attempt,
            attempt_cancel: request.attempt_hook.as_ref().map(|_| cancel.clone()),
            send_index: 1,
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
        trace.cache_work.prepare(&request);
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
        // Date the failure before cache finalization and billing settlement can await.
        // ModelAttempt::ts_ms is assigned later, when the terminal record is constructed.
        if matches!(&stop, AttemptStop::Ended(result) if result.is_err()) {
            trace.account_observation.failure_received_at_ms = Some(clock.now_ms());
        }
        // Sample the network retry budget before joining diagnostics for the terminal record.
        let attempt_elapsed = retry_clock.attempt_elapsed(attempt_started);
        // A caller's cancel must not wait for request hashing or a store load.
        trace.cache_evidence = if matches!(stop, AttemptStop::Cancelled) {
            trace.cache_work.finish_without_waiting(
                &request,
                trace.sent_at_ms,
                trace.served_account_ref.clone(),
            )
        } else {
            trace
                .cache_work
                .finish(&request, trace.sent_at_ms, trace.served_account_ref.clone())
                .await
        };
        let result = match stop {
            AttemptStop::Cancelled => {
                if let Some(websocket) = websocket {
                    websocket.close();
                }
                if trace.attempt_hook.is_some() {
                    if let Err(error) = trace
                        .settle_send(request_digest, AttemptOutcome::Cancelled, None)
                        .await
                    {
                        step_seal.end_attempt();
                        return Err((error, attempt_index as u32));
                    }
                }
                // A caller cancel is cooperative: the run loop keeps reading until this task
                // ends, so the in-flight attempt is recorded before the run's Cancelled
                // terminal.
                publish_attempt(
                    tx,
                    trace.into_attempt(
                        attempt_index + step_seal.first_attempt as usize,
                        request_digest,
                        AttemptOutcome::Cancelled,
                    ),
                )
                .await;
                step_seal.end_attempt();
                return Ok((Vec::new(), 0));
            }
            AttemptStop::Sealed => {
                if let Some(websocket) = websocket {
                    websocket.close();
                }
                // The seal keeps a fallback if its bound expires before the journal replies.
                // Otherwise the billing outcome is queued before the seal takes the terminal.
                if trace.attempt_hook.is_some() {
                    let _ = trace
                        .settle_send(request_digest, AttemptOutcome::SealedInFlight, None)
                        .await;
                }
                // The seal, not the run loop, writes this record and then the run's
                // Interrupted terminal. Nothing is sent on the stream, and the stream stays
                // open until the aborted run task drops its receiver, so the run loop never
                // observes the stop.
                if trace.attempt_hook.is_some() {
                    step_seal.end_attempt();
                } else {
                    step_seal.hand_over(trace.into_attempt(
                        attempt_index + step_seal.first_attempt as usize,
                        request_digest,
                        AttemptOutcome::SealedInFlight,
                    ));
                }
                tx.closed().await;
                return Ok((Vec::new(), 0));
            }
            AttemptStop::Ended(result) => *result,
        };
        let outcome = match &result {
            // An empty completion means the receiver closed mid-stream: there is no one to
            // send a record to, and the attempt did not complete.
            Ok(completion) if completion.is_empty() => {
                if trace.attempt_hook.is_some() {
                    let _ = trace
                        .settle_send(request_digest, AttemptOutcome::Cancelled, None)
                        .await;
                }
                step_seal.end_attempt();
                return Ok((Vec::new(), 0));
            }
            Ok(completion) if tool_args_cut(completion) => AttemptOutcome::ToolArgsTrailingGarbage,
            Ok(_) => AttemptOutcome::Complete,
            Err(failure) => failure.outcome.clone(),
        };
        if trace.attempt_hook.is_some() {
            if let Err(error) = trace
                .settle_send(request_digest, outcome.clone(), None)
                .await
            {
                step_seal.end_attempt();
                return Err((error, attempt_index as u32));
            }
        }
        publish_attempt(
            tx,
            trace.clone().into_attempt(
                attempt_index + step_seal.first_attempt as usize,
                request_digest,
                outcome,
            ),
        )
        .await;
        // Only after the record is queued to the run loop: a seal waiting on this attempt
        // then proceeds knowing the record took the ordinary path and is not its to write.
        step_seal.end_attempt();
        match result {
            Ok(completion) => return Ok((completion, attempts.retries_used())),
            Err(mut failure) => {
                #[cfg(test)]
                attempt_effect_hooks::at(attempt_effect_hooks::Boundary::FailedAttempt);
                let facts = AttemptFacts {
                    error: &failure.error,
                    cause: failure.cause,
                    outcome: failure.outcome.clone(),
                    retry_forbidden: failure.retry_forbidden,
                    over_https: trace.transport.as_deref() != Some("websocket"),
                    attempt_elapsed,
                    got_response_bytes: trace.response_len != 0,
                    can_refresh: auth_recovery.is_some(),
                };
                let mut inputs = AttemptInputs {
                    until_deadline: || {
                        deadline.map(|deadline| {
                            deadline.saturating_duration_since(attempt_deadline_now!())
                        })
                    },
                    jitter: || {
                        if use_codex {
                            codex_retry::jitter()
                        } else {
                            retry_jitter()
                        }
                    },
                };
                let mut decision =
                    attempt_policy::decide_attempt(&mut attempts, &facts, &mut inputs);
                if let AttemptDecision::SwitchToHttps(pending) = decision {
                    // The configuration callback can change or block. Only the actual switch
                    // result authorizes fallback; the deadline source runs after that effect.
                    #[cfg(test)]
                    attempt_effect_hooks::at(attempt_effect_hooks::Boundary::Switch);
                    let switched = request.family
                        == Some(crate::spec::WireFamilyId::OpenAiResponsesChatgpt)
                        && websocket.is_some_and(|session| session.try_switch_to_https());
                    #[cfg(test)]
                    if switched {
                        attempt_test_hooks::record(attempt_test_hooks::Event::SwitchToHttps);
                    }
                    decision = attempt_policy::resolve_https_switch(
                        &mut attempts,
                        &facts,
                        pending,
                        switched,
                        inputs.until_deadline,
                    );
                }
                let recovering = matches!(decision, AttemptDecision::RefreshAndResend);
                // Why a refreshed credential may not be sent, which ends the step: another
                // account, or a host its record doesn't list.
                let mut refused_refresh_error = None;
                if recovering {
                    let recovery = auth_recovery
                        .as_mut()
                        .expect("the policy requires a refresher before requesting recovery");
                    let refresh_started = retry_clock.mark();
                    let refreshed = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok((Vec::new(), 0)),
                        _ = tx.closed() => return Ok((Vec::new(), 0)),
                        refreshed = recovery.recovery.refresher.refresh_rejected(
                            failure.error.status.unwrap_or(401)
                        ) => refreshed,
                    };
                    let needs_reauth = matches!(
                        &refreshed,
                        crate::credential_refresh::RejectedRefresh::NeedsReauth
                    );
                    let refreshed = refreshed.into_token();
                    #[cfg(test)]
                    attempt_test_hooks::record(attempt_test_hooks::Event::Refresh {
                        rejected: true,
                        credential: refreshed.is_some(),
                    });
                    if needs_reauth {
                        recovery
                            .recovery
                            .current
                            .write()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .definite_auth_failure = Some((
                            step_seal.step_id,
                            step_seal.first_attempt.saturating_add(attempt_index as u32),
                        ));
                    }
                    let refresh_result = match refreshed.as_ref() {
                        None => RejectedRefreshResult::NoCredential,
                        Some(refreshed) => {
                            recovery
                                .recovery
                                .current
                                .write()
                                .unwrap_or_else(|poison| poison.into_inner())
                                .auth_refreshed = true;
                            refused_refresh_error = refreshed_account_changed_error(
                                recovery.recovery.frozen_account_ref.as_deref(),
                                &recovery.sent_auth,
                                refreshed,
                            )
                            .map(|mut error| {
                                redact_chatgpt_account_error(&mut error, request.family);
                                error
                            });
                            let account_changed = refused_refresh_error.is_some();
                            if !account_changed {
                                refused_refresh_error = refreshed
                                    .hosts
                                    .check(&refreshed.auth, &request.url)
                                    .err()
                                    .map(|error| *error);
                            }
                            // Compare accounts before credentials; neither credential is formatted.
                            if account_changed {
                                RejectedRefreshResult::AccountChanged
                            } else if refused_refresh_error.is_some() {
                                RejectedRefreshResult::HostNotAllowed
                            } else if refreshed.auth == *recovery.sent_auth {
                                RejectedRefreshResult::UnchangedCredential
                            } else {
                                RejectedRefreshResult::Refreshed
                            }
                        }
                    };
                    decision = attempt_policy::decide_rejected_refresh(
                        &mut attempts,
                        &facts,
                        refresh_result,
                    );
                    if matches!(decision, AttemptDecision::Resend { .. }) {
                        refreshed_after_rejection = true;
                        let refreshed =
                            refreshed.expect("a resend requires a refreshed credential");
                        recovery.adopt(
                            refreshed.auth,
                            refreshed.account_ref,
                            refreshed.hosts,
                            &mut request,
                            &mut served_account_ref,
                        );
                        credential_refresh = Some(refreshed.evidence);
                        attempt_policy::note_refresh_elapsed(
                            &mut attempts,
                            retry_clock.elapsed_since(refresh_started),
                        );
                    }
                }

                let switch_to_https = match &decision {
                    AttemptDecision::Resend {
                        switch_to_https, ..
                    }
                    | AttemptDecision::EndStep {
                        switch_to_https, ..
                    } => *switch_to_https,
                    AttemptDecision::RefreshAndResend | AttemptDecision::SwitchToHttps(_) => {
                        unreachable!("pending effects were resolved above")
                    }
                };
                match decision {
                    AttemptDecision::EndStep {
                        reason,
                        retries_used,
                        note_unrecovered_rejection,
                        ..
                    } => {
                        if refreshed_after_rejection
                            && failure.error.class == ErrorClass::Auth
                            && matches!(failure.error.status, Some(401 | 403))
                        {
                            if let Some(recovery) = &auth_recovery {
                                recovery
                                    .recovery
                                    .current
                                    .write()
                                    .unwrap_or_else(|poison| poison.into_inner())
                                    .definite_auth_failure = Some((
                                    step_seal.step_id,
                                    step_seal.first_attempt.saturating_add(attempt_index as u32),
                                ));
                            }
                        }
                        if note_unrecovered_rejection {
                            auth_recovery
                                .as_ref()
                                .expect("the policy requires a refresher to report a rejection")
                                .recovery
                                .refresher
                                .note_unrecovered_rejection(failure.error.status.unwrap_or(401));
                        }
                        let (decision, warning_reason) = match reason {
                            EndReason::Authentication => ("not_retryable", "authentication"),
                            EndReason::AccountChanged => ("not_retryable", "account_changed"),
                            EndReason::CredentialHostNotAllowed => {
                                ("not_retryable", "credential_host_not_allowed")
                            }
                            EndReason::UnchangedCredential => {
                                ("not_retryable", "unchanged_credential")
                            }
                            EndReason::RetryVeto => ("not_retryable", "retry_veto"),
                            EndReason::AttemptLimit => ("attempts_exhausted", "attempt_limit"),
                            EndReason::WaitingBudget => ("wall_exhausted", "waiting_budget"),
                            EndReason::RetryAfterExceedsDeadline => {
                                ("not_retryable", "retry_after_exceeds_deadline")
                            }
                            EndReason::CodexTerminal => ("not_retryable", "codex_terminal"),
                            EndReason::NoServerRetryTime => {
                                ("not_retryable", "no_server_retry_time")
                            }
                            EndReason::ConnectionAttemptLimit => {
                                ("attempts_exhausted", "connection_attempt_limit")
                            }
                        };
                        warn_attempt_retry(&failure, &trace, decision, warning_reason);
                        if matches!(
                            reason,
                            EndReason::AccountChanged | EndReason::CredentialHostNotAllowed
                        ) {
                            let error = refused_refresh_error
                                .expect("a refused refresh end requires the replacement error");
                            return Err((error, retries_used));
                        }
                        if reason == EndReason::ConnectionAttemptLimit {
                            failure.error.class = ErrorClass::Transient;
                            failure.error.provider_code = Some("connection_failed".into());
                            failure.error.message = format!(
                                "connection failed after {MAX_ATTEMPTS} failed attempts: {}",
                                failure.error.message
                            );
                        }
                        return Err((failure.error, retries_used));
                    }
                    AttemptDecision::Resend {
                        delay,
                        escape_client,
                        reason,
                        ..
                    } => {
                        use_escape_client = escape_client;
                        warn_attempt_retry(
                            &failure,
                            &trace,
                            if switch_to_https {
                                "switching_to_https"
                            } else {
                                "retrying"
                            },
                            reason,
                        );
                        // A rejected-token resend resets the display without entering the sleep
                        // effect, unlike an ordinary retry whose server-advised delay is zero.
                        if !recovering {
                            let wait_started = retry_clock.mark();
                            let mut wait = retry_wait::Wait::new(
                                &step_seal,
                                request.run_id.as_deref(),
                                &failure.error,
                                delay,
                                step_seal
                                    .first_attempt
                                    .saturating_add(attempt_index as u32)
                                    .saturating_add(2),
                            );
                            if delay >= Duration::from_secs(1) {
                                if let Some(wait) = &mut wait {
                                    wait.publish(tx).await;
                                }
                            }
                            tokio::select! {
                                biased;
                                _ = cancel.cancelled() => return Ok((Vec::new(), 0)),
                                _ = tx.closed() => return Ok((Vec::new(), 0)),
                                // No attempt is in flight; the seal owns the terminal record.
                                _ = step_seal.tripped() => {
                                    tx.closed().await;
                                    return Ok((Vec::new(), 0));
                                }
                                _ = async {
                                    let sleep = retry_clock.sleep(delay);
                                    tokio::pin!(sleep);
                                    if delay < Duration::from_secs(1) && wait.is_some() {
                                        tokio::select! {
                                            _ = &mut sleep => return,
                                            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                                                if let Some(wait) = &mut wait { wait.publish(tx).await; }
                                            }
                                        }
                                    }
                                    sleep.await;
                                } => {}
                            }
                            if let Some(wait) = wait {
                                wait.finish(tx).await;
                            }
                            attempt_policy::note_waited(
                                &mut attempts,
                                retry_clock.elapsed_since(wait_started),
                            );
                        }
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => return Ok((Vec::new(), 0)),
                            result = tx.send(NormalizedEvent::Display(DisplayEvent::DisplayReset)) => {
                                if result.is_err() {
                                    return Ok((Vec::new(), 0));
                                }
                            }
                        }
                    }
                    AttemptDecision::RefreshAndResend | AttemptDecision::SwitchToHttps(_) => {
                        unreachable!("pending effects were resolved above")
                    }
                }
            }
        }
    }
}

/// Whether a failed attempt means the provider rejected the token itself: an Auth-class HTTP
/// 401, including a ChatGPT socket upgrade refused with 401. A 403, on HTTPS or on a socket
/// upgrade, is not: an HTTPS 403 is an entitlement refusal, and a refused upgrade 403 is sent
/// again over HTTPS before it is classified. A ChatGPT backend 401 about its own upstream API key
/// is classified Transient before it gets here, so it does not qualify either.
#[cfg(test)]
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
    Some(ProviderError { provider_details: None, cause: None, provider_subcode: None,
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
    affordable_max_tokens: None, account_continue: None, })
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
    /// Terminal evidence for one step attempt, cleared before every step and attempt.
    /// It is queried only at the run loop's signed-out gate and is never serialized.
    definite_auth_failure: Option<(StepId, u32)>,
    auth_refreshed: bool,
    /// Local to this provider/run. Even a refresh returning the same header bytes starts
    /// a new socket authentication revision; it never changes the canonical request body.
    revision: u64,
    auth: Arc<AuthHandle>,
    account_ref: Option<String>,
    /// The hosts the vault listed with `auth`; they move with it on every refresh.
    hosts: CredentialHosts,
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
        hosts: CredentialHosts,
        request: &mut PreparedHttpRequest,
        served_account_ref: &mut Option<String>,
    ) {
        *request = (self.rebuild)(&auth, request);
        served_account_ref.clone_from(&account_ref);
        let auth = Arc::new(auth);
        self.sent_auth = Arc::clone(&auth);
        let mut current = self
            .recovery
            .current
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        request.auth_revision = current.revision.wrapping_add(1);
        *current = ServedAuth {
            definite_auth_failure: None,
            auth_refreshed: true,
            auth,
            account_ref,
            hosts,
            revision: request.auth_revision,
        };
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

/// What an attempt tells the step's shutdown seal once its request has reached a server (see
/// [`StepSeal::sent_note`]). A connection that never opened put no bytes anywhere: it refreshed
/// no provider cache, so it must not restart the session's cache clock.
#[derive(Clone)]
struct SentNote {
    seal: ShutdownSeal,
    step_id: StepId,
    body: Arc<dyn AsRef<[u8]> + Send + Sync>,
    sha256: String,
    route: broca_core::refusal_fallback::StepRoute,
    cache_ttl_ms: Option<u64>,
}

impl SentNote {
    fn note(&self, sent_at_ms: u64) {
        self.seal.note_sent(
            broca_core::SentRequest {
                step_id: self.step_id,
                body: Arc::clone(&self.body),
                sha256: self.sha256.clone(),
                sent_at_ms,
                route: self.route,
            },
            self.cache_ttl_ms,
        );
    }
}

/// The shutdown seal for one step, if the caller supplied one, plus the step id the seal's
/// record needs. Without a seal every method is a no-op and `tripped` never resolves.
#[derive(Clone, Copy)]
struct StepSeal<'a> {
    progress: Option<&'a broca_core::provider::LiveStepProgress>,
    seal: Option<&'a ShutdownSeal>,
    step_id: StepId,
    /// The prompt-cache lifetime the frozen config's markers declare, which each send restarts.
    cache_ttl_ms: Option<u64>,
    /// The ordinal this call's first attempt is recorded with; see `StepRuntime::first_attempt`.
    first_attempt: u32,
    /// The model the step is sent to, which a warm of its bytes must address too.
    route: broca_core::refusal_fallback::StepRoute,
}

impl StepSeal<'_> {
    fn begin_attempt(&self) -> bool {
        self.seal.is_none_or(ShutdownSeal::begin_attempt)
    }

    /// The note an attempt gives the seal once its bytes have reached a server: the exact
    /// bytes, so a cache warm during the run re-sends them, and when they went, which restarts
    /// the session's cache clock.
    fn sent_note(&self, body: &Bytes, digest: &RequestDigest) -> Option<SentNote> {
        self.seal.map(|seal| SentNote {
            seal: seal.clone(),
            step_id: self.step_id,
            body: Arc::new(body.clone()),
            sha256: digest.sha256.clone(),
            route: self.route,
            cache_ttl_ms: self.cache_ttl_ms,
        })
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

#[cfg(test)]
fn retry_delay(error: &ProviderError, failed_attempt_index: usize) -> Duration {
    attempt_policy::generic_delay(error, failed_attempt_index, retry_jitter)
}

fn retry_jitter() -> f64 {
    #[cfg(test)]
    attempt_effect_hooks::at(attempt_effect_hooks::Boundary::Jitter);
    #[cfg(test)]
    if let Some(factor) = attempt_test_hooks::jitter(false) {
        return factor;
    }
    rand::rng().random_range((1.0 - BACKOFF_JITTER)..=(1.0 + BACKOFF_JITTER))
}

fn warn_attempt_retry(
    failure: &AttemptFailure,
    trace: &AttemptTrace,
    decision: &str,
    reason: &str,
) {
    let gaps = trace.content_gaps.snapshot();
    #[cfg(test)]
    attempt_test_hooks::record(attempt_test_hooks::Event::Decision {
        action: decision.to_owned(),
        reason: reason.to_owned(),
    });
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
            warn_private_failure(&failure.error, trace, "model attempt ended chunk_error");
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
                max_content_gap_ms = gaps.max_content_gap_ms,
                first_content_ms = gaps.first_content_ms,
                tail_gap_ms = gaps.tail_gap_ms,
                max_gap_after_event = gaps.max_gap_after_event,
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
    let gaps = trace.content_gaps.snapshot();
    tracing::warn!(target: "provider",
        outcome = ?failure.outcome,
        class = ?failure.error.class,
        transport = trace.transport.as_deref().unwrap_or("https"),
        max_content_gap_ms = gaps.max_content_gap_ms,
        first_content_ms = gaps.first_content_ms,
        tail_gap_ms = gaps.tail_gap_ms,
        max_gap_after_event = if trace.ephemeral_session { "private" } else { gaps.max_gap_after_event },
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
    #[cfg(test)]
    attempt_test_hooks::record(attempt_test_hooks::Event::Attempt {
        ordinal: attempt.attempt,
        outcome: attempt.outcome.clone(),
    });
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
    normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
    header_timeout: Duration,
    trace: &mut AttemptTrace,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    let mut observation = SseObservationGuard::begin(trace);
    let result = drive_http_attempt(
        client,
        request,
        clock,
        normalizer,
        tx,
        header_timeout,
        observation.trace,
    )
    .await;
    observation.failed = result.is_err();
    result
}

async fn drive_http_attempt(
    client: &ProviderHttpClient,
    request: &PreparedHttpRequest,
    clock: &Clock,
    mut normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
    header_timeout: Duration,
    trace: &mut AttemptTrace,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    trace.ephemeral_session = request.ephemeral_session;
    if trace.attempt_hook.is_some() {
        let evidence = trace.response_evidence.get_or_insert_with(Default::default);
        normalizer.set_response_evidence(evidence.clone());
    }
    if request.ephemeral_session {
        normalizer.private_error_diagnostics();
    }
    trace.transport = Some("https".into());
    let anthropic_api_host = request_gzip::is_anthropic_api_host(&request.url);
    let gzip = encode_request_gzip(client, anthropic_api_host, &request.body).await;
    // Legacy telemetry treats the uncompressed retry as the attempt. The billing hook instead
    // settles each physical send, preserving the compressed refusal before resetting the trace.
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
            content_gaps::log_sse_attempt(trace, true);
            if trace.attempt_hook.is_some() {
                trace.http_status = Some(status);
                trace.cache_evidence = trace
                    .cache_work
                    .finish(request, trace.sent_at_ms, trace.served_account_ref.clone())
                    .await;
                trace
                    .settle_send(
                        &RequestDigest::of(&request.body),
                        AttemptOutcome::HttpError,
                        Some(BillingAttemptOutcome::CompressionRefused),
                    )
                    .await
                    .map_err(|error| {
                        AttemptFailure::transport_without_reissue(
                            error,
                            AttemptOutcome::ProviderError,
                        )
                    })?;
            }
            *trace = unsent_trace;
            trace.cache_work.prepare(request);
            trace.send_index = 2;
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
        let retry_after_secs = if codex_retry::applies(request.family) {
            codex_retry::retry_after_secs(resp.headers())
        } else {
            parse_retry_after_header(resp.headers())
        };
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
        if trace.attempt_hook.is_some() {
            if let Ok(response) = serde_json::from_str::<Value>(&text) {
                normalizer.observe_error_response(&response);
            }
        }
        log_opencode_go_refusal(&request.headers, code, &headers, &text);
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
        let observed_at_ms = clock.now_ms();
        let error = match request.family {
            Some(family) => {
                let error = classify_chatgpt_upstream_key_rejection(family, code, &text, error);
                classify_account_limit_exhausted_with_auth(
                    AccountLimitContext {
                        family,
                        subscription_auth: request.subscription_auth,
                    },
                    code,
                    &headers,
                    &text,
                    UNIX_EPOCH + Duration::from_millis(observed_at_ms),
                    error,
                )
            }
            None => error,
        };
        let error = opencode_payment_required(&request.url, code, error);
        let error = inception_payment_required(&request.url, code, error);
        if code == 429
            && matches!(
                request.family,
                Some(
                    crate::spec::WireFamilyId::Gemini | crate::spec::WireFamilyId::GoogleCodeAssist
                )
            )
            && error.is_account_limit_exhausted()
        {
            tracing::warn!(
                target: "provider",
                session_address = request.session_address.as_deref().unwrap_or("unknown"),
                run_id = request.run_id.as_deref().unwrap_or("unknown"),
                credential_id = request.credential_id.as_deref().unwrap_or("unknown"),
                account_ref = request.account_ref.as_deref().unwrap_or("unknown"),
                wire_model_id = request.wire_model_id.as_deref().unwrap_or("unknown"),
                request_body_len = request.body.len(),
                provider_details = ?error.provider_details,
                "Google provider quota refusal"
            );
        }
        // An exhausted account refills in hours or days. Re-sending within the step cannot
        // succeed, and honoring the wait would park the run far past any step budget.
        let retry_forbidden = retry_forbidden
            || error.is_account_limit_exhausted()
            || error.is_billing_exhausted()
            || error.is_credit_insufficient();
        if error.is_account_limit_exhausted() {
            trace.account_observation.limit = Some(broca_protocol::AttemptAccountLimit {
                disposition: broca_protocol::provider::AccountLimitDisposition::ACCOUNT,
                observed_at_ms,
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
        trace.attempt_hook.is_some(),
    );
    let compressed = gzip.is_some();
    let body = gzip.unwrap_or_else(|| {
        if request.codex_wire {
            Bytes::from(
                crate::chatgpt_wire::request_bytes(&request.body)
                    .expect("a rendered Responses body is a JSON object"),
            )
        } else {
            request.body.clone()
        }
    });
    if trace.attempt_hook.is_some() {
        trace.reserve_send(request, body.len(), compressed).await?;
    }
    #[cfg(test)]
    attempt_test_hooks::send(trace.attempt_ordinal, trace.send_index, false);
    // A deterministic connect refusal still reserves an attempt, but never stamps a send.
    #[cfg(test)]
    if client
        .fail_connects
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            left.checked_sub(1)
        })
        .is_ok()
    {
        return Err(AttemptFailure::transport(
            transport_error("refused test connection (no socket opened)"),
            AttemptOutcome::ConnectFailed,
        ));
    }
    // Stamped as the request is handed to the client: a provider's prompt cache counts its
    // lifetime from the request, and after a restart this stamp is what dates it.
    trace.sent_at_ms = Some(now_millis());
    trace.sent_at = Some(std::time::Instant::now());
    trace.content_gaps.start();
    let response = tokio::time::timeout(header_timeout, builder.body(body).send()).await;
    // A connect error put no bytes on any server; anything else may have reached one.
    if !matches!(&response, Ok(Err(error)) if error.is_connect()) {
        trace.note_reached_server();
    }
    trace.cache_work.start(request);
    let response = response
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
                if trace.attempt_hook.is_some() {
                    trace.sent_at_ms = None;
                    trace.sent_at = None;
                    trace.content_gaps = ContentGaps::default();
                }
                AttemptOutcome::ConnectFailed
            } else {
                AttemptOutcome::NoStatus
            };
            AttemptFailure::transport(transport_error(&e.to_string()), outcome)
        })?;
    if trace.attempt_hook.is_some() {
        trace.vendor_request_id = vendor_request_id(response.headers());
    }
    if let Some(observer) = &trace.quota_observer {
        observer.headers(response.headers(), now_millis());
    }
    Ok(response)
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
            #[cfg(test)]
            if attempt_test_hooks::is_active() && !websocket.forced_https() {
                attempt_test_hooks::send(trace.attempt_ordinal, trace.send_index, true);
            }
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

fn vendor_request_id(headers: &reqwest::header::HeaderMap) -> Option<String> {
    // Response object ids and client-generated correlation ids are not vendor request ids.
    [
        "request-id",
        "x-request-id",
        "x-amzn-requestid",
        "x-goog-request-id",
    ]
    .iter()
    .find_map(|name| {
        headers
            .get(*name)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
    })
    .map(str::to_owned)
}

fn observe_response_finish(
    evidence: &Option<crate::normalize::SharedResponseEvidence>,
    events: &[NormalizedEvent],
) {
    if let Some(evidence) = evidence {
        for event in events {
            if let NormalizedEvent::Control(ProviderControl::StepFinished {
                service_tier_used,
                speed_used,
                served_model,
                ..
            }) = event
            {
                let mut evidence = evidence.lock().unwrap();
                if let Some(model) = served_model {
                    evidence.observe_served_model(model);
                }
                if service_tier_used.is_some() {
                    evidence.service_tier_used = service_tier_used.clone();
                }
                if speed_used.is_some() {
                    evidence.speed_used = speed_used.clone();
                }
            }
        }
    }
}

fn response_envelope(
    evidence: crate::normalize::ResponseEvidence,
    vendor_request_id: Option<String>,
) -> AttemptEnvelope {
    use broca_records::record::{AttemptEnvelope, AttemptModelUsage};
    fn model_row<'a>(
        models: &'a mut Vec<AttemptModelUsage>,
        model: &str,
    ) -> &'a mut AttemptModelUsage {
        let index = models
            .iter()
            .position(|row| row.model_used == model)
            .unwrap_or_else(|| {
                models.push(AttemptModelUsage {
                    model_used: model.to_owned(),
                    usage: Default::default(),
                    cache_write_5m_tokens: None,
                    cache_write_1h_tokens: None,
                });
                models.len() - 1
            });
        &mut models[index]
    }
    let mut models = Vec::new();
    for model in evidence.handoff_models {
        model_row(&mut models, &model);
    }
    let mut envelope = AttemptEnvelope {
        usage: evidence.usage,
        model_used: evidence.model_used,
        finish_reason: evidence.finish_reason,
        service_tier_used: evidence.service_tier_used,
        speed_used: evidence.speed_used,
        vendor_request_id,
        usage_raw: evidence.usage_raw,
        ..Default::default()
    };
    let split = |raw: &Value, key: &str| {
        raw.get("cache_creation")
            .and_then(|creation| creation.get(key))
            .and_then(Value::as_u64)
    };
    let mut tagged_iterations = false;
    let mut unattributed_iterations = false;
    if let Some(raw) = &envelope.usage_raw {
        envelope.cache_write_5m_tokens = split(raw, "ephemeral_5m_input_tokens");
        envelope.cache_write_1h_tokens = split(raw, "ephemeral_1h_input_tokens");
        if let Some(iterations) = raw.get("iterations").and_then(Value::as_array) {
            for entry in iterations {
                let Some(model) = entry
                    .get("model")
                    .and_then(Value::as_str)
                    .filter(|model| !model.is_empty())
                else {
                    unattributed_iterations = true;
                    continue;
                };
                tagged_iterations = true;
                let counters = |key: &str| entry.get(key).and_then(Value::as_u64);
                let usage = broca_protocol::Usage {
                    input_tokens: counters("input_tokens"),
                    cached_input_tokens: counters("cache_read_input_tokens"),
                    cache_write_tokens: counters("cache_creation_input_tokens"),
                    cache_write_5m_tokens: split(entry, "ephemeral_5m_input_tokens"),
                    cache_write_1h_tokens: split(entry, "ephemeral_1h_input_tokens"),
                    output_tokens: counters("output_tokens"),
                    reasoning_tokens: entry
                        .get("output_tokens_details")
                        .and_then(|details| details.get("thinking_tokens"))
                        .and_then(Value::as_u64),
                }
                .with_complete_cache_write_lifetimes();
                let model = model_row(&mut models, model);
                model.usage += usage;
                model.cache_write_5m_tokens = broca_protocol::usage::sum_reported(
                    model.cache_write_5m_tokens,
                    split(entry, "ephemeral_5m_input_tokens"),
                );
                model.cache_write_1h_tokens = broca_protocol::usage::sum_reported(
                    model.cache_write_1h_tokens,
                    split(entry, "ephemeral_1h_input_tokens"),
                );
            }
        }
    }
    if models.len() > 1 {
        // A handoff proves which models served, not their shares. Keep only tagged
        // counters on named rows; unsplit/untagged counters remain solely in usage_raw.
        envelope.model_used = None;
        envelope.usage = None;
        envelope.cache_write_5m_tokens = None;
        envelope.cache_write_1h_tokens = None;
        envelope.usage_by_model = Some(models);
    } else if unattributed_iterations {
        // A single known name cannot stand in for an iteration with no model tag.
        envelope.model_used = None;
        envelope.usage = None;
        envelope.cache_write_5m_tokens = None;
        envelope.cache_write_1h_tokens = None;
    } else if tagged_iterations {
        if let Some(model) = models.pop() {
            envelope.model_used = Some(model.model_used);
            envelope.usage = Some(model.usage);
            envelope.cache_write_5m_tokens = model.cache_write_5m_tokens;
            envelope.cache_write_1h_tokens = model.cache_write_1h_tokens;
        }
    }
    if envelope
        .usage
        .is_some_and(|usage| usage.reasoning_tokens.is_some())
        || envelope.usage_by_model.as_ref().is_some_and(|models| {
            models
                .iter()
                .any(|model| model.usage.reasoning_tokens.is_some())
        })
    {
        // Canonical usage includes reasoning in output even when the vendor reports
        // reasoning separately from its visible-output counter.
        envelope.reasoning_included_in_output = Some(true);
    }
    envelope
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
    if trace.attempt_hook.is_some() {
        let evidence = trace.response_evidence.get_or_insert_with(Default::default);
        normalizer.set_response_evidence(evidence.clone());
    }
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
    let content_limit = normalizer
        .content_deadline()
        .unwrap_or(STREAM_CONTENT_TIMEOUT);
    let content = tokio::time::sleep(content_limit);
    tokio::pin!(content);
    let mut completion = Vec::new();
    let mut rate_window = stream_floor_from_env().map(StreamRateWindow::new);
    // Socket frames are tapped by the receive pump. SSE borrows the normalizer's decoded
    // payloads so ordinary deltas incur no second decoder or per-record allocation.
    if let Some(observer) = trace.quota_observer.as_ref().filter(|observer| {
        observer.wants_frames() && trace.transport.as_deref() != Some("websocket")
    }) {
        let observer = observer.clone();
        normalizer.set_raw_sse_observer(Arc::new(move |frame| {
            crate::quota::observe_frame(observer.as_ref(), frame);
        }));
    }

    loop {
        let next = tokio::select! {
            biased;
            _ = &mut idle => {
                // A body left open after the step's last record has nothing more to give.
                if step_closed(normalizer.as_ref(), &completion) {
                    break;
                }
                return Err(AttemptFailure::transport(
                    stream_timeout_error(if split_deadlines {
                        "waiting for any complete SSE record"
                    } else {
                        "waiting for a complete event"
                    }),
                    AttemptOutcome::IdleTimeout,
                ));
            }
            _ = &mut content, if split_deadlines => return Err(AttemptFailure::transport(
                content_timeout_error(content_limit),
                AttemptOutcome::ContentTimeout,
            )),
            next = stream.next() => next,
        };
        let Some(chunk) = next else { break };
        // The step's last record already arrived (a proxy reset the body before its clean end,
        // say): nothing was lost, and asking the model again would bill a second answer.
        if chunk.is_err() && step_closed(normalizer.as_ref(), &completion) {
            break;
        }
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
        if let Some(observation) = trace
            .websocket_observation
            .upgrade()
            .filter(|_| trace.transport.as_deref() == Some("websocket"))
        {
            observation.lock().unwrap().observe_text(chunk);
        }
        if chunk.len() > max_stream_bytes.saturating_sub(stream_bytes) {
            return Err(AttemptFailure::decode(
                stream_limit_error(&format!(
                    "provider stream exceeds the {max_stream_bytes}-byte per-attempt limit"
                )),
                AttemptOutcome::ByteLimit,
            ));
        }
        stream_bytes += chunk.len();
        let websocket = trace.transport.as_deref() == Some("websocket");
        if websocket {
            if let Some(error) = responses_websocket::wrapped_error(chunk, trace) {
                trace.content_gaps.observe(Some("error"));
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
        if normalizer.progress_marker() != progress_marker {
            trace.content_gaps.observe(normalizer.last_event_type());
        }
        let progress = normalizer.content_progress();
        if progress.content_bytes != trace.content_progress.content_bytes {
            trace.last_content_elapsed = trace.elapsed_since_send();
        }
        *trace.content_progress = progress;
        let events = result
            .map_err(|error| AttemptFailure::normalize(error, AttemptOutcome::DecodeError))?;
        observe_response_finish(&trace.response_evidence, &events);
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
            content.as_mut().reset(now + content_limit);
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
        if !record_attempt_events(events, &mut completion, tx, trace).await? {
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

    // `finish` runs only at a clean end of body. A grammar that cannot close the step from
    // what arrived emits nothing here, and the completion check below reports the cut as a
    // transport fault, the same `eof_before_completion` for every wire family; `finish`
    // fails only for what was received but does not decode.
    let result = normalizer.finish();
    if normalizer.progress_marker() != progress_marker {
        trace.content_gaps.observe(normalizer.last_event_type());
    }
    let progress = normalizer.content_progress();
    if progress.content_bytes != trace.content_progress.content_bytes {
        trace.last_content_elapsed = trace.elapsed_since_send();
    }
    *trace.content_progress = progress;
    let final_events = result
        .map_err(|error| AttemptFailure::normalize(error, AttemptOutcome::EofBeforeCompletion))?;
    observe_response_finish(&trace.response_evidence, &final_events);
    if !record_attempt_events(final_events, &mut completion, tx, trace).await? {
        return Ok(Vec::new());
    }
    validate_attempt_completion(completion)
}

async fn record_attempt_events(
    events: Vec<NormalizedEvent>,
    completion: &mut Vec<NormalizedEvent>,
    tx: &mpsc::Sender<NormalizedEvent>,
    trace: &mut AttemptTrace,
) -> Result<bool, AttemptFailure> {
    for event in events {
        match event {
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                return Err(in_band_failure(error, trace));
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

/// The failure for an in-band error event on a 2xx stream. An account wall reported in-band is
/// the same wall an HTTP refusal reports: the account refills only at its reset, so the step
/// ends without re-sending, and the attempt records the wall for the pause and routing decisions
/// exactly as a refused HTTP attempt does.
fn in_band_failure(error: ProviderError, trace: &mut AttemptTrace) -> AttemptFailure {
    let account_wall = error.is_account_limit_exhausted();
    if account_wall {
        trace.account_observation.limit = Some(broca_protocol::AttemptAccountLimit {
            disposition: broca_protocol::provider::AccountLimitDisposition::ACCOUNT,
            observed_at_ms: now_millis(),
            resets_at_ms: error.resets_at_ms,
            resets_at_source: error.resets_at_source.clone(),
            limit_window: error.limit_window.clone(),
        });
    }
    AttemptFailure {
        retry_forbidden: account_wall,
        ..AttemptFailure::control(error)
    }
}

fn stream_timeout_error(activity: &str) -> ProviderError {
    ProviderError {
        provider_details: None,
        cause: None,
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
        account_continue: None,
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
        provider_details: None,
        cause: None,
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
        account_continue: None,
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
        provider_details: None,
        cause: None,
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
        account_continue: None,
    }
}

fn transport_error(message: &str) -> ProviderError {
    ProviderError {
        provider_details: None,
        cause: None,
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
        account_continue: None,
    }
}

fn stream_limit_error(message: &str) -> ProviderError {
    ProviderError {
        provider_details: None,
        cause: None,
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
        account_continue: None,
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
/// the captured status, type, code and complete message. Only its single masked key token may
/// vary. A different 401 keeps its ordinary classification rather than borrowing this exception.
fn classify_chatgpt_upstream_key_rejection(
    family: crate::spec::WireFamilyId,
    status: u16,
    body: &str,
    error: ProviderError,
) -> ProviderError {
    if family != crate::spec::WireFamilyId::OpenAiResponsesChatgpt || status != 401 {
        return error;
    }
    let Some(detail) = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|body| body.get("error").cloned())
    else {
        return error;
    };
    if detail.get("type").and_then(Value::as_str) != Some("invalid_request_error")
        || detail.get("code").and_then(Value::as_str) != Some("invalid_api_key")
    {
        return error;
    }
    let key = detail
        .get("message")
        .and_then(Value::as_str)
        .and_then(|message| message.strip_prefix("Incorrect API key provided: sk-"))
        .and_then(|tail| {
            tail.strip_suffix(
                ". You can find your API key at https://platform.openai.com/account/api-keys.",
            )
        });
    if !key.is_some_and(|key| {
        !key.is_empty()
            && key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'*'))
    }) {
        return error;
    }
    ProviderError { provider_details: None, cause: None, provider_subcode: None,
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
    affordable_max_tokens: None, account_continue: None, }
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
    classify_account_limit_exhausted_with_auth(family.into(), status, headers, body, now, error)
}

#[derive(Clone, Copy)]
struct AccountLimitContext {
    family: crate::spec::WireFamilyId,
    subscription_auth: bool,
}

impl From<crate::spec::WireFamilyId> for AccountLimitContext {
    fn from(family: crate::spec::WireFamilyId) -> Self {
        Self {
            family,
            subscription_auth: false,
        }
    }
}

fn classify_account_limit_exhausted_with_auth(
    context: AccountLimitContext,
    status: u16,
    headers: &crate::wire::ResponseHeaders,
    body: &str,
    now: SystemTime,
    error: ProviderError,
) -> ProviderError {
    let family = context.family;
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
            if context.subscription_auth
                && (error.provider_code.as_deref() == Some("personal-team-blocked:spending-limit")
                    || extract_provider_code(body).as_deref()
                        == Some("personal-team-blocked:spending-limit"))
            {
                let resets_at_ms = retry_after_deadline(now, error.retry_after_secs);
                return ProviderError {
                    class: ErrorClass::Transient,
                    provider_code: Some(ACCOUNT_LIMIT_EXHAUSTED.to_owned()),
                    resets_at_ms,
                    resets_at_source: resets_at_ms.map(|_| "retry_after".to_owned()),
                    limit_window: Some(broca_protocol::error::AccountLimitWindow::Credit),
                    ..exhausted
                };
            }
            return exhausted;
        }
    }
    if status == 403
        && matches!(
            family,
            crate::spec::WireFamilyId::OpenAiChat | crate::spec::WireFamilyId::AnthropicMessages
        )
    {
        return kimi_usage_limit_reached(family, body, error);
    }
    if status != 429 {
        return error;
    }
    if error.provider_code.as_deref() == Some(crate::service_tier::SERVICE_TIER_UNAVAILABLE) {
        return error;
    }
    if status == 429 {
        if let Some(limit_window) = opencode_go_usage_limit_window(body) {
            let resets_at_ms = retry_after_deadline(now, error.retry_after_secs);
            return ProviderError {
                refusal: None,
                class: ErrorClass::Transient,
                provider_code: Some(ACCOUNT_LIMIT_EXHAUSTED.to_owned()),
                resets_at_ms,
                resets_at_source: resets_at_ms.map(|_| "retry_after".to_owned()),
                limit_window: Some(broca_protocol::error::AccountLimitWindow::Quota(
                    limit_window,
                )),
                account_ref: None,
                ..error
            };
        }
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
                    limit_window: limit
                        .limit_window
                        .map(broca_protocol::error::AccountLimitWindow::Quota),
                    account_ref: None,
                    ..error
                },
                None => error,
            }
        }
        crate::spec::WireFamilyId::OpenAiResponsesChatgpt => {
            chatgpt_usage_limit_reached(status, headers, body, now, error)
        }
        crate::spec::WireFamilyId::OpenAiResponses => {
            meta_subscription_quota_exhausted(status, body, error)
        }
        crate::spec::WireFamilyId::OpenAiChat => error,
        // Code Assist wraps the Gemini request but answers errors in the same Google RPC shape,
        // so both families share one decoder.
        crate::spec::WireFamilyId::Gemini | crate::spec::WireFamilyId::GoogleCodeAssist => {
            google_quota_exhausted(body, now, error)
        }
        _ => error,
    }
}

const OPENCODE_GO_LIMIT_WINDOWS: [&str; 3] = ["5 hour", "weekly", "monthly"];

/// OpenCode Go's documented-in-source 429 uses a typed body code, a named allowance window and
/// a relative `Retry-After`. A bare rate-limit refusal does not have this complete signature.
fn opencode_go_usage_limit_window(body: &str) -> Option<String> {
    let body: Value = serde_json::from_str(body).ok()?;
    if body.get("type").and_then(Value::as_str) != Some("error")
        || body.pointer("/error/type").and_then(Value::as_str) != Some("GoUsageLimitError")
        || body
            .pointer("/metadata/workspace")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        return None;
    }
    let limit_window = body
        .pointer("/metadata/limitName")
        .and_then(Value::as_str)?;
    OPENCODE_GO_LIMIT_WINDOWS
        .contains(&limit_window)
        .then(|| limit_window.to_owned())
}

fn retry_after_deadline(now: SystemTime, retry_after_secs: Option<f64>) -> Option<u64> {
    let seconds = retry_after_secs.filter(|seconds| seconds.is_finite() && *seconds >= 0.0)?;
    let millis = (seconds * 1_000.0).ceil();
    let millis = if millis.is_finite() {
        millis.min(u64::MAX as f64) as u64
    } else {
        u64::MAX
    };
    Some(system_time_millis(now).saturating_add(millis))
}

const OPENCODE_GO_ERROR_CODES: [&str; 12] = [
    "AuthError",
    "GoUsageLimitError",
    "FreeUsageLimitError",
    "BlackUsageLimitError",
    "RateLimitError",
    "ModelError",
    "RegionError",
    "DataPolicyError",
    "CreditsError",
    "MonthlyLimitError",
    "UserLimitError",
    "MissingSessionID",
];

fn opencode_go_body_error_code(body: &str) -> Option<&'static str> {
    let body: Value = serde_json::from_str(body).ok()?;
    let error = body.get("error")?.as_object()?;
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .or_else(|| error.get("type").and_then(Value::as_str))?;
    OPENCODE_GO_ERROR_CODES
        .iter()
        .copied()
        .find(|candidate| *candidate == code)
}

fn is_opencode_go_request(headers: &[(String, String)]) -> bool {
    headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("x-opencode-client") && value == "pi")
        && headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("x-opencode-session"))
}

#[derive(Debug, PartialEq, Eq)]
struct OpenCodeGoRefusalLogFields {
    status: u16,
    body_error_code: Option<&'static str>,
    safe_headers: Vec<(String, String)>,
}

fn opencode_go_refusal_log_fields(
    request_headers: &[(String, String)],
    status: u16,
    response_headers: &crate::wire::ResponseHeaders,
    body: &str,
) -> Option<OpenCodeGoRefusalLogFields> {
    if !is_opencode_go_request(request_headers) {
        return None;
    }
    const SAFE_HEADERS: [&str; 6] = [
        "retry-after",
        "content-type",
        "x-request-id",
        "request-id",
        "server",
        "cf-ray",
    ];
    let safe_headers = SAFE_HEADERS
        .iter()
        .filter_map(|name| {
            response_headers
                .get(name)
                .map(|value| ((*name).to_owned(), value.to_owned()))
        })
        .collect();
    Some(OpenCodeGoRefusalLogFields {
        status,
        body_error_code: opencode_go_body_error_code(body),
        safe_headers,
    })
}

fn log_opencode_go_refusal(
    request_headers: &[(String, String)],
    status: u16,
    response_headers: &crate::wire::ResponseHeaders,
    body: &str,
) {
    let Some(fields) =
        opencode_go_refusal_log_fields(request_headers, status, response_headers, body)
    else {
        return;
    };
    tracing::warn!(
        target: "provider",
        http_status = fields.status,
        body_error_code = fields.body_error_code.unwrap_or("unrecognized"),
        response_headers = ?fields.safe_headers,
        "OpenCode Go request refused"
    );
}

/// Meta's Muse subscription quota refusal is an account wall, not a short rate limit: its HTTP
/// 429 carries a structured `rate_limit_error`, an absolute `resets_at`, and a subscription
/// exhaustion message. The complete captured sentence and matching textual/numeric reset are
/// required so a per-minute throttle cannot become an account wall through a prose substring.
fn meta_subscription_quota_exhausted(
    status: u16,
    body: &str,
    error: ProviderError,
) -> ProviderError {
    if status != 429 {
        return error;
    }
    let Ok(body) = serde_json::from_str::<Value>(body) else {
        return error;
    };
    let Some(detail) = body.get("error") else {
        return error;
    };
    if detail.get("type").and_then(Value::as_str) != Some("rate_limit_error")
        || detail.get("code").and_then(Value::as_str) != Some("rate_limit_exceeded")
    {
        return error;
    }
    let Some(stamp) = detail
        .get("message")
        .and_then(Value::as_str)
        .and_then(|message| {
            message.strip_prefix("Subscription quota exhausted. Your usage window resets at ")
        })
        .and_then(|tail| tail.strip_suffix('.'))
        .filter(|stamp| stamp.len() == 20)
    else {
        return error;
    };
    let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(stamp) else {
        return error;
    };
    if parsed.timestamp_subsec_nanos() != 0
        || parsed.format("%Y-%m-%dT%H:%M:%SZ").to_string() != stamp
    {
        return error;
    }
    let Ok(seconds) = u64::try_from(parsed.timestamp()) else {
        return error;
    };
    if detail.get("resets_at").and_then(Value::as_u64) != Some(seconds) {
        return error;
    }
    let Some(resets_at_ms) = seconds.checked_mul(1_000) else {
        return error;
    };
    ProviderError {
        refusal: None,
        class: ErrorClass::Transient,
        provider_code: Some(ACCOUNT_LIMIT_EXHAUSTED.to_string()),
        resets_at_ms: Some(resets_at_ms),
        resets_at_source: Some("body".to_owned()),
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
        ..error
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
/// marked `retry_after`, not a stated absolute reset from the body. The body names the plan but
/// not which window ran out, so `limit_window` comes only from the response's `x-codex-*` quota
/// headers when they state it (see [`chatgpt_spent_window`]), else it stays empty: inventing a
/// window name would mislead a router. A ChatGPT 429 with any other body (an ordinary throttle)
/// is returned unchanged and keeps retrying.
fn chatgpt_usage_limit_reached(
    status: u16,
    headers: &crate::wire::ResponseHeaders,
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
        limit_window: chatgpt_spent_window(headers)
            .map(|window| broca_protocol::error::AccountLimitWindow::Quota(window.into())),
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
        ..error
    }
}

/// The usage window a ChatGPT/Codex response's quota headers show spent, by the name routing
/// uses for it: `five_hour` or `seven_day`. The headers describe two windows, primary and
/// secondary, each by its length (`x-codex-{primary,secondary}-window-minutes`) and how much of
/// it is used (`x-codex-{primary,secondary}-used-percent`). The spent window is the one used at
/// or above 100 percent, named by its length: 300 minutes is `five_hour`, 10080 is `seven_day`.
/// Only what the headers state is read: when a header is missing or doesn't parse, when both or
/// neither window is spent, or when the spent one has another length, there is no window.
///
/// Captured 2026-11-02 from an account past its weekly allowance, the 429 refusing it carried
/// `x-codex-primary-used-percent: 100`, `x-codex-primary-window-minutes: 10080`,
/// `x-codex-secondary-used-percent: 0` and `x-codex-secondary-window-minutes: 0`.
fn chatgpt_spent_window(headers: &crate::wire::ResponseHeaders) -> Option<&'static str> {
    use cortexkit_chatgpt_policy::QuotaHeader;
    let read = |used: QuotaHeader, minutes: QuotaHeader| -> Option<(bool, u64)> {
        let used: f64 = headers.get(used.name())?.trim().parse().ok()?;
        let minutes: u64 = headers.get(minutes.name())?.trim().parse().ok()?;
        used.is_finite().then_some((used >= 100.0, minutes))
    };
    let primary = read(
        QuotaHeader::PrimaryUsedPercent,
        QuotaHeader::PrimaryWindowMinutes,
    )?;
    let secondary = read(
        QuotaHeader::SecondaryUsedPercent,
        QuotaHeader::SecondaryWindowMinutes,
    )?;
    let minutes = match (primary, secondary) {
        ((true, minutes), (false, _)) | ((false, _), (true, minutes)) => minutes,
        _ => return None,
    };
    continued_window(minutes)
}

/// Routing's name for a ChatGPT usage window of `minutes`: `five_hour` for 300, `seven_day` for
/// 10080, none for any other length.
fn continued_window(minutes: u64) -> Option<&'static str> {
    match minutes {
        300 => Some("five_hour"),
        10_080 => Some("seven_day"),
        _ => None,
    }
}

/// The usage window a ChatGPT/Codex `codex.rate_limits` socket frame shows spent, by the same
/// rule as the response headers (see [`chatgpt_spent_window`]): exactly one of
/// `rate_limits.primary` and `rate_limits.secondary` at or above 100 `used_percent`, named by its
/// `window_minutes`. A window the frame leaves out or sends as `null` isn't spent. A window
/// whose numbers are missing or don't parse, both or neither spent, or another length give no
/// window. Reset times are never read.
///
/// Captured 2026-11-02 on a socket for an account past its weekly allowance:
/// `{"type":"codex.rate_limits", "rate_limits":{"allowed":false,"limit_reached":true,
/// "primary":{"used_percent":100,"window_minutes":10080,...},"secondary":null}, ...}`.
pub(crate) fn chatgpt_frame_window(frame: &Value) -> Option<&'static str> {
    let limits = frame.get("rate_limits")?;
    let read = |name: &str| -> Option<Option<(bool, u64)>> {
        match limits.get(name) {
            None | Some(Value::Null) => Some(None),
            Some(window) => {
                let used = window.get("used_percent")?.as_f64()?;
                let minutes = window.get("window_minutes")?.as_u64()?;
                used.is_finite().then_some(Some((used >= 100.0, minutes)))
            }
        }
    };
    let spent = |window: Option<(bool, u64)>| window.filter(|(spent, _)| *spent);
    let (primary, secondary) = (read("primary")?, read("secondary")?);
    let minutes = match (spent(primary), spent(secondary)) {
        (Some((_, minutes)), None) | (None, Some((_, minutes))) => minutes,
        _ => return None,
    };
    continued_window(minutes)
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
            digits.parse::<u32>().ok()
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
/// Without credential context, preserve the API-key classification: permanent billing exhaustion.
/// The auth-aware caller classifies a subscription refusal as a transient account limit.
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
            account_continue: None,
            ..error.clone()
        })
    } else {
        None
    }
}

/// OpenCode's hosted gateways (Zen at `https://opencode.ai/zen/...` and Go at
/// `https://opencode.ai/inference/go/...`) refuse a request from an account with no funds left
/// with HTTP 402 Payment Required, for example
/// `{"error":{"type":"server_error","message":"Upstream request failed: Insufficient account funds"}}`.
/// The body's `error.type` is a generic `server_error` that also labels one-off upstream
/// failures, so on its own it would let routing retry the same empty account. The status is the
/// structured signal, so the decision keys on status 402 and the OpenCode endpoint, never on the
/// message wording. Other hosts' 402s keep their own classification (OpenRouter's, for one, is a
/// request its credit cannot pay for, not an empty balance).
///
/// Like the other `billing_exhausted` refusals it is permanent with no reset: the balance stays
/// empty until someone tops it up, so re-sending within the run cannot succeed.
fn opencode_payment_required(
    request_url: &str,
    status: u16,
    error: ProviderError,
) -> ProviderError {
    if status != 402 || !is_opencode_gateway_url(request_url) {
        return error;
    }
    ProviderError {
        refusal: None,
        class: ErrorClass::Permanent,
        provider_code: Some(BILLING_EXHAUSTED.to_string()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
        ..error
    }
}

/// Whether a request URL points at OpenCode's Zen or Go gateway.
fn is_opencode_gateway_url(request_url: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(request_url) else {
        return false;
    };
    url.host_str() == Some("opencode.ai")
        && (url.path().starts_with("/zen/") || url.path().starts_with("/inference/go/"))
}

/// Inception documents HTTP 402 as "billing inactive or quota exceeded"
/// (https://docs.inceptionlabs.ai/resources/error-codes), with the generic `account_error` body
/// "Account is inactive" (https://docs.inceptionlabs.ai/api-reference/chat/create-a-chat-completion).
/// The 100M free tokens are a one-time credit (https://docs.inceptionlabs.ai/resources/faq), not a
/// resetting allowance: retrying cannot replenish them. Use the status on the exact API host, not
/// the body wording, so routing fences the exhausted credential while other OpenAI-compatible
/// providers keep their own 402 semantics. Preserve the provider's message.
fn inception_payment_required(
    request_url: &str,
    status: u16,
    error: ProviderError,
) -> ProviderError {
    if status != 402
        || !reqwest::Url::parse(request_url)
            .is_ok_and(|url| url.host_str() == Some("api.inceptionlabs.ai"))
    {
        return error;
    }
    ProviderError {
        refusal: None,
        class: ErrorClass::Permanent,
        provider_code: Some(BILLING_EXHAUSTED.to_string()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
        ..error
    }
}

/// Captured Kimi usage walls have no absolute reset. Match only their complete messages under
/// the observed wire discriminator; a window name does not supply a reset timestamp.
fn kimi_usage_limit_reached(
    family: crate::spec::WireFamilyId,
    body: &str,
    error: ProviderError,
) -> ProviderError {
    let Some(detail) = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|body| body.get("error").cloned())
    else {
        return error;
    };
    if detail.get("code").is_some() {
        return error;
    }
    let matches = matches!(
        (
            family,
            detail.get("type").and_then(Value::as_str),
            detail.get("message").and_then(Value::as_str),
        ),
        (
            crate::spec::WireFamilyId::OpenAiChat,
            Some("access_terminated_error"),
            Some(KIMI_WEEKLY_MESSAGE | KIMI_FIVE_HOUR_MESSAGE),
        ) | (
            crate::spec::WireFamilyId::AnthropicMessages,
            Some("permission_error"),
            Some(
                KIMI_FIVE_HOUR_MESSAGE
                    | KIMI_BILLING_PRICING_MESSAGE
                    | KIMI_BILLING_MEMBERSHIP_MESSAGE
            ),
        )
    );
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
        account_continue: None,
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
    match serde_json::from_str::<Value>(body) {
        Ok(body) => google_quota_exhausted_in(&body, now, error),
        Err(_) => ProviderError {
            provider_details: None,
            ..error
        },
    }
}

/// [`google_quota_exhausted`] for an already-parsed body. A Gemini or Code Assist stream that
/// answered 200 can end with the same `{"error": ...}` object a 429 carries, and its normalizer
/// passes that object here so the wall is recognized the same way in both places.
pub(crate) fn google_quota_exhausted_in(
    body: &Value,
    now: SystemTime,
    error: ProviderError,
) -> ProviderError {
    let provider_details = google_rpc_error_details(body).map(Box::new);
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
        return ProviderError {
            provider_details,
            ..error
        };
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
        account_continue: None,
        provider_details,
        ..error
    }
}

const GOOGLE_RPC_METADATA_MAX_KEYS: usize = 16;
const GOOGLE_RPC_DIMENSIONS_MAX_KEYS: usize = 8;
const GOOGLE_RPC_VIOLATIONS_MAX: usize = 8;

/// Copy only bounded typed quota fields; provider messages can contain request content.
fn google_rpc_error_details(body: &Value) -> Option<GoogleRpcErrorDetails> {
    let details = body
        .get("error")
        .and_then(|error| error.get("details"))
        .and_then(Value::as_array)?;
    let mut error_info = None;
    let mut violations = Vec::new();
    let mut retry_info = None;

    for detail in details {
        match detail.get("@type").and_then(Value::as_str) {
            Some(GOOGLE_RPC_ERROR_INFO_TYPE) if error_info.is_none() => {
                error_info = Some(GoogleRpcErrorInfo {
                    reason: bounded_google_string(detail.get("reason"), 128),
                    domain: bounded_google_string(detail.get("domain"), 128),
                    metadata: bounded_google_string_map(
                        detail.get("metadata"),
                        GOOGLE_RPC_METADATA_MAX_KEYS,
                    ),
                });
            }
            Some(GOOGLE_RPC_QUOTA_FAILURE_TYPE) if violations.len() < GOOGLE_RPC_VIOLATIONS_MAX => {
                let Some(items) = detail.get("violations").and_then(Value::as_array) else {
                    continue;
                };
                for item in items
                    .iter()
                    .take(GOOGLE_RPC_VIOLATIONS_MAX - violations.len())
                {
                    if let Some(violation) = google_rpc_quota_violation(item) {
                        violations.push(violation);
                    }
                }
            }
            Some(GOOGLE_RPC_RETRY_INFO_TYPE) if retry_info.is_none() => {
                if let Some(retry_delay) = bounded_google_string(detail.get("retryDelay"), 128) {
                    retry_info = Some(GoogleRpcRetryInfo {
                        retry_delay: Some(retry_delay),
                    });
                }
            }
            _ => {}
        }
    }

    let quota_failure = (!violations.is_empty()).then_some(GoogleRpcQuotaFailure { violations });
    if error_info.is_none() && quota_failure.is_none() && retry_info.is_none() {
        return None;
    }
    Some(GoogleRpcErrorDetails {
        error_info,
        quota_failure,
        retry_info,
    })
}

fn google_rpc_quota_violation(value: &Value) -> Option<GoogleRpcQuotaViolation> {
    let violation = GoogleRpcQuotaViolation {
        subject: bounded_google_string(value.get("subject"), 256),
        description: bounded_google_string(value.get("description"), 256),
        quota_metric: bounded_google_string(value.get("quotaMetric"), 128),
        quota_id: bounded_google_string(value.get("quotaId"), 128),
        quota_dimensions: bounded_google_string_map(
            value.get("quotaDimensions"),
            GOOGLE_RPC_DIMENSIONS_MAX_KEYS,
        ),
        quota_value: bounded_google_number_or_string(value.get("quotaValue"), 128),
        future_quota_value: bounded_google_number_or_string(value.get("futureQuotaValue"), 128),
    };
    (violation.subject.is_some()
        || violation.description.is_some()
        || violation.quota_metric.is_some()
        || violation.quota_id.is_some()
        || !violation.quota_dimensions.is_empty()
        || violation.quota_value.is_some()
        || violation.future_quota_value.is_some())
    .then_some(violation)
}

fn bounded_google_string(value: Option<&Value>, max_chars: usize) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(|value| value.chars().take(max_chars).collect())
}

fn bounded_google_number_or_string(value: Option<&Value>, max_chars: usize) -> Option<String> {
    let value = value?;
    let value = value.as_str().map(str::to_owned).or_else(|| {
        value
            .as_i64()
            .map(|number| number.to_string())
            .or_else(|| value.as_u64().map(|number| number.to_string()))
    })?;
    Some(value.chars().take(max_chars).collect())
}

fn bounded_google_string_map(
    value: Option<&Value>,
    max_keys: usize,
) -> std::collections::BTreeMap<String, String> {
    value
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|object| object.iter())
        .filter_map(|(key, value)| {
            let value = bounded_google_string(Some(value), 128)?;
            let key = key.chars().take(128).collect();
            Some((key, value))
        })
        .take(max_keys)
        .collect()
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
const GOOGLE_RPC_QUOTA_FAILURE_TYPE: &str = "type.googleapis.com/google.rpc.QuotaFailure";
const GOOGLE_RPC_RETRY_INFO_TYPE: &str = "type.googleapis.com/google.rpc.RetryInfo";
const GOOGLE_QUOTA_EXHAUSTED_REASON: &str = "QUOTA_EXHAUSTED";

use cortexkit_anthropic_policy::{
    UNIFIED_REPRESENTATIVE_CLAIM as ANTHROPIC_UNIFIED_REPRESENTATIVE_CLAIM,
    UNIFIED_RESET as ANTHROPIC_UNIFIED_RESET, UNIFIED_STATUS as ANTHROPIC_UNIFIED_STATUS,
};

const KIMI_WEEKLY_MESSAGE: &str = "You've reached your weekly (7-day) usage limit. Your quota will reset when the current 7-day window ends. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/membership/subscription?tab=quota";
const KIMI_FIVE_HOUR_MESSAGE: &str = "You've reached your 5-hour usage limit. Your quota will reset when the current 5-hour window ends. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/membership/subscription?tab=quota";
const KIMI_BILLING_PRICING_MESSAGE: &str = "You've reached your usage limit for this billing cycle. Your quota will be refreshed in the next cycle. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/code/#pricing";
const KIMI_BILLING_MEMBERSHIP_MESSAGE: &str = "You've reached your usage limit for this billing cycle. Your quota will be refreshed in the next cycle. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/membership/subscription?tab=quota";

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
                && body.as_ref().is_some_and(|body| {
                    body.pointer("/error/code").is_none()
                        && cortexkit_anthropic_policy::anthropic_credit_balance_exhausted_exact(
                            code,
                            body.pointer("/error/type")
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                            message,
                        )
                }) =>
        {
            Some(BILLING_EXHAUSTED.to_string())
        }
        // OpenAI's empty prepaid balance (`credit_balance_exhausted`, error type
        // `insufficient_quota`) is about the credential, not this request, so it gets the code
        // that names the credential and stops routing to it. An unknown family is included
        // because public callers may hold only the body, and these codes are OpenAI's own.
        code_now
            if matches!(
                family,
                None | Some(
                    WireFamilyId::OpenAiChat
                        | WireFamilyId::OpenAiResponses
                        | WireFamilyId::OpenAiResponsesChatgpt
                )
            ) && crate::normalize::openai_billing_exhausted(
                code_now,
                body.as_ref()
                    .and_then(|body| body.get("error").filter(|v| v.is_object()).or(Some(body)))
                    .and_then(|error| error.get("type"))
                    .and_then(Value::as_str),
            ) =>
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
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        class,
        // Keep the compiler refusal recognizable even if unrelated response fields exceed
        // the diagnostic cap. Classification uses the full body, never truncated JSON.
        message: if family == Some(WireFamilyId::AnthropicMessages)
            && body
                .as_ref()
                .is_some_and(|body| crate::structured_output::grammar_rejection_body(code, body))
        {
            "400: {\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Schema is too complex for compilation\"}}".to_owned()
        } else {
            http_error_message(code, truncate_error_body(text), class)
        },
        status: Some(code),
        retry_after_secs,
        provider_code,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    };
    // Run after the context-overflow and billing classification above, so a 400 those give a
    // more specific code keeps it.
    let error_object = body.as_ref().and_then(|body| body.get("error"));
    if family == Some(WireFamilyId::AnthropicMessages) {
        crate::request_invalid::classify_anthropic_request_invalid(error, error_object)
    } else {
        crate::request_invalid::classify_request_invalid(error, error_object)
    }
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
#[path = "transport_classifier_captures.rs"]
mod classifier_captures;

#[cfg(test)]
#[path = "transport/tests.rs"]
mod tests;

#[cfg(test)]
mod stream_floor_tests;

/// A tool call whose arguments the output-token ceiling cut off, driven through the live
/// transport for every wire family that streams tool arguments as a string. Each fixture is
/// the smallest stream carrying the family's own wire shape for "arguments started, output
/// stopped at the cap". The ceiling case must fail once, permanent, naming the ceiling; the
/// controls carry the same truncated arguments without a ceiling stop reason and must keep
/// the ordinary transient parse failure, including its single retry.
#[cfg(test)]
#[path = "transport/output_ceiling_tests.rs"]
mod output_ceiling_tests;

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

fn warn_private_failure(error: &ProviderError, trace: &AttemptTrace, message: &'static str) {
    let gaps = trace.content_gaps.snapshot();
    tracing::warn!(
        target: "provider",
        class = ?error.class,
        status = error.status,
        provider_code = error.provider_code.as_deref().filter(|code| crate::safe_provider_code(code)).unwrap_or("none"),
        max_content_gap_ms = gaps.max_content_gap_ms,
        first_content_ms = gaps.first_content_ms,
        tail_gap_ms = gaps.tail_gap_ms,
        max_gap_after_event = "private",
        message,
    );
}

#[cfg(test)]
#[path = "transport_chunk_error_tests.rs"]
mod chunk_error_tests;

#[cfg(test)]
#[path = "transport_tool_args_tests.rs"]
mod tool_args_tests;

#[cfg(test)]
#[path = "transport_closed_step_tests.rs"]
mod closed_step_tests;

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

#[cfg(test)]
mod recorded_version_authority_tests {
    use crate::families::{
        earlier_renderer_versions, frozen_config_for_family, EarlierRendererVersion,
    };
    use crate::spec::WireFamilyId;
    use crate::wire::AuthHandle;
    use broca_core::Provider;

    #[test]
    fn a_frozen_version_cannot_vouch_for_unsupported_renderer_bytes() {
        let mut rejected = 0;
        for family in [
            WireFamilyId::OpenAiChat,
            WireFamilyId::AnthropicMessages,
            WireFamilyId::BedrockConverse,
            WireFamilyId::OpenAiResponses,
            WireFamilyId::OpenAiResponsesChatgpt,
            WireFamilyId::GoogleCodeAssist,
        ] {
            for &(version, disposition) in earlier_renderer_versions(family) {
                if !matches!(disposition, EarlierRendererVersion::NotReproducible(_)) {
                    continue;
                }
                let mut frozen = frozen_config_for_family(family, "recorded-model");
                frozen.renderer_version = version;
                if family == WireFamilyId::GoogleCodeAssist {
                    frozen.project_id = Some("fixture-project".into());
                }
                let provider = crate::factory::live_provider(frozen, AuthHandle::None).unwrap();
                assert!(
                    !provider.reproduces_renderer_version(version),
                    "{family:?} cannot vouch for recorded version {version}"
                );
                rejected += 1;
            }
        }
        assert!(
            rejected >= 20,
            "the guard must exercise every family's non-reproducible versions"
        );
    }
}
