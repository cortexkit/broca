// Adapted from OpenAI Codex, revision a6baf8867cb4c9726213c0884a5c8b11f0cfd8bf (Apache-2.0).
// Copyright 2025 OpenAI. See THIRD-PARTY-NOTICES.md and LICENSES/Apache-2.0.txt.
// broca integrates session-owned sockets with its shared Responses normalizer and HTTPS fallback.

//! Responses sockets. The pump answers pings even between model steps and runs; it never
//! decodes model output. Text records go through the same Responses normalizer as HTTPS.

use super::*;
use futures::SinkExt;
use std::borrow::Cow;
use std::collections::{hash_map::RandomState, HashSet};
use std::hash::BuildHasher;
use std::sync::{atomic::AtomicU16, Mutex, RwLock};
use tokio::sync::oneshot;
use tokio_tungstenite::{connect_async_with_config, tungstenite::client::IntoClientRequest};
use tungstenite::{
    extensions::{compression::deflate::DeflateConfig, ExtensionsConfig},
    Message,
};

#[path = "transport_websocket_pool.rs"]
mod pool;
pub use pool::Connections;
pub(super) use pool::Lease;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
// Codex's default stream budget is five retries after the initial request. Retain its
// six-failure threshold across this run, including upgrades that fell back before sending.
const FAILURE_LIMIT: usize = 6;
// OpenAI limits connections to 60 minutes. Retire at 40 minutes to leave headroom
// above the longest healthy request we have measured (about 14 minutes).
const MAX_SOCKET_AGE: Duration = Duration::from_secs(40 * 60);
const BETA: &str = "responses_websockets=2026-02-06";
// Only the first 512 bytes of a refused upgrade's body are read, and only to recognise a ChatGPT
// usage-limit refusal (whose body is about 200 bytes). The body is never logged or stored.
const UPGRADE_BODY_CLASSIFY_LIMIT: usize = 512;
// How much of a wrapped error frame the local WARN line carries; see `WrappedErrorFrame`.
const WRAPPED_FRAME_LOG_LIMIT: usize = 1024;

const DISTINCT_LIMIT: usize = 4096;

#[derive(Default)]
struct DistinctHashes {
    hashes: HashSet<u64>,
    hasher: RandomState,
    overflow: bool,
}

impl DistinctHashes {
    fn observe(&mut self, text: &str) {
        let hash = self.hasher.hash_one(text);
        if self.hashes.contains(&hash) {
            return;
        }
        if self.hashes.len() < DISTINCT_LIMIT {
            self.hashes.insert(hash);
        } else {
            self.overflow = true;
        }
    }
}

// Socket counters include frames read ahead of the normalizer, but exclude idle gaps by
// subtracting the snapshot taken when an attempt acquires the socket.
#[derive(Default)]
struct FrameCounts {
    response_create_sent: AtomicU64,
    other_text_sent: AtomicU64,
    pongs_sent: AtomicU64,
    text_received: AtomicU64,
    pings_received: AtomicU64,
    pongs_received: AtomicU64,
    closes_received: AtomicU64,
}

impl FrameCounts {
    fn snapshot(&self) -> [u64; 7] {
        [
            &self.response_create_sent,
            &self.other_text_sent,
            &self.pongs_sent,
            &self.text_received,
            &self.pings_received,
            &self.pongs_received,
            &self.closes_received,
        ]
        .map(|count| count.load(Ordering::Relaxed))
    }
}

/// Count outbound `response.create` writes confirmed by the pump, not completed-response
/// continuation receipts. Cancellation can prevent a write acknowledgement from reaching
/// the awaiting task without changing this count.
#[derive(Clone)]
pub(super) struct PhysicalSends {
    frames: Arc<FrameCounts>,
    before: u64,
}
impl PhysicalSends {
    pub(super) fn count(&self) -> u32 {
        self.frames
            .response_create_sent
            .load(Ordering::Relaxed)
            .saturating_sub(self.before)
            .try_into()
            .unwrap_or(u32::MAX)
    }
}

// A newtype lets Serde's borrowing support reach strings inside Option. Plain
// Option<Cow<str>> would allocate even for strings without JSON escapes.
#[derive(serde::Deserialize)]
struct ObservationString<'a>(#[serde(borrow)] Cow<'a, str>);

#[derive(serde::Deserialize)]
struct ObservationId<'a> {
    #[serde(borrow)]
    id: Option<ObservationString<'a>>,
}

#[derive(serde::Deserialize)]
struct ObservationError<'a> {
    #[serde(borrow)]
    code: Option<ObservationString<'a>>,
}

// Unknown fields are skipped instead of constructing another copy of model output.
#[derive(serde::Deserialize)]
struct ObservationEvent<'a> {
    #[serde(rename = "type", borrow)]
    kind: Option<ObservationString<'a>>,
    sequence_number: Option<u64>,
    #[serde(borrow)]
    response_id: Option<ObservationString<'a>>,
    #[serde(borrow)]
    item_id: Option<ObservationString<'a>>,
    output_index: Option<u64>,
    #[serde(borrow)]
    delta: Option<ObservationString<'a>>,
    #[serde(borrow)]
    response: Option<ObservationId<'a>>,
    #[serde(borrow)]
    item: Option<ObservationId<'a>>,
    #[serde(borrow)]
    error: Option<ObservationError<'a>>,
}

#[derive(Default)]
pub(super) struct AttemptObservation {
    continuation: Option<bool>,
    suffix_item_count: Option<u64>,
    frame_len: Option<u64>,
    frame_sha256: Option<String>,
    full_input_reason: Option<&'static str>,
    frames: Option<(Arc<FrameCounts>, [u64; 7])>,
    close_code: Option<Arc<AtomicU16>>,
    generation: Option<u64>,
    reused: Option<bool>,
    age_ms: Option<u64>,
    response_create_enqueued: u64,
    sequence_count: u64,
    sequence_min: Option<u64>,
    sequence_max: Option<u64>,
    sequence_repeated_or_backwards: u64,
    response_ids: DistinctHashes,
    item_ids: DistinctHashes,
    output_index_max: Option<u64>,
    argument_deltas: u64,
    argument_delta_bytes: u64,
    whitespace_deltas: u64,
    empty_deltas: u64,
    delta_hashes: DistinctHashes,
    wrapped_error_code: Option<&'static str>,
}

impl AttemptObservation {
    pub(super) fn observe_text(&mut self, raw: &[u8]) {
        let Ok(event) = serde_json::from_slice::<ObservationEvent<'_>>(raw) else {
            return;
        };
        if let Some(sequence) = event.sequence_number {
            self.sequence_count += 1;
            // Compare with the high-water mark, so replaying an older range is counted
            // throughout the replay, not just at its first backwards jump.
            if self.sequence_max.is_some_and(|max| sequence <= max) {
                self.sequence_repeated_or_backwards += 1;
            }
            self.sequence_min = Some(self.sequence_min.map_or(sequence, |min| min.min(sequence)));
            self.sequence_max = Some(self.sequence_max.map_or(sequence, |max| max.max(sequence)));
        }
        for id in [
            event.response_id.as_ref(),
            event
                .response
                .as_ref()
                .and_then(|response| response.id.as_ref()),
        ]
        .into_iter()
        .flatten()
        {
            self.response_ids.observe(&id.0);
        }
        for id in [
            event.item_id.as_ref(),
            event.item.as_ref().and_then(|item| item.id.as_ref()),
        ]
        .into_iter()
        .flatten()
        {
            self.item_ids.observe(&id.0);
        }
        if let Some(index) = event.output_index {
            self.output_index_max = Some(self.output_index_max.map_or(index, |max| max.max(index)));
        }
        match event.kind.as_ref().map(|kind| kind.0.as_ref()) {
            Some("response.function_call_arguments.delta") => {
                if let Some(ObservationString(delta)) = event.delta {
                    self.argument_deltas += 1;
                    self.argument_delta_bytes += delta.len() as u64;
                    if delta.is_empty() {
                        self.empty_deltas += 1;
                    } else {
                        self.whitespace_deltas += u64::from(delta.chars().all(char::is_whitespace));
                    }
                    self.delta_hashes.observe(&delta);
                }
            }
            Some("error") => {
                self.wrapped_error_code = Some(
                    match event
                        .error
                        .as_ref()
                        .and_then(|error| error.code.as_ref())
                        .map(|code| code.0.as_ref())
                    {
                        Some("websocket_connection_limit_reached") => {
                            "websocket_connection_limit_reached"
                        }
                        _ => "other",
                    },
                );
            }
            _ => {}
        }
    }
}

struct ObservationGuard {
    observation: Arc<Mutex<AttemptObservation>>,
    started: Instant,
    failed: bool,
}

impl ObservationGuard {
    fn begin(trace: &mut AttemptTrace) -> Self {
        let observation = Arc::new(Mutex::new(AttemptObservation::default()));
        trace.websocket_observation = Arc::downgrade(&observation);
        Self {
            observation,
            started: Instant::now(),
            failed: true,
        }
    }
}

impl Drop for ObservationGuard {
    fn drop(&mut self) {
        let stats = self.observation.lock().unwrap();
        let frames = stats.frames.as_ref().map_or([0; 7], |(counts, before)| {
            let after = counts.snapshot();
            std::array::from_fn(|index| after[index].saturating_sub(before[index]))
        });
        let elapsed = self.started.elapsed();
        macro_rules! emit {
            ($($log:ident)::+ ! ($($prefix:tt)*)) => {
                $($log)::+!($($prefix)*
                    transport = "websocket", failed = self.failed, elapsed_ms = elapsed.as_millis() as u64,
                    socket_generation = stats.generation, socket_reused = stats.reused, socket_age_ms = stats.age_ms,
                    continuation = stats.continuation, suffix_item_count = stats.suffix_item_count,
                    frame_len = stats.frame_len, frame_sha256 = stats.frame_sha256.as_deref(), full_input_reason = stats.full_input_reason,
                    response_create_enqueued = stats.response_create_enqueued, response_create_sent = frames[0],
                    other_text_sent = frames[1], pongs_sent = frames[2], text_received = frames[3],
                    pings_received = frames[4], pongs_received = frames[5], closes_received = frames[6],
                    close_code = stats.close_code.as_ref().map(|code| code.load(Ordering::Acquire)).filter(|code| *code != 0),
                    wrapped_error_code = stats.wrapped_error_code,
                    sequence_count = stats.sequence_count, sequence_min = stats.sequence_min, sequence_max = stats.sequence_max,
                    sequence_repeated_or_backwards = stats.sequence_repeated_or_backwards,
                    distinct_response_ids = stats.response_ids.hashes.len(), response_ids_overflow = stats.response_ids.overflow,
                    distinct_item_ids = stats.item_ids.hashes.len(), item_ids_overflow = stats.item_ids.overflow,
                    output_index_max = stats.output_index_max, argument_deltas = stats.argument_deltas,
                    argument_delta_bytes = stats.argument_delta_bytes, whitespace_deltas = stats.whitespace_deltas, empty_deltas = stats.empty_deltas,
                    distinct_deltas = stats.delta_hashes.hashes.len(), distinct_deltas_overflow = stats.delta_hashes.overflow,
                    "Responses websocket attempt summary"
                );
            };
        }
        if self.failed || elapsed > Duration::from_secs(120) {
            emit!(tracing::info!(target: "provider.websocket.attempt",));
        } else {
            emit!(tracing::debug!(target: "provider.websocket.attempt",));
        }
    }
}

enum UpgradeFailure {
    /// The upgrade was answered 401: the token itself was refused.
    AuthRejected(u16),
    /// The upgrade's own body already says the account's usage limit is reached, so an HTTPS
    /// round trip would only repeat the refusal.
    AccountLimit(u16, Box<ProviderError>),
    /// No socket for this attempt: send it over HTTPS instead.
    Transport,
}

type HttpsPolicy = Arc<dyn Fn() -> bool + Send + Sync>;

pub(super) struct Session {
    state: Mutex<State>,
    policy: RwLock<HttpsPolicy>,
}

#[derive(Default)]
struct State {
    socket: Option<Arc<Socket>>,
    failures: usize,
    turn_state: Option<String>,
    force_https: bool,
    generation: u64,
    #[cfg(test)]
    clock_offset: Duration,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::default()),
            policy: RwLock::new(Arc::new(|| false)),
        }
    }
}

impl Session {
    fn take_completed_socket(&self) -> Option<Arc<Socket>> {
        let mut state = self.state.lock().unwrap();
        let socket = state.socket.take()?;
        if state.force_https
            || socket.closed.load(Ordering::Acquire)
            || socket.shutdown.is_cancelled()
            || socket.continuation.lock().unwrap().is_none()
        {
            socket.shutdown.cancel();
            return None;
        }
        Some(socket)
    }

    pub(super) fn set_policy(&self, policy: HttpsPolicy) {
        *self.policy.write().unwrap() = policy;
    }

    pub(super) fn close(&self) {
        let mut state = self.state.lock().unwrap();
        if let Some(socket) = state.socket.take() {
            socket.shutdown.cancel();
        }
        *state = State {
            generation: state.generation,
            ..State::default()
        };
    }

    fn fail(&self) {
        let mut state = self.state.lock().unwrap();
        if let Some(socket) = state.socket.take() {
            socket.shutdown.cancel();
        }
        state.failures += 1;
    }

    pub(super) fn switch_to_https(&self) {
        let mut state = self.state.lock().unwrap();
        state.force_https = true;
        if let Some(socket) = state.socket.take() {
            socket.shutdown.cancel();
        }
    }

    fn now(&self) -> Instant {
        let now = Instant::now();
        #[cfg(test)]
        let now = now + self.state.lock().unwrap().clock_offset;
        now
    }

    #[cfg(test)]
    pub(super) fn advance_clock(&self, elapsed: Duration) {
        self.state.lock().unwrap().clock_offset += elapsed;
    }

    #[cfg(test)]
    pub(super) async fn wait_closed(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self
                    .state
                    .lock()
                    .unwrap()
                    .socket
                    .as_ref()
                    .is_some_and(|socket| socket.closed.load(Ordering::Acquire))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("socket pump observes the peer close");
    }

    fn forced_https(&self) -> bool {
        let policy = Arc::clone(&self.policy.read().unwrap());
        // Evaluate on every attempt so config edits still take effect immediately. The
        // callback may read disk; it must not hold up socket teardown or failure accounting.
        let configured_https = policy();
        let state = self.state.lock().unwrap();
        configured_https || state.force_https || state.failures >= FAILURE_LIMIT
    }

    async fn connection(
        &self,
        request: &PreparedHttpRequest,
    ) -> Result<(Arc<Socket>, bool), UpgradeFailure> {
        let now = self.now();
        {
            let mut state = self.state.lock().unwrap();
            if let Some(socket) = &state.socket {
                let age = now.saturating_duration_since(socket.established_at);
                if !socket.closed.load(Ordering::Acquire)
                    && connection_identity_matches(socket, request)
                    && age < MAX_SOCKET_AGE
                {
                    return Ok((Arc::clone(socket), true));
                }
                if age >= MAX_SOCKET_AGE {
                    tracing::info!(target: "provider", transport = "websocket", socket_generation = socket.generation, socket_age_ms = age.as_millis() as u64, "Retiring Responses websocket before its connection lifetime limit");
                }
                socket.shutdown.cancel();
                let auth_changed = !connection_identity_matches(socket, request);
                state.socket = None;
                if auth_changed {
                    state.turn_state = None;
                }
            }
        }
        let generation = {
            let mut state = self.state.lock().unwrap();
            state.generation += 1;
            state.generation
        };
        let result = tokio::time::timeout(CONNECT_TIMEOUT, connect(request, generation, now)).await;
        match result {
            Ok(Ok((socket, turn_state))) => {
                let mut state = self.state.lock().unwrap();
                if state.turn_state.is_none() {
                    state.turn_state = turn_state;
                }
                state.socket = Some(Arc::clone(&socket));
                Ok((socket, false))
            }
            Ok(Err(tungstenite::Error::Http(response))) => {
                let status = response.status().as_u16();
                if status == 401 {
                    // Only a 401 says the token itself was refused. It must reach the shared
                    // re-read/pause path, not send the stale token again over HTTPS.
                    warn_upgrade_refusal(status, response.headers(), "credential_reread");
                    return Err(UpgradeFailure::AuthRejected(status));
                }
                if let Some(error) =
                    upgrade_account_limit(status, response.headers(), response.body().as_deref())
                {
                    warn_upgrade_refusal(status, response.headers(), "account_limit");
                    return Err(UpgradeFailure::AccountLimit(status, Box::new(error)));
                }
                warn_upgrade_refusal(status, response.headers(), "https");
                if status == 403 {
                    // A 403 upgrade is not a verdict on the token: in production, freshly
                    // refreshed tokens kept getting it while the same account's HTTPS requests
                    // were answerable. Send this request over HTTPS, whose answer broca can
                    // classify (usage limit, entitlement, or a real 401), and keep the rest of
                    // the run there instead of asking for a refused upgrade again.
                    self.switch_to_https();
                } else {
                    self.fail();
                }
                Err(UpgradeFailure::Transport)
            }
            _ => {
                // Do not format the upgrade error: it can contain authentication headers.
                tracing::warn!(target: "provider", transport = "websocket", "Responses websocket upgrade failed; using HTTPS before sending the request");
                self.fail();
                Err(UpgradeFailure::Transport)
            }
        }
    }

    fn observe_turn_state(&self, value: &Value) {
        if value.get("type").and_then(Value::as_str) == Some("response.metadata") {
            if let Some(headers) = value.get("headers").and_then(Value::as_object) {
                let state = headers.iter().find_map(|(name, value)| {
                    name.eq_ignore_ascii_case("x-codex-turn-state")
                        .then(|| value.as_str().map(str::to_owned))
                        .flatten()
                });
                let mut guard = self.state.lock().unwrap();
                if guard.turn_state.is_none() {
                    guard.turn_state = state;
                }
            }
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.close();
    }
}

struct SendCommand {
    text: String,
    response_create: bool,
    result: oneshot::Sender<Result<(), std::io::Error>>,
}

struct Socket {
    auth_revision: u64,
    credential_id: Option<String>,
    account_ref: Option<String>,
    url: String,
    continuation: Mutex<Option<crate::chatgpt_continuation::CompletedResponse>>,
    send: mpsc::Sender<SendCommand>,
    recv: tokio::sync::Mutex<mpsc::Receiver<Result<Vec<u8>, std::io::Error>>>,
    shutdown: CancellationToken,
    closed: Arc<AtomicBool>,
    close_code: Arc<AtomicU16>,
    frames: Arc<FrameCounts>,
    headers: Vec<(String, String)>,
    generation: u64,
    established_at: Instant,
    last_completed_at: Mutex<Option<Instant>>,
    attempt_active: tokio::sync::watch::Sender<bool>,
}

impl Drop for Socket {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn connection_identity_matches(socket: &Socket, request: &PreparedHttpRequest) -> bool {
    if request.codex_wire
        && (socket.auth_revision != request.auth_revision
            || socket.url != request.url
            || socket.credential_id != request.credential_id
            || socket.account_ref != request.account_ref)
    {
        return false;
    }
    // Turn metadata is sent on every response.create too. All other headers, including
    // credentials, account and routing, must still match exactly before reusing a socket.
    let connection_scoped = |(name, _): &&(String, String)| {
        !request.codex_wire || !name.eq_ignore_ascii_case("x-codex-turn-metadata")
    };
    socket
        .headers
        .iter()
        .filter(connection_scoped)
        .eq(request.headers.iter().filter(connection_scoped))
}

struct AttemptActivity(tokio::sync::watch::Sender<bool>);

impl AttemptActivity {
    fn begin(socket: &Socket) -> Self {
        socket.attempt_active.send_replace(true);
        Self(socket.attempt_active.clone())
    }
}

impl Drop for AttemptActivity {
    fn drop(&mut self) {
        self.0.send_replace(false);
    }
}

struct ContinuationAttempt<'a> {
    socket: &'a Socket,
    enabled: bool,
    keep_socket: bool,
}
impl Drop for ContinuationAttempt<'_> {
    fn drop(&mut self) {
        if self.enabled && !self.keep_socket {
            // A dropped consumer or failed stream can leave unread response records.
            // Retire the socket so any remaining records cannot enter the next attempt.
            *self.socket.continuation.lock().unwrap() = None;
            self.socket.closed.store(true, Ordering::Release);
            self.socket.shutdown.cancel();
        }
    }
}

async fn connect(
    request: &PreparedHttpRequest,
    generation: u64,
    established_at: Instant,
) -> Result<(Arc<Socket>, Option<String>), tungstenite::Error> {
    let connecting_at = Instant::now();
    let mut url = reqwest::Url::parse(&request.url)
        .map_err(|_| std::io::Error::other("invalid Responses socket URL"))?;
    let scheme = match url.scheme() {
        "https" => "wss",
        "http" => "ws",
        _ => return Err(std::io::Error::other("Responses socket requires HTTP(S)").into()),
    };
    url.set_scheme(scheme)
        .map_err(|_| std::io::Error::other("invalid socket scheme"))?;
    let mut upgrade = url.as_str().into_client_request()?;
    let headers = if request.codex_wire {
        crate::chatgpt_wire::websocket_headers(&request.headers)
    } else {
        request.headers.clone()
    };
    for (name, value) in &headers {
        let mut value = tungstenite::http::HeaderValue::from_str(value)?;
        value.set_sensitive(true);
        upgrade.headers_mut().insert(
            tungstenite::http::HeaderName::from_bytes(name.as_bytes())?,
            value,
        );
    }
    upgrade.headers_mut().insert("openai-beta", BETA.parse()?);
    if !upgrade.headers().contains_key("thread-id") {
        if let Some(id) = upgrade.headers().get("session-id").cloned() {
            upgrade.headers_mut().insert("thread-id", id);
        }
    }
    if let Some(id) = upgrade.headers().get("thread-id").cloned() {
        upgrade.headers_mut().insert("x-client-request-id", id);
    }
    // Codex builds handshake headers without turn state; it carries the sticky token in
    // response.create.client_metadata once the handshake or response metadata supplies it.
    let mut extensions = ExtensionsConfig::default();
    extensions.permessage_deflate = Some(DeflateConfig::default());
    let mut config = tungstenite::protocol::WebSocketConfig::default();
    config.extensions = extensions;
    use tracing::instrument::WithSubscriber;
    // Dependency trace logs may dump frame or handshake contents. Keep library IO silent;
    // the transport emits only its own payload-free diagnostics outside this scope.
    let (stream, response) = connect_async_with_config(upgrade, Some(config), false)
        .with_subscriber(tracing::subscriber::NoSubscriber::default())
        .await?;
    let turn_state = response
        .headers()
        .get("x-codex-turn-state")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let (send, commands) = mpsc::channel::<SendCommand>(1);
    let (messages, recv) = mpsc::channel(256);
    let shutdown = CancellationToken::new();
    let closed = Arc::new(AtomicBool::new(false));
    let close_code = Arc::new(AtomicU16::new(0));
    let frames = Arc::new(FrameCounts::default());
    let (attempt_active, activity) = tokio::sync::watch::channel(false);
    let socket = Arc::new(Socket {
        auth_revision: request.auth_revision,
        credential_id: request.credential_id.clone(),
        account_ref: request.account_ref.clone(),
        url: request.url.clone(),
        continuation: Mutex::new(None),
        send,
        recv: tokio::sync::Mutex::new(recv),
        shutdown: shutdown.clone(),
        closed: Arc::clone(&closed),
        close_code: Arc::clone(&close_code),
        frames: Arc::clone(&frames),
        headers: request.headers.clone(),
        generation,
        established_at: established_at + connecting_at.elapsed(),
        last_completed_at: Mutex::new(None),
        attempt_active,
    });
    spawn_pump(
        stream,
        commands,
        messages,
        shutdown,
        (closed, close_code, frames),
        activity,
    );
    Ok((socket, turn_state))
}

fn spawn_pump<S>(
    mut stream: tokio_tungstenite::WebSocketStream<S>,
    mut commands: mpsc::Receiver<SendCommand>,
    messages: mpsc::Sender<Result<Vec<u8>, std::io::Error>>,
    shutdown: CancellationToken,
    diagnostics: (Arc<AtomicBool>, Arc<AtomicU16>, Arc<FrameCounts>),
    mut activity: tokio::sync::watch::Receiver<bool>,
) -> JoinHandle<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (closed, close_code, frames) = diagnostics;
    use tracing::instrument::WithSubscriber;
    tokio::spawn(
        async move {
            // Enclose the whole pump, not just reads: a blocked request/Pong write must
            // also be interrupted when the run closes its socket.
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {}
                _ = async {
            loop {
                let message = tokio::select! {
                    biased;
                    command = commands.recv() => {
                        let Some(command) = command else { break };
                        let result = stream.send(Message::Text(command.text.into())).await
                            .map_err(|_| std::io::Error::other("Responses socket send failed"));
                        let failed = result.is_err();
                        if !failed {
                            let count = if command.response_create { &frames.response_create_sent } else { &frames.other_text_sent };
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                        let _ = command.result.send(result);
                        if failed { break }
                        continue;
                    }
                    message = stream.next() => message,
                };
                let record = match message {
                    Some(Ok(Message::Text(text))) => {
                        frames.text_received.fetch_add(1, Ordering::Relaxed);
                        Ok(text.as_bytes().to_vec())
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        frames.pings_received.fetch_add(1, Ordering::Relaxed);
                        match stream.send(Message::Pong(payload)).await {
                            Ok(()) => { frames.pongs_sent.fetch_add(1, Ordering::Relaxed); }
                            Err(_) => break,
                        }
                        Ok(Vec::new())
                    }
                    Some(Ok(Message::Pong(_))) => {
                        frames.pongs_received.fetch_add(1, Ordering::Relaxed);
                        Ok(Vec::new())
                    }
                    Some(Ok(Message::Frame(_))) => continue,
                    Some(Ok(Message::Binary(_))) => {
                        Err(std::io::Error::other("unexpected binary websocket event"))
                    }
                    Some(Ok(Message::Close(frame))) => {
                        frames.closes_received.fetch_add(1, Ordering::Relaxed);
                        if let Some(frame) = frame {
                            close_code.store(u16::from(frame.code), Ordering::Release);
                        }
                        // The peer's reason can contain arbitrary output; only its numeric code is diagnostic.
                        let reason = if close_code.load(Ordering::Acquire) == 1012 {
                            "Responses socket server restart before response.completed"
                        } else {
                            "Responses socket closed before response.completed"
                        };
                        closed.store(true, Ordering::Release);
                        Err(std::io::Error::other(reason))
                    }
                    Some(Err(_)) | None => Err(std::io::Error::other(
                        "Responses socket failed before response.completed",
                    )),
                };
                let failed = record.is_err();
                if record.as_ref().is_ok_and(Vec::is_empty) {
                    // Keepalives refresh an active attempt's idle window, but have no consumer
                    // between attempts. Dropping them there keeps the next send command runnable.
                    if !*activity.borrow_and_update() { continue; }
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break,
                        _ = activity.changed() => continue,
                        result = messages.send(record) => if result.is_err() { break },
                    }
                    continue;
                }
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    result = messages.send(record) => if result.is_err() { break },
                }
                if failed {
                    break;
                }
            }
                } => {}
            }
            closed.store(true, Ordering::Release);
            // Bound both the client close and any automatically queued peer-close reply.
            let _ = tokio::time::timeout(Duration::from_secs(1), async {
                let _ = stream.close(None).await;
                let _ = stream.flush().await;
            })
            .await;
        }
        .with_subscriber(tracing::subscriber::NoSubscriber::default()),
    )
}

pub(super) async fn drive(
    session: &Session,
    client: &ProviderHttpClient,
    request: &PreparedHttpRequest,
    clock: &Clock,
    normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
    trace: &mut AttemptTrace,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    trace.ephemeral_session = request.ephemeral_session;
    if session.forced_https() {
        // A break-glass switch may be turned on between steps. Close the unused socket now.
        if let Some(socket) = session.state.lock().unwrap().socket.take() {
            socket.shutdown.cancel();
        }
        return drive_attempt(
            client,
            request,
            clock,
            normalizer,
            tx,
            STREAM_IDLE_TIMEOUT,
            trace,
        )
        .await;
    }
    let mut observation = ObservationGuard::begin(trace);
    let result = drive_socket(session, client, request, clock, normalizer, tx, trace).await;
    let result = apply_chatgpt_response_deadline(result, request, trace);
    observation.failed =
        result.is_err() || observation.observation.lock().unwrap().frames.is_none();
    result.map_err(|mut failure| {
        if trace.transport.as_deref() != Some("websocket") {
            return failure;
        }
        // An upgrade refused for authentication is not a socket failure: the shared 401 path
        // re-reads the credential and tries the socket again, so it must not count toward the
        // switch to HTTPS for the rest of the run.
        if failure.outcome != AttemptOutcome::HttpError
            || (request.codex_wire && trace.socket_generation.is_some())
        {
            session.fail();
        }
        // Backends can echo authorization headers in error bodies or parser diagnostics.
        // Only typed ceiling evidence has a message built entirely from safe metadata.
        if failure.output_ceiling.is_none() {
            failure.error.message = match trace
                .wrapped_error_frame
                .as_deref()
                .filter(|_| failure.outcome == AttemptOutcome::HttpError)
            {
                Some(frame) => frame.run_error_message(),
                None => format!(
                    "Responses websocket attempt failed ({:?}); response body omitted",
                    failure.outcome
                ),
            };
        }
        failure.error.provider_code = failure
            .error
            .provider_code
            .filter(|code| crate::safe_provider_code(code));
        // Already shape-checked where it was set; checked again because this is the boundary
        // whose rule is that nothing but safe metadata leaves a websocket failure.
        failure.error.provider_subcode = failure
            .error
            .provider_subcode
            .filter(|code| crate::safe_provider_subcode(code));
        if !trace.chunk_error_warned && !trace.defer_failure_warning {
            warn_failure(&failure, trace);
        }
        failure
    })
}

async fn drive_socket(
    session: &Session,
    client: &ProviderHttpClient,
    request: &PreparedHttpRequest,
    clock: &Clock,
    normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
    trace: &mut AttemptTrace,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    trace.transport = Some("websocket".into());
    let (socket, reused) = match session.connection(request).await {
        Ok(connection) => connection,
        Err(UpgradeFailure::AuthRejected(status)) => {
            trace.http_status = Some(status);
            let mut error = transport_error(
                "Responses websocket authentication rejected; upgrade body omitted",
            );
            error.class = ErrorClass::Auth;
            error.status = Some(status);
            error.provider_code = Some("websocket_auth_rejected".into());
            return Err(AttemptFailure::http(error, false));
        }
        Err(UpgradeFailure::AccountLimit(status, error)) => {
            trace.http_status = Some(status);
            trace.account_observation.limit = Some(broca_protocol::AttemptAccountLimit {
                resets_at_ms: error.resets_at_ms,
                resets_at_source: error.resets_at_source.clone(),
                limit_window: error.limit_window.clone(),
            });
            // An exhausted account refills in hours or days: no retry within the step.
            return Err(AttemptFailure::http(*error, true));
        }
        Err(UpgradeFailure::Transport) => {
            return drive_attempt(
                client,
                request,
                clock,
                normalizer,
                tx,
                STREAM_IDLE_TIMEOUT,
                trace,
            )
            .await
        }
    };
    let mut retirement = ContinuationAttempt {
        socket: &socket,
        enabled: request.codex_wire,
        keep_socket: false,
    };
    let now = session.now();
    trace.socket_generation = Some(socket.generation);
    trace.socket_reused = Some(reused);
    trace.socket_age = Some(now.saturating_duration_since(socket.established_at));
    if let Some(observation) = trace.websocket_observation.upgrade() {
        let mut observation = observation.lock().unwrap();
        observation.frames = Some((Arc::clone(&socket.frames), socket.frames.snapshot()));
        observation.close_code = Some(Arc::clone(&socket.close_code));
        observation.generation = trace.socket_generation;
        observation.reused = trace.socket_reused;
        observation.age_ms = trace.socket_age_ms();
    }
    trace.socket_since_last_completed = socket
        .last_completed_at
        .lock()
        .unwrap()
        .map(|completed| now.saturating_duration_since(completed));
    let logical: Value = serde_json::from_slice(&request.body).map_err(|_| {
        AttemptFailure::decode(
            transport_error("invalid rendered Responses socket request"),
            AttemptOutcome::DecodeError,
        )
    })?;
    let mut payload = if request.codex_wire {
        Value::Object(
            logical
                .as_object()
                .unwrap()
                .iter()
                .filter(|(key, _)| !matches!(key.as_str(), "input" | "previous_response_id"))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        )
    } else {
        logical.clone()
    };
    let mut continuation = false;
    let mut full_input_reason = "no_completed_response";
    if request.codex_wire {
        // A completion receipt is one-shot, including when compatibility fails. Never
        // fall back to an older successful response after starting another stream.
        if let Some(previous) = socket.continuation.lock().unwrap().take() {
            match previous.suffix_start(&logical) {
                Ok(start) => {
                    payload["input"] =
                        Value::Array(logical["input"].as_array().unwrap()[start..].to_vec());
                    payload["previous_response_id"] = Value::String(previous.response_id);
                    continuation = true;
                }
                Err(reason) => full_input_reason = reason,
            }
        }
    }
    if payload.get("input").is_none() {
        payload["input"] = logical["input"].clone();
    }
    payload["type"] = Value::String("response.create".into());
    if let Some(turn_state) = session.state.lock().unwrap().turn_state.clone() {
        payload["client_metadata"]["x-codex-turn-state"] = Value::String(turn_state);
    }
    let send_started_at_ms = now_millis();
    if request.codex_wire {
        payload["client_metadata"]["x-codex-ws-stream-request-start-ms"] =
            Value::String(send_started_at_ms.to_string());
    }
    let text = if request.codex_wire {
        String::from_utf8(crate::chatgpt_wire::request_bytes(&payload)).expect("JSON is UTF-8")
    } else {
        serde_json::to_string(&payload).expect("a rendered JSON value serializes")
    };
    let digest = RequestDigest::of(text.as_bytes());
    trace.transport = Some("websocket".into());
    trace.http_status = Some(101);
    trace.websocket_request = Some(Box::new(broca_protocol::provider::WebSocketRequestDigest {
        len: digest.len,
        sha256: digest.sha256,
        codex: request.codex_wire.then(|| {
            Box::new(broca_protocol::provider::ChatgptContinuationTrace {
                continuation,
                suffix_item_count: if continuation {
                    payload["input"]
                        .as_array()
                        .map_or(0, |input| input.len() as u64)
                } else {
                    0
                },
                connection_generation: socket.generation,
                previous_response_id_present: payload.get("previous_response_id").is_some(),
                physical_send_count: 0,
                full_input_reason: (!continuation).then(|| full_input_reason.into()),
            })
        }),
    }));
    trace.websocket_close_code = Some(Arc::clone(&socket.close_code));
    if request.codex_wire {
        trace.websocket_sends = Some(PhysicalSends {
            frames: Arc::clone(&socket.frames),
            before: socket.frames.response_create_sent.load(Ordering::Relaxed),
        });
    }
    if request.codex_wire {
        if let Some(observation) = trace.websocket_observation.upgrade() {
            let mut observation = observation.lock().unwrap();
            let frame = trace.websocket_request.as_ref().unwrap();
            observation.frame_len = Some(frame.len);
            observation.frame_sha256 = Some(frame.sha256.clone());
            observation.continuation = Some(continuation);
            observation.suffix_item_count =
                frame.codex.as_ref().map(|codex| codex.suffix_item_count);
            observation.full_input_reason = (!continuation).then_some(full_input_reason);
        }
    }
    trace.sent_at_ms = Some(if request.codex_wire {
        send_started_at_ms
    } else {
        now_millis()
    });
    trace.sent_at = Some(std::time::Instant::now());
    let _activity = AttemptActivity::begin(&socket);
    let (result, receive) = oneshot::channel();
    let sent = tokio::time::timeout(STREAM_IDLE_TIMEOUT, async {
        socket
            .send
            .send(SendCommand {
                text,
                response_create: true,
                result,
            })
            .await
            .map_err(|_| ())?;
        if let Some(observation) = trace.websocket_observation.upgrade() {
            observation.lock().unwrap().response_create_enqueued += 1;
        }
        receive.await.map_err(|_| ())?.map_err(|_| ())
    })
    .await;
    if !matches!(sent, Ok(Ok(()))) {
        return Err(AttemptFailure::transport(
            transport_error("Responses socket send failed"),
            AttemptOutcome::ChunkError,
        ));
    }
    let completed_id = Mutex::new(None);
    let mut recv = socket.recv.lock().await;
    let stream = futures::stream::unfold(&mut *recv, |recv| async {
        let record = recv.recv().await.unwrap_or_else(|| {
            Err(std::io::Error::other(
                "Responses socket failed before response.completed",
            ))
        });
        if let Ok(raw) = &record {
            if let Ok(value) = serde_json::from_slice(raw) {
                session.observe_turn_state(&value);
                if value["type"] == "response.completed" {
                    *completed_id.lock().unwrap() =
                        value["response"]["id"].as_str().map(str::to_owned);
                }
            }
        }
        Some((record, recv))
    });
    let mut normalizer = normalizer;
    normalizer.private_error_diagnostics();
    // The existing body driver supplies all semantic idle, size, completion and rate bounds.
    // Reframing is confined to its input adapter; the response digest counts raw socket text.
    let result = drive_body_stream_traced(stream, normalizer, tx, MAX_STREAM_BYTES, trace).await;
    if result
        .as_ref()
        .is_ok_and(|completion| tool_args_cut(completion))
    {
        // The provider is still generating this response. Reusing its socket would both
        // keep spending tokens and mix the abandoned response with the next request.
        session.close();
        return result;
    }
    if let Ok(completion) = &result {
        retirement.keep_socket = true;
        if let (Some(render_output), Some(response_id)) = (
            &request.completed_output,
            completed_id.into_inner().unwrap(),
        ) {
            if !response_id.is_empty() {
                if let Some(assistant) = completion.iter().find_map(|event| match event {
                    NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                        Some(message)
                    }
                    _ => None,
                }) {
                    let output = render_output(assistant);
                    *socket.continuation.lock().unwrap() =
                        Some(crate::chatgpt_continuation::CompletedResponse {
                            request: logical,
                            output,
                            response_id,
                        });
                }
            }
        }
    }
    if result
        .as_ref()
        .is_ok_and(|completion| !completion.is_empty())
    {
        *socket.last_completed_at.lock().unwrap() = Some(session.now());
    }
    result
}

fn warn_failure(failure: &AttemptFailure, trace: &AttemptTrace) {
    warn_failure_decision(failure, trace, "not_retryable", "standalone_attempt");
}

pub(super) fn warn_failure_decision(
    failure: &AttemptFailure,
    trace: &AttemptTrace,
    decision: &str,
    reason: &str,
) {
    if trace.ephemeral_session {
        warn_private_failure(&failure.error, "model websocket attempt failed");
        return;
    }
    let cut = failure.output_ceiling.as_ref();
    tracing::warn!(
        target: "provider",
        outcome = ?failure.outcome,
        class = ?failure.error.class,
        provider_code = failure.error.provider_code.as_deref().unwrap_or("none"),
        elapsed_ms = %trace.elapsed_since_send().map(|elapsed| elapsed.as_millis().to_string()).unwrap_or_else(|| "unknown".into()),
        response_bytes = trace.response_len,
        transport = "websocket",
        close_code = %trace.websocket_close_code.as_ref().map(|code| code.load(Ordering::Acquire)).filter(|code| *code != 0).map(|code| code.to_string()).unwrap_or_else(|| "none".into()),
        provider_stop_reason = cut.map(|cut| cut.provider_stop_reason.as_str()),
        max_output_tokens = %cut.and_then(|cut| cut.max_output_tokens).map(|cap| cap.to_string()).unwrap_or_else(|| "unknown".into()),
        output_tokens = cut.and_then(|cut| cut.output_tokens),
        tool_name = cut.map(|cut| cut.tool_name.as_str()),
        argument_bytes = cut.map(|cut| cut.argument_bytes),
        socket_generation = trace.socket_generation,
        socket_reused = trace.socket_reused,
        socket_age_ms = trace.socket_age_ms(),
        socket_since_last_completed_ms = trace.socket_since_last_completed_ms(),
        output_seen = trace.output_seen,
        retry_decision = decision,
        retry_reason = reason,
        error_frame = trace.wrapped_error_frame.as_deref().map(|frame| frame.log_body.as_str()),
        "model websocket attempt failed"
    );
}

// Responses sockets can wrap an HTTP refusal in an error event. This is transport
// metadata, not a second Responses decoder; ordinary event errors stay with the normalizer.
pub(super) fn wrapped_error(raw: &[u8], trace: &mut AttemptTrace) -> Option<AttemptFailure> {
    let value: Value = serde_json::from_slice(raw).ok()?;
    if value.get("type")?.as_str()? != "error" {
        return None;
    }
    let code = value.pointer("/error/code").and_then(Value::as_str);
    if code == Some("websocket_connection_limit_reached") {
        return Some(AttemptFailure::transport(
            transport_error("Responses socket connection limit reached"),
            AttemptOutcome::ChunkError,
        ));
    }
    if code == Some("previous_response_not_found") {
        // Codex 0.163.0 treats this named cache miss as retryable before status mapping,
        // even for a wrapped 400. Failure guards retire the established socket; the normal
        // accounted attempt loop retries with full input, never a resend inside this attempt.
        let retry = [value.pointer("/error/headers"), value.get("headers")]
            .into_iter()
            .flatten()
            .find_map(|source| {
                let mut headers = reqwest::header::HeaderMap::new();
                extend_wrapped_headers(&mut headers, source);
                parse_retry_after_header(&headers)
            });
        let mut error = transport_error("Previous response was not found; retrying full input");
        error.class = ErrorClass::Transient;
        error.provider_code = Some("previous_response_not_found".into());
        error.retry_after_secs = retry;
        return Some(AttemptFailure::transport(error, AttemptOutcome::ChunkError));
    }
    let status = value
        .get("status")
        .or_else(|| value.get("status_code"))?
        .as_u64()?;
    let status = u16::try_from(status).ok()?;
    if !(400..600).contains(&status) {
        return None;
    }
    let mut headers = reqwest::header::HeaderMap::new();
    for source in [value.get("headers"), value.pointer("/error/headers")]
        .into_iter()
        .flatten()
    {
        extend_wrapped_headers(&mut headers, source);
    }
    let retry = parse_retry_after_header(&headers);
    trace.account_observation.ratelimit_headers = ratelimit_unified_headers(&headers);
    let headers = response_headers(&headers);
    let text = String::from_utf8_lossy(raw);
    let family = crate::spec::WireFamilyId::OpenAiResponsesChatgpt;
    trace.wrapped_error_frame = Some(Box::new(WrappedErrorFrame::read(status, &value)));
    let error = classify_http_error(status, &text, retry, None);
    let error = classify_chatgpt_upstream_key_rejection(family, status, &text, error);
    let error =
        classify_account_limit_exhausted(family, status, &headers, &text, SystemTime::now(), error);
    trace.http_status = Some(status);
    if error.is_account_limit_exhausted() {
        trace.account_observation.limit = Some(broca_protocol::AttemptAccountLimit {
            resets_at_ms: error.resets_at_ms,
            resets_at_source: error.resets_at_source.clone(),
            limit_window: error.limit_window.clone(),
        });
    }
    let forbidden = error.is_account_limit_exhausted()
        || headers
            .get("x-should-retry")
            .is_some_and(|value| value.eq_ignore_ascii_case("false"));
    Some(AttemptFailure::http(error, forbidden))
}

/// A provider refusal the Responses websocket wrapped in an error frame (`{"type":"error",
/// "status":400,"error":{...}}`), kept so the attempt's failure can say why without carrying the
/// frame's text.
///
/// The frame answers a `response.create` that was already sent, so its `error.message` can quote
/// the request back: a schema refusal repeats the schema, and backends have echoed credentials.
/// The run error is returned to the caller that started the run, so it gets only
/// [`Self::run_error_message`]: the HTTP status and the frame's short identifier fields, each kept
/// only when it has an identifier's shape, so none of them can hold a sentence of request content.
/// The frame's message text is never part of it.
///
/// A copy of the frame, minus any `headers` object it wraps and cut to
/// [`WRAPPED_FRAME_LOG_LIMIT`] bytes, goes only to broca's own WARN line for the failed attempt
/// (`log_body`). broca's log is written on the machine that ran the request, which already holds
/// the full transcript in its session log, so the frame adds nothing that machine did not have;
/// and it is often the only place the backend's reason (such as a model the account cannot use) is
/// written down. Ephemeral sessions, which promise to retain no content, never log it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct WrappedErrorFrame {
    status: u16,
    code: Option<String>,
    error_type: Option<String>,
    param: Option<String>,
    /// The frame without its `headers` objects, cut to [`WRAPPED_FRAME_LOG_LIMIT`] bytes.
    pub(super) log_body: String,
}

impl WrappedErrorFrame {
    fn read(status: u16, frame: &Value) -> Self {
        let field = |pointer: &str| frame.pointer(pointer).and_then(Value::as_str);
        let identifier = |value: Option<&str>| {
            value
                .filter(|value| crate::safe_provider_subcode(value))
                .map(str::to_owned)
        };
        let mut logged = frame.clone();
        if let Some(object) = logged.as_object_mut() {
            object.remove("headers");
            if let Some(error) = object.get_mut("error").and_then(Value::as_object_mut) {
                error.remove("headers");
            }
        }
        let mut log_body = logged.to_string();
        if log_body.len() > WRAPPED_FRAME_LOG_LIMIT {
            let mut end = WRAPPED_FRAME_LOG_LIMIT;
            while !log_body.is_char_boundary(end) {
                end -= 1;
            }
            log_body.truncate(end);
        }
        Self {
            status,
            code: identifier(field("/error/code")),
            error_type: identifier(field("/error/type")),
            param: field("/error/param")
                .or_else(|| field("/param"))
                .filter(|param| safe_error_param(param))
                .map(str::to_owned),
            log_body,
        }
    }

    /// `Responses websocket attempt failed (HttpError 400); code=<c> type=<t> param=<p>; message
    /// omitted`, naming only the fields the frame had in an identifier's shape. With none, the
    /// message says the body was omitted, as for every other websocket failure.
    pub(super) fn run_error_message(&self) -> String {
        let fields = [
            ("code", &self.code),
            ("type", &self.error_type),
            ("param", &self.param),
        ]
        .into_iter()
        .filter_map(|(name, value)| value.as_ref().map(|value| format!("{name}={value}")))
        .collect::<Vec<_>>();
        if fields.is_empty() {
            format!(
                "Responses websocket attempt failed (HttpError {}); response body omitted",
                self.status
            )
        } else {
            format!(
                "Responses websocket attempt failed (HttpError {}); {}; message omitted",
                self.status,
                fields.join(" ")
            )
        }
    }
}

/// Whether an `error.param` may be named in a run error: 1 to 128 characters, each a lowercase
/// ASCII letter, a digit, `_`, `.`, `[` or `]`. That is the shape of the request path OpenAI puts
/// there (`text.format.schema`, `input[3].content`). It has no space, `:`, `/`, `-` or capital, so
/// it cannot hold a sentence, a URL, a bearer token or a JWT. Any other value is left out.
fn safe_error_param(param: &str) -> bool {
    (1..=128).contains(&param.len())
        && param.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'_' | b'.' | b'[' | b']')
        })
}

fn extend_wrapped_headers(headers: &mut reqwest::header::HeaderMap, source: &Value) {
    if let Some(source) = source.as_object() {
        for (key, value) in source {
            let text = match value {
                Value::String(text) => text.clone(),
                Value::Number(number) => number.to_string(),
                _ => continue,
            };
            if let (Ok(name), Ok(value)) = (
                reqwest::header::HeaderName::from_bytes(key.as_bytes()),
                reqwest::header::HeaderValue::from_str(&text),
            ) {
                headers.insert(name, value);
            }
        }
    }
}

/// Response headers of a refused upgrade that are safe to log: the request ids an operator can
/// quote to the backend's support, the server, the content type, and the Codex backend's own
/// `x-codex-*` headers. Every other header is left out because it may carry a cookie or
/// credential.
fn safe_upgrade_header(name: &str) -> bool {
    matches!(
        name,
        "cf-ray" | "x-request-id" | "request-id" | "server" | "content-type"
    ) || name.starts_with("x-codex-")
}

/// Log one WARN for a refused socket upgrade: its status, what broca does next, and only the
/// headers [`safe_upgrade_header`] allows. The body is never logged; it can echo credentials.
fn warn_upgrade_refusal(status: u16, headers: &reqwest::header::HeaderMap, next: &'static str) {
    let headers = headers
        .iter()
        .filter(|(name, _)| safe_upgrade_header(name.as_str()))
        .filter_map(|(name, value)| {
            let value = value.to_str().ok()?;
            // Bound each value so one oversized header cannot flood the log line.
            let value: String = value.chars().take(128).collect();
            Some(format!("{name}={value}"))
        })
        .collect::<Vec<_>>()
        .join(" ");
    tracing::warn!(
        target: "provider",
        transport = "websocket",
        upgrade_status = status,
        next,
        headers = %headers,
        "Responses websocket upgrade refused"
    );
}

/// Return the ChatGPT account-limit error stated by a refused upgrade's body, if the body states
/// one. Only the first [`UPGRADE_BODY_CLASSIFY_LIMIT`] bytes are classified, the same way as an
/// HTTPS error answer. Any other body returns `None`, and the caller sends the request over HTTPS
/// for the backend to answer there.
fn upgrade_account_limit(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    body: Option<&[u8]>,
) -> Option<ProviderError> {
    let body = body?;
    let text = String::from_utf8_lossy(&body[..body.len().min(UPGRADE_BODY_CLASSIFY_LIMIT)]);
    let error = classify_http_error(status, &text, parse_retry_after_header(headers), None);
    let error = classify_account_limit_exhausted(
        crate::spec::WireFamilyId::OpenAiResponsesChatgpt,
        status,
        &response_headers(headers),
        &text,
        SystemTime::now(),
        error,
    );
    error.is_account_limit_exhausted().then_some(error)
}

pub(super) fn frame_record(raw: &[u8]) -> Vec<u8> {
    if raw.is_empty() {
        return b": keepalive\n\n".to_vec();
    }
    // JSON text may be pretty-printed: one SSE data line per JSON line preserves its value.
    let mut framed = Vec::with_capacity(raw.len() + 16);
    for line in raw.split(|byte| *byte == b'\n') {
        framed.extend_from_slice(b"data: ");
        framed.extend_from_slice(line);
        framed.push(b'\n');
    }
    framed.push(b'\n');
    framed
}

#[cfg(test)]
mod pump_tests {
    use super::*;
    use crate::wire::HttpMethod;

    #[test]
    fn observation_borrows_unescaped_strings_and_skips_unneeded_output() {
        let raw = br#"{"type":"response.function_call_arguments.delta","sequence_number":7,"response_id":"r1","item_id":"i1","output_index":2,"delta":"plain fragment","response":{"id":"r1","output":[{"content":[{"text":"ignored"}]}]},"item":{"id":"i1","arguments":"ignored"},"error":{"code":"ignored","message":"ignored"},"unknown":{"nested":[1,true,null]}}"#;
        let event: ObservationEvent<'_> = serde_json::from_slice(raw).unwrap();
        for text in [
            event.kind,
            event.response_id,
            event.item_id,
            event.delta,
            event.response.unwrap().id,
            event.item.unwrap().id,
            event.error.unwrap().code,
        ]
        .into_iter()
        .flatten()
        {
            assert!(
                matches!(text.0, Cow::Borrowed(_)),
                "unescaped fields must borrow the input"
            );
        }
        let mut observation = AttemptObservation::default();
        observation.observe_text(raw);
        assert_eq!(observation.argument_deltas, 1);
        assert_eq!(observation.argument_delta_bytes, 14);
        assert_eq!(observation.sequence_min, Some(7));
        assert_eq!(observation.output_index_max, Some(2));
        assert_eq!(observation.response_ids.hashes.len(), 1);
        assert_eq!(observation.item_ids.hashes.len(), 1);
    }

    #[test]
    fn observation_decodes_escaped_strings_before_counting_and_hashing() {
        let raw = br#"{"type":"response.function_call_arguments.delta","delta":"\"caf\u00e9\"\n","response_id":"r\u0031","item_id":"i\u0031"}"#;
        let event: ObservationEvent<'_> = serde_json::from_slice(raw).unwrap();
        assert!(matches!(event.delta.unwrap().0, Cow::Owned(_)));
        let mut observation = AttemptObservation::default();
        observation.observe_text(raw);
        observation.observe_text("{\"type\":\"response.function_call_arguments.delta\",\"delta\":\"\\\"café\\\"\\n\",\"response\":{\"id\":\"r1\"},\"item\":{\"id\":\"i1\"}}".as_bytes());
        assert_eq!(observation.argument_deltas, 2);
        assert_eq!(observation.argument_delta_bytes, 16);
        assert_eq!(observation.delta_hashes.hashes.len(), 1);
        assert_eq!(observation.response_ids.hashes.len(), 1);
        assert_eq!(observation.item_ids.hashes.len(), 1);
        observation.observe_text(br#"{"type":"error","error":{"code":"\u0077ebsocket_connection_limit_reached","message":"ignored"}}"#);
        assert_eq!(
            observation.wrapped_error_code,
            Some("websocket_connection_limit_reached")
        );
    }

    #[test]
    #[ignore = "manual optimized-build CPU timing benchmark"]
    fn observation_timing_production_shaped_stream() {
        const EVENTS: usize = 20_000;
        const ROUNDS: usize = 100;
        let records: Vec<_> = (0..EVENTS)
            .map(|index| {
                serde_json::to_vec(&serde_json::json!({
                    "type":"response.function_call_arguments.delta",
                    "sequence_number":index, "response_id":"resp_0123456789",
                    "item_id":"fc_0123456789", "output_index":0,
                    "delta":format!("const value_{index} = await lookup(\"key_{index}\");\n"),
                }))
                .unwrap()
            })
            .collect();
        let bytes: usize = records.iter().map(Vec::len).sum();
        for _ in 0..5 {
            let mut observation = AttemptObservation::default();
            for record in &records {
                observation.observe_text(std::hint::black_box(record));
            }
            std::hint::black_box(observation);
        }
        let started = Instant::now();
        for _ in 0..ROUNDS {
            let mut observation = AttemptObservation::default();
            for record in &records {
                observation.observe_text(std::hint::black_box(record));
            }
            assert_eq!(observation.argument_deltas, EVENTS as u64);
            assert_eq!(observation.sequence_repeated_or_backwards, 0);
            std::hint::black_box(observation);
        }
        let elapsed = started.elapsed();
        println!("observation timing: events={EVENTS} warmup_rounds=5 measured_rounds={ROUNDS} mean_event_bytes={} total_events={} elapsed_ns={} ns_per_event={:.1}",
            bytes / EVENTS, EVENTS * ROUNDS, elapsed.as_nanos(),
            elapsed.as_nanos() as f64 / (EVENTS * ROUNDS) as f64);
    }

    #[test]
    fn fingerprint_memory_is_capped_and_ordered_sequences_are_not_replays() {
        let mut stats = AttemptObservation::default();
        for sequence in 0..(DISTINCT_LIMIT + 10) {
            stats.observe_text(
                serde_json::json!({
                    "type":"response.function_call_arguments.delta",
                    "sequence_number":sequence, "delta":sequence.to_string(),
                    "response_id":sequence.to_string(), "item_id":sequence.to_string()
                })
                .to_string()
                .as_bytes(),
            );
        }
        assert_eq!(stats.sequence_repeated_or_backwards, 0);
        for hashes in [&stats.delta_hashes, &stats.response_ids, &stats.item_ids] {
            assert_eq!(hashes.hashes.len(), DISTINCT_LIMIT);
            assert!(hashes.overflow);
        }
        assert_eq!(stats.argument_deltas, (DISTINCT_LIMIT + 10) as u64);
    }

    #[test]
    fn summary_level_tracks_failure_and_slow_success_and_releases_hashes() {
        let capture = broca_test_support::LogCapture::new();
        for (failed, seconds) in [(true, 0), (false, 121), (false, 0)] {
            let mut trace = AttemptTrace::default();
            capture.in_scope(|| {
                let mut guard = ObservationGuard::begin(&mut trace);
                guard.failed = failed;
                guard.started -= Duration::from_secs(seconds);
            });
            assert!(trace.websocket_observation.upgrade().is_none());
        }
        let events = capture.events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].level, tracing::Level::INFO);
        assert_eq!(events[1].level, tracing::Level::INFO);
        assert_eq!(events[2].level, tracing::Level::DEBUG);
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_interrupts_a_blocked_socket_write() {
        let (io, peer) = tokio::io::duplex(8);
        let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
            io,
            tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let (send, commands) = mpsc::channel(1);
        let (messages, _records) = mpsc::channel(8);
        let shutdown = CancellationToken::new();
        let closed = Arc::new(AtomicBool::new(false));
        let frames = Arc::new(FrameCounts::default());
        let mut task = spawn_pump(
            stream,
            commands,
            messages,
            shutdown.clone(),
            (
                Arc::clone(&closed),
                Arc::new(AtomicU16::new(0)),
                Arc::clone(&frames),
            ),
            tokio::sync::watch::channel(false).1,
        );
        let (result, mut receive) = oneshot::channel();
        send.send(SendCommand {
            text: "test-payload".repeat(4),
            response_create: false,
            result,
        })
        .await
        .unwrap();
        tokio::task::yield_now().await;
        assert!(
            matches!(receive.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "the tiny unread peer keeps the frame write pending"
        );
        assert!(!closed.load(Ordering::Acquire));
        assert_eq!(
            frames.snapshot()[1],
            0,
            "queued Text is not a successful send"
        );
        shutdown.cancel();
        let ended = tokio::time::timeout(Duration::from_secs(2), &mut task).await;
        task.abort();
        drop(peer);
        assert!(
            ended.is_ok(),
            "cancel must interrupt the write, not wait for the peer to read"
        );
        assert!(closed.load(Ordering::Acquire));
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_interrupts_a_blocked_pong_without_counting_it_as_sent() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (io, peer) = tokio::io::duplex(8);
            let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
                io,
                tungstenite::protocol::Role::Client,
                None,
            )
            .await;
            let mut server = tokio_tungstenite::WebSocketStream::from_raw_socket(
                peer,
                tungstenite::protocol::Role::Server,
                None,
            )
            .await;
            let (_send, commands) = mpsc::channel(1);
            let (messages, _records) = mpsc::channel(8);
            let shutdown = CancellationToken::new();
            let frames = Arc::new(FrameCounts::default());
            let (_active, activity) = tokio::sync::watch::channel(true);
            let task = spawn_pump(
                stream,
                commands,
                messages,
                shutdown.clone(),
                (
                    Arc::new(AtomicBool::new(false)),
                    Arc::new(AtomicU16::new(0)),
                    Arc::clone(&frames),
                ),
                activity,
            );
            server
                .send(Message::Ping(vec![1; 125].into()))
                .await
                .unwrap();
            tokio::task::yield_now().await;
            assert_eq!(frames.pings_received.load(Ordering::Relaxed), 1);
            assert_eq!(
                frames.pongs_sent.load(Ordering::Relaxed),
                0,
                "the unread eight-byte peer keeps the Pong send pending"
            );
            shutdown.cancel();
            task.await.unwrap();
            assert_eq!(
                frames.pongs_sent.load(Ordering::Relaxed),
                0,
                "cancelling a pending Pong is not a successful send"
            );
        })
        .await
        .expect("the blocked-Pong fixture must terminate");
    }

    #[tokio::test]
    async fn pump_counts_control_frames_and_other_outgoing_text_separately() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (io, peer) = tokio::io::duplex(4096);
            let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
                io,
                tungstenite::protocol::Role::Client,
                None,
            )
            .await;
            let mut server = tokio_tungstenite::WebSocketStream::from_raw_socket(
                peer,
                tungstenite::protocol::Role::Server,
                None,
            )
            .await;
            let (send, commands) = mpsc::channel(1);
            let (messages, mut records) = mpsc::channel(8);
            let shutdown = CancellationToken::new();
            let frames = Arc::new(FrameCounts::default());
            let close_code = Arc::new(AtomicU16::new(0));
            let (_active, activity) = tokio::sync::watch::channel(true);
            let task = spawn_pump(
                stream,
                commands,
                messages,
                shutdown.clone(),
                (
                    Arc::new(AtomicBool::new(false)),
                    Arc::clone(&close_code),
                    Arc::clone(&frames),
                ),
                activity,
            );
            let (result, received) = oneshot::channel();
            send.send(SendCommand {
                text: "other".into(),
                response_create: false,
                result,
            })
            .await
            .unwrap();
            assert!(matches!(
                server.next().await.unwrap().unwrap(),
                Message::Text(_)
            ));
            received.await.unwrap().unwrap();
            server.send(Message::Ping(vec![1].into())).await.unwrap();
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                Message::Pong(vec![1].into())
            );
            assert!(records.recv().await.unwrap().unwrap().is_empty());
            server.send(Message::Pong(vec![2].into())).await.unwrap();
            assert!(records.recv().await.unwrap().unwrap().is_empty());
            server
                .close(Some(tungstenite::protocol::CloseFrame {
                    code: tungstenite::protocol::frame::coding::CloseCode::Policy,
                    reason: "PRIVATE-REASON".into(),
                }))
                .await
                .unwrap();
            assert!(records.recv().await.unwrap().is_err());
            assert_eq!(frames.snapshot(), [0, 1, 1, 0, 1, 1, 1]);
            assert_eq!(close_code.load(Ordering::Acquire), 1008);
            shutdown.cancel();
            task.await.unwrap();
        })
        .await
        .expect("the control-frame fixture must terminate");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_ping_gap_does_not_block_next_response_create() {
        use crate::families::{openai_responses_chatgpt_frozen_config, OpenAiResponsesChatgpt};
        let (io, peer) = tokio::io::duplex(4096);
        let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
            io,
            tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let mut server = tokio_tungstenite::WebSocketStream::from_raw_socket(
            peer,
            tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        let (send, commands) = mpsc::channel(1);
        let (messages, records) = mpsc::channel(256);
        let shutdown = CancellationToken::new();
        let closed = Arc::new(AtomicBool::new(false));
        let close_code = Arc::new(AtomicU16::new(0));
        let (attempt_active, activity) = tokio::sync::watch::channel(false);
        let socket = Arc::new(Socket {
            auth_revision: 0,
            credential_id: None,
            account_ref: None,
            url: String::new(),
            continuation: Mutex::new(None),
            send,
            recv: tokio::sync::Mutex::new(records),
            shutdown: shutdown.clone(),
            closed: Arc::clone(&closed),
            close_code: Arc::clone(&close_code),
            frames: Arc::new(FrameCounts::default()),
            headers: Vec::new(),
            generation: 1,
            established_at: Instant::now(),
            last_completed_at: Mutex::new(None),
            attempt_active,
        });
        let task = spawn_pump(
            stream,
            commands,
            messages,
            shutdown.clone(),
            (closed, close_code, Arc::clone(&socket.frames)),
            activity,
        );
        let session = Session::default();
        session.state.lock().unwrap().socket = Some(Arc::clone(&socket));
        let completed = Arc::new(tokio::sync::Notify::new());
        let gap_done = Arc::new(tokio::sync::Notify::new());
        let done_gate = Arc::clone(&completed);
        let gap_gate = Arc::clone(&gap_done);
        let peer_task = tokio::spawn(async move {
            let completion = r#"{"type":"response.completed","response":{"id":"r","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}"#;
            let first = server.next().await.unwrap().unwrap();
            assert!(matches!(first, Message::Text(ref text) if text.contains("response.create")));
            server.send(Message::Text(completion.into())).await.unwrap();
            done_gate.notified().await;
            for n in 0..257u16 {
                let payload = n.to_be_bytes().to_vec();
                server
                    .send(Message::Ping(payload.clone().into()))
                    .await
                    .unwrap();
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(1), server.next())
                        .await
                        .expect("every idle Ping must be acknowledged")
                        .unwrap()
                        .unwrap(),
                    Message::Pong(payload.into())
                );
            }
            gap_gate.notify_one();
            let next = server.next().await.unwrap().unwrap();
            assert!(matches!(next, Message::Text(ref text) if text.contains("response.create")));
            server.send(Message::Text(completion.into())).await.unwrap();
            server
        });
        let request: PreparedHttpRequest = HttpRequest {
            method: HttpMethod::Post,
            url: "http://unused.test/responses".into(),
            headers: Vec::new(),
            body: br#"{"model":"m","input":[]}"#.to_vec(),
        }
        .into();
        let client: ProviderHttpClient = reqwest::Client::new().into();
        let clock = Clock::default();
        let (tx, _rx) = mpsc::channel(256);
        for step in 1..=2 {
            let normalizer = OpenAiResponsesChatgpt
                .normalizer(&openai_responses_chatgpt_frozen_config("m"), StepId(step));
            let mut trace = AttemptTrace::default();
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                drive_socket(
                    &session, &client, &request, &clock, normalizer, &tx, &mut trace,
                ),
            )
            .await
            .expect("response.create must complete without reaching the 300s send timeout")
            .unwrap();
            assert!(!result.is_empty());
            assert_eq!(trace.socket_reused, Some(true));
            assert!(!*socket.attempt_active.borrow());
            if step == 1 {
                completed.notify_one();
                tokio::time::timeout(Duration::from_secs(1), gap_done.notified())
                    .await
                    .expect("257 idle Pings must complete");
                assert!(
                    socket.recv.lock().await.is_empty(),
                    "idle keepalives must not accumulate before the next attempt"
                );
            }
        }
        let server = peer_task.await.unwrap();
        assert!(
            socket.recv.lock().await.is_empty(),
            "idle keepalives must not consume receive slots"
        );
        session.close();
        drop(server);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn completed_attempt_interrupts_congested_keepalive_delivery() {
        let (io, peer) = tokio::io::duplex(4096);
        let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
            io,
            tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let mut server = tokio_tungstenite::WebSocketStream::from_raw_socket(
            peer,
            tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        let (send, commands) = mpsc::channel(1);
        let (messages, mut records) = mpsc::channel(1);
        let shutdown = CancellationToken::new();
        let (active, activity) = tokio::sync::watch::channel(true);
        let task = spawn_pump(
            stream,
            commands,
            messages.clone(),
            shutdown.clone(),
            (
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicU16::new(0)),
                Arc::new(FrameCounts::default()),
            ),
            activity,
        );
        server.send(Message::Ping(vec![1].into())).await.unwrap();
        assert_eq!(
            server.next().await.unwrap().unwrap(),
            Message::Pong(vec![1].into())
        );
        assert!(
            records.recv().await.unwrap().unwrap().is_empty(),
            "active keepalive must reach the idle-window driver"
        );
        messages.try_send(Ok(b"completed".to_vec())).unwrap();
        server.send(Message::Ping(vec![2].into())).await.unwrap();
        assert_eq!(
            server.next().await.unwrap().unwrap(),
            Message::Pong(vec![2].into())
        );
        active.send_replace(false);
        let (result, received) = oneshot::channel();
        send.send(SendCommand {
            text: "next".into(),
            response_create: false,
            result,
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), received)
            .await
            .expect("ending an attempt must release congested keepalive delivery")
            .unwrap()
            .unwrap();
        shutdown.cancel();
        drop(server);
        task.await.unwrap();
    }
}
#[test]
fn policy_read_does_not_hold_session_lock() {
    let session = Arc::new(Session::default());
    let (entered, read_started) = std::sync::mpsc::channel();
    let (release, held_read) = std::sync::mpsc::channel();
    let held_read = std::sync::Mutex::new(held_read);
    session.set_policy(Arc::new(move || {
        entered.send(()).unwrap();
        held_read.lock().unwrap().recv().unwrap();
        false
    }));
    let reader = session.clone();
    let task = std::thread::spawn(move || reader.forced_https());
    read_started.recv().unwrap();
    let lock_available = session.state.try_lock().is_ok();
    release.send(()).unwrap();
    assert!(!task.join().unwrap());
    assert!(
        lock_available,
        "transport config read held the session mutex"
    );
    session.set_policy(Arc::new(|| true));
    assert!(
        session.forced_https(),
        "a later attempt must refresh policy"
    );
}
#[tokio::test]
async fn codex_failed_send_retires_socket_and_consumes_receipt() {
    use crate::families::{openai_responses_chatgpt_frozen_config, OpenAiResponsesChatgpt};
    let logical = serde_json::json!({"model":"m","stream":true,"input":[],"tools":[],"tool_choice":"auto","parallel_tool_calls":true,"reasoning":{},"store":false,"include":["reasoning.encrypted_content"]});
    let mut request: PreparedHttpRequest = HttpRequest {
        method: crate::wire::HttpMethod::Post,
        url: "http://127.0.0.1:1/responses".into(),
        headers: vec![],
        body: broca_protocol::canonical_json_bytes(&logical),
    }
    .into();
    request.codex_wire = true;
    let (send, commands) = mpsc::channel(1);
    drop(commands);
    let (_messages, records) = mpsc::channel(1);
    let (attempt_active, _) = tokio::sync::watch::channel(false);
    let socket = Arc::new(Socket {
        auth_revision: 0,
        credential_id: None,
        account_ref: None,
        url: request.url.clone(),
        continuation: Mutex::new(Some(crate::chatgpt_continuation::CompletedResponse {
            request: logical,
            output: vec![],
            response_id: "resp_prior".into(),
        })),
        send,
        recv: tokio::sync::Mutex::new(records),
        shutdown: CancellationToken::new(),
        closed: Arc::new(AtomicBool::new(false)),
        close_code: Arc::new(AtomicU16::new(0)),
        frames: Arc::new(FrameCounts::default()),
        headers: vec![],
        generation: 1,
        established_at: Instant::now(),
        last_completed_at: Mutex::new(None),
        attempt_active,
    });
    let session = Session::default();
    session.state.lock().unwrap().socket = Some(Arc::clone(&socket));
    let (tx, _rx) = mpsc::channel(256);
    let mut trace = AttemptTrace::default();
    let result = drive_socket(
        &session,
        &build_provider_http_client(),
        &request,
        &Clock::default(),
        OpenAiResponsesChatgpt.normalizer(&openai_responses_chatgpt_frozen_config("m"), StepId(1)),
        &tx,
        &mut trace,
    )
    .await;
    assert!(result.is_err());
    assert!(socket.closed.load(Ordering::Acquire));
    assert!(socket.shutdown.is_cancelled());
    assert!(socket.continuation.lock().unwrap().is_none());
    assert_eq!(
        trace
            .websocket_request
            .unwrap()
            .codex
            .unwrap()
            .physical_send_count,
        0
    );
}

#[tokio::test]
async fn codex_completed_write_is_counted_when_cancelled_before_acknowledgement() {
    use crate::families::{openai_responses_chatgpt_frozen_config, OpenAiResponsesChatgpt};
    let logical = serde_json::json!({"model":"m","stream":true,"input":[],"tools":[],"tool_choice":"auto","parallel_tool_calls":true,"reasoning":{},"store":false,"include":["reasoning.encrypted_content"]});
    let mut request: PreparedHttpRequest = HttpRequest {
        method: crate::wire::HttpMethod::Post,
        url: "http://127.0.0.1:1/responses".into(),
        headers: vec![],
        body: broca_protocol::canonical_json_bytes(&logical),
    }
    .into();
    request.codex_wire = true;
    let (send, mut commands) = mpsc::channel(1);
    let (_messages, records) = mpsc::channel(1);
    let (attempt_active, _) = tokio::sync::watch::channel(false);
    let socket = Arc::new(Socket {
        auth_revision: 0,
        credential_id: None,
        account_ref: None,
        url: request.url.clone(),
        continuation: Mutex::new(Some(crate::chatgpt_continuation::CompletedResponse {
            request: logical,
            output: vec![],
            response_id: "resp_prior".into(),
        })),
        send,
        recv: tokio::sync::Mutex::new(records),
        shutdown: CancellationToken::new(),
        closed: Arc::new(AtomicBool::new(false)),
        close_code: Arc::new(AtomicU16::new(0)),
        frames: Arc::new(FrameCounts::default()),
        headers: vec![],
        generation: 1,
        established_at: Instant::now(),
        last_completed_at: Mutex::new(None),
        attempt_active,
    });
    let session = Session::default();
    session.state.lock().unwrap().socket = Some(Arc::clone(&socket));
    let (tx, _rx) = mpsc::channel(256);
    let client = build_provider_http_client();
    let clock = Clock::default();
    let mut trace = AttemptTrace::default();
    let normalizer =
        OpenAiResponsesChatgpt.normalizer(&openai_responses_chatgpt_frozen_config("m"), StepId(1));
    let mut attempt = Box::pin(drive_socket(
        &session, &client, &request, &clock, normalizer, &tx, &mut trace,
    ));
    let command = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut attempt => panic!("send must await its acknowledgement"),
            command = commands.recv() => command.expect("one response.create command"),
        }
    })
    .await
    .expect("request reaches the socket pump");
    // The pump confirms the write but has not delivered its acknowledgement to the attempt.
    socket
        .frames
        .response_create_sent
        .fetch_add(1, Ordering::Relaxed);
    drop(attempt);
    drop(command);
    let record = trace.into_attempt(
        0,
        &RequestDigest::of(&request.body),
        AttemptOutcome::Cancelled,
    );
    assert_eq!(
        record
            .websocket_request
            .unwrap()
            .codex
            .unwrap()
            .physical_send_count,
        1
    );
}
