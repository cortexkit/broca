// Adapted from OpenAI Codex, revision a6baf8867cb4c9726213c0884a5c8b11f0cfd8bf (Apache-2.0).
// Copyright 2025 OpenAI. See THIRD-PARTY-NOTICES.md and LICENSES/Apache-2.0.txt.
// broca integrates run-local sockets with its shared Responses normalizer and HTTPS fallback.

//! Run-local Responses sockets. The pump answers pings even between model steps; it never
//! decodes model output. Text records go through the same Responses normalizer as HTTPS.

use super::*;
use futures::SinkExt;
use std::sync::{atomic::AtomicU16, Mutex, RwLock};
use tokio::sync::oneshot;
use tokio_tungstenite::{connect_async_with_config, tungstenite::client::IntoClientRequest};
use tungstenite::{
    extensions::{compression::deflate::DeflateConfig, ExtensionsConfig},
    Message,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
// Codex's default stream budget is five retries after the initial request. Retain its
// six-failure threshold across this run, including upgrades that fell back before sending.
const FAILURE_LIMIT: usize = 6;
// Retire before the backend's 60-minute connection lifetime, not in the middle of a response.
const MAX_SOCKET_AGE: Duration = Duration::from_secs(55 * 60);
const BETA: &str = "responses_websockets=2026-02-06";

enum UpgradeFailure {
    AuthRejected(u16),
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
    pub(super) fn set_policy(&self, policy: HttpsPolicy) {
        *self.policy.write().unwrap() = policy;
    }

    pub(super) fn close(&self) {
        let mut state = self.state.lock().unwrap();
        if let Some(socket) = state.socket.take() {
            socket.shutdown.cancel();
        }
        *state = State::default();
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

    fn forced_https(&self) -> bool {
        let policy = Arc::clone(&self.policy.read().unwrap());
        let state = self.state.lock().unwrap();
        policy() || state.force_https || state.failures >= FAILURE_LIMIT
    }

    async fn connection(
        &self,
        request: &PreparedHttpRequest,
    ) -> Result<(Arc<Socket>, bool), UpgradeFailure> {
        let now = self.now();
        {
            let mut state = self.state.lock().unwrap();
            if let Some(socket) = &state.socket {
                if !socket.closed.load(Ordering::Acquire)
                    && socket.headers == request.headers
                    && now.saturating_duration_since(socket.established_at) < MAX_SOCKET_AGE
                {
                    return Ok((Arc::clone(socket), true));
                }
                socket.shutdown.cancel();
                let auth_changed = socket.headers != request.headers;
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
            Ok(Err(tungstenite::Error::Http(response)))
                if matches!(response.status().as_u16(), 401 | 403) =>
            {
                // An auth refusal must reach the shared re-read/pause path, not send stale
                // credentials again over HTTPS. Do not retain the upgrade body or headers.
                Err(UpgradeFailure::AuthRejected(response.status().as_u16()))
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
    result: oneshot::Sender<Result<(), std::io::Error>>,
}

struct Socket {
    send: mpsc::Sender<SendCommand>,
    recv: tokio::sync::Mutex<mpsc::Receiver<Result<Vec<u8>, std::io::Error>>>,
    shutdown: CancellationToken,
    closed: Arc<AtomicBool>,
    close_code: Arc<AtomicU16>,
    headers: Vec<(String, String)>,
    generation: u64,
    established_at: Instant,
    last_completed_at: Mutex<Option<Instant>>,
    attempt_active: tokio::sync::watch::Sender<bool>,
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
    for (name, value) in &request.headers {
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
    let (attempt_active, activity) = tokio::sync::watch::channel(false);
    let socket = Arc::new(Socket {
        send,
        recv: tokio::sync::Mutex::new(recv),
        shutdown: shutdown.clone(),
        closed: Arc::clone(&closed),
        close_code: Arc::clone(&close_code),
        headers: request.headers.clone(),
        generation,
        established_at: established_at + connecting_at.elapsed(),
        last_completed_at: Mutex::new(None),
        attempt_active,
    });
    spawn_pump(
        stream, commands, messages, shutdown, closed, close_code, activity,
    );
    Ok((socket, turn_state))
}

fn spawn_pump<S>(
    mut stream: tokio_tungstenite::WebSocketStream<S>,
    mut commands: mpsc::Receiver<SendCommand>,
    messages: mpsc::Sender<Result<Vec<u8>, std::io::Error>>,
    shutdown: CancellationToken,
    closed: Arc<AtomicBool>,
    close_code: Arc<AtomicU16>,
    mut activity: tokio::sync::watch::Receiver<bool>,
) -> JoinHandle<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
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
                        let _ = command.result.send(result);
                        if failed { break }
                        continue;
                    }
                    message = stream.next() => message,
                };
                let record = match message {
                    Some(Ok(Message::Text(text))) => Ok(text.as_bytes().to_vec()),
                    Some(Ok(Message::Ping(payload))) => {
                        if stream.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                        Ok(Vec::new())
                    }
                    Some(Ok(Message::Pong(_))) => Ok(Vec::new()),
                    Some(Ok(Message::Frame(_))) => continue,
                    Some(Ok(Message::Binary(_))) => {
                        Err(std::io::Error::other("unexpected binary websocket event"))
                    }
                    Some(Ok(Message::Close(frame))) => {
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
    client: &reqwest::Client,
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
    let result = drive_socket(session, client, request, clock, normalizer, tx, trace).await;
    let result = apply_chatgpt_response_deadline(result, request, trace);
    result.map_err(|mut failure| {
        if trace.transport.as_deref() != Some("websocket") {
            return failure;
        }
        // An upgrade refused for authentication is not a socket failure: the shared 401 path
        // re-reads the credential and tries the socket again, so it must not count toward the
        // switch to HTTPS for the rest of the run.
        if failure.outcome != AttemptOutcome::HttpError {
            session.fail();
        }
        // Backends can echo authorization headers in error bodies or parser diagnostics.
        // Only typed ceiling evidence has a message built entirely from safe metadata.
        if failure.output_ceiling.is_none() {
            failure.error.message = format!(
                "Responses websocket attempt failed ({:?}); response body omitted",
                failure.outcome
            );
        }
        failure.error.provider_code = failure
            .error
            .provider_code
            .filter(|code| crate::safe_provider_code(code));
        if !trace.chunk_error_warned && !trace.defer_failure_warning {
            warn_failure(&failure, trace);
        }
        failure
    })
}

async fn drive_socket(
    session: &Session,
    client: &reqwest::Client,
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
    let now = session.now();
    trace.socket_generation = Some(socket.generation);
    trace.socket_reused = Some(reused);
    trace.socket_age = Some(now.saturating_duration_since(socket.established_at));
    trace.socket_since_last_completed = socket
        .last_completed_at
        .lock()
        .unwrap()
        .map(|completed| now.saturating_duration_since(completed));
    let mut payload: Value = serde_json::from_slice(&request.body).map_err(|_| {
        AttemptFailure::decode(
            transport_error("invalid rendered Responses socket request"),
            AttemptOutcome::DecodeError,
        )
    })?;
    payload["type"] = Value::String("response.create".into());
    if let Some(turn_state) = session.state.lock().unwrap().turn_state.clone() {
        payload["client_metadata"]["x-codex-turn-state"] = Value::String(turn_state);
    }
    let text = serde_json::to_string(&payload).expect("a rendered JSON value serializes");
    let digest = RequestDigest::of(text.as_bytes());
    trace.transport = Some("websocket".into());
    trace.http_status = Some(101);
    trace.websocket_request = Some(Box::new(broca_protocol::provider::WebSocketRequestDigest {
        len: digest.len,
        sha256: digest.sha256,
    }));
    trace.websocket_close_code = Some(Arc::clone(&socket.close_code));
    trace.sent_at_ms = Some(now_millis());
    trace.sent_at = Some(std::time::Instant::now());
    let _activity = AttemptActivity::begin(&socket);
    let (result, receive) = oneshot::channel();
    let sent = tokio::time::timeout(STREAM_IDLE_TIMEOUT, async {
        socket
            .send
            .send(SendCommand { text, result })
            .await
            .map_err(|_| ())?;
        receive.await.map_err(|_| ())?.map_err(|_| ())
    })
    .await;
    if !matches!(sent, Ok(Ok(()))) {
        return Err(AttemptFailure::transport(
            transport_error("Responses socket send failed"),
            AttemptOutcome::ChunkError,
        ));
    }
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
    let retry = parse_retry_after_header(&headers);
    trace.account_observation.ratelimit_headers = ratelimit_unified_headers(&headers);
    let headers = response_headers(&headers);
    let text = String::from_utf8_lossy(raw);
    let family = crate::spec::WireFamilyId::OpenAiResponsesChatgpt;
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
        let mut task = spawn_pump(
            stream,
            commands,
            messages,
            shutdown.clone(),
            Arc::clone(&closed),
            Arc::new(AtomicU16::new(0)),
            tokio::sync::watch::channel(false).1,
        );
        let (result, mut receive) = oneshot::channel();
        send.send(SendCommand {
            text: "test-payload".repeat(4),
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
            send,
            recv: tokio::sync::Mutex::new(records),
            shutdown: shutdown.clone(),
            closed: Arc::clone(&closed),
            close_code: Arc::clone(&close_code),
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
            closed,
            close_code,
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
        let client = reqwest::Client::new();
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
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU16::new(0)),
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
