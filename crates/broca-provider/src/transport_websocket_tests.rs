use super::*;
use crate::families::{openai_responses_chatgpt_frozen_config, OpenAiResponsesChatgpt};
use broca_core::{
    lease::FileLeaseStore, run_loop, AllowAll, FileWalStore, RunRequest, StubToolPlane, WalStore,
};
use broca_protocol::{
    content::{ContentBlock, Message, Role},
    BindIdentity, ModelHandle, RunFinishReason, RunId, StopCondition,
};
use broca_wal::WalRecord;
use futures::SinkExt;
use std::{collections::BTreeMap, sync::Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tungstenite::{
    handshake::server::{Request, Response},
    Message as WsMessage,
};

fn text_events() -> Vec<Value> {
    vec![
        serde_json::json!({"type":"response.output_text.delta","delta":"done"}),
        serde_json::json!({"type":"response.completed","response":{"id":"r","status":"completed","usage":{"input_tokens":12,"output_tokens":1,"input_tokens_details":{"cached_tokens":8}}}}),
    ]
}
fn tool_events() -> Vec<Value> {
    vec![
        serde_json::json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc","call_id":"call1","name":"lookup","arguments":""}}),
        serde_json::json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"x\":1}"}),
        serde_json::json!({"type":"response.completed","response":{"id":"r1","status":"completed","usage":{"input_tokens":10,"output_tokens":2}}}),
    ]
}

const CUT_ARGS: &str = r#"{"content":"PRIVATE-TOOL-CONTENT"#;

fn cut_tool_events(ceiling: bool) -> Vec<Value> {
    vec![
        serde_json::json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc","call_id":"call1","name":"write_file","arguments":""}}),
        serde_json::json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":CUT_ARGS}),
        serde_json::json!({"type":if ceiling {"response.incomplete"} else {"response.completed"},"response":{"id":"r","status":if ceiling {"incomplete"} else {"completed"},"incomplete_details":if ceiling {serde_json::json!({"reason":"max_output_tokens"})} else {Value::Null},"usage":{"input_tokens":12,"output_tokens":4096}}}),
    ]
}

#[derive(Default, Debug)]
struct Observed {
    handshakes: usize,
    headers: Vec<BTreeMap<String, String>>,
    upgrade_bytes: Vec<Vec<u8>>,
    requests: Vec<Vec<u8>>,
    http_bodies: Vec<Vec<u8>>,
    http_headers: Vec<String>,
    closes: usize,
    held_cuts: usize,
}

struct CaptureUpgrade<S> {
    inner: S,
    bytes: Arc<Mutex<Vec<u8>>>,
}
impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for CaptureUpgrade<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &result {
            let mut captured = self.bytes.lock().unwrap();
            if !captured.windows(4).any(|part| part == b"\r\n\r\n") {
                captured.extend_from_slice(&buf.filled()[before..]);
            }
        }
        result
    }
}
impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for CaptureUpgrade<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
struct Mock {
    url: String,
    observed: Arc<Mutex<Observed>>,
    shutdown: CancellationToken,
    changed: Arc<tokio::sync::Notify>,
    cut_gates: Arc<tokio::sync::Semaphore>,
    /// When set, every upgrade is refused with this status, headers and body.
    upgrade_refusal: Arc<Mutex<Option<UpgradeRefusal>>>,
    /// When set, every HTTPS request is answered with this error status and JSON body.
    http_error: Arc<Mutex<Option<(u16, String)>>>,
}
#[derive(Clone)]
struct UpgradeRefusal {
    status: u16,
    headers: Vec<(&'static str, &'static str)>,
    body: String,
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
impl Mock {
    async fn start(replies: Vec<Vec<Value>>, close_after: bool, reject: bool) -> Self {
        Self::start_internal(replies, close_after, reject, None, false).await
    }
    async fn start_gated(replies: Vec<Vec<Value>>) -> Self {
        Self::start_internal(replies, true, false, None, true).await
    }
    async fn start_internal(
        replies: Vec<Vec<Value>>,
        close_after: bool,
        reject: bool,
        auth_refusal: Option<u16>,
        hold_cuts: bool,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let observed = Arc::new(Mutex::new(Observed::default()));
        let queue = Arc::new(Mutex::new(std::collections::VecDeque::from(replies)));
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let seen = Arc::clone(&observed);
        let changed = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&changed);
        let rejected = Arc::new(AtomicBool::new(false));
        let cut_gates = Arc::new(tokio::sync::Semaphore::new(0));
        let gates = Arc::clone(&cut_gates);
        let upgrade_refusal = Arc::new(Mutex::new(None::<UpgradeRefusal>));
        let http_error = Arc::new(Mutex::new(None::<(u16, String)>));
        let (refusals, http_errors) = (Arc::clone(&upgrade_refusal), Arc::clone(&http_error));
        tokio::spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    accepted = listener.accept() => {
                        let (mut tcp, _) = accepted.unwrap();
                        let seen = Arc::clone(&seen);
                        let queue = Arc::clone(&queue);
                        let notify = Arc::clone(&notify);
                        let rejected = Arc::clone(&rejected);
                        let gates = Arc::clone(&gates);
                        let refusal = refusals.lock().unwrap().clone();
                        let http_error = http_errors.lock().unwrap().clone();
                        clients.spawn(async move {
                            let mut prefix = [0; 4];
                            tcp.peek(&mut prefix).await.unwrap();
                            if prefix.starts_with(b"POST") {
                                let mut header = Vec::new();
                                while !header.ends_with(b"\r\n\r\n") {
                                    header.push(tcp.read_u8().await.unwrap());
                                }
                                 let header = String::from_utf8(header).unwrap();
                                 seen.lock().unwrap().http_headers.push(header.clone());
                                let length = header.lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length: ").map(|value| value.parse::<usize>().unwrap())).unwrap();
                                let mut body = vec![0; length];
                                tcp.read_exact(&mut body).await.unwrap();
                                seen.lock().unwrap().http_bodies.push(body);
                                if let Some((status, body)) = http_error {
                                    tcp.write_all(format!("HTTP/1.1 {status} Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                                    return;
                                }
                                let events = queue.lock().unwrap().pop_front().unwrap_or_else(text_events);
                                let sse: String = events.iter().map(|event| format!("data: {event}\n\n")).collect();
                                tcp.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}", sse.len()).as_bytes()).await.unwrap();
                                return;
                            }
                            let capture = Arc::clone(&seen);
                            let mut config = tungstenite::protocol::WebSocketConfig::default();
                            config.extensions.permessage_deflate = Some(tungstenite::extensions::compression::deflate::DeflateConfig::default());
                             let raw = Arc::new(Mutex::new(Vec::new()));
                             let tcp = CaptureUpgrade { inner: tcp, bytes: Arc::clone(&raw) };
                             let upgrade = tokio_tungstenite::accept_hdr_async_with_config(tcp, move |request: &Request, response: Response| {
                                let headers = request.headers().iter().map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned())).collect();
                                let mut observed = capture.lock().unwrap();
                                observed.handshakes += 1;
                                observed.headers.push(headers);
                                if reject {
                                    return Err(tungstenite::http::Response::builder().status(503).body(Some("upgrade refused".into())).unwrap());
                                }
                                if let Some(refusal) = &refusal {
                                    let mut response = tungstenite::http::Response::builder().status(refusal.status);
                                    for (name, value) in &refusal.headers {
                                        response = response.header(*name, *value);
                                    }
                                    return Err(response.body(Some(refusal.body.clone())).unwrap());
                                }
                                if let Some(status) = auth_refusal.filter(|_| !rejected.swap(true, Ordering::SeqCst)) {
                                    return Err(tungstenite::http::Response::builder().status(status).body(Some("PRIVATE-UPGRADE-BODY Bearer mock-token".into())).unwrap());
                                }
                                Ok(response)
                             }, Some(config)).await;
                             seen.lock().unwrap().upgrade_bytes.push(raw.lock().unwrap().clone());
                            let Ok(mut socket) = upgrade else { return; };
                            while let Some(Ok(message)) = socket.next().await {
                                match message {
                                    WsMessage::Text(text) => {
                                        seen.lock().unwrap().requests.push(text.as_bytes().to_vec());
                                        notify.notify_one();
                                         let events = queue.lock().unwrap().pop_front().unwrap_or_else(text_events);
                                         let incomplete = !events.iter().any(|event| event["type"] == "response.completed");
                                         for event in events { if socket.send(WsMessage::Text(event.to_string().into())).await.is_err() { return; } }
                                         if hold_cuts && incomplete {
                                             seen.lock().unwrap().held_cuts += 1;
                                             notify.notify_one();
                                             gates.acquire().await.unwrap().forget();
                                         }
                                        if close_after {
                                            let _ = socket.close(Some(tungstenite::protocol::CloseFrame { code: tungstenite::protocol::frame::coding::CloseCode::Restart, reason: "PRIVATE-CLOSE-REASON".into() })).await;
                                            return;
                                        }
                                    }
                                    WsMessage::Close(_) => {
                                        seen.lock().unwrap().closes += 1;
                                        notify.notify_one();
                                        let _ = socket.flush().await;
                                        return;
                                    }
                                    _ => {}
                                }
                            }
                        });
                    }
                }
            }
            clients.abort_all();
        });
        Self {
            url,
            observed,
            shutdown,
            changed,
            cut_gates,
            upgrade_refusal,
            http_error,
        }
    }
    async fn wait_closes(&self, n: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.changed.notified();
                if self.observed.lock().unwrap().closes >= n {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the run closes its socket before the bounded wait ends"));
    }
    async fn wait_request(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.changed.notified();
                if !self.observed.lock().unwrap().requests.is_empty() {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("mock receives response.create");
    }
    async fn wait_held_cut(&self, n: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.changed.notified();
                if self.observed.lock().unwrap().held_cuts >= n {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("mock holds the scripted stream cut");
    }
}
fn provider(mock: &Mock) -> LiveProvider<OpenAiResponsesChatgpt> {
    let mut frozen = openai_responses_chatgpt_frozen_config("m");
    frozen.base_url = Some(mock.url.clone());
    LiveProvider::new(
        OpenAiResponsesChatgpt,
        frozen,
        AuthHandle::Headers(vec![
            ("authorization".into(), "Bearer mock-token".into()),
            ("chatgpt-account-id".into(), "account".into()),
            ("session-id".into(), "session".into()),
            ("originator".into(), "codex_cli_rs".into()),
            ("user-agent".into(), "codex_cli_rs/0.144.0".into()),
        ]),
    )
}

fn codex_provider(mock: &Mock) -> LiveProvider<OpenAiResponsesChatgpt> {
    let mut provider = provider(mock);
    let mut frozen = (*provider.frozen).clone();
    frozen.chatgpt_wire = Some(
        serde_json::from_str(include_str!(
            "../tests/fixtures/chatgpt_codex_0_163_identity.json"
        ))
        .unwrap(),
    );
    frozen.prompt_cache_key = Some("caller-cache-key".into());
    provider.frozen = Arc::new(frozen);
    provider
}

#[tokio::test]
async fn codex_profile_headers_are_observed_from_real_upgrade() {
    let mock = Mock::start(vec![text_events()], false, false).await;
    let provider = codex_provider(&mock);
    let events = step(&provider, 1, &opts()).await;
    assert!(attempts(&events)
        .iter()
        .all(|a| a.outcome == AttemptOutcome::Complete));
    let observed = mock.observed.lock().unwrap();
    let raw = String::from_utf8(observed.upgrade_bytes[0].clone()).unwrap();
    let lines: Vec<_> = raw.split("\r\n").collect();
    for expected in [
        "originator: codex_cli_rs",
        "user-agent: codex_cli_rs/0.163.0 (Mac OS 26.0.1; arm64) Apple_Terminal/455",
        "thread-id: 11111111-2222-5333-8444-555555555555",
        "x-client-request-id: 11111111-2222-5333-8444-555555555555",
        "session-id: caller-cache-key",
        "x-codex-window-id: 12345678-1234-4234-8234-123456789abc",
        "openai-beta: responses_websockets=2026-02-06",
        "chatgpt-account-id: account",
        "authorization: Bearer mock-token",
    ] {
        assert!(
            lines.contains(&expected),
            "missing {expected:?} in captured upgrade {raw:?}"
        );
    }
    assert!(!lines
        .iter()
        .any(|line| line.starts_with("accept:") || line.starts_with("content-type:")));
    let names: Vec<_> = lines
        .iter()
        .skip(1)
        .filter_map(|line| line.split_once(": ").map(|(key, _)| key))
        .collect();
    let expected_body: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/chatgpt_codex_0_163_plain.json"
    ))
    .unwrap();
    let expected_metadata = format!(
        "x-codex-turn-metadata: {}",
        expected_body["client_metadata"]["x-codex-turn-metadata"]
            .as_str()
            .unwrap()
    );
    assert!(lines.contains(&expected_metadata.as_str()));
    assert_eq!(
        names,
        [
            "Host",
            "Connection",
            "Upgrade",
            "Sec-WebSocket-Version",
            "Sec-WebSocket-Key",
            "chatgpt-account-id",
            "authorization",
            "user-agent",
            "openai-beta",
            "x-codex-turn-metadata",
            "originator",
            "x-client-request-id",
            "session-id",
            "thread-id",
            "x-codex-window-id",
            "sec-websocket-extensions"
        ]
    );
    let canonical = provider.render_request(&opts());
    let attempt = attempts(&events)[0];
    assert_eq!(
        attempt.request_body_sha256,
        RequestDigest::of(&canonical).sha256
    );
    assert!(observed.requests[0]
        .starts_with(b"{\"type\":\"response.create\",\"model\":\"m\",\"stream\":true,\"input\":"));
}

#[tokio::test]
async fn codex_profile_https_orders_wire_without_changing_canonical_digest() {
    let mock = Mock::start(vec![text_events()], false, true).await;
    let provider = codex_provider(&mock);
    let canonical = provider.render_request(&opts());
    let events = step(&provider, 1, &opts()).await;
    let observed = mock.observed.lock().unwrap();
    assert_eq!(observed.http_bodies.len(), 1);
    assert!(observed.http_headers[0]
        .contains("x-client-request-id: 11111111-2222-5333-8444-555555555555\r\n"));
    assert!(observed.http_bodies[0].starts_with(b"{\"model\":\"m\",\"stream\":true,\"input\":"));
    assert_eq!(
        serde_json::from_slice::<Value>(&observed.http_bodies[0]).unwrap(),
        serde_json::from_slice::<Value>(&canonical).unwrap()
    );
    assert_eq!(
        attempts(&events)[0].request_body_sha256,
        RequestDigest::of(&canonical).sha256
    );
}

#[tokio::test]
async fn codex_profile_refresh_preserves_frozen_identity() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let refresh = Arc::new(RefreshScript {
        expiring: Mutex::new(std::collections::VecDeque::from([
            None,
            Some(fresh_credential("fresh-token", "account", "expiring")),
        ])),
        rejected: Mutex::new(None),
        rereads: AtomicUsize::new(0),
    });
    let provider = codex_provider(&mock)
        .with_account_refs(Some("account".into()), Some("account".into()))
        .with_credential_refresher(refresh);
    let first = step(&provider, 1, &opts()).await;
    let second = step(&provider, 2, &opts()).await;
    assert!(attempts(&second)[0].credential_refresh.is_some());
    assert_eq!(
        attempts(&first)[0].request_body_sha256,
        attempts(&second)[0].request_body_sha256
    );
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 2);
    assert_eq!(seen.headers[1]["authorization"], "Bearer fresh-token");
    for name in [
        "originator",
        "user-agent",
        "thread-id",
        "x-client-request-id",
        "session-id",
        "x-codex-window-id",
        "x-codex-turn-metadata",
    ] {
        assert_eq!(seen.headers[0][name], seen.headers[1][name], "{name}");
    }
    let mut first_body: Value = serde_json::from_slice(&seen.requests[0]).unwrap();
    let mut second_body: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
    for (body, events) in [(&mut first_body, &first), (&mut second_body, &second)] {
        let sent = attempts(events)[0].sent_at_ms.unwrap().to_string();
        assert_eq!(
            body["client_metadata"]["x-codex-ws-stream-request-start-ms"],
            sent
        );
        body["client_metadata"]
            .as_object_mut()
            .unwrap()
            .remove("x-codex-ws-stream-request-start-ms");
    }
    assert_eq!(
        first_body["client_metadata"],
        second_body["client_metadata"]
    );
}

fn follow_up(mut options: CallOptions, events: &[NormalizedEvent], text: &str) -> CallOptions {
    let assistant = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("successful canonical assistant output");
    let mut message = Message::new(Role::Assistant, assistant.content.clone());
    message.origin = options.target_model.clone();
    options.prompt.push(message);
    options.prompt.push(Message::new(
        Role::User,
        vec![ContentBlock::Text { text: text.into() }],
    ));
    options
}

fn codex_trace(
    events: &[NormalizedEvent],
    index: usize,
) -> &broca_protocol::provider::ChatgptContinuationTrace {
    attempts(events)[index]
        .websocket_request
        .as_ref()
        .unwrap()
        .codex
        .as_deref()
        .unwrap()
}

fn assert_canonical(attempt: &ModelAttempt, canonical: &[u8]) {
    assert_eq!(attempt.request_body_len, canonical.len() as u64);
    let sha: String = Sha256::digest(canonical)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(attempt.request_body_sha256, sha);
}

#[tokio::test]
async fn codex_continuation_large_canonical_request_has_small_suffix_and_real_stamp() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let provider = codex_provider(&mock);
    let mut initial = opts();
    initial.prompt[0].content = vec![ContentBlock::Text {
        text: "deterministic cache prefix. ".repeat(150_000),
    }];
    let first = step(&provider, 1, &initial).await;
    let next = follow_up(initial, &first, "Next question");
    let canonical = provider.render_request(&next);
    assert!(canonical.len() > 3 * 1024 * 1024);
    let before = now_millis();
    let second = step(&provider, 2, &next).await;
    let after = now_millis();
    assert_eq!(attempts(&second).len(), 1);
    assert_canonical(attempts(&second)[0], &canonical);
    let codex = codex_trace(&second, 0);
    assert!(codex.continuation);
    assert_eq!(codex.suffix_item_count, 1);
    assert_eq!(codex.physical_send_count, 1);
    assert!(codex.connection_generation > 0);
    assert!(codex.previous_response_id_present);
    assert_eq!(codex.full_input_reason, None);
    assert_eq!(
        codex.connection_generation,
        codex_trace(&first, 0).connection_generation
    );
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 1);
    assert_eq!(seen.requests.len(), 2);
    assert!(seen.requests[1].len() < 2048);
    let frame: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
    assert_eq!(frame["previous_response_id"], "r");
    assert_eq!(
        frame["input"],
        serde_json::json!([{"type":"message","role":"user","content":[{"type":"input_text","text":"Next question"}]}])
    );
    let stamp = frame["client_metadata"]["x-codex-ws-stream-request-start-ms"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!(stamp >= before && stamp <= after);
    assert_eq!(Some(stamp), attempts(&second)[0].sent_at_ms);
    assert!(
        serde_json::from_slice::<Value>(&canonical).unwrap()["client_metadata"]
            .get("x-codex-ws-stream-request-start-ms")
            .is_none()
    );
    let digest = attempts(&second)[0].websocket_request.as_ref().unwrap();
    assert_eq!(digest.len, seen.requests[1].len() as u64);
    let sha: String = Sha256::digest(&seen.requests[1])
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(digest.sha256, sha);
}

#[tokio::test]
async fn codex_continuation_under_retirement_age_reuses_socket() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let provider = codex_provider(&mock);
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "Next");
    let canonical = provider.render_request(&next);
    provider
        .websocket
        .advance_clock(Duration::from_secs(40 * 60 - 1));
    let second = step(&provider, 2, &next).await;
    assert_eq!(attempts(&second).len(), 1);
    assert_eq!(attempts(&second)[0].outcome, AttemptOutcome::Complete);
    assert_canonical(attempts(&second)[0], &canonical);
    let codex = codex_trace(&second, 0);
    assert!(codex.continuation);
    assert_eq!(codex.suffix_item_count, 1);
    assert!(codex.previous_response_id_present);
    assert_eq!(
        codex.connection_generation,
        codex_trace(&first, 0).connection_generation
    );
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 1);
    assert_eq!(seen.closes, 0);
    assert_eq!(seen.requests.len(), 2);
    let frame: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
    assert_eq!(frame["previous_response_id"], "r");
    assert_eq!(
        frame["input"],
        serde_json::json!([{"type":"message","role":"user","content":[{"type":"input_text","text":"Next"}]}])
    );
}

#[tokio::test]
async fn codex_continuation_equal_input_sends_empty_suffix() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let provider = codex_provider(&mock);
    let first = step(&provider, 1, &opts()).await;
    let mut next = follow_up(opts(), &first, "unused");
    next.prompt.pop();
    let second = step(&provider, 2, &next).await;
    assert!(codex_trace(&second, 0).continuation);
    assert_eq!(codex_trace(&second, 0).suffix_item_count, 0);
    let seen = mock.observed.lock().unwrap();
    let frame: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
    assert_eq!(frame["input"], serde_json::json!([]));
    assert_eq!(frame["previous_response_id"], "r");
}

async fn codex_reset_case(case: &str) {
    let close_after = case == "closed";
    let mock = Mock::start(vec![text_events(), text_events()], close_after, false).await;
    let mut provider = codex_provider(&mock);
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "Next");
    let canonical = provider.render_request(&next);
    match case {
        "headers" => {
            let mut current = provider.auth.write().unwrap();
            let AuthHandle::Headers(headers) = Arc::make_mut(&mut current.auth) else {
                panic!("fixture headers")
            };
            headers.push(("x-test-rotation".into(), "new".into()));
        }
        "auth_revision" => provider.auth.write().unwrap().revision += 1,
        "retire" => provider.close_run_transport(),
        "resume" => {
            provider.close_run_transport();
            provider = codex_provider(&mock);
        }
        "closed" => provider.websocket.wait_closed().await,
        _ => panic!("unknown reset case"),
    }
    let second = step(&provider, 2, &next).await;
    assert_eq!(
        attempts(&second).len(),
        1,
        "reset must precede send for {case}"
    );
    assert_canonical(attempts(&second)[0], &canonical);
    assert!(!codex_trace(&second, 0).continuation, "{case}");
    assert_eq!(codex_trace(&second, 0).suffix_item_count, 0);
    assert!(!codex_trace(&second, 0).previous_response_id_present);
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 2, "{case}");
    assert_eq!(seen.requests.len(), 2, "{case}");
    let frame: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
    let full: Value = serde_json::from_slice(&canonical).unwrap();
    assert_eq!(frame["input"], full["input"], "{case}");
    assert!(frame.get("previous_response_id").is_none());
    if case != "resume" {
        assert!(
            codex_trace(&second, 0).connection_generation
                > codex_trace(&first, 0).connection_generation
        );
    }
}
#[tokio::test]
async fn codex_continuation_age_reset_is_full() {
    for age_secs in [40 * 60, 40 * 60 + 1] {
        let mock = Mock::start(vec![text_events(); 3], false, false).await;
        let provider = codex_provider(&mock);
        let first = step(&provider, 1, &opts()).await;
        let next = follow_up(opts(), &first, "Next");
        let canonical = provider.render_request(&next);
        provider
            .websocket
            .advance_clock(Duration::from_secs(age_secs));
        let second = step(&provider, 2, &next).await;
        assert_eq!(attempts(&second).len(), 1);
        assert_eq!(attempts(&second)[0].outcome, AttemptOutcome::Complete);
        assert_canonical(attempts(&second)[0], &canonical);
        let codex = codex_trace(&second, 0);
        assert!(!codex.continuation);
        assert_eq!(codex.suffix_item_count, 0);
        assert!(!codex.previous_response_id_present);
        assert_eq!(
            codex.connection_generation,
            codex_trace(&first, 0).connection_generation + 1
        );
        mock.wait_closes(1).await;
        {
            let seen = mock.observed.lock().unwrap();
            assert_eq!(seen.handshakes, 2);
            assert_eq!(seen.requests.len(), 2);
            let frame: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
            let full: Value = serde_json::from_slice(&canonical).unwrap();
            assert_eq!(frame["input"], full["input"]);
            assert!(frame.get("previous_response_id").is_none());
        }
        provider.close_run_transport();
        let reconnected_events = step(&provider, 3, &next).await;
        assert_canonical(attempts(&reconnected_events)[0], &canonical);
        assert_eq!(provider.render_request(&next), canonical);
        let seen = mock.observed.lock().unwrap();
        assert_eq!(seen.handshakes, 3);
        let mut retired_headers = seen.headers[1].clone();
        let mut reconnected_headers = seen.headers[2].clone();
        // The websocket handshake nonce must change, but the frozen wire identity must not.
        retired_headers.remove("sec-websocket-key");
        reconnected_headers.remove("sec-websocket-key");
        assert_eq!(retired_headers, reconnected_headers);
        let retired = std::str::from_utf8(&seen.requests[1]).unwrap();
        let reconnected = std::str::from_utf8(&seen.requests[2]).unwrap();
        // Only the per-send timestamp may differ from a full-input reconnect frame.
        let stamp = "x-codex-ws-stream-request-start-ms";
        assert_eq!(
            retired,
            reconnected.replacen(
                &format!(
                    "\"{stamp}\":\"{}\"",
                    attempts(&reconnected_events)[0].sent_at_ms.unwrap()
                ),
                &format!(
                    "\"{stamp}\":\"{}\"",
                    attempts(&second)[0].sent_at_ms.unwrap()
                ),
                1,
            )
        );
    }
}

#[tokio::test]
async fn codex_age_retirement_does_not_pin_https() {
    let mock = Mock::start(vec![text_events(); 8], false, false).await;
    let provider = codex_provider(&mock);
    for id in 1..=8 {
        if id > 1 {
            provider
                .websocket
                .advance_clock(Duration::from_secs(40 * 60));
        }
        let events = step(&provider, id, &opts()).await;
        assert_eq!(attempts(&events).len(), 1);
        let attempt = attempts(&events)[0];
        assert_eq!(attempt.outcome, AttemptOutcome::Complete);
        assert_eq!(attempt.transport.as_deref(), Some("websocket"));
        assert_eq!(codex_trace(&events, 0).connection_generation, id);
    }
    mock.wait_closes(7).await;
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 8);
    assert_eq!(seen.requests.len(), 8);
    assert!(seen.http_bodies.is_empty());
}
#[tokio::test]
async fn codex_continuation_header_reset_is_full() {
    codex_reset_case("headers").await;
}
#[tokio::test]
async fn codex_continuation_auth_revision_reset_is_full() {
    codex_reset_case("auth_revision").await;
}
#[tokio::test]
async fn codex_continuation_explicit_retirement_is_full() {
    codex_reset_case("retire").await;
}
#[tokio::test]
async fn codex_continuation_resume_first_request_is_full() {
    codex_reset_case("resume").await;
}
#[tokio::test]
async fn codex_continuation_closed_socket_reset_is_full() {
    codex_reset_case("closed").await;
}

#[tokio::test]
async fn codex_continuation_endpoint_change_is_full() {
    let first_mock = Mock::start(vec![text_events()], false, false).await;
    let second_mock = Mock::start(vec![text_events()], false, false).await;
    let mut provider = codex_provider(&first_mock);
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "Next");
    Arc::make_mut(&mut provider.frozen).base_url = Some(second_mock.url.clone());
    let second = step(&provider, 2, &next).await;
    assert!(!codex_trace(&second, 0).continuation);
    assert_eq!(first_mock.observed.lock().unwrap().requests.len(), 1);
    assert_eq!(second_mock.observed.lock().unwrap().requests.len(), 1);
}

#[tokio::test]
async fn codex_continuation_same_token_refresh_retires_socket() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let refresh = Arc::new(RefreshScript {
        expiring: Mutex::new(std::collections::VecDeque::from([
            None,
            Some(fresh_credential("mock-token", "account", "expiring")),
        ])),
        rejected: Mutex::new(None),
        rereads: AtomicUsize::new(0),
    });
    let provider = codex_provider(&mock)
        .with_account_refs(Some("account".into()), Some("account".into()))
        .with_credential_refresher(refresh);
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "Next");
    let second = step(&provider, 2, &next).await;
    assert!(attempts(&second)[0].credential_refresh.is_some());
    assert!(!codex_trace(&second, 0).continuation);
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 2);
    for (name, value) in &seen.headers[0] {
        if name != "sec-websocket-key" {
            assert_eq!(seen.headers[1].get(name), Some(value));
        }
    }
}

#[tokio::test]
async fn codex_continuation_failed_stream_retires_socket_and_retry_is_full() {
    let failed = vec![
        serde_json::json!({"type":"response.failed","response":{"error":{"code":"server_error","message":"test"}}}),
    ];
    let mock = Mock::start(vec![text_events(), failed, text_events()], false, false).await;
    let provider = codex_provider(&mock);
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "Next");
    let second = step(&provider, 2, &next).await;
    assert_eq!(attempts(&second).len(), 2);
    assert_eq!(attempts(&second)[1].outcome, AttemptOutcome::Complete);
    assert!(codex_trace(&second, 0).continuation);
    assert!(!codex_trace(&second, 1).continuation);
    let canonical = provider.render_request(&next);
    for attempt in attempts(&second) {
        assert_canonical(attempt, &canonical);
    }
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 2);
    assert_eq!(seen.requests.len(), 3);
    let frame: Value = serde_json::from_slice(&seen.requests[2]).unwrap();
    assert!(frame.get("previous_response_id").is_none());
    assert_eq!(
        frame["input"],
        serde_json::from_slice::<Value>(&canonical).unwrap()["input"]
    );
}

#[tokio::test]
async fn codex_continuation_incomplete_consumes_receipt_without_seeding_another() {
    for reason in ["max_output_tokens", "interrupted", "content_filter"] {
        let mut incomplete = text_events();
        incomplete[1]["type"] = serde_json::json!("response.incomplete");
        incomplete[1]["response"]["status"] = serde_json::json!("incomplete");
        incomplete[1]["response"]["incomplete_details"] = serde_json::json!({"reason":reason});
        let mock = Mock::start(vec![text_events(), incomplete, text_events()], false, false).await;
        let provider = codex_provider(&mock);
        let first = step(&provider, 1, &opts()).await;
        let next = follow_up(opts(), &first, "Next");
        let second = step(&provider, 2, &next).await;
        assert!(codex_trace(&second, 0).continuation);
        let third_options = follow_up(next, &second, "After incomplete");
        let third = step(&provider, 3, &third_options).await;
        assert!(!codex_trace(&third, 0).continuation, "{reason}");
        assert_eq!(mock.observed.lock().unwrap().handshakes, 1);
    }
}

#[tokio::test]
async fn codex_continuation_tool_argument_cut_retires_without_retry() {
    let cut = vec![
        serde_json::json!({"type":"response.created","response":{"id":"resp_cut"}}),
        serde_json::json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc","call_id":"bad","name":"lookup","arguments":""}}),
        serde_json::json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"key\":\"alpha\"}"}),
        serde_json::json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":" ".repeat(4097)}),
    ];
    let mock = Mock::start(vec![text_events(), cut, text_events()], false, false).await;
    let provider = codex_provider(&mock);
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "Next");
    let second = step(&provider, 2, &next).await;
    assert_eq!(attempts(&second).len(), 1);
    assert_eq!(
        attempts(&second)[0].outcome,
        AttemptOutcome::ToolArgsTrailingGarbage
    );
    assert!(codex_trace(&second, 0).continuation);
    mock.wait_closes(1).await;
    let third_options = follow_up(next, &second, "After cut");
    let third = step(&provider, 3, &third_options).await;
    assert!(!codex_trace(&third, 0).continuation);
    assert_eq!(attempts(&third).len(), 1);
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 2);
    assert_eq!(seen.requests.len(), 3);
    assert!(seen.http_bodies.is_empty());
}

#[tokio::test]
async fn codex_continuation_consumer_drop_retires_pending_stream() {
    let mock = Mock::start_internal(
        vec![text_events(), vec![], text_events()],
        false,
        false,
        None,
        true,
    )
    .await;
    let provider = codex_provider(&mock);
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "Next");
    let receiver = provider.step(StepId(2), &next).await;
    mock.wait_held_cut(1).await;
    drop(receiver);
    mock.cut_gates.add_permits(1);
    mock.wait_closes(1).await;
    let third = step(&provider, 3, &next).await;
    assert!(!codex_trace(&third, 0).continuation);
    assert_eq!(attempts(&third).len(), 1);
    assert_eq!(mock.observed.lock().unwrap().handshakes, 2);
}

#[tokio::test]
async fn codex_continuation_https_switch_discards_socket_baseline() {
    let mock = Mock::start(
        vec![text_events(), text_events(), text_events()],
        false,
        false,
    )
    .await;
    let provider = codex_provider(&mock);
    let https = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&https);
    provider.set_chatgpt_https_policy(Arc::new(move || flag.load(Ordering::Acquire)));
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "Next");
    https.store(true, Ordering::Release);
    let second = step(&provider, 2, &next).await;
    assert_eq!(attempts(&second)[0].transport.as_deref(), Some("https"));
    mock.wait_closes(1).await;
    https.store(false, Ordering::Release);
    let third_options = follow_up(next, &second, "After HTTPS");
    let third = step(&provider, 3, &third_options).await;
    assert!(!codex_trace(&third, 0).continuation);
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 2);
    assert_eq!(seen.requests.len(), 2);
    assert_eq!(seen.http_bodies.len(), 1);
}

struct CountingCodex(Arc<AtomicUsize>);
impl WireFamily for CountingCodex {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
        self.0.fetch_add(1, Ordering::Relaxed);
        OpenAiResponsesChatgpt.shape(frozen, opts)
    }
    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        OpenAiResponsesChatgpt.endpoint(frozen, auth, body)
    }
    fn normalizer(
        &self,
        frozen: &FrozenRenderConfig,
        step_id: StepId,
    ) -> Box<dyn StreamNormalizer> {
        OpenAiResponsesChatgpt.normalizer(frozen, step_id)
    }
}

#[tokio::test]
async fn codex_continuation_renders_each_canonical_request_once() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let base = codex_provider(&mock);
    let count = Arc::new(AtomicUsize::new(0));
    let provider = LiveProvider::new(
        CountingCodex(Arc::clone(&count)),
        (*base.frozen).clone(),
        (*base.current_auth().auth).clone(),
    );
    let first = step(&provider, 1, &opts()).await;
    let second = step(&provider, 2, &follow_up(opts(), &first, "Next")).await;
    assert!(codex_trace(&second, 0).continuation);
    assert_eq!(count.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn codex_continuation_property_mismatch_records_full_input_reason() {
    let mock = Mock::start(
        vec![text_events(), text_events(), text_events()],
        false,
        false,
    )
    .await;
    let mut provider = codex_provider(&mock);
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "Next");
    Arc::make_mut(&mut provider.frozen).service_tier = Some("priority".into());
    let second = step(&provider, 2, &next).await;
    assert!(!codex_trace(&second, 0).continuation);
    assert_eq!(
        codex_trace(&second, 0).full_input_reason.as_deref(),
        Some("service_tier_changed")
    );
    let third = step(
        &provider,
        3,
        &follow_up(next, &second, "After changed tier"),
    )
    .await;
    assert!(codex_trace(&third, 0).continuation);
    assert_eq!(mock.observed.lock().unwrap().handshakes, 1);
}

async fn codex_tool_run(
    replies: Vec<Vec<Value>>,
    expected_creates: usize,
    expected_handshakes: usize,
) -> Vec<WalRecord> {
    let mock = Mock::start(replies, false, false).await;
    let provider = codex_provider(&mock);
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "codex-tool-turn-{}-{}",
        expected_creates,
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let bind = BindIdentity::new(dir.join("project"), "websocket", "turn");
    let request = tool_run_request(&bind);
    let wal = FileWalStore::new(dir.join("wal"));
    let count = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&count);
    let plane = StubToolPlane::new().with_tool("lookup", move |_| {
        calls.fetch_add(1, Ordering::Relaxed);
        broca_core::StubReply::Output(broca_protocol::ToolOutput::Text {
            text: "found".into(),
        })
    });
    let (tx, mut rx) = mpsc::channel(256);
    let collect = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        run_loop(
            request,
            &provider,
            &plane,
            &FileLeaseStore::new(dir.join("lease")),
            &AllowAll,
            &wal,
            &tx,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    drop(tx);
    collect.await.unwrap();
    assert_eq!(result.reason, RunFinishReason::Completed);
    assert_eq!(result.steps, 2);
    assert_eq!(
        count.load(Ordering::Relaxed),
        1,
        "one actual tool handler invocation"
    );
    mock.wait_closes(1).await;
    let records = wal.read_records(&bind).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ToolResult { .. }))
            .count(),
        1
    );
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.requests.len(), expected_creates);
    assert_eq!(seen.handshakes, expected_handshakes);
    assert!(seen.http_bodies.is_empty());
    let continuation: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
    assert_eq!(continuation["previous_response_id"], "r1");
    assert_eq!(continuation["input"].as_array().unwrap().len(), 1);
    assert_eq!(continuation["input"][0]["type"], "function_call_output");
    if expected_creates == 3 {
        let retry: Value = serde_json::from_slice(&seen.requests[2]).unwrap();
        assert!(retry.get("previous_response_id").is_none());
        assert!(retry["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["type"] == "function_call"));
        assert!(retry["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["type"] == "function_call_output"));
    }
    records.into_iter().map(|frame| frame.record).collect()
}

#[tokio::test]
async fn codex_continuation_tool_turn_uses_canonical_output_and_dispatches_once() {
    let records = codex_tool_run(vec![tool_events(), text_events()], 2, 1).await;
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
            .count(),
        2
    );
}

#[tokio::test]
async fn codex_continuation_completed_without_id_does_not_seed_receipt() {
    for missing in [false, true] {
        let mut no_id = text_events();
        if missing {
            no_id[1]["response"].as_object_mut().unwrap().remove("id");
        } else {
            no_id[1]["response"]["id"] = serde_json::json!("");
        }
        let mock = Mock::start(vec![text_events(), no_id, text_events()], false, false).await;
        let provider = codex_provider(&mock);
        let first = step(&provider, 1, &opts()).await;
        let next = follow_up(opts(), &first, "Next");
        let second = step(&provider, 2, &next).await;
        assert!(codex_trace(&second, 0).continuation);
        let third = step(&provider, 3, &follow_up(next, &second, "After missing id")).await;
        assert!(!codex_trace(&third, 0).continuation);
        assert_eq!(mock.observed.lock().unwrap().handshakes, 1);
    }
}

#[tokio::test]
async fn codex_continuation_failed_normalization_retires_completed_response() {
    let mock = Mock::start(
        vec![text_events(), cut_tool_events(false), text_events()],
        false,
        false,
    )
    .await;
    let provider = codex_provider(&mock);
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "Next");
    let second = step(&provider, 2, &next).await;
    assert_eq!(attempts(&second).len(), 2);
    assert!(codex_trace(&second, 0).continuation);
    assert!(!codex_trace(&second, 1).continuation);
    assert_eq!(attempts(&second)[1].outcome, AttemptOutcome::Complete);
    let canonical = provider.render_request(&next);
    for attempt in attempts(&second) {
        assert_canonical(attempt, &canonical);
    }
    assert_eq!(mock.observed.lock().unwrap().handshakes, 2);
}

async fn codex_previous_response_miss(status: Option<u16>) {
    let mut miss = serde_json::json!({"type":"error","error":{"code":"previous_response_not_found","message":"PRIVATE-BACKEND-MESSAGE"}});
    if let Some(status) = status {
        miss["status"] = serde_json::json!(status);
    }
    let records = codex_tool_run(vec![tool_events(), vec![miss], text_events()], 3, 2).await;
    let second_step: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::ModelAttemptFinished {
                step_id,
                attempt,
                request_body_len,
                request_body_sha256,
                websocket_request,
                outcome,
                ..
            } if *step_id == StepId(2) => Some((
                attempt,
                request_body_len,
                request_body_sha256,
                websocket_request.as_ref().unwrap(),
                outcome,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        second_step.len(),
        2,
        "cache miss recovery must be an accounted retry"
    );
    assert_eq!(*second_step[0].0, 0);
    assert_eq!(*second_step[1].0, 1);
    assert_eq!(second_step[0].1, second_step[1].1);
    assert_eq!(second_step[0].2, second_step[1].2);
    assert_eq!(*second_step[0].4, AttemptOutcome::ChunkError);
    assert_eq!(*second_step[1].4, AttemptOutcome::Complete);
    assert!(second_step[0].3.codex.as_ref().unwrap().continuation);
    assert!(!second_step[1].3.codex.as_ref().unwrap().continuation);
    assert_eq!(
        second_step[0].3.codex.as_ref().unwrap().physical_send_count,
        1
    );
    assert_eq!(
        second_step[1].3.codex.as_ref().unwrap().physical_send_count,
        1
    );
    assert_ne!(
        second_step[0]
            .3
            .codex
            .as_ref()
            .unwrap()
            .connection_generation,
        second_step[1]
            .3
            .codex
            .as_ref()
            .unwrap()
            .connection_generation
    );
}

#[tokio::test]
async fn codex_previous_response_not_found_without_status_is_accounted_retry() {
    codex_previous_response_miss(None).await;
}
#[tokio::test]
async fn codex_previous_response_not_found_400_is_accounted_retry() {
    codex_previous_response_miss(Some(400)).await;
}

#[test]
fn codex_previous_response_code_precedes_generic_status_mapping() {
    for status in [None, Some(400)] {
        let mut raw = serde_json::json!({"type":"error","error":{"code":"previous_response_not_found","message":"PRIVATE-BACKEND-MESSAGE"}});
        if let Some(status) = status {
            raw["status"] = serde_json::json!(status);
        }
        let failure = responses_websocket::wrapped_error(
            &serde_json::to_vec(&raw).unwrap(),
            &mut AttemptTrace::default(),
        )
        .expect("named cache miss is transport-retryable without a status");
        assert_eq!(failure.error.class, ErrorClass::Transient);
        assert_eq!(failure.outcome, AttemptOutcome::ChunkError);
        assert!(!failure.retry_forbidden);
        assert_eq!(
            failure.error.provider_code.as_deref(),
            Some("previous_response_not_found")
        );
        assert!(!failure.error.message.contains("PRIVATE"));
    }
    let other = serde_json::json!({"type":"error","status":400,"error":{"code":"other_error","message":"test"}});
    let failure = responses_websocket::wrapped_error(
        &serde_json::to_vec(&other).unwrap(),
        &mut AttemptTrace::default(),
    )
    .unwrap();
    assert_eq!(failure.outcome, AttemptOutcome::HttpError);
    assert_ne!(failure.error.class, ErrorClass::Transient);
}

#[test]
fn codex_previous_response_retry_advice_is_nested_first_with_top_level_fallback() {
    for (nested, top, expected) in [
        (
            serde_json::json!({"Retry-After":"1"}),
            serde_json::json!({"Retry-After":"2"}),
            Some(1.0),
        ),
        (
            serde_json::json!({"Retry-After":"invalid"}),
            serde_json::json!({"Retry-After":"2"}),
            Some(2.0),
        ),
        (
            serde_json::json!({"Retry-After-Ms":"250"}),
            serde_json::json!({}),
            Some(0.25),
        ),
        (Value::Null, Value::Null, None),
    ] {
        let raw = serde_json::json!({"type":"error","status":400,"headers":top,"error":{"code":"previous_response_not_found","headers":nested}});
        let failure = responses_websocket::wrapped_error(
            &serde_json::to_vec(&raw).unwrap(),
            &mut AttemptTrace::default(),
        )
        .unwrap();
        assert_eq!(failure.error.retry_after_secs, expected);
    }
}
fn opts() -> CallOptions {
    serde_json::from_value(serde_json::json!({
        "model":{"provider_module_id":"openai","model_id":"m"},
        "prompt":[{"role":"user","content":[{"type":"text","text":"hi"}]}],
        "tools":[], "tool_choice":{"type":"auto"}, "generation":{}
    }))
    .unwrap()
}
async fn step(provider: &impl Provider, id: u64, opts: &CallOptions) -> Vec<NormalizedEvent> {
    let mut rx = provider.step(StepId(id), opts).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    })
    .await
    .expect("mock step terminates")
}
fn attempts(events: &[NormalizedEvent]) -> Vec<&ModelAttempt> {
    events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => Some(attempt),
            _ => None,
        })
        .collect()
}

fn terminal_error(events: &[NormalizedEvent]) -> &ProviderError {
    events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => Some(error),
            _ => None,
        })
        .expect("the step ends in an error")
}

async fn observed_socket_attempt(mock: &Mock) -> broca_test_support::LogEvent {
    let provider = provider(mock);
    observed_provider_attempt(&provider).await
}

async fn observed_provider_attempt(
    provider: &LiveProvider<OpenAiResponsesChatgpt>,
) -> broca_test_support::LogEvent {
    let request = provider
        .family
        .endpoint(
            &provider.frozen,
            &provider.current_auth().auth,
            provider.render_request(&opts()),
        )
        .into();
    let (tx, _rx) = mpsc::channel(256);
    let (_, logs) = broca_test_support::capture_logs(async {
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            responses_websocket::drive(
                &provider.websocket,
                &provider.client,
                &request,
                &provider.clock,
                provider.family.normalizer(&provider.frozen, StepId(1)),
                &tx,
                &mut AttemptTrace::default(),
            ),
        )
        .await
        .expect("the scripted socket attempt must terminate");
    })
    .await;
    let summaries: Vec<_> = logs
        .into_iter()
        .filter(|log| log.message() == "Responses websocket attempt summary")
        .collect();
    assert_eq!(summaries.len(), 1, "one summary per attempt");
    summaries.into_iter().next().unwrap()
}

async fn delta_fingerprint(distinct: bool) -> broca_test_support::LogEvent {
    let mut events = vec![
        serde_json::json!({"type":"response.output_item.added","output_index":3,"item":{"type":"function_call","id":"PRIVATE-ITEM-ID","call_id":"call1","name":"lookup","arguments":""}}),
    ];
    for delta in std::iter::once("[".to_owned())
        .chain((0..12).map(|n| {
            if distinct {
                format!("\"PRIVATE-DELTA-{n}\",")
            } else {
                "\"PRIVATE-DELTA\",".into()
            }
        }))
        .chain(["\"\"]".into(), " \n".into()])
    {
        events.push(serde_json::json!({"type":"response.function_call_arguments.delta","response_id":"PRIVATE-RESPONSE-ID","item_id":"PRIVATE-ITEM-ID","output_index":3,"delta":delta}));
    }
    events.push(serde_json::json!({"type":"response.completed","response":{"id":"PRIVATE-RESPONSE-ID","status":"completed"}}));
    let mock = Mock::start(vec![events], false, false).await;
    let summary = observed_socket_attempt(&mock).await;
    assert_eq!(summary.target, "provider.websocket.attempt");
    assert_eq!(summary.level, tracing::Level::DEBUG);
    for (field, expected) in [
        ("response_create_enqueued", "1"),
        ("response_create_sent", "1"),
        ("other_text_sent", "0"),
        ("text_received", "17"),
        ("argument_deltas", "15"),
        ("whitespace_deltas", "1"),
        ("empty_deltas", "0"),
        ("distinct_response_ids", "1"),
        ("distinct_item_ids", "1"),
        ("output_index_max", "3"),
        ("distinct_deltas_overflow", "false"),
        ("socket_generation", "1"),
        ("socket_reused", "false"),
    ] {
        assert_eq!(summary.fields[field], expected, "{field}");
    }
    assert_eq!(
        summary.fields["argument_delta_bytes"],
        if distinct { "224" } else { "198" }
    );
    let logs = format!("{summary:?}");
    for secret in [
        "PRIVATE-ITEM-ID",
        "PRIVATE-RESPONSE-ID",
        "PRIVATE-DELTA",
        "mock-token",
    ] {
        assert!(!logs.contains(secret));
    }
    summary
}

#[tokio::test]
async fn websocket_summary_repeated_deltas_have_few_hashes() {
    let summary = delta_fingerprint(false).await;
    assert_eq!(summary.fields["distinct_deltas"], "4");
}

#[tokio::test]
async fn websocket_summary_distinct_deltas_have_many_hashes() {
    let summary = delta_fingerprint(true).await;
    assert_eq!(summary.fields["distinct_deltas"], "15");
}

#[tokio::test]
async fn websocket_summary_separates_empty_and_whitespace_deltas() {
    let mut events = tool_events();
    for delta in ["", " \n"] {
        events.insert(2, serde_json::json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":delta}));
    }
    let mock = Mock::start(vec![events], false, false).await;
    let summary = observed_socket_attempt(&mock).await;
    assert_eq!(summary.fields["argument_deltas"], "3");
    assert_eq!(summary.fields["argument_delta_bytes"], "9");
    assert_eq!(summary.fields["whitespace_deltas"], "1");
    assert_eq!(summary.fields["empty_deltas"], "1");
    assert_eq!(summary.fields["distinct_deltas"], "3");
}

#[tokio::test]
async fn websocket_summary_detects_replayed_sequence_numbers() {
    let mut events: Vec<_> = [10, 11, 10, 11, 12].into_iter().map(|sequence| {
        serde_json::json!({"type":"response.output_text.delta","sequence_number":sequence,"delta":"x"})
    }).collect();
    events.extend(text_events().into_iter().skip(1));
    let mock = Mock::start(vec![events], false, false).await;
    let summary = observed_socket_attempt(&mock).await;
    assert_eq!(summary.fields["sequence_count"], "5");
    assert_eq!(summary.fields["sequence_min"], "10");
    assert_eq!(summary.fields["sequence_max"], "12");
    assert_eq!(summary.fields["sequence_repeated_or_backwards"], "2");
}

#[tokio::test]
async fn websocket_summary_allowlists_wrapped_error_codes() {
    for (code, expected) in [
        (
            "websocket_connection_limit_reached",
            "websocket_connection_limit_reached",
        ),
        ("PRIVATE-UNKNOWN-CODE", "other"),
    ] {
        let mock = Mock::start(vec![vec![serde_json::json!({"type":"error","status":400,"error":{"code":code,"message":"PRIVATE-ERROR-MESSAGE"}})]], true, false).await;
        let summary = observed_socket_attempt(&mock).await;
        assert_eq!(summary.level, tracing::Level::INFO);
        assert_eq!(summary.fields["failed"], "true");
        assert_eq!(summary.fields["wrapped_error_code"], expected);
        assert!(!format!("{summary:?}").contains("PRIVATE-"));
    }
}

#[tokio::test]
async fn websocket_summary_records_numeric_close_code() {
    let mock = Mock::start(
        vec![vec![
            serde_json::json!({"type":"response.output_text.delta","delta":"x"}),
        ]],
        true,
        false,
    )
    .await;
    let summary = observed_socket_attempt(&mock).await;
    assert_eq!(summary.level, tracing::Level::INFO);
    assert_eq!(summary.fields["closes_received"], "1");
    assert_eq!(summary.fields["close_code"], "1012");
    assert!(!format!("{summary:?}").contains("PRIVATE-CLOSE-REASON"));
}

#[tokio::test]
async fn websocket_summary_reused_socket_resets_attempt_counters() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let provider = provider(&mock);
    let first = observed_provider_attempt(&provider).await;
    let second = observed_provider_attempt(&provider).await;
    assert_eq!(first.fields["socket_reused"], "false");
    assert_eq!(second.fields["socket_reused"], "true");
    assert_eq!(second.fields["socket_generation"], "1");
    for summary in [first, second] {
        assert_eq!(summary.fields["response_create_enqueued"], "1");
        assert_eq!(summary.fields["response_create_sent"], "1");
        assert_eq!(summary.fields["text_received"], "2");
        assert!(summary.fields["socket_age_ms"].parse::<u64>().is_ok());
    }
}

#[tokio::test]
async fn retention_websocket_diagnostics_omit_backend_content() {
    use tracing::instrument::WithSubscriber;
    const MARKER: &str = "PRIVATE-RETENTION-WEBSOCKET-CONTENT";
    for event in [
        serde_json::json!({"type":"error","status":400,"error":{"code":MARKER,"message":MARKER}}),
        serde_json::json!({"type":"response.failed","response":{"error":{"code":MARKER,"type":MARKER,"message":MARKER}}}),
    ] {
        let mock = Mock::start(vec![vec![event]], true, false).await;
        let provider = provider(&mock);
        let mut request: PreparedHttpRequest = provider
            .family
            .endpoint(
                &provider.frozen,
                &provider.current_auth().auth,
                provider.render_request(&opts()),
            )
            .into();
        request.ephemeral_session = true;
        request.family = Some(crate::spec::WireFamilyId::OpenAiResponsesChatgpt);
        let (tx, _rx) = mpsc::channel(256);
        let mut trace = AttemptTrace::default();
        let logs = super::chunk_error_tests::ProviderWarnings::default();
        responses_websocket::drive(
            &provider.websocket,
            &provider.client,
            &request,
            &provider.clock,
            provider.family.normalizer(&provider.frozen, StepId(1)),
            &tx,
            &mut trace,
        )
        .with_subscriber(logs.clone())
        .await
        .unwrap_err();
        let logs = logs.0.lock().unwrap();
        assert!(!logs.is_empty(), "the fixture must reach an error log site");
        assert!(!format!("{logs:?}").contains(MARKER));
        let failure = logs
            .iter()
            .find(|log| log["message"] == "model websocket attempt failed")
            .unwrap();
        assert!(failure.contains_key("class"));
        assert_eq!(failure["provider_code"], "none");
        assert!(
            failure.keys().all(|key| matches!(
                key.as_str(),
                "class" | "status" | "provider_code" | "message"
            )),
            "{failure:?}"
        );
    }
}

#[tokio::test]
async fn websocket_output_ceiling_matches_https_without_retry() {
    let expected = format!(
        "output ceiling reached mid tool call: finish=length, max_output_tokens=unknown (tool write_file, {} argument bytes received; provider stop reason max_output_tokens, output_tokens=4096)",
        CUT_ARGS.len()
    );
    let mut errors = Vec::new();
    for https in [false, true] {
        let mock = Mock::start(vec![cut_tool_events(true)], false, false).await;
        let provider = provider(&mock);
        provider.set_chatgpt_https_policy(Arc::new(move || https));
        let events = step(&provider, 1, &opts()).await;
        let error = terminal_error(&events);
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(error.provider_code.as_deref(), Some("output_ceiling"));
        assert_eq!(error.message, expected);
        assert!(events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Error {
                retries_used: 0,
                ..
            })
        )));
        let attempts = attempts(&events);
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].outcome, AttemptOutcome::DecodeError);
        assert_eq!(
            attempts[0].transport.as_deref(),
            Some(if https { "https" } else { "websocket" })
        );
        let seen = mock.observed.lock().unwrap();
        assert_eq!(seen.http_bodies.len(), usize::from(https));
        assert_eq!(seen.requests.len(), usize::from(!https));
        errors.push(error.clone());
    }
    assert_eq!(errors[0], errors[1]);
}

#[tokio::test]
async fn websocket_context_overflow_matches_https_without_retry() {
    for code in ["context_length_exceeded", "context_window_exceeded"] {
        for event in [
            serde_json::json!({"type":"error","code":code,"message":"Your input exceeds the context window of this model."}),
            serde_json::json!({"type":"response.failed","response":{"error":{"code":code,"message":"Your input exceeds the context window of this model."}}}),
        ] {
            for https in [false, true] {
                let mock = Mock::start(vec![vec![event.clone()]], false, false).await;
                let provider = provider(&mock);
                provider.set_chatgpt_https_policy(Arc::new(move || https));
                let events = step(&provider, 1, &opts()).await;
                let error = terminal_error(&events);
                assert_eq!(error.class, ErrorClass::ContextOverflow);
                assert_eq!(error.provider_code.as_deref(), Some(code));
                assert_eq!(attempts(&events).len(), 1);
                assert!(events.iter().any(|event| matches!(
                    event,
                    NormalizedEvent::Control(ProviderControl::Error {
                        retries_used: 0,
                        ..
                    })
                )));
                let seen = mock.observed.lock().unwrap();
                assert_eq!(seen.http_bodies.len(), usize::from(https));
                assert_eq!(seen.requests.len(), usize::from(!https));
            }
        }
    }
}

#[tokio::test]
async fn websocket_malformed_tool_stream_is_generic_and_retried_once() {
    let mock = Mock::start(vec![cut_tool_events(false); 2], false, false).await;
    let mut provider = provider(&mock);
    provider.retry_clock.virtual_waits = Some(Arc::new(Mutex::new(Vec::new())));
    let events = step(&provider, 1, &opts()).await;
    let error = terminal_error(&events);
    assert_eq!(error.class, ErrorClass::Transient);
    assert_eq!(error.provider_code, None);
    assert_eq!(
        error.message,
        "Responses websocket attempt failed (DecodeError); response body omitted"
    );
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Error {
            retries_used: 1,
            ..
        })
    )));
    let attempts = attempts(&events);
    assert_eq!(attempts.len(), 2);
    assert!(attempts
        .iter()
        .all(|attempt| attempt.outcome == AttemptOutcome::DecodeError));
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.requests.len(), 2);
    assert_eq!(seen.handshakes, 2);
    assert!(seen.http_bodies.is_empty());
}

#[tokio::test]
async fn websocket_reuses_one_handshake_across_steps() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let provider = provider(&mock);
    for id in 1..=2 {
        assert_eq!(
            attempts(&step(&provider, id, &opts()).await)[0].outcome,
            AttemptOutcome::Complete
        );
    }
    assert_eq!(
        mock.observed.lock().unwrap().handshakes,
        1,
        "reuse must not open a second socket"
    );
    provider.close_run_transport();
    mock.wait_closes(1).await;
}

#[tokio::test]
async fn websocket_upgrade_headers_match_codex_and_existing_https_headers_exactly() {
    let mock = Mock::start(vec![text_events()], false, false).await;
    let provider = provider(&mock);
    step(&provider, 1, &opts()).await;
    let seen = mock.observed.lock().unwrap();
    let headers = &seen.headers[0];
    let expected = BTreeMap::from([
        ("accept", "text/event-stream"),
        ("content-type", "application/json"),
        ("authorization", "Bearer mock-token"),
        ("chatgpt-account-id", "account"),
        ("session-id", "session"),
        ("originator", "codex_cli_rs"),
        ("user-agent", "codex_cli_rs/0.144.0"),
        ("thread-id", "session"),
        ("x-client-request-id", "session"),
        ("openai-beta", "responses_websockets=2026-02-06"),
        ("connection", "Upgrade"),
        ("upgrade", "websocket"),
        ("sec-websocket-version", "13"),
    ]);
    for (name, value) in &expected {
        assert_eq!(
            headers.get(*name).map(String::as_str),
            Some(*value),
            "header {name}"
        );
    }
    assert_eq!(
        headers["sec-websocket-extensions"],
        "permessage-deflate; client_max_window_bits"
    );
    assert!(!headers["sec-websocket-key"].is_empty());
    assert_eq!(
        headers.len(),
        expected.len() + 3,
        "only host, key and deflate vary"
    );
    assert_eq!(headers["host"], mock.url.trim_start_matches("http://"));
}

#[tokio::test]
async fn websocket_attempt_durably_records_transport_and_preserves_body_digest() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let provider = provider(&mock);
    let options = opts();
    let ws_events = step(&provider, 1, &options).await;
    provider.set_chatgpt_https_policy(Arc::new(|| true));
    let http_events = step(&provider, 2, &options).await;
    let ws = attempts(&ws_events)[0];
    let http = attempts(&http_events)[0];
    let record = WalRecord::model_attempt_finished(StepId(1), ws.clone());
    let json = serde_json::to_value(record).unwrap();
    assert_eq!(
        json["transport"], "websocket",
        "the transport must survive the WAL conversion"
    );
    assert_eq!(http.transport.as_deref(), Some("https"));
    assert_eq!(ws.request_body_sha256, http.request_body_sha256);
    assert_eq!(ws.request_body_len, http.request_body_len);
    let seen = mock.observed.lock().unwrap();
    assert_eq!(
        RequestDigest::of(&seen.http_bodies[0]).sha256,
        ws.request_body_sha256
    );
    assert_eq!(
        RequestDigest::of(&seen.requests[0]).sha256,
        ws.websocket_request.as_ref().unwrap().sha256
    );
    let mut framed: Value = serde_json::from_slice(&seen.requests[0]).unwrap();
    assert_eq!(
        framed.as_object_mut().unwrap().remove("type"),
        Some(Value::String("response.create".into()))
    );
    assert!(framed.get("previous_response_id").is_none());
    assert_eq!(
        framed,
        serde_json::from_slice::<Value>(&seen.http_bodies[0]).unwrap()
    );
}

#[tokio::test]
async fn websocket_refused_upgrade_falls_back_without_double_sending() {
    let mock = Mock::start(vec![text_events()], false, true).await;
    let mut provider = provider(&mock);
    provider.retry_clock.virtual_waits = Some(Arc::new(Mutex::new(Vec::new())));
    let events = step(&provider, 1, &opts()).await;
    let attempts = attempts(&events);
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, AttemptOutcome::Complete);
    assert_eq!(attempts[0].transport.as_deref(), Some("https"));
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 1);
    assert_eq!(seen.http_bodies.len(), 1);
    assert!(seen.requests.is_empty());
}

#[tokio::test]
async fn websocket_repeated_upgrade_failures_pin_https_for_the_run_only() {
    let mock = Mock::start(vec![text_events(); 8], false, true).await;
    let provider = provider(&mock);
    for id in 1..=7 {
        assert_eq!(
            attempts(&step(&provider, id, &opts()).await)[0].outcome,
            AttemptOutcome::Complete
        );
    }
    assert_eq!(mock.observed.lock().unwrap().handshakes, 6);
    provider.close_run_transport();
    step(&provider, 8, &opts()).await;
    assert_eq!(
        mock.observed.lock().unwrap().handshakes,
        7,
        "fallback cannot leak into a new run"
    );
}

#[tokio::test]
async fn websocket_reconnects_after_server_restart() {
    let mock = Mock::start(vec![text_events(), text_events()], true, false).await;
    let provider = provider(&mock);
    for id in 1..=2 {
        assert_eq!(
            attempts(&step(&provider, id, &opts()).await)
                .last()
                .unwrap()
                .outcome,
            AttemptOutcome::Complete
        );
    }
    assert_eq!(mock.observed.lock().unwrap().handshakes, 2);
}

#[tokio::test]
async fn websocket_full_tool_turn_completes_and_closes_on_run_end() {
    let mock = Mock::start(vec![tool_events(), text_events()], false, false).await;
    let provider = provider(&mock);
    let dir = broca_test_support::scratch_root().join("ws-tool-turn");
    let bind = BindIdentity::new(dir.join("project"), "websocket", "turn");
    let request = tool_run_request(&bind);
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(256);
    let collect = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        run_loop(
            request,
            &provider,
            &StubToolPlane::new().with_canned_text("lookup", "found"),
            &FileLeaseStore::new(dir.join("lease")),
            &AllowAll,
            &wal,
            &tx,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    drop(tx);
    collect.await.unwrap();
    assert_eq!(result.reason, RunFinishReason::Completed);
    assert_eq!(result.steps, 2);
    mock.wait_closes(1).await;
    let records = wal.read_records(&bind).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ModelStepFinished { .. }))
            .count(),
        2
    );
    assert!(records
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::ToolResult { .. })));
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.requests.len(), 2);
    let second: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
    assert!(second["input"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["type"] == "function_call_output"));
    assert!(second["input"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["role"] == "user"));
    assert!(second.get("previous_response_id").is_none());
}

#[tokio::test]
async fn websocket_socket_stays_open_until_response_completed() {
    let mock = Mock::start(vec![text_events()], false, false).await;
    let mut provider = provider(&mock);
    provider.retry_clock.virtual_waits = Some(Arc::new(Mutex::new(Vec::new())));
    let events = step(&provider, 1, &opts()).await;
    assert_eq!(attempts(&events).len(), 1);
    assert_eq!(attempts(&events)[0].outcome, AttemptOutcome::Complete);
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::AssistantMessage { .. })
    )));
    assert_eq!(
        mock.observed.lock().unwrap().closes,
        0,
        "completion does not close a reusable socket"
    );
}

#[tokio::test]
async fn websocket_cancel_closes_socket_and_records_cancelled_attempt() {
    let mock = Mock::start(vec![vec![]], false, false).await;
    let provider = provider(&mock);
    let cancel = CancellationToken::new();
    let mut rx = provider
        .step_with_cancel(StepId(1), &opts(), cancel.clone(), None)
        .await;
    mock.wait_request().await;
    cancel.cancel();
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
    }
    assert_eq!(attempts(&events)[0].outcome, AttemptOutcome::Cancelled);
    assert_eq!(attempts(&events)[0].transport.as_deref(), Some("websocket"));
    mock.wait_closes(1).await;
}

#[tokio::test]
async fn websocket_close_before_completion_is_chunk_error_with_close_code() {
    close_failure(None).await;
}
#[tokio::test]
async fn websocket_long_chunk_cut_retries_instead_of_response_deadline() {
    for seconds in [895, 2280] {
        let elapsed = Duration::from_secs(seconds);
        close_failure(Some(elapsed)).await;
        let mock = Mock::start(vec![vec![], text_events()], true, false).await;
        let mut provider = provider(&mock);
        let waits = Arc::new(Mutex::new(Vec::new()));
        provider.retry_clock.virtual_waits = Some(waits.clone());
        provider.retry_clock.virtual_attempt_elapsed = Some(elapsed);
        let events = step(&provider, 1, &opts()).await;
        let attempts = attempts(&events);
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].outcome, AttemptOutcome::ChunkError);
        assert_eq!(attempts[1].outcome, AttemptOutcome::Complete);
        assert!(attempts
            .iter()
            .all(|attempt| attempt.transport.as_deref() == Some("websocket")));
        assert_eq!(waits.lock().unwrap().len(), 1);
        assert!(!events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Error { .. })
        )));
        let seen = mock.observed.lock().unwrap();
        assert_eq!(seen.requests.len(), 2);
        assert!(seen.http_bodies.is_empty());
    }
}
async fn close_failure(elapsed: Option<Duration>) {
    let mock = Mock::start(
        vec![vec![
            serde_json::json!({"type":"response.created","response":{"id":"r"}}),
        ]],
        true,
        false,
    )
    .await;
    let provider = provider(&mock);
    let mut request: PreparedHttpRequest = provider
        .family
        .endpoint(
            &provider.frozen,
            &provider.current_auth().auth,
            provider.render_request(&opts()),
        )
        .into();
    request.family = Some(crate::spec::WireFamilyId::OpenAiResponsesChatgpt);
    let (tx, _rx) = mpsc::channel(256);
    let mut trace = AttemptTrace {
        virtual_attempt_elapsed: elapsed,
        ..Default::default()
    };
    let logs = super::chunk_error_tests::ProviderWarnings::default();
    use tracing::instrument::WithSubscriber;
    let failure = responses_websocket::drive(
        &provider.websocket,
        &provider.client,
        &request,
        &provider.clock,
        provider.family.normalizer(&provider.frozen, StepId(1)),
        &tx,
        &mut trace,
    )
    .with_subscriber(logs.clone())
    .await
    .unwrap_err();
    let warnings = logs.0.lock().unwrap();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0]["transport"], "websocket");
    assert_eq!(warnings[0]["close_code"], "1012");
    assert_eq!(warnings[0]["outcome"], "ChunkError");
    assert_eq!(warnings[0]["class"], "ProviderUnavailable");
    assert_eq!(warnings[0]["provider_code"], "none");
    drop(warnings);
    assert_eq!(failure.outcome, AttemptOutcome::ChunkError);
    assert!(!failure.retry_forbidden);
    assert_eq!(failure.error.provider_code, None);
    let attempt = trace.into_attempt(0, &RequestDigest::of(&request.body), failure.outcome);
    assert_eq!(attempt.websocket_close_code, Some(1012));
}

#[tokio::test(start_paused = true)]
async fn websocket_idle_and_keepalive_only_bounds_match_sse() {
    for (keepalive, outcome, duration) in [
        (false, AttemptOutcome::IdleTimeout, STREAM_IDLE_TIMEOUT),
        (true, AttemptOutcome::ContentTimeout, STREAM_CONTENT_TIMEOUT),
    ] {
        let stream = futures::stream::unfold((), move |_| async move {
            if keepalive {
                tokio::time::sleep(Duration::from_secs(100)).await;
                Some((Ok::<_, std::io::Error>(Vec::<u8>::new()), ()))
            } else {
                futures::future::pending().await
            }
        });
        let normalizer = OpenAiResponsesChatgpt
            .normalizer(&openai_responses_chatgpt_frozen_config("m"), StepId(1));
        let (tx, _rx) = mpsc::channel(256);
        let mut trace = AttemptTrace {
            transport: Some("websocket".into()),
            ..Default::default()
        };
        let start = Instant::now();
        let failure =
            drive_body_stream_traced(stream, normalizer, &tx, MAX_STREAM_BYTES, &mut trace)
                .await
                .unwrap_err();
        assert_eq!(failure.outcome, outcome);
        assert_eq!(start.elapsed(), duration);
    }
}

#[tokio::test]
async fn websocket_failure_after_send_retries_on_a_new_socket_not_https() {
    let mock = Mock::start(vec![vec![], text_events()], true, false).await;
    let mut provider = provider(&mock);
    provider.retry_clock.virtual_waits = Some(Arc::new(Mutex::new(Vec::new())));
    let events = step(&provider, 1, &opts()).await;
    let attempts = attempts(&events);
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].outcome, AttemptOutcome::ChunkError);
    assert_eq!(attempts[0].websocket_close_code, Some(1012));
    assert_eq!(attempts[1].outcome, AttemptOutcome::Complete);
    assert!(attempts
        .iter()
        .all(|attempt| attempt.transport.as_deref() == Some("websocket")));
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.requests.len(), 2);
    assert!(
        seen.http_bodies.is_empty(),
        "a sent request cannot fall back outside the retry loop"
    );
}

#[tokio::test]
async fn websocket_shutdown_seal_closes_socket_and_hands_over_attempt() {
    let mock = Mock::start(vec![vec![]], false, false).await;
    let provider = provider(&mock);
    let seal = ShutdownSeal::new();
    let rx = provider
        .step_with_shutdown_seal(
            StepId(1),
            Arc::new(opts()),
            CancellationToken::new(),
            None,
            seal.clone(),
        )
        .await;
    mock.wait_request().await;
    let collected = seal.seal_and_collect(Duration::from_secs(5)).await.unwrap();
    assert_eq!(collected.attempt.transport.as_deref(), Some("websocket"));
    assert_eq!(collected.attempt.outcome, AttemptOutcome::SealedInFlight);
    mock.wait_closes(1).await;
    drop(rx);
}

#[tokio::test]
async fn websocket_turn_state_is_sticky_metadata_without_changing_canonical_body() {
    let mut first = text_events();
    first.insert(0, serde_json::json!({"type":"response.metadata","headers":{"X-Codex-Turn-State":"route-token"}}));
    let mock = Mock::start(vec![first, text_events()], false, false).await;
    let provider = provider(&mock);
    let initial = step(&provider, 1, &opts()).await;
    let second = step(&provider, 2, &opts()).await;
    assert_eq!(
        attempts(&initial)[0].request_body_sha256,
        attempts(&second)[0].request_body_sha256
    );
    let seen = mock.observed.lock().unwrap();
    let framed: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
    assert_eq!(
        framed["client_metadata"]["x-codex-turn-state"],
        "route-token"
    );
    assert!(!seen.headers[0].contains_key("x-codex-turn-state"));
}

#[tokio::test]
async fn websocket_errors_and_warnings_never_echo_headers_or_payloads() {
    use tracing::instrument::WithSubscriber;
    for event in [
        serde_json::json!({"type":"error","status":429,"error":{"code":"PRIVATE-PAYLOAD-CODE","message":"Bearer mock-token PRIVATE-PAYLOAD-TEXT"},"headers":{"x-private":"PRIVATE-HEADER-VALUE"}}),
        serde_json::json!({"type":"error","status":400,"error":{"code":"output_ceiling","message":"output ceiling reached mid tool call: PRIVATE-PAYLOAD-TEXT Bearer mock-token"}}),
        serde_json::json!({"type":"response.failed","response":{"error":{"code":"PRIVATE-PAYLOAD-CODE","type":"PRIVATE-PAYLOAD-TYPE","message":"Bearer mock-token PRIVATE-PAYLOAD-TEXT"}}}),
        // Event type names are logged only when they are short and use a safe character set
        // (`crate::sse::safe_event_type_name`); anything else is logged as `other`.
        serde_json::json!({"type":"PRIVATE PAYLOAD TYPE carrying text that is not an event name"}),
    ] {
        let mock = Mock::start(vec![vec![event]], true, false).await;
        let provider = provider(&mock);
        let mut request: PreparedHttpRequest = provider
            .family
            .endpoint(
                &provider.frozen,
                &provider.current_auth().auth,
                provider.render_request(&opts()),
            )
            .into();
        request.family = Some(crate::spec::WireFamilyId::OpenAiResponsesChatgpt);
        let (tx, _rx) = mpsc::channel(256);
        let mut trace = AttemptTrace::default();
        let logs = super::chunk_error_tests::ProviderWarnings::default();
        let failure = responses_websocket::drive(
            &provider.websocket,
            &provider.client,
            &request,
            &provider.clock,
            provider.family.normalizer(&provider.frozen, StepId(1)),
            &tx,
            &mut trace,
        )
        .with_subscriber(logs.clone())
        .await
        .unwrap_err();
        // A wrapped error frame's own text may reach broca's local WARN line, in its
        // `error_frame` field only (see `WrappedErrorFrame`); never the headers it wraps.
        let mut warnings = logs.0.lock().unwrap().clone();
        for warning in &mut warnings {
            if let Some(frame) = warning.remove("error_frame") {
                assert!(!frame.contains("PRIVATE-HEADER-VALUE"), "{frame}");
                assert!(!frame.contains("x-private"), "{frame}");
            }
        }
        let diagnostics = format!("{:?} {:?}", failure.error, warnings);
        for private in ["PRIVATE-", "mock-token", "Bearer", "codex_cli_rs"] {
            assert!(
                !diagnostics.contains(private),
                "socket errors and diagnostics must omit headers and payloads"
            );
        }
    }
}

#[tokio::test]
async fn websocket_decode_failure_warns_once_without_response_or_tool_content() {
    use tracing::instrument::WithSubscriber;
    for ceiling in [false, true] {
        let mut replies = cut_tool_events(ceiling);
        replies.insert(0, serde_json::json!({"type":"response.output_text.delta","delta":"PRIVATE-RESPONSE-CONTENT"}));
        let response_bytes: usize = replies.iter().map(|event| event.to_string().len()).sum();
        let mock = Mock::start(vec![replies], false, false).await;
        let provider = provider(&mock);
        let request: PreparedHttpRequest = provider
            .family
            .endpoint(
                &provider.frozen,
                &provider.current_auth().auth,
                provider.render_request(&opts()),
            )
            .into();
        let (tx, _rx) = mpsc::channel(256);
        let mut trace = AttemptTrace::default();
        let logs = super::chunk_error_tests::ProviderWarnings::default();
        let failure = responses_websocket::drive(
            &provider.websocket,
            &provider.client,
            &request,
            &provider.clock,
            provider.family.normalizer(&provider.frozen, StepId(1)),
            &tx,
            &mut trace,
        )
        .with_subscriber(logs.clone())
        .await
        .unwrap_err();
        assert_eq!(failure.outcome, AttemptOutcome::DecodeError);
        let warnings = logs.0.lock().unwrap();
        assert_eq!(warnings.len(), 1, "one WARN for each failed socket attempt");
        let log = &warnings[0];
        assert_eq!(log["message"], "model websocket attempt failed");
        assert_eq!(log["outcome"], "DecodeError");
        assert_eq!(
            log["class"],
            if ceiling { "Permanent" } else { "Transient" }
        );
        assert_eq!(
            log["provider_code"],
            if ceiling { "output_ceiling" } else { "none" }
        );
        assert_eq!(log["transport"], "websocket");
        assert_eq!(
            log["response_bytes"].parse::<usize>().unwrap(),
            response_bytes
        );
        assert!(log["elapsed_ms"].parse::<u64>().is_ok());
        assert!(log.contains_key("close_code"));
        if ceiling {
            assert_eq!(log["provider_stop_reason"], "max_output_tokens");
            assert_eq!(log["tool_name"], "write_file");
            assert_eq!(log["argument_bytes"], CUT_ARGS.len().to_string());
            assert_eq!(log["output_tokens"], "4096");
            assert_eq!(log["max_output_tokens"], "unknown");
        }
        let diagnostics = format!("{warnings:?} {:?}", failure.error);
        for private in ["PRIVATE-", "mock-token", "Bearer"] {
            assert!(
                !diagnostics.contains(private),
                "socket WARN must omit content and credentials"
            );
        }
    }
}

struct RefreshScript {
    expiring:
        Mutex<std::collections::VecDeque<Option<crate::credential_refresh::RefreshedCredential>>>,
    rejected: Mutex<Option<crate::credential_refresh::RefreshedCredential>>,
    rereads: AtomicUsize,
}
impl crate::credential_refresh::CredentialRefresher for RefreshScript {
    fn refresh_expiring(
        &self,
    ) -> crate::credential_refresh::RefreshFuture<'_, crate::credential_refresh::RefreshedCredential>
    {
        Box::pin(async { self.expiring.lock().unwrap().pop_front().flatten() })
    }
    fn refresh_rejected(
        &self,
        _status: u16,
    ) -> crate::credential_refresh::RefreshFuture<'_, crate::credential_refresh::RefreshedCredential>
    {
        Box::pin(async {
            self.rereads.fetch_add(1, Ordering::SeqCst);
            self.rejected.lock().unwrap().take()
        })
    }
}
fn fresh_credential(
    token: &str,
    account: &str,
    reason: &str,
) -> crate::credential_refresh::RefreshedCredential {
    crate::credential_refresh::RefreshedCredential {
        auth: AuthHandle::Headers(vec![
            ("authorization".into(), format!("Bearer {token}")),
            ("chatgpt-account-id".into(), account.into()),
            ("session-id".into(), "session".into()),
            ("originator".into(), "codex_cli_rs".into()),
            ("user-agent".into(), "codex_cli_rs/0.144.0".into()),
        ]),
        account_ref: Some(account.into()),
        evidence: CredentialRefresh {
            vault_op: "get_scoped".into(),
            min_ttl_ms: Some(300_000),
            force_refresh: false,
            record_version: Some(2),
            reason: Some(reason.into()),
        },
    }
}

#[tokio::test]
async fn websocket_token_refresh_between_steps_reauthenticates_before_sending() {
    let mut first = text_events();
    first.insert(0, serde_json::json!({"type":"response.metadata","headers":{"x-codex-turn-state":"old-token-routing"}}));
    let mock = Mock::start(vec![first, text_events()], false, false).await;
    let refresh = Arc::new(RefreshScript {
        expiring: Mutex::new(std::collections::VecDeque::from([
            None,
            Some(fresh_credential("fresh-token", "account", "expiring")),
        ])),
        rejected: Mutex::new(None),
        rereads: AtomicUsize::new(0),
    });
    let provider = provider(&mock)
        .with_account_refs(Some("account".into()), Some("account".into()))
        .with_credential_refresher(refresh);
    let first = step(&provider, 1, &opts()).await;
    let second = step(&provider, 2, &opts()).await;
    assert_eq!(attempts(&first)[0].outcome, AttemptOutcome::Complete);
    assert_eq!(attempts(&second)[0].outcome, AttemptOutcome::Complete);
    assert!(attempts(&second)[0].credential_refresh.is_some());
    {
        let seen = mock.observed.lock().unwrap();
        assert_eq!(
            seen.handshakes, 2,
            "a changed token requires exactly one fresh handshake"
        );
        assert!(seen.headers[0]["authorization"] == "Bearer mock-token");
        assert!(
            seen.headers[1]["authorization"] == "Bearer fresh-token",
            "the second step must authenticate with the current token"
        );
        assert_eq!(seen.requests.len(), 2);
        assert!(seen.http_bodies.is_empty());
        let second_body: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
        assert!(
            second_body.get("client_metadata").is_none(),
            "old credential routing must be discarded"
        );
    }
    mock.wait_closes(1).await;
}

#[tokio::test]
async fn websocket_upgrade_auth_refusal_rereads_and_recovers_without_https() {
    // Only a 401 upgrade is a token rejection. A 403 upgrade falls back to HTTPS instead; see
    // `websocket_upgrade_403_falls_back_to_https_without_a_credential_reread`.
    {
        let status = 401;
        let mock =
            Mock::start_internal(vec![text_events()], false, false, Some(status), false).await;
        let refresh = Arc::new(RefreshScript {
            expiring: Mutex::new(Default::default()),
            rejected: Mutex::new(Some(fresh_credential("fresh-token", "account", "rejected"))),
            rereads: AtomicUsize::new(0),
        });
        let provider = provider(&mock)
            .with_account_refs(Some("account".into()), Some("account".into()))
            .with_credential_refresher(refresh.clone());
        let events = step(&provider, 1, &opts()).await;
        let attempts = attempts(&events);
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].http_status, Some(status));
        assert_eq!(attempts[0].outcome, AttemptOutcome::HttpError);
        assert_eq!(attempts[1].outcome, AttemptOutcome::Complete);
        assert!(attempts
            .iter()
            .all(|attempt| attempt.transport.as_deref() == Some("websocket")));
        assert_eq!(refresh.rereads.load(Ordering::SeqCst), 1);
        let seen = mock.observed.lock().unwrap();
        assert_eq!(seen.handshakes, 2);
        assert_eq!(
            seen.requests.len(),
            1,
            "no model request was sent under the rejected token"
        );
        assert!(seen.headers[1]["authorization"] == "Bearer fresh-token");
        assert!(
            seen.http_bodies.is_empty(),
            "auth rejection must never silently fall back with stale credentials"
        );
    }
}

async fn paused_run(provider: &impl Provider, label: &str) -> Vec<broca_wal::FramedRecord> {
    let dir = broca_test_support::scratch_root().join(label);
    let bind = BindIdentity::new(dir.join("project"), "websocket-auth", label);
    let request = RunRequest {
        directives: None,
        session_class: None,
        retention: None,
        on_restart: None,
        keep_warm: None,
        send_id: None,
        request_identity_digest: None,
        title: None,
        project_id: None,
        principal: None,
        scope: None,
        run_id: RunId(label.into()),
        session: bind.clone(),
        model: ModelHandle::new("openai", "m"),
        input: vec![Message {
            role: Role::User,
            content: vec![ContentBlock::Text { text: "hi".into() }],
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
        }],
        tools: vec![],
        policy_allowed: Default::default(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 2 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    };
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(256);
    let collect = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        run_loop(
            request,
            provider,
            &StubToolPlane::new(),
            &FileLeaseStore::new(dir.join("lease")),
            &AllowAll,
            &wal,
            &tx,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    drop(tx);
    collect.await.unwrap();
    assert!(
        result.paused.is_some(),
        "auth failures must durably pause the run"
    );
    wal.read_records(&bind).unwrap()
}

#[tokio::test]
async fn websocket_account_mismatch_pauses_before_any_socket_send() {
    let mock = Mock::start(vec![text_events()], false, false).await;
    let refresh = Arc::new(RefreshScript {
        expiring: Mutex::new(std::collections::VecDeque::from([Some(fresh_credential(
            "other-token",
            "other-account",
            "expiring",
        ))])),
        rejected: Mutex::new(None),
        rereads: AtomicUsize::new(0),
    });
    let provider = provider(&mock)
        .with_account_refs(Some("account".into()), Some("account".into()))
        .with_credential_refresher(refresh);
    let records = paused_run(&provider, "ws-account-mismatch").await;
    assert!(records.iter().any(|frame| matches!(&frame.record, WalRecord::RunPaused { error: Some(error), .. } if error.provider_code.as_deref() == Some("account_changed"))));
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 0);
    assert!(seen.requests.is_empty());
    assert!(seen.http_bodies.is_empty());
}

#[tokio::test]
async fn websocket_upgrade_auth_refusal_without_a_fresh_token_pauses() {
    let mock = Mock::start_internal(vec![], false, false, Some(401), false).await;
    let refresh = Arc::new(RefreshScript {
        expiring: Mutex::new(Default::default()),
        rejected: Mutex::new(None),
        rereads: AtomicUsize::new(0),
    });
    let provider = provider(&mock)
        .with_account_refs(Some("account".into()), Some("account".into()))
        .with_credential_refresher(refresh.clone());
    let records = paused_run(&provider, "ws-unrecoverable-auth").await;
    assert_eq!(refresh.rereads.load(Ordering::SeqCst), 1);
    assert!(records.iter().any(|frame| matches!(&frame.record, WalRecord::RunPaused { error: Some(error), .. } if error.status == Some(401))));
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 1);
    assert!(seen.requests.is_empty());
    assert!(seen.http_bodies.is_empty());
}

const UPGRADE_SECRET_BODY: &str = "PRIVATE-UPGRADE-BODY Bearer mock-token";

fn upgrade_refusal(status: u16, body: &str) -> UpgradeRefusal {
    UpgradeRefusal {
        status,
        headers: vec![
            ("cf-ray", "8f00aa11bb22cc33-FRA"),
            ("x-request-id", "req-upgrade-1"),
            ("server", "cloudflare"),
            ("x-codex-plan-type", "pro"),
            ("content-type", "application/json"),
            ("set-cookie", "__cf_bm=PRIVATE-COOKIE"),
            ("x-private-echo", "Bearer mock-token"),
        ],
        body: body.into(),
    }
}

fn rejected_token_refresher() -> Arc<RefreshScript> {
    Arc::new(RefreshScript {
        expiring: Mutex::new(Default::default()),
        rejected: Mutex::new(Some(fresh_credential("fresh-token", "account", "rejected"))),
        rereads: AtomicUsize::new(0),
    })
}

#[tokio::test]
async fn websocket_upgrade_403_falls_back_to_https_without_a_credential_reread() {
    let mock = Mock::start(vec![], false, false).await;
    *mock.upgrade_refusal.lock().unwrap() = Some(upgrade_refusal(403, UPGRADE_SECRET_BODY));
    let refresh = rejected_token_refresher();
    let provider = provider(&mock)
        .with_account_refs(Some("account".into()), Some("account".into()))
        .with_credential_refresher(refresh.clone());
    let first = step(&provider, 1, &opts()).await;
    let second = step(&provider, 2, &opts()).await;
    for events in [&first, &second] {
        let attempts = attempts(events);
        assert_eq!(
            attempts.len(),
            1,
            "the HTTPS fallback is the same attempt, not a retry"
        );
        assert_eq!(attempts[0].transport.as_deref(), Some("https"));
        assert_eq!(attempts[0].http_status, Some(200));
        assert_eq!(attempts[0].outcome, AttemptOutcome::Complete);
        assert!(attempts[0].credential_refresh.is_none());
    }
    assert_eq!(
        refresh.rereads.load(Ordering::SeqCst),
        0,
        "an upgrade 403 is not a token rejection"
    );
    let seen = mock.observed.lock().unwrap();
    assert_eq!(
        seen.handshakes, 1,
        "after an upgrade 403 the rest of the run uses HTTPS"
    );
    assert!(seen.requests.is_empty());
    assert_eq!(seen.http_bodies.len(), 2);
    assert!(seen.headers[0]["authorization"] == "Bearer mock-token");
}

#[tokio::test]
async fn websocket_upgrade_refusal_warns_once_with_only_safe_headers() {
    let mock = Mock::start(vec![], false, false).await;
    *mock.upgrade_refusal.lock().unwrap() = Some(upgrade_refusal(403, UPGRADE_SECRET_BODY));
    let provider = provider(&mock);
    let (events, logs) = logged_step(&provider).await;
    assert_eq!(attempts(&events)[0].outcome, AttemptOutcome::Complete);
    let warnings = logs.0.lock().unwrap();
    let refusals: Vec<_> = warnings
        .iter()
        .filter(|warning| warning.contains_key("upgrade_status"))
        .collect();
    assert_eq!(
        refusals.len(),
        1,
        "one WARN per refused upgrade: {warnings:?}"
    );
    assert_eq!(refusals[0]["upgrade_status"], "403");
    assert_eq!(refusals[0]["next"], "https");
    let headers = &refusals[0]["headers"];
    for safe in [
        "cf-ray=8f00aa11bb22cc33-FRA",
        "x-request-id=req-upgrade-1",
        "server=cloudflare",
        "x-codex-plan-type=pro",
        "content-type=application/json",
    ] {
        assert!(headers.contains(safe), "{safe} missing from {headers}");
    }
    assert!(
        !warnings.iter().any(|warning| warning
            .get("message")
            .is_some_and(|message| message.contains("upgrade failed; using HTTPS"))),
        "the refusal is not reported a second time as a generic upgrade failure"
    );
    let logged = format!("{warnings:?}");
    for private in ["PRIVATE-", "mock-token", "Bearer", "cookie"] {
        assert!(
            !logged.contains(private),
            "upgrade WARN must omit the body, cookies and credentials"
        );
    }
}

const USAGE_LIMIT_BODY: &str = r#"{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"pro","resets_at":1790021043,"eligible_promo":null,"resets_in_seconds":441721}}"#;

#[tokio::test]
async fn websocket_upgrade_403_then_https_usage_limit_is_account_limit_exhausted() {
    let mock = Mock::start(vec![], false, false).await;
    *mock.upgrade_refusal.lock().unwrap() = Some(upgrade_refusal(403, UPGRADE_SECRET_BODY));
    *mock.http_error.lock().unwrap() = Some((429, USAGE_LIMIT_BODY.into()));
    let refresh = rejected_token_refresher();
    let provider = provider(&mock)
        .with_account_refs(Some("account".into()), Some("account".into()))
        .with_credential_refresher(refresh.clone());
    let events = step(&provider, 1, &opts()).await;
    let error = terminal_error(&events);
    assert!(error.is_account_limit_exhausted(), "{error:?}");
    assert_eq!(error.status, Some(429));
    assert_eq!(error.resets_at_ms, Some(1_790_021_043_000));
    let attempts = attempts(&events);
    assert_eq!(attempts.len(), 1, "an exhausted account is not retried");
    assert_eq!(attempts[0].transport.as_deref(), Some("https"));
    assert_eq!(attempts[0].http_status, Some(429));
    assert_eq!(refresh.rereads.load(Ordering::SeqCst), 0);
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 1);
    assert_eq!(seen.http_bodies.len(), 1);
}

#[tokio::test]
async fn websocket_upgrade_usage_limit_body_is_reported_without_https() {
    let mock = Mock::start(vec![], false, false).await;
    *mock.upgrade_refusal.lock().unwrap() = Some(upgrade_refusal(429, USAGE_LIMIT_BODY));
    let refresh = rejected_token_refresher();
    let provider = provider(&mock)
        .with_account_refs(Some("account".into()), Some("account".into()))
        .with_credential_refresher(refresh.clone());
    let events = step(&provider, 1, &opts()).await;
    let error = terminal_error(&events);
    assert!(error.is_account_limit_exhausted(), "{error:?}");
    assert_eq!(error.resets_at_ms, Some(1_790_021_043_000));
    assert!(
        !error.message.contains("usage limit"),
        "the upgrade body is classified, never carried in the message"
    );
    let attempts = attempts(&events);
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].transport.as_deref(), Some("websocket"));
    assert_eq!(attempts[0].http_status, Some(429));
    assert_eq!(refresh.rereads.load(Ordering::SeqCst), 0);
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 1);
    assert!(
        seen.http_bodies.is_empty(),
        "a usage limit stated by the upgrade needs no HTTPS round trip"
    );
}

fn virtual_attempts(provider: &mut LiveProvider<OpenAiResponsesChatgpt>, seconds: &[u64]) {
    provider.retry_clock.virtual_waits = Some(Arc::new(Mutex::new(Vec::new())));
    provider.retry_clock.virtual_retry_attempts = Some(Arc::new(Mutex::new(
        seconds
            .iter()
            .map(|seconds| Duration::from_secs(*seconds))
            .collect(),
    )));
}

async fn logged_step(
    provider: &LiveProvider<OpenAiResponsesChatgpt>,
) -> (
    Vec<NormalizedEvent>,
    super::chunk_error_tests::ProviderWarnings,
) {
    use tracing::instrument::WithSubscriber;
    let body = provider.render_request(&opts());
    let digest = RequestDigest::of(&body);
    let mut request: PreparedHttpRequest = provider
        .family
        .endpoint(&provider.frozen, &provider.current_auth().auth, body)
        .into();
    request.family = Some(crate::spec::WireFamilyId::OpenAiResponsesChatgpt);
    let (tx, mut rx) = mpsc::channel(256);
    let cancel = CancellationToken::new();
    let logs = super::chunk_error_tests::ProviderWarnings::default();
    let result = drive_step_attempts(
        &provider.client,
        &provider.clock,
        request,
        &digest,
        None,
        None,
        || ToolNameRestoringNormalizer {
            live_progress: None,
            inner: provider.family.normalizer(&provider.frozen, StepId(1)),
            names: Arc::new(crate::tool_name_alias::ToolNameMap::build(&[], &[])),
        },
        &tx,
        &cancel,
        None,
        StepSeal {
            seal: None,
            step_id: StepId(1),
            cache_ttl_ms: None,
        },
        &provider.retry_clock,
        Some(&provider.websocket),
        &AtomicUsize::new(0),
    )
    .with_subscriber(logs.clone())
    .await;
    drop(tx);
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
    }
    match result {
        Ok((completion, _)) => events.extend(completion),
        Err((error, retries_used)) => {
            events.push(NormalizedEvent::Control(ProviderControl::Error {
                error,
                retries_used,
            }))
        }
    }
    (events, logs)
}

#[tokio::test]
async fn websocket_two_long_stream_cuts_switch_to_https_and_complete() {
    let partial = vec![serde_json::json!({"type":"response.output_text.delta","delta":"partial"})];
    let mock = Mock::start_gated(vec![partial.clone(), partial, text_events()]).await;
    let mut provider = provider(&mock);
    virtual_attempts(&mut provider, &[305, 533, 1]);
    provider.retry_clock.virtual_attempt_elapsed = Some(Duration::from_secs(533));
    let exercise = async {
        let (events, logs) = logged_step(&provider).await;
        let recorded = attempts(&events);
        assert_eq!(recorded.len(), 3);
        assert_eq!(recorded[2].outcome, AttemptOutcome::Complete);
        assert_eq!(recorded[2].transport.as_deref(), Some("https"));
        assert!(recorded
            .iter()
            .all(|attempt| attempt.request_body_sha256 == recorded[0].request_body_sha256));
        let warnings = logs.0.lock().unwrap();
        assert_eq!(warnings.len(), 2);
        assert_eq!(warnings[0]["retry_decision"], "retrying");
        assert_eq!(warnings[1]["retry_decision"], "switching_to_https");
        assert_eq!(
            warnings[1]["retry_reason"],
            "consecutive_mid_response_closes"
        );
        assert_eq!(warnings[0]["socket_generation"], "1");
        assert_eq!(warnings[1]["socket_generation"], "2");
        for warning in warnings.iter() {
            assert_eq!(warning["socket_reused"], "false");
            assert_eq!(warning["output_seen"], "true");
            assert!(warning["socket_age_ms"].parse::<u64>().unwrap() >= 533_000);
            assert!(!warning.contains_key("socket_since_last_completed_ms"));
        }
    };
    let release = async {
        for n in 1..=2 {
            mock.wait_held_cut(n).await;
            mock.cut_gates.add_permits(1);
        }
    };
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        tokio::join!(exercise, release);
    })
    .await
    .unwrap();
    assert_eq!(mock.observed.lock().unwrap().requests.len(), 2);
    assert_eq!(mock.observed.lock().unwrap().http_bodies.len(), 1);
    provider.retry_clock.virtual_retry_attempts = None;
    let events = step(&provider, 2, &opts()).await;
    assert_eq!(attempts(&events)[0].transport.as_deref(), Some("https"));
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.requests[0], seen.requests[1]);
    let mut socket_body: Value = serde_json::from_slice(&seen.requests[0]).unwrap();
    socket_body.as_object_mut().unwrap().remove("type");
    assert_eq!(
        socket_body,
        serde_json::from_slice::<Value>(&seen.http_bodies[0]).unwrap()
    );
}

#[tokio::test]
async fn websocket_attempt_exhaustion_recovers_the_same_step_over_https() {
    let mut replies = vec![vec![]; MAX_ATTEMPTS];
    replies.push(text_events());
    let mock = Mock::start(replies, true, false).await;
    let mut provider = provider(&mock);
    virtual_attempts(&mut provider, &[0, 0, 0, 0, 0, 170, 1]);
    let (events, logs) = logged_step(&provider).await;
    let recorded = attempts(&events);
    assert_eq!(recorded.len(), MAX_ATTEMPTS + 1);
    assert_eq!(recorded[MAX_ATTEMPTS].transport.as_deref(), Some("https"));
    assert_eq!(recorded[MAX_ATTEMPTS].outcome, AttemptOutcome::Complete);
    assert!(recorded
        .iter()
        .all(|attempt| attempt.request_body_sha256 == recorded[0].request_body_sha256));
    let warnings = logs.0.lock().unwrap();
    assert_eq!(
        warnings[MAX_ATTEMPTS - 1]["retry_decision"],
        "switching_to_https"
    );
    assert_eq!(
        warnings[MAX_ATTEMPTS - 1]["retry_reason"],
        "socket_attempt_limit"
    );
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, MAX_ATTEMPTS);
    assert!(seen.requests.iter().all(|body| body == &seen.requests[0]));
    assert_eq!(seen.http_bodies[0], provider.render_request(&opts()));
}

#[tokio::test]
async fn websocket_https_recovery_has_a_separate_bounded_allowance() {
    let mut replies = vec![vec![]; MAX_ATTEMPTS];
    replies.extend((0..MAX_ATTEMPTS).map(|_| vec![serde_json::json!({"type":"response.failed","response":{"error":{"code":"server_error","message":"unavailable"}}})]));
    let mock = Mock::start(replies, true, false).await;
    let mut provider = provider(&mock);
    provider.retry_clock.virtual_waits = Some(Arc::new(Mutex::new(Vec::new())));
    let events = step(&provider, 1, &opts()).await;
    assert_eq!(attempts(&events).len(), 2 * MAX_ATTEMPTS);
    terminal_error(&events);
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.requests.len(), MAX_ATTEMPTS);
    assert_eq!(seen.http_bodies.len(), MAX_ATTEMPTS);
    assert!(seen
        .http_bodies
        .iter()
        .all(|body| body == &seen.http_bodies[0]));
}

#[tokio::test]
async fn websocket_no_bytes_failures_still_exhaust_the_waiting_wall() {
    let mock = Mock::start(vec![vec![]; MAX_ATTEMPTS], true, false).await;
    let mut provider = provider(&mock);
    virtual_attempts(&mut provider, &[100, 145, 0, 0, 0, 0, 0]);
    let (events, logs) = logged_step(&provider).await;
    assert_eq!(attempts(&events).len(), 2);
    terminal_error(&events);
    assert_eq!(
        logs.0.lock().unwrap()[1]["retry_decision"],
        "wall_exhausted"
    );
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.requests.len(), 2);
    assert!(seen.http_bodies.is_empty());
}

#[tokio::test]
async fn websocket_over_age_socket_is_retired_before_the_next_send() {
    let mock = Mock::start(vec![text_events(); 3], false, false).await;
    let provider = provider(&mock);
    for id in 1..=2 {
        assert_eq!(
            attempts(&step(&provider, id, &opts()).await)[0].outcome,
            AttemptOutcome::Complete
        );
    }
    assert_eq!(mock.observed.lock().unwrap().handshakes, 1);
    provider
        .websocket
        .advance_clock(Duration::from_secs(40 * 60 + 1));
    assert_eq!(
        attempts(&step(&provider, 3, &opts()).await)[0].outcome,
        AttemptOutcome::Complete
    );
    assert_eq!(mock.observed.lock().unwrap().handshakes, 2);
    mock.wait_closes(1).await;
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 2);
    assert_eq!(seen.requests.len(), 3);
    assert!(seen.requests.iter().all(|body| body == &seen.requests[0]));
}

fn tool_run_request(bind: &BindIdentity) -> RunRequest {
    RunRequest { directives: None, session_class: None, retention: None,
    on_restart: None, keep_warm: None, send_id: None, request_identity_digest: None, title: None,
    project_id: None, principal: None, scope: None, run_id: RunId("ws-run".into()), session: bind.clone(),
    model: ModelHandle::new("openai", "m"),
    input: vec![Message { role: Role::User, content: vec![ContentBlock::Text { text: "lookup".into() }], cache_prefix_blocks: None, origin: None, ordinal: None, tags: Default::default() }],
    tools: vec![serde_json::from_value(serde_json::json!({"name":"lookup","description":"lookup","input_schema":{"type":"object","properties":{"x":{"type":"integer"}}},"execution_mode":"parallel"})).unwrap()],
    policy_allowed: std::collections::HashSet::from(["lookup".into()]), tool_choice: ToolChoice::Auto, generation: GenerationConfig::default(),
    cache: None, stop_when: vec![StopCondition::StepCount { n: 4 }], submission_id: None,
    budget: None, context_limit: None, work_class: None, context_management: None, variant: None, service_tier: None, inherit_frozen_cache: false, structured_output: None, }
}

#[tokio::test]
async fn websocket_retried_tool_step_dispatches_only_the_successful_completion() {
    let partial = tool_events()[..2].to_vec();
    let mock =
        Mock::start_gated(vec![partial.clone(), partial, tool_events(), text_events()]).await;
    let mut provider = provider(&mock);
    virtual_attempts(&mut provider, &[105, 533, 1, 1]);
    let dir = broca_test_support::scratch_root().join("ws-retried-tool-turn");
    let bind = BindIdentity::new(dir.join("project"), "websocket", "retried-turn");
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(256);
    let collect = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let plane = StubToolPlane::new().with_canned_text("lookup", "found");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let run = run_loop(
        tool_run_request(&bind),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        &tx,
    );
    let release = async {
        for n in 1..=2 {
            mock.wait_held_cut(n).await;
            let records = wal.read_records(&bind).unwrap();
            assert!(
                plane.log().is_empty(),
                "failed model output cannot dispatch a tool"
            );
            assert!(!records.iter().any(|frame| matches!(
                frame.record,
                WalRecord::ModelStepFinished { .. } | WalRecord::ToolResult { .. }
            )));
            mock.cut_gates.add_permits(1);
        }
    };
    let (result, _) = tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        tokio::join!(run, release)
    })
    .await
    .unwrap();
    assert_eq!(result.unwrap().reason, RunFinishReason::Completed);
    assert_eq!(
        plane.exec_count(&broca_protocol::ToolCallId("call1".into())),
        1
    );
    assert_eq!(
        plane
            .log()
            .iter()
            .filter(|event| matches!(
                event,
                broca_core::StubEvent::Dispatched(_) | broca_core::StubEvent::Fenced(_)
            ))
            .count(),
        1
    );
    drop(tx);
    collect.await.unwrap();
    let records = wal.read_records(&bind).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ToolResult { .. }))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ModelStepFinished { .. }))
            .count(),
        2
    );
    let recorded: Vec<_> = records
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::ModelAttemptFinished {
                request_body_sha256,
                transport,
                outcome,
                ..
            } => Some((request_body_sha256, transport, outcome)),
            _ => None,
        })
        .collect();
    assert_eq!(recorded.len(), 4);
    assert!(recorded[..3]
        .iter()
        .all(|attempt| attempt.0 == recorded[0].0));
    assert_eq!(recorded[2].1.as_deref(), Some("https"));
    assert_eq!(*recorded[2].2, AttemptOutcome::Complete);
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.requests.len(), 2);
    assert_eq!(seen.requests[0], seen.requests[1]);
    assert_eq!(seen.http_bodies.len(), 2);
}

#[tokio::test]
async fn websocket_reused_failure_reports_age_and_time_since_completion() {
    let mock = Mock::start(
        vec![
            text_events(),
            vec![serde_json::json!({"type":"error","status":500,"error":{"code":"server_error"}})],
            text_events(),
        ],
        false,
        false,
    )
    .await;
    let mut provider = provider(&mock);
    provider.retry_clock.virtual_waits = Some(Arc::new(Mutex::new(Vec::new())));
    assert_eq!(
        attempts(&step(&provider, 1, &opts()).await)[0].outcome,
        AttemptOutcome::Complete
    );
    provider.websocket.advance_clock(Duration::from_secs(123));
    let (_, logs) = logged_step(&provider).await;
    let warnings = logs.0.lock().unwrap();
    let warning = warnings
        .iter()
        .find(|warning| {
            warning
                .get("socket_reused")
                .is_some_and(|reused| reused == "true")
        })
        .unwrap();
    assert_eq!(warning["socket_generation"], "1");
    assert!(warning["socket_age_ms"].parse::<u64>().unwrap() >= 123_000);
    assert!(
        warning["socket_since_last_completed_ms"]
            .parse::<u64>()
            .unwrap()
            >= 123_000
    );
    assert_eq!(warning["output_seen"], "false");
}

#[tokio::test]
async fn websocket_run_failure_threshold_starts_a_full_https_allowance() {
    let mut replies = vec![vec![]; 4];
    replies.push(text_events());
    replies.extend(vec![vec![]; 2]);
    replies.extend((0..MAX_ATTEMPTS - 1).map(|_| vec![serde_json::json!({"type":"response.failed","response":{"error":{"code":"server_error","message":"unavailable"}}})]));
    replies.push(text_events());
    let mock = Mock::start(replies, true, false).await;
    let mut provider = provider(&mock);
    provider.retry_clock.virtual_waits = Some(Arc::new(Mutex::new(Vec::new())));
    let first = step(&provider, 1, &opts()).await;
    assert_eq!(attempts(&first).len(), 5);
    assert_eq!(attempts(&first)[4].outcome, AttemptOutcome::Complete);
    let second = step(&provider, 2, &opts()).await;
    let recorded = attempts(&second);
    assert_eq!(recorded.len(), 2 + MAX_ATTEMPTS);
    assert!(recorded[2..]
        .iter()
        .all(|attempt| attempt.transport.as_deref() == Some("https")));
    assert_eq!(recorded.last().unwrap().outcome, AttemptOutcome::Complete);
    assert_eq!(
        mock.observed.lock().unwrap().http_bodies.len(),
        MAX_ATTEMPTS
    );
}

#[tokio::test]
async fn trailing_tool_whitespace_closes_websocket_and_next_step_uses_a_new_socket() {
    let events = vec![
        serde_json::json!({"type":"response.created","response":{"id":"r1"}}),
        serde_json::json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"bad","name":"lookup","arguments":""}}),
        serde_json::json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"key\":\"alpha\"}"}),
        serde_json::json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":" ".repeat(4097)}),
    ];
    let mock = Mock::start(vec![events, text_events()], false, false).await;
    let provider = provider(&mock);
    let first = step(&provider, 1, &opts()).await;
    assert_eq!(attempts(&first).len(), 1);
    assert_eq!(
        attempts(&first)[0].outcome,
        AttemptOutcome::ToolArgsTrailingGarbage
    );
    assert!(tool_args_cut(&first));
    mock.wait_closes(1).await;
    let second = step(&provider, 2, &opts()).await;
    assert_eq!(attempts(&second)[0].outcome, AttemptOutcome::Complete);
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 2);
    assert_eq!(seen.requests.len(), 2);
    assert!(
        seen.http_bodies.is_empty(),
        "the local cut must not trigger a retry or HTTPS fallback"
    );
}

/// A Responses socket can wrap an HTTP refusal in an `error` event. The ChatGPT decisions apply
/// to that wrapped body the same way they apply to an HTTPS answer: the captured usage-limit
/// body is an account wall with the body's reset and no retry, and the backend's own `sk-` key
/// rejection is transient rather than a failure of the token broca sent.
#[test]
fn wrapped_chatgpt_refusals_keep_their_classes() {
    let usage = serde_json::json!({"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"pro","resets_at":1790021043,"eligible_promo":null,"resets_in_seconds":441721}});
    let mut trace = AttemptTrace::default();
    let failure = responses_websocket::wrapped_error(usage.to_string().as_bytes(), &mut trace)
        .expect("a wrapped 429 is a refusal");
    assert!(failure.error.is_account_limit_exhausted());
    assert_eq!(failure.error.class, ErrorClass::Transient);
    assert_eq!(failure.error.resets_at_ms, Some(1_790_021_043_000));
    assert_eq!(failure.error.resets_at_source.as_deref(), Some("body"));
    assert!(failure.retry_forbidden);
    assert_eq!(trace.http_status, Some(429));
    assert_eq!(
        trace.account_observation.limit,
        Some(broca_protocol::AttemptAccountLimit {
            resets_at_ms: Some(1_790_021_043_000),
            resets_at_source: Some("body".into()),
            limit_window: None,
        })
    );

    let key = serde_json::json!({"type":"error","status":401,"error":{"message":"Incorrect API key provided: sk-svcac***********fvMA. You can find your API key at https://platform.openai.com/account/api-keys.","type":"invalid_request_error","param":null,"code":"invalid_api_key"}});
    let mut trace = AttemptTrace::default();
    let failure = responses_websocket::wrapped_error(key.to_string().as_bytes(), &mut trace)
        .expect("a wrapped 401 is a refusal");
    assert_eq!(failure.error.class, ErrorClass::Transient);
    assert_eq!(
        failure.error.provider_code.as_deref(),
        Some(super::CHATGPT_UPSTREAM_KEY_REJECTED)
    );
    assert!(!failure.retry_forbidden);
    assert_eq!(trace.account_observation.limit, None);
}

/// The production refusal of a structured-output schema, as the ChatGPT backend wraps it in a
/// websocket error frame. RECONSTRUCTED, NOT CAPTURED: only `status`, `error.type`, `error.code`
/// and `error.param` were recorded; the message text is written for the test, with a bearer
/// token in it to prove the message never leaves the frame.
fn request_invalid_frame() -> Value {
    serde_json::json!({"type":"error","status":400,"error":{"type":"invalid_request_error","code":"invalid_json_schema","param":"text.format.schema","message":"Invalid schema for response_format 'result': PRIVATE-SCHEMA-TEXT Bearer mock-token"}})
}

/// A wrapped request-validation 400 takes its typed code from the frame's `error.type` and its
/// detail from `error.code`. The message is never read into either.
#[test]
fn wrapped_request_invalid_frame_extracts_type_and_code_only() {
    let mut trace = AttemptTrace::default();
    let failure = responses_websocket::wrapped_error(
        request_invalid_frame().to_string().as_bytes(),
        &mut trace,
    )
    .expect("a wrapped 400 is a refusal");
    assert_eq!(failure.error.class, ErrorClass::Permanent);
    assert_eq!(
        failure.error.provider_code.as_deref(),
        Some(broca_protocol::error::REQUEST_INVALID)
    );
    assert_eq!(
        failure.error.provider_subcode.as_deref(),
        Some("invalid_json_schema")
    );
    assert_eq!(failure.error.status, Some(400));
    assert_eq!(trace.http_status, Some(400));
    // With `error.code` null, `error.type` alone still marks it request_invalid.
    let mut frame = request_invalid_frame();
    frame["error"]["code"] = Value::Null;
    let failure = responses_websocket::wrapped_error(frame.to_string().as_bytes(), &mut trace)
        .expect("a wrapped 400 is a refusal");
    assert_eq!(
        failure.error.provider_code.as_deref(),
        Some(broca_protocol::error::REQUEST_INVALID)
    );
    assert_eq!(failure.error.provider_subcode, None);
    // A wrapped 400 without the request-validation type gets no typed code.
    let mut frame = request_invalid_frame();
    frame["error"]["type"] = Value::String("permission_error".into());
    let failure = responses_websocket::wrapped_error(frame.to_string().as_bytes(), &mut trace)
        .expect("a wrapped 400 is a refusal");
    assert_ne!(
        failure.error.provider_code.as_deref(),
        Some(broca_protocol::error::REQUEST_INVALID)
    );
    assert_eq!(failure.error.provider_subcode, None);
}

/// Through the real websocket step: the refusal ends the step on its one attempt, carries the
/// typed code and detail through the websocket failure handling (which replaces the message and
/// keeps only allowlisted codes), and still omits the frame's message. The run error's message
/// names the frame's status and its identifier-shaped fields so a reader can tell why the request
/// was refused; the schema path in `param` is such an identifier, the schema text is not.
#[tokio::test]
async fn websocket_request_invalid_ends_the_step_with_typed_code_and_no_body() {
    let mock = Mock::start(
        vec![vec![request_invalid_frame()], text_events()],
        false,
        false,
    )
    .await;
    let provider = provider(&mock);
    let events = step(&provider, 1, &opts()).await;
    let error = terminal_error(&events);
    assert_eq!(error.class, ErrorClass::Permanent);
    assert_eq!(
        error.provider_code.as_deref(),
        Some(broca_protocol::error::REQUEST_INVALID)
    );
    assert_eq!(
        error.provider_subcode.as_deref(),
        Some("invalid_json_schema")
    );
    assert_eq!(error.status, Some(400));
    assert_eq!(
        error.message,
        "Responses websocket attempt failed (HttpError 400); code=invalid_json_schema type=invalid_request_error param=text.format.schema; message omitted"
    );
    let rendered = serde_json::to_string(error).unwrap();
    assert!(!rendered.contains("PRIVATE-SCHEMA-TEXT"), "{rendered}");
    assert!(!rendered.contains("mock-token"), "{rendered}");
    assert!(!rendered.contains("Invalid schema"), "{rendered}");
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Error {
            retries_used: 0,
            ..
        })
    )));
    assert_eq!(attempts(&events).len(), 1);
    assert_eq!(mock.observed.lock().unwrap().requests.len(), 1);
}

const MODEL_REFUSAL_TEXT: &str =
    "The 'gpt-5.6' model is not supported when using Codex with a ChatGPT account.";

/// A wrapped refusal with no identifier-shaped field (one that explains itself only in prose,
/// such as a refusal of a model the account cannot use) names its status and still omits the
/// frame's text from the run error.
#[tokio::test]
async fn websocket_wrapped_refusal_without_identifier_fields_names_only_its_status() {
    for frame in [
        serde_json::json!({"type":"error","status":400,"error":{"message":MODEL_REFUSAL_TEXT}}),
        serde_json::json!({"type":"error","status":400,"error":{"detail":MODEL_REFUSAL_TEXT}}),
        // Fields that are not identifier-shaped are left out, never rewritten.
        serde_json::json!({"type":"error","status":400,"error":{"message":MODEL_REFUSAL_TEXT,"code":"Not An Identifier","param":"input: Bearer mock-token"}}),
    ] {
        let mock = Mock::start(vec![vec![frame.clone()], text_events()], false, false).await;
        let provider = provider(&mock);
        let events = step(&provider, 1, &opts()).await;
        let error = terminal_error(&events);
        assert_eq!(error.class, ErrorClass::Permanent, "{frame}");
        assert_eq!(error.status, Some(400));
        assert_eq!(
            error.message,
            "Responses websocket attempt failed (HttpError 400); response body omitted",
            "{frame}"
        );
        assert_eq!(attempts(&events).len(), 1);
    }
}

/// The wrapped frame's identifier fields are read from `error`, with `param` also accepted at the
/// top level of the frame, and the logged copy drops every `headers` object and is capped.
#[test]
fn wrapped_error_frame_keeps_identifier_fields_and_a_capped_header_free_log_body() {
    let long = "x".repeat(4096);
    let frame = serde_json::json!({
        "type":"error",
        "status":400,
        "param":"input[3].content",
        "headers":{"x-private":"PRIVATE-HEADER-VALUE"},
        "error":{"type":"invalid_request_error","code":"model_not_supported","message":long,"headers":{"set-cookie":"PRIVATE-COOKIE"}}
    });
    let mut trace = AttemptTrace::default();
    responses_websocket::wrapped_error(frame.to_string().as_bytes(), &mut trace)
        .expect("a wrapped 400 is a refusal");
    let recorded = trace.wrapped_error_frame.expect("the frame is kept");
    assert_eq!(
        recorded.run_error_message(),
        "Responses websocket attempt failed (HttpError 400); code=model_not_supported type=invalid_request_error param=input[3].content; message omitted"
    );
    assert_eq!(recorded.log_body.len(), 1024);
    assert!(
        !recorded.log_body.contains("PRIVATE-"),
        "{}",
        recorded.log_body
    );
    assert!(
        !recorded.log_body.contains("headers"),
        "{}",
        recorded.log_body
    );
}

/// The failed attempt's WARN line carries the wrapped frame in its `error_frame` field, capped at
/// 1 KiB, so the backend's reason can be read in broca's own log, while the returned run error's
/// message omits the frame's text.
#[tokio::test]
async fn websocket_wrapped_refusal_warn_carries_the_capped_frame() {
    use tracing::instrument::WithSubscriber;
    let padding = "p".repeat(2048);
    let frame = serde_json::json!({"type":"error","status":400,"error":{"detail":MODEL_REFUSAL_TEXT,"padding":padding},"headers":{"x-private":"PRIVATE-HEADER-VALUE"}});
    let mock = Mock::start(vec![vec![frame]], true, false).await;
    let provider = provider(&mock);
    let mut request: PreparedHttpRequest = provider
        .family
        .endpoint(
            &provider.frozen,
            &provider.current_auth().auth,
            provider.render_request(&opts()),
        )
        .into();
    request.family = Some(crate::spec::WireFamilyId::OpenAiResponsesChatgpt);
    let (tx, _rx) = mpsc::channel(256);
    let mut trace = AttemptTrace::default();
    let logs = super::chunk_error_tests::ProviderWarnings::default();
    let failure = responses_websocket::drive(
        &provider.websocket,
        &provider.client,
        &request,
        &provider.clock,
        provider.family.normalizer(&provider.frozen, StepId(1)),
        &tx,
        &mut trace,
    )
    .with_subscriber(logs.clone())
    .await
    .unwrap_err();
    assert!(!failure.error.message.contains(MODEL_REFUSAL_TEXT));
    let warnings = logs.0.lock().unwrap();
    let attempt = warnings
        .iter()
        .find(|warning| {
            warning.get("message").map(String::as_str) == Some("model websocket attempt failed")
        })
        .expect("one WARN for the failed attempt");
    assert_eq!(attempt["transport"], "websocket");
    let logged = &attempt["error_frame"];
    assert!(logged.contains(MODEL_REFUSAL_TEXT), "{logged}");
    assert_eq!(logged.len(), 1024);
    assert!(!logged.contains("PRIVATE-HEADER-VALUE"), "{logged}");
}
