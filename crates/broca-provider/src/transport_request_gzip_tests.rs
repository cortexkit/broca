//! Request gzip and response decoding for `api.anthropic.com`, driven through the live
//! transport against a local server. The provider client resolves `api.anthropic.com` to that
//! server, so the request URL carries the first-party host as it does in production.

use std::{
    io::Write as _,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use broca_protocol::{
    display::DisplayEvent, CallOptions, ContentBlock, Message, ModelAttempt, NormalizedEvent,
    ProviderControl, Role, StepId,
};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;

use super::{lower_hex, request_gzip, LiveProvider, ProviderHttpClient};
use crate::{
    families::{anthropic_frozen_config, AnthropicMessages},
    wire::AuthHandle,
};

/// One request as the server received it: headers in arrival order, lowercased, and the raw
/// body bytes exactly as sent.
#[derive(Clone, Debug)]
struct Captured {
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Captured {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// The headers a re-send must repeat: all of them but the ones describing the body.
    fn headers_without_body_framing(&self) -> Vec<(String, String)> {
        self.headers
            .iter()
            .filter(|(key, _)| key != "content-encoding" && key != "content-length")
            .cloned()
            .collect()
    }
}

struct Reply {
    status: u16,
    headers: Vec<(&'static str, &'static str)>,
    body: Vec<u8>,
}

impl Reply {
    fn sse() -> Self {
        Self {
            status: 200,
            headers: vec![("content-type", "text/event-stream")],
            body: sse_body().into_bytes(),
        }
    }
}

struct Server {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<Captured>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    /// Answer each request with the next reply, one connection per request.
    async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let task = tokio::spawn(async move {
            let mut replies = replies.into_iter();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request(&mut socket).await;
                seen.lock().unwrap().push(request);
                let reply = replies.next().unwrap_or(Reply {
                    status: 500,
                    headers: Vec::new(),
                    body: b"unexpected extra request".to_vec(),
                });
                let mut head = format!(
                    "HTTP/1.1 {} Scripted\r\nContent-Length: {}\r\nConnection: close\r\n",
                    reply.status,
                    reply.body.len()
                );
                for (name, value) in &reply.headers {
                    head.push_str(&format!("{name}: {value}\r\n"));
                }
                head.push_str("\r\n");
                socket.write_all(head.as_bytes()).await.unwrap();
                socket.write_all(&reply.body).await.unwrap();
                socket.shutdown().await.ok();
            }
        });
        Self {
            address,
            requests,
            task,
        }
    }

    fn requests(&self) -> Vec<Captured> {
        self.requests.lock().unwrap().clone()
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Captured {
    let mut request = Vec::new();
    let mut chunk = [0u8; 8192];
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
    let head = String::from_utf8_lossy(&request[..header_end]).into_owned();
    let headers: Vec<(String, String)> = head
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        })
        .collect();
    let content_length = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .map(|(_, value)| value.parse::<usize>().unwrap())
        .unwrap_or(0);
    while request.len() < header_end + content_length {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "client disconnected before request body completed");
        request.extend_from_slice(&chunk[..read]);
    }
    Captured {
        headers,
        body: request[header_end..header_end + content_length].to_vec(),
    }
}

fn sse_records() -> Vec<String> {
    [
        r#"{"type":"message_start","message":{"id":"msg_gzip","type":"message","role":"assistant","model":"claude-test","content":[],"usage":{"input_tokens":10,"output_tokens":1}}}"#,
        r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}"#,
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}"#,
        r#"{"type":"content_block_stop","index":0}"#,
        r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}"#,
        r#"{"type":"message_stop"}"#,
    ]
    .iter()
    .map(|data| format!("data: {data}\n\n"))
    .collect()
}

fn sse_body() -> String {
    sse_records().concat()
}

/// A request whose rendered body is far above the 4,096-unit threshold, or well below it.
fn options(prompt_chars: usize) -> CallOptions {
    CallOptions::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "n".repeat(prompt_chars),
        }],
    )])
}

fn provider_for(base_url: String, client: &ProviderHttpClient) -> LiveProvider<AnthropicMessages> {
    let mut frozen = anthropic_frozen_config("claude-test");
    frozen.base_url = Some(base_url);
    LiveProvider::with_client_and_clock(
        client.clone(),
        AnthropicMessages,
        frozen,
        AuthHandle::None,
        broca_core::clock::Clock::default(),
    )
}

/// A provider whose requests go to `http://api.anthropic.com:<port>`, served by `server`.
fn anthropic_provider(
    server: &Server,
    gzip_enabled: bool,
) -> (LiveProvider<AnthropicMessages>, ProviderHttpClient) {
    let client = ProviderHttpClient::resolving_anthropic_api_to(server.address, gzip_enabled);
    let base = format!(
        "http://{}:{}",
        request_gzip::ANTHROPIC_API_HOST,
        server.address.port()
    );
    (provider_for(base, &client), client)
}

async fn run_step(
    provider: &LiveProvider<AnthropicMessages>,
    opts: &CallOptions,
) -> Vec<NormalizedEvent> {
    let mut rx = provider.start_step(
        StepId(1),
        Arc::new(opts.clone()),
        CancellationToken::new(),
        None,
    );
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
    }
    events
}

fn attempts(events: &[NormalizedEvent]) -> Vec<ModelAttempt> {
    events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                Some(attempt.clone())
            }
            _ => None,
        })
        .collect()
}

fn retries_used_at_finish(events: &[NormalizedEvent]) -> u32 {
    events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::Finished { retries_used, .. }) => {
                Some(*retries_used)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("the step must finish, got {events:?}"))
}

fn text_deltas(events: &[NormalizedEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Display(DisplayEvent::TextDelta { delta, .. }) => Some(delta.as_str()),
            _ => None,
        })
        .collect()
}

fn gunzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Read as _;
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_end(&mut out)
        .expect("the request body is one valid gzip member");
    out
}

fn sha256_hex(bytes: &[u8]) -> String {
    lower_hex(&Sha256::digest(bytes))
}

/// The decoded wire body is the canonical rendered body, then `\n` and 0 to 256 spaces and tabs.
fn assert_canonical_plus_padding(decoded: &[u8], canonical: &[u8]) {
    assert!(
        decoded.starts_with(canonical),
        "the decoded body must start with the canonical rendered bytes"
    );
    let tail = &decoded[canonical.len()..];
    assert_eq!(tail.first(), Some(&b'\n'), "padding starts with a newline");
    assert!(tail.len() - 1 <= request_gzip::MAX_PAD);
    assert!(tail[1..].iter().all(|byte| *byte == b' ' || *byte == b'\t'));
}

#[tokio::test]
async fn a_large_request_to_the_anthropic_host_is_gzipped_and_decodes_to_the_canonical_body() {
    let server = Server::start(vec![Reply::sse()]).await;
    let (provider, client) = anthropic_provider(&server, true);
    let opts = options(20_000);
    let canonical = broca_core::Provider::render_request(&provider, &opts);

    let events = run_step(&provider, &opts).await;

    assert_eq!(text_deltas(&events), "hello world");
    assert_eq!(retries_used_at_finish(&events), 0);
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    let sent = &requests[0];
    assert_eq!(sent.header("content-encoding"), Some("gzip"));
    assert_eq!(
        sent.header("accept-encoding"),
        Some(request_gzip::CLAUDE_CODE_ACCEPT_ENCODING)
    );
    assert_eq!(
        sent.header("content-length"),
        Some(sent.body.len().to_string().as_str()),
        "content-length is the compressed size"
    );
    assert!(sent.body.len() < canonical.len());
    assert_canonical_plus_padding(&gunzip(&sent.body), &canonical);
    // The attempt record still describes the canonical body, not the wire bytes.
    let records = attempts(&events);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].request_body_len, canonical.len() as u64);
    assert_eq!(records[0].request_body_sha256, sha256_hex(&canonical));
    assert!(!client.request_gzip.is_latched());
}

#[tokio::test]
async fn a_gzip_rejection_resends_once_uncompressed_and_latches_gzip_off() {
    let server = Server::start(vec![
        // A proxy in front of the API: no `request-id`, no `cf-ray`.
        Reply {
            status: 415,
            headers: vec![("content-type", "text/plain")],
            body: b"Unsupported Media Type".to_vec(),
        },
        Reply::sse(),
        Reply::sse(),
    ])
    .await;
    let (provider, client) = anthropic_provider(&server, true);
    let opts = options(20_000);
    let canonical = broca_core::Provider::render_request(&provider, &opts);

    let events = run_step(&provider, &opts).await;

    assert_eq!(text_deltas(&events), "hello world");
    assert_eq!(
        retries_used_at_finish(&events),
        0,
        "the uncompressed re-send is not one of the step's retries"
    );
    let records = attempts(&events);
    assert_eq!(records.len(), 1, "one attempt, recorded once: {records:?}");
    assert_eq!(records[0].attempt, 0);
    assert_eq!(records[0].http_status, Some(200));
    assert_eq!(records[0].request_body_sha256, sha256_hex(&canonical));
    assert_eq!(
        records[0].response_body_len,
        sse_body().len() as u64,
        "the refused response leaves nothing in the record"
    );
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].header("content-encoding"), Some("gzip"));
    assert_eq!(requests[1].header("content-encoding"), None);
    assert_eq!(
        requests[1].body, canonical,
        "the re-send carries the identity body without padding"
    );
    assert_eq!(
        requests[1].headers_without_body_framing(),
        requests[0].headers_without_body_framing(),
        "the re-send repeats every other header"
    );
    assert!(client.request_gzip.is_latched());

    // The latch outlives the step: the next one goes out uncompressed straight away.
    let events = run_step(&provider, &opts).await;
    assert_eq!(text_deltas(&events), "hello world");
    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[2].header("content-encoding"), None);
    assert_eq!(requests[2].body, canonical);
}

#[tokio::test]
async fn an_anthropic_400_with_a_request_id_is_not_resent() {
    let server = Server::start(vec![Reply {
        status: 400,
        headers: vec![
            ("content-type", "application/json"),
            ("request-id", "req_011Cg89yGVn6hhW1MPGnxKf3"),
        ],
        body: br#"{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens: must be positive"}}"#.to_vec(),
    }])
    .await;
    let (provider, client) = anthropic_provider(&server, true);

    let events = run_step(&provider, &options(20_000)).await;

    let requests = server.requests();
    assert_eq!(requests.len(), 1, "an ordinary API error is never re-sent");
    assert_eq!(requests[0].header("content-encoding"), Some("gzip"));
    let error = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => Some(error),
            _ => None,
        })
        .expect("the 400 ends the step");
    assert_eq!(error.status, Some(400));
    assert!(
        error.message.contains("max_tokens: must be positive"),
        "the API's own error passes through: {}",
        error.message
    );
    let records = attempts(&events);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].http_status, Some(400));
    assert!(!client.request_gzip.is_latched());
}

#[tokio::test]
async fn a_compressed_sse_response_streams_through_the_normalizer() {
    let records = sse_records();
    let (head, rest) = records.split_at(3);
    let (head, rest) = (head.concat(), rest.concat());
    let decoded = format!("{head}{rest}");
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let accept_encoding = Arc::new(Mutex::new(None));
    let seen_accept_encoding = accept_encoding.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        *seen_accept_encoding.lock().unwrap() =
            request.header("accept-encoding").map(str::to_owned);
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Encoding: gzip\r\n\
                  Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        // The first records, sync-flushed so they decode on their own, as Anthropic's
        // compressed stream delivers each event.
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(6));
        encoder.write_all(head.as_bytes()).unwrap();
        encoder.flush().unwrap();
        let first = std::mem::take(encoder.get_mut());
        socket
            .write_all(format!("{:x}\r\n", first.len()).as_bytes())
            .await
            .unwrap();
        socket.write_all(&first).await.unwrap();
        socket.write_all(b"\r\n").await.unwrap();
        socket.flush().await.unwrap();
        // The rest only after the client has shown the first text, which proves it decodes
        // as the stream arrives instead of waiting for the end.
        release_rx.await.unwrap();
        encoder.write_all(rest.as_bytes()).unwrap();
        let tail = encoder.finish().unwrap();
        socket
            .write_all(format!("{:x}\r\n", tail.len()).as_bytes())
            .await
            .unwrap();
        socket.write_all(&tail).await.unwrap();
        socket.write_all(b"\r\n0\r\n\r\n").await.unwrap();
        socket.flush().await.unwrap();
    });
    let client = ProviderHttpClient::resolving_anthropic_api_to(address, true);
    let provider = provider_for(
        format!(
            "http://{}:{}",
            request_gzip::ANTHROPIC_API_HOST,
            address.port()
        ),
        &client,
    );
    let mut rx = provider.start_step(
        StepId(1),
        Arc::new(options(100)),
        CancellationToken::new(),
        None,
    );

    let mut events = Vec::new();
    let mut release = Some(release_tx);
    loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(20), rx.recv())
            .await
            .expect("the first text must arrive before the rest of the stream is sent");
        let Some(event) = event else { break };
        if matches!(
            &event,
            NormalizedEvent::Display(DisplayEvent::TextDelta { .. })
        ) {
            if let Some(release) = release.take() {
                release.send(()).unwrap();
            }
        }
        events.push(event);
    }
    server.await.unwrap();

    assert_eq!(
        accept_encoding.lock().unwrap().as_deref(),
        Some(request_gzip::CLAUDE_CODE_ACCEPT_ENCODING)
    );
    assert_eq!(text_deltas(&events), "hello world");
    assert_eq!(retries_used_at_finish(&events), 0);
    // Response byte accounting covers the decoded stream, as for an uncompressed answer.
    let records = attempts(&events);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].response_body_len, decoded.len() as u64);
    assert_eq!(
        records[0].response_body_sha256,
        sha256_hex(decoded.as_bytes())
    );
}

#[tokio::test]
async fn a_request_to_any_other_host_is_never_gzipped() {
    let server = Server::start(vec![Reply::sse()]).await;
    // A custom base URL, relay or mock: same server, but the URL names it by address.
    let client = ProviderHttpClient::resolving_anthropic_api_to(server.address, true);
    let provider = provider_for(format!("http://{}", server.address), &client);
    let opts = options(20_000);
    let canonical = broca_core::Provider::render_request(&provider, &opts);

    let events = run_step(&provider, &opts).await;

    assert_eq!(text_deltas(&events), "hello world");
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].header("content-encoding"), None);
    assert_eq!(
        requests[0].header("accept-encoding"),
        None,
        "other hosts get no accept-encoding, as before"
    );
    assert_eq!(requests[0].body, canonical);
}

#[tokio::test]
async fn a_short_body_or_the_kill_switch_keeps_the_anthropic_request_uncompressed() {
    let server = Server::start(vec![Reply::sse(), Reply::sse()]).await;
    let (provider, _client) = anthropic_provider(&server, true);
    let short = options(10);
    let short_canonical = broca_core::Provider::render_request(&provider, &short);
    assert!(short_canonical.len() < request_gzip::MIN_BODY_UTF16_UNITS);
    run_step(&provider, &short).await;

    let (switched_off, _client) = anthropic_provider(&server, false);
    let large = options(20_000);
    let large_canonical = broca_core::Provider::render_request(&switched_off, &large);
    run_step(&switched_off, &large).await;

    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    for (request, canonical) in requests.iter().zip([&short_canonical, &large_canonical]) {
        assert_eq!(request.header("content-encoding"), None);
        assert_eq!(&request.body, canonical);
        // The host still asks for a compressed answer: that does not depend on request gzip.
        assert_eq!(
            request.header("accept-encoding"),
            Some(request_gzip::CLAUDE_CODE_ACCEPT_ENCODING)
        );
    }
}

#[tokio::test]
async fn a_refused_compressed_warm_is_resent_once_uncompressed() {
    let usage = br#"{"id":"msg_warm","type":"message","usage":{"input_tokens":1,"cache_read_input_tokens":9000,"output_tokens":1}}"#;
    let server = Server::start(vec![
        Reply {
            status: 400,
            headers: vec![
                ("content-type", "application/json"),
                ("request-id", "req_warm"),
            ],
            body: br#"{"type":"error","error":{"type":"invalid_request_error","message":"The request body is not valid JSON: expected value at line 1 column 1"}}"#.to_vec(),
        },
        Reply {
            status: 200,
            headers: vec![("content-type", "application/json")],
            body: usage.to_vec(),
        },
    ])
    .await;
    let client = ProviderHttpClient::resolving_anthropic_api_to(server.address, true);
    let body = format!(
        r#"{{"max_tokens":1,"messages":[{{"role":"user","content":"{}"}}]}}"#,
        "n".repeat(20_000)
    );
    let request = crate::wire::HttpRequest {
        method: crate::wire::HttpMethod::Post,
        url: format!(
            "http://{}:{}/v1/messages",
            request_gzip::ANTHROPIC_API_HOST,
            server.address.port()
        ),
        headers: vec![("content-type".into(), "application/json".into())],
        body: body.clone().into_bytes(),
    };

    let sent = super::send_warm(
        &client,
        &broca_core::clock::Clock::default(),
        request,
        crate::spec::WireFamilyId::AnthropicMessages,
        None,
    )
    .await
    .expect("the uncompressed re-send is the warm's answer");

    assert_eq!(sent.http_status, 200);
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].header("content-encoding"), Some("gzip"));
    assert_canonical_plus_padding(&gunzip(&requests[0].body), body.as_bytes());
    assert_eq!(requests[1].header("content-encoding"), None);
    assert_eq!(requests[1].body, body.as_bytes());
    assert_eq!(sent.warm_body_sha256, sha256_hex(body.as_bytes()));
    assert!(client.request_gzip.is_latched());
}
