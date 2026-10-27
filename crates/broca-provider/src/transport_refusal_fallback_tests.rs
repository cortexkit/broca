//! Routing a step to the episode's client-side refusal-fallback model: what the provider renders
//! and sends on each route, what it records, and what it leaves alone.

use super::*;
use crate::families::{anthropic_frozen_config, AnthropicMessages};
use crate::wire::{ClaudeCodePersona, RefusalFallbackConfig};
use broca_core::refusal_fallback::StepRoute;
use broca_protocol::request::ModelHandle;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SOURCE: &str = "claude-opus-5-5";
const FALLBACK: &str = "claude-opus-4-8";

fn persona() -> ClaudeCodePersona {
    ClaudeCodePersona {
        version: ClaudeCodePersona::CURRENT_VERSION.to_string(),
        device_id: "d".repeat(64),
        account_uuid: Some("account".into()),
        session_id: "00000000-0000-4000-8000-000000000001".into(),
        prompt_id: Some("00000000-0000-4000-8000-000000000002".into()),
        cc_version_suffix: None,
        prompt_ordinal: Some(1),
    }
}

fn anthropic_oauth(
    model: &str,
    max_output_tokens: u32,
    base_url: Option<&str>,
) -> FrozenRenderConfig {
    let mut frozen = anthropic_frozen_config(model);
    frozen.max_output_tokens = max_output_tokens;
    frozen.claude_code_persona = Some(persona());
    frozen.base_url = base_url.map(str::to_owned);
    frozen
}

/// A source config for `SOURCE` that froze `FALLBACK` as its refusal fallback.
fn source_with_fallback(base_url: Option<&str>) -> (FrozenRenderConfig, FrozenRenderConfig) {
    let fallback = anthropic_oauth(FALLBACK, 1_000, base_url);
    let mut source = anthropic_oauth(SOURCE, 2_000, base_url);
    source.refusal_fallback = Some(Box::new(RefusalFallbackConfig {
        model: ModelHandle::new("anthropic", FALLBACK),
        context_limit: Some(200_000),
        frozen: fallback.clone(),
    }));
    (source, fallback)
}

fn options() -> CallOptions {
    CallOptions::new(vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "hello".into(),
        }],
    )])
}

fn body(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("rendered body is JSON")
}

#[test]
fn the_fallback_route_renders_the_fallback_models_request_and_nothing_else_moves() {
    let (source, fallback) = source_with_fallback(None);
    let provider = LiveProvider::new(AnthropicMessages, source.clone(), AuthHandle::None);
    let opts = options();
    let blob = provider.render_config_blob();
    let version = provider.renderer_version();
    let source_bytes = provider.render_request(&opts);
    assert_eq!(body(&source_bytes)["model"], SOURCE);
    assert_eq!(
        provider.refusal_fallback_model(),
        Some(ModelHandle::new("anthropic", FALLBACK))
    );
    assert_eq!(provider.refusal_fallback_context_limit(), Some(200_000));

    provider.set_step_route(StepRoute::Fallback);
    let fallback_bytes = provider.render_request(&opts);
    assert_eq!(fallback_bytes, AnthropicMessages.render(&fallback, &opts));
    let rendered = body(&fallback_bytes);
    assert_eq!(rendered["model"], FALLBACK);
    assert_eq!(
        rendered["max_tokens"], 1_000,
        "the fallback's own output cap"
    );
    // The recorded config, its version and the resume checks describe the source episode on
    // either route.
    assert_eq!(provider.render_config_blob(), blob);
    assert_eq!(provider.renderer_version(), version);
    assert!(provider.reproduces_renderer_version(version));
    assert_eq!(
        blob["refusal_fallback"]["frozen"]["wire_model_id"],
        FALLBACK
    );

    provider.set_step_route(StepRoute::Source);
    assert_eq!(provider.render_request(&opts), source_bytes);
}

#[test]
fn output_limits_follow_the_model_selected_for_the_step() {
    let (source, fallback) = source_with_fallback(None);
    let provider = LiveProvider::new(AnthropicMessages, source.clone(), AuthHandle::None);
    assert_eq!(provider.output_limits().unwrap().max, 2_000);
    provider.set_step_route(StepRoute::Fallback);
    assert_eq!(provider.output_limits(), fallback.output_limits());
    assert_eq!(provider.output_limits().unwrap().max, 1_000);
    assert_eq!(provider.output_limits().unwrap().floor, 1_000);
    provider.set_step_route(StepRoute::Source);
    assert_eq!(provider.output_limits(), source.output_limits());
}

#[test]
fn a_provider_without_a_fallback_ignores_the_route() {
    let source = anthropic_oauth(SOURCE, 2_000, None);
    let provider = LiveProvider::new(AnthropicMessages, source.clone(), AuthHandle::None);
    let opts = options();
    let blob = provider.render_config_blob();
    let bytes = provider.render_request(&opts);
    assert_eq!(provider.refusal_fallback_model(), None);
    assert!(blob.get("refusal_fallback").is_none());
    provider.set_step_route(StepRoute::Fallback);
    assert_eq!(provider.render_request(&opts), bytes);
    assert_eq!(provider.render_config_blob(), blob);
}

const ANSWER: &[u8] = b"event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n\
event: content_block_start\n\
data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n\
event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n\
event: content_block_stop\n\
data: {\"type\":\"content_block_stop\",\"index\":0}\n\n\
event: message_delta\n\
data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n\
event: message_stop\n\
data: {\"type\":\"message_stop\"}\n\n";

/// Serve one request with [`ANSWER`], returning its header block and body.
async fn serve_one(listener: tokio::net::TcpListener) -> (String, Vec<u8>) {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "client disconnected before the request headers");
        request.extend_from_slice(&chunk[..read]);
        if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let head = String::from_utf8_lossy(&request[..header_end]).into_owned();
    let length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    while request.len() < header_end + length {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "client disconnected before the request body");
        request.extend_from_slice(&chunk[..read]);
    }
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        ANSWER.len()
    );
    socket.write_all(response.as_bytes()).await.unwrap();
    socket.write_all(ANSWER).await.unwrap();
    socket.flush().await.unwrap();
    (head, request[header_end..header_end + length].to_vec())
}

fn header(head: &str, name: &str) -> Option<String> {
    head.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim()
            .eq_ignore_ascii_case(name)
            .then(|| value.trim().to_string())
    })
}

#[tokio::test]
async fn a_fallback_step_sends_the_fallback_request_and_continues_the_attempt_ordinals() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(serve_one(listener));
    let (source, fallback) = source_with_fallback(Some(&base_url));
    let auth = AuthHandle::Bearer("token".into());
    let provider = LiveProvider::new(AnthropicMessages, source.clone(), auth.clone());
    provider.set_step_route(StepRoute::Fallback);
    let opts = Arc::new(options());
    let expected_body = provider.render_request(&opts);

    let mut rx = provider
        .step_with_runtime(
            StepId(4),
            Arc::clone(&opts),
            broca_core::provider::StepRuntime {
                cancel: CancellationToken::new(),
                deadline: None,
                seal: None,
                progress: None,
                // Three attempts of this step were recorded before the re-send.
                first_attempt: 3,
            },
        )
        .await;
    let mut attempts = Vec::new();
    let mut finished = false;
    while let Some(event) = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
        .await
        .expect("the step ends")
    {
        match event {
            NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                attempts.push(attempt)
            }
            NormalizedEvent::Control(ProviderControl::Finished { .. }) => finished = true,
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                panic!("the fallback step failed: {error:?}")
            }
            _ => {}
        }
    }
    assert!(finished);
    let (head, sent) = server.await.unwrap();
    assert_eq!(
        sent, expected_body,
        "the bytes sent are the fallback render"
    );
    assert_eq!(body(&sent)["model"], FALLBACK);
    // The betas are the ones the fallback model's own request carries, not the source's.
    let fallback_betas = header(&head, "anthropic-beta").expect("an OAuth request names its betas");
    let expected_headers = AnthropicMessages.endpoint(&fallback, &auth, expected_body.clone());
    let expected_betas = expected_headers
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("anthropic-beta"))
        .map(|(_, value)| value.clone())
        .expect("the fallback endpoint names its betas");
    assert_eq!(fallback_betas, expected_betas);
    let source_request =
        AnthropicMessages.endpoint(&source, &auth, AnthropicMessages.render(&source, &opts));
    let source_betas = source_request
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("anthropic-beta"))
        .map(|(_, value)| value.clone())
        .unwrap();
    assert_ne!(
        fallback_betas, source_betas,
        "the two models' beta sets differ, so this checks which one was sent"
    );
    // One attempt, numbered after the three the step already recorded.
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt, 3);
    assert_eq!(attempts[0].outcome, AttemptOutcome::Complete);
}

/// `options()` carrying a step whose output cap the run recorded lowered to `cap`.
fn options_with_recorded_cap(cap: u32) -> CallOptions {
    let mut opts = options();
    opts.step_context = Some(broca_protocol::request::StepContext {
        run_id: "run".into(),
        step_index: 1,
        session_address: None,
        credential_id: None,
        account_ref: None,
        max_output_tokens: Some(cap),
    });
    opts
}

#[test]
fn a_fallback_step_renders_the_fallback_model_with_its_recorded_output_cap() {
    let (source, fallback) = source_with_fallback(None);
    let provider = LiveProvider::new(AnthropicMessages, source.clone(), AuthHandle::None);
    // Below both models' frozen caps (source 2000, fallback 1000).
    let opts = options_with_recorded_cap(600);

    // The source route renders exactly what a provider without a fallback renders: the source
    // config with the recorded cap applied.
    let source_bytes = provider.render_request(&opts);
    let without_fallback = LiveProvider::new(
        AnthropicMessages,
        anthropic_oauth(SOURCE, 2_000, None),
        AuthHandle::None,
    );
    assert_eq!(source_bytes, without_fallback.render_request(&opts));
    assert_eq!(
        source_bytes,
        AnthropicMessages.render(&source.for_step(&opts), &opts)
    );
    assert_eq!(body(&source_bytes)["max_tokens"], 600);

    provider.set_step_route(StepRoute::Fallback);
    let fallback_bytes = provider.render_request(&opts);
    assert_eq!(
        fallback_bytes,
        AnthropicMessages.render(&fallback.for_step(&opts), &opts)
    );
    let rendered = body(&fallback_bytes);
    assert_eq!(rendered["model"], FALLBACK);
    assert_eq!(rendered["max_tokens"], 600, "the step's recorded cap");

    // A recorded cap above the fallback's own frozen cap leaves the fallback's cap in place.
    let above = options_with_recorded_cap(1_500);
    assert_eq!(body(&provider.render_request(&above))["max_tokens"], 1_000);

    provider.set_step_route(StepRoute::Source);
    assert_eq!(provider.render_request(&opts), source_bytes);
}

#[tokio::test]
async fn a_fallback_step_sends_the_fallback_model_with_its_recorded_output_cap() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(serve_one(listener));
    let (source, _) = source_with_fallback(Some(&base_url));
    let provider = LiveProvider::new(
        AnthropicMessages,
        source,
        AuthHandle::Bearer("token".into()),
    );
    provider.set_step_route(StepRoute::Fallback);
    let opts = Arc::new(options_with_recorded_cap(600));
    let expected_body = provider.render_request(&opts);

    let mut rx = provider
        .step_with_runtime(
            StepId(2),
            Arc::clone(&opts),
            broca_core::provider::StepRuntime {
                cancel: CancellationToken::new(),
                deadline: None,
                seal: None,
                progress: None,
                first_attempt: 0,
            },
        )
        .await;
    let mut finished = false;
    while let Some(event) = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
        .await
        .expect("the step ends")
    {
        match event {
            NormalizedEvent::Control(ProviderControl::Finished { .. }) => finished = true,
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                panic!("the fallback step failed: {error:?}")
            }
            _ => {}
        }
    }
    assert!(finished);
    let (_, sent) = server.await.unwrap();
    assert_eq!(
        sent, expected_body,
        "the bytes sent are the fallback render"
    );
    let sent = body(&sent);
    assert_eq!(sent["model"], FALLBACK);
    assert_eq!(sent["max_tokens"], 600, "the step's recorded cap");
}
