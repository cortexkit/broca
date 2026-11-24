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
        bedrock_frozen_config, openai_chat_frozen_config, openai_responses_chatgpt_frozen_config,
        openai_responses_frozen_config, AnthropicMessages, BedrockConverse, OpenAiChat,
        OpenAiResponses, OpenAiResponsesChatgpt,
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
    let mut provider = LiveProvider::new(family, frozen, AuthHandle::None);
    provider.retry_clock = super::RetryClock {
        virtual_waits: Some(Arc::new(Mutex::new(Vec::new()))),
        ..super::RetryClock::default()
    };
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

/// The control case: a parse failure classified as transient, with one decode retry.
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
