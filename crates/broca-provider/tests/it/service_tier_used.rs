//! The service tier a provider REPORTS having served a step at, per wire family.
//!
//! Every family records the tier exactly as the provider spelled it, from wherever that
//! family reports it, and records nothing when the response reports nothing. It is never
//! taken from the request: providers downgrade a requested tier (OpenAI priority to default
//! on traffic ramp-up, Gemini priority to standard on congestion) and bill the tier they used.
//!
//! Synthetic fixtures preserve the provider field placement and terminal tier transitions
//! in tests/live/captures and tests/fixtures without retaining real response content.

use broca_protocol::{FinishReason, NormalizedEvent, ProviderControl, StepId};
use broca_provider::families::{
    anthropic_frozen_config, gemini_frozen_config, google_codeassist_frozen_config,
    openai_chat_frozen_config, openai_responses_chatgpt_frozen_config,
    openai_responses_frozen_config, AnthropicMessages, Gemini, GoogleCodeAssist, OpenAiChat,
    OpenAiResponses, OpenAiResponsesChatgpt,
};
use broca_provider::wire::{FrozenRenderConfig, ResponseHeaders, WireFamily};

/// What a step's `StepFinished` said about the tier and speed it was served at.
#[derive(Debug, PartialEq, Eq)]
struct Served {
    service_tier_used: Option<String>,
    speed_used: Option<String>,
}

/// Feed `headers` (as the transport does, before the body) and then `body` through a fresh
/// normalizer of `family`, and return what the single `StepFinished` reported.
fn served<F: WireFamily>(
    family: &F,
    frozen: &FrozenRenderConfig,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Served {
    let mut normalizer = family.normalizer(frozen, StepId(1));
    normalizer.observe_response_headers(&ResponseHeaders::new(headers.iter().copied()));
    let mut events = Vec::new();
    // Small chunks, so a value split across network reads is still read whole.
    for chunk in body.chunks(61) {
        events.extend(normalizer.push(chunk).expect("stream normalizes"));
    }
    events.extend(normalizer.finish().expect("stream ends cleanly"));
    let finished = events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished {
                service_tier_used,
                speed_used,
                finish_reason,
                ..
            }) => Some((
                service_tier_used.clone(),
                speed_used.clone(),
                *finish_reason,
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(finished.len(), 1, "exactly one StepFinished: {events:?}");
    let (service_tier_used, speed_used, finish_reason) = finished.into_iter().next().unwrap();
    assert_ne!(
        finish_reason,
        FinishReason::Other,
        "the stream completed normally, so the fixture really reached its terminal"
    );
    Served {
        service_tier_used,
        speed_used,
    }
}

fn tier(value: &str) -> Served {
    Served {
        service_tier_used: Some(value.into()),
        speed_used: None,
    }
}

fn nothing() -> Served {
    Served {
        service_tier_used: None,
        speed_used: None,
    }
}

/// The synthetic response body stored in the live-harness artifact format.
fn capture(relative: &str) -> String {
    let path = format!(
        "{}/tests/live/captures/{relative}",
        env!("CARGO_MANIFEST_DIR")
    );
    let capture: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("read capture"))
            .expect("capture is JSON");
    let text = capture["raw_response_text"]
        .as_str()
        .expect("capture has a UTF-8 body")
        .to_owned();
    assert!(!text.is_empty(), "{relative} carries a recorded body");
    text
}

fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!(
        "{}/tests/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("read fixture")
}

/// Drop one exact substring, asserting it was there, so a fixture variant differs from the
/// recording by that field alone.
fn without(body: &str, field: &str) -> String {
    assert!(body.contains(field), "the recording carries {field}");
    body.replacen(field, "", 1)
}

// ───────────────────────── Anthropic Messages: `usage.service_tier` ─────────────────────────

fn anthropic_stream(start_usage: &str, delta_usage: &str) -> String {
    let events = [
        (
            "message_start",
            format!(
                r#"{{"type":"message_start","message":{{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"m","usage":{start_usage}}}}}"#
            ),
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#
                .into(),
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}"#
                .into(),
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#.into(),
        ),
        (
            "message_delta",
            format!(
                r#"{{"type":"message_delta","delta":{{"stop_reason":"end_turn"}},"usage":{delta_usage}}}"#
            ),
        ),
        ("message_stop", r#"{"type":"message_stop"}"#.into()),
    ];
    events
        .iter()
        .map(|(event, data)| format!("event: {event}\ndata: {data}\n\n"))
        .collect()
}

#[test]
fn anthropic_records_usage_service_tier_from_a_recorded_stream() {
    // A synthetic claude-opus-4-8-shaped stream: `"service_tier":"standard"` inside message_start's usage.
    let body = capture("anthropic/signed_thinking.json");
    assert_eq!(
        served(
            &AnthropicMessages,
            &anthropic_frozen_config("m"),
            &[],
            body.as_bytes()
        ),
        tier("standard")
    );
}

#[test]
fn anthropic_records_speed_separately_from_the_tier() {
    // Fast mode is reported as `usage.speed`, not as a tier; the terminal usage carries it.
    let body = anthropic_stream(
        r#"{"input_tokens":3,"output_tokens":1,"service_tier":"standard"}"#,
        r#"{"output_tokens":2,"speed":"fast"}"#,
    );
    assert_eq!(
        served(
            &AnthropicMessages,
            &anthropic_frozen_config("m"),
            &[],
            body.as_bytes()
        ),
        Served {
            service_tier_used: Some("standard".into()),
            speed_used: Some("fast".into()),
        }
    );
}

#[test]
fn anthropic_without_a_reported_tier_records_none() {
    let body = anthropic_stream(
        r#"{"input_tokens":3,"output_tokens":1}"#,
        r#"{"output_tokens":2}"#,
    );
    assert_eq!(
        served(
            &AnthropicMessages,
            &anthropic_frozen_config("m"),
            &[],
            body.as_bytes()
        ),
        nothing()
    );
}

// ─────────────── OpenAI Responses (platform and ChatGPT codex): `response.service_tier` ───────────────

#[test]
fn responses_records_the_terminal_tier_not_the_created_echo() {
    // A synthetic gpt-4o-mini-shaped stream: response.created and response.in_progress say "auto" (the
    // request's value, echoed before the tier is decided); response.completed says "default".
    let body = capture("openai_responses/text_completes.json");
    assert!(body.contains(r#""service_tier":"auto""#));
    assert_eq!(
        served(
            &OpenAiResponses,
            &openai_responses_frozen_config("gpt-4o-mini"),
            &[],
            body.as_bytes()
        ),
        tier("default")
    );
}

#[test]
fn chatgpt_codex_records_the_terminal_tier_from_a_recorded_stream() {
    // A synthetic ChatGPT codex backend turn: created/in_progress echo "auto", completed reports
    // "default".
    let body = fixture("codex_reasoning_turn_response.sse");
    assert_eq!(
        served(
            &OpenAiResponsesChatgpt,
            &openai_responses_chatgpt_frozen_config("gpt-5.5"),
            &[],
            body.as_bytes()
        ),
        tier("default")
    );
}

#[test]
fn responses_without_a_reported_tier_records_none() {
    let body = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"hi\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
    );
    assert_eq!(
        served(
            &OpenAiResponses,
            &openai_responses_frozen_config("m"),
            &[],
            body.as_bytes()
        ),
        nothing()
    );
}

// ─────────────────── OpenAI Chat (and xAI): top-level `service_tier` on the chunks ───────────────────

fn chat_stream(service_tier: &str) -> String {
    [
        format!(
            r#"{{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-4o-mini","service_tier":{service_tier},"choices":[{{"index":0,"delta":{{"role":"assistant","content":"hi"}},"finish_reason":null}}]}}"#
        ),
        format!(
            r#"{{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-4o-mini","service_tier":{service_tier},"choices":[{{"index":0,"delta":{{}},"finish_reason":"stop"}}]}}"#
        ),
        format!(
            r#"{{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-4o-mini","service_tier":{service_tier},"choices":[],"usage":{{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}}}"#
        ),
        "[DONE]".to_string(),
    ]
    .iter()
    .map(|data| format!("data: {data}\n\n"))
    .collect()
}

#[test]
fn chat_records_the_chunk_service_tier() {
    // No recorded OpenAI or xAI chat stream is in the repository (the chat captures are
    // DeepSeek, which reports no tier), so this is the documented chunk shape.
    assert_eq!(
        served(
            &OpenAiChat,
            &openai_chat_frozen_config("gpt-4o-mini"),
            &[],
            chat_stream(r#""default""#).as_bytes()
        ),
        tier("default")
    );
}

#[test]
fn chat_without_a_reported_tier_records_none() {
    // A live DeepSeek stream over the chat grammar: no `service_tier` key anywhere.
    let recorded = capture("openai_chat/text_completes.json");
    assert!(!recorded.contains("service_tier"));
    assert_eq!(
        served(
            &OpenAiChat,
            &openai_chat_frozen_config("deepseek-v4-flash"),
            &[],
            recorded.as_bytes()
        ),
        nothing()
    );
    // A server that sends the key as null reported nothing either.
    assert_eq!(
        served(
            &OpenAiChat,
            &openai_chat_frozen_config("gpt-4o-mini"),
            &[],
            chat_stream("null").as_bytes()
        ),
        nothing()
    );
}

// ─────────── Gemini API: the `x-gemini-service-tier` header, and `usageMetadata.serviceTier` ───────────

/// The synthetic Gemini stream's body tier field, which the header tests remove so
/// only the header can supply a tier.
const GEMINI_BODY_TIER: &str = r#","serviceTier":"standard""#;

#[test]
fn gemini_records_the_response_header_tier() {
    let body = without(&capture("gemini/text_completes.json"), GEMINI_BODY_TIER);
    let frozen = gemini_frozen_config("gemini-2.5-flash");
    assert_eq!(
        served(
            &Gemini,
            &frozen,
            &[("X-Gemini-Service-Tier", "standard")],
            body.as_bytes()
        ),
        tier("standard")
    );
    assert_eq!(served(&Gemini, &frozen, &[], body.as_bytes()), nothing());
}

#[test]
fn gemini_records_the_body_tier_from_a_recorded_stream() {
    // The synthetic stream carries `usageMetadata.serviceTier: "standard"`.
    let body = capture("gemini/text_completes.json");
    assert_eq!(
        served(
            &Gemini,
            &gemini_frozen_config("gemini-2.5-flash"),
            &[],
            body.as_bytes()
        ),
        tier("standard")
    );
}

#[test]
fn gemini_header_wins_over_the_body_when_they_differ() {
    // The header is where Google documents the tier used; the body field is undocumented.
    let body = capture("gemini/text_completes.json");
    assert_eq!(
        served(
            &Gemini,
            &gemini_frozen_config("gemini-2.5-flash"),
            &[("x-gemini-service-tier", "priority")],
            body.as_bytes()
        ),
        tier("priority")
    );
}

#[test]
fn code_assist_reads_the_same_header_and_records_none_without_it() {
    // No Code Assist response has been recorded. Its decode is Gemini's behind a `{response}`
    // unwrap that also accepts a bare response, so the recorded Gemini body stands in for it.
    let body = without(&capture("gemini/text_completes.json"), GEMINI_BODY_TIER);
    let frozen = google_codeassist_frozen_config("gemini-2.5-flash");
    assert_eq!(
        served(
            &GoogleCodeAssist,
            &frozen,
            &[("x-gemini-service-tier", "standard")],
            body.as_bytes()
        ),
        tier("standard")
    );
    assert_eq!(
        served(&GoogleCodeAssist, &frozen, &[], body.as_bytes()),
        nothing()
    );
}

// ───────────── End to end: HTTP response → transport → normalizer → run loop → WAL ─────────────

// A file-level `cfg` would delete this module when the feature is off and report "0 tests ok";
// gating a module keeps a visible notice instead.
#[cfg(not(feature = "live"))]
#[test]
fn the_end_to_end_cases_need_the_live_feature() {
    eprintln!(
        "SKIP service_tier_used end-to-end cases: compiled without the live feature. \
         Re-run with `-F broca-provider/live` to execute them."
    );
}

#[cfg(feature = "live")]
mod end_to_end {
    use super::*;
    use broca_core::{
        run_loop, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunRequest, StubToolPlane,
        WalStore,
    };
    use broca_protocol::{
        content::{ContentBlock, Message, Role},
        request::{ModelHandle, StopCondition, ToolChoice},
        BindIdentity, GenerationConfig, RunId,
    };
    use broca_provider::{wire::AuthHandle, LiveProvider};
    use broca_wal::WalRecord;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::mpsc,
    };

    /// Serves ONE 200 response with `headers` and `body`, and keeps the request body it got.
    async fn serve_once(
        headers: Vec<(&'static str, &'static str)>,
        body: Vec<u8>,
    ) -> (String, Arc<Mutex<Vec<u8>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let request = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&request);
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            *seen.lock().unwrap() = read_request_body(&mut socket).await;
            let mut head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: text/event-stream\r\n",
                body.len()
            );
            for (name, value) in headers {
                head.push_str(&format!("{name}: {value}\r\n"));
            }
            head.push_str("\r\n");
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
            socket.flush().await.unwrap();
        });
        (url, request)
    }

    async fn read_request_body(socket: &mut TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut chunk = [0u8; 8192];
        let header_end = loop {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client disconnected before sending the request");
            request.extend_from_slice(&chunk[..read]);
            if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
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
            assert_ne!(read, 0, "client disconnected mid-body");
            request.extend_from_slice(&chunk[..read]);
        }
        request[header_end..header_end + content_length].to_vec()
    }

    fn scratch(tag: &str) -> PathBuf {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-service-tier-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        dir
    }

    /// Run one real step of `family` against `url` and return the WAL's model step records.
    async fn run_one_step<F: WireFamily + 'static>(
        tag: &str,
        family: F,
        mut frozen: FrozenRenderConfig,
        url: String,
    ) -> Vec<WalRecord> {
        let dir = scratch(tag);
        frozen.base_url = Some(url);
        let provider = LiveProvider::new(family, frozen, AuthHandle::Bearer("test".into()));
        // This fixture serves one SSE body over HTTP, so pin HTTPS rather than opening a socket to the mock.
        broca_core::Provider::set_chatgpt_https_policy(&provider, Arc::new(|| true));
        let session = BindIdentity::new(dir.join("proj"), "service-tier", tag);
        let request = RunRequest {
            retention: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId(format!("tier-{tag}")),
            session: session.clone(),
            model: ModelHandle::new("p", "m"),
            input: vec![Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::User,
                content: vec![ContentBlock::Text { text: "hi".into() }],
            }],
            tools: vec![],
            policy_allowed: Default::default(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            stop_when: vec![StopCondition::StepCount { n: 1 }],
            submission_id: None,
            budget: None,
            context_limit: None,
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen_cache: false,
        };
        let store = FileLeaseStore::new(dir.join("leases"));
        let wal = FileWalStore::new(dir.join("wal"));
        let (tx, mut rx) = mpsc::channel::<OutputEvent>(4096);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        run_loop(
            request,
            &provider,
            &StubToolPlane::new(),
            &store,
            &AllowAll,
            &wal,
            &tx,
        )
        .await
        .expect("run completes at the lease/WAL level");
        drop(tx);
        drain.await.unwrap();
        let steps = WalStore::read_records(&wal, &session)
            .unwrap()
            .into_iter()
            .map(|framed| framed.record)
            .filter(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
            .collect::<Vec<_>>();
        assert_eq!(steps.len(), 1, "one model step was recorded");
        steps
    }

    fn recorded_tier(step: &WalRecord) -> Option<&str> {
        match step {
            WalRecord::ModelStepFinished {
                service_tier_used, ..
            } => service_tier_used.as_deref(),
            _ => unreachable!(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gemini_header_tier_reaches_the_wal() {
        let body = without(&capture("gemini/text_completes.json"), GEMINI_BODY_TIER);
        let (url, _) = serve_once(
            vec![("x-gemini-service-tier", "standard")],
            body.into_bytes(),
        )
        .await;
        let steps = run_one_step(
            "gemini-header",
            Gemini,
            gemini_frozen_config("gemini-2.5-flash"),
            url,
        )
        .await;
        assert_eq!(recorded_tier(&steps[0]), Some("standard"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gemini_without_the_header_records_no_tier_key() {
        let body = without(&capture("gemini/text_completes.json"), GEMINI_BODY_TIER);
        let (url, _) = serve_once(vec![], body.into_bytes()).await;
        let steps = run_one_step(
            "gemini-no-header",
            Gemini,
            gemini_frozen_config("gemini-2.5-flash"),
            url,
        )
        .await;
        assert_eq!(recorded_tier(&steps[0]), None);
        let bytes = serde_json::to_string(&steps[0]).unwrap();
        assert!(
            !bytes.contains("service_tier_used") && !bytes.contains("speed_used"),
            "an unreported tier writes no bytes: {bytes}"
        );
    }

    /// A ChatGPT run frozen with the priority tier renders `service_tier: "priority"` into the
    /// request. The recorded codex response reports `default` (the downgrade), and `default` is
    /// what the step records: the requested tier never reaches the record.
    #[tokio::test(flavor = "multi_thread")]
    async fn priority_chatgpt_run_records_the_tier_the_response_reported() {
        let (url, request) = serve_once(
            vec![],
            fixture("codex_reasoning_turn_response.sse").into_bytes(),
        )
        .await;
        let mut frozen = openai_responses_chatgpt_frozen_config("gpt-5.5");
        frozen.service_tier = Some("priority".into());
        let steps = run_one_step("chatgpt-priority", OpenAiResponsesChatgpt, frozen, url).await;
        let sent: serde_json::Value =
            serde_json::from_slice(&request.lock().unwrap()).expect("request body is JSON");
        assert_eq!(
            sent["service_tier"],
            serde_json::json!("priority"),
            "the run requested priority"
        );
        assert_eq!(recorded_tier(&steps[0]), Some("default"));
    }
}
