use broca_records::record::{AttemptEnvelope, AttemptId, BillingAttemptOutcome};
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
    AttemptOutcome, CallOptions, ErrorClass, FinishReason, Message, MessageId, ModelAttempt,
    ModelHandle, NormalizedEvent, ProviderControl, ProviderError, StepContext, StepId, Tool,
    ToolCallId, ToolChoice, Usage,
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

include!("../transport_quota_tests.rs");

use super::{
    build_provider_http_client, classify_account_limit_exhausted, classify_http_error,
    collect_error_body, drive_attempt, drive_body_stream, drive_body_stream_with_limit,
    drive_stream, drive_stream_with_header_timeout, dump_files_to_delete, extract_provider_code,
    http_error, parse_retry_after_header, parse_retry_after_value, publish_completion,
    request_dump_filename, retry_delay, send_attempt, transport_error, write_dump_file,
    AttemptTrace, DumpFileMeta, DumpRetentionCaps, LiveProvider, PreparedHttpRequest,
    ProviderHttpClient, RequestDigest, GOOGLE_RPC_DIMENSIONS_MAX_KEYS, GOOGLE_RPC_ERROR_INFO_TYPE,
    GOOGLE_RPC_METADATA_MAX_KEYS, GOOGLE_RPC_QUOTA_FAILURE_TYPE, GOOGLE_RPC_RETRY_INFO_TYPE,
    GOOGLE_RPC_VIOLATIONS_MAX, MAX_ERROR_BODY_BYTES, MAX_HTTP_ERROR_BODY_READ_BYTES,
    STREAM_CONTENT_TIMEOUT, STREAM_IDLE_TIMEOUT,
};
use broca_core::Clock;
// Directory mode is only asserted on Unix, so keep that helper import under the same cfg.
#[cfg(unix)]
use super::create_dump_dir;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;

mod response_evidence_tests {
    include!("../transport_response_evidence_tests.rs");
}

mod context_overflow {
    include!("../transport_context_overflow_tests.rs");
}

mod gemini_errors {
    include!("../transport_gemini_error_tests.rs");
}

mod request_invalid {
    include!("../transport_request_invalid_tests.rs");
}

mod credential_hosts {
    include!("../transport_credential_hosts_tests.rs");
}

/// A strict schema the provider can't compile is told apart from other refusals before the
/// error body is cut to its stored size, so a long body still names the cause.
#[test]
fn compiler_refusal_is_classified_before_error_body_truncation() {
    let body = serde_json::json!({"error":{"type":"invalid_request_error","message":"Schema is too complex for compilation"},"padding":"x".repeat(MAX_ERROR_BODY_BYTES + 100)}).to_string();
    let error = super::http_error_in_family(
        400,
        &body,
        None,
        Some(WireFamilyId::AnthropicMessages),
        None,
    );
    assert!(crate::structured_output::grammar_rejection(&error));
}

mod attempt_equivalence {
    include!("../transport/attempt_equivalence_http.rs");
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
        credential_hosts: Default::default(),
        quota_observer: None,
        cache_session: None,
        cache_provider: None,
        subscription_auth: false,
        media_renderer: None,
        media_render_tags: false,
        auth_revision: 0,
        family: Arc::new(family),
        frozen: Arc::new(frozen),
        auth: Arc::new(AuthHandle::None),
        opts: Arc::new(opts),
        step_id: StepId(7),
    };
    let offloaded = tokio::task::spawn_blocking(move || job.render())
        .await
        .expect("render job joins")
        .expect("render job succeeds");
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
                    input: serde_json::json!({"nested": {"retained": true}}).into(),
                    thought_signature: Some("call-signature".into()),
                    provider_state: None,
                }),
            ],
            ModelHandle::new("other-provider", "other-model"),
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                // A large request catches ownership paths that accidentally special-case
                // small golden fixtures.
                text: "x".repeat(512 * 1024).into(),
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
        reply: None,
    }]);
    opts.tool_choice = Arc::new(ToolChoice::Auto);
    opts.provider_options = Arc::new(Value::Object(provider_options));
    opts.step_context = Some(StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "render-job-run".into(),
        step_index: 7,
        max_output_tokens: None,
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
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "panic-translation".into(),
        step_index: 1,
        max_output_tokens: None,
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
            RetryAttemptBehavior::AnthropicOverloaded => Box::new(AnthropicOverloadedNormalizer),
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
                raw_finish_reason: None,
                step_id: self.step_id,
                usage: Usage {
                    output_tokens: Some(999),
                    ..Usage::default()
                },
                finish_reason: FinishReason::Stop,
                service_tier_used: None,
                speed_used: None,
                served_model: None,
                usage_iterations: None,
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
                provider_details: None,
                cause: None,
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
                account_continue: None,
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
            mid: None,
            part: Some(0),
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
    order: Arc<Mutex<Vec<&'static str>>>,
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
        let order = Arc::new(Mutex::new(Vec::new()));
        let wire_order = order.clone();
        let task = tokio::spawn(async move {
            let mut script = script.into_iter();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (head, body) = read_http_request(&mut socket).await;
                wire_order.lock().unwrap().push("write");
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
                    } => write_truncated_http_response(&mut socket, status, &headers, body).await,
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
            order,
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

pub(super) struct TestAttemptHook {
    pub(super) order: Arc<Mutex<Vec<&'static str>>>,
    pub(super) sends: Mutex<Vec<(AttemptId, broca_core::run::PhysicalSend)>>,
    pub(super) finished: Mutex<Vec<(AttemptId, ModelAttempt, BillingAttemptOutcome)>>,
    pub(super) responses: Mutex<Vec<Option<AttemptEnvelope>>>,
    pub(super) deny: bool,
}

impl TestAttemptHook {
    pub(super) fn new(order: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            order,
            sends: Default::default(),
            finished: Default::default(),
            responses: Default::default(),
            deny: false,
        }
    }
}

#[async_trait::async_trait]
impl broca_core::run::AttemptHook for TestAttemptHook {
    async fn before_send(
        &self,
        send: broca_core::run::PhysicalSend,
        _cancel: CancellationToken,
    ) -> Result<AttemptId, ProviderError> {
        assert!(
            send.warm.is_none(),
            "model-step sends must not carry warm policy"
        );
        if self.deny {
            let mut error = transport_error("reservation writer refused");
            error.class = ErrorClass::Permanent;
            error.provider_code = Some("attempt_durability_failed".into());
            return Err(error);
        }
        let id = broca_core::attempt_id::mint_attempt_id().unwrap();
        self.sends.lock().unwrap().push((id.clone(), send));
        self.order.lock().unwrap().push("durable");
        Ok(id)
    }
    async fn finished(
        &self,
        id: AttemptId,
        attempt: ModelAttempt,
        outcome: BillingAttemptOutcome,
        response: Option<AttemptEnvelope>,
    ) -> Result<(), ProviderError> {
        self.responses.lock().unwrap().push(response);
        self.finished.lock().unwrap().push((id, attempt, outcome));
        self.order.lock().unwrap().push("outcome");
        Ok(())
    }
}

mod attempt_receipts {
    use super::*;
    include!("../transport_attempt_receipt_tests.rs");
}

#[tokio::test]
async fn connect_failure_has_no_sent_at() {
    // Bind and release a local port to get a real reqwest connect error, rather than
    // exercising only the transport's synthetic fail-connect switch.
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let request = PreparedHttpRequest::from(HttpRequest {
        method: HttpMethod::Post,
        url: format!("http://{address}"),
        headers: vec![],
        body: b"{}".to_vec(),
    });
    let hook = Arc::new(TestAttemptHook::new(Default::default()));
    let mut trace = AttemptTrace {
        attempt_hook: Some(hook.clone()),
        ..Default::default()
    };
    let failure = send_attempt(
        &build_provider_http_client(),
        &request,
        false,
        None,
        Duration::from_secs(5),
        &mut trace,
    )
    .await
    .unwrap_err();
    assert_eq!(failure.outcome, AttemptOutcome::ConnectFailed);
    assert_eq!(trace.sent_at_ms, None);
    trace
        .settle_send(&RequestDigest::of(&request.body), failure.outcome, None)
        .await
        .unwrap();
    assert_eq!(hook.sends.lock().unwrap().len(), 1);
    assert_eq!(hook.finished.lock().unwrap()[0].1.sent_at_ms, None);
}

async fn one_post_redirect(
    status: u16,
    location: String,
) -> (String, tokio::task::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let url = format!("http://{}/original", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (head, body) = read_http_request(&mut socket).await;
        assert!(head.starts_with("POST /original HTTP/1.1"));
        write_http_response(&mut socket, status, &[("location", &location)], b"").await;
        body
    });
    (url, task)
}

/// Every send stops at a redirect, journaled or not: the target never gets a second POST.
#[tokio::test]
async fn redirects_stop_before_second_post() {
    for status in [307, 308] {
        for journaled in [false, true] {
            let target = ScriptedServer::start(vec![ScriptResponse::Http {
                status: 400,
                headers: vec![],
                body: b"second POST",
            }])
            .await;
            let port = reqwest::Url::parse(&target.url).unwrap().port().unwrap();
            let (url, source) =
                one_post_redirect(status, format!("http://localhost:{port}/redirected")).await;
            let request = PreparedHttpRequest::from(HttpRequest {
                method: HttpMethod::Post,
                url,
                headers: vec![],
                body: b"model request".to_vec(),
            });
            let hook = Arc::new(TestAttemptHook::new(Default::default()));
            let mut trace = AttemptTrace {
                attempt_hook: journaled
                    .then(|| hook.clone() as Arc<dyn broca_core::run::AttemptHook>),
                ..Default::default()
            };
            let response = send_attempt(
                &build_provider_http_client(),
                &request,
                false,
                None,
                Duration::from_secs(5),
                &mut trace,
            )
            .await
            .unwrap();
            assert_eq!(response.status().as_u16(), status, "journaled {journaled}");
            assert_eq!(source.await.unwrap(), b"model request");
            assert!(
                target.request_bodies().is_empty(),
                "journaled {journaled}: a redirect was followed"
            );
        }
    }
}

#[tokio::test]
async fn journaled_redirect_to_refused_connection_keeps_sent_evidence() {
    for status in [307, 308] {
        let refused = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = refused.local_addr().unwrap();
        drop(refused);
        let (url, source) = one_post_redirect(
            status,
            format!("http://localhost:{}/redirected", address.port()),
        )
        .await;
        let request = PreparedHttpRequest::from(HttpRequest {
            method: HttpMethod::Post,
            url,
            headers: vec![],
            body: b"model request".to_vec(),
        });
        let hook = Arc::new(TestAttemptHook::new(Default::default()));
        let mut trace = AttemptTrace {
            attempt_hook: Some(hook.clone()),
            ..Default::default()
        };
        let response = send_attempt(
            &build_provider_http_client(),
            &request,
            false,
            None,
            Duration::from_secs(5),
            &mut trace,
        )
        .await;
        assert!(
            response.is_ok(),
            "original POST received a redirect, not a connect refusal: {response:?}"
        );
        assert_eq!(response.unwrap().status().as_u16(), status);
        assert_eq!(source.await.unwrap(), b"model request");
        assert!(trace.sent_at_ms.is_some());
        assert_eq!(hook.sends.lock().unwrap().len(), 1);
    }
}

#[test]
fn legacy_render_keeps_billing_only_metadata_absent() {
    let mut frozen = crate::families::anthropic_frozen_config("m");
    frozen.speed = Some("fast".into());
    let rendered = super::RenderJob {
        credential_hosts: Default::default(),
        quota_observer: None,
        cache_session: None,
        cache_provider: None,
        subscription_auth: false,
        media_renderer: None,
        media_render_tags: false,
        auth_revision: 0,
        family: Arc::new(AnthropicMessages),
        frozen: Arc::new(frozen),
        auth: Arc::new(AuthHandle::None),
        opts: Arc::new(CallOptions::new(Vec::new())),
        step_id: StepId(1),
    }
    .render()
    .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&rendered.request.body).unwrap()["speed"],
        "fast"
    );
    assert!(rendered.request.attempt_hook.is_none());
    assert_eq!(
        rendered.request.speed_requested, None,
        "legacy render must not clone billing-only speed metadata"
    );
}

#[test]
#[ignore = "manual measurement of retained HTTP-wrapper sharing overhead"]
fn legacy_http_wrapper_sharing_component_cost() {
    const ITERATIONS: u128 = 100_000;
    const SAMPLES: usize = 9;
    let client = build_provider_http_client();
    let mut allocation = Vec::with_capacity(SAMPLES);
    let mut cloning = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let start = std::time::Instant::now();
        for _ in 0..ITERATIONS {
            std::hint::black_box(Arc::new(super::JournaledHttpClients {
                clients: std::sync::OnceLock::new(),
                builder: Box::new(super::provider_client_builder),
            }));
        }
        allocation.push(start.elapsed().as_nanos() / ITERATIONS);
        let start = std::time::Instant::now();
        for _ in 0..ITERATIONS {
            std::hint::black_box(Arc::clone(&client.journaled));
        }
        cloning.push(start.elapsed().as_nanos() / ITERATIONS);
    }
    allocation.sort_unstable();
    cloning.sort_unstable();
    assert!(client.journaled.clients.get().is_none());
    // Isolate the retained sharing component, not TLS/client construction or a
    // historical implementation. Each sample includes loop and drop overhead.
    println!("legacy sharing component: os={} debug_assertions={} batches={SAMPLES} iterations_per_batch={ITERATIONS} arc_allocate_drop_median_batch_mean_ns={} arc_clone_drop_median_batch_mean_ns={}",
        std::env::consts::OS, cfg!(debug_assertions), allocation[SAMPLES / 2], cloning[SAMPLES / 2]);
}

#[tokio::test]
async fn journaled_render_attaches_speed_metadata_before_send() {
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 400,
        headers: vec![],
        body: b"invalid",
    }])
    .await;
    let mut frozen = crate::families::anthropic_frozen_config("m");
    frozen.base_url = Some(server.url.clone());
    frozen.speed = Some("fast".into());
    let provider = LiveProvider::new(AnthropicMessages, frozen, AuthHandle::None);
    let hook = Arc::new(TestAttemptHook::new(Default::default()));
    let mut stream = broca_core::Provider::step_with_runtime(
        &provider,
        StepId(1),
        Arc::new(CallOptions::new(Vec::new())),
        broca_core::provider::StepRuntime {
            cancel: CancellationToken::new(),
            deadline: None,
            seal: None,
            progress: None,
            first_attempt: 0,
            attempt_hook: Some(hook.clone()),
        },
    )
    .await;
    while stream.recv().await.is_some() {}
    assert_eq!(
        hook.sends.lock().unwrap()[0].1.speed_requested.as_deref(),
        Some("fast")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&server.request_bodies()[0]).unwrap()["speed"],
        "fast"
    );
}

struct SettlementDelayHook {
    inner: TestAttemptHook,
    waits: Arc<Mutex<Vec<Duration>>>,
}
#[async_trait::async_trait]
impl broca_core::run::AttemptHook for SettlementDelayHook {
    async fn before_send(
        &self,
        send: broca_core::run::PhysicalSend,
        cancel: CancellationToken,
    ) -> Result<AttemptId, ProviderError> {
        broca_core::run::AttemptHook::before_send(&self.inner, send, cancel).await
    }
    async fn finished(
        &self,
        id: AttemptId,
        attempt: ModelAttempt,
        outcome: BillingAttemptOutcome,
        response: Option<AttemptEnvelope>,
    ) -> Result<(), ProviderError> {
        self.waits.lock().unwrap().push(Duration::from_secs(239));
        broca_core::run::AttemptHook::finished(&self.inner, id, attempt, outcome, response).await
    }
}

#[tokio::test]
async fn settlement_wait_is_excluded_from_retry_wall_budget() {
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status: 429,
            headers: vec![("retry-after", "2")],
            body: b"",
        },
        ScriptResponse::Http {
            status: 200,
            headers: vec![],
            body: b"done",
        },
    ])
    .await;
    let mut provider = retry_test_provider(&server, 0);
    let waits = use_virtual_retry_waits(&mut provider);
    let hook = Arc::new(SettlementDelayHook {
        inner: TestAttemptHook::new(Default::default()),
        waits: waits.clone(),
    });
    let mut stream = broca_core::Provider::step_with_runtime(
        &provider,
        StepId(1),
        Arc::new(CallOptions::new(vec![])),
        broca_core::provider::StepRuntime {
            cancel: CancellationToken::new(),
            deadline: None,
            seal: None,
            progress: None,
            first_attempt: 0,
            attempt_hook: Some(hook),
        },
    )
    .await;
    let mut events = Vec::new();
    while let Some(event) = stream.recv().await {
        events.push(event);
    }
    assert_eq!(
        server.request_bodies().len(),
        2,
        "settlement wait must not consume the legacy network retry budget"
    );
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Finished { .. })
    )));
    assert_eq!(
        *waits.lock().unwrap(),
        [
            Duration::from_secs(239),
            Duration::from_secs(2),
            Duration::from_secs(239)
        ]
    );
}

#[tokio::test]
async fn legacy_entry_points_match_wall_boundary_expectations() {
    for (second_elapsed, expected_count) in [
        (Duration::from_secs(18) - Duration::from_nanos(1), 3),
        (Duration::from_secs(18), 3),
        (Duration::from_secs(18) + Duration::from_nanos(1), 2),
    ] {
        let mut paths = Vec::new();
        for runtime_entry in [false, true] {
            let server = ScriptedServer::start(vec![
                ScriptResponse::Http {
                    status: 429,
                    headers: vec![("retry-after", "2")],
                    body: b"",
                },
                ScriptResponse::Http {
                    status: 429,
                    headers: vec![("retry-after", "120")],
                    body: b"",
                },
                ScriptResponse::Http {
                    status: 429,
                    headers: vec![("retry-after", "2")],
                    body: b"",
                },
            ])
            .await;
            let mut provider = retry_test_provider(&server, 0);
            let waits = use_virtual_retry_waits(&mut provider);
            provider.retry_clock.virtual_retry_attempts = Some(Arc::new(Mutex::new(
                [Duration::from_secs(100), second_elapsed, Duration::ZERO].into(),
            )));
            provider.retry_clock.virtual_wait_overhead = Some(Duration::ZERO);
            let mut stream = if runtime_entry {
                broca_core::Provider::step_with_runtime(
                    &provider,
                    StepId(1),
                    Arc::new(CallOptions::new(vec![])),
                    broca_core::provider::StepRuntime {
                        cancel: CancellationToken::new(),
                        deadline: None,
                        seal: None,
                        progress: None,
                        first_attempt: 0,
                        attempt_hook: None,
                    },
                )
                .await
            } else {
                provider.start_step(
                    StepId(1),
                    Arc::new(CallOptions::new(Vec::new())),
                    CancellationToken::new(),
                    None,
                )
            };
            let mut records = Vec::new();
            let mut retries_used = None;
            while let Some(event) = stream.recv().await {
                if let NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) =
                    event
                {
                    assert_eq!(attempt.attempt as usize, records.len());
                    assert_eq!(attempt.outcome, AttemptOutcome::HttpError);
                    assert_eq!(attempt.http_status, Some(429));
                    assert_eq!(attempt.transport.as_deref(), Some("https"));
                    assert_eq!(attempt.response_body_len, 0);
                    assert_eq!(
                        attempt.request_body_len as usize,
                        br#"{"retry_test":"byte-identical"}"#.len()
                    );
                    let mut value = serde_json::to_value(attempt).unwrap();
                    value.as_object_mut().unwrap().remove("ts_ms");
                    value.as_object_mut().unwrap().remove("sent_at_ms");
                    records.push(value);
                } else if let NormalizedEvent::Control(ProviderControl::Error {
                    retries_used: retries,
                    ..
                }) = event
                {
                    retries_used = Some(retries);
                }
            }
            // The strict greater-than predicate permits equality at 240 s, but not one nanosecond
            // beyond it: 100 s + 2 s + second_elapsed + the 120 s Retry-After.
            assert_eq!(records.len(), expected_count);
            assert_eq!(retries_used, Some(expected_count as u32 - 1));
            assert_eq!(server.request_bodies().len(), expected_count);
            assert!(
                provider.client.journaled.clients.get().is_none(),
                "legacy retry must not build journal-only HTTP pools"
            );
            assert_eq!(
                *waits.lock().unwrap(),
                if expected_count == 3 {
                    vec![Duration::from_secs(2), Duration::from_secs(120)]
                } else {
                    vec![Duration::from_secs(2)]
                }
            );
            paths.push(records);
        }
        assert_eq!(paths[0], paths[1], "legacy entry points must produce identical attempt records apart from wall-clock timestamps");
    }
}

#[tokio::test]
async fn warm_default_hook_refuses_settlement() {
    use broca_core::run::AttemptHook;
    let hook = TestAttemptHook::new(Default::default());
    let error = hook
        .warm_finished(
            broca_core::attempt_id::mint_attempt_id().unwrap(),
            Default::default(),
            None,
            "aa".into(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.provider_code.as_deref(),
        Some("attempt_durability_failed")
    );
    assert!(hook.sends.lock().unwrap().is_empty());
    assert!(hook.finished.lock().unwrap().is_empty());
    assert!(hook.order.lock().unwrap().is_empty());
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
    let family =
        RetryTestFamily::with_behaviors(vec![RetryAttemptBehavior::Success; super::MAX_ATTEMPTS]);
    let frozen = openai_chat_frozen_config("retry-map-sharing");
    let normalizer_factory = || super::build_retry_normalizer(&family, &frozen, StepId(1), &names);

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
            .filter(|event| matches!(event, NormalizedEvent::Display(DisplayEvent::DisplayReset)))
            .count(),
        1,
        "one reset separates the failed and successful generations"
    );
    let reset = events
        .iter()
        .position(|event| matches!(event, NormalizedEvent::Display(DisplayEvent::DisplayReset)));
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
    let provider =
        retry_test_provider_with_behaviors(&server, vec![RetryAttemptBehavior::EmptyCompletion]);

    let events = collect_step_events(&provider, CancellationToken::new(), None).await;
    assert!(terminal_error_if_any(&events).is_none());
    assert_eq!(server.request_bodies().len(), 2);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, NormalizedEvent::Display(DisplayEvent::DisplayReset)))
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
    // Room for the OpenAI Responses families' Codex schedule: six stream rounds of one send
    // and four request-layer re-sends each. Other families stop after six sends.
    let server = ScriptedServer::start(vec![response; 30]).await;
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
    // Codex's schedule for an HTTPS 5xx: each of the six stream rounds (the first send and
    // five stream retries) is one send plus four request-layer re-sends.
    assert_deadline_retry_count(&events, 29);
    assert_eq!(sends, 30);
    assert_eq!(waits, 29);
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

/// A stream that trickles below the configured floor must fail once as `Transient` and must not
/// reissue the same request to the same provider. Retrying would only re-bill the degraded
/// endpoint; the router needs the health signal immediately.
///
/// The floor is read from process-global environment variables at every stream start, so
/// setting them here would change the floor for every other stream test running in this
/// binary at the same time, and a lock only this test takes cannot stop that. The test body
/// therefore runs in a child test process that is started with the variables already set;
/// nothing in this process ever writes them.
///
/// Real wall time (not `start_paused`): the client parks on socket IO while the server drips,
/// so virtual time would never auto-advance past the drip sleeps.
#[tokio::test]
async fn stream_floor_breach_is_transient_and_not_reissued() {
    const CHILD: &str = "BROCA_TEST_STREAM_FLOOR_CHILD";
    const NAME: &str = "transport::tests::stream_floor_breach_is_transient_and_not_reissued";
    if std::env::var_os(CHILD).is_none() {
        let output = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", NAME, "--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .env("BROCA_MIN_STREAM_BYTES_PER_SEC", "50")
            .env("BROCA_MIN_STREAM_WINDOW_SECS", "1")
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "stream floor child failed:\n{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        // The child must have run the body, not matched nothing and passed.
        assert!(
            stdout.contains("1 passed"),
            "stream floor child ran no test:\n{stdout}"
        );
        return;
    }
    assert_eq!(
        super::stream_floor_from_env().map(|floor| floor.window),
        Some(Duration::from_secs(1)),
        "the child must see the floor its parent set"
    );

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

/// OpenRouter reports an upstream fault after its 200 as a chunk whose `error.code` is the
/// upstream HTTP status. A 502 there is the same passing fault as an HTTP 502, so the step
/// re-sends and completes instead of ending permanent.
#[tokio::test]
async fn openrouter_mid_stream_upstream_error_retries_and_completes() {
    const ERROR_STREAM: &[u8] = concat!(
        "data: {\"id\":\"gen-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\"},",
        "\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"gen-1\",\"error\":{\"code\":502,\"message\":\"Provider returned error\",",
        "\"metadata\":{\"error_type\":\"provider_unavailable\"}},",
        "\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\"},\"finish_reason\":\"error\"}]}\n\n",
    )
    .as_bytes();
    const OK_STREAM: &[u8] = concat!(
        "data: {\"id\":\"gen-2\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},",
        "\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    )
    .as_bytes();
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status: 200,
            headers: vec![("content-type", "text/event-stream")],
            body: ERROR_STREAM,
        },
        ScriptResponse::Http {
            status: 200,
            headers: vec![("content-type", "text/event-stream")],
            body: OK_STREAM,
        },
    ])
    .await;
    let mut frozen = openai_chat_frozen_config("openai/gpt-4o");
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
    assert!(
        !events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Error { .. })
        )),
        "{:?}",
        events.iter().find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => Some(error),
            _ => None,
        })
    );
    assert_eq!(server.request_bodies().len(), 2);
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Finished {
            retries_used: 1,
            ..
        })
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

/// A ChatGPT plan allowance used up mid-stream, with a stated reset, is the same account wall
/// an HTTP 429 reports: the step ends after one attempt with a transient
/// `account_limit_exhausted` that keeps the reset, and the attempt records the wall for the
/// account.
#[tokio::test]
async fn timed_responses_stream_account_wall_ends_the_step_with_its_reset() {
    const RESETS_AT: u64 = 4_102_444_800; // 2100-01-01, always in the future
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status: 200,
            headers: vec![("content-type", "text/event-stream")],
            body: concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_wall\"}}\n\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_wall\",\"status\":\"failed\",",
                "\"error\":{\"code\":\"usage_limit_reached\",\"message\":\"The usage limit has been reached\",",
                "\"resets_at\":4102444800}}}\n\n",
            )
            .as_bytes(),
        },
        // What a re-send would get; the wall must leave it unused.
        ScriptResponse::Http {
            status: 200,
            headers: vec![("content-type", "text/event-stream")],
            body: concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_ok\"}}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"status\":\"completed\"}}\n\n",
            )
            .as_bytes(),
        },
    ])
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

    assert_eq!(server.request_bodies().len(), 1);
    let error = terminal_error(&events);
    assert_eq!(error.class, ErrorClass::Transient);
    assert!(error.is_account_limit_exhausted(), "{error:?}");
    assert_eq!(error.resets_at_ms, Some(RESETS_AT * 1_000));
    assert_eq!(error.resets_at_source.as_deref(), Some("body"));
    let limit = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                attempt.account_observation.limit.as_ref()
            }
            _ => None,
        })
        .expect("the attempt records the account wall");
    assert_eq!(
        limit.disposition,
        broca_protocol::provider::AccountLimitDisposition::ACCOUNT
    );
    assert_eq!(limit.resets_at_ms, Some(RESETS_AT * 1_000));
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
        provider_details: None,
        cause: None,
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
        account_continue: None,
    };
    assert_eq!(retry_delay(&error, 0), Duration::from_millis(2_500));
}

#[test]
fn retry_after_delay_is_capped_at_one_hundred_twenty_seconds() {
    let error = ProviderError {
        provider_details: None,
        cause: None,
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
        account_continue: None,
    };
    assert_eq!(retry_delay(&error, 0), Duration::from_secs(120));
}

#[test]
fn computed_backoff_doubles_from_two_seconds_to_a_thirty_second_cap() {
    let error = ProviderError {
        provider_details: None,
        cause: None,
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
        account_continue: None,
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
        _status: u16,
    ) -> crate::credential_refresh::RefreshFuture<'_, crate::credential_refresh::RefreshedCredential>
    {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            crate::credential_refresh::RejectedRefresh::Token(
                crate::credential_refresh::RefreshedCredential {
                    hosts: Default::default(),
                    account_ref: self.account_ref.clone(),
                    auth: AuthHandle::Bearer("fresh-test-token".into()),
                    evidence: super::CredentialRefresh {
                        vault_op: "get".into(),
                        min_ttl_ms: None,
                        force_refresh: true,
                        record_version: Some(2),
                        reason: Some(super::CredentialRefresh::REASON_REJECTED.into()),
                    },
                },
            )
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
    assert_eq!(
        error.limit_window,
        Some(broca_protocol::error::AccountLimitWindow::Quota(
            "seven_day".into()
        ))
    );
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
    let error = classify_anthropic_429(WireFamilyId::OpenAiChat, &CAPTURED_EXHAUSTED_HEADERS, now);
    assert_eq!(error.provider_code.as_deref(), Some("rate_limit_error"));
}

/// A refused service tier stays a permanent tier refusal even if its 429 also says the account
/// window is `rejected`: the reset would not make the tier available.
#[test]
fn a_refused_service_tier_is_not_relabelled_an_exhausted_account() {
    let now = UNIX_EPOCH + Duration::from_secs(1_790_387_965);
    let headers = crate::wire::ResponseHeaders::new(CAPTURED_EXHAUSTED_HEADERS.iter().copied());
    let tier_refusal = ProviderError { provider_details: None, cause: None, provider_subcode: None,
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
    affordable_max_tokens: None, account_continue: None, };
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
/// for byte from the terminal error recorded in broca's session log for a production run on
/// 2026-09-16 (log file `4430787e24105d49.wal`). Using the captured bytes rather than a
/// hand-written body pins the shape the backend actually sends.
const CHATGPT_USAGE_LIMIT_BODY: &str = r#"{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"pro","resets_at":1790021043,"eligible_promo":null,"resets_in_seconds":441721}}"#;

const KIMI_WEEKLY_USAGE_LIMIT_BODY: &str = r#"{"error":{"message":"You've reached your weekly (7-day) usage limit. Your quota will reset when the current 7-day window ends. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/membership/subscription?tab=quota","type":"access_terminated_error"}}"#;

const META_SUBSCRIPTION_QUOTA_429_BODY: &str = r#"{"error":{"code":"rate_limit_exceeded","message":"Subscription quota exhausted. Your usage window resets at 2026-10-26T00:00:00Z.","param":null,"resets_at":1792972800,"type":"rate_limit_error"}}"#;

fn classify_chatgpt_429(family: WireFamilyId, body: &str, now: SystemTime) -> ProviderError {
    let headers = crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>());
    let error = classify_http_error(429, body, None, None);
    classify_account_limit_exhausted(family, 429, &headers, body, now, error)
}

fn classify_meta_429(body: &str) -> ProviderError {
    let headers = crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>());
    let error = classify_http_error(429, body, None, None);
    classify_account_limit_exhausted(
        WireFamilyId::OpenAiResponses,
        429,
        &headers,
        body,
        UNIX_EPOCH,
        error,
    )
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

/// The quota headers of a real ChatGPT/Codex 429 that refused an account past its weekly
/// allowance, captured 2026-11-02 (`broca-module-serve/tests/fixtures/usage-observe/
/// codex-quota-capture-2026-11-02.json`, its first entry).
fn captured_chatgpt_429_quota_headers() -> Vec<(String, String)> {
    let capture: Value = serde_json::from_str(include_str!(
        "../../../broca-module-serve/tests/fixtures/usage-observe/codex-quota-capture-2026-11-02.json"
    ))
    .unwrap();
    assert_eq!(capture[0]["status"], 429);
    capture[0]["headers"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
        .collect()
}

fn classify_chatgpt_429_with(headers: &[(String, String)]) -> ProviderError {
    let headers = crate::wire::ResponseHeaders::new(headers.iter().cloned());
    let error = classify_http_error(429, CHATGPT_USAGE_LIMIT_BODY, None, None);
    classify_account_limit_exhausted(
        WireFamilyId::OpenAiResponsesChatgpt,
        429,
        &headers,
        CHATGPT_USAGE_LIMIT_BODY,
        UNIX_EPOCH + Duration::from_millis(1_789_579_322_081),
        error,
    )
}

fn with_header(
    mut headers: Vec<(String, String)>,
    name: &str,
    value: Option<&str>,
) -> Vec<(String, String)> {
    headers.retain(|(candidate, _)| candidate != name);
    if let Some(value) = value {
        headers.push((name.to_owned(), value.to_owned()));
    }
    headers
}

/// A ChatGPT usage-limit 429 takes its window from the quota headers that came with it: the
/// captured weekly refusal's spent primary window is 10080 minutes long, so it is
/// `seven_day`. The other cases are that capture with one header changed.
#[test]
fn a_chatgpt_usage_limit_takes_the_window_its_quota_headers_show_spent() {
    let captured = captured_chatgpt_429_quota_headers();
    let error = classify_chatgpt_429_with(&captured);
    assert!(error.is_account_limit_exhausted());
    assert_eq!(error.limit_window.as_deref(), Some("seven_day"));
    assert_eq!(error.resets_at_ms, Some(1_790_021_043_000));

    // The capture with its spent window five hours long instead.
    let five_hour = with_header(
        captured.clone(),
        "x-codex-primary-window-minutes",
        Some("300"),
    );
    assert_eq!(
        classify_chatgpt_429_with(&five_hour)
            .limit_window
            .as_deref(),
        Some("five_hour")
    );
    // The spent window may be the secondary one.
    let secondary = with_header(
        with_header(
            with_header(captured.clone(), "x-codex-primary-used-percent", Some("40")),
            "x-codex-secondary-used-percent",
            Some("100"),
        ),
        "x-codex-secondary-window-minutes",
        Some("300"),
    );
    assert_eq!(
        classify_chatgpt_429_with(&secondary)
            .limit_window
            .as_deref(),
        Some("five_hour")
    );

    // No window when the headers don't state exactly one spent window of a known length.
    for (case, headers) in [
        ("no quota headers", Vec::new()),
        (
            "both spent",
            with_header(
                with_header(
                    captured.clone(),
                    "x-codex-secondary-used-percent",
                    Some("100"),
                ),
                "x-codex-secondary-window-minutes",
                Some("300"),
            ),
        ),
        (
            "neither spent",
            with_header(captured.clone(), "x-codex-primary-used-percent", Some("99")),
        ),
        (
            "a header missing",
            with_header(captured.clone(), "x-codex-secondary-used-percent", None),
        ),
        (
            "a header that doesn't parse",
            with_header(
                captured.clone(),
                "x-codex-primary-window-minutes",
                Some("weekly"),
            ),
        ),
        (
            "another length",
            with_header(
                captured.clone(),
                "x-codex-primary-window-minutes",
                Some("1440"),
            ),
        ),
    ] {
        let error = classify_chatgpt_429_with(&headers);
        assert!(error.is_account_limit_exhausted(), "{case}");
        assert_eq!(error.limit_window, None, "{case}");
    }
}

#[test]
fn meta_429_without_resets_at_stays_rate_limited() {
    let body = r#"{"error":{"code":"rate_limit_exceeded","message":"Request rate limit reached","type":"rate_limit_error"}}"#;
    let error = classify_meta_429(body);

    assert_eq!(error.class, ErrorClass::Transient);
    assert!(error.is_rate_limited());
    assert!(!error.is_account_limit_exhausted());
    assert_eq!(error.provider_code.as_deref(), Some("rate_limit_exceeded"));
    assert_eq!(error.resets_at_ms, None);
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
        provider_details: None,
        cause: None,
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
        account_continue: None,
    };
    let usage_no_reset =
        r#"{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached"}}"#;
    let usage_string_reset = r#"{"error":{"type":"usage_limit_reached","resets_at":"1790021043","resets_in_seconds":60}}"#;
    let usage_not_included_code =
        r#"{"error":{"message":"not in plan","code":"usage_not_included"}}"#;
    let usage_not_included_type =
        r#"{"error":{"type":"usage_not_included","message":"not in plan"}}"#;
    let not_an_sk_key =
        r#"{"error":{"message":"Incorrect API key provided: sess-123","code":"invalid_api_key"}}"#;
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
    let document: Value =
        serde_json::from_str(cortexkit_chatgpt_policy::vectors::RESPONSES_JSON).unwrap();
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
    let document: Value =
        serde_json::from_str(cortexkit_chatgpt_policy::vectors::RESPONSES_JSON).unwrap();
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
        let error = super::classify_http_error_in_family(403, body, None, None, Some(family), None);
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
            provider_details: None,
            cause: None,
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
            account_continue: None,
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

#[test]
fn xai_subscription_spending_limit_is_an_untimed_account_wall() {
    let headers = crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>());
    for family in [WireFamilyId::OpenAiChat, WireFamilyId::OpenAiResponses] {
        for status in [402, 403] {
            for (method, expected_code, expected_class) in [
                (
                    Some("oauth"),
                    ACCOUNT_LIMIT_EXHAUSTED,
                    ErrorClass::Transient,
                ),
                (
                    Some("chatgpt"),
                    ACCOUNT_LIMIT_EXHAUSTED,
                    ErrorClass::Transient,
                ),
                (
                    Some("antigravity"),
                    ACCOUNT_LIMIT_EXHAUSTED,
                    ErrorClass::Transient,
                ),
                (Some("apikey"), BILLING_EXHAUSTED, ErrorClass::Permanent),
                (None, BILLING_EXHAUSTED, ErrorClass::Permanent),
            ] {
                let provider = LiveProvider::new(
                    OpenAiResponses,
                    frozen_config_for_family(family, "grok-4.7"),
                    AuthHandle::Bearer("test-token".into()),
                )
                .with_auth_method(method);
                let error = super::classify_account_limit_exhausted_with_auth(
                    provider.account_limit_context(),
                    status,
                    &headers,
                    XAI_SPENDING_LIMIT_BODY,
                    UNIX_EPOCH,
                    classify_http_error(status, XAI_SPENDING_LIMIT_BODY, None, None),
                );
                assert_eq!(
                    error.provider_code.as_deref(),
                    Some(expected_code),
                    "{family:?} {status} {method:?}"
                );
                assert_eq!(error.class, expected_class);
                assert_eq!(error.status, Some(status));
                assert_eq!(error.resets_at_ms, None);
                assert_eq!(error.resets_at_source, None);
                assert_eq!(
                    error.limit_window,
                    (expected_code == ACCOUNT_LIMIT_EXHAUSTED)
                        .then_some(broca_protocol::error::AccountLimitWindow::Credit)
                );
            }
        }
    }
    let context = super::AccountLimitContext {
        family: WireFamilyId::OpenAiResponses,
        subscription_auth: true,
    };
    for (status, body) in [
        (400, XAI_SPENDING_LIMIT_BODY),
        (403, r#"{"error":{"code":"insufficient_quota"}}"#),
    ] {
        let raw = classify_http_error(status, body, None, None);
        let error = super::classify_account_limit_exhausted_with_auth(
            context,
            status,
            &headers,
            body,
            UNIX_EPOCH,
            raw.clone(),
        );
        assert_eq!(error, raw);
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
    // Before this classification, the error is a plain permanent with no provider code.
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

/// Matches six captured Anthropic credit refusals from 2026-10-16 through 2026-10-27.
/// The fixed type and complete message are preserved; varying request ids are omitted.
const ANTHROPIC_CREDIT_BALANCE_BODY: &str = r#"{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}"#;

#[tokio::test]
async fn captured_xai_subscription_spending_reports_credit_window() {
    for retry_hint in [false, true] {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 403,
            headers: if retry_hint {
                vec![("retry-after", "60")]
            } else {
                Vec::new()
            },
            body: XAI_SPENDING_LIMIT_BODY.as_bytes(),
        }])
        .await;
        let mut provider = retry_test_provider(&server, 0).with_auth_method(Some("oauth"));
        let mut frozen = (*provider.frozen).clone();
        frozen.family_id = WireFamilyId::OpenAiChat;
        provider.frozen = Arc::new(frozen);
        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        let error = terminal_error(&events);
        assert!(error.is_account_limit_exhausted());
        assert_eq!(
            error.limit_window,
            Some(broca_protocol::error::AccountLimitWindow::Credit)
        );
        let attempt = events
            .iter()
            .find_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                    Some(attempt)
                }
                _ => None,
            })
            .expect("a refused HTTP attempt is reported");
        let report = broca_module_serve::account_routing::attempt_body(
            "credit-event",
            "admitted-run",
            attempt,
            None,
        );
        let observed = attempt
            .account_observation
            .limit
            .as_ref()
            .unwrap()
            .observed_at_ms;
        let mut expected = serde_json::json!({
            "event_id":"credit-event", "caller":{"runner":"broca"},
            "kind":"account_limit_exhausted", "admission_id":"admitted-run",
            "limit_window":"credit", "observed_at_ms":observed,
        });
        if retry_hint {
            expected["resets_at_ms"] = serde_json::json!(observed + 60_000);
            expected["resets_at_source"] = serde_json::json!("retry_after");
        }
        assert_eq!(report, expected);
        assert_eq!(server.request_bodies().len(), 1);
    }
}

#[test]
fn anthropic_subscription_credit_balance_keeps_permanent_billing_refusal() {
    let headers = crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>());
    let raw = classify_http_error(400, ANTHROPIC_CREDIT_BALANCE_BODY, None, None);
    let classified = super::classify_account_limit_exhausted_with_auth(
        super::AccountLimitContext {
            family: WireFamilyId::AnthropicMessages,
            subscription_auth: true,
        },
        400,
        &headers,
        ANTHROPIC_CREDIT_BALANCE_BODY,
        UNIX_EPOCH,
        raw.clone(),
    );
    assert_eq!(classified, raw);
    assert_eq!(classified.class, ErrorClass::Permanent);
    assert!(classified.is_billing_exhausted());
    assert_eq!(classified.limit_window, None);
}

#[test]
fn anthropic_credit_balance_400_is_billing_exhausted() {
    // The family-aware call explicitly selects AnthropicMessages; the public entry point
    // infers that family from the envelope. Both recognize the exact captured message.
    for error in [
        super::classify_http_error_in_family(
            400,
            ANTHROPIC_CREDIT_BALANCE_BODY,
            None,
            None,
            Some(WireFamilyId::AnthropicMessages),
            None,
        ),
        classify_http_error(400, ANTHROPIC_CREDIT_BALANCE_BODY, None, None),
    ] {
        assert_eq!(error.class, ErrorClass::Permanent);
        assert!(error.is_billing_exhausted());
        assert_eq!(error.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
        assert_eq!(error.status, Some(400));
        assert_eq!(error.resets_at_ms, None);
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
    let foreign =
        r#"{"error":{"type":"invalid_request_error","message":"Your credit balance is too low"}}"#;
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
    let body =
        r#"{"error":{"code":"insufficient_quota","message":"You exceeded your current quota"}}"#;
    let error = classify_http_error(429, body, None, None);
    assert_eq!(error.class, ErrorClass::Permanent);
    assert!(error.is_billing_exhausted());
    assert_eq!(error.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
    assert_eq!(error.resets_at_ms, None);
}

/// OpenAI's HTTP answer for an empty prepaid balance is `billing_exhausted` for the OpenAI
/// families and for a body with no family, keyed on either its `credit_balance_exhausted`
/// code or its `insufficient_quota` type, and keeps the provider's message. Another family's
/// body with the same code is left alone.
#[test]
fn openai_credit_balance_exhausted_http_body_is_billing_exhausted() {
    const CREDIT_BODY: &str = r#"{"error":{"message":"You have no credits remaining. Add credits to continue using the API.","type":"insufficient_quota","param":null,"code":"credit_balance_exhausted"}}"#;
    const TYPE_ONLY_BODY: &str = r#"{"error":{"message":"You have no credits remaining. Add credits to continue using the API.","type":"insufficient_quota","param":null,"code":null}}"#;
    const CODE_ONLY_BODY: &str = r#"{"error":{"message":"You have no credits remaining. Add credits to continue using the API.","code":"credit_balance_exhausted"}}"#;
    for family in [
        None,
        Some(WireFamilyId::OpenAiChat),
        Some(WireFamilyId::OpenAiResponses),
        Some(WireFamilyId::OpenAiResponsesChatgpt),
    ] {
        for body in [CREDIT_BODY, TYPE_ONLY_BODY, CODE_ONLY_BODY] {
            for status in [400, 402, 429] {
                let error = super::http_error_in_family(status, body, None, family, None);
                assert_eq!(
                    error.class,
                    ErrorClass::Permanent,
                    "{family:?} {status} {body}"
                );
                assert_eq!(
                    error.provider_code.as_deref(),
                    Some(BILLING_EXHAUSTED),
                    "{family:?} {status} {body}"
                );
                assert!(
                    error.message.contains("You have no credits remaining"),
                    "{}",
                    error.message
                );
                assert_eq!(error.status, Some(status));
            }
        }
    }
    let error =
        super::http_error_in_family(400, CODE_ONLY_BODY, None, Some(WireFamilyId::Gemini), None);
    assert_eq!(
        error.provider_code.as_deref(),
        Some("credit_balance_exhausted")
    );
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

/// OpenCode Zen's refusal from an account with no funds left, captured verbatim from a
/// production run. The type is a generic `server_error`; the 402 status is the signal.
const OPENCODE_INSUFFICIENT_FUNDS_BODY: &str = r#"{"error":{"type":"server_error","message":"Upstream request failed: Insufficient account funds"}}"#;

/// Classifies an HTTP refusal through the same steps as a live attempt: the status and body
/// table, the account-limit decoders, then the endpoint-specific payment check.
fn classify_at_url(url: &str, status: u16, body: &str) -> ProviderError {
    let headers = crate::wire::ResponseHeaders::new(std::iter::empty::<(&str, &str)>());
    let family = WireFamilyId::OpenAiChat;
    let error = super::classify_http_error_in_family(status, body, None, None, Some(family), None);
    let error = classify_account_limit_exhausted(family, status, &headers, body, UNIX_EPOCH, error);
    let error = super::opencode_payment_required(url, status, error);
    super::inception_payment_required(url, status, error)
}

const INCEPTION_ACCOUNT_INACTIVE_BODY: &str = r#"{"error":{"message":"Account is inactive","type":"account_error","param":null,"code":"account_error"}}"#;

#[test]
fn inception_402_is_billing_exhausted_regardless_of_body_wording() {
    for body in [
        INCEPTION_ACCOUNT_INACTIVE_BODY,
        r#"{"error":{"message":"Quota exceeded","type":"account_error"}}"#,
        "Payment Required",
    ] {
        let error = classify_at_url(
            "https://api.inceptionlabs.ai/v1/chat/completions",
            402,
            body,
        );
        assert_eq!(error.class, ErrorClass::Permanent);
        assert_eq!(error.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
        assert_eq!(error.status, Some(402));
        assert!(error.message.contains(body));
        assert!(error.is_credential_scoped());
        assert_eq!(error.resets_at_ms, None);
        assert_eq!(error.resets_at_source, None);
        assert_eq!(error.limit_window, None);
        assert_eq!(error.credential_id, None);
    }
}

#[test]
fn inception_billing_mapping_requires_402_and_the_exact_api_host() {
    for (url, status) in [
        ("https://api.example.com/v1/chat/completions", 402),
        (
            "https://api.inceptionlabs.ai.example.com/v1/chat/completions",
            402,
        ),
        (
            "https://example.com/api.inceptionlabs.ai/v1/chat/completions",
            402,
        ),
        (
            "https://api.inceptionlabs.ai@example.com/v1/chat/completions",
            402,
        ),
        ("https://inceptionlabs.ai/v1/chat/completions", 402),
        ("not a URL", 402),
        ("https://api.inceptionlabs.ai/v1/chat/completions", 403),
        ("https://api.inceptionlabs.ai/v1/chat/completions", 429),
    ] {
        let error = classify_at_url(url, status, INCEPTION_ACCOUNT_INACTIVE_BODY);
        let unchanged = classify_http_error(status, INCEPTION_ACCOUNT_INACTIVE_BODY, None, None);
        assert_eq!(error, unchanged, "{url} {status}");
    }
}

fn assert_opencode_billing_exhausted(error: &ProviderError) {
    assert_eq!(error.class, ErrorClass::Permanent);
    assert!(error.is_billing_exhausted());
    assert_eq!(error.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
    assert_eq!(error.status, Some(402));
    assert_eq!(error.resets_at_ms, None);
    assert_eq!(error.account_ref, None);
    assert_eq!(error.credential_id, None);
    assert!(
        error.message.contains("Insufficient account funds"),
        "the provider's message is kept: {}",
        error.message
    );
}

#[test]
fn opencode_zen_402_is_billing_exhausted() {
    let error = classify_at_url(
        "https://opencode.ai/zen/v1/chat/completions",
        402,
        OPENCODE_INSUFFICIENT_FUNDS_BODY,
    );
    assert_opencode_billing_exhausted(&error);
}

#[test]
fn opencode_go_402_is_billing_exhausted() {
    let error = classify_at_url(
        "https://opencode.ai/inference/go/v1/chat/completions",
        402,
        OPENCODE_INSUFFICIENT_FUNDS_BODY,
    );
    assert_opencode_billing_exhausted(&error);
}

#[test]
fn the_opencode_402_mapping_needs_status_402_and_an_opencode_gateway() {
    let unchanged = classify_http_error(402, OPENCODE_INSUFFICIENT_FUNDS_BODY, None, None);
    assert_eq!(unchanged.class, ErrorClass::Permanent);
    assert_eq!(unchanged.provider_code.as_deref(), Some("server_error"));
    for (url, status) in [
        ("https://api.example.com/v1/chat/completions", 402),
        ("https://openrouter.ai/api/v1/chat/completions", 402),
        // OpenCode's host, but not one of its metered gateways.
        ("https://opencode.ai/api/v1/chat/completions", 402),
        // A look-alike host is not OpenCode.
        (
            "https://opencode.ai.example.com/zen/v1/chat/completions",
            402,
        ),
        ("https://opencode.ai/zen/v1/chat/completions", 500),
    ] {
        let error = classify_at_url(url, status, OPENCODE_INSUFFICIENT_FUNDS_BODY);
        assert!(!error.is_billing_exhausted(), "{url} {status}");
        assert_eq!(
            error.provider_code.as_deref(),
            Some("server_error"),
            "{url} {status}"
        );
    }
    // OpenRouter's unaffordable-request 402 keeps its own code on its own host.
    let openrouter = classify_at_url(
        "https://openrouter.ai/api/v1/chat/completions",
        402,
        OPENROUTER_CREDIT_402_BODY,
    );
    assert!(openrouter.is_credit_insufficient());
}

fn chatgpt_retry_test_provider(server: &ScriptedServer) -> LiveProvider<RetryTestFamily> {
    let mut provider = retry_test_provider(server, 0);
    let mut frozen = (*provider.frozen).clone();
    frozen.family_id = WireFamilyId::OpenAiResponsesChatgpt;
    provider.frozen = Arc::new(frozen);
    provider
}

fn meta_responses_retry_test_provider(server: &ScriptedServer) -> LiveProvider<RetryTestFamily> {
    let mut provider = retry_test_provider(server, 0);
    let mut frozen = (*provider.frozen).clone();
    frozen.family_id = WireFamilyId::OpenAiResponses;
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
async fn meta_subscription_quota_429_ends_the_step_after_one_attempt() {
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 429,
        headers: Vec::new(),
        body: META_SUBSCRIPTION_QUOTA_429_BODY.as_bytes(),
    }])
    .await;
    let mut provider = meta_responses_retry_test_provider(&server);
    let waits = use_virtual_retry_waits(&mut provider);

    let events = collect_step_events(&provider, CancellationToken::new(), None).await;
    let error = terminal_error(&events);
    assert_eq!(error.class, ErrorClass::Transient);
    assert_eq!(
        error.provider_code.as_deref(),
        Some(ACCOUNT_LIMIT_EXHAUSTED)
    );
    assert_eq!(error.status, Some(429));
    assert_eq!(error.resets_at_ms, Some(1_792_972_800_000));
    assert_eq!(error.resets_at_source.as_deref(), Some("body"));
    assert_eq!(error.limit_window, None);
    assert_eq!(
        server.request_bodies().len(),
        1,
        "never re-sent within the step"
    );
    assert!(waits.lock().unwrap().is_empty(), "no retry wait");
}

/// An ordinary ChatGPT 429 is not an exhausted account. As in Codex it is retried only when
/// the server gave a retry time, and then after exactly that time; without one it ends the
/// step, still `transient`.
#[tokio::test]
async fn an_ordinary_chatgpt_429_retries_only_on_the_server_retry_time() {
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status: 429,
            headers: vec![("Retry-After", "3")],
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
    let waits = use_virtual_retry_waits(&mut provider);

    let events = collect_step_events(&provider, CancellationToken::new(), None).await;
    assert!(terminal_error_if_any(&events).is_none());
    assert_eq!(server.request_bodies().len(), 2);
    assert_eq!(*waits.lock().unwrap(), vec![Duration::from_secs(3)]);

    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 429,
        headers: Vec::new(),
        body: br#"{"error":{"type":"rate_limit_exceeded","message":"Rate limit reached"}}"#,
    }])
    .await;
    let mut provider = chatgpt_retry_test_provider(&server);
    let waits = use_virtual_retry_waits(&mut provider);
    let events = collect_step_events(&provider, CancellationToken::new(), None).await;
    let error = terminal_error(&events);
    assert_eq!(error.class, ErrorClass::Transient);
    assert!(!error.is_account_limit_exhausted());
    assert_eq!(server.request_bodies().len(), 1);
    assert!(waits.lock().unwrap().is_empty());
}

/// An Antigravity (Code Assist) quota-wall 429 body. Everything up to and including
/// `"quotaResetTimeStamp": "2026-` is copied byte for byte from the terminal error recorded
/// in broca's log for a production run on 2026-10-03 (log file `5224d2a1d3c2f34b.wal`); the
/// log's message cap cut the body there. The rest is COMPLETED
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
fn google_rpc_quota_details_survive_error_message_truncation() {
    let body = serde_json::json!({
        "error": {
            "code": 429,
            "message": format!("Individual quota reached. {}", "x".repeat(700)),
            "status": "RESOURCE_EXHAUSTED",
            "details": [
                {
                    "@type": GOOGLE_RPC_ERROR_INFO_TYPE,
                    "reason": "QUOTA_EXHAUSTED",
                    "domain": "cloudcode-pa.googleapis.com",
                    "metadata": {
                        "model": "gemini-3.8-flash-high",
                        "quotaResetDelay": "21m0.959250039s",
                        "quotaResetTimeStamp": "2026-10-03T03:40:39Z"
                    }
                },
                {
                    "@type": GOOGLE_RPC_QUOTA_FAILURE_TYPE,
                    "violations": [{
                        "subject": "project:123",
                        "description": "The model-tier request allowance is exhausted.",
                        "quotaMetric": "generativelanguage.googleapis.com/generate_content_free_tier_requests",
                        "quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier",
                        "quotaDimensions": {"model": "gemini-3.8-flash-high", "region": "global"},
                        "quotaValue": "1000",
                        "futureQuotaValue": "2000"
                    }]
                },
                {
                    "@type": GOOGLE_RPC_RETRY_INFO_TYPE,
                    "retryDelay": "21m0.959250039s"
                }
            ]
        }
    })
    .to_string();
    assert!(body.len() > MAX_ERROR_BODY_BYTES);

    for family in [WireFamilyId::GoogleCodeAssist, WireFamilyId::Gemini] {
        let error = classify_google_429(family, &body, UNIX_EPOCH);
        assert!(error.is_account_limit_exhausted(), "{family:?}");
        assert!(error.message.len() <= 5 + MAX_ERROR_BODY_BYTES);
        let details = serde_json::to_value(error.provider_details.unwrap()).unwrap();
        assert_eq!(details["errorInfo"]["reason"], "QUOTA_EXHAUSTED");
        assert_eq!(
            details["errorInfo"]["domain"],
            "cloudcode-pa.googleapis.com"
        );
        assert_eq!(
            details["errorInfo"]["metadata"],
            serde_json::json!({
                "model": "gemini-3.8-flash-high",
                "quotaResetDelay": "21m0.959250039s",
                "quotaResetTimeStamp": "2026-10-03T03:40:39Z"
            })
        );
        assert_eq!(
            details["quotaFailure"]["violations"][0],
            serde_json::json!({
                "subject": "project:123",
                "description": "The model-tier request allowance is exhausted.",
                "quotaMetric": "generativelanguage.googleapis.com/generate_content_free_tier_requests",
                "quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier",
                "quotaDimensions": {"model": "gemini-3.8-flash-high", "region": "global"},
                "quotaValue": "1000",
                "futureQuotaValue": "2000"
            })
        );
        assert_eq!(details["retryInfo"]["retryDelay"], "21m0.959250039s");
    }
}

#[test]
fn google_rpc_quota_details_are_bounded_and_malformed_details_do_not_break_classification() {
    let metadata = (0..64)
        .map(|index| (format!("key-{index}"), serde_json::json!("m".repeat(200))))
        .collect::<serde_json::Map<_, _>>();
    let dimensions = (0..32)
        .map(|index| {
            (
                format!("dimension-{index}"),
                serde_json::json!("d".repeat(200)),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let violation = serde_json::json!({
        "subject": "subject-oversized",
        "description": "d".repeat(400),
        "quotaMetric": "q".repeat(200),
        "quotaId": "i".repeat(200),
        "quotaDimensions": dimensions,
        "quotaValue": "1",
        "futureQuotaValue": "2"
    });
    let mut details = vec![serde_json::json!({
        "@type": GOOGLE_RPC_ERROR_INFO_TYPE,
        "reason": "QUOTA_EXHAUSTED",
        "metadata": metadata
    })];
    details.extend((0..64).map(|_| {
        serde_json::json!({
            "@type": GOOGLE_RPC_QUOTA_FAILURE_TYPE,
            "violations": [violation.clone()]
        })
    }));
    let body = serde_json::json!({
        "error": {
            "status": "RESOURCE_EXHAUSTED",
            "details": details
        }
    })
    .to_string();
    let error = classify_google_429(WireFamilyId::GoogleCodeAssist, &body, UNIX_EPOCH);
    assert!(error.is_account_limit_exhausted());
    let details = error.provider_details.unwrap();
    let info = details.error_info.unwrap();
    assert_eq!(info.metadata.len(), GOOGLE_RPC_METADATA_MAX_KEYS);
    assert!(info
        .metadata
        .values()
        .all(|value| value.chars().count() <= 128));
    let violations = details.quota_failure.unwrap().violations;
    assert_eq!(violations.len(), GOOGLE_RPC_VIOLATIONS_MAX);
    assert!(violations.iter().all(|violation| {
        violation.description.as_ref().unwrap().chars().count() <= 256
            && violation.quota_metric.as_ref().unwrap().chars().count() <= 128
            && violation.quota_id.as_ref().unwrap().chars().count() <= 128
            && violation.quota_dimensions.len() <= GOOGLE_RPC_DIMENSIONS_MAX_KEYS
            && violation
                .quota_dimensions
                .values()
                .all(|value| value.chars().count() <= 128)
    }));

    for malformed in [
        r#"{"error":{"status":"RESOURCE_EXHAUSTED","details":{"not":"an array"}}}"#,
        r#"{"error":{"status":"RESOURCE_EXHAUSTED","details":[null,7,{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":"bad"}]}}"#,
    ] {
        let error = classify_google_429(WireFamilyId::GoogleCodeAssist, malformed, UNIX_EPOCH);
        assert_eq!(error.class, ErrorClass::Transient);
        assert_eq!(error.provider_code.as_deref(), Some("RESOURCE_EXHAUSTED"));
        assert!(!error.is_account_limit_exhausted());
        assert!(error.provider_details.is_none());
    }
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
                (attempt.outcome == AttemptOutcome::HttpError && attempt.http_status == Some(429))
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

/// A retry-test provider whose requests carry `family`, sent over plain HTTPS (the ChatGPT
/// family's socket is switched off), so the step takes that family's retry decision.
fn family_retry_test_provider(
    server: &ScriptedServer,
    family: WireFamilyId,
) -> LiveProvider<RetryTestFamily> {
    let mut provider = retry_test_provider(server, 0);
    Arc::make_mut(&mut provider.frozen).family_id = family;
    provider
}

const CODEX_FAMILIES: [WireFamilyId; 2] = [
    WireFamilyId::OpenAiResponses,
    WireFamilyId::OpenAiResponsesChatgpt,
];

/// A 429 capacity refusal in the shape OpenAI's Codex backend sends.
const CAPACITY_429_BODY: &[u8] = br#"{"error":{"code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later","type":"rate_limit_error"},"status":429}"#;

fn attempt_digests(events: &[NormalizedEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                Some(attempt.request_body_sha256.clone())
            }
            _ => None,
        })
        .collect()
}

/// A 429 with `Retry-After: 469` waits exactly 469 s (no 120 s cap) and then sends the same
/// bytes again, for both OpenAI Responses families.
#[tokio::test]
async fn codex_families_wait_exactly_the_server_retry_after_and_resend_the_same_bytes() {
    for family in CODEX_FAMILIES {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 429,
                headers: vec![("Retry-After", "469")],
                body: CAPACITY_429_BODY,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            },
        ])
        .await;
        let mut provider = family_retry_test_provider(&server, family);
        let waits = use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none(), "{family:?}");
        assert_eq!(
            *waits.lock().unwrap(),
            vec![Duration::from_secs(469)],
            "{family:?}"
        );
        let bodies = server.request_bodies();
        assert_eq!(bodies.len(), 2, "{family:?}");
        assert_eq!(bodies[0], bodies[1], "{family:?}");
        let digests = attempt_digests(&events);
        assert_eq!(digests.len(), 2, "{family:?}");
        assert_eq!(digests[0], digests[1], "{family:?}");
    }
}

/// A 429 without a server retry time ends the step at once, as Codex's `RetryLimit` does.
/// `retry-after-ms` is not a retry time for these families: Codex does not read it.
#[tokio::test]
async fn codex_families_end_a_429_without_a_server_retry_time() {
    for family in CODEX_FAMILIES {
        for headers in [Vec::new(), vec![("retry-after-ms", "5000")]] {
            let server = ScriptedServer::start(vec![
                ScriptResponse::Http {
                    status: 429,
                    headers: headers.clone(),
                    body: CAPACITY_429_BODY,
                },
                ScriptResponse::Http {
                    status: 200,
                    headers: Vec::new(),
                    body: b"must not be sent",
                },
            ])
            .await;
            let mut provider = family_retry_test_provider(&server, family);
            let waits = use_virtual_retry_waits(&mut provider);

            let events = collect_step_events(&provider, CancellationToken::new(), None).await;
            let error = terminal_error(&events);
            assert_eq!(error.status, Some(429), "{family:?} {headers:?}");
            assert_eq!(error.class, ErrorClass::Transient, "{family:?} {headers:?}");
            assert_eq!(error.retry_after_secs, None, "{family:?} {headers:?}");
            assert_eq!(server.request_bodies().len(), 1, "{family:?} {headers:?}");
            assert!(waits.lock().unwrap().is_empty(), "{family:?} {headers:?}");
        }
    }
}

/// A step deadline that comes before the server's time ends the step at once, with the
/// server's time on the error. Without the deadline the same answer is retried (see
/// `codex_families_wait_exactly_the_server_retry_after_and_resend_the_same_bytes`).
#[tokio::test]
async fn codex_families_end_when_the_server_retry_after_is_past_the_step_deadline() {
    for family in CODEX_FAMILIES {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 429,
                headers: vec![("Retry-After", "469")],
                body: CAPACITY_429_BODY,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"must not be sent",
            },
        ])
        .await;
        let mut provider = family_retry_test_provider(&server, family);
        let waits = use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(
            &provider,
            CancellationToken::new(),
            Some(Instant::now() + Duration::from_secs(100)),
        )
        .await;
        let error = terminal_error(&events);
        assert_eq!(error.status, Some(429), "{family:?}");
        assert_eq!(error.retry_after_secs, Some(469.0), "{family:?}");
        assert_eq!(server.request_bodies().len(), 1, "{family:?}");
        assert!(waits.lock().unwrap().is_empty(), "{family:?}");
    }
}

/// A ChatGPT `usage_limit_reached` 429 stays `account_limit_exhausted` with its reset time,
/// and ends the step even though it states a retry time: Codex never retries it.
#[tokio::test]
async fn codex_usage_limit_reached_stays_account_limit_exhausted() {
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 429,
        headers: vec![("Retry-After", "30")],
        body: br#"{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"pro","resets_at":1790021043,"eligible_promo":null,"resets_in_seconds":441721}}"#,
    }])
    .await;
    let mut provider = family_retry_test_provider(&server, WireFamilyId::OpenAiResponsesChatgpt);
    let waits = use_virtual_retry_waits(&mut provider);

    let events = collect_step_events(&provider, CancellationToken::new(), None).await;
    let error = terminal_error(&events);
    assert!(error.is_account_limit_exhausted(), "{error:?}");
    assert_eq!(error.resets_at_ms, Some(1_790_021_043_000));
    assert_eq!(server.request_bodies().len(), 1);
    assert!(waits.lock().unwrap().is_empty());
}

/// A cancel while the step waits out a long server retry time stops it promptly, before any
/// re-send. The wait is real time: nothing in it holds up the cancel.
#[tokio::test]
async fn codex_cancel_during_a_long_server_wait_stops_before_resend() {
    for family in CODEX_FAMILIES {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 429,
                headers: vec![("Retry-After", "469")],
                body: CAPACITY_429_BODY,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"must not be sent",
            },
        ])
        .await;
        let provider = family_retry_test_provider(&server, family);
        let cancel = CancellationToken::new();
        let mut rx = provider.start_step(
            StepId(1),
            Arc::new(CallOptions::new(Vec::new())),
            cancel.clone(),
            None,
        );
        let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("the 429 attempt is recorded");
        assert!(
            matches!(
                &first,
                Some(NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }))
                    if attempt.http_status == Some(429)
            ),
            "{first:?}"
        );
        // The record is published before the wait starts; cancel inside the wait.
        let started = Instant::now();
        cancel.cancel();
        assert!(tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("the cancelled wait stops promptly")
            .is_none());
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(server.request_bodies().len(), 1, "{family:?}");
    }
}

/// A headless step must return an actionable transient failure instead of waiting for a
/// connection forever. Inject refusals and retry waits so no socket or real backoff is needed.
#[tokio::test]
async fn codex_connection_failures_end_the_step_after_six_attempts() {
    for family in CODEX_FAMILIES {
        let server = ScriptedServer::start(Vec::new()).await;
        let mut provider = family_retry_test_provider(&server, family);
        provider
            .client
            .fail_connects
            .store(usize::MAX, Ordering::SeqCst);
        let waits = use_virtual_retry_waits(&mut provider);
        let mut rx = provider.start_step(
            StepId(1),
            Arc::new(CallOptions::new(Vec::new())),
            CancellationToken::new(),
            None,
        );
        let events = tokio::time::timeout(Duration::from_secs(5), async {
            let mut events = Vec::new();
            let mut attempts = 0;
            while let Some(event) = rx.recv().await {
                if let NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) =
                    &event
                {
                    assert_eq!(attempt.outcome, AttemptOutcome::ConnectFailed);
                    assert_eq!(attempt.attempt, attempts);
                    attempts += 1;
                    assert!(attempts <= 6, "connection retry bound exceeded: {family:?}");
                }
                events.push(event);
            }
            assert_eq!(attempts, 6);
            events
        })
        .await
        .expect("connection exhaustion must end the step without cancellation");
        let error = terminal_error(&events);
        assert_eq!(error.class, ErrorClass::Transient);
        assert_eq!(error.provider_code.as_deref(), Some("connection_failed"));
        assert!(error.message.contains("connection failed"), "{error:?}");
        assert_eq!(provider.family.render_calls.load(Ordering::SeqCst), 1);
        assert!(server.request_bodies().is_empty(), "no request was sent");
        let digests = attempt_digests(&events);
        assert_eq!(digests.len(), 6);
        assert!(digests.iter().all(|digest| digest == &digests[0]));

        let waits = waits.lock().unwrap();
        assert_eq!(waits.len(), 5);
        for (wait, nominal) in waits[..4].iter().zip([200, 400, 800, 1600]) {
            let millis = wait.as_millis() as u64;
            assert!(millis >= nominal * 9 / 10 && millis < nominal * 11 / 10);
        }
        assert_eq!(waits[4], Duration::from_secs(5));
    }
}

#[tokio::test]
async fn codex_connection_failures_then_success_reset_the_next_step() {
    for family in CODEX_FAMILIES {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            };
            2
        ])
        .await;
        let mut provider = family_retry_test_provider(&server, family);
        let waits = use_virtual_retry_waits(&mut provider);
        for _ in 0..2 {
            provider.client.fail_connects.store(5, Ordering::SeqCst);
            let events = collect_step_events(&provider, CancellationToken::new(), None).await;
            assert!(terminal_error_if_any(&events).is_none(), "{events:?}");
            assert!(events.iter().any(|event| matches!(
                event,
                NormalizedEvent::Control(ProviderControl::Finished {
                    retries_used: 5,
                    ..
                })
            )));
            let outcomes: Vec<_> = events
                .iter()
                .filter_map(|event| match event {
                    NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                        Some(attempt.outcome.clone())
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(
                outcomes,
                [
                    AttemptOutcome::ConnectFailed,
                    AttemptOutcome::ConnectFailed,
                    AttemptOutcome::ConnectFailed,
                    AttemptOutcome::ConnectFailed,
                    AttemptOutcome::ConnectFailed,
                    AttemptOutcome::Complete,
                ]
            );
            let digests = attempt_digests(&events);
            assert_eq!(digests.len(), 6);
            assert!(digests.iter().all(|digest| digest == &digests[0]));
        }
        assert_eq!(provider.family.render_calls.load(Ordering::SeqCst), 2);
        let bodies = server.request_bodies();
        assert_eq!(bodies.len(), 2);
        assert_eq!(bodies[0], bodies[1]);
        let waits = waits.lock().unwrap();
        assert_eq!(waits.len(), 10);
        assert_eq!(waits[4], Duration::from_secs(5));
        assert_eq!(
            waits[9],
            Duration::from_secs(5),
            "backoff resets after success"
        );
    }
}

/// Every family other than the two OpenAI Responses families uses this retry schedule: a
/// `Retry-After` is capped at 120 s, and a 429 without one is still retried on the 2 s
/// doubling backoff.
#[tokio::test]
async fn other_families_keep_the_capped_retry_after_and_retry_a_plain_429() {
    for family in [
        WireFamilyId::OpenAiChat,
        WireFamilyId::AnthropicMessages,
        WireFamilyId::Gemini,
        WireFamilyId::BedrockConverse,
        WireFamilyId::GoogleCodeAssist,
    ] {
        let server = ScriptedServer::start(vec![
            ScriptResponse::Http {
                status: 429,
                headers: vec![("Retry-After", "469")],
                body: br#"{"error":{"message":"rate limited"}}"#,
            },
            ScriptResponse::Http {
                status: 429,
                headers: Vec::new(),
                body: br#"{"error":{"message":"rate limited"}}"#,
            },
            ScriptResponse::Http {
                status: 200,
                headers: Vec::new(),
                body: b"success",
            },
        ])
        .await;
        let mut provider = family_retry_test_provider(&server, family);
        let waits = use_virtual_retry_waits(&mut provider);

        let events = collect_step_events(&provider, CancellationToken::new(), None).await;
        assert!(terminal_error_if_any(&events).is_none(), "{family:?}");
        assert_eq!(server.request_bodies().len(), 3, "{family:?}");
        let waits = waits.lock().unwrap();
        assert_eq!(waits.len(), 2, "{family:?}");
        assert_eq!(waits[0], super::MAX_RETRY_AFTER_DELAY, "{family:?}");
        // The second failed attempt's computed wait: 4 s nominal, scaled by 0.8..=1.2.
        assert!(
            (Duration::from_millis(3_200)..=Duration::from_millis(4_800)).contains(&waits[1]),
            "{family:?} waited {:?}",
            waits[1]
        );
    }
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

fn mark_primary_client(provider: &mut LiveProvider<RetryTestFamily>) {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-test-primary-pool", "yes".parse().unwrap());
    // The escape pool is rebuilt independently, so only primary-pool requests carry this.
    provider.client = super::provider_client_builder()
        .default_headers(headers)
        .build()
        .unwrap()
        .into();
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
    let mut transport_provider = retry_test_provider(&transport_server, 0);
    use_virtual_retry_waits(&mut transport_provider);
    mark_primary_client(&mut transport_provider);
    let events = collect_step_events(&transport_provider, CancellationToken::new(), None).await;
    assert!(terminal_error_if_any(&events).is_none());
    assert_eq!(
        transport_server.request_header("x-test-primary-pool"),
        [Some("yes".into()), None]
    );
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
    let mut http_provider = retry_test_provider(&http_server, 0);
    use_virtual_retry_waits(&mut http_provider);
    mark_primary_client(&mut http_provider);
    let events = collect_step_events(&http_provider, CancellationToken::new(), None).await;
    assert!(terminal_error_if_any(&events).is_none());
    assert_eq!(
        http_server.request_header("x-test-primary-pool"),
        [Some("yes".into()), Some("yes".into())]
    );
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
    let mut provider = retry_test_provider(&server, 0);
    use_virtual_retry_waits(&mut provider);
    mark_primary_client(&mut provider);

    let events = collect_step_events(&provider, CancellationToken::new(), None).await;
    assert!(terminal_error_if_any(&events).is_none());
    assert_eq!(server.request_bodies().len(), 3);
    assert_eq!(
        server.request_header("x-test-primary-pool"),
        [Some("yes".into()), Some("yes".into()), None]
    );
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

/// A body that ends cleanly before the step's terminal record was cut in transit. Every
/// family reports that the same way, as a transport fault, never as a decode failure that
/// earns only the one deterministic-decode retry.
#[tokio::test]
async fn a_body_cut_before_its_terminal_is_a_transport_fault_on_every_family() {
    let sse_cut = |record: &str| format!("data: {record}\n\n").into_bytes();
    let mut bedrock_cut = crate::families::bedrock_converse::test_event_stream_message(
        "messageStart",
        br#"{"role":"assistant"}"#,
    );
    bedrock_cut.extend(
        crate::families::bedrock_converse::test_event_stream_message(
            "contentBlockDelta",
            br#"{"contentBlockIndex":0,"delta":{"text":"partial"}}"#,
        ),
    );
    bedrock_cut.extend(
        crate::families::bedrock_converse::test_event_stream_message(
            "messageStop",
            br#"{"stopReason":"end_turn"}"#,
        ),
    );
    let cases: Vec<(&str, Box<dyn StreamNormalizer>, Vec<u8>)> = vec![
        (
            "openai_chat",
            OpenAiChat.normalizer(&openai_chat_frozen_config("m"), StepId(1)),
            sse_cut(
                r#"{"id":"c","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}"#,
            ),
        ),
        (
            "gemini",
            Gemini.normalizer(&gemini_frozen_config("m"), StepId(1)),
            sse_cut(
                r#"{"candidates":[{"content":{"parts":[{"text":"partial"}],"role":"model"},"index":0}]}"#,
            ),
        ),
        (
            "google_codeassist",
            GoogleCodeAssist.normalizer(
                &frozen_config_for_family(WireFamilyId::GoogleCodeAssist, "m"),
                StepId(1),
            ),
            sse_cut(
                r#"{"response":{"candidates":[{"content":{"parts":[{"text":"partial"}],"role":"model"},"index":0}]}}"#,
            ),
        ),
        (
            "bedrock_converse",
            BedrockConverse.normalizer(
                &frozen_config_for_family(WireFamilyId::BedrockConverse, "m"),
                StepId(1),
            ),
            bedrock_cut,
        ),
    ];
    for (name, normalizer, wire) in cases {
        let stream = futures::stream::iter([Ok::<_, std::io::Error>(wire)]);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let failure = drive_body_stream(stream, normalizer, &tx)
            .await
            .expect_err("a body cut before its terminal must fail the attempt");
        assert_eq!(
            failure.cause,
            Some(super::AttemptCause::TransportTransient),
            "{name}: {}",
            failure.error.message
        );
        assert_eq!(
            failure.outcome,
            AttemptOutcome::EofBeforeCompletion,
            "{name}"
        );
        // The class every transport fault carries, Anthropic's and Responses' cut included.
        assert_eq!(
            failure.error.class,
            ErrorClass::ProviderUnavailable,
            "{name}"
        );
        while let Ok(event) = rx.try_recv() {
            assert!(
                !matches!(
                    event,
                    NormalizedEvent::Control(
                        ProviderControl::AssistantMessage { .. }
                            | ProviderControl::StepFinished { .. }
                            | ProviderControl::Finished { .. }
                    )
                ),
                "{name}"
            );
        }
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
        "message": format!("Incorrect API key provided: sk-private-{}{marker}. You can find your API key at https://platform.openai.com/account/api-keys.", "x".repeat(600)),
        "type": "invalid_request_error",
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
        classify_chatgpt_upstream_key_rejection, classify_http_error, CHATGPT_UPSTREAM_KEY_REJECTED,
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
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: "run-abc".into(),
            step_index: 7,
            max_output_tokens: None,
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
