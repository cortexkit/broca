use super::chunk_error_tests::ProviderWarnings;
use super::*;
use crate::families::{frozen_config_for_family, openai_responses_frozen_config, OpenAiResponses};
use crate::spec::WireFamilyId;
use crate::wire::HttpMethod;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::instrument::WithSubscriber;

const MARKER: &str = "PRIVATE-RETENTION-PROVIDER-CONTENT-91c";

fn options(ephemeral_session: bool) -> CallOptions {
    let mut opts = CallOptions::new(vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: MARKER.into(),
        }],
    )]);
    opts.ephemeral_session = ephemeral_session;
    opts.step_context = Some(broca_protocol::StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "retention-diagnostic-test".into(),
        step_index: 1,
        max_output_tokens: None,
    });
    opts
}

#[test]
fn retention_flag_does_not_change_any_family_rendered_body() {
    let forever = options(false);
    let ephemeral = options(true);
    for id in [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::Gemini,
        WireFamilyId::GoogleCodeAssist,
        WireFamilyId::BedrockConverse,
    ] {
        let mut frozen = frozen_config_for_family(id, "test-model");
        frozen.project_id = Some("test-project".into());
        let family = crate::registry::wire_family(id).unwrap();
        assert_eq!(
            family.render(&frozen, &forever),
            family.render(&frozen, &ephemeral),
            "{id:?}"
        );
    }
}

#[test]
fn retention_request_dumps_are_suppressed_before_any_write() {
    // The dump directory is cached once per process. A child supplies the real environment
    // switch without racing other tests or allowing their renders to create control files.
    const CHILD: &str = "BROCA_TEST_RETENTION_DUMP_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let dir = broca_test_support::scratch_root().join("retention-request-dumps");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "transport::retention_tests::retention_request_dumps_are_suppressed_before_any_write", "--nocapture"])
            .env(CHILD, "1")
            .env("BROCA_DUMP_REQUEST_DIR", &dir)
            .output().unwrap();
        assert!(
            output.status.success(),
            "dump child failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return;
    }
    let dir = PathBuf::from(std::env::var_os("BROCA_DUMP_REQUEST_DIR").unwrap());
    let render = |ephemeral_session| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let rendered = runtime.block_on(async {
            tokio::task::spawn_blocking(move || {
                RenderJob {
                    subscription_auth: false,
                    #[cfg(feature = "test-attachments")]
                    media_renderer: None,
                    #[cfg(feature = "test-attachments")]
                    media_render_tags: false,
                    auth_revision: 0,
                    family: Arc::new(OpenAiResponses),
                    frozen: Arc::new(openai_responses_frozen_config("test-model")),
                    auth: Arc::new(AuthHandle::None),
                    opts: Arc::new(options(ephemeral_session)),
                    step_id: StepId(1),
                }
                .render()
                .unwrap()
            })
            .await
            .unwrap()
        });
        // Runtime shutdown joins the dump's blocking writer; assertions cannot race a write.
        drop(runtime);
        rendered
    };
    let ephemeral = render(true);
    assert!(ephemeral.request.ephemeral_session);
    assert!(
        !dir.exists(),
        "a retention request must not even create the dump directory"
    );
    assert!(
        DUMP_REQUEST_DIR.get().is_none(),
        "suppression precedes the dump environment lookup"
    );
    let forever = render(false);
    assert!(!forever.request.ephemeral_session);
    assert_eq!(ephemeral.request.body, forever.request.body);
    let paths: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(
        paths.len(),
        1,
        "the forever request is the positive dump control"
    );
    assert_eq!(std::fs::read(&paths[0]).unwrap(), forever.request.body);
}

async fn http_script(bodies: Vec<String>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        for body in bodies {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                header.push(socket.read_u8().await.unwrap());
            }
            let header = String::from_utf8(header).unwrap();
            let length = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(|value| value.parse::<usize>().unwrap())
                })
                .unwrap();
            let mut request = vec![0; length];
            socket.read_exact(&mut request).await.unwrap();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        }
    });
    (url, task)
}

fn error_body(code: &str) -> String {
    format!(
        "data: {}\n\n",
        serde_json::json!({
            "type": "response.failed", "response": {"error": {
                "code": code, "type": MARKER, "message": MARKER
            }}
        })
    )
}

#[tokio::test]
async fn retention_https_calls_private_diagnostics_on_every_retry() {
    let (url, server) = http_script(vec![error_body(MARKER), error_body("invalid_prompt")]).await;
    let family = OpenAiResponses;
    let mut frozen = openai_responses_frozen_config("test-model");
    frozen.base_url = Some(url);
    let rendered = RenderJob {
        subscription_auth: false,
        #[cfg(feature = "test-attachments")]
        media_renderer: None,
        #[cfg(feature = "test-attachments")]
        media_render_tags: false,
        auth_revision: 0,
        family: Arc::new(OpenAiResponses),
        frozen: Arc::new(frozen.clone()),
        auth: Arc::new(AuthHandle::None),
        opts: Arc::new(options(true)),
        step_id: StepId(1),
    }
    .render()
    .unwrap();
    let (tx, _rx) = mpsc::channel(256);
    let cancel = CancellationToken::new();
    let logs = ProviderWarnings::default();
    let clock = RetryClock {
        virtual_waits: Some(Arc::new(std::sync::Mutex::new(Vec::new()))),
        ..Default::default()
    };
    let provider_clock = broca_core::clock::Clock::default();
    let failure = drive_step_attempts(
        &build_provider_http_client(),
        &provider_clock,
        rendered.request,
        &rendered.request_digest,
        None,
        None,
        || build_retry_normalizer(&family, &frozen, StepId(1), &rendered.name_map),
        &tx,
        &cancel,
        None,
        StepSeal {
            seal: None,
            step_id: StepId(1),
            cache_ttl_ms: None,
            first_attempt: 0,
            route: broca_core::refusal_fallback::StepRoute::Source,
        },
        &clock,
        None,
        &AtomicUsize::new(0),
    )
    .with_subscriber(logs.clone())
    .await
    .unwrap_err();
    server.await.unwrap();
    assert_eq!(failure.1, 1);
    let logs = logs.0.lock().unwrap();
    assert_eq!(
        logs.len(),
        2,
        "each attempt must execute the normalizer's error path"
    );
    assert!(
        !format!("{logs:?}").contains(MARKER),
        "backend codes, types and messages must stay out of logs"
    );
    for log in logs.iter() {
        assert!(!log.contains_key("code"));
        assert!(!log.contains_key("error_type"));
        assert!(!log.contains_key("event_type"));
    }
}

#[tokio::test]
async fn forever_https_keeps_provider_error_diagnostics_unchanged() {
    let (url, server) = http_script(vec![error_body("invalid_prompt")]).await;
    let family = OpenAiResponses;
    let frozen = openai_responses_frozen_config("test-model");
    let request: PreparedHttpRequest = crate::wire::HttpRequest {
        method: HttpMethod::Post,
        url,
        headers: Vec::new(),
        body: b"{}".to_vec(),
    }
    .into();
    let (tx, _rx) = mpsc::channel(8);
    let logs = ProviderWarnings::default();
    let provider_clock = broca_core::clock::Clock::default();
    let failure = drive_attempt(
        &build_provider_http_client(),
        &request,
        &provider_clock,
        family.normalizer(&frozen, StepId(1)),
        &tx,
        Duration::from_secs(2),
        &mut AttemptTrace::default(),
    )
    .with_subscriber(logs.clone())
    .await
    .unwrap_err();
    server.await.unwrap();
    assert_eq!(failure.error.message, MARKER);
    let logs = logs.0.lock().unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0]["code"], "invalid_prompt");
    assert_eq!(logs[0]["error_type"], MARKER);
    assert_eq!(logs[0]["message"], MARKER);
    assert_eq!(logs[0]["event_type"], "response.failed");
    assert!(format!("{logs:?}").contains(MARKER));
}

#[tokio::test]
async fn retention_chunk_error_logs_only_safe_error_metadata() {
    let stream = futures::stream::iter([Err::<Vec<u8>, _>(std::io::Error::other(MARKER))]);
    let family = OpenAiResponses;
    let frozen = openai_responses_frozen_config("test-model");
    let (tx, _rx) = mpsc::channel(8);
    let logs = ProviderWarnings::default();
    let mut trace = AttemptTrace {
        ephemeral_session: true,
        http_status: Some(200),
        ..Default::default()
    };
    let failure = drive_body_stream_traced(
        stream,
        family.normalizer(&frozen, StepId(1)),
        &tx,
        MAX_STREAM_BYTES,
        &mut trace,
    )
    .with_subscriber(logs.clone())
    .await
    .unwrap_err();
    assert_eq!(failure.outcome, AttemptOutcome::ChunkError);
    let logs = logs.0.lock().unwrap();
    assert_eq!(logs.len(), 1);
    let keys: Vec<_> = logs[0].keys().map(String::as_str).collect();
    assert_eq!(keys, ["class", "message", "provider_code"]);
    assert_eq!(logs[0]["provider_code"], "none");
    assert!(!format!("{logs:?}").contains(MARKER));
}

#[test]
fn retention_error_log_filters_backend_codes_and_preserves_status() {
    for code in [MARKER, "rate_limit_exceeded"] {
        let mut error = transport_error(MARKER);
        error.status = Some(429);
        error.provider_code = Some(code.into());
        let capture = broca_test_support::LogCapture::new();
        capture.in_scope(|| warn_private_failure(&error, "provider failure"));
        let logs: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|event| event.level == tracing::Level::WARN)
            .map(|event| event.fields)
            .collect();
        assert_eq!(logs.len(), 1);
        let keys: Vec<_> = logs[0].keys().map(String::as_str).collect();
        assert_eq!(keys, ["class", "message", "provider_code", "status"]);
        assert_eq!(logs[0]["status"], "429");
        assert_eq!(
            logs[0]["provider_code"],
            if code == MARKER { "none" } else { code }
        );
        assert!(!format!("{logs:?}").contains(MARKER));
    }
}
