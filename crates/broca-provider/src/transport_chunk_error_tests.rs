use super::*;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::instrument::WithSubscriber;

#[derive(Clone, Default)]
pub(super) struct ProviderWarnings(pub(super) Arc<Mutex<Vec<BTreeMap<String, String>>>>);

impl tracing::Subscriber for ProviderWarnings {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        struct Fields(BTreeMap<String, String>);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name().into(), format!("{value:?}"));
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0.insert(field.name().into(), value.into());
            }
        }
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}

#[tokio::test]
async fn chunk_error_warn_includes_transport_sources_and_safe_stream_metadata() {
    use crate::families::{openai_responses_chatgpt_frozen_config, OpenAiResponsesChatgpt};
    const SECRET: &str = "PRIVATE-STREAM-CONTENT-UNIQUE-7a";
    let body = r#"event: response.created
data: {"type":"response.created","response":{"id":"r"}}

event: response.reasoning_summary_text.delta
data: {"type":"response.reasoning_summary_text.delta","delta":"hidden reasoning"}

event: response.function_call_arguments.delta
data: {"type":"response.function_call_arguments.delta","output_index":0,"delta":"args"}

event: UNTRUSTED_EVENT_NAME_MARKER with spaces and a deliberately overlong suffix beyond sixty-four bytes
data: {"type":"response.output_text.delta","delta":"overlong event label"}

event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"UNIQUE_MARKER"}

event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"second"}

event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"third"}

: keepalive

: keepalive

: keepalive

event: response.incomplete
data: {"type":"response.incomplete","delta":"cut"#
        .replace("UNIQUE_MARKER", SECRET);
    let received_bytes = body.len();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
        }
        // Delay both headers and body termination: elapsed time must include the header wait.
        tokio::time::sleep(Duration::from_millis(40)).await;
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len() + 100).as_bytes()).await.unwrap();
        socket.write_all(body.as_bytes()).await.unwrap();
        socket.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        // The advertised body remains unfinished, forcing a transport error rather than clean EOF.
        socket.shutdown().await.unwrap();
    });
    let mut frozen = openai_responses_chatgpt_frozen_config("test-model");
    frozen.base_url = Some(format!("http://{address}"));
    let family = OpenAiResponsesChatgpt;
    let request = family
        .endpoint(
            &frozen,
            &AuthHandle::Bearer("PRIVATE-CREDENTIAL".into()),
            Vec::new(),
        )
        .into();
    let normalizer = Box::new(ToolNameRestoringNormalizer {
        live_progress: None,
        inner: family.normalizer(&frozen, StepId(1)),
        names: Arc::new(crate::tool_name_alias::ToolNameMap::build(&[], &[])),
    });
    let (tx, _rx) = mpsc::channel(32);
    let logs = ProviderWarnings::default();
    let mut trace = AttemptTrace::default();
    let clock = broca_core::clock::Clock::default();
    let error = drive_attempt(
        &reqwest::Client::new(),
        &request,
        &clock,
        normalizer,
        &tx,
        Duration::from_secs(2),
        &mut trace,
    )
    .with_subscriber(logs.clone())
    .await
    .expect_err("mid-event disconnect must be chunk_error");
    assert_eq!(error.outcome, AttemptOutcome::ChunkError);
    server.await.unwrap();
    let logs = logs.0.lock().unwrap();
    assert_eq!(
        logs.len(),
        1,
        "one provider WARN per failed attempt: {logs:?}"
    );
    let log = &logs[0];
    assert_eq!(log["message"], "model attempt ended chunk_error");
    assert!(
        log["elapsed_ms"].parse::<u64>().unwrap() >= 70,
        "elapsed time includes waiting for headers: {log:?}"
    );
    assert_eq!(
        log["response_bytes"].parse::<usize>().unwrap(),
        received_bytes
    );
    assert_eq!(
        log["last_event_type"], "response.output_text.delta",
        "incomplete SSE event does not replace the last complete event"
    );
    assert!(
        log["last_content_ms"].parse::<u64>().unwrap() < log["elapsed_ms"].parse::<u64>().unwrap(),
        "content timestamp must precede the cut: {log:?}"
    );
    assert_eq!(log["keepalives_after_content"], "3");
    assert_eq!(log["event_count"], "7");
    assert!(
        log["event_type_counts"].contains("response.output_text.delta=3"),
        "the warning counts text deltas without logging their contents: {log:?}"
    );
    assert!(log["event_type_counts"].contains("response.reasoning_summary_text.delta=1"));
    assert!(log["event_type_counts"].contains("response.function_call_arguments.delta=1"));
    assert!(log["event_type_counts"].contains("other=1"));
    assert!(
        log["error_chain"].contains(" -> "),
        "transport_sources: nested causes must be retained: {log:?}"
    );
    assert!(
        log["error_chain"].contains("end of file before message length reached"),
        "transport_sources: the underlying premature EOF must be visible: {log:?}"
    );
    let rendered = format!("{logs:?}");
    assert!(
        !rendered.contains(SECRET),
        "response content must not enter diagnostic logs"
    );
    assert!(
        !rendered.contains("UNTRUSTED_EVENT_NAME_MARKER"),
        "provider-controlled event names are sanitized before logging"
    );
    assert!(
        !rendered.contains("PRIVATE-CREDENTIAL"),
        "credentials must not enter diagnostic logs"
    );
}

#[tokio::test]
async fn chunk_error_warn_does_not_invent_elapsed_time_without_a_send_stamp() {
    use crate::families::{openai_responses_chatgpt_frozen_config, OpenAiResponsesChatgpt};
    let normalizer = OpenAiResponsesChatgpt.normalizer(
        &openai_responses_chatgpt_frozen_config("test-model"),
        StepId(1),
    );
    let (tx, _rx) = mpsc::channel(8);
    let logs = ProviderWarnings::default();
    let mut trace = AttemptTrace::default();
    let stream = futures::stream::iter([Err::<Vec<u8>, _>(std::io::Error::other("body reset"))]);
    let error = drive_body_stream_traced(stream, normalizer, &tx, MAX_STREAM_BYTES, &mut trace)
        .with_subscriber(logs.clone())
        .await
        .expect_err("the scripted body fails");
    assert_eq!(error.outcome, AttemptOutcome::ChunkError);
    let logs = logs.0.lock().unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(
        logs[0]["last_content_ms"], "none",
        "no decoded content event means no content timestamp"
    );
    assert_eq!(logs[0]["keepalives_after_content"], "0");
    assert_eq!(logs[0]["event_count"], "0");
    assert_eq!(logs[0]["event_type_counts"], "none");
    assert_eq!(
        logs[0]["elapsed_ms"], "unknown",
        "an absent send stamp cannot claim a measured zero milliseconds"
    );
}
