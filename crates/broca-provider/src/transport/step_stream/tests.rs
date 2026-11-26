use super::*;
use crate::families::{openai_chat_frozen_config, OpenAiChat};
use crate::spec::WireFamilyId;
use broca_protocol::provider::{InFlightStep, RunStatusStepPhase};

fn sse(payload: &str) -> Vec<u8> {
    format!("data: {payload}\n\n").into_bytes()
}

async fn assert_bookkeeping_stalls(family: WireFamilyId, frame: Vec<u8>, websocket: bool) {
    let normalizer = crate::registry::wire_family(family).unwrap().normalizer(
        &crate::families::frozen_config_for_family(family, "m"),
        StepId(1),
    );
    let mut trace = AttemptTrace {
        transport: websocket.then(|| "websocket".into()),
        ..AttemptTrace::default()
    };
    let stream = futures::stream::unfold(frame, |frame| async {
        tokio::time::sleep(Duration::from_secs(150)).await;
        Some((Ok::<_, std::io::Error>(frame.clone()), frame))
    });
    let (tx, _rx) = mpsc::channel(64);
    let started = tokio::time::Instant::now();
    let failure = tokio::time::timeout(
        Duration::from_secs(1801),
        drive_body_stream_traced(stream, normalizer, &tx, MAX_STREAM_BYTES, &mut trace),
    )
    .await
    .expect("bookkeeping alone must not extend the content deadline")
    .unwrap_err();
    assert_eq!(failure.outcome, AttemptOutcome::ContentTimeout);
    assert_eq!(started.elapsed(), Duration::from_secs(1800));
}

macro_rules! bookkeeping_stall_case {
    ($name:ident, $family:ident, $payload:expr) => {
        #[tokio::test(start_paused = true)]
        async fn $name() {
            assert_bookkeeping_stalls(WireFamilyId::$family, sse($payload), false).await;
        }
    };
}

bookkeeping_stall_case!(
    step_liveness_anthropic_ping_is_not_content,
    AnthropicMessages,
    r#"{"type":"ping"}"#
);
bookkeeping_stall_case!(
    step_liveness_chat_empty_delta_is_not_content,
    OpenAiChat,
    r#"{"id":"m","choices":[{"index":0,"delta":{"content":""}}]}"#
);
bookkeeping_stall_case!(
    step_liveness_responses_in_progress_is_not_content,
    OpenAiResponses,
    r#"{"type":"response.in_progress","response":{"id":"m"}}"#
);
bookkeeping_stall_case!(
    step_liveness_gemini_usage_is_not_content,
    Gemini,
    r#"{"usageMetadata":{"promptTokenCount":1}}"#
);
bookkeeping_stall_case!(
    step_liveness_codeassist_usage_is_not_content,
    GoogleCodeAssist,
    r#"{"response":{"usageMetadata":{"promptTokenCount":1}}}"#
);

#[tokio::test(start_paused = true)]
async fn step_liveness_chatgpt_socket_frames_are_not_all_content() {
    assert_bookkeeping_stalls(
        WireFamilyId::OpenAiResponsesChatgpt,
        br#"{"type":"response.in_progress","response":{"id":"m"}}"#.to_vec(),
        true,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn step_liveness_bedrock_metadata_is_liveness_but_not_content() {
    assert_bookkeeping_stalls(
        WireFamilyId::BedrockConverse,
        crate::families::bedrock_converse::test_event_stream_message("metadata", b"{}"),
        false,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn step_liveness_reasoning_keeps_a_stream_alive_past_the_first_content_deadline() {
    let normalizer = OpenAiChat.normalizer(&openai_chat_frozen_config("m"), StepId(1));
    let frames = futures::stream::iter(0..15).then(|_| async {
        tokio::time::sleep(Duration::from_secs(150)).await;
        Ok::<_, std::io::Error>(sse(r#"{"id":"m","choices":[{"index":0,"delta":{"reasoning_content":"more reasoning"}}]}"#))
    }).chain(futures::stream::iter([
        Ok(sse(r#"{"id":"m","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}]}"#)),
        Ok(sse("[DONE]")),
    ]));
    let (tx, _rx) = mpsc::channel(64);
    let started = tokio::time::Instant::now();
    let completion = drive_body_stream_traced(
        frames,
        normalizer,
        &tx,
        MAX_STREAM_BYTES,
        &mut AttemptTrace::default(),
    )
    .await
    .unwrap();
    assert!(started.elapsed() > Duration::from_secs(1800));
    assert!(completion.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Finished { .. })
    )));
}

#[tokio::test(start_paused = true)]
async fn step_liveness_reported_idle_deadline_fails_the_attempt() {
    let progress = Arc::new(std::sync::Mutex::new(Some(InFlightStep {
        retry_awaits: None,
        report_awaits: true,
        step_id: StepId(1),
        started_at_ms: 1,
        attempt: 0,
        attempt_started_at_ms: 1,
        liveness: Default::default(),
        last_content_at_ms: None,
        max_content_gap_ms: None,
        max_content_gap_ended_at_ms: None,
        request: Default::default(),
    })));
    let (tx, _rx) = mpsc::channel(64);
    let mut trace = AttemptTrace {
        live_progress: Some(progress.clone()),
        ..AttemptTrace::default()
    };
    let normalizer = OpenAiChat.normalizer(&openai_chat_frozen_config("m"), StepId(1));
    let stream = futures::stream::pending::<Result<Vec<u8>, std::io::Error>>();
    let mut attempt = Box::pin(drive_body_stream_traced(
        stream,
        normalizer,
        &tx,
        MAX_STREAM_BYTES,
        &mut trace,
    ));
    assert!(futures::poll!(&mut attempt).is_pending());
    let until_idle = {
        let snapshot = progress.lock().unwrap();
        let step = snapshot.as_ref().unwrap();
        assert_eq!(
            step.liveness.phase,
            Some(RunStatusStepPhase::AwaitingResponse)
        );
        assert!(step.liveness.last_event_at_ms.is_none());
        let until_idle = step.liveness.idle_deadline_ms.unwrap() - step.attempt_started_at_ms;
        assert_eq!(until_idle, 300_000);
        assert_eq!(
            step.liveness.content_deadline_ms.unwrap() - step.attempt_started_at_ms,
            1_800_000
        );
        Duration::from_millis(until_idle)
    };
    tokio::time::advance(until_idle - Duration::from_millis(1)).await;
    assert!(futures::poll!(&mut attempt).is_pending());
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(
        attempt.await.unwrap_err().outcome,
        AttemptOutcome::IdleTimeout
    );
    let snapshot = progress.lock().unwrap();
    assert!(snapshot
        .as_ref()
        .unwrap()
        .liveness
        .idle_deadline_ms
        .is_none());
}
