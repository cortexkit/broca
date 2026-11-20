//! A body that fails after the step's last record keeps the step it already closed.

use super::completion::completion_holds_step;
use super::*;
use crate::families::{
    anthropic_frozen_config, openai_chat_frozen_config, AnthropicMessages, OpenAiChat,
};
use serde_json::json;

fn data(value: serde_json::Value) -> Vec<u8> {
    format!("data: {value}\n\n").into_bytes()
}

/// Anthropic's records for a short text answer, `message_stop` last.
fn anthropic_answer() -> Vec<Vec<u8>> {
    vec![
        data(
            json!({"type":"message_start","message":{"id":"m","type":"message","role":"assistant","content":[],"usage":{"input_tokens":10,"output_tokens":1}}}),
        ),
        data(
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        ),
        data(
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}),
        ),
        data(json!({"type":"content_block_stop","index":0})),
        data(
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}),
        ),
        data(json!({"type":"message_stop"})),
    ]
}

async fn drive(
    normalizer: Box<dyn StreamNormalizer>,
    records: Vec<Vec<u8>>,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    let mut chunks: Vec<Result<Vec<u8>, std::io::Error>> = records.into_iter().map(Ok).collect();
    chunks.push(Err(std::io::Error::other("connection reset by the proxy")));
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let mut trace = AttemptTrace::default();
    drive_body_stream_traced(
        futures::stream::iter(chunks),
        normalizer,
        &tx,
        usize::MAX,
        &mut trace,
    )
    .await
}

#[tokio::test]
async fn a_reset_after_message_stop_keeps_the_answer() {
    let normalizer = AnthropicMessages.normalizer(&anthropic_frozen_config("m"), StepId(1));
    let completion = drive(normalizer, anthropic_answer())
        .await
        .expect("the step closed before the reset");
    assert!(completion_holds_step(&completion));
}

#[tokio::test]
async fn a_reset_before_message_stop_still_fails_the_attempt() {
    let normalizer = AnthropicMessages.normalizer(&anthropic_frozen_config("m"), StepId(1));
    let mut records = anthropic_answer();
    records.pop();
    let failure = drive(normalizer, records).await.unwrap_err();
    assert_eq!(failure.outcome, AttemptOutcome::ChunkError);
}

/// Chat names no last record: a reset before the body's clean end may have cut a usage chunk,
/// so the attempt fails as before.
#[tokio::test]
async fn a_chat_reset_after_its_finish_reason_still_fails_the_attempt() {
    let normalizer = OpenAiChat.normalizer(&openai_chat_frozen_config("m"), StepId(1));
    let records = vec![
        data(json!({"id":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"ok"}}]})),
        data(json!({"id":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})),
    ];
    let failure = drive(normalizer, records).await.unwrap_err();
    assert_eq!(failure.outcome, AttemptOutcome::ChunkError);
}
