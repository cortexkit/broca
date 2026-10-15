use super::*;
use crate::families::{
    anthropic_frozen_config, bedrock_converse::test_event_stream_message, bedrock_frozen_config,
    openai_chat_frozen_config, openai_responses_chatgpt_frozen_config,
    openai_responses_frozen_config, AnthropicMessages, BedrockConverse, OpenAiChat,
    OpenAiResponses, OpenAiResponsesChatgpt,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug)]
enum Family {
    Anthropic,
    Chat,
    Responses,
    Chatgpt,
    Bedrock,
}

impl Family {
    fn normalizer(self) -> Box<dyn StreamNormalizer> {
        let step = StepId(1);
        match self {
            Self::Anthropic => AnthropicMessages.normalizer(&anthropic_frozen_config("m"), step),
            Self::Chat => OpenAiChat.normalizer(&openai_chat_frozen_config("m"), step),
            Self::Responses => {
                OpenAiResponses.normalizer(&openai_responses_frozen_config("m"), step)
            }
            Self::Chatgpt => OpenAiResponsesChatgpt
                .normalizer(&openai_responses_chatgpt_frozen_config("m"), step),
            Self::Bedrock => BedrockConverse.normalizer(&bedrock_frozen_config("m"), step),
        }
    }

    fn record(self, kind: &str, value: Value, websocket: bool) -> Vec<u8> {
        if matches!(self, Self::Bedrock) {
            test_event_stream_message(kind, &serde_json::to_vec(&value).unwrap())
        } else if websocket {
            serde_json::to_vec(&value).unwrap()
        } else {
            format!("data: {value}\n\n").into_bytes()
        }
    }

    fn start(self, websocket: bool) -> Vec<Vec<u8>> {
        let (kind, start, call_kind, call) = match self {
            Self::Anthropic => ("message_start", json!({"type":"message_start","message":{"id":"m"}}),
                "content_block_start", json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"bad","name":"lookup","input":{}}})),
            Self::Chat => return vec![self.record("", json!({"id":"m","choices":[{"delta":{"tool_calls":[{"index":0,"id":"bad","function":{"name":"lookup","arguments":""}}]}}]}), websocket)],
            Self::Responses | Self::Chatgpt => ("response.created", json!({"type":"response.created","response":{"id":"m"}}),
                "response.output_item.added", json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"bad","name":"lookup","arguments":""}})),
            Self::Bedrock => ("messageStart", json!({"role":"assistant"}), "contentBlockStart",
                json!({"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"bad","name":"lookup"}}})),
        };
        vec![
            self.record(kind, start, websocket),
            self.record(call_kind, call, websocket),
        ]
    }

    fn delta(self, arguments: &str, websocket: bool) -> Vec<u8> {
        let (kind, value) = match self {
            Self::Anthropic => (
                "content_block_delta",
                json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":arguments}}),
            ),
            Self::Chat => (
                "",
                json!({"id":"m","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":arguments}}]}}]}),
            ),
            Self::Responses | Self::Chatgpt => (
                "response.function_call_arguments.delta",
                json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":arguments}),
            ),
            Self::Bedrock => (
                "contentBlockDelta",
                json!({"contentBlockIndex":0,"delta":{"toolUse":{"input":arguments}}}),
            ),
        };
        self.record(kind, value, websocket)
    }
}

const FAMILIES: [Family; 5] = [
    Family::Anthropic,
    Family::Chat,
    Family::Responses,
    Family::Chatgpt,
    Family::Bedrock,
];

#[test]
fn wire_normalizers_report_real_content_progress() {
    for family in FAMILIES {
        let mut normalizer = family.normalizer();
        for chunk in family.start(false) {
            normalizer.push(&chunk).unwrap();
        }
        assert_eq!(
            normalizer.content_progress().content_bytes,
            None,
            "{family:?}"
        );
        normalizer.push(&family.delta("{}", false)).unwrap();
        let progress = normalizer.content_progress();
        assert!(progress.content_bytes.is_some_and(|n| n > 0), "{family:?}");
        assert!(progress.last_content_at_ms.is_some(), "{family:?}");
    }
}

#[tokio::test]
async fn metadata_only_attempt_omits_content_evidence_and_log_time() {
    use tracing::instrument::WithSubscriber;
    let normalizer = OpenAiResponses.normalizer(&openai_responses_frozen_config("m"), StepId(1));
    let raw = b": keepalive\n\ndata: {\"type\":\"response.in_progress\"}\n\n";
    let stream = futures::stream::iter([
        Ok(raw.to_vec()),
        Err(std::io::Error::other("held stream cut")),
    ]);
    let (tx, _rx) = mpsc::channel(32);
    let logs = super::chunk_error_tests::ProviderWarnings::default();
    let mut trace = AttemptTrace {
        sent_at: Some(std::time::Instant::now()),
        ..Default::default()
    };
    drive_body_stream_traced(stream, normalizer, &tx, MAX_STREAM_BYTES, &mut trace)
        .with_subscriber(logs.clone())
        .await
        .unwrap_err();
    assert_eq!(logs.0.lock().unwrap()[0]["last_content_ms"], "none");
    let attempt = trace.into_attempt(0, &RequestDigest::of(b"{}"), AttemptOutcome::ChunkError);
    let value = serde_json::to_value(attempt).unwrap();
    assert!(value.get("first_content_at_ms").is_none());
    assert!(value.get("last_content_at_ms").is_none());
    assert!(value.get("content_bytes").is_none());
    assert!(value["non_content_bytes"].as_u64().is_some_and(|n| n > 0));
    assert_eq!(value["event_types"].as_array().unwrap().len(), 2);
}

fn cut(events: &[NormalizedEvent]) -> &broca_protocol::provider::ToolArgsTrailingGarbage {
    events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::ToolArgsTrailingGarbage { cut }) => Some(cut),
            _ => None,
        })
        .expect("the guard must reject the call")
}

#[tokio::test]
async fn stream_detector_cuts_each_family_before_terminal_and_further_reads() {
    for family in FAMILIES {
        for websocket in [false, true] {
            if websocket && !matches!(family, Family::Responses | Family::Chatgpt) {
                continue;
            }
            let mut chunks = family.start(websocket);
            chunks.push(family.delta(r#"{"key":"alpha"}"#, websocket));
            chunks.push(family.delta(&" ".repeat(4096), websocket));
            chunks.push(family.delta(" ", websocket));
            let expected_reads = chunks.len();
            // A poison record proves the body driver stops polling, not just suppressing output.
            chunks.push(b"unread poison: must not read after the whitespace cut".to_vec());
            let reads = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&reads);
            let stream = futures::stream::iter(chunks.into_iter().map(move |chunk| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::io::Error>(chunk)
            }));
            let (tx, _rx) = mpsc::channel(64);
            let mut trace = AttemptTrace::default();
            if websocket {
                trace.transport = Some("websocket".into());
            }
            let events = drive_body_stream_traced(
                stream,
                family.normalizer(),
                &tx,
                MAX_STREAM_BYTES,
                &mut trace,
            )
            .await
            .unwrap_or_else(|error| panic!("{family:?} websocket={websocket}: {error:?}"));
            assert_eq!(reads.load(Ordering::SeqCst), expected_reads, "{family:?}");
            let rejection = cut(&events);
            assert_eq!(rejection.json_bytes, 15);
            assert_eq!(rejection.whitespace_bytes, 4097);
            assert_eq!(rejection.tool_call_id.0, "bad");
            assert!(events.iter().any(|event| matches!(
                event,
                NormalizedEvent::Control(ProviderControl::Finished { .. })
            )));
            let message = events
                .iter()
                .find_map(|event| match event {
                    NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                        Some(message)
                    }
                    _ => None,
                })
                .unwrap();
            assert_eq!(
                message.tool_calls().next().unwrap().input,
                json!({"key":"alpha"})
            );
        }
    }
}

#[test]
fn long_string_whitespace_and_small_trailing_runs_do_not_cut_any_family() {
    for family in FAMILIES {
        for prefix in [r#"{"key":"alpha"}"#, r#"{"key":"unterminated"#] {
            let mut normalizer = family.normalizer();
            for chunk in family.start(false) {
                normalizer.push(&chunk).unwrap();
            }
            normalizer.push(&family.delta(prefix, false)).unwrap();
            let size = if prefix.ends_with('}') { 4096 } else { 9000 };
            let events = normalizer
                .push(&family.delta(&" ".repeat(size), false))
                .unwrap();
            assert!(!tool_args_cut(&events), "{family:?} {prefix}");
        }
    }
}

#[test]
fn terminal_backstop_rejects_done_only_responses_arguments() {
    for family in [Family::Responses, Family::Chatgpt] {
        let mut normalizer = family.normalizer();
        for chunk in family.start(false) {
            normalizer.push(&chunk).unwrap();
        }
        let args = format!("{{\"key\":\"alpha\"}}{}", " ".repeat(4097));
        let events = normalizer.push(&family.record("", json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":args}), false)).unwrap();
        assert_eq!(cut(&events).whitespace_bytes, 4097);
        assert_eq!(cut(&events).json_bytes, 15);
    }
}

fn other_call(family: Family, index: u64, id: &str, args: &str) -> Vec<Vec<u8>> {
    let (kind, start, delta_kind, delta) = match family {
        Family::Anthropic => ("", json!({"type":"content_block_start","index":index,"content_block":{"type":"tool_use","id":id,"name":"lookup","input":{}}}),
            "", json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":args}})),
        Family::Chat => return vec![family.record("", json!({"id":"m","choices":[{"delta":{"tool_calls":[{"index":index,"id":id,"function":{"name":"lookup","arguments":args}}]}}]}), false)],
        Family::Responses | Family::Chatgpt => return vec![family.record("", json!({"type":"response.output_item.added","output_index":index,"item":{"type":"function_call","call_id":id,"name":"lookup","arguments":args}}), false)],
        Family::Bedrock => ("contentBlockStart", json!({"contentBlockIndex":index,"start":{"toolUse":{"toolUseId":id,"name":"lookup"}}}),
            "contentBlockDelta", json!({"contentBlockIndex":index,"delta":{"toolUse":{"input":args}}})),
    };
    vec![
        family.record(kind, start, false),
        family.record(delta_kind, delta, false),
    ]
}

#[test]
fn cut_omits_incomplete_calls_and_preserves_completed_text_and_calls() {
    for family in FAMILIES {
        let mut normalizer = family.normalizer();
        for chunk in family.start(false) {
            normalizer.push(&chunk).unwrap();
        }
        for chunk in other_call(family, 1, "unfinished", "{")
            .into_iter()
            .chain(other_call(family, 2, "good", r#"{"key":"beta"}"#))
        {
            normalizer.push(&chunk).unwrap();
        }
        let text = match family {
            Family::Anthropic => vec![
                family.record("", json!({"type":"content_block_start","index":3,"content_block":{"type":"text","text":"completed text"}}), false),
                family.record("", json!({"type":"content_block_stop","index":3}), false),
                family.record("", json!({"type":"content_block_start","index":4,"content_block":{"type":"text","text":"unfinished text"}}), false),
            ],
            Family::Chat => vec![family.record("", json!({"id":"m","choices":[{"delta":{"content":"completed text"}}]}), false)],
            Family::Responses | Family::Chatgpt => vec![
                family.record("", json!({"type":"response.output_text.done","output_index":3,"content_index":0,"text":"completed text"}), false),
                family.record("", json!({"type":"response.output_text.delta","output_index":4,"content_index":0,"delta":"unfinished text"}), false),
            ],
            Family::Bedrock => vec![
                family.record("contentBlockDelta", json!({"contentBlockIndex":3,"delta":{"text":"completed text"}}), false),
                family.record("contentBlockStop", json!({"contentBlockIndex":3}), false),
                family.record("contentBlockDelta", json!({"contentBlockIndex":4,"delta":{"text":"unfinished text"}}), false),
            ],
        };
        for chunk in text {
            normalizer.push(&chunk).unwrap();
        }
        normalizer
            .push(&family.delta(r#"{"key":"alpha"}"#, false))
            .unwrap();
        let events = normalizer
            .push(&family.delta(&" ".repeat(4097), false))
            .unwrap();
        assert_eq!(cut(&events).omitted_tool_calls, 1, "{family:?}");
        let message = events
            .iter()
            .find_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    Some(message)
                }
                _ => None,
            })
            .unwrap();
        let calls: Vec<_> = message
            .tool_calls()
            .map(|call| call.tool_call_id.0.as_str())
            .collect();
        assert_eq!(calls, ["bad", "good"], "{family:?}");
        assert!(message.content.iter().any(|block| matches!(block, broca_protocol::ContentBlock::Text { text } if text == "completed text")));
        assert!(!message.content.iter().any(|block| matches!(block, broca_protocol::ContentBlock::Text { text } if text == "unfinished text")));
    }
}
