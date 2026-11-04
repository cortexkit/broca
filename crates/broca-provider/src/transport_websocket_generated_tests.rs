use super::*;
use crate::wire::{NormalizeError, WireFamily};
use proptest::prelude::*;
use serde_json::json;

mod support {
    include!("../tests/generated_support/mod.rs");
}

fn normalize_framed(raw: &[u8], sizes: &[usize]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
    let frozen = crate::families::openai_responses_chatgpt_frozen_config("generated-test-model");
    let mut normalizer = crate::families::OpenAiResponsesChatgpt.normalizer(&frozen, StepId(1));
    let mut events = Vec::new();
    for chunk in support::chunks(raw, sizes) {
        events.extend(normalizer.push(chunk)?);
    }
    events.extend(normalizer.finish()?);
    Ok(events)
}

#[test]
fn websocket_frame_adapter_chunk_boundaries() {
    support::guarded(|| {
        let recorded = include_str!("../tests/fixtures/codex_reasoning_turn_response.sse");
        let messages = crate::sse::parse_data_events(recorded).unwrap();
        let expected = normalize_framed(recorded.as_bytes(), &[recorded.len()]).unwrap();
        assert!(expected.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Finished { .. })
        )));
        support::runner()
            .run(&proptest::collection::vec(1usize..257, 1..33), |sizes| {
                let mut framed = responses_websocket::frame_record(&[]);
                for message in &messages {
                    framed.extend(responses_websocket::frame_record(message.as_bytes()));
                }
                // Tungstenite delivers whole text messages; only the adapted SSE bytes may be split.
                prop_assert_eq!(&normalize_framed(&framed, &sizes).unwrap(), &expected);
                prop_assert_eq!(&normalize_framed(&framed, &[1]).unwrap(), &expected);
                Ok(())
            })
            .unwrap();
    });
}

fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|value| json!(value)),
        any::<String>().prop_map(Value::String)
    ];
    leaf.prop_recursive(3, 48, 6, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
            proptest::collection::btree_map(any::<String>(), inner, 0..6)
                .prop_map(|values| Value::Object(values.into_iter().collect())),
        ]
    })
}

fn check_socket_record(raw: &[u8], sizes: &[usize]) {
    let _ = responses_websocket::wrapped_error(raw, &mut AttemptTrace::default());
    let _: Result<Vec<NormalizedEvent>, NormalizeError> =
        normalize_framed(&responses_websocket::frame_record(raw), sizes);
}

#[test]
fn websocket_frame_adapter_bad_input() {
    support::guarded(|| {
        let messages = crate::sse::parse_data_events(include_str!(
            "../tests/fixtures/codex_reasoning_turn_response.sse"
        ))
        .unwrap();
        support::runner().run(&(
            proptest::collection::vec(any::<u8>(), 0..2049), json_value(),
            any::<usize>(), 1usize..33,
            proptest::collection::vec(1usize..257, 1..17),
        ), |(bytes, value, at, count, sizes)| {
            check_socket_record(&bytes, &sizes);
            let pretty = serde_json::to_vec_pretty(&value).unwrap();
            check_socket_record(&pretty, &sizes);
            let mut sse = crate::sse::SseDecoder::default();
            let decoded = sse.push(&responses_websocket::frame_record(&pretty)).unwrap();
            prop_assert_eq!(decoded.len(), 1);
            prop_assert_eq!(&serde_json::from_str::<Value>(&decoded[0]).unwrap(), &value);
            let error = json!({"type":"error","status":at % 700,"headers":value,"error":{"code":value,"headers":value}});
            check_socket_record(&serde_json::to_vec(&error).unwrap(), &sizes);
            let record = &messages[at % messages.len()];
            for damaged in support::damaged(record.as_bytes(), at, count, &bytes[..bytes.len().min(32)]) {
                check_socket_record(&damaged, &sizes);
            }
            Ok(())
        }).unwrap();
        check_socket_record(b"\xff", &[1]);
        let oversized = format!(
            "{{\"padding\":\"{}\"}}",
            "x".repeat(crate::sse::MAX_SSE_RECORD_BYTES)
        );
        assert!(matches!(
            normalize_framed(
                &responses_websocket::frame_record(oversized.as_bytes()),
                &[65536]
            ),
            Err(NormalizeError::StreamLimit(_))
        ));
    });
}

#[test]
fn chatgpt_wire_generated_request_and_identity_round_trips() {
    support::guarded(|| {
        // chatgpt_wire encodes requests and frozen identity, not inbound stream records.
        // The request encoder reorders object fields; non-objects are not requests.
        let identity: crate::chatgpt_wire::ChatgptWire = serde_json::from_str(include_str!(
            "../tests/fixtures/chatgpt_codex_0_163_identity.json"
        ))
        .unwrap();
        support::runner()
            .run(
                &(
                    proptest::collection::btree_map(any::<String>(), json_value(), 0..8),
                    any::<String>(),
                    proptest::collection::vec(any::<u8>(), 0..2049),
                ),
                |(fields, text, bytes)| {
                    let mut body = Value::Object(fields.into_iter().collect());
                    body["model"] = json!(text);
                    body["input"] = json!([{"type":"message","role":"user","content":text}]);
                    let encoded = crate::chatgpt_wire::request_bytes(&body);
                    prop_assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), body);
                    let mut changed = identity.clone();
                    changed.thread_id = text.clone();
                    changed.session_id = text;
                    prop_assert_eq!(
                        serde_json::from_slice::<crate::chatgpt_wire::ChatgptWire>(
                            &serde_json::to_vec(&changed).unwrap()
                        )
                        .unwrap(),
                        changed
                    );
                    let _: Result<crate::chatgpt_wire::ChatgptWire, serde_json::Error> =
                        serde_json::from_slice(&bytes);
                    Ok(())
                },
            )
            .unwrap();
    });
}
