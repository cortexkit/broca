use broca_protocol::{provider::ProviderControl, StepId};
use broca_provider::{families::*, spec::WireFamilyId, sse::SseDecoder, wire::*, NormalizedEvent};
use proptest::prelude::*;
use serde_json::{json, Value};

#[path = "../generated_support/mod.rs"]
mod support;

fn family(id: WireFamilyId) -> &'static dyn WireFamily {
    match id {
        WireFamilyId::AnthropicMessages => &AnthropicMessages,
        WireFamilyId::OpenAiChat => &OpenAiChat,
        WireFamilyId::OpenAiResponses => &OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt => &OpenAiResponsesChatgpt,
        WireFamilyId::Gemini => &Gemini,
        WireFamilyId::GoogleCodeAssist => &GoogleCodeAssist,
        WireFamilyId::BedrockConverse => &BedrockConverse,
    }
}

fn recorded(id: WireFamilyId) -> Vec<u8> {
    match id {
        WireFamilyId::AnthropicMessages => {
            include_bytes!("../fixtures/anthropic-tool-call.sse").to_vec()
        }
        WireFamilyId::OpenAiChat => {
            include_bytes!("../fixtures/openai_chat_reasoning_content.sse").to_vec()
        }
        WireFamilyId::OpenAiResponses => {
            include_bytes!("../fixtures/live_responses_prefix.sse").to_vec()
        }
        WireFamilyId::OpenAiResponsesChatgpt => {
            include_bytes!("../fixtures/codex_reasoning_turn_response.sse").to_vec()
        }
        WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist => {
            let response: Value = serde_json::from_str(include_str!(
                "../fixtures/gemini_thought_parts_response.json"
            ))
            .unwrap();
            let payload = if id == WireFamilyId::GoogleCodeAssist {
                json!({"response": response})
            } else {
                response
            };
            format!("data: {payload}\n\n").into_bytes()
        }
        WireFamilyId::BedrockConverse => {
            let events: Vec<(String, Value)> =
                serde_json::from_str(include_str!("../fixtures/bedrock_reasoning_stream.json"))
                    .unwrap();
            events
                .iter()
                .flat_map(|(kind, value)| {
                    super::bedrock_converse_conformance::event_stream_message(kind, value)
                })
                .collect()
        }
    }
}

fn normalize(
    id: WireFamilyId,
    raw: &[u8],
    sizes: &[usize],
) -> Result<Vec<NormalizedEvent>, NormalizeError> {
    let frozen = frozen_config_for_family(id, "generated-test-model");
    let mut normalizer = family(id).normalizer(&frozen, StepId(1));
    let mut events = Vec::new();
    for chunk in support::chunks(raw, sizes) {
        events.extend(normalizer.push(chunk)?);
    }
    events.extend(normalizer.finish()?);
    Ok(events)
}

fn check_result(result: Result<Vec<NormalizedEvent>, NormalizeError>) {
    match result {
        Ok(events) => {
            for event in events {
                match event {
                    NormalizedEvent::Control(control) => {
                        serde_json::to_vec(&control).unwrap();
                    }
                    NormalizedEvent::Display(display) => {
                        serde_json::to_vec(&display).unwrap();
                    }
                }
            }
        }
        Err(
            NormalizeError::Invalid(_)
            | NormalizeError::StreamLimit(_)
            | NormalizeError::OutputCeiling(_),
        ) => {}
    }
}

fn generated_family(id: WireFamilyId) {
    support::guarded(move || {
        let raw = recorded(id);
        let whole = normalize(id, &raw, &[raw.len()]).expect("recorded stream must be valid");
        assert!(whole.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::AssistantMessage { .. })
        )));
        assert!(whole.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Finished { .. })
        )));
        assert_eq!(normalize(id, &raw, &[1]).unwrap(), whole, "one-byte chunks");
        support::runner()
            .run(&proptest::collection::vec(1usize..257, 1..33), |sizes| {
                let split = normalize(id, &raw, &sizes)
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                prop_assert_eq!(&split, &whole);
                Ok(())
            })
            .unwrap();

        support::runner()
            .run(
                &(
                    any::<usize>(),
                    1usize..33,
                    proptest::collection::vec(any::<u8>(), 0..2049),
                    proptest::collection::vec(1usize..257, 1..17),
                ),
                |(at, count, arbitrary, sizes)| {
                    check_result(normalize(id, &arbitrary, &sizes));
                    // Framing arbitrary data also exercises JSON decoding instead of only the record splitter.
                    let framed = if id == WireFamilyId::BedrockConverse {
                        super::bedrock_converse_conformance::raw_event_stream_message(
                            "contentBlockDelta",
                            &arbitrary,
                        )
                    } else {
                        [b"data: ".as_slice(), arbitrary.as_slice(), b"\n\n"].concat()
                    };
                    check_result(normalize(id, &framed, &sizes));
                    for damaged in
                        support::damaged(&raw, at, count, &arbitrary[..arbitrary.len().min(32)])
                    {
                        check_result(normalize(id, &damaged, &sizes));
                    }
                    Ok(())
                },
            )
            .unwrap();

        check_result(normalize(id, &[], &[1]));
        check_result(normalize(id, &raw[..raw.len() - 1], &[1]));
        let invalid_utf8 = if id == WireFamilyId::BedrockConverse {
            super::bedrock_converse_conformance::raw_event_stream_message(
                "contentBlockDelta",
                b"\xff",
            )
        } else {
            b"data: \xff\n\n".to_vec()
        };
        assert!(matches!(
            normalize(id, &invalid_utf8, &[1]),
            Err(NormalizeError::Invalid(_))
        ));

        // Large fields are separate from the small generated cases to bound allocations and runtime.
        let oversized = if id == WireFamilyId::BedrockConverse {
            super::bedrock_converse_conformance::event_stream_message(
                "contentBlockDelta",
                &json!({"delta":{"text":"x".repeat(16 * 1024 * 1024)}}),
            )
        } else {
            format!(
                "data: {{\"padding\":\"{}\"}}\n\n",
                "x".repeat(broca_provider::sse::MAX_SSE_RECORD_BYTES)
            )
            .into_bytes()
        };
        let error =
            normalize(id, &oversized, &[65536]).expect_err("oversized field must be rejected");
        if id == WireFamilyId::BedrockConverse {
            assert!(
                matches!(error, NormalizeError::Invalid(message) if message == "event-stream message exceeds maximum size")
            );
        } else {
            assert!(matches!(error, NormalizeError::StreamLimit(_)));
        }
    });
}

macro_rules! family_test {
    ($name:ident, $family:ident) => {
        #[test]
        fn $name() {
            generated_family(WireFamilyId::$family);
        }
    };
}

family_test!(anthropic_messages, AnthropicMessages);
family_test!(openai_chat, OpenAiChat);
family_test!(openai_responses, OpenAiResponses);
family_test!(chatgpt_codex, OpenAiResponsesChatgpt);
family_test!(gemini, Gemini);
family_test!(code_assist, GoogleCodeAssist);
family_test!(bedrock_converse, BedrockConverse);

fn decode(raw: &[u8], sizes: &[usize]) -> Result<Vec<String>, NormalizeError> {
    let mut decoder = SseDecoder::default();
    let mut payloads = Vec::new();
    for chunk in support::chunks(raw, sizes) {
        payloads.extend(decoder.push(chunk)?);
    }
    payloads.extend(decoder.flush()?);
    Ok(payloads)
}

#[test]
fn sse_chunk_boundaries_and_bad_input() {
    support::guarded(|| {
        for id in [
            WireFamilyId::AnthropicMessages,
            WireFamilyId::OpenAiChat,
            WireFamilyId::OpenAiResponses,
            WireFamilyId::OpenAiResponsesChatgpt,
            WireFamilyId::Gemini,
            WireFamilyId::GoogleCodeAssist,
        ] {
            let raw = recorded(id);
            let whole = decode(&raw, &[raw.len()]).unwrap();
            assert!(!whole.is_empty());
            assert_eq!(decode(&raw, &[1]).unwrap(), whole);
            support::runner()
                .run(&proptest::collection::vec(1usize..257, 1..33), |sizes| {
                    prop_assert_eq!(&decode(&raw, &sizes).unwrap(), &whole);
                    Ok(())
                })
                .unwrap();
        }
        support::runner().run(&(
            any::<String>(),
            proptest::collection::vec(any::<u8>(), 0..2049),
            proptest::collection::vec(1usize..65, 1..17),
            0usize..3,
            any::<usize>(),
        ), |(text, bytes, sizes, newline, at)| {
            let payload = serde_json::to_string(&text).unwrap();
            let eol = ["\n", "\r\n", "\r"][newline];
            let raw = format!("\u{feff}: heartbeat{eol}{eol}event: generated{eol}data: {payload}{eol}{eol}");
            prop_assert_eq!(decode(raw.as_bytes(), &sizes).unwrap(), vec![payload.clone()]);
            prop_assert_eq!(decode(raw.as_bytes(), &[1]).unwrap(), vec![payload]);
            for damaged in support::damaged(raw.as_bytes(), at, 3, &bytes) {
                let _: Result<Vec<String>, NormalizeError> = decode(&damaged, &sizes);
            }
            let _: Result<Vec<String>, NormalizeError> = decode(&bytes, &sizes);
            let framed = [b"data: ".as_slice(), bytes.as_slice(), b"\n\n"].concat();
            let _: Result<Vec<String>, NormalizeError> = decode(&framed, &sizes);
            Ok(())
        }).unwrap();
        assert!(matches!(
            decode(b"data: \xff\n\n", &[1]),
            Err(NormalizeError::Invalid(_))
        ));
        let oversized = vec![b'x'; broca_provider::sse::MAX_SSE_RECORD_BYTES + 1];
        assert!(matches!(
            decode(&oversized, &[65536]),
            Err(NormalizeError::StreamLimit(_))
        ));
    });
}
