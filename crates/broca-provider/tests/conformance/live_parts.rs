//! Live display coordinates are indices of stored blocks, including blocks with no deltas.
use broca_protocol::{ContentBlock, DisplayEvent, NormalizedEvent, ProviderControl, StepId};
use broca_provider::families::openai_chat::OpenAiChatNormalizer;
use broca_provider::families::openai_responses::OpenAiResponsesNormalizer;
use broca_provider::policy::UsagePolicy;
use broca_provider::wire::StreamNormalizer;

fn drive(mut n: impl StreamNormalizer, fixture: &str) -> Vec<NormalizedEvent> {
    let mut events = Vec::new();
    for chunk in fixture.as_bytes().chunks(17) {
        events.extend(n.push(chunk).unwrap());
    }
    events.extend(n.finish().unwrap());
    events
}

fn assert_parts(events: &[NormalizedEvent], resets: usize) {
    let message = events
        .iter()
        .find_map(|e| match e {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("the step assembled a message");
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, NormalizedEvent::Display(DisplayEvent::DisplayReset)))
            .count(),
        resets
    );
    let start = events
        .iter()
        .rposition(|e| matches!(e, NormalizedEvent::Display(DisplayEvent::DisplayReset)))
        .map_or(0, |i| i + 1);
    let mut deltas = 0;
    for e in &events[start..] {
        if let NormalizedEvent::Display(event) = e {
            if event.delta().is_none() {
                continue;
            }
            let (_, part) = event
                .coordinates()
                .expect("every delta has a stored coordinate");
            let block = &message.content[part as usize];
            match (event, block) {
                (DisplayEvent::TextDelta { delta, .. }, ContentBlock::Text { text }) => {
                    assert!(text.contains(delta))
                }
                (
                    DisplayEvent::ReasoningDelta { delta, .. },
                    ContentBlock::Reasoning { text, .. },
                ) => assert!(text.contains(delta)),
                (DisplayEvent::ReasoningDelta { .. }, ContentBlock::RedactedReasoning { .. }) => {}
                (
                    DisplayEvent::ToolInputDelta { tool_call_id, .. },
                    ContentBlock::ToolCall(call),
                ) => assert_eq!(tool_call_id, &call.tool_call_id),
                pair => panic!("wrong stored part: {pair:?}"),
            }
            deltas += 1;
        }
    }
    assert!(deltas > 0);
}

#[test]
fn chat_late_reasoning_resets_only_shifted_indices() {
    let before = broca_provider::display_index_resets();
    let events = drive(
        OpenAiChatNormalizer::new(StepId(1), UsagePolicy::OpenAiUsage),
        include_str!("../fixtures/live_chat_late_reasoning.sse"),
    );
    assert_parts(&events, 1);
    assert!(broca_provider::display_index_resets() > before);
    let ordered = drive(
        OpenAiChatNormalizer::new(StepId(2), UsagePolicy::OpenAiUsage),
        include_str!("../fixtures/openai_chat_reasoning_content.sse"),
    );
    assert_parts(&ordered, 0);
}

#[test]
fn responses_reordered_items_correct_live_indices() {
    let events = drive(
        OpenAiResponsesNormalizer::new(StepId(1)),
        include_str!("../fixtures/live_responses_reordered.sse"),
    );
    assert_parts(&events, 1);
    let ordered = drive(
        OpenAiResponsesNormalizer::new(StepId(2)),
        include_str!("../fixtures/live_responses_prefix.sse"),
    );
    assert_parts(&ordered, 0);
}

#[test]
fn responses_empty_items_reserve_no_phantom_parts_or_resets() {
    let events = drive(
        OpenAiResponsesNormalizer::new(StepId(1)),
        include_str!("../fixtures/live_responses_empty_prefix.sse"),
    );
    assert_parts(&events, 0);
    let parts: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            NormalizedEvent::Display(e) => e.coordinates().map(|(_, part)| part),
            _ => None,
        })
        .collect();
    assert_eq!(parts, vec![0]);
}

#[test]
fn chat_redacted_prefix_counts_without_display_deltas() {
    use broca_provider::policy::{ReasoningEchoFormat, ReasoningEchoPolicy, ReasoningEchoTurns};
    let normalizer = OpenAiChatNormalizer::new(StepId(1), UsagePolicy::OpenAiUsage)
        .with_reasoning_echo(Some(ReasoningEchoPolicy {
            format: ReasoningEchoFormat::OpenRouterDetails,
            turns: ReasoningEchoTurns::EveryAssistant,
        }));
    let events = drive(
        normalizer,
        include_str!("../fixtures/live_chat_redacted_prefix.sse"),
    );
    assert_parts(&events, 0);
    assert!(events.iter().any(|e| matches!(
        e,
        NormalizedEvent::Display(DisplayEvent::TextDelta { part: Some(1), .. })
    )));
}

#[test]
fn gemini_delta_less_prefix_counts_stored_blocks() {
    use broca_provider::families::{gemini_frozen_config, Gemini};
    use broca_provider::wire::WireFamily;
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/live_gemini_prefix.json")).unwrap();
    let mut n = Gemini.normalizer(&gemini_frozen_config("gemini-test"), StepId(1));
    let mut events = n.push(format!("data: {fixture}\n\n").as_bytes()).unwrap();
    events.extend(n.finish().unwrap());
    assert_parts(&events, 0);
    assert!(events.iter().any(|e| matches!(
        e,
        NormalizedEvent::Display(DisplayEvent::TextDelta { part: Some(1), .. })
    )));
}

#[test]
fn bedrock_delta_less_blocks_count_before_streamed_tool_and_text() {
    use broca_provider::families::{bedrock_frozen_config, BedrockConverse};
    use broca_provider::wire::WireFamily;
    let fixture: Vec<(String, serde_json::Value)> =
        serde_json::from_str(include_str!("../fixtures/bedrock_reasoning_stream.json")).unwrap();
    let raw: Vec<u8> = fixture
        .iter()
        .flat_map(|(kind, payload)| {
            super::bedrock_converse_conformance::event_stream_message(kind, payload)
        })
        .collect();
    let mut n = BedrockConverse.normalizer(&bedrock_frozen_config("bedrock-test"), StepId(1));
    let mut events = n.push(&raw).unwrap();
    events.extend(n.finish().unwrap());
    assert_parts(&events, 0);
    assert!(events.iter().any(|e| matches!(
        e,
        NormalizedEvent::Display(DisplayEvent::ToolInputDelta { part: Some(3), .. })
    )));
}

#[test]
fn anthropic_delta_less_prefix_keeps_stored_indices_without_reset() {
    let events = broca_provider::AnthropicNormalizer::normalize_sse(
        StepId(1),
        include_str!("../fixtures/anthropic-tool-call.sse"),
    )
    .unwrap();
    assert_parts(&events, 0);
    assert_eq!(
        events.iter().find_map(|e| match e {
            NormalizedEvent::Display(e) => e.coordinates().map(|(_, p)| p),
            _ => None,
        }),
        Some(1)
    );
}
