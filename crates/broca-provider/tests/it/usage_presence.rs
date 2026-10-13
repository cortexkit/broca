use broca_protocol::{NormalizedEvent, ProviderControl, StepId, Usage};
use broca_provider::families::*;
use broca_provider::policy::UsagePolicy;
use broca_provider::wire::{FrozenRenderConfig, WireFamily};
use serde_json::{json, Value};

fn normalize(family: &dyn WireFamily, config: FrozenRenderConfig, records: &[Value]) -> Usage {
    let wire = records
        .iter()
        .map(|r| format!("data: {r}\n\n"))
        .collect::<String>();
    let mut normalizer = family.normalizer(&config, StepId(1));
    let mut events = normalizer.push(wire.as_bytes()).unwrap();
    events.extend(normalizer.finish().unwrap());
    events
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(usage),
            _ => None,
        })
        .expect("step usage")
}

fn chat(wire_usage: Value, policy: UsagePolicy) -> Usage {
    let mut config = openai_chat_frozen_config("synthetic");
    config.usage = policy;
    normalize(
        &OpenAiChat,
        config,
        &[
            json!({"id":"usage", "choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}],"usage":wire_usage}),
        ],
    )
}

#[test]
fn chat_three_reported_counts_serialize_without_unreported_measurements() {
    // Synthetic input for the ollama-cloud investigation: given exactly three counts,
    // the parser must retain those counts (including zero) and invent no other keys.
    // A capture adds nothing to that input-determined property; this does not assert
    // what Ollama sends. The downstream usage store observed 3,263 ollama-cloud facts
    // with zero cache-write/reasoning, but the old normalizer produced those zeros
    // for both missing and explicit-zero keys. After deployment, that store's count
    // of measurements per event can distinguish Ollama's actual response shapes.
    let usage = chat(
        json!({"prompt_tokens":12,"prompt_tokens_details":{"cached_tokens":0},"completion_tokens":8}),
        UsagePolicy::OpenAiUsage,
    );
    assert_eq!(usage.cached_input_tokens, Some(0));
    assert_eq!(usage.cache_write_tokens, None);
    assert_eq!(usage.reasoning_tokens, None);
    assert_eq!(
        serde_json::to_value(usage).unwrap(),
        json!({"input_tokens":12,"cached_input_tokens":0,"output_tokens":8})
    );
}

#[test]
fn chat_preserves_each_reported_zero_and_positive_detail() {
    for n in [0, 7] {
        let usage = chat(
            json!({"prompt_tokens":20,"completion_tokens":20,
            "prompt_tokens_details":{"cached_tokens":n,"cache_write_tokens":n},
            "completion_tokens_details":{"reasoning_tokens":n}}),
            UsagePolicy::OpenAiUsage,
        );
        assert_eq!(usage.cached_input_tokens, Some(n));
        assert_eq!(usage.cache_write_tokens, Some(n));
        assert_eq!(usage.reasoning_tokens, Some(n));
    }
    assert_eq!(chat(json!({}), UsagePolicy::OpenAiUsage), Usage::default());
}

#[test]
fn deepseek_top_level_cache_shape_preserves_zero_without_inventing_cache_write() {
    let usage = chat(
        json!({"prompt_tokens":12,"prompt_cache_hit_tokens":0,"completion_tokens":0}),
        UsagePolicy::OpenAiUsage,
    );
    assert_eq!(usage.cached_input_tokens, Some(0));
    assert_eq!(usage.output_tokens, Some(0));
    assert_eq!(usage.cache_write_tokens, None);
    assert_eq!(usage.reasoning_tokens, None);
}

#[test]
fn xai_sibling_counts_preserve_zero_absence_and_reasoning_subset() {
    for (wire, output, reasoning) in [
        (json!({}), None, None),
        (json!({"completion_tokens":0}), Some(0), None),
        (
            json!({"completion_tokens_details":{"reasoning_tokens":0}}),
            Some(0),
            Some(0),
        ),
        (
            json!({"completion_tokens_details":{"reasoning_tokens":7}}),
            Some(7),
            Some(7),
        ),
        (
            json!({"completion_tokens":3,"completion_tokens_details":{"reasoning_tokens":7}}),
            Some(10),
            Some(7),
        ),
    ] {
        let usage = chat(wire, UsagePolicy::OpenAiUsageReasoningSeparate);
        assert_eq!(usage.output_tokens, output);
        assert_eq!(usage.reasoning_tokens, reasoning);
        assert_eq!(usage.cache_write_tokens, None);
    }
}

#[test]
fn anthropic_preserves_positive_cache_write_and_zero_without_inventing_reasoning() {
    for n in [0, 7] {
        let usage = normalize(
            &AnthropicMessages,
            anthropic_frozen_config("synthetic"),
            &[
                json!({"type":"message_start","message":{"id":"usage","usage":{"input_tokens":0,"cache_creation_input_tokens":n,"cache_read_input_tokens":0,"output_tokens":0}}}),
                json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":0}}),
                json!({"type":"message_stop"}),
            ],
        );
        assert_eq!(usage.input_tokens, Some(0));
        assert_eq!(usage.cached_input_tokens, Some(0));
        assert_eq!(usage.cache_write_tokens, Some(n));
        assert_eq!(usage.output_tokens, Some(0));
        assert_eq!(usage.reasoning_tokens, None);
        assert_eq!(
            serde_json::to_value(usage).unwrap()["cache_write_tokens"],
            n
        );
    }
}

fn responses(family: &dyn WireFamily, config: FrozenRenderConfig, usage: Value) -> Usage {
    normalize(
        family,
        config,
        &[
            json!({"type":"response.created","response":{"id":"usage","status":"in_progress"}}),
            json!({"type":"response.completed","response":{"id":"usage","status":"completed","usage":usage}}),
        ],
    )
}

#[test]
fn responses_preserves_zero_and_omits_unreported_cache_write() {
    let usage = responses(
        &OpenAiResponses,
        openai_responses_frozen_config("synthetic"),
        json!({"input_tokens":0,"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0}}),
    );
    assert_eq!(usage.input_tokens, Some(0));
    assert_eq!(usage.output_tokens, Some(0));
    assert_eq!(usage.reasoning_tokens, Some(0));
    assert_eq!(usage.cache_write_tokens, None);
    assert_eq!(usage.cached_input_tokens, None);
}

#[test]
fn chatgpt_responses_preserves_zero_and_omits_unreported_cache_write() {
    let usage = responses(
        &OpenAiResponsesChatgpt,
        openai_responses_chatgpt_frozen_config("synthetic"),
        json!({"input_tokens":0,"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0}}),
    );
    assert_eq!(usage.input_tokens, Some(0));
    assert_eq!(usage.reasoning_tokens, Some(0));
    assert_eq!(usage.cache_write_tokens, None);
}

fn gemini(
    family: &dyn WireFamily,
    config: FrozenRenderConfig,
    metadata: Value,
    wrapped: bool,
) -> Usage {
    let record = json!({"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":metadata});
    normalize(
        family,
        config,
        &[if wrapped {
            json!({"response":record})
        } else {
            record
        }],
    )
}

#[test]
fn gemini_sibling_counts_preserve_zero_absence_and_reasoning_subset() {
    for (metadata, output, reasoning) in [
        (json!({}), None, None),
        (json!({"candidatesTokenCount":0}), Some(0), None),
        (json!({"thoughtsTokenCount":0}), Some(0), Some(0)),
        (json!({"thoughtsTokenCount":7}), Some(7), Some(7)),
        (
            json!({"candidatesTokenCount":3,"thoughtsTokenCount":7}),
            Some(10),
            Some(7),
        ),
    ] {
        let usage = gemini(&Gemini, gemini_frozen_config("synthetic"), metadata, false);
        assert_eq!(usage.output_tokens, output);
        assert_eq!(usage.reasoning_tokens, reasoning);
        assert_eq!(usage.cache_write_tokens, None);
    }
}

#[test]
fn antigravity_envelope_preserves_zero_and_omits_cache_write() {
    let usage = gemini(
        &GoogleCodeAssist,
        google_codeassist_frozen_config("synthetic"),
        json!({"promptTokenCount":0,"candidatesTokenCount":0,"thoughtsTokenCount":0}),
        true,
    );
    assert_eq!(usage.input_tokens, Some(0));
    assert_eq!(usage.output_tokens, Some(0));
    assert_eq!(usage.reasoning_tokens, Some(0));
    assert_eq!(usage.cache_write_tokens, None);
}

#[test]
fn gemini_explicit_zero_replaces_prior_positive_cumulative_counts() {
    let usage = normalize(
        &Gemini,
        gemini_frozen_config("synthetic"),
        &[
            json!({"candidates":[{"content":{"parts":[{"text":"ok"}]}}],"usageMetadata":{"candidatesTokenCount":4,"thoughtsTokenCount":7}}),
            json!({"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":0,"thoughtsTokenCount":0}}),
        ],
    );
    assert_eq!(usage.output_tokens, Some(0));
    assert_eq!(usage.reasoning_tokens, Some(0));
}
