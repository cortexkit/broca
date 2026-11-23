use super::*;
use crate::families::anthropic_apikey_frozen_config;
use crate::families::anthropic_messages::AnthropicMessages;
use crate::wire::{AuthHandle, WireFamily};
use broca_protocol::content::ContentBlock;
use broca_protocol::request::{CallOptions, EpisodeRenderInput};
use broca_protocol::{Message, Role};
use serde_json::{json, Value};

/// An api-key Opus 5.5 run whose model keeps the cache across an effort change, under adaptive
/// thinking at `effort`.
fn opus(effort: &str) -> FrozenRenderConfig {
    let mut frozen = anthropic_apikey_frozen_config("claude-opus-5-5");
    frozen.effort_change_cache = true;
    frozen.variant = Some(FrozenVariant {
        name: effort.into(),
        request: VariantRequest::Anthropic {
            thinking: Some(json!({"type": "adaptive", "display": "summarized"})),
            effort: Some(effort.into()),
        },
    });
    frozen
}

fn text(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}

/// A request at the start of each run's own first step: the prompt up to `runs` user turns,
/// each run's frozen config in `configs`, oldest first.
fn request(configs: &[FrozenRenderConfig]) -> CallOptions {
    let mut prompt = Vec::new();
    let mut episodes = Vec::new();
    for (index, config) in configs.iter().enumerate() {
        if index > 0 {
            prompt.push(text(Role::Assistant, &format!("answer {index}")));
        }
        episodes.push(EpisodeRenderInput {
            prompt_start: prompt.len(),
            first_user: Some(prompt.len()),
            frozen_config: serde_json::to_value(config).unwrap(),
        });
        prompt.push(text(Role::User, &format!("question {}", index + 1)));
    }
    let mut opts = CallOptions::new(prompt);
    opts.render_episodes = std::sync::Arc::new(episodes.into());
    opts
}

fn render(configs: &[FrozenRenderConfig]) -> Value {
    let frozen = configs.last().unwrap();
    serde_json::from_slice(&AnthropicMessages.render(frozen, &request(configs))).unwrap()
}

#[test]
fn only_a_cache_keeping_form_keeps_the_cache() {
    assert!(keeps_cache(&opus("high")));
    let mut no_fact = opus("high");
    no_fact.effort_change_cache = false;
    assert!(!keeps_cache(&no_fact), "no fact rides");
    let mut between_tools = opus("high");
    between_tools.variant = Some(FrozenVariant {
        name: "high".into(),
        request: VariantRequest::Anthropic {
            thinking: Some(json!({"type": "between_tools"})),
            effort: Some("high".into()),
        },
    });
    assert!(
        keeps_cache(&between_tools) && !per_message_effort(&between_tools),
        "the catalog decides the ride; Anthropic refuses a per-message effort without adaptive \
         thinking, so that request sends its change at the top level"
    );
}

/// The later run's effort goes in as an empty system message right after its first user
/// message, and the top level keeps the first run's effort, so the first run's request is a
/// prefix of the second's.
#[test]
fn a_cache_keeping_effort_change_is_sent_per_message() {
    let first = render(&[opus("high")]);
    let second = render(&[opus("high"), opus("low")]);
    assert_eq!(second["output_config"], json!({"effort": "high"}));
    let messages = second["messages"].as_array().unwrap();
    assert_eq!(
        messages[3],
        json!({"role": "system", "content": [], "output_config": {"effort": "low"}}),
        "{messages:#?}"
    );
    assert_eq!(messages[2]["role"], "user");
    let earlier = first["messages"].as_array().unwrap();
    assert_eq!(&messages[..earlier.len()], &earlier[..]);
    // A third run back at the first effort adds its own message; the earlier ones stay put.
    let third = render(&[opus("high"), opus("low"), opus("high")]);
    let third_messages = third["messages"].as_array().unwrap();
    assert_eq!(&third_messages[..messages.len()], &messages[..]);
    assert_eq!(
        third_messages.last().unwrap()["output_config"],
        json!({"effort": "high"})
    );
    assert_eq!(third["output_config"], json!({"effort": "high"}));
}

/// A run that can't keep the cache across its change sends it at the top level, as before, and
/// later cache-keeping changes count from its effort.
#[test]
fn a_change_that_rides_resets_the_top_level_effort() {
    let mut riding = opus("low");
    riding.effort_change_cache = false;
    let body = render(&[opus("high"), riding.clone()]);
    assert_eq!(body["output_config"], json!({"effort": "low"}));
    assert!(!body["messages"].to_string().contains("output_config"));
    let body = render(&[opus("high"), riding, opus("max")]);
    assert_eq!(body["output_config"], json!({"effort": "low"}));
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(
        messages.last().unwrap()["output_config"],
        json!({"effort": "max"})
    );
}

/// The run's beta is on every request of a run that may send a per-message effort, so the header
/// is the same before and after the first change.
#[test]
fn an_api_key_run_that_keeps_the_cache_carries_the_beta() {
    let headers = |frozen: &FrozenRenderConfig| {
        AnthropicMessages
            .endpoint(frozen, &AuthHandle::None, Vec::new())
            .headers
    };
    let beta = |frozen: &FrozenRenderConfig| {
        headers(frozen)
            .into_iter()
            .find(|(name, _)| name == "anthropic-beta")
            .map(|(_, value)| value)
            .unwrap_or_default()
    };
    assert!(beta(&opus("high")).contains(PER_MESSAGE_EFFORT_BETA));
    let mut no_fact = opus("high");
    no_fact.effort_change_cache = false;
    assert!(!beta(&no_fact).contains(PER_MESSAGE_EFFORT_BETA));
}

/// A platform API run on GPT-6 sends a later run's effort as an input configuration update and
/// keeps the first run's effort at the top level, as OpenAI's reasoning guide describes; a run
/// whose model doesn't keep the cache changes the top-level effort.
#[test]
fn a_platform_gpt6_effort_change_goes_in_as_a_configuration_update() {
    use crate::families::{openai_responses::OpenAiResponses, openai_responses_frozen_config};
    let gpt6 = |effort: &str, keeps: bool| {
        let mut frozen = openai_responses_frozen_config("gpt-6.2-sol");
        frozen.reasoning = crate::policy::ReasoningPolicy::OpenAiResponsesReasoning {
            effort: effort.into(),
            summary: Some("auto".into()),
        };
        frozen.effort_change_cache = keeps;
        frozen
    };
    let render = |configs: &[FrozenRenderConfig]| -> Value {
        let frozen = configs.last().unwrap();
        serde_json::from_slice(&OpenAiResponses.render(frozen, &request(configs))).unwrap()
    };
    let body = render(&[gpt6("high", true), gpt6("low", true)]);
    assert_eq!(body["reasoning"]["effort"], "high");
    assert!(
        body["input"].to_string().contains("configuration_update"),
        "{}",
        body["input"]
    );
    let body = render(&[gpt6("high", false), gpt6("low", false)]);
    assert_eq!(body["reasoning"]["effort"], "low");
    assert!(!body["input"].to_string().contains("configuration_update"));
}
