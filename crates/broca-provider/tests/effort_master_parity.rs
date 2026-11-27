use broca_protocol::{request::EpisodeRenderInput, CallOptions, ContentBlock, Message, Role};
use broca_provider::{
    families::{
        anthropic_apikey_frozen_config, openai_responses_frozen_config, AnthropicMessages,
        OpenAiResponses,
    },
    policy::ReasoningPolicy,
    variant::{FrozenVariant, VariantRequest},
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::json;

fn request(configs: &[FrozenRenderConfig]) -> CallOptions {
    let message = |role, text: &str, ordinal| {
        let mut message = Message::new(role, vec![ContentBlock::Text { text: text.into() }]);
        message.ordinal = ordinal;
        message
    };
    let mut opts = CallOptions::new(vec![
        message(Role::System, "prepended system", None),
        message(Role::User, "earlier question", Some(40)),
        message(Role::Assistant, "earlier reply", Some(41)),
        message(Role::User, "question in the new effort", Some(42)),
        message(Role::Assistant, "reply in the new effort", Some(43)),
        message(Role::User, "next question", Some(44)),
    ]);
    let episodes = configs
        .iter()
        .zip([40, 42])
        .map(|(config, position)| EpisodeRenderInput {
            prompt_start: position,
            first_user: Some(position),
            frozen_config: serde_json::to_value(config).unwrap(),
        })
        .collect::<Vec<_>>();
    opts.render_episodes = std::sync::Arc::new(episodes.into());
    opts
}

fn claude(effort: &str) -> FrozenRenderConfig {
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

fn responses(effort: &str) -> FrozenRenderConfig {
    let mut frozen = openai_responses_frozen_config("gpt-6-luna");
    frozen.effort_change_cache = true;
    frozen.reasoning = ReasoningPolicy::OpenAiResponsesReasoning {
        effort: effort.into(),
        summary: Some("auto".into()),
    };
    frozen
}

#[test]
fn no_note_effort_positions_match_master_after_a_seal_and_system_prepend() {
    let claude = [claude("low"), claude("high")];
    let responses = [responses("low"), responses("high")];
    let bodies = [
        AnthropicMessages.render(&claude[1], &request(&claude)),
        OpenAiResponses.render(&responses[1], &request(&responses)),
    ];
    let hashes = bodies
        .each_ref()
        .map(|bytes| broca_core::render::request_digest_hex(bytes));
    assert_eq!(
        hashes,
        [
            "4d163e6b41be0dc319c0bb78031d68ea07bd836f86e85ba51098de7b217e5676",
            "4fd854fb3f19a4358af6f0c2385ca17c9cfebe4dc7540e9e3442f8d967ee927a"
        ],
        "{:?}",
        bodies.map(|bytes| String::from_utf8(bytes).unwrap())
    );
}

#[test]
fn first_plan_note_keeps_adaptive_effort_before_the_reply_signed_after_it() {
    use broca_protocol::ModelHandle;
    use broca_provider::policy::CrossModelThinking;
    let model = ModelHandle::new("anthropic", "claude-opus-5-5");
    let freeze = |effort| {
        let mut frozen = claude(effort);
        frozen.cache = None;
        frozen.anthropic_turn_system = true;
        frozen.reasoning = ReasoningPolicy::AnthropicInterleaved {
            budget_tokens: 1024,
            last_turn_only: true,
            echo_scope: broca_provider::policy::ThinkingEchoScope::AllAssistantMessages,
            cross_model: CrossModelThinking::SameModelOnly,
        };
        frozen
    };
    let low = freeze("low");
    let high = freeze("high");
    let at = |role, text: &str, ordinal| {
        let mut message = Message::new(role, vec![ContentBlock::Text { text: text.into() }]);
        message.ordinal = Some(ordinal);
        message
    };
    let mut prefix = CallOptions::new(vec![
        at(Role::User, "U2", 2),
        at(Role::Assistant, "A2", 3),
        at(Role::User, "U3", 4),
    ]);
    let episodes =
        [(0, &low), (2, &low), (4, &high), (6, &high), (8, &high)].map(|(ordinal, config)| {
            EpisodeRenderInput {
                prompt_start: ordinal,
                first_user: Some(ordinal),
                frozen_config: serde_json::to_value(config).unwrap(),
            }
        });
    prefix.render_episodes = std::sync::Arc::new(episodes[..3].to_vec().into());
    prefix.target_model = Some(model.clone());
    let signed = AnthropicMessages.shape(&high, &prefix);
    assert_eq!(signed["messages"][3]["output_config"]["effort"], "high");
    let mut a3 = Message::assistant_from(
        vec![
            ContentBlock::Reasoning {
                text: "under high effort".into(),
                signature: Some("signed-after-effort".into()),
            },
            ContentBlock::Text { text: "A3".into() },
        ],
        model,
    );
    a3.ordinal = Some(5);
    prefix.prompt.extend([
        a3,
        at(Role::User, "U4", 6),
        at(Role::Assistant, "A4", 7),
        at(Role::User, "U5", 8),
    ]);
    prefix.render_episodes = std::sync::Arc::new(episodes.to_vec().into());
    assert!(!prefix.render_episodes.raw_ordinals);
    let view = broca_core::plan_notes::view_for(
        &prefix,
        [(9, broca_core::plan_notes::Note::Text("first note"))],
        true,
    )
    .unwrap();
    let body = AnthropicMessages.shape(&high, &view);
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(&messages[..4], signed["messages"].as_array().unwrap());
    assert_eq!(
        messages[4]["content"][0]["signature"],
        "signed-after-effort"
    );
    assert_eq!(messages.last().unwrap()["content"][0]["text"], "first note");
}

#[test]
fn note_free_anthropic_effort_matches_the_0149_bytes() {
    let configs = [claude("low"), claude("high")];
    let bytes = AnthropicMessages.render(&configs[1], &request(&configs));
    assert_eq!(
        bytes,
        include_str!("fixtures/anthropic_note_free_effort_0149.json")
            .trim_end()
            .as_bytes()
    );
}

#[test]
fn an_unused_fallback_echo_scope_does_not_keep_an_extra_sealed_episode() {
    let mut primary = serde_json::to_value(claude("low")).unwrap();
    primary["reasoning"]["echo_scope"] = "all_assistant_messages".into();
    let mut fallback = primary.clone();
    fallback["reasoning"] = json!({"mode":"anthropic_interleaved","budget_tokens":1024,"last_turn_only":true,"echo_scope":"all_assistant_messages"});
    let mut config = primary.clone();
    config["refusal_fallback"] = json!({"model":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},"frozen":fallback});
    let regular = broca_provider::families::openai_responses_chatgpt::episode_render_key(&config);
    assert_ne!(regular["unreadable"], true);
    config["refusal_fallback"]["frozen"]["reasoning"]
        .as_object_mut()
        .unwrap()
        .remove("echo_scope");
    assert_eq!(
        broca_provider::families::openai_responses_chatgpt::episode_render_key(&config),
        regular
    );
}
