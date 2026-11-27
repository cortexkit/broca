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

/// Sealing history keeps only the runs whose `episode_render_key` changes (and every run from the
/// cut on), and the request must render the same bytes from those runs as from all of them. The
/// Anthropic effort and thinking mode of an earlier run decide its per-message efforts, so runs
/// that differ only there must not look alike to the seal.
#[test]
fn sealed_runs_render_the_same_anthropic_effort_changes() {
    let omitted = |effort: &str| {
        let mut frozen = opus(effort);
        frozen.variant = Some(FrozenVariant {
            name: effort.into(),
            request: VariantRequest::Anthropic {
                thinking: Some(json!({"type": "adaptive", "display": "omitted"})),
                effort: Some(effort.into()),
            },
        });
        frozen
    };
    for configs in [
        vec![opus("high"), opus("high"), opus("low"), opus("high")],
        vec![opus("high"), opus("high"), omitted("high"), omitted("low")],
    ] {
        // Every message carries its position in the session as its ordinal, as a view that
        // cut earlier messages passes them, and the cut falls at the start of the last run.
        let mut whole = request(&configs);
        for (ordinal, message) in whole.prompt.iter_mut().enumerate() {
            message.ordinal = Some(ordinal as u64);
        }
        let mut history = broca_protocol::request::RenderHistory::clone(&whole.render_episodes);
        history.raw_ordinals = true;
        let first_kept = history.episodes.last().unwrap().prompt_start;
        whole.prompt.drain(..first_kept);
        whole.render_episodes = std::sync::Arc::new(history.clone());
        let reduced_history =
            crate::families::openai_responses_chatgpt::reduce_episodes(&history, first_kept);
        assert!(
            reduced_history.episodes.len() < history.episodes.len(),
            "the seal drops a run"
        );
        let mut reduced = whole.clone();
        reduced.render_episodes = std::sync::Arc::new(reduced_history);
        let frozen = configs.last().unwrap();
        let parse = |opts: &CallOptions| -> Value {
            serde_json::from_slice(&AnthropicMessages.render(frozen, opts)).unwrap()
        };
        assert_eq!(parse(&reduced), parse(&whole), "{configs:#?}");
    }
}

/// An earlier run with no Anthropic effort (another family, or no effort variant) resets the walk
/// like a run that rides: the next run with an effort anchors the top level, so a later change on
/// the same model still goes in per message and the earlier request stays a prefix.
#[test]
fn an_earlier_run_without_an_effort_starts_a_new_anchor() {
    let mut no_variant = opus("high");
    no_variant.variant = None;
    let other_family = crate::families::openai_responses_frozen_config("gpt-6.2-sol");
    for earlier in [no_variant, other_family] {
        let first = render(&[earlier.clone(), opus("high")]);
        let body = render(&[earlier.clone(), opus("high"), opus("low")]);
        assert_eq!(
            body["output_config"],
            json!({"effort": "high"}),
            "after {:?}",
            earlier.family_id
        );
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(
            messages.last().unwrap()["output_config"],
            json!({"effort": "low"}),
            "{messages:#?}"
        );
        let earlier_messages = first["messages"].as_array().unwrap();
        assert_eq!(&messages[..earlier_messages.len()], &earlier_messages[..]);
    }
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

/// A fork's ask lowers its answer's effort only below the run's own, and only where the model
/// keeps its cache across the change; a hint at or above the run's effort, or on a model the
/// catalog didn't vouch for, sends the run's effort.
#[test]
fn a_fork_ask_lowers_its_answer_effort_only_where_the_cache_holds() {
    assert_eq!(answer_effort(&opus("high"), "low").as_deref(), Some("low"));
    assert_eq!(answer_effort(&opus("high"), "high"), None, "not lower");
    assert_eq!(answer_effort(&opus("medium"), "high"), None, "never higher");
    let mut no_fact = opus("high");
    no_fact.effort_change_cache = false;
    assert_eq!(
        answer_effort(&no_fact, "low"),
        None,
        "the cache would be lost"
    );
    assert_eq!(
        answer_effort(&opus("high"), "unheard-of"),
        None,
        "can't be ordered"
    );
}

/// The lowered effort goes after the question as a per-message effort, so every byte before it
/// is the request without the hint, and the top level keeps the run's effort.
#[test]
fn a_fork_answer_effort_follows_the_question_and_keeps_the_prefix() {
    let frozen = opus("high");
    let plain = request(std::slice::from_ref(&frozen));
    let mut lowered = plain.clone();
    std::sync::Arc::make_mut(&mut lowered.render_episodes).answer_effort = Some("low".into());
    let parse = |opts: &CallOptions| -> Value {
        serde_json::from_slice(&AnthropicMessages.render(&frozen, opts)).unwrap()
    };
    let (plain, lowered) = (parse(&plain), parse(&lowered));
    assert_eq!(lowered["output_config"], json!({"effort": "high"}));
    let before = plain["messages"].as_array().unwrap();
    let after = lowered["messages"].as_array().unwrap();
    assert_eq!(&after[..before.len()], &before[..]);
    assert_eq!(after.len(), before.len() + 1);
    assert_eq!(
        after.last().unwrap(),
        &json!({"role": "system", "content": [], "output_config": {"effort": "low"}})
    );
}

/// On GPT-6 the lowered effort is a configuration update right before the question, the
/// prompt's last message; the top-level effort stays the run's.
#[test]
fn a_fork_answer_effort_on_gpt6_is_an_update_before_the_question() {
    use crate::families::{openai_responses::OpenAiResponses, openai_responses_frozen_config};
    let mut frozen = openai_responses_frozen_config("gpt-6.2-sol");
    frozen.reasoning = crate::policy::ReasoningPolicy::OpenAiResponsesReasoning {
        effort: "high".into(),
        summary: Some("auto".into()),
    };
    frozen.effort_change_cache = true;
    let hint = answer_effort(&frozen, "low");
    assert_eq!(hint.as_deref(), Some("low"));
    let mut opts = request(std::slice::from_ref(&frozen));
    std::sync::Arc::make_mut(&mut opts.render_episodes).answer_effort = hint;
    let body: Value = serde_json::from_slice(&OpenAiResponses.render(&frozen, &opts)).unwrap();
    assert_eq!(body["reasoning"]["effort"], "high");
    let input = body["input"].as_array().unwrap();
    let question = input.len() - 1;
    assert!(
        input[question - 1]
            .to_string()
            .contains("configuration_update")
            && input[question - 1].to_string().contains("low"),
        "{input:#?}"
    );
    assert!(
        input[question].to_string().contains("question 1"),
        "{input:#?}"
    );
}

fn native_tool(
    form: broca_protocol::tool_addition::ToolAdditionForm,
) -> broca_core::plan_notes::Note<'static> {
    broca_core::plan_notes::Note::ToolAddition(
        form,
        broca_protocol::tool_addition::ToolDefinition {
            name: "find".into(),
            description: "Find a place".into(),
            input_schema: json!({"type":"object"}).into(),
        },
    )
}

#[test]
fn a_view_gap_can_flush_an_effort_after_a_native_declaration() {
    use broca_protocol::tool_addition::ToolAdditionForm::AnthropicToolAddition;
    let frozen = opus("high");
    let mut opts = request(&[opus("low"), frozen.clone()]);
    opts.prompt = vec![
        text(Role::User, "old note anchor"),
        text(Role::Assistant, "reply kept past the cut"),
    ];
    opts.prompt[0].ordinal = Some(0);
    opts.prompt[1].ordinal = Some(4);
    std::sync::Arc::make_mut(&mut opts.render_episodes).raw_ordinals = true;
    let view =
        broca_core::plan_notes::view(&opts, [(1, native_tool(AnthropicToolAddition))]).unwrap();
    let wire = AnthropicMessages.shape(&frozen, &view);
    assert_eq!(wire["messages"][1]["content"][0]["type"], "tool_addition");
    assert_eq!(wire["messages"][2]["output_config"]["effort"], "high");
}

#[test]
fn older_notes_do_not_mix_prompt_indices_and_ordinals_for_effort() {
    use broca_protocol::tool_addition::ToolAdditionForm::AnthropicToolAddition;
    let frozen = opus("high");
    let mut opts = request(&[opus("low"), frozen.clone()]);
    broca_protocol::content::stamp_ordinals(&mut opts.prompt);
    let view = broca_core::plan_notes::view(
        &opts,
        [
            (1, broca_core::plan_notes::Note::Text("Earlier note")),
            (3, native_tool(AnthropicToolAddition)),
        ],
    )
    .unwrap();
    let wire = AnthropicMessages.shape(&frozen, &view);
    assert!(wire["messages"].to_string().contains("tool_addition"));
    assert_eq!(
        wire["messages"].as_array().unwrap().last().unwrap()["output_config"]["effort"],
        "high"
    );
}

#[test]
fn responses_keeps_answer_effort_when_a_native_addition_ends_the_prompt() {
    use crate::families::{openai_responses::OpenAiResponses, openai_responses_frozen_config};
    use broca_protocol::tool_addition::ToolAdditionForm::ResponsesAdditionalTools;
    let mut frozen = openai_responses_frozen_config("gpt-6-luna");
    frozen.reasoning = crate::policy::ReasoningPolicy::OpenAiResponsesReasoning {
        effort: "high".into(),
        summary: Some("auto".into()),
    };
    frozen.effort_change_cache = true;
    let mut opts = request(std::slice::from_ref(&frozen));
    broca_protocol::content::stamp_ordinals(&mut opts.prompt);
    std::sync::Arc::make_mut(&mut opts.render_episodes).answer_effort = Some("low".into());
    let opts =
        broca_core::plan_notes::view(&opts, [(1, native_tool(ResponsesAdditionalTools))]).unwrap();
    let wire = OpenAiResponses.shape(&frozen, &opts);
    let items = wire["input"].as_array().unwrap();
    assert_eq!(items[0]["type"], "configuration_update");
    assert_eq!(items[0]["reasoning"]["effort"], "low");
    assert_eq!(items[1]["role"], "user");
    assert_eq!(items[2]["type"], "additional_tools");
}

#[test]
fn responses_keeps_a_native_addition_next_to_an_actual_configuration_update() {
    use crate::families::OpenAiResponses;
    use broca_protocol::tool_addition::ToolAdditionForm::ResponsesAdditionalTools;
    let gpt = |effort: &str| {
        let mut config = crate::families::openai_responses_frozen_config("gpt-6-luna");
        config.reasoning = crate::policy::ReasoningPolicy::OpenAiResponsesReasoning {
            effort: effort.into(),
            summary: Some("auto".into()),
        };
        config.effort_change_cache = true;
        config
    };
    let frozen = gpt("high");
    let mut opts = request(&[gpt("low"), frozen.clone()]);
    opts.prompt = vec![
        text(Role::User, "old anchor"),
        text(Role::Assistant, "reply kept past the view gap"),
    ];
    opts.prompt[0].ordinal = Some(0);
    opts.prompt[1].ordinal = Some(4);
    std::sync::Arc::make_mut(&mut opts.render_episodes).raw_ordinals = true;
    let opts =
        broca_core::plan_notes::view(&opts, [(1, native_tool(ResponsesAdditionalTools))]).unwrap();
    let wire = OpenAiResponses.shape(&frozen, &opts);
    let input = wire["input"].as_array().unwrap();
    assert_eq!(input[1]["type"], "additional_tools");
    assert_eq!(input[2]["type"], "configuration_update");
}
