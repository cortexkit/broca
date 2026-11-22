//! The ChatGPT backend's reasoning-effort updates: a run whose effort differs from the one in
//! force gets a `configuration_update` input item before its first user message, and each
//! request's serialized input stays a byte prefix of the next request's.
use std::sync::Arc;

use broca_protocol::{
    request::{EpisodeRenderInput, RenderHistory},
    CallOptions, ContentBlock, Message, Role,
};
use broca_provider::{
    families::{
        openai_responses_chatgpt::OpenAiResponsesChatgpt, openai_responses_chatgpt_frozen_config,
    },
    policy::ReasoningPolicy,
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::{json, Value};

fn config(model: &str, effort: &str, admitted: bool) -> FrozenRenderConfig {
    let mut frozen = openai_responses_chatgpt_frozen_config(model);
    frozen.reasoning = ReasoningPolicy::OpenAiResponsesReasoning {
        effort: effort.into(),
        summary: Some("auto".into()),
    };
    frozen.chatgpt_effort_updates = admitted;
    frozen
}

/// One run of a session: the render config it was admitted with, as its run start stores it,
/// and its input, one message named after the run's effort (or none).
#[derive(Clone)]
struct Run {
    frozen_config: Value,
    effort: String,
    role: Option<Role>,
}

fn start(model: &str, effort: &str, admitted: bool, role: Option<Role>) -> Run {
    Run {
        frozen_config: serde_json::to_value(config(model, effort, admitted)).unwrap(),
        effort: effort.into(),
        role,
    }
}

fn records(model: &str, efforts: &[&str], admitted: bool) -> Vec<Run> {
    efforts
        .iter()
        .map(|effort| start(model, effort, admitted, Some(Role::User)))
        .collect()
}

/// The newest run's request: every run's input in order, each message at its position in the
/// session, with every run's place and render config as the run loop passes them, rendered
/// under the newest run's config.
fn request(runs: &[Run]) -> (FrozenRenderConfig, CallOptions) {
    let mut prompt = Vec::new();
    let mut episodes = Vec::new();
    for run in runs {
        let prompt_start = prompt.len();
        if let Some(role) = run.role {
            prompt.push(Message {
                turn_system: false,
                role,
                content: vec![ContentBlock::Text {
                    text: format!("episode-{}", run.effort).into(),
                }],
                origin: None,
                ordinal: Some(prompt_start as u64),
                tags: Default::default(),
                cache_prefix_blocks: None,
            });
        }
        episodes.push(EpisodeRenderInput {
            prompt_start,
            first_user: (run.role == Some(Role::User)).then_some(prompt_start),
            frozen_config: run.frozen_config.clone(),
        });
    }
    let frozen = serde_json::from_value(runs.last().unwrap().frozen_config.clone()).unwrap();
    let mut opts = CallOptions::new(prompt);
    opts.render_episodes = Arc::new(RenderHistory::from(episodes));
    (frozen, opts)
}

fn render(runs: &[Run]) -> Vec<u8> {
    let (frozen, opts) = request(runs);
    OpenAiResponsesChatgpt.render(&frozen, &opts)
}

fn body(runs: &[Run]) -> Value {
    serde_json::from_slice(&render(runs)).unwrap()
}
fn input_bytes(runs: &[Run]) -> Vec<u8> {
    #[derive(serde::Deserialize)]
    struct WireInput<'a> {
        #[serde(borrow)]
        input: &'a serde_json::value::RawValue,
    }
    let bytes = render(runs);
    let wire: WireInput<'_> = serde_json::from_slice(&bytes).unwrap();
    wire.input.get().as_bytes().to_vec()
}
fn update(effort: &str) -> Value {
    json!({"type": "configuration_update", "reasoning": {"effort": effort}})
}

#[test]
fn anchors_top_level_effort_to_first_episode() {
    let rows = records("gpt-6.1-sol", &["xhigh", "high"], true);
    assert_eq!(body(&rows)["reasoning"]["effort"], "xhigh");
}

#[test]
fn inserts_update_before_second_episode_user_and_preserves_first_prefix() {
    let rows = records("gpt-6.1-sol", &["xhigh", "high"], true);
    let second = body(&rows);
    assert_eq!(second["input"][1], update("high"));
    assert_eq!(second["input"][2]["role"], "user");
    assert_eq!(second["input"][2]["content"], "episode-high");
    let first = input_bytes(&rows[..1]);
    let next = input_bytes(&rows);
    assert!(
        next.starts_with(&first[..first.len() - 1]),
        "serialized episode-1 prefix changed"
    );
}

#[test]
fn return_to_initial_effort_inserts_a_second_update() {
    let rows = records("gpt-6.1-sol", &["xhigh", "high", "xhigh"], true);
    let input = body(&rows)["input"].clone();
    assert_eq!(input[1], update("high"));
    assert_eq!(input[3], update("xhigh"));
    assert_eq!(input[4]["role"], "user");
}

#[test]
fn earlier_update_remains_in_serialized_prefix_when_third_effort_is_unchanged() {
    let rows = records("gpt-6.1-sol", &["xhigh", "high", "high"], true);
    let second = input_bytes(&rows[..2]);
    let third = input_bytes(&rows);
    assert!(
        third.starts_with(&second[..second.len() - 1]),
        "request 2 input bytes (including its update) changed in request 3"
    );
    let input = body(&rows)["input"].clone();
    assert_eq!(
        input
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "configuration_update")
            .count(),
        1
    );
    assert_eq!(input[3]["role"], "user");
}

#[test]
fn unsupported_models_keep_request_level_effort() {
    for model in ["gpt-5.6", "gpt-5.6-sol", "gpt-60", "gpt-7"] {
        let rendered = body(&records(model, &["xhigh", "high"], true));
        assert_eq!(rendered["reasoning"]["effort"], "high");
        assert_eq!(rendered["input"].as_array().unwrap().len(), 2);
    }
}

#[test]
fn pre_change_session_and_new_append_keep_legacy_bytes() {
    let mut rows = records("gpt-6.1-sol", &["xhigh", "high"], false);
    let old = render(&rows);
    assert_eq!(old, br#"{"include":["reasoning.encrypted_content"],"input":[{"content":"episode-xhigh","role":"user","type":"message"},{"content":"episode-high","role":"user","type":"message"}],"model":"gpt-6.1-sol","reasoning":{"effort":"high","summary":"auto"},"store":false,"stream":true}"#);
    // A run started before the admission choice existed stores a config without it.
    for row in &mut rows {
        assert_eq!(row.frozen_config["chatgpt_effort_updates"], false);
        row.frozen_config
            .as_object_mut()
            .unwrap()
            .remove("chatgpt_effort_updates");
    }
    assert_eq!(render(&rows), old);
    assert_eq!(body(&rows)["reasoning"]["effort"], "high");
    rows.push(start("gpt-6.1-sol", "xhigh", true, Some(Role::User)));
    let current = render(&rows);
    rows.last_mut()
        .unwrap()
        .frozen_config
        .as_object_mut()
        .unwrap()
        .remove("chatgpt_effort_updates");
    assert_eq!(
        render(&rows),
        current,
        "new admission changed legacy session behavior"
    );
}

#[test]
fn userless_and_empty_episodes_use_stable_user_separators() {
    let mut rows = records("gpt-6.1-sol", &["xhigh"], true);
    rows.push(start("gpt-6.1-sol", "high", true, None));
    let second = input_bytes(&rows);
    rows.push(start("gpt-6.1-sol", "xhigh", true, Some(Role::Assistant)));
    let third = input_bytes(&rows);
    assert!(third.starts_with(&second[..second.len() - 1]));
    let input = body(&rows)["input"].clone();
    assert_eq!(input[1], update("high"));
    assert_eq!(input[2]["role"], "user");
    assert_eq!(input[3], update("xhigh"));
    assert_eq!(input[4]["role"], "user");
}

#[test]
fn unchanged_effort_has_identical_legacy_bytes() {
    assert_eq!(
        render(&records("gpt-6.1-sol", &["xhigh", "xhigh"], true)),
        render(&records("gpt-6.1-sol", &["xhigh", "xhigh"], false))
    );
}

#[test]
fn transformed_synthetic_rows_do_not_move_episode_updates() {
    let (frozen, opts) = request(&records("gpt-6.1-sol", &["xhigh", "high"], true));
    let mut head = opts.prompt[0].clone();
    head.ordinal = None;
    head.role = Role::System;
    let mut transformed = vec![head.clone(), head];
    transformed.extend(opts.prompt.clone());
    let rendered = OpenAiResponsesChatgpt.shape(&frozen, &opts.with_prompt(transformed));
    assert_eq!(rendered["input"][1], update("high"));
    assert_eq!(rendered["input"][2]["role"], "user");
}

#[test]
fn dropped_user_messages_still_separate_transformed_updates() {
    let (frozen, opts) = request(&records("gpt-6.1-sol", &["xhigh", "high", "xhigh"], true));
    let rendered = OpenAiResponsesChatgpt.shape(&frozen, &opts.with_prompt(vec![]));
    assert_eq!(rendered["input"][0], update("high"));
    assert_eq!(rendered["input"][1]["role"], "user");
    assert_eq!(rendered["input"][2], update("xhigh"));
    assert_eq!(rendered["input"][3]["role"], "user");
}

/// Rendering with the runs `reduce_episodes` keeps gives the same bytes as rendering with every
/// run, over generated sessions: each run's effort and whether it admitted updates vary, and a
/// compaction view cuts a varying number of leading messages (the request renders only messages
/// at or after the cut, with native ordinals). A store that seals history relies on this.
#[test]
fn reduced_episodes_render_the_same_bytes_as_every_episode() {
    let efforts = ["low", "medium", "high", "xhigh"];
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut next = |bound: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % bound
    };
    let mut reduced_any = 0;
    for _ in 0..300 {
        let runs = 1 + next(12) as usize;
        let mut rows = Vec::new();
        let mut effort = efforts[next(4) as usize];
        for run in 0..runs {
            // Mostly the same effort as the run before, as real sessions are.
            if run > 0 && next(4) == 0 {
                effort = efforts[next(4) as usize];
            }
            let admitted = next(10) != 0;
            rows.push(start(
                "gpt-6.1-sol",
                effort,
                admitted,
                (next(5) != 0).then_some(Role::User),
            ));
        }
        let (frozen, opts) = request(&rows);
        let mut history = RenderHistory::clone(&opts.render_episodes);
        history.raw_ordinals = true;
        let prompt = opts.prompt.clone();
        let first_kept = next(prompt.len() as u64 + 1) as usize;
        let kept: Vec<Message> = prompt[first_kept..].to_vec();
        let mut whole = CallOptions::new(kept.clone());
        whole.render_episodes = Arc::new(history.clone());
        let reduced_history = broca_provider::families::openai_responses_chatgpt::reduce_episodes(
            &history, first_kept,
        );
        reduced_any += usize::from(reduced_history.episodes.len() < history.episodes.len());
        let mut reduced = CallOptions::new(kept);
        reduced.render_episodes = Arc::new(reduced_history);
        assert_eq!(
            OpenAiResponsesChatgpt.render(&frozen, &reduced),
            OpenAiResponsesChatgpt.render(&frozen, &whole),
            "{runs} runs, cut before {first_kept}"
        );
    }
    // The generated sessions do drop runs, so the check isn't passing on unreduced lists alone.
    assert!(
        reduced_any > 50,
        "only {reduced_any} sessions dropped a run"
    );
}
