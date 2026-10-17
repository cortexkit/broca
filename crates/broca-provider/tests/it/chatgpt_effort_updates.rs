//! Recorded Codex effort-update choices, WAL replay, and identical serialized input prefixes.
use std::sync::Arc;

use broca_protocol::{
    caller::RunFinishReason, request::ModelHandle, BindIdentity, CallOptions, ContentBlock,
    Message, Role, RunId, Usage,
};
use broca_provider::{
    families::{
        openai_responses_chatgpt::OpenAiResponsesChatgpt, openai_responses_chatgpt_frozen_config,
    },
    policy::ReasoningPolicy,
    wire::{FrozenRenderConfig, WireFamily},
};
use broca_wal::{RunConfig, RunOrigin, WalRecord};
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

fn start(model: &str, effort: &str, admitted: bool, role: Option<Role>) -> WalRecord {
    let frozen = config(model, effort, admitted);
    WalRecord::RunStarted {
        session_class: None,
        retention: None,
        hooks: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        marker_order_version: None,
        manifest: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        ts_ms: None,
        run_id: RunId(format!("run-{effort}")),
        session: BindIdentity::new("/tmp/p", "h", "s"),
        config: Box::new(RunConfig {
            model: ModelHandle::new("chatgpt", model),
            tools: vec![].into(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: Value::Null,
            cache_tiers: Value::Null,
            context_epoch: 0,
            renderer_version: frozen.renderer_version,
            frozen_render_config: serde_json::to_value(&frozen).unwrap(),
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![],
            context_management: None,
        }),
        input: role
            .map(|role| Message {
                role,
                content: vec![ContentBlock::Text {
                    text: format!("episode-{effort}"),
                }],
                origin: None,
                ordinal: None,
                cache_prefix_blocks: None,
            })
            .into_iter()
            .collect::<Vec<_>>()
            .into(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    }
}

fn finish() -> WalRecord {
    WalRecord::RunFinished {
        ts_ms: None,
        reason: RunFinishReason::Completed,
        usage: Usage::default(),
        error: None,
        retries_used: 0,
    }
}

fn records(model: &str, efforts: &[&str], admitted: bool) -> Vec<WalRecord> {
    let mut out = Vec::new();
    for effort in efforts {
        if !out.is_empty() {
            out.push(finish());
        }
        out.push(start(model, effort, admitted, Some(Role::User)));
    }
    out
}

fn render(records: &[WalRecord]) -> Vec<u8> {
    let prior = broca_core::replay::replay(records).unwrap();
    let frozen: FrozenRenderConfig =
        serde_json::from_value(prior.run_config.unwrap().frozen_render_config).unwrap();
    let mut opts = CallOptions::new(prior.prompt);
    opts.render_episodes = Arc::new(prior.render_episodes.unwrap().into());
    OpenAiResponsesChatgpt.render(&frozen, &opts)
}

fn body(records: &[WalRecord]) -> Value {
    serde_json::from_slice(&render(records)).unwrap()
}
fn input_bytes(records: &[WalRecord]) -> Vec<u8> {
    #[derive(serde::Deserialize)]
    struct WireInput<'a> {
        #[serde(borrow)]
        input: &'a serde_json::value::RawValue,
    }
    let bytes = render(records);
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
    let second = input_bytes(&rows[..3]);
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
    // An old serialized blob lacks the additive admission choice entirely.
    for row in &mut rows {
        if let WalRecord::RunStarted { config, .. } = row {
            assert!(config
                .frozen_render_config
                .get("chatgpt_effort_updates")
                .is_none());
            config
                .frozen_render_config
                .as_object_mut()
                .unwrap()
                .remove("chatgpt_effort_updates");
        }
    }
    let decoded: Vec<WalRecord> =
        serde_json::from_slice(&serde_json::to_vec(&rows).unwrap()).unwrap();
    assert_eq!(render(&decoded), old);
    assert_eq!(body(&decoded)["reasoning"]["effort"], "high");
    rows.push(finish());
    rows.push(start("gpt-6.1-sol", "xhigh", true, Some(Role::User)));
    let current = render(&rows);
    if let WalRecord::RunStarted { config, .. } = rows.last_mut().unwrap() {
        config
            .frozen_render_config
            .as_object_mut()
            .unwrap()
            .remove("chatgpt_effort_updates");
    }
    assert_eq!(
        render(&rows),
        current,
        "new admission changed legacy session behavior"
    );
}

#[test]
fn crash_mid_second_episode_replays_identical_request_bytes() {
    let rows = records("gpt-6.1-sol", &["xhigh", "high"], true);
    let before_crash = render(&rows);
    // RunStarted is durable before a model call; a crash before its commit leaves this prefix.
    let decoded: Vec<WalRecord> =
        serde_json::from_slice(&serde_json::to_vec(&rows).unwrap()).unwrap();
    assert_eq!(render(&decoded), before_crash);
    assert_eq!(body(&decoded)["input"][1], update("high"));
}

#[test]
fn userless_and_empty_episodes_use_stable_user_separators() {
    let mut rows = records("gpt-6.1-sol", &["xhigh"], true);
    rows.push(finish());
    rows.push(start("gpt-6.1-sol", "high", true, None));
    let second = input_bytes(&rows);
    rows.push(finish());
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
    let rows = records("gpt-6.1-sol", &["xhigh", "high"], true);
    let prior = broca_core::replay::replay(&rows).unwrap();
    let frozen: FrozenRenderConfig =
        serde_json::from_value(prior.run_config.unwrap().frozen_render_config).unwrap();
    let mut opts = CallOptions::new(prior.prompt);
    opts.render_episodes = Arc::new(prior.render_episodes.unwrap().into());
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
    let rows = records("gpt-6.1-sol", &["xhigh", "high", "xhigh"], true);
    let prior = broca_core::replay::replay(&rows).unwrap();
    let frozen: FrozenRenderConfig =
        serde_json::from_value(prior.run_config.unwrap().frozen_render_config).unwrap();
    let mut opts = CallOptions::new(prior.prompt);
    opts.render_episodes = Arc::new(prior.render_episodes.unwrap().into());
    let rendered = OpenAiResponsesChatgpt.shape(&frozen, &opts.with_prompt(vec![]));
    assert_eq!(rendered["input"][0], update("high"));
    assert_eq!(rendered["input"][1]["role"], "user");
    assert_eq!(rendered["input"][2], update("xhigh"));
    assert_eq!(rendered["input"][3]["role"], "user");
}

fn checkpoint(rows: &[WalRecord]) -> broca_core::AdmissionCheckpointV1 {
    let frames = rows
        .iter()
        .enumerate()
        .map(|(index, record)| broca_wal::FramedRecord {
            seq: index as u64 + 1,
            fence: 1,
            record: record.clone(),
        })
        .collect::<Vec<_>>();
    broca_core::AdmissionCheckpointV1::from_frames(&frames).unwrap()
}

#[test]
fn unchanged_effort_store_prior_and_full_replay_render_identical_new_episode_bytes() {
    for admitted in [true, false] {
        let mut rows = records("gpt-6.1-sol", &["xhigh", "xhigh", "xhigh"], admitted);
        rows.push(finish());
        // Changes unrelated to reasoning effort must not force historical replay.
        if let WalRecord::RunStarted { config, .. } = &mut rows[2] {
            config.frozen_render_config["max_output_tokens"] = json!(1234);
        }
        let full = broca_core::replay::replay(&rows).unwrap();
        let mut prefix = broca_core::StorePrefix::empty();
        prefix.prefix = full.prompt.clone();
        prefix.watermark_seq = rows.len() as u64 - 1;
        let mut fast = broca_core::resume_state_from_store(
            &prefix,
            &rows[rows.len() - 1..],
            full.run_config.clone(),
            full.run_id.clone(),
            None,
        )
        .unwrap();
        assert!(
            fast.render_episodes.is_none(),
            "store prefix must actually lack episode history"
        );
        let durable = serde_json::to_vec(&checkpoint(&rows)).unwrap();
        let checkpoint: broca_core::AdmissionCheckpointV1 =
            serde_json::from_slice(&durable).unwrap();
        checkpoint.restore_unchanged_effort(&mut fast);
        assert_eq!(
            fast.render_episodes.as_ref().unwrap().len(),
            1,
            "only the first anchor is needed; no full history replay"
        );
        for effort in ["xhigh", "high"] {
            let frozen = config("gpt-6.1-sol", effort, true);
            let new_input = Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "new episode".into(),
                }],
            );
            let build = |prior: &broca_core::ReplayState| {
                let mut opts = CallOptions::new(prior.prompt.clone());
                opts.render_episodes = Arc::new(prior.render_episodes.clone().unwrap().into());
                Arc::make_mut(&mut opts.render_episodes).push(
                    broca_protocol::request::EpisodeRenderInput {
                        prompt_start: opts.prompt.len(),
                        first_user: Some(opts.prompt.len()),
                        frozen_config: serde_json::to_value(&frozen).unwrap(),
                    },
                );
                opts.prompt.push(new_input.clone());
                broca_protocol::content::stamp_ordinals(&mut opts.prompt);
                OpenAiResponsesChatgpt.render(&frozen, &opts)
            };
            assert_eq!(
                build(&fast),
                build(&full),
                "fast/full new-episode bytes differ at effort {effort}"
            );
        }
    }
}

#[test]
fn changed_or_unknown_checkpoint_effort_history_cannot_seed_the_fast_path() {
    for efforts in [vec!["xhigh", "high"], vec!["xhigh", "high", "xhigh"]] {
        let mut rows = records("gpt-6.1-sol", &efforts, true);
        rows.push(finish());
        let full = broca_core::replay::replay(&rows).unwrap();
        let mut prior = full.clone();
        prior.render_episodes = None;
        checkpoint(&rows).restore_unchanged_effort(&mut prior);
        assert!(
            prior.render_episodes.is_none(),
            "an earlier effort change must remain sticky"
        );
    }
    for admitted in [true, false] {
        let rows = records("gpt-6.1-sol", &["xhigh", "xhigh"], admitted);
        let mut encoded = serde_json::to_value(checkpoint(&rows)).unwrap();
        encoded.as_object_mut().unwrap().remove("effort_history");
        let old: broca_core::AdmissionCheckpointV1 = serde_json::from_value(encoded).unwrap();
        assert!(
            !old.ownership_complete(),
            "old checkpoint needs a complete WAL fold once"
        );
        let mut prior = broca_core::replay::replay(&rows).unwrap();
        prior.render_episodes = None;
        old.restore_unchanged_effort(&mut prior);
        assert!(prior.render_episodes.is_none());
    }
}
