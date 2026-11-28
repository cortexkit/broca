use super::*;
use broca_provider::{families::AnthropicMessages, wire::WireFamily};

// An omitted echo scope once meant that only the last assistant reply's thinking was echoed.
const LEGACY: &str = r#"{
    "family_id":"anthropic_messages",
    "renderer_version":2,
    "wire_model_id":"claude-opus-4-8",
    "max_output_tokens":2048,
    "reasoning":{"mode":"anthropic_interleaved","budget_tokens":1024,"last_turn_only":true},
    "tool_id":"passthrough",
    "system_prompt":{"mode":"required_identity_lead","identity":"You are Claude Code, Anthropic's official CLI for Claude."},
    "usage":"anthropic_usage",
    "cache":{"version":1,"class":"markers","syntax":"anthropic_cache_control","ttl":"1h","max_breakpoints":4,"lookback_blocks":20},
    "token_limit_field":"max_tokens"
}"#;

tokio::task_local! { static LEGACY_RUN: (); }
tokio::task_local! { static ECHO_DISABLED: (); }

pub(crate) fn legacy_echo_blob() -> Option<Value> {
    LEGACY_RUN
        .try_with(|_| {
            let mut blob: Value = serde_json::from_str(LEGACY).unwrap();
            if ECHO_DISABLED.try_with(|_| ()).is_ok() {
                blob["reasoning"]["last_turn_only"] = false.into();
            }
            blob
        })
        .ok()
}

fn route() -> (ModelHandle, FrozenRenderConfig) {
    (
        ModelHandle::new("anthropic", "claude-opus-4-8"),
        serde_json::from_str(LEGACY).unwrap(),
    )
}

#[tokio::test]
async fn legacy_echo_unfinished_run_is_interrupted_without_a_provider_send() {
    ROUTE.scope(route(), LEGACY_RUN.scope((), Box::pin(async {
        let scenario = reader_scenario(vec![text_step("a1", "original answer")]);
        let old = new_source(&scenario, None, scenario.steps.clone()).await;
        let through = old.records.iter().position(|record| matches!(record, Record::StepStart(_))).unwrap();
        let start = Start { records: old.records[..=through].to_vec(), parts: old.parts, snapshot: None, converted: false };
        let resumed = new_source(&scenario, Some(start), vec![text_step("not sent", "not sent")]).await;
        assert!(resumed.outcome.requests.is_empty());
        assert!(resumed.records.iter().any(|record| matches!(record, Record::RunEnd(end)
            if end.error.as_ref().is_some_and(|error| error.parse()["provider_code"] == "frozen_render_config_unverifiable"))));
    }))).await;
}

#[tokio::test]
async fn legacy_echo_recorded_request_is_not_rebuilt_with_new_bytes() {
    let (model, frozen) = route();
    ROUTE.scope((model, frozen.clone()), Box::pin(async {
        let original = reader_scenario(vec![attempted(text_step("a1", "a legacy answer"), vec![attempt(1, 200, "complete")])]);
        let old = LEGACY_RUN.scope((), Box::pin(new_source(&original, None, original.steps.clone()))).await;
        let recorded = historical::session_at(&old.records).await;
        let parts = MemoryParts { parts: old.parts.clone() };
        let replay = broca_session_loop::recorded_request::rebuild(
            &recorded, &recorded.open_log_records().await.unwrap(), &parts, &bind(), "r1", Some(1),
            broca_session_loop::recorded_request::Through::Request,
        ).await;
        assert!(matches!(replay, Err(ref error) if error.contains("retired last-assistant thinking echo")));
    })).await;
}

#[tokio::test]
async fn legacy_echo_finished_history_allows_a_fresh_run_without_earlier_signatures() {
    let (model, mut frozen) = route();
    frozen.renderer_version = 4;
    ROUTE
        .scope(
            (model, frozen.clone()),
            Box::pin(async {
                let signed = |signature: &str| ContentBlock::Reasoning {
                    text: "legacy thinking".into(),
                    signature: Some(signature.into()),
                };
                let original = reader_scenario(vec![
                    answer(
                        "a1",
                        vec![signed("first-old-signature"), call("c1", "read", json!({}))],
                        FinishReason::ToolCalls,
                    ),
                    answer(
                        "a2",
                        vec![call("c2", "read", json!({}))],
                        FinishReason::ToolCalls,
                    ),
                    answer(
                        "a3",
                        vec![
                            signed("last-old-signature"),
                            ContentBlock::Text {
                                text: "a legacy answer".into(),
                            },
                        ],
                        FinishReason::Stop,
                    ),
                ]);
                let old = LEGACY_RUN
                    .scope(
                        (),
                        Box::pin(new_source(&original, None, original.steps.clone())),
                    )
                    .await;
                let next = FRESH_RUN
                    .scope(
                        "r2",
                        Box::pin(new_source(
                            &original,
                            Some(Start {
                                records: old.records,
                                parts: old.parts,
                                snapshot: None,
                                converted: false,
                            }),
                            vec![text_step("a4", "new answer")],
                        )),
                    )
                    .await;
                assert_eq!(next.outcome.requests.len(), 1);
                let request = &next.outcome.requests[0];
                assert_eq!(
                    request.render_episodes.first().unwrap().frozen_config["reasoning"]
                        .get("echo_scope"),
                    None
                );
                assert_eq!(
                    request.render_episodes.last().unwrap().frozen_config["reasoning"]
                        ["echo_scope"],
                    "all_assistant_messages"
                );
                let sent = AnthropicMessages.shape(&frozen, request)["messages"].to_string();
                assert!(
                    sent.contains("a legacy answer")
                        && !sent.contains("first-old-signature")
                        && !sent.contains("last-old-signature")
                );
            }),
        )
        .await;
}

#[tokio::test]
async fn legacy_echo_marker_is_irrelevant_when_echo_was_disabled() {
    let (model, mut frozen) = route();
    let broca_provider::policy::ReasoningPolicy::AnthropicInterleaved { last_turn_only, .. } =
        &mut frozen.reasoning
    else {
        panic!("fixture reasoning")
    };
    *last_turn_only = false;
    ROUTE
        .scope(
            (model, frozen),
            Box::pin(async {
                let scenario = reader_scenario(vec![text_step("answer", "done")]);
                let old = ECHO_DISABLED
                    .scope(
                        (),
                        LEGACY_RUN.scope(
                            (),
                            Box::pin(new_source(&scenario, None, scenario.steps.clone())),
                        ),
                    )
                    .await;
                let through = old
                    .records
                    .iter()
                    .position(|record| matches!(record, Record::StepStart(_)))
                    .unwrap();
                let start = Start {
                    records: old.records[..=through].to_vec(),
                    parts: old.parts,
                    snapshot: None,
                    converted: false,
                };
                let resumed = new_source(&scenario, Some(start), scenario.steps.clone()).await;
                assert_eq!(resumed.outcome.requests.len(), 1);
                assert!(resumed
                    .records
                    .iter()
                    .all(|record| !matches!(record, Record::ConfigRefrozen(_))));
            }),
        )
        .await;
}

#[tokio::test]
async fn legacy_echo_offline_next_media_preview_does_not_reproduce_retired_bytes() {
    ROUTE.scope(route(), LEGACY_RUN.scope((), Box::pin(async {
        let scenario = reader_scenario(vec![text_step("reply", "done")]);
        let old = new_source(&scenario, None, scenario.steps.clone()).await;
        let session = historical::session_at(&old.records).await;
        let result = broca_session_loop::run::offline_next_media_request(&session, &MemoryParts { parts: old.parts }, &bind(), 1).await;
        assert!(matches!(result, Err(broca_session_loop::run::LoopError::Unsupported(reason)) if reason.contains("retired last-assistant")));
    }))).await;
}

#[tokio::test]
async fn a_signed_0149_run_after_legacy_keeps_its_resume_bytes_but_new_v4_strips_once() {
    let (model, old) = route();
    let sign = |id: &str, sig: &str| {
        answer(
            id,
            vec![
                ContentBlock::Reasoning {
                    text: "signed history".into(),
                    signature: Some(sig.into()),
                },
                ContentBlock::Text { text: id.into() },
            ],
            FinishReason::Stop,
        )
    };
    let scenario = reader_scenario(vec![sign("legacy-one", "legacy-covered-prefix")]);
    let prior = ROUTE
        .scope(
            (model.clone(), old.clone()),
            LEGACY_RUN.scope(
                (),
                Box::pin(new_source(&scenario, None, scenario.steps.clone())),
            ),
        )
        .await;
    let all = ROUTE
        .scope(
            (model.clone(), old.clone()),
            FRESH_RUN.scope(
                "r2",
                Box::pin(new_source(
                    &scenario,
                    Some(Start {
                        records: prior.records,
                        parts: prior.parts,
                        snapshot: None,
                        converted: false,
                    }),
                    vec![sign("old-all", "old-all-signature")],
                )),
            ),
        )
        .await;
    let includes_old = |request: &CallOptions| {
        AnthropicMessages.shape(&old, request)["messages"]
            .to_string()
            .contains("legacy-covered-prefix")
    };
    assert!(includes_old(&all.outcome.requests[0]));
    let cut = all
        .records
        .iter()
        .position(|record| matches!(record, Record::StepStart(step) if step.run_id == "r2"))
        .unwrap();
    let resumed = ROUTE
        .scope(
            (model.clone(), old.clone()),
            Box::pin(new_source(
                &scenario,
                Some(Start {
                    records: all.records[..=cut].to_vec(),
                    parts: all.parts.clone(),
                    snapshot: None,
                    converted: false,
                }),
                vec![text_step("retry", "done")],
            )),
        )
        .await;
    assert_eq!(
        AnthropicMessages.render(&old, &resumed.outcome.requests[0]),
        AnthropicMessages.render(&old, &all.outcome.requests[0])
    );
    let mut new = old.clone();
    new.renderer_version = 4;
    let after = ROUTE
        .scope(
            (model, new),
            FRESH_RUN.scope(
                "r3",
                Box::pin(new_source(
                    &scenario,
                    Some(Start {
                        records: all.records,
                        parts: all.parts,
                        snapshot: None,
                        converted: false,
                    }),
                    vec![text_step("new", "done")],
                )),
            ),
        )
        .await;
    let body = AnthropicMessages
        .shape(&old, &after.outcome.requests[0])
        .to_string();
    // Exact v2 replay keeps the sent bytes. The first v4 request cuts at its own run start.
    assert_eq!(
        after.outcome.requests[0].render_episodes.signed_floor,
        Some(5)
    );
    assert!(
        !body.contains("legacy-covered-prefix") && !body.contains("old-all-signature"),
        "{body}"
    );
}

#[tokio::test]
async fn writer_preview_uses_the_first_v4_runs_legacy_transition_floor() {
    ROUTE
        .scope(
            route(),
            LEGACY_RUN.scope(
                (),
                Box::pin(async {
                    let scenario = reader_scenario(vec![answer(
                        "thinking",
                        vec![ContentBlock::Reasoning {
                            text: "only visible when echoed".into(),
                            signature: Some("legacy-preview-signature".into()),
                        }],
                        FinishReason::Stop,
                    )]);
                    let history = new_source(&scenario, None, scenario.steps.clone()).await;
                    let mut records = history.records.clone();
                    let mut delivery = writer_start(&history, "anthropic_tool_addition", 1);
                    records.push(delivery.records.pop().unwrap());
                    let session = historical::session_at(&records).await;
                    let visible = broca_session_loop::run::next_note_anchors(
                        &session,
                        &MemoryParts {
                            parts: history.parts,
                        },
                        &bind(),
                    )
                    .await
                    .unwrap();
                    assert!(
                        !visible.contains(&1),
                        "the future captured floor hides the thinking-only successor"
                    );
                }),
            ),
        )
        .await;
}
