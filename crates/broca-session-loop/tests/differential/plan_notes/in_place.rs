use super::*;
use broca_provider::{
    families::{anthropic_apikey_frozen_config, AnthropicMessages},
    wire::WireFamily,
};

fn claude() -> (ModelHandle, FrozenRenderConfig) {
    let mut frozen = anthropic_apikey_frozen_config("claude-opus-5-5");
    frozen.anthropic_turn_system = true;
    frozen.cache = None;
    (ModelHandle::new("anthropic", "claude-opus-5-5"), frozen)
}

fn note(body: &Value) -> (usize, Vec<u8>) {
    let (index, note) = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .find(|(_, message)| {
            message["role"] == "system" && message["content"].to_string().contains("tool_addition")
        })
        .expect("the native note must remain in messages");
    (index, serde_json::to_vec(note).unwrap())
}

#[tokio::test]
async fn a_note_keeps_the_same_bytes_and_index_within_and_between_runs() {
    let (model, frozen) = claude();
    ROUTE
        .scope(
            (model, frozen.clone()),
            Box::pin(async {
                let scenario = reader_scenario(vec![
                    answer(
                        "a1",
                        vec![call("read-first", "read", json!({}))],
                        FinishReason::ToolCalls,
                    ),
                    text_step("a2", "done"),
                ]);
                let base = BUDGET
                    .scope(
                        Some(budgeted(&[], Some(1))),
                        Box::pin(new_source(&scenario, None, scenario.steps.clone())),
                    )
                    .await;
                let first = BUDGET
                    .scope(
                        Some(budgeted(&[], Some(1))),
                        Box::pin(new_source(
                            &scenario,
                            Some(writer_start(&base, "anthropic_tool_addition", 1)),
                            scenario.steps.clone(),
                        )),
                    )
                    .await;
                let wire = |opts: &CallOptions| AnthropicMessages.shape(&frozen, opts);
                let original = note(&wire(&first.outcome.requests[0]));
                assert_eq!(
                    wire(&first.outcome.requests[1])["tool_choice"],
                    json!({"type":"none"})
                );
                assert_eq!(note(&wire(&first.outcome.requests[1])), original);
                let start = Start {
                    records: first.records,
                    parts: first.parts,
                    snapshot: None,
                    converted: false,
                };
                let next = FRESH_RUN
                    .scope(
                        "r2",
                        Box::pin(new_source(
                            &scenario,
                            Some(start),
                            vec![text_step("a3", "still noted")],
                        )),
                    )
                    .await;
                assert_eq!(note(&wire(&next.outcome.requests[0])), original);
                assert_eq!(
                    next.records
                        .iter()
                        .filter(|record| matches!(record, Record::PlanNotes(_)))
                        .count(),
                    1
                );
            }),
        )
        .await;
}

#[tokio::test]
async fn server_default_fallback_stays_on_with_notes() {
    use broca_provider::wire::RefusalFallbackConfig;
    for (target, supported, addition) in [
        ("claude-fable-5-2", true, true),
        ("claude-opus-5-5", false, true),
        ("claude-opus-4-5", true, false),
    ] {
        let (model, mut frozen) = claude();
        frozen.server_side_fallback = true;
        let mut candidate = anthropic_apikey_frozen_config(target);
        candidate.anthropic_turn_system = supported;
        frozen.refusal_fallback = Some(Box::new(RefusalFallbackConfig {
            model: ModelHandle::new("anthropic", target),
            context_limit: Some(100_000),
            frozen: candidate,
        }));
        ROUTE
            .scope(
                (model, frozen.clone()),
                Box::pin(async {
                    let scenario = reader_scenario(vec![text_step("a1", "done")]);
                    let base = new_source(&scenario, None, scenario.steps.clone()).await;
                    let wire = |opts: &CallOptions| AnthropicMessages.shape(&frozen, opts);
                    assert_eq!(wire(&base.outcome.requests[0])["fallbacks"], "default");
                    let mut start = writer_start(&base, "anthropic_tool_addition", 1);
                    if !addition {
                        let Record::PlanNotes(delivery) = start.records.last_mut().unwrap() else {
                            panic!("note delivery")
                        };
                        delivery.notes = vec![PlanNote {
                            kind: PlanNoteKind::System,
                            text: NOTE.into(),
                            form: "turn_system".into(),
                            tool: None,
                            tool_dispatch: None,
                        }];
                    }
                    let sent = new_source(&scenario, Some(start), scenario.steps.clone()).await;
                    let body = wire(&sent.outcome.requests[0]);
                    assert_eq!(body["fallbacks"], "default");
                    assert_eq!(body["tools"], wire(&base.outcome.requests[0])["tools"]);
                    assert_eq!(body["messages"][1]["role"], "system");
                }),
            )
            .await;
    }
}

fn text_delivery(anchor: u64, parts: &[(PlanNoteKind, &str)]) -> Record {
    Record::PlanNotes(PlanNotes {
        blocks: Some(true),
        generation: 1,
        anchor,
        refused_tools: vec![],
        model_key: "anthropic:claude-opus-5-5".into(),
        notes: parts
            .iter()
            .map(|(kind, text)| PlanNote {
                kind: *kind,
                text: (*text).into(),
                form: "turn_system".into(),
                tool: None,
                tool_dispatch: None,
            })
            .collect(),
    })
}

#[tokio::test]
async fn claude_projects_text_and_removal_in_one_system_message_without_rewriting_the_prefix() {
    let (model, frozen) = claude();
    ROUTE.scope((model, frozen.clone()), Box::pin(async {
        let mut scenario = reader_scenario(vec![text_step("a1", "ready")]);
        scenario.input.insert(0, Message::new(Role::System, vec![ContentBlock::Text { text: "Stable plan".into() }]));
        let base = new_source(&scenario, None, scenario.steps.clone()).await;
        let mut start = writer_start(&base, "anthropic_tool_addition", 2);
        start.records.pop();
        let removal = broca_core::plan_notes::tool_removal("read");
        start.records.push(text_delivery(2, &[(PlanNoteKind::System, NOTE), (PlanNoteKind::ToolRemoval, &removal)]));
        let noted = new_source(&scenario, Some(start), scenario.steps.clone()).await;
        let before = AnthropicMessages.shape(&frozen, &base.outcome.requests[0]);
        let after = AnthropicMessages.shape(&frozen, &noted.outcome.requests[0]);
        assert_eq!(after["system"], before["system"]);
        assert_eq!(after["messages"][1], json!({"role":"system", "content":[{"type":"text", "text":NOTE}, {"type":"text", "text": removal}]}));
    })).await;
}

#[tokio::test]
async fn a_view_that_hides_the_reply_never_moves_a_note_to_a_different_position() {
    view_hides_reply(false, false).await;
}

#[tokio::test]
async fn failed_optional_note_estimate_does_not_fail_the_compacted_send() {
    view_hides_reply(true, false).await;
}

#[tokio::test]
async fn restoring_an_anchor_renders_its_original_and_not_the_redelivered_copy() {
    view_hides_reply(false, true).await;
}

async fn view_hides_reply(fail_estimate: bool, restore: bool) {
    use broca_core::compaction_v2::received::ReceivedView;
    use cortexkit_role_compaction::view::{Edit, EditOp, View};
    let (model, frozen) = claude();
    ROUTE.scope((model, frozen.clone()), LAYOUT.scope(Some(cortexkit_compaction_view::PLAN_VERSION), COMPACTION.scope(Some(Arc::new(CompactionScript { context_limit: 100_000, rides: vec![] })), Box::pin(async {
        let scenario = compacted_scenario(vec![attempted(text_step("a1", "reply removed by the view"), vec![attempt(1, 200, "complete")])]);
        let base = new_source(&scenario, None, scenario.steps.clone()).await;
        let mut start = writer_start(&base, "anthropic_tool_addition", 1);
        start.records.pop();
        start.records.push(text_delivery(1, &[(PlanNoteKind::System, NOTE)]));
        let first = new_source(&scenario, Some(start), scenario.steps.clone()).await;
        let wire = |opts: &CallOptions| AnthropicMessages.shape(&frozen, opts);
        let original = wire(&first.outcome.requests[0]);
        assert_eq!(original["messages"][1]["content"][0]["text"], NOTE);
        let reply = first.records.iter().find_map(|record| match record {
            Record::EntryFacts(entry) if entry.ordinal == 2 => Some(entry.mid.clone()), _ => None,
        }).unwrap();
        let view = ReceivedView::from(View::seal(1, vec![Edit::new(1, EditOp::Drop { mid: reply, part: None })]));
        let mut records = first.records;
        records.push(broca_session_loop::compaction::view_record(None, &view, None).unwrap());
        records.push(Record::ViewApplied(broca_session_store::record::ViewApplied {
            step_id: 1, version: view.view().version, checksum: view.view().checksum.to_string(), head_kept: Some(true),
        }));
        let run = FRESH_RUN.scope("r2", Box::pin(new_source(&scenario, Some(Start {
            records, parts: first.parts, snapshot: None, converted: false,
        }), vec![text_step("a2", "accepted without the erased note")])));
        let next = if fail_estimate {
            NOTE_ESTIMATE_READ_FAILURE.scope(std::cell::Cell::new(false), Box::pin(async {
                let next = run.await;
                assert!(NOTE_ESTIMATE_READ_FAILURE.with(std::cell::Cell::get));
                next
            })).await
        } else { run.await };
        let sent = wire(&next.outcome.requests[0]);
        assert_eq!(sent["system"], original["system"]);
        assert!(!sent.to_string().contains(NOTE));
        assert!(next.records.iter().any(|record| matches!(record, Record::ViewApplied(view) if view.version == 1)));
        if restore {
            let revived = ReceivedView::from(View::seal(2, vec![]));
            let mut records = next.records;
            records.push(text_delivery(3, &[(PlanNoteKind::System, NOTE)]));
            records.push(broca_session_loop::compaction::view_record(Some(&view), &revived, None).unwrap());
            let next = FRESH_RUN.scope("r3", Box::pin(new_source(&scenario, Some(Start {
                records, parts: next.parts, snapshot: None, converted: false,
            }), vec![text_step("a3", "restored")]))).await;
            let sent = wire(&next.outcome.requests[0]);
            assert_eq!(sent["messages"][1], original["messages"][1]);
            assert_eq!(sent.to_string().matches(NOTE).count(), 1);
            assert!(sent.to_string().contains("reply removed by the view"));
        }
    })))).await;
}

#[tokio::test]
async fn fallback_projection_uses_its_own_reply_visibility() {
    use broca_provider::policy::{CrossModelThinking, ReasoningPolicy};
    let (model, mut frozen, fallback) =
        historical::fallback_config("claude-opus-5-5", "claude-opus-4-8");
    frozen.anthropic_turn_system = true;
    frozen.reasoning = ReasoningPolicy::AnthropicInterleaved {
        budget_tokens: 1024,
        last_turn_only: true,
        echo_scope: broca_provider::policy::ThinkingEchoScope::AllAssistantMessages,
        cross_model: CrossModelThinking::SameModelOnly,
    };
    let target = frozen.refusal_fallback.as_mut().unwrap();
    target.frozen.anthropic_turn_system = true;
    target.frozen.reasoning = frozen.reasoning.clone();
    ROUTE
        .scope(
            (model, frozen.clone()),
            FALLBACK.scope(
                Some(fallback),
                Box::pin(async {
                    let script = reader_scenario(vec![answer(
                        "thinking",
                        vec![ContentBlock::Reasoning {
                            text: "visible only to the source".into(),
                            signature: Some("source-signature".into()),
                        }],
                        FinishReason::Stop,
                    )]);
                    let base = new_source(&script, None, script.steps.clone()).await;
                    let first = new_source(
                        &script,
                        Some(writer_start(&base, "anthropic_tool_addition", 1)),
                        script.steps.clone(),
                    )
                    .await;
                    let next = FRESH_RUN
                        .scope(
                            "r2",
                            Box::pin(new_source(
                                &script,
                                Some(Start {
                                    records: first.records,
                                    parts: first.parts,
                                    snapshot: None,
                                    converted: false,
                                }),
                                vec![refusal("source refused"), text_step("fallback", "accepted")],
                            )),
                        )
                        .await;
                    assert_eq!(next.outcome.requests.len(), 2);
                    assert!(next.outcome.requests[0]
                        .prompt
                        .iter()
                        .any(|message| message.tool_addition.is_some()));
                    assert!(next.outcome.requests[1]
                        .prompt
                        .iter()
                        .all(|message| message.tool_addition.is_none()));
                    assert_eq!(
                        next.outcome.requests[1].target_model.as_ref(),
                        Some(&frozen.refusal_fallback.as_ref().unwrap().model)
                    );
                }),
            ),
        )
        .await;
}

#[tokio::test]
async fn a_restore_cannot_keep_thinking_signed_behind_the_live_copy() {
    check_signed_restore(false).await;
}

#[tokio::test]
async fn a_wire_neutral_restore_keeps_the_copy_and_is_applied() {
    check_signed_restore(true).await;
}

async fn check_signed_restore(wire_neutral: bool) {
    use broca_core::compaction_v2::received::ReceivedView;
    use cortexkit_role_compaction::view::{Edit, EditOp, View};
    let (model, mut frozen) = claude();
    frozen.renderer_version = 4;
    frozen.reasoning =
        broca_provider::families::anthropic_frozen_config("claude-opus-5-5").reasoning;
    ROUTE.scope((model, frozen.clone()), LAYOUT.scope(Some(cortexkit_compaction_view::PLAN_VERSION), COMPACTION.scope(Some(Arc::new(CompactionScript { context_limit: 100_000, rides: vec![] })), Box::pin(async {
        let previous = if wire_neutral {
            answer("a1", vec![ContentBlock::Reasoning { text: "foreign thinking".into(), signature: Some("foreign".into()) }], FinishReason::Stop)
        } else { text_step("a1", "erased answer") };
        let scenario = compacted_scenario(vec![previous]);
        let base = new_source(&scenario, None, scenario.steps.clone()).await;
        let mut initial = writer_start(&base, "anthropic_tool_addition", 1);
        initial.records.pop(); initial.records.push(text_delivery(1, &[(PlanNoteKind::System, NOTE)]));
        let first = new_source(&scenario, Some(initial), scenario.steps.clone()).await;
        let reply = first.records.iter().find_map(|record| match record { Record::EntryFacts(entry) if entry.ordinal == 2 => Some(entry.mid.clone()), _ => None }).unwrap();
        let hidden = ReceivedView::from(View::seal(1, vec![Edit::new(1, EditOp::Drop { mid: reply, part: None })]));
        let mut records = first.records;
        if wire_neutral {
            for record in &mut records {
                if let Record::Assistant(assistant) = record {
                    Arc::make_mut(&mut assistant.message).origin = Some(ModelHandle::new("google", "gemini"));
                }
            }
        }
        records.push(broca_session_loop::compaction::view_record(None, &hidden, None).unwrap());
        records.push(Record::ViewApplied(broca_session_store::record::ViewApplied { step_id: 1, version: 1, checksum: hidden.view().checksum.to_string(), head_kept: Some(true) }));
        records.push(text_delivery(3, &[(PlanNoteKind::System, NOTE)]));
        let signed = attempted(answer("a2", vec![ContentBlock::Reasoning { text: "behind copy".into(), signature: Some("signed-behind-copy".into()) }, ContentBlock::Text { text: "reply to copy".into() }], FinishReason::Stop), vec![attempt(1, 200, "complete")]);
        let second = FRESH_RUN.scope("r2", Box::pin(new_source(&scenario, Some(Start { records, parts: first.parts, snapshot: None, converted: false }), vec![signed]))).await;
        assert_eq!(second.outcome.requests[0].prompt.last().unwrap().role, Role::System);
        let restored = ReceivedView::from(View::seal(2, vec![]));
        let mut records = second.records;
        records.push(broca_session_loop::compaction::view_record(Some(&hidden), &restored, None).unwrap());
        let last = FRESH_RUN.scope("r3", Box::pin(new_source(&scenario, Some(Start { records, parts: second.parts, snapshot: None, converted: false }), vec![text_step("a3", "done")]))).await;
        let sent = AnthropicMessages.shape(&frozen, &last.outcome.requests[0]).to_string();
        let applied = last.records.iter().any(|record| matches!(record, Record::ViewApplied(view) if view.version == 2));
        assert_eq!(applied, wire_neutral, "{sent}");
        assert!(sent.contains("signed-behind-copy"));
        if wire_neutral {
            assert!(!sent.contains("foreign thinking"));
        } else {
            assert!(last.outcome.calls.iter().any(|call| call.contains("signed_part_invalid")), "{:?}", last.outcome.calls);
        }
    })))).await;
}
