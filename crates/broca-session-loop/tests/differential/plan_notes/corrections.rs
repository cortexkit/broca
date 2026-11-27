use super::*;

async fn repeat_change(notes: [PlanNote; 3], remove_again: bool) -> Source {
    let model = ModelHandle::new("openai", "gpt-6-luna");
    let route = (
        model,
        broca_provider::families::openai_responses_frozen_config("gpt-6-luna"),
    );
    ROUTE
        .scope(
            route,
            Box::pin(async {
                let first = reader_scenario(vec![text_step("first", "ready")]);
                let a = new_source(&first, None, first.steps.clone()).await;
                let b = FRESH_RUN
                    .scope(
                        "r2",
                        Box::pin(new_source(
                            &first,
                            Some(Start {
                                records: a.records,
                                parts: a.parts,
                                snapshot: None,
                                converted: false,
                            }),
                            first.steps.clone(),
                        )),
                    )
                    .await;
                let script = if remove_again {
                    reader_scenario(vec![
                        answer(
                            "called",
                            vec![call("tool-call", "read", json!({}))],
                            FinishReason::ToolCalls,
                        ),
                        text_step("end", "done"),
                    ])
                } else {
                    reader_scenario(vec![text_step("end", "done")])
                };
                let c = FRESH_RUN
                    .scope(
                        "r3",
                        Box::pin(new_source(
                            &script,
                            Some(Start {
                                records: b.records,
                                parts: b.parts,
                                snapshot: None,
                                converted: false,
                            }),
                            script.steps.clone(),
                        )),
                    )
                    .await;
                let step = c
                    .records
                    .iter()
                    .position(
                        |record| matches!(record, Record::StepStart(step) if step.run_id == "r3"),
                    )
                    .unwrap();
                let mut records = c.records[..step].to_vec();
                let deliveries: Vec<_> = notes
                    .into_iter()
                    .enumerate()
                    .map(|(i, note)| {
                        Record::PlanNotes(PlanNotes {
                            blocks: Some(true),
                            generation: i as u64 + 1,
                            anchor: i as u64 * 2 + 1,
                            refused_tools: if note.kind == PlanNoteKind::ToolRemoval {
                                vec!["read".into()]
                            } else {
                                vec![]
                            },
                            notes: vec![note],
                            model_key: "openai:gpt-6-luna".into(),
                        })
                    })
                    .collect();
                records.extend(deliveries);
                new_source(
                    &script,
                    Some(Start {
                        records,
                        parts: c.parts,
                        snapshot: None,
                        converted: false,
                    }),
                    script.steps.clone(),
                )
                .await
            }),
        )
        .await
}

fn note(kind: PlanNoteKind, text: String) -> PlanNote {
    PlanNote {
        kind,
        text,
        form: "turn_system".into(),
        tool: None,
        tool_dispatch: None,
    }
}

fn rendered_text(source: &Source) -> Vec<String> {
    source.outcome.requests[0]
        .prompt
        .iter()
        .filter(|message| message.turn_system)
        .map(text_of)
        .collect()
}

#[tokio::test]
async fn a_marked_removal_after_restoration_is_visible_and_refuses_calls() {
    let removal = broca_core::plan_notes::tool_removal("read");
    let restoration = "Tool \"read\" is now available.".to_owned();
    let last = repeat_change(
        [
            note(PlanNoteKind::ToolRemoval, removal.clone()),
            note(PlanNoteKind::ToolRestored, restoration.clone()),
            note(PlanNoteKind::ToolRemoval, removal.clone()),
        ],
        true,
    )
    .await;
    assert_eq!(
        rendered_text(&last),
        [removal.clone(), restoration, removal]
    );
    assert!(!last
        .records
        .iter()
        .any(|record| matches!(record, Record::ToolIntent(intent) if intent.run_id == "r3")));
    assert!(last.records.iter().any(|record| matches!(record, Record::ToolReply(reply) if reply.result.tool_call_id.0 == "tool-call" && reply.result.is_error)));
}

#[tokio::test]
async fn a_marked_system_correction_can_repeat_an_earlier_note_after_an_undo() {
    let a = "Answer as a reviewer.".to_owned();
    let b = "Answer as a worker.".to_owned();
    let last = repeat_change(
        [
            note(PlanNoteKind::System, a.clone()),
            note(PlanNoteKind::System, b.clone()),
            note(PlanNoteKind::System, a.clone()),
        ],
        false,
    )
    .await;
    assert_eq!(rendered_text(&last), [a.clone(), b, a]);
}
