use super::*;
use broca_session_store::record::{PlanNote, PlanNoteKind, PlanNotes};

const NOTE: &str = "Answer in French from now on.";

/// A run whose start delivered plan notes (llm-runner `plan_notes`): its requests show the
/// system note right after the run's input, at system authority, and a call to the tool the
/// pending change removes is refused with the removal wording instead of going out. The run is
/// resumed from its stored start, so this also shows that the notes and the refusal come back
/// from the session's records.
#[tokio::test]
async fn delivered_notes_render_after_the_input_and_gate_the_removed_tool() {
    plan_note_gate_after_resume(false).await;
}

#[tokio::test]
async fn a_restoration_after_a_removal_allows_the_tool_on_resume() {
    plan_note_gate_after_resume(true).await;
}

async fn plan_note_gate_after_resume(restore: bool) {
    let scenario = Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Sequential)],
        manifest: None,
        stop_when: vec![StopCondition::StepCount { n: 8 }],
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "read", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    };
    for layout in LAYOUTS {
        let scenario = scenario.clone();
        LAYOUT
            .scope(
                layout,
                Box::pin(async move {
                    let source = new_source(&scenario, None, scenario.steps.clone()).await;
                    // The run's start batch, without anything after it: the run is resumed
                    // from there with the notes its start delivered.
                    let started = source
                        .records
                        .iter()
                        .position(|record| matches!(record, Record::StepStart(_)))
                        .expect("the run starts a step");
                    let mut start = source.cut(started);
                    let mut deliveries = vec![Record::PlanNotes(PlanNotes {
                        generation: 1,
                        anchor: 1,
                        notes: vec![
                            PlanNote {
                                kind: PlanNoteKind::System,
                                text: NOTE.into(),
                                form: "turn_system".into(),
                            },
                            PlanNote {
                                kind: PlanNoteKind::ToolRemoval,
                                text: broca_core::plan_notes::tool_removal("read"),
                                form: "turn_system".into(),
                            },
                        ],
                        refused_tools: vec!["read".into()],
                        model_key: "p:m".into(),
                    })];
                    if restore {
                        deliveries.push(Record::PlanNotes(PlanNotes {
                            generation: 2,
                            anchor: 1,
                            notes: vec![PlanNote {
                                kind: PlanNoteKind::ToolRestored,
                                text: "Tool \"read\" is now available.".into(),
                                form: "turn_system".into(),
                            }],
                            refused_tools: Vec::new(),
                            model_key: "p:m".into(),
                        }));
                    }
                    {
                        let mut session = Session::load(
                            start.snapshot.as_ref().expect("the cut has a log"),
                            SessionAddr("0123456789abcdef".into()),
                            &SessionTriple {
                                project_root: "/project".into(),
                                harness: "differential".into(),
                                session: "s1".into(),
                            },
                            &DeploymentKey::new(1, &[7; 32]),
                        )
                        .await
                        .unwrap();
                        session.append(&deliveries, true).await.unwrap();
                    }
                    start.records.extend(deliveries);
                    let resumed =
                        Box::pin(new_source(&scenario, Some(start), scenario.steps.clone())).await;
                    let requests = &resumed.outcome.requests;
                    assert_eq!(requests.len(), 2, "{layout:?}");
                    for request in requests {
                        let at = request
                            .prompt
                            .iter()
                            .position(|message| message.turn_system && text_of(message) == NOTE)
                            .unwrap_or_else(|| panic!("{layout:?}: the note renders"));
                        assert_eq!(request.prompt[at].role, Role::System);
                        assert!(
                            text_of(&request.prompt[at - 1]).ends_with("read a"),
                            "{layout:?}"
                        );
                    }
                    assert_eq!(
                        resumed
                            .records
                            .iter()
                            .any(|record| matches!(record, Record::ToolIntent(_))),
                        restore,
                        "{layout:?}: only a restored tool can be dispatched"
                    );
                    if restore {
                        for request in requests {
                            let notes = request
                                .prompt
                                .iter()
                                .filter(|message| message.turn_system)
                                .map(text_of)
                                .collect::<Vec<_>>();
                            assert_eq!(
                                notes,
                                [
                                    NOTE.to_owned(),
                                    broca_core::plan_notes::tool_removal("read"),
                                    "Tool \"read\" is now available.".to_owned(),
                                ],
                                "{layout:?}"
                            );
                        }
                        return;
                    }
                    // The removed tool never went out, and the model was told why.
                    let refusal = resumed
                        .records
                        .iter()
                        .find_map(|record| match record {
                            Record::ToolReply(reply) => Some(reply.result.clone()),
                            _ => None,
                        })
                        .expect("the call is answered");
                    assert!(refusal.is_error);
                    let ToolOutput::Error { message, .. } = &refusal.output else {
                        panic!("{layout:?}: the refusal is an error: {:?}", refusal.output);
                    };
                    assert_eq!(*message, broca_core::plan_notes::tool_removal("read"));
                }),
            )
            .await;
    }
}

fn text_of(message: &Message) -> String {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.to_string()),
            _ => None,
        })
        .collect()
}
