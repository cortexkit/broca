use super::*;
use broca_session_store::record::{PlanNote, PlanNoteKind, PlanNotes};
pub(super) use conservative::apply_at_start;

const NOTE: &str = "Answer in French from now on.";

use broca_provider::wire::FrozenRenderConfig;

tokio::task_local! {
    static ROUTE: (ModelHandle, FrozenRenderConfig);
    static FRESH_RUN: &'static str;
    pub(super) static CUT_EVIDENCE_MISSING: ();
    pub(super) static SIGNING_CONFIG_READ_FAILS: ();
    pub(super) static APPLY_NOTE_READ_FAILS: ();
    pub(super) static CUTOFF_INDEX_MISSING: ();
    pub(super) static DAMAGED_AFTER_SEAL: (broca_session_store::codec::Digest, std::cell::Cell<bool>);
    pub(super) static MISSING_RENDER_CONFIG: broca_session_store::codec::Digest;
    pub(super) static RAW_RENDER: Value;
    pub(super) static NOTE_ESTIMATE_READ_FAILURE: std::cell::Cell<bool>;
}

pub(super) fn route() -> Option<(ModelHandle, FrozenRenderConfig)> {
    ROUTE.try_with(Clone::clone).ok()
}

pub(super) fn fresh_run() -> Option<&'static str> {
    FRESH_RUN.try_with(|run| *run).ok()
}

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
                        blocks: None,
                        generation: 1,
                        anchor: 1,
                        notes: vec![
                            PlanNote {
                                kind: PlanNoteKind::System,
                                text: NOTE.into(),
                                form: "turn_system".into(),
                                tool: None,
                                tool_dispatch: None,
                            },
                            PlanNote {
                                kind: PlanNoteKind::ToolRemoval,
                                text: broca_core::plan_notes::tool_removal("read"),
                                form: "turn_system".into(),
                                tool: None,
                                tool_dispatch: None,
                            },
                        ],
                        refused_tools: vec!["read".into()],
                        model_key: "p:m".into(),
                    })];
                    if restore {
                        deliveries.push(Record::PlanNotes(PlanNotes {
                            blocks: None,
                            generation: 2,
                            anchor: 1,
                            notes: vec![PlanNote {
                                kind: PlanNoteKind::ToolRestored,
                                text: "Tool \"read\" is now available.".into(),
                                form: "turn_system".into(),
                                tool: None,
                                tool_dispatch: None,
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

#[tokio::test]
async fn an_inline_tool_dispatches_only_from_its_delivery_and_resumes_with_its_frozen_route() {
    let route = (
        ModelHandle::new("openai", "gpt-6-luna"),
        broca_provider::families::openai_responses_frozen_config("gpt-6-luna"),
    );
    ROUTE.scope(route, Box::pin(inline_tool_dispatch())).await;
}

async fn inline_tool_dispatch() {
    use broca_core::capability::{AddedToolDispatch, ProviderTransport};
    use broca_protocol::tool_addition::{AdditionPart, ToolAdditionForm};
    use broca_session_store::record::PlanNoteTool;

    let scenario = Scenario {
        input: vec![user("find a destination")],
        tools: vec![tool("read", ExecutionMode::Sequential)],
        manifest: None,
        stop_when: vec![StopCondition::StepCount { n: 8 }],
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "find", json!({}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    };
    for layout in LAYOUTS {
        let scenario = scenario.clone();
        LAYOUT.scope(layout, Box::pin(async move {
            let source = new_source(&scenario, None, scenario.steps.clone()).await;
            assert!(!source.records.iter().any(|record| matches!(record, Record::ToolIntent(_))));
            let before_step = source.records.iter().position(|record| matches!(record, Record::StepStart(_))).unwrap();
            let mut records = source.records[..before_step].to_vec();
            let dispatch = AddedToolDispatch {
                module: Some("maps".into()),
                sequential: true,
                reply: None,
                provider: Some(ProviderTransport { preset: Some("travel".into()), fetched: true, declarations: Default::default() }),
                provider_reply: None,
                schema_pin: None,
            };
            let delivery = PlanNotes {
                blocks: Some(true),
                generation: 1, anchor: 1, refused_tools: Vec::new(), model_key: "p:m".into(),
                notes: vec![PlanNote {
                    kind: PlanNoteKind::ToolAddition, text: String::new(),
                    form: "responses_additional_tools".into(),
                    tool: Some(PlanNoteTool { name: "find".into(), description: "Find a destination".into(), input_schema: r#"{"type":"object"}"#.into() }),
                    tool_dispatch: Some(serde_json::to_string(&dispatch).unwrap()),
                }],
            };
            records.push(Record::PlanNotes(delivery));
            let start = Start { records, parts: source.parts.clone(), converted: true, snapshot: None };
            let resumed = Box::pin(new_source(&scenario, Some(start), scenario.steps.clone())).await;
            assert!(resumed.outcome.calls.iter().any(|call| serde_json::from_str::<String>(call).ok().as_deref() == Some("dispatch_route c1 provider=\"maps\" preset=Some(\"travel\")")), "{:?}", resumed.outcome.calls);
            assert!(resumed.records.iter().any(|record| matches!(record, Record::ToolIntent(_))));
            assert_eq!(resumed.outcome.requests.len(), 2);
            for request in &resumed.outcome.requests {
                let note_index = request.prompt.iter().position(|message| message.tool_addition.is_some()).expect("the delivery survives resume");
                let message = &request.prompt[note_index];
                assert_eq!(message.role, Role::System);
                assert!(message.turn_system);
                assert_eq!(message.ordinal, None);
                assert!(text_of(&request.prompt[note_index - 1]).ends_with("find a destination"));
                let note = message.tool_addition.as_ref().unwrap();
                assert_eq!(note.form, ToolAdditionForm::ResponsesAdditionalTools);
                assert!(matches!(&note.parts[..], [AdditionPart::Tool(tool)] if tool.name == "find" && tool.input_schema.text() == r#"{"type":"object"}"#));
                assert_eq!(request.tools.iter().map(|tool| tool.name.as_str()).collect::<Vec<_>>(), ["read"]);
            }
        })).await;
    }
}

fn writer_start(source: &Source, form: &str, anchor: u64) -> Start {
    use broca_core::capability::{AddedToolDispatch, ProviderTransport};
    use broca_session_store::record::PlanNoteTool;

    let before = source
        .records
        .iter()
        .position(|record| matches!(record, Record::StepStart(_)))
        .unwrap();
    let mut records = source.records[..before].to_vec();
    records.push(Record::PlanNotes(PlanNotes {
        blocks: Some(true),
        generation: 1,
        anchor,
        notes: vec![PlanNote {
            kind: PlanNoteKind::ToolAddition,
            text: String::new(),
            form: form.into(),
            tool: Some(PlanNoteTool {
                name: "find".into(),
                description: "Find a destination".into(),
                input_schema: r#"{"type":"object"}"#.into(),
            }),
            tool_dispatch: Some(
                serde_json::to_string(&AddedToolDispatch {
                    module: Some("maps".into()),
                    sequential: false,
                    reply: None,
                    provider_reply: None,
                    schema_pin: None,
                    provider: Some(ProviderTransport {
                        preset: Some("travel".into()),
                        fetched: true,
                        declarations: Default::default(),
                    }),
                })
                .unwrap(),
            ),
        }],
        refused_tools: Vec::new(),
        model_key: "p:m".into(),
    }));
    Start {
        records,
        parts: source.parts.clone(),
        converted: false,
        snapshot: None,
    }
}

fn reader_scenario(steps: Vec<Vec<NormalizedEvent>>) -> Scenario {
    Scenario {
        input: vec![user("Find a place")],
        tools: vec![tool("read", ExecutionMode::Sequential)],
        manifest: None,
        stop_when: vec![StopCondition::StepCount { n: 8 }],
        steps,
    }
}

#[tokio::test]
async fn finalize_keeps_native_declaration_and_disables_tool_calls() {
    let mut frozen = broca_provider::families::anthropic_frozen_config("claude-opus-5-5");
    frozen.anthropic_turn_system = true;
    ROUTE
        .scope(
            (
                ModelHandle::new("anthropic", "claude-opus-5-5"),
                frozen.clone(),
            ),
            BUDGET.scope(
                Some(budgeted(&[], Some(1))),
                Box::pin(async {
                    use broca_provider::wire::WireFamily;
                    let scenario = reader_scenario(vec![
                        answer(
                            "a1",
                            vec![call("c1", "read", json!({}))],
                            FinishReason::ToolCalls,
                        ),
                        text_step("a2", "done"),
                    ]);
                    let base = new_source(&scenario, None, scenario.steps.clone()).await;
                    let run = new_source(
                        &scenario,
                        Some(writer_start(&base, "anthropic_tool_addition", 1)),
                        scenario.steps.clone(),
                    )
                    .await;
                    let request = &run.outcome.requests[1];
                    assert_eq!(request.tools, run.outcome.requests[0].tools);
                    let body = broca_provider::families::AnthropicMessages.shape(&frozen, request);
                    assert_eq!(body["tool_choice"], json!({"type":"none"}));
                    assert_eq!(body["messages"][1]["content"][0]["type"], "tool_addition");
                }),
            ),
        )
        .await;
}

#[tokio::test]
async fn paused_writer_run_applies_a_view_without_redelivering_a_cut_note_mid_run() {
    use broca_core::compaction_v2::received::ReceivedView;
    use cortexkit_role_compaction::view::{Edit, EditOp, View};

    let frozen = broca_provider::families::openai_responses_frozen_config("gpt-6-luna");
    ROUTE.scope((ModelHandle::new("openai", "gpt-6-luna"), frozen), LAYOUT.scope(Some(cortexkit_compaction_view::PLAN_VERSION), COMPACTION.scope(Some(Arc::new(CompactionScript { context_limit: 100_000, rides: vec![] })), Box::pin(async {
        let scenario = compacted_scenario(vec![answer("a1", vec![call("c1", "read", json!({}))], FinishReason::ToolCalls), text_step("a2", "done")]);
        let base = new_source(&scenario, None, scenario.steps.clone()).await;
        let writer = new_source(&scenario, Some(writer_start(&base, "responses_additional_tools", 1)), scenario.steps.clone()).await;
        assert!(writer.outcome.requests[0].prompt.iter().any(|message| message.tool_addition.is_some()));
        let first = writer.records.iter().find_map(|record| match record {
            Record::EntryFacts(entry) if entry.ordinal == 1 => Some(entry.mid.clone()),
            _ => None,
        }).unwrap();
        let boundary = writer.records.iter().position(|record| matches!(record, Record::StepStart(start) if start.step_id == 2)).unwrap();
        let mut records = writer.records[..boundary].to_vec();
        records.push(Record::Pause(broca_session_store::record::Pause {
            run_id: "r1".into(), reason: broca_protocol::caller::PauseReason::Restart,
            ts_ms: None, error: None, account_limit: None,
        }));
        let view = ReceivedView::from(View::seal(1, vec![Edit::new(1, EditOp::Cut { through: first })]));
        records.push(broca_session_loop::compaction::view_record(None, &view, None).unwrap());
        let paused = Start { records, parts: writer.parts.clone(), converted: false, snapshot: None };
        let reader = new_source(&scenario, Some(paused), vec![text_step("a2", "done")]).await;
        assert!(reader.outcome.requests[0].prompt.iter().all(|message| message.tool_addition.is_none()));
        assert_eq!(reader.outcome.requests[0].tools, writer.outcome.requests[0].tools);
        assert!(reader.records.iter().any(|record| matches!(record, Record::ViewApplied(_))));
        assert_eq!(reader.records.iter().filter(|record| matches!(record, Record::PlanNotes(_))).count(), 1);
    })))).await;
}

#[tokio::test]
async fn native_addition_keeps_its_anchor_next_to_a_cache_keeping_effort_change() {
    use broca_provider::variant::{FrozenVariant, VariantRequest};
    use broca_provider::wire::WireFamily;

    let opus = |effort: &str| {
        let mut frozen =
            broca_provider::families::anthropic_apikey_frozen_config("claude-opus-5-5");
        frozen.anthropic_turn_system = true;
        frozen.effort_change_cache = true;
        frozen.variant = Some(FrozenVariant {
            name: effort.into(),
            request: VariantRequest::Anthropic {
                thinking: Some(json!({"type":"adaptive", "display":"summarized"})),
                effort: Some(effort.into()),
            },
        });
        frozen
    };
    let model = ModelHandle::new("anthropic", "claude-opus-5-5");
    let scenario = reader_scenario(vec![text_step("a1", "first")]);
    let base = ROUTE
        .scope(
            (model.clone(), opus("low")),
            Box::pin(new_source(&scenario, None, scenario.steps.clone())),
        )
        .await;
    let mut note = writer_start(&base, "anthropic_tool_addition", 3)
        .records
        .pop()
        .unwrap();
    if let Record::PlanNotes(delivery) = &mut note {
        delivery.model_key = "anthropic:claude-opus-5-5".into();
    }
    let mut records = base.records;
    records.push(note);
    let start = Start {
        records,
        parts: base.parts,
        converted: false,
        snapshot: None,
    };
    let frozen = opus("high");
    let reader = ROUTE
        .scope(
            (model, frozen.clone()),
            FRESH_RUN.scope(
                "r2",
                Box::pin(new_source(
                    &scenario,
                    Some(start),
                    vec![text_step("a2", "done")],
                )),
            ),
        )
        .await;
    let request = &reader.outcome.requests[0];
    assert_eq!(request.tools[0].name, "read");
    assert!(request
        .prompt
        .iter()
        .any(|message| message.tool_addition.is_some()));
    let body = broca_provider::families::AnthropicMessages.shape(&frozen, request);
    assert_eq!(body["output_config"]["effort"], "low");
    assert!(body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|message| message["output_config"]["effort"] == "high"));
    assert!(body["messages"].to_string().contains("tool_addition"));
}

#[tokio::test]
async fn a_replayed_empty_assistant_is_not_a_reply_for_an_anthropic_addition() {
    let frozen = broca_provider::families::anthropic_frozen_config("claude-opus-5-5");
    ROUTE.scope((ModelHandle::new("anthropic", "claude-opus-5-5"), frozen), Box::pin(async {
        let scenario = reader_scenario(vec![text_step("a1", "")]);
        let finished = new_source(&scenario, None, scenario.steps.clone()).await;
        assert!(finished.records.iter().any(|record| matches!(record, Record::Assistant(reply) if reply.message.content.iter().all(|block| matches!(block, ContentBlock::Text { text } if text.is_empty())))));
        let delivery = writer_start(&finished, "anthropic_tool_addition", 1).records.pop().unwrap();
        let mut records = finished.records;
        records.push(delivery);
        let start = Start { records, parts: finished.parts, converted: false, snapshot: None };
        let reader = FRESH_RUN.scope("r2", Box::pin(new_source(&scenario, Some(start), vec![text_step("a2", "done")]))).await;
        assert!(reader.outcome.requests[0].prompt.iter().all(|message| message.tool_addition.is_none()));
        assert_eq!(reader.outcome.requests[0].tools[0].name, "read");
    })).await;
}

#[tokio::test]
async fn readiness_failure_sends_nothing_and_delivers_no_more_plan_notes() {
    let frozen = broca_provider::families::openai_responses_frozen_config("gpt-6-luna");
    ROUTE
        .scope(
            (ModelHandle::new("openai", "gpt-6-luna"), frozen),
            LAYOUT.scope(
                Some(cortexkit_compaction_view::PLAN_VERSION),
                COMPACTION.scope(
                    Some(Arc::new(CompactionScript {
                        context_limit: 100_000,
                        rides: vec![],
                    })),
                    Box::pin(async {
                        let scenario = compacted_scenario(vec![text_step("a1", "done")]);
                        let base = new_source(&scenario, None, scenario.steps.clone()).await;
                        let mut start = writer_start(&base, "responses_additional_tools", 1);
                        let frozen = start
                            .records
                            .iter_mut()
                            .find_map(|record| match record {
                                Record::RunStart(start) => start.run_choices.as_mut(),
                                _ => None,
                            })
                            .unwrap();
                        frozen["config"]["tool_choice"] =
                            serde_json::to_value(ToolChoice::None).unwrap();
                        let run = READY
                            .scope(
                                Some(ReadyScript::Fails),
                                Box::pin(new_source(
                                    &scenario,
                                    Some(start),
                                    scenario.steps.clone(),
                                )),
                            )
                            .await;
                        assert!(run.outcome.requests.is_empty());
                        assert!(!run
                            .records
                            .iter()
                            .any(|record| matches!(record, Record::Attempt(_))));
                        assert_eq!(
                            run.records
                                .iter()
                                .filter(|record| matches!(record, Record::PlanNotes(_)))
                                .count(),
                            1
                        );
                    }),
                ),
            ),
        )
        .await;
}

#[path = "plan_notes/conservative.rs"]
mod conservative;
#[path = "plan_notes/historical.rs"]
mod historical;

#[path = "plan_notes/in_place.rs"]
mod in_place;

#[path = "plan_notes/legacy_echo.rs"]
mod legacy_echo;
pub(super) use legacy_echo::legacy_echo_blob;

#[path = "plan_notes/revision14.rs"]
pub(super) mod revision14;

#[path = "plan_notes/revision14_review.rs"]
mod revision14_review;

#[path = "plan_notes/corrections.rs"]
mod corrections;

#[path = "plan_notes/canonical_floor.rs"]
mod canonical_floor;
