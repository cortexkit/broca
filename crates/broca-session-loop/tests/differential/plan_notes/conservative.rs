use super::*;
use broca_protocol::session_change::{AppliedChange, NotesDelivery};
use broca_provider::{
    families::{anthropic_apikey_frozen_config, AnthropicMessages},
    policy::{CrossModelThinking, ReasoningPolicy},
    wire::WireFamily,
};
use broca_session_store::record::SessionRefresh;

type Change = (AppliedChange, Option<NotesDelivery>);

tokio::task_local! {
    pub(super) static APPLY: Change;
}

pub(crate) fn apply_at_start() -> (Option<AppliedChange>, Option<NotesDelivery>) {
    APPLY
        .try_with(|(applied, delivered)| (Some(applied.clone()), delivered.clone()))
        .unwrap_or_default()
}

pub(super) fn refresh(source: Source, generation: u64) -> Start {
    let mut records = source.records;
    records.push(Record::SessionRefresh(SessionRefresh {
        generation,
        policy: "notes_until_rebuild".into(),
        run_in_progress: false,
        applies_from_next_run: true,
        setup: vec![],
        manifest: None,
        selection: None,
        refetch: vec![],
        system_note: None,
        accepted_at_ms: None,
    }));
    Start {
        records,
        parts: source.parts,
        converted: false,
        snapshot: None,
    }
}

pub(super) fn apply(generation: u64, notes: Option<NotesDelivery>) -> Change {
    (
        AppliedChange {
            generation,
            applied_by: "append".into(),
            manifest_digest: format!("plan-{generation}"),
            composition_digest: None,
            absent_items: vec![],
            reason: None,
        },
        notes,
    )
}

fn manifest(system_text: &str) -> Arc<broca_records::record::PlanManifest> {
    Arc::new(
        serde_json::from_value(json!({
            "plan":"{}", "plan_identity":"origin", "composition_digest":"composition",
            "fetches":[], "tools":[], "system_text":system_text, "system_digest":system_text,
            "join_version":1, "role_versions":{},
        }))
        .unwrap(),
    )
}

#[tokio::test]
async fn a_full_apply_does_not_treat_already_projected_system_text_as_the_original() {
    let mut frozen = anthropic_apikey_frozen_config("claude-opus-5-5");
    frozen.renderer_version = 4;
    frozen.anthropic_turn_system = true;
    frozen.cache = None;
    frozen.reasoning = ReasoningPolicy::AnthropicInterleaved {
        budget_tokens: 1024,
        last_turn_only: true,
        echo_scope: broca_provider::policy::ThinkingEchoScope::AllAssistantMessages,
        cross_model: CrossModelThinking::SameModelOnly,
    };
    ROUTE
        .scope(
            (
                ModelHandle::new("anthropic", "claude-opus-5-5"),
                frozen.clone(),
            ),
            Box::pin(async {
                let system = |text: &str| {
                    Message::new(Role::System, vec![ContentBlock::Text { text: text.into() }])
                };
                let mut first = reader_scenario(vec![text_step("a1", "ready")]);
                first.input.splice(0..0, [system("P"), system("H")]);
                first.manifest = Some(manifest("P"));
                let first = new_source(&first, None, first.steps.clone()).await;
                let wire = |opts: &CallOptions| AnthropicMessages.shape(&frozen, opts);
                let system_blocks = |texts: &[&str]| {
                    json!(texts
                        .iter()
                        .map(|text| json!({"type":"text", "text":text}))
                        .collect::<Vec<_>>())
                };
                assert_eq!(
                    wire(&first.outcome.requests[0])["system"],
                    system_blocks(&["P", "H"])
                );
                let z = tool("find", ExecutionMode::Sequential);
                let signed = answer(
                    "a2",
                    vec![
                        ContentBlock::Reasoning {
                            text: "signed under H alone".into(),
                            signature: Some("single-h-signature".into()),
                        },
                        ContentBlock::Text {
                            text: "ready under H".into(),
                        },
                    ],
                    FinishReason::Stop,
                );
                let mut second =
                    reader_scenario(vec![attempted(signed, vec![attempt(1, 200, "complete")])]);
                second.manifest = Some(manifest(""));
                let prepared = APPLY
                    .scope(
                        apply(1, None),
                        FRESH_RUN.scope(
                            "r2",
                            Box::pin(new_source(
                                &second,
                                Some(refresh(first, 1)),
                                second.steps.clone(),
                            )),
                        ),
                    )
                    .await;
                let cut = prepared
                    .records
                    .iter()
                    .position(
                        |record| matches!(record, Record::StepStart(step) if step.run_id == "r2"),
                    )
                    .unwrap();
                let mut records = prepared.records[..cut].to_vec();
                let anchor = records
                    .iter()
                    .rev()
                    .find_map(|record| match record {
                        Record::EntryFacts(facts) => Some(facts.ordinal),
                        _ => None,
                    })
                    .unwrap();
                // Replay the writer's PlanNotes from the log instead of generating a delivery.
                records.push(Record::PlanNotes(PlanNotes {
                    generation: 2,
                    anchor,
                    blocks: Some(true),
                    model_key: "anthropic:claude-opus-5-5".into(),
                    refused_tools: vec![],
                    notes: vec![PlanNote {
                        kind: PlanNoteKind::ToolAddition,
                        text: String::new(),
                        form: "anthropic_tool_addition".into(),
                        tool: Some(broca_session_store::record::PlanNoteTool {
                            name: z.name.clone(),
                            description: z.description.clone(),
                            input_schema: z.input_schema.text().into(),
                        }),
                        tool_dispatch: Some(json!({"sequential":true}).to_string()),
                    }],
                }));
                let second = new_source(
                    &second,
                    Some(Start {
                        records,
                        parts: prepared.parts,
                        snapshot: None,
                        converted: false,
                    }),
                    second.steps.clone(),
                )
                .await;
                let previous = wire(&second.outcome.requests[0]);
                assert!(previous["messages"].to_string().contains("tool_addition"));
                assert_eq!(previous["system"], system_blocks(&["H"]));
                let mut last = reader_scenario(vec![text_step("a3", "accepted")]);
                last.tools.push(z);
                last.manifest = Some(manifest("H"));
                let last = APPLY
                    .scope(
                        apply(2, None),
                        FRESH_RUN.scope(
                            "r3",
                            Box::pin(new_source(
                                &last,
                                Some(refresh(second, 2)),
                                last.steps.clone(),
                            )),
                        ),
                    )
                    .await;
                assert_eq!(last.outcome.requests.len(), 1);
                let sent = wire(&last.outcome.requests[0]);
                assert_eq!(sent["tools"].as_array().unwrap().len(), 2);
                assert_eq!(sent["system"], system_blocks(&["H", "H"]));
                assert!(sent["messages"].to_string().contains("ready under H"));
                assert!(!sent.to_string().contains("single-h-signature"));
            }),
        )
        .await;
}
