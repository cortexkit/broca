//! A run's plan changes: the record of a pending change its start applies, and the plan notes
//! (llm-runner `plan_notes`) each request renders and the tools the gate refuses while a change
//! waits under `notes_until_rebuild`.

use super::*;
use broca_core::{capability::AddedToolDispatch, plan_notes::Note};
use broca_protocol::session_change::{NoteKind, NotesDelivery};
use broca_protocol::{
    json::Json,
    tool_addition::{ToolAdditionForm, ToolDefinition},
};
use broca_session_store::record::{PlanNote, PlanNoteKind, PlanNotes};

/// The records of `admission`'s plan change, which its start's batch writes after the run's
/// input so the start is still followed directly by its input: the pending change the run
/// applies, and the notes it delivers about a change still pending.
pub(super) fn change_records<W: Writer>(
    admission: &Admission,
    writer: &W,
) -> impl Iterator<Item = Record> {
    let applied = (admission.applies_refresh.as_ref())
        .map(|applied| Record::RefreshApplied(refresh_applied(applied)));
    let notes = admission
        .plan_notes
        .as_ref()
        .map(|notes| at_input(notes, writer, admission.input.len()))
        .filter(|delivery| !delivery.notes.is_empty() || !delivery.refused_tools.is_empty())
        .map(Record::PlanNotes);
    applied.into_iter().chain(notes)
}

/// The record of `delivery`, the notes a run start delivers, anchored at the last of the
/// `inputs` messages the start appends to `writer`'s conversation: every request from the run's
/// first renders the notes right after that message.
fn at_input<W: Writer>(delivery: &NotesDelivery, writer: &W, inputs: usize) -> PlanNotes {
    record(
        delivery,
        newest_entry(writer) + inputs as u64,
        WRITE_NOTE_FIELDS,
    )
}

fn newest_entry<W: Writer>(writer: &W) -> u64 {
    let conversation = writer.conversation();
    conversation.window_start() - 1 + conversation.messages.len() as u64
}

/// The record of `delivery`, the notes a run start delivers, anchored at the conversation
/// entry of ordinal `anchor`.
fn record(delivery: &NotesDelivery, anchor: u64, write_new_fields: bool) -> PlanNotes {
    PlanNotes {
        generation: delivery.generation,
        anchor,
        blocks: (write_new_fields && !delivery.notes.is_empty()).then_some(true),
        notes: delivery
            .notes
            .iter()
            .filter(|note| write_new_fields || note.kind != NoteKind::ToolAddition)
            .map(|note| PlanNote {
                kind: match note.kind {
                    NoteKind::System => PlanNoteKind::System,
                    NoteKind::ToolRemoval => PlanNoteKind::ToolRemoval,
                    NoteKind::ToolAddition => PlanNoteKind::ToolAddition,
                    NoteKind::ToolRestored => PlanNoteKind::ToolRestored,
                },
                text: note.text.clone(),
                form: note.form.clone(),
                tool: note.tool.as_ref().filter(|_| write_new_fields).map(|tool| {
                    broca_session_store::record::PlanNoteTool {
                        name: tool.name.clone(),
                        description: tool.description.clone(),
                        input_schema: tool.input_schema.text().to_owned(),
                    }
                }),
                tool_dispatch: note.tool_dispatch.clone().filter(|_| write_new_fields),
            })
            .collect(),
        refused_tools: delivery.refused_tools.clone(),
        model_key: delivery.model_key.clone(),
    }
}

/// A run start's application of a pending plan change, as the record that follows the run's
/// input in the start's batch keeps it.
pub(crate) fn refresh_applied(
    applied: &broca_protocol::session_change::AppliedChange,
) -> broca_session_store::record::RefreshApplied {
    broca_session_store::record::RefreshApplied {
        generation: applied.generation,
        applied_by: applied.applied_by.clone(),
        manifest_digest: applied.manifest_digest.clone(),
        composition_digest: applied.composition_digest.clone(),
        absent_items: applied.absent_items.clone(),
        reason: applied.reason.clone(),
    }
}

// Enable new ledger fields only after deployed readers understand them.
const WRITE_NOTE_FIELDS: bool = false;

/// The tools the newest delivery naming each tool says to refuse. Older removal notes still
/// render after a correction, but their tools become callable again. Reading the deliveries in
/// order keeps the gate the same on resume (llm-runner `plan_notes`).
pub(super) fn removed_tools(
    operations: &broca_session_store::fold::operations::Operations,
) -> std::collections::BTreeSet<String> {
    operations
        .plan
        .as_ref()
        .map_or_else(Default::default, |plan| {
            PlanNotes::refusals(&plan.notes)
                .into_iter()
                .map(str::to_owned)
                .collect()
        })
}

/// Place durable notes after their recorded entries. A cut entry is absent from this request;
/// only the next run's delivery may add a fresh copy after its own input.
pub(crate) fn project<W: Writer>(
    writer: &W,
    admission: &Admission,
    opts: CallOptions,
) -> Result<CallOptions, LoopError> {
    project_for(writer, admission, opts, None)
}

pub(super) fn for_fallback<W: Writer, P: Provider>(
    writer: &W,
    admission: &Admission,
    provider: &P,
    opts: &CallOptions,
) -> Result<Option<CallOptions>, LoopError> {
    if !writer
        .operations()
        .plan
        .as_ref()
        .is_some_and(|plan| plan.has_rendered_notes())
    {
        return Ok(None);
    }
    provider
        .refusal_fallback_model()
        .map(|model| project_for(writer, admission, opts.clone(), Some(&model)))
        .transpose()
}

pub(super) fn fallback_carries<W: Writer>(writer: &W, admission: &Admission) -> bool {
    let Some(plan) = writer
        .operations()
        .plan
        .as_ref()
        .filter(|plan| plan.has_rendered_notes())
    else {
        return true;
    };
    let Ok(frozen) = serde_json::from_value::<broca_provider::wire::FrozenRenderConfig>(
        admission.render_config.as_ref().clone(),
    ) else {
        return true;
    };
    let Some(fallback) = frozen.refusal_fallback.as_ref() else {
        return true;
    };
    plan.latest_notes().all(|(_, note)| {
        broca_provider::turn_system::tool_addition::carries_form(
            &fallback.frozen,
            &fallback.model.provider_module_id,
            (note.kind == PlanNoteKind::ToolAddition).then_some(note.form.as_str()),
        )
    })
}

pub(super) fn project_for<W: Writer>(
    writer: &W,
    admission: &Admission,
    mut opts: CallOptions,
    target: Option<&ModelHandle>,
) -> Result<CallOptions, LoopError> {
    if let Some(target) = target {
        opts.target_model = Some(target.clone());
    }
    let Some(plan) = writer
        .operations()
        .plan
        .as_ref()
        .filter(|plan| plan.has_rendered_notes())
    else {
        return Ok(opts);
    };
    let mut notes = Vec::new();
    let frozen = serde_json::from_value::<broca_provider::wire::FrozenRenderConfig>(
        admission.render_config.as_ref().clone(),
    )
    .ok();
    let render = frozen.as_ref().map(|config| {
        config
            .refusal_fallback
            .as_ref()
            .filter(|fallback| target == Some(&fallback.model))
            .map_or(config, |fallback| &fallback.frozen)
    });
    let anchors = rendering_anchors(render, &opts, true);
    let held = |anchor| {
        opts.prompt
            .iter()
            .any(|message| message.ordinal.is_some_and(|ordinal| ordinal + 1 == anchor))
    };
    let has_text_form = render.is_some_and(broca_provider::turn_system::supported);
    for (delivery, note) in plan.visible_notes(
        |anchor| {
            if has_text_form {
                anchors.contains(&anchor)
            } else {
                held(anchor)
            }
        },
        held,
    ) {
        let note = match note.kind {
            PlanNoteKind::System | PlanNoteKind::ToolRemoval | PlanNoteKind::ToolRestored
                if delivery.blocks != Some(true) || !has_text_form =>
            {
                Note::LegacyText(&note.text)
            }
            PlanNoteKind::System | PlanNoteKind::ToolRemoval | PlanNoteKind::ToolRestored => {
                Note::Text(&note.text)
            }
            PlanNoteKind::ToolAddition => {
                // There is no legacy string form for a native definition. An incompatible
                // route gets no declaration and its tool gate treats the name as undeclared.
                if !render.is_some_and(|frozen| {
                    broca_provider::turn_system::tool_addition::carries_form(
                        frozen,
                        &admission.model.provider_module_id,
                        Some(&note.form),
                    )
                }) {
                    continue;
                }
                let form = ToolAdditionForm::from_name(&note.form).ok_or_else(|| {
                    LoopError::Parts(format!("unknown tool-addition form {:?}", note.form))
                })?;
                Note::ToolAddition(form, definition(note)?)
            }
        };
        notes.push((delivery.anchor, note));
    }
    let anthropic = render.is_some_and(|render| {
        render.family_id == broca_provider::spec::WireFamilyId::AnthropicMessages
    });
    Ok(broca_core::plan_notes::view_for(&opts, notes, anthropic).unwrap_or(opts))
}

pub(crate) fn rendering_anchors(
    frozen: Option<&broca_provider::wire::FrozenRenderConfig>,
    opts: &CallOptions,
    end_is_legal: bool,
) -> std::collections::BTreeSet<u64> {
    if let Some(claude) = frozen
        .filter(|frozen| frozen.family_id == broca_provider::spec::WireFamilyId::AnthropicMessages)
    {
        return broca_provider::turn_system::rendering_anchors(claude, opts, end_is_legal);
    }
    opts.prompt
        .iter()
        .filter_map(|message| message.ordinal.and_then(|ordinal| ordinal.checked_add(1)))
        .collect()
}

fn definition(note: &PlanNote) -> Result<ToolDefinition, LoopError> {
    let tool = note
        .tool
        .as_ref()
        .ok_or_else(|| LoopError::Parts("tool-addition note is missing its definition".into()))?;
    let input_schema = Json::from_canonical_text(&tool.input_schema).ok_or_else(|| {
        LoopError::Parts(format!(
            "tool-addition note for {:?} has a noncanonical schema",
            tool.name
        ))
    })?;
    Ok(ToolDefinition {
        name: tool.name.clone(),
        description: tool.description.clone(),
        input_schema,
    })
}

/// The run's batch begins with its own applied manifest, then admits the inline declarations
/// delivered through this start. A resume reads their routes from the original PlanNotes records,
/// so a later refresh cannot change the provider or preset for an admitted call.
pub(super) fn dispatch_manifest<W: Writer>(
    writer: &W,
    admission: &Admission,
) -> Result<ToolManifest, LoopError> {
    let mut manifest = ToolManifest::from_shared_tools(Arc::clone(&admission.tools));
    if let Some(plan) = admission.manifest.as_deref() {
        manifest = manifest
            .with_plan_manifest(plan)
            .map_err(|error| LoopError::Parts(format!("the run's frozen plan: {error}")))?;
    }
    let notes = writer
        .operations()
        .plan
        .iter()
        .flat_map(|plan| &plan.notes)
        .flat_map(|delivery| &delivery.notes);
    let render = serde_json::from_value::<broca_provider::wire::FrozenRenderConfig>(
        admission.render_config.as_ref().clone(),
    )
    .ok();
    for note in notes {
        if note.kind == PlanNoteKind::ToolAddition {
            if !render.as_ref().is_some_and(|frozen| {
                broca_provider::turn_system::tool_addition::carries_form(
                    frozen,
                    &admission.model.provider_module_id,
                    Some(&note.form),
                )
            }) {
                continue;
            }
            let definition = definition(note)?;
            let dispatch: AddedToolDispatch = serde_json::from_str(
                note.tool_dispatch.as_deref().unwrap_or_default(),
            )
            .map_err(|error| {
                LoopError::Parts(format!(
                    "frozen dispatch for tool-addition note {:?}: {error}",
                    definition.name
                ))
            })?;
            manifest.add_inline_tool(definition, dispatch);
        }
    }
    Ok(manifest.with_run_transport(&admission.transport))
}

/// `opts` as the model sees it once a plan refresh applied: the system text of the plan the run
/// froze in place of the session's original system message (`durable_system`), as a view of the
/// request only. A session on the current compaction contract renders its system text from its
/// entries instead. A step's live request and its offline rebuild both take it from here, so a
/// rebuilt request matches the bytes the step sent.
pub(super) fn with_plan_system<W: Writer>(
    writer: &W,
    admission: &Admission,
    durable_system: &[Message],
    opts: CallOptions,
) -> CallOptions {
    let applied = writer
        .operations()
        .plan
        .as_ref()
        .is_some_and(|plan| plan.applied_generation > 0);
    match &admission.manifest {
        Some(manifest) if applied && admission.compaction_plan_version.is_none() => {
            broca_core::plan_system::view(&opts, durable_system, &manifest.system_text)
                .unwrap_or(opts)
        }
        _ => opts,
    }
}

#[cfg(test)]
mod write_tests {
    use super::*;
    use broca_protocol::session_change::DeliveredNote;

    #[test]
    fn reader_encoding_excludes_all_writer_only_note_fields() {
        let note = DeliveredNote {
            kind: NoteKind::System,
            text: "Read the latest instructions.".into(),
            form: "turn_system".into(),
            tool: None,
            tool_dispatch: None,
        };
        let mut native = note.clone();
        native.kind = NoteKind::ToolAddition;
        native.tool = Some(ToolDefinition {
            name: "find".into(),
            description: "Search the index".into(),
            input_schema: Json::from(serde_json::json!({"type":"object"})),
        });
        native.tool_dispatch = Some("frozen transport metadata".into());
        let delivery = NotesDelivery {
            generation: 1,
            model_key: "anthropic:claude-opus-5-5".into(),
            notes: vec![note, native],
            refused_tools: vec![],
        };
        let encoded = record(&delivery, 2, false);
        assert_eq!(encoded.notes.len(), 1);
        assert_eq!(encoded.blocks, None);
        assert!(encoded.notes[0].tool.is_none() && encoded.notes[0].tool_dispatch.is_none());
        let mut malformed = delivery;
        malformed.notes[0].tool = malformed.notes[1].tool.clone();
        malformed.notes[0].tool_dispatch = malformed.notes[1].tool_dispatch.clone();
        assert_eq!(record(&malformed, 2, false), encoded);
        let record = Record::PlanNotes(encoded);
        assert_eq!(record.kind(), 109);
        // Pin the legacy note bytes so an older binary can still decode reader-written logs.
        assert_eq!(
            broca_core::render::request_digest_hex(&record.encode_body()),
            "f8702c74ec283885ae6730573e4535e207dc34b447002ac3bd21c6d1b9cab1db"
        );
    }
}
