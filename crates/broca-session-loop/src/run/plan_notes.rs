//! A run's plan changes: the record of a pending change its start applies, and the plan notes
//! (llm-runner `plan_notes`) each request renders and the tools the gate refuses while a change
//! waits under `notes_until_rebuild`.

use super::*;
use broca_protocol::session_change::{NoteKind, NotesDelivery};
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
    let notes = (admission.plan_notes.as_ref())
        .map(|notes| Record::PlanNotes(at_input(notes, writer, admission.input.len())));
    applied.into_iter().chain(notes)
}

/// The record of `delivery`, the notes a run start delivers, anchored at the last of the
/// `inputs` messages the start appends to `writer`'s conversation: every request from the run's
/// first renders the notes right after that message.
fn at_input<W: Writer>(delivery: &NotesDelivery, writer: &W, inputs: usize) -> PlanNotes {
    let conversation = writer.conversation();
    let last_input =
        conversation.window_start() - 1 + conversation.messages.len() as u64 + inputs as u64;
    record(delivery, last_input)
}

/// The record of `delivery`, the notes a run start delivers, anchored at the conversation
/// entry of ordinal `anchor`.
fn record(delivery: &NotesDelivery, anchor: u64) -> PlanNotes {
    PlanNotes {
        generation: delivery.generation,
        anchor,
        notes: delivery
            .notes
            .iter()
            .map(|note| PlanNote {
                kind: match note.kind {
                    NoteKind::System => PlanNoteKind::System,
                    NoteKind::ToolRemoval => PlanNoteKind::ToolRemoval,
                },
                text: note.text.clone(),
                form: note.form.clone(),
            })
            .collect(),
        refused_tools: delivery.refused_tools.clone(),
        model_key: delivery.model_key.clone(),
    }
}

/// A run start's application of a pending plan change, as the record that follows the run's
/// input in the start's batch keeps it.
fn refresh_applied(
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

/// The tools a pending plan change removes from the applied plan, which a run that delivered
/// notes about the change, and every run after it until the change applies in full, refuses to
/// call (llm-runner `plan_notes`). Read from the session's notes, so a resumed run refuses the
/// same tools.
pub(super) fn removed_tools(
    operations: &broca_session_store::fold::operations::Operations,
) -> std::collections::BTreeSet<String> {
    operations
        .plan
        .iter()
        .flat_map(|plan| &plan.notes)
        .flat_map(|delivered| delivered.refused_tools.iter().cloned())
        .collect()
}

/// `opts` with the plan notes the session delivered since its pending change was accepted
/// (llm-runner `plan_notes`), each after the user turn of the run that delivered it. They
/// render in every request until the change applies in full, which clears them from the fold.
pub(super) fn with_plan_notes<W: Writer>(writer: &W, opts: CallOptions) -> CallOptions {
    let Some(plan) = writer
        .operations()
        .plan
        .as_ref()
        .filter(|plan| !plan.notes.is_empty())
    else {
        return opts;
    };
    let notes = plan.notes.iter().flat_map(|delivered| {
        delivered
            .notes
            .iter()
            .map(move |note| (delivered.anchor, note.text.as_str()))
    });
    broca_core::plan_notes::view(&opts, notes).unwrap_or(opts)
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
