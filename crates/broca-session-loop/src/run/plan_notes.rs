//! A run's plan changes: the record of a pending change its start applies, and the plan notes
//! (llm-runner `plan_notes`) each request renders and the tools the gate refuses while a change
//! waits under `notes_until_rebuild`.

use super::*;

/// A run start's application of a pending plan change, as the record that follows the run's
/// input in the start's batch keeps it.
pub(super) fn refresh_applied(
    applied: &broca_protocol::session_change::AppliedChange,
) -> broca_session_store::record::RefreshApplied {
    broca_session_store::record::RefreshApplied {
        generation: applied.generation,
        applied_by: applied.applied_by.clone(),
        manifest_digest: applied.manifest_digest.clone(),
        composition_digest: applied.composition_digest.clone(),
        absent_items: applied.absent_items.clone(),
        reason: None,
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
