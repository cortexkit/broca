//! Plan notes at a run start (llm-runner `plan_notes`): whether a plan change pending
//! under policy `notes_until_rebuild` applies in full at this run, or what the run tells the
//! model about it while the applied plan's system text and tools keep rendering.

use std::collections::BTreeSet;

use broca_protocol::session_change::{
    DeliveredNote, NoteKind, NotesDelivery, NOTE_FORM_TURN_SYSTEM,
};
use broca_protocol::{
    json::Json,
    tool_addition::{ToolAdditionForm, ToolDefinition},
};
use broca_records::record::PlanManifest;
use broca_session_store::record::{PlanNote, PlanNoteKind, PlanNotes};

mod additions;
mod resolved_route;
pub(crate) use resolved_route::form as frozen_addition_form;
pub use resolved_route::reconcile_note_route;
pub(crate) use resolved_route::text_form;

/// How long a change may wait for a cheaper moment before it applies in full anyway, counted
/// from the acceptance of the oldest change still pending (llm-runner: "at least six hours").
pub(crate) const APPLY_AFTER_MS: u64 = 6 * 60 * 60 * 1000;

// Enable only after deployed readers understand the stored ToolRestored kind.
pub(crate) const WRITE_TOOL_RESTORATIONS: bool = false;

/// What a run start does with a change pending under `notes_until_rebuild`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AtStart {
    /// The change applies in full at this run. `reason` is `no_note_form` when nothing but the
    /// missing form made it apply.
    Apply { reason: Option<&'static str> },
    /// The change keeps waiting. The run records the delivery, when it delivers anything new.
    Wait(Option<NotesDelivery>),
}

/// What [`at_start`] reads about the session and the run.
pub(crate) struct Facts<'a> {
    /// The manifest the session's latest run froze: the plan that keeps rendering.
    pub current: Option<&'a PlanManifest>,
    /// The refresh whose system change the latest delivered system note describes.
    pub noted_system: Option<&'a PlanManifest>,
    pub pending: &'a PlanManifest,
    pub generation: u64,
    /// The pending refresh's `system_note`.
    pub system_note: Option<&'a str>,
    /// Every delivery since the change was first accepted (the fold's notes).
    pub delivered: &'a [PlanNotes],
    /// The ordinal of the first conversation entry the session still renders: a note anchored
    /// before it went out with the history a compaction cut sealed, and renders no more.
    pub window_start: u64,
    /// Anchors that the next request's projected view and renderer can carry. Without that
    /// projection, only anchors before `window_start` are known to be cut.
    pub rendering_anchors: Option<&'a BTreeSet<u64>>,
    pub redelivery_enabled: bool,
    pub pending_since_ms: Option<u64>,
    pub now_ms: u64,
    /// This run loses the prompt cache anyway: a flush, a model change, an effort change that
    /// loses the cache, a proven lapse of the cache, or a compaction provider joining.
    pub cache_lost: bool,
    /// The run's route takes a system-role turn mid-conversation.
    pub turn_system: bool,
    pub tool_addition_form: Option<ToolAdditionForm>,
    pub scoped: bool,
    pub model_key: String,
}

pub(crate) fn last_system_note(delivered: &[PlanNotes]) -> Option<(&PlanNotes, &PlanNote)> {
    delivered.iter().rev().find_map(|delivery| {
        delivery
            .notes
            .iter()
            .rev()
            .find(|note| note.kind == PlanNoteKind::System)
            .map(|note| (delivery, note))
    })
}

fn last_tool_note<'a>(
    delivered: &'a [PlanNotes],
    name: &str,
    removal: &str,
) -> Option<(&'a PlanNotes, &'a PlanNote)> {
    delivered.iter().rev().find_map(|delivery| {
        delivery
            .notes
            .iter()
            .rev()
            .find(|note| {
                (note.kind == PlanNoteKind::ToolRemoval && note.text == removal)
                    || note.restored_tool() == Some(name)
            })
            .map(|note| (delivery, note))
    })
}

/// A no-op can apply at the next run unless doing so would erase a delivered note.
pub(crate) fn unchanged_surface(current: &PlanManifest, pending: &PlanManifest) -> bool {
    current.system_text == pending.system_text && current.tools == pending.tools
}

pub(crate) fn has_delivered_notes(delivered: &[PlanNotes]) -> bool {
    delivered.iter().any(|delivery| !delivery.notes.is_empty())
}

/// The run start's decision. The change applies in full where it costs no extra prompt rewrite
/// (`cache_lost`), once the oldest pending change is six hours old, and when it changes the
/// system text on a route with no system-role turn to carry the note: a system note never goes
/// out at the person's authority. Otherwise the run delivers what no rendering note says yet:
/// the system note, the removal wording for dropped tools, and declarations for new tools when
/// the route can make them callable. Changed definitions wait for a full apply. Every removed
/// tool is refused at the gate from this run on, with or without a note.
pub(crate) fn at_start(facts: &Facts<'_>) -> AtStart {
    let aged = facts
        .pending_since_ms
        .is_some_and(|since| facts.now_ms >= since.saturating_add(APPLY_AFTER_MS));
    let Some(current) = facts.current else {
        // Nothing frozen to keep rendering.
        return AtStart::Apply { reason: None };
    };
    if facts.cache_lost || aged {
        return AtStart::Apply { reason: None };
    }
    let drops_notes = has_delivered_notes(facts.delivered);
    if !drops_notes && unchanged_surface(current, facts.pending) {
        return AtStart::Apply { reason: None };
    }
    let system_changed =
        facts.noted_system.unwrap_or(current).system_text != facts.pending.system_text;
    let system_note = if system_changed {
        if let Some(note) = facts.system_note.filter(|_| facts.turn_system) {
            Some(note)
        } else if drops_notes {
            // The prefix still carries the old note unless the resolved route forces an apply.
            None
        } else {
            return AtStart::Apply {
                reason: Some("no_note_form"),
            };
        }
    } else {
        // Re-deliver only the note describing the current pending text, not a superseded change.
        facts
            .noted_system
            .and_then(|_| last_system_note(facts.delivered))
            .filter(|(delivery, _)| facts.turn_system && !delivery_renders(facts, delivery))
            .map(|(_, note)| note.text.as_str())
    };
    let mut notes = Vec::new();
    if let Some(text) = system_note {
        notes.push(DeliveredNote {
            kind: NoteKind::System,
            text: text.to_owned(),
            form: NOTE_FORM_TURN_SYSTEM.into(),
            tool: None,
            tool_dispatch: None,
        });
    }
    let kept: BTreeSet<&str> = facts
        .pending
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    let removed: BTreeSet<&str> = current
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .chain(additions::delivered(facts).map(|(tool, _)| tool.name.as_str()))
        .filter(|name| !kept.contains(name))
        .collect();
    let refused = PlanNotes::refusals(facts.delivered);
    if facts.turn_system {
        for name in &removed {
            let text = broca_core::plan_notes::tool_removal(name);
            let already_rendering =
                last_tool_note(facts.delivered, name, &text).is_some_and(|(delivery, note)| {
                    delivery_renders(facts, delivery) && note.kind == PlanNoteKind::ToolRemoval
                });
            if !already_rendering {
                notes.push(DeliveredNote {
                    kind: NoteKind::ToolRemoval,
                    text,
                    form: NOTE_FORM_TURN_SYSTEM.into(),
                    tool: None,
                    tool_dispatch: None,
                });
            }
        }
        if WRITE_TOOL_RESTORATIONS && !facts.delivered.is_empty() {
            // Only an unchanged, already-rendered definition can become callable with a text
            // correction. A new or changed definition waits until the entire plan is applied.
            let restorable: BTreeSet<_> = current
                .tools
                .iter()
                .filter(|tool| facts.pending.tools.contains(tool))
                .map(|tool| tool.name.as_str())
                .collect();
            for name in restorable {
                let removal = broca_core::plan_notes::tool_removal(name);
                let reanchor = last_tool_note(facts.delivered, name, &removal).is_some_and(
                    |(delivery, note)| {
                        !delivery_renders(facts, delivery)
                            && note.kind == PlanNoteKind::ToolRestored
                    },
                );
                if refused.contains(name) || reanchor {
                    notes.push(DeliveredNote {
                        kind: NoteKind::ToolRestored,
                        text: cortexkit_role_llm_runner::plan_notes::tool_addition(name),
                        form: NOTE_FORM_TURN_SYSTEM.into(),
                        tool: None,
                        tool_dispatch: None,
                    });
                }
            }
        }
    }
    notes.extend(redeliver_additions(facts, &refused));
    additions::append_to(&mut notes, facts, current, &refused);
    if notes.is_empty() && removed.is_subset(&refused) {
        return AtStart::Wait(None);
    }
    AtStart::Wait(Some(NotesDelivery {
        generation: facts.generation,
        notes,
        refused_tools: removed.into_iter().map(str::to_owned).collect(),
        model_key: facts.model_key.clone(),
    }))
}

fn renders(facts: &Facts<'_>, anchor: u64) -> bool {
    anchor >= facts.window_start
        && facts
            .rendering_anchors
            .is_none_or(|anchors| anchors.contains(&anchor))
}

fn delivery_renders(facts: &Facts<'_>, delivery: &PlanNotes) -> bool {
    if delivery.blocks == Some(true) {
        return !facts.redelivery_enabled || renders(facts, delivery.anchor);
    }
    delivery.anchor >= facts.window_start
}

/// Copy a native declaration only while the pending plan still offers the recorded definition.
fn redeliver_additions(facts: &Facts<'_>, refused: &BTreeSet<&str>) -> Vec<DeliveredNote> {
    if !facts.redelivery_enabled {
        return Vec::new();
    }
    let mut notes = Vec::new();
    let marked = facts
        .delivered
        .iter()
        .filter(|delivery| delivery.blocks == Some(true))
        .cloned()
        .collect::<Vec<_>>();
    for previous in broca_session_store::fold::operations::latest_plan_notes(&marked).filter_map(
        |(anchor, note)| {
            (note.kind == PlanNoteKind::ToolAddition && !renders(facts, anchor)).then_some(note)
        },
    ) {
        if facts
            .delivered
            .iter()
            .any(|delivery| renders(facts, delivery.anchor) && delivery.notes.contains(previous))
        {
            continue;
        }
        if previous.kind == PlanNoteKind::ToolAddition {
            let offered = previous.tool.as_ref().is_some_and(|old| {
                !refused.contains(old.name.as_str())
                    && facts.pending.tools.iter().any(|tool| {
                        tool.name == old.name
                            && tool.description == old.description
                            && tool.input_schema.text() == old.input_schema
                    })
            });
            if !offered
                || facts.tool_addition_form.map(ToolAdditionForm::as_str)
                    != Some(previous.form.as_str())
            {
                continue;
            }
        }
        if let Some(note) = delivered_note(previous) {
            if !notes.contains(&note) {
                notes.push(note);
            }
        }
    }
    notes
}

fn delivered_note(note: &PlanNote) -> Option<DeliveredNote> {
    let tool = if let Some(tool) = &note.tool {
        Some(ToolDefinition {
            name: tool.name.clone(),
            description: tool.description.clone(),
            input_schema: Json::from_canonical_text(&tool.input_schema)?,
        })
    } else {
        None
    };
    Some(DeliveredNote {
        kind: match note.kind {
            PlanNoteKind::System => NoteKind::System,
            PlanNoteKind::ToolRemoval => NoteKind::ToolRemoval,
            PlanNoteKind::ToolAddition => NoteKind::ToolAddition,
            PlanNoteKind::ToolRestored => NoteKind::ToolRestored,
        },
        text: note.text.clone(),
        form: note.form.clone(),
        tool,
        tool_dispatch: note.tool_dispatch.clone(),
    })
}

#[cfg(test)]
mod tests;
