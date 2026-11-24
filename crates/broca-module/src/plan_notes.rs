//! Plan notes at a run start (llm-runner `plan_notes`): whether a plan change pending
//! under policy `notes_until_rebuild` applies in full at this run, or what the run tells the
//! model about it while the applied plan's system text and tools keep rendering.

use std::collections::BTreeSet;

use broca_protocol::session_change::{
    DeliveredNote, NoteKind, NotesDelivery, NOTE_FORM_TURN_SYSTEM,
};
use broca_records::record::PlanManifest;
use broca_session_store::record::{PlanNoteKind, PlanNotes};

/// How long a change may wait for a cheaper moment before it applies in full anyway, counted
/// from the acceptance of the oldest change still pending (llm-runner: "at least six hours").
pub(crate) const APPLY_AFTER_MS: u64 = 6 * 60 * 60 * 1000;

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
    pub pending: &'a PlanManifest,
    pub generation: u64,
    /// The pending refresh's `system_note`.
    pub system_note: Option<&'a str>,
    /// Every delivery since the change was first accepted (the fold's notes).
    pub delivered: &'a [PlanNotes],
    /// The ordinal of the first conversation entry the session still renders: a note anchored
    /// before it went out with the history a compaction cut sealed, and renders no more.
    pub window_start: u64,
    pub pending_since_ms: Option<u64>,
    pub now_ms: u64,
    /// This run loses the prompt cache anyway: a flush, a model change, an effort change that
    /// loses the cache, a proven lapse of the cache, or a compaction provider joining.
    pub cache_lost: bool,
    /// The run's route takes a system-role turn mid-conversation.
    pub turn_system: bool,
    pub model_key: String,
}

/// The run start's decision. The change applies in full where it costs no extra prompt rewrite
/// (`cache_lost`), once the oldest pending change is six hours old, and when it changes the
/// system text on a route with no system-role turn to carry the note: a system note never goes
/// out at the person's authority. Otherwise the run delivers what no rendering note says yet:
/// the system note, and the removal wording of each tool the change removes. Tool additions and
/// changed definitions get no note, because only a native tool form could make them callable;
/// they wait for the full apply. Every removed tool is refused at the gate from this run on,
/// with or without a note.
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
    let system_changed = current.system_text != facts.pending.system_text;
    let system_note = match facts.system_note {
        Some(note) if facts.turn_system => Some(note),
        // Acceptance requires a note for a change of system text, so a missing one is only a
        // missing form.
        _ if system_changed => {
            return AtStart::Apply {
                reason: Some("no_note_form"),
            }
        }
        _ => None,
    };
    // A note still renders while its anchor is in the window.
    let rendering = |kind: PlanNoteKind, text: &str| {
        facts
            .delivered
            .iter()
            .filter(|delivered| delivered.anchor >= facts.window_start)
            .flat_map(|delivered| &delivered.notes)
            .any(|note| note.kind == kind && note.text == text)
    };
    let mut notes = Vec::new();
    if let Some(text) = system_note.filter(|_| system_changed) {
        if !rendering(PlanNoteKind::System, text) {
            notes.push(DeliveredNote {
                kind: NoteKind::System,
                text: text.to_owned(),
                form: NOTE_FORM_TURN_SYSTEM.into(),
            });
        }
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
        .filter(|name| !kept.contains(name))
        .collect();
    if facts.turn_system {
        for name in &removed {
            let text = broca_core::plan_notes::tool_removal(name);
            if !rendering(PlanNoteKind::ToolRemoval, &text) {
                notes.push(DeliveredNote {
                    kind: NoteKind::ToolRemoval,
                    text,
                    form: NOTE_FORM_TURN_SYSTEM.into(),
                });
            }
        }
    }
    let refused: BTreeSet<&str> = facts
        .delivered
        .iter()
        .flat_map(|delivered| delivered.refused_tools.iter().map(String::as_str))
        .collect();
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

#[cfg(test)]
mod tests;
