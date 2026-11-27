use std::collections::{BTreeMap, BTreeSet};

use broca_core::capability::{AddedToolDispatch, ProviderTransport};
use broca_protocol::{
    session_change::{DeliveredNote, NoteKind},
    tool_addition::ToolDefinition,
};
use broca_records::record::PlanManifest;
use broca_session_store::record::{PlanNote, PlanNoteKind, PlanNoteTool};

use super::Facts;

pub(super) fn delivered<'a>(
    facts: &'a Facts<'_>,
) -> impl Iterator<Item = (&'a PlanNoteTool, &'a PlanNote)> {
    facts
        .delivered
        .iter()
        .flat_map(|delivery| &delivery.notes)
        .filter_map(|note| {
            if note.kind != PlanNoteKind::ToolAddition {
                return None;
            }
            Some((note.tool.as_ref()?, note))
        })
}

pub(super) fn append_to(
    notes: &mut Vec<DeliveredNote>,
    facts: &Facts<'_>,
    applied: &PlanManifest,
    refused: &BTreeSet<&str>,
) {
    let Some(form) = facts.tool_addition_form else {
        return;
    };
    let mut seen: BTreeSet<_> = applied
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    let mut transports = BTreeMap::new();
    for (tool, note) in delivered(facts) {
        // Re-delivery reuses the recorded definition, not a newer definition under this name.
        seen.insert(tool.name.as_str());
        if let Some(dispatch) = note
            .tool_dispatch
            .as_deref()
            .and_then(|frozen| serde_json::from_str::<AddedToolDispatch>(frozen).ok())
        {
            if let (Some(module), Some(provider)) = (dispatch.module, dispatch.provider) {
                transports.entry(module).or_insert(provider);
            }
        }
    }
    for tool in facts.pending.tools.iter() {
        let name = tool.name.as_str();
        if !seen.insert(name) || refused.contains(name) {
            continue;
        }
        let dispatch = AddedToolDispatch::from_plan(tool, facts.pending);
        if let (Some(module), Some(provider)) = (&dispatch.module, &dispatch.provider) {
            if !facts.scoped
                && provider.preset.as_deref() == Some(broca_fetch_plan::CONFINED_TOOL_PRESET)
            {
                continue;
            }
            let applied_transport = ProviderTransport::of(applied, module);
            let earlier_inline = transports.iter().find_map(|(name, facts)| {
                broca_protocol::same_module(name, module).then_some(facts)
            });
            if applied_transport
                .as_ref()
                .or(earlier_inline)
                .is_some_and(|prior| prior != provider)
            {
                continue;
            }
            transports
                .entry(module.clone())
                .or_insert_with(|| provider.clone());
        }
        let Ok(tool_dispatch) = serde_json::to_string(&dispatch) else {
            continue;
        };
        notes.push(DeliveredNote {
            kind: NoteKind::ToolAddition,
            text: String::new(),
            form: form.as_str().into(),
            tool: Some(ToolDefinition {
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema: tool.input_schema.clone(),
            }),
            tool_dispatch: Some(tool_dispatch),
        });
    }
}
