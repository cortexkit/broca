//! The image engine's state as the session store saves it: the conversion between the engine
//! (`broca_core`'s `SessionMedia` and `broca_provider`'s `MediaAging`, with the media limits in
//! force) and the store's saved media index (`StoredMediaIndex`).
//!
//! The store saves the index with a seal and hands it back when a session opens, without reading
//! it. Whoever owns a session's engine writes it through [`to_stored`] and restores it through
//! [`from_stored`]: the session's actor, and the converter, which writes the index of a session
//! it converts from the old storage.

use broca_core::media_request::{
    CaptureFacts, MediaBinding, MediaId, MediaIndexState, MediaLocation, SessionMedia,
};
use broca_protocol::ids::StepId;
use broca_protocol::media_event::{MediaLimits, MediaMessage, MediaSlot};
use broca_provider::media_request::{AgingState, MediaAging, MediaCost, Occurrence};
use broca_session_store::record::MediaObject;
use broca_session_store::session::{
    AcceptedMessage, HeldSkeleton, PlacedSkeleton, ShownOccurrence, StoredAging, StoredBinding,
    StoredCapture, StoredMediaId, StoredMediaIndex, StoredObject, StoredOccurrence,
    StoredReplacement, StoredSelection, StoredSkeleton, StoredSlot,
};

/// A saved index read back: the engine's state, its aging state, the media limits in force, whether
/// a step was open, and where each medium's bytes are stored, by digest.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredMedia {
    pub media: MediaIndexState,
    pub aging: AgingState,
    pub limits: Option<MediaLimits>,
    pub step_open: bool,
    pub objects: Vec<(String, MediaObject)>,
}

/// The index to save for an engine: `media` and its aging state `aging`, under the media limits
/// `limits`, with whether a step is open and where each medium's bytes are stored, by digest.
pub fn to_stored(
    media: &SessionMedia,
    aging: &MediaAging,
    limits: Option<&MediaLimits>,
    step_open: bool,
    objects: &[(String, MediaObject)],
) -> StoredMediaIndex {
    let state = media.index_state();
    let aging = aging.state();
    StoredMediaIndex {
        steps: state.boundary,
        last_step: state.last_step.map(|step| step.0),
        step_open,
        batch: state.batch,
        captures: state.captures.into_iter().map(stored_capture).collect(),
        pending: ids(&state.pending),
        accepted: state
            .held
            .iter()
            .map(|message| AcceptedMessage { ids: ids(message) })
            .collect(),
        protected: ids(&state.protected),
        placed: state
            .skeleton
            .iter()
            .map(|(ordinal, skeleton)| PlacedSkeleton {
                ordinal: *ordinal,
                skeleton: stored_skeleton(skeleton),
            })
            .collect(),
        held: state
            .held_messages
            .iter()
            .map(|((record, message), skeleton)| HeldSkeleton {
                record: *record,
                message: *message,
                skeleton: stored_skeleton(skeleton),
            })
            .collect(),
        replacements: state
            .replacements
            .into_iter()
            .map(|(references, from, to)| StoredReplacement {
                references,
                from,
                to,
            })
            .collect(),
        selection: state.selection.map(|bindings| StoredSelection {
            bindings: bindings.into_iter().map(stored_binding).collect(),
        }),
        covered: state.covered,
        has_view: state.has_view,
        limits: limits.map(|limits| limits.0.clone()),
        aging: StoredAging {
            shown: aging
                .shown
                .iter()
                .map(|(occurrence, cost, protected)| ShownOccurrence {
                    occurrence: stored_occurrence(occurrence),
                    bytes: cost.bytes,
                    tokens: cost.tokens,
                    protected: *protected,
                })
                .collect(),
            unprotected: aging.unprotected.iter().map(stored_occurrence).collect(),
            aged: aging.aged.iter().map(stored_occurrence).collect(),
            bytes: aging.bytes,
            tokens: aging.tokens,
        },
        objects: objects
            .iter()
            .map(|(sha256, object)| StoredObject {
                sha256: sha256.clone(),
                object: object.clone(),
            })
            .collect(),
    }
}

/// The engine's state saved as `index`. Refused when a skeleton slot names a tool call without
/// its part, or a part without its call. The engine's own restores
/// (`SessionMedia::from_index_state`, `MediaAging::from_state`) refuse the rest of what doesn't
/// agree.
pub fn from_stored(index: &StoredMediaIndex) -> Result<StoredMedia, String> {
    let media = MediaIndexState {
        boundary: index.steps,
        last_step: index.last_step.map(StepId),
        batch: index.batch.clone(),
        captures: index.captures.iter().map(capture_facts).collect(),
        pending: media_ids(&index.pending),
        held: index
            .accepted
            .iter()
            .map(|message| media_ids(&message.ids))
            .collect(),
        protected: media_ids(&index.protected),
        skeleton: index
            .placed
            .iter()
            .map(|placed| Ok((placed.ordinal, media_message(&placed.skeleton)?)))
            .collect::<Result<_, String>>()?,
        held_messages: index
            .held
            .iter()
            .map(|held| Ok(((held.record, held.message), media_message(&held.skeleton)?)))
            .collect::<Result<_, String>>()?,
        replacements: index
            .replacements
            .iter()
            .map(|replacement| {
                (
                    replacement.references.clone(),
                    replacement.from,
                    replacement.to,
                )
            })
            .collect(),
        selection: index
            .selection
            .as_ref()
            .map(|selection| selection.bindings.iter().map(media_binding).collect()),
        covered: index.covered,
        has_view: index.has_view,
    };
    let aging = AgingState {
        shown: index
            .aging
            .shown
            .iter()
            .map(|shown| {
                (
                    occurrence(&shown.occurrence),
                    MediaCost {
                        bytes: shown.bytes,
                        tokens: shown.tokens,
                    },
                    shown.protected,
                )
            })
            .collect(),
        unprotected: index.aging.unprotected.iter().map(occurrence).collect(),
        aged: index.aging.aged.iter().map(occurrence).collect(),
        bytes: index.aging.bytes,
        tokens: index.aging.tokens,
    };
    Ok(StoredMedia {
        media,
        aging,
        limits: index.limits.clone().map(MediaLimits),
        step_open: index.step_open,
        objects: index
            .objects
            .iter()
            .map(|stored| (stored.sha256.clone(), stored.object.clone()))
            .collect(),
    })
}

fn stored_id(id: &MediaId) -> StoredMediaId {
    StoredMediaId {
        record: id.record,
        message: id.message,
        block: id.block,
        part: id.part,
    }
}

fn media_id(id: &StoredMediaId) -> MediaId {
    MediaId {
        record: id.record,
        message: id.message,
        block: id.block,
        part: id.part,
    }
}

fn ids(ids: &[MediaId]) -> Vec<StoredMediaId> {
    ids.iter().map(stored_id).collect()
}

fn media_ids(ids: &[StoredMediaId]) -> Vec<MediaId> {
    ids.iter().map(media_id).collect()
}

fn stored_capture(capture: CaptureFacts) -> StoredCapture {
    StoredCapture {
        id: stored_id(&capture.id),
        reference: capture.reference,
        ordinal: capture.ordinal,
        tool_call: capture.tool_call,
        first_step: capture.first_step,
        person: capture.person,
    }
}

fn capture_facts(capture: &StoredCapture) -> CaptureFacts {
    CaptureFacts {
        id: media_id(&capture.id),
        reference: capture.reference.clone(),
        ordinal: capture.ordinal,
        tool_call: capture.tool_call.clone(),
        first_step: capture.first_step,
        person: capture.person,
    }
}

fn stored_skeleton(message: &MediaMessage) -> StoredSkeleton {
    StoredSkeleton {
        role: message.role,
        blocks: message.blocks,
        slots: message
            .slots
            .iter()
            .map(|slot| StoredSlot {
                block: slot.block,
                call: slot.tool_part.as_ref().map(|(call, _)| call.clone()),
                part: slot.tool_part.as_ref().map(|(_, part)| *part),
                reference: slot.reference.clone(),
            })
            .collect(),
    }
}

fn media_message(skeleton: &StoredSkeleton) -> Result<MediaMessage, String> {
    let slots = skeleton
        .slots
        .iter()
        .map(|slot| {
            let tool_part = match (&slot.call, slot.part) {
                (Some(call), Some(part)) => Some((call.clone(), part)),
                (None, None) => None,
                _ => {
                    return Err(format!(
                        "a media slot at block {} names a tool call without its part, or a part \
                         without its call",
                        slot.block
                    ))
                }
            };
            Ok(MediaSlot {
                block: slot.block,
                tool_part,
                reference: slot.reference.clone(),
            })
        })
        .collect::<Result<_, String>>()?;
    Ok(MediaMessage {
        role: skeleton.role,
        blocks: skeleton.blocks,
        slots,
    })
}

fn stored_binding(binding: MediaBinding) -> StoredBinding {
    StoredBinding {
        id: stored_id(&binding.id),
        message: binding.location.message,
        block: binding.location.block,
        part: binding.location.part,
        reference: binding.reference,
        tool: binding.tool,
        protected: binding.protected,
    }
}

fn media_binding(binding: &StoredBinding) -> MediaBinding {
    MediaBinding {
        id: media_id(&binding.id),
        location: MediaLocation {
            message: binding.message,
            block: binding.block,
            part: binding.part,
        },
        reference: binding.reference.clone(),
        tool: binding.tool,
        protected: binding.protected,
    }
}

fn stored_occurrence(occurrence: &Occurrence) -> StoredOccurrence {
    StoredOccurrence {
        capture: stored_id(&occurrence.capture),
        repetition: occurrence.repetition,
    }
}

fn occurrence(occurrence: &StoredOccurrence) -> Occurrence {
    Occurrence {
        capture: media_id(&occurrence.capture),
        repetition: occurrence.repetition,
    }
}

#[cfg(test)]
mod tests;
