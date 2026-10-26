//! Preparation shared by the native media renderers. This module does not change any family's
//! wire grammar. Call it with the post-compaction-view prompt, before tag rendering, then use
//! `PreparedMedia::base64` at the corresponding location in the family's request shape.
//!
//! `prepare` enforces the encoded media payload budget. A renderer MUST subsequently call
//! `fit_request` with the length of its actual serialized request (including markers, text,
//! tools, wrappers and base64). Admission must use that same measurement before publishing
//! new input. Payload-only preparation is not proof that a complete request fits.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use broca_core::media_request::{MediaBinding, MediaChange, MediaId, MediaLocation, SessionMedia};
use broca_protocol::{
    content::media_not_shown_marker, ContentBlock, MediaKind, MediaMarkerReason, MediaRef, Message,
    ToolOutput, ToolOutputPart,
};

use crate::media_profile::{MediaCarrier, MediaProfile};

/// An occurrence in the assembled prompt. Replacement messages can repeat a captured reference;
/// each repetition counts against the budget even though the object is read from one digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Occurrence {
    pub capture: MediaId,
    pub repetition: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaCost {
    pub bytes: u64,
    pub tokens: u64,
}

/// Deltas derived from durable captures and the selected compaction view. These are replay
/// inputs, not new WAL records. Replay them in order, with a Boundary after selecting each
/// durable step's view. Reapplying an unchanged Boundary on retry is idempotent.
#[derive(Debug, Clone, Copy)]
pub enum AgingEvent {
    Show {
        id: Occurrence,
        cost: MediaCost,
        protected: bool,
    },
    Hide {
        id: Occurrence,
    },
    Release {
        id: Occurrence,
    },
    Boundary,
}

/// Running totals and an oldest-first queue. Rebuilding from reference deltas is O(media);
/// advancing an unchanged view touches only newly shown or newly unprotected media. Bytes
/// and message text are never retained here. An aged occurrence never becomes young again
/// merely because a compaction view temporarily hides it.
#[derive(Debug, Default)]
pub struct MediaAging {
    shown: BTreeMap<Occurrence, (MediaCost, bool)>,
    unprotected: BTreeSet<Occurrence>,
    aged: BTreeSet<Occurrence>,
    by_capture: HashMap<MediaId, BTreeSet<Occurrence>>,
    bytes: u64,
    tokens: u64,
}

impl MediaAging {
    /// Reconcile a newly selected post-compaction view. At a step with no view change,
    /// `SessionMedia::take_changes` supplies only new captures and protection releases;
    /// feed those to `fold_changes` instead. Return deltas for an independent replay fold.
    pub fn select_view(
        &mut self,
        profile: &MediaProfile,
        bindings: &[MediaBinding],
    ) -> Vec<AgingEvent> {
        let mut events = self.reconcile_view(profile, bindings);
        self.apply(profile, AgingEvent::Boundary);
        events.push(AgingEvent::Boundary);
        events
    }

    fn reconcile_view(
        &mut self,
        profile: &MediaProfile,
        bindings: &[MediaBinding],
    ) -> Vec<AgingEvent> {
        let entries = aging_inputs(profile, bindings);
        let selected: BTreeSet<_> = entries
            .iter()
            .filter_map(|entry| match entry {
                AgingEvent::Show { id, .. } => Some(*id),
                _ => None,
            })
            .collect();
        let mut events: Vec<_> = self
            .shown
            .keys()
            .filter(|id| !selected.contains(id))
            .map(|id| AgingEvent::Hide { id: *id })
            .collect();
        for entry in entries {
            if let AgingEvent::Show {
                id,
                cost,
                protected,
            } = entry
            {
                match self.shown.get(&id) {
                    Some((previous, was_protected))
                        if previous == &cost && *was_protected == protected => {}
                    Some((previous, true)) if previous == &cost && !protected => {
                        events.push(AgingEvent::Release { id })
                    }
                    _ => events.push(entry),
                }
            }
        }
        for event in &events {
            self.apply(profile, *event);
        }
        events
    }

    /// Fold the core's reference-only WAL projection. No transcript walk or byte read is
    /// needed on an unchanged view. Apply Boundary after the step's selected view is final.
    pub fn fold_changes(
        &mut self,
        profile: &MediaProfile,
        changes: impl IntoIterator<Item = MediaChange>,
    ) {
        for change in changes {
            match change {
                MediaChange::Show(binding) => {
                    for event in aging_inputs(profile, &[binding]) {
                        self.apply(profile, event);
                    }
                }
                MediaChange::Release(capture) => {
                    // A replacement can repeat a capture; all occurrences share its protection
                    // boundary but each keeps its own cost in the running totals.
                    let ids = self.by_capture.get(&capture).cloned().unwrap_or_default();
                    for id in ids {
                        self.apply(profile, AgingEvent::Release { id });
                    }
                }
                MediaChange::View(bindings) => {
                    self.reconcile_view(profile, &bindings);
                }
            }
        }
    }

    /// Rebuild from durable records, without hydrating historical requests or writing an
    /// aging record. Compaction changes re-select reference metadata; unchanged steps fold
    /// only their new media. The caller supplies the current episode's frozen profile, not
    /// today's catalog. A changed recorded profile reselects metadata under its new costs;
    /// another episode with the same profile keeps the aging prefix.
    pub fn replay_wal(
        profile: &MediaProfile,
        session: &mut SessionMedia,
        records: &[broca_wal::WalRecord],
    ) -> Result<Self, String> {
        let mut state = Self::default();
        let mut step_open = false;
        let mut active = profile.clone();
        for record in records {
            session.fold(record)?;
            let frozen = match record {
                broca_wal::WalRecord::RunStarted { config, .. } => {
                    Some(&config.frozen_render_config)
                }
                broca_wal::WalRecord::RunConfigRefrozen {
                    frozen_render_config,
                    ..
                } => Some(frozen_render_config),
                _ => None,
            };
            let next: Option<MediaProfile> = frozen
                .and_then(|frozen| frozen.get("media"))
                .filter(|value| !value.is_null())
                .map(|value| {
                    serde_json::from_value(value.clone())
                        .map_err(|error| format!("invalid frozen media profile: {error}"))
                })
                .transpose()?;
            if let Some(next) = next.filter(|next| next != &active) {
                active = next;
                state = Self::default();
                state.reconcile_view(&active, &session.selected_bindings());
                session.take_changes();
            } else {
                state.fold_changes(&active, session.take_changes());
            }
            match record {
                broca_wal::WalRecord::RunStarted { .. } => step_open = false,
                broca_wal::WalRecord::StepStarted { .. } => step_open = true,
                broca_wal::WalRecord::ModelAttemptFinished { .. } => {
                    state.apply(&active, AgingEvent::Boundary)
                }
                broca_wal::WalRecord::ModelStepFinished { .. } => {
                    state.apply(&active, AgingEvent::Boundary);
                    step_open = false;
                }
                _ => {}
            }
        }
        if step_open {
            state.apply(&active, AgingEvent::Boundary);
        }
        Ok(state)
    }

    pub fn replay(profile: &MediaProfile, events: impl IntoIterator<Item = AgingEvent>) -> Self {
        let mut state = Self::default();
        for event in events {
            state.apply(profile, event);
        }
        state
    }

    pub fn apply(&mut self, profile: &MediaProfile, event: AgingEvent) {
        match event {
            AgingEvent::Show {
                id,
                cost,
                protected,
            } => {
                self.remove(id);
                self.shown.insert(id, (cost, protected));
                self.by_capture.entry(id.capture).or_default().insert(id);
                if !protected && !self.aged.contains(&id) {
                    self.add(id, cost);
                }
            }
            AgingEvent::Hide { id } => self.remove(id),
            AgingEvent::Release { id } => {
                if let Some((cost, protected)) = self.shown.get_mut(&id) {
                    if *protected {
                        *protected = false;
                        let cost = *cost;
                        if !self.aged.contains(&id) {
                            self.add(id, cost);
                        }
                    }
                }
            }
            AgingEvent::Boundary => {
                let thresholds = profile.aging;
                if self.bytes > thresholds.byte_trigger || self.tokens > thresholds.token_trigger {
                    while self.bytes > thresholds.byte_target
                        || self.tokens > thresholds.token_target
                    {
                        let Some(id) = self.unprotected.pop_first() else {
                            break;
                        };
                        let cost = self.shown[&id].0;
                        self.bytes -= cost.bytes;
                        self.tokens -= cost.tokens;
                        self.aged.insert(id);
                    }
                }
            }
        }
    }

    fn add(&mut self, id: Occurrence, cost: MediaCost) {
        self.unprotected.insert(id);
        self.bytes = self.bytes.saturating_add(cost.bytes);
        self.tokens = self.tokens.saturating_add(cost.tokens);
    }

    fn remove(&mut self, id: Occurrence) {
        if self.unprotected.remove(&id) {
            let cost = self.shown[&id].0;
            self.bytes -= cost.bytes;
            self.tokens -= cost.tokens;
        }
        self.shown.remove(&id);
        if let Some(ids) = self.by_capture.get_mut(&id.capture) {
            ids.remove(&id);
            if ids.is_empty() {
                self.by_capture.remove(&id.capture);
            }
        }
    }

    pub fn is_aged(&self, id: Occurrence) -> bool {
        self.aged.contains(&id)
    }

    pub fn totals(&self) -> MediaCost {
        MediaCost {
            bytes: self.bytes,
            tokens: self.tokens,
        }
    }
}

/// Only shown media have bytes. Debug output deliberately does not print payloads.
#[derive(Clone, PartialEq, Eq)]
pub struct PreparedMedia {
    pub id: Occurrence,
    pub location: MediaLocation,
    pub reference: MediaRef,
    pub carrier: MediaCarrier,
    pub protected: bool,
    pub tool: bool,
    pub base64: String,
}

impl std::fmt::Debug for PreparedMedia {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedMedia")
            .field("id", &self.id)
            .field("location", &self.location)
            .field("encoded_bytes", &self.encoded_bytes())
            .finish_non_exhaustive()
    }
}

impl PreparedMedia {
    pub fn encoded_bytes(&self) -> u64 {
        self.base64.len() as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedBundle {
    /// References stay at their locations; markers replace media that are not shown. Stored
    /// messages and tag identities are unchanged. Render tags only after this preparation.
    pub prompt: Vec<Message>,
    pub media: Vec<PreparedMedia>,
}

impl PreparedBundle {
    /// Add these tokens to the non-media estimate; do not feed inline base64 through a
    /// bytes-per-token heuristic. This total follows the frozen family's media rules.
    pub fn estimated_media_tokens(&self, profile: &MediaProfile) -> u64 {
        self.media
            .iter()
            .map(|media| profile.media_tokens(&media.reference))
            .sum()
    }

    pub fn encoded_media_bytes(&self) -> u64 {
        self.media.iter().map(PreparedMedia::encoded_bytes).sum()
    }

    fn mark(&mut self, index: usize, reason: MediaMarkerReason) {
        let item = self.media.remove(index);
        mark(&mut self.prompt, item.location, &item.reference, reason);
    }
}

#[derive(Debug)]
pub enum PrepareError {
    Object(broca_core::media_store::MediaReadError),
    RequestTooLarge,
}

impl PrepareError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Object(error) => error.code().unwrap_or("media_object_corrupt"),
            Self::RequestTooLarge => "request_too_large",
        }
    }
}

impl std::fmt::Display for PrepareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Object(error) => error.fmt(f),
            Self::RequestTooLarge => f.write_str("request_too_large: protected media or non-media request content still exceeds the frozen profile's limits"),
        }
    }
}

impl std::error::Error for PrepareError {}

/// Hydrate an already-selected view. `aging` must have received the view's metadata deltas and
/// durable Boundary first. Preparation is pure with respect to that state, so attempts and
/// retries cannot themselves advance aging. A resumed caller rebuilds it with `MediaAging::replay`.
pub fn prepare(
    session: &SessionMedia,
    profile: Option<&MediaProfile>,
    prompt: &[Message],
    aging: &MediaAging,
) -> Result<PreparedBundle, PrepareError> {
    let bindings = session.bindings(prompt);
    let mut by_location: HashMap<_, _> = bindings
        .iter()
        .map(|binding| {
            (
                (
                    binding.location.message,
                    binding.location.block,
                    binding.location.part,
                ),
                binding,
            )
        })
        .collect();
    let mut bundle = PreparedBundle {
        prompt: prompt.to_vec(),
        media: Vec::new(),
    };
    let mut repeats = BTreeMap::<MediaId, u32>::new();
    for (location, reference, tool) in references(prompt) {
        let binding = by_location.remove(&(location.message, location.block, location.part));
        let id = binding.map(|binding| {
            let repetition = repeats.entry(binding.id).or_default();
            let id = Occurrence {
                capture: binding.id,
                repetition: *repetition,
            };
            *repetition += 1;
            id
        });
        let reason = if binding.is_none() {
            Some(MediaMarkerReason::Provenance)
        } else if let Some(profile) = profile {
            eligibility(profile, reference, tool).err().or_else(|| {
                (binding.is_some_and(|binding| !binding.protected) && aging.is_aged(id.unwrap()))
                    .then_some(MediaMarkerReason::Aged)
            })
        } else {
            Some(MediaMarkerReason::Modality)
        };
        if let Some(reason) = reason {
            mark(&mut bundle.prompt, location, reference, reason);
            continue;
        }
        let profile = profile.expect("eligible profile");
        let bytes = session.read(reference).map_err(PrepareError::Object)?;
        let binding = binding.expect("eligible capture");
        bundle.media.push(PreparedMedia {
            id: id.expect("eligible occurrence"),
            location,
            reference: reference.clone(),
            carrier: if tool {
                profile.kind(reference.kind).tool_result
            } else {
                profile.kind(reference.kind).user_message
            },
            protected: binding.protected,
            tool,
            base64: STANDARD.encode(bytes),
        });
    }
    if let Some(profile) = profile {
        fit_request(profile, &mut bundle, PreparedBundle::encoded_media_bytes)?;
    }
    Ok(bundle)
}

fn eligibility(
    profile: &MediaProfile,
    media: &MediaRef,
    tool: bool,
) -> Result<(), MediaMarkerReason> {
    if !profile.accepts_modality(media.kind) {
        return Err(MediaMarkerReason::Modality);
    }
    let kind = profile.kind(media.kind);
    let carrier = if tool {
        kind.tool_result
    } else {
        kind.user_message
    };
    if carrier == MediaCarrier::Marker {
        return Err(MediaMarkerReason::Carrier);
    }
    if !crate::person_media::accepts_mime(profile, media) {
        return Err(MediaMarkerReason::Mime);
    }
    if kind
        .max_item_bytes
        .is_some_and(|limit| !limit.admits(media.byte_size))
    {
        return Err(MediaMarkerReason::TooLarge);
    }
    Ok(())
}

/// Turn the assembled view into replayable metadata. No bytes are read for accounting: the
/// store verifies byte_size before hydration and padded base64 has exactly this length.
/// Diff these entries against the previous selected view to emit Show/Hide/Release events.
/// Boundary is idempotent when retrying the same durable step and selected view.
pub fn aging_inputs(profile: &MediaProfile, bindings: &[MediaBinding]) -> Vec<AgingEvent> {
    let mut repeats = BTreeMap::<MediaId, u32>::new();
    bindings
        .iter()
        .filter_map(|binding| {
            eligibility(profile, &binding.reference, binding.tool).ok()?;
            let repetition = repeats.entry(binding.id).or_default();
            let id = Occurrence {
                capture: binding.id,
                repetition: *repetition,
            };
            *repetition += 1;
            Some(AgingEvent::Show {
                id,
                cost: MediaCost {
                    bytes: crate::media_profile::base64_encoded_len(binding.reference.byte_size),
                    tokens: profile.media_tokens(&binding.reference),
                },
                protected: binding.protected,
            })
        })
        .collect()
}

/// Re-render after each removal: marker and envelope overhead is not a constant, and limits
/// may stop applying as soon as the image count crosses a dimension threshold. A renderer's
/// closure must return the actual serialized byte count, not a raw-byte or token estimate.
pub fn fit_request(
    profile: &MediaProfile,
    bundle: &mut PreparedBundle,
    encoded_request_bytes: impl Fn(&PreparedBundle) -> u64,
) -> Result<(), PrepareError> {
    loop {
        if encoded_request_bytes(bundle) <= profile.request_cap_bytes
            && !breaks_counts(profile, bundle)
        {
            return Ok(());
        }
        let oldest = bundle
            .media
            .iter()
            .enumerate()
            .filter(|(_, media)| !media.protected)
            .min_by_key(|(_, media)| (!media.tool, media.id))
            .map(|(index, _)| index);
        let Some(index) = oldest else {
            return Err(PrepareError::RequestTooLarge);
        };
        bundle.mark(index, MediaMarkerReason::RequestCap);
    }
}

fn breaks_counts(profile: &MediaProfile, bundle: &PreparedBundle) -> bool {
    let mut counts = [0u32; 4];
    for media in &bundle.media {
        let index = match media.reference.kind {
            MediaKind::Image => 0,
            MediaKind::Document => 1,
            MediaKind::Audio => 2,
            MediaKind::Video => 3,
        };
        counts[index] += 1;
        if profile
            .kind(media.reference.kind)
            .max_count
            .is_some_and(|max| counts[index] > max)
        {
            return true;
        }
    }
    profile
        .max_image_side(counts[0], counts[1])
        .is_some_and(|max| {
            bundle.media.iter().any(|media| {
                media.reference.kind == MediaKind::Image
                    && (media.reference.width.unwrap_or(0) > max
                        || media.reference.height.unwrap_or(0) > max)
            })
        })
}

fn references(prompt: &[Message]) -> Vec<(MediaLocation, &MediaRef, bool)> {
    let mut out = Vec::new();
    for (message, value) in prompt.iter().enumerate() {
        for (block, value) in value.content.iter().enumerate() {
            match value {
                ContentBlock::Media(media) => out.push((
                    MediaLocation {
                        message,
                        block,
                        part: None,
                    },
                    media,
                    false,
                )),
                ContentBlock::ToolResult(result) => {
                    if let ToolOutput::Parts { parts } = &result.output {
                        for (part, value) in parts.iter().enumerate() {
                            if let ToolOutputPart::Media { media } = value {
                                out.push((
                                    MediaLocation {
                                        message,
                                        block,
                                        part: Some(part),
                                    },
                                    media,
                                    true,
                                ));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    out
}

fn mark(
    prompt: &mut [Message],
    location: MediaLocation,
    media: &MediaRef,
    reason: MediaMarkerReason,
) {
    let text = media_not_shown_marker(media, reason);
    let block = &mut prompt[location.message].content[location.block];
    if let Some(part) = location.part {
        if let ContentBlock::ToolResult(result) = block {
            if let ToolOutput::Parts { parts } = &mut result.output {
                parts[part] = ToolOutputPart::Text { text };
            }
        }
    } else {
        *block = ContentBlock::Text { text };
    }
}

#[cfg(test)]
mod tests;
