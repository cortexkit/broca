//! Session-scoped render inputs. Bytes are read only after an exact WAL-derived capture
//! match; membership in the shared object store alone never authorizes a render.
//!
//! Fold durable records once on resume, then only the newly durable tail. Call `bindings`
//! on the post-compaction-view prompt, not on the stored transcript. No media decisions or
//! bytes are written to the WAL by this projection.

use std::collections::{BTreeMap, HashMap};

use broca_protocol::{
    ContentBlock, MediaRef, Message, StepId, ToolCallId, ToolOutput, ToolOutputPart,
};
use broca_wal::WalRecord;

use crate::media_store::{MediaReadError, MediaStore};
use crate::placement::{AppendKind, Placement, PlacementAction};

/// A capture occurrence, not a digest. Two admissions of identical bytes have different ids
/// and can have different protection lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MediaId {
    pub record: u64,
    pub message: usize,
    pub block: usize,
    pub part: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaLocation {
    pub message: usize,
    pub block: usize,
    pub part: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaBinding {
    pub id: MediaId,
    pub location: MediaLocation,
    pub reference: MediaRef,
    pub tool: bool,
    pub protected: bool,
}

#[derive(Debug, Clone)]
struct Capture {
    id: MediaId,
    reference: MediaRef,
    ordinal: Option<u64>,
    tool_call: Option<ToolCallId>,
    // None is an admitted input waiting for its first durable StepStarted.
    first_step: Option<u64>,
    person: bool,
}

type Position = (u64, Option<ToolCallId>, String);

/// Metadata deltas for an aging fold. A selected view changes rarely; its snapshot replaces
/// the selected set. All other records emit only new captures and protection releases.
#[derive(Debug)]
pub enum MediaChange {
    Show(MediaBinding),
    Release(MediaId),
    View(Vec<MediaBinding>),
}

/// Metadata-only projection paired with one session's media store. The caller must retain
/// the session lease while reading, just as it does while registering captures.
pub struct SessionMedia {
    store: MediaStore,
    placement: Placement,
    records: u64,
    boundary: u64,
    last_step: Option<StepId>,
    captures: HashMap<MediaId, Capture>,
    digests: HashMap<String, Vec<MediaId>>,
    sources: HashMap<String, Vec<MediaId>>,
    positions: HashMap<Position, Vec<MediaId>>,
    tools: HashMap<(ToolCallId, Option<usize>), Vec<MediaId>>,
    pending: Vec<MediaId>,
    steers: Vec<Vec<MediaId>>,
    protected: Vec<MediaId>,
    changes: Vec<MediaChange>,
    skeleton: BTreeMap<u64, Message>,
    held_messages: HashMap<(u64, usize), Message>,
    compaction: crate::compaction::CompactionState,
    batch_order: HashMap<ToolCallId, usize>,
    replacements: Vec<(Vec<MediaRef>, u64, u64)>,
}

impl SessionMedia {
    pub fn new(store: MediaStore) -> Self {
        Self {
            store,
            placement: Placement::default(),
            records: 0,
            boundary: 0,
            last_step: None,
            captures: HashMap::new(),
            digests: HashMap::new(),
            sources: HashMap::new(),
            positions: HashMap::new(),
            tools: HashMap::new(),
            pending: Vec::new(),
            steers: Vec::new(),
            protected: Vec::new(),
            changes: Vec::new(),
            skeleton: BTreeMap::new(),
            held_messages: HashMap::new(),
            compaction: Default::default(),
            batch_order: HashMap::new(),
            replacements: Vec::new(),
        }
    }

    /// Fold in durable WAL order, including records with no media. The positional placement
    /// kernel is shared with replay so held steers cannot acquire a premature ordinal.
    pub fn fold(&mut self, record: &WalRecord) -> Result<(), String> {
        let tool_ordinal = self.placement.held_tool_result_ordinal();
        let actions = self.placement.observe(record)?;
        self.compaction.fold(record);
        self.records += 1;
        match record {
            WalRecord::RunStarted { input, .. } => {
                self.last_step = None;
                self.steers.clear();
                let first = actions
                    .iter()
                    .find_map(|action| match action {
                        PlacementAction::Append {
                            first,
                            kind: AppendKind::Input,
                            ..
                        } => Some(*first),
                        _ => None,
                    })
                    .expect("RunStarted places input");
                for (i, message) in input.iter().enumerate() {
                    let ids = self.capture_message(message, i, Some(first + i as u64), true);
                    self.pending.extend(ids);
                }
            }
            WalRecord::Steered { input, .. }
            | WalRecord::SendDelivered {
                input: Some(input), ..
            } => {
                for (i, message) in input.iter().enumerate() {
                    let ids = self.capture_message(message, i, None, true);
                    self.pending.extend(ids.iter().copied());
                    self.steers.push(ids);
                }
            }
            // Queue media become prompt occurrences only at the consuming RunStarted.
            WalRecord::ToolBatchStarted { planned_calls, .. } => {
                self.batch_order = planned_calls
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(i, id)| (id, i))
                    .collect();
            }
            WalRecord::ToolResult { result, .. } | WalRecord::LateResult { result, .. } => {
                if let ToolOutput::Parts { parts } = &result.output {
                    let block_index = self
                        .batch_order
                        .get(&result.tool_call_id)
                        .copied()
                        .unwrap_or(0);
                    let shown = matches!(record, WalRecord::ToolResult { .. });
                    if let Some(ordinal) = tool_ordinal.filter(|_| shown) {
                        let mut result = (**result).clone();
                        if let ToolOutput::Parts { parts } = &mut result.output {
                            for part in parts {
                                if let ToolOutputPart::Text { text } = part {
                                    text.clear();
                                }
                            }
                        }
                        let message = self.skeleton.entry(ordinal).or_insert_with(|| {
                            let mut message = Message::new(broca_protocol::Role::Tool, Vec::new());
                            message.ordinal = Some(ordinal);
                            message
                        });
                        if message.content.len() <= block_index {
                            message
                                .content
                                .resize_with(block_index + 1, || ContentBlock::Text {
                                    text: String::new(),
                                });
                        }
                        message.content[block_index] = ContentBlock::ToolResult(result);
                    }
                    for (part, value) in parts.iter().enumerate() {
                        if let ToolOutputPart::Media { media } = value {
                            self.insert(
                                Capture {
                                    id: MediaId {
                                        record: self.records,
                                        message: 0,
                                        block: block_index,
                                        part: Some(part),
                                    },
                                    reference: media.clone(),
                                    ordinal: tool_ordinal,
                                    tool_call: Some(result.tool_call_id.clone()),
                                    first_step: Some(self.boundary),
                                    person: false,
                                },
                                shown,
                            );
                        }
                    }
                }
            }
            _ => {}
        }
        for action in actions {
            if let PlacementAction::PlaceHeld { first, .. } = action {
                let steers = std::mem::take(&mut self.steers);
                for (i, ids) in steers.into_iter().enumerate() {
                    for id in ids {
                        self.captures.get_mut(&id).expect("capture id").ordinal =
                            Some(first + i as u64);
                        self.index_position(id);
                        self.show(id);
                        if let Some(mut message) =
                            self.held_messages.remove(&(id.record, id.message))
                        {
                            message.ordinal = Some(first + i as u64);
                            self.skeleton.insert(first + i as u64, message);
                        }
                    }
                }
            }
        }
        if let WalRecord::StepStarted { step_id } = record {
            if self.last_step != Some(*step_id) {
                for id in std::mem::take(&mut self.protected) {
                    self.changes.push(MediaChange::Release(id));
                }
                self.boundary += 1;
                self.last_step = Some(*step_id);
                for id in std::mem::take(&mut self.pending) {
                    self.captures.get_mut(&id).expect("capture id").first_step =
                        Some(self.boundary);
                    self.protected.push(id);
                }
            }
        }
        if matches!(
            record,
            WalRecord::CompactionMessage { .. } | WalRecord::CompactionViewApplied { .. }
        ) {
            self.index_replacements();
            self.changes
                .push(MediaChange::View(self.selected_bindings()));
        }
        Ok(())
    }

    fn index_replacements(&mut self) {
        use crate::compaction_provider::view::ViewRange;
        self.replacements.clear();
        if let Some(applied) = &self.compaction.view {
            for range in &applied.view.ranges {
                if let ViewRange::Replace { from, to, bytes } = range {
                    if let Ok(messages) = serde_json::from_str::<Vec<Message>>(bytes) {
                        for message in messages {
                            let refs: Vec<_> = crate::compaction::media_refs(&message.content)
                                .cloned()
                                .collect();
                            if !refs.is_empty() {
                                self.replacements.push((refs, *from, *to));
                            }
                        }
                    }
                }
            }
        } else if let Some(applied) = &self.compaction.applied {
            for message in applied.replacement.iter() {
                let refs: Vec<_> = crate::compaction::media_refs(&message.content)
                    .cloned()
                    .collect();
                if !refs.is_empty() {
                    self.replacements
                        .push((refs, applied.range.from, applied.range.to));
                }
            }
        }
    }

    /// Drain after each folded record. The final in-progress step must be aged after its
    /// latest compaction selection, before preparing its request, even if no attempt finished.
    pub fn take_changes(&mut self) -> Vec<MediaChange> {
        std::mem::take(&mut self.changes)
    }

    /// Re-select only when a compaction view or the episode's frozen profile changes. The
    /// skeleton holds media references and empty text slots, not a second transcript copy.
    pub fn selected_bindings(&self) -> Vec<MediaBinding> {
        let prompt: Vec<_> = self.skeleton.values().cloned().collect();
        let selected = crate::compaction::view::rendered_history(&self.compaction, &prompt);
        self.bindings(&selected)
    }

    fn show(&mut self, id: MediaId) {
        let capture = &self.captures[&id];
        let covered = self
            .compaction
            .view
            .as_ref()
            .map(|view| view.view.covers_through.ordinal)
            .or_else(|| {
                self.compaction
                    .applied
                    .as_ref()
                    .map(|applied| applied.range.to.saturating_sub(1))
            });
        if capture
            .ordinal
            .is_some_and(|ordinal| covered.is_some_and(|covered| ordinal <= covered))
        {
            return;
        }
        self.changes.push(MediaChange::Show(MediaBinding {
            id,
            location: MediaLocation {
                message: 0,
                block: id.block,
                part: id.part,
            },
            reference: capture.reference.clone(),
            tool: capture.tool_call.is_some(),
            protected: capture.person
                && capture.first_step.is_none_or(|step| step == self.boundary),
        }));
    }

    fn insert(&mut self, capture: Capture, announce: bool) {
        let id = capture.id;
        self.sources
            .entry(capture.reference.sha256.clone())
            .or_default()
            .push(id);
        let entries = self
            .digests
            .entry(capture.reference.sha256.clone())
            .or_default();
        if !entries
            .iter()
            .any(|id| self.captures[id].reference == capture.reference)
        {
            entries.push(id);
        }
        if let Some(call) = &capture.tool_call {
            self.tools
                .entry((call.clone(), id.part))
                .or_default()
                .push(id);
        }
        self.captures.insert(id, capture);
        self.index_position(id);
        if announce && (self.captures[&id].ordinal.is_some() || !self.captures[&id].person) {
            self.show(id);
        }
    }

    fn index_position(&mut self, id: MediaId) {
        let capture = &self.captures[&id];
        if let Some(ordinal) = capture.ordinal {
            self.positions
                .entry((
                    ordinal,
                    capture.tool_call.clone(),
                    capture.reference.sha256.clone(),
                ))
                .or_default()
                .push(id);
        }
    }

    fn capture_message(
        &mut self,
        message: &Message,
        index: usize,
        ordinal: Option<u64>,
        person: bool,
    ) -> Vec<MediaId> {
        let mut ids = Vec::new();
        let has_media = crate::compaction::media_refs(&message.content)
            .next()
            .is_some();
        if has_media {
            let mut metadata = message.clone();
            metadata.ordinal = ordinal;
            for block in &mut metadata.content {
                match block {
                    ContentBlock::Media(_) => {}
                    ContentBlock::ToolResult(result) => {
                        if let ToolOutput::Parts { parts } = &mut result.output {
                            for part in parts {
                                if let ToolOutputPart::Text { text } = part {
                                    text.clear();
                                }
                            }
                        }
                    }
                    _ => {
                        *block = ContentBlock::Text {
                            text: String::new(),
                        }
                    }
                }
            }
            if let Some(ordinal) = ordinal {
                self.skeleton.insert(ordinal, metadata);
            } else {
                self.held_messages.insert((self.records, index), metadata);
            }
        }
        for (block, value) in message.content.iter().enumerate() {
            match value {
                ContentBlock::Media(media) => {
                    let id = MediaId {
                        record: self.records,
                        message: index,
                        block,
                        part: None,
                    };
                    self.insert(
                        Capture {
                            id,
                            reference: media.clone(),
                            ordinal,
                            tool_call: None,
                            first_step: None,
                            person,
                        },
                        true,
                    );
                    ids.push(id);
                }
                ContentBlock::ToolResult(result) => {
                    if let ToolOutput::Parts { parts } = &result.output {
                        for (part, value) in parts.iter().enumerate() {
                            if let ToolOutputPart::Media { media } = value {
                                self.insert(
                                    Capture {
                                        id: MediaId {
                                            record: self.records,
                                            message: index,
                                            block,
                                            part: Some(part),
                                        },
                                        reference: media.clone(),
                                        ordinal,
                                        tool_call: Some(result.tool_call_id.clone()),
                                        first_step: Some(self.boundary),
                                        person: false,
                                    },
                                    true,
                                );
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        ids
    }

    /// Enumerate the media of the assembled view once, in prompt order. A missing binding
    /// is deliberately absent: the provider must replace it with a provenance marker.
    /// Replacement messages have no ordinal; their references inherit the oldest matching
    /// capture's age, never a new input's protection merely because the digest is shared.
    pub fn bindings(&self, prompt: &[Message]) -> Vec<MediaBinding> {
        let mut out = Vec::new();
        let mut cursors = HashMap::<Position, usize>::new();
        let mut replacement_cursor = 0;
        for (message_index, message) in prompt.iter().enumerate() {
            let carried = if message.ordinal.is_none() {
                let refs: Vec<_> = crate::compaction::media_refs(&message.content)
                    .cloned()
                    .collect();
                let found = self.replacements[replacement_cursor..]
                    .iter()
                    .position(|(candidate, _, _)| *candidate == refs);
                found.map(|offset| {
                    replacement_cursor += offset + 1;
                    let (_, from, to) = &self.replacements[replacement_cursor - 1];
                    (*from, *to)
                })
            } else {
                None
            };
            for (block_index, block) in message.content.iter().enumerate() {
                let mut bind = |reference: &MediaRef,
                                part: Option<usize>,
                                tool_call: Option<&ToolCallId>| {
                    let position = message
                        .ordinal
                        .map(|ordinal| (ordinal, tool_call.cloned(), reference.sha256.clone()));
                    let ids = if let Some(position) = &position {
                        self.positions.get(position)
                    } else if carried.is_some() {
                        self.sources.get(&reference.sha256)
                    } else if let Some(call) = tool_call {
                        self.tools.get(&(call.clone(), part))
                    } else {
                        self.digests.get(&reference.sha256)
                    };
                    let cursor = position.map(|position| cursors.entry(position).or_default());
                    let capture = ids.and_then(|ids| {
                        if let Some((from, to)) = carried {
                            return ids
                                .iter()
                                .map(|id| &self.captures[id])
                                .filter(|capture| {
                                    capture.reference == *reference
                                        && capture
                                            .ordinal
                                            .is_some_and(|ordinal| from <= ordinal && ordinal < to)
                                })
                                .min_by_key(|capture| (!self.is_protected(capture), capture.id));
                        }
                        let start = cursor.as_deref().copied().unwrap_or(0);
                        let found = ids[start.min(ids.len())..]
                            .iter()
                            .map(|id| &self.captures[id])
                            .find(|capture| capture.reference == *reference);
                        if let Some(cursor) = cursor {
                            *cursor += 1;
                        }
                        found.or_else(|| {
                            ids.iter()
                                .map(|id| &self.captures[id])
                                .find(|capture| capture.reference == *reference)
                        })
                    });
                    if let Some(capture) = capture {
                        out.push(MediaBinding {
                            id: capture.id,
                            location: MediaLocation {
                                message: message_index,
                                block: block_index,
                                part,
                            },
                            reference: reference.clone(),
                            tool: tool_call.is_some(),
                            protected: self.is_protected(capture),
                        });
                    }
                };
                match block {
                    ContentBlock::Media(media) => bind(media, None, None),
                    ContentBlock::ToolResult(result) => {
                        if let ToolOutput::Parts { parts } = &result.output {
                            for (part, value) in parts.iter().enumerate() {
                                if let ToolOutputPart::Media { media } = value {
                                    bind(media, Some(part), Some(&result.tool_call_id));
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

    fn is_protected(&self, capture: &Capture) -> bool {
        capture.person && capture.first_step.is_none_or(|step| step == self.boundary)
    }

    /// Read and verify afresh. Exact metadata equality is required as well as a digest match.
    pub fn read(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        if !self.digests.get(&reference.sha256).is_some_and(|ids| {
            ids.iter()
                .any(|id| self.captures[id].reference == *reference)
        }) {
            return Err(MediaReadError::Corrupt(reference.sha256.clone()));
        }
        self.store.read_verified(reference)
    }
}
