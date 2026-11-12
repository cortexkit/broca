//! Session-scoped render inputs. Bytes are read only after an exact WAL-derived capture
//! match; membership in the shared object store alone never authorizes a render.
//!
//! Fold durable records once on resume, then only the newly durable tail. Call `bindings`
//! on the post-compaction-view prompt, not on the stored transcript. No media decisions or
//! bytes are written to the WAL by this projection.

use std::collections::{BTreeMap, HashMap};

use broca_protocol::{
    media_event::{MediaEvent, MediaMessage},
    ContentBlock, MediaRef, Message, StepId, ToolCallId, ToolOutput, ToolOutputPart,
};
use broca_wal::WalRecord;
use serde::{Deserialize, Serialize};

use crate::media_store::{MediaBytes, MediaReadError, MediaStoreClass, ReadOnlyMediaStore};
use crate::placement::{AppendKind, Placement, PlacementAction};

mod initialization;
mod seed;
mod wal_events;
pub use initialization::{initialize_media_checkpoint, InitializedMedia};
pub use wal_events::{WalMediaEvents, WalMediaEventsError};

impl seed::MediaCheckpoint {
    /// Restore the authenticated compaction state without replaying media admissions;
    /// their occurrence IDs and transcript placement are already restored.
    pub(crate) fn restore_authenticated_compaction(
        &mut self,
        authenticated: crate::admission::AuthenticatedCompaction,
    ) {
        self.session.compaction = authenticated.into_state();
    }

    pub(crate) fn prepare_content_free_continuation(&mut self) {
        // Without a replacement view, the saved media references and empty text slots
        // suffice to recompute selection as the tail grows. A MediaChange::Show announces
        // newly visible media at position zero; the selected index instead needs its
        // full-fold position among all visible media.
        if !self.session.restored_view {
            self.session.restored_selection = None;
            self.session.restored_covered = None;
        }
    }

    /// A qualified media-free prefix has no captures or held media. Retain its current
    /// profile and absolute placement when the first capture arrives in a later suffix.
    pub(crate) fn after_media_free_prefix(
        placement: crate::placement::Placement,
        seq: u64,
        frames: &[broca_wal::FramedRecord],
    ) -> Result<Self, String> {
        let mut checkpoint = Self::empty_at(placement, seq);
        let mut profile = None;
        for frame in frames {
            let frozen = match &frame.record {
                WalRecord::RunStarted { config, .. } => Some(&config.frozen_render_config),
                WalRecord::RunConfigRefrozen {
                    frozen_render_config,
                    ..
                } => Some(frozen_render_config),
                _ => None,
            };
            if let Some(media) = frozen.and_then(|v| v.get("media")).filter(|v| !v.is_null()) {
                if profile.as_ref() != Some(media) {
                    profile = Some(media.clone());
                    checkpoint.history.push(seed::MediaHistory::Profile {
                        media: media.clone(),
                        selected: Vec::new(),
                    });
                }
            }
            if matches!(
                frame.record,
                WalRecord::ModelAttemptFinished { .. } | WalRecord::ModelStepFinished { .. }
            ) && !matches!(
                checkpoint.history.last(),
                Some(seed::MediaHistory::Boundary)
            ) {
                checkpoint.history.push(seed::MediaHistory::Boundary);
            }
        }
        // Decoding preserves the last profile so later profiles can be compared with it.
        Self::decode(&checkpoint.encode())
    }
}
pub use seed::count_seed_fallback;
pub use seed::{
    seed_fallback_count, DurableMediaSeed, MediaCheckpoint, MediaHistory, MediaSeedFold,
};

/// A capture occurrence, not a digest. Two admissions of identical bytes have different ids
/// and can have different protection lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MediaId {
    pub record: u64,
    pub message: usize,
    pub block: usize,
    pub part: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaLocation {
    pub message: usize,
    pub block: usize,
    pub part: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaBinding {
    pub id: MediaId,
    pub location: MediaLocation,
    pub reference: MediaRef,
    pub tool: bool,
    pub protected: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MediaChange {
    Show(MediaBinding),
    Release(MediaId),
    View(Vec<MediaBinding>),
}

/// One capture as [`MediaFacts`] lists it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CaptureFacts {
    pub id: MediaId,
    pub reference: MediaRef,
    pub ordinal: Option<u64>,
    pub tool_call: Option<ToolCallId>,
    /// The step the capture first showed in; `None` while a person's medium waits for it.
    pub first_step: Option<u64>,
    pub person: bool,
}

/// Everything a [`SessionMedia`] knows about a session's media, in a fixed order, so two
/// engines fed the same history the two ways (old records through `fold`, events through
/// `apply`) can be compared whole. Content-free, like the engine itself. The old placement
/// state and the record counter are left out: only `fold` keeps them.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MediaFacts {
    /// The step counter.
    pub boundary: u64,
    /// The open step, whose repeated start is a retry.
    pub last_step: Option<StepId>,
    /// In id order.
    pub captures: Vec<CaptureFacts>,
    /// Captures waiting for their first step.
    pub pending: Vec<MediaId>,
    /// Accepted messages not placed yet, one entry of capture ids per message.
    pub held: Vec<Vec<MediaId>>,
    /// Captures protected from aging for the current step.
    pub protected: Vec<MediaId>,
    /// The text-free copies of placed messages the engine keeps, by ordinal: messages that
    /// hold media, and results messages of tool results whose output is a list of parts.
    pub skeleton: Vec<(u64, MediaMessage)>,
    /// Held messages that hold media, by (record, position in the record).
    pub held_messages: Vec<((u64, usize), MediaMessage)>,
    /// The open batch's calls with their positions, in position order.
    pub batch_order: Vec<(ToolCallId, usize)>,
    /// What [`SessionMedia::selected_bindings`] answers.
    pub selected: Vec<MediaBinding>,
    pub covered: Option<u64>,
    pub replacements: Vec<(Vec<MediaRef>, u64, u64)>,
    /// The lookups `bindings` answers from, keyed and sorted for comparison; each list of
    /// ids keeps its own order, which decides which capture a repeated position means.
    pub positions: FactsLookup<(u64, Option<String>, String)>,
    pub digests: FactsLookup<String>,
    pub sources: FactsLookup<String>,
    pub tools: FactsLookup<(String, Option<usize>)>,
}

/// One of the engine's lookups as [`MediaFacts`] lists it: each key, sorted, with the capture
/// ids it holds in their own order.
pub type FactsLookup<K> = Vec<(K, Vec<MediaId>)>;

/// Metadata-only projection paired with one session's media store. Live callers retain
/// the session lease; offline readers verify afresh and refuse objects reclaimed meanwhile.
#[derive(Debug, Clone)]
pub struct SessionMedia {
    store: Option<std::sync::Arc<dyn MediaBytes>>,
    read_only: Option<(std::path::PathBuf, MediaStoreClass)>,
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
    restored_selection: Option<Vec<MediaBinding>>,
    restored_covered: Option<u64>,
    restored_view: bool,
}

impl SessionMedia {
    pub fn new(store: impl MediaBytes + 'static) -> Self {
        Self::metadata(Some(std::sync::Arc::new(store)))
    }

    /// Resolve no files until preparation actually requires an eligible native object.
    /// Historical references hidden by compaction or aging need no store on disk.
    pub fn read_only(state_root: &std::path::Path, class: MediaStoreClass) -> Self {
        let mut session = Self::metadata(None);
        session.read_only = Some((state_root.to_owned(), class));
        session
    }

    fn metadata(store: Option<std::sync::Arc<dyn MediaBytes>>) -> Self {
        Self {
            store,
            read_only: None,
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
            restored_selection: None,
            restored_covered: None,
            restored_view: false,
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
                self.start_run();
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
                self.order_batch(planned_calls.iter().cloned());
            }
            WalRecord::ToolResult { result, .. } | WalRecord::LateResult { result, .. } => {
                if let ToolOutput::Parts { parts } = &result.output {
                    let shown = matches!(record, WalRecord::ToolResult { .. });
                    self.capture_tool_result(
                        &result.tool_call_id,
                        tool_ordinal,
                        shown,
                        || {
                            let mut result = (**result).clone();
                            if let ToolOutput::Parts { parts } = &mut result.output {
                                for part in parts {
                                    if let ToolOutputPart::Text { text } = part {
                                        text.clear();
                                    }
                                }
                            }
                            result
                        },
                        parts
                            .iter()
                            .enumerate()
                            .filter_map(|(part, value)| match value {
                                ToolOutputPart::Media { media } => Some((part, media)),
                                ToolOutputPart::Text { .. } => None,
                            }),
                    );
                }
            }
            _ => {}
        }
        for action in actions {
            if let PlacementAction::PlaceHeld { first, .. } = action {
                self.place_held(first);
            }
        }
        if let WalRecord::StepStarted { step_id } = record {
            self.start_step(*step_id);
        }
        if matches!(
            record,
            WalRecord::CompactionMessage { .. } | WalRecord::CompactionViewApplied { .. }
        ) {
            if matches!(record, WalRecord::CompactionViewApplied { .. }) || !self.restored_view {
                self.restored_selection = None;
                self.restored_covered = None;
                self.restored_view = false;
            }
            self.index_replacements();
            self.changes
                .push(MediaChange::View(self.selected_bindings()));
        }
        Ok(())
    }

    /// Apply one content-free event: the second entry point beside [`Self::fold`], for a
    /// storage that states each fact the engine reads instead of writing the old records. It
    /// makes the state changes `fold` makes for the record the event stands for, through the
    /// same functions, but it runs neither the old placement rules nor the old compaction
    /// state: the event carries every ordinal. Changes are drained with
    /// [`Self::take_changes`], as after `fold`.
    ///
    /// Every reference the event captures also joins the set of references the session may
    /// render. `fold` adds those from the records themselves, including a queued send's
    /// before it is delivered; on events, a delivered send's media join when it is accepted.
    pub fn apply(&mut self, event: &MediaEvent) -> Result<(), String> {
        match event {
            MediaEvent::RunStart { record, inputs, .. } => {
                let messages = inputs
                    .iter()
                    .map(|input| input.skeleton.to_message())
                    .collect::<Result<Vec<_>, _>>()?;
                // `capture_message` names its captures by the record being folded.
                self.records = *record;
                self.start_run();
                for (input, message) in inputs.iter().zip(&messages) {
                    self.admit_references(message);
                    let ids =
                        self.capture_message(message, input.message, Some(input.ordinal), true);
                    self.pending.extend(ids);
                }
            }
            MediaEvent::Accepted { record, messages } => {
                let messages = messages
                    .iter()
                    .map(MediaMessage::to_message)
                    .collect::<Result<Vec<_>, _>>()?;
                self.records = *record;
                for (i, message) in messages.iter().enumerate() {
                    self.admit_references(message);
                    let ids = self.capture_message(message, i, None, true);
                    self.pending.extend(ids.iter().copied());
                    self.steers.push(ids);
                }
            }
            MediaEvent::Placed { first } => self.place_held(*first),
            MediaEvent::BatchStart { calls } => self.order_batch(calls.iter().cloned()),
            MediaEvent::ToolResult {
                record,
                call,
                ordinal,
                shown,
                media,
            } => {
                self.records = *record;
                for part in media {
                    self.compaction.captured.insert(&part.reference);
                }
                self.capture_tool_result(
                    call,
                    *ordinal,
                    *shown,
                    || {
                        let mut parts = Vec::new();
                        for media in media {
                            if parts.len() <= media.part {
                                parts.resize_with(media.part + 1, || ToolOutputPart::Text {
                                    text: String::new(),
                                });
                            }
                            parts[media.part] = ToolOutputPart::Media {
                                media: media.reference.clone(),
                            };
                        }
                        broca_protocol::ToolResult {
                            tool_call_id: call.clone(),
                            output: ToolOutput::Parts { parts },
                            is_error: false,
                        }
                    },
                    media.iter().map(|media| (media.part, &media.reference)),
                );
            }
            MediaEvent::StepStart { step } => self.start_step(*step),
            MediaEvent::Truncated { len } => self.truncate(*len),
            // Aging reads these; the captures and their protection don't change.
            MediaEvent::AttemptEnd | MediaEvent::StepEnd | MediaEvent::LimitsChanged { .. } => {}
        }
        Ok(())
    }

    /// A run starts: a step id of the previous run must not count as a retry of this run's
    /// step, and steers the previous run never placed are not this run's.
    fn start_run(&mut self) {
        self.last_step = None;
        self.steers.clear();
    }

    /// Add a message's references to the set the session may render. On events this is the
    /// only way in; `fold` adds them from the records it reads.
    fn admit_references(&mut self, message: &Message) {
        for reference in crate::compaction::media_refs(&message.content) {
            self.compaction.captured.insert(reference);
        }
    }

    /// Remember each planned call's position in the batch: its result takes that position
    /// (its block) in the batch's results message. A call id planned twice takes its last
    /// position.
    fn order_batch(&mut self, calls: impl Iterator<Item = ToolCallId>) {
        self.batch_order = calls.enumerate().map(|(i, id)| (id, i)).collect();
    }

    /// Capture the media of one tool result whose output is a list of parts. A shown result
    /// with an ordinal also puts its text-free copy (`skeleton`) at its call's block of the
    /// results message. A late result (`shown` false) is captured but neither placed nor
    /// announced.
    fn capture_tool_result<'a>(
        &mut self,
        call: &ToolCallId,
        ordinal: Option<u64>,
        shown: bool,
        skeleton: impl FnOnce() -> broca_protocol::ToolResult,
        media: impl Iterator<Item = (usize, &'a MediaRef)>,
    ) {
        let block_index = self.batch_order.get(call).copied().unwrap_or(0);
        if let Some(ordinal) = ordinal.filter(|_| shown) {
            let result = skeleton();
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
        for (part, media) in media {
            self.insert(
                Capture {
                    id: MediaId {
                        record: self.records,
                        message: 0,
                        block: block_index,
                        part: Some(part),
                    },
                    reference: media.clone(),
                    ordinal,
                    tool_call: Some(call.clone()),
                    first_step: Some(self.boundary),
                    person: false,
                },
                shown,
            );
        }
    }

    /// Place every held (accepted, not yet placed) message, in the order accepted, at
    /// consecutive ordinals from `first`. A message without media still takes its position.
    fn place_held(&mut self, first: u64) {
        let steers = std::mem::take(&mut self.steers);
        for (i, ids) in steers.into_iter().enumerate() {
            for id in ids {
                self.captures.get_mut(&id).expect("capture id").ordinal = Some(first + i as u64);
                self.index_position(id);
                self.show(id);
                if let Some(mut message) = self.held_messages.remove(&(id.record, id.message)) {
                    message.ordinal = Some(first + i as u64);
                    self.skeleton.insert(first + i as u64, message);
                }
            }
        }
    }

    /// A model step starts. Media protected for the previous step lose their protection, the
    /// step counter moves, and media waiting for their first step are protected for this one.
    /// Starting the step that is already open (a retry) changes nothing.
    fn start_step(&mut self, step_id: StepId) {
        if self.last_step != Some(step_id) {
            self.end_step();
            self.last_step = Some(step_id);
            for id in std::mem::take(&mut self.pending) {
                self.captures.get_mut(&id).expect("capture id").first_step = Some(self.boundary);
                self.protected.push(id);
            }
        }
    }

    /// The current step ends: the media it protected lose their protection, and the step
    /// counter moves past it. A person's medium is protected only while the counter still
    /// names the step it first showed in.
    fn end_step(&mut self) {
        for id in std::mem::take(&mut self.protected) {
            self.changes.push(MediaChange::Release(id));
            if let Some(selected) = &mut self.restored_selection {
                for binding in selected.iter_mut().filter(|binding| binding.id == id) {
                    binding.protected = false;
                }
            }
        }
        self.boundary += 1;
    }

    /// Cut the session back to the messages at ordinals below `len` (an undo, or a recovery
    /// that removed a damaged tail). Captures of later messages and of every held message are
    /// forgotten, with every lookup that names them; the run's batch and step end. Captures
    /// without an ordinal that aren't held (a tool result with no results message) stay, as
    /// they never had a position to lose. Aging learns of the removed captures from
    /// [`Self::holds`], and of the ended protection from the `Release` changes.
    fn truncate(&mut self, len: u64) {
        let held: std::collections::HashSet<MediaId> = std::mem::take(&mut self.steers)
            .into_iter()
            .flatten()
            .collect();
        self.held_messages.clear();
        let removed: std::collections::HashSet<MediaId> = self
            .captures
            .values()
            .filter(|capture| {
                held.contains(&capture.id) || capture.ordinal.is_some_and(|ordinal| ordinal >= len)
            })
            .map(|capture| capture.id)
            .collect();
        if !removed.is_empty() {
            let kept = |ids: &mut Vec<MediaId>| {
                ids.retain(|id| !removed.contains(id));
                !ids.is_empty()
            };
            self.sources.retain(|_, ids| kept(ids));
            self.positions.retain(|_, ids| kept(ids));
            self.tools.retain(|_, ids| kept(ids));
            self.pending.retain(|id| !removed.contains(id));
            self.protected.retain(|id| !removed.contains(id));
            if let Some(selected) = &mut self.restored_selection {
                selected.retain(|binding| !removed.contains(&binding.id));
            }
            self.captures.retain(|id, _| !removed.contains(id));
            // A reference is read through the oldest capture of it (see `insert`). Rebuild
            // from the captures that remain, so a reference the cut leaves in the transcript
            // stays readable when its oldest capture was cut.
            self.digests.clear();
            for (digest, ids) in &self.sources {
                let entries = self.digests.entry(digest.clone()).or_default();
                for id in ids {
                    let reference = &self.captures[id].reference;
                    if !entries
                        .iter()
                        .any(|entry| self.captures[entry].reference == *reference)
                    {
                        entries.push(*id);
                    }
                }
            }
        }
        drop(self.skeleton.split_off(&len));
        // A converted session's compaction covered a prefix of the old transcript. Messages
        // the cut removed are gone, so a later message at one of their ordinals is new and
        // must not count as covered.
        self.restored_covered = self
            .restored_covered
            .and_then(|covered| len.checked_sub(1).map(|last| covered.min(last)));
        self.batch_order.clear();
        self.last_step = None;
        self.end_step();
    }

    /// Whether the engine still holds the capture `id`; only a transcript cut removes one.
    pub fn holds(&self, id: MediaId) -> bool {
        self.captures.contains_key(&id)
    }

    fn index_replacements(&mut self) {
        use crate::compaction_provider::view::ViewRange;
        if self.restored_selection.is_some() {
            return;
        }
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
        if let Some(selected) = &self.restored_selection {
            return selected.clone();
        }
        let prompt: Vec<_> = self.skeleton.values().cloned().collect();
        let selected = crate::compaction::view::rendered_history(&self.compaction, &prompt);
        self.bindings(&selected)
    }

    /// Every fact this engine holds, for comparing it with another engine fed the same
    /// history (see [`MediaFacts`]).
    pub fn facts(&self) -> MediaFacts {
        fn sorted<K: Ord, V>(entries: impl Iterator<Item = (K, V)>) -> Vec<(K, V)> {
            let mut entries: Vec<_> = entries.collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            entries
        }
        let mut captures: Vec<_> = self
            .captures
            .values()
            .map(|capture| CaptureFacts {
                id: capture.id,
                reference: capture.reference.clone(),
                ordinal: capture.ordinal,
                tool_call: capture.tool_call.clone(),
                first_step: capture.first_step,
                person: capture.person,
            })
            .collect();
        captures.sort_by_key(|capture| capture.id);
        let mut batch_order: Vec<_> = self
            .batch_order
            .iter()
            .map(|(call, position)| (call.clone(), *position))
            .collect();
        batch_order.sort_by(|a, b| (a.1, &a.0 .0).cmp(&(b.1, &b.0 .0)));
        MediaFacts {
            boundary: self.boundary,
            last_step: self.last_step,
            captures,
            pending: self.pending.clone(),
            held: self.steers.clone(),
            protected: self.protected.clone(),
            skeleton: self
                .skeleton
                .iter()
                .map(|(ordinal, message)| (*ordinal, MediaMessage::of(message)))
                .collect(),
            held_messages: sorted(
                self.held_messages
                    .iter()
                    .map(|(key, message)| (*key, MediaMessage::of(message))),
            ),
            batch_order,
            selected: self.selected_bindings(),
            covered: self.covered(),
            replacements: self.replacements.clone(),
            positions: sorted(self.positions.iter().map(|((ordinal, call, digest), ids)| {
                (
                    (
                        *ordinal,
                        call.as_ref().map(|call| call.0.clone()),
                        digest.clone(),
                    ),
                    ids.clone(),
                )
            })),
            digests: sorted(
                self.digests
                    .iter()
                    .map(|(digest, ids)| (digest.clone(), ids.clone())),
            ),
            sources: sorted(
                self.sources
                    .iter()
                    .map(|(digest, ids)| (digest.clone(), ids.clone())),
            ),
            tools: sorted(
                self.tools
                    .iter()
                    .map(|((call, part), ids)| ((call.0.clone(), *part), ids.clone())),
            ),
        }
    }

    fn covered(&self) -> Option<u64> {
        self.restored_covered.or_else(|| {
            self.compaction
                .view
                .as_ref()
                .map(|view| view.view.covers_through.ordinal)
                .or_else(|| {
                    self.compaction
                        .applied
                        .as_ref()
                        .map(|applied| applied.range.to.saturating_sub(1))
                })
        })
    }

    fn show(&mut self, id: MediaId) {
        let capture = &self.captures[&id];
        let covered = self.covered();
        if capture
            .ordinal
            .is_some_and(|ordinal| covered.is_some_and(|covered| ordinal <= covered))
        {
            return;
        }
        let binding = MediaBinding {
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
        };
        if let Some(selected) = &mut self.restored_selection {
            selected.push(binding.clone());
        }
        self.changes.push(MediaChange::Show(binding));
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
        let bytes = if let Some((state_root, class)) = &self.read_only {
            ReadOnlyMediaStore::open_existing(state_root, *class)
                .map_err(|source| {
                    if source.kind() == std::io::ErrorKind::NotFound {
                        MediaReadError::StoreMissing {
                            root: state_root.join(class.directory()),
                            digest: reference.sha256.clone(),
                        }
                    } else {
                        MediaReadError::Io {
                            digest: reference.sha256.clone(),
                            source,
                        }
                    }
                })?
                .read_verified(reference)?
        } else {
            self.store
                .as_ref()
                .expect("render session has a media store")
                .read_verified(reference)?
        };
        if crate::media_validation::validate_media(&bytes, &reference.mime).as_ref()
            != Ok(reference)
        {
            return Err(MediaReadError::Corrupt(reference.sha256.clone()));
        }
        Ok(bytes)
    }
}
