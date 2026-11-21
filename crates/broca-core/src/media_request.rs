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

use crate::media_store::{MediaBytes, MediaReadError};
use crate::placement::{AppendKind, Placement, PlacementAction};
use compact::{MessageCopy, RefId, References};

mod compact;
pub mod held;
mod seed;

impl seed::MediaCheckpoint {}
pub use seed::DurableMediaSeed;
pub use seed::{MediaCheckpoint, MediaHistory};

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

/// A capture as the engine keeps it: a [`Capture`] whose reference is its position in the
/// session's [`References`].
#[derive(Debug, Clone)]
struct Entry {
    id: MediaId,
    reference: RefId,
    ordinal: Option<u64>,
    tool_call: Option<ToolCallId>,
    first_step: Option<u64>,
    person: bool,
}

/// What an engine is fed: nothing yet, the old records (see [`SessionMedia::fold`]) or
/// content-free events (see [`SessionMedia::apply`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Feed {
    Nothing,
    Records,
    /// The compaction state is never set, the engine's [`References`] is the set of references
    /// the session may render, and message copies are kept as shapes.
    Events,
}

/// Where a placed medium sits: its message's ordinal, for a tool's medium the call whose
/// result holds it, and its digest.
type Position = (u64, Option<ToolCallId>, String);

/// Adds `id` to a list of capture ids. A list starts with room for exactly one: most hold one
/// capture for the session's whole life, and a vector's first push otherwise makes room for
/// four.
fn push_id(ids: &mut Vec<MediaId>, id: MediaId) {
    if ids.capacity() == 0 {
        ids.reserve_exact(1);
    }
    ids.push(id);
}

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

/// The engine's whole state as content-free data, for saving it with a session and restoring it
/// later ([`SessionMedia::index_state`], [`SessionMedia::from_index_state`]). The lookups that
/// answer `bindings` are left out: a restore rebuilds them from the captures, in id order, as the
/// old storage's media checkpoint does. So are the old placement state and the record counter,
/// which only `fold` reads, and the compaction state, which a saved selection stands for.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct MediaIndexState {
    /// The step counter.
    pub boundary: u64,
    /// The latest step started in the current run, whose repeated start is a retry.
    pub last_step: Option<StepId>,
    /// The latest tool batch's calls in planned order. A call planned twice holds its last
    /// position; the position its earlier plan held holds a call planned later.
    pub batch: Vec<ToolCallId>,
    /// Every capture, in id order.
    pub captures: Vec<CaptureFacts>,
    /// Captures waiting for their first step.
    pub pending: Vec<MediaId>,
    /// Accepted messages not placed yet, one entry of capture ids per message, in the order
    /// accepted.
    pub held: Vec<Vec<MediaId>>,
    /// Captures protected from aging for the current step.
    pub protected: Vec<MediaId>,
    /// The text-free copies of placed messages, by ordinal.
    pub skeleton: Vec<(u64, MediaMessage)>,
    /// Accepted messages not placed yet that hold media, by (record, position in the record).
    pub held_messages: Vec<((u64, usize), MediaMessage)>,
    /// Media a first-protocol compaction put in its replacement messages, with the ordinals
    /// the replacement covers.
    pub replacements: Vec<(Vec<MediaRef>, u64, u64)>,
    /// The media shown, when a first-protocol compaction decided them: such a selection can't
    /// be computed again without that compaction's records, so it is saved as it stands and
    /// grows with each medium shown after it. `None` when the selection follows from the
    /// skeletons alone.
    pub selection: Option<Vec<MediaBinding>>,
    /// The last ordinal a first-protocol compaction covers.
    pub covered: Option<u64>,
    /// A first-protocol compaction view was in force.
    pub has_view: bool,
}

/// Metadata-only projection paired with one session's media store. Live callers retain
/// the session lease; offline readers verify afresh and refuse objects reclaimed meanwhile.
#[derive(Debug, Clone)]
pub struct SessionMedia {
    store: Option<std::sync::Arc<dyn MediaBytes>>,
    placement: Placement,
    records: u64,
    boundary: u64,
    last_step: Option<StepId>,
    feed: Feed,
    /// Every reference captured, each kept once.
    refs: References,
    /// Every capture, in id order.
    captures: Vec<Entry>,
    /// Each capture beside its reference, in order: a reference's captures in the order
    /// captured. A digest's captures are those of its references, and a reference is read
    /// through the oldest capture of it.
    sources: Vec<(RefId, MediaId)>,
    /// Each placed capture beside its ordinal, in order. The captures of a [`Position`] are
    /// those at its ordinal with its call and digest, in the order they took it.
    positions: Vec<(u64, MediaId)>,
    tools: HashMap<(ToolCallId, Option<usize>), Vec<MediaId>>,
    pending: Vec<MediaId>,
    steers: Vec<Vec<MediaId>>,
    protected: Vec<MediaId>,
    changes: Vec<MediaChange>,
    skeleton: BTreeMap<u64, MessageCopy>,
    held_messages: HashMap<(u64, usize), MessageCopy>,
    compaction: crate::compaction::CompactionState,
    batch_order: HashMap<ToolCallId, usize>,
    replacements: Vec<(Vec<MediaRef>, u64, u64)>,
    restored_selection: Option<Vec<MediaBinding>>,
    restored_covered: Option<u64>,
    restored_view: bool,
}

impl SessionMedia {
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

    /// The engine is fed a record. An engine fed only events until now gives its compaction
    /// state every reference it captured, which is what that state would hold had the
    /// events added them as they came.
    fn feed_records(&mut self) {
        if self.feed == Feed::Events {
            for reference in self.refs.iter() {
                self.compaction.captured.insert(reference);
            }
        }
        self.feed = Feed::Records;
    }

    /// Fold in durable WAL order, including records with no media. The positional placement
    /// kernel is shared with replay so held steers cannot acquire a premature ordinal.
    pub fn fold(&mut self, record: &WalRecord) -> Result<(), String> {
        self.feed_records();
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
                                        *text = Default::default();
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

    pub fn new(store: impl MediaBytes + 'static) -> Self {
        Self::metadata(Some(std::sync::Arc::new(store)))
    }

    fn metadata(store: Option<std::sync::Arc<dyn MediaBytes>>) -> Self {
        Self {
            store,
            placement: Placement::default(),
            records: 0,
            boundary: 0,
            last_step: None,
            feed: Feed::Nothing,
            refs: References::default(),
            captures: Vec::new(),
            sources: Vec::new(),
            positions: Vec::new(),
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
    /// An engine fed only events keeps that set as its table of captured references; one
    /// that was fed records keeps adding them to its compaction state.
    pub fn apply(&mut self, event: &MediaEvent) -> Result<(), String> {
        if self.feed == Feed::Nothing {
            self.feed = Feed::Events;
        }
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
                if self.feed == Feed::Records {
                    for part in media {
                        self.compaction.captured.insert(&part.reference);
                    }
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
                                    text: String::new().into(),
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
    /// only way in; `fold` adds them from the records it reads. An engine fed only events
    /// has them in its table of references once their captures join it.
    fn admit_references(&mut self, message: &Message) {
        if self.feed == Feed::Records {
            for reference in crate::compaction::media_refs(&message.content) {
                self.compaction.captured.insert(reference);
            }
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
            let mut message = match self.skeleton.remove(&ordinal) {
                Some(copy) => copy.message(&self.refs, Some(ordinal)),
                None => {
                    let mut message = Message::new(broca_protocol::Role::Tool, Vec::new());
                    message.ordinal = Some(ordinal);
                    message
                }
            };
            if message.content.len() <= block_index {
                message
                    .content
                    .resize_with(block_index + 1, || ContentBlock::Text {
                        text: String::new().into(),
                    });
            }
            message.content[block_index] = ContentBlock::ToolResult(result);
            let copy = self.keep_copy(message, Some(ordinal));
            self.skeleton.insert(ordinal, copy);
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
                self.capture_mut(&id).ordinal = Some(first + i as u64);
                self.index_position(id);
                self.show(id);
                if let Some(mut copy) = self.held_messages.remove(&(id.record, id.message)) {
                    copy.place(first + i as u64);
                    self.skeleton.insert(first + i as u64, copy);
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
                self.capture_mut(&id).first_step = Some(self.boundary);
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
            .iter()
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
            self.sources.retain(|(_, id)| !removed.contains(id));
            self.positions.retain(|(_, id)| !removed.contains(id));
            self.tools.retain(|_, ids| kept(ids));
            self.pending.retain(|id| !removed.contains(id));
            self.protected.retain(|id| !removed.contains(id));
            if let Some(selected) = &mut self.restored_selection {
                selected.retain(|binding| !removed.contains(&binding.id));
            }
            // A reference the cut leaves in the transcript stays readable through the oldest
            // capture of it that remains (see `sources`).
            self.captures
                .retain(|capture| !removed.contains(&capture.id));
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
        self.find_capture(&id).is_some()
    }

    /// The capture `id`, if the engine holds it.
    fn find_capture(&self, id: &MediaId) -> Option<&Entry> {
        let at = self
            .captures
            .binary_search_by_key(id, |capture| capture.id)
            .ok()?;
        Some(&self.captures[at])
    }

    /// The capture `id`, which the engine holds: every id it hands out or keeps names one.
    fn capture(&self, id: &MediaId) -> &Entry {
        self.find_capture(id).expect("capture id")
    }

    fn capture_mut(&mut self, id: &MediaId) -> &mut Entry {
        let at = self
            .captures
            .binary_search_by_key(id, |capture| capture.id)
            .expect("capture id");
        &mut self.captures[at]
    }

    /// The copy to keep of a message at `ordinal` (none for one not placed yet): its shape
    /// on an engine fed only events, where a copy holds nothing else.
    fn keep_copy(&mut self, message: Message, ordinal: Option<u64>) -> MessageCopy {
        MessageCopy::keep(message, ordinal, &mut self.refs, self.feed == Feed::Events)
    }

    /// The engine's message copies as messages, in order.
    fn copies(&self) -> Vec<Message> {
        self.skeleton
            .iter()
            .map(|(ordinal, copy)| copy.message(&self.refs, Some(*ordinal)))
            .collect()
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
        let prompt = self.copies();
        let selected = if self.feed == Feed::Events {
            crate::compaction::view::uncompacted_history(&prompt, |reference| {
                self.refs.find(reference).is_some()
            })
        } else {
            crate::compaction::view::rendered_history(&self.compaction, &prompt)
        };
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

        let captures: Vec<_> = self
            .captures
            .iter()
            .map(|capture| CaptureFacts {
                id: capture.id,
                reference: self.refs.get(capture.reference).clone(),
                ordinal: capture.ordinal,
                tool_call: capture.tool_call.clone(),
                first_step: capture.first_step,
                person: capture.person,
            })
            .collect();
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
                .map(|(ordinal, copy)| {
                    (
                        *ordinal,
                        MediaMessage::of(&copy.message(&self.refs, Some(*ordinal))),
                    )
                })
                .collect(),
            held_messages: sorted(
                self.held_messages
                    .iter()
                    .map(|(key, copy)| (*key, MediaMessage::of(&copy.message(&self.refs, None)))),
            ),
            batch_order,
            selected: self.selected_bindings(),
            covered: self.covered(),
            replacements: self.replacements.clone(),
            positions: self.positions_by_key().into_iter().collect(),
            digests: self.digests().into_iter().collect(),
            sources: self.sources_by_digest().into_iter().collect(),
            tools: sorted(
                self.tools
                    .iter()
                    .map(|((call, part), ids)| ((call.0.clone(), *part), ids.clone())),
            ),
        }
    }

    /// The engine's state as data, to save with the session (see [`MediaIndexState`]).
    pub fn index_state(&self) -> MediaIndexState {
        let facts = self.facts();
        // A call planned twice holds only its last position, so a position before it can be
        // empty: it takes a call whose own position comes later, which that position then
        // overwrites when the batch is ordered again.
        let mut batch: Vec<Option<ToolCallId>> = Vec::new();
        for (call, position) in &facts.batch_order {
            if batch.len() <= *position {
                batch.resize(position + 1, None);
            }
            batch[*position] = Some(call.clone());
        }
        let mut later = None;
        for slot in batch.iter_mut().rev() {
            match slot {
                Some(call) => later = Some(call.clone()),
                None => slot.clone_from(&later),
            }
        }
        let decided = self.restored_selection.is_some()
            || self.compaction.view.is_some()
            || self.compaction.applied.is_some();
        MediaIndexState {
            boundary: facts.boundary,
            last_step: facts.last_step,
            batch: batch.into_iter().flatten().collect(),
            captures: facts.captures,
            pending: facts.pending,
            held: facts.held,
            protected: facts.protected,
            skeleton: facts.skeleton,
            held_messages: facts.held_messages,
            replacements: facts.replacements,
            selection: decided.then_some(facts.selected),
            covered: facts.covered,
            has_view: self.compaction.view.is_some() || self.restored_view,
        }
    }

    /// An engine holding a saved state (see [`MediaIndexState`]), reading media bytes from
    /// `store`. It continues from that state through either entry point; a saved selection is
    /// used as it stands until a first-protocol compaction record arrives through `fold`.
    /// Refused when a skeleton names a block past its message's last, or an id names no capture.
    pub fn from_index_state(
        state: MediaIndexState,
        store: impl MediaBytes + 'static,
    ) -> Result<Self, String> {
        let mut session = Self::new(store);
        session.boundary = state.boundary;
        session.last_step = state.last_step;
        session.order_batch(state.batch.into_iter());
        session.feed = Feed::Events;
        for capture in state.captures {
            if session.holds(capture.id) {
                return Err(format!("the capture {:?} repeats", capture.id));
            }
            session.insert(
                Capture {
                    id: capture.id,
                    reference: capture.reference,
                    ordinal: capture.ordinal,
                    tool_call: capture.tool_call,
                    first_step: capture.first_step,
                    person: capture.person,
                },
                false,
            );
        }
        let named = state
            .pending
            .iter()
            .chain(&state.protected)
            .chain(state.held.iter().flatten())
            .chain(state.selection.iter().flatten().map(|binding| &binding.id));
        for id in named {
            if !session.holds(*id) {
                return Err(format!("{id:?} names no capture"));
            }
        }
        session.pending = state.pending;
        session.steers = state.held;
        session.protected = state.protected;
        for (ordinal, skeleton) in state.skeleton {
            let mut message = skeleton.to_message()?;
            message.ordinal = Some(ordinal);
            let copy = session.keep_copy(message, Some(ordinal));
            session.skeleton.insert(ordinal, copy);
        }
        for (key, skeleton) in state.held_messages {
            let copy = session.keep_copy(skeleton.to_message()?, None);
            session.held_messages.insert(key, copy);
        }
        session.replacements = state.replacements;
        session.restored_selection = state.selection;
        session.restored_covered = state.covered;
        session.restored_view = state.has_view;
        Ok(session)
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
        let capture = self.capture(&id);
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
            reference: self.refs.get(capture.reference).clone(),
            tool: capture.tool_call.is_some(),
            protected: capture.person
                && capture.first_step.is_none_or(|step| step == self.boundary),
        };
        if let Some(selected) = &mut self.restored_selection {
            selected.push(binding.clone());
        }
        self.changes.push(MediaChange::Show(binding));
    }

    /// Keep `capture`, and show it when `announce` asks and it is shown at once: when it is
    /// placed, or is a tool's.
    fn insert(&mut self, capture: Capture, announce: bool) {
        let id = capture.id;
        let reference = self.refs.intern(&capture.reference);
        let at = self
            .sources
            .partition_point(|source| *source < (reference, id));
        self.sources.insert(at, (reference, id));
        if let Some(call) = &capture.tool_call {
            push_id(self.tools.entry((call.clone(), id.part)).or_default(), id);
        }
        let shown = capture.ordinal.is_some() || !capture.person;
        let entry = Entry {
            id,
            reference,
            ordinal: capture.ordinal,
            tool_call: capture.tool_call,
            first_step: capture.first_step,
            person: capture.person,
        };
        match self
            .captures
            .binary_search_by_key(&id, |capture| capture.id)
        {
            Ok(at) => self.captures[at] = entry,
            Err(at) => self.captures.insert(at, entry),
        }
        self.index_position(id);
        if announce && shown {
            self.show(id);
        }
    }

    fn index_position(&mut self, id: MediaId) {
        if let Some(ordinal) = self.capture(&id).ordinal {
            let at = self
                .positions
                .partition_point(|position| *position < (ordinal, id));
            self.positions.insert(at, (ordinal, id));
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
                                    *text = Default::default();
                                }
                            }
                        }
                    }
                    _ => {
                        *block = ContentBlock::Text {
                            text: String::new().into(),
                        }
                    }
                }
            }
            let copy = self.keep_copy(metadata, ordinal);
            if let Some(ordinal) = ordinal {
                self.skeleton.insert(ordinal, copy);
            } else {
                self.held_messages.insert((self.records, index), copy);
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
                    // Without a place or a call, the reference binds to its oldest capture:
                    // the first of its digest's captures that is of this reference.
                    let ids: Option<Vec<MediaId>> = if let Some(position) = &position {
                        self.position(position)
                    } else if carried.is_some() {
                        self.of_digest(&reference.sha256)
                    } else if let Some(call) = tool_call {
                        self.tools.get(&(call.clone(), part)).cloned()
                    } else {
                        self.of_digest(&reference.sha256)
                    };
                    let cursor = position.map(|position| cursors.entry(position).or_default());
                    let capture = ids.and_then(|ids| {
                        if let Some((from, to)) = carried {
                            return ids
                                .iter()
                                .map(|id| self.capture(id))
                                .filter(|capture| {
                                    self.refs.get(capture.reference) == reference
                                        && capture
                                            .ordinal
                                            .is_some_and(|ordinal| from <= ordinal && ordinal < to)
                                })
                                .min_by_key(|capture| (!self.is_protected(capture), capture.id));
                        }
                        let start = cursor.as_deref().copied().unwrap_or(0);
                        let found = ids[start.min(ids.len())..]
                            .iter()
                            .map(|id| self.capture(id))
                            .find(|capture| self.refs.get(capture.reference) == reference);
                        if let Some(cursor) = cursor {
                            *cursor += 1;
                        }
                        found.or_else(|| {
                            ids.iter()
                                .map(|id| self.capture(id))
                                .find(|capture| self.refs.get(capture.reference) == reference)
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

    /// The captures at `position`, in the order they took it; `None` when there are none.
    fn position(&self, (ordinal, call, digest): &Position) -> Option<Vec<MediaId>> {
        let start = self.positions.partition_point(|(at, _)| at < ordinal);
        let ids: Vec<MediaId> = self.positions[start..]
            .iter()
            .take_while(|(at, _)| at == ordinal)
            .map(|(_, id)| *id)
            .filter(|id| {
                let capture = self.capture(id);
                capture.tool_call == *call && self.refs.get(capture.reference).sha256 == *digest
            })
            .collect();
        (!ids.is_empty()).then_some(ids)
    }

    /// The captures of `reference`, in the order captured.
    fn of_reference(&self, reference: RefId) -> &[(RefId, MediaId)] {
        let start = self.sources.partition_point(|(at, _)| *at < reference);
        let len = self.sources[start..].partition_point(|(at, _)| *at == reference);
        &self.sources[start..start + len]
    }

    /// The captures of `digest`, in the order captured; `None` when there are none.
    fn of_digest(&self, digest: &str) -> Option<Vec<MediaId>> {
        let mut ids: Vec<MediaId> = self
            .refs
            .of_digest(digest)
            .iter()
            .flat_map(|reference| self.of_reference(*reference))
            .map(|(_, id)| *id)
            .collect();
        ids.sort_unstable();
        (!ids.is_empty()).then_some(ids)
    }

    /// The captures of each position, by position.
    fn positions_by_key(&self) -> BTreeMap<(u64, Option<String>, String), Vec<MediaId>> {
        let mut positions: BTreeMap<_, Vec<MediaId>> = BTreeMap::new();
        for (ordinal, id) in &self.positions {
            let capture = self.capture(id);
            let key = (
                *ordinal,
                capture.tool_call.as_ref().map(|call| call.0.clone()),
                self.refs.get(capture.reference).sha256.clone(),
            );
            positions.entry(key).or_default().push(*id);
        }
        positions
    }

    /// The captures of each digest, in the order captured.
    fn sources_by_digest(&self) -> BTreeMap<String, Vec<MediaId>> {
        let mut sources: BTreeMap<String, Vec<MediaId>> = BTreeMap::new();
        for (reference, id) in &self.sources {
            sources
                .entry(self.refs.get(*reference).sha256.clone())
                .or_default()
                .push(*id);
        }
        for ids in sources.values_mut() {
            ids.sort_unstable();
        }
        sources
    }

    /// For each digest, the oldest capture of each of its references, in the order captured:
    /// the captures its references are read through.
    fn digests(&self) -> BTreeMap<String, Vec<MediaId>> {
        let mut digests: BTreeMap<String, Vec<MediaId>> = BTreeMap::new();
        for (reference, id) in &self.sources {
            let digest = &self.refs.get(*reference).sha256;
            let ids = digests.entry(digest.clone()).or_default();
            // A reference's captures are in order, so its oldest comes first.
            if !ids
                .iter()
                .any(|seen| self.capture(seen).reference == *reference)
            {
                ids.push(*id);
            }
        }
        for ids in digests.values_mut() {
            ids.sort_unstable();
        }
        digests
    }

    fn is_protected(&self, capture: &Entry) -> bool {
        capture.person && capture.first_step.is_none_or(|step| step == self.boundary)
    }

    /// Read and verify afresh. Exact metadata equality is required as well as a digest match.
    pub fn read(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        if !self
            .refs
            .find(reference)
            .is_some_and(|reference| !self.of_reference(reference).is_empty())
        {
            return Err(MediaReadError::Corrupt(reference.sha256.clone()));
        }
        let bytes = self
            .store
            .as_ref()
            .expect("render session has a media store")
            .read_verified(reference)?;
        if crate::media_validation::validate_media(&bytes, &reference.mime).as_ref()
            != Ok(reference)
        {
            return Err(MediaReadError::Corrupt(reference.sha256.clone()));
        }
        Ok(bytes)
    }
}
