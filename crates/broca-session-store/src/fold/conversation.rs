//! The conversation fold: the messages the model sees, rebuilt from transcript records alone.
//!
//! Transcript records are written in the order their messages take in the conversation, so the
//! fold appends them as they come, with one exception: an assistant message that calls tools is
//! held until every one of its calls has a reply. A run's budget-finalize step is the other
//! exception: its calls are never run, so its message enters at once without them. It then enters the conversation together with
//! one tool message carrying the replies in the order the assistant made the calls, which is
//! the shape every provider requires: a call is never sent without its result.
//!
//! Compaction and setup records don't change the conversation. A compaction view is applied when
//! a request is rendered, never to the conversation itself.
//!
//! Beside each window message the fold keeps what `session.entries` serves about it: its
//! `EntryFacts` record, and the text delivered with its tool calls (from `ResultAppend`: every
//! ride, which carries an envelope, and in sessions that keep delivered text apart from results,
//! text delivered before envelopes existed too). An assistant message still waiting for
//! its tool results already has an ordinal reserved, one past the last placed message, and its
//! facts are kept with it until it is placed.

use std::sync::Arc;

use broca_protocol::content::{ContentBlock, Message, Role, ToolResult};
use broca_protocol::ids::ToolCallId;

use crate::codec::Digest;
use crate::fold::Bound;
use crate::record::{
    EntryFacts, Marker, Notice, PartKind, PartRef, Placement, Record, RideEnvelope, Stream,
    TurnFacts,
};

/// Why a record can't be folded into the conversation. Each one means the records don't
/// describe a conversation a provider would accept, so the fold refuses rather than guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldError {
    /// A ledger record was handed to the conversation fold.
    LedgerRecord(&'static str),
    /// A reply for a call no held assistant message made.
    ReplyWithoutCall(String),
    /// A reply for a call whose every copy already has one.
    SecondReply(String),
    /// A message other than a run's own input arrived while an assistant message was still
    /// waiting for replies, where it would land between a call and its result.
    InputInsideToolTurn,
    /// An assistant message arrived while another was still waiting for replies.
    AssistantInsideToolTurn,
    /// Entry facts for an ordinal the conversation hasn't placed or reserved.
    FactsWithoutEntry(u64),
    /// A seal would keep more runs past the window than their declared cap.
    Bound(&'static str),
    /// Delivered text would join a result whose content isn't held in memory: it must be read
    /// back before the fold changes it.
    ResultNotHeld(String),
}

crate::message! {
    /// Text delivered with one of a message's tool calls.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Delivered {
        /// The tool call's position among the message's content blocks.
        1 req part: u32,
        2 opt notice: Notice,
        /// As delivered, before any rendering.
        3 req text: String,
        /// The id of the send the text is, when its record names one.
        4 opt send_id: String,
        /// The ride envelope, when the text was delivered with one: requests render it after
        /// the result through the runner's ride rules, and never join it into the result.
        5 opt ride: RideEnvelope,
    }
}

crate::message! {
    /// What `session.entries` serves about one window message besides the message itself.
    /// Its facts are shared with the records that carry them, and with every read that serves
    /// them.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct EntryState {
        /// Absent for sessions written before entry facts were recorded.
        1 opt facts: Arc<EntryFacts>,
        2 rep delivered: Delivered,
        /// For an assistant message that made tool calls: what reads serve about each call.
        3 opt turn: Arc<TurnFacts>,
    }
}

crate::message! {
    /// Where one run's input sits in the window, and the render config it ran under. A
    /// request carries every window run's place and config, so a renderer can tell each run's
    /// requests apart from the next one's.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Episode {
        1 req run_id: String,
        /// The ordinal of the run's first input message.
        2 req start: u64,
        /// The ordinal of its first user message, if its input has one.
        3 opt first_user: u64,
        /// The render config part in force when the run started.
        4 opt render_config: PartRef,
        /// What renderers read from that config when rendering other runs' messages.
        5 opt render_key: Digest,
        /// The first model answer's ordinal, derived from an Assistant record rather than input.
        /// Optional so older checkpoints remain readable and older readers may ignore it.
        6 opt first_model_answer: u64,
    }
}

crate::message! {
    /// An assistant message that called tools, held until each call has its reply.
    #[derive(Debug, Clone)]
    struct ToolTurn {
        1 req assistant: Arc<Message>,
        /// The calls in the order the assistant made them.
        2 rep calls: ToolCallId,
        /// The replies in the order they arrived. A turn holds one step's calls, so finding
        /// one by its call id is a short scan.
        3 rep replies: ToolResult,
    }
}

state! {
    /// The conversation as the model sees it.
    #[derive(Debug, Clone)]
    pub struct Conversation {
        /// The messages in conversation order. An assistant message whose calls aren't all
        /// answered isn't here yet. In a stored session this starts at the open log's
        /// checkpoint, so it holds the active window, never the whole history. Each message is
        /// shared with the record that placed it, where one did, and with every read and request
        /// that takes it.
        1 rep pub messages: WindowMessage => {
            owner: "run loop",
            reset: "never",
            bound: Bound::Window,
        },
        2 opt open: ToolTurn => {
            owner: "run loop",
            reset: "tool replies",
            bound: Bound::OpenWork("one assistant message's tool calls"),
        },
        /// Whether text delivered with a tool result without a ride envelope becomes part of
        /// that result in the conversation. Sessions on the first compaction protocol stored it
        /// that way; sessions on the current contract keep the result as returned and add the
        /// text when a request renders. A ride is always kept apart.
        3 req pub appends_in_conversation: bool => {
            owner: "admission",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// The latest compaction marker: where the provider's cut falls. Requests render from
        /// its first kept entry.
        4 opt pub marker: Marker => {
            owner: "compaction",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// How many messages the conversation has ever placed, the window's included. A message's
        /// ordinal is its place in the whole conversation, counting from 1, so the window's first
        /// message has ordinal `placed - messages.len() + 1`, and older ones are in sealed
        /// segments.
        5 req pub placed: u64 => {
            owner: "run loop",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// One per window message, in the same order.
        6 rep pub entries: EntryState => {
            owner: "run loop",
            reset: "never",
            bound: Bound::Window,
        },
        /// The held assistant message's entry, at its reserved ordinal.
        7 opt pub open_entry: EntryState => {
            owner: "run loop",
            reset: "tool replies",
            bound: Bound::OpenWork("one assistant message's tool calls"),
        },
        /// A revert into sealed history left no window: the messages before the new run are in
        /// sealed segments only. Reads serve them from there, and nothing may render until a
        /// window is copied back in, after the compaction provider answers an `over_limit`
        /// notice (not built yet).
        8 req pub window_pending: bool => {
            owner: "revert",
            reset: "a window copied back in",
            bound: Bound::Fixed,
        },
        /// The render config part the latest setup marker named.
        9 opt pub render_config: PartRef => {
            owner: "admission",
            reset: "setup_stub",
            bound: Bound::Fixed,
        },
        /// One per run whose input is in the window, in run order. After a cut, the first is
        /// the run whose input the cut split, when its later messages are in the window.
        10 rep pub episodes: Episode => {
            owner: "run loop",
            reset: "never",
            bound: Bound::Window,
        },
        /// The render key the latest setup marker named.
        11 opt pub render_key: Digest => {
            owner: "admission",
            reset: "setup_stub",
            bound: Bound::Fixed,
        },
        /// The runs before the window whose settings still shape a request: the session's first
        /// run, and every later one whose render key differs from the run before it. A request
        /// carries them ahead of the window's runs, as it carried every run before history was
        /// sealed. The key changes rarely (at most 6 times in any stored session), so a seal
        /// that would keep more than the cap refuses.
        12 rep pub sealed_episodes: Episode => {
            owner: "seal",
            reset: "never",
            bound: Bound::Capped(SEALED_EPISODES_CAP),
        },
        /// When the latest cut split a run whose steps continue above it, that run's
        /// repetition-warning counters over its sealed steps (call signatures, result digests and
        /// counts, never call text), so a request's warnings still count them. The run loop
        /// computes and reads them; the store only keeps them.
        13 opt pub window_repetition: serde_json::Value => {
            owner: "seal",
            reset: "a cut",
            bound: Bound::PerRun("one count per distinct failing call"),
        },
        /// The transcript holds a message of the first compaction protocol, which only today's
        /// run loop renders, so the new run loop refuses the session.
        14 req pub first_protocol_compaction: bool => {
            owner: "compaction",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// How many human prompts the session's runs started with: user messages in a run's
        /// input with anything besides tool results. Steers and delivered sends join the
        /// running prompt, so they don't count. A Claude Code persona numbers its prompts by
        /// this, so it counts the whole history, sealed runs too.
        15 req pub human_prompts: u64 => {
            owner: "run loop",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// On a session that joins delivered text into its tool results
        /// (`appends_in_conversation`) and then enrolled in the current compaction contract: the
        /// last ordinal placed before the enrollment. Results up to it keep the text joined into
        /// them, as they were stored; text delivered with a later result stays apart from it, as
        /// on the contract. Absent on every other session.
        16 opt pub joined_through: u64 => {
            owner: "admission",
            reset: "never",
            bound: Bound::Fixed,
        },
    }
    runtime {
        /// Which placed window messages changed after they were placed, for a reader that keeps
        /// what it derived from the window and brings it up to date at each read (see
        /// [`WindowChanges`]).
        pub changes: WindowChanges => {
            owner: "run loop",
            reset: "a copy, or a read from storage",
            bound: Bound::Capped(RESTATED_CAP),
        },
        /// The seqs of the records the held assistant message and its replies so far came from,
        /// which both messages of its turn take as their sources once it is placed (see
        /// `WindowMessage::sources`). Empty when the turn was read from a checkpoint.
        open_sources: OpenSources => {
            owner: "run loop",
            reset: "a read from storage",
            bound: Bound::OpenWork("one assistant message's tool calls"),
        },
    }
}

/// The seqs the held tool turn's records so far were folded at (see `Conversation::open_sources`):
/// `None` when one of them isn't known.
#[derive(Debug, Clone, Default)]
pub struct OpenSources(Option<Vec<u64>>);

impl crate::held::HeldBytes for OpenSources {
    fn heap_bytes(&self) -> usize {
        self.0
            .as_ref()
            .map_or(0, |seqs| seqs.capacity() * size_of::<u64>())
    }
}

/// The most restatements [`WindowChanges`] keeps of each kind; past it, the oldest two become
/// one.
pub const RESTATED_CAP: usize = 64;

/// Which placed window messages changed after they were placed: a later record restated a
/// message's entry state (its facts, its turn facts, or the texts delivered with its calls), or
/// joined delivered text into the message itself. A reader that keeps what it derived from the
/// window takes a [`ChangeMark`] at each read, and at its next read asks for the lowest ordinal
/// changed since, rather than comparing every kept message with the window's.
///
/// Kept only in memory, for the fold it belongs to: a fold read from storage or copied starts a
/// new one, which answers a mark taken on another fold with "everything".
pub struct WindowChanges {
    /// Tells this fold's changes apart from any other fold's.
    origin: u64,
    /// How many changes it recorded.
    count: u64,
    states: Restated,
    messages: Restated,
}

/// Where a reader of the window last looked (see [`WindowChanges`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChangeMark {
    origin: u64,
    count: u64,
}

/// Restatements of one kind, each as (the change count after it, the ordinal it changed), in
/// change order. Only those that can still be the lowest ordinal changed since some mark are
/// kept, so both the counts and the ordinals ascend.
#[derive(Debug, Clone, Default)]
struct Restated(Vec<(u64, u64)>);

impl crate::held::HeldBytes for WindowChanges {
    fn heap_bytes(&self) -> usize {
        crate::held::HeldBytes::heap_bytes(&self.states.0)
            + crate::held::HeldBytes::heap_bytes(&self.messages.0)
    }
}

impl Conversation {
    /// The heap the assistant message held for its tool results holds, with the replies it has
    /// so far, for the memory report (see `crate::held`).
    pub fn open_heap_bytes(&self) -> usize {
        crate::held::HeldBytes::heap_bytes(&self.open)
    }

    /// The addresses of what the fold holds behind its `Arc`s: every window message, the held
    /// assistant message, and every entry's facts and turn facts. The memory report counts each
    /// once, here, and leaves it out of another holder that shares it.
    pub fn shared_addresses(&self) -> std::collections::HashSet<usize> {
        let entries = self.entries.iter().chain(&self.open_entry);
        self.messages
            .iter()
            .map(WindowMessage::allocation)
            .chain(
                self.open
                    .as_ref()
                    .map(|turn| Arc::as_ptr(&turn.assistant) as usize),
            )
            .chain(
                entries
                    .clone()
                    .filter_map(|entry| entry.facts.as_ref())
                    .map(|facts| Arc::as_ptr(facts) as usize),
            )
            .chain(
                entries
                    .filter_map(|entry| entry.turn.as_ref())
                    .map(|turn| Arc::as_ptr(turn) as usize),
            )
            .collect()
    }
}

impl Restated {
    fn push(&mut self, count: u64, ordinal: u64) {
        // An earlier change at the same or a later ordinal is never the lowest since any mark
        // this one is after.
        while self.0.last().is_some_and(|(_, last)| *last >= ordinal) {
            self.0.pop();
        }
        self.0.push((count, ordinal));
        if self.0.len() > RESTATED_CAP {
            // The oldest two become one at the lower ordinal, which a mark between them reads
            // as an earlier change than it was: more is derived again, never less.
            let (_, oldest) = self.0.remove(0);
            self.0[0].1 = oldest;
        }
    }

    /// The lowest ordinal changed after the change count `count`.
    fn since(&self, count: u64) -> Option<u64> {
        let at = self.0.partition_point(|(changed, _)| *changed <= count);
        self.0.get(at).map(|(_, ordinal)| *ordinal)
    }
}

impl Default for WindowChanges {
    fn default() -> Self {
        static ORIGINS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self {
            origin: ORIGINS.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            count: 0,
            states: Restated::default(),
            messages: Restated::default(),
        }
    }
}

/// A copy is another fold's: it answers the original's marks with "everything".
impl Clone for WindowChanges {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl std::fmt::Debug for WindowChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Nothing a fold's content decides: two folds of the same records print alike.
        f.write_str("WindowChanges")
    }
}

impl WindowChanges {
    /// Where a reader looking now stands.
    pub fn mark(&self) -> ChangeMark {
        ChangeMark {
            origin: self.origin,
            count: self.count,
        }
    }

    /// The lowest ordinal whose entry state a record restated since `mark`, if any. A mark
    /// taken on another fold reads as everything changed: ordinal 0.
    pub fn states_since(&self, mark: ChangeMark) -> Option<u64> {
        self.since(&self.states, mark)
    }

    /// The lowest ordinal whose message itself changed since `mark` (delivered text joined into
    /// a tool message), if any. A mark taken on another fold reads as everything changed:
    /// ordinal 0.
    pub fn messages_since(&self, mark: ChangeMark) -> Option<u64> {
        self.since(&self.messages, mark)
    }

    fn since(&self, restated: &Restated, mark: ChangeMark) -> Option<u64> {
        if mark.origin != self.origin || mark.count > self.count {
            return Some(0);
        }
        restated.since(mark.count)
    }

    fn state(&mut self, ordinal: u64) {
        self.count += 1;
        self.states.push(self.count, ordinal);
    }

    fn message(&mut self, ordinal: u64) {
        self.count += 1;
        self.messages.push(self.count, ordinal);
    }

    /// The window was emptied or replaced: every reader derives it again.
    fn everything(&mut self) {
        self.state(0);
        self.message(0);
    }
}

/// The most change points [`Conversation::sealed_episodes`] keeps besides the session's first run.
pub const SEALED_EPISODES_CAP: usize = 64;

impl Conversation {
    pub fn new(appends_in_conversation: bool) -> Self {
        Self {
            messages: Vec::new(),
            open: None,
            appends_in_conversation,
            marker: None,
            placed: 0,
            entries: Vec::new(),
            open_entry: None,
            window_pending: false,
            render_config: None,
            episodes: Vec::new(),
            render_key: None,
            sealed_episodes: Vec::new(),
            window_repetition: None,
            first_protocol_compaction: false,
            human_prompts: 0,
            joined_through: None,
            changes: WindowChanges::default(),
            open_sources: OpenSources::default(),
        }
    }

    /// The session enrolled in the current compaction contract after placing message
    /// `through_ordinal`: a session that joined delivered text into its results keeps the text
    /// joined into results up to it, and keeps text delivered with a later result apart from it,
    /// as the contract does. Which results join depends only on their ordinals, so a fold that
    /// learns of the enrollment before it folds the records placing them (a session's window,
    /// folded again from a checkpoint older than the enrollment) folds them the same.
    pub fn enrolled(&mut self, through_ordinal: Option<u64>) {
        if let (true, Some(through)) = (self.appends_in_conversation, through_ordinal) {
            self.joined_through = Some(through);
        }
    }

    /// Whether text delivered with a result of the newest tool message is joined into it: on a
    /// session that joins delivered text, unless that message was placed after the session
    /// enrolled in the current compaction contract.
    fn joins_delivered_text(&self) -> bool {
        if !self.appends_in_conversation {
            return false;
        }
        let Some(through) = self.joined_through else {
            return true;
        };
        self.messages
            .iter()
            .rposition(|message| message.role() == Role::Tool)
            .is_some_and(|at| self.window_start() + at as u64 <= through)
    }

    /// A copy without the window: the count of placed messages, the held tool turn and its
    /// entry, and the marker. Applying records to it tells which ordinals they place, for the
    /// cost of one tool turn rather than the window.
    pub fn without_window(&self) -> Self {
        Self {
            messages: Vec::new(),
            open: self.open.clone(),
            appends_in_conversation: self.appends_in_conversation,
            marker: self.marker.clone(),
            placed: self.placed,
            entries: Vec::new(),
            open_entry: self.open_entry.clone(),
            window_pending: self.window_pending,
            render_config: self.render_config.clone(),
            episodes: Vec::new(),
            render_key: self.render_key,
            sealed_episodes: self.sealed_episodes.clone(),
            window_repetition: self.window_repetition.clone(),
            first_protocol_compaction: self.first_protocol_compaction,
            human_prompts: self.human_prompts,
            joined_through: self.joined_through,
            changes: WindowChanges::default(),
            open_sources: self.open_sources.clone(),
        }
    }

    /// Empties the window, keeping the count of placed messages and any held tool turn. A
    /// checkpoint carries no window: it's rebuilt from the log's own records.
    pub fn clear_window(&mut self) {
        self.messages.clear();
        self.entries.clear();
        self.episodes.clear();
        self.changes.everything();
    }

    /// Empties the window for a log that starts at a cut, keeping what of the window's runs still
    /// shapes a request: the run whose input the cut split stays a window run (its later messages
    /// are above the cut), and the runs wholly below the cut join the sealed ones where their
    /// render key changes. `first_kept` is the ordinal of the first entry above the cut, and
    /// `starts_run` the run whose input that entry is, if it's a run's input: a run other than
    /// the last one before the cut starts exactly at the cut, and no earlier run spans it.
    pub fn seal_window(
        &mut self,
        first_kept: u64,
        starts_run: Option<&str>,
    ) -> Result<(), FoldError> {
        self.messages.clear();
        self.entries.clear();
        self.window_repetition = None;
        self.changes.everything();
        let episodes = std::mem::take(&mut self.episodes);
        // The run in force at the cut: the last one starting before it, unless the next run
        // starts at the cut.
        let spanning = episodes
            .iter()
            .rposition(|episode| episode.start < first_kept)
            .filter(|at| {
                let next_at_cut = match episodes.get(at + 1) {
                    Some(next) => next.start <= first_kept,
                    None => starts_run.is_some_and(|run| run != episodes[*at].run_id),
                };
                !next_at_cut
            });
        for (at, episode) in episodes.into_iter().enumerate() {
            if episode.start >= first_kept || Some(at) == spanning {
                self.episodes.push(episode);
                continue;
            }
            let changes = match self.sealed_episodes.last() {
                None => true,
                Some(last) => last.render_key != episode.render_key || last.render_key.is_none(),
            };
            if changes {
                // The session's first run, then at most the cap of change points.
                if self.sealed_episodes.len() > SEALED_EPISODES_CAP {
                    return Err(FoldError::Bound("sealed_episodes"));
                }
                self.sealed_episodes.push(episode);
            }
        }
        Ok(())
    }

    /// The window message with `ordinal` and its entry state.
    pub fn window_entry(&self, ordinal: u64) -> Option<(&WindowMessage, &EntryState)> {
        let start = self.window_start();
        if ordinal < start {
            return None;
        }
        let at = (ordinal - start) as usize;
        self.messages.get(at).zip(self.entries.get(at))
    }

    /// The held assistant message, its reserved ordinal and its entry state.
    pub fn open_entry(&self) -> Option<(u64, &Arc<Message>, &EntryState)> {
        let turn = self.open.as_ref()?;
        let entry = self.open_entry.as_ref()?;
        Some((self.placed + 1, &turn.assistant, entry))
    }

    /// The ordinal of the window's first message, or the next ordinal when the window is empty.
    pub fn window_start(&self) -> u64 {
        self.placed - self.messages.len() as u64 + 1
    }

    /// Every window message with all its content, or the first whose content isn't held.
    pub fn held_messages(&self) -> Result<Vec<Arc<Message>>, NotHeld> {
        let start = self.window_start();
        (start..=self.placed)
            .zip(&self.messages)
            .map(|(ordinal, message)| message.held().cloned().ok_or(NotHeld { ordinal }))
            .collect()
    }

    /// The window's messages of `role` with all their content, or the first whose content isn't
    /// held.
    pub fn held_messages_of(&self, role: Role) -> Result<Vec<Arc<Message>>, NotHeld> {
        (self.window_start()..=self.placed)
            .zip(&self.messages)
            .filter(|(_, message)| message.role() == role)
            .map(|(ordinal, message)| message.held().cloned().ok_or(NotHeld { ordinal }))
            .collect()
    }

    /// The window's newest message, with all its content: the newest message is never released
    /// from memory, so this is `None` only for an empty window.
    pub fn newest(&self) -> Option<&Arc<Message>> {
        self.messages.last().and_then(WindowMessage::held)
    }

    /// The window message with `ordinal` with all its content: `Ok(None)` when it isn't in the
    /// window, `Err` when its content isn't held.
    pub fn held_message(&self, ordinal: u64) -> Result<Option<&Arc<Message>>, NotHeld> {
        match self.window_message(ordinal) {
            None => Ok(None),
            Some(message) => message.held().map(Some).ok_or(NotHeld { ordinal }),
        }
    }

    /// The window message with `ordinal`, if it's in the window.
    pub fn window_message(&self, ordinal: u64) -> Option<&WindowMessage> {
        let start = self.window_start();
        (ordinal >= start)
            .then(|| self.messages.get((ordinal - start) as usize))
            .flatten()
    }

    fn place(&mut self, message: Arc<Message>, entry: EntryState, sources: Arc<[u64]>) {
        self.messages
            .push(WindowMessage::from_records(message, sources));
        self.entries.push(entry);
        self.placed += 1;
    }

    /// A run's input message is about to be placed: the first one of a run starts its episode,
    /// and the first user message among them is the episode's first user message.
    fn note_run_input(&mut self, input: &crate::record::Input) {
        let ordinal = self.placed + 1;
        let starts = self
            .episodes
            .last()
            .is_none_or(|episode| episode.run_id != input.run_id);
        if starts {
            self.episodes.push(Episode {
                run_id: input.run_id.clone(),
                start: ordinal,
                first_user: None,
                render_config: self.render_config.clone(),
                render_key: self.render_key,
                first_model_answer: None,
            });
        }
        let episode = self.episodes.last_mut().expect("pushed above");
        if episode.first_user.is_none() && input.message.role == Role::User {
            episode.first_user = Some(ordinal);
        }
        if input.message.role == Role::User
            && input
                .message
                .content
                .iter()
                .any(|block| !matches!(block, ContentBlock::ToolResult(_)))
        {
            self.human_prompts += 1;
        }
    }

    /// The assistant message still waiting for replies, if any.
    pub fn open_tool_turn(&self) -> Option<&Message> {
        self.open.as_ref().map(|turn| &*turn.assistant)
    }

    /// A render-only view before an entry in the window. A held tool turn and everything after
    /// the cut are omitted, so rebuilding the request that produced that turn cannot include
    /// its own answer or results. The original conversation is unchanged.
    pub fn before_entry(&self, ordinal: u64) -> Option<Self> {
        let start = self.window_start();
        if ordinal < start || ordinal > self.placed + 1 {
            return None;
        }
        let mut before = self.clone();
        let keep = (ordinal - start) as usize;
        before.messages.truncate(keep);
        before.entries.truncate(keep);
        before.placed = ordinal - 1;
        before.open = None;
        before.open_entry = None;
        before.episodes.retain(|episode| episode.start < ordinal);
        before.changes.everything();
        Some(before)
    }

    /// Folds `record` without knowing its seq: the messages it places can't be released from
    /// memory, since nothing could read them back (see `WindowMessage::sources`).
    pub fn apply(&mut self, record: &Record) -> Result<(), FoldError> {
        self.apply_at(None, record)
    }

    /// Folds `record`, at `seq` in the open log when it is known.
    pub fn apply_at(&mut self, seq: Option<u64>, record: &Record) -> Result<(), FoldError> {
        let sources =
            || -> Arc<[u64]> { seq.map_or_else(|| Arc::from([]), |seq| Arc::from([seq])) };
        if record.stream() != Stream::Transcript {
            return Err(FoldError::LedgerRecord(record.name()));
        }
        match record {
            Record::Input(input) => {
                if self.open.is_some() {
                    // A new run's input ends a turn its interrupted run left unanswered: that
                    // turn never reached the conversation and never will. Any other input there
                    // would split a call from its result.
                    if input.placement != Placement::RunInput {
                        return Err(FoldError::InputInsideToolTurn);
                    }
                    self.open = None;
                    self.open_entry = None;
                }
                if input.placement == Placement::RunInput {
                    self.note_run_input(input);
                }
                self.place(Arc::clone(&input.message), EntryState::default(), sources());
            }
            Record::Assistant(assistant) => {
                if let Some(episode) = self
                    .episodes
                    .last_mut()
                    .filter(|episode| episode.run_id == assistant.run_id)
                {
                    episode.first_model_answer.get_or_insert(self.placed + 1);
                }
                if self.open.is_some() {
                    return Err(FoldError::AssistantInsideToolTurn);
                }
                let calls: Vec<ToolCallId> = assistant
                    .message
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::ToolCall(call) => Some(call.tool_call_id.clone()),
                        _ => None,
                    })
                    .collect();
                if assistant.budget_finalize == Some(true) {
                    // A run's budget-finalize step is its last, and the calls a provider emits in
                    // it anyway are never run, so they have no results. Its message enters at
                    // once without them, keeping its text.
                    let mut message = Message::clone(&assistant.message);
                    message
                        .content
                        .retain(|block| !matches!(block, ContentBlock::ToolCall(_)));
                    self.place(Arc::new(message), EntryState::default(), sources());
                } else if calls.is_empty() {
                    self.place(
                        Arc::clone(&assistant.message),
                        EntryState::default(),
                        sources(),
                    );
                } else {
                    self.open = Some(ToolTurn {
                        assistant: Arc::clone(&assistant.message),
                        calls,
                        replies: Vec::new(),
                    });
                    self.open_entry = Some(EntryState::default());
                    self.open_sources = OpenSources(seq.map(|seq| vec![seq]));
                }
            }
            Record::ToolReply(reply) => {
                let id = &reply.result.tool_call_id;
                let Some(turn) = self.open.as_mut().filter(|turn| turn.calls.contains(id)) else {
                    return Err(FoldError::ReplyWithoutCall(id.0.clone()));
                };
                // A model can repeat a call id in one message; each copy gets its own reply, in
                // the order they come, and no copy gets a second one.
                let replied = turn
                    .replies
                    .iter()
                    .filter(|known| &known.tool_call_id == id)
                    .count();
                let asked = turn.calls.iter().filter(|call| *call == id).count();
                if replied >= asked {
                    return Err(FoldError::SecondReply(id.0.clone()));
                }
                turn.replies.push(reply.result.clone());
                // A reply whose seq isn't known, or a turn read from a checkpoint, leaves the turn's
                // messages without sources.
                self.open_sources.0 = match (self.open_sources.0.take(), seq) {
                    (Some(mut known), Some(seq)) => {
                        known.push(seq);
                        Some(known)
                    }
                    _ => None,
                };
                if turn.replies.len() == turn.calls.len() {
                    let mut turn = self.open.take().expect("checked above");
                    let results = turn
                        .calls
                        .iter()
                        .map(|id| {
                            let at = turn
                                .replies
                                .iter()
                                .position(|reply| &reply.tool_call_id == id)
                                .expect("every call has a reply");
                            ContentBlock::ToolResult(turn.replies.remove(at))
                        })
                        .collect();
                    let entry = self.open_entry.take().unwrap_or_default();
                    // Both messages of the turn come back from the same records: the assistant's
                    // record and its replies, folded again in order.
                    let turn_sources: Arc<[u64]> = self
                        .open_sources
                        .0
                        .take()
                        .map_or_else(|| Arc::from([]), Arc::from);
                    self.place(turn.assistant, entry, Arc::clone(&turn_sources));
                    self.place(
                        Arc::new(Message::new(Role::Tool, results)),
                        EntryState::default(),
                        turn_sources,
                    );
                }
            }
            Record::ResultAppend(append) => {
                // A ride (text with an envelope) is never joined into the stored result, in
                // either layout: requests render it after the result through the runner's ride
                // rules, and reads serve the result as the tool returned it. Text without one is
                // joined on a session that joins delivered text, unless the result was placed
                // after the session enrolled in the current compaction contract
                // (`joined_through`).
                if append.ride.is_none() && self.joins_delivered_text() {
                    if let Some(at) = append_to_newest_result(
                        &mut self.messages,
                        &append.tool_call_id,
                        &append.text,
                    )? {
                        let ordinal = self.window_start() + at as u64;
                        self.changes.message(ordinal);
                        match seq {
                            Some(seq) => self.messages[at].add_source(seq),
                            // Its content changed by a record nothing can read back: it stays
                            // held for good.
                            None => {
                                let held = Arc::clone(
                                    self.messages[at].held().expect("just changed in place"),
                                );
                                self.messages[at] = WindowMessage::new(held);
                            }
                        }
                    }
                } else {
                    self.attach_delivery(append);
                }
            }
            Record::EntryFacts(facts) => {
                let start = self.window_start();
                if self.open.is_some() && facts.ordinal == self.placed + 1 {
                    self.open_entry
                        .get_or_insert_with(EntryState::default)
                        .facts = Some(Arc::clone(facts));
                } else if facts.ordinal >= start && facts.ordinal <= self.placed {
                    self.entries[(facts.ordinal - start) as usize].facts = Some(Arc::clone(facts));
                    self.changes.state(facts.ordinal);
                } else if facts.ordinal > self.placed {
                    return Err(FoldError::FactsWithoutEntry(facts.ordinal));
                }
                // Facts for a message below the window have nothing to attach to here, as the
                // message itself isn't held: a fold that keeps only counts drops both.
            }
            Record::TurnFacts(turn) => {
                let start = self.window_start();
                if self.open.is_some() && turn.ordinal == self.placed + 1 {
                    self.open_entry.get_or_insert_with(EntryState::default).turn =
                        Some(Arc::clone(turn));
                } else if turn.ordinal >= start && turn.ordinal <= self.placed {
                    self.entries[(turn.ordinal - start) as usize].turn = Some(Arc::clone(turn));
                    self.changes.state(turn.ordinal);
                } else if turn.ordinal > self.placed {
                    return Err(FoldError::FactsWithoutEntry(turn.ordinal));
                }
            }
            Record::Marker(marker) => self.marker = Some(marker.clone()),
            Record::SetupStub(stub) => {
                self.render_config = stub
                    .parts
                    .iter()
                    .find(|part| part.kind == PartKind::RenderConfig)
                    .cloned();
                self.render_key = stub.render_key;
            }
            Record::Compaction(compaction) => {
                self.first_protocol_compaction |= compaction.version == 1;
            }
            _ => unreachable!("every transcript kind is matched above"),
        }
        Ok(())
    }
}

impl Conversation {
    /// Attaches delivered text to the newest window message that made the call. Delivered text is
    /// only ever attached to a result of a batch that already finished, so an unknown call is
    /// one whose message left the window, and the text goes with it.
    fn attach_delivery(&mut self, append: &crate::record::ResultAppend) {
        let start = self.window_start();
        for (at, (message, entry)) in self
            .messages
            .iter()
            .zip(&mut self.entries)
            .enumerate()
            .rev()
        {
            let part = message.blocks().position(|block| {
                block.kind() == BlockKind::ToolCall
                    && block.tool_call_id() == Some(&append.tool_call_id)
            });
            if let Some(part) = part {
                entry.delivered.push(Delivered {
                    part: part as u32,
                    notice: append.notice.clone(),
                    text: append.text.clone(),
                    send_id: append.send_id.clone(),
                    ride: append.ride.clone(),
                });
                self.changes.state(start + at as u64);
                return;
            }
        }
    }
}

mod window_message;
pub use window_message::{BlockKind, BlockRef, BlockShape, Blocks, NotHeld, WindowMessage};

/// Why a window message couldn't be read back (see [`Conversation::read_back`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadBackError {
    /// No message with that ordinal is in the window.
    NotInWindow(u64),
    /// The records didn't fold.
    Fold(FoldError),
    /// The records folded to a message other than the one the window holds.
    Mismatch(u64),
}

impl Conversation {
    /// The window message at `ordinal` with all its content: the message itself while it is
    /// held, or else folded again from `records`, the records at its sources in order
    /// (`WindowMessage::sources`), and checked against what the window keeps of it.
    pub fn read_back(
        &self,
        ordinal: u64,
        records: &[Record],
    ) -> Result<Arc<Message>, ReadBackError> {
        let message = self
            .window_message(ordinal)
            .ok_or(ReadBackError::NotInWindow(ordinal))?;
        if let Some(held) = message.held() {
            return Ok(Arc::clone(held));
        }
        // A tool turn's records place its assistant message, then its results message, so the
        // results message's records start one place before it.
        let turn = matches!(
            records.first(),
            Some(Record::Assistant(assistant))
                if assistant.message.content.iter().any(|block| matches!(block, ContentBlock::ToolCall(_)))
                    && assistant.budget_finalize != Some(true)
        );
        let first = if turn && message.role() == Role::Tool {
            ordinal - 1
        } else {
            ordinal
        };
        let mut scratch = Conversation::new(self.appends_in_conversation);
        scratch.joined_through = self.joined_through;
        scratch.placed = first - 1;
        for record in records {
            scratch.apply(record).map_err(ReadBackError::Fold)?;
        }
        let read = scratch
            .held_message(ordinal)
            .ok()
            .flatten()
            .ok_or(ReadBackError::Mismatch(ordinal))?;
        if !message.stands_for(read) {
            return Err(ReadBackError::Mismatch(ordinal));
        }
        Ok(Arc::clone(read))
    }

    /// Releases the blocks at `blocks` (indexes into its blocks) of the window message at
    /// `ordinal` from memory, unless it is the window's newest message, which the fold may still
    /// change, or its sources aren't known. Whether anything was released.
    pub fn release(&mut self, ordinal: u64, blocks: &[usize]) -> bool {
        if ordinal >= self.placed {
            return false;
        }
        let start = self.window_start();
        let Some(at) = ordinal.checked_sub(start).map(|at| at as usize) else {
            return false;
        };
        let Some(released) = self
            .messages
            .get(at)
            .and_then(|message| message.releasing(blocks))
        else {
            return false;
        };
        let changed = released != self.messages[at];
        self.messages[at] = released;
        changed
    }

    /// The ordinals of the window's messages whose content was released, ascending.
    pub fn released_ordinals(&self) -> Vec<u64> {
        let start = self.window_start();
        self.messages
            .iter()
            .enumerate()
            .filter(|(_, message)| message.is_released())
            .map(|(at, _)| start + at as u64)
            .collect()
    }

    /// Holds the window message at `ordinal` whole again: `whole` is that message read back
    /// (see [`Conversation::read_back`]), checked against what the window keeps of it. Whether
    /// it was taken: a message not in the window, or one `whole` doesn't stand for, is left as it
    /// is.
    pub fn restore(&mut self, ordinal: u64, whole: Arc<Message>) -> bool {
        let start = self.window_start();
        let Some(at) = ordinal.checked_sub(start).map(|at| at as usize) else {
            return false;
        };
        let Some(message) = self.messages.get_mut(at) else {
            return false;
        };
        if message.held().is_some() || !message.stands_for(&whole) {
            return false;
        }
        let sources: Arc<[u64]> = message.sources().into();
        *message = WindowMessage::from_records(whole, sources);
        true
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod generate;

/// Appends `text` to the named result in the newest tool message, if it holds that result, and
/// says where that message is in `messages`. Delivered text is only ever attached to a result of
/// the batch that just finished. The message is copied first only when another holder shares it.
fn append_to_newest_result(
    messages: &mut [WindowMessage],
    id: &ToolCallId,
    text: &str,
) -> Result<Option<usize>, FoldError> {
    use broca_protocol::content::{ToolOutput, ToolOutputPart};
    let Some(at) = messages.iter().rposition(|m| m.role() == Role::Tool) else {
        return Ok(None);
    };
    let Some(block) = messages[at].blocks().position(|block| {
        block.kind() == BlockKind::ToolResult && block.tool_call_id() == Some(id)
    }) else {
        return Ok(None);
    };
    let not_held = || FoldError::ResultNotHeld(id.0.clone());
    if let ContentBlock::ToolResult(result) =
        &messages[at].held().ok_or_else(not_held)?.content[block]
    {
        if matches!(
            result.output,
            ToolOutput::Json { .. } | ToolOutput::Denied { .. }
        ) {
            return Ok(None);
        }
    }
    let held = messages[at].held_mut().ok_or_else(not_held)?;
    let ContentBlock::ToolResult(result) = &mut held.content[block] else {
        unreachable!("found above");
    };
    match &mut result.output {
        ToolOutput::Text { text: output } => *output = output.appended(text),
        ToolOutput::Error { message, .. } => message.push_str(text),
        ToolOutput::Parts { parts } => parts.push(ToolOutputPart::Text {
            text: text.to_owned().into(),
        }),
        ToolOutput::Json { .. } | ToolOutput::Denied { .. } => unreachable!("checked above"),
    }
    Ok(Some(at))
}
