//! The run's projection of the transcript a step's compaction calls read (see
//! [`super::transcript`]), kept between those calls.
//!
//! A step reads the transcript four times: at its boundary, to prepare its request, in the
//! notice after its answer, and in the notice after its tool results. Projecting an entry
//! computes SHA-256 digests of its canonical JSON (JCS) content, so projecting the whole window
//! each time costs time in proportion to the window. The run keeps each window message's projected entry instead and, at
//! each read, projects only the messages placed since the last one.
//!
//! A placed message's entry still changes in a few ways, which the conversation fold records as
//! they happen (`Conversation::changes`), so each read asks it what changed since the last one
//! rather than comparing every kept entry:
//! - a later record restates a placed message's facts or turn facts, or delivers text with one of
//!   its calls: the message is projected again with everything after it;
//! - text delivered without an envelope on a session that joins it into results changes the
//!   newest tool message itself, and the entry of the assistant message whose calls it answers:
//!   both are projected again, with everything after them;
//! - the assistant message whose calls are still running is projected again at every read.
//!
//! A seal or a revert moves the window's start, and an enrollment changes how messages placed
//! before it are served: either replaces everything kept, as does a transcript read through
//! another session. The sealed entries the views name come in front of the window's, rebuilt
//! when the views or the window's start change.

use std::sync::{Arc, Mutex};

use broca_core::compaction_v2::entries::RideEnvelopes;
use broca_protocol::content::Message;
use broca_session_store::fold::conversation::{ChangeMark, Conversation, Delivered, EntryState};
use broca_session_store::session::{EnrollmentOverlay, StoredEntry};
use cortexkit_role_compaction::read::Entry;

use super::{BuiltFor, Transcript};

/// What a run keeps of the transcript between its steps' reads. Every read the run makes shares
/// one: cloning it shares the kept projection. A new one keeps nothing, and the first read
/// projects the whole window.
#[derive(Clone, Default)]
pub struct WindowTranscript(Arc<Mutex<Kept>>);

/// What the window's entries were served under: a change to any of it replaces them all.
#[derive(Clone, PartialEq)]
struct WindowKey {
    session: String,
    window_start: u64,
    /// The newest message placed before the session enrolled, for a session that enrolled.
    enrolled_through: Option<u64>,
}

#[derive(Default)]
struct Kept {
    key: Option<WindowKey>,
    /// Where the conversation's record of changes stood at the last read.
    seen: ChangeMark,
    /// Each window message projected so far, in window order.
    states: Vec<KeptMessage>,
    /// The transcript: `sealed` sealed entries in front, then one entry per window message, then
    /// the held assistant message's entry when `open` is set.
    entries: Arc<Vec<Entry>>,
    sealed: usize,
    open: Option<OpenRides>,
    /// What decided which sealed entries are in front; `None` when there are none.
    sealed_for: Option<BuiltFor>,
    sealed_messages: crate::compaction::SealedMessages,
    sealed_delivered: Arc<Vec<(String, Vec<Delivered>)>>,
    /// The ride envelopes of the sealed entries and the served window entries, rebuilt from them
    /// when `rides_stale` is set.
    rides: Arc<RideEnvelopes>,
    rides_stale: bool,
}

/// One projected window message: its entry state as it was when projected, and whether it is a
/// global system message the session's enrollment suppressed, whose entry has no parts and
/// carries no rides.
struct KeptMessage {
    state: EntryState,
    suppressed: bool,
}

/// The held assistant message's id and delivered texts, whose rides a transcript adds.
struct OpenRides {
    mid: String,
    delivered: Vec<Delivered>,
}

impl Kept {
    /// Forgets the window's entries, keeping the sealed ones in front.
    fn reset_window(&mut self, key: WindowKey) {
        self.key = Some(key);
        self.states.clear();
        let sealed = self.sealed;
        Arc::make_mut(&mut self.entries).truncate(sealed);
        self.open = None;
        self.sealed_for = None;
        self.rides_stale = true;
    }

    /// Forgets the entries of the window messages from position `from` on, and their rides.
    fn truncate_window(&mut self, from: usize) {
        #[cfg(test)]
        CHECKED.with(|count| count.set(count.get() + self.states.len().saturating_sub(from)));
        if from >= self.states.len() {
            return;
        }
        // Every window message has an entry, so the window's entries before `from` are `from`.
        let keep = self.sealed + from;
        if !self.rides_stale {
            let rides = Arc::make_mut(&mut self.rides);
            for kept in &self.states[from..] {
                if let (false, Some(facts)) = (kept.suppressed, &kept.state.facts) {
                    crate::entries::forget_rides(rides, &facts.mid, &kept.state.delivered);
                }
            }
        }
        self.states.truncate(from);
        Arc::make_mut(&mut self.entries).truncate(keep);
    }

    /// The window position of the first kept message whose entry changed since the last read:
    /// its entry state was restated, or the message after it changed (text joined into a tool
    /// message changes the entry of the assistant message whose calls it answers, too).
    fn first_changed(&self, conversation: &Conversation) -> Option<usize> {
        let changes = &conversation.changes;
        let restated = changes.states_since(self.seen);
        let rewritten = changes
            .messages_since(self.seen)
            .map(|ordinal| ordinal.saturating_sub(1));
        let ordinal = match (restated, rewritten) {
            (Some(a), Some(b)) => a.min(b),
            (a, b) => a.or(b)?,
        };
        Some(ordinal.saturating_sub(conversation.window_start()) as usize)
    }

    fn rebuild_rides(&mut self) {
        let mut rides = RideEnvelopes::default();
        for (mid, delivered) in self.sealed_delivered.iter() {
            crate::entries::push_rides(&mut rides, mid, delivered);
        }
        for kept in &self.states {
            if let (false, Some(facts)) = (kept.suppressed, &kept.state.facts) {
                crate::entries::push_rides(&mut rides, &facts.mid, &kept.state.delivered);
            }
        }
        self.rides = Arc::new(rides);
        self.rides_stale = false;
    }
}

/// `stored` as the transcript serves it: as the session's enrollment decided for a message placed
/// before it, with no content for one it suppressed.
fn served(enrollment: Option<&EnrollmentOverlay>, stored: StoredEntry) -> StoredEntry {
    match enrollment {
        Some(overlay) => overlay.apply(stored),
        None => stored,
    }
}

/// One window entry's projection, counted for the tests that check how many a step makes.
fn project(stored: &StoredEntry) -> Result<Entry, String> {
    #[cfg(test)]
    PROJECTED.with(|count| count.set(count.get() + 1));
    crate::entries::project(stored)
}

#[cfg(test)]
thread_local! {
    /// How many window entries this thread projected through the kept transcript.
    pub(super) static PROJECTED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// How many kept window entries this thread's reads went back over because they changed.
    pub(super) static CHECKED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A handle on a run's kept transcript that doesn't keep it: the session's owner sizes it for a
/// memory report while the run holds it, and finds nothing once the run has dropped it.
#[derive(Clone, Default)]
pub struct WeakWindow(std::sync::Weak<Mutex<Kept>>);

impl WeakWindow {
    /// What the run's kept transcript holds (see `broca_session_store::held`), or `None` once
    /// the run has dropped it. Holds it, and the lock each of the run's reads takes, only for
    /// the length of the walk.
    pub fn held_bytes(&self) -> Option<usize> {
        let kept = self.0.upgrade()?;
        let held = kept.lock().ok().map(|kept| kept.held_bytes());
        held
    }
}

impl Kept {
    /// The compaction entries and what they were projected from. Entries are the compaction
    /// contract's type, sized by their serialized walk (`held::serde_heap_bytes`); the sealed
    /// entries' ride envelopes and what decided them count at their inline size.
    fn held_bytes(&self) -> usize {
        use broca_session_store::held::{serde_heap_bytes, HeldBytes};
        // A tool part's input shares its text with the call in the session's message, which
        // the session holds anyway, so the walk's count of that text is taken back out.
        let shared_inputs: usize = self
            .entries
            .iter()
            .flat_map(|entry| &entry.parts)
            .map(|part| match &part.body {
                cortexkit_role_compaction::part::PartBody::Tool(tool) => tool.input.as_str().len(),
                _ => 0,
            })
            .sum();
        let entries = 2 * std::mem::size_of::<usize>()
            + std::mem::size_of::<Vec<Entry>>()
            + self.entries.capacity() * std::mem::size_of::<Entry>()
            + self
                .entries
                .iter()
                .map(serde_heap_bytes)
                .sum::<usize>()
                .saturating_sub(shared_inputs);
        let states = self.states.capacity() * std::mem::size_of::<KeptMessage>()
            + self
                .states
                .iter()
                .map(|kept| kept.state.heap_bytes())
                .sum::<usize>();
        let key = self.key.as_ref().map_or(0, |key| key.session.heap_bytes());
        let open = self.open.as_ref().map_or(0, |open| {
            std::mem::size_of::<OpenRides>() + open.mid.heap_bytes() + open.delivered.heap_bytes()
        });
        std::mem::size_of::<Self>()
            + entries
            + states
            + key
            + open
            + self.sealed_messages.heap_bytes()
            + self.sealed_delivered.heap_bytes()
            + 2 * std::mem::size_of::<usize>()
            + std::mem::size_of::<RideEnvelopes>()
    }
}

impl WindowTranscript {
    /// A handle on what this transcript keeps that doesn't keep it (see [`WeakWindow`]).
    pub fn downgrade(&self) -> WeakWindow {
        WeakWindow(Arc::downgrade(&self.0))
    }

    /// The first window ordinal [`WindowTranscript::update`] would project, when brought up to
    /// `conversation` read through session `session` under `enrollment`: a caller reads back the
    /// released messages from there on first.
    pub(super) fn projects_from(
        &self,
        session: String,
        conversation: &Conversation,
        enrollment: Option<&EnrollmentOverlay>,
    ) -> u64 {
        let kept = self.0.lock().expect("window transcript");
        let key = window_key(session, conversation, enrollment);
        let from =
            if kept.key.as_ref() != Some(&key) || kept.states.len() > conversation.entries.len() {
                0
            } else {
                kept.first_changed(conversation)
                    .map_or(kept.states.len(), |changed| changed.min(kept.states.len()))
            };
        conversation.window_start() + from as u64
    }

    /// Brings the window's entries up to `conversation`, read through session `session` under
    /// `enrollment`: projects the messages placed since the last read, and again any whose entry
    /// changed (see the module's notes). `whole` gives a window message whose content was
    /// released, read back, for each one from [`WindowTranscript::projects_from`] on.
    pub(super) fn update(
        &self,
        session: String,
        conversation: &Conversation,
        enrollment: Option<&EnrollmentOverlay>,
        whole: impl Fn(u64) -> Option<Arc<Message>>,
    ) -> Result<(), String> {
        let mut kept = self.0.lock().expect("window transcript");
        let key = window_key(session, conversation, enrollment);
        if kept.key.as_ref() != Some(&key) || kept.states.len() > conversation.entries.len() {
            kept.reset_window(key);
        }
        // The held assistant message's entry, projected at the last read, goes first: it is
        // projected again below.
        if kept.open.take().is_some() {
            Arc::make_mut(&mut kept.entries).pop();
        }
        if let Some(from) = kept.first_changed(conversation) {
            kept.truncate_window(from);
        }
        kept.seen = conversation.changes.mark();
        let from = conversation.window_start() + kept.states.len() as u64;
        for stored in
            broca_session_store::session::window_entries_with(conversation, from, usize::MAX, whole)
                .map_err(|not| format!("window message {} isn't held in memory", not.ordinal))?
        {
            let state = stored.state.clone();
            let suppressed = enrollment.is_some_and(|overlay| overlay.suppressed(stored.ordinal));
            let stored = served(enrollment, stored);
            let entry = project(&stored)?;
            Arc::make_mut(&mut kept.entries).push(entry);
            if let (false, false, Some(facts)) = (kept.rides_stale, suppressed, &stored.state.facts)
            {
                crate::entries::push_rides(
                    Arc::make_mut(&mut kept.rides),
                    &facts.mid,
                    &stored.state.delivered,
                );
            }
            kept.states.push(KeptMessage { state, suppressed });
        }
        if let Some((ordinal, message, state)) = conversation.open_entry() {
            let stored = served(
                enrollment,
                StoredEntry {
                    ordinal,
                    message: message.clone(),
                    next: None,
                    state: state.clone(),
                    open: true,
                },
            );
            let entry = project(&stored)?;
            Arc::make_mut(&mut kept.entries).push(entry);
            kept.open = Some(OpenRides {
                mid: stored
                    .state
                    .facts
                    .as_ref()
                    .map(|facts| facts.mid.clone())
                    .unwrap_or_default(),
                delivered: stored.state.delivered,
            });
        }
        Ok(())
    }

    /// What decided the sealed entries in front, if any are kept.
    pub(super) fn sealed_for(&self) -> Option<BuiltFor> {
        self.0.lock().expect("window transcript").sealed_for.clone()
    }

    /// Puts `entries` in front of the window's, as the sealed entries the views under `key` name,
    /// with their messages and delivered texts. Nothing changes when the same views are kept and
    /// there are no sealed entries either way.
    pub(super) fn set_sealed(
        &self,
        key: Option<BuiltFor>,
        entries: Vec<Entry>,
        messages: Vec<(u64, Arc<Message>)>,
        delivered: Vec<(String, Vec<Delivered>)>,
    ) {
        let mut kept = self.0.lock().expect("window transcript");
        if kept.sealed == 0 && entries.is_empty() && kept.sealed_for == key {
            return;
        }
        if !kept.rides_stale {
            // The rides of the sealed entries in front change with them; the window's stay.
            let old = Arc::clone(&kept.sealed_delivered);
            let rides = Arc::make_mut(&mut kept.rides);
            for (mid, delivered) in old.iter() {
                crate::entries::forget_rides(rides, mid, delivered);
            }
            for (mid, delivered) in &delivered {
                crate::entries::push_rides(rides, mid, delivered);
            }
        }
        let sealed = kept.sealed;
        let count = entries.len();
        Arc::make_mut(&mut kept.entries).splice(..sealed, entries);
        kept.sealed = count;
        kept.sealed_for = key;
        kept.sealed_messages = Arc::new(messages);
        kept.sealed_delivered = Arc::new(delivered);
    }

    /// The transcript as kept: shared, so dropping it drops a reference rather than the window.
    pub(super) fn transcript(&self) -> Transcript {
        let mut kept = self.0.lock().expect("window transcript");
        if kept.rides_stale {
            kept.rebuild_rides();
        }
        let rides = match &kept.open {
            Some(open) if !open.delivered.is_empty() && !open.mid.is_empty() => {
                let mut rides = (*kept.rides).clone();
                crate::entries::push_rides(&mut rides, &open.mid, &open.delivered);
                Arc::new(rides)
            }
            _ => Arc::clone(&kept.rides),
        };
        Transcript {
            entries: Arc::clone(&kept.entries),
            sealed: Arc::clone(&kept.sealed_messages),
            rides,
        }
    }

    /// How many sealed entries the kept transcript puts in front of the window's.
    #[cfg(debug_assertions)]
    pub(super) fn sealed_count(&self) -> usize {
        self.0.lock().expect("window transcript").sealed
    }
}

#[cfg(test)]
mod tests;

/// What the window's kept entries were projected under (see [`WindowTranscript::update`]).
fn window_key(
    session: String,
    conversation: &Conversation,
    enrollment: Option<&EnrollmentOverlay>,
) -> WindowKey {
    WindowKey {
        session,
        window_start: conversation.window_start(),
        enrolled_through: enrollment.map(EnrollmentOverlay::through_ordinal),
    }
}
