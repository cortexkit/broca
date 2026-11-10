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
    /// The transcript: `sealed` sealed entries in front, then the served window messages'
    /// entries, then the held assistant message's entry when `open` is set.
    entries: Arc<Vec<Entry>>,
    sealed: usize,
    open: Option<OpenRides>,
    /// What decided which sealed entries are in front; `None` when there are none.
    sealed_for: Option<BuiltFor>,
    sealed_messages: Arc<Vec<(u64, Message)>>,
    sealed_delivered: Arc<Vec<(String, Vec<Delivered>)>>,
    /// The ride envelopes of the sealed entries and the served window entries, rebuilt from them
    /// when `rides_stale` is set.
    rides: Arc<RideEnvelopes>,
    rides_stale: bool,
}

/// One projected window message: its entry state as it was when projected, whether it was
/// served (an enrollment leaves out the global system messages it suppressed), and how many
/// window messages before it were.
struct KeptMessage {
    state: EntryState,
    served: bool,
    served_before: usize,
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

    /// How many of the kept window messages were served.
    fn served(&self) -> usize {
        self.states
            .last()
            .map_or(0, |last| last.served_before + usize::from(last.served))
    }

    /// Forgets the entries of the window messages from position `from` on, and their rides.
    fn truncate_window(&mut self, from: usize) {
        #[cfg(test)]
        CHECKED.with(|count| count.set(count.get() + self.states.len().saturating_sub(from)));
        let Some(first) = self.states.get(from) else {
            return;
        };
        let keep = self.sealed + first.served_before;
        if !self.rides_stale {
            let rides = Arc::make_mut(&mut self.rides);
            for kept in &self.states[from..] {
                if let (true, Some(facts)) = (kept.served, &kept.state.facts) {
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
            if let (true, Some(facts)) = (kept.served, &kept.state.facts) {
                crate::entries::push_rides(&mut rides, &facts.mid, &kept.state.delivered);
            }
        }
        self.rides = Arc::new(rides);
        self.rides_stale = false;
    }
}

/// `stored` as the transcript serves it: as the session's enrollment decided for a message placed
/// before it, `None` for one it suppressed.
fn served(enrollment: Option<&EnrollmentOverlay>, stored: StoredEntry) -> Option<StoredEntry> {
    match enrollment {
        Some(overlay) => overlay.apply(stored),
        None => Some(stored),
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

impl WindowTranscript {
    /// Brings the window's entries up to `conversation`, read through session `session` under
    /// `enrollment`: projects the messages placed since the last read, and again any whose entry
    /// changed (see the module's notes).
    pub(super) fn update(
        &self,
        session: String,
        conversation: &Conversation,
        enrollment: Option<&EnrollmentOverlay>,
    ) -> Result<(), String> {
        let mut kept = self.0.lock().expect("window transcript");
        let key = WindowKey {
            session,
            window_start: conversation.window_start(),
            enrolled_through: enrollment.map(EnrollmentOverlay::through_ordinal),
        };
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
        for stored in broca_session_store::session::window_entries(conversation, from, usize::MAX) {
            let state = stored.state.clone();
            let served_before = kept.served();
            let Some(stored) = served(enrollment, stored) else {
                kept.states.push(KeptMessage {
                    state,
                    served: false,
                    served_before,
                });
                continue;
            };
            let entry = project(&stored)?;
            Arc::make_mut(&mut kept.entries).push(entry);
            if let (false, Some(facts)) = (kept.rides_stale, &stored.state.facts) {
                crate::entries::push_rides(
                    Arc::make_mut(&mut kept.rides),
                    &facts.mid,
                    &stored.state.delivered,
                );
            }
            kept.states.push(KeptMessage {
                state,
                served: true,
                served_before,
            });
        }
        if let Some((ordinal, message, state)) = conversation.open_entry() {
            let stored = StoredEntry {
                ordinal,
                message: message.clone(),
                next: None,
                state: state.clone(),
                open: true,
            };
            if let Some(stored) = served(enrollment, stored) {
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
        messages: Vec<(u64, Message)>,
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
