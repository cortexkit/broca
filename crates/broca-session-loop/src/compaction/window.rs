//! The run's projection of the transcript a step's compaction calls read (see
//! [`super::transcript`]), kept between those calls.
//!
//! A step reads the transcript four times: at its boundary, to prepare its request, in the
//! notice after its answer, and in the notice after its tool results. Projecting an entry
//! computes SHA-256 digests of its canonical JSON (JCS) content, so projecting the whole window
//! each time costs time in proportion to the window. The run keeps each window message's projected entry instead and, at
//! each read, projects only the messages placed since the last one.
//!
//! A placed message's entry still changes in a few ways, so each read checks what it kept:
//! - a later record restates a placed message's facts or turn facts, or delivers text with one of
//!   its calls: the message's entry state no longer matches the state it was projected from, and
//!   it is projected again with everything after it;
//! - text delivered without an envelope on a session that joins it into results changes the
//!   newest tool message itself (and the entry of the assistant message whose calls it answers):
//!   both are projected again when that message no longer matches the one projected;
//! - the assistant message whose calls are still running is projected again at every read.
//!
//! A seal or a revert moves the window's start, and an enrollment changes how messages placed
//! before it are served: either replaces everything kept, as does a transcript read through
//! another session. The sealed entries the views name come in front of the window's, rebuilt
//! when the views or the window's start change.

use std::sync::{Arc, Mutex};

use broca_core::compaction_v2::entries::RideEnvelopes;
use broca_protocol::content::{Message, Role};
use broca_session_store::fold::conversation::{Conversation, Delivered, EntryState};
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
    /// Each window message projected so far, in window order: its entry state as it was when
    /// projected, and whether it is a global system message the session's enrollment suppressed,
    /// whose entry has no parts and which the request's stored prompt leaves out.
    states: Vec<(EntryState, bool)>,
    /// The newest tool message among them, by window position, as it was when projected.
    newest_tool: Option<(usize, Message)>,
    /// The transcript: `sealed` sealed entries in front, then one entry per window message, then
    /// the held assistant message's entry when `open` is set.
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
        self.newest_tool = None;
        let sealed = self.sealed;
        Arc::make_mut(&mut self.entries).truncate(sealed);
        self.open = None;
        self.sealed_for = None;
        self.rides_stale = true;
    }

    /// Forgets the entries of the window messages from position `from` on.
    fn truncate_window(&mut self, from: usize, conversation: &Conversation) {
        self.states.truncate(from);
        let keep = self.sealed + from;
        Arc::make_mut(&mut self.entries).truncate(keep);
        self.newest_tool = conversation.messages[..from]
            .iter()
            .rposition(|message| message.role == Role::Tool)
            .map(|at| (at, conversation.messages[at].clone()));
        self.rides_stale = true;
    }

    /// The window position of the first kept message whose entry changed since it was projected.
    fn first_changed(&self, conversation: &Conversation) -> Option<usize> {
        let restated = self
            .states
            .iter()
            .zip(&conversation.entries)
            .position(|((state, _), now)| state != now);
        // Text joined into the newest tool message changes it, and the entry of the assistant
        // message before it, whose calls it answers.
        let joined = self
            .newest_tool
            .as_ref()
            .filter(|(at, message)| conversation.messages.get(*at) != Some(message))
            .map(|(at, _)| at.saturating_sub(1));
        match (restated, joined) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    fn rebuild_rides(&mut self) {
        let mut rides = RideEnvelopes::default();
        for (mid, delivered) in self.sealed_delivered.iter() {
            crate::entries::push_rides(&mut rides, mid, delivered);
        }
        for (state, suppressed) in &self.states {
            if let (false, Some(facts)) = (*suppressed, &state.facts) {
                crate::entries::push_rides(&mut rides, &facts.mid, &state.delivered);
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
            kept.truncate_window(from, conversation);
        }
        let from = conversation.window_start() + kept.states.len() as u64;
        for stored in broca_session_store::session::window_entries(conversation, from, usize::MAX) {
            let position = (stored.ordinal - conversation.window_start()) as usize;
            let state = stored.state.clone();
            if stored.message.role == Role::Tool {
                kept.newest_tool = Some((position, stored.message.clone()));
            }
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
            kept.states.push((state, suppressed));
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
        messages: Vec<(u64, Message)>,
        delivered: Vec<(String, Vec<Delivered>)>,
    ) {
        let mut kept = self.0.lock().expect("window transcript");
        if kept.sealed == 0 && entries.is_empty() && kept.sealed_for == key {
            return;
        }
        let sealed = kept.sealed;
        let count = entries.len();
        Arc::make_mut(&mut kept.entries).splice(..sealed, entries);
        kept.sealed = count;
        kept.sealed_for = key;
        kept.sealed_messages = Arc::new(messages);
        kept.sealed_delivered = Arc::new(delivered);
        kept.rides_stale = true;
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
            suppressed: kept
                .states
                .iter()
                .enumerate()
                .filter(|(_, (_, suppressed))| *suppressed)
                .map(|(position, _)| kept.sealed + position)
                .collect(),
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
