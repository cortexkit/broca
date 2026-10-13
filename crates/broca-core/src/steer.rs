//! The hand-off of steered messages from the session's lineage actor to the running turn.
//!
//! A steer's position in the conversation is derived from WAL order alone: replay places a
//! `Steered` record's message at the episode's next `StepStarted`, or at its `RunFinished` when
//! no step follows (see `WalRecord::Steered`). The live loop must render exactly what that
//! replay will reconstruct, so the two writers involved agree through one lock:
//!
//! - the actor appends the `Steered` record and queues its message while holding the lock
//!   ([`SteerInbox::deliver`]);
//! - the loop takes the queued messages and appends `StepStarted` while holding the same lock
//!   ([`SteerInbox::take_at_step`]).
//!
//! So every steer is either before that `StepStarted` in the WAL and in that step's prompt, or
//! after it in both. Once the run will write no further step the loop closes the inbox
//! ([`SteerInbox::close`]) before its terminal record, and the actor then treats the session as
//! having no running turn: no `Steered` record can follow an episode's terminal.

use std::sync::{Arc, Mutex};

use broca_protocol::content::Message;

#[derive(Default)]
struct State {
    pending: Vec<Message>,
    closed: bool,
}

/// Steered messages waiting for the running turn's next step. Shared by the lineage actor and
/// the one run it was created for.
#[derive(Default)]
pub struct SteerInbox {
    state: Mutex<State>,
}

impl SteerInbox {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Queue `messages` for the running turn, calling `record` (which appends their `Steered`
    /// record) under the same lock. Returns `false` without calling `record` once the inbox is
    /// closed: the turn will render no further step, so the caller must not steer into it.
    pub fn deliver(&self, messages: Vec<Message>, record: impl FnOnce()) -> bool {
        let mut state = self.lock();
        if state.closed {
            return false;
        }
        record();
        state.pending.extend(messages);
        true
    }

    /// Take every queued message and call `record` (which appends `StepStarted`) under the same
    /// lock, so the taken messages are exactly the ones the WAL holds before that record.
    pub fn take_at_step(&self, record: impl FnOnce()) -> Vec<Message> {
        let mut state = self.lock();
        let taken = std::mem::take(&mut state.pending);
        record();
        taken
    }

    /// At a final answer the turn could extend: close the inbox if nothing is queued and return
    /// `true` (the turn ends here), or leave it open and return `false` (a steer is waiting, so
    /// the turn runs one more step to render it).
    pub fn close_if_empty(&self) -> bool {
        let mut state = self.lock();
        if state.pending.is_empty() {
            state.closed = true;
            true
        } else {
            false
        }
    }

    /// The turn will render no further step. Messages still queued stay undelivered to a model;
    /// their records are already in the WAL, and replay places them at the episode's close.
    pub fn close(&self) {
        self.lock().closed = true;
    }

    /// Whether the inbox has been closed.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::content::{ContentBlock, Role};

    fn message(text: &str) -> Message {
        Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
    }

    /// A delivery records and queues; the next step takes it with its record; a closed inbox
    /// refuses without recording.
    #[test]
    fn delivery_step_and_close_follow_one_lock() {
        let inbox = SteerInbox::new();
        let mut log = Vec::new();
        assert!(inbox.deliver(vec![message("a")], || log.push("steered")));
        let taken = inbox.take_at_step(|| log.push("step_started"));
        assert_eq!(taken, vec![message("a")]);
        assert_eq!(log, ["steered", "step_started"]);

        assert!(inbox.deliver(vec![message("b")], || log.push("steered")));
        assert!(
            !inbox.close_if_empty(),
            "a queued steer keeps the turn open"
        );
        assert!(!inbox.is_closed());
        inbox.take_at_step(|| {});
        assert!(inbox.close_if_empty());
        assert!(!inbox.deliver(vec![message("c")], || log.push("late")));
        assert!(!log.contains(&"late"));
    }
}
