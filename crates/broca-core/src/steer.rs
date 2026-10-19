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
/// the one run it was created for. The same lock orders boundary cancellation before step
/// admission, so the WAL can never place a new StepStarted after an accepted cancellation.
#[derive(Default)]
pub struct SteerInbox {
    state: Mutex<State>,
    boundary_cancel: tokio_util::sync::CancellationToken,
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
    /// record) under the same lock. `record` returns the runner tags the writer assigned to the
    /// record, one entry per message, and the queued messages carry them: the turn renders the
    /// same tags a replay of the record attaches. Returns `false` without calling `record` once
    /// the inbox is closed: the turn will render no further step, so the caller must not steer
    /// into it.
    pub fn deliver(
        &self,
        mut messages: Vec<Message>,
        record: impl FnOnce() -> Option<Vec<broca_protocol::tags::MessageTags>>,
    ) -> bool {
        let mut state = self.lock();
        if state.closed {
            return false;
        }
        if let Some(tags) = record() {
            for (message, tags) in messages.iter_mut().zip(&tags) {
                crate::tags::attach(message, Some(tags));
            }
        }
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

    /// Serialize cancellation and step admission with WAL order. The marker is queued before
    /// waking the loop; its caller must sync before acknowledging the cancellation.
    pub fn cancel_at_boundary<E>(&self, record: impl FnOnce() -> Result<(), E>) -> Result<(), E> {
        let _state = self.lock();
        if !self.boundary_cancel.is_cancelled() {
            record()?;
            self.boundary_cancel.cancel();
        }
        Ok(())
    }

    /// The acknowledgement barrier is shared by the actor and crash probes. A duplicate still
    /// waits for the writer, so retrying after a failed barrier never acknowledges volatile data.
    pub async fn request_boundary_cancel(
        &self,
        writer: &crate::WalHandle,
        run_id: &broca_protocol::RunId,
    ) -> Result<(), crate::WalWriteError> {
        self.cancel_at_boundary(|| {
            writer
                .record(broca_wal::WalRecord::RunCancelRequested {
                    run_id: run_id.clone(),
                    at: broca_wal::record::RunCancelAt::StepBoundary,
                })
                .into_result()
                .map(|_| ())
        })?;
        writer.sync().await?;
        Ok(())
    }

    pub fn boundary_cancel(&self) -> &tokio_util::sync::CancellationToken {
        &self.boundary_cancel
    }

    /// Replay has already proved the cancellation durable. Keep the actor's shared signal in
    /// sync with that intent so a later immediate cancel records an escalation after a restart.
    pub fn restore_boundary_cancel(&self) {
        let _state = self.lock();
        self.boundary_cancel.cancel();
    }

    /// A pending cancel and a new step cannot cross: whichever holds the lock first records
    /// first. Returning None means no StepStarted was written and no steer was consumed.
    pub fn try_take_at_step(&self, record: impl FnOnce()) -> Option<Vec<Message>> {
        let mut state = self.lock();
        if self.boundary_cancel.is_cancelled() {
            return None;
        }
        let taken = std::mem::take(&mut state.pending);
        record();
        Some(taken)
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
        assert!(inbox.deliver(vec![message("a")], || {
            log.push("steered");
            None
        }));
        let taken = inbox.take_at_step(|| log.push("step_started"));
        assert_eq!(taken, vec![message("a")]);
        assert_eq!(log, ["steered", "step_started"]);

        assert!(inbox.deliver(vec![message("b")], || {
            log.push("steered");
            None
        }));
        assert!(
            !inbox.close_if_empty(),
            "a queued steer keeps the turn open"
        );
        assert!(!inbox.is_closed());
        inbox.take_at_step(|| {});
        assert!(inbox.close_if_empty());
        assert!(!inbox.deliver(vec![message("c")], || {
            log.push("late");
            None
        }));
        assert!(!log.contains(&"late"));
    }
}
