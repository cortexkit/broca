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
    /// Queued sends with an ordered `delivery` list, in queue order, that this turn may still
    /// deliver (see `crate::delivery_order`). The actor adds them and the run takes them, both
    /// under this lock, so the WAL order of acceptance, replacement and delivery is the order in
    /// which this list changed.
    ordered: Vec<crate::delivery_order::OrderedSend>,
    /// Ordered sends this turn delivered or ended, so the actor drops them from its queue even
    /// before it next re-reads the queue from the WAL.
    consumed: Vec<broca_protocol::SubmissionId>,
}

/// What [`SteerInbox::accept_ordered`] did with a new ordered send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderedAcceptance {
    /// The send is queued for this turn; `superseded` names the undelivered send it replaced.
    Queued {
        superseded: Option<broca_protocol::SubmissionId>,
    },
    /// The turn renders no further step; the caller queues the send for a later run itself.
    Closed,
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

    /// Give the turn the ordered sends already queued when it starts.
    pub fn seed_ordered(&self, sends: Vec<crate::delivery_order::OrderedSend>) {
        self.lock().ordered = sends;
    }

    /// Queue an ordered send for this turn. Under the lock, an undelivered send from the same
    /// sender with the same dedupe key is removed and `record` is called with its id, so the
    /// caller writes the replacement's `SendEnded` and the new send's records in the order the
    /// turn sees them. A send the turn already delivered is never replaced. Once the inbox is
    /// closed nothing is recorded and the caller queues the send itself.
    pub fn accept_ordered(
        &self,
        send: crate::delivery_order::OrderedSend,
        record: impl FnOnce(Option<&broca_protocol::SubmissionId>),
    ) -> OrderedAcceptance {
        let mut state = self.lock();
        if state.closed {
            return OrderedAcceptance::Closed;
        }
        let replaced = state.ordered.iter().position(|queued| {
            queued.replaced_by(send.principal.as_ref(), send.order.dedupe_key.as_deref())
        });
        let superseded = replaced.map(|at| state.ordered.remove(at).submission_id);
        record(superseded.as_ref());
        state.ordered.push(send);
        OrderedAcceptance::Queued { superseded }
    }

    /// The ordered sends this turn delivered or ended since the last call.
    pub fn take_consumed(&self) -> Vec<broca_protocol::SubmissionId> {
        std::mem::take(&mut self.lock().consumed)
    }

    /// Take every steered message and call `record` (which appends `StepStarted`) under the lock,
    /// unless a boundary cancellation is pending. A pending cancel and a new step cannot cross:
    /// whichever holds the lock first records first. Returning None means no StepStarted was
    /// written and no steer was consumed.
    ///
    /// This also decides every queued ordered send at this step boundary: an expired one ends (`SendEnded`), one whose first possible mode is `steer` is
    /// delivered (`SendDelivered`, its input taken after the steered messages), and the rest stay
    /// queued. `record` receives those records and must append them before `StepStarted`. It
    /// returns the runner tags the writer assigned to each record, in order (`None` for a record
    /// without parts), and each delivered message carries its record's tags, as a replay of the
    /// record attaches them.
    pub fn try_take_at_step_ordered(
        &self,
        run_id: &broca_protocol::RunId,
        now_ms: u64,
        record: impl FnOnce(
            Vec<broca_wal::WalRecord>,
        ) -> Vec<Option<Vec<broca_protocol::tags::MessageTags>>>,
    ) -> Option<Vec<Message>> {
        use crate::delivery_order::{choose_mode, ended, steered, Boundary};
        let mut state = self.lock();
        if self.boundary_cancel.is_cancelled() {
            return None;
        }
        let mut taken = std::mem::take(&mut state.pending);
        let mut records = Vec::new();
        // Each steer delivery's record position and the messages it delivers, in order.
        let mut delivered = Vec::new();
        let mut kept = Vec::new();
        for send in std::mem::take(&mut state.ordered) {
            if send.order.expired_at(now_ms) {
                records.push(ended(
                    &send.submission_id,
                    broca_wal::record::SendEndReason::Expired,
                ));
                state.consumed.push(send.submission_id);
                continue;
            }
            let has_text = crate::delivery_order::appended_text(&send.input).is_some();
            if choose_mode(&send.order, Boundary::STEP, has_text)
                == Some(broca_protocol::DeliveryMode::Steer)
            {
                records.push(steered(&send, run_id));
                delivered.push((records.len() - 1, send.input.clone()));
                state.consumed.push(send.submission_id);
            } else {
                kept.push(send);
            }
        }
        state.ordered = kept;
        let assigned = record(records);
        for (position, mut messages) in delivered {
            if let Some(Some(tags)) = assigned.get(position) {
                for (message, tags) in messages.iter_mut().zip(tags) {
                    crate::tags::attach(message, Some(tags));
                }
            }
            taken.extend(messages);
        }
        Some(taken)
    }

    /// Decide the queued ordered sends at the commit of a tool batch whose results (in planned
    /// order) are `results`: an expired one ends, one whose first possible mode is `tool_result`
    /// is delivered on the batch's carrier result, and the rest stay queued (a `steer` send waits
    /// for the step boundary that follows). `record` receives the records, which the caller
    /// appends between `ToolBatchFinished` and `TurnFinished`. Returns the text each delivery
    /// appends, by result, for the live prompt.
    pub fn deliver_with_tool_results<'a>(
        &self,
        run_id: &broca_protocol::RunId,
        now_ms: u64,
        results: impl IntoIterator<Item = &'a broca_protocol::content::ToolResult>,
        record: impl FnOnce(Vec<broca_wal::WalRecord>),
    ) -> Vec<(broca_protocol::ToolCallId, String)> {
        use crate::delivery_order::{
            appended_text, carrier, choose_mode, ended, with_tool_result, Boundary,
        };
        let mut state = self.lock();
        if state.closed || state.ordered.is_empty() {
            return Vec::new();
        }
        let carrier = carrier(results).cloned();
        let boundary = Boundary::tool_batch(carrier.is_some());
        let mut records = Vec::new();
        let mut appends = Vec::new();
        let mut kept = Vec::new();
        for send in std::mem::take(&mut state.ordered) {
            if send.order.expired_at(now_ms) {
                records.push(ended(
                    &send.submission_id,
                    broca_wal::record::SendEndReason::Expired,
                ));
                state.consumed.push(send.submission_id);
                continue;
            }
            let text = appended_text(&send.input);
            match (
                choose_mode(&send.order, boundary, text.is_some()),
                &carrier,
                text,
            ) {
                (Some(broca_protocol::DeliveryMode::ToolResult), Some(carrier), Some(text)) => {
                    records.push(with_tool_result(&send, run_id, carrier, text.clone()));
                    appends.push((carrier.clone(), text));
                    state.consumed.push(send.submission_id);
                }
                _ => kept.push(send),
            }
        }
        state.ordered = kept;
        if !records.is_empty() {
            record(records);
        }
        appends
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
    fn ordered(
        id: &str,
        modes: &[broca_protocol::DeliveryMode],
        key: Option<&str>,
    ) -> crate::delivery_order::OrderedSend {
        crate::delivery_order::OrderedSend {
            submission_id: broca_protocol::SubmissionId(id.into()),
            order: broca_protocol::DeliveryOrder {
                modes: modes.to_vec(),
                dedupe_key: key.map(str::to_owned),
                expires_at_ms: None,
                template: None,
            },
            principal: None,
            input: vec![message(id)],
        }
    }

    fn kinds(records: &[broca_wal::WalRecord]) -> Vec<String> {
        records
            .iter()
            .map(|record| match record {
                broca_wal::WalRecord::SendDelivered {
                    submission_id,
                    mode,
                    ..
                } => {
                    format!("delivered {} {}", submission_id.0, mode.as_str())
                }
                broca_wal::WalRecord::SendEnded {
                    submission_id,
                    reason,
                } => {
                    format!("ended {} {}", submission_id.0, reason.as_str())
                }
                other => format!("{other:?}"),
            })
            .collect()
    }

    /// Under the inbox lock, a newer send from the same sender with the same key replaces a
    /// queued one; once the turn delivered a send it is gone from the queue, so a later send
    /// under its key replaces nothing.
    #[test]
    fn dedupe_replaces_only_an_undelivered_send() {
        use broca_protocol::DeliveryMode::Steer;
        let inbox = SteerInbox::new();
        let run = broca_protocol::RunId("r".into());
        assert_eq!(
            inbox.accept_ordered(ordered("a", &[Steer], Some("k")), |old| assert!(
                old.is_none()
            )),
            OrderedAcceptance::Queued { superseded: None }
        );
        assert_eq!(
            inbox.accept_ordered(ordered("b", &[Steer], Some("k")), |_| {}),
            OrderedAcceptance::Queued {
                superseded: Some(broca_protocol::SubmissionId("a".into()))
            }
        );
        let mut written = Vec::new();
        let taken = inbox
            .try_take_at_step_ordered(&run, 0, |records| {
                written = records;
                Vec::new()
            })
            .unwrap();
        assert_eq!(kinds(&written), vec!["delivered b steer"]);
        assert_eq!(taken, vec![message("b")]);
        assert_eq!(
            inbox.accept_ordered(ordered("c", &[Steer], Some("k")), |old| assert!(
                old.is_none()
            )),
            OrderedAcceptance::Queued { superseded: None },
            "a delivered send is never replaced"
        );
        assert_eq!(
            inbox.take_consumed(),
            vec![broca_protocol::SubmissionId("b".into())]
        );
    }

    /// At a step boundary an expired send ends and a send whose list has no mode possible there
    /// stays queued; a closed inbox accepts nothing.
    #[test]
    fn a_step_boundary_expires_and_keeps_sends() {
        use broca_protocol::DeliveryMode::{IdleStart, ToolResult};
        let inbox = SteerInbox::new();
        let mut expiring = ordered("x", &[ToolResult], None);
        expiring.order.expires_at_ms = Some(10);
        inbox.seed_ordered(vec![expiring, ordered("w", &[ToolResult, IdleStart], None)]);
        let mut written = Vec::new();
        let taken = inbox
            .try_take_at_step_ordered(&broca_protocol::RunId("r".into()), 10, |records| {
                written = records;
                Vec::new()
            })
            .unwrap();
        assert!(taken.is_empty());
        assert_eq!(kinds(&written), vec!["ended x expired"]);
        inbox.close();
        assert_eq!(
            inbox.accept_ordered(ordered("y", &[ToolResult], None), |_| panic!("closed")),
            OrderedAcceptance::Closed
        );
    }
}
