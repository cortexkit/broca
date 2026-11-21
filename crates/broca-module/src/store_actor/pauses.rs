//! Sends that reach a session kept in the session store while its latest run is paused: an
//! interrupt ends the paused run and starts after it, while a steer into a run paused for a
//! restart waits for that run to resume.

use broca_protocol::{Delivery, PauseReason, RunId};
use broca_session_loop::queue;
use broca_session_store::backend::SessionBackend;
use broca_session_store::record::Record;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{frozen_media, unserved, Start, StoreActor};
use crate::actor::SendOutcome;
use crate::executor::StoreRunMode;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// An interrupt on a session whose latest run is paused for anything but held calls (calls
    /// whose provider answers later) is queued first, ahead of every plain send, and durably before
    /// the `pending` reply. The paused run then ends `Cancelled`, and its end drains the interrupt.
    /// If the process stops in between, the interrupt remains queued behind the paused run; the
    /// session's next open ends the paused run (`recover`).
    pub(super) async fn interrupt_paused(
        &mut self,
        start: Start,
        reply: &mut oneshot::Sender<SendOutcome>,
    ) -> SendOutcome {
        let outcome = self.queue(start, Delivery::Interrupt, reply).await;
        if matches!(outcome, SendOutcome::Pending { .. }) {
            self.end_paused().await;
        }
        outcome
    }

    /// Whether the queue's first send is an interrupt while the session's latest run is paused
    /// for anything but held calls: that run has to end before the interrupt runs.
    /// Interrupts drain ahead of every other send, so one waiting is first in the queue.
    pub(super) fn interrupt_waits_on_pause(&self) -> bool {
        super::pause_interrupt_waits(&self.session.operations)
    }

    /// Ends the session's paused run `Cancelled`: the run is resumed with cancellation already
    /// tripped, and the run loop checks this before any other resume work, so the run's end is
    /// all it writes and no model is called. The end drains the queue (`finish_run`). A run
    /// that can't be read back stays paused, and the session's next open tries again.
    pub(super) async fn end_paused(&mut self) {
        let Some(run) = self
            .session
            .operations
            .run
            .as_ref()
            .filter(|run| run.finished.is_none() && run.paused.is_some())
        else {
            return;
        };
        match self.recovery_start(run) {
            Ok(start) => {
                let cancel = CancellationToken::new();
                cancel.cancel();
                self.start_run_under(start, None, None, StoreRunMode::Resume, cancel)
                    .await;
            }
            Err(error) => tracing::error!(
                target: "broca.store_actor",
                session = %self.key.session,
                run_id = %run.run_id,
                error = %error,
                "an interrupt could not end the paused run yet: its start could not be read back",
            ),
        }
    }

    /// A steer into the session's run `paused` for a restart: its attachments are checked against
    /// the media the run's start froze, its input goes through the session's `pre_user`
    /// transforms, and its record is written durably before the reply, which names the paused
    /// run. The run places its messages at its resume's first boundary. The steer's settings
    /// aren't compared with the run's: the run resumes with the settings it froze.
    ///
    /// A run on the session store takes no input-token budget, so a steer naming one is refused
    /// by name.
    pub(super) async fn steer_restart_paused(
        &mut self,
        mut start: Start,
        paused: RunId,
        reply: &mut oneshot::Sender<SendOutcome>,
    ) -> SendOutcome {
        if start
            .request
            .budget
            .as_ref()
            .is_some_and(|budget| budget.max_input_tokens.is_some())
        {
            return SendOutcome::Error(unserved(
                "a steer naming an input-token cap into a run paused for a restart",
            ));
        }
        let frozen = match self.latest_admission() {
            Ok(Some(frozen)) => frozen,
            Ok(None) => return SendOutcome::Error("the paused run has no start yet".into()),
            Err(error) => return SendOutcome::Error(error),
        };
        let staged = match self
            .stage_media(&mut start.request, Some(frozen_media(&frozen)))
            .await
        {
            Ok(staged) => staged,
            Err(outcome) => return outcome,
        };
        // The paused run already has its system prompt; a steer adds conversation only.
        let input = start
            .request
            .input
            .iter()
            .filter(|message| message.role != broca_protocol::content::Role::System)
            .cloned()
            .collect();
        let manifest = match self.frozen_manifest() {
            Ok(manifest) => manifest,
            Err(error) => return SendOutcome::Error(error),
        };
        let prepared = match self
            .transform_input(
                input,
                Delivery::Steer,
                manifest.as_deref(),
                None,
                broca_core::clock::epoch_millis(),
                reply,
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        if let Some(provider) = prepared.refused {
            return SendOutcome::PreUserUnavailable { provider };
        }
        let receipt = queue::SteerReceipt {
            ts_ms: self.retention_timestamp(),
            sent_by: start.sent_by,
            send_id: start.send_id,
            request_identity_digest: start.identity,
            title: start.title,
            delivery: Some(Delivery::Steer),
            hooks: prepared.hooks,
            ..queue::SteerReceipt::default()
        };
        let record = match queue::steer(
            prepared.input,
            prepared.at_ms,
            &receipt,
            start.message_marks.as_ref(),
        ) {
            Ok(steer) => Record::Steer(steer),
            Err(error) => return SendOutcome::Error(error),
        };
        if let Err(error) = self.append_own_with_media(record, &staged).await {
            return SendOutcome::Error(format!("{error:?}"));
        }
        SendOutcome::Paused {
            run_id: paused,
            reason: PauseReason::Restart,
            delivered: None,
        }
    }
}
