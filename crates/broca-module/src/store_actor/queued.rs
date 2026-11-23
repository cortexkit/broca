//! Sends that wait in the queue behind the latest run: writing their queued record, and
//! answering a retry of one still queued.

use super::*;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// Queues `start` behind the latest run, whether running or paused: the queue entry is
    /// durable before the reply, which is `pending`. A retry of a send still queued is compared
    /// with its record. A steer is queued with its mode, which the queued record keeps; a plain
    /// send's record names none. A new send's input goes through the session's `pre_user`
    /// transforms first; the record keeps the request as sent and what the transforms did,
    /// with their final input, in its receipt. `reply` is the sender's; the transforms stop for
    /// it when the sender stops waiting.
    pub(super) async fn queue(
        &mut self,
        mut start: Start,
        delivery: broca_protocol::Delivery,
        reply: &mut oneshot::Sender<SendOutcome>,
    ) -> SendOutcome {
        if let Some(retry) = self.retry_of_queued_send(&start, delivery).await {
            return retry;
        }
        // A send's attachments are staged into its input and checked before its input
        // transforms; the queued record keeps them.
        let staged = match self.stage_media(&mut start.request, None).await {
            Ok(staged) => staged,
            Err(outcome) => return outcome,
        };
        let manifest = match self.frozen_manifest() {
            Ok(manifest) => manifest,
            Err(error) => return SendOutcome::Error(error),
        };
        let prepared = match self
            .transform_input(
                start.request.input.clone(),
                delivery,
                manifest.as_deref(),
                None,
                self.retention_timestamp()
                    .or_else(broca_core::clock::epoch_millis),
                reply,
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        if let Some(provider) = prepared.refused.clone() {
            return SendOutcome::PreUserUnavailable { provider };
        }
        self.queue_transformed(start, delivery, prepared, &staged)
            .await
    }

    /// The answer to a send that repeats one still queued under the same id, compared with its
    /// record; `None` for a new send.
    pub(super) async fn retry_of_queued_send(
        &self,
        start: &Start,
        delivery: broca_protocol::Delivery,
    ) -> Option<SendOutcome> {
        let submission_id = broca_protocol::SubmissionId(format!("sub-{}", start.run_id.0));
        let existing = match self.queued(&submission_id.0).await {
            Err(error) => return Some(SendOutcome::Error(error)),
            Ok(existing) => existing?,
        };
        // A plain retry of an ordered send asks for another delivery.
        match self.queued_order(&submission_id.0).await {
            Err(error) => return Some(SendOutcome::Error(error)),
            Ok(Some(_)) => {
                return Some(SendOutcome::SendIdReuse {
                    differing: "delivery",
                })
            }
            Ok(None) => {}
        }
        Some(retry_of_queued(
            &existing,
            start.request.clone(),
            start.message_marks.as_ref(),
            start.identity.as_deref(),
            start.conversation_key.as_deref(),
            delivery,
            &self.parts,
            submission_id,
        ))
    }

    /// Writes the queued record of `start`, whose input its transforms left as `prepared` and
    /// whose attachments are `staged`, with the storage policy it freezes in the same batch.
    pub(super) async fn queue_transformed(
        &mut self,
        start: Start,
        delivery: broca_protocol::Delivery,
        prepared: crate::actor::pre_user::PreparedInput,
        staged: &[crate::person_media::StagedMedia],
    ) -> SendOutcome {
        let Start {
            run_id,
            one_shot,
            mut request,
            title,
            send_id,
            identity,
            conversation_key,
            sent_by,
            scope: _,
            // A queued send keeps its request as sent; the plan is frozen into it when it
            // drains, at the episode's start.
            manifest: _,
            session_class,
            prepared: start_prepared,
            message_marks,
        } = start;
        let submission_id = broca_protocol::SubmissionId(format!("sub-{}", run_id.0));
        request.staged_uploads = None;
        let receipt = queue::Receipt {
            session_class: session_class.filter(|_| self.frozen_session_class().is_none()),
            conversation_key,
            sent_by,
            principal: request.principal.clone(),
            request_identity_digest: identity,
            title,
            hooks: prepared.queued_hooks(),
            retention: start_prepared.retention,
            ..queue::Receipt::default()
        };
        let at = queue::Queued {
            written_at_ms: prepared.at_ms,
            ts_ms: self.retention_timestamp(),
            delivery: (delivery != broca_protocol::Delivery::Queue).then_some(delivery),
        };
        let record = match queue::queued_send(
            &submission_id.0,
            send_id.as_deref(),
            &request,
            &receipt,
            at,
            message_marks.as_ref(),
            &mut self.parts,
        ) {
            Ok(send) => Record::QueuedSend(send),
            Err(error) => return SendOutcome::Error(error),
        };
        let batch = self.with_retention_set(record, start_prepared.retention_policy);
        if let Err(error) = self.append_own_batch_with_media(&batch, staged).await {
            return SendOutcome::Error(format!("{error:?}"));
        }
        if one_shot {
            self.one_shot_submissions.insert(submission_id.clone());
        }
        SendOutcome::Pending { submission_id }
    }
}
