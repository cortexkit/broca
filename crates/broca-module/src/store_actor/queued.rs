//! Sends that wait in the queue behind the latest run: writing their queued record, and
//! answering a retry of one still queued.

use super::*;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// An otherwise immediate send can be awaiting its input transforms when the drain starts.
    /// Keep the original request and the prepared input separate so it can still queue without
    /// repeating the transforms or losing its media.
    pub(super) async fn start_or_defer_send(
        &mut self,
        mut start: Start,
        delivery: broca_protocol::Delivery,
        mut reply: oneshot::Sender<SendOutcome>,
    ) {
        let outcome = async {
            self.freeze_plan(&mut start)?;
            let staged = self.stage_media(&mut start.request, None).await?;
            for item in &staged {
                self.held.hold(&item.reference, Arc::clone(&item.bytes));
            }
            let at_ms = start
                .prepared
                .retention
                .map(|_| (self.now_ms)())
                .or_else(broca_core::clock::epoch_millis);
            let prepared = self
                .transform_input(
                    start.request.input.clone(),
                    delivery,
                    start.manifest.as_deref(),
                    start.scope.as_ref(),
                    at_ms,
                    &mut reply,
                )
                .await
                .map_err(|outcome| {
                    SendOutcome::Error(format!("input preparation failed: {outcome:?}"))
                })?;
            Ok((staged, at_ms, prepared))
        }
        .await;
        let (staged, at_ms, prepared) = match outcome {
            Ok(value) => value,
            Err(outcome) => {
                self.held.clear();
                let _ = reply.send(outcome);
                return;
            }
        };
        if self.is_accepting_drain() {
            self.held.clear();
            let outcome = if let Some(provider) = prepared.refused.clone() {
                SendOutcome::PreUserUnavailable { provider }
            } else {
                self.queue_transformed(start, delivery, prepared, &staged)
                    .await
            };
            let _ = reply.send(outcome);
            return;
        }
        // A fresh run still starts if a transform refused its input, then ends with that
        // refusal before any model call.
        start.prepared.at_ms = at_ms;
        start.prepared.pre_model_error = prepared
            .refused
            .as_ref()
            .map(|provider| crate::actor::pre_user::refusal_error(provider));
        start.request.input = prepared.input;
        start.prepared.hooks = prepared.hooks;
        self.start_run(start, None, Some(reply), StoreRunMode::Fresh)
            .await;
    }

    /// The submission id of the first queued send that starts a run on an idle session: a plain
    /// send, or an ordered one whose list names `idle_start`. An ordered send whose order can't
    /// be read back is returned as the error, with why.
    pub(super) async fn first_to_drain(&self) -> Result<Option<String>, (String, String)> {
        // By position, without copying the queue: nothing here writes, so it doesn't change.
        for index in 0..self.session.operations().queue.len() {
            let pending = &self.session.operations().queue[index];
            let starts = !pending.ordered
                || self
                    .order_starts_runs(&pending.submission_id)
                    .await
                    .map_err(|error| (pending.submission_id.clone(), error))?;
            if starts {
                return Ok(Some(pending.submission_id.clone()));
            }
        }
        Ok(None)
    }

    /// Ends the queued send `submission_id`, whose record or order can't be read back into a
    /// run, with a retraction carrying `error`, then drains the next send. Left queued it would
    /// never run, and every later send would queue behind it, again at every open.
    pub(super) async fn end_unreadable_drain(
        &mut self,
        submission_id: String,
        error: String,
        waiting: &mut Option<(broca_protocol::SubmissionId, oneshot::Sender<SendOutcome>)>,
    ) {
        let submission = broca_protocol::SubmissionId(submission_id);
        let run_id = crate::actor::run_id_of_queued(&submission);
        let refusal = broca_core::RunError::Config(format!(
            "the queued send could not be read back to start its run: {error}"
        ));
        match self.end_refused_drain(&submission, &run_id, refusal).await {
            Ok(()) => Box::pin(self.drain_once(waiting)).await,
            // The send stays queued, and the queue waits for the next drain.
            Err(ended) => tracing::error!(
                target: "broca.store_actor",
                session = %self.key.session,
                submission_id = %submission.0,
                %error,
                %ended,
                "a queued send that could not be read back could not be ended",
            ),
        }
    }

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
        let first_deferred =
            self.is_accepting_drain() && self.session.operations().owner.needs_admission();
        let prepared = match self
            .transform_input(
                start.request.input.clone(),
                delivery,
                if first_deferred {
                    start.manifest.as_deref()
                } else {
                    manifest.as_deref()
                },
                if first_deferred {
                    start.scope.as_ref()
                } else {
                    None
                },
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
            scope,
            manifest,
            session_class,
            prepared: start_prepared,
            message_marks,
        } = start;
        let submission_id = broca_protocol::SubmissionId(format!("sub-{}", run_id.0));
        request.staged_uploads = None;
        // Unlike a send queued behind a run, a send accepted during the drain can be the
        // session's first. Keep its admission inputs and register it for boot recovery before
        // acknowledging the queue write, even if no run has ever registered this session.
        let first_deferred =
            self.is_accepting_drain() && self.session.operations().owner.needs_admission();
        let (scope, manifest) = if first_deferred {
            let manifest = match manifest
                .as_deref()
                .map(|manifest| queue::receipt_manifest(manifest, &mut self.parts))
                .transpose()
            {
                Ok(manifest) => manifest,
                Err(error) => return SendOutcome::Error(error),
            };
            (scope, manifest)
        } else {
            (None, None)
        };
        if self.is_accepting_drain() {
            if let Err(error) = self.track_recovery() {
                return SendOutcome::Transient(error);
            }
        }
        let receipt = queue::Receipt {
            session_class: session_class.filter(|_| self.frozen_session_class().is_none()),
            conversation_key,
            sent_by,
            principal: request.principal.clone(),
            request_identity_digest: identity,
            title,
            hooks: prepared.queued_hooks(),
            retention: start_prepared.retention,
            scope,
            manifest,
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
