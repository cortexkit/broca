//! Keyed retries of sends on a session in the session store: one that started a run is answered
//! from that run's records, and one that never started a run of its own (a steer, and a queued
//! send since withdrawn, ended undelivered, delivered into a running run, or refused when it
//! drained) from its own records, through the reads `send.status` serves
//! (`crate::store_status`).

use broca_protocol::{DeliveryOrder, RunId, SubmissionRequest};
use broca_session_loop::writer::Writer as _;
use broca_session_store::backend::SessionBackend;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::Record;

use super::{finished, with_turn_receipt, Keyed, StoreActor};
use crate::actor::{SendKey, SendOutcome};

/// What a keyed send's retry is compared on.
#[derive(Clone, Copy)]
pub(super) struct KeyedSend<'a> {
    /// The run id the send's key names.
    pub run_id: &'a RunId,
    pub key: &'a SendKey,
    pub request: &'a SubmissionRequest,
    pub delivery: broca_protocol::Delivery,
    pub conversation_key: Option<&'a str>,
    pub order: Option<&'a DeliveryOrder>,
    /// Whether a steer the session's owner sent under the same `send_id` answers for the key:
    /// not for a module the session's plan names as a sender.
    pub legacy_steers: bool,
}

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// A keyed send is looked up by the run id its key names: a run started under the key answers
    /// the retry from that run's records when the payload is the same, and is refused naming
    /// what differs when it is not. A send that never started, a steer, or one queued and since
    /// settled is answered from its records too (`unstarted_retry`). A key that has not been used
    /// starts a run.
    pub(super) async fn keyed(&mut self, send: KeyedSend<'_>) -> Result<Keyed, String> {
        let KeyedSend {
            run_id,
            key,
            request,
            delivery,
            conversation_key,
            order,
            legacy_steers: _,
        } = send;
        let digest = broca_core::send_identity_digest_with_conversation_key(
            request,
            delivery,
            conversation_key,
        );
        let session = &self.session;
        let find = |kind, id: String| async move {
            session
                .find_id(kind, &id)
                .await
                .map_err(|error| format!("{error:?}"))
        };
        let by_run = find(IdKind::RunId, run_id.0.clone()).await?;
        let submission = find(IdKind::SubmissionId, format!("sub-{}", run_id.0)).await?;
        let Some(start) = by_run.iter().find_map(|(_, record)| match record {
            Record::RunStart(start) => Some(start),
            _ => None,
        }) else {
            let mut holders = submission;
            if let Some(send_id) = key.send_id() {
                holders.extend(find(IdKind::SendId, send_id.to_owned()).await?);
            }
            // A key whose send is still queued is answered by the queue (`queue`).
            let queued_id = format!("sub-{}", run_id.0);
            let still_queued = !holders.is_empty()
                && holders.iter().all(|(_, record)| {
                    matches!(record, Record::QueuedSend(_) | Record::DeliveryOrdered(_))
                })
                && self
                    .session
                    .operations()
                    .queue
                    .iter()
                    .any(|pending| pending.submission_id == queued_id);
            if holders.is_empty() || still_queued {
                return Ok(Keyed::Fresh(digest));
            }
            return self.unstarted_retry(send, &holders, digest).await;
        };
        // The delivery order is part of a keyed send's identity: a run an ordered send started
        // is answered only to a retry asking for the same order, and a run a plain send started
        // only to a plain retry.
        let earlier = submission.iter().find_map(|(_, record)| match record {
            Record::DeliveryOrdered(ordered) => Some(&ordered.order),
            _ => None,
        });
        let same_order = match (earlier, order) {
            (None, None) => true,
            (Some(earlier), Some(retry)) => {
                serde_json::from_value::<broca_protocol::DeliveryOrder>(earlier.clone())
                    .map_err(|error| format!("the send's recorded order: {error}"))
                    .map(|earlier| crate::actor::same_order(retry, &earlier))?
            }
            _ => false,
        };
        if !same_order {
            return Ok(Keyed::Answered(SendOutcome::SendIdReuse {
                differing: "delivery",
            }));
        }
        let Some(stored) = start.request_identity_digest.as_deref() else {
            // Nothing can prove the payload is the same send, so the retry neither runs again nor
            // passes as the same one.
            return Ok(Keyed::Answered(SendOutcome::SendIdReuse {
                differing: "request identity because the run's start recorded no digest",
            }));
        };
        if stored != digest {
            let differing = broca_core::admission::send_identity_difference_with_conversation_key(
                stored,
                request,
                delivery,
                conversation_key,
            )
            .unwrap_or("request identity");
            return Ok(Keyed::Answered(SendOutcome::SendIdReuse { differing }));
        }
        let outcome = match self
            .session
            .operations()
            .run
            .as_ref()
            .filter(|run| run.run_id == run_id.0)
        {
            Some(run) => match (run.finished, run.paused) {
                (Some(reason), _) => finished(run_id, reason),
                _ if self.run.is_some() => SendOutcome::Active {
                    run_id: run_id.clone(),
                    delivered: None,
                },
                // A run paused for sign-in is resumed by its retry; whether the
                // credential is ready now is the executor's to decide. Runner-owned pauses are
                // reported.
                (None, Some(broca_protocol::PauseReason::AuthRequired)) => {
                    return Ok(Keyed::Resume)
                }
                (
                    None,
                    Some(
                        reason @ (broca_protocol::PauseReason::Restart
                        | broca_protocol::PauseReason::AccountLimit
                        | broca_protocol::PauseReason::ToolHeld),
                    ),
                ) => SendOutcome::Paused {
                    run_id: run_id.clone(),
                    reason,
                    delivered: None,
                },
                // An unfinished run that opening the session did not recover is not driven here:
                // a retry is answered from the run's records, which read it as active.
                (None, None) => SendOutcome::Active {
                    run_id: run_id.clone(),
                    delivered: None,
                },
            },
            // An earlier run: a later run started only after this one ended.
            None => match by_run.iter().find_map(|(_, record)| match record {
                Record::RunEnd(end) => Some(end.reason),
                _ => None,
            }) {
                Some(reason) => finished(run_id, reason),
                None => SendOutcome::Error(format!(
                    "the run {} that a recorded send started has no end, yet a later run started",
                    run_id.0
                )),
            },
        };
        // An ordered send that started its own run was delivered as that run's turn, so its
        // retry carries the `turn` receipt naming the run, as a retry of one delivered into a
        // running run carries `step`. The send's order is in its records, so this holds after a
        // restart.
        let ordered = self
            .session
            .find_id(IdKind::SubmissionId, &format!("sub-{}", run_id.0))
            .await
            .map_err(|error| format!("{error:?}"))?
            .iter()
            .any(|(_, record)| matches!(record, Record::DeliveryOrdered(_)));
        let outcome = if ordered {
            with_turn_receipt(outcome, run_id)
        } else {
            outcome
        };
        Ok(Keyed::Answered(outcome))
    }

    /// A keyed send that did not start a run, whose key the session's records already hold
    /// (`holders`) and whose send no longer waits in the queue, is answered in this order:
    /// - a send whose earlier order differs (a plain send retried as an ordered one, the reverse,
    ///   or another list) is refused naming `delivery`;
    /// - a steer under the key is answered by the run it joined, once the retry's payload is the
    ///   steer's, and refused naming what differs otherwise;
    /// - any other send is answered with what became of it: retracted, ended undelivered,
    ///   delivered into a run, or ended by its refusal when it drained. Its payload isn't
    ///   compared.
    ///
    /// A key whose records name no such send starts a run.
    pub(super) async fn unstarted_retry(
        &mut self,
        send: KeyedSend<'_>,
        holders: &[(u64, Record)],
        digest: String,
    ) -> Result<Keyed, String> {
        let submission_id = format!("sub-{}", send.run_id.0);
        let queued = holders.iter().any(
            |(_, record)| matches!(record, Record::QueuedSend(queued) if queued.submission_id == submission_id),
        );
        let earlier = holders.iter().find_map(|(_, record)| match record {
            Record::DeliveryOrdered(ordered) if ordered.submission_id == submission_id => {
                Some(&ordered.order)
            }
            _ => None,
        });
        let same_order = match (earlier, send.order) {
            (None, None) => true,
            (Some(earlier), Some(retry)) => {
                let earlier = serde_json::from_value::<DeliveryOrder>(earlier.clone())
                    .map_err(|error| format!("the send's recorded order: {error}"))?;
                crate::actor::same_order(retry, &earlier)
            }
            _ => false,
        };
        if (queued || earlier.is_some()) && !same_order {
            return Ok(Keyed::Answered(SendOutcome::SendIdReuse {
                differing: "delivery",
            }));
        }
        let send_id = send.key.send_id().unwrap_or_default();
        if let Some(steer) = holders
            .iter()
            .filter(|_| send.legacy_steers && !send_id.is_empty())
            .find_map(|(_, record)| match record {
                Record::Steer(steer)
                    if receipt_str(
                        steer
                            .receipt
                            .as_ref()
                            .map(broca_protocol::json::Json::parse)
                            .as_ref(),
                        "send_id",
                    ) == Some(send_id) =>
                {
                    Some(steer)
                }
                _ => None,
            })
        {
            let receipt = steer
                .receipt
                .as_ref()
                .map(broca_protocol::json::Json::parse);
            let Some(stored) = receipt_str(receipt.as_ref(), "request_identity_digest") else {
                return Ok(Keyed::Answered(SendOutcome::SendIdReuse {
                    differing: "request identity because the steer record has no digest",
                }));
            };
            if stored != digest {
                return Ok(Keyed::Answered(SendOutcome::SendIdReuse {
                    differing:
                        broca_core::admission::send_identity_difference_with_conversation_key(
                            stored,
                            send.request,
                            send.delivery,
                            send.conversation_key,
                        )
                        .unwrap_or("request identity"),
                }));
            }
            return Ok(Keyed::Answered(
                self.recorded_outcome(send.run_id, send_id, true)
                    .await?
                    .unwrap_or_else(|| {
                        SendOutcome::Error("the recorded steer disappeared from the session".into())
                    }),
            ));
        }
        Ok(
            match self.recorded_outcome(send.run_id, send_id, false).await? {
                Some(outcome) => Keyed::Answered(outcome),
                None => Keyed::Fresh(digest),
            },
        )
    }

    /// What became of the send `send_id` whose own run id is `own_run`, as `send.status` reads
    /// it from the session's records (`store_status::Reader::recorded_send_outcome`). The
    /// records are made durable first, so a crash can't then take back a step the answer
    /// counted.
    async fn recorded_outcome(
        &mut self,
        own_run: &RunId,
        send_id: &str,
        legacy_steers: bool,
    ) -> Result<Option<SendOutcome>, String> {
        if self.session.unsynced() {
            self.session
                .sync()
                .await
                .map_err(|error| format!("{error:?}"))?;
        }
        let addr = broca_core::session_addr(&self.key);
        let reader = crate::store_status::Reader {
            labels: Default::default(),
            parts: &self.parts,
            live: true,
            settled: self
                .settled
                .as_deref()
                .map(|settled| (settled, addr.as_str())),
            session_name: &self.key.session,
            live_step: None,
        };
        reader
            .recorded_send_outcome(&self.session, own_run, send_id, legacy_steers)
            .await
            .map_err(|error| format!("{error:?}"))
    }
}

/// The string field `name` of a record's receipt.
fn receipt_str<'a>(receipt: Option<&'a serde_json::Value>, name: &str) -> Option<&'a str> {
    receipt?.get(name)?.as_str()
}
