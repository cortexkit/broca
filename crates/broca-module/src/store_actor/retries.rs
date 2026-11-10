//! Keyed retries of sends that never started a run of their own, on a session kept in the new
//! storage: a steer, and a queued send since withdrawn, ended undelivered, delivered into a
//! running run, or refused when it drained. Each is answered from the session's records as
//! today's actor answers it (`send_order_differs`, `answer_steer_retry` and
//! `send_fate::recorded_send_outcome` in `crate::actor`), by the reads `send.status` serves
//! (`crate::store_status`).

use broca_protocol::{DeliveryOrder, RunId, SubmissionRequest};
use broca_session_store::backend::SessionBackend;
use broca_session_store::record::Record;

use super::{Keyed, StoreActor};
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
    /// not for a module the session's plan names as a sender, as in today's actor.
    pub legacy_steers: bool,
}

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// A keyed send no run started under, whose key the session's records already hold
    /// (`holders`) and whose send no longer waits in the queue, answered as today's actor
    /// answers it, in its order:
    /// - a send whose earlier order differs (a plain send retried as an ordered one, the reverse,
    ///   or another list) is refused naming `delivery`;
    /// - a steer under the key is answered by the run it joined, once the retry's payload is the
    ///   steer's, and refused naming what differs otherwise;
    /// - any other send is answered with what became of it: retracted, ended undelivered,
    ///   delivered into a run, or ended by its refusal when it drained. Its payload isn't
    ///   compared, as today's actor doesn't compare it.
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
                    if receipt_str(steer.receipt.as_ref(), "send_id") == Some(send_id) =>
                {
                    Some(steer)
                }
                _ => None,
            })
        {
            let Some(stored) = receipt_str(steer.receipt.as_ref(), "request_identity_digest")
            else {
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
    /// records are made durable first, as today's actor syncs its log before it reads a
    /// receipt: a crash can't then take back a step the answer counted.
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
