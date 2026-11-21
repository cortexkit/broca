//! Send state notices (`event.broca.send_state.v1.{sender}`) of the sessions kept in the new
//! storage: each change of a keyed send's state, for the module that sent it.
//!
//! Occipital follows the sends its apps queue and AFT the notices it sends; both re-read
//! `session.send_status` when a notice arrives instead of polling every send they wait on.
//! Like run events, each notice is derived from one of the session's records once a barrier has
//! made that record durable, so no notice names a change a crash could take back.
//!
//! Which records change a send, and the state each moves it to:
//! - `QueuedSend`: queued, `pending`;
//! - `RunStart` naming the send, or a `Delivery` of it into a running run: `active`;
//! - `SendEnded`: `superseded` or `expired` (`not_delivered` for a reason this build can't read);
//! - `Retraction`: `retracted`, or `finished` when admitting the send failed;
//! - the `Pause` and `RunEnd` of the run a send started or was delivered into: `paused`,
//!   `finished`.
//!
//! Only a send that a module made (a reserved principal: a module the daemon launched, whose
//! module id ends the subject) and that carries the caller's own key (`send_id`) has notices: an
//! unkeyed send can't be re-read, and only a module has a durable bus consumer (one that keeps
//! its place across restarts) to read them. A seeded run (from an import or a fork) has none, as
//! it has no run events.

use std::collections::HashMap;
use std::sync::Arc;

use broca_protocol::send_states::SendStateNotice;
use broca_protocol::{BindIdentity, BindPrincipal};
use broca_session_store::backend::SessionBackend;
use broca_session_store::record::Record;
use broca_session_store::session::{Appended, Session};

/// One notice and the module it is for.
#[derive(Debug, Clone, PartialEq)]
pub struct SendStateChange {
    pub sender: String,
    pub notice: SendStateNotice,
}

/// Where the send state notices of a store session go once their records are durable.
pub trait SendStateSink: Send + Sync {
    /// The notices of records a barrier just made durable, in seq order. Called on the
    /// session's writer, with the session's appends held, so it returns quickly and never waits
    /// on the bus.
    fn durable(&self, session: &BindIdentity, changes: Vec<SendStateChange>);
}

/// A keyed send a module sent: its key and the module.
#[derive(Debug, Clone, PartialEq)]
struct Sent {
    send_id: String,
    sender: String,
}

/// The most sends one run carries notices for: the send that started it and the sends
/// delivered into it. Past it, later deliveries still have their `active` notice, but not the
/// run's pause and end, which their readers then learn on their next read.
const SENDS_PER_RUN: usize = 64;

/// Projects `session`'s appends into send state notices for `sink`, from its next append on.
/// The sends already queued and the run already open are remembered first, so a change after
/// the session reopens still names its send.
pub fn observe<B: SessionBackend>(
    session: &mut Session<B>,
    key: &BindIdentity,
    sink: Arc<dyn SendStateSink>,
) {
    let mut projector = Projector::new(key.clone(), sink);
    let operations = &session.operations;
    for pending in &operations.queue {
        if let (Some(send_id), Some(sender)) = (&pending.send_id, &pending.sender) {
            projector.queued.insert(
                pending.submission_id.clone(),
                Sent {
                    send_id: send_id.clone(),
                    sender: sender.clone(),
                },
            );
        }
    }
    if let Some(run) = operations.run.as_ref().filter(|run| run.finished.is_none()) {
        if let Some(start) = run
            .start
            .as_ref()
            .filter(|start| start.seeded != Some(true))
        {
            if let Some(sent) = run_start_send(start) {
                projector.runs.insert(run.run_id.clone(), vec![sent]);
            }
        }
    }
    session.add_append_observer(Box::new(move |appended| projector.appended(appended)));
}

/// The keyed send a run start names and the module that sent it, if both are known.
fn run_start_send(start: &broca_session_store::record::RunStart) -> Option<Sent> {
    let sender = start
        .run_choices
        .as_ref()
        .and_then(|choices| choices.pointer("/record/sent_by"))
        .and_then(module_of)?;
    Some(Sent {
        send_id: start.send_id.clone()?,
        sender,
    })
}

/// The module id of a recorded principal, when it names a module.
fn module_of(principal: &serde_json::Value) -> Option<String> {
    match serde_json::from_value(principal.clone()).ok()? {
        BindPrincipal::Reserved { module_id } => Some(module_id),
        _ => None,
    }
}

struct Projector {
    session: BindIdentity,
    session_ref: String,
    sink: Arc<dyn SendStateSink>,
    /// Notices of records appended without a barrier, released when a barrier makes them
    /// durable.
    unsynced: Vec<SendStateChange>,
    /// Queued keyed sends not yet started, delivered or ended, by submission id.
    queued: HashMap<String, Sent>,
    /// The keyed sends each open run started with or took delivery of, by run id.
    runs: HashMap<String, Vec<Sent>>,
}

impl Projector {
    fn new(session: BindIdentity, sink: Arc<dyn SendStateSink>) -> Self {
        Self {
            session_ref: broca_core::session_refs::agent_session_ref(&session),
            session,
            sink,
            unsynced: Vec::new(),
            queued: HashMap::new(),
            runs: HashMap::new(),
        }
    }

    fn appended(&mut self, appended: Appended<'_>) {
        match appended {
            Appended::Records {
                first,
                records,
                durable,
            } => {
                for (offset, record) in records.iter().enumerate() {
                    self.project(first + offset as u64, record);
                }
                if durable {
                    self.release();
                }
            }
            Appended::Synced => self.release(),
            // A revert took back records told before. Their notices go with them if not yet
            // released; the sends and runs they named are re-read by readers on their next
            // notice or read, so nothing remembered from them is needed.
            Appended::Replaced => {
                self.unsynced.clear();
                self.queued.clear();
                self.runs.clear();
            }
        }
    }

    fn release(&mut self) {
        if self.unsynced.is_empty() {
            return;
        }
        let changes = std::mem::take(&mut self.unsynced);
        self.sink.durable(&self.session, changes);
    }

    fn notice(&mut self, sent: &Sent, seq: u64, state: &'static str) {
        self.unsynced.push(SendStateChange {
            sender: sent.sender.clone(),
            notice: SendStateNotice {
                v: 1,
                session_ref: self.session_ref.clone(),
                send_id: sent.send_id.clone(),
                seq,
                state,
            },
        });
    }

    fn joins_run(&mut self, run_id: &str, sent: Sent) {
        let sends = self.runs.entry(run_id.to_owned()).or_default();
        if sends.len() < SENDS_PER_RUN {
            sends.push(sent);
        }
    }

    fn project(&mut self, seq: u64, record: &Record) {
        match record {
            Record::QueuedSend(send) => {
                let sender = send
                    .receipt
                    .as_ref()
                    .map(|receipt| receipt.parse())
                    .and_then(|receipt| {
                        receipt
                            .get("sent_by")
                            .or(receipt.get("principal"))
                            .and_then(module_of)
                    });
                if let (Some(send_id), Some(sender)) = (send.send_id.clone(), sender) {
                    let sent = Sent { send_id, sender };
                    self.notice(&sent, seq, "pending");
                    self.queued.insert(send.submission_id.clone(), sent);
                }
            }
            Record::RunStart(start) => {
                if start.seeded == Some(true) {
                    return;
                }
                let queued = start
                    .submission_id
                    .as_ref()
                    .and_then(|submission| self.queued.remove(submission));
                if let Some(sent) = queued.or_else(|| run_start_send(start)) {
                    self.notice(&sent, seq, "active");
                    self.joins_run(&start.run_id, sent);
                }
            }
            Record::Delivery(delivery) => {
                if let Some(sent) = self.queued.remove(&delivery.submission_id) {
                    self.notice(&sent, seq, "active");
                    self.joins_run(&delivery.run_id, sent);
                }
            }
            Record::SendEnded(ended) => {
                if let Some(sent) = self.queued.remove(&ended.submission_id) {
                    let reason = ended
                        .reason
                        .clone()
                        .and_then(|reason| {
                            serde_json::from_value::<broca_wal::record::SendEndReason>(reason).ok()
                        })
                        .map_or("not_delivered", |reason| reason.as_str());
                    self.notice(&sent, seq, reason);
                }
            }
            Record::Retraction(retraction) => {
                if let Some(sent) = self.queued.remove(&retraction.submission_id) {
                    let state = if retraction.failure.is_some() {
                        "finished"
                    } else {
                        "retracted"
                    };
                    self.notice(&sent, seq, state);
                }
            }
            Record::Pause(pause) => {
                for sent in self.runs.get(&pause.run_id).cloned().unwrap_or_default() {
                    self.notice(&sent, seq, "paused");
                }
            }
            Record::RunEnd(end) => {
                for sent in self.runs.remove(&end.run_id).unwrap_or_default() {
                    self.notice(&sent, seq, "finished");
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use broca_protocol::{PauseReason, RunFinishReason};
    use broca_session_store::record::{
        Delivery, Pause, QueuedSend, Retraction, RunEnd, RunStart, SendEnded,
    };
    use serde_json::json;

    use super::*;

    #[derive(Default)]
    struct Recorded(Mutex<Vec<Vec<SendStateChange>>>);

    impl SendStateSink for Recorded {
        fn durable(&self, _: &BindIdentity, changes: Vec<SendStateChange>) {
            self.0.lock().unwrap().push(changes);
        }
    }

    fn projector(sink: Arc<Recorded>) -> Projector {
        Projector::new(BindIdentity::new("/project", "broca", "s1"), sink)
    }

    fn module(id: &str) -> serde_json::Value {
        json!({"kind": "reserved", "module_id": id})
    }

    fn queued(submission: &str, send_id: Option<&str>, sent_by: serde_json::Value) -> Record {
        Record::QueuedSend(QueuedSend {
            named_speaker: None,
            media: Vec::new(),
            submission_id: submission.into(),
            send_id: send_id.map(str::to_owned),
            input: Vec::new(),
            setup: Vec::new(),
            written_at_ms: None,
            ts_ms: None,
            request: None,
            receipt: Some(broca_protocol::json::Json::from_value(
                &json!({ "sent_by": sent_by }),
            )),
            delivery: None,
            message_marks: None,
        })
    }

    fn run_start(run: &str, submission: Option<&str>, send: Option<(&str, &str)>) -> Record {
        Record::RunStart(Box::new(RunStart {
            imported_ids: None,
            message_marks: None,
            named_speaker: None,
            run_id: run.into(),
            model: serde_json::from_value(json!({"provider_module_id": "p", "model_id": "m"}))
                .unwrap(),
            setup: Vec::new(),
            renderer_version: 1,
            submission_id: submission.map(str::to_owned),
            send_id: send.map(|(id, _)| id.to_owned()),
            request_identity_digest: None,
            ts_ms: Some(1),
            written_at_ms: Some(1),
            session_class: None,
            retention_secs: None,
            title: None,
            context_limit: None,
            service_tier: None,
            route_decision_id: None,
            compaction_plan_version: None,
            auth_selection: None,
            run_choices: send.map(|(_, sender)| json!({"record": {"sent_by": module(sender)}})),
            origin: None,
            principal: None,
            scope: None,
            hooks: None,
            keep_warm: None,
            on_restart: None,
            seeded: None,
            refusal_schedule: None,
        }))
    }

    fn delivered(submission: &str, run: &str) -> Record {
        Record::Delivery(Delivery {
            submission_id: submission.into(),
            run_id: run.into(),
            mode: "steer".into(),
            input: Vec::new(),
            tool_call_id: None,
            appended_text: None,
            written_at_ms: None,
            tags_v1: None,
            input_listed: None,
        })
    }

    fn pause(run: &str) -> Record {
        Record::Pause(Pause {
            run_id: run.into(),
            reason: PauseReason::AuthRequired,
            ts_ms: None,
            error: None,
            account_limit: None,
        })
    }

    fn end(run: &str) -> Record {
        Record::RunEnd(RunEnd {
            run_id: run.into(),
            reason: RunFinishReason::Completed,
            usage: None,
            ts_ms: None,
            retries_used: 0,
            interrupt_reason: None,
            error: None,
        })
    }

    /// Feeds `records` from seq 1, one durable batch each, and returns each notice as
    /// `(sender, send_id, seq, state)`.
    fn project(records: Vec<Record>) -> Vec<(String, String, u64, &'static str)> {
        let sink = Arc::new(Recorded::default());
        let mut projector = projector(Arc::clone(&sink));
        for (index, record) in records.into_iter().enumerate() {
            projector.appended(Appended::Records {
                first: index as u64 + 1,
                records: &[record],
                durable: true,
            });
        }
        let batches = std::mem::take(&mut *sink.0.lock().unwrap());
        batches
            .into_iter()
            .flatten()
            .map(|change| {
                (
                    change.sender,
                    change.notice.send_id,
                    change.notice.seq,
                    change.notice.state,
                )
            })
            .collect()
    }

    fn row(
        sender: &str,
        send: &str,
        seq: u64,
        state: &'static str,
    ) -> (String, String, u64, &'static str) {
        (sender.into(), send.into(), seq, state)
    }

    /// A queued send that starts its own run is pending, then active, then finished with the
    /// run; one delivered into that run is active from its delivery and follows the run's
    /// pause and end too. Each notice names the record's seq and the module that sent it.
    #[test]
    fn a_keyed_send_moves_from_pending_through_its_run() {
        let notices = project(vec![
            queued("sub-1", Some("send-1"), module("occipital")),
            run_start("run-1", Some("sub-1"), None),
            queued("sub-2", Some("steer-1"), module("aft")),
            delivered("sub-2", "run-1"),
            pause("run-1"),
            end("run-1"),
        ]);
        assert_eq!(
            notices,
            [
                row("occipital", "send-1", 1, "pending"),
                row("occipital", "send-1", 2, "active"),
                row("aft", "steer-1", 3, "pending"),
                row("aft", "steer-1", 4, "active"),
                row("occipital", "send-1", 5, "paused"),
                row("aft", "steer-1", 5, "paused"),
                row("occipital", "send-1", 6, "finished"),
                row("aft", "steer-1", 6, "finished"),
            ]
        );
    }

    /// A send that starts a run at once has no queued record: its run start names it and its
    /// sender. A replaced, expired, retracted or refused send ends with its own notice.
    #[test]
    fn a_direct_start_and_each_way_a_queued_send_ends_have_notices() {
        // The reasons as the queue writes them (`store_actor::ordered`).
        let superseded = serde_json::to_value(broca_wal::record::SendEndReason::Superseded {
            by: broca_protocol::SubmissionId("sub-9".into()),
        })
        .unwrap();
        let expired = serde_json::to_value(broca_wal::record::SendEndReason::Expired).unwrap();
        let ended = |submission: &str, reason: serde_json::Value| {
            Record::SendEnded(SendEnded {
                submission_id: submission.into(),
                reason: Some(reason),
            })
        };
        let notices = project(vec![
            run_start("run-1", Some("sub-0"), Some(("direct", "prefrontal-core"))),
            queued("sub-1", Some("a"), module("aft")),
            ended("sub-1", superseded),
            queued("sub-2", Some("b"), module("aft")),
            ended("sub-2", expired),
            queued("sub-3", Some("c"), module("aft")),
            Record::Retraction(Retraction {
                submission_id: "sub-3".into(),
                failure: None,
            }),
            queued("sub-4", Some("d"), module("aft")),
            Record::Retraction(Retraction {
                submission_id: "sub-4".into(),
                failure: Some(json!({"class": "permanent"})),
            }),
        ]);
        assert_eq!(
            notices,
            [
                row("prefrontal-core", "direct", 1, "active"),
                row("aft", "a", 2, "pending"),
                row("aft", "a", 3, "superseded"),
                row("aft", "b", 4, "pending"),
                row("aft", "b", 5, "expired"),
                row("aft", "c", 6, "pending"),
                row("aft", "c", 7, "retracted"),
                row("aft", "d", 8, "pending"),
                row("aft", "d", 9, "finished"),
            ]
        );
    }

    /// Only a send that a module made and that carries the caller's `send_id` has notices: an
    /// unkeyed send can't be re-read, and a person's direct route has no durable bus consumer to
    /// read them.
    #[test]
    fn unkeyed_sends_and_sends_from_no_module_have_none() {
        let notices = project(vec![
            queued("sub-1", None, module("aft")),
            run_start("run-1", Some("sub-1"), None),
            queued("sub-2", Some("x"), json!({"kind": "direct"})),
            delivered("sub-2", "run-1"),
            end("run-1"),
        ]);
        assert!(notices.is_empty(), "{notices:?}");
    }

    /// A notice is released only once a barrier makes its record durable, and a revert takes
    /// back the notices of records not yet durable.
    #[test]
    fn notices_wait_for_their_barrier_and_go_with_a_revert() {
        let sink = Arc::new(Recorded::default());
        let mut projector = projector(Arc::clone(&sink));
        projector.appended(Appended::Records {
            first: 1,
            records: &[queued("sub-1", Some("a"), module("aft"))],
            durable: false,
        });
        assert!(sink.0.lock().unwrap().is_empty());
        projector.appended(Appended::Synced);
        assert_eq!(sink.0.lock().unwrap().len(), 1);
        projector.appended(Appended::Records {
            first: 2,
            records: &[queued("sub-2", Some("b"), module("aft"))],
            durable: false,
        });
        projector.appended(Appended::Replaced);
        projector.appended(Appended::Synced);
        assert_eq!(
            sink.0.lock().unwrap().len(),
            1,
            "the reverted notice is never released"
        );
    }
}
