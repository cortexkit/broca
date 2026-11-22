//! Run events (`event.broca.run.*.v1`) of the sessions kept in the session store.
//!
//! Prefrontal, Magic Context and Colliculus learn that a run started, paused, consumed a steer or
//! ended from these events, and fall back to polling while none arrive. Each event is derived
//! from one of the session's records once a barrier has made that record durable, so no event
//! ever names a record a crash could take back. Each event carries a body, a subject and a
//! publication id:
//!
//! - `seq` is the record's seq, and `epoch` the session's incarnation in hex, the
//!   `incarnation` reads serve. Readers dedupe on `(epoch, run_id, seq)`.
//! - `at_ms` is the record's own timestamp, or the wall clock when the record has none.
//! - A steer accepted into a running run is reported when the step that places it starts
//!   (`run.steer_consumed`).
//! - A run seeded by an import or a fork never calls a provider and nobody waits on it, so it
//!   has no events.
//!
//! The projector runs on the session's writer as an append observer, beside the subscribe
//! stream's. It hands each durable batch's events to a [`RunEventSink`], which records them in
//! the outbox the publisher drains (see `broca_module_serve::bus_events`).
//!
//! The same projection names the end of each run routing admitted ([`RoutedRunEnd`]), so the
//! `run_finished` report routing is owed is stored with the end itself. The run's own reporter
//! still sends it at once; the stored copy is what reaches routing when that report can't: the
//! process stopped before routing answered (a daemon cut takes routing and the bus down
//! together), or the run ended while paused, where no reporting task runs.

use std::sync::Arc;

use broca_protocol::run_events::{RunEvent, RunEventDetail, StructuredOutputStatus};
use broca_protocol::{BindIdentity, RunFinishReason};
use broca_session_store::backend::SessionBackend;
use broca_session_store::record::Record;
use broca_session_store::session::{Appended, Session};

/// Where the run events of a store session go once their records are durable.
pub trait RunEventSink: Send + Sync {
    /// The events of records a barrier just made durable, in seq order, and `through`, the seq
    /// of the last record that barrier made durable. Called on the session's writer, with the
    /// session's appends held, so it returns quickly and never waits on the bus.
    fn durable(&self, session: &BindIdentity, epoch: &str, through: u64, events: Vec<RunEvent>);

    /// The ends of runs routing admitted that the same barrier made durable, called on the
    /// writer like [`Self::durable`]. A sink with no routing to report to ignores them.
    fn routed_ends(&self, _session: &BindIdentity, _ends: Vec<RoutedRunEnd>) {}
}

/// The durable end of a run routing admitted: what its `run_finished` report names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedRunEnd {
    /// The report's event id: the session id and the end record's seq, `<session id>:<seq>`,
    /// as the run's own reporter names the same report.
    pub event_id: String,
    pub admission_id: String,
    pub run_id: String,
    pub reason: RunFinishReason,
}

/// The admission routing granted the run started with `selection` (its run start's auth
/// selection), when routing chose its account. Other decision paths hold no admission.
fn routed_admission(selection: Option<&serde_json::Value>) -> Option<String> {
    let decision = selection?.get("account_decision")?;
    if decision.get("path")?.as_str()? != broca_records::record::ACCOUNT_DECISION_ROUTED {
        return None;
    }
    decision.get("admission_id")?.as_str().map(str::to_owned)
}

/// Projects `session`'s appends into run events for `sink`, from its next append on.
pub fn observe<B: SessionBackend>(
    session: &mut Session<B>,
    key: &BindIdentity,
    sink: Arc<dyn RunEventSink>,
) {
    let mut projector = Projector::new(
        key.clone(),
        hex(&session.head().incarnation.0),
        sink,
        wall_now_ms,
    );
    projector.session_id = hex(&session.head().session_id.0);
    // A run open when the session opens (paused, or resumed after a restart) started before
    // this projector saw anything, so its admission comes from the open run the session rebuilt
    // from its records when it loaded.
    if let Some(run) = session
        .operations
        .run
        .as_ref()
        .filter(|run| run.finished.is_none())
    {
        projector.admission = run
            .start
            .as_ref()
            .and_then(|start| routed_admission(start.auth_selection.as_ref()))
            .map(|admission| (run.run_id.clone(), admission));
    }
    session.add_append_observer(Box::new(move |appended| projector.appended(appended)));
}

fn wall_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The steers accepted into the open run since its last step started.
#[derive(Default)]
struct PendingSteers {
    count: u64,
    send_ids: Vec<String>,
    /// Whether a send id was left out to keep ids within the 512-byte prefix.
    truncated: bool,
    last_at_ms: Option<u64>,
}

/// The longest run of send ids a `run.steer_consumed` starts with: once the next id can't fit,
/// later ids can't join without reordering, so they are left out and the event says so.
const SEND_IDS_BYTES: usize = 512;

struct Projector {
    session: BindIdentity,
    session_ref: String,
    epoch: String,
    sink: Arc<dyn RunEventSink>,
    now_ms: fn() -> u64,
    /// Events of records appended without a barrier, released when a barrier makes them durable.
    unsynced: Vec<RunEvent>,
    /// The seq of the last record appended.
    last: u64,
    /// The run whose records follow, and whether it was seeded (it has no events).
    run: Option<(String, bool)>,
    steers: PendingSteers,
    structured: Option<StructuredOutputStatus>,
    /// The session's id, which names a routed run's end report (see [`RoutedRunEnd`]).
    session_id: String,
    /// The open run and the admission routing granted it, when routing chose its account.
    admission: Option<(String, String)>,
    /// Ends of routed runs appended without a barrier, released with the events.
    unsynced_ends: Vec<RoutedRunEnd>,
}

impl Projector {
    fn new(
        session: BindIdentity,
        epoch: String,
        sink: Arc<dyn RunEventSink>,
        now_ms: fn() -> u64,
    ) -> Self {
        Self {
            session_ref: broca_core::session_refs::agent_session_ref(&session),
            session,
            epoch,
            sink,
            now_ms,
            unsynced: Vec::new(),
            last: 0,
            run: None,
            steers: PendingSteers::default(),
            structured: None,
            session_id: String::new(),
            admission: None,
            unsynced_ends: Vec::new(),
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
                    let seq = first + offset as u64;
                    self.project(seq, record);
                    self.last = seq;
                }
                if durable {
                    self.release();
                }
            }
            Appended::Synced => self.release(),
            // A revert took back records told before; their events, if not yet released, go
            // with them, and the run state they built is no longer known.
            Appended::Replaced => {
                self.unsynced.clear();
                self.unsynced_ends.clear();
                self.admission = None;
                self.run = None;
                self.steers = PendingSteers::default();
                self.structured = None;
            }
        }
    }

    fn release(&mut self) {
        let events = std::mem::take(&mut self.unsynced);
        self.sink
            .durable(&self.session, &self.epoch, self.last, events);
        if !self.unsynced_ends.is_empty() {
            let ends = std::mem::take(&mut self.unsynced_ends);
            self.sink.routed_ends(&self.session, ends);
        }
    }

    fn project(&mut self, seq: u64, record: &Record) {
        let detail = match record {
            Record::RunStart(start) => {
                let seeded = start.seeded == Some(true);
                self.run = Some((start.run_id.clone(), seeded));
                self.admission = routed_admission(start.auth_selection.as_ref())
                    .map(|admission| (start.run_id.clone(), admission));
                self.steers = PendingSteers::default();
                self.structured = None;
                Some((
                    start.run_id.as_str(),
                    start.ts_ms.or(start.written_at_ms),
                    RunEventDetail::Started {},
                ))
            }
            Record::Steer(steer) => {
                let pending = &mut self.steers;
                pending.count += 1;
                pending.last_at_ms = steer.written_at_ms.or(pending.last_at_ms);
                let send_id = steer.receipt.as_ref().and_then(|receipt| {
                    receipt
                        .parse()
                        .get("send_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                });
                if let Some(id) = send_id {
                    let held: usize = pending.send_ids.iter().map(String::len).sum();
                    if !pending.truncated && held + id.len() <= SEND_IDS_BYTES {
                        pending.send_ids.push(id);
                    } else {
                        pending.truncated = true;
                    }
                }
                None
            }
            Record::StepStart(start) if self.steers.count > 0 => {
                let pending = std::mem::take(&mut self.steers);
                Some((
                    start.run_id.as_str(),
                    pending.last_at_ms,
                    RunEventDetail::SteerConsumed {
                        step: start.step_id,
                        consumed: pending.count,
                        send_ids: pending.send_ids,
                        truncated: pending.truncated,
                    },
                ))
            }
            Record::StructuredResult(result) => {
                self.structured = Some(
                    match result.result.get("validation").and_then(|v| v.as_str()) {
                        Some("passed") => StructuredOutputStatus::Passed,
                        Some("failed") => StructuredOutputStatus::Failed,
                        _ => StructuredOutputStatus::Absent,
                    },
                );
                None
            }
            Record::Pause(pause) => Some((
                pause.run_id.as_str(),
                pause.ts_ms,
                RunEventDetail::Paused {
                    pause_reason: pause.reason,
                },
            )),
            Record::RunEnd(end) => {
                if let Some((_, admission_id)) = self
                    .admission
                    .take()
                    .filter(|(run_id, _)| *run_id == end.run_id)
                {
                    self.unsynced_ends.push(RoutedRunEnd {
                        event_id: format!("{}:{seq}", self.session_id),
                        admission_id,
                        run_id: end.run_id.clone(),
                        reason: end.reason,
                    });
                }
                let error_class = end.error.as_ref().and_then(|error| {
                    serde_json::from_value::<broca_protocol::ProviderError>(error.parse())
                        .map(|error| error.class.shared_class())
                        .ok()
                });
                Some((
                    end.run_id.as_str(),
                    end.ts_ms,
                    RunEventDetail::Ended {
                        terminal: end.reason,
                        error_class,
                        structured_output: Some(
                            self.structured
                                .take()
                                .unwrap_or(StructuredOutputStatus::Absent),
                        ),
                    },
                ))
            }
            _ => None,
        };
        let Some((run_id, at_ms, detail)) = detail else {
            return;
        };
        let seeded = self
            .run
            .as_ref()
            .is_some_and(|(open, seeded)| open == run_id && *seeded);
        let event = RunEvent {
            v: 1,
            session_ref: self.session_ref.clone(),
            run_id: run_id.to_owned(),
            seq,
            at_ms: at_ms.unwrap_or_else(|| (self.now_ms)()),
            detail,
        };
        if matches!(record, Record::RunEnd(_)) {
            self.run = None;
        }
        if !seeded {
            self.unsynced.push(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use broca_protocol::{ErrorClass, PauseReason, RunFinishReason};
    use broca_session_store::record::{Pause, RunEnd, StructuredResult};

    use super::*;

    #[derive(Default)]
    struct Recorded(Mutex<Vec<(u64, Vec<RunEvent>)>>);

    impl RunEventSink for Recorded {
        fn durable(&self, _: &BindIdentity, _: &str, through: u64, events: Vec<RunEvent>) {
            self.0.lock().unwrap().push((through, events));
        }
    }

    fn projector(sink: Arc<Recorded>) -> Projector {
        Projector::new(
            BindIdentity::new("/project", "broca", "s1"),
            "00".repeat(16),
            sink,
            || 42,
        )
    }

    fn pause(ts_ms: Option<u64>) -> Record {
        Record::Pause(Pause {
            run_id: "r1".into(),
            reason: PauseReason::AuthRequired,
            ts_ms,
            error: None,
            account_limit: None,
        })
    }

    fn failed_end() -> Record {
        let error = serde_json::json!({"class": "auth_required", "message": "signed out"});
        Record::RunEnd(RunEnd {
            run_id: "r1".into(),
            reason: RunFinishReason::Error,
            usage: None,
            ts_ms: Some(7),
            retries_used: 0,
            interrupt_reason: None,
            error: Some(broca_protocol::json::Json::from_value(&error)),
        })
    }

    /// Records appended without a barrier hand nothing; the barrier that makes them durable
    /// hands their events. A pause is `run.paused`, and a failed end carries its error class and
    /// the structured result's validation.
    #[test]
    fn events_wait_for_the_barrier_and_carry_what_the_records_say() {
        let sink = Arc::new(Recorded::default());
        let mut projector = projector(Arc::clone(&sink));
        let records = [
            pause(None),
            Record::StructuredResult(StructuredResult {
                run_id: "r1".into(),
                result: serde_json::json!({"validation": "failed"}),
            }),
            failed_end(),
        ];
        projector.appended(Appended::Records {
            first: 10,
            records: &records,
            durable: false,
        });
        assert!(sink.0.lock().unwrap().is_empty(), "nothing is durable yet");
        projector.appended(Appended::Synced);
        let handed = std::mem::take(&mut *sink.0.lock().unwrap());
        assert_eq!(handed.len(), 1);
        let (through, events) = &handed[0];
        assert_eq!(*through, 12);
        let summary: Vec<_> = events
            .iter()
            .map(|event| (event.seq, event.at_ms, event.detail.clone()))
            .collect();
        assert_eq!(
            summary,
            [
                (
                    10,
                    42,
                    RunEventDetail::Paused {
                        pause_reason: PauseReason::AuthRequired
                    }
                ),
                (
                    12,
                    7,
                    RunEventDetail::Ended {
                        terminal: RunFinishReason::Error,
                        error_class: Some(ErrorClass::Auth),
                        structured_output: Some(StructuredOutputStatus::Failed),
                    }
                ),
            ]
        );
    }

    /// A revert takes back records appended without a barrier, and their events with them.
    #[test]
    fn a_revert_drops_events_not_yet_durable() {
        let sink = Arc::new(Recorded::default());
        let mut projector = projector(Arc::clone(&sink));
        projector.appended(Appended::Records {
            first: 3,
            records: &[pause(Some(5))],
            durable: false,
        });
        projector.appended(Appended::Replaced);
        projector.appended(Appended::Synced);
        let handed = std::mem::take(&mut *sink.0.lock().unwrap());
        assert!(handed.iter().all(|(_, events)| events.is_empty()));
    }
}
