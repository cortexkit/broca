//! How the runs of this process's sessions ended or paused, for `diagnostics.findings`.
//!
//! The findings report runs that ended in an error, runs interrupted other than by a restart,
//! and runs still paused for sign-in or an account limit. Each session reports its own runs
//! here from its records once a barrier has made them durable, the way it reports run events
//! (see [`crate::store_run_events`]), so no outcome names a record a crash could take back.
//!
//! The outcomes live in memory ([`crate::diagnostics::RUN_OUTCOMES`]), so what they cover
//! starts when the process started. A run paused before then is reported when its session
//! opens (see [`paused_at_open`]); one whose session stays closed is not.

use broca_protocol::{PauseReason, ProviderError, RunFinishReason};
use broca_session_loop::writer::Writer;
use broca_session_store::backend::SessionBackend;
use broca_session_store::record::{Record, RunStart};
use broca_session_store::session::{Appended, Session};

use crate::diagnostics::{bounded, Ring, RingEvent};

/// One run's outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct RunOutcome {
    pub at_ms: i64,
    pub run_id: String,
    /// The model provider module the run's start froze; absent when its start isn't known.
    pub provider: Option<String>,
    /// The model id the run's start froze; absent when its start isn't known.
    pub model: Option<String>,
    /// The credential the run used: its start's, or the one an account move chose since.
    pub credential: Option<String>,
    pub kind: OutcomeKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OutcomeKind {
    /// The run ended `error`. `class` is the provider error's class (`transient`, `permanent`,
    /// `auth_required` or `context_overflow`), `code` its
    /// provider code and `status` its HTTP status; each is absent when the end recorded no
    /// provider error, or the error had none. `step` is the last step the run started.
    Error {
        class: Option<String>,
        code: Option<String>,
        status: Option<u16>,
        step: Option<u64>,
    },
    /// The run ended `interrupted`, with the reason its end recorded.
    Interrupted { reason: Option<String> },
    /// The run paused for sign-in (`AuthRequired`) or an account limit (`AccountLimit`).
    /// Pauses for a restart or a held tool call need nobody to act and aren't reported.
    Paused { reason: PauseReason },
    /// A run reported paused started a step again or ended without an error or interruption.
    Resumed,
}

impl RingEvent for RunOutcome {
    fn at_ms(&self) -> i64 {
        self.at_ms
    }
    fn bound(&mut self) {
        self.run_id = bounded(std::mem::take(&mut self.run_id), 256);
        let limit = |value: &mut Option<String>, limit| {
            if let Some(text) = value.take() {
                *value = Some(bounded(text, limit));
            }
        };
        limit(&mut self.provider, 256);
        limit(&mut self.model, 256);
        limit(&mut self.credential, 512);
        match &mut self.kind {
            OutcomeKind::Error { class, code, .. } => {
                limit(class, 64);
                limit(code, 128);
            }
            OutcomeKind::Interrupted { reason } => limit(reason, 128),
            OutcomeKind::Paused { .. } | OutcomeKind::Resumed => {}
        }
    }
}

/// Whether a pause for `reason` is reported: only one somebody has to act on.
fn reported(reason: PauseReason) -> bool {
    matches!(
        reason,
        PauseReason::AuthRequired | PauseReason::AccountLimit
    )
}

/// The credential id an auth selection (a run start's, or an account move's) names.
fn credential_of(selection: &serde_json::Value) -> Option<String> {
    selection
        .get("credential_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

/// The run whose records follow.
struct OpenRun {
    run_id: String,
    provider: Option<String>,
    model: Option<String>,
    credential: Option<String>,
    /// The last step it started.
    step: Option<u64>,
    /// Seeded from an import or a fork: it never calls a provider, so it has no outcomes.
    seeded: bool,
    /// Its last reported outcome is a pause no later outcome has cleared.
    paused: bool,
}

impl OpenRun {
    fn started(start: &RunStart) -> Self {
        Self {
            run_id: start.run_id.clone(),
            provider: Some(start.model.provider_module_id.clone()),
            model: Some(start.model.model_id.clone()),
            credential: start.auth_selection.as_ref().and_then(credential_of),
            step: None,
            seeded: start.seeded == Some(true),
            paused: false,
        }
    }
}

struct Recorder {
    target: &'static Ring<RunOutcome>,
    now_ms: fn() -> i64,
    run: Option<OpenRun>,
    /// Outcomes of records appended without a barrier, recorded once a barrier makes them durable.
    unsynced: Vec<RunOutcome>,
}

impl Recorder {
    fn appended(&mut self, appended: Appended<'_>) {
        match appended {
            Appended::Records {
                records, durable, ..
            } => {
                for record in records {
                    self.project(record);
                }
                if durable {
                    self.release();
                }
            }
            Appended::Synced => self.release(),
            // A revert took back records appended before; their outcomes, if not yet recorded,
            // go with them, and the run they belonged to is no longer known.
            Appended::Replaced => {
                self.unsynced.clear();
                self.run = None;
            }
        }
    }

    fn release(&mut self) {
        for outcome in self.unsynced.drain(..) {
            self.target.record(outcome);
        }
    }

    fn project(&mut self, record: &Record) {
        if let Record::StepStart(start) = record {
            if let Some(run) = self.run.as_mut().filter(|run| run.run_id == start.run_id) {
                run.step = Some(start.step_id);
            }
        }
        let (run_id, at_ms, kind) = match record {
            Record::RunStart(start) => {
                self.run = Some(OpenRun::started(start));
                return;
            }
            Record::AuthRefrozen(refrozen) => {
                if let Some(run) = self
                    .run
                    .as_mut()
                    .filter(|run| run.run_id == refrozen.run_id)
                {
                    run.credential = credential_of(&refrozen.auth_selection);
                }
                return;
            }
            Record::Pause(pause) if reported(pause.reason) => (
                &pause.run_id,
                pause.ts_ms,
                OutcomeKind::Paused {
                    reason: pause.reason,
                },
            ),
            Record::StepStart(start)
                if self
                    .run
                    .as_ref()
                    .is_some_and(|run| run.run_id == start.run_id && run.paused) =>
            {
                (&start.run_id, None, OutcomeKind::Resumed)
            }
            Record::RunEnd(end) => {
                let kind = match end.reason {
                    RunFinishReason::Error => {
                        let error = end.error.as_ref().and_then(|error| {
                            serde_json::from_value::<ProviderError>(error.parse()).ok()
                        });
                        let shared = error.map(broca_protocol::caller::SharedProviderError::from);
                        OutcomeKind::Error {
                            class: shared.as_ref().map(|error| error.class_wire().to_owned()),
                            status: shared.as_ref().and_then(|error| error.as_inner().status),
                            code: shared.and_then(|error| error.as_inner().provider_code.clone()),
                            step: self
                                .run
                                .as_ref()
                                .filter(|run| run.run_id == end.run_id)
                                .and_then(|run| run.step),
                        }
                    }
                    RunFinishReason::Interrupted => OutcomeKind::Interrupted {
                        reason: end.interrupt_reason.clone(),
                    },
                    _ if self
                        .run
                        .as_ref()
                        .is_some_and(|run| run.run_id == end.run_id && run.paused) =>
                    {
                        OutcomeKind::Resumed
                    }
                    _ => {
                        self.run = None;
                        return;
                    }
                };
                (&end.run_id, end.ts_ms, kind)
            }
            _ => return,
        };
        let open = self.run.as_mut().filter(|run| &run.run_id == run_id);
        if open.as_ref().is_some_and(|run| run.seeded) {
            return;
        }
        let (provider, model, credential) = open.as_ref().map_or((None, None, None), |run| {
            (
                run.provider.clone(),
                run.model.clone(),
                run.credential.clone(),
            )
        });
        if let Some(run) = open {
            run.paused = matches!(kind, OutcomeKind::Paused { .. });
        }
        self.unsynced.push(RunOutcome {
            at_ms: at_ms
                .and_then(|ms| i64::try_from(ms).ok())
                .unwrap_or_else(|| (self.now_ms)()),
            run_id: run_id.clone(),
            provider,
            model,
            credential,
            kind,
        });
        if matches!(record, Record::RunEnd(_)) {
            self.run = None;
        }
    }
}

fn wall_now_ms() -> i64 {
    crate::diagnostics::now_ms()
}

/// Reports `session`'s run outcomes from its next append on. The run already open (a paused
/// one, when the session reopens) is taken from the session's run state, so its later outcomes name
/// its provider and credential, and its resume clears its pause.
pub fn observe<B: SessionBackend>(session: &mut Session<B>) {
    observe_into(session, &crate::diagnostics::RUN_OUTCOMES, wall_now_ms);
}

fn observe_into<B: SessionBackend>(
    session: &mut Session<B>,
    target: &'static Ring<RunOutcome>,
    now_ms: fn() -> i64,
) {
    let run = session.operations().run.as_ref().and_then(|run| {
        let start = run.start.as_ref()?;
        let mut open = OpenRun::started(start);
        if run.finished.is_some() {
            return None;
        }
        if let Some(selection) = &run.refrozen_auth {
            open.credential = credential_of(selection);
        }
        open.paused = run.paused.is_some_and(reported);
        Some(open)
    });
    let mut recorder = Recorder {
        target,
        now_ms,
        run,
        unsynced: Vec::new(),
    };
    session.add_append_observer(Box::new(move |appended| recorder.appended(appended)));
}

/// The outcome to report for `session`'s latest run when the session opens: its pause, when it
/// is paused for sign-in or an account limit, dated by its pause record. A pause from before
/// this process started is otherwise never reported. Reads the run's records from its start.
pub async fn paused_at_open<B: SessionBackend>(
    session: &Session<B>,
) -> Result<Option<RunOutcome>, String> {
    let Some(run) = session.operations().run.as_ref().filter(|run| {
        run.finished.is_none()
            && run.cancel.is_none()
            && !run.seeded
            && run.paused.is_some_and(reported)
    }) else {
        return Ok(None);
    };
    let Some(start) = run.start.as_ref() else {
        return Ok(None);
    };
    let first = session
        .find_id(broca_session_store::idindex::IdKind::RunId, &run.run_id)
        .await
        .map_err(|error| format!("{error:?}"))?
        .into_iter()
        .find_map(|(seq, record)| matches!(record, Record::RunStart(_)).then_some(seq))
        .ok_or("the paused run has no run start")?;
    let records = session
        .records_from(first)
        .await
        .map_err(|error| format!("{error:?}"))?;
    let Some(pause) = records.iter().rev().find_map(|(_, record)| match record {
        Record::Pause(pause) if pause.run_id == run.run_id && reported(pause.reason) => Some(pause),
        _ => None,
    }) else {
        return Err("the paused run has no pause record".into());
    };
    let mut open = OpenRun::started(start);
    if let Some(selection) = &run.refrozen_auth {
        open.credential = credential_of(selection);
    }
    Ok(Some(RunOutcome {
        at_ms: pause
            .ts_ms
            .and_then(|ms| i64::try_from(ms).ok())
            .unwrap_or_else(wall_now_ms),
        run_id: run.run_id.clone(),
        provider: open.provider,
        model: open.model,
        credential: open.credential,
        kind: OutcomeKind::Paused {
            reason: pause.reason,
        },
    }))
}

/// The outcomes a test session reports, kept apart from the process's own ring.
#[cfg(test)]
pub(crate) fn observe_for_test<B: SessionBackend>(
    session: &mut Session<B>,
) -> &'static Ring<RunOutcome> {
    let ring: &'static Ring<RunOutcome> = Box::leak(Box::new(Ring::new(0)));
    observe_into(session, ring, || 1);
    ring
}

#[cfg(test)]
#[path = "run_outcomes_tests.rs"]
mod tests;
