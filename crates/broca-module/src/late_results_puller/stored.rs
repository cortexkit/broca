//! Late-result eligibility comes from the session's own durable records, not a WAL projection.

use super::*;
use broca_session_store::fold::operations::{Resolution, Run};
use broca_session_store::record::{CallOutcome, Record, ToolHeld, ToolIntent};

pub(super) enum StoredLineage {
    Readable {
        records: Vec<(u64, Record)>,
        run: Option<Box<Run>>,
    },
    Unreadable,
}

struct Call<'a> {
    intent: &'a ToolIntent,
    held: Option<&'a ToolHeld>,
    terminal: bool,
    batch_committed: bool,
    run_ended: bool,
    closing: bool,
}

impl StoredLineage {
    pub(super) async fn read(
        runtime: &crate::store_open::StoreRuntime,
        bind: &BindIdentity,
        mut keys: HashSet<String>,
    ) -> Option<Self> {
        match runtime.current_view(bind).await {
            Ok(None) => None,
            Ok(Some(view)) => {
                // Stop at the oldest registered intent's run boundary. The shared snapshot
                // must include every key it classifies, but no earlier runs or transcript.
                let mut oldest_run = None;
                let records = view
                    .ledger_tail_until(|record| match record {
                        Record::ToolIntent(intent) => {
                            if intent.call_key.as_ref().is_some_and(|key| keys.remove(key))
                                && keys.is_empty()
                            {
                                oldest_run = Some(intent.run_id.clone());
                            }
                            false
                        }
                        Record::RunStart(start) => oldest_run.as_ref() == Some(&start.run_id),
                        _ => false,
                    })
                    .await;
                Some(match records {
                    Ok(mut records) => {
                        records.reverse();
                        Self::Readable {
                            records,
                            run: view.operations.run.clone().map(Box::new),
                        }
                    }
                    Err(_) => Self::Unreadable,
                })
            }
            Err(_) => Some(Self::Unreadable),
        }
    }

    fn call(&self, key: &str) -> Option<Call<'_>> {
        let Self::Readable { records, .. } = self else {
            return None;
        };
        let intent = records.iter().find_map(|(_, record)| match record {
            Record::ToolIntent(intent) if intent.call_key.as_deref() == Some(key) => Some(intent),
            _ => None,
        })?;
        let held = records.iter().find_map(|(_, record)| match record {
            Record::ToolHeld(held)
                if held.call_key == key
                    && held.batch_id == intent.batch_id
                    && held.tool_call_id == intent.tool_call_id =>
            {
                Some(held)
            }
            _ => None,
        });
        let mut call = Call {
            intent,
            held,
            terminal: false,
            batch_committed: false,
            run_ended: false,
            closing: false,
        };
        for (_, record) in records {
            match record {
                Record::ToolOutcome(outcome) if outcome.run_id == intent.run_id => {
                    if outcome.batch_id == intent.batch_id
                        && outcome.tool_call_id == intent.tool_call_id
                    {
                        call.terminal = true;
                    }
                    if matches!(
                        outcome.outcome,
                        CallOutcome::Interrupted | CallOutcome::MaxSteps
                    ) && records.iter().any(|(_, record)| {
                        matches!(record, Record::ToolHeld(held)
                            if held.batch_id == outcome.batch_id
                                && held.tool_call_id == outcome.tool_call_id)
                    }) {
                        call.closing = true;
                    }
                }
                Record::BatchEnd(end)
                    if end.run_id == intent.run_id && end.batch_id == intent.batch_id =>
                {
                    call.batch_committed = true;
                }
                Record::RunEnd(end) if end.run_id == intent.run_id => call.run_ended = true,
                Record::CancelRequest(cancel) if cancel.run_id == intent.run_id => {
                    call.closing = true
                }
                _ => {}
            }
        }
        Some(call)
    }

    pub(super) fn held_call_id(&self, key: &str) -> Option<ToolCallId> {
        self.call(key)?.held.map(|held| held.tool_call_id.clone())
    }

    pub(super) fn needs_intake(&self, key: &str) -> bool {
        // A missing intent or unreadable store may hide a hold. A readable ordinary dispatch
        // starts no feed; its held-record callback starts one if the provider later holds it.
        self.call(key).is_none_or(|call| call.held.is_some())
    }

    pub(super) fn retirement_reason(&self, key: &str) -> Option<&'static str> {
        let call = self.call(key)?;
        if call.run_ended {
            Some("run_ended")
        } else if call.terminal && (call.held.is_none() || call.batch_committed) {
            Some("terminal_and_batch_committed")
        } else {
            None
        }
    }

    pub(super) fn classify(
        &self,
        module: &str,
        entry: &LateEntry,
        row: &RegistrationRow,
    ) -> Classified {
        let Self::Readable { run, .. } = self else {
            return Classified::Blocked("session_store_unreadable".into());
        };
        let Some(call) = self.call(&entry.call_key) else {
            return Classified::Blocked("intent_missing".into());
        };
        if call.terminal {
            return settled_without_delivery(module, entry, "already_terminal");
        }
        if call.run_ended {
            return settled_without_delivery(module, entry, "run_ended");
        }
        if call
            .intent
            .module_id
            .as_deref()
            .is_some_and(|target| target != module)
        {
            return settled_without_delivery(module, entry, "dispatched_elsewhere");
        }
        let Some(held) = call.held else {
            return if row.state == RegistrationState::Registered {
                Classified::Pending
            } else {
                settled_without_delivery(module, entry, "not_held")
            };
        };
        if held.module != module {
            return settled_without_delivery(module, entry, "dispatched_elsewhere");
        }
        if call.closing {
            return settled_without_delivery(module, entry, "run_cancelling");
        }
        // A call key belongs to one intent, run and batch. Even an open later run cannot take
        // an earlier run's answer, and only a durable held resolution can wake the actor.
        match run
            .as_ref()
            .filter(|run| run.run_id == call.intent.run_id && run.finished.is_none())
        {
            Some(run) if run.cancel.is_some() => {
                settled_without_delivery(module, entry, "run_cancelling")
            }
            Some(run) if matches!(run.resolution(), Resolution::ProviderHeld(_)) => {
                Classified::Delivered
            }
            _ => Classified::Blocked("episode_unsettled".into()),
        }
    }
}

#[cfg(test)]
mod tests;
