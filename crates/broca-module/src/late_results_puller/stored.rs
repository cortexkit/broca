//! Late-result eligibility comes from the session's own durable records, not a WAL projection.

use super::*;
use broca_session_store::fold::operations::{Resolution, Run};
use broca_session_store::record::{CallOutcome, Record, ToolHeld, ToolIntent};

/// Extend a lost-database rebuild with every backup class's `broca-session-store` inventory.
/// Only the latest open batch can still accept an answer; read its durable dispatch records
/// back to the run's start, which also retains the frozen scope.
/// The caller must keep intake fenced until both this scan and the WAL scan succeed.
pub async fn rebuild_store_from_sessions_with_progress(
    store: &LateResultsStore,
    state_root: &Path,
    runtime: Option<&crate::store_open::StoreRuntime>,
    report: &mut RebuildReport,
    progress: impl Fn(&RebuildReport),
) -> Result<(), String> {
    use broca_session_store::backend::LocalBackend;
    let inventory = || {
        let mut sessions = Vec::new();
        for class in broca_wal::StorageClass::ALL {
            let backend = LocalBackend::new(class.store_root(state_root));
            for address in backend
                .stored_sessions()
                .map_err(|error| format!("{error:?}"))?
            {
                sessions.push((class, address));
            }
        }
        Ok::<_, String>(sessions)
    };
    let sessions = inventory()?;
    for (class, address) in &sessions {
        let head = class
            .store_root(state_root)
            .join("heads")
            .join(&address.0)
            .join("head");
        let scanned = async {
            let runtime = runtime.ok_or("session store runtime is unavailable")?;
            let backend = LocalBackend::new(class.store_root(state_root));
            let (bytes, _) = backend
                .read_stored_head(address)
                .map_err(|error| format!("{error:?}"))?
                .ok_or("session head disappeared")?;
            let triple = broca_session_store::head::stored_triple(&bytes)
                .map_err(|error| format!("{error:?}"))?;
            let bind = BindIdentity::new(triple.project_root, triple.harness, triple.session);
            if broca_core::session_addr(&bind) != address.0 {
                return Err("session head identity differs from its address".into());
            }
            let view = runtime
                .current_view(&bind)
                .await
                .map_err(|error| format!("{error:?}"))?
                .ok_or("inventoried session disappeared")?;
            let calls = rebuild_registrations(&view, &bind).await?;
            for call in &calls {
                store.ensure_feed(&call.module, true)?;
            }
            store.register_calls(&calls)?;
            report.registered += calls.len();
            Ok::<_, String>(())
        }
        .await;
        report.sessions += 1;
        if let Err(error) = scanned {
            tracing::warn!(target: "late_results", session_head = %head.display(), "late-results rebuild incomplete: {error}");
            report.unreadable.push(head);
        }
        progress(report);
    }
    if inventory()? != sessions {
        return Err("session store inventory changed during late-results rebuild".into());
    }
    Ok(())
}

async fn rebuild_registrations(
    view: &broca_session_store::session::SessionView<broca_session_store::backend::LocalBackend>,
    bind: &BindIdentity,
) -> Result<Vec<LateResultRegistration>, String> {
    let Some(run) = view
        .operations
        .run
        .as_ref()
        .filter(|run| run.finished.is_none() && run.cancel.is_none())
    else {
        return Ok(Vec::new());
    };
    let Some(batch) = &run.open_batch else {
        return Ok(Vec::new());
    };
    if batch
        .calls
        .iter()
        .any(|call| call.held_reply.is_some() && (!call.intent || call.key.is_none()))
    {
        return Err("open held call has no intent or call key".into());
    }
    let keys: Vec<_> = batch
        .calls
        .iter()
        .filter(|call| call.intent && call.terminal.is_none())
        .filter_map(|call| call.key.as_ref())
        .collect();
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut records = view
        .ledger_tail_until(
            |record| matches!(record, Record::RunStart(start) if start.run_id == run.run_id),
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    records.reverse();
    let start = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::RunStart(start) if start.run_id == run.run_id => Some(start),
            _ => None,
        })
        .ok_or("open run has no durable start")?;
    let scope: Option<broca_protocol::SessionScope> = start
        .scope
        .clone()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| format!("invalid frozen run scope: {error}"))?;
    let lineage = StoredLineage::Readable {
        records,
        run: Some(Box::new(run.clone())),
    };
    let mut registrations = Vec::new();
    for key in keys {
        let call = lineage.call(key).ok_or("open call has no durable intent")?;
        if call.intent.run_id != run.run_id || call.intent.batch_id != batch.batch_id {
            return Err("open call intent belongs to another run or batch".into());
        }
        if call.terminal || call.run_ended || call.closing {
            continue;
        }
        let module = call
            .held
            .as_ref()
            .map(|held| &held.module)
            .or(call.intent.module_id.as_ref());
        let Some(module) = module else {
            continue;
        };
        if call.held.is_some()
            && call
                .intent
                .module_id
                .as_ref()
                .is_some_and(|target| target != module)
        {
            return Err("held call module differs from its intent".into());
        }
        cortexkit_role_tool_provider::call::validate_call_key(key)
            .map_err(|error| error.to_string())?;
        let (lineage, _) = key
            .rsplit_once(':')
            .ok_or("registered call key has no lineage")?;
        registrations.push(LateResultRegistration {
            lineage: lineage.into(),
            call_key: key.clone(),
            module: module.clone(),
            bind: bind.clone(),
            scope: scope.clone(),
        });
    }
    Ok(registrations)
}

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
    /// The provider the call was handed back from for the person's steer, whose terminal for
    /// it is delivered as a notice.
    yielded_by: Option<&'a str>,
    /// Whether that notice is already in the session.
    noticed: bool,
}

/// The send id of the notice that delivers a handed-back call's terminal: one per call, so a
/// redelivered entry finds its notice already sent.
pub fn notice_send_id(call_key: &str) -> String {
    format!("{NOTICE_SEND_ID_PREFIX}{call_key}")
}

const NOTICE_SEND_ID_PREFIX: &str = "late-notice:";

/// Whether `send_id` has the form of a notice's send id ([`notice_send_id`]). It checks the
/// prefix only, not that the call exists.
pub fn is_notice_send_id(send_id: &str) -> bool {
    send_id.starts_with(NOTICE_SEND_ID_PREFIX)
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
            yielded_by: None,
            noticed: false,
        };
        let notice = notice_send_id(key);
        for (_, record) in records {
            match record {
                Record::QueuedSend(queued) if queued.send_id.as_deref() == Some(&notice) => {
                    call.noticed = true;
                }
                Record::ToolOutcome(outcome) if outcome.run_id == intent.run_id => {
                    if outcome.batch_id == intent.batch_id
                        && outcome.tool_call_id == intent.tool_call_id
                    {
                        call.terminal = true;
                        call.yielded_by = outcome.yielded_by.as_deref();
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

    /// The id and tool of a call handed back for the person's steer whose notice isn't in the
    /// session yet.
    pub(super) fn yielded_call(&self, key: &str) -> Option<(ToolCallId, String)> {
        let call = self.call(key)?;
        (call.yielded_by.is_some() && !call.noticed).then(|| {
            (
                call.intent.tool_call_id.clone(),
                call.intent.tool_name.clone().unwrap_or_default(),
            )
        })
    }

    pub(super) fn needs_intake(&self, key: &str) -> bool {
        // A missing intent or unreadable store may hide a hold. A readable ordinary dispatch
        // starts no feed; its held-record callback starts one if the provider later holds it. A
        // call handed back for the person's steer needs the feed until its notice is in the
        // session, since its provider's own terminal for it is still owed.
        self.call(key)
            .is_none_or(|call| call.held.is_some() || (call.yielded_by.is_some() && !call.noticed))
    }

    /// Whether the call finished without its provider holding it. A handed-back call didn't:
    /// its provider's terminal for it is still owed.
    pub(super) fn answered_inline(&self, key: &str) -> bool {
        self.call(key)
            .is_some_and(|call| call.terminal && call.held.is_none() && call.yielded_by.is_none())
    }

    /// Whether no withdrawal can ever be owed for the call: it finished with a result, or no
    /// provider ever held it. A held call left unfinished when its run ended may be owed one,
    /// since a cancelled hold is withdrawn, so its watch stays for withdrawal to settle.
    pub(super) fn withdrawal_never_owed(&self, key: &str) -> bool {
        self.call(key)
            .is_some_and(|call| call.terminal || call.held.is_none())
    }

    pub(super) fn retirement_reason(&self, key: &str) -> Option<&'static str> {
        let call = self.call(key)?;
        // A handed-back call's registration lives until its notice is in the session, however
        // its run ended: a Stop ends the turn only.
        if call.yielded_by.is_some() {
            return call.noticed.then_some("notice_delivered");
        }
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
        // A call handed back for the person's steer takes its provider's terminal as a notice,
        // whether or not its run has ended since.
        if let Some(carrier) = call.yielded_by {
            return if call.noticed {
                settled_without_delivery(module, entry, "already_noticed")
            } else if carrier != module {
                settled_without_delivery(module, entry, "dispatched_elsewhere")
            } else {
                Classified::Delivered
            };
        }
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
        if !broca_protocol::same_module(&held.module, module) {
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
