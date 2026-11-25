//! Late-result eligibility comes from the session's own durable records, not a log projection.

use super::*;
use broca_session_store::backend::SessionBackend;
use broca_session_store::fold::operations::{Resolution, Run};
use broca_session_store::record::{CallOutcome, Record, RunStart, ToolHeld, ToolIntent};
use broca_session_store::session::{Session, SessionError};

/// Extend a lost-database rebuild with every backup class's `broca-session-store` inventory.
/// The latest open batch can still accept an answer; read its durable dispatch records back to
/// the run's start, which also retains the frozen scope. Runs started within
/// [`REBUILD_WINDOW`] are read too, for what they still owe a provider after they finished (see
/// [`rebuild_registrations`]); a session whose latest run started before it is not read at all
/// unless that run is still open.
/// The caller must keep intake fenced until this scan succeeds.
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
        for class in broca_records::session_storage::StorageClass::ALL {
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
                .current_view_for(&bind, "late_results")
                .await
                .map_err(|error| format!("{error:?}"))?
                .ok_or("inventoried session disappeared")?;
            let calls = rebuild_registrations(&view, &bind, crate::actor::epoch_ms()).await?;
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

/// How far back a lost-database rebuild looks for what a finished run still owes a provider: the
/// terminal of a call handed back for the person's steer, which is delivered as a notice, and the
/// withdrawal of a held call its run cancelled or ended around. The normal paths keep both for as
/// long as they are owed, but a rebuild has only the sessions' records to find them in, and
/// reading every session's whole history would make its cost grow with that history.
///
/// The provider contract keeps a settled result unacked for 24 hours after it settled
/// (tool-provider `CONTRACT.md` §7, "Retention"). It bounds neither how long a held call may run
/// nor how long a withdrawal may stay owed, and broca puts no bound of its own on a hold
/// (`docs/module-surface.md`, "Ack guarantee, narrowed"). Seven days covers that day with a wide
/// margin for a provider slow to settle a call or to answer a withdrawal. An obligation of a run
/// started earlier is lost with the database: a handed-back call's late entry is then acked
/// without a notice, and a withdrawal owed for a call cancelled that long ago is not sent again,
/// so its provider's work runs to its end and its result is acked without delivery. A run still
/// open is rebuilt however long ago it started, since its batch waits for its answer.
pub(crate) const REBUILD_WINDOW: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The registrations a lost database held for the session `view` at `bind`, rebuilt from its
/// records as of `now_ms`: every call of its open batch still waiting on its provider; and, of
/// the runs started within [`REBUILD_WINDOW`], every call handed back for the person's steer
/// whose notice isn't in the session yet, and every held call its provider never answered,
/// which may still be owed a withdrawal. Each comes with its withdrawal watch, as a registration
/// made at dispatch does, so the intake and withdrawal recovery treat it as they would have:
/// the intake retires the registration of a call whose run ended and keeps its watch, and
/// recovery withdraws the call if the session shows it was cancelled while held.
async fn rebuild_registrations(
    view: &broca_session_store::session::SessionView<broca_session_store::backend::LocalBackend>,
    bind: &BindIdentity,
    now_ms: u64,
) -> Result<Vec<LateResultRegistration>, String> {
    let Some(latest) = view.operations.run.as_ref() else {
        return Ok(Vec::new());
    };
    let since_ms = now_ms.saturating_sub(REBUILD_WINDOW.as_millis() as u64);
    // A start that records no time can't be shown to be old, so its run is read.
    let recent = |start: &RunStart| {
        start
            .written_at_ms
            .or(start.ts_ms)
            .is_none_or(|at| at >= since_ms)
    };
    let open = (latest.finished.is_none() && latest.cancel.is_none())
        .then_some(latest.open_batch.as_ref())
        .flatten();
    if let Some(batch) = open {
        if batch
            .calls
            .iter()
            .any(|call| call.held_reply.is_some() && (!call.intent || call.key.is_none()))
        {
            return Err("open held call has no intent or call key".into());
        }
    }
    let waiting: Vec<&String> = open
        .iter()
        .flat_map(|batch| &batch.calls)
        .filter(|call| call.intent && call.terminal.is_none())
        .filter_map(|call| call.key.as_ref())
        .collect();
    if waiting.is_empty() && !latest.start.as_ref().is_none_or(recent) {
        return Ok(Vec::new());
    }
    // Back to the oldest run started within the window, or to the open run's start, whichever
    // is further.
    let mut records = view
        .ledger_tail_until(|record| matches!(record, Record::RunStart(start) if !recent(start)))
        .await
        .map_err(|error| format!("{error:?}"))?;
    records.reverse();
    let open_run = open.map(|_| latest.run_id.as_str());
    // The frozen scope of each run whose calls are rebuilt, decoded only for a call registered.
    let scopes: HashMap<String, Option<serde_json::Value>> = records
        .iter()
        .filter_map(|(_, record)| match record {
            Record::RunStart(start) if recent(start) || Some(start.run_id.as_str()) == open_run => {
                Some((start.run_id.clone(), start.scope.clone()))
            }
            _ => None,
        })
        .collect();
    if open_run.is_some_and(|run| !scopes.contains_key(run)) {
        return Err("open run has no durable start".into());
    }
    let keys: Vec<String> = records
        .iter()
        .filter_map(|(_, record)| match record {
            Record::ToolIntent(intent) if scopes.contains_key(&intent.run_id) => {
                intent.call_key.clone()
            }
            _ => None,
        })
        .collect();
    if waiting.iter().any(|key| !keys.contains(*key)) {
        return Err("open call has no durable intent".into());
    }
    let lineage = StoredLineage::Readable {
        records,
        run: Some(Box::new(latest.clone())),
        foreign: Vec::new(),
    };
    let mut registrations = Vec::new();
    for key in &keys {
        let call = lineage
            .call(key)
            .ok_or("rebuilt call has no durable intent")?;
        if waiting.contains(&key)
            && open.is_some_and(|batch| {
                call.intent.run_id != latest.run_id || call.intent.batch_id != batch.batch_id
            })
        {
            return Err("open call intent belongs to another run or batch".into());
        }
        let waits = waiting.contains(&key) && !call.terminal && !call.run_ended && !call.closing;
        let unnoticed = call.yielded_by.is_some() && !call.noticed;
        let withdrawable = call.held.is_some() && (!call.terminal || call.held_closed);
        if !(waits || unnoticed || withdrawable) {
            continue;
        }
        let module = call
            .held
            .map(|held| held.module.as_str())
            .or(call.yielded_by)
            .or(call.intent.module_id.as_deref());
        let Some(module) = module else {
            continue;
        };
        if call.held.is_some()
            && call
                .intent
                .module_id
                .as_ref()
                .is_some_and(|target| !broca_protocol::same_module(target, module))
        {
            return Err("held call module differs from its intent".into());
        }
        cortexkit_role_tool_provider::call::validate_call_key(key)
            .map_err(|error| error.to_string())?;
        let (lineage, _) = key
            .rsplit_once(':')
            .ok_or("registered call key has no lineage")?;
        let scope: Option<broca_protocol::SessionScope> = scopes
            .get(&call.intent.run_id)
            .cloned()
            .flatten()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| format!("invalid frozen run scope: {error}"))?;
        registrations.push(LateResultRegistration {
            lineage: lineage.into(),
            call_key: key.clone(),
            module: module.to_owned(),
            bind: bind.clone(),
            scope,
        });
    }
    Ok(registrations)
}

pub(super) enum StoredLineage {
    Readable {
        records: Vec<(u64, Record)>,
        run: Option<Box<Run>>,
        /// Registered keys an earlier session at this triple minted: that session was deleted,
        /// so nothing of theirs can be delivered any more.
        foreign: Vec<String>,
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
    /// Whether the call's own terminal is the close the run wrote for it while its provider
    /// held it (a cancel or stop), not the provider's answer: the provider may still be working
    /// on it, so a withdrawal is owed.
    held_closed: bool,
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

/// The records the intake classifies a session's registered calls by: the session's ledger from
/// the run start of the run that made the oldest of them on, oldest first, and the session's
/// latest run as its records fold it.
#[derive(Debug)]
pub struct HeldCallRecords {
    records: Vec<(u64, Record)>,
    run: Option<Box<Run>>,
    /// The keys that belong to an earlier session at this triple (see [`held_call_records`]).
    foreign: Vec<String>,
}

/// The [`HeldCallRecords`] of the calls `call_keys` names in `session`. The read walks the ledger
/// back from its newest record and stops at the run start of the oldest call's run: it holds
/// every record of the runs the calls were made in, and nothing earlier.
///
/// A key that names neither this session's id nor the lineage it was converted from, and whose
/// intent this session doesn't hold, was minted by a session deleted from this triple before
/// this one was created there. Such a key is set apart, not looked for: the walk would never
/// find it, and would read the whole ledger looking.
pub async fn held_call_records<B: SessionBackend>(
    session: &Session<B>,
    call_keys: &[String],
) -> Result<HeldCallRecords, SessionError> {
    let own = broca_session_store::codec::hex(&session.head().session_id.0);
    let converted = session.operations.source_lineage.as_deref();
    let mut foreign = Vec::new();
    for key in call_keys {
        let lineage = key.rsplit_once(':').map(|(lineage, _)| lineage);
        if lineage.is_some_and(|lineage| lineage == own || Some(lineage) == converted) {
            continue;
        }
        // A key this session's lineage didn't mint may still be its own: a converted session's
        // keys made before its source lineage was recorded name only the old log. Its intent
        // says so, found through the id index rather than the walk.
        let named = session
            .find_id(broca_session_store::idindex::IdKind::CallKey, key)
            .await?;
        if !named
            .iter()
            .any(|(_, record)| matches!(record, Record::ToolIntent(_)))
        {
            foreign.push(key.clone());
        }
    }
    let mut keys: HashSet<&str> = call_keys
        .iter()
        .map(String::as_str)
        .filter(|key| !foreign.iter().any(|foreign| foreign == key))
        .collect();
    if keys.is_empty() {
        return Ok(HeldCallRecords {
            records: Vec::new(),
            run: session.operations.run.clone().map(Box::new),
            foreign,
        });
    }
    let mut oldest_run = None;
    let mut records = session
        .ledger_tail_until(|record| match record {
            Record::ToolIntent(intent) => {
                if intent
                    .call_key
                    .as_deref()
                    .is_some_and(|key| keys.remove(key))
                    && keys.is_empty()
                {
                    oldest_run = Some(intent.run_id.clone());
                }
                false
            }
            Record::RunStart(start) => oldest_run.as_ref() == Some(&start.run_id),
            _ => false,
        })
        .await?;
    records.reverse();
    Ok(HeldCallRecords {
        records,
        run: session.operations.run.clone().map(Box::new),
        foreign,
    })
}

impl StoredLineage {
    /// The session `bind` as the intake classifies its registered calls `keys`: `None` when the
    /// store holds no session there. An open session's actor answers from memory when the
    /// `registry` is given; otherwise the session is read as stored.
    pub(super) async fn read(
        runtime: &crate::store_open::StoreRuntime,
        registry: Option<&crate::LineageRegistry>,
        bind: &BindIdentity,
        keys: Vec<String>,
    ) -> Option<Self> {
        let request = crate::store_reads::StoreReadRequest::HeldCalls { call_keys: keys };
        match runtime.read_own(registry, bind, &request).await {
            Ok(None) => None,
            Ok(Some(crate::store_reads::StoreReadReply::HeldCalls(Ok(held)))) => {
                Some(Self::Readable {
                    records: held.records,
                    run: held.run,
                    foreign: held.foreign,
                })
            }
            Ok(Some(_)) | Err(_) => Some(Self::Unreadable),
        }
    }

    /// Whether the call `key` names was made in a session deleted from this triple before the
    /// one there now was created.
    fn foreign(&self, key: &str) -> bool {
        matches!(self, Self::Readable { foreign, .. } if foreign.iter().any(|foreign| foreign == key))
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
            held_closed: false,
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
                        call.held_closed = held.is_some()
                            && matches!(
                                outcome.outcome,
                                CallOutcome::Interrupted | CallOutcome::MaxSteps
                            );
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
        // session, since its provider's own terminal for it is still owed. A call of a deleted
        // session needs nothing more.
        !self.foreign(key)
            && self.call(key).is_none_or(|call| {
                call.held.is_some() || (call.yielded_by.is_some() && !call.noticed)
            })
    }

    /// Whether the call finished without its provider holding it. A handed-back call didn't:
    /// its provider's terminal for it is still owed.
    pub(super) fn answered_inline(&self, key: &str) -> bool {
        self.call(key)
            .is_some_and(|call| call.terminal && call.held.is_none() && call.yielded_by.is_none())
    }

    /// Whether no withdrawal can ever be owed for the call: its provider answered it, or no
    /// provider ever held it. A held call the run closed itself (a cancel or stop wrote its
    /// terminal), or one left unfinished when its run ended, may be owed one, since a closed
    /// hold is withdrawn, so its watch stays for withdrawal to settle. A deleted session's call
    /// was withdrawn, if it was owed, before the session went.
    pub(super) fn withdrawal_never_owed(&self, key: &str) -> bool {
        self.foreign(key)
            || self
                .call(key)
                .is_some_and(|call| call.held.is_none() || (call.terminal && !call.held_closed))
    }

    pub(super) fn retirement_reason(&self, key: &str) -> Option<&'static str> {
        if self.foreign(key) {
            return Some("session_deleted");
        }
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
        // As for a session no root holds any more: nothing of a deleted session can be delivered.
        if self.foreign(&entry.call_key) {
            return settled_without_delivery(module, entry, "session_deleted");
        }
        let Some(call) = self.call(&entry.call_key) else {
            return Classified::Blocked("intent_missing".into());
        };
        // A call handed back for the person's steer takes its provider's terminal as a notice,
        // whether or not its run has ended since.
        if let Some(carrier) = call.yielded_by {
            return if call.noticed {
                settled_without_delivery(module, entry, "already_noticed")
            } else if !broca_protocol::same_module(carrier, module) {
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
            .is_some_and(|target| !broca_protocol::same_module(target, module))
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
