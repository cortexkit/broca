//! Small durable approval and completed-step usage facts for actor status.
//! The writer tap copies IDs, deadlines and usage totals, never arguments, messages or tool results.
use broca_protocol::RunId;
use broca_wal::{FramedRecord, WalLineageId, WalRecord};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct RunFacts {
    prepared: Option<bool>,
    title: Option<Option<String>>,
    pending: BTreeMap<String, (String, broca_wire::PendingApproval)>,
}
#[derive(Default)]
struct Facts {
    current: Option<RunId>,
    usage: Option<broca_core::RunUsageProjection>,
    // At most the current run is cached. Historical withdrawal obligations are
    // durable debt records, not status facts retained on the writer thread.
    runs: HashMap<RunId, RunFacts>,
    #[cfg(test)]
    close_visits: usize,
}

pub(crate) struct ApprovalState {
    #[cfg(test)]
    before_awaiting_facts: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    facts: Mutex<Facts>,
    covered: AtomicU64,
    changed: tokio::sync::Notify,
    pub(crate) healthy: Arc<AtomicBool>,
    pub(crate) debt_ready: AtomicBool,
}
impl Default for ApprovalState {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            before_awaiting_facts: Mutex::default(),
            facts: Mutex::default(),
            covered: AtomicU64::new(0),
            changed: tokio::sync::Notify::new(),
            healthy: Arc::new(AtomicBool::new(true)),
            debt_ready: AtomicBool::new(false),
        }
    }
}
impl ApprovalState {
    pub(crate) fn observe(
        &self,
        seq: u64,
        _: u64,
        lineage: Option<WalLineageId>,
        record: &WalRecord,
    ) {
        let mut facts = self.facts.lock().unwrap();
        if seq <= self.covered.load(Ordering::Acquire) {
            return;
        }
        Self::fold(&mut facts, seq, lineage, record);
        self.covered.store(seq, Ordering::Release);
        drop(facts);
        self.changed.notify_waiters();
    }
    fn fold(facts: &mut Facts, seq: u64, lineage: Option<WalLineageId>, record: &WalRecord) {
        if let WalRecord::RunStarted { run_id, .. } = record {
            if facts
                .usage
                .as_ref()
                .is_none_or(|usage| usage.run_id() != run_id)
            {
                facts.usage = Some(broca_core::RunUsageProjection::new(run_id.clone()));
            }
        }
        if let Some(usage) = &mut facts.usage {
            usage.observe(record);
        }
        match record {
            WalRecord::RunStarted { run_id, title, .. } => {
                facts.current = Some(run_id.clone());
                facts.runs.clear();
                let run = facts.runs.entry(run_id.clone()).or_default();
                run.prepared.get_or_insert(false);
                run.title = Some(title.clone());
            }
            WalRecord::ToolCallDeferred {
                batch_id,
                call_id,
                ask,
                ..
            } => {
                let Some(run_id) = facts.current.clone() else {
                    return;
                };
                let run = facts.runs.entry(run_id).or_default();
                run.prepared = Some(true);
                if let Some(lineage) = lineage {
                    let key = format!("{lineage}:{seq}");
                    run.pending.insert(
                        key.clone(),
                        (
                            batch_id.0.clone(),
                            broca_wire::PendingApproval {
                                call_key: key,
                                call_id: call_id.0.clone(),
                                elicitation_id: None,
                                expires_at_ms: ask.expires_at_ms,
                            },
                        ),
                    );
                }
            }
            WalRecord::ApprovalRecorded { call_key, .. } => {
                if let Some(run) = facts.current.as_ref().and_then(|id| facts.runs.get_mut(id)) {
                    run.pending.remove(call_key);
                }
            }
            WalRecord::AwaitingApproval { pending, .. } => {
                if let Some(run) = facts.current.as_ref().and_then(|id| facts.runs.get_mut(id)) {
                    for entry in pending {
                        if let Some((_, call)) = run.pending.get_mut(&entry.call_key) {
                            call.elicitation_id = entry.elicitation_id.clone();
                        }
                    }
                }
            }
            WalRecord::ToolResult {
                batch_id, result, ..
            } => Self::close(facts, &batch_id.0, &result.tool_call_id.0),
            WalRecord::ToolNoDispatch {
                batch_id,
                tool_call_id,
                ..
            }
            | WalRecord::ToolDispatchIntent {
                batch_id,
                tool_call_id,
                ..
            } => Self::close(facts, &batch_id.0, &tool_call_id.0),
            _ => {}
        }
    }
    /// A tool intent or result visits only the current run's cache entry. Its pending
    /// calls belong to one tool batch; scanning them costs O(batch calls), independently
    /// of earlier runs. This is not an O(1) bound on all work for the record.
    fn close(facts: &mut Facts, batch: &str, call_id: &str) {
        if let Some(run) = facts.current.as_ref().and_then(|id| facts.runs.get_mut(id)) {
            #[cfg(test)]
            {
                facts.close_visits += 1;
            }
            run.pending
                .retain(|_, (id, call)| id != batch || call.call_id != call_id);
        }
    }
    pub(crate) fn seed(
        &self,
        lineage: Option<WalLineageId>,
        head: u64,
        current: Option<RunId>,
        frames: &[FramedRecord],
        prefix: Option<&broca_core::DeferredCallProjection>,
        complete: bool,
    ) {
        let Ok(mut facts) = self.facts.lock() else {
            self.healthy.store(false, Ordering::Release);
            return;
        };
        if self.covered.load(Ordering::Acquire) > head {
            return;
        }
        facts.current = current;
        facts.usage = None;
        facts.runs.clear();
        if let Some(id) = facts.current.clone() {
            facts.runs.entry(id).or_default().prepared = complete.then_some(false);
        }
        if let Some(prefix) = prefix {
            for (key, call) in &prefix.calls {
                let Some(id) = &call.run_id else { continue };
                if facts.current.as_ref() != Some(id) {
                    continue;
                }
                let run = facts.runs.entry(id.clone()).or_default();
                run.prepared = Some(true);
                if call.state == broca_core::OpenCallState::Deferred {
                    if let WalRecord::ToolCallDeferred {
                        batch_id,
                        call_id,
                        ask,
                        ..
                    } = &*call.prepared
                    {
                        run.pending.insert(
                            key.clone(),
                            (
                                batch_id.0.clone(),
                                broca_wire::PendingApproval {
                                    call_key: key.clone(),
                                    call_id: call_id.0.clone(),
                                    elicitation_id: call.elicitation_id.clone(),
                                    expires_at_ms: ask.expires_at_ms,
                                },
                            ),
                        );
                    }
                }
            }
        }
        for frame in frames {
            Self::fold(&mut facts, frame.seq, lineage, &frame.record);
        }
        self.covered.store(head, Ordering::Release);
        drop(facts);
        self.changed.notify_waiters();
    }
    pub(crate) fn prepared(&self, run_id: &RunId, head: u64) -> Option<bool> {
        if !self.healthy.load(Ordering::Acquire) {
            return None;
        }
        let facts = self.facts.lock().ok()?;
        match facts.runs.get(run_id)?.prepared {
            Some(true) => Some(true),
            other if self.covered.load(Ordering::Acquire) >= head => other,
            _ => None,
        }
    }
    pub(crate) async fn wait_covered(&self, head: u64) {
        let wait = async {
            loop {
                let changed = self.changed.notified();
                if self.covered.load(Ordering::Acquire) >= head
                    || !self.healthy.load(Ordering::Acquire)
                {
                    return;
                }
                changed.await;
            }
        };
        let _ = tokio::time::timeout(std::time::Duration::from_millis(100), wait).await;
    }
    pub(crate) fn awaiting(
        &self,
        run_id: &RunId,
        head: u64,
    ) -> Option<broca_wire::AwaitingApproval> {
        if !self.healthy.load(Ordering::Acquire) || self.covered.load(Ordering::Acquire) < head {
            return None;
        }
        #[cfg(test)]
        if let Some(probe) = self.before_awaiting_facts.lock().unwrap().take() {
            probe();
        }
        let facts = self.facts.lock().ok()?;
        let run = facts.runs.get(run_id)?;
        let batch = run.pending.values().next()?.0.clone();
        Some(broca_wire::AwaitingApproval {
            batch_id: batch,
            pending: run
                .pending
                .values()
                .map(|(_, pending)| pending.clone())
                .collect(),
        })
    }
    pub(crate) fn run_usage(
        &self,
        run_id: &RunId,
        head: u64,
    ) -> Option<broca_protocol::usage::RunUsage> {
        if !self.healthy.load(Ordering::Acquire) {
            return None;
        }
        let facts = self.facts.lock().ok()?;
        if self.covered.load(Ordering::Acquire) < head {
            return None;
        }
        let projection = facts
            .usage
            .as_ref()
            .filter(|usage| usage.run_id() == run_id)?;
        Some(projection.usage.clone())
    }

    pub(crate) fn title(&self, run_id: &RunId) -> Option<Option<String>> {
        if !self.healthy.load(Ordering::Acquire) {
            return None;
        }
        self.facts.lock().ok()?.runs.get(run_id)?.title.clone()
    }
    pub(crate) fn finished(&self, run_id: &RunId) {
        if let Ok(mut facts) = self.facts.lock() {
            if facts.current.as_ref() == Some(run_id) {
                facts.current = None;
                facts.usage = None;
                facts.runs.clear();
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn retained_runs(&self) -> usize {
        self.facts.lock().unwrap().runs.len()
    }
    #[cfg(test)]
    pub(crate) fn close_visits(&self) -> usize {
        self.facts.lock().unwrap().close_visits
    }
    #[cfg(test)]
    pub(crate) fn on_awaiting_facts_lock(&self, probe: impl FnOnce() + Send + 'static) {
        *self.before_awaiting_facts.lock().unwrap() = Some(Box::new(probe));
    }
}
