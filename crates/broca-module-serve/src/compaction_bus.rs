//! Bounded, durable intake of compaction claim checks. Broker failure never gates a run.
use crate::bus_events::{now_ms, BusConnector};
use broca_core::compaction_provider::notice::{event_subject, ViewReady, VIEW_READY_EVENT};
use broca_core::CompactionViews;
use broca_diagnostics::{spawn_blocking, JobKind};
use broca_module::LineageRegistry;
use broca_store::compaction_facts::CompactionFacts;
use broca_store::{
    compaction_receipts::{ReceiptWrite, ViewReceipt},
    SqliteStore,
};
use cortexkit_bus_trait::{Message, StreamCursor};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

/// The tracing target of this module's compaction lines; the fleet logger prints it as the
/// `broca.compaction` logger.
const LOG: &str = "compaction";
/// The name of the durable JetStream consumer this module reads compaction events from, as
/// the log lines report it.
const DURABLE: &str = "m_broca";
/// How long the consumer waits before it tries again after a failed or timed-out pass.
const RETRY: Duration = Duration::from_secs(1);

#[derive(Default)]
pub struct Metrics {
    attached: AtomicBool,
    consumed: AtomicU64,
    acked: AtomicU64,
    dropped_unknown_session: AtomicU64,
    last_error: Mutex<Option<String>>,
}
static METRICS: OnceLock<Arc<Metrics>> = OnceLock::new();
impl Metrics {
    fn error(&self, error: String) {
        *self.last_error.lock().unwrap() = Some(error);
    }
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"attached": self.attached.load(Ordering::Relaxed),
            "consumed": self.consumed.load(Ordering::Relaxed), "acked": self.acked.load(Ordering::Relaxed),
            "dropped_unknown_session": self.dropped_unknown_session.load(Ordering::Relaxed),
            "last_error": self.last_error.lock().unwrap().clone()})
    }
}
pub fn health() -> serde_json::Value {
    METRICS
        .get()
        .map_or_else(|| Metrics::default().snapshot(), |m| m.snapshot())
}

pub struct Service {
    store: Arc<SqliteStore>,
    registry: LineageRegistry,
    wal: Arc<dyn broca_core::WalStore>,
    inbox: Arc<broca_core::ViewReadyInbox>,
    connector: Arc<dyn BusConnector>,
    stop: CancellationToken,
    pub metrics: Arc<Metrics>,
}
impl Service {
    pub fn new(
        store: Arc<SqliteStore>,
        registry: LineageRegistry,
        wal: Arc<dyn broca_core::WalStore>,
        inbox: Arc<broca_core::ViewReadyInbox>,
        connector: Arc<dyn BusConnector>,
        stop: CancellationToken,
    ) -> Self {
        let metrics = Arc::new(Metrics::default());
        let _ = METRICS.set(metrics.clone());
        Self {
            store,
            registry,
            wal,
            inbox,
            connector,
            stop,
            metrics,
        }
    }

    /// Rebuild before serving: even an acknowledged announcement must survive a process loss.
    pub async fn restore(&self) -> Result<(), String> {
        let mut after = String::new();
        loop {
            let store = self.store.clone();
            let cursor = after.clone();
            let page = spawn_blocking(JobKind::StoreRead, move || {
                store.compaction_receipts(&cursor, 256)
            })
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
            if page.is_empty() {
                return Ok(());
            }
            for receipt in page {
                after.clone_from(&receipt.ready.session_ref);
                self.inbox.announce(receipt.ready);
            }
        }
    }

    /// Receipt commit, inbox enqueue, then ACK. In particular, ACK never waits for a model step.
    pub async fn handle(&self, message: &Message, account: &str) -> Result<(), String> {
        if message.subject != event_subject(account, "magic-context", VIEW_READY_EVENT) {
            return Ok(());
        }
        if message.wire_size() > 8192 {
            tracing::info!(target: LOG, message_id = %message.id, bytes = message.wire_size(),
                outcome = "refused", reason = "oversize", "compaction view_ready handled");
            return Err("oversize compaction view_ready".into());
        }
        let Ok(ready) = ViewReady::from_headers(&message.headers) else {
            tracing::info!(target: LOG, message_id = %message.id, outcome = "refused",
                reason = "malformed", "compaction view_ready handled");
            return Err("malformed compaction view_ready".into());
        };
        let handled = |outcome: &str| {
            tracing::info!(target: LOG, session_ref = %ready.session_ref,
                view_version = ready.view_version, outcome, "compaction view_ready handled");
        };
        if message.id != ready.message_id() || message.digest.to_string() != ready.digest {
            handled("identity_mismatch");
            return Err("compaction view_ready identity mismatch".into());
        }
        let Some((_, facts)) = self.session(&ready.session_ref).await? else {
            self.metrics
                .dropped_unknown_session
                .fetch_add(1, Ordering::Relaxed);
            handled("dropped_unknown_session");
            return Ok(());
        };
        if facts.resolved(ready.view_version) {
            handled("already_resolved");
            return Ok(());
        }
        let receipt = ViewReceipt {
            ready: ready.clone(),
            lineage_id: facts.lineage_id,
            received_at_ms: now_ms(),
        };
        let store = self.store.clone();
        let durable = receipt.clone();
        let recorded = spawn_blocking(JobKind::StoreWrite, move || {
            store.receive_compaction_view(&durable)
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        // A redelivery after receipt commit must still refill an inbox lost in a restart.
        #[cfg(feature = "test-clock")]
        receipt_barrier("ViewReceiptRecorded").await?;
        // A receipt replaces the stored one when the lineage changes or, within the same
        // lineage, its view version increases. The store keeps only one receipt per session,
        // so the replaced version is no longer stored; log it here as cleared.
        if let ReceiptWrite::Recorded {
            replaced: Some(old),
        } = &recorded
        {
            let reason = if old.lineage_id == receipt.lineage_id {
                "superseded"
            } else {
                "lineage_changed"
            };
            tracing::info!(target: LOG, session_ref = %ready.session_ref,
                view_version = %old.view_version, reason, "compaction view receipt cleared");
        }
        // `receipt_held`: a receipt for this view or a newer one was already durable, as on a
        // redelivery; the inbox is refilled all the same.
        handled(match recorded {
            ReceiptWrite::Recorded { .. } => "receipted",
            ReceiptWrite::Held => "receipt_held",
        });
        self.inbox.announce(ready);
        Ok(())
    }

    pub async fn settle_delivery(
        &self,
        message: &Message,
        account: &str,
        reader: &mut dyn StreamCursor,
    ) -> Result<(), String> {
        self.metrics.consumed.fetch_add(1, Ordering::Relaxed);
        self.handle(message, account).await?;
        reader.ack().await.map_err(|e| e.to_string())?;
        self.metrics.acked.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "test-clock")]
        receipt_barrier("ViewReadyAcked").await?;
        Ok(())
    }

    async fn session(
        &self,
        reference: &str,
    ) -> Result<Option<(broca_protocol::BindIdentity, CompactionFacts)>, String> {
        let store = self.store.clone();
        let reference = reference.to_owned();
        let refs = spawn_blocking(JobKind::StoreRead, move || {
            store.session_refs(std::iter::once(reference.as_str()))
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        let Some((_, session)) = refs.sessions.values().next() else {
            if refs.complete {
                return Ok(None);
            }
            return Err("compaction session-ref projection is incomplete".into());
        };
        let session = session.clone();
        let lookup_session = session.clone();
        let store = self.store.clone();
        let wal = self.wal.clone();
        let facts = spawn_blocking(JobKind::StoreRead, move || {
            current_facts(&store, wal.as_ref(), &lookup_session)
        })
        .await
        .map_err(|e| e.to_string())??;
        Ok(facts.setup.then_some((session, facts)))
    }

    async fn reconcile_one(&self, receipt: ViewReceipt) -> Result<(), String> {
        let current = self.session(&receipt.ready.session_ref).await?;
        let version = receipt.ready.view_version;
        let reason = match current {
            None => "session_gone",
            Some((_, facts)) if facts.lineage_id != receipt.lineage_id => "lineage_changed",
            Some((session, facts)) => {
                if facts.resolved(version) {
                    self.registry
                        .route_compaction_changed(&session)
                        .await
                        .map_err(|e| e.to_string())?;
                    // A durable notice settled this version: applied or refused by name, or
                    // passed over because a newer version was applied or refused first.
                    if facts.refused == version {
                        "refused"
                    } else if facts.noticed == version {
                        "applied"
                    } else {
                        "superseded"
                    }
                } else {
                    self.inbox.announce(receipt.ready.clone());
                    if self
                        .registry
                        .route_compaction_ready(&session, receipt.ready.clone())
                        .await?
                    {
                        // The idle boundary records both the view and its new estimate before
                        // waking admission. The old estimate cannot release a ceiling-held head.
                        self.registry
                            .route_compaction_changed(&session)
                            .await
                            .map_err(|e| e.to_string())?;
                        // The idle boundary applies the newest announced version, which may
                        // be newer than this receipt's; that one's receipt clears on its own.
                        if self
                            .inbox
                            .newest_ready(&receipt.ready.session_ref, "", Some(version))
                            .is_some()
                        {
                            "superseded"
                        } else {
                            "applied"
                        }
                    } else {
                        // Not applied at the idle boundary (a run owns the session, or the
                        // view was deferred): the receipt stays for a later pass.
                        return Ok(());
                    }
                }
            }
        };
        self.clear(receipt, reason).await
    }

    async fn clear(&self, receipt: ViewReceipt, reason: &str) -> Result<(), String> {
        let ready = receipt.ready.clone();
        let store = self.store.clone();
        spawn_blocking(JobKind::StoreWrite, move || {
            store.clear_compaction_receipt(&receipt)
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        self.inbox.forget_ready(&ready);
        tracing::info!(target: LOG, session_ref = %ready.session_ref,
            view_version = ready.view_version, reason, "compaction view receipt cleared");
        Ok(())
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tokio::join!(self.consume(), self.reconcile());
        })
    }

    /// Reads `view_ready` announcements one delivery per pass. A pass that settled a delivery
    /// goes straight on to the next, so a backlog drains without waiting between messages;
    /// only a failed or timed-out pass waits [`RETRY`] before attaching again.
    async fn consume(&self) {
        let mut cursor: Option<Box<dyn StreamCursor>> = None;
        let mut account = String::new();
        // Paces passes that found nothing waiting. The NATS cursor's `next()` itself blocks for
        // its pull window (two seconds) before it answers "nothing waiting", so by then this tick
        // is already due and costs nothing; it only stops a cursor that answers at once from
        // spinning, holding it to one empty pass a second. An empty pass is not an error and
        // keeps the cursor.
        let mut idle = tokio::time::interval(RETRY);
        idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let pass = async {
                if cursor.is_none() {
                    let connection = self.connector.connect().await?;
                    account = connection.account.account().to_owned();
                    cursor = Some(self.connector.compaction_consumer().await?);
                    self.metrics.attached.store(true, Ordering::Relaxed);
                    tracing::info!(target: LOG, account = %account, durable = DURABLE,
                        "compaction consumer attached");
                }
                let reader = cursor.as_mut().ok_or("compaction cursor absent")?;
                let Some(delivery) = reader.next().await.map_err(|e| e.to_string())? else {
                    return Ok(false);
                };
                self.settle_delivery(&delivery.message, &account, reader.as_mut())
                    .await?;
                Ok::<_, String>(true)
            };
            let outcome = tokio::select! {
                _ = self.stop.cancelled() => break,
                outcome = tokio::time::timeout(Duration::from_secs(15), pass) => outcome,
            };
            match outcome {
                Ok(Ok(true)) => {}
                Ok(Ok(false)) => {
                    tokio::select! { _ = self.stop.cancelled() => break, _ = idle.tick() => {} }
                }
                outcome => {
                    let error = match outcome {
                        Ok(Err(e)) => e,
                        _ => "compaction bus operation timed out".into(),
                    };
                    // Logged once per attachment: a broker that stays down fails before a
                    // cursor exists, and those retries only update the health snapshot.
                    if cursor.take().is_some() {
                        tracing::info!(target: LOG, account = %account, durable = DURABLE,
                            error = %error, "compaction consumer detached; attaching again");
                    }
                    self.metrics.error(error);
                    self.metrics.attached.store(false, Ordering::Relaxed);
                    tokio::select! {
                        _ = self.stop.cancelled() => break,
                        _ = tokio::time::sleep(RETRY) => {}
                    }
                }
            }
        }
        self.metrics.attached.store(false, Ordering::Relaxed);
    }

    async fn reconcile(&self) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut after = String::new();
        // Retry each receipt at most once every thirty seconds.
        // The bounded page is revisited after a full pass; no receipt is removed by a timeout.
        let mut attempts = std::collections::HashMap::new();
        loop {
            tokio::select! { _ = self.stop.cancelled() => break, _ = tick.tick() => {} }
            let store = self.store.clone();
            let start = after.clone();
            let page = match spawn_blocking(JobKind::StoreRead, move || {
                store.compaction_receipts(&start, 32)
            })
            .await
            {
                Ok(Ok(page)) => page,
                Ok(Err(e)) => {
                    self.metrics.error(e.to_string());
                    continue;
                }
                Err(e) => {
                    self.metrics.error(e.to_string());
                    continue;
                }
            };
            if page.is_empty() {
                after.clear();
            }
            for receipt in page {
                after.clone_from(&receipt.ready.session_ref);
                let key = (
                    receipt.ready.session_ref.clone(),
                    receipt.ready.view_version,
                );
                let now = tokio::time::Instant::now();
                attempts.retain(|_, at| now.duration_since(*at) < Duration::from_secs(30));
                if attempts.contains_key(&key) {
                    continue;
                }
                if attempts.len() >= 1000 {
                    break;
                }
                attempts.insert(key, now);
                tokio::select! {
                    _ = self.stop.cancelled() => return,
                    outcome = tokio::time::timeout(Duration::from_secs(10), self.reconcile_one(receipt)) => match outcome {
                        Ok(Ok(())) => {},
                        Ok(Err(e)) => self.metrics.error(e),
                        Err(_) => self.metrics.error("compaction idle boundary timed out".into()),
                    }
                }
            }
        }
    }
}

#[cfg(feature = "test-clock")]
async fn receipt_barrier(point: &str) -> Result<(), String> {
    let root = match crate::state_root::resolve_state_root() {
        Ok(root) if root.origin == crate::state_root::StateRootOrigin::Explicit => root.path,
        _ => return Ok(()),
    };
    let gate = root.join("test-compaction-bus-gate");
    if std::fs::read_to_string(&gate).ok().as_deref() != Some(point) {
        return Ok(());
    }
    let reached = root.join("test-compaction-bus-reached");
    let temp = reached.with_extension("tmp");
    std::fs::write(&temp, point)
        .and_then(|()| std::fs::rename(&temp, &reached))
        .map_err(|e| e.to_string())?;
    while gate.exists() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

const FACTS_TAIL_BYTES: u64 = 256 * 1024;

fn current_facts(
    store: &SqliteStore,
    wal: &dyn broca_core::WalStore,
    session: &broca_protocol::BindIdentity,
) -> Result<CompactionFacts, String> {
    let facts = store.compaction_facts(session).map_err(|e| e.to_string())?;
    let identity = wal.file_identity(session).map_err(|e| e.to_string())?;
    let bytes = wal.live_wal_bytes(session).map_err(|e| e.to_string())?;
    if let Some(facts) = facts.filter(|f| f.current_file(identity, bytes)) {
        return Ok(facts);
    }
    // Projection lag must stay cheap even when a message frame is larger than the
    // suffix budget. Unknown suffixes retry later; they never widen to a full read.
    let range = wal
        .read_records_tail_bounded(session, FACTS_TAIL_BYTES)
        .map_err(|e| e.to_string())?
        .ok_or("compaction facts projection is behind the bounded WAL suffix")?;
    let flushed = wal.flush_for_read(session).map_err(|e| e.to_string())?;
    if range
        .next_anchor
        .is_some_and(|a| Some(a.file_id) != flushed)
    {
        return Err("compaction WAL changed during its durability proof".into());
    }
    store
        .project_compaction_facts(session, &range)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "compaction facts projection cannot cover the bounded WAL suffix".into())
}

pub async fn apply_idle(
    session: &broca_protocol::BindIdentity,
    writer: &broca_core::WalHandle,
    records: &[broca_wal::WalRecord],
    ready: &ViewReady,
    plane: &Arc<dyn broca_core::CompactionPlane>,
    views: Option<Arc<dyn broca_core::CompactionViews>>,
) -> Result<bool, String> {
    // The view is not offered at all; `apply_idle_view` logs the decision when it is.
    let deferred = |reason: &str| -> Result<bool, String> {
        tracing::info!(target: LOG, session_ref = %ready.session_ref,
            view_version = ready.view_version, choice = "deferred", reason,
            "compaction view decided at the idle boundary");
        Ok(false)
    };
    let Some((config, run_id)) = records.iter().rev().find_map(|record| match record {
        broca_wal::WalRecord::RunStarted { config, run_id, .. } => Some((config.as_ref(), run_id)),
        _ => None,
    }) else {
        return deferred("no_run");
    };
    let prior = broca_core::replay::replay(records).map_err(|e| e.to_string())?;
    // A parked or crash-interrupted episode must first replay its recorded boundary.
    // Actor idleness alone does not make its in-flight request safe to rewrite.
    if !prior.run_finished_durable {
        return deferred("run_unfinished");
    }
    let mut raw = prior.prompt;
    broca_protocol::content::stamp_ordinals(&mut raw);
    let (frozen, opts, checkpoint) =
        crate::wal_render::boundary_options(records, config, &run_id.0, 0)?;
    let broca_core::compaction::CompactionCheckpoint::State(mut state) = checkpoint else {
        return deferred("no_compaction_state");
    };
    let family = broca_provider::registry::wire_family(frozen.family_id)
        .ok_or("compaction wire renderer absent")?;
    broca_core::compaction::apply_idle_view(
        session,
        writer,
        plane,
        views,
        ready,
        &mut state,
        &opts.prompt,
        &raw,
        frozen.family_id.thinking_is_prefix_bound(),
        &config.model.model_id,
        config.context_limit,
        |state| {
            broca_core::compaction::model_options(&opts, state)
                .map(|opts| family.render(&frozen, &opts).len())
                .map_err(|e| e.to_string())
        },
    )
    .await
    .map_err(|e| e.to_string())
}

#[cfg(test)]
#[path = "compaction_bus/tests.rs"]
mod tests;
