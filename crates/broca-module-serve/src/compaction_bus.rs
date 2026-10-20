//! Bounded, durable intake of compaction claim checks. Broker failure never gates a run.
use crate::bus_events::{now_ms, BusConnector};
use broca_core::compaction_provider::notice::{event_subject, ViewReady, VIEW_READY_EVENT};
use broca_diagnostics::{spawn_blocking, JobKind};
use broca_module::LineageRegistry;
use broca_store::{compaction_receipts::ViewReceipt, SqliteStore};
use cortexkit_bus_trait::{Message, StreamCursor};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

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
    inbox: Arc<broca_core::ViewReadyInbox>,
    connector: Arc<dyn BusConnector>,
    stop: CancellationToken,
    pub metrics: Arc<Metrics>,
}
impl Service {
    pub fn new(
        store: Arc<SqliteStore>,
        registry: LineageRegistry,
        inbox: Arc<broca_core::ViewReadyInbox>,
        connector: Arc<dyn BusConnector>,
        stop: CancellationToken,
    ) -> Self {
        let metrics = Arc::new(Metrics::default());
        let _ = METRICS.set(metrics.clone());
        Self {
            store,
            registry,
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
            return Err("oversize compaction view_ready".into());
        }
        let ready = ViewReady::from_headers(&message.headers)
            .map_err(|_| "malformed compaction view_ready")?;
        if message.id != ready.message_id() || message.digest.to_string() != ready.digest {
            return Err("compaction view_ready identity mismatch".into());
        }
        let Some((session, lineage, records)) = self.session(&ready.session_ref).await? else {
            self.metrics
                .dropped_unknown_session
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        };
        if resolved(&ready, &records) {
            return Ok(());
        }
        let receipt = ViewReceipt {
            ready: ready.clone(),
            lineage_id: lineage,
            received_at_ms: now_ms(),
        };
        let store = self.store.clone();
        let durable = receipt.clone();
        spawn_blocking(JobKind::StoreWrite, move || {
            store.receive_compaction_view(&durable)
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        // A redelivery after receipt commit must still refill an inbox lost in a restart.
        #[cfg(feature = "test-clock")]
        receipt_barrier("ViewReceiptRecorded").await?;
        self.inbox.announce(ready);
        let _ = session;
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
    ) -> Result<
        Option<(
            broca_protocol::BindIdentity,
            String,
            Vec<broca_wal::FramedRecord>,
        )>,
        String,
    > {
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
        let (records, lineage) = self
            .registry
            .read_durable_wal(session)
            .await
            .map_err(|e| e.to_string())?;
        let Some(lineage) = lineage else {
            return Ok(None);
        };
        if !records.iter().any(|r| {
            matches!(
                &r.record,
                broca_wal::WalRecord::CompactionMessage {
                    kind: broca_wal::CompactionMessageKind::Setup,
                    ..
                }
            )
        }) {
            return Ok(None);
        }
        Ok(Some((session.clone(), lineage.to_string(), records)))
    }

    async fn reconcile_one(&self, receipt: ViewReceipt) -> Result<(), String> {
        let current = self.session(&receipt.ready.session_ref).await?;
        let clear = match current {
            None => true,
            Some((_, lineage, _)) if lineage != receipt.lineage_id => true,
            Some((session, _, records)) => {
                if resolved(&receipt.ready, &records) {
                    self.registry
                        .route_compaction_changed(&session)
                        .await
                        .map_err(|e| e.to_string())?;
                    true
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
                        return self.clear(receipt).await;
                    }
                    false
                }
            }
        };
        if clear {
            self.clear(receipt).await?;
        }
        Ok(())
    }

    async fn clear(&self, receipt: ViewReceipt) -> Result<(), String> {
        let ready = receipt.ready.clone();
        let store = self.store.clone();
        spawn_blocking(JobKind::StoreWrite, move || {
            store.clear_compaction_receipt(&receipt)
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        self.inbox.forget_ready(&ready);
        Ok(())
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tokio::join!(self.consume(), self.reconcile());
        })
    }

    async fn consume(&self) {
        let mut cursor: Option<Box<dyn StreamCursor>> = None;
        let mut account = String::new();
        let mut retry = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! { _ = self.stop.cancelled() => break, _ = retry.tick() => {} }
            let pass = async {
                if cursor.is_none() {
                    let connection = self.connector.connect().await?;
                    account = connection.account.account().to_owned();
                    cursor = Some(self.connector.compaction_consumer().await?);
                    self.metrics.attached.store(true, Ordering::Relaxed);
                }
                let reader = cursor.as_mut().ok_or("compaction cursor absent")?;
                if let Some(delivery) = reader.next().await.map_err(|e| e.to_string())? {
                    self.settle_delivery(&delivery.message, &account, reader.as_mut())
                        .await?;
                }
                Ok::<_, String>(())
            };
            tokio::select! {
                _ = self.stop.cancelled() => break,
                outcome = tokio::time::timeout(Duration::from_secs(15), pass) => match outcome {
                    Ok(Ok(())) => {},
                    outcome => {
                        self.metrics.error(match outcome { Ok(Err(e)) => e, _ => "compaction bus operation timed out".into() });
                        self.metrics.attached.store(false, Ordering::Relaxed);
                        cursor = None;
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

fn resolved(ready: &ViewReady, records: &[broca_wal::FramedRecord]) -> bool {
    records.iter().any(|frame| match &frame.record {
        broca_wal::WalRecord::CompactionNotice { headers, .. } => {
            headers
                .get("applied_view_version")
                .and_then(|v| v.parse::<u64>().ok())
                .is_some_and(|v| v >= ready.view_version)
                || headers.get("events").is_some_and(|events| {
                    events.split(',').any(|event| {
                        event.starts_with(&format!("view_refused:{}:", ready.view_version))
                    })
                })
        }
        _ => false,
    })
}

pub async fn apply_idle(
    session: &broca_protocol::BindIdentity,
    writer: &broca_core::WalHandle,
    records: &[broca_wal::WalRecord],
    ready: &ViewReady,
    plane: &Arc<dyn broca_core::CompactionPlane>,
) -> Result<bool, String> {
    let Some((config, run_id)) = records.iter().rev().find_map(|record| match record {
        broca_wal::WalRecord::RunStarted { config, run_id, .. } => Some((config.as_ref(), run_id)),
        _ => None,
    }) else {
        return Ok(false);
    };
    let prior = broca_core::replay::replay(records).map_err(|e| e.to_string())?;
    // A parked or crash-interrupted episode must first replay its recorded boundary.
    // Actor idleness alone does not make its in-flight request safe to rewrite.
    if !prior.run_finished_durable {
        return Ok(false);
    }
    let mut raw = prior.prompt;
    broca_protocol::content::stamp_ordinals(&mut raw);
    let (frozen, opts, checkpoint) =
        crate::wal_render::boundary_options(records, config, &run_id.0, 0)?;
    let broca_core::compaction::CompactionCheckpoint::State(mut state) = checkpoint else {
        return Ok(false);
    };
    let family = broca_provider::registry::wire_family(frozen.family_id)
        .ok_or("compaction wire renderer absent")?;
    broca_core::compaction::apply_idle_view(
        session,
        writer,
        plane,
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
