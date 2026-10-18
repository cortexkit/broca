//! WAL-to-outbox projection and the asynchronous ck-bus claim-check publisher.

use async_trait::async_trait;
use broca_core::{FileWalStore, WalStore};
use broca_module::{
    serve::{ServeHandler, StatusResponse},
    LineageRegistry,
};
use broca_protocol::{
    run_events::{RunEvent, RunEventDetail},
    BindIdentity,
};
use broca_store::{
    bus_events::{BusPublisher, DrainResult, Metrics},
    SqliteStore,
};
use cortexkit_bus_naming::AccountNames;
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

static METRICS: OnceLock<Arc<Metrics>> = OnceLock::new();

/// Whether this process started the publisher. With the switch off every counter reads zero,
/// which is indistinguishable from an enabled publisher that has had nothing to send, so the
/// health block states which of the two an operator is looking at.
static ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn health() -> serde_json::Value {
    let mut snapshot = METRICS
        .get()
        .map(|m| m.snapshot())
        .unwrap_or_else(|| Metrics::default().snapshot());
    if let Some(fields) = snapshot.as_object_mut() {
        fields.insert(
            "enabled".into(),
            serde_json::Value::Bool(ENABLED.load(Ordering::Relaxed)),
        );
    }
    snapshot
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Config {
    pub enabled: bool,
}

impl Config {
    pub fn load_default_path() -> Result<Self, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
            crate::config_home::ConfigHome::Resolved { path, .. } => {
                match std::fs::read_to_string(&path) {
                    Ok(text) => Self::parse(&text)
                        .map_err(|e| format!("bus events config {}: {e}", path.display())),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
                    Err(e) => Err(format!("read bus events config {}: {e}", path.display())),
                }
            }
        }
    }
    fn parse(text: &str) -> Result<Self, String> {
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let config: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&text).map_err(|e| e.to_string())?;
        match config.get("bus_run_events_enabled") {
            None => Ok(Self::default()),
            Some(value) => value
                .as_bool()
                .map(|enabled| Self { enabled })
                .ok_or_else(|| "bus_run_events_enabled must be a boolean".into()),
        }
    }
}

/// Hints contain no record bodies. Overflow and process loss both request WAL recovery.
#[derive(Default)]
pub struct Hints(Mutex<HintState>);
#[derive(Default)]
struct HintState {
    heads: DurableHeads,
    recovery: bool,
}
type DurableHeads = HashMap<BindIdentity, (u64, String)>;
impl Hints {
    pub fn observer(self: &Arc<Self>) -> Arc<broca_core::wal_writer::DurableWalObserver> {
        let hints = self.clone();
        Arc::new(move |session, seq, epoch| {
            let Some(epoch) = epoch else {
                return;
            };
            let mut state = hints.0.lock().unwrap();
            if state.heads.len() >= 4096 && !state.heads.contains_key(session) {
                state.recovery = true;
            } else {
                state
                    .heads
                    .insert(session.clone(), (seq, epoch.to_string()));
            }
        })
    }
    fn take(&self) -> (DurableHeads, bool) {
        let mut state = self.0.lock().unwrap();
        (
            std::mem::take(&mut state.heads),
            std::mem::take(&mut state.recovery),
        )
    }
}

/// A transport connection with its machine account and next credential refresh deadline.
pub struct BusConnection {
    pub publisher: Arc<dyn BusPublisher>,
    pub account: AccountNames,
    pub refresh_at_ms: u64,
}

#[async_trait]
pub trait BusConnector: Send + Sync {
    async fn connect(&self) -> Result<BusConnection, String>;
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn recover_heads(store: &SqliteStore, wal: &FileWalStore) -> Result<(DurableHeads, bool), String> {
    let mut found = DurableHeads::new();
    let mut incomplete = false;
    for session in store.bus_event_sessions().map_err(|e| e.to_string())? {
        let head = match wal.flush_for_read(&session) {
            Ok(Some(head)) => head,
            Ok(None) => continue,
            Err(broca_wal::WalError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                continue
            }
            Err(_) => {
                incomplete = true;
                continue;
            }
        };
        match wal.read_records_range(&session, 0, Some(head), None) {
            Ok(range) => {
                if let Some(epoch) = range.lineage_id {
                    found.insert(session, (head, epoch.to_string()));
                }
            }
            Err(_) => {
                incomplete = true;
            }
        }
    }
    Ok((found, incomplete))
}

pub struct Service {
    pub config: Config,
    pub store: Arc<SqliteStore>,
    pub wal: Arc<FileWalStore>,
    pub registry: LineageRegistry,
    pub connector: Option<Arc<dyn BusConnector>>,
    pub hints: Arc<Hints>,
    pub stop: CancellationToken,
}

impl Service {
    pub fn spawn(self) -> Option<tokio::task::JoinHandle<()>> {
        let Self {
            config,
            store,
            wal,
            registry,
            connector,
            hints,
            stop,
        } = self;
        let _ = METRICS.set(store.bus_metrics_arc());
        if !config.enabled {
            return None;
        }
        ENABLED.store(true, Ordering::Relaxed);
        Some(tokio::spawn(async move {
            let mut publisher = None;
            let mut account_known = false;
            let mut refresh_at_ms = 0;
            let mut recovery = true;
            let mut recovery_at = tokio::time::Instant::now();
            let mut retry_at = tokio::time::Instant::now();
            let mut backoff = Duration::from_secs(1);
            let mut progress = ProgressThrottle::default();
            let mut progress_at = tokio::time::Instant::now();
            let mut prune_at = tokio::time::Instant::now();
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {_ = stop.cancelled()=>break,_ = tick.tick()=>{}}
                let now = now_ms();
                if publisher.is_none() && tokio::time::Instant::now() >= retry_at {
                    let result = async {
                        let connection = connector
                            .as_ref()
                            .ok_or("run event bus transport is not installed")?
                            .connect()
                            .await?;
                        let account = &connection.account;
                        for name in [
                            "run.started",
                            "run.paused",
                            "run.ended",
                            "run.steer_consumed",
                            "run.progress",
                        ] {
                            account
                                .event_subject("broca", name, 1)
                                .map_err(|_| "bus naming client refuses dotted run events")?;
                        }
                        store.enable_bus_events(account.account())?;
                        account_known = true;
                        refresh_at_ms = connection.refresh_at_ms;
                        Ok::<_, String>(connection.publisher)
                    }
                    .await;
                    match result {
                        Ok(connected) => {
                            publisher = Some(connected);
                            recovery = true;
                            backoff = Duration::from_secs(1);
                        }
                        Err(_) => {
                            store
                                .bus_metrics()
                                .bus_publish_failures
                                .fetch_add(1, Ordering::Relaxed);
                            retry_at = tokio::time::Instant::now() + backoff;
                            backoff = (backoff * 2).min(Duration::from_secs(60));
                        }
                    }
                }
                if publisher.is_some() && now >= refresh_at_ms {
                    publisher = None;
                    retry_at = tokio::time::Instant::now();
                }
                let (heads, overflow) = hints.take();
                recovery |= overflow;
                // Once an account is known, WAL projection continues during a broker outage.
                if account_known {
                    let mut jobs = heads;
                    if recovery && tokio::time::Instant::now() >= recovery_at {
                        let store = store.clone();
                        let wal = wal.clone();
                        if let Ok(Ok((found, incomplete))) = broca_diagnostics::spawn_blocking(
                            broca_diagnostics::JobKind::WalRead,
                            move || recover_heads(&store, &wal),
                        )
                        .await
                        {
                            jobs.extend(found);
                            recovery = incomplete;
                            if incomplete {
                                tracing::warn!(target: "bus", "run event recovery has unreadable WALs; healthy sessions continue");
                            }
                        }
                        recovery_at = tokio::time::Instant::now() + Duration::from_secs(60);
                    }
                    for (session, (head, epoch)) in jobs {
                        let s = store.clone();
                        let w = wal.clone();
                        if !matches!(
                            broca_diagnostics::spawn_blocking(
                                broca_diagnostics::JobKind::Projection,
                                move || {
                                    let after = s
                                        .bus_event_position(&session, &epoch)
                                        .map_err(|e| e.to_string())?;
                                    let range = w
                                        .read_records_range(&session, after, Some(head), None)
                                        .map_err(|e| e.to_string())?;
                                    if range.lineage_id.is_none_or(|id| id.to_string() != epoch) {
                                        return Err(
                                            "WAL epoch changed during run event projection".into(),
                                        );
                                    }
                                    let broca_wal::WalRangeRecords::Complete(records) =
                                        range.into_records()
                                    else {
                                        return Err("run event WAL is absent".into());
                                    };
                                    s.project_bus_events(&session, &epoch, &records, now)
                                        .map_err(|e| e.to_string())
                                }
                            )
                            .await,
                            Ok(Ok(()))
                        ) {
                            recovery = true;
                            store
                                .bus_metrics()
                                .bus_publish_failures
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(target: "bus", "run event projection failed; scheduling WAL recovery");
                        }
                    }
                } else if !heads.is_empty() {
                    recovery = true;
                }
                if tokio::time::Instant::now() >= prune_at {
                    let _ = store.prune_bus_events(now);
                    prune_at = tokio::time::Instant::now() + Duration::from_secs(60);
                }
                if let Some(bus) = publisher.as_ref() {
                    if tokio::time::Instant::now() >= retry_at {
                        // One sequential publisher preserves each run's WAL order through retries.
                        for _ in 0..64 {
                            match store.drain_bus_event(bus.as_ref(), now).await {
                                Ok(DrainResult::Published) => {
                                    backoff = Duration::from_secs(1);
                                }
                                Ok(DrainResult::Empty) => break,
                                _ => {
                                    retry_at = tokio::time::Instant::now() + backoff;
                                    backoff = (backoff * 2).min(Duration::from_secs(60));
                                    break;
                                }
                            }
                        }
                    }
                    if tokio::time::Instant::now() < progress_at {
                        continue;
                    }
                    progress_at = tokio::time::Instant::now() + Duration::from_secs(30);
                    for session in registry.active_event_sessions() {
                        if let Ok(StatusResponse::Active { run_id, .. }) =
                            registry.status(&session).await
                        {
                            if let Ok(broca_wire::RunStatusResponse::Active {
                                step: Some(step),
                                ..
                            }) = registry
                                .run_status(&session, broca_protocol::RunId(run_id.clone()))
                                .await
                            {
                                if !progress.due(&run_id, tokio::time::Instant::now()) {
                                    continue;
                                }
                                let range = match wal.read_records_range(&session, 0, Some(0), None)
                                {
                                    Ok(r) => r,
                                    Err(_) => continue,
                                };
                                let Some(epoch) = range.lineage_id else {
                                    continue;
                                };
                                let epoch = epoch.to_string();
                                let seq = store.bus_event_position(&session, &epoch).unwrap_or(0);
                                if seq == 0 {
                                    continue;
                                }
                                let event = RunEvent {
                                    v: 1,
                                    session_ref: broca_core::wal_writer::agent_session_ref(
                                        &session,
                                    ),
                                    run_id,
                                    seq,
                                    at_ms: now,
                                    detail: RunEventDetail::Progress {
                                        step: step.step_id.0,
                                        last_content_at_ms: step.last_content_at_ms,
                                        max_content_gap_ms: step.max_content_gap_ms,
                                    },
                                };
                                if let Ok(Some(event)) =
                                    store.retain_bus_progress(&session, &epoch, &event, now)
                                {
                                    match bus.publish(&event).await {
                                        Ok(receipt) => {
                                            let _ =
                                                store.acknowledge_bus_event(&event, receipt, now);
                                        }
                                        Err(_) => {
                                            store
                                                .bus_metrics()
                                                .bus_publish_failures
                                                .fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    progress.prune();
                }
            }
        }))
    }
}

#[derive(Default)]
struct ProgressThrottle(HashMap<String, tokio::time::Instant>);
impl ProgressThrottle {
    fn due(&mut self, run: &str, now: tokio::time::Instant) -> bool {
        if self
            .0
            .get(run)
            .is_some_and(|last| now.duration_since(*last) < Duration::from_secs(120))
        {
            return false;
        }
        self.0.insert(run.into(), now);
        true
    }
    fn prune(&mut self) {
        let now = tokio::time::Instant::now();
        self.0
            .retain(|_, last| now.duration_since(*last) < Duration::from_secs(240));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bus_event_config_is_off_by_default_and_strict() {
        assert!(!Config::default().enabled);
        assert!(!Config::parse("{}").unwrap().enabled);
        assert!(
            !Config::parse(r#"{"bus_run_events_enabled":false}"#)
                .unwrap()
                .enabled
        );
        assert!(
            Config::parse(r#"{"bus_run_events_enabled":true}"#)
                .unwrap()
                .enabled
        );
        for value in ["1", "null", r#""true""#] {
            assert!(Config::parse(&format!(r#"{{"bus_run_events_enabled":{value}}}"#)).is_err());
        }
    }

    #[test]
    fn bus_event_consumer_declaration_tracks_the_switch() {
        use subc_protocol::manifest::ConsumerRole;
        let has_bus = |enabled| {
            crate::consumer_routes::ConsumerRoutes::default().with_bus_events(enabled).consumer_roles().iter().any(|role| matches!(role,ConsumerRole::ServiceClient {of} if of.iter().any(|id|id=="ckbus")))
        };
        assert!(!has_bus(false));
        assert!(has_bus(true));
    }

    #[test]
    fn bus_recovery_missing_session_does_not_hide_healthy_sessions() {
        use broca_core::{RunIndex, RunIndexRow, RunIndexState};
        let root = broca_test_support::scratch_root().join(format!("bus-recovery-{}", now_ms()));
        let wal = FileWalStore::new(root.join("wal"));
        let store = SqliteStore::open_in_memory().unwrap();
        let missing = BindIdentity::new("/test", "test", "missing");
        let healthy = BindIdentity::new("/test", "test", "healthy");
        for session in [&missing, &healthy] {
            store
                .index_run_active(&RunIndexRow {
                    run_id: session.session.clone(),
                    session: session.clone(),
                    submission_id: None,
                    request_identity_digest: None,
                    state: RunIndexState::Active,
                    usage: None,
                    final_step_finish_reason: None,
                    retries_used: 0,
                    title: None,
                    withdrawals_owed: false,
                })
                .unwrap();
        }
        let writer = wal.open(&healthy).unwrap();
        writer.sync().unwrap();
        drop(writer);
        let (heads, incomplete) = recover_heads(&store, &wal).unwrap();
        assert!(!incomplete);
        assert!(heads.contains_key(&healthy));
        assert!(!heads.contains_key(&missing));
    }

    #[tokio::test(start_paused = true)]
    async fn bus_progress_throttle_is_per_run_at_least_two_minutes() {
        let mut throttle = ProgressThrottle::default();
        let now = tokio::time::Instant::now();
        assert!(throttle.due("a", now));
        assert!(throttle.due("b", now));
        assert!(!throttle.due("a", now + Duration::from_secs(119)));
        assert!(throttle.due("a", now + Duration::from_secs(120)));
        assert!(!throttle.due("a", now + Duration::from_secs(121)));
    }
}
