//! Run events from session writers into the outbox, and the asynchronous ck-bus claim-check
//! publisher that drains it.

use async_trait::async_trait;
use broca_module::{
    serve::{ServeHandler, StatusResponse},
    LineageRegistry,
};
use broca_protocol::{
    run_events::{RunEvent, RunEventDetail},
    send_states::SendStateNotice,
    BindIdentity,
};
use broca_store::{
    bus_events::RunEventStore,
    bus_events::{drain_bus_event, BusPublisher, DrainResult, Metrics},
};
use cortexkit_bus_naming::AccountNames;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

static METRICS: OnceLock<Arc<Metrics>> = OnceLock::new();

/// Whether this process started the publisher. With the switch off every counter reads zero,
/// which is indistinguishable from an enabled publisher that has had nothing to send, so the
/// health block states which of the two an operator is looking at.
static ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Whether the enabled publisher is actually doing its work, as opposed to merely running.
///
/// The counters in [`Metrics`] only move once events reach the outbox, so a publisher that is
/// stuck before that point reads exactly like an idle one: every number zero while runs flow.
/// These readings separate the stages: batches waiting with none recorded means events never
/// reach the outbox; a loop age that keeps growing means one pass of the loop is stuck.
struct Liveness {
    /// Whether the machine account is known, which is what recording events waits for.
    account_known: std::sync::atomic::AtomicBool,
    /// When the loop last began a pass, on `broca_diagnostics`' monotonic clock; zero before the
    /// first pass.
    pass_started_ms: AtomicU64,
    /// Batches of run events from sessions in the new storage recorded in the outbox.
    store_batches_recorded: AtomicU64,
    /// Batches of them waiting to be recorded, because the machine account isn't known yet or
    /// recording failed. A count that stays above zero means none reach the outbox.
    store_batches_waiting: AtomicU64,
    /// Batches of them dropped because too many were waiting.
    store_batches_dropped: AtomicU64,
}

impl Liveness {
    const fn new() -> Self {
        Self {
            account_known: std::sync::atomic::AtomicBool::new(false),
            pass_started_ms: AtomicU64::new(0),
            store_batches_recorded: AtomicU64::new(0),
            store_batches_waiting: AtomicU64::new(0),
            store_batches_dropped: AtomicU64::new(0),
        }
    }

    fn pass_started(&self) {
        self.pass_started_ms
            .store(broca_diagnostics::now_ms(), Ordering::Relaxed);
    }
}

static LIVENESS: Liveness = Liveness::new();

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
        let count = |value: &AtomicU64| serde_json::Value::from(value.load(Ordering::Relaxed));
        fields.insert(
            "bus_account_known".into(),
            serde_json::Value::Bool(LIVENESS.account_known.load(Ordering::Relaxed)),
        );
        fields.insert(
            "bus_store_batches_recorded".into(),
            count(&LIVENESS.store_batches_recorded),
        );
        fields.insert(
            "bus_store_batches_waiting".into(),
            count(&LIVENESS.store_batches_waiting),
        );
        fields.insert(
            "bus_store_batches_dropped".into(),
            count(&LIVENESS.store_batches_dropped),
        );
        // Milliseconds since the loop began its latest pass. A running loop begins one every
        // quarter second, so a value that keeps growing means a pass is stuck. Null when no
        // loop has started, which is always the case with `bus_run_events_enabled` off.
        let started = LIVENESS.pass_started_ms.load(Ordering::Relaxed);
        fields.insert(
            "bus_loop_pass_age_ms".into(),
            (started != 0)
                .then(|| broca_diagnostics::now_ms().saturating_sub(started))
                .into(),
        );
    }
    snapshot
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Config {
    pub enabled: bool,
}

impl Config {
    /// Its section of broca's settings.
    pub fn from_settings(settings: &crate::settings::Settings) -> Result<Self, String> {
        Self::parse(settings.text())
            .map_err(|error| format!("bus events config {}: {error}", settings.source()))
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

/// The run events of sessions kept in the new storage (see [`broca_module::store_run_events`]),
/// on their way to the outbox.
///
/// A session's writer hands each durable batch's events here, and they are recorded in the
/// outbox there and then, before the writer goes on. So a run's `run.ended` is in the outbox
/// before its end can retire the session's recovery registration, and a crash can lose it only
/// between the barrier and that write. Recording needs the machine account, which is known once
/// the bus has connected; until then the events wait here, in order, and the publisher's loop
/// records them. Once any wait, later ones queue behind them, keeping each session's order.
pub struct StoreRunEvents {
    store: Arc<dyn RunEventStore>,
    waiting: Mutex<VecDeque<WaitingEvents>>,
    /// Send state notices (see [`broca_module::store_send_states`]) that wait for the account,
    /// oldest first, each batch with the module each notice is for.
    waiting_sends: Mutex<VecDeque<Vec<(String, SendStateNotice)>>>,
    /// Each session's epoch and the seq of its last durable record, which a `run.progress`
    /// beat carries as its `seq`.
    positions: Mutex<HashMap<BindIdentity, (String, u64)>>,
}

struct WaitingEvents {
    session: BindIdentity,
    epoch: String,
    events: Vec<RunEvent>,
}

/// The most batches that wait for the account. Past it the oldest are dropped: a module that
/// can't reach the bus for that long has readers on their fallback polls already.
const STORE_EVENTS_WAITING_CAP: usize = 10_000;

impl StoreRunEvents {
    pub fn new(store: Arc<dyn RunEventStore>) -> Arc<Self> {
        Arc::new(Self {
            store,
            waiting: Mutex::default(),
            waiting_sends: Mutex::default(),
            positions: Mutex::default(),
        })
    }

    /// Records the batches waiting, oldest first: `false` while the account isn't known or a
    /// recording failed, leaving the rest waiting.
    fn record_waiting(&self, now_ms: u64) -> bool {
        let mut waiting = self.waiting.lock().unwrap();
        while let Some(batch) = waiting.front_mut() {
            match self.store.record_run_events(
                &batch.session,
                &batch.epoch,
                &mut batch.events,
                now_ms,
            ) {
                Ok(true) => {
                    waiting.pop_front();
                    LIVENESS
                        .store_batches_recorded
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(false) => break,
                Err(error) => {
                    tracing::warn!(target: "bus", %error, "could not record waiting run events; retrying on the next pass");
                    break;
                }
            }
        }
        LIVENESS
            .store_batches_waiting
            .store(waiting.len() as u64, Ordering::Relaxed);
        let mut sends = self.waiting_sends.lock().unwrap();
        while let Some(batch) = sends.front() {
            match self.store.record_send_states(batch, now_ms) {
                Ok(true) => {
                    sends.pop_front();
                }
                Ok(false) => break,
                Err(error) => {
                    tracing::warn!(target: "bus", %error, "could not record waiting send state notices; retrying on the next pass");
                    break;
                }
            }
        }
        waiting.is_empty() && sends.is_empty()
    }

    /// The session's epoch and last durable seq, while its writer has told one.
    fn position(&self, session: &BindIdentity) -> Option<(String, u64)> {
        self.positions.lock().unwrap().get(session).cloned()
    }

    /// Forgets the positions of sessions with no run in flight, which no beat names.
    fn keep_positions(&self, active: &HashSet<BindIdentity>) {
        self.positions
            .lock()
            .unwrap()
            .retain(|session, _| active.contains(session));
    }
}

impl broca_module::store_send_states::SendStateSink for StoreRunEvents {
    fn durable(
        &self,
        _session: &BindIdentity,
        changes: Vec<broca_module::store_send_states::SendStateChange>,
    ) {
        let changes: Vec<(String, SendStateNotice)> = changes
            .into_iter()
            .map(|change| (change.sender, change.notice))
            .collect();
        let mut waiting = self.waiting_sends.lock().unwrap();
        if waiting.is_empty() {
            match self.store.record_send_states(&changes, now_ms()) {
                Ok(true) => return,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(target: "bus", %error, "could not record send state notices; they wait for the publisher's next pass");
                }
            }
        }
        if waiting.len() >= STORE_EVENTS_WAITING_CAP {
            waiting.pop_front();
            tracing::warn!(target: "bus", "too many send state batches wait for the bus; dropping the oldest");
        }
        waiting.push_back(changes);
    }
}

impl broca_module::store_run_events::RunEventSink for StoreRunEvents {
    fn durable(
        &self,
        session: &BindIdentity,
        epoch: &str,
        through: u64,
        mut events: Vec<RunEvent>,
    ) {
        self.positions
            .lock()
            .unwrap()
            .insert(session.clone(), (epoch.to_owned(), through));
        if events.is_empty() {
            return;
        }
        let mut waiting = self.waiting.lock().unwrap();
        if waiting.is_empty() {
            match self
                .store
                .record_run_events(session, epoch, &mut events, now_ms())
            {
                Ok(true) => {
                    LIVENESS
                        .store_batches_recorded
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(target: "bus", %error, "could not record run events; they wait for the publisher's next pass");
                }
            }
        }
        if waiting.len() >= STORE_EVENTS_WAITING_CAP {
            waiting.pop_front();
            LIVENESS
                .store_batches_dropped
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(target: "bus", "too many run event batches wait for the bus; dropping the oldest");
        }
        waiting.push_back(WaitingEvents {
            session: session.clone(),
            epoch: epoch.to_owned(),
            events,
        });
        LIVENESS
            .store_batches_waiting
            .store(waiting.len() as u64, Ordering::Relaxed);
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
    async fn compaction_consumer(
        &self,
    ) -> Result<Box<dyn cortexkit_bus_trait::StreamCursor>, String> {
        Err("compaction bus consumer unavailable".into())
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Longest a single pass of the publisher loop spends publishing from the outbox.
const DRAIN_SLICE: Duration = Duration::from_millis(200);

pub struct Service {
    pub config: Config,
    pub store: Arc<dyn RunEventStore>,
    pub registry: LineageRegistry,
    pub connector: Option<Arc<dyn BusConnector>>,
    /// The run events of sessions in the new storage, when they are installed.
    pub store_events: Option<Arc<StoreRunEvents>>,
    pub stop: CancellationToken,
}

impl Service {
    pub fn spawn(self) -> Option<tokio::task::JoinHandle<()>> {
        let Self {
            config,
            store,
            registry,
            connector,
            store_events,
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
            let mut retry_at = tokio::time::Instant::now();
            let mut backoff = Duration::from_secs(1);
            // Whether the stale backlog must be replaced with skip markers before the next
            // publish: set when a connection comes up and after a failed publish, which is
            // how an outage shows while a publisher is held.
            let mut skip_due = true;
            let mut progress = ProgressThrottle::default();
            let mut progress_at = tokio::time::Instant::now();
            let mut prune_at = tokio::time::Instant::now();
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {_ = stop.cancelled()=>break,_ = tick.tick()=>{}}
                LIVENESS.pass_started();
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
                            broca_store::bus_events::SKIPPED_EVENT,
                        ] {
                            account
                                .event_subject("broca", name, 1)
                                .map_err(|_| "bus naming client refuses dotted run events")?;
                        }
                        store.enable_bus_events(account.account())?;
                        if !account_known {
                            tracing::info!(target: "bus", account = account.account(), "run events armed: the machine account is known, so durable runs' events are now recorded and published");
                        }
                        account_known = true;
                        LIVENESS.account_known.store(true, Ordering::Relaxed);
                        refresh_at_ms = connection.refresh_at_ms;
                        Ok::<_, String>(connection.publisher)
                    }
                    .await;
                    match result {
                        // Once the account is known, events reach the outbox even while the
                        // broker is away, so a new connection only has to drain it.
                        Ok(connected) => {
                            publisher = Some(connected);
                            backoff = Duration::from_secs(1);
                            skip_due = true;
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
                if let Some(events) = store_events.as_ref().filter(|_| account_known) {
                    let events = Arc::clone(events);
                    let _ = broca_diagnostics::spawn_blocking(
                        broca_diagnostics::JobKind::StoreWrite,
                        move || events.record_waiting(now),
                    )
                    .await;
                }
                if tokio::time::Instant::now() >= prune_at {
                    let _ = store.prune_bus_events(now);
                    prune_at = tokio::time::Instant::now() + Duration::from_secs(60);
                }
                if let Some(bus) = publisher.as_ref() {
                    if skip_due && tokio::time::Instant::now() >= retry_at {
                        let s = store.clone();
                        match broca_diagnostics::spawn_blocking(
                            broca_diagnostics::JobKind::StoreWrite,
                            move || s.skip_stale_bus_events(now_ms()),
                        )
                        .await
                        {
                            Ok(Ok(summary)) => {
                                skip_due = false;
                                if summary.skipped > 0 {
                                    tracing::warn!(target: "bus", skipped = summary.skipped, sessions = summary.sessions, "queued run events older than two minutes are not published; one run.skipped marker per session stands in for them");
                                }
                            }
                            failed => {
                                // Publishing now could replay stale events, so the drain waits
                                // until the backlog has been replaced.
                                let error = match failed {
                                    Ok(Err(error)) => error.to_string(),
                                    _ => "the skip task stopped".to_string(),
                                };
                                tracing::warn!(target: "bus", %error, "could not replace the stale run event backlog; publishing waits");
                                retry_at = tokio::time::Instant::now() + backoff;
                                backoff = (backoff * 2).min(Duration::from_secs(60));
                            }
                        }
                    }
                    if !skip_due && tokio::time::Instant::now() >= retry_at {
                        // One sequential publisher preserves each run's record order through retries.
                        // A pass drains until the outbox is empty or its budget is spent, so a
                        // backlog clears at the broker's pace rather than a fixed count per pass.
                        let drain_started = tokio::time::Instant::now();
                        while drain_started.elapsed() < DRAIN_SLICE {
                            match drain_bus_event(store.as_ref(), bus.as_ref(), now_ms()).await {
                                Ok(DrainResult::Published) => {
                                    backoff = Duration::from_secs(1);
                                }
                                Ok(DrainResult::Empty) => break,
                                _ => {
                                    retry_at = tokio::time::Instant::now() + backoff;
                                    backoff = (backoff * 2).min(Duration::from_secs(60));
                                    skip_due = true;
                                    break;
                                }
                            }
                        }
                    }
                    if tokio::time::Instant::now() < progress_at {
                        continue;
                    }
                    progress_at = tokio::time::Instant::now() + Duration::from_secs(30);
                    let active = registry.active_event_sessions();
                    if let Some(events) = &store_events {
                        events.keep_positions(&active.iter().cloned().collect());
                    }
                    for session in active {
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
                                // A session beats at its last durable record.
                                let Some((epoch, seq)) = store_events
                                    .as_ref()
                                    .and_then(|events| events.position(&session))
                                else {
                                    continue;
                                };
                                if seq == 0 {
                                    continue;
                                }
                                let event = RunEvent {
                                    v: 1,
                                    session_ref: broca_core::session_refs::agent_session_ref(
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
use broca_store::SqliteStore;
#[cfg(test)]
pub(crate) mod tests {
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

    /// A session in the new storage hands its run events to [`StoreRunEvents`]. Before the
    /// machine account is known they wait, in order; once it is, the first pass records them,
    /// and later ones are recorded as they arrive. Each is published with the id, subject and
    /// headers prefrontal checks.
    #[test]
    fn store_run_events_wait_for_the_account_then_publish_in_order() {
        use broca_module::store_run_events::RunEventSink;
        use broca_protocol::RunFinishReason;
        use cortexkit_bus_trait::PublishReceipt;
        use sha2::Digest as _;
        let root = broca_test_support::scratch_root().join(format!("store-events-{}", now_ms()));
        std::fs::create_dir_all(&root).unwrap();
        let store = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
        let events = StoreRunEvents::new(store.clone());
        let session = BindIdentity::new(&root, "broca", "s1");
        let session_ref = broca_core::session_refs::agent_session_ref(&session);
        let event = |seq: u64, detail: RunEventDetail| RunEvent {
            v: 1,
            session_ref: session_ref.clone(),
            run_id: "r1".into(),
            seq,
            at_ms: now_ms(),
            detail,
        };
        let epoch = "0123456789abcdef0123456789abcdef";
        events.durable(
            &session,
            epoch,
            2,
            vec![event(2, RunEventDetail::Started {})],
        );
        events.durable(&session, epoch, 5, Vec::new());
        assert!(
            store.pending_bus_event().unwrap().is_none(),
            "nothing is recorded before the account is known"
        );
        assert_eq!(events.position(&session), Some((epoch.to_owned(), 5)));
        store.enable_bus_events("box_test").unwrap();
        assert!(
            events.record_waiting(now_ms()),
            "the account is known: nothing waits"
        );
        events.durable(
            &session,
            epoch,
            9,
            vec![event(
                9,
                RunEventDetail::Ended {
                    terminal: RunFinishReason::Completed,
                    error_class: None,
                    structured_output: Some(
                        broca_protocol::run_events::StructuredOutputStatus::Absent,
                    ),
                },
            )],
        );
        let mut published = Vec::new();
        while let Some(publication) = store.pending_bus_event().unwrap() {
            store
                .acknowledge_bus_event(&publication, PublishReceipt::new(1, false), now_ms())
                .unwrap();
            published.push(publication);
        }
        let shapes: Vec<_> = published
            .iter()
            .map(|p| {
                (
                    p.subject.clone(),
                    p.headers["kind"].clone(),
                    p.headers["run_id"].clone(),
                    p.headers["session_ref"].clone(),
                    p.headers["epoch"].clone(),
                    p.headers["seq"].clone(),
                )
            })
            .collect();
        assert_eq!(
            shapes,
            [
                (
                    "ck.box_test.event.broca.run.started.v1".to_owned(),
                    "run.started".to_owned(),
                    "r1".to_owned(),
                    session_ref.clone(),
                    epoch.to_owned(),
                    "2".to_owned(),
                ),
                (
                    "ck.box_test.event.broca.run.ended.v1".to_owned(),
                    "run.ended".to_owned(),
                    "r1".to_owned(),
                    session_ref.clone(),
                    epoch.to_owned(),
                    "9".to_owned(),
                ),
            ]
        );
        // The id prefrontal dedupes on: the SHA-256 of the JSON Canonicalization Scheme (RFC
        // 8785) encoding of `[epoch, run_id, seq, subject, null]`.
        for publication in &published {
            let seq: u64 = publication.headers["seq"].parse().unwrap();
            let key =
                serde_jcs::to_vec(&(epoch, "r1", seq, &publication.subject, None::<u64>)).unwrap();
            let expected: String = sha2::Sha256::digest(key)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            assert_eq!(publication.event_key, expected);
            // The stored body for this event key is what `run.event.get` serves: it belongs to
            // the session that wrote it and matches the digest the bus message carried.
            let (owner, body) = store
                .get_bus_event(&publication.event_key)
                .unwrap()
                .expect("the published event's body is kept for run.event.get");
            assert_eq!(owner, session);
            assert_eq!(
                cortexkit_bus_trait::ContentDigest::of_bytes(&body),
                publication.digest()
            );
        }
        let _ = std::fs::remove_dir_all(&root);
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
