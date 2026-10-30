//! Keep the refused source prefix alive without following the active fallback route.

use crate::keep_warm::{AutomaticWarmer, Declare, WarmSlot};
use crate::RunExecutor;
use broca_core::keep_warm_wait::{KeepWarmDriver, WarmTick};
use broca_core::{CacheClock, RecordedRequest, ShutdownSeal, WalHandle, WalStore};
use broca_protocol::BindIdentity;
use broca_wal::{RefusalFallbackPhase, RunConfig, WalRecord};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestStats {
    pub(crate) tasks: std::sync::atomic::AtomicU64,
    pub(crate) subscriptions: std::sync::atomic::AtomicU64,
    pub(crate) tap_calls: std::sync::atomic::AtomicU64,
    pub(crate) record_clones: std::sync::atomic::AtomicU64,
}

#[cfg(test)]
pub(crate) fn test_stats(session: &BindIdentity) -> Arc<TestStats> {
    static STATS: std::sync::OnceLock<
        Mutex<std::collections::HashMap<BindIdentity, std::sync::Weak<TestStats>>>,
    > = std::sync::OnceLock::new();
    let mut stats = STATS.get_or_init(Mutex::default).lock().unwrap();
    if let Some(stats) = stats.get(session).and_then(std::sync::Weak::upgrade) {
        return stats;
    }
    let entry = Arc::new(TestStats::default());
    stats.insert(session.clone(), Arc::downgrade(&entry));
    entry
}

// Leave time for quota lookup and network delivery before a read becomes a cache rewrite.
const FIVE_MINUTE_LEAD_MS: u64 = 30_000;
const ONE_HOUR_LEAD_MS: u64 = 120_000;

#[derive(Clone)]
pub(crate) enum Event {
    Record(u64, Box<WalRecord>),
    RunCancelled,
}

#[derive(Clone)]
pub(crate) struct Events(broadcast::Sender<Event>);

impl Default for Events {
    fn default() -> Self {
        Self(broadcast::channel(128).0)
    }
}

impl Events {
    pub(crate) fn observe(&self, seq: u64, record: &WalRecord) -> bool {
        let relevant = match record {
            WalRecord::RunStarted { .. }
            | WalRecord::ResumeStarted { .. }
            | WalRecord::ModelStepRefused { .. }
            | WalRecord::RunFinished { .. }
            | WalRecord::RunPaused { .. }
            | WalRecord::RunCancelRequested { .. }
            | WalRecord::RunConfigRefrozen { .. }
            | WalRecord::RunAuthRefrozen { .. }
            | WalRecord::CompactionMessage { .. }
            | WalRecord::CompactionViewApplied { .. }
            | WalRecord::WriterClosed { .. } => true,
            WalRecord::ModelStepFinished { .. } => true,
            WalRecord::ModelAttemptFinished {
                credential_refresh,
                served_account_ref,
                ..
            } => credential_refresh.is_some() || served_account_ref.is_some(),
            _ => false,
        };
        if relevant && self.0.receiver_count() != 0 {
            let _ = self.0.send(Event::Record(seq, Box::new(record.clone())));
            return true;
        }
        false
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.0.subscribe()
    }

    pub(crate) fn run_cancelled(&self) {
        let _ = self.0.send(Event::RunCancelled);
    }
}

#[derive(Default)]
struct State {
    head: bool,
    running: bool,
    paused: bool,
    candidate: Option<usize>,
    config: Option<RunConfig>,
    account: Option<String>,
}

impl State {
    fn observe(&mut self, at: usize, record: &WalRecord) {
        match record {
            WalRecord::RunStarted {
                config,
                session_class,
                ..
            } => {
                self.head = session_class.as_deref() == Some("head");
                self.running = true;
                self.paused = false;
                if self
                    .config
                    .as_ref()
                    .is_some_and(|old| old != config.as_ref())
                {
                    self.candidate = None;
                }
                self.config = Some((**config).clone());
            }
            WalRecord::ResumeStarted { .. } => {
                self.running = true;
                self.paused = false;
            }
            WalRecord::ModelStepRefused { .. } => self.candidate = Some(at),
            WalRecord::ModelStepFinished {
                fallback,
                finish_reason,
                ..
            } => {
                let round_active = matches!(fallback.refusal_fallback.as_ref().map(|s| s.phase),
                    Some(RefusalFallbackPhase::Fallback { remaining }) if remaining > 0);
                if fallback.route.is_some() && !round_active {
                    self.candidate = None;
                }
                if fallback.route.is_none() {
                    self.candidate = if round_active
                        && *finish_reason == Some(broca_protocol::FinishReason::ContentFilter)
                    {
                        Some(at)
                    } else {
                        None
                    };
                }
            }
            WalRecord::RunFinished { .. } => {
                self.running = false;
                self.paused = false;
            }
            WalRecord::RunPaused { .. } => {
                self.running = false;
                self.paused = true;
            }
            WalRecord::RunCancelRequested { .. } => self.running = false,
            WalRecord::RunConfigRefrozen {
                renderer_version,
                frozen_render_config,
                ..
            } => {
                self.candidate = None;
                if let Some(config) = self.config.as_mut() {
                    config.renderer_version = *renderer_version;
                    config.frozen_render_config = frozen_render_config.clone();
                }
            }
            WalRecord::RunAuthRefrozen { auth_selection, .. } => {
                self.candidate = None;
                if let Some(config) = self.config.as_mut() {
                    config.auth_selection = Some(auth_selection.clone());
                }
            }
            WalRecord::CompactionMessage { .. } | WalRecord::CompactionViewApplied { .. } => {
                self.candidate = None;
            }
            WalRecord::ModelAttemptFinished {
                credential_refresh,
                served_account_ref,
                ..
            } => {
                if credential_refresh.is_some()
                    || (self.candidate.is_some()
                        && served_account_ref
                            .as_ref()
                            .is_some_and(|account| self.account.as_ref() != Some(account)))
                {
                    self.candidate = None;
                }
                if served_account_ref.is_some() {
                    self.account = served_account_ref.clone();
                }
            }
            _ => {}
        }
    }
}

fn lead_ms(ttl_ms: u64) -> u64 {
    if ttl_ms >= 3_600_000 {
        ONE_HOUR_LEAD_MS
    } else {
        FIVE_MINUTE_LEAD_MS.min(ttl_ms / 2)
    }
}

fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

struct Prefix {
    warmer: Arc<AutomaticWarmer>,
    request: RecordedRequest,
    ttl_ms: u64,
    next: tokio::time::Instant,
    spent: f64,
    idle_spent: f64,
    budget_stopped: bool,
}

impl Prefix {
    fn schedule(&mut self, sent_at_ms: u64) {
        self.next = tokio::time::Instant::now()
            + std::time::Duration::from_millis(
                sent_at_ms
                    .saturating_add(self.ttl_ms.saturating_sub(lead_ms(self.ttl_ms)))
                    .saturating_sub(epoch_ms()),
            );
    }
}

pub(crate) struct Task {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    pub(crate) holds_idle: Arc<AtomicBool>,
    #[cfg(test)]
    completed_warms: Arc<std::sync::atomic::AtomicU64>,
}

pub(crate) type Export = Option<(
    Arc<dyn broca_core::ExportIndex>,
    Arc<dyn broca_core::ChargeBasisResolver + Send + Sync>,
)>;

pub(crate) struct Runtime<W: WalStore> {
    pub(crate) session: BindIdentity,
    pub(crate) writer: Arc<WalHandle>,
    pub(crate) store: Arc<W>,
    pub(crate) executor: Arc<dyn RunExecutor>,
    pub(crate) declare: Declare,
    pub(crate) slot: WarmSlot,
    pub(crate) stopping: CancellationToken,
    pub(crate) export: Export,
}

impl Task {
    pub(crate) async fn stop(mut self) {
        self.cancel.cancel();
        // A schedule rebuild may be waiting on a WAL-read thread. Abort and join the async
        // task so it releases the session writer even when that read has not returned.
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

// A run's shutdown seal records its latest in-flight request for cancellation and warming.
// Give source warming an empty seal and its own clock so it always uses the recorded source
// request, never the run's newer Opus 4.8 request.
pub(crate) fn spawn<W: WalStore + 'static>(
    runtime: Runtime<W>,
    events: broadcast::Receiver<Event>,
    records: Vec<WalRecord>,
    safe_head: u64,
) -> Task {
    #[cfg(test)]
    test_stats(&runtime.session)
        .tasks
        .fetch_add(1, Ordering::Relaxed);
    let cancel = CancellationToken::new();
    let holds_idle = Arc::new(AtomicBool::new(false));
    #[cfg(test)]
    let completed_warms = Arc::new(std::sync::atomic::AtomicU64::new(
        records
            .iter()
            .filter(|r| matches!(r, WalRecord::CacheWarmed { .. }))
            .count() as u64,
    ));
    let task = tokio::spawn(run(
        runtime,
        events,
        records,
        safe_head,
        cancel.clone(),
        holds_idle.clone(),
        #[cfg(test)]
        completed_warms.clone(),
    ));
    Task {
        cancel,
        task,
        holds_idle,
        #[cfg(test)]
        completed_warms,
    }
}

fn rebuild<W: WalStore>(
    records: &[WalRecord],
    state: &State,
    runtime: &Runtime<W>,
) -> Option<Prefix> {
    let Runtime {
        session,
        writer,
        executor,
        declare,
        slot,
        ..
    } = runtime;
    let at = state.candidate?;
    // Excluding the refusal's later fallback answer is essential: both physical requests
    // share a step id, but only the first one touched the source cache.
    let end = if matches!(records[at], WalRecord::ModelStepRefused { .. }) {
        at
    } else {
        at + 1
    };
    let request = broca_core::cache_warm::last_recorded_request(&records[..end]).ok()?;
    if request.route.is_some() {
        return None;
    }
    let ttl_ms = declare(&request.config)?.ttl_ms?;
    let clock = CacheClock::default();
    clock.seed_if_unknown(&records[..end], Some(ttl_ms));
    if let WalRecord::ModelStepRefused { usage, .. } = &records[at] {
        clock.note_usage(clock.read()?.last_sent_at_ms, *usage);
    }
    for record in &records[at..] {
        if let WalRecord::CacheWarmed {
            request_body_sha256,
            sent_at_ms,
            usage,
            envelope,
            ..
        } = record
        {
            if request_body_sha256 == &request.request_body_sha256
                && envelope
                    .as_ref()
                    .is_none_or(|e| e.outcome == broca_wal::record::BillingAttemptOutcome::Complete)
            {
                clock.note_sent(*sent_at_ms, Some(ttl_ms));
                clock.note_usage(*sent_at_ms, *usage);
            }
        }
    }
    let last_sent = clock.read()?.last_sent_at_ms;
    let warmer = Arc::new(AutomaticWarmer {
        trigger: broca_core::cache_warm::SOURCE_KEEP_WARM_TRIGGER,
        retention: records.iter().any(|record| {
            matches!(
                record,
                WalRecord::RunStarted {
                    retention: Some(_),
                    ..
                }
            )
        }),
        session: session.clone(),
        writer: writer.clone(),
        executor: executor.clone(),
        clock,
        seal: ShutdownSeal::new(),
        slot: slot.clone(),
        declare: declare.clone(),
        recorded: Mutex::new(Some(request.clone())),
    });
    let mut prefix = Prefix {
        warmer,
        request,
        ttl_ms,
        next: tokio::time::Instant::now(),
        spent: 0.0,
        idle_spent: 0.0,
        budget_stopped: false,
    };
    let mut idle = false;
    for record in &records[at..] {
        match record {
            WalRecord::RunStarted { .. } => {
                idle = false;
                prefix.spent = 0.0;
                prefix.idle_spent = 0.0;
            }
            WalRecord::ResumeStarted { .. } => idle = false,
            WalRecord::RunFinished { .. } => idle = true,
            WalRecord::CacheWarmed {
                request_body_sha256,
                usage,
                envelope,
                reservation,
                ttl_ms,
                ..
            } if request_body_sha256 == &prefix.request.request_body_sha256 => {
                let cost = match envelope
                    .as_deref()
                    .filter(|e| e.outcome != broca_wal::record::BillingAttemptOutcome::Complete)
                {
                    Some(envelope) => prefix
                        .warmer
                        .failed_cost(
                            &prefix.request.config,
                            reservation.as_deref()?,
                            envelope,
                            *ttl_ms,
                        )
                        .ok()?,
                    None => prefix
                        .warmer
                        .recorded_cost(&prefix.request.config, usage)
                        .ok()?,
                };
                prefix.spent += cost;
                if idle {
                    prefix.idle_spent += cost;
                }
            }
            _ => {}
        }
    }
    prefix.schedule(last_sent);
    Some(prefix)
}

async fn run<W: WalStore + 'static>(
    runtime: Runtime<W>,
    mut events: broadcast::Receiver<Event>,
    records: Vec<WalRecord>,
    mut safe_head: u64,
    cancel: CancellationToken,
    holds_idle: Arc<AtomicBool>,
    #[cfg(test)] completed_warms: Arc<std::sync::atomic::AtomicU64>,
) {
    let Runtime {
        session,
        writer,
        store,
        stopping,
        export,
        ..
    } = &runtime;
    let mut state = State::default();
    for (at, record) in records.iter().enumerate() {
        state.observe(at, record);
    }
    let mut prefix = rebuild(&records, &state, &runtime);
    drop(records);
    let mut tick: Option<std::pin::Pin<Box<dyn std::future::Future<Output = WarmTick> + Send>>> =
        None;
    loop {
        holds_idle.store(
            state.head && !state.paused && prefix.as_ref().is_some_and(|p| !p.budget_stopped),
            Ordering::Release,
        );
        let next = prefix
            .as_ref()
            .filter(|prefix| {
                !state.paused && (state.head || state.running) && !prefix.budget_stopped
            })
            .map(|prefix| prefix.next);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = stopping.cancelled() => break,
            event = events.recv() => {
                let (seq, record) = match event {
                    Ok(Event::Record(seq, record)) => (seq, *record),
                    Ok(Event::RunCancelled) => {
                        state.running = false;
                        if !state.head { tick = None; }
                        continue;
                    }
                    Err(_) => break,
                };
                if seq <= safe_head { continue; }
                safe_head = seq;
                state.observe(0, &record);
                if state.candidate.is_none() { prefix = None; }
                if prefix.is_none() || state.paused || (!state.head && !state.running) { tick = None; }
                if matches!(record, WalRecord::RunStarted { .. }) {
                    if let Some(prefix) = prefix.as_mut() {
                        prefix.idle_spent = 0.0;
                        prefix.spent = 0.0;
                        prefix.budget_stopped = false;
                    }
                }
                let new_refusal = matches!(record, WalRecord::ModelStepRefused { .. })
                    || (matches!(record, WalRecord::ModelStepFinished { finish_reason: Some(broca_protocol::FinishReason::ContentFilter), .. })
                        && state.candidate.is_some());
                if new_refusal {
                    tick = None;
                    let store = store.clone();
                    let read_session = session.clone();
                    let read = broca_diagnostics::spawn_blocking(broca_diagnostics::JobKind::WalRead, move || {
                        let range = store.read_records_range(&read_session, 0, Some(seq), None).ok()?;
                        let records = crate::durable_read::DurableRead::classify(&range, 0, Some(seq))
                            .records().ok()?.iter().map(|f| f.record.clone()).collect::<Vec<_>>();
                        Some(records)
                    }).await;
                    if let Ok(Some(records)) = read {
                        state = State::default();
                        for (at, record) in records.iter().enumerate() { state.observe(at, record); }
                        prefix = rebuild(&records, &state, &runtime);
                    } else { prefix = None; }
                }
            }
            outcome = async { match tick.as_mut() { Some(future) => future.await, None => std::future::pending().await } } => {
                tick = None;
                let Some(current) = prefix.as_mut() else { continue; };
                if let Some((index, resolver)) = export.as_ref().filter(|_| matches!(&outcome,
                    WarmTick::Sent | WarmTick::SentWithCost { .. } | WarmTick::ResponsesSent { .. } | WarmTick::StopWithCost { .. })) {
                    let index = index.clone();
                    let resolver = resolver.clone();
                    let store = store.clone();
                    let session = session.clone();
                    let run_id = current.request.run_id.clone();
                    let head = writer.durable_head().load(Ordering::Acquire);
                    let exporting = broca_diagnostics::spawn_blocking(broca_diagnostics::JobKind::StoreWrite, move || {
                        let range = store.read_records_range(&session, 0, Some(head), None).map_err(|e| e.to_string())?;
                        let read = crate::durable_read::DurableRead::classify(&range, 0, Some(head));
                        index.emit_run(read.records()?, &run_id, resolver.as_ref()).map_err(|e| e.to_string())
                    });
                    let result = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        _ = stopping.cancelled() => break,
                        result = exporting => result,
                    };
                    if !matches!(result, Ok(Ok(()))) {
                        tracing::warn!(target: "export", "source cache warm usage export failed (non-fatal)");
                    }
                }
                let cost = match outcome {
                    WarmTick::Sent => current.warmer.costs(&current.request.config).map(|c| c.warm).unwrap_or(f64::INFINITY),
                    WarmTick::SentWithCost { cost } | WarmTick::ResponsesSent { cost, .. } => cost,
                    WarmTick::Skipped => {
                        current.next = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
                        continue;
                    }
                    WarmTick::Stop { reason, .. } if reason == "budget" => {
                        tracing::info!(target: "cache_warm", stop = "budget", model = %current.request.config.model.model_id,
                            "source cache warming reached one rewrite's cost");
                        current.budget_stopped = true;
                        continue;
                    }
                    WarmTick::Stop { reason, detail } | WarmTick::StopWithCost { reason, detail, .. } => {
                        tracing::info!(target: "cache_warm", stop = %reason, detail = ?detail,
                            model = %current.request.config.model.model_id, "source cache warming stopped");
                        prefix = None;
                        continue;
                    }
                };
                current.spent += cost;
                if !state.running { current.idle_spent += cost; }
                current.schedule(current.warmer.last_sent_at_ms().unwrap_or_default());
                #[cfg(test)]
                completed_warms.fetch_add(1, Ordering::Release);
            }
            _ = async { match next { Some(deadline) => tokio::time::sleep_until(deadline).await, None => std::future::pending().await } }, if tick.is_none() => {
                let Some(current) = prefix.as_mut() else { continue; };
                // A cache read is worth buying only while accumulated rent is below one rewrite.
                // The model catalog's write/read prices for the source request's frozen cache TTL,
                // rather than a fixed warm count, set the cap.
                let allowed = current.warmer.costs(&current.request.config).is_ok_and(|costs| costs.allows_next(current.spent) && (state.running || costs.allows_next(current.idle_spent)));
                if !allowed {
                    current.budget_stopped = true;
                    tracing::info!(target: "cache_warm", stop = "budget", model = %current.request.config.model.model_id,
                        "source cache warming reached one rewrite's cost");
                    continue;
                }
                // Ordinary head run boundaries do not cancel and resend an already-sent warm.
                // Only a prefix invalidation or a lifecycle stop drops the owned request future.
                let warmer = current.warmer.clone();
                let config = current.request.config.clone();
                let run_id = current.request.run_id.clone();
                tick = Some(Box::pin(async move { warmer.warm(&config, &run_id).await }));
            }
        }
    }
    holds_idle.store(false, Ordering::Release);
}

#[cfg(test)]
#[path = "source_warm_tests.rs"]
pub(crate) mod tests;
