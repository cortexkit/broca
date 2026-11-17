//! Refresh a provider's cached prompt while tools are outstanding. The tool wait owns both
//! the timer and provider future, so receiving the tool results stops that future's local IO.

use broca_protocol::RunId;
use broca_wal::RunConfig;
use std::{future::Future, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
pub struct WarmTiming {
    pub ttl_ms: u64,
    pub lead_ms: u64,
    pub after_send_ms: Option<u64>,
}

impl WarmTiming {
    /// Warming between turns and warming while a run waits for tools share the lead time
    /// and send interval.
    pub fn for_ttl(ttl_ms: u64, after_send_ms: Option<u64>) -> Self {
        Self {
            ttl_ms,
            lead_ms: 30_000.min(ttl_ms / 2),
            after_send_ms,
        }
    }

    pub fn next_due_ms(self, sent_at_ms: u64) -> u64 {
        sent_at_ms.saturating_add(
            self.after_send_ms
                .unwrap_or_else(|| self.ttl_ms.saturating_sub(self.lead_ms)),
        )
    }
}

pub enum WarmTick {
    Sent,
    SentWithCost {
        cost: f64,
    },
    ResponsesSent {
        usage: broca_protocol::Usage,
        cost: f64,
    },
    Skipped,
    Stop {
        reason: String,
        detail: Option<String>,
    },
    StopWithCost {
        reason: String,
        detail: Option<String>,
        cost: f64,
    },
}

/// The returned future must own the provider request so dropping it when tools finish stops
/// the request instead of leaving it running. Once a warm reports usage, queue `CacheWarmed`
/// (the warm and its usage in the session's write-ahead log) before any further await, so
/// cancellation cannot lose the reported usage.
///
/// `C` is what the driver reads a run's frozen request from: today's run loop hands it the
/// run's `RunConfig`; another run loop may hand it its own frozen admission.
#[async_trait::async_trait]
pub trait KeepWarmDriver<C: ?Sized + Sync = RunConfig>: Send + Sync {
    fn timing(&self, config: &C) -> Result<WarmTiming, String>;
    /// The cache lifetime requests under `config` ask the provider for, when they ask for one
    /// it honours. Unlike `timing`'s lifetime, which may be broca's own estimate for scheduling
    /// warms, this one can prove the cache expired. `None` when the request names none.
    fn requested_ttl_ms(&self, _config: &C) -> Option<u64> {
        None
    }
    fn last_sent_at_ms(&self) -> Option<u64>;
    fn note_attempt(&self, _attempt: &broca_protocol::ModelAttempt) {}
    /// Observe a warming stop for live reports, independently of durable usage accounting.
    fn note_stop(&self, _run_id: &RunId, _reason: &str) {}
    /// The seal a run's model calls note each request on as their transport starts sending it,
    /// for a run loop that gives its calls no seal of its own: a warm re-sends those bytes, and
    /// their send and usage date the cache. `None` when the loop's own seal already does, as
    /// today's run loop's shutdown seal does.
    fn step_seal(&self) -> Option<crate::ShutdownSeal> {
        None
    }
    fn costs(&self, config: &C) -> Result<crate::keep_warm::WarmCosts, String>;
    /// Price a durable warm exactly as the live driver did. Responses drivers must
    /// override this with usage pricing, not the fixed output-token quote.
    fn recorded_cost(&self, config: &C, _usage: &broca_protocol::Usage) -> Result<f64, String> {
        self.costs(config).map(|costs| costs.warm)
    }
    /// Price measured usage of a failed sent attempt, never the successful-refresh estimate.
    fn failed_cost(
        &self,
        _config: &C,
        _reservation: &broca_wal::record::AttemptReservation,
        _envelope: &broca_wal::record::AttemptEnvelope,
        _ttl_ms: Option<u64>,
    ) -> Result<f64, String> {
        Err("reported warm usage pricing unavailable".into())
    }
    async fn warm(&self, config: &C, run_id: &RunId) -> WarmTick;
}

pub struct WarmWait<'a, C: ?Sized + Sync = RunConfig> {
    pub driver: &'a Arc<dyn KeepWarmDriver<C>>,
    pub config: &'a C,
    pub run_id: &'a RunId,
    pub cancel: Option<&'a CancellationToken>,
    /// Records whether this run already logged that prompt-cache warming is unsupported,
    /// so later tool waits do not repeat the same permanent refusal.
    pub unsupported_logged: &'a std::sync::atomic::AtomicBool,
    /// A missed refresh disables automatic warming across all tool waits in this run.
    pub warm_missed: &'a std::sync::atomic::AtomicBool,
    /// The run's warming mode, which decides when it stops (`crate::keep_warm::allows_warm`).
    pub keep_warm: &'a broca_protocol::request::KeepWarm,
    /// Warms this wait's batch already sent before a restart, which count toward `always` and
    /// `hours`'s cap as its spend counts toward `auto`'s.
    pub warms_before: u32,
}

impl<C: ?Sized + Sync> WarmWait<'_, C> {
    fn log_unsupported(&self) -> bool {
        !self
            .unsupported_logged
            .swap(true, std::sync::atomic::Ordering::Relaxed)
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn delay<C: ?Sized + Sync>(driver: &dyn KeepWarmDriver<C>, timing: WarmTiming) -> Option<Duration> {
    let sent = driver.last_sent_at_ms()?;
    Some(Duration::from_millis(
        timing.next_due_ms(sent).saturating_sub(now_ms()),
    ))
}

/// Start only after the WAL commits the tool calls, not while the run is still deciding what
/// to dispatch. If a tool result and warm timer are ready together, prioritize the result.
/// Keep the warm future here rather than spawning it so results cancel its local IO before
/// the next model request. Cancellation likewise drops the future without awaiting it.
pub async fn wait_tools<F: Future, C: ?Sized + Sync>(
    tools: F,
    warm: Option<WarmWait<'_, C>>,
) -> F::Output {
    wait_tools_with_spend(tools, warm, 0.0).await
}

pub async fn wait_tools_with_spend<F: Future, C: ?Sized + Sync>(
    tools: F,
    warm: Option<WarmWait<'_, C>>,
    initial_spend: f64,
) -> F::Output {
    let Some(warm) = warm else {
        return tools.await;
    };
    if warm.warm_missed.load(std::sync::atomic::Ordering::Relaxed) {
        return tools.await;
    }
    let timing = match warm.driver.timing(warm.config) {
        Ok(timing) => timing,
        Err(reason) => {
            warm.driver.note_stop(warm.run_id, "unsupported");
            if warm.log_unsupported() {
                tracing::info!(target: "cache_warm", stop = "unsupported", detail = %reason, "keep_warm stopped");
            }
            return tools.await;
        }
    };
    if timing.ttl_ms == 0
        || timing.after_send_ms == Some(0)
        || (timing.after_send_ms.is_none() && timing.lead_ms >= timing.ttl_ms)
    {
        warm.driver.note_stop(warm.run_id, "unsupported");
        if warm.log_unsupported() {
            tracing::info!(target: "cache_warm", stop = "unsupported", "keep_warm has no positive scheduling interval");
        }
        return tools.await;
    }
    let Some(initial) = delay(warm.driver.as_ref(), timing) else {
        warm.driver.note_stop(warm.run_id, "unsupported");
        if warm.log_unsupported() {
            tracing::info!(target: "cache_warm", stop = "unsupported", "keep_warm has no cache clock");
        }
        return tools.await;
    };
    tokio::pin!(tools);
    let mut next = Some(tokio::time::Instant::now() + initial);
    let mut tick: Option<std::pin::Pin<Box<dyn Future<Output = WarmTick> + Send + '_>>> = None;
    let mut spent = initial_spend;
    let mut warms = warm.warms_before;
    let mut tick_cost = 0.0;
    let mut stopped = false;
    loop {
        tokio::select! {
            biased;
            result = &mut tools => {
                // The wait's ordinary ends, once per tool batch: debug, so they don't bury the stops
                // that mean warming couldn't do its job (unsupported, missed, capped).
                if !stopped {
                    let reason = if warm.cancel.is_some_and(CancellationToken::is_cancelled) { "cancelled" } else { "tools_done" };
                    tracing::debug!(target: "cache_warm", stop = reason, "keep_warm stopped");
                }
                return result;
            }
            _ = async { match warm.cancel { Some(cancel) => cancel.cancelled().await, None => std::future::pending().await } }, if !stopped => {
                tick = None;
                next = None;
                stopped = true;
                tracing::debug!(target: "cache_warm", stop = "cancelled", "keep_warm stopped");
            }
            outcome = async { match tick.as_mut() { Some(future) => future.await, None => std::future::pending().await } } => {
                tick = None;
                match outcome {
                    WarmTick::Sent => {
                        warms += 1;
                        spent += tick_cost;
                        next = delay(warm.driver.as_ref(), timing).map(|delay| tokio::time::Instant::now() + delay);
                    }
                    WarmTick::SentWithCost { cost } => {
                        warms += 1;
                        spent += cost;
                        next = delay(warm.driver.as_ref(), timing).map(|delay| tokio::time::Instant::now() + delay);
                    }
                    WarmTick::ResponsesSent { usage, cost } => {
                        warms += 1;
                        spent += cost;
                        let missed = usage.cached_input_tokens.zip(usage.input_tokens).map(|(cached, fresh)| (cached, cached.saturating_add(fresh)));
                        if let Some((cached, total)) = missed.filter(|(cached, total)| *cached < total.saturating_sub(*cached)) {
                            warm.warm_missed.store(true, std::sync::atomic::Ordering::Relaxed);
                            warm.driver.note_stop(warm.run_id, "warm_missed");
                            stopped = true;
                            next = None;
                            tracing::info!(target: "cache_warm", stop = "warm_missed", cached_input_tokens = cached, total_input_tokens = total, run_id = %warm.run_id.0, spent, "keep_warm stopped");
                        } else {
                            next = delay(warm.driver.as_ref(), timing).map(|delay| tokio::time::Instant::now() + delay);
                        }
                    }
                    WarmTick::Skipped => {
                        next = Some(tokio::time::Instant::now() + Duration::from_millis((timing.lead_ms / 10).clamp(1, 1000)));
                    }
                    WarmTick::Stop { reason, detail } => {
                        warm.driver.note_stop(warm.run_id, &reason);
                        stopped = true;
                        next = None;
                        if reason != "unsupported" || warm.log_unsupported() {
                            tracing::info!(target: "cache_warm", stop = %reason, detail = ?detail, "keep_warm stopped");
                        }
                    }
                    WarmTick::StopWithCost { reason, detail, cost } => {
                        warm.driver.note_stop(warm.run_id, &reason);
                        spent += cost;
                        stopped = true;
                        next = None;
                        if reason != "unsupported" || warm.log_unsupported() {
                            tracing::info!(target: "cache_warm", stop = %reason, detail = ?detail, spent, "keep_warm stopped");
                        }
                    }
                }
            }
            _ = async { match next { Some(deadline) => tokio::time::sleep_until(deadline).await, None => std::future::pending().await } }, if tick.is_none() => {
                next = None;
                let costs = warm.driver.costs(warm.config);
                match crate::keep_warm::allows_warm(warm.keep_warm, costs.clone(), spent, warms, now_ms()) {
                    Ok(()) => {
                        // Only `auto` is gated on price, but every mode keeps its spend.
                        tick_cost = costs.map_or(0.0, |costs| costs.warm);
                        tick = Some(warm.driver.warm(warm.config, warm.run_id));
                    }
                    Err((reason, detail)) => {
                        warm.driver.note_stop(warm.run_id, reason);
                        stopped = true;
                        tracing::info!(target: "cache_warm", stop = reason, detail = %detail, warms, "keep_warm stopped");
                    }
                }
            }
        }
    }
}

/// A restart pause does not buy a second cache-bust allowance for the same batch.
pub fn batch_spend(
    warm: Option<&WarmWait<'_>>,
    step_id: broca_protocol::StepId,
    frames: &[broca_wal::FramedRecord],
) -> f64 {
    batch_spend_iter(warm, step_id, frames.iter())
}

pub(crate) fn batch_spend_iter<'a>(
    warm: Option<&WarmWait<'_>>,
    step_id: broca_protocol::StepId,
    frames: impl Iterator<Item = &'a broca_wal::FramedRecord>,
) -> f64 {
    let Some(warm) = warm else { return 0.0 };
    frames
        .filter_map(|frame| match &frame.record {
            broca_wal::WalRecord::CacheWarmed {
                trigger,
                run_id,
                step_id: warmed_step,
                usage,
                envelope,
                reservation,
                ttl_ms,
                ..
            } if trigger.as_deref() == Some("keep_warm")
                && run_id == warm.run_id
                && *warmed_step == step_id =>
            {
                Some(match envelope.as_deref() {
                    envelope if crate::cache_warm::warm_reached_the_cache(envelope) => warm
                        .driver
                        .recorded_cost(warm.config, usage)
                        .unwrap_or(f64::INFINITY),
                    Some(envelope) if crate::cache_warm::warm_budget_needs_pricing(envelope) => {
                        match reservation.as_deref() {
                            Some(reservation) => warm.driver.failed_cost(warm.config, reservation, envelope, *ttl_ms).unwrap_or(f64::INFINITY),
                            None => {
                                tracing::warn!(target: "cache_warm", stop = "pricing_unavailable", "warm budget exhausted: failed warm has no reservation identifying its model");
                                f64::INFINITY
                            }
                        }
                    }
                    _ => 0.0,
                })
            }
            _ => None,
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    struct ChargedDriver {
        costs_called: AtomicUsize,
        warms: AtomicUsize,
        next_decision: tokio::sync::Notify,
        stops: std::sync::Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl KeepWarmDriver for ChargedDriver {
        fn note_stop(&self, _: &RunId, reason: &str) {
            self.stops.lock().unwrap().push(reason.into());
        }
        fn timing(&self, _: &RunConfig) -> Result<WarmTiming, String> {
            Ok(WarmTiming {
                ttl_ms: 1000,
                lead_ms: 0,
                after_send_ms: Some(1),
            })
        }
        fn last_sent_at_ms(&self) -> Option<u64> {
            Some(0)
        }
        fn costs(&self, _: &RunConfig) -> Result<crate::keep_warm::WarmCosts, String> {
            if self.costs_called.fetch_add(1, Ordering::SeqCst) == 1 {
                self.next_decision.notify_one();
            }
            Ok(crate::keep_warm::WarmCosts {
                warm: 10.0,
                bust: 21.0,
            })
        }
        async fn warm(&self, _: &RunConfig, _: &RunId) -> WarmTick {
            self.warms.fetch_add(1, Ordering::SeqCst);
            // A measured failed first attempt cost four, then the successful resend cost ten.
            WarmTick::SentWithCost { cost: 14.0 }
        }
    }

    /// A Responses run's driver whose every warm reports `cached` cached and `fresh` uncached
    /// input tokens. Each warm is due a second after the last send, which reads as now, so the
    /// scheduler waits on the paused clock between warms rather than warming in a busy loop.
    struct ResponsesDriver {
        usage: broca_protocol::Usage,
        warms: AtomicUsize,
        stops: std::sync::Mutex<Vec<String>>,
    }
    impl ResponsesDriver {
        fn new(cached: u64, fresh: u64) -> Arc<Self> {
            Arc::new(Self {
                usage: broca_protocol::Usage {
                    input_tokens: Some(fresh),
                    cached_input_tokens: Some(cached),
                    ..Default::default()
                },
                warms: AtomicUsize::new(0),
                stops: Default::default(),
            })
        }
        fn warms(&self) -> usize {
            self.warms.load(Ordering::SeqCst)
        }
    }
    #[async_trait::async_trait]
    impl KeepWarmDriver<()> for ResponsesDriver {
        fn note_stop(&self, _: &RunId, reason: &str) {
            self.stops.lock().unwrap().push(reason.into());
        }
        fn timing(&self, _: &()) -> Result<WarmTiming, String> {
            Ok(WarmTiming {
                ttl_ms: 1000,
                lead_ms: 0,
                after_send_ms: Some(1000),
            })
        }
        fn last_sent_at_ms(&self) -> Option<u64> {
            Some(now_ms())
        }
        fn costs(&self, _: &()) -> Result<crate::keep_warm::WarmCosts, String> {
            Ok(crate::keep_warm::WarmCosts {
                warm: 0.0,
                bust: 1.0,
            })
        }
        async fn warm(&self, _: &(), _: &RunId) -> WarmTick {
            self.warms.fetch_add(1, Ordering::SeqCst);
            WarmTick::ResponsesSent {
                usage: self.usage,
                cost: 0.0,
            }
        }
    }

    /// Waits on `tools` while `driver` warms, as one tool wait of run `r` whose earlier waits
    /// set `missed`.
    async fn responses_wait(
        driver: &Arc<ResponsesDriver>,
        missed: &AtomicBool,
        tools: impl Future<Output = ()>,
    ) {
        let dynamic: Arc<dyn KeepWarmDriver<()>> = driver.clone();
        let unsupported = AtomicBool::new(false);
        let run = RunId("r".into());
        wait_tools(
            tools,
            Some(WarmWait {
                driver: &dynamic,
                config: &(),
                run_id: &run,
                cancel: None,
                unsupported_logged: &unsupported,
                warm_missed: missed,
                keep_warm: &broca_protocol::request::KeepWarm::Auto,
                warms_before: 0,
            }),
        )
        .await;
    }

    /// A Responses warm whose cached tokens are fewer than its uncached ones missed the cached
    /// prefix: warming stops after that one warm, noted `warm_missed`, for this tool wait and
    /// every later one of the run.
    #[tokio::test(start_paused = true)]
    async fn a_responses_warm_that_missed_the_prefix_stops_warming_for_the_run() {
        let driver = ResponsesDriver::new(15_616, 164_384);
        let missed = AtomicBool::new(false);
        // The tools finish an hour after the scheduler stops, or after ten warms if it never
        // does: room for many more warms either way.
        responses_wait(&driver, &missed, async {
            while driver.stops.lock().unwrap().is_empty() && driver.warms() < 10 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            tokio::time::sleep(Duration::from_secs(3600)).await;
        })
        .await;
        assert_eq!(driver.warms(), 1);
        assert_eq!(*driver.stops.lock().unwrap(), ["warm_missed"]);
        assert!(missed.load(Ordering::SeqCst));
        responses_wait(
            &driver,
            &missed,
            tokio::time::sleep(Duration::from_secs(3600)),
        )
        .await;
        assert_eq!(
            driver.warms(),
            1,
            "a later tool wait of the run sends no warm"
        );
    }

    /// A Responses warm that read most of its input from the cache keeps warming at its cadence.
    #[tokio::test(start_paused = true)]
    async fn a_responses_warm_that_hit_the_prefix_keeps_warming() {
        let driver = ResponsesDriver::new(170_000, 10_000);
        let missed = AtomicBool::new(false);
        // The tools finish after three warms, or as soon as the scheduler stops, so a wrongly
        // stopped scheduler fails the assertions below rather than leaving the wait hanging.
        responses_wait(&driver, &missed, async {
            while driver.warms() < 3 && driver.stops.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        assert!(driver.warms() >= 3, "warms: {}", driver.warms());
        assert!(driver.stops.lock().unwrap().is_empty());
        assert!(!missed.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn live_budget_counts_failed_usage_before_a_successful_resend() {
        let driver = Arc::new(ChargedDriver {
            costs_called: AtomicUsize::new(0),
            warms: AtomicUsize::new(0),
            next_decision: Default::default(),
            stops: Default::default(),
        });
        let done = CancellationToken::new();
        let task = {
            let driver = driver.clone();
            let done = done.clone();
            tokio::spawn(async move {
                let driver: Arc<dyn KeepWarmDriver> = driver;
                let config = serde_json::from_value(serde_json::json!({
                    "model": broca_protocol::request::ModelHandle::new("p", "m"), "tools": [],
                    "tool_choice": broca_protocol::ToolChoice::Auto, "generation": broca_protocol::GenerationConfig::default(),
                    "response_format": broca_protocol::request::ResponseFormat::default(), "context_epoch": 0, "renderer_version": 1,
                })).unwrap();
                let unsupported = AtomicBool::new(false);
                let missed = AtomicBool::new(false);
                let run = RunId("r".into());
                wait_tools_with_spend(
                    done.cancelled(),
                    Some(WarmWait {
                        driver: &driver,
                        config: &config,
                        run_id: &run,
                        cancel: None,
                        unsupported_logged: &unsupported,
                        warm_missed: &missed,
                        keep_warm: &broca_protocol::request::KeepWarm::Auto,
                        warms_before: 0,
                    }),
                    0.0,
                )
                .await;
            })
        };
        driver.next_decision.notified().await;
        done.cancel();
        task.await.unwrap();
        assert_eq!(*driver.stops.lock().unwrap(), ["budget"]);
        assert_eq!(
            driver.warms.load(Ordering::SeqCst),
            1,
            "fourteen spent leaves no room for another ten-unit warm"
        );
    }
}
