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
/// with that warm and usage in the session store before any further await, so cancellation
/// cannot lose the reported usage.
///
/// `C` is what the driver reads a run's frozen request from: the run loop hands it the run's
/// `RunConfig`; another run loop may hand it its own frozen admission.
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
    /// their send and usage date the cache. `None` when the loop's own seal already does.
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
