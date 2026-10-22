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

pub enum WarmTick {
    Sent,
    ResponsesSent {
        usage: broca_protocol::Usage,
        cost: f64,
    },
    Skipped,
    Stop {
        reason: String,
        detail: Option<String>,
    },
}

/// The returned future must own the provider request so dropping it when tools finish stops
/// the request instead of leaving it running. Once a warm reports usage, queue `CacheWarmed`
/// (the warm and its usage in the session's write-ahead log) before any further await, so
/// cancellation cannot lose the reported usage.
#[async_trait::async_trait]
pub trait KeepWarmDriver: Send + Sync {
    fn timing(&self, config: &RunConfig) -> Result<WarmTiming, String>;
    fn last_sent_at_ms(&self) -> Option<u64>;
    fn note_attempt(&self, _attempt: &broca_protocol::ModelAttempt) {}
    fn costs(&self, config: &RunConfig) -> Result<crate::keep_warm::WarmCosts, String>;
    /// Price a durable warm exactly as the live driver did. Responses drivers must
    /// override this with usage pricing, not the fixed output-token quote.
    fn recorded_cost(
        &self,
        config: &RunConfig,
        _usage: &broca_protocol::Usage,
    ) -> Result<f64, String> {
        self.costs(config).map(|costs| costs.warm)
    }
    async fn warm(&self, config: &RunConfig, run_id: &RunId) -> WarmTick;
}

pub struct WarmWait<'a> {
    pub driver: &'a Arc<dyn KeepWarmDriver>,
    pub config: &'a RunConfig,
    pub run_id: &'a RunId,
    pub cancel: Option<&'a CancellationToken>,
    /// Records whether this run already logged that prompt-cache warming is unsupported,
    /// so later tool waits do not repeat the same permanent refusal.
    pub unsupported_logged: &'a std::sync::atomic::AtomicBool,
    /// A missed refresh disables automatic warming across all tool waits in this run.
    pub warm_missed: &'a std::sync::atomic::AtomicBool,
}

impl WarmWait<'_> {
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

fn delay(driver: &dyn KeepWarmDriver, timing: WarmTiming) -> Option<Duration> {
    let sent = driver.last_sent_at_ms()?;
    let interval = timing
        .after_send_ms
        .unwrap_or_else(|| timing.ttl_ms.saturating_sub(timing.lead_ms));
    Some(Duration::from_millis(
        sent.saturating_add(interval).saturating_sub(now_ms()),
    ))
}

/// Start only after the WAL commits the tool calls, not while the run is still deciding what
/// to dispatch. If a tool result and warm timer are ready together, prioritize the result.
/// Keep the warm future here rather than spawning it so results cancel its local IO before
/// the next model request. Cancellation likewise drops the future without awaiting it.
pub async fn wait_tools<F: Future>(tools: F, warm: Option<WarmWait<'_>>) -> F::Output {
    wait_tools_with_spend(tools, warm, 0.0).await
}

pub async fn wait_tools_with_spend<F: Future>(
    tools: F,
    warm: Option<WarmWait<'_>>,
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
        if warm.log_unsupported() {
            tracing::info!(target: "cache_warm", stop = "unsupported", "keep_warm has no positive scheduling interval");
        }
        return tools.await;
    }
    let Some(initial) = delay(warm.driver.as_ref(), timing) else {
        if warm.log_unsupported() {
            tracing::info!(target: "cache_warm", stop = "unsupported", "keep_warm has no cache clock");
        }
        return tools.await;
    };
    tokio::pin!(tools);
    let mut next = Some(tokio::time::Instant::now() + initial);
    let mut tick: Option<std::pin::Pin<Box<dyn Future<Output = WarmTick> + Send + '_>>> = None;
    let mut spent = initial_spend;
    let mut tick_cost = 0.0;
    let mut stopped = false;
    loop {
        tokio::select! {
            biased;
            result = &mut tools => {
                if !stopped {
                    let reason = if warm.cancel.is_some_and(CancellationToken::is_cancelled) { "cancelled" } else { "tools_done" };
                    tracing::info!(target: "cache_warm", stop = reason, "keep_warm stopped");
                }
                return result;
            }
            _ = async { match warm.cancel { Some(cancel) => cancel.cancelled().await, None => std::future::pending().await } }, if !stopped => {
                tick = None;
                next = None;
                stopped = true;
                tracing::info!(target: "cache_warm", stop = "cancelled", "keep_warm stopped");
            }
            outcome = async { match tick.as_mut() { Some(future) => future.await, None => std::future::pending().await } } => {
                tick = None;
                match outcome {
                    WarmTick::Sent => {
                        spent += tick_cost;
                        next = delay(warm.driver.as_ref(), timing).map(|delay| tokio::time::Instant::now() + delay);
                    }
                    WarmTick::ResponsesSent { usage, cost } => {
                        spent += cost;
                        let missed = usage.cached_input_tokens.zip(usage.input_tokens).map(|(cached, fresh)| (cached, cached.saturating_add(fresh)));
                        if let Some((cached, total)) = missed.filter(|(cached, total)| *cached < total.saturating_sub(*cached)) {
                            warm.warm_missed.store(true, std::sync::atomic::Ordering::Relaxed);
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
                        stopped = true;
                        next = None;
                        if reason != "unsupported" || warm.log_unsupported() {
                            tracing::info!(target: "cache_warm", stop = %reason, detail = ?detail, "keep_warm stopped");
                        }
                    }
                }
            }
            _ = async { match next { Some(deadline) => tokio::time::sleep_until(deadline).await, None => std::future::pending().await } }, if tick.is_none() => {
                next = None;
                match warm.driver.costs(warm.config) {
                    Ok(costs) if costs.allows_next(spent) => {
                        tick_cost = costs.warm;
                        tick = Some(warm.driver.warm(warm.config, warm.run_id));
                    }
                    result => {
                        stopped = true;
                        let detail = result.err().unwrap_or_else(|| "one cache bust costs no more than the next warm".into());
                        tracing::info!(target: "cache_warm", stop = "budget", detail = %detail, "keep_warm stopped");
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
                ..
            } if trigger.as_deref() == Some("keep_warm")
                && run_id == warm.run_id
                && *warmed_step == step_id =>
            {
                Some(
                    warm.driver
                        .recorded_cost(warm.config, usage)
                        .unwrap_or(f64::INFINITY),
                )
            }
            _ => None,
        })
        .sum()
}
