//! Idle cache timers belong to the registry, not to actors that close between turns.

use super::{LineageKey, LineageRegistry, RegistryHandle};
use tokio_util::sync::CancellationToken;

pub(super) struct Scheduled {
    id: u64,
    due_ms: u64,
    cancel: CancellationToken,
}

#[cfg(test)]
#[path = "idle_warm_tests.rs"]
mod tests;

impl RegistryHandle {
    pub(crate) fn stop_idle_warm(&self, key: &LineageKey, reason: &str) {
        self.stop_idle_warm_if(key, reason, None);
    }

    fn stop_idle_warm_if(&self, key: &LineageKey, reason: &str, id: Option<u64>) {
        let mut stops = self.inner.idle_warm_stops.lock().unwrap();
        let mut schedules = self.inner.idle_warms.lock().unwrap();
        if id.is_some_and(|id| schedules.get(key).is_none_or(|s| s.id != id)) {
            return;
        }
        let scheduled = schedules.remove(key);
        if reason == "real_run" {
            stops.remove(key);
        } else if scheduled.is_some() {
            // A read that reopens the actor must not retry a refused warm. A real run, or a
            // process restart rebuilding eligibility from durable records, can try again.
            stops.insert(key.clone());
        }
        drop(schedules);
        drop(stops);
        if let Some(scheduled) = scheduled {
            scheduled.cancel.cancel();
            // A real run taking over is the ordinary end of idle warming, once per turn.
            if reason == "real_run" {
                tracing::debug!(target: "cache_warm", session = %broca_core::session_addr(key),
                    reason, next_due_ms = scheduled.due_ms, "idle cache warming stopped");
            } else {
                tracing::info!(target: "cache_warm", session = %broca_core::session_addr(key),
                    reason, next_due_ms = scheduled.due_ms, "idle cache warming stopped");
            }
        }
    }

    pub(crate) fn has_idle_warm(&self, key: &LineageKey) -> bool {
        self.inner.idle_warms.lock().unwrap().contains_key(key)
    }

    /// When the session's idle warming timer is due, if one is armed.
    #[cfg(test)]
    pub(crate) fn idle_warm_due_ms(&self, key: &LineageKey) -> Option<u64> {
        self.inner
            .idle_warms
            .lock()
            .unwrap()
            .get(key)
            .map(|scheduled| scheduled.due_ms)
    }

    pub(crate) fn arm_idle_warm(&self, key: &LineageKey, due_ms: u64, now_ms: u64) {
        let stops = self.inner.idle_warm_stops.lock().unwrap();
        if self.is_stopping() || stops.contains(key) {
            return;
        }
        let id = self
            .inner
            .next_gen
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let cancel = CancellationToken::new();
        let previous = self.inner.idle_warms.lock().unwrap().insert(
            key.clone(),
            Scheduled {
                id,
                due_ms,
                cancel: cancel.clone(),
            },
        );
        drop(stops);
        if let Some(previous) = previous {
            previous.cancel.cancel();
        }
        tracing::debug!(target: "cache_warm", session = %broca_core::session_addr(key),
            reason = "cache_alive", next_due_ms = due_ms, "idle cache warming armed");
        let inner = std::sync::Arc::downgrade(&self.inner);
        let stopping = self.stopping_signal();
        let key = key.clone();
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(due_ms.saturating_sub(now_ms));
        tokio::spawn(async move {
            let work = async {
                tokio::time::sleep_until(deadline).await;
                let Some(inner) = inner.upgrade() else { return };
                let registry = LineageRegistry { inner };
                let result = registry.route_warm_mode(&key, None, true).await;
                let reason = match result {
                    Ok(crate::actor::WarmOutcome::Warmed(_)) => "warm_not_rearmed",
                    Ok(crate::actor::WarmOutcome::Refused(_)) => "warm_refused",
                    _ => "warm_failed",
                };
                let handle = registry.handle();
                // A completed warm may have armed a newer timer before answering this caller.
                handle.stop_idle_warm_if(&key, reason, Some(id));
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {},
                _ = stopping.cancelled() => {
                    if let Some(inner) = inner.upgrade() {
                        RegistryHandle { inner }.stop_idle_warm_if(&key, "shutdown", Some(id));
                    }
                },
                _ = work => {},
            }
        });
    }
}
