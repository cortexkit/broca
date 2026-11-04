use std::sync::{
    atomic::{AtomicU64, Ordering},
    LazyLock, Mutex,
};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub(crate) enum Outcome {
    Finished,
    Failed,
    Unfinished,
}

#[derive(Default)]
struct Stats {
    probes: AtomicU64,
    lookup_failed: AtomicU64,
    lookup_unfinished: AtomicU64,
    warnings: AtomicU64,
    wait_us_total: AtomicU64,
    wait_us_max: AtomicU64,
    last_warning: Mutex<Option<Instant>>,
}

impl Stats {
    fn observe(&self, outcome: Outcome, elapsed: Duration, now: Instant) -> bool {
        self.probes.fetch_add(1, Ordering::Relaxed);
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.wait_us_total.fetch_add(micros, Ordering::Relaxed);
        self.wait_us_max.fetch_max(micros, Ordering::Relaxed);
        match outcome {
            Outcome::Finished => return false,
            Outcome::Failed => {
                self.lookup_failed.fetch_add(1, Ordering::Relaxed);
            }
            Outcome::Unfinished => {
                self.lookup_unfinished.fetch_add(1, Ordering::Relaxed);
            }
        }
        let mut last = self
            .last_warning
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if last.is_some_and(|last| now.saturating_duration_since(last) < Duration::from_secs(60)) {
            return false;
        }
        *last = Some(now);
        self.warnings.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn health(&self) -> serde_json::Value {
        serde_json::json!({
            "probes": self.probes.load(Ordering::Relaxed),
            "lookup_failed": self.lookup_failed.load(Ordering::Relaxed),
            "lookup_unfinished": self.lookup_unfinished.load(Ordering::Relaxed),
            "warnings": self.warnings.load(Ordering::Relaxed),
            "wait_us_total": self.wait_us_total.load(Ordering::Relaxed),
            "wait_us_max": self.wait_us_max.load(Ordering::Relaxed),
        })
    }
}

static STATS: LazyLock<Stats> = LazyLock::new(Stats::default);

pub(crate) fn observe(provider: &str, outcome: Outcome, elapsed: Duration) {
    if STATS.observe(outcome, elapsed, Instant::now()) {
        let cause = match outcome {
            Outcome::Failed => "lookup_failed",
            Outcome::Unfinished => "lookup_unfinished",
            Outcome::Finished => unreachable!("a successful lookup emits no warning"),
        };
        // The error may contain provider data. Log only the discovery status, not its body.
        tracing::warn!(target: "tool_keepalive", provider, cause,
            "tool keepalive discovery unavailable; admitting without a keepalive promise");
    }
}

/// Process-local discovery counters. Wait times describe optional probes, including cache
/// hits; they are not an estimate of production daemon RTT or total admission latency.
pub fn health() -> serde_json::Value {
    STATS.health()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_and_unfinished_probes_are_distinct_and_warnings_are_rate_limited() {
        let stats = Stats::default();
        let now = Instant::now();
        assert!(stats.observe(Outcome::Failed, Duration::from_micros(10), now));
        assert!(!stats.observe(Outcome::Unfinished, Duration::from_micros(20), now));
        assert!(!stats.observe(Outcome::Failed, Duration::from_micros(30), now));
        assert!(!stats.observe(Outcome::Finished, Duration::from_micros(40), now));
        assert!(stats.observe(
            Outcome::Unfinished,
            Duration::from_micros(50),
            now + Duration::from_secs(60)
        ));
        assert_eq!(
            stats.health(),
            serde_json::json!({
                "probes": 5, "lookup_failed": 2, "lookup_unfinished": 2,
                "warnings": 2, "wait_us_total": 150, "wait_us_max": 50,
            })
        );
    }
}
