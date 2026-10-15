use std::future::Future;
use std::time::Duration;

/// Summarize the last 32 debug enum variants without dumping record payloads.
pub fn record_kinds<T: std::fmt::Debug>(records: impl IntoIterator<Item = T>) -> String {
    let kinds: Vec<_> = records
        .into_iter()
        .map(|record| {
            format!("{record:?}")
                .split([' ', '{', '('])
                .next()
                .unwrap_or("?")
                .to_owned()
        })
        .collect();
    format!(
        "records={}, tail={:?}",
        kinds.len(),
        &kinds[kinds.len().saturating_sub(32)..]
    )
}

/// A deadlock guard, not a latency requirement. Real WAL and SQLite commits can
/// take seconds on a busy test runner even when the state transition is correct.
pub const EVENT_HANG_GUARD: Duration = Duration::from_secs(60);

/// Synchronous counterpart for subprocess markers and blocking store probes.
/// The probe itself must return; this guard cannot interrupt blocking I/O.
pub fn wait_until_sync<T>(description: &str, mut observe: impl FnMut() -> Result<T, String>) -> T {
    let deadline = std::time::Instant::now() + EVENT_HANG_GUARD;
    loop {
        match observe() {
            Ok(value) => return value,
            Err(summary) => assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {description} after {EVENT_HANG_GUARD:?}; last observed: {summary}"
            ),
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Await one event with the same hang guard as [`wait_until`]. The diagnostic
/// callback runs only on timeout, so it can inspect a WAL or registry snapshot.
/// Wrap the whole polling loop, not each iteration, so an async probe that never
/// answers cannot evade the hang guard or reset it with partial progress.
pub async fn wait_for<T>(
    description: &str,
    event: impl Future<Output = T>,
    diagnostic: impl FnOnce() -> String,
) -> T {
    event_timeout(description, event, diagnostic)
        .await
        .unwrap_or_else(|_| {
            panic!("timed out waiting for {description} after {EVENT_HANG_GUARD:?}")
        })
}

/// Like [`wait_for`], but preserve the timeout result for tests that must release
/// a blocking fixture or reap a task before asserting success.
pub async fn event_timeout<T>(
    description: &str,
    event: impl Future<Output = T>,
    diagnostic: impl FnOnce() -> String,
) -> Result<T, tokio::time::error::Elapsed> {
    let result = tokio::time::timeout(EVENT_HANG_GUARD, event).await;
    if result.is_err() {
        eprintln!(
            "timed out waiting for {description} after {EVENT_HANG_GUARD:?}; observed: {}",
            diagnostic()
        );
    }
    result
}

/// Poll an eventual-state assertion, reporting its last observation on a hang.
/// Return `Ok(value)` when ready or `Err(summary)` describing the current state.
/// Use only with a running clock; latency tests should keep their own budgets.
pub async fn wait_until<T, F, Fut>(description: &str, observe: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    wait_with_guard(description, EVENT_HANG_GUARD, observe).await
}

async fn wait_with_guard<T, F, Fut>(description: &str, guard: Duration, mut observe: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let mut last_seen = "first observation has not completed".to_owned();
    let result = tokio::time::timeout(guard, async {
        loop {
            match observe().await {
                Ok(value) => return value,
                Err(summary) => last_seen = summary,
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    result.unwrap_or_else(|_| {
        panic!("timed out waiting for {description} after {guard:?}; last observed: {last_seen}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn successful_event_does_not_read_failure_diagnostics() {
        let observed = wait_for("ready event", async { 7 }, || {
            panic!("a ready event must not run failure diagnostics")
        })
        .await;
        assert_eq!(observed, 7);
    }

    #[tokio::test(start_paused = true)]
    async fn whole_event_guard_bounds_a_stuck_async_probe_and_reports_state() {
        let diagnosed = std::cell::Cell::new(false);
        let start = tokio::time::Instant::now();
        let result = event_timeout("stuck registry reply", std::future::pending::<()>(), || {
            diagnosed.set(true);
            "registry reply is pending".to_owned()
        })
        .await;
        assert!(result.is_err());
        assert!(diagnosed.get(), "the guard must report state on timeout");
        assert_eq!(start.elapsed(), Duration::from_secs(60));
    }

    #[tokio::test]
    async fn returns_the_successful_observation() {
        let mut polls = 0;
        let observed = wait_until("three observations", || {
            polls += 1;
            std::future::ready(if polls == 3 {
                Ok(polls)
            } else {
                Err(format!("polls={polls}"))
            })
        })
        .await;
        assert_eq!(observed, 3);
    }

    #[tokio::test(start_paused = true)]
    #[should_panic(
        expected = "waiting for RunPaused after 1s; last observed: records=[RunStarted]"
    )]
    async fn hang_names_the_event_and_last_state() {
        wait_with_guard("RunPaused", Duration::from_secs(1), || async {
            Err::<(), _>("records=[RunStarted]".to_owned())
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    #[should_panic(expected = "last observed: first observation has not completed")]
    async fn also_bounds_a_stuck_observation() {
        wait_with_guard("stuck status read", Duration::from_secs(1), || {
            std::future::pending::<Result<(), String>>()
        })
        .await;
    }
}
