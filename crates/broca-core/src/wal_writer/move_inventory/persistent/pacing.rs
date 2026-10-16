use tokio_util::sync::CancellationToken;

/// Cooperative pacing, not OS priority: sleep three units per unit of payload
/// work for roughly 25% of one worker's CPU duty cycle. An unthrottled 10.5-minute
/// scan of 58,000 production logs would take about 42 minutes on a cold cache,
/// while leaving roughly 75% of that core available. I/O waits count as work,
/// so an I/O-bound rebuild can use less CPU and take longer. Warm-cache metadata
/// checks do not pay these pauses. Cancellation interrupts a pause within 50 ms.
pub(super) fn after_payload(work: std::time::Duration, cancel: &CancellationToken) {
    let pause = work.saturating_mul(3);
    let start = std::time::Instant::now();
    while let Some(remaining) = pause.checked_sub(start.elapsed()) {
        if cancel.is_cancelled() {
            return;
        }
        std::thread::park_timeout(remaining.min(std::time::Duration::from_millis(50)));
    }
}
