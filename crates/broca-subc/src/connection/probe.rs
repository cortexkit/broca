//! Whether a silent connection may be probed now: a Ping waits while the daemon is busy with
//! a request on channel zero, since it answers that channel in order.

use super::*;

pub(super) enum ChannelZeroProbeBlocker {
    Live,
    TombstoneUntil(Instant),
}

/// Distinguishes live channel-0 traffic from retained route.open cleanup state. A live request
/// suppresses both probe writes and conviction because the daemon's connection-scoped FIFO cannot
/// answer a Ping while servicing that request. This is load-bearing for route.open: the daemon
/// answers it only after the target module accepts the bind, which can hold the connection's
/// loop longer than the ordinary response deadline, and a probe behind that healthy work would
/// wrongly declare every route on the socket dead.
///
/// A retained route.open tombstone serves a different purpose: it lets the reader close a route
/// the daemon committed after the caller left. Treating that cleanup identity as live forever also
/// suppresses recovery forever on a dead socket. Tombstones therefore block only through the
/// bounded bind-relay remainder; after that deadline they remain available for cleanup but are no
/// longer evidence that the daemon is busy.
pub(super) fn channel_zero_probe_blocker(
    runtime: &ConnectionRuntime,
    now: Instant,
) -> Option<ChannelZeroProbeBlocker> {
    let waiters = runtime.pending.lock().unwrap();
    let mut latest_tombstone = None;
    for (key, entry) in waiters.iter() {
        if key.0 != 0 {
            continue;
        }
        match entry {
            PendingEntry::Live {
                blocks_probe: true, ..
            } => return Some(ChannelZeroProbeBlocker::Live),
            PendingEntry::Live { .. } => {}
            PendingEntry::LateRouteOpen { blocks_probe_until } if *blocks_probe_until > now => {
                latest_tombstone = Some(
                    latest_tombstone.map_or(*blocks_probe_until, |current: Instant| {
                        current.max(*blocks_probe_until)
                    }),
                );
            }
            PendingEntry::LateRouteOpen { .. } => {}
        }
    }
    latest_tombstone.map(ChannelZeroProbeBlocker::TombstoneUntil)
}

enum ProbeRequestAction {
    None,
    WakeAt(Instant),
    Start {
        runtime: Arc<ConnectionRuntime>,
        corr: u64,
    },
}

/// Start a probe now or remember it until channel-zero traffic stops blocking it. A timeout is a
/// durable request for a health decision: live control work postpones that decision instead of
/// discarding it, while a tombstone supplies the bounded wake-up time.
pub(super) fn request_liveness_probe(
    shared: &Arc<ConnectionShared>,
    runtime: &Arc<ConnectionRuntime>,
) {
    let action = {
        let mut state = shared.current.lock().unwrap();
        if !state.active || state.runtime.conn != runtime.conn {
            ProbeRequestAction::None
        } else if state.probe_in_flight {
            if channel_zero_probe_blocker(runtime, Instant::now()).is_some() {
                // Channel-zero activity will end the current probe as alive. Remember this timeout
                // so that verdict, which is about older traffic, isn't taken as the only health
                // decision for the newer silence.
                state.probe_deferred = true;
            }
            ProbeRequestAction::None
        } else {
            match channel_zero_probe_blocker(runtime, Instant::now()) {
                Some(ChannelZeroProbeBlocker::Live) => {
                    state.probe_deferred = true;
                    state.deferred_probe_wake = None;
                    ProbeRequestAction::None
                }
                Some(ChannelZeroProbeBlocker::TombstoneUntil(deadline)) => {
                    state.probe_deferred = true;
                    if state.deferred_probe_wake == Some(deadline) {
                        ProbeRequestAction::None
                    } else {
                        state.deferred_probe_wake = Some(deadline);
                        ProbeRequestAction::WakeAt(deadline)
                    }
                }
                None => {
                    state.probe_deferred = false;
                    state.deferred_probe_wake = None;
                    state.probe_in_flight = true;
                    ProbeRequestAction::Start {
                        runtime: Arc::clone(runtime),
                        corr: shared.corr.fetch_add(1, Ordering::Relaxed),
                    }
                }
            }
        }
    };

    match action {
        ProbeRequestAction::None => {}
        ProbeRequestAction::WakeAt(deadline) => {
            let owner = Arc::downgrade(shared);
            let conn = runtime.conn;
            tokio::spawn(async move {
                time::sleep_until(time::Instant::from_std(deadline)).await;
                let Some(owner) = owner.upgrade() else {
                    return;
                };
                let runtime = {
                    let mut state = owner.current.lock().unwrap();
                    if !state.active
                        || state.runtime.conn != conn
                        || !state.probe_deferred
                        || state.deferred_probe_wake != Some(deadline)
                    {
                        return;
                    }
                    state.deferred_probe_wake = None;
                    Arc::clone(&state.runtime)
                };
                request_liveness_probe(&owner, &runtime);
            });
        }
        ProbeRequestAction::Start { runtime, corr } => {
            let owner = Arc::downgrade(shared);
            tokio::spawn(async move { run_liveness_probe(owner, runtime, corr).await });
        }
    }
}
