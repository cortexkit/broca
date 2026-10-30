//! Bounded ownership of failed Goodbyes, shared by session and late-open cleanup.

use crate::connection::{ConnError, RouteHandle};
use futures::future::{join_all, BoxFuture};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::{
    sync::oneshot,
    time::{sleep_until, timeout_at, Instant},
};
use tokio_util::sync::CancellationToken;

// This is a cleanup budget, not a limit on live routes or the actor's idle TTL.
pub(crate) const MAX_PENDING_GOODBYES: usize = 1024;
const MAX_CLOSE_AGE: Duration = Duration::from_secs(30);
const CLOSE_PASS_BUDGET: Duration = Duration::from_secs(5);
const RETRY_TICK: Duration = Duration::from_secs(1);

/// Real connection targets snapshot a socket generation without retaining its owner while waiting.
pub(crate) trait CloseTarget: Send + Sync {
    fn attempt(&self, handle: RouteHandle) -> BoxFuture<'static, Result<(), ConnError>>;
    fn abandon(&self, handle: RouteHandle);
}
struct Entry {
    since: Instant,
    waiters: Vec<oneshot::Sender<bool>>,
}
#[derive(Clone, Copy, Default)]
enum Admission {
    // Test adapters need not represent a socket generation.
    #[default]
    Unfenced,
    Active(u64),
    Retired,
}
#[derive(Default)]
struct Pending {
    routes: HashMap<RouteHandle, Entry>,
    running: bool,
    admission: Admission,
}
pub(crate) struct RouteCloser {
    target: Arc<dyn CloseTarget>,
    pending: Mutex<Pending>,
    shutdown: CancellationToken,
    failures: AtomicU64,
    overflows: AtomicU64,
    giveups: AtomicU64,
}
impl RouteCloser {
    pub(crate) fn new(target: Arc<dyn CloseTarget>) -> Arc<Self> {
        Arc::new(Self {
            target,
            pending: Mutex::new(Pending::default()),
            shutdown: CancellationToken::new(),
            failures: AtomicU64::new(0),
            overflows: AtomicU64::new(0),
            giveups: AtomicU64::new(0),
        })
    }
    pub(crate) async fn close(self: &Arc<Self>, handle: RouteHandle) -> bool {
        let (tx, rx) = oneshot::channel();
        self.enqueue(handle, Some(tx));
        rx.await.unwrap_or(false)
    }
    /// The ingress reader has already tried and failed to enqueue this Goodbye. Only
    /// transfer ownership here: waiting for writer capacity would stop unrelated replies.
    pub(crate) fn enqueue_failed(self: &Arc<Self>, handle: RouteHandle) -> bool {
        self.failed(handle, false);
        self.enqueue(handle, None)
    }
    fn enqueue(
        self: &Arc<Self>,
        handle: RouteHandle,
        waiter: Option<oneshot::Sender<bool>>,
    ) -> bool {
        let mut pending = self.pending.lock().unwrap();
        if self.shutdown.is_cancelled() {
            return false;
        }
        // Admission and the loss hook use the same lock: a handoff that was prepared before
        // deactivation cannot recreate retry ownership after that hook has returned.
        if matches!(pending.admission, Admission::Retired)
            || matches!(pending.admission, Admission::Active(generation) if generation != handle.generation())
        {
            if let Some(waiter) = waiter {
                let _ = waiter.send(true);
            }
            return false;
        }
        if !pending.routes.contains_key(&handle) && pending.routes.len() == MAX_PENDING_GOODBYES {
            drop(pending);
            self.overflows.fetch_add(1, Ordering::Relaxed);
            self.target.abandon(handle);
            tracing::warn!(target: "broca.tool_routes", channel = handle.channel,
                "Goodbye retry queue full; giving up route cleanup");
            return false;
        }
        let entry = pending.routes.entry(handle).or_insert_with(|| Entry {
            since: Instant::now(),
            waiters: Vec::new(),
        });
        if let Some(waiter) = waiter {
            entry.waiters.push(waiter);
        }
        if pending.running {
            return true;
        }
        pending.running = true;
        drop(pending);
        let weak = Arc::downgrade(self);
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                let Some(closer) = weak.upgrade() else {
                    return;
                };
                let deadline = Instant::now() + CLOSE_PASS_BUDGET;
                let snapshot: Vec<_> = closer
                    .pending
                    .lock()
                    .unwrap()
                    .routes
                    .iter()
                    .map(|(handle, entry)| (*handle, entry.since + MAX_CLOSE_AGE))
                    .collect();
                // Every route gets the same pass budget, so N congested routes cost at most
                // one budget rather than N sequential writer-queue timeouts.
                join_all(snapshot.into_iter().map(|(handle, expires)| {
                    let closer = &closer;
                    async move {
                        if Instant::now() >= expires { closer.give_up(handle); return; }
                        let outcome = tokio::select! {
                            biased;
                            _ = closer.shutdown.cancelled() => return,
                            result = timeout_at(deadline.min(expires), closer.target.attempt(handle)) => result,
                        };
                        let success = matches!(outcome, Ok(Ok(()) | Err(ConnError::StaleRouteHandle(_))));
                        if !success { closer.failed(handle, outcome.is_err()); }
                        if !success && Instant::now() >= expires { closer.give_up(handle); return; }
                        let mut pending = closer.pending.lock().unwrap();
                        if success {
                            if let Some(entry) = pending.routes.remove(&handle) {
                                for waiter in entry.waiters { let _ = waiter.send(true); }
                            }
                        } else if let Some(entry) = pending.routes.get_mut(&handle) {
                            for waiter in entry.waiters.drain(..) { let _ = waiter.send(false); }
                        }
                    }
                })).await;
                let wake_at = {
                    let mut pending = closer.pending.lock().unwrap();
                    if pending.routes.is_empty() || closer.shutdown.is_cancelled() {
                        pending.running = false;
                        return;
                    }
                    let expiry = pending
                        .routes
                        .values()
                        .map(|entry| entry.since + MAX_CLOSE_AGE)
                        .min()
                        .expect("a nonempty cleanup queue has an expiry");
                    (Instant::now() + RETRY_TICK).min(expiry)
                };
                drop(closer);
                tokio::select! { biased; _ = shutdown.cancelled() => return, _ = sleep_until(wake_at) => {} }
            }
        });
        true
    }
}

impl RouteCloser {
    fn failed(&self, handle: RouteHandle, timed_out: bool) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        // Provider error text may contain private data; log only identity and outcome.
        tracing::warn!(target: "broca.tool_routes", channel = handle.channel,
            timed_out, "tool route Goodbye failed; close queued for retry");
    }
    fn give_up(&self, handle: RouteHandle) {
        let entry = self.pending.lock().unwrap().routes.remove(&handle);
        if let Some(entry) = entry {
            self.giveups.fetch_add(1, Ordering::Relaxed);
            self.target.abandon(handle);
            for waiter in entry.waiters {
                let _ = waiter.send(false);
            }
            tracing::warn!(target: "broca.tool_routes", channel = handle.channel,
                "Goodbye retry age limit reached; giving up route cleanup");
        }
    }
    /// Called synchronously by generation deactivation, before any later retry can run.
    pub(crate) fn connection_lost(&self, generation: u64) {
        let mut pending = self.pending.lock().unwrap();
        if !matches!(pending.admission, Admission::Active(active) if active != generation) {
            pending.admission = Admission::Retired;
        }
        pending.routes.retain(|handle, entry| {
            if handle.generation() != generation {
                return true;
            }
            for waiter in entry.waiters.drain(..) {
                let _ = waiter.send(true);
            }
            false
        });
    }
    /// Called under the connection generation lock, never while holding a queue guard.
    pub(crate) fn connection_active(&self, generation: u64) {
        let mut pending = self.pending.lock().unwrap();
        pending.admission = Admission::Active(generation);
    }
    pub(crate) fn stop(&self) {
        self.shutdown.cancel();
        self.pending.lock().unwrap().routes.clear();
    }
    pub(crate) fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }
    pub(crate) fn pending(&self) -> usize {
        self.pending.lock().unwrap().routes.len()
    }
    pub(crate) fn overflows(&self) -> u64 {
        self.overflows.load(Ordering::Relaxed)
    }
    pub(crate) fn giveups(&self) -> u64 {
        self.giveups.load(Ordering::Relaxed)
    }
}
impl Drop for RouteCloser {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
