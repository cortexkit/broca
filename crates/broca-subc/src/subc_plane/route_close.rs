//! Route teardown is retried independently of tool dispatch: a failed Goodbye must not
//! leave a route channel allocated for the lifetime of the shared connection.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct RouteCloser {
    conn: Arc<dyn ToolPlaneConnection>,
    pending: Mutex<(Vec<RouteHandle>, bool)>,
    failures: AtomicU64,
}

impl RouteCloser {
    pub(super) fn new(conn: Arc<dyn ToolPlaneConnection>) -> Arc<Self> {
        Arc::new(Self {
            conn,
            pending: Mutex::new((Vec::new(), false)),
            failures: AtomicU64::new(0),
        })
    }

    async fn attempt(&self, handle: RouteHandle) -> bool {
        match tokio::time::timeout(Duration::from_secs(5), self.conn.route_close(&handle)).await {
            Ok(Ok(()) | Err(ConnError::StaleRouteHandle(_))) => true,
            result => {
                self.failures.fetch_add(1, Ordering::Relaxed);
                // Connection errors may contain provider-supplied text. Log only the outcome,
                // never an error body or the session name.
                tracing::warn!(target: "broca.tool_routes", channel = handle.channel,
                    timed_out = result.is_err(), "tool route Goodbye failed; close queued for retry");
                false
            }
        }
    }

    pub(super) async fn close(self: &Arc<Self>, handle: RouteHandle) -> bool {
        // Actor teardown has its own timeout. Dropping its waiter must not drop the only
        // owner of a failed or congested Goodbye before it reaches the retry queue.
        let closer = Arc::clone(self);
        tokio::spawn(async move { closer.close_inner(handle).await })
            .await
            .unwrap_or(false)
    }

    async fn close_inner(self: &Arc<Self>, handle: RouteHandle) -> bool {
        if self.attempt(handle).await {
            return true;
        }
        let start = {
            let mut pending = self.pending.lock().unwrap();
            let start = !pending.1;
            pending.1 = true;
            if !pending.0.contains(&handle) {
                pending.0.push(handle);
            }
            start
        };
        if start {
            let closer = Arc::clone(self);
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_secs(1)).await;
                    let handles = closer.pending.lock().unwrap().0.clone();
                    for handle in handles {
                        if closer.attempt(handle).await {
                            closer
                                .pending
                                .lock()
                                .unwrap()
                                .0
                                .retain(|item| *item != handle);
                        }
                    }
                    let mut pending = closer.pending.lock().unwrap();
                    if pending.0.is_empty() {
                        pending.1 = false;
                        break;
                    }
                }
            });
        }
        false
    }

    pub(super) fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    pub(super) fn pending(&self) -> usize {
        self.pending.lock().unwrap().0.len()
    }
}
