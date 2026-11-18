//! A cancellation seal is the durable withdrawal intent. The SQLite outbox retains its
//! registration even after intake pruning, and bounds transport retries: a withdrawal that keeps
//! failing is tried at most `MAX_ATTEMPTS` times while the process runs, and once more, with the
//! same budget, at each start.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use broca_core::tool_plane::{LateResultRegistration, ProviderHeldReply};
use cortexkit_role_tool_provider::withdraw::WithdrawVerdict;
use serde_json::{json, Value};

use crate::{held_slots::HeldWithdrawal, late_results_store::LateResultsStore};

const MAX_ATTEMPTS: u32 = 3;
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(25);
const RECOVERY_SESSIONS_PER_BATCH: usize = 4;
const RECOVERY_BATCH_INTERVAL: Duration = Duration::from_secs(3);

#[async_trait]
pub trait WithdrawTransport: Send + Sync {
    /// The verdict follows tool-provider caller policy; the raw answer is retained for
    /// unclassified replies and diagnostics rather than guessed into a successful result.
    async fn withdraw(
        &self,
        call: &LateResultRegistration,
        session_identity: bool,
    ) -> (WithdrawVerdict, Value);
}

pub struct HeldWithdrawals {
    store: Arc<LateResultsStore>,
    transport: Arc<dyn WithdrawTransport>,
    active: Mutex<HashSet<(String, String)>>,
    health: Mutex<Value>,
    stopping: std::sync::OnceLock<tokio_util::sync::CancellationToken>,
    sweep: Mutex<SweepProgress>,
    store_runtime: std::sync::OnceLock<std::sync::Weak<crate::store_open::StoreRuntime>>,
}

#[derive(Default)]
struct SweepProgress {
    running: bool,
    rows_remaining: usize,
}

impl HeldWithdrawals {
    pub fn new(store: Arc<LateResultsStore>, transport: Arc<dyn WithdrawTransport>) -> Arc<Self> {
        Arc::new(Self {
            store,
            transport,
            active: Mutex::default(),
            health: Mutex::new(json!({"failed": 0, "pending": 0, "scope_gone": 0})),
            stopping: std::sync::OnceLock::new(),
            sweep: Mutex::default(),
            store_runtime: std::sync::OnceLock::new(),
        })
    }

    /// Read cancellation evidence from the session's own storage before withdrawing its call.
    pub fn with_store_runtime(&self, runtime: Arc<crate::store_open::StoreRuntime>) {
        let _ = self.store_runtime.set(Arc::downgrade(&runtime));
    }

    pub fn health(&self) -> Value {
        let mut health = self.health.lock().expect("withdrawal health").clone();
        let sweep = self.sweep.lock().expect("withdrawal sweep");
        health["sweep_running"] = json!(sweep.running);
        health["rows_remaining"] = json!(sweep.rows_remaining);
        health
    }

    pub fn bind_stopping_signal(&self, stopping: tokio_util::sync::CancellationToken) {
        let _ = self.stopping.set(stopping);
    }

    fn refresh_health(&self) {
        let details = |scope_gone, count| {
            if count == 0 {
                return Vec::new();
            }
            self.store
                .withdrawal_details(scope_gone)
                .unwrap_or_else(|error| vec![json!({"code": error})])
                .into_iter()
                .map(|body| json!({"code": body.get("code"), "answer": body.get("answer"), "reason": body.get("reason")}))
                .collect::<Vec<_>>()
        };
        let health = match self.store.withdrawal_counts() {
            Ok((watching, pending, failed, scope_gone)) => json!({
                "failed": failed, "pending": pending, "watching": watching,
                "failure_details": details(false, failed),
                "scope_gone": scope_gone,
                "scope_gone_details": details(true, scope_gone),
            }),
            Err(error) => json!({"error": error}),
        };
        *self.health.lock().expect("withdrawal health") = health;
    }

    /// Reconcile the seal-to-send crash gap, without opening a run or re-dispatching a call.
    pub async fn recover(&self) -> Result<(), String> {
        // The runtime's existing registration/feed snapshot discovers history first. A
        // never-held root must not open an extra SQLite read transaction for the outbox.
        if !self.store.has_held_history() {
            return Ok(());
        }
        let stopping = self
            .stopping
            .get()
            .ok_or("held withdrawal stop signal is not installed")?;
        if stopping.is_cancelled() {
            return Ok(());
        }
        {
            let mut sweep = self.sweep.lock().expect("withdrawal sweep");
            if sweep.running {
                return Ok(());
            }
            sweep.running = true;
        }
        let _sweep = SweepGuard(&self.sweep);
        let mut sessions: HashMap<_, Vec<_>> = HashMap::new();
        // A failed withdrawal is asked again once per start, with a fresh retry budget: what made
        // the provider refuse may be gone since (it restarted, was fixed, or its task ended), and
        // until it answers the row keeps the module's health degraded with nothing to clear it.
        // A withdrawal whose scope is gone never is: no route can carry it any more.
        for (call, state, attempts) in self.store.withdrawal_rows()? {
            if state != "scope_gone" {
                sessions
                    .entry(call.bind.clone())
                    .or_default()
                    .push((call, state, attempts));
            }
        }
        let mut sessions: Vec<_> = sessions.into_iter().collect();
        sessions.sort_by(|(a, _), (b, _)| a.session.cmp(&b.session));
        self.sweep.lock().expect("withdrawal sweep").rows_remaining =
            sessions.iter().map(|(_, rows)| rows.len()).sum();
        let started = std::time::Instant::now();
        let mut sessions_read = 0;
        let mut rows_settled = 0;
        let mut withdrawals_sent = 0;
        let mut failure = None;
        for (index, (bind, rows)) in sessions.into_iter().enumerate() {
            if stopping.is_cancelled() {
                break;
            }
            if index > 0 && index % RECOVERY_SESSIONS_PER_BATCH == 0 {
                tokio::select! {
                    biased;
                    _ = stopping.cancelled() => break,
                    _ = tokio::time::sleep(RECOVERY_BATCH_INTERVAL) => {}
                }
            }
            let runtime = self.store_runtime.get().and_then(std::sync::Weak::upgrade);
            let held = runtime
                .as_ref()
                .map(|runtime| runtime.holds(&bind).map_err(|error| error.to_string()))
                .transpose()?
                .unwrap_or(false);
            if let (true, Some(runtime)) = (held, runtime) {
                sessions_read += 1;
                for (call, state, attempts) in &rows {
                    let attempts = if state == "failed" {
                        None
                    } else {
                        Some(*attempts)
                    };
                    match self.drive(call.clone(), attempts).await {
                        Ok(sent) => withdrawals_sent += sent,
                        Err(error) => failure = Some(error),
                    }
                }
                // Every registered call gets a watch, and a call answered without a hold, or
                // whose run ended, never needs one. Retiring those here keeps the outbox from
                // growing with every call. A watch a withdrawal above already attempted stays.
                // A call answered inline gives up its registration too; a held one keeps it for
                // intake, since its provider may still send the result.
                let keys: Vec<String> = rows
                    .iter()
                    .map(|(call, _, _)| call.call_key.clone())
                    .collect();
                if let Some(settled) =
                    crate::late_results_puller::settled_watches(&runtime, &bind, &keys).await
                {
                    let module = |key: &str| {
                        rows.iter()
                            .find(|(call, _, _)| call.call_key == key)
                            .map(|(call, _, _)| (call.module.clone(), key.to_owned()))
                    };
                    let (inline, held): (Vec<_>, Vec<_>) =
                        settled.into_iter().partition(|watch| watch.inline);
                    let inline: Vec<_> =
                        inline.iter().filter_map(|w| module(&w.call_key)).collect();
                    let held: Vec<_> = held.iter().filter_map(|w| module(&w.call_key)).collect();
                    match self
                        .store
                        .remove_spent_calls(&inline)
                        .and_then(|inline| Ok(inline + self.store.retire_watches(&held)?))
                    {
                        Ok(removed) => rows_settled += removed,
                        Err(error) => failure = Some(error),
                    }
                }
                self.sweep.lock().expect("withdrawal sweep").rows_remaining -= rows.len();
                tokio::task::yield_now().await;
                continue;
            }
            // The store doesn't hold the session, or isn't installed yet: its cancellations
            // can't be read, so the rows stay for a later sweep.
            failure = Some(format!(
                "held withdrawals wait: no session store holds {}",
                broca_core::session_addr(&bind)
            ));
            self.sweep.lock().expect("withdrawal sweep").rows_remaining -= rows.len();
            tokio::task::yield_now().await;
        }
        self.refresh_health();
        tracing::info!(target: "late_results", sessions_read, rows_settled, withdrawals_sent,
            elapsed_ms = started.elapsed().as_millis() as u64, "provider-held withdrawal recovery sweep finished");
        if let Some(error) = failure {
            self.health.lock().expect("withdrawal health")["error"] = json!(error);
            return Err(error);
        }
        Ok(())
    }

    /// Withdraw `call`, counting on from `attempts` sent before. `None` asks a failed withdrawal
    /// again with a fresh budget; otherwise a failed or done withdrawal is left as it is.
    async fn drive(
        &self,
        call: LateResultRegistration,
        attempts: Option<u32>,
    ) -> Result<u32, String> {
        let stopping = self
            .stopping
            .get()
            .ok_or("held withdrawal stop signal is not installed")?;
        if stopping.is_cancelled() {
            return Ok(0);
        }
        let key = (call.module.clone(), call.call_key.clone());
        if !self
            .active
            .lock()
            .expect("withdrawal active")
            .insert(key.clone())
        {
            return Ok(0);
        }
        let _guard = Active {
            set: &self.active,
            key,
        };
        let Some((state, durable_attempts)) =
            self.store.withdrawal_state(&call.module, &call.call_key)?
        else {
            return Ok(0);
        };
        let retry_failed = attempts.is_none() && state == "failed";
        if state == "done" || (state == "failed" && !retry_failed) {
            return Ok(0);
        }
        let mut attempts = match attempts {
            Some(attempts) => attempts.max(durable_attempts),
            None if retry_failed => 0,
            None => durable_attempts,
        };
        // Live callers arrive only after sync. Recovery reads the same sealed log: an open
        // hold, a shutdown pause, or an interrupted restart is never a withdrawal intent.
        let Some(session_identity) = self.cancellation_identity(&call).await? else {
            return Ok(0);
        };
        let mut sent = 0;
        while attempts < MAX_ATTEMPTS {
            if stopping.is_cancelled() {
                return Ok(sent);
            }
            attempts += 1;
            self.store.record_withdrawal(
                &call,
                "pending",
                attempts,
                &json!({"attempt": attempts}),
            )?;
            self.refresh_health();
            // tool-provider 0.13.2 CONTRACT.md §5: stored answers are byte-identical for
            // every repeat. A crash after send but before recording its reply is safe.
            let reply = tokio::select! {
                biased;
                _ = stopping.cancelled() => return Ok(sent),
                reply = tokio::time::timeout(ATTEMPT_TIMEOUT, self.transport.withdraw(&call, session_identity)) => reply,
            };
            sent += 1;
            let (verdict, raw) = match reply {
                Ok(result) => result,
                Err(_) => (
                    WithdrawVerdict::Retry(
                        serde_json::from_value(
                            json!({"code": "module_timeout", "message": "tool.withdraw timed out"}),
                        )
                        .expect("route error decodes"),
                    ),
                    json!({"code": "module_timeout", "message": "tool.withdraw timed out"}),
                ),
            };
            let retry = verdict.retries();
            let failed = matches!(
                verdict,
                WithdrawVerdict::Terminal(_)
                    | WithdrawVerdict::Retry(_)
                    | WithdrawVerdict::Unclassified(_)
            ) || raw.get("answer").and_then(Value::as_str) == Some("refused");
            let state = if retry && attempts < MAX_ATTEMPTS {
                "pending"
            } else if failed {
                "failed"
            } else {
                "done"
            };
            self.store.record_withdrawal(&call, state, attempts, &raw)?;
            self.refresh_health();
            if !retry || attempts == MAX_ATTEMPTS {
                return Ok(sent);
            }
            tokio::select! {
                biased;
                _ = stopping.cancelled() => return Ok(sent),
                _ = tokio::time::sleep(Duration::from_millis(100 * u64::from(attempts))) => {}
            }
        }
        self.store.record_withdrawal(
            &call,
            "failed",
            attempts,
            &json!({"code": "withdrawal_retry_exhausted", "message": "withdrawal retry budget exhausted"}),
        )?;
        self.refresh_health();
        Ok(sent)
    }

    /// Whether `call` was cancelled while held, and if so whether its withdrawal names the
    /// session's identity. `None` when it wasn't cancelled, or the store doesn't hold it.
    async fn cancellation_identity(
        &self,
        call: &LateResultRegistration,
    ) -> Result<Option<bool>, String> {
        let Some(runtime) = self.store_runtime.get().and_then(std::sync::Weak::upgrade) else {
            return Ok(None);
        };
        if !runtime
            .holds(&call.bind)
            .map_err(|error| error.to_string())?
        {
            return Ok(None);
        }
        store::cancellation_identity(&runtime, call).await
    }
}

mod store;

struct SweepGuard<'a>(&'a Mutex<SweepProgress>);

impl Drop for SweepGuard<'_> {
    fn drop(&mut self) {
        self.0.lock().expect("withdrawal sweep").running = false;
    }
}

struct Active<'a> {
    set: &'a Mutex<HashSet<(String, String)>>,
    key: (String, String),
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.set
            .lock()
            .expect("withdrawal active")
            .remove(&self.key);
    }
}

#[async_trait]
impl HeldWithdrawal for HeldWithdrawals {
    async fn withdraw(&self, reply: &ProviderHeldReply) -> Result<(), String> {
        if let Some(call) = self.store.withdrawal_call(&reply.module, &reply.call_key)? {
            let result = self.drive(call, Some(0)).await.map(|_| ());
            if let Err(error) = &result {
                self.health.lock().expect("withdrawal health")["error"] = json!(error);
            }
            return result;
        }
        Ok(())
    }
}
