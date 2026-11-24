//! A cancellation seal is the durable withdrawal intent. The SQLite outbox retains its
//! registration even after intake pruning, and bounds transport retries: a withdrawal whose route
//! keeps refusing is tried at most `MAX_ATTEMPTS` times while the process runs, and once more,
//! with the same budget, at each start. A provider answering `withdraw_pending` has accepted the
//! withdrawal but can't yet confirm the call stopped, so that withdrawal stays owed and is asked
//! again, with no attempt limit, until the provider answers it.
//!
//! Each `drive` sends one attempt and returns; a retry waits in a task of its own, so neither a
//! run's end nor a recovery sweep waits out a retry delay.

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
/// How often a sweep waiting for the deployment key checks whether it has loaded.
const KEY_WAIT_INTERVAL: Duration = Duration::from_millis(250);

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
    /// This service, for the tasks that send a later retry.
    this: std::sync::Weak<HeldWithdrawals>,
    store: Arc<LateResultsStore>,
    transport: Arc<dyn WithdrawTransport>,
    active: Mutex<HashSet<(String, String)>>,
    /// Withdrawals waiting out a retry delay in their own task.
    waiting: Mutex<HashSet<(String, String)>>,
    health: Mutex<Value>,
    stopping: std::sync::OnceLock<tokio_util::sync::CancellationToken>,
    sweep: Mutex<SweepProgress>,
    store_runtime: std::sync::OnceLock<std::sync::Weak<crate::store_open::StoreRuntime>>,
}

/// What one recovery sweep did, for its closing log line, and the last failure it met.
#[derive(Default)]
struct SweepTally {
    sessions_read: usize,
    rows_settled: usize,
    withdrawals_sent: u32,
    failure: Option<String>,
}

#[derive(Default)]
struct SweepProgress {
    running: bool,
    rows_remaining: usize,
}

impl HeldWithdrawals {
    pub fn new(store: Arc<LateResultsStore>, transport: Arc<dyn WithdrawTransport>) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            this: this.clone(),
            store,
            transport,
            active: Mutex::default(),
            waiting: Mutex::default(),
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
        let mut tally = SweepTally::default();
        // An encrypted session can't be read until the deployment key loads, which a start often
        // reaches the sweep before: such a session waits for the key rather than failing.
        let mut waiting = Vec::new();
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
            // A head that can't be checked is left to the read below, which reports it.
            let wait = runtime.as_ref().is_some_and(|runtime| {
                runtime.holds(&bind).unwrap_or(false)
                    && !runtime.readable_now(&bind).unwrap_or(true)
            });
            if wait {
                waiting.push((bind, rows));
                continue;
            }
            self.sweep_session(runtime, bind, rows, &mut tally).await;
        }
        let runtime = self.store_runtime.get().and_then(std::sync::Weak::upgrade);
        if let (false, Some(runtime)) = (waiting.is_empty(), runtime) {
            while !runtime.key_loaded() && !stopping.is_cancelled() {
                tokio::select! {
                    biased;
                    _ = stopping.cancelled() => {}
                    _ = tokio::time::sleep(KEY_WAIT_INTERVAL) => {}
                }
            }
            for (bind, rows) in waiting {
                if stopping.is_cancelled() {
                    break;
                }
                self.sweep_session(Some(runtime.clone()), bind, rows, &mut tally)
                    .await;
            }
        }
        let SweepTally {
            sessions_read,
            rows_settled,
            withdrawals_sent,
            failure,
        } = tally;
        self.refresh_health();
        tracing::info!(target: "late_results", sessions_read, rows_settled, withdrawals_sent,
            elapsed_ms = started.elapsed().as_millis() as u64, "provider-held withdrawal recovery sweep finished");
        if let Some(error) = failure {
            self.health.lock().expect("withdrawal health")["error"] = json!(error);
            return Err(error);
        }
        Ok(())
    }

    /// Withdraws the owed calls of one session and retires the watches it no longer needs,
    /// adding what it did to `tally`. While no store is installed, the rows stay for a later
    /// sweep. A session no root keeps any more was deleted, and its rows are settled (see
    /// [`Self::settle_deleted`]).
    async fn sweep_session(
        &self,
        runtime: Option<Arc<crate::store_open::StoreRuntime>>,
        bind: broca_protocol::BindIdentity,
        rows: Vec<(LateResultRegistration, String, u32)>,
        tally: &mut SweepTally,
    ) {
        let held = match runtime.as_ref().map(|runtime| runtime.holds(&bind)) {
            Some(Ok(held)) => Some(held),
            Some(Err(error)) => {
                tally.failure = Some(error.to_string());
                None
            }
            None => None,
        };
        if held == Some(false) {
            self.settle_deleted(&bind, &rows, tally);
        } else if let (Some(true), Some(runtime)) = (held, runtime) {
            tally.sessions_read += 1;
            // Whether each call was cancelled while held, from one read of the session's ledger
            // back to the oldest of their intents, rather than one read per call.
            let calls: Vec<_> = rows.iter().map(|(call, _, _)| call.clone()).collect();
            let mut identities = match store::cancellation_identities(&runtime, &calls).await {
                Ok(identities) => Some(identities),
                Err(error) => {
                    tally.failure = Some(error);
                    None
                }
            };
            for (call, state, attempts) in &rows {
                let attempts = if state == "failed" {
                    None
                } else {
                    Some(*attempts)
                };
                let known = identities
                    .as_mut()
                    .and_then(|identities| identities.remove(&call.call_key));
                match self.drive_with(call.clone(), attempts, known).await {
                    Ok(sent) => tally.withdrawals_sent += sent,
                    // Named, so a failure the sweep reports can be traced to its session.
                    Err(error) => {
                        tally.failure = Some(format!(
                            "{} (session {}, call {} of {})",
                            error,
                            broca_core::session_addr(&bind),
                            call.call_key,
                            call.module
                        ));
                    }
                }
            }
            // Every registered call gets a watch, and a call answered without a hold, or whose
            // run ended, never needs one. Retiring those here keeps the outbox from growing with
            // every call. A watch a withdrawal above already attempted stays. A call answered
            // inline gives up its registration too; a held one keeps it for intake, since its
            // provider may still send the result.
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
                let inline: Vec<_> = inline.iter().filter_map(|w| module(&w.call_key)).collect();
                let held: Vec<_> = held.iter().filter_map(|w| module(&w.call_key)).collect();
                match self
                    .store
                    .remove_spent_calls(&inline)
                    .and_then(|inline| Ok(inline + self.store.retire_watches(&held)?))
                {
                    Ok(removed) => tally.rows_settled += removed,
                    Err(error) => tally.failure = Some(error),
                }
            }
        } else {
            // No store is installed yet, or the session's root couldn't be read: its
            // cancellations can't be read, so the rows stay for a later sweep.
            tally.failure = Some(format!(
                "held withdrawals wait: no session store holds {}",
                broca_core::session_addr(&bind)
            ));
        }
        self.sweep.lock().expect("withdrawal sweep").rows_remaining -= rows.len();
        tokio::task::yield_now().await;
    }

    /// Settles the `rows` of the session `bind`, which no root keeps any more: retention or an
    /// operator deleted it. Its calls can't take a result, and none can be withdrawn, since a
    /// withdrawal names the call's identity as the session's ledger recorded it. So the calls'
    /// registrations and unattempted watches go, and a withdrawal already attempted is recorded
    /// as failed with its scope ended, which health reports apart from failures that need
    /// attention. Before this, such rows stayed for a later sweep that could never read them, and
    /// every sweep reported the session as a failure.
    fn settle_deleted(
        &self,
        bind: &broca_protocol::BindIdentity,
        rows: &[(LateResultRegistration, String, u32)],
        tally: &mut SweepTally,
    ) {
        let calls: Vec<_> = rows
            .iter()
            .map(|(call, _, _)| (call.module.clone(), call.call_key.clone()))
            .collect();
        let mut settled = match self.store.remove_spent_calls(&calls) {
            Ok(removed) => removed,
            Err(error) => {
                tally.failure = Some(error);
                return;
            }
        };
        let gone = json!({"code": "scope_ended", "message": "the call's session was deleted"});
        for (call, state, attempts) in rows {
            if state == "scope_gone" || state == "failed" {
                continue;
            }
            // A row the removal above kept had a withdrawal attempted.
            match self.store.withdrawal_state(&call.module, &call.call_key) {
                Ok(Some(_)) => match self
                    .store
                    .record_withdrawal(call, "failed", *attempts, &gone)
                {
                    Ok(()) => settled += 1,
                    Err(error) => tally.failure = Some(error),
                },
                Ok(None) => {}
                Err(error) => tally.failure = Some(error),
            }
        }
        tracing::warn!(target: "late_results", session = %broca_core::session_addr(bind),
            calls = rows.len(), settled, "provider-held calls of a deleted session settled");
        tally.rows_settled += settled;
        self.refresh_health();
    }

    /// Send one attempt to withdraw `call`, counting on from `attempts` sent before, and return
    /// how many were sent. A retryable answer schedules the next attempt. `None` asks a failed
    /// withdrawal again with a fresh budget; otherwise a failed or done withdrawal is left as it
    /// is.
    async fn drive(
        &self,
        call: LateResultRegistration,
        attempts: Option<u32>,
    ) -> Result<u32, String> {
        self.drive_with(call, attempts, None).await
    }

    /// [`Self::drive`], with the call's cancellation identity already read when `known` holds it.
    async fn drive_with(
        &self,
        call: LateResultRegistration,
        attempts: Option<u32>,
        known: Option<Result<Option<bool>, String>>,
    ) -> Result<u32, String> {
        let stopping = self
            .stopping
            .get()
            .ok_or("held withdrawal stop signal is not installed")?;
        if stopping.is_cancelled() {
            return Ok(0);
        }
        let key = (call.module.clone(), call.call_key.clone());
        if self
            .waiting
            .lock()
            .expect("withdrawal waiting")
            .contains(&key)
        {
            return Ok(0);
        }
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
        let attempts = match attempts {
            Some(attempts) => attempts.max(durable_attempts),
            None if retry_failed => 0,
            None => durable_attempts,
        };
        let given = known.is_some();
        // Live callers arrive only after sync. Recovery reads the same sealed log: an open
        // hold, a shutdown pause, or an interrupted restart is never a withdrawal intent.
        let identity = match known {
            Some(identity) => identity?,
            None => self.cancellation_identity(&call).await?,
        };
        let Some(session_identity) = identity else {
            return Ok(0);
        };
        if stopping.is_cancelled() {
            return Ok(0);
        }
        let attempts = attempts + 1;
        self.store
            .record_withdrawal(&call, "pending", attempts, &json!({"attempt": attempts}))?;
        self.refresh_health();
        // tool-provider 0.13.2 CONTRACT.md §5: stored answers are byte-identical for every
        // repeat. A crash after send but before recording its reply is safe.
        let reply = tokio::select! {
            biased;
            _ = stopping.cancelled() => return Ok(0),
            reply = tokio::time::timeout(ATTEMPT_TIMEOUT, self.transport.withdraw(&call, session_identity)) => reply,
        };
        let (verdict, raw) = match reply {
            Ok(result) => result,
            Err(_) => {
                let raw = json!({"code": "module_timeout", "message": "tool.withdraw timed out"});
                (
                    WithdrawVerdict::Retry(
                        serde_json::from_value(raw.clone()).expect("route error decodes"),
                    ),
                    raw,
                )
            }
        };
        let retry = match &verdict {
            // The provider still owes this withdrawal an answer (tool-provider 0.15.12 §5), so
            // it is asked again however many times it took.
            WithdrawVerdict::Retry(error)
                if error.code == cortexkit_role_tool_provider::errors::WITHDRAW_PENDING =>
            {
                Some(error)
            }
            WithdrawVerdict::Retry(error) if attempts < MAX_ATTEMPTS => Some(error),
            _ => None,
        };
        if let Some(error) = retry {
            // A retry asks with the identity this attempt used when it was given rather than
            // read: the session it came from may be gone by then (see
            // [`Self::withdraw_before_delete`]).
            let given = given.then_some(session_identity);
            self.store
                .record_withdrawal(&call, "pending", attempts, &raw)?;
            self.refresh_health();
            // The provider's delay hint, or a doubling backoff, within the contract's 1 to 60
            // second bounds.
            let backoff_ms = 1_000u64.saturating_mul(1 << (attempts - 1).min(6));
            let delay = Duration::from_millis(
                cortexkit_role_tool_provider::errors::withdraw_retry_delay_ms(error, backoff_ms),
            );
            self.retry_later(call, attempts, given, delay, stopping.clone());
            return Ok(1);
        }
        let failed = matches!(
            verdict,
            WithdrawVerdict::Terminal(_)
                | WithdrawVerdict::Retry(_)
                | WithdrawVerdict::Unclassified(_)
        ) || raw.get("answer").and_then(Value::as_str) == Some("refused");
        let state = if failed { "failed" } else { "done" };
        self.store.record_withdrawal(&call, state, attempts, &raw)?;
        self.refresh_health();
        Ok(1)
    }

    /// Asks `call` again after `delay`, counting on from `attempts`, unless the module stops
    /// first. The call stays claimed while it waits, so nothing else sends it meanwhile.
    fn retry_later(
        &self,
        call: LateResultRegistration,
        attempts: u32,
        session_identity: Option<bool>,
        delay: Duration,
        stopping: tokio_util::sync::CancellationToken,
    ) {
        let Some(this) = self.this.upgrade() else {
            return;
        };
        let key = (call.module.clone(), call.call_key.clone());
        this.waiting
            .lock()
            .expect("withdrawal waiting")
            .insert(key.clone());
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = stopping.cancelled() => {}
                _ = tokio::time::sleep(delay) => {}
            }
            this.waiting
                .lock()
                .expect("withdrawal waiting")
                .remove(&key);
            if !stopping.is_cancelled() {
                let known = session_identity.map(|identity| Ok(Some(identity)));
                if let Err(error) = this.drive_with(call, Some(attempts), known).await {
                    this.health.lock().expect("withdrawal health")["error"] = json!(error);
                }
            }
        });
    }

    /// Whether a withdrawal is being sent or waits to be asked again.
    #[cfg(test)]
    pub(crate) fn busy(&self) -> bool {
        !self.active.lock().expect("withdrawal active").is_empty()
            || !self.waiting.lock().expect("withdrawal waiting").is_empty()
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
impl crate::retention::WithdrawBeforeDelete for HeldWithdrawals {
    /// Sends one withdrawal for each of `calls` at once, each bounded by the attempt timeout,
    /// with the identity read from the session's plan before it is deleted. A call already
    /// withdrawn, or never registered here, is skipped. One the provider asks to ask again is
    /// retried later with that same identity, since the session can't be read any more.
    async fn withdraw_before_delete(&self, calls: Vec<crate::retention::HeldAtDeletion>) {
        let attempts = calls.into_iter().map(|held| async move {
            let call = match self.store.withdrawal_call(&held.module, &held.call_key) {
                Ok(Some(call)) => call,
                Ok(None) => return,
                Err(error) => {
                    self.health.lock().expect("withdrawal health")["error"] = json!(error);
                    return;
                }
            };
            let known = Some(Ok(Some(held.session_identity)));
            if let Err(error) = self.drive_with(call, None, known).await {
                self.health.lock().expect("withdrawal health")["error"] = json!(error);
            }
        });
        futures::future::join_all(attempts).await;
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
