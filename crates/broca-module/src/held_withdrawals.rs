//! A cancellation seal is the durable withdrawal intent. The SQLite outbox retains its
//! registration even after intake pruning, and bounds transport retries across restarts.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use broca_core::{
    tool_plane::{LateResultRegistration, ProviderHeldReply},
    FileWalStore, WalStore,
};
use broca_protocol::RunFinishReason;
use broca_wal::{DispatchOutcome, FramedRecord, WalRecord};
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
    async fn withdraw(&self, call: &LateResultRegistration) -> (WithdrawVerdict, Value);
}

pub struct HeldWithdrawals {
    store: Arc<LateResultsStore>,
    wal: Arc<FileWalStore>,
    transport: Arc<dyn WithdrawTransport>,
    active: Mutex<HashSet<(String, String)>>,
    health: Mutex<Value>,
    stopping: std::sync::OnceLock<tokio_util::sync::CancellationToken>,
    sweep: Mutex<SweepProgress>,
}

#[derive(Default)]
struct SweepProgress {
    running: bool,
    rows_remaining: usize,
}

impl HeldWithdrawals {
    pub fn new(
        store: Arc<LateResultsStore>,
        wal: Arc<FileWalStore>,
        transport: Arc<dyn WithdrawTransport>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            wal,
            transport,
            active: Mutex::default(),
            health: Mutex::new(json!({"failed": 0, "pending": 0})),
            stopping: std::sync::OnceLock::new(),
            sweep: Mutex::default(),
        })
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
        let health = match self.store.withdrawal_counts() {
            Ok((watching, pending, failed)) => json!({
                "failed": failed, "pending": pending, "watching": watching,
                "failure_details": if failed > 0 { self.store.withdrawal_failure_details().unwrap_or_else(|error| vec![json!({"code": error})]).into_iter().map(|body| json!({"code": body.get("code"), "answer": body.get("answer"), "reason": body.get("reason")})).collect::<Vec<_>>() } else { Vec::new() },
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
        self.store.prune_tombstoned()?;
        let mut sessions: HashMap<_, Vec<_>> = HashMap::new();
        for (call, state, attempts) in self.store.withdrawal_rows()? {
            if state != "failed" {
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
            // One immutable WAL snapshot serves every row in the session, including cleanup.
            // Never let the number of old inline calls multiply whole-log decoding at boot.
            match self.wal.read_records(&bind) {
                Ok(records) => {
                    sessions_read += 1;
                    let facts = broca_core::held_call_facts(&records, None);
                    let inline: Vec<_> = rows
                        .iter()
                        .filter(|(call, state, _)| {
                            state == "watching"
                                && crate::late_results_puller::inline_registration_settled(
                                    &call.call_key,
                                    &records,
                                    &facts,
                                )
                        })
                        .map(|(call, _, _)| (call.module.clone(), call.call_key.clone()))
                        .collect();
                    match self.store.remove_inline_calls(&inline) {
                        Ok(settled) => rows_settled += settled,
                        Err(error) => failure = Some(error),
                    }
                    for (call, _, attempts) in &rows {
                        if stopping.is_cancelled() {
                            break;
                        }
                        if cancelled_hold(&records, &call.call_key) {
                            match self.drive(call.clone(), *attempts, Some(&records)).await {
                                Ok(sent) => withdrawals_sent += sent,
                                Err(error) => failure = Some(error),
                            }
                        }
                    }
                }
                Err(error) => failure = Some(error.to_string()),
            }
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

    async fn drive(
        &self,
        call: LateResultRegistration,
        mut attempts: u32,
        snapshot: Option<&[FramedRecord]>,
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
        if state == "failed" || state == "done" {
            return Ok(0);
        }
        attempts = attempts.max(durable_attempts);
        // Live callers arrive only after sync. Recovery reads the same sealed log: an open
        // hold, a shutdown pause, or an interrupted restart is never a withdrawal intent.
        let records;
        let records = match snapshot {
            Some(records) => records,
            None => {
                records = self
                    .wal
                    .read_records(&call.bind)
                    .map_err(|error| error.to_string())?;
                &records
            }
        };
        if !cancelled_hold(records, &call.call_key) {
            return Ok(0);
        }
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
                reply = tokio::time::timeout(ATTEMPT_TIMEOUT, self.transport.withdraw(&call)) => reply,
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
}

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
            let result = self.drive(call, 0, None).await.map(|_| ());
            if let Err(error) = &result {
                self.health.lock().expect("withdrawal health")["error"] = json!(error);
            }
            return result;
        }
        Ok(())
    }
}

fn cancelled_hold(records: &[FramedRecord], key: &str) -> bool {
    let mut held = None;
    for frame in records {
        match &frame.record {
            WalRecord::RunStarted { .. } => held = None,
            WalRecord::ToolCallProviderHeld {
                call_key,
                batch_id,
                tool_call_id,
                ..
            } if call_key == key => held = Some((batch_id, tool_call_id, true)),
            WalRecord::ToolResult {
                batch_id,
                result,
                outcome,
                reason,
                ..
            } => {
                if let Some((batch, call, eligible)) = &mut held {
                    if *batch == batch_id && *call == &result.tool_call_id {
                        // Only the typed synthetic cancellation terminal preserves the
                        // withdrawal intent. A provider-delivered terminal settles this call,
                        // even if a sibling later causes the whole run to be cancelled.
                        *eligible = *outcome == DispatchOutcome::Interrupted
                            && reason.as_deref() == Some("provider_held_cancelled");
                    }
                }
            }
            WalRecord::ToolNoDispatch {
                batch_id,
                tool_call_id,
                ..
            } => {
                if let Some((batch, call, eligible)) = &mut held {
                    if *batch == batch_id && *call == tool_call_id {
                        *eligible = false;
                    }
                }
            }
            WalRecord::RunFinished {
                reason: RunFinishReason::Cancelled,
                ..
            } if held.is_some_and(|(_, _, eligible)| eligible) => return true,
            WalRecord::RunFinished { .. } => held = None,
            _ => {}
        }
    }
    false
}
