//! A cancellation seal is the durable withdrawal intent. The SQLite outbox retains its
//! registration even after intake pruning, and bounds transport retries across restarts.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use broca_core::{
    tool_plane::{LateResultRegistration, ProviderHeldReply},
    FileWalStore, WalStore,
};
use broca_protocol::RunFinishReason;
use broca_wal::{FramedRecord, WalRecord};
use cortexkit_role_tool_provider::withdraw::WithdrawVerdict;
use serde_json::{json, Value};

use crate::{held_slots::HeldWithdrawal, late_results_store::LateResultsStore};

const MAX_ATTEMPTS: u32 = 3;
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(25);

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
        })
    }

    pub fn health(&self) -> Value {
        self.health.lock().expect("withdrawal health").clone()
    }

    fn refresh_health(&self) {
        let health = match self.store.withdrawal_rows() {
            Ok(rows) => json!({
                "failed": rows.iter().filter(|(_, state, _)| state == "failed").count(),
                "pending": rows.iter().filter(|(_, state, _)| state == "pending").count(),
                "failure_details": if rows.iter().any(|(_, state, _)| state == "failed") { self.store.withdrawal_failure_details().unwrap_or_else(|error| vec![json!({"code": error})]).into_iter().map(|body| json!({"code": body.get("code"), "answer": body.get("answer"), "reason": body.get("reason")})).collect::<Vec<_>>() } else { Vec::new() },
            }),
            Err(error) => json!({"error": error}),
        };
        *self.health.lock().expect("withdrawal health") = health;
    }

    /// Reconcile the seal-to-send crash gap, without opening a run or re-dispatching a call.
    pub async fn recover(&self) -> Result<(), String> {
        let mut failure = None;
        for (call, state, attempts) in self.store.withdrawal_rows()? {
            if state != "failed" {
                if let Err(error) = self.drive(call, attempts).await {
                    failure = Some(error);
                }
            }
        }
        self.refresh_health();
        if let Some(error) = failure {
            self.health.lock().expect("withdrawal health")["error"] = json!(error);
            return Err(error);
        }
        Ok(())
    }

    async fn drive(&self, call: LateResultRegistration, mut attempts: u32) -> Result<(), String> {
        let key = (call.module.clone(), call.call_key.clone());
        if !self
            .active
            .lock()
            .expect("withdrawal active")
            .insert(key.clone())
        {
            return Ok(());
        }
        let _guard = Active {
            set: &self.active,
            key,
        };
        let Some((_, state, durable_attempts)) = self
            .store
            .withdrawal_rows()?
            .into_iter()
            .find(|(row, _, _)| row.module == call.module && row.call_key == call.call_key)
        else {
            return Ok(());
        };
        if state == "failed" {
            return Ok(());
        }
        attempts = attempts.max(durable_attempts);
        // Live callers arrive only after sync. Recovery reads the same sealed log: an open
        // hold, a shutdown pause, or an interrupted restart is never a withdrawal intent.
        let records = self
            .wal
            .read_records(&call.bind)
            .map_err(|error| error.to_string())?;
        if !cancelled_hold(&records, &call.call_key) {
            return Ok(());
        }
        while attempts < MAX_ATTEMPTS {
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
            let (verdict, raw) = match tokio::time::timeout(
                ATTEMPT_TIMEOUT,
                self.transport.withdraw(&call),
            )
            .await
            {
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
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100 * u64::from(attempts))).await;
        }
        self.store.record_withdrawal(
            &call,
            "failed",
            attempts,
            &json!({"code": "withdrawal_retry_exhausted", "message": "withdrawal retry budget exhausted"}),
        )?;
        self.refresh_health();
        Ok(())
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
        for (call, state, attempts) in self.store.withdrawal_rows()? {
            if call.module == reply.module && call.call_key == reply.call_key && state != "failed" {
                let result = self.drive(call, attempts).await;
                if let Err(error) = &result {
                    self.health.lock().expect("withdrawal health")["error"] = json!(error);
                }
                return result;
            }
        }
        Ok(())
    }
}

fn cancelled_hold(records: &[FramedRecord], key: &str) -> bool {
    let mut held = false;
    for frame in records {
        match &frame.record {
            WalRecord::RunStarted { .. } => held = false,
            WalRecord::ToolCallProviderHeld { call_key, .. } if call_key == key => held = true,
            WalRecord::RunFinished {
                reason: RunFinishReason::Cancelled,
                ..
            } if held => return true,
            WalRecord::RunFinished { .. } => held = false,
            _ => {}
        }
    }
    false
}
