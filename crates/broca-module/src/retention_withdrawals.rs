//! Retained questions are serviced from identity-only rows, independently of session actors.
use crate::approval::{
    withdrawal_step, RefusalWindows, Refused, WithdrawalStep, WITHDRAWAL_RETRY_EXHAUSTED,
    WITHDRAWAL_RETRY_WINDOW,
};
use async_trait::async_trait;
use broca_core::tool_plane::{ApprovalWithdrawal, BatchCtx, ElicitationPlane};
use broca_protocol::{BatchId, RunId};
use broca_store::retention_withdrawals::{RetentionQuestionId, RetentionWithdrawal};
use std::sync::Arc;

#[async_trait]
pub trait RetentionWithdrawalAdapter: Send + Sync {
    async fn withdraw(&self, question: &RetentionWithdrawal, id: &str) -> ApprovalWithdrawal;
}

pub struct ScopedRetentionWithdrawalAdapter(pub Arc<dyn ElicitationPlane>);
#[async_trait]
impl RetentionWithdrawalAdapter for ScopedRetentionWithdrawalAdapter {
    async fn withdraw(&self, question: &RetentionWithdrawal, id: &str) -> ApprovalWithdrawal {
        let Some(scope) = &question.scope else {
            return ApprovalWithdrawal::NotSent {
                reason: "scope_unknown".into(),
            };
        };
        let ctx = BatchCtx {
            // Retention withdrawals do not capture tool media.
            media_capture: None,
            batch_id: BatchId(format!("retention:{}", question.call_key)),
            bind: question.session.clone(),
            run_id: RunId(format!("retention:{}", question.lineage_id)),
            scope: Some(scope.to_scope()),
        };
        self.0.withdraw(&ctx, id).await
    }
}

impl super::retention::RetentionService {
    /// Provider retries run concurrently with deletion so even a hung withdrawal route
    /// cannot extend the lifetime of expired transcript content.
    pub(crate) async fn run_withdrawal_worker(&self) {
        let mut backoff = std::time::Duration::from_millis(100);
        let mut changes = self.clock.subscribe();
        let mut refusals = RefusalWindows::default();
        let mut store_failures = RefusalWindows::<()>::default();
        loop {
            let empty = match self.withdraw_retained_questions_with(&mut refusals).await {
                Ok(empty) => {
                    if let Some(window) = store_failures.settled(&()) {
                        tracing::info!(target: "retention", failures = window.refusals, "content-free withdrawal store is readable again");
                    }
                    empty
                }
                Err(error) => {
                    match store_failures.refused(&(), &error, tokio::time::Instant::now()) {
                        Refused::First => {
                            tracing::warn!(target: "retention", retry_window_s = WITHDRAWAL_RETRY_WINDOW.as_secs(), "content-free withdrawal retry; retrying quietly: {error}");
                        }
                        Refused::Retrying => {}
                        Refused::Exhausted(window) => {
                            // The pending rows stay in the store, so the next process retries
                            // them. This worker parks rather than returning: the retention
                            // task treats a finished worker as its own end.
                            tracing::error!(target: "retention", failures = window.refusals, "content-free withdrawals failed for {}s; no more attempts until restart: {error}", WITHDRAWAL_RETRY_WINDOW.as_secs());
                            std::future::pending::<()>().await;
                        }
                    }
                    false
                }
            };
            if empty {
                backoff = std::time::Duration::from_millis(100);
                self.withdrawal_wake.notified().await;
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {},
                    _ = self.withdrawal_wake.notified() => {},
                    _ = changes.changed() => {},
                }
                backoff = backoff
                    .saturating_mul(2)
                    .min(std::time::Duration::from_secs(5));
            }
        }
    }

    /// One pass with fresh refusal windows, so a refused question is never given up on.
    #[cfg(test)]
    pub(crate) async fn withdraw_retained_questions(&self) -> Result<bool, String> {
        self.withdraw_retained_questions_with(&mut RefusalWindows::default())
            .await
    }

    /// A pass makes no WAL or actor access. Failed storage operations return an error;
    /// refused withdrawals stay pending until they settle or their retry window ends.
    pub(crate) async fn withdraw_retained_questions_with(
        &self,
        refusals: &mut RefusalWindows<(String, String)>,
    ) -> Result<bool, String> {
        let rows = self.index.pending_withdrawals()?;
        refusals.retain(|(lineage, call_key)| {
            rows.iter()
                .any(|row| &row.lineage_id == lineage && &row.call_key == call_key)
        });
        for question in rows {
            let key = (question.lineage_id.clone(), question.call_key.clone());
            let now = self.clock.now_ms();
            if now >= question.expires_at_ms {
                let settled = self
                    .index
                    .settle_withdrawal(&question.lineage_id, &question.call_key)?;
                if let Some(window) = refusals.settled(&key) {
                    tracing::info!(target: "retention", session = %question.session.session, call_key = %question.call_key, refusals = window.refusals, last_code = %window.last_code, "content-free withdrawal settled after refusals: the question expired");
                }
                if settled && question.scope.is_none() {
                    tracing::warn!(target: "retention", session = %question.session.session, call_key = %question.call_key, "scope_unknown withdrawal expired unserviced");
                } else if settled && matches!(question.question, RetentionQuestionId::Unknown) {
                    tracing::warn!(target: "retention", session = %question.session.session, call_key = %question.call_key, "id_unknown withdrawal expired unserviced");
                }
                continue;
            }
            if question.scope.is_none() {
                continue;
            }
            let RetentionQuestionId::Known(id) = &question.question else {
                continue;
            };
            let Some(adapter) = self.withdrawal_adapter.get() else {
                continue;
            };
            let exhausted = refusals.exhausted(&key, tokio::time::Instant::now());
            let exhausted = match exhausted {
                Some(window) => Some(window),
                None => {
                    let wait = std::time::Duration::from_millis(
                        question.expires_at_ms.saturating_sub(now),
                    )
                    .min(std::time::Duration::from_secs(5));
                    let reply = tokio::time::timeout(wait, adapter.withdraw(&question, id)).await;
                    match withdrawal_step(reply.ok()) {
                        WithdrawalStep::Settle(_, reason) => {
                            if let Some(code) = reason.as_deref() {
                                if refusals.final_refusal(&key, code) {
                                    tracing::warn!(target: "retention", session = %question.session.session, call_key = %question.call_key, %code, "content-free withdrawal refused; the refusal is final");
                                }
                            }
                            self.index
                                .settle_withdrawal(&question.lineage_id, &question.call_key)?;
                            if let Some(window) = refusals.settled(&key) {
                                tracing::info!(target: "retention", session = %question.session.session, call_key = %question.call_key, refusals = window.refusals, last_code = %window.last_code, reason = reason.as_deref().unwrap_or("withdrawn"), "content-free withdrawal settled after refusals");
                            }
                            continue;
                        }
                        WithdrawalStep::Retry(code) => {
                            match refusals.refused(&key, &code, tokio::time::Instant::now()) {
                                Refused::First => {
                                    tracing::warn!(target: "retention", session = %question.session.session, call_key = %question.call_key, %code, retry_window_s = WITHDRAWAL_RETRY_WINDOW.as_secs(), "content-free withdrawal refused; retrying quietly until it settles or the retry window ends");
                                    continue;
                                }
                                Refused::Retrying => continue,
                                Refused::Exhausted(window) => Some(window),
                            }
                        }
                    }
                }
            };
            if let Some(window) = exhausted {
                // Persist the stop reason without the question or its scope before deleting
                // the pending row. Logs are not guaranteed to survive restarts or rotation.
                self.index
                    .give_up_withdrawal(&question.lineage_id, &question.call_key)?;
                refusals.settled(&key);
                tracing::warn!(target: "retention", session = %question.session.session, call_key = %question.call_key, refusals = window.refusals, last_code = %window.last_code, reason = WITHDRAWAL_RETRY_EXHAUSTED, retry_window_s = WITHDRAWAL_RETRY_WINDOW.as_secs(), "content-free withdrawal given up; the question stays visible until it is answered or expires");
            }
        }
        Ok(self.index.pending_withdrawals()?.is_empty())
    }

    pub fn set_withdrawal_adapter(&self, adapter: Arc<dyn RetentionWithdrawalAdapter>) {
        let _ = self.withdrawal_adapter.set(adapter);
        self.withdrawal_wake.notify_one();
    }
}
