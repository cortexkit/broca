//! Retained questions are serviced from identity-only rows, independently of session actors.
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
        loop {
            let empty = match self.withdraw_retained_questions().await {
                Ok(empty) => empty,
                Err(error) => {
                    tracing::warn!(target: "retention", "content-free withdrawal retry: {error}");
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

    /// A pass makes no WAL or actor access. Failed storage and route operations remain pending.
    pub(crate) async fn withdraw_retained_questions(&self) -> Result<bool, String> {
        let rows = self.index.pending_withdrawals()?;
        for question in rows {
            let now = self.clock.now_ms();
            if now >= question.expires_at_ms {
                let settled = self
                    .index
                    .settle_withdrawal(&question.lineage_id, &question.call_key)?;
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
            let wait = std::time::Duration::from_millis(question.expires_at_ms.saturating_sub(now))
                .min(std::time::Duration::from_secs(5));
            let answer = tokio::time::timeout(wait, adapter.withdraw(&question, id)).await;
            let terminal = match answer {
                Ok(ApprovalWithdrawal::Acknowledged) => true,
                Ok(ApprovalWithdrawal::NotSent { reason }) => {
                    crate::approval::terminal_refusal(&reason)
                }
                Ok(ApprovalWithdrawal::Retry(error)) => {
                    crate::approval::terminal_refusal(&error.code)
                }
                Err(_) => false,
            };
            if terminal {
                self.index
                    .settle_withdrawal(&question.lineage_id, &question.call_key)?;
            }
        }
        Ok(self.index.pending_withdrawals()?.is_empty())
    }

    pub fn set_withdrawal_adapter(&self, adapter: Arc<dyn RetentionWithdrawalAdapter>) {
        let _ = self.withdrawal_adapter.set(adapter);
        self.withdrawal_wake.notify_one();
    }
}
