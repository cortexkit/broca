//! Session-wide input accounting over durable model-step records, independent of episode budgets.

use broca_protocol::{BudgetExhaustedDetail, Usage};
use broca_wal::WalRecord;
use cortexkit_role_llm_runner::{
    run::budget_exhausted_causes::{LIMIT_REACHED, USAGE_UNREPORTED},
    send::Budget,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputTokenBudget {
    pub limit: Option<u64>,
    pub spent: u64,
    pub usage_unreported: bool,
}

impl InputTokenBudget {
    pub fn admit(&self, requested: Option<u64>) -> Result<Option<u64>, String> {
        let mut budget = Budget::new();
        budget.max_input_tokens = requested;
        budget.check(true, self.limit)?;
        Ok(requested.or(self.limit))
    }

    pub fn observe(&mut self, usage: Usage) {
        // For usage_unreported, spent is the measured sum BEFORE the unknown step.
        // It is not a zero estimate of that step or a claim about later consumption.
        if self.usage_unreported {
            return;
        }
        match usage.fill_tokens() {
            Some(input) => self.spent = self.spent.saturating_add(input),
            None => self.usage_unreported = true,
        }
    }

    pub fn exhausted(&self) -> Option<BudgetExhaustedDetail> {
        let limit = self.limit?;
        let cause = if self.usage_unreported {
            USAGE_UNREPORTED
        } else if self.spent >= limit {
            LIMIT_REACHED
        } else {
            return None;
        };
        Some(BudgetExhaustedDetail::new(limit, self.spent, cause))
    }

    pub fn fold(&mut self, record: &WalRecord) -> Result<(), String> {
        match record {
            WalRecord::RunStarted { config, .. } => {
                if let Some(limit) = config
                    .budget
                    .as_ref()
                    .and_then(|budget| budget.max_input_tokens)
                {
                    if limit == 0 {
                        return Err("budget.max_input_tokens must be at least 1".into());
                    }
                    // A later send can lower the input cap while earlier accepted sends wait.
                    // Starting an earlier send must retain the tighter limit, not restore its old cap.
                    self.limit = Some(self.limit.map_or(limit, |frozen| frozen.min(limit)));
                }
            }
            WalRecord::InputTokenCapFrozen { limit } => self.limit = self.admit(Some(*limit))?,
            WalRecord::Queued { request, .. } => {
                self.limit = self.admit(
                    request
                        .budget
                        .as_ref()
                        .and_then(|budget| budget.max_input_tokens),
                )?;
            }
            WalRecord::ModelStepFinished { usage, .. } => self.observe(*usage),
            _ => {}
        }
        Ok(())
    }
}
