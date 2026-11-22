//! Session-wide input accounting over durable model-step records, independent of episode budgets.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputTokenBudget {
    pub limit: Option<u64>,
    pub spent: u64,
    pub usage_unreported: bool,
}
