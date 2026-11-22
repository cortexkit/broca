//! Deterministic budget transitions over durable events, shared by admission, replay and live execution.

use broca_protocol::request::{BudgetConfig, BudgetKind};

use serde::{Deserialize, Serialize};

/// Version of the budget state's shape.
pub const BUDGET_REPLAY_STATE_VERSION: u32 = 1;

/// A run's tool-call budget so far, so each reminder fires once: how many tool calls the run
/// made, which of `BudgetConfig::nudges` (reminder texts sent into the run at a tool-call count)
/// it has sent, and whether the finalize reminder (`finalize_text`, sent at
/// `finalize_at_tool_calls` to tell the model to finish) has fired.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetReplayStateV1 {
    pub version: u32,
    pub tool_call_count: u64,
    /// Canonically sorted indexes into the frozen `BudgetConfig::nudges` list.
    pub fired_nudge_indices: Vec<usize>,
    pub finalize_fired: bool,
    pub finalize_pending: bool,
    /// The last accepted `BudgetFired.threshold`; retained to reject reordered firings.
    pub budget_last_threshold: Option<u64>,
}

impl BudgetReplayStateV1 {
    pub(crate) fn fresh() -> Self {
        Self {
            version: BUDGET_REPLAY_STATE_VERSION,
            tool_call_count: 0,
            fired_nudge_indices: Vec::new(),
            finalize_fired: false,
            finalize_pending: false,
            budget_last_threshold: None,
        }
    }
}

impl Default for BudgetReplayStateV1 {
    fn default() -> Self {
        Self::fresh()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A firing the budget has due: its threshold, kind, nudge index and text.
pub struct Due {
    pub threshold: u64,
    pub kind: BudgetKind,
    pub index: Option<usize>,
    pub text: String,
}

/// The next firing due: which nudge it is (`None` for the finalize firing) and the message it
/// places. Older budget firing records stored neither, so replay derives them, and so does any
/// tool that rebuilds a session from those records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueFiring {
    pub nudge_index: Option<usize>,
    pub text: String,
}

/// Return all outstanding firings in threshold order, with nudges before a tied finalize.
/// Deciding does not spend an allowance: only applying its durable firing does.
pub fn decide(state: &BudgetReplayStateV1, config: &BudgetConfig) -> Vec<Due> {
    let mut due: Vec<_> = config
        .nudges
        .iter()
        .enumerate()
        .filter(|(index, nudge)| {
            state.tool_call_count >= nudge.at_tool_calls
                && !state.fired_nudge_indices.contains(index)
        })
        .map(|(index, nudge)| Due {
            threshold: nudge.at_tool_calls,
            kind: BudgetKind::Nudge,
            index: Some(index),
            text: nudge.text.clone(),
        })
        .collect();
    if state.tool_call_count >= config.finalize_at_tool_calls && !state.finalize_fired {
        due.push(Due {
            threshold: config.finalize_at_tool_calls,
            kind: BudgetKind::Finalize,
            index: None,
            text: config.finalize_text.clone(),
        });
    }
    due.sort_by_key(|firing| {
        (
            firing.threshold,
            matches!(firing.kind, BudgetKind::Finalize),
        )
    });
    due
}
