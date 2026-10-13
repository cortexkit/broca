//! Deterministic budget transitions over durable events, shared by admission, replay and live execution.

use broca_protocol::{
    request::{BudgetConfig, BudgetKind},
    RunId,
};
use broca_wal::WalRecord;

use serde::{Deserialize, Serialize};

/// Version of the durable budget state carried by an admission checkpoint.
pub const BUDGET_REPLAY_STATE_VERSION: u32 = 1;

/// Per-episode budget facts required to resume from a materialized WAL watermark.
///
/// `None` on [`crate::LastEpisodeV1`] means a checkpoint predates this state or encountered a
/// budget record that cannot be attributed unambiguously. Callers must replay the complete WAL
/// in either case rather than infer a firing history.
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

    /// Validate durable state before using it to avoid changing a live budget decision.
    pub fn is_valid_for(&self, budget: &BudgetConfig) -> bool {
        self.version == BUDGET_REPLAY_STATE_VERSION
            && self
                .fired_nudge_indices
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            && self
                .fired_nudge_indices
                .iter()
                .all(|index| *index < budget.nudges.len())
            && self
                .budget_last_threshold
                .is_none_or(|last| self.tool_call_count >= last)
            && (!self.finalize_pending || self.finalize_fired)
    }
}

impl Default for BudgetReplayStateV1 {
    fn default() -> Self {
        Self::fresh()
    }
}

/// Borrowed episode context and mutable budget facts. A firing records a threshold, not a
/// nudge index, so applying it requires the frozen config and the episode identity.
/// These references do not resolve config or access the WAL.
pub(crate) struct State<'a> {
    pub facts: &'a mut BudgetReplayStateV1,
    pub config: Option<&'a BudgetConfig>,
    pub run_id: Option<&'a RunId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Due {
    pub threshold: u64,
    pub kind: BudgetKind,
    pub index: Option<usize>,
    pub text: String,
}

/// Return all outstanding firings in threshold order, with nudges before a tied finalize.
/// Deciding does not spend an allowance: only applying its durable firing does.
pub(crate) fn decide(state: &BudgetReplayStateV1, config: &BudgetConfig) -> Vec<Due> {
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

/// Fold one durable event. Invalid firings leave the facts unchanged; callers choose
/// whether to discard an untrustworthy checkpoint or reject a corrupt replay.
pub(crate) fn apply(state: State<'_>, record: &WalRecord) -> Result<(), String> {
    let facts = state.facts;
    match record {
        WalRecord::RunStarted { .. } => *facts = BudgetReplayStateV1::fresh(),
        WalRecord::ModelStepFinished {
            assistant_message, ..
        } => {
            facts.tool_call_count += assistant_message.tool_calls().count() as u64;
            // The finalize allowance is spent once its model response is durable.
            facts.finalize_pending = false;
        }
        WalRecord::BudgetFired {
            run_id,
            kind,
            threshold,
            ..
        } => {
            let config = state
                .config
                .ok_or("BudgetFired has no frozen budget config")?;
            if state.run_id != Some(run_id) {
                return Err("BudgetFired run_id does not match the current episode".into());
            }
            if facts.tool_call_count < *threshold {
                return Err(format!(
                    "BudgetFired threshold {threshold} is early at tool-call count {}",
                    facts.tool_call_count
                ));
            }
            if facts
                .budget_last_threshold
                .is_some_and(|last| *threshold < last)
            {
                return Err(format!(
                    "BudgetFired threshold {threshold} is reordered after threshold {}",
                    facts.budget_last_threshold.unwrap_or_default()
                ));
            }
            let expected = decide(facts, config)
                .into_iter()
                .next()
                .ok_or("BudgetFired is duplicate or no firing is due")?;
            if expected.threshold != *threshold || expected.kind != *kind {
                return Err(format!(
                    "BudgetFired is out of expected order: expected {:?} at threshold {}",
                    expected.kind, expected.threshold
                ));
            }
            match expected.index {
                Some(index) => {
                    facts.fired_nudge_indices.push(index);
                    facts.fired_nudge_indices.sort_unstable();
                }
                None => {
                    facts.finalize_fired = true;
                    facts.finalize_pending = true;
                }
            }
            facts.budget_last_threshold = Some(*threshold);
        }
        _ => {}
    }
    Ok(())
}
