//! Message placement over durable records, independent of message content and storage.

use broca_protocol::{request::BudgetConfig, BatchId, RunId, ToolCallId};

use crate::BudgetReplayStateV1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendKind {
    Input,
    Assistant,
    Finalize,
    ToolTurn,
    Budget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldKind {
    Assistant,
    Steer,
}

/// One record may truncate an abandoned import and then place its held steers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementAction {
    Append {
        first: u64,
        count: u64,
        kind: AppendKind,
    },
    Hold {
        count: u64,
        kind: HoldKind,
    },
    PlaceHeld {
        first: u64,
        count: u64,
    },
    Truncate {
        to: u64,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Batch {
    id: Option<BatchId>,
    planned: Vec<ToolCallId>,
    prepared: Vec<bool>,
    terminals: Vec<bool>,
}

/// The next ordinal is absolute, including a materialized prefix not held in memory.
/// A projection watermark never splits a held steer or an open tool batch.
/// Changes to persisted Placement or Batch fields require bumping
/// `projection_plan::OFFSET_SEED_VERSION`; incompatible index seeds are rebuilt.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Placement {
    next: u64,
    steers: u64,
    assistant_held: bool,
    batch: Option<Batch>,
    run_id: Option<RunId>,
    #[serde(skip)]
    budget_config: Option<BudgetConfig>,
    budget: BudgetReplayStateV1,
    import_start: Option<u64>,
    completed_steps: u64,
}

impl Default for Placement {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Placement {
    /// Rehydrate an index seed with the episode's independently verified frozen
    /// config. Budget message text is deliberately absent from serialized seeds.
    pub fn from_index_seed(mut seed: Self, budget_config: Option<BudgetConfig>) -> Self {
        seed.budget_config = budget_config;
        seed
    }

    pub fn new(next: u64) -> Self {
        Self::seeded(next, None, None, BudgetReplayStateV1::default(), 0)
    }

    pub fn seeded(
        next: u64,
        run_id: Option<RunId>,
        budget_config: Option<BudgetConfig>,
        budget: BudgetReplayStateV1,
        completed_steps: u64,
    ) -> Self {
        Self {
            next,
            steers: 0,
            assistant_held: false,
            batch: None,
            run_id,
            budget_config,
            budget,
            import_start: None,
            completed_steps,
        }
    }
}
