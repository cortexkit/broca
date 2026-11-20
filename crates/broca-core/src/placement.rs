//! Message placement over durable records, independent of message content and storage.

use broca_protocol::{caller::RunFinishReason, request::BudgetConfig, BatchId, RunId, ToolCallId};
use broca_wal::{RunOrigin, SeedOriginKind, WalRecord};

use crate::{budget_kernel, BudgetReplayStateV1};

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
    pub fn held_tool_result_ordinal(&self) -> Option<u64> {
        self.assistant_held.then_some(self.next.saturating_add(1))
    }

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

    pub fn next_ordinal(&self) -> u64 {
        self.next
    }

    pub fn held_assistant_ordinal(&self) -> Option<u64> {
        self.assistant_held.then_some(self.next)
    }

    pub fn budget(&self) -> &BudgetReplayStateV1 {
        &self.budget
    }

    pub fn completed_steps(&self) -> u64 {
        self.completed_steps
    }

    fn append(&mut self, count: u64, kind: AppendKind) -> PlacementAction {
        let first = self.next;
        self.next = self.next.saturating_add(count);
        PlacementAction::Append { first, count, kind }
    }

    fn place_steers(&mut self, actions: &mut Vec<PlacementAction>) {
        if self.steers != 0 {
            let first = self.next;
            let count = std::mem::take(&mut self.steers);
            self.next = self.next.saturating_add(count);
            actions.push(PlacementAction::PlaceHeld { first, count });
        }
    }

    fn terminal(&mut self, id: &ToolCallId) {
        if let Some(batch) = &mut self.batch {
            if let Some(position) =
                batch
                    .planned
                    .iter()
                    .enumerate()
                    .find_map(|(position, planned)| {
                        (planned == id && !batch.terminals[position]).then_some(position)
                    })
            {
                batch.terminals[position] = true;
            }
        }
    }

    /// Apply in WAL order. Budget validation uses the same kernel as admission and execution.
    /// Without a frozen budget, read-only transcript assembly skips budget records.
    pub fn observe(&mut self, record: &WalRecord) -> Result<Vec<PlacementAction>, String> {
        let mut actions = Vec::new();
        match record {
            WalRecord::RunStarted {
                run_id,
                config,
                input,
                origin,
                ..
            } => {
                self.steers = 0;
                self.assistant_held = false;
                self.batch = None;
                self.completed_steps = 0;
                self.run_id = Some(run_id.clone());
                self.budget_config = config.budget.clone();
                self.import_start = matches!(
                    origin,
                    RunOrigin::SeededFrom {
                        origin_kind: SeedOriginKind::Import,
                        ..
                    }
                )
                .then_some(self.next);
                budget_kernel::apply(
                    budget_kernel::State {
                        facts: &mut self.budget,
                        config: self.budget_config.as_ref(),
                        run_id: self.run_id.as_ref(),
                    },
                    record,
                )?;
                actions.push(self.append(input.len() as u64, AppendKind::Input));
            }
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } => {
                self.completed_steps += 1;
                let finalize = self.budget.finalize_pending;
                budget_kernel::apply(
                    budget_kernel::State {
                        facts: &mut self.budget,
                        config: self.budget_config.as_ref(),
                        run_id: self.run_id.as_ref(),
                    },
                    record,
                )?;
                if finalize {
                    self.assistant_held = false;
                    self.batch = None;
                    actions.push(self.append(1, AppendKind::Finalize));
                } else if assistant_message.tool_calls().next().is_some() {
                    self.assistant_held = true;
                    self.batch = None;
                    actions.push(PlacementAction::Hold {
                        count: 1,
                        kind: HoldKind::Assistant,
                    });
                } else {
                    actions.push(self.append(1, AppendKind::Assistant));
                }
            }
            WalRecord::ToolBatchStarted {
                batch_id,
                planned_calls,
                ..
            } => {
                self.batch = Some(Batch {
                    id: Some(batch_id.clone()),
                    planned: planned_calls.clone(),
                    prepared: vec![false; planned_calls.len()],
                    terminals: vec![false; planned_calls.len()],
                });
            }
            WalRecord::ToolCallDeferred {
                batch_id, call_id, ..
            } => {
                if let Some(batch) = &mut self.batch {
                    if batch.id.as_ref() == Some(batch_id) {
                        if let Some(position) = batch.planned.iter().position(|id| id == call_id) {
                            batch.prepared[position] = true;
                        }
                    }
                }
            }
            WalRecord::ApprovalRecorded {
                call_id,
                closing_result: Some(_),
                ..
            } => {
                if self.batch.as_ref().is_some_and(|b| {
                    b.planned
                        .iter()
                        .position(|id| id == call_id)
                        .is_some_and(|position| b.prepared[position])
                }) {
                    self.terminal(call_id);
                }
            }
            WalRecord::ToolNoDispatch { tool_call_id, .. } => self.terminal(tool_call_id),
            WalRecord::ToolResult { result, .. } => self.terminal(&result.tool_call_id),
            WalRecord::ToolBatchFinished { .. } => {
                if self
                    .batch
                    .as_ref()
                    .is_some_and(|b| b.terminals.iter().all(|terminal| *terminal))
                {
                    let count = if std::mem::take(&mut self.assistant_held) {
                        2
                    } else {
                        0
                    };
                    self.batch = None;
                    actions.push(self.append(count, AppendKind::ToolTurn));
                }
            }
            WalRecord::Steered { input, .. }
            | WalRecord::SendDelivered {
                input: Some(input), ..
            } => {
                let count = input.len() as u64;
                self.steers = self.steers.saturating_add(count);
                actions.push(PlacementAction::Hold {
                    count,
                    kind: HoldKind::Steer,
                });
            }
            WalRecord::StepStarted { .. } => self.place_steers(&mut actions),
            WalRecord::RunFinished { reason, .. } => {
                if self.completed_steps == 0 && *reason == RunFinishReason::Interrupted {
                    if let Some(to) = self.import_start {
                        self.next = to;
                        actions.push(PlacementAction::Truncate { to });
                    }
                }
                self.place_steers(&mut actions);
            }
            WalRecord::BudgetFired { ordinal, .. } if self.budget_config.is_some() => {
                if *ordinal != self.next {
                    return Err(format!(
                        "budget message ordinal {ordinal} does not match prompt position {}",
                        self.next
                    ));
                }
                budget_kernel::apply(
                    budget_kernel::State {
                        facts: &mut self.budget,
                        config: self.budget_config.as_ref(),
                        run_id: self.run_id.as_ref(),
                    },
                    record,
                )?;
                actions.push(self.append(1, AppendKind::Budget));
            }
            _ => {}
        }
        Ok(actions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use broca_protocol::{content::Message, ContentBlock, Role, StepId};

    fn user() -> Message {
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "steer".into(),
            }],
        )
    }

    #[test]
    fn held_steers_wait_for_step_or_terminal() {
        let mut placement = Placement::new(7);
        let steer = WalRecord::Steered {
            message_marks: None,
            sent_by: None,
            written_at_ms: None,
            input: vec![user()].into(),
            tags: None,
            retention: None,
            ts_ms: None,
            title: None,
            send_id: None,
            request_identity_digest: None,
            delivery: None,
            hooks: None,
        };
        assert_eq!(
            placement.observe(&steer).unwrap(),
            vec![PlacementAction::Hold {
                count: 1,
                kind: HoldKind::Steer
            }]
        );
        assert_eq!(placement.next_ordinal(), 7);
        assert_eq!(
            placement
                .observe(&WalRecord::StepStarted { step_id: StepId(1) })
                .unwrap(),
            vec![PlacementAction::PlaceHeld { first: 7, count: 1 }]
        );
        assert!(placement
            .observe(&WalRecord::StepStarted { step_id: StepId(2) })
            .unwrap()
            .is_empty());
    }
}
