//! A run's tool-call budget: the reminders its caller asked for once the model has made so many
//! tool calls, and the finalize reminder after which the run takes one last step with tools
//! disabled. Today's budget kernel decides which firings are due; this module turns them into
//! the records a batch's barrier carries, as today's loop writes them.

use broca_core::budget_kernel::{self, BudgetReplayStateV1};
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::request::{BudgetConfig, BudgetKind};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::record::{BudgetFire, BudgetFiring, Input, Placement, Record};

/// The budget the run froze at its start, if it has one.
pub fn frozen(operations: &Operations) -> Option<BudgetConfig> {
    operations
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
        .and_then(|start| start.run_choices.as_ref())
        .and_then(|choices| choices.get("config"))
        .and_then(|config| config.get("budget"))
        .and_then(|budget| serde_json::from_value(budget.clone()).ok())
}

/// The budget firings due now, as the records that make them durable: each firing, then the
/// reminder message it places in the conversation, naming the firing as the record that
/// accepted it.
///
/// `operations` and `conversation` are the folds before the batch the records join;
/// `first_seq` is the seq of that batch's first record and `before` how many of its records
/// come before these. `placing` is how many messages the batch's own records place before
/// these: a batch that closes a tool turn places the assistant message and its results.
/// Each reminder takes the next conversation position, counted from 0 as today's records count
/// it.
pub fn due(
    conversation: &Conversation,
    operations: &Operations,
    first_seq: u64,
    before: usize,
    placing: u64,
) -> Vec<Record> {
    let (Some(config), Some(run)) = (frozen(operations), operations.run.as_ref()) else {
        return Vec::new();
    };
    let mut fired: Vec<usize> = run
        .budget
        .nudges_fired
        .iter()
        .map(|index| *index as usize)
        .collect();
    fired.sort_unstable();
    let state = BudgetReplayStateV1 {
        version: budget_kernel::BUDGET_REPLAY_STATE_VERSION,
        tool_call_count: run.tool_call_count,
        fired_nudge_indices: fired,
        finalize_fired: run.budget.finalize_fired,
        finalize_pending: run.budget.finalize_pending,
        budget_last_threshold: None,
    };
    let base = conversation.placed + placing;
    let mut records = Vec::new();
    for (offset, firing) in budget_kernel::decide(&state, &config)
        .into_iter()
        .enumerate()
    {
        let fire_seq = first_seq + (before + records.len()) as u64;
        records.push(Record::BudgetFire(BudgetFire {
            run_id: run.run_id.clone(),
            firing: match firing.kind {
                BudgetKind::Nudge => BudgetFiring::Nudge,
                BudgetKind::Finalize => BudgetFiring::Finalize,
            },
            threshold: firing.threshold,
            ordinal: base + offset as u64,
            caller_text: Some(firing.text.clone()),
            tags_v1: None,
            nudge_index: firing.index.map(|index| index as u64),
        }));
        records.push(Record::Input(Input {
            run_id: run.run_id.clone(),
            placement: Placement::BudgetNudge,
            message: Message::new(Role::User, vec![ContentBlock::Text { text: firing.text }]),
            written_at_ms: None,
            submission_id: None,
            notice: None,
            tags_v1: None,
            receipt_seq: Some(fire_seq),
            pruned: Vec::new(),
        }));
    }
    records
}

/// Whether the run's next step is its finalize step: a finalize firing asked for one last step
/// with tools disabled, and it hasn't happened yet.
pub fn finalizing(operations: &Operations) -> bool {
    operations
        .run
        .as_ref()
        .is_some_and(|run| run.budget.finalize_pending)
}
