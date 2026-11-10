//! Stop-condition evaluation shared by run loops.

use broca_protocol::{AssistantMessage, StopCondition};

/// The first satisfied stop condition, or None.
pub fn stop_satisfied<'a>(
    conds: &'a [StopCondition],
    completed_steps: u64,
    last: &AssistantMessage,
) -> Option<&'a StopCondition> {
    conds.iter().find(|c| match c {
        StopCondition::StepCount { n } => completed_steps >= *n,
        StopCondition::HasToolCall { name } => last
            .tool_calls()
            .any(|tc| name.as_ref().is_none_or(|n| &tc.tool_name == n)),
        StopCondition::LoopFinished => last.tool_calls().next().is_none(),
    })
}

/// Whether any satisfied stop other than LoopFinished forbids adding a model step
/// to process a queued steer. Check every condition, even after a matching LoopFinished.
pub fn limiting_stop_satisfied(
    conds: &[StopCondition],
    completed_steps: u64,
    last: &AssistantMessage,
) -> bool {
    conds.iter().any(|condition| {
        !matches!(condition, StopCondition::LoopFinished)
            && stop_satisfied(std::slice::from_ref(condition), completed_steps, last).is_some()
    })
}
