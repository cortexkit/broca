//! What one dispatched call resolved to, before its batch takes it.

use super::*;

/// What a call resolved to, before the batch takes it: the result and outcome, the code the
/// outcome records, and what its post-tool hooks did.
pub(super) struct Resolved {
    pub(super) result: ToolResult,
    pub(super) outcome: CallOutcome,
    pub(super) reason: Option<String>,
    /// The refusal detail a received call's outcome records with its `reason` (see
    /// [`DispatchResult::Refused`]).
    pub(super) detail: Option<Value>,
    pub(super) hooks: Option<Value>,
    /// The provider of a call handed back for the person's steer, whose later terminal for it
    /// becomes a notice (see [`DispatchResult::Yielded`]).
    pub(super) yielded_by: Option<String>,
}

impl From<(ToolResult, CallOutcome)> for Resolved {
    fn from((result, outcome): (ToolResult, CallOutcome)) -> Self {
        Self {
            result,
            outcome,
            reason: None,
            detail: None,
            hooks: None,
            yielded_by: None,
        }
    }
}

/// A call's result and outcome from what the tool plane said:
/// - a result is the tool's own answer;
/// - never sent is safe: the model gets an error result and may try again;
/// - sent with no result may have had its effect: the model gets an error result, the outcome
///   is unknown, and the run stops.
pub(super) fn resolve(call: &ToolCall, result: DispatchResult) -> (ToolResult, CallOutcome) {
    let error = |message: String| ToolResult {
        tool_call_id: call.tool_call_id.clone(),
        output: ToolOutput::error(message),
        is_error: true,
    };
    match result {
        DispatchResult::Completed(result)
        | DispatchResult::Yielded { result, .. }
        | DispatchResult::Refused { result, .. } => (result, CallOutcome::Completed),
        DispatchResult::NotSent { reason } => (
            error(format!("tool not dispatched: {reason}")),
            CallOutcome::Completed,
        ),
        DispatchResult::SentButOutcomeUnknown { reason } => (
            error(format!(
                "tool outcome unknown (interrupted for review): {reason}"
            )),
            CallOutcome::Unknown,
        ),
        // Never sent, so the refusal is definite: the model sees it and the run goes on.
        DispatchResult::ScopeDrained {
            reason,
            sent: false,
        } => (
            broca_core::tool_results::route_drained_result(&call.tool_call_id, &reason),
            CallOutcome::Completed,
        ),
        // Sent with no result: the side effect may have applied, as with any lost reply.
        DispatchResult::ScopeDrained { reason, sent: true } => (
            error(format!(
                "tool outcome unknown (interrupted for review): its route was drained \
                 ({reason}) after the call was sent"
            )),
            CallOutcome::Unknown,
        ),
    }
}
