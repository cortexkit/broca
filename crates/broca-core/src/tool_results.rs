//! Tool-hook subjects and synthetic tool results shared by dispatch and recovery.

use broca_protocol::{StepId, ToolCall, ToolCallId, ToolOutput, ToolResult};
use broca_wal::{DispatchOutcome, NoDispatchReason};

/// The `route_drained` result for a call a scope drain stopped before it was sent. The text
/// leads with the name so the model, and anyone reading the transcript, sees why.
pub fn route_drained_result(id: &ToolCallId, reason: &str) -> ToolResult {
    ToolResult {
        tool_call_id: id.clone(),
        output: ToolOutput::error(format!(
            "route_drained: the tool was not run because its route was closed ({reason})"
        )),
        is_error: true,
    }
}

pub(crate) fn error_result(call: &ToolCall, message: &str) -> ToolResult {
    ToolResult {
        tool_call_id: call.tool_call_id.clone(),
        output: ToolOutput::error(message),
        is_error: true,
    }
}

/// The shared `NoDispatchReason → ToolResult` rebuild — used BOTH by the live loop
/// and on replay, so a reconstructed no-dispatch result is BYTE-IDENTICAL to what
/// the live run produced (one function is the single source of truth for the
/// result bytes, so the two paths can't drift).
pub fn no_dispatch_result(tool_call_id: &ToolCallId, reason: &NoDispatchReason) -> ToolResult {
    let output = match reason {
        NoDispatchReason::Denied { reason } => ToolOutput::Denied {
            reason: reason.clone(),
        },
        NoDispatchReason::ValidationFailed { detail }
        | NoDispatchReason::PolicyBlocked { detail } => ToolOutput::error(detail),
        // An attributed outage keeps the stable `message` and carries what was down in
        // sibling fields; a plain unavailable result has no attribution and renders (and
        // serializes) exactly as before.
        NoDispatchReason::Unavailable {
            detail,
            module,
            code,
            refusal_detail,
        } => match module {
            // Built by the one constructor that checks every member against the runner
            // contract (and omits one that does not fit), so a daemon value out of contract
            // never reaches a history page. The message stays the stable detail recorded.
            Some(module) => {
                let mut output = ToolOutput::module_unavailable(
                    module.as_str(),
                    code.clone().unwrap_or_default(),
                    refusal_detail.clone().unwrap_or_default(),
                );
                if let ToolOutput::Error { message, .. } = &mut output {
                    message.clone_from(detail);
                }
                output
            }
            None => ToolOutput::error(detail),
        },
    };
    ToolResult {
        tool_call_id: tool_call_id.clone(),
        output,
        is_error: true,
    }
}

/// The text of the result a resume writes for an indeterminate call. It says plainly that no
/// result was recorded, because that is all the runner knows: the call may have run.
pub const OUTCOME_UNKNOWN_TEXT: &str = "outcome_unknown: this tool call was sent, but no result \
     was recorded before the run stopped, so it may or may not have run. It will not be sent \
     again.";

/// The text of the result a resume writes for a planned call of the same batch that never left
/// the process. It definitely did not run.
pub const NOT_DISPATCHED_TEXT: &str =
    "tool not run: the run was interrupted before this call was dispatched";

/// The result a resume records for an indeterminate call: an error whose text says the call has
/// no recorded result. Written with outcome [`DispatchOutcome::Unknown`], which every reader
/// already treats as indeterminate, so the call stays marked as possibly run.
pub fn outcome_unknown_result(tool_call_id: &ToolCallId) -> ToolResult {
    ToolResult {
        tool_call_id: tool_call_id.clone(),
        output: ToolOutput::error(OUTCOME_UNKNOWN_TEXT),
        is_error: true,
    }
}

/// The result for a planned call that never left the process because the run stopped first. A
/// resume writes it for such a call of an open batch, and a run asked to stop writes it for each
/// call it no longer sends. Recorded with outcome [`DispatchOutcome::Interrupted`]: the call
/// definitely did not run and is never sent later.
pub fn not_dispatched_result(tool_call_id: &ToolCallId) -> ToolResult {
    ToolResult {
        tool_call_id: tool_call_id.clone(),
        output: ToolOutput::error(NOT_DISPATCHED_TEXT),
        is_error: true,
    }
}

/// A provider-held call has a known owner, so crash close-out must not claim an unknown outcome.
pub fn provider_held_interrupted_result(tool_call_id: &ToolCallId) -> ToolResult {
    provider_held_close_result(tool_call_id, broca_protocol::RunFinishReason::Interrupted).0
}

/// Close a provider's known obligation without pretending that its outcome is unknown.
pub fn provider_held_close_result(
    tool_call_id: &ToolCallId,
    reason: broca_protocol::RunFinishReason,
) -> (ToolResult, DispatchOutcome, &'static str) {
    let (code, text, outcome) = match reason {
        broca_protocol::RunFinishReason::Cancelled => (
            "provider_held_cancelled",
            "cancelled",
            DispatchOutcome::Interrupted,
        ),
        _ => (
            "provider_held_interrupted",
            "interrupted",
            DispatchOutcome::Interrupted,
        ),
    };
    (
        ToolResult {
            tool_call_id: tool_call_id.clone(),
            output: ToolOutput::error(format!(
                "{code}: the run was {text} while the provider held the call"
            )),
            is_error: true,
        },
        outcome,
        code,
    )
}

/// The subject a pre-tool hook sees for `call` in `phase`.
pub fn pre_tool_subject(
    step_id: StepId,
    call: &ToolCall,
    phase: cortexkit_role_step_transform_provider::subscription::Phase,
) -> crate::hooks::Subject {
    crate::hooks::Subject::PreTool {
        step_id: step_id.0.to_string(),
        phase,
        tool: call.tool_name.clone(),
        tool_call_id: call.tool_call_id.0.clone(),
        call_key: None,
        input: call.input.clone().parse(),
    }
}

/// The result a call gets when a pre-tool hook refuses it, and the outcome's reason code. A
/// provider that denied the call gives the model its own text; one that was unavailable gives
/// a fixed message naming it.
pub fn pre_tool_refusal(
    call: &ToolCall,
    provider: &str,
    text: &str,
    unavailable: bool,
) -> (ToolResult, &'static str) {
    let reason = if unavailable {
        "pre_tool_unavailable"
    } else {
        "pre_tool_denied"
    };
    let message = if unavailable {
        format!("{reason}: {provider}; the tool was not executed")
    } else {
        text.into()
    };
    (error_result(call, &message), reason)
}

/// A tool result after its post-tool hooks: the edited text in the original output's form, or
/// a withheld result when a hook the plan requires was unavailable. Also returns the outcome's
/// reason code and the hook trace.
pub fn transformed_tool_result(
    original: &ToolResult,
    output: &crate::hooks::ChainOutput,
) -> (ToolResult, Option<String>, Option<Box<broca_wal::Hooks>>) {
    let mut result = original.clone();
    let mut reason = None;
    if let Some((provider, _, _)) = &output.refusal {
        reason = Some("post_tool_unavailable".into());
        result.output = ToolOutput::error(format!(
            "post_tool_unavailable: {provider}; the call executed, but its result is withheld"
        ));
        result.is_error = true;
    } else if let crate::hooks::Subject::PostTool { blocks, .. } = &output.subject {
        if !output.operations.is_empty() {
            let text = blocks[0].clone();
            result.output = match &original.output {
                ToolOutput::Error { .. } => ToolOutput::error(text),
                ToolOutput::Denied { .. } if text.starts_with("denied: ") => ToolOutput::Denied {
                    reason: text[8..].into(),
                },
                _ => ToolOutput::Text { text },
            };
        }
    }
    let metadata = output.trace.metadata(
        serde_json::to_value(original).unwrap(),
        serde_json::to_value(&result).unwrap(),
        false,
    );
    (result, reason, metadata)
}

/// The text a post-tool hook sees for a result: the model's text for it, with media as markers
/// (provenance permits only captured references). Never called for a JSON result, which stays
/// hookless.
pub fn post_tool_text(result: &ToolResult) -> String {
    match &result.output {
        ToolOutput::Parts { parts } => broca_protocol::content::media_parts_text(
            parts,
            broca_protocol::MediaMarkerReason::Carrier,
        ),
        ToolOutput::Text { text } => text.clone(),
        ToolOutput::Error {
            message,
            module,
            code,
            detail,
            nothing_sent,
            retry,
            ..
        } => ToolOutput::error_model_text(
            message,
            module.as_deref(),
            code.as_deref(),
            detail.as_deref(),
            *nothing_sent,
            retry.as_deref(),
        ),
        ToolOutput::Denied { reason } => format!("denied: {reason}"),
        ToolOutput::Json { .. } => unreachable!("a JSON result has no post-tool hooks"),
    }
}

#[cfg(test)]
mod unavailable_result_tests {
    use super::no_dispatch_result;
    use broca_protocol::ToolCallId;
    use broca_protocol::ToolOutput;
    use broca_wal::NoDispatchReason;

    /// A call refused because its module stayed down keeps the exact matchable message
    /// and carries the outage in sibling fields, so the model sees what was down and
    /// that nothing was sent.
    #[test]
    fn attributed_outage_pins_message_and_carries_fields() {
        let id = ToolCallId("call".into());
        let result = no_dispatch_result(
            &id,
            &NoDispatchReason::Unavailable {
                detail: broca_protocol::TOOL_PLANE_UNAVAILABLE_MESSAGE.into(),
                module: Some("aft".into()),
                code: Some("target_unavailable".into()),
                refusal_detail: Some("supervised_not_registered".into()),
            },
        );
        assert!(result.is_error);
        let ToolOutput::Error {
            message,
            module,
            code,
            detail,
            nothing_sent,
            retry,
        } = &result.output
        else {
            panic!("an unavailable no-dispatch is an error result");
        };
        assert_eq!(message, "tool plane unavailable");
        assert_eq!(module.as_deref(), Some("aft"));
        assert_eq!(code.as_deref(), Some("target_unavailable"));
        assert_eq!(detail.as_deref(), Some("supervised_not_registered"));
        assert_eq!(*nothing_sent, Some(true));
        assert!(retry.as_deref().is_some_and(|retry| !retry.is_empty()));
        // The serialized shape keeps the matchable message beside the new fields.
        let json = serde_json::to_value(&result.output).unwrap();
        assert_eq!(json["message"], "tool plane unavailable");
        assert_eq!(json["module"], "aft");
        assert_eq!(json["code"], "target_unavailable");
        assert_eq!(json["nothing_sent"], true);
    }

    /// A plain unavailable result serializes exactly as before: no new keys.
    #[test]
    fn plain_unavailable_serializes_without_new_keys() {
        let result = no_dispatch_result(
            &ToolCallId("call".into()),
            &NoDispatchReason::Unavailable {
                detail: "tool plane unavailable".into(),
                module: None,
                code: None,
                refusal_detail: None,
            },
        );
        assert_eq!(
            serde_json::to_value(&result.output).unwrap(),
            serde_json::json!({"kind": "error", "message": "tool plane unavailable"})
        );
    }
}
