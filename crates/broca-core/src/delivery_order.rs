//! Sends whose `delivery` is an ordered list of modes (the `delivery_order` group).
//!
//! Such a send is durably queued (`DeliveryOrdered` then `Queued`, one barrier) and waits for a
//! request boundary where one of its modes is possible. At each boundary the runner takes the
//! FIRST mode in the send's list that is possible there:
//!
//! - `tool_result`: when a tool batch of the running turn has just finished with a text or error
//!   result to carry the send's text, and the send's input is text only. Decided as the batch
//!   commits, so the delivery record sits before the turn's `TurnFinished`.
//! - `turn_system`: never possible in this build ([`TURN_SYSTEM_TEMPLATE_UNIMPLEMENTED`]); the
//!   list falls through to its next mode.
//! - `steer`: at every step boundary of a running turn.
//! - `idle_start`: when the session is idle; the actor drains the send into a new run.
//!
//! A send whose list has no possible mode at a boundary stays queued for the next one, across
//! runs. A list without `idle_start` therefore never starts a run.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolOutput, ToolResult},
    BindPrincipal, DeliveryMode, DeliveryOrder, RunId, SubmissionId, ToolCallId,
};
use broca_wal::{record::SendEndReason, WalRecord};
use std::sync::Arc;

/// Why `turn_system` is never possible in this build, on every provider lane: a send cannot yet
/// name a plan-declared template, so the runner has no template bytes and typed slots to validate
/// and render. A list naming it falls through to its next mode.
pub const TURN_SYSTEM_TEMPLATE_UNIMPLEMENTED: &str = "turn_system_template_unimplemented";

/// What a request boundary offers, for choosing a send's mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Boundary {
    /// A tool batch has just finished with a result that can carry text.
    pub tool_result: bool,
    /// A turn is running and will render another step.
    pub running: bool,
    /// The session is idle and may start a run.
    pub idle: bool,
}

impl Boundary {
    /// The commit of a tool batch in a running turn.
    pub fn tool_batch(carrier: bool) -> Self {
        Self {
            tool_result: carrier,
            running: true,
            idle: false,
        }
    }

    /// A step boundary of a running turn.
    pub const STEP: Self = Self {
        tool_result: false,
        running: true,
        idle: false,
    };

    /// An idle session.
    pub const IDLE: Self = Self {
        tool_result: false,
        running: false,
        idle: true,
    };
}

/// The first mode of `order` possible at `boundary`, or `None` when the send must wait.
/// `has_text` says whether the send's input is text only (required by `tool_result`).
pub fn choose_mode(
    order: &DeliveryOrder,
    boundary: Boundary,
    has_text: bool,
) -> Option<DeliveryMode> {
    order.modes.iter().copied().find(|mode| match mode {
        DeliveryMode::ToolResult => boundary.tool_result && has_text,
        DeliveryMode::TurnSystem => false,
        DeliveryMode::Steer => boundary.running,
        DeliveryMode::IdleStart => boundary.idle,
    })
}

/// The text a `tool_result` delivery appends: the send's text blocks joined by blank lines, after
/// a blank line separating it from the tool's own output. `None` unless every input message is a
/// user message holding only text blocks.
pub fn appended_text(input: &[Message]) -> Option<String> {
    let mut parts = Vec::new();
    for message in input {
        if message.role != Role::User {
            return None;
        }
        for block in &message.content {
            match block {
                ContentBlock::Text { text } => parts.push(text.as_str()),
                _ => return None,
            }
        }
    }
    (!parts.is_empty()).then(|| format!("\n\n{}", parts.join("\n\n")))
}

/// The result a `tool_result` delivery rides on: the last result of the batch (in planned order)
/// whose output is text or an error message. Structured and denied results are never edited.
pub fn carrier<'a>(results: impl IntoIterator<Item = &'a ToolResult>) -> Option<&'a ToolCallId> {
    results
        .into_iter()
        .filter(|result| {
            matches!(
                result.output,
                ToolOutput::Text { .. } | ToolOutput::Error { .. }
            )
        })
        .last()
        .map(|result| &result.tool_call_id)
}

/// Append `text` to the rendered copy of a tool result. Only text and error outputs carry a
/// delivery (see [`carrier`]); any other output is left unchanged.
pub fn append_to_result(result: &mut ToolResult, text: &str) {
    match &mut result.output {
        ToolOutput::Text { text: output } => output.push_str(text),
        ToolOutput::Error { message } => message.push_str(text),
        ToolOutput::Json { .. } | ToolOutput::Denied { .. } => {}
    }
}

/// Apply a `tool_result` delivery to the newest tool message of `prompt` that holds the named
/// result. Returns whether a result was found. Replay calls this at the delivery record, which
/// follows the batch's `ToolBatchFinished`, so the tool message is the prompt's newest one.
pub fn apply_to_prompt(prompt: &mut [Message], tool_call_id: &ToolCallId, text: &str) -> bool {
    for message in prompt.iter_mut().rev() {
        if message.role != Role::Tool {
            continue;
        }
        for block in &mut message.content {
            if let ContentBlock::ToolResult(result) = block {
                if &result.tool_call_id == tool_call_id {
                    append_to_result(result, text);
                    return true;
                }
            }
        }
        return false;
    }
    false
}

/// One queued ordered send, as the running turn sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderedSend {
    pub submission_id: SubmissionId,
    pub order: DeliveryOrder,
    /// The principal that sent it (a plan-named sender, or else the principal that queued it);
    /// dedupe replaces only sends from the same one.
    pub principal: Option<BindPrincipal>,
    /// The user message(s) a `steer` delivery places.
    pub input: Vec<Message>,
}

impl OrderedSend {
    /// The queued entry of an ordered send, or `None` for a plain queued send.
    pub fn from_pending(pending: &crate::PendingSubmission) -> Option<Self> {
        let order = pending.order.clone()?;
        let input = match pending
            .hooks
            .as_ref()
            .and_then(|hooks| hooks.final_value.clone())
            .and_then(|value| serde_json::from_value::<Vec<Message>>(value).ok())
        {
            Some(input) => input,
            None => pending.request.input.clone(),
        };
        Some(Self {
            submission_id: pending.submission_id.clone(),
            order,
            principal: pending
                .sent_by
                .clone()
                .or_else(|| pending.principal.clone()),
            input: input
                .into_iter()
                .filter(|message| message.role != Role::System)
                .collect(),
        })
    }

    /// Whether `other` replaces this send: the same sender and the same dedupe key.
    pub fn replaced_by(&self, principal: Option<&BindPrincipal>, dedupe_key: Option<&str>) -> bool {
        dedupe_key.is_some()
            && self.order.dedupe_key.as_deref() == dedupe_key
            && self.principal.as_ref() == principal
    }
}

/// The record that ends `submission_id` undelivered.
pub fn ended(submission_id: &SubmissionId, reason: SendEndReason) -> WalRecord {
    WalRecord::SendEnded {
        submission_id: submission_id.clone(),
        reason,
    }
}

/// The record of a `steer` delivery of `send` into `run_id`.
pub fn steered(send: &OrderedSend, run_id: &RunId) -> WalRecord {
    WalRecord::SendDelivered {
        tags: None,
        submission_id: send.submission_id.clone(),
        run_id: run_id.clone(),
        mode: DeliveryMode::Steer,
        input: Some(Arc::new(send.input.clone())),
        tool_call_id: None,
        appended_text: None,
    }
}

/// The record of a `tool_result` delivery of `send` into `run_id`, riding on `tool_call_id`.
pub fn with_tool_result(
    send: &OrderedSend,
    run_id: &RunId,
    tool_call_id: &ToolCallId,
    text: String,
) -> WalRecord {
    WalRecord::SendDelivered {
        tags: None,
        submission_id: send.submission_id.clone(),
        run_id: run_id.clone(),
        mode: DeliveryMode::ToolResult,
        input: None,
        tool_call_id: Some(tool_call_id.clone()),
        appended_text: Some(text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(modes: &[DeliveryMode]) -> DeliveryOrder {
        DeliveryOrder {
            modes: modes.to_vec(),
            dedupe_key: None,
            expires_at_ms: None,
        }
    }

    fn text_result(id: &str, text: &str) -> ToolResult {
        ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: text.into() },
            is_error: false,
        }
    }

    /// The first possible mode wins; `turn_system` always falls through in this build; a list
    /// without `idle_start` is never chosen on an idle session.
    #[test]
    fn the_first_possible_mode_is_chosen() {
        use DeliveryMode::*;
        let both = order(&[ToolResult, Steer]);
        assert_eq!(
            choose_mode(&both, Boundary::tool_batch(true), true),
            Some(ToolResult)
        );
        assert_eq!(
            choose_mode(&both, Boundary::tool_batch(true), false),
            Some(Steer)
        );
        assert_eq!(
            choose_mode(&both, Boundary::tool_batch(false), true),
            Some(Steer)
        );
        assert_eq!(choose_mode(&both, Boundary::IDLE, true), None);
        let system_first = order(&[TurnSystem, Steer, IdleStart]);
        assert_eq!(
            choose_mode(&system_first, Boundary::STEP, true),
            Some(Steer)
        );
        assert_eq!(
            choose_mode(&system_first, Boundary::IDLE, true),
            Some(IdleStart)
        );
        assert_eq!(
            choose_mode(&order(&[TurnSystem]), Boundary::STEP, true),
            None
        );
        assert_eq!(
            choose_mode(&order(&[IdleStart]), Boundary::STEP, true),
            None
        );
    }

    /// The carrier is the batch's last text or error result; the appended text is applied to
    /// exactly that one result of the newest tool message.
    #[test]
    fn a_tool_result_delivery_appends_to_exactly_one_result() {
        let json = ToolResult {
            tool_call_id: ToolCallId("t3".into()),
            output: ToolOutput::Json {
                value: serde_json::json!({"a": 1}),
            },
            is_error: false,
        };
        let results = [text_result("t1", "one"), text_result("t2", "two"), json];
        assert_eq!(carrier(&results), Some(&ToolCallId("t2".into())));
        let mut prompt = vec![
            Message::new(Role::User, vec![ContentBlock::Text { text: "hi".into() }]),
            Message::new(
                Role::Tool,
                results
                    .iter()
                    .cloned()
                    .map(ContentBlock::ToolResult)
                    .collect(),
            ),
        ];
        assert!(apply_to_prompt(
            &mut prompt,
            &ToolCallId("t2".into()),
            "\n\nnote"
        ));
        let ContentBlock::ToolResult(first) = &prompt[1].content[0] else {
            panic!()
        };
        let ContentBlock::ToolResult(second) = &prompt[1].content[1] else {
            panic!()
        };
        assert_eq!(first.output, ToolOutput::Text { text: "one".into() });
        assert_eq!(
            second.output,
            ToolOutput::Text {
                text: "two\n\nnote".into()
            }
        );
        assert!(!apply_to_prompt(
            &mut prompt,
            &ToolCallId("missing".into()),
            "x"
        ));
    }

    #[test]
    fn appended_text_needs_text_only_user_input() {
        let text = Message::new(Role::User, vec![ContentBlock::Text { text: "a".into() }]);
        assert_eq!(
            appended_text(std::slice::from_ref(&text)).as_deref(),
            Some("\n\na")
        );
        let assistant = Message::new(
            Role::Assistant,
            vec![ContentBlock::Text { text: "a".into() }],
        );
        assert_eq!(appended_text(&[text, assistant]), None);
        assert_eq!(appended_text(&[]), None);
    }
}
