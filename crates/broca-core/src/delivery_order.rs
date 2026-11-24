//! Sends whose `delivery` is an ordered list of modes (the `delivery_order` group).
//!
//! Such a send is durably queued (`DeliveryOrdered` then `Queued`, one barrier) and waits for a
//! request boundary where one of its modes is possible. At each boundary the runner takes the
//! FIRST mode in the send's list that is possible there:
//!
//! - `tool_result`: when a tool batch of the running turn has just finished with a text or error
//!   result to carry the send's text, and the send's input is text only. Decided as the batch
//!   commits, so the delivery record sits before the turn's `TurnFinished`.
//! - `turn_system`: rendered template text at a confirmed provider route and legal position.
//! - `steer`: at every step boundary of a running turn.
//! - `idle_start`: when the session is idle; the actor drains the send into a new run.
//!
//! A send whose list has no possible mode at a boundary stays queued for the next one, across
//! runs. A list without `idle_start` therefore never starts a run.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolOutput, ToolResult},
    BindPrincipal, DeliveryMode, DeliveryOrder, RunId, SubmissionId, ToolCallId,
};
use broca_records::record::SendEndReason;

/// What a request boundary offers, for choosing a send's mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Boundary {
    /// A tool batch has just finished with a result that can carry text.
    pub tool_result: bool,
    /// The route accepts a turn-scoped system entry at this position.
    pub turn_system: bool,
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
            turn_system: false,
            running: true,
            idle: false,
        }
    }

    /// A step boundary of a running turn.
    pub const STEP: Self = Self {
        tool_result: false,
        turn_system: false,
        running: true,
        idle: false,
    };

    /// An idle session.
    pub const IDLE: Self = Self {
        tool_result: false,
        turn_system: false,
        running: false,
        idle: true,
    };
}

/// `senders` with each module spelled by its canonical id ([`broca_protocol::canonical_module`]).
/// The contract's delivery checks compare ids exactly; a plan written before a module's rename
/// names its old id and a caller after it presents the new one, so both sides are passed
/// through the canonical spelling.
pub fn canonical_senders(
    senders: &[cortexkit_role_llm_runner::delivery_order::PlanSender],
) -> Vec<cortexkit_role_llm_runner::delivery_order::PlanSender> {
    senders
        .iter()
        .cloned()
        .map(|mut sender| {
            sender.module = broca_protocol::canonical_module(&sender.module).to_owned();
            sender
        })
        .collect()
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
        DeliveryMode::TurnSystem => boundary.turn_system && has_text && order.template.is_some(),
        DeliveryMode::Steer => boundary.running,
        DeliveryMode::IdleStart => boundary.idle,
        _ => false,
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

/// Mode-3 renders delivered text after the result, independently of its native
/// output kind. Called only for the newly completed batch's first placement.
pub fn carrier_mode3<'a>(
    results: impl IntoIterator<Item = &'a ToolResult>,
) -> Option<&'a ToolCallId> {
    results
        .into_iter()
        .last()
        .map(|result| &result.tool_call_id)
}

/// New ride envelopes preserve the native content list; legacy deliveries keep their
/// original renderer because a resumed model request must not acquire different bytes.
pub fn append_ride(
    result: &mut ToolResult,
    ride: &broca_records::record::RideMetadata,
    text: &str,
) {
    let rides = [cortexkit_role_llm_runner::rides::Ride::new(
        &ride.send_id,
        &ride.sender,
        &ride.call_key,
        text.strip_prefix("\n\n").unwrap_or(text),
    )];
    append_ride_output(&mut result.output, &rides);
}

pub(crate) fn append_ride_output(
    output: &mut ToolOutput,
    rides: &[cortexkit_role_llm_runner::rides::Ride],
) {
    use cortexkit_role_llm_runner::rides::{self, ContentItem};
    match output {
        ToolOutput::Text { text } => *text = rides::append(text, rides).into(),
        ToolOutput::Error { message, .. } => *message = rides::append(message, rides),
        ToolOutput::Parts { parts } => {
            let mut content: Vec<_> = std::mem::take(parts)
                .into_iter()
                .map(|part| match part {
                    broca_protocol::ToolOutputPart::Text { text } => {
                        ContentItem::Text(text.to_string())
                    }
                    part => ContentItem::Opaque(part),
                })
                .collect();
            rides::append_to_content(&mut content, rides);
            *parts = content
                .into_iter()
                .map(|item| match item {
                    ContentItem::Text(text) => {
                        broca_protocol::ToolOutputPart::Text { text: text.into() }
                    }
                    ContentItem::Opaque(part) => part,
                })
                .collect();
        }
        ToolOutput::Json { value } => {
            *output = ToolOutput::Text {
                text: rides::append(&value.to_string(), rides).into(),
            }
        }
        ToolOutput::Denied { reason } => *output = ToolOutput::error(rides::append(reason, rides)),
    }
}

/// One queued ordered send, as the running turn sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderedSend {
    pub admitted_at_ms: Option<u64>,
    pub submission_id: SubmissionId,
    pub order: DeliveryOrder,
    /// The principal that sent it (a plan-named sender, or else the principal that queued it);
    /// dedupe replaces only sends from the same one.
    pub principal: Option<BindPrincipal>,
    /// The user message(s) a `steer` delivery places.
    pub input: Vec<Message>,
}

impl OrderedSend {
    /// Whether `other` replaces this send: the same sender and the same dedupe key.
    pub fn replaced_by(&self, principal: Option<&BindPrincipal>, dedupe_key: Option<&str>) -> bool {
        dedupe_key.is_some()
            && self.order.dedupe_key.as_deref() == dedupe_key
            && self.principal.as_ref() == principal
    }
}

/// The messages a `turn_system` delivery of `send` places: its input as turn-scoped system
/// text. Admission chooses whether system authority is possible; the delivery's record keeps
/// these messages, so a later render uses them without rendering the send again.
pub fn system_messages(send: &OrderedSend) -> Vec<Message> {
    let mut input = send.input.clone();
    for message in &mut input {
        message.role = Role::System;
        message.turn_system = true;
    }
    input
}

/// What became of the queued send whose submission is `sub-<run_id>`, read from the
/// session's log alone.
///
/// A queued send's reply names only its submission id, and the run it would start takes the
/// run id that submission id was minted from. Callers poll `run.status` on that run id, so
/// the id must keep answering after the send went somewhere other than its own run: delivered
/// into a run that was already going, replaced, expired or withdrawn. Every answer here
/// comes from durable records, so it is the same before and after the session's actor
/// restarts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueuedSendFate {
    /// Queued and still waiting for a boundary that can deliver it.
    Pending,
    /// Delivered into the run `run_id`, which was already going, in `mode`: the send became
    /// part of that run and starts none of its own.
    Delivered { run_id: RunId, mode: DeliveryMode },
    /// Started its own run under this run id. The run's own records answer for it.
    Started,
    /// Ended undelivered: replaced by a newer send under the same dedupe key, or expired.
    Ended(SendEndReason),
    /// Withdrawn by `session.retract` before it started.
    Retracted,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(modes: &[DeliveryMode]) -> DeliveryOrder {
        DeliveryOrder {
            modes: modes.to_vec(),
            dedupe_key: None,
            expires_at_ms: None,
            template: None,
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

/// Reader support ships before any writer enables the new resume-sensitive envelope.
pub const RIDES_V2_WRITER_ENABLED: bool = true;

#[cfg(test)]
mod rename_tests {
    use cortexkit_role_llm_runner::delivery_order::{admit, Mode, PlanSender};

    /// A plan written before a module's rename names its old id; the module sends under its new
    /// one afterwards, and keeps the plan's grants. A near name gets none.
    #[test]
    fn a_plan_naming_either_id_of_a_renamed_module_grants_both() {
        // Plans written before the rename name the old id; plans recomposed after it name the
        // new one. Either plan admits a caller presenting either id.
        for named in ["prefrontal-core", "prefrontal"] {
            let senders: Vec<PlanSender> = serde_json::from_value(serde_json::json!([
                {"module": named, "idle_start": true}
            ]))
            .unwrap();
            let senders = super::canonical_senders(&senders);
            let owner = Some("reserved:cingulate");
            for caller in ["prefrontal", "prefrontal-core"] {
                let caller = broca_protocol::canonical_module(caller);
                assert_eq!(
                    admit(caller, owner, &senders, &[Mode::IdleStart]),
                    Ok(()),
                    "plan names {named}"
                );
            }
            let near = broca_protocol::canonical_module("prefrontal2");
            assert!(admit(near, owner, &senders, &[Mode::IdleStart]).is_err());
        }
    }
}

#[cfg(test)]
mod ride_v2_tests {
    use super::*;
    use broca_protocol::{MediaKind, MediaRef, ToolOutputPart};
    use cortexkit_role_llm_runner::rides::Ride;
    use serde_json::Value;

    fn parts(value: &Value) -> Vec<ToolOutputPart> {
        value
            .as_array()
            .unwrap()
            .iter()
            .map(|item| {
                if let Some(text) = item["text"].as_str() {
                    ToolOutputPart::Text { text: text.into() }
                } else {
                    ToolOutputPart::Media {
                        media: MediaRef {
                            kind: MediaKind::Image,
                            mime: "image/png".into(),
                            byte_size: 1,
                            sha256: item["image"].as_str().unwrap().into(),
                            width: Some(1),
                            height: Some(1),
                        },
                    }
                }
            })
            .collect()
    }

    #[test]
    fn rides_v2_contract_helper_vectors_pin_native_rendering() {
        let vectors: Value = serde_json::from_str(
            cortexkit_role_llm_runner::vectors::file("result-rides.json").unwrap(),
        )
        .unwrap();
        for case in vectors["strings"].as_array().unwrap() {
            let rides: Vec<_> = case["rides"]
                .as_array()
                .unwrap()
                .iter()
                .map(|text| Ride::new("send", "sender", "key", text.as_str().unwrap()))
                .collect();
            for mut output in [
                ToolOutput::Text {
                    text: case["output"].as_str().unwrap().into(),
                },
                ToolOutput::error(case["output"].as_str().unwrap()),
            ] {
                append_ride_output(&mut output, &rides);
                let actual = match output {
                    ToolOutput::Text { text } => text,
                    ToolOutput::Error { message, .. } => message.into(),
                    _ => unreachable!(),
                };
                assert_eq!(
                    actual,
                    case["expected"].as_str().unwrap(),
                    "{}",
                    case["name"]
                );
            }
        }
        for case in vectors["content"].as_array().unwrap() {
            let rides: Vec<_> = case["rides"]
                .as_array()
                .unwrap()
                .iter()
                .map(|text| Ride::new("send", "sender", "key", text.as_str().unwrap()))
                .collect();
            let mut output = ToolOutput::Parts {
                parts: parts(&case["input"]),
            };
            append_ride_output(&mut output, &rides);
            assert_eq!(
                output,
                ToolOutput::Parts {
                    parts: parts(&case["expected"])
                },
                "{}",
                case["name"]
            );
        }
    }
}
