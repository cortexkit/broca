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
use broca_wal::{record::SendEndReason, WalRecord};
use std::sync::Arc;

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

/// Append `text` to the rendered copy of a tool result. Only text and error outputs carry a
/// delivery (see [`carrier`]); any other output is left unchanged.
pub fn append_to_result(result: &mut ToolResult, text: &str) {
    match &mut result.output {
        ToolOutput::Text { text: output } => output.push_str(text),
        ToolOutput::Error { message, .. } => message.push_str(text),
        ToolOutput::Json { .. } | ToolOutput::Denied { .. } => {}
        // The Rendering contract keeps multipart results in source order, including appended text.
        ToolOutput::Parts { parts } => {
            parts.push(broca_protocol::ToolOutputPart::Text { text: text.into() })
        }
    }
}

/// New ride envelopes preserve the native content list; legacy deliveries keep their
/// original renderer because a resumed model request must not acquire different bytes.
pub fn append_ride(result: &mut ToolResult, ride: &broca_wal::record::RideMetadata, text: &str) {
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
        ToolOutput::Text { text } => *text = rides::append(text, rides),
        ToolOutput::Error { message, .. } => *message = rides::append(message, rides),
        ToolOutput::Parts { parts } => {
            let mut content: Vec<_> = std::mem::take(parts)
                .into_iter()
                .map(|part| match part {
                    broca_protocol::ToolOutputPart::Text { text } => ContentItem::Text(text),
                    part => ContentItem::Opaque(part),
                })
                .collect();
            rides::append_to_content(&mut content, rides);
            *parts = content
                .into_iter()
                .map(|item| match item {
                    ContentItem::Text(text) => broca_protocol::ToolOutputPart::Text { text },
                    ContentItem::Opaque(part) => part,
                })
                .collect();
        }
        ToolOutput::Json { value } => {
            *output = ToolOutput::Text {
                text: rides::append(&value.to_string(), rides),
            }
        }
        ToolOutput::Denied { reason } => *output = ToolOutput::error(rides::append(reason, rides)),
    }
}

pub fn apply_ride_to_prompt(
    prompt: &mut [Message],
    call: &ToolCallId,
    ride: &broca_wal::record::RideMetadata,
    text: &str,
) -> bool {
    if let Some(message) = prompt
        .iter_mut()
        .rfind(|message| message.role == Role::Tool)
    {
        for block in message.content.iter_mut().rev() {
            if let ContentBlock::ToolResult(result) = block {
                if &result.tool_call_id == call {
                    append_ride(result, ride, text);
                    return true;
                }
            }
        }
    }
    false
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
            admitted_at_ms: pending.admitted_at_ms,
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
        ride: None,
        written_at_ms: send.admitted_at_ms,
        tags: None,
        submission_id: send.submission_id.clone(),
        run_id: run_id.clone(),
        mode: DeliveryMode::Steer,
        input: Some(Arc::new(send.input.clone())),
        tool_call_id: None,
        appended_text: None,
    }
}

/// Freeze a rendered template at its conversation position. Admission chooses whether
/// system authority is possible; replay uses the stored message without re-rendering slots.
pub fn system_delivered(send: &OrderedSend, run_id: &RunId) -> WalRecord {
    let mut input = send.input.clone();
    for message in &mut input {
        message.role = Role::System;
        message.turn_system = true;
    }
    WalRecord::SendDelivered {
        ride: None,
        written_at_ms: send.admitted_at_ms,
        tags: None,
        submission_id: send.submission_id.clone(),
        run_id: run_id.clone(),
        mode: DeliveryMode::TurnSystem,
        input: Some(Arc::new(input)),
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
        ride: None,
        written_at_ms: send.admitted_at_ms,
        tags: None,
        submission_id: send.submission_id.clone(),
        run_id: run_id.clone(),
        mode: DeliveryMode::ToolResult,
        input: None,
        tool_call_id: Some(tool_call_id.clone()),
        appended_text: Some(text),
    }
}

/// What became of the queued send whose submission is `sub-<run_id>`, read from the WAL alone.
///
/// A queued send's reply names only its submission id, and the run it would start takes the
/// run id that submission id was minted from. Callers poll `run.status` on that run id, so the
/// id must keep answering after the send went somewhere other than its own run: delivered into
/// a run that was already going, replaced, expired or withdrawn. Every answer here comes from
/// durable records, so it is the same before and after the session's actor restarts.
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

/// The fate of the queued send minted from `run_id` (submission `sub-<run_id>`), or `None` when
/// the records hold no queued send under that submission (the id is not a queued send's).
///
/// The records that settle a queued send are each written once, after its `Queued` record:
/// `SendDelivered` or a `RunStarted` naming it when it is delivered, `SendEnded` or
/// `Retracted` when it is not. The first settling record wins, so a later record about another
/// send never changes an answer once given.
pub fn queued_send_fate<'a>(
    records: impl IntoIterator<Item = &'a WalRecord>,
    run_id: &str,
) -> Option<QueuedSendFate> {
    let submission = format!("sub-{run_id}");
    let mut fate = None;
    for record in records {
        let settled = match record {
            WalRecord::Queued { submission_id, .. } if submission_id.0 == submission => {
                fate.get_or_insert(QueuedSendFate::Pending);
                continue;
            }
            WalRecord::SendDelivered {
                submission_id,
                run_id: into,
                mode,
                ..
            } if submission_id.0 == submission => QueuedSendFate::Delivered {
                run_id: into.clone(),
                mode: *mode,
            },
            WalRecord::RunStarted {
                run_id: started,
                submission_id,
                ..
            } if started.0 == run_id
                || submission_id.as_ref().is_some_and(|id| id.0 == submission) =>
            {
                QueuedSendFate::Started
            }
            WalRecord::SendEnded {
                submission_id,
                reason,
            } if submission_id.0 == submission => QueuedSendFate::Ended(reason.clone()),
            WalRecord::Retracted { submission_id, .. } if submission_id.0 == submission => {
                QueuedSendFate::Retracted
            }
            _ => continue,
        };
        match fate {
            None | Some(QueuedSendFate::Pending) => fate = Some(settled),
            Some(_) => {}
        }
    }
    fate
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

    fn text_result(id: &str, text: &str) -> ToolResult {
        ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: text.into() },
            is_error: false,
        }
    }

    fn record(json: &str) -> WalRecord {
        serde_json::from_str(json).unwrap()
    }

    /// A queued send's fate is the first record that settles it: queued alone is pending, and a
    /// delivery, an end, a retraction or its own run start each settle it for good. An id that
    /// names no queued send has no fate.
    #[test]
    fn a_queued_sends_fate_is_its_first_settling_record() {
        let queued = record(
            r#"{"type":"queued","submission_id":"sub-r2","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"note"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#,
        );
        let delivered = record(
            r#"{"type":"send_delivered","submission_id":"sub-r2","run_id":"r1","mode":"steer","input":[{"role":"user","content":[{"type":"text","text":"note"}]}]}"#,
        );
        let superseded = record(
            r#"{"type":"send_ended","submission_id":"sub-r2","reason":{"kind":"superseded","by":"sub-r3"}}"#,
        );
        let retracted = record(r#"{"type":"retracted","submission_id":"sub-r2"}"#);
        let other = record(r#"{"type":"retracted","submission_id":"sub-r9"}"#);

        assert_eq!(
            queued_send_fate([&queued], "r2"),
            Some(QueuedSendFate::Pending)
        );
        assert_eq!(
            queued_send_fate([&queued, &delivered, &superseded], "r2"),
            Some(QueuedSendFate::Delivered {
                run_id: RunId("r1".into()),
                mode: DeliveryMode::Steer
            })
        );
        assert_eq!(
            queued_send_fate([&queued, &superseded], "r2"),
            Some(QueuedSendFate::Ended(SendEndReason::Superseded {
                by: SubmissionId("sub-r3".into())
            }))
        );
        assert_eq!(
            queued_send_fate([&queued, &other, &retracted], "r2"),
            Some(QueuedSendFate::Retracted)
        );
        assert_eq!(
            queued_send_fate([&queued, &other], "r2"),
            Some(QueuedSendFate::Pending)
        );
        assert_eq!(queued_send_fate([&queued], "r9"), None);
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

/// Reader support ships before any writer enables the new resume-sensitive envelope.
pub const RIDES_V2_WRITER_ENABLED: bool = false;

#[derive(Default)]
pub(crate) struct RideLedger {
    anchors: crate::call_identity::CallAnchors,
    sends: std::collections::HashMap<String, (String, String, Vec<String>)>,
    orders: std::collections::HashMap<String, (Option<broca_protocol::BindPrincipal>, Vec<String>)>,
}

impl RideLedger {
    pub(crate) fn observe(&mut self, record: &WalRecord, seq: u64) {
        self.anchors.observe(record, seq);
        match record {
            WalRecord::DeliveryOrdered {
                submission_id,
                order,
                sent_by,
            } => {
                let forms = order
                    .modes
                    .iter()
                    .filter_map(|mode| match mode {
                        DeliveryMode::ToolResult => Some("tool_result".into()),
                        DeliveryMode::TurnSystem => Some("turn_system".into()),
                        _ => None,
                    })
                    .collect();
                self.orders
                    .insert(submission_id.0.clone(), (sent_by.clone(), forms));
            }
            WalRecord::Queued {
                submission_id,
                send_id,
                sent_by,
                principal,
                ..
            } => {
                let (ordered_sender, forms) = self
                    .orders
                    .get(&submission_id.0)
                    .cloned()
                    .unwrap_or_default();
                let principal = sent_by
                    .as_ref()
                    .or(ordered_sender.as_ref())
                    .or(principal.as_ref());
                let sender = match principal {
                    Some(broca_protocol::BindPrincipal::Reserved { module_id }) => {
                        module_id.clone()
                    }
                    _ => "broca".into(),
                };
                self.sends.insert(
                    submission_id.0.clone(),
                    (
                        send_id.clone().unwrap_or_else(|| submission_id.0.clone()),
                        sender,
                        forms,
                    ),
                );
            }
            WalRecord::SendDelivered { submission_id, .. }
            | WalRecord::SendEnded { submission_id, .. }
            | WalRecord::Retracted { submission_id, .. } => {
                self.sends.remove(&submission_id.0);
                self.orders.remove(&submission_id.0);
            }
            _ => {}
        }
    }

    pub(crate) fn seed_pending(&mut self, pending: &crate::PendingSubmission) {
        let Some(order) = pending.order.as_ref() else {
            return;
        };
        let sender = match pending
            .display_sent_by
            .as_ref()
            .or(pending.sent_by.as_ref())
            .or(pending.principal.as_ref())
        {
            Some(BindPrincipal::Reserved { module_id }) => module_id.clone(),
            _ => "broca".into(),
        };
        let forms = order
            .modes
            .iter()
            .filter_map(|mode| match mode {
                DeliveryMode::ToolResult => Some("tool_result".into()),
                DeliveryMode::TurnSystem => Some("turn_system".into()),
                _ => None,
            })
            .collect();
        self.sends.insert(
            pending.submission_id.0.clone(),
            (
                pending
                    .send_id
                    .clone()
                    .unwrap_or_else(|| pending.submission_id.0.clone()),
                sender,
                forms,
            ),
        );
    }

    pub(crate) fn envelope(
        &self,
        record: &WalRecord,
        lineage: Option<&str>,
    ) -> Option<broca_wal::record::RideMetadata> {
        let WalRecord::SendDelivered {
            submission_id,
            mode: DeliveryMode::ToolResult,
            tool_call_id: Some(call),
            ..
        } = record
        else {
            return None;
        };
        let (send_id, sender, forms) = self.sends.get(&submission_id.0)?;
        Some(broca_wal::record::RideMetadata {
            call_key: self
                .anchors
                .slots()
                .iter()
                .rfind(|slot| &slot.id == call)?
                .key(lineage)?,
            send_id: send_id.clone(),
            sender: sender.clone(),
            forms: forms.clone(),
            form: broca_wal::record::RideForm::Append,
        })
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
                    ToolOutput::Error { message, .. } => message,
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

    #[test]
    fn rides_v2_framed_resume_preserves_old_and_new_multipart_rules() {
        let source = serde_json::json!([{"image":"opaque"},{"text":"caption"}]);
        let mut old = ToolResult {
            tool_call_id: ToolCallId("call".into()),
            output: ToolOutput::Parts {
                parts: parts(&source),
            },
            is_error: false,
        };
        append_to_result(&mut old, "\n\nnote");
        assert_eq!(
            old.output,
            ToolOutput::Parts {
                parts: parts(
                    &serde_json::json!([{"image":"opaque"},{"text":"caption"},{"text":"\n\nnote"}])
                )
            }
        );
        for (input, expected) in [
            (
                source,
                serde_json::json!([{"image":"opaque"},{"text":"caption\n\nnote"}]),
            ),
            (
                serde_json::json!([{"text":"caption"},{"image":"opaque"}]),
                serde_json::json!([{"text":"caption"},{"image":"opaque"},{"text":"\n\nnote"}]),
            ),
        ] {
            let record: WalRecord = serde_json::from_value(serde_json::json!({"type":"send_delivered","submission_id":"submission","run_id":"run","mode":"tool_result","tool_call_id":"call","appended_text":"\n\nnote","ride":{"call_key":"log:7","send_id":"send","sender":"sender","forms":["tool_result"],"form":"append"}})).unwrap();
            let bytes = broca_wal::framing::encode(8, 1, &record).unwrap();
            let broca_wal::framing::DecodeStep::Record { framed, .. } =
                broca_wal::framing::decode_next(&bytes)
            else {
                panic!("ride frame refused")
            };
            let WalRecord::SendDelivered {
                ride: Some(ride),
                appended_text: Some(text),
                tool_call_id: Some(call),
                ..
            } = framed.record
            else {
                panic!("ride envelope lost")
            };
            let mut prompt = vec![Message::new(
                Role::Tool,
                vec![ContentBlock::ToolResult(ToolResult {
                    tool_call_id: call.clone(),
                    output: ToolOutput::Parts {
                        parts: parts(&input),
                    },
                    is_error: false,
                })],
            )];
            assert!(apply_ride_to_prompt(&mut prompt, &call, &ride, &text));
            let ContentBlock::ToolResult(result) = &prompt[0].content[0] else {
                unreachable!()
            };
            assert_eq!(
                result.output,
                ToolOutput::Parts {
                    parts: parts(&expected)
                }
            );
        }
    }
}
