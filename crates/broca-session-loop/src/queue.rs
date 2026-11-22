//! A queued send in the store's shape: built from the request a caller sent, and rebuilt into
//! that request when the send drains into a run or a retry is compared with it. Also a
//! steer's record, which keeps its messages and a receipt the same way.
//!
//! A send queued by the store actor and one converted from the old log format's `Queued`
//! record are the same record: its messages are typed, its tool list is a setup part, and the rest of
//! the request is kept whole. The receipt holds what accompanies the request, such as who
//! sent it, its title and its identity digest.

use broca_protocol::request::SubmissionRequest;
use broca_session_store::record::{PartKind, QueuedSend, Steer};
use serde::Serialize;
use serde_json::Value;

use crate::parts::Parts;

/// The `Queued` record's fields that have no typed home in [`QueuedSend`], under the same
/// names. Each is left out when absent.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Receipt {
    /// A class an unlabelled session may adopt when this queued send starts its run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_by: Option<broca_protocol::BindPrincipal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_warm: Option<broca_protocol::request::KeepWarm>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_identity_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<broca_protocol::BindPrincipal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hooks: Option<Box<broca_records::record::Hooks>>,
}

/// Where a queued send sits in time and how it asked to be delivered.
#[derive(Debug, Clone, Copy, Default)]
pub struct Queued {
    /// When its message was accepted, which the message keeps when it is placed.
    pub written_at_ms: Option<u64>,
    /// When a session with a retention policy admitted it.
    pub ts_ms: Option<u64>,
    /// `None` for a plain queued send, whose record names no mode.
    pub delivery: Option<broca_protocol::Delivery>,
}

/// The record for `request`, queued as `submission_id`, with the display marks its admission
/// kept (`message_marks`). Its tool list, when it carries one, is stored in `parts` first.
pub fn queued_send(
    submission_id: &str,
    send_id: Option<&str>,
    request: &SubmissionRequest,
    receipt: &Receipt,
    at: Queued,
    message_marks: Option<&broca_protocol::MessageMarks>,
    parts: &mut dyn Parts,
) -> Result<QueuedSend, String> {
    let json = |error: serde_json::Error| format!("a queued send: {error}");
    let Value::Object(mut rest) = serde_json::to_value(request).map_err(json)? else {
        return Err("a queued send's request does not serialize as an object".into());
    };
    let input = match rest.remove("input") {
        Some(input) => serde_json::from_value(input).map_err(json)?,
        None => Vec::new(),
    };
    let mut setup = Vec::new();
    if let Some(tools) = rest.remove("tools") {
        setup.push(parts.put(PartKind::Tools, &tools)?);
    }
    let receipt = match serde_json::to_value(receipt).map_err(json)? {
        Value::Object(receipt) if receipt.is_empty() => None,
        receipt => Some(receipt),
    };
    let delivery = match at.delivery {
        Some(delivery) => match serde_json::to_value(delivery).map_err(json)? {
            Value::String(name) => Some(name),
            other => return Err(format!("a delivery mode serializes as {other}, not a name")),
        },
        None => None,
    };
    // Stored so the drained run's start, a ride's notice and reads serve them alike.
    let message_marks = message_marks
        .map(serde_json::to_value)
        .transpose()
        .map_err(json)?;
    Ok(QueuedSend {
        // Filled as the record is appended, by the append that stores the media's bytes
        // (`Session::append_input_media`).
        media: Vec::new(),
        submission_id: submission_id.to_owned(),
        send_id: send_id.map(str::to_owned),
        input,
        setup,
        written_at_ms: at.written_at_ms,
        ts_ms: at.ts_ms,
        request: (!rest.is_empty()).then_some(Value::Object(rest).into()),
        receipt: receipt.map(Into::into),
        delivery,
        named_speaker: broca_session_store::record::marks_name_speaker(message_marks.as_ref()),
        kept_notice: broca_session_store::record::marks_kept_notice(message_marks.as_ref()),
        message_marks,
    })
}

/// The request `send` was queued with: its messages and its tool list back in place.
pub fn request_of(send: &QueuedSend, parts: &dyn Parts) -> Result<SubmissionRequest, String> {
    let mut request = match send.request.as_ref().map(broca_protocol::json::Json::parse) {
        Some(Value::Object(rest)) => rest,
        Some(other) => return Err(format!("a queued send's request is {other}, not an object")),
        None => serde_json::Map::new(),
    };
    let json = |error: serde_json::Error| format!("a queued send: {error}");
    request.insert(
        "input".into(),
        serde_json::to_value(&send.input).map_err(json)?,
    );
    for part in &send.setup {
        match part.kind {
            PartKind::Tools => {
                request.insert("tools".into(), crate::parts::value(parts, part)?);
            }
            other => return Err(format!("a queued send names a {other:?} part")),
        }
    }
    serde_json::from_value(Value::Object(request)).map_err(json)
}

/// The `Steered` record's fields beside its messages, time and part numbers, under the same
/// names. Each is left out when absent.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SteerReceipt {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_by: Option<broca_protocol::BindPrincipal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_identity_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<broca_protocol::Delivery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hooks: Option<Box<broca_records::record::Hooks>>,
}

/// The record of a steer into a running run: its messages, accepted at `written_at_ms`, with the
/// display marks its admission kept (`message_marks`). The run places them at its next step
/// boundary, so the record carries no part numbers yet.
pub fn steer(
    input: Vec<broca_protocol::Message>,
    written_at_ms: Option<u64>,
    receipt: &SteerReceipt,
    message_marks: Option<&broca_protocol::MessageMarks>,
) -> Result<Steer, String> {
    let receipt = match serde_json::to_value(receipt).map_err(|e| format!("a steer: {e}"))? {
        Value::Object(receipt) if receipt.is_empty() => None,
        receipt => Some(receipt),
    };
    // Stored on the steer record.
    let message_marks = message_marks
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| format!("a steer's marks: {e}"))?;
    Ok(Steer {
        // Filled as the record is appended, by the append that stores the media's bytes.
        media: Vec::new(),
        input,
        written_at_ms,
        tags_v1: None,
        receipt: receipt.map(Into::into),
        named_speaker: broca_session_store::record::marks_name_speaker(message_marks.as_ref()),
        kept_notice: broca_session_store::record::marks_kept_notice(message_marks.as_ref()),
        message_marks,
    })
}

/// The receipt field `name` of `send`, if its receipt has it.
pub fn receipt_field(send: &QueuedSend, name: &str) -> Option<Value> {
    send.receipt
        .as_ref()?
        .parse()
        .get_mut(name)
        .map(Value::take)
}

#[cfg(test)]
mod tests;
