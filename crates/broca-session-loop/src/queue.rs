//! A queued send in the store's shape: built from the request a caller sent, and rebuilt into
//! that request when the send drains into a run or a retry is compared with it. Also a steer's
//! record, which keeps its messages and a receipt the same way.
//!
//! The record splits the request the way the converter splits today's `Queued` record, so a
//! send queued by the store actor and one converted from today's log are the same record:
//! its messages are typed, its tool list is a setup part, and the rest of the request is kept
//! whole. What today's record keeps beside the request (who sent it, its title, its identity
//! digest and so on) is the record's receipt.

use broca_protocol::request::SubmissionRequest;
use broca_session_store::record::{PartKind, QueuedSend, Steer};
use serde::Serialize;
use serde_json::Value;

use crate::parts::Parts;

/// Today's `Queued` record's fields that have no typed home in [`QueuedSend`], under the same
/// names. Each is left out when absent, as today's record leaves it out.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Receipt {
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
    pub hooks: Option<Box<broca_wal::Hooks>>,
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

/// The record for `request`, queued as `submission_id`. Its tool list, when it carries one, is
/// stored in `parts` first.
pub fn queued_send(
    submission_id: &str,
    send_id: Option<&str>,
    request: &SubmissionRequest,
    receipt: &Receipt,
    at: Queued,
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
    Ok(QueuedSend {
        submission_id: submission_id.to_owned(),
        send_id: send_id.map(str::to_owned),
        input,
        setup,
        written_at_ms: at.written_at_ms,
        ts_ms: at.ts_ms,
        request: (!rest.is_empty()).then_some(Value::Object(rest)),
        receipt,
        delivery,
    })
}

/// The request `send` was queued with: its messages and its tool list back in place.
pub fn request_of(send: &QueuedSend, parts: &dyn Parts) -> Result<SubmissionRequest, String> {
    let mut request = match &send.request {
        Some(Value::Object(rest)) => rest.clone(),
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
                request.insert("tools".into(), parts.get(part)?);
            }
            other => return Err(format!("a queued send names a {other:?} part")),
        }
    }
    serde_json::from_value(Value::Object(request)).map_err(json)
}

/// Today's `Steered` record's fields beside its messages, time and part numbers, under the same
/// names. Each is left out when absent, as today's record leaves it out.
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
    pub hooks: Option<Box<broca_wal::Hooks>>,
}

/// The record of a steer into a running run: its messages, accepted at `written_at_ms`. The run
/// places them at its next step boundary, so the record carries no part numbers yet.
pub fn steer(
    input: Vec<broca_protocol::Message>,
    written_at_ms: Option<u64>,
    receipt: &SteerReceipt,
) -> Result<Steer, String> {
    let receipt = match serde_json::to_value(receipt).map_err(|e| format!("a steer: {e}"))? {
        Value::Object(receipt) if receipt.is_empty() => None,
        receipt => Some(receipt),
    };
    Ok(Steer {
        input,
        written_at_ms,
        tags_v1: None,
        receipt,
    })
}

/// The receipt field `name` of `send`, if its receipt has it.
pub fn receipt_field<'a>(send: &'a QueuedSend, name: &str) -> Option<&'a Value> {
    send.receipt.as_ref()?.get(name)
}

#[cfg(test)]
mod tests;
