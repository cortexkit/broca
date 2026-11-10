//! Recorded call facts exported beside a Thalamus transcript, located by source message and id.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::Value;

use super::{HeadError, HeadHistory, OPENCODE2_CARRIER};

/// The call side file beside an export (`head.json` → `head.calls.json`).
pub fn calls_path(source: &Path) -> PathBuf {
    source.with_extension("calls.json")
}

/// One call's recorded facts. Missing fields remain unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallRow {
    pub mid: String,
    pub tool_call_id: String,
    pub call_key: String,
    pub call_carrier: Option<String>,
    pub schema_pin: Option<String>,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
    pub unanswered: Option<String>,
}

impl CallRow {
    fn carrier(&self) -> &str {
        self.call_carrier.as_deref().unwrap_or(OPENCODE2_CARRIER)
    }
}

/// Reads every member strictly, including the envelope, and rejects duplicate source calls.
pub fn read_calls(bytes: &[u8]) -> Result<Vec<CallRow>, HeadError> {
    let refuse = |reason: String| HeadError::Calls(reason);
    let value: Value =
        serde_json::from_slice(bytes).map_err(|error| refuse(format!("not JSON: {error}")))?;
    let envelope = value
        .as_object()
        .ok_or_else(|| refuse("not an object".into()))?;
    if let Some(key) = envelope.keys().find(|key| key.as_str() != "calls") {
        return Err(refuse(format!("member `{key}` is not one the import maps")));
    }
    let rows = envelope
        .get("calls")
        .and_then(Value::as_array)
        .ok_or_else(|| refuse("`calls` is not an array".into()))?;
    let mut identities = HashSet::new();
    rows.iter()
        .enumerate()
        .map(|(index, value)| {
            let row = row_of(value).map_err(|reason| refuse(format!("row {index}: {reason}")))?;
            if !identities.insert((row.mid.clone(), row.tool_call_id.clone())) {
                return Err(refuse(format!(
                    "row {index}: duplicate call ({}, {})",
                    row.mid, row.tool_call_id
                )));
            }
            Ok(row)
        })
        .collect()
}

fn row_of(value: &Value) -> Result<CallRow, String> {
    const KEYS: &[&str] = &[
        "mid",
        "tool_call_id",
        "call_key",
        "call_carrier",
        "schema_pin",
        "started_at",
        "finished_at",
        "unanswered",
    ];
    let object = value.as_object().ok_or("the row is not an object")?;
    if let Some(key) = object.keys().find(|key| !KEYS.contains(&key.as_str())) {
        return Err(format!("member `{key}` is not one the import maps"));
    }
    let string = |key: &str| -> Result<Option<String>, String> {
        match object.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(text)) => Ok(Some(text.clone())),
            Some(_) => Err(format!("`{key}` is not a string")),
        }
    };
    let time = |key: &str| -> Result<Option<u64>, String> {
        match object.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_u64()
                .map(Some)
                .ok_or_else(|| format!("`{key}` is not Unix milliseconds")),
        }
    };
    let call_key = string("call_key")?
        .filter(|key| !key.is_empty())
        .ok_or("the row has no `call_key`")?;
    let mid = string("mid")?
        .filter(|mid| !mid.is_empty())
        .ok_or("the row has no `mid`")?;
    let tool_call_id = string("tool_call_id")?
        .filter(|id| !id.is_empty())
        .ok_or("the row has no `tool_call_id`")?;
    let unanswered = string("unanswered")?;
    if let Some(reason) = &unanswered {
        if !["interrupted", "cancelled", "denied", "not_executed"].contains(&reason.as_str()) {
            return Err(format!(
                "`unanswered` {reason:?} is not a closure the import maps"
            ));
        }
    }
    Ok(CallRow {
        mid,
        tool_call_id,
        call_key,
        call_carrier: string("call_carrier")?,
        schema_pin: string("schema_pin")?,
        started_at: time("started_at")?,
        finished_at: time("finished_at")?,
        unanswered,
    })
}

/// A present side file covers every keyed call. Its carrier is per call, since a history may
/// have moved between runners; the transcript's host alone cannot identify the minting runner.
pub fn apply_calls(history: &mut HeadHistory, rows: &[CallRow]) -> Result<(), HeadError> {
    let mut by_call = HashMap::new();
    for row in rows {
        if by_call
            .insert((row.mid.as_str(), row.tool_call_id.as_str()), row)
            .is_some()
        {
            return Err(HeadError::Calls(format!(
                "duplicate call ({}, {})",
                row.mid, row.tool_call_id
            )));
        }
    }
    let calls: HashSet<(&str, &str)> = history
        .messages
        .iter()
        .flat_map(|message| {
            message.calls.iter().map(move |call| {
                (
                    message.mid.as_deref().unwrap_or_default(),
                    call.source_tool_call_id.as_str(),
                )
            })
        })
        .collect();
    for row in rows {
        if !calls.contains(&(row.mid.as_str(), row.tool_call_id.as_str())) {
            return Err(HeadError::Calls(format!(
                "call ({}, {}) with key {} matches no call in the export",
                row.mid, row.tool_call_id, row.call_key
            )));
        }
    }
    for call in calls {
        if !by_call.contains_key(&call) {
            return Err(HeadError::Calls(format!(
                "call ({}, {}) has no row in the calls side file",
                call.0, call.1
            )));
        }
    }
    for message in &mut history.messages {
        for call in &mut message.calls {
            let row = by_call[&(
                message.mid.as_deref().unwrap_or_default(),
                call.source_tool_call_id.as_str(),
            )];
            call.call_key = row.call_key.clone();
            call.carrier = row.carrier().to_owned();
            call.schema_pin = row.schema_pin.clone();
            call.started_at = row.started_at;
            call.finished_at = row.finished_at;
            call.unanswered = row.unanswered.clone();
        }
    }
    Ok(())
}
