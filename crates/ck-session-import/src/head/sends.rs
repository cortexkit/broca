//! Thalamus's side file: what its runner recorded about each send it placed, which no host's
//! transcript holds. `session.export` writes it beside the export as `<name>.sends.json`: a
//! JSON array of rows `{mid | call_key, send_id, client_message_id?, sender, marks?,
//! delivered_as}`. A send that rode on a tool result is listed by the call's key instead of a
//! message id.

use std::path::{Path, PathBuf};

use serde_json::Value;

use super::{HeadError, HeadHistory, SendFacts};
use broca_protocol::{ContentBlock, Role};

/// The side file's path beside a source file: the source's name with its extension replaced by
/// `sends.json` (`head.json` → `head.sends.json`).
pub fn sends_path(source: &Path) -> PathBuf {
    source.with_extension("sends.json")
}

/// One row of the side file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendRow {
    /// The message the send placed, or for a send that rode on a tool result, the call's key.
    pub target: SendTarget,
    pub send_id: String,
    pub facts: SendFacts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendTarget {
    Message(String),
    Call(String),
}

const KEYS: [&str; 9] = [
    "mid",
    "call_key",
    "send_id",
    "client_message_id",
    "sender",
    "marks",
    "delivered_as",
    "template",
    "text",
];

/// Reads a side file's bytes. Every member is checked; a member or value the import can't map
/// refuses the file by name.
pub fn read_sends(bytes: &[u8]) -> Result<Vec<SendRow>, HeadError> {
    let refuse = |reason: String| HeadError::Sends(reason);
    let value: Value =
        serde_json::from_slice(bytes).map_err(|error| refuse(format!("not JSON: {error}")))?;
    let Value::Array(rows) = value else {
        return Err(refuse("the side file is not a JSON array".into()));
    };
    rows.iter()
        .enumerate()
        .map(|(index, row)| row_of(row).map_err(|reason| refuse(format!("row {index}: {reason}"))))
        .collect()
}

fn row_of(row: &Value) -> Result<SendRow, String> {
    let object = row.as_object().ok_or("the row is not an object")?;
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
    let send_id = string("send_id")?.ok_or("the row has no `send_id`")?;
    let sender = match object.get("sender") {
        Some(Value::String(sender)) if !sender.is_empty() => sender.clone(),
        Some(Value::String(_)) | None | Some(Value::Null) => {
            return Err("the row has no `sender`".into())
        }
        Some(_) => return Err("`sender` is not a plain module id".into()),
    };
    let delivered_as = string("delivered_as")?.ok_or("the row has no `delivered_as`")?;
    let delivery_mode = match delivered_as.as_str() {
        "steer" | "idle_start" | "tool_result" => Some(delivered_as.clone()),
        // A person's message that waited and then started its own run, with no delivery
        // order: broca serves no delivery facts for such a send of its own.
        "queue" => None,
        other => {
            return Err(format!(
                "`delivered_as` {other:?} is not a delivery the import maps"
            ))
        }
    };
    let target =
        match (string("mid")?, string("call_key")?) {
            (Some(mid), None) if delivered_as != "tool_result" => SendTarget::Message(mid),
            (None, Some(key)) if delivered_as == "tool_result" => SendTarget::Call(key),
            (Some(_), Some(_)) => return Err("the row names both a message and a call".into()),
            (None, None) => return Err("the row names neither a message nor a call".into()),
            _ => return Err(
                "a send that rode on a tool result is named by its call, any other by its message"
                    .into(),
            ),
        };
    let marks = match object.get("marks") {
        None | Some(Value::Null) => None,
        Some(marks) => Some(marks_of(marks)?),
    };
    // A ride's text is what the model saw with the call's result, so a ride without it can't
    // be imported; any other send's text is its message's.
    let text = string("text")?;
    match (&target, &text) {
        (SendTarget::Call(_), None) => {
            return Err("a send that rode on a tool result carries no `text`".into())
        }
        (SendTarget::Message(_), Some(_)) => {
            return Err("only a send that rode on a tool result carries `text`".into())
        }
        _ => {}
    }
    Ok(SendRow {
        target,
        send_id: send_id.clone(),
        facts: SendFacts {
            client_message_id: string("client_message_id")?,
            sender,
            template: template_of(object.get("template"))?,
            marks,
            delivery_mode,
            delivery_tool_call_id: None,
            text,
            send_id,
        },
    })
}

/// The plan template a send was rendered from, by its id: a string, or `{id, slots}`, whose
/// slots were already rendered into the text and aren't part of the notice.
fn template_of(template: Option<&Value>) -> Result<Option<String>, String> {
    match template {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(id)) => Ok(Some(id.clone())),
        Some(Value::Object(object)) => {
            if let Some(key) = object
                .keys()
                .find(|key| !matches!(key.as_str(), "id" | "slots"))
            {
                return Err(format!("`template.{key}` is not one the import maps"));
            }
            object
                .get("id")
                .and_then(Value::as_str)
                .map(|id| Some(id.to_owned()))
                .ok_or_else(|| "`template` has no `id`".into())
        }
        Some(_) => Err("`template` is not a string or `{id, slots}`".into()),
    }
}

/// The marks as llm-runner's `MessageMarks` stores them: `speaks_for {agent_id}` and `notice`,
/// nothing else. A false notice is absent, as stored.
fn marks_of(marks: &Value) -> Result<Value, String> {
    let object = marks.as_object().ok_or("`marks` is not an object")?;
    let mut out = serde_json::Map::new();
    for (key, value) in object {
        match key.as_str() {
            "speaks_for" => {
                let speaks = value
                    .as_object()
                    .filter(|speaks| speaks.len() == 1)
                    .and_then(|speaks| speaks.get("agent_id"))
                    .and_then(Value::as_str)
                    .ok_or("`marks.speaks_for` is not `{agent_id}`")?;
                out.insert(key.clone(), serde_json::json!({ "agent_id": speaks }));
            }
            "notice" => match value {
                Value::Bool(true) => {
                    out.insert(key.clone(), Value::Bool(true));
                }
                Value::Bool(false) => {}
                _ => return Err("`marks.notice` is not a boolean".into()),
            },
            other => return Err(format!("mark `{other}` is not one the import maps")),
        }
    }
    Ok(Value::Object(out))
}

/// Puts each row's facts on the message it names: a send's on the user message it placed, a
/// ride's on the tool message holding the call's result. A row naming a message or call the
/// imported history doesn't hold, or a message that became none, refuses the import, as does a
/// second send for the same message.
pub fn apply_sends(history: &mut HeadHistory, rows: Vec<SendRow>) -> Result<(), HeadError> {
    for row in rows {
        match &row.target {
            SendTarget::Message(mid) => {
                let source = history
                    .rows
                    .iter()
                    .find(|source| &source.mid == mid)
                    .ok_or_else(|| {
                        HeadError::Sends(format!(
                            "send {} names message {mid}, which the imported history doesn't hold",
                            row.send_id
                        ))
                    })?;
                let at = source.message.ok_or_else(|| {
                    HeadError::Sends(format!(
                        "send {} names message {mid}, which the host never sends its model",
                        row.send_id
                    ))
                })?;
                if history.messages[at].message.role != Role::User {
                    return Err(HeadError::Sends(format!(
                        "send {} names message {mid}, which isn't a user message",
                        row.send_id
                    )));
                }
                let message = &mut history.messages[at];
                if message.send.is_some() {
                    return Err(HeadError::Sends(format!(
                        "send {} names message {mid}, which another send already names",
                        row.send_id
                    )));
                }
                message.send = Some(row.facts);
            }
            SendTarget::Call(key) => {
                let call = history
                    .messages
                    .iter()
                    .flat_map(|message| &message.calls)
                    .find(|call| &call.call_key == key)
                    .ok_or_else(|| {
                        HeadError::Sends(format!(
                            "send {} rode on call {key}, which the imported history doesn't hold",
                            row.send_id
                        ))
                    })?;
                let id = call.tool_call_id.clone();
                let at = history
                    .messages
                    .iter()
                    .position(|message| {
                        message.message.content.iter().any(|block| {
                            matches!(block, ContentBlock::ToolResult(result) if result.tool_call_id.0 == id)
                        })
                    })
                    .expect("every imported call has its result");
                let mut facts = row.facts;
                facts.delivery_tool_call_id = Some(id);
                history.messages[at].rides.push(facts);
            }
        }
    }
    Ok(())
}
