//! An OpenCode 2 head's history, from the session export Thalamus's `session.export` writes.
//!
//! The file is OpenCode 2's `/api/experimental/session/{id}/export` answer without its `data`
//! wrapper: exactly `{"info": <session>, "messages": [<rows>]}`, the rows in the order the export
//! returns them, each with its parts inside it. Each row becomes the messages OpenCode 2 sends its
//! model for it, as llm-runner's `opencode_v2` serializer profile projects a row
//! (`serializer_profile/opencode_v2.rs`, `projected_content`) and Thalamus's runner maps it to
//! entries (`thalamus-harness-module/src/neutral/opencode2.rs`, `entry`):
//!
//! | Row type | Becomes |
//! |---|---|
//! | `user` | a user message: each skill's text, the row's text, then its files |
//! | `synthetic`, `skill` | a user message with the row's text |
//! | foreground `shell` | a user message with OpenCode's shell text |
//! | `location-switched` | a user message with OpenCode's working-directory text |
//! | `assistant` | an assistant message; its tool calls' results follow as a tool message |
//! | `system` | nothing; counted as a dropped system message |
//! | `model-switched` | nothing |
//! | `agent-switched`, `idle`, background `shell` | nothing, and no move-list row: OpenCode never sends them and Thalamus serves no entry for them |
//! | `compaction` that only marks a runner's view | nothing, and no move-list row |
//! | any other row, or a completed compaction of OpenCode's own | refused by name |

use std::collections::HashSet;

use broca_protocol::{
    ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolOutputPart, ToolResult,
};
use serde_json::Value;

use super::{
    base64_bytes, pair_calls, Draft, HeadError, HeadHistory, HeadMedium, HeadUsage, MediaCheck,
    OpenCalls, SourceRow, OPENCODE2_CARRIER,
};

/// The rows of an export file: its `messages`, after checking the file's top-level shape.
pub fn export_rows(bytes: &[u8]) -> Result<Vec<Value>, HeadError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| HeadError::Unreadable(format!("not JSON: {error}")))?;
    let Value::Object(mut top) = value else {
        return Err(HeadError::Unreadable(
            "an OpenCode 2 export is an object with `info` and `messages`".into(),
        ));
    };
    let mut keys: Vec<&str> = top.keys().map(String::as_str).collect();
    keys.sort_unstable();
    if keys != ["info", "messages"] {
        return Err(HeadError::Unreadable(format!(
            "an OpenCode 2 export holds exactly `info` and `messages`, not {keys:?}"
        )));
    }
    match top.remove("messages") {
        Some(Value::Array(rows)) => Ok(rows),
        _ => Err(HeadError::Unreadable(
            "an OpenCode 2 export's `messages` is not an array".into(),
        )),
    }
}

/// Reads an OpenCode 2 export file's bytes as a head's history. `check` validates each
/// medium's bytes.
pub fn read(bytes: &[u8], check: &mut MediaCheck<'_>) -> Result<HeadHistory, HeadError> {
    let rows = export_rows(bytes)?;
    let mut drafts = Vec::new();
    let mut moves = Vec::new();
    let mut dropped_system_messages = 0;
    let mut ids = HashSet::new();
    for (index, row) in rows.iter().enumerate() {
        let mid = row["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| HeadError::row(&format!("#{index}"), "the row has no id"))?
            .to_owned();
        if !ids.insert(mid.clone()) {
            return Err(HeadError::row(&mid, "the id repeats an earlier row's"));
        }
        let kind = row["type"]
            .as_str()
            .ok_or_else(|| HeadError::row(&mid, "the row has no type"))?;
        let first = drafts.len();
        match kind {
            "user" => drafts.extend(user(&mid, row, check)?),
            "synthetic" | "skill" => {
                drafts.extend(text_row(&mid, row, text_of(&mid, &row["text"])?)?)
            }
            "shell" if row["metadata"]["background"] == true => continue,
            "shell" => drafts.extend(text_row(&mid, row, Some(shell_text(&mid, row)?))?),
            "assistant" => drafts.extend(assistant(&mid, row, check)?),
            "system" => dropped_system_messages += 1,
            // OpenCode tells its model about a change of working directory in a user message
            // (llm-runner's `opencode_v2::projected_content`; Thalamus's `neutral/opencode2.rs`
            // serves it as a user entry).
            "location-switched" => {
                let directory = row["location"]["directory"].as_str().ok_or_else(|| {
                    HeadError::row(&mid, "the location switch names no directory")
                })?;
                drafts.extend(text_row(
                    &mid,
                    row,
                    Some(format!(
                        "The working directory has been changed to {directory}."
                    )),
                )?)
            }
            // Rows Thalamus serves as entries with nothing for the model.
            "model-switched" => {}
            // Rows that are no entry at all: no move-list row either.
            "agent-switched" | "idle" => continue,
            "compaction" => {
                compaction(&mid, row)?;
                continue;
            }
            other => {
                return Err(HeadError::row(
                    &mid,
                    format!("row type `{other}` is not one OpenCode 2 sends its model"),
                ))
            }
        }
        moves.push(SourceRow {
            mid,
            message: (drafts.len() > first).then_some(first),
            finished_summary: false,
        });
    }
    // OpenCode 2 sends no call without its result, so a call left without one means the head
    // wasn't idle.
    let paired = pair_calls(drafts, OPENCODE2_CARRIER, OpenCalls::Refuse)?;
    for row in &mut moves {
        row.message = row.message.and_then(|draft| paired.at[draft]);
    }
    Ok(HeadHistory {
        messages: paired.messages,
        rows: moves,
        dropped_system_messages,
    })
}

/// A compaction row is no message. One that only marks where a runner's own view cuts
/// (`metadata.keepID` naming the row itself) changes nothing the model sees. A completed one
/// of OpenCode's own replaces the history before it with a summary, which the import doesn't
/// map, so it refuses.
fn compaction(mid: &str, row: &Value) -> Result<(), HeadError> {
    let keep = row["metadata"]["keepID"].as_str();
    if row["status"] == "completed" && keep != Some(mid) {
        return Err(HeadError::row(
            mid,
            "OpenCode compacted the session itself, which the import doesn't map",
        ));
    }
    Ok(())
}

/// The row's own creation time.
fn created(mid: &str, row: &Value) -> Result<Option<u64>, HeadError> {
    match &row["time"]["created"] {
        Value::Null => Ok(None),
        value => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| HeadError::row(mid, "`time.created` is not a time in milliseconds")),
    }
}

fn draft(mid: &str, row: &Value, message: Message) -> Result<Draft, HeadError> {
    let at = created(mid, row)?;
    Ok(Draft {
        mid: Some(mid.to_owned()),
        message,
        written_at_ms: at,
        created_at_ms: at,
        usage: None,
        media: Vec::new(),
        keys: Vec::new(),
        row: mid.to_owned(),
        boundary: false,
    })
}

/// A text value as OpenCode 2 stores it: a string. A `text_ref` names text stored elsewhere,
/// which the export doesn't carry.
fn text_of(mid: &str, value: &Value) -> Result<Option<String>, HeadError> {
    match value {
        Value::Null => Ok(None),
        Value::String(text) => Ok(Some(text.clone())),
        _ => Err(HeadError::row(
            mid,
            "a text is stored by reference, which the export doesn't carry",
        )),
    }
}

/// A user message of one text, or none when the text is empty.
fn text_row(mid: &str, row: &Value, text: Option<String>) -> Result<Option<Draft>, HeadError> {
    match text.filter(|text| !text.is_empty()) {
        Some(text) => Ok(Some(draft(
            mid,
            row,
            Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }]),
        )?)),
        None => Ok(None),
    }
}

/// The text OpenCode sends for a shell command the person ran (llm-runner's
/// `opencode_v2::shell_text`).
fn shell_text(mid: &str, row: &Value) -> Result<String, HeadError> {
    let command = row["command"]
        .as_str()
        .ok_or_else(|| HeadError::row(mid, "the shell row has no command"))?;
    let output = row["output"]["output"].as_str().unwrap_or("");
    Ok(format!(
        "The following shell command was executed by the user:\n\nCommand:\n{command}\n\nOutput:\n{output}"
    ))
}

fn array<'a>(mid: &str, value: &'a Value, name: &str) -> Result<&'a [Value], HeadError> {
    match value {
        Value::Null => Ok(&[]),
        Value::Array(items) => Ok(items),
        _ => Err(HeadError::row(mid, format!("`{name}` is not an array"))),
    }
}

/// A user row: each skill's text, then the row's text, then its files, as OpenCode sends them.
/// A text or directory attachment is sent as text, an image or PDF as media, and a file of any
/// other type isn't sent at all.
fn user(mid: &str, row: &Value, check: &mut MediaCheck<'_>) -> Result<Option<Draft>, HeadError> {
    let mut content = Vec::new();
    let mut media = Vec::new();
    for skill in array(mid, &row["skills"], "skills")? {
        if let Some(text) = text_of(mid, &skill["text"])? {
            content.push(ContentBlock::Text { text: text.into() });
        }
    }
    if let Some(text) = text_of(mid, &row["text"])?.filter(|text| !text.is_empty()) {
        content.push(ContentBlock::Text { text: text.into() });
    }
    for file in array(mid, &row["files"], "files")? {
        let mime = file["mime"].as_str().unwrap_or_default();
        match mime {
            "text/plain" | "application/x-directory" => content.push(ContentBlock::Text {
                text: attachment_text(mid, file)?.into(),
            }),
            _ if mime.starts_with("image/") || mime == "application/pdf" => {
                let (declared, bytes) = inline_bytes(mid, file)?;
                let reference = check(&bytes, &declared).map_err(|reason| HeadError::Media {
                    mid: mid.to_owned(),
                    reason,
                })?;
                media.push(HeadMedium {
                    block: content.len() as u32,
                    part: None,
                    bytes,
                });
                content.push(ContentBlock::Media(reference));
            }
            _ => {}
        }
    }
    if content.is_empty() {
        return Ok(None);
    }
    let mut out = draft(mid, row, Message::new(Role::User, content))?;
    out.media = media;
    Ok(Some(out))
}

/// The text OpenCode sends for a text or directory attachment (`textAttachment` and
/// `directoryAttachment` in OpenCode's `to-llm-message.ts`, as Thalamus's runner maps them in
/// `neutral/opencode2.rs`, `attachment_text`).
fn attachment_text(mid: &str, file: &Value) -> Result<String, HeadError> {
    let directory = file["mime"] == "application/x-directory";
    let uri = file["source"]["uri"].as_str();
    let name = file["name"].as_str();
    let data = file["data"].as_str().unwrap_or_default();
    let mut lines = vec![if directory {
        format!(
            "Attached directory: {}",
            name.or(uri).unwrap_or("directory")
        )
    } else {
        format!(
            "Attached file: {}",
            name.or(uri).unwrap_or("inline attachment")
        )
    }];
    if let Some(description) = file["description"].as_str() {
        lines.push(format!("Description: {description}"));
    }
    if !(directory && data.is_empty()) {
        let bytes = base64_bytes(data)
            .map_err(|reason| HeadError::row(mid, format!("an attached text file: {reason}")))?;
        lines.push(String::new());
        lines.push(String::from_utf8_lossy(&bytes).into_owned());
    }
    Ok(format!("\n\n{}", lines.join("\n")))
}

/// The bytes a file or media block carries inline, and its declared type: a user row's file
/// keeps them as base64 `data`, a tool result's file as a base64 `data:` URL in `uri`
/// (Thalamus's `entry_codec/opencode2.rs`, `inline_media`). Any other `uri` names a file stored
/// elsewhere, which the export doesn't carry.
fn inline_bytes(mid: &str, file: &Value) -> Result<(String, Vec<u8>), HeadError> {
    let declared = file["mime"].as_str().or_else(|| file["mediaType"].as_str());
    let refuse = |reason: String| HeadError::Media {
        mid: mid.to_owned(),
        reason,
    };
    if let Some(data) = file["data"].as_str() {
        let bytes = base64_bytes(data).map_err(refuse)?;
        let declared = declared.ok_or_else(|| refuse("the file declares no type".into()))?;
        return Ok((declared.to_owned(), bytes));
    }
    let uri = file["uri"]
        .as_str()
        .ok_or_else(|| refuse("the file carries no bytes".into()))?;
    let (url_mime, bytes) = data_url(uri).ok_or_else(|| {
        refuse("the file is stored by reference, which the export doesn't carry".into())
    })?;
    Ok((declared.unwrap_or(&url_mime).to_owned(), bytes))
}

/// The type and bytes of a base64 `data:` URL.
fn data_url(uri: &str) -> Option<(String, Vec<u8>)> {
    let rest = uri.strip_prefix("data:")?;
    let (meta, data) = rest.split_once(',')?;
    let mime = meta.strip_suffix(";base64")?;
    Some((mime.to_owned(), base64_bytes(data).ok()?))
}

/// An assistant row: its text, reasoning and tool calls, then one tool message with the calls'
/// results. Reasoning keeps its text; the provider's payload in `native` is not imported. A
/// call OpenCode hadn't finished leaves the head busy, and refuses the import.
fn assistant(mid: &str, row: &Value, check: &mut MediaCheck<'_>) -> Result<Vec<Draft>, HeadError> {
    let mut content = Vec::new();
    let mut keys = Vec::new();
    let mut results = Vec::new();
    let mut media = Vec::new();
    for item in array(mid, &row["content"], "content")? {
        match item["type"].as_str() {
            Some("text") => {
                if let Some(text) = text_of(mid, &item["text"])?.filter(|text| !text.is_empty()) {
                    content.push(ContentBlock::Text { text: text.into() });
                }
            }
            Some("reasoning") => {
                if let Some(text) = text_of(mid, &item["text"])?.filter(|text| !text.is_empty()) {
                    content.push(ContentBlock::Reasoning {
                        text: text.into(),
                        signature: None,
                    });
                }
            }
            Some("tool") => {
                let (call, key, result, result_media) =
                    tool(mid, item, results.len() as u32, check)?;
                content.push(ContentBlock::ToolCall(call));
                keys.push(Some(key));
                results.push(ContentBlock::ToolResult(result));
                media.extend(result_media);
            }
            other => {
                return Err(HeadError::row(
                    mid,
                    format!("an assistant item of type {other:?} is not text, reasoning or a tool"),
                ))
            }
        }
    }
    if content.is_empty() {
        return Ok(Vec::new());
    }
    let mut message = draft(mid, row, Message::new(Role::Assistant, content))?;
    message.usage = usage(&row["tokens"]);
    message.keys = keys;
    let mut out = vec![message];
    if !results.is_empty() {
        let completed = match &row["time"]["completed"] {
            Value::Null => None,
            value => Some(value.as_u64().ok_or_else(|| {
                HeadError::row(mid, "`time.completed` is not a time in milliseconds")
            })?),
        };
        out.push(Draft {
            mid: None,
            message: Message::new(Role::Tool, results),
            written_at_ms: completed,
            created_at_ms: None,
            usage: None,
            media,
            keys: Vec::new(),
            row: mid.to_owned(),
            boundary: false,
        });
    }
    Ok(out)
}

/// A tool item: its call, the key Thalamus's runner gave it (`<message id>/<call id>`, its
/// `call_identity.rs`, `opencode2_key`), and its result. `block` is the result's position in
/// the tool message.
fn tool(
    mid: &str,
    item: &Value,
    block: u32,
    check: &mut MediaCheck<'_>,
) -> Result<(ToolCall, String, ToolResult, Vec<HeadMedium>), HeadError> {
    let call_id = item["id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| HeadError::row(mid, "a tool item has no call id"))?
        .to_owned();
    if item["executed"] == true {
        return Err(HeadError::row(
            mid,
            format!("call {call_id} was run by the model's provider, which the import doesn't map"),
        ));
    }
    let state = &item["state"];
    let input = match &state["input"] {
        Value::String(text) => {
            serde_json::from_str(text).unwrap_or_else(|_| state["input"].clone())
        }
        input => input.clone(),
    };
    let name = item["name"]
        .as_str()
        .ok_or_else(|| HeadError::row(mid, format!("call {call_id} names no tool")))?;
    let call = ToolCall {
        tool_call_id: ToolCallId(call_id.clone()),
        tool_name: name.to_owned(),
        input: input.into(),
        thought_signature: None,
        provider_state: None,
    };
    let mut media = Vec::new();
    let (output, is_error) = match state["status"].as_str() {
        Some("completed") => {
            let blocks = array(mid, &state["content"], "state.content")?;
            let mut parts = Vec::new();
            for content in blocks {
                if content["type"] == "text" {
                    let text = text_of(mid, &content["text"])?.unwrap_or_default();
                    parts.push(ToolOutputPart::Text { text: text.into() });
                    continue;
                }
                let (declared, bytes) = inline_bytes(mid, content)?;
                let reference = check(&bytes, &declared).map_err(|reason| HeadError::Media {
                    mid: mid.to_owned(),
                    reason,
                })?;
                media.push(HeadMedium {
                    block,
                    part: Some(parts.len() as u32),
                    bytes,
                });
                parts.push(ToolOutputPart::Media { media: reference });
            }
            let output = if media.is_empty() {
                ToolOutput::Text {
                    text: parts
                        .into_iter()
                        .filter_map(|part| match part {
                            ToolOutputPart::Text { text } => Some(text),
                            ToolOutputPart::Media { .. } => None,
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                        .into(),
                }
            } else {
                ToolOutput::Parts { parts }
            };
            (output, false)
        }
        Some("error") => {
            let error = &state["error"];
            let text = error["message"]
                .as_str()
                .or_else(|| error.as_str())
                .map_or_else(|| error.to_string(), str::to_owned);
            (ToolOutput::error(text), true)
        }
        _ => {
            return Err(HeadError::OpenCall {
                mid: mid.to_owned(),
                call_id,
            })
        }
    };
    let result = ToolResult {
        tool_call_id: ToolCallId(call_id.clone()),
        output,
        is_error,
    };
    Ok((call, format!("{mid}/{call_id}"), result, media))
}

/// The step usage Thalamus's runner serves for an assistant row (`neutral/opencode2.rs`,
/// `usage`). OpenCode stores 0 for a count the provider didn't report, so a 0 is unknown; the
/// prompt counts input and both cache counts, and the output counts reasoning.
fn usage(tokens: &Value) -> Option<HeadUsage> {
    let read = |value: &Value| value.as_u64().unwrap_or(0);
    let known = |n: u64| Some(n).filter(|&n| n > 0);
    let (input, cache_read, cache_write) = (
        read(&tokens["input"]),
        read(&tokens["cache"]["read"]),
        read(&tokens["cache"]["write"]),
    );
    let (output, reasoning) = (read(&tokens["output"]), read(&tokens["reasoning"]));
    let usage = HeadUsage {
        prompt_tokens: known(input + cache_read + cache_write),
        cache_read_tokens: known(cache_read),
        cache_write_tokens: known(cache_write),
        output_tokens: known(output + reasoning),
        reasoning_tokens: known(reasoning),
    };
    (usage != HeadUsage::default()).then_some(usage)
}
