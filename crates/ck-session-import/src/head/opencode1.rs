//! OpenCode 1's per-row model projection, from an extracted SQLite session.
//!
//! Authority: OpenCode v1.19.4, commit `148842ef7cb1bc34192b3fff57d0f9721ab9c801`,
//! `session/message-v2.ts`, `toModelMessagesEffect`. This preserves the whole history, not the
//! host's current compaction window. Provider-specific media relocation, output truncation and
//! reasoning replay belong to the destination renderer, not to stored history.
//!
//! | Source row/part | Canonical projection |
//! |---|---|
//! | user text | nonempty, non-`ignored` text (including synthetic text) |
//! | user file | media, except `text/plain` and `application/x-directory` (already text parts) |
//! | user compaction / subtask | `What did we do so far?` / `The following tool was executed by the user` |
//! | assistant with error | omitted, except `MessageAbortedError` with a part other than step-start/reasoning |
//! | assistant text / reasoning | text / reasoning text, without any provider payload |
//! | completed tool | call and result; compacted output is `[Old tool result content cleared]`, without attachments |
//! | errored tool | error result, except interrupted metadata's string output is a successful result |
//! | provider-executed tool | refused: the canonical import cannot represent provider-owned execution |
//! | pending/running tool | refused: the held head must be idle (stricter than the host's interruption placeholder) |
//! | system | omitted and counted |
//! | step markers, snapshots, patches, other non-model parts | omitted |
//!
//! The signed-reasoning empty-text separator and interrupted-error output are version-sensitive
//! v1.19.4 rules. Empty assistant text next to signed reasoning becomes a single space. Media is
//! retained as objects and reasoning as reasoning even when the source provider would relocate
//! media or turn reasoning into text on a model switch. No file URL is opened by the reader.

use std::path::Path;

use broca_protocol::{
    ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolOutputPart, ToolResult,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde_json::Value;

use super::{
    base64_bytes, pair_calls, Draft, HeadError, HeadHistory, HeadMedium, MediaCheck, OpenCalls,
    SourceRow,
};

/// Reads only a digest-checked, standalone extract: immutable mode never follows a host WAL.
pub fn read(
    path: &Path,
    session: &str,
    check: &mut MediaCheck<'_>,
) -> Result<HeadHistory, HeadError> {
    let unreadable = |error: &dyn std::fmt::Display| HeadError::Unreadable(error.to_string());
    let connection = Connection::open_with_flags(
        crate::sqlite_read::immutable_uri(path),
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| unreadable(&error))?;
    if connection
        .query_row("SELECT 1 FROM session WHERE id=?1", [session], |_| Ok(()))
        .optional()
        .map_err(|error| unreadable(&error))?
        .is_none()
    {
        return Err(HeadError::Unreadable(format!(
            "OpenCode session {session:?} was not found"
        )));
    }
    let rows =
        crate::opencode::load_messages(&connection, session).map_err(|error| unreadable(&error))?;
    let mut parts =
        crate::opencode::load_parts(&connection, session).map_err(|error| unreadable(&error))?;
    let mut drafts = Vec::new();
    let mut moves = Vec::new();
    let mut dropped_system_messages = 0;
    let mut ids = std::collections::HashSet::new();
    for (mid, data, time) in rows {
        if mid.is_empty() || !ids.insert(mid.clone()) {
            return Err(HeadError::row(
                &mid,
                "message id is empty or repeats an earlier row",
            ));
        }
        let row: Value =
            serde_json::from_str(&data).map_err(|error| HeadError::row(&mid, error.to_string()))?;
        let items = parts
            .remove(&mid)
            .unwrap_or_default()
            .into_iter()
            .map(|part| {
                serde_json::from_str::<Value>(&part)
                    .map_err(|error| HeadError::row(&mid, error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let time =
            u64::try_from(time).map_err(|_| HeadError::row(&mid, "time_created is negative"))?;
        let first = drafts.len();
        let finished_summary =
            row["role"] == "assistant" && row["summary"] == true && row["finish"] == "stop";
        match row["role"].as_str() {
            Some("user") => drafts.extend(user(&mid, time, &items, check)?),
            Some("assistant") => drafts.extend(assistant(&mid, time, &row, &items, check)?),
            Some("system") => dropped_system_messages += 1,
            role => {
                return Err(HeadError::row(
                    &mid,
                    format!("unsupported OpenCode role {role:?}"),
                ))
            }
        }
        if drafts.len() == first {
            drafts.push(Draft::boundary(&mid));
        }
        moves.push(SourceRow {
            mid,
            message: Some(first),
            finished_summary,
        });
    }
    // The standalone host minted no runner call keys. Temporary pairing keys let the shared
    // finalizer check real results and rename duplicate ids; none escape into call facts.
    let mut paired = pair_calls(drafts, "", OpenCalls::Refuse)?;
    for message in &mut paired.messages {
        message.calls.clear();
    }
    for row in &mut moves {
        row.message = row.message.and_then(|index| paired.at[index]);
    }
    Ok(HeadHistory {
        messages: paired.messages,
        rows: moves,
        dropped_system_messages,
    })
}

fn draft(mid: &str, time: u64, role: Role, content: Vec<ContentBlock>) -> Draft {
    Draft {
        mid: Some(mid.into()),
        message: Message::new(role, content),
        written_at_ms: Some(time),
        created_at_ms: Some(time),
        usage: None,
        media: Vec::new(),
        keys: Vec::new(),
        row: mid.into(),
        boundary: false,
    }
}

fn text(mid: &str, value: &Value, field: &str) -> Result<String, HeadError> {
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| HeadError::row(mid, format!("{field} is not text")))
}

fn medium(
    mid: &str,
    file: &Value,
    block: u32,
    part: Option<u32>,
    check: &mut MediaCheck<'_>,
) -> Result<(broca_protocol::MediaRef, HeadMedium), HeadError> {
    let fail = |reason: String| HeadError::Media {
        mid: mid.into(),
        reason,
    };
    let mime = file["mime"]
        .as_str()
        .ok_or_else(|| fail("file has no MIME type".into()))?;
    let url = file["url"]
        .as_str()
        .ok_or_else(|| fail("file has no inline URL".into()))?;
    let (header, data) = url
        .split_once(',')
        .ok_or_else(|| fail("file is not an inline data URL".into()))?;
    if header != format!("data:{mime};base64") {
        return Err(fail(format!(
            "file must be inline base64 with declared type {mime}"
        )));
    }
    let bytes = base64_bytes(data).map_err(fail)?;
    let reference = check(&bytes, mime).map_err(fail)?;
    Ok((reference, HeadMedium { block, part, bytes }))
}

fn user(
    mid: &str,
    time: u64,
    items: &[Value],
    check: &mut MediaCheck<'_>,
) -> Result<Option<Draft>, HeadError> {
    let mut content = Vec::new();
    let mut media = Vec::new();
    for item in items {
        match item["type"].as_str() {
            Some("text") if item["ignored"] != true => {
                let text = text(mid, &item["text"], "user text")?;
                if !text.is_empty() {
                    content.push(ContentBlock::Text { text });
                }
            }
            Some("file")
                if item["mime"] != "text/plain" && item["mime"] != "application/x-directory" =>
            {
                let (reference, bytes) = medium(mid, item, content.len() as u32, None, check)?;
                content.push(ContentBlock::Media(reference));
                media.push(bytes);
            }
            Some("compaction") => content.push(ContentBlock::Text {
                text: "What did we do so far?".into(),
            }),
            Some("subtask") => content.push(ContentBlock::Text {
                text: "The following tool was executed by the user".into(),
            }),
            _ => {}
        }
    }
    if content.is_empty() {
        return Ok(None);
    }
    let mut out = draft(mid, time, Role::User, content);
    out.media = media;
    Ok(Some(out))
}

fn assistant(
    mid: &str,
    time: u64,
    row: &Value,
    items: &[Value],
    check: &mut MediaCheck<'_>,
) -> Result<Vec<Draft>, HeadError> {
    if !row["error"].is_null()
        && !(row["error"]["name"] == "MessageAbortedError"
            && items
                .iter()
                .any(|item| item["type"] != "step-start" && item["type"] != "reasoning"))
    {
        return Ok(Vec::new());
    }
    let signed = items.iter().any(|item| {
        item["type"] == "reasoning" && !item["metadata"]["anthropic"]["signature"].is_null()
    });
    let mut content = Vec::new();
    let mut results = Vec::new();
    let mut media = Vec::new();
    let mut keys = Vec::new();
    for item in items {
        match item["type"].as_str() {
            Some("text") => {
                let mut text = text(mid, &item["text"], "assistant text")?;
                if text.is_empty() && signed {
                    text = " ".into();
                }
                if !text.is_empty() {
                    content.push(ContentBlock::Text { text });
                }
            }
            Some("reasoning") => {
                let text = text(mid, &item["text"], "reasoning text")?;
                if !text.is_empty() {
                    content.push(ContentBlock::Reasoning {
                        text,
                        signature: None,
                    });
                }
            }
            Some("tool") => {
                let id = text(mid, &item["callID"], "tool callID")?;
                if item["metadata"]["providerExecuted"] == true {
                    return Err(HeadError::row(
                        mid,
                        format!("call {id} was executed by the provider"),
                    ));
                }
                let state = &item["state"];
                let (output, is_error) = match state["status"].as_str() {
                    Some("completed") => {
                        let compacted = state["time"]["compacted"].as_u64().is_some_and(|n| n > 0);
                        let output = if compacted {
                            "[Old tool result content cleared]".into()
                        } else {
                            text(mid, &state["output"], "tool output")?
                        };
                        let mut parts = vec![ToolOutputPart::Text {
                            text: output.clone(),
                        }];
                        if !compacted {
                            if let Some(attachments) = state["attachments"].as_array() {
                                for file in attachments {
                                    let (reference, bytes) = medium(
                                        mid,
                                        file,
                                        results.len() as u32,
                                        Some(parts.len() as u32),
                                        check,
                                    )?;
                                    parts.push(ToolOutputPart::Media { media: reference });
                                    media.push(bytes);
                                }
                            }
                        }
                        (
                            if parts.len() > 1 {
                                ToolOutput::Parts { parts }
                            } else {
                                ToolOutput::Text { text: output }
                            },
                            false,
                        )
                    }
                    Some("error") => {
                        if state["metadata"]["interrupted"] == true
                            && state["metadata"]["output"].is_string()
                        {
                            (
                                ToolOutput::Text {
                                    text: text(
                                        mid,
                                        &state["metadata"]["output"],
                                        "interrupted output",
                                    )?,
                                },
                                false,
                            )
                        } else {
                            (
                                ToolOutput::error(text(mid, &state["error"], "tool error")?),
                                true,
                            )
                        }
                    }
                    _ => {
                        return Err(HeadError::OpenCall {
                            mid: mid.into(),
                            call_id: id,
                        })
                    }
                };
                content.push(ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId(id.clone()),
                    tool_name: text(mid, &item["tool"], "tool name")?,
                    input: state["input"].clone(),
                    thought_signature: None,
                }));
                keys.push(Some(format!("{mid}/{id}")));
                results.push(ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId(id),
                    output,
                    is_error,
                }));
            }
            _ => {}
        }
    }
    if content.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = draft(mid, time, Role::Assistant, content);
    out.keys = keys;
    let mut drafts = vec![out];
    if !results.is_empty() {
        let mut out = draft(mid, time, Role::Tool, results);
        out.mid = None;
        out.created_at_ms = None;
        out.media = media;
        drafts.push(out);
    }
    Ok(drafts)
}
