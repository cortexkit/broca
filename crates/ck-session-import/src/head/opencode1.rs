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
//! | pending/running tool in an older row | call with recorded input (or `{}`), and the host's error result `[Tool execution was interrupted]` |
//! | pending/running tool in the last source message row | refused: the head may still be working |
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

mod audit;
pub use audit::{check, check_with_history, Counts, Refusal, ToolResults};

const INTERRUPTED: &str = "[Tool execution was interrupted]";

/// Reads only a digest-checked, standalone extract: immutable mode never follows a host WAL.
pub fn read(
    path: &Path,
    session: &str,
    check: &mut MediaCheck<'_>,
) -> Result<HeadHistory, HeadError> {
    read_audited(path, session, check, &mut audit::Audit::default())
}

fn read_audited(
    path: &Path,
    session: &str,
    check: &mut MediaCheck<'_>,
    audit: &mut audit::Audit,
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
    let row_count = rows.len();
    for (index, (mid, data, time)) in rows.into_iter().enumerate() {
        let repeated = audit
            .ordinals
            .insert(mid.clone(), index as u64 + 1)
            .is_some();
        if mid.is_empty() || repeated {
            return Err(HeadError::row(
                &mid,
                "message id is empty or repeats an earlier row",
            ));
        }
        let row: Value =
            serde_json::from_str(&data).map_err(|error| HeadError::row(&mid, error.to_string()))?;
        let role = match row["role"].as_str() {
            Some("user") => "user",
            Some("assistant") => "assistant",
            Some("system") => "system",
            _ => "other",
        };
        *audit.counts.source_rows_by_role.entry(role).or_default() += 1;
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
        // SQLite's json_extract represents true as 1; the consumer's query also accepts a
        // numeric 1. Objects, strings and every other value are not a finished summary.
        let finished_summary = (row["summary"] == true || row["summary"].as_f64() == Some(1.0))
            && row["finish"] == "stop";
        // A crash can leave old tool parts unfinished forever. Only the last source row can
        // still be live, even when that row's other parts would project nothing.
        if index + 1 == row_count {
            if let Some(item) = items.iter().find(|item| {
                item["type"] == "tool"
                    && matches!(
                        item["state"]["status"].as_str(),
                        Some("pending" | "running")
                    )
            }) {
                return Err(HeadError::OpenCall {
                    mid: mid.clone(),
                    call_id: text(&mid, &item["callID"], "tool callID")?,
                });
            }
        }
        match row["role"].as_str() {
            Some("user") => drafts.extend(user(&mid, time, &items, check)?),
            Some("assistant") => drafts.extend(assistant(
                &mid,
                time,
                &row,
                &items,
                check,
                &mut audit.counts.tool_results,
            )?),
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
                    content.push(ContentBlock::Text { text: text.into() });
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
    counts: &mut ToolResults,
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
                    content.push(ContentBlock::Text { text: text.into() });
                }
            }
            Some("reasoning") => {
                let text = text(mid, &item["text"], "reasoning text")?;
                if !text.is_empty() {
                    content.push(ContentBlock::Reasoning {
                        text: text.into(),
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
                        counts.completed += 1;
                        let compacted = state["time"]["compacted"].as_u64().is_some_and(|n| n > 0);
                        let output = if compacted {
                            "[Old tool result content cleared]".into()
                        } else {
                            text(mid, &state["output"], "tool output")?
                        };
                        let mut parts = vec![ToolOutputPart::Text {
                            text: output.clone().into(),
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
                                ToolOutput::Text {
                                    text: output.into(),
                                }
                            },
                            false,
                        )
                    }
                    Some("error") => {
                        if state["metadata"]["interrupted"] == true
                            && state["metadata"]["output"].is_string()
                        {
                            counts.interrupted_as_result += 1;
                            (
                                ToolOutput::Text {
                                    text: text(
                                        mid,
                                        &state["metadata"]["output"],
                                        "interrupted output",
                                    )?
                                    .into(),
                                },
                                false,
                            )
                        } else {
                            counts.error += 1;
                            (
                                ToolOutput::error(text(mid, &state["error"], "tool error")?),
                                true,
                            )
                        }
                    }
                    Some("pending" | "running") => {
                        counts.open_call_placeholders += 1;
                        (ToolOutput::error(INTERRUPTED), true)
                    }
                    other => {
                        return Err(HeadError::row(
                            mid,
                            format!("call {id} has unknown tool status {other:?}"),
                        ))
                    }
                };
                content.push(ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId(id.clone()),
                    tool_name: text(mid, &item["tool"], "tool name")?,
                    input: if matches!(state["status"].as_str(), Some("pending" | "running"))
                        && state["input"].is_null()
                    {
                        serde_json::json!({}).into()
                    } else {
                        state["input"].clone().into()
                    },
                    thought_signature: None,
                    provider_state: None,
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
