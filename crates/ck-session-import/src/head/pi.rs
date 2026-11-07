//! A Pi head's history, from the head's session journal (Pi's session JSONL), as Thalamus's
//! `thalamus-pi` runner serves it.
//!
//! The active path is llm-runner's `pi_session_projection` over the journal's complete lines:
//! the last entry is the leaf, its `parentId` chain back to the root is the path, abandoned
//! branches are left out, and the path's last `context_edit` of each entry replaces or removes
//! its content. That is the rule Thalamus's runner reads its journal by
//! (`thalamus-harness-pi/src/content.rs`, `path` and `walk_until`) and serves rows by
//! (`pi_served_body`). Each entry then becomes what Pi's `convertToLlm` sends the model for it,
//! as the projection's `projected_content` gives it:
//!
//! | Entry | Becomes |
//! |---|---|
//! | `message`, role `user` | a user message: its text and images |
//! | `message`, role `assistant` | an assistant message: text, reasoning text, tool calls |
//! | `message`, role `assistant`, `stopReason` `error` or `aborted` | nothing: Pi never sends it, nor its calls |
//! | `message`, role `toolResult` | a tool message with the call's result |
//! | a call still without a result when the next user or assistant turn arrives, or at the end | a tool message with Pi's error result `No result provided`, under an id broca mints |
//! | `message`, role `bashExecution` | a user message: Pi's shell text and its images; nothing when excluded from context |
//! | `message`, role `system` | nothing; counted as a dropped system message |
//! | `custom_message` | a user message with its content |
//! | `branch_summary` | a user message with Pi's branch-summary text |
//! | an entry a `context_edit` removed | nothing |
//! | a `compaction` on the active path | refused by name |
//! | control entries (model and thinking-level changes, labels, edits) | nothing, and no move-list row |

use std::collections::{HashMap, HashSet};

use broca_protocol::{
    ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolOutputPart, ToolResult,
};
use cortexkit_role_llm_runner::serializer_profile::pi_session_projection;
use serde_json::Value;

use super::{
    base64_bytes, pair_calls, Draft, HeadError, HeadHistory, HeadMedium, HeadUsage, MediaCheck,
    OpenCalls, SourceRow, PI_CARRIER,
};

/// The journal's entries, header first, from its complete lines: a last line without its
/// newline is a write still in progress and is left out, as Thalamus's runner leaves it.
pub fn journal_entries(bytes: &[u8]) -> Result<Vec<Value>, HeadError> {
    let complete = match bytes.iter().rposition(|byte| *byte == b'\n') {
        Some(end) => &bytes[..end],
        None => &[][..],
    };
    let mut entries = Vec::new();
    for (index, line) in complete.split(|byte| *byte == b'\n').enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let entry: Value = serde_json::from_slice(line).map_err(|error| {
            HeadError::Unreadable(format!("line {} isn't a Pi entry: {error}", index + 1))
        })?;
        entries.push(entry);
    }
    Ok(entries)
}

/// Reads a Pi journal's bytes as a head's history. `session` is the session the caller named,
/// which the journal's header must hold; its call keys name it. `check` validates each
/// medium's bytes.
pub fn read(
    bytes: &[u8],
    session: &str,
    check: &mut MediaCheck<'_>,
) -> Result<HeadHistory, HeadError> {
    let entries = journal_entries(bytes)?;
    let header = entries
        .first()
        .filter(|header| header["type"] == "session")
        .ok_or_else(|| HeadError::Unreadable("the journal has no session header".into()))?;
    if header["id"].as_str() != Some(session) {
        return Err(HeadError::Unreadable(format!(
            "the journal is session {:?}, not {session:?}",
            header["id"].as_str().unwrap_or_default()
        )));
    }
    let projection = pi_session_projection(&entries)
        .map_err(|error| HeadError::Unreadable(error.to_string()))?;
    if let Some(cut) = &projection.native_cut {
        return Err(HeadError::row(
            &cut.entry_id,
            "Pi compacted the session itself, which the import doesn't map",
        ));
    }
    let mut drafts = Vec::new();
    let mut moves = Vec::new();
    let mut dropped_system_messages = 0;
    let mut ids = HashSet::new();
    // The calls of turns Pi leaves out, by call id, with the turn that made them.
    let mut skipped_calls: HashMap<String, String> = HashMap::new();
    for entry in &projection.entries {
        let mid = entry.entry_id.clone();
        if !ids.insert(mid.clone()) {
            return Err(HeadError::row(&mid, "the id repeats an earlier entry's"));
        }
        let first = drafts.len();
        let row = &entry.served_body;
        let content = &entry.projected_content;
        let role = match row["type"].as_str() {
            Some("custom_message" | "branch_summary") => "user",
            _ => match row["message"]["role"].as_str() {
                Some("bashExecution") => "user",
                Some(role) => role,
                None => return Err(HeadError::row(&mid, "the message has no role")),
            },
        };
        if !content.is_null() {
            match role {
                "system" => dropped_system_messages += 1,
                "user" => drafts.extend(user(&mid, row, content, check)?),
                // Pi never sends its model an assistant turn that errored or was aborted, nor
                // that turn's calls (pi-ai's `transform-messages.ts`, Pi 1.5.0). The turn still
                // ends the one before it, as any assistant turn does.
                "assistant" if skipped(row) => {
                    for call in blocks(&mid, content)?
                        .into_iter()
                        .filter(|block| block_type(block) == "toolCall")
                    {
                        if let Some(id) = call["id"].as_str() {
                            skipped_calls.insert(id.to_owned(), mid.clone());
                        }
                    }
                    drafts.push(Draft::boundary(&mid));
                }
                "assistant" => drafts.extend(assistant(&mid, row, content, session)?),
                "toolResult" => drafts.push(tool_result(&mid, row, content, check)?),
                other => {
                    return Err(HeadError::row(
                        &mid,
                        format!("role `{other}` is not one Pi sends its model"),
                    ))
                }
            }
            // A user or assistant turn that carries nothing to import is still a turn to Pi:
            // it ends the turn before it.
            if drafts.len() == first && matches!(role, "user" | "assistant") {
                drafts.push(Draft::boundary(&mid));
            }
        }
        moves.push(SourceRow {
            mid,
            message: (drafts.len() > first).then_some(first),
        });
    }
    // Pi closes a call left without a result when the next user or assistant turn arrives, or
    // at the conversation's end, with this error result (pi-ai's `transform-messages.ts`).
    let paired = match pair_calls(drafts, PI_CARRIER, OpenCalls::Close(NO_RESULT)) {
        Ok(paired) => paired,
        Err(HeadError::OrphanResult {
            mid,
            call_id,
            skipped_turn: None,
        }) if skipped_calls.contains_key(&call_id) => {
            return Err(HeadError::OrphanResult {
                mid,
                skipped_turn: skipped_calls.get(&call_id).cloned(),
                call_id,
            })
        }
        Err(error) => return Err(error),
    };
    for row in &mut moves {
        row.message = row.message.and_then(|draft| paired.at[draft]);
    }
    Ok(HeadHistory {
        messages: paired.messages,
        rows: moves,
        dropped_system_messages,
    })
}

/// The text of the result Pi gives a call left without one.
pub const NO_RESULT: &str = "No result provided";

/// Whether Pi leaves an assistant entry out of what it sends: its turn errored or was aborted.
fn skipped(row: &Value) -> bool {
    matches!(
        row["message"]["stopReason"].as_str(),
        Some("error" | "aborted")
    )
}

/// The entry's timestamp in Unix milliseconds. Every Pi entry carries one in RFC 3339.
fn timestamp(mid: &str, row: &Value) -> Result<u64, HeadError> {
    row["timestamp"]
        .as_str()
        .and_then(rfc3339_ms)
        .ok_or_else(|| HeadError::row(mid, "the entry has no readable RFC 3339 timestamp"))
}

fn draft(mid: &str, row: &Value, message: Message) -> Result<Draft, HeadError> {
    let at = timestamp(mid, row)?;
    Ok(Draft {
        mid: Some(mid.to_owned()),
        message,
        written_at_ms: Some(at),
        created_at_ms: Some(at),
        usage: None,
        media: Vec::new(),
        keys: Vec::new(),
        row: mid.to_owned(),
        boundary: false,
    })
}

/// The content's blocks: a string is one text block.
fn blocks<'a>(mid: &str, content: &'a Value) -> Result<Vec<&'a Value>, HeadError> {
    match content {
        Value::String(_) => Ok(vec![content]),
        Value::Array(items) => Ok(items.iter().collect()),
        _ => Err(HeadError::row(
            mid,
            "the content is stored by reference, which the journal doesn't carry",
        )),
    }
}

fn block_type(block: &Value) -> &str {
    if block.is_string() {
        "text"
    } else {
        block["type"].as_str().unwrap_or_default()
    }
}

fn block_text(block: &Value) -> Option<&str> {
    block.as_str().or_else(|| block["text"].as_str())
}

/// An image block's bytes, which Pi keeps as base64 `data` with its `mimeType`.
fn image(
    mid: &str,
    block: &Value,
    check: &mut MediaCheck<'_>,
) -> Result<(broca_protocol::MediaRef, Vec<u8>), HeadError> {
    let refuse = |reason: String| HeadError::Media {
        mid: mid.to_owned(),
        reason,
    };
    let data = block["data"]
        .as_str()
        .ok_or_else(|| refuse("the image carries no bytes".into()))?;
    let declared = block["mimeType"]
        .as_str()
        .ok_or_else(|| refuse("the image declares no type".into()))?;
    let bytes = base64_bytes(data).map_err(refuse)?;
    let reference = check(&bytes, declared).map_err(refuse)?;
    Ok((reference, bytes))
}

/// A message Pi sends as the user's: its text and images.
fn user(
    mid: &str,
    row: &Value,
    content: &Value,
    check: &mut MediaCheck<'_>,
) -> Result<Option<Draft>, HeadError> {
    let mut out = Vec::new();
    let mut media = Vec::new();
    for block in blocks(mid, content)? {
        match block_type(block) {
            "text" => {
                let text = block_text(block).unwrap_or_default();
                if !text.is_empty() {
                    out.push(ContentBlock::Text {
                        text: text.to_owned(),
                    });
                }
            }
            "image" => {
                let (reference, bytes) = image(mid, block, check)?;
                media.push(HeadMedium {
                    block: out.len() as u32,
                    part: None,
                    bytes,
                });
                out.push(ContentBlock::Media(reference));
            }
            other => {
                return Err(HeadError::row(
                    mid,
                    format!("a user block of type {other:?} is not text or an image"),
                ))
            }
        }
    }
    if out.is_empty() {
        return Ok(None);
    }
    let mut draft = draft(mid, row, Message::new(Role::User, out))?;
    draft.media = media;
    Ok(Some(draft))
}

/// An assistant message: its text, its reasoning text without Pi's `thinkingSignature`, and
/// its tool calls. Thalamus's runner keys the calls of a reply that stopped to use tools
/// `<session>/<entry id>/<call id>`; any other reply's calls were never run, and have no key.
fn assistant(
    mid: &str,
    row: &Value,
    content: &Value,
    session: &str,
) -> Result<Option<Draft>, HeadError> {
    let tool_use = row["message"]["stopReason"] == "toolUse";
    let mut out = Vec::new();
    let mut keys = Vec::new();
    for block in blocks(mid, content)? {
        match block_type(block) {
            "text" => {
                let text = block_text(block).unwrap_or_default();
                if !text.is_empty() {
                    out.push(ContentBlock::Text {
                        text: text.to_owned(),
                    });
                }
            }
            "thinking" => {
                let text = block["thinking"].as_str().unwrap_or_default();
                if !text.is_empty() {
                    out.push(ContentBlock::Reasoning {
                        text: text.to_owned(),
                        signature: None,
                    });
                }
            }
            "toolCall" => {
                let call_id = block["id"]
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| HeadError::row(mid, "a tool call has no id"))?;
                let name = block["name"]
                    .as_str()
                    .ok_or_else(|| HeadError::row(mid, format!("call {call_id} names no tool")))?;
                keys.push(tool_use.then(|| format!("{session}/{mid}/{call_id}")));
                out.push(ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId(call_id.to_owned()),
                    tool_name: name.to_owned(),
                    input: block.get("arguments").cloned().unwrap_or(Value::Null),
                    thought_signature: None,
                }));
            }
            other => {
                return Err(HeadError::row(
                    mid,
                    format!(
                        "an assistant block of type {other:?} is not text, thinking or a tool call"
                    ),
                ))
            }
        }
    }
    if out.is_empty() {
        return Ok(None);
    }
    let mut draft = draft(mid, row, Message::new(Role::Assistant, out))?;
    draft.usage = usage(&row["message"]);
    draft.keys = keys;
    Ok(Some(draft))
}

/// A tool result entry: one tool message with the result of the call it names. Its text blocks
/// are the output, joined by a newline as Thalamus's runner serves it; with images, the blocks
/// stay as parts in order.
fn tool_result(
    mid: &str,
    row: &Value,
    content: &Value,
    check: &mut MediaCheck<'_>,
) -> Result<Draft, HeadError> {
    let message = &row["message"];
    let call_id = message["toolCallId"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| HeadError::row(mid, "the tool result names no call"))?;
    let is_error = match &message["isError"] {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        _ => return Err(HeadError::row(mid, "`isError` is not a boolean")),
    };
    let mut parts = Vec::new();
    let mut media = Vec::new();
    for block in blocks(mid, content)? {
        match block_type(block) {
            "text" => parts.push(ToolOutputPart::Text {
                text: block_text(block).unwrap_or_default().to_owned(),
            }),
            "image" => {
                let (reference, bytes) = image(mid, block, check)?;
                media.push(HeadMedium {
                    block: 0,
                    part: Some(parts.len() as u32),
                    bytes,
                });
                parts.push(ToolOutputPart::Media { media: reference });
            }
            other => {
                return Err(HeadError::row(
                    mid,
                    format!("a tool result block of type {other:?} is not text or an image"),
                ))
            }
        }
    }
    let text = || {
        parts
            .iter()
            .filter_map(|part| match part {
                ToolOutputPart::Text { text } => Some(text.as_str()),
                ToolOutputPart::Media { .. } => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let output = match (is_error, media.is_empty()) {
        (true, true) => ToolOutput::error(text()),
        (true, false) => {
            return Err(HeadError::row(
                mid,
                "a failed tool result carries images, which a failed result can't hold",
            ))
        }
        (false, true) => ToolOutput::Text { text: text() },
        (false, false) => ToolOutput::Parts { parts },
    };
    let mut draft = draft(
        mid,
        row,
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId(call_id.to_owned()),
                output,
                is_error,
            })],
        ),
    )?;
    // A tool results message has no creation time of its own, as for broca's own.
    draft.created_at_ms = None;
    draft.media = media;
    Ok(draft)
}

/// The step usage Thalamus's runner serves for a Pi assistant message
/// (`neutral/pi.rs`, `usage`): Pi stores all four counts as 0 when the provider reported none,
/// so all-zero counts are unknown; otherwise each count that is 0 is unknown, and the prompt
/// counts input and both cache counts.
fn usage(message: &Value) -> Option<HeadUsage> {
    let counts = &message["usage"];
    let read = |key: &str| counts[key].as_u64().unwrap_or(0);
    let (input, cache_read, cache_write, output) = (
        read("input"),
        read("cacheRead"),
        read("cacheWrite"),
        read("output"),
    );
    if input + cache_read + cache_write + output == 0 {
        return None;
    }
    let known = |n: u64| Some(n).filter(|&n| n > 0);
    Some(HeadUsage {
        prompt_tokens: known(input + cache_read + cache_write),
        cache_read_tokens: known(cache_read),
        cache_write_tokens: known(cache_write),
        output_tokens: known(output),
        reasoning_tokens: known(read("reasoning")),
    })
}

/// An RFC 3339 date-time (`2026-01-02T03:04:05.678Z`, or with a `+hh:mm` offset) in Unix
/// milliseconds; `None` for anything else. Fractions beyond milliseconds are cut.
pub fn rfc3339_ms(stamp: &str) -> Option<u64> {
    let bytes = stamp.as_bytes();
    let digits = |from: usize, len: usize| -> Option<i64> {
        let slice = bytes.get(from..from + len)?;
        slice
            .iter()
            .all(u8::is_ascii_digit)
            .then(|| std::str::from_utf8(slice).ok()?.parse().ok())
            .flatten()
    };
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let (year, month, day) = (digits(0, 4)?, digits(5, 2)?, digits(8, 2)?);
    let (hour, minute, second) = (digits(11, 2)?, digits(14, 2)?, digits(17, 2)?);
    let mut at = 19;
    let mut millis = 0i64;
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        let start = at;
        while bytes.get(at).is_some_and(u8::is_ascii_digit) {
            at += 1;
        }
        if at == start {
            return None;
        }
        let fraction = &stamp[start..at.min(start + 3)];
        millis = fraction.parse::<i64>().ok()? * 10i64.pow(3 - fraction.len() as u32);
    }
    let offset_minutes = match bytes.get(at) {
        Some(b'Z' | b'z') if at + 1 == bytes.len() => 0,
        Some(sign @ (b'+' | b'-')) if at + 6 == bytes.len() && bytes[at + 3] == b':' => {
            let minutes = digits(at + 1, 2)? * 60 + digits(at + 4, 2)?;
            if *sign == b'+' {
                minutes
            } else {
                -minutes
            }
        }
        _ => return None,
    };
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=12).contains(&month)
        || day < 1
        || day > month_days[(month - 1) as usize]
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    // Days since 1970-01-01 of the civil date (Howard Hinnant's `days_from_civil`).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offset_minutes * 60;
    u64::try_from(seconds * 1_000 + millis).ok()
}
