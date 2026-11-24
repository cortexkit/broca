//! On-demand surviving history (llm-runner §4.6) and display rows (§4.7).
//! Broca's history is append-only and wholly numbered. Keep this adapter over the existing
//! read helpers so a future history index can replace paging without changing the wire surface.

use broca_protocol::content::{ContentBlock, MediaRef, Role, ToolOutput, ToolOutputPart};
use cortexkit_role_llm_runner::{
    display::{
        DisplayEntry, DisplayPart, DisplayRow, FilePart, Limits, OtherPart, TextPart, ToolCallPart,
        ToolResultPart,
    },
    history::{HistoryForm, HistoryMode, HistoryRequest},
    read::descriptors::Reference,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    registry::LineageKey,
    serve::{ServeError, ServeHandler, SessionReadMessage, SessionReadRequest},
};

#[derive(Debug)]
pub enum HistoryError {
    Serve(ServeError),
    Unrepresentable { ordinal: u64, field: &'static str },
    Refused { code: &'static str, message: String },
}

impl From<ServeError> for HistoryError {
    fn from(error: ServeError) -> Self {
        Self::Serve(error)
    }
}

fn refused(code: &'static str, message: impl Into<String>) -> HistoryError {
    HistoryError::Refused {
        code,
        message: message.into(),
    }
}

// A boundary names the first entry of a forward page or the exclusive end of a backward page.
// Snapshot binding makes a cursor from removed or rewritten history expire.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    incarnation: String,
    history_version: u64,
    boundary: u64,
    backward: bool,
}

/// A `session.history` page cursor, bound to `history_version` as the page serves it. On the new
/// storage that is the served version (`store_exposure::served_history_version`), never the
/// stored one: a cursor taken before replies were served cleaned, used after, would join a page
/// of uncleaned text to a page of cleaned text, so it expires (`cursor_expired`) and the client
/// starts its paged read again, as the raised version asks of its cache.
fn cursor(incarnation: &str, history_version: u64, boundary: u64, backward: bool) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&Cursor {
            incarnation: incarnation.into(),
            history_version,
            boundary,
            backward,
        })
        .expect("history cursor JSON"),
    )
}

fn parse_cursor(token: &str) -> Result<Cursor, HistoryError> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .ok_or_else(|| {
            ServeError::InvalidParams {
                field: "cursor".into(),
                message: "session.history cursor is malformed".into(),
            }
            .into()
        })
}

fn media(media: &MediaRef) -> Reference {
    Reference::MediaRef {
        part: None,
        kind: media.kind.as_str().into(),
        mime: media.mime.clone(),
        byte_size: media.byte_size,
        sha256: media.sha256.clone(),
        width: media.width.map(u64::from),
        height: media.height.map(u64::from),
    }
}

/// The pure projection for broca's canonical stored message schema. Part keys are the same
/// stable part keys served by session.read, including unnumbered tool calls.
pub fn project(entry: &SessionReadMessage, limits: Option<&Limits>) -> DisplayRow {
    let exposed = broca_protocol::tags::exposed_message(&entry.message);
    let parts = exposed
        .content
        .iter()
        .enumerate()
        .map(|(index, block)| {
            let key = entry
                .part_keys
                .as_ref()
                .and_then(|keys| keys.get(index))
                .cloned()
                .unwrap_or_else(|| {
                    broca_protocol::tags::part_key(entry.ordinal.saturating_sub(1), index)
                });
            match block {
                ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                    DisplayPart::Text(TextPart::new(key, text))
                }
                ContentBlock::Reasoning { text, .. } => {
                    DisplayPart::Reasoning(TextPart::new(key, text))
                }
                ContentBlock::RedactedReasoning { .. } => {
                    DisplayPart::Other(OtherPart::new(key, "redacted_reasoning"))
                }
                // Anthropic's server-side fallback handoff: shown as a typed placeholder so the
                // part keys of the parts after it do not shift.
                ContentBlock::ProviderFallback { .. } => {
                    DisplayPart::Other(OtherPart::new(key, "provider_fallback"))
                }
                ContentBlock::Media(value) => DisplayPart::File(FilePart::media(key, media(value))),
                ContentBlock::ToolCall(call) => {
                    let mut part = ToolCallPart::new(
                        key,
                        &call.tool_call_id.0,
                        &call.tool_name,
                        &(call.input).parse(),
                        limits,
                    );
                    if let Some(fact) = entry
                        .tool_calls
                        .iter()
                        .find(|fact| fact.tool_call_id == call.tool_call_id.0)
                    {
                        part.call_key = fact.call_key.clone();
                        part.call_carrier = fact.call_carrier.clone();
                    }
                    if let Some(time) = entry
                        .tool_times
                        .iter()
                        .find(|time| time.tool_call_id == call.tool_call_id.0)
                    {
                        part.call_key = time.call_key.clone();
                        part.call_carrier = time.call_carrier.clone();
                        part.schema_pin = time.schema_pin.clone();
                        part.started_at = time.started_at;
                        part.finished_at = time.finished_at;
                        part.unanswered = time.unanswered.clone();
                    }
                    DisplayPart::ToolCall(part)
                }
                ContentBlock::ToolResult(result) => {
                    let (text, files) = match &result.output {
                        ToolOutput::Text { text } => (text.to_string(), vec![]),
                        ToolOutput::Json { value } => (value.to_string(), vec![]),
                        ToolOutput::Denied { reason } => (reason.clone(), vec![]),
                        ToolOutput::Error {
                            message,
                            module,
                            code,
                            detail,
                            nothing_sent,
                            retry,
                        } => (
                            ToolOutput::error_model_text(
                                message,
                                module.as_deref(),
                                code.as_deref(),
                                detail.as_deref(),
                                *nothing_sent,
                                retry.as_deref(),
                            ),
                            vec![],
                        ),
                        ToolOutput::Parts { parts } => (
                            cortexkit_role_llm_runner::run::join_text_parts(
                                parts.iter().filter_map(|part| match part {
                                    ToolOutputPart::Text { text } => Some(text.as_str()),
                                    _ => None,
                                }),
                            ),
                            parts
                                .iter()
                                .filter_map(|part| match part {
                                    ToolOutputPart::Media { media: value } => {
                                        Some(FilePart::media("", media(value)))
                                    }
                                    _ => None,
                                })
                                .collect(),
                        ),
                    };
                    DisplayPart::ToolResult(ToolResultPart::new(
                        key,
                        &result.tool_call_id.0,
                        result.is_error,
                        &text,
                        files,
                        limits,
                    ))
                }
            }
        })
        .collect();
    let role = match entry.message.role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    };
    let mut row = DisplayRow::new(role, parts);
    row.notices.clone_from(&entry.notices);
    row.sender.clone_from(&entry.sender);
    row.speaks_for.clone_from(&entry.speaks_for);
    row.notice = entry.notice;
    row.keep = entry.keep;
    row.model_fallback.clone_from(&entry.model_fallback);
    if let Some(origin) = &entry.message.origin {
        row = row.with_model(format!("{}/{}", origin.provider_module_id, origin.model_id));
    }
    row
}

/// One entry of the raw form: the message as `session.read` serves it, with the time it was
/// written, which the raw form always carries. Of the facts a display-facts read adds, the raw
/// form keeps the sender and the send's caller-visible id.
pub fn raw_entry(entry: &SessionReadMessage, written_at: u64) -> Value {
    let mut value = serde_json::to_value(entry).expect("history entry JSON");
    for field in [
        "client_message_id",
        "created_at",
        "completed_at",
        "reasoning_summaries",
        "tool_times",
        "notices",
    ] {
        value
            .as_object_mut()
            .expect("an entry is an object")
            .remove(field);
    }
    value["written_at"] = written_at.into();
    value
}

/// Read one page only; output-reference derivation shares the page's entries and target cache.
pub async fn read<H: ServeHandler + ?Sized>(
    handler: &H,
    session: &LineageKey,
    request: &HistoryRequest,
) -> Result<Value, HistoryError> {
    let mode = request.mode().map_err(|field| ServeError::InvalidParams {
        field: field.into(),
        message: format!("session.history does not accept {field} in this request"),
    })?;
    let named = match mode {
        HistoryMode::FromMid(mid) | HistoryMode::BeforeMid(mid) => Some(mid),
        _ => None,
    };
    // A session kept in the session store is read from it: its head facts here, its pages through
    // `handler.read`, and what output references need through the handler's store reads.
    let store = handler.store_history_head(session, named).await?;

    // The store's own ids aren't positions, so it resolves a named message itself.
    let store_mid = store.as_ref().map(|store| store.mid_ordinal);
    let head = match store {
        Some(store) => broca_wire::HeadResponse {
            incarnation: store.incarnation,
            history_version: store.history_version,
            last_ordinal: store.last_ordinal,
            first_ordinal: store.last_ordinal.map(|_| 1),
            ..Default::default()
        },
        None => handler.head(session).await?,
    };
    if head.incarnation == "unwritten" {
        if let HistoryMode::Cursor(token) = mode {
            parse_cursor(token)?;
            return Err(refused(
                "cursor_expired",
                "session.history cursor names another lineage",
            ));
        }
        if matches!(mode, HistoryMode::FromMid(_) | HistoryMode::BeforeMid(_)) {
            return Err(refused(
                "unknown_mid",
                "session.history mid is not retained",
            ));
        }
        if matches!(
            mode,
            HistoryMode::FromOrdinal(_) | HistoryMode::BeforeOrdinal(_)
        ) {
            return Err(refused(
                "unknown_ordinal",
                "session.history ordinal is not retained",
            ));
        }
        return Ok(
            serde_json::json!({"messages": [], "incarnation": head.incarnation, "history_version": head.history_version}),
        );
    }
    let lineage = head.incarnation.clone();
    let end = head.last_ordinal.map_or(1, |last| last.saturating_add(1));
    let (boundary, backward) = match mode {
        HistoryMode::Newest => (end, true),
        HistoryMode::Cursor(token) => {
            let position = parse_cursor(token)?;
            if position.incarnation != lineage
                || position.history_version != head.history_version
                || position.boundary > end
            {
                return Err(refused(
                    "cursor_expired",
                    "session.history cursor is not in this session's surviving history",
                ));
            }
            (position.boundary, position.backward)
        }
        HistoryMode::FromMid(mid) | HistoryMode::BeforeMid(mid) => {
            let at = match store_mid {
                Some(found) => found,
                None => broca_core::position_of_mid(mid, head.mode3, head.lineage_id.as_deref())
                    .map(|position| position + 1),
            }
            .filter(|at| *at < end)
            .ok_or_else(|| {
                refused(
                    "unknown_mid",
                    format!("session.history mid {mid} is not retained"),
                )
            })?;
            (at, matches!(mode, HistoryMode::BeforeMid(_)))
        }
        HistoryMode::FromOrdinal(at) | HistoryMode::BeforeOrdinal(at) => {
            let at = at.max(1);
            if at >= end {
                return Err(refused(
                    "unknown_ordinal",
                    format!("session.history ordinal {at} is not retained"),
                ));
            }
            (at, matches!(mode, HistoryMode::BeforeOrdinal(_)))
        }
    };
    let limit = request
        .limit
        .unwrap_or(100)
        .min(broca_core::SESSION_READ_MAX_LIMIT);
    let start = if backward {
        boundary.saturating_sub(limit).max(1)
    } else {
        boundary
    };
    let stop = if backward {
        boundary
    } else {
        end.min(start.saturating_add(limit))
    };
    let mut entries = Vec::new();
    let mut entry_bytes = 0;
    let mut from = start;
    while from < stop {
        let mut read = SessionReadRequest::new(session.clone(), Some(from), Some(stop - from));
        read.history_version = Some(head.history_version);
        read.include_rich_ui = request.form() == HistoryForm::Display;
        // Raw history also reports positive sender evidence, but no other opt-in envelope facts.
        read.include_display_facts = true;
        read.max_bytes = Some(broca_wire::READ_MAXIMUM_MAX_BYTES);
        // Display needs stored payloads; raw history honors the caller's descriptor choice.
        read.media = if request.form() == HistoryForm::Display {
            Default::default()
        } else {
            request.media.unwrap_or_default()
        };
        let page = handler.read(read).await?;
        let Some(last) = page.messages.last() else {
            break;
        };
        from = last.ordinal.saturating_add(1);
        for entry in page.messages {
            entry_bytes += broca_protocol::serialized_json_bytes(&entry);
            entries.push(entry);
        }
        if !backward {
            break;
        }
        // Stage only a bounded raw suffix before deriving descriptors. Limits can make a
        // display page smaller than its byte budget; cursors still cover every omitted entry.
        let mut dropped = 0;
        while entries.len() - dropped > 1 && entry_bytes > broca_wire::READ_MAXIMUM_MAX_BYTES {
            entry_bytes -= broca_protocol::serialized_json_bytes(&entries[dropped]);
            dropped += 1;
        }
        entries.drain(..dropped);
    }
    let mut creation_times = Vec::with_capacity(entries.len());
    for entry in &entries {
        // session.read already derives durable and legacy message times. Do not substitute
        // a sentinel when that shared derivation has no evidence for a creation time.
        creation_times.push(entry.written_at.ok_or(HistoryError::Unrepresentable {
            ordinal: entry.ordinal,
            field: "written_at",
        })?);
    }
    let refs = if request.form() == HistoryForm::Display {
        crate::output_refs::page_output_refs(
            handler,
            session,
            &lineage,
            head.history_version,
            &entries,
        )
        .await?
    } else {
        Default::default()
    };
    let mut messages = Vec::new();
    for (entry, written_at) in entries.iter().zip(creation_times) {
        let value = if request.form() == HistoryForm::Display {
            let mut row = project(entry, request.limits.as_ref());
            if entry.message.role == Role::Assistant {
                row.rich_ui = entry.rich_ui.clone();
            }
            if entry.message.role == Role::User {
                // The store recorded each message's client id beside it.
                row.client_message_id.clone_from(&entry.client_message_id);
            }
            if let Some(refs) = refs.get(&entry.ordinal.saturating_sub(1)) {
                row = refs.attach(row);
            }
            serde_json::to_value(
                DisplayEntry::new(entry.ordinal, &entry.mid, written_at, row)
                    .with_part_keys(entry.part_keys.clone().unwrap_or_default()),
            )
            .expect("display entry JSON")
        } else {
            raw_entry(entry, written_at)
        };
        messages.push(value);
    }
    let cap = request
        .max_bytes
        .unwrap_or(broca_wire::READ_DEFAULT_MAX_BYTES)
        .min(broca_wire::READ_MAXIMUM_MAX_BYTES);
    // A single oversize entry is returned whole, as on session.read; only the hard frame limit
    // refuses it. Backward reads keep the suffix, never the older end of the requested page.
    let envelope = |first: Option<&Value>, last: Option<&Value>| {
        let first = first
            .and_then(|entry| entry["ordinal"].as_u64())
            .unwrap_or(start);
        let after = last
            .and_then(|entry| entry["ordinal"].as_u64())
            .map_or(stop, |last| last + 1);
        let mut page = serde_json::json!({"messages": [], "incarnation": lineage, "history_version": head.history_version});
        if first > 1 {
            page["older"] = cursor(&lineage, head.history_version, first, true).into();
        }
        if after < end {
            page["newer"] = cursor(&lineage, head.history_version, after, false).into();
        }
        page
    };
    let page = cap_messages(messages, cap, backward, envelope);
    let first = page["messages"]
        .as_array()
        .and_then(|messages| messages.first())
        .and_then(|entry| entry["ordinal"].as_u64())
        .unwrap_or(start);
    let bytes = broca_wire::serialized_result_frame_bytes(&page);
    if bytes > broca_wire::SUBC_MAX_FRAME_BYTES {
        return Err(
            ServeError::ReplyTooLarge(crate::serve::ReplyTooLargeDetail::new(
                bytes,
                broca_wire::SUBC_MAX_FRAME_BYTES,
                crate::serve::ReplyTooLargeSubject::Message {
                    session: session.session.clone(),
                    ordinal: first,
                },
            ))
            .into(),
        );
    }
    Ok(page)
}

/// `messages` as a history page, trimmed to `cap` bytes: from the front on a `backward` read,
/// which keeps the newest end, else from the back. `envelope` builds the page around the
/// messages kept, given the first and last of them, with an empty `messages` array.
///
/// The boundary rule is the one `session.history` has always kept: a page is served when it
/// holds at most one message, or when its JSON is at most `cap` bytes (`<=`; `serve`'s
/// `cap_page_bytes` instead trims while a page is `>=` its cap) and its frame fits the
/// transport's frame limit. Each message is measured once and the page's size kept up to date
/// as messages are dropped: building and measuring the whole page again for each message dropped
/// cost time quadratic in the page's length.
fn cap_messages(
    mut messages: Vec<Value>,
    cap: u64,
    backward: bool,
    envelope: impl Fn(Option<&Value>, Option<&Value>) -> Value,
) -> Value {
    let sizes: Vec<u64> = messages
        .iter()
        .map(broca_protocol::serialized_json_bytes)
        .collect();
    let mut kept = sizes
        .iter()
        .fold(0u64, |sum, size| sum.saturating_add(*size));
    let (mut low, mut high) = (0, messages.len());
    loop {
        let window = &messages[low..high];
        let shell = envelope(window.first(), window.last());
        let shell_bytes = broca_protocol::serialized_json_bytes(&shell);
        let framing = broca_wire::serialized_result_frame_bytes(&shell).saturating_sub(shell_bytes);
        // The messages, and the commas between them, inside the shell's empty array.
        let bytes = shell_bytes
            .saturating_add(kept)
            .saturating_add(window.len().saturating_sub(1) as u64);
        if window.len() <= 1
            || (bytes <= cap && bytes.saturating_add(framing) <= broca_wire::SUBC_MAX_FRAME_BYTES)
        {
            break;
        }
        if backward {
            kept = kept.saturating_sub(sizes[low]);
            low += 1;
        } else {
            high -= 1;
            kept = kept.saturating_sub(sizes[high]);
        }
    }
    messages.truncate(high);
    messages.drain(..low);
    let mut page = envelope(messages.first(), messages.last());
    page["messages"] = Value::Array(messages);
    page
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Trimming a page from one measurement of each message keeps exactly the messages the old
    /// rule kept, building and measuring the whole page again for each message dropped: at every
    /// cap, from either end, with cursors that change length as the window moves.
    #[test]
    fn capped_history_pages_keep_what_measuring_each_page_kept() {
        let messages: Vec<Value> = (1..=40u64)
            .map(|ordinal| json!({"ordinal": ordinal * 7, "text": "x".repeat((ordinal * 37 % 90) as usize)}))
            .collect();
        let envelope = |first: Option<&Value>, last: Option<&Value>| {
            let first = first
                .and_then(|entry| entry["ordinal"].as_u64())
                .unwrap_or(7);
            let after = last
                .and_then(|entry| entry["ordinal"].as_u64())
                .map_or(281, |last| last + 1);
            let mut page = json!({"messages": [], "incarnation": "i", "history_version": 3});
            if first > 1 {
                page["older"] = cursor("i", 3, first, true).into();
            }
            if after < 300 {
                page["newer"] = cursor("i", 3, after, false).into();
            }
            page
        };
        let measured_each_time = |mut messages: Vec<Value>, cap: u64, backward: bool| loop {
            let mut page = envelope(messages.first(), messages.last());
            page["messages"] = Value::Array(messages.clone());
            if messages.len() <= 1
                || (broca_protocol::serialized_json_bytes(&page) <= cap
                    && broca_wire::serialized_result_frame_bytes(&page)
                        <= broca_wire::SUBC_MAX_FRAME_BYTES)
            {
                break page;
            }
            if backward {
                messages.remove(0);
            } else {
                messages.pop();
            }
        };
        let whole = {
            let mut page = envelope(messages.first(), messages.last());
            page["messages"] = Value::Array(messages.clone());
            broca_protocol::serialized_json_bytes(&page)
        };
        for cap in (0..whole + 20).step_by(3).chain([u64::MAX]) {
            for backward in [false, true] {
                let expected = measured_each_time(messages.clone(), cap, backward);
                let served = cap_messages(messages.clone(), cap, backward, envelope);
                assert_eq!(
                    serde_json::to_string(&served).unwrap(),
                    serde_json::to_string(&expected).unwrap(),
                    "cap {cap}, backward {backward}"
                );
            }
        }
    }

    #[test]
    fn display_reply_matches_session_read_exposure() {
        let mut entry: SessionReadMessage = serde_json::from_value(json!({
            "ordinal": 1, "mid": "m0", "written_at": 123,
            "message": {"role": "assistant", "content": [
                {"type": "text", "text": "§32§ I have read it  "},
                {"type": "reasoning", "text": "§8§ thought"},
                {"type": "tool_call", "tool_call_id": "call", "tool_name": "work", "input": {"text": "§9§ args"}},
                {"type": "tool_result", "tool_call_id": "call", "is_error": false, "output": {"kind": "text", "text": "§10§ result"}}
            ]}
        })).unwrap();
        let raw = entry.message.clone();
        for clean in [true, false] {
            broca_protocol::tags::number_message(
                &mut entry.message,
                broca_protocol::tags::ComputedRules {
                    render: true,
                    clean,
                },
                &mut 0,
            )
            .unwrap();
            let served = serde_json::to_value(&entry).unwrap();
            let row = serde_json::to_value(project(&entry, None)).unwrap();
            assert_eq!(row["text"], served["message"]["content"][0]["text"]);
            assert_eq!(row["parts"][1]["text"], "§8§ thought");
            assert_eq!(row["parts"][2]["arguments"], "{\"text\":\"§9§ args\"}");
            assert_eq!(row["parts"][3]["content"], "§10§ result");
            assert_eq!(entry.message.content, raw.content);
        }
        entry.message.role = Role::User;
        assert_eq!(project(&entry, None).text, "§32§ I have read it  ");
    }

    #[test]
    fn projection_preserves_native_part_order_keys_media_and_utf8_limits() {
        let entry: SessionReadMessage = serde_json::from_value(json!({
            "ordinal": 8, "mid":"mdeadbeef.7", "written_at":123,
            "part_keys": (0..6).map(|n| format!("m7#{n}")).collect::<Vec<_>>(),
            "message":{"role":"assistant", "origin":{"provider_module_id":"mock", "model_id":"model"}, "content":[
                {"type":"text","text":"first"},
                {"type":"reasoning","text":"thinking"},
                {"type":"tool_call","tool_call_id":"call", "tool_name":"work", "input":{"a":"éé"}},
                {"type":"tool_result","tool_call_id":"call", "is_error":true, "output":{"kind":"parts","parts":[
                    {"kind":"text","text":"ééé"},
                    {"kind":"media","media":{"kind":"image","mime":"image/png","byte_size":5,"sha256":"a".repeat(64),"width":1,"height":2}},
                    {"kind":"text","text":"last"}
                ]}},
                {"type":"redacted_reasoning","data":"opaque"},
                {"type":"text","text":"second"}
            ]}
        })).unwrap();
        let row = project(&entry, Some(&Limits::new(7, 1, 5)));
        assert_eq!(row.text, "firstsecond");
        assert_eq!(row.model.as_deref(), Some("mock/model"));
        let row = serde_json::to_value(row).unwrap();
        assert_eq!(
            row["parts"]
                .as_array()
                .unwrap()
                .iter()
                .map(|part| part["part"].clone())
                .collect::<Vec<_>>(),
            (0..6).map(|n| json!(format!("m7#{n}"))).collect::<Vec<_>>()
        );
        assert_eq!(row["parts"][2]["arguments"], "{\"a\":\"");
        assert_eq!(row["parts"][2]["arguments_truncated"], true);
        assert_eq!(row["parts"][3]["content"], "éé");
        assert_eq!(row["parts"][3]["truncated"], true);
        assert_eq!(
            row["parts"][3]["files"][0]["media"]["sha256"],
            "a".repeat(64)
        );
        assert_eq!(row["parts"][4]["type"], "other");
        let whole = serde_json::to_value(project(&entry, None)).unwrap();
        assert_eq!(whole["parts"][3]["content"], "ééélast");
        assert!(whole["parts"][3].get("truncated").is_none());
    }
}
