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
use std::collections::{BTreeMap, VecDeque};

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
// Lineage binding makes a cursor from a removed/recreated session expire, not read an empty page.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    lineage: String,
    boundary: u64,
    backward: bool,
}

fn cursor(lineage: &str, boundary: u64, backward: bool) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&Cursor {
            lineage: lineage.into(),
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

fn ordinal(mid: &str) -> Option<u64> {
    let text = mid.strip_prefix('m')?;
    let number: u64 = text.parse().ok()?;
    (number.to_string() == text).then_some(number)
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

#[derive(Clone, Default)]
struct RowMetadata {
    client_message_id: Option<String>,
    sender: Option<String>,
    tool_pins: BTreeMap<String, broca_wal::DispatchSchemaPin>,
}

fn send_metadata(
    digest: Option<&str>,
    principal: Option<&broca_protocol::BindPrincipal>,
) -> RowMetadata {
    RowMetadata {
        client_message_id: digest.and_then(broca_core::digest_client_message_id),
        sender: match principal {
            Some(broca_protocol::BindPrincipal::Reserved { module_id }) => Some(module_id.clone()),
            _ => None,
        },
        tool_pins: Default::default(),
    }
}

/// Sender metadata and tool schema pins are durable beside message bodies. Reuse
/// replay placement so held messages and their dispatch facts keep the same ordinals.
fn metadata(records: &[broca_wal::WalRecord]) -> Result<BTreeMap<u64, RowMetadata>, ServeError> {
    use broca_core::placement::{AppendKind, Placement, PlacementAction};
    use broca_wal::WalRecord;
    let mut placement = Placement::default();
    let mut queued = BTreeMap::new();
    let mut senders = BTreeMap::new();
    let mut pending = VecDeque::new();
    let mut rows = BTreeMap::new();
    let mut assistant = None;
    let mut tool_pins: BTreeMap<u64, BTreeMap<String, broca_wal::DispatchSchemaPin>> =
        BTreeMap::new();
    for record in records {
        let mut current = RowMetadata::default();
        match record {
            WalRecord::DeliveryOrdered {
                submission_id,
                sent_by,
                ..
            } => {
                senders.insert(submission_id.0.clone(), sent_by.clone());
            }
            WalRecord::Queued {
                submission_id,
                request_identity_digest,
                principal,
                ..
            } => {
                let principal = senders
                    .get(&submission_id.0)
                    .and_then(Option::as_ref)
                    .or(principal.as_ref());
                queued.insert(
                    submission_id.0.clone(),
                    send_metadata(request_identity_digest.as_deref(), principal),
                );
            }
            WalRecord::RunStarted {
                request_identity_digest,
                principal,
                submission_id,
                origin,
                ..
            } => {
                pending.clear();
                assistant = None;
                if matches!(origin, broca_wal::RunOrigin::Fresh) {
                    current = submission_id
                        .as_ref()
                        .and_then(|id| queued.get(&id.0))
                        .cloned()
                        .unwrap_or_else(|| {
                            send_metadata(request_identity_digest.as_deref(), principal.as_ref())
                        });
                }
            }
            WalRecord::Steered {
                request_identity_digest,
                input,
                ..
            } => {
                // Legacy steers have no recorded principal; do not guess the session owner's.
                pending.extend(std::iter::repeat_n(
                    send_metadata(request_identity_digest.as_deref(), None),
                    input.len(),
                ));
            }
            WalRecord::SendDelivered {
                submission_id,
                input: Some(input),
                ..
            } => {
                pending.extend(std::iter::repeat_n(
                    queued.get(&submission_id.0).cloned().unwrap_or_default(),
                    input.len(),
                ));
            }
            WalRecord::ToolDispatchIntent {
                tool_call_id,
                schema_pin: Some(pin),
                ..
            } => {
                if let Some(ordinal) = assistant {
                    tool_pins
                        .entry(ordinal)
                        .or_default()
                        .entry(tool_call_id.0.clone())
                        .or_insert_with(|| pin.clone());
                }
            }
            _ => {}
        }
        let actions = placement.observe(record).map_err(|message| {
            ServeError::Open(
                format!("session.history metadata placement: {message}"),
                Some(broca_protocol::ErrorClass::Transient),
            )
        })?;
        if matches!(record, WalRecord::ModelStepFinished { .. }) {
            assistant = actions
                .iter()
                .find_map(|action| match action {
                    PlacementAction::Append {
                        first,
                        kind: AppendKind::Assistant | AppendKind::Finalize,
                        ..
                    } => Some(*first),
                    _ => None,
                })
                .or_else(|| placement.held_assistant_ordinal());
        }
        for action in actions {
            match action {
                PlacementAction::Append {
                    first,
                    count,
                    kind: AppendKind::Input,
                } => {
                    for ordinal in first..first + count {
                        rows.insert(ordinal, current.clone());
                    }
                }
                PlacementAction::PlaceHeld { first, count } => {
                    for ordinal in first..first + count {
                        rows.insert(ordinal, pending.pop_front().unwrap_or_default());
                    }
                }
                PlacementAction::Truncate { to } => {
                    rows.retain(|ordinal, _| *ordinal < to);
                    tool_pins.retain(|ordinal, _| *ordinal < to);
                }
                _ => {}
            }
        }
    }
    for (ordinal, pins) in tool_pins {
        rows.entry(ordinal).or_default().tool_pins = pins;
    }
    Ok(rows)
}

/// The pure projection for broca's canonical stored message schema. Part keys are the same
/// stable keys served by session.read's tag rows, including unnumbered tool calls.
pub fn project(entry: &SessionReadMessage, limits: Option<&Limits>) -> DisplayRow {
    let parts = entry
        .message
        .content
        .iter()
        .enumerate()
        .map(|(index, block)| {
            let key = broca_protocol::tags::part_key(entry.ordinal, index);
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
                ContentBlock::Media(value) => DisplayPart::File(FilePart::media(key, media(value))),
                ContentBlock::ToolCall(call) => {
                    let mut part = ToolCallPart::new(
                        key,
                        &call.tool_call_id.0,
                        &call.tool_name,
                        &call.input,
                        limits,
                    );
                    part.call_key = entry
                        .tool_calls
                        .iter()
                        .find(|fact| fact.tool_call_id == call.tool_call_id.0)
                        .and_then(|fact| fact.call_key.clone());
                    DisplayPart::ToolCall(part)
                }
                ContentBlock::ToolResult(result) => {
                    let (text, files) = match &result.output {
                        ToolOutput::Text { text } => (text.clone(), vec![]),
                        ToolOutput::Json { value } => (value.to_string(), vec![]),
                        ToolOutput::Denied { reason } => (reason.clone(), vec![]),
                        ToolOutput::Error { message } => (message.clone(), vec![]),
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
    if let Some(origin) = &entry.message.origin {
        row = row.with_model(format!("{}/{}", origin.provider_module_id, origin.model_id));
    }
    row
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
    let head = handler.head(session).await?;
    let Some(lineage) = head.lineage_id else {
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
        return Ok(serde_json::json!({"messages": []}));
    };
    let end = head.last_ordinal.map_or(0, |last| last.saturating_add(1));
    let (boundary, backward) = match mode {
        HistoryMode::Newest => (end, true),
        HistoryMode::Cursor(token) => {
            let position = parse_cursor(token)?;
            if position.lineage != lineage || position.boundary > end {
                return Err(refused(
                    "cursor_expired",
                    "session.history cursor is not in this session's surviving history",
                ));
            }
            (position.boundary, position.backward)
        }
        HistoryMode::FromMid(mid) | HistoryMode::BeforeMid(mid) => {
            let at = ordinal(mid).filter(|at| *at < end).ok_or_else(|| {
                refused(
                    "unknown_mid",
                    format!("session.history mid {mid} is not retained"),
                )
            })?;
            (at, matches!(mode, HistoryMode::BeforeMid(_)))
        }
        HistoryMode::FromOrdinal(at) | HistoryMode::BeforeOrdinal(at) => {
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
        boundary.saturating_sub(limit)
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
        read.lineage_id = Some(lineage.clone());
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
        while entries.len() > 1 && entry_bytes > broca_wire::READ_MAXIMUM_MAX_BYTES {
            entry_bytes -= broca_protocol::serialized_json_bytes(&entries.remove(0));
        }
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
    let records = if request.form() == HistoryForm::Display {
        match entries.iter().find_map(|entry| entry.run.as_ref()) {
            Some(run) => handler.session_log(session, &run.run_id).await?,
            None => None,
        }
    } else {
        None
    };
    let metadata = records
        .as_deref()
        .map(metadata)
        .transpose()?
        .unwrap_or_default();
    let refs = if request.form() == HistoryForm::Display {
        crate::output_refs::page_output_refs(handler, session, &lineage, &entries, records).await?
    } else {
        Default::default()
    };
    let mut messages = Vec::new();
    for (entry, written_at) in entries.iter().zip(creation_times) {
        let value = if request.form() == HistoryForm::Display {
            let mut row = project(entry, request.limits.as_ref());
            if let Some(metadata) = metadata.get(&entry.ordinal) {
                for call in row.tool_calls_mut() {
                    call.schema_pin = metadata.tool_pins.get(&call.tool_call_id).and_then(|pin| {
                        cortexkit_role_tool_provider::call::SchemaPin::new(
                            &call.name,
                            &pin.schema_digest,
                            pin.semantics,
                        )
                        .encode()
                        .ok()
                    });
                }
            }
            if entry.message.role == Role::User {
                if let Some(metadata) = metadata.get(&entry.ordinal) {
                    row.client_message_id = metadata.client_message_id.clone();
                    row.sender = metadata.sender.clone();
                }
            }
            if let Some(refs) = refs.get(&entry.ordinal) {
                row = refs.attach(row);
            }
            serde_json::to_value(
                DisplayEntry::new(&entry.mid, written_at, row)
                    .with_ordinal(entry.ordinal)
                    .with_tags(entry.tags.clone()),
            )
            .expect("display entry JSON")
        } else {
            let mut value = serde_json::to_value(entry).expect("history entry JSON");
            value["written_at"] = written_at.into();
            value
        };
        messages.push(value);
    }
    let cap = request
        .max_bytes
        .unwrap_or(broca_wire::READ_DEFAULT_MAX_BYTES)
        .min(broca_wire::READ_MAXIMUM_MAX_BYTES);
    // A single oversize entry is returned whole, as on session.read; only the hard frame limit
    // refuses it. Backward reads keep the suffix, never the older end of the requested page.
    let (page, first) = loop {
        let first = messages
            .first()
            .and_then(|entry| entry["ordinal"].as_u64())
            .unwrap_or(start);
        let after = messages
            .last()
            .and_then(|entry| entry["ordinal"].as_u64())
            .map_or(stop, |last| last + 1);
        let mut page = serde_json::json!({"messages": messages, "lineage_id": lineage, "history_version": head.history_version.unwrap_or(0)});
        if first > 0 {
            page["older"] = cursor(&lineage, first, true).into();
        }
        if after < end {
            page["newer"] = cursor(&lineage, after, false).into();
        }
        if messages.len() <= 1
            || (broca_protocol::serialized_json_bytes(&page) <= cap
                && broca_wire::serialized_result_frame_bytes(&page)
                    <= broca_wire::SUBC_MAX_FRAME_BYTES)
        {
            break (page, first);
        }
        if backward {
            messages.remove(0);
        } else {
            messages.pop();
        }
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn projection_preserves_native_part_order_keys_media_and_utf8_limits() {
        let entry: SessionReadMessage = serde_json::from_value(json!({
            "ordinal": 7, "mid":"m7", "written_at":123,
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
