//! Readers for importing foreign harness sessions into broca.
//!
//! Container decoding belongs here; engine-side admission remains responsible for validating
//! tool-call/result pairing. Tree-backed readers must select the path to the latest leaf and
//! exclude sidechains. OpenCode is flat, so its reader does not need branch selection.

#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::io::{self, BufRead, Write};
use std::path::Path;

use broca_protocol::{ContentBlock, Message, Role, ToolCallId};
use serde_json::Value;

pub mod claude_code;
pub mod codex;
pub mod goose;
pub mod opencode;
pub mod pi;

mod sqlite_read;

pub(crate) const MAX_SESSION_SOURCE_BYTES: u64 = 128 * 1024 * 1024;
pub(crate) const MAX_JSONL_LINE_BYTES: usize = 8 * 1024 * 1024;
const MAX_SESSION_TITLE_CHARS: usize = 80;

pub(crate) fn truncate_session_title(title: String) -> String {
    if title.chars().count() <= MAX_SESSION_TITLE_CHARS {
        return title;
    }
    let mut truncated = title
        .chars()
        .take(MAX_SESSION_TITLE_CHARS - 1)
        .collect::<String>();
    truncated.push('…');
    truncated
}

/// Refuse oversized JSONL sources before retaining their parsed rows.
pub(crate) fn check_session_source_size(path: &Path) -> io::Result<u64> {
    let size = std::fs::metadata(path)?.len();
    if size > MAX_SESSION_SOURCE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "source session {} is {size} bytes; the import reader limit is {} bytes",
                path.display(),
                MAX_SESSION_SOURCE_BYTES
            ),
        ));
    }
    Ok(size)
}

/// Read one JSONL record without allowing an unbounded line or source to grow the buffer.
pub(crate) fn read_bounded_jsonl_line<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
    bytes_read: &mut u64,
) -> io::Result<bool> {
    line.clear();
    let mut read_any = false;
    loop {
        let (count, ended_line) = {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                return Ok(read_any);
            }
            let count = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |index| index + 1);
            if line.len().saturating_add(count) > MAX_JSONL_LINE_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("JSONL record exceeds the {MAX_JSONL_LINE_BYTES}-byte line limit"),
                ));
            }
            if bytes_read.saturating_add(count as u64) > MAX_SESSION_SOURCE_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "source session exceeds the {MAX_SESSION_SOURCE_BYTES}-byte import reader limit"
                    ),
                ));
            }
            line.extend_from_slice(&available[..count]);
            (count, available[count - 1] == b'\n')
        };
        reader.consume(count);
        *bytes_read += count as u64;
        read_any = true;
        if ended_line {
            break;
        }
    }
    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
    Ok(true)
}

/// Replace image blocks nested in imported tool output before it reaches a model prompt.
///
/// Canonical tool output has no image block, so keeping the source JSON can turn a large
/// base64 payload into ordinary prompt text. A fixed text placeholder preserves the position
/// of omitted media without retaining its bytes.
pub(crate) fn omit_nested_tool_images(value: &mut Value) -> usize {
    match value {
        Value::Array(items) => items.iter_mut().map(omit_nested_tool_images).sum(),
        Value::Object(object) => {
            let kind = object.get("type").and_then(Value::as_str);
            let has_base64_source =
                object
                    .get("source")
                    .and_then(Value::as_object)
                    .is_some_and(|source| {
                        source.get("type").and_then(Value::as_str) == Some("base64")
                            && source.get("data").and_then(Value::as_str).is_some()
                    });
            if matches!(kind, Some("image" | "image_url" | "input_image")) || has_base64_source {
                *value = serde_json::json!({
                    "type": "text",
                    "text": "[image omitted from imported tool output]"
                });
                return 1;
            }
            object.values_mut().map(omit_nested_tool_images).sum()
        }
        _ => 0,
    }
}

/// Metadata cheap enough to show while choosing a source session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSummary {
    pub id: String,
    pub title: Option<String>,
    pub cwd: Option<String>,
    pub message_count: usize,
    /// `None` when obtaining a size would require scanning stored session payloads.
    pub approximate_size_bytes: Option<u64>,
}

/// One surviving imported block used to translate a source transcript coordinate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedBlockCoordinate {
    pub message_ordinal: u64,
    pub block_index: usize,
}

impl ImportedBlockCoordinate {
    pub fn flat_id(&self) -> String {
        format!("m{}#{}", self.message_ordinal, self.block_index)
    }
}

/// Translation from OpenCode's source-row coordinates to the history committed by
/// `session.import` after its tool-pair normalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinateMap {
    by_ordinal: HashMap<u64, ImportedBlockRange>,
    by_message_id: HashMap<String, ImportedBlockRange>,
    dates_by_ordinal: HashMap<u64, String>,
    dates_by_message_id: HashMap<String, String>,
    imported_block_counts: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportedBlockRange {
    start: Option<ImportedBlockCoordinate>,
    end: Option<ImportedBlockCoordinate>,
}

impl CoordinateMap {
    pub fn start_coordinate_for_ordinal(&self, ordinal: u64) -> Option<&ImportedBlockCoordinate> {
        self.by_ordinal
            .get(&ordinal)
            .and_then(|range| range.start.as_ref())
    }

    pub fn start_coordinate_for_message_id(
        &self,
        message_id: &str,
    ) -> Option<&ImportedBlockCoordinate> {
        self.by_message_id
            .get(message_id)
            .and_then(|range| range.start.as_ref())
    }

    pub fn coordinate_for_ordinal(&self, ordinal: u64) -> Option<&ImportedBlockCoordinate> {
        self.by_ordinal
            .get(&ordinal)
            .and_then(|range| range.end.as_ref())
    }

    pub fn coordinate_for_message_id(&self, message_id: &str) -> Option<&ImportedBlockCoordinate> {
        self.by_message_id
            .get(message_id)
            .and_then(|range| range.end.as_ref())
    }

    pub fn date_for_ordinal(&self, ordinal: u64) -> Option<&str> {
        self.dates_by_ordinal.get(&ordinal).map(String::as_str)
    }

    pub fn date_for_message_id(&self, message_id: &str) -> Option<&str> {
        self.dates_by_message_id.get(message_id).map(String::as_str)
    }

    pub fn contains_flat_id(&self, flat_id: &str) -> bool {
        let Some((message, block)) = flat_id.strip_prefix('m').and_then(|id| id.split_once('#'))
        else {
            return false;
        };
        let (Ok(message), Ok(block)) = (message.parse::<usize>(), block.parse::<usize>()) else {
            return false;
        };
        self.imported_block_counts
            .get(message)
            .is_some_and(|count| block < *count)
    }
}

/// One source session converted to broca's canonical wire schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportedSession {
    pub messages: Vec<Message>,
    pub source_ref: String,
    pub warnings: Vec<String>,
    /// Claude Code client-side API failure notices omitted before canonical import.
    pub dropped_api_error_messages: usize,
    /// Present for OpenCode, whose magic-context rows use source message coordinates.
    pub coordinate_map: Option<CoordinateMap>,
}

/// Conversion failed because the selected source session contains no replayable history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoImportableContentError;

impl fmt::Display for NoImportableContentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("session has no importable content")
    }
}

impl Error for NoImportableContentError {}

/// Common boundary implemented by each harness container reader.
///
/// Implementations enumerate lightweight metadata and convert exactly one selected session.
/// Readers for tree-shaped containers must resolve the path to the latest leaf and exclude
/// sidechains rather than flattening every branch into one conversation.
pub trait HarnessReader {
    fn harness(&self) -> &'static str;
    fn enumerate(&self) -> Result<Vec<SessionSummary>, Box<dyn Error + Send + Sync>>;
    fn convert(&self, session_id: &str) -> Result<ImportedSession, Box<dyn Error + Send + Sync>>;
    /// Return the selected session's project directory without listing every session.
    ///
    /// Readers that do not store a directory may return `None`; callers then use their own
    /// current directory. Implementations should keep this lookup scoped to `session_id`.
    fn session_cwd(
        &self,
        _session_id: &str,
    ) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
        Ok(None)
    }
}

pub(crate) fn finalize_import(
    mut messages: Vec<Message>,
    source_ref: String,
    mut warnings: Vec<String>,
    missing_id_prefix: &str,
) -> Result<ImportedSession, Box<dyn Error + Send + Sync>> {
    normalize_tool_blocks_and_ids(&mut messages, &mut warnings, missing_id_prefix);
    if messages.is_empty() {
        return Err(NoImportableContentError.into());
    }
    Ok(ImportedSession {
        messages,
        source_ref,
        warnings,
        dropped_api_error_messages: 0,
        coordinate_map: None,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OriginMessageCoordinate {
    pub ordinal: u64,
    pub message_id: String,
    pub date: Option<String>,
}

pub(crate) fn build_coordinate_map(
    messages: &[Message],
    block_origins: &[OriginMessageCoordinate],
    source_messages: &[OriginMessageCoordinate],
) -> Result<CoordinateMap, Box<dyn Error + Send + Sync>> {
    let block_count = messages
        .iter()
        .map(|message| message.content.len())
        .sum::<usize>();
    if block_count != block_origins.len() {
        return Err(format!(
            "OpenCode coordinate provenance has {} blocks for {block_count} converted blocks",
            block_origins.len()
        )
        .into());
    }

    let mut provenance = block_origins.iter().cloned().map(Some);
    let mut tracked = Vec::with_capacity(messages.len());
    for message in messages {
        let blocks = message
            .content
            .iter()
            .cloned()
            .map(|block| (block, provenance.next().flatten()))
            .collect();
        tracked.push((message.role, blocks));
    }

    // Mirror broca-core's session.import normalization so emitted ids point at the durable
    // prompt, including ordinal shifts caused by synthesized interrupted tool results.
    let normalized = normalize_for_session_import(tracked);
    let imported_block_counts = normalized
        .iter()
        .map(|(_, blocks)| blocks.len())
        .collect::<Vec<_>>();
    // One source row can become an assistant message followed by a tool-result message. Preserve
    // both edges so an imported compartment includes the whole source row while anchoring its end.
    let mut direct = HashMap::<u64, ImportedBlockRange>::new();
    for (message_ordinal, (_, blocks)) in normalized.iter().enumerate() {
        for (block_index, (_, origin)) in blocks.iter().enumerate() {
            if let Some(origin) = origin {
                let coordinate = ImportedBlockCoordinate {
                    message_ordinal: message_ordinal as u64,
                    block_index,
                };
                direct
                    .entry(origin.ordinal)
                    .and_modify(|range| range.end = Some(coordinate.clone()))
                    .or_insert(ImportedBlockRange {
                        start: Some(coordinate.clone()),
                        end: Some(coordinate),
                    });
            }
        }
    }

    let mut next_ranges = vec![None; source_messages.len()];
    let mut next = None;
    for (index, source) in source_messages.iter().enumerate().rev() {
        if let Some(range) = direct.get(&source.ordinal) {
            next = Some(range.clone());
        }
        next_ranges[index] = next.clone();
    }

    let mut by_ordinal = HashMap::new();
    let mut by_message_id = HashMap::new();
    let mut dates_by_ordinal = HashMap::new();
    let mut dates_by_message_id = HashMap::new();
    let mut prior = None;
    for (index, source) in source_messages.iter().enumerate() {
        let range = if let Some(range) = direct.get(&source.ordinal) {
            prior = range.end.clone();
            Some(range.clone())
        } else {
            // Starts move toward the next surviving block and ends toward the previous one.
            // A fully dropped span therefore has inverted edges; the optional compartment-state
            // import can skip it rather than treating it as history on either side.
            match (next_ranges[index].as_ref(), prior.as_ref()) {
                (Some(next), Some(previous)) => Some(ImportedBlockRange {
                    start: next.start.clone(),
                    end: Some(previous.clone()),
                }),
                (Some(next), None) => Some(ImportedBlockRange {
                    start: next.start.clone(),
                    end: None,
                }),
                (None, Some(previous)) => Some(ImportedBlockRange {
                    // The sentinel is just past imported messages. A trailing dropped span has no
                    // forward block for its start, so this start uses the sentinel and its end
                    // remains anchored to the previous surviving block.
                    start: Some(ImportedBlockCoordinate {
                        message_ordinal: imported_block_counts.len() as u64,
                        block_index: 0,
                    }),
                    end: Some(previous.clone()),
                }),
                (None, None) => None,
            }
        };
        if let Some(range) = range {
            by_ordinal.insert(source.ordinal, range.clone());
            by_message_id.insert(source.message_id.clone(), range);
        }
        if let Some(date) = &source.date {
            dates_by_ordinal.insert(source.ordinal, date.clone());
            dates_by_message_id.insert(source.message_id.clone(), date.clone());
        }
    }

    Ok(CoordinateMap {
        by_ordinal,
        by_message_id,
        dates_by_ordinal,
        dates_by_message_id,
        imported_block_counts,
    })
}

type TrackedBlock = (ContentBlock, Option<OriginMessageCoordinate>);
type TrackedMessage = (Role, Vec<TrackedBlock>);

fn normalize_for_session_import(messages: Vec<TrackedMessage>) -> Vec<TrackedMessage> {
    let mut normalized = Vec::with_capacity(messages.len());
    let mut pending = Vec::new();
    let mut pending_counts = HashMap::new();

    for (role, blocks) in messages {
        if role == Role::System {
            continue;
        }
        if role != Role::Tool {
            flush_tracked_pending(&mut normalized, &mut pending, &mut pending_counts);
        }

        let mut retained = Vec::with_capacity(blocks.len());
        for (block, origin) in blocks {
            if let ContentBlock::ToolResult(result) = &block {
                let Some(count) = pending_counts.get_mut(&result.tool_call_id) else {
                    continue;
                };
                *count -= 1;
                if *count == 0 {
                    pending_counts.remove(&result.tool_call_id);
                }
                if let Some(index) = pending.iter().position(|id| id == &result.tool_call_id) {
                    pending.remove(index);
                }
            }
            if let ContentBlock::ToolCall(call) = &block {
                *pending_counts
                    .entry(call.tool_call_id.clone())
                    .or_insert(0usize) += 1;
                pending.push(call.tool_call_id.clone());
            }
            retained.push((block, origin));
        }
        if !retained.is_empty() {
            normalized.push((role, retained));
        }
    }
    flush_tracked_pending(&mut normalized, &mut pending, &mut pending_counts);
    normalized
}

fn flush_tracked_pending(
    messages: &mut Vec<TrackedMessage>,
    pending: &mut Vec<ToolCallId>,
    pending_counts: &mut HashMap<ToolCallId, usize>,
) {
    if pending.is_empty() {
        return;
    }
    let blocks = pending
        .drain(..)
        .map(|tool_call_id| {
            (
                ContentBlock::ToolResult(broca_protocol::ToolResult {
                    tool_call_id,
                    output: broca_protocol::ToolOutput::Error {
                        message: "tool not run: interrupted before dispatch".into(),
                    },
                    is_error: true,
                }),
                None,
            )
        })
        .collect();
    pending_counts.clear();
    messages.push((Role::Tool, blocks));
}

fn normalize_tool_blocks_and_ids(
    messages: &mut Vec<Message>,
    warnings: &mut Vec<String>,
    missing_id_prefix: &str,
) {
    let reserved_ids: HashSet<String> = messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) if !call.tool_call_id.0.trim().is_empty() => {
                Some(call.tool_call_id.0.clone())
            }
            _ => None,
        })
        .collect();
    let mut rehomed = 0usize;

    // Establish the canonical role boundaries before pairing results. The core import pass uses
    // these boundaries to interrupt pending calls, so assigning ids against the source roles could
    // make a later result refer to a call that core has already closed as interrupted.
    let source_messages = std::mem::take(messages);
    for message in source_messages {
        let source_role = message.role;
        let origin = message.origin;
        let mut grouped_role = None;
        let mut grouped = Vec::new();
        for block in message.content {
            let block_role = match block {
                ContentBlock::ToolCall(_) => Role::Assistant,
                ContentBlock::ToolResult(_) => Role::Tool,
                _ => source_role,
            };
            if matches!(block, ContentBlock::ToolCall(_)) && source_role != Role::Assistant {
                rehomed += 1;
            }
            if grouped_role.is_some_and(|role| role != block_role) {
                push_group(
                    messages,
                    grouped_role.take().unwrap(),
                    origin.clone(),
                    &mut grouped,
                );
            }
            grouped_role = Some(block_role);
            grouped.push(block);
        }
        if let Some(role) = grouped_role {
            push_group(messages, role, origin, &mut grouped);
        }
    }

    let mut used_ids = HashSet::new();
    let mut occurrences: HashMap<String, usize> = HashMap::new();
    let mut pending_ids: HashMap<String, VecDeque<ToolCallId>> = HashMap::new();
    let mut missing = 0usize;
    let mut duplicate = 0usize;

    for message in messages.iter_mut() {
        // System messages are discarded by the core import normalizer without ending a pending
        // tool turn. Every other non-tool message is the same boundary where core synthesizes
        // interrupted results, so aliases from that prior turn must no longer be reachable.
        if message.role != Role::Tool && message.role != Role::System {
            pending_ids.clear();
        }
        for block in &mut message.content {
            match block {
                ContentBlock::ToolCall(call) => {
                    let original = call.tool_call_id.0.trim().to_string();
                    let occurrence = occurrences.entry(original.clone()).or_default();
                    *occurrence += 1;
                    let assigned = if original.is_empty() {
                        missing += 1;
                        unique_generated_id(
                            &format!("{missing_id_prefix}-missing-tool-id-{missing}"),
                            &reserved_ids,
                            &mut used_ids,
                        )
                    } else if used_ids.insert(original.clone()) {
                        original.clone()
                    } else {
                        duplicate += 1;
                        unique_generated_id(
                            &format!("{original}-{occurrence}"),
                            &reserved_ids,
                            &mut used_ids,
                        )
                    };
                    call.tool_call_id = ToolCallId(assigned.clone());
                    pending_ids
                        .entry(original)
                        .or_default()
                        .push_back(ToolCallId(assigned));
                }
                ContentBlock::ToolResult(result) => {
                    let original = result.tool_call_id.0.trim();
                    if let Some(assigned) =
                        pending_ids.get_mut(original).and_then(VecDeque::pop_front)
                    {
                        result.tool_call_id = assigned;
                    }
                }
                _ => {}
            }
        }
    }

    if rehomed > 0 {
        warnings.push(format!(
            "Re-homed {rehomed} tool call block(s) from non-assistant source messages so provider tool pairing remains valid"
        ));
    }
    if missing > 0 || duplicate > 0 {
        warnings.push(format!(
            "Assigned deterministic unique ids to {missing} missing and {duplicate} duplicate tool call id(s)"
        ));
    }
}

fn unique_generated_id(
    candidate: &str,
    reserved_ids: &HashSet<String>,
    used_ids: &mut HashSet<String>,
) -> String {
    let mut suffix = 1usize;
    let mut value = candidate.to_string();
    while reserved_ids.contains(&value) || !used_ids.insert(value.clone()) {
        suffix += 1;
        value = format!("{candidate}-{suffix}");
    }
    value
}

fn push_group(
    messages: &mut Vec<Message>,
    role: Role,
    origin: Option<broca_protocol::ModelHandle>,
    content: &mut Vec<ContentBlock>,
) {
    if content.is_empty() {
        return;
    }
    let content = std::mem::take(content);
    let message = if role == Role::Assistant {
        match origin {
            Some(model) => Message::assistant_from(content, model),
            None => Message::new(Role::Assistant, content),
        }
    } else {
        Message::new(role, content)
    };
    messages.push(message);
}

/// Size and character counts for the compact JSON encoding of canonical messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SerializedMessageMetrics {
    /// Number of bytes in the compact JSON encoding.
    pub byte_count: usize,
    /// Number of Unicode scalar values in the compact JSON encoding.
    pub char_count: u64,
}

impl SerializedMessageMetrics {
    /// Estimate tokens by dividing the encoded character count by four, rounded up.
    pub fn estimated_tokens(self) -> u64 {
        self.char_count.div_ceil(4)
    }
}

#[derive(Default)]
struct JsonMetricsWriter {
    byte_count: usize,
    char_count: u64,
}

impl Write for JsonMetricsWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.byte_count += bytes.len();
        // serde_json emits valid UTF-8. Each Unicode scalar has exactly one byte that is not a
        // continuation byte, even when one scalar's bytes arrive in separate writes.
        self.char_count += bytes.iter().filter(|byte| (**byte & 0xc0) != 0x80).count() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn estimate_from_metrics(metrics: Result<SerializedMessageMetrics, serde_json::Error>) -> u64 {
    metrics
        .map(SerializedMessageMetrics::estimated_tokens)
        .unwrap_or(0)
}

/// Measure the compact JSON encoding without retaining a second full transcript buffer.
pub fn measure_serialized_messages(
    messages: &[Message],
) -> Result<SerializedMessageMetrics, serde_json::Error> {
    let mut writer = JsonMetricsWriter::default();
    serde_json::to_writer(&mut writer, messages)?;
    Ok(SerializedMessageMetrics {
        byte_count: writer.byte_count,
        char_count: writer.char_count,
    })
}

/// Estimate tokens using the deliberately simple UTF-8 character-count / 4 heuristic.
///
/// This estimate is only for the import-time context-fill warning, not billing or admission.
/// Keep-tail boundary placement is deferred to the MC-data orchestration slice and is not
/// performed by this conversion crate.
pub fn estimate_tokens(messages: &[Message]) -> u64 {
    estimate_from_metrics(measure_serialized_messages(messages))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Cursor;

    use broca_protocol::{
        ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
    };

    use super::{
        build_coordinate_map, check_session_source_size, estimate_from_metrics, estimate_tokens,
        finalize_import, measure_serialized_messages, normalize_for_session_import,
        normalize_tool_blocks_and_ids, read_bounded_jsonl_line, NoImportableContentError,
        OriginMessageCoordinate, MAX_JSONL_LINE_BYTES, MAX_SESSION_SOURCE_BYTES,
    };

    #[test]
    fn jsonl_reader_rejects_oversized_records_before_buffering_them() {
        let bytes = vec![b'x'; MAX_JSONL_LINE_BYTES + 1];
        let mut reader = Cursor::new(bytes);
        let mut line = Vec::new();
        let mut bytes_read = 0;

        let error = read_bounded_jsonl_line(&mut reader, &mut line, &mut bytes_read).unwrap_err();

        assert!(error.to_string().contains("line limit"));
        assert!(line.len() <= MAX_JSONL_LINE_BYTES);
        assert!(bytes_read <= MAX_JSONL_LINE_BYTES as u64);
    }

    #[test]
    fn jsonl_reader_refuses_bytes_after_the_source_limit() {
        let mut reader = Cursor::new(b"x".to_vec());
        let mut line = Vec::new();
        let mut bytes_read = MAX_SESSION_SOURCE_BYTES;

        let error = read_bounded_jsonl_line(&mut reader, &mut line, &mut bytes_read).unwrap_err();

        assert!(error.to_string().contains("source session exceeds"));
        assert!(line.is_empty());
        assert_eq!(bytes_read, MAX_SESSION_SOURCE_BYTES);
    }

    #[test]
    fn session_source_size_is_rejected_before_reading_contents() {
        let path = broca_test_support::scratch_root()
            .join(format!("ck-import-source-cap-{}.jsonl", std::process::id()));
        let file = File::create(&path).unwrap();
        file.set_len(MAX_SESSION_SOURCE_BYTES + 1).unwrap();
        drop(file);

        let error = check_session_source_size(&path).unwrap_err();

        assert!(error.to_string().contains("import reader limit"));
        let _ = std::fs::remove_file(path);
    }

    fn tool_call(id: &str, name: &str) -> ContentBlock {
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId(id.into()),
            tool_name: name.into(),
            input: serde_json::Value::Null,
            thought_signature: None,
        })
    }

    fn tool_result(id: &str, text: &str) -> ContentBlock {
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: text.into() },
            is_error: false,
        })
    }

    fn through_core_pairing(messages: Vec<Message>) -> Vec<Message> {
        let tracked = messages
            .into_iter()
            .map(|message| {
                (
                    message.role,
                    message
                        .content
                        .into_iter()
                        .map(|block| (block, None))
                        .collect(),
                )
            })
            .collect();
        normalize_for_session_import(tracked)
            .into_iter()
            .map(|(role, blocks)| {
                Message::new(role, blocks.into_iter().map(|(block, _)| block).collect())
            })
            .collect()
    }

    fn retained_results(messages: &[Message]) -> Vec<(String, bool, String)> {
        messages
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|block| match block {
                ContentBlock::ToolResult(result) => {
                    let text = match &result.output {
                        ToolOutput::Parts { parts } => broca_protocol::content::media_parts_text(
                            parts,
                            broca_protocol::MediaMarkerReason::NotRequested,
                        ),
                        ToolOutput::Text { text } => text.clone(),
                        ToolOutput::Error { message } => message.clone(),
                        ToolOutput::Json { value } => value.to_string(),
                        ToolOutput::Denied { reason } => reason.clone(),
                    };
                    Some((result.tool_call_id.0.clone(), result.is_error, text))
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn estimate_uses_rounded_up_chars_over_four() {
        let messages = vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: "x".into() }],
        )];
        let encoded_chars = serde_json::to_string(&messages).unwrap().chars().count() as u64;
        assert_eq!(estimate_tokens(&messages), encoded_chars.div_ceil(4));
    }

    #[test]
    fn serialized_metrics_match_owned_json_for_unicode_and_nested_tools() {
        let messages = vec![
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "café 東京 🙂 𐍈".into(),
                }],
            ),
            Message::new(
                Role::Assistant,
                vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("call-unicode".into()),
                    tool_name: "search".into(),
                    input: serde_json::json!({
                        "query": "crab 🦀",
                        "nested": { "items": [1, true, null, "é"] }
                    }),
                    thought_signature: None,
                })],
            ),
        ];
        let encoded = serde_json::to_vec(&messages).unwrap();
        let encoded_chars = serde_json::to_string(&messages).unwrap().chars().count() as u64;

        let metrics = measure_serialized_messages(&messages).unwrap();

        assert_eq!(metrics.byte_count, encoded.len());
        assert_eq!(metrics.char_count, encoded_chars);
        assert_eq!(estimate_tokens(&messages), encoded_chars.div_ceil(4));
    }

    #[test]
    fn serialization_failure_still_estimates_zero() {
        struct RejectWrites;

        impl std::io::Write for RejectWrites {
            fn write(&mut self, _bytes: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("intentional write failure"))
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let error = serde_json::to_writer(RejectWrites, &["message"]).unwrap_err();
        assert_eq!(estimate_from_metrics(Err(error)), 0);
    }

    /// Re-homing splits a message by role without merging or reordering what it keeps.
    ///
    /// The pass rewrites message boundaries, which is exactly where block structure gets lost:
    /// two text blocks fused into one let a construct opened in the first close on a delimiter
    /// in the second, and text moved across a tool call turns a preamble into commentary on a
    /// result it was written ahead of. Both produce well-formed output that says something the
    /// source did not, so neither shows up as a parse failure downstream.
    #[test]
    fn rehoming_preserves_block_order_and_boundaries_within_each_role() {
        let mut messages = vec![Message::new(
            Role::User,
            vec![
                ContentBlock::Text {
                    text: "first paragraph".into(),
                },
                ContentBlock::Text {
                    text: "second paragraph".into(),
                },
                tool_call("c1", "read"),
                ContentBlock::Text {
                    text: "trailing note".into(),
                },
            ],
        )];

        normalize_tool_blocks_and_ids(&mut messages, &mut Vec::new(), "test");

        // The call is re-homed to its own assistant message, and the text around it keeps both
        // its grouping and its sequence rather than being pooled into one block or one message.
        let roles: Vec<Role> = messages.iter().map(|m| m.role).collect();
        assert_eq!(
            roles,
            vec![Role::User, Role::Assistant, Role::User],
            "the tool call must split the run of text rather than being hoisted past it"
        );
        assert_eq!(
            messages[0].content,
            vec![
                ContentBlock::Text {
                    text: "first paragraph".into()
                },
                ContentBlock::Text {
                    text: "second paragraph".into()
                },
            ],
            "adjacent text blocks must stay separate through the split"
        );
        assert_eq!(
            messages[2].content,
            vec![ContentBlock::Text {
                text: "trailing note".into()
            }],
            "text after the call must stay after it"
        );
    }

    #[test]
    fn every_reader_rehomes_calls_and_uniquifies_missing_and_duplicate_ids() {
        for prefix in ["cc", "codex", "goose", "opencode", "pi"] {
            let call = |id: &str, name: &str| {
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId(id.into()),
                    tool_name: name.into(),
                    input: serde_json::Value::Null,
                    thought_signature: None,
                })
            };
            let source = Message::new(
                Role::User,
                vec![
                    ContentBlock::Text {
                        text: "prompt".into(),
                    },
                    call("duplicate", "one"),
                    call("duplicate", "two"),
                    call("", "three"),
                    call("", "four"),
                    ContentBlock::ToolResult(ToolResult {
                        tool_call_id: ToolCallId("duplicate".into()),
                        output: ToolOutput::Text {
                            text: "done".into(),
                        },
                        is_error: false,
                    }),
                ],
            );
            let imported = finalize_import(
                vec![source],
                format!("{prefix}:fixture"),
                Vec::new(),
                prefix,
            )
            .unwrap();
            assert_eq!(
                imported
                    .messages
                    .iter()
                    .map(|message| message.role)
                    .collect::<Vec<_>>(),
                vec![Role::User, Role::Assistant, Role::Tool]
            );
            let calls = imported.messages[1]
                .content
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::ToolCall(call) => Some(call.tool_call_id.0.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(calls[0], "duplicate");
            assert_eq!(calls[1], "duplicate-2");
            assert_eq!(calls[2], format!("{prefix}-missing-tool-id-1"));
            assert_eq!(calls[3], format!("{prefix}-missing-tool-id-2"));
            assert_eq!(
                imported.messages[2].content[0],
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("duplicate".into()),
                    output: ToolOutput::Text {
                        text: "done".into()
                    },
                    is_error: false,
                })
            );
            assert!(imported
                .warnings
                .iter()
                .any(|warning| warning.contains("Re-homed")));
            assert!(imported
                .warnings
                .iter()
                .any(|warning| warning.contains("unique ids")));
        }
    }

    #[test]
    fn reused_id_after_interruption_keeps_the_later_real_result() {
        let mut normalized = vec![
            Message::new(Role::Assistant, vec![tool_call("dup", "first")]),
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "continue".into(),
                }],
            ),
            Message::new(Role::Assistant, vec![tool_call("dup", "second")]),
            Message::new(Role::Tool, vec![tool_result("dup", "ok")]),
        ];
        normalize_tool_blocks_and_ids(&mut normalized, &mut Vec::new(), "test");

        assert_eq!(normalized[2].content[0], tool_call("dup-2", "second"));
        assert_eq!(normalized[3].content[0], tool_result("dup-2", "ok"));
        assert_eq!(
            retained_results(&through_core_pairing(normalized.clone())),
            vec![
                (
                    "dup".into(),
                    true,
                    "tool not run: interrupted before dispatch".into()
                ),
                ("dup-2".into(), false, "ok".into()),
            ]
        );

        // The former transcript-global pairing rewrote the real result to the first call's id.
        // Core had already interrupted that call, so it dropped "ok" and synthesized both errors.
        let ContentBlock::ToolResult(old_result) = &mut normalized[3].content[0] else {
            unreachable!("fixture ends in a tool result");
        };
        old_result.tool_call_id = ToolCallId("dup".into());
        assert_eq!(
            retained_results(&through_core_pairing(normalized)),
            vec![
                (
                    "dup".into(),
                    true,
                    "tool not run: interrupted before dispatch".into()
                ),
                (
                    "dup-2".into(),
                    true,
                    "tool not run: interrupted before dispatch".into()
                ),
            ]
        );
    }

    #[test]
    fn well_formed_pairing_is_byte_stable_and_reused_ids_stay_turn_scoped() {
        let mut well_formed = vec![
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "run it".into(),
                }],
            ),
            Message::new(Role::Assistant, vec![tool_call("one", "first")]),
            Message::new(Role::Tool, vec![tool_result("one", "done")]),
        ];
        let original = well_formed.clone();
        let mut warnings = Vec::new();
        normalize_tool_blocks_and_ids(&mut well_formed, &mut warnings, "test");
        assert_eq!(well_formed, original);
        assert!(warnings.is_empty());

        let mut reused = vec![
            Message::new(Role::Assistant, vec![tool_call("dup", "first")]),
            Message::new(Role::Tool, vec![tool_result("dup", "one")]),
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "again".into(),
                }],
            ),
            Message::new(Role::Assistant, vec![tool_call("dup", "second")]),
            Message::new(Role::Tool, vec![tool_result("dup", "two")]),
        ];
        normalize_tool_blocks_and_ids(&mut reused, &mut Vec::new(), "test");
        assert_eq!(reused[0].content[0], tool_call("dup", "first"));
        assert_eq!(reused[1].content[0], tool_result("dup", "one"));
        assert_eq!(reused[3].content[0], tool_call("dup-2", "second"));
        assert_eq!(reused[4].content[0], tool_result("dup-2", "two"));
        assert_eq!(through_core_pairing(reused.clone()), reused);
    }

    #[test]
    fn reused_ids_within_one_tool_turn_pair_fifo() {
        let mut messages = vec![
            Message::new(
                Role::Assistant,
                vec![tool_call("dup", "first"), tool_call("dup", "second")],
            ),
            Message::new(
                Role::Tool,
                vec![tool_result("dup", "one"), tool_result("dup", "two")],
            ),
        ];
        normalize_tool_blocks_and_ids(&mut messages, &mut Vec::new(), "test");

        assert_eq!(messages[0].content[0], tool_call("dup", "first"));
        assert_eq!(messages[0].content[1], tool_call("dup-2", "second"));
        assert_eq!(messages[1].content[0], tool_result("dup", "one"));
        assert_eq!(messages[1].content[1], tool_result("dup-2", "two"));
        assert_eq!(through_core_pairing(messages.clone()), messages);
    }

    #[test]
    fn coordinate_map_snaps_dropped_starts_forward_and_ends_backward() {
        let result = |id: &str| {
            ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId(id.into()),
                output: ToolOutput::Text {
                    text: "result".into(),
                },
                is_error: false,
            })
        };
        let messages = vec![
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "first".into(),
                }],
            ),
            Message::new(Role::Tool, vec![result("orphan-before")]),
            Message::new(
                Role::Assistant,
                vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("interrupted".into()),
                    tool_name: "work".into(),
                    input: serde_json::Value::Null,
                    thought_signature: None,
                })],
            ),
            Message::new(Role::Tool, vec![result("orphan-inside")]),
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "future".into(),
                }],
            ),
        ];
        let source = (0..5)
            .map(|ordinal| OriginMessageCoordinate {
                ordinal,
                message_id: format!("source-{ordinal}"),
                date: None,
            })
            .collect::<Vec<_>>();
        let map = build_coordinate_map(&messages, &source, &source).unwrap();

        assert_eq!(
            map.start_coordinate_for_ordinal(1).unwrap().flat_id(),
            "m1#0"
        );
        assert_eq!(map.coordinate_for_ordinal(1).unwrap().flat_id(), "m0#0");
        assert_eq!(
            map.start_coordinate_for_ordinal(2).unwrap().flat_id(),
            "m1#0"
        );
        assert_eq!(map.coordinate_for_ordinal(2).unwrap().flat_id(), "m1#0");
        assert_eq!(
            map.start_coordinate_for_ordinal(3).unwrap().flat_id(),
            "m3#0"
        );
        assert_eq!(map.coordinate_for_ordinal(3).unwrap().flat_id(), "m1#0");
        assert_eq!(
            map.start_coordinate_for_ordinal(4).unwrap().flat_id(),
            "m3#0"
        );
        assert!(map.contains_flat_id("m2#0"));
        assert!(!map.contains_flat_id("m4#0"));
    }

    #[test]
    fn coordinate_map_marks_a_trailing_dropped_start_as_unmapped() {
        let messages = vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "survives".into(),
            }],
        )];
        let origins = vec![OriginMessageCoordinate {
            ordinal: 0,
            message_id: "source-0".into(),
            date: None,
        }];
        let source = vec![
            origins[0].clone(),
            OriginMessageCoordinate {
                ordinal: 1,
                message_id: "source-1".into(),
                date: None,
            },
        ];
        let map = build_coordinate_map(&messages, &origins, &source).unwrap();

        assert_eq!(map.coordinate_for_ordinal(1).unwrap().flat_id(), "m0#0");
        assert_eq!(
            map.start_coordinate_for_ordinal(1).unwrap().message_ordinal,
            1
        );
        assert!(!map.contains_flat_id("m1#0"));
    }

    #[test]
    fn empty_conversion_has_a_typed_error() {
        let error = finalize_import(Vec::new(), "pi:empty".into(), Vec::new(), "pi").unwrap_err();
        assert!(error.downcast_ref::<NoImportableContentError>().is_some());
        assert_eq!(error.to_string(), "session has no importable content");
    }
}
