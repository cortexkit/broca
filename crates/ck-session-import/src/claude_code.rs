//! Claude Code JSONL reader.
//!
//! Claude Code stores one session per `~/.claude/projects/<escaped-project>/<uuid>.jsonl`.
//! UUID-bearing rows form a tree. Conversion follows the ancestry of the newest non-sidechain
//! leaf, while non-message rows on that path remain structural only.
//!
//! Inspection of local Claude Code containers in August 2026 found two useful drifts from the
//! basic Messages API shape: plain user prompts may use a string instead of a block array, and
//! tool results may contain strings, singleton text-block arrays, or longer block arrays. Strings
//! and singleton text blocks become canonical text output. Multi-block arrays, including an
//! observed all-text pair, remain JSON because flattening would invent a separator and erase the
//! source block boundaries.
//! Current containers also include the metadata row kinds listed in
//! [`OBSERVED_NON_MESSAGE_ROW_KINDS`]. They do not contribute conversation content.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};

use broca_protocol::{
    ContentBlock, Message, ModelHandle, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    check_session_source_size, finalize_import, omit_nested_tool_images, read_bounded_jsonl_line,
    HarnessReader, ImportedSession, SessionSummary,
};

type ReaderResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const TITLE_MAX_CHARS: usize = 80;

/// Non-message rows observed in Claude Code containers or documented by older container shapes.
///
/// `system` and `attachment` can carry UUIDs and therefore still participate in ancestry even
/// though their payloads are not imported. The remaining kinds are session/UI metadata.
pub const OBSERVED_NON_MESSAGE_ROW_KINDS: &[&str] = &[
    "ai-title",
    "attachment",
    "file-history-snapshot",
    "last-prompt",
    "mode",
    "permission-mode",
    "queue-operation",
    "summary",
    "system",
];

/// Content blocks intentionally omitted because the canonical import schema cannot safely replay
/// them. Redacted thinking is an undecryptable provider blob; top-level images have no canonical
/// content-block representation. Images nested in tool results are retained as JSON output.
pub const DROPPED_CONTENT_BLOCK_KINDS: &[&str] = &["image", "redacted_thinking"];

#[derive(Debug, Clone)]
pub struct ClaudeCodeReader {
    projects_root: PathBuf,
}

impl ClaudeCodeReader {
    pub fn new(projects_root: impl Into<PathBuf>) -> Self {
        Self {
            projects_root: projects_root.into(),
        }
    }

    pub fn default_path() -> PathBuf {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default()
            .join(".claude/projects")
    }

    pub fn projects_root(&self) -> &Path {
        &self.projects_root
    }

    fn session_files(&self) -> ReaderResult<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(&self.projects_root)? {
            let path = entry?.path();
            if path.is_dir() {
                for child in fs::read_dir(path)? {
                    let child = child?.path();
                    if child
                        .extension()
                        .is_some_and(|extension| extension == "jsonl")
                    {
                        files.push(child);
                    }
                }
            } else if path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
            {
                files.push(path);
            }
        }
        files.sort();
        Ok(files)
    }

    fn find_session(&self, session_id: &str) -> ReaderResult<PathBuf> {
        let matches: Vec<_> = self
            .session_files()?
            .into_iter()
            .filter(|path| session_id_for(path) == session_id)
            .collect();
        match matches.as_slice() {
            [path] => Ok(path.clone()),
            [] => Err(IoError::new(
                ErrorKind::NotFound,
                format!("Claude Code session '{session_id}' was not found"),
            )
            .into()),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                format!("Claude Code session id '{session_id}' is not unique"),
            )
            .into()),
        }
    }
}

impl Default for ClaudeCodeReader {
    fn default() -> Self {
        Self::new(Self::default_path())
    }
}

impl HarnessReader for ClaudeCodeReader {
    fn harness(&self) -> &'static str {
        "claude-code"
    }

    fn enumerate(&self) -> ReaderResult<Vec<SessionSummary>> {
        let mut sessions = Vec::new();
        for path in self.session_files()? {
            let rows = match load_summary_rows(&path) {
                Ok(rows) => rows,
                Err(error) => {
                    eprintln!(
                        "WARNING: skipped unreadable Claude Code session {} during enumeration: {error}",
                        path.display()
                    );
                    continue;
                }
            };
            let selected = match select_latest_branch(&rows) {
                Ok(selected) => selected,
                Err(error) => {
                    eprintln!(
                        "WARNING: skipped malformed Claude Code session {} during enumeration: {error}",
                        path.display()
                    );
                    continue;
                }
            };
            let title = selected
                .iter()
                .filter(|index| !rows[**index].is_api_error)
                .find_map(|index| rows[*index].title.as_deref())
                .map(str::to_string);
            let cwd = selected
                .iter()
                .rev()
                .find_map(|index| rows[*index].cwd.clone());
            let message_count = selected
                .iter()
                .filter(|index| {
                    !rows[**index].is_api_error
                        && matches!(rows[**index].kind.as_str(), "user" | "assistant")
                })
                .count();
            let latest_timestamp = selected
                .last()
                .map(|index| rows[*index].timestamp.clone())
                .unwrap_or_default();
            sessions.push((
                latest_timestamp,
                SessionSummary {
                    id: session_id_for(&path),
                    title,
                    cwd,
                    message_count,
                    approximate_size_bytes: Some(fs::metadata(path)?.len()),
                },
            ));
        }
        sessions.sort_by(|(left_time, left), (right_time, right)| {
            right_time
                .cmp(left_time)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(sessions.into_iter().map(|(_, summary)| summary).collect())
    }

    fn convert(&self, session_id: &str) -> ReaderResult<ImportedSession> {
        let path = self.find_session(session_id)?;
        let rows = load_rows(&path)?;
        let selected = select_latest_branch(&rows)?;
        let mut messages = Vec::new();
        let mut dropped_system = 0usize;
        let mut dropped_api_errors = 0usize;
        let mut dropped_redacted = 0usize;
        let mut omitted_images = 0usize;
        let mut skipped_blocks = BTreeSet::new();
        let mut skipped_rows = BTreeSet::new();

        for row in &rows {
            let kind = row
                .value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("<missing>");
            if !matches!(kind, "user" | "assistant") {
                skipped_rows.insert(kind.to_string());
            }
        }

        let mut previous_assistant_id = None;
        for index in selected {
            let row = &rows[index].value;
            if is_api_error_message(row) {
                previous_assistant_id = None;
                dropped_api_errors += 1;
                continue;
            }
            let kind = row.get("type").and_then(Value::as_str).unwrap_or("");
            // A streamed API response has one transcript row per content block, with distinct
            // row UUIDs but the same message.id. Only adjacent assistant fragments belong together;
            // a user/tool-result row must retain its position between assistant messages.
            let assistant_id = (kind == "assistant")
                .then(|| row.get("message")?.get("id")?.as_str())
                .flatten()
                .filter(|id| !id.is_empty());
            let join_previous_assistant =
                assistant_id.is_some() && assistant_id == previous_assistant_id;
            previous_assistant_id = assistant_id;
            let message_count = messages.len();
            match kind {
                "system" => dropped_system += 1,
                "user" | "assistant" => convert_message_row(
                    row,
                    &mut messages,
                    &mut dropped_redacted,
                    &mut omitted_images,
                    &mut skipped_blocks,
                    join_previous_assistant,
                ),
                _ => {}
            }
            if !join_previous_assistant && messages.len() == message_count {
                // A wholly omitted first fragment owns no output to extend. The last emitted
                // assistant message may belong to a different API response.
                previous_assistant_id = None;
            }
        }

        let mut warnings = Vec::new();
        if dropped_system > 0 {
            warnings.push(format!(
                "Dropped {dropped_system} Claude Code system row(s)"
            ));
        }
        if dropped_api_errors > 0 {
            warnings.push(format!(
                "Dropped {dropped_api_errors} Claude Code API error message row(s)"
            ));
        }
        if dropped_redacted > 0 {
            warnings.push(format!(
                "Dropped {dropped_redacted} Claude Code redacted_thinking block(s); foreign encrypted reasoning cannot be replayed"
            ));
        }
        if omitted_images > 0 {
            warnings.push(format!(
                "Omitted {omitted_images} unsupported Claude Code tool-result image block(s)"
            ));
        }
        if !skipped_blocks.is_empty() {
            warnings.push(format!(
                "Skipped unsupported Claude Code content block kinds: {}",
                skipped_blocks.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }
        if !skipped_rows.is_empty() {
            warnings.push(format!(
                "Skipped Claude Code non-message row kinds: {}",
                skipped_rows.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }

        let mut imported = finalize_import(messages, format!("cc:{session_id}"), warnings, "cc")?;
        imported.dropped_api_error_messages = dropped_api_errors;
        Ok(imported)
    }

    fn session_cwd(&self, session_id: &str) -> ReaderResult<Option<String>> {
        let path = self.find_session(session_id)?;
        let rows = load_summary_rows(&path)?;
        let selected = select_latest_branch(&rows)?;
        Ok(selected
            .iter()
            .rev()
            .find_map(|index| rows[*index].cwd.clone()))
    }
}

/// Claude Code sets this producer flag for client-side failure notices. Model metadata does not
/// narrow the rule: a flagged row remains client output even if a future version attaches the
/// model that the failed request targeted.
fn is_api_error_message(row: &Value) -> bool {
    row.get("isApiErrorMessage").and_then(Value::as_bool) == Some(true)
}

#[derive(Debug)]
struct SourceRow {
    value: Value,
    uuid: Option<String>,
    parent_uuid: Option<String>,
    timestamp: String,
    order: usize,
    sidechain: bool,
    message_fingerprint: [u8; 32],
    kind: String,
    is_api_error: bool,
    title: Option<String>,
    cwd: Option<String>,
}

fn load_rows(path: &Path) -> ReaderResult<Vec<SourceRow>> {
    load_rows_with_values(path, true)
}

fn load_summary_rows(path: &Path) -> ReaderResult<Vec<SourceRow>> {
    load_rows_with_values(path, false)
}

fn load_rows_with_values(path: &Path, retain_values: bool) -> ReaderResult<Vec<SourceRow>> {
    check_session_source_size(path)?;
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut bytes_read = 0;
    let mut line_index = 0;
    let mut rows = Vec::new();
    while read_bounded_jsonl_line(&mut reader, &mut line, &mut bytes_read).map_err(|error| {
        IoError::new(
            ErrorKind::InvalidData,
            format!(
                "read Claude Code session {} line {}: {error}",
                path.display(),
                line_index + 1
            ),
        )
    })? {
        if line.iter().all(u8::is_ascii_whitespace) {
            line_index += 1;
            continue;
        }
        let value: Value = serde_json::from_slice(&line).map_err(|error| {
            IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "decode Claude Code session {} line {}: {error}",
                    path.display(),
                    line_index + 1
                ),
            )
        })?;
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let title = (!retain_values && kind == "user")
            .then(|| first_user_text(&value))
            .flatten()
            .map(|title| truncate_title(&title));
        let cwd = (!retain_values)
            .then(|| value.get("cwd").and_then(Value::as_str))
            .flatten()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string);
        let is_api_error = value.get("isApiErrorMessage").and_then(Value::as_bool) == Some(true);
        let message_fingerprint = fingerprint_message(value.get("message"));
        let uuid = value
            .get("uuid")
            .and_then(Value::as_str)
            .map(str::to_string);
        let parent_uuid = value
            .get("parentUuid")
            .and_then(Value::as_str)
            .map(str::to_string);
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let sidechain = value
            .get("isSidechain")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let value = if retain_values { value } else { Value::Null };
        rows.push(SourceRow {
            value,
            uuid,
            parent_uuid,
            timestamp,
            sidechain,
            order: line_index,
            message_fingerprint,
            kind,
            is_api_error,
            title,
            cwd,
        });
        line_index += 1;
    }
    Ok(rows)
}

fn fingerprint_message(message: Option<&Value>) -> [u8; 32] {
    let bytes = serde_json::to_vec(&message).expect("a JSON value always serializes");
    Sha256::digest(bytes).into()
}

fn select_latest_branch(rows: &[SourceRow]) -> ReaderResult<Vec<usize>> {
    let mut by_uuid: HashMap<&str, usize> = HashMap::new();
    for (index, row) in rows.iter().enumerate().filter(|(_, row)| !row.sidechain) {
        if let Some(uuid) = row.uuid.as_deref() {
            if let Some(previous_index) = by_uuid.get(uuid).copied() {
                let previous = &rows[previous_index];
                // Only the complete message fingerprint and timestamp establish rewrite identity;
                // row metadata such as slug and parentUuid is expected to change.
                if previous.message_fingerprint != row.message_fingerprint
                    || previous.timestamp != row.timestamp
                {
                    return Err(IoError::new(
                        ErrorKind::InvalidData,
                        format!("conflicting Claude Code row uuid '{uuid}'"),
                    )
                    .into());
                }
            }
            // Claude Code appends metadata rewrites. The appended row must win because a
            // parentUuid rewrite records the branch structure produced by compaction.
            by_uuid.insert(uuid, index);
        }
    }

    let parent_ids: HashSet<&str> = by_uuid
        .values()
        .filter_map(|index| rows[*index].parent_uuid.as_deref())
        .collect();
    let leaf = by_uuid
        .values()
        .copied()
        .filter(|index| {
            rows[*index]
                .uuid
                .as_deref()
                .is_some_and(|uuid| !parent_ids.contains(uuid))
        })
        .max_by(|left, right| {
            rows[*left]
                .timestamp
                .cmp(&rows[*right].timestamp)
                .then_with(|| rows[*left].order.cmp(&rows[*right].order))
        });
    let Some(mut current) = leaf else {
        return Ok(Vec::new());
    };

    let mut branch = Vec::new();
    let mut visited = HashSet::new();
    loop {
        let uuid = rows[current].uuid.as_deref().unwrap_or_default();
        if !visited.insert(uuid) {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!("cycle in Claude Code session tree at row '{uuid}'"),
            )
            .into());
        }
        branch.push(current);
        let Some(parent_uuid) = rows[current].parent_uuid.as_deref() else {
            break;
        };
        let Some(parent) = by_uuid.get(parent_uuid) else {
            break;
        };
        current = *parent;
    }
    branch.reverse();
    Ok(branch)
}

fn convert_message_row(
    row: &Value,
    messages: &mut Vec<Message>,
    dropped_redacted: &mut usize,
    omitted_images: &mut usize,
    skipped_blocks: &mut BTreeSet<String>,
    join_previous_assistant: bool,
) {
    let source_role = match row.get("type").and_then(Value::as_str) {
        Some("user") => Role::User,
        Some("assistant") => Role::Assistant,
        _ => return,
    };
    let Some(message) = row.get("message").and_then(Value::as_object) else {
        return;
    };
    if message.get("role").and_then(Value::as_str) == Some("system") {
        return;
    }
    let origin = (source_role == Role::Assistant)
        .then(|| assistant_origin(row, message))
        .flatten();
    let Some(content) = message.get("content") else {
        return;
    };

    if let Some(text) = content.as_str() {
        push_grouped_message(
            messages,
            source_role,
            origin,
            vec![ContentBlock::Text {
                text: text.to_string(),
            }],
            join_previous_assistant,
        );
        return;
    }
    let Some(blocks) = content.as_array() else {
        skipped_blocks.insert(format!("<{}>", json_kind(content)));
        return;
    };

    let mut pending_role = None;
    let mut pending = Vec::new();
    for block in blocks {
        let Some(converted) =
            convert_block(block, dropped_redacted, omitted_images, skipped_blocks)
        else {
            continue;
        };
        let role = if matches!(converted, ContentBlock::ToolResult(_)) {
            Role::Tool
        } else {
            source_role
        };
        if pending_role.is_some_and(|pending_role| pending_role != role) {
            push_grouped_message(
                messages,
                pending_role.unwrap(),
                origin.clone(),
                pending,
                join_previous_assistant,
            );
            pending = Vec::new();
        }
        pending_role = Some(role);
        pending.push(converted);
    }
    if let Some(role) = pending_role {
        push_grouped_message(messages, role, origin, pending, join_previous_assistant);
    }
}

fn singleton_text_block(value: &Value) -> Option<&str> {
    let blocks = value.as_array()?;
    let [block] = blocks.as_slice() else {
        return None;
    };
    if block.get("type").and_then(Value::as_str) != Some("text") {
        return None;
    }
    block.get("text").and_then(Value::as_str)
}

fn convert_block(
    block: &Value,
    dropped_redacted: &mut usize,
    omitted_images: &mut usize,
    skipped_blocks: &mut BTreeSet<String>,
) -> Option<ContentBlock> {
    let kind = block.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "text" => block
            .get("text")
            .and_then(Value::as_str)
            .map(|text| ContentBlock::Text {
                text: text.to_string(),
            }),
        "thinking" => {
            block
                .get("thinking")
                .and_then(Value::as_str)
                .map(|text| ContentBlock::Reasoning {
                    text: text.to_string(),
                    signature: None,
                })
        }
        "redacted_thinking" => {
            *dropped_redacted += 1;
            None
        }
        "tool_use" => Some(ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId(
                block
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            ),
            tool_name: block
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            input: block.get("input").cloned().unwrap_or(Value::Null),
            thought_signature: None,
        })),
        "tool_result" => {
            let mut content = block.get("content").cloned().unwrap_or(Value::Null);
            *omitted_images += omit_nested_tool_images(&mut content);
            let output = match content {
                Value::String(text) => ToolOutput::Text { text },
                value => match singleton_text_block(&value) {
                    Some(text) => ToolOutput::Text {
                        text: text.to_string(),
                    },
                    None => ToolOutput::Json { value },
                },
            };
            Some(ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId(
                    block
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                ),
                output,
                is_error: block
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            }))
        }
        other => {
            skipped_blocks.insert(if other.is_empty() {
                "<missing>".to_string()
            } else {
                other.to_string()
            });
            None
        }
    }
}

fn push_grouped_message(
    messages: &mut Vec<Message>,
    role: Role,
    origin: Option<ModelHandle>,
    content: Vec<ContentBlock>,
    join_previous_assistant: bool,
) {
    if content.is_empty() {
        return;
    }
    if role == Role::Assistant && join_previous_assistant {
        if let Some(previous) = messages
            .last_mut()
            .filter(|last| last.role == Role::Assistant)
        {
            previous.content.extend(content);
            return;
        }
    }
    let message = if role == Role::Assistant {
        match origin {
            Some(model) => Message::assistant_from(content, model),
            None => Message::new(role, content),
        }
    } else {
        Message::new(role, content)
    };
    messages.push(message);
}

fn assistant_origin(row: &Value, message: &serde_json::Map<String, Value>) -> Option<ModelHandle> {
    message
        .get("model")
        .or_else(|| row.get("model"))
        .and_then(Value::as_str)
        .filter(|model| !model.trim().is_empty())
        .map(|model| ModelHandle::new("anthropic", model))
}

fn first_user_text(row: &Value) -> Option<String> {
    if row.get("type").and_then(Value::as_str) != Some("user") {
        return None;
    }
    let content = row.get("message")?.get("content")?;
    if let Some(text) = content.as_str() {
        return (!text.trim().is_empty()).then(|| text.to_string());
    }
    content.as_array()?.iter().find_map(|block| {
        (block.get("type").and_then(Value::as_str) == Some("text"))
            .then(|| block.get("text").and_then(Value::as_str))
            .flatten()
            .filter(|text| !text.trim().is_empty())
            .map(str::to_string)
    })
}

fn truncate_title(title: &str) -> String {
    if title.chars().count() <= TITLE_MAX_CHARS {
        return title.to_string();
    }
    let mut truncated: String = title.chars().take(TITLE_MAX_CHARS - 1).collect();
    truncated.push('…');
    truncated
}

fn session_id_for(path: &Path) -> String {
    path.file_stem()
        .map(|stem| stem.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::PathBuf;

    use crate::MAX_JSONL_LINE_BYTES;
    use broca_module_serve::wire::{ImportOriginKind, ImportOriginParams, ImportParams};
    use broca_protocol::{CallOptions, ContentBlock, Role, ToolOutput, ToolResult};
    use broca_provider::{
        families::{anthropic_frozen_config, AnthropicMessages},
        wire::WireFamily,
    };
    use serde_json::{json, Value};

    use super::{
        convert_block, load_rows, load_summary_rows, select_latest_branch, ClaudeCodeReader,
        HarnessReader, DROPPED_CONTENT_BLOCK_KINDS, OBSERVED_NON_MESSAGE_ROW_KINDS,
    };

    const TOOL_SESSION: &str = "11111111-1111-4111-8111-111111111111";
    const TEXT_SESSION: &str = "22222222-2222-4222-8222-222222222222";
    const REWRITE_SESSION: &str = "44444444-4444-4444-8444-444444444444";
    const CONFLICT_SESSION: &str = "55555555-5555-4555-8555-555555555555";
    const API_ERROR_SESSION: &str = "66666666-6666-4666-8666-666666666666";

    fn fixture_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/claude-code")
    }

    fn fixture() -> ClaudeCodeReader {
        ClaudeCodeReader::new(fixture_root())
    }

    fn rewrite_fixture() -> ClaudeCodeReader {
        ClaudeCodeReader::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/claude-code-rewrites"),
        )
    }

    fn api_error_fixture_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/claude-code-api-errors")
    }

    fn api_error_fixture() -> ClaudeCodeReader {
        ClaudeCodeReader::new(api_error_fixture_root())
    }

    fn round_trip(imported: &crate::ImportedSession, import_id: &str) {
        let params = ImportParams {
            messages: imported.messages.clone(),
            import_id: import_id.into(),
            origin: ImportOriginParams {
                kind: ImportOriginKind::Import,
                source_ref: imported.source_ref.clone(),
            },
        };
        let decoded: ImportParams =
            serde_json::from_slice(&serde_json::to_vec(&params).unwrap()).unwrap();
        assert_eq!(decoded.messages, imported.messages);
    }

    fn split_rows() -> Vec<Value> {
        load_rows(
            &PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/claude-code-split/-tmp-split-project/split.jsonl"),
        )
        .unwrap()
        .into_iter()
        .map(|row| row.value)
        .collect()
    }

    fn convert_split_rows(rows: &[Value], label: &str) -> crate::ImportedSession {
        let root = broca_test_support::scratch_root()
            .join(format!("ck-claude-split-{label}-{}", std::process::id()));
        let project = root.join("project");
        fs::create_dir_all(&project).unwrap();
        fs::write(
            project.join("split.jsonl"),
            rows.iter()
                .map(|row| serde_json::to_string(row).unwrap())
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let imported = ClaudeCodeReader::new(&root).convert("split").unwrap();
        fs::remove_dir_all(root).unwrap();
        imported
    }

    #[test]
    fn split_assistant_rows_preserve_block_order_and_strip_foreign_signatures() {
        let mut rows = split_rows();
        for (index, label) in [(1, "first"), (2, "second")] {
            let content = rows[index]["message"]["content"].as_array_mut().unwrap();
            content[0]["thought_signature"] = json!("foreign-tool-signature");
            content.insert(0, json!({"type": "text", "text": label}));
            content.insert(
                0,
                json!({"type": "thinking", "thinking": label, "signature": "foreign-thinking-signature"}),
            );
        }
        let imported = convert_split_rows(&rows, "block-order");
        assert_eq!(imported.messages.len(), 3);
        let blocks = &imported.messages[1].content;
        let ordered = blocks
            .iter()
            .map(|block| match block {
                ContentBlock::Reasoning { text, signature } => {
                    assert!(signature.is_none());
                    ("thinking", text.as_str())
                }
                ContentBlock::Text { text } => ("text", text.as_str()),
                ContentBlock::ToolCall(call) => {
                    assert!(call.thought_signature.is_none());
                    ("tool_use", call.tool_call_id.0.as_str())
                }
                other => panic!("unexpected block {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ordered,
            [
                ("thinking", "first"),
                ("text", "first"),
                ("tool_use", "toolu_A"),
                ("thinking", "second"),
                ("text", "second"),
                ("tool_use", "toolu_B"),
            ]
        );
        round_trip(&imported, "claude-split-order");
    }

    #[test]
    fn different_or_missing_api_ids_never_merge_assistant_rows() {
        for (label, id) in [
            ("different", Some(json!("msg_other"))),
            ("empty", Some(json!(""))),
            ("missing", None),
        ] {
            let mut rows = split_rows();
            if label != "different" {
                rows[1]["message"].as_object_mut().unwrap().remove("id");
            }
            let message = rows[2]["message"].as_object_mut().unwrap();
            match id {
                Some(id) => {
                    message.insert("id".into(), id);
                }
                None => {
                    message.remove("id");
                }
            }
            let imported = convert_split_rows(&rows, label);
            assert_eq!(
                imported
                    .messages
                    .iter()
                    .map(|message| message.role)
                    .collect::<Vec<_>>(),
                [Role::User, Role::Assistant, Role::Assistant, Role::Tool],
                "{label}"
            );
        }
    }

    #[test]
    fn repeated_usage_snapshots_do_not_change_imported_messages() {
        let mut rows = split_rows();
        let snapshots = convert_split_rows(&rows, "usage-snapshots");
        rows[2]["message"]["usage"] = rows[1]["message"]["usage"].clone();
        let repeated = convert_split_rows(&rows, "usage-repeated");
        for row in &mut rows {
            row["message"].as_object_mut().unwrap().remove("usage");
        }
        let without_usage = convert_split_rows(&rows, "usage-absent");
        assert_eq!(snapshots.messages, repeated.messages);
        assert_eq!(snapshots.messages, without_usage.messages);
    }

    #[test]
    fn omitted_fragment_does_not_join_a_different_api_message() {
        let mut rows = split_rows();
        rows[1]["message"]["id"] = json!("msg_other");
        let mut omitted = rows[2].clone();
        omitted["uuid"] = json!("omitted");
        omitted["message"]["content"] =
            json!([{"type": "redacted_thinking", "data": "foreign-blob"}]);
        rows[2]["parentUuid"] = json!("omitted");
        rows.insert(2, omitted);
        let imported = convert_split_rows(&rows, "omitted-fragment");
        assert_eq!(
            imported
                .messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            [Role::User, Role::Assistant, Role::Assistant, Role::Tool]
        );
    }

    #[test]
    fn split_and_joined_api_messages_convert_identically() {
        let mut rows = split_rows();
        let split = convert_split_rows(&rows, "split-control");
        let second = rows.remove(2);
        rows[1]["message"]["content"]
            .as_array_mut()
            .unwrap()
            .extend(second["message"]["content"].as_array().unwrap().clone());
        rows[2]["parentUuid"] = json!("a1");
        let joined = convert_split_rows(&rows, "joined-control");
        assert_eq!(split.messages, joined.messages);
        assert_eq!(split.warnings, joined.warnings);
    }

    fn converted_tool_result(content: Value, is_error: bool) -> ToolResult {
        let block = json!({
            "type": "tool_result",
            "tool_use_id": "toolu_test",
            "content": content,
            "is_error": is_error,
        });
        let mut dropped_redacted = 0;
        let mut omitted_images = 0;
        let mut skipped_blocks = BTreeSet::new();
        let converted = convert_block(
            &block,
            &mut dropped_redacted,
            &mut omitted_images,
            &mut skipped_blocks,
        )
        .expect("tool result should convert");
        assert_eq!(dropped_redacted, 0);
        assert_eq!(omitted_images, 0);
        assert!(skipped_blocks.is_empty());
        match converted {
            ContentBlock::ToolResult(result) => result,
            _ => panic!("tool result converted to a different content block"),
        }
    }

    #[test]
    fn singleton_text_block_tool_result_becomes_text() {
        let result =
            converted_tool_result(json!([{"type": "text", "text": "singleton output"}]), true);

        assert_eq!(
            result.output,
            ToolOutput::Text {
                text: "singleton output".into()
            }
        );
        assert!(result.is_error);
    }

    #[test]
    fn multi_block_text_tool_result_stays_json() {
        let content = json!([
            {"type": "text", "text": "first output"},
            {"type": "text", "text": "second output"}
        ]);
        let result = converted_tool_result(content.clone(), false);

        assert_eq!(result.output, ToolOutput::Json { value: content });
        assert!(!result.is_error);
    }

    #[test]
    fn tool_result_images_are_omitted_without_losing_surrounding_text() {
        let block = json!({
            "type": "tool_result",
            "tool_use_id": "toolu_image",
            "content": [
                {"type": "text", "text": "before image"},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "PRIVATE_BASE64"}},
                {"type": "text", "text": "after image"}
            ]
        });
        let mut dropped_redacted = 0;
        let mut omitted_images = 0;
        let converted = convert_block(
            &block,
            &mut dropped_redacted,
            &mut omitted_images,
            &mut BTreeSet::new(),
        )
        .expect("tool result should remain importable");
        let ContentBlock::ToolResult(result) = converted else {
            panic!("expected a tool result")
        };
        let encoded = serde_json::to_string(&result.output).unwrap();
        assert_eq!(omitted_images, 1);
        assert!(encoded.contains("before image"));
        assert!(encoded.contains("after image"));
        assert!(encoded.contains("image omitted from imported tool output"));
        assert!(!encoded.contains("PRIVATE_BASE64"));
    }

    #[test]
    fn string_tool_result_stays_text() {
        let result = converted_tool_result(json!("plain output"), false);

        assert_eq!(
            result.output,
            ToolOutput::Text {
                text: "plain output".into()
            }
        );
        assert!(!result.is_error);
    }

    #[test]
    fn enumerates_scrubbed_fixture_sessions() {
        let sessions = fixture().enumerate().unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].id, TOOL_SESSION);
        assert_eq!(
            sessions[0].title.as_deref(),
            Some("Please inspect the synthetic fixture.")
        );
        assert_eq!(sessions[0].cwd.as_deref(), Some("/tmp/scrubbed-project"));
        assert_eq!(
            fixture().session_cwd(TOOL_SESSION).unwrap().as_deref(),
            Some("/tmp/scrubbed-project")
        );
        assert_eq!(sessions[0].message_count, 4);
        assert!(sessions[0]
            .approximate_size_bytes
            .is_some_and(|size| size > 0));
    }

    #[test]
    fn claude_summary_rows_discard_message_payloads_but_keep_display_metadata() {
        let reader = fixture();
        let path = reader.find_session(TOOL_SESSION).unwrap();

        let rows = load_summary_rows(&path).unwrap();

        assert!(rows.iter().all(|row| row.value.is_null()));
        assert!(rows
            .iter()
            .any(|row| { row.title.as_deref() == Some("Please inspect the synthetic fixture.") }));
        assert!(rows
            .iter()
            .any(|row| row.cwd.as_deref() == Some("/tmp/scrubbed-project")));
    }

    #[test]
    fn selected_claude_session_rejects_an_oversized_jsonl_record() {
        let root = broca_test_support::scratch_root()
            .join(format!("ck-claude-line-cap-{}", std::process::id()));
        let project = root.join("project");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&project).unwrap();
        let header = json!({
            "type": "user",
            "uuid": "header",
            "parentUuid": null,
            "timestamp": "2026-01-01T00:00:00Z",
            "cwd": "/tmp/project",
            "message": {"role": "user", "content": "header"}
        });
        let oversized = json!({
            "type": "user",
            "uuid": "large",
            "parentUuid": "header",
            "timestamp": "2026-01-01T00:00:01Z",
            "message": {"role": "user", "content": "x".repeat(MAX_JSONL_LINE_BYTES)}
        });
        let path = project.join("oversized.jsonl");
        fs::write(&path, format!("{}\n{}\n", header, oversized)).unwrap();

        let error = ClaudeCodeReader::new(&root)
            .convert("oversized")
            .unwrap_err();
        assert!(error.to_string().contains("line limit"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn converts_tool_reasoning_and_per_message_origins() {
        let imported = fixture().convert(TOOL_SESSION).unwrap();
        assert_eq!(imported.source_ref, format!("cc:{TOOL_SESSION}"));
        assert_eq!(
            imported
                .messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            vec![Role::User, Role::Assistant, Role::Tool, Role::Assistant]
        );
        assert!(!imported
            .messages
            .iter()
            .any(|message| message.role == Role::System));

        let assistants: Vec<_> = imported
            .messages
            .iter()
            .filter(|message| message.role == Role::Assistant)
            .collect();
        assert_eq!(assistants.len(), 2);
        assert_eq!(
            assistants[0].origin.as_ref().unwrap().provider_module_id,
            "anthropic"
        );
        assert_eq!(
            assistants[0].origin.as_ref().unwrap().model_id,
            "claude-fixture-a"
        );
        assert_eq!(
            assistants[1].origin.as_ref().unwrap().model_id,
            "claude-fixture-b"
        );
        assert!(assistants[0].content.iter().any(|block| matches!(
            block,
            ContentBlock::Reasoning {
                signature: None,
                ..
            }
        )));
        assert!(assistants[0].content.iter().all(|block| !matches!(
            block,
            ContentBlock::ToolCall(call) if call.thought_signature.is_some()
        )));
        assert!(!assistants[0]
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::RedactedReasoning { .. })));

        let tool = imported
            .messages
            .iter()
            .find(|message| message.role == Role::Tool)
            .unwrap();
        assert!(matches!(
            &tool.content[0],
            ContentBlock::ToolResult(result)
                if matches!(&result.output, ToolOutput::Text { text } if text == "synthetic output")
        ));
        assert!(matches!(
            &tool.content[1],
            ContentBlock::ToolResult(result)
                if matches!(&result.output, ToolOutput::Json { value } if value.is_array())
        ));
        round_trip(&imported, "claude-tool-round-trip");
    }

    #[test]
    fn imported_singleton_text_tool_result_renders_bare_anthropic_text() {
        let imported = fixture().convert(TOOL_SESSION).unwrap();
        let rendered = AnthropicMessages.shape(
            &anthropic_frozen_config("claude-fixture-render"),
            &CallOptions::new(imported.messages),
        );
        let rendered_result = rendered["messages"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|message| message["content"].as_array().into_iter().flatten())
            .find(|block| {
                block.get("type").and_then(Value::as_str) == Some("tool_result")
                    && block.get("tool_use_id").and_then(Value::as_str) == Some("toolu_text")
            })
            .unwrap();

        assert_eq!(
            rendered_result.get("content").and_then(Value::as_str),
            Some("synthetic output")
        );
    }

    #[test]
    fn signed_reasoning_and_redacted_blobs_are_stripped() {
        let raw = fs::read_to_string(
            fixture_root()
                .join("-tmp-scrubbed-project")
                .join(format!("{TOOL_SESSION}.jsonl")),
        )
        .unwrap();
        assert!(raw.contains("foreign-signed-thinking"));
        assert!(raw.contains("foreign-redacted-blob"));

        let imported = fixture().convert(TOOL_SESSION).unwrap();
        let encoded = serde_json::to_string(&imported.messages).unwrap();
        assert!(!encoded.contains("foreign-signed-thinking"));
        assert!(!encoded.contains("foreign-redacted-blob"));
        assert!(imported
            .warnings
            .iter()
            .any(|warning| warning.contains("redacted_thinking")));
    }

    #[test]
    fn latest_non_sidechain_leaf_excludes_abandoned_branches() {
        let raw = fs::read_to_string(
            fixture_root()
                .join("-tmp-scrubbed-project")
                .join(format!("{TOOL_SESSION}.jsonl")),
        )
        .unwrap();
        assert!(raw.contains("ABANDONED BRANCH MARKER"));
        assert!(raw.contains("SIDECHAIN MARKER"));

        let imported = fixture().convert(TOOL_SESSION).unwrap();
        let encoded = serde_json::to_string(&imported.messages).unwrap();
        assert!(encoded.contains("Selected branch complete."));
        assert!(!encoded.contains("ABANDONED BRANCH MARKER"));
        assert!(!encoded.contains("SIDECHAIN MARKER"));
        assert_eq!(imported.messages.len(), 4);
    }

    #[test]
    fn duplicate_uuid_metadata_rewrite_uses_later_parent() {
        let reader = rewrite_fixture();
        let sessions = reader.enumerate().unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, REWRITE_SESSION);
        assert_eq!(sessions[0].title.as_deref(), Some("Rewrite fixture root."));
        assert_eq!(sessions[0].cwd.as_deref(), Some("/tmp/rewrite-project"));
        assert_eq!(sessions[0].message_count, 4);

        let imported = reader.convert(REWRITE_SESSION).unwrap();
        let encoded = serde_json::to_string(&imported.messages).unwrap();
        assert_eq!(imported.messages.len(), 4);
        assert!(encoded.contains("NEW PARENT MARKER"));
        assert!(!encoded.contains("OLD PARENT MARKER"));
        assert!(encoded.contains("Rewrite fixture complete."));
    }

    #[test]
    fn duplicate_uuid_conflicting_message_is_invalid_data() {
        let error = rewrite_fixture().convert(CONFLICT_SESSION).unwrap_err();
        let io_error = error.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(io_error.kind(), std::io::ErrorKind::InvalidData);
        assert!(io_error
            .to_string()
            .contains("conflicting Claude Code row uuid 'conflict-row'"));
    }

    #[test]
    fn skipped_kinds_are_documented() {
        assert!(OBSERVED_NON_MESSAGE_ROW_KINDS.contains(&"queue-operation"));
        assert!(OBSERVED_NON_MESSAGE_ROW_KINDS.contains(&"summary"));
        assert!(OBSERVED_NON_MESSAGE_ROW_KINDS.contains(&"attachment"));
        assert!(DROPPED_CONTENT_BLOCK_KINDS.contains(&"redacted_thinking"));
        assert!(DROPPED_CONTENT_BLOCK_KINDS.contains(&"image"));

        let imported = fixture().convert(TOOL_SESSION).unwrap();
        assert!(imported
            .warnings
            .iter()
            .any(|warning| warning.contains("queue-operation")));
        assert!(imported
            .warnings
            .iter()
            .any(|warning| warning.contains("system")));
    }

    #[test]
    fn api_error_row_between_genuine_turns_is_omitted() {
        let imported = api_error_fixture().convert(API_ERROR_SESSION).unwrap();
        let encoded = serde_json::to_string(&imported.messages).unwrap();

        assert_eq!(imported.messages.len(), 3);
        assert!(encoded.contains("Genuine turn before the API error."));
        assert!(encoded.contains("Genuine turn after the API error."));
        assert!(!encoded.contains("Prompt is too long"));
        assert_eq!(imported.dropped_api_error_messages, 1);
        assert!(imported
            .warnings
            .iter()
            .any(|warning| warning.contains("Dropped 1 Claude Code API error message row")));
    }

    #[test]
    fn api_error_row_remains_in_branch_graph_for_descendants() {
        let path = api_error_fixture_root()
            .join("-tmp-api-error-project")
            .join(format!("{API_ERROR_SESSION}.jsonl"));
        let rows = load_rows(&path).unwrap();
        assert_eq!(rows.len(), 4, "fixture graph anchor changed");

        let selected = select_latest_branch(&rows).unwrap();
        let selected_uuids = selected
            .iter()
            .map(|index| rows[*index].uuid.as_deref().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            selected_uuids,
            vec![
                "api-user-before",
                "api-error",
                "api-user-after",
                "api-assistant-ordinary"
            ]
        );

        let imported = api_error_fixture().convert(API_ERROR_SESSION).unwrap();
        let encoded = serde_json::to_string(&imported.messages).unwrap();
        assert!(encoded.contains("Genuine turn before the API error."));
        assert!(encoded.contains("Ordinary assistant reply."));
    }

    #[test]
    fn ordinary_assistant_row_is_unaffected_by_api_error_filter() {
        let imported = api_error_fixture().convert(API_ERROR_SESSION).unwrap();
        let assistants = imported
            .messages
            .iter()
            .filter(|message| message.role == Role::Assistant)
            .collect::<Vec<_>>();

        assert_eq!(assistants.len(), 1);
        assert_eq!(
            assistants[0].origin.as_ref().unwrap().model_id,
            "claude-ordinary"
        );
        assert!(matches!(
            &assistants[0].content[0],
            ContentBlock::Text { text } if text == "Ordinary assistant reply."
        ));
    }

    #[test]
    fn torn_sibling_is_skipped_but_selected_torn_session_is_fatal() {
        let root = broca_test_support::scratch_root()
            .join(format!("ck-claude-torn-sibling-{}", std::process::id()));
        let project = root.join("project");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&project).unwrap();
        fs::copy(
            fixture_root()
                .join("-tmp-text-project")
                .join(format!("{TEXT_SESSION}.jsonl")),
            project.join(format!("{TEXT_SESSION}.jsonl")),
        )
        .unwrap();
        let torn_id = "33333333-3333-4333-8333-333333333333";
        fs::write(project.join(format!("{torn_id}.jsonl")), "{\"type\":").unwrap();
        let reader = ClaudeCodeReader::new(&root);

        let sessions = reader.enumerate().unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, TEXT_SESSION);
        assert_eq!(reader.convert(TEXT_SESSION).unwrap().messages.len(), 2);
        assert!(reader.convert(torn_id).is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn text_only_session_round_trips_through_import_wire_schema() {
        let imported = fixture().convert(TEXT_SESSION).unwrap();
        assert_eq!(imported.messages.len(), 2);
        assert_eq!(imported.messages[0].role, Role::User);
        assert!(matches!(
            &imported.messages[0].content[0],
            ContentBlock::Text { text } if text == "A plain synthetic prompt."
        ));
        assert_eq!(
            imported.messages[1].origin.as_ref().unwrap().model_id,
            "claude-text-fixture"
        );
        round_trip(&imported, "claude-text-round-trip");
    }
}
