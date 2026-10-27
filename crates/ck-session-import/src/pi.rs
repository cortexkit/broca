//! Pi JSONL session reader.
//!
//! Pi currently stores version 3 sessions below `~/.pi/agent/sessions/<escaped-cwd>/`.
//! Inspection of live containers found `text`, `thinking`, `toolCall`, and `image` content
//! blocks. Tool results are messages with role `toolResult`, not `toolResult` content blocks.
//! The canonical schema has no image block, so image content is skipped with a warning.
//!
//! Live data also contains `thinking_level_change`, `compaction`, and `custom_message` control
//! rows. They participate in the parent tree but not in model history. A migrated session can
//! have its initial `model_change` and first message as separate roots, so the reader uses that
//! root model event as a fallback origin. Duplicate session UUIDs can remain after a project is
//! moved; in that case the newest container copy wins.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use broca_protocol::{
    ContentBlock, Message, ModelHandle, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use serde_json::Value;

use crate::{
    check_session_source_size, finalize_import, omit_nested_tool_images, read_bounded_jsonl_line,
    truncate_session_title, HarnessReader, ImportedSession, SessionSummary,
};

type ReaderResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Pi rows that affect the UI or future requests but do not represent conversation history.
pub const SKIPPED_CONTROL_ROW_KINDS: &[&str] =
    &["thinking_level_change", "compaction", "custom_message"];

#[derive(Debug, Clone)]
pub struct PiReader {
    root: PathBuf,
}

impl PiReader {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn default_path() -> PathBuf {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default()
            .join(".pi/agent/sessions")
    }

    pub fn sessions_root(&self) -> &Path {
        &self.root
    }

    fn discover(&self, selected_id: Option<&str>) -> ReaderResult<Vec<LoadedSession>> {
        self.discover_with_loader(selected_id, LoadedSession::load)
    }

    fn session_paths(&self) -> ReaderResult<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for directory in std::fs::read_dir(&self.root)? {
            let directory = directory?;
            if !directory.file_type()?.is_dir() {
                continue;
            }
            for entry in std::fs::read_dir(directory.path())? {
                let entry = entry?;
                let path = entry.path();
                if entry.file_type()?.is_file()
                    && path.extension().and_then(|extension| extension.to_str()) == Some("jsonl")
                {
                    paths.push(path);
                }
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn discover_with_loader(
        &self,
        selected_id: Option<&str>,
        mut load: impl FnMut(PathBuf) -> ReaderResult<LoadedSession>,
    ) -> ReaderResult<Vec<LoadedSession>> {
        let paths = self.session_paths()?;
        let mut sessions: HashMap<String, LoadedSession> = HashMap::new();
        for path in paths {
            // A session header is mandatory in `load`, so a missing or different header cannot
            // belong to the selected session. Avoid decoding and retaining its complete body.
            if selected_id.is_some_and(|selected| {
                LoadedSession::header_id(&path).as_deref() != Some(selected)
            }) {
                continue;
            }
            let candidate = match load(path.clone()) {
                Ok(candidate) => candidate,
                Err(error)
                    if selected_id.is_some()
                        && LoadedSession::header_id(&path).as_deref() == selected_id =>
                {
                    return Err(error);
                }
                Err(error) => {
                    eprintln!(
                        "WARNING: skipped unreadable Pi session {} during discovery: {error}",
                        path.display()
                    );
                    continue;
                }
            };
            match sessions.entry(candidate.id.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if candidate.freshness_key() > entry.get().freshness_key() {
                        entry.insert(candidate);
                    }
                }
            }
        }
        Ok(sessions.into_values().collect())
    }
}

impl Default for PiReader {
    fn default() -> Self {
        Self::new(Self::default_path())
    }
}

impl HarnessReader for PiReader {
    fn harness(&self) -> &'static str {
        "pi"
    }

    fn enumerate(&self) -> ReaderResult<Vec<SessionSummary>> {
        let mut newest = HashMap::<String, (SystemTime, u64, PathBuf)>::new();
        for path in self.session_paths()? {
            let session = match LoadedSession::load(path.clone()) {
                Ok(session) => session,
                Err(error) => {
                    eprintln!(
                        "WARNING: skipped unreadable Pi session {} during enumeration: {error}",
                        path.display()
                    );
                    continue;
                }
            };
            let key = (session.modified, session.size, session.path.clone());
            let replace = newest
                .get(&session.id)
                .is_none_or(|current| key.cmp(current).is_gt());
            if replace {
                newest.insert(session.id, key);
            }
        }

        // Retain only the winning paths between passes; each parsed session drops before the
        // next file is opened, so enumeration does not hold every source transcript in memory.
        let mut summaries = Vec::with_capacity(newest.len());
        for (id, (_, _, path)) in newest {
            let session = match LoadedSession::load(path.clone()) {
                Ok(session) => session,
                Err(error) => {
                    eprintln!(
                        "WARNING: skipped unreadable Pi session {} during enumeration: {error}",
                        path.display()
                    );
                    continue;
                }
            };
            let selected = match session.selected_path() {
                Ok(selected) => selected,
                Err(error) => {
                    eprintln!(
                        "WARNING: skipped malformed Pi parent tree for session '{}': {error}",
                        session.id
                    );
                    continue;
                }
            };
            let title = first_user_text(&selected).map(truncate_session_title);
            let message_count = selected
                .iter()
                .filter(|node| node.kind == "message")
                .count();
            summaries.push((
                session.timestamp.clone(),
                SessionSummary {
                    id,
                    title,
                    cwd: nonempty(session.cwd),
                    message_count,
                    approximate_size_bytes: Some(session.size),
                },
            ));
        }
        summaries.sort_by(|(left_timestamp, left), (right_timestamp, right)| {
            right_timestamp
                .cmp(left_timestamp)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(summaries.into_iter().map(|(_, summary)| summary).collect())
    }

    fn convert(&self, session_id: &str) -> ReaderResult<ImportedSession> {
        let session = self
            .discover(Some(session_id))?
            .into_iter()
            .find(|session| session.id == session_id)
            .ok_or_else(|| {
                IoError::new(
                    ErrorKind::NotFound,
                    format!("Pi session '{session_id}' was not found"),
                )
            })?;
        let mut warnings = Vec::new();
        let mut path = session.selected_path()?;
        let mut compaction_summary = None;
        if let Some(compaction_index) = path.iter().rposition(|node| node.kind == "compaction") {
            let compaction = path[compaction_index];
            compaction_summary = compaction
                .value
                .get("summary")
                .and_then(Value::as_str)
                .filter(|summary| !summary.is_empty())
                .map(str::to_owned);
            let first_kept_id = compaction
                .value
                .get("firstKeptEntryId")
                .and_then(Value::as_str);
            if let Some(first_kept_id) = first_kept_id {
                if first_kept_id == compaction.id {
                    path = path[compaction_index + 1..].to_vec();
                } else if let Some(first_kept_index) = path
                    .iter()
                    .position(|node| node.id == first_kept_id)
                    .filter(|index| *index < compaction_index)
                {
                    let mut retained = path[first_kept_index..compaction_index].to_vec();
                    retained.extend_from_slice(&path[compaction_index + 1..]);
                    path = retained;
                } else {
                    warnings.push(format!(
                        "Pi compaction {} refers to invalid firstKeptEntryId '{}'; importing records after the compaction row",
                        compaction.id, first_kept_id
                    ));
                    path = path[compaction_index + 1..].to_vec();
                }
            } else {
                warnings.push(format!(
                    "Pi compaction {} has no firstKeptEntryId; importing records after the compaction row",
                    compaction.id
                ));
                path = path[compaction_index + 1..].to_vec();
            }
        }
        let first_order = path.first().map_or(usize::MAX, |node| node.order);
        let mut current_model = session
            .nodes
            .iter()
            .filter(|node| {
                node.kind == "model_change" && node.parent_id.is_none() && node.order < first_order
            })
            .max_by_key(|node| node.order)
            .and_then(|node| model_change(&node.value));
        let mut messages = Vec::new();
        let mut skipped_controls = Vec::new();
        let mut skipped_blocks = Vec::new();
        let mut skipped_rows = Vec::new();

        if let Some(summary) = compaction_summary {
            messages.push(Message::new(
                Role::User,
                vec![ContentBlock::Text { text: summary }],
            ));
        }

        for node in path {
            match node.kind.as_str() {
                "model_change" => {
                    if let Some(origin) = model_change(&node.value) {
                        current_model = Some(origin);
                    } else {
                        warnings.push(format!("Skipped malformed Pi model_change row {}", node.id));
                    }
                }
                "message" => {
                    if let Some(message) = convert_message(
                        node,
                        current_model.clone(),
                        &mut warnings,
                        &mut skipped_blocks,
                    ) {
                        messages.push(message);
                    }
                }
                "compaction" => {}
                kind if SKIPPED_CONTROL_ROW_KINDS.contains(&kind) => {
                    push_unique(&mut skipped_controls, kind);
                }
                other => push_unique(&mut skipped_rows, other),
            }
        }

        if !skipped_controls.is_empty() {
            warnings.push(format!(
                "Skipped Pi control row kinds: {}",
                skipped_controls.join(", ")
            ));
        }
        if !skipped_blocks.is_empty() {
            warnings.push(format!(
                "Skipped unsupported Pi content block kinds: {}",
                skipped_blocks.join(", ")
            ));
        }
        if !skipped_rows.is_empty() {
            warnings.push(format!(
                "Skipped unsupported Pi row kinds: {}",
                skipped_rows.join(", ")
            ));
        }

        finalize_import(messages, format!("pi:{}", session.id), warnings, "pi")
    }

    fn session_cwd(&self, session_id: &str) -> ReaderResult<Option<String>> {
        Ok(self
            .discover(Some(session_id))?
            .into_iter()
            .find(|session| session.id == session_id)
            .and_then(|session| nonempty(session.cwd)))
    }
}

#[derive(Debug)]
struct LoadedSession {
    path: PathBuf,
    id: String,
    timestamp: String,
    cwd: String,
    nodes: Vec<Node>,
    size: u64,
    modified: SystemTime,
}

impl LoadedSession {
    fn header_id(path: &Path) -> Option<String> {
        let file = File::open(path).ok()?;
        let mut reader = BufReader::new(file);
        let mut line = Vec::new();
        let mut bytes_read = 0;
        while read_bounded_jsonl_line(&mut reader, &mut line, &mut bytes_read).ok()? {
            let value: Value = serde_json::from_slice(&line).ok()?;
            if value.get("type").and_then(Value::as_str) == Some("session") {
                return value.get("id").and_then(Value::as_str).map(str::to_owned);
            }
        }
        None
    }

    fn load(path: PathBuf) -> ReaderResult<Self> {
        let size = check_session_source_size(&path)?;
        let metadata = std::fs::metadata(&path)?;
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);
        let mut line = Vec::new();
        let mut bytes_read = 0;
        let mut header: Option<(String, String, String)> = None;
        let mut nodes = Vec::new();
        let mut ids = HashSet::new();
        let mut index = 0;

        while read_bounded_jsonl_line(&mut reader, &mut line, &mut bytes_read).map_err(|error| {
            invalid_data(format!(
                "read Pi session {} line {}: {error}",
                path.display(),
                index + 1
            ))
        })? {
            let line_number = index + 1;
            if line.iter().all(u8::is_ascii_whitespace) {
                index += 1;
                continue;
            }
            let value: Value = serde_json::from_slice(&line).map_err(|error| {
                invalid_data(format!(
                    "decode Pi session {} line {line_number}: {error}",
                    path.display()
                ))
            })?;
            let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
            if kind == "session" {
                if header.is_some() {
                    return Err(invalid_data(format!(
                        "Pi session {} has more than one session row",
                        path.display()
                    ))
                    .into());
                }
                let id = required_string(&value, "id", &path, line_number)?;
                let timestamp = required_string(&value, "timestamp", &path, line_number)?;
                let cwd = value
                    .get("cwd")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                header = Some((id, timestamp, cwd));
                index += 1;
                continue;
            }
            let Some(id) = value.get("id").and_then(Value::as_str) else {
                index += 1;
                continue;
            };
            if !ids.insert(id.to_string()) {
                return Err(invalid_data(format!(
                    "Pi session {} has duplicate row id '{id}'",
                    path.display()
                ))
                .into());
            }
            nodes.push(Node {
                id: id.to_string(),
                parent_id: value
                    .get("parentId")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                kind: kind.to_string(),
                value,
                order: line_number,
            });
            index += 1;
        }

        let (id, timestamp, cwd) = header.ok_or_else(|| {
            invalid_data(format!(
                "Pi session {} is missing its session row",
                path.display()
            ))
        })?;
        Ok(Self {
            path,
            id,
            timestamp,
            cwd,
            nodes,
            size,
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        })
    }

    fn freshness_key(&self) -> (SystemTime, u64, &Path) {
        (self.modified, self.size, &self.path)
    }

    fn selected_path(&self) -> ReaderResult<Vec<&Node>> {
        if self.nodes.is_empty() {
            return Ok(Vec::new());
        }
        let by_id: HashMap<&str, usize> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.id.as_str(), index))
            .collect();
        let parents: HashSet<&str> = self
            .nodes
            .iter()
            .filter_map(|node| node.parent_id.as_deref())
            .collect();
        let leaf = self
            .nodes
            .iter()
            .filter(|node| !parents.contains(node.id.as_str()))
            .max_by_key(|node| node.order)
            .ok_or_else(|| invalid_data(format!("Pi session '{}' has no leaf", self.id)))?;

        let mut selected = Vec::new();
        let mut current = leaf;
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(current.id.as_str()) {
                return Err(invalid_data(format!(
                    "Pi session '{}' contains a parent cycle at '{}'",
                    self.id, current.id
                ))
                .into());
            }
            selected.push(current);
            let Some(parent_id) = current.parent_id.as_deref() else {
                break;
            };
            let parent_index = by_id.get(parent_id).ok_or_else(|| {
                invalid_data(format!(
                    "Pi session '{}' row '{}' references missing parent '{}'",
                    self.id, current.id, parent_id
                ))
            })?;
            current = &self.nodes[*parent_index];
        }
        selected.reverse();
        Ok(selected)
    }
}

#[derive(Debug)]
struct Node {
    id: String,
    parent_id: Option<String>,
    kind: String,
    value: Value,
    order: usize,
}

/// The session title for `ck-import list`.
///
/// Handles BOTH content spellings for the same reason `convert_message` does. This is the symptom
/// an operator meets FIRST and is least likely to connect to lost content: a session whose opening
/// user message is scalar listed with an empty title, which reads like a session that opened with
/// no prompt rather than like a reader that cannot see one.
fn first_user_text(path: &[&Node]) -> Option<String> {
    path.iter()
        .filter(|node| node.kind == "message")
        .filter_map(|node| node.value.get("message"))
        .filter(|message| message.get("role").and_then(Value::as_str) == Some("user"))
        .find_map(|message| {
            let content = message.get("content")?;
            if let Some(text) = content.as_str() {
                return Some(text.to_string());
            }
            content
                .as_array()?
                .iter()
                .find(|block| block.get("type").and_then(Value::as_str) == Some("text"))
                .and_then(|block| block.get("text").and_then(Value::as_str))
                .map(str::to_string)
        })
        .filter(|text| !text.trim().is_empty())
}

fn convert_message(
    node: &Node,
    threaded_model: Option<ModelHandle>,
    warnings: &mut Vec<String>,
    skipped_blocks: &mut Vec<String>,
) -> Option<Message> {
    let message = node.value.get("message").unwrap_or(&Value::Null);
    let source_role = message.get("role").and_then(Value::as_str).unwrap_or("");
    if source_role == "system" {
        return None;
    }
    if source_role == "toolResult" {
        return convert_tool_result(node, message, warnings);
    }
    let role = match source_role {
        "user" => Role::User,
        "assistant" => Role::Assistant,
        other => {
            warnings.push(format!(
                "Skipped Pi message {} with unsupported role '{other}'",
                node.id
            ));
            return None;
        }
    };
    let mut content = Vec::new();
    // SCALAR CONTENT IS A SUPPORTED PI REPRESENTATION, NOT MALFORMED INPUT. Pi's own type is
    // `string | (TextContent | ImageContent)[]`, its documented session format shows the scalar
    // form, and SessionManager persists it unconverted.
    //
    // The array-only loop below turned that into SILENT LOSS: `as_array()` on a string yields
    // None, the message converted to zero blocks, and the caller dropped it WITHOUT A WARNING.
    // A user instruction simply vanished from the imported transcript, and the only visible trace
    // was a message count nobody had a reason to check.
    //
    // crates/ck-session-import/src/claude_code.rs already handles the same shape with `as_str()`,
    // so this was an inconsistency between two readers in one crate rather than a deliberate
    // narrowing.
    if let Some(text) = message.get("content").and_then(Value::as_str) {
        if !text.is_empty() {
            content.push(ContentBlock::Text {
                text: text.to_string(),
            });
        }
    }
    for block in message
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        match block.get("type").and_then(Value::as_str).unwrap_or("") {
            "text" => {
                if let Some(text) = block.get("text").and_then(Value::as_str) {
                    content.push(ContentBlock::Text {
                        text: text.to_string(),
                    });
                }
            }
            "thinking" => {
                if let Some(text) = block.get("thinking").and_then(Value::as_str) {
                    content.push(ContentBlock::Reasoning {
                        text: text.to_string(),
                        signature: None,
                    });
                }
            }
            "toolCall" => content.push(ContentBlock::ToolCall(ToolCall {
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
                input: block.get("arguments").cloned().unwrap_or(Value::Null),
                thought_signature: None,
            })),
            other => push_unique(skipped_blocks, other),
        }
    }
    if content.is_empty() {
        return None;
    }
    if role == Role::Assistant {
        let origin = message_origin(message).or(threaded_model);
        Some(match origin {
            Some(model) => Message::assistant_from(content, model),
            None => Message::new(role, content),
        })
    } else {
        Some(Message::new(role, content))
    }
}

fn convert_tool_result(
    node: &Node,
    message: &Value,
    warnings: &mut Vec<String>,
) -> Option<Message> {
    let Some(call_id) = message.get("toolCallId").and_then(Value::as_str) else {
        warnings.push(format!(
            "Skipped Pi tool result {} without a toolCallId",
            node.id
        ));
        return None;
    };
    let blocks = message
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut sanitized = Value::Array(blocks);
    let omitted_images = omit_nested_tool_images(&mut sanitized);
    if omitted_images > 0 {
        warnings.push(format!(
            "Omitted {omitted_images} unsupported Pi tool-result image block(s)"
        ));
    }
    let blocks = sanitized.as_array().cloned().unwrap_or_default();
    let is_error = message
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let output = if is_error {
        ToolOutput::error(tool_result_text(&blocks))
    } else if let [block] = blocks.as_slice() {
        match (
            block.get("type").and_then(Value::as_str),
            block.get("text").and_then(Value::as_str),
        ) {
            (Some("text"), Some(text)) => ToolOutput::Text {
                text: text.to_string(),
            },
            _ => ToolOutput::Json {
                value: Value::Array(blocks),
            },
        }
    } else {
        ToolOutput::Json {
            value: Value::Array(blocks),
        }
    };
    Some(Message::new(
        Role::Tool,
        vec![ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId(call_id.to_string()),
            output,
            is_error,
        })],
    ))
}

fn tool_result_text(blocks: &[Value]) -> String {
    let text = blocks
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    if text.is_empty() {
        serde_json::to_string(blocks).unwrap_or_else(|_| "Pi tool execution failed".to_string())
    } else {
        text
    }
}

fn model_change(value: &Value) -> Option<ModelHandle> {
    let provider = value.get("provider").and_then(Value::as_str)?;
    let model = value.get("modelId").and_then(Value::as_str)?;
    Some(ModelHandle::new(provider, model))
}

fn message_origin(message: &Value) -> Option<ModelHandle> {
    let provider = message.get("provider").and_then(Value::as_str)?;
    let model = message.get("model").and_then(Value::as_str)?;
    Some(ModelHandle::new(provider, model))
}

fn required_string(
    value: &Value,
    key: &str,
    path: &Path,
    line_number: usize,
) -> ReaderResult<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            invalid_data(format!(
                "Pi session {} line {line_number} is missing string field '{key}'",
                path.display()
            ))
            .into()
        })
}

fn invalid_data(message: String) -> IoError {
    IoError::new(ErrorKind::InvalidData, message)
}

fn nonempty(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|known| known == value) {
        values.push(value.to_string());
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, FileTimes, OpenOptions};
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime};

    use broca_module_serve::wire::{ImportOriginKind, ImportOriginParams, ImportParams};
    use broca_protocol::{ContentBlock, Role, ToolOutput};

    use super::{
        convert_message, convert_tool_result, HarnessReader, LoadedSession, Node, PiReader,
        SKIPPED_CONTROL_ROW_KINDS,
    };

    fn fixture_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pi")
    }

    fn fixture() -> PiReader {
        PiReader::new(fixture_root())
    }

    fn write_pi_session(path: &Path, id: &str, text: &str, message_count: usize) {
        let mut rows = vec![serde_json::json!({
            "type": "session",
            "id": id,
            "timestamp": "2026-01-01T00:00:00Z",
            "cwd": "/tmp/pi-perf-test"
        })
        .to_string()];
        for index in 0..message_count.max(1) {
            rows.push(
                serde_json::json!({
                    "type": "message",
                    "id": format!("{id}-message-{index}"),
                    "parentId": (index > 0).then(|| format!("{id}-message-{}", index - 1)),
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": format!("{text}-{index}")}]
                    }
                })
                .to_string(),
            );
        }
        fs::write(path, format!("{}\n", rows.join("\n"))).unwrap();
    }

    fn set_modified(path: &Path, seconds_after_epoch: u64) {
        let file = OpenOptions::new().write(true).open(path).unwrap();
        file.set_times(
            FileTimes::new()
                .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(seconds_after_epoch)),
        )
        .unwrap();
    }

    fn round_trip(imported: crate::ImportedSession, import_id: &str) {
        let params = ImportParams {
            messages: imported.messages,
            import_id: import_id.into(),
            origin: ImportOriginParams {
                kind: ImportOriginKind::Import,
                source_ref: imported.source_ref,
            },
        };
        let encoded = serde_json::to_value(&params).unwrap();
        let decoded: ImportParams = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
    }

    #[test]
    fn enumerates_scrubbed_fixture_sessions() {
        let sessions = fixture().enumerate().unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].id, "00000000-0000-4000-8000-000000000001");
        assert_eq!(
            sessions[0].title.as_deref(),
            Some("Inspect the scrubbed fixture.")
        );
        assert_eq!(sessions[0].cwd.as_deref(), Some("/tmp/scrubbed-project"));
        assert_eq!(
            fixture()
                .session_cwd("00000000-0000-4000-8000-000000000001")
                .unwrap()
                .as_deref(),
            Some("/tmp/scrubbed-project")
        );
        assert_eq!(sessions[0].message_count, 5);
        assert!(sessions[0]
            .approximate_size_bytes
            .is_some_and(|size| size > 0));
    }

    #[test]
    fn selected_discovery_loads_only_matching_header_files_but_list_loads_all() {
        let root = broca_test_support::scratch_root()
            .join(format!("ck-pi-selected-probe-{}", std::process::id()));
        let project = root.join("project");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&project).unwrap();
        let target_path = project.join("target.jsonl");
        write_pi_session(&target_path, "target", "selected", 1);
        for index in 0..3 {
            write_pi_session(
                &project.join(format!("unrelated-{index}.jsonl")),
                &format!("unrelated-{index}"),
                "large-unrelated-body",
                64,
            );
        }
        let reader = PiReader::new(&root);
        let mut loaded = Vec::new();

        let selected = reader
            .discover_with_loader(Some("target"), |path| {
                loaded.push(path.clone());
                LoadedSession::load(path)
            })
            .unwrap();

        assert_eq!(loaded, vec![target_path]);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "target");
        let mut listed_ids = reader
            .enumerate()
            .unwrap()
            .into_iter()
            .map(|session| session.id)
            .collect::<Vec<_>>();
        listed_ids.sort();
        assert_eq!(
            listed_ids,
            vec!["target", "unrelated-0", "unrelated-1", "unrelated-2"]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn selected_duplicate_still_uses_the_fresher_copy() {
        let root = broca_test_support::scratch_root()
            .join(format!("ck-pi-selected-fresh-{}", std::process::id()));
        let project = root.join("project");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&project).unwrap();
        let stale = project.join("stale.jsonl");
        let fresh = project.join("fresh.jsonl");
        write_pi_session(&stale, "target", "stale", 1);
        write_pi_session(&fresh, "target", "fresh", 1);
        set_modified(&stale, 1_700_000_000);
        set_modified(&fresh, 1_700_000_001);

        let imported = PiReader::new(&root).convert("target").unwrap();
        let encoded = serde_json::to_string(&imported.messages).unwrap();
        assert!(encoded.contains("fresh-0"));
        assert!(!encoded.contains("stale-0"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compaction_summary_and_first_kept_entry_replace_older_history() {
        let root = broca_test_support::scratch_root()
            .join(format!("ck-pi-compaction-{}", std::process::id()));
        let project = root.join("project");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&project).unwrap();
        let path = project.join("compacted.jsonl");
        let rows = [
            serde_json::json!({"type":"session","id":"pi-compacted","timestamp":"2026-01-01T00:00:00Z","cwd":"/tmp/pi"}),
            serde_json::json!({"type":"message","id":"old","parentId":null,"message":{"role":"user","content":"OLD_PI_CONTEXT_MUST_NOT_SURVIVE"}}),
            serde_json::json!({"type":"message","id":"kept","parentId":"old","message":{"role":"user","content":"KEPT_AFTER_COMPACTION"}}),
            serde_json::json!({"type":"compaction","id":"compact","parentId":"kept","summary":"PI_COMPACTION_SUMMARY","firstKeptEntryId":"kept"}),
            serde_json::json!({"type":"message","id":"answer","parentId":"compact","message":{"role":"assistant","content":"POST_COMPACTION_ANSWER"}}),
        ];
        fs::write(
            &path,
            rows.iter()
                .map(serde_json::Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let imported = PiReader::new(&root).convert("pi-compacted").unwrap();
        let transcript = serde_json::to_string(&imported.messages).unwrap();
        assert!(!transcript.contains("OLD_PI_CONTEXT_MUST_NOT_SURVIVE"));
        assert!(transcript.contains("PI_COMPACTION_SUMMARY"));
        assert!(transcript.contains("KEPT_AFTER_COMPACTION"));
        assert!(transcript.contains("POST_COMPACTION_ANSWER"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tool_result_images_are_omitted_with_an_actionable_warning() {
        let node = Node {
            id: "image-result".into(),
            parent_id: None,
            kind: "message".into(),
            value: serde_json::json!({
                "message": {
                    "role": "toolResult",
                    "toolCallId": "call-image",
                    "content": [
                        {"type": "text", "text": "before image"},
                        {"type": "image", "data": "PRIVATE_BASE64"},
                        {"type": "text", "text": "after image"}
                    ]
                }
            }),
            order: 1,
        };
        let mut warnings = Vec::new();
        let message =
            convert_tool_result(&node, node.value.get("message").unwrap(), &mut warnings).unwrap();
        let encoded = serde_json::to_string(&message).unwrap();
        assert!(encoded.contains("before image"));
        assert!(encoded.contains("after image"));
        assert!(encoded.contains("image omitted from imported tool output"));
        assert!(!encoded.contains("PRIVATE_BASE64"));
        assert!(warnings.iter().any(|warning| warning.contains("Omitted 1")));
    }

    #[test]
    fn converts_tool_reasoning_and_per_message_origins() {
        let imported = fixture()
            .convert("00000000-0000-4000-8000-000000000001")
            .unwrap();
        assert_eq!(
            imported.source_ref,
            "pi:00000000-0000-4000-8000-000000000001"
        );
        assert_eq!(
            imported
                .messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            vec![
                Role::User,
                Role::User,
                Role::Assistant,
                Role::Tool,
                Role::Assistant
            ]
        );
        assert!(!imported
            .messages
            .iter()
            .any(|message| message.role == Role::System));

        let first_assistant = &imported.messages[2];
        assert_eq!(
            first_assistant.origin.as_ref().unwrap().provider_module_id,
            "anthropic"
        );
        assert_eq!(
            first_assistant.origin.as_ref().unwrap().model_id,
            "claude-scrubbed"
        );
        let final_assistant = &imported.messages[4];
        assert_eq!(
            final_assistant.origin.as_ref().unwrap().provider_module_id,
            "openai"
        );
        assert_eq!(
            final_assistant.origin.as_ref().unwrap().model_id,
            "gpt-scrubbed"
        );
        assert!(matches!(
            &imported.messages[3].content[0],
            ContentBlock::ToolResult(result)
                if matches!(&result.output, ToolOutput::Text { text } if text == "scrubbed output")
        ));
        round_trip(imported, "pi-tool-round-trip");
    }

    #[test]
    fn strips_signed_reasoning_and_tool_call_signatures() {
        let raw = std::fs::read_to_string(
            fixture_root()
                .join("--tmp-scrubbed-project--")
                .join("2026-02-02T00-00-00-000Z_00000000-0000-4000-8000-000000000001.jsonl"),
        )
        .unwrap();
        assert!(raw.contains("fixture-thinking-signature"));
        assert!(raw.contains("fixture-tool-signature"));

        let imported = fixture()
            .convert("00000000-0000-4000-8000-000000000001")
            .unwrap();
        let assistant = &imported.messages[2];
        assert!(assistant.content.iter().any(|block| matches!(
            block,
            ContentBlock::Reasoning {
                signature: None,
                ..
            }
        )));
        assert!(assistant.content.iter().any(|block| matches!(
            block,
            ContentBlock::ToolCall(call) if call.thought_signature.is_none()
        )));
        assert!(!assistant
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::RedactedReasoning { .. })));
    }

    #[test]
    fn latest_leaf_excludes_the_abandoned_branch() {
        let raw = std::fs::read_to_string(
            fixture_root()
                .join("--tmp-scrubbed-project--")
                .join("2026-02-02T00-00-00-000Z_00000000-0000-4000-8000-000000000001.jsonl"),
        )
        .unwrap();
        assert!(raw.contains("ABANDONED BRANCH"));

        let imported = fixture()
            .convert("00000000-0000-4000-8000-000000000001")
            .unwrap();
        let encoded = serde_json::to_string(&imported.messages).unwrap();
        assert!(!encoded.contains("ABANDONED BRANCH"));
        assert!(encoded.contains("Selected branch complete."));
    }

    #[test]
    fn documents_skipped_control_and_content_kinds() {
        assert_eq!(
            SKIPPED_CONTROL_ROW_KINDS,
            &["thinking_level_change", "compaction", "custom_message"]
        );
        let imported = fixture()
            .convert("00000000-0000-4000-8000-000000000001")
            .unwrap();
        assert!(imported
            .warnings
            .iter()
            .any(|warning| warning.contains("control row kinds")));
        assert!(imported
            .warnings
            .iter()
            .any(|warning| warning.contains("content block kinds: image")));
    }

    #[test]
    fn torn_sibling_is_skipped_but_selected_torn_session_is_fatal() {
        let root = broca_test_support::scratch_root()
            .join(format!("ck-pi-torn-sibling-{}", std::process::id()));
        let project = root.join("project");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&project).unwrap();
        fs::copy(
            fixture_root()
                .join("--tmp-text-project--")
                .join("2026-01-01T00-00-00-000Z_00000000-0000-4000-8000-000000000002.jsonl"),
            project.join("valid.jsonl"),
        )
        .unwrap();
        fs::write(
            project.join("torn.jsonl"),
            "{\"type\":\"session\",\"id\":\"pi-torn\",\"timestamp\":\"2026-01-03T00:00:00Z\"}\n{\"type\":",
        )
        .unwrap();
        let reader = PiReader::new(&root);

        let sessions = reader.enumerate().unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "00000000-0000-4000-8000-000000000002");
        assert_eq!(reader.convert(&sessions[0].id).unwrap().messages.len(), 2);
        assert!(reader.convert("pi-torn").is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn malformed_parent_tree_is_skipped_from_enumeration_but_selected_import_fails() {
        let root = broca_test_support::scratch_root()
            .join(format!("ck-pi-parent-tree-{}", std::process::id()));
        let project = root.join("project");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&project).unwrap();
        write_pi_session(&project.join("valid.jsonl"), "valid", "usable", 1);
        fs::write(
            project.join("malformed.jsonl"),
            concat!(
                "{\"type\":\"session\",\"id\":\"malformed\",\"timestamp\":\"2026-01-02T00:00:00Z\",\"cwd\":\"/tmp/pi\"}\n",
                "{\"type\":\"message\",\"id\":\"leaf\",\"parentId\":\"missing\",\"message\":{\"role\":\"user\",\"content\":\"unreachable parent\"}}\n"
            ),
        )
        .unwrap();
        let reader = PiReader::new(&root);

        let summaries = reader.enumerate().unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, "valid");
        assert!(reader.convert("valid").is_ok());
        assert!(reader.convert("malformed").is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn selected_pi_session_rejects_an_oversized_jsonl_record() {
        let root = broca_test_support::scratch_root()
            .join(format!("ck-pi-line-cap-{}", std::process::id()));
        let project = root.join("project");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&project).unwrap();
        let rows = [
            serde_json::json!({
                "type": "session",
                "id": "large",
                "timestamp": "2026-01-01T00:00:00Z",
                "cwd": "/tmp/project"
            }),
            serde_json::json!({
                "type": "message",
                "id": "large-message",
                "parentId": null,
                "message": {"role": "user", "content": "x".repeat(crate::MAX_JSONL_LINE_BYTES)}
            }),
        ];
        fs::write(
            project.join("large.jsonl"),
            rows.iter()
                .map(serde_json::Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let error = PiReader::new(&root).convert("large").unwrap_err();

        assert!(error.to_string().contains("line limit"));
        let _ = fs::remove_dir_all(root);
    }

    /// A PAIRED TEST, and the pairing is the whole assertion.
    ///
    /// Pi's content type is `string | (TextContent|ImageContent)[]`, so BOTH spellings are valid
    /// input and both must yield the same canonical message. Before this, the scalar arm converted
    /// to zero blocks and the caller dropped the message WITH NO WARNING -- a user instruction gone
    /// from the transcript, traceable only through a message count nobody had reason to check.
    ///
    /// Asserting only the scalar arm would pass against a reader that had broken the ARRAY path
    /// instead. Running both through one code path with one expected value is what makes this a
    /// discriminator rather than a snapshot.
    #[test]
    fn scalar_and_array_user_content_convert_identically() {
        let convert = |content: serde_json::Value| {
            let node = Node {
                id: "scalar-vs-array".into(),
                parent_id: None,
                kind: "message".into(),
                value: serde_json::json!({
                    "message": { "role": "user", "content": content }
                }),
                order: 1,
            };
            let mut warnings = Vec::new();
            let message = convert_message(&node, None, &mut warnings, &mut Vec::new());
            (message, warnings)
        };

        let (scalar, scalar_warnings) = convert(serde_json::json!("Do not delete files."));
        let (array, array_warnings) =
            convert(serde_json::json!([{"type": "text", "text": "Do not delete files."}]));

        let scalar = scalar.expect("scalar user content must convert, not vanish");
        let array = array.expect("array user content must convert");

        assert_eq!(scalar.role, Role::User);
        assert_eq!(scalar.content, array.content, "both spellings, one message");
        assert_eq!(
            scalar.content,
            vec![ContentBlock::Text {
                text: "Do not delete files.".into()
            }]
        );
        // The silent half: a dropped message left no warning either, so nothing downstream could
        // report the loss. Neither arm may produce one.
        assert!(scalar_warnings.is_empty(), "{scalar_warnings:?}");
        assert!(array_warnings.is_empty(), "{array_warnings:?}");
    }

    #[test]
    fn explicit_message_origin_wins_over_threaded_model_state() {
        let node = Node {
            id: "message-origin".into(),
            parent_id: None,
            kind: "message".into(),
            value: serde_json::json!({
                "message": {
                    "role": "assistant",
                    "provider": "message-provider",
                    "model": "message-model",
                    "content": [{"type": "text", "text": "answer"}]
                }
            }),
            order: 1,
        };
        let message = convert_message(
            &node,
            Some(broca_protocol::ModelHandle::new(
                "thread-provider",
                "thread-model",
            )),
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap();
        let origin = message.origin.unwrap();
        assert_eq!(origin.provider_module_id, "message-provider");
        assert_eq!(origin.model_id, "message-model");
    }

    #[test]
    fn text_only_session_round_trips_through_import_wire_schema() {
        let imported = fixture()
            .convert("00000000-0000-4000-8000-000000000002")
            .unwrap();
        assert_eq!(imported.messages.len(), 2);
        assert_eq!(imported.messages[0].role, Role::User);
        assert_eq!(imported.messages[1].role, Role::Assistant);
        round_trip(imported, "pi-text-round-trip");
    }
}
