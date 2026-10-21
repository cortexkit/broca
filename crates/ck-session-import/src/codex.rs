//! Codex JSONL session reader.
//!
//! Codex stores one rollout per file below `~/.codex/sessions/YYYY/MM/DD`. Current files differ
//! from the nominal Responses API shape in two useful ways: the model id is carried by
//! `turn_context` rows rather than `session_meta`, and newer builds also emit `world_state`,
//! `inter_agent_communication_metadata`, custom-tool, search, and agent-message items. The reader
//! uses turn context only to stamp assistant origins and skips those non-history or unsupported
//! records. `reasoning` items are dropped because their `encrypted_content` is undecryptable
//! foreign state; no encrypted or signed reasoning bytes are copied into canonical messages.

#[cfg(test)]
use std::cell::Cell;
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};

use broca_protocol::{
    ContentBlock, Message, ModelHandle, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use serde_json::Value;

use crate::{
    check_session_source_size, finalize_import, read_bounded_jsonl_line, truncate_session_title,
    HarnessReader, ImportedSession, SessionSummary,
};

type ReaderResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[cfg(test)]
thread_local! {
    static FULL_PARSE_COUNT: Cell<usize> = const { Cell::new(0) };
}

/// Rows observed in real Codex rollouts that do not represent conversation history.
pub const STATE_ROW_TYPES: &[&str] = &[
    "turn_context",
    "compacted",
    "event_msg",
    "world_state",
    "inter_agent_communication_metadata",
];

/// Response item kinds observed on disk but not covered by the v1 function-tool transcript shape.
pub const UNSUPPORTED_RESPONSE_ITEM_TYPES: &[&str] = &[
    "custom_tool_call",
    "custom_tool_call_output",
    "agent_message",
    "web_search_call",
    "tool_search_call",
    "tool_search_output",
];

#[derive(Debug, Clone)]
pub struct CodexReader {
    sessions_root: PathBuf,
}

impl CodexReader {
    pub fn new(sessions_root: impl Into<PathBuf>) -> Self {
        Self {
            sessions_root: sessions_root.into(),
        }
    }

    pub fn default_path() -> PathBuf {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default()
            .join(".codex/sessions")
    }

    fn rollout_paths(&self) -> ReaderResult<Vec<PathBuf>> {
        let mut paths = Vec::new();
        collect_rollout_paths(&self.sessions_root, &mut paths)?;
        paths.sort_by(|left, right| right.cmp(left));
        Ok(paths)
    }

    fn find_session(&self, session_id: &str) -> ReaderResult<PathBuf> {
        for path in self.rollout_paths()? {
            // Selection needs only the first session_meta id. Once it matches, convert performs
            // the sole full parse, so any later torn row still fails the selected import loudly.
            if probe_rollout_id(&path)
                .ok()
                .flatten()
                .as_deref()
                .is_some_and(|id| id == session_id)
            {
                return Ok(path);
            }
        }
        Err(IoError::new(
            ErrorKind::NotFound,
            format!("Codex session '{session_id}' was not found"),
        )
        .into())
    }
}

impl Default for CodexReader {
    fn default() -> Self {
        Self::new(Self::default_path())
    }
}

impl HarnessReader for CodexReader {
    fn harness(&self) -> &'static str {
        "codex"
    }

    fn enumerate(&self) -> ReaderResult<Vec<SessionSummary>> {
        let mut sessions = Vec::new();
        for path in self.rollout_paths()? {
            let metadata = match inspect_rollout(&path) {
                Ok(metadata) => metadata,
                Err(error) => {
                    eprintln!(
                        "WARNING: skipped unreadable Codex rollout {} during enumeration: {error}",
                        path.display()
                    );
                    continue;
                }
            };
            sessions.push(SessionSummary {
                id: metadata.id,
                title: metadata.title,
                cwd: metadata.cwd,
                message_count: metadata.response_item_count,
                approximate_size_bytes: Some(fs::metadata(path)?.len()),
            });
        }
        Ok(sessions)
    }

    fn convert(&self, session_id: &str) -> ReaderResult<ImportedSession> {
        let path = self.find_session(session_id)?;
        convert_rollout(&path, session_id)
    }

    fn session_cwd(&self, session_id: &str) -> ReaderResult<Option<String>> {
        let path = self.find_session(session_id)?;
        Ok(inspect_rollout(&path)?.cwd)
    }
}

fn convert_rollout(path: &Path, session_id: &str) -> ReaderResult<ImportedSession> {
    let rows = read_rows(path)?;
    let mut messages = Vec::new();
    let mut conversion = ResponseConversion::default();
    let mut skipped_state = 0usize;
    let mut skipped_rows = Vec::new();

    for row in rows {
        let row_type = string_field(&row, "type").unwrap_or("");
        let payload = row.get("payload").unwrap_or(&Value::Null);
        match row_type {
            "session_meta" => update_model(&mut conversion.current_model, payload),
            "turn_context" => {
                conversion.batch.flush(&mut messages);
                update_model(&mut conversion.current_model, payload);
                skipped_state += 1;
            }
            "response_item" => convert_response_item(payload, &mut messages, &mut conversion),
            "compacted" => {
                conversion.batch.flush(&mut messages);
                // Codex replacement history supersedes the transcript before this row. Keeping
                // both would replay compacted context and can make the imported history exceed
                // what the source session actually retained.
                messages.clear();
                if let Some(replacement) =
                    payload.get("replacement_history").and_then(Value::as_array)
                {
                    if replacement.is_empty() {
                        conversion.warnings.push(
                            "Codex compaction has an empty replacement_history; older history was omitted"
                                .into(),
                        );
                    }
                    for item in replacement {
                        convert_response_item(item, &mut messages, &mut conversion);
                    }
                } else if let Some(summary) = payload.get("message").and_then(Value::as_str) {
                    if !summary.is_empty() {
                        messages.push(Message::new(
                            Role::User,
                            vec![ContentBlock::Text {
                                text: summary.to_string(),
                            }],
                        ));
                    } else {
                        conversion.warnings.push(
                            "Codex compaction has an empty legacy summary; older history was omitted"
                                .into(),
                        );
                    }
                } else {
                    conversion.warnings.push(
                        "Codex compaction has no supported replacement_history or legacy summary; older history was omitted"
                            .into(),
                    );
                }
            }
            kind if STATE_ROW_TYPES.contains(&kind) => skipped_state += 1,
            other => remember_kind(&mut skipped_rows, other),
        }
    }
    conversion.batch.flush(&mut messages);

    if skipped_state > 0 {
        conversion.warnings.push(format!(
            "Skipped {skipped_state} Codex event/state rows ({})",
            STATE_ROW_TYPES
                .iter()
                .copied()
                .filter(|kind| *kind != "compacted")
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if conversion.skipped_system > 0 {
        conversion.warnings.push(format!(
            "Dropped {} Codex system/developer messages",
            conversion.skipped_system
        ));
    }
    if conversion.dropped_reasoning > 0 {
        conversion.warnings.push(format!(
            "Dropped {} encrypted Codex reasoning items",
            conversion.dropped_reasoning
        ));
    }
    if !conversion.skipped_items.is_empty() {
        conversion.warnings.push(format!(
            "Skipped unsupported Codex response item kinds: {}",
            conversion.skipped_items.join(", ")
        ));
    }
    if !skipped_rows.is_empty() {
        conversion.warnings.push(format!(
            "Skipped unsupported Codex row types: {}",
            skipped_rows.join(", ")
        ));
    }

    finalize_import(
        messages,
        format!("codex:{session_id}"),
        conversion.warnings,
        "codex",
    )
}

#[derive(Debug)]
struct RolloutMetadata {
    id: String,
    title: Option<String>,
    cwd: Option<String>,
    response_item_count: usize,
}

fn collect_rollout_paths(directory: &Path, paths: &mut Vec<PathBuf>) -> ReaderResult<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_rollout_paths(&entry.path(), paths)?;
        } else if file_type.is_file()
            && entry.path().extension().and_then(|value| value.to_str()) == Some("jsonl")
            && entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("rollout-"))
        {
            paths.push(entry.path());
        }
    }
    Ok(())
}

fn probe_rollout_id(path: &Path) -> ReaderResult<Option<String>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut bytes_read = 0;
    let mut index = 0;
    while read_bounded_jsonl_line(&mut reader, &mut line, &mut bytes_read).map_err(|error| {
        IoError::new(
            ErrorKind::InvalidData,
            format!(
                "read Codex rollout {} line {}: {error}",
                path.display(),
                index + 1
            ),
        )
    })? {
        let row = decode_rollout_row(path, index, &line)?;
        if string_field(&row, "type") == Some("session_meta") {
            return Ok(row
                .get("payload")
                .and_then(|payload| string_field(payload, "id"))
                .map(str::to_owned));
        }
        index += 1;
    }
    Ok(None)
}

fn inspect_rollout(path: &Path) -> ReaderResult<RolloutMetadata> {
    check_session_source_size(path)?;
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut bytes_read = 0;
    let mut line_index = 0;
    let mut id = None;
    let mut cwd = None;
    let mut title = None;
    let mut response_item_count = 0usize;

    while read_bounded_jsonl_line(&mut reader, &mut line, &mut bytes_read).map_err(|error| {
        IoError::new(
            ErrorKind::InvalidData,
            format!(
                "read Codex rollout {} line {}: {error}",
                path.display(),
                line_index + 1
            ),
        )
    })? {
        let row = decode_rollout_row(path, line_index, &line)?;
        line_index += 1;
        let row_type = string_field(&row, "type").unwrap_or("");
        let payload = row.get("payload").unwrap_or(&Value::Null);
        if row_type == "session_meta" && id.is_none() {
            id = string_field(payload, "id").map(str::to_owned);
            cwd = string_field(payload, "cwd")
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned);
        } else if row_type == "response_item" {
            response_item_count += 1;
            if title.is_none()
                && string_field(payload, "type") == Some("message")
                && string_field(payload, "role") == Some("user")
            {
                title = first_text(payload, "input_text").map(truncate_session_title);
            }
        }
    }

    let id = id.ok_or_else(|| {
        IoError::new(
            ErrorKind::InvalidData,
            format!("Codex rollout {} has no session_meta id", path.display()),
        )
    })?;
    Ok(RolloutMetadata {
        id,
        title,
        cwd,
        response_item_count,
    })
}

fn read_rows(path: &Path) -> ReaderResult<Vec<Value>> {
    #[cfg(test)]
    FULL_PARSE_COUNT.with(|count| count.set(count.get() + 1));

    check_session_source_size(path)?;
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut bytes_read = 0;
    let mut rows = Vec::new();
    let mut index = 0;
    while read_bounded_jsonl_line(&mut reader, &mut line, &mut bytes_read).map_err(|error| {
        IoError::new(
            ErrorKind::InvalidData,
            format!(
                "read Codex rollout {} line {}: {error}",
                path.display(),
                index + 1
            ),
        )
    })? {
        rows.push(decode_rollout_row(path, index, &line)?);
        index += 1;
    }
    Ok(rows)
}

fn decode_rollout_row(path: &Path, index: usize, line: &[u8]) -> ReaderResult<Value> {
    serde_json::from_slice(line).map_err(|error| {
        IoError::new(
            ErrorKind::InvalidData,
            format!(
                "decode Codex rollout {} line {}: {error}",
                path.display(),
                index + 1
            ),
        )
        .into()
    })
}

#[cfg(test)]
fn take_full_parse_count() -> usize {
    FULL_PARSE_COUNT.with(|count| count.replace(0))
}

#[derive(Default)]
struct ResponseConversion {
    current_model: Option<String>,
    batch: ResponseItemBatch,
    skipped_system: usize,
    dropped_reasoning: usize,
    skipped_items: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Default)]
struct ResponseItemBatch {
    role: Option<Role>,
    assistant_model: Option<String>,
    blocks: Vec<ContentBlock>,
}

impl ResponseItemBatch {
    fn push(
        &mut self,
        messages: &mut Vec<Message>,
        role: Role,
        block: ContentBlock,
        assistant_model: Option<&str>,
    ) {
        let assistant_model = if role == Role::Assistant {
            assistant_model.map(str::to_owned)
        } else {
            None
        };
        if self.role.is_some()
            && (self.role != Some(role) || self.assistant_model != assistant_model)
        {
            self.flush(messages);
        }
        self.role = Some(role);
        self.assistant_model = assistant_model;
        self.blocks.push(block);
    }

    fn flush(&mut self, messages: &mut Vec<Message>) {
        let Some(role) = self.role.take() else {
            return;
        };
        let blocks = std::mem::take(&mut self.blocks);
        if role == Role::Assistant {
            push_assistant(messages, blocks, self.assistant_model.take().as_deref());
        } else {
            self.assistant_model = None;
            messages.push(Message::new(role, blocks));
        }
    }
}

fn convert_response_item(
    item: &Value,
    messages: &mut Vec<Message>,
    conversion: &mut ResponseConversion,
) {
    if let Some(model) = item_model(item) {
        conversion.current_model = Some(model.to_string());
    }
    match string_field(item, "type").unwrap_or("") {
        "function_call" => {
            let call_id = string_field(item, "call_id").unwrap_or("");
            let tool_name = string_field(item, "name").unwrap_or("unknown");
            let raw_arguments = string_field(item, "arguments").unwrap_or("null");
            let input = serde_json::from_str(raw_arguments).unwrap_or_else(|error| {
                conversion.warnings.push(format!(
                    "Preserved invalid JSON arguments for Codex tool call {call_id}: {error}"
                ));
                Value::String(raw_arguments.to_string())
            });
            let block = ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId(call_id.to_string()),
                tool_name: tool_name.to_string(),
                input,
                thought_signature: None,
            });
            conversion.batch.push(
                messages,
                Role::Assistant,
                block,
                conversion.current_model.as_deref(),
            );
        }
        "function_call_output" => {
            let call_id = string_field(item, "call_id").unwrap_or("");
            let output = item.get("output").cloned().unwrap_or(Value::Null);
            let output = match output {
                Value::String(text) => ToolOutput::Text { text },
                value => ToolOutput::Json { value },
            };
            conversion.batch.push(
                messages,
                Role::Tool,
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId(call_id.to_string()),
                    output,
                    is_error: false,
                }),
                None,
            );
        }
        "message" => {
            conversion.batch.flush(messages);
            convert_message(
                item,
                messages,
                conversion.current_model.as_deref(),
                &mut conversion.skipped_system,
            );
        }
        "reasoning" => {
            conversion.batch.flush(messages);
            conversion.dropped_reasoning += 1;
        }
        other => {
            conversion.batch.flush(messages);
            remember_kind(&mut conversion.skipped_items, other);
        }
    }
}

fn convert_message(
    item: &Value,
    messages: &mut Vec<Message>,
    model: Option<&str>,
    skipped_system: &mut usize,
) {
    let role = string_field(item, "role").unwrap_or("");
    if matches!(role, "system" | "developer") {
        *skipped_system += 1;
        return;
    }
    let canonical_role = match role {
        "user" => Role::User,
        "assistant" => Role::Assistant,
        _ => return,
    };
    let blocks = item
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|part| match string_field(part, "type") {
            Some("input_text" | "output_text") => {
                string_field(part, "text").map(|text| ContentBlock::Text {
                    text: text.to_string(),
                })
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if blocks.is_empty() {
        return;
    }
    if canonical_role == Role::Assistant {
        push_assistant(messages, blocks, model);
    } else {
        messages.push(Message::new(canonical_role, blocks));
    }
}

fn push_assistant(messages: &mut Vec<Message>, blocks: Vec<ContentBlock>, model: Option<&str>) {
    let message = match model {
        Some(model) => Message::assistant_from(blocks, ModelHandle::new("openai", model)),
        None => Message::new(Role::Assistant, blocks),
    };
    messages.push(message);
}

fn update_model(current_model: &mut Option<String>, value: &Value) {
    if let Some(model) = item_model(value) {
        *current_model = Some(model.to_string());
    }
}

fn item_model(value: &Value) -> Option<&str> {
    value
        .get("model")
        .or_else(|| value.get("model_id"))
        .and_then(Value::as_str)
        .filter(|model| !model.trim().is_empty())
}

fn first_text(item: &Value, part_type: &str) -> Option<String> {
    item.get("content")
        .and_then(Value::as_array)?
        .iter()
        .find(|part| string_field(part, "type") == Some(part_type))
        .and_then(|part| string_field(part, "text"))
        .filter(|text| !text.trim().is_empty())
        .map(str::to_owned)
}

fn string_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn remember_kind(kinds: &mut Vec<String>, kind: &str) {
    if !kind.is_empty() && !kinds.iter().any(|known| known == kind) {
        kinds.push(kind.to_string());
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use broca_module_serve::wire::{ImportOriginKind, ImportOriginParams, ImportParams};
    use broca_protocol::{ContentBlock, Role, ToolOutput};

    use super::{
        convert_rollout, inspect_rollout, take_full_parse_count, CodexReader, HarnessReader,
        STATE_ROW_TYPES, UNSUPPORTED_RESPONSE_ITEM_TYPES,
    };

    fn fixture() -> CodexReader {
        CodexReader::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex/sessions"),
        )
    }

    fn parallel_fixture() -> CodexReader {
        CodexReader::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex/parallel"),
        )
    }

    fn temp_root(label: &str) -> PathBuf {
        broca_test_support::scratch_root().join(format!("ck-codex-{label}-{}", std::process::id()))
    }

    fn write_rollout(path: &std::path::Path, id: &str, text: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let rows = [
            serde_json::json!({
                "timestamp": "2026-01-01T00:00:00Z",
                "type": "session_meta",
                "payload": { "id": id, "cwd": "/tmp/codex-probe-test" }
            }),
            serde_json::json!({
                "timestamp": "2026-01-01T00:00:01Z",
                "type": "turn_context",
                "payload": { "model": "gpt-probe-test" }
            }),
            serde_json::json!({
                "timestamp": "2026-01-01T00:00:02Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{ "type": "input_text", "text": text }]
                }
            }),
            serde_json::json!({
                "timestamp": "2026-01-01T00:00:03Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{ "type": "output_text", "text": format!("reply-{text}") }]
                }
            }),
        ];
        let body = rows
            .into_iter()
            .map(|row| row.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(path, format!("{body}\n")).unwrap();
    }

    fn legacy_find_session(reader: &CodexReader, session_id: &str) -> PathBuf {
        reader
            .rollout_paths()
            .unwrap()
            .into_iter()
            .find(|path| inspect_rollout(path).is_ok_and(|metadata| metadata.id == session_id))
            .expect("legacy full-parse selection must find the fixture session")
    }

    fn assert_wire_round_trip(imported: &crate::ImportedSession, import_id: &str) {
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

    #[test]
    fn enumerates_scrubbed_fixture_sessions() {
        let sessions = fixture().enumerate().unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].id, "codex-text");
        assert_eq!(sessions[1].id, "codex-tools");
        assert_eq!(
            sessions[1].title.as_deref(),
            Some("Inspect the scrubbed file.")
        );
        assert_eq!(sessions[1].cwd.as_deref(), Some("/tmp/scrubbed-project"));
        assert_eq!(
            fixture().session_cwd("codex-tools").unwrap().as_deref(),
            Some("/tmp/scrubbed-project")
        );
        assert_eq!(sessions[1].message_count, 9);
        assert!(sessions[1]
            .approximate_size_bytes
            .is_some_and(|size| size > 0));
    }

    #[test]
    fn enumeration_streams_rollouts_without_collecting_full_rows() {
        let _ = take_full_parse_count();

        let sessions = fixture().enumerate().unwrap();

        assert_eq!(sessions.len(), 2);
        assert_eq!(take_full_parse_count(), 0);
    }

    #[test]
    fn converts_tools_stamps_origin_and_strips_signed_reasoning() {
        let imported = fixture().convert("codex-tools").unwrap();
        assert_eq!(imported.source_ref, "codex:codex-tools");
        assert_eq!(
            imported
                .messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            [
                Role::User,
                Role::Assistant,
                Role::Assistant,
                Role::Tool,
                Role::Assistant,
            ]
        );
        assert!(!imported
            .messages
            .iter()
            .any(|message| message.role == Role::System));

        let assistants = imported
            .messages
            .iter()
            .filter(|message| message.role == Role::Assistant)
            .collect::<Vec<_>>();
        assert_eq!(assistants.len(), 3);
        let origins = assistants
            .iter()
            .map(|message| message.origin.as_ref().unwrap())
            .collect::<Vec<_>>();
        assert!(origins
            .iter()
            .all(|origin| origin.provider_module_id == "openai"));
        assert_eq!(origins[0].model_id, "gpt-scrubbed");
        assert_eq!(origins[1].model_id, "gpt-scrubbed");
        assert_eq!(origins[2].model_id, "gpt-scrubbed-next");
        let call = assistants
            .iter()
            .flat_map(|message| &message.content)
            .find_map(|block| match block {
                ContentBlock::ToolCall(call) => Some(call),
                _ => None,
            })
            .unwrap();
        assert_eq!(call.tool_name, "read_scrubbed");
        assert_eq!(call.input["path"], "/tmp/example.txt");
        assert!(call.thought_signature.is_none());

        let tool = imported
            .messages
            .iter()
            .find(|message| message.role == Role::Tool)
            .unwrap();
        assert!(matches!(
            &tool.content[0],
            ContentBlock::ToolResult(result)
                if matches!(&result.output, ToolOutput::Text { text } if text == "scrubbed output")
        ));
        assert!(!imported
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .any(|block| matches!(
                block,
                ContentBlock::Reasoning { .. } | ContentBlock::RedactedReasoning { .. }
            )));
        let canonical_json = serde_json::to_string(&imported.messages).unwrap();
        assert!(!canonical_json.contains("SIGNED-REASONING-MUST-NOT-SURVIVE"));
        assert!(!canonical_json.contains("SIGNED-CALL-MUST-NOT-SURVIVE"));
        assert!(!canonical_json.contains("SYSTEM-PROMPT-MUST-NOT-SURVIVE"));
        assert_wire_round_trip(&imported, "codex-tool-round-trip");
    }

    #[test]
    fn codex_compaction_replaces_older_history_and_keeps_following_turns() {
        let root = temp_root("compaction");
        let path = root.join("2026/01/01/rollout-compacted.jsonl");
        let rows = [
            serde_json::json!({"type":"session_meta","payload":{"id":"compacted","cwd":"/tmp/project"}}),
            serde_json::json!({"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"OLD_CONTEXT_MUST_NOT_SURVIVE"}]}}),
            serde_json::json!({"type":"compacted","payload":{"replacement_history":[{"type":"message","role":"user","content":[{"type":"input_text","text":"REPLACEMENT_CONTEXT"}]}]}}),
            serde_json::json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"POST_COMPACTION_ANSWER"}]}}),
        ];
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            rows.iter()
                .map(serde_json::Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let imported = convert_rollout(&path, "compacted").unwrap();
        let transcript = serde_json::to_string(&imported.messages).unwrap();
        assert!(!transcript.contains("OLD_CONTEXT_MUST_NOT_SURVIVE"));
        assert!(transcript.contains("REPLACEMENT_CONTEXT"));
        assert!(transcript.contains("POST_COMPACTION_ANSWER"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn codex_legacy_compaction_summary_replaces_older_history() {
        let root = temp_root("legacy-compaction");
        let path = root.join("2026/01/01/rollout-legacy-compacted.jsonl");
        let rows = [
            serde_json::json!({"type":"session_meta","payload":{"id":"legacy-compacted"}}),
            serde_json::json!({"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"OLD_CONTEXT_MUST_NOT_SURVIVE"}]}}),
            serde_json::json!({"type":"compacted","payload":{"message":"LEGACY_COMPACTION_SUMMARY"}}),
        ];
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            rows.iter()
                .map(serde_json::Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let imported = convert_rollout(&path, "legacy-compacted").unwrap();
        let transcript = serde_json::to_string(&imported.messages).unwrap();
        assert!(!transcript.contains("OLD_CONTEXT_MUST_NOT_SURVIVE"));
        assert!(transcript.contains("LEGACY_COMPACTION_SUMMARY"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn selected_codex_rollout_rejects_an_oversized_jsonl_record() {
        let root = temp_root("line-cap");
        let path = root.join("2026/01/01/rollout-large.jsonl");
        let header = serde_json::json!({
            "type": "session_meta",
            "payload": {"id": "large", "cwd": "/tmp/project"}
        });
        let oversized = serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "x".repeat(crate::MAX_JSONL_LINE_BYTES)}]
            }
        });
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, format!("{}\n{}\n", header, oversized)).unwrap();

        let error = CodexReader::new(&root).convert("large").unwrap_err();

        assert!(error.to_string().contains("line limit"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn batches_parallel_function_calls_and_outputs_by_turn() {
        let imported = parallel_fixture().convert("codex-parallel-tools").unwrap();
        assert_eq!(
            imported
                .messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            [Role::User, Role::Assistant, Role::Tool]
        );

        let assistant = &imported.messages[1];
        assert_eq!(
            assistant
                .origin
                .as_ref()
                .map(|origin| (origin.provider_module_id.as_str(), origin.model_id.as_str())),
            Some(("openai", "gpt-parallel"))
        );
        let calls = assistant
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::ToolCall(call) => Some((
                    call.tool_call_id.0.as_str(),
                    call.tool_name.as_str(),
                    call.input["value"].as_i64(),
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            calls,
            [("c1", "lookup_one", Some(1)), ("c2", "lookup_two", Some(2))]
        );

        let outputs = imported.messages[2]
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::ToolResult(result) => match &result.output {
                    ToolOutput::Text { text } => {
                        Some((result.tool_call_id.0.as_str(), text.as_str()))
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(outputs, [("c1", "ok1"), ("c2", "ok2")]);
    }

    #[test]
    fn documents_skipped_real_container_kinds() {
        let imported = fixture().convert("codex-tools").unwrap();
        let warnings = imported.warnings.join("\n");
        assert!(warnings.contains("event/state"));
        assert!(warnings.contains("custom_tool_call"));
        assert!(warnings.contains("encrypted Codex reasoning"));
        assert_eq!(
            STATE_ROW_TYPES,
            [
                "turn_context",
                "compacted",
                "event_msg",
                "world_state",
                "inter_agent_communication_metadata"
            ]
        );
        assert!(UNSUPPORTED_RESPONSE_ITEM_TYPES.contains(&"agent_message"));
        assert!(UNSUPPORTED_RESPONSE_ITEM_TYPES.contains(&"web_search_call"));
        assert!(UNSUPPORTED_RESPONSE_ITEM_TYPES.contains(&"tool_search_call"));
    }

    #[test]
    fn selected_conversion_full_parses_only_the_matching_rollout() {
        let root = temp_root("selected-probe");
        let day = root.join("2026/01/01");
        let _ = fs::remove_dir_all(&root);
        write_rollout(&day.join("rollout-a-target.jsonl"), "target", "selected");
        for label in ["b", "c", "d"] {
            write_rollout(
                &day.join(format!("rollout-{label}-unrelated.jsonl")),
                &format!("unrelated-{label}"),
                "large-unrelated-body",
            );
        }
        let reader = CodexReader::new(&root);
        let _ = take_full_parse_count();

        let imported = reader.convert("target").unwrap();
        let full_parses = take_full_parse_count();

        assert_eq!(full_parses, 1, "only the selected rollout is fully parsed");
        assert!(serde_json::to_string(&imported.messages)
            .unwrap()
            .contains("selected"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn duplicate_session_ids_keep_reverse_sorted_freshness_winner() {
        let root = temp_root("duplicate-freshness");
        let _ = fs::remove_dir_all(&root);
        write_rollout(
            &root.join("2026/01/01/rollout-stale.jsonl"),
            "duplicate",
            "stale",
        );
        write_rollout(
            &root.join("2026/01/02/rollout-fresh.jsonl"),
            "duplicate",
            "fresh",
        );

        let imported = CodexReader::new(&root).convert("duplicate").unwrap();
        let encoded = serde_json::to_string(&imported.messages).unwrap();

        assert!(encoded.contains("fresh"));
        assert!(!encoded.contains("stale"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn probe_selection_matches_legacy_full_parse_output() {
        let root = temp_root("legacy-equivalence");
        let day = root.join("2026/01/01");
        let _ = fs::remove_dir_all(&root);
        write_rollout(
            &day.join("rollout-z-unrelated.jsonl"),
            "unrelated",
            "ignore-me",
        );
        write_rollout(
            &day.join("rollout-m-target.jsonl"),
            "target",
            "selected-history",
        );
        write_rollout(&day.join("rollout-a-older.jsonl"), "older", "older-history");
        let reader = CodexReader::new(&root);
        let legacy_path = legacy_find_session(&reader, "target");
        let legacy = convert_rollout(&legacy_path, "target").unwrap();

        let probed = reader.convert("target").unwrap();

        assert_eq!(
            serde_json::to_vec(&probed.messages).unwrap(),
            serde_json::to_vec(&legacy.messages).unwrap()
        );
        assert_eq!(probed, legacy);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn torn_sibling_is_skipped_but_selected_torn_rollout_is_fatal() {
        let root = broca_test_support::scratch_root()
            .join(format!("ck-codex-torn-sibling-{}", std::process::id()));
        let day = root.join("2026/01/01");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&day).unwrap();
        fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/codex/sessions/2026/01/02/rollout-2026-01-02T10-00-00-codex-text.jsonl"),
            day.join("rollout-valid.jsonl"),
        )
        .unwrap();
        fs::write(
            day.join("rollout-torn.jsonl"),
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-torn\"}}\n{\"type\":",
        )
        .unwrap();
        let reader = CodexReader::new(&root);

        let sessions = reader.enumerate().unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "codex-text");
        assert_eq!(reader.convert("codex-text").unwrap().messages.len(), 2);
        let selected_error = reader.convert("codex-torn").unwrap_err().to_string();
        assert!(selected_error.contains("decode Codex rollout"));
        assert!(selected_error.contains("line 2"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn text_only_session_round_trips_through_import_wire_schema() {
        let imported = fixture().convert("codex-text").unwrap();
        assert_eq!(imported.messages.len(), 2);
        assert_wire_round_trip(&imported, "codex-text-round-trip");
    }
}
