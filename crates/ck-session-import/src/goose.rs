//! Goose SQLite reader.
//!
//! Goose schema version 14, inspected from a live database, stores sessions in
//! `~/.local/share/goose/sessions/sessions.db`. Compared with the early design note, the message
//! payload column is named `content_json`, message ordering uses `created_timestamp`, and the
//! session row has `provider_name` plus a JSON `model_config_json` containing `model_name`.
//! Newer message metadata can carry an `inference` provider and requested/resolved model. That
//! per-message identity takes precedence; older assistant rows fall back to the session's saved
//! provider/model pair when both exist. Connections prefer SQLite's live read-only snapshot mode
//! so running Goose is not blocked and committed WAL frames remain visible.

use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};

use broca_protocol::{
    ContentBlock, Message, ModelHandle, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use rusqlite::OptionalExtension;
use serde_json::Value;

use crate::{
    finalize_import, omit_nested_tool_images,
    sqlite_read::{open_read_only_database, ReadOnlyDatabase},
    truncate_session_title, HarnessReader, ImportedSession, SessionSummary,
};

type ReaderResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Goose content kinds stripped because opaque provider reasoning cannot be replayed safely.
pub const STRIPPED_CONTENT_KINDS: &[&str] = &["redactedThinking"];

#[derive(Debug, Clone)]
pub struct GooseReader {
    database: PathBuf,
}

impl GooseReader {
    pub fn new(database: impl Into<PathBuf>) -> Self {
        Self {
            database: database.into(),
        }
    }

    pub fn default_path() -> PathBuf {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default()
            .join(".local/share/goose/sessions/sessions.db")
    }

    pub fn database_path(&self) -> &Path {
        &self.database
    }

    fn open(&self) -> ReaderResult<ReadOnlyDatabase> {
        open_read_only_database(&self.database, "Goose").map_err(Into::into)
    }
}

impl Default for GooseReader {
    fn default() -> Self {
        Self::new(Self::default_path())
    }
}

impl HarnessReader for GooseReader {
    fn harness(&self) -> &'static str {
        "goose"
    }

    fn enumerate(&self) -> ReaderResult<Vec<SessionSummary>> {
        let ReadOnlyDatabase {
            connection,
            fallback_warning,
            ..
        } = self.open()?;
        if let Some(warning) = fallback_warning {
            eprintln!("{warning}");
        }
        let mut statement = connection.prepare(
            "SELECT s.id, s.name, s.working_dir, COUNT(m.id) \
             FROM sessions s LEFT JOIN messages m ON m.session_id = s.id \
             GROUP BY s.id, s.name, s.working_dir, s.updated_at \
             ORDER BY s.updated_at DESC, s.id",
        )?;
        let rows = statement.query_map([], |row| {
            let title: String = row.get(1)?;
            let directory: String = row.get(2)?;
            let count: i64 = row.get(3)?;
            Ok(SessionSummary {
                id: row.get(0)?,
                title: nonempty(title).map(truncate_session_title),
                cwd: nonempty(directory),
                message_count: usize::try_from(count.max(0)).unwrap_or(usize::MAX),
                approximate_size_bytes: None,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn convert(&self, session_id: &str) -> ReaderResult<ImportedSession> {
        let ReadOnlyDatabase {
            connection,
            fallback_warning,
            ..
        } = self.open()?;
        let session = connection
            .query_row(
                "SELECT provider_name, model_config_json FROM sessions WHERE id = ?1",
                [session_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .optional()?;
        let Some((provider_name, model_config_json)) = session else {
            let fallback_detail = fallback_warning
                .as_deref()
                .map(|warning| format!("; {warning}"))
                .unwrap_or_default();
            return Err(IoError::new(
                ErrorKind::NotFound,
                format!("Goose session '{session_id}' was not found{fallback_detail}"),
            )
            .into());
        };

        let mut warnings: Vec<String> = fallback_warning.into_iter().collect();
        let origin = session_origin(provider_name, model_config_json, &mut warnings);
        let source_messages = {
            let mut statement = connection.prepare(
                "SELECT id, role, content_json, metadata_json FROM messages WHERE session_id = ?1 \
                 ORDER BY created_timestamp, id",
            )?;
            let rows = statement.query_map([session_id], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        let mut messages = Vec::new();
        let mut skipped_signed = 0usize;
        let mut omitted_images = 0usize;
        let mut skipped_unknown = Vec::new();

        for (row_id, role, raw_content, raw_metadata) in source_messages {
            if role == "system" {
                continue;
            }
            let source_role = match role.as_str() {
                "user" => Role::User,
                "assistant" => Role::Assistant,
                "tool" => Role::Tool,
                other => {
                    warnings.push(format!(
                        "Skipped Goose message row {row_id} with unsupported role '{other}'"
                    ));
                    continue;
                }
            };
            let message_origin = if source_role == Role::Assistant {
                message_origin(
                    raw_metadata.as_deref(),
                    origin.clone(),
                    row_id,
                    &mut warnings,
                )
            } else {
                None
            };
            let parts: Value = serde_json::from_str(&raw_content).map_err(|error| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("decode Goose message row {row_id}: {error}"),
                )
            })?;
            let parts = parts.as_array().ok_or_else(|| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("Goose message row {row_id} content_json is not an array"),
                )
            })?;

            let mut pending_role = None;
            let mut pending = Vec::new();
            for part in parts {
                let kind = part.get("type").and_then(Value::as_str).unwrap_or("");
                let converted = match kind {
                    "text" => part
                        .get("text")
                        .and_then(Value::as_str)
                        .map(|text| (source_role, ContentBlock::Text { text: text.into() })),
                    "thinking" | "reasoning" => reasoning_text(part).map(|text| {
                        (
                            source_role,
                            ContentBlock::Reasoning {
                                text: text.to_string(),
                                signature: None,
                            },
                        )
                    }),
                    "redactedThinking" => {
                        skipped_signed += 1;
                        None
                    }
                    "toolRequest" => convert_tool_request(part)
                        .map(|call| (source_role, ContentBlock::ToolCall(call))),
                    "toolResponse" => convert_tool_response(part, &mut omitted_images)
                        .map(|result| (Role::Tool, ContentBlock::ToolResult(result))),
                    other => {
                        if !skipped_unknown.iter().any(|known| known == other) {
                            skipped_unknown.push(other.to_string());
                        }
                        None
                    }
                };

                if let Some((block_role, block)) = converted {
                    if pending_role.is_some_and(|pending_role| pending_role != block_role) {
                        flush_message(
                            &mut messages,
                            pending_role.take().expect("pending role exists"),
                            message_origin.clone(),
                            &mut pending,
                        );
                    }
                    pending_role = Some(block_role);
                    pending.push(block);
                }
            }
            if let Some(role) = pending_role {
                flush_message(&mut messages, role, message_origin, &mut pending);
            }
        }

        if skipped_signed > 0 {
            warnings.push(format!(
                "Stripped {skipped_signed} opaque Goose reasoning parts ({})",
                STRIPPED_CONTENT_KINDS.join(", ")
            ));
        }
        if omitted_images > 0 {
            warnings.push(format!(
                "Omitted {omitted_images} unsupported Goose tool-result image block(s)"
            ));
        }
        if !skipped_unknown.is_empty() {
            warnings.push(format!(
                "Skipped unsupported Goose content kinds: {}",
                skipped_unknown.join(", ")
            ));
        }

        finalize_import(messages, format!("goose:{session_id}"), warnings, "goose")
    }

    fn session_cwd(&self, session_id: &str) -> ReaderResult<Option<String>> {
        let ReadOnlyDatabase { connection, .. } = self.open()?;
        let directory = connection
            .query_row(
                "SELECT working_dir FROM sessions WHERE id = ?1",
                [session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(directory.and_then(nonempty))
    }
}

fn nonempty(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn session_origin(
    provider_name: Option<String>,
    model_config_json: Option<String>,
    warnings: &mut Vec<String>,
) -> Option<ModelHandle> {
    let provider = provider_name.and_then(nonempty)?;
    let raw_config = model_config_json?;
    let config: Value = match serde_json::from_str(&raw_config) {
        Ok(config) => config,
        Err(error) => {
            warnings.push(format!(
                "Could not decode Goose session model metadata; session-level origin fallback was omitted: {error}"
            ));
            return None;
        }
    };
    let model = config
        .get("model_name")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .and_then(nonempty)?;
    Some(ModelHandle::new(provider, model))
}

fn message_origin(
    raw_metadata: Option<&str>,
    fallback: Option<ModelHandle>,
    row_id: i64,
    warnings: &mut Vec<String>,
) -> Option<ModelHandle> {
    let Some(raw_metadata) = raw_metadata else {
        return fallback;
    };
    let metadata: Value = match serde_json::from_str(raw_metadata) {
        Ok(metadata) => metadata,
        Err(error) => {
            warnings.push(format!(
                "Could not decode Goose message row {row_id} inference metadata; used session origin: {error}"
            ));
            return fallback;
        }
    };
    let Some(inference) = metadata.get("inference") else {
        return fallback;
    };
    let Some(provider) = inference
        .get("provider")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    else {
        return fallback;
    };
    let Some(model) = inference
        .get("resolvedModel")
        .and_then(Value::as_str)
        .or_else(|| inference.get("requestedModel").and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty())
    else {
        return fallback;
    };
    Some(ModelHandle::new(provider, model))
}

fn reasoning_text(part: &Value) -> Option<&str> {
    part.get("text")
        .or_else(|| part.get("thinking"))
        .or_else(|| part.get("reasoning"))
        .and_then(Value::as_str)
}

fn convert_tool_request(part: &Value) -> Option<ToolCall> {
    let call_id = part.get("id").and_then(Value::as_str).unwrap_or("");
    let call = part.get("toolCall")?;
    let value = call.get("value")?;
    let tool_name = value.get("name").and_then(Value::as_str)?;
    Some(ToolCall {
        tool_call_id: ToolCallId(call_id.to_string()),
        tool_name: tool_name.to_string(),
        input: value.get("arguments").cloned().unwrap_or(Value::Null),
        thought_signature: None,
    })
}

fn convert_tool_response(part: &Value, omitted_images: &mut usize) -> Option<ToolResult> {
    let call_id = part.get("id").and_then(Value::as_str)?;
    let result = part.get("toolResult")?;
    let status = result.get("status").and_then(Value::as_str).unwrap_or("");
    let (output, is_error) = if status == "success" {
        let value = result.get("value").unwrap_or(&Value::Null);
        let is_error = value
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mut content = if value.is_array() {
            value.clone()
        } else {
            value.get("content").cloned().unwrap_or(Value::Null)
        };
        *omitted_images += omit_nested_tool_images(&mut content);
        if is_error {
            (ToolOutput::error(output_text(&content)), true)
        } else if content_is_empty(&content) {
            // An imported tool result is replayed to a model, so `content` is the field that
            // matters: MCP defines it as the model-facing result and `structuredContent` as a
            // machine-readable duplicate for programmatic callers. Preferring the structured
            // form whenever it exists would discard the text the original model actually saw.
            // Fall back to it only when there is no content to replay, and unwrap it -- the
            // surrounding CallToolResult envelope is transport framing, not tool output.
            match value.get("structuredContent") {
                Some(structured_content) => (
                    ToolOutput::Json {
                        value: structured_content.clone(),
                    },
                    false,
                ),
                None => (content_output(content), false),
            }
        } else {
            (content_output(content), false)
        }
    } else {
        let mut error = result
            .get("error")
            .cloned()
            .unwrap_or_else(|| Value::String("Goose tool execution failed".into()));
        *omitted_images += omit_nested_tool_images(&mut error);
        (ToolOutput::error(value_text(&error)), true)
    };
    Some(ToolResult {
        tool_call_id: ToolCallId(call_id.to_string()),
        output,
        is_error,
    })
}

/// Whether a `CallToolResult` carries no content blocks to replay to a model.
///
/// Absent and empty-array are both "nothing to replay": Goose omits the field entirely for
/// structured-only results, while an MCP server that returns an explicit `[]` means the same
/// thing. Treating only one of those as empty would route otherwise-identical results down
/// different branches.
fn content_is_empty(content: &Value) -> bool {
    match content {
        Value::Null => true,
        Value::Array(parts) => parts.is_empty(),
        _ => false,
    }
}

fn content_output(content: Value) -> ToolOutput {
    let Some(parts) = content.as_array() else {
        return ToolOutput::Json { value: content };
    };
    let texts: Option<Vec<&str>> = parts
        .iter()
        .map(|part| {
            (part.get("type").and_then(Value::as_str) == Some("text"))
                .then(|| part.get("text").and_then(Value::as_str))
                .flatten()
        })
        .collect();
    match texts {
        Some(texts) => ToolOutput::Text {
            text: texts.join("\n"),
        },
        None => ToolOutput::Json { value: content },
    }
}

fn output_text(content: &Value) -> String {
    match content_output(content.clone()) {
        // Media ingress requires owned captures; this text adapter never emits references.
        ToolOutput::Parts { parts } => broca_protocol::content::media_parts_text(
            &parts,
            broca_protocol::MediaMarkerReason::NotRequested,
        ),
        ToolOutput::Text { text } => text,
        ToolOutput::Json { value } => value_text(&value),
        ToolOutput::Denied { reason } => reason,
        ToolOutput::Error { message, .. } => message,
    }
}

fn value_text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn flush_message(
    messages: &mut Vec<Message>,
    role: Role,
    origin: Option<ModelHandle>,
    blocks: &mut Vec<ContentBlock>,
) {
    if blocks.is_empty() {
        return;
    }
    let content = std::mem::take(blocks);
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use broca_module_serve::wire::{ImportOriginKind, ImportOriginParams, ImportParams};
    use broca_protocol::{ContentBlock, Role, ToolOutput};
    use serde_json::json;

    use super::{convert_tool_response, GooseReader, HarnessReader, STRIPPED_CONTENT_KINDS};

    fn fixture() -> GooseReader {
        GooseReader::new(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/goose.db"))
    }

    #[test]
    fn enumerates_scrubbed_fixture_sessions() {
        let sessions = fixture().enumerate().unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].id, "goose-tools");
        assert_eq!(sessions[0].title.as_deref(), Some("Tool fixture"));
        assert_eq!(sessions[0].cwd.as_deref(), Some("/tmp/scrubbed-goose"));
        assert_eq!(
            fixture().session_cwd("goose-tools").unwrap().as_deref(),
            Some("/tmp/scrubbed-goose")
        );
        assert_eq!(sessions[0].message_count, 5);
        assert!(sessions[0].approximate_size_bytes.is_none());
    }

    #[test]
    fn sqlite_size_estimate_is_deferred_until_import() {
        let path = broca_test_support::scratch_root()
            .join(format!("ck-goose-bytes-{}.db", std::process::id()));
        let _ = fs::remove_file(&path);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE sessions (id TEXT, name TEXT, working_dir TEXT, updated_at INTEGER);\n\
                 CREATE TABLE messages (id INTEGER, session_id TEXT, role TEXT, content_json TEXT, metadata_json TEXT, created_timestamp INTEGER);\n\
                 INSERT INTO sessions VALUES ('bytes', 'T', '/tmp', 1);\n\
                 INSERT INTO messages VALUES (1, 'bytes', 'user', 'é', '🙂', 1);",
            )
            .unwrap();
        drop(connection);

        let summary = GooseReader::new(&path).enumerate().unwrap().remove(0);
        assert_eq!(summary.approximate_size_bytes, None);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn tool_result_images_are_omitted_and_text_is_retained() {
        let part = json!({
            "id": "call-image",
            "toolResult": {
                "status": "success",
                "value": {"content": [
                    {"type": "text", "text": "before image"},
                    {"type": "image", "source": {"type": "base64", "data": "PRIVATE_BASE64"}},
                    {"type": "text", "text": "after image"}
                ]}
            }
        });
        let mut omitted_images = 0;
        let result = convert_tool_response(&part, &mut omitted_images).unwrap();
        assert_eq!(omitted_images, 1);
        assert!(matches!(
            result.output,
            ToolOutput::Text { ref text }
                if text.contains("before image")
                    && text.contains("after image")
                    && text.contains("image omitted from imported tool output")
                    && !text.contains("PRIVATE_BASE64")
        ));
    }

    #[test]
    fn converts_tools_reasoning_origin_and_documents_skipped_kinds() {
        let imported = fixture().convert("goose-tools").unwrap();
        assert_eq!(imported.source_ref, "goose:goose-tools");
        assert!(imported
            .warnings
            .iter()
            .any(|warning| warning.contains(STRIPPED_CONTENT_KINDS[0])));
        assert!(!imported
            .messages
            .iter()
            .any(|message| message.role == Role::System));

        let assistants: Vec<_> = imported
            .messages
            .iter()
            .filter(|message| message.role == Role::Assistant)
            .collect();
        let assistant = assistants[0];
        assert_eq!(
            assistant.origin.as_ref().unwrap().provider_module_id,
            "anthropic"
        );
        assert_eq!(
            assistant.origin.as_ref().unwrap().model_id,
            "claude-scrubbed"
        );
        assert_eq!(
            assistants[1].origin.as_ref().unwrap().provider_module_id,
            "fallback-provider"
        );
        assert_eq!(
            assistants[1].origin.as_ref().unwrap().model_id,
            "fallback-model"
        );
        assert!(assistant.content.iter().any(|block| matches!(
            block,
            ContentBlock::Reasoning {
                text,
                signature: None
            } if text == "Unsigned imported thought."
        )));
        assert!(assistant.content.iter().any(|block| matches!(
            block,
            ContentBlock::ToolCall(call)
                if call.tool_call_id.0 == "call-scrubbed"
                    && call.tool_name == "lookup"
                    && call.thought_signature.is_none()
        )));
        assert!(!assistant
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
                if matches!(&result.output, ToolOutput::Text { text } if text == "scrubbed output")
        ));

        let params = ImportParams {
            messages: imported.messages.clone(),
            import_id: "goose-tool-round-trip".into(),
            origin: ImportOriginParams {
                kind: ImportOriginKind::Import,
                source_ref: imported.source_ref,
            },
        };
        let decoded: ImportParams =
            serde_json::from_slice(&serde_json::to_vec(&params).unwrap()).unwrap();
        assert_eq!(decoded.messages, imported.messages);
    }

    #[test]
    fn structured_tool_response_imports_the_structured_value() {
        let path = broca_test_support::scratch_root().join(format!(
            "ck-goose-structured-tool-{}.db",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE sessions (id TEXT PRIMARY KEY, provider_name TEXT, model_config_json TEXT);\n\
                 CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT, content_json TEXT, metadata_json TEXT, created_timestamp INTEGER);",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO sessions VALUES (?1, NULL, NULL)",
                ["structured-tool"],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages VALUES (1, ?1, 'assistant', ?2, NULL, 1)",
                rusqlite::params![
                    "structured-tool",
                    serde_json::json!([{
                        "type": "toolRequest",
                        "id": "call-structured",
                        "toolCall": {
                            "status": "success",
                            "value": {"name": "weather", "arguments": {"city": "Paris"}}
                        }
                    }])
                    .to_string()
                ],
            )
            .unwrap();
        let structured = serde_json::json!({"temperature": 12, "unit": "C"});
        connection
            .execute(
                "INSERT INTO messages VALUES (2, ?1, 'user', ?2, NULL, 2)",
                rusqlite::params![
                    "structured-tool",
                    serde_json::json!([{
                        "type": "toolResponse",
                        "id": "call-structured",
                        "toolResult": {
                            "status": "success",
                            "value": {
                                "content": [],
                                "structuredContent": structured.clone(),
                                "isError": false
                            }
                        }
                    }])
                    .to_string()
                ],
            )
            .unwrap();
        drop(connection);

        let imported = GooseReader::new(&path).convert("structured-tool").unwrap();
        let result = imported
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .find_map(|block| match block {
                ContentBlock::ToolResult(result) => Some(result),
                _ => None,
            })
            .unwrap();
        assert!(!result.is_error);
        assert_eq!(result.output, ToolOutput::Json { value: structured });

        let _ = fs::remove_file(path);
    }

    /// Text a model was shown must survive import even when a structured duplicate exists.
    ///
    /// MCP servers may return both `content` and `structuredContent` for one call: the first is
    /// what the model read, the second is the same result for programmatic callers. An importer
    /// that prefers the structured form whenever it is present silently rewrites the transcript,
    /// replacing the text the assistant reasoned over with a machine-readable object it never
    /// saw. The imported conversation stays well-formed, so nothing downstream reports it.
    #[test]
    fn structured_content_does_not_displace_text_the_model_saw() {
        let path = broca_test_support::scratch_root().join(format!(
            "ck-goose-structured-and-text-{}.db",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE sessions (id TEXT PRIMARY KEY, provider_name TEXT, model_config_json TEXT);\n\
                 CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT, content_json TEXT, metadata_json TEXT, created_timestamp INTEGER);",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO sessions VALUES (?1, NULL, NULL)",
                ["structured-and-text"],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages VALUES (1, ?1, 'assistant', ?2, NULL, 1)",
                rusqlite::params![
                    "structured-and-text",
                    serde_json::json!([{
                        "type": "toolRequest",
                        "id": "call-both",
                        "toolCall": {
                            "status": "success",
                            "value": {"name": "weather", "arguments": {"city": "Paris"}}
                        }
                    }])
                    .to_string()
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages VALUES (2, ?1, 'user', ?2, NULL, 2)",
                rusqlite::params![
                    "structured-and-text",
                    serde_json::json!([{
                        "type": "toolResponse",
                        "id": "call-both",
                        "toolResult": {
                            "status": "success",
                            "value": {
                                "content": [{"type": "text", "text": "Paris: 12C, clear."}],
                                "structuredContent": {"temperature": 12, "unit": "C"},
                                "isError": false
                            }
                        }
                    }])
                    .to_string()
                ],
            )
            .unwrap();
        drop(connection);

        let imported = GooseReader::new(&path)
            .convert("structured-and-text")
            .unwrap();
        let result = imported
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .find_map(|block| match block {
                ContentBlock::ToolResult(result) => Some(result),
                _ => None,
            })
            .unwrap();
        assert!(!result.is_error);
        assert_eq!(
            result.output,
            ToolOutput::Text {
                text: "Paris: 12C, clear.".to_string()
            },
            "content text must win over a structured duplicate, or import rewrites what the \
             model was shown"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn signed_reasoning_is_reverted_to_unsigned_content() {
        let imported = fixture().convert("goose-tools").unwrap();
        let reasoning = imported
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .find_map(|block| match block {
                ContentBlock::Reasoning { text, signature } => Some((text, signature)),
                _ => None,
            })
            .unwrap();
        assert_eq!(reasoning.0, "Unsigned imported thought.");
        assert!(reasoning.1.is_none());
        assert!(!serde_json::to_string(&imported.messages)
            .unwrap()
            .contains("foreign-signature-must-not-survive"));
    }

    #[test]
    fn text_only_session_round_trips_through_import_wire_schema() {
        let imported = fixture().convert("goose-text").unwrap();
        let params = ImportParams {
            messages: imported.messages.clone(),
            import_id: "goose-text-round-trip".into(),
            origin: ImportOriginParams {
                kind: ImportOriginKind::Import,
                source_ref: imported.source_ref,
            },
        };
        let decoded: ImportParams =
            serde_json::from_slice(&serde_json::to_vec(&params).unwrap()).unwrap();
        assert_eq!(decoded.messages, imported.messages);
        assert_eq!(decoded.messages.len(), 2);
        assert!(decoded
            .messages
            .iter()
            .all(|message| message.origin.is_none()));
    }
}
