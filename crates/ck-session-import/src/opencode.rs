//! OpenCode SQLite reader.
//!
//! Current OpenCode stores sessions in `~/.local/share/opencode/opencode.db`. The legacy
//! `storage/message` directory layout is intentionally out of scope and may become a fallback
//! in a later milestone. Database connections prefer SQLite's live read-only snapshot mode so
//! importing sees committed WAL frames without mutating or blocking the user's harness database.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};

use broca_protocol::{
    ContentBlock, Message, ModelHandle, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use rusqlite::{Connection, ErrorCode, OptionalExtension};
use serde_json::Value;

use crate::{
    build_coordinate_map, finalize_import,
    sqlite_read::{open_read_only_database, ReadOnlyDatabase},
    truncate_session_title, HarnessReader, ImportedSession, OriginMessageCoordinate,
    SessionSummary,
};

type ReaderResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// The read-only snapshot changed while SQLite was reading it and can be retried safely.
#[derive(Debug)]
pub struct RetryableOpenCodeSnapshotError {
    action: &'static str,
    source: rusqlite::Error,
}

impl fmt::Display for RetryableOpenCodeSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "OpenCode database changed while attempting to {}; retry after the current OpenCode write finishes: {}",
            self.action, self.source
        )
    }
}

impl Error for RetryableOpenCodeSnapshotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Part kinds observed in OpenCode that describe UI/checkpoint state rather than model history.
pub const UI_ONLY_PART_KINDS: &[&str] = &[
    "step-start",
    "step-finish",
    "snapshot",
    "patch",
    "file",
    "compaction",
];

#[derive(Debug, Clone)]
pub struct OpenCodeReader {
    database: PathBuf,
}

impl OpenCodeReader {
    pub fn new(database: impl Into<PathBuf>) -> Self {
        Self {
            database: database.into(),
        }
    }

    pub fn default_path() -> PathBuf {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default()
            .join(".local/share/opencode/opencode.db")
    }

    pub fn database_path(&self) -> &Path {
        &self.database
    }

    fn open(&self) -> ReaderResult<ReadOnlyDatabase> {
        open_read_only_database(&self.database, "OpenCode")
            .map_err(|error| sqlite_snapshot_error("open the database", error))
    }
}

impl Default for OpenCodeReader {
    fn default() -> Self {
        Self::new(Self::default_path())
    }
}

impl HarnessReader for OpenCodeReader {
    fn harness(&self) -> &'static str {
        "opencode"
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
        let mut statement = connection
            .prepare(
                "SELECT s.id, s.title, s.directory, COUNT(DISTINCT m.id) \
                 FROM session s LEFT JOIN message m ON m.session_id = s.id \
                 GROUP BY s.id, s.title, s.directory, s.time_updated \
                 ORDER BY s.time_updated DESC, s.id",
            )
            .map_err(|error| sqlite_snapshot_error("prepare session enumeration", error))?;
        let rows = statement
            .query_map([], |row| {
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
            })
            .map_err(|error| sqlite_snapshot_error("enumerate sessions", error))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| sqlite_snapshot_error("enumerate sessions", error))
    }

    fn convert(&self, session_id: &str) -> ReaderResult<ImportedSession> {
        let ReadOnlyDatabase {
            connection,
            fallback_warning,
            ..
        } = self.open()?;
        let exists = connection
            .query_row("SELECT 1 FROM session WHERE id = ?1", [session_id], |_| {
                Ok(())
            })
            .optional()
            .map_err(|error| sqlite_snapshot_error("find the selected session", error))?
            .is_some();
        if !exists {
            let fallback_detail = fallback_warning
                .as_deref()
                .map(|warning| format!("; {warning}"))
                .unwrap_or_default();
            return Err(IoError::new(
                ErrorKind::NotFound,
                format!("OpenCode session '{session_id}' was not found{fallback_detail}"),
            )
            .into());
        }

        let source_rows = load_messages(&connection, session_id)?;
        let parts_by_message = load_parts(&connection, session_id)?;
        convert_rows(source_rows, parts_by_message, fallback_warning, session_id)
    }

    fn session_cwd(&self, session_id: &str) -> ReaderResult<Option<String>> {
        let ReadOnlyDatabase { connection, .. } = self.open()?;
        let directory = connection
            .query_row(
                "SELECT directory FROM session WHERE id = ?1",
                [session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| sqlite_snapshot_error("look up selected session cwd", error))?;
        Ok(directory.and_then(nonempty))
    }
}

// OpenCode messages receive zero-based ordinals after sorting by `time_created` and then `id`.
// Saved compartment boundaries still match through their persisted source IDs: older snapshots
// may use one-based ordinals, and inserting an earlier message can shift calculated positions.
fn load_messages(
    connection: &Connection,
    session_id: &str,
) -> ReaderResult<Vec<(String, String, i64)>> {
    let mut statement = connection
        .prepare(
            "SELECT id, data, time_created FROM message WHERE session_id = ?1 \
             ORDER BY time_created, id",
        )
        .map_err(|error| sqlite_snapshot_error("prepare selected messages", error))?;
    let rows = statement
        .query_map([session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(|error| sqlite_snapshot_error("read selected messages", error))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| sqlite_snapshot_error("read selected messages", error))
}

fn convert_rows(
    source_rows: Vec<(String, String, i64)>,
    mut parts_by_message: HashMap<String, Vec<String>>,
    fallback_warning: Option<String>,
    session_id: &str,
) -> ReaderResult<ImportedSession> {
    let mut messages = Vec::new();
    let mut warnings: Vec<String> = fallback_warning.into_iter().collect();
    let mut skipped_ui = 0usize;
    let mut skipped_unknown = Vec::new();
    let mut origin_messages = Vec::with_capacity(source_rows.len());
    let mut block_origins = Vec::new();

    for (origin_ordinal, (message_id, raw_message, time_created)) in
        source_rows.into_iter().enumerate()
    {
        let origin = OriginMessageCoordinate {
            ordinal: origin_ordinal as u64,
            message_id: message_id.clone(),
            date: unix_millis_date(time_created),
        };
        origin_messages.push(origin.clone());
        let messages_before = messages.len();
        let metadata: Value = serde_json::from_str(&raw_message).map_err(|error| {
            IoError::new(
                ErrorKind::InvalidData,
                format!("decode OpenCode message {message_id}: {error}"),
            )
        })?;
        let role = metadata.get("role").and_then(Value::as_str).unwrap_or("");
        if role == "system" {
            continue;
        }
        let canonical_role = match role {
            "user" => Role::User,
            "assistant" => Role::Assistant,
            "tool" => Role::Tool,
            other => {
                warnings.push(format!(
                    "Skipped OpenCode message {message_id} with unsupported role '{other}'"
                ));
                continue;
            }
        };
        let origin = (canonical_role == Role::Assistant)
            .then(|| model_origin(&metadata))
            .flatten();

        let parts = parts_by_message
            .remove(&message_id)
            .unwrap_or_default()
            .into_iter()
            .map(|raw_part| {
                serde_json::from_str(&raw_part).map_err(|error| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        format!("decode part for OpenCode message {message_id}: {error}"),
                    )
                })
            })
            .collect::<Result<Vec<Value>, _>>()?;
        convert_message_parts(
            &parts,
            &mut messages,
            canonical_role,
            origin.as_ref(),
            &mut skipped_ui,
            &mut skipped_unknown,
        );
        extend_new_block_origins(
            &mut block_origins,
            &messages,
            messages_before,
            origin_messages.last().unwrap(),
        );
    }

    if skipped_ui > 0 {
        warnings.push(format!(
            "Skipped {skipped_ui} UI-only OpenCode parts ({})",
            UI_ONLY_PART_KINDS.join(", ")
        ));
    }
    if !skipped_unknown.is_empty() {
        warnings.push(format!(
            "Skipped unsupported OpenCode part kinds: {}",
            skipped_unknown.join(", ")
        ));
    }

    let mut imported = finalize_import(
        messages,
        format!("opencode:{session_id}"),
        warnings,
        "opencode",
    )?;
    imported.coordinate_map = Some(build_coordinate_map(
        &imported.messages,
        &block_origins,
        &origin_messages,
    )?);
    Ok(imported)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolTurnBatching {
    PerPart,
    PerStep,
}

fn convert_message_parts(
    parts: &[Value],
    messages: &mut Vec<Message>,
    role: Role,
    origin: Option<&ModelHandle>,
    skipped_ui: &mut usize,
    skipped_unknown: &mut Vec<String>,
) {
    let batching = if parts.iter().any(|part| {
        matches!(
            part.get("type").and_then(Value::as_str),
            Some("step-start" | "step-finish")
        )
    }) {
        ToolTurnBatching::PerStep
    } else {
        // Older OpenCode rows have no step markers. Keep their historical per-part conversion
        // because there is no reliable boundary for deciding which calls were produced together.
        ToolTurnBatching::PerPart
    };
    convert_message_parts_with_batching(
        parts,
        messages,
        role,
        origin,
        skipped_ui,
        skipped_unknown,
        batching,
    );
}

fn convert_message_parts_with_batching(
    parts: &[Value],
    messages: &mut Vec<Message>,
    role: Role,
    origin: Option<&ModelHandle>,
    skipped_ui: &mut usize,
    skipped_unknown: &mut Vec<String>,
    batching: ToolTurnBatching,
) {
    let mut pending = Vec::new();
    let mut results = Vec::new();
    for part in parts {
        let kind = part.get("type").and_then(Value::as_str).unwrap_or("");
        match kind {
            "step-start" | "step-finish" => {
                *skipped_ui += 1;
                if batching == ToolTurnBatching::PerStep {
                    flush_step(messages, role, origin, &mut pending, &mut results);
                }
            }
            "text" => push_text(&mut pending, part, "text"),
            "reasoning" => {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    pending.push(ContentBlock::Reasoning {
                        text: text.to_string(),
                        signature: None,
                    });
                }
            }
            "tool" => {
                let (call, result) = convert_tool(part);
                pending.push(ContentBlock::ToolCall(call));
                if let Some(result) = result {
                    if batching == ToolTurnBatching::PerStep {
                        results.push(ContentBlock::ToolResult(result));
                    } else {
                        flush_message(messages, role, origin.cloned(), &mut pending);
                        messages.push(Message::new(
                            Role::Tool,
                            vec![ContentBlock::ToolResult(result)],
                        ));
                    }
                }
            }
            kind if UI_ONLY_PART_KINDS.contains(&kind) => *skipped_ui += 1,
            other => {
                if !skipped_unknown.iter().any(|known| known == other) {
                    skipped_unknown.push(other.to_string());
                }
            }
        }
    }
    flush_step(messages, role, origin, &mut pending, &mut results);
}

fn flush_step(
    messages: &mut Vec<Message>,
    role: Role,
    origin: Option<&ModelHandle>,
    pending: &mut Vec<ContentBlock>,
    results: &mut Vec<ContentBlock>,
) {
    flush_message(messages, role, origin.cloned(), pending);
    if !results.is_empty() {
        messages.push(Message::new(Role::Tool, std::mem::take(results)));
    }
}

fn extend_new_block_origins(
    block_origins: &mut Vec<OriginMessageCoordinate>,
    messages: &[Message],
    messages_before: usize,
    origin: &OriginMessageCoordinate,
) {
    let blocks_added = messages[messages_before..]
        .iter()
        .map(|message| message.content.len())
        .sum();
    block_origins.extend(std::iter::repeat_n(origin.clone(), blocks_added));
}

fn load_parts(
    connection: &Connection,
    session_id: &str,
) -> ReaderResult<HashMap<String, Vec<String>>> {
    load_parts_counted(connection, session_id, || {})
}

fn load_parts_counted(
    connection: &Connection,
    session_id: &str,
    mut record_query: impl FnMut(),
) -> ReaderResult<HashMap<String, Vec<String>>> {
    record_query();
    let mut statement = connection
        .prepare(
            "SELECT message_id, data FROM part WHERE session_id = ?1 \
             ORDER BY message_id, time_created, id",
        )
        .map_err(|error| sqlite_snapshot_error("prepare OpenCode parts", error))?;
    let rows = statement
        .query_map([session_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| sqlite_snapshot_error("read OpenCode parts", error))?;
    let mut parts_by_message: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        let (message_id, data) =
            row.map_err(|error| sqlite_snapshot_error("read OpenCode parts", error))?;
        parts_by_message.entry(message_id).or_default().push(data);
    }
    Ok(parts_by_message)
}

fn unix_millis_date(timestamp: i64) -> Option<String> {
    let days = timestamp.checked_div(86_400_000)?;
    // Civil-date conversion for Unix days, adapted from Howard Hinnant's public-domain
    // algorithm: https://howardhinnant.github.io/date_algorithms.html#civil_from_days
    // OpenCode stores UTC epoch milliseconds, so no local timezone is involved.
    let shifted = days.checked_add(719_468)?;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    Some(format!("{year:04}-{month:02}-{day:02}"))
}

fn sqlite_snapshot_error(
    action: &'static str,
    error: rusqlite::Error,
) -> Box<dyn Error + Send + Sync> {
    let retryable = matches!(
        &error,
        rusqlite::Error::SqliteFailure(details, _)
            if matches!(
                details.code,
                ErrorCode::DatabaseBusy
                    | ErrorCode::DatabaseLocked
                    | ErrorCode::DatabaseCorrupt
                    | ErrorCode::NotADatabase
            )
    );
    if retryable {
        Box::new(RetryableOpenCodeSnapshotError {
            action,
            source: error,
        })
    } else {
        Box::new(error)
    }
}

fn nonempty(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn model_origin(metadata: &Value) -> Option<ModelHandle> {
    let provider = metadata
        .get("providerID")
        .or_else(|| metadata.get("providerId"))
        .and_then(Value::as_str)?;
    let model = metadata
        .get("modelID")
        .or_else(|| metadata.get("modelId"))
        .and_then(Value::as_str)?;
    Some(ModelHandle::new(provider, model))
}

fn push_text(blocks: &mut Vec<ContentBlock>, part: &Value, key: &str) {
    if let Some(text) = part.get(key).and_then(Value::as_str) {
        blocks.push(ContentBlock::Text {
            text: text.to_string(),
        });
    }
}

fn convert_tool(part: &Value) -> (ToolCall, Option<ToolResult>) {
    let call_id = part
        .get("callID")
        .or_else(|| part.get("callId"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let tool_name = part
        .get("tool")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let state = part.get("state").unwrap_or(&Value::Null);
    let input = state.get("input").cloned().unwrap_or(Value::Null);
    let call = ToolCall {
        tool_call_id: ToolCallId(call_id.to_string()),
        tool_name: tool_name.to_string(),
        input,
        thought_signature: None,
    };
    let status = state.get("status").and_then(Value::as_str).unwrap_or("");
    let result = match status {
        "completed" => {
            let output = state.get("output").cloned().unwrap_or(Value::Null);
            let output = match output {
                Value::String(text) => ToolOutput::Text { text },
                value => ToolOutput::Json { value },
            };
            Some(ToolResult {
                tool_call_id: ToolCallId(call_id.to_string()),
                output,
                is_error: false,
            })
        }
        "error" | "failed" => Some(ToolResult {
            tool_call_id: ToolCallId(call_id.to_string()),
            output: ToolOutput::Error {
                message: state
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("OpenCode tool execution failed")
                    .to_string(),
            },
            is_error: true,
        }),
        _ => None,
    };
    (call, result)
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
            None => Message::new(Role::Assistant, content),
        }
    } else {
        Message::new(role, content)
    };
    messages.push(message);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use rusqlite::{Connection, OpenFlags};

    use broca_module_serve::wire::{ImportOriginKind, ImportOriginParams, ImportParams};
    use broca_protocol::{ContentBlock, Message, Role, ToolOutput};

    use super::{
        convert_message_parts, convert_message_parts_with_batching, convert_rows,
        extend_new_block_origins, load_messages, load_parts_counted, sqlite_snapshot_error,
        HarnessReader, OpenCodeReader, ReaderResult, RetryableOpenCodeSnapshotError,
        ToolTurnBatching,
    };
    use crate::{
        sqlite_read::{immutable_uri, read_only_uri},
        OriginMessageCoordinate,
    };

    static TEST_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let sequence = TEST_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = broca_test_support::scratch_root().join(format!(
                "ck-opencode-{label}-{}-{sequence}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn database_path(&self) -> PathBuf {
            self.path.join("opencode.db")
        }

        #[cfg(unix)]
        fn make_read_only(&self) {
            fs::set_permissions(self.database_path(), fs::Permissions::from_mode(0o444)).unwrap();
            fs::set_permissions(&self.path, fs::Permissions::from_mode(0o555)).unwrap();
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            #[cfg(unix)]
            {
                let _ = fs::set_permissions(&self.path, fs::Permissions::from_mode(0o755));
                let _ =
                    fs::set_permissions(self.database_path(), fs::Permissions::from_mode(0o644));
            }
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn create_wal_database(path: &Path) -> Connection {
        let connection = Connection::open(path).unwrap();
        let mode: String = connection
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        connection
            .execute_batch(
                "CREATE TABLE session (id TEXT, title TEXT, directory TEXT, time_updated INTEGER);\n\
                 CREATE TABLE message (id TEXT, session_id TEXT, data TEXT, time_created INTEGER);\n\
                 CREATE TABLE part (id TEXT, session_id TEXT, message_id TEXT, data TEXT, time_created INTEGER);\n\
                 PRAGMA wal_checkpoint(TRUNCATE);",
            )
            .unwrap();
        connection
    }

    fn insert_message(
        connection: &Connection,
        session_id: &str,
        message_id: &str,
        role: &str,
        time_created: i64,
        parts: &[serde_json::Value],
    ) {
        let message = serde_json::json!({"role": role}).to_string();
        connection
            .execute(
                "INSERT INTO message VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![message_id, session_id, message, time_created],
            )
            .unwrap();
        for (index, part) in parts.iter().enumerate() {
            let part_id = format!("{message_id}-p{index}");
            connection
                .execute(
                    "INSERT INTO part VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![
                        part_id,
                        session_id,
                        message_id,
                        part.to_string(),
                        time_created
                    ],
                )
                .unwrap();
        }
    }

    fn insert_session(connection: &Connection, session_id: &str) {
        let message = serde_json::json!({"role": "user"}).to_string();
        let part = serde_json::json!({"type": "text", "text": "committed text"}).to_string();
        connection
            .execute(
                "INSERT INTO session VALUES (?1, 'Committed', '/tmp', 1)",
                [session_id],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO message VALUES ('m1', ?1, ?2, 1)",
                rusqlite::params![session_id, message],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO part VALUES ('p1', ?1, 'm1', ?2, 1)",
                rusqlite::params![session_id, part],
            )
            .unwrap();
    }

    fn fixture() -> OpenCodeReader {
        OpenCodeReader::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/opencode.db"),
        )
    }

    fn text_message(texts: &[&str], role: Role) -> Message {
        Message::new(
            role,
            texts
                .iter()
                .map(|text| ContentBlock::Text {
                    text: (*text).to_string(),
                })
                .collect(),
        )
    }

    fn legacy_load_parts_counted(
        connection: &Connection,
        session_id: &str,
        message_id: &str,
        mut record_query: impl FnMut(),
    ) -> ReaderResult<Vec<String>> {
        record_query();
        let mut statement = connection
            .prepare(
                "SELECT data FROM part WHERE session_id = ?1 AND message_id = ?2 \
                 ORDER BY time_created, id",
            )
            .map_err(|error| sqlite_snapshot_error("prepare OpenCode parts", error))?;
        let rows = statement
            .query_map([session_id, message_id], |row| row.get(0))
            .map_err(|error| sqlite_snapshot_error("read OpenCode parts", error))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| sqlite_snapshot_error("read OpenCode parts", error))
    }

    fn completed_tool_part(id: &str, name: &str, output: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "tool",
            "callID": id,
            "tool": name,
            "state": {
                "status": "completed",
                "input": {"id": id},
                "output": output,
            }
        })
    }

    fn convert_parts_for_test(
        parts: &[serde_json::Value],
        batching: Option<ToolTurnBatching>,
    ) -> (Vec<Message>, usize, Vec<String>) {
        let mut messages = Vec::new();
        let mut skipped_ui = 0;
        let mut skipped_unknown = Vec::new();
        let origin = broca_protocol::ModelHandle::new("openai", "gpt-test");
        match batching {
            Some(batching) => convert_message_parts_with_batching(
                parts,
                &mut messages,
                Role::Assistant,
                Some(&origin),
                &mut skipped_ui,
                &mut skipped_unknown,
                batching,
            ),
            None => convert_message_parts(
                parts,
                &mut messages,
                Role::Assistant,
                Some(&origin),
                &mut skipped_ui,
                &mut skipped_unknown,
            ),
        }
        (messages, skipped_ui, skipped_unknown)
    }

    fn convert_assistant_row(parts: &[serde_json::Value]) -> crate::ImportedSession {
        let source_rows = vec![(
            "m-assistant".to_string(),
            serde_json::json!({
                "role": "assistant",
                "providerID": "openai",
                "modelID": "gpt-parallel",
            })
            .to_string(),
            1,
        )];
        let parts_by_message = HashMap::from([(
            "m-assistant".to_string(),
            parts.iter().map(serde_json::Value::to_string).collect(),
        )]);
        convert_rows(source_rows, parts_by_message, None, "parallel").unwrap()
    }

    fn call_ids(message: &Message) -> Vec<&str> {
        message
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::ToolCall(call) => Some(call.tool_call_id.0.as_str()),
                _ => None,
            })
            .collect()
    }

    fn result_ids(message: &Message) -> Vec<&str> {
        message
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::ToolResult(result) => Some(result.tool_call_id.0.as_str()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn same_step_parallel_tools_share_one_assistant_and_one_tool_message() {
        let parts = vec![
            serde_json::json!({"type": "step-start"}),
            completed_tool_part("c1", "search", "one"),
            completed_tool_part("c2", "read", "two"),
            serde_json::json!({"type": "step-finish"}),
        ];
        let imported = convert_assistant_row(&parts);
        assert_eq!(
            imported
                .messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            [Role::Assistant, Role::Tool]
        );
        assert_eq!(call_ids(&imported.messages[0]), ["c1", "c2"]);
        assert_eq!(result_ids(&imported.messages[1]), ["c1", "c2"]);

        let (legacy, _, _) = convert_parts_for_test(&parts, Some(ToolTurnBatching::PerPart));
        assert_eq!(
            legacy
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            [Role::Assistant, Role::Tool, Role::Assistant, Role::Tool],
            "the former per-part path fabricated a dependent turn for the second call"
        );
    }

    #[test]
    fn markerless_tool_parts_keep_the_legacy_serialized_shape() {
        let parts = vec![
            serde_json::json!({"type": "text", "text": "before"}),
            completed_tool_part("c1", "search", "one"),
            completed_tool_part("c2", "read", "two"),
        ];
        let (actual, actual_skipped, actual_unknown) = convert_parts_for_test(&parts, None);
        let (legacy, legacy_skipped, legacy_unknown) =
            convert_parts_for_test(&parts, Some(ToolTurnBatching::PerPart));
        assert_eq!(
            serde_json::to_vec(&actual).unwrap(),
            serde_json::to_vec(&legacy).unwrap()
        );
        assert_eq!(actual_skipped, legacy_skipped);
        assert_eq!(actual_unknown, legacy_unknown);
    }

    #[test]
    fn text_and_parallel_tools_remain_in_the_same_assistant_step() {
        let parts = vec![
            serde_json::json!({"type": "step-start"}),
            serde_json::json!({"type": "text", "text": "working"}),
            completed_tool_part("c1", "search", "one"),
            completed_tool_part("c2", "read", "two"),
            serde_json::json!({"type": "step-finish"}),
        ];
        let (messages, _, _) = convert_parts_for_test(&parts, None);
        assert_eq!(
            messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            [Role::Assistant, Role::Tool]
        );
        assert!(matches!(
            messages[0].content.first(),
            Some(ContentBlock::Text { text }) if text == "working"
        ));
        assert_eq!(call_ids(&messages[0]), ["c1", "c2"]);
        assert_eq!(result_ids(&messages[1]), ["c1", "c2"]);
    }

    #[test]
    fn completed_single_tool_steps_match_the_legacy_bytes() {
        let parts = vec![
            serde_json::json!({"type": "step-start"}),
            completed_tool_part("c1", "search", "one"),
            serde_json::json!({"type": "step-finish"}),
            serde_json::json!({"type": "step-start"}),
            completed_tool_part("c2", "read", "two"),
            serde_json::json!({"type": "step-finish"}),
        ];
        let (actual, actual_skipped, actual_unknown) = convert_parts_for_test(&parts, None);
        let (legacy, legacy_skipped, legacy_unknown) =
            convert_parts_for_test(&parts, Some(ToolTurnBatching::PerPart));
        assert_eq!(
            serde_json::to_vec(&actual).unwrap(),
            serde_json::to_vec(&legacy).unwrap()
        );
        assert_eq!(actual_skipped, legacy_skipped);
        assert_eq!(actual_unknown, legacy_unknown);
    }

    #[test]
    fn incomplete_tool_step_keeps_a_dangling_call_without_inventing_a_result() {
        let parts = vec![
            serde_json::json!({"type": "step-start"}),
            serde_json::json!({
                "type": "tool",
                "callID": "pending",
                "tool": "search",
                "state": {"status": "running", "input": {"q": "x"}},
            }),
            serde_json::json!({"type": "step-finish"}),
        ];
        let (actual, _, _) = convert_parts_for_test(&parts, None);
        let (legacy, _, _) = convert_parts_for_test(&parts, Some(ToolTurnBatching::PerPart));
        assert_eq!(
            serde_json::to_vec(&actual).unwrap(),
            serde_json::to_vec(&legacy).unwrap()
        );
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].role, Role::Assistant);
        assert_eq!(call_ids(&actual[0]), ["pending"]);
        assert!(actual[0]
            .content
            .iter()
            .all(|block| !matches!(block, ContentBlock::ToolResult(_))));
    }

    #[test]
    fn regrouped_parallel_step_keeps_coordinate_provenance_aligned() {
        let parts = vec![
            serde_json::json!({"type": "step-start"}),
            completed_tool_part("c1", "search", "one"),
            completed_tool_part("c2", "read", "two"),
            serde_json::json!({"type": "step-finish"}),
        ];
        let imported = convert_assistant_row(&parts);
        let coordinates = imported.coordinate_map.as_ref().unwrap();
        let emitted_blocks = imported
            .messages
            .iter()
            .map(|message| message.content.len())
            .sum::<usize>();
        let mapped_blocks = imported
            .messages
            .iter()
            .enumerate()
            .flat_map(|(message_index, message)| {
                (0..message.content.len())
                    .map(move |block_index| format!("m{message_index}#{block_index}"))
            })
            .filter(|flat_id| coordinates.contains_flat_id(flat_id))
            .count();
        assert_eq!(mapped_blocks, emitted_blocks);
        assert_eq!(
            coordinates
                .coordinate_for_message_id("m-assistant")
                .map(|coordinate| coordinate.flat_id()),
            Some("m1#1".to_string())
        );
    }

    #[test]
    fn bulk_parts_query_matches_legacy_order_and_runs_once() {
        let directory = TestDirectory::new("bulk-parts");
        let connection = create_wal_database(&directory.database_path());
        connection
            .execute("INSERT INTO session VALUES ('ordered', 'T', '/tmp', 1)", [])
            .unwrap();
        for (message_id, role, time_created) in [
            ("m-a", "user", 1),
            ("m-b", "assistant", 2),
            ("m-empty", "user", 3),
        ] {
            connection
                .execute(
                    "INSERT INTO message VALUES (?1, 'ordered', ?2, ?3)",
                    rusqlite::params![
                        message_id,
                        serde_json::json!({"role": role}).to_string(),
                        time_created
                    ],
                )
                .unwrap();
        }
        for (part_id, message_id, text, time_created) in [
            ("b-z", "m-b", "b-late", 30),
            ("a-z", "m-a", "a-late", 20),
            ("b-a", "m-b", "b-early", 10),
            ("a-b", "m-a", "a-early-b", 5),
            ("a-a", "m-a", "a-early-a", 5),
        ] {
            connection
                .execute(
                    "INSERT INTO part VALUES (?1, 'ordered', ?2, ?3, ?4)",
                    rusqlite::params![
                        part_id,
                        message_id,
                        serde_json::json!({"type": "text", "text": text}).to_string(),
                        time_created
                    ],
                )
                .unwrap();
        }

        let source_rows = load_messages(&connection, "ordered").unwrap();
        let mut legacy_query_count = 0usize;
        let mut legacy_parts = HashMap::new();
        for (message_id, _, _) in &source_rows {
            let parts = legacy_load_parts_counted(&connection, "ordered", message_id, || {
                legacy_query_count += 1
            })
            .unwrap();
            legacy_parts.insert(message_id.clone(), parts);
        }
        let mut bulk_query_count = 0usize;
        let bulk_parts =
            load_parts_counted(&connection, "ordered", || bulk_query_count += 1).unwrap();

        let legacy = convert_rows(source_rows.clone(), legacy_parts, None, "ordered").unwrap();
        let bulk = convert_rows(source_rows, bulk_parts, None, "ordered").unwrap();
        assert_eq!(
            serde_json::to_vec(&bulk.messages).unwrap(),
            serde_json::to_vec(&legacy.messages).unwrap(),
            "bulk grouping must preserve the legacy per-message conversion bytes"
        );
        assert_eq!(bulk, legacy);
        assert_eq!(legacy_query_count, 3);
        assert_eq!(bulk_query_count, 1);
        assert_eq!(
            bulk.messages,
            vec![
                text_message(&["a-early-a", "a-early-b", "a-late"], Role::User),
                text_message(&["b-early", "b-late"], Role::Assistant),
            ]
        );
    }

    #[test]
    fn incremental_block_provenance_preserves_split_row_order_and_empty_rows() {
        let prior = OriginMessageCoordinate {
            ordinal: 0,
            message_id: "prior".into(),
            date: None,
        };
        let split = OriginMessageCoordinate {
            ordinal: 1,
            message_id: "split".into(),
            date: None,
        };
        let empty = OriginMessageCoordinate {
            ordinal: 2,
            message_id: "empty".into(),
            date: None,
        };
        let mut messages = vec![text_message(&["prior"], Role::User)];
        let mut block_origins = vec![prior.clone()];

        let messages_before = messages.len();
        messages.push(text_message(&["answer", "call"], Role::Assistant));
        messages.push(text_message(&["result"], Role::Tool));
        extend_new_block_origins(&mut block_origins, &messages, messages_before, &split);
        assert_eq!(
            block_origins,
            vec![prior, split.clone(), split.clone(), split]
        );

        let messages_before = messages.len();
        extend_new_block_origins(&mut block_origins, &messages, messages_before, &empty);
        assert_eq!(block_origins.len(), 4);
    }

    #[test]
    fn enumerates_scrubbed_fixture_sessions() {
        let sessions = fixture().enumerate().unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].id, "ses-tools");
        assert_eq!(sessions[0].cwd.as_deref(), Some("/tmp/scrubbed-project"));
        assert_eq!(
            fixture().session_cwd("ses-tools").unwrap().as_deref(),
            Some("/tmp/scrubbed-project")
        );
        assert_eq!(sessions[0].message_count, 3);
        assert!(sessions[0].approximate_size_bytes.is_none());
    }

    #[test]
    fn live_read_only_uri_preserves_immutable_fallback() {
        let path = std::path::Path::new("/tmp/a b?#%.db");
        assert_eq!(read_only_uri(path), "file:/tmp/a%20b%3F%23%25.db?mode=ro");
        assert_eq!(
            immutable_uri(path),
            "file:/tmp/a%20b%3F%23%25.db?immutable=1"
        );
    }

    #[test]
    fn busy_locked_or_torn_read_snapshots_return_a_typed_retryable_error() {
        for code in [
            rusqlite::ffi::SQLITE_BUSY,
            rusqlite::ffi::SQLITE_LOCKED,
            rusqlite::ffi::SQLITE_CORRUPT,
        ] {
            let error = rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None);
            let error = sqlite_snapshot_error("read a test snapshot", error);
            assert!(error
                .downcast_ref::<RetryableOpenCodeSnapshotError>()
                .is_some());
            assert!(error.to_string().contains("retry after"));
        }
    }

    #[test]
    fn live_wal_rows_are_visible_but_immutable_revert_check_hides_them() {
        let directory = TestDirectory::new("live-wal");
        let path = directory.database_path();
        let writer = create_wal_database(&path);
        insert_session(&writer, "live-session");
        assert!(
            fs::metadata(format!("{}-wal", path.display()))
                .unwrap()
                .len()
                > 0
        );

        let sessions = OpenCodeReader::new(&path).enumerate().unwrap();
        assert!(sessions.iter().any(|session| session.id == "live-session"));

        let immutable = Connection::open_with_flags(
            immutable_uri(&path),
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        let immutable_count: i64 = immutable
            .query_row("SELECT COUNT(*) FROM session", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            immutable_count, 0,
            "revert-check: immutable=1 must demonstrate the committed-WAL visibility bug"
        );
    }

    #[cfg(unix)]
    #[test]
    fn dormant_wal_database_without_shm_uses_immutable_fallback() {
        let directory = TestDirectory::new("dormant-fallback");
        let path = directory.database_path();
        let writer = create_wal_database(&path);
        insert_session(&writer, "dormant-session");
        drop(writer);
        let _ = fs::remove_file(format!("{}-shm", path.display()));
        let _ = fs::remove_file(format!("{}-wal", path.display()));
        directory.make_read_only();

        let reader = OpenCodeReader::new(&path);
        let opened = reader.open().unwrap();
        assert!(opened.used_immutable_fallback);
        assert!(opened.fallback_warning.is_none());
        let count: i64 = opened
            .connection
            .query_row("SELECT COUNT(*) FROM session", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[cfg(unix)]
    #[test]
    fn immutable_fallback_warns_when_leftover_wal_may_be_invisible() {
        let directory = TestDirectory::new("leftover-wal-warning");
        let path = directory.database_path();
        let writer = create_wal_database(&path);
        insert_session(&writer, "warning-session");
        drop(writer);
        let _ = fs::remove_file(format!("{}-shm", path.display()));
        fs::write(format!("{}-wal", path.display()), b"leftover wal bytes").unwrap();
        directory.make_read_only();

        let imported = OpenCodeReader::new(&path)
            .convert("warning-session")
            .unwrap();
        assert!(imported.warnings.iter().any(|warning| {
            warning.contains("immutable fallback")
                && warning.contains("18 bytes")
                && warning.contains("may be invisible")
        }));
        assert_eq!(imported.messages.len(), 1);
    }

    #[test]
    fn parts_are_scoped_by_session_and_non_assistant_calls_are_rehomed() {
        let path = broca_test_support::scratch_root()
            .join(format!("ck-opencode-parts-{}.db", std::process::id()));
        let _ = fs::remove_file(&path);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE session (id TEXT, title TEXT, directory TEXT, time_updated INTEGER);\n\
                 CREATE TABLE message (id TEXT, session_id TEXT, data TEXT, time_created INTEGER);\n\
                 CREATE TABLE part (id TEXT, session_id TEXT, message_id TEXT, data TEXT, time_created INTEGER);",
            )
            .unwrap();
        connection
            .execute("INSERT INTO session VALUES ('target', 'T', '/tmp', 2)", [])
            .unwrap();
        connection
            .execute("INSERT INTO session VALUES ('other', 'O', '/tmp', 1)", [])
            .unwrap();
        let target_message = serde_json::json!({"role": "user"}).to_string();
        connection
            .execute(
                "INSERT INTO message VALUES ('same', 'target', ?1, 1)",
                [&target_message],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO message VALUES ('same', 'other', ?1, 1)",
                [&target_message],
            )
            .unwrap();
        let tool = serde_json::json!({
            "type": "tool",
            "tool": "lookup",
            "state": {"status": "completed", "input": {}, "output": "ok"}
        })
        .to_string();
        let foreign = serde_json::json!({"type": "text", "text": "FOREIGN"}).to_string();
        connection
            .execute(
                "INSERT INTO part VALUES ('p1', 'target', 'same', ?1, 1)",
                [&tool],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO part VALUES ('p2', 'other', 'same', ?1, 1)",
                [&foreign],
            )
            .unwrap();
        drop(connection);

        let reader = OpenCodeReader::new(&path);
        let imported = reader.convert("target").unwrap();
        assert_eq!(
            imported
                .messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            vec![Role::Assistant, Role::Tool]
        );
        assert!(!serde_json::to_string(&imported.messages)
            .unwrap()
            .contains("FOREIGN"));
        assert!(imported
            .warnings
            .iter()
            .any(|warning| warning.contains("Re-homed")));
        assert!(imported
            .warnings
            .iter()
            .any(|warning| warning.contains("missing")));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn sqlite_size_estimate_is_deferred_until_import() {
        let path = broca_test_support::scratch_root()
            .join(format!("ck-opencode-bytes-{}.db", std::process::id()));
        let _ = fs::remove_file(&path);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE session (id TEXT, title TEXT, directory TEXT, time_updated INTEGER);\n\
                 CREATE TABLE message (id TEXT, session_id TEXT, data TEXT, time_created INTEGER);\n\
                 CREATE TABLE part (id TEXT, session_id TEXT, message_id TEXT, data TEXT, time_created INTEGER);",
            )
            .unwrap();
        connection
            .execute("INSERT INTO session VALUES ('bytes', 'T', '/tmp', 1)", [])
            .unwrap();
        connection
            .execute("INSERT INTO message VALUES ('m', 'bytes', 'é', 1)", [])
            .unwrap();
        connection
            .execute("INSERT INTO part VALUES ('p', 'bytes', 'm', '🙂', 1)", [])
            .unwrap();
        drop(connection);

        let summary = OpenCodeReader::new(&path).enumerate().unwrap().remove(0);
        assert_eq!(summary.approximate_size_bytes, None);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn converts_tool_reasoning_origin_and_skips_non_history_parts() {
        let imported = fixture().convert("ses-tools").unwrap();
        assert_eq!(imported.source_ref, "opencode:ses-tools");
        assert!(imported
            .warnings
            .iter()
            .any(|warning| warning.contains("UI-only")));
        assert!(!imported
            .messages
            .iter()
            .any(|message| message.role == Role::System));

        let assistant = imported
            .messages
            .iter()
            .find(|message| message.role == Role::Assistant)
            .unwrap();
        assert_eq!(
            assistant.origin.as_ref().unwrap().provider_module_id,
            "anthropic"
        );
        assert_eq!(
            assistant.origin.as_ref().unwrap().model_id,
            "claude-scrubbed"
        );
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
            import_id: "tool-fixture-round-trip".into(),
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
    fn mixed_rows_and_tool_split_preserve_exact_coordinate_provenance() {
        let directory = TestDirectory::new("mixed-provenance");
        let path = directory.database_path();
        let connection = create_wal_database(&path);
        connection
            .execute(
                "INSERT INTO session VALUES ('mixed', 'Mixed', '/tmp', 5)",
                [],
            )
            .unwrap();
        insert_message(
            &connection,
            "mixed",
            "m-user",
            "user",
            1,
            &[
                serde_json::json!({"type": "text", "text": "one"}),
                serde_json::json!({"type": "text", "text": "two"}),
            ],
        );
        insert_message(
            &connection,
            "mixed",
            "m-system",
            "system",
            2,
            &[serde_json::json!({"type": "text", "text": "ignored"})],
        );
        insert_message(
            &connection,
            "mixed",
            "m-assistant",
            "assistant",
            3,
            &[
                serde_json::json!({"type": "text", "text": "answer"}),
                serde_json::json!({
                    "type": "tool",
                    "callID": "call-1",
                    "tool": "lookup",
                    "state": {
                        "status": "completed",
                        "input": {"query": "value"},
                        "output": "found"
                    }
                }),
            ],
        );
        insert_message(
            &connection,
            "mixed",
            "m-ui-only",
            "user",
            4,
            &[serde_json::json!({"type": "step-start"})],
        );
        insert_message(
            &connection,
            "mixed",
            "m-final",
            "user",
            5,
            &[serde_json::json!({"type": "text", "text": "done"})],
        );
        drop(connection);

        let imported = OpenCodeReader::new(&path).convert("mixed").unwrap();
        assert_eq!(
            imported
                .messages
                .iter()
                .map(|message| (message.role, message.content.len()))
                .collect::<Vec<_>>(),
            vec![
                (Role::User, 2),
                (Role::Assistant, 2),
                (Role::Tool, 1),
                (Role::User, 1),
            ]
        );
        let coordinates = imported.coordinate_map.as_ref().unwrap();
        let expected = [
            (0, "m-user", "m0#1"),
            (1, "m-system", "m0#1"),
            (2, "m-assistant", "m2#0"),
            (3, "m-ui-only", "m2#0"),
            (4, "m-final", "m3#0"),
        ];
        for (ordinal, message_id, flat_id) in expected {
            assert_eq!(
                coordinates
                    .coordinate_for_ordinal(ordinal)
                    .map(|coordinate| coordinate.flat_id()),
                Some(flat_id.to_string())
            );
            assert_eq!(
                coordinates
                    .coordinate_for_message_id(message_id)
                    .map(|coordinate| coordinate.flat_id()),
                Some(flat_id.to_string())
            );
        }
        for flat_id in ["m0#0", "m0#1", "m1#0", "m1#1", "m2#0", "m3#0"] {
            assert!(coordinates.contains_flat_id(flat_id));
        }
        for flat_id in ["m0#2", "m1#2", "m2#1", "m3#1"] {
            assert!(!coordinates.contains_flat_id(flat_id));
        }
    }

    #[test]
    fn large_session_keeps_every_incremental_provenance_coordinate_aligned() {
        const ROWS: usize = 205;
        let directory = TestDirectory::new("large-provenance");
        let path = directory.database_path();
        let mut connection = create_wal_database(&path);
        connection
            .execute(
                "INSERT INTO session VALUES ('large', 'Large', '/tmp', 205)",
                [],
            )
            .unwrap();
        let transaction = connection.transaction().unwrap();
        for index in 0..ROWS {
            let message_id = format!("m-{index:03}");
            insert_message(
                &transaction,
                "large",
                &message_id,
                "user",
                index as i64,
                &[serde_json::json!({"type": "text", "text": message_id})],
            );
        }
        transaction.commit().unwrap();
        drop(connection);

        let imported = OpenCodeReader::new(&path).convert("large").unwrap();
        assert_eq!(imported.messages.len(), ROWS);
        let coordinates = imported.coordinate_map.as_ref().unwrap();
        for index in 0..ROWS {
            let message_id = format!("m-{index:03}");
            let flat_id = format!("m{index}#0");
            assert_eq!(
                coordinates
                    .coordinate_for_ordinal(index as u64)
                    .map(|coordinate| coordinate.flat_id()),
                Some(flat_id.clone())
            );
            assert_eq!(
                coordinates
                    .coordinate_for_message_id(&message_id)
                    .map(|coordinate| coordinate.flat_id()),
                Some(flat_id)
            );
        }
    }

    #[test]
    fn coordinate_map_uses_source_ids_and_imported_flat_block_ids() {
        let imported = fixture().convert("ses-tools").unwrap();
        let coordinates = imported.coordinate_map.as_ref().unwrap();
        assert!(coordinates.coordinate_for_ordinal(0).is_none());
        assert_eq!(
            coordinates
                .start_coordinate_for_ordinal(0)
                .unwrap()
                .flat_id(),
            "m0#0"
        );
        assert_eq!(
            coordinates
                .coordinate_for_message_id("m-user")
                .unwrap()
                .flat_id(),
            "m0#0"
        );
        assert_eq!(
            coordinates
                .coordinate_for_message_id("m-assistant")
                .unwrap()
                .flat_id(),
            "m2#0"
        );
        assert_eq!(
            coordinates.date_for_message_id("m-assistant"),
            Some("1970-01-01")
        );
    }

    #[test]
    fn text_only_session_round_trips_through_import_wire_schema() {
        let imported = fixture().convert("ses-text").unwrap();
        let params = ImportParams {
            messages: imported.messages.clone(),
            import_id: "fixture-round-trip".into(),
            origin: ImportOriginParams {
                kind: ImportOriginKind::Import,
                source_ref: imported.source_ref,
            },
        };
        let encoded = serde_json::to_vec(&params).unwrap();
        let decoded: ImportParams = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.messages, imported.messages);
        assert_eq!(decoded.messages.len(), 2);
    }
}
