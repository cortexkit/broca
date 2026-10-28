//! `ck-import` converts a foreign harness transcript and seeds an empty broca lineage.

#![forbid(unsafe_code)]

use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use broca_core::import::{IMPORT_MAX_MESSAGES, IMPORT_MAX_SERIALIZED_BYTES};
use broca_module_serve::SessionClient;
use broca_protocol::BindIdentity;
use broca_subc::SubcConnection;
use ck_session_import::claude_code::ClaudeCodeReader;
use ck_session_import::codex::CodexReader;
use ck_session_import::goose::GooseReader;
use ck_session_import::opencode::OpenCodeReader;
use ck_session_import::pi::PiReader;
use ck_session_import::{
    measure_serialized_messages, HarnessReader, ImportedSession, SerializedMessageMetrics,
    SessionSummary,
};
use clap::{error::ErrorKind as ClapErrorKind, Parser};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[derive(Parser, Debug)]
#[command(
    name = "ck-import",
    version,
    about = "Import a foreign harness transcript into broca"
)]
struct Args {
    /// Source harness (opencode, claude-code, codex, goose, or pi).
    harness_name: String,
    /// Source session id shown by --list.
    source: Option<String>,
    /// List source sessions instead of importing one.
    #[arg(long)]
    list: bool,
    /// Emit line-delimited JSON events on stdout.
    #[arg(long)]
    json: bool,
    /// Path to the subc connection file. Required for import mode.
    #[arg(long)]
    subc: Option<PathBuf>,
    /// Override the source container path (database file or sessions root, per harness).
    #[arg(long)]
    source_db: Option<PathBuf>,
    /// Target broca session id (defaults to a fresh UUID and is printed).
    #[arg(long)]
    session: Option<String>,
    /// Target BindIdentity project root (defaults to source cwd, then $PWD).
    #[arg(long)]
    project_root: Option<PathBuf>,
    /// Target BindIdentity harness.
    #[arg(long = "harness", default_value = "ck-app")]
    target_harness: String,
    /// Import idempotency key (defaults to a digest of source_ref).
    #[arg(long)]
    import_id: Option<String>,
    /// Context window used for the non-blocking 95%-full estimate warning.
    #[arg(long, default_value_t = 200_000)]
    context_limit: u64,
    /// Broca management-surface module id.
    #[arg(long, default_value = "broca")]
    module_id: String,
    /// Time to wait for a supervised broca module to register.
    #[arg(long, default_value_t = 30_000)]
    bind_timeout_ms: u64,
    /// Test-only effective message count used to exercise the cap without a huge fixture.
    #[arg(long, hide = true)]
    message_count_override: Option<usize>,
}

struct Output {
    json: bool,
}

impl Output {
    fn event(&self, value: Value, human: impl FnOnce() -> String) {
        if self.json {
            println!("{value}");
        } else {
            eprintln!("{}", human());
        }
    }

    fn done(&self, value: Value, human: impl FnOnce() -> String) {
        if self.json {
            println!("{value}");
        } else {
            eprintln!("{}", human());
        }
    }

    fn error(&self, message: &str) {
        if self.json {
            println!("{}", json!({"event": "error", "message": message}));
        } else {
            eprintln!("ERROR: {message}");
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let raw_args: Vec<_> = std::env::args_os().collect();
    let json_requested = raw_args.iter().any(|argument| argument == "--json");
    let args = match Args::try_parse_from(&raw_args) {
        Ok(args) => args,
        Err(error)
            if matches!(
                error.kind(),
                ClapErrorKind::DisplayHelp | ClapErrorKind::DisplayVersion
            ) =>
        {
            error.exit();
        }
        Err(error) => {
            if json_requested {
                println!(
                    "{}",
                    json!({"event": "error", "message": error.to_string()})
                );
            } else {
                let _ = error.print();
            }
            std::process::exit(error.exit_code());
        }
    };
    let output = Output { json: args.json };
    if let Err(error) = run(args, &output).await {
        output.error(&error);
        std::process::exit(1);
    }
}

async fn run(args: Args, output: &Output) -> Result<(), String> {
    let reader: Box<dyn HarnessReader> = match args.harness_name.as_str() {
        "codex" => Box::new(CodexReader::new(
            args.source_db
                .clone()
                .unwrap_or_else(CodexReader::default_path),
        )),
        "opencode" => Box::new(OpenCodeReader::new(
            args.source_db
                .clone()
                .unwrap_or_else(OpenCodeReader::default_path),
        )),
        "claude-code" => Box::new(ClaudeCodeReader::new(
            args.source_db
                .clone()
                .unwrap_or_else(ClaudeCodeReader::default_path),
        )),
        "goose" => Box::new(GooseReader::new(
            args.source_db
                .clone()
                .unwrap_or_else(GooseReader::default_path),
        )),
        "pi" => Box::new(PiReader::new(
            args.source_db
                .clone()
                .unwrap_or_else(PiReader::default_path),
        )),
        _ => {
            return Err(format!(
                "unsupported harness '{}'; this build supports: opencode, claude-code, codex, goose, pi",
                args.harness_name
            ));
        }
    };
    if args.context_limit == 0 {
        return Err("--context-limit must be greater than zero".into());
    }
    let source_harness = reader.harness();
    if args.list {
        if args.source.is_some() {
            return Err("a source session id cannot be combined with --list".into());
        }
        let sessions = reader
            .enumerate()
            .map_err(|error| format!("list {source_harness} sessions: {error}"))?;
        if output.json {
            for session in sessions {
                println!(
                    "{}",
                    json!({
                        "event": "progress",
                        "action": "session",
                        "id": session.id,
                        "title": session.title,
                        "cwd": session.cwd,
                        "message_count": session.message_count,
                        "approximate_size_bytes": session.approximate_size_bytes,
                    })
                );
            }
            println!("{}", json!({"event": "done", "action": "list"}));
        } else {
            eprint!("{}", format_session_table(&sessions));
        }
        return Ok(());
    }

    let source_id = args.source.as_deref().ok_or_else(|| {
        "missing <path-or-session-id>; use --list to choose a session".to_string()
    })?;
    output.event(
        json!({"event": "progress", "action": "converting", "source": source_id}),
        || format!("Converting {source_harness} session {source_id}..."),
    );
    let (imported, source_cwd) =
        convert_source(reader.as_ref(), source_id, args.project_root.as_ref())?;

    let session_id = args.session.unwrap_or_else(fresh_uuid);

    for warning in &imported.warnings {
        output.event(json!({"event": "warning", "message": warning}), || {
            format!("WARNING: {warning}")
        });
    }
    let dropped_api_error_messages = imported.dropped_api_error_messages;

    let effective_count =
        effective_message_count(imported.messages.len(), args.message_count_override);
    let serialized_metrics = check_caps(&imported.messages, effective_count)?;

    let estimated_tokens = serialized_metrics.estimated_tokens();
    if estimated_tokens.saturating_mul(100) >= args.context_limit.saturating_mul(95) {
        let warning = format!(
            "estimated imported history is {estimated_tokens} tokens, at least 95% of the \
             {}-token context limit; import will continue",
            args.context_limit
        );
        output.event(
            json!({
                "event": "warning",
                "kind": "context_fill",
                "estimated_tokens": estimated_tokens,
                "context_limit": args.context_limit,
                "message": warning,
            }),
            || format!("WARNING: {warning}"),
        );
    }

    let project_root = resolve_project_root(args.project_root, source_cwd.as_deref());
    let import_id = args
        .import_id
        .unwrap_or_else(|| digest_import_id(&imported.source_ref));
    let subc = args
        .subc
        .ok_or_else(|| "--subc <subc-connection.json> is required for import mode".to_string())?;
    let identity = BindIdentity::new(&project_root, &args.target_harness, &session_id);
    identity
        .validate_keyable()
        .map_err(|error| format!("invalid target identity: {error}"))?;

    output.event(
        json!({
            "event": "progress",
            "action": "importing",
            "session": session_id,
            "project_root": project_root,
            "harness": args.target_harness,
            "message_count": imported.messages.len(),
        }),
        || {
            format!(
                "Importing {} messages into session {} (project_root={}, harness={})...",
                imported.messages.len(),
                session_id,
                project_root.display(),
                args.target_harness
            )
        },
    );

    let connection = SubcConnection::connect(&subc)
        .await
        .map_err(|error| format!("connect subc ({}): {error}", subc.display()))?;
    let client = SessionClient::bind_with_timeout(
        connection,
        &args.module_id,
        identity,
        std::time::Duration::from_millis(args.bind_timeout_ms),
    )
    .await
    .map_err(|error| format!("bind broca management surface: {error}"))?;
    let result = client
        .import(imported.messages, &import_id, &imported.source_ref)
        .await
        .map_err(|error| format!("session.import: {error}"))?;

    output.done(
        json!({
            "event": "done",
            "session": session_id,
            "project_root": project_root,
            "harness": args.target_harness,
            "import_id": import_id,
            "source_ref": imported.source_ref,
            "run_id": result.run_id,
            "synthesized_tool_results": result.synthesized_tool_results,
            "dropped_orphan_tool_results": result.dropped_orphan_tool_results,
            "dropped_system_messages": result.dropped_system_messages,
            "dropped_api_error_messages": dropped_api_error_messages,
            // Absent rather than 0 against a server that predates the field: it dropped
            // them too and simply did not say so.
            "already_imported": result.already_imported,
        }),
        || {
            format!(
                "Imported session {session_id}: run_id={}, synthesized_tool_results={}, \
                 dropped_orphan_tool_results={}, dropped_system_messages={}, \
                 dropped_api_error_messages={}, already_imported={}",
                result.run_id,
                result.synthesized_tool_results,
                result.dropped_orphan_tool_results,
                result
                    .dropped_system_messages
                    .map_or_else(|| "unreported".to_string(), |n| n.to_string()),
                dropped_api_error_messages,
                result.already_imported
            )
        },
    );
    Ok(())
}

fn convert_source(
    reader: &dyn HarnessReader,
    source_id: &str,
    project_root: Option<&PathBuf>,
) -> Result<(ImportedSession, Option<String>), String> {
    let source_harness = reader.harness();
    let imported = reader
        .convert(source_id)
        .map_err(|error| format!("convert {source_harness} session: {error}"))?;
    let source_cwd = if project_root.is_none() {
        reader
            .session_cwd(source_id)
            .map_err(|error| format!("inspect selected {source_harness} session: {error}"))?
    } else {
        None
    };
    Ok((imported, source_cwd))
}

fn resolve_project_root(project_root: Option<PathBuf>, source_cwd: Option<&str>) -> PathBuf {
    project_root
        .or_else(|| source_cwd.map(PathBuf::from))
        .unwrap_or_else(current_dir_fallback)
}

fn effective_message_count(real_count: usize, override_count: Option<usize>) -> usize {
    override_count.map_or(real_count, |override_count| real_count.max(override_count))
}

fn check_caps(
    messages: &[broca_protocol::Message],
    effective_count: usize,
) -> Result<SerializedMessageMetrics, String> {
    check_caps_with_measure(effective_count, || measure_serialized_messages(messages))
}

fn check_caps_with_measure(
    effective_count: usize,
    measure: impl FnOnce() -> Result<SerializedMessageMetrics, serde_json::Error>,
) -> Result<SerializedMessageMetrics, String> {
    if effective_count > IMPORT_MAX_MESSAGES {
        return Err(format!(
            "session too large to import in one op: {effective_count} messages exceeds the \
             {IMPORT_MAX_MESSAGES}-message limit. Choose a shorter source session or remove old \
             history before retrying"
        ));
    }
    let metrics = measure()
        .map_err(|error| format!("serialize converted messages for size check: {error}"))?;
    if metrics.byte_count > IMPORT_MAX_SERIALIZED_BYTES {
        return Err(format!(
            "session too large to import in one op: converted history is {} bytes, exceeding \
             the {IMPORT_MAX_SERIALIZED_BYTES}-byte (48 MiB) client limit. Remove large tool \
             outputs or old history before retrying",
            metrics.byte_count
        ));
    }
    Ok(metrics)
}

fn format_session_table(sessions: &[SessionSummary]) -> String {
    let mut table = String::from("ID\tMESSAGES\tSIZE_BYTES\tTITLE\tCWD\n");
    for session in sessions {
        let _ = writeln!(
            table,
            "{}\t{}\t{}\t{}\t{}",
            session.id,
            session.message_count,
            session
                .approximate_size_bytes
                .map_or_else(|| "-".to_string(), |size| size.to_string()),
            session.title.as_deref().unwrap_or("-"),
            session.cwd.as_deref().unwrap_or("-")
        );
    }
    table
}

fn digest_import_id(source_ref: &str) -> String {
    format!("source-{:x}", Sha256::digest(source_ref.as_bytes()))
}

fn current_dir_fallback() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn fresh_uuid() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seed = format!(
        "{}:{}:{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let digest = Sha256::digest(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::error::Error;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use broca_protocol::{ContentBlock, Message, Role};
    use ck_session_import::{
        HarnessReader, ImportedSession, SerializedMessageMetrics, SessionSummary,
    };

    use super::{
        check_caps_with_measure, convert_source, digest_import_id, effective_message_count,
        fresh_uuid, resolve_project_root, IMPORT_MAX_MESSAGES, IMPORT_MAX_SERIALIZED_BYTES,
    };

    #[derive(Default)]
    struct CountingReader {
        enumerate_calls: AtomicUsize,
        session_cwd_calls: AtomicUsize,
        convert_calls: AtomicUsize,
    }

    impl CountingReader {
        fn enumerate_calls(&self) -> usize {
            self.enumerate_calls.load(Ordering::SeqCst)
        }

        fn convert_calls(&self) -> usize {
            self.convert_calls.load(Ordering::SeqCst)
        }

        fn session_cwd_calls(&self) -> usize {
            self.session_cwd_calls.load(Ordering::SeqCst)
        }
    }

    impl HarnessReader for CountingReader {
        fn harness(&self) -> &'static str {
            "counting"
        }

        fn enumerate(&self) -> Result<Vec<SessionSummary>, Box<dyn Error + Send + Sync>> {
            self.enumerate_calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![SessionSummary {
                id: "selected".into(),
                title: None,
                cwd: Some("/source/project".into()),
                message_count: 1,
                approximate_size_bytes: Some(1),
            }])
        }

        fn convert(
            &self,
            session_id: &str,
        ) -> Result<ImportedSession, Box<dyn Error + Send + Sync>> {
            self.convert_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ImportedSession {
                messages: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "imported".into(),
                    }],
                )],
                source_ref: format!("counting:{session_id}"),
                warnings: Vec::new(),
                dropped_api_error_messages: 0,
                coordinate_map: None,
            })
        }

        fn session_cwd(
            &self,
            _session_id: &str,
        ) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
            self.session_cwd_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some("/source/project".into()))
        }
    }

    #[test]
    fn explicit_project_root_skips_enumeration_but_still_converts() {
        let reader = CountingReader::default();
        let configured_root = PathBuf::from("/configured/project");

        let (imported, source_cwd) =
            convert_source(&reader, "selected", Some(&configured_root)).unwrap();
        let resolved_root =
            resolve_project_root(Some(configured_root.clone()), source_cwd.as_deref());

        assert_eq!(reader.enumerate_calls(), 0);
        assert_eq!(reader.session_cwd_calls(), 0);
        assert_eq!(reader.convert_calls(), 1);
        assert_eq!(imported.source_ref, "counting:selected");
        assert_eq!(resolved_root, configured_root);
    }

    #[test]
    fn inferred_project_root_uses_selected_cwd_without_enumerating() {
        let reader = CountingReader::default();

        let (imported, source_cwd) = convert_source(&reader, "selected", None).unwrap();
        let resolved_root = resolve_project_root(None, source_cwd.as_deref());

        assert_eq!(reader.enumerate_calls(), 0);
        assert_eq!(reader.session_cwd_calls(), 1);
        assert_eq!(reader.convert_calls(), 1);
        assert_eq!(imported.source_ref, "counting:selected");
        assert_eq!(resolved_root, Path::new("/source/project"));
    }

    #[test]
    fn default_import_id_is_stable_per_source() {
        assert_eq!(
            digest_import_id("opencode:a"),
            digest_import_id("opencode:a")
        );
        assert_ne!(
            digest_import_id("opencode:a"),
            digest_import_id("opencode:b")
        );
    }

    #[test]
    fn a_message_count_override_can_only_inflate_the_real_count() {
        assert_eq!(effective_message_count(100_001, Some(1)), 100_001);
        assert_eq!(effective_message_count(2, Some(100_001)), 100_001);
        assert_eq!(effective_message_count(7, None), 7);
    }

    #[test]
    fn cap_preflight_measures_the_transcript_once() {
        let calls = Cell::new(0);
        let metrics = check_caps_with_measure(1, || {
            calls.set(calls.get() + 1);
            Ok(SerializedMessageMetrics {
                byte_count: 123,
                char_count: 17,
            })
        })
        .unwrap();

        assert_eq!(calls.get(), 1);
        assert_eq!(metrics.byte_count, 123);
        assert_eq!(metrics.estimated_tokens(), 5);
    }

    #[test]
    fn cap_preflight_preserves_size_and_serialization_failures() {
        let too_many_calls = Cell::new(0);
        let too_many = check_caps_with_measure(IMPORT_MAX_MESSAGES + 1, || {
            too_many_calls.set(too_many_calls.get() + 1);
            Ok(SerializedMessageMetrics {
                byte_count: 0,
                char_count: 0,
            })
        })
        .unwrap_err();
        assert_eq!(too_many_calls.get(), 0);
        assert!(too_many.contains("messages exceeds"));

        let too_large = check_caps_with_measure(1, || {
            Ok(SerializedMessageMetrics {
                byte_count: IMPORT_MAX_SERIALIZED_BYTES + 1,
                char_count: 0,
            })
        })
        .unwrap_err();
        assert!(too_large.contains(&format!(
            "converted history is {} bytes",
            IMPORT_MAX_SERIALIZED_BYTES + 1
        )));

        struct RejectWrites;

        impl std::io::Write for RejectWrites {
            fn write(&mut self, _bytes: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("intentional write failure"))
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let serialization_error = serde_json::to_writer(RejectWrites, &["message"]).unwrap_err();
        let error = check_caps_with_measure(1, || Err(serialization_error)).unwrap_err();
        assert!(error.starts_with("serialize converted messages for size check:"));
    }

    #[test]
    fn fresh_session_id_is_uuid_shaped_and_unique() {
        let first = fresh_uuid();
        let second = fresh_uuid();
        assert_eq!(first.len(), 36);
        assert_eq!(&first[14..15], "4");
        assert_ne!(first, second);
    }
}
