//! Deadline evidence for clients of a real daemon, collected before the rig is torn down.

pub use broca_test_support::rig_log_diagnostics;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

pub const CLIENT_DEADLINE: Duration = Duration::from_secs(120);

pub struct ClientRun {
    pub lines: Vec<String>,
    pub exit_code: i32,
}

pub async fn run_with_deadline(
    command: &mut Command,
    deadline: Duration,
    observe: impl Fn(&str),
    rig_diagnostics: impl Fn() -> String,
) -> Result<ClientRun, String> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn broca-session");
    let stdout = child.stdout.take().expect("broca-session stdout");
    let stderr = child.stderr.take().expect("broca-session stderr");
    let captured_stderr = Arc::new(Mutex::new(Vec::new()));
    let stderr_lines = Arc::clone(&captured_stderr);
    let mut stderr_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("[broca-session stderr] {line}");
            stderr_lines.lock().unwrap().push(line);
        }
    });
    let mut lines = Vec::new();
    let completed = tokio::time::timeout(deadline, async {
        let mut stdout_lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = stdout_lines.next_line().await {
            observe(&line);
            eprintln!("[broca-session] {line}");
            lines.push(line);
        }
        let status = child.wait().await.expect("wait for broca-session");
        (&mut stderr_task).await.expect("read broca-session stderr");
        status
    })
    .await;

    match completed {
        Ok(status) => {
            lines.extend(captured_stderr.lock().unwrap().iter().cloned());
            Ok(ClientRun {
                lines,
                exit_code: status.code().unwrap_or(-1),
            })
        }
        Err(_) => {
            let mut diagnostics =
                format!("=== broca-session deadline diagnostics ({deadline:?}) ===\n");
            append_process_status(&mut diagnostics, "broca-session", &mut child);
            diagnostics.push_str(&format!("--- client stdout ---\n{}\n", lines.join("\n")));
            diagnostics.push_str(&format!(
                "--- client stderr ---\n{}\n",
                captured_stderr.lock().unwrap().join("\n")
            ));
            diagnostics.push_str(&rig_diagnostics());
            // Emit evidence before cleanup: killing the client closes its subscription.
            eprintln!("{diagnostics}");
            let _ = child.kill().await;
            let cleanup = match child.wait().await {
                Ok(status) => format!("reaped broca-session after deadline: {status}"),
                Err(error) => format!("failed to reap broca-session after deadline: {error}"),
            };
            stderr_task.abort();
            let _ = stderr_task.await;
            eprintln!("{cleanup}");
            Err(format!("{diagnostics}\n{cleanup}"))
        }
    }
}

pub fn daemon_diagnostics(rig_dir: &Path, session: &str, daemon: &mut Child) -> String {
    let mut output = String::new();
    append_process_status(&mut output, "subc-core daemon", daemon);
    output.push_str(&rig_diagnostics(
        &rig_dir.join("runtime/data-home/cortexkit"),
        &rig_dir.join("runtime/broca-state"),
        session,
    ));
    output
}

/// The log tails, watchdog stall files and session WAL of one rig whose directories are not
/// under the `runtime/` layout [`daemon_diagnostics`] assumes, so the caller passes the paths. `data_home` is the `cortexkit` directory under the daemon's
/// `XDG_DATA_HOME`; `state_root` is broca's `BROCA_STATE_ROOT`.
pub fn rig_diagnostics(data_home: &Path, state_root: &Path, session: &str) -> String {
    let mut output = rig_log_diagnostics(data_home, state_root);
    append_session_wal(&mut output, &state_root.join("wal"), session);
    output
}

/// The daemon's supervisor record of `module_id`, asked over a fresh control connection.
///
/// A module the daemon reports as `failed` may have left no log of its own: a process that
/// never started writes neither its stderr capture nor its structured log. The supervisor's
/// list entry (spawn generation, last exit, restart budget), retained stderr and terminal
/// history are then the only account of why, and they are gone once the rig is torn down.
pub async fn supervisor_diagnostics(connection_file: &Path, module_id: &str) -> String {
    let report = match broca_subc::SubcConnection::connect(connection_file).await {
        Ok(conn) => conn.supervisor_module_report(module_id).await,
        Err(error) => serde_json::Value::String(format!("connect to the daemon: {error}")),
    };
    format!(
        "--- daemon supervisor record: {module_id} ---\n{}\n",
        serde_json::to_string_pretty(&report).unwrap_or_else(|error| error.to_string())
    )
}

pub fn append_process_status(output: &mut String, name: &str, child: &mut Child) {
    match child.try_wait() {
        Ok(Some(status)) => output.push_str(&format!("{name} process status: exited {status}\n")),
        Ok(None) => output.push_str(&format!(
            "{name} process status: running (pid={:?})\n",
            child.id()
        )),
        Err(error) => output.push_str(&format!("{name} process status: unavailable ({error})\n")),
    }
}

pub fn append_session_wal(output: &mut String, wal_dir: &Path, session: &str) {
    output.push_str(&format!("--- session WAL record kinds: {session} ---\n"));
    output.push_str("Decoded physical prefix, not a durable-head assertion; a concurrent torn tail may be omitted.\n");
    let entries = match std::fs::read_dir(wal_dir) {
        Ok(entries) => entries,
        Err(error) => {
            output.push_str(&format!("unavailable: {error}\n"));
            return;
        }
    };
    let mut paths: Vec<_> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "wal"))
        .collect();
    paths.sort();
    let mut found = false;
    for path in paths {
        let frames = match broca_wal::replay_file(&path, broca_wal::DEFAULT_MAX_READ_BYTES) {
            Ok(frames) => frames,
            Err(error) => {
                output.push_str(&format!("{}: decode error: {error}\n", path.display()));
                continue;
            }
        };
        // The daemon canonicalizes project roots, so find the admitted session rather
        // than guessing its WAL address from the client's spelling of that root.
        if !frames.iter().any(|frame| matches!(&frame.record, broca_wal::WalRecord::RunStarted { session: bind, .. } if bind.session == session)) {
            continue;
        }
        found = true;
        output.push_str(&format!("{}:\n", path.display()));
        for frame in frames {
            match serde_json::to_value(&frame.record) {
                Ok(value) => {
                    let kind = value["type"]
                        .as_str()
                        .unwrap_or("unknown")
                        .split('_')
                        .map(|part| {
                            let mut chars = part.chars();
                            chars
                                .next()
                                .map(|first| {
                                    first.to_ascii_uppercase().to_string() + chars.as_str()
                                })
                                .unwrap_or_default()
                        })
                        .collect::<String>();
                    output.push_str(&format!(
                        "seq={} {kind}{}\n",
                        frame.seq,
                        record_detail(&frame.record)
                    ));
                }
                Err(error) => {
                    output.push_str(&format!("seq={} kind unavailable: {error}\n", frame.seq))
                }
            }
        }
    }
    if !found {
        output.push_str("No decoded RunStarted matched this session.\n");
    }
}

/// Identifiers, outcomes and timings of the records that decide whether a run ends: run
/// boundaries and compaction calls, answers and waits. Never message content.
fn record_detail(record: &broca_wal::WalRecord) -> String {
    use broca_wal::WalRecord as R;
    match record {
        R::RunStarted {
            run_id, send_id, ..
        } => format!(" run_id={} send_id={send_id:?}", run_id.0),
        R::RunFinished {
            reason,
            ts_ms,
            error,
            ..
        } => format!(
            " reason={reason:?} ts_ms={ts_ms:?} provider_code={:?}",
            error
                .as_ref()
                .and_then(|error| error.provider_code.as_deref())
        ),
        R::CompactionRequest { request_id, .. } => format!(" request_id={request_id}"),
        R::CompactionAnswer {
            request_id,
            disposition,
            wait,
            ..
        } => format!(
            " request_id={request_id} disposition={disposition:?} wait_bound_ms={:?}",
            wait.as_ref().map(|wait| wait.bound_ms)
        ),
        R::CompactionWait { exited, .. } => format!(" exited={exited:?}"),
        R::CompactionMessage {
            kind,
            version,
            range,
            request_id,
            ..
        } => format!(
            " kind={kind:?} version={version} range=[{},{}) request_id={request_id:?}",
            range.from, range.to
        ),
        _ => String::new(),
    }
}

#[test]
fn wal_record_detail_names_compaction_outcomes_and_terminals() {
    let wait = broca_wal::WalRecord::CompactionAnswer {
        provider: "p".into(),
        request_id: "r1".into(),
        cursor: broca_wal::CompactionCursor {
            lineage_id: "l".into(),
            after_ordinal: None,
        },
        disposition: broca_wal::CompactionDisposition::Wait,
        rejected: None,
        wait: Some(broca_wal::CompactionWait {
            reason: "reason text stays out of the dump".into(),
            bound_ms: 200,
        }),
    };
    let detail = record_detail(&wait);
    assert_eq!(
        detail,
        " request_id=r1 disposition=Wait wait_bound_ms=Some(200)"
    );
    let exited = broca_wal::WalRecord::CompactionWait {
        exited: broca_wal::CompactionWaitExit::Exceeded,
        reason: "reason text stays out of the dump".into(),
    };
    assert_eq!(record_detail(&exited), " exited=Exceeded");
}

#[test]
fn wal_diagnostics_name_missing_and_undecodable_files() {
    let dir = broca_test_support::scratch_root()
        .join(format!("session-wal-diagnostics-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut output = String::new();
    append_session_wal(&mut output, &dir, "s1");
    assert!(output.contains("unavailable:"));
    std::fs::create_dir_all(&dir).unwrap();
    // A damaged final record is a recoverable torn tail, so it decodes. Damage followed by an
    // intact record is real corruption: flip one payload byte of the first of two frames.
    let step = |id| broca_wal::WalRecord::StepStarted {
        step_id: broca_protocol::StepId(id),
    };
    let mut first = broca_wal::framing::encode(1, 0, &step(1)).unwrap();
    let last = first.len() - 1;
    first[last] ^= 0xff;
    let second = broca_wal::framing::encode(2, 0, &step(2)).unwrap();
    std::fs::write(dir.join("broken.wal"), [first, second].concat()).unwrap();
    output.clear();
    append_session_wal(&mut output, &dir, "s1");
    assert!(output.contains("broken.wal: decode error:"), "{output}");
    assert!(output.contains("No decoded RunStarted matched this session."));
    std::fs::remove_dir_all(dir).unwrap();
}
