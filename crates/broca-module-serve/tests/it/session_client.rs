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

/// The log tails, watchdog stall files and stored sessions of one rig whose directories are not
/// under the `runtime/` layout [`daemon_diagnostics`] assumes, so the caller passes the paths.
/// `data_home` is the `cortexkit` directory under the daemon's `XDG_DATA_HOME`; `state_root`
/// is broca's `BROCA_STATE_ROOT`. `session` names the session the failure is about.
pub fn rig_diagnostics(data_home: &Path, state_root: &Path, session: &str) -> String {
    let mut output = rig_log_diagnostics(data_home, state_root);
    output.push_str(&format!("--- failing session: {session} ---\n"));
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
