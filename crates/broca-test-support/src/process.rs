use std::process::ExitStatus;
use std::time::{Duration, Instant};

#[cfg(any(windows, test))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ProcessInfo {
    image: String,
    pid: u32,
    parent_pid: u32,
    in_job: Option<bool>,
}

#[cfg(any(windows, test))]
fn format_survivor_block(root_pid: u32, survivors: &[ProcessInfo]) -> Option<String> {
    if survivors.is_empty() {
        return None;
    }
    let mut block = format!("test rig: surviving processes after cleanup of pid {root_pid}:");
    for process in survivors {
        use std::fmt::Write as _;
        let _ = write!(
            block,
            "\n  image={:?} pid={} parent_pid={} in_job={:?}",
            process.image, process.pid, process.parent_pid, process.in_job
        );
    }
    Some(block)
}

#[cfg(windows)]
mod windows_processes {
    use super::ProcessInfo;

    pub(super) struct ProcessTreeDiagnostic {
        pub root_pid: u32,
        pub tree: Vec<ProcessInfo>,
        pub tree_pids: Vec<u32>,
        pub errors: Vec<String>,
    }

    pub(super) fn capture_before(root_pid: u32) -> ProcessTreeDiagnostic {
        let mut errors = Vec::new();
        let all_processes = process_snapshot(&mut errors);
        let mut tree_pids = vec![root_pid];
        loop {
            let previous_len = tree_pids.len();
            for process in &all_processes {
                if tree_pids.contains(&process.parent_pid) && !tree_pids.contains(&process.pid) {
                    tree_pids.push(process.pid);
                }
            }
            if tree_pids.len() == previous_len {
                break;
            }
        }
        let mut tree: Vec<_> = all_processes
            .into_iter()
            .filter(|process| tree_pids.contains(&process.pid))
            .collect();
        for process in &mut tree {
            process.in_job = process_job_status(process.pid, &mut errors);
        }
        ProcessTreeDiagnostic {
            root_pid,
            tree,
            tree_pids,
            errors,
        }
    }

    pub(super) fn report_after(diagnostic: ProcessTreeDiagnostic) {
        let mut errors = diagnostic.errors;
        let tree_pids = diagnostic.tree_pids;
        let tracked = tree_pids.len();
        let observed = diagnostic.tree.len();
        let root_pid = diagnostic.root_pid;
        let processes = process_snapshot(&mut errors);
        let mut survivors: Vec<_> = processes
            .into_iter()
            .filter(|process| {
                tree_pids.contains(&process.pid) || tree_pids.contains(&process.parent_pid)
            })
            .collect();
        for process in &mut survivors {
            process.in_job = process_job_status(process.pid, &mut errors);
        }
        if let Some(block) = super::format_survivor_block(diagnostic.root_pid, &survivors) {
            let _ = writeln_stderr(&block);
        }
        let first_error = errors
            .first()
            .map(|error| format!(" first_error={error:?}"));
        let _ = writeln_stderr(&format!(
            "test rig: process tree cleanup pid={} tracked={} records={} survivors={} errors={}{}",
            root_pid,
            tracked,
            observed,
            survivors.len(),
            errors.len(),
            first_error.unwrap_or_default()
        ));
    }

    pub(super) fn report_skipped(reason: &str) {
        let _ = writeln_stderr(&format!("test rig: process tree cleanup skipped: {reason}"));
    }

    fn process_snapshot(errors: &mut Vec<String>) -> Vec<ProcessInfo> {
        match broca_owner_only::test_support::process_snapshot() {
            Ok(processes) => processes
                .into_iter()
                .map(|process| ProcessInfo {
                    image: process.image,
                    pid: process.pid,
                    parent_pid: process.parent_pid,
                    in_job: None,
                })
                .collect(),
            Err(error) => {
                errors.push(format!("process snapshot failed: {error}"));
                Vec::new()
            }
        }
    }

    fn process_job_status(pid: u32, errors: &mut Vec<String>) -> Option<bool> {
        match broca_owner_only::test_support::process_is_in_job(pid) {
            Ok(in_job) => Some(in_job),
            Err(error) => {
                errors.push(format!("could not query job membership for {pid}: {error}"));
                None
            }
        }
    }

    fn writeln_stderr(message: &str) -> std::io::Result<()> {
        use std::io::Write;
        writeln!(std::io::stderr().lock(), "{message}")
    }
}

pub(crate) fn describe_exit_status(status: ExitStatus) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("{status} (signal {signal})");
        }
    }
    status.to_string()
}

/// Stop a rig's child before removing its files or shutting down the Tokio runtime.
///
/// On Windows, killing a supervisor does not kill its modules. Terminate its tree
/// while the supervisor is still alive so grandchildren cannot retain CI output
/// handles. The caller must also set `kill_on_drop(true)` when spawning, to cover
/// startup failures before the child is installed in the rig.
/// A pre- and post-kill process snapshot names descendants that remain alive, so
/// leaked test processes can be identified from CI output.
///
/// Drop cannot await, so reap with bounded synchronous polling. This also works
/// inside a current-thread runtime, where blocking on an async wait would deadlock.
/// Cleanup errors are reported rather than panicking during another panic.
/// A child that failed before cleanup is reported with its exit status (and Unix signal),
/// so a startup kill is not hidden behind the rig's readiness timeout.
pub fn kill_and_reap(child: &mut tokio::process::Child) {
    if let Ok(Some(status)) = child.try_wait() {
        if !status.success() {
            eprintln!(
                "test rig: child exited before cleanup: {}",
                describe_exit_status(status)
            );
        }
        #[cfg(windows)]
        windows_processes::report_skipped("child exited before cleanup");
        return;
    }
    #[cfg(windows)]
    let process_tree = child.id().map(windows_processes::capture_before);
    #[cfg(windows)]
    if let Some(pid) = child.id() {
        #[cfg(test)]
        // The test uses a no-op taskkill so its detached grandchild survives until the post-kill snapshot.
        let taskkill =
            std::env::var_os("BROCA_TEST_SUPPORT_TASKKILL").unwrap_or_else(|| "taskkill".into());
        #[cfg(not(test))]
        let taskkill = std::ffi::OsString::from("taskkill");
        let result = std::process::Command::new(taskkill)
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        match result {
            Ok(status) if status.success() => {}
            Ok(status) => eprintln!(
                "test rig: taskkill could not terminate process tree {pid}: {}",
                describe_exit_status(status)
            ),
            Err(error) => {
                eprintln!("test rig: taskkill could not terminate process tree {pid}: {error}")
            }
        }
    }
    if let Err(error) = child.start_kill() {
        eprintln!("test rig: could not kill child: {error}");
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            result => {
                eprintln!("test rig: child was not reaped within cleanup deadline: {result:?}");
                break;
            }
        }
    }
    #[cfg(windows)]
    match process_tree {
        Some(diagnostic) => windows_processes::report_after(diagnostic),
        None => windows_processes::report_skipped("child process id is unavailable"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sleeping_child() {
        if std::env::var_os("BROCA_TEST_SUPPORT_SLEEP").is_some() {
            std::thread::sleep(Duration::from_secs(60));
        }
    }

    #[cfg(windows)]
    #[test]
    fn supervisor_child() {
        if std::env::var_os("BROCA_TEST_SUPPORT_TREE").is_some() {
            // Deliberately inherit stdout: killing only the supervisor must not pass
            // the pipe-closure assertion in cleanup_terminates_descendant_output_handles.
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "process::tests::sleeping_child", "--nocapture"])
                .env("BROCA_TEST_SUPPORT_SLEEP", "1")
                .spawn()
                .unwrap();
            println!("descendant-started");
            child.wait().unwrap();
        }
    }

    #[cfg(windows)]
    fn survivor_test_directory() -> std::path::PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "broca-test-support-survivor-{}-{nonce}",
            std::process::id()
        ))
    }

    #[cfg(windows)]
    struct StopSurvivor(std::path::PathBuf);

    #[cfg(windows)]
    impl Drop for StopSurvivor {
        fn drop(&mut self) {
            let _ = std::fs::write(&self.0, b"stop");
        }
    }

    #[cfg(windows)]
    #[test]
    fn survivor_grandchild() {
        if let Some(path) = std::env::var_os("BROCA_TEST_SUPPORT_SURVIVOR_STOP") {
            let path = std::path::PathBuf::from(path);
            while !path.exists() {
                std::thread::sleep(Duration::from_millis(10));
            }
            let _ = std::fs::write(path.with_extension("done"), b"done");
        }
    }

    #[cfg(windows)]
    #[test]
    fn survivor_daemon_child() {
        let Some(path) = std::env::var_os("BROCA_TEST_SUPPORT_SURVIVOR_STOP") else {
            return;
        };
        use std::os::windows::process::CommandExt;
        use std::process::{Command, Stdio};
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        let path = std::path::PathBuf::from(path);
        let mut grandchild = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process::tests::survivor_grandchild",
                "--nocapture",
            ])
            .env("BROCA_TEST_SUPPORT_SURVIVOR_STOP", &path)
            .creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        println!("SURVIVOR_TEST_GRANDCHILD_PID={}", grandchild.id());
        while !path.exists() {
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = grandchild.wait();
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn survivor_diagnostic_parent() {
        if std::env::var_os("BROCA_TEST_SUPPORT_SURVIVOR_PARENT").is_none() {
            return;
        }
        use tokio::io::{AsyncBufReadExt, BufReader};
        let stop_file =
            std::path::PathBuf::from(std::env::var_os("BROCA_TEST_SUPPORT_SURVIVOR_STOP").unwrap());
        let _stop_on_drop = StopSurvivor(stop_file.clone());
        let mut daemon = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process::tests::survivor_daemon_child",
                "--nocapture",
            ])
            .env("BROCA_TEST_SUPPORT_SURVIVOR_STOP", &stop_file)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(daemon.stdout.take().unwrap()).lines();
        let pid_line = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let line = lines
                    .next_line()
                    .await
                    .unwrap()
                    .expect("daemon must report its detached grandchild");
                if line.starts_with("SURVIVOR_TEST_GRANDCHILD_PID=") {
                    break line;
                }
            }
        })
        .await
        .expect("daemon must start its detached grandchild");
        let grandchild_pid = pid_line.split_once('=').unwrap().1.parse::<u32>().unwrap();
        kill_and_reap(&mut daemon);
        println!("SURVIVOR_TEST_PID={grandchild_pid}");
        let _ = std::fs::write(&stop_file, b"stop");
    }

    #[cfg(windows)]
    struct RemoveTestDirectory(std::path::PathBuf);

    #[cfg(windows)]
    impl Drop for RemoveTestDirectory {
        fn drop(&mut self) {
            let done_file = self.0.join("stop").with_extension("done");
            for _ in 0..500 {
                if done_file.exists() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn cleanup_reports_detached_grandchild_survivor() {
        let directory = survivor_test_directory();
        std::fs::create_dir(&directory).unwrap();
        let _remove_directory_on_drop = RemoveTestDirectory(directory.clone());
        let _stop_on_drop = StopSurvivor(directory.join("stop"));
        let executable = std::env::current_exe().unwrap();
        std::fs::copy(&executable, directory.join("taskkill.exe")).unwrap();
        let output = tokio::time::timeout(
            Duration::from_secs(20),
            tokio::process::Command::new(&executable)
                .args([
                    "--exact",
                    "process::tests::survivor_diagnostic_parent",
                    "--nocapture",
                ])
                .env("BROCA_TEST_SUPPORT_SURVIVOR_PARENT", "1")
                .env("BROCA_TEST_SUPPORT_SURVIVOR_STOP", directory.join("stop"))
                .env(
                    "BROCA_TEST_SUPPORT_TASKKILL",
                    directory.join("taskkill.exe"),
                )
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("survivor diagnostic helper must finish")
        .unwrap();
        assert!(
            output.status.success(),
            "helper failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let pid = stdout
            .lines()
            .find_map(|line| line.strip_prefix("SURVIVOR_TEST_PID="))
            .expect("helper must report the grandchild pid");
        let image = executable.file_name().unwrap().to_string_lossy();
        assert!(
            stderr.contains(&*image),
            "survivor image missing from: {stderr}"
        );
        assert!(
            stderr.contains(&format!("pid={pid}")),
            "survivor pid missing from: {stderr}"
        );
        let done_file = directory.join("stop").with_extension("done");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !done_file.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached grandchild must exit after the test releases it");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn cleanup_terminates_descendant_output_handles() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "process::tests::supervisor_child", "--nocapture"])
            .env("BROCA_TEST_SUPPORT_TREE", "1")
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut line = String::new();
            loop {
                line.clear();
                assert_ne!(
                    stdout.read_line(&mut line).await.unwrap(),
                    0,
                    "supervisor stdout closed early; exit status: {:?}",
                    child.try_wait().unwrap().map(describe_exit_status)
                );
                if line.contains("descendant-started") {
                    break;
                }
            }
        })
        .await
        .expect("supervisor must spawn its descendant");
        kill_and_reap(&mut child);
        tokio::time::timeout(Duration::from_secs(5), stdout.read_to_end(&mut Vec::new()))
            .await
            .expect("descendant must release inherited stdout")
            .unwrap();
        assert!(child.try_wait().unwrap().is_some());
    }

    #[tokio::test]
    async fn cleanup_kills_and_reaps_before_returning() {
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "process::tests::sleeping_child"])
            .env("BROCA_TEST_SUPPORT_SLEEP", "1")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        assert!(child.try_wait().unwrap().is_none());
        kill_and_reap(&mut child);
        let status = child.try_wait().unwrap().expect("child must be reaped");
        assert!(
            !status.success(),
            "child should be killed, not finish its sleep: {}",
            describe_exit_status(status)
        );
        // Cleanup is also safe after the process has already been reaped.
        kill_and_reap(&mut child);
    }

    #[test]
    fn survivor_block_names_image_and_pid() {
        let block = format_survivor_block(
            11,
            &[ProcessInfo {
                image: "grandchild.exe".to_owned(),
                pid: 22,
                parent_pid: 11,
                in_job: Some(false),
            }],
        )
        .unwrap();
        assert!(block.contains("grandchild.exe"), "{block}");
        assert!(block.contains("pid=22"), "{block}");
        assert!(block.contains("parent_pid=11"), "{block}");
        assert!(block.contains("in_job=Some(false)"), "{block}");
    }

    #[cfg(unix)]
    #[test]
    fn exit_status_diagnostics_name_unix_signal() {
        use std::os::unix::process::ExitStatusExt;
        let message = describe_exit_status(ExitStatus::from_raw(9));
        assert!(message.contains("signal 9"), "{message}");
        let message = describe_exit_status(ExitStatus::from_raw(17 << 8));
        assert!(message.contains("17"), "{message}");
    }

    #[cfg(windows)]
    #[test]
    fn exit_status_diagnostics_name_windows_exit_code() {
        use std::os::windows::process::ExitStatusExt;
        let message = describe_exit_status(ExitStatus::from_raw(17));
        assert!(message.contains("17"), "{message}");
    }
}
