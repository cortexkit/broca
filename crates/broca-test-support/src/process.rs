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
#[derive(Clone, Debug, Eq, PartialEq)]
enum ProcessWaitOutcome {
    Exited,
    TimedOut,
    OpenFailedInvalidParameter,
    Failed(String),
}

#[cfg(any(windows, test))]
#[derive(Clone, Debug, Eq, PartialEq)]
enum ProcessWaitClassification {
    Exited,
    StillAlive,
    Failed(String),
}

#[cfg(any(windows, test))]
fn classify_process_wait(outcome: ProcessWaitOutcome) -> ProcessWaitClassification {
    match outcome {
        ProcessWaitOutcome::Exited => ProcessWaitClassification::Exited,
        ProcessWaitOutcome::OpenFailedInvalidParameter => ProcessWaitClassification::Exited,
        ProcessWaitOutcome::TimedOut => ProcessWaitClassification::StillAlive,
        ProcessWaitOutcome::Failed(error) => ProcessWaitClassification::Failed(error),
    }
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

#[cfg(any(windows, test))]
fn report_cleanup_diagnostic(message: &str) {
    // Libtest captures eprintln!, but direct stderr writes split its status lines.
    // Keep a durable copy too, because successful tests hide captured diagnostics.
    eprintln!("{message}");
    if let Err(error) = append_cleanup_diagnostic(message) {
        eprintln!("test rig: cannot persist process cleanup diagnostic: {error}");
    }
}

#[cfg(any(windows, test))]
fn append_cleanup_diagnostic(message: &str) -> std::io::Result<()> {
    use std::io::Write;
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"));
    let directory = target.join("rig-survivors");
    std::fs::create_dir_all(&directory)?;
    let thread = std::thread::current();
    let test = thread.name().unwrap_or("unnamed-test");
    let filename: String = test
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.join(format!("{filename}.txt")))?;
    writeln!(
        file,
        "test={test} test_process={}\n{message}",
        std::process::id()
    )
}

#[cfg(windows)]
mod windows_processes {
    use super::{classify_process_wait, Duration, Instant, ProcessInfo, ProcessWaitClassification};

    // OpenProcess uses ERROR_INVALID_PARAMETER when the PID has already exited.
    const ERROR_INVALID_PARAMETER_CODE: i32 = 87;

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
            match process_job_status(process.pid) {
                Ok(in_job) => process.in_job = in_job,
                Err(error) => errors.push(format!(
                    "could not query job membership for {}: {error}",
                    process.pid
                )),
            }
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
        let candidates: Vec<_> = processes
            .into_iter()
            .filter(|process| {
                tree_pids.contains(&process.pid) || tree_pids.contains(&process.parent_pid)
            })
            .collect();

        // Share one deadline across every candidate so a large tree cannot multiply the wait.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut survivors = Vec::new();
        for process in candidates {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let timeout_ms = remaining.as_millis().min(u32::MAX as u128) as u32;
            match classify_process_wait(process_wait_outcome(process.pid, timeout_ms)) {
                ProcessWaitClassification::Exited => {}
                ProcessWaitClassification::StillAlive => survivors.push(process),
                ProcessWaitClassification::Failed(error) => {
                    errors.push(format!(
                        "could not wait for process {}: {error}",
                        process.pid
                    ));
                }
            }
        }

        let mut confirmed_survivors = Vec::new();
        for mut process in survivors {
            match process_job_status(process.pid) {
                Ok(Some(in_job)) => {
                    process.in_job = Some(in_job);
                    confirmed_survivors.push(process);
                }
                // A process may exit between the wait and the job-membership query.
                Ok(None) => {}
                Err(error) => {
                    errors.push(format!(
                        "could not query job membership for {}: {error}",
                        process.pid
                    ));
                    confirmed_survivors.push(process);
                }
            }
        }
        let survivors = confirmed_survivors;
        if let Some(block) = super::format_survivor_block(diagnostic.root_pid, &survivors) {
            super::report_cleanup_diagnostic(&block);
        }
        // Clean cleanup stays silent, including in the artifact directory.
        if survivors.is_empty() && errors.is_empty() {
            return;
        }
        let first_error = errors
            .first()
            .map(|error| format!(" first_error={error:?}"));
        super::report_cleanup_diagnostic(&format!(
            "test rig: process tree cleanup pid={} tracked={} records={} survivors={} errors={}{}",
            root_pid,
            tracked,
            observed,
            survivors.len(),
            errors.len(),
            first_error.unwrap_or_default()
        ));
    }

    /// Skipping is routine (the child already exited, or never got a pid), so it stays inside
    /// the harness's capture and only shows with a failing test's output.
    pub(super) fn report_skipped(reason: &str) {
        eprintln!("test rig: process tree cleanup skipped: {reason}");
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

    fn process_wait_outcome(pid: u32, timeout_ms: u32) -> super::ProcessWaitOutcome {
        use broca_owner_only::test_support::ProcessWaitError;

        match broca_owner_only::test_support::process_wait_for_exit(pid, timeout_ms) {
            Ok(true) => super::ProcessWaitOutcome::Exited,
            Ok(false) => super::ProcessWaitOutcome::TimedOut,
            Err(ProcessWaitError::Open(error))
                if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER_CODE) =>
            {
                super::ProcessWaitOutcome::OpenFailedInvalidParameter
            }
            Err(ProcessWaitError::Open(error)) => {
                super::ProcessWaitOutcome::Failed(format!("OpenProcess failed: {error}"))
            }
            Err(ProcessWaitError::Wait(error)) => {
                super::ProcessWaitOutcome::Failed(format!("WaitForSingleObject failed: {error}"))
            }
        }
    }

    fn process_job_status(pid: u32) -> Result<Option<bool>, std::io::Error> {
        match broca_owner_only::test_support::process_is_in_job(pid) {
            Ok(in_job) => Ok(Some(in_job)),
            Err(error) if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER_CODE) => Ok(None),
            Err(error) => Err(error),
        }
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

/// Stop a rig's supervisor and every process it launched, returning only once none of them
/// is still running, so the rig can then remove the directory they write into.
///
/// On Unix, [`kill_and_reap`] ends only the supervisor. Its modules are left running as
/// orphans: a module still starting up goes on creating its state files, so the rig's
/// directory removal can fail on a directory a module is still filling, or a module can
/// recreate the directory after it was removed.
///
/// The tree is frozen with SIGSTOP from the supervisor down, and a process's children are
/// listed only once it is stopped and so can no longer fork. That way no module can be
/// launched or respawned between being listed and being killed. Every frozen process is
/// then killed, the supervisor reaped, and the rest awaited until they are gone. A process
/// still running at the deadline, or a failure of the process tools, is reported rather
/// than panicking, because this runs from `Drop`, often during another panic. A supervisor
/// that already exited before cleanup leaves no tree to find; [`kill_and_reap`] reports it.
///
/// On Windows [`kill_and_reap`] already ends the whole tree and waits for it.
pub fn kill_and_reap_tree(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    {
        let descendants = match (child.try_wait(), child.id()) {
            (Ok(None), Some(root)) => unix_tree::freeze_descendants(root),
            _ => Vec::new(),
        };
        unix_tree::signal("KILL", &descendants);
        kill_and_reap(child);
        unix_tree::await_exit(&descendants);
    }
    #[cfg(not(unix))]
    kill_and_reap(child);
}

/// Remove a rig's scratch directory, reporting a failure instead of hiding it.
///
/// Call it only after [`kill_and_reap_tree`] or an equivalent has ended every process that
/// writes into the directory. While another panic unwinds the failure is printed, because
/// a second panic would abort the test binary; otherwise it fails the test.
pub fn remove_rig_dir(dir: impl AsRef<std::path::Path>) {
    let dir = dir.as_ref();
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            let message = format!("test rig: cannot remove {}: {error}", dir.display());
            if std::thread::panicking() {
                eprintln!("{message}");
            } else {
                panic!("{message}");
            }
        }
    }
}

/// Unix process-tree helpers built on the `pgrep` and `kill` utilities, because this crate
/// forbids the unsafe calls that signalling a process directly would take.
#[cfg(unix)]
mod unix_tree {
    use super::{Duration, Instant};
    use std::process::{Command, Stdio};

    /// Stop `root` and then every process below it, top-down. Returns the descendants
    /// (without `root`) in the order they were stopped.
    pub(super) fn freeze_descendants(root: u32) -> Vec<u32> {
        signal("STOP", &[root]);
        let mut tree = vec![root];
        let mut next = 0;
        while let Some(&parent) = tree.get(next) {
            next += 1;
            for pid in children(parent) {
                if !tree.contains(&pid) {
                    signal("STOP", &[pid]);
                    tree.push(pid);
                }
            }
        }
        tree.remove(0);
        tree
    }

    /// Send `name` (as `kill -s` spells it) to `pids`. A process that has already exited is
    /// not an error, so only a failure to run `kill` at all is reported.
    pub(super) fn signal(name: &str, pids: &[u32]) {
        if pids.is_empty() {
            return;
        }
        let result = Command::new("kill")
            .args(["-s", name])
            .args(pids.iter().map(u32::to_string))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Err(error) = result {
            eprintln!("test rig: cannot send SIG{name} to {pids:?}: {error}");
        }
    }

    fn children(parent: u32) -> Vec<u32> {
        match Command::new("pgrep")
            .args(["-P", &parent.to_string()])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
        {
            Ok(output) => String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|line| line.trim().parse().ok())
                .collect(),
            Err(error) => {
                eprintln!("test rig: cannot list children of {parent}: {error}");
                Vec::new()
            }
        }
    }

    /// True while `pid` names a process that can still run. On Linux a zombie counts as
    /// gone: it can no longer write, and in a container whose first process does not reap
    /// orphans it may never disappear. Elsewhere the system's init process reaps orphans
    /// promptly, so `kill -0` is enough.
    pub(super) fn exists(pid: u32) -> bool {
        #[cfg(target_os = "linux")]
        {
            // The state follows the command name, which may itself contain ')'.
            std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
                stat.rsplit_once(')')
                    .and_then(|(_, rest)| rest.split_whitespace().next())
                    .is_some_and(|state| !matches!(state, "Z" | "X"))
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
        }
    }

    /// Wait until no process in `pids` can still run, up to a deadline.
    pub(super) fn await_exit(pids: &[u32]) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut running = pids.to_vec();
        loop {
            running.retain(|&pid| exists(pid));
            if running.is_empty() {
                return;
            }
            if Instant::now() >= deadline {
                eprintln!("test rig: processes still running after cleanup: {running:?}");
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sleeping_child() {
        if std::env::var_os("BROCA_TEST_SUPPORT_SLEEP").is_some() {
            if let Some(ready) = std::env::var_os("BROCA_TEST_SUPPORT_SLEEP_READY") {
                println!("nested cleanup child stdout");
                eprintln!("nested cleanup child stderr");
                std::fs::write(ready, b"ready").unwrap();
            }
            std::thread::sleep(Duration::from_secs(60));
        }
    }

    #[test]
    fn captured_survivor_report_child() {
        if std::env::var_os("BROCA_TEST_SUPPORT_REPORT").is_some() {
            let block = format_survivor_block(
                11,
                &[ProcessInfo {
                    image: "detached-child.exe".to_owned(),
                    pid: 22,
                    parent_pid: 11,
                    in_job: Some(false),
                }],
            )
            .unwrap();
            report_cleanup_diagnostic(&block);
            report_cleanup_diagnostic("second cleanup diagnostic");
        }
    }

    #[test]
    fn cleanup_diagnostics_preserve_libtest_status_and_append_artifact() {
        let directory = std::env::temp_dir().join(format!(
            "broca-captured-survivor-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "process::tests::captured_survivor_report_child"])
            .env("BROCA_TEST_SUPPORT_REPORT", "1")
            .env("CARGO_TARGET_DIR", &directory)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.contains("test process::tests::captured_survivor_report_child ... ok"),
            "{stdout}"
        );
        assert!(!stdout.contains("surviving processes"), "{stdout}");
        assert!(!stderr.contains("surviving processes"), "{stderr}");
        let path =
            directory.join("rig-survivors/process__tests__captured_survivor_report_child.txt");
        let report = std::fs::read_to_string(&path);
        let _ = std::fs::remove_dir_all(&directory);
        let report = report.expect("successful tests must retain survivor diagnostics");
        assert!(
            report.contains("test=process::tests::captured_survivor_report_child"),
            "{report}"
        );
        assert!(
            report.contains("detached-child.exe") && report.contains("pid=22"),
            "{report}"
        );
        assert!(report.contains("second cleanup diagnostic"), "{report}");
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
        let ready = crate::scratch_root().join("cleanup-child-ready");
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "process::tests::sleeping_child", "--nocapture"])
            .env("BROCA_TEST_SUPPORT_SLEEP", "1")
            .env("BROCA_TEST_SUPPORT_SLEEP_READY", &ready)
            // Nested libtest banners bypass this test's capture and corrupt the parent's results.
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let started = tokio::time::timeout(Duration::from_secs(10), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(child.try_wait().unwrap().is_none());
        kill_and_reap(&mut child);
        let _ = std::fs::remove_file(ready);
        started.expect("sleeping child must start before cleanup");
        let status = child.try_wait().unwrap().expect("child must be reaped");
        assert!(
            !status.success(),
            "child should be killed, not finish its sleep: {}",
            describe_exit_status(status)
        );
        // Cleanup is also safe after the process has already been reaped.
        kill_and_reap(&mut child);
    }

    /// Launch `sleeping_child` from this test binary. A separate function, because the
    /// ckdev guard tracks every value derived from a `current_exe()` binding as a build
    /// path, and the caller only writes out the child's pid.
    #[cfg(unix)]
    fn spawn_sleeping_child() -> std::process::Child {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "process::tests::sleeping_child", "--nocapture"])
            .env("BROCA_TEST_SUPPORT_SLEEP", "1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap()
    }

    /// A stand-in supervisor: it launches one long-running child, the way a daemon launches
    /// a module, publishes that child's pid, and waits for it.
    #[cfg(unix)]
    #[test]
    fn tree_supervisor_child() {
        let Some(pid_file) = std::env::var_os("BROCA_TEST_SUPPORT_TREE_SUPERVISOR") else {
            return;
        };
        let pid_file = std::path::PathBuf::from(pid_file);
        let mut module = spawn_sleeping_child();
        // Publish by rename so the test never reads a partly written pid.
        let staging = pid_file.with_extension("tmp");
        std::fs::write(&staging, module.id().to_string()).unwrap();
        std::fs::rename(&staging, &pid_file).unwrap();
        let _ = module.wait();
    }

    /// Killing only the supervisor leaves its child running for the rest of its 60 s sleep,
    /// the way a killed daemon leaves a module running that can still write into the rig's
    /// directory. Tree cleanup must have ended that child by the time it returns.
    #[cfg(unix)]
    #[tokio::test]
    async fn tree_cleanup_ends_the_supervisors_children_before_returning() {
        let pid_file = crate::scratch_root().join(format!(
            "tree-cleanup-module-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut supervisor = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process::tests::tree_supervisor_child",
                "--nocapture",
            ])
            .env("BROCA_TEST_SUPPORT_TREE_SUPERVISOR", &pid_file)
            // Nested libtest banners bypass this test's capture and corrupt the parent's results.
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let module: u32 = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(pid) = std::fs::read_to_string(&pid_file)
                    .ok()
                    .and_then(|pid| pid.trim().parse().ok())
                {
                    break pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("supervisor must launch its child");
        let _ = std::fs::remove_file(&pid_file);
        assert!(unix_tree::exists(module), "child {module} is not running");
        kill_and_reap_tree(&mut supervisor);
        let survived = unix_tree::exists(module);
        if survived {
            unix_tree::signal("KILL", &[module]);
        }
        assert!(
            !survived,
            "child {module} outlived its supervisor's cleanup"
        );
        assert!(supervisor.try_wait().unwrap().is_some());
    }

    #[test]
    fn rig_dir_removal_accepts_a_missing_directory() {
        remove_rig_dir(crate::scratch_root().join("rig-dir-that-was-never-created"));
    }

    /// A failed removal must fail the test that owns the rig, not vanish into a `let _`.
    #[test]
    fn rig_dir_removal_failure_is_reported() {
        // `remove_dir_all` refuses a regular file, even for root, which would ignore a
        // permission-based failure.
        let path = crate::scratch_root().join(format!(
            "rig-dir-removal-failure-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, b"not a directory").unwrap();
        let result = std::panic::catch_unwind(|| remove_rig_dir(&path));
        std::fs::remove_file(&path).unwrap();
        let message = result.expect_err("a failed removal must panic");
        let message = message
            .downcast_ref::<String>()
            .expect("panic carries the formatted message");
        assert!(message.contains("cannot remove"), "{message}");
    }

    #[tokio::test]
    async fn cleanup_keeps_nested_libtest_output_out_of_parent() {
        let output = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "process::tests::cleanup_kills_and_reaps_before_returning",
                    "--test-threads=1",
                ])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("bounded nested cleanup subprocess")
        .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert_eq!(stdout.matches("running 1 test").count(), 1, "{stdout}");
        assert!(
            stdout.contains("test process::tests::cleanup_kills_and_reaps_before_returning ... ok"),
            "{stdout}"
        );
        assert!(!stdout.contains("nested cleanup child"), "{stdout}");
        assert!(!stderr.contains("nested cleanup child"), "{stderr}");
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

    #[test]
    fn process_wait_classifies_signaled_process_as_exited() {
        assert_eq!(
            classify_process_wait(ProcessWaitOutcome::Exited),
            ProcessWaitClassification::Exited
        );
    }

    #[test]
    fn process_wait_classifies_open_failed_gone_process_as_exited() {
        assert_eq!(
            classify_process_wait(ProcessWaitOutcome::OpenFailedInvalidParameter),
            ProcessWaitClassification::Exited
        );
    }

    #[test]
    fn process_wait_classifies_timeout_as_still_alive() {
        assert_eq!(
            classify_process_wait(ProcessWaitOutcome::TimedOut),
            ProcessWaitClassification::StillAlive
        );
    }

    #[test]
    fn process_wait_preserves_unexpected_errors() {
        assert_eq!(
            classify_process_wait(ProcessWaitOutcome::Failed("access denied".to_owned())),
            ProcessWaitClassification::Failed("access denied".to_owned())
        );
    }

    #[cfg(windows)]
    #[test]
    fn process_wait_times_out_for_the_current_process() {
        let still_alive =
            broca_owner_only::test_support::process_wait_for_exit(std::process::id(), 0).unwrap();
        assert!(!still_alive);
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
