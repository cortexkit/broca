use std::process::ExitStatus;
use std::time::{Duration, Instant};

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
        return;
    }
    #[cfg(windows)]
    if let Some(pid) = child.id() {
        let result = std::process::Command::new("taskkill")
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
            Ok(Some(_)) => return,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            result => {
                eprintln!("test rig: child was not reaped within cleanup deadline: {result:?}");
                return;
            }
        }
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
