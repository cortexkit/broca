use std::ffi::OsStr;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Build a long-lived rig child with no connection to the test runner's stdio.
///
/// Daemons pass their standard handles to supervised modules. Giving the daemon
/// rig-owned files keeps those descendants from holding nextest's output open,
/// while retaining their output for diagnostics.
pub fn rig_command(
    program: impl AsRef<OsStr>,
    scratch_dir: impl AsRef<Path>,
    name: &str,
) -> io::Result<Command> {
    let scratch_dir = scratch_dir.as_ref();
    let (stdout, stderr) = create_log_files(scratch_dir, name)?;
    let mut command = Command::new(program);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    Ok(command)
}

/// Tokio counterpart to [`rig_command`] for asynchronous test rigs.
pub fn tokio_rig_command(
    program: impl AsRef<OsStr>,
    scratch_dir: impl AsRef<Path>,
    name: &str,
) -> io::Result<tokio::process::Command> {
    let scratch_dir = scratch_dir.as_ref();
    let (stdout, stderr) = create_log_files(scratch_dir, name)?;
    let mut command = tokio::process::Command::new(program);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    Ok(command)
}

/// Format the retained stdout and stderr files for an explicit failure diagnostic.
pub fn rig_process_logs(scratch_dir: impl AsRef<Path>, name: &str) -> String {
    let scratch_dir = scratch_dir.as_ref();
    let mut output = String::new();
    super::append_file_tail(
        &mut output,
        &format!("{name} stdout"),
        &rig_log_path(scratch_dir, name, "stdout"),
    );
    super::append_file_tail(
        &mut output,
        &format!("{name} stderr"),
        &rig_log_path(scratch_dir, name, "stderr"),
    );
    output
}

/// Print a rig child's output while unwinding, after the caller has reaped it.
pub fn dump_rig_process_logs_on_failure(scratch_dir: impl AsRef<Path>, name: &str) {
    if std::thread::panicking() {
        eprintln!("{}", rig_process_logs(scratch_dir, name));
    }
}

/// Own a long-lived Tokio rig child and print its logs if the test unwinds.
pub struct RigChild {
    child: tokio::process::Child,
    scratch_dir: PathBuf,
    name: String,
}

impl RigChild {
    pub fn new(child: tokio::process::Child, scratch_dir: impl AsRef<Path>, name: &str) -> Self {
        Self {
            child,
            scratch_dir: scratch_dir.as_ref().to_path_buf(),
            name: name.to_owned(),
        }
    }
}

impl Deref for RigChild {
    type Target = tokio::process::Child;

    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

impl DerefMut for RigChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

impl Drop for RigChild {
    fn drop(&mut self) {
        super::kill_and_reap(&mut self.child);
        dump_rig_process_logs_on_failure(&self.scratch_dir, &self.name);
    }
}

fn rig_log_path(scratch_dir: &Path, name: &str, stream: &str) -> PathBuf {
    scratch_dir.join(format!("{name}.{stream}.log"))
}

fn create_log_files(scratch_dir: &Path, name: &str) -> io::Result<(std::fs::File, std::fs::File)> {
    std::fs::create_dir_all(scratch_dir)?;
    let stdout = std::fs::File::create(rig_log_path(scratch_dir, name, "stdout"))?;
    let stderr = std::fs::File::create(rig_log_path(scratch_dir, name, "stderr"))?;
    Ok((stdout, stderr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[cfg(unix)]
    #[tokio::test]
    async fn rig_child_stdout_is_not_the_parents_stdout() {
        use std::os::unix::fs::MetadataExt;

        let parent = std::fs::metadata("/dev/fd/1").expect("parent stdout metadata");
        let root = std::env::temp_dir().join(format!(
            "broca-rig-stdio-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        let mut child = tokio_rig_command(std::env::current_exe().unwrap(), &root, "fd-check")
            .unwrap()
            .args([
                "--exact",
                "rig_stdio::tests::report_child_stdout_identity",
                "--nocapture",
            ])
            .env("BROCA_RIG_STDOUT_IDENTITY", "1")
            .spawn()
            .expect("spawn child through rig helper");
        let status = child.wait().await.expect("wait for child");
        assert!(status.success(), "child test failed: {status}");
        let logs = rig_process_logs(&root, "fd-check");
        let identity = logs
            .lines()
            .find_map(|line| line.split_once("CHILD_STDOUT_ID=").map(|(_, rest)| rest))
            .unwrap_or_else(|| panic!("child did not report its stdout identity:\n{logs}"));
        let mut fields = identity.split_whitespace();
        let child_dev = fields.next().unwrap().parse::<u64>().unwrap();
        let child_ino = fields.next().unwrap().parse::<u64>().unwrap();
        assert_ne!(
            (child_dev, child_ino),
            (parent.dev(), parent.ino()),
            "the rig child inherited the test process's stdout"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn report_child_stdout_identity() {
        if std::env::var_os("BROCA_RIG_STDOUT_IDENTITY").is_some() {
            use std::os::unix::fs::MetadataExt;
            let stdout = std::fs::metadata("/dev/fd/1").expect("child stdout metadata");
            println!("CHILD_STDOUT_ID={} {}", stdout.dev(), stdout.ino());
        }
    }

    #[tokio::test]
    async fn rig_child_logs_are_printed_when_the_test_unwinds() {
        const CHILD_ENV: &str = "BROCA_RIG_LOG_DUMP_CHILD";
        if let Some(root) = std::env::var_os(CHILD_ENV) {
            let root = PathBuf::from(root);
            let child = tokio_rig_command(std::env::current_exe().unwrap(), &root, "daemon")
                .unwrap()
                .args([
                    "--exact",
                    "rig_stdio::tests::emit_rig_log_markers",
                    "--nocapture",
                ])
                .env("BROCA_RIG_LOG_MARKERS", "1")
                .spawn()
                .expect("spawn the log-producing child through the rig helper");
            let mut child = RigChild::new(child, &root, "daemon");
            child
                .wait()
                .await
                .expect("wait for the log-producing child");
            panic!("intentional failure to verify rig logs are printed");
        }

        let root = std::env::temp_dir().join(format!(
            "broca-rig-log-dump-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "rig_stdio::tests::rig_child_logs_are_printed_when_the_test_unwinds",
                "--nocapture",
            ])
            .env(CHILD_ENV, &root)
            .output()
            .expect("run the intentional failure in a child test process");
        assert!(
            !output.status.success(),
            "the child test must fail deliberately"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("RIG_STDOUT_MARKER"),
            "daemon stdout should be included in failure diagnostics: {stderr}"
        );
        assert!(
            stderr.contains("RIG_STDERR_MARKER"),
            "daemon stderr should be included in failure diagnostics: {stderr}"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn emit_rig_log_markers() {
        if std::env::var_os("BROCA_RIG_LOG_MARKERS").is_some() {
            println!("RIG_STDOUT_MARKER");
            eprintln!("RIG_STDERR_MARKER");
        }
    }
}
