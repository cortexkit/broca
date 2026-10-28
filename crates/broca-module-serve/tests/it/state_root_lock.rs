//! One `ck-broca` per state root, checked against the real binary.
//!
//! A second process on a locked root must exit at once, non-zero, with one ERROR line naming
//! the root, and without writing anything to it. A killed holder must free the root for the
//! next boot. The lock's own unit tests (`state_root_lock`) cover the descriptor not leaking
//! into a spawned child.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use broca_module_serve::state_root_lock::{self, StateRootLockError, LOCK_FILE};

const HELD: &str = "another broca process holds the state root";

struct Machine(PathBuf);

impl Machine {
    fn new(tag: &str) -> Self {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-root-lock-boot-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        for dir in ["runtime", "data", "config", "cwd"] {
            std::fs::create_dir_all(root.join(dir)).expect("create scratch layout");
        }
        Self(root)
    }

    fn state_root(&self) -> PathBuf {
        self.0.join("state")
    }

    fn connection_file(&self) -> PathBuf {
        self.0.join("runtime").join("subc-connection.json")
    }

    /// `ck-broca` on this machine's state root. The data home, where the logger writes, is
    /// outside the state root, so anything the process writes to the root is boot work.
    fn command(&self, label: &str) -> std::process::Command {
        let mut command = broca_test_support::ckdev_command(
            env!("CARGO_BIN_EXE_ck-broca"),
            broca_test_support::scratch_root(),
        );
        command
            .arg("--subc")
            .arg(self.connection_file())
            .env("BROCA_STATE_ROOT", self.state_root())
            .env("XDG_DATA_HOME", self.0.join("data"))
            .env("XDG_CONFIG_HOME", self.0.join("config"))
            .env("HOME", self.0.join("home"))
            .env_remove("BROCA_CONFIG_PATH")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env("SUBC_MODULE_ID", "broca")
            .current_dir(self.0.join("cwd"))
            .stdin(Stdio::null())
            .stdout(std::fs::File::create(self.0.join(format!("{label}.stdout.log"))).unwrap())
            .stderr(std::fs::File::create(self.0.join(format!("{label}.stderr.log"))).unwrap());
        command
    }

    fn stderr(&self, label: &str) -> String {
        // `main` returns the error and the runtime prints it with `Debug`, which doubles each
        // backslash; undo that so Windows paths compare.
        std::fs::read_to_string(self.0.join(format!("{label}.stderr.log")))
            .unwrap_or_default()
            .replace("\\\\", "\\")
    }

    /// Run `ck-broca` to completion.
    fn run(&self, label: &str) -> Output {
        broca_test_support::retry_executable_file_busy(|| self.command(label).output())
            .expect("run ck-broca")
    }

    fn spawn(&self, label: &str) -> Reap {
        Reap(
            broca_test_support::retry_executable_file_busy(|| self.command(label).spawn())
                .expect("spawn ck-broca"),
        )
    }

    /// Every log line the processes on this machine wrote that reports the held lock.
    fn held_log_lines(&self) -> Vec<String> {
        let logs = self.0.join("data/cortexkit/broca/logs");
        let mut lines = Vec::new();
        for entry in std::fs::read_dir(&logs).into_iter().flatten().flatten() {
            let text = std::fs::read_to_string(entry.path()).unwrap_or_default();
            lines.extend(
                text.lines()
                    .filter(|line| line.contains(HELD))
                    .map(str::to_owned),
            );
        }
        lines
    }
}

impl Drop for Machine {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Kills the child if a failed assertion leaves it running.
struct Reap(Child);

impl Drop for Reap {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Every entry under `root` with its length, modification time and, for files, its bytes.
///
/// The lock file's bytes are not read: this test holds its lock, and Windows refuses to read
/// a byte range another handle has locked. Its length and modification time are still
/// compared, which shows a second broca did not rewrite it.
fn tree(root: &Path) -> BTreeMap<PathBuf, (u64, std::time::SystemTime, Option<Vec<u8>>)> {
    let mut out = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).expect("list the state root") {
            let path = entry.expect("read an entry").path();
            let metadata = std::fs::symlink_metadata(&path).expect("stat an entry");
            let bytes = if metadata.is_dir() {
                pending.push(path.clone());
                None
            } else if path == root.join(LOCK_FILE) {
                None
            } else {
                Some(std::fs::read(&path).expect("read a file"))
            };
            out.insert(
                path.strip_prefix(root).unwrap().to_path_buf(),
                (metadata.len(), metadata.modified().unwrap(), bytes),
            );
        }
    }
    out
}

/// A second `ck-broca` on a root another process holds refuses before any boot work: it
/// exits non-zero with one ERROR line naming the root, and the root is byte-for-byte as it
/// was. The holder is this test process, through the same function `ck-broca` locks with,
/// so nothing else writes to the root while it is compared.
#[test]
fn a_second_broca_on_a_held_root_exits_at_once_and_writes_nothing() {
    let machine = Machine::new("second");
    let root = machine.state_root();
    // A root that has served before: the state a racing boot recovery would touch.
    std::fs::create_dir_all(root.join("wal")).unwrap();
    std::fs::write(root.join("run-index.db"), b"index").unwrap();
    let holder = state_root_lock::acquire(&root).expect("lock the root as a running broca");
    let before = tree(&root);

    let output = machine.run("second");

    let stderr = machine.stderr("second");
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains(HELD), "{stderr}");
    assert!(
        stderr.contains(&root.display().to_string()),
        "the refusal names the root: {stderr}"
    );
    assert!(
        !stderr.contains("tool-plane consumer connect"),
        "the refused process went on to connect: {stderr}"
    );
    assert_eq!(
        tree(&root),
        before,
        "the refused process wrote to the state root"
    );
    let lines = machine.held_log_lines();
    assert_eq!(lines.len(), 1, "exactly one log line reports it: {lines:?}");
    assert!(lines[0].contains(" ERROR "), "{lines:?}");
    drop(holder);
}

/// The root lock dies with its holder, even under SIGKILL (TerminateProcess on Windows): the
/// kernel releases it, and the next boot gets past it. While the real holder runs, a second
/// real `ck-broca` is refused.
#[tokio::test]
async fn a_killed_holder_frees_the_root_for_the_next_boot() {
    let machine = Machine::new("killed");
    let root = machine.state_root();
    // A daemon stand-in that authenticates every connection and then never answers. A booting
    // broca connects only after it holds the root lock, so each authenticated connection is a
    // process past the lock; the silence keeps it waiting there rather than exiting.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let key = vec![0x52; 32];
    let daemon_id = [0x25; 16];
    subc_transport::connection_file::write_atomic(
        machine.connection_file(),
        &subc_transport::connection_file::ConnectionInfo {
            schema: subc_transport::connection_file::SCHEMA_VERSION,
            wire_version: None,
            endpoints: vec![subc_transport::connection_file::Endpoint {
                host: "127.0.0.1".into(),
                port: listener.local_addr().unwrap().port(),
            }],
            key: key.clone(),
            daemon_id,
            pid: std::process::id(),
            daemon_ver: "root-lock".into(),
        },
    )
    .unwrap();
    let authenticated = Arc::new(AtomicUsize::new(0));
    let server = {
        let authenticated = authenticated.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let key = key.clone();
                let authenticated = authenticated.clone();
                tokio::spawn(async move {
                    if subc_transport::authenticate_server(
                        &mut stream,
                        &key,
                        &daemon_id,
                        "root-lock",
                        broca_test_support::STAND_IN_AUTH_DEADLINE,
                    )
                    .await
                    .is_ok()
                    {
                        authenticated.fetch_add(1, Ordering::SeqCst);
                        while let Ok(Some(_)) = subc_transport::read_frame(&mut stream).await {}
                    }
                });
            }
        })
    };
    let past_the_lock = |at_least: usize| {
        let authenticated = authenticated.clone();
        async move {
            let seen = authenticated.load(Ordering::SeqCst);
            if seen >= at_least {
                Ok(())
            } else {
                Err(format!("{seen} authenticated connections"))
            }
        }
    };

    let mut first = machine.spawn("first");
    broca_test_support::wait_until("the first broca past the root lock", || past_the_lock(1)).await;
    assert!(
        first.0.try_wait().unwrap().is_none(),
        "the first broca exited: {}",
        machine.stderr("first")
    );
    assert!(matches!(
        state_root_lock::acquire(&root),
        Err(StateRootLockError::Held { .. })
    ));

    // A real second broca while the real first one serves.
    let second = machine.run("second");
    let stderr = machine.stderr("second");
    assert!(!second.status.success(), "{stderr}");
    assert!(stderr.contains(HELD), "{stderr}");

    // SIGKILL on Unix, TerminateProcess on Windows: no destructor or exit handler runs.
    first.0.kill().unwrap();
    first.0.wait().unwrap();
    drop(
        state_root_lock::acquire(&root)
            .expect("the kernel released the killed holder's lock on the root"),
    );

    // Let the stand-in finish counting any handshake the killed process completed before it
    // died, so the count below moves only for the next process.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let seen = authenticated.load(Ordering::SeqCst);
    let mut next = machine.spawn("next");
    broca_test_support::wait_until("the next broca past the root lock", || {
        past_the_lock(seen + 1)
    })
    .await;
    assert!(
        next.0.try_wait().unwrap().is_none(),
        "the next broca exited: {}",
        machine.stderr("next")
    );
    assert!(
        !machine.stderr("next").contains(HELD),
        "{}",
        machine.stderr("next")
    );
    assert!(matches!(
        state_root_lock::acquire(&root),
        Err(StateRootLockError::Held { .. })
    ));
    assert!(root.join(LOCK_FILE).is_file());
    next.0.kill().unwrap();
    next.0.wait().unwrap();
    server.abort();
}
