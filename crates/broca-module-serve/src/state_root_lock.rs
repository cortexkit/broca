//! One `ck-broca` process per state root.
//!
//! Boot recovery assumes it is alone on its state root: it rebuilds the fence index, reconciles
//! cuts, seals crash-orphaned WALs and releases System One batches whose owners died. Two
//! processes doing that at once race each other, and a reader in one can see the other's
//! half-rebuilt fence index and admit a send to a session that is mid-move. The daemon reaps an
//! old broca before it starts a new one, but overlap still happens: a restart whose boot sweep
//! could not verify a recorded child and left it running, descendants a kill did not reach, a
//! broca started by hand, and test rigs.
//!
//! So `ck-broca` takes an exclusive OS advisory lock on [`LOCK_FILE`] at the top of the state
//! root before anything else touches the root, and keeps it until the process exits. The lock
//! is the same kind the System One owner-lifetime lock and the WAL feature floor use
//! (`std::fs::File::try_lock`: `flock` on Unix, `LockFileEx` on Windows), so the kernel frees it
//! when the process dies, however it dies, and no stale lock can outlive its holder.
//!
//! The descriptor is never inherited by a child process: Rust opens every file close-on-exec on
//! Unix, and `broca_owner_only` creates Windows handles non-inheritable. A child that outlives
//! broca therefore cannot keep the root locked.
//!
//! A held lock is refused at once, never waited for: the daemon then counts a failed start and
//! applies its restart backoff, instead of a second process sitting blocked on a root another
//! one is serving.
//!
//! The command-line tools (store-maintenance, wal-fold, broca-session) do not take this lock.

use std::fmt;
use std::fs::{File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

/// The lock file at the top of the state root. It holds no data; only its OS lock matters.
pub const LOCK_FILE: &str = "broca.lock";

/// The held lock on one state root. Dropping it releases the lock.
#[derive(Debug)]
pub struct StateRootLock {
    file: File,
    path: PathBuf,
}

impl StateRootLock {
    /// The lock file this lock is held on.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Keep the lock until the process exits. The descriptor is deliberately leaked: the lock
    /// must outlast every task the runtime leaves behind when `main` returns, and the kernel
    /// releases it when the process ends.
    pub fn hold_until_exit(self) {
        std::mem::forget(self.file);
    }
}

/// Why the state root could not be locked.
#[derive(Debug)]
pub enum StateRootLockError {
    /// Another process holds the lock.
    Held { root: PathBuf, lock: PathBuf },
    /// The root or the lock file could not be created, opened or locked.
    Io {
        action: &'static str,
        path: PathBuf,
        error: io::Error,
    },
}

impl fmt::Display for StateRootLockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Held { root, lock } => write!(
                f,
                "refusing to start: another broca process holds the state root {} (its lock \
                 file {} is locked). Two processes must never recover or write one state root, \
                 so this one exits without touching it.",
                root.display(),
                lock.display()
            ),
            Self::Io {
                action,
                path,
                error,
            } => write!(
                f,
                "refusing to start: cannot {action} {}: {error}. Without the state-root lock \
                 another broca could be serving the same root.",
                path.display()
            ),
        }
    }
}

impl std::error::Error for StateRootLockError {}

/// Take the exclusive lock on `root`, creating the root when it does not exist yet.
///
/// When the root already exists and the lock is held elsewhere, this writes nothing: opening
/// an existing lock file neither truncates nor modifies it.
pub fn acquire(root: &Path) -> Result<StateRootLock, StateRootLockError> {
    create_root_if_missing(root)?;
    let path = root.join(LOCK_FILE);
    let file = broca_owner_only::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|error| StateRootLockError::Io {
            action: "open the state-root lock",
            path: path.clone(),
            error,
        })?;
    match file.try_lock() {
        Ok(()) => Ok(StateRootLock { file, path }),
        Err(TryLockError::WouldBlock) => Err(StateRootLockError::Held {
            root: root.to_path_buf(),
            lock: path,
        }),
        Err(TryLockError::Error(error)) => Err(StateRootLockError::Io {
            action: "lock the state-root lock",
            path,
            error,
        }),
    }
}

/// Create a missing root owner-only and, on Unix, make the new directory durable by syncing its
/// parent (directory sync is a no-op elsewhere).
///
/// The state-format marker rules depend on that sync: they write the marker into a root that
/// already exists, and they skip the parent sync for an existing root, so the sync must happen
/// here when this function is the one that created it.
fn create_root_if_missing(root: &Path) -> Result<(), StateRootLockError> {
    let io_error = |action: &'static str, path: &Path| {
        let path = path.to_path_buf();
        move |error| StateRootLockError::Io {
            action,
            path,
            error,
        }
    };
    if root
        .try_exists()
        .map_err(io_error("check the state root", root))?
    {
        return Ok(());
    }
    broca_owner_only::create_dir_all(root).map_err(io_error("create the state root", root))?;
    let parent = match root.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    sync_dir(parent).map_err(io_error("sync the state root's parent directory", parent))
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    // Directory handles can be synced on Unix only, the same choice the state-format marker
    // and the WAL directory sync make.
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Set in a re-executed copy of this test binary to make it sleep, standing in for a
    /// long-lived child process spawned while the lock is held.
    const CHILD_SLEEP_ENV: &str = "BROCA_STATE_ROOT_LOCK_TEST_CHILD_SLEEP";

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let dir = broca_test_support::scratch_root().join(format!(
                "broca-root-lock-{tag}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create scratch dir");
            Self(dir)
        }

        fn root(&self) -> PathBuf {
            self.0.join("root")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn a_held_root_is_refused_by_name_and_freed_on_drop() {
        let scratch = Scratch::new("held");
        let root = scratch.root();
        let first = acquire(&root).expect("a missing root is created and locked");
        assert_eq!(first.path(), root.join(LOCK_FILE));
        let error = acquire(&root).expect_err("a second lock on the same root is refused");
        assert!(
            matches!(&error, StateRootLockError::Held { root: r, .. } if *r == root),
            "{error:?}"
        );
        let message = error.to_string();
        assert!(message.contains("another broca process holds the state root"));
        assert!(message.contains(&root.display().to_string()), "{message}");
        drop(first);
        acquire(&root).expect("dropping the holder frees the root");
    }

    /// Helper run only inside the child the next test spawns: it sleeps so the child is still
    /// alive while the parent checks the lock. Run normally, it returns at once.
    #[test]
    fn child_process_that_sleeps() {
        if std::env::var_os(CHILD_SLEEP_ENV).is_some() {
            std::thread::sleep(std::time::Duration::from_secs(60));
        }
    }

    /// A child spawned while the lock is held must not inherit the lock's descriptor. If it
    /// did, the child would keep the lock after the parent released it, and a fresh broca
    /// would be refused for as long as that child lived.
    #[test]
    fn a_spawned_child_does_not_inherit_the_lock() {
        let scratch = Scratch::new("inherit");
        let root = scratch.root();
        let lock = acquire(&root).expect("lock the root");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "state_root_lock::tests::child_process_that_sleeps",
                "--test-threads=1",
            ])
            .env(CHILD_SLEEP_ENV, "1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn the sleeping child");
        drop(lock);
        let reacquired = acquire(&root);
        let child_alive = child.try_wait().expect("poll the child").is_none();
        let _ = child.kill();
        let _ = child.wait();
        assert!(
            child_alive,
            "the child exited before the lock was checked, so the check proves nothing"
        );
        reacquired.expect("the child holds no copy of the lock descriptor");
    }
}
