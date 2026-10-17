//! Test-only support for the broca workspace.
//!
//! Every test in this workspace that needs scratch space on disk takes it from
//! [`scratch_root`] instead of `std::env::temp_dir()`. The test still composes its own
//! unique name (label, pid, nonce) exactly as before; only the parent directory moves
//! from `$TMPDIR` to `$TMPDIR/broca-tests/<pid>/`.
//!
//! # Why one owned root
//!
//! `$TMPDIR` is shared with every other project on the machine. Before this crate each
//! test left one uniquely named entry at the top of it and nothing ever removed them, so
//! the litter grew by one entry per test run and was paid for by whoever listed the
//! directory. Gathering them under one root makes broca's scratch invisible to those
//! listings -- and therefore also invisible to any machine-wide janitor. Consolidating
//! means owning reclamation: the age sweep in [`scratch_root`] is that reclamation, and
//! it is the only one.
//!
//! # Why the pid stays one level down
//!
//! The per-process directory is named by the test process's pid. A cleanup tool (or a
//! person) looking inside `broca-tests/` can still tell which process a scratch tree
//! belonged to, so a live run's scratch stays distinguishable from an orphan's.
//!
//! # Why there is no teardown removal
//!
//! Nothing removes a process's own scratch when it exits. A failing test's scratch is the
//! evidence you read to find out why it failed, so it is left in place and reclaimed only
//! once it has aged past [`SWEEP_AGE`].
#![forbid(unsafe_code)]

mod ckdev;
mod process;
mod rig_logs;
mod rig_stdio;
mod tracing_capture;
mod wait;
pub use ckdev::{
    ckdev_binary, ckdev_binary_pair, ckdev_binary_with_companion, ckdev_command,
    retry_executable_file_busy,
};
pub use process::{kill_and_reap, kill_and_reap_tree, remove_rig_dir};
pub use rig_logs::{append_file_tail, rig_log_diagnostics};
pub use rig_stdio::{
    dump_rig_process_logs_on_failure, rig_command, rig_process_logs, tokio_rig_command, RigChild,
};
pub use tracing_capture::{capture_logs, LogCapture, LogEvent};
pub use wait::{
    event_timeout, record_kinds, wait_for, wait_until, wait_until_sync, EVENT_HANG_GUARD,
};

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

/// Name of the directory under `$TMPDIR` that holds every broca test process's scratch.
pub const ROOT_DIR_NAME: &str = "broca-tests";

/// Stand-ins test the client's requests, not handshake timing; allow scheduler delays.
/// The real daemon allows 2 s, so handshake-timing tests must use the real daemon.
pub const STAND_IN_AUTH_DEADLINE: Duration = Duration::from_secs(30);

/// Resolve a real subc test binary.
///
/// CI builds `subc-core` binaries in the sibling `subconscious/target/debug` directory without overrides.
pub fn subc_bin(name: &str, env_key: &str) -> PathBuf {
    std::env::var_os(env_key)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../subconscious/target/debug")
                .join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
        })
}

/// How old a sibling process's scratch directory must be before the sweep removes it.
///
/// The sweep judges by AGE, not by whether the owning pid is still alive. Under nextest
/// many test binaries run at once, each with its own pid directory, and a liveness check
/// would need either `unsafe` FFI or spawning `ps`, with platform-specific answers either
/// way. Age needs neither, and it still reclaims the case that actually leaves orphans: a
/// test run that was killed before it finished.
///
/// The threshold has to be far longer than any real test process lives, because a sweep
/// that reaches a running binary's scratch deletes files out from under its tests and
/// shows up as an unrelated flaky failure. A full gate run takes minutes to well under an
/// hour even on a loaded machine; twelve hours leaves more than an order of magnitude of
/// margin while still bounding the litter to about half a day of runs.
pub const SWEEP_AGE: Duration = Duration::from_secs(12 * 60 * 60);

/// The scratch root for this test process. `$TMPDIR/broca-tests/<pid>/`, created on first use.
///
/// The first call in a process also sweeps sibling `<pid>` directories under
/// `broca-tests/` whose modification time is older than [`SWEEP_AGE`]. Sweep failures are
/// ignored: another process may remove the same directory between the listing and the
/// removal, and a test must never fail because a concurrent sweep got there first.
///
/// # Panics
///
/// Panics if the root directory cannot be created, since no test that asked for scratch
/// space could proceed without it.
/// The directory name for one test process's scratch root: the pid, then a creation nonce.
///
/// THE NONCE EXISTS BECAUSE A PID ALONE IS REUSED, and the reuse is not theoretical here --
/// this box issues pids across the whole range the live root already holds. With a bare pid
/// name, a new process whose pid matches a dead one's directory would ADOPT it through
/// `create_dir_all`, which succeeds on an existing directory AND LEAVES ITS MTIME ALONE
/// (measured: a directory aged 50 hours keeps that mtime through `mkdir -p`). The live
/// process would then own a directory that reads as aged, and a concurrent sibling's sweep
/// would remove a RUNNING process's scratch -- surfacing as an unrelated test flaking
/// somewhere else entirely.
///
/// Composed in one place because the sweep's filter and the tests must plant the same shape.
/// A format change that moved only one of them would leave the sweep matching nothing: the
/// arms would still pass, and reclamation would silently stop.
/// `pub` so the sweep's own integration arms PLANT THROUGH THE COMPOSER rather than carrying a
/// second copy of the format. A fixture that spells the name itself drifts silently: the plant
/// stops matching the predicate, the sweep skips it, and the arm then passes for the wrong
/// reason -- it would be asserting that nothing removed a directory the sweep never considered.
pub fn scratch_dir_name(pid: u32, nonce: u128) -> String {
    format!("{pid}-{nonce:032x}")
}

/// Whether `name` is a scratch directory this crate created.
///
/// Deliberately strict: anything else under the root belongs to someone else and the sweep
/// must not remove it. Pinned against `scratch_dir_name` AND against the live root's own
/// name, so the predicate cannot drift from what is actually created.
fn is_scratch_dir_name(name: &str) -> bool {
    match name.split_once('-') {
        Some((pid, nonce)) => {
            !pid.is_empty()
                && pid.bytes().all(|b| b.is_ascii_digit())
                && nonce.len() == 32
                && nonce.bytes().all(|b| b.is_ascii_hexdigit())
        }
        None => false,
    }
}

pub fn scratch_root() -> PathBuf {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        let parent = std::env::temp_dir().join(ROOT_DIR_NAME);
        #[cfg(windows)]
        let create_parent = broca_owner_only::create_dir_all(&parent)
            .and_then(|()| broca_owner_only::validate_inheritable_directory(&parent));
        #[cfg(not(windows))]
        let create_parent = std::fs::create_dir_all(&parent);
        create_parent.unwrap_or_else(|error| {
            panic!(
                "creating the test scratch parent {} failed: {error}",
                parent.display()
            )
        });
        let nonce = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or(0);
        let own_name = scratch_dir_name(std::process::id(), nonce);
        let root = parent.join(&own_name);
        // `create_dir`, NOT `create_dir_all`, for the leaf: adopting an existing directory is
        // the defect this name shape exists to prevent, so the call that could adopt one is
        // the wrong call. With the nonce a collision is effectively impossible, and if it
        // happened it would be a real fault rather than something to paper over.
        #[cfg(windows)]
        let create_root = broca_owner_only::create_dir(&root);
        #[cfg(not(windows))]
        let create_root = std::fs::create_dir(&root);
        create_root.unwrap_or_else(|error| {
            panic!(
                "creating the test scratch root {} failed: {error}",
                root.display()
            )
        });
        sweep_aged_siblings(&parent, &own_name, SystemTime::now(), SWEEP_AGE);
        root
    })
    .clone()
}

/// Removes every numerically named directory under `parent`, other than `own_name`, whose
/// modification time is at least `max_age` before `now`. Entries that are not directories,
/// whose names are not pids, or whose age cannot be read are left alone; every error is
/// ignored.
fn sweep_aged_siblings(parent: &Path, own_name: &str, now: SystemTime, max_age: Duration) {
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name == own_name || !is_scratch_dir_name(name) {
            continue;
        }
        // `symlink_metadata` so a symlink planted here is judged as itself and never
        // followed into whatever it points at.
        let Ok(metadata) = entry.path().symlink_metadata() else {
            continue;
        };
        if !metadata.is_dir() {
            continue;
        }
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        // A modification time in the future (clock skew) yields an error here and the
        // directory is kept, which is the safe direction.
        let aged = now.duration_since(modified).is_ok_and(|age| age >= max_age);
        if aged {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

/// List state-tree entries whose Unix permissions grant access to group or other users.
#[cfg(unix)]
pub fn public_tree_entries(root: &std::path::Path) -> std::io::Result<Vec<String>> {
    let mut pending = vec![root.to_path_buf()];
    let mut found = Vec::new();
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path)?;
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                found.push(path.strip_prefix(root).unwrap().display().to_string());
            }
            if metadata.is_dir() {
                pending.push(path);
            }
        }
    }
    found.sort();
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE PREDICATE MUST ACCEPT WHAT IS ACTUALLY CREATED, not merely what the composer emits.
    ///
    /// This is the arm that stops the dangerous half of a rename. The sweep skips any name
    /// `is_scratch_dir_name` rejects, so a format change applied to the composer and not to
    /// the predicate turns the sweep into a silent no-op -- and BOTH existing arms keep
    /// passing, because they plant their siblings through the composer too. Reading the live
    /// root's own directory name is what ties the predicate to the creation path.
    #[test]
    fn the_predicate_accepts_the_name_the_live_root_actually_has() {
        let root = scratch_root();
        let name = root.file_name().unwrap().to_str().unwrap();
        assert!(
            is_scratch_dir_name(name),
            "the sweep would skip this process's own directory shape: {name}"
        );
        assert!(name.starts_with(&format!("{}-", std::process::id())));
    }

    /// The predicate must REFUSE a bare pid, which is the pre-nonce shape.
    ///
    /// Not hypothetical housekeeping: directories in that shape exist on this box right now,
    /// left by the version this fix replaces. They are other people's data as far as the new
    /// sweep is concerned, and removing them is not its job.
    #[test]
    fn the_predicate_refuses_shapes_this_crate_did_not_create() {
        for wrong in [
            "5000",
            "",
            "-",
            "5000-",
            "-abc",
            "5000-xyz",
            "5000-abc",
            "broca-tests",
        ] {
            assert!(
                !is_scratch_dir_name(wrong),
                "{wrong:?} is not a name this crate composes, so the sweep must not touch it"
            );
        }
        assert!(is_scratch_dir_name(&scratch_dir_name(1, 2)));
    }

    #[test]
    fn scratch_root_is_a_per_process_directory_under_the_owned_root() {
        let root = scratch_root();
        assert!(root.is_dir());
        assert_eq!(
            root.parent().unwrap(),
            std::env::temp_dir().join(ROOT_DIR_NAME),
            "scratch must live under the one owned root, not loose in $TMPDIR"
        );
        // The name is checked by shape rather than by equality: it carries a creation nonce,
        // so an equality assertion would have to recompute the nonce and would only be
        // asserting its own arithmetic.
        assert_eq!(scratch_root(), root, "the root is stable within a process");
    }
}
