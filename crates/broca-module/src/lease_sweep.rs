//! A one-time pass that removes the lease files of sessions that no longer exist.
//!
//! Every session lease leaves a file in the state root's `leases` folder, and until broca's
//! session leases removed their file on release once the session was gone (see
//! `broca_core::session_leases`), every deleted session and every address an open found empty
//! left one behind: 390,977 files for about 189,000 stored sessions on one computer in November
//! 2026. A file is named by a hash of its lease key, so most of those can't be named back to a
//! session, and a lease that can't be named can't be acquired to remove it. This pass works from
//! the other side: it collects the file of every session the store still holds, and removes the
//! others through `remove`, which removes a file only when it can take the file's lock, as a
//! lease's own removal does.
//!
//! A lease taken while the pass runs is never removed: taking a lease writes its file, so a file
//! changed since the pass began is skipped, and one held now can't be locked. Every session the
//! pass keeps is one with a head when it lists them, after it began; a session created before
//! the pass began wrote its head under its lease, before letting go of it.

use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::time::SystemTime;

use broca_protocol::BindIdentity;
use broca_records::session_storage::StorageClass;
use broca_session_store::backend::LocalBackend;
use broca_session_store::head;

/// The state-root file that records the pass ran to its end, so later starts skip it.
pub const SWEPT: &str = "leases-swept";

/// What one pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Swept {
    /// Lease files of sessions the store holds, kept.
    pub kept: usize,
    /// Lease files removed.
    pub removed: usize,
    /// Lease files changed since the pass began, or held, missing or replaced when removed.
    pub skipped: usize,
}

/// How many heads or lease files the pass reads between two checks of `stopping`.
const STOP_CHECK_EVERY: usize = 1_000;

/// Removes, through `remove`, every lease file under `state_root` that names no session the
/// session store holds and is unchanged since `began`, then records the pass as done. `remove`
/// gets a file's name in the leases folder and answers whether it removed it. `None` when an
/// earlier pass already ran to its end. A head that can't be read stops the pass before it
/// removes anything: without it the pass can't tell which files are still a session's. Once
/// `stopping` answers true the pass stops where it is, unrecorded, and the next start runs it
/// again.
pub fn sweep_once(
    state_root: &Path,
    began: SystemTime,
    remove: impl Fn(&str) -> io::Result<bool>,
    stopping: impl Fn() -> bool,
) -> io::Result<Option<Swept>> {
    let swept = state_root.join(SWEPT);
    if swept.try_exists()? {
        return Ok(None);
    }
    let live = live_lease_files(state_root, &stopping)?;
    let mut outcome = Swept::default();
    let leases = state_root.join("leases");
    let entries = match std::fs::read_dir(&leases) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            mark_swept(state_root)?;
            return Ok(Some(outcome));
        }
        Err(error) => return Err(error),
    };
    for (read, entry) in entries.enumerate() {
        if read % STOP_CHECK_EVERY == 0 && stopping() {
            return Err(stopped());
        }
        let entry = entry?;
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if !name.ends_with(".lease") {
            continue;
        }
        if live.contains(&name) {
            outcome.kept += 1;
            continue;
        }
        let unchanged = match entry.metadata().and_then(|meta| meta.modified()) {
            Ok(modified) => modified < began,
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(error),
        };
        if unchanged && remove(&name)? {
            outcome.removed += 1;
        } else {
            outcome.skipped += 1;
        }
    }
    mark_swept(state_root)?;
    Ok(Some(outcome))
}

/// Records that the pass ran to its end, durably, so a crash after it doesn't run it again.
fn mark_swept(state_root: &Path) -> io::Result<()> {
    broca_owner_only::OpenOptions::new()
        .write(true)
        .create(true)
        .open(state_root.join(SWEPT))?
        .sync_all()?;
    broca_records::move_fence::sync_dir(state_root)
}

/// The lease file of every session the session store holds a head for in any of its roots,
/// including one whose deletion a crash cut short: its logs are still there.
fn live_lease_files(
    state_root: &Path,
    stopping: &impl Fn() -> bool,
) -> io::Result<HashSet<String>> {
    let mut live = HashSet::new();
    for class in StorageClass::ALL {
        let backend = LocalBackend::new(class.store_root(state_root));
        let (sessions, deletions) = backend.stored_sessions_and_deletions().map_err(described)?;
        let heads = sessions
            .iter()
            .map(|addr| {
                backend
                    .read_stored_head(addr)
                    .map(|head| head.map(|(bytes, _)| bytes))
            })
            .chain(deletions.iter().map(|addr| backend.deleted_head(addr)));
        for (read, head) in heads.enumerate() {
            if read % STOP_CHECK_EVERY == 0 && stopping() {
                return Err(stopped());
            }
            // A head removed since it was listed is a session that's gone.
            let Some(bytes) = head.map_err(described)? else {
                continue;
            };
            let triple = head::stored_triple(&bytes).map_err(described)?;
            live.insert(broca_core::session_lease_file(&BindIdentity::new(
                triple.project_root,
                triple.harness,
                triple.session,
            )));
        }
    }
    Ok(live)
}

/// The error a pass stopped by the module's stop answers.
fn stopped() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "the module is stopping")
}

/// A store error as an I/O error the pass reports.
fn described(error: impl std::fmt::Debug) -> io::Error {
    io::Error::other(format!("{error:?}"))
}

#[cfg(test)]
#[path = "lease_sweep_tests.rs"]
mod tests;
