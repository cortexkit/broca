//! The sessions outside the ephemeral root that retention has to read.
//!
//! Each retention pass reads every session in the ephemeral root, where every session the older
//! `retention` seconds keep is stored. Under llm-runner `session_retention` a session in the
//! durable or ordinary root can come due too: one kept for days and backed up, one whose
//! assigned policy its owner replaced with days, and any session its owner released. Those roots
//! hold far more sessions than any pass could read, so the actor marks such a session here, with
//! an empty file named for it, before the batch that gives it a deadline. A pass reads only the
//! marked sessions of those roots.
//!
//! A mark without a deadline behind it (a crash between the mark and its batch, or a session
//! deleted since) costs one read and is removed by the pass that finds it. A missing mark is
//! what must never happen, so the mark is synced before its batch is written.

use std::{io, path::Path};

use broca_records::session_storage::StorageClass;
use broca_session_store::backend::SessionAddr;
use broca_session_store::fold::operations::Operations;
use cortexkit_role_llm_runner::session_retention::{Keep, RetentionPolicy};

const DIRECTORY: &str = "retained";

/// The roots whose sessions are found through their marks.
pub(crate) const MARKED: [StorageClass; 2] = [StorageClass::Durable, StorageClass::Ordinary];

/// Whether a session stored in a root of `class`, holding `operations`, may come due only
/// through a mark: it is outside the ephemeral root, and released or kept by a policy its first
/// send carrying one froze, other than `forever`.
pub(crate) fn needs_mark(class: StorageClass, operations: &Operations) -> bool {
    if class == StorageClass::Ephemeral {
        return false;
    }
    if operations.released_at_ms.is_some() {
        return true;
    }
    match operations
        .retention_policy
        .as_ref()
        .map(RetentionPolicy::from_value)
    {
        None => false,
        Some(Ok(policy)) => policy.keep != Keep::Forever,
        // A damaged policy keeps its mark, so the pass that can't decide the session says so.
        Some(Err(_)) => true,
    }
}

/// Marks the session at `addr` in `root`, synced, before the batch that may give it a deadline.
pub(crate) fn mark(root: &Path, addr: &SessionAddr) -> io::Result<()> {
    let dir = root.join(DIRECTORY);
    broca_owner_only::create_dir_all(&dir)?;
    let file = broca_owner_only::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.join(&addr.0))?;
    broca_session_store::durable::sync(&file)?;
    broca_session_store::durable::sync_dir(&dir)
}

/// Removes the session's mark: it was deleted, or nothing can give it a deadline now.
pub(crate) fn remove(root: &Path, addr: &SessionAddr) -> io::Result<()> {
    match std::fs::remove_file(root.join(DIRECTORY).join(&addr.0)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// The sessions marked in `root`. A name that isn't an address is no session's mark.
pub(crate) fn marked(root: &Path) -> io::Result<Vec<SessionAddr>> {
    let entries = match std::fs::read_dir(root.join(DIRECTORY)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut marked = Vec::new();
    for entry in entries {
        let name = entry?.file_name();
        if let Some(name) = name
            .to_str()
            .filter(|name| name.len() == 16 && name.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            marked.push(SessionAddr(name.into()));
        }
    }
    Ok(marked)
}
