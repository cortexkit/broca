//! Sessions that moved here from another computer and haven't started a run here yet.
//!
//! Routing keeps each conversation on one provider account, so its prompt cache stays warm, but
//! it keeps that affinity per computer: a moved session's first run here would be a fresh pick
//! and could land on another account, rebuilding the whole prompt cache. So that one run names
//! the credential the session last ran on (`RunControl::moved_here`); routing honours it while
//! it has no affinity for the conversation, and the run's admission gives it one.
//!
//! The mark is an empty file named for the session, in the storage class's root beside the
//! recovery candidates. The move writes it before it publishes the session, and the session's
//! actor removes it once a run's start is durable here. A run refused before its start leaves
//! it, so the next run asks again. A mark left behind (the session deleted or moved on first)
//! is harmless: it only ever names the credential of a run the session itself recorded.

use std::{io, path::Path};

use broca_session_store::backend::SessionAddr;

const DIRECTORY: &str = "moved-here";

/// Marks the session as moved here. Synced, so a crash after the session is published can't
/// lose it.
pub(crate) fn mark(root: &Path, addr: &SessionAddr) -> io::Result<()> {
    let dir = root.join(DIRECTORY);
    broca_owner_only::create_dir_all(&dir)?;
    broca_session_store::durable::sync_dir(root)?;
    let file = broca_owner_only::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.join(&addr.0))?;
    broca_session_store::durable::sync(&file)?;
    broca_session_store::durable::sync_dir(&dir)
}

pub(crate) fn is_marked(root: &Path, addr: &SessionAddr) -> bool {
    root.join(DIRECTORY).join(&addr.0).exists()
}

/// Removes the mark. Not synced: a mark a crash brings back only names the credential again on
/// a session whose routing affinity already exists, and routing ignores it then.
pub(crate) fn clear(root: &Path, addr: &SessionAddr) -> io::Result<()> {
    match std::fs::remove_file(root.join(DIRECTORY).join(&addr.0)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
