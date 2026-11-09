//! The one-time step that turns the new session storage's first, single-folder tree into its
//! durable root.
//!
//! The new storage first kept every session in one folder, `session-store/`, and the backup
//! descriptor captured all of it. Only long-lived agent heads, moved or imported there, were
//! ever placed in it, so everything in that tree is durable by construction. The storage now
//! keeps one root per backup class (`broca_wal::StorageClass`), and `session-store/` is the
//! ordinary root, which backups leave
//! out. So at startup, before any store opens and before the class roots are created, a state
//! root with no durable root and a `session-store/` holding anything has that folder renamed to
//! `session-store-durable/`. Its move journals and staged sessions go to
//! `session-store-moves/` first, where moves now keep them. On every later start the durable
//! root exists and the step does nothing.

use std::io;
use std::path::Path;

use broca_wal::migration;

/// What [`class_single_folder_tree`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Layout {
    /// Nothing: the durable root already exists, or there is no single-folder tree to keep.
    Unchanged,
    /// The single-folder tree became the durable root, after `journals` move journals and
    /// `staged` staged sessions went to the moves folder.
    Classed { journals: usize, staged: usize },
}

/// Makes the single-folder tree under `state_root` the durable root, once (see the module's
/// description). Each step is one rename on the state root's filesystem, synced, so a crash
/// leaves a tree the next start finishes: the journals and staged sessions already moved stay
/// moved, and the folder is renamed then. Refuses, touching nothing more, a journal or staged
/// session whose name the moves folder already holds.
pub fn class_single_folder_tree(state_root: &Path) -> io::Result<Layout> {
    let durable = state_root.join(broca_wal::SESSION_STORAGE_DURABLE_DIR);
    match std::fs::symlink_metadata(&durable) {
        Ok(_) => return Ok(Layout::Unchanged),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let single = state_root.join(broca_wal::SESSION_STORAGE_DIR);
    let holds_anything = match std::fs::read_dir(&single) {
        Ok(mut entries) => entries.next().transpose()?.is_some(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(error),
    };
    if !holds_anything {
        return Ok(Layout::Unchanged);
    }
    let moves = broca_wal::session_storage_moves(state_root);
    owner_only_dir(&moves)?;
    // The single-folder tree kept journals in `moves/` and staged sessions in `staging/`.
    let journals = move_entries(
        &single.join("moves"),
        &moves.join(broca_wal::MOVE_JOURNAL_DIR),
    )?;
    let staged = move_entries(
        &single.join("staging"),
        &moves.join(broca_wal::MOVE_STAGING_DIR),
    )?;
    std::fs::rename(&single, &durable)?;
    migration::sync_dir(state_root)?;
    tracing::info!(
        target: "store_layout",
        from = %single.display(),
        to = %durable.display(),
        journals,
        staged,
        "made the new storage's single-folder tree its durable root"
    );
    Ok(Layout::Classed { journals, staged })
}

/// Renames every entry of the folder `from` into the folder `to`, creating `to`, then removes
/// `from`. Returns how many entries moved; an absent `from` moves none.
fn move_entries(from: &Path, to: &Path) -> io::Result<usize> {
    let entries = match std::fs::read_dir(from) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    owner_only_dir(to)?;
    let mut moved = 0;
    for entry in entries {
        let entry = entry?;
        let target = to.join(entry.file_name());
        // A rename replaces a file it lands on; nothing already in the moves folder is ever
        // replaced.
        if std::fs::symlink_metadata(&target).is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "{} can't move to {}, which already exists",
                    entry.path().display(),
                    target.display()
                ),
            ));
        }
        std::fs::rename(entry.path(), &target)?;
        moved += 1;
    }
    migration::sync_dir(to)?;
    std::fs::remove_dir(from)?;
    migration::sync_dir(from.parent().expect("a folder of the tree has a parent"))?;
    Ok(moved)
}

/// Creates the owner-only folder `path` when it doesn't exist, as startup creates the state
/// root's folders.
fn owner_only_dir(path: &Path) -> io::Result<()> {
    match broca_owner_only::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => {}
        Err(error) => return Err(error),
    }
    migration::sync_dir(path.parent().expect("a state-root folder has a parent"))
}

#[cfg(test)]
#[path = "store_layout_tests.rs"]
mod tests;
