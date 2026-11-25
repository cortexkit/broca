//! Expiry of the rebuildable plaintext request dumps.

use std::fs::{self, DirEntry, Metadata};
use std::io;
use std::path::Path;
use std::time::SystemTime;

use crate::store_request_dump::{DUMP_FOLDER, DUMP_MAX_AGE};

#[derive(Default)]
struct Removed {
    runs: u64,
    bytes: u64,
    files: u64,
}

fn warn(path: &Path, error: &io::Error) {
    tracing::warn!(target: "retention", path = %path.display(), %error, "request dump sweep failed");
}

fn metadata(path: &Path) -> Option<Metadata> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => {
            warn(path, &error);
            None
        }
    }
}

fn entries(path: &Path, mut visit: impl FnMut(DirEntry)) {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) => {
            warn(path, &error);
            return;
        }
    };
    for entry in entries {
        match entry {
            Ok(entry) => visit(entry),
            Err(error) => warn(path, &error),
        }
    }
}

fn expired(metadata: &Metadata, now: SystemTime) -> bool {
    metadata
        .modified()
        .ok()
        .and_then(|time| now.duration_since(time).ok())
        .is_some_and(|age| age > DUMP_MAX_AGE)
}

fn remove_file(path: &Path, metadata: &Metadata, removed: &mut Removed) {
    match fs::remove_file(path) {
        Ok(()) => {
            removed.files += 1;
            removed.bytes = removed.bytes.saturating_add(metadata.len());
        }
        Err(error) => warn(path, &error),
    }
}

fn tree_bytes(path: &Path) -> u64 {
    let mut bytes = 0_u64;
    entries(path, |entry| {
        let path = entry.path();
        let Some(metadata) = metadata(&path) else {
            return;
        };
        if metadata.is_dir() {
            bytes = bytes.saturating_add(tree_bytes(&path));
        } else if metadata.is_file() {
            bytes = bytes.saturating_add(metadata.len());
        }
    });
    bytes
}

// The owner-only state root protects the listing, and std's remove_dir_all does not follow symlinks.
pub(crate) fn sweep_request_dumps(state_root: &Path) {
    let root = state_root.join(DUMP_FOLDER);
    if !metadata(&root).is_some_and(|metadata| metadata.is_dir()) {
        return;
    }
    let now = SystemTime::now();
    let mut removed = Removed::default();
    entries(&root, |entry| {
        let path = entry.path();
        let Some(meta) = metadata(&path) else { return };
        if meta.is_file() && expired(&meta, now) {
            remove_file(&path, &meta, &mut removed);
        } else if meta.is_dir() {
            entries(&path, |entry| {
                let run = entry.path();
                let Some(metadata) = metadata(&run) else {
                    return;
                };
                if metadata.is_dir() && expired(&metadata, now) {
                    let bytes = tree_bytes(&run);
                    match fs::remove_dir_all(&run) {
                        Ok(()) => {
                            removed.runs += 1;
                            removed.bytes = removed.bytes.saturating_add(bytes);
                        }
                        Err(error) => warn(&run, &error),
                    }
                }
            });
            // A session with another run (or any other entry) must be kept.
            if fs::read_dir(&path).is_ok_and(|mut entries| entries.next().is_none()) {
                if let Err(error) = fs::remove_dir(&path) {
                    warn(&path, &error);
                }
            }
        }
    });
    if removed.runs > 0 || removed.files > 0 {
        tracing::info!(target: "retention", runs = removed.runs, bytes = removed.bytes, "expired request dumps removed");
    }
}
