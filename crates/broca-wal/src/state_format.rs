//! The `state-format` marker that divides overhaul roots from legacy ones.
//!
//! A state root holds a `state-format` file beside its `wal/` directory. The value
//! `broca-state/2` marks an overhaul root, whose log frames are version 4: their headers carry
//! `appended_ms` and `writer_id`. Every other root keeps today's frames, so a build that merges
//! the newer format writes it only where its readers expect it.
//!
//! Overhaul root creation writes the marker before any other file, so a root without one is a
//! legacy root rather than a half-created overhaul root.

use std::path::Path;

use crate::fs_ops;

/// The marker file's name in a state root, next to the `wal/` directory.
pub const MARKER_FILENAME: &str = "state-format";

/// The value that marks a root as an overhaul root.
pub const OVERHAUL_MARKER: &[u8] = b"broca-state/2";

/// Read the marker of the state root that holds the log at `wal_path`.
///
/// A missing or unreadable marker, and the temporary file an interrupted creation leaves,
/// both mean a legacy root. Reading a root as legacy at worst keeps the older frame format;
/// reading it as overhaul would write frames a legacy reader cannot open.
///
/// A state root keeps its marker beside its `wal/` directory, so the root is the log's
/// grandparent. A log placed directly inside a state root names that root as its own parent,
/// so both levels are checked.
pub fn is_overhaul_root(wal_path: &Path) -> bool {
    let parent = wal_path.parent().unwrap_or_else(|| Path::new(""));
    let mut roots = vec![parent];
    if let Some(grandparent) = parent.parent() {
        roots.push(grandparent);
    }
    roots
        .into_iter()
        .any(|root| match fs_ops::read(root.join(MARKER_FILENAME)) {
            Ok(marker) => marker == OVERHAUL_MARKER || marker == b"broca-state/2\n",
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                tracing::warn!(
                    target: "wal",
                    "cannot read the state-format marker in {}: {error}; keeping legacy frames",
                    root.display()
                );
                false
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// One test's state root, with a `wal/` directory, and the log path inside it.
    fn wal_root() -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "state-format-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("wal")).unwrap();
        root.join("wal")
    }

    /// Write the state root's marker for the log at `wal`.
    fn mark(wal: &Path, marker: &[u8]) {
        std::fs::write(wal.parent().unwrap().join(MARKER_FILENAME), marker).unwrap();
    }

    #[test]
    fn only_the_overhaul_marker_marks_an_overhaul_root() {
        for (marker, expected) in [
            (Some(b"broca-state/2\n".as_slice()), true),
            (Some(b"broca-state/2".as_slice()), true),
            (Some(b"".as_slice()), false),
            (Some(b"broca-state/1\n".as_slice()), false),
            (Some(b"broca-state/20\n".as_slice()), false),
            (None, false),
        ] {
            let wal = wal_root();
            if let Some(marker) = marker {
                mark(&wal, marker);
            }
            assert_eq!(
                is_overhaul_root(&wal),
                expected,
                "marker {:?}",
                marker.map(String::from_utf8_lossy)
            );
        }
    }

    #[test]
    fn an_interrupted_creation_leaves_a_legacy_root() {
        let wal = wal_root();
        // Only the temporary marker a creation writes before its rename reaches this root.
        mark(&wal, b"");
        std::fs::write(
            wal.parent().unwrap().join("state-format.tmp"),
            OVERHAUL_MARKER,
        )
        .unwrap();
        assert!(!is_overhaul_root(&wal));
    }
}
