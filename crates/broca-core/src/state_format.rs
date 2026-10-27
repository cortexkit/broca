//! The state-format marker that tells a `broca-state/2` root apart from a legacy one.
//!
//! A root holding `state-format` = `broca-state/2` is written by the overhaul format, which the
//! version 4 WAL frame belongs to. Every other root keeps the frames today's readers already
//! understand, so nothing on a production root starts writing a format its readers cannot open.
//! Overhaul root creation writes the marker before any other file in the root, so a root without
//! one is a legacy root rather than a half-created overhaul root.

use std::path::Path;

/// The marker file's name in a state root, next to the `wal/` directory.
pub const MARKER_FILENAME: &str = broca_wal::state_format::MARKER_FILENAME;

/// What a state root's marker says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateFormat {
    /// The marker reads `broca-state/2`.
    Overhaul,
    /// No usable marker: a legacy root, whose writers keep legacy frames.
    Legacy,
}

/// Read the marker of the state root that holds the logs in `wal_root` (a store's `wal/`
/// directory).
///
/// A missing or unreadable marker is [`StateFormat::Legacy`]: treating an unknown root as
/// overhaul is what would put frames on a production disk that its readers cannot open.
pub fn read_beside(wal_root: &Path) -> StateFormat {
    if broca_wal::state_format::is_overhaul_root(wal_root) {
        StateFormat::Overhaul
    } else {
        StateFormat::Legacy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// One test's state root with a `wal/` directory.
    fn scratch_root() -> std::path::PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "state-format-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("wal")).unwrap();
        root
    }

    #[test]
    fn only_the_overhaul_marker_marks_an_overhaul_root() {
        for (marker, expected) in [
            (Some(b"broca-state/2\n".as_slice()), StateFormat::Overhaul),
            (Some(b"broca-state/2".as_slice()), StateFormat::Overhaul),
            (Some(b"".as_slice()), StateFormat::Legacy),
            (Some(b"broca-state/1\n".as_slice()), StateFormat::Legacy),
            (Some(b"broca-state/20\n".as_slice()), StateFormat::Legacy),
            (None, StateFormat::Legacy),
        ] {
            let root = scratch_root();
            if let Some(marker) = marker {
                std::fs::write(root.join(MARKER_FILENAME), marker).unwrap();
            }
            assert_eq!(
                read_beside(&root.join("wal")),
                expected,
                "marker {:?}",
                marker.map(String::from_utf8_lossy)
            );
        }
    }
}
