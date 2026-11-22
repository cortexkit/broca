//! A state root holding a `state-format` marker is in a format this build doesn't run.

use std::path::{Path, PathBuf};

/// The file whose presence marks a state root as another format, such as the removed
/// `broca-state/2` staging format. This build runs only unmarked roots.
pub const STATE_FORMAT_MARKER: &str = "state-format";

/// Opening a store in a marked state root, or in one whose marker can't be checked, refused.
#[derive(Debug)]
pub struct MarkedStateRoot {
    pub root: PathBuf,
    /// Why the marker couldn't be checked; `None` when it is present.
    pub unreadable: Option<std::io::Error>,
}

impl std::fmt::Display for MarkedStateRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.unreadable {
            Some(error) => write!(
                f,
                "refusing state root {}: its {STATE_FORMAT_MARKER} marker can't be checked: {error}",
                self.root.display()
            ),
            None => write!(
                f,
                "refusing state root {}: it is marked with a {STATE_FORMAT_MARKER} this build \
                 doesn't run",
                self.root.display()
            ),
        }
    }
}

impl std::error::Error for MarkedStateRoot {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.unreadable.as_ref().map(|error| error as _)
    }
}

/// Refuse `root` when it holds a `state-format` marker, or when the marker's presence can't be
/// checked: an unreadable marker is no evidence that the root is in this build's format.
pub fn refuse_marked_state_root(root: &Path) -> Result<(), MarkedStateRoot> {
    match std::fs::symlink_metadata(root.join(STATE_FORMAT_MARKER)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(MarkedStateRoot {
            root: root.to_path_buf(),
            unreadable: None,
        }),
        Err(error) => Err(MarkedStateRoot {
            root: root.to_path_buf(),
            unreadable: Some(error),
        }),
    }
}
