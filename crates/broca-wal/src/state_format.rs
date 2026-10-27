//! The `state-format` marker, and the one decision a process makes from it: which frame format
//! its logs are written in.
//!
//! A state root holds a `state-format` file beside its `wal/` directory. The value
//! `broca-state/2` marks an overhaul root, whose log frames are version 4: their headers carry
//! `appended_ms` and `writer_id`. A root without the file is a legacy root and keeps today's
//! frames byte for byte. Overhaul root creation writes the marker before any other file, so a
//! root without one is a legacy root rather than a half-created overhaul root.
//!
//! [`classify_state_root`] reads the marker once, at boot, and returns the [`WalConfig`] every
//! store and log of that process is built with. It fails closed: a marker it cannot read, or one
//! naming a format this build does not know, is an error and boot refuses, because guessing
//! either way could write frames the root's readers cannot open.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::fs_ops;

/// The marker file's name in a state root, next to the `wal/` directory.
pub const MARKER_FILENAME: &str = "state-format";

/// The value that marks a root as an overhaul root. One trailing newline is also accepted, so
/// an operator may write the file with an editor.
pub const OVERHAUL_MARKER: &[u8] = b"broca-state/2";

/// The directory beside the marker that holds the root's ordinary logs.
pub const WAL_DIRNAME: &str = "wal";

/// Which frame format a state root's logs are written in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WalFormat {
    /// Today's frames (versions 1 to 3), byte for byte what deployed readers expect.
    #[default]
    Legacy,
    /// Version 4 frames, stamped with `appended_ms` and `writer_id` under `frame-stamps/v1`.
    Overhaul,
}

/// How a process writes its logs, decided once at boot by [`classify_state_root`] and handed to
/// every store and log it opens. The default is [`WalFormat::Legacy`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalConfig {
    pub format: WalFormat,
}

impl WalConfig {
    pub const LEGACY: Self = Self {
        format: WalFormat::Legacy,
    };
    pub const OVERHAUL: Self = Self {
        format: WalFormat::Overhaul,
    };

    pub fn is_overhaul(self) -> bool {
        self.format == WalFormat::Overhaul
    }
}

/// Why a state root's log format could not be decided, or why a store refuses to write there.
#[derive(Clone, Debug)]
pub enum StateRootError {
    /// The marker exists (or may exist) but could not be read.
    UnreadableMarker {
        path: PathBuf,
        error: Arc<io::Error>,
    },
    /// The marker names a format this build does not write.
    UnknownMarker { path: PathBuf, found: String },
    /// On an overhaul root, `wal/` is a symbolic link, so its logs would land wherever the
    /// link points rather than in the root the marker describes.
    WalSymlink { wal: PathBuf },
    /// On an overhaul root, `wal/` resolves outside the state root.
    WalOutsideRoot { wal: PathBuf, resolved: PathBuf },
    /// On an overhaul root, `wal/` exists but is not a directory.
    WalNotDirectory { wal: PathBuf },
    /// On an overhaul root, `wal/` or the root itself could not be resolved.
    UnresolvableWal {
        path: PathBuf,
        error: Arc<io::Error>,
    },
    /// A store built without the boot decision found a marker other than a plain absence:
    /// `found` says what. Such a store writes only legacy frames, so it refuses every writable
    /// open rather than put them on a root that may be an overhaul root.
    OverhaulRootNeedsConfig { root: PathBuf, found: String },
}

impl fmt::Display for StateRootError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnreadableMarker { path, error } => write!(
                f,
                "cannot read the state-format marker {}: {error}; refusing to guess the state \
                 root's log format",
                path.display()
            ),
            Self::UnknownMarker { path, found } => write!(
                f,
                "the state-format marker {} reads {found:?}, a format this build does not write",
                path.display()
            ),
            Self::WalSymlink { wal } => write!(
                f,
                "the overhaul state root's log directory {} is a symbolic link; refusing to \
                 write logs through it",
                wal.display()
            ),
            Self::WalOutsideRoot { wal, resolved } => write!(
                f,
                "the overhaul state root's log directory {} resolves to {}, outside the root; \
                 refusing to write logs there",
                wal.display(),
                resolved.display()
            ),
            Self::WalNotDirectory { wal } => write!(
                f,
                "the overhaul state root's log directory {} is not a directory",
                wal.display()
            ),
            Self::UnresolvableWal { path, error } => write!(
                f,
                "cannot resolve {} on an overhaul state root: {error}",
                path.display()
            ),
            Self::OverhaulRootNeedsConfig { root, found } => write!(
                f,
                "wal store refused: this store was built without the boot state-format \
                 decision and writes only legacy frames, but the state root {} is not plainly \
                 legacy ({found})",
                root.display()
            ),
        }
    }
}

impl std::error::Error for StateRootError {}

/// Decide the log format of the state root at `root`, once, at boot.
///
/// - A missing marker (`NotFound`) is [`WalConfig::LEGACY`].
/// - A marker reading `broca-state/2` (optionally with one trailing newline) is
///   [`WalConfig::OVERHAUL`], provided `wal/` is either absent or a real directory inside the
///   root. A `wal/` that is a symbolic link, or that resolves outside the root, is refused.
/// - Any other read error, or any other marker value, is an error.
pub fn classify_state_root(root: &Path) -> Result<WalConfig, StateRootError> {
    let path = root.join(MARKER_FILENAME);
    let marker = match fs_ops::read(&path) {
        Ok(marker) => marker,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(WalConfig::LEGACY),
        Err(error) => {
            return Err(StateRootError::UnreadableMarker {
                path,
                error: Arc::new(error),
            })
        }
    };
    if marker.strip_suffix(b"\n").unwrap_or(&marker) != OVERHAUL_MARKER {
        return Err(StateRootError::UnknownMarker {
            path,
            found: describe_marker(&marker),
        });
    }
    check_wal_dir(root)?;
    Ok(WalConfig::OVERHAUL)
}

/// The check a store built without the boot decision makes once, at construction. Such a store
/// writes only legacy frames, so it may write only where the marker is plainly absent: `None`
/// there, and otherwise the refusal its writable opens return. An unreadable or unknown marker is
/// no proof of a legacy root, so it refuses too.
pub fn legacy_store_refusal(root: &Path) -> Option<StateRootError> {
    let found = match classify_state_root(root) {
        Ok(config) if !config.is_overhaul() => return None,
        Ok(_) => "it is marked broca-state/2".to_owned(),
        Err(error) => error.to_string(),
    };
    Some(StateRootError::OverhaulRootNeedsConfig {
        root: root.to_path_buf(),
        found,
    })
}

/// On an overhaul root, `wal/` must be absent or a real directory that resolves to exactly
/// `<root>/wal`. The directory entry itself is checked for a symbolic link (resolving a link
/// follows it, so a dangling one would otherwise look absent), and both paths are resolved so a
/// root that is itself reached through a link, as `/tmp` is on macOS, still passes.
fn check_wal_dir(root: &Path) -> Result<(), StateRootError> {
    let wal = root.join(WAL_DIRNAME);
    let unresolvable = |path: &Path| {
        let path = path.to_path_buf();
        move |error: io::Error| StateRootError::UnresolvableWal {
            path,
            error: Arc::new(error),
        }
    };
    let mut file_type = None;
    for entry in fs_ops::read_dir(root).map_err(unresolvable(root))? {
        let entry = entry.map_err(unresolvable(root))?;
        if entry.file_name() == WAL_DIRNAME {
            file_type = Some(entry.file_type().map_err(unresolvable(&wal))?);
        }
    }
    // Not created yet: the first log open creates it as a plain directory.
    let Some(file_type) = file_type else {
        return Ok(());
    };
    if file_type.is_symlink() {
        return Err(StateRootError::WalSymlink { wal });
    }
    if !file_type.is_dir() {
        return Err(StateRootError::WalNotDirectory { wal });
    }
    let resolved = fs_ops::canonicalize(&wal).map_err(unresolvable(&wal))?;
    let canonical_root = fs_ops::canonicalize(root).map_err(unresolvable(root))?;
    if resolved != canonical_root.join(WAL_DIRNAME) {
        return Err(StateRootError::WalOutsideRoot { wal, resolved });
    }
    Ok(())
}

/// A marker's bytes as an operator can read them in a refusal, without dumping a large file.
fn describe_marker(bytes: &[u8]) -> String {
    const SHOWN: usize = 64;
    let text = String::from_utf8_lossy(&bytes[..bytes.len().min(SHOWN)]).into_owned();
    if bytes.len() > SHOWN {
        format!("{text}... ({} bytes)", bytes.len())
    } else {
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// One test's state root, created empty under the owner-only scratch root.
    fn scratch_root(tag: &str) -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "state-format-{tag}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn mark(root: &Path, marker: &[u8]) {
        std::fs::write(root.join(MARKER_FILENAME), marker).unwrap();
    }

    #[test]
    fn a_missing_marker_is_legacy_and_the_overhaul_marker_is_overhaul() {
        let root = scratch_root("missing");
        assert_eq!(classify_state_root(&root).unwrap(), WalConfig::LEGACY);
        assert!(legacy_store_refusal(&root).is_none());
        // The temporary file an interrupted creation leaves is not the marker.
        std::fs::write(root.join("state-format.tmp"), OVERHAUL_MARKER).unwrap();
        assert_eq!(classify_state_root(&root).unwrap(), WalConfig::LEGACY);

        for marker in [&b"broca-state/2"[..], b"broca-state/2\n"] {
            let root = scratch_root("overhaul");
            mark(&root, marker);
            assert_eq!(classify_state_root(&root).unwrap(), WalConfig::OVERHAUL);
            assert!(
                matches!(
                    legacy_store_refusal(&root),
                    Some(StateRootError::OverhaulRootNeedsConfig { .. })
                ),
                "a store built without the boot decision refuses an overhaul root"
            );
        }
    }

    #[test]
    fn an_unknown_marker_is_an_error_not_a_legacy_root() {
        for marker in [
            &b""[..],
            b"broca-state/1\n",
            b"broca-state/20\n",
            b"broca-state/2\n\n",
            b" broca-state/2",
        ] {
            let root = scratch_root("unknown");
            mark(&root, marker);
            let error = classify_state_root(&root).unwrap_err();
            assert!(
                matches!(&error, StateRootError::UnknownMarker { found, .. }
                    if found.as_bytes() == marker),
                "{marker:?}: {error:?}"
            );
            assert!(legacy_store_refusal(&root).is_some(), "{marker:?}");
        }
    }

    #[test]
    fn an_unreadable_marker_refuses_boot() {
        // A directory where the marker belongs cannot be read as a file, whoever runs the test
        // (a permission bit would not stop a root user from reading it).
        let root = scratch_root("unreadable");
        std::fs::create_dir(root.join(MARKER_FILENAME)).unwrap();
        let error = classify_state_root(&root).unwrap_err();
        assert!(
            matches!(error, StateRootError::UnreadableMarker { .. }),
            "{error:?}"
        );
        assert!(error.to_string().contains(MARKER_FILENAME), "{error}");
        let refusal = legacy_store_refusal(&root).expect("an unreadable marker is not legacy");
        assert!(
            matches!(refusal, StateRootError::OverhaulRootNeedsConfig { .. }),
            "{refusal:?}"
        );
    }

    #[test]
    fn an_overhaul_root_accepts_a_real_wal_directory_or_none() {
        let root = scratch_root("real-wal");
        mark(&root, OVERHAUL_MARKER);
        assert_eq!(classify_state_root(&root).unwrap(), WalConfig::OVERHAUL);
        std::fs::create_dir(root.join(WAL_DIRNAME)).unwrap();
        assert_eq!(classify_state_root(&root).unwrap(), WalConfig::OVERHAUL);
    }

    #[test]
    fn an_overhaul_root_refuses_a_wal_that_is_not_a_directory() {
        let root = scratch_root("wal-file");
        mark(&root, OVERHAUL_MARKER);
        std::fs::write(root.join(WAL_DIRNAME), b"").unwrap();
        let error = classify_state_root(&root).unwrap_err();
        assert!(
            matches!(error, StateRootError::WalNotDirectory { .. }),
            "{error:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn an_overhaul_root_refuses_a_symlinked_wal_directory() {
        let outside = scratch_root("outside");
        for target in ["outside", "sibling", "dangling"] {
            let root = scratch_root("symlinked-wal");
            mark(&root, OVERHAUL_MARKER);
            let target = match target {
                "outside" => outside.clone(),
                "sibling" => {
                    std::fs::create_dir(root.join("logs")).unwrap();
                    root.join("logs")
                }
                _ => root.join("never-created"),
            };
            std::os::unix::fs::symlink(&target, root.join(WAL_DIRNAME)).unwrap();
            let error = classify_state_root(&root).unwrap_err();
            assert!(
                matches!(error, StateRootError::WalSymlink { .. }),
                "{}: {error:?}",
                target.display()
            );
            assert!(error.to_string().contains("symbolic link"), "{error}");
        }
        // The same link on a legacy root is left alone: legacy roots keep today's behavior.
        let root = scratch_root("legacy-symlinked-wal");
        std::os::unix::fs::symlink(&outside, root.join(WAL_DIRNAME)).unwrap();
        assert_eq!(classify_state_root(&root).unwrap(), WalConfig::LEGACY);
    }

    #[cfg(unix)]
    #[test]
    fn a_root_reached_through_a_link_still_classifies() {
        // The root path itself may pass through a link (macOS's /tmp does); only `wal/` must
        // resolve inside the resolved root.
        let real = scratch_root("linked-root");
        mark(&real, OVERHAUL_MARKER);
        std::fs::create_dir(real.join(WAL_DIRNAME)).unwrap();
        let link = scratch_root("root-link").join("root");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert_eq!(classify_state_root(&link).unwrap(), WalConfig::OVERHAUL);
    }
}
