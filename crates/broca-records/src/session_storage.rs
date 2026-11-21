//! Which of the session store's three roots holds a session, and where a new session goes.
//!
//! The session store places each new session in one of three backup classes ([`StorageClass`]),
//! chosen once at creation by [`placement`]. It stores them in three complete stores,
//! [`SESSION_STORAGE_DIR`], [`SESSION_STORAGE_DURABLE_DIR`] and
//! [`SESSION_STORAGE_EPHEMERAL_DIR`]. Only the durable ones are backed up. A session's root is
//! the one its head exists in ([`session_storage_class`]); a head in two roots is refused.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::features::SESSION_STORAGE_DIR;
use crate::fs_ops as fs;

/// The folder of a state root holding the session store's durable sessions, the one class of the
/// three that backups capture.
pub const SESSION_STORAGE_DURABLE_DIR: &str = "session-store-durable";

/// The folder of a state root holding the session store's sessions with a retention policy, which
/// backups leave out so no backup outlives the retention promise.
pub const SESSION_STORAGE_EPHEMERAL_DIR: &str = "session-store-ephemeral";

/// The folder of a state root where moves of a session between computers keep their staged
/// copies and stream caches. It sits beside the three class roots, not inside one, so a backup of the durable root
/// never carries a half-staged session; and on the state root's filesystem, so publishing a
/// staged session is still one rename.
pub const SESSION_STORAGE_MOVES_DIR: &str = "session-store-moves";

/// The folder of [`SESSION_STORAGE_MOVES_DIR`] that holds each move's staged session.
pub const MOVE_STAGING_DIR: &str = "staging";

/// The refusal of a session whose head exists in two of the session store's roots. Placement puts
/// a session in exactly one root, so a second head is damage, which is never resolved by
/// preferring one root.
pub const SESSION_IN_TWO_CLASSES: &str = "session_in_two_classes";

/// A session's backup class, decided once when it is created ([`placement`]) and kept for its
/// whole life: the folder holding it is authoritative afterwards, so changing `backup_classes`
/// never moves a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageClass {
    /// Backed up: `wal-durable/`, [`SESSION_STORAGE_DURABLE_DIR`].
    Durable,
    /// Not backed up, because its callers relaunch rather than restore it: `wal/`,
    /// [`SESSION_STORAGE_DIR`].
    Ordinary,
    /// Not backed up, because the session has a retention policy: `wal-ephemeral/`,
    /// [`SESSION_STORAGE_EPHEMERAL_DIR`].
    Ephemeral,
}

impl StorageClass {
    /// Every class, in the order the session store's roots are probed.
    pub const ALL: [Self; 3] = [Self::Durable, Self::Ordinary, Self::Ephemeral];

    /// The class's name, as move exports carry it and refusals print it.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Ordinary => "ordinary",
            Self::Ephemeral => "ephemeral",
        }
    }

    /// The class's position in [`Self::ALL`].
    pub const fn index(self) -> usize {
        match self {
            Self::Durable => 0,
            Self::Ordinary => 1,
            Self::Ephemeral => 2,
        }
    }

    /// The folder of a state root holding the session store's sessions of this class.
    pub const fn store_dir(self) -> &'static str {
        match self {
            Self::Durable => SESSION_STORAGE_DURABLE_DIR,
            Self::Ordinary => SESSION_STORAGE_DIR,
            Self::Ephemeral => SESSION_STORAGE_EPHEMERAL_DIR,
        }
    }

    /// The root of the session store's sessions of this class under `state_root`: a complete
    /// store, with its own heads, logs, objects and setup parts.
    pub fn store_root(self, state_root: &Path) -> PathBuf {
        state_root.join(self.store_dir())
    }
}

impl std::fmt::Display for StorageClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// The class a new session is placed in: a retention policy places it in the ephemeral class,
/// whatever else holds; else a session selected for backup (`durable`) is placed in the durable
/// class; everything else is ordinary. Both storages decide through this one function, so they
/// never place a session differently. What selects a session for backup depends on what creates
/// it: a send by its label ([`selected_for_backup`]), and an import, which only ever brings a
/// long-lived agent head onto broca, always.
pub fn placement(retained: bool, durable: bool) -> StorageClass {
    if retained {
        StorageClass::Ephemeral
    } else if durable {
        StorageClass::Durable
    } else {
        StorageClass::Ordinary
    }
}

/// Whether a send's `session_class` selects its session for backup: the label is one of
/// `backup_classes` (the daemon's setting, `["head"]` by default).
pub fn selected_for_backup(session_class: Option<&str>, backup_classes: &[String]) -> bool {
    session_class.is_some_and(|class| backup_classes.iter().any(|value| value == class))
}

/// Where the session store keeps the head of the session at `address` in the root of
/// `class` under `state_root`. The session exists there exactly when this file does: creating a
/// session writes its head before anything else. The layout is the store's local backend's,
/// `<root>/heads/<address>/head`.
pub fn session_storage_head(state_root: &Path, class: StorageClass, address: &str) -> PathBuf {
    class
        .store_root(state_root)
        .join("heads")
        .join(address)
        .join("head")
}

/// The class of the session store's root holding the session at `address`, `None` when no root
/// holds its head. A head found in two roots is refused [`SESSION_IN_TWO_CLASSES`]
/// ([`SessionInTwoClasses`] inside an `InvalidData` error). Only a head found absent counts as
/// absent: any other failure to check it is an error, so an unreadable folder never reads as
/// holding nothing.
pub fn session_storage_class(state_root: &Path, address: &str) -> io::Result<Option<StorageClass>> {
    session_storage_class_by(state_root, address, exists)
}

/// [`session_storage_class`], checking whether each root's head exists with `exists`: for a
/// caller that checks the head its own way, such as without following a link, as it did when
/// the session store had one root. Errors as [`session_storage_class`], and as `exists` does.
pub fn session_storage_class_by(
    state_root: &Path,
    address: &str,
    exists: impl Fn(&Path) -> io::Result<bool>,
) -> io::Result<Option<StorageClass>> {
    let mut found = Vec::new();
    for class in StorageClass::ALL {
        if exists(&session_storage_head(state_root, class, address))? {
            found.push(class);
        }
    }
    match found.as_slice() {
        [] => Ok(None),
        [class] => Ok(Some(*class)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            SessionInTwoClasses {
                address: address.to_owned(),
                classes: found,
            },
        )),
    }
}

/// A session with a head in more than one of the session store's roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInTwoClasses {
    pub address: String,
    /// The classes whose roots hold a head, in [`StorageClass::ALL`] order.
    pub classes: Vec<StorageClass>,
}

impl SessionInTwoClasses {
    /// The refusal inside `error`, when it is one.
    pub fn of(error: &io::Error) -> Option<&Self> {
        error.get_ref()?.downcast_ref::<Self>()
    }
}

impl std::fmt::Display for SessionInTwoClasses {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let classes: Vec<&str> = self.classes.iter().map(|class| class.name()).collect();
        write!(
            f,
            "{SESSION_IN_TWO_CLASSES}: the session {} has a head in more than one class of the \
             new storage ({}); an operator must remove the copy that doesn't belong",
            self.address,
            classes.join(", ")
        )
    }
}

impl std::error::Error for SessionInTwoClasses {}

/// The folder under `state_root` holding moves' staged sessions and stream caches.
pub fn session_storage_moves(state_root: &Path) -> PathBuf {
    state_root.join(SESSION_STORAGE_MOVES_DIR)
}

fn exists(path: &Path) -> io::Result<bool> {
    fs::try_exists(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(tag: &str) -> PathBuf {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-wal-session-storage-{}-{tag}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    /// A head in any one of the three roots claims its address for the session store, and names
    /// its root's class.
    #[test]
    fn a_head_in_any_class_root_claims_the_address() {
        for class in StorageClass::ALL {
            let root = root(&format!("claims-{class}"));
            let address = "00000000000000aa";
            assert_eq!(session_storage_class(&root, address).unwrap(), None);
            let head = session_storage_head(&root, class, address);
            assert!(head.starts_with(root.join(class.store_dir())), "{class}");
            std::fs::create_dir_all(head.parent().unwrap()).unwrap();
            std::fs::write(&head, b"head").unwrap();
            assert_eq!(session_storage_class(&root, address).unwrap(), Some(class));
            assert_eq!(
                session_storage_class(&root, "00000000000000bb").unwrap(),
                None
            );
            let _ = std::fs::remove_dir_all(&root);
        }
    }

    /// A head in two roots is damage: refused by name, never read as either root's.
    #[test]
    fn a_head_in_two_class_roots_is_refused_by_name() {
        let root = root("two-classes");
        let address = "00000000000000aa";
        for class in [StorageClass::Durable, StorageClass::Ephemeral] {
            let head = session_storage_head(&root, class, address);
            std::fs::create_dir_all(head.parent().unwrap()).unwrap();
            std::fs::write(&head, b"head").unwrap();
        }
        let error = session_storage_class(&root, address).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            SessionInTwoClasses::of(&error),
            Some(&SessionInTwoClasses {
                address: address.into(),
                classes: vec![StorageClass::Durable, StorageClass::Ephemeral],
            })
        );
        assert!(error.to_string().starts_with(SESSION_IN_TWO_CLASSES));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn placement_puts_retention_first_then_backup_classes() {
        let classes = vec!["head".to_owned()];
        let send = |retained, class| placement(retained, selected_for_backup(class, &classes));
        // A retention policy wins over a durable label.
        assert_eq!(send(true, Some("head")), StorageClass::Ephemeral);
        assert_eq!(send(true, None), StorageClass::Ephemeral);
        assert_eq!(send(false, Some("head")), StorageClass::Durable);
        assert_eq!(send(false, Some("worker")), StorageClass::Ordinary);
        assert_eq!(send(false, None), StorageClass::Ordinary);
        assert!(!selected_for_backup(Some("head"), &[]));
        // An import is selected for backup whatever its label, and its retention still wins.
        assert_eq!(placement(false, true), StorageClass::Durable);
        assert_eq!(placement(true, true), StorageClass::Ephemeral);
    }
}
