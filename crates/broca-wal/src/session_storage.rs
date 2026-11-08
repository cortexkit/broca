//! Which storage owns a session: today's (a WAL) or the new session storage
//! (`broca-session-store`, under [`SESSION_STORAGE_DIR`]).
//!
//! The new storage owns a session once its head exists ([`session_storage_head`]). A move of a
//! live session from today's storage into the new one writes a journal naming the session
//! ([`session_storage_move_journal`]) before it touches anything else, and deletes it last, so
//! while a move is in flight the journal names the session too. Readers of today's storage
//! that don't go through `FileWalStore::check_move` ask [`owned_by_store`] and skip or refuse a
//! session it claims: otherwise they could expire, project or rewrite a WAL that a move is
//! copying, or that the new storage has already taken over.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::framing::{session_storage_head, SESSION_STORAGE_DIR};
use crate::fs_ops as fs;

/// The folder of the new session storage that holds move journals, one file per address.
pub const MOVE_JOURNAL_DIR: &str = "moves";

/// Where a move of the session at `address` into the new session storage keeps its journal.
pub fn session_storage_move_journal(state_root: &Path, address: &str) -> PathBuf {
    state_root
        .join(SESSION_STORAGE_DIR)
        .join(MOVE_JOURNAL_DIR)
        .join(address)
}

/// A move's journal: what startup needs to finish or undo the move after a crash, without
/// touching anything the journal doesn't name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveJournal {
    /// `store-move:<uuid>`. It is also the cut id of the move's WAL freeze, so startup can tell
    /// this move's freeze from any other.
    pub move_id: String,
    /// The session's address, which both storages file it under.
    pub address: String,
    pub project_root: String,
    pub harness: String,
    pub session: String,
    /// The lineage of the WAL being moved, as its first frame records it.
    pub lineage_id: Option<String>,
    /// The private folder the move writes the session into before publishing it, relative to
    /// the new storage's root.
    pub staging: String,
    /// The WAL prefix the move copies, recorded once the WAL is frozen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen: Option<FrozenPrefix>,
    /// The conversation layout the staged session was written with, recorded once known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub appends_in_conversation: Option<bool>,
}

/// The frozen WAL prefix a move copies: everything before `end_offset`, whose SHA-256 is
/// `sha256`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenPrefix {
    /// The frozen WAL's path, relative to the state root.
    pub wal: String,
    pub end_offset: u64,
    /// Lowercase hex.
    pub sha256: String,
}

/// Who owns the session at an address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreOwnership {
    /// Today's storage: no head and no move journal.
    Today,
    /// The new storage: the session's head exists.
    Held,
    /// A move into the new storage is in flight (or was interrupted): its journal names the
    /// session and no head exists yet.
    Moving { move_id: String },
}

/// Who owns the session at `address` under `state_root`. Only a file found absent counts as
/// absent: any other failure to check, or a journal that can't be read, is an error, so an
/// unreadable folder never reads as today's.
pub fn store_ownership(state_root: &Path, address: &str) -> io::Result<StoreOwnership> {
    if exists(&session_storage_head(state_root, address))? {
        return Ok(StoreOwnership::Held);
    }
    let journal = session_storage_move_journal(state_root, address);
    let bytes = match fs::read(&journal) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(StoreOwnership::Today),
        Err(error) => return Err(error),
    };
    let journal: MoveJournal = serde_json::from_slice(&bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unreadable session move journal {}: {error}",
                journal.display()
            ),
        )
    })?;
    Ok(StoreOwnership::Moving {
        move_id: journal.move_id,
    })
}

/// Whether the new session storage owns the session at `address`, or a move into it is in
/// flight: its head exists, or a move journal names it. A reader of today's storage that sees
/// `true` skips the session or refuses it. Errors as [`store_ownership`].
pub fn owned_by_store(state_root: &Path, address: &str) -> io::Result<bool> {
    if exists(&session_storage_head(state_root, address))? {
        return Ok(true);
    }
    exists(&session_storage_move_journal(state_root, address))
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

    fn journal(move_id: &str) -> MoveJournal {
        MoveJournal {
            move_id: move_id.into(),
            address: "00000000000000aa".into(),
            project_root: "/p".into(),
            harness: "h".into(),
            session: "s".into(),
            lineage_id: None,
            staging: "staging/x".into(),
            frozen: None,
            appends_in_conversation: None,
        }
    }

    #[test]
    fn a_head_or_a_journal_claims_the_address_and_nothing_else_does() {
        let root = root("claims");
        let address = "00000000000000aa";
        assert!(!owned_by_store(&root, address).unwrap());
        assert_eq!(
            store_ownership(&root, address).unwrap(),
            StoreOwnership::Today
        );

        let path = session_storage_move_journal(&root, address);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_vec(&journal("store-move:1")).unwrap()).unwrap();
        assert!(owned_by_store(&root, address).unwrap());
        assert_eq!(
            store_ownership(&root, address).unwrap(),
            StoreOwnership::Moving {
                move_id: "store-move:1".into()
            }
        );
        // Another address stays today's.
        assert!(!owned_by_store(&root, "00000000000000bb").unwrap());

        let head = session_storage_head(&root, address);
        std::fs::create_dir_all(head.parent().unwrap()).unwrap();
        std::fs::write(&head, b"head").unwrap();
        assert!(owned_by_store(&root, address).unwrap());
        assert_eq!(
            store_ownership(&root, address).unwrap(),
            StoreOwnership::Held
        );
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            store_ownership(&root, address).unwrap(),
            StoreOwnership::Held
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn an_unreadable_journal_is_an_error_not_absence() {
        let root = root("unreadable");
        let address = "00000000000000aa";
        let path = session_storage_move_journal(&root, address);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"not json").unwrap();
        assert!(owned_by_store(&root, address).unwrap());
        assert_eq!(
            store_ownership(&root, address).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
