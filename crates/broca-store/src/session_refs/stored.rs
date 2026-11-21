//! The sessions this computer keeps, as the session-ref census reads them: every session
//! folder of each storage class's session store, and the bind identity its head names.

use broca_protocol::BindIdentity;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_wal::StorageClass;
use std::collections::BTreeSet;
use std::path::Path;

/// The session stores of one state root, one per storage class.
pub struct StoredSessions {
    stores: Vec<LocalBackend>,
}

/// What the census reads for one address.
pub(super) enum Stored {
    /// A session, created under `identity`; reading its head took `bytes`.
    Session { identity: BindIdentity, bytes: u64 },
    /// A folder with no head: a creation or a move that hasn't written its head yet, or a
    /// deletion. It holds no session, so it names none.
    NoSession,
}

impl StoredSessions {
    pub fn new(state_root: &Path) -> Self {
        Self {
            stores: StorageClass::ALL
                .into_iter()
                .map(|class| LocalBackend::new(class.store_root(state_root)))
                .collect(),
        }
    }

    /// Every session folder's address, across the classes. Listing folders reads no head, so
    /// it stays cheap with many sessions; a folder without a head reads as
    /// [`Stored::NoSession`].
    pub(super) fn addresses(&self) -> Result<BTreeSet<String>, String> {
        let mut addresses = BTreeSet::new();
        for store in &self.stores {
            let folders = store
                .session_folders()
                .map_err(|error| format!("{error:?}"))?;
            addresses.extend(folders.into_iter().map(|folder| folder.0));
        }
        Ok(addresses)
    }

    /// The identity the session at `address` was created under. The head is read without its
    /// key, as every key-free reader of heads does to name a session: the census only maps
    /// refs to sessions, and a move that follows the map opens the head with its key and
    /// refuses one that doesn't check. A head that names a session at another address is
    /// damaged, so it is refused rather than mapped.
    pub(super) fn identity(&self, address: &str) -> Result<Stored, String> {
        let addr = SessionAddr(address.to_owned());
        for store in &self.stores {
            let Some((head, _)) = store
                .read_stored_head(&addr)
                .map_err(|error| format!("{error:?}"))?
            else {
                continue;
            };
            let triple = broca_session_store::head::stored_triple(&head)
                .map_err(|error| format!("session head is unreadable: {error:?}"))?;
            let identity = BindIdentity::new(triple.project_root, triple.harness, triple.session);
            if broca_core::session_addr(&identity) != address {
                return Err("session head names a session at another address".into());
            }
            return Ok(Stored::Session {
                identity,
                bytes: head.len() as u64,
            });
        }
        Ok(Stored::NoSession)
    }
}
