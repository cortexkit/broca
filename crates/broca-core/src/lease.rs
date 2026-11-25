//! The durable single-writer lease, provided by the shared `cortexkit-lease` crate.
//!
//! A multi-session daemon must never have two live writers on one session's log, so
//! `start_run` acquires this lease and FAILS if a live writer holds it. The lease's
//! monotonic `epoch` is the lease-tenure stamp every log append carries for forensic
//! correlation. It is not compared on the append path and does not reject a stale stamp;
//! single-writer safety comes from the OS lock plus the log file-integrity check.
//!
//! The mechanics (OS advisory lock for liveness + persisted epoch stamp) live in
//! `cortexkit-lease`, shared across CortexKit modules. This module is only the seam
//! that maps broca's session identity (`BindIdentity`) to the shared crate's
//! namespaced `LeaseKey` — kept in ONE place so the key derivation can't drift.

use std::path::{Path, PathBuf};

use broca_protocol::BindIdentity;
use cortexkit_lease::LeaseKey;

pub use cortexkit_lease::{FileLeaseStore, LeaseError, LeaseHandle, LeaseStore};

/// The lease module and backend every session lease is taken under (see [`lease_key_for`]).
const SESSION_LEASE: (&str, &str) = ("broca", "wal");

/// The name of `session`'s lease file in the leases folder, as `cortexkit-lease` derives it:
/// FNV-1a 64 over the lease key's module, backend and scope joined by U+001F, in hex.
pub fn session_lease_file(session: &BindIdentity) -> String {
    let key = lease_key_for(session);
    format!(
        "{}.lease",
        crate::session_refs::joined_triple_addr(&format!(
            "{}\u{1f}{}\u{1f}{}",
            key.module_id, key.backend, key.scope_key
        ))
    )
}

/// The leases under `state_root`, as broca's own processes take them: a session's lease file is
/// removed when its last holder lets go and no session is stored at its address any more.
pub fn session_leases(state_root: &Path) -> SessionLeaseStore {
    SessionLeaseStore {
        files: FileLeaseStore::new(state_root.join("leases")),
        state_root: state_root.to_owned(),
    }
}

/// A file lease store whose session leases clean up after the sessions they guarded.
///
/// Every lease file stays on disk after its lease is released, so without this one file stayed
/// behind for every session ever deleted, and for every address an open looked at and found
/// empty: 390,660 files on one computer by November 2026. A session's lease taken here removes
/// its file as it is released, while it still holds the lock, when the session store has no head
/// for the session in any of its roots. A removed file only resets the lease's epoch, which broca
/// stamps on its writes for diagnosis and never compares. Every broca process that takes these
/// leases must use a `cortexkit-lease` that checks the locked file is still the one on disk, as
/// 0.1.8 does, or a waiting acquirer could lock the removed file.
pub struct SessionLeaseStore {
    files: FileLeaseStore,
    state_root: PathBuf,
}

impl LeaseStore for SessionLeaseStore {
    fn acquire(&self, key: &LeaseKey) -> Result<Box<dyn LeaseHandle>, LeaseError> {
        let held = self.files.acquire(key)?;
        if (key.module_id.as_str(), key.backend.as_str()) != SESSION_LEASE {
            return Ok(held);
        }
        Ok(Box::new(SessionLease {
            held: Some(held),
            state_root: self.state_root.clone(),
            addr: crate::session_refs::joined_triple_addr(&key.scope_key),
        }))
    }

    fn acquire_shared(&self, key: &LeaseKey) -> Result<Box<dyn LeaseHandle>, LeaseError> {
        self.files.acquire_shared(key)
    }
}

/// A session's exclusive lease from [`SessionLeaseStore`]: on release it removes its file when
/// no session is stored at `addr`, and otherwise only lets go.
#[derive(Debug)]
struct SessionLease {
    held: Option<Box<dyn LeaseHandle>>,
    state_root: PathBuf,
    addr: String,
}

impl SessionLease {
    fn held(&self) -> &dyn LeaseHandle {
        self.held
            .as_deref()
            .expect("a session lease holds its file until it is released")
    }
}

impl LeaseHandle for SessionLease {
    fn epoch(&self) -> u64 {
        self.held().epoch()
    }

    fn key(&self) -> &LeaseKey {
        self.held().key()
    }

    fn raise_epoch(&self, at_least: u64) -> Result<u64, LeaseError> {
        self.held().raise_epoch(at_least)
    }

    fn remove(mut self: Box<Self>) -> Result<(), LeaseError> {
        match self.held.take() {
            Some(held) => held.remove(),
            None => Ok(()),
        }
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        let Some(held) = self.held.take() else {
            return;
        };
        // A head in any root, or one that can't be read, keeps the file: only a session that is
        // provably gone lets it go.
        match broca_records::session_storage::session_storage_class(&self.state_root, &self.addr) {
            Ok(None) => {}
            _ => return,
        }
        if let Err(error) = held.remove() {
            // The lock is released either way; the file stays, as it did before removal existed.
            tracing::debug!(target: "lease", session = %self.addr,
                "could not remove the lease file of a session that is gone: {error}");
        }
    }
}

/// Cut authority is serialized across daemon processes as well as tasks. Session
/// leases are acquired only after this lock, keeping the lock order acyclic.
pub fn acquire_cut(
    state_root: &std::path::Path,
    cut_id: &str,
) -> Result<Box<dyn LeaseHandle>, LeaseError> {
    FileLeaseStore::new(state_root.join("leases")).acquire(&LeaseKey::new(
        "broca",
        "agent-data-cut",
        cut_id,
    ))
}

/// Map a session's bind identity to the shared crate's namespaced lease key. The scope
/// is the full identity triple (project root, harness, session) so two sessions that
/// share a name but differ in project or harness never collide — exactly the
/// distinctness the file lease enforced before this crate existed.
///
/// The namespace is `broca`/`wal` and the scope joins the triple with U+001F, which
/// [`BindIdentity::validate_keyable`] refuses inside any field, so one identity never
/// aliases another's lock. Every lease ever taken used these bytes, so they never change.
pub fn lease_key_for(session: &BindIdentity) -> LeaseKey {
    LeaseKey::new(
        "broca",
        "wal",
        format!(
            "{}\u{1f}{}\u{1f}{}",
            session.project_root.display(),
            session.harness,
            session.session
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(root: &str, harness: &str, session: &str) -> BindIdentity {
        BindIdentity {
            project_root: root.into(),
            harness: harness.into(),
            session: session.into(),
        }
    }

    #[test]
    fn key_carries_module_and_backend() {
        let k = lease_key_for(&ident("/tmp/p", "runner", "s1"));
        assert_eq!(k.module_id, "broca");
        assert_eq!(k.backend, "wal");
    }

    #[test]
    fn distinct_identities_map_to_distinct_keys() {
        // The full triple must stay distinct: sessions sharing a name but differing in
        // project or harness must not collide (the distinctness the file lease enforced
        // before this crate, now carried by the scope key).
        let base = lease_key_for(&ident("/tmp/p", "runner", "s1"));
        assert_ne!(base, lease_key_for(&ident("/tmp/p", "runner", "s2")));
        assert_ne!(base, lease_key_for(&ident("/tmp/other", "runner", "s1")));
        assert_ne!(base, lease_key_for(&ident("/tmp/p", "pi", "s1")));
    }

    #[test]
    fn same_identity_maps_to_same_key() {
        // Deterministic: the same session always maps to the same lock across
        // processes/restarts.
        assert_eq!(
            lease_key_for(&ident("/tmp/p", "runner", "s1")),
            lease_key_for(&ident("/tmp/p", "runner", "s1"))
        );
    }

    /// A fresh state root for one test, under the owned scratch root.
    fn state_root(name: &str) -> PathBuf {
        let root = broca_test_support::scratch_root()
            .join(format!("core-session-leases-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("leases")).unwrap();
        root
    }

    fn lease_files(root: &Path) -> usize {
        std::fs::read_dir(root.join("leases")).unwrap().count()
    }

    /// Writes a head for `session` in the root of `class`, as the session store files one.
    fn store_head(
        root: &Path,
        class: broca_records::session_storage::StorageClass,
        session: &BindIdentity,
    ) {
        let head = broca_records::session_storage::session_storage_head(
            root,
            class,
            &crate::session_addr(session),
        );
        std::fs::create_dir_all(head.parent().unwrap()).unwrap();
        std::fs::write(head, b"head").unwrap();
    }

    /// A session lease released while no session is stored at its address removes its file;
    /// one released while the session's head exists in any root keeps it, as does one whose
    /// session has heads in two roots, which is damage no lease decides.
    #[test]
    fn a_session_lease_removes_its_file_only_once_the_session_is_gone() {
        use broca_records::session_storage::StorageClass;
        let root = state_root("gone");
        let leases = session_leases(&root);
        let gone = ident("/tmp/p", "runner", "gone");
        drop(leases.acquire(&lease_key_for(&gone)).unwrap());
        assert_eq!(lease_files(&root), 0, "no session: the file is removed");

        let stored = ident("/tmp/p", "runner", "stored");
        store_head(&root, StorageClass::Durable, &stored);
        let held = leases.acquire(&lease_key_for(&stored)).unwrap();
        assert_eq!(held.key(), &lease_key_for(&stored));
        drop(held);
        assert_eq!(lease_files(&root), 1, "a stored session keeps its file");

        let damaged = ident("/tmp/p", "runner", "damaged");
        store_head(&root, StorageClass::Ordinary, &damaged);
        store_head(&root, StorageClass::Ephemeral, &damaged);
        drop(leases.acquire(&lease_key_for(&damaged)).unwrap());
        assert_eq!(
            lease_files(&root),
            2,
            "a session in two roots keeps its file"
        );

        // The kept file is taken again with the epoch it holds, and still guards one writer.
        let again = leases.acquire(&lease_key_for(&stored)).unwrap();
        assert!(again.epoch() > 1, "the kept file's epoch carries on");
        assert!(matches!(
            leases.acquire(&lease_key_for(&stored)),
            Err(LeaseError::Held { .. })
        ));
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The lease file a session's lease takes is the one [`session_lease_file`] names.
    #[test]
    fn the_session_lease_file_is_the_one_the_store_takes() {
        use broca_records::session_storage::StorageClass;
        let root = state_root("file");
        let session = ident("/srv/project", "runner", "s1");
        store_head(&root, StorageClass::Ordinary, &session);
        drop(
            session_leases(&root)
                .acquire(&lease_key_for(&session))
                .unwrap(),
        );
        let files: Vec<String> = std::fs::read_dir(root.join("leases"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(files, vec![session_lease_file(&session)]);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A lease that is not a session's, such as a cut's, is left as the file store leaves it.
    #[test]
    fn other_leases_keep_their_files() {
        let root = state_root("other");
        let leases = session_leases(&root);
        drop(
            leases
                .acquire(&LeaseKey::new("broca", "agent-data-cut", "cut-1"))
                .unwrap(),
        );
        assert_eq!(lease_files(&root), 1);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_key_bytes_are_pinned() {
        let key = lease_key_for(&ident("/srv/project", "runner", "s1"));
        assert_eq!(key.module_id, "broca");
        assert_eq!(key.backend, "wal");
        assert_eq!(key.scope_key.as_bytes(), b"/srv/project\x1frunner\x1fs1");
    }
}
