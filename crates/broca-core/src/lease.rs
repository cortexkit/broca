//! The durable single-writer lease, provided by the shared `cortexkit-lease` crate.
//!
//! A multi-session daemon must never have two live writers on one session's WAL, so
//! `start_run` acquires this lease and FAILS if a live writer holds it. The lease's
//! monotonic `epoch` is the lease-tenure stamp every WAL append carries for forensic
//! correlation. It is not compared on the append path and does not reject a stale stamp;
//! single-writer safety comes from the OS lock plus the WAL file-integrity check.
//!
//! The mechanics (OS advisory lock for liveness + persisted epoch stamp) live in
//! `cortexkit-lease`, shared across CortexKit modules. This module is only the seam
//! that maps broca's session identity (`BindIdentity`) to the shared crate's
//! namespaced `LeaseKey` — kept in ONE place so the key derivation can't drift.

use broca_protocol::BindIdentity;
use cortexkit_lease::LeaseKey;

pub use cortexkit_lease::{FileLeaseStore, LeaseError, LeaseHandle, LeaseStore};

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

/// This module's id in the shared lease namespace. The shared lease root is shared
/// across modules, so the id keeps broca's locks distinct from any other module's.
const MODULE_ID: &str = "broca";

/// The storage the lease guards: the per-session write-ahead log (each append carries the
/// epoch as a forensic lease-tenure stamp).
const BACKEND: &str = "wal";

/// Map a session's bind identity to the shared crate's namespaced lease key. The scope
/// is the full identity triple (project root, harness, session) so two sessions that
/// share a name but differ in project or harness never collide — exactly the
/// distinctness the file lease enforced before this crate existed.
pub fn lease_key_for(session: &BindIdentity) -> LeaseKey {
    let scope = format!(
        "{}\u{1f}{}\u{1f}{}",
        session.project_root.display(),
        session.harness,
        session.session
    );
    LeaseKey::new(MODULE_ID, BACKEND, scope)
}

pub(crate) enum LeaseProbe {
    Held,
    Free(Option<std::fs::File>),
}

/// Preview maintenance must not create a lease or change its epoch. The pinned
/// lease crate exposes no read-only probe, so open its stable FNV-1a filename
/// without creation and hold a shared OS lock. Tests compare this with a real
/// FileLeaseStore writer to detect any dependency addressing drift.
pub(crate) fn probe_writer(
    state_root: &std::path::Path,
    session: &BindIdentity,
) -> std::io::Result<LeaseProbe> {
    let key = lease_key_for(session);
    let identity = format!(
        "{}\u{1f}{}\u{1f}{}",
        key.module_id, key.backend, key.scope_key
    );
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in identity.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let path = state_root.join("leases").join(format!("{hash:016x}.lease"));
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LeaseProbe::Free(None))
        }
        Err(error) => return Err(error),
    };
    match file.try_lock_shared() {
        Ok(()) => Ok(LeaseProbe::Free(Some(file))),
        Err(std::fs::TryLockError::WouldBlock) => Ok(LeaseProbe::Held),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

/// Hold for the entire mutating fold, resume, or unfold operation. Report-only
/// callers must not acquire this lease. The OS lock releases on process death;
/// the persisted epoch is forensic metadata, not a TTL or a stale-file lock.
///
/// This is the premise of archive::read_newest_member's start-stamp precedence,
/// not operator hygiene: A stalls, B snapshots X, a writer appends Y, then A
/// snapshots XY and unlinks. Without serialization B's newer stamp serves X.
/// Unsealed resume must also take a fresh stamp, since locks do not span crashes.
pub fn acquire_archive_pass(
    state_root: &std::path::Path,
) -> Result<Box<dyn LeaseHandle>, LeaseError> {
    FileLeaseStore::new(state_root.join("leases")).acquire(&LeaseKey::new(
        "broca",
        "wal-archive",
        "fold-pass",
    ))
}

/// Diagnostic path for the fixed archive-pass key. The shared crate keeps its
/// path derivation private; this is its FNV-1a filename for
/// `broca<US>wal-archive<US>fold-pass`, not a second locking implementation.
/// The fold contention test verifies that the held lease actually exists here.
pub fn archive_pass_lock_path(state_root: &std::path::Path) -> std::path::PathBuf {
    state_root.join("leases/978f70b3270ef8da.lease")
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
    fn archive_pass_lease_excludes_other_operators_and_releases_on_drop() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-fold-lock-{}", std::process::id()));
        let first = acquire_archive_pass(&root).unwrap();
        assert!(matches!(
            acquire_archive_pass(&root),
            Err(LeaseError::Held { .. })
        ));
        drop(first);
        drop(acquire_archive_pass(&root).unwrap());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn key_carries_module_and_backend() {
        let k = lease_key_for(&ident("/tmp/p", "runner", "s1"));
        assert_eq!(k.module_id, MODULE_ID);
        assert_eq!(k.backend, BACKEND);
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
}
