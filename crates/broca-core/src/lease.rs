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

use std::path::{Path, PathBuf};

use broca_protocol::BindIdentity;
use cortexkit_lease::LeaseKey;

pub use cortexkit_lease::{FileLeaseStore, LeaseError, LeaseHandle, LeaseStore};
// The canonical key derivation lives in `broca-wal`'s tenure module.
// This module re-exports its key shape and tenure refusal so the derivation
// cannot drift between the two spellings; the byte contract is pinned below.
pub use broca_wal::writer_tenure::{TenureError, WalLeaseKey};

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
/// The bytes come from the canonical derivation in `broca-wal`'s tenure module:
/// this wrapper only converts that identical triple into the shared lease crate's
/// key type. Returning `WalLeaseKey` directly would require `broca-wal` to
/// depend on `cortexkit-lease`, a manifest change not made here, so the
/// conversion stays in this wrapper.
pub fn lease_key_for(session: &BindIdentity) -> LeaseKey {
    let canonical = broca_wal::writer_tenure::lease_key_for(session);
    LeaseKey::new(canonical.module_id, canonical.backend, canonical.scope_key)
}

/// Proof that a live session lease acquisition owns the writer for one session.
///
/// Holds the real operating-system lock handle, so dropping it releases the
/// lease. The type is opaque (fields private) and non-`Clone`, with no public
/// constructor, no `From<u64>`, and no handle-accepting factory: the only mint
/// path is [`WalLeaseDomain::acquire`], which locks through the shared backend
/// and binds the full session identity plus the lease root it acquired under.
/// A production caller cannot manufacture authority from a bare fence integer,
/// another session's lease, a borrowed handle, or a path string.
///
/// This is the core-side owner the writer task retains until the WAL is closed.
/// It mirrors `broca-wal`'s test-only tenure (identity plus epoch, no handle)
/// but carries the live lock: the two will unify when `broca-wal` gains the
/// shared lease dependency and mints this same owner directly. Until then the
/// store opener matches the carried root against its configured domain.
///
/// ```compile_fail,E0599
/// fn must_stay_unique(tenure: broca_core::lease::OwnedWalTenure) {
///     let _copy = tenure.clone();
/// }
/// ```
///
/// ```compile_fail,E0277
/// fn no_fence_conversion() {
///     let _tenure: broca_core::lease::OwnedWalTenure = 1u64.into();
/// }
/// ```
#[derive(Debug)]
pub struct OwnedWalTenure {
    lease: Box<dyn LeaseHandle>,
    session: BindIdentity,
    lease_root: PathBuf,
}

impl OwnedWalTenure {
    /// The session this tenure was acquired for.
    pub fn session(&self) -> &BindIdentity {
        &self.session
    }

    /// The lease directory the tenure was acquired under.
    pub fn lease_root(&self) -> &Path {
        &self.lease_root
    }

    /// The acquisition's epoch stamp: the value the writer encodes in its frame
    /// fence field. Reading it is inspection only; presenting any other integer
    /// never creates authority, and comparing it never substitutes for holding
    /// this tenure.
    pub fn epoch(&self) -> u64 {
        self.lease.epoch()
    }

    /// Refuse use under any session but the bound one. A tenure acquired for
    /// session A must never authorize writes for session B, so every entry
    /// point that consumes a tenure re-checks the binding before touching I/O.
    pub fn require_session(&self, expected: &BindIdentity) -> Result<(), TenureError> {
        if &self.session == expected {
            Ok(())
        } else {
            let describe = |identity: &BindIdentity| {
                format!(
                    "session '{}' harness '{}' root '{}'",
                    identity.session,
                    identity.harness,
                    identity.project_root.display()
                )
            };
            Err(TenureError::SessionMismatch {
                expected: describe(expected),
                found: describe(&self.session),
            })
        }
    }

    /// Move the live lock out. Only the writer-open path calls this, after the
    /// session binding check, so authority flows from the consumed tenure into
    /// the blocking recovery closure and then into the writer task.
    pub(crate) fn into_lease(self) -> Box<dyn LeaseHandle> {
        self.lease
    }
}

/// The trusted lease-acquisition boundary for one state root.
///
/// Owns the lease directory (`<state_root>/leases`, the same directory the
/// production lease store acquires in) and is the only type allowed to mint an
/// [`OwnedWalTenure`]. Callers never supply a lease handle or a fence integer:
/// authority always flows from a real acquisition, never from caller values.
#[derive(Debug, Clone)]
pub struct WalLeaseDomain {
    lease_root: PathBuf,
}

impl WalLeaseDomain {
    /// Configure the boundary for a state root's lease directory.
    pub fn new(lease_root: PathBuf) -> Self {
        Self { lease_root }
    }

    /// The lease directory this boundary acquires in.
    pub fn lease_root(&self) -> &Path {
        &self.lease_root
    }

    /// Lock the session's lease and mint its tenure. Refuses ambiguous
    /// identities (whose U+001F separator would alias another session's lock)
    /// and fails with `Held` while a live writer holds the session.
    pub fn acquire(&self, session: &BindIdentity) -> Result<OwnedWalTenure, LeaseError> {
        session.validate_keyable().map_err(|reason| {
            LeaseError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                reason,
            ))
        })?;
        let key = lease_key_for(session);
        let lease = FileLeaseStore::new(&self.lease_root).acquire(&key)?;
        Ok(OwnedWalTenure {
            lease,
            session: session.clone(),
            lease_root: self.lease_root.clone(),
        })
    }
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

    #[test]
    fn key_bytes_match_the_canonical_wal_derivation() {
        // The shared bytes must never drift: the core key is built from the
        // canonical broca-wal derivation, field for field.
        let session = ident("/srv/project", "runner", "s1");
        let core_key = lease_key_for(&session);
        let canonical = broca_wal::writer_tenure::lease_key_for(&session);
        assert_eq!(core_key.module_id, canonical.module_id);
        assert_eq!(core_key.backend, canonical.backend);
        assert_eq!(core_key.scope_key, canonical.scope_key);
    }

    #[test]
    fn domain_acquire_refuses_ambiguous_identity() {
        let domain = WalLeaseDomain::new(broca_test_support::scratch_root().join("leases"));
        let ambiguous = BindIdentity {
            project_root: "/tmp/project".into(),
            harness: "runner".into(),
            session: "s1".into(),
        };
        // Build the ambiguous identity without the struct literal helper so the
        // separator is explicit at the call site.
        let ambiguous = BindIdentity::new(
            ambiguous.project_root.clone(),
            format!("run{}ner", '\u{1f}'),
            ambiguous.session.clone(),
        );
        let error = domain
            .acquire(&ambiguous)
            .expect_err("ambiguous identity must not mint");
        assert!(
            matches!(error, LeaseError::Io(_)),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn tenure_rejects_wrong_session() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-tenure-bind-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let domain = WalLeaseDomain::new(root.join("leases"));
        let session = ident("/tmp/p", "runner", "s1");
        let tenure = domain.acquire(&session).expect("keyable identity mints");
        assert!(tenure.require_session(&session).is_ok());
        let error = tenure
            .require_session(&ident("/tmp/p", "runner", "s2"))
            .expect_err("other session must be refused");
        assert!(
            matches!(error, TenureError::SessionMismatch { .. }),
            "unexpected error: {error:?}"
        );
        drop(tenure);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn tenure_reports_bound_identity_root_and_epoch() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-tenure-meta-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let domain = WalLeaseDomain::new(root.join("leases"));
        let session = ident("/tmp/p", "runner", "s1");
        let tenure = domain.acquire(&session).expect("mints");
        assert_eq!(tenure.session(), &session);
        assert_eq!(tenure.lease_root(), &root.join("leases"));
        assert!(tenure.epoch() >= 1);
        drop(tenure);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn tenure_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<OwnedWalTenure>();
    }
}
