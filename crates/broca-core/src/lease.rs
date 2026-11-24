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

    #[test]
    fn the_key_bytes_are_pinned() {
        let key = lease_key_for(&ident("/srv/project", "runner", "s1"));
        assert_eq!(key.module_id, "broca");
        assert_eq!(key.backend, "wal");
        assert_eq!(key.scope_key.as_bytes(), b"/srv/project\x1frunner\x1fs1");
    }
}
