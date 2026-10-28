//! Opaque writer tenure: proof that a session lease acquisition owns the WAL.
//!
//! A writable log exists only while its session lease is held. This module defines
//! the tenure type for lease-aware writer open: an opaque, non-cloneable
//! [`OwnedWalTenure`] that binds one session identity to the lease root it was
//! acquired under, plus the namespaced lease-key derivation every acquisition
//! must use. Only the trusted lease-acquisition boundary mints tenures: the type
//! has no public constructor, no conversion from a raw or borrowed lease handle,
//! and no conversion from a bare fence integer. The single mint path lives on
//! [`WalLeaseDomain`], which owns the lease root.
//!
//! What this module deliberately leaves out:
//!
//! * Real lock acquisition stays with the existing lease backend. [`WalLeaseDomain`]
//!   owns the lease root but has no `acquire` method yet: acquiring the
//!   operating-system lock needs the shared lease crate as a dependency of
//!   `broca-wal`, which is a manifest change outside this change. The consuming
//!   writer-open entry points that take a tenure (`FileWal`, the store) are
//!   deferred to later work.
//! * The key derivation here duplicates the one in `broca-core`'s lease module.
//!   Keep the two byte-identical until they are merged into one shared function.
//!
//! No WAL bytes or log features change here: this module adds an in-memory
//! capability only. Nothing in the crate mints a tenure outside tests yet, so
//! production behaviour is unchanged.

use std::path::{Path, PathBuf};

use broca_protocol::BindIdentity;

/// Module half of the lease namespace: broca's locks, never another module's.
const LEASE_MODULE_ID: &str = "broca";

/// Backend half of the lease namespace: the per-session write-ahead log. Each
/// append carries the tenure's epoch as a forensic stamp; the stamp alone never
/// authorizes an append.
const LEASE_BACKEND: &str = "wal";

/// Field separator inside the scope key. [`BindIdentity::validate_keyable`]
/// rejects this separator inside any field, so one identity can never silently
/// alias another identity's lock.
const KEY_SEPARATOR: char = '\u{1f}';

/// The namespaced lease-key triple, field for field: the owning module, the
/// guarded backend, and the full session scope. This mirrors the shared lease
/// crate's key shape without depending on it (`broca-wal` does not depend on the
/// shared lease crate; that dependency is deferred to later work); compare
/// [`lease_key_for`] for the byte contract.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WalLeaseKey {
    /// Which module owns the lock (`"broca"`).
    pub module_id: String,
    /// Which backend the lock guards (`"wal"`).
    pub backend: String,
    /// The full session scope: project root, harness and session joined with
    /// U+001F, exactly as the existing derivation builds it.
    pub scope_key: String,
}

/// Map a session identity to its namespaced lease key.
///
/// This is the existing derivation, unchanged: the `broca`/`wal` namespace and
/// the U+001F-joined `(project_root, harness, session)` scope. The key bytes are
/// identical to today's for every input, including ambiguous ones; refusing an
/// ambiguous identity is the mint boundary's job ([`WalLeaseDomain`]), not this
/// pure function's, so the bytes can never drift between the two spellings.
/// Keep byte-identical with `broca-core`'s `lease_key_for` until the two are
/// merged.
pub fn lease_key_for(session: &BindIdentity) -> WalLeaseKey {
    WalLeaseKey {
        module_id: LEASE_MODULE_ID.to_owned(),
        backend: LEASE_BACKEND.to_owned(),
        scope_key: format!(
            "{}{}{}{}{}",
            session.project_root.display(),
            KEY_SEPARATOR,
            session.harness,
            KEY_SEPARATOR,
            session.session
        ),
    }
}

/// The trusted lease-acquisition boundary for one state root.
///
/// The domain owns the lease root (`<state_root>/leases`, the same directory
/// the production lease store acquires in) and is the only type allowed to mint
/// an [`OwnedWalTenure`]. Callers never supply a lease handle or a fence
/// integer: later work adds an `acquire` method that locks through the shared
/// backend internally and returns the tenure, so authority always flows from a
/// real acquisition, never from caller-provided values. That method is absent
/// here because this change cannot touch the manifest.
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

    /// Mint a tenure for tests. Validates the identity, then binds it to this
    /// domain's lease root with the given epoch stamp.
    ///
    /// Production minting (real lock acquisition) is deferred to later work;
    /// this crate-internal constructor exists so the tenure's guards are testable
    /// before the backend dependency lands. It is not reachable from other
    /// crates, so no production caller can manufacture authority through it.
    #[cfg(test)]
    pub(crate) fn mint_for_test(
        &self,
        session: &BindIdentity,
        epoch: u64,
    ) -> Result<OwnedWalTenure, TenureError> {
        session
            .validate_keyable()
            .map_err(TenureError::AmbiguousIdentity)?;
        Ok(OwnedWalTenure {
            session: session.clone(),
            lease_root: self.lease_root.clone(),
            epoch,
        })
    }
}

/// Proof that a live session lease acquisition owns the writer for one session.
///
/// The tenure binds the session identity to the lease root it was acquired
/// under and carries the epoch stamp the writer encodes in its frame fence
/// field. It is opaque (all fields private) and non-`Clone`, and there is no
/// public constructor, `From` impl, or builder that accepts a raw or borrowed
/// lease handle or a bare `u64` fence:
///
/// ```compile_fail,E0599
/// fn must_stay_unique(tenure: broca_wal::writer_tenure::OwnedWalTenure) {
///     let _copy = tenure.clone();
/// }
/// ```
///
/// ```compile_fail,E0277
/// fn no_fence_conversion() {
///     let _tenure: broca_wal::writer_tenure::OwnedWalTenure = 1u64.into();
/// }
/// ```
///
/// ```compile_fail,E0599
/// fn no_epoch_constructor() {
///     let _tenure =
///         broca_wal::writer_tenure::OwnedWalTenure::from_epoch(1u64);
/// }
/// ```
///
/// ```compile_fail,E0599
/// fn no_borrowed_handle_constructor(
///     tenure: broca_wal::writer_tenure::OwnedWalTenure,
/// ) {
///     let _tenure =
///         broca_wal::writer_tenure::OwnedWalTenure::from_borrowed(&tenure);
/// }
/// ```
///
/// Reading the bound identity out is allowed; extracting anything that could
/// release the lease while the WAL stays writable is not, because this type
/// never holds a release handle in the first place. Field access stays private
/// so callers go through the named accessors:
///
/// ```compile_fail,E0616
/// fn no_field_access(tenure: broca_wal::writer_tenure::OwnedWalTenure) {
///     let _epoch = tenure.epoch;
/// }
/// ```
#[derive(Debug)]
pub struct OwnedWalTenure {
    session: BindIdentity,
    lease_root: PathBuf,
    epoch: u64,
}

impl OwnedWalTenure {
    /// The session this tenure was acquired for.
    pub fn session(&self) -> &BindIdentity {
        &self.session
    }

    /// The lease root the tenure was acquired under.
    pub fn lease_root(&self) -> &Path {
        &self.lease_root
    }

    /// The acquisition's epoch stamp: the value the writer encodes in its frame
    /// fence field. Reading it is inspection only; presenting any other integer
    /// never creates authority, and comparing it never substitutes for holding
    /// this tenure.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Refuse use under any session but the bound one. A tenure acquired for
    /// session A must never authorize writes for session B, so every entry
    /// point that consumes a tenure re-checks the binding before touching I/O.
    pub fn require_session(&self, expected: &BindIdentity) -> Result<(), TenureError> {
        if &self.session == expected {
            Ok(())
        } else {
            Err(TenureError::SessionMismatch {
                expected: describe(expected),
                found: describe(&self.session),
            })
        }
    }
}

/// One-line identity rendering for refusal messages: harness, session and root.
fn describe(identity: &BindIdentity) -> String {
    format!(
        "session '{}' harness '{}' root '{}'",
        identity.session,
        identity.harness,
        identity.project_root.display()
    )
}

/// Why tenure minting or binding validation refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenureError {
    /// The identity contains the U+001F key separator, so its key would alias
    /// another session's lock. Carries the validator's message naming the field.
    AmbiguousIdentity(String),
    /// The tenure is bound to a different session than the one requesting use.
    SessionMismatch {
        /// The session the caller asked to act as.
        expected: String,
        /// The session the tenure was acquired for.
        found: String,
    },
}

impl std::fmt::Display for TenureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TenureError::AmbiguousIdentity(reason) => {
                write!(f, "cannot mint writer tenure: {reason}")
            }
            TenureError::SessionMismatch { expected, found } => {
                write!(f, "writer tenure is bound to {found}, not {expected}")
            }
        }
    }
}

impl std::error::Error for TenureError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(root: &str, harness: &str, session: &str) -> BindIdentity {
        BindIdentity::new(root, harness, session)
    }

    fn domain() -> WalLeaseDomain {
        WalLeaseDomain::new(PathBuf::from("/state/leases"))
    }

    #[test]
    fn lease_key_uses_the_broca_wal_namespace_and_unit_separator_scope() {
        let key = lease_key_for(&identity("/srv/project", "runner", "s1"));

        assert_eq!(key.module_id, "broca");
        assert_eq!(key.backend, "wal");
        assert_eq!(key.scope_key, "/srv/project\u{1f}runner\u{1f}s1");
        // Pin the exact bytes, separators included: the lock a tenure guards
        // must be the same lock today's derivation guards, byte for byte.
        assert_eq!(key.scope_key.as_bytes(), b"/srv/project\x1frunner\x1fs1");
    }

    #[test]
    fn distinct_identities_map_to_distinct_keys() {
        // Sessions sharing a name but differing in project or harness must
        // never collide: the scope carries the full identity triple.
        let base = lease_key_for(&identity("/srv/project", "runner", "s1"));
        assert_ne!(
            base,
            lease_key_for(&identity("/srv/project", "runner", "s2"))
        );
        assert_ne!(base, lease_key_for(&identity("/srv/other", "runner", "s1")));
        assert_ne!(base, lease_key_for(&identity("/srv/project", "pi", "s1")));
    }

    #[test]
    fn same_identity_maps_to_same_key() {
        // Deterministic: the same session always maps to the same lock,
        // across processes and restarts.
        assert_eq!(
            lease_key_for(&identity("/srv/project", "runner", "s1")),
            lease_key_for(&identity("/srv/project", "runner", "s1"))
        );
    }

    #[test]
    fn mint_refuses_ambiguous_identity() {
        // An identity containing the U+001F separator would alias another
        // session's lock, so minting refuses instead of manufacturing a key
        // that collides.
        let ambiguous = identity("/srv/project", "run\u{1f}ner", "s1");
        let error = domain()
            .mint_for_test(&ambiguous, 7)
            .expect_err("ambiguous identity must not mint");
        assert!(
            matches!(error, TenureError::AmbiguousIdentity(_)),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn tenure_rejects_wrong_session() {
        let tenure = domain()
            .mint_for_test(&identity("/srv/project", "runner", "s1"), 7)
            .expect("keyable identity mints");
        assert!(
            tenure
                .require_session(&identity("/srv/project", "runner", "s1"))
                .is_ok(),
            "bound session must be accepted"
        );
        let error = tenure
            .require_session(&identity("/srv/project", "runner", "s2"))
            .expect_err("other session must be refused");
        assert!(
            matches!(error, TenureError::SessionMismatch { .. }),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn tenure_reports_bound_identity_root_and_epoch() {
        let session = identity("/srv/project", "runner", "s1");
        let tenure = domain().mint_for_test(&session, 41).expect("mints");

        assert_eq!(tenure.session(), &session);
        assert_eq!(tenure.lease_root(), Path::new("/state/leases"));
        assert_eq!(tenure.epoch(), 41);
    }

    #[test]
    fn tenure_is_send_and_sync() {
        // The writer task will own the tenure across a blocking thread
        // boundary, so the type must stay transferable and shareable.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<OwnedWalTenure>();
    }
}
