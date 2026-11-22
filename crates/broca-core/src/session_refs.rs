//! Names for a session that other stores and modules key on: its address (the folder the
//! session store keeps it under), the ref agent moves name it by between computers
//! (`agent-data/v1`), and an older ref derived from the session's triple, which old handles and
//! the markers a move leaves on a session (frozen while it moves, released once it left) still
//! use. Also the session-ref index's write boundary, with the durable marker that keeps a
//! session admitted while the index can't take its row.
use broca_protocol::BindIdentity;
use cortexkit_lease::{FileLeaseStore, LeaseError, LeaseHandle, LeaseKey, LeaseStore};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub const SESSION_PROJECTION_INCOMPLETE: &str = "session-ref-projection.incomplete";

fn canonical_root(root: &Path) -> io::Result<PathBuf> {
    if root.as_os_str().is_empty() {
        Path::new(".")
    } else {
        root
    }
    .canonicalize()
}
fn acquire(root: &Path) -> Result<Box<dyn LeaseHandle>, LeaseError> {
    FileLeaseStore::new(root).acquire(&LeaseKey::new("broca", "session-ref-projection", "gap"))
}
fn lease_error(error: LeaseError) -> io::Error {
    io::Error::other(error.to_string())
}

pub fn session_projection_incomplete(root: &Path) -> io::Result<bool> {
    root.join(SESSION_PROJECTION_INCOMPLETE).try_exists()
}

pub struct ProjectionAdmission {
    _lease: Box<dyn LeaseHandle>,
}
impl ProjectionAdmission {
    pub(crate) fn begin(root: &Path) -> io::Result<Self> {
        let root = canonical_root(root)?;
        let lease = loop {
            match acquire(&root) {
                Ok(lease) => break lease,
                // Repair holds this lease only for its short snapshot/unlink
                // barriers, never while reading WALs or waiting for SQLite.
                Err(LeaseError::Held { .. }) => {
                    std::thread::sleep(std::time::Duration::from_millis(1))
                }
                Err(error) => return Err(lease_error(error)),
            }
        };
        let file = broca_owner_only::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(root.join(SESSION_PROJECTION_INCOMPLETE))?;
        // Repeat both barriers even for an existing marker: a concurrent creator
        // may not have made its directory entry durable before this append.
        file.sync_all()?;
        broca_records::move_fence::sync_dir(&root)?;
        Ok(Self { _lease: lease })
    }
}

pub struct ProjectionRepair {
    root: PathBuf,
    tenure: u64,
}
impl ProjectionRepair {
    pub fn begin(root: &Path) -> io::Result<Option<Self>> {
        let root = canonical_root(root)?;
        match acquire(&root) {
            Ok(lease) => Ok(Some(Self {
                root,
                tenure: lease.epoch(),
            })),
            Err(LeaseError::Held { .. }) => Ok(None),
            Err(error) => Err(lease_error(error)),
        }
    }
    /// The lease's epoch, recorded when the repair began, shows whether another writer of the
    /// marker took the lease in between, in this process or another. Only the tenure right after
    /// the repair's own may unlink the marker; the lease is not held while the census runs.
    pub fn finish(self) -> io::Result<bool> {
        let lease = match acquire(&self.root) {
            Ok(lease) => lease,
            Err(LeaseError::Held { .. }) => return Ok(false),
            Err(error) => return Err(lease_error(error)),
        };
        if self.tenure.checked_add(1) != Some(lease.epoch()) {
            return Ok(false);
        }
        let path = self.root.join(SESSION_PROJECTION_INCOMPLETE);
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        broca_records::move_fence::sync_dir(&self.root)?;
        Ok(true)
    }
}

/// A session's address: FNV-1a 64 over its bind triple joined by U+001F, as 16 hex digits. It
/// names the session's folders in the session store, so changing any part of the triple
/// addresses a different session. Every store computes it here, so a log line, a folder and an
/// index row always agree.
pub fn session_addr(session: &BindIdentity) -> String {
    let key = format!(
        "{}\u{1f}{}\u{1f}{}",
        session.project_root.display(),
        session.harness,
        session.session
    );
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Writes `session`'s row in the session-ref index under `address`. When the index can't take
/// it, a durable marker under `root` (broca's state root) keeps the index marked incomplete
/// until a census (a pass that lists every stored session and records the missing rows)
/// repairs it, so a move asking meanwhile is told to retry rather than that the ref is
/// unknown; the returned admission holds that marker.
pub fn record_candidate(
    index: Option<&dyn SessionRefIndex>,
    root: &Path,
    address: &str,
    session: &BindIdentity,
) -> Result<Option<ProjectionAdmission>, String> {
    if let Some(index) = index {
        if let Err(error) = index.record_session(address, session) {
            let admission = ProjectionAdmission::begin(root)
                .map_err(|marker| format!("session-ref projection failed ({error}); cannot persist incomplete marker: {marker}"))?;
            tracing::warn!(%error, "session-ref index unavailable; a durable incomplete marker keeps the session admitted");
            return Ok(Some(admission));
        }
    }
    Ok(None)
}

pub fn agent_session_ref(session: &BindIdentity) -> String {
    cortexkit_role_llm_runner::session_ref(&cortexkit_role_llm_runner::SessionIdentity {
        runner: "broca",
        harness: &session.harness,
        session: &session.session,
    })
}

/// The older ref derived from a session's triple, kept for old handles and the move markers.
/// It is a fixed function of the triple: it must not change when the `agent-data/v1` ref's
/// derivation is versioned, or an old handle or marker would stop naming its session.
pub fn legacy_session_ref(harness: &str, project_root: &str, session: &str) -> String {
    use sha2::{Digest, Sha256};
    let bytes = format!(
        r#"{{"harness":{},"project_root":{},"session":{}}}"#,
        serde_json::to_string(harness).unwrap(),
        serde_json::to_string(project_root).unwrap(),
        serde_json::to_string(session).unwrap(),
    );
    Sha256::digest(bytes.as_bytes())[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Where a new session's ref is recorded before its first record is written. When the index
/// can't take it, a durable gap marker keeps the session admitted (see [`record_candidate`]).
pub trait SessionRefIndex: Send + Sync {
    fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The expected value was computed by a separate Python implementation of FNV-1a, not by
    /// anything in this crate, so a coordinated change to the function and a helper sharing its
    /// constants still fails here.
    #[test]
    fn session_addr_is_pinned_to_an_independently_computed_digest() {
        let session = BindIdentity::new("/tmp/p", "runner", "s1");
        assert_eq!(session_addr(&session), "31a7840439280f40");
    }
}
