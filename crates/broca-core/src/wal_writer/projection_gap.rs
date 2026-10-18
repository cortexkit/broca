//! A content-free durable gap marker keeps SQLite outages off the WAL path.
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
    pub(super) fn begin(root: &Path) -> io::Result<Self> {
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
        broca_wal::migration::sync_dir(&root)?;
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
    /// The persisted lease tenure detects overlapping fallback writers across
    /// processes as well as threads. Only the immediately following tenure may
    /// unlink the marker; the lease is not held for the census duration.
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
        broca_wal::migration::sync_dir(&self.root)?;
        Ok(true)
    }
}
