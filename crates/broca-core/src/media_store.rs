//! Content-addressed media, partitioned by the session WAL's durability class.
//!
//! Capture callers validate bytes first, then use `register_and_append` to keep
//! every storage barrier ahead of the WAL write. Rendering uses `read_verified`;
//! references are never a license to trust cached or previously read bytes.

use broca_protocol::{BindIdentity, MediaRef};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

mod lifecycle;
pub use lifecycle::{
    preview_sweep, session_media_digests, spawn_startup_sweep, sweep, SweepReport,
};

/// Stores deduplicate only within a WAL durability class, never across classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaStoreClass {
    Durable,
    NonDurable,
    Ephemeral,
}

impl MediaStoreClass {
    pub const fn directory(self) -> &'static str {
        match self {
            Self::Durable => "media-durable",
            Self::NonDurable => "media",
            Self::Ephemeral => "media-ephemeral",
        }
    }
}

/// Create even empty backup trees before publishing the backup catalog. Repeat
/// parent barriers on every open: an existing entry may be an interrupted create.
pub fn create_media_directories(state_root: &Path) -> io::Result<()> {
    for name in ["media-durable", "media", "media-ephemeral", "media-uploads"] {
        for child in ["objects", "refs"] {
            let path = state_root.join(name).join(child);
            broca_wal::migration::durable_dir(state_root, &path).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("create media directory {}: {error}", path.display()),
                )
            })?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct MediaStore {
    root: PathBuf,
}

impl MediaStore {
    /// Startup must have created the state root. This also pays creation barriers
    /// when a store is opened independently of the serving process.
    pub fn open(state_root: &Path, class: MediaStoreClass) -> io::Result<Self> {
        let root = state_root.join(class.directory());
        for child in ["objects", "refs"] {
            broca_wal::migration::durable_dir(state_root, &root.join(child))?;
        }
        Ok(Self { root })
    }

    /// The session actor must retain its lease for this handle's lifetime. A
    /// maintenance sweep uses the exclusive store lock and skips leased sessions.
    pub fn session(&self, identity: &BindIdentity) -> io::Result<MediaManifest> {
        let registration_lock = self.lock()?;
        registration_lock.lock_shared()?;
        // The WAL may not exist yet. Persist the identity so maintenance can
        // still check its writer lease before reclaiming an uncommitted capture.
        let address = crate::wal_writer::session_addr(identity);
        let identity_path = self.root.join("identities").join(address);
        broca_wal::migration::atomic_json(self.root.parent().unwrap(), &identity_path, identity)?;
        let path = self
            .root
            .join("refs")
            .join(crate::wal_writer::session_addr(identity));
        let file = broca_owner_only::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;
        Ok(MediaManifest {
            store: self.clone(),
            path,
            file,
            refs_synced: false,
        })
    }

    fn lock(&self) -> io::Result<File> {
        broca_owner_only::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.root.join("store.lock"))
    }

    /// Maintenance holds this lock across both manifest reconciliation and object
    /// deletion, so a registration cannot race either half of a sweep.
    pub fn with_exclusive_lock<T>(&self, action: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
        let lock = self.lock()?;
        lock.lock()?;
        action()
    }

    /// Read afresh for each render, failing closed with the offending digest.
    pub fn read_verified(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        if !valid_digest(&reference.sha256) {
            return Err(MediaReadError::Corrupt(reference.sha256.clone()));
        }
        let path = self.root.join("objects").join(&reference.sha256);
        let bytes = fs::read(&path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                MediaReadError::Missing(reference.sha256.clone())
            } else {
                MediaReadError::Io {
                    digest: reference.sha256.clone(),
                    source: error,
                }
            }
        })?;
        if !matches_bytes(reference, &bytes) {
            return Err(MediaReadError::Corrupt(reference.sha256.clone()));
        }
        Ok(bytes)
    }

    fn publish(
        &self,
        reference: &MediaRef,
        bytes: &[u8],
        steps: &mut impl StorageSteps,
    ) -> io::Result<()> {
        // Always republish from held bytes. Besides repairing a corrupt object,
        // this repeats the directory barrier after a previous failed publication.
        static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
        let directory = self.root.join("objects");
        let (temp, mut file) = loop {
            let temp = directory.join(format!(
                ".{}-{}",
                std::process::id(),
                NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
            ));
            match broca_owner_only::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)
            {
                Ok(file) => break (temp, file),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        };
        let result = (|| {
            file.write_all(bytes)?;
            steps.file_sync(&file, Step::ObjectFileSynced)?;
            drop(file);
            fs::rename(&temp, directory.join(&reference.sha256))?;
            steps.dir_sync(&directory, Step::ObjectPublished)?;
            self.read_verified(reference).map_err(io::Error::other)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(temp);
        }
        result
    }
}

/// One append-only digest manifest, addressed by the full bind triple.
pub struct MediaManifest {
    store: MediaStore,
    path: PathBuf,
    file: File,
    refs_synced: bool,
}

impl MediaManifest {
    /// The callback is the only WAL-write seam: it is never entered on a failed
    /// storage barrier. Keep the session lease until its WAL is durable. A failed
    /// callback may leave an extra manifest entry; the WAL remains authoritative.
    pub fn register_and_append<T>(
        &mut self,
        reference: &MediaRef,
        bytes: &[u8],
        append: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<T> {
        self.register_with(reference, bytes, append, &mut RealSteps)
    }

    fn register_with<T>(
        &mut self,
        reference: &MediaRef,
        bytes: &[u8],
        append: impl FnOnce() -> io::Result<T>,
        steps: &mut impl StorageSteps,
    ) -> io::Result<T> {
        if !valid_digest(&reference.sha256) || !matches_bytes(reference, bytes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("media_object_corrupt {}", reference.sha256),
            ));
        }
        let lock = self.store.lock()?;
        lock.lock_shared()?;
        self.store.publish(reference, bytes, steps)?;
        recover_torn_tail(&mut self.file, &self.path)?;
        self.file
            .write_all(format!("{}\n", reference.sha256).as_bytes())?;
        steps.file_sync(&self.file, Step::ManifestSynced)?;
        if !self.refs_synced {
            steps.dir_sync(&self.store.root.join("refs"), Step::RefsSynced)?;
            self.refs_synced = true;
        }
        // The durable manifest must be backed by verified bytes at the WAL
        // boundary, not just at the earlier publication. Repair from held bytes
        // while the shared lock prevents maintenance from undoing the repair.
        if self.store.read_verified(reference).is_err() {
            self.store.publish(reference, bytes, steps)?;
        }
        let result = append()?;
        steps.reached(Step::WalAppended)?;
        Ok(result)
    }
}

#[derive(Debug)]
pub enum MediaReadError {
    Missing(String),
    Corrupt(String),
    Io { digest: String, source: io::Error },
}

impl MediaReadError {
    /// Missing and corrupt objects have media refusal codes. Other I/O failures
    /// retain their source error for the caller's ordinary storage-error path.
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::Missing(_) => Some("media_object_missing"),
            Self::Corrupt(_) => Some("media_object_corrupt"),
            Self::Io { .. } => None,
        }
    }

    pub fn digest(&self) -> &str {
        match self {
            Self::Missing(digest) | Self::Corrupt(digest) | Self::Io { digest, .. } => digest,
        }
    }
}

impl std::fmt::Display for MediaReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            self.code().unwrap_or("read media object"),
            self.digest()
        )?;
        if let Self::Io { source, .. } = self {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

impl std::error::Error for MediaReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn matches_bytes(reference: &MediaRef, bytes: &[u8]) -> bool {
    reference.byte_size == bytes.len() as u64
        && format!("{:x}", Sha256::digest(bytes)) == reference.sha256
}

// A failed write can leave part of a digest at EOF. Never join a retry to that
// prefix: only complete newline-terminated entries are append-only records.
// The next manifest fsync makes both this tail repair and the new entry durable.
fn recover_torn_tail(file: &mut File, path: &Path) -> io::Result<()> {
    let length = file.metadata()?.len();
    let start = length.saturating_sub(65);
    file.seek(SeekFrom::Start(start))?;
    let mut tail = Vec::new();
    file.read_to_end(&mut tail)?;
    if tail.last().is_some_and(|byte| *byte != b'\n') {
        let complete = tail
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(start, |at| start + at as u64 + 1);
        // Windows append handles lack FILE_WRITE_DATA, which set_len requires.
        // Reopen only for repair under the session lease and shared store lock;
        // keep normal writes on the append handle so they land at the new EOF.
        let repair = broca_owner_only::OpenOptions::new()
            .write(true)
            .open(path)?;
        repair.set_len(complete)?;
        repair.sync_all()?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    ObjectFileSynced,
    ObjectPublished,
    ManifestSynced,
    RefsSynced,
    WalAppended,
    WalDirectorySynced,
}

trait StorageSteps {
    fn reached(&mut self, _step: Step) -> io::Result<()> {
        Ok(())
    }
    fn file_sync(&mut self, file: &File, step: Step) -> io::Result<()> {
        file.sync_all()?;
        self.reached(step)
    }
    fn dir_sync(&mut self, directory: &Path, step: Step) -> io::Result<()> {
        broca_wal::migration::sync_dir(directory)?;
        self.reached(step)
    }
}

struct RealSteps;
impl StorageSteps for RealSteps {}

#[cfg(test)]
mod tests;
