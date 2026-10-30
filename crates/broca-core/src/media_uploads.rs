//! Session-owned staging. Uploads never publish objects or write a session WAL.

use broca_protocol::BindIdentity;
use cortexkit_role_llm_runner::attachments as wire;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// The upload maximum is derived from the largest media-store maximum (video).
// Session bytes, upload count and global bytes are operator safety bounds, not
// limits imposed by cortexkit_role_llm_runner::attachments. Reservations include
// expired uploads until maintenance releases them.
pub const UPLOAD_MAX: u64 = largest_store_maximum();
pub const SESSION_MAX: u64 = 512 * 1024 * 1024;
pub const SESSION_UPLOAD_MAX: u64 = 128;
pub const GLOBAL_MAX: u64 = 4 * 1024 * 1024 * 1024;
// A fixed hour from begin bounds reservation lifetime without metadata writes
// on retries. Even the largest upload needs only 32 of the 2 MiB chunks.
pub const OPEN_TTL_SECONDS: u64 = 3_600;
pub const RETENTION_SECONDS: u64 = wire::UNREFERENCED_RETENTION_SECONDS;

const fn largest_store_maximum() -> u64 {
    use broca_protocol::MediaKind;
    let kinds = [
        MediaKind::Image,
        MediaKind::Document,
        MediaKind::Audio,
        MediaKind::Video,
    ];
    let mut maximum = 0;
    let mut i = 0;
    while i < kinds.len() {
        let limit = crate::media_validation::store_max_bytes(kinds[i]);
        if limit > maximum {
            maximum = limit;
        }
        i += 1;
    }
    maximum
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadError {
    pub code: &'static str,
    pub subject: String,
    pub field: Option<&'static str>,
    pub received: Option<u64>,
}

impl std::fmt::Display for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.subject)
    }
}
impl std::error::Error for UploadError {}
impl From<io::Error> for UploadError {
    fn from(error: io::Error) -> Self {
        Self::new("upload_store_failed", error.to_string())
    }
}
impl UploadError {
    pub fn error_class(&self) -> broca_protocol::ErrorClass {
        match self.code {
            "upload_staging_full" | "upload_session_full" | "upload_store_failed" => {
                broca_protocol::ErrorClass::Transient
            }
            _ => broca_protocol::ErrorClass::Permanent,
        }
    }
    pub fn new(code: &'static str, subject: impl Into<String>) -> Self {
        Self {
            code,
            subject: subject.into(),
            field: None,
            received: None,
        }
    }
    pub fn invalid(field: &'static str, subject: impl Into<String>) -> Self {
        Self {
            field: Some(field),
            ..Self::new("invalid_params", subject)
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    identity: BindIdentity,
    mime: String,
    bytes: u64,
    sha256: String,
    expires: u64,
    committed: bool,
    gone: bool,
}

pub struct Uploads {
    root: PathBuf,
}

impl Uploads {
    /// Read a committed upload without changing staging, stores or WALs. Ownership,
    /// expiry and the committed hash are checked again at the send boundary.
    pub fn read_committed(
        &self,
        identity: &BindIdentity,
        id: &str,
        now: u64,
    ) -> Result<(String, Vec<u8>), UploadError> {
        let _lock = self.lock()?;
        let meta = self.load(identity, id, now)?;
        if !meta.committed {
            return Err(UploadError::new("upload_not_committed", id));
        }
        let bytes = fs::read(self.data_path(id))?;
        wire::verify_commit(meta.bytes, &meta.sha256, &bytes)
            .map_err(|code| UploadError::new(code, id))?;
        Ok((meta.mime, bytes))
    }

    /// Publication chooses the store alongside the already selected WAL durability class.
    pub fn media_store(
        &self,
        class: crate::media_store::MediaStoreClass,
    ) -> io::Result<crate::media_store::MediaStore> {
        crate::media_store::MediaStore::open(
            self.root.parent().expect("uploads have a state root"),
            class,
        )
    }

    pub fn session_media_store(
        &self,
        identity: &BindIdentity,
    ) -> io::Result<crate::media_store::MediaStore> {
        let state = self.root.parent().expect("uploads have a state root");
        let wal = crate::FileWalStore::new(state.join("wal"));
        let class = wal
            .media_store_class(&crate::wal_writer::session_addr(identity))
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::other("session WAL has no media store class"))?;
        self.media_store(class)
    }
    /// Startup creates the lock and directories. Opening for a refused operation
    /// must not create even an empty staging file.
    pub fn at(state_root: &Path) -> Self {
        Self {
            root: state_root.join("media-uploads"),
        }
    }

    fn lock(&self) -> Result<File, UploadError> {
        let file = File::open(self.root.join("uploads.lock"))?;
        file.lock()?;
        Ok(file)
    }

    fn metadata_path(&self, id: &str) -> PathBuf {
        self.root.join("refs").join(id)
    }
    fn data_path(&self, id: &str) -> PathBuf {
        self.root.join("objects").join(id)
    }

    fn inventory(&self) -> Result<Vec<(String, Metadata)>, UploadError> {
        let mut entries = Vec::new();
        for entry in fs::read_dir(self.root.join("refs"))? {
            let entry = entry?;
            let id = entry
                .file_name()
                .into_string()
                .map_err(|_| io::Error::other("invalid upload filename"))?;
            if !valid_id(&id) {
                return Err(io::Error::other("invalid upload filename").into());
            }
            let meta: Metadata =
                serde_json::from_slice(&fs::read(entry.path())?).map_err(io::Error::other)?;
            if meta.bytes > UPLOAD_MAX || meta.sha256.len() != 64 {
                return Err(io::Error::other(format!("invalid upload metadata {id}")).into());
            }
            entries.push((id, meta));
        }
        Ok(entries)
    }

    fn staged_files(&self) -> Result<Vec<String>, UploadError> {
        let mut files = Vec::new();
        for entry in fs::read_dir(self.root.join("objects"))? {
            let entry = entry?;
            let id = entry
                .file_name()
                .into_string()
                .map_err(|_| io::Error::other("invalid upload filename"))?;
            if !valid_id(&id) || !entry.file_type()?.is_file() {
                return Err(io::Error::other("invalid upload object").into());
            }
            File::open(entry.path())?;
            files.push(id);
        }
        Ok(files)
    }

    fn load(&self, identity: &BindIdentity, id: &str, now: u64) -> Result<Metadata, UploadError> {
        let unknown = || UploadError::new("unknown_upload", id);
        if !valid_id(id) {
            return Err(unknown());
        }
        let bytes = match fs::read(self.metadata_path(id)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Err(unknown()),
            Err(error) => return Err(error.into()),
        };
        let meta: Metadata = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        if &meta.identity != identity {
            return Err(unknown());
        }
        if meta.gone || now >= meta.expires {
            return Err(UploadError::new("upload_gone", id));
        }
        if !self.data_path(id).try_exists()? {
            return Err(UploadError::new("upload_gone", id));
        }
        Ok(meta)
    }

    fn save(&self, id: &str, meta: &Metadata) -> Result<(), UploadError> {
        broca_wal::migration::atomic_json(
            self.root.parent().unwrap(),
            &self.metadata_path(id),
            meta,
        )?;
        Ok(())
    }

    pub fn begin(
        &self,
        identity: &BindIdentity,
        request: &wire::UploadBeginRequest,
        now: u64,
    ) -> Result<wire::UploadBeginReply, UploadError> {
        request.check().map_err(|field| {
            UploadError::invalid(field, format!("upload for {}", identity.session))
        })?;
        if request.bytes > UPLOAD_MAX {
            return Err(UploadError::new(
                "upload_size_mismatch",
                format!("upload for {} exceeds {UPLOAD_MAX} bytes", identity.session),
            ));
        }
        let _lock = self.lock()?;
        let inventory = self.inventory()?;
        let mut session_bytes = 0u64;
        let mut session_count = 0u64;
        let mut global = 0u64;
        for (id, meta) in &inventory {
            if meta.gone && !self.data_path(id).try_exists()? {
                continue;
            }
            global = global.saturating_add(meta.bytes);
            if meta.identity == *identity {
                session_bytes = session_bytes.saturating_add(meta.bytes);
                session_count += 1;
            }
        }
        check_reservation(request.bytes, session_bytes, session_count, global).map_err(|code| {
            UploadError::new(code, format!("upload staging for {}", identity.session))
        })?;
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = format!(
            "{:x}",
            Sha256::digest(format!(
                "{}:{:?}:{nonce}",
                std::process::id(),
                std::time::SystemTime::now()
            ))
        );
        let meta = Metadata {
            identity: identity.clone(),
            mime: request.mime.clone(),
            bytes: request.bytes,
            sha256: request.sha256.clone(),
            expires: now.saturating_add(OPEN_TTL_SECONDS),
            committed: false,
            gone: false,
        };
        let file = broca_owner_only::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.data_path(&id))?;
        file.sync_all()?;
        broca_wal::migration::sync_dir(&self.root.join("objects"))?;
        self.save(&id, &meta)?;
        Ok(wire::UploadBeginReply::new(id))
    }

    pub fn append(
        &self,
        identity: &BindIdentity,
        id: &str,
        offset: u64,
        bytes: &[u8],
        now: u64,
    ) -> Result<wire::UploadAppendReply, UploadError> {
        self.append_inner(identity, id, offset, bytes, now, true)
    }

    /// Unlike `append`, reject an earlier chunk even if its bytes match. The refusal returns
    /// the current byte count so the caller can resume from that offset.
    pub fn append_strict(
        &self,
        identity: &BindIdentity,
        id: &str,
        offset: u64,
        bytes: &[u8],
        now: u64,
    ) -> Result<wire::UploadAppendReply, UploadError> {
        self.append_inner(identity, id, offset, bytes, now, false)
    }

    fn append_inner(
        &self,
        identity: &BindIdentity,
        id: &str,
        offset: u64,
        bytes: &[u8],
        now: u64,
        replay_identical: bool,
    ) -> Result<wire::UploadAppendReply, UploadError> {
        wire::check_chunk_size(bytes.len()).map_err(|code| UploadError::new(code, id))?;
        let _lock = self.lock()?;
        let meta = self.load(identity, id, now)?;
        if meta.committed {
            return Err(UploadError::invalid("upload_id", id));
        }
        let mut file = broca_owner_only::OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.data_path(id))?;
        let received = file.metadata()?.len();
        let end = offset.checked_add(bytes.len() as u64);
        if replay_identical && offset < received && end.is_some_and(|end| end <= received) {
            file.seek(SeekFrom::Start(offset))?;
            let mut previous = vec![0; bytes.len()];
            file.read_exact(&mut previous)?;
            if previous == bytes {
                return Ok(wire::UploadAppendReply::new(received));
            }
        }
        if offset != received {
            return Err(UploadError {
                received: Some(received),
                ..UploadError::new("upload_offset_mismatch", id)
            });
        }
        if end.is_none_or(|end| end > meta.bytes || end > UPLOAD_MAX) {
            return Err(UploadError::new("upload_size_mismatch", id));
        }
        file.seek(SeekFrom::End(0))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(wire::UploadAppendReply::new(end.unwrap()))
    }

    pub fn commit(
        &self,
        identity: &BindIdentity,
        id: &str,
        now: u64,
    ) -> Result<wire::UploadCommitReply, UploadError> {
        let _lock = self.lock()?;
        let mut meta = self.load(identity, id, now)?;
        let bytes = fs::read(self.data_path(id))?;
        wire::verify_commit(meta.bytes, &meta.sha256, &bytes)
            .map_err(|code| UploadError::new(code, id))?;
        if !meta.committed {
            meta.committed = true;
            meta.expires = now.saturating_add(RETENTION_SECONDS);
            self.save(id, &meta)?;
        }
        Ok(wire::UploadCommitReply::new(
            wire::states::COMMITTED,
            meta.sha256,
            meta.mime,
            meta.bytes,
        ))
    }

    pub fn status(
        &self,
        identity: &BindIdentity,
        id: &str,
        now: u64,
    ) -> Result<wire::UploadStatusReply, UploadError> {
        let _lock = self.lock()?;
        match self.load(identity, id, now) {
            Ok(meta) => Ok(wire::UploadStatusReply::new(
                if meta.committed {
                    wire::states::COMMITTED
                } else {
                    wire::states::OPEN
                },
                fs::metadata(self.data_path(id))?.len(),
            )),
            Err(error) if matches!(error.code, "unknown_upload" | "upload_gone") => {
                Ok(wire::UploadStatusReply::new(wire::states::GONE, 0))
            }
            Err(error) => Err(error),
        }
    }

    /// Only maintenance reclaims staging, and it validates the entire inventory
    /// before removing anything. Sends currently refuse every upload reference,
    /// so no session history depends on the staged bytes being reclaimed here.
    pub fn sweep(&self, now: u64, write: bool) -> Result<usize, UploadError> {
        if !self.root.exists() {
            return Ok(0);
        }
        let _lock = self.lock()?;
        let entries = self.inventory()?;
        let files: std::collections::HashSet<_> = self.staged_files()?.into_iter().collect();
        let live: std::collections::HashSet<_> = entries
            .iter()
            .filter(|(_, meta)| !meta.gone)
            .map(|(id, _)| id.as_str())
            .collect();
        for (id, meta) in &entries {
            if !meta.gone && !files.contains(id) {
                return Err(io::Error::other(format!("missing upload body {id}")).into());
            }
        }
        let orphans: Vec<_> = files
            .into_iter()
            .filter(|id| !live.contains(id.as_str()))
            .collect();
        let expired: Vec<_> = entries
            .into_iter()
            .filter(|(_, meta)| !meta.gone && now >= meta.expires)
            .collect();
        let count = expired.len() + orphans.len();
        if write {
            for id in orphans {
                fs::remove_file(self.data_path(&id))?;
            }
            for (id, mut meta) in expired {
                meta.gone = true;
                self.save(&id, &meta)?;
                match fs::remove_file(self.data_path(&id)) {
                    Ok(()) => (),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                    Err(error) => return Err(error.into()),
                }
            }
            broca_wal::migration::sync_dir(&self.root.join("objects"))?;
        }
        Ok(count)
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 128 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

fn check_reservation(
    bytes: u64,
    session: u64,
    count: u64,
    global: u64,
) -> Result<(), &'static str> {
    if bytes > UPLOAD_MAX {
        return Err("upload_size_mismatch");
    }
    if session.saturating_add(bytes) > SESSION_MAX || count >= SESSION_UPLOAD_MAX {
        return Err("upload_session_full");
    }
    if global.saturating_add(bytes) > GLOBAL_MAX {
        return Err("upload_staging_full");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
