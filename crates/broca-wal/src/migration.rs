//! Content-free files protecting session WALs during moves between machines.
//! A cut id identifies one move batch. `.moving` names the batch freezing writes;
//! `.moved` names the destination machine holding the session and hides old bytes
//! from reads as well. Unfinished release/activate operations recorded in cut JSON
//! also freeze writes, including new WALs with no sidecar yet. Older builds that
//! only enforce these protections inspect the files without completing or undoing
//! the move, so downgrading cannot resume a partly installed session.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs, io,
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Freeze {
    #[serde(rename = "cutId")]
    pub cut_id: String,
    #[serde(rename = "endOffset")]
    pub end_offset: u64,
    pub lineage_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Moved {
    #[serde(rename = "cutId")]
    pub cut_id: String,
    pub holder: String,
    pub lineage_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoveRefusal {
    Moving { cut_id: String },
    Moved { holder: String },
}
impl std::fmt::Display for MoveRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Moving { cut_id } => write!(f, "session_moving {{cutId: {cut_id}}}"),
            Self::Moved { holder } => write!(f, "session_moved {{holder: {holder}}}"),
        }
    }
}
impl std::error::Error for MoveRefusal {}

impl From<MoveRefusal> for crate::WalError {
    fn from(value: MoveRefusal) -> Self {
        match value {
            MoveRefusal::Moving { cut_id } => Self::SessionMoving { cut_id },
            MoveRefusal::Moved { holder } => Self::SessionMoved { holder },
        }
    }
}

pub fn guard(wal: &Path, session_ref: &str, write: bool) -> Result<(), crate::WalError> {
    check(wal, session_ref, write)
        .map_err(crate::WalError::Io)?
        .map_err(Into::into)
}

/// Makes a rename, create or unlink inside `path` durable. On unix that takes an fsync of the
/// directory itself, and a failure must propagate.
#[cfg(unix)]
pub fn sync_dir(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    io_trace::observe(io_trace::Event::DirectorySync(path.to_owned()))?;
    fs::File::open(path)?.sync_all()
}

/// NTFS needs no directory handle to make a rename or unlink durable, and `File::open` cannot
/// open a directory there (it fails with "Access is denied"), so there is nothing to do. This is
/// the same rule as the WAL fold's directory sync.
#[cfg(not(unix))]
pub fn sync_dir(_path: &Path) -> io::Result<()> {
    #[cfg(test)]
    io_trace::observe(io_trace::Event::DirectorySync(_path.to_owned()))?;
    Ok(())
}

fn sync_file(file: &fs::File) -> io::Result<()> {
    #[cfg(test)]
    io_trace::observe(io_trace::Event::FileSync)?;
    file.sync_all()
}

/// Every newly created parent is synced too: a synced file in an uncommitted
/// directory would disappear together with the directory after a crash.
pub fn durable_dir(path: &Path) -> io::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    durable_dir(parent)?;
    match broca_owner_only::create_dir(path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    sync_dir(path)?;
    sync_dir(parent)
}

pub fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(invalid),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
fn invalid(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

/// The caller serializes writes to this destination. The cut-fence scan checks
/// only completed `.json` files, never a temporary with a partially written intent.
pub fn atomic_json(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("cut path has no parent"))?;
    durable_dir(parent)?;
    let temp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec(value).map_err(invalid)?;
    let mut file = broca_owner_only::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    sync_dir(parent)
}

pub fn unlink(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => sync_dir(path.parent().unwrap_or(Path::new("."))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn existing_freeze(
    path: &Path,
    wanted: &Freeze,
) -> io::Result<Option<Result<Freeze, MoveRefusal>>> {
    let file = match broca_owner_only::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let owner: Freeze = serde_json::from_reader(&file).map_err(invalid)?;
    if owner.cut_id != wanted.cut_id {
        return Ok(Some(Err(MoveRefusal::Moving {
            cut_id: owner.cut_id,
        })));
    }
    // A prior process may have published the entry but died before its directory
    // barrier. Complete both barriers even when the cached JSON is readable.
    sync_file(&file)?;
    sync_dir(path.parent().unwrap_or(Path::new(".")))?;
    Ok(Some(Ok(owner)))
}

struct FreezeTemp(PathBuf);
impl Drop for FreezeTemp {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
fn freeze_temp(path: &Path) -> io::Result<(fs::File, FreezeTemp)> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    loop {
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temp = path.with_extension(format!("moving.{}.{id}.tmp", std::process::id()));
        match broca_owner_only::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
        {
            Ok(file) => return Ok((file, FreezeTemp(temp))),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
}

fn publication_ready(_destination: &Path) -> io::Result<()> {
    #[cfg(test)]
    io_trace::observe(io_trace::Event::BeforePublish(_destination.to_owned()))?;
    Ok(())
}

fn exclusive_link(temp: &Path, destination: &Path) -> io::Result<()> {
    let result = publication_ready(destination).and_then(|()| fs::hard_link(temp, destination));
    result.map_err(|error: io::Error| {
        io::Error::new(
            error.kind(),
            format!(
                "cannot publish freeze {} with an exclusive hard link: {error}",
                destination.display()
            ),
        )
    })
}

pub fn freeze(wal: &Path, wanted: &Freeze) -> io::Result<Result<Freeze, MoveRefusal>> {
    if let Some(moved) = read_json::<Moved>(&wal.with_extension("moved"))? {
        return Ok(Err(MoveRefusal::Moved {
            holder: moved.holder,
        }));
    }
    let path = wal.with_extension("moving");
    let parent = path.parent().unwrap_or(Path::new("."));
    durable_dir(parent)?;
    let bytes = serde_json::to_vec(wanted).map_err(invalid)?;
    loop {
        if let Some(owner) = existing_freeze(&path, wanted)? {
            return Ok(owner);
        }
        let (mut file, temp) = freeze_temp(&path)?;
        file.write_all(&bytes)?;
        sync_file(&file)?;
        // A same-directory hard link publishes the complete, synced inode and
        // fails exclusively if another cut already owns the destination.
        match exclusive_link(&temp.0, &path) {
            Ok(()) => {
                #[cfg(test)]
                io_trace::observe(io_trace::Event::Published(path.clone()))?;
                drop(file);
                match fs::remove_file(&temp.0) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
                sync_dir(parent)?;
                return Ok(Ok(wanted.clone()));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
}

pub fn abort_freeze(wal: &Path, cut_id: &str) -> io::Result<()> {
    if let Some(owner) = read_json::<Freeze>(&wal.with_extension("moving"))? {
        if owner.cut_id == cut_id {
            unlink(&wal.with_extension("moving"))?;
        }
    }
    Ok(())
}

pub fn release(wal: &Path, cut_id: &str, holder: &str) -> io::Result<Result<(), MoveRefusal>> {
    let moved_path = wal.with_extension("moved");
    if let Some(moved) = read_json::<Moved>(&moved_path)? {
        if moved.cut_id == cut_id && moved.holder == holder {
            if let Some(owner) = read_json::<Freeze>(&wal.with_extension("moving"))? {
                if owner.cut_id != cut_id {
                    return Ok(Err(MoveRefusal::Moving {
                        cut_id: owner.cut_id,
                    }));
                }
                unlink(&wal.with_extension("moving"))?;
            }
            return Ok(Ok(()));
        }
        return Ok(Err(MoveRefusal::Moved {
            holder: moved.holder,
        }));
    }
    let owner = read_json::<Freeze>(&wal.with_extension("moving"))?
        .ok_or_else(|| invalid("release has no owned freeze"))?;
    if owner.cut_id != cut_id {
        return Ok(Err(MoveRefusal::Moving {
            cut_id: owner.cut_id,
        }));
    }
    // Publish the tombstone first. A crash before unlink still refuses both reads
    // and writes; tombstone precedence prevents the old WAL being exposed.
    atomic_json(
        &moved_path,
        &Moved {
            cut_id: cut_id.to_owned(),
            holder: holder.to_owned(),
            lineage_id: owner.lineage_id,
        },
    )?;
    unlink(&wal.with_extension("moving"))?;
    Ok(Ok(()))
}

/// Find a pending release or activation naming this session's shared contract
/// ref. Refuse on unreadable cut JSON: otherwise a reader could append to an
/// installed WAL before the move's completion is durably recorded.
/// `session_ref` comes from the shared role helper, never the WAL filename hash.
pub fn cut_fence(wal_root: &Path, session_ref: &str) -> io::Result<Option<String>> {
    let cuts = wal_root
        .parent()
        .unwrap_or(Path::new("."))
        .join("agent-data/cuts");
    let entries = match fs::read_dir(cuts) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let state: Value = read_json(&path)?.ok_or_else(|| invalid("cut state disappeared"))?;
        let Some(intent) = state.get("intent").filter(|v| !v.is_null()) else {
            continue;
        };
        let op = intent
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("cut intent has no op"))?;
        if !matches!(op, "release" | "activate") {
            continue;
        }
        let refs = state
            .get("refs")
            .or_else(|| state.get("ref_set"))
            .or_else(|| intent.get("request").and_then(|r| r.get("refs")))
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("cut fence has no refs"))?;
        for reference in refs {
            let kind = reference
                .get("ref_kind")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("cut ref has no kind"))?;
            let value = reference
                .get("ref")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("cut ref has no value"))?;
            if kind == "broca-session" && value == session_ref {
                let cut_id = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .ok_or_else(|| invalid("cut filename"))?;
                return Ok(Some(cut_id.to_owned()));
            }
        }
    }
    Ok(None)
}

pub fn check(wal: &Path, session_ref: &str, write: bool) -> io::Result<Result<(), MoveRefusal>> {
    if wal
        .components()
        .any(|part| part.as_os_str() == "wal-staging")
    {
        return Err(invalid("wal-staging is not a live session population"));
    }
    if write {
        if let Some(cut_id) = cut_fence(wal.parent().unwrap_or(Path::new(".")), session_ref)? {
            return Ok(Err(MoveRefusal::Moving { cut_id }));
        }
    }
    if let Some(moved) = read_json::<Moved>(&wal.with_extension("moved"))? {
        return Ok(Err(MoveRefusal::Moved {
            holder: moved.holder,
        }));
    }
    if write {
        if let Some(owner) = read_json::<Freeze>(&wal.with_extension("moving"))? {
            return Ok(Err(MoveRefusal::Moving {
                cut_id: owner.cut_id,
            }));
        }
    }
    Ok(Ok(()))
}

/// Staging is owned by one cut and kept outside `wal/` and `wal-archive/`, so
/// transcript readers, retention sweeps and archive folds never discover it.
pub fn staging_path(wal_root: &Path, cut_id: &str, address: &str) -> io::Result<PathBuf> {
    if [cut_id, address].iter().any(|s| {
        s.is_empty()
            || !s
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    }) {
        return Err(invalid("invalid staging cut or address"));
    }
    Ok(wal_root
        .parent()
        .unwrap_or(Path::new("."))
        .join("wal-staging")
        .join(cut_id)
        .join(format!("{address}.wal")))
}

/// Verify before publishing staging; a mismatch leaves no file for activation.
pub fn stage(path: &Path, bytes: &[u8], digest: &str) -> io::Result<()> {
    use sha2::{Digest, Sha256};
    if format!("{:x}", Sha256::digest(bytes)) != digest {
        return Err(invalid("digest_mismatch"));
    }
    let parent = path.parent().ok_or_else(|| invalid("staging path"))?;
    durable_dir(parent)?;
    let temp = path.with_extension("wal.tmp");
    let mut file = broca_owner_only::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(temp, path)?;
    sync_dir(parent)
}

pub fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod io_trace;
#[cfg(test)]
mod tests;
