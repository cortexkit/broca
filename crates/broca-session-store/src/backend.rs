//! Backends: where a session's bytes live.
//!
//! A backend stores opaque bytes: a session's head, which it updates only by compare-and-swap,
//! and its logs, which it only appends to. Records, folds and encryption all live above it, so the
//! same session layer runs on local files, in memory for tests, and later on Cloudflare.
//!
//! Every mutation of a log names the head version its writer last committed, and the backend
//! refuses it once the head has moved: a writer another writer replaced can't append to a log it
//! no longer owns. An append also names the log end it expects, so it never lands on top of bytes
//! it hasn't seen. A writer refused either way stops and reloads; it never retries blindly.
//!
//! This is the part of the interface the open log needs. Immutable objects, setup parts and
//! deployment state come with sealing and setup.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::codec::{hex, Id128};

/// A head version: it only grows, and it is the compare-and-swap version and the fencing token.
pub type Version = u64;

/// A session's storage address: the hash of its (project root, harness, session) triple, as
/// today's log file names use.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionAddr(pub String);

#[derive(Debug)]
pub enum BackendError {
    /// The head moved since the version the caller named, or a log doesn't end where the caller
    /// expected: another writer acted. The caller stops and reloads.
    Fenced,
    /// The named log doesn't exist.
    NoSuchLog,
    Io(io::Error),
    /// Stored bytes this backend didn't write.
    Corrupt(&'static str),
}

impl From<io::Error> for BackendError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[allow(async_fn_in_trait)]
pub trait SessionBackend {
    /// The stored head and its version, or `None` when the session has no head.
    async fn read_head(&self, s: &SessionAddr) -> Result<Option<(Vec<u8>, Version)>, BackendError>;
    /// Stores `head` as version `next` if the stored head is still at `expected` (`None`: there
    /// is no head yet), and returns `next`. Durable on return.
    async fn write_head(
        &self,
        s: &SessionAddr,
        head: &[u8],
        expected: Option<Version>,
        next: Version,
    ) -> Result<Version, BackendError>;
    /// Creates `log` empty, or empties it if a crashed creation left it behind.
    async fn create_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError>;
    async fn read_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        range: Range<u64>,
    ) -> Result<Vec<u8>, BackendError>;
    async fn log_end(&self, s: &SessionAddr, log: &Id128) -> Result<u64, BackendError>;
    /// Appends `bytes` to `log`, which must end at `expected_end`, and returns the new end. With
    /// `barrier`, everything in the log is durable on return.
    async fn append_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        bytes: &[u8],
        expected_end: u64,
        at: Version,
        barrier: bool,
    ) -> Result<u64, BackendError>;
    /// Cuts `log`, which must end at `expected_end`, back to `to`. Only a writer opening a log
    /// with a torn tail does this. Durable on return.
    async fn truncate_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        to: u64,
        expected_end: u64,
        at: Version,
    ) -> Result<(), BackendError>;
}

/// A backend in memory, for tests.
#[derive(Default)]
pub struct MemoryBackend {
    sessions: Mutex<HashMap<SessionAddr, MemorySession>>,
}

#[derive(Default)]
struct MemorySession {
    head: Option<(Vec<u8>, Version)>,
    logs: HashMap<Id128, Vec<u8>>,
}

impl MemorySession {
    fn fence(&self, at: Version) -> Result<(), BackendError> {
        match &self.head {
            Some((_, version)) if *version == at => Ok(()),
            _ => Err(BackendError::Fenced),
        }
    }
}

impl MemoryBackend {
    /// Replaces a log's bytes, as a crash or damage would leave them. Tests only.
    pub fn set_log(&self, s: &SessionAddr, log: &Id128, bytes: Vec<u8>) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions
            .entry(s.clone())
            .or_default()
            .logs
            .insert(*log, bytes);
    }
}

impl SessionBackend for MemoryBackend {
    async fn read_head(&self, s: &SessionAddr) -> Result<Option<(Vec<u8>, Version)>, BackendError> {
        let sessions = self.sessions.lock().unwrap();
        Ok(sessions.get(s).and_then(|session| session.head.clone()))
    }

    async fn write_head(
        &self,
        s: &SessionAddr,
        head: &[u8],
        expected: Option<Version>,
        next: Version,
    ) -> Result<Version, BackendError> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.entry(s.clone()).or_default();
        let current = session.head.as_ref().map(|(_, version)| *version);
        if current != expected || expected.is_some_and(|expected| next <= expected) {
            return Err(BackendError::Fenced);
        }
        session.head = Some((head.to_vec(), next));
        Ok(next)
    }

    async fn create_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.entry(s.clone()).or_default();
        session.fence(at)?;
        session.logs.insert(*log, Vec::new());
        Ok(())
    }

    async fn read_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        range: Range<u64>,
    ) -> Result<Vec<u8>, BackendError> {
        let sessions = self.sessions.lock().unwrap();
        let bytes = sessions
            .get(s)
            .and_then(|session| session.logs.get(log))
            .ok_or(BackendError::NoSuchLog)?;
        bytes
            .get(range.start as usize..range.end as usize)
            .map(<[u8]>::to_vec)
            .ok_or(BackendError::Corrupt("range past the log's end"))
    }

    async fn log_end(&self, s: &SessionAddr, log: &Id128) -> Result<u64, BackendError> {
        let sessions = self.sessions.lock().unwrap();
        sessions
            .get(s)
            .and_then(|session| session.logs.get(log))
            .map(|bytes| bytes.len() as u64)
            .ok_or(BackendError::NoSuchLog)
    }

    async fn append_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        bytes: &[u8],
        expected_end: u64,
        at: Version,
        _barrier: bool,
    ) -> Result<u64, BackendError> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(s).ok_or(BackendError::NoSuchLog)?;
        session.fence(at)?;
        let stored = session.logs.get_mut(log).ok_or(BackendError::NoSuchLog)?;
        if stored.len() as u64 != expected_end {
            return Err(BackendError::Fenced);
        }
        stored.extend_from_slice(bytes);
        Ok(stored.len() as u64)
    }

    async fn truncate_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        to: u64,
        expected_end: u64,
        at: Version,
    ) -> Result<(), BackendError> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(s).ok_or(BackendError::NoSuchLog)?;
        session.fence(at)?;
        let stored = session.logs.get_mut(log).ok_or(BackendError::NoSuchLog)?;
        if stored.len() as u64 != expected_end || to > expected_end {
            return Err(BackendError::Fenced);
        }
        stored.truncate(to as usize);
        Ok(())
    }
}

/// A backend on local files:
/// - `<root>/heads/<addr>/head`: the head, as its version (8 bytes, little-endian) then its
///   bytes. Replaced by writing a temporary file, syncing it, renaming it into place and syncing
///   the folder.
/// - `<root>/logs/<addr>/<change id>`: one append-only file per log. A barrier syncs it with
///   `sync_all`, which issues `F_FULLFSYNC` on macOS and survives power loss.
///
/// Only one process may write a session: the caller holds the session's lease (an OS file
/// lock) for as long as it writes. So the head version the fences compare against is kept in
/// memory, loaded from the head file the first time a session is touched.
pub struct LocalBackend {
    root: PathBuf,
    fences: Mutex<HashMap<SessionAddr, Version>>,
    open_logs: Mutex<HashMap<(SessionAddr, Id128), File>>,
}

const VERSION_LEN: usize = 8;

impl LocalBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            fences: Mutex::new(HashMap::new()),
            open_logs: Mutex::new(HashMap::new()),
        }
    }

    fn head_dir(&self, s: &SessionAddr) -> PathBuf {
        self.root.join("heads").join(&s.0)
    }

    fn log_dir(&self, s: &SessionAddr) -> PathBuf {
        self.root.join("logs").join(&s.0)
    }

    fn log_path(&self, s: &SessionAddr, log: &Id128) -> PathBuf {
        self.log_dir(s).join(hex(&log.0))
    }

    fn read_head_file(&self, s: &SessionAddr) -> Result<Option<(Vec<u8>, Version)>, BackendError> {
        let mut bytes = Vec::new();
        match File::open(self.head_dir(s).join("head")) {
            Ok(mut file) => file.read_to_end(&mut bytes)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if bytes.len() < VERSION_LEN {
            return Err(BackendError::Corrupt("head file shorter than its version"));
        }
        let version = u64::from_le_bytes(bytes[..VERSION_LEN].try_into().unwrap());
        Ok(Some((bytes.split_off(VERSION_LEN), version)))
    }

    fn fence(&self, s: &SessionAddr, at: Version) -> Result<(), BackendError> {
        let mut fences = self.fences.lock().unwrap();
        let current = match fences.get(s) {
            Some(version) => *version,
            None => {
                let version = self
                    .read_head_file(s)?
                    .map(|(_, version)| version)
                    .ok_or(BackendError::Fenced)?;
                fences.insert(s.clone(), version);
                version
            }
        };
        if current == at {
            Ok(())
        } else {
            Err(BackendError::Fenced)
        }
    }

    fn with_log<T>(
        &self,
        s: &SessionAddr,
        log: &Id128,
        act: impl FnOnce(&mut File) -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        let mut open_logs = self.open_logs.lock().unwrap();
        let key = (s.clone(), *log);
        if !open_logs.contains_key(&key) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.log_path(s, log))
                .map_err(|error| match error.kind() {
                    io::ErrorKind::NotFound => BackendError::NoSuchLog,
                    _ => BackendError::Io(error),
                })?;
            open_logs.insert(key.clone(), file);
        }
        act(open_logs.get_mut(&key).expect("inserted above"))
    }
}

/// Makes a folder's entries durable. Windows has no folder sync; durability of renames there is
/// unproven and is tested before the Windows backend is relied on.
fn sync_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

impl SessionBackend for LocalBackend {
    async fn read_head(&self, s: &SessionAddr) -> Result<Option<(Vec<u8>, Version)>, BackendError> {
        let head = self.read_head_file(s)?;
        if let Some((_, version)) = &head {
            self.fences.lock().unwrap().insert(s.clone(), *version);
        }
        Ok(head)
    }

    async fn write_head(
        &self,
        s: &SessionAddr,
        head: &[u8],
        expected: Option<Version>,
        next: Version,
    ) -> Result<Version, BackendError> {
        let current = self.read_head_file(s)?.map(|(_, version)| version);
        if current != expected || expected.is_some_and(|expected| next <= expected) {
            return Err(BackendError::Fenced);
        }
        let dir = self.head_dir(s);
        std::fs::create_dir_all(&dir)?;
        let temp = dir.join("head.tmp");
        let mut file = File::create(&temp)?;
        file.write_all(&next.to_le_bytes())?;
        file.write_all(head)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temp, dir.join("head"))?;
        sync_dir(&dir)?;
        self.fences.lock().unwrap().insert(s.clone(), next);
        Ok(next)
    }

    async fn create_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.fence(s, at)?;
        let dir = self.log_dir(s);
        std::fs::create_dir_all(&dir)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(self.log_path(s, log))?;
        file.sync_all()?;
        sync_dir(&dir)?;
        self.open_logs
            .lock()
            .unwrap()
            .insert((s.clone(), *log), file);
        Ok(())
    }

    async fn read_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        range: Range<u64>,
    ) -> Result<Vec<u8>, BackendError> {
        self.with_log(s, log, |file| {
            let mut bytes = vec![0u8; (range.end - range.start) as usize];
            file.seek(SeekFrom::Start(range.start))?;
            file.read_exact(&mut bytes)?;
            Ok(bytes)
        })
    }

    async fn log_end(&self, s: &SessionAddr, log: &Id128) -> Result<u64, BackendError> {
        self.with_log(s, log, |file| Ok(file.metadata()?.len()))
    }

    async fn append_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        bytes: &[u8],
        expected_end: u64,
        at: Version,
        barrier: bool,
    ) -> Result<u64, BackendError> {
        self.fence(s, at)?;
        self.with_log(s, log, |file| {
            if file.metadata()?.len() != expected_end {
                return Err(BackendError::Fenced);
            }
            file.seek(SeekFrom::Start(expected_end))?;
            file.write_all(bytes)?;
            if barrier {
                file.sync_all()?;
            }
            Ok(expected_end + bytes.len() as u64)
        })
    }

    async fn truncate_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        to: u64,
        expected_end: u64,
        at: Version,
    ) -> Result<(), BackendError> {
        self.fence(s, at)?;
        self.with_log(s, log, |file| {
            if file.metadata()?.len() != expected_end || to > expected_end {
                return Err(BackendError::Fenced);
            }
            file.set_len(to)?;
            file.sync_all()?;
            Ok(())
        })
    }
}
