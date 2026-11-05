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
//! Besides the head and logs, a session has immutable objects: sealed segments and directory
//! pages. Each belongs to the change that wrote it, and its name starts with that change's id
//! (`<change id>/<name>`), so a change that never committed is undone by deleting its prefix.
//! Storage shared across sessions is the setup store: the parts runs are started with (tool
//! definitions, system text, plans), stored once each per deployment and never deleted (see
//! `setup`). It isn't fenced: a part is named by its content, so two writers storing the same
//! name store the same bytes.

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
    /// Deletes `log`, if it exists. Durable on return.
    async fn delete_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError>;
    /// Stores object `name`, whose first component is the id of the change writing it. Its data
    /// is durable on return; the name is durable after [`sync_objects`](Self::sync_objects) for
    /// that change. An object of that name already there is a crashed attempt's leftover, and is
    /// replaced.
    async fn put_object(
        &self,
        s: &SessionAddr,
        name: &str,
        bytes: &[u8],
        at: Version,
    ) -> Result<(), BackendError>;
    /// Makes the names of every object `change` stored durable.
    async fn sync_objects(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError>;
    async fn get_object(
        &self,
        s: &SessionAddr,
        name: &str,
    ) -> Result<Option<Vec<u8>>, BackendError>;
    /// Deletes object `name`, if it exists.
    async fn delete_object(
        &self,
        s: &SessionAddr,
        name: &str,
        at: Version,
    ) -> Result<(), BackendError>;
    /// Deletes every object `change` stored. Durable on return.
    async fn delete_change(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError>;
    /// Stores setup part `name`, unless a part of that name is already stored: names are derived
    /// from content, so the stored part already holds these bytes. Durable on return, name
    /// included.
    async fn put_setup(&self, name: &str, bytes: &[u8]) -> Result<(), BackendError>;
    async fn get_setup(&self, name: &str) -> Result<Option<Vec<u8>>, BackendError>;
}

/// The change id an object's name starts with.
pub fn object_change(name: &str) -> &str {
    name.split('/').next().unwrap_or_default()
}

/// A backend in memory, for tests.
#[derive(Default)]
pub struct MemoryBackend {
    sessions: Mutex<HashMap<SessionAddr, MemorySession>>,
    setup: Mutex<HashMap<String, Vec<u8>>>,
    /// Mutations left before the backend "crashes": every mutation after that fails. Tests only.
    crash_after: Mutex<Option<usize>>,
    /// Mutations left before exactly one of them fails. Tests only.
    fail_at: Mutex<Option<usize>>,
}

#[derive(Default)]
struct MemorySession {
    head: Option<(Vec<u8>, Version)>,
    logs: HashMap<Id128, Vec<u8>>,
    objects: std::collections::BTreeMap<String, Vec<u8>>,
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
    /// Makes the backend fail every mutation after the next `mutations`, as if the process died
    /// there: what landed before stays, nothing after lands. `None` revives it. Tests only.
    pub fn crash_after(&self, mutations: Option<usize>) {
        *self.crash_after.lock().unwrap() = mutations;
    }

    /// Makes the backend fail exactly one mutation, the one after the next `mutations`, and no
    /// other. Tests only.
    pub fn fail_once(&self, mutations: usize) {
        *self.fail_at.lock().unwrap() = Some(mutations);
    }

    fn mutate(&self) -> Result<(), BackendError> {
        let mut once = self.fail_at.lock().unwrap();
        match once.as_mut() {
            Some(0) => {
                *once = None;
                return Err(BackendError::Io(io::Error::other("failed once")));
            }
            Some(n) => *n -= 1,
            None => {}
        }
        drop(once);
        let mut left = self.crash_after.lock().unwrap();
        match left.as_mut() {
            Some(0) => Err(BackendError::Io(io::Error::other("crashed"))),
            Some(n) => {
                *n -= 1;
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// Every object name stored for `s`. Tests only.
    pub fn object_names(&self, s: &SessionAddr) -> Vec<String> {
        let sessions = self.sessions.lock().unwrap();
        sessions
            .get(s)
            .map(|session| session.objects.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Every log stored for `s`. Tests only.
    pub fn log_names(&self, s: &SessionAddr) -> Vec<Id128> {
        let sessions = self.sessions.lock().unwrap();
        sessions
            .get(s)
            .map(|session| session.logs.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Replaces a stored setup part's bytes, as damage would leave them. Tests only.
    pub fn set_setup(&self, name: &str, bytes: Vec<u8>) {
        self.setup.lock().unwrap().insert(name.into(), bytes);
    }

    /// Every setup part name stored. Tests only.
    pub fn setup_names(&self) -> Vec<String> {
        self.setup.lock().unwrap().keys().cloned().collect()
    }

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
        self.mutate()?;
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
        self.mutate()?;
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
        self.mutate()?;
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
        self.mutate()?;
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

    async fn delete_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.mutate()?;
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(s).ok_or(BackendError::NoSuchLog)?;
        session.fence(at)?;
        session.logs.remove(log);
        Ok(())
    }

    async fn put_object(
        &self,
        s: &SessionAddr,
        name: &str,
        bytes: &[u8],
        at: Version,
    ) -> Result<(), BackendError> {
        self.mutate()?;
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(s).ok_or(BackendError::Fenced)?;
        session.fence(at)?;
        session.objects.insert(name.to_owned(), bytes.to_vec());
        Ok(())
    }

    async fn sync_objects(
        &self,
        _s: &SessionAddr,
        _change: &Id128,
        _at: Version,
    ) -> Result<(), BackendError> {
        self.mutate()
    }

    async fn get_object(
        &self,
        s: &SessionAddr,
        name: &str,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let sessions = self.sessions.lock().unwrap();
        Ok(sessions
            .get(s)
            .and_then(|session| session.objects.get(name).cloned()))
    }

    async fn delete_object(
        &self,
        s: &SessionAddr,
        name: &str,
        at: Version,
    ) -> Result<(), BackendError> {
        self.mutate()?;
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(s).ok_or(BackendError::Fenced)?;
        session.fence(at)?;
        session.objects.remove(name);
        Ok(())
    }

    async fn delete_change(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.mutate()?;
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(s).ok_or(BackendError::Fenced)?;
        session.fence(at)?;
        let prefix = format!("{}/", hex(&change.0));
        session.objects.retain(|name, _| !name.starts_with(&prefix));
        Ok(())
    }

    async fn put_setup(&self, name: &str, bytes: &[u8]) -> Result<(), BackendError> {
        self.mutate()?;
        self.setup
            .lock()
            .unwrap()
            .entry(name.into())
            .or_insert_with(|| bytes.to_vec());
        Ok(())
    }

    async fn get_setup(&self, name: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.setup.lock().unwrap().get(name).cloned())
    }
}

/// A backend on local files:
/// - `<root>/heads/<addr>/head`: the head, as its version (8 bytes, little-endian) then its
///   bytes. Replaced by writing a temporary file, syncing it, renaming it into place and syncing
///   the folder.
/// - `<root>/logs/<addr>/<change id>`: one append-only file per log. A barrier syncs it with
///   `sync_all`, which issues `F_FULLFSYNC` on macOS and survives power loss.
/// - `<root>/objects/<addr>/<change id>/<name>`: immutable objects, one folder per change, so
///   undoing a change deletes one folder.
/// - `<root>/setup/<name>`: setup parts, shared by every session. Written to a temporary file,
///   synced, and renamed into place, so a part is either whole or absent.
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

    fn object_dir(&self, s: &SessionAddr) -> PathBuf {
        self.root.join("objects").join(&s.0)
    }

    /// An object's path. A name may hold only `[0-9a-zA-Z._-]` and `/`, with no empty or `.`
    /// components, so it can never leave the session's folder.
    fn object_path(&self, s: &SessionAddr, name: &str) -> Result<PathBuf, BackendError> {
        let safe = name.split('/').all(|part| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        });
        if !safe {
            return Err(BackendError::Corrupt(
                "object name outside the session's folder",
            ));
        }
        Ok(self.object_dir(s).join(name))
    }

    /// A setup part's path. A name is lowercase hex only, so it can't leave the setup folder.
    fn setup_path(&self, name: &str) -> Result<PathBuf, BackendError> {
        if name.is_empty() || !name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err(BackendError::Corrupt("setup part name is not hex"));
        }
        Ok(self.root.join("setup").join(name))
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

    async fn delete_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.fence(s, at)?;
        self.open_logs.lock().unwrap().remove(&(s.clone(), *log));
        match std::fs::remove_file(self.log_path(s, log)) {
            Ok(()) => sync_dir(&self.log_dir(s))?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    async fn put_object(
        &self,
        s: &SessionAddr,
        name: &str,
        bytes: &[u8],
        at: Version,
    ) -> Result<(), BackendError> {
        self.fence(s, at)?;
        let path = self.object_path(s, name)?;
        let dir = path
            .parent()
            .expect("an object lives in its change's folder");
        std::fs::create_dir_all(dir)?;
        let mut file = File::create(&path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(())
    }

    async fn sync_objects(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.fence(s, at)?;
        let dir = self.object_dir(s).join(hex(&change.0));
        if dir.exists() {
            for entry in std::fs::read_dir(&dir)? {
                let path = entry?.path();
                if path.is_dir() {
                    sync_dir(&path)?;
                }
            }
            sync_dir(&dir)?;
            sync_dir(&self.object_dir(s))?;
        }
        Ok(())
    }

    async fn get_object(
        &self,
        s: &SessionAddr,
        name: &str,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        match std::fs::read(self.object_path(s, name)?) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn delete_object(
        &self,
        s: &SessionAddr,
        name: &str,
        at: Version,
    ) -> Result<(), BackendError> {
        self.fence(s, at)?;
        match std::fs::remove_file(self.object_path(s, name)?) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn delete_change(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.fence(s, at)?;
        match std::fs::remove_dir_all(self.object_dir(s).join(hex(&change.0))) {
            Ok(()) => sync_dir(&self.object_dir(s))?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    async fn put_setup(&self, name: &str, bytes: &[u8]) -> Result<(), BackendError> {
        let path = self.setup_path(name)?;
        if path.exists() {
            return Ok(());
        }
        let dir = path
            .parent()
            .expect("a setup part lives in the setup folder");
        std::fs::create_dir_all(dir)?;
        // A unique temporary name, so two writers storing the same part never share a file.
        let temp = dir.join(format!(".{name}.{}.tmp", hex(&crate::keys::random_id().0)));
        let mut file = File::create(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temp, &path)?;
        sync_dir(dir)?;
        Ok(())
    }

    async fn get_setup(&self, name: &str) -> Result<Option<Vec<u8>>, BackendError> {
        match std::fs::read(self.setup_path(name)?) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}
