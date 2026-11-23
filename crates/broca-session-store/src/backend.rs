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
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use broca_owner_only::OpenOptions;

use crate::codec::{hex, Id128};

/// A head version: it only grows, and it is the compare-and-swap version and the fencing token.
pub type Version = u64;

/// A session's storage address: the hash of its (project root, harness, session) triple.
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
    /// The size of object `name` in bytes, or `None` when there is no such object. The default
    /// fetches the object; a backend that can tell its size without reading it does.
    async fn object_size(&self, s: &SessionAddr, name: &str) -> Result<Option<u64>, BackendError> {
        Ok(self
            .get_object(s, name)
            .await?
            .map(|bytes| bytes.len() as u64))
    }
    /// Bytes `range` of object `name`, or `None` when there is no such object. A range that
    /// runs past the object's end is an error. A sealed segment is read this way a block at a
    /// time, so reading one message never fetches the whole segment. The default fetches the
    /// whole object and keeps the range; a backend that can read a range reads only it.
    async fn get_object_range(
        &self,
        s: &SessionAddr,
        name: &str,
        range: Range<u64>,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let Some(bytes) = self.get_object(s, name).await? else {
            return Ok(None);
        };
        let span = usize::try_from(range.start)
            .ok()
            .zip(usize::try_from(range.end).ok())
            .filter(|(start, end)| start <= end && *end <= bytes.len());
        match span {
            Some((start, end)) => Ok(Some(bytes[start..end].to_vec())),
            None => Err(BackendError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("range {range:?} runs past object {name}"),
            ))),
        }
    }
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
    /// Deletes the session `s` whole, if the stored head is still at `at`: its head first, so
    /// from then on the session reads as one that never existed, then its logs and objects.
    /// Durable on return. A crash part way leaves the session absent; a backend that can leave
    /// some of its logs and objects behind marks them so they are found and removed later.
    async fn delete_session(&self, s: &SessionAddr, at: Version) -> Result<(), BackendError>;
    /// Stores setup part `name`, unless a part of that name is already stored: names are derived
    /// from content, so the stored part already holds these bytes. Durable on return, name
    /// included.
    async fn put_setup(&self, name: &str, bytes: &[u8]) -> Result<(), BackendError>;
    async fn get_setup(&self, name: &str) -> Result<Option<Vec<u8>>, BackendError>;
    /// The names of every setup part stored, in both cipher modes' name spaces, in no particular
    /// order. A part being written is not listed until it is complete.
    async fn list_setup(&self) -> Result<Vec<String>, BackendError>;
}

/// A borrowed backend is a backend, so a session can borrow one for a short task (a test, a
/// converter pass) or own a handle for its whole life (a session actor).
impl<T: SessionBackend + ?Sized> SessionBackend for &T {
    async fn read_head(&self, s: &SessionAddr) -> Result<Option<(Vec<u8>, Version)>, BackendError> {
        (**self).read_head(s).await
    }
    async fn write_head(
        &self,
        s: &SessionAddr,
        head: &[u8],
        expected: Option<Version>,
        next: Version,
    ) -> Result<Version, BackendError> {
        (**self).write_head(s, head, expected, next).await
    }
    async fn create_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        (**self).create_log(s, log, at).await
    }
    async fn read_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        range: Range<u64>,
    ) -> Result<Vec<u8>, BackendError> {
        (**self).read_log(s, log, range).await
    }
    async fn log_end(&self, s: &SessionAddr, log: &Id128) -> Result<u64, BackendError> {
        (**self).log_end(s, log).await
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
        (**self)
            .append_log(s, log, bytes, expected_end, at, barrier)
            .await
    }
    async fn truncate_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        to: u64,
        expected_end: u64,
        at: Version,
    ) -> Result<(), BackendError> {
        (**self).truncate_log(s, log, to, expected_end, at).await
    }
    async fn delete_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        (**self).delete_log(s, log, at).await
    }
    async fn put_object(
        &self,
        s: &SessionAddr,
        name: &str,
        bytes: &[u8],
        at: Version,
    ) -> Result<(), BackendError> {
        (**self).put_object(s, name, bytes, at).await
    }
    async fn sync_objects(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        (**self).sync_objects(s, change, at).await
    }
    async fn get_object(
        &self,
        s: &SessionAddr,
        name: &str,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        (**self).get_object(s, name).await
    }
    async fn get_object_range(
        &self,
        s: &SessionAddr,
        name: &str,
        range: Range<u64>,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        (**self).get_object_range(s, name, range).await
    }
    async fn object_size(&self, s: &SessionAddr, name: &str) -> Result<Option<u64>, BackendError> {
        (**self).object_size(s, name).await
    }
    async fn delete_object(
        &self,
        s: &SessionAddr,
        name: &str,
        at: Version,
    ) -> Result<(), BackendError> {
        (**self).delete_object(s, name, at).await
    }
    async fn delete_change(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        (**self).delete_change(s, change, at).await
    }
    async fn delete_session(&self, s: &SessionAddr, at: Version) -> Result<(), BackendError> {
        (**self).delete_session(s, at).await
    }
    async fn put_setup(&self, name: &str, bytes: &[u8]) -> Result<(), BackendError> {
        (**self).put_setup(name, bytes).await
    }
    async fn get_setup(&self, name: &str) -> Result<Option<Vec<u8>>, BackendError> {
        (**self).get_setup(name).await
    }
    async fn list_setup(&self) -> Result<Vec<String>, BackendError> {
        (**self).list_setup().await
    }
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
    /// How many objects were fetched (`get_object`, or a range by `get_object_range`), found or
    /// not. Tests only.
    object_reads: std::sync::atomic::AtomicUsize,
    /// How many object bytes those fetches returned. Tests only.
    object_bytes_read: std::sync::atomic::AtomicUsize,
    /// How many log reads (`read_log`) were served, but for those of only a log's checkpoint
    /// frame. Tests only.
    log_reads: std::sync::atomic::AtomicUsize,
    /// How many log reads read only a log's checkpoint frame (see
    /// [`crate::log::reads_checkpoint_frame_only`]). Tests only.
    checkpoint_frame_reads: std::sync::atomic::AtomicUsize,
}

#[derive(Default, Clone)]
struct MemorySession {
    head: Option<(Vec<u8>, Version)>,
    logs: HashMap<Id128, Vec<u8>>,
    /// Each log's length at its last barrier: what [`MemoryBackend::power_loss`] keeps, as the
    /// file backend's sync makes only that much survive. Creating and truncating a log count as
    /// synced, as the file backend syncs both.
    synced: HashMap<Id128, usize>,
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
    /// A copy of everything the backend holds now, as a crash at this moment would leave it:
    /// a test reopens a session from it. Its read counters and failure settings start fresh.
    /// Tests only.
    pub fn snapshot(&self) -> MemoryBackend {
        MemoryBackend {
            sessions: Mutex::new(self.sessions.lock().unwrap().clone()),
            setup: Mutex::new(self.setup.lock().unwrap().clone()),
            ..Default::default()
        }
    }

    /// A copy of the backend as a power loss at this moment would leave it: like [`Self::snapshot`],
    /// but each log keeps only what its last barrier synced. Tests only.
    pub fn power_loss(&self) -> MemoryBackend {
        let mut sessions = self.sessions.lock().unwrap().clone();
        for session in sessions.values_mut() {
            for (log, bytes) in &mut session.logs {
                bytes.truncate(session.synced.get(log).copied().unwrap_or(0));
            }
        }
        MemoryBackend {
            sessions: Mutex::new(sessions),
            setup: Mutex::new(self.setup.lock().unwrap().clone()),
            ..Default::default()
        }
    }

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

    /// How many objects have been fetched from this backend so far, found or not. Tests only.
    pub fn object_reads(&self) -> usize {
        self.object_reads.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many object bytes the fetches so far returned. Tests only.
    pub fn object_bytes_read(&self) -> usize {
        self.object_bytes_read
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many log reads this backend has served so far, but for those of only a log's
    /// checkpoint frame. Tests only.
    pub fn log_reads(&self) -> usize {
        self.log_reads.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many log reads this backend has served so far that read only a log's checkpoint
    /// frame. Tests only.
    pub fn checkpoint_frame_reads(&self) -> usize {
        self.checkpoint_frame_reads
            .load(std::sync::atomic::Ordering::Relaxed)
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

    /// Replaces an object's bytes, as damage would leave them. Tests only.
    pub fn set_object(&self, s: &SessionAddr, name: &str, bytes: Vec<u8>) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions
            .entry(s.clone())
            .or_default()
            .objects
            .insert(name.to_owned(), bytes);
    }

    /// Replaces a log's bytes, as a crash or damage would leave them. Tests only.
    pub fn set_log(&self, s: &SessionAddr, log: &Id128, bytes: Vec<u8>) {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.entry(s.clone()).or_default();
        session.synced.insert(*log, bytes.len());
        session.logs.insert(*log, bytes);
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
        session.synced.insert(*log, 0);
        Ok(())
    }

    async fn read_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        range: Range<u64>,
    ) -> Result<Vec<u8>, BackendError> {
        let sessions = self.sessions.lock().unwrap();
        let log = sessions.get(s).and_then(|session| session.logs.get(log));
        let read = log.ok_or(BackendError::NoSuchLog).and_then(|bytes| {
            bytes
                .get(range.start as usize..range.end as usize)
                .map(<[u8]>::to_vec)
                .ok_or(BackendError::Corrupt("range past the log's end"))
        });
        let checkpoint_frame = match (&read, log) {
            (Ok(read), Some(log)) => {
                crate::log::reads_checkpoint_frame_only(range.start, read, log.len() as u64)
            }
            _ => false,
        };
        let counter = if checkpoint_frame {
            &self.checkpoint_frame_reads
        } else {
            &self.log_reads
        };
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        read
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
        barrier: bool,
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
        let end = stored.len();
        if barrier {
            session.synced.insert(*log, end);
        }
        Ok(end as u64)
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
        session.synced.insert(*log, to as usize);
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
        self.object_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let sessions = self.sessions.lock().unwrap();
        let bytes = sessions
            .get(s)
            .and_then(|session| session.objects.get(name).cloned());
        self.object_bytes_read.fetch_add(
            bytes.as_ref().map_or(0, Vec::len),
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(bytes)
    }

    async fn object_size(&self, s: &SessionAddr, name: &str) -> Result<Option<u64>, BackendError> {
        let sessions = self.sessions.lock().unwrap();
        Ok(sessions
            .get(s)
            .and_then(|session| session.objects.get(name))
            .map(|bytes| bytes.len() as u64))
    }

    async fn get_object_range(
        &self,
        s: &SessionAddr,
        name: &str,
        range: Range<u64>,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        self.object_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let sessions = self.sessions.lock().unwrap();
        let Some(bytes) = sessions
            .get(s)
            .and_then(|session| session.objects.get(name))
        else {
            return Ok(None);
        };
        let span = usize::try_from(range.start)
            .ok()
            .zip(usize::try_from(range.end).ok())
            .filter(|(start, end)| start <= end && *end <= bytes.len());
        let Some((start, end)) = span else {
            return Err(BackendError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("range {range:?} runs past object {name}"),
            )));
        };
        self.object_bytes_read
            .fetch_add(end - start, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(bytes[start..end].to_vec()))
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

    async fn delete_session(&self, s: &SessionAddr, at: Version) -> Result<(), BackendError> {
        self.mutate()?;
        let mut sessions = self.sessions.lock().unwrap();
        sessions.get_mut(s).ok_or(BackendError::Fenced)?.fence(at)?;
        sessions.remove(s);
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

    async fn list_setup(&self) -> Result<Vec<String>, BackendError> {
        Ok(self.setup_names())
    }
}

/// A backend on local files:
/// - `<root>/heads/<addr>/head`: the head, as its version (8 bytes, little-endian) then its
///   bytes. Replaced by writing a temporary file, syncing it, renaming it into place and syncing
///   the folder.
/// - `<root>/heads/<addr>/v/<version>`: every head version, also kept as its own immutable file
///   in the same layout, written and renamed into place before `head` is replaced. Nothing
///   loading a session reads them. They exist so a backup that walks live files, and captures a
///   `head` newer than the logs and objects beside it, still holds an older head that is complete
///   next to them, which the operator's restore (see `restore`) can choose. A version is deleted
///   once the version after it has existed for [`HEAD_VERSION_KEEP_MS`], and the newest two are
///   always kept.
/// - `<root>/logs/<addr>/<change id>`: one append-only file per log. A barrier syncs it
///   (`crate::durable::sync`).
/// - `<root>/objects/<addr>/<change id>/<name>`: immutable objects, one folder per change, so
///   undoing a change deletes one folder.
/// - `<root>/setup/<name>`: setup parts of encrypted sessions, shared by every session. Written to
///   a temporary file, synced, and renamed into place, so a part is either whole or absent.
/// - `<root>/setup-plain/<hex>`: setup parts of plain sessions, named `plain/<hex>` (see
///   `setup::PLAIN_PREFIX`), written the same way. A folder of their own, so a build that knows
///   no plain sessions never lists them among the encrypted parts.
///
/// Files and folders are created owner-only, including temporary files before their rename.
///
/// Only one process may write a session: the caller holds the session's lease (an OS file
/// lock) for as long as it writes. So the head version the fences compare against is kept in
/// memory, loaded from the head file the first time a session is touched.
pub struct LocalBackend {
    root: PathBuf,
    fences: Mutex<HashMap<SessionAddr, Version>>,
    open_logs: Mutex<HashMap<(SessionAddr, Id128), File>>,
    /// Counts of actual file reads, for tests: reads of a log, and reads of only a log's
    /// checkpoint frame (see [`crate::log::reads_checkpoint_frame_only`]).
    #[cfg(feature = "fs-override")]
    log_reads: Option<(
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    )>,
}

const VERSION_LEN: usize = 8;

/// The folder of a session's head folder that keeps every head version.
const VERSIONS_DIR: &str = "v";
/// What a deleted session's head is renamed to until the rest of its folders are removed.
const DELETING: &str = "head.deleting";

/// How long a head version is kept after the version after it was written: far longer than any
/// backup's walk of the store, so the version that was current when a walk began is still there
/// for the walk to copy. A kept version whose files were deleted meanwhile is harmless: the
/// restore checks that every file a version names exists before choosing it.
pub const HEAD_VERSION_KEEP_MS: u64 = 24 * 60 * 60 * 1000;

/// The folder plain sessions' setup parts live in, beside the encrypted parts' `setup`.
const PLAIN_SETUP_DIR: &str = "setup-plain";

impl LocalBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            fences: Mutex::new(HashMap::new()),
            open_logs: Mutex::new(HashMap::new()),
            #[cfg(feature = "fs-override")]
            log_reads: None,
        }
    }

    /// Count actual file reads across handles belonging to one test runtime: in `reads` the reads
    /// of a log, in `checkpoint_frame_reads` those of only a log's checkpoint frame.
    #[cfg(feature = "fs-override")]
    pub fn with_log_reads_for_test(
        mut self,
        reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        checkpoint_frame_reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        self.log_reads = Some((reads, checkpoint_frame_reads));
        self
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

    /// A setup part's path. A name is lowercase hex only, after the plain prefix for a plain
    /// part, so it can't leave its setup folder.
    fn setup_path(&self, name: &str) -> Result<PathBuf, BackendError> {
        let (folder, file) = match name.strip_prefix(crate::setup::PLAIN_PREFIX) {
            Some(file) => (PLAIN_SETUP_DIR, file),
            None => ("setup", name),
        };
        if file.is_empty() || !file.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err(BackendError::Corrupt("setup part name is not hex"));
        }
        Ok(self.root.join(folder).join(file))
    }

    /// The stored head of `s` and its version, read from its file now, leaving the fences a
    /// writer keeps untouched: for a caller that only routes on what a head names, such as its
    /// cipher mode, and never writes.
    pub fn read_stored_head(
        &self,
        s: &SessionAddr,
    ) -> Result<Option<(Vec<u8>, Version)>, BackendError> {
        self.read_head_file(s)
    }

    fn read_head_file(&self, s: &SessionAddr) -> Result<Option<(Vec<u8>, Version)>, BackendError> {
        read_head_at(&self.head_dir(s).join("head"))
    }

    /// The stamp of `s`'s head file, read from its metadata without opening it, or `None` when
    /// it has no head. Two equal stamps mean the head wasn't written in between: every head write
    /// renames a new file into place ([`write_head_file`]).
    pub fn head_stamp(&self, s: &SessionAddr) -> Result<Option<HeadStamp>, BackendError> {
        match std::fs::metadata(self.head_dir(s).join("head")) {
            Ok(metadata) => Ok(Some(HeadStamp::of(&metadata))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// The head versions kept for `s` (see [`HEAD_VERSION_KEEP_MS`]), oldest first.
    pub fn head_versions(&self, s: &SessionAddr) -> Result<Vec<Version>, BackendError> {
        Ok(kept_versions(&self.head_dir(s).join(VERSIONS_DIR))?)
    }

    /// The head kept as `version` of `s`, or `None` when that version isn't kept. A file whose
    /// version isn't the one its name says is refused as damage.
    pub fn read_head_version(
        &self,
        s: &SessionAddr,
        version: Version,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let path = self
            .head_dir(s)
            .join(VERSIONS_DIR)
            .join(version.to_string());
        match read_head_at(&path)? {
            Some((bytes, stored)) if stored == version => Ok(Some(bytes)),
            Some(_) => Err(BackendError::Corrupt(
                "a kept head version holds another version",
            )),
            None => Ok(None),
        }
    }

    /// Every session this store holds a head for. A session folder without a head, such as one a
    /// creation or a move left before its head was written, holds no session.
    pub fn stored_sessions(&self) -> Result<Vec<SessionAddr>, BackendError> {
        Ok(self.stored_sessions_and_deletions()?.0)
    }

    /// As [`Self::stored_sessions`], with every session whose deletion a crash cut short, which
    /// [`Self::finish_deletion`] completes. One pass over the folders finds both: a folder without
    /// a head costs a second lookup, and only those do.
    pub fn stored_sessions_and_deletions(
        &self,
    ) -> Result<(Vec<SessionAddr>, Vec<SessionAddr>), BackendError> {
        let mut sessions = Vec::new();
        let mut deletions = Vec::new();
        for name in self.session_folders()? {
            let dir = self.head_dir(&name);
            if dir.join("head").is_file() {
                sessions.push(name);
            } else if dir.join(DELETING).is_file() {
                deletions.push(name);
            }
        }
        Ok((sessions, deletions))
    }

    /// The head of the deleted session whose deletion at `s` a crash cut short: it names the
    /// session in the clear, so its lease can be taken before [`Self::finish_deletion`] runs.
    /// `None` when no deletion is unfinished there.
    pub fn deleted_head(&self, s: &SessionAddr) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(read_head_at(&self.head_dir(s).join(DELETING))?.map(|(bytes, _)| bytes))
    }

    /// Removes what a deletion of `s` that a crash cut short left behind: the session's logs,
    /// objects and kept head versions. Nothing when no deletion is unfinished there, or when a
    /// session has been created there since. The caller holds the session's lease, so no session
    /// is being created there meanwhile. Durable on return; `true` when it removed anything.
    pub fn finish_deletion(&self, s: &SessionAddr) -> Result<bool, BackendError> {
        let heads = self.head_dir(s);
        if heads.join("head").is_file() || !heads.join(DELETING).is_file() {
            return Ok(false);
        }
        self.remove_session_folders(s)?;
        Ok(true)
    }

    /// Removes the session folders of `s`, its head folder last: the head folder holds the
    /// deletion mark, so a crash part way leaves it to be found again.
    fn remove_session_folders(&self, s: &SessionAddr) -> Result<(), BackendError> {
        for dir in [self.log_dir(s), self.object_dir(s), self.head_dir(s)] {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {
                    if let Some(parent) = dir.parent() {
                        sync_dir(parent)?;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    /// The name of every session folder in this store, sorted, without checking each for a head:
    /// a pass that reads only some of them finds a missing head when it reads one
    /// ([`Self::read_stored_head`] answers `None`). Checking every folder costs a file lookup
    /// each, 13 s for the 171,000 folders one store held, where listing them took 0.3 s.
    pub fn session_folders(&self) -> Result<Vec<SessionAddr>, BackendError> {
        let entries = match std::fs::read_dir(self.root.join("heads")) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut folders = Vec::new();
        for entry in entries {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                folders.push(SessionAddr(name.to_owned()));
            }
        }
        folders.sort();
        Ok(folders)
    }

    /// Cuts `log` of `s` back to `to` bytes, after keeping the bytes past `to` in a file beside
    /// it, `<log>.restore-cut-<stamp_ms>`, synced first, so nothing cut is lost. Returns that
    /// file's path. Only restore does this, holding the session.
    pub fn cut_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        to: u64,
        stamp_ms: u64,
    ) -> Result<PathBuf, BackendError> {
        self.open_logs.lock().unwrap().remove(&(s.clone(), *log));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.log_path(s, log))
            .map_err(|error| match error.kind() {
                io::ErrorKind::NotFound => BackendError::NoSuchLog,
                _ => BackendError::Io(error),
            })?;
        if to > file.metadata()?.len() {
            return Err(BackendError::Corrupt("a cut past the log's end"));
        }
        let mut tail = Vec::new();
        file.seek(SeekFrom::Start(to))?;
        file.read_to_end(&mut tail)?;
        let dir = self.log_dir(s);
        let sidecar = dir.join(format!("{}.restore-cut-{stamp_ms}", hex(&log.0)));
        // A new file, never one already there: an earlier cut's bytes are never overwritten.
        let mut kept = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&sidecar)?;
        kept.write_all(&tail)?;
        crate::durable::sync(&kept)?;
        sync_dir(&dir)?;
        file.set_len(to)?;
        crate::durable::sync(&file)?;
        Ok(sidecar)
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

/// What a head file's metadata says of it (see [`LocalBackend::head_stamp`]): a replaced file has
/// another inode on Unix and, on every platform, the modification time of its own write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeadStamp {
    len: u64,
    modified: Option<std::time::SystemTime>,
    inode: u64,
}

impl HeadStamp {
    fn of(metadata: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        let inode = std::os::unix::fs::MetadataExt::ino(metadata);
        #[cfg(not(unix))]
        let inode = 0;
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            inode,
        }
    }
}

thread_local! {
    static HEAD_FILE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// How many head files this thread has opened and read. A test bounds with it what a pass over
/// many sessions reads.
pub fn head_file_reads() -> u64 {
    HEAD_FILE_READS.with(std::cell::Cell::get)
}

/// A head file's version and bytes, or `None` when there is no such file.
fn read_head_at(path: &Path) -> Result<Option<(Vec<u8>, Version)>, BackendError> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut file) => {
            HEAD_FILE_READS.with(|reads| reads.set(reads.get() + 1));
            file.read_to_end(&mut bytes)?
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if bytes.len() < VERSION_LEN {
        return Err(BackendError::Corrupt("head file shorter than its version"));
    }
    let version = u64::from_le_bytes(bytes[..VERSION_LEN].try_into().unwrap());
    Ok(Some((bytes.split_off(VERSION_LEN), version)))
}

/// Makes a folder's entries durable. Windows has no folder sync; durability of renames there is
/// unproven and is tested before the Windows backend is relied on.
pub(crate) fn sync_dir(path: &Path) -> io::Result<()> {
    crate::durable::sync_dir(path)
}

/// `create_dir_all` that also makes each folder it creates durable in its parent. A new
/// session's `heads/<addr>`, `logs/<addr>` and object folders are entries in folders that
/// already existed; syncing the new folder itself doesn't keep its entry through a power loss.
fn create_dir_durably(dir: &Path) -> io::Result<()> {
    let mut created = Vec::new();
    let mut at = Some(dir);
    while let Some(path) = at.filter(|path| !path.exists()) {
        created.push(path);
        at = path.parent();
    }
    if created.is_empty() {
        return Ok(());
    }
    broca_owner_only::create_dir_all(dir)?;
    for path in created {
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
        }
    }
    Ok(())
}

/// Writes a head file, its version then its bytes, as `name` in `dir`: to the temporary file
/// `temp` first, synced, then renamed into place, and the folder synced.
fn write_head_file(
    dir: &Path,
    temp: &str,
    name: &str,
    version: Version,
    head: &[u8],
) -> io::Result<()> {
    write_file(dir, temp, name, &[&version.to_le_bytes(), head])
}

/// Writes complete parts to a temporary file before publishing them with a durable rename.
fn write_file(dir: &Path, temp: &str, name: &str, parts: &[&[u8]]) -> io::Result<()> {
    let temp = dir.join(temp);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp)?;
    for part in parts {
        file.write_all(part)?;
    }
    crate::durable::sync(&file)?;
    drop(file);
    std::fs::rename(&temp, dir.join(name))?;
    sync_dir(dir)
}

/// The head versions kept in `versions`, oldest first. A file whose name isn't a version, such as
/// a version still being written, isn't one.
fn kept_versions(versions: &Path) -> io::Result<Vec<Version>> {
    let entries = match std::fs::read_dir(versions) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut kept = Vec::new();
    for entry in entries {
        let name = entry?.file_name();
        if let Some(version) = name
            .to_str()
            .filter(|name| name.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|name| name.parse::<Version>().ok())
        {
            kept.push(version);
        }
    }
    kept.sort_unstable();
    Ok(kept)
}

/// Deletes every head version in `versions` whose next version was written at least
/// [`HEAD_VERSION_KEEP_MS`] before `now`, keeping the newest two. Returns how many it deleted.
fn prune_versions(versions: &Path, now: std::time::SystemTime) -> io::Result<usize> {
    let kept = kept_versions(versions)?;
    let keep = std::time::Duration::from_millis(HEAD_VERSION_KEEP_MS);
    let mut deleted = 0;
    // A version's age is how long the next one has existed. Its mtime is when it was written:
    // version files are never changed after their rename.
    for pair in kept.windows(2).take(kept.len().saturating_sub(2)) {
        let next = std::fs::metadata(versions.join(pair[1].to_string()))?.modified()?;
        if now.duration_since(next).is_ok_and(|age| age >= keep) {
            std::fs::remove_file(versions.join(pair[0].to_string()))?;
            deleted += 1;
        }
    }
    if deleted > 0 {
        sync_dir(versions)?;
    }
    Ok(deleted)
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
        if expected.is_none() {
            // A session created where a deletion was cut short: what that session left goes
            // first, so none of it is mistaken for the new session's.
            self.finish_deletion(s)?;
        }
        let dir = self.head_dir(s);
        let versions = dir.join(VERSIONS_DIR);
        create_dir_durably(&versions)?;
        // The version's own file first, so `head` never names a version that isn't kept.
        write_head_file(
            &versions,
            &format!(".{next}.tmp"),
            &next.to_string(),
            next,
            head,
        )?;
        write_head_file(&dir, "head.tmp", "head", next, head)?;
        self.fences.lock().unwrap().insert(s.clone(), next);
        // The head is committed, so a failure to delete old versions mustn't read as a failed
        // write: the versions stay, and the next write deletes them.
        // A move pins a pre-freeze head version. Even an old version must survive every
        // residence update until the move has aborted or the retained source is collected.
        if !matches!(
            crate::head::stored_residence(head),
            Ok(crate::head::Residence::Moving(_) | crate::head::Residence::Moved(_))
        ) {
            let _ = prune_versions(&versions, std::time::SystemTime::now());
        }
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
        create_dir_durably(&dir)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(self.log_path(s, log))?;
        crate::durable::sync(&file)?;
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
            #[cfg(feature = "fs-override")]
            if let Some((reads, checkpoint_frame_reads)) = &self.log_reads {
                let log_len = file.metadata()?.len();
                let counter =
                    if crate::log::reads_checkpoint_frame_only(range.start, &bytes, log_len) {
                        checkpoint_frame_reads
                    } else {
                        reads
                    };
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
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
                let timing = crate::metrics::Phase::Sync.start();
                let synced = crate::durable::sync(file);
                timing.finish(Some(&s.0));
                synced?;
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
            crate::durable::sync(file)?;
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
        create_dir_durably(dir)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.write_all(bytes)?;
        crate::durable::sync(&file)?;
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

    async fn get_object_range(
        &self,
        s: &SessionAddr,
        name: &str,
        range: Range<u64>,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let mut file = match std::fs::File::open(self.object_path(s, name)?) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let len = usize::try_from(range.end.saturating_sub(range.start))
            .map_err(|_| io::Error::other(format!("range {range:?} is too long")))?;
        let mut bytes = vec![0; len];
        file.seek(SeekFrom::Start(range.start))?;
        file.read_exact(&mut bytes)?;
        Ok(Some(bytes))
    }

    async fn object_size(&self, s: &SessionAddr, name: &str) -> Result<Option<u64>, BackendError> {
        match std::fs::metadata(self.object_path(s, name)?) {
            Ok(metadata) => Ok(Some(metadata.len())),
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

    async fn delete_session(&self, s: &SessionAddr, at: Version) -> Result<(), BackendError> {
        self.fence(s, at)?;
        // The head goes first and alone decides whether the session exists, so whatever a crash
        // leaves of the rest belongs to no session. It is renamed to the deletion mark rather
        // than removed, so the rest is found and removed after a crash (see `finish_deletion`).
        let heads = self.head_dir(s);
        match std::fs::rename(heads.join("head"), heads.join(DELETING)) {
            Ok(()) => sync_dir(&heads)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.fences.lock().unwrap().remove(s);
        self.open_logs
            .lock()
            .unwrap()
            .retain(|(addr, _), _| addr != s);
        self.remove_session_folders(s)
    }

    async fn put_setup(&self, name: &str, bytes: &[u8]) -> Result<(), BackendError> {
        let path = self.setup_path(name)?;
        if path.exists() {
            return Ok(());
        }
        let dir = path
            .parent()
            .expect("a setup part lives in the setup folder");
        broca_owner_only::create_dir_all(dir)?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("a setup part's path ends in its hex name");
        // A unique temporary name, so two writers storing the same part never share a file.
        let temp = format!(".{file_name}.{}.tmp", hex(&crate::keys::random_id().0));
        write_file(dir, &temp, file_name, &[bytes])?;
        Ok(())
    }

    async fn get_setup(&self, name: &str) -> Result<Option<Vec<u8>>, BackendError> {
        match std::fs::read(self.setup_path(name)?) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn list_setup(&self) -> Result<Vec<String>, BackendError> {
        let mut names = Vec::new();
        for (folder, prefix) in [("setup", ""), (PLAIN_SETUP_DIR, crate::setup::PLAIN_PREFIX)] {
            let entries = match std::fs::read_dir(self.root.join(folder)) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            for entry in entries {
                let name = entry?.file_name();
                let name = name.to_str().ok_or(BackendError::Corrupt(
                    "a setup part's file name is not text",
                ))?;
                // A part being written sits in a dot-prefixed temporary file until it is renamed.
                if !name.starts_with('.') {
                    names.push(format!("{prefix}{name}"));
                }
            }
        }
        Ok(names)
    }
}

#[cfg(test)]
mod tests;

pub mod unpublished;
