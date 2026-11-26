//! Restoring a session after its store came back from a backup.
//!
//! A backup of the store that walks live files, without a snapshot, lists every file first, then
//! reads them, small ones first, and may read a file again later. So a restored session can
//! hold a head newer than the logs and objects beside it: a change that committed after the
//! listing named files the backup never listed. And a session's open log, read after the
//! listing, can hold records naming files written after it: a medium, a setup part, an import's
//! id map, an enrollment's overlay. Each was durable before the record naming it was appended,
//! so the backup holds the log only further along than the files it names.
//!
//! Normal loading never falls back to anything: a head naming a missing file is damage, refused
//! by name. Restore is the operator's repair, run on one session while nothing else holds it:
//! - **Kept.** When the session's head and everything it names are present and pass their
//!   checks, nothing is written.
//! - **A version.** Otherwise it checks the head, then every kept head version (see
//!   `backend::HEAD_VERSION_KEEP_MS`) newest first, for the files the version's changes wrote:
//!   its open log and checkpoint, its directory pages and sealed segments, its id index pages,
//!   and what the sealed records and the checkpoint name. The newest complete one is chosen, and
//!   written as a new version above every kept one. That undoes the changes (seals, reverts,
//!   recoveries) the backup didn't capture whole.
//! - **A prefix of the open log.** The chosen version's open log is shared with every version
//!   that names it, and the head doesn't bound its length. So if a record in it names a file the
//!   backup lacks, the log is cut back to where the batch holding the first such record starts.
//!   Everything the kept records name was written before them, so the prefix is complete. The
//!   bytes cut are kept in a file beside the log, `<log>.restore-cut-<ms>`, never deleted.
//!
//! Restore deletes nothing. Loading the session afterwards treats it as after a crash: a seal
//! whose intent ends the log is undone, a torn tail is cut, and the module ends an unfinished run
//! as interrupted. A file the backup lacks that no version and no cut can do without leaves the
//! session unrecoverable, reported with what is lacking, and nothing changes.

use std::path::PathBuf;
use std::{collections::BTreeMap, sync::Mutex};

use sha2::{Digest as _, Sha256};

use super::{chain_base, log_bind, media, Position, Session, SessionError};
use crate::backend::{BackendError, LocalBackend, SessionAddr, SessionBackend, Version};
use crate::checkpoint::Checkpoint;
use crate::codec::{hex, Id128, Message as _};
use crate::damage::{self, Verdict};
use crate::directory::{DirectoryError, Pages, FANOUT};
use crate::fold::operations::Operations;
use crate::head::{self, Head, HeadError, Residence};
use crate::idindex::{DataPage, IndexError, IndexPages, IndexRoot, RefPage};
use crate::keys::{DataKey, DeploymentKey};
use crate::log::ReadLog;
use crate::record::{
    EnrolledEntries, ImportReceipt, ImportedIdMap, ImportedIds, MediaObject, PartRef, Record,
};
use crate::segment::Segment;
use crate::setup::{LookupError, SetupError, SetupReader};

/// What restore did for one session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Restored {
    /// Everything the session's head names is there and passes its checks. Nothing was written.
    Kept { version: Version },
    /// The session was made whole from what the backup holds.
    Repaired {
        /// The version of the head the session had.
        from: Version,
        /// The version restored: `from` itself when only the open log was cut.
        to: Version,
        /// The version `to` was written as, above every kept version; `None` when the head
        /// stayed as it was.
        written: Option<Version>,
        /// How the open log was cut, if it was.
        cut: Option<LogCut>,
    },
    /// No version is complete next to the files the backup holds. What each version checked
    /// lacks, newest first. Nothing was written.
    Unrecoverable { lacking: Vec<(Version, String)> },
    /// The session doesn't load for a reason restore doesn't repair: a head or open log that
    /// fails its checks, or a recovery in progress. Nothing was written.
    Refused { reason: String },
}

/// How restore cut a session's open log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogCut {
    pub log: Id128,
    /// The seq of the record naming a file the backup lacks.
    pub record: u64,
    /// That file.
    pub lacking: String,
    /// The seq of the first record cut away: the first of that record's batch.
    pub first_cut: u64,
    /// The length the log was cut back to, and the length it had.
    pub at: u64,
    pub was: u64,
    /// The file holding the bytes cut away.
    pub sidecar: PathBuf,
}

#[derive(Debug)]
pub enum RestoreError {
    Backend(BackendError),
    /// The session is encrypted and the deployment key hasn't loaded.
    KeyUnavailable,
    /// Restore wrote, and the session's head still isn't complete: what it lacks.
    StillLacking(String),
}

impl From<BackendError> for RestoreError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

/// What a check of one head version found wrong with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finding {
    /// A file the version needs is absent, or holds bytes that don't pass their check, as an
    /// object copied while it was being written does: the backup lacks it whole.
    Lacks(String),
    /// Something restore doesn't repair: the head or the open log fails its checks, or a
    /// recovery is in progress.
    Refused(String),
}

/// Why a check stopped.
enum Stop {
    Found(Finding),
    Error(RestoreError),
}

impl From<BackendError> for Stop {
    fn from(error: BackendError) -> Self {
        Self::Error(RestoreError::Backend(error))
    }
}

fn lacks(what: impl Into<String>) -> Stop {
    Stop::Found(Finding::Lacks(what.into()))
}

fn refused(what: impl Into<String>) -> Stop {
    Stop::Found(Finding::Refused(what.into()))
}

/// The session as one kept head version names it, for a check: the head it reads is that
/// version's, every other read goes to the store, and nothing can be written.
struct Pinned<'a, B: SessionBackend> {
    store: &'a B,
    head: &'a [u8],
    version: Version,
    capture: Option<Capture<'a>>,
}

#[derive(Clone, Copy)]
struct Capture<'a> {
    files: &'a Mutex<BTreeMap<String, Vec<u8>>>,
    setup_from: Option<&'a BTreeMap<String, Vec<u8>>>,
}

impl<B: SessionBackend> Pinned<'_, B> {
    fn capture(&self, name: String, bytes: &[u8]) {
        if let Some(files) = self.capture {
            files.files.lock().unwrap().insert(name, bytes.to_vec());
        }
    }
}

fn read_only() -> BackendError {
    BackendError::Corrupt("a restore check only reads")
}

impl<B: SessionBackend> SessionBackend for Pinned<'_, B> {
    async fn read_head(
        &self,
        _s: &SessionAddr,
    ) -> Result<Option<(Vec<u8>, Version)>, BackendError> {
        Ok(Some((self.head.to_vec(), self.version)))
    }
    async fn write_head(
        &self,
        _s: &SessionAddr,
        _head: &[u8],
        _expected: Option<Version>,
        _next: Version,
    ) -> Result<Version, BackendError> {
        Err(read_only())
    }
    async fn create_log(
        &self,
        _s: &SessionAddr,
        _log: &Id128,
        _at: Version,
    ) -> Result<(), BackendError> {
        Err(read_only())
    }
    async fn read_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, BackendError> {
        let whole = range.start == 0 && range.end == self.store.log_end(s, log).await?;
        let bytes = self.store.read_log(s, log, range).await?;
        if whole {
            self.capture(format!("logs/{}/{}", s.0, hex(&log.0)), &bytes);
        }
        Ok(bytes)
    }
    async fn log_end(&self, s: &SessionAddr, log: &Id128) -> Result<u64, BackendError> {
        self.store.log_end(s, log).await
    }
    async fn append_log(
        &self,
        _s: &SessionAddr,
        _log: &Id128,
        _bytes: &[u8],
        _expected_end: u64,
        _at: Version,
        _barrier: bool,
    ) -> Result<u64, BackendError> {
        Err(read_only())
    }
    async fn truncate_log(
        &self,
        _s: &SessionAddr,
        _log: &Id128,
        _to: u64,
        _expected_end: u64,
        _at: Version,
    ) -> Result<(), BackendError> {
        Err(read_only())
    }
    async fn delete_log(
        &self,
        _s: &SessionAddr,
        _log: &Id128,
        _at: Version,
    ) -> Result<(), BackendError> {
        Err(read_only())
    }
    async fn put_object(
        &self,
        _s: &SessionAddr,
        _name: &str,
        _bytes: &[u8],
        _at: Version,
    ) -> Result<(), BackendError> {
        Err(read_only())
    }
    async fn sync_objects(
        &self,
        _s: &SessionAddr,
        _change: &Id128,
        _at: Version,
    ) -> Result<(), BackendError> {
        Err(read_only())
    }
    async fn get_object(
        &self,
        s: &SessionAddr,
        name: &str,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let bytes = self.store.get_object(s, name).await?;
        if let Some(bytes) = &bytes {
            self.capture(format!("objects/{}/{name}", s.0), bytes);
        }
        Ok(bytes)
    }
    async fn delete_object(
        &self,
        _s: &SessionAddr,
        _name: &str,
        _at: Version,
    ) -> Result<(), BackendError> {
        Err(read_only())
    }
    async fn delete_change(
        &self,
        _s: &SessionAddr,
        _change: &Id128,
        _at: Version,
    ) -> Result<(), BackendError> {
        Err(read_only())
    }
    async fn delete_session(&self, _s: &SessionAddr, _at: Version) -> Result<(), BackendError> {
        Err(read_only())
    }
    async fn put_setup(&self, _name: &str, _bytes: &[u8]) -> Result<(), BackendError> {
        Err(read_only())
    }
    async fn get_setup(&self, name: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let path = match name.strip_prefix(crate::setup::PLAIN_PREFIX) {
            Some(hex) => format!("setup-plain/{hex}"),
            None => format!("setup/{name}"),
        };
        // When checking a durable move, use its original setup names. A second copy of a
        // part may have become readable since the stream was built, without changing the cut.
        if self
            .capture
            .and_then(|capture| capture.setup_from)
            .is_some_and(|files| !files.contains_key(&path))
        {
            return Ok(None);
        }
        let bytes = self.store.get_setup(name).await?;
        if let Some(bytes) = &bytes {
            self.capture(path, bytes);
        }
        Ok(bytes)
    }
    async fn list_setup(&self) -> Result<Vec<String>, BackendError> {
        self.store.list_setup().await
    }
}

/// A file a record, or the operational state, names.
enum Named {
    /// A medium's bytes, stored as `object` says, whose SHA-256 is `sha256`.
    Medium {
        sha256: String,
        object: MediaObject,
    },
    Setup(PartRef),
    /// An import's id map and receipt.
    Import(ImportedIds),
    /// An enrollment's decisions.
    Enrollment(crate::record::EnrollmentOverlayRef),
    /// The saved media index a checkpoint names.
    MediaIndex(String),
}

/// The setup parts `record` names, which were stored before it was appended.
fn named_parts(record: &Record) -> &[PartRef] {
    match record {
        Record::RunStart(start) => &start.setup,
        Record::SetupStub(stub) => &stub.parts,
        Record::SessionRefresh(refresh) => &refresh.setup,
        _ => &[],
    }
}

/// Every file `record` names, each written before the record was appended.
fn named_by(record: &Record) -> Vec<Named> {
    let mut named: Vec<Named> = media::record_objects(record)
        .into_iter()
        .map(|(sha256, object)| Named::Medium { sha256, object })
        .collect();
    named.extend(named_parts(record).iter().cloned().map(Named::Setup));
    match record {
        Record::RunStart(start) => named.extend(start.imported_ids.clone().map(Named::Import)),
        Record::CompactionEnrolled(enrolled) => {
            named.push(Named::Enrollment(enrolled.overlay.clone()))
        }
        _ => {}
    }
    named
}

/// The files the operational state names: an import's and an enrollment's objects.
fn named_by_state(operations: &Operations) -> Vec<Named> {
    let mut named: Vec<Named> = operations
        .imported_ids
        .clone()
        .map(Named::Import)
        .into_iter()
        .collect();
    named.extend(
        operations
            .enrolled
            .as_ref()
            .map(|enrolled| Named::Enrollment(enrolled.overlay.clone())),
    );
    named
}

/// The files a log's checkpoint names: what its operational state names, and its saved media
/// index. The media index is derived, but a version naming one is complete only with it, as with
/// an import's id map.
fn named_by_checkpoint(checkpoint: &Checkpoint) -> Vec<Named> {
    let mut named = named_by_state(&checkpoint.operational);
    named.extend(
        checkpoint
            .media
            .as_ref()
            .and_then(|media| media.object.clone())
            .map(Named::MediaIndex),
    );
    named
}

/// Checks the files records name, for one session.
struct Objects<'a, B: SessionBackend> {
    store: &'a B,
    addr: &'a SessionAddr,
    key: &'a DataKey,
    setup: SetupReader<'a, B>,
}

impl<B: SessionBackend> Objects<'_, B> {
    /// What the first file `named` lists that the backup lacks is, if any.
    async fn first_lacking(&self, named: Vec<Named>) -> Result<Option<String>, Stop> {
        for file in named {
            if let Some(what) = self.lacking(&file).await? {
                return Ok(Some(what));
            }
        }
        Ok(None)
    }

    /// What `file` is, when the backup lacks it.
    async fn lacking(&self, file: &Named) -> Result<Option<String>, Stop> {
        match file {
            Named::Medium { sha256, object } => {
                let name = media::object_name(
                    self.key,
                    &object.log,
                    &object.generation,
                    object.seq,
                    sha256,
                );
                let whole = self
                    .sealed_object(&name)
                    .await?
                    .is_some_and(|bytes| hex(&Sha256::digest(bytes)) == *sha256);
                Ok((!whole).then(|| format!("medium {name}")))
            }
            Named::Setup(part) => match self.setup.get(part).await {
                Ok(_) => Ok(None),
                Err(LookupError::Store(SetupError::Missing(_) | SetupError::Corrupt(_))) => {
                    Ok(Some(format!("setup part {}", hex(&part.digest.0))))
                }
                Err(LookupError::Store(SetupError::Backend(error))) => Err(error.into()),
                Err(LookupError::Store(SetupError::CorruptStored(_))) => {
                    Ok(Some(format!("setup part {}", hex(&part.digest.0))))
                }
                Err(LookupError::KeyUnavailable) => Err(Stop::Error(RestoreError::KeyUnavailable)),
            },
            Named::Import(ids) => {
                let map = super::import::id_map_name(ids);
                let whole = self
                    .sealed_object(&map)
                    .await?
                    .is_some_and(|bytes| ImportedIdMap::decode(&bytes).is_ok());
                if !whole {
                    return Ok(Some(format!("import id map {map}")));
                }
                let receipt = super::import::receipt_name(ids);
                let whole = self
                    .sealed_object(&receipt)
                    .await?
                    .is_some_and(|bytes| ImportReceipt::decode(&bytes).is_ok());
                Ok((!whole).then(|| format!("import receipt {receipt}")))
            }
            Named::Enrollment(overlay) => {
                let name = super::enrollment::enrollment_name(overlay);
                let whole = self
                    .sealed_object(&name)
                    .await?
                    .is_some_and(|bytes| EnrolledEntries::decode(&bytes).is_ok());
                Ok((!whole).then(|| format!("enrollment overlay {name}")))
            }
            Named::MediaIndex(name) => {
                let whole = self
                    .sealed_object(name)
                    .await?
                    .is_some_and(|bytes| super::media_index::is_whole(&bytes));
                Ok((!whole).then(|| format!("media index {name}")))
            }
        }
    }

    /// The bytes of the session object `name`, sealed under the session key with its name, or
    /// `None` when it is absent or doesn't open.
    async fn sealed_object(&self, name: &str) -> Result<Option<Vec<u8>>, Stop> {
        let Some(sealed) = self.store.get_object(self.addr, name).await? else {
            return Ok(None);
        };
        Ok(self.key.frame_key().open_unit(name.as_bytes(), &sealed))
    }
}

/// A head version whose change files are all there, as its check read it.
struct Complete {
    head: Head,
    /// Its open log, as read: `None` for a head still creating its session, which names no log
    /// that must exist.
    log: Option<OpenLogRead>,
}

struct OpenLogRead {
    id: Id128,
    read: ReadLog,
    /// The first record naming a file the backup lacks: its index in `read.records`, and that
    /// file.
    lacking: Option<(usize, String)>,
}

impl Complete {
    fn lacking(&self) -> Option<&str> {
        self.log
            .as_ref()
            .and_then(|log| log.lacking.as_ref())
            .map(|(_, what)| what.as_str())
    }
}

/// Checks head version `version`, stored as `bytes`, the way restore needs: everything its
/// changes wrote must be there and pass its checks, or the version isn't complete; and the
/// first record of its open log naming a file the backup lacks is found. `current` says it is
/// the session's head, not a kept older version.
async fn check_version<B: SessionBackend>(
    store: &B,
    addr: &SessionAddr,
    deployment: &DeploymentKey,
    bytes: &[u8],
    position: (Version, bool),
    capture: Option<Capture<'_>>,
) -> Result<Complete, Stop> {
    let (version, current) = position;
    let (head, key) = match head::open(bytes, deployment) {
        Ok(opened) => opened,
        Err(HeadError::NotLoaded | HeadError::NoKey) => {
            return Err(Stop::Error(RestoreError::KeyUnavailable));
        }
        Err(error) => return Err(refused(format!("its head doesn't open: {error:?}"))),
    };
    match head.residence {
        Residence::Local(()) => {}
        // A head still creating its session names a log its load creates, so nothing it names
        // must exist. An older one is never chosen: it would restart the session empty.
        Residence::Creating(_) if current => return Ok(Complete { head, log: None }),
        Residence::Creating(_) => {
            return Err(refused(
                "it is the session's creation, before its first record",
            ))
        }
        Residence::Recovering(_) => return Err(refused("a recovery is in progress")),
        Residence::Moving(_) => return Err(refused("a move is in progress")),
        Residence::Moved(_) => return Err(refused("the session moved to another computer")),
    }
    let Some(open) = head.open_log.clone() else {
        return Err(refused("its head names no open log"));
    };
    // First what a load checks: the open log reads, ends in at most a torn tail, and its records
    // replay onto its checkpoint's state.
    let pinned = Pinned {
        store,
        head: bytes,
        version,
        capture,
    };
    match Session::view(&pinned, addr.clone(), &head.triple, deployment).await {
        Ok(_) => {}
        Err(SessionError::Backend(BackendError::NoSuchLog)) => {
            return Err(lacks(format!("log {}", hex(&open.change.0))))
        }
        Err(SessionError::Backend(BackendError::Io(error))) => {
            return Err(BackendError::Io(error).into())
        }
        Err(error) => return Err(refused(format!("it doesn't load: {error:?}"))),
    }
    let store = &pinned;
    let end = store.log_end(addr, &open.change).await?;
    let log_bytes = store.read_log(addr, &open.change, 0..end).await?;
    let frame_key = key.frame_key();
    let bind = log_bind(&head.session_id, &open.change);
    let read = match damage::analyse(
        &log_bytes,
        &frame_key,
        &bind,
        open.synced_len,
        chain_base,
        None,
    ) {
        Ok(Verdict::Opens(read)) => read,
        _ => return Err(refused("its open log changed while it was checked")),
    };
    let checkpoint = Checkpoint::decode(&read.checkpoint)
        .map_err(|error| refused(format!("its checkpoint doesn't decode: {error:?}")))?;
    let objects = Objects {
        store,
        addr,
        key: &key,
        setup: SetupReader::new(store, deployment, head.cipher()),
    };
    // What the checkpoint's state names was written before the change that wrote the
    // checkpoint.
    if let Some(what) = objects
        .first_lacking(named_by_checkpoint(&checkpoint))
        .await?
    {
        return Err(lacks(what));
    }
    // The sealed history: every directory page, every segment, and what their kept records
    // name.
    let pages = Pages {
        backend: store,
        addr,
        session: head.session_id,
        key: &frame_key,
        fanout: FANOUT,
    };
    for root in [&head.transcript_dir, &head.ledger_dir] {
        let entries = pages
            .entries_from(root.as_ref(), 0)
            .await
            .map_err(directory_finding)?;
        for entry in entries {
            let segment = format!("sealed segment {}", entry.object);
            let bytes = store
                .get_object(addr, &entry.object)
                .await?
                .ok_or_else(|| lacks(segment.clone()))?;
            let opened = Segment::open(&bytes, &frame_key, entry.object.as_bytes())
                .map_err(|_| lacks(segment.clone()))?;
            for position in 0..opened.index.blocks.len() {
                let records = opened.block(position).map_err(|_| lacks(segment.clone()))?;
                for (_, record) in records.iter().filter(|(seq, _)| entry.keeps(*seq)) {
                    if let Some(what) = objects.first_lacking(named_by(record)).await? {
                        return Err(lacks(what));
                    }
                }
            }
        }
    }
    // The id index: its root, every run's ref pages, and their data pages.
    if let Some(root) = &head.id_index {
        let index = IndexPages {
            backend: store,
            addr,
            session: head.session_id,
            key: &frame_key,
        };
        let root: IndexRoot = index.read(root).await.map_err(index_finding)?;
        for level in &root.levels {
            let refs = level
                .runs
                .iter()
                .flat_map(|run| &run.refs)
                .chain(level.merge.iter().flat_map(|merge| &merge.done));
            for info in refs {
                let page: RefPage = index.read(&info.object).await.map_err(index_finding)?;
                for data in &page.pages {
                    let _: DataPage = index.read(&data.object).await.map_err(index_finding)?;
                }
            }
        }
    }
    // Then the open log's own records, in order: the first one naming a file the backup lacks
    // is where its log is cut.
    let mut lacking = None;
    for (index, (_, record)) in read.records.iter().enumerate() {
        if let Some(what) = objects.first_lacking(named_by(record)).await? {
            lacking = Some((index, what));
            break;
        }
    }
    Ok(Complete {
        head,
        log: Some(OpenLogRead {
            id: open.change,
            read,
            lacking,
        }),
    })
}

fn directory_finding(error: DirectoryError) -> Stop {
    match error {
        DirectoryError::Missing(name) | DirectoryError::Corrupt(name) => {
            lacks(format!("directory page {name}"))
        }
        DirectoryError::Backend(error) => error.into(),
        DirectoryError::OutOfOrder => refused("a directory's entries are out of order"),
    }
}

fn index_finding(error: IndexError) -> Stop {
    match error {
        IndexError::Missing(name) | IndexError::Corrupt(name) => {
            lacks(format!("id index page {name}"))
        }
        IndexError::Backend(error) => error.into(),
    }
}

/// What the session at `addr` lacks or refuses, judged as restore judges it, writing nothing:
/// `None` when its head and everything it names are complete, so restore would keep it.
pub async fn check(
    store: &LocalBackend,
    addr: &SessionAddr,
    deployment: &DeploymentKey,
) -> Result<Option<Finding>, RestoreError> {
    let Some((bytes, version)) = store.read_stored_head(addr)? else {
        return Ok(Some(Finding::Refused("the session has no head".into())));
    };
    match check_version(store, addr, deployment, &bytes, (version, true), None).await {
        Ok(complete) => Ok(complete.lacking().map(|what| Finding::Lacks(what.into()))),
        Err(Stop::Found(finding)) => Ok(Some(finding)),
        Err(Stop::Error(error)) => Err(error),
    }
}

/// Captures stored bytes while the restore checker walks a pinned head's complete graph.
/// Using the same walk keeps media, setup and auxiliary objects in sync with restore's checks.
pub(super) async fn reachable<B: SessionBackend>(
    store: &B,
    addr: &SessionAddr,
    deployment: &DeploymentKey,
    bytes: &[u8],
    version: Version,
    setup_from: Option<&BTreeMap<String, Vec<u8>>>,
) -> Result<(BTreeMap<String, Vec<u8>>, Option<Finding>), RestoreError> {
    let files = Mutex::new(BTreeMap::new());
    let finding = match check_version(
        store,
        addr,
        deployment,
        bytes,
        (version, false),
        Some(Capture {
            files: &files,
            setup_from,
        }),
    )
    .await
    {
        Ok(complete) => complete.lacking().map(|what| Finding::Lacks(what.into())),
        Err(Stop::Found(finding)) => Some(finding),
        Err(Stop::Error(error)) => return Err(error),
    };
    Ok((files.into_inner().unwrap(), finding))
}

/// Restores the session at `addr` from the files its store holds after a backup came back (see
/// the module's description). The caller holds the session, so nothing else reads or writes it
/// meanwhile. `now_ms` names the file the bytes of a cut are kept in.
pub async fn restore(
    store: &LocalBackend,
    addr: &SessionAddr,
    deployment: &DeploymentKey,
    now_ms: u64,
) -> Result<Restored, RestoreError> {
    restore_with(store, addr, deployment, now_ms, true).await
}

/// [`restore`], cutting a version's open log only with `cut_logs`. Without it a version whose
/// open log names a file the backup lacks isn't complete: how restore would judge a backup if it
/// could only choose a version, which the tests compare against.
async fn restore_with(
    store: &LocalBackend,
    addr: &SessionAddr,
    deployment: &DeploymentKey,
    now_ms: u64,
    cut_logs: bool,
) -> Result<Restored, RestoreError> {
    let Some((current_bytes, current)) = store.read_stored_head(addr)? else {
        return Ok(Restored::Refused {
            reason: "the session has no head".into(),
        });
    };
    let lacking_in = |complete: &Complete| complete.lacking().unwrap_or_default().to_owned();
    let mut lacking = Vec::new();
    let mut chosen = None;
    match check_version(
        store,
        addr,
        deployment,
        &current_bytes,
        (current, true),
        None,
    )
    .await
    {
        Ok(complete) if complete.lacking().is_none() => {
            return Ok(Restored::Kept { version: current })
        }
        Ok(complete) if cut_logs => chosen = Some((current, current_bytes.clone(), complete)),
        Ok(complete) => lacking.push((current, lacking_in(&complete))),
        Err(Stop::Found(Finding::Lacks(what))) => lacking.push((current, what)),
        // Only a session that lacks a file is restore's to repair.
        Err(Stop::Found(Finding::Refused(reason))) => return Ok(Restored::Refused { reason }),
        Err(Stop::Error(error)) => return Err(error),
    }
    let kept = store.head_versions(addr)?;
    if chosen.is_none() {
        for version in kept.iter().rev().copied().filter(|v| *v != current) {
            let bytes = match store.read_head_version(addr, version) {
                Ok(Some(bytes)) => bytes,
                Ok(None) => continue,
                Err(BackendError::Corrupt(what)) => {
                    lacking.push((version, what.into()));
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            match check_version(store, addr, deployment, &bytes, (version, false), None).await {
                Ok(complete) if cut_logs || complete.lacking().is_none() => {
                    chosen = Some((version, bytes, complete));
                    break;
                }
                Ok(complete) => lacking.push((version, lacking_in(&complete))),
                Err(Stop::Found(Finding::Lacks(what) | Finding::Refused(what))) => {
                    lacking.push((version, what))
                }
                Err(Stop::Error(error)) => return Err(error),
            }
        }
    }
    let Some(chosen) = chosen else {
        return Ok(Restored::Unrecoverable { lacking });
    };
    let (version, bytes, complete) = chosen;
    let cut = match &complete.log {
        Some(OpenLogRead {
            id,
            read,
            lacking: Some((index, what)),
        }) => {
            let batch = read
                .batches
                .iter()
                .position(|batch| batch.contains(index))
                .expect("every record read is in a complete batch");
            let at = read.batch_starts[batch];
            // The prefix must load as the chosen version, before anything is written.
            let pinned = Pinned {
                store,
                head: &bytes,
                version,
                capture: None,
            };
            let position = Position {
                version,
                log: *id,
                len: at,
            };
            if let Err(error) = Session::view_at(
                &pinned,
                addr.clone(),
                &complete.head.triple,
                deployment,
                &position,
            )
            .await
            {
                return Ok(Restored::Refused {
                    reason: format!("its open log cut back to {at} bytes doesn't load: {error:?}"),
                });
            }
            let was = store.log_end(addr, id).await?;
            let sidecar = store.cut_log(addr, id, at, now_ms)?;
            Some(LogCut {
                log: *id,
                record: read.records[*index].0,
                lacking: what.clone(),
                first_cut: read.records[read.batches[batch].start].0,
                at,
                was,
                sidecar,
            })
        }
        _ => None,
    };
    let written = if version == current {
        None
    } else {
        let next = kept.iter().copied().fold(current, Version::max) + 1;
        store.write_head(addr, &bytes, Some(current), next).await?;
        Some(next)
    };
    if let Some(finding) = check(store, addr, deployment).await? {
        let what = match finding {
            Finding::Lacks(what) | Finding::Refused(what) => what,
        };
        return Err(RestoreError::StillLacking(what));
    }
    Ok(Restored::Repaired {
        from: current,
        to: version,
        written,
        cut,
    })
}

#[cfg(test)]
mod tests;
